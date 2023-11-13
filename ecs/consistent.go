package ecs

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"sync"

	"github.com/nStangl/distributed-kv-store/util"

	proto "github.com/nStangl/distributed-kv-store/protocol"
	"lukechampine.com/uint128"
)

type (
	Member fmt.Stringer

	Hasher interface {
		Sum128([]byte) uint128.Uint128
	}

	ServerMember struct {
		store KVStore
		proto string
		addr  *net.TCPAddr
	}

	MD5Hasher struct{}

	HashConfig struct{}

	// Implements sort.Interface
	SortedHashKeys []uint128.Uint128

	Consistent struct {
		mu sync.RWMutex

		hasher     Hasher
		config     HashConfig
		sortedKeys SortedHashKeys
		ring       map[uint128.Uint128]Member
		members    map[string]Member
	}
)

func (x SortedHashKeys) Len() int           { return len(x) }
func (x SortedHashKeys) Less(i, j int) bool { return x[i].Cmp(x[j]) == -1 }
func (x SortedHashKeys) Swap(i, j int)      { x[i], x[j] = x[j], x[i] }

var (
	ErrEmptyRing = errors.New("empty_hashring")
)

func (s ServerMember) String() string {
	return s.proto
}

func NewServerMember(store *KVStore) ServerMember {
	remote := store.proto.Conn().RemoteAddr().(*net.TCPAddr)

	return ServerMember{
		store: store.Copy(),
		proto: store.proto.Conn().RemoteAddr().String(),
		addr:  util.CopyAddr(remote),
	}
}

func (hs MD5Hasher) Sum128(data []byte) uint128.Uint128 {
	return util.MD5HashUint128(string(data))
}

func NewConsistent(members []Member, config HashConfig) *Consistent {
	c := &Consistent{
		hasher:  MD5Hasher{},
		config:  config,
		members: make(map[string]Member),
		ring:    make(map[uint128.Uint128]Member),
	}

	for _, member := range members {
		c.add(member)
	}

	return c
}

func (c *Consistent) Add(member Member) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[member.String()]; ok {
		// This member already exists
		return
	}
	c.add(member)
}

func (c *Consistent) add(member Member) {
	hkey := c.hasher.Sum128([]byte(member.String()))
	c.ring[hkey] = member
	c.members[member.String()] = member
	c.sortedKeys = append(c.sortedKeys, hkey)

	sort.Sort(c.sortedKeys)
}

func (c *Consistent) deleteSlice(val uint128.Uint128) {
	for i := 0; i < len(c.sortedKeys); i++ {
		if c.sortedKeys[i].Equals(val) {
			c.sortedKeys = append(c.sortedKeys[:i], c.sortedKeys[i+1:]...)
			break
		}
	}
}

func (c *Consistent) Remove(name string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.members[name]; !ok {
		// No member with that name
		return
	}

	hkey := c.hasher.Sum128([]byte(name))
	delete(c.ring, hkey)
	delete(c.members, name)
	c.deleteSlice(hkey)
	sort.Sort(c.sortedKeys)
}

// Find the nearest Member in consistent hash ring for a given key
func (c *Consistent) LocateKey(key []byte) Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.members) == 0 {
		panic("The consistent hash ring is empty")
	}

	return c.ring[c.sortedKeys[c.searchNextRingIndex(key)]]
}

// Get Member that succeeds the given member in the hash ring.
// If ring only contains one member, the successor is the given member.
func (c *Consistent) GetSuccessor(name string) (Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, ok := c.members[name]; !ok {
		// No member with that name
		return nil, fmt.Errorf("member %q does not exist in hash ring, cannot find successor", name)
	}
	if len(c.members) < 1 {
		return nil, ErrEmptyRing
	}
	if len(c.members) == 1 {
		return c.members[name], nil
	}

	hkey := c.hasher.Sum128([]byte(name))
	idx := sort.Search(len(c.sortedKeys), func(i int) bool {
		return (c.sortedKeys[i].Cmp(hkey) != -1)
	})

	if idx == len(c.sortedKeys) {
		panic(fmt.Sprintf("Name %s could not be found in sorted set, even though members contains it", name))
	}

	idx = (idx + 1) % len(c.sortedKeys)

	return c.ring[c.sortedKeys[idx]], nil
}

// Get Member that preceds the given member in the hash ring.
// If ring only contains one member, the predecessor is the given member.
func (c *Consistent) GetPredecessor(name string) (Member, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if _, ok := c.members[name]; !ok {
		// No member with that name
		return nil, fmt.Errorf("member %q does not exist in hash ring, cannot find predecessor", name)
	}
	if len(c.members) < 1 {
		return nil, ErrEmptyRing
	}
	if len(c.members) == 1 {
		return c.members[name], nil
	}

	hkey := c.hasher.Sum128([]byte(name))
	idx := sort.Search(len(c.sortedKeys), func(i int) bool {
		return (c.sortedKeys[i].Cmp(hkey) != -1)
	})

	if idx == len(c.sortedKeys) {
		panic(fmt.Sprintf("Name %s could not be found in sorted set, even though members contains it", name))
	}

	idx = util.Modulo(idx-1, len(c.sortedKeys))

	return c.ring[c.sortedKeys[idx]], nil
}

// GetRanges each Member is responsible for
func (c *Consistent) GetRanges() []proto.KeyRange {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keyranges := make([]proto.KeyRange, 0, len(c.members))

	for name, member := range c.members {
		server := member.(ServerMember)

		hkey := c.hasher.Sum128([]byte(name))
		// Get index of predecessor
		idx := sort.Search(len(c.sortedKeys), func(i int) bool {
			return (c.sortedKeys[i].Cmp(hkey) != -1)
		})

		idx = util.Modulo(idx-1, len(c.sortedKeys))

		predecessorHKey := c.sortedKeys[idx]

		keyranges = append(keyranges, proto.KeyRange{
			ID:          server.store.id,
			Addr:        server.store.addr,
			PrivateAddr: server.store.privateAddr,
			Start:       predecessorHKey,
			End:         hkey,
		})
	}

	return keyranges
}

func (c *Consistent) FindRange(address string) (proto.KeyRange, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if r, ok := c.members[address]; ok {
		server := r.(ServerMember)

		hkey := c.hasher.Sum128([]byte(address))
		// Get index of predecessor
		idx := sort.Search(len(c.sortedKeys), func(i int) bool {
			return (c.sortedKeys[i].Cmp(hkey) != -1)
		})

		idx = util.Modulo(idx-1, len(c.sortedKeys))

		predecessorHKey := c.sortedKeys[idx]

		return proto.KeyRange{
			ID:          server.store.id,
			Addr:        server.addr,
			PrivateAddr: server.store.privateAddr,
			Start:       predecessorHKey,
			End:         hkey,
		}, true
	}

	return proto.KeyRange{}, false
}

// Can be used to conveniently hash a string.
// For testing purposes, not actually needed.
func (c *Consistent) HashKey(name string) uint128.Uint128 {
	h := c.hasher.Sum128([]byte(name))
	return h
}

func (c *Consistent) searchNextRingIndex(key []byte) int {
	targetKey := c.hasher.Sum128(key)

	targetIndex := sort.Search(len(c.sortedKeys), func(i int) bool {
		return c.sortedKeys[i].Cmp(targetKey) != -1 // >=
	})

	if targetIndex >= len(c.sortedKeys) {
		targetIndex = 0
	}

	return targetIndex
}

func (c *Consistent) GetConfig() HashConfig {
	return c.config
}

// Get a copy of the members
func (c *Consistent) GetMembers() []Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	members := make([]Member, 0, len(c.members))
	for _, member := range c.members {
		members = append(members, member)
	}
	return members
}

// Get a copy of the ring
func (c *Consistent) GetRing() map[uint128.Uint128]Member {
	c.mu.RLock()
	defer c.mu.RUnlock()

	res := make(map[uint128.Uint128]Member)
	for h, member := range c.ring {
		res[h] = member
	}

	return res
}

func (c *Consistent) GetSortedSet() []uint128.Uint128 {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.sortedKeys
}
