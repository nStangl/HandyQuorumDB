package memtable

import (
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/nStangl/distributed-kv-store/server/data"
)

type (
	RedBlackTree struct {
		tree *redblacktree.Tree
	}

	RedBlackTreeIterator struct {
		iter redblacktree.Iterator
	}
)

var (
	_ Table    = (*RedBlackTree)(nil)
	_ Iterator = (*RedBlackTreeIterator)(nil)
)

func NewRedBlackTree() *RedBlackTree {
	return &RedBlackTree{tree: redblacktree.NewWithStringComparator()}
}

func (t *RedBlackTree) Get(key string) data.Result {
	v, ok := t.tree.Get(key)
	if !ok {
		return data.Result{Kind: data.Missing}
	}

	return v.(data.Result)
}

func (t *RedBlackTree) Set(key, value string) {
	t.tree.Put(key, data.Result{Kind: data.Present, Value: value})
}

func (t *RedBlackTree) Del(key string) {
	if n := t.tree.GetNode(key); n != nil {
		n.Value = data.Result{Kind: data.Deleted}
	}
}

func (t *RedBlackTree) Size() int { return t.tree.Size() }

func (t *RedBlackTree) Iterator() Iterator {
	return &RedBlackTreeIterator{iter: t.tree.Iterator()}
}

func (t *RedBlackTreeIterator) Next() bool { return t.iter.Next() }

func (t *RedBlackTreeIterator) Value() Element {
	var (
		n = t.iter.Node()
		k = n.Key.(string)
		v = n.Value.(data.Result)
	)

	return Element{
		Kind:  v.Kind,
		Key:   k,
		Value: v.Value,
	}
}
