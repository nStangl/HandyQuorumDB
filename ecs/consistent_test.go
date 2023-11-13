package ecs

import (
	"fmt"
	"testing"
)

type TestMember struct {
	Addr string
}

func (t TestMember) String() string { return t.Addr }

func create() *Consistent {
	cfg := HashConfig{}
	return NewConsistent(nil, cfg)
}

func TestBasicAdd(t *testing.T) {
	c := create()

	c.Add(MustServerMember("localhost", 8080))
	c.Add(MustServerMember("localhost", 8081))
	c.Add(MustServerMember("localhost", 8082))

	key := "very-special-key"
	owner := c.LocateKey([]byte(key))

	if owner == nil {
		t.Error("Key could not be assigned to node")
	}

	c.Add(MustServerMember("127.0.0.1", 4235))
	c.Add(MustServerMember("127.0.0.1", 9843))

	owner = c.LocateKey([]byte(key))
	if owner == nil {
		t.Error("Key could not be assigned to node")
	}
}

func TestSuccessorOne(t *testing.T) {
	c := create()

	node := MustServerMember("abc.de", 123)
	c.Add(node)
	suc, err := c.GetSuccessor(node.String())
	if err != nil {
		t.Error(err)
	}
	if suc != node {
		t.Error("Wrong successor. Expected", node, "but got", suc)
	}
}

func TestSuccessor(t *testing.T) {
	c := create()

	node1 := MustServerMember("www.google.com", 9234)
	node2 := MustServerMember("www.alibaba.com", 7233)

	c.Add(node1)
	c.Add(node2)

	suc, err := c.GetSuccessor(node1.String())
	if err != nil {
		t.Error(err)
	}
	if suc != node2 {
		t.Error("Wrong successor for", node1, ". Expected", node2, "got:", suc.String())
	}
	suc2, err := c.GetSuccessor(node2.String())
	if err != nil {
		t.Error(err)
	}
	if suc2 != node1 {
		t.Error("Wrong successor for", node2, ". Expected", node1, "got:", suc2.String())
	}
}

func TestPredecessorOne(t *testing.T) {
	c := create()

	node := MustServerMember("abc.de", 123)
	c.Add(node)
	suc, err := c.GetPredecessor(node.String())
	if err != nil {
		t.Error(err)
	}
	if suc != node {
		t.Error("Wrong predecessor. Expected", node, "but got", suc)
	}
}

func TestPredecessor(t *testing.T) {
	c := create()

	node1 := MustServerMember("www.google.com", 9234)
	node2 := MustServerMember("www.alibaba.com", 7233)

	c.Add(node1)
	c.Add(node2)

	pre, err := c.GetPredecessor(node1.String())
	if err != nil {
		t.Error(err)
	}
	if pre != node2 {
		t.Error("Wrong predecessor for", node1, ". Expected", node2, "got:", pre.String())
	}
	pre2, err := c.GetPredecessor(node2.String())
	if err != nil {
		t.Error(err)
	}
	if pre2 != node1 {
		t.Error("Wrong predecessor for", node2, ". Expected", node1, "got:", pre2.String())
	}
}

func MustServerMember(host string, port int) TestMember {
	return TestMember{Addr: fmt.Sprintf("%s:%d", host, port)}
}
