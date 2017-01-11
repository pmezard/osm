package main

type unionNode struct {
	Parent *unionNode
	Id     int
	Rank   int
}

type UnionFind struct {
	nodes []unionNode
}

func NewUnionFind(count int) *UnionFind {
	u := &UnionFind{
		nodes: make([]unionNode, count),
	}
	for i := range u.nodes {
		u.nodes[i].Id = i
		u.nodes[i].Parent = &u.nodes[i]
	}
	return u
}

func (u *UnionFind) find(n *unionNode) *unionNode {
	if n.Parent == n {
		return n
	}
	return u.find(n.Parent)
}

func (u *UnionFind) Find(id int) int {
	return u.find(&u.nodes[id]).Id
}

func (u *UnionFind) Merge(i1, i2 int) {
	n1 := u.find(&u.nodes[i1])
	n2 := u.find(&u.nodes[i2])
	if n1 == n2 {
		return
	}
	if n1.Rank < n2.Rank {
		n1.Parent = n2
	} else if n1.Rank > n2.Rank {
		n2.Parent = n1
	} else {
		n2.Parent = n1
		n1.Rank++
	}
}
