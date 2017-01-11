package main

import "testing"

func TestUnionFind(t *testing.T) {
	uf := NewUnionFind(5)

	checkId := func(i, j int) {
		n := uf.Find(i)
		if n != j {
			t.Fatalf("unexpected id: %d: %d != %d", i, n, j)
		}
	}

	for i := 0; i < 4; i++ {
		checkId(i, i)
	}

	uf.Merge(1, 3)
	checkId(0, 0)
	checkId(1, 1)
	checkId(2, 2)
	checkId(3, 1)
	checkId(4, 4)

	uf.Merge(0, 2)
	checkId(0, 0)
	checkId(1, 1)
	checkId(2, 0)
	checkId(3, 1)
	checkId(4, 4)

	uf.Merge(2, 1)
	checkId(0, 0)
	checkId(1, 0)
	checkId(2, 0)
	checkId(3, 0)
	checkId(4, 4)

	uf.Merge(2, 4)
	checkId(0, 0)
	checkId(1, 0)
	checkId(2, 0)
	checkId(3, 0)
	checkId(4, 0)
}
