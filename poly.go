package main

import (
	"fmt"

	"github.com/pmezard/gogeos/geos"
)

// Returns the inclusion matrix where h[i][j] is true if rings[i] contains
// rings[j]. Rings do not contain themselves.
func computeInclusion(rings []*geos.Geometry) ([][]bool, error) {
	h := make([][]bool, len(rings))
	for i, outer := range rings {
		h[i] = make([]bool, len(rings))
		for j, inner := range rings {
			if i == j {
				continue
			}
			ok, err := outer.Contains(inner)
			if err != nil {
				return nil, err
			}
			if ok {
				h[i][j] = true
			}
		}
	}
	// Handle exact shapes, they do no contain themselves
	for i := range rings {
		for j := range rings {
			if h[i][j] && h[j][i] {
				h[i][j] = false
				h[j][i] = false
			}
		}
	}
	return h, nil
}

type inclusionNode struct {
	Id       int
	Shape    *geos.Geometry
	Children []*inclusionNode
}

// Returns a (id -> node) map of the inclusion DAG generated from the inclusion
// matrix.
func makeInclusionGraph(contains [][]bool, geoms []*geos.Geometry) map[int]*inclusionNode {
	nodes := map[int]*inclusionNode{}
	for i, row := range contains {
		n, ok := nodes[i]
		if !ok {
			n = &inclusionNode{
				Id:    i,
				Shape: geoms[i],
			}
			nodes[n.Id] = n
		}
		for j, ok := range row {
			if !ok {
				continue
			}
			c, ok := nodes[j]
			if !ok {
				c = &inclusionNode{
					Id:    j,
					Shape: geoms[j],
				}
				nodes[j] = c
			}
			n.Children = append(n.Children, c)
		}
	}
	return nodes
}

// Turns an inclusion DAG into a tree by keeping the longest inclusion chains.
func makeInclusionTree(root *inclusionNode) error {
	type Parent struct {
		Id     int
		Weight int
	}
	parents := map[int]Parent{}

	// DFS on the graph, detect cycles, and collect parent nodes by keeping
	// those belonging to the longest chains from the root.
	seen := map[int]bool{}
	var traverse func(n *inclusionNode, weight int) error
	traverse = func(n *inclusionNode, weight int) error {
		if _, ok := seen[n.Id]; ok {
			return fmt.Errorf("cycle detected")
		}
		seen[n.Id] = true
		for _, c := range n.Children {
			p, ok := parents[c.Id]
			if !ok || p.Weight < weight {
				parents[c.Id] = Parent{
					Id:     n.Id,
					Weight: weight,
				}
			}
			if err := traverse(c, weight+1); err != nil {
				return err
			}
		}
		delete(seen, n.Id)
		return nil
	}
	err := traverse(root, 0)
	if err != nil {
		return err
	}

	// Traverse the graph a second time and prune parent not kept in the first
	// pass, turning the graph into a tree
	var filter func(n *inclusionNode)
	filter = func(n *inclusionNode) {
		kept := []*inclusionNode{}
		for _, c := range n.Children {
			parent := parents[c.Id]
			if parent.Id == n.Id {
				kept = append(kept, c)
			}
			filter(c)
		}
		n.Children = kept
	}
	filter(root)
	return nil
}

func makeInclusionTrees(geoms []*geos.Geometry) ([]*inclusionNode, error) {
	// TODO: merge this step with the previous one
	h, err := computeInclusion(geoms)
	if err != nil {
		return nil, err
	}
	graph := makeInclusionGraph(h, geoms)
	children := map[int]bool{}
	for _, n := range graph {
		for _, c := range n.Children {
			children[c.Id] = true
		}
	}
	roots := []*inclusionNode{}
	for id := range h {
		if children[id] {
			continue
		}
		n := graph[id]
		err := makeInclusionTree(n)
		if err != nil {
			return nil, err
		}
		roots = append(roots, n)
	}
	return roots, nil
}

func createGeosSimplePolygon(ring *Linestring) (*geos.Geometry, error) {
	if len(ring.Points) < 4 {
		panic("not enough points")
	}
	if ring.Points[0] != ring.Points[len(ring.Points)-1] {
		panic("unclosed")
	}
	coords := make([]geos.Coord, len(ring.Points))
	for i := range coords {
		coords[i] = createGeosPoint(ring.Points[i])
	}
	poly, err := geos.NewPolygon(coords)
	if err != nil {
		return nil, err
	}
	// Poor man's solution to handle invalid polygons
	return poly.Buffer(0)
}

func createGeosPolygon(outer *geos.Geometry, inners []*geos.Geometry) (
	*geos.Geometry, error) {
	// Merge inner polygons with a single call to UnaryUnion, much faster than
	// calling Union repeatedly.
	collection, err := geos.NewCollection(geos.MULTIPOLYGON, inners...)
	if err != nil {
		return nil, err
	}
	merged, err := collection.UnaryUnion()
	if err != nil {
		return nil, err
	}
	return outer.Difference(merged)
}

func treesToPolygons(roots []*inclusionNode) ([]*geos.Geometry, error) {
	polygons := []*geos.Geometry{}
	for len(roots) > 0 {
		root := roots[len(roots)-1]
		roots = roots[:len(roots)-1]
		outer := root.Shape
		inners := []*geos.Geometry{}
		for _, c := range root.Children {
			inners = append(inners, c.Shape)
			for _, cc := range c.Children {
				roots = append(roots, cc)
			}
		}
		p, err := createGeosPolygon(outer, inners)
		if err != nil {
			return nil, err
		}
		polygons = append(polygons, p)
	}
	return polygons, nil
}

// Returns a collection of polygons (one outer ring, zero or more inner rings),
// built from a collection of rings. The algorithms works like:
// - Build a sequence of inclusion trees from all rings. A tree describes a set
// of rings including each other. A parent includes its children. Trees do not
// overlap.
// - Turn the roots and immediate children into outer and inner rings and recurse
// on the new roots produced by children children.
func makePolygons(rings []*Linestring) ([]*geos.Geometry, error) {
	// TODO: Fast-path trivial cases
	geoms := []*geos.Geometry{}
	for _, r := range rings {
		g, err := createGeosSimplePolygon(r)
		if err != nil {
			return nil, fmt.Errorf("cannot make linear ring: %s", err)
		}
		geoms = append(geoms, g)
	}
	trees, err := makeInclusionTrees(geoms)
	if err != nil {
		return nil, err
	}
	// TODO: check polygons do not intersect.
	return treesToPolygons(trees)
}
