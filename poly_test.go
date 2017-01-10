package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/paulsmith/gogeos/geos"
)

func makeGeosPolygons(rings []*Linestring) []*geos.Geometry {
	geoms := []*geos.Geometry{}
	for _, r := range rings {
		g, err := createGeosSimplePolygon(r)
		if err != nil {
			panic(err)
		}
		geoms = append(geoms, g)
	}
	return geoms
}

func makeTestRing(points []Point) *Linestring {
	scaled := []Point{}
	for _, p := range points {
		p.Lon *= 10000
		p.Lat *= 10000
		scaled = append(scaled, p)
	}
	r := &Linestring{
		Points: scaled,
	}
	if r.Start() != r.End() {
		r.Points = append(r.Points, r.Start())
	}
	return r
}

func printNode(n *inclusionNode, w io.Writer, prefix string) {
	fmt.Fprintf(w, "%s%d: [", prefix, n.Id)
	for _, c := range n.Children {
		fmt.Fprintf(w, "%d ", c.Id)
	}
	fmt.Fprintf(w, "]\n")
	for _, c := range n.Children {
		printNode(c, w, prefix+"  ")
	}
}

func printTrees(t *testing.T, rings []*Linestring) string {
	geoms := makeGeosPolygons(rings)
	nodes, err := makeInclusionTrees(geoms)
	if err != nil {
		t.Fatal(err)
	}
	buf := &bytes.Buffer{}
	for _, n := range nodes {
		printNode(n, buf, "")
	}
	return buf.String()
}

func stripLines(s string) string {
	lines := []string{}
	scanner := bufio.NewScanner(bytes.NewBuffer([]byte(s)))
	for scanner.Scan() {
		l := strings.TrimSpace(scanner.Text())
		if l != "" {
			lines = append(lines, strings.TrimSpace(scanner.Text()))
		}
	}
	if scanner.Err() != nil {
		panic(scanner.Err())
	}
	return strings.Join(lines, "\n")
}

func checkTrees(t *testing.T, rings []*Linestring, expected string) {
	s := printTrees(t, rings)
	expected = stripLines(expected)
	s = stripLines(s)
	if expected != s {
		t.Fatal(s)
	}
}

func TestMakeContainmentTrees(t *testing.T) {
	// Single polygon
	checkTrees(t, []*Linestring{
		makeTestRing([]Point{{0, 0}, {0, 1}, {1, 1}, {1, 0}}),
	}, `
0: []
	`)

	// Simple full inclusion
	checkTrees(t, []*Linestring{
		makeTestRing([]Point{{0, 0}, {0, 3}, {3, 3}, {3, 0}}),
		makeTestRing([]Point{{1, 1}, {1, 2}, {2, 2}, {2, 1}}),
	}, `
0: [1 ]
1: []
	`)

	// Disjoint shapes
	checkTrees(t, []*Linestring{
		makeTestRing([]Point{{0, 0}, {0, 3}, {3, 3}, {3, 0}}),
		makeTestRing([]Point{{4, 4}, {4, 5}, {5, 5}, {5, 4}}),
	}, `
0: []
1: []
	`)

	// Island
	checkTrees(t, []*Linestring{
		makeTestRing([]Point{{0, 0}, {0, 5}, {5, 5}, {5, 0}}),
		makeTestRing([]Point{{1, 1}, {1, 4}, {4, 4}, {4, 1}}),
		makeTestRing([]Point{{2, 2}, {2, 3}, {3, 3}, {3, 2}}),
	}, `
0: [1 ]
1: [2 ]
2: []
	`)

	// One hole plus one island
	checkTrees(t, []*Linestring{
		makeTestRing([]Point{{0, 0}, {0, 5}, {7, 5}, {7, 0}}),
		makeTestRing([]Point{{1, 1}, {1, 4}, {4, 4}, {4, 1}}),
		makeTestRing([]Point{{2, 2}, {2, 3}, {3, 3}, {3, 2}}),
		makeTestRing([]Point{{5, 2}, {5, 3}, {6, 3}, {6, 2}}),
	}, `
0: [1 3 ]
1: [2 ]
2: []
3: []
	`)

	// Equal shapes with a parent.
	checkTrees(t, []*Linestring{
		makeTestRing([]Point{{1, 1}, {1, 2}, {2, 2}, {2, 1}}),
		makeTestRing([]Point{{1, 1}, {1, 2}, {2, 2}, {2, 1}}),
		makeTestRing([]Point{{0, 0}, {0, 3}, {3, 3}, {3, 0}}),
	}, `
2: [0 1 ]
0: []
1: []
	`)

	// Equal shapes without parent.
	checkTrees(t, []*Linestring{
		makeTestRing([]Point{{1, 1}, {1, 2}, {2, 2}, {2, 1}}),
		makeTestRing([]Point{{1, 1}, {1, 2}, {2, 2}, {2, 1}}),
	}, `
0: []
1: []
	`)
}
