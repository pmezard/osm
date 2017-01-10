package main

import (
	"fmt"

	"github.com/paulsmith/gogeos/geos"
)

type Linestring struct {
	Id     int64   `json:"id"`
	Role   string  `json:"role"`
	Points []Point `json:"points"`
}

func (ls *Linestring) Start() Point {
	return ls.Points[0]
}

func (ls *Linestring) End() Point {
	return ls.Points[len(ls.Points)-1]
}

func (ls *Linestring) Clone() *Linestring {
	points := make([]Point, len(ls.Points))
	copy(points, ls.Points)
	return &Linestring{
		Id:     ls.Id,
		Role:   ls.Role,
		Points: points,
	}
}

func (ls *Linestring) Reverse() {
	l := len(ls.Points)
	for i := 0; i < l/2; i++ {
		j := l - 1 - i
		ls.Points[i], ls.Points[j] = ls.Points[j], ls.Points[i]
	}
}

func buildLinestring(way *Way, nodes NodePoints) (*Linestring, error) {
	points := make([]Point, len(way.Nodes))
	for i, n := range way.Nodes {
		p, err := nodes.FindPoint(n)
		if err != nil {
			return nil, err
		}
		points[i] = p.Point
	}
	return &Linestring{
		Id:     way.Id,
		Points: points,
	}, nil
}

// RingParts is used to iteratively add lines together to form a ring.
type RingParts struct {
	parts []*Linestring
	start Point
	end   Point
	role  string
}

func (r *RingParts) Start() Point {
	return r.start
}

func (r *RingParts) End() Point {
	return r.end
}

// Returns true if Start() == End()
func (r *RingParts) IsClosed() bool {
	return r.start == r.end
}

// Add a Linestring to the current set. Panic if linestring does not start or
// end with the current end point. Input Linestring is copied.
func (r *RingParts) Push(p *Linestring) {
	p = p.Clone()
	if p.End() == r.end {
		p.Reverse()
	}
	if r.end == p.Start() {
		r.end = p.End()
	} else {
		panic("ring and part are not linked")
	}
	r.parts = append(r.parts, p)
}

// Remove the most recently added Linestring from the set. Panic if set is
// empty.
func (r *RingParts) Pop() *Linestring {
	end := len(r.parts) - 1
	p := r.parts[end]
	r.parts = r.parts[:end]
	r.end = p.Start()
	return p
}

// Combine all Linestrings into a single one. Panic if RingParts is not closed
// or is empty.
func (r *RingParts) MakeRing() *Linestring {
	if !r.IsClosed() {
		panic("ring must be closed")
	}
	if len(r.parts) == 0 {
		panic("ring has no part")
	}
	base := r.parts[0].Clone()
	for _, other := range r.parts[1:] {
		if base.End() != other.Start() {
			panic("parts are not linked")
		}
		base.Points = append(base.Points, other.Points[1:]...)
		if base.Role != "" && base.Role != other.Role {
			base.Role = ""
		}
	}
	if base.Start() != base.End() {
		panic("unclosed ring")
	}
	return base
}

func createGeosPoint(p Point) geos.Coord {
	return geos.Coord{
		X: float64(p.Lon) / 1e7,
		Y: float64(p.Lat) / 1e7,
	}
}

func makeLinearRing(r *Linestring) (*geos.Geometry, error) {
	coords := make([]geos.Coord, len(r.Points))
	for i, p := range r.Points {
		coords[i] = createGeosPoint(p)
	}
	return geos.NewLinearRing(coords...)
}

// Returns true if Linestring is closed and non self-intersecting.
func isValidRing(r *Linestring) bool {
	ring, err := makeLinearRing(r)
	if err != nil {
		return false
	}
	if ok, err := ring.IsRing(); err != nil || !ok {
		return false
	}
	if ok, err := ring.IsSimple(); err != nil || !ok {
		return false
	}
	return true
}

func makeRing(parts RingParts, lines []*Linestring, seen map[int64]bool) *Linestring {
	if parts.Start() == parts.End() {
		r := parts.MakeRing()
		if !isValidRing(r) {
			return nil
		}
		return r
	}
	// TODO: collect end points in a map to speedup iterations
	for _, next := range lines {
		if seen[next.Id] {
			continue
		}
		if next.Start() != parts.End() && next.End() != parts.End() {
			continue
		}
		seen[next.Id] = true
		parts.Push(next)
		r := makeRing(parts, lines, seen)
		if r != nil {
			return r
		}
		parts.Pop()
		seen[next.Id] = false
	}
	return nil
}

// Take a collection of lines and combine them to form rings. Returned
// Linestring first and last points are equal. The call fails if not all lines
// end in a ring.
func makeRings(lines []*Linestring) ([]*Linestring, error) {
	rings := []*Linestring{}
	seen := map[int64]bool{}
	for _, line := range lines {
		if seen[line.Id] {
			continue
		}
		seen[line.Id] = true
		parts := RingParts{
			parts: []*Linestring{line},
			start: line.Start(),
			end:   line.End(),
		}
		r := makeRing(parts, lines, seen)
		if r == nil {
			return nil, fmt.Errorf("could not close ring")
		}
		rings = append(rings, r)
	}
	return rings, nil
}
