package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/pmezard/gogeos/geos"
)

type Point struct {
	Lon int64 `json:"lon"`
	Lat int64 `json:"lat"`
}

type NodePoint struct {
	Id    int64
	Point Point
}

type NodePoints []NodePoint

func (points NodePoints) FindPoint(id int64) (NodePoint, error) {
	i := sort.Search(len(points), func(i int) bool {
		return points[i].Id >= id
	})
	if i == len(points) {
		return NodePoint{}, fmt.Errorf("cannot resolve node: %d", id)
	}
	return points[i], nil
}

func buildNodeArray(r *O5MReader) (NodePoints, error) {
	// Count nodes
	resets := []ResetPoint{}
	count := 0
	for r.Next() {
		if r.Kind() == ResetKind {
			resets = append(resets, r.ResetPoint())
			if len(resets) > 1 {
				break
			}
		} else if r.Kind() == NodeKind {
			if len(resets) == 0 {
				return nil, fmt.Errorf("node found before first reset")
			}
			count += 1
		}
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	if len(resets) != 2 {
		return nil, fmt.Errorf("more or less than 2 resets until nodes end")
	}

	// Collect nodes
	points := make([]NodePoint, count)
	err := r.Seek(resets[0])
	if err != nil {
		return nil, err
	}
	i := 0
	for r.Next() {
		if r.Kind() != NodeKind {
			continue
		}
		n := r.Node()
		points[i] = NodePoint{
			Id: n.Id,
			Point: Point{
				Lon: n.Lon,
				Lat: n.Lat,
			},
		}
		if i > 0 && points[i-1].Id >= points[i].Id {
			return nil, fmt.Errorf("nodes are not sorted by id: %d >= %d",
				points[i-1].Id, points[i].Id)
		}
		i += 1
		if i == len(points) {
			break
		}
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	if i != len(points) {
		return nil, fmt.Errorf("could not collect all nodes")
	}
	return NodePoints(points), r.Seek(resets[1])
}

func buildGeometry(rings []*Linestring) ([]*geos.Geometry, error) {
	// Bail out on non-ring inputs
	for _, ring := range rings {
		if ring.Role == "inner" || ring.Role == "outer" || ring.Role == "" {
			continue
		} else {
			return nil, fmt.Errorf("unsupported ring role: %s", ring.Role)
		}
	}
	all, err := makeRings(rings)
	if err != nil {
		return nil, err
	}
	return makePolygons(all)
}

type Location struct {
	Type        string          `json:"type"`
	Coordinates [][][][]float64 `json:"coordinates"`
}

func linearRingToJson(r *geos.Geometry) ([][]float64, error) {
	typ, err := r.Type()
	if typ != geos.LINEARRING {
		return nil, err
	}
	pointCount, err := r.NPoint()
	if err != nil {
		return nil, err
	}
	if pointCount <= 0 {
		return nil, fmt.Errorf("empty linear ring")
	}
	coords, err := r.Coords()
	if err != nil {
		return nil, fmt.Errorf("cannot get coordinates: %s", err)
	}
	ring := make([][]float64, len(coords))
	for j, p := range coords {
		ring[j] = []float64{p.X, p.Y}
	}
	return ring, nil
}

func isClockwise(ring [][]float64) bool {
	if len(ring) < 3 {
		// Undefined
		return false
	}
	area := 0.
	for i := 1; i < len(ring); i++ {
		p1 := ring[i-1]
		p2 := ring[i]
		area += (p2[0] - p1[0]) * (p2[1] + p1[1])
	}
	p1 := ring[len(ring)-1]
	p2 := ring[0]
	area += (p2[0] - p1[0]) * (p2[1] + p1[1])
	return area > 0
}

func reverseJsonRing(ring [][]float64) {
	for i := 0; i < len(ring)/2; i++ {
		j := len(ring) - 1 - i
		ring[i], ring[j] = ring[j], ring[i]
	}
}

func polygonsToJson(polygons []*geos.Geometry) (*Location, error) {
	loc := &Location{
		Type: "multipolygon",
	}
	shapes := [][][][]float64{}
	for _, g := range polygons {
		typ, err := g.Type()
		if err != nil {
			return nil, err
		}
		if typ != geos.POLYGON {
			return nil, fmt.Errorf("cannot handle geometry type: %d", typ)
		}
		geomCount, err := g.NGeometry()
		if err != nil {
			return nil, err
		}
		if geomCount <= 0 {
			return nil, fmt.Errorf("empty geometry")
		}
		shell, err := g.Shell()
		if err != nil {
			return nil, err
		}
		holes, err := g.Holes()
		if err != nil {
			return nil, err
		}
		rings := make([][][]float64, 0, len(holes)+1)
		inner, err := linearRingToJson(shell)
		if err != nil {
			return nil, fmt.Errorf("cannot extract inner ring: %s", err)
		}
		if isClockwise(inner) {
			reverseJsonRing(inner)
		}
		rings = append(rings, inner)
		for _, hole := range holes {
			outer, err := linearRingToJson(hole)
			if err != nil {
				return nil, fmt.Errorf("cannot extract outer ring: %s", err)
			}
			if !isClockwise(outer) {
				reverseJsonRing(outer)
			}
			rings = append(rings, outer)
		}
		shapes = append(shapes, rings)
	}
	loc.Coordinates = shapes
	return loc, nil
}

type RelationJson struct {
	Id       string       `json:"id"`
	Name     string       `json:"name"`
	Location Location     `json:"shape"`
	Tags     []StringPair `json:"tags"`
}

func makeJsonRelation(rel *Relation, polygons []*geos.Geometry) (*RelationJson, error) {
	if len(polygons) == 0 {
		return nil, fmt.Errorf("empty relation")
	}
	location, err := polygonsToJson(polygons)
	if err != nil {
		return nil, err
	}
	r := &RelationJson{
		Id:       strconv.Itoa(int(rel.Id)),
		Location: *location,
	}
	for _, tag := range rel.Tags {
		if tag.Key == "name" {
			r.Name = tag.Value
		}
		r.Tags = append(r.Tags, tag)
	}
	return r, nil
}

type sortedRefs []Ref

func (s sortedRefs) Len() int {
	return len(s)
}

func (s sortedRefs) Less(i, j int) bool {
	return s[i].Id < s[j].Id
}

func (s sortedRefs) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

var (
	IgnoredRelations = map[string]bool{
		"":                true, // at least on France/Spain shared territory
		"subarea":         true, // related but takes no part in geometry
		"subarea:FIXME":   true,
		"collection":      true, // deprecated
		"disused:subarea": true,
	}
)

func collectWayRefs(rel *Relation) ([]Ref, []Ref, error) {
	wayIds := []Ref{}
	relIds := []Ref{}
	for _, ref := range rel.Refs {
		if ref.Type != 1 {
			if ref.Type == 0 {
				// Points
				continue
			} else if ref.Type == 2 {
				// Relation
				if ref.Role == "outer" || ref.Role == "inner" {
					// Relations made of other relations (France borders)
					relIds = append(relIds, ref)
					continue
				}
				if IgnoredRelations[ref.Role] {
					continue
				}
				return nil, nil, fmt.Errorf("cannot handle relation relation: %s", ref.Role)
			} else {
				return nil, nil, fmt.Errorf("unsupported reference type: %d", ref.Type)
			}
		}
		wayIds = append(wayIds, ref)
	}
	sort.Sort(sortedRefs(wayIds))
	sort.Sort(sortedRefs(relIds))
	return wayIds, relIds, nil
}

func collectWayGeometries(wayIds []Ref, db *WaysDb) ([]*Linestring, error) {
	rings := []*Linestring{}
	if len(wayIds) <= 0 {
		return rings, nil
	}
	for _, ref := range wayIds {
		ring, err := db.Get(ref.Id)
		if err != nil {
			return nil, err
		}
		if ring == nil {
			return nil, fmt.Errorf("cannot resolve way: %d", ref.Id)
		}
		ring.Role = ref.Role
		rings = append(rings, ring)
	}
	return rings, nil
}

func collectRelationWays(relIds []Ref, db *WaysDb) ([]*Linestring, error) {
	rings := []*Linestring{}
	if len(relIds) <= 0 {
		return rings, nil
	}
	for _, ref := range relIds {
		rel, err := db.GetRelation(ref.Id)
		if err != nil {
			return nil, err
		}
		if rel == nil {
			return nil, fmt.Errorf("cannot resolve subrelation: %d", ref.Id)
		}
		wayIds, subIds, err := collectWayRefs(rel)
		if err != nil {
			return nil, err
		}
		if len(subIds) > 0 {
			lines, err := collectRelationWays(subIds, db)
			if err != nil {
				return nil, err
			}
			rings = append(rings, lines...)
		}
		ways, err := collectWayGeometries(wayIds, db)
		if err != nil {
			return nil, err
		}
		rings = append(rings, ways...)
	}
	return rings, nil
}

func getTag(rel *Relation, key string) string {
	for _, tag := range rel.Tags {
		if tag.Key == "type" {
			return tag.Value
		}
	}
	return ""
}

func isMultilineString(rel *Relation) bool {
	return getTag(rel, "type") == "multilinestring"
}

func isCollection(rel *Relation) bool {
	return getTag(rel, "type") == "collection"
}

func patchRings(rel *Relation, rings []*Linestring) []*Linestring {
	if rel.Id != 1362232 {
		return rings
	}
	// Metropolitan France polygon is not closed
	rings = append(rings,
		&Linestring{
			Id: 0,
			Points: []Point{
				{-17641958, 433431448},
				{-17668244, 433425557},
			},
		},
		&Linestring{
			Id: 1,
			Points: []Point{
				{37501395, 434237009},
				{37469067, 434193643},
			},
		})
	return rings
}

func buildRelation(rel *Relation, db *WaysDb) (*RelationJson, error) {
	if isCollection(rel) || isMultilineString(rel) {
		return nil, nil
	}
	// Collect way and relation ids and sort them
	wayIds, relIds, err := collectWayRefs(rel)
	if err != nil {
		return nil, err
	}
	rings, err := collectWayGeometries(wayIds, db)
	if err != nil {
		return nil, err
	}
	subRings, err := collectRelationWays(relIds, db)
	if err != nil {
		return nil, err
	}
	rings = append(rings, subRings...)
	rings = patchRings(rel, rings)
	polygons, err := buildGeometry(rings)
	if err != nil {
		return nil, err
	}
	return makeJsonRelation(rel, polygons)
}
