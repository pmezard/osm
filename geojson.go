package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/paulsmith/gogeos/geos"
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

type Linestring struct {
	Id     int64   `json:"id"`
	Role   string  `json:"role"`
	Points []Point `json:"points"`
}

func buildWay(way *Way, nodes NodePoints) (*Linestring, error) {
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

func reverseRing(r *Linestring) {
	for i := 0; i < len(r.Points)/2; i++ {
		j := len(r.Points) - 1 - i
		r.Points[i], r.Points[j] = r.Points[j], r.Points[i]
	}
}

func mergeRings(r1, r2 *Linestring) {
	if r1.Points[0] == r2.Points[0] {
		reverseRing(r1)
	} else if r1.Points[len(r1.Points)-1] == r2.Points[len(r2.Points)-1] {
		reverseRing(r2)
	}
	if r1.Points[len(r1.Points)-1] == r2.Points[0] {
		r1.Points = append(r1.Points, r2.Points[1:]...)
	} else if r1.Points[0] == r2.Points[len(r2.Points)-1] {
		r1.Points = append(r2.Points, r1.Points[1:]...)
	} else {
		panic("ring endpoints mismatch")
	}
}

func closeRings(rings []*Linestring) ([]*Linestring, error) {
	closed := []*Linestring{}
	open := []*Linestring{}
	for _, ring := range rings {
		if len(ring.Points) > 2 && ring.Points[0] == ring.Points[len(ring.Points)-1] {
			closed = append(closed, ring)
		} else {
			open = append(open, ring)
		}
	}
	if len(open) == 0 {
		return closed, nil
	}
	endPoints := map[Point][]int{}
	for i, ring := range open {
		first := ring.Points[0]
		last := ring.Points[len(ring.Points)-1]
		endPoints[first] = append(endPoints[first], i)
		endPoints[last] = append(endPoints[last], i)
	}
	for i := range open {
		ring := open[i]
		if ring == nil {
			continue
		}
		open[i] = nil
		for {
		Next:
			first := ring.Points[0]
			last := ring.Points[len(ring.Points)-1]
			for _, p := range []Point{first, last} {
				endPoint := endPoints[p]
				if len(endPoint) != 2 {
					return nil, fmt.Errorf("endpoint shared by more than one ring")
				}
				for _, j := range endPoint {
					other := open[j]
					if other == nil {
						continue
					}
					mergeRings(ring, other)
					open[j] = nil
					goto Next
				}
			}
			break
		}
		if ring.Points[0] == ring.Points[len(ring.Points)-1] {
			closed = append(closed, ring)
		} else {
			return nil, fmt.Errorf("could not merge rings")
		}
	}
	return closed, nil
}

func createGeosPoint(p Point) geos.Coord {
	return geos.Coord{
		X: float64(p.Lon) / 1e7,
		Y: float64(p.Lat) / 1e7,
	}
}

func createGeosPolygon(ring *Linestring) (*geos.Geometry, error) {
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

func createGeosGeometry(outer *Linestring, inner []*Linestring) (*geos.Geometry, error) {
	// Merge inner polygons with a single call to UnaryUnion, much faster than
	// calling Union repeatedly.
	innerPolys := make([]*geos.Geometry, 0, len(inner))
	for _, ring := range inner {
		p, err := createGeosPolygon(ring)
		if err != nil {
			return nil, err
		}
		innerPolys = append(innerPolys, p)
	}
	collection, err := geos.NewCollection(geos.MULTIPOLYGON, innerPolys...)
	if err != nil {
		return nil, err
	}
	merged, err := collection.UnaryUnion()
	if err != nil {
		return nil, err
	}
	// Then substract from the outer ring
	o, err := createGeosPolygon(outer)
	if err != nil {
		return nil, err
	}
	return o.Difference(merged)
}

func buildGeometry(rings []*Linestring) (*geos.Geometry, error) {
	// Bail out on non-ring inputs
	inner := []*Linestring{}
	outer := []*Linestring{}
	for _, ring := range rings {
		if ring.Role == "inner" {
			inner = append(inner, ring)
		} else if ring.Role == "outer" {
			outer = append(outer, ring)
		} else {
			return nil, fmt.Errorf("unsupported ring role: %s", ring.Role)
		}
	}
	inner, err := closeRings(inner)
	if err != nil {
		return nil, err
	}
	outer, err = closeRings(outer)
	if err != nil {
		return nil, err
	}
	if len(outer) > 1 {
		// Multiple outer include the island within island case which probably
		// requires some hierarchical containment test.
		return nil, fmt.Errorf("cannot handle geometry with multiple outer rings")
	} else if len(outer) == 0 {
		return nil, fmt.Errorf("no outer ring")
	}
	return createGeosGeometry(outer[0], inner)
}

type Location struct {
	Type        string        `json:"type"`
	Coordinates [][][]float64 `json:"coordinates"`
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

func geosToJson(g *geos.Geometry) ([]*Location, error) {
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
	loc := &Location{
		Type: "polygon",
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
	loc.Coordinates = rings
	return []*Location{loc}, nil
}

type RelationJson struct {
	Id       string   `json:"id"`
	Name     string   `json:"name"`
	Location Location `json:"loc"`
}

func makeJsonRelation(rel *Relation, g *geos.Geometry) (*RelationJson, error) {
	locations, err := geosToJson(g)
	if err != nil {
		return nil, err
	}
	if len(locations) > 1 {
		return nil, fmt.Errorf("cannot handle multipart json geometries")
	}
	if len(locations) == 0 {
		return nil, fmt.Errorf("cannot build relation json")
	}
	r := &RelationJson{
		Id:       strconv.Itoa(int(rel.Id)),
		Location: *locations[0],
	}
	for _, tag := range rel.Tags {
		if tag.Key == "name" {
			r.Name = tag.Value
		}
	}
	return r, nil
}

func isMultiPolygon(rel *Relation) bool {
	for _, tag := range rel.Tags {
		if tag.Key == "type" && tag.Value == "multipolygon" {
			return true
		}
	}
	return false
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

func collectWayRefs(rel *Relation) ([]Ref, error) {
	wayIds := []Ref{}
	for _, ref := range rel.Refs {
		if ref.Type == 0 {
			if ref.Role == "admin_centre" || ref.Role == "label" {
				continue
			}
			return nil, fmt.Errorf("cannot handle node relation: %s", ref.Role)
		}
		if ref.Type != 1 {
			if ref.Role == "subarea" {
				continue
			}
			return nil, fmt.Errorf("cannot handle relation relation: %s", ref.Role)
		}
		wayIds = append(wayIds, ref)
	}
	sort.Sort(sortedRefs(wayIds))
	return wayIds, nil
}

func collectWayGeometries(wayIds []Ref, db *WaysDb) ([]*Linestring, error) {
	// Resolve ways in a single scan
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
			continue
		}
		ring.Role = ref.Role
		rings = append(rings, ring)
	}
	return rings, nil
}

func buildRelation(rel *Relation, db *WaysDb) (*RelationJson, error) {
	if isMultiPolygon(rel) {
		return nil, fmt.Errorf("cannot handle multipolygons: %d", rel.Id)
	}
	// Collect way ids and sort them
	wayIds, err := collectWayRefs(rel)
	if err != nil {
		return nil, err
	}
	rings, err := collectWayGeometries(wayIds, db)
	if err != nil {
		return nil, err
	}
	g, err := buildGeometry(rings)
	if err != nil {
		return nil, err
	}
	return makeJsonRelation(rel, g)
}

func indexWays(r *O5MReader, nodes NodePoints, db *WaysDb) error {
	i := 0
	for r.Next() {
		if r.Kind() != WayKind {
			continue
		}
		w := r.Way()
		ring, err := buildWay(w, nodes)
		if err != nil {
			return err
		}
		err = db.Put(ring)
		if err != nil {
			return err
		}
		i++
		if (i % 100) == 0 {
			fmt.Println("indexed", i)
		}
	}
	return r.Err()
}
