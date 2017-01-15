package main

import (
	"fmt"

	"github.com/pmezard/gogeos/geos"
)

type Centroid struct {
	Lon    float64 `json:"lon"`
	Lat    float64 `json:"lat"`
	NodeId int64   `json:"nodeid"`
}

func makeGeometriesFromLocation(loc *Location) ([]*geos.Geometry, error) {
	polygons := [][][][]float64{}
	if loc.Type == "multipolygon" {
		polygons = append(polygons, loc.Coordinates...)
	} else {
		return nil, fmt.Errorf("unsupported location type: %s", loc.Type)
	}
	geoms := []*geos.Geometry{}
	for _, poly := range polygons {
		// Assume first ring is outer, remaining ones, inner rings.
		rings := [][]geos.Coord{}
		for _, ring := range poly {
			r := make([]geos.Coord, len(ring))
			for i, p := range ring {
				r[i] = geos.Coord{
					X: p[0],
					Y: p[1],
				}
			}
			rings = append(rings, r)
		}
		if len(rings) == 0 {
			geoms = append(geoms, nil)
			continue
		}
		g, err := geos.NewPolygon(rings[0], rings[1:]...)
		if err != nil {
			return nil, err
		}
		geoms = append(geoms, g)
	}
	return geoms, nil
}

func getNeighbourVertices(ringLen, i int) (int, int) {
	ai := 0
	bi := 0
	if i > 0 {
		ai = i - 1
	} else {
		ai = ringLen - 1
	}
	if i < ringLen-1 {
		bi = i + 1
	} else {
		bi = 0
	}
	return ai, bi
}

func findConvexVertex(ring [][]float64) int {
	// Assume ring is outer ring and outer ring is clockwise
	l := len(ring)
	for i, v := range ring {
		ai, bi := getNeighbourVertices(l, i)
		a := ring[ai]
		b := ring[bi]
		crossp := ((a[0]-v[0])*(b[1]-v[1]) - (a[1]-v[1])*(b[0]-v[0]))
		if crossp >= 0 {
			return i
		}
	}
	return -1
}

func isInTriangle(a, v, b, q []float64) bool {
	// Barycentric coordinates tests. Not robust but will do for now.
	d := (v[1]-b[1])*(a[0]-b[0]) + (b[0]-v[0])*(a[1]-b[1])
	x := ((v[1]-b[1])*(q[0]-b[0]) + (b[0]-v[0])*(q[1]-b[1])) / d
	y := ((b[1]-a[1])*(q[0]-b[0]) + (a[0]-b[0])*(q[1]-b[1])) / d
	z := 1 - x - y
	return 0 <= x && x <= 1 && 0 <= y && y <= 1 && 0 <= z && z <= 1
}

func computeBarycenter(ring [][]float64) []float64 {
	c := []float64{0, 0}
	for _, p := range ring {
		c[0] += p[0]
		c[1] += p[1]
	}
	c[0] /= float64(len(ring))
	c[1] /= float64(len(ring))
	return c
}

func computeSimplePolygonCentroid(ring [][]float64) (*Centroid, error) {
	// See 3.6 in http://apodeline.free.fr/FAQ/CGAFAQ/CGAFAQ-3.html

	vi := findConvexVertex(ring)
	if vi < 0 {
		return nil, fmt.Errorf("cannot find convex vertex")
	}
	ai, bi := getNeighbourVertices(len(ring), vi)

	a := ring[ai]
	v := ring[vi]
	b := ring[bi]

	qIndex := -1
	qDist := float64(-1)
	for i, q := range ring {
		if i == ai || i == vi || i == bi {
			continue
		}
		if !isInTriangle(a, v, b, q) {
			continue
		}
		// Find the shortest diagonal to v
		dx := v[0] - q[0]
		dy := v[1] - q[1]
		d := dx*dx + dy*dy
		if qDist < 0 || d < qDist {
			qDist = d
			qIndex = i
		}
	}
	c := []float64{0, 0}
	if qIndex < 0 {
		// Convex polygon, return barycenter
		c = computeBarycenter(ring)
	} else {
		// Middle of (v, q) diagonal
		q := ring[qIndex]
		c[0] = (v[0] + q[0]) / 2
		c[1] = (v[1] + q[1]) / 2
	}
	return &Centroid{
		Lon: c[0],
		Lat: c[1],
	}, nil
}

func isCentroidInPolygon(c *Centroid, poly *geos.Geometry) (bool, error) {
	p, err := geos.NewPoint(geos.Coord{
		X: c.Lon,
		Y: c.Lat,
	})
	if err != nil {
		return false, err
	}
	ok, err := poly.Contains(p)
	if err != nil {
		return false, err
	}
	return ok, nil
}

func computeCentroid(loc *Location) (*Centroid, error) {
	polygons, err := makeGeometriesFromLocation(loc)
	if err != nil {
		return nil, err
	}
	// Find the largest polygon
	maxArea := float64(0)
	maxPoly := -1
	for i, p := range polygons {
		area, err := p.Area()
		if err != nil {
			return nil, err
		}
		if area > maxArea {
			maxArea = area
			maxPoly = i
		}
	}
	if maxPoly < 0 {
		return nil, nil
	}
	poly := loc.Coordinates[maxPoly]
	if len(poly) <= 0 {
		return nil, fmt.Errorf("invalid empty polygon")
	}
	outer := poly[0]

	// Cheap attempt with barycenter
	center := computeBarycenter(outer[1:])
	c := &Centroid{
		Lon: center[0],
		Lat: center[1],
	}
	ok, err := isCentroidInPolygon(c, polygons[maxPoly])
	if err != nil {
		return nil, err
	}
	if ok {
		return c, nil
	}

	c, err = computeSimplePolygonCentroid(outer[1:])
	if err != nil {
		return nil, err
	}
	// Centroid computation works with non-convex polygons but not always with
	// holes
	ok, err = isCentroidInPolygon(c, polygons[maxPoly])
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return c, nil
}
