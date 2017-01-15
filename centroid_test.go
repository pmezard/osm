package main

import "testing"

func checkCentroid(t *testing.T, coords [][][][]float64, x, y float64) {
	c, err := computeCentroid(&Location{
		Type:        "multipolygon",
		Coordinates: coords,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c.Lon != x {
		t.Fatalf("x mismatch: %f != %f", c.Lon, x)
	}
	if c.Lat != y {
		t.Fatalf("y mismatch: %f != %f", c.Lat, y)
	}

}

func TestComputeCentroid(t *testing.T) {
	// Square
	coords := [][][][]float64{
		{
			{
				{0, 0},
				{0, 1},
				{1, 1},
				{1, 0},
				{0, 0},
			},
		},
	}
	checkCentroid(t, coords, 0.5, 0.5)

	// Horseshoe
	coords = [][][][]float64{
		{
			{
				{0, 0},
				{3, 0},
				{3, 3},
				{3, 2},
				{1, 2},
				{1, 1},
				{3, 1},
				{3, 0},
				{0, 0},
			},
		},
	}
	checkCentroid(t, coords, 2.125, 1.125)

	// Square with hole
	coords = [][][][]float64{
		{
			{
				{0, 0},
				{0, 3},
				{3, 3},
				{3, 0},
				{0, 0},
			},
			{
				{1, 1},
				{2, 1},
				{2, 2},
				{1, 2},
				{1, 1},
			},
		},
	}
	c, err := computeCentroid(&Location{
		Type:        "multipolygon",
		Coordinates: coords,
	})
	if err != nil {
		t.Fatal(err)
	}
	if c != nil {
		t.Fatal("unexpected centroid")
	}
}
