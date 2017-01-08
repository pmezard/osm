package main

import "testing"

func makeSegment(id int64, p1, p2 Point) *Linestring {
	return &Linestring{
		Id:     id,
		Points: []Point{p1, p2},
	}
}

func TestCloseRings(t *testing.T) {
	points := []Point{
		{63157253, 495828250},
		{63393455, 495385894},
		{62918950, 495482440},
		{63122770, 495816200},
		{63249607, 495308781},
		{63553830, 495556220},
		{63391705, 495382442},
		{63425441, 495417741},
		{63396664, 495392000},
	}

	makeSegments := func(indices ...int) []*Linestring {
		segments := []*Linestring{}
		for i := range indices {
			if i == 0 {
				continue
			}
			segments = append(segments, &Linestring{
				Id: int64(i - 1),
				Points: []Point{
					points[indices[i-1]],
					points[indices[i]],
				},
			})
		}
		return segments
	}
	tests := [][]*Linestring{
		makeSegments(0, 1, 2, 0),
		makeSegments(0, 1, 2, 4, 0),
		makeSegments(0, 3, 2, 4, 6, 1, 8, 7, 5, 0),
	}
	for _, test := range tests {
		rings, err := closeRings(test)
		if err != nil {
			t.Fatal(err)
		}
		if len(rings) != 1 {
			t.Fatal("could not merge rings")
		}
	}
}
