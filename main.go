package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/alecthomas/kingpin"
)

var (
	app = kingpin.New("o5m", "openstreetmap o5m manipulation tool")
)

var (
	countCmd  = app.Command("count", "count o5m elements")
	countPath = countCmd.Arg("path", "o5m file path").Required().String()
)

func countFn() error {
	r, err := NewO5MReader(*countPath)
	if err != nil {
		return err
	}

	nodes := 0
	ways := 0
	relations := 0
	resets := 0
	for r.Next() {
		if r.Kind() == NodeKind {
			nodes += 1
		} else if r.Kind() == WayKind {
			ways += 1
		} else if r.Kind() == RelationKind {
			relations += 1
		} else if r.Kind() == ResetKind {
			resets += 1
		}
	}
	if r.Err() != nil {
		return r.Err()
	}
	fmt.Println("resets", resets)
	fmt.Println("nodes", nodes)
	fmt.Println("ways", ways)
	fmt.Println("relations", relations)
	return nil
}

var (
	geojsonCmd  = app.Command("geojson", "convert o5m to geojson")
	geojsonPath = geojsonCmd.Arg("path", "o5m file path").Required().String()
)

type ESDoc struct {
	Id     string        `json:"_id"`
	Type   string        `json:"_type"`
	Source *RelationJson `json:"_source"`
}

func geojsonFn() error {
	r, err := NewO5MReader(*geojsonPath)
	if err != nil {
		return err
	}
	nodes, err := buildNodeArray(r)
	if err != nil {
		return err
	}
	wayReader, err := NewO5MReader(*geojsonPath)
	if err != nil {
		return err
	}
	resets := []ResetPoint{}
	for r.Next() {
		if r.Kind() == ResetKind {
			resets = append(resets, r.ResetPoint())
		} else if r.Kind() == RelationKind {
			if len(resets) != 2 {
				return fmt.Errorf("not enough reset points")
			}
			rel := r.Relation()
			js, err := buildRelation(rel, wayReader, resets[0], resets[1], nodes)
			if err != nil {
				fmt.Fprintf(os.Stderr, "ERROR %d %s\n", rel.Id, err)
			}
			if js == nil {
				continue
			}
			doc := ESDoc{
				Id:     js.Id,
				Type:   "relation",
				Source: js,
			}
			data, err := json.Marshal(&doc)
			if err != nil {
				return err
			}
			fmt.Println(string(data))
		}
	}
	if r.Err() != nil {
		return r.Err()
	}
	return nil
}

func dispatch() error {
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))
	switch cmd {
	case countCmd.FullCommand():
		return countFn()
	case geojsonCmd.FullCommand():
		return geojsonFn()
	}
	return fmt.Errorf("unknown command: %s", cmd)
}

func main() {
	err := dispatch()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}
