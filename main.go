package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

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
	r, err := NewO5MReader(*countPath, NodeKind, WayKind, RelationKind)
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
	locationsCmd     = app.Command("locations", "convert o5m to geojson")
	locationsPath    = locationsCmd.Arg("path", "o5m file path").Required().String()
	locationsWays    = locationsCmd.Arg("ways", "ways db path").Required().String()
	locationsDb      = locationsCmd.Arg("db", "output locations db path").Required().String()
	locationsId      = locationsCmd.Flag("id", "relation id").String()
	locationsWorkers = locationsCmd.Flag("workers", "workers count").Default("1").Int()
)

func locationsFn() error {
	start := time.Now()
	workers := *locationsWorkers
	r, err := NewO5MReader(*locationsPath, NodeKind, WayKind)
	if err != nil {
		return err
	}
	ways, err := OpenWaysDb(*locationsWays)
	if err != nil {
		return err
	}
	relId := int64(-1)
	if *locationsId != "" {
		relId, err = strconv.ParseInt(*locationsId, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid relation identifier: %s", err)
		}
	}
	locations, err := OpenWaysDb(*locationsDb)
	if err != nil {
		return err
	}
	defer locations.Close()

	type Request struct {
		Relation *Relation
		Location *Location
		Err      error
	}
	pendings := make(chan Request)
	results := make(chan Request)
	running := sync.WaitGroup{}
	done := make(chan bool)
	for i := 0; i < workers; i++ {
		running.Add(1)
		go func() {
			defer running.Done()
			for rq := range pendings {
				loc, err := buildLocation(rq.Relation, ways, locations)
				if err != nil {
					rq.Err = err
				} else {
					rq.Location = loc
				}
				results <- rq
			}
		}()
	}
	go func() {
		running.Wait()
		close(results)
	}()
	seen := 0
	converted := 0
	go func() {
		for rq := range results {
			seen++
			if seen%100 == 0 {
				fmt.Printf("converted %d/%d\n", converted, seen)
			}
			rel := rq.Relation
			if rq.Err != nil {
				fmt.Printf("ERROR %d %s: %s\n", rel.Id, rel.Name(), rq.Err)
				continue
			}
			if rq.Location == nil {
				continue
			}
			converted++
		}
		close(done)
	}()

	stop := false
	for r.Next() && !stop {
		if r.Kind() != RelationKind {
			continue
		}
		rel := r.Relation()
		if relId >= 0 {
			if relId != rel.Id {
				continue
			} else {
				stop = true
			}
		}
		rq := Request{
			Relation: rel.Clone(),
		}
		pendings <- rq
	}
	close(pendings)
	if r.Err() != nil {
		return r.Err()
	}
	<-done
	end := time.Now()
	duration := (end.Sub(start) / time.Second)
	fmt.Printf("written: %d/%d in %ds\n", converted, seen, duration)
	return nil
}

var (
	geojsonCmd     = app.Command("geojson", "convert o5m to geojson")
	geojsonPath    = geojsonCmd.Arg("path", "o5m file path").Required().String()
	geojsonLoc     = geojsonCmd.Arg("locations", "locations db path").Required().String()
	geojsonOutpath = geojsonCmd.Arg("outpath", "jsonl output path").Required().String()
)

func geojsonFn() error {
	type ESDoc struct {
		Id     string        `json:"_id"`
		Type   string        `json:"_type"`
		Source *RelationJson `json:"_source"`
	}

	start := time.Now()
	r, err := NewO5MReader(*geojsonPath, NodeKind, WayKind)
	if err != nil {
		return err
	}
	locations, err := OpenWaysDb(*geojsonLoc)
	if err != nil {
		return err
	}
	outFp, err := os.Create(*geojsonOutpath)
	if err != nil {
		return err
	}
	defer outFp.Close()

	seen := 0
	for r.Next() {
		if r.Kind() != RelationKind {
			continue
		}
		rel := r.Relation()
		js, err := buildRelation(rel, locations)
		if err != nil {
			fmt.Printf("ERROR: %s(%d): %s\n", rel.Name(), rel.Id, err)
			continue
		}
		if js == nil {
			continue
		}
		doc := ESDoc{
			Id:     js.Id,
			Type:   "boundary",
			Source: js,
		}
		data, err := json.Marshal(&doc)
		fmt.Fprintln(outFp, string(data))
		seen++
		if seen%1000 == 0 {
			fmt.Println("converted", seen)
		}
	}
	if r.Err() != nil {
		return r.Err()
	}
	end := time.Now()
	duration := (end.Sub(start) / time.Second)
	fmt.Printf("written: %d in %ds\n", seen, duration)
	return nil
}

func indexWays(r *O5MReader, nodes NodePoints, db *WaysDb) error {
	i := 0
	for r.Next() {
		if r.Kind() != WayKind {
			continue
		}
		w := r.Way()
		ring, err := buildLinestring(w, nodes)
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

var (
	indexWaysCmd = app.Command("indexways", "index ways in k/v store")
	indexWaysO5m = indexWaysCmd.Arg("o5mPath", "o5m file path").Required().String()
	indexWaysDb  = indexWaysCmd.Arg("dbPath", "output DB path").Required().String()
)

func indexWaysFn() error {
	r, err := NewO5MReader(*indexWaysO5m)
	if err != nil {
		return err
	}
	if _, err := os.Stat(*indexWaysDb); err == nil {
		err = os.Remove(*indexWaysDb)
		if err != nil {
			return err
		}
	}
	db, err := OpenWaysDb(*indexWaysDb)
	if err != nil {
		return err
	}
	defer db.Close()
	nodes, err := buildNodeArray(r)
	if err != nil {
		return err
	}
	return indexWays(r, nodes, db)
}

func indexRelations(r *O5MReader, db *WaysDb) error {
	// List relations to collect
	fmt.Println("listing relations to collect")
	kept := map[int64]bool{}
	resets := []ResetPoint{}
	for r.Next() {
		if r.Kind() != RelationKind {
			if r.Kind() == ResetKind {
				resets = append(resets, r.ResetPoint())
			}
			continue
		}
		rel := r.Relation()
		if isMultilineString(rel) {
			kept[rel.Id] = true
			continue
		}
		for _, ref := range rel.Refs {
			if ref.Type != 2 {
				continue
			}
			if ref.Role == "inner" || ref.Role == "outer" ||
				rel.Id == 11980 && ref.Role == "subarea" {
				kept[ref.Id] = true
			}
		}
	}
	if len(resets) != 3 {
		return fmt.Errorf("could not collect reset points")
	}
	fmt.Println("collecting")
	err := r.Seek(resets[2])
	if err != nil {
		return err
	}
	i := 0
	for r.Next() {
		if r.Kind() != RelationKind {
			continue
		}
		rel := r.Relation()
		if !kept[rel.Id] {
			continue
		}
		fmt.Println("indexing", rel.Id, rel.Name())
		err := db.PutRelation(rel)
		if err != nil {
			return err
		}
		i++
		if (i % 100) == 0 {
			fmt.Println("indexed", i)
		}
	}
	fmt.Println("indexed", i)
	return r.Err()
}

var (
	indexRelationsCmd = app.Command("indexrelations",
		"index multistring relations in k/v store")
	indexRelationsO5m = indexRelationsCmd.Arg("o5mPath", "o5m file path").
				Required().String()
	indexRelationsDb = indexRelationsCmd.Arg("dbPath", "output DB path").
				Required().String()
)

func indexRelationsFn() error {
	r, err := NewO5MReader(*indexRelationsO5m, NodeKind, WayKind)
	if err != nil {
		return err
	}
	db, err := OpenWaysDb(*indexRelationsDb)
	if err != nil {
		return err
	}
	defer db.Close()
	return indexRelations(r, db)
}

func dispatch() error {
	cmd := kingpin.MustParse(app.Parse(os.Args[1:]))
	switch cmd {
	case countCmd.FullCommand():
		return countFn()
	case geojsonCmd.FullCommand():
		return geojsonFn()
	case indexWaysCmd.FullCommand():
		return indexWaysFn()
	case indexRelationsCmd.FullCommand():
		return indexRelationsFn()
	case locationsCmd.FullCommand():
		return locationsFn()
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
