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
	geojsonCmd     = app.Command("geojson", "convert o5m to geojson")
	geojsonPath    = geojsonCmd.Arg("path", "o5m file path").Required().String()
	geojsonDb      = geojsonCmd.Arg("waysdb", "ways db path").Required().String()
	geojsonOutpath = geojsonCmd.Arg("outpath", "jsonl output path").Required().String()
	geojsonId      = geojsonCmd.Flag("id", "relation id").String()
	geojsonWorkers = geojsonCmd.Flag("workers", "workers count").Default("1").Int()
)

type ESDoc struct {
	Id     string        `json:"_id"`
	Type   string        `json:"_type"`
	Source *RelationJson `json:"_source"`
}

func processRelation(db *WaysDb, rel *Relation) (string, error) {
	js, err := buildRelation(rel, db)
	if err != nil {
		return "", err
	}
	if js == nil {
		return "", nil
	}
	doc := ESDoc{
		Id:     js.Id,
		Type:   "boundary",
		Source: js,
	}
	data, err := json.Marshal(&doc)
	return string(data), err
}

func geojsonFn() error {
	start := time.Now()
	workers := *geojsonWorkers
	r, err := NewO5MReader(*geojsonPath)
	if err != nil {
		return err
	}
	db, err := OpenWaysDb(*geojsonDb)
	if err != nil {
		return err
	}
	outFp, err := os.Create(*geojsonOutpath)
	if err != nil {
		return err
	}
	defer outFp.Close()

	relId := int64(-1)
	if *geojsonId != "" {
		relId, err = strconv.ParseInt(*geojsonId, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid relation identifier: %s", err)
		}
	}

	type Request struct {
		Relation *Relation
		Output   string
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
				data, err := processRelation(db, rq.Relation)
				if err != nil {
					rq.Err = err
				} else {
					rq.Output = data
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
				fmt.Fprintf(os.Stderr, "converted %d/%d\n", converted, seen)
			}
			rel := rq.Relation
			if rq.Err != nil {
				fmt.Fprintf(os.Stderr, "ERROR %d %s: %s\n", rel.Id, rel.Name(), rq.Err)
				continue
			}
			if rq.Output == "" {
				continue
			}
			fmt.Fprintln(outFp, rq.Output)
			converted++
		}
		close(done)
	}()

	for r.Next() {
		if r.Kind() != RelationKind {
			continue
		}
		rel := r.Relation()
		if relId >= 0 && relId != rel.Id {
			continue
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
	fmt.Fprintf(os.Stderr, "written: %d/%d in %ds\n", converted, seen, duration)
	return nil
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

var (
	indexRelationsCmd = app.Command("indexrelations",
		"index multistring relations in k/v store")
	indexRelationsO5m = indexRelationsCmd.Arg("o5mPath", "o5m file path").
				Required().String()
	indexRelationsDb = indexRelationsCmd.Arg("dbPath", "output DB path").
				Required().String()
)

func indexRelationsFn() error {
	r, err := NewO5MReader(*indexRelationsO5m)
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
