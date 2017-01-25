package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
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
	locationsCmd     = app.Command("indexlocations", "convert o5m to geojson")
	locationsPath    = locationsCmd.Arg("path", "o5m file path").Required().String()
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
	db, err := OpenWaysDb(*locationsDb)
	if err != nil {
		return err
	}
	defer db.Close()

	relId := int64(-1)
	if *locationsId != "" {
		relId, err = strconv.ParseInt(*locationsId, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid relation identifier: %s", err)
		}
	}
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
				loc, err := buildLocation(rq.Relation, db)
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
				level := getTag(rel, "admin_level")
				fmt.Printf("ERROR %s(%d)[level=%s]: %s\n", rel.Name(), rel.Id,
					level, rq.Err)
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
		if ignoreRelation(rel) {
			continue
		}
		ok, err := db.HasLocation(rel.Id)
		if err != nil {
			return err
		}
		if ok {
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
	fmt.Printf("written: %d/%d in %ds\n", converted, seen, duration)
	return nil
}

var (
	geojsonCmd     = app.Command("geojson", "convert o5m to geojson")
	geojsonPath    = geojsonCmd.Arg("path", "o5m file path").Required().String()
	geojsonDb      = geojsonCmd.Arg("db", "db path").Required().String()
	geojsonOutpath = geojsonCmd.Arg("outpath", "jsonl output path").Required().String()
	geojsonId      = geojsonCmd.Flag("id", "relation id").String()
)

func geojsonFn() error {
	type ESDoc struct {
		Id     string        `json:"_id"`
		Type   string        `json:"_type"`
		Source *RelationJson `json:"_source"`
	}
	relId := int64(-1)
	if *geojsonId != "" {
		id, err := strconv.ParseUint(*geojsonId, 10, 64)
		if err != nil {
			return err
		}
		relId = int64(id)
	}

	start := time.Now()
	r, err := NewO5MReader(*geojsonPath, NodeKind, WayKind)
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

	seen := 0
	stop := false
	for r.Next() && !stop {
		if r.Kind() != RelationKind {
			continue
		}
		rel := r.Relation()
		if relId > 0 {
			if relId != rel.Id {
				continue
			}
			stop = true
		}
		js, err := buildRelation(rel, db)
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

var (
	indexCentersCmd = app.Command("indexcenters", "index relations admin_center")
	indexCentersO5m = indexCentersCmd.Arg("o5mPath", "o5m file path").
			Required().String()
	indexCentersDb = indexCentersCmd.Arg("db", "locations db path").
			Required().String()
)

func indexCentersFn() error {
	// Collect admin_center nodes
	db, err := OpenWaysDb(*indexCentersDb)
	if err != nil {
		return err
	}
	defer db.Close()
	nodeIds := map[int64][]int64{}
	r, err := NewO5MReader(*indexCentersO5m, NodeKind, WayKind)
	if err != nil {
		return err
	}
	polygons := 0
	indexed := 0
	for r.Next() {
		if r.Kind() != RelationKind {
			continue
		}
		rel := r.Relation()
		if ignoreRelation(rel) {
			continue
		}
		loc, err := db.GetLocation(rel.Id)
		if err != nil {
			return err
		}
		polygons++
		if loc == nil || len(loc.Coordinates) == 0 {
			continue
		}
		centerId := int64(-1)
		for _, ref := range rel.Refs {
			if ref.Type == 0 && (ref.Role == "admin_center" || ref.Role == "admin_centre") {
				centerId = ref.Id
			}
		}
		if centerId >= 0 {
			nodeIds[centerId] = append(nodeIds[centerId], rel.Id)
			continue
		}
		c, err := computeCentroid(loc)
		if err != nil {
			level := getTag(rel, "admin_level")
			fmt.Printf("cannot compute centroid: %s(%d)[level=%s]: %s\n",
				rel.Name(), rel.Id, level, err)
			continue
		}
		if c != nil {
			/*
				level := getTag(rel, "admin_level")
					fmt.Printf("CENTROID %s(%d)[level=%s]: %f,%f\n", rel.Name(), rel.Id, level,
						c.Lon, c.Lat)
			*/
			indexed++
			err = db.PutCentroid(rel.Id, c)
			if err != nil {
				return err
			}
		} else {
			level := getTag(rel, "admin_level")
			fmt.Printf("cannot get admin_center: %s(%d)[level=%s]\n",
				rel.Name(), rel.Id, level)
		}
	}
	if r.Err() != nil {
		return r.Err()
	}

	r, err = NewO5MReader(*indexCentersO5m)
	if err != nil {
		return err
	}
	seenNode := false
	for r.Next() {
		if r.Kind() != NodeKind {
			if seenNode && r.Kind() == ResetKind {
				break
			}
			continue
		}
		seenNode = true
		n := r.Node()
		c := &Centroid{
			NodeId: n.Id,
			Lon:    float64(n.Lon) / 1e7,
			Lat:    float64(n.Lat) / 1e7,
		}
		relIds := nodeIds[n.Id]
		for _, relId := range relIds {
			err = db.PutCentroid(relId, c)
			if err != nil {
				return err
			}
			indexed++
		}
	}
	fmt.Printf("indexed: %d/%d\n", indexed, polygons)
	return nil
}

var (
	printNodesCmd = app.Command("printnodes", "print node ids and lat/lng")
	printNodesO5m = printNodesCmd.Arg("o5mPath", "o5m file path").
			Required().String()
)

func formatCoord(c int64) string {
	s := fmt.Sprintf("%f", float64(c)/1e7)
	if !strings.ContainsRune(s, '.') {
		s += ".0"
	}
	return s
}

func printNodesFn() error {
	r, err := NewO5MReader(*printNodesO5m, WayKind, RelationKind)
	if err != nil {
		return err
	}
	count := 0
	resets := 0
	for r.Next() {
		if r.Kind() != NodeKind {
			if r.Kind() == ResetKind {
				resets++
				if resets > 1 {
					break
				}
			}
			continue
		}
		n := r.Node()
		fmt.Printf("%d %s %s\n", n.Id, formatCoord(n.Lat), formatCoord(n.Lon))
		count++
	}
	fmt.Println(count, "nodes")
	return r.Err()
}

var (
	printXmlNodesCmd = app.Command("printxmlnodes",
		"print node ids and lat/lng from osm file")
	printXmlNodesPath = printXmlNodesCmd.Arg("osmPath", "osm file path").
				Required().String()
)

var (
	// <node id="135821" lat="45.191733" lon="5.7346073"
	reNode = regexp.MustCompile(
		`^\s*<node\s+id="([^"]+)"\s+lat="([^"]+)"\s+lon="([^"]+)"`)
)

func printXmlNodesFn() error {
	fp, err := os.Open(*printXmlNodesPath)
	if err != nil {
		return err
	}
	defer fp.Close()

	count := 0
	prefix := []byte("<node")
	scanner := bufio.NewScanner(fp)
	for scanner.Scan() {
		data := scanner.Bytes()
		data = bytes.TrimSpace(data)
		if !bytes.HasPrefix(data, prefix) {
			continue
		}
		count++
		m := reNode.FindSubmatch(data)
		if m == nil {
			return fmt.Errorf("could not match node line: %s", string(data))
		}
		id := string(m[1])
		lat := string(m[2])
		lon := string(m[3])
		fmt.Println(id, lat, lon)
	}
	fmt.Println(count, "nodes")
	return scanner.Err()
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
	case indexCentersCmd.FullCommand():
		return indexCentersFn()
	case printNodesCmd.FullCommand():
		return printNodesFn()
	case printXmlNodesCmd.FullCommand():
		return printXmlNodesFn()
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
