package main

import (
	"encoding/binary"
	"encoding/json"

	"github.com/boltdb/bolt"
)

var (
	waysBucket      = []byte("ways")
	relationsBucket = []byte("relations")
	locationsBucket = []byte("locations")
	nodesBucket     = []byte("nodes")
	centroidsBucket = []byte("centroids")
)

type WaysDb struct {
	db *bolt.DB
}

func OpenWaysDb(path string) (*WaysDb, error) {
	db, err := bolt.Open(path, 0666, nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if db != nil {
			db.Close()
		}
	}()
	err = db.Update(func(tx *bolt.Tx) error {
		names := [][]byte{
			waysBucket,
			relationsBucket,
			locationsBucket,
			nodesBucket,
			centroidsBucket,
		}
		for _, name := range names {
			_, err := tx.CreateBucketIfNotExists(name)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	waysDb := &WaysDb{
		db: db,
	}
	db = nil
	return waysDb, nil
}

func (db *WaysDb) Close() error {
	if db.db != nil {
		return db.db.Close()
	}
	return nil
}

func makeByteKey(id int64) []byte {
	buf := make([]byte, 9)
	n := binary.PutVarint(buf, id)
	return buf[:n]
}

func (db *WaysDb) putJson(bucket []byte, id int64, o interface{}) error {
	data, err := json.Marshal(o)
	if err != nil {
		return err
	}
	key := makeByteKey(id)
	return db.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucket).Put(key, data)
	})
}

func (db *WaysDb) getJson(bucket []byte, id int64, o interface{}) (bool, error) {
	key := makeByteKey(id)
	found := false
	err := db.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(bucket).Get(key)
		if data == nil {
			return nil
		}
		found = true
		return json.Unmarshal(data, o)
	})
	return found, err
}

func (db *WaysDb) Put(w *Linestring) error {
	return db.putJson(waysBucket, w.Id, w)
}

func (db *WaysDb) Get(id int64) (*Linestring, error) {
	w := &Linestring{}
	ok, err := db.getJson(waysBucket, id, w)
	if !ok {
		w = nil
	}
	return w, err
}

func (db *WaysDb) PutRelation(r *Relation) error {
	return db.putJson(relationsBucket, r.Id, r)
}

func (db *WaysDb) GetRelation(id int64) (*Relation, error) {
	r := &Relation{}
	ok, err := db.getJson(relationsBucket, id, r)
	if !ok {
		r = nil
	}
	return r, err
}

func (db *WaysDb) PutLocation(id int64, doc *Location) error {
	return db.putJson(locationsBucket, id, doc)
}

func (db *WaysDb) GetLocation(id int64) (*Location, error) {
	doc := &Location{}
	ok, err := db.getJson(locationsBucket, id, doc)
	if !ok {
		doc = nil
	}
	return doc, err
}

func (db *WaysDb) PutNode(id int64, doc *Node) error {
	return db.putJson(nodesBucket, id, doc)
}

func (db *WaysDb) GetNode(id int64) (*Node, error) {
	doc := &Node{}
	ok, err := db.getJson(nodesBucket, id, doc)
	if !ok {
		doc = nil
	}
	return doc, err
}

func (db *WaysDb) PutCentroid(id int64, doc *Centroid) error {
	return db.putJson(centroidsBucket, id, doc)
}

func (db *WaysDb) GetCentroid(id int64) (*Centroid, error) {
	doc := &Centroid{}
	ok, err := db.getJson(centroidsBucket, id, doc)
	if !ok {
		doc = nil
	}
	return doc, err
}
