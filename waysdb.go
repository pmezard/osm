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
		for _, name := range [][]byte{waysBucket, relationsBucket, locationsBucket} {
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
