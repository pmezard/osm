package main

import (
	"encoding/binary"
	"encoding/json"

	"github.com/boltdb/bolt"
)

var (
	waysBucket = []byte("ways")
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
		_, err := tx.CreateBucketIfNotExists(waysBucket)
		return err
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

func (db *WaysDb) Put(w *Linestring) error {
	data, err := json.Marshal(w)
	if err != nil {
		return err
	}
	key := makeByteKey(w.Id)
	return db.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(waysBucket).Put(key, data)
	})
}

func (db *WaysDb) Get(id int64) (*Linestring, error) {
	key := makeByteKey(id)
	w := &Linestring{}
	err := db.db.View(func(tx *bolt.Tx) error {
		data := tx.Bucket(waysBucket).Get(key)
		if data == nil {
			w = nil
			return nil
		}
		return json.Unmarshal(data, w)
	})
	return w, err
}
