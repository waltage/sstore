package sstore

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path"

	"github.com/boltdb/bolt"
	logz "github.com/waltage/dwio-logz"
)

var (
	dbLog = logz.NewLog(os.Stdout, "sstore.db", logz.Info)
	svLog = logz.NewLog(os.Stdout, "sstore.server", logz.Info)
	clLog = logz.NewLog(os.Stdout, "sstore.client", logz.Info)
)

var defaultBucket = "DEFAULT"

type sstore struct {
	db    *bolt.DB
	_init bool
}

func newSStore(localPath, dbName string) *sstore {
	fp := path.Join(localPath, dbName+".db")
	b := sstore{}
	db, err := bolt.Open(fp, 0600, nil)
	if err != nil {
		dbLog.Error("could not open ", err)
		return nil
	}
	b.db = db
	b._init = true
	return &b
}

func (ss *sstore) defaultInit() {
	if !ss._init {
		cwd, err := os.Getwd()
		if err != nil {
			dbLog.Error(err)
		}

		fp := path.Join(cwd, "_default.db")
		db, err := bolt.Open(fp, 0600, nil)
		if err != nil {
			dbLog.Error("could not open ", err)
		}
		ss.db = db
		ss._init = true
	}
}

func (ss *sstore) close() {
	if ss.db != nil {
		err := ss.db.Close()
		if err != nil {
			dbLog.Error(err)
		}
	}
}

func (ss *sstore) ListBuckets() (buckets []string, err error) {
	ss.defaultInit()
	err = ss.db.View(func(tx *bolt.Tx) error {
		c := tx.Cursor()
		for _, b := range iterBuckets(c) {
			buckets = append(buckets, b.Bucket)
		}
		return nil
	})
	if err != nil {
		dbLog.Warn(err)
	}
	return
}

func (ss *sstore) ListKeys(bucket string) (keys []*Key, err error) {
	ss.defaultInit()
	err = ss.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return ef("no keys in bucket '%s'", bucket)
		}
		c := b.Cursor()
		for _, k := range iterKeys(c) {
			k.Bucket = bucket
			keys = append(keys, k)
		}
		return nil
	})
	if err != nil {
		dbLog.Warn(err)
		return nil, err
	}
	SortKeys(keys)
	return
}

func (ss *sstore) SearchKeys(bucket, prefix string) (keys []*Key, err error) {
	ss.defaultInit()
	if bucket == "" || prefix == "" {
		return nil, nil
	}
	err = ss.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))
		if b == nil {
			return ef("bucket '%s' not found", bucket)
		}
		c := b.Cursor()
		for _, k := range iterKeysPrefix(c, prefix) {
			k.Bucket = bucket
			keys = append(keys, k)
		}
		return nil
	})
	if err != nil {
		dbLog.Warn(err)
		return
	}
	return
}

func (ss *sstore) Put(key *Key, obj Storable) (*Key, error) {
	ss.defaultInit()
	nKey := key.Copy()
	if nKey.Bucket == "" {
		nKey.Bucket = defaultBucket
	}
	err := ss.db.Update(func(tx *bolt.Tx) error {
		buck, err := tx.CreateBucketIfNotExists([]byte(nKey.Bucket))
		if err != nil {
			return err
		}
		versions := iterKeysPrefix(buck.Cursor(), key.ID)
		if len(versions) == 0 {
			return insertOp(nKey, obj, buck)
		}

		max := versions[len(versions)-1]
		if nKey.Version+1 > max.Version {
			err := rmKeys(versions, buck)
			if err != nil {
				return err
			}
			return insertOp(nKey, obj, buck)
		} else {
			_ = rmKeys(versions[:len(versions)-1], buck)
			return ef("stale key '%d' should be '%d'",
				nKey.Version, max.Version)
		}
	})
	if err != nil {
		dbLog.Warn(err)
		return nil, err
	}
	return nKey, err
}

func (ss *sstore) Get(key *Key, obj Storable) error {
	ss.defaultInit()
	if key.Bucket == "" {
		key.Bucket = defaultBucket
	}
	err := ss.db.View(func(tx *bolt.Tx) error {
		buck := tx.Bucket([]byte(key.Bucket))
		if buck == nil {
			return ef("bucket not found")
		}
		bts := buck.Get(key.KeyBytes())
		err := obj.Decode(bts)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		dbLog.Warn(err)
		return err
	}
	return err
}

func (ss *sstore) NewKey(bucket string) *Key {
	b := make([]byte, 6)
	_, err := rand.Read(b)
	if err != nil {
		dbLog.Info(err)
		b = []byte{0, 0, 0, 0, 0, 1}
	}
	s := base64.URLEncoding.EncodeToString(b)

	k := Key{
		Bucket:  bucket,
		ID:      s,
		Version: 1,
	}
	return &k
}

func (ss *sstore) Version(key *Key) (uint64, error) {
	ss.defaultInit()
	if key.Bucket == "" {
		key.Bucket = defaultBucket
	}
	var currVersion uint64 = 0
	err := ss.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(key.Bucket))
		if b == nil {
			return ef("bucket '%s' not found", key.Bucket)
		}

		versions := iterKeysPrefix(b.Cursor(), key.ID)
		if len(versions) == 0 {
			currVersion = 0
			return nil
		}
		currVersion = versions[len(versions)-1].Version
		return nil
	})
	if err != nil {
		dbLog.Warn(err)
		return 0, err
	}
	return currVersion, err
}

func (ss *sstore) Delete(key *Key) error {
	ss.defaultInit()
	if key.Bucket == "" {
		key.Bucket = "DEFAULT"
	}
	err := ss.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(key.Bucket))
		if b == nil {
			return ef("bucket '%s' not found", key.Bucket)
		}
		versions := iterKeysPrefix(b.Cursor(), key.ID)
		return rmKeys(versions, b)
	})
	if err != nil {
		dbLog.Warn(err)
	}
	return err
}

func iterKeys(c *bolt.Cursor) []*Key {
	var ret []*Key
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v != nil {
			key := ParseKeyString(string(k))
			ret = append(ret, key)
		}
	}
	SortKeys(ret)
	return ret
}

func iterBuckets(c *bolt.Cursor) []*Key {
	var ret []*Key
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil {
			key := ParseKeyString(string(k) + "::")
			ret = append(ret, key)
		}
	}
	SortKeys(ret)
	return ret
}

func iterKeysPrefix(c *bolt.Cursor, pref string) []*Key {
	var ret []*Key
	bts := []byte(pref)
	for k, v := c.Seek(bts); bytes.HasPrefix(k, bts); k, v = c.Next() {
		if v != nil {
			key := ParseKeyString(string(k))
			ret = append(ret, key)
		}
	}
	SortKeys(ret)
	return ret
}

func rmKeys(list []*Key, buck *bolt.Bucket) error {
	for _, k := range list {
		err := buck.Delete(k.KeyBytes())
		if err != nil {
			return err
		}
	}
	return nil
}

func insertOp(key *Key, obj Storable, buck *bolt.Bucket) error {
	key.Version++
	kBytes := key.KeyBytes()
	oBytes := obj.Encode()
	err := buck.Put(kBytes, oBytes)
	if err != nil {
		key.Version--
		dbLog.Warn(err)
		return err
	}
	return nil
}
