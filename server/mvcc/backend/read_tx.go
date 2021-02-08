// Copyright 2017 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"bytes"
	"github.com/dgraph-io/badger/v3"
	"math"
	"sync"

	bolt "go.etcd.io/bbolt"
)

// safeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
// is known to never overwrite any key so range is safe.
var safeRangeBucket = []byte("key")

type ReadTx interface {
	Lock()
	Unlock()
	RLock()
	RUnlock()

	UnsafeRange(bucketName []byte, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)
	UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error
}


// Base type for readTxBoltDB and concurrentReadTxBoltDB to eliminate duplicate functions between these
type baseReadTxBoltDB struct {
	// mu protects accesses to the txReadBuffer
	mu  sync.RWMutex
	buf txReadBuffer

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	txMu    *sync.RWMutex
	tx      *bolt.Tx
	buckets map[string]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	txWg *sync.WaitGroup
}

func (baseReadTx *baseReadTxBoltDB) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	if err := baseReadTx.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	baseReadTx.txMu.Lock()
	err := unsafeForEachBoltDB(baseReadTx.tx, bucketName, visitNoDup)
	baseReadTx.txMu.Unlock()
	if err != nil {
		return err
	}
	return baseReadTx.buf.ForEach(bucketName, visitor)
}

func (baseReadTx *baseReadTxBoltDB) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := baseReadTx.buf.Range(bucketName, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}

	// find/cache bucket
	bn := string(bucketName)
	baseReadTx.txMu.RLock()
	bucket, ok := baseReadTx.buckets[bn]
	baseReadTx.txMu.RUnlock()
	lockHeld := false
	if !ok {
		baseReadTx.txMu.Lock()
		lockHeld = true
		bucket = baseReadTx.tx.Bucket(bucketName)
		baseReadTx.buckets[bn] = bucket
	}

	// ignore missing bucket since may have been created in this batch
	if bucket == nil {
		if lockHeld {
			baseReadTx.txMu.Unlock()
		}
		return keys, vals
	}
	if !lockHeld {
		baseReadTx.txMu.Lock()
		lockHeld = true
	}
	c := bucket.Cursor()
	baseReadTx.txMu.Unlock()

	k2, v2 := unsafeRangeBoltDB(c, key, endKey, limit-int64(len(keys)))
	return append(k2, keys...), append(v2, vals...)
}

type readTxBoltDB struct {
	baseReadTxBoltDB
}

func (rt *readTxBoltDB) Lock()    { rt.mu.Lock() }
func (rt *readTxBoltDB) Unlock()  { rt.mu.Unlock() }
func (rt *readTxBoltDB) RLock()   { rt.mu.RLock() }
func (rt *readTxBoltDB) RUnlock() { rt.mu.RUnlock() }

func (rt *readTxBoltDB) reset() {
	rt.buf.reset()
	rt.buckets = make(map[string]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
}

type concurrentReadTxBoltDB struct {
	baseReadTxBoltDB
}

func (rt *concurrentReadTxBoltDB) Lock()   {}
func (rt *concurrentReadTxBoltDB) Unlock() {}

// RLock is no-op. concurrentReadTxBoltDB does not need to be locked after it is created.
func (rt *concurrentReadTxBoltDB) RLock() {}

// RUnlock signals the end of concurrentReadTxBoltDB.
func (rt *concurrentReadTxBoltDB) RUnlock() { rt.txWg.Done() }


// Base type for readTxBoltDB and concurrentReadTxBoltDB to eliminate duplicate functions between these
type baseReadTxBadgerDB struct {
	// mu protects accesses to the txReadBuffer
	mu  sync.RWMutex
	buf txReadBuffer

	// TODO: group and encapsulate {txMu, tx, buckets, txWg}, as they share the same lifecycle.
	// txMu protects accesses to buckets and tx on Range requests.
	txMu    *sync.RWMutex
	tx      *badger.Txn
	buckets map[string]*bolt.Bucket
	// txWg protects tx from being rolled back at the end of a batch interval until all reads using this tx are done.
	txWg *sync.WaitGroup
}

func (baseReadTx *baseReadTxBadgerDB) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}
		return visitor(k, v)
	}
	if err := baseReadTx.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}
	baseReadTx.txMu.Lock()
	err := unsafeForEachBadgerDB(baseReadTx.tx, bucketName, visitNoDup)
	baseReadTx.txMu.Unlock()
	if err != nil {
		return err
	}
	return baseReadTx.buf.ForEach(bucketName, visitor)
}

func (baseReadTx *baseReadTxBadgerDB) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		limit = 1
	}
	if limit <= 0 {
		limit = math.MaxInt64
	}
	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		panic("do not use unsafeRange on non-keys bucket")
	}
	keys, vals := baseReadTx.buf.Range(bucketName, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}

	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	c := baseReadTx.tx.NewIterator(opts)
	defer c.Close()

	baseReadTx.txMu.Unlock()

	k2, v2 := unsafeRangeBadgerDB(c, key, endKey, limit-int64(len(keys)))
	return append(k2, keys...), append(v2, vals...)
}

type readTxBadgerDB struct {
	baseReadTxBadgerDB
}

func (rt *readTxBadgerDB) Lock()    { rt.mu.Lock() }
func (rt *readTxBadgerDB) Unlock()  { rt.mu.Unlock() }
func (rt *readTxBadgerDB) RLock()   { rt.mu.RLock() }
func (rt *readTxBadgerDB) RUnlock() { rt.mu.RUnlock() }

func (rt *readTxBadgerDB) reset() {
	rt.buf.reset()
	//rt.buckets = make(map[string]*bolt.Bucket)
	rt.tx = nil
	rt.txWg = new(sync.WaitGroup)
}

type concurrentReadTxBadgerDB struct {
	baseReadTxBadgerDB
}

func (rt *concurrentReadTxBadgerDB) Lock()   {}
func (rt *concurrentReadTxBadgerDB) Unlock() {}

// RLock is no-op. concurrentReadTxBoltDB does not need to be locked after it is created.
func (rt *concurrentReadTxBadgerDB) RLock() {}

// RUnlock signals the end of concurrentReadTxBoltDB.
func (rt *concurrentReadTxBadgerDB) RUnlock() { rt.txWg.Done() }
