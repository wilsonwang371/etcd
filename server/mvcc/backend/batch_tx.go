// Copyright 2015 The etcd Authors
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
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type BatchTx interface {
	ReadTx
	UnsafeCreateBucket(name []byte)
	UnsafePut(bucketName []byte, key []byte, value []byte)
	UnsafeSeqPut(bucketName []byte, key []byte, value []byte)
	UnsafeDelete(bucketName []byte, key []byte)
	// Commit commits a previous tx and begins a new writable one.
	Commit()
	// CommitAndStop commits the previous tx and does not create a new one.
	CommitAndStop()
}

type batchTxBadgerDB struct {
	sync.Mutex
	tx      *badger.Txn
	backend *badgerdbBackend

	pending int
}

func (t *batchTxBadgerDB) Lock() {
	t.Mutex.Lock()
}

func (t *batchTxBadgerDB) Unlock() {
	if t.pending >= t.backend.batchLimit {
		t.commit(false)
	}
	t.Mutex.Unlock()
}


func (t *batchTxBadgerDB) RLock() {
	panic("unexpected RLock")
}

func (t *batchTxBadgerDB) RUnlock() {
	panic("unexpected RUnlock")
}

func (t *batchTxBadgerDB) UnsafeCreateBucket(name []byte) {
	// TODO
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTxBadgerDB) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTxBadgerDB) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, true)
}

func (t *batchTxBadgerDB) unsafePut(_ []byte, key []byte, value []byte, _ bool) {
	if err := t.tx.Set(key, value); err != nil {
		t.backend.lg.Fatal(
			"failed to write data",
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTxBadgerDB) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := t.tx.NewIterator(opts)
	defer it.Close()
	return unsafeRangeBadgerDB(it, key, endKey, limit)
}

func unsafeRangeBadgerDB(it *badger.Iterator, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	for it.Seek(key); it.ValidForPrefix(key) && isMatch(key); it.Next() {
		item := it.Item()
		keys = append(keys, item.Key())
		val, err := item.ValueCopy(nil)
		if err != nil {
			break
		}
		vs = append(vs, val)
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}


// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTxBadgerDB) UnsafeDelete(bucketName []byte, key []byte) {
	err := t.tx.Delete(key)
	if err != nil {
		t.backend.lg.Fatal(
			"failed to delete a key",
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTxBadgerDB) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	return unsafeForEachBadgerDB(t.tx, bucketName, visitor)
}

func unsafeForEachBadgerDB(tx *badger.Txn, bucket []byte, visitor func(k, v []byte) error) error {
	opts := badger.DefaultIteratorOptions
	opts.PrefetchSize = 10
	it := tx.NewIterator(opts)
	defer it.Close()
	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		k := item.Key()
		err := item.Value(func(v []byte) error {
			return visitor(k, v)
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTxBadgerDB) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTxBadgerDB) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBadgerDB) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *batchTxBadgerDB) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		if t.pending == 0 && !stop {
			return
		}

		//start := time.Now()

		// gofail: var beforeCommit struct{}
		err := t.tx.Commit()
		// gofail: var afterCommit struct{}

		//rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
		//spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
		//writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		//commitSec.Observe(time.Since(start).Seconds())
		atomic.AddInt64(&t.backend.commits, 1)

		t.pending = 0
		if err != nil {
			t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
		}
	}
	if !stop {
		t.tx = t.backend.begin(true)
	}
}









type batchTxBoltDB struct {
	sync.Mutex
	tx      *bolt.Tx
	backend *boltdbBackend

	pending int
}

func (t *batchTxBoltDB) Lock() {
	t.Mutex.Lock()
}

func (t *batchTxBoltDB) Unlock() {
	if t.pending >= t.backend.batchLimit {
		t.commit(false)
	}
	t.Mutex.Unlock()
}

// BatchTx interface embeds ReadTx interface. But RLock() and RUnlock() do not
// have appropriate semantics in BatchTx interface. Therefore should not be called.
// TODO: might want to decouple ReadTx and BatchTx

func (t *batchTxBoltDB) RLock() {
	panic("unexpected RLock")
}

func (t *batchTxBoltDB) RUnlock() {
	panic("unexpected RUnlock")
}

func (t *batchTxBoltDB) UnsafeCreateBucket(name []byte) {
	_, err := t.tx.CreateBucket(name)
	if err != nil && err != bolt.ErrBucketExists {
		t.backend.lg.Fatal(
			"failed to create a bucket",
			zap.String("bucket-name", string(name)),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTxBoltDB) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTxBoltDB) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, true)
}

func (t *batchTxBoltDB) unsafePut(bucketName []byte, key []byte, value []byte, seq bool) {
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.String("bucket-name", string(bucketName)),
		)
	}
	if seq {
		// it is useful to increase fill percent when the workloads are mostly append-only.
		// this can delay the page split and reduce space usage.
		bucket.FillPercent = 0.9
	}
	if err := bucket.Put(key, value); err != nil {
		t.backend.lg.Fatal(
			"failed to write to a bucket",
			zap.String("bucket-name", string(bucketName)),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTxBoltDB) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.String("bucket-name", string(bucketName)),
		)
	}
	return unsafeRangeBoltDB(bucket.Cursor(), key, endKey, limit)
}

func unsafeRangeBoltDB(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	if limit <= 0 {
		limit = math.MaxInt64
	}
	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		vs = append(vs, cv)
		keys = append(keys, ck)
		if limit == int64(len(keys)) {
			break
		}
	}
	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTxBoltDB) UnsafeDelete(bucketName []byte, key []byte) {
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		t.backend.lg.Fatal(
			"failed to find a bucket",
			zap.String("bucket-name", string(bucketName)),
		)
	}
	err := bucket.Delete(key)
	if err != nil {
		t.backend.lg.Fatal(
			"failed to delete a key",
			zap.String("bucket-name", string(bucketName)),
			zap.Error(err),
		)
	}
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTxBoltDB) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	return unsafeForEachBoltDB(t.tx, bucketName, visitor)
}

func unsafeForEachBoltDB(tx *bolt.Tx, bucket []byte, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket); b != nil {
		return b.ForEach(visitor)
	}
	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTxBoltDB) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTxBoltDB) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBoltDB) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()
	return t.pending
}

func (t *batchTxBoltDB) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		if t.pending == 0 && !stop {
			return
		}

		start := time.Now()

		// gofail: var beforeCommit struct{}
		err := t.tx.Commit()
		// gofail: var afterCommit struct{}

		rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
		spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
		writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		commitSec.Observe(time.Since(start).Seconds())
		atomic.AddInt64(&t.backend.commits, 1)

		t.pending = 0
		if err != nil {
			t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
		}
	}
	if !stop {
		t.tx = t.backend.begin(true)
	}
}

type batchTxBufferedBoltDB struct {
	batchTxBoltDB
	buf txWriteBuffer
}

func newBatchTxBufferedBoltDB(backend *boltdbBackend) *batchTxBufferedBoltDB {
	tx := &batchTxBufferedBoltDB{
		batchTxBoltDB: batchTxBoltDB{backend: backend},
		buf: txWriteBuffer{
			txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			seq:      true,
		},
	}
	tx.Commit()
	return tx
}

func (t *batchTxBufferedBoltDB) Unlock() {
	if t.pending != 0 {
		t.backend.readTx.Lock() // blocks txReadBuffer for writing.
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.Unlock()
		if t.pending >= t.backend.batchLimit {
			t.commit(false)
		}
	}
	t.batchTxBoltDB.Unlock()
}

func (t *batchTxBufferedBoltDB) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBufferedBoltDB) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBufferedBoltDB) commit(stop bool) {
	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.Unlock()
}

func (t *batchTxBufferedBoltDB) unsafeCommit(stop bool) {
	if t.backend.readTx.tx != nil {
		// wait all store read transactions using the current boltdb tx to finish,
		// then close the boltdb tx
		go func(tx *bolt.Tx, wg *sync.WaitGroup) {
			wg.Wait()
			if err := tx.Rollback(); err != nil {
				t.backend.lg.Fatal("failed to rollback tx", zap.Error(err))
			}
		}(t.backend.readTx.tx, t.backend.readTx.txWg)
		t.backend.readTx.reset()
	}

	t.batchTxBoltDB.commit(stop)

	if !stop {
		t.backend.readTx.tx = t.backend.begin(false)
	}
}

func (t *batchTxBufferedBoltDB) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.batchTxBoltDB.UnsafePut(bucketName, key, value)
	t.buf.put(bucketName, key, value)
}

func (t *batchTxBufferedBoltDB) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.batchTxBoltDB.UnsafeSeqPut(bucketName, key, value)
	t.buf.putSeq(bucketName, key, value)
}






type batchTxBufferedBadgerDB struct {
	batchTxBadgerDB
	buf txWriteBuffer
}

func newBatchTxBufferedBadgerDB(backend *badgerdbBackend) *batchTxBufferedBadgerDB {
	tx := &batchTxBufferedBadgerDB{
		batchTxBadgerDB: batchTxBadgerDB{backend: backend},
		buf: txWriteBuffer{
			txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			seq:      true,
		},
	}
	tx.Commit()
	return tx
}

func (t *batchTxBufferedBadgerDB) Unlock() {
	if t.pending != 0 {
		t.backend.readTx.Lock() // blocks txReadBuffer for writing.
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.Unlock()
		if t.pending >= t.backend.batchLimit {
			t.commit(false)
		}
	}
	t.batchTxBadgerDB.Unlock()
}

func (t *batchTxBufferedBadgerDB) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBufferedBadgerDB) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBufferedBadgerDB) commit(stop bool) {
	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.Unlock()
}

func (t *batchTxBufferedBadgerDB) unsafeCommit(stop bool) {
	if t.backend.readTx.tx != nil {
		// wait all store read transactions using the current boltdb tx to finish,
		// then close the boltdb tx
		go func(tx *badger.Txn, wg *sync.WaitGroup) {
			wg.Wait()
			tx.Discard()
		}(t.backend.readTx.tx, t.backend.readTx.txWg)
		t.backend.readTx.reset()
	}

	t.batchTxBadgerDB.commit(stop)

	if !stop {
		t.backend.readTx.tx = t.backend.begin(false)
	}
}

func (t *batchTxBufferedBadgerDB) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.batchTxBadgerDB.UnsafePut(bucketName, key, value)
	t.buf.put(bucketName, key, value)
}

func (t *batchTxBufferedBadgerDB) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.batchTxBadgerDB.UnsafeSeqPut(bucketName, key, value)
	t.buf.putSeq(bucketName, key, value)
}
