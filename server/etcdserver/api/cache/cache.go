// Copyright 2021 The etcd Authors
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

// Package cache provides a cache layer for InternalRaftRequest.
package cache

import (
	pb "go.etcd.io/etcd/api/v3/etcdserverpb"
	"sync"
	"time"
)

const (
	CachedDataLenMin = 4096

	batchCount = 10000
	sleepDuration = time.Millisecond * 100
	bucketCount = 32
)

type cacheBucket struct {
	mu    sync.RWMutex
	cache map[uint64]*pb.InternalRaftRequest
	queue []uint64
}

func (b *cacheBucket) run() {
	t := time.NewTimer(sleepDuration)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		}
		if len(b.queue) > 50000 {
			count := len(b.queue) - batchCount
			for i := 0; i < count; i++ {
				id := b.queue[0]
				b.mu.Lock()
				b.queue = b.queue[1:]
				delete(b.cache, id)
				b.mu.Unlock()
			}
		}
		t.Reset(sleepDuration)
	}
}

type InternalRaftRequestCache struct {
	bucket [bucketCount]*cacheBucket
}

func NewInternalRaftRequestCache() *InternalRaftRequestCache {
	res := &InternalRaftRequestCache{
		bucket: [32]*cacheBucket{},
	}
	for i := 0; i < bucketCount; i++ {
		res.bucket[i] = &cacheBucket{
			mu:    sync.RWMutex{},
			cache: map[uint64]*pb.InternalRaftRequest{},
			queue: []uint64{},
		}
		go res.bucket[i].run()
	}
	return res
}


func (c *InternalRaftRequestCache) Put(id uint64, r *pb.InternalRaftRequest) {
	if id == 0 {
		return
	}
	c.bucket[id % bucketCount].mu.Lock()
	defer c.bucket[id % bucketCount].mu.Unlock()
	if _, ok := c.bucket[id % bucketCount].cache[id]; ok {
		panic("invalid id")
	}
	c.bucket[id % bucketCount].cache[id] = r
	c.bucket[id % bucketCount].queue = append(c.bucket[id % bucketCount].queue, id)
}

func (c *InternalRaftRequestCache) Get(id uint64) *pb.InternalRaftRequest{
	if id == 0 {
		return nil
	}
	c.bucket[id % bucketCount].mu.RLock()
	defer c.bucket[id % bucketCount].mu.RUnlock()
	if r, ok := c.bucket[id % bucketCount].cache[id]; ok {
		return r
	}
	return nil
}
