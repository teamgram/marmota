// Copyright (c) 2026 The Teamgram Authors (https://teamgram.net).
//  All rights reserved.
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

package cache

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/zeromicro/go-zero/core/errorx"
	"github.com/zeromicro/go-zero/core/hash"
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/syncx"
)

type batchCluster struct {
	cache.Cache
	dispatcher  *hash.ConsistentHash
	errNotFound error
}

// New returns a BatchCache.
func New(c cache.ClusterConf, barrier syncx.SingleFlight, st *cache.Stat,
	errNotFound error, opts ...cache.Option) BatchCache {
	if len(c) == 0 || cache.TotalWeights(c) <= 0 {
		log.Fatal("no cache nodes")
	}

	if len(c) == 1 {
		return NewNode(redis.MustNewRedis(c[0].RedisConf), barrier, st, errNotFound, opts...)
	}

	innerCache := cache.New(c, barrier, st, errNotFound, opts...)

	dispatcher := hash.NewConsistentHash()
	for _, node := range c {
		rds := redis.MustNewRedis(node.RedisConf)
		bn := newBatchNodeForCluster(rds, st, errNotFound, opts...)
		dispatcher.AddWithWeight(bn, node.Weight)
	}

	return &batchCluster{
		Cache:       innerCache,
		dispatcher:  dispatcher,
		errNotFound: errNotFound,
	}
}

func (bc *batchCluster) Takes(query func(keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error), keys ...string) error {
	return bc.TakesCtx(context.Background(), query, cacheF, keys...)
}

func (bc *batchCluster) TakesCtx(ctx context.Context,
	query func(keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error), keys ...string) error {
	var be errorx.BatchError
	nodes := make(map[any][]string)

	for _, key := range keys {
		c, ok := bc.dispatcher.Get(key)
		if !ok {
			be.Add(fmt.Errorf("key %q not found", key))
			continue
		}
		nodes[c] = append(nodes[c], key)
	}

	for c, ks := range nodes {
		if err := c.(*batchNode).TakesCtx(ctx, query, cacheF, ks...); err != nil {
			be.Add(err)
		}
	}

	return be.Err()
}

func (bc *batchCluster) TakesWithExpire(
	query func(expire time.Duration, keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error), keys ...string) error {
	return bc.TakesWithExpireCtx(context.Background(), query, cacheF, keys...)
}

func (bc *batchCluster) TakesWithExpireCtx(ctx context.Context,
	query func(expire time.Duration, keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error), keys ...string) error {
	var be errorx.BatchError
	nodes := make(map[any][]string)

	for _, key := range keys {
		c, ok := bc.dispatcher.Get(key)
		if !ok {
			be.Add(fmt.Errorf("key %q not found", key))
			continue
		}
		nodes[c] = append(nodes[c], key)
	}

	for c, ks := range nodes {
		if err := c.(*batchNode).TakesWithExpireCtx(ctx, query, cacheF, ks...); err != nil {
			be.Add(err)
		}
	}

	return be.Err()
}
