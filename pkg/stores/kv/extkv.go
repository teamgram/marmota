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

package kv

import (
	"context"
	"fmt"
	"log"

	extredis "github.com/teamgram/marmota/pkg/stores/redis"

	"github.com/zeromicro/go-zero/core/errorx"
	"github.com/zeromicro/go-zero/core/hash"
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/kv"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type (
	// Pipeliner is an alias of redis.Pipeliner.
	Pipeliner = redis.Pipeliner

	// Pipeline provides access to redis pipelining on a specific node.
	Pipeline interface {
		Pipelined(fn func(Pipeliner) error) error
		PipelinedCtx(ctx context.Context, fn func(Pipeliner) error) error
	}

	// ExtStore extends kv.Store with additional operations.
	ExtStore interface {
		kv.Store

		ExpireWithResult(key string, seconds int) (bool, error)
		ExpireWithResultCtx(ctx context.Context, key string, seconds int) (bool, error)
		Mget(keys ...string) ([]string, error)
		MgetCtx(ctx context.Context, keys ...string) ([]string, error)
		GetPipeline(key string) (Pipeline, error)
	}

	extClusterStore struct {
		kv.Store
		dispatcher *hash.ConsistentHash
	}
)

// NewStore returns an ExtStore backed by consistent hash dispatching.
func NewStore(c kv.KvConf) ExtStore {
	if len(c) == 0 || cache.TotalWeights(c) <= 0 {
		log.Fatal("no kv nodes")
	}

	inner := kv.NewStore(c)
	dispatcher := hash.NewConsistentHash()
	for _, node := range c {
		rds := redis.MustNewRedis(node.RedisConf)
		dispatcher.AddWithWeight(rds, node.Weight)
	}

	return &extClusterStore{
		Store:      inner,
		dispatcher: dispatcher,
	}
}

// ExpireWithResult calls EXPIRE and returns whether the timeout was set.
func (s *extClusterStore) ExpireWithResult(key string, seconds int) (bool, error) {
	return s.ExpireWithResultCtx(context.Background(), key, seconds)
}

// ExpireWithResultCtx calls EXPIRE and returns whether the timeout was set.
func (s *extClusterStore) ExpireWithResultCtx(ctx context.Context, key string, seconds int) (bool, error) {
	node, err := s.getRedis(key)
	if err != nil {
		return false, err
	}

	return extredis.ExpireWithResultCtx(ctx, node, key, seconds)
}

// Mget retrieves multiple keys, preserving original key order.
func (s *extClusterStore) Mget(keys ...string) ([]string, error) {
	return s.MgetCtx(context.Background(), keys...)
}

// MgetCtx retrieves multiple keys, preserving original key order.
// Keys are grouped by their consistent-hash node for efficiency.
func (s *extClusterStore) MgetCtx(ctx context.Context, keys ...string) ([]string, error) {
	switch len(keys) {
	case 0:
		return nil, nil
	case 1:
		node, err := s.getRedis(keys[0])
		if err != nil {
			return nil, err
		}

		val, err := node.GetCtx(ctx, keys[0])
		if err != nil {
			return nil, err
		}

		return []string{val}, nil
	default:
		var be errorx.BatchError

		nodes := make(map[any][]string)
		idxList := make(map[any][]int)
		result := make([]string, len(keys))

		for i, key := range keys {
			c, ok := s.dispatcher.Get(key)
			if !ok {
				be.Add(fmt.Errorf("key %q not found", key))
				continue
			}
			nodes[c] = append(nodes[c], key)
			idxList[c] = append(idxList[c], i)
		}

		for c, ks := range nodes {
			rds := c.(*redis.Redis)
			switch rds.Type {
			case redis.ClusterType:
				// Use pipeline for Redis Cluster since MGET may span slots.
				cmds := make([]*redis.StringCmd, len(ks))
				err := rds.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
					for i, k := range ks {
						cmds[i] = pipe.Get(ctx, k)
					}
					return nil
				})
				if err != nil && err != redis.Nil {
					be.Add(err)
				} else {
					for i, cmd := range cmds {
						val, err := cmd.Result()
						if err != nil && err != redis.Nil {
							be.Add(err)
						} else {
							result[idxList[c][i]] = val
						}
					}
				}
			default:
				// Use MGET for single-node redis.
				vals, err := rds.MgetCtx(ctx, ks...)
				if err != nil {
					be.Add(err)
				} else {
					for i := range ks {
						result[idxList[c][i]] = vals[i]
					}
				}
			}
		}

		return result, be.Err()
	}
}

// GetPipeline returns a Pipeline for the node that owns the given key.
func (s *extClusterStore) GetPipeline(key string) (Pipeline, error) {
	node, err := s.getRedis(key)
	if err != nil {
		return nil, err
	}

	return node, nil
}

func (s *extClusterStore) getRedis(key string) (*redis.Redis, error) {
	val, ok := s.dispatcher.Get(key)
	if !ok {
		return nil, kv.ErrNoRedisNode
	}

	return val.(*redis.Redis), nil
}
