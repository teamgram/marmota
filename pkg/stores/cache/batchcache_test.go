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
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/redis/redistest"
	"github.com/zeromicro/go-zero/core/syncx"
)

var errTestNotFound = errors.New("not found")

func TestBatchNode_Takes(t *testing.T) {
	r := redistest.CreateRedis(t)
	bc := NewNode(r, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	// Pre-set 5 keys via cache.Set (stores JSON-serialized values)
	for i := 0; i < 5; i++ {
		assert.Nil(t, bc.Set(fmt.Sprintf("key:%d", i), i))
	}

	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("key:%d", i)
	}

	results := make(map[string]int)
	queryCalledWith := make([]string, 0)

	err := bc.Takes(
		func(keys ...string) (map[string]any, error) {
			queryCalledWith = append(queryCalledWith, keys...)
			m := make(map[string]any)
			for _, k := range keys {
				var n int
				fmt.Sscanf(k, "key:%d", &n)
				m[k] = n
			}
			return m, nil
		},
		func(k, v string) (any, error) {
			var n int
			if err := json.Unmarshal([]byte(v), &n); err != nil {
				return nil, err
			}
			results[k] = n
			return n, nil
		},
		keys...,
	)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(queryCalledWith))
	assert.Equal(t, 5, len(results))
}

func TestBatchNode_Takes_AllMiss(t *testing.T) {
	r := redistest.CreateRedis(t)
	bc := NewNode(r, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	keys := []string{"miss:1", "miss:2", "miss:3"}
	queryCount := 0

	err := bc.Takes(
		func(keys ...string) (map[string]any, error) {
			queryCount++
			m := make(map[string]any)
			for _, k := range keys {
				m[k] = k
			}
			return m, nil
		},
		func(k, v string) (any, error) {
			// cacheF is only called for cache hits, not during initial miss+query
			return nil, errors.New("should not be called for misses")
		},
		keys...,
	)
	assert.Nil(t, err)
	assert.Equal(t, 1, queryCount)

	// Second call: all keys should now be cached
	hitCount := 0
	err = bc.Takes(
		func(keys ...string) (map[string]any, error) {
			return nil, errors.New("should not query DB on second call")
		},
		func(k, v string) (any, error) {
			hitCount++
			return v, nil
		},
		keys...,
	)
	assert.Nil(t, err)
	assert.Equal(t, 3, hitCount)
}

func TestBatchNode_Takes_NotFoundPlaceholder(t *testing.T) {
	r := redistest.CreateRedis(t)
	bc := NewNode(r, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	// Query returns empty map -> keys not in result -> placeholders set
	err := bc.Takes(
		func(keys ...string) (map[string]any, error) {
			return map[string]any{}, nil
		},
		func(k, v string) (any, error) {
			return nil, errors.New("should not be called")
		},
		"notexist:1", "notexist:2",
	)
	assert.Nil(t, err)

	// Second call: placeholders should prevent DB query
	err = bc.Takes(
		func(keys ...string) (map[string]any, error) {
			return nil, errors.New("should not query DB again")
		},
		func(k, v string) (any, error) {
			return nil, errors.New("should not be called")
		},
		"notexist:1", "notexist:2",
	)
	assert.Nil(t, err)
}

func TestBatchNode_Takes_QueryError(t *testing.T) {
	r := redistest.CreateRedis(t)
	bc := NewNode(r, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	queryErr := errors.New("db connection failed")
	err := bc.Takes(
		func(keys ...string) (map[string]any, error) {
			return nil, queryErr
		},
		func(k, v string) (any, error) {
			return nil, nil
		},
		"err:1", "err:2",
	)
	assert.ErrorIs(t, err, queryErr)
}

func TestBatchNode_Takes_EmptyKeys(t *testing.T) {
	r := redistest.CreateRedis(t)
	bc := NewNode(r, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	err := bc.Takes(
		func(keys ...string) (map[string]any, error) {
			return nil, errors.New("should not be called")
		},
		func(k, v string) (any, error) {
			return nil, errors.New("should not be called")
		},
	)
	assert.Nil(t, err)
}

func TestBatchNode_TakesWithExpire(t *testing.T) {
	r := redistest.CreateRedis(t)
	bc := NewNode(r, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	var receivedExpire time.Duration
	err := bc.TakesWithExpire(
		func(expire time.Duration, keys ...string) (map[string]any, error) {
			receivedExpire = expire
			m := make(map[string]any)
			for _, k := range keys {
				m[k] = k
			}
			return m, nil
		},
		func(k, v string) (any, error) {
			return v, nil
		},
		"exp:1", "exp:2",
	)
	assert.Nil(t, err)
	assert.True(t, receivedExpire > 0)
}

func TestBatchNode_TakesWithExpireCtx(t *testing.T) {
	r := redistest.CreateRedis(t)
	bc := NewNode(r, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	ctx := context.Background()
	var receivedExpire time.Duration
	err := bc.TakesWithExpireCtx(ctx,
		func(expire time.Duration, keys ...string) (map[string]any, error) {
			receivedExpire = expire
			m := make(map[string]any)
			for _, k := range keys {
				m[k] = k
			}
			return m, nil
		},
		func(k, v string) (any, error) {
			return v, nil
		},
		"expctx:1",
	)
	assert.Nil(t, err)
	assert.True(t, receivedExpire > 0)
}

func TestBatchCluster_Takes(t *testing.T) {
	const total = 100
	r1 := redistest.CreateRedis(t)
	r2 := redistest.CreateRedis(t)

	conf := cache.ClusterConf{
		{
			RedisConf: redis.RedisConf{
				Host: r1.Addr,
				Type: redis.NodeType,
			},
			Weight: 100,
		},
		{
			RedisConf: redis.RedisConf{
				Host: r2.Addr,
				Type: redis.NodeType,
			},
			Weight: 100,
		},
	}

	bc := New(conf, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	// Pre-set half the keys
	for i := 0; i < total/2; i++ {
		assert.Nil(t, bc.Set(fmt.Sprintf("cluster:%d", i), i))
	}

	keys := make([]string, total)
	for i := 0; i < total; i++ {
		keys[i] = fmt.Sprintf("cluster:%d", i)
	}

	hitCount := 0
	err := bc.TakesCtx(context.Background(),
		func(keys ...string) (map[string]any, error) {
			m := make(map[string]any)
			for _, k := range keys {
				var n int
				fmt.Sscanf(k, "cluster:%d", &n)
				m[k] = n
			}
			return m, nil
		},
		func(k, v string) (any, error) {
			hitCount++
			return v, nil
		},
		keys...,
	)
	assert.Nil(t, err)
	assert.Equal(t, total/2, hitCount)
}

func TestBatchCluster_SingleNode(t *testing.T) {
	r := redistest.CreateRedis(t)
	conf := cache.ClusterConf{
		{
			RedisConf: redis.RedisConf{
				Host: r.Addr,
				Type: redis.NodeType,
			},
			Weight: 100,
		},
	}

	bc := New(conf, syncx.NewSingleFlight(), cache.NewStat("test"), errTestNotFound)

	err := bc.Takes(
		func(keys ...string) (map[string]any, error) {
			m := make(map[string]any)
			for _, k := range keys {
				m[k] = k
			}
			return m, nil
		},
		func(k, v string) (any, error) {
			return v, nil
		},
		"single:1", "single:2",
	)
	assert.Nil(t, err)
}
