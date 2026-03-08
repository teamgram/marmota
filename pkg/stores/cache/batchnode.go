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
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/zeromicro/go-zero/core/jsonx"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/mathx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/stores/cache"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/syncx"
)

var errPlaceholder = errors.New("placeholder")

type batchNode struct {
	cache.Cache
	rds            *redis.Redis
	expiry         time.Duration
	notFoundExpiry time.Duration
	unstableExpiry mathx.Unstable
	stat           *cache.Stat
	errNotFound    error
}

func newOptions(opts ...cache.Option) cache.Options {
	var o cache.Options
	for _, opt := range opts {
		opt(&o)
	}
	if o.Expiry <= 0 {
		o.Expiry = 7 * 24 * time.Hour
	}
	if o.NotFoundExpiry <= 0 {
		o.NotFoundExpiry = time.Minute
	}
	return o
}

// NewNode returns a BatchCache backed by a single redis node.
func NewNode(rds *redis.Redis, barrier syncx.SingleFlight, st *cache.Stat,
	errNotFound error, opts ...cache.Option) BatchCache {
	inner := cache.NewNode(rds, barrier, st, errNotFound, opts...)
	o := newOptions(opts...)
	return &batchNode{
		Cache:          inner,
		rds:            rds,
		expiry:         o.Expiry,
		notFoundExpiry: o.NotFoundExpiry,
		unstableExpiry: mathx.NewUnstable(expiryDeviation),
		stat:           st,
		errNotFound:    errNotFound,
	}
}

// newBatchNodeForCluster creates a batchNode without an inner cache.Cache.
// Used by batchCluster where single-key ops are handled by the cluster's embedded Cache.
func newBatchNodeForCluster(rds *redis.Redis, st *cache.Stat,
	errNotFound error, opts ...cache.Option) *batchNode {
	o := newOptions(opts...)
	return &batchNode{
		rds:            rds,
		expiry:         o.Expiry,
		notFoundExpiry: o.NotFoundExpiry,
		unstableExpiry: mathx.NewUnstable(expiryDeviation),
		stat:           st,
		errNotFound:    errNotFound,
	}
}

// String returns the redis address for consistent hash compatibility.
func (bn *batchNode) String() string {
	return bn.rds.Addr
}

func (bn *batchNode) Takes(query func(keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error), keys ...string) error {
	return bn.TakesCtx(context.Background(), query, cacheF, keys...)
}

func (bn *batchNode) TakesCtx(ctx context.Context,
	query func(keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error), keys ...string) error {
	return bn.doTakes(ctx, query, cacheF, bn.expiry, keys...)
}

func (bn *batchNode) TakesWithExpire(
	query func(expire time.Duration, keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error), keys ...string) error {
	return bn.TakesWithExpireCtx(context.Background(), query, cacheF, keys...)
}

func (bn *batchNode) TakesWithExpireCtx(ctx context.Context,
	query func(expire time.Duration, keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error), keys ...string) error {
	expire := bn.aroundDuration(bn.expiry)
	return bn.doTakes(ctx, func(keys ...string) (map[string]any, error) {
		return query(expire, keys...)
	}, cacheF, expire, keys...)
}

func (bn *batchNode) doTakes(ctx context.Context,
	query func(keys ...string) (map[string]any, error),
	cacheF func(k, v string) (any, error),
	expire time.Duration,
	keys ...string) error {
	if len(keys) == 0 {
		return nil
	}

	logger := logx.WithContext(ctx)
	cmds := make([]*redis.StringCmd, len(keys))

	err := bn.rds.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
		for i, key := range keys {
			cmds[i] = pipe.Get(ctx, key)
		}
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		logger.Error(err)
		return err
	}

	qKeys := make([]string, 0, len(keys))
	for i, cmd := range cmds {
		if err := bn.doGetCache(ctx, keys[i], cmd, cacheF); err != nil {
			if errors.Is(err, errPlaceholder) {
				continue
			}
			if !errors.Is(err, bn.errNotFound) {
				continue
			}
			qKeys = append(qKeys, keys[i])
		}
	}

	if len(qKeys) == 0 {
		return nil
	}

	values, err := query(qKeys...)
	if err != nil {
		bn.stat.IncrementDbFails()
		return err
	}

	if err := bn.rds.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
		seconds := int(math.Ceil(bn.aroundDuration(expire).Seconds()))
		notFoundSeconds := int(math.Ceil(bn.aroundDuration(bn.notFoundExpiry).Seconds()))

		for _, key := range qKeys {
			v, ok := values[key]
			if !ok || v == nil {
				pipe.SetEx(ctx, key, notFoundPlaceholder, time.Duration(notFoundSeconds)*time.Second)
				continue
			}

			data, err := jsonx.MarshalToString(v)
			if err != nil {
				logger.Error(err)
				continue
			}
			pipe.SetEx(ctx, key, data, time.Duration(seconds)*time.Second)
		}
		return nil
	}); err != nil {
		logger.Error(err)
	}

	return nil
}

func (bn *batchNode) doGetCache(ctx context.Context, key string,
	cmd *redis.StringCmd, cacheF func(k, v string) (any, error)) error {
	bn.stat.IncrementTotal()
	data, err := cmd.Result()
	if errors.Is(err, redis.Nil) {
		err = nil
	}
	if err != nil {
		bn.stat.IncrementMiss()
		return err
	}

	if len(data) == 0 {
		bn.stat.IncrementMiss()
		return bn.errNotFound
	}

	bn.stat.IncrementHit()
	if data == notFoundPlaceholder {
		return errPlaceholder
	}

	return bn.processCache(ctx, key, data, cacheF)
}

func (bn *batchNode) processCache(ctx context.Context, key, data string,
	cacheF func(k, v string) (any, error)) error {
	_, err := cacheF(key, data)
	if err == nil {
		return nil
	}

	report := fmt.Sprintf("unmarshal cache, node: %s, key: %s, value: %s, error: %v",
		bn.rds.Addr, key, data, err)
	logger := logx.WithContext(ctx)
	logger.Error(report)
	stat.Report(report)
	if _, e := bn.rds.DelCtx(ctx, key); e != nil {
		logger.Errorf("delete invalid cache, node: %s, key: %s, value: %s, error: %v",
			bn.rds.Addr, key, data, e)
	}

	return bn.errNotFound
}

func (bn *batchNode) aroundDuration(duration time.Duration) time.Duration {
	return bn.unstableExpiry.AroundDuration(duration)
}
