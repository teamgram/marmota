// Copyright 2024 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package idempotent

import (
	"context"
	"fmt"
	"time"

	"github.com/zeromicro/go-zero/core/fx"
	"github.com/zeromicro/go-zero/core/jsonx"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

var (
	ErrorCacheEmpty = fmt.Errorf("cache empty")
)

type Idempotent struct {
	key       string
	store     *redis.Redis
	redisLock *redis.RedisLock
}

func NewIdempotent(store *redis.Redis, key string) *Idempotent {
	return &Idempotent{
		key:       "idempotent.1#" + key,
		store:     store,
		redisLock: redis.NewRedisLock(store, "idempotent-lock.1#"+key),
	}
}

func (i *Idempotent) TryGetCacheValue(ctx context.Context, v any) (cached bool, err error) {
	var (
		data string
	)

	data, err = i.store.GetCtx(ctx, i.key)
	if err != nil {
		return false, err
	} else if data == "" {
		return false, nil
	}

	return true, jsonx.UnmarshalFromString(data, v)
}

func (i *Idempotent) SetCacheValue(ctx context.Context, v any, expired int) error {
	data, err := jsonx.MarshalToString(v)
	if err != nil {
		return err
	}

	return i.store.SetexCtx(ctx, i.key, data, expired)
}

// Lock Lock
func (i *Idempotent) Lock(ctx context.Context, seconds int) (bool, error) {
	i.redisLock.SetExpire(seconds)
	return i.redisLock.AcquireCtx(ctx)
}

// Unlock Unlock
func (i *Idempotent) Unlock(ctx context.Context) error {
	_, err := i.redisLock.ReleaseCtx(ctx)
	return err
}

// waitCache keeps trying to read the cached value within the given number of seconds.
// It returns nil once the value is found in cache, or the last error (including ErrorCacheEmpty)
// if the retries are exhausted.
func waitCache(ctx context.Context, seconds int, getter func(context.Context, any) (bool, error), v any) error {
	return fx.DoWithRetryCtx(
		ctx,
		func(ctx context.Context, retryCount int) error {
			cached, err := getter(ctx, v)
			if err != nil {
				return err
			}
			if !cached {
				return ErrorCacheEmpty
			}
			return nil
		},
		fx.WithInterval(time.Second),
		fx.WithRetry(seconds+1),
	)
}

// DoIdempotent ensures that for a given key the user function fn is executed at most once
// across distributed workers, and subsequent callers reuse the cached result.
//
//   - First caller:
//     - Tries cache; if miss, acquires a distributed lock, executes fn, stores the result in cache,
//       and returns (false, nil).
//   - Concurrent callers:
//     - If cache already has the value, return (true, nil).
//     - If the lock is held by another worker, they wait up to `seconds` by polling the cache:
//       - If the value appears in cache during this window, return (true, nil).
//       - If it never appears, return an error (for example ErrorCacheEmpty or underlying Redis error).
//
// seconds is the lock TTL in seconds and should be greater than the worst-case execution time of fn.
// expired is the cache TTL in seconds.
func DoIdempotent(ctx context.Context, store *redis.Redis, key string, v any, seconds, expired int, fn func(ctx context.Context, v any) error) (bool, error) {
	idempotent := NewIdempotent(store, key)
	cached, err := idempotent.TryGetCacheValue(ctx, v)
	if err != nil {
		return cached, err
	} else if cached {
		// logx.Debugf("cached success - %v", v)
		return true, nil
	}

	ok, err := idempotent.Lock(ctx, seconds)
	if err != nil {
		return false, err
	} else {
		if ok {
			defer idempotent.Unlock(ctx)

			err = fn(ctx, v)
			if err != nil {
				return false, err
			}

			err = idempotent.SetCacheValue(ctx, v, expired)
			if err != nil {
				return false, err
			} else {
				// logx.Debugf("do success - %v", v)
				return false, nil
			}
		} else {
			err = waitCache(ctx, seconds, idempotent.TryGetCacheValue, v)
			if err != nil {
				return false, err
			} else {
				// logx.Debugf("retry success - %v", v)
				return true, nil
			}
		}
	}
}
