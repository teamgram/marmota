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
	ErrorDoEmpty    = fmt.Errorf("do empty")
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
			err = fx.DoWithRetryCtx(
				ctx,
				func(ctx context.Context, retryCount int) error {
					if retryCount == 0 {
						return ErrorDoEmpty
					}
					cached, err = idempotent.TryGetCacheValue(ctx, v)
					if err != nil {
						return err
					}
					if v == nil {
						return ErrorCacheEmpty
					} else {
						return nil
					}
				},
				fx.WithInterval(time.Second),
				fx.WithRetry(seconds+1))
			if err != nil {
				return false, err
			} else {
				// logx.Debugf("retry success - %v", v)
				return true, nil
			}
		}
	}
}
