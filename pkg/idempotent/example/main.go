// Copyright © 2024 Teamgram Authors. All Rights Reserved.
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

package main

import (
	"context"
	"github.com/teamgram/marmota/pkg/idempotent"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"time"
)

type CacheValue struct {
	Key   string
	Value string
}

func main() {
	store := redis.MustNewRedis(redis.RedisConf{
		Host:        "127.0.0.1:6379",
		Type:        "node",
		Pass:        "",
		Tls:         false,
		NonBlock:    true,
		PingTimeout: 16,
	})

	// T1(store)
	// T2(store)
	T3(store)
}

func T1(store *redis.Redis) {
	var (
		cData *CacheValue
		ctx   = context.Background()
	)

	idempotent := idempotent.NewIdempotent(store, "0123456789")
	_, err := idempotent.TryGetCacheValue(ctx, &cData)
	if err == nil {
		if cData == nil {
			//
		} else {
			logx.Debugf("cData: %v", cData)
			return
		}
	} else {
		logx.Errorf("TryGetCacheValue error: %v", err)
		return
	}

	ok, err := idempotent.Lock(ctx, 5)
	if err != nil {
		logx.Errorf("Lock error: %v", err)
		return
	} else if ok {
		idempotent.SetCacheValue(
			ctx,
			&CacheValue{
				Key:   "k",
				Value: "v",
			},
			5)
		idempotent.Unlock(ctx)
	} else {
		logx.Errorf("Lock failed")
		return
	}

	_, err = idempotent.TryGetCacheValue(ctx, &cData)
	if err == nil {
		if cData == nil {
			//
		} else {
			logx.Debugf("cData: %v", cData)
			return
		}
	} else {
		logx.Errorf("TryGetCacheValue error: %v", err)
		return
	}
}

func T2(store *redis.Redis) {
	ctx := context.Background()
	firstLock := idempotent.NewIdempotent(store, "0123456789")
	ok, err := firstLock.Lock(ctx, 10)
	_, _ = ok, err
	secondLock := idempotent.NewIdempotent(store, "0123456789")
	ok, err = secondLock.Lock(ctx, 10)
	err = firstLock.Unlock(ctx)
	ok, err = secondLock.Lock(ctx, 10)
}

func T3(store *redis.Redis) {
	logx.Infof("start")
	go func() {
		var (
			cData *CacheValue
			ctx   = context.Background()
		)
		cached, err := idempotent.DoIdempotent(
			ctx,
			store,
			"0123456789",
			&cData,
			10,
			30,
			func(ctx context.Context, v any) error {
				logx.Infof("ready to sleep 5s")
				//time.Sleep(1 * time.Second)
				logx.Infof("wake up")
				*v.(**CacheValue) = &CacheValue{
					Key:   "k",
					Value: "v",
				}

				return nil
			})
		logx.Infof("cached: %v, v: %v, err: %v", cached, cData, err)
	}()

	var (
		cData *CacheValue
		ctx   = context.Background()
	)
	//time.Sleep(100 * time.Millisecond)
	logx.Infof("start again")
	cached, err := idempotent.DoIdempotent(
		ctx,
		store,
		"0123456789",
		&cData,
		10,
		30,
		func(ctx context.Context, v any) error {
			*v.(**CacheValue) = &CacheValue{
				Key:   "k",
				Value: "v",
			}

			return nil
		})
	logx.Infof("cached: %v, v: %v, err: %v", cached, cData, err)

	var (
		cData2 *CacheValue
	)

	cached2, err2 := idempotent.DoIdempotent(
		ctx,
		store,
		"0123456789",
		&cData2,
		10,
		30,
		func(ctx context.Context, v any) error {
			*v.(**CacheValue) = &CacheValue{
				Key:   "k",
				Value: "v",
			}

			return nil
		})
	logx.Infof("cached: %v, v: %v, err: %v", cached2, cData2, err2)
	time.Sleep(1 * time.Second)
}
