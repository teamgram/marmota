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
	"time"

	"github.com/teamgram/marmota/pkg/idempotent"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stores/redis"
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
	err := idempotent.TryGetCacheValue(ctx, &cData)
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

	err = idempotent.TryGetCacheValue(ctx, &cData)
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
		v, err := idempotent.DoIdempotent(
			context.Background(),
			store,
			"0123456789",
			10,
			30,
			func() (interface{}, error) {
				logx.Infof("ready to sleep 5s")
				time.Sleep(1 * time.Second)
				logx.Infof("wake up")
				return &CacheValue{
					Key:   "k",
					Value: "v",
				}, nil
			})
		logx.Infof("v: %v, err: %v", v, err)
	}()

	time.Sleep(100 * time.Millisecond)
	logx.Infof("start again")
	v, err := idempotent.DoIdempotent(
		context.Background(),
		store,
		"0123456789",
		10,
		30,
		func() (interface{}, error) {
			return &CacheValue{
				Key:   "k",
				Value: "v",
			}, nil
		})
	logx.Infof("v: %v, err: %v", v, err)
}
