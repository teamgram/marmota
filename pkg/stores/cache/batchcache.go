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
	"time"

	"github.com/zeromicro/go-zero/core/stores/cache"
)

const (
	notFoundPlaceholder = "*"
	expiryDeviation     = 0.05
)

type (
	CacheConf = cache.CacheConf
	Option    = cache.Option
)

var (
	NewStat = cache.NewStat
)

// BatchCache extends cache.Cache with batch operations.
type BatchCache interface {
	cache.Cache

	// Takes takes results from cache first, if not found,
	// query from DB and set cache using c.expiry, then return the result.
	Takes(query func(keys ...string) (map[string]any, error),
		cacheF func(k, v string) (any, error), keys ...string) error
	// TakesCtx takes results from cache first, if not found,
	// query from DB and set cache using c.expiry, then return the result.
	TakesCtx(ctx context.Context, query func(keys ...string) (map[string]any, error),
		cacheF func(k, v string) (any, error), keys ...string) error
	// TakesWithExpire takes results from cache first, if not found,
	// query from DB and set cache using given expire, then return the result.
	TakesWithExpire(query func(expire time.Duration, keys ...string) (map[string]any, error),
		cacheF func(k, v string) (any, error), keys ...string) error
	// TakesWithExpireCtx takes results from cache first, if not found,
	// query from DB and set cache using given expire, then return the result.
	TakesWithExpireCtx(ctx context.Context, query func(expire time.Duration, keys ...string) (map[string]any, error),
		cacheF func(k, v string) (any, error), keys ...string) error
}
