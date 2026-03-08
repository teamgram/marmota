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

package redis

import (
	"context"
	"time"

	red "github.com/redis/go-redis/v9"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

// ExpireWithResult is like redis.Expire but returns the boolean result
// indicating whether the timeout was actually set.
func ExpireWithResult(rds *redis.Redis, key string, seconds int) (bool, error) {
	return ExpireWithResultCtx(context.Background(), rds, key, seconds)
}

// ExpireWithResultCtx is like redis.ExpireCtx but returns the boolean result
// indicating whether the timeout was actually set.
func ExpireWithResultCtx(ctx context.Context, rds *redis.Redis, key string, seconds int) (bool, error) {
	var cmd *red.BoolCmd
	err := rds.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
		cmd = pipe.Expire(ctx, key, time.Duration(seconds)*time.Second)
		return nil
	})
	if err != nil {
		return false, err
	}

	return cmd.Val(), nil
}

// MsetStrings sets multiple key-value pairs where values are strings.
func MsetStrings(rds *redis.Redis, fieldsAndValues map[string]string) error {
	return MsetStringsCtx(context.Background(), rds, fieldsAndValues)
}

// MsetStringsCtx sets multiple key-value pairs where values are strings.
func MsetStringsCtx(ctx context.Context, rds *redis.Redis, fieldsAndValues map[string]string) error {
	args := make([]any, 0, len(fieldsAndValues)*2)
	for k, v := range fieldsAndValues {
		args = append(args, k, v)
	}

	_, err := rds.MsetCtx(ctx, args...)
	return err
}

// Msetnx sets multiple key-value pairs only if none of the keys exist.
// Returns true if all keys were set, false if no key was set.
func Msetnx(rds *redis.Redis, fieldsAndValues map[string]string) (bool, error) {
	return MsetnxCtx(context.Background(), rds, fieldsAndValues)
}

// MsetnxCtx sets multiple key-value pairs only if none of the keys exist.
// Returns true if all keys were set, false if no key was set.
func MsetnxCtx(ctx context.Context, rds *redis.Redis, fieldsAndValues map[string]string) (bool, error) {
	vals := make([]any, 0, len(fieldsAndValues)*2)
	for k, v := range fieldsAndValues {
		vals = append(vals, k, v)
	}

	var cmd *red.BoolCmd
	err := rds.PipelinedCtx(ctx, func(pipe redis.Pipeliner) error {
		cmd = pipe.MSetNX(ctx, vals...)
		return nil
	})
	if err != nil {
		return false, err
	}

	return cmd.Val(), nil
}
