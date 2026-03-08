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
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zeromicro/go-zero/core/stores/kv"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/core/stores/redis/redistest"
)

func TestExtStore_ExpireWithResult(t *testing.T) {
	r := redistest.CreateRedis(t)
	conf := kv.KvConf{{
		RedisConf: redis.RedisConf{Host: r.Addr, Type: redis.NodeType},
		Weight:    100,
	}}
	s := NewStore(conf)

	assert.Nil(t, s.Set("mykey", "myval"))
	ok, err := s.ExpireWithResult("mykey", 60)
	assert.Nil(t, err)
	assert.True(t, ok)

	ok, err = s.ExpireWithResult("nonexist", 60)
	assert.Nil(t, err)
	assert.False(t, ok)
}

func TestExtStore_Mget(t *testing.T) {
	r1 := redistest.CreateRedis(t)
	r2 := redistest.CreateRedis(t)
	conf := kv.KvConf{
		{RedisConf: redis.RedisConf{Host: r1.Addr, Type: redis.NodeType}, Weight: 100},
		{RedisConf: redis.RedisConf{Host: r2.Addr, Type: redis.NodeType}, Weight: 100},
	}
	s := NewStore(conf)

	// Set some keys
	for i := 0; i < 10; i++ {
		assert.Nil(t, s.Set(fmt.Sprintf("k:%d", i), fmt.Sprintf("v:%d", i)))
	}

	// Mget all keys
	keys := make([]string, 10)
	for i := 0; i < 10; i++ {
		keys[i] = fmt.Sprintf("k:%d", i)
	}
	vals, err := s.Mget(keys...)
	assert.Nil(t, err)
	assert.Equal(t, 10, len(vals))
	for i, v := range vals {
		assert.Equal(t, fmt.Sprintf("v:%d", i), v)
	}
}

func TestExtStore_Mget_Empty(t *testing.T) {
	r := redistest.CreateRedis(t)
	conf := kv.KvConf{{
		RedisConf: redis.RedisConf{Host: r.Addr, Type: redis.NodeType},
		Weight:    100,
	}}
	s := NewStore(conf)
	vals, err := s.Mget()
	assert.Nil(t, err)
	assert.Nil(t, vals)
}

func TestExtStore_Mget_Single(t *testing.T) {
	r := redistest.CreateRedis(t)
	conf := kv.KvConf{{
		RedisConf: redis.RedisConf{Host: r.Addr, Type: redis.NodeType},
		Weight:    100,
	}}
	s := NewStore(conf)
	assert.Nil(t, s.Set("solo", "value"))
	vals, err := s.Mget("solo")
	assert.Nil(t, err)
	assert.Equal(t, []string{"value"}, vals)
}

func TestExtStore_GetPipeline(t *testing.T) {
	r := redistest.CreateRedis(t)
	conf := kv.KvConf{{
		RedisConf: redis.RedisConf{Host: r.Addr, Type: redis.NodeType},
		Weight:    100,
	}}
	s := NewStore(conf)

	pipe, err := s.GetPipeline("anykey")
	assert.Nil(t, err)
	assert.NotNil(t, pipe)
}
