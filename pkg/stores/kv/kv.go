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
	"github.com/zeromicro/go-zero/core/stores/kv"
	"github.com/zeromicro/go-zero/core/stores/redis"

	red "github.com/redis/go-redis/v9"
)

type (
	KvConf = kv.KvConf
	//Pipeline  = red.Pipeline
	//Pipeliner = red.Pipeliner
	Store = kv.Store

	// IntCmd is an alias of redis.IntCmd.
	IntCmd = redis.IntCmd
	// FloatCmd is an alias of redis.FloatCmd.
	FloatCmd = redis.FloatCmd
	// StringCmd is an alias of redis.StringCmd.
	StringCmd = redis.StringCmd
	// MapStringStringCmd is an alias of redis.MapStringStringCmd.
	MapStringStringCmd = red.MapStringStringCmd
)

var (
	ErrNoRedisNode = kv.ErrNoRedisNode
	// NewStore       = kv.NewStore
)
