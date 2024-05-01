// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package kv

import (
	"github.com/zeromicro/go-zero/core/stores/kv"
	"github.com/zeromicro/go-zero/core/stores/redis"
)

type (
	KvConf    = kv.KvConf
	Pipeline  = kv.Pipeline
	Pipeliner = kv.Pipeliner
	Store     = kv.Store

	// IntCmd is an alias of redis.IntCmd.
	IntCmd = redis.IntCmd
	// FloatCmd is an alias of redis.FloatCmd.
	FloatCmd = redis.FloatCmd
	// StringCmd is an alias of redis.StringCmd.
	StringCmd = redis.StringCmd
	// MapStringStringCmd is an alias of redis.MapStringStringCmd.
	MapStringStringCmd = redis.MapStringStringCmd
)

var (
	ErrNoRedisNode = kv.ErrNoRedisNode
	NewStore       = kv.NewStore
)
