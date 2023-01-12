// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Author: Benqi (wubenqi@gmail.com)
//

package kv

import (
	"github.com/zeromicro/go-zero/core/stores/kv"
)

type (
	KvConf = kv.KvConf
)

var (
	ErrNoRedisNode = kv.ErrNoRedisNode
	NewStore       = kv.NewStore
)
