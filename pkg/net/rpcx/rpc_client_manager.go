// Copyright 2022 Teamgram Authors
//  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: teamgramio (teamgram.io@gmail.com)
//

package rpcx

import (
	"github.com/zeromicro/go-zero/core/logx"
	"io"
	"strings"

	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/zrpc"
)

var (
	clientManager = syncx.NewResourceManager()
)

type client struct {
	zrpc.Client
}

func (c *client) Close() error {
	return nil
}

func GetCachedRpcClient(c zrpc.RpcClientConf) zrpc.Client {
	var (
		val io.Closer
		err error
	)
	if c.Etcd.Key == "" {
		panic(c)
	}
	logx.Infof("client: %v", c)
	if len(c.Endpoints) > 0 {
		val, err = clientManager.GetResource(strings.Join(c.Endpoints, "/"), func() (io.Closer, error) {
			cli := zrpc.MustNewClient(c)
			return &client{
				Client: cli,
			}, nil
		})
		if err != nil {
			panic(err)
		}
	} else {
		val, err = clientManager.GetResource(c.Etcd.Key, func() (io.Closer, error) {
			cli := zrpc.MustNewClient(c)
			return &client{
				Client: cli,
			}, nil
		})
		if err != nil {
			panic(err)
		}
	}

	return val.(zrpc.Client)
}
