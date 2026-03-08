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

package main

import (
	"context"
	"fmt"

	"github.com/teamgram/marmota/pkg/stores/kv"

	"github.com/zeromicro/go-zero/core/conf"
)

type Config struct {
	KV kv.KvConf
}

func main() {
	var c Config
	conf.MustLoad("./kv.yaml", &c)

	dIdList := []string{
		"tm_1572598612301451270",
		"tm_1572599736324591618",
	}

	kv2 := kv.NewStore(c.KV)

	// MgetCtx
	vals, _ := kv2.MgetCtx(context.Background(), dIdList...)
	for _, v := range vals {
		fmt.Println(v)
	}

	// pipeline
	pipes := make(map[interface{}][]string)
	for _, dId := range dIdList {
		pipe, _ := kv2.GetPipeline(dId)
		pipes[pipe] = append(pipes[pipe], dId)
	}

	for pipe, idList := range pipes {
		var (
			cmds = make([]*kv.StringCmd, len(idList))
		)

		pipe.(kv.Pipeline).PipelinedCtx(context.Background(),
			func(pipe kv.Pipeliner) error {
				for i, id := range idList {
					cmds[i] = pipe.Get(context.Background(), id)
				}
				return nil
			})

		for _, cmd := range cmds {
			fmt.Println(cmd.Result())
		}
	}
}
