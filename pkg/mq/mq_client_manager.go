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

package kafka

import (
	"github.com/zeromicro/go-zero/core/syncx"
	"io"
)

var (
	clientManager = syncx.NewResourceManager()
)

type client struct {
	*Producer
}

//func (c *client) Close() error {
//	return nil
//}

func GetCachedMQClient(c *KafkaProducerConf) *Producer {
	var (
		val io.Closer
		err error
	)
	val, err = clientManager.GetResource(c.Topic, func() (io.Closer, error) {
		cli := MustKafkaProducer(c)
		return cli, nil
	})
	if err != nil {
		panic(err)
	}

	return val.(*Producer)
}
