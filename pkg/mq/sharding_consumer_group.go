// Copyright 2023 Teamgram Authors
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
	"container/list"
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

func MustShardingConsumerGroup(c *KafkaShardingConsumerConf) *ShardingConsumerGroup {
	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Fetch.Default = 8 * 1024 * 1024
	config.Consumer.Return.Errors = false
	config.ClientID = c.ClientId
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	consumerGroup, err := sarama.NewConsumerGroup(c.Brokers, c.Group, config)
	if err != nil {
		panic(err)
	}
	cg := &ShardingConsumerGroup{
		ConsumerGroup: consumerGroup,
		c:             c,
		chMessage:     make([]chan *CMessage, c.Concurrency),
	}
	for i := 0; i < len(cg.chMessage); i++ {
		cg.chMessage[i] = make(chan *CMessage, c.QueueBuffer)
		go cg.Run(i)
	}
	//for i := 0; i < ChannelNum; i++ {
	//	cg.chArrays[i] = make(chan Cmd2Value, 50)
	//	go cg.Run(i)
	//}

	return cg
}

type CMessage struct {
	Message     *sarama.ConsumerMessage
	MarkMessage func()
}

// ShardingConsumerGroup represents a Sarama consumer GroupName consumer
type ShardingConsumerGroup struct {
	sarama.ConsumerGroup
	c         *KafkaShardingConsumerConf
	chMessage []chan *CMessage
	cb        MessageHandlerF
}

func (c *ShardingConsumerGroup) RegisterHandler(cb MessageHandlerF) {
	c.cb = cb
}

// Start start consume messages, watch signals
func (c *ShardingConsumerGroup) Start() {
	ctx := context.Background()
	for {
		err := c.ConsumerGroup.Consume(ctx, c.Topics(), c)
		if err != nil {
			logx.Error(err)
			return
		}
	}
}

func (c *ShardingConsumerGroup) Run(channelID int) {
	for {
		select {
		case message := <-c.chMessage[channelID]:
			if len(message.Message.Value) != 0 {
				c.cb(context.Background(), string(message.Message.Key), message.Message.Value)
			}
			message.MarkMessage()
		}
	}
}

// Stop Stop consume messages, watch signals
func (c *ShardingConsumerGroup) Stop() {
}

func (c *ShardingConsumerGroup) Topics() []string {
	return c.c.Topics
}

func (c *ShardingConsumerGroup) Group() string {
	return c.c.Group
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *ShardingConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	// close(c.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *ShardingConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ShardingConsumerGroup) Sharding(message *sarama.ConsumerMessage) int {
	hashFunc := fnv.New32a()
	_, _ = hashFunc.Write(message.Key)
	return int(hashFunc.Sum32()) % c.c.Concurrency
}

type None struct{}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *ShardingConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) (err error) {
	for {
		if session == nil {
			logx.Info(" sess == nil, waiting ")
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}

	waitCommitQueue := list.New()
	waitCommitMap := make(map[int64]None, 100000)
	var mutex sync.Mutex

	for msg := range claim.Messages() {
		mutex.Lock()
		waitCommitQueue.PushBack(msg)
		mutex.Unlock()
		c.chMessage[c.Sharding(msg)] <- &CMessage{
			Message: msg,
			MarkMessage: func() {
				mutex.Lock()
				defer mutex.Unlock()

				if waitCommitQueue.Front().Value.(*sarama.ConsumerMessage).Offset == msg.Offset {
					waitCommitQueue.Remove(waitCommitQueue.Front())
					session.MarkMessage(msg, "")
					for waitCommitQueue.Len() > 0 {
						item := waitCommitQueue.Front()
						offset := item.Value.(*sarama.ConsumerMessage).Offset

						if _, ok := waitCommitMap[offset]; !ok {
							break
						}
						delete(waitCommitMap, offset)
						session.MarkMessage(item.Value.(*sarama.ConsumerMessage), "")
						waitCommitQueue.Remove(item)
					}
				} else {
					waitCommitMap[msg.Offset] = None{}
				}
			},
		}
	}

	return nil
}
