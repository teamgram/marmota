// Copyright © 2024 Teamgram open source community. All rights reserved.
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

package kafka

import (
	"container/list"
	"context"
	"hash/fnv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

func MustShardingConsumerGroup(c *KafkaShardingConsumerConf) *ShardingConsumerGroup {
	config, err := BuildConsumerGroupConfig(&c.KafkaConsumerConf, sarama.OffsetNewest, true)
	if err != nil {
		panic(err)
	}
	consumerGroup, err := NewConsumerGroup(config, c.Brokers, c.Group)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cg := &ShardingConsumerGroup{
		ConsumerGroup: consumerGroup,
		c:             c,
		chMessage:     make([]chan *CMessage, c.Concurrency),
		state:         newConnState(),
		ctx:           ctx,
		cancel:        cancel,
	}
	for i := 0; i < len(cg.chMessage); i++ {
		cg.chMessage[i] = make(chan *CMessage, c.QueueBuffer)
		go cg.Run(i)
	}

	return cg
}

type CMessage struct {
	Message     *sarama.ConsumerMessage
	MarkMessage func()
}

// ShardingConsumerGroup represents a Sarama consumer GroupName consumer
type ShardingConsumerGroup struct {
	sarama.ConsumerGroup
	c          *KafkaShardingConsumerConf
	chMessage  []chan *CMessage
	cb         MessageHandlerF
	state      *ConnState
	ctx        context.Context
	cancel     context.CancelFunc
	cancelOnce sync.Once
}

func (c *ShardingConsumerGroup) RegisterHandler(cb MessageHandlerF) {
	c.cb = cb
}

// Start start consume messages, watch signals.
//
// See ConsumerGroup.Start in consumer_group.go for why this backs off
// from 1s up to 10s instead of hot-looping while Kafka is unreachable.
func (c *ShardingConsumerGroup) Start() {
	runConsumerLoop(c.ctx, c.ConsumerGroup, c.Topics(), c.Group(), c, c.state)
}

func (c *ShardingConsumerGroup) Run(channelID int) {
	for {
		select {
		case message := <-c.chMessage[channelID]:
			if len(message.Message.Value) != 0 {
				c.cb(context.Background(), tryGetMethodByHeaders(message.Message.Headers), string(message.Message.Key), message.Message.Value)
			}
			message.MarkMessage()
		}
	}
}

// Stop stops consuming messages and releases the underlying consumer group.
func (c *ShardingConsumerGroup) Stop() {
	c.cancelOnce.Do(func() {
		c.cancel()
		_ = c.ConsumerGroup.Close()
	})
}

func (c *ShardingConsumerGroup) Topics() []string {
	return c.c.Topics
}

func (c *ShardingConsumerGroup) Group() string {
	return c.c.Group
}

// IsHealthy reports whether the consumer group currently holds a live
// session with the broker coordinator.
func (c *ShardingConsumerGroup) IsHealthy() bool {
	return c.state.IsHealthy()
}

// State returns a point-in-time snapshot of the connection state, useful
// for a /healthz endpoint or metrics.
func (c *ShardingConsumerGroup) State() ConnSnapshot {
	return c.state.Snapshot()
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *ShardingConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	// A session was successfully (re)established with the group
	// coordinator, i.e. we are connected.
	c.state.markUp()
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
