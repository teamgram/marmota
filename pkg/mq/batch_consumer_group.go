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

/*
** description("").
** copyright('tuoyun,www.tuoyun.net').
** author("fg,Gordon@tuoyun.net").
** time(2021/5/11 9:36).
 */

package kafka

import (
	"context"
	"hash/crc32"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

const (
	ConsumerMsgs        = 3
	AggregationMessages = 4
	ChannelNum          = 100
)

type MsgDataToMQ struct {
	TraceId string
	MsgType string
	MsgData []byte
}

type MsgChannelValue struct {
	AggregationID string //maybe userID or super groupID
	TriggerID     string
	MsgList       []*MsgDataToMQ
	// lastSeq       uint64
}

type TriggerChannelValue struct {
	triggerID string
	cMsgList  []*sarama.ConsumerMessage
}

type Cmd2Value struct {
	Cmd   int
	Value interface{}
}

type BatchMessageHandlerF func(ctx context.Context, msgs MsgChannelValue)
type BatchAggregationIdListHandlerF func(triggerID string, idList []string)

// BatchConsumerGroup kafka consumer
type BatchConsumerGroup struct {
	sarama.ConsumerGroup
	c                 *KafkaConsumerConf
	chArrays          [ChannelNum]chan Cmd2Value
	msgDistributionCh chan Cmd2Value
	cb                BatchAggregationIdListHandlerF
	cb2               BatchMessageHandlerF
}

func MustKafkaBatchConsumer(c *KafkaConsumerConf) *BatchConsumerGroup {
	cg := &BatchConsumerGroup{
		c: c,
	}

	cg.msgDistributionCh = make(chan Cmd2Value) //no buffer channel
	go cg.MessagesDistributionHandle()
	for i := 0; i < ChannelNum; i++ {
		cg.chArrays[i] = make(chan Cmd2Value, 50)
		go cg.Run(i)
	}

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.Consumer.Return.Errors = false

	cli, err := sarama.NewClient(c.Brokers, config)
	if err != nil {
		panic(err)
	}
	consumerGroup, err := sarama.NewConsumerGroupFromClient(c.Group, cli)
	if err != nil {
		panic(err)
	}
	cg.ConsumerGroup = consumerGroup

	return cg
}

func (c *BatchConsumerGroup) Run(channelID int) {
	for {
		select {
		case cmd := <-c.chArrays[channelID]:
			switch cmd.Cmd {
			case AggregationMessages:
				if c.cb2 != nil {
					c.cb2(context.Background(), cmd.Value.(MsgChannelValue))
				}
			}
		}
	}
}

func (c *BatchConsumerGroup) MessagesDistributionHandle() {
	for {
		aggregationMsgs := make(map[string][]*MsgDataToMQ, ChannelNum)
		select {
		case cmd := <-c.msgDistributionCh:
			switch cmd.Cmd {
			case ConsumerMsgs:
				triggerChannelValue := cmd.Value.(TriggerChannelValue)
				triggerID := triggerChannelValue.triggerID
				consumerMessages := triggerChannelValue.cMsgList
				aggregationIDList := make([]string, 0, len(triggerChannelValue.cMsgList))

				//Aggregation map[userid]message list
				logx.Debug(triggerID, "batch messages come to distribution center", len(consumerMessages))
				for i := 0; i < len(consumerMessages); i++ {
					msgFromMQ := MsgDataToMQ{
						MsgData: consumerMessages[i].Value,
					}
					aggregationIDList = append(aggregationIDList, string(consumerMessages[i].Key))
					if oldM, ok := aggregationMsgs[string(consumerMessages[i].Key)]; ok {
						oldM = append(oldM, &msgFromMQ)
						aggregationMsgs[string(consumerMessages[i].Key)] = oldM
					} else {
						m := make([]*MsgDataToMQ, 0, 100)
						m = append(m, &msgFromMQ)
						aggregationMsgs[string(consumerMessages[i].Key)] = m
					}
				}

				if c.cb != nil {
					c.cb(triggerID, aggregationIDList)
				}

				logx.Debug(triggerID, " - generate map list users len: ", len(aggregationMsgs))
				for aggregationID, v := range aggregationMsgs {
					if len(v) >= 0 {
						hashCode := getHashCode(aggregationID)
						channelID := hashCode % ChannelNum
						logx.Debugf("triggerID(%d) - generate channelID, {hashCode: %d, channelID: %d, aggregationID: %d)", triggerID, hashCode, channelID, aggregationID)
						c.chArrays[channelID] <- Cmd2Value{Cmd: AggregationMessages, Value: MsgChannelValue{AggregationID: aggregationID, MsgList: v, TriggerID: triggerID}}
					}
				}
			}
		}
	}
}

func (c *BatchConsumerGroup) Topics() []string {
	return c.c.Topics
}

func (c *BatchConsumerGroup) Group() string {
	return c.c.Group
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *BatchConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *BatchConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func OperationIDGenerator() string {
	return strconv.FormatInt(time.Now().UnixNano()+int64(rand.Uint32()), 10)
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *BatchConsumerGroup) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		if sess == nil {
			logx.Info(" sess == nil, waiting ")
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	rwLock := new(sync.RWMutex)
	logx.Infof("online new session msg come", claim.HighWaterMarkOffset(), claim.Topic(), claim.Partition())
	cMsg := make([]*sarama.ConsumerMessage, 0, 1000)
	t := time.NewTicker(time.Duration(100) * time.Millisecond)
	var triggerID string
	go func() {
		for {
			select {
			case <-t.C:
				if len(cMsg) > 0 {
					rwLock.Lock()
					ccMsg := make([]*sarama.ConsumerMessage, 0, 1000)
					for _, v := range cMsg {
						ccMsg = append(ccMsg, v)
					}
					cMsg = make([]*sarama.ConsumerMessage, 0, 1000)
					rwLock.Unlock()
					split := 1000
					triggerID = OperationIDGenerator()
					logx.Info(triggerID, " - timer trigger msg consumer start, len(msg) is ", len(ccMsg))
					for i := 0; i < len(ccMsg)/split; i++ {
						//log.Debug()
						c.msgDistributionCh <- Cmd2Value{Cmd: ConsumerMsgs, Value: TriggerChannelValue{
							triggerID: triggerID, cMsgList: ccMsg[i*split : (i+1)*split]}}
					}
					if (len(ccMsg) % split) > 0 {
						c.msgDistributionCh <- Cmd2Value{Cmd: ConsumerMsgs, Value: TriggerChannelValue{
							triggerID: triggerID, cMsgList: ccMsg[split*(len(ccMsg)/split):]}}
					}
					//sess.MarkMessage(ccMsg[len(cMsg)-1], "")

					logx.Info(triggerID, " - timer trigger msg consumer end, len(msg) is ", len(cMsg))
				}
			}
		}
	}()

	for msg := range claim.Messages() {
		rwLock.Lock()
		if len(msg.Value) != 0 {
			cMsg = append(cMsg, msg)
		}
		rwLock.Unlock()
		sess.MarkMessage(msg, "")
	}

	return nil
}

func (c *BatchConsumerGroup) RegisterHandlers(cb BatchAggregationIdListHandlerF, cb2 BatchMessageHandlerF) {
	c.cb = cb
	c.cb2 = cb2
}

// Start start consume messages, watch signals
func (c *BatchConsumerGroup) Start() {
	ctx := context.Background()
	for {
		err := c.ConsumerGroup.Consume(ctx, c.Topics(), c)
		if err != nil {
			logx.Error(err)
			return
		}
	}
}

// Stop Stop consume messages, watch signals
func (c *BatchConsumerGroup) Stop() {
}

// String hashes a string to a unique hashcode.
//
// crc32 returns a uint32, but for our use we need
// and non negative integer. Here we cast to an integer
// and invert it if the result is negative.
func getHashCode(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}
