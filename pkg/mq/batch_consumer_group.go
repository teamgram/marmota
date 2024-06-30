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

/*
** description("").
** copyright('tuoyun,www.tuoyun.net').
** author("fg,Gordon@tuoyun.net").
** time(2021/5/11 9:36).
 */

package kafka

import (
	"context"
	"errors"
	"hash/crc32"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
	ztrace "github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel/codes"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ConsumerMsgs        = 3
	AggregationMessages = 4
	ChannelNum          = 100
)

type MsgDataToMQCtx struct {
	Ctx     context.Context
	Method  string
	MsgData []byte
}

type MsgChannelValue struct {
	AggregationID string //maybe userID or super groupID
	TriggerID     string
	MsgList       []*MsgDataToMQCtx
}

type TriggerChannelValue struct {
	triggerID string
	cMsgList  []*sarama.ConsumerMessage
}

type Cmd2Value struct {
	Cmd   int
	Value interface{}
}

type BatchMessageHandlerF func(msgs MsgChannelValue)
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

	config, err := BuildConsumerGroupConfig(c, sarama.OffsetNewest, true)
	if err != nil {
		panic(err)
	}
	consumerGroup, err := NewConsumerGroup(config, c.Brokers, c.Group)
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
					c.cb2(cmd.Value.(MsgChannelValue))
				}
			}
		}
	}
}

func (c *BatchConsumerGroup) MessagesDistributionHandle() {
	for {
		aggregationMsgs := make(map[string][]*MsgDataToMQCtx, ChannelNum)
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
					msgFromMQ := MsgDataToMQCtx{
						Ctx:     injectTraceHeaders(consumerMessages[i].Headers),
						Method:  tryGetMethodByHeaders(consumerMessages[i].Headers),
						MsgData: consumerMessages[i].Value,
					}
					aggregationIDList = append(aggregationIDList, string(consumerMessages[i].Key))
					if oldM, ok := aggregationMsgs[string(consumerMessages[i].Key)]; ok {
						oldM = append(oldM, &msgFromMQ)
						aggregationMsgs[string(consumerMessages[i].Key)] = oldM
					} else {
						m := make([]*MsgDataToMQCtx, 0, 100)
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
						logx.Debugf("triggerID(%s) - generate channelID, {hashCode: %d, channelID: %d, aggregationID: %s)", triggerID, hashCode, channelID, aggregationID)
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
	logx.Info("online new session msg come", claim.HighWaterMarkOffset(), claim.Topic(), claim.Partition())
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
			logx.WithContext(ctx).Error("consume err", err, "topic", c.Topics(), "groupID", c.Group())
			if errors.Is(err, sarama.ErrClosedConsumerGroup) {
				return
			}
			if errors.Is(err, context.Canceled) {
				return
			}
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

func WrapperTracerHandler(aggregationID, triggerID string, msg *MsgDataToMQCtx, cb func(ctx context.Context, method, key string, value []byte) error) error {
	_ = triggerID

	ctx, span := startConsumerSpan(msg.Ctx, aggregationID)
	defer span.End()

	err := cb(ctx, msg.Method, aggregationID, msg.MsgData)
	if err != nil {
		s, ok := status.FromError(err)
		if ok {
			span.SetStatus(codes.Error, s.Message())
			span.SetAttributes(ztrace.StatusCodeAttr(s.Code()))
			ztrace.MessageSent.Event(ctx, 1, s.Proto())
		} else {
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}

	span.SetAttributes(ztrace.StatusCodeAttr(gcodes.OK))
	return err
}
