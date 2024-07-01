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
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/teamgram/marmota/pkg/error2"
	"github.com/zeromicro/go-zero/core/logx"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

var (
	DefaultDataChanSize = 1000
	DefaultSize         = 100
	DefaultBuffer       = 100
	DefaultWorker       = 5
	DefaultInterval     = time.Second
)

type Config struct {
	size       int           // Number of message aggregations
	buffer     int           // The number of caches running in a single coroutine
	dataBuffer int           // The size of the main data channel
	worker     int           // Number of coroutines processed in parallel
	interval   time.Duration // Time of message aggregations
	syncWait   bool          // Whether to wait synchronously after distributing messages have been consumed
}

type Option func(c *Config)

func WithSize(s int) Option {
	return func(c *Config) {
		c.size = s
	}
}

func WithBuffer(b int) Option {
	return func(c *Config) {
		c.buffer = b
	}
}

func WithWorker(w int) Option {
	return func(c *Config) {
		c.worker = w
	}
}

func WithInterval(i time.Duration) Option {
	return func(c *Config) {
		c.interval = i
	}
}

func WithSyncWait(wait bool) Option {
	return func(c *Config) {
		c.syncWait = wait
	}
}

func WithDataBuffer(size int) Option {
	return func(c *Config) {
		c.dataBuffer = size
	}
}

type BatcherConsumerMessage struct {
	config *Config

	globalCtx  context.Context
	cancel     context.CancelFunc
	Do         func(channelID int, msg *MsgConsumerMessage)
	OnComplete func(lastMessage *sarama.ConsumerMessage, totalCount int)
	Sharding   func(key string) int
	Key        func(data *sarama.ConsumerMessage) string
	HookFunc   func(triggerID string, messages map[string][]*sarama.ConsumerMessage, totalCount int, lastMessage *sarama.ConsumerMessage)
	data       chan *sarama.ConsumerMessage
	chArrays   []chan *MsgConsumerMessage
	wait       sync.WaitGroup
	counter    sync.WaitGroup
}

func emptyOnComplete(*sarama.ConsumerMessage, int) {}
func emptyHookFunc(string, map[string][]*sarama.ConsumerMessage, int, *sarama.ConsumerMessage) {
}

func NewBatcherConsumerMessage(opts ...Option) *BatcherConsumerMessage {
	b := &BatcherConsumerMessage{
		OnComplete: emptyOnComplete,
		HookFunc:   emptyHookFunc,
	}
	config := &Config{
		size:     DefaultSize,
		buffer:   DefaultBuffer,
		worker:   DefaultWorker,
		interval: DefaultInterval,
	}
	for _, opt := range opts {
		opt(config)
	}
	b.config = config
	b.data = make(chan *sarama.ConsumerMessage, DefaultDataChanSize)
	b.globalCtx, b.cancel = context.WithCancel(context.Background())

	b.chArrays = make([]chan *MsgConsumerMessage, b.config.worker)
	for i := 0; i < b.config.worker; i++ {
		b.chArrays[i] = make(chan *MsgConsumerMessage, b.config.buffer)
	}
	return b
}

func (b *BatcherConsumerMessage) Worker() int {
	return b.config.worker
}

func (b *BatcherConsumerMessage) Start() error {
	if b.Sharding == nil {
		return error2.Wrap(errors.New("Sharding function is required"), "")
	}
	if b.Do == nil {
		return error2.Wrap(errors.New("Do function is required"), "")
	}
	if b.Key == nil {
		return error2.Wrap(errors.New("Key function is required"), "")
	}
	b.wait.Add(b.config.worker)
	for i := 0; i < b.config.worker; i++ {
		go b.run(i, b.chArrays[i])
	}
	b.wait.Add(1)
	go b.scheduler()
	return nil
}

func (b *BatcherConsumerMessage) Put(ctx context.Context, data *sarama.ConsumerMessage) error {
	if data == nil {
		return error2.Wrap(errors.New("data can not be nil"), "")
	}
	select {
	case <-b.globalCtx.Done():
		return error2.Wrap(errors.New("data channel is closed"), "")
	case <-ctx.Done():
		return ctx.Err()
	case b.data <- data:
		return nil
	}
}

func (b *BatcherConsumerMessage) scheduler() {
	ticker := time.NewTicker(b.config.interval)
	defer func() {
		ticker.Stop()
		for _, ch := range b.chArrays {
			close(ch)
		}
		close(b.data)
		b.wait.Done()
	}()

	vals := make(map[string][]*sarama.ConsumerMessage)
	count := 0
	var lastAny *sarama.ConsumerMessage

	for {
		select {
		case data, ok := <-b.data:
			if !ok {
				// If the data channel is closed unexpectedly
				return
			}
			if data == nil {
				if count > 0 {
					b.distributeMessage(vals, count, lastAny)
				}
				return
			}

			key := b.Key(data)
			vals[key] = append(vals[key], data)
			lastAny = data

			count++
			if count >= b.config.size {

				b.distributeMessage(vals, count, lastAny)
				vals = make(map[string][]*sarama.ConsumerMessage)
				count = 0
			}

		case <-ticker.C:
			if count > 0 {

				b.distributeMessage(vals, count, lastAny)
				vals = make(map[string][]*sarama.ConsumerMessage)
				count = 0
			}
		}
	}
}

type MsgConsumerMessage struct {
	Key       string
	TriggerID string
	MsgList   []*MsgDataToMQCtx
}

func (m MsgConsumerMessage) String() string {
	var sb strings.Builder
	sb.WriteString("Key: ")
	sb.WriteString(m.Key)
	sb.WriteString(", Values: [")
	for i, v := range m.MsgList {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(fmt.Sprintf("%v", *v))
	}
	sb.WriteString("]")
	return sb.String()
}

func (b *BatcherConsumerMessage) distributeMessage(messages map[string][]*sarama.ConsumerMessage, totalCount int, lastMessage *sarama.ConsumerMessage) {
	triggerID := OperationIDGenerator()
	b.HookFunc(triggerID, messages, totalCount, lastMessage)
	for key, data := range messages {
		if b.config.syncWait {
			b.counter.Add(1)
		}
		channelID := b.Sharding(key)
		msgList := make([]*MsgDataToMQCtx, 0, len(data))
		for i := range data {
			msgList = append(msgList, &MsgDataToMQCtx{
				Ctx:     injectTraceHeaders(data[i].Headers),
				Method:  tryGetMethodByHeaders(data[i].Headers),
				MsgData: data[i].Value,
			})
		}
		b.chArrays[channelID] <- &MsgConsumerMessage{Key: key, TriggerID: triggerID, MsgList: msgList}
	}
	if b.config.syncWait {
		b.counter.Wait()
	}
	b.OnComplete(lastMessage, totalCount)
}

func (b *BatcherConsumerMessage) run(channelID int, ch <-chan *MsgConsumerMessage) {
	defer b.wait.Done()
	for {
		select {
		case messages, ok := <-ch:
			if !ok {
				return
			}
			b.Do(channelID, messages)
			if b.config.syncWait {
				b.counter.Done()
			}
		}
	}
}

func (b *BatcherConsumerMessage) Close() {
	b.cancel() // Signal to stop put data
	b.data <- nil
	//wait all goroutines exit
	b.wait.Wait()
}

const (
	size           = 500
	mainDataBuffer = 500
	subChanBuffer  = 50
	worker         = 50
	interval       = 100 * time.Millisecond
)

type BatchMessageHandlerFV2 func(channelID int, msg *MsgConsumerMessage)

// BatchConsumerGroupV2 kafka consumer
type BatchConsumerGroupV2 struct {
	sarama.ConsumerGroup
	c *KafkaConsumerConf

	messageBatches *BatcherConsumerMessage
}

func MustKafkaBatchConsumerV2(c *KafkaConsumerConf, autoCommitEnable bool) *BatchConsumerGroupV2 {
	cg := &BatchConsumerGroupV2{
		c: c,
	}

	config, err := BuildConsumerGroupConfig(c, sarama.OffsetNewest, autoCommitEnable)
	if err != nil {
		panic(err)
	}
	consumerGroup, err := NewConsumerGroup(config, c.Brokers, c.Group)
	if err != nil {
		panic(err)
	}

	b := NewBatcherConsumerMessage(
		WithSize(size),
		WithWorker(worker),
		WithInterval(interval),
		WithDataBuffer(mainDataBuffer),
		WithSyncWait(true),
		WithBuffer(subChanBuffer),
	)

	b.Sharding = func(key string) int {
		hashCode := getHashCode(key)
		return int(hashCode) % cg.messageBatches.Worker()
	}
	b.Key = func(consumerMessage *sarama.ConsumerMessage) string {
		return string(consumerMessage.Key)
	}

	// b.Do = cg.do
	cg.messageBatches = b
	cg.ConsumerGroup = consumerGroup

	return cg
}

//func (c *BatchConsumerGroupV2) do(ctx context.Context, channelId int, val *MsgConsumerMessage) {
//	//
//}

func (c *BatchConsumerGroupV2) Topics() []string {
	return c.c.Topics
}

func (c *BatchConsumerGroupV2) Group() string {
	return c.c.Group
}

func (c *BatchConsumerGroupV2) Setup(_ sarama.ConsumerGroupSession) error { return nil }
func (c *BatchConsumerGroupV2) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *BatchConsumerGroupV2) ConsumeClaim(session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim) error { // a instance in the consumer group
	logx.Info("online new session msg come", "highWaterMarkOffset", claim.HighWaterMarkOffset(), "topic", claim.Topic(), "partition", claim.Partition())
	c.messageBatches.OnComplete = func(lastMessage *sarama.ConsumerMessage, totalCount int) {
		session.MarkMessage(lastMessage, "")
		session.Commit()
	}
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			if len(msg.Value) == 0 {
				continue
			}
			err := c.messageBatches.Put(context.Background(), msg)
			if err != nil {
				logx.Error("put msg to  error", err, "msg", msg)
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *BatchConsumerGroupV2) RegisterHandler(cb2 BatchMessageHandlerFV2) {
	c.messageBatches.Do = cb2
}

// Start start consume messages, watch signals
func (c *BatchConsumerGroupV2) Start() {
	// ctx, cancel = context.WithCancel(context.Background())
	go func() {
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
	}()

	c.messageBatches.Start()
}

// Stop Stop consume messages, watch signals
func (c *BatchConsumerGroupV2) Stop() {
}
