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
	"fmt"
	"sync"

	"github.com/IBM/sarama"
	ztrace "github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel/codes"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MessageHandlerF func(ctx context.Context, method, key string, value []byte)

// ConsumerGroup kafka consumer
type ConsumerGroup struct {
	sarama.ConsumerGroup
	c          *KafkaConsumerConf
	cb         map[string]MessageHandlerF
	state      *ConnState
	ctx        context.Context
	cancel     context.CancelFunc
	cancelOnce sync.Once
}

func MustKafkaConsumer(c *KafkaConsumerConf) *ConsumerGroup {
	config, err := BuildConsumerGroupConfig(c, sarama.OffsetNewest, true)
	if err != nil {
		panic(err)
	}
	consumerGroup, err := NewConsumerGroup(config, c.Brokers, c.Group)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cg := &ConsumerGroup{
		ConsumerGroup: consumerGroup,
		c:             c,
		cb:            map[string]MessageHandlerF{},
		state:         newConnState(),
		ctx:           ctx,
		cancel:        cancel,
	}

	return cg
}

func (c *ConsumerGroup) Topics() []string {
	return c.c.Topics
}

func (c *ConsumerGroup) Group() string {
	return c.c.Group
}

// IsHealthy reports whether the consumer group currently holds a live
// session with the broker coordinator.
func (c *ConsumerGroup) IsHealthy() bool {
	return c.state.IsHealthy()
}

// State returns a point-in-time snapshot of the connection state, useful
// for a /healthz endpoint or metrics.
func (c *ConsumerGroup) State() ConnSnapshot {
	return c.state.Snapshot()
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *ConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	// A session was successfully (re)established with the group
	// coordinator, i.e. we are connected.
	c.state.markUp()
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *ConsumerGroup) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (c *ConsumerGroup) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		func(message *sarama.ConsumerMessage) {
			ctx, span := startConsumerSpan(injectTraceHeaders(message.Headers), string(message.Key))
			defer span.End()

			var (
				err error
			)
			if len(message.Value) != 0 {
				c.cb[message.Topic](ctx, tryGetMethodByHeaders(message.Headers), string(message.Key), message.Value)
			} else {
				// logx.Debugf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
				err = fmt.Errorf("message(%v) get from kafka but is nil", message.Key)
			}

			if err != nil {
				s, ok := status.FromError(err)
				if ok {
					span.SetStatus(codes.Error, s.Message())
					span.SetAttributes(ztrace.StatusCodeAttr(s.Code()))
					ztrace.MessageSent.Event(ctx, 1, s.Proto())
				} else {
					span.SetStatus(codes.Error, err.Error())
				}
			} else {
				span.SetAttributes(ztrace.StatusCodeAttr(gcodes.OK))
			}
		}(message)

		session.MarkMessage(message, "")
	}

	return nil
}

func (c *ConsumerGroup) RegisterHandlers(topic string, cb MessageHandlerF) {
	c.cb[topic] = cb
}

// Start start consume messages, watch signals.
//
// Failed attempts (Kafka unreachable, broker timeout, etc.) back off from
// 1s up to 10s instead of spinning the CPU, and every attempt updates the
// shared ConnState so IsHealthy()/State() reflect current reachability.
// This is what lets the consumer notice Kafka came back on its own,
// without needing a new message or a service restart to kick it.
func (c *ConsumerGroup) Start() {
	runConsumerLoop(c.ctx, c.ConsumerGroup, c.Topics(), c.Group(), c, c.state)
}

// Stop stops consuming messages and releases the underlying consumer group.
func (c *ConsumerGroup) Stop() {
	c.cancelOnce.Do(func() {
		c.cancel()
		_ = c.ConsumerGroup.Close()
	})
}
