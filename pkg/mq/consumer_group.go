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
	"fmt"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
	ztrace "github.com/zeromicro/go-zero/core/trace"
	"go.opentelemetry.io/otel/codes"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type MessageHandlerF func(ctx context.Context, method, key string, value []byte)

// ConsumerGroup kafka consumer
type ConsumerGroup struct {
	sarama.ConsumerGroup
	c  *KafkaConsumerConf
	cb map[string]MessageHandlerF
	//ctx    context.Context
	//cancel context.Context
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

	cg := &ConsumerGroup{
		ConsumerGroup: consumerGroup,
		c:             c,
		cb:            map[string]MessageHandlerF{},
	}

	return cg
}

func (c *ConsumerGroup) Topics() []string {
	return c.c.Topics
}

func (c *ConsumerGroup) Group() string {
	return c.c.Group
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (c *ConsumerGroup) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
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

// Start start consume messages, watch signals
func (c *ConsumerGroup) Start() {
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
func (c *ConsumerGroup) Stop() {
}
