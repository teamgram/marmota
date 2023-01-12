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

	"github.com/Shopify/sarama"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/rescue"
)

type MessageHandlerF func(ctx context.Context, key string, value []byte)

// ConsumerGroup kafka consumer
type ConsumerGroup struct {
	sarama.ConsumerGroup
	c  *KafkaConsumerConf
	cb map[string]MessageHandlerF
	//ctx    context.Context
	//cancel context.Context
}

func MustKafkaConsumer(c *KafkaConsumerConf) *ConsumerGroup {
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
	cg := &ConsumerGroup{
		ConsumerGroup: consumerGroup,
		c:             c,
		cb:            map[string]MessageHandlerF{},
	}
	//go func() {
	//	cg.consume()
	//}()

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
		// log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		// c.cb[message.Topic](context.Background(), string(message.Key), message.Value)
		c.consumeMessage(session, message)
	}

	return nil
}

func (c *ConsumerGroup) consumeMessage(session sarama.ConsumerGroupSession, message *sarama.ConsumerMessage) {
	defer rescue.Recover(func() {
		session.MarkMessage(message, "")
	})

	c.cb[message.Topic](context.Background(), string(message.Key), message.Value)
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
			logx.Error(err)
			return
		}
	}
}

// Stop Stop consume messages, watch signals
func (c *ConsumerGroup) Stop() {
}
