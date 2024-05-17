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
	"context"
	"errors"
	"time"

	"github.com/teamgram/marmota/pkg/error2"

	"github.com/IBM/sarama"
)

const (
	maxRetry = 10 // number of retries
)

const (
	MessageIDHeaderName = "message_id"
	SpanHeaderName      = "span_id"
	TraceHeaderName     = "trace_id"
)

var errEmptyMsg = errors.New("binary msg is empty")

type Producer struct {
	producer sarama.SyncProducer
	c        *KafkaProducerConf
}

func MustKafkaProducer(c *KafkaProducerConf) *Producer {
	kc := sarama.NewConfig()
	kc.Producer.Return.Successes = true //Whether to enable the successes channel to be notified after the message is sent successfully
	kc.Producer.Return.Errors = true
	kc.Producer.RequiredAcks = sarama.WaitForAll        //Set producer Message Reply level 0 1 all
	kc.Producer.Partitioner = sarama.NewHashPartitioner //Set the hash-key automatic hash partition. When sending a message, you must specify the key value of the message. If there is no key, the partition will be selected randomly

	var (
		producer sarama.SyncProducer
		err      error
	)

	for i := 0; i <= maxRetry; i++ {
		producer, err = sarama.NewSyncProducer(c.Brokers, kc) // Initialize the client
		if err == nil {
			break
		}

		time.Sleep(time.Duration(1) * time.Second)
	}
	if err != nil {
		panic(err)
	}

	return &Producer{producer, c}
}

// SendMessage
// Input send msg to kafka
// NOTE: If producer has beed created failed, the message will lose.
func (p *Producer) SendMessage(ctx context.Context, key string, value []byte) (partition int32, offset int64, err error) {
	ctx, span := startProducerSpan(ctx, "SendMessage")
	defer func() {
		endProducerSpan(span, err)
	}()

	if len(value) == 0 {
		err = error2.Wrapf(errors.New("len(value) == 0 "), "")
		return
	}

	// Prepare Kafka message
	kMsg := &sarama.ProducerMessage{
		Topic: p.Topic(),
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	// Validate message key and value
	if kMsg.Key.Length() == 0 || kMsg.Value.Length() == 0 {
		err = error2.Wrap(errEmptyMsg, "")
		return
	}

	// Attach context metadata as headers
	kMsg.Headers = extractTraceHeaders(ctx)

	// Send the message
	partition, offset, err = p.producer.SendMessage(kMsg)
	if err != nil {
		err = error2.Wrapf(err, "p.producer.SendMessage error")
	}

	return
}

func (p *Producer) Close() (err error) {
	if p.producer != nil {
		return p.producer.Close()
	}
	return
}

func (p *Producer) Topic() string {
	return p.c.Topic
}
