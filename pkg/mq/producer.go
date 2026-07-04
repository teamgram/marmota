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
	"errors"
	"sync"
	"time"

	"github.com/teamgram/marmota/pkg/error2"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

var errEmptyMsg = errors.New("binary msg is empty")

// healthCheckMinInterval/MaxInterval bound how often the background
// prober re-tests broker reachability while the producer is otherwise
// idle, so a Kafka outage is detected and cleared without waiting for the
// next real SendMessage call to (maybe) discover it.
const (
	healthCheckMinInterval = time.Second
	healthCheckMaxInterval = 10 * time.Second
)

type Producer struct {
	client   sarama.Client
	producer sarama.SyncProducer
	c        *KafkaProducerConf

	state *ConnState

	stopOnce sync.Once
	stopCh   chan struct{}
}

func MustKafkaProducer(c *KafkaProducerConf) *Producer {
	conf, err := BuildProducerConfig(*c)
	if err != nil {
		panic(err)
	}

	client, err := sarama.NewClient(c.Brokers, conf)
	if err != nil {
		panic(error2.Wrapf(err, "sarama.NewClient failed - {addr: %v}", c.Brokers))
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		client.Close()
		panic(error2.Wrapf(err, "NewSyncProducerFromClient failed - {addr: %v}", c.Brokers))
	}

	p := &Producer{
		client:   client,
		producer: producer,
		c:        c,
		state:    newConnState(),
		stopCh:   make(chan struct{}),
	}

	go p.healthLoop()

	return p
}

// healthLoop actively probes broker connectivity every 1-10s (backing off
// while the cluster is down, resetting to 1s once it recovers), instead
// of only discovering Kafka is back the next time the app happens to produce a message.
func (p *Producer) healthLoop() {
	backoff := NewBackoff(healthCheckMinInterval, healthCheckMaxInterval)
	timer := time.NewTimer(healthCheckMinInterval)
	defer timer.Stop()

	for {
		select {
		case <-p.stopCh:
			return
		case <-timer.C:
		}

		wait := healthCheckMinInterval
		if err := p.client.RefreshMetadata(p.c.Topic); err != nil {
			p.state.markDown(err)
			wait = backoff.Next()
			logx.Errorf("kafka producer: health check failed, topic=%v, brokers=%v, err=%v, retrying in %s",
				p.c.Topic, p.c.Brokers, err, wait)
		} else {
			p.state.markUp()
			backoff.Reset()
		}
		timer.Reset(wait)
	}
}

// IsHealthy reports whether Kafka was reachable on the last health check
// or real send.
func (p *Producer) IsHealthy() bool {
	return p.state.IsHealthy()
}

// State returns a point-in-time snapshot of the connection state, useful
// for a /healthz endpoint or metrics.
func (p *Producer) State() ConnSnapshot {
	return p.state.Snapshot()
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
	p.recordResult(err)
	if err != nil {
		err = error2.Wrapf(err, "p.producer.SendMessage error")
	}

	return
}

func (p *Producer) SendMessageV2(ctx context.Context, method, key string, value []byte) (partition int32, offset int64, err error) {
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
	kMsg.Headers = append([]sarama.RecordHeader{{Key: []byte("method"), Value: []byte(method)}}, extractTraceHeaders(ctx)...)

	// Send the message
	partition, offset, err = p.producer.SendMessage(kMsg)
	p.recordResult(err)
	if err != nil {
		err = error2.Wrapf(err, "p.producer.SendMessage error")
	}

	return
}

// recordResult folds the outcome of a real send into the shared
// connection state, so a successful send clears an unhealthy state
// immediately rather than waiting for the next health-check tick, and a
// connectivity failure is recorded even if it happens between ticks.
func (p *Producer) recordResult(err error) {
	if err == nil {
		p.state.markUp()
		return
	}
	if isConnectivityErr(err) {
		p.state.markDown(err)
	}
}

func (p *Producer) Close() (err error) {
	p.stopOnce.Do(func() { close(p.stopCh) })
	if p.producer != nil {
		err = p.producer.Close()
	}
	if p.client != nil {
		if cerr := p.client.Close(); err == nil {
			err = cerr
		}
	}
	return
}

func (p *Producer) Topic() string {
	return p.c.Topic
}
