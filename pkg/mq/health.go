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
** description("Check health of Kafka service").
** copyright('tuoyun,www.tuoyun.net').
** author("Meisam Ghanbari,meisam.ghanbari.pro@gmail.com").
** time(2026/07/02 12:06).
 */

package kafka

import (
	"context"
	"errors"
	"math/rand"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/zeromicro/go-zero/core/logx"
)

type ConnState struct {
	healthy   atomic.Bool
	mu        sync.RWMutex
	lastErr   error
	lastOK    time.Time
	lastCheck time.Time
}

func newConnState() *ConnState {
	s := &ConnState{}
	now := time.Now()
	s.healthy.Store(true)
	s.lastOK, s.lastCheck = now, now
	return s
}

// IsHealthy reports whether the last connectivity check or real
// send/consume attempt succeeded.
func (s *ConnState) IsHealthy() bool {
	return s.healthy.Load()
}

func (s *ConnState) markUp() {
	wasDown := !s.healthy.Swap(true)
	s.mu.Lock()
	now := time.Now()
	s.lastOK, s.lastCheck, s.lastErr = now, now, nil
	s.mu.Unlock()
	if wasDown {
		logx.Info("kafka: connection restored")
	}
}

func (s *ConnState) markDown(err error) {
	wasUp := s.healthy.Swap(false)
	s.mu.Lock()
	s.lastCheck, s.lastErr = time.Now(), err
	s.mu.Unlock()
	if wasUp {
		logx.Errorf("kafka: connection lost: %v", err)
	}
}

// ConnSnapshot is a point-in-time, race-free copy of a ConnState. Handy for
// exposing over a /healthz endpoint or metrics.
type ConnSnapshot struct {
	Healthy   bool
	LastErr   error
	LastOK    time.Time
	LastCheck time.Time
}

func (s *ConnState) Snapshot() ConnSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return ConnSnapshot{
		Healthy:   s.healthy.Load(),
		LastErr:   s.lastErr,
		LastOK:    s.lastOK,
		LastCheck: s.lastCheck,
	}
}

// isConnectivityErr classifies whether a send/consume error is about
// reaching the broker (worth flipping ConnState to down)
func isConnectivityErr(err error) bool {
	if err == nil {
		return false
	}
	switch {
	case errors.Is(err, sarama.ErrOutOfBrokers),
		errors.Is(err, sarama.ErrNotConnected),
		errors.Is(err, sarama.ErrControllerNotAvailable),
		errors.Is(err, sarama.ErrLeaderNotAvailable),
		errors.Is(err, sarama.ErrRequestTimedOut),
		errors.Is(err, context.DeadlineExceeded):
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// Backoff is a capped-exponential backoff with jitter, used to pace
// reconnect/health-check attempts (default: 1s up to 10s) instead of
// hot-looping while Kafka is unreachable.
type Backoff struct {
	min, max time.Duration

	mu  sync.Mutex
	cur time.Duration
}

func NewBackoff(min, max time.Duration) *Backoff {
	if min <= 0 {
		min = time.Second
	}
	if max < min {
		max = min
	}
	return &Backoff{min: min, max: max, cur: min}
}

// Next returns the delay to wait before the next attempt
// and advances the internal state towards max.
func (b *Backoff) Next() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	d := b.cur

	next := b.cur * 2
	if next > b.max || next <= 0 {
		next = b.max
	}
	b.cur = next

	// +/-25% jitter so many replicas hitting the same outage don't all
	// retry in lockstep.
	jitter := time.Duration((rand.Float64()*0.5 - 0.25) * float64(d))
	d += jitter
	if d < b.min {
		d = b.min
	}
	return d
}

// Reset returns the backoff to its minimum delay; call after a successful (re)connect.
func (b *Backoff) Reset() {
	b.mu.Lock()
	b.cur = b.min
	b.mu.Unlock()
}

// runConsumerLoop repeatedly calls group.Consume, which blocks for the
// duration of a consumer-group session and returns when the session ends
// (rebalance, broker error, context cancel, etc.).
//
// On error, it records the failure in state and waits out a 1-10s backoff
// instead of immediately retrying, so a Kafka outage becomes a paced
// retry instead of a CPU-spinning loop. On a clean return (successful
// session, e.g. right after Setup() was called) it marks the state
// healthy and resets the backoff so recovery is fast again. It returns
// once ctx is canceled or the group is closed.
func runConsumerLoop(ctx context.Context, group sarama.ConsumerGroup, topics []string, groupID string, handler sarama.ConsumerGroupHandler, state *ConnState) {
	backoff := NewBackoff(time.Second, 10*time.Second)

	for {
		if ctx.Err() != nil {
			return
		}

		err := group.Consume(ctx, topics, handler)
		if err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) || errors.Is(err, context.Canceled) {
				return
			}

			state.markDown(err)
			wait := backoff.Next()
			logx.WithContext(ctx).Errorf("kafka consumer: consume error, topics=%v, group=%v, err=%v, retrying in %s",
				topics, groupID, err, wait)

			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return
			}
			continue
		}

		if ctx.Err() != nil {
			return
		}

		state.markUp()
		backoff.Reset()
	}
}
