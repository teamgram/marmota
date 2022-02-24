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

package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	kafka "github.com/teamgram/marmota/pkg/mq"
)

func main() {
	job := kafka.MustKafkaConsumer(&kafka.KafkaConsumerConf{
		Topics:  []string{"teamgram-test-topic"},
		Brokers: []string{"127.0.0.1:9092"},
		Group:   "teamgram-test-group-job",
	})

	job.RegisterHandlers("teamgram-test-topic",
		func(ctx context.Context, key string, value []byte) {
			fmt.Println("key: ", key, ", value: ", string(value))
		})

	defer job.Stop()
	go job.Start()
	// signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	for {
		s := <-c
		fmt.Println("get a signal ", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			// job.Close()
			fmt.Println("exit...")
			return
		case syscall.SIGHUP:
		default:
			return
		}
	}
}
