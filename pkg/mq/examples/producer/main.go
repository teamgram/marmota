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
	"github.com/teamgram/marmota/pkg/hack"
	kafka "github.com/teamgram/marmota/pkg/mq"
	"math/rand"
	"strconv"
)

func main() {
	producer := kafka.MustKafkaProducer(&kafka.KafkaProducerConf{
		Topic:   "teamgram-test-topic",
		Brokers: []string{"127.0.0.1:9092"},
	})

	// doSendMessage(producer)
	doSendMessageV2(producer)
}

func doSendMessage(producer *kafka.Producer) {
	i := 0

	for {
		k := "sync.TL_sync_pushUpdates#136817694#" + strconv.FormatInt(int64(i), 10)
		v := strconv.FormatInt(rand.Int63(), 10)
		_, _, err := producer.SendMessage(context.Background(), k, hack.Bytes(v))
		if err != nil {
			fmt.Println("error - ", err)
		}
		fmt.Println("k:", k, "v:", v)
		i = i + 1
		// time.Sleep(100 * time.Millisecond)
	}
}

func doSendMessageV2(producer *kafka.Producer) {
	i := 0

	for {
		k := strconv.FormatInt(int64(i%10), 10)
		v := strconv.FormatInt(rand.Int63(), 10)
		_, _, err := producer.SendMessageV2(context.Background(), "sendMessage", k, hack.Bytes(v))
		if err != nil {
			fmt.Println("error - ", err)
		}
		fmt.Println("k:", k, "v:", v)
		i = i + 1
		// time.Sleep(100 * time.Millisecond)
	}
}
