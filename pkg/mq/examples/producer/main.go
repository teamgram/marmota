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
	"math/rand"
	"time"

	kafka "github.com/teamgram/marmota/pkg/mq"
)

func main() {
	producer := kafka.MustKafkaProducer(&kafka.KafkaProducerConf{
		Topic:   "teamgram-test-topic",
		Brokers: []string{"127.0.0.1:9092"},
	})

	rt2 := time.Now()
	for i := 0; i < 100000; i++ {
		// rt := time.Now()
		producer.SendMessage(context.Background(), "11", []byte(fmt.Sprintf("msg: %d", rand.Int())))
		// fmt.Println("cost: ", time.Since(rt))
	}
	fmt.Println("cost: ", time.Since(rt2))
}
