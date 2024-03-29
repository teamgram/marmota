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

package kafka

// KafkaProducerConf kafka producer settings.
type KafkaProducerConf struct {
	Brokers []string
	Topic   string
}

// KafkaConsumerConf kafka client settings.
type KafkaConsumerConf struct {
	Brokers []string
	Topics  []string
	Group   string
}

// KafkaBatchConsumerConf kafka client settings.
type KafkaBatchConsumerConf struct {
	KafkaConsumerConf
	Duration   int `json:",default=100"`
	ChannelNum int `json:"default=50"`
}

type KafkaShardingConsumerConf struct {
	KafkaConsumerConf
	Concurrency int    `json:",default=2048"`
	QueueBuffer int    `json:",default=128"`
	ClientId    string `json:",default=sarama"`
}
