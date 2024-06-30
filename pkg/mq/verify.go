// Copyright © 2024 OpenIM open source community. All rights reserved.
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
	"fmt"

	"github.com/IBM/sarama"

	"github.com/teamgram/marmota/pkg/error2"
)

func Check(ctx context.Context, conf *KafkaConsumerConf, topics []string) error {
	kfk, err := BuildConsumerGroupConfig(conf, sarama.OffsetNewest, false)
	if err != nil {
		return err
	}
	cli, err := sarama.NewClient(conf.Brokers, kfk)
	if err != nil {
		return error2.Wrapf(err, "NewClient failed - {config: %v}", fmt.Sprintf("%+v", conf))
	}
	defer cli.Close()

	existingTopics, err := cli.Topics()
	if err != nil {
		return error2.Wrap(err, "Failed to list topics")
	}

	existingTopicsMap := make(map[string]bool)
	for _, t := range existingTopics {
		existingTopicsMap[t] = true
	}

	for _, topic := range topics {
		if !existingTopicsMap[topic] {
			return error2.Wrap(fmt.Errorf("topic not exist - {topic: %v}", topic), "")
		}
	}
	return nil
}
