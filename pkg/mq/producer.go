package kafka

import (
	"context"
	"errors"

	"github.com/teamgram/marmota/pkg/error2"

	"github.com/Shopify/sarama"
)

// spanName is used to identify the span name for the SQL execution.
const spanName = "kafka.producer"

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

	pub, err := sarama.NewSyncProducer(c.Brokers, kc)
	if err != nil {
		panic(err)
	}
	return &Producer{pub, c}
}

// SendMessage
// Input send msg to kafka
// NOTE: If producer has beed created failed, the message will lose.
func (p *Producer) SendMessage(ctx context.Context, key string, value []byte) (partition int32, offset int64, err error) {
	if len(value) == 0 {
		err = error2.Wrap(errors.New("len(value) == 0 "), "")
		return
	}

	kMsg := &sarama.ProducerMessage{}
	kMsg.Topic = p.Topic()
	kMsg.Key = sarama.StringEncoder(key)
	kMsg.Value = sarama.ByteEncoder(value)
	if kMsg.Key.Length() == 0 || kMsg.Value.Length() == 0 {
		partition = -1
		offset = -1
		err = error2.Wrap(errors.New("key or value == 0"), "")
		return
	}

	_, span := startSpan(ctx, "SendMessage")
	defer func() {
		endSpan(span, err)
	}()

	partition, offset, err = p.producer.SendMessage(kMsg)
	err = error2.Wrap(err, "")
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
