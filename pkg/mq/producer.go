package kafka

import (
	"context"
	"github.com/Shopify/sarama"
)

// spanName is used to identify the span name for the SQL execution.
const spanName = "kafka.producer"

type Producer struct {
	SyncProducer sarama.SyncProducer
	c            *KafkaProducerConf
}

func MustKafkaProducer(c *KafkaProducerConf) *Producer {
	kc := sarama.NewConfig()
	kc.Producer.Return.Successes = true                 //Whether to enable the successes channel to be notified after the message is sent successfully
	kc.Producer.RequiredAcks = sarama.WaitForAll        //Set producer Message Reply level 0 1 all
	kc.Producer.Retry.Max = 10                          // Retry up to 10 times to produce the message
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
	ctx, span := startSpan(ctx, "SendMessage")
	defer func() {
		endSpan(span, err)
	}()

	partition, offset, err = p.SyncProducer.SendMessage(&sarama.ProducerMessage{
		Topic: p.Topic(),
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	})

	return
}

func (p *Producer) Close() (err error) {
	if p.SyncProducer != nil {
		return p.SyncProducer.Close()
	}
	return
}

func (p *Producer) Topic() string {
	return p.c.Topic
}
