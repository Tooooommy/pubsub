package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/Tooooommy/pubsub"
)

type Conf struct {
	pubsub.Conf
	Offset  int64
	Version string
}

func newConf() *Conf {
	return &Conf{
		Conf:    pubsub.NewConf("kafka", []string{"127.0.0.1:9092"}),
		Offset:  0,
		Version: "3.0.0",
	}
}

func (cfg *Conf) KafkaConfig() *sarama.Config {
	kc := sarama.NewConfig()
	kc.Producer.RequiredAcks = sarama.WaitForAll
	kc.Producer.Retry.Max = 10
	kc.Producer.Return.Successes = true
	kc.Producer.Partitioner = sarama.NewHashPartitioner
	return kc
}
