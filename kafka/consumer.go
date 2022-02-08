package kafka

import (
	"context"

	"github.com/Shopify/sarama"
)

type (
	consumer struct {
		ps []int32
		c  sarama.Consumer
		g  sarama.ConsumerGroup
	}

	ConsumerGroupHandle func()
)

func WithConsumerGroupHandle(s *subscriber) sarama.ConsumerGroupHandler {
	return &innerConsumerGroupHandler{s: s}
}

type innerConsumerGroupHandler struct {
	s *subscriber
}

func (i *innerConsumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (i *innerConsumerGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (i *innerConsumerGroupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for msg := range c.Messages() {
		i.s.channel <- &message{
			key:   string(msg.Key),
			value: msg.Value,
			msg:   msg,
			sess:  s,
		}
	}
	return nil
}

func newConsumer(cfg *Conf) (*consumer, error) {
	kc := sarama.NewConfig()
	kc.Version, _ = sarama.ParseKafkaVersion(cfg.Version)
	kc.Consumer.Return.Errors = true
	kc.Consumer.Offsets.Initial = cfg.Offset
	var err error
	c := &consumer{}
	if len(cfg.Group) <= 0 {
		c.c, err = sarama.NewConsumer(cfg.URL, kc)
		if err != nil {
			return c, err
		}
		c.ps, err = c.c.Partitions(cfg.Topic)
	} else {
		c.g, err = sarama.NewConsumerGroup(cfg.URL, cfg.Group, kc)
	}
	return c, err
}

func (c *consumer) produce(s *subscriber) {
	for _, p := range c.ps {
		pc, _ := c.c.ConsumePartition(s.conf.Topic, p, sarama.OffsetNewest)
		for i := 0; i < s.conf.Processors; i++ {
			s.producerRoutines.Run(func() {
				for msg:= range pc.Messages() {
					s.channel <- &message{
						key:   string(msg.Key),
						value: msg.Value,
						msg:   msg,
					}
				}
			})
		}
	}
}

func (c *consumer) produceGroup(s *subscriber) {
	for i := 0; i < s.conf.Processors; i++ {
		topic := []string{s.conf.Topic}
		_ = c.g.Consume(context.Background(), topic, WithConsumerGroupHandle(s))
	}
}

func (c *consumer) Close() {
	if c.c != nil {
		_ = c.c.Close()
	}
	if c.g != nil {
		_ = c.g.Close()
	}
}
