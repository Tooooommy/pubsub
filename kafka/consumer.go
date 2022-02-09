package kafka

import (
	"github.com/Shopify/sarama"
)

func WithConsumerGroupHandle(s *subscriber) sarama.ConsumerGroupHandler {
	return &innerConsumerGroupHandler{s: s}
}

type innerConsumerGroupHandler struct {
	s *subscriber
}

func (inner *innerConsumerGroupHandler) Setup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (inner *innerConsumerGroupHandler) Cleanup(s sarama.ConsumerGroupSession) error {
	return nil
}

func (inner *innerConsumerGroupHandler) ConsumeClaim(s sarama.ConsumerGroupSession, c sarama.ConsumerGroupClaim) error {
	for i := 0; i < inner.s.conf.Processors; i++ {
		inner.s.producerRoutines.Run(func() {
			for msg := range c.Messages() {
				inner.s.channel <- &message{
					key:   string(msg.Key),
					value: msg.Value,
					msg:   msg,
					sess:  s,
				}
			}
		})
	}
	return nil
}
