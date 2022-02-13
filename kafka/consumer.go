package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/Tooooommy/pubsub"
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
	for i := 0; i < inner.s.conf.Routines; i++ {
		inner.s.routines.Run(func() {
			for ch := range c.Messages() {
				msg := &message{
					key:   string(ch.Key),
					value: ch.Value,
					msg:   ch,
					sess:  s,
				}
				pubsub.ConsumeOne(inner.s.metrics, inner.s.handle, msg)
			}
		})
	}
	return nil
}
