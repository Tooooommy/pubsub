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
