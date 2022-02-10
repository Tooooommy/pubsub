package kafka

import (
	"github.com/Shopify/sarama"
)

type (
	message struct {
		topic string
		key   string
		value []byte
		msg   *sarama.ConsumerMessage
		sess  sarama.ConsumerGroupSession
	}
)

func (m *message) Topic() string {
	return m.topic
}

func (m *message) Key() string {
	return m.key
}

func (m *message) Value() []byte {
	return m.value
}

func (m *message) Ack() error {
	m.sess.MarkMessage(m.msg, "")
	return nil
}

func (m *message) Nack() error {
	return nil
}
