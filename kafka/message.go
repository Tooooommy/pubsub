package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/zeromicro/go-zero/core/logx"
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

func (m *message) Ack(err error) {
	if err == nil {
		m.sess.MarkMessage(m.msg, "")
	} else {
		logx.Error("Error on message ack: %+v, Error: %+v", m, err)
	}
}
