package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
)

type (
	message struct {
		topic    string
		key      string
		value    []byte
		msg      pulsar.Message
		consumer pulsar.Consumer
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
	m.consumer.Ack(m.msg)
	return nil
}

func (m *message) Nack() error {
	m.consumer.Nack(m.msg)
	return nil
}
