package pulsar

import "github.com/apache/pulsar-client-go/pulsar"

type (
	message struct {
		topic string
		key   string
		value []byte
		msg   pulsar.Message
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
	return
}
