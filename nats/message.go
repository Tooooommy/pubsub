package nats

import "github.com/nats-io/nats.go"

type (
	message struct {
		topic string
		key   string
		value []byte
		msg   *nats.Msg
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
	if err != nil {
		m.msg.Nak()
	} else {
		m.msg.Ack()
	}
}
