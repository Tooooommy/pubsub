package pulsar

import (
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zeromicro/go-zero/core/logx"
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

func (m *message) Ack(err error) {
	if err == nil {
		m.consumer.Ack(m.msg)
	} else {
		m.consumer.Nack(m.msg)
		logx.Error("Error on message ack: %+v, Error: %+v", m, err)
	}
	return
}
