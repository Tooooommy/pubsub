package nats

import (
	"github.com/nats-io/nats.go"
)

type (
	connector struct {
		c  *nats.Conn
		s  nats.JetStreamContext
		ch chan *nats.Msg
	}
)

func newConn(conf *Conf) (*connector, error) {
	c, err := conf.Connect()
	if err != nil {
		return nil, err
	}
	s, err := conf.Stream(c)
	if err != nil {
		return nil, err
	}
	return &connector{c: c, s: s}, nil
}

func (c *connector) publish(m *nats.Msg) error {
	if c.s != nil {
		_, err := c.s.PublishMsg(m)
		return err
	}
	return c.c.PublishMsg(m)
}

func (c *connector) subscribe(topic string) (chan *nats.Msg, error) {
	var err error
	c.ch = make(chan *nats.Msg)
	if c.s != nil {
		_, err = c.s.ChanSubscribe(topic, c.ch)
	} else {
		_, err = c.c.ChanSubscribe(topic, c.ch)
	}
	return c.ch, err
}

func (c *connector) Chan() chan *nats.Msg {
	return c.ch
}

func (c *connector) Clone() {
	close(c.ch)
	c.c.Close()
}
