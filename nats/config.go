package nats

import (
	"strings"

	"github.com/Tooooommy/pubsub"
	"github.com/nats-io/nats.go"
)

// Conf ...
type Conf struct {
	pubsub.Conf
	IsStream bool
}

func newConf() *Conf {
	return &Conf{
		Conf:     pubsub.NewConf("nats", []string{nats.DefaultURL}),
		IsStream: true,
	}
}

// Connect ...
func (conf *Conf) Connect() (*nats.Conn, error) {
	url := strings.Join(conf.URL, ",")
	return nats.Connect(url, nats.Name(conf.Name))
}

// Stream ...
func (conf *Conf) Stream(conn *nats.Conn) (nats.JetStreamContext, error) {
	if conf.IsStream {
		return conn.JetStream(nats.Domain(conf.Name))
	}
	return nil, nil
}
