package nats

import (
	"strings"

	"github.com/Tooooommy/pubsub"
	"github.com/nats-io/nats.go"
)

// Conf ...
type Conf struct {
	pubsub.Conf
	stream bool
}

func newConf() *Conf {
	return &Conf{
		Conf:   pubsub.NewConf("nats", []string{"nats://localhost:4222"}),
		stream: false,
	}
}

// Connect ...
func (cfg *Conf) Connect() (*nats.Conn, error) {
	if len(cfg.URL) > 0 {
		cfg.URL = append(cfg.URL, nats.DefaultURL)
	}
	url := strings.Join(cfg.URL, ",")
	return nats.Connect(url, nats.Name(cfg.Name))
}

// Stream ...
func (cfg *Conf) Stream(conn *nats.Conn) (nats.JetStreamContext, error) {
	if cfg.stream {
		return conn.JetStream(nats.Domain(cfg.Name))
	}
	return nil, nil
}
