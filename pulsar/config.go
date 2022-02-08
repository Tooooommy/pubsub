package pulsar

import (
	"time"

	"github.com/Tooooommy/pubsub"
	"github.com/apache/pulsar-client-go/pulsar"
)

type Conf struct {
	pubsub.Conf
	OperationTimeout  int
	ConnectionTimeout int
	MaxConnections    int
}

func newConf() *Conf {
	return &Conf{
		Conf: pubsub.NewConf("pulsar", []string{"pulsar://localhost:6650"}),
	}
}

func (cfg *Conf) ClientOptions() pulsar.ClientOptions {
	operationTimeout := time.Duration(cfg.OperationTimeout) * time.Second
	connectionTimeout := time.Duration(cfg.ConnectionTimeout) * time.Second
	return pulsar.ClientOptions{
		URL:               cfg.URL[0],
		OperationTimeout:  operationTimeout,
		ConnectionTimeout: connectionTimeout,
		ListenerName:      cfg.Name,
	}
}

func (cfg *Conf) ProducerOptions() pulsar.ProducerOptions {
	return pulsar.ProducerOptions{
		Topic: cfg.Topic,
		Name:  cfg.Name,
	}
}

func (cfg *Conf) ConsumerOptions() pulsar.ConsumerOptions {
	return pulsar.ConsumerOptions{
		Topic:            cfg.Topic,
		SubscriptionName: cfg.Group,
		Type:             pulsar.Shared,
	}
}
