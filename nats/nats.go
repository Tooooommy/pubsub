package nats

import (
	"context"

	"github.com/Tooooommy/pubsub"
	"github.com/nats-io/nats.go"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
)

type (
	publisher struct {
		conf     *Conf
		producer *connector
		executor *executors.ChunkExecutor
	}

	subscriber struct {
		conf             *Conf
		consumer         *connector
		channel          chan pubsub.Message
		handle           pubsub.MessageHandle
		producerRoutines *threading.RoutineGroup
		consumerRoutines *threading.RoutineGroup
		metrics          *stat.Metrics
	}

	Option func(*Conf)
)

// NewPublisher ...
func NewPublisher(options ...Option) (pubsub.Publisher, error) {
	conf := newConf()
	for _, option := range options {
		option(conf)
	}

	producer, err := newConn(conf)
	if err != nil {
		return nil, err
	}
	execute := func(tasks []interface{}) {
		for i := range tasks {
			if err := producer.publish(tasks[i].(*nats.Msg)); err != nil {
				logx.Error(err)
			}
		}
	}
	return &publisher{
		conf:     conf,
		producer: producer,
		executor: conf.ChunkExecutor(execute),
	}, err
}

// WithEnableStream ...
func WithEnableStream(stream bool) Option {
	return func(cfg *Conf) {
		cfg.stream = stream
	}
}

func (p *publisher) Publish(ctx context.Context, payload []byte, keys ...string) error {
	m := &nats.Msg{
		Subject: p.conf.Topic,
		Data:    payload,
		Header:  nats.Header{"key": keys},
	}

	if p.executor != nil {
		return p.executor.Add(m, len(m.Data))
	}

	return p.producer.publish(m)
}

// NewSubscriber ...
func NewSubscriber(handle pubsub.MessageHandle, options ...Option) (pubsub.Subscriber, error) {
	conf := newConf()
	for _, option := range options {
		option(conf)
	}

	consumer, err := newConn(conf)
	if err != nil {
		return nil, err
	}

	return &subscriber{
		conf:             conf,
		consumer:         consumer,
		channel:          make(chan pubsub.Message, conf.MaxMsgChan),
		handle:           handle,
		producerRoutines: threading.NewRoutineGroup(),
		consumerRoutines: threading.NewRoutineGroup(),
		metrics:          conf.Metrics,
	}, err
}

func (s *subscriber) Start() {
	s.consume()
	s.produce()
	s.producerRoutines.Wait()
	close(s.channel)
	s.consumerRoutines.Wait()
	s.consumer.Clone()
}

func (s *subscriber) produce() {
	ch, err := s.consumer.subscribe(s.conf.Topic, s.conf.Group)
	if err != nil {
		logx.Error(err)
		return
	}
	for i := 0; i < s.conf.Processors; i++ {
		s.producerRoutines.Run(func() {
			for msg := range ch {
				key := ""
				if header, ok := msg.Header["key"]; ok && len(header) > 0 {
					key = header[0]
				}
				s.channel <- &message{
					topic: msg.Subject,
					value: msg.Data,
					key:   key,
					msg:   msg,
				}
			}
		})
	}
}

func (s *subscriber) consume() {
	for i := 0; i < s.conf.Consumers; i++ {
		s.consumerRoutines.Run(func() {
			for msg := range s.channel {
				pubsub.ConsumeOne(s.metrics, s.handle, msg)
			}
		})
	}
}

func (s *subscriber) Stop() {
	s.consumer.Clone()
	_ = logx.Close()
}
