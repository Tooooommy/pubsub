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
		conf     *Conf
		consumer *connector
		handle   pubsub.MessageHandle
		routines *threading.RoutineGroup
		metrics  *stat.Metrics
	}

	Option func(*Conf)
)

// NewPublisher ...
func NewPublisher(options ...Option) (pubsub.Publisher, error) {
	conf := newConf()
	for _, option := range options {
		option(conf)
	}

	err := conf.ValidateTopic()
	if err != nil {
		return nil, err
	}

	err = conf.ValidateURL()
	if err != nil {
		return nil, err
	}

	producer, err := newConn(conf)
	if err != nil {
		return nil, err
	}

	execute := func(tasks []interface{}) {
		for i := range tasks {
			err := producer.publish(tasks[i].(*nats.Msg))
			if err != nil {
				logx.Errorf("Error on executor execute:%+v,  err: %+v", tasks[i], err)
			}
		}
	}

	return &publisher{
		conf:     conf,
		producer: producer,
		executor: conf.ChunkExecutor(execute),
	}, err
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

	err := conf.ValidateTopic()
	if err != nil {
		return nil, err
	}

	err = conf.ValidateURL()
	if err != nil {
		return nil, err
	}

	consumer, err := newConn(conf)
	if err != nil {
		return nil, err
	}

	_, err = consumer.subscribe(conf.Topic)
	if err != nil {
		return nil, err
	}

	return &subscriber{
		conf:     conf,
		consumer: consumer,
		handle:   handle,
		routines: threading.NewRoutineGroup(),
		metrics:  conf.Metrics,
	}, nil
}

func (s *subscriber) Start() {
	s.consume()
	s.routines.Wait()
	s.consumer.Clone()
}

func (s *subscriber) consume() {
	for i := 0; i < s.conf.Routines; i++ {
		s.routines.Run(func() {
			for ch := range s.consumer.Chan() {
				msg := &message{
					topic: ch.Subject,
					value: ch.Data,
					msg:   ch,
				}
				header, ok := ch.Header["key"]
				if ok && len(header) > 0 {
					msg.key = header[0]
				}
				pubsub.ConsumeOne(s.metrics, s.handle, msg)
			}
		})
	}
}

func (s *subscriber) Stop() {
	s.consumer.Clone()
	_ = logx.Close()
}
