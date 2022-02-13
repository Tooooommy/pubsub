package pulsar

import (
	"context"

	"github.com/Tooooommy/pubsub"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
)

type (
	publisher struct {
		conf     *Conf
		producer pulsar.Producer
		executor *executors.ChunkExecutor
	}

	subscriber struct {
		conf     *Conf
		consumer pulsar.Consumer
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

	client, err := pulsar.NewClient(conf.ClientOptions())
	if err != nil {
		return nil, err
	}

	producer, err := client.CreateProducer(conf.ProducerOptions())
	if err != nil {
		return nil, err
	}

	execute := func(tasks []interface{}) {
		for _, task := range tasks {
			_, err := producer.Send(context.Background(), task.(*pulsar.ProducerMessage))
			if err != nil {
				logx.Error(err)
			}
		}
	}

	return &publisher{
		conf:     conf,
		producer: producer,
		executor: conf.ChunkExecutor(execute),
	}, nil
}

func (p *publisher) Publish(ctx context.Context, payload []byte, keys ...string) error {
	pm := &pulsar.ProducerMessage{Payload: payload}
	if len(keys) > 0 {
		pm.Key = keys[0]
	}

	if p.executor != nil {
		return p.executor.Add(pm, len(pm.Payload))
	}
	_, err := p.producer.Send(ctx, pm)
	return err
}

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

	err = conf.ValidateGroup()
	if err != nil {
		return nil, err
	}

	client, err := pulsar.NewClient(conf.ClientOptions())
	if err != nil {
		return nil, err
	}

	customer, err := client.Subscribe(conf.ConsumerOptions())
	if err != nil {
		return nil, err
	}

	return &subscriber{
		conf:     conf,
		consumer: customer,
		handle:   handle,
		routines: threading.NewRoutineGroup(),
		metrics:  conf.Metrics,
	}, err
}

// Start ...
func (s *subscriber) Start() {
	s.consume()
	s.routines.Wait()
	s.consumer.Close()
}

func (s *subscriber) consume() {
	for i := 0; i < s.conf.Routines; i++ {
		s.routines.Run(func() {
			for ch := range s.consumer.Chan() {
				pubsub.ConsumeOne(s.metrics, s.handle, &message{
					key:   ch.Message.Key(),
					value: ch.Message.Payload(),
					msg:   ch.Message,
				})
			}
		})
	}
}

// Stop ...
func (s *subscriber) Stop() {
	s.consumer.Close()
	_ = logx.Close()
}
