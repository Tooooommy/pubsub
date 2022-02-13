package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"github.com/Tooooommy/pubsub"
	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/threading"
)

type (
	publisher struct {
		conf     *Conf
		producer sarama.SyncProducer
		executor *executors.ChunkExecutor
	}

	subscriber struct {
		conf     *Conf
		consumer sarama.ConsumerGroup
		handle   pubsub.MessageHandle
		routines *threading.RoutineGroup
		metrics  *stat.Metrics
	}

	Option func(*Conf)
)

func NewPublisher(options ...Option) (pubsub.Publisher, error) {
	conf := newConf()
	for _, option := range options {
		option(conf)
	}

	err := conf.ValidateURL()
	if err != nil {
		return nil, err
	}

	err = conf.ValidateTopic()
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(conf.URL, conf.KafkaConfig())
	if err != nil {
		return nil, err
	}

	execute := func(tasks []interface{}) {
		for i := range tasks {
			_, _, err := producer.SendMessage(tasks[i].(*sarama.ProducerMessage))
			if err != nil {
				logx.Errorf("Error on executor execute:%+v,  err: %+v", tasks[i], err)
			}
		}
	}
	return &publisher{
		producer: producer,
		executor: conf.ChunkExecutor(execute),
		conf:     conf,
	}, err
}

func (p *publisher) Publish(ctx context.Context, payload []byte, keys ...string) error {
	km := &sarama.ProducerMessage{
		Topic: p.conf.Topic,
		Value: sarama.ByteEncoder(payload),
	}
	if len(keys) > 0 {
		km.Key = sarama.StringEncoder(keys[0])
	}

	if p.executor != nil {
		return p.executor.Add(km, len(payload))
	}

	_, _, err := p.producer.SendMessage(km)
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

	consumer, err := sarama.NewConsumerGroup(conf.URL, conf.Group, conf.KafkaConfig())
	if err != nil {
		return nil, err
	}
	return &subscriber{
		conf:     conf,
		consumer: consumer,
		handle:   handle,
		routines: threading.NewRoutineGroup(),
		metrics:  conf.Metrics,
	}, err
}

func (s *subscriber) Start() {
	s.consume()
	s.routines.Wait()
	s.consumer.Close()
}

func (s *subscriber) consume() {
	err := s.consumer.Consume(context.Background(), []string{s.conf.Topic},
		WithConsumerGroupHandle(s))
	if err != nil {
		logx.Errorf("Error on consume Topic: %+v, Error: %+v", s.conf.Topic, err)
		return
	}
}

func (s *subscriber) Stop() {
	_ = s.consumer.Close()
	_ = logx.Close()
}
