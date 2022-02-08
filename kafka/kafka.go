package kafka

import (
	"context"
	"errors"

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
		conf             *Conf
		consumer         *consumer
		handle           pubsub.MessageHandle
		channel          chan pubsub.Message
		producerRoutines *threading.RoutineGroup
		consumerRoutines *threading.RoutineGroup
		metrics          *stat.Metrics
	}

	Option func(*Conf)
)

func NewPublisher(options ...Option) (pubsub.Publisher, error) {
	conf := newConf()
	for _, option := range options {
		option(conf)
	}

	if len(conf.Topic) == 0 {
		return nil, errors.New("topic name is required")
	}

	producer, err := sarama.NewSyncProducer(conf.URL, conf.KafkaConfig())
	if err != nil {
		return nil, err
	}

	execute := func(tasks []interface{}) {
		chunk := make([]*sarama.ProducerMessage, len(tasks))
		for i := range tasks {
			chunk[i] = tasks[i].(*sarama.ProducerMessage)
		}
		if err := producer.SendMessages(chunk); err != nil {
			logx.Error(err)
		}
	}
	return &publisher{
		producer: producer,
		executor: conf.ChunkExecutor(execute),
		conf:     conf,
	}, err
}

func (p *publisher) Publish(ctx context.Context, payload []byte, keys...string) error {
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

	if len(conf.Topic) == 0 {
		return nil, errors.New("topic name is required")
	}

	if len(conf.URL) == 0 {
		return nil, errors.New("at least 1 broker host is required")
	}

	consumer, err := newConsumer(conf)
	return &subscriber{
		conf:             conf,
		consumer:         consumer,
		handle:           handle,
		channel:          make(chan pubsub.Message, conf.MaxMsgChan),
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
	s.consumer.Close()
}

func (s *subscriber) produce() {
	c := s.consumer
	if c.c != nil {
		c.produce(s)
	} else {
		c.produceGroup(s)
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
	s.consumer.Close()
}
