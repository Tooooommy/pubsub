package pubsub

import (
	"context"
	"errors"
	"time"

	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/timex"
)

var (
	ErrTopicEmpty = errors.New("topic name is required")
	ErrURLEmpty   = errors.New("at least 1 broker host is required")
	ErrGroupEmpty = errors.New("group name is required")
)

type (

	// Publisher ...
	Publisher interface {
		Publish(context.Context, []byte, ...string) error
	}

	// Subscriber ...
	Subscriber interface {
		Start()
		Stop()
	}

	// Message ...
	Message interface {
		Topic() string
		Key() string
		Value() []byte
		Ack() error
		Nack() error
	}

	// MessageHandle ...
	MessageHandle func(Message)

	Conf struct {
		Name          string
		URL           []string
		Topic         string
		Group         string
		MaxMsgChan    int
		ChunkSize     int
		FlushInterval int
		Routines      int
		Metrics       *stat.Metrics
	}
)

func NewConf(name string, url []string) Conf {
	return Conf{
		Name:          name,
		URL:           url,
		Topic:         name + ".topic",
		Group:         name + ".group",
		MaxMsgChan:    1024,
		ChunkSize:     0,
		FlushInterval: 0,
		Routines:      8,
		Metrics:       stat.NewMetrics(name),
	}
}

func (conf *Conf) ValidateTopic() error {
	if len(conf.Topic) == 0 {
		return ErrTopicEmpty
	}
	return nil
}

func (conf *Conf) ValidateURL() error {

	if len(conf.URL) == 0 {
		return ErrURLEmpty
	}

	return nil
}

func (conf *Conf) ValidateGroup() error {

	if len(conf.Group) == 0 {
		return ErrGroupEmpty
	}

	return nil
}

// ConsumeOne ...
func ConsumeOne(metrics *stat.Metrics, handle MessageHandle, msg Message) {
	startTime := timex.Now()
	handle(msg)
	metrics.Add(stat.Task{Duration: timex.Since(startTime)})
}

// ChunkExecutor ...
func (conf *Conf) ChunkExecutor(execute executors.Execute) *executors.ChunkExecutor {
	if conf.ChunkSize == 0 && conf.FlushInterval == 0 {
		return nil
	}
	var opts []executors.ChunkOption
	if conf.ChunkSize > 0 {
		opts = append(opts, executors.WithChunkBytes(conf.ChunkSize))
	}
	if conf.FlushInterval > 0 {
		opts = append(opts, executors.WithFlushInterval(time.Duration(conf.FlushInterval)))
	}
	return executors.NewChunkExecutor(execute, opts...)
}
