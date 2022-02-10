package pubsub

import (
	"context"
	"time"

	"github.com/zeromicro/go-zero/core/executors"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/timex"
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
		Consumers     int
		Processors    int
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
		Consumers:     8,
		Processors:    8,
		Metrics:       stat.NewMetrics(name),
	}
}

// ConsumeOne ...
func ConsumeOne(metrics *stat.Metrics, handle MessageHandle, msg Message) {
	startTime := timex.Now()
	handle(msg)
	metrics.Add(stat.Task{Duration: timex.Since(startTime)})
}

// ChunkExecutor ...
func (cfg *Conf) ChunkExecutor(execute executors.Execute) *executors.ChunkExecutor {
	if cfg.ChunkSize == 0 && cfg.FlushInterval == 0 {
		return nil
	}
	var opts []executors.ChunkOption
	if cfg.ChunkSize > 0 {
		opts = append(opts, executors.WithChunkBytes(cfg.ChunkSize))
	}
	if cfg.FlushInterval > 0 {
		opts = append(opts, executors.WithFlushInterval(time.Duration(cfg.FlushInterval)))
	}
	return executors.NewChunkExecutor(execute, opts...)
}
