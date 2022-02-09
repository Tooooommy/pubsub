package pulsar

import (
	"context"
	"fmt"
	"github.com/Tooooommy/pubsub"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
)

func TestSubscribe(t *testing.T) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
	if err != nil {
		t.Error(err)
		return
	}
	topic := "my-topic"
	sub := "sub"
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: sub,
		Type:             pulsar.Shared,
	})
	if err != nil {
		t.Error(err)
		return
	}
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		t.Error(err)
		return
	}

	num := uint64(1000)
	counter := uint64(0)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for ch := range consumer.Chan() {
			fmt.Println(ch.Message.Key(), string(ch.Message.Payload()))
			atomic.AddUint64(&counter, 1)
			if atomic.LoadUint64(&counter) >= num {
				break
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := uint64(0); i < num; i++ {
			producer.Send(context.Background(), &pulsar.ProducerMessage{
				Key:     fmt.Sprintf("hello-%+v", i),
				Payload: []byte("world"),
			})
		}
	}()
	wg.Wait()
}

func TestSub(t *testing.T) {
	sub, err := NewSubscriber(func(m pubsub.Message) error {
		fmt.Println(m.Topic(), m.Key(), string(m.Value()))
		return nil
	})
	if err != nil {
		t.Error(err)
		return
	}
	sub.Start()
	defer sub.Stop()
}

func TestPub(t *testing.T) {
	pub, err := NewPublisher()
	if err != nil {
		t.Error(err)
		return
	}
	err = pub.Publish(context.Background(), []byte("hello"), "world")
	if err != nil {
		t.Error(err)
		return
	}
}
