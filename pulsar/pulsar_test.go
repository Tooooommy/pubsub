package pulsar

import (
	"context"
	"fmt"
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

func TestPubSub(t *testing.T) {
	_ , err := NewPublisher()
	if err != nil {
		t.Error(err)
		return
	}
}

func TestChannel(t *testing.T)  {
	chs := make(chan int, 100)
	for i:=0;i<100;i++ {

	}
	close(chs)
	for num := range chs {
		fmt.Println(num)
	}
}
