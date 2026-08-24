package thingspanel

import (
	"sync"
	"testing"
	"time"

	"github.com/DrmagicE/gmqtt"
)

type capturePublisher struct {
	mu      sync.Mutex
	message *gmqtt.Message
	ready   chan struct{}
}

type blockingPublisher struct {
	started chan struct{}
	release chan struct{}
}

func (p *blockingPublisher) Publish(*gmqtt.Message) {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-p.release
}

func (p *capturePublisher) Publish(message *gmqtt.Message) {
	p.mu.Lock()
	p.message = message
	p.mu.Unlock()
	select {
	case p.ready <- struct{}{}:
	default:
	}
}

func TestSendDataUsesBrokerPublisher(t *testing.T) {
	publisher := &capturePublisher{ready: make(chan struct{}, 1)}
	client := &MqttClient{}
	client.SetPublisher(publisher)
	payload := []byte("0")
	if err := client.SendData("devices/status/device-1", payload); err != nil {
		t.Fatal(err)
	}
	select {
	case <-publisher.ready:
	case <-time.After(time.Second):
		t.Fatal("publisher did not receive queued message")
	}
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	if publisher.message == nil || publisher.message.Topic != "devices/status/device-1" || string(publisher.message.Payload) != "0" || publisher.message.QoS != 1 {
		t.Fatalf("unexpected message: %+v", publisher.message)
	}
	payload[0] = '1'
	if string(publisher.message.Payload) != "0" {
		t.Fatal("published payload must not alias the caller buffer")
	}
}

func TestSendDataRequiresPublisher(t *testing.T) {
	if err := (&MqttClient{}).SendData("devices/status/device-1", []byte("0")); err == nil {
		t.Fatal("missing broker publisher must fail")
	}
}

func TestSendDataDoesNotBlockWhenBrokerIsBusy(t *testing.T) {
	publisher := &blockingPublisher{started: make(chan struct{}, 1), release: make(chan struct{})}
	client := &MqttClient{}
	client.SetPublisher(publisher)
	if err := client.SendData("devices/status/device-1", []byte("0")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-publisher.started:
	case <-time.After(time.Second):
		t.Fatal("publisher worker did not start")
	}
	started := time.Now()
	if err := client.SendData("devices/status/device-2", []byte("0")); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("busy broker publisher must not block the broker hook")
	}
	close(publisher.release)
}

func TestSendDataRejectsFullQueue(t *testing.T) {
	publisher := &capturePublisher{ready: make(chan struct{}, 1)}
	client := &MqttClient{publisher: publisher, sendCh: make(chan *gmqtt.Message, 1)}
	client.sendCh <- &gmqtt.Message{}
	started := time.Now()
	if err := client.SendData("devices/status/device-1", []byte("0")); err == nil {
		t.Fatal("full queue must return an error")
	}
	if time.Since(started) > 100*time.Millisecond {
		t.Fatal("full queue must not block the broker hook")
	}
}
