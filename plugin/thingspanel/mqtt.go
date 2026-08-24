package thingspanel

import (
	"errors"
	"sync"

	"github.com/DrmagicE/gmqtt"
	"github.com/DrmagicE/gmqtt/server"
)

type MqttClient struct {
	mu        sync.RWMutex
	publisher server.Publisher
	sendCh    chan *gmqtt.Message
	start     sync.Once
}

var DefaultMqttClient *MqttClient = &MqttClient{}

func (c *MqttClient) SetPublisher(publisher server.Publisher) {
	c.mu.Lock()
	c.publisher = publisher
	if c.sendCh == nil {
		c.sendCh = make(chan *gmqtt.Message, 8192)
	}
	sendCh := c.sendCh
	c.mu.Unlock()
	c.start.Do(func() { go c.sendWorker(sendCh) })
}

func (c *MqttClient) SendData(topic string, data []byte) error {
	c.mu.RLock()
	ready, sendCh := c.publisher != nil, c.sendCh
	c.mu.RUnlock()
	if !ready || sendCh == nil {
		return errors.New("broker publisher is not initialized")
	}
	select {
	case sendCh <- &gmqtt.Message{QoS: 1, Topic: topic, Payload: append([]byte(nil), data...)}:
		return nil
	default:
		return errors.New("broker publish queue is full")
	}
}

func (c *MqttClient) sendWorker(sendCh <-chan *gmqtt.Message) {
	for message := range sendCh {
		c.mu.RLock()
		publisher := c.publisher
		c.mu.RUnlock()
		if publisher != nil {
			publisher.Publish(message)
		}
	}
}
