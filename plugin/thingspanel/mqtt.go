package thingspanel

import (
	"errors"

	"github.com/DrmagicE/gmqtt"
	"github.com/DrmagicE/gmqtt/server"
)

type MqttClient struct {
	Publisher server.Publisher
}

var DefaultMqttClient *MqttClient = &MqttClient{}

func (c *MqttClient) SetPublisher(publisher server.Publisher) { c.Publisher = publisher }

func (c *MqttClient) SendData(topic string, data []byte) error {
	if c.Publisher == nil {
		return errors.New("broker publisher is not initialized")
	}
	c.Publisher.Publish(&gmqtt.Message{QoS: 1, Topic: topic, Payload: append([]byte(nil), data...)})
	return nil
}
