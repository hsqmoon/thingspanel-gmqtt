package thingspanel

import (
	"testing"

	"github.com/DrmagicE/gmqtt"
)

type capturePublisher struct{ message *gmqtt.Message }

func (p *capturePublisher) Publish(message *gmqtt.Message) { p.message = message }

func TestSendDataUsesBrokerPublisher(t *testing.T) {
	publisher := &capturePublisher{}
	client := &MqttClient{Publisher: publisher}
	payload := []byte("0")
	if err := client.SendData("devices/status/device-1", payload); err != nil {
		t.Fatal(err)
	}
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
