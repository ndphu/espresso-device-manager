package handler

import (
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/espresso-commons"
	"github.com/ndphu/espresso-commons/dao"
	"github.com/ndphu/espresso-commons/messaging"
	"github.com/ndphu/espresso-commons/model/command"
	"github.com/ndphu/espresso-commons/model/device"
	"github.com/ndphu/espresso-commons/repo"
	"gopkg.in/mgo.v2/bson"
	"log"
)

type TextCommandHandler struct {
	DeviceRepo      *repo.DeviceRepo
	TextCommandRepo *repo.TextCommandRepo
	client          mqtt.Client
}

func NewTextCommandHandler(dr *repo.DeviceRepo, tcr *repo.TextCommandRepo, r *messaging.MessageRouter) *TextCommandHandler {
	return &TextCommandHandler{
		DeviceRepo:      dr,
		TextCommandRepo: tcr,
		client:          r.GetMQTTClient(),
	}
}

func (t *TextCommandHandler) HandleMessage(msg *messaging.Message) {
	tcId := string(msg.Payload)
	tc := command.TextCommand{}
	err := dao.FindById(t.TextCommandRepo, bson.ObjectIdHex(tcId), &tc)
	if err != nil {
		log.Println("Failed to get text command with id", tcId, "error:", err)
	} else {
		log.Println("Device id", tc.TargetDeviceId)
		targetDevice := device.Device{}
		err = dao.FindById(t.DeviceRepo, tc.TargetDeviceId, &targetDevice)
		if err != nil {
			log.Println("Cannot get device from text command", err)
		} else {
			deviceSerial := targetDevice.Serial
			topic := commons.GetCommandTopicFromSerial(deviceSerial)
			log.Println("Publish to", topic)
			token := t.client.Publish(topic, commons.DefaultToDeviceQos, false, tc.Text)
			if token.Wait() && token.Error() != nil {
				log.Println("Failed to publish message", token.Error())
			} else {
				log.Println("Message published")
			}
		}
	}
}
