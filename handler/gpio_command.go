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
	//"encoding/json"
	"fmt"
	"log"
)

type GPIOCommandHandler struct {
	DeviceRepo      *repo.DeviceRepo
	GPIOCommandRepo *repo.GPIOCommandRepo
	client          mqtt.Client
}

func NewGPIOCommandHandler(dr *repo.DeviceRepo, gcr *repo.GPIOCommandRepo, r *messaging.MessageRouter) *GPIOCommandHandler {
	return &GPIOCommandHandler{
		DeviceRepo:      dr,
		GPIOCommandRepo: gcr,
		client:          r.GetMQTTClient(),
	}
}

func (t *GPIOCommandHandler) HandleMessage(msg *messaging.Message) {
	gc := command.GPIOCommand{}
	err := dao.FindById(t.GPIOCommandRepo, bson.ObjectIdHex(string(msg.Payload)), &gc)
	if err != nil {
		log.Println("Failed to get gpio command with id", string(msg.Payload), "error:", err)
	} else {
		log.Println("Device id", gc.TargetDeviceId)
		targetDevice := device.Device{}
		err = dao.FindById(t.DeviceRepo, gc.TargetDeviceId, &targetDevice)
		if err != nil {
			log.Println("Cannot get device from text command", err)
		} else {
			deviceSerial := targetDevice.Serial
			// TODO support multiple message broker
			// right now use a single one and hardcodded
			topic := commons.GetCommandTopicFromSerial(deviceSerial)
			log.Println("Publish to", topic)
			state := 0
			if gc.State {
				state = 1
			}
			commandString := fmt.Sprintf("GPIO_WRITE;%d;%d;", gc.Pin, state)

			//token := t.client.Publish(topic, commons.DefaultToDeviceQos, false, tc.Text)
			token := t.client.Publish(topic, commons.DefaultToDeviceQos, false, commandString)
			if token.Wait() && token.Error() != nil {
				log.Println("Failed to publish message", token.Error())
			} else {
				log.Println("Message published")
			}
		}
	}
}
