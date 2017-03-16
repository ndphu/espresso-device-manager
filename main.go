package main

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/espresso-commons"
	"github.com/ndphu/espresso-commons/dao"
	"github.com/ndphu/espresso-commons/messaging"
	"github.com/ndphu/espresso-commons/model/device"
	"github.com/ndphu/espresso-commons/repo"
	"github.com/ndphu/espresso-device-manager/handler"
	"github.com/ndphu/espresso-device-manager/monitor"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

var (
	DeviceHelloTopic      = "/espresso/devices/hello"
	Qos              byte = 1
	Session          *mgo.Session
)

func main() {
	s, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected to DB!")
	Session = s

	deviceRepo := repo.NewDeviceRepo(Session)
	textCommandRepo := repo.NewTextCommandRepo(Session)
	gpioCommandRepo := repo.NewGPIOCommandRepo(Session)
	deviceStatusRepo := repo.NewDeviceStatusRepo(Session)

	msgRouter, err := messaging.NewMessageRouter(commons.BrokerHost, commons.BrokerPort, "", "", fmt.Sprintf("device-manager-%d", time.Now().UnixNano()))

	if err != nil {
		panic(err)
	}

	defer msgRouter.Stop()
	// Init handler
	commandHandler := handler.NewCommandHandler(deviceRepo, textCommandRepo, gpioCommandRepo, msgRouter)
	msgRouter.Subscribe(string(messaging.IPCCommand), commandHandler)
	defer msgRouter.Unsubscribe(string(messaging.IPCCommand), commandHandler)
	// end Init Handler

	// device monitor
	deviceMonitor := monitor.NewDeviceMonitor(msgRouter, deviceRepo, deviceStatusRepo)
	deviceMonitor.Start()
	defer deviceMonitor.Stop()
	// deviceMonitor.IsDeviceMonitored(id)
	// end device monitor

	msgc := make(chan mqtt.Message)

	if token := msgRouter.GetMQTTClient().Subscribe(DeviceHelloTopic, Qos, func(c mqtt.Client, msg mqtt.Message) {
		msgc <- msg
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("Subscribed to", DeviceHelloTopic)

	for {
		msg := <-msgc
		fmt.Printf("[%s] %s\n", msg.Topic(), string(msg.Payload()))
		serial := string(msg.Payload())
		d := device.Device{}
		//count, err := dao.CountBy(deviceRepo, bson.M{"serial": serial})
		err := dao.FindOne(deviceRepo, bson.M{"serial": serial}, &d)
		if err != nil {
			if err.Error() == "not found" {
				d = device.Device{
					Name:   fmt.Sprintf("Unknow device %d", time.Now().UnixNano()),
					Serial: serial,
				}
				dao.Insert(deviceRepo, &d)
				fmt.Println("Inserted new device with id", d.Id.Hex())
				//msgRouter.Publish(msg)
				publishDeviceAddedEvent(msgRouter, &d)
			} else {
				fmt.Println("Insert device failed", err)
			}
		} else {
			// device already added to DB
		}
	}

}

func publishDeviceAddedEvent(msgr *messaging.MessageRouter, dv *device.Device) {
	msg := messaging.Message{
		Destination: messaging.IPCDevice,
		Source:      messaging.DeviceManager,
		Payload:     dv.Id.Hex(),
		Type:        messaging.DeviceAdded,
	}

	msgr.Publish(msg)
}
