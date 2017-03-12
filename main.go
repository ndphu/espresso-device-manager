package main

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/espresso-commons/dao"
	"github.com/ndphu/espresso-commons/messaging"
	"github.com/ndphu/espresso-commons/model/device"
	"github.com/ndphu/espresso-commons/repo"
	"github.com/ndphu/espresso-device-manager/handler"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
)

var (
	DeviceHelloTopic = "/espresso/device/hello"
	//Broker                = "tcp://localhost:1883"
	Broker       = "tcp://19november.freeddns.org:5370"
	Qos     byte = 1
	Session *mgo.Session
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

	// o := mqtt.NewClientOptions()
	// o.AddBroker(Broker)
	// o.SetClientID("espresso-device-service")
	// o.SetAutoReconnect(false)

	// c := mqtt.NewClient(o)

	// if token := c.Connect(); token.Wait() && token.Error() != nil {
	// 	panic(token.Error())
	// }

	// fmt.Println("Connected to broker!")

	msgRouter, err := messaging.NewMessageRouter("19november.freeddns.org", 5370, "", "", fmt.Sprintf("device-manager-%d", time.Now().UnixNano()))

	if err != nil {
		panic(err)
	}

	defer msgRouter.Stop()

	tch, _ := handler.NewTextCommandHandler(deviceRepo, textCommandRepo, msgRouter)
	msgRouter.Subscribe(string(messaging.MessageDestination_TextCommand), tch)

	gch, _ := handler.NewGPIOCommandHandler(deviceRepo, gpioCommandRepo, msgRouter)
	msgRouter.Subscribe(string(messaging.MessageDestination_GPIOCommand), gch)

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
		count, err := dao.CountBy(deviceRepo, bson.M{"serial": serial})
		if err != nil {
			fmt.Println("Fail to query to DB", err)
		} else {
			if count == 0 {
				d := device.Device{
					Name:   fmt.Sprintf("Unknow device %d", time.Now().UnixNano()),
					Serial: serial,
				}
				dao.Insert(deviceRepo, &d)
				fmt.Println("Insert new device with id", d.Id.Hex())
			} else {
				fmt.Println("Update device status to online")
			}

		}

	}

}
