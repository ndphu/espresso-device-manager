package main

import (
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/espresso-commons/dao"
	"github.com/ndphu/espresso-commons/model"
	"github.com/ndphu/espresso-commons/repo"
	"gopkg.in/mgo.v2"
	"time"
)

var (
	DeviceHelloTopic      = "/espresso/device/hello"
	Broker                = "tcp://localhost:1883"
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

	o := mqtt.NewClientOptions()
	o.AddBroker(Broker)
	o.SetClientID("espresso-device-service")
	o.SetAutoReconnect(false)

	c := mqtt.NewClient(o)

	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("Connected to broker!")

	msgc := make(chan mqtt.Message)

	if token := c.Subscribe(DeviceHelloTopic, Qos, func(c mqtt.Client, msg mqtt.Message) {
		msgc <- msg
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("Subscribed to", DeviceHelloTopic)

	for {
		msg := <-msgc
		fmt.Printf("[%s] %s\n", msg.Topic(), string(msg.Payload()))
		d := model.Device{
			Name:   fmt.Sprintf("Unknow device %d", time.Now().UnixNano()),
			Serial: string(msg.Payload()),
		}
		dao.Insert(deviceRepo, &d)
		fmt.Println("Insert new device with id", d.Id.Hex())

	}
}
