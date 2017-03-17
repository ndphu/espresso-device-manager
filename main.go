package main

import (
	"fmt"
	"github.com/ndphu/espresso-commons"
	"github.com/ndphu/espresso-commons/messaging"
	"github.com/ndphu/espresso-commons/repo"
	"github.com/ndphu/espresso-device-manager/handler"
	"github.com/ndphu/espresso-device-manager/monitor"
	"gopkg.in/mgo.v2"
	"time"
)

var (
	DevicesHealthTopic      = "/esp/devices/health"
	Qos                byte = 1
	Session            *mgo.Session
)

func main() {
	s, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	fmt.Println("Connected to DB!")
	Session = s
	// init repositories
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
	//deviceMonitor.Start()
	defer deviceMonitor.Stop()
	deviceMonitor.Run()
	// end device monitor
}
