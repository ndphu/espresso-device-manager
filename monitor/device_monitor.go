package monitor

import (
	"encoding/json"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/espresso-commons"
	"github.com/ndphu/espresso-commons/dao"
	"github.com/ndphu/espresso-commons/messaging"
	"github.com/ndphu/espresso-commons/model/device"
	"github.com/ndphu/espresso-commons/repo"
	"gopkg.in/mgo.v2/bson"
	"time"
)

type DeviceMonitor struct {
	client             mqtt.Client
	messageRouter      *messaging.MessageRouter
	deviceRepo         *repo.DeviceRepo
	deviceStatusRepo   *repo.DeviceStatusRepo
	monitoringDevices  map[string]bool
	running            bool
	monitoringInterval int
	offlineInterval    int
}

func NewDeviceMonitor(msgr *messaging.MessageRouter, dr *repo.DeviceRepo, dsr *repo.DeviceStatusRepo) *DeviceMonitor {
	return &DeviceMonitor{
		client:             msgr.GetMQTTClient(),
		messageRouter:      msgr,
		deviceRepo:         dr,
		deviceStatusRepo:   dsr,
		monitoringDevices:  make(map[string]bool),
		running:            true,
		monitoringInterval: 30,
		offlineInterval:    120,
	}
}

func (d *DeviceMonitor) IsDeviceMonitored(id string) bool {
	_, exists := d.monitoringDevices[id]
	return exists
}

func (d *DeviceMonitor) insertDeviceStatus(ds *device.DeviceStatus) error {
	ds.Timestamp = time.Now()
	//ds.Online = true
	err := dao.Insert(d.deviceStatusRepo, ds)
	if err != nil {
		return err
	} else {
		return nil
	}
}

func (d *DeviceMonitor) publishDeviceUpdateEvent(dv *device.Device, online bool) {
	msg := messaging.Message{
		Destination: messaging.MessageDestination_DeviceUpdated,
		Payload:     dv.Id.Hex(),
		Source:      messaging.MessageSource_DeviceManager,
		//Type:        messaging.MessageType_DeviceUpdated,
	}

	if online {
		msg.Type = messaging.MessageType_DeviceOnline
	} else {
		msg.Type = messaging.MessageType_DeviceOffline
	}

	d.messageRouter.Publish(msg)
}

func (d *DeviceMonitor) updateDeviceOnStatusChanged(ds *device.DeviceStatus) error {
	dv := device.Device{}
	err := dao.FindOne(d.deviceRepo, bson.M{"serial": ds.Serial}, &dv)
	if err != nil {
		return err
	} else {
		//dv.Online = true
		if dv.Online != ds.Online {
			dv.Online = ds.Online
			err := dao.Update(d.deviceRepo, &dv)
			if err != nil {
				return err
			}
			d.publishDeviceUpdateEvent(&dv, ds.Online)
		}
		return nil
	}
}

func (d *DeviceMonitor) IsDeviceStillOnline(dv *device.Device) bool {
	latestStatus := device.DeviceStatus{}
	err := dao.FindOneLatest(d.deviceStatusRepo, bson.M{"serial": dv.Serial}, "timestamp", &latestStatus)
	if err != nil {
		fmt.Println("Failed to get latest event", err)
		return false
	} else {
		duration := time.Since(latestStatus.Timestamp)
		fmt.Println("Duration", duration.Seconds())
		return duration.Seconds() < float64(d.offlineInterval)
	}
}

func (d *DeviceMonitor) MessageHandler(msg mqtt.Message) {
	fmt.Println("[", msg.Topic(), "]", string(msg.Payload()))

	ds := device.DeviceStatus{}
	err := json.Unmarshal(msg.Payload(), &ds)
	if err != nil {
		fmt.Println("Cannot parse device health message", err)
	} else {
		ds.Online = true
		d.insertDeviceStatus(&ds)
		d.updateDeviceOnStatusChanged(&ds)
	}
}

func (d *DeviceMonitor) MonitorDevice(dv *device.Device) {
	d.client.Subscribe(fmt.Sprintf("/espresso/device/%s/health", dv.Serial), commons.DefaultToDeviceQos, func(client mqtt.Client, msg mqtt.Message) {
		d.MessageHandler(msg)
	})
}

func (d *DeviceMonitor) Start() {
	d.running = true
	go func() {
		for d.running {
			fmt.Println("Monitoring thread started!")
			devices := make([]device.Device, 0)
			dao.FindAll(d.deviceRepo, bson.M{"managed": true}, 0, 999, &devices)
			for i := 0; i < len(devices); i++ {
				dv := devices[i]
				fmt.Println("Checking device", dv.Name)
				//d.MonitorDevice(device)
				if !d.IsDeviceMonitored(dv.Id.Hex()) {
					d.MonitorDevice(&dv)
				}
				if dv.Online {
					// check online status
					if !d.IsDeviceStillOnline(&dv) {
						offlineStatus := device.DeviceStatus{
							Uptime: -1,
							Serial: dv.Serial,
							Free:   -1,
							Online: false,
						}

						err := d.insertDeviceStatus(&offlineStatus)
						if err != nil {
							fmt.Println("Failed to create offline status", err)
						} else {
							err = d.updateDeviceOnStatusChanged(&offlineStatus)
							if err != nil {
								fmt.Println("Failed to update device status", err)
							}
						}
					}
				}
			}
			fmt.Println("Monitoring thread finished!")
			time.Sleep(time.Duration(d.monitoringInterval) * time.Second)
		}
	}()
}

func (d *DeviceMonitor) Stop() {
	d.running = false
}
