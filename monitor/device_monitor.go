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
	"sync"
	"time"
)

type DeviceMonitor struct {
	client                mqtt.Client
	messageRouter         *messaging.MessageRouter
	deviceRepo            *repo.DeviceRepo
	deviceStatusRepo      *repo.DeviceStatusRepo
	monitoringDevices     map[string]bool
	running               bool
	monitoringInterval    int
	offlineInterval       int
	monitoringDevicesLock sync.Mutex
}

func NewDeviceMonitor(msgr *messaging.MessageRouter, dr *repo.DeviceRepo, dsr *repo.DeviceStatusRepo) *DeviceMonitor {
	monitor := DeviceMonitor{
		client:                msgr.GetMQTTClient(),
		messageRouter:         msgr,
		deviceRepo:            dr,
		deviceStatusRepo:      dsr,
		monitoringDevices:     make(map[string]bool),
		running:               true,
		monitoringInterval:    30,
		offlineInterval:       120,
		monitoringDevicesLock: sync.Mutex{},
	}

	msgr.Subscribe(string(messaging.MessageDestination_DeviceUpdated), &monitor)

	return &monitor
}

func (d *DeviceMonitor) OnNewMessage(msg *messaging.Message) {
	// only handle message from UI
	if msg.Source == messaging.MessageSource_UI {
		deviceId := msg.Payload
		var dv device.Device
		err := dao.FindById(d.deviceRepo, bson.ObjectIdHex(deviceId), &dv)
		if err != nil {
			fmt.Println(err)
		} else {
			switch msg.Type {
			case messaging.MessageType_DeviceAdded:
			case messaging.MessageType_DeviceUpdated:
				if !d.IsDeviceMonitored(deviceId) {
					d.MonitorDevice(&dv)
				}
				break
			case messaging.MessageType_DeviceRemoved:
				if d.IsDeviceMonitored(deviceId) {
					d.StopMonitorDevice(&dv)
				}
				break
			}
		}

	}
}

func (d *DeviceMonitor) IsDeviceMonitored(id string) bool {
	d.monitoringDevicesLock.Lock()
	_, exists := d.monitoringDevices[id]
	d.monitoringDevicesLock.Unlock()
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
	d.monitoringDevicesLock.Lock()
	healthTopic := fmt.Sprintf("/espresso/device/%s/health", dv.Serial)
	fmt.Println("Subscribe to health topic", healthTopic)
	if token := d.client.Subscribe(healthTopic, commons.DefaultToDeviceQos, func(client mqtt.Client, msg mqtt.Message) {
		d.MessageHandler(msg)
	}); token.Wait() && token.Error() != nil {
		fmt.Println("Fail to monitor device", dv.Name, "error:", token.Error())
	} else {
		d.monitoringDevices[dv.Id.Hex()] = true
	}
	d.monitoringDevicesLock.Unlock()
}

func (d *DeviceMonitor) StopMonitorDevice(dv *device.Device) {
	d.monitoringDevicesLock.Lock()
	healthTopic := fmt.Sprintf("/espresso/device/%s/health", dv.Serial)
	fmt.Println("Unsubscribe to health topic", healthTopic)
	if token := d.client.Unsubscribe(healthTopic); token.Wait() && token.Error() != nil {
		fmt.Println("Fail to unsubscribe device", dv.Name, "error:", token.Error())
	} else {
		d.monitoringDevices[dv.Id.Hex()] = false
	}
	d.monitoringDevicesLock.Unlock()
}

func (d *DeviceMonitor) Start() {
	d.running = true
	go func() {
		for d.running {
			fmt.Println("======= Monitoring thread started =======")
			devices := make([]device.Device, 0)
			dao.FindAll(d.deviceRepo, bson.M{"managed": true, "deleted": false}, 0, 999, &devices)
			for i := 0; i < len(devices); i++ {
				dv := devices[i]
				fmt.Println("Checking device >>>", dv.Name)
				//d.MonitorDevice(device)
				if !d.IsDeviceMonitored(dv.Id.Hex()) {
					d.MonitorDevice(&dv)
					fmt.Println("Started monitoring thread")
				}
				if dv.Online {
					// check online status
					if !d.IsDeviceStillOnline(&dv) {
						fmt.Println("Updating device status to offline")
						offlineStatus := device.DeviceStatus{
							Uptime: -1,
							Serial: dv.Serial,
							Free:   -1,
							Online: false,
						}
						err := d.insertDeviceStatus(&offlineStatus)
						if err != nil {
							fmt.Println("Create offline status failed", err)
						} else {
							err = d.updateDeviceOnStatusChanged(&offlineStatus)
							if err != nil {
								fmt.Println("Update device status failed", err)
							}
						}
					}
				}
			}
			fmt.Println("======= Monitoring thread finished =======")
			time.Sleep(time.Duration(d.monitoringInterval) * time.Second)
		}
	}()
}

func (d *DeviceMonitor) Stop() {
	d.running = false
}
