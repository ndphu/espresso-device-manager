package monitor

import (
	"encoding/json"
	"fmt"
	"github.com/eclipse/paho.mqtt.golang"
	"github.com/ndphu/espresso-commons/dao"
	"github.com/ndphu/espresso-commons/messaging"
	"github.com/ndphu/espresso-commons/model/device"
	"github.com/ndphu/espresso-commons/repo"
	"gopkg.in/mgo.v2/bson"
	"sync"
	"time"
)

var (
	DefaultQos         byte   = 1
	DevicesHealthTopic string = "/esp/devices/health"
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

	msgr.Subscribe(string(messaging.IPCDevice), &monitor)

	return &monitor
}

func (d *DeviceMonitor) OnNewMessage(msg *messaging.Message) {
	// only handle message from UI
	if msg.Source == messaging.UI {
		deviceId := msg.Payload
		var dv device.Device
		err := dao.FindById(d.deviceRepo, bson.ObjectIdHex(deviceId), &dv)
		if err != nil {
			fmt.Println(err)
		} else {
			switch msg.Type {
			case messaging.DeviceAdded:
			case messaging.DeviceUpdated:
				if !d.IsDeviceMonitored(deviceId) {
					d.MonitorDevice(&dv)
				}
				break
			case messaging.DeviceRemoved:
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

// Publish Device online/offline to device ipc topic
func (d *DeviceMonitor) publishDeviceUpdateEvent(dv *device.Device, online bool) {
	msg := messaging.Message{
		Destination: messaging.IPCDevice,
		Payload:     dv.Id.Hex(),
		Source:      messaging.DeviceManager,
	}

	if online {
		msg.Type = messaging.DeviceOnline
	} else {
		msg.Type = messaging.DeviceOffline
	}

	d.messageRouter.Publish(msg)
}

// Process the new device status record to update the device's status
func (d *DeviceMonitor) updateDeviceOnStatusChanged(dv *device.Device, ds *device.DeviceStatus) error {

	//dv.Online = true
	if dv.Online != ds.Online {
		dv.Online = ds.Online
		err := dao.Update(d.deviceRepo, dv)
		if err != nil {
			return err
		}
		d.publishDeviceUpdateEvent(dv, ds.Online)
	}
	return nil
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

func (d *DeviceMonitor) MonitorDevice(dv *device.Device) {
	d.monitoringDevicesLock.Lock()
	d.monitoringDevices[dv.Id.Hex()] = true
	d.monitoringDevicesLock.Unlock()
}

func (d *DeviceMonitor) StopMonitorDevice(dv *device.Device) {
	d.monitoringDevicesLock.Lock()
	d.monitoringDevices[dv.Id.Hex()] = false
	d.monitoringDevicesLock.Unlock()
}

func (d *DeviceMonitor) Run() {
	msgc := make(chan mqtt.Message)

	if token := d.client.Subscribe(DevicesHealthTopic, DefaultQos, func(c mqtt.Client, msg mqtt.Message) {
		msgc <- msg
	}); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("Subscribed to", DevicesHealthTopic)
	go d.startMonitoring()
	for {
		msg := <-msgc
		fmt.Printf("[%s] %s\n", msg.Topic(), string(msg.Payload()))
		deviceStatus := device.DeviceStatus{}
		err := json.Unmarshal(msg.Payload(), &deviceStatus)
		if err != nil {
			fmt.Println("Fail to parse device status", err)
		} else {
			err := d.insertDeviceStatus(&deviceStatus)
			if err != nil {
				fmt.Println("Fail to insert device status", err)
			} else {
				dv := device.Device{}
				// select a device with serial, and also not marked as deleted
				err := dao.FindOne(d.deviceRepo, bson.M{"serial": deviceStatus.Serial, "deleted": false}, &dv)
				if err != nil && err.Error() == "not found" {
					dv = device.Device{
						Name:   fmt.Sprintf("Unknow device %d", time.Now().Unix()),
						Serial: deviceStatus.Serial,
					}
					dao.Insert(d.deviceRepo, &dv)
					fmt.Println("Inserted new device with id", dv.Id.Hex())
					publishDeviceAddedEvent(d.messageRouter, &dv)
				} else {
					deviceStatus.Online = true
					d.updateDeviceOnStatusChanged(&dv, &deviceStatus)
				}
			}

		}

	}
}

func (d *DeviceMonitor) startMonitoring() {
	d.running = true
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
						err = d.updateDeviceOnStatusChanged(&dv, &offlineStatus)
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
}

func (d *DeviceMonitor) Stop() {
	d.running = false
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
