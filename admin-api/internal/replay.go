package internal

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

const replayBatchSize = 100

type ReplayService struct {
	devices     *DeviceRepository
	vehicles    *VehicleRepository
	rawMessages *RawMessageRepository
	publisher   *EventPublisher
}

func NewReplayService(devices *DeviceRepository, vehicles *VehicleRepository, rawMessages *RawMessageRepository, publisher *EventPublisher) *ReplayService {
	return &ReplayService{
		devices:     devices,
		vehicles:    vehicles,
		rawMessages: rawMessages,
		publisher:   publisher,
	}
}

func (rs *ReplayService) TriggerReplay(deviceID primitive.ObjectID) error {
	ctx := context.Background()

	device, err := rs.devices.FindByID(ctx, deviceID)
	if err != nil {
		return err
	}
	if device == nil {
		return ErrNotFound
	}
	if device.VehicleID == nil {
		return ErrDeviceNotAssociated
	}

	vehicle, err := rs.vehicles.FindByID(ctx, *device.VehicleID)
	if err != nil {
		return err
	}
	if vehicle == nil {
		return ErrNotFound
	}

	go rs.runReplay(device, vehicle)
	return nil
}

func (rs *ReplayService) runReplay(device *Device, vehicle *Vehicle) {
	ctx := context.Background()
	clientID := vehicle.ClientID.Hex()
	deviceID := device.ID.Hex()

	log.Printf("replay started deviceId=%s clientId=%s", deviceID, clientID)

	var skip int64
	for {
		messages, err := rs.rawMessages.FindByDeviceIDOrdered(ctx, device.ID, skip, replayBatchSize)
		if err != nil {
			log.Printf("replay error fetching batch deviceId=%s skip=%d: %v", deviceID, skip, err)
			return
		}
		if len(messages) == 0 {
			break
		}

		for _, msg := range messages {
			event := ClientEvent{
				ClientID:   clientID,
				DeviceID:   deviceID,
				MessageID:  msg.MessageID,
				RawPayload: msg.RawPayload,
				ReceivedAt: msg.ReceivedAt.Format(time.RFC3339Nano),
			}

			data, err := json.Marshal(event)
			if err != nil {
				log.Printf("replay marshal error messageId=%s: %v", msg.MessageID, err)
				continue
			}

			if err := rs.publisher.Publish(ctx, clientID, data); err != nil {
				log.Printf("replay publish error messageId=%s: %v", msg.MessageID, err)
			}
		}

		skip += int64(len(messages))
		if int64(len(messages)) < replayBatchSize {
			break
		}
	}

	log.Printf("replay completed deviceId=%s clientId=%s total=%d", deviceID, clientID, skip)
}
