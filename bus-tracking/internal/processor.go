package internal

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Processor struct {
	devices     *DeviceRepository
	vehicles    *VehicleRepository
	rawMessages *RawMessageRepository
	publisher   *EventPublisher
	storage     *S3Storage
}

func NewProcessor(devices *DeviceRepository, vehicles *VehicleRepository, rawMessages *RawMessageRepository, publisher *EventPublisher, storage *S3Storage) *Processor {
	return &Processor{
		devices:     devices,
		vehicles:    vehicles,
		rawMessages: rawMessages,
		publisher:   publisher,
		storage:     storage,
	}
}

func (p *Processor) Process(ctx context.Context, event RawEvent) {
	receivedAt, err := time.Parse(time.RFC3339Nano, event.ReceivedAt)
	if err != nil {
		receivedAt = time.Now().UTC()
	}

	device, err := p.devices.FindByDeviceIdentifier(ctx, event.DeviceType, event.DeviceIdentifier, event.Serial)
	if err != nil {
		log.Printf("error finding device serial=%s: %v", event.Serial, err)
		return
	}

	if device == nil {
		device, err = p.devices.Create(ctx, event.DeviceType, event.DeviceIdentifier, event.Serial)
		if err != nil {
			log.Printf("error creating device serial=%s: %v", event.Serial, err)
			return
		}
		log.Printf("device created serial=%s id=%s", event.Serial, device.ID.Hex())
	}

	now := time.Now().UTC()
	rawMsg := RawMessage{
		ID:               primitive.NewObjectID(),
		MessageID:        event.MessageID,
		DeviceID:         &device.ID,
		Serial:           event.Serial,
		DeviceIdentifier: event.DeviceIdentifier,
		RawPayload:       event.RawPayload,
		ReceivedAt:       receivedAt,
		Status:           "pending",
		CreatedAt:        now,
	}

	if err := p.rawMessages.Save(ctx, rawMsg); err != nil {
		log.Printf("error saving raw_message messageId=%s: %v", event.MessageID, err)
		return
	}

	if device.VehicleID == nil {
		_ = p.rawMessages.UpdateStatus(ctx, rawMsg.ID, "processed", &now)
		return
	}

	vehicle, err := p.vehicles.FindByID(ctx, *device.VehicleID)
	if err != nil {
		log.Printf("error finding vehicle vehicleId=%s: %v", device.VehicleID.Hex(), err)
		_ = p.rawMessages.UpdateStatus(ctx, rawMsg.ID, "failed", &now)
		return
	}
	if vehicle == nil {
		log.Printf("vehicle not found vehicleId=%s messageId=%s", device.VehicleID.Hex(), event.MessageID)
		_ = p.rawMessages.UpdateStatus(ctx, rawMsg.ID, "processed", &now)
		return
	}

	clientEvent := ClientEvent{
		ClientID:   vehicle.ClientID.Hex(),
		DeviceID:   device.ID.Hex(),
		MessageID:  event.MessageID,
		RawPayload: event.RawPayload,
		ReceivedAt: event.ReceivedAt,
	}

	data, err := json.Marshal(clientEvent)
	if err != nil {
		log.Printf("error marshaling client event messageId=%s: %v", event.MessageID, err)
		_ = p.rawMessages.UpdateStatus(ctx, rawMsg.ID, "failed", &now)
		return
	}

	if err := p.publisher.Publish(ctx, clientEvent.ClientID, data); err != nil {
		log.Printf("error publishing client event messageId=%s: %v", event.MessageID, err)
		_ = p.rawMessages.UpdateStatus(ctx, rawMsg.ID, "failed", &now)
		return
	}

	_ = p.rawMessages.UpdateStatus(ctx, rawMsg.ID, "processed", &now)
	log.Printf("client event published messageId=%s clientId=%s", event.MessageID, clientEvent.ClientID)

	if p.storage != nil {
		go p.saveToS3(rawMsg)
	}
}

func (p *Processor) saveToS3(msg RawMessage) {
	ctx := context.Background()
	key, err := p.storage.Save(ctx, msg)
	if err != nil {
		log.Printf("s3 save failed messageId=%s: %v", msg.MessageID, err)
		return
	}
	if err := p.rawMessages.UpdateS3Key(ctx, msg.ID, key); err != nil {
		log.Printf("s3 key update failed messageId=%s: %v", msg.MessageID, err)
	}
}
