package internal

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Vehicle struct {
	ID       primitive.ObjectID `bson:"_id,omitempty"`
	ClientID primitive.ObjectID `bson:"clientId"`
}

type Device struct {
	ID               primitive.ObjectID  `bson:"_id,omitempty"`
	DeviceType       string              `bson:"deviceType"`
	Serial           string              `bson:"serial"`
	DeviceIdentifier string              `bson:"deviceIdentifier"`
	VehicleID        *primitive.ObjectID `bson:"vehicleId"`
	CreatedAt        time.Time           `bson:"createdAt"`
	UpdatedAt        time.Time           `bson:"updatedAt"`
}

type RawMessage struct {
	ID               primitive.ObjectID  `bson:"_id,omitempty"`
	MessageID        string              `bson:"messageId"`
	DeviceID         *primitive.ObjectID `bson:"deviceId"`
	Serial           string              `bson:"serial"`
	DeviceIdentifier string              `bson:"deviceIdentifier"`
	RawPayload       string              `bson:"rawPayload"`
	S3Key            string              `bson:"s3Key,omitempty"`
	ReceivedAt       time.Time           `bson:"receivedAt"`
	ProcessedAt      *time.Time          `bson:"processedAt"`
	Status           string              `bson:"status"` // pending | processed | failed
	CreatedAt        time.Time           `bson:"createdAt"`
}

type Client struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	Description string             `bson:"description"`
	GlobalID    string             `bson:"globalId"`
	CreatedAt   time.Time          `bson:"createdAt"`
}

type RawEvent struct {
	MessageID        string `json:"messageId"`
	ReceivedAt       string `json:"receivedAt"`
	Protocol         string `json:"protocol"`
	DeviceType       string `json:"deviceType"`
	Serial           string `json:"serial"`
	DeviceIdentifier string `json:"deviceIdentifier"`
	RawPayload       string `json:"rawPayload"`
}

type ClientEvent struct {
	ClientID   string `json:"clientId"`
	DeviceID   string `json:"deviceId"`
	MessageID  string `json:"messageId"`
	RawPayload string `json:"rawPayload"`
	ReceivedAt string `json:"receivedAt"`
}
