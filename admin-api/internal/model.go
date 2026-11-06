package internal

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Client struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"  json:"id"`
	Description string             `bson:"description"    json:"description"`
	GlobalID    string             `bson:"globalId"       json:"globalId"`
	CreatedAt   time.Time          `bson:"createdAt"      json:"createdAt"`
}

type Vehicle struct {
	ID           primitive.ObjectID `bson:"_id,omitempty"   json:"id"`
	Description  string             `bson:"description"     json:"description"`
	LicensePlate string             `bson:"licensePlate"    json:"licensePlate"`
	Capacity     int                `bson:"capacity"        json:"capacity"`
	ClientID     primitive.ObjectID `bson:"clientId"        json:"clientId"`
	CreatedAt    time.Time          `bson:"createdAt"       json:"createdAt"`
	UpdatedAt    time.Time          `bson:"updatedAt"       json:"updatedAt"`
}

type Device struct {
	ID               primitive.ObjectID  `bson:"_id,omitempty"      json:"id"`
	DeviceType       string              `bson:"deviceType"         json:"deviceType"`
	Serial           string              `bson:"serial"             json:"serial"`
	DeviceIdentifier string              `bson:"deviceIdentifier"   json:"deviceIdentifier"`
	VehicleID        *primitive.ObjectID `bson:"vehicleId"          json:"vehicleId,omitempty"`
	CreatedAt        time.Time           `bson:"createdAt"          json:"createdAt"`
	UpdatedAt        time.Time           `bson:"updatedAt"          json:"updatedAt"`
}

type RawMessage struct {
	ID               primitive.ObjectID  `bson:"_id,omitempty"      json:"id"`
	MessageID        string              `bson:"messageId"          json:"messageId"`
	DeviceID         *primitive.ObjectID `bson:"deviceId"           json:"deviceId,omitempty"`
	Serial           string              `bson:"serial"             json:"serial"`
	DeviceIdentifier string              `bson:"deviceIdentifier"   json:"deviceIdentifier"`
	RawPayload       string              `bson:"rawPayload"         json:"rawPayload"`
	ReceivedAt       time.Time           `bson:"receivedAt"         json:"receivedAt"`
	ProcessedAt      *time.Time          `bson:"processedAt"        json:"processedAt,omitempty"`
	Status           string              `bson:"status"             json:"status"`
	CreatedAt        time.Time           `bson:"createdAt"          json:"createdAt"`
}

type ClientEvent struct {
	ClientID   string `json:"clientId"`
	DeviceID   string `json:"deviceId"`
	MessageID  string `json:"messageId"`
	RawPayload string `json:"rawPayload"`
	ReceivedAt string `json:"receivedAt"`
}
