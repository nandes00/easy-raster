package internal

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DeviceRepository struct {
	col *mongo.Collection
}

func NewDeviceRepository(db *mongo.Database) *DeviceRepository {
	return &DeviceRepository{col: db.Collection("devices")}
}

func (r *DeviceRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "deviceType", Value: 1}, {Key: "serial", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "deviceIdentifier", Value: 1}}},
		{Keys: bson.D{{Key: "vehicleId", Value: 1}}},
	})
	return err
}

func (r *DeviceRepository) FindByDeviceIdentifier(ctx context.Context, deviceType, deviceIdentifier, serial string) (*Device, error) {
	filter := bson.M{
		"deviceType": deviceType,
		"$or": bson.A{
			bson.M{"deviceIdentifier": deviceIdentifier},
			bson.M{"serial": serial},
		},
	}
	var device Device
	err := r.col.FindOne(ctx, filter).Decode(&device)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return &device, err
}

func (r *DeviceRepository) Create(ctx context.Context, deviceType, deviceIdentifier, serial string) (*Device, error) {
	now := time.Now().UTC()
	device := Device{
		ID:               primitive.NewObjectID(),
		DeviceType:       deviceType,
		Serial:           serial,
		DeviceIdentifier: deviceIdentifier,
		VehicleID:        nil,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	_, err := r.col.InsertOne(ctx, device)
	if err != nil {
		return nil, err
	}
	return &device, nil
}

type VehicleRepository struct {
	col *mongo.Collection
}

func NewVehicleRepository(db *mongo.Database) *VehicleRepository {
	return &VehicleRepository{col: db.Collection("vehicles")}
}

func (r *VehicleRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*Vehicle, error) {
	var vehicle Vehicle
	err := r.col.FindOne(ctx, bson.M{"_id": id}).Decode(&vehicle)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return &vehicle, err
}

type RawMessageRepository struct {
	col *mongo.Collection
}

func NewRawMessageRepository(db *mongo.Database) *RawMessageRepository {
	return &RawMessageRepository{col: db.Collection("raw_messages")}
}

func (r *RawMessageRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateMany(ctx, []mongo.IndexModel{
		{Keys: bson.D{{Key: "messageId", Value: 1}}, Options: options.Index().SetUnique(true)},
		{Keys: bson.D{{Key: "deviceId", Value: 1}}},
		{Keys: bson.D{{Key: "serial", Value: 1}}},
		{Keys: bson.D{{Key: "receivedAt", Value: 1}}},
	})
	return err
}

func (r *RawMessageRepository) Save(ctx context.Context, msg RawMessage) error {
	_, err := r.col.InsertOne(ctx, msg)
	return err
}

func (r *RawMessageRepository) UpdateStatus(ctx context.Context, id primitive.ObjectID, status string, processedAt *time.Time) error {
	_, err := r.col.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"status": status, "processedAt": processedAt}},
	)
	return err
}

func (r *RawMessageRepository) UpdateS3Key(ctx context.Context, id primitive.ObjectID, s3Key string) error {
	_, err := r.col.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{"$set": bson.M{"s3Key": s3Key}},
	)
	return err
}

// FindByDeviceIDOrdered returns messages for a device ordered oldest→newest, paginated.
func (r *RawMessageRepository) FindByDeviceIDOrdered(ctx context.Context, deviceID primitive.ObjectID, skip, limit int64) ([]RawMessage, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "receivedAt", Value: 1}}).
		SetSkip(skip).
		SetLimit(limit)

	cursor, err := r.col.Find(ctx, bson.M{"deviceId": deviceID}, opts)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)

	var messages []RawMessage
	if err := cursor.All(ctx, &messages); err != nil {
		return nil, err
	}
	return messages, nil
}
