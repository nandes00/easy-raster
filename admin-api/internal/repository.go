package internal

import (
	"context"
	"regexp"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const maxSearchLen = 100

type ClientRepository struct {
	col *mongo.Collection
}

func NewClientRepository(db *mongo.Database) *ClientRepository {
	return &ClientRepository{col: db.Collection("clients")}
}

func (r *ClientRepository) EnsureIndexes(ctx context.Context) error {
	_, err := r.col.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "globalId", Value: 1}},
		Options: options.Index().SetUnique(true),
	})
	return err
}

func (r *ClientRepository) FindAll(ctx context.Context) ([]Client, error) {
	cursor, err := r.col.Find(ctx, bson.M{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var clients []Client
	if err := cursor.All(ctx, &clients); err != nil {
		return nil, err
	}
	return clients, nil
}

func (r *ClientRepository) Create(ctx context.Context, description, globalID string) (*Client, error) {
	client := Client{
		ID:          primitive.NewObjectID(),
		Description: description,
		GlobalID:    globalID,
		CreatedAt:   time.Now().UTC(),
	}
	_, err := r.col.InsertOne(ctx, client)
	if err != nil {
		return nil, err
	}
	return &client, nil
}

type DeviceRepository struct {
	col *mongo.Collection
}

func NewDeviceRepository(db *mongo.Database) *DeviceRepository {
	return &DeviceRepository{col: db.Collection("devices")}
}

func (r *DeviceRepository) FindPaginated(ctx context.Context, search string, unassignedOnly bool, page, limit int) ([]Device, int64, error) {
	filter := deviceSearchFilter(search, unassignedOnly)
	total, err := r.col.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	opts := options.Find().
		SetSkip(int64((page - 1) * limit)).
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "createdAt", Value: -1}})
	cursor, err := r.col.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)
	devices := make([]Device, 0)
	if err := cursor.All(ctx, &devices); err != nil {
		return nil, 0, err
	}
	return devices, total, nil
}

func deviceSearchFilter(search string, unassignedOnly bool) bson.M {
	filter := bson.M{}
	if unassignedOnly {
		filter["vehicleId"] = nil
	}
	if search != "" {
		safe := safeSearch(search)
		filter["$or"] = bson.A{
			bson.M{"serial": bson.M{"$regex": safe, "$options": "i"}},
			bson.M{"deviceIdentifier": bson.M{"$regex": safe, "$options": "i"}},
		}
	}
	return filter
}

func (r *DeviceRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*Device, error) {
	var device Device
	err := r.col.FindOne(ctx, bson.M{"_id": id}).Decode(&device)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return &device, err
}

func (r *DeviceRepository) AssociateVehicle(ctx context.Context, deviceID, vehicleID primitive.ObjectID) error {
	_, err := r.col.UpdateOne(ctx,
		bson.M{"_id": deviceID},
		bson.M{"$set": bson.M{"vehicleId": vehicleID, "updatedAt": time.Now().UTC()}},
	)
	return err
}

type VehicleRepository struct {
	col *mongo.Collection
}

func NewVehicleRepository(db *mongo.Database) *VehicleRepository {
	return &VehicleRepository{col: db.Collection("vehicles")}
}

func (r *VehicleRepository) FindPaginated(ctx context.Context, search string, page, limit int) ([]Vehicle, int64, error) {
	filter := vehicleSearchFilter(search)
	total, err := r.col.CountDocuments(ctx, filter)
	if err != nil {
		return nil, 0, err
	}
	opts := options.Find().
		SetSkip(int64((page - 1) * limit)).
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "createdAt", Value: -1}})
	cursor, err := r.col.Find(ctx, filter, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)
	vehicles := make([]Vehicle, 0)
	if err := cursor.All(ctx, &vehicles); err != nil {
		return nil, 0, err
	}
	return vehicles, total, nil
}

func vehicleSearchFilter(search string) bson.M {
	if search == "" {
		return bson.M{}
	}
	safe := safeSearch(search)
	return bson.M{"$or": bson.A{
		bson.M{"description": bson.M{"$regex": safe, "$options": "i"}},
		bson.M{"licensePlate": bson.M{"$regex": safe, "$options": "i"}},
	}}
}

func safeSearch(s string) string {
	if len(s) > maxSearchLen {
		s = s[:maxSearchLen]
	}
	return regexp.QuoteMeta(s)
}

func (r *VehicleRepository) FindByID(ctx context.Context, id primitive.ObjectID) (*Vehicle, error) {
	var vehicle Vehicle
	err := r.col.FindOne(ctx, bson.M{"_id": id}).Decode(&vehicle)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return &vehicle, err
}

func (r *VehicleRepository) Create(ctx context.Context, description, licensePlate string, capacity int, clientID primitive.ObjectID) (*Vehicle, error) {
	now := time.Now().UTC()
	vehicle := Vehicle{
		ID:           primitive.NewObjectID(),
		Description:  description,
		LicensePlate: licensePlate,
		Capacity:     capacity,
		ClientID:     clientID,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	_, err := r.col.InsertOne(ctx, vehicle)
	if err != nil {
		return nil, err
	}
	return &vehicle, nil
}

type RawMessageRepository struct {
	col *mongo.Collection
}

func NewRawMessageRepository(db *mongo.Database) *RawMessageRepository {
	return &RawMessageRepository{col: db.Collection("raw_messages")}
}

func (r *RawMessageRepository) FindByDeviceID(ctx context.Context, deviceID primitive.ObjectID) ([]RawMessage, error) {
	opts := options.Find().SetSort(bson.D{{Key: "receivedAt", Value: -1}})
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

func (r *RawMessageRepository) CountSince(ctx context.Context, d time.Duration) (int64, error) {
	return r.col.CountDocuments(ctx, bson.M{"receivedAt": bson.M{"$gte": time.Now().UTC().Add(-d)}})
}

func (r *RawMessageRepository) FindRecentPaginated(ctx context.Context, page, limit int) ([]RawMessage, int64, error) {
	total, err := r.col.CountDocuments(ctx, bson.M{})
	if err != nil {
		return nil, 0, err
	}
	opts := options.Find().
		SetSkip(int64((page - 1) * limit)).
		SetLimit(int64(limit)).
		SetSort(bson.D{{Key: "receivedAt", Value: -1}})
	cursor, err := r.col.Find(ctx, bson.M{}, opts)
	if err != nil {
		return nil, 0, err
	}
	defer cursor.Close(ctx)
	messages := make([]RawMessage, 0)
	if err := cursor.All(ctx, &messages); err != nil {
		return nil, 0, err
	}
	return messages, total, nil
}

func (r *RawMessageRepository) FindLastByDeviceID(ctx context.Context, deviceID primitive.ObjectID) (*RawMessage, error) {
	opts := options.FindOne().SetSort(bson.D{{Key: "receivedAt", Value: -1}})
	var msg RawMessage
	err := r.col.FindOne(ctx, bson.M{"deviceId": deviceID}, opts).Decode(&msg)
	if err == mongo.ErrNoDocuments {
		return nil, nil
	}
	return &msg, err
}

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
