package main

import (
	"context"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"

	"admin-api/internal"
	gofrhttp "gofr.dev/pkg/gofr/http"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"gofr.dev/pkg/gofr"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("MONGO_URI is required")
	}
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")
	kafkaClientTopic := getEnv("KAFKA_CLIENT_TOPIC", "tracking.client.events")
	kafkaIAMAuth := os.Getenv("KAFKA_IAM_AUTH") == "true"

	cognitoUserPoolID := os.Getenv("COGNITO_USER_POOL_ID")
	if cognitoUserPoolID == "" {
		log.Fatal("COGNITO_USER_POOL_ID is required")
	}
	cognitoRegion := getEnv("AWS_REGION", "us-east-1")

	syncEnabled := os.Getenv("SYNC_WITH_THIRDPARTY_API") == "true"
	thirdParty := internal.NewThirdPartyClient(
		syncEnabled,
		os.Getenv("THIRDPARTY_VEHICLE_URL"),
		os.Getenv("THIRDPARTY_DEVICE_URL"),
	)

	db, err := internal.ConnectMongo(mongoURI)
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}

	clientRepo := internal.NewClientRepository(db)
	vehicleRepo := internal.NewVehicleRepository(db)
	deviceRepo := internal.NewDeviceRepository(db)
	rawMsgRepo := internal.NewRawMessageRepository(db)

	setupCtx := context.Background()
	publisher, err := internal.NewEventPublisher(setupCtx, kafkaBroker, kafkaClientTopic, kafkaIAMAuth)
	if err != nil {
		log.Fatalf("failed to create kafka publisher: %v", err)
	}
	replaySvc := internal.NewReplayService(deviceRepo, vehicleRepo, rawMsgRepo, publisher)

	if err := clientRepo.EnsureIndexes(setupCtx); err != nil {
		log.Printf("warning: client indexes: %v", err)
	}

	app := gofr.New()
	app.UseMiddleware(internal.NewCognitoValidator(cognitoRegion, cognitoUserPoolID).Middleware())

	// GET /clients
	app.GET("/clients", func(c *gofr.Context) (interface{}, error) {
		clients, err := clientRepo.FindAll(c.Context)
		if err != nil {
			return nil, err
		}
		return clients, nil
	})

	// POST /clients  body: {"description":"...","globalId":"..."}
	app.POST("/clients", func(c *gofr.Context) (interface{}, error) {
		var body struct {
			Description string `json:"description"`
			GlobalID    string `json:"globalId"`
		}
		if err := c.Bind(&body); err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"body"}}
		}
		client, err := clientRepo.Create(c.Context, body.Description, body.GlobalID)
		if err != nil {
			return nil, err
		}
		return client, nil
	})

	// GET /vehicles?search=&page=1&limit=20
	app.GET("/vehicles", func(c *gofr.Context) (interface{}, error) {
		page, limit := parsePage(c)
		vehicles, total, err := vehicleRepo.FindPaginated(c.Context, c.Param("search"), page, limit)
		if err != nil {
			return nil, err
		}
		return pageOf(vehicles, total, page, limit), nil
	})

	// POST /vehicles  body: {"description":"...","licensePlate":"...","capacity":N,"clientId":"..."}
	app.POST("/vehicles", func(c *gofr.Context) (interface{}, error) {
		var body struct {
			Description  string `json:"description"`
			LicensePlate string `json:"licensePlate"`
			Capacity     int    `json:"capacity"`
			ClientID     string `json:"clientId"`
		}
		if err := c.Bind(&body); err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"body"}}
		}
		clientID, err := primitive.ObjectIDFromHex(body.ClientID)
		if err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"clientId"}}
		}
		vehicle, err := vehicleRepo.Create(c.Context, body.Description, body.LicensePlate, body.Capacity, clientID)
		if err != nil {
			return nil, err
		}
		thirdParty.SyncVehicle(c.Context, vehicle)
		return vehicle, nil
	})

	// GET /devices?search=&page=1&limit=20
	app.GET("/devices", func(c *gofr.Context) (interface{}, error) {
		page, limit := parsePage(c)
		devices, total, err := deviceRepo.FindPaginated(c.Context, c.Param("search"), false, page, limit)
		if err != nil {
			return nil, err
		}
		return pageOf(devices, total, page, limit), nil
	})

	// GET /devices/unassigned?search=&page=1&limit=20
	app.GET("/devices/unassigned", func(c *gofr.Context) (interface{}, error) {
		page, limit := parsePage(c)
		devices, total, err := deviceRepo.FindPaginated(c.Context, c.Param("search"), true, page, limit)
		if err != nil {
			return nil, err
		}
		return pageOf(devices, total, page, limit), nil
	})

	// GET /devices/{id}/last-position
	app.GET("/devices/{id}/last-position", func(c *gofr.Context) (interface{}, error) {
		deviceID, err := primitive.ObjectIDFromHex(c.PathParam("id"))
		if err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"id"}}
		}
		device, err := deviceRepo.FindByID(c.Context, deviceID)
		if err != nil {
			return nil, err
		}
		if device == nil {
			return nil, gofrhttp.ErrorEntityNotFound{Name: "device", Value: c.PathParam("id")}
		}
		msg, err := rawMsgRepo.FindLastByDeviceID(c.Context, deviceID)
		if err != nil {
			return nil, err
		}
		if msg == nil {
			return nil, nil
		}
		pos, ok := internal.ParsePosition(deviceID.Hex(), msg.RawPayload)
		if !ok {
			return nil, nil
		}
		return pos, nil
	})

	// GET /devices/{id}/messages
	app.GET("/devices/{id}/messages", func(c *gofr.Context) (interface{}, error) {
		id, err := primitive.ObjectIDFromHex(c.PathParam("id"))
		if err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"id"}}
		}
		messages, err := rawMsgRepo.FindByDeviceID(c.Context, id)
		if err != nil {
			return nil, err
		}
		return messages, nil
	})

	// GET /messages/stats
	app.GET("/messages/stats", func(c *gofr.Context) (interface{}, error) {
		last24h, err := rawMsgRepo.CountSince(c.Context, 24*time.Hour)
		if err != nil {
			return nil, err
		}
		return map[string]int64{"last24h": last24h}, nil
	})

	// GET /messages?page=1&limit=20
	app.GET("/messages", func(c *gofr.Context) (interface{}, error) {
		page, limit := parsePage(c)
		messages, total, err := rawMsgRepo.FindRecentPaginated(c.Context, page, limit)
		if err != nil {
			return nil, err
		}
		return pageOf(messages, total, page, limit), nil
	})

	// POST /devices/{id}/associate-vehicle  body: {"vehicleId":"..."}
	app.POST("/devices/{id}/associate-vehicle", func(c *gofr.Context) (interface{}, error) {
		deviceID, err := primitive.ObjectIDFromHex(c.PathParam("id"))
		if err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"id"}}
		}
		var body struct {
			VehicleID string `json:"vehicleId"`
		}
		if err := c.Bind(&body); err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"body"}}
		}
		vehicleID, err := primitive.ObjectIDFromHex(body.VehicleID)
		if err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"vehicleId"}}
		}
		if err := deviceRepo.AssociateVehicle(c.Context, deviceID, vehicleID); err != nil {
			return nil, err
		}
		return map[string]string{"status": "associated"}, nil
	})

	// POST /devices/{id}/replay
	app.POST("/devices/{id}/replay", func(c *gofr.Context) (interface{}, error) {
		deviceID, err := primitive.ObjectIDFromHex(c.PathParam("id"))
		if err != nil {
			return nil, gofrhttp.ErrorInvalidParam{Params: []string{"id"}}
		}
		if err := replaySvc.TriggerReplay(deviceID); err != nil {
			switch err {
			case internal.ErrNotFound:
				return nil, gofrhttp.ErrorEntityNotFound{Name: "device", Value: deviceID.Hex()}
			case internal.ErrDeviceNotAssociated:
				return nil, &statusError{code: http.StatusUnprocessableEntity, msg: err.Error()}
			}
			return nil, err
		}
		return map[string]string{"status": "replay started"}, nil
	})

	app.Run()
}

type pageResult struct {
	Items interface{} `json:"items"`
	Total int64       `json:"total"`
	Page  int         `json:"page"`
	Limit int         `json:"limit"`
	Pages int         `json:"pages"`
}

func pageOf(items interface{}, total int64, page, limit int) pageResult {
	pages := int(math.Ceil(float64(total) / float64(limit)))
	return pageResult{Items: items, Total: total, Page: page, Limit: limit, Pages: pages}
}

func parsePage(c *gofr.Context) (page, limit int) {
	page, limit = 1, 20
	if p, err := strconv.Atoi(c.Param("page")); err == nil && p > 0 {
		page = p
	}
	if l, err := strconv.Atoi(c.Param("limit")); err == nil && l > 0 && l <= 500 {
		limit = l
	}
	return
}

// statusError is an error that carries an HTTP status code for GoFR.
type statusError struct {
	code int
	msg  string
}

func (e *statusError) Error() string   { return e.msg }
func (e *statusError) StatusCode() int { return e.code }
