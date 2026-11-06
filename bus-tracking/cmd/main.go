package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"bus-tracking/internal"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func getEnv(key, fallback string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return fallback
}

func main() {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		log.Fatal("MONGO_URI is required")
	}
	sqsQueueURL := os.Getenv("SQS_QUEUE_URL")
	kafkaBroker := getEnv("KAFKA_BROKER", "kafka:9092")
	kafkaClientTopic := getEnv("KAFKA_CLIENT_TOPIC", "tracking.client.events")
	kafkaIAMAuth := os.Getenv("KAFKA_IAM_AUTH") == "true"

	if sqsQueueURL == "" {
		log.Fatal("SQS_QUEUE_URL is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	mongoClient, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatalf("failed to connect to MongoDB: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	db := mongoClient.Database("tracking")

	deviceRepo := internal.NewDeviceRepository(db)
	vehicleRepo := internal.NewVehicleRepository(db)
	rawMsgRepo := internal.NewRawMessageRepository(db)

	if err := deviceRepo.EnsureIndexes(ctx); err != nil {
		log.Printf("warning: failed to ensure device indexes: %v", err)
	}
	if err := rawMsgRepo.EnsureIndexes(ctx); err != nil {
		log.Printf("warning: failed to ensure raw_messages indexes: %v", err)
	}

	publisher, err := internal.NewEventPublisher(ctx, kafkaBroker, kafkaClientTopic, kafkaIAMAuth)
	if err != nil {
		log.Fatalf("failed to create kafka publisher: %v", err)
	}
	defer publisher.Close()

	var storage *internal.S3Storage
	if bucket := os.Getenv("S3_BUCKET"); bucket != "" {
		s, err := internal.NewS3Storage(ctx, bucket)
		if err != nil {
			log.Fatalf("failed to create S3 storage: %v", err)
		}
		storage = s
		log.Printf("S3 storage initialized bucket=%s", bucket)
	} else {
		log.Println("warning: S3_BUCKET not set — raw payloads will not be saved to S3")
	}

	processor := internal.NewProcessor(deviceRepo, vehicleRepo, rawMsgRepo, publisher, storage)

	consumer, err := internal.NewSQSConsumer(ctx, sqsQueueURL)
	if err != nil {
		log.Fatalf("failed to create SQS consumer: %v", err)
	}

	consumer.Run(ctx, func(body []byte) error {
		var event internal.RawEvent
		if err := json.Unmarshal(body, &event); err != nil {
			log.Printf("failed to unmarshal event: %v", err)
			return err
		}
		processor.Process(context.Background(), event)
		return nil
	})

	log.Println("shutting down")
}