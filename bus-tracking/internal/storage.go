package internal

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3Storage struct {
	client *s3.Client
	bucket string
}

func NewS3Storage(ctx context.Context, bucket string) (*S3Storage, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	opts := []func(*s3.Options){}
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true // required for LocalStack
		})
	}

	return &S3Storage{
		client: s3.NewFromConfig(cfg, opts...),
		bucket: bucket,
	}, nil
}

func (s *S3Storage) Save(ctx context.Context, msg RawMessage) (string, error) {
	key := s3KeyFor(msg)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   strings.NewReader(msg.RawPayload),
	})
	return key, err
}

func s3KeyFor(msg RawMessage) string {
	t := msg.ReceivedAt
	return fmt.Sprintf("raw/%d/%02d/%02d/%s/%s",
		t.Year(), t.Month(), t.Day(),
		msg.DeviceIdentifier,
		msg.MessageID,
	)
}
