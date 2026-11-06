package tracker

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type Publisher interface {
	Publish(ctx context.Context, queueURL string, key, value []byte) error
	Close() error
}

type SQSPublisher struct {
	client *sqs.Client
}

func NewSQSPublisher(ctx context.Context) (*SQSPublisher, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	return &SQSPublisher{client: sqs.NewFromConfig(cfg)}, nil
}

func (s *SQSPublisher) Publish(ctx context.Context, queueURL string, key, value []byte) error {
	input := &sqs.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String(string(value)),
	}

	// FIFO queues require MessageGroupId to preserve per-device ordering
	if strings.HasSuffix(queueURL, ".fifo") {
		input.MessageGroupId = aws.String(string(key))
	}

	_, err := s.client.SendMessage(ctx, input)
	return err
}

func (s *SQSPublisher) Close() error {
	return nil
}
