package internal

import (
	"context"
	"log"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

type SQSConsumer struct {
	client   *sqs.Client
	queueURL string
}

func NewSQSConsumer(ctx context.Context, queueURL string) (*SQSConsumer, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	opts := []func(*sqs.Options){}
	if endpoint := os.Getenv("AWS_ENDPOINT_URL"); endpoint != "" {
		opts = append(opts, func(o *sqs.Options) {
			o.BaseEndpoint = aws.String(endpoint)
		})
	}

	return &SQSConsumer{
		client:   sqs.NewFromConfig(cfg, opts...),
		queueURL: queueURL,
	}, nil
}

func (c *SQSConsumer) Run(ctx context.Context, handle func([]byte) error) {
	log.Printf("consuming from SQS queue: %s", c.queueURL)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		out, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(c.queueURL),
			MaxNumberOfMessages: 10,
			WaitTimeSeconds:     20,
		})
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("error receiving from SQS: %v", err)
			continue
		}

		for _, msg := range out.Messages {
			if err := handle([]byte(aws.ToString(msg.Body))); err != nil {
				log.Printf("error processing SQS message: %v", err)
				continue // deixa na fila para reentrega após visibility timeout
			}

			if _, err := c.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(c.queueURL),
				ReceiptHandle: msg.ReceiptHandle,
			}); err != nil {
				log.Printf("error deleting SQS message: %v", err)
			}
		}
	}
}
