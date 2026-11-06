package internal

import (
	"context"
	"crypto/tls"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/aws_msk_iam_v2"
)

type EventPublisher struct {
	writer    *kafka.Writer
	transport *kafka.Transport
	topic     string
}

// NewEventPublisher creates a Kafka producer. When useIAM is true, it
// authenticates to the broker via AWS_MSK_IAM using the ambient AWS
// credentials (ECS task role); otherwise it connects in plaintext, which
// only the local dev Kafka container accepts.
func NewEventPublisher(ctx context.Context, brokers, topic string, useIAM bool) (*EventPublisher, error) {
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(brokers, ",")...),
		Balancer:               &kafka.Hash{},
		RequiredAcks:           kafka.RequireOne,
		AllowAutoTopicCreation: true,
	}

	var transport *kafka.Transport
	if useIAM {
		cfg, err := awsconfig.LoadDefaultConfig(ctx)
		if err != nil {
			return nil, err
		}
		transport = &kafka.Transport{
			SASL: aws_msk_iam_v2.NewMechanism(cfg),
			TLS:  &tls.Config{},
		}
		writer.Transport = transport
	}

	return &EventPublisher{topic: topic, writer: writer, transport: transport}, nil
}

func (p *EventPublisher) Publish(ctx context.Context, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafka.Message{
		Topic: p.topic,
		Key:   []byte(key),
		Value: value,
	})
}

// Close shuts down the writer. kafka.Writer.Close only tears down the
// transport created internally by kafka.NewWriter; since useIAM wires in a
// custom *kafka.Transport for MSK IAM auth, its pooled connections have to
// be closed here explicitly or they leak.
func (p *EventPublisher) Close() error {
	err := p.writer.Close()
	if p.transport != nil {
		p.transport.CloseIdleConnections()
	}
	return err
}
