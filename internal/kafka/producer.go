package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/models"
)

type Producer struct {
	writer *kafka.Writer
	logger *zap.Logger
}

func NewProducer(cfg config.KafkaConfig, logger *zap.Logger) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(cfg.Brokers...),
		Topic:        cfg.TopicChanges,
		Balancer:     &kafka.Hash{},
		BatchSize:    cfg.BatchSize,
		BatchTimeout: cfg.BatchTimeout,
		MaxAttempts:  cfg.MaxRetries,
		RequiredAcks: kafka.RequireAll,
		Async:        false,
	}

	logger.Info("kafka producer created", zap.Strings("brokers", cfg.Brokers), zap.String("topic", cfg.TopicChanges))

	return &Producer{
		writer: w,
		logger: logger,
	}
}

func (p *Producer) PublishChangeEvent(ctx context.Context, event *models.ChangeEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshaling change event: %w", err)
	}

	msg := kafka.Message{
		Key:   []byte(event.DocumentID),
		Value: data,
		Time:  time.Now(),
		Headers: []kafka.Header{
			{Key: "event_type", Value: []byte(event.Type)},
			{Key: "collection", Value: []byte(event.Collection)},
		},
	}

	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publishing change event: %w", err)
	}

	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, events []*models.ChangeEvent) error {
	msgs := make([]kafka.Message, len(events))
	for i, event := range events {
		data, err := json.Marshal(event)
		if err != nil {
			return fmt.Errorf("marshaling event %d: %w", i, err)
		}
		msgs[i] = kafka.Message{
			Key:   []byte(event.DocumentID),
			Value: data,
			Time:  time.Now(),
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(event.Type)},
				{Key: "collection", Value: []byte(event.Collection)},
			},
		}
	}

	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		return fmt.Errorf("publishing batch of %d events: %w", len(events), err)
	}

	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
