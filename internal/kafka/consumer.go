package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"

	"github.com/shubhsaxena/high-scale-search/internal/config"
	"github.com/shubhsaxena/high-scale-search/internal/models"
	"github.com/shubhsaxena/high-scale-search/internal/observability"
)

type MessageHandler func(ctx context.Context, event *models.ChangeEvent) error

type Consumer struct {
	reader     *kafka.Reader
	dlqWriter  *kafka.Writer
	handler    MessageHandler
	cfg        config.KafkaConfig
	logger     *zap.Logger
	wg         sync.WaitGroup
	cancelFunc context.CancelFunc
}

func NewConsumer(cfg config.KafkaConfig, handler MessageHandler, logger *zap.Logger) *Consumer {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        cfg.Brokers,
		Topic:          cfg.TopicChanges,
		GroupID:        cfg.ConsumerGroup,
		MinBytes:       1e3,  // 1KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        500 * time.Millisecond,
		CommitInterval: time.Second,
		StartOffset:    kafka.LastOffset,
	})

	dlqWriter := &kafka.Writer{
		Addr:     kafka.TCP(cfg.Brokers...),
		Topic:    cfg.TopicDLQ,
		Balancer: &kafka.Hash{},
	}

	logger.Info("kafka consumer created",
		zap.Strings("brokers", cfg.Brokers),
		zap.String("topic", cfg.TopicChanges),
		zap.String("group", cfg.ConsumerGroup),
	)

	return &Consumer{
		reader:    reader,
		dlqWriter: dlqWriter,
		handler:   handler,
		cfg:       cfg,
		logger:    logger,
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	c.cancelFunc = cancel

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumeLoop(ctx)
	}()

	c.logger.Info("kafka consumer started")
	return nil
}

func (c *Consumer) consumeLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("kafka consumer shutting down")
			return
		default:
		}

		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("fetching kafka message", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		c.processMessage(ctx, msg)
	}
}

func (c *Consumer) processMessage(ctx context.Context, msg kafka.Message) {
	start := time.Now()

	var event models.ChangeEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		c.logger.Error("unmarshaling kafka message",
			zap.Error(err),
			zap.Int64("offset", msg.Offset),
			zap.Int("partition", msg.Partition),
		)
		c.sendToDLQ(ctx, msg, fmt.Sprintf("unmarshal error: %v", err))
		c.commitMessage(ctx, msg)
		return
	}

	// Track indexing lag
	lag := time.Since(event.Timestamp)
	observability.IndexingLag.Set(lag.Seconds())

	var lastErr error
	for attempt := 0; attempt < c.cfg.MaxRetries; attempt++ {
		if err := c.handler(ctx, &event); err != nil {
			lastErr = err
			c.logger.Warn("handler error, retrying",
				zap.Error(err),
				zap.Int("attempt", attempt+1),
				zap.String("doc_id", event.DocumentID),
			)
			backoff := time.Duration(1<<uint(attempt)) * 100 * time.Millisecond
			time.Sleep(backoff)
			continue
		}
		lastErr = nil
		break
	}

	if lastErr != nil {
		c.logger.Error("handler failed after retries, sending to DLQ",
			zap.Error(lastErr),
			zap.String("doc_id", event.DocumentID),
		)
		observability.IndexingEventsTotal.WithLabelValues(event.Type, "dlq").Inc()
		c.sendToDLQ(ctx, msg, fmt.Sprintf("handler error after retries: %v", lastErr))
	} else {
		observability.IndexingEventsTotal.WithLabelValues(event.Type, "success").Inc()
	}

	c.commitMessage(ctx, msg)

	duration := time.Since(start)
	c.logger.Debug("message processed",
		zap.String("doc_id", event.DocumentID),
		zap.Duration("duration", duration),
	)
}

func (c *Consumer) sendToDLQ(ctx context.Context, msg kafka.Message, reason string) {
	dlqMsg := kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
		Headers: append(msg.Headers,
			kafka.Header{Key: "dlq_reason", Value: []byte(reason)},
			kafka.Header{Key: "original_topic", Value: []byte(c.cfg.TopicChanges)},
			kafka.Header{Key: "original_partition", Value: []byte(fmt.Sprintf("%d", msg.Partition))},
			kafka.Header{Key: "original_offset", Value: []byte(fmt.Sprintf("%d", msg.Offset))},
		),
	}

	if err := c.dlqWriter.WriteMessages(ctx, dlqMsg); err != nil {
		c.logger.Error("failed to send to DLQ",
			zap.Error(err),
			zap.Int64("offset", msg.Offset),
		)
	}
}

func (c *Consumer) commitMessage(ctx context.Context, msg kafka.Message) {
	if err := c.reader.CommitMessages(ctx, msg); err != nil {
		c.logger.Error("committing kafka message",
			zap.Error(err),
			zap.Int64("offset", msg.Offset),
		)
	}
}

func (c *Consumer) HealthCheck(ctx context.Context) error {
	conn, err := kafka.DialContext(ctx, "tcp", c.cfg.Brokers[0])
	if err != nil {
		return fmt.Errorf("kafka health check dial: %w", err)
	}
	defer conn.Close()

	_, err = conn.Brokers()
	if err != nil {
		return fmt.Errorf("kafka health check brokers: %w", err)
	}
	return nil
}

func (c *Consumer) Stop() error {
	if c.cancelFunc != nil {
		c.cancelFunc()
	}
	c.wg.Wait()

	var errs []error
	if err := c.reader.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing reader: %w", err))
	}
	if err := c.dlqWriter.Close(); err != nil {
		errs = append(errs, fmt.Errorf("closing dlq writer: %w", err))
	}

	if len(errs) > 0 {
		return fmt.Errorf("consumer close errors: %v", errs)
	}
	return nil
}
