package queue

import (
	"async-event-dispatcher/internal/config"
	"async-event-dispatcher/internal/domain/notification"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/IBM/sarama"
)

type NotificationProcessor interface {
	ProcessNotification(ctx context.Context, notification *notification.Notification) error
}

type DLQProducer interface {
	SendToDLQ(ctx context.Context, dlqTopic string, notification *notification.Notification, reason string) error
}

type ConsumerGroupHandler struct {
	processor   NotificationProcessor
	dlqProducer DLQProducer
	dlqTopic    string
	maxRetries  int
}

func NewConsumerGroupHandler(processor NotificationProcessor, dlqProducer DLQProducer, dlqTopic string, maxRetries int) *ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		processor:   processor,
		dlqProducer: dlqProducer,
		dlqTopic:    dlqTopic,
		maxRetries:  maxRetries,
	}
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		var notification notification.Notification
		if err := json.Unmarshal(msg.Value, &notification); err != nil {
			slog.Error("Failed to unmarshal kafka message", slog.String("error", err.Error()))
			session.MarkMessage(msg, "")
			continue
		}
		slog.Info("Kafka Consumer recieved message",
			slog.String("notification_id", notification.ID),
			slog.String("user_id", notification.UserID),
		)

		err := h.processWithRetry(session.Context(), &notification)
		if err != nil {
			slog.Error("Max retries reached. Sending to DLQ...",
				slog.String("notification_id", notification.ID),
				slog.String("error", err.Error()))
		}

		if err != nil {
			slog.Error("Failed to process notification after retries",
				slog.String("id", notification.ID),
				slog.String("error", err.Error()),
			)

			if h.dlqProducer != nil {
				if dlqErr := h.dlqProducer.SendToDLQ(session.Context(), h.dlqTopic, &notification, err.Error()); dlqErr != nil {
					slog.Error("Failed to send to DLQ", slog.String("error", dlqErr.Error()))
				} else {
					slog.Warn("Message sent to DLQ successfully",
						slog.String("id", notification.ID),
						slog.String("dlq_topic", h.dlqTopic),
					)
				}
			} else {
				slog.Error("CRITICAL: dlqProducer is nil! Cannot send to DLQ",
					slog.String("id", notification.ID),
				)
			}
		}

		session.MarkMessage(msg, "")
	}
	return nil
}

func (h *ConsumerGroupHandler) processWithRetry(ctx context.Context, notification *notification.Notification) error {
	var lastErr error
	backoff := 1 * time.Second

	maxAttempts := h.maxRetries
	if maxAttempts <= 0 {
		maxAttempts = 3
	}
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := h.processor.ProcessNotification(ctx, notification)
		if err == nil {
			return nil
		}

		lastErr = err
		slog.Warn("Failed to process notification. Retrying in progress...",
			slog.String("notification_id", notification.ID),
			slog.Int("attempt", attempt),
			slog.Int("max_retries", maxAttempts),
			slog.String("error", lastErr.Error()))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
		}

	}

	if lastErr != nil {
		return fmt.Errorf("failed after %d attempts: %w", maxAttempts, lastErr)
	}

	return fmt.Errorf("Failed after %d attempts: %s", maxAttempts, lastErr)
}

type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	handler *ConsumerGroupHandler
}

func NewConsumer(cfg config.KafkaConfig, groupID string, processor NotificationProcessor, dlqProducer DLQProducer) (*Consumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(cfg.Brokers, groupID, saramaConfig)

	if err != nil {
		return nil, fmt.Errorf("Error creating consumer group %v", err)
	}

	handler := NewConsumerGroupHandler(processor, dlqProducer, cfg.DLQTopic, 3)

	return &Consumer{
		group:   group,
		topic:   cfg.Topic,
		handler: handler,
	}, nil
}

func (c *Consumer) Start(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if err := c.group.Consume(ctx, []string{c.topic}, c.handler); err != nil {
					slog.Error("Error from consumer group", slog.String("error", err.Error()))
				}
			}
		}
	}()
}

func (c *Consumer) Close() error {
	return c.group.Close()
}
