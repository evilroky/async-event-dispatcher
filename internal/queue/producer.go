package queue

import (
	"async-event-dispatcher/internal/config"
	"async-event-dispatcher/internal/domain/notification"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/IBM/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

func NewProducer(cfg config.KafkaConfig) (*Producer, error) {

	saramaCfg := sarama.NewConfig()

	saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	saramaCfg.Producer.Retry.Max = 5
	saramaCfg.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("Failed to start sarama producer: %s", err)
	}

	return &Producer{
		producer: producer,
		topic:    cfg.Topic,
	}, nil
}

func (p *Producer) PublishNotification(event *notification.Notification) error {

	bytes, err := json.Marshal(event)

	if err != nil {
		return fmt.Errorf("Failed to marshal notification event: %s", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(event.UserID),
		Value: sarama.ByteEncoder(bytes),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("Failed to send message to kafka %d: %s", partition, err)
	}

	slog.Debug("Message sent to kafka %d",
		slog.String("topic", p.topic),
		slog.Int("partition", int(partition)),
		slog.Int64("offset", offset),
		slog.String("notifications_id", event.ID),
	)

	return nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func (p *Producer) SendToDLQ(ctx context.Context, dlqTopic string, notification *notification.Notification, reason string) error {
	payload, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("Failed to marshal notification event for DLQ: %s", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: dlqTopic,
		Key:   sarama.StringEncoder(notification.ID),
		Value: sarama.ByteEncoder(payload),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("error_reason"),
				Value: []byte(reason),
			},
		},
	}

	_, _, err = p.producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("Failed to send message to DLQ: %s", err)
	}

	slog.Warn("Message sent to DLQ",
		slog.String("notifications_id", notification.ID),
		slog.String("dlq_topic", dlqTopic),
		slog.String("reason", reason),
	)
	return nil
}
