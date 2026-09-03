package service

import (
	"async-event-dispatcher/internal/domain/notification"
	"async-event-dispatcher/internal/queue"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type NotificationService struct {
	notifRepo    notification.NotificationRepository
	templateRepo notification.TemplateRepository
	producer     *queue.Producer
}

func NewNotificationService(
	notifRepo notification.NotificationRepository,
	templateRepo notification.TemplateRepository,
	producer *queue.Producer,
) *NotificationService {
	return &NotificationService{
		notifRepo:    notifRepo,
		templateRepo: templateRepo,
		producer:     producer,
	}
}

func (s *NotificationService) Send(ctx context.Context, req notification.SendNotificationRequest) (*notification.Notification, error) {
	_, err := s.templateRepo.GetByCode(ctx, req.TemplateCode)
	if err != nil {
		return nil, fmt.Errorf("Template check failed: %s", err)
	}

	payloadBytes, err := json.Marshal(req.Payload)
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal payload: %s", err)
	}

	now := time.Now()
	notif := &notification.Notification{
		ID:           uuid.New().String(),
		UserID:       req.UserID,
		TemplateCode: req.TemplateCode,
		Payload:      payloadBytes,
		Status:       notification.StatusPending,
		CreatedAt:    now,
		UpdatedAt:    now,
	}

	if err := s.notifRepo.Create(ctx, notif); err != nil {
		return nil, fmt.Errorf("Failed to save notification in database: %s", err)
	}

	if err := s.producer.PublishNotification(notif); err != nil {
		slog.Error("Failed to publish notification kafka",
			slog.String("id", notif.ID),
			slog.String("error", err.Error()),
		)

		reason := "Kafka publish error: " + err.Error()
		_ = s.notifRepo.UpdateStatus(ctx, notif.ID, notification.StatusFailed, &reason)
		return nil, fmt.Errorf("Failed to publish notification: %s", err)
	}

	return notif, nil
}

func (s *NotificationService) GetByID(ctx context.Context, id string) (*notification.Notification, error) {
	return s.notifRepo.GetByID(ctx, id)
}

func (s *NotificationService) ProcessNotification(ctx context.Context, notif *notification.Notification) error {
	return fmt.Errorf("simulated worker failure for notification %s", notif.ID)
	tmpl, err := s.templateRepo.GetByCode(ctx, notif.TemplateCode)
	if err != nil {
		reason := fmt.Sprintf("Template %s not found", notif.TemplateCode)
		_ = s.notifRepo.UpdateStatus(ctx, notif.ID, notification.StatusFailed, &reason)
		return fmt.Errorf("Failed to get template: %s", err)
	}

	slog.Info("Sending notification...",
		slog.String("id", notif.ID),
		slog.String("channel", string(tmpl.Channel)),
		slog.String("user_id", notif.UserID),
	)

	if err := s.notifRepo.UpdateStatus(ctx, notif.ID, notification.StatusSent, nil); err != nil {
		return fmt.Errorf("Failed to update notification status to sent: %s", err)
	}
	return nil
}
