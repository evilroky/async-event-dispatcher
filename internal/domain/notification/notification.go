package notification

import (
	"context"
	"encoding/json"
	"time"
)

const (
	StatusPending = "pending"
	StatusSent    = "sent"
	StatusFailed  = "failed"
)

type Channel string

const (
	ChannelEmail    Channel = "email"
	ChannelTelegram Channel = "telegram"
	ChannelPush     Channel = "push"
)

type Notification struct {
	ID           int             `json:"id"`
	UserID       string          `json:"user_id"`
	TemplateCode string          `json:"template_code"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	ErrorReason  *string         `json:"error_reason,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	UpdatedAt    time.Time       `json:"updated_at"`
}

type Template struct {
	ID           int       `json:"id"`
	Code         string    `json:"code"`
	Channel      Channel   `json:"channel"`
	BodyTemplate string    `json:"body_template"`
	CreatedAt    time.Time `json:"created_at"`
}

type SendNotificationRequest struct {
	UserID       string                 `json:"user_id" validate:"required"`
	TemplateCode string                 `json:"template_code" validate:"required"`
	Payload      map[string]interface{} `json:"payload"`
}

type NotificationRepository interface {
	Create(ctx context.Context, n *Notification) error
	GetByID(ctx context.Context, id string) (*Notification, error)
	UpdateStatus(ctx context.Context, id string, status string, errorReason *string) error
}

type TemplateRepository interface {
	GetByCode(ctx context.Context, code string) (*Template, error)
	Create(ctx context.Context, n *Template) error
}
