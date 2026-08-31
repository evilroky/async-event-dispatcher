package postgres

import (
	"async-event-dispatcher/internal/domain/notification"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type NotificationRepo struct {
	db *pgxpool.Pool
}

func NewNotificationRepo(db *pgxpool.Pool) *NotificationRepo {
	return &NotificationRepo{db: db}
}

func (r *NotificationRepo) Create(ctx context.Context, n *notification.Notification) error {
	query := `
			INSERT INTO notifications (id, user_id, template_code, payload, status, created_at, updated_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
	`

	_, err := r.db.Exec(ctx, query,
		n.ID,
		n.UserID,
		n.TemplateCode,
		n.Payload,
		n.Status,
		n.CreatedAt,
		n.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("Failed to create notification: %w", err)
	}

	return nil
}

func (r *NotificationRepo) GetByID(ctx context.Context, id string) (*notification.Notification, error) {
	query := `
		SELECT id, user_id, template_code, payload, status, created_at, updated_at\
		FROM notifications
		WHERE id = $1`

	var n notification.Notification
	err := r.db.QueryRow(ctx, query, id).Scan(
		&n.ID,
		&n.UserID,
		&n.TemplateCode,
		&n.Payload,
		&n.Status,
		&n.ErrorReason,
		&n.CreatedAt,
		&n.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("Notification not found")
		}
		return nil, fmt.Errorf("Failed to get notification by id: %w", err)
	}
	return &n, nil
}

func (r *NotificationRepo) UpdateStatus(ctx context.Context, id string, status string, errorReason *string) error {
	query := `
		UPDATE notifications
		SET status = $1, error_reason = $2, updated_at = NOW()
		WHERE id = $3`
	_, err := r.db.Exec(ctx, query, status, errorReason, id)
	if err != nil {
		return fmt.Errorf("Failed to update notification status: %w", err)
	}

	return nil
}
