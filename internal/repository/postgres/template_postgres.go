package postgres

import (
	"async-event-dispatcher/internal/domain/notification"
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TemplateRepo struct {
	db *pgxpool.Pool
}

func NewTemplateRepo(db *pgxpool.Pool) *TemplateRepo {
	return &TemplateRepo{db: db}
}

func (r *TemplateRepo) Create(ctx context.Context, t *notification.Template) error {
	query := `
			INSERT INTO templates (code, channel, body_template, created_at)
			VALUES ($1, $2, $3, $4)
			RETURNING id`

	err := r.db.QueryRow(ctx, query, t.Code, t.Channel, t.BodyTemplate, t.CreatedAt).Scan(&t.ID)
	if err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	return nil
}

func (r *TemplateRepo) GetByCode(ctx context.Context, code string) (*notification.Template, error) {
	query := `
			SELECT id, code, channel, body_template, created_at
			FROM templates
			WHERE code = $1`
	var t notification.Template
	err := r.db.QueryRow(ctx, query, code).Scan(
		&t.ID,
		&t.Code,
		&t.Channel,
		&t.BodyTemplate,
		&t.CreatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, errors.New("Template not found")
		}
		return nil, fmt.Errorf("Failde to get template by Code: %w", err)
	}
	return &t, nil
}
