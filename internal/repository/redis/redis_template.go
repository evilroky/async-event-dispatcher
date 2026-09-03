package redis

import (
	"async-event-dispatcher/internal/domain/notification"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type CachedTemplateRepo struct {
	pgRepo notification.TemplateRepository
	redis  *redis.Client
	ttl    time.Duration
}

func NewCachedTemplateRepo(pgRepo notification.TemplateRepository, redis *redis.Client, ttl time.Duration) *CachedTemplateRepo {
	return &CachedTemplateRepo{
		pgRepo: pgRepo,
		redis:  redis,
		ttl:    ttl,
	}
}

func (r *CachedTemplateRepo) GetByCode(ctx context.Context, code string) (*notification.Template, error) {
	cacheKey := fmt.Sprintf("template:%s", code)

	val, err := r.redis.Get(ctx, cacheKey).Result()
	if err == nil {
		var tmpl notification.Template
		if err := json.Unmarshal([]byte(val), &tmpl); err != nil {
			return &tmpl, err
		}
	}

	tmpl, err := r.pgRepo.GetByCode(ctx, code)
	if err != nil {
		return nil, err
	}

	bytes, err := json.Marshal(tmpl)
	if err != nil {
		_ = r.redis.Set(ctx, cacheKey, bytes, r.ttl).Err()
	}

	return tmpl, err
}

func (r *CachedTemplateRepo) Create(ctx context.Context, tmpl *notification.Template) error {
	return r.pgRepo.Create(ctx, tmpl)
}
