package main

import (
	"async-event-dispatcher/internal/config"
	"async-event-dispatcher/internal/queue"
	"async-event-dispatcher/internal/repository/postgres"
	"async-event-dispatcher/internal/repository/redis"
	"context"
	"log/slog"
	"os"
	"time"
)

func main() {
	cfg := config.GetConfig()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	logger.Info("Starting Async Event Dispatcher by evilroky...",
		slog.String("env", cfg.Env),
		slog.String("port", cfg.HTTP.Port),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	pgPool, err := postgres.NewPostgresPool(ctx, cfg.PG)
	if err != nil {
		slog.Error("Failed to connect to Postgres pool", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer pgPool.Close()

	slog.Info("Connected to Postgres pool")

	redisClient, err := redis.NewRedisClient(ctx, cfg.Redis)

	if err != nil {
		slog.Error("Failed to connect to Redis client", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer redisClient.Close()

	slog.Info("Connected to Redis client")

	kafkaProducer, err := queue.NewProducer(cfg.Kafka)
	if err != nil {
		slog.Error("Failed to create kafka producer", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer kafkaProducer.Close()

	slog.Info("Connected to Kafka producer")

}
