package main

import (
	"async-event-dispatcher/internal/config"
	delivery "async-event-dispatcher/internal/delivery/http"
	"async-event-dispatcher/internal/domain/notification"
	"async-event-dispatcher/internal/queue"
	"async-event-dispatcher/internal/repository/postgres"
	"async-event-dispatcher/internal/repository/redis"
	"async-event-dispatcher/internal/service"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	//cfg
	cfg := config.GetConfig()
	//logger
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	logger.Info("Starting Async Event Dispatcher by evilroky...",
		slog.String("env", cfg.Env),
		slog.String("port", cfg.HTTP.Port),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	//pg
	pgPool, err := postgres.NewPostgresPool(ctx, cfg.PG)
	if err != nil {
		slog.Error("Failed to connect to Postgres pool", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer pgPool.Close()

	slog.Info("Connected to Postgres pool")
	//redis
	redisClient, err := redis.NewRedisClient(ctx, cfg.Redis)

	if err != nil {
		slog.Error("Failed to connect to Redis client", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer redisClient.Close()

	slog.Info("Connected to Redis client")
	//kafka producer
	kafkaProducer, err := queue.NewProducer(cfg.Kafka)
	if err != nil {
		slog.Error("Failed to create kafka producer", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer kafkaProducer.Close()

	slog.Info("Connected to Kafka producer")
	//test
	notifRepo := postgres.NewNotificationRepo(pgPool)
	pgTemplateRepo := postgres.NewTemplateRepo(pgPool)

	var templateRepo notification.TemplateRepository = redis.NewCachedTemplateRepo(
		pgTemplateRepo,
		redisClient,
		10*time.Minute,
	)

	_ = templateRepo.Create(ctx, &notification.Template{
		Code:         "welcome_email",
		Channel:      notification.ChannelEmail,
		BodyTemplate: "Hello, {{}.name}! Welcome to Async event dispatcher!",
		CreatedAt:    time.Now(),
	})

	notifService := service.NewNotificationService(notifRepo, templateRepo, kafkaProducer)
	httpHandler := delivery.NewHandler(notifService)
	//kafka consumer
	kafkaConsumer, err := queue.NewConsumer(cfg.Kafka, "notification-workers", notifService, kafkaProducer)
	if err != nil {
		slog.Error("Failed to create kafka consumer", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer kafkaConsumer.Close()

	kafkaConsumer.Start(ctx)
	slog.Info("Kafka Consumer worker started!")
	//server start
	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", cfg.HTTP.Port),
		Handler:      httpHandler.InitRoutes(),
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("HTTP server is listening", slog.String("addr", server.Addr))
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("Failed to start HTTP server", slog.String("error", err.Error()))
		}
	}()
	//server shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("Shutting down server...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("Failed to shutdown server", slog.String("error", err.Error()))
	}

	slog.Info("Server gracefully shutdown")

}
