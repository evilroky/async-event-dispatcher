package main

import (
	"async-event-dispatcher/internal/config"
	"fmt"
	"log/slog"
	"os"
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

	fmt.Println("Listening on port", cfg.HTTP.Port)
}
