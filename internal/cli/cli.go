package cli

import (
	"log/slog"
	"os"
)

func InitLogging(level slog.Level) {
	opts := slog.HandlerOptions{
		Level: level,
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &opts))
	slog.SetDefault(logger)
}
