package loadtester

import (
	"fmt"
	"log"
	"os"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Logger *zap.SugaredLogger

func newLogger(logLevel zapcore.Level) (*zap.Logger, error) {

	cfg := zap.NewProductionConfig()

	cfg.Sampling = nil

	cfg.Level = zap.NewAtomicLevelAt(logLevel)

	cfg.EncoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		zapcore.RFC3339NanoTimeEncoder(t.UTC(), enc)
	}

	cfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder

	cfg.DisableStacktrace = true

	return cfg.Build()
}

func init() {
	logLevel := zapcore.InfoLevel

	if levelStr, ok := os.LookupEnv("LOG_LEVEL"); ok && levelStr != "" {
		level, err := zapcore.ParseLevel(levelStr)
		if err != nil {
			log.Fatal(fmt.Errorf("Invalid LOG_LEVEL environment variable value: %w", err))
		} else {
			logLevel = level
		}
	}

	if err := SetLogLevel(logLevel); err != nil {
		log.Fatal(fmt.Errorf("Failed to set log level: %w", err))
	}
}

func SetLogLevel(logLevel zapcore.Level) error {

	logger, err := newLogger(logLevel)
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)

	Logger = logger.Sugar()

	return nil
}
