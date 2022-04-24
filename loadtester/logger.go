package loadtester

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func NewLogger(logLevel zapcore.Level) (*zap.SugaredLogger, error) {

	cfg := zap.NewProductionConfig()

	cfg.Sampling = nil

	cfg.Level = zap.NewAtomicLevelAt(logLevel)

	cfg.EncoderConfig.EncodeTime = func(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
		zapcore.RFC3339NanoTimeEncoder(t.UTC(), enc)
	}

	cfg.EncoderConfig.EncodeDuration = zapcore.StringDurationEncoder

	cfg.DisableStacktrace = true

	logger, err := cfg.Build()
	if err != nil {
		return nil, err
	}

	return logger.Sugar(), nil
}
