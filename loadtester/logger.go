package loadtester

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type SugaredLogger interface {
	// Fatalw will call os.Exit(1), so use with care only when it makes sense
	Fatalw(msg string, keysAndValues ...interface{})
	Debugw(msg string, keysAndValues ...interface{})
	Warnw(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Infow(msg string, keysAndValues ...interface{})
	// Panicw will panic, so use with care
	Panicw(msg string, keysAndValues ...interface{})
}

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
