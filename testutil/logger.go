// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"os"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

type TestLogger struct {
	*zap.Logger
	traceVerboseLogger *zap.Logger
}

func (t *TestLogger) Intercept(hook func(entry zapcore.Entry) error) {
	logger := t.Logger.WithOptions(zap.Hooks(hook))
	t.Logger = logger
}

func (t *TestLogger) Silence() {
	atomicLevel := zap.NewAtomicLevelAt(zapcore.FatalLevel)
	core := t.Logger.Core()
	t.Logger = zap.New(core, zap.AddCaller(), zap.IncreaseLevel(atomicLevel))
	t.traceVerboseLogger = zap.New(core, zap.AddCaller(), zap.IncreaseLevel(atomicLevel))
}

func (tl *TestLogger) Trace(msg string, fields ...zap.Field) {
	tl.traceVerboseLogger.Log(zapcore.DebugLevel, msg, fields...)
}

func (tl *TestLogger) Verbo(msg string, fields ...zap.Field) {
	tl.traceVerboseLogger.Log(zapcore.DebugLevel, msg, fields...)
}

func MakeLogger(t *testing.T, node ...int) *TestLogger {
	defaultEncoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "msg",
		StacktraceKey:  "stacktrace",
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}
	config := defaultEncoderConfig
	config.EncodeLevel = func(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
		enc.AppendString(strings.ToUpper(l.String()))
	}
	config.EncodeTime = zapcore.TimeEncoderOfLayout("[01-02|15:04:05.000]")
	config.ConsoleSeparator = " "
	encoder := zapcore.NewConsoleEncoder(config)
	if strings.ToLower(os.Getenv("LOG_LEVEL")) == "info" {
		encoder = &DebugSwallowingEncoder{consoleEncoder: encoder, ObjectEncoder: encoder, pool: buffer.NewPool()}
	}

	atomicLevel := zap.NewAtomicLevelAt(zapcore.DebugLevel)

	core := zapcore.NewCore(encoder, zapcore.AddSync(os.Stdout), atomicLevel)

	logger := zap.New(core, zap.AddCaller())
	logger = logger.With(zap.String("test", t.Name()))
	if len(node) > 0 {
		logger = logger.With(zap.Int("myNodeID", node[0]))
	}

	traceVerboseLogger := zap.New(core, zap.AddCaller(), zap.AddCallerSkip(1))
	traceVerboseLogger = traceVerboseLogger.With(zap.String("test", t.Name()))

	if len(node) > 0 {
		traceVerboseLogger = traceVerboseLogger.With(zap.Int("myNodeID", node[0]))
	}

	l := &TestLogger{Logger: logger, traceVerboseLogger: traceVerboseLogger}

	return l
}

type DebugSwallowingEncoder struct {
	zapcore.ObjectEncoder
	consoleEncoder zapcore.Encoder
	pool           buffer.Pool
}

func (dse *DebugSwallowingEncoder) Clone() zapcore.Encoder {
	return &DebugSwallowingEncoder{
		pool:           dse.pool,
		ObjectEncoder:  dse.ObjectEncoder,
		consoleEncoder: dse.consoleEncoder.Clone(),
	}
}

func (dse *DebugSwallowingEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	if entry.Level == zapcore.DebugLevel {
		return dse.pool.Get(), nil
	}
	return dse.consoleEncoder.EncodeEntry(entry, fields)
}
