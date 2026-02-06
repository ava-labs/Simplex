// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
)

type TestLogger struct {
	*zap.Logger
	t                  *testing.T
	traceVerboseLogger *zap.Logger
	panicOnError       bool
	panicOnWarn        bool
	atomicLevel        zap.AtomicLevel
}

// keywordFilterCore is a zapcore.Core wrapper that only logs entries whose
// message contains at least one of the configured keywords.
type keywordFilterCore struct {
	zapcore.Core
	keywords []string
}

func (k keywordFilterCore) Check(ent zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	for _, kw := range k.keywords {
		if strings.Contains(ent.Message, kw) {
			return k.Core.Check(ent, ce)
		}
	}
	// If no keyword matches, drop the entry by returning ce unchanged.
	return ce
}

func (tl *TestLogger) Intercept(hook func(entry zapcore.Entry) error) {
	logger := tl.Logger.WithOptions(zap.Hooks(hook))
	tl.Logger = logger
}

func (tl *TestLogger) Silence() {
	tl.atomicLevel.SetLevel(zapcore.FatalLevel)
}

// SilenceExceptKeywords silences all logs EXCEPT those whose message contains
// at least one of the provided keywords.
func (tl *TestLogger) SilenceExceptKeywords(keywords ...string) {
	core := tl.Logger.Core()
	filteredCore := keywordFilterCore{
		Core:     core,
		keywords: keywords,
	}
	tl.Logger = zap.New(filteredCore, zap.AddCaller())
	tl.traceVerboseLogger = zap.New(filteredCore, zap.AddCaller())
}

func (tl *TestLogger) SetPanicOnError(panicOnError bool) {
	tl.panicOnError = panicOnError
}

func (tl *TestLogger) SetPanicOnWarn(panicOnWarn bool) {
	tl.panicOnWarn = panicOnWarn
}

func (tl *TestLogger) Trace(msg string, fields ...zap.Field) {
	tl.traceVerboseLogger.Log(zapcore.DebugLevel, msg, fields...)
}

func (tl *TestLogger) Verbo(msg string, fields ...zap.Field) {
	tl.traceVerboseLogger.Log(zapcore.DebugLevel, msg, fields...)
}

func (tl *TestLogger) Warn(msg string, fields ...zap.Field) {
	tl.Logger.Warn(msg, fields...)
	if tl.panicOnWarn {
		panicMsg := fmt.Sprintf("WARN during test %s: %s", tl.t.Name(), msg)
		panic(panicMsg)
	}
}

func (tl *TestLogger) Error(msg string, fields ...zap.Field) {
	tl.Logger.Error(msg, fields...)
	if tl.panicOnError {
		panicMsg := fmt.Sprintf("ERROR during test %s: %s", tl.t.Name(), msg)
		panic(panicMsg)
	}
}

func MakeLogger(t *testing.T, node ...int) *TestLogger {
	return MakeLoggerWithFile(t, nil, node...)
}

// MakeLoggerWithFile creates a TestLogger that optionally writes to a file in addition to stdout.
// If fileWriter is nil, logs only to stdout (same as MakeLogger).
// If fileWriter is provided, logs to both stdout and the file.
func MakeLoggerWithFile(t *testing.T, fileWriter zapcore.WriteSyncer, node ...int) *TestLogger {
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

	// Create stdout encoder
	stdoutEncoder := zapcore.NewConsoleEncoder(config)
	if strings.ToLower(os.Getenv("LOG_LEVEL")) == "info" {
		stdoutEncoder = &DebugSwallowingEncoder{consoleEncoder: stdoutEncoder, ObjectEncoder: stdoutEncoder, pool: buffer.NewPool()}
	}

	atomicLevel := zap.NewAtomicLevelAt(zapcore.DebugLevel)

	// Create stdout core
	stdoutCore := zapcore.NewCore(stdoutEncoder, zapcore.AddSync(os.Stdout), atomicLevel)

	// If file writer is provided, create a tee core with both stdout and file
	var core zapcore.Core
	if fileWriter != nil {
		fileEncoder := zapcore.NewConsoleEncoder(config)
		if strings.ToLower(os.Getenv("LOG_LEVEL")) == "info" {
			fileEncoder = &DebugSwallowingEncoder{consoleEncoder: fileEncoder, ObjectEncoder: fileEncoder, pool: buffer.NewPool()}
		}
		fileCore := zapcore.NewCore(fileEncoder, fileWriter, atomicLevel)
		core = zapcore.NewTee(stdoutCore, fileCore)
	} else {
		core = stdoutCore
	}

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

	l := &TestLogger{t: t, Logger: logger, traceVerboseLogger: traceVerboseLogger,
		atomicLevel: atomicLevel,
	}

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

func (tl *TestLogger) SetLevel(level zapcore.Level) {
	tl.atomicLevel.SetLevel(level)
}
