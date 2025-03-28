// Copyright (C) 2019-2025, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package simplex

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestMonitorDoubleClose(t *testing.T) {
	start := time.Now()
	mon := NewMonitor(start, makeLogger(t))
	require.True(t, mon.shouldRun())
	mon.Close()
	require.False(t, mon.shouldRun())
	mon.Close()
	require.False(t, mon.shouldRun())
}

func TestMonitorPrematureCancelTask(t *testing.T) {
	start := time.Now()

	ticked := make(chan struct{})

	mon := NewMonitor(start, makeLogger(t))
	mon.logger.(*testLogger).intercept(func(entry zapcore.Entry) error {
		if entry.Message == "Ticked" {
			ticked <- struct{}{}
		}
		return nil
	})

	t.Run("Cancelled task does not fire", func(t *testing.T) {
		panic := func(_ bool) {
			panic("test failed")
		}

		cancel := mon.WaitUntil(time.Hour, panic)
		cancel()

		mon.AdvanceTime(start.Add(time.Hour))

		select {
		case <-ticked:
		case <-time.After(time.Minute):
			require.FailNow(t, "timed out waiting on tick")
		}
	})

	t.Run("Non-Cancelled task fires", func(t *testing.T) {
		finish := make(chan struct{})

		mon.WaitUntil(time.Hour, func(_ bool) {
			close(finish)
		})

		mon.AdvanceTime(start.Add(time.Hour * 2))

		<-ticked
		<-finish
	})
}

func TestMonitorAsyncWaitFor(t *testing.T) {
	start := time.Now()
	mon := NewMonitor(start, makeLogger(t))

	var wg sync.WaitGroup
	wg.Add(1)
	mon.WaitFor(
		func(b bool) {
			wg.Done()
		},
	)
	wg.Wait()
}

func TestMonitorAsyncWaitUntilWithWaitFor(t *testing.T) {
	start := time.Now()
	mon := NewMonitor(start, makeLogger(t))

	var wg sync.WaitGroup
	wg.Add(1)
	mon.WaitUntil(10*time.Millisecond, func(b bool) {
		wg.Done()
	})
	mon.WaitFor(func(_ bool) {
		mon.AdvanceTime(start.Add(10 * time.Millisecond))
	},
	)
	wg.Wait()
}

func TestMonitorAsyncWaitForWithNestedWaitUntil(t *testing.T) {
	start := time.Now()
	mon := NewMonitor(start, makeLogger(t))

	var wg sync.WaitGroup
	wg.Add(1)
	mon.WaitFor(func(_ bool) {
		go mon.AdvanceTime(start.Add(10 * time.Millisecond))
		mon.WaitUntil(10*time.Millisecond, func(b bool) {
			wg.Done()
		},
		)
	},
	)
	wg.Wait()
}

type testLogger struct {
	*zap.Logger
}

func (tl *testLogger) Trace(msg string, fields ...zap.Field) {
	tl.Log(zapcore.DebugLevel, msg, fields...)
}

func (tl *testLogger) Verbo(msg string, fields ...zap.Field) {
	tl.Log(zapcore.DebugLevel, msg, fields...)
}

func (t *testLogger) intercept(hook func(entry zapcore.Entry) error) {
	logger := t.Logger.WithOptions(zap.Hooks(hook))
	t.Logger = logger
}

func makeLogger(t *testing.T) *testLogger {
	logger, err := zap.NewDevelopment()
	require.NoError(t, err)
	return &testLogger{Logger: logger}
}
