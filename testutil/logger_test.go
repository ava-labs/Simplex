// Copyright (C) 2019-2024, Ava Labs, Inc. All rights reserved.
// See the file LICENSE for licensing terms.

package testutil

import (
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"testing"
)

func TestSilence(t *testing.T) {
	l1 := MakeLogger(t)
	l2 := MakeLogger(t)

	l1.Silence()

	l1.Intercept(func(entry zapcore.Entry) error {
		t.Fatal("shouldn't be logged")
		return nil
	})

	var c int

	l2.Intercept(func(entry zapcore.Entry) error {
		c++
		return nil
	})

	l1.Debug("Debug message")
	l1.Info("Info message")

	l2.Debug("Debug message")
	l2.Info("Info message")

	require.Equal(t, 2, c)
}
