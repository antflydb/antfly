// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package termite

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestRequestQueueRejectWhenBusyDoesNotRetainWork(t *testing.T) {
	queue := NewRequestQueue(RequestQueueConfig{
		MaxConcurrentRequests: 1,
		MaxQueueSize:          100,
		RejectWhenBusy:        true,
	}, zap.NewNop())

	release, err := queue.Acquire(context.Background())
	require.NoError(t, err)
	defer release()

	_, err = queue.Acquire(context.Background())
	require.ErrorIs(t, err, ErrQueueFull)
	stats := queue.Stats()
	require.Zero(t, stats.CurrentQueued)
	require.Equal(t, int64(1), stats.TotalRejected)
}
