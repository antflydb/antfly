// Copyright 2025 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package termite

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var (
	// ErrQueueFull is returned when the request queue is at capacity
	ErrQueueFull = errors.New("request queue is full")

	// ErrRequestTimeout is returned when a request exceeds the timeout
	ErrRequestTimeout = errors.New("request timeout exceeded")
)

// RequestQueue manages concurrent request limiting and queuing with backpressure
type RequestQueue struct {
	maxConcurrent int64         // Max concurrent requests (0 = unlimited)
	maxQueueSize  int64         // Max queued requests (0 = unlimited)
	timeout       time.Duration // Request timeout (0 = no timeout)

	// Semaphore channel for concurrency control
	sem chan struct{}

	// Metrics
	currentActive  atomic.Int64 // Currently processing
	currentQueued  atomic.Int64 // Currently waiting in queue
	totalProcessed atomic.Int64 // Total requests processed
	totalRejected  atomic.Int64 // Total requests rejected (queue full)
	totalTimedOut  atomic.Int64 // Total requests timed out

	logger *zap.Logger
}

// RequestQueueConfig holds configuration for the request queue
type RequestQueueConfig struct {
	MaxConcurrentRequests int           // 0 = unlimited
	MaxQueueSize          int           // 0 = unlimited (only when MaxConcurrent > 0)
	RequestTimeout        time.Duration // 0 = no timeout
}

// NewRequestQueue creates a new request queue with the given configuration
func NewRequestQueue(config RequestQueueConfig, logger *zap.Logger) *RequestQueue {
	if logger == nil {
		logger = zap.NewNop()
	}

	q := &RequestQueue{
		maxConcurrent: int64(config.MaxConcurrentRequests),
		maxQueueSize:  int64(config.MaxQueueSize),
		timeout:       config.RequestTimeout,
		logger:        logger,
	}

	// Only create semaphore if concurrency limiting is enabled
	if config.MaxConcurrentRequests > 0 {
		q.sem = make(chan struct{}, config.MaxConcurrentRequests)
		logger.Info("Request queue initialized",
			zap.Int("max_concurrent", config.MaxConcurrentRequests),
			zap.Int("max_queue_size", config.MaxQueueSize),
			zap.Duration("timeout", config.RequestTimeout))
	} else {
		logger.Info("Request queue disabled (unlimited concurrency)")
	}

	return q
}

// Acquire attempts to acquire a slot for processing a request.
// Returns a release function that must be called when the request is done.
// Returns an error if the queue is full or the context is cancelled.
func (q *RequestQueue) Acquire(ctx context.Context) (release func(), err error) {
	// If no concurrency limit, just track metrics
	if q.sem == nil {
		q.currentActive.Add(1)
		return func() {
			q.currentActive.Add(-1)
			q.totalProcessed.Add(1)
		}, nil
	}

	// Apply timeout if configured
	if q.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, q.timeout)
		defer func() {
			if err != nil {
				cancel()
			}
		}()
		// Note: we don't defer cancel() for success case because
		// the release function handles cleanup
	}

	// Check if we can acquire immediately (non-blocking)
	select {
	case q.sem <- struct{}{}:
		// Got a slot immediately
		q.currentActive.Add(1)
		return q.makeRelease(), nil
	default:
		// Need to queue
	}

	// Check queue capacity before waiting
	if q.maxQueueSize > 0 {
		queued := q.currentQueued.Load()
		if queued >= q.maxQueueSize {
			q.totalRejected.Add(1)
			q.logger.Warn("Request rejected: queue full",
				zap.Int64("queued", queued),
				zap.Int64("max_queue", q.maxQueueSize))
			return nil, ErrQueueFull
		}
	}

	// Enter queue
	q.currentQueued.Add(1)
	queueStart := time.Now()

	q.logger.Debug("Request queued",
		zap.Int64("queue_depth", q.currentQueued.Load()))

	// Wait for a slot
	select {
	case q.sem <- struct{}{}:
		// Got a slot
		q.currentQueued.Add(-1)
		q.currentActive.Add(1)
		q.logger.Debug("Request dequeued",
			zap.Duration("wait_time", time.Since(queueStart)))
		return q.makeRelease(), nil

	case <-ctx.Done():
		// Context cancelled or timed out
		q.currentQueued.Add(-1)
		if ctx.Err() == context.DeadlineExceeded {
			q.totalTimedOut.Add(1)
			q.logger.Warn("Request timed out in queue",
				zap.Duration("wait_time", time.Since(queueStart)),
				zap.Duration("timeout", q.timeout))
			return nil, ErrRequestTimeout
		}
		return nil, ctx.Err()
	}
}

// makeRelease creates a release function for a successfully acquired slot
func (q *RequestQueue) makeRelease() func() {
	return func() {
		q.currentActive.Add(-1)
		q.totalProcessed.Add(1)
		<-q.sem
	}
}

// Stats returns current queue statistics
func (q *RequestQueue) Stats() QueueStats {
	return QueueStats{
		CurrentActive:  q.currentActive.Load(),
		CurrentQueued:  q.currentQueued.Load(),
		TotalProcessed: q.totalProcessed.Load(),
		TotalRejected:  q.totalRejected.Load(),
		TotalTimedOut:  q.totalTimedOut.Load(),
		MaxConcurrent:  q.maxConcurrent,
		MaxQueueSize:   q.maxQueueSize,
	}
}

// QueueStats holds queue statistics
type QueueStats struct {
	CurrentActive  int64 `json:"current_active"`
	CurrentQueued  int64 `json:"current_queued"`
	TotalProcessed int64 `json:"total_processed"`
	TotalRejected  int64 `json:"total_rejected"`
	TotalTimedOut  int64 `json:"total_timed_out"`
	MaxConcurrent  int64 `json:"max_concurrent"`
	MaxQueueSize   int64 `json:"max_queue_size"`
}

// IsEnabled returns true if request queuing is enabled
func (q *RequestQueue) IsEnabled() bool {
	return q.sem != nil
}

// WriteQueueFullResponse writes a 503 response with Retry-After header
func WriteQueueFullResponse(w http.ResponseWriter, retryAfter time.Duration) {
	w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds())))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = w.Write([]byte(`{"error":"service overloaded, please retry later"}`))
}

// WriteTimeoutResponse writes a 504 response
func WriteTimeoutResponse(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusGatewayTimeout)
	_, _ = w.Write([]byte(`{"error":"request timeout exceeded"}`))
}
