// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

package sources

import (
	"context"
	"io"
	"net/http"
	"sync"
	"time"
)

// HTTP client timeouts normally cap the complete response. For streaming,
// preserve that budget for waiting on headers and each blocked body read while
// the caller's context bounds the whole operation. Backpressure between reads
// does not consume the provider's idle budget. One timer is reused per response.
type googleStreamBudget struct {
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelCauseFunc
	timer          *time.Timer
	timeout        time.Duration
	deadline       time.Time
	active, closed bool
}

func newGoogleStreamBudget(ctx context.Context, timeout time.Duration) *googleStreamBudget {
	if timeout <= 0 {
		timeout = time.Minute
	}
	ctx, cancel := context.WithCancelCause(ctx)
	budget := &googleStreamBudget{ctx: ctx, cancel: cancel, timeout: timeout, active: true, deadline: time.Now().Add(timeout)}
	budget.mu.Lock()
	budget.timer = time.AfterFunc(timeout, budget.expire)
	budget.mu.Unlock()
	return budget
}

func (b *googleStreamBudget) expire() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.active || b.closed {
		return
	}
	// An earlier timer callback may run after the next read starts. Recheck
	// the current deadline so that callback cannot cancel a fresh read.
	if remaining := time.Until(b.deadline); remaining > 0 {
		b.timer.Reset(remaining)
		return
	}
	b.cancel(context.DeadlineExceeded)
}

func (b *googleStreamBudget) begin() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return context.Canceled
	}
	if err := context.Cause(b.ctx); err != nil {
		return err
	}
	b.active = true
	b.deadline = time.Now().Add(b.timeout)
	b.timer.Reset(b.timeout)
	return nil
}

func (b *googleStreamBudget) end() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active && !time.Now().Before(b.deadline) {
		b.cancel(context.DeadlineExceeded)
	}
	b.active = false
	b.timer.Stop()
	return context.Cause(b.ctx)
}

func (b *googleStreamBudget) close() {
	b.mu.Lock()
	b.closed = true
	b.active = false
	b.timer.Stop()
	b.mu.Unlock()
	b.cancel(context.Canceled)
}

type googleStreamBody struct {
	io.ReadCloser
	budget *googleStreamBudget
}

func (b *googleStreamBody) Read(out []byte) (int, error) {
	if err := b.budget.begin(); err != nil {
		return 0, err
	}
	n, err := b.ReadCloser.Read(out)
	if cause := b.budget.end(); cause != nil {
		return n, cause
	}
	return n, err
}
func (b *googleStreamBody) Close() error {
	b.budget.close()
	return b.ReadCloser.Close()
}

func doGoogleStream(client *http.Client, request *http.Request) (*http.Response, error) {
	budget := newGoogleStreamBudget(request.Context(), client.Timeout)
	streamClient := *client
	streamClient.Timeout = 0
	response, err := streamClient.Do(request.WithContext(budget.ctx))
	cause := budget.end()
	if err != nil || cause != nil {
		budget.close()
		if response != nil {
			_ = response.Body.Close()
		}
		if cause != nil {
			return nil, cause
		}
		return nil, err
	}
	response.Body = &googleStreamBody{ReadCloser: response.Body, budget: budget}
	return response, nil
}
