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

package admission

import (
	"container/list"
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"
)

// DefaultOperationTimeout bounds owned operations across admission, transport
// and response-body consumption. Supply an HTTP client to choose another policy.
const DefaultOperationTimeout = 90 * time.Second

// Doer sends one HTTP request with caller-selected transport policy.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// SQLNoReplayDoer supplies an explicitly non-replaying SQL transport.
type SQLNoReplayDoer interface {
	DoSQLNoReplay(*http.Request) (*http.Response, error)
}

// Config bounds one client's HTTP operations independently of
// the server's execution policy. Zero queue capacity or wait means fail fast.
type Config struct {
	MaxConcurrentRequests int
	MaxQueuedRequests     int
	MaxQueueWait          time.Duration
}

// CapacityError refuses an operation before invoking its HTTP client.
// RequestSent is always false; the wrapper performs no operation retries.
type CapacityError struct {
	Reason      string
	RequestSent bool
	cause       error
}

func (e *CapacityError) Error() string { return "client request admission refused: " + e.Reason }
func (e *CapacityError) Unwrap() error { return e.cause }

type requestWaiter struct {
	ready chan struct{}
}

// Client implements the generated client's Doer and SQLNoReplayDoer
// contracts. A response owns admission until its Body.Close finishes. Close stops
// new work and wakes waiters; callers still close active response bodies.
// A supplied HTTP client and its connection pool remain owned by their caller.
type Client struct {
	next      Doer
	policy    Config
	closeIdle func()
	mu        sync.Mutex
	active    int
	waiters   list.List
	closed    bool
}

// New creates reusable bounded admission. A nil policy selects
// 100 active requests, 100 waiters and a one-second wait ceiling. A nil next
// creates an owned HTTP client with DefaultOperationTimeout; otherwise next is borrowed. An already bounded
// client with a nil policy is reused, so constructors never stack local queues.
func New(next Doer, policy *Config) (*Client, error) {
	if existing, ok := next.(*Client); ok && existing != nil {
		if policy != nil {
			return nil, errors.New("HTTP client already has request admission")
		}
		return existing, nil
	}
	config := Config{100, 100, time.Second}
	if policy != nil {
		config = *policy
	}
	if config.MaxConcurrentRequests <= 0 || config.MaxQueuedRequests < 0 || config.MaxQueueWait < 0 {
		return nil, errors.New("invalid client request admission limits")
	}
	client := &Client{next: next, policy: config}
	// A typed nil *http.Client can enter through a generated client option.
	if httpClient, ok := next.(*http.Client); next == nil || (ok && httpClient == nil) {
		transport := http.DefaultTransport
		if standard, ok := transport.(*http.Transport); ok {
			owned := standard.Clone()
			// Retain a useful admitted burst without accumulating unbounded
			// idle sockets across targets. Preserve explicitly tighter limits.
			ceiling := config.MaxConcurrentRequests
			if owned.MaxIdleConns == 0 || owned.MaxIdleConns > ceiling {
				owned.MaxIdleConns = ceiling
			}
			if owned.MaxIdleConnsPerHost == 0 || owned.MaxIdleConnsPerHost > ceiling {
				owned.MaxIdleConnsPerHost = ceiling
			}
			if owned.MaxConnsPerHost <= 0 || owned.MaxConnsPerHost > ceiling {
				owned.MaxConnsPerHost = ceiling
			}
			transport = owned
			client.closeIdle = owned.CloseIdleConnections
		}
		client.next = &http.Client{
			Transport: transport,
			Timeout:   DefaultOperationTimeout,
			// A redirect is not evidence that the original mutation had no
			// effect. Owned clients return it instead of replaying requests.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	return client, nil
}

func contextFailure(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func capacityFailure(reason string, cause error) error {
	return &CapacityError{Reason: reason, cause: cause}
}

func canceledCapacity(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return capacityFailure("deadline_exceeded", err)
	}
	return capacityFailure("canceled", err)
}

func (c *Client) wakeHead() {
	if head := c.waiters.Front(); head != nil {
		select {
		case head.Value.(*requestWaiter).ready <- struct{}{}:
		default:
		}
	}
}

func (c *Client) acquire(ctx context.Context) error {
	// Context methods are caller callbacks. Cache their immutable authority
	// before locking; inspect only the channel and timestamp under the mutex.
	done := ctx.Done()
	deadline, hasDeadline := ctx.Deadline()
	stopped := func() error {
		if hasDeadline && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		select {
		case <-done:
			return context.Canceled
		default:
			return nil
		}
	}
	attributeCancellation := func(fallback error) error {
		if err := ctx.Err(); err != nil {
			return canceledCapacity(err)
		}
		return canceledCapacity(fallback)
	}
	c.mu.Lock()
	if err := stopped(); err != nil {
		c.mu.Unlock()
		return attributeCancellation(err)
	}
	if c.closed {
		c.mu.Unlock()
		return capacityFailure("client_closed", nil)
	}
	if c.waiters.Len() == 0 && c.active < c.policy.MaxConcurrentRequests {
		c.active++
		c.mu.Unlock()
		return nil
	}
	if c.waiters.Len() >= c.policy.MaxQueuedRequests {
		c.mu.Unlock()
		return capacityFailure("queue_full", nil)
	}
	if c.policy.MaxQueueWait == 0 {
		c.mu.Unlock()
		return capacityFailure("wait_timeout", nil)
	}
	until := time.Now().Add(c.policy.MaxQueueWait)
	waiter := &requestWaiter{ready: make(chan struct{}, 1)}
	entry := c.waiters.PushBack(waiter)
	c.wakeHead()
	c.mu.Unlock()
	timer := time.NewTimer(time.Until(until))
	defer timer.Stop()
	for {
		select {
		case <-done:
		case <-timer.C:
		case <-waiter.ready:
		}
		c.mu.Lock()
		var failure error
		canceled := stopped()
		if canceled != nil {
			failure = canceledCapacity(canceled)
		} else if c.closed {
			failure = capacityFailure("client_closed", nil)
		} else if !time.Now().Before(until) {
			failure = capacityFailure("wait_timeout", nil)
		}
		if failure != nil {
			c.waiters.Remove(entry)
			c.wakeHead()
			c.mu.Unlock()
			if canceled != nil {
				return attributeCancellation(canceled)
			}
			return failure
		}
		if c.waiters.Front() == entry && c.active < c.policy.MaxConcurrentRequests {
			c.active++
			c.waiters.Remove(entry)
			c.wakeHead()
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
	}
}

func (c *Client) release() {
	c.mu.Lock()
	c.active--
	c.wakeHead()
	closeIdle := c.closed && c.active == 0
	c.mu.Unlock()
	if closeIdle && c.closeIdle != nil {
		c.closeIdle()
	}
}

// Snapshot describes this client's current local capacity and policy.
type Snapshot struct {
	Active int
	Queued int
	Closed bool
	Policy Config
}

// Stats returns a coherent snapshot without invoking caller code under the lock.
func (c *Client) Stats() Snapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Snapshot{Active: c.active, Queued: c.waiters.Len(), Closed: c.closed, Policy: c.policy}
}

// Close wakes waiting callers without closing a caller-supplied HTTP transport.
func (c *Client) Close() error {
	c.mu.Lock()
	c.closed = true
	for entry := c.waiters.Front(); entry != nil; entry = entry.Next() {
		select {
		case entry.Value.(*requestWaiter).ready <- struct{}{}:
		default:
		}
	}
	c.mu.Unlock()
	if c.closeIdle != nil {
		c.closeIdle()
	}
	return nil
}

func (c *Client) Do(request *http.Request) (*http.Response, error) {
	return c.do(request, c.next.Do)
}

// DoOperation holds one admission lease and the original HTTP timeout across
// an operation that may make sequential transport attempts (for example route
// negotiation). The callback must use the supplied unwrapped transport without
// concurrent fanout. The returned response retains admission until Body.Close.
// This does not authorize retries; the operation owns its replay policy.
func (c *Client) DoOperation(request *http.Request, operation func(*http.Request, Doer) (*http.Response, error)) (*http.Response, error) {
	return c.do(request, func(current *http.Request) (*http.Response, error) {
		return operation(current, c.next)
	})
}

// DoSQLNoReplay preserves generated SQL's no-redirect/no-replay contract while
// keeping its request and response under the same admission owner.
func (c *Client) DoSQLNoReplay(request *http.Request) (*http.Response, error) {
	switch client := c.next.(type) {
	case *http.Client:
		owned := *client
		owned.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		return c.do(request, owned.Do)
	case SQLNoReplayDoer:
		return c.do(request, client.DoSQLNoReplay)
	default:
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, errors.New("SQL custom transport must implement SQLNoReplayDoer")
	}
}

func (c *Client) do(request *http.Request, send func(*http.Request) (*http.Response, error)) (*http.Response, error) {
	ctx := request.Context()
	var cancel context.CancelFunc
	// http.Client starts Timeout at Do. Establish it before waiting, then retain
	// that same context through the response body rather than restarting it.
	if client, ok := c.next.(*http.Client); ok && client.Timeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, client.Timeout)
	} else {
		// Own cancellation even when a borrowed transport has no timeout.
		// This adds no deadline and never cancels the caller's parent.
		ctx, cancel = context.WithCancel(ctx)
	}
	request = request.Clone(ctx)
	if err := c.acquire(ctx); err != nil {
		cancel()
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, err
	}
	owned := true
	defer func() {
		if owned {
			c.release()
			cancel()
		}
	}()
	if err := contextFailure(ctx); err != nil {
		if request.Body != nil {
			_ = request.Body.Close()
		}
		return nil, canceledCapacity(err)
	}
	response, err := send(request)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body = &admittedBody{ReadCloser: response.Body, release: c.release, cancel: cancel}
			owned = false
			_ = response.Body.Close()
		}
		return response, err
	}
	if response == nil {
		return nil, errors.New("HTTP client returned no response")
	}
	if response.Body != nil && response.Body != http.NoBody {
		response.Body = &admittedBody{ReadCloser: response.Body, release: c.release, cancel: cancel}
		owned = false
	}
	return response, nil
}

type admittedBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
	cancel  context.CancelFunc
	err     error
}

func (body *admittedBody) Close() error {
	body.once.Do(func() {
		defer body.release()
		// Closing signals the end of our owned operation before joining the
		// transport. A transport may need cancellation to finish Close; its
		// capacity stays charged until that physical retirement returns.
		body.cancel()
		body.err = body.ReadCloser.Close()
	})
	return body.err
}
