// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package proxy

import (
	"container/list"
	"context"
	"errors"
	"sync"
)

var errByteAdmissionRequestTooLarge = errors.New("byte admission request exceeds capacity")

type byteAdmissionWaiter struct {
	bytes    int64
	ready    chan struct{}
	granted  bool
	element  *list.Element
	bypassed int
}

// Bound the amount of work that may pass an older request which cannot yet fit.
// This keeps otherwise usable memory busy without allowing a stream of small
// requests to starve a large PDF or multimodal body indefinitely.
const maxByteAdmissionBypasses = 8

// byteAdmission is a cancellation-aware, aging weighted semaphore for memory
// that must remain resident across an inference request. Requests are FIFO when
// the head fits. When it does not, bounded bypass admits later work that fits;
// after maxByteAdmissionBypasses the head becomes a barrier until it can run.
type byteAdmission struct {
	mu      sync.Mutex
	limit   int64
	used    int64
	waiters list.List
}

func newByteAdmission(limit int64) *byteAdmission {
	return &byteAdmission{limit: limit}
}

// byteAdmissionLease is invocation-owned and must not be copied after use.
// A zero lease is safe to defer before the retained body size is discovered.
// Only successful admission creates ownership; failed acquisition never
// releases another request's credit. Release is idempotent.
type byteAdmissionLease struct {
	admission *byteAdmission
	bytes     int64
}

func (l *byteAdmissionLease) Acquire(ctx context.Context, admission *byteAdmission, bytes int64) error {
	if l.admission != nil {
		return errors.New("byte admission lease already acquired")
	}
	if err := admission.Acquire(ctx, bytes); err != nil {
		return err
	}
	l.admission, l.bytes = admission, bytes
	return nil
}

func (l *byteAdmissionLease) Release() {
	if l.admission == nil {
		return
	}
	admission, bytes := l.admission, l.bytes
	l.admission, l.bytes = nil, 0
	admission.Release(bytes)
}

func (a *byteAdmission) Acquire(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if bytes > a.limit {
		return errByteAdmissionRequestTooLarge
	}

	waiter := &byteAdmissionWaiter{bytes: bytes, ready: make(chan struct{})}
	a.mu.Lock()
	waiter.element = a.waiters.PushBack(waiter)
	a.grantWaitersLocked()
	a.mu.Unlock()

	select {
	case <-waiter.ready:
		return nil
	case <-ctx.Done():
		a.mu.Lock()
		if waiter.granted {
			// Admission won the race with cancellation. The caller owns the
			// bytes and must observe success so its deferred Release remains
			// paired with the grant.
			a.mu.Unlock()
			return nil
		}
		a.waiters.Remove(waiter.element)
		waiter.element = nil
		a.grantWaitersLocked()
		a.mu.Unlock()
		return ctx.Err()
	}
}

func (a *byteAdmission) grantWaitersLocked() {
	for {
		front := a.waiters.Front()
		if front == nil {
			return
		}
		available := a.limit - a.used
		selected := front
		head := front.Value.(*byteAdmissionWaiter)
		if head.bytes > available {
			if head.bypassed >= maxByteAdmissionBypasses {
				return
			}
			selected = nil
			for candidate := front.Next(); candidate != nil; candidate = candidate.Next() {
				if candidate.Value.(*byteAdmissionWaiter).bytes <= available {
					selected = candidate
					break
				}
			}
			if selected == nil {
				return
			}
			head.bypassed++
		}
		waiter := selected.Value.(*byteAdmissionWaiter)
		a.waiters.Remove(selected)
		waiter.element = nil
		waiter.granted = true
		a.used += waiter.bytes
		close(waiter.ready)
	}
}

func (a *byteAdmission) Release(bytes int64) {
	if bytes <= 0 {
		return
	}
	a.mu.Lock()
	if bytes > a.used {
		a.mu.Unlock()
		panic("inference body admission released more bytes than reserved")
	}
	a.used -= bytes
	a.grantWaitersLocked()
	a.mu.Unlock()
}

func (a *byteAdmission) Used() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used
}

// Limit returns the immutable process-wide capacity guarded by this admission
// controller. Callers use it to reduce concurrency before requesting a
// reservation instead of turning a valid low-memory configuration into a
// permanent admission failure.
func (a *byteAdmission) Limit() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.limit
}
