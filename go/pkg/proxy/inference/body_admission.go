// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0

package proxy

import (
	"context"
	"sync"
)

// byteAdmission is a cancellation-aware weighted semaphore for memory that
// must remain resident across an inference request. A generation channel is
// used instead of sync.Cond so waiters can also observe context cancellation.
type byteAdmission struct {
	mu      sync.Mutex
	limit   int64
	used    int64
	changed chan struct{}
}

func newByteAdmission(limit int64) *byteAdmission {
	return &byteAdmission{limit: limit, changed: make(chan struct{})}
}

func (a *byteAdmission) Acquire(ctx context.Context, bytes int64) error {
	if bytes <= 0 {
		return nil
	}
	for {
		a.mu.Lock()
		if bytes <= a.limit-a.used {
			a.used += bytes
			a.mu.Unlock()
			return nil
		}
		changed := a.changed
		a.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
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
	close(a.changed)
	a.changed = make(chan struct{})
	a.mu.Unlock()
}

func (a *byteAdmission) Used() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.used
}
