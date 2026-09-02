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
	"errors"
	"testing"
	"time"
)

func TestByteAdmissionBoundsAggregateRetainedBodies(t *testing.T) {
	admission := newByteAdmission(10)
	if err := admission.Acquire(context.Background(), 7); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := admission.Acquire(ctx, 4); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want context.Canceled", err)
	}
	if got := admission.Used(); got != 7 {
		t.Fatalf("used = %d, want 7", got)
	}

	admission.Release(7)
	if err := admission.Acquire(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	admission.Release(10)
}

func TestByteAdmissionWakesBlockedWaiterAfterRelease(t *testing.T) {
	admission := newByteAdmission(10)
	if err := admission.Acquire(context.Background(), 10); err != nil {
		t.Fatal(err)
	}

	acquired := make(chan error, 1)
	go func() {
		acquired <- admission.Acquire(context.Background(), 1)
	}()
	admission.Release(10)

	select {
	case err := <-acquired:
		if err != nil {
			t.Fatal(err)
		}
		admission.Release(1)
	case <-time.After(time.Second):
		t.Fatal("blocked waiter was not woken after release")
	}
}
