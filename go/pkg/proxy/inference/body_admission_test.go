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

func TestByteAdmissionDoesNotStarveOlderLargeWaiter(t *testing.T) {
	admission := newByteAdmission(10)
	if err := admission.Acquire(context.Background(), 6); err != nil {
		t.Fatal(err)
	}

	large := make(chan error, 1)
	go func() { large <- admission.Acquire(context.Background(), 8) }()
	waitForAdmissionWaiters(t, admission, 1)

	small := make(chan error, 1)
	go func() { small <- admission.Acquire(context.Background(), 4) }()
	waitForAdmissionWaiters(t, admission, 2)
	select {
	case err := <-small:
		t.Fatalf("newer small waiter bypassed queue with error %v", err)
	default:
	}

	admission.Release(6)
	if err := receiveAdmission(t, large); err != nil {
		t.Fatal(err)
	}
	if got := admission.Used(); got != 8 {
		t.Fatalf("used = %d after large grant, want 8", got)
	}
	select {
	case err := <-small:
		t.Fatalf("small waiter acquired before large release with error %v", err)
	default:
	}

	admission.Release(8)
	if err := receiveAdmission(t, small); err != nil {
		t.Fatal(err)
	}
	admission.Release(4)
}

func TestByteAdmissionCancellationRemovesQueueHead(t *testing.T) {
	admission := newByteAdmission(10)
	if err := admission.Acquire(context.Background(), 10); err != nil {
		t.Fatal(err)
	}

	largeCtx, cancelLarge := context.WithCancel(context.Background())
	large := make(chan error, 1)
	go func() { large <- admission.Acquire(largeCtx, 8) }()
	waitForAdmissionWaiters(t, admission, 1)
	small := make(chan error, 1)
	go func() { small <- admission.Acquire(context.Background(), 2) }()
	waitForAdmissionWaiters(t, admission, 2)

	cancelLarge()
	if err := receiveAdmission(t, large); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}
	waitForAdmissionWaiters(t, admission, 1)
	admission.Release(10)
	if err := receiveAdmission(t, small); err != nil {
		t.Fatal(err)
	}
	admission.Release(2)
}

func TestByteAdmissionRejectsImpossibleRequest(t *testing.T) {
	admission := newByteAdmission(10)
	if err := admission.Acquire(context.Background(), 11); !errors.Is(err, errByteAdmissionRequestTooLarge) {
		t.Fatalf("Acquire error = %v, want errByteAdmissionRequestTooLarge", err)
	}
}

func TestByteAdmissionRejectsAlreadyCanceledRequestWithCapacity(t *testing.T) {
	admission := newByteAdmission(10)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := admission.Acquire(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire error = %v, want context.Canceled", err)
	}
	if got := admission.Used(); got != 0 {
		t.Fatalf("used = %d after canceled request, want 0", got)
	}
}

func waitForAdmissionWaiters(t *testing.T, admission *byteAdmission, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		admission.mu.Lock()
		got := admission.waiters.Len()
		admission.mu.Unlock()
		if got == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiter count did not reach %d", want)
}

func receiveAdmission(t *testing.T, result <-chan error) error {
	t.Helper()
	select {
	case err := <-result:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for admission result")
		return nil
	}
}
