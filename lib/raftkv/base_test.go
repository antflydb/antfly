// Copyright 2025 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

package raftkv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/antflydb/antfly/src/raft"
	"github.com/google/uuid"
)

func TestProposer_ProposeOnly(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	// Simulate raft accepting the proposal
	go func() {
		proposal := <-proposeC
		proposal.ProposeDoneC <- nil
	}()

	ctx := context.Background()
	err := p.ProposeOnly(ctx, []byte("test data"))
	if err != nil {
		t.Errorf("ProposeOnly() error = %v", err)
	}
}

func TestProposer_ProposeOnly_ContextCanceled(t *testing.T) {
	proposeC := make(chan *raft.Proposal) // unbuffered, will block
	p := NewProposer(proposeC)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	err := p.ProposeOnly(ctx, []byte("test data"))
	if !errors.Is(err, context.Canceled) {
		t.Errorf("ProposeOnly() error = %v, want context.Canceled", err)
	}
}

func TestProposer_ProposeOnly_Closing(t *testing.T) {
	proposeC := make(chan *raft.Proposal) // unbuffered, will block
	p := NewProposer(proposeC)
	p.Close()

	ctx := context.Background()
	err := p.ProposeOnly(ctx, []byte("test data"))
	if !errors.Is(err, ErrClosing) {
		t.Errorf("ProposeOnly() error = %v, want ErrClosing", err)
	}
}

func TestProposer_ProposeOnly_ClosingWhileWaitingForAcceptance(t *testing.T) {
	proposeC := make(chan *raft.Proposal)
	p := NewProposer(proposeC)

	done := make(chan error, 1)
	go func() {
		done <- p.ProposeOnly(context.Background(), []byte("test data"))
	}()

	var proposal *raft.Proposal
	select {
	case proposal = <-proposeC:
	case <-time.After(time.Second):
		t.Fatal("proposal was not sent")
	}
	if proposal == nil {
		t.Fatal("proposal is nil")
	}

	closeDone := make(chan struct{})
	go func() {
		p.Close()
		close(closeDone)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosing) {
			t.Fatalf("ProposeOnly() error = %v, want %v", err, ErrClosing)
		}
	case <-time.After(time.Second):
		t.Fatal("ProposeOnly() did not return after Close")
	}

	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("Close() did not return after proposal waiter exited")
	}
}

func TestProposer_ProposeAndWait(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id := uuid.New()

	// Simulate raft accepting and committing
	go func() {
		proposal := <-proposeC
		proposal.ProposeDoneC <- nil
		// Simulate commit callback
		time.Sleep(10 * time.Millisecond)
		p.NotifyCommit(id, nil)
	}()

	ctx := context.Background()
	err := p.ProposeAndWait(ctx, []byte("test data"), id)
	if err != nil {
		t.Errorf("ProposeAndWait() error = %v", err)
	}
}

func TestProposer_ProposeAndWait_CommitError(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id := uuid.New()
	commitErr := errors.New("commit failed")

	// Simulate raft accepting but commit failing
	go func() {
		proposal := <-proposeC
		proposal.ProposeDoneC <- nil
		time.Sleep(10 * time.Millisecond)
		p.NotifyCommit(id, commitErr)
	}()

	ctx := context.Background()
	err := p.ProposeAndWait(ctx, []byte("test data"), id)
	if !errors.Is(err, commitErr) {
		t.Errorf("ProposeAndWait() error = %v, want %v", err, commitErr)
	}
}

func TestProposer_NotifyCommit_NoWaiter(t *testing.T) {
	proposeC := make(chan *raft.Proposal)
	p := NewProposer(proposeC)

	// Should not panic when no one is waiting
	p.NotifyCommit(uuid.New(), nil)
}

func TestProposer_NotifyCommit_DoubleNotify(t *testing.T) {
	proposeC := make(chan *raft.Proposal)
	p := NewProposer(proposeC)

	id := uuid.New()
	ch := make(chan error, 1)
	p.callbackChanMap.Store(id, ch)

	// First notify should work (buffered channel, no goroutine needed)
	p.NotifyCommit(id, nil)

	select {
	case err := <-ch:
		if err != nil {
			t.Errorf("NotifyCommit() sent error = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Error("NotifyCommit() did not send to channel")
	}

	// Second notify should not panic (channel already deleted by LoadAndDelete)
	p.NotifyCommit(id, nil)
}

func TestProposer_NotifyCommit_PreservesError(t *testing.T) {
	proposeC := make(chan *raft.Proposal)
	p := NewProposer(proposeC)

	id := uuid.New()
	ch := make(chan error, 1)
	p.callbackChanMap.Store(id, ch)

	commitErr := errors.New("something went wrong")
	p.NotifyCommit(id, commitErr)

	select {
	case err := <-ch:
		if !errors.Is(err, commitErr) {
			t.Errorf("NotifyCommit() sent error = %v, want %v", err, commitErr)
		}
	case <-time.After(time.Second):
		t.Error("NotifyCommit() did not send to channel")
	}
}

func TestProposer_RegisterCallback_WaitForCommit(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id := uuid.New()
	ch := p.RegisterCallback(id)

	// Verify callback is registered
	if !p.HasCallback(id) {
		t.Error("HasCallback() = false after RegisterCallback")
	}

	// Simulate commit notification
	go func() {
		time.Sleep(10 * time.Millisecond)
		p.NotifyCommit(id, nil)
	}()

	ctx := context.Background()
	if err := p.WaitForCommit(ctx, ch); err != nil {
		t.Errorf("WaitForCommit() error = %v", err)
	}
}

func TestProposer_RegisterCallback_WaitForCommit_Error(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id := uuid.New()
	ch := p.RegisterCallback(id)

	commitErr := errors.New("commit failed")
	go func() {
		time.Sleep(10 * time.Millisecond)
		p.NotifyCommit(id, commitErr)
	}()

	ctx := context.Background()
	err := p.WaitForCommit(ctx, ch)
	if !errors.Is(err, commitErr) {
		t.Errorf("WaitForCommit() error = %v, want %v", err, commitErr)
	}
}

func TestProposer_WaitForCommit_ContextCanceled(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id := uuid.New()
	ch := p.RegisterCallback(id)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := p.WaitForCommit(ctx, ch)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("WaitForCommit() error = %v, want context.Canceled", err)
	}

	// Clean up
	p.UnregisterCallback(id)
}

func TestProposer_WaitForCommit_Closing(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id := uuid.New()
	ch := p.RegisterCallback(id)

	done := make(chan error, 1)
	go func() {
		done <- p.WaitForCommit(context.Background(), ch)
	}()

	time.Sleep(10 * time.Millisecond)
	p.Close()

	select {
	case err := <-done:
		if !errors.Is(err, ErrClosing) {
			t.Fatalf("WaitForCommit() error = %v, want %v", err, ErrClosing)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitForCommit() did not return after Close")
	}
}

func TestProposer_Close_ReleasesRegisteredCallbacks(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id1 := uuid.New()
	id2 := uuid.New()
	ch1 := p.RegisterCallback(id1)
	ch2 := p.RegisterCallback(id2)

	p.Close()

	for _, ch := range []chan error{ch1, ch2} {
		select {
		case err := <-ch:
			if !errors.Is(err, ErrClosing) {
				t.Fatalf("callback error = %v, want %v", err, ErrClosing)
			}
		case <-time.After(time.Second):
			t.Fatal("callback not released on close")
		}
	}
}

func TestProposer_UnregisterCallback(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id := uuid.New()
	p.RegisterCallback(id)

	if !p.HasCallback(id) {
		t.Error("HasCallback() = false after RegisterCallback")
	}

	p.UnregisterCallback(id)

	if p.HasCallback(id) {
		t.Error("HasCallback() = true after UnregisterCallback")
	}
}

func TestProposer_HasCallback(t *testing.T) {
	proposeC := make(chan *raft.Proposal, 1)
	p := NewProposer(proposeC)

	id := uuid.New()

	// Not registered
	if p.HasCallback(id) {
		t.Error("HasCallback() = true for unregistered ID")
	}

	// Register
	p.RegisterCallback(id)
	if !p.HasCallback(id) {
		t.Error("HasCallback() = false after RegisterCallback")
	}

	// After notify (LoadAndDelete removes it)
	p.NotifyCommit(id, nil)
	if p.HasCallback(id) {
		t.Error("HasCallback() = true after NotifyCommit")
	}
}
