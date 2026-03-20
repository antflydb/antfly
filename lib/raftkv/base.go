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
	"fmt"
	"sync"

	"github.com/antflydb/antfly/src/raft"
	"github.com/google/uuid"
	"github.com/puzpuzpuz/xsync/v4"
)

// ErrClosing is returned when an operation is attempted on a store that is shutting down.
var ErrClosing = errors.New("store is closing")

// Proposer handles proposing operations to Raft and waiting for commits.
type Proposer struct {
	proposeC        chan<- *raft.Proposal
	closedC         chan struct{}
	closeOnce       sync.Once
	inflight        sync.WaitGroup
	callbackChanMap *xsync.Map[uuid.UUID, chan error]
}

// NewProposer creates a new Proposer with the given proposal channel.
func NewProposer(proposeC chan<- *raft.Proposal) *Proposer {
	return &Proposer{
		proposeC:        proposeC,
		closedC:         make(chan struct{}),
		callbackChanMap: xsync.NewMap[uuid.UUID, chan error](),
	}
}

// ProposeOnly sends data to Raft and waits for the proposal to be accepted,
// but does not wait for the commit to be applied.
func (p *Proposer) ProposeOnly(ctx context.Context, data []byte) error {
	// Track inflight proposals so Close() can wait for them to drain
	// before closing proposeC, preventing "send on closed channel" panics.
	p.inflight.Add(1)
	defer p.inflight.Done()

	// Give closedC priority: when both closedC and proposeC are ready,
	// select picks randomly, which would panic sending on closed proposeC.
	select {
	case <-p.closedC:
		return ErrClosing
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	proposeDoneC := make(chan error, 1)

	// Single select: closedC ensures we return ErrClosing if Close() has been
	// called, even when proposeC is also ready. The inflight WaitGroup prevents
	// Close() from calling close(proposeC) while we're in this select.
	select {
	case p.proposeC <- &raft.Proposal{
		Data:         data,
		ProposeDoneC: proposeDoneC,
	}:
	case <-p.closedC:
		return ErrClosing
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-proposeDoneC:
		if err != nil {
			return fmt.Errorf("proposing to raft: %w", err)
		}
	case <-p.closedC:
		return ErrClosing
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

// ProposeAndWait sends data to Raft and waits for both the proposal to be
// accepted and the commit to be applied. The id is used to correlate the
// commit callback.
func (p *Proposer) ProposeAndWait(ctx context.Context, data []byte, id uuid.UUID) error {
	commitDoneC := make(chan error, 1)
	p.callbackChanMap.Store(id, commitDoneC)
	defer p.callbackChanMap.Delete(id)

	if err := p.ProposeOnly(ctx, data); err != nil {
		return err
	}

	return p.WaitForCommit(ctx, commitDoneC)
}

// NotifyCommit sends the result of a commit to any waiting caller.
// This should be called from the commit processor after applying an operation.
// Uses LoadAndDelete to prevent double-notification on shutdown races.
//
// The callback channel is buffered (size 1), so the send always succeeds
// without blocking. If the receiver has already left (e.g., context cancelled),
// the error is buffered and eventually garbage collected.
func (p *Proposer) NotifyCommit(id uuid.UUID, err error) {
	if callbackChan, ok := p.callbackChanMap.LoadAndDelete(id); ok {
		select {
		case callbackChan <- err:
		default:
		}
	}
}

// RegisterCallback registers a callback channel for a given UUID.
// The caller must call UnregisterCallback to clean up if not waiting for commit.
// This is useful when the caller needs to hold a lock during proposal but release
// it before waiting for commit.
func (p *Proposer) RegisterCallback(id uuid.UUID) chan error {
	ch := make(chan error, 1)
	p.callbackChanMap.Store(id, ch)
	return ch
}

// UnregisterCallback removes a registered callback. This should be called
// if the caller decides not to wait for commit (e.g., due to an error).
func (p *Proposer) UnregisterCallback(id uuid.UUID) {
	p.callbackChanMap.Delete(id)
}

// WaitForCommit waits for a commit callback on the given channel.
// The channel should have been obtained from RegisterCallback.
func (p *Proposer) WaitForCommit(ctx context.Context, commitDoneC chan error) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closedC:
		return ErrClosing
	case err := <-commitDoneC:
		return err
	}
}

// HasCallback returns true if a callback is registered for the given UUID.
// This is useful for determining if anyone is waiting for a commit.
func (p *Proposer) HasCallback(id uuid.UUID) bool {
	_, ok := p.callbackChanMap.Load(id)
	return ok
}

// Close signals that the proposer is shutting down and closes the proposal channel.
// After Close is called, new proposals will return ErrClosing.
// This also closes proposeC to trigger raft node shutdown.
func (p *Proposer) Close() {
	p.closeOnce.Do(func() {
		// Close closedC first to signal any in-flight proposals and waiters to exit.
		close(p.closedC)

		// Wake all commit waiters so shutdown does not hang on operations that will
		// never be applied after raft is being torn down.
		p.callbackChanMap.Range(func(id uuid.UUID, _ chan error) bool {
			if ch, ok := p.callbackChanMap.LoadAndDelete(id); ok {
				select {
				case ch <- ErrClosing:
				default:
				}
			}
			return true
		})

		// Wait for all in-flight ProposeOnly calls to finish before closing proposeC.
		// This prevents "send on closed channel" panics from goroutines still in the
		// select between closedC and proposeC.
		p.inflight.Wait()
		close(p.proposeC)
	})
}
