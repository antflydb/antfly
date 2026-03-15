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

	"github.com/antflydb/antfly/src/raft"
	"go.uber.org/zap"
)

type mockProcessor struct {
	applied  [][]byte
	flushed  int
	loaded   int
	applyErr error
	flushErr error
	loadErr  error
}

func (m *mockProcessor) Apply(_ context.Context, data []byte) error {
	if m.applyErr != nil {
		return m.applyErr
	}
	m.applied = append(m.applied, data)
	return nil
}

func (m *mockProcessor) Flush(_ context.Context) error {
	if m.flushErr != nil {
		return m.flushErr
	}
	m.flushed++
	return nil
}

func (m *mockProcessor) LoadSnapshot(_ context.Context) error {
	if m.loadErr != nil {
		return m.loadErr
	}
	m.loaded++
	return nil
}

func TestReadCommits_BasicFlow(t *testing.T) {
	commitC := make(chan *raft.Commit)
	errorC := make(chan error)
	processor := &mockProcessor{}
	logger := zap.NewNop()

	done := make(chan struct{})
	go func() {
		ReadCommits(context.Background(), commitC, errorC, processor, logger)
		close(done)
	}()

	// Send a commit with two entries
	applyDoneC := make(chan struct{})
	commitC <- &raft.Commit{
		Data:       [][]byte{[]byte("entry1"), []byte("entry2")},
		ApplyDoneC: applyDoneC,
	}

	// Wait for apply to complete
	<-applyDoneC

	// Close channels to end the loop
	close(commitC)
	close(errorC)
	<-done

	// Verify
	if len(processor.applied) != 2 {
		t.Errorf("applied %d entries, want 2", len(processor.applied))
	}
	if processor.flushed != 1 {
		t.Errorf("flushed %d times, want 1", processor.flushed)
	}
}

func TestReadCommits_NilCommitLoadsSnapshot(t *testing.T) {
	commitC := make(chan *raft.Commit)
	errorC := make(chan error)
	processor := &mockProcessor{}
	logger := zap.NewNop()

	done := make(chan struct{})
	go func() {
		ReadCommits(context.Background(), commitC, errorC, processor, logger)
		close(done)
	}()

	// Send nil commit (snapshot signal)
	commitC <- nil

	// Send a normal commit to verify we continue processing
	applyDoneC := make(chan struct{})
	commitC <- &raft.Commit{
		Data:       [][]byte{[]byte("after-snapshot")},
		ApplyDoneC: applyDoneC,
	}
	<-applyDoneC

	close(commitC)
	close(errorC)
	<-done

	if processor.loaded != 1 {
		t.Errorf("loaded %d snapshots, want 1", processor.loaded)
	}
	if len(processor.applied) != 1 {
		t.Errorf("applied %d entries, want 1", len(processor.applied))
	}
}

func TestReadCommits_ApplyError(t *testing.T) {
	commitC := make(chan *raft.Commit)
	errorC := make(chan error)
	processor := &mockProcessor{applyErr: errors.New("apply failed")}
	logger := zap.NewNop()

	done := make(chan struct{})
	go func() {
		ReadCommits(context.Background(), commitC, errorC, processor, logger)
		close(done)
	}()

	// Send a commit - apply will fail but processing should continue
	applyDoneC := make(chan struct{})
	commitC <- &raft.Commit{
		Data:       [][]byte{[]byte("entry1")},
		ApplyDoneC: applyDoneC,
	}
	<-applyDoneC

	close(commitC)
	close(errorC)
	<-done

	// Flush should still be called even if apply failed
	if processor.flushed != 1 {
		t.Errorf("flushed %d times, want 1", processor.flushed)
	}
}

func TestReadCommits_FlushErrorPanics(t *testing.T) {
	commitC := make(chan *raft.Commit)
	errorC := make(chan error)
	processor := &mockProcessor{flushErr: errors.New("flush failed")}

	// Replace the global logger with one that converts Panic to a recoverable panic
	// (zap.NewNop() won't call os.Exit but will still panic on logger.Panic)
	logger := zap.NewNop()

	panicked := make(chan bool, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicked <- true
			} else {
				panicked <- false
			}
		}()
		ReadCommits(context.Background(), commitC, errorC, processor, logger)
	}()

	// Send a commit - flush will fail and should panic
	applyDoneC := make(chan struct{})
	commitC <- &raft.Commit{
		Data:       [][]byte{[]byte("entry1")},
		ApplyDoneC: applyDoneC,
	}

	if didPanic := <-panicked; !didPanic {
		t.Error("ReadCommits did not panic on Flush error")
	}
}

func TestReadCommits_MultipleCommits(t *testing.T) {
	commitC := make(chan *raft.Commit)
	errorC := make(chan error)
	processor := &mockProcessor{}
	logger := zap.NewNop()

	done := make(chan struct{})
	go func() {
		ReadCommits(context.Background(), commitC, errorC, processor, logger)
		close(done)
	}()

	// Send multiple commits
	for range 3 {
		applyDoneC := make(chan struct{})
		commitC <- &raft.Commit{
			Data:       [][]byte{[]byte("entry")},
			ApplyDoneC: applyDoneC,
		}
		<-applyDoneC
	}

	close(commitC)
	close(errorC)
	<-done

	if len(processor.applied) != 3 {
		t.Errorf("applied %d entries, want 3", len(processor.applied))
	}
	if processor.flushed != 3 {
		t.Errorf("flushed %d times, want 3", processor.flushed)
	}
}
