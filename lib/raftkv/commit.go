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

// Package raftkv provides common patterns for Raft-backed key-value stores.
package raftkv

import (
	"context"
	"errors"

	"github.com/antflydb/antfly/src/raft"
	"go.uber.org/zap"
)

// CommitProcessor handles applying committed Raft entries to a state machine.
type CommitProcessor interface {
	// Apply decodes and processes a single operation from the Raft log.
	// Implementations may batch operations internally for efficiency.
	Apply(ctx context.Context, data []byte) error

	// Flush commits any accumulated batch to storage.
	// Called after processing all entries in a commit.
	Flush(ctx context.Context) error

	// LoadSnapshot handles the nil commit signal indicating a snapshot restore.
	LoadSnapshot(ctx context.Context) error
}

// ReadCommits reads committed entries from Raft and applies them using the processor.
// It runs until commitC is closed, then drains errorC.
//
// A nil commit signals that the processor should load a snapshot.
// After processing all entries in a commit, Flush is called and ApplyDoneC is closed.
func ReadCommits(
	ctx context.Context,
	commitC <-chan *raft.Commit,
	errorC <-chan error,
	processor CommitProcessor,
	logger *zap.Logger,
) {
	for commit := range commitC {
		if commit == nil {
			// Signaled to load snapshot
			logger.Info("Received nil commit, loading snapshot")
			if err := processor.LoadSnapshot(ctx); err != nil {
				logger.Panic("Failed to load snapshot after nil commit", zap.Error(err))
			}
			continue
		}

		for _, data := range commit.Data {
			if err := processor.Apply(ctx, data); err != nil {
				logger.Error("Failed to apply commit entry", zap.Error(err))
			}
		}

		if err := processor.Flush(ctx); err != nil {
			logger.Panic("Failed to flush batch — state machine is inconsistent", zap.Error(err))
		}

		close(commit.ApplyDoneC)
	}

	if err, ok := <-errorC; ok {
		// ErrRemoved is expected during cluster reconfiguration
		if errors.Is(err, raft.ErrRemoved) {
			logger.Info("Received ID removal notification from raft", zap.Error(err))
			return
		}
		logger.Fatal("Received error from raft error channel", zap.Error(err))
	}
}
