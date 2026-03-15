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

package indexes

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// RebuildState manages rebuild state tracking for indexes using a simple file-based approach.
// This allows indexes to resume rebuilds after restarts without mixing rebuild metadata
// with the main data store.
type RebuildState struct {
	indexPath string
}

// NewRebuildState creates a new rebuild state tracker for the given index path
func NewRebuildState(indexPath string) *RebuildState {
	return &RebuildState{
		indexPath: indexPath,
	}
}

// statePath returns the path to the rebuild state file
func (rs *RebuildState) statePath() string {
	return filepath.Join(rs.indexPath, "rebuild.state")
}

// Check checks if a rebuild is in progress and returns the last indexed key.
// Returns (nil, false, nil) if no rebuild is in progress.
// Returns (lastKey, true, nil) if rebuild should resume from lastKey.
func (rs *RebuildState) Check() (rebuildFrom []byte, needsRebuild bool, err error) {
	data, err := os.ReadFile(rs.statePath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil // No rebuild in progress
		}
		return nil, false, fmt.Errorf("reading rebuild state: %w", err)
	}
	return data, true, nil // Resume from this key
}

// Update atomically writes the current rebuild progress.
// Uses write-to-temp-then-rename pattern for atomicity.
func (rs *RebuildState) Update(key []byte) error {
	tmpPath := rs.statePath() + ".tmp"
	f, err := os.Create(tmpPath) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return fmt.Errorf("creating temp rebuild state file: %w", err)
	}

	if _, err := f.Write(key); err != nil {
		_ = f.Close()
		return fmt.Errorf("writing rebuild state: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("syncing rebuild state: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing rebuild state file: %w", err)
	}

	if err := os.Rename(tmpPath, rs.statePath()); err != nil {
		return fmt.Errorf("renaming rebuild state file: %w", err)
	}

	return nil
}

// Clear removes the rebuild state file, indicating rebuild is complete
func (rs *RebuildState) Clear() error {
	err := os.Remove(rs.statePath())
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing rebuild state: %w", err)
	}
	return nil
}
