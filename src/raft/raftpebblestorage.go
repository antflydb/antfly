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

package raft

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/logger"
	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap"
)

// Key prefixes for different types of Raft data
var (
	// Prefix for Raft log entries
	logEntryKeyPrefix = []byte("entry-")

	peerPrefix = []byte("peer:")

	// Key for Raft hard state
	hardStateKey = []byte("hardstate")
	// Key for Raft conf state
	confStateKey = []byte("confstate")

	// Key for metadata about the most recent snapshot
	snapshotMetaKey = []byte("snapshot-meta")

	// Key for metadata about the log indexes
	firstIndexKey = []byte("first-index")
	lastIndexKey  = []byte("last-index")
)

// PebbleStorage implements the raft.Storage interface using Pebble as the
// underlying storage engine
type PebbleStorage struct {
	// The directory path where the Pebble DB instance is stored
	dir string

	// The Pebble DB instance
	db *pebble.DB

	// Protects concurrent access to the storage
	mu sync.RWMutex

	// The first available log entry index
	firstIndex uint64

	// The last available log entry index
	lastIndex uint64

	// The most recent snapshot
	snapshot raftpb.Snapshot

	// Logger for diagnostics
	logger *zap.Logger

	// Used by SaveRaftState to avoid repeated allocations
	uint64Buf [8]byte
}

// Used for testing
var PebbleStorageInMem = false

// cleanupCorruptedPebbleDir detects and cleans up corrupted Pebble state.
// This handles the case where a previous Pebble initialization was interrupted,
// leaving a CURRENT file that points to a non-existent MANIFEST.
// Returns true if cleanup was performed, false otherwise.
func cleanupCorruptedPebbleDir(zl *zap.Logger, dir string) (bool, error) {
	currentFile := filepath.Join(dir, "CURRENT")
	data, err := os.ReadFile(currentFile) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		if os.IsNotExist(err) {
			// No CURRENT file means no corruption to clean up
			return false, nil
		}
		return false, fmt.Errorf("reading CURRENT file: %w", err)
	}

	// CURRENT file contains the manifest name (e.g., "MANIFEST-000001\n")
	manifestName := strings.TrimSpace(string(data))
	if manifestName == "" {
		// Empty CURRENT file is also corrupted
		zl.Warn("Empty CURRENT file detected, cleaning up corrupted Pebble directory",
			zap.String("dir", dir))
		if err := os.RemoveAll(dir); err != nil {
			return false, fmt.Errorf("removing corrupted pebble dir: %w", err)
		}
		return true, nil
	}

	manifestPath := filepath.Join(dir, manifestName)
	if _, err := os.Stat(manifestPath); err != nil { //nolint:gosec // G703: path constructed from internal config, not user input
		if os.IsNotExist(err) {
			// CURRENT points to non-existent MANIFEST - this is the corruption we're looking for
			zl.Warn("Corrupted Pebble state detected: CURRENT points to missing MANIFEST",
				zap.String("dir", dir),
				zap.String("manifest", manifestName))
			if err := os.RemoveAll(dir); err != nil {
				return false, fmt.Errorf("removing corrupted pebble dir: %w", err)
			}
			return true, nil
		}
		return false, fmt.Errorf("checking manifest file: %w", err)
	}

	// MANIFEST exists, no corruption detected
	return false, nil
}

// NewPebbleStorage creates a new PebbleStorage. If cache is non-nil, the shared
// block cache is used; otherwise a dedicated 64MB cache is created.
func NewPebbleStorage(zl *zap.Logger, dir string, cache *pebbleutils.Cache) (*PebbleStorage, error) {
	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return nil, err
	}

	// Check for and clean up corrupted Pebble state from previous failed initialization.
	// This prevents "could not open manifest file" errors.
	if cleaned, err := cleanupCorruptedPebbleDir(zl, dir); err != nil {
		return nil, fmt.Errorf("checking for corrupted pebble state: %w", err)
	} else if cleaned {
		// Directory was removed, recreate it
		if err := os.MkdirAll(dir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
			return nil, err
		}
	}

	// Open Pebble database
	opts := &pebble.Options{
		Logger:          &logger.NoopLoggerAndTracer{},
		LoggerAndTracer: &logger.NoopLoggerAndTracer{},
	}
	cache.Apply(opts, 64<<20) // 64MB fallback
	if PebbleStorageInMem {
		opts.FS = vfs.NewMem()
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	ps := &PebbleStorage{
		dir:    dir,
		db:     db,
		logger: zl.Named("pebble-storage"),
	}

	// Initialize state from disk
	if err := ps.initialize(); err != nil {
		// Close DB before returning error
		_ = db.Close()
		return nil, err
	}

	return ps, nil
}

// initialize loads the initial state from disk
func (ps *PebbleStorage) initialize() error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Load the first and last index
	firstIndex, err := ps.loadFirstIndex()
	if err != nil {
		return err
	}
	ps.firstIndex = firstIndex

	lastIndex, err := ps.loadLastIndex()
	if err != nil {
		return err
	}
	ps.lastIndex = lastIndex

	// Load snapshot metadata if it exists
	snapshotMeta, err := ps.LoadSnapshot()
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return err
	}

	if snapshotMeta != nil {
		ps.logger.Info("Initialized snapshotMeta", zap.Stringer("snapshotMeta", snapshotMeta))
		ps.snapshot = *snapshotMeta
	}

	ps.logger.Info(
		"Initialized storage",
		zap.Uint64("firstIndex", ps.firstIndex),
		zap.Uint64("lastIndex", ps.lastIndex),
	)
	return nil
}

// Close closes the underlying Pebble database
func (ps *PebbleStorage) Close() (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	if err := ps.db.Close(); err != nil && !errors.Is(err, pebble.ErrClosed) {
		return err
	}
	return nil
}

// InitialState implements the raft.Storage interface
func (ps *PebbleStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	// TODO (ajr) Should these be in a transaction?
	hardState, err := ps.loadHardState()
	if err != nil && !errors.Is(err, pebble.ErrNotFound) {
		return raftpb.HardState{}, raftpb.ConfState{}, err
	}

	var confState raftpb.ConfState
	if !raft.IsEmptySnap(ps.snapshot) {
		confState = ps.snapshot.Metadata.ConfState
	} else {
		confState, err = ps.loadConfState()
		if err != nil && !errors.Is(err, pebble.ErrNotFound) {
			return raftpb.HardState{}, raftpb.ConfState{}, err
		}
	}

	return hardState, confState, nil
}

// Entries implements the raft.Storage interface
func (ps *PebbleStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	if lo < ps.firstIndex {
		return nil, raft.ErrCompacted
	}

	if hi > ps.lastIndex+1 {
		return nil, raft.ErrUnavailable
	}

	// Cap hi to lastIndex+1
	hi = min(hi, ps.lastIndex+1)

	// Check if there are no entries
	if lo >= hi {
		return nil, nil
	}

	entries := make([]raftpb.Entry, 0, hi-lo)
	var size uint64 = 0

	// Create iterator options for the range [lo, hi)
	loKey := makeLogEntryKey(lo)
	hiKey := makeLogEntryKey(hi)
	iterOpts := &pebble.IterOptions{
		LowerBound: loKey,
		UpperBound: hiKey,
	}

	// Create iterator with context
	iter, err := ps.db.NewIterWithContext(context.Background(), iterOpts)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = iter.Close()
	}()

	// Scan through the range
	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()
		if value == nil {
			continue
		}

		var entry raftpb.Entry
		if err := entry.Unmarshal(value); err != nil {
			return nil, err
		}

		size += uint64(entry.Size()) //nolint:gosec // G115: bounded value, cannot overflow in practice
		if size > maxSize && len(entries) > 0 {
			break
		}

		entries = append(entries, entry)
	}

	// Check for iterator errors
	if err := iter.Error(); err != nil {
		return nil, err
	}

	return entries, nil
}

// Term implements the raft.Storage interface
func (ps *PebbleStorage) Term(i uint64) (uint64, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return ps.term(i)
}

func (ps *PebbleStorage) term(i uint64) (uint64, error) {
	if i == 0 {
		return 0, nil
	}

	// Snapshot boundary — raft requires Term(snap.Index) to remain
	// available for log matching even after the snapshot is installed.
	if !raft.IsEmptySnap(ps.snapshot) && i == ps.snapshot.Metadata.Index {
		return ps.snapshot.Metadata.Term, nil
	}

	// Term is available for [FirstIndex()-1, LastIndex()]. Anything
	// strictly below FirstIndex()-1 has been compacted away.
	if i+1 < ps.firstIndex {
		return 0, raft.ErrCompacted
	}

	if i > ps.lastIndex {
		return 0, raft.ErrUnavailable
	}

	// Read the entry from disk. After Compact(i), entry i is kept as a
	// sentinel so Term(FirstIndex()-1) succeeds.
	entry, err := ps.getEntry(i)
	if err != nil {
		// Sentinel may be missing if firstIndex was set without a real
		// compaction (e.g. snapshot install). Treat as compacted.
		if i+1 == ps.firstIndex {
			return 0, raft.ErrCompacted
		}
		return 0, err
	}

	return entry.Term, nil
}

// LastIndex implements the raft.Storage interface
func (ps *PebbleStorage) LastIndex() (uint64, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	return ps.lastIndex, nil
}

// FirstIndex implements the raft.Storage interface
func (ps *PebbleStorage) FirstIndex() (uint64, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return ps.firstIndex, nil
}

// Snapshot implements the raft.Storage interface
func (ps *PebbleStorage) Snapshot() (raftpb.Snapshot, error) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if raft.IsEmptySnap(ps.snapshot) {
		return raftpb.Snapshot{}, raft.ErrSnapshotTemporarilyUnavailable
	}
	return ps.snapshot, nil
}

// CreateSnapshot creates a snapshot from the current state
func (ps *PebbleStorage) CreateSnapshot(index uint64, cs *raftpb.ConfState, data []byte) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if index <= ps.snapshot.Metadata.Index {
		return errors.New("requested index is older than current snapshot")
	}

	if index > ps.lastIndex {
		return errors.New("requested index is unavailable")
	}

	// Get the term for the index (safe without a separate transaction
	// because we already hold ps.mu).
	term, err := ps.term(index)
	if err != nil {
		return err
	}

	snapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     index,
			Term:      term,
			ConfState: *cs,
		},
		Data: data,
	}

	snapdata, err := snapshot.Marshal()
	if err != nil {
		return err
	}

	if err := ps.db.Set(snapshotMetaKey, snapdata, pebble.Sync); err != nil {
		return err
	}

	// Update in-memory state only after successful disk write.
	ps.snapshot = snapshot
	return nil
}

// Compact compacts the log up to the given index. The entry at compactIndex is
// kept as a boundary sentinel so that Term(compactIndex) remains available for
// raft log matching. After compaction, FirstIndex() returns compactIndex + 1.
func (ps *PebbleStorage) Compact(compactIndex uint64) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if compactIndex < ps.firstIndex {
		return raft.ErrCompacted
	}

	if compactIndex > ps.lastIndex {
		return raft.ErrUnavailable
	}

	// Delete all entries before compactIndex, keeping compactIndex as a
	// boundary sentinel so that Term(compactIndex) remains available.
	batch := ps.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	if err := batch.DeleteRange(
		makeLogEntryKey(0),
		makeLogEntryKey(compactIndex),
		nil,
	); err != nil {
		return err
	}

	// FirstIndex advances past the sentinel to compactIndex + 1.
	newFirstIndex := compactIndex + 1
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], newFirstIndex)
	if err := batch.Set(firstIndexKey, buf[:], nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	ps.firstIndex = newFirstIndex

	return nil
}

// SaveRaftState atomically saves both the hard state and entries
func (ps *PebbleStorage) SaveRaftState(
	snap *raftpb.Snapshot,
	confState raftpb.ConfState,
	hardState raftpb.HardState,
	entries []raftpb.Entry,
) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Create a batch for atomic updates
	batch := ps.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()

	// Track new state in locals; only apply to in-memory fields after commit succeeds.
	newSnapshot := ps.snapshot
	newFirstIndex := ps.firstIndex
	newLastIndex := ps.lastIndex

	if snap != nil && !raft.IsEmptySnap(*snap) {
		// Reject any snapshot whose index is not strictly newer than the
		// currently installed snapshot, matching etcd MemoryStorage semantics.
		if ps.snapshot.Metadata.Index >= snap.Metadata.Index {
			return raft.ErrSnapOutOfDate
		}

		newSnapshot = *snap
		newFirstIndex = snap.Metadata.Index + 1
		newLastIndex = snap.Metadata.Index

		// Remove all old log entries up to and including snap.Index.
		// Without this, entries from the prior log linger on disk as
		// orphans after the snapshot resets the index boundaries.
		if err := batch.DeleteRange(
			makeLogEntryKey(0),
			makeLogEntryKey(snap.Metadata.Index+1),
			nil,
		); err != nil {
			return err
		}

		// Save snapshot
		data, err := snap.Marshal()
		if err != nil {
			return err
		}

		if err := batch.Set(snapshotMetaKey, data, nil); err != nil {
			return err
		}

		// Update first and last indices
		binary.BigEndian.PutUint64(ps.uint64Buf[:], newFirstIndex)
		if err := batch.Set(firstIndexKey, ps.uint64Buf[:], nil); err != nil {
			return err
		}
		binary.BigEndian.PutUint64(ps.uint64Buf[:], newLastIndex)
		if err := batch.Set(lastIndexKey, ps.uint64Buf[:], nil); err != nil {
			return err
		}
		confStateData, err := snap.Metadata.ConfState.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(confStateKey, confStateData, nil); err != nil {
			return err
		}
	} else {
		confStateData, err := confState.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(confStateKey, confStateData, nil); err != nil {
			return err
		}
	}
	// Add hard state to batch if it's not empty
	if !raft.IsEmptyHardState(hardState) {
		hardStateData, err := hardState.Marshal()
		if err != nil {
			return err
		}
		if err := batch.Set(hardStateKey, hardStateData, nil); err != nil {
			return err
		}
	}

	// Add entries to batch
	if len(entries) > 0 {
		// Iterate through entries and add them to batch
		for _, entry := range entries {
			// Skip entries that are already compacted
			if entry.Index < newFirstIndex {
				continue
			}

			// Delete any conflicting entries
			//
			// This will only happen once per entries loop because we
			// set the lastIndex below
			if entry.Index <= newLastIndex {
				if err := batch.DeleteRange(
					makeLogEntryKey(entry.Index),
					makeLogEntryKey(newLastIndex+1),
					nil,
				); err != nil {
					return err
				}
			}
			newLastIndex = entry.Index

			// Add the new entry
			key := makeLogEntryKey(entry.Index)
			value, err := entry.Marshal()
			if err != nil {
				return err
			}

			if err := batch.Set(key, value, nil); err != nil {
				return err
			}

		}

		// Add lastIndex update to batch
		binary.BigEndian.PutUint64(ps.uint64Buf[:], newLastIndex)
		if err := batch.Set(lastIndexKey, ps.uint64Buf[:], nil); err != nil {
			return err
		}
	}

	// Commit the entire batch atomically with sync
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	// Only update in-memory state after successful commit
	ps.snapshot = newSnapshot
	ps.firstIndex = newFirstIndex
	ps.lastIndex = newLastIndex

	return nil
}

// Append appends the new entries to storage
func (ps *PebbleStorage) append(entries []raftpb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ps.mu.Lock()
	defer ps.mu.Unlock()

	batch := ps.db.NewBatch()
	defer func() { _ = batch.Close() }()

	newLastIndex := ps.lastIndex

	// Iterate through entries and write them to storage
	for _, entry := range entries {
		// Check for log consistency
		if entry.Index < ps.firstIndex {
			continue
		}

		if entry.Index > newLastIndex+1 {
			return fmt.Errorf(
				"entry index %d is too large, lastIndex is %d",
				entry.Index,
				newLastIndex,
			)
		}

		// Delete conflicting entries
		if entry.Index <= newLastIndex {
			if err := batch.DeleteRange(
				makeLogEntryKey(entry.Index),
				makeLogEntryKey(newLastIndex+1),
				nil,
			); err != nil {
				return err
			}
		}

		// Add the new entry
		key := makeLogEntryKey(entry.Index)
		value, err := entry.Marshal()
		if err != nil {
			return err
		}

		if err := batch.Set(key, value, nil); err != nil {
			return err
		}

		newLastIndex = entry.Index
	}

	// Persist lastIndex
	binary.BigEndian.PutUint64(ps.uint64Buf[:], newLastIndex)
	if err := batch.Set(lastIndexKey, ps.uint64Buf[:], nil); err != nil {
		return err
	}

	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}

	// Update in-memory state only after successful commit.
	ps.lastIndex = newLastIndex

	return nil
}

// SetHardState saves the current HardState
func (ps *PebbleStorage) SetHardState(st raftpb.HardState) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	data, err := st.Marshal()
	if err != nil {
		return err
	}

	return ps.db.Set(hardStateKey, data, pebble.Sync)
}

// Helper methods for loading/saving data

func (ps *PebbleStorage) loadHardState() (raftpb.HardState, error) {
	var hardState raftpb.HardState
	data, closer, err := ps.db.Get(hardStateKey)
	if err != nil {
		return hardState, err
	}
	defer func() { _ = closer.Close() }()

	if err := hardState.Unmarshal(data); err != nil {
		return hardState, err
	}

	return hardState, nil
}

func (ps *PebbleStorage) loadConfState() (raftpb.ConfState, error) {
	var confState raftpb.ConfState
	data, closer, err := ps.db.Get(confStateKey)
	if err != nil {
		return confState, err
	}
	defer func() { _ = closer.Close() }()

	if err := confState.Unmarshal(data); err != nil {
		return confState, err
	}

	return confState, nil
}

func (ps *PebbleStorage) loadFirstIndex() (uint64, error) {
	data, closer, err := ps.db.Get(firstIndexKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return 1, nil // Default is 1 if not found
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = closer.Close() }()

	return binary.BigEndian.Uint64(data), nil
}

func (ps *PebbleStorage) loadLastIndex() (uint64, error) {
	data, closer, err := ps.db.Get(lastIndexKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, nil // Default is 0 if not found
	}
	if err != nil {
		return 0, err
	}
	defer func() { _ = closer.Close() }()

	return binary.BigEndian.Uint64(data), nil
}

func (ps *PebbleStorage) LoadSnapshot() (snapshot *raftpb.Snapshot, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	data, closer, err := ps.db.Get(snapshotMetaKey)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()

	snapshot = &raftpb.Snapshot{}
	if err := snapshot.Unmarshal(data); err != nil {
		return nil, err
	}

	return snapshot, nil
}

func (ps *PebbleStorage) getEntry(index uint64) (entry raftpb.Entry, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)

	key := makeLogEntryKey(index)
	value, closer, err := ps.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return entry, raft.ErrUnavailable
		}
		return entry, err
	}
	defer func() { _ = closer.Close() }()

	if err := entry.Unmarshal(value); err != nil {
		return entry, err
	}

	return entry, nil
}

// makeLogEntryKey creates a key for a log entry at the given index
func makeLogEntryKey(index uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], index)
	return append(logEntryKeyPrefix, buf[:]...)
}

// GetAllEntries retrieves all log entries currently available in the storage.
// Note: This might be memory-intensive for large logs.
func (ps *PebbleStorage) GetAllEntries() ([]raftpb.Entry, error) {
	ps.mu.RLock() // Lock for reading storage state (though we might not need it if only using iterator)
	defer ps.mu.RUnlock()

	entries := make([]raftpb.Entry, 0) // Start with an empty slice, size unknown

	// Define iteration options for prefix scan
	prefixIterOptions := &pebble.IterOptions{
		LowerBound: logEntryKeyPrefix,
		UpperBound: append(logEntryKeyPrefix, 0xFF),
	}

	// Create an iterator
	iter, err := ps.db.NewIter(prefixIterOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to create pebble iterator: %w", err)
	}
	defer func() { _ = iter.Close() }() // Ensure iterator is closed

	// Iterate through the keys with the specified prefix
	for iter.First(); iter.Valid(); iter.Next() {
		value := iter.Value()
		if value == nil {
			// This shouldn't happen with Valid() check, but safety first
			ps.logger.Warn(
				"Iterator returned nil value for valid key",
				zap.String("key", types.FormatKey(iter.Key())),
			)
			continue
		}

		var entry raftpb.Entry
		// Make a copy of the value byte slice before unmarshalling
		// as the slice returned by iter.Value() may be reused.
		valueCopy := make([]byte, len(value))
		copy(valueCopy, value)

		if err := entry.Unmarshal(valueCopy); err != nil {
			// Log error and potentially return, depending on desired behavior
			ps.logger.Error(
				"Failed to unmarshal log entry",
				zap.String("key", types.FormatKey(iter.Key())),
				zap.Error(err),
			)
			// Decide if we should stop or continue. Let's stop on error.
			return nil, fmt.Errorf("failed to unmarshal entry for key %x: %w", iter.Key(), err)
		}
		entries = append(entries, entry)
	}

	// Check for iterator errors after the loop
	if err := iter.Error(); err != nil {
		return nil, fmt.Errorf("iterator error during prefix scan: %w", err)
	}

	return entries, nil
}

func (ps *PebbleStorage) AddPeers(peers ...common.Peer) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	ps.mu.Lock()
	defer ps.mu.Unlock()
	batch := ps.db.NewBatch()
	defer func() {
		_ = batch.Close()
	}()
	for _, peer := range peers {
		err := batch.Set(
			encoding.EncodeUint64Ascending(bytes.Clone(peerPrefix), uint64(peer.ID)),
			[]byte(peer.URL),
			nil,
		)
		if err != nil {
			return fmt.Errorf("adding peer %d: %w", peer.ID, err)
		}
	}
	return batch.Commit(pebble.Sync)
}

func (ps *PebbleStorage) RemovePeer(id types.ID) (err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	ps.mu.Lock()
	defer ps.mu.Unlock()

	return ps.db.Delete(
		encoding.EncodeUint64Ascending(bytes.Clone(peerPrefix), uint64(id)),
		pebble.Sync,
	)
}

func (ps *PebbleStorage) ListPeers() (peers map[types.ID]string, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	peers = make(map[types.ID]string)
	iter, err := ps.db.NewIter(&pebble.IterOptions{
		LowerBound: peerPrefix,
		UpperBound: encoding.EncodeUint64Ascending(bytes.Clone(peerPrefix), ^uint64(0)),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if value == nil {
			continue
		}
		_, id, err := encoding.DecodeUint64Ascending(key[len(peerPrefix):])
		if err != nil {
			return nil, fmt.Errorf("decoding peer id for key %s: %w", key, err)
		}
		peers[types.ID(id)] = string(value)
	}

	if err := iter.Error(); err != nil {
		return nil, err
	}

	return peers, nil
}
