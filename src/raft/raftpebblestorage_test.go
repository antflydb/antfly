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
	"os"
	"testing"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/zap/zaptest"
)

// Helper function to create a temporary PebbleStorage for testing
func newTestPebbleStorage(t *testing.T) (*PebbleStorage, func()) {
	t.Helper()
	require := require.New(t)
	assert := assert.New(t)

	dir := t.TempDir()
	ps, err := NewPebbleStorage(zaptest.NewLogger(t), dir, nil)
	require.NoError(err, "Failed to create PebbleStorage")

	cleanup := func() {
		err := ps.Close()
		// Assert that the error is either nil or pebble.ErrClosed
		if err != nil {
			assert.ErrorIs(
				err,
				pebble.ErrClosed,
				"Failed to close PebbleStorage with unexpected error",
			)
		}
		// No need to explicitly remove dir, t.TempDir() handles it
	}
	return ps, cleanup
}

// TestNewPebbleStorage tests the creation and initialization of PebbleStorage
func TestNewPebbleStorage(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	dir := t.TempDir()
	ps, err := NewPebbleStorage(zaptest.NewLogger(t), dir, nil)
	require.NoError(err, "NewPebbleStorage failed")
	defer ps.Close()

	// Check initial state
	assert.Equal(uint64(1), ps.firstIndex, "Expected initial firstIndex to be 1")
	assert.Equal(uint64(0), ps.lastIndex, "Expected initial lastIndex to be 0")
	assert.True(raft.IsEmptySnap(ps.snapshot), "Expected initial snapshot to be empty")

	// Check if DB files were created
	files, err := os.ReadDir(dir)
	require.NoError(err, "Failed to read test directory")
	assert.NotEmpty(
		files,
		"Expected database files to be created in %s, but directory is empty",
		dir,
	)

	// Test reopening storage
	require.NoError(ps.Close(), "Failed to close storage")
	ps, err = NewPebbleStorage(zaptest.NewLogger(t), dir, nil)
	require.NoError(err, "Failed to reopen PebbleStorage")
	defer ps.Close()

	assert.Equal(uint64(1), ps.firstIndex, "Expected firstIndex after reopen to be 1")
	assert.Equal(uint64(0), ps.lastIndex, "Expected lastIndex after reopen to be 0")
}

// TestInitialState tests loading the initial HardState and ConfState
func TestInitialState(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Initial state should be empty
	hs, cs, err := ps.InitialState()
	require.NoError(err, "InitialState failed")
	assert.True(raft.IsEmptyHardState(hs), "Expected empty HardState initially")
	// Use assert.Equal for ConfState comparison
	assert.Equal(raftpb.ConfState{}, cs, "Expected empty ConfState initially")

	// Save some state
	testHS := raftpb.HardState{Term: 1, Vote: 1, Commit: 1}
	testCS := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	testEntries := []raftpb.Entry{
		{Index: 1, Term: 1},
	} // Need at least one entry for commit index > 0

	err = ps.SaveRaftState(&raftpb.Snapshot{}, testCS, testHS, testEntries)
	require.NoError(err, "SaveRaftState failed")

	// Reload and check
	hs, cs, err = ps.InitialState()
	require.NoError(err, "InitialState after save failed")
	assert.Equal(testHS, hs, "HardState mismatch")
	assert.Equal(testCS, cs, "ConfState mismatch")
}

// TestEntries tests retrieving log entries
func TestEntries(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	testEntries := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("entry1")},
		{Index: 2, Term: 1, Data: []byte("entry2")},
		{Index: 3, Term: 2, Data: []byte("entry3")},
		{Index: 4, Term: 2, Data: []byte("entry4")},
	}

	err := ps.append(testEntries)
	require.NoError(err, "Append failed")

	tests := []struct {
		name      string
		lo, hi    uint64
		maxSize   uint64
		want      []raftpb.Entry
		wantErr   error
		wantPanic bool // For cases where raft might panic
	}{
		{"all entries", 1, 5, 1000, testEntries, nil, false},
		{"subset", 2, 4, 1000, testEntries[1:3], nil, false},
		{"single entry", 3, 4, 1000, testEntries[2:3], nil, false},
		{"empty range", 3, 3, 1000, nil, nil, false},
		{
			"maxSize limit",
			1,
			5,
			uint64(testEntries[0].Size() + testEntries[1].Size()),
			testEntries[0:2],
			nil,
			false,
		},
		{"maxSize exact", 1, 5, uint64(testEntries[0].Size()), testEntries[0:1], nil, false},
		{"lo below firstIndex", 0, 3, 1000, nil, raft.ErrCompacted, false},
		{
			"hi above lastIndex",
			3,
			6,
			1000,
			nil,
			raft.ErrUnavailable,
			false,
		}, // Raft asks for lastIndex+1
		{"lo equals hi", 2, 2, 1000, nil, nil, false},
		{"lo > hi", 4, 2, 1000, nil, nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				entriesFunc := func() {
					got, err := ps.Entries(tt.lo, tt.hi, tt.maxSize)

					if tt.wantErr != nil {
						assert.ErrorIs(
							err,
							tt.wantErr,
							"Entries(%d, %d, %d) error mismatch",
							tt.lo,
							tt.hi,
							tt.maxSize,
						)
						assert.Nil(
							got,
							"Expected nil entries when error is %v, but got %v",
							tt.wantErr,
							got,
						)
					} else {
						assert.NoError(err, "Entries(%d, %d, %d) unexpected error", tt.lo, tt.hi, tt.maxSize)
						// Handle nil vs empty slice comparison
						if tt.want == nil {
							// If want is nil, expect got to be nil as well (usually happens with errors, handled above)
							// Or if no error, want should ideally be an empty slice for clarity.
							// Assuming if want is nil and no error, the expected result is an empty slice.
							assert.Empty(got, "Entries(%d, %d, %d) expected empty slice, got non-empty", tt.lo, tt.hi, tt.maxSize)
						} else {
							assert.Equal(tt.want, got, "Entries(%d, %d, %d) result mismatch", tt.lo, tt.hi, tt.maxSize)
						}
					}
				}

				if tt.wantPanic {
					assert.Panics(entriesFunc, "Expected panic but did not occur")
				} else {
					assert.NotPanics(entriesFunc, "Panic occurred unexpectedly")
				}
			}()
		})
	}
}

// TestTerm tests retrieving the term of a log entry
func TestTerm(t *testing.T) {
	require := require.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	testEntries := []raftpb.Entry{
		{Index: 3, Term: 1}, // Compacted up to index 2
		{Index: 4, Term: 2},
		{Index: 5, Term: 2},
	}
	ps.firstIndex = 3 // Simulate compaction
	ps.lastIndex = 5
	err := ps.append(testEntries) // Append will handle index checks based on ps state
	require.NoError(err, "Append failed")

	tests := []struct {
		name    string
		index   uint64
		want    uint64
		wantErr error
	}{
		{"valid index", 4, 2, nil},
		{"last index", 5, 2, nil},
		{"first available index", 3, 1, nil},
		{"compacted index", 2, 0, raft.ErrCompacted},
		{"index zero", 0, 0, nil}, // Special case
		{"unavailable index", 6, 0, raft.ErrUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Re-scope assert for the subtest if needed, or use the parent's assert
			assert := assert.New(t)
			got, err := ps.Term(tt.index)

			if tt.wantErr != nil {
				assert.ErrorIs(err, tt.wantErr, "Term(%d) error mismatch", tt.index)
			} else {
				assert.NoError(err, "Term(%d) unexpected error", tt.index)
				assert.Equal(tt.want, got, "Term(%d) result mismatch", tt.index)
			}
		})
	}
}

// TestFirstLastIndex tests retrieving the first and last log indices
func TestFirstLastIndex(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Initial state
	first, err := ps.FirstIndex()
	require.NoError(err, "FirstIndex failed")
	assert.Equal(uint64(1), first, "Expected initial first index 1")

	last, err := ps.LastIndex()
	require.NoError(err, "LastIndex failed")
	assert.Equal(uint64(0), last, "Expected initial last index 0")

	// After appending entries
	testEntries := []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	err = ps.append(testEntries)
	require.NoError(err, "Append failed")

	first, err = ps.FirstIndex()
	require.NoError(err, "FirstIndex after append failed")
	assert.Equal(uint64(1), first, "Expected first index 1 after append")

	last, err = ps.LastIndex()
	require.NoError(err, "LastIndex after append failed")
	assert.Equal(uint64(2), last, "Expected last index 2 after append")

	// After compaction — sentinel at index 2, FirstIndex becomes 3
	err = ps.Compact(2)
	require.NoError(err, "Compact failed")

	first, err = ps.FirstIndex()
	require.NoError(err, "FirstIndex after compact failed")
	assert.Equal(uint64(3), first, "Expected first index 3 after compact")

	last, err = ps.LastIndex() // Last index should remain unchanged
	require.NoError(err, "LastIndex after compact failed")
	assert.Equal(uint64(2), last, "Expected last index 2 after compact")
}

// TestPebbleStorageSnapshot tests creating and retrieving snapshots
func TestPebbleStorageSnapshot(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Initial snapshot should return ErrSnapshotTemporarilyUnavailable
	snap, err := ps.Snapshot()
	require.ErrorIs(err, raft.ErrSnapshotTemporarilyUnavailable, "Expected ErrSnapshotTemporarilyUnavailable when no snapshot exists")
	assert.True(raft.IsEmptySnap(snap), "Expected empty snapshot initially")

	// Need some entries to create a snapshot
	testEntries := []raftpb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
	}
	err = ps.append(testEntries)
	require.NoError(err, "Append failed")

	// Create a snapshot
	snapIndex := uint64(2)
	snapTerm, err := ps.Term(snapIndex)
	require.NoError(err, "Term failed")

	snapData := []byte("snapshot data")
	snapCS := raftpb.ConfState{Voters: []uint64{1, 2}}
	err = ps.CreateSnapshot(snapIndex, &snapCS, snapData)
	require.NoError(err, "CreateSnapshot failed")

	// Retrieve the snapshot
	snap, err = ps.Snapshot()
	require.NoError(err, "Snapshot after create failed")

	expectedSnap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     snapIndex,
			Term:      snapTerm,
			ConfState: snapCS,
		},
		Data: snapData,
	}
	assert.Equal(expectedSnap, snap, "Snapshot mismatch")

	// Test creating snapshot with invalid index
	err = ps.CreateSnapshot(4, &snapCS, snapData) // Index 4 > lastIndex (3)
	assert.Error(err, "Expected error when creating snapshot with index > lastIndex")

	err = ps.CreateSnapshot(1, &snapCS, snapData) // Index 1 < current snapshot index (2)
	assert.Error(err, "Expected error when creating snapshot with index < current snapshot index")

	// Test reopening and checking snapshot
	require.NoError(ps.Close(), "Failed to close storage")

	psReopened, err := NewPebbleStorage(zaptest.NewLogger(t), ps.dir, nil)
	require.NoError(err, "Failed to reopen PebbleStorage")
	defer psReopened.Close()

	snap, err = psReopened.Snapshot()
	require.NoError(err, "Snapshot after reopen failed")
	assert.Equal(expectedSnap, snap, "Snapshot mismatch after reopen")
}

// TestCompact tests log compaction
func TestCompact(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	testEntries := []raftpb.Entry{
		{Index: 1, Term: 1},
		{Index: 2, Term: 1},
		{Index: 3, Term: 2},
		{Index: 4, Term: 2},
		{Index: 5, Term: 3},
	}
	err := ps.append(testEntries)
	require.NoError(err, "Append failed")

	compactIndex := uint64(3) // Keep entry 3 as sentinel, FirstIndex becomes 4
	err = ps.Compact(compactIndex)
	require.NoError(err, "Compact failed")

	// Check first index (sentinel at compactIndex, first real entry at compactIndex+1)
	first, err := ps.FirstIndex()
	require.NoError(err, "FirstIndex after compact failed")
	assert.Equal(compactIndex+1, first, "First index mismatch after compact")

	// Check last index (should be unchanged)
	last, err := ps.LastIndex()
	require.NoError(err, "LastIndex after compact failed")
	assert.Equal(uint64(5), last, "Last index mismatch after compact")

	// Entries before FirstIndex should be compacted
	_, err = ps.Entries(1, 4, 1000)
	assert.ErrorIs(err, raft.ErrCompacted, "Getting entries [1, 4) should return ErrCompacted")

	// Term of compacted entries should fail
	_, err = ps.Term(2)
	assert.ErrorIs(err, raft.ErrCompacted, "Getting term for index 2 should return ErrCompacted")

	// Term of sentinel (compactIndex) should still work for log matching
	sentinelTerm, err := ps.Term(compactIndex)
	require.NoError(err, "Term(compactIndex) should succeed via sentinel")
	assert.Equal(uint64(2), sentinelTerm, "Sentinel term mismatch")

	// Available entries start at FirstIndex (compactIndex+1)
	entries, err := ps.Entries(compactIndex+1, 6, 1000)
	require.NoError(err, "Entries(4, 6) failed after compact")
	expectedEntries := testEntries[3:] // Entries with index 4, 5
	assert.Equal(expectedEntries, entries, "Entries(4, 6) mismatch after compact")

	// Test compacting already compacted index
	err = ps.Compact(2)
	assert.ErrorIs(
		err,
		raft.ErrCompacted,
		"Compacting index 2 (< firstIndex) should return ErrCompacted",
	)

	// Test compacting unavailable index
	err = ps.Compact(6)
	assert.ErrorIs(
		err,
		raft.ErrUnavailable,
		"Compacting index 6 (> lastIndex) should return ErrUnavailable",
	)
}

// TestAppend tests appending entries, including overwriting
func TestAppend(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Initial append
	entries1 := []raftpb.Entry{
		{Index: 1, Term: 1, Data: []byte("a")},
		{Index: 2, Term: 1, Data: []byte("b")},
	}
	err := ps.append(entries1)
	require.NoError(err, "Initial Append failed")

	last, err := ps.LastIndex()
	require.NoError(err)
	assert.Equal(uint64(2), last, "Last index after initial append")

	ents, err := ps.Entries(1, 3, 1000)
	require.NoError(err)
	assert.Equal(entries1, ents, "Entries mismatch after initial append")

	// Append more entries
	entries2 := []raftpb.Entry{
		{Index: 3, Term: 2, Data: []byte("c")},
		{Index: 4, Term: 2, Data: []byte("d")},
	}
	err = ps.append(entries2)
	require.NoError(err, "Second Append failed")

	last, err = ps.LastIndex()
	require.NoError(err)
	assert.Equal(uint64(4), last, "Last index after second append")

	ents, err = ps.Entries(1, 5, 1000)
	require.NoError(err)
	expectedAll := append(entries1, entries2...)
	assert.Equal(expectedAll, ents, "Entries mismatch after second append")

	// Append overlapping/overwriting entries
	entries3 := []raftpb.Entry{
		{Index: 3, Term: 3, Data: []byte("c-new")}, // Overwrite index 3
		{Index: 4, Term: 3, Data: []byte("d-new")}, // Overwrite index 4
		{Index: 5, Term: 3, Data: []byte("e")},     // New entry
	}
	err = ps.append(entries3)
	require.NoError(err, "Third Append (overwrite) failed")

	last, err = ps.LastIndex()
	require.NoError(err)
	assert.Equal(uint64(5), last, "Last index after overwrite")

	// Check final state
	ents, err = ps.Entries(1, 6, 1000)
	require.NoError(err)
	expectedFinal := []raftpb.Entry{
		entries1[0], // Index 1
		entries1[1], // Index 2
		entries3[0], // Index 3 (new)
		entries3[1], // Index 4 (new)
		entries3[2], // Index 5 (new)
	}
	assert.Equal(expectedFinal, ents, "Entries mismatch after overwrite")

	// Test appending empty slice (should be no-op)
	currentLast, err := ps.LastIndex()
	require.NoError(err)
	err = ps.append([]raftpb.Entry{})
	assert.NoError(err, "Append with empty slice failed")
	last, err = ps.LastIndex()
	require.NoError(err)
	assert.Equal(currentLast, last, "Last index changed after appending empty slice")

	// Test appending entry with gap (should fail)
	err = ps.append([]raftpb.Entry{{Index: 7, Term: 4}})
	assert.Error(err, "Expected error when appending entry with index gap")
}

// TestSaveRaftState tests atomically saving HardState, ConfState, and Entries
func TestSaveRaftState(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Initial state
	hs1 := raftpb.HardState{Term: 1, Vote: 1, Commit: 1}
	cs1 := raftpb.ConfState{Voters: []uint64{1}}
	ents1 := []raftpb.Entry{{Index: 1, Term: 1}}

	err := ps.SaveRaftState(&raftpb.Snapshot{}, cs1, hs1, ents1)
	require.NoError(err, "SaveRaftState 1 failed")

	// Verify state 1
	rHs, rCs, err := ps.InitialState()
	require.NoError(err)
	assert.Equal(hs1, rHs, "HS mismatch after save 1")
	assert.Equal(cs1, rCs, "CS mismatch after save 1")

	rEnts, err := ps.Entries(1, 2, 1000)
	require.NoError(err)
	assert.Equal(ents1, rEnts, "Entries mismatch after save 1")

	rLast, err := ps.LastIndex()
	require.NoError(err)
	assert.Equal(uint64(1), rLast, "LastIndex mismatch after save 1")

	// Save new state, overwriting previous entries
	hs2 := raftpb.HardState{Term: 2, Vote: 2, Commit: 2}
	cs2 := raftpb.ConfState{Voters: []uint64{1, 2}}
	ents2 := []raftpb.Entry{
		{Index: 1, Term: 2}, // Overwrite index 1
		{Index: 2, Term: 2}, // New index 2
	}
	err = ps.SaveRaftState(&raftpb.Snapshot{}, cs2, hs2, ents2)
	require.NoError(err, "SaveRaftState 2 failed")

	// Verify state 2
	rHs, rCs, err = ps.InitialState()
	require.NoError(err)
	assert.Equal(hs2, rHs, "HS mismatch after save 2")
	assert.Equal(cs2, rCs, "CS mismatch after save 2")

	rEnts, err = ps.Entries(1, 3, 1000)
	require.NoError(err)
	assert.Equal(ents2, rEnts, "Entries mismatch after save 2")

	rLast, err = ps.LastIndex()
	require.NoError(err)
	assert.Equal(uint64(2), rLast, "LastIndex mismatch after save 2")

	// Save state with a snapshot
	snap3 := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     3,
			Term:      3,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Data: []byte("snap3"),
	}
	hs3 := raftpb.HardState{Term: 3, Vote: 3, Commit: 3} // HardState might be updated with snapshot
	cs3 := snap3.Metadata.ConfState                      // ConfState comes from snapshot
	ents3 := []raftpb.Entry{}                            // No new entries with snapshot usually

	err = ps.SaveRaftState(
		&snap3,
		cs3,
		hs3,
		ents3,
	) // cs3 is redundant here as snapshot takes precedence
	require.NoError(err, "SaveRaftState 3 (snapshot) failed")

	// Verify state 3
	rHs, rCs, err = ps.InitialState()
	require.NoError(err)
	assert.Equal(hs3, rHs, "HS mismatch after save 3")
	assert.Equal(cs3, rCs, "CS mismatch after save 3")

	rSnap, err := ps.Snapshot()
	require.NoError(err)
	assert.Equal(snap3, rSnap, "Snapshot mismatch after save 3")

	rFirst, err := ps.FirstIndex()
	require.NoError(err)
	assert.Equal(
		snap3.Metadata.Index+1,
		rFirst,
		"FirstIndex mismatch after save 3",
	) // First index becomes snap.Index + 1

	rLast, err = ps.LastIndex()
	require.NoError(err)
	assert.Equal(
		snap3.Metadata.Index,
		rLast,
		"LastIndex mismatch after save 3",
	) // Last index becomes snap.Index

	// Term(snap.Index) must remain available for raft log matching
	snapTerm, err := ps.Term(snap3.Metadata.Index)
	require.NoError(err, "Term(snap.Index) must succeed after snapshot install")
	assert.Equal(snap3.Metadata.Term, snapTerm, "Term(snap.Index) mismatch")

	// Entries before snapshot index should be gone
	_, err = ps.Entries(1, 4, 1000)
	assert.ErrorIs(err, raft.ErrCompacted, "Expected raft.ErrCompacted for entries before snapshot")

	// Verify firstIndex survives restart (regression: was persisted as snap.Index not snap.Index+1)
	require.NoError(ps.Close(), "Failed to close storage")
	psReopened, err := NewPebbleStorage(zaptest.NewLogger(t), ps.dir, nil)
	require.NoError(err, "Failed to reopen PebbleStorage after snapshot")
	defer psReopened.Close()

	rFirst, err = psReopened.FirstIndex()
	require.NoError(err)
	assert.Equal(snap3.Metadata.Index+1, rFirst, "FirstIndex mismatch after reopen with snapshot")

	rLast, err = psReopened.LastIndex()
	require.NoError(err)
	assert.Equal(snap3.Metadata.Index, rLast, "LastIndex mismatch after reopen with snapshot")
}

// TestSaveRaftStateSnapOutOfDate verifies that SaveRaftState unconditionally
// rejects a snapshot whose index is not strictly newer than the current one,
// even when the call also carries a non-empty HardState or entries.
func TestSaveRaftStateSnapOutOfDate(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Install a snapshot at index 5.
	snap5 := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     5,
			Term:      3,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Data: []byte("snap5"),
	}
	hs := raftpb.HardState{Term: 3, Vote: 1, Commit: 5}
	err := ps.SaveRaftState(&snap5, snap5.Metadata.ConfState, hs, nil)
	require.NoError(err, "initial snapshot install failed")

	// Equal index — must be rejected.
	snapEqual := snap5
	snapEqual.Data = []byte("snap5-dup")
	err = ps.SaveRaftState(&snapEqual, snapEqual.Metadata.ConfState, raftpb.HardState{}, nil)
	assert.ErrorIs(err, raft.ErrSnapOutOfDate, "snapshot with equal index should be rejected")

	// Older index — must be rejected.
	snapOlder := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     3,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2}},
		},
		Data: []byte("snap3"),
	}
	err = ps.SaveRaftState(&snapOlder, snapOlder.Metadata.ConfState, raftpb.HardState{}, nil)
	assert.ErrorIs(err, raft.ErrSnapOutOfDate, "snapshot with older index should be rejected")

	// Older snapshot bundled with non-empty HardState — must still be rejected.
	// This is the case the old code would have incorrectly accepted.
	hs2 := raftpb.HardState{Term: 4, Vote: 2, Commit: 6}
	err = ps.SaveRaftState(&snapOlder, snapOlder.Metadata.ConfState, hs2, nil)
	assert.ErrorIs(err, raft.ErrSnapOutOfDate,
		"stale snapshot with non-empty HardState should still be rejected")

	// Newer index — must be accepted.
	snap10 := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     10,
			Term:      5,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3}},
		},
		Data: []byte("snap10"),
	}
	err = ps.SaveRaftState(&snap10, snap10.Metadata.ConfState, raftpb.HardState{Term: 5, Vote: 1, Commit: 10}, nil)
	assert.NoError(err, "newer snapshot should be accepted")

	rSnap, err := ps.Snapshot()
	require.NoError(err)
	assert.Equal(snap10, rSnap, "snapshot should be updated to snap10")
}

// TestSetHardState tests saving only the HardState
func TestSetHardState(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Initial state
	hs1 := raftpb.HardState{Term: 1, Vote: 1, Commit: 1}
	err := ps.SetHardState(hs1)
	require.NoError(err, "SetHardState 1 failed")

	rHs, _, err := ps.InitialState()
	require.NoError(err)
	assert.Equal(hs1, rHs, "HardState mismatch after set 1")

	// Update state
	hs2 := raftpb.HardState{Term: 2, Vote: 1, Commit: 2}
	err = ps.SetHardState(hs2)
	require.NoError(err, "SetHardState 2 failed")

	rHs, _, err = ps.InitialState()
	require.NoError(err)
	assert.Equal(hs2, rHs, "HardState mismatch after set 2")
}

// TestMakeLogEntryKey ensures keys are generated correctly and are distinct
func TestMakeLogEntryKey(t *testing.T) {
	assert := assert.New(t)

	key1 := makeLogEntryKey(1)
	key2 := makeLogEntryKey(2)
	key100 := makeLogEntryKey(100)
	keyMax := makeLogEntryKey(^uint64(0)) // Max uint64

	assert.NotEqual(key1, key2, "Keys for index 1 and 2 should not be equal")
	assert.NotEqual(key2, key100, "Keys for index 2 and 100 should not be equal")
	assert.True(bytes.HasPrefix(key1, logEntryKeyPrefix), "Key should have expected prefix")
	assert.Len(key1, len(logEntryKeyPrefix)+8, "Key should have correct length")
	assert.Len(keyMax, len(logEntryKeyPrefix)+8, "Max key should have correct length")

	// Check ordering (important for Pebble iteration)
	// bytes.Compare returns -1 if a < b, 0 if a == b, 1 if a > b
	assert.Negative(bytes.Compare(key1, key2), "Key 1 should sort before Key 2")
	assert.Negative(bytes.Compare(key2, key100), "Key 2 should sort before Key 100")
	assert.Negative(bytes.Compare(key100, keyMax), "Key 100 should sort before Key Max")
}

// TestAddPeersDurability verifies that peers added via AddPeers are durably
// persisted and survive a close/reopen cycle. This is a regression test for a
// bug where AddPeers wrote to ps.db directly instead of the batch, so the
// batch.Commit was a no-op and peers were lost on restart.
func TestAddPeersDurability(t *testing.T) {
	require := require.New(t)

	dir := t.TempDir()
	ps, err := NewPebbleStorage(zaptest.NewLogger(t), dir, nil)
	require.NoError(err)

	// Add peers
	peers := []common.Peer{
		{ID: types.ID(2), URL: "http://node2:8080"},
		{ID: types.ID(3), URL: "http://node3:8080"},
		{ID: types.ID(4), URL: "http://node4:8080"},
	}
	require.NoError(ps.AddPeers(peers...))

	// Verify peers are readable before close
	listed, err := ps.ListPeers()
	require.NoError(err)
	require.Len(listed, 3)
	require.Equal("http://node2:8080", listed[types.ID(2)])
	require.Equal("http://node3:8080", listed[types.ID(3)])
	require.Equal("http://node4:8080", listed[types.ID(4)])

	// Close and reopen
	require.NoError(ps.Close())
	ps, err = NewPebbleStorage(zaptest.NewLogger(t), dir, nil)
	require.NoError(err)
	defer ps.Close()

	// Peers must survive the restart
	listed, err = ps.ListPeers()
	require.NoError(err)
	require.Len(listed, 3, "peers should be durably persisted across restart")
	require.Equal("http://node2:8080", listed[types.ID(2)])
	require.Equal("http://node3:8080", listed[types.ID(3)])
	require.Equal("http://node4:8080", listed[types.ID(4)])
}

// TestSaveRaftStateAtomicity verifies that SaveRaftState does not mutate
// in-memory state when the batch commit fails. This is a regression test for
// a bug where in-memory fields (snapshot, firstIndex, lastIndex) were updated
// before the batch was committed, leaving torn state on disk failure.
func TestSaveRaftStateAtomicity(t *testing.T) {
	require := require.New(t)
	assert := assert.New(t)

	ps, cleanup := newTestPebbleStorage(t)
	defer cleanup()

	// Set up initial state
	hs := raftpb.HardState{Term: 1, Vote: 1, Commit: 1}
	cs := raftpb.ConfState{Voters: []uint64{1, 2, 3}}
	entries := []raftpb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1}}
	require.NoError(ps.SaveRaftState(&raftpb.Snapshot{}, cs, hs, entries))

	// Record the state before the failed write
	origFirstIndex := ps.firstIndex
	origLastIndex := ps.lastIndex
	origSnapshot := ps.snapshot

	assert.Equal(uint64(1), origFirstIndex)
	assert.Equal(uint64(2), origLastIndex)
	assert.True(raft.IsEmptySnap(origSnapshot))

	// Close the Pebble DB to force the next SaveRaftState to fail
	require.NoError(ps.db.Close())

	// Attempt SaveRaftState with a snapshot — this should fail
	snap := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     10,
			Term:      5,
			ConfState: raftpb.ConfState{Voters: []uint64{1, 2, 3, 4, 5}},
		},
		Data: []byte("snapshot-data"),
	}
	hs2 := raftpb.HardState{Term: 5, Vote: 2, Commit: 10}
	cs2 := snap.Metadata.ConfState

	err := ps.SaveRaftState(&snap, cs2, hs2, nil)
	require.Error(err, "SaveRaftState should fail on closed DB")

	// In-memory state must be unchanged after the failed commit
	assert.Equal(origFirstIndex, ps.firstIndex,
		"firstIndex must not change after failed SaveRaftState")
	assert.Equal(origLastIndex, ps.lastIndex,
		"lastIndex must not change after failed SaveRaftState")
	assert.Equal(origSnapshot, ps.snapshot,
		"snapshot must not change after failed SaveRaftState")
}
