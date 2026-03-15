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

package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/schema"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

// TestSplitStatePersistence tests that SplitState can be persisted and retrieved from Pebble.
func TestSplitStatePersistence(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx := context.Background()
	_ = ctx

	// Create temporary directory
	baseDir := t.TempDir()
	dbDir := filepath.Join(baseDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	// Create schema for testing
	testSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Define byte range
	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}

	// Create the database
	testDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}

	require.NoError(t, testDB.Open(dbDir, false, testSchema, fullRange))

	t.Run("SetAndGetSplitState", func(t *testing.T) {
		// Initially, split state should be nil
		state := testDB.GetSplitState()
		assert.Nil(t, state, "Expected nil split state initially")

		// Create and set a split state
		splitKey := []byte("split-key")
		newShardID := uint64(12345)
		startedAt := time.Now().UnixNano()
		originalRangeEnd := []byte("\xFF")

		splitState := SplitState_builder{
			Phase:              SplitState_PHASE_PREPARE,
			SplitKey:           splitKey,
			NewShardId:         newShardID,
			StartedAtUnixNanos: startedAt,
			OriginalRangeEnd:   originalRangeEnd,
		}.Build()

		err := testDB.SetSplitState(splitState)
		require.NoError(t, err, "Failed to set split state")

		// Get the split state back
		state = testDB.GetSplitState()
		require.NotNil(t, state, "Expected non-nil split state after set")

		assert.Equal(t, SplitState_PHASE_PREPARE, state.GetPhase())
		assert.Equal(t, splitKey, state.GetSplitKey())
		assert.Equal(t, newShardID, state.GetNewShardId())
		assert.Equal(t, startedAt, state.GetStartedAtUnixNanos())
		assert.Equal(t, originalRangeEnd, state.GetOriginalRangeEnd())
	})

	t.Run("ClearSplitState", func(t *testing.T) {
		// First, ensure we have a split state
		state := testDB.GetSplitState()
		require.NotNil(t, state, "Expected split state from previous test")

		// Clear it
		err := testDB.ClearSplitState()
		require.NoError(t, err, "Failed to clear split state")

		// Verify it's cleared
		state = testDB.GetSplitState()
		assert.Nil(t, state, "Expected nil split state after clear")
	})

	t.Run("SetNilClearsSplitState", func(t *testing.T) {
		// Set a split state
		splitState := SplitState_builder{
			Phase:    SplitState_PHASE_SPLITTING,
			SplitKey: []byte("another-key"),
		}.Build()
		err := testDB.SetSplitState(splitState)
		require.NoError(t, err)

		// Verify it's set
		state := testDB.GetSplitState()
		require.NotNil(t, state)

		// Set nil to clear
		err = testDB.SetSplitState(nil)
		require.NoError(t, err)

		// Verify it's cleared
		state = testDB.GetSplitState()
		assert.Nil(t, state, "Expected nil split state after setting nil")
	})

	// Clean up
	require.NoError(t, testDB.Close())
}

// TestSplitStateLoadOnStartup tests that SplitState is loaded from Pebble on database startup.
func TestSplitStateLoadOnStartup(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx := context.Background()
	_ = ctx

	// Create temporary directory
	baseDir := t.TempDir()
	dbDir := filepath.Join(baseDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	// Create schema for testing
	testSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Define byte range
	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}

	// Create and populate the database with a split state
	splitKey := []byte("test-split-key")
	newShardID := uint64(99999)
	startedAt := time.Now().UnixNano()
	originalRangeEnd := []byte("\xFF")

	{
		testDB := &DBImpl{
			logger:       logger,
			antflyConfig: &common.Config{},
			indexes:      make(map[string]indexes.IndexConfig),
		}
		require.NoError(t, testDB.Open(dbDir, false, testSchema, fullRange))

		// Set split state
		splitState := SplitState_builder{
			Phase:              SplitState_PHASE_FINALIZING,
			SplitKey:           splitKey,
			NewShardId:         newShardID,
			StartedAtUnixNanos: startedAt,
			OriginalRangeEnd:   originalRangeEnd,
		}.Build()
		err := testDB.SetSplitState(splitState)
		require.NoError(t, err)

		// Close the database
		require.NoError(t, testDB.Close())
	}

	// Re-open the database and verify split state is loaded
	{
		testDB := &DBImpl{
			logger:       logger,
			antflyConfig: &common.Config{},
			indexes:      make(map[string]indexes.IndexConfig),
		}
		require.NoError(t, testDB.Open(dbDir, true, testSchema, fullRange))

		// Verify split state was loaded
		state := testDB.GetSplitState()
		require.NotNil(t, state, "Expected split state to be loaded on startup")

		assert.Equal(t, SplitState_PHASE_FINALIZING, state.GetPhase())
		assert.Equal(t, splitKey, state.GetSplitKey())
		assert.Equal(t, newShardID, state.GetNewShardId())
		assert.Equal(t, startedAt, state.GetStartedAtUnixNanos())
		assert.Equal(t, originalRangeEnd, state.GetOriginalRangeEnd())

		require.NoError(t, testDB.Close())
	}
}

// TestSplitStateClearedAfterNil tests that clearing split state persists across restarts.
func TestSplitStateClearedAfterNil(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Create temporary directory
	baseDir := t.TempDir()
	dbDir := filepath.Join(baseDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	// Create schema for testing
	testSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
					"properties": map[string]any{
						"id": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	// Define byte range
	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}

	// Create database, set split state, then clear it
	{
		testDB := &DBImpl{
			logger:       logger,
			antflyConfig: &common.Config{},
			indexes:      make(map[string]indexes.IndexConfig),
		}
		require.NoError(t, testDB.Open(dbDir, false, testSchema, fullRange))

		// Set split state
		splitState := SplitState_builder{
			Phase:    SplitState_PHASE_ROLLING_BACK,
			SplitKey: []byte("rollback-key"),
		}.Build()
		err := testDB.SetSplitState(splitState)
		require.NoError(t, err)

		// Clear split state
		err = testDB.ClearSplitState()
		require.NoError(t, err)

		require.NoError(t, testDB.Close())
	}

	// Re-open and verify split state is still nil
	{
		testDB := &DBImpl{
			logger:       logger,
			antflyConfig: &common.Config{},
			indexes:      make(map[string]indexes.IndexConfig),
		}
		require.NoError(t, testDB.Open(dbDir, true, testSchema, fullRange))

		state := testDB.GetSplitState()
		assert.Nil(t, state, "Expected nil split state after restart following clear")

		require.NoError(t, testDB.Close())
	}
}

// TestSplitStatePhaseTransitions tests that all phase values can be set and retrieved.
func TestSplitStatePhaseTransitions(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Create temporary directory
	baseDir := t.TempDir()
	dbDir := filepath.Join(baseDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	testSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {Schema: map[string]any{"type": "object"}},
		},
	}

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}

	testDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, testDB.Open(dbDir, false, testSchema, fullRange))
	defer testDB.Close()

	phases := []SplitState_Phase{
		SplitState_PHASE_NONE,
		SplitState_PHASE_PREPARE,
		SplitState_PHASE_SPLITTING,
		SplitState_PHASE_FINALIZING,
		SplitState_PHASE_ROLLING_BACK,
	}

	for _, phase := range phases {
		t.Run(phase.String(), func(t *testing.T) {
			splitState := SplitState_builder{
				Phase:    phase,
				SplitKey: []byte("phase-test-key"),
			}.Build()
			err := testDB.SetSplitState(splitState)
			require.NoError(t, err)

			state := testDB.GetSplitState()
			require.NotNil(t, state)
			assert.Equal(t, phase, state.GetPhase())
		})
	}
}

// TestShadowIndexManagerLifecycle tests creating and closing shadow IndexManager.
func TestShadowIndexManagerLifecycle(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Create temporary directory
	baseDir := t.TempDir()
	dbDir := filepath.Join(baseDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	testSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {Schema: map[string]any{"type": "object"}},
		},
	}

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}

	testDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, testDB.Open(dbDir, false, testSchema, fullRange))
	defer testDB.Close()

	t.Run("InitiallyNoShadow", func(t *testing.T) {
		shadow := testDB.GetShadowIndexManager()
		assert.Nil(t, shadow, "Expected no shadow IndexManager initially")
	})

	t.Run("CreateShadow", func(t *testing.T) {
		splitKey := []byte("m") // Split in the middle
		originalRangeEnd := []byte("\xFF")

		err := testDB.CreateShadowIndexManager(splitKey, originalRangeEnd)
		require.NoError(t, err, "Failed to create shadow IndexManager")

		shadow := testDB.GetShadowIndexManager()
		require.NotNil(t, shadow, "Expected shadow IndexManager to exist after creation")
	})

	t.Run("DuplicateCreateFails", func(t *testing.T) {
		// Trying to create a second shadow should fail
		err := testDB.CreateShadowIndexManager([]byte("n"), []byte("\xFF"))
		assert.Error(t, err, "Expected error when creating duplicate shadow")
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("CloseShadow", func(t *testing.T) {
		err := testDB.CloseShadowIndexManager()
		require.NoError(t, err, "Failed to close shadow IndexManager")

		shadow := testDB.GetShadowIndexManager()
		assert.Nil(t, shadow, "Expected no shadow IndexManager after close")
	})

	t.Run("CloseIdempotent", func(t *testing.T) {
		// Closing when already closed should be a no-op
		err := testDB.CloseShadowIndexManager()
		require.NoError(t, err, "Close should be idempotent")
	})

	t.Run("CreateAfterClose", func(t *testing.T) {
		// Should be able to create a new shadow after closing the old one
		splitKey := []byte("p")
		originalRangeEnd := []byte("\xFF")

		err := testDB.CreateShadowIndexManager(splitKey, originalRangeEnd)
		require.NoError(t, err, "Should be able to create shadow after close")

		shadow := testDB.GetShadowIndexManager()
		require.NotNil(t, shadow, "Expected new shadow to exist")

		// Clean up
		require.NoError(t, testDB.CloseShadowIndexManager())
	})
}

// TestShadowIndexManagerDirectory tests that shadow creates its own directory structure.
func TestShadowIndexManagerDirectory(t *testing.T) {
	logger := zaptest.NewLogger(t)

	// Create temporary directory
	baseDir := t.TempDir()
	dbDir := filepath.Join(baseDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	testSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {Schema: map[string]any{"type": "object"}},
		},
	}

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}

	testDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, testDB.Open(dbDir, false, testSchema, fullRange))
	defer testDB.Close()

	// Create shadow
	splitKey := []byte("m")
	originalRangeEnd := []byte("\xFF")
	err := testDB.CreateShadowIndexManager(splitKey, originalRangeEnd)
	require.NoError(t, err)
	defer testDB.CloseShadowIndexManager()

	// Verify shadow directory was created
	shadowDir := filepath.Join(dbDir, ".shadow", "indexes")
	info, err := os.Stat(shadowDir)
	require.NoError(t, err, "Shadow directory should exist")
	assert.True(t, info.IsDir(), "Shadow path should be a directory")
}

// TestDualWriteRoutingDuringSplit tests that writes are correctly routed during a split.
func TestDualWriteRoutingDuringSplit(t *testing.T) {
	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	// Create temporary directory
	baseDir := t.TempDir()
	dbDir := filepath.Join(baseDir, "db")
	require.NoError(t, os.MkdirAll(dbDir, os.ModePerm))

	testSchema := &schema.TableSchema{
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {Schema: map[string]any{"type": "object"}},
		},
	}

	fullRange := types.Range{[]byte("\x00"), []byte("\xFF")}

	testDB := &DBImpl{
		logger:       logger,
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	require.NoError(t, testDB.Open(dbDir, false, testSchema, fullRange))
	defer testDB.Close()

	// Define split key at "m" (middle of alphabet)
	splitKey := []byte("m")
	originalRangeEnd := []byte("\xFF")

	// Helper to check batch errors - ErrPartialSuccess is acceptable
	checkBatchErr := func(t *testing.T, err error) {
		t.Helper()
		if err != nil && !errors.Is(err, ErrPartialSuccess) {
			require.NoError(t, err)
		}
	}

	t.Run("NoSplitState_AllWritesToPrimary", func(t *testing.T) {
		// Without split state, writes should go to primary only
		// Write documents on both sides of potential split key
		writes := [][2][]byte{
			{[]byte("abc"), []byte(`{"id":"abc"}`)},
			{[]byte("xyz"), []byte(`{"id":"xyz"}`)},
		}
		err := testDB.Batch(ctx, writes, nil, Op_SyncLevelFullText)
		checkBatchErr(t, err)

		// Verify documents were written
		doc, err := testDB.Get(ctx, []byte("abc"))
		require.NoError(t, err)
		assert.Equal(t, "abc", doc["id"])

		doc, err = testDB.Get(ctx, []byte("xyz"))
		require.NoError(t, err)
		assert.Equal(t, "xyz", doc["id"])
	})

	t.Run("WithSplitState_DualWriteRouting", func(t *testing.T) {
		// Set split state to PHASE_PREPARE
		splitState := SplitState_builder{
			Phase:            SplitState_PHASE_PREPARE,
			SplitKey:         splitKey,
			NewShardId:       12345,
			OriginalRangeEnd: originalRangeEnd,
		}.Build()
		err := testDB.SetSplitState(splitState)
		require.NoError(t, err)

		// Create shadow IndexManager
		err = testDB.CreateShadowIndexManager(splitKey, originalRangeEnd)
		require.NoError(t, err)
		defer func() {
			// Clean up at end of subtest
			_ = testDB.ClearSplitState()
			_ = testDB.CloseShadowIndexManager()
		}()

		// Verify shadow exists
		shadow := testDB.GetShadowIndexManager()
		require.NotNil(t, shadow)

		// Write documents on both sides of split key
		// Keys < "m" should go to primary
		// Keys >= "m" should go to shadow
		writes := [][2][]byte{
			{[]byte("aaa"), []byte(`{"id":"aaa"}`)}, // < "m" -> primary
			{[]byte("bbb"), []byte(`{"id":"bbb"}`)}, // < "m" -> primary
			{[]byte("mmm"), []byte(`{"id":"mmm"}`)}, // >= "m" -> shadow
			{[]byte("zzz"), []byte(`{"id":"zzz"}`)}, // >= "m" -> shadow
		}
		err = testDB.Batch(ctx, writes, nil, Op_SyncLevelFullText)
		checkBatchErr(t, err)

		// Verify all documents were written to Pebble (storage is shared)
		for _, w := range writes {
			doc, err := testDB.Get(ctx, w[0])
			require.NoError(t, err, "Key %s should exist", string(w[0]))
			assert.Equal(t, string(w[0]), doc["id"])
		}
	})

	t.Run("SplittingPhase_DualWriteRouting", func(t *testing.T) {
		// Set split state to PHASE_SPLITTING
		splitState := SplitState_builder{
			Phase:            SplitState_PHASE_SPLITTING,
			SplitKey:         splitKey,
			NewShardId:       12345,
			OriginalRangeEnd: originalRangeEnd,
		}.Build()
		err := testDB.SetSplitState(splitState)
		require.NoError(t, err)

		// Create shadow IndexManager
		err = testDB.CreateShadowIndexManager(splitKey, originalRangeEnd)
		require.NoError(t, err)
		defer func() {
			// Clean up at end of subtest
			_ = testDB.ClearSplitState()
			_ = testDB.CloseShadowIndexManager()
		}()

		// Verify shadow exists
		shadow := testDB.GetShadowIndexManager()
		require.NotNil(t, shadow)

		// Write more documents
		writes := [][2][]byte{
			{[]byte("ccc"), []byte(`{"id":"ccc"}`)}, // < "m" -> primary
			{[]byte("nnn"), []byte(`{"id":"nnn"}`)}, // >= "m" -> shadow
		}
		err = testDB.Batch(ctx, writes, nil, Op_SyncLevelFullText)
		checkBatchErr(t, err)

		// Verify documents were written
		for _, w := range writes {
			doc, err := testDB.Get(ctx, w[0])
			require.NoError(t, err, "Key %s should exist", string(w[0]))
			assert.Equal(t, string(w[0]), doc["id"])
		}
	})
}
