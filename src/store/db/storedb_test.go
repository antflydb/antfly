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
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/vectorindex"
	"github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/snapstore"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestIsInSplitOffRange(t *testing.T) {
	tests := []struct {
		name     string
		key      []byte
		splitKey []byte
		want     bool
	}{
		{"no pending split", []byte("abc"), nil, false},
		{"empty split key", []byte("abc"), []byte{}, false},
		{"key before split", []byte("abc"), []byte("def"), false},
		{"key at split", []byte("def"), []byte("def"), true},
		{"key after split", []byte("ghi"), []byte("def"), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isInSplitOffRange(tt.key, tt.splitKey)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestValidateBatchKeys(t *testing.T) {
	byteRange := types.Range{[]byte("a"), []byte("z")}
	splitState := SplitState_builder{
		Phase:            SplitState_PHASE_PREPARE,
		SplitKey:         []byte("m"),
		OriginalRangeEnd: []byte("z"),
	}.Build()

	t.Run("empty batch passes", func(t *testing.T) {
		batch := BatchOp_builder{}.Build()
		assert.NoError(t, ValidateBatchKeys(batch, byteRange, nil))
	})

	t.Run("valid writes pass", func(t *testing.T) {
		batch := BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("foo"), Value: []byte("v")}.Build(),
				Write_builder{Key: []byte("bar"), Value: []byte("v")}.Build(),
			},
		}.Build()
		assert.NoError(t, ValidateBatchKeys(batch, byteRange, nil))
	})

	t.Run("write out of range fails", func(t *testing.T) {
		batch := BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("zzz"), Value: []byte("v")}.Build(),
			},
		}.Build()
		err := ValidateBatchKeys(batch, byteRange, nil)
		require.Error(t, err)
		var keyErr common.ErrKeyOutOfRange
		assert.ErrorAs(t, err, &keyErr)
	})

	t.Run("write in split-off range fails", func(t *testing.T) {
		batch := BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("m"), Value: []byte("v")}.Build(),
			},
		}.Build()
		assert.NoError(t, ValidateBatchKeys(batch, byteRange, splitState))
	})

	t.Run("write before split key passes", func(t *testing.T) {
		batch := BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("d"), Value: []byte("v")}.Build(),
			},
		}.Build()
		assert.NoError(t, ValidateBatchKeys(batch, byteRange, splitState))
	})

	t.Run("delete out of range fails", func(t *testing.T) {
		batch := BatchOp_builder{
			Deletes: [][]byte{[]byte("zzz")},
		}.Build()
		err := ValidateBatchKeys(batch, byteRange, nil)
		require.Error(t, err)
	})

	t.Run("delete in split-off range fails", func(t *testing.T) {
		batch := BatchOp_builder{
			Deletes: [][]byte{[]byte("n")},
		}.Build()
		assert.NoError(t, ValidateBatchKeys(batch, byteRange, splitState))
	})

	t.Run("transform out of range fails", func(t *testing.T) {
		batch := BatchOp_builder{
			Transforms: []*Transform{
				Transform_builder{Key: []byte("zzz")}.Build(),
			},
		}.Build()
		err := ValidateBatchKeys(batch, byteRange, nil)
		require.Error(t, err)
	})

	t.Run("transform in split-off range fails", func(t *testing.T) {
		batch := BatchOp_builder{
			Transforms: []*Transform{
				Transform_builder{Key: []byte("m")}.Build(),
			},
		}.Build()
		assert.NoError(t, ValidateBatchKeys(batch, byteRange, splitState))
	})

	t.Run("mixed valid operations pass", func(t *testing.T) {
		batch := BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("bar"), Value: []byte("v")}.Build(),
			},
			Deletes: [][]byte{[]byte("baz")},
			Transforms: []*Transform{
				Transform_builder{Key: []byte("foo")}.Build(),
			},
		}.Build()
		assert.NoError(t, ValidateBatchKeys(batch, byteRange, splitState))
	})

	t.Run("first invalid key short-circuits", func(t *testing.T) {
		batch := BatchOp_builder{
			Writes: []*Write{
				Write_builder{Key: []byte("bar"), Value: []byte("v")}.Build(),
				Write_builder{Key: []byte("zzz"), Value: []byte("v")}.Build(), // out of range
			},
			Deletes: [][]byte{[]byte("baz")},
		}.Build()
		err := ValidateBatchKeys(batch, byteRange, nil)
		require.Error(t, err)
		var keyErr common.ErrKeyOutOfRange
		require.ErrorAs(t, err, &keyErr)
		assert.Equal(t, []byte("zzz"), keyErr.Key)
	})
}

func TestDBWrapperSnapshot(t *testing.T) {
	dir := t.TempDir()
	t.Cleanup(func() { os.RemoveAll("./antflydb") })
	// Load test data
	val := map[string]any{
		"bar":   "bazz",
		"other": 1,
	}
	b, err := json.Marshal(val)
	require.NoError(t, err)

	lg := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(common.RootAntflyDir, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	coreDB := NewDBImplForTest(lg, snapStore)
	s := &StoreDB{
		logger:         lg,
		dbDir:          dir,
		snapStore:      snapStore,
		coreDB:         coreDB,
		loadSnapshotID: func(_ context.Context) (string, error) { return "foo", nil },
	}
	fs := []float32{1.0}
	hashID := uint64(12345) // Test hash ID
	vecBuf := make([]byte, 0)
	vecBuf, _ = vectorindex.EncodeEmbeddingWithHashID(vecBuf, fs, hashID)
	require.NoError(t, s.coreDB.Open(dir, false, nil, types.Range{nil, []byte{0xFF}}))
	err = s.coreDB.Batch(
		t.Context(),
		[][2][]byte{{[]byte("foo"), b}, {[]byte("foo:i:bar:e"), vecBuf}},
		nil,
		Op_SyncLevelFullText,
	)
	// FullText sync level returns ErrPartialSuccess indicating KV write succeeded
	if err != nil && !errors.Is(err, ErrPartialSuccess) {
		require.NoError(t, err)
	}

	doc, err := s.coreDB.Get(t.Context(), []byte("foo"))
	require.NotEmpty(t, doc)
	require.NotEmpty(t, doc["_embeddings"])
	require.NoError(t, err)

	v, err := s.Lookup(t.Context(), "foo")
	require.NoError(t, err)
	require.NotNil(t, v)
	require.NotEmpty(t, v["_embeddings"])
	assert.Equal(t, "bazz", v["bar"])
	assert.EqualValues(t, 1, v["other"])

	require.NoError(t, s.CreateDBSnapshot("foo"))
	t.Cleanup(func() { os.RemoveAll(filepath.Dir("./tmpsnapshot/")) })

	// Remove the directory - loadPersistentSnapshot will handle closing the DB
	os.RemoveAll(dir)

	require.NoError(t, s.loadPersistentSnapshot(t.Context()))
	pdb, err := pebble.Open(dir+"/pebble", pebbleutils.NewPebbleOpts())
	require.NoError(t, err)
	coreDB.SetPebbleDB(pdb)
	v, err = s.Lookup(t.Context(), "foo")
	require.NoError(t, err)
	require.NotNil(t, v)
	require.NotEmpty(t, v["_embeddings"])
	assert.Equal(t, "bazz", v["bar"])
	assert.EqualValues(t, 1, v["other"])
}

func newTestStoreDBForSplitReplay(t *testing.T, root string, shardID, nodeID types.ID, byteRange types.Range) *StoreDB {
	t.Helper()

	lg := zaptest.NewLogger(t)
	snapStore, err := snapstore.NewLocalSnapStore(root, shardID, nodeID)
	require.NoError(t, err)

	coreDB := NewDBImplForTest(lg, snapStore)
	dbDir := common.StorageDBDir(root, shardID, nodeID)
	require.NoError(t, coreDB.Open(dbDir, false, nil, byteRange))

	return &StoreDB{
		logger:    lg,
		dataDir:   root,
		dbDir:     dbDir,
		snapStore: snapStore,
		coreDB:    coreDB,
		byteRange: byteRange,
	}
}

func TestSplit_AlreadyAppliedIsIdempotent(t *testing.T) {
	splitKey := []byte("m")
	newShardID := uint64(42)

	storeDB := &StoreDB{
		byteRange: types.Range{[]byte("a"), splitKey},
		splitState: SplitState_builder{
			Phase:            SplitState_PHASE_SPLITTING,
			SplitKey:         splitKey,
			NewShardId:       newShardID,
			OriginalRangeEnd: []byte("z"),
		}.Build(),
	}

	err := storeDB.Split(context.Background(), newShardID, splitKey)
	require.NoError(t, err)
}

func TestStartSplitReplayIfNeeded_AppliesParentSplitDeltas(t *testing.T) {
	root := t.TempDir()
	parentID := types.ID(10)
	childID := types.ID(11)
	nodeID := types.ID(1)
	splitKey := []byte("m")

	parent := newTestStoreDBForSplitReplay(t, root, parentID, nodeID, types.Range{[]byte("a"), splitKey})
	child := newTestStoreDBForSplitReplay(t, root, childID, nodeID, types.Range{splitKey, []byte("z")})
	t.Cleanup(func() {
		if child.splitReplayCancel != nil {
			child.splitReplayCancel()
		}
		require.NoError(t, parent.coreDB.Close())
		require.NoError(t, child.coreDB.Close())
	})

	parentSplitState := SplitState_builder{
		Phase:            SplitState_PHASE_SPLITTING,
		SplitKey:         splitKey,
		NewShardId:       uint64(childID),
		OriginalRangeEnd: []byte("z"),
	}.Build()
	parent.splitState = parentSplitState
	require.NoError(t, parent.coreDB.SetSplitState(parentSplitState))

	child.restoredArchiveMetadata = &common.ArchiveMetadata{
		Split: &common.SplitMetadata{
			ParentShardID:  parentID.String(),
			ReplayFenceSeq: 0,
		},
	}
	child.localSplitSourceLookup = func(id types.ID) *StoreDB {
		if id == parentID {
			return parent
		}
		return nil
	}
	require.NoError(t, child.startSplitReplayIfNeeded())

	value, err := json.Marshal(map[string]any{"name": "mango"})
	require.NoError(t, err)
	writeCtx := storeutils.WithTimestamp(context.Background(), 123)
	require.NoError(t, parent.coreDB.Batch(writeCtx, [][2][]byte{{[]byte("mango"), value}}, nil, Op_SyncLevelWrite))

	require.Eventually(t, func() bool {
		doc, err := child.coreDB.Get(context.Background(), []byte("mango"))
		return err == nil && doc["name"] == "mango" && !child.IsInitializing()
	}, 5*time.Second, 100*time.Millisecond)

	require.Eventually(t, func() bool {
		required, caughtUp, cutoverReady, seq, replayParentID := child.SplitReplayInfo()
		return required && caughtUp && !cutoverReady && seq == 1 && replayParentID == parentID
	}, 5*time.Second, 100*time.Millisecond)
}

func TestStartSplitReplayIfNeeded_ZeroDeltaSplitMarksCutoverReadyWhenParentStops(t *testing.T) {
	root := t.TempDir()
	parentID := types.ID(12)
	childID := types.ID(13)
	nodeID := types.ID(1)
	splitKey := []byte("m")

	parent := newTestStoreDBForSplitReplay(t, root, parentID, nodeID, types.Range{[]byte("a"), splitKey})
	child := newTestStoreDBForSplitReplay(t, root, childID, nodeID, types.Range{splitKey, []byte("z")})
	t.Cleanup(func() {
		if child.splitReplayCancel != nil {
			child.splitReplayCancel()
		}
		require.NoError(t, parent.coreDB.Close())
		require.NoError(t, child.coreDB.Close())
	})

	parentSplitState := SplitState_builder{
		Phase:            SplitState_PHASE_SPLITTING,
		SplitKey:         splitKey,
		NewShardId:       uint64(childID),
		OriginalRangeEnd: []byte("z"),
	}.Build()
	parent.splitState = parentSplitState
	require.NoError(t, parent.coreDB.SetSplitState(parentSplitState))

	child.restoredArchiveMetadata = &common.ArchiveMetadata{
		Split: &common.SplitMetadata{
			ParentShardID:  parentID.String(),
			ReplayFenceSeq: 0,
		},
	}
	child.localSplitSourceLookup = func(id types.ID) *StoreDB {
		if id == parentID {
			return parent
		}
		return nil
	}
	require.NoError(t, child.startSplitReplayIfNeeded())

	require.Eventually(t, func() bool {
		required, caughtUp, cutoverReady, seq, replayParentID := child.SplitReplayInfo()
		return required && caughtUp && !cutoverReady && seq == 0 && replayParentID == parentID
	}, 5*time.Second, 100*time.Millisecond)

	parent.splitState = nil
	require.NoError(t, parent.coreDB.SetSplitState(nil))

	require.Eventually(t, func() bool {
		required, caughtUp, cutoverReady, seq, replayParentID := child.SplitReplayInfo()
		return required && caughtUp && cutoverReady && seq == 0 && replayParentID == parentID
	}, 5*time.Second, 100*time.Millisecond)
}

func TestApplyOpFinalizeSplit_WaitsForLocalChildReplayBeforeDeletingParentRange(t *testing.T) {
	root := t.TempDir()
	parentID := types.ID(20)
	childID := types.ID(21)
	nodeID := types.ID(1)
	splitKey := []byte("m")

	parent := newTestStoreDBForSplitReplay(t, root, parentID, nodeID, types.Range{[]byte("a"), splitKey})
	child := newTestStoreDBForSplitReplay(t, root, childID, nodeID, types.Range{splitKey, []byte("z")})
	t.Cleanup(func() {
		if child.splitReplayCancel != nil {
			child.splitReplayCancel()
		}
		require.NoError(t, parent.coreDB.Close())
		require.NoError(t, child.coreDB.Close())
	})

	parentSplitState := SplitState_builder{
		Phase:            SplitState_PHASE_SPLITTING,
		SplitKey:         splitKey,
		NewShardId:       uint64(childID),
		OriginalRangeEnd: []byte("z"),
	}.Build()
	parent.splitState = parentSplitState
	require.NoError(t, parent.coreDB.SetSplitState(parentSplitState))

	child.restoredArchiveMetadata = &common.ArchiveMetadata{
		Split: &common.SplitMetadata{
			ParentShardID:  parentID.String(),
			ReplayFenceSeq: 0,
		},
	}
	child.localSplitSourceLookup = func(id types.ID) *StoreDB {
		if id == parentID {
			return parent
		}
		return nil
	}
	parent.localSplitSourceLookup = func(id types.ID) *StoreDB {
		if id == childID {
			return child
		}
		return nil
	}
	require.NoError(t, child.startSplitReplayIfNeeded())

	value, err := json.Marshal(map[string]any{"name": "mango"})
	require.NoError(t, err)
	writeCtx := storeutils.WithTimestamp(context.Background(), 456)
	require.NoError(t, parent.coreDB.Batch(writeCtx, [][2][]byte{{[]byte("mango"), value}}, nil, Op_SyncLevelWrite))

	require.NoError(t, parent.applyOpFinalizeSplit(context.Background(), FinalizeSplitOp_builder{
		NewRangeEnd: splitKey,
	}.Build()))

	require.Eventually(t, func() bool {
		doc, err := child.coreDB.Get(context.Background(), []byte("mango"))
		return err == nil && doc["name"] == "mango"
	}, 5*time.Second, 100*time.Millisecond)

	_, err = parent.coreDB.Get(context.Background(), []byte("mango"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotFound)

	assert.Nil(t, parent.GetSplitState())
	splitDeltaSeq, err := parent.coreDB.GetSplitDeltaSeq()
	require.NoError(t, err)
	assert.Zero(t, splitDeltaSeq)
}

// goos: darwin
// goarch: arm64
// pkg: github.com/antflydb/antfly/src/store
// cpu: Apple M1 Pro
// BenchmarkEncodeDecode/Small/Gob-10         	   32028	     36174 ns/op	   60695 B/op	     292 allocs/op
// BenchmarkEncodeDecode/Small/JSON-10        	   69991	     16290 ns/op	   26854 B/op	      59 allocs/op
// BenchmarkEncodeDecode/Small/Protobuf-10    	  132816	      8652 ns/op	   25497 B/op	      56 allocs/op
// BenchmarkEncodeDecode/Medium/Gob-10        	   14425	     83480 ns/op	  108868 B/op	    1131 allocs/op
// BenchmarkEncodeDecode/Medium/JSON-10       	   22370	     52355 ns/op	   80232 B/op	     304 allocs/op
// BenchmarkEncodeDecode/Medium/Protobuf-10   	   22723	     52636 ns/op	  102748 B/op	     514 allocs/op
// BenchmarkEncodeDecode/Large/Gob-10         	    2366	    467591 ns/op	  686499 B/op	    8793 allocs/op
// BenchmarkEncodeDecode/Large/JSON-10        	    2172	    599767 ns/op	 2531570 B/op	    2668 allocs/op
// BenchmarkEncodeDecode/Large/Protobuf-10    	    3049	    378732 ns/op	  728171 B/op	    4577 allocs/op

func BenchmarkEncodeDecode(b *testing.B) {
	// Create test data with various sizes
	smallDeletes := [][]byte{[]byte("key3")}
	smallOp := NewBatchOp(BatchOp_builder{
		Writes: []*Write{
			Write_builder{Key: []byte("key1"), Value: []byte("value1")}.Build(),
			Write_builder{Key: []byte("key2"), Value: []byte("value2")}.Build(),
		},
		Deletes: smallDeletes,
	}.Build())

	mediumWrites := make([]*Write, 100)
	for i := range 100 {
		mediumWrites[i] = Write_builder{
			Key:   fmt.Appendf(nil, "key%d", i),
			Value: fmt.Appendf(nil, "value%d", i),
		}.Build()
	}
	mediumDeletes := make([][]byte, 50)
	for i := range 50 {
		mediumDeletes[i] = fmt.Appendf(nil, "delete%d", i)
	}
	mediumOp := NewBatchOp(BatchOp_builder{
		Writes:  mediumWrites,
		Deletes: mediumDeletes,
	}.Build())

	largeWrites := make([]*Write, 1000)
	for i := range 1000 {
		largeWrites[i] = Write_builder{
			Key:   fmt.Appendf(nil, "key%d", i),
			Value: fmt.Appendf(nil, "value%d", i),
		}.Build()
	}
	largeDeletes := make([][]byte, 500)
	for i := range 500 {
		largeDeletes[i] = fmt.Appendf(nil, "delete%d", i)
	}
	largeOp := NewBatchOp(BatchOp_builder{
		Writes:  largeWrites,
		Deletes: largeDeletes,
	}.Build())

	benchmarks := []struct {
		name string
		op   *Op
	}{
		{"Small", smallOp},
		{"Medium", mediumOp},
		{"Large", largeOp},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name+"/Protobuf", func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				encoded, err := EncodeProto(bm.op)
				if err != nil {
					b.Fatal(err)
				}

				decoded := &Op{}
				require.NoError(b, DecodeProto(encoded, decoded))
				_ = decoded
			}
		})
	}
}

func BenchmarkEncodeOnly(b *testing.B) {
	writes := make([]*Write, 100)
	for i := range 100 {
		writes[i] = Write_builder{
			Key:   fmt.Appendf(nil, "key%d", i),
			Value: fmt.Appendf(nil, "value%d", i),
		}.Build()
	}
	deletes := make([][]byte, 50)
	for i := range 50 {
		deletes[i] = fmt.Appendf(nil, "delete%d", i)
	}
	op := NewBatchOp(BatchOp_builder{
		Writes:  writes,
		Deletes: deletes,
	}.Build())

	b.Run("Protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if _, err := EncodeProto(op); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkDecodeOnly(b *testing.B) {
	writes := make([]*Write, 100)
	for i := range 100 {
		writes[i] = Write_builder{
			Key:   fmt.Appendf(nil, "key%d", i),
			Value: fmt.Appendf(nil, "value%d", i),
		}.Build()
	}
	deletes := make([][]byte, 50)
	for i := range 50 {
		deletes[i] = fmt.Appendf(nil, "delete%d", i)
	}
	op := NewBatchOp(BatchOp_builder{
		Writes:  writes,
		Deletes: deletes,
	}.Build())

	// Pre-encode the data
	protoData, err := EncodeProto(op)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("Protobuf", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := &Op{}
			require.NoError(b, DecodeProto(protoData, decoded))
			_ = decoded
		}
	})
}
