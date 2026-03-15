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
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestBleveIndex_PauseResume(t *testing.T) {
	logger := zaptest.NewLogger(t).Sugar().Desugar()
	tempDir := t.TempDir()

	db, err := pebble.Open(filepath.Join(tempDir, "test.db"), pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer db.Close()

	// Create BleveIndexV2
	idx, err := NewBleveIndexV2(logger, nil, db, tempDir, "pause_test",
		NewFullTextIndexConfig("", true), // memory-only for testing
		nil,
	)
	require.NoError(t, err)

	bi := idx.(*BleveIndexV2)
	err = bi.Open(true, nil, types.Range{[]byte(""), []byte("\xff")})
	require.NoError(t, err)
	defer bi.Close()

	t.Run("basic pause and resume", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Pause the index
		err := bi.Pause(ctx)
		assert.NoError(t, err)
		assert.True(t, bi.paused.Load(), "Index should be paused")

		// Resume the index
		bi.Resume()
		assert.False(t, bi.paused.Load(), "Index should be resumed")
	})

	t.Run("double pause is idempotent", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// First pause
		err := bi.Pause(ctx)
		assert.NoError(t, err)

		// Second pause should succeed without error
		err = bi.Pause(ctx)
		assert.NoError(t, err)

		bi.Resume()
	})

	t.Run("pause with timeout", func(t *testing.T) {
		// Create context with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := bi.Pause(ctx)
		assert.NoError(t, err)

		bi.Resume()
	})

	t.Run("resume when not paused is safe", func(t *testing.T) {
		// Ensure we're not paused
		bi.Resume()
		assert.False(t, bi.paused.Load())

		// Resume again - should be no-op
		bi.Resume()
		assert.False(t, bi.paused.Load())
	})
}

func TestGraphIndex_PauseResume(t *testing.T) {
	dir := t.TempDir()
	lg := zaptest.NewLogger(t)

	// Open pebble database
	pdb, err := pebble.Open(dir, pebbleutils.NewMemPebbleOpts())
	require.NoError(t, err)
	defer pdb.Close()

	// Create graph index config
	edgeTypes := []EdgeTypeConfig{
		{
			Name:             "test_edge",
			MaxWeight:        1.0,
			MinWeight:        0.0,
			AllowSelfLoops:   false,
			RequiredMetadata: nil,
		},
	}
	config := GraphIndexConfig{
		EdgeTypes:           &edgeTypes,
		MaxEdgesPerDocument: 100,
	}

	indexConfig, err := NewIndexConfig("test_graph", config)
	require.NoError(t, err)

	// Create graph index
	index, err := NewGraphIndexV0(lg, &common.Config{}, pdb, dir, "test_graph", indexConfig, nil)
	require.NoError(t, err)

	graphIndex, ok := index.(*GraphIndexV0)
	require.True(t, ok, "Index should be *GraphIndexV0")
	defer graphIndex.Close()

	ctx := context.Background()

	t.Run("basic pause and resume", func(t *testing.T) {
		// Pause the index
		err := graphIndex.Pause(ctx)
		assert.NoError(t, err)
		assert.True(t, graphIndex.paused.Load(), "Index should be paused")

		// Resume the index
		graphIndex.Resume()
		assert.False(t, graphIndex.paused.Load(), "Index should be resumed")
	})

	t.Run("double pause is safe", func(t *testing.T) {
		// First pause
		err := graphIndex.Pause(ctx)
		assert.NoError(t, err)

		// Second pause should succeed
		err = graphIndex.Pause(ctx)
		assert.NoError(t, err)

		graphIndex.Resume()
	})

	t.Run("resume when not paused is safe", func(t *testing.T) {
		// Ensure we're not paused
		graphIndex.Resume()
		assert.False(t, graphIndex.paused.Load())

		// Resume again - should be no-op
		graphIndex.Resume()
		assert.False(t, graphIndex.paused.Load())
	})
}
