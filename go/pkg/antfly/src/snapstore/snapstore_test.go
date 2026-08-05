// Copyright 2026 Antfly, Inc.
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

package snapstore

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSnapshotPublishesAtomically(t *testing.T) {
	store, err := NewLocalSnapStore(t.TempDir(), types.ID(1), types.ID(1))
	require.NoError(t, err)
	require.NoError(t, store.Put(context.Background(), "snapshot", strings.NewReader("old")))

	_, err = store.CreateSnapshot(
		context.Background(),
		"snapshot",
		filepath.Join(t.TempDir(), "missing"),
		nil,
	)
	require.Error(t, err)

	sourceDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(sourceDir, "data"), []byte("new"), 0o600))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(
		t,
		store.Put(canceled, "snapshot", strings.NewReader("new")),
		context.Canceled,
	)
	_, err = store.CreateSnapshot(canceled, "snapshot", sourceDir, nil)
	require.ErrorIs(t, err, context.Canceled)

	extractDir := t.TempDir()
	marker := filepath.Join(extractDir, "marker")
	require.NoError(t, os.WriteFile(marker, []byte("keep"), 0o600))
	_, err = store.ExtractSnapshot(canceled, "snapshot", extractDir, true)
	require.ErrorIs(t, err, context.Canceled)
	_, err = os.Stat(marker)
	require.NoError(t, err)

	reader, err := store.Get(context.Background(), "snapshot")
	require.NoError(t, err)
	defer func() { _ = reader.Close() }()
	content, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "old", string(content))

	entries, err := os.ReadDir(store.snapDir)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.NotContains(t, entry.Name(), ".tmp-")
	}
}
