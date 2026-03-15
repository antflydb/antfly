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

package inflight

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestWALBuffer_Sync verifies that Sync() flushes data to disk
func TestWALBuffer_Sync(t *testing.T) {
	testDir := "./walbuffertest_sync"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	require.NotNil(t, wb)
	defer wb.Close()

	// Enqueue some data
	data1 := []byte("test-data-1")
	data2 := []byte("test-data-2")
	data3 := []byte("test-data-3")

	err = wb.Enqueue(data1, 1)
	require.NoError(t, err)
	err = wb.Enqueue(data2, 1)
	require.NoError(t, err)
	err = wb.Enqueue(data3, 1)
	require.NoError(t, err)

	// Sync to ensure data is flushed to disk
	err = wb.Sync()
	require.NoError(t, err)

	// Close and reopen to verify persistence
	err = wb.Close()
	require.NoError(t, err)

	// Reopen WAL
	wb2, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	require.NotNil(t, wb2)
	defer wb2.Close()

	// Verify data is still there after reopen
	merger := &mockMerger{}
	err = wb2.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 3)
	require.Equal(t, data1, merger.mergedData[0])
	require.Equal(t, data2, merger.mergedData[1])
	require.Equal(t, data3, merger.mergedData[2])
}

// TestWALBuffer_SyncOnClosed verifies Sync() behavior on closed buffer
func TestWALBuffer_SyncOnClosed(t *testing.T) {
	testDir := "./walbuffertest_sync_closed"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	require.NotNil(t, wb)

	// Enqueue some data
	err = wb.Enqueue([]byte("data"), 1)
	require.NoError(t, err)

	// Close the buffer
	err = wb.Close()
	require.NoError(t, err)

	// Sync on closed buffer should fail or be safe
	// (depending on implementation, check if it returns an error)
	_ = wb.Sync()
	// We don't require an error here because the underlying WAL might handle it gracefully
	// but we want to ensure it doesn't panic
}

// TestWALBuffer_SyncEmpty verifies Sync() on empty buffer
func TestWALBuffer_SyncEmpty(t *testing.T) {
	testDir := "./walbuffertest_sync_empty"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	require.NotNil(t, wb)
	defer wb.Close()

	// Sync empty buffer should succeed
	err = wb.Sync()
	require.NoError(t, err)
}
