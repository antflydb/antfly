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
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/wal"
)

// mockMetrics implements WALBufferMetrics for testing
type mockMetrics struct {
	dequeueAttempts atomic.Int64
	dequeueFailures map[string]*atomic.Int64
	itemsDiscarded  atomic.Int64
	pendingRetries  atomic.Int64
}

func newMockMetrics() *mockMetrics {
	return &mockMetrics{
		dequeueFailures: make(map[string]*atomic.Int64),
	}
}

func (m *mockMetrics) IncDequeueAttempts(count int) {
	m.dequeueAttempts.Add(int64(count))
}

func (m *mockMetrics) IncDequeueFailure(errorType string) {
	if _, ok := m.dequeueFailures[errorType]; !ok {
		m.dequeueFailures[errorType] = &atomic.Int64{}
	}
	m.dequeueFailures[errorType].Add(1)
}

func (m *mockMetrics) IncItemsDiscarded(count int) {
	m.itemsDiscarded.Add(int64(count))
}

func (m *mockMetrics) SetPendingRetries(count int) {
	m.pendingRetries.Store(int64(count))
}

func TestWAL(t *testing.T) {
	// write some entries
	//
	os.RemoveAll("./waltest")
	log, err := wal.Open("./waltest", &wal.Options{NoSync: true})
	defer os.RemoveAll("./waltest")
	require.NoError(t, err)
	for i := uint64(1); i <= 1000; i++ {
		err := log.Write(i, []byte("entry"))
		require.NoError(t, err)
	}

	// truncate the log from index starting 350 and ending with 950.
	err = log.TruncateFront(1000)
	require.NoError(t, err)
	err = log.TruncateBack(1000)
	require.NoError(t, err)

	// fi, err := log.FirstIndex()
	// require.NoError(t, err)
	// li, err := log.LastIndex()
	// require.NoError(t, err)
	// fmt.Println("FirstIndex:", fi, "LastIndex:", li)
	// files, err := os.ReadDir("./waltest")
	// require.NoError(t, err)
	// for _, file := range files {
	// 	fmt.Println(file.Name())
	// }
}

type mockMerger struct {
	mergedData [][]byte
	executeErr error
	mergeErr   error
}

func (m *mockMerger) Merge(datum []byte) error {
	if m.mergeErr != nil {
		return m.mergeErr
	}
	m.mergedData = append(m.mergedData, datum)
	return nil
}

func (m *mockMerger) Execute(ctx context.Context) error {
	return m.executeErr
}

func (m *mockMerger) Reset() {
	m.mergedData = nil
	m.executeErr = nil
	m.mergeErr = nil
}

func TestWALBuffer_EnqueueDequeue(t *testing.T) {
	testDir := "./walbuffertest_enqdeq"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	require.NotNil(t, wb)

	merger := &mockMerger{}

	// Test: Dequeue from empty buffer
	err = wb.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Empty(t, merger.mergedData)

	// Test: Enqueue some items
	data1 := []byte("entry1")
	data2 := []byte("entry2")
	data3 := []byte("entry3")

	err = wb.Enqueue(data1, 1)
	require.NoError(t, err)
	err = wb.Enqueue(data2, 1)
	require.NoError(t, err)
	err = wb.Enqueue(data3, 1)
	require.NoError(t, err)

	// Test: Dequeue items (less than available)
	merger.Reset()
	err = wb.Dequeue(t.Context(), merger, 2)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 2)
	require.Equal(t, data1, merger.mergedData[0])
	require.Equal(t, data2, merger.mergedData[1])

	// Check WAL first index after partial dequeue
	firstIdx, err := wb.log.FirstIndex()
	require.NoError(t, err)
	// wal.Log writes items starting at index 1.
	// After enqueuing 3 items (indices 1, 2, 3) and dequeuing 2 (indices 1, 2),
	// the next item to read should be at index 3.
	// TruncateFront(i) keeps items from index i. So TruncateFront(3)
	require.Equal(t, uint64(3), firstIdx, "WAL should be truncated up to the last dequeued item + 1")

	// Test: Dequeue remaining item
	merger.Reset()
	err = wb.Dequeue(t.Context(), merger, 10) // Max > remaining
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 1)
	require.Equal(t, data3, merger.mergedData[0])

	// Check WAL indices after full dequeue (should be empty or have placeholder)
	firstIdx, err = wb.log.FirstIndex()
	require.NoError(t, err)
	lastIdx, err := wb.log.LastIndex()
	require.NoError(t, err)
	// After all data items are dequeued, a placeholder might be written.
	// TruncateFront should clean this up.
	// The exact state depends on the WAL implementation details after truncation of all user data.
	// Often, FirstIndex might become LastIndex + 1 or similar if a placeholder is used and then truncated.
	// If all actual data is gone, and the placeholder is also truncated, firstIndex can be > lastIndex.
	// For tidwall/wal, if all entries are truncated, FirstIndex usually becomes the index *after* the last truncated entry.
	// And LastIndex will be that same value if a placeholder was written and then immediately targeted by TruncateFront.
	// So, firstIdx should be greater than or equal to lastIdx if a placeholder was involved and then truncated.
	// Or simply, the log appears "empty" in terms of user data.
	// We expect the next item to be written to be index 4 (if it was 1,2,3).
	// FirstIndex will be 4. LastIndex will be 4 (due to placeholder).
	// Then TruncateFront(4) is called.
	require.Positive(t, firstIdx, "FirstIndex should be non-zero")
	require.Positive(t, lastIdx, "LastIndex should be non-zero")
	// Check that no user data can be read
	merger.Reset()
	err = wb.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Empty(t, merger.mergedData, "No data should be dequeued after all items are processed")

	// Test: Operations on closed buffer
	err = wb.Close()
	require.NoError(t, err)

	err = wb.Enqueue([]byte("wont-work"), 1)
	require.ErrorIs(t, err, ErrBufferClosed)

	err = wb.Dequeue(t.Context(), merger, 1)
	require.ErrorIs(t, err, ErrBufferClosed)

	// Test: NewWALBuffer with existing directory
	wb2, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	require.NotNil(t, wb2)
	defer wb2.Close()

	// Check if it loads previous state (should be empty as per above)
	merger.Reset()
	err = wb2.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Empty(t, merger.mergedData, "New WALBuffer on existing dir should be empty if previously cleared")

	// Test Merge error
	merger.Reset()
	err = wb2.Enqueue([]byte("data4"), 1)
	require.NoError(t, err)
	merger.mergeErr = errors.New("merge failed")
	err = wb2.Dequeue(t.Context(), merger, 1)
	require.Error(t, err) // Dequeue itself won't return the mergeErr directly, but it stops processing.
	require.Contains(t, err.Error(), "merge failed", "Error should reflect a problem during merging, but the function doesn't propagate merger.Merge err")
	// The log item should NOT be truncated if merge fails
	merger.Reset() // Clear mergeErr and data

	// Test Execute error
	merger.Reset()
	err = wb2.Enqueue([]byte("data5"), 1)
	require.NoError(t, err)
	merger.executeErr = errors.New("execute failed")
	err = wb2.Dequeue(t.Context(), merger, 1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "execute failed")
	// The log item should NOT be truncated if execute fails
	merger.Reset() // Clear executeErr and data
	err = wb2.Dequeue(t.Context(), merger, 1)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 1)
	require.Equal(t, []byte("data4"), merger.mergedData[0])
	merger.Reset()
	err = wb2.Dequeue(t.Context(), merger, 1)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 1)
	require.Equal(t, []byte("data5"), merger.mergedData[0])

}

func TestWALBuffer_MaxRetries(t *testing.T) {
	testDir := "./walbuffertest_maxretries"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	metrics := newMockMetrics()
	var discardedItems [][]byte
	var discardedAttempts []int

	wb, err := NewWALBufferWithOptions(nil, testDir, "testlog", WALBufferOptions{
		MaxRetries: 3,
		Metrics:    metrics,
		OnDiscard: func(index uint64, data []byte, attempts int) {
			discardedItems = append(discardedItems, data)
			discardedAttempts = append(discardedAttempts, attempts)
		},
	})
	require.NoError(t, err)
	require.NotNil(t, wb)
	defer wb.Close()

	// Enqueue an item
	err = wb.Enqueue([]byte("retry-item"), 1)
	require.NoError(t, err)

	// Create a merger that always fails execute
	merger := &mockMerger{
		executeErr: errors.New("execute failed"),
	}

	// First 3 attempts should fail but keep the item
	for i := range 3 {
		merger.Reset()
		merger.executeErr = errors.New("execute failed")
		err = wb.Dequeue(t.Context(), merger, 10)
		require.Error(t, err)
		require.Contains(t, err.Error(), "execute failed")
		require.Len(t, merger.mergedData, 1, "attempt %d: item should still be merged", i+1)
	}

	// Verify failure metrics were recorded
	require.Equal(t, int64(3), metrics.dequeueFailures["other"].Load())

	// 4th dequeue should discard the item (since attempts >= maxRetries)
	merger.Reset()
	merger.executeErr = nil // Execute would succeed, but item should be discarded before merge
	err = wb.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Empty(t, merger.mergedData, "item should have been discarded, not merged")

	// Verify discard callback was called
	require.Len(t, discardedItems, 1)
	require.Equal(t, []byte("retry-item"), discardedItems[0])
	require.Equal(t, 3, discardedAttempts[0])

	// Verify metrics
	require.Equal(t, int64(1), metrics.itemsDiscarded.Load())
}

func TestWALBuffer_MaxRetriesZeroMeansInfinite(t *testing.T) {
	testDir := "./walbuffertest_infinite"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	metrics := newMockMetrics()

	wb, err := NewWALBufferWithOptions(nil, testDir, "testlog", WALBufferOptions{
		MaxRetries: 0, // Infinite retries
		Metrics:    metrics,
	})
	require.NoError(t, err)
	require.NotNil(t, wb)
	defer wb.Close()

	// Enqueue an item
	err = wb.Enqueue([]byte("infinite-item"), 1)
	require.NoError(t, err)

	merger := &mockMerger{
		executeErr: errors.New("execute failed"),
	}

	// Should keep retrying (test 10 attempts)
	for i := range 10 {
		merger.Reset()
		merger.executeErr = errors.New("execute failed")
		err = wb.Dequeue(t.Context(), merger, 10)
		require.Error(t, err)
		require.Len(t, merger.mergedData, 1, "attempt %d: item should still be merged", i+1)
	}

	// No items should be discarded
	require.Equal(t, int64(0), metrics.itemsDiscarded.Load())
}

func TestWALBuffer_MetricsOnSuccess(t *testing.T) {
	testDir := "./walbuffertest_metrics"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	metrics := newMockMetrics()

	wb, err := NewWALBufferWithOptions(nil, testDir, "testlog", WALBufferOptions{
		MaxRetries: 5,
		Metrics:    metrics,
	})
	require.NoError(t, err)
	require.NotNil(t, wb)
	defer wb.Close()

	// Enqueue some items
	err = wb.Enqueue([]byte("item1"), 1)
	require.NoError(t, err)
	err = wb.Enqueue([]byte("item2"), 1)
	require.NoError(t, err)
	err = wb.Enqueue([]byte("item3"), 1)
	require.NoError(t, err)

	merger := &mockMerger{}

	// Dequeue all items successfully
	err = wb.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 3)

	// Verify metrics
	require.Equal(t, int64(3), metrics.dequeueAttempts.Load())
	require.Equal(t, int64(0), metrics.itemsDiscarded.Load())
	require.Equal(t, int64(0), metrics.pendingRetries.Load())
}

func TestWALBuffer_MixedDiscardAndProcess(t *testing.T) {
	testDir := "./walbuffertest_mixed"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	metrics := newMockMetrics()
	var discardedItems [][]byte

	wb, err := NewWALBufferWithOptions(nil, testDir, "testlog", WALBufferOptions{
		MaxRetries: 2,
		Metrics:    metrics,
		OnDiscard: func(index uint64, data []byte, attempts int) {
			discardedItems = append(discardedItems, data)
		},
	})
	require.NoError(t, err)
	require.NotNil(t, wb)
	defer wb.Close()

	// Enqueue first item
	err = wb.Enqueue([]byte("poison-item"), 1)
	require.NoError(t, err)

	merger := &mockMerger{
		executeErr: errors.New("execute failed"),
	}

	// Fail twice to hit max retries
	for range 2 {
		merger.Reset()
		merger.executeErr = errors.New("execute failed")
		err = wb.Dequeue(t.Context(), merger, 10)
		require.Error(t, err)
	}

	// Enqueue second item (after first has accumulated retries)
	err = wb.Enqueue([]byte("good-item"), 1)
	require.NoError(t, err)

	// Next dequeue should discard first item and process second
	merger.Reset()
	merger.executeErr = nil
	err = wb.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 1)
	require.Equal(t, []byte("good-item"), merger.mergedData[0])

	// First item should have been discarded
	require.Len(t, discardedItems, 1)
	require.Equal(t, []byte("poison-item"), discardedItems[0])
}

func TestWALBuffer_ItemCount(t *testing.T) {
	testDir := "./walbuffertest_itemcount"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	defer wb.Close()

	// Empty buffer has zero items
	require.Equal(t, 0, wb.Len())

	// Enqueue single-item entries
	require.NoError(t, wb.Enqueue([]byte("a"), 1))
	require.Equal(t, 1, wb.Len())
	require.NoError(t, wb.Enqueue([]byte("b"), 1))
	require.Equal(t, 2, wb.Len())

	// Enqueue a multi-item entry
	require.NoError(t, wb.Enqueue([]byte("batch-of-5"), 5))
	require.Equal(t, 7, wb.Len())

	// Dequeue all — Len should go to zero
	merger := &mockMerger{}
	err = wb.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 3)
	require.Equal(t, 0, wb.Len())
}

func TestWALBuffer_ItemCountBatch(t *testing.T) {
	testDir := "./walbuffertest_itemcount_batch"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	defer wb.Close()

	// EnqueueBatch with varying counts
	data := [][]byte{[]byte("p1"), []byte("p2"), []byte("p3")}
	counts := []uint32{10, 20, 30}
	require.NoError(t, wb.EnqueueBatch(data, counts))
	require.Equal(t, 60, wb.Len())

	// Partial dequeue (2 of 3 entries)
	merger := &mockMerger{}
	err = wb.Dequeue(t.Context(), merger, 2)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 2)
	require.Equal(t, 30, wb.Len()) // only the last entry (count=30) remains

	// Dequeue remaining
	merger.Reset()
	err = wb.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 1)
	require.Equal(t, 0, wb.Len())
}

func TestWALBuffer_ItemCountRecovery(t *testing.T) {
	testDir := "./walbuffertest_itemcount_recovery"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	// Create buffer, enqueue items, close
	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)

	require.NoError(t, wb.Enqueue([]byte("x"), 3))
	require.NoError(t, wb.Enqueue([]byte("y"), 7))
	require.NoError(t, wb.EnqueueBatch(
		[][]byte{[]byte("a"), []byte("b")},
		[]uint32{100, 200},
	))
	require.Equal(t, 310, wb.Len())

	require.NoError(t, wb.Close())

	// Reopen — item count should be recovered from headers
	wb2, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	defer wb2.Close()

	require.Equal(t, 310, wb2.Len())

	// Dequeue one entry, close, reopen — count should reflect the dequeue
	merger := &mockMerger{}
	err = wb2.Dequeue(t.Context(), merger, 1)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 1)
	require.Equal(t, 307, wb2.Len())

	require.NoError(t, wb2.Close())

	wb3, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	defer wb3.Close()

	require.Equal(t, 307, wb3.Len())
}

func TestWALBuffer_ItemCountWithExecuteFailure(t *testing.T) {
	testDir := "./walbuffertest_itemcount_fail"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	defer wb.Close()

	require.NoError(t, wb.Enqueue([]byte("a"), 10))
	require.NoError(t, wb.Enqueue([]byte("b"), 20))
	require.Equal(t, 30, wb.Len())

	// Execute failure should NOT deduct items (they stay in WAL)
	merger := &mockMerger{executeErr: errors.New("boom")}
	err = wb.Dequeue(t.Context(), merger, 10)
	require.Error(t, err)
	require.Equal(t, 30, wb.Len())

	// Successful dequeue should deduct
	merger.Reset()
	err = wb.Dequeue(t.Context(), merger, 10)
	require.NoError(t, err)
	require.Len(t, merger.mergedData, 2)
	require.Equal(t, 0, wb.Len())
}

func TestWALBuffer_EntryCount(t *testing.T) {
	testDir := "./walbuffertest_entrycount"
	os.RemoveAll(testDir)
	defer os.RemoveAll(testDir)

	wb, err := NewWALBuffer(nil, testDir, "testlog")
	require.NoError(t, err)
	defer wb.Close()

	ec, err := wb.EntryCount()
	require.NoError(t, err)
	require.Equal(t, 0, ec)

	// Enqueue 3 entries with different item counts
	require.NoError(t, wb.Enqueue([]byte("a"), 1))
	require.NoError(t, wb.Enqueue([]byte("b"), 50))
	require.NoError(t, wb.Enqueue([]byte("c"), 100))

	ec, err = wb.EntryCount()
	require.NoError(t, err)
	require.Equal(t, 3, ec)
	require.Equal(t, 151, wb.Len()) // 1+50+100

	// Dequeue 2 entries
	merger := &mockMerger{}
	err = wb.Dequeue(t.Context(), merger, 2)
	require.NoError(t, err)

	ec, err = wb.EntryCount()
	require.NoError(t, err)
	require.Equal(t, 1, ec)
	require.Equal(t, 100, wb.Len())
}
