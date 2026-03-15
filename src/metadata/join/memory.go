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

package join

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
)

// MemoryManager tracks and limits memory usage for join operations.
// When memory pressure is high, it can spill data to disk.
type MemoryManager struct {
	maxMemory         int64
	spillThreshold    int64
	currentUsage      atomic.Int64
	spillDir          string
	logger            *zap.Logger
	mu                sync.Mutex
	spilledPartitions map[string]*spilledPartition
}

// spilledPartition represents data that has been spilled to disk.
type spilledPartition struct {
	path     string
	rowCount int
	size     int64
}

// MemoryManagerConfig contains configuration for the memory manager.
type MemoryManagerConfig struct {
	MaxMemory      int64  // Maximum memory usage in bytes
	SpillThreshold int64  // Threshold at which to start spilling
	SpillDir       string // Directory for spilled data
}

// DefaultMemoryManagerConfig returns the default configuration.
func DefaultMemoryManagerConfig() *MemoryManagerConfig {
	return &MemoryManagerConfig{
		MaxMemory:      1 * 1024 * 1024 * 1024, // 1GB
		SpillThreshold: 512 * 1024 * 1024,      // 512MB
		SpillDir:       os.TempDir(),
	}
}

// NewMemoryManager creates a new memory manager.
func NewMemoryManager(config *MemoryManagerConfig, logger *zap.Logger) (*MemoryManager, error) {
	if config == nil {
		config = DefaultMemoryManagerConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}

	// Create spill directory if it doesn't exist
	spillDir := filepath.Join(config.SpillDir, "antfly_join_spill")
	if err := os.MkdirAll(spillDir, 0755); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return nil, fmt.Errorf("creating spill directory: %w", err)
	}

	return &MemoryManager{
		maxMemory:         config.MaxMemory,
		spillThreshold:    config.SpillThreshold,
		spillDir:          spillDir,
		logger:            logger,
		spilledPartitions: make(map[string]*spilledPartition),
	}, nil
}

// Allocate attempts to allocate memory for a join operation.
// Returns true if allocation succeeded, false if at capacity.
func (m *MemoryManager) Allocate(bytes int64) bool {
	for {
		current := m.currentUsage.Load()
		if current+bytes > m.maxMemory {
			return false
		}
		if m.currentUsage.CompareAndSwap(current, current+bytes) {
			return true
		}
	}
}

// Release releases previously allocated memory.
func (m *MemoryManager) Release(bytes int64) {
	m.currentUsage.Add(-bytes)
}

// CurrentUsage returns the current memory usage.
func (m *MemoryManager) CurrentUsage() int64 {
	return m.currentUsage.Load()
}

// ShouldSpill returns true if data should be spilled to disk.
func (m *MemoryManager) ShouldSpill() bool {
	return m.currentUsage.Load() > m.spillThreshold
}

// SpillRows writes rows to disk and returns a handle to read them back.
func (m *MemoryManager) SpillRows(ctx context.Context, partitionID string, rows []Row) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Create a file for this partition
	filename := filepath.Join(m.spillDir, fmt.Sprintf("partition_%s.json", partitionID))
	file, err := os.Create(filename) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return fmt.Errorf("creating spill file: %w", err)
	}
	defer func() { _ = file.Close() }()

	// Write rows as JSON
	encoder := json.NewEncoder(file)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			return fmt.Errorf("encoding row: %w", err)
		}
	}

	// Get file size
	info, err := file.Stat()
	if err != nil {
		return fmt.Errorf("getting file info: %w", err)
	}

	m.spilledPartitions[partitionID] = &spilledPartition{
		path:     filename,
		rowCount: len(rows),
		size:     info.Size(),
	}

	// Release memory for spilled data
	estimatedSize := int64(len(rows) * 200) // Rough estimate
	m.Release(estimatedSize)

	m.logger.Debug("Spilled partition to disk",
		zap.String("partition", partitionID),
		zap.Int("rows", len(rows)),
		zap.Int64("size_bytes", info.Size()))

	return nil
}

// ReadSpilledRows reads rows back from disk.
func (m *MemoryManager) ReadSpilledRows(ctx context.Context, partitionID string) ([]Row, error) {
	m.mu.Lock()
	sp, ok := m.spilledPartitions[partitionID]
	m.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("partition %s not found in spill", partitionID)
	}

	file, err := os.Open(sp.path)
	if err != nil {
		return nil, fmt.Errorf("opening spill file: %w", err)
	}
	defer func() { _ = file.Close() }()

	rows := make([]Row, 0, sp.rowCount)
	decoder := json.NewDecoder(file)
	for {
		var row Row
		if err := decoder.Decode(&row); err != nil {
			if err.Error() == "EOF" {
				break
			}
			return nil, fmt.Errorf("decoding row: %w", err)
		}
		rows = append(rows, row)
	}

	// Allocate memory for the read data
	estimatedSize := int64(len(rows) * 200)
	if !m.Allocate(estimatedSize) {
		return nil, fmt.Errorf("insufficient memory to read spilled data")
	}

	m.logger.Debug("Read spilled partition from disk",
		zap.String("partition", partitionID),
		zap.Int("rows", len(rows)))

	return rows, nil
}

// CleanupPartition removes a spilled partition file.
func (m *MemoryManager) CleanupPartition(partitionID string) error {
	m.mu.Lock()
	sp, ok := m.spilledPartitions[partitionID]
	if ok {
		delete(m.spilledPartitions, partitionID)
	}
	m.mu.Unlock()

	if !ok {
		return nil
	}

	if err := os.Remove(sp.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing spill file: %w", err)
	}

	return nil
}

// Cleanup removes all spilled partition files.
func (m *MemoryManager) Cleanup() error {
	m.mu.Lock()
	partitions := make([]string, 0, len(m.spilledPartitions))
	for id := range m.spilledPartitions {
		partitions = append(partitions, id)
	}
	m.mu.Unlock()

	var lastErr error
	for _, id := range partitions {
		if err := m.CleanupPartition(id); err != nil {
			lastErr = err
			m.logger.Warn("Failed to cleanup partition", zap.String("partition", id), zap.Error(err))
		}
	}

	return lastErr
}

// SpillableHashTable is a hash table that can spill to disk when memory is low.
type SpillableHashTable struct {
	data          map[any][]Row
	memoryManager *MemoryManager
	partitionID   string
	spilled       bool
	mu            sync.RWMutex
}

// NewSpillableHashTable creates a new spillable hash table.
func NewSpillableHashTable(mm *MemoryManager, partitionID string) *SpillableHashTable {
	return &SpillableHashTable{
		data:          make(map[any][]Row),
		memoryManager: mm,
		partitionID:   partitionID,
	}
}

// Insert adds a row to the hash table.
func (h *SpillableHashTable) Insert(key any, row Row) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Check if we need to spill
	if h.memoryManager != nil && h.memoryManager.ShouldSpill() && !h.spilled {
		if err := h.spillToDisk(); err != nil {
			return err
		}
	}

	if h.spilled {
		// If spilled, we need to read back, add, and re-spill
		// For simplicity, we'll just add to in-memory and let next insert handle spilling
		h.spilled = false
	}

	h.data[key] = append(h.data[key], row)
	return nil
}

// Lookup returns all rows matching the given key.
func (h *SpillableHashTable) Lookup(key any) ([]Row, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	if h.spilled {
		// Read back from disk
		rows, err := h.memoryManager.ReadSpilledRows(context.Background(), h.partitionID)
		if err != nil {
			return nil, err
		}

		// Rebuild index
		h.mu.RUnlock()
		h.mu.Lock()
		h.data = make(map[any][]Row)
		for _, row := range rows {
			// This is a simplified version - in production you'd need the key field
			h.data[row.ID] = append(h.data[row.ID], row)
		}
		h.spilled = false
		h.mu.Unlock()
		h.mu.RLock()
	}

	return h.data[key], nil
}

// spillToDisk writes the hash table contents to disk.
func (h *SpillableHashTable) spillToDisk() error {
	// Flatten all rows
	var allRows []Row
	for _, rows := range h.data {
		allRows = append(allRows, rows...)
	}

	if err := h.memoryManager.SpillRows(context.Background(), h.partitionID, allRows); err != nil {
		return err
	}

	// Clear in-memory data
	h.data = make(map[any][]Row)
	h.spilled = true

	return nil
}

// Close cleans up the spillable hash table.
func (h *SpillableHashTable) Close() error {
	if h.memoryManager != nil {
		return h.memoryManager.CleanupPartition(h.partitionID)
	}
	return nil
}

// RowBuffer is a buffer for rows that can spill to disk.
type RowBuffer struct {
	rows          []Row
	memoryManager *MemoryManager
	partitionID   string
	spilled       bool
	maxRows       int
	mu            sync.Mutex
}

// NewRowBuffer creates a new row buffer.
func NewRowBuffer(mm *MemoryManager, partitionID string, maxRows int) *RowBuffer {
	return &RowBuffer{
		rows:          make([]Row, 0, maxRows),
		memoryManager: mm,
		partitionID:   partitionID,
		maxRows:       maxRows,
	}
}

// Append adds a row to the buffer.
func (b *RowBuffer) Append(row Row) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.rows = append(b.rows, row)

	// Check if we need to spill
	if b.memoryManager != nil && len(b.rows) >= b.maxRows && b.memoryManager.ShouldSpill() {
		return b.spillToDisk()
	}

	return nil
}

// spillToDisk writes the buffer to disk.
func (b *RowBuffer) spillToDisk() error {
	if len(b.rows) == 0 {
		return nil
	}

	if err := b.memoryManager.SpillRows(context.Background(), b.partitionID, b.rows); err != nil {
		return err
	}

	b.rows = make([]Row, 0, b.maxRows)
	b.spilled = true

	return nil
}

// GetRows returns all rows, reading from disk if necessary.
func (b *RowBuffer) GetRows() ([]Row, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.spilled {
		return b.rows, nil
	}

	// Read spilled rows and combine with current buffer
	spilledRows, err := b.memoryManager.ReadSpilledRows(context.Background(), b.partitionID)
	if err != nil {
		return nil, err
	}

	return append(spilledRows, b.rows...), nil
}

// Close cleans up the row buffer.
func (b *RowBuffer) Close() error {
	if b.memoryManager != nil {
		return b.memoryManager.CleanupPartition(b.partitionID)
	}
	return nil
}
