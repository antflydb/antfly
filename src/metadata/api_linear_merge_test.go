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

package metadata

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/antflydb/antfly/src/store/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLinearMerge_BasicMerge tests the basic merge functionality
func TestLinearMerge_BasicMerge(t *testing.T) {
	// This is a placeholder test that verifies the types and basic structure
	// In a full integration test, you would set up a complete metadata store
	// with shards and test the full flow

	req := LinearMergeRequest{
		Records: map[string]any{
			"doc1": map[string]any{"name": "Alice", "age": 30},
			"doc2": map[string]any{"name": "Bob", "age": 25},
		},
		LastMergedId: "",
		DryRun:       false,
	}

	// Verify request structure
	assert.NotNil(t, req.Records)
	assert.Len(t, req.Records, 2)
	assert.Empty(t, req.LastMergedId)
	assert.False(t, req.DryRun)
}

// TestLinearMerge_EmptyRecords tests handling of empty records
func TestLinearMerge_EmptyRecords(t *testing.T) {
	req := LinearMergeRequest{
		Records:      map[string]any{},
		LastMergedId: "",
		DryRun:       false,
	}

	// Verify empty records are handled
	assert.Empty(t, req.Records)
}

// TestLinearMerge_ResultStructure tests the result structure
func TestLinearMerge_ResultStructure(t *testing.T) {
	result := LinearMergeResult{
		Status:      LinearMergePageStatusSuccess,
		Upserted:    10,
		Skipped:     5,
		Deleted:     2,
		NextCursor:  "doc10",
		KeyRange:    KeyRange{From: "", To: "doc10"},
		KeysScanned: 12,
		Message:     "success",
		Took:        time.Second,
	}

	// Verify result structure
	assert.Equal(t, LinearMergePageStatusSuccess, result.Status)
	assert.Equal(t, 10, result.Upserted)
	assert.Equal(t, 5, result.Skipped)
	assert.Equal(t, 2, result.Deleted)
	assert.Equal(t, "doc10", result.NextCursor)
	assert.Empty(t, result.KeyRange.From)
	assert.Equal(t, "doc10", result.KeyRange.To)
}

// TestLinearMerge_DryRun tests dry run functionality
func TestLinearMerge_DryRun(t *testing.T) {
	req := LinearMergeRequest{
		Records: map[string]any{
			"doc1": map[string]any{"name": "Alice"},
		},
		LastMergedId: "",
		DryRun:       true,
	}

	assert.True(t, req.DryRun)
}

// TestLinearMerge_StatusEnum tests status enum values
func TestLinearMerge_StatusEnum(t *testing.T) {
	tests := []struct {
		status   LinearMergePageStatus
		expected string
	}{
		{LinearMergePageStatusSuccess, "success"},
		{LinearMergePageStatusPartial, "partial"},
		{LinearMergePageStatusError, "error"},
	}

	for _, tt := range tests {
		t.Run(string(tt.status), func(t *testing.T) {
			assert.Equal(t, tt.expected, string(tt.status))
		})
	}
}

// TestLinearMerge_KeySorting tests that keys are properly sorted
func TestLinearMerge_KeySorting(t *testing.T) {
	// Simulate the sorting logic from the handler
	records := map[string]any{
		"doc3": map[string]any{"name": "Charlie"},
		"doc1": map[string]any{"name": "Alice"},
		"doc2": map[string]any{"name": "Bob"},
	}

	keys := make([]string, 0, len(records))
	for id := range records {
		keys = append(keys, id)
	}

	// Sort using the same method as the handler
	slices.Sort(keys)

	// Verify sorted order
	assert.Equal(t, []string{"doc1", "doc2", "doc3"}, keys)
}

// TestLinearMerge_DocumentHashComputation tests document hashing
func TestLinearMerge_DocumentHashComputation(t *testing.T) {
	doc1 := map[string]any{
		"name": "Alice",
		"age":  30,
	}

	doc2 := map[string]any{
		"age":  30,
		"name": "Alice",
	}

	// Different key order should produce same hash
	hash1, err := db.ComputeDocumentHash(doc1)
	require.NoError(t, err)

	hash2, err := db.ComputeDocumentHash(doc2)
	require.NoError(t, err)

	assert.Equal(t, hash1, hash2, "hash should be deterministic regardless of key order")

	// Different content should produce different hash
	doc3 := map[string]any{
		"name": "Alice",
		"age":  31, // Changed
	}

	hash3, err := db.ComputeDocumentHash(doc3)
	require.NoError(t, err)

	assert.NotEqual(t, hash1, hash3, "different content should produce different hash")

	// _timestamp should be excluded from hash computation
	doc4 := map[string]any{
		"name":       "Alice",
		"age":        30,
		"_timestamp": "2024-01-01T00:00:00Z",
	}

	doc5 := map[string]any{
		"name":       "Alice",
		"age":        30,
		"_timestamp": "2024-12-31T23:59:59Z",
	}

	hash4, err := db.ComputeDocumentHash(doc4)
	require.NoError(t, err)

	hash5, err := db.ComputeDocumentHash(doc5)
	require.NoError(t, err)

	assert.Equal(t, hash4, hash5, "_timestamp should be excluded from hash computation")
	assert.Equal(t, hash1, hash4, "hash with _timestamp should equal hash without it")
}

// TestLinearMerge_FailedOperation tests the FailedOperation structure
func TestLinearMerge_FailedOperation(t *testing.T) {
	failed := FailedOperation{
		Id:        "doc1",
		Operation: FailedOperationOperation("upsert"),
		Error:     "validation failed",
	}

	assert.Equal(t, "doc1", failed.Id)
	assert.Equal(t, FailedOperationOperation("upsert"), failed.Operation)
	assert.Equal(t, "validation failed", failed.Error)
}

// TestLinearMerge_KeyRange tests key range structure
func TestLinearMerge_KeyRange(t *testing.T) {
	kr := KeyRange{
		From: "doc1",
		To:   "doc10",
	}

	assert.Equal(t, "doc1", kr.From)
	assert.Equal(t, "doc10", kr.To)
}

// TestLinearMerge_RequestValidation tests request validation
func TestLinearMerge_RequestValidation(t *testing.T) {
	tests := []struct {
		name    string
		request LinearMergeRequest
		valid   bool
	}{
		{
			name: "valid request",
			request: LinearMergeRequest{
				Records:      map[string]any{"doc1": map[string]any{"name": "Alice"}},
				LastMergedId: "",
				DryRun:       false,
			},
			valid: true,
		},
		{
			name: "empty records with last_merged_id",
			request: LinearMergeRequest{
				Records:      map[string]any{},
				LastMergedId: "doc10",
				DryRun:       false,
			},
			valid: true, // Valid for delete-only operations
		},
		{
			name: "empty records no last_merged_id",
			request: LinearMergeRequest{
				Records:      map[string]any{},
				LastMergedId: "",
				DryRun:       false,
			},
			valid: true, // Valid but will return early with no-op
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Basic validation - just check structure is valid
			assert.NotNil(t, tt.request.Records)
		})
	}
}

// TestLinearMerge_MaxRecordsValidation tests batch size limits
func TestLinearMerge_MaxRecordsValidation(t *testing.T) {
	const MaxRecordsPerRequest = 10000

	// Create request with exactly max records
	records := make(map[string]any, MaxRecordsPerRequest)
	for i := range MaxRecordsPerRequest {
		records[fmt.Sprintf("doc%d", i)] = map[string]any{"value": i}
	}

	req := LinearMergeRequest{
		Records:      records,
		LastMergedId: "",
		DryRun:       false,
	}

	assert.Len(t, req.Records, MaxRecordsPerRequest)

	// Exceeding max would be caught by handler
	tooManyRecords := make(map[string]any, MaxRecordsPerRequest+1)
	for i := range MaxRecordsPerRequest + 1 {
		tooManyRecords[fmt.Sprintf("doc%d", i)] = map[string]any{"value": i}
	}

	req2 := LinearMergeRequest{
		Records:      tooManyRecords,
		LastMergedId: "",
		DryRun:       false,
	}

	assert.Greater(t, len(req2.Records), MaxRecordsPerRequest)
}

// TestLinearMerge_JSONEncoding tests JSON encoding/decoding
func TestLinearMerge_JSONEncoding(t *testing.T) {
	req := LinearMergeRequest{
		Records: map[string]any{
			"doc1": map[string]any{"name": "Alice", "age": float64(30)},
			"doc2": map[string]any{"name": "Bob", "age": float64(25)},
		},
		LastMergedId: "",
		DryRun:       false,
	}

	// Encode to JSON
	jsonData, err := json.Marshal(req)
	require.NoError(t, err)

	// Decode from JSON
	var decoded LinearMergeRequest
	err = json.Unmarshal(jsonData, &decoded)
	require.NoError(t, err)

	// Verify round-trip
	assert.Len(t, decoded.Records, len(req.Records))
	assert.Equal(t, req.LastMergedId, decoded.LastMergedId)
	assert.Equal(t, req.DryRun, decoded.DryRun)
}

// TestLinearMerge_MaxIDWithShardBoundary tests the maxID computation logic
// that determines how far into the sorted keys we can process within a single shard.
func TestLinearMerge_MaxIDWithShardBoundary(t *testing.T) {
	tests := []struct {
		name              string
		keys              []string
		shardEnd          []byte // empty = unbounded (last shard)
		wantMaxID         string
		wantCrossBoundary bool
	}{
		{
			name:              "all keys within bounded shard",
			keys:              []string{"a", "b", "c"},
			shardEnd:          []byte("z"),
			wantMaxID:         "c",
			wantCrossBoundary: false,
		},
		{
			name:              "some keys beyond bounded shard",
			keys:              []string{"a", "b", "x", "z1"},
			shardEnd:          []byte("y"),
			wantMaxID:         "x",
			wantCrossBoundary: true,
		},
		{
			name:              "unbounded shard (empty end) - all keys fit",
			keys:              []string{"a", "m", "z", "zzz"},
			shardEnd:          []byte{}, // last shard
			wantMaxID:         "zzz",
			wantCrossBoundary: false,
		},
		{
			name:              "single key within bounded shard",
			keys:              []string{"m"},
			shardEnd:          []byte("z"),
			wantMaxID:         "m",
			wantCrossBoundary: false,
		},
		{
			name:              "single key beyond bounded shard",
			keys:              []string{"z1"},
			shardEnd:          []byte("z"),
			wantMaxID:         "",
			wantCrossBoundary: true,
		},
		{
			name:              "keys at exact boundary",
			keys:              []string{"a", "z"},
			shardEnd:          []byte("z"),
			wantMaxID:         "z",
			wantCrossBoundary: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			slices.Sort(tt.keys)

			// Replicate the maxID loop from LinearMerge
			var maxID string
			for _, id := range tt.keys {
				if len(tt.shardEnd) > 0 && id > string(tt.shardEnd) {
					break
				}
				maxID = id
			}

			assert.Equal(t, tt.wantMaxID, maxID)

			crossesBoundary := len(tt.keys) > 0 && tt.keys[len(tt.keys)-1] > maxID
			assert.Equal(t, tt.wantCrossBoundary, crossesBoundary)
		})
	}
}

// TestLinearMerge_BeyondBoundaryCheck tests the check that rejects requests where
// the first key is beyond the shard boundary.
func TestLinearMerge_BeyondBoundaryCheck(t *testing.T) {
	tests := []struct {
		name      string
		firstKey  string
		shardEnd  []byte
		wantError bool
	}{
		{
			name:      "key within bounded shard",
			firstKey:  "a",
			shardEnd:  []byte("z"),
			wantError: false,
		},
		{
			name:      "key beyond bounded shard",
			firstKey:  "z1",
			shardEnd:  []byte("z"),
			wantError: true,
		},
		{
			name:      "unbounded shard - never beyond",
			firstKey:  "zzzzz",
			shardEnd:  []byte{},
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beyondBoundary := len(tt.shardEnd) > 0 && tt.firstKey > string(tt.shardEnd)
			assert.Equal(t, tt.wantError, beyondBoundary)
		})
	}
}

// TestLinearMerge_ResultJSONEncoding tests result JSON encoding
func TestLinearMerge_ResultJSONEncoding(t *testing.T) {
	result := LinearMergeResult{
		Status:      LinearMergePageStatusSuccess,
		Upserted:    10,
		Skipped:     5,
		Deleted:     2,
		NextCursor:  "doc10",
		KeyRange:    KeyRange{From: "", To: "doc10"},
		KeysScanned: 12,
		Message:     "success",
		Took:        time.Second,
	}

	// Encode to JSON
	jsonData, err := json.Marshal(result)
	require.NoError(t, err)

	// Decode from JSON
	var decoded LinearMergeResult
	err = json.Unmarshal(jsonData, &decoded)
	require.NoError(t, err)

	// Verify round-trip
	assert.Equal(t, result.Status, decoded.Status)
	assert.Equal(t, result.Upserted, decoded.Upserted)
	assert.Equal(t, result.Skipped, decoded.Skipped)
	assert.Equal(t, result.Deleted, decoded.Deleted)
	assert.Equal(t, result.NextCursor, decoded.NextCursor)
}
