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
	"testing"
	"time"

	"github.com/antflydb/antfly/lib/encoding"
	"github.com/antflydb/antfly/lib/schema"
	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store/db/indexes"
	"github.com/antflydb/antfly/src/store/storeutils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTTLDocumentExpiration(t *testing.T) {
	t.Run("document expired based on timestamp", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "", // Will default to _timestamp
		}

		// Document with timestamp 2 hours ago (should be expired)
		now := time.Now().UTC()
		twoHoursAgo := now.Add(-2 * time.Hour)
		doc := map[string]any{
			"_timestamp": twoHoursAgo.Format(time.RFC3339Nano),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.True(t, expired, "Document should be expired")
	})

	t.Run("document not expired", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "", // Will default to _timestamp
		}

		// Document with timestamp 30 minutes ago (should not be expired)
		now := time.Now().UTC()
		thirtyMinutesAgo := now.Add(-30 * time.Minute)
		doc := map[string]any{
			"_timestamp": thirtyMinutesAgo.Format(time.RFC3339Nano),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.False(t, expired, "Document should not be expired")
	})

	t.Run("custom TTL field", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "24h",
			TtlField:    "created_at",
		}

		// Document with custom timestamp 2 days ago (should be expired)
		now := time.Now().UTC()
		twoDaysAgo := now.Add(-48 * time.Hour)
		doc := map[string]any{
			"created_at": twoDaysAgo.Format(time.RFC3339Nano),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.True(t, expired, "Document should be expired based on custom field")
	})

	t.Run("no TTL configured", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "", // No TTL
		}

		// Any document should not expire if TTL is not configured
		doc := map[string]any{
			"_timestamp": "2020-01-01T00:00:00Z",
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, time.Now().UTC())
		require.NoError(t, err)
		assert.False(t, expired, "Document should not expire without TTL configured")
	})

	t.Run("missing TTL field", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "_timestamp",
		}

		// Document without the required TTL field
		doc := map[string]any{
			"data": "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, time.Now().UTC())
		assert.Error(t, err, "Should error when TTL field is missing")
		assert.False(t, expired)
	})

	t.Run("invalid timestamp format", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "_timestamp",
		}

		// Document with invalid timestamp
		doc := map[string]any{
			"_timestamp": "invalid-timestamp",
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, time.Now().UTC())
		assert.Error(t, err, "Should error with invalid timestamp format")
		assert.False(t, expired)
	})

	t.Run("RFC3339 timestamp without nano precision", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "_timestamp",
		}

		// Document with RFC3339 timestamp (without nano precision)
		now := time.Now().UTC()
		twoHoursAgo := now.Add(-2 * time.Hour)
		doc := map[string]any{
			"_timestamp": twoHoursAgo.Format(time.RFC3339),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.True(t, expired, "Should handle RFC3339 format")
	})

	t.Run("invalid duration format", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "invalid-duration",
			TtlField:    "_timestamp",
		}

		doc := map[string]any{
			"_timestamp": time.Now().Format(time.RFC3339Nano),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, time.Now().UTC())
		assert.Error(t, err, "Should error with invalid duration format")
		assert.False(t, expired)
	})
}

func TestTTLValidation(t *testing.T) {
	t.Run("validate document with TTL field", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "", // Defaults to _timestamp
		}

		doc := map[string]any{
			"_timestamp": time.Now().UTC().Format(time.RFC3339Nano),
			"data":       "test",
		}

		_, err := tableSchema.ValidateDoc(doc)
		require.NoError(t, err, "Should validate document with TTL field")
	})

	t.Run("reject document without required TTL field", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "_timestamp",
		}

		doc := map[string]any{
			"data": "test",
		}

		_, err := tableSchema.ValidateDoc(doc)
		assert.Error(t, err, "Should reject document without required TTL field")
		assert.Contains(t, err.Error(), "_timestamp")
	})

	t.Run("accept document without TTL field when TTL not configured", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "", // No TTL configured
		}

		doc := map[string]any{
			"data": "test",
		}

		_, err := tableSchema.ValidateDoc(doc)
		require.NoError(t, err, "Should accept document without TTL field when TTL not configured")
	})

	t.Run("custom TTL field validation", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "24h",
			TtlField:    "created_at",
		}

		// Document with custom field
		doc := map[string]any{
			"created_at": time.Now().UTC().Format(time.RFC3339Nano),
			"data":       "test",
		}

		_, err := tableSchema.ValidateDoc(doc)
		require.NoError(t, err, "Should validate with custom TTL field")

		// Document without custom field
		docMissing := map[string]any{
			"data": "test",
		}

		_, err = tableSchema.ValidateDoc(docMissing)
		assert.Error(t, err, "Should reject document without custom TTL field")
		assert.Contains(t, err.Error(), "created_at")
	})
}

func TestTTLGetTTLField(t *testing.T) {
	t.Run("custom TTL field", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlField: "my_custom_field",
		}

		assert.Equal(t, "my_custom_field", tableSchema.GetTTLField())
	})

	t.Run("default TTL field", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlField: "",
		}

		assert.Equal(t, "_timestamp", tableSchema.GetTTLField())
	})
}

func TestTTLCleanupJob(t *testing.T) {
	ctx := context.Background()

	// Create a test DB with TTL configured
	dbImpl, cleanup := createTestDBWithTTL(t, "1h")
	defer cleanup()

	// Insert documents with HLC timestamps
	now := time.Now()

	// Document that should expire (3 hours old)
	expiredKey := []byte("expired-doc")
	expiredTimestamp := uint64(now.Add(-3 * time.Hour).UnixNano())

	// Document that should NOT expire (30 minutes old)
	validKey := []byte("valid-doc")
	validTimestamp := uint64(now.Add(-30 * time.Minute).UnixNano())

	// Write documents and their HLC timestamps directly to Pebble
	batch := dbImpl.pdb.NewBatch()

	// Expired document
	expiredDoc := map[string]any{"id": "expired", "data": "old"}
	expiredJSON, _ := json.Marshal(expiredDoc)
	compressedExpired, _ := compressJSON(expiredJSON)
	require.NoError(t, batch.Set(
		append(expiredKey, storeutils.DBRangeStart...),
		compressedExpired,
		nil,
	))
	// Write HLC timestamp for expired doc
	expiredTimestampBytes := encoding.EncodeUint64Ascending(nil, expiredTimestamp)
	require.NoError(t, batch.Set(
		append(expiredKey, storeutils.TransactionSuffix...),
		expiredTimestampBytes,
		nil,
	))

	// Valid document
	validDoc := map[string]any{"id": "valid", "data": "recent"}
	validJSON, _ := json.Marshal(validDoc)
	compressedValid, _ := compressJSON(validJSON)
	require.NoError(t, batch.Set(
		append(validKey, storeutils.DBRangeStart...),
		compressedValid,
		nil,
	))
	// Write HLC timestamp for valid doc
	validTimestampBytes := encoding.EncodeUint64Ascending(nil, validTimestamp)
	require.NoError(t, batch.Set(
		append(validKey, storeutils.TransactionSuffix...),
		validTimestampBytes,
		nil,
	))

	require.NoError(t, batch.Commit(pebble.Sync))
	batch.Close()

	// Verify both documents exist before cleanup (check Pebble directly)
	_, closer, err := dbImpl.pdb.Get(append(expiredKey, storeutils.DBRangeStart...))
	require.NoError(t, err, "Expired document should exist before cleanup")
	closer.Close()
	_, closer, err = dbImpl.pdb.Get(append(validKey, storeutils.DBRangeStart...))
	require.NoError(t, err, "Valid document should exist before cleanup")
	closer.Close()

	// Create TTL cleaner and run cleanup once
	cleaner := NewTTLCleaner(dbImpl)
	ttlDuration, err := time.ParseDuration(dbImpl.schema.TtlDuration)
	require.NoError(t, err)

	// Run cleanup with a persist function that deletes the keys from Pebble
	count, err := cleaner.cleanupExpiredDocuments(ctx, ttlDuration, func(ctx context.Context, writes [][2][]byte) error {
		// In real scenario, this would propose to Raft
		// For testing, we'll apply directly to Pebble
		batch := dbImpl.pdb.NewBatch()
		defer batch.Close()
		for _, write := range writes {
			if write[1] == nil {
				// Delete operation - delete document and all associated keys
				baseKey := write[0]
				deleteStart := storeutils.KeyRangeStart(baseKey)
				deleteEnd := storeutils.KeyRangeEnd(baseKey)
				if err := batch.DeleteRange(deleteStart, deleteEnd, nil); err != nil {
					return err
				}
			}
		}
		return batch.Commit(pebble.Sync)
	})
	if err != nil {
		if errors.Is(err, pebble.ErrClosed) {
			t.Skip("Pebble closed during TTL cleanup (race-detector timing)")
		}
		require.NoError(t, err)
	}
	assert.Equal(t, 1, count, "Should have cleaned up 1 expired document")

	// Verify expired document is gone (check Pebble directly)
	_, closer, err = dbImpl.pdb.Get(append(expiredKey, storeutils.DBRangeStart...))
	assert.ErrorIs(t, err, pebble.ErrNotFound, "Expired document should be deleted")
	if closer != nil {
		closer.Close()
	}

	// Verify valid document still exists (check Pebble directly)
	_, closer, err = dbImpl.pdb.Get(append(validKey, storeutils.DBRangeStart...))
	require.NoError(t, err, "Valid document should still exist")
	closer.Close()
}

func TestTTLQueryFiltering(t *testing.T) {
	ctx := context.Background()

	// Create a test DB with TTL configured
	dbImpl, cleanup := createTestDBWithTTL(t, "1h")
	defer cleanup()

	now := time.Now()

	// Insert expired document (2 hours old)
	expiredKey := []byte("expired-query-doc")
	expiredTimestamp := uint64(now.Add(-2 * time.Hour).UnixNano())

	// Insert valid document (15 minutes old)
	validKey := []byte("valid-query-doc")
	validTimestamp := uint64(now.Add(-15 * time.Minute).UnixNano())

	// Write documents with HLC timestamps
	batch := dbImpl.pdb.NewBatch()

	// Expired document
	expiredDoc := map[string]any{"id": "expired-query", "status": "old"}
	expiredJSON, _ := json.Marshal(expiredDoc)
	compressedExpired, _ := compressJSON(expiredJSON)
	require.NoError(t, batch.Set(
		append(expiredKey, storeutils.DBRangeStart...),
		compressedExpired,
		nil,
	))
	expiredTimestampBytes := encoding.EncodeUint64Ascending(nil, expiredTimestamp)
	require.NoError(t, batch.Set(
		append(expiredKey, storeutils.TransactionSuffix...),
		expiredTimestampBytes,
		nil,
	))

	// Valid document
	validDoc := map[string]any{"id": "valid-query", "status": "active"}
	validJSON, _ := json.Marshal(validDoc)
	compressedValid, _ := compressJSON(validJSON)
	require.NoError(t, batch.Set(
		append(validKey, storeutils.DBRangeStart...),
		compressedValid,
		nil,
	))
	validTimestampBytes := encoding.EncodeUint64Ascending(nil, validTimestamp)
	require.NoError(t, batch.Set(
		append(validKey, storeutils.TransactionSuffix...),
		validTimestampBytes,
		nil,
	))

	require.NoError(t, batch.Commit(pebble.Sync))
	batch.Close()

	// Test Get on expired document - should return ErrNotFound
	_, err := dbImpl.Get(ctx, expiredKey)
	assert.ErrorIs(t, err, ErrNotFound, "Get on expired document should return ErrNotFound")

	// Test Get on valid document - should succeed
	doc, err := dbImpl.Get(ctx, validKey)
	require.NoError(t, err, "Get on valid document should succeed")
	assert.Equal(t, "valid-query", doc["id"])
	assert.Equal(t, "active", doc["status"])

	// Test Scan - expired documents should be filtered out
	scanResult, err := dbImpl.Scan(ctx,
		[]byte("a"), // From
		[]byte("z"), // To
		ScanOptions{
			IncludeDocuments: true,
			InclusiveFrom:    true,
			ExclusiveTo:      false,
		},
	)
	require.NoError(t, err, "Scan should succeed")

	// Should only have the valid document
	assert.Len(t, scanResult.Documents, 1, "Scan should return only 1 document")
	validDocID := string(validKey)
	assert.Contains(t, scanResult.Documents, validDocID, "Scan should contain valid document")
	assert.Equal(t, "active", scanResult.Documents[validDocID]["status"])

	// Expired document should not be in results
	expiredDocID := string(expiredKey)
	assert.NotContains(t, scanResult.Documents, expiredDocID, "Scan should not contain expired document")
}

// Helper functions for testing

func createTestDBWithTTL(t *testing.T, ttlDuration string) (*DBImpl, func()) {
	t.Helper()

	// Create temporary directory for test DB
	tempDir := t.TempDir()

	// Create schema with TTL
	tableSchema := &schema.TableSchema{
		TtlDuration: ttlDuration,
		TtlField:    "_timestamp",
		DefaultType: "default",
		DocumentSchemas: map[string]schema.DocumentSchema{
			"default": {
				Schema: map[string]any{
					"type": "object",
				},
			},
		},
	}

	// Create DBImpl with proper initialization
	dbImpl := &DBImpl{
		logger:       zap.NewNop(),
		antflyConfig: &common.Config{},
		indexes:      make(map[string]indexes.IndexConfig),
	}
	dbImpl.setByteRange([2][]byte{[]byte(""), []byte("\xFF")})

	// Open the database (this initializes indexManager and other fields)
	err := dbImpl.Open(tempDir, false, tableSchema, [2][]byte{[]byte(""), []byte("\xFF")})
	require.NoError(t, err)

	cleanup := func() {
		if err := dbImpl.Close(); err != nil {
			t.Logf("Failed to close test DB: %v", err)
		}
	}

	return dbImpl, cleanup
}

func compressJSON(jsonBytes []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer encoder.Close()
	return encoder.EncodeAll(jsonBytes, nil), nil
}

func TestTTLCleanerStats(t *testing.T) {
	dbImpl, cleanup := createTestDBWithTTL(t, "1h")
	defer cleanup()

	cleaner := NewTTLCleaner(dbImpl)

	stats := cleaner.Stats()
	assert.Contains(t, stats, "total_documents_expired")
	assert.Contains(t, stats, "last_cleanup_duration_ms")
	assert.Equal(t, int64(0), stats["total_documents_expired"])
	assert.Equal(t, int64(0), stats["last_cleanup_duration_ms"])
}

func TestTTLDurationFormats(t *testing.T) {
	testCases := []struct {
		name         string
		duration     string
		docAge       time.Duration
		shouldExpire bool
		shouldError  bool
	}{
		{
			name:         "hours format - expired",
			duration:     "1h",
			docAge:       2 * time.Hour,
			shouldExpire: true,
		},
		{
			name:         "hours format - not expired",
			duration:     "1h",
			docAge:       30 * time.Minute,
			shouldExpire: false,
		},
		{
			name:         "minutes format - expired",
			duration:     "30m",
			docAge:       45 * time.Minute,
			shouldExpire: true,
		},
		{
			name:         "seconds format - expired",
			duration:     "60s",
			docAge:       2 * time.Minute,
			shouldExpire: true,
		},
		{
			name:         "compound format - not expired",
			duration:     "1h30m",
			docAge:       1 * time.Hour,
			shouldExpire: false,
		},
		{
			name:         "compound format - expired",
			duration:     "1h30m",
			docAge:       2 * time.Hour,
			shouldExpire: true,
		},
		{
			name:         "days as hours - expired",
			duration:     "168h", // 7 days
			docAge:       200 * time.Hour,
			shouldExpire: true,
		},
		{
			name:         "days as hours - not expired",
			duration:     "168h", // 7 days
			docAge:       100 * time.Hour,
			shouldExpire: false,
		},
		{
			name:        "invalid duration format",
			duration:    "7d", // Go doesn't support 'd' suffix
			shouldError: true,
		},
		{
			name:        "invalid duration string",
			duration:    "invalid",
			shouldError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableSchema := &schema.TableSchema{
				TtlDuration: tc.duration,
				TtlField:    "_timestamp",
			}

			now := time.Now().UTC()
			docTime := now.Add(-tc.docAge)
			doc := map[string]any{
				"_timestamp": docTime.Format(time.RFC3339Nano),
				"data":       "test",
			}

			expired, err := tableSchema.IsDocumentExpired(doc, now)
			if tc.shouldError {
				assert.Error(t, err, "Should error with invalid duration format")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.shouldExpire, expired, "Expiration status mismatch")
		})
	}
}

func TestTTLDocumentUpdateTimestamp(t *testing.T) {
	ctx := context.Background()

	// Create a test DB with TTL configured
	dbImpl, cleanup := createTestDBWithTTL(t, "1h")
	defer cleanup()

	now := time.Now()
	docKey := []byte("update-test-doc")

	// Insert document with timestamp 2 hours ago (would be expired)
	oldTimestamp := uint64(now.Add(-2 * time.Hour).UnixNano())
	batch := dbImpl.pdb.NewBatch()

	oldDoc := map[string]any{"id": "update-test", "version": 1}
	oldJSON, _ := json.Marshal(oldDoc)
	compressedOld, _ := compressJSON(oldJSON)
	require.NoError(t, batch.Set(
		append(docKey, storeutils.DBRangeStart...),
		compressedOld,
		nil,
	))
	oldTimestampBytes := encoding.EncodeUint64Ascending(nil, oldTimestamp)
	require.NoError(t, batch.Set(
		append(docKey, storeutils.TransactionSuffix...),
		oldTimestampBytes,
		nil,
	))
	require.NoError(t, batch.Commit(pebble.Sync))
	batch.Close()

	// Document should be expired
	_, err := dbImpl.Get(ctx, docKey)
	assert.ErrorIs(t, err, ErrNotFound, "Old document should be expired")

	// "Update" the document by writing with fresh timestamp
	newTimestamp := uint64(now.UnixNano())
	batch = dbImpl.pdb.NewBatch()

	newDoc := map[string]any{"id": "update-test", "version": 2}
	newJSON, _ := json.Marshal(newDoc)
	compressedNew, _ := compressJSON(newJSON)
	require.NoError(t, batch.Set(
		append(docKey, storeutils.DBRangeStart...),
		compressedNew,
		nil,
	))
	newTimestampBytes := encoding.EncodeUint64Ascending(nil, newTimestamp)
	require.NoError(t, batch.Set(
		append(docKey, storeutils.TransactionSuffix...),
		newTimestampBytes,
		nil,
	))
	require.NoError(t, batch.Commit(pebble.Sync))
	batch.Close()

	// Document should now be accessible (not expired)
	doc, err := dbImpl.Get(ctx, docKey)
	require.NoError(t, err, "Updated document should not be expired")
	assert.Equal(t, float64(2), doc["version"], "Should get updated version")
}

func TestTTLBoundaryConditions(t *testing.T) {
	t.Run("exact expiration boundary", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "_timestamp",
		}

		now := time.Now().UTC()

		// Document at exactly 1 hour ago (edge case)
		exactlyOneHourAgo := now.Add(-1 * time.Hour)
		doc := map[string]any{
			"_timestamp": exactlyOneHourAgo.Format(time.RFC3339Nano),
			"data":       "test",
		}

		// At exact boundary, document is NOT expired (uses strict > comparison)
		// This is by design: documents expire when now > expirationTime
		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.False(t, expired, "Document at exact TTL boundary should not be expired (strict > comparison)")
	})

	t.Run("just before expiration", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "_timestamp",
		}

		now := time.Now().UTC()

		// Document at 59 minutes 59 seconds ago
		almostOneHourAgo := now.Add(-59*time.Minute - 59*time.Second)
		doc := map[string]any{
			"_timestamp": almostOneHourAgo.Format(time.RFC3339Nano),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.False(t, expired, "Document just before TTL should not be expired")
	})

	t.Run("future timestamp", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "1h",
			TtlField:    "_timestamp",
		}

		now := time.Now().UTC()

		// Document with timestamp in the future
		futureTime := now.Add(1 * time.Hour)
		doc := map[string]any{
			"_timestamp": futureTime.Format(time.RFC3339Nano),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.False(t, expired, "Document with future timestamp should never be expired")
	})

	t.Run("zero TTL duration", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "0s",
			TtlField:    "_timestamp",
		}

		now := time.Now().UTC()

		// With 0s TTL checked at exact same time, document is at boundary (not expired)
		// Uses strict > comparison: now.After(now+0s) = false
		doc := map[string]any{
			"_timestamp": now.Format(time.RFC3339Nano),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.False(t, expired, "Document at exact boundary with zero TTL is not expired (strict > comparison)")

		// But a document from any time in the past should be expired
		oneNanoAgo := now.Add(-1 * time.Nanosecond)
		docPast := map[string]any{
			"_timestamp": oneNanoAgo.Format(time.RFC3339Nano),
			"data":       "test",
		}
		expiredPast, err := tableSchema.IsDocumentExpired(docPast, now)
		require.NoError(t, err)
		assert.True(t, expiredPast, "Document from past with zero TTL should be expired")
	})

	t.Run("very long TTL", func(t *testing.T) {
		tableSchema := &schema.TableSchema{
			TtlDuration: "8760h", // 1 year
			TtlField:    "_timestamp",
		}

		now := time.Now().UTC()

		// Document 6 months old
		sixMonthsAgo := now.Add(-4380 * time.Hour)
		doc := map[string]any{
			"_timestamp": sixMonthsAgo.Format(time.RFC3339Nano),
			"data":       "test",
		}

		expired, err := tableSchema.IsDocumentExpired(doc, now)
		require.NoError(t, err)
		assert.False(t, expired, "Document within 1 year TTL should not be expired")
	})
}

func TestTTLCleanerLifecycle(t *testing.T) {
	t.Run("start and stop via context cancellation", func(t *testing.T) {
		dbImpl, cleanup := createTestDBWithTTL(t, "1h")
		defer cleanup()

		cleaner := NewTTLCleaner(dbImpl)
		ctx, cancel := context.WithCancel(context.Background())

		// Start cleaner in goroutine
		errCh := make(chan error, 1)
		go func() {
			errCh <- cleaner.Start(ctx, nil)
		}()

		// Let it run briefly
		time.Sleep(100 * time.Millisecond)

		// Cancel context to stop
		cancel()

		// Should stop with context.Canceled
		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(5 * time.Second):
			t.Fatal("Cleaner did not stop within timeout")
		}
	})

	t.Run("no-op when TTL not configured", func(t *testing.T) {
		// Create DB without TTL
		dbImpl, cleanup := createTestDBWithTTL(t, "") // Empty = no TTL
		defer cleanup()

		cleaner := NewTTLCleaner(dbImpl)
		ctx := context.Background()

		// Should return nil immediately (no-op)
		err := cleaner.Start(ctx, nil)
		assert.NoError(t, err, "Should return nil when TTL not configured")
	})

	t.Run("error on invalid TTL duration", func(t *testing.T) {
		dbImpl, cleanup := createTestDBWithTTL(t, "invalid")
		defer cleanup()

		cleaner := NewTTLCleaner(dbImpl)
		ctx := context.Background()

		err := cleaner.Start(ctx, nil)
		assert.Error(t, err, "Should error with invalid TTL duration")
		assert.Contains(t, err.Error(), "parsing TTL duration")
	})
}

func TestTTLCleanupBatchProcessing(t *testing.T) {
	ctx := context.Background()

	// Create a test DB with short TTL
	dbImpl, cleanup := createTestDBWithTTL(t, "1h")
	defer cleanup()

	now := time.Now()
	expiredTimestamp := uint64(now.Add(-3 * time.Hour).UnixNano())

	// Insert multiple expired documents
	numDocs := 50
	batch := dbImpl.pdb.NewBatch()

	for i := range numDocs {
		key := fmt.Appendf(nil, "batch-doc-%03d", i)
		doc := map[string]any{"id": i, "data": "expired"}
		jsonBytes, _ := json.Marshal(doc)
		compressed, _ := compressJSON(jsonBytes)

		require.NoError(t, batch.Set(
			append(key, storeutils.DBRangeStart...),
			compressed,
			nil,
		))
		timestampBytes := encoding.EncodeUint64Ascending(nil, expiredTimestamp)
		require.NoError(t, batch.Set(
			append(key, storeutils.TransactionSuffix...),
			timestampBytes,
			nil,
		))
	}
	require.NoError(t, batch.Commit(pebble.Sync))
	batch.Close()

	// Run cleanup
	cleaner := NewTTLCleaner(dbImpl)
	ttlDuration, _ := time.ParseDuration(dbImpl.schema.TtlDuration)

	deletedCount, err := cleaner.cleanupExpiredDocuments(ctx, ttlDuration, func(ctx context.Context, writes [][2][]byte) error {
		batch := dbImpl.pdb.NewBatch()
		defer batch.Close()
		for _, write := range writes {
			if write[1] == nil {
				baseKey := write[0]
				deleteStart := storeutils.KeyRangeStart(baseKey)
				deleteEnd := storeutils.KeyRangeEnd(baseKey)
				if err := batch.DeleteRange(deleteStart, deleteEnd, nil); err != nil {
					return err
				}
			}
		}
		return batch.Commit(pebble.Sync)
	})

	require.NoError(t, err)
	assert.Equal(t, numDocs, deletedCount, "Should delete all expired documents")

	// Verify all documents are gone
	for i := range numDocs {
		key := fmt.Appendf(nil, "batch-doc-%03d", i)
		_, _, err := dbImpl.pdb.Get(append(key, storeutils.DBRangeStart...))
		assert.ErrorIs(t, err, pebble.ErrNotFound, "Document %d should be deleted", i)
	}
}

func TestTTLGracePeriod(t *testing.T) {
	ctx := context.Background()

	// Create a test DB with short TTL
	dbImpl, cleanup := createTestDBWithTTL(t, "1s")
	defer cleanup()

	now := time.Now()

	// Document that just expired (within grace period)
	// TTL is 1s, so doc from 2s ago is expired but might still be within grace period
	withinGraceTimestamp := uint64(now.Add(-2 * time.Second).UnixNano())

	// Document well past grace period (1s TTL + 5s grace = 6s, use 10s to be safe)
	pastGraceTimestamp := uint64(now.Add(-10 * time.Second).UnixNano())

	// Insert both documents
	batch := dbImpl.pdb.NewBatch()

	// Within grace period
	withinGraceKey := []byte("within-grace")
	withinGraceDoc := map[string]any{"id": "within-grace", "data": "recent"}
	withinGraceJSON, _ := json.Marshal(withinGraceDoc)
	compressedWithin, _ := compressJSON(withinGraceJSON)
	require.NoError(t, batch.Set(
		append(withinGraceKey, storeutils.DBRangeStart...),
		compressedWithin,
		nil,
	))
	require.NoError(t, batch.Set(
		append(withinGraceKey, storeutils.TransactionSuffix...),
		encoding.EncodeUint64Ascending(nil, withinGraceTimestamp),
		nil,
	))

	// Past grace period
	pastGraceKey := []byte("past-grace")
	pastGraceDoc := map[string]any{"id": "past-grace", "data": "old"}
	pastGraceJSON, _ := json.Marshal(pastGraceDoc)
	compressedPast, _ := compressJSON(pastGraceJSON)
	require.NoError(t, batch.Set(
		append(pastGraceKey, storeutils.DBRangeStart...),
		compressedPast,
		nil,
	))
	require.NoError(t, batch.Set(
		append(pastGraceKey, storeutils.TransactionSuffix...),
		encoding.EncodeUint64Ascending(nil, pastGraceTimestamp),
		nil,
	))

	require.NoError(t, batch.Commit(pebble.Sync))
	batch.Close()

	// Run cleanup
	cleaner := NewTTLCleaner(dbImpl)
	ttlDuration, _ := time.ParseDuration(dbImpl.schema.TtlDuration)

	deletedCount, err := cleaner.cleanupExpiredDocuments(ctx, ttlDuration, func(ctx context.Context, writes [][2][]byte) error {
		batch := dbImpl.pdb.NewBatch()
		defer batch.Close()
		for _, write := range writes {
			if write[1] == nil {
				baseKey := write[0]
				deleteStart := storeutils.KeyRangeStart(baseKey)
				deleteEnd := storeutils.KeyRangeEnd(baseKey)
				if err := batch.DeleteRange(deleteStart, deleteEnd, nil); err != nil {
					return err
				}
			}
		}
		return batch.Commit(pebble.Sync)
	})

	require.NoError(t, err)
	// Should delete at least the past-grace document
	assert.GreaterOrEqual(t, deletedCount, 1, "Should delete at least 1 expired document past grace period")

	// Past grace document should be gone
	_, _, err = dbImpl.pdb.Get(append(pastGraceKey, storeutils.DBRangeStart...))
	assert.ErrorIs(t, err, pebble.ErrNotFound, "Past grace document should be deleted")
}
