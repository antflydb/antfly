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

package common

import (
	"bytes"
	"encoding/base64"
	"maps"
	"os"
	"path/filepath"
	"testing"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Helper function to create a temporary Pebble DB and populate it with data.
// Forces flushes to create multiple SSTables based on key ranges.
func setupTestDB(t *testing.T, data map[string]string, flushPoints [][]byte) (*pebble.DB, types.Range) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(dir, pebbleutils.NewPebbleOpts())
	require.NoError(t, err)

	var minKey, maxKey []byte

	// Insert data and determine overall range
	for k, v := range data {
		keyBytes := []byte(k)
		require.NoError(t, db.Set(keyBytes, []byte(v), pebble.Sync))
		if minKey == nil || bytes.Compare(keyBytes, minKey) < 0 {
			minKey = keyBytes
		}
		// Find the logical successor for the max key to define the range end
		if maxKey == nil || bytes.Compare(keyBytes, maxKey) >= 0 {
			// Simple increment for testing purposes. Might need adjustment for complex keys.
			nextKey := make([]byte, len(keyBytes))
			copy(nextKey, keyBytes)
			if len(nextKey) > 0 {
				nextKey[len(nextKey)-1]++ // Increment the last byte
			} else {
				nextKey = append(nextKey, 0x01) // Handle empty key case?
			}
			maxKey = nextKey
		}
	}

	// Flush at specified points to create SSTables
	for range flushPoints {
		require.NoError(t, db.Flush())
		// Optional: Insert more data between flushes if needed
		// require.NoError(t, db.Set(point, []byte("flush_marker"), pebble.Sync)) // Removed this as it might create tiny unwanted tables
	}
	require.NoError(t, db.Flush()) // Final flush

	// Define the overall range based on inserted data
	testRange := types.Range{minKey, maxKey}
	if minKey == nil || maxKey == nil {
		// Handle case with no data
		testRange = types.Range{[]byte("a"), []byte("z")} // Default test range if no data
	}

	return db, testRange
}

func TestFindSplitKeyByFileSize(t *testing.T) {
	tests := []struct {
		name          string
		data          map[string]string
		flushPoints   [][]byte    // Keys to flush after inserting up to that point (approx)
		targetRange   types.Range // The specific range to test splitting within
		expectedKey   []byte
		expectError   bool
		errorContains string
	}{
		{
			name: "Simple split between two tables",
			data: map[string]string{
				"apple":  "1", // Table 1 (approx)
				"banana": "2", // Table 1
				"carrot": "3", // Table 2 (approx)
				"date":   "4", // Table 2
			},
			flushPoints: [][]byte{[]byte("banana")},                    // Flush after banana
			targetRange: types.Range{[]byte("apple"), []byte("datez")}, // Range covering all data
			// Iterator fallback finds [banana, carrot, date] (apple skipped as it equals range start)
			// Middle index is 3/2 = 1, validKeys[1] = carrot
			expectedKey: []byte("carrot"),
			expectError: false,
		},
		{
			name: "Split within a single large table (approximated)",
			data: map[string]string{
				"key01": "v", "key02": "v", "key03": "v", "key04": "v", "key05": "v",
				"key06": "v", "key07": "v", "key08": "v", "key09": "v", "key10": "v",
			},
			flushPoints: [][]byte{}, // No intermediate flushes, likely one big L0 table
			targetRange: types.Range{[]byte("key01"), []byte("key11")},
			// Iterator fallback finds 9 valid keys [key02..key10] (key01 is skipped as it equals range start)
			// Middle index is 9/2 = 4, validKeys[4] = key06
			expectedKey: []byte("key06"),
			expectError: false,
		},
		{
			name: "Range covers only part of the data",
			data: map[string]string{
				"a": "1", "b": "2", "c": "3", // Table 1
				"d": "4", "e": "5", "f": "6", // Table 2
				"g": "7", "h": "8", "i": "9", // Table 3
			},
			flushPoints: [][]byte{[]byte("c"), []byte("f")},
			targetRange: types.Range{[]byte("d"), []byte("g")}, // Range covers only Table 2
			// Iterator fallback finds keys [e, f] in range (d is skipped as it equals range start)
			// Middle index is 2/2 = 1, which is "f"
			expectedKey: []byte("f"),
			expectError: false,
		},
		{
			name: "Split key should be within the range",
			data: map[string]string{
				"k1": "v", "k2": "v", // Table 1
				"k5": "v", "k6": "v", // Table 2
			},
			flushPoints: [][]byte{[]byte("k2")},
			targetRange: types.Range{[]byte("k0"), []byte("k7")}, // Range wider than data
			// Tables are [k1, k2] and [k5, k6]. Both overlap [k0, k7).
			// Sorted: [k1, k2], [k5, k6]. Midpoint index 0. Key 'k1' is valid (>k0, <k7). Returns k1.
			expectedKey: []byte("k1"), // Updated expectation based on current logic
			expectError: false,
		},
		{
			name: "No overlapping tables",
			data: map[string]string{
				"x": "1", "y": "2", "z": "3",
			},
			flushPoints:   [][]byte{},
			targetRange:   types.Range{[]byte("a"), []byte("m")}, // Range before all data
			expectError:   true,
			errorContains: "no overlapping tables found for range", // Updated expected error
		},
		{
			name: "Range start equals potential split key",
			data: map[string]string{
				"a": "1", // Table 1
				"b": "2", // Table 1
				"c": "3", // Table 2
				"d": "4", // Table 2
			},
			flushPoints: [][]byte{[]byte("b")},
			targetRange: types.Range{
				[]byte("c"),
				[]byte("e"),
			}, // Range starts exactly at the second table's key [c, d]
			// Iterator fallback finds keys [c, d] in range
			// Middle index is 2/2 = 1, which is "d"
			expectedKey: []byte("d"),
			expectError: false,
		},
		{
			name:          "Empty database",
			data:          map[string]string{},
			flushPoints:   [][]byte{},
			targetRange:   types.Range{[]byte("a"), []byte("z")},
			expectError:   true,
			errorContains: "no overlapping tables found for range", // Updated expected error
		},
		{
			name: "Multiple levels involved (simulated)",
			// Simulate L0 and L1 by flushing, compacting (implicitly), adding more, flushing again
			data: map[string]string{
				// Initial data -> likely L0 -> maybe Lbase after compaction
				"c": "3", "d": "4",
				// Add more data -> new L0
				"a": "1", "b": "2",
				"e": "5", "f": "6",
			},
			// Flush after 'd', then add 'a','b','e','f' and flush again.
			// This setup is complex to guarantee levels, but tests table selection logic.
			flushPoints: func() [][]byte {
				// This setup is tricky. Let's just create distinct tables.
				return [][]byte{[]byte("b"), []byte("d")} // Flush after b, then after d
			}(),
			targetRange: types.Range{[]byte("a"), []byte("g")}, // Cover all data
			// Iterator fallback finds [b, c, d, e, f] in range (a is skipped as it equals range start)
			// Middle index is 5/2 = 2, validKeys[2] = d
			expectedKey: []byte("d"),
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Rebuild data map for each test to avoid modification issues if flushPoints logic changes
			currentData := make(map[string]string)
			maps.Copy(currentData, tc.data)
			// Sort keys to apply flushes logically if needed, though current setup uses explicit points
			// sort.Strings(keysInOrder)

			// Setup DB
			db, testRange := setupTestDB(t, currentData, tc.flushPoints)
			t.Cleanup(func() {
				if err := db.Close(); err != nil {
					t.Logf("failed to close DB: %v", err)
				}
			})

			// Use the test case's target range if specified, otherwise use the full data range
			rangeToTest := tc.targetRange
			if rangeToTest[0] == nil && rangeToTest[1] == nil {
				rangeToTest = testRange
			}

			// Call the function
			splitKey, err := FindSplitKeyByFileSize(zap.NewNop(), db, rangeToTest, nil)

			// Assertions
			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					require.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.expectedKey, splitKey, "Expected split key %q, got %q", tc.expectedKey, splitKey)
				// Verify the split key is actually within the *target range* and > start
				require.Positive(t, bytes.Compare(splitKey, rangeToTest[0]), "Split key must be greater than range start")
				require.Negative(t, bytes.Compare(splitKey, rangeToTest[1]), "Split key must be less than range end")
			}
		})
	}
}

// Helper to create a test DB specifically for the edge case where the only suitable
// split key found is the start key of the range.
func setupDBForStartKeySplitIssue(t *testing.T) (*pebble.DB, types.Range) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(dir, pebbleutils.NewPebbleOpts())
	require.NoError(t, err)

	// Create two distinct SSTables
	// Table 1
	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.Sync))
	require.NoError(t, db.Set([]byte("b"), []byte("2"), pebble.Sync))
	require.NoError(t, db.Flush())

	// Table 2
	require.NoError(t, db.Set([]byte("c"), []byte("3"), pebble.Sync))
	require.NoError(t, db.Set([]byte("d"), []byte("4"), pebble.Sync))
	require.NoError(t, db.Flush())

	// Range starts exactly at the beginning of the second table
	testRange := types.Range{[]byte("c"), []byte("e")}

	return db, testRange
}

func TestFindSplitKeyByFileSize_StartKeyIssue(t *testing.T) {
	db, testRange := setupDBForStartKeySplitIssue(t)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close DB: %v", err)
		}
	})

	splitKey, err := FindSplitKeyByFileSize(zap.NewNop(), db, testRange, nil)

	// Iterator fallback finds [d] in range [c, e) (c is skipped as it equals range start)
	// Middle index is 1/2 = 0, validKeys[0] = d
	require.NoError(t, err)
	require.Equal(t, []byte("d"), splitKey)
}

// Helper to create a test DB where the range covers only one table.
func setupDBForSingleTableRange(t *testing.T) (*pebble.DB, types.Range) {
	t.Helper()
	dir := t.TempDir()
	db, err := pebble.Open(dir, pebbleutils.NewPebbleOpts())
	require.NoError(t, err)

	// Table 1
	require.NoError(t, db.Set([]byte("a"), []byte("1"), pebble.Sync))
	require.NoError(t, db.Flush())
	// Table 2
	require.NoError(t, db.Set([]byte("d"), []byte("4"), pebble.Sync))
	require.NoError(t, db.Set([]byte("e"), []byte("5"), pebble.Sync))
	require.NoError(t, db.Flush())
	// Table 3
	require.NoError(t, db.Set([]byte("h"), []byte("8"), pebble.Sync))
	require.NoError(t, db.Flush())

	// Range covers only the second table
	testRange := types.Range{[]byte("d"), []byte("g")} // [d, g)

	return db, testRange
}

func TestFindSplitKeyByFileSize_SingleTableRangeIssue(t *testing.T) {
	db, testRange := setupDBForSingleTableRange(t)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close DB: %v", err)
		}
	})

	splitKey, err := FindSplitKeyByFileSize(zap.NewNop(), db, testRange, nil)

	// Iterator fallback finds [e] in range [d, g) (d is skipped as it equals range start)
	// Middle index is 1/2 = 0, validKeys[0] = e
	require.NoError(t, err)
	require.Equal(t, []byte("e"), splitKey)
}

// TestFindSplitKeyByFileSize_SingleKeyRange tests that a range with only one key
// correctly returns an error since it cannot be split further.
func TestFindSplitKeyByFileSize_SingleKeyRange(t *testing.T) {
	dir := t.TempDir()
	db, err := pebble.Open(dir, pebbleutils.NewPebbleOpts())
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Logf("failed to close DB: %v", err)
		}
	})

	// Create a single key in the database
	require.NoError(t, db.Set([]byte("only-key"), []byte("value"), pebble.Sync))
	require.NoError(t, db.Flush())

	// Range contains only the single key
	testRange := types.Range{[]byte("only-key"), []byte("only-keyz")}

	splitKey, err := FindSplitKeyByFileSize(zap.NewNop(), db, testRange, nil)

	// Should return error because there's no valid split key
	// (the only key equals the range start, which is not a valid split point)
	require.Error(t, err)
	require.Nil(t, splitKey)
	require.Contains(t, err.Error(), "couldn't find suitable split key")
}

func TestArchiveRoundtrip(t *testing.T) {
	tests := []struct {
		name        string
		archiveType ArchiveType
		files       map[string][]byte
	}{
		{
			name:        "gzip small files",
			archiveType: ArchiveGzip,
			files: map[string][]byte{
				"file1.txt":           []byte("hello world"),
				"dir1/file2.txt":      []byte("nested file"),
				"dir1/dir2/file3.txt": []byte("deeply nested"),
				"empty.txt":           []byte(""),
				"binary.bin":          bytes.Repeat([]byte{0x00, 0xFF, 0xAA, 0x55}, 256),
			},
		},
		{
			name:        "snappy large file",
			archiveType: ArchiveSnappy,
			files: map[string][]byte{
				"large.bin": bytes.Repeat([]byte("large file content"), 100000), // ~1.7MB
			},
		},
		{
			name:        "zstd mixed files",
			archiveType: ArchiveZstd,
			files: map[string][]byte{
				"text.txt":              []byte("plain text file"),
				"data/binary.dat":       bytes.Repeat([]byte{0x01, 0x02, 0x03, 0x04}, 1024),
				"path/to/deep/file.log": []byte("log content"),
			},
		},
		{
			name:        "long paths",
			archiveType: ArchiveGzip,
			files: map[string][]byte{
				// Test with moderately long paths (tar has a 100 char limit for names in POSIX format)
				"dir1/dir2/dir3/dir4/dir5/file_with_a_reasonably_long_name.txt": []byte("content"),
				// Path with exactly 99 characters (just under the limit)
				"a/b/c/d/e/f/g/h/i/j/k/l/m/n/o/p/q/r/s/t/u/v/w/x/y/z/exactly_ninety_nine_chars_path.txt": []byte(
					"99 char path",
				),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary directories
			srcDir := t.TempDir()
			extractDir := filepath.Join(t.TempDir(), "extract")
			archiveFile := filepath.Join(t.TempDir(), "archive.tar.gz")

			// Create source files
			for path, content := range tt.files {
				fullPath := filepath.Join(srcDir, path)
				err := os.MkdirAll(filepath.Dir(fullPath), 0o755)
				require.NoError(t, err)
				err = os.WriteFile(fullPath, content, 0o644)
				require.NoError(t, err)
			}

			// Create archive
			_, err := CreateArchive(srcDir, archiveFile, tt.archiveType)
			require.NoError(t, err)

			// Verify archive was created
			info, err := os.Stat(archiveFile)
			require.NoError(t, err)
			require.Positive(t, info.Size(), "archive should not be empty")

			// Extract archive
			err = ExtractArchive(archiveFile, extractDir, tt.archiveType, false)
			require.NoError(t, err)

			// Verify extracted files match original
			for path, expectedContent := range tt.files {
				extractedPath := filepath.Join(extractDir, path)
				actualContent, err := os.ReadFile(extractedPath)
				require.NoError(t, err)
				require.Equal(t, expectedContent, actualContent, "file %s content mismatch", path)
			}

			// Test overwrite protection
			err = ExtractArchive(archiveFile, extractDir, tt.archiveType, false)
			require.Error(t, err)
			require.Contains(t, err.Error(), "already exists")

			// Test overwrite enabled
			err = ExtractArchive(archiveFile, extractDir, tt.archiveType, true)
			require.NoError(t, err)
		})
	}
}

func TestArchiveWithSymlinks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping symlink test in short mode")
	}

	srcDir := t.TempDir()
	extractDir := filepath.Join(t.TempDir(), "extract")
	archiveFile := filepath.Join(t.TempDir(), "archive.tar.gz")

	// Create a regular file
	targetFile := filepath.Join(srcDir, "target.txt")
	err := os.WriteFile(targetFile, []byte("target content"), 0o644)
	require.NoError(t, err)

	// Create a symlink
	symlinkPath := filepath.Join(srcDir, "link.txt")
	err = os.Symlink("target.txt", symlinkPath)
	if err != nil {
		t.Skip("symlinks not supported on this platform")
	}

	// Create archive
	_, err = CreateArchive(srcDir, archiveFile, ArchiveGzip)
	require.NoError(t, err)

	// Extract archive
	err = ExtractArchive(archiveFile, extractDir, ArchiveGzip, false)
	require.NoError(t, err)

	// Verify symlink was preserved
	extractedLink := filepath.Join(extractDir, "link.txt")
	info, err := os.Lstat(extractedLink)
	require.NoError(t, err)
	require.NotEqual(t, 0, info.Mode()&os.ModeSymlink, "extracted file should be a symlink")

	// Verify symlink target
	target, err := os.Readlink(extractedLink)
	require.NoError(t, err)
	require.Equal(t, "target.txt", target)
}

func TestArchiveErrCases(t *testing.T) {
	t.Run("non-existent source", func(t *testing.T) {
		_, err := CreateArchive("/non/existent/path", "archive.tar.gz", ArchiveGzip)
		require.Error(t, err)
	})

	t.Run("non-existent archive", func(t *testing.T) {
		err := ExtractArchive("/non/existent/archive.tar.gz", t.TempDir(), ArchiveGzip, false)
		require.Error(t, err)
	})

	t.Run("invalid archive type", func(t *testing.T) {
		srcDir := t.TempDir()
		_, err := CreateArchive(srcDir, "archive.tar.gz", ArchiveType(99))
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported archive type")
	})

	t.Run("corrupted archive", func(t *testing.T) {
		// Create a corrupted archive file
		archiveFile := filepath.Join(t.TempDir(), "corrupted.tar.gz")
		err := os.WriteFile(archiveFile, []byte("not a valid archive"), 0o644)
		require.NoError(t, err)

		err = ExtractArchive(archiveFile, t.TempDir(), ArchiveGzip, false)
		require.Error(t, err)
	})
}

func TestArchiveWithVeryLongPaths(t *testing.T) {
	// Test handling of paths that exceed tar format limits
	srcDir := t.TempDir()
	extractDir := filepath.Join(t.TempDir(), "extract")
	archiveFile := filepath.Join(t.TempDir(), "archive.tar.gz")

	// Create a path that's longer than 100 characters (POSIX tar limit)
	// This should trigger PAX header usage in the tar writer
	longPath := "very/long/path/that/exceeds/one/hundred/characters/limit/in/posix/tar/format/and/should/use/pax/headers/file.txt"
	require.Greater(t, len(longPath), 100, "test path should exceed 100 characters")

	fullPath := filepath.Join(srcDir, longPath)
	err := os.MkdirAll(filepath.Dir(fullPath), 0o755)
	require.NoError(t, err)
	err = os.WriteFile(fullPath, []byte("content"), 0o644)
	require.NoError(t, err)

	// This should work with PAX headers
	_, err = CreateArchive(srcDir, archiveFile, ArchiveGzip)
	require.NoError(t, err)

	// Extract and verify
	err = ExtractArchive(archiveFile, extractDir, ArchiveGzip, false)
	require.NoError(t, err)

	// Verify file exists at the long path
	extractedPath := filepath.Join(extractDir, longPath)
	content, err := os.ReadFile(extractedPath)
	require.NoError(t, err)
	require.Equal(t, []byte("content"), content)
}

// TestCopyDir tests the CopyDir function for Archive Format v2 support.
func TestCopyDir(t *testing.T) {
	t.Run("CopyEmptyDirectory", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := filepath.Join(t.TempDir(), "dst")

		err := CopyDir(srcDir, dstDir)
		require.NoError(t, err)

		// Verify destination exists
		info, err := os.Stat(dstDir)
		require.NoError(t, err)
		require.True(t, info.IsDir())
	})

	t.Run("CopyFilesAndSubdirectories", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := filepath.Join(t.TempDir(), "dst")

		// Create source structure
		files := map[string][]byte{
			"file1.txt":           []byte("content1"),
			"subdir/file2.txt":    []byte("content2"),
			"subdir/deep/file.db": []byte("database"),
		}

		for path, content := range files {
			fullPath := filepath.Join(srcDir, path)
			err := os.MkdirAll(filepath.Dir(fullPath), 0o755)
			require.NoError(t, err)
			err = os.WriteFile(fullPath, content, 0o644)
			require.NoError(t, err)
		}

		// Copy directory
		err := CopyDir(srcDir, dstDir)
		require.NoError(t, err)

		// Verify all files were copied
		for path, expectedContent := range files {
			fullPath := filepath.Join(dstDir, path)
			content, err := os.ReadFile(fullPath)
			require.NoError(t, err, "File %s should exist", path)
			require.Equal(t, expectedContent, content, "File %s content mismatch", path)
		}
	})

	t.Run("CopyPreservesPermissions", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := filepath.Join(t.TempDir(), "dst")

		// Create file with specific permissions
		srcFile := filepath.Join(srcDir, "executable.sh")
		err := os.WriteFile(srcFile, []byte("#!/bin/bash\necho hello"), 0o755)
		require.NoError(t, err)

		err = CopyDir(srcDir, dstDir)
		require.NoError(t, err)

		dstFile := filepath.Join(dstDir, "executable.sh")
		info, err := os.Stat(dstFile)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
	})

	t.Run("CopySymlinks", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := filepath.Join(t.TempDir(), "dst")

		// Create a file and symlink
		srcFile := filepath.Join(srcDir, "target.txt")
		err := os.WriteFile(srcFile, []byte("target content"), 0o644)
		require.NoError(t, err)

		srcLink := filepath.Join(srcDir, "link.txt")
		err = os.Symlink("target.txt", srcLink)
		require.NoError(t, err)

		err = CopyDir(srcDir, dstDir)
		require.NoError(t, err)

		// Verify symlink was copied
		dstLink := filepath.Join(dstDir, "link.txt")
		linkTarget, err := os.Readlink(dstLink)
		require.NoError(t, err)
		require.Equal(t, "target.txt", linkTarget)
	})

	t.Run("ErrorOnNonExistentSource", func(t *testing.T) {
		dstDir := filepath.Join(t.TempDir(), "dst")
		err := CopyDir("/nonexistent/path", dstDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "stat source")
	})

	t.Run("ErrorOnFileAsSource", func(t *testing.T) {
		srcFile := filepath.Join(t.TempDir(), "file.txt")
		err := os.WriteFile(srcFile, []byte("content"), 0o644)
		require.NoError(t, err)

		dstDir := filepath.Join(t.TempDir(), "dst")
		err = CopyDir(srcFile, dstDir)
		require.Error(t, err)
		require.Contains(t, err.Error(), "source is not a directory")
	})

	t.Run("CopyLargeFile", func(t *testing.T) {
		srcDir := t.TempDir()
		dstDir := filepath.Join(t.TempDir(), "dst")

		// Create a larger file (1MB)
		largeContent := bytes.Repeat([]byte("0123456789ABCDEF"), 65536) // 1MB
		srcFile := filepath.Join(srcDir, "large.bin")
		err := os.WriteFile(srcFile, largeContent, 0o644)
		require.NoError(t, err)

		err = CopyDir(srcDir, dstDir)
		require.NoError(t, err)

		// Verify content
		dstFile := filepath.Join(dstDir, "large.bin")
		content, err := os.ReadFile(dstFile)
		require.NoError(t, err)
		require.Len(t, content, len(largeContent))
		require.Equal(t, largeContent, content)
	})
}

// TestDetectArchiveType tests magic byte detection for all compression formats.
func TestDetectArchiveType(t *testing.T) {
	tests := []struct {
		name         string
		archiveType  ArchiveType
		expectedType ArchiveType
	}{
		{
			name:         "gzip compression",
			archiveType:  ArchiveGzip,
			expectedType: ArchiveGzip,
		},
		{
			name:         "snappy compression",
			archiveType:  ArchiveSnappy,
			expectedType: ArchiveSnappy,
		},
		{
			name:         "zstd compression",
			archiveType:  ArchiveZstd,
			expectedType: ArchiveZstd,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcDir := t.TempDir()
			archiveFile := filepath.Join(t.TempDir(), "test.archive")

			// Create a test file
			require.NoError(t, os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("test content"), 0o644))

			// Create archive with specific compression
			_, err := CreateArchive(srcDir, archiveFile, tt.archiveType)
			require.NoError(t, err)

			// Detect compression type from magic bytes
			detected, err := DetectArchiveType(archiveFile)
			require.NoError(t, err)
			require.Equal(t, tt.expectedType, detected, "detected compression type should match")
		})
	}

	t.Run("unknown format", func(t *testing.T) {
		// Create a file with random content
		randomFile := filepath.Join(t.TempDir(), "random.bin")
		require.NoError(t, os.WriteFile(randomFile, []byte("not a valid archive header"), 0o644))

		_, err := DetectArchiveType(randomFile)
		require.Error(t, err)
		require.Contains(t, err.Error(), "unknown archive format")
	})

	t.Run("non-existent file", func(t *testing.T) {
		_, err := DetectArchiveType("/non/existent/file.tar.gz")
		require.Error(t, err)
	})
}

// TestArchiveWithMetadata tests roundtrip with shard info metadata.
func TestArchiveWithMetadata(t *testing.T) {
	srcDir := t.TempDir()
	extractDir := filepath.Join(t.TempDir(), "extract")
	archiveFile := filepath.Join(t.TempDir(), "archive.tar.zst")

	// Create source files
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "data.txt"), []byte("test data"), 0o644))
	subDir := filepath.Join(srcDir, "subdir")
	require.NoError(t, os.MkdirAll(subDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(subDir, "nested.txt"), []byte("nested content"), 0o644))

	// Create archive with metadata
	shardInfo := &ShardInfo{
		ShardID:    "deadbeef",
		NodeID:     "cafebabe",
		RangeStart: "YWFh", // base64 of "aaa"
		RangeEnd:   "enp6", // base64 of "zzz"
		TableName:  "test_table",
	}

	opts := CreateArchiveOptions{
		ArchiveType: ArchiveZstd,
		Metadata: &ArchiveMetadata{
			// Note: FormatVersion, AntflyVersion, CreatedAt, and Compression
			// are always overwritten by the archive creator to reflect the actual state
			Shard: shardInfo,
		},
	}

	_, err := CreateArchiveWithOptions(srcDir, archiveFile, opts)
	require.NoError(t, err)

	// Extract archive with auto-detection
	result, err := ExtractArchiveWithResult(archiveFile, extractDir, false)
	require.NoError(t, err)

	// Verify metadata was extracted
	require.NotNil(t, result.Metadata, "metadata should be present")
	require.Equal(t, CurrentArchiveFormatVersion, result.Metadata.FormatVersion)
	require.NotEmpty(t, result.Metadata.AntflyVersion, "antfly version should be set")
	require.Equal(t, "zstd", result.Metadata.Compression)

	// Verify shard info
	require.NotNil(t, result.Metadata.Shard, "shard info should be present")
	require.Equal(t, "deadbeef", result.Metadata.Shard.ShardID)
	require.Equal(t, "cafebabe", result.Metadata.Shard.NodeID)
	require.Equal(t, "YWFh", result.Metadata.Shard.RangeStart)
	require.Equal(t, "enp6", result.Metadata.Shard.RangeEnd)
	require.Equal(t, "test_table", result.Metadata.Shard.TableName)

	// Verify files were extracted (metadata file should NOT be extracted)
	content, err := os.ReadFile(filepath.Join(extractDir, "data.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("test data"), content)

	content, err = os.ReadFile(filepath.Join(extractDir, "subdir", "nested.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("nested content"), content)

	// Verify metadata file was not extracted to disk
	_, err = os.Stat(filepath.Join(extractDir, MetadataFileName))
	require.True(t, os.IsNotExist(err), "metadata file should not be extracted to disk")
}

// TestArchiveAutoDetection tests extract without explicit type.
func TestArchiveAutoDetection(t *testing.T) {
	tests := []struct {
		name        string
		archiveType ArchiveType
	}{
		{"gzip auto-detection", ArchiveGzip},
		{"snappy auto-detection", ArchiveSnappy},
		{"zstd auto-detection", ArchiveZstd},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srcDir := t.TempDir()
			extractDir := filepath.Join(t.TempDir(), "extract")
			archiveFile := filepath.Join(t.TempDir(), "archive")

			// Create test files
			files := map[string][]byte{
				"file1.txt":        []byte("content 1"),
				"dir/file2.txt":    []byte("content 2"),
				"dir/sub/file.bin": bytes.Repeat([]byte{0xAB, 0xCD}, 100),
			}

			for path, content := range files {
				fullPath := filepath.Join(srcDir, path)
				require.NoError(t, os.MkdirAll(filepath.Dir(fullPath), 0o755))
				require.NoError(t, os.WriteFile(fullPath, content, 0o644))
			}

			// Create archive with specific compression
			_, err := CreateArchive(srcDir, archiveFile, tt.archiveType)
			require.NoError(t, err)

			// Extract with auto-detection (using ExtractArchiveWithResult)
			result, err := ExtractArchiveWithResult(archiveFile, extractDir, false)
			require.NoError(t, err)

			// Verify metadata includes correct compression type
			require.NotNil(t, result.Metadata, "metadata should be present")
			require.Equal(t, tt.archiveType.String(), result.Metadata.Compression)

			// Verify all files were extracted correctly
			for path, expectedContent := range files {
				extractedPath := filepath.Join(extractDir, path)
				actualContent, err := os.ReadFile(extractedPath)
				require.NoError(t, err, "file %s should exist", path)
				require.Equal(t, expectedContent, actualContent, "file %s content mismatch", path)
			}
		})
	}
}

// TestNewShardInfo tests the ShardInfo constructor.
func TestNewShardInfo(t *testing.T) {
	shardID := types.ID(0xDEADBEEF)
	nodeID := types.ID(0xCAFEBABE)
	keyRange := types.Range{[]byte("start"), []byte("end")}
	tableName := "my_table"

	info := NewShardInfo(shardID, nodeID, keyRange, tableName)

	require.Equal(t, "deadbeef", info.ShardID)
	require.Equal(t, "cafebabe", info.NodeID)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("start")), info.RangeStart)
	require.Equal(t, base64.StdEncoding.EncodeToString([]byte("end")), info.RangeEnd)
	require.Equal(t, tableName, info.TableName)
}

// TestArchiveTypeString tests the ArchiveType.String() method.
func TestArchiveTypeString(t *testing.T) {
	require.Equal(t, "gzip", ArchiveGzip.String())
	require.Equal(t, "snappy", ArchiveSnappy.String())
	require.Equal(t, "zstd", ArchiveZstd.String())
	require.Equal(t, "unknown", ArchiveType(99).String())
}

// TestArchiveMetadataAutoPopulation tests that metadata is auto-populated.
func TestArchiveMetadataAutoPopulation(t *testing.T) {
	srcDir := t.TempDir()
	extractDir := filepath.Join(t.TempDir(), "extract")
	archiveFile := filepath.Join(t.TempDir(), "archive.tar.zst")

	// Create a test file
	require.NoError(t, os.WriteFile(filepath.Join(srcDir, "test.txt"), []byte("test"), 0o644))

	// Create archive with minimal options (no explicit metadata)
	opts := CreateArchiveOptions{
		ArchiveType: ArchiveZstd,
		// No Metadata - should auto-populate
	}

	_, err := CreateArchiveWithOptions(srcDir, archiveFile, opts)
	require.NoError(t, err)

	// Extract and verify auto-populated metadata
	result, err := ExtractArchiveWithResult(archiveFile, extractDir, false)
	require.NoError(t, err)

	require.NotNil(t, result.Metadata)
	require.Equal(t, CurrentArchiveFormatVersion, result.Metadata.FormatVersion)
	require.NotEmpty(t, result.Metadata.AntflyVersion)
	require.NotEmpty(t, result.Metadata.CreatedAt)
	require.Equal(t, "zstd", result.Metadata.Compression)
	require.Nil(t, result.Metadata.Shard, "shard info should be nil when not provided")
}

// TestArchiveFormatV2Structure tests that archives with Archive Format v2 structure
// (pebble/ and indexes/ subdirectories) work correctly.
func TestArchiveFormatV2Structure(t *testing.T) {
	srcDir := t.TempDir()

	// Create v2 structure with pebble/ and indexes/ subdirectories
	pebbleDir := filepath.Join(srcDir, "pebble")
	indexesDir := filepath.Join(srcDir, "indexes")
	require.NoError(t, os.MkdirAll(pebbleDir, 0o755))
	require.NoError(t, os.MkdirAll(indexesDir, 0o755))

	// Add pebble files
	require.NoError(t, os.WriteFile(filepath.Join(pebbleDir, "MANIFEST-000001"), []byte("manifest"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(pebbleDir, "000001.sst"), []byte("sst data"), 0o644))

	// Add index files
	fullTextDir := filepath.Join(indexesDir, "full_text_v0")
	require.NoError(t, os.MkdirAll(fullTextDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(fullTextDir, "index.bleve"), []byte("bleve data"), 0o644))

	aknnDir := filepath.Join(indexesDir, "aknn_v0")
	require.NoError(t, os.MkdirAll(aknnDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(aknnDir, "hnsw.bin"), []byte("hnsw data"), 0o644))

	// Create archive
	archiveFile := filepath.Join(t.TempDir(), "archive.tar.zst")
	_, err := CreateArchive(srcDir, archiveFile, ArchiveZstd)
	require.NoError(t, err)

	// Extract and verify (use subdirectory that doesn't exist yet)
	extractDir := filepath.Join(t.TempDir(), "extracted")
	err = ExtractArchive(archiveFile, extractDir, ArchiveZstd, false)
	require.NoError(t, err)

	// Verify pebble/ subdirectory detection works
	pebbleSubdir := filepath.Join(extractDir, "pebble")
	info, err := os.Stat(pebbleSubdir)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "pebble/ should be a directory")

	// Verify indexes/ subdirectory
	indexesSubdir := filepath.Join(extractDir, "indexes")
	info, err = os.Stat(indexesSubdir)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "indexes/ should be a directory")

	// Verify files exist
	content, err := os.ReadFile(filepath.Join(pebbleSubdir, "MANIFEST-000001"))
	require.NoError(t, err)
	require.Equal(t, []byte("manifest"), content)

	content, err = os.ReadFile(filepath.Join(indexesSubdir, "full_text_v0", "index.bleve"))
	require.NoError(t, err)
	require.Equal(t, []byte("bleve data"), content)

	content, err = os.ReadFile(filepath.Join(indexesSubdir, "aknn_v0", "hnsw.bin"))
	require.NoError(t, err)
	require.Equal(t, []byte("hnsw data"), content)
}
