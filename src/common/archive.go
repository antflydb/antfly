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
	"archive/tar"
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/antflydb/antfly/lib/pebbleutils"
	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/lib/utils"
	"github.com/cockroachdb/pebble/v2"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap"
)

// ArchiveType represents the compression type to use
type ArchiveType int

const (
	ArchiveGzip ArchiveType = iota
	ArchiveSnappy
	ArchiveZstd
)

const (
	// CurrentArchiveFormatVersion is the current archive format version.
	// v3 includes embedded metadata with antfly version, timestamps, and shard info.
	CurrentArchiveFormatVersion = 3

	// MetadataFileName is the name of the metadata file embedded in archives.
	MetadataFileName = "__antfly_metadata__.json"
)

// Magic bytes for compression format detection
var (
	magicGzip   = []byte{0x1f, 0x8b}
	magicZstd   = []byte{0x28, 0xb5, 0x2f, 0xfd}
	magicSnappy = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59} // Framed format
)

// ArchiveMetadata contains metadata about an Antfly archive.
type ArchiveMetadata struct {
	// FormatVersion is the archive format version (increment when format changes).
	FormatVersion int `json:"format_version"`

	// AntflyVersion is the Antfly binary version that created this archive.
	AntflyVersion string `json:"antfly_version"`

	// CreatedAt is the creation timestamp in RFC3339 format.
	CreatedAt string `json:"created_at"`

	// Compression is the compression type used ("gzip", "snappy", "zstd").
	Compression string `json:"compression"`

	// Shard contains shard-specific metadata (optional, nil for non-shard archives).
	Shard *ShardInfo `json:"shard,omitempty"`

	// Split contains split-specific metadata for split-child archives.
	Split *SplitMetadata `json:"split,omitempty"`
}

// ShardInfo contains shard-specific metadata for archive identification.
type ShardInfo struct {
	// ShardID is the hex-encoded shard ID.
	ShardID string `json:"shard_id"`

	// NodeID is the hex-encoded node ID.
	NodeID string `json:"node_id"`

	// RangeStart is the base64-encoded start key of the shard range.
	RangeStart string `json:"range_start"`

	// RangeEnd is the base64-encoded end key of the shard range.
	RangeEnd string `json:"range_end"`

	// TableName is the name of the table this shard belongs to (optional).
	TableName string `json:"table_name,omitempty"`
}

// SplitMetadata contains split replay information embedded in split-child archives.
type SplitMetadata struct {
	// ParentShardID is the hex-encoded parent shard ID that produced this archive.
	ParentShardID string `json:"parent_shard_id"`

	// ReplayFenceSeq is the parent-side split delta sequence included in the archive.
	// The child must replay deltas with sequence > ReplayFenceSeq before cutover.
	ReplayFenceSeq uint64 `json:"replay_fence_seq"`

	// SplitKey is the base64-encoded split key for debugging and validation.
	SplitKey string `json:"split_key,omitempty"`
}

// CreateArchiveOptions configures archive creation.
type CreateArchiveOptions struct {
	// ArchiveType specifies the compression format to use.
	ArchiveType ArchiveType

	// Metadata is optional metadata to embed in the archive.
	// If nil, metadata will still be created with basic info (version, timestamp).
	Metadata *ArchiveMetadata
}

// ExtractArchiveResult contains the result of archive extraction.
type ExtractArchiveResult struct {
	// Metadata is the metadata extracted from the archive, or nil if not present.
	Metadata *ArchiveMetadata
}

// NewShardInfo creates a ShardInfo from types.
func NewShardInfo(shardID, nodeID types.ID, r types.Range, tableName string) *ShardInfo {
	return &ShardInfo{
		ShardID:    shardID.String(), // ID.String() returns hex format
		NodeID:     nodeID.String(),  // ID.String() returns hex format
		RangeStart: base64.StdEncoding.EncodeToString(r[0]),
		RangeEnd:   base64.StdEncoding.EncodeToString(r[1]),
		TableName:  tableName,
	}
}

// compressionName returns the string name for an ArchiveType.
func (t ArchiveType) String() string {
	switch t {
	case ArchiveGzip:
		return "gzip"
	case ArchiveSnappy:
		return "snappy"
	case ArchiveZstd:
		return "zstd"
	default:
		return "unknown"
	}
}

// DetectArchiveType reads magic bytes from a file to determine compression type.
func DetectArchiveType(filePath string) (ArchiveType, error) {
	f, err := os.Open(filepath.Clean(filePath))
	if err != nil {
		return 0, fmt.Errorf("opening file: %w", err)
	}
	defer func() { _ = f.Close() }()
	return DetectArchiveTypeFromReader(f)
}

// DetectArchiveTypeFromReader reads magic bytes from a reader to determine compression type.
func DetectArchiveTypeFromReader(r io.Reader) (ArchiveType, error) {
	// Read enough bytes to detect all formats (snappy needs 10 bytes)
	header := make([]byte, 10)
	n, err := io.ReadFull(r, header)
	if err != nil && err != io.ErrUnexpectedEOF {
		return 0, fmt.Errorf("reading header: %w", err)
	}
	header = header[:n]

	// Check in order of specificity (longer prefixes first)
	if len(header) >= len(magicSnappy) && bytes.HasPrefix(header, magicSnappy) {
		return ArchiveSnappy, nil
	}
	if len(header) >= len(magicZstd) && bytes.HasPrefix(header, magicZstd) {
		return ArchiveZstd, nil
	}
	if len(header) >= len(magicGzip) && bytes.HasPrefix(header, magicGzip) {
		return ArchiveGzip, nil
	}

	return 0, fmt.Errorf("unknown archive format: header %x", header)
}

// KeyFilter is a function that returns true if a key should be skipped
// during split key selection (e.g., metadata keys).
type KeyFilter func(key []byte) bool

func FindSplitKeyByFileSize(lg *zap.Logger, db *pebble.DB, r types.Range, skipKey KeyFilter) (key []byte, err error) {
	defer pebbleutils.RecoverPebbleClosed(&err)
	// Get SSTable information
	if err := db.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush database: %w", err)
	}
	tables, err := db.SSTables()
	if err != nil {
		return nil, fmt.Errorf("failed to get SSTable info: %w", err)
	}

	// Calculate total file size for tables overlapping the range
	var totalSize uint64
	var overlappingTables []pebble.TableInfo // Flattened list of overlapping tables

	var smallestKey, largestKey []byte
	rangeEnd := r.EndForPebble() // Use helper for Pebble-compatible End (empty = unbounded = 0xFF)
	for i, level := range tables {
		for j, table := range level {
			if i == 0 && j == 0 {
				smallestKey = table.Smallest.UserKey
			}
			// Check for overlap: table.Largest >= r[0] && table.Smallest < r[1]
			// Use >= because the range start is inclusive: if table.Largest == r[0], the table
			// contains the boundary key which IS within the range [r[0], r[1])
			// Empty table.Largest.UserKey means unbounded (+infinity), which is always >= r[0]
			tableLargestGtRangeStart := len(table.Largest.UserKey) == 0 || bytes.Compare(table.Largest.UserKey, r[0]) >= 0
			if tableLargestGtRangeStart &&
				bytes.Compare(table.Smallest.UserKey, rangeEnd) < 0 {
				overlappingTables = append(overlappingTables, table.TableInfo)
			}
			if j == len(level)-1 && i == len(tables)-1 {
				largestKey = table.Largest.UserKey
			}
		}
	}

	if len(overlappingTables) == 0 {
		return nil, fmt.Errorf(
			"no overlapping tables found for range: %s got range on disk: %s",
			r,
			types.Range{smallestKey, largestKey},
		)
	}

	// Sort overlapping tables by Smallest UserKey
	sort.Slice(overlappingTables, func(i, j int) bool {
		return bytes.Compare(
			overlappingTables[i].Smallest.UserKey,
			overlappingTables[j].Smallest.UserKey,
		) < 0
	})
	lg.Debug("Sorted overlapping tables", zap.String("range", r.String()), zap.Int("table_count", len(overlappingTables)))
	for _, table := range overlappingTables {
		lg.Debug("Table info",
			zap.ByteString("smallest", table.Smallest.UserKey),
			zap.ByteString("largest", table.Largest.UserKey),
			zap.Uint64("size", table.Size),
		)
	}

	// Calculate total size based *only* on the sorted, overlapping tables
	for _, table := range overlappingTables {
		totalSize += table.Size
	}
	lg.Debug("Calculated total overlapping size", zap.Uint64("total_size", totalSize))

	// Find the key that divides the total size approximately in half
	var runningSize uint64
	halfSize := totalSize / 2
	lg.Debug("Calculated half size", zap.Uint64("half_size", halfSize))
	var candidateKey []byte

	// isValidSplitKey checks if a key is valid for splitting:
	// - within range (r[0], r[1])
	// - not a key that should be skipped (e.g., metadata keys)
	isValidSplitKey := func(key []byte) bool {
		return bytes.Compare(key, r[0]) > 0 &&
			bytes.Compare(key, rangeEnd) < 0 &&
			(skipKey == nil || !skipKey(key))
	}

	// Iterate through the sorted overlapping tables to find the split point
	lg.Debug("Finding midpoint table")
	midpointTableIndex := -1
	for i, table := range overlappingTables {
		// Check if adding this table crosses the midpoint
		if runningSize < halfSize && runningSize+table.Size >= halfSize {
			// This table is the first one that makes the cumulative size reach/exceed half.
			lg.Debug("Midpoint table found",
				zap.Int("index", i),
				zap.ByteString("smallest_key", table.Smallest.UserKey))
			midpointTableIndex = i
			break // Found the table containing the size midpoint
		}
		runningSize += table.Size
	}
	if midpointTableIndex == -1 {
		lg.Debug("Midpoint table not found")
	}

	if midpointTableIndex != -1 {
		lg.Debug("Checking midpoint table key")
		// Try the Smallest key of the table that crossed the midpoint
		potentialKey := overlappingTables[midpointTableIndex].Smallest.UserKey
		lg.Debug("Potential key from midpoint table", zap.String("key", types.FormatKey(potentialKey)))

		if isValidSplitKey(potentialKey) {
			lg.Debug("Midpoint key is valid", zap.String("key", types.FormatKey(potentialKey)))
			candidateKey = potentialKey
		} else {
			lg.Debug("Midpoint key is invalid, checking next table",
				zap.String("key", types.FormatKey(potentialKey)),
				zap.String("range_start", types.FormatKey(r[0])),
				zap.ByteString("range_end", r[1]))
			// If the midpoint table's key is invalid (e.g., == r[0] or metadata key),
			// try the *next* table's start key, if one exists.
			if midpointTableIndex+1 < len(overlappingTables) {
				lg.Debug("Checking next table key")
				nextTableKey := overlappingTables[midpointTableIndex+1].Smallest.UserKey
				lg.Debug("Potential key from next table", zap.String("key", types.FormatKey(nextTableKey)))
				// Check if the next table's key is valid
				if isValidSplitKey(nextTableKey) {
					lg.Debug("Next table key is valid", zap.String("key", types.FormatKey(nextTableKey)))
					candidateKey = nextTableKey
				} else {
					lg.Debug("Next table key is invalid", zap.String("key", types.FormatKey(nextTableKey)))
				}
			} else {
				lg.Debug("No next table to check")
			}
		}
	}

	// If no candidate key found yet (e.g., midpoint logic failed, or keys were invalid), try fallback.
	if candidateKey == nil && len(overlappingTables) > 0 {
		lg.Debug("Midpoint logic failed or key invalid, trying fallback")
		// Fallback: Use the start key of the middle table in the sorted list.
		middleFallbackIndex := len(overlappingTables) / 2
		middleTable := overlappingTables[middleFallbackIndex]
		potentialKey := middleTable.Smallest.UserKey
		lg.Debug("Fallback: checking middle table key",
			zap.Int("index", middleFallbackIndex),
			zap.String("key", types.FormatKey(potentialKey)))
		// Ensure fallback key is strictly within the range and not a metadata key
		if isValidSplitKey(potentialKey) {
			lg.Debug("Fallback: middle table key is valid", zap.String("key", types.FormatKey(potentialKey)))
			candidateKey = potentialKey
		} else {
			lg.Debug("Fallback: middle table key is invalid, checking first table",
				zap.String("key", types.FormatKey(potentialKey)))
			// If middle didn't work, try the first table's key as a last resort, if valid
			firstTableKey := overlappingTables[0].Smallest.UserKey
			lg.Debug("Fallback: checking first table key", zap.String("key", types.FormatKey(firstTableKey)))
			if isValidSplitKey(firstTableKey) {
				lg.Debug("Fallback: first table key is valid", zap.String("key", types.FormatKey(firstTableKey)))
				candidateKey = firstTableKey
			} else {
				lg.Debug("Fallback: first table key is invalid", zap.String("key", types.FormatKey(firstTableKey)))
			}
		}
	}

	// Iterator-based fallback: scan the database to find a key in the middle of the range
	// This handles cases where SST table boundaries are all metadata keys
	if candidateKey == nil {
		lg.Debug("SST table approach failed, using iterator fallback")

		// Collect valid split keys within the range (strictly greater than r[0])
		iter, err := db.NewIter(&pebble.IterOptions{
			LowerBound: r[0],
			UpperBound: r[1],
		})
		if err != nil {
			lg.Error("Failed to create iterator for split key fallback", zap.Error(err))
		} else {
			defer func() { _ = iter.Close() }()

			// Collect keys that are valid split candidates (strictly > r[0] and not skipped)
			var validKeys [][]byte
			const maxKeysToCollect = 10000 // Limit to avoid memory issues
			for iter.First(); iter.Valid() && len(validKeys) < maxKeysToCollect; iter.Next() {
				key := iter.Key()
				// Skip the range start key - we need keys strictly greater than r[0]
				if bytes.Equal(key, r[0]) {
					continue
				}
				if skipKey == nil || !skipKey(key) {
					// Make a copy since iter.Key() is only valid until next iteration
					keyCopy := make([]byte, len(key))
					copy(keyCopy, key)
					validKeys = append(validKeys, keyCopy)
				}
			}
			if err := iter.Error(); err != nil {
				lg.Error("Iterator error during split key fallback", zap.Error(err))
			} else if len(validKeys) >= 1 {
				// Pick the middle key from valid candidates
				midIndex := len(validKeys) / 2
				candidateKey = validKeys[midIndex]
				lg.Debug("Iterator fallback found split key",
					zap.Int("total_keys", len(validKeys)),
					zap.Int("mid_index", midIndex),
					zap.ByteString("split_key", candidateKey))
			} else {
				lg.Debug("Iterator fallback found no valid split keys (shard may have only one document)")
			}
		}
	}

	// Final check and return
	if candidateKey == nil {
		// This is expected when a shard contains only one document - log at debug level
		lg.Debug("Couldn't find suitable split key within the range (shard may be at minimum size)", zap.String("range", r.String()))
		return nil, fmt.Errorf("couldn't find suitable split key within the range %s", r)
	}

	// Ensure the final candidate key is strictly greater than the start of the range,
	// otherwise, the split might not be effective.
	// This check should ideally be redundant given the checks above, but serves as a safeguard.
	if bytes.Compare(candidateKey, r[0]) <= 0 {
		lg.Error("Calculated split key is not greater than the range start",
			zap.ByteString("split_key", candidateKey),
			zap.ByteString("range_start", r[0]))
		return nil, fmt.Errorf(
			"calculated split key %s is not greater than the range start %s",
			candidateKey,
			r[0],
		)
	}
	// Only validate against range end if it's bounded (non-empty)
	// Empty r[1] means unbounded (+infinity), so any key is valid
	if len(r[1]) > 0 && bytes.Compare(candidateKey, r[1]) >= 0 {
		lg.Error("Calculated split key is not less than the range end",
			zap.ByteString("split_key", candidateKey),
			zap.ByteString("range_end", r[1]))
		return nil, fmt.Errorf(
			"calculated split key %s is not less than the range end %s",
			candidateKey,
			r[1],
		)
	}

	lg.Debug("Successfully found split key",
		zap.ByteString("split_key", candidateKey),
		zap.String("range", r.String()))
	return candidateKey, nil
}

func GetDirectorySize(path string) (uint64, error) {
	var size uint64
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			s := info.Size()
			if s < 0 {
				return fmt.Errorf("unexpected negative file size %d", s)
			}
			size += uint64(s)
		}
		return nil
	})
	return size, err
}

// writeMetadataEntry writes the archive metadata as the first entry in a tar archive.
func writeMetadataEntry(tw *tar.Writer, opts CreateArchiveOptions) error {
	// Build metadata, using provided or creating default
	metadata := opts.Metadata
	if metadata == nil {
		metadata = &ArchiveMetadata{}
	}

	// Always set these fields from current state
	metadata.FormatVersion = CurrentArchiveFormatVersion
	metadata.AntflyVersion = utils.GetVersion()
	metadata.CreatedAt = time.Now().UTC().Format(time.RFC3339)
	metadata.Compression = opts.ArchiveType.String()

	// Serialize metadata to JSON
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}

	// Write metadata file as first entry
	header := &tar.Header{
		Name:    MetadataFileName,
		Mode:    0o644,
		Size:    int64(len(data)),
		ModTime: time.Now(),
	}
	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("writing metadata header: %w", err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("writing metadata content: %w", err)
	}

	return nil
}

// CreateArchive archives a directory using tar with the specified compression.
// This is a convenience wrapper that calls CreateArchiveWithOptions.
func CreateArchive(
	sourceDir string,
	destFile string,
	archiveType ArchiveType,
) (os.FileInfo, error) {
	return CreateArchiveWithOptions(sourceDir, destFile, CreateArchiveOptions{
		ArchiveType: archiveType,
	})
}

// CreateArchiveWithOptions archives a directory using tar with the specified options.
// It embeds metadata as the first entry in the archive.
func CreateArchiveWithOptions(
	sourceDir string,
	destFile string,
	opts CreateArchiveOptions,
) (os.FileInfo, error) {
	// Create the target file
	file, err := os.Create(filepath.Clean(destFile))
	if err != nil {
		return nil, fmt.Errorf("failed to create archive file: %w", err)
	}
	removeFile := true
	defer func() {
		_ = file.Close()
		if removeFile {
			_ = os.Remove(destFile)
		}
	}()

	// Set up the compression and writer chain
	var compression io.WriteCloser

	switch opts.ArchiveType {
	case ArchiveGzip:
		// For gzip compression, use the fastest level for performance.
		gw, err := gzip.NewWriterLevel(file, gzip.BestSpeed)
		if err != nil {
			// This should not happen with a valid constant.
			return nil, fmt.Errorf("creating gzip writer: %w", err)
		}
		compression = gw
	case ArchiveSnappy:
		// For snappy compression
		compression = snappy.NewBufferedWriter(file)
	case ArchiveZstd:
		// For zstd compression, use speed level 3 (fast)
		zw, err := zstd.NewWriter(file, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return nil, fmt.Errorf("creating zstd writer: %w", err)
		}
		compression = zw
	default:
		return nil, errors.New("unsupported archive type")
	}

	// Create a new tar writer
	tw := tar.NewWriter(compression)

	// Write metadata as the first entry
	if err := writeMetadataEntry(tw, opts); err != nil {
		return nil, fmt.Errorf("writing metadata: %w", err)
	}

	// Create a buffer to improve file copy performance.
	buf := make([]byte, 64*1024*1024) // 64MB buffer

	// Walk through the source directory
	err = filepath.Walk(sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get the relative path for the file header
		relPath, err := filepath.Rel(sourceDir, path)
		if err != nil {
			return fmt.Errorf("failed to get relative path: %w", err)
		}

		// Skip the root directory
		if relPath == "." {
			return nil
		}

		var link string
		if info.Mode()&os.ModeSymlink == os.ModeSymlink {
			if link, err = os.Readlink(path); err != nil {
				return fmt.Errorf("failed to read symlink %s: %w", path, err)
			}
		}

		// Create a new tar header
		header, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return fmt.Errorf("failed to create tar header: %w", err)
		}

		// Update the name to correctly reflect the path inside the tar
		header.Name = filepath.ToSlash(relPath)

		// For long paths, the tar writer will automatically use PAX headers
		// when Format is not specified (defaults to FormatUnknown which auto-detects)
		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("failed to write tar header for %s: %w", relPath, err)
		}

		if !info.Mode().IsRegular() { // nothing more to do for non-regular
			return nil
		}

		if info.IsDir() {
			return nil
		}
		srcFile, err := os.Open(filepath.Clean(path)) //nolint:gosec // G122: path from filepath.Walk within trusted base directory
		if err != nil {
			return fmt.Errorf("failed to open file %s: %w", path, err)
		}
		defer func() {
			_ = srcFile.Close()
		}()

		_, err = io.CopyBuffer(tw, srcFile, buf)
		if err != nil {
			return fmt.Errorf("failed to copy file content %s: %w", path, err)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking directory: %w", err)
	}

	// Close the tar writer to flush the tar trailer
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("closing tar writer: %w", err)
	}

	// Close the compression writer to flush compressed data
	if err := compression.Close(); err != nil {
		return nil, fmt.Errorf("closing compression writer: %w", err)
	}

	// Sync the file to ensure all data is written to disk
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("syncing file: %w", err)
	}

	removeFile = false
	return file.Stat()
}

// ExtractArchive extracts a compressed tar archive to a destination directory.
// It auto-detects the compression format using magic bytes.
// This is a convenience wrapper that calls ExtractArchiveWithResult and discards the result.
func ExtractArchive(
	archiveFile string,
	destDir string,
	archiveType ArchiveType,
	overwrite bool,
) error {
	_, err := ExtractArchiveWithResult(archiveFile, destDir, overwrite)
	return err
}

// ExtractArchiveWithResult extracts a compressed tar archive to a destination directory.
// It auto-detects the compression format using magic bytes and returns metadata if present.
func ExtractArchiveWithResult(
	archiveFile string,
	destDir string,
	overwrite bool,
) (*ExtractArchiveResult, error) {
	// Check if destination already exists
	if _, err := os.Stat(destDir); err == nil {
		// Directory exists
		if !overwrite {
			return nil, fmt.Errorf(
				"destination directory %s already exists and overwrite is not enabled",
				destDir,
			)
		}

		// If overwrite is enabled, remove the existing directory
		if err := os.RemoveAll(destDir); err != nil {
			return nil, fmt.Errorf("failed to remove existing directory: %w", err)
		}
	} else if !os.IsNotExist(err) {
		// Some other error occurred
		return nil, fmt.Errorf("failed to check destination directory: %w", err)
	}

	// Detect compression type
	archiveType, err := DetectArchiveType(archiveFile)
	if err != nil {
		return nil, fmt.Errorf("detecting archive type: %w", err)
	}

	// Open the archive file
	file, err := os.Open(filepath.Clean(archiveFile))
	if err != nil {
		return nil, fmt.Errorf("failed to open archive: %w", err)
	}
	defer func() {
		_ = file.Close()
	}()

	// Set up the decompression reader
	var decompression io.Reader

	switch archiveType {
	case ArchiveGzip:
		// For gzip decompression
		gr, err := gzip.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer func() {
			_ = gr.Close()
		}()
		decompression = gr
	case ArchiveSnappy:
		// For snappy decompression
		decompression = snappy.NewReader(file)
	case ArchiveZstd:
		// For zstd decompression
		zr, err := zstd.NewReader(file)
		if err != nil {
			return nil, fmt.Errorf("failed to create zstd reader: %w", err)
		}
		defer zr.Close()
		decompression = zr
	default:
		return nil, errors.New("unsupported archive type")
	}

	// Create a new tar reader
	tr := tar.NewReader(decompression)

	// Ensure the destination directory exists
	if err := os.MkdirAll(destDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return nil, fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Create a buffer to improve file copy performance.
	buf := make([]byte, 64*1024*1024) // 64MB buffer

	result := &ExtractArchiveResult{}

	// Process each file in the tarball
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar header: %w", err)
		}

		// Check if this is the metadata file
		if header.Name == MetadataFileName {
			metadata, err := readMetadataEntry(tr, header.Size)
			if err != nil {
				return nil, fmt.Errorf("reading metadata: %w", err)
			}
			result.Metadata = metadata
			continue // Don't extract the metadata file to disk
		}

		// Check for ZipSlip vulnerability
		if strings.Contains(destDir, "..") {
			return nil, fmt.Errorf("illegal file path: %s", destDir)
		}
		target := filepath.Join(filepath.Clean(destDir), filepath.Clean(header.Name))
		if !strings.HasPrefix(target, filepath.Clean(destDir)+string(os.PathSeparator)) {
			return nil, fmt.Errorf("illegal file path: %s", target)
		}

		// Handle based on the entry type
		switch header.Typeflag {
		case tar.TypeDir:
			// Create directory
			if err := os.MkdirAll(target, os.ModePerm); err != nil { //nolint:gosec // G703: internal path with traversal protection; G301: standard permissions for data directory
				return nil, fmt.Errorf("failed to create directory: %w", err)
			}
		case tar.TypeReg:
			// Create file
			fileDir := filepath.Dir(target)
			if err := os.MkdirAll(fileDir, os.ModePerm); err != nil { //nolint:gosec // G703: internal path with traversal protection; G301: standard permissions for data directory
				return nil, fmt.Errorf("failed to create parent directory: %w", err)
			}

			outFile, err := os.Create(filepath.Clean(target)) //nolint:gosec // G703: internal path with traversal protection
			if err != nil {
				return nil, fmt.Errorf("failed to create file: %w", err)
			}
			defer func() { _ = outFile.Close() }()

			// To prevent decompression bombs, we'll read up to a certain limit.
			limit := int64(1_000_000_000_000) // 1TB limit
			limitedReader := io.LimitReader(tr, limit)

			// Copy with a buffer for performance.
			written, err := io.CopyBuffer(outFile, limitedReader, buf)
			if err != nil {
				return nil, fmt.Errorf(
					"failed to write file contents: %w, expected bytes: %d got bytes: %d ",
					err,
					header.Size,
					written,
				)
			}

			// If we wrote up to the limit, we need to check if the file was truncated.
			if written == limit {
				// Try to read one more byte; if successful, the file is too large.
				n, _ := tr.Read(make([]byte, 1))
				if n > 0 {
					return nil, fmt.Errorf("file %s exceeds maximum allowed size of 1TB", target)
				}
			}

			mode := header.Mode
			if mode < 0 {
				return nil, fmt.Errorf("header has negative size %d", header.Mode)
			}
			if mode > math.MaxUint32 {
				return nil, fmt.Errorf("header has too large size %d", header.Mode)
			}
			// Set file permissions
			if err := os.Chmod(target, os.FileMode(mode)); err != nil { //nolint:gosec // G703: internal path with traversal protection
				return nil, fmt.Errorf("failed to set file permissions: %w", err)
			}
		case tar.TypeSymlink:
			// Create parent directory if needed
			parentDir := filepath.Dir(target)
			if err := os.MkdirAll(parentDir, os.ModePerm); err != nil { //nolint:gosec // G703: internal path with traversal protection; G301: standard permissions for data directory
				return nil, fmt.Errorf("failed to create parent directory for symlink: %w", err)
			}

			// Create the symlink
			if err := os.Symlink(header.Linkname, target); err != nil {
				return nil, fmt.Errorf("failed to create symlink: %w", err)
			}
		default:
			return nil, fmt.Errorf("unsupported file type: %d for %s", header.Typeflag, target)
		}
	}

	return result, nil
}

// readMetadataEntry reads and parses the archive metadata from a tar entry.
func readMetadataEntry(tr *tar.Reader, size int64) (*ArchiveMetadata, error) {
	// Limit metadata size to 1MB to prevent abuse
	if size > 1024*1024 {
		return nil, fmt.Errorf("metadata too large: %d bytes", size)
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(tr, data); err != nil {
		return nil, fmt.Errorf("reading metadata content: %w", err)
	}

	var metadata ArchiveMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("parsing metadata JSON: %w", err)
	}

	return &metadata, nil
}

// CopyDir recursively copies a directory from src to dst.
// The destination directory is created if it doesn't exist.
// Symlinks are copied as symlinks, not followed.
func CopyDir(src, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return fmt.Errorf("stat source: %w", err)
	}
	if !srcInfo.IsDir() {
		return fmt.Errorf("source is not a directory: %s", src)
	}

	// Create destination directory with same permissions
	if err := os.MkdirAll(dst, srcInfo.Mode()); err != nil {
		return fmt.Errorf("creating destination directory: %w", err)
	}

	// Walk the source directory
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Get relative path
		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return fmt.Errorf("getting relative path: %w", err)
		}
		if relPath == "." {
			return nil // Skip the root directory itself
		}

		dstPath := filepath.Join(dst, relPath)

		// Handle symlinks
		if info.Mode()&os.ModeSymlink != 0 {
			link, err := os.Readlink(path)
			if err != nil {
				return fmt.Errorf("reading symlink: %w", err)
			}
			return os.Symlink(link, dstPath) //nolint:gosec // G122: path from filepath.Walk within trusted base directory
		}

		// Handle directories
		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		// Handle regular files
		return copyFile(path, dstPath, info.Mode())
	})
}

// copyFile copies a single file from src to dst with the specified permissions.
func copyFile(src, dst string, mode os.FileMode) error {
	srcFile, err := os.Open(src) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return fmt.Errorf("opening source file: %w", err)
	}
	defer func() { _ = srcFile.Close() }()

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return fmt.Errorf("creating destination file: %w", err)
	}
	defer func() { _ = dstFile.Close() }()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return fmt.Errorf("copying file contents: %w", err)
	}

	if err := dstFile.Sync(); err != nil {
		return fmt.Errorf("syncing file: %w", err)
	}

	return nil
}
