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

package snapstore

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/antflydb/antfly/lib/types"
	"github.com/antflydb/antfly/src/common"
)

// SnapshotOptions configures snapshot creation with shard metadata.
type SnapshotOptions struct {
	// ShardID is the shard this snapshot belongs to.
	ShardID types.ID
	// NodeID is the node that created this snapshot.
	NodeID types.ID
	// Range is the key range of the shard.
	Range types.Range
	// TableName is the name of the table (optional).
	TableName string
}

// SnapStore provides an abstraction for storing and retrieving Raft snapshots.
// This allows the snapshot storage to be swapped out (e.g., local filesystem, S3, GCS)
// without changing the core Raft or storage logic.
type SnapStore interface {
	// Get returns a reader for the snapshot with the given ID.
	// Returns os.ErrNotExist if the snapshot doesn't exist.
	Get(ctx context.Context, snapID string) (io.ReadCloser, error)

	// Put stores a snapshot with the given ID from the reader.
	// The implementation should handle atomic writes (e.g., temp file + rename).
	Put(ctx context.Context, snapID string, r io.Reader) error

	// Delete removes a snapshot with the given ID.
	// Returns nil if the snapshot doesn't exist (idempotent).
	Delete(ctx context.Context, snapID string) error

	// Path returns the absolute file path for a snapshot ID.
	// This is primarily for backward compatibility with code that needs direct file access
	// (e.g., archive extraction). Returns an error for non-filesystem stores.
	Path(snapID string) (string, error)

	// Exists checks if a snapshot with the given ID exists.
	Exists(ctx context.Context, snapID string) (bool, error)

	// List returns all snapshot IDs in the store.
	// Useful for cleanup and debugging.
	List(ctx context.Context) ([]string, error)

	// RemoveAll removes the entire snapshot store and all its contents.
	// This is used for cleanup operations (e.g., on shard deletion or initialization failure).
	// Returns nil if the store doesn't exist (idempotent).
	RemoveAll(ctx context.Context) error

	// CreateSnapshot creates a snapshot archive from the source directory and stores it.
	// The sourceDir should contain the database files to snapshot.
	// opts contains shard metadata to embed in the archive (can be nil for basic archives).
	// Returns file size and any error encountered.
	CreateSnapshot(ctx context.Context, snapID string, sourceDir string, opts *SnapshotOptions) (int64, error)

	// ExtractSnapshot retrieves a snapshot and extracts it to the target directory.
	// The targetDir will be created if it doesn't exist.
	// If removeExisting is true, any existing content in targetDir will be removed first.
	// Returns the archive metadata if present.
	ExtractSnapshot(ctx context.Context, snapID string, targetDir string, removeExisting bool) (*common.ArchiveMetadata, error)
}

// LocalSnapStore implements SnapStore using the local filesystem.
// This is the default implementation that maintains backward compatibility
// with the existing snapshot directory structure.
type LocalSnapStore struct {
	snapDir string // Base directory for storing snapshots
}

// NewLocalSnapStore creates a new filesystem-based snapshot store.
// It constructs the snapshot directory path from the provided parameters and creates it if it doesn't exist.
func NewLocalSnapStore(dataDir string, shardID, nodeID types.ID) (*LocalSnapStore, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("dataDir cannot be empty")
	}
	if shardID == 0 {
		return nil, fmt.Errorf("shardID cannot be zero")
	}
	if nodeID == 0 {
		return nil, fmt.Errorf("nodeID cannot be zero")
	}

	snapDir := common.SnapDir(dataDir, shardID, nodeID)

	// Ensure the directory exists
	if err := os.MkdirAll(snapDir, os.ModePerm); err != nil { //nolint:gosec // G301: standard permissions for data directory
		return nil, fmt.Errorf("creating snapshot directory %s: %w", snapDir, err)
	}

	return &LocalSnapStore{
		snapDir: snapDir,
	}, nil
}

// Get returns a reader for the snapshot file.
func (s *LocalSnapStore) Get(ctx context.Context, snapID string) (io.ReadCloser, error) {
	if err := validateSnapID(snapID); err != nil {
		return nil, err
	}

	path := s.archiveFilePath(snapID)
	file, err := os.Open(path) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return nil, err
	}

	return file, nil
}

// Put stores a snapshot by copying from the reader to a file.
// Uses atomic write pattern: write to temp file, then rename.
func (s *LocalSnapStore) Put(ctx context.Context, snapID string, r io.Reader) error {
	if err := validateSnapID(snapID); err != nil {
		return err
	}

	finalPath := s.archiveFilePath(snapID)

	// Write to temporary file first for atomic operation
	tempPath := finalPath + ".tmp"
	defer func() { _ = os.Remove(tempPath) }() // Clean up temp file on error

	tempFile, err := os.Create(tempPath) //nolint:gosec // G304: internal file I/O, not user-controlled
	if err != nil {
		return fmt.Errorf("creating temp file %s: %w", tempPath, err)
	}

	// Copy data to temp file
	if _, err := io.Copy(tempFile, r); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("writing snapshot data: %w", err)
	}

	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return fmt.Errorf("syncing temp file: %w", err)
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}

	// Atomic rename
	if err := os.Rename(tempPath, finalPath); err != nil {
		return fmt.Errorf("renaming %s to %s: %w", tempPath, finalPath, err)
	}

	return nil
}

// Delete removes a snapshot file.
func (s *LocalSnapStore) Delete(ctx context.Context, snapID string) error {
	if err := validateSnapID(snapID); err != nil {
		return err
	}

	path := s.archiveFilePath(snapID)
	err := os.Remove(path)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting snapshot %s: %w", snapID, err)
	}

	return nil
}

// Path returns the absolute file path for a snapshot.
func (s *LocalSnapStore) Path(snapID string) (string, error) {
	if err := validateSnapID(snapID); err != nil {
		return "", err
	}
	return s.archiveFilePath(snapID), nil
}

// Exists checks if a snapshot file exists.
func (s *LocalSnapStore) Exists(ctx context.Context, snapID string) (bool, error) {
	if err := validateSnapID(snapID); err != nil {
		return false, err
	}

	path := s.archiveFilePath(snapID)
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// List returns all snapshot IDs in the directory.
func (s *LocalSnapStore) List(ctx context.Context) ([]string, error) {
	entries, err := os.ReadDir(s.snapDir)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("reading snapshot directory: %w", err)
	}

	snapIDs := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		// Extract snapshot ID from filename (remove .tar.zst extension)
		if len(name) > 8 && name[len(name)-8:] == ".tar.zst" {
			snapID := name[:len(name)-8]
			snapIDs = append(snapIDs, snapID)
		}
	}

	return snapIDs, nil
}

// RemoveAll removes the entire snapshot directory and all its contents.
func (s *LocalSnapStore) RemoveAll(ctx context.Context) error {
	err := os.RemoveAll(s.snapDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("removing snapshot directory %s: %w", s.snapDir, err)
	}
	return nil
}

// CreateSnapshot creates a snapshot archive from the source directory and stores it.
func (s *LocalSnapStore) CreateSnapshot(ctx context.Context, snapID string, sourceDir string, opts *SnapshotOptions) (int64, error) {
	if err := validateSnapID(snapID); err != nil {
		return 0, err
	}

	archiveFile := s.archiveFilePath(snapID)

	// Build archive options with metadata
	archiveOpts := common.CreateArchiveOptions{
		ArchiveType: common.ArchiveZstd,
	}

	// Add shard info if options provided
	if opts != nil {
		archiveOpts.Metadata = &common.ArchiveMetadata{
			Shard: common.NewShardInfo(opts.ShardID, opts.NodeID, opts.Range, opts.TableName),
		}
	}

	// Create the snapshot archive with metadata
	archiveInfo, err := common.CreateArchiveWithOptions(sourceDir, archiveFile, archiveOpts)
	if err != nil {
		return 0, fmt.Errorf("creating snapshot archive: %w", err)
	}

	return archiveInfo.Size(), nil
}

// ExtractSnapshot retrieves a snapshot and extracts it to the target directory.
func (s *LocalSnapStore) ExtractSnapshot(ctx context.Context, snapID string, targetDir string, removeExisting bool) (*common.ArchiveMetadata, error) {
	if err := validateSnapID(snapID); err != nil {
		return nil, err
	}

	archiveFile := s.archiveFilePath(snapID)

	// Check if snapshot exists
	if _, err := os.Stat(archiveFile); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("snapshot %s not found: %w", snapID, os.ErrNotExist)
		}
		return nil, fmt.Errorf("checking snapshot file: %w", err)
	}

	// Extract the archive with auto-detection and metadata parsing
	result, err := common.ExtractArchiveWithResult(archiveFile, targetDir, removeExisting)
	if err != nil {
		return nil, fmt.Errorf("extracting snapshot archive: %w", err)
	}

	return result.Metadata, nil
}

// archiveFilePath returns the full path to a snapshot archive file.
// If snapID already ends with ".afb" (portable backup), the extension is kept as-is.
// Otherwise ".tar.zst" is appended for native snapshots.
func (s *LocalSnapStore) archiveFilePath(snapID string) string {
	if strings.HasSuffix(snapID, ".afb") {
		return filepath.Join(s.snapDir, snapID)
	}
	return filepath.Join(s.snapDir, fmt.Sprintf("%s.tar.zst", snapID))
}

// validateSnapID ensures the snapshot ID is valid and doesn't contain path traversal.
func validateSnapID(snapID string) error {
	if snapID == "" {
		return fmt.Errorf("snapshot ID cannot be empty")
	}
	if filepath.Base(snapID) != snapID {
		return fmt.Errorf("invalid snapshot ID (contains path separators): %s", snapID)
	}
	return nil
}
