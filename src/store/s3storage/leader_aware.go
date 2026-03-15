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

package s3storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2/objstorage/remote"
)

// ErrNotLeader is returned when a non-leader replica attempts a write or delete
// operation on S3 storage. Callers can check for this with errors.Is.
var ErrNotLeader = errors.New("not raft leader")

// LeaderAwareS3Storage wraps S3Storage to only allow writes from the Raft leader.
// This prevents multiple replicas from writing conflicting sstables to S3.
//
// Design:
// - All replicas configure Pebble with this storage and CreateOnSharedAll
// - Only the leader (isLeader=true) can write new sstables to S3
// - All replicas can read sstables from S3 (foreign objects)
// - On leadership change, the flag is updated by DBImpl.LeaderFactory
//
// IMPORTANT: This works because Antfly's Raft replication ensures followers
// receive the leader's compacted sstables as foreign S3 objects rather than
// compacting independently. Followers apply Raft log entries to their local
// Pebble, but the leader's compaction outputs (written to S3) are shared
// across all replicas. If a follower ever does attempt a local compaction
// that targets S3, CreateObject returns ErrNotLeader as a safety net.
type LeaderAwareS3Storage struct {
	underlying *S3Storage
	isLeader   *atomic.Bool
}

// NewLeaderAwareS3Storage creates a leadership-aware wrapper around S3Storage.
// The isLeader pointer is shared with DBImpl and updated by LeaderFactory.
func NewLeaderAwareS3Storage(underlying *S3Storage, isLeader *atomic.Bool) *LeaderAwareS3Storage {
	return &LeaderAwareS3Storage{
		underlying: underlying,
		isLeader:   isLeader,
	}
}

// CreateObject implements remote.Storage.CreateObject.
// Only the Raft leader can write new sstables to S3.
func (s *LeaderAwareS3Storage) CreateObject(objectName string) (io.WriteCloser, error) {
	if !s.isLeader.Load() {
		return nil, fmt.Errorf("cannot write to S3 object %s: %w", objectName, ErrNotLeader)
	}
	return s.underlying.CreateObject(objectName)
}

// ReadObject implements remote.Storage.ReadObject.
func (s *LeaderAwareS3Storage) ReadObject(
	ctx context.Context,
	objectName string,
) (remote.ObjectReader, int64, error) {
	return s.underlying.ReadObject(ctx, objectName)
}

// Size implements remote.Storage.Size.
func (s *LeaderAwareS3Storage) Size(objectName string) (int64, error) {
	return s.underlying.Size(objectName)
}

// IsNotExistError implements remote.Storage.IsNotExistError.
func (s *LeaderAwareS3Storage) IsNotExistError(err error) bool {
	return s.underlying.IsNotExistError(err)
}

// Delete implements remote.Storage.Delete.
// Only the Raft leader can delete objects (used by Pebble's GC).
func (s *LeaderAwareS3Storage) Delete(objectName string) error {
	if !s.isLeader.Load() {
		return fmt.Errorf("cannot delete S3 object %s: %w", objectName, ErrNotLeader)
	}
	return s.underlying.Delete(objectName)
}

// List implements remote.Storage.List.
func (s *LeaderAwareS3Storage) List(prefix, delimiter string) ([]string, error) {
	return s.underlying.List(prefix, delimiter)
}

// Close implements remote.Storage.Close.
func (s *LeaderAwareS3Storage) Close() error {
	return s.underlying.Close()
}

// Verify interface implementation
var _ remote.Storage = (*LeaderAwareS3Storage)(nil)
