// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations.

package metadata

import (
	"bytes"
	"context"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/gofrs/flock"
	"github.com/minio/minio-go/v7"
)

const (
	clusterBackupAttemptLeaseVersion       = 1
	clusterBackupAttemptLeaseDir           = ".antfly-go-attempt-leases"
	clusterBackupAttemptLeaseDuration      = 10 * time.Minute
	clusterBackupAttemptLeaseRenewInterval = time.Minute
	clusterBackupAttemptLeaseSafetyMargin  = time.Minute
	clusterBackupAttemptCleanupTimeout     = 2 * time.Minute

	clusterBackupAttemptLeaseStateActive     = "active"
	clusterBackupAttemptLeaseStateReclaiming = "reclaiming"
)

type clusterBackupAttemptLeaseRecord struct {
	Version    uint32    `json:"version"`
	AttemptID  string    `json:"attempt_id"`
	Generation uint64    `json:"generation"`
	State      string    `json:"state"`
	ExpiresAt  time.Time `json:"expires_at"`
}

func validateClusterBackupAttemptLease(
	lease *clusterBackupAttemptLeaseRecord,
	expectedAttemptID string,
) error {
	if lease == nil ||
		lease.Version != clusterBackupAttemptLeaseVersion ||
		lease.AttemptID != expectedAttemptID ||
		lease.Generation == 0 ||
		lease.ExpiresAt.IsZero() {
		return errors.New("invalid cluster backup attempt lease")
	}
	if err := common.ValidateBackupID(lease.AttemptID); err != nil {
		return fmt.Errorf("invalid cluster backup attempt lease: %w", err)
	}
	switch lease.State {
	case clusterBackupAttemptLeaseStateActive,
		clusterBackupAttemptLeaseStateReclaiming:
		return nil
	default:
		return errors.New("invalid cluster backup attempt lease state")
	}
}

func encodeClusterBackupAttemptLease(lease *clusterBackupAttemptLeaseRecord) ([]byte, error) {
	if err := validateClusterBackupAttemptLease(lease, lease.AttemptID); err != nil {
		return nil, err
	}
	var body bytes.Buffer
	writer := &boundedWriter{writer: &body, remaining: maxBackupMetadataBytes}
	if err := stdjson.NewEncoder(writer).Encode(lease); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func decodeClusterBackupAttemptLease(
	body []byte,
	expectedAttemptID string,
) (*clusterBackupAttemptLeaseRecord, error) {
	var lease clusterBackupAttemptLeaseRecord
	decoder := stdjson.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&lease); err != nil {
		return nil, fmt.Errorf("decoding cluster backup attempt lease: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid trailing cluster backup attempt lease data")
	}
	if err := validateClusterBackupAttemptLease(&lease, expectedAttemptID); err != nil {
		return nil, err
	}
	return &lease, nil
}

func clusterBackupAttemptLeaseObjectKey(prefix, attemptID string) string {
	key := path.Join(clusterBackupAttemptLeaseDir, attemptID+".json")
	if prefix != "" {
		key = path.Join(prefix, key)
	}
	return key
}

func clusterBackupAttemptLeasePath(resolvedLocation, attemptID string) string {
	return filepath.Join(
		strings.TrimPrefix(resolvedLocation, "file://"),
		clusterBackupAttemptLeaseDir,
		attemptID+".json",
	)
}

func createClusterBackupAttemptLease(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
	now time.Time,
) error {
	lease := &clusterBackupAttemptLeaseRecord{
		Version:    clusterBackupAttemptLeaseVersion,
		AttemptID:  attemptID,
		Generation: 1,
		State:      clusterBackupAttemptLeaseStateActive,
		ExpiresAt:  now.UTC().Add(clusterBackupAttemptLeaseDuration),
	}
	_, err := mutateClusterBackupAttemptLease(
		ctx,
		resolvedLocation,
		s3Info,
		attemptID,
		func(current *clusterBackupAttemptLeaseRecord) (*clusterBackupAttemptLeaseRecord, bool, error) {
			if current != nil {
				return nil, false, fmt.Errorf(
					"%w: attempt lease %s",
					ErrBackupAlreadyExists,
					attemptID,
				)
			}
			return lease, true, nil
		},
	)
	return err
}

func readClusterBackupAttemptLeaseFile(
	leasePath, attemptID string,
) (*clusterBackupAttemptLeaseRecord, error) {
	file, err := os.Open(filepath.Clean(leasePath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = file.Close() }()
	body, err := readBackupMetadata(file)
	if err != nil {
		return nil, err
	}
	return decodeClusterBackupAttemptLease(body, attemptID)
}

func readClusterBackupAttemptLeaseObject(
	ctx context.Context,
	client *minio.Client,
	bucket, objectKey, attemptID string,
) (*clusterBackupAttemptLeaseRecord, string, error) {
	info, err := client.StatObject(ctx, bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		if isS3ObjectNotFound(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	if info.Size <= 0 || info.Size > maxBackupMetadataBytes || info.ETag == "" {
		return nil, "", errors.New("invalid cluster backup attempt lease object identity")
	}
	options := minio.GetObjectOptions{}
	if err := options.SetMatchETag(info.ETag); err != nil {
		return nil, "", err
	}
	object, err := client.GetObject(ctx, bucket, objectKey, options)
	if err != nil {
		if isS3ObjectNotFound(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	defer func() { _ = object.Close() }()
	body, err := readBackupMetadata(object)
	if err != nil {
		if isS3ObjectNotFound(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	lease, err := decodeClusterBackupAttemptLease(body, attemptID)
	return lease, info.ETag, err
}

type clusterBackupAttemptLeaseMutation func(
	current *clusterBackupAttemptLeaseRecord,
) (next *clusterBackupAttemptLeaseRecord, result bool, err error)

func mutateClusterBackupAttemptLease(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
	mutate clusterBackupAttemptLeaseMutation,
) (bool, error) {
	if s3Info != nil {
		// The immutable attempt marker is published before its lease, so the
		// bucket already exists. Avoid repeating bucket-admission work on every
		// heartbeat.
		client, err := s3Info.NewMinioClient()
		if err != nil {
			return false, err
		}
		objectKey := clusterBackupAttemptLeaseObjectKey(s3Info.Prefix, attemptID)
		for range 16 {
			current, etag, err := readClusterBackupAttemptLeaseObject(
				ctx,
				client,
				s3Info.Bucket,
				objectKey,
				attemptID,
			)
			if err != nil {
				if common.IsS3CreateConflict(err) {
					continue
				}
				return false, err
			}
			next, result, err := mutate(current)
			if err != nil || next == nil {
				return result, err
			}
			body, err := encodeClusterBackupAttemptLease(next)
			if err != nil {
				return false, err
			}
			options := minio.PutObjectOptions{ContentType: "application/json"}
			if etag == "" {
				options.SetMatchETagExcept("*")
			} else {
				options.SetMatchETag(etag)
			}
			if _, err := client.PutObject(
				ctx,
				s3Info.Bucket,
				objectKey,
				bytes.NewReader(body),
				int64(len(body)),
				options,
			); err != nil {
				if common.IsS3CreateConflict(err) {
					continue
				}
				return false, err
			}
			return result, nil
		}
		return false, errors.New("cluster backup attempt lease update conflict")
	}

	leasePath := clusterBackupAttemptLeasePath(resolvedLocation, attemptID)
	if err := os.MkdirAll(filepath.Dir(leasePath), 0o750); err != nil {
		return false, err
	}
	// One stable repository-local lock avoids accumulating a lock file per
	// completed attempt and avoids split-lock races caused by unlinking a lock
	// file while another process still has its inode open.
	leaseLock := flock.New(filepath.Join(filepath.Dir(leasePath), ".mutate.lock"))
	locked, err := leaseLock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return false, err
	}
	if !locked {
		return false, errors.New("cluster backup attempt lease lock unavailable")
	}
	defer func() { _ = leaseLock.Close() }()
	current, err := readClusterBackupAttemptLeaseFile(leasePath, attemptID)
	if err != nil {
		return false, err
	}
	replace := current != nil
	next, result, err := mutate(current)
	if err != nil || next == nil {
		return result, err
	}
	body, err := encodeClusterBackupAttemptLease(next)
	if err != nil {
		return false, err
	}
	if err := writeBytesFileAtomically(ctx, leasePath, body, replace); err != nil {
		return false, err
	}
	return result, nil
}

func renewClusterBackupAttemptLease(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
	now time.Time,
) (time.Time, bool, error) {
	var expiresAt time.Time
	owned, err := mutateClusterBackupAttemptLease(
		ctx,
		resolvedLocation,
		s3Info,
		attemptID,
		func(current *clusterBackupAttemptLeaseRecord) (*clusterBackupAttemptLeaseRecord, bool, error) {
			now = now.UTC()
			if current == nil ||
				current.State != clusterBackupAttemptLeaseStateActive ||
				!current.ExpiresAt.After(now) {
				return nil, false, nil
			}
			if current.Generation == ^uint64(0) {
				return nil, false, errors.New("cluster backup attempt lease generation exhausted")
			}
			next := *current
			next.Generation++
			next.ExpiresAt = now.Add(clusterBackupAttemptLeaseDuration)
			expiresAt = next.ExpiresAt
			return &next, true, nil
		},
	)
	return expiresAt, owned, err
}

func claimExpiredClusterBackupAttemptLease(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
	now time.Time,
) (bool, error) {
	return mutateClusterBackupAttemptLease(
		ctx,
		resolvedLocation,
		s3Info,
		attemptID,
		func(current *clusterBackupAttemptLeaseRecord) (*clusterBackupAttemptLeaseRecord, bool, error) {
			if current == nil {
				// The attempt marker is published before its lease. A crash in
				// that window leaves no sidecar, so recovery creates the
				// reclaiming record atomically before deleting shared objects.
				return &clusterBackupAttemptLeaseRecord{
					Version:    clusterBackupAttemptLeaseVersion,
					AttemptID:  attemptID,
					Generation: 1,
					State:      clusterBackupAttemptLeaseStateReclaiming,
					ExpiresAt: now.UTC().Add(
						clusterBackupAttemptCleanupTimeout +
							clusterBackupAttemptLeaseSafetyMargin,
					),
				}, true, nil
			}
			if current.ExpiresAt.After(now.UTC()) {
				return nil, false, nil
			}
			if current.Generation == ^uint64(0) {
				return nil, false, errors.New("cluster backup attempt lease generation exhausted")
			}
			next := *current
			next.Generation++
			next.State = clusterBackupAttemptLeaseStateReclaiming
			next.ExpiresAt = now.UTC().Add(
				clusterBackupAttemptCleanupTimeout +
					clusterBackupAttemptLeaseSafetyMargin,
			)
			return &next, true, nil
		},
	)
}

func deleteClusterBackupAttemptLease(
	ctx context.Context,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
) error {
	if s3Info != nil {
		client, err := s3Info.NewMinioClient()
		if err != nil {
			return err
		}
		return client.RemoveObject(
			ctx,
			s3Info.Bucket,
			clusterBackupAttemptLeaseObjectKey(s3Info.Prefix, attemptID),
			minio.RemoveObjectOptions{},
		)
	}
	return removeFileAndSyncDirectory(
		ctx,
		clusterBackupAttemptLeasePath(resolvedLocation, attemptID),
	)
}

type clusterBackupAttemptLeaseController struct {
	stop             context.CancelFunc
	done             <-chan struct{}
	resolvedLocation string
	s3Info           *common.S3Info
	attemptID        string
}

func startClusterBackupAttemptLease(
	ctx context.Context,
	cancelBackup context.CancelFunc,
	resolvedLocation string,
	s3Info *common.S3Info,
	attemptID string,
) (*clusterBackupAttemptLeaseController, error) {
	now := time.Now().UTC()
	if err := createClusterBackupAttemptLease(
		ctx,
		resolvedLocation,
		s3Info,
		attemptID,
		now,
	); err != nil {
		return nil, err
	}
	// The operation context still cancels backup work, but lease ownership
	// must survive long enough to perform bounded cleanup after a client
	// disconnect. The handler stops this context explicitly on every exit.
	leaseCtx, stop := context.WithCancel(context.WithoutCancel(ctx))
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(clusterBackupAttemptLeaseRenewInterval)
		defer ticker.Stop()
		expiresAt := now.Add(clusterBackupAttemptLeaseDuration)
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(
					leaseCtx,
					clusterBackupAttemptLeaseRenewInterval,
				)
				nextExpiry, owned, err := renewClusterBackupAttemptLease(
					renewCtx,
					resolvedLocation,
					s3Info,
					attemptID,
					time.Now().UTC(),
				)
				renewCancel()
				if owned && err == nil {
					expiresAt = nextExpiry
					continue
				}
				if err == nil ||
					!time.Now().UTC().Before(expiresAt.Add(-clusterBackupAttemptLeaseSafetyMargin)) {
					cancelBackup()
					return
				}
			}
		}
	}()
	return &clusterBackupAttemptLeaseController{
		stop:             stop,
		done:             done,
		resolvedLocation: resolvedLocation,
		s3Info:           s3Info,
		attemptID:        attemptID,
	}, nil
}

func (controller *clusterBackupAttemptLeaseController) Stop() {
	if controller == nil {
		return
	}
	controller.stop()
	<-controller.done
}

// StopAndAcquireCleanupWindow stops background renewal, then atomically proves
// that this producer still owns an active lease and extends it beyond the
// bounded cleanup deadline. A fenced or ambiguous producer must leave cleanup
// to the reclaiming owner.
func (controller *clusterBackupAttemptLeaseController) StopAndAcquireCleanupWindow(
	ctx context.Context,
) (bool, error) {
	if controller == nil {
		return false, nil
	}
	controller.Stop()
	_, owned, err := renewClusterBackupAttemptLease(
		ctx,
		controller.resolvedLocation,
		controller.s3Info,
		controller.attemptID,
		time.Now().UTC(),
	)
	return owned, err
}
