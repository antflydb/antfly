// Copyright 2026 Antfly, Inc.
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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	stdjson "encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/antflydb/antfly/go/pkg/antfly/lib/types"
	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	json "github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/gofrs/flock"
	"github.com/minio/minio-go/v7"
)

// backupStore abstracts reading and writing backup metadata to either
// local filesystem or S3-compatible object storage.
type backupStore interface {
	EnsureMetadataAbsent(ctx context.Context, id string) error
	ReserveBackupID(ctx context.Context, id, owner string) error
	BackupIDReservationOwnedBy(ctx context.Context, id, owner string) (bool, error)
	DeleteMetadata(ctx context.Context, id string) error
	DeleteArtifact(ctx context.Context, name string) error
	ValidateArtifact(ctx context.Context, name string) error
	ValidateArtifactMetadata(ctx context.Context, name string, expectedSize uint64) error
	ValidateArtifactIdentity(
		ctx context.Context,
		artifact common.BackupArtifactIntegrity,
	) error
	ReleaseBackupID(ctx context.Context, id, owner string) (bool, error)
	WriteMetadata(
		ctx context.Context,
		id string,
		table *store.Table,
		format common.BackupFormat,
		artifacts []common.BackupArtifactIntegrity,
	) error
	ReadMetadata(ctx context.Context, id string) (*backupMetadata, error)
	ResolvedLocation() string
}

const (
	backupMetadataVersion  = 2
	maxBackupMetadataBytes = 16 * 1024 * 1024

	backupReservationVersion       = 1
	maxBackupReservationBytes      = 4 * 1024
	backupReservationStateActive   = "active"
	backupReservationStateReleased = "released"
)

var (
	ErrBackupAlreadyExists    = common.ErrBackupAlreadyExists
	ErrBackupMetadataTooLarge = errors.New("backup metadata exceeds the 16 MiB limit")
)

type boundedWriter struct {
	writer    io.Writer
	remaining int64
}

type backupReservation struct {
	Version    uint32 `json:"version"`
	Owner      string `json:"owner"`
	Generation uint64 `json:"generation"`
	State      string `json:"state"`
}

func validateBackupReservation(reservation *backupReservation) error {
	if reservation == nil ||
		reservation.Version != backupReservationVersion ||
		reservation.Generation == 0 {
		return errors.New("invalid backup reservation")
	}
	if err := common.ValidateBackupID(reservation.Owner); err != nil {
		return fmt.Errorf("invalid backup reservation owner: %w", err)
	}
	switch reservation.State {
	case backupReservationStateActive, backupReservationStateReleased:
		return nil
	default:
		return errors.New("invalid backup reservation state")
	}
}

func encodeBackupReservation(reservation *backupReservation) ([]byte, error) {
	if err := validateBackupReservation(reservation); err != nil {
		return nil, err
	}
	var body bytes.Buffer
	if err := stdjson.NewEncoder(&body).Encode(reservation); err != nil {
		return nil, err
	}
	if body.Len() > maxBackupReservationBytes {
		return nil, errors.New("backup reservation is too large")
	}
	return body.Bytes(), nil
}

func decodeBackupReservation(body []byte) (*backupReservation, error) {
	if bytes.Equal(bytes.TrimSpace(body), []byte("reserved")) {
		// Anonymous reservations from older Go releases cannot be attributed
		// safely. Treat them as active but unowned so automated cleanup fails
		// closed rather than deleting a retry's objects.
		return &backupReservation{
			State: backupReservationStateActive,
		}, nil
	}
	if len(body) == 0 || len(body) > maxBackupReservationBytes {
		return nil, errors.New("invalid backup reservation size")
	}
	var reservation backupReservation
	decoder := stdjson.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&reservation); err != nil {
		return nil, fmt.Errorf("decoding backup reservation: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("invalid trailing backup reservation data")
	}
	if err := validateBackupReservation(&reservation); err != nil {
		return nil, err
	}
	return &reservation, nil
}

type backupReservationMutation func(
	current *backupReservation,
) (next *backupReservation, result bool, err error)

func (w *boundedWriter) Write(data []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, ErrBackupMetadataTooLarge
	}
	if int64(len(data)) > w.remaining {
		data = data[:w.remaining]
		n, err := w.writer.Write(data)
		w.remaining -= int64(n)
		if err != nil {
			return n, err
		}
		return n, ErrBackupMetadataTooLarge
	}
	n, err := w.writer.Write(data)
	w.remaining -= int64(n)
	return n, err
}

func readBackupMetadata(r io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, maxBackupMetadataBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxBackupMetadataBytes {
		return nil, ErrBackupMetadataTooLarge
	}
	return data, nil
}

type backupMetadata struct {
	Version   uint32                           `json:"version"`
	Format    common.BackupFormat              `json:"format"`
	Table     *store.Table                     `json:"table"`
	Artifacts []common.BackupArtifactIntegrity `json:"artifacts,omitempty"`
}

func newBackupMetadata(
	table *store.Table,
	format common.BackupFormat,
	artifacts []common.BackupArtifactIntegrity,
) (*backupMetadata, error) {
	if table == nil {
		return nil, fmt.Errorf("table metadata is required")
	}
	format = common.NormalizeBackupFormat(format)
	switch format {
	case common.BackupFormatNative:
		if len(artifacts) != 0 {
			return nil, errors.New("native backup metadata must not declare portable artifacts")
		}
	case common.BackupFormatPortable:
		if err := validatePortableArtifactIntegrities(table, artifacts); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported backup format %q", format)
	}
	return &backupMetadata{
		Version:   backupMetadataVersion,
		Format:    format,
		Table:     table,
		Artifacts: append([]common.BackupArtifactIntegrity(nil), artifacts...),
	}, nil
}

func validatePortableArtifactIntegrities(
	table *store.Table,
	artifacts []common.BackupArtifactIntegrity,
) error {
	if table == nil || len(table.Shards) == 0 || len(artifacts) != len(table.Shards) {
		return errors.New("portable backup artifact identities do not match table shards")
	}
	seenNames := make(map[string]struct{}, len(artifacts))
	seenShards := make(map[types.ID]struct{}, len(artifacts))
	var artifactBackupID string
	for _, artifact := range artifacts {
		decoded, err := hex.DecodeString(artifact.SHA256)
		if err != nil || len(decoded) != sha256.Size ||
			hex.EncodeToString(decoded) != artifact.SHA256 ||
			artifact.SizeBytes == 0 ||
			artifact.Name == "" ||
			path.Base(artifact.Name) != artifact.Name ||
			strings.ContainsAny(artifact.Name, `/\`) {
			return fmt.Errorf("invalid portable backup artifact identity %q", artifact.Name)
		}
		if _, duplicate := seenNames[artifact.Name]; duplicate {
			return fmt.Errorf("duplicate portable backup artifact identity %q", artifact.Name)
		}
		seenNames[artifact.Name] = struct{}{}

		stem, ok := strings.CutSuffix(artifact.Name, ".afb")
		separator := strings.LastIndexByte(stem, '-')
		if !ok || separator <= 0 || separator == len(stem)-1 {
			return fmt.Errorf("portable backup artifact %q is not bound to a shard", artifact.Name)
		}
		backupID := stem[:separator]
		if err := common.ValidateBackupID(backupID); err != nil {
			return fmt.Errorf("portable backup artifact %q has an invalid backup ID: %w", artifact.Name, err)
		}
		if artifactBackupID == "" {
			artifactBackupID = backupID
		} else if backupID != artifactBackupID {
			return errors.New("portable backup artifacts do not share one backup ID")
		}
		shardID, err := types.IDFromString(stem[separator+1:])
		_, shardExists := table.Shards[shardID]
		if err != nil || !shardExists {
			return fmt.Errorf("portable backup artifact %q references an unknown shard", artifact.Name)
		}
		if artifact.Name != common.ShardPortableBackupFileName(backupID, shardID) {
			return fmt.Errorf("portable backup artifact %q is not canonically named", artifact.Name)
		}
		if _, duplicate := seenShards[shardID]; duplicate {
			return fmt.Errorf("portable backup artifacts contain duplicate shard %s", shardID)
		}
		seenShards[shardID] = struct{}{}
	}
	return nil
}

func decodeBackupMetadata(data []byte) (*backupMetadata, error) {
	var metadata backupMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("unmarshalling backup metadata: %w", err)
	}
	if metadata.Version != backupMetadataVersion {
		return nil, fmt.Errorf("unsupported backup metadata version %d", metadata.Version)
	}
	if metadata.Table == nil {
		return nil, fmt.Errorf("backup metadata is missing table")
	}
	switch metadata.Format {
	case common.BackupFormatNative:
		if len(metadata.Artifacts) != 0 {
			return nil, errors.New("native backup metadata declares portable artifacts")
		}
	case common.BackupFormatPortable:
		if err := validatePortableArtifactIntegrities(metadata.Table, metadata.Artifacts); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("unsupported backup format %q", metadata.Format)
	}
	return &metadata, nil
}

func writeJSONFileAtomically(ctx context.Context, filePath string, value any) error {
	var body bytes.Buffer
	writer := &boundedWriter{writer: &body, remaining: maxBackupMetadataBytes}
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return fmt.Errorf("encoding metadata to JSON: %w", err)
	}
	return writeBytesFileAtomically(ctx, filePath, body.Bytes(), false)
}

// writeBytesFileAtomically publishes an exact byte sequence after fsyncing it.
// replace=false uses a hard-link commit so immutable metadata cannot be
// replaced; mutable control records use an atomic rename and
// last-completed-write order.
func writeBytesFileAtomically(
	ctx context.Context,
	filePath string,
	body []byte,
	replace bool,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(body) > maxBackupMetadataBytes {
		return ErrBackupMetadataTooLarge
	}
	dir := filepath.Dir(filePath)
	file, err := os.CreateTemp(dir, "."+filepath.Base(filePath)+".tmp-*") //#nosec G304,G703 -- caller validates the destination directory
	if err != nil {
		return fmt.Errorf("creating temporary metadata file: %w", err)
	}
	tempPath := file.Name()
	defer func() {
		_ = file.Close()
		_ = os.Remove(tempPath)
	}()
	if err := file.Chmod(0o600); err != nil {
		return fmt.Errorf("setting metadata file permissions: %w", err)
	}
	if _, err := file.Write(body); err != nil {
		return fmt.Errorf("writing metadata: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing metadata file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing metadata file: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if replace {
		if err := os.Rename(tempPath, filePath); err != nil {
			return fmt.Errorf("replacing metadata file: %w", err)
		}
	} else {
		if err := os.Link(tempPath, filePath); err != nil {
			if os.IsExist(err) {
				return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, filepath.Base(filePath))
			}
			return fmt.Errorf("publishing metadata file: %w", err)
		}
	}
	dirHandle, err := os.Open(dir) //#nosec G304 -- caller validates the destination directory
	if err != nil {
		return fmt.Errorf("opening metadata directory for sync: %w", err)
	}
	defer func() { _ = dirHandle.Close() }()
	if err := dirHandle.Sync(); err != nil {
		return fmt.Errorf("syncing metadata directory: %w", err)
	}
	return nil
}

// newBackupStore authorizes a location against a named external_io connection
// before constructing the protocol-specific store.
func newBackupStore(
	config *common.Config,
	connection, capability, location string,
) (backupStore, error) {
	resolvedLocation, s3Config, err := resolveBackupLocation(
		config,
		connection,
		capability,
		location,
	)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(location, "s3://") {
		return &s3BackupStore{s3Config: s3Config}, nil
	}
	return &fileBackupStore{location: resolvedLocation}, nil
}

func resolveBackupLocation(
	config *common.Config,
	connection, capability, location string,
) (string, *common.S3Info, error) {
	switch {
	case strings.HasPrefix(location, "s3://"):
		s3Config, err := config.ResolveS3Info(connection, capability, location)
		if err != nil {
			return "", nil, fmt.Errorf("authorizing S3 backup location: %w", err)
		}
		return location, &s3Config, nil
	case strings.HasPrefix(location, "file://"):
		resolved, err := config.ResolveFilesystemPath(connection, capability, location)
		if err != nil {
			return "", nil, fmt.Errorf("authorizing filesystem backup location: %w", err)
		}
		return "file://" + resolved, nil, nil
	default:
		return "", nil, fmt.Errorf("unsupported backup location %q", location)
	}
}

// fileBackupStore reads/writes backup metadata to the local filesystem.
type fileBackupStore struct {
	location string
}

func (s *fileBackupStore) resolveAndValidate(id string) (string, error) {
	if err := common.ValidateBackupID(id); err != nil {
		return "", err
	}
	baseDir := strings.TrimPrefix(s.location, "file://")
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return "", fmt.Errorf("resolving base directory: %w", err)
	}
	filePath := filepath.Join(absBase, filepath.Base(id)+"-metadata.json")
	if !strings.HasPrefix(filePath, absBase+string(filepath.Separator)) {
		return "", fmt.Errorf("invalid backup id %q: path traversal detected", id)
	}
	return filePath, nil
}

func (s *fileBackupStore) ResolvedLocation() string {
	if strings.HasPrefix(s.location, "file://") {
		return s.location
	}
	return "file://" + s.location
}

func (s *fileBackupStore) EnsureMetadataAbsent(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return err
	}
	if _, err := os.Stat(filePath); err == nil {
		return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("checking backup metadata %s: %w", filePath, err)
	}
	return nil
}

func (s *fileBackupStore) reservationPath(id string) (string, error) {
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(filePath, "-metadata.json") + "-reservation", nil
}

func readFileBackupReservation(reservationPath string) (*backupReservation, error) {
	file, err := os.Open(filepath.Clean(reservationPath))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() ||
		info.Size() <= 0 ||
		info.Size() > maxBackupReservationBytes {
		return nil, errors.New("invalid backup reservation file identity")
	}
	body, err := io.ReadAll(io.LimitReader(file, maxBackupReservationBytes+1))
	if err != nil {
		return nil, err
	}
	return decodeBackupReservation(body)
}

func (s *fileBackupStore) mutateBackupReservation(
	ctx context.Context,
	id string,
	mutate backupReservationMutation,
) (bool, error) {
	reservationPath, err := s.reservationPath(id)
	if err != nil {
		return false, err
	}
	if err := os.MkdirAll(filepath.Dir(reservationPath), 0o750); err != nil {
		return false, fmt.Errorf("creating backup metadata directory: %w", err)
	}
	reservationLock := flock.New(reservationPath + ".lock")
	locked, err := reservationLock.TryLockContext(ctx, 10*time.Millisecond)
	if err != nil {
		return false, err
	}
	if !locked {
		return false, errors.New("backup reservation lock unavailable")
	}
	defer func() { _ = reservationLock.Close() }()
	current, err := readFileBackupReservation(reservationPath)
	if err != nil {
		return false, err
	}
	next, result, err := mutate(current)
	if err != nil || next == nil {
		return result, err
	}
	body, err := encodeBackupReservation(next)
	if err != nil {
		return false, err
	}
	if err := writeBytesFileAtomically(
		ctx,
		reservationPath,
		body,
		current != nil,
	); err != nil {
		return false, err
	}
	return result, nil
}

func reserveBackupIDMutation(
	id, owner string,
) backupReservationMutation {
	return func(current *backupReservation) (*backupReservation, bool, error) {
		if current != nil && current.State != backupReservationStateReleased {
			return nil, false, fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
		}
		generation := uint64(1)
		if current != nil {
			if current.Generation == ^uint64(0) {
				return nil, false, errors.New("backup reservation generation exhausted")
			}
			generation = current.Generation + 1
		}
		return &backupReservation{
			Version:    backupReservationVersion,
			Owner:      owner,
			Generation: generation,
			State:      backupReservationStateActive,
		}, true, nil
	}
}

func reservationOwnedByMutation(
	owner string,
) backupReservationMutation {
	return func(current *backupReservation) (*backupReservation, bool, error) {
		owned := current != nil &&
			current.Version == backupReservationVersion &&
			current.State == backupReservationStateActive &&
			current.Owner == owner
		return nil, owned, nil
	}
}

func releaseBackupIDMutation(
	owner string,
) backupReservationMutation {
	return func(current *backupReservation) (*backupReservation, bool, error) {
		if current == nil ||
			current.Version != backupReservationVersion ||
			current.State != backupReservationStateActive ||
			current.Owner != owner {
			return nil, false, nil
		}
		if current.Generation == ^uint64(0) {
			return nil, false, errors.New("backup reservation generation exhausted")
		}
		next := *current
		next.Generation++
		next.State = backupReservationStateReleased
		return &next, true, nil
	}
}

func (s *fileBackupStore) ReserveBackupID(ctx context.Context, id, owner string) error {
	if err := common.ValidateBackupID(owner); err != nil {
		return fmt.Errorf("invalid backup reservation owner: %w", err)
	}
	if err := s.EnsureMetadataAbsent(ctx, id); err != nil {
		return err
	}
	_, err := s.mutateBackupReservation(ctx, id, reserveBackupIDMutation(id, owner))
	return err
}

func removeFileAndSyncDirectory(ctx context.Context, filePath string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(filePath); err != nil && !os.IsNotExist(err) {
		return err
	}
	dir, err := os.Open(filepath.Dir(filePath)) //#nosec G304 -- caller supplies an authorized backup path
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}

func (s *fileBackupStore) DeleteMetadata(ctx context.Context, id string) error {
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return err
	}
	return removeFileAndSyncDirectory(ctx, filePath)
}

func (s *fileBackupStore) DeleteArtifact(ctx context.Context, name string) error {
	if name == "" || filepath.Base(name) != name {
		return fmt.Errorf("invalid backup artifact name %q", name)
	}
	root := strings.TrimPrefix(s.location, "file://")
	return removeFileAndSyncDirectory(ctx, filepath.Join(root, name))
}

func (s *fileBackupStore) ValidateArtifact(ctx context.Context, name string) error {
	return s.ValidateArtifactMetadata(ctx, name, 0)
}

func (s *fileBackupStore) ValidateArtifactMetadata(
	ctx context.Context,
	name string,
	expectedSize uint64,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if name == "" || filepath.Base(name) != name {
		return fmt.Errorf("invalid backup artifact name %q", name)
	}
	rootPath := strings.TrimPrefix(s.location, "file://")
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("opening backup root: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(name)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 {
		return fmt.Errorf("backup artifact %q is not a non-empty regular file", name)
	}
	if expectedSize != 0 && uint64(info.Size()) != expectedSize {
		return fmt.Errorf(
			"%w: %s has an unexpected file size",
			common.ErrBackupArtifactIntegrityMismatch,
			name,
		)
	}
	return nil
}

func (s *fileBackupStore) ValidateArtifactIdentity(
	ctx context.Context,
	artifact common.BackupArtifactIntegrity,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := common.ValidateBackupArtifactIntegrity(&artifact); err != nil {
		return err
	}
	if artifact.Name == "" || filepath.Base(artifact.Name) != artifact.Name {
		return fmt.Errorf("invalid backup artifact name %q", artifact.Name)
	}
	rootPath := strings.TrimPrefix(s.location, "file://")
	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return fmt.Errorf("opening backup root: %w", err)
	}
	defer func() { _ = root.Close() }()
	file, err := root.Open(artifact.Name)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	initial, err := file.Stat()
	if err != nil {
		return err
	}
	if !initial.Mode().IsRegular() || initial.Size() <= 0 ||
		uint64(initial.Size()) != artifact.SizeBytes {
		return fmt.Errorf(
			"%w: %s has an unexpected file identity",
			common.ErrBackupArtifactIntegrityMismatch,
			artifact.Name,
		)
	}
	if err := common.VerifyBackupArtifact(ctx, file, artifact); err != nil {
		return err
	}
	verified, err := file.Stat()
	if err != nil {
		return err
	}
	currentFile, err := root.Open(artifact.Name)
	if err != nil {
		return err
	}
	defer func() { _ = currentFile.Close() }()
	current, err := currentFile.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(initial, verified) ||
		!os.SameFile(initial, current) ||
		verified.Size() != initial.Size() ||
		!verified.ModTime().Equal(initial.ModTime()) ||
		current.Size() != initial.Size() ||
		!current.ModTime().Equal(initial.ModTime()) {
		return fmt.Errorf(
			"%w: %s changed during verification",
			common.ErrBackupArtifactIntegrityMismatch,
			artifact.Name,
		)
	}
	return nil
}

func (s *fileBackupStore) BackupIDReservationOwnedBy(
	ctx context.Context,
	id, owner string,
) (bool, error) {
	if err := common.ValidateBackupID(owner); err != nil {
		return false, fmt.Errorf("invalid backup reservation owner: %w", err)
	}
	return s.mutateBackupReservation(ctx, id, reservationOwnedByMutation(owner))
}

func (s *fileBackupStore) ReleaseBackupID(
	ctx context.Context,
	id, owner string,
) (bool, error) {
	if err := common.ValidateBackupID(owner); err != nil {
		return false, fmt.Errorf("invalid backup reservation owner: %w", err)
	}
	return s.mutateBackupReservation(ctx, id, releaseBackupIDMutation(owner))
}

func (s *fileBackupStore) WriteMetadata(
	ctx context.Context,
	id string,
	table *store.Table,
	format common.BackupFormat,
	artifacts []common.BackupArtifactIntegrity,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	metadata, err := newBackupMetadata(table, format, artifacts)
	if err != nil {
		return err
	}
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return err
	}
	return writeJSONFileAtomically(ctx, filePath, metadata)
}

func (s *fileBackupStore) ReadMetadata(
	ctx context.Context,
	id string,
) (*backupMetadata, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(filePath) //#nosec G304 -- path validated by resolveAndValidate
	if err != nil {
		return nil, fmt.Errorf("reading metadata file %s: %w", filePath, err)
	}
	defer func() { _ = file.Close() }()
	data, err := readBackupMetadata(file)
	if err != nil {
		return nil, fmt.Errorf("reading metadata file %s: %w", filePath, err)
	}
	metadata, err := decodeBackupMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("decoding table metadata from %s: %w", filePath, err)
	}
	return metadata, nil
}

// s3BackupStore reads/writes backup metadata to an S3-compatible object store.
type s3BackupStore struct {
	s3Config *common.S3Info
}

func (s *s3BackupStore) ResolvedLocation() string {
	location := "s3://" + s.s3Config.Bucket
	if s.s3Config.Prefix != "" {
		location += "/" + s.s3Config.Prefix
	}
	return location
}

func isS3ObjectNotFound(err error) bool {
	response := minio.ToErrorResponse(err)
	return response.Code == minio.NoSuchKey
}

func (s *s3BackupStore) objectKey(id string) string {
	objectKey := id + "-metadata.json"
	if s.s3Config.Prefix != "" {
		objectKey = path.Join(s.s3Config.Prefix, objectKey)
	}
	return objectKey
}

func (s *s3BackupStore) reservationKey(id string) string {
	objectKey := id + "-reservation"
	if s.s3Config.Prefix != "" {
		objectKey = path.Join(s.s3Config.Prefix, objectKey)
	}
	return objectKey
}

func (s *s3BackupStore) EnsureMetadataAbsent(ctx context.Context, id string) error {
	if err := common.ValidateBackupID(id); err != nil {
		return err
	}
	client, err := s.s3Config.EnsureBucket(ctx)
	if err != nil {
		return err
	}
	if _, err := client.StatObject(
		ctx,
		s.s3Config.Bucket,
		s.objectKey(id),
		minio.StatObjectOptions{},
	); err == nil {
		return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
	} else if !isS3ObjectNotFound(err) {
		return fmt.Errorf("checking backup metadata %s: %w", s.objectKey(id), err)
	}
	return nil
}

func readS3BackupReservation(
	ctx context.Context,
	client *minio.Client,
	bucket, objectKey string,
) (*backupReservation, string, error) {
	info, err := client.StatObject(ctx, bucket, objectKey, minio.StatObjectOptions{})
	if err != nil {
		if isS3ObjectNotFound(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	if info.Size <= 0 ||
		info.Size > maxBackupReservationBytes ||
		info.ETag == "" {
		return nil, "", errors.New("invalid backup reservation object identity")
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
	body, err := io.ReadAll(io.LimitReader(object, maxBackupReservationBytes+1))
	if err != nil {
		if isS3ObjectNotFound(err) || common.IsS3CreateConflict(err) {
			return nil, "", nil
		}
		return nil, "", err
	}
	reservation, err := decodeBackupReservation(body)
	return reservation, info.ETag, err
}

func (s *s3BackupStore) mutateBackupReservation(
	ctx context.Context,
	id string,
	mutate backupReservationMutation,
) (bool, error) {
	if err := common.ValidateBackupID(id); err != nil {
		return false, err
	}
	client, err := s.s3Config.EnsureBucket(ctx)
	if err != nil {
		return false, err
	}
	objectKey := s.reservationKey(id)
	for range 16 {
		current, etag, err := readS3BackupReservation(
			ctx,
			client,
			s.s3Config.Bucket,
			objectKey,
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
		body, err := encodeBackupReservation(next)
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
			s.s3Config.Bucket,
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
	return false, errors.New("backup reservation update conflict")
}

func (s *s3BackupStore) ReserveBackupID(ctx context.Context, id, owner string) error {
	if err := common.ValidateBackupID(owner); err != nil {
		return fmt.Errorf("invalid backup reservation owner: %w", err)
	}
	if err := s.EnsureMetadataAbsent(ctx, id); err != nil {
		return err
	}
	_, err := s.mutateBackupReservation(ctx, id, reserveBackupIDMutation(id, owner))
	return err
}

func (s *s3BackupStore) artifactKey(name string) (string, error) {
	if name == "" || path.Base(name) != name {
		return "", fmt.Errorf("invalid backup artifact name %q", name)
	}
	if s.s3Config.Prefix == "" {
		return name, nil
	}
	return path.Join(s.s3Config.Prefix, name), nil
}

func (s *s3BackupStore) removeObject(ctx context.Context, objectKey string) error {
	client, err := s.s3Config.EnsureBucket(ctx)
	if err != nil {
		return err
	}
	return client.RemoveObject(ctx, s.s3Config.Bucket, objectKey, minio.RemoveObjectOptions{})
}

func (s *s3BackupStore) DeleteMetadata(ctx context.Context, id string) error {
	if err := common.ValidateBackupID(id); err != nil {
		return err
	}
	return s.removeObject(ctx, s.objectKey(id))
}

func (s *s3BackupStore) DeleteArtifact(ctx context.Context, name string) error {
	key, err := s.artifactKey(name)
	if err != nil {
		return err
	}
	return s.removeObject(ctx, key)
}

func (s *s3BackupStore) ValidateArtifact(ctx context.Context, name string) error {
	return s.ValidateArtifactMetadata(ctx, name, 0)
}

func (s *s3BackupStore) ValidateArtifactMetadata(
	ctx context.Context,
	name string,
	expectedSize uint64,
) error {
	key, err := s.artifactKey(name)
	if err != nil {
		return err
	}
	client, err := s.s3Config.NewMinioClient()
	if err != nil {
		return err
	}
	info, err := client.StatObject(
		ctx,
		s.s3Config.Bucket,
		key,
		minio.StatObjectOptions{},
	)
	if err != nil {
		return err
	}
	if info.Size <= 0 {
		return fmt.Errorf("backup artifact %q is empty", name)
	}
	if expectedSize != 0 && uint64(info.Size) != expectedSize {
		return fmt.Errorf(
			"%w: %s has an unexpected object size",
			common.ErrBackupArtifactIntegrityMismatch,
			name,
		)
	}
	return nil
}

func (s *s3BackupStore) ValidateArtifactIdentity(
	ctx context.Context,
	artifact common.BackupArtifactIntegrity,
) error {
	if err := common.ValidateBackupArtifactIntegrity(&artifact); err != nil {
		return err
	}
	key, err := s.artifactKey(artifact.Name)
	if err != nil {
		return err
	}
	client, err := s.s3Config.NewMinioClient()
	if err != nil {
		return err
	}
	info, err := client.StatObject(
		ctx,
		s.s3Config.Bucket,
		key,
		minio.StatObjectOptions{},
	)
	if err != nil {
		return err
	}
	if info.Size <= 0 || uint64(info.Size) != artifact.SizeBytes {
		return fmt.Errorf(
			"%w: %s has an unexpected object identity",
			common.ErrBackupArtifactIntegrityMismatch,
			artifact.Name,
		)
	}
	options := minio.GetObjectOptions{}
	if info.ETag != "" {
		options.SetMatchETag(info.ETag)
	}
	object, err := client.GetObject(
		ctx,
		s.s3Config.Bucket,
		key,
		options,
	)
	if err != nil {
		return err
	}
	verifyErr := common.VerifyBackupArtifact(ctx, object, artifact)
	closeErr := object.Close()
	if verifyErr != nil {
		return verifyErr
	}
	if closeErr != nil {
		return closeErr
	}
	current, err := client.StatObject(
		ctx,
		s.s3Config.Bucket,
		key,
		minio.StatObjectOptions{},
	)
	if err != nil {
		return err
	}
	if current.Size != info.Size || current.ETag != info.ETag {
		return fmt.Errorf(
			"%w: %s changed during verification",
			common.ErrBackupArtifactIntegrityMismatch,
			artifact.Name,
		)
	}
	return nil
}

func (s *s3BackupStore) BackupIDReservationOwnedBy(
	ctx context.Context,
	id, owner string,
) (bool, error) {
	if err := common.ValidateBackupID(owner); err != nil {
		return false, fmt.Errorf("invalid backup reservation owner: %w", err)
	}
	return s.mutateBackupReservation(ctx, id, reservationOwnedByMutation(owner))
}

func (s *s3BackupStore) ReleaseBackupID(
	ctx context.Context,
	id, owner string,
) (bool, error) {
	if err := common.ValidateBackupID(owner); err != nil {
		return false, fmt.Errorf("invalid backup reservation owner: %w", err)
	}
	return s.mutateBackupReservation(ctx, id, releaseBackupIDMutation(owner))
}

func (s *s3BackupStore) WriteMetadata(
	ctx context.Context,
	id string,
	table *store.Table,
	format common.BackupFormat,
	artifacts []common.BackupArtifactIntegrity,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := common.ValidateBackupID(id); err != nil {
		return err
	}
	metadata, err := newBackupMetadata(table, format, artifacts)
	if err != nil {
		return err
	}
	bucket := s.s3Config.Bucket
	minioClient, err := s.s3Config.EnsureBucket(ctx)
	if err != nil {
		return err
	}

	var b bytes.Buffer
	writer := &boundedWriter{writer: &b, remaining: maxBackupMetadataBytes}
	if err := json.NewEncoder(writer).Encode(metadata); err != nil {
		return fmt.Errorf("encoding table metadata to JSON: %w", err)
	}
	objectKey := s.objectKey(id)
	options := minio.PutObjectOptions{ContentType: "application/json"}
	options.SetMatchETagExcept("*")
	if _, err := minioClient.PutObject(ctx, bucket, objectKey, &b, int64(b.Len()), options); err != nil {
		if common.IsS3CreateConflict(err) {
			return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
		}
		return fmt.Errorf("uploading file to object store: %w", err)
	}
	return nil
}

func (s *s3BackupStore) ReadMetadata(
	ctx context.Context,
	id string,
) (*backupMetadata, error) {
	if err := common.ValidateBackupID(id); err != nil {
		return nil, err
	}
	bucket := s.s3Config.Bucket
	minioClient, err := s.s3Config.NewMinioClient()
	if err != nil {
		return nil, fmt.Errorf("creating S3 client: %w", err)
	}
	objectKey := s.objectKey(id)
	obj, err := minioClient.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting object %s from bucket %s: %w", objectKey, bucket, err)
	}
	defer func() { _ = obj.Close() }()

	data, err := readBackupMetadata(obj)
	if err != nil {
		return nil, fmt.Errorf("reading object data for %s from bucket %s: %w", objectKey, bucket, err)
	}
	metadata, err := decodeBackupMetadata(data)
	if err != nil {
		return nil, fmt.Errorf("decoding table metadata for %s from bucket %s: %w", objectKey, bucket, err)
	}
	return metadata, nil
}
