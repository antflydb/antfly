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
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/antflydb/antfly/go/pkg/antfly/src/common"
	"github.com/antflydb/antfly/go/pkg/antfly/src/store"
	json "github.com/antflydb/antfly/go/pkg/libaf/json"
	"github.com/minio/minio-go/v7"
)

// backupStore abstracts reading and writing backup metadata to either
// local filesystem or S3-compatible object storage.
type backupStore interface {
	EnsureMetadataAbsent(ctx context.Context, id string) error
	ReserveBackupID(ctx context.Context, id string) error
	WriteMetadata(ctx context.Context, id string, table *store.Table, format common.BackupFormat) error
	ReadMetadata(ctx context.Context, id string) (*store.Table, common.BackupFormat, error)
	ResolvedLocation() string
}

const (
	backupMetadataVersion  = 1
	maxBackupMetadataBytes = 16 * 1024 * 1024
)

var (
	ErrBackupAlreadyExists    = common.ErrBackupAlreadyExists
	ErrBackupMetadataTooLarge = errors.New("backup metadata exceeds the 16 MiB limit")
)

type boundedWriter struct {
	writer    io.Writer
	remaining int64
}

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
	Version uint32              `json:"version"`
	Format  common.BackupFormat `json:"format"`
	Table   *store.Table        `json:"table"`
}

func newBackupMetadata(table *store.Table, format common.BackupFormat) (*backupMetadata, error) {
	if table == nil {
		return nil, fmt.Errorf("table metadata is required")
	}
	format = common.NormalizeBackupFormat(format)
	switch format {
	case common.BackupFormatNative, common.BackupFormatPortable:
	default:
		return nil, fmt.Errorf("unsupported backup format %q", format)
	}
	return &backupMetadata{
		Version: backupMetadataVersion,
		Format:  format,
		Table:   table,
	}, nil
}

func decodeBackupMetadata(data []byte) (*store.Table, common.BackupFormat, error) {
	var metadata backupMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, "", fmt.Errorf("unmarshalling backup metadata: %w", err)
	}
	if metadata.Version != backupMetadataVersion {
		return nil, "", fmt.Errorf("unsupported backup metadata version %d", metadata.Version)
	}
	if metadata.Table == nil {
		return nil, "", fmt.Errorf("backup metadata is missing table")
	}
	switch metadata.Format {
	case common.BackupFormatNative, common.BackupFormatPortable:
	default:
		return nil, "", fmt.Errorf("unsupported backup format %q", metadata.Format)
	}
	return metadata.Table, metadata.Format, nil
}

func writeJSONFileAtomically(ctx context.Context, filePath string, value any) error {
	if err := ctx.Err(); err != nil {
		return err
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
	writer := &boundedWriter{writer: file, remaining: maxBackupMetadataBytes}
	if err := json.NewEncoder(writer).Encode(value); err != nil {
		return fmt.Errorf("encoding metadata to JSON: %w", err)
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
	if err := os.Link(tempPath, filePath); err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, filepath.Base(filePath))
		}
		return fmt.Errorf("publishing metadata file: %w", err)
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

func (s *fileBackupStore) ReserveBackupID(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.EnsureMetadataAbsent(ctx, id); err != nil {
		return err
	}
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return err
	}
	reservationPath := strings.TrimSuffix(filePath, "-metadata.json") + "-reservation"
	if err := os.MkdirAll(filepath.Dir(reservationPath), 0o750); err != nil {
		return fmt.Errorf("creating backup metadata directory: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.OpenFile(
		reservationPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o600,
	) //#nosec G304 -- path validated by resolveAndValidate
	if err != nil {
		if os.IsExist(err) {
			return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
		}
		return fmt.Errorf("reserving backup ID: %w", err)
	}
	if _, err := file.WriteString("reserved\n"); err != nil {
		_ = file.Close()
		return fmt.Errorf("writing backup reservation: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("syncing backup reservation: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing backup reservation: %w", err)
	}
	dir, err := os.Open(filepath.Dir(reservationPath)) //#nosec G304 -- authorized backup directory
	if err != nil {
		return fmt.Errorf("opening backup directory for sync: %w", err)
	}
	defer func() { _ = dir.Close() }()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("syncing backup directory: %w", err)
	}
	return nil
}

func (s *fileBackupStore) WriteMetadata(
	ctx context.Context,
	id string,
	table *store.Table,
	format common.BackupFormat,
) error {
	metadata, err := newBackupMetadata(table, format)
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
) (*store.Table, common.BackupFormat, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return nil, "", err
	}
	file, err := os.Open(filePath) //#nosec G304 -- path validated by resolveAndValidate
	if err != nil {
		return nil, "", fmt.Errorf("reading metadata file %s: %w", filePath, err)
	}
	defer func() { _ = file.Close() }()
	data, err := readBackupMetadata(file)
	if err != nil {
		return nil, "", fmt.Errorf("reading metadata file %s: %w", filePath, err)
	}
	table, format, err := decodeBackupMetadata(data)
	if err != nil {
		return nil, "", fmt.Errorf("decoding table metadata from %s: %w", filePath, err)
	}
	return table, format, nil
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

func (s *s3BackupStore) ReserveBackupID(ctx context.Context, id string) error {
	if err := s.EnsureMetadataAbsent(ctx, id); err != nil {
		return err
	}
	client, err := s.s3Config.EnsureBucket(ctx)
	if err != nil {
		return err
	}
	payload := strings.NewReader("reserved\n")
	options := minio.PutObjectOptions{ContentType: "text/plain"}
	options.SetMatchETagExcept("*")
	if _, err := client.PutObject(
		ctx,
		s.s3Config.Bucket,
		s.reservationKey(id),
		payload,
		int64(payload.Len()),
		options,
	); err != nil {
		if common.IsS3CreateConflict(err) {
			return fmt.Errorf("%w: %s", ErrBackupAlreadyExists, id)
		}
		return fmt.Errorf("reserving backup ID: %w", err)
	}
	return nil
}

func (s *s3BackupStore) WriteMetadata(
	ctx context.Context,
	id string,
	table *store.Table,
	format common.BackupFormat,
) error {
	if err := common.ValidateBackupID(id); err != nil {
		return err
	}
	metadata, err := newBackupMetadata(table, format)
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
) (*store.Table, common.BackupFormat, error) {
	if err := common.ValidateBackupID(id); err != nil {
		return nil, "", err
	}
	bucket := s.s3Config.Bucket
	minioClient, err := s.s3Config.NewMinioClient()
	if err != nil {
		return nil, "", fmt.Errorf("creating S3 client: %w", err)
	}
	objectKey := s.objectKey(id)
	obj, err := minioClient.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("getting object %s from bucket %s: %w", objectKey, bucket, err)
	}
	defer func() { _ = obj.Close() }()

	data, err := readBackupMetadata(obj)
	if err != nil {
		return nil, "", fmt.Errorf("reading object data for %s from bucket %s: %w", objectKey, bucket, err)
	}
	table, format, err := decodeBackupMetadata(data)
	if err != nil {
		return nil, "", fmt.Errorf("decoding table metadata for %s from bucket %s: %w", objectKey, bucket, err)
	}
	return table, format, nil
}
