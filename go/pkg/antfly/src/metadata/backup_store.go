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
	WriteMetadata(ctx context.Context, id string, table *store.Table, format common.BackupFormat) error
	ReadMetadata(ctx context.Context, id string) (*store.Table, common.BackupFormat, error)
}

const backupMetadataVersion = 1

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

func writeJSONFileAtomically(filePath string, value any) error {
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
	if err := json.NewEncoder(file).Encode(value); err != nil {
		return fmt.Errorf("encoding metadata to JSON: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("syncing metadata file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("closing metadata file: %w", err)
	}
	if err := os.Rename(tempPath, filePath); err != nil {
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

func (s *fileBackupStore) WriteMetadata(
	_ context.Context,
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
	return writeJSONFileAtomically(filePath, metadata)
}

func (s *fileBackupStore) ReadMetadata(
	_ context.Context,
	id string,
) (*store.Table, common.BackupFormat, error) {
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(filePath) //#nosec G304 -- path validated by resolveAndValidate
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
	prefix := s.s3Config.Prefix
	minioClient, err := s.s3Config.NewMinioClient()
	if err != nil {
		return fmt.Errorf("creating S3 client: %w", err)
	}
	if ok, err := minioClient.BucketExists(ctx, bucket); err != nil {
		return fmt.Errorf("checking if bucket %s exists: %w", bucket, err)
	} else if !ok {
		return fmt.Errorf("bucket %s does not exist", bucket)
	}

	var b bytes.Buffer
	if err := json.NewEncoder(&b).Encode(metadata); err != nil {
		return fmt.Errorf("encoding table metadata to JSON: %w", err)
	}
	objectKey := id + "-metadata.json"
	if prefix != "" {
		objectKey = path.Join(prefix, objectKey)
	}
	if _, err := minioClient.PutObject(ctx, bucket, objectKey, &b, int64(b.Len()), minio.PutObjectOptions{}); err != nil {
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
	prefix := s.s3Config.Prefix
	minioClient, err := s.s3Config.NewMinioClient()
	if err != nil {
		return nil, "", fmt.Errorf("creating S3 client: %w", err)
	}
	objectKey := id + "-metadata.json"
	if prefix != "" {
		objectKey = path.Join(prefix, objectKey)
	}
	obj, err := minioClient.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, "", fmt.Errorf("getting object %s from bucket %s: %w", objectKey, bucket, err)
	}
	defer func() { _ = obj.Close() }()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, "", fmt.Errorf("reading object data for %s from bucket %s: %w", objectKey, bucket, err)
	}
	table, format, err := decodeBackupMetadata(data)
	if err != nil {
		return nil, "", fmt.Errorf("decoding table metadata for %s from bucket %s: %w", objectKey, bucket, err)
	}
	return table, format, nil
}
