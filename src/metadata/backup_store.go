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
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	json "github.com/antflydb/antfly/pkg/libaf/json"
	"github.com/antflydb/antfly/src/common"
	"github.com/antflydb/antfly/src/store"
	"github.com/minio/minio-go/v7"
)

// backupStore abstracts reading and writing backup metadata to either
// local filesystem or S3-compatible object storage.
type backupStore interface {
	WriteMetadata(ctx context.Context, id string, table *store.Table) error
	ReadMetadata(ctx context.Context, id string) (*store.Table, error)
}

// newBackupStore returns a backupStore for the given location.
// Locations starting with "s3://" use S3; all others use the local filesystem.
func newBackupStore(location string, s3Config *common.S3Info) backupStore {
	if strings.HasPrefix(location, "s3://") {
		return &s3BackupStore{location: location, s3Config: s3Config}
	}
	return &fileBackupStore{location: location}
}

// fileBackupStore reads/writes backup metadata to the local filesystem.
type fileBackupStore struct {
	location string
}

func (s *fileBackupStore) resolveAndValidate(id string) (string, error) {
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

func (s *fileBackupStore) WriteMetadata(_ context.Context, id string, table *store.Table) error {
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return err
	}
	file, err := os.Create(filePath) //#nosec G304,G703 -- path validated by resolveAndValidate
	if err != nil {
		return fmt.Errorf("creating file: %w", err)
	}
	defer func() { _ = file.Close() }()
	if err := json.NewEncoder(file).Encode(table); err != nil {
		return fmt.Errorf("encoding table metadata to JSON: %w", err)
	}
	return nil
}

func (s *fileBackupStore) ReadMetadata(_ context.Context, id string) (*store.Table, error) {
	filePath, err := s.resolveAndValidate(id)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filePath) //#nosec G304 -- path validated by resolveAndValidate
	if err != nil {
		return nil, fmt.Errorf("reading metadata file %s: %w", filePath, err)
	}
	var table store.Table
	if err := json.Unmarshal(data, &table); err != nil {
		return nil, fmt.Errorf("unmarshalling table metadata from %s: %w", filePath, err)
	}
	return &table, nil
}

// s3BackupStore reads/writes backup metadata to an S3-compatible object store.
type s3BackupStore struct {
	location string
	s3Config *common.S3Info
}

func (s *s3BackupStore) WriteMetadata(ctx context.Context, id string, table *store.Table) error {
	bucket, prefix, err := common.ParseS3URL(s.location)
	if err != nil {
		return fmt.Errorf("parsing bucket URL: %w", err)
	}
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
	if err := json.NewEncoder(&b).Encode(table); err != nil {
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

func (s *s3BackupStore) ReadMetadata(ctx context.Context, id string) (*store.Table, error) {
	bucket, prefix, err := common.ParseS3URL(s.location)
	if err != nil {
		return nil, fmt.Errorf("parsing bucket URL: %w", err)
	}
	minioClient, err := s.s3Config.NewMinioClient()
	if err != nil {
		return nil, fmt.Errorf("creating S3 client: %w", err)
	}
	objectKey := id + "-metadata.json"
	if prefix != "" {
		objectKey = path.Join(prefix, objectKey)
	}
	obj, err := minioClient.GetObject(ctx, bucket, objectKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, fmt.Errorf("getting object %s from bucket %s: %w", objectKey, bucket, err)
	}
	defer func() { _ = obj.Close() }()

	data, err := io.ReadAll(obj)
	if err != nil {
		return nil, fmt.Errorf("reading object data for %s from bucket %s: %w", objectKey, bucket, err)
	}
	var table store.Table
	if err := json.Unmarshal(data, &table); err != nil {
		return nil, fmt.Errorf("unmarshalling table metadata for %s from bucket %s: %w", objectKey, bucket, err)
	}
	return &table, nil
}
