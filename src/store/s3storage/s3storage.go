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
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2/objstorage/remote"
	"github.com/minio/minio-go/v7"
)

// S3Storage implements the remote.Storage interface for Pebble
// to store SST files in S3-compatible object storage.
type S3Storage struct {
	client     *minio.Client
	bucketName string
	prefix     string
}

// NewS3Storage creates a new S3 storage backend for Pebble.
// The prefix is used as a directory prefix for all objects (e.g., "antfly/shard-123/").
func NewS3Storage(client *minio.Client, bucketName, prefix string) (*S3Storage, error) {
	if client == nil {
		return nil, errors.New("minio client cannot be nil")
	}
	if bucketName == "" {
		return nil, errors.New("bucket name cannot be empty")
	}

	// Ensure bucket exists
	ctx := context.Background()
	exists, err := client.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("checking if bucket exists: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("bucket %s does not exist", bucketName)
	}

	return &S3Storage{
		client:     client,
		bucketName: bucketName,
		prefix:     strings.TrimSuffix(prefix, "/"),
	}, nil
}

// fullPath returns the full S3 object path including prefix.
// Uses string concatenation (not path.Join) to avoid path cleaning/normalization
// that could corrupt object names, and to match the strip logic in List.
func (s *S3Storage) fullPath(objectName string) string {
	if s.prefix == "" {
		return objectName
	}
	return s.prefix + "/" + objectName
}

// CreateObject implements remote.Storage.CreateObject.
// Data is streamed to S3 via io.Pipe rather than buffered in memory,
// so arbitrarily large sstables can be uploaded without OOM risk.
func (s *S3Storage) CreateObject(objectName string) (io.WriteCloser, error) {
	pr, pw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	w := &s3Writer{pw: pw, done: make(chan error, 1), cancel: cancel}
	go func() {
		_, err := s.client.PutObject(
			ctx,
			s.bucketName,
			s.fullPath(objectName),
			pr,
			-1, // unknown size — MinIO uses multipart upload automatically
			minio.PutObjectOptions{ContentType: "application/octet-stream"},
		)
		_ = pr.CloseWithError(err)
		w.done <- err
	}()
	return w, nil
}

// ReadObject implements remote.Storage.ReadObject.
func (s *S3Storage) ReadObject(
	ctx context.Context,
	objectName string,
) (remote.ObjectReader, int64, error) {
	fullPath := s.fullPath(objectName)

	// Get object info for size
	objInfo, err := s.client.StatObject(ctx, s.bucketName, fullPath, minio.StatObjectOptions{})
	if err != nil {
		return nil, 0, fmt.Errorf("stat object %s: %w", objectName, err)
	}

	reader := &s3Reader{
		storage:    s,
		objectName: objectName,
		size:       objInfo.Size,
	}

	return reader, objInfo.Size, nil
}

// Size implements remote.Storage.Size.
func (s *S3Storage) Size(objectName string) (int64, error) {
	ctx := context.Background()
	fullPath := s.fullPath(objectName)

	objInfo, err := s.client.StatObject(ctx, s.bucketName, fullPath, minio.StatObjectOptions{})
	if err != nil {
		return 0, fmt.Errorf("stat object %s: %w", objectName, err)
	}

	return objInfo.Size, nil
}

// IsNotExistError implements remote.Storage.IsNotExistError.
func (s *S3Storage) IsNotExistError(err error) bool {
	if err == nil {
		return false
	}
	// Minio returns specific error responses for not found
	errResp := minio.ToErrorResponse(err)
	return errResp.Code == "NoSuchKey" || errResp.Code == "NotFound"
}

// Delete implements remote.Storage.Delete.
// Used by Pebble's GC to clean up unreferenced sstables.
func (s *S3Storage) Delete(objectName string) error {
	ctx := context.Background()
	fullPath := s.fullPath(objectName)

	if err := s.client.RemoveObject(ctx, s.bucketName, fullPath, minio.RemoveObjectOptions{}); err != nil {
		return fmt.Errorf("delete object %s: %w", objectName, err)
	}
	return nil
}

// List implements remote.Storage.List.
// Returns object names with both the storage prefix and the caller's prefix stripped,
// per the remote.Storage interface contract: List("a/", "") on objects [a/x, a/y]
// returns [x, y].
func (s *S3Storage) List(prefix, delimiter string) ([]string, error) {
	ctx := context.Background()
	fullPrefix := s.fullPath(prefix)

	var objects []string
	objectCh := s.client.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Prefix:    fullPrefix,
		Recursive: delimiter == "",
	})

	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("listing objects with prefix %s: %w", prefix, object.Err)
		}
		// Strip the full prefix (storage prefix + caller's prefix) from results.
		// fullPrefix already includes both, so a single TrimPrefix suffices.
		objName := strings.TrimPrefix(object.Key, fullPrefix)
		objects = append(objects, objName)
	}

	return objects, nil
}

// Close implements remote.Storage.Close.
func (s *S3Storage) Close() error {
	return nil
}

// uploadTimeout is the maximum time to wait for an S3 upload to complete
// after the writer is closed. This prevents hanging on shutdown if S3 is slow.
const uploadTimeout = 5 * time.Minute

// s3Writer streams data to S3 via io.Pipe, avoiding buffering entire sstables
// in memory. The background goroutine runs PutObject reading from the pipe;
// Close signals EOF and waits for the upload to finish.
type s3Writer struct {
	pw     *io.PipeWriter
	done   chan error
	cancel context.CancelFunc
	closed bool
}

func (w *s3Writer) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

func (w *s3Writer) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.pw.Close() // signal EOF to the PutObject goroutine
	select {
	case err := <-w.done:
		return err
	case <-time.After(uploadTimeout):
		w.cancel() // cancel the PutObject context so the goroutine doesn't leak
		return errors.New("timed out waiting for S3 upload to complete")
	}
}

// s3Reader implements remote.ObjectReader for reading objects from S3.
type s3Reader struct {
	storage    *S3Storage
	objectName string
	size       int64
}

func (r *s3Reader) ReadAt(ctx context.Context, p []byte, offset int64) error {
	fullPath := r.storage.fullPath(r.objectName)

	opts := minio.GetObjectOptions{}
	if err := opts.SetRange(offset, offset+int64(len(p))-1); err != nil {
		return fmt.Errorf("setting range: %w", err)
	}

	obj, err := r.storage.client.GetObject(ctx, r.storage.bucketName, fullPath, opts)
	if err != nil {
		return fmt.Errorf("getting object range: %w", err)
	}
	defer func() { _ = obj.Close() }()

	_, err = io.ReadFull(obj, p)
	return err
}

func (r *s3Reader) Close() error {
	return nil
}

func (r *s3Reader) Size() int64 {
	return r.size
}

// Verify interface implementations at compile time
var (
	_ remote.Storage      = (*S3Storage)(nil)
	_ io.WriteCloser      = (*s3Writer)(nil)
	_ remote.ObjectReader = (*s3Reader)(nil)
)
