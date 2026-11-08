// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sources

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/minio/minio-go/v7"
)

// migrateDestination is the one small surface MigrateLocalCopy needs from a
// bucket: whether a key already exists (and its size), an exactly-once
// create, and a full read-back for independent digest verification. S3 and
// GCS share every byte of copy/verify/resume logic in migrate_local.go
// through this; only how each talks to its bucket differs.
type migrateDestination interface {
	// stat reports an existing object's size, or exists=false if there is
	// none. It never treats a provider's transient error as "missing".
	stat(ctx context.Context, key string) (size int64, exists bool, err error)
	// create uploads the complete object under key. It must not overwrite
	// an existing object; a provider that can enforce that atomically
	// (GCS's generation precondition) returns errMigrateDestinationExists
	// when it does, which the caller treats as a benign race rather than
	// this object's own failure.
	create(ctx context.Context, key string, data []byte, contentType string) error
	// read returns the complete object's current bytes.
	read(ctx context.Context, key string) ([]byte, error)
}

// errMigrateDestinationExists signals that create lost a race: something
// else created the same content-addressed key between this run's existence
// check and its own create. The object is left for verification (and, if
// that still fails, for a later run) rather than being reported as this
// object's own corruption.
var errMigrateDestinationExists = errors.New("destination object already exists")

// s3Destination is the S3-compatible migrateDestination, backed by the same
// minio.Client the S3 source connector uses. It has no atomic create-if-
// absent precondition in wide use across S3-compatible servers, so resuming
// relies on the existence check in migrateCopyOne rather than on create
// itself; concurrent migrate-local runs against the same destination are not
// supported.
type s3Destination struct {
	client *minio.Client
	bucket string
}

func (d *s3Destination) stat(ctx context.Context, key string) (int64, bool, error) {
	info, err := d.client.StatObject(ctx, d.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == http.StatusNotFound {
			return 0, false, nil
		}
		return 0, false, err
	}
	return info.Size, true, nil
}

func (d *s3Destination) create(ctx context.Context, key string, data []byte, contentType string) error {
	// A seekable *bytes.Reader lets minio-go send one ordinary signed PUT
	// instead of chunked/streaming transfer encoding, which not every
	// S3-compatible endpoint parses identically; DisableContentSha256
	// mirrors that choice for the same reason.
	_, err := d.client.PutObject(ctx, d.bucket, key, bytes.NewReader(data), int64(len(data)), minio.PutObjectOptions{ContentType: contentType, DisableContentSha256: true})
	return err
}

func (d *s3Destination) read(ctx context.Context, key string) ([]byte, error) {
	obj, err := d.client.GetObject(ctx, d.bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	return io.ReadAll(io.LimitReader(obj, maxLocalMediaBytes+1))
}

// gcsDestination is the GCS migrateDestination, talking to the same JSON API
// base the GCS source connector reads from, over an HTTP client built the
// same way (GoogleCredentialsForGCSWrite instead of the read-only scope every
// source connection requests, then GoogleHTTPClient). Every create passes
// ifGenerationMatch=0, so GCS itself refuses to let two writers create the
// same content-addressed key: the second gets back a precondition failure,
// not a silently overwritten object.
type gcsDestination struct {
	client *http.Client
	bucket string
	// apiBase and uploadBase default to the real GCS JSON API when empty;
	// tests point them at a local fake JSON-API server instead.
	apiBase, uploadBase string
}

// gcsJSONAPIBase and gcsUploadAPIBase are vars, not consts, so a test can
// point the whole package at a local JSON-API fake (e.g. fake-gcs-server)
// without threading an override through every call site.
var gcsJSONAPIBase = "https://storage.googleapis.com/storage/v1"
var gcsUploadAPIBase = "https://storage.googleapis.com/upload/storage/v1"

func (d *gcsDestination) jsonBase() string {
	if d.apiBase != "" {
		return d.apiBase
	}
	return gcsJSONAPIBase
}
func (d *gcsDestination) uploadBaseURL() string {
	if d.uploadBase != "" {
		return d.uploadBase
	}
	return gcsUploadAPIBase
}

func (d *gcsDestination) stat(ctx context.Context, key string) (int64, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.jsonBase()+"/b/"+url.PathEscape(d.bucket)+"/o/"+url.PathEscape(key), nil)
	if err != nil {
		return 0, false, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, false, googleRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return 0, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		return 0, false, googleResponseError(resp)
	}
	var meta struct {
		Size string `json:"size"`
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, false, err
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return 0, false, err
	}
	size, err := strconv.ParseInt(meta.Size, 10, 64)
	if err != nil || size < 0 {
		return 0, false, fmt.Errorf("invalid GCS object size")
	}
	return size, true, nil
}

// create sends a JSON API "multipart" upload (one metadata part naming the
// object, one media part with its bytes) rather than a resumable session:
// every object here is already bounded and fully buffered (the local store's
// own 256 MiB object cap), so one request needs no resumable session to
// recover from, and multipart is the simpler, single-round-trip mechanism.
func (d *gcsDestination) create(ctx context.Context, key string, data []byte, contentType string) error {
	metaJSON, err := json.Marshal(struct {
		Name string `json:"name"`
	}{key})
	if err != nil {
		return err
	}
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)
	metaPart, err := writer.CreatePart(map[string][]string{"Content-Type": {"application/json; charset=UTF-8"}})
	if err != nil {
		return err
	}
	if _, err := metaPart.Write(metaJSON); err != nil {
		return err
	}
	mediaPart, err := writer.CreatePart(map[string][]string{"Content-Type": {contentType}})
	if err != nil {
		return err
	}
	if _, err := mediaPart.Write(data); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.uploadBaseURL()+"/b/"+url.PathEscape(d.bucket)+"/o?uploadType=multipart&ifGenerationMatch=0", &body)
	if err != nil {
		return err
	}
	// multipart/related, not multipart/form-data (Writer.FormDataContentType's
	// label): GCS's multipart upload has no form fields, just ordered parts.
	req.Header.Set("Content-Type", "multipart/related; boundary="+writer.Boundary())
	resp, err := d.client.Do(req)
	if err != nil {
		return googleRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusPreconditionFailed {
		return errMigrateDestinationExists
	}
	if resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("GCS upload denied (HTTP 403); the connection's credential needs GCS write scope (devstorage.read_write), not just the read-only scope ordinary GCS sources use: %w", googleResponseError(resp))
	}
	if resp.StatusCode != http.StatusOK {
		return googleResponseError(resp)
	}
	return nil
}

func (d *gcsDestination) read(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.jsonBase()+"/b/"+url.PathEscape(d.bucket)+"/o/"+url.PathEscape(key)+"?alt=media", nil)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, googleRequestError(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, googleResponseError(resp)
	}
	return io.ReadAll(io.LimitReader(resp.Body, maxLocalMediaBytes+1))
}

// migrateGCSWriteClient builds a GCS HTTP client scoped for migrate-local's
// destination (read/write), mirroring CredentialFactory's "credentials"
// secret resolution for a GCS connection exactly, except for the scope
// itself. resolve is nil in live mode (env-var credentials only; a
// connection using the service's own secret store needs the in-process,
// stopped-service repoint path instead).
func migrateGCSWriteClient(ctx context.Context, connection Connection, resolve func(context.Context, string) (string, error)) (*http.Client, error) {
	if connection.Provider != ProviderGCS {
		return nil, fmt.Errorf("%w: connection is not a GCS connection", ErrInvalidInput)
	}
	raw, err := migrateConnectionSecret(ctx, connection, "credentials", connection.CredentialsEnv, resolve)
	if err != nil {
		return nil, err
	}
	credentials, err := GoogleCredentialsForGCSWrite(ctx, raw)
	if err != nil {
		return nil, err
	}
	client := GoogleHTTPClient(ctx, credentials)
	client.Timeout = 60 * time.Second
	return client, nil
}

// migrateConnectionSecret resolves one credential field exactly like
// CredentialFactory does: a secret-ref through resolve, or an environment
// variable by name.
func migrateConnectionSecret(ctx context.Context, connection Connection, field, envName string, resolve func(context.Context, string) (string, error)) (string, error) {
	if ref := connection.SecretRefs[field]; ref != "" {
		if resolve == nil {
			return "", fmt.Errorf("runtime secret resolver required for connection field %q", field)
		}
		value, err := resolve(ctx, ref)
		if err != nil || value == "" {
			return "", fmt.Errorf("credential secret for %q is unavailable", field)
		}
		return value, nil
	}
	if envName == "" {
		return "", fmt.Errorf("credential environment reference required for %q", field)
	}
	value, ok := os.LookupEnv(envName)
	if !ok || value == "" {
		return "", fmt.Errorf("credential environment variable %s is not set", envName)
	}
	return value, nil
}

// migrateOpenDestination builds the migrateDestination for toConnection's
// provider. factory resolves ordinary (read-scoped) source connectors, used
// for S3; GCS needs its own write-scoped client instead, built by
// migrateGCSWriteClient.
func migrateOpenDestination(ctx context.Context, factory Factory, toConnection Connection, bucket string, resolveSecret func(context.Context, string) (string, error)) (migrateDestination, error) {
	switch toConnection.Provider {
	case ProviderS3:
		connector, err := factory(ctx, toConnection)
		if err != nil {
			return nil, err
		}
		s3, ok := connector.(*S3Connector)
		if !ok {
			return nil, fmt.Errorf("connection did not resolve to an S3 client")
		}
		return &s3Destination{client: s3.Client, bucket: bucket}, nil
	case ProviderGCS:
		client, err := migrateGCSWriteClient(ctx, toConnection, resolveSecret)
		if err != nil {
			return nil, err
		}
		return &gcsDestination{client: client, bucket: bucket}, nil
	default:
		return nil, fmt.Errorf("%w: migrate-local only supports S3-compatible and GCS destinations (connection has provider %q)", ErrInvalidInput, toConnection.Provider)
	}
}
