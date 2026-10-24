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
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"io"
	"net/http"
	"path/filepath"
	"strings"
)

type S3Config struct {
	Bucket          string   `json:"bucket"`
	Key             string   `json:"key,omitempty"`
	Prefix          string   `json:"prefix,omitempty"`
	Suffixes        []string `json:"suffixes,omitempty"`
	Patterns        []string `json:"patterns,omitempty"`
	ExcludePatterns []string `json:"exclude_patterns,omitempty"`
	MaxObjects      int      `json:"max_objects,omitempty"`
}
type S3Connector struct{ Client *minio.Client }

func (c *S3Connector) Validate(_ context.Context, r Request) error {
	cfg, err := decodeSourceConfig[S3Config](r.Config)
	if err != nil {
		return err
	}
	if r.Mode != ImportModeSource || strings.TrimSpace(cfg.Bucket) == "" || cfg.MaxObjects < 0 || cfg.MaxObjects > maxImportSourceObjects || cfg.Key != "" && cfg.Prefix != "" {
		return fmt.Errorf("%w: invalid S3 selection", ErrInvalidInput)
	}
	if c.Client == nil {
		return fmt.Errorf("S3 client required")
	}
	return nil
}
func (c *S3Connector) Enumerate(ctx context.Context, r Request) (Inventory, error) {
	if err := c.Validate(ctx, r); err != nil {
		return Inventory{}, err
	}
	cfg, _ := decodeSourceConfig[S3Config](r.Config)
	out := Inventory{Authoritative: !r.Preview && len(r.RetrySourceKeys) == 0 && cfg.MaxObjects == 0, BatchObjects: 1, BatchBytes: maxImportSourceObjectBytes}
	limit := maxImportSourceObjects
	if cfg.MaxObjects > 0 {
		limit = cfg.MaxObjects
	}
	if r.Preview {
		limit = 1
	}
	retry := exactSourceKeySet(r.RetrySourceKeys)
	add := func(info minio.ObjectInfo, listed bool) error {
		if !sourceObjectAllowed(info.Key, cfg.Suffixes, cfg.Patterns, cfg.ExcludePatterns) {
			return nil
		}
		if len(r.RetrySourceKeys) > 0 {
			if _, ok := retry[info.Key]; !ok {
				return nil
			}
			delete(retry, info.Key)
		}
		if len(out.Objects) >= limit {
			return fmt.Errorf("%w: S3 inventory limit exceeded", ErrInvalidInput)
		}
		// ListObjects does not return version IDs. Resolve each selected entry
		// through HEAD and fence it against the listed identity before persisting
		// the version used for every subsequent download and authorization check.
		if listed {
			pinned, err := c.Client.StatObject(ctx, cfg.Bucket, info.Key, minio.StatObjectOptions{})
			if err != nil {
				return err
			}
			if strings.Trim(pinned.ETag, `"`) != strings.Trim(info.ETag, `"`) || pinned.Size != info.Size {
				return &RevisionChangedError{Expected: strings.Trim(info.ETag, `"`), Actual: strings.Trim(pinned.ETag, `"`)}
			}
			pinned.Key = info.Key
			info = pinned
		}
		rev := strings.Trim(info.ETag, `"`)
		if rev == "" || info.Size < 0 {
			return fmt.Errorf("invalid S3 revision or size")
		}
		uri := canonicalS3URI(cfg.Bucket, info.Key)
		out.Objects = append(out.Objects, Item{ID: "s3://" + strings.Trim(r.SourceNamespace, "/") + "/" + cfg.Bucket + "/" + info.Key, Path: info.Key, Filename: filepath.Base(info.Key), Revision: rev, URL: uri, Size: info.Size, SourceKind: ProviderS3, ReadMode: sourceReadBytes, ObjectIndex: len(out.Objects) + 1, Metadata: map[string]any{"source_namespace": r.SourceNamespace, "bucket": cfg.Bucket, "key": info.Key, "etag": rev, "size": info.Size, "version_id": info.VersionID}})
		return nil
	}
	if cfg.Key != "" {
		info, err := c.Client.StatObject(ctx, cfg.Bucket, cfg.Key, minio.StatObjectOptions{})
		if err != nil {
			response := minio.ToErrorResponse(err)
			if response.StatusCode != http.StatusNotFound {
				return Inventory{}, err
			}
			if len(retry) > 0 || r.Preview {
				return Inventory{}, fmt.Errorf("%w: selected S3 item missing", ErrInvalidInput)
			}
		} else {
			info.Key = cfg.Key
			if err := add(info, false); err != nil {
				return Inventory{}, err
			}
		}
	} else {
		listCtx, cancel := context.WithCancel(ctx)
		defer cancel()
		for info := range c.Client.ListObjects(listCtx, cfg.Bucket, minio.ListObjectsOptions{Prefix: cfg.Prefix, Recursive: true}) {
			if info.Err != nil {
				return Inventory{}, info.Err
			}
			if err := add(info, true); err != nil {
				return Inventory{}, err
			}
			if (cfg.MaxObjects > 0 || r.Preview) && len(out.Objects) >= limit {
				break
			}
		}
		if err := ctx.Err(); err != nil {
			return Inventory{}, err
		}
	}
	if len(retry) > 0 {
		return Inventory{}, fmt.Errorf("%w: selected S3 retries missing", ErrInvalidInput)
	}
	return out, nil
}
func (c *S3Connector) OpenSource(ctx context.Context, r Request, o Item) (io.ReadCloser, error) {
	cfg, err := decodeSourceConfig[S3Config](r.Config)
	if err != nil {
		return nil, err
	}
	if err := validatePinnedS3Source(cfg, o); err != nil {
		return nil, err
	}
	opts := minio.GetObjectOptions{VersionID: configString(o.Metadata, "version_id")}
	if err := opts.SetMatchETag(o.Revision); err != nil {
		return nil, err
	}
	reader, info, _, err := (minio.Core{Client: c.Client}).GetObject(ctx, cfg.Bucket, o.Path, opts)
	if err != nil {
		return nil, err
	}
	if err := validateS3Response(o, info); err != nil {
		reader.Close()
		return nil, err
	}
	return reader, nil
}
func (c *S3Connector) Read(ctx context.Context, r Request, o Item) ([]byte, error) {
	reader, err := c.OpenSource(ctx, r, o)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	data, err := readBounded(reader)
	if err == nil && int64(len(data)) != o.Size {
		err = fmt.Errorf("S3 source length changed")
	}
	return data, err
}
func (c *S3Connector) CheckAccess(ctx context.Context, r Request, o Item) error {
	cfg, err := decodeSourceConfig[S3Config](r.Config)
	if err != nil {
		return err
	}
	if err := validatePinnedS3Source(cfg, o); err != nil {
		return err
	}
	// A HEAD establishes object metadata access, but does not exercise SSE-KMS
	// decryption. Reauthorize cached plaintext through the same GET data plane
	// as a fresh download. Request one byte for nonempty objects, and an ordinary
	// empty GET for zero-length objects (a range would return 416).
	opts := minio.GetObjectOptions{VersionID: configString(o.Metadata, "version_id")}
	if err := opts.SetMatchETag(o.Revision); err != nil {
		return err
	}
	if o.Size > 0 {
		if err := opts.SetRange(0, 0); err != nil {
			return err
		}
	}
	reader, info, headers, err := (minio.Core{Client: c.Client}).GetObject(ctx, cfg.Bucket, o.Path, opts)
	if err != nil {
		return err
	}
	defer reader.Close()
	if contentRange := headers.Get("Content-Range"); contentRange != "" {
		if o.Size <= 0 || contentRange != fmt.Sprintf("bytes 0-0/%d", o.Size) || info.Size != 1 {
			return fmt.Errorf("S3 access probe returned an invalid content range")
		}
		// The range describes the complete object's size; Content-Length is
		// only the one-byte payload. Version and ETag still come from this GET.
		info.Size = o.Size
	}
	if err := validateS3Response(o, info); err != nil {
		return err
	}
	// Read a byte to exercise content access, even on compatible servers which
	// defer errors to the body. Bound reads when a server ignores Range.
	data, err := io.ReadAll(io.LimitReader(reader, 1))
	if err != nil {
		return err
	}
	if o.Size > 0 && len(data) != 1 || o.Size == 0 && len(data) != 0 {
		return fmt.Errorf("S3 access probe returned an unexpected content length")
	}
	return nil
}
func (c *S3Connector) ValidateAbsentSources(context.Context, Request, []string) error { return nil }

func validatePinnedS3Source(cfg S3Config, o Item) error {
	if cfg.Bucket != configString(o.Metadata, "bucket") || o.Path == "" || o.Revision == "" || o.Size < 0 {
		return fmt.Errorf("invalid pinned S3 source")
	}
	return nil
}

func validateS3Response(o Item, info minio.ObjectInfo) error {
	version := configString(o.Metadata, "version_id")
	// Empty version IDs denote unversioned objects (including old manifests).
	// A versioned response cannot prove access to those snapshots, even when
	// the replacement happens to have the same ETag and size. The literal S3
	// version ID "null" is retained and sent explicitly like any other ID.
	if info.VersionID != version {
		return fmt.Errorf("S3 version changed: expected %q, got %q; enumerate a fresh snapshot", version, info.VersionID)
	}
	if strings.Trim(info.ETag, `"`) != o.Revision || info.Size != o.Size {
		return &RevisionChangedError{Expected: o.Revision, Actual: strings.Trim(info.ETag, `"`)}
	}
	return nil
}
