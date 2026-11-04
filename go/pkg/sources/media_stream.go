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
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/minio/minio-go/v7"
)

// MediaRange names an exact range in an immutable provider representation.
// Length is always positive; it never means an unbounded read.
type MediaRange struct{ Offset, Length int64 }

func (r MediaRange) validate(size int64) error {
	if r.Offset < 0 || r.Length <= 0 || r.Offset > size || r.Length > size-r.Offset {
		return fmt.Errorf("%w: invalid media range", ErrInvalidInput)
	}
	return nil
}

// MediaConnector streams original bytes. A successful Close must confirm the
// complete requested range and any provider revision/checksum postconditions.
// A caller must check both the copy result and Close before accepting bytes.
type MediaConnector interface {
	CheckMedia(context.Context, Request, Item) error
	OpenMedia(context.Context, Request, Item, MediaRange) (io.ReadCloser, error)
}

// StableMediaConnector promises the entire returned range is fenced to one
// provider version before its first byte. It permits one open response across
// bounded windows. Mutable Drive representations deliberately do not opt in.
type StableMediaConnector interface {
	MediaConnector
	StableMediaRanges() bool
}

// MediaContentScope identifies the provider byte namespace independently of
// credentials and publication policy. Custom media transports must provide an
// equally stable namespace before a host can reuse verified byte digests.
func (c *S3Connector) MediaContentScope() string {
	return ProviderS3 + "|" + c.Client.EndpointURL().String()
}
func (c *GoogleConnector) MediaContentScope() string {
	return c.ProviderName + "|" + c.base()
}

func (*S3Connector) StableMediaRanges() bool       { return true }
func (c *GoogleConnector) StableMediaRanges() bool { return c.ProviderName == ProviderGCS }

// Check injected policy as well as streaming authority. Exact native types
// already combine these checks; an embedding wrapper may override CheckAccess
// while promoting the native CheckMedia method.
func checkMediaAccess(ctx context.Context, connector Connector, media MediaConnector, request Request, item Item) error {
	switch connector.(type) {
	case *S3Connector, *GoogleConnector, *driveMediaConnector:
	default:
		if err := connector.CheckAccess(ctx, request, item); err != nil {
			return err
		}
	}
	return media.CheckMedia(ctx, request, item)
}

type mediaBody struct {
	checksum         hash.Hash
	expectedChecksum string
	body             io.ReadCloser
	remaining        int64
	verify           func() error
	once             sync.Once
	closeErr         error
}

func (b *mediaBody) Read(p []byte) (int, error) {
	if b.remaining == 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.body.Read(p)
	b.remaining -= int64(n)
	if b.checksum != nil {
		_, _ = b.checksum.Write(p[:n])
	}
	if err == io.EOF && b.remaining != 0 {
		err = io.ErrUnexpectedEOF
	}
	return n, err
}
func (b *mediaBody) Close() error {
	b.once.Do(func() {
		b.closeErr = b.body.Close()
		if b.remaining != 0 {
			b.closeErr = errors.Join(b.closeErr, io.ErrUnexpectedEOF)
			return
		}
		if b.checksum != nil && hex.EncodeToString(b.checksum.Sum(nil)) != b.expectedChecksum {
			b.closeErr = errors.Join(b.closeErr, fmt.Errorf("Drive source checksum mismatch"))
			return
		}
		if b.verify != nil {
			b.closeErr = errors.Join(b.closeErr, b.verify())
		}
	})
	return b.closeErr
}

func mediaProviderError(err error) error {
	if err == nil {
		return nil
	}
	response := minio.ToErrorResponse(err)
	if response.StatusCode == http.StatusPreconditionFailed || response.Code == "PreconditionFailed" || response.Code == "NoSuchVersion" {
		return &RevisionChangedError{Expected: "pinned source", Actual: "provider version changed"}
	}
	var google *GoogleHTTPError
	if errors.As(err, &google) && (google.Status == http.StatusPreconditionFailed || google.Status == http.StatusConflict) {
		return &RevisionChangedError{Expected: "pinned source", Actual: "provider version changed"}
	}
	return err
}

func (c *S3Connector) CheckMedia(ctx context.Context, r Request, item Item) error {
	return mediaProviderError(c.CheckAccess(ctx, r, item))
}
func (c *S3Connector) OpenMedia(ctx context.Context, r Request, item Item, span MediaRange) (io.ReadCloser, error) {
	cfg, err := decodeSourceConfig[S3Config](r.Config)
	if err != nil {
		return nil, err
	}
	if err := span.validate(item.Size); err != nil {
		return nil, err
	}
	if err := validatePinnedS3Source(cfg, item); err != nil {
		return nil, err
	}
	opts := minio.GetObjectOptions{VersionID: configString(item.Metadata, "version_id")}
	if err := opts.SetMatchETag(item.Revision); err != nil {
		return nil, err
	}
	ranged := span.Offset != 0 || span.Length != item.Size
	if ranged {
		if err := opts.SetRange(span.Offset, span.Offset+span.Length-1); err != nil {
			return nil, err
		}
	}
	body, info, headers, err := (minio.Core{Client: c.Client}).GetObject(ctx, cfg.Bucket, item.Path, opts)
	if err != nil {
		return nil, mediaProviderError(err)
	}
	if ranged {
		expected := fmt.Sprintf("bytes %d-%d/%d", span.Offset, span.Offset+span.Length-1, item.Size)
		if headers.Get("Content-Range") != expected || info.Size != span.Length {
			body.Close()
			return nil, fmt.Errorf("invalid provider content range")
		}
		info.Size = item.Size
	}
	if err := validateS3Response(item, info); err != nil {
		body.Close()
		return nil, mediaProviderError(err)
	}
	return &mediaBody{body: body, remaining: span.Length}, nil
}

func (c *GoogleConnector) CheckMedia(ctx context.Context, r Request, item Item) error {
	if c.ProviderName != ProviderGoogleDrive {
		return mediaProviderError(c.CheckAccess(ctx, r, item))
	}
	client, err := c.client(ctx, r)
	if err != nil {
		return err
	}
	// The same pinned token/metadata response proves content permission and
	// revision. Do not add a second metadata GET just to check its version.
	return mediaProviderError(c.checkDriveContentAccess(ctx, client, configString(item.Metadata, "file_id"), item.Revision))
}

func (c *GoogleConnector) OpenMedia(ctx context.Context, r Request, item Item, span MediaRange) (io.ReadCloser, error) {
	if configString(item.Metadata, "export_mime_type") != "" {
		if err := span.validate(item.Size); err != nil {
			return nil, err
		}
		if err := c.CheckMedia(ctx, r, item); err != nil {
			return nil, err
		}
		if c.ContentStore == nil || !ValidContentRef(item.ContentRef) {
			return nil, fmt.Errorf("native Drive export content storage unavailable")
		}
		data, found, err := c.ContentStore.Load(ctx, item.ContentRef)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("pinned Drive export is unavailable; synchronize this source again")
		}
		if int64(len(data)) != item.Size || fmt.Sprintf("%x", sha256.Sum256(data)) != configString(item.Metadata, "representation_sha256") {
			return nil, &RevisionChangedError{Expected: item.Revision, Actual: "export bytes changed"}
		}
		return io.NopCloser(bytes.NewReader(data[span.Offset : span.Offset+span.Length])), nil
	}
	if err := span.validate(item.Size); err != nil {
		return nil, err
	}
	client, err := c.client(ctx, r)
	if err != nil {
		return nil, err
	}
	cfg, err := decodeSourceConfig[GoogleConfig](r.Config)
	if err != nil {
		return nil, err
	}
	var path string
	var verify func() error
	if c.ProviderName == ProviderGCS {
		if configString(item.Metadata, "bucket") != cfg.Bucket {
			return nil, ErrInvalidInput
		}
		path = "/b/" + url.PathEscape(cfg.Bucket) + "/o/" + url.PathEscape(item.Path) + "?alt=media&generation=" + url.QueryEscape(item.Revision) + "&ifGenerationMatch=" + url.QueryEscape(item.Revision)
	} else {
		id := configString(item.Metadata, "file_id")
		if !googleFileID(id) || configString(item.Metadata, "export_mime_type") != "" {
			return nil, fmt.Errorf("%w: media requires an original binary Drive object", ErrInvalidInput)
		}
		if err := c.checkDriveRevision(ctx, client, id, item.Revision); err != nil {
			return nil, err
		}
		path = "/files/" + url.PathEscape(id) + "?alt=media&supportsAllDrives=true"
		verify = func() error { return c.checkDriveRevision(ctx, client, id, item.Revision) }
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base()+path, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept-Encoding", "identity")
	ranged := span.Offset != 0 || span.Length != item.Size
	if ranged {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", span.Offset, span.Offset+span.Length-1))
	}
	response, err := doGoogleStream(client, req)
	if err != nil {
		return nil, googleRequestError(err)
	}
	expectedStatus := http.StatusOK
	if ranged {
		expectedStatus = http.StatusPartialContent
	}
	if response.StatusCode != expectedStatus {
		defer response.Body.Close()
		return nil, mediaProviderError(googleResponseError(response))
	}
	if response.ContentLength != span.Length {
		response.Body.Close()
		return nil, fmt.Errorf("source length changed")
	}
	if ranged && response.Header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", span.Offset, span.Offset+span.Length-1, item.Size) {
		response.Body.Close()
		return nil, fmt.Errorf("invalid provider content range")
	}
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		response.Body.Close()
		return nil, fmt.Errorf("encoded media range is not byte stable")
	}
	if c.ProviderName == ProviderGCS && response.Header.Get("X-Goog-Generation") != item.Revision {
		response.Body.Close()
		return nil, &RevisionChangedError{Expected: item.Revision, Actual: response.Header.Get("X-Goog-Generation")}
	}
	body := &mediaBody{body: response.Body, remaining: span.Length, verify: verify}
	if c.ProviderName == ProviderGoogleDrive && !ranged {
		if expected := configString(item.Metadata, "md5_checksum"); expected != "" {
			body.checksum = md5.New()
			body.expectedChecksum = expected
		}
	}
	return body, nil
}

// parseMediaRanges bounds multipart work and validates ranges before provider
// acquisition. ServeContent still owns RFC conditional and response semantics.
func parseMediaRanges(header string, size int64) ([]MediaRange, error) {
	if header == "" {
		return []MediaRange{{0, size}}, nil
	}
	if !strings.HasPrefix(header, "bytes=") {
		return nil, ErrInvalidInput
	}
	parts := strings.Split(strings.TrimPrefix(header, "bytes="), ",")
	if len(parts) > 16 {
		return nil, ErrInvalidInput
	}
	var ranges []MediaRange
	for _, part := range parts {
		a, b, ok := strings.Cut(strings.TrimSpace(part), "-")
		if !ok {
			return nil, ErrInvalidInput
		}
		var offset, length int64
		if a == "" {
			suffix, err := strconv.ParseInt(b, 10, 64)
			if err != nil || suffix <= 0 {
				return nil, ErrInvalidInput
			}
			if suffix > size {
				suffix = size
			}
			offset, length = size-suffix, suffix
		} else {
			start, err := strconv.ParseInt(a, 10, 64)
			if err != nil || start < 0 {
				return nil, ErrInvalidInput
			}
			if start >= size {
				continue
			}
			end := size - 1
			if b != "" {
				end, err = strconv.ParseInt(b, 10, 64)
				if err != nil || end < start {
					return nil, ErrInvalidInput
				}
				if end >= size {
					end = size - 1
				}
			}
			offset, length = start, end-start+1
		}
		if length > 0 {
			ranges = append(ranges, MediaRange{offset, length})
		}
	}
	if len(ranges) == 0 {
		return nil, ErrInvalidInput
	}
	return ranges, nil
}

// VerifyMediaDigest streams original bytes with bounded memory and requires both
// the exact length and the connector's successful Close/version postconditions.
func VerifyMediaDigest(ctx context.Context, media MediaConnector, request Request, item Item) (string, error) {
	if media == nil || item.Size < 1 {
		return "", fmt.Errorf("%w: invalid media representation", ErrInvalidInput)
	}
	if upper, _ := item.Metadata["size_is_upper_bound"].(bool); upper || (configString(item.Metadata, "export_mime_type") != "" && !validContentSHA(configString(item.Metadata, "representation_sha256"))) {
		return "", fmt.Errorf("%w: original binary representation required", ErrInvalidInput)
	}
	if err := media.CheckMedia(ctx, request, item); err != nil {
		return "", mediaProviderError(err)
	}
	body, err := media.OpenMedia(ctx, request, item, MediaRange{0, item.Size})
	if err != nil {
		return "", mediaProviderError(err)
	}
	hash := sha256.New()
	copied, copyErr := io.CopyBuffer(hash, io.LimitReader(body, item.Size), make([]byte, 64<<10))
	closeErr := body.Close()
	if copyErr != nil {
		return "", copyErr
	}
	if closeErr != nil {
		return "", mediaProviderError(closeErr)
	}
	if copied != item.Size {
		return "", io.ErrUnexpectedEOF
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// ValidateMediaInventory proves that an entire selected representation inventory
// remains readable. Drive's native media adapter already performs the item audit
// in ValidateInventory; do not issue the same remote checks a second time.
func ValidateMediaInventory(ctx context.Context, connector Connector, request Request, inventory Inventory) error {
	media, ok := connector.(MediaConnector)
	if !ok {
		return fmt.Errorf("%w: provider does not support media", ErrInvalidInput)
	}
	if err := connector.ValidateInventory(ctx, request, inventory); err != nil {
		return err
	}
	if _, ok := connector.(*driveMediaConnector); ok {
		return nil
	}
	for _, item := range inventory.Objects {
		if err := media.CheckMedia(ctx, request, item); err != nil {
			return err
		}
	}
	return nil
}
