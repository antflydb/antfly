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
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
)

// Collaboration attachments are bounded during acquisition. Reacquire their
// original bytes and check the complete parent/attachment revision for each
// HTTP delivery. Native Antfly producers perform interpretation afterwards.
func (c *CollaborationConnector) CheckMedia(ctx context.Context, r Request, item Item) error {
	if !c.NativeAttachments {
		return fmt.Errorf("native attachment acquisition is not enabled")
	}
	if err := c.Validate(ctx, r); err != nil {
		return err
	}
	config, err := decodeSourceConfig[CollaborationConfig](r.Config)
	if err != nil {
		return err
	}
	var ref collaborationRef
	if err := json.Unmarshal([]byte(configString(item.Metadata, "acquisition")), &ref); err != nil {
		return ErrInvalidInput
	}
	return c.authorizeRetained(ctx, config, ref, item)
}
func (c *CollaborationConnector) nativeMediaBytes(ctx context.Context, r Request, item Item) ([]byte, error) {
	if !c.NativeAttachments {
		return nil, fmt.Errorf("native attachment acquisition is not enabled")
	}
	data, err := c.Read(ctx, r, item)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != item.Size {
		return nil, &RevisionChangedError{Expected: item.Revision, Actual: "attachment length changed"}
	}
	return data, nil
}
func (c *CollaborationConnector) OpenMedia(ctx context.Context, r Request, item Item, span MediaRange) (io.ReadCloser, error) {
	if !c.NativeAttachments {
		return nil, fmt.Errorf("native attachment acquisition is not enabled")
	}
	if err := span.validate(item.Size); err != nil {
		return nil, err
	}
	data, err := c.nativeMediaBytes(ctx, r, item)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data[span.Offset : span.Offset+span.Length])), nil
}

// MediaContentScope names the trusted provider byte namespace, independently of
// credentials. Hosted adapters also bind their own tenant/credential identity.
func (c *CollaborationConnector) MediaContentScope() string {
	return fmt.Sprintf("native-collaboration-v1|%s|%s|%s|%s|%s", c.ProviderName, c.APIBase, c.GmailBase, c.CalendarBase, c.DriveBase)
}

// AcquiredMediaConnector resolves a bounded representation once per HTTP
// delivery. Returned bytes are request-local and never persisted by the proxy.
type AcquiredMediaConnector interface {
	AcquireMedia(context.Context, Request, Item) ([]byte, bool, error)
}

func (c *GoogleConnector) AcquireMedia(ctx context.Context, r Request, item Item) ([]byte, bool, error) {
	if configString(item.Metadata, "export_mime_type") == "" {
		return nil, false, nil
	}
	data, err := c.exportMediaBytes(ctx, r, item)
	return data, true, err
}

func (c *GoogleConnector) exportMediaBytes(ctx context.Context, r Request, item Item) ([]byte, error) {
	if err := c.CheckMedia(ctx, r, item); err != nil {
		return nil, err
	}
	// Export output is allowed to vary in length; compare the complete acquired
	// representation with the signed pin before exposing even its first byte.
	readItem := item
	readItem.Metadata = cloneSourceMetadata(item.Metadata)
	readItem.Metadata["size_is_upper_bound"] = true
	data, err := c.Read(ctx, r, readItem)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != item.Size || fmt.Sprintf("%x", sha256.Sum256(data)) != configString(item.Metadata, "representation_sha256") {
		return nil, &RevisionChangedError{Expected: item.Revision, Actual: "export representation changed; synchronize this source again"}
	}
	return data, nil
}
func (c *CollaborationConnector) AcquireMedia(ctx context.Context, r Request, item Item) ([]byte, bool, error) {
	data, err := c.nativeMediaBytes(ctx, r, item)
	return data, true, err
}

type acquiredMedia struct{ data []byte }

func (acquiredMedia) CheckMedia(context.Context, Request, Item) error { return nil }
func (acquiredMedia) StableMediaRanges() bool                         { return true }
func (m acquiredMedia) OpenMedia(_ context.Context, _ Request, item Item, span MediaRange) (io.ReadCloser, error) {
	if err := span.validate(item.Size); err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(m.data[span.Offset : span.Offset+span.Length])), nil
}
