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
// nativeAttachment resolves current object metadata, selection and parent
// revision without downloading any attachment bodies. Do not use the retained
// text authorization shortcut here: HEAD and conditional delivery must observe
// deleted messages, removed files and changed parent revisions too.
func (c *CollaborationConnector) nativeAttachment(ctx context.Context, r Request, item Item) (collaborationFile, error) {
	if !c.NativeAttachments {
		return collaborationFile{}, fmt.Errorf("native attachment acquisition is not enabled")
	}
	if err := c.Validate(ctx, r); err != nil {
		return collaborationFile{}, err
	}
	config, err := decodeSourceConfig[CollaborationConfig](r.Config)
	if err != nil {
		return collaborationFile{}, err
	}
	var ref collaborationRef
	if json.Unmarshal([]byte(configString(item.Metadata, "acquisition")), &ref) != nil || ref.Attachment == "" {
		return collaborationFile{}, ErrInvalidInput
	}
	defer c.beginDiscordReadPass()()
	if err := c.validateRef(ctx, config, ref); err != nil {
		return collaborationFile{}, err
	}
	if ref.Resource == "gmail" {
		if err := c.checkGmailContentAccess(ctx); err != nil {
			return collaborationFile{}, err
		}
	}
	attachment := ref.Attachment
	ref.Attachment = ""
	material, err := c.load(ctx, config, ref)
	if err != nil {
		return collaborationFile{}, err
	}
	if len(material.Files) > 1000 || len(material.Text) > maxImportSourceObjectBytes {
		return collaborationFile{}, ErrInvalidInput
	}
	metadata := collaborationMetadata(ref, material)
	parent := c.collaborationInput(r, ref, material, material.Text, metadata)
	if parent.Revision != configString(item.Metadata, "parent_revision") || parent.ID != configString(item.Metadata, "parent_identity") {
		return collaborationFile{}, &RevisionChangedError{Expected: configString(item.Metadata, "parent_revision"), Actual: parent.Revision}
	}
	for _, file := range material.Files {
		if file.ID != attachment {
			continue
		}
		if file.driveFile {
			if err := c.checkDriveContentAccess(ctx, file.ID, configString(item.Metadata, "drive_revision")); err != nil {
				return collaborationFile{}, err
			}
		}
		return file, nil
	}
	return collaborationFile{}, fmt.Errorf("%w: attachment no longer selected or accessible", ErrInvalidInput)
}
func (c *CollaborationConnector) CheckMedia(ctx context.Context, r Request, item Item) error {
	_, err := c.nativeAttachment(ctx, r, item)
	return err
}
func (c *CollaborationConnector) nativeMediaBytes(ctx context.Context, r Request, item Item) ([]byte, error) {
	file, err := c.nativeAttachment(ctx, r, item)
	if err != nil {
		return nil, err
	}
	if file.driveFile {
		file, err = c.workspaceDriveFile(ctx, file.ID, maxImportSourceObjectBytes)
		if err != nil {
			return nil, err
		}
		if file.Revision != configString(item.Metadata, "drive_revision") {
			return nil, &RevisionChangedError{Expected: configString(item.Metadata, "drive_revision"), Actual: file.Revision}
		}
	}
	data := file.Data
	if file.gmailMessageID != "" {
		data, err = c.readGmailAttachment(ctx, file, maxImportSourceObjectBytes)
	}
	if err == nil && file.URL != "" {
		data, err = c.downloadAttachment(ctx, file.URL)
	}
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(data)
	if int64(len(data)) != item.Size || fmt.Sprintf("%x", digest) != configString(item.Metadata, "attachment_sha256") {
		return nil, &RevisionChangedError{Expected: item.Revision, Actual: "attachment bytes changed"}
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
