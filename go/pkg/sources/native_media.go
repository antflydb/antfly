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
	"fmt"
	"io"
)

// Collaboration attachments are already bounded during acquisition. Keep their
// original bytes, and recheck the complete parent/attachment revision before
// serving any range. Native Antfly producers perform interpretation afterwards.
func (c *CollaborationConnector) CheckMedia(ctx context.Context, r Request, item Item) error {
	if !c.NativeAttachments {
		return fmt.Errorf("native attachment acquisition is not enabled")
	}
	_, err := c.nativeMediaBytes(ctx, r, item)
	return err
}
func (c *CollaborationConnector) nativeMediaBytes(ctx context.Context, r Request, item Item) ([]byte, error) {
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

// WithAcquisitionContentStore installs private representation storage on a
// request-local connector. Shared provider clients are never mutated.
func WithAcquisitionContentStore(connector Connector, store AcquisitionContentStore) Connector {
	switch c := connector.(type) {
	case *driveMediaConnector:
		google := *c.GoogleConnector
		google.ContentStore = store
		copy := *c
		copy.GoogleConnector = &google
		return &copy
	default:
		return connector
	}
}
