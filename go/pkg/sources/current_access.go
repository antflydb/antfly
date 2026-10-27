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
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"
)

// Metadata visibility and content permission are separate Drive capabilities.
// An empty revision authorizes a retained snapshot without requiring a content
// refresh merely because the remote document was edited since publication.
func (c *CollaborationConnector) checkDriveContentAccess(ctx context.Context, id, revision string) error {
	if !googleFileID(id) {
		return ErrInvalidInput
	}
	var file struct {
		ID           string `json:"id"`
		Version      string `json:"version"`
		Capabilities struct {
			CanDownload bool `json:"canDownload"`
		} `json:"capabilities"`
	}
	if err := c.drive().get(ctx, c.HTTPClient, "/files/"+url.PathEscape(id)+"?fields=id,version,capabilities(canDownload)&supportsAllDrives=true", &file); err != nil {
		return err
	}
	if file.ID != id || !file.Capabilities.CanDownload {
		return fmt.Errorf("%w: Drive content download access revoked", ErrInvalidInput)
	}
	if revision != "" && file.Version != revision {
		return &RevisionChangedError{Expected: revision, Actual: file.Version}
	}
	return nil
}

// AuthorizeInventory renews only authority for an already published snapshot.
// It never reads normalized blobs or downloads/extracts content, and it permits
// remote edits while preserving the published content revision until the next sync.
func (c *CollaborationConnector) AuthorizeInventory(ctx context.Context, r Request, inventory Inventory) error {
	var policy struct {
		TTL int `json:"permission_ttl_seconds"`
	}
	if err := json.Unmarshal(r.Config, &policy); err != nil {
		return ErrInvalidInput
	}
	if policy.TTL == 0 {
		policy.TTL = 900
	}
	if policy.TTL < 60 || policy.TTL > 86400 {
		return ErrInvalidInput
	}
	c.scopeChecks = map[string]time.Time{}
	c.calendarRoles = nil
	c.observedScopes = nil
	c.accessAuditWindow = time.Duration(policy.TTL) * time.Second
	defer func() { c.accessAuditWindow = 0 }()
	started := time.Now()
	for {
		err := c.authorizeInventoryOnce(ctx, r, inventory)
		var api *ProviderHTTPError
		if !errors.As(err, &api) || api.Status != 429 {
			return err
		}
		delay := time.Minute
		if !api.RetryAt.IsZero() {
			delay = time.Until(api.RetryAt)
			if delay < 0 {
				delay = 0
			}
		}
		if time.Since(started)+delay >= c.accessAuditWindow {
			return err
		}
		if c.accessWait != nil {
			if err := c.accessWait(ctx, delay); err != nil {
				return err
			}
		} else {
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
func (c *CollaborationConnector) authorizeInventoryOnce(ctx context.Context, r Request, inventory Inventory) error {
	if err := c.Validate(ctx, r); err != nil {
		return err
	}
	config, err := decodeSourceConfig[CollaborationConfig](r.Config)
	if err != nil {
		return err
	}
	if c.ProviderName == ProviderGoogleWorkspace && config.Resource == "drive" {
		if err := c.checkedAuthority("drive-folder/"+config.FolderID, func() error {
			var folder struct {
				ID           string `json:"id"`
				Capabilities struct {
					CanListChildren bool `json:"canListChildren"`
				} `json:"capabilities"`
			}
			if err := c.drive().get(ctx, c.HTTPClient, "/files/"+url.PathEscape(config.FolderID)+"?fields=id,capabilities(canListChildren)&supportsAllDrives=true", &folder); err != nil {
				return err
			}
			if folder.ID == "" || !folder.Capabilities.CanListChildren {
				return fmt.Errorf("%w: Drive folder listing access revoked", ErrInvalidInput)
			}
			return nil
		}); err != nil {
			return err
		}
		for _, item := range inventory.Objects {
			id := configString(item.Metadata, "file_id")
			if time.Since(c.scopeChecks["drive/"+id]) > c.accessCheckWindow() {
				if err := c.checkDriveContentAccess(ctx, id, ""); err != nil {
					return err
				}
				c.scopeChecks["drive/"+id] = time.Now()
			}
		}
		return nil
	}
	for _, item := range inventory.Objects {
		var ref collaborationRef
		if json.Unmarshal([]byte(configString(item.Metadata, "acquisition")), &ref) != nil {
			return ErrInvalidInput
		}
		if err := c.authorizeCurrent(ctx, config, ref, item, false); err != nil {
			return err
		}
	}
	// Empty inventories retain selected-container grant checks too.
	switch c.ProviderName {
	case ProviderSlack:
		for _, id := range config.ChannelIDs {
			key := refKey(ProviderSlack, id, "")
			if time.Since(c.scopeChecks[key]) > c.accessCheckWindow() {
				if err := c.slackScope(ctx, config, id); err != nil {
					return err
				}
				c.scopeChecks[key] = time.Now()
			}
		}
	case ProviderDiscord:
		for _, id := range config.ChannelIDs {
			key := refKey(ProviderDiscord, id, "")
			if time.Since(c.scopeChecks[key]) > c.accessCheckWindow() {
				if err := c.discordScope(ctx, config, id); err != nil {
					return err
				}
				c.scopeChecks[key] = time.Now()
			}
		}
	case ProviderGoogleWorkspace:
		if config.Resource == "gmail" {
			if err := c.checkedAuthority("gmail-account", func() error { _, err := c.gmailAccount(ctx); return err }); err != nil {
				return err
			}
			for _, label := range config.LabelIDs {
				if err := c.checkedAuthority("gmail-label/"+label, func() error { var value any; return c.get(ctx, "gmail", "/labels/"+url.PathEscape(label), &value) }); err != nil {
					return err
				}
			}
		} else {
			for _, id := range config.CalendarIDs {
				if err := c.checkedAuthority("calendar-selection/"+id, func() error { _, err := c.calendarID(ctx, id); return err }); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (c *CollaborationConnector) checkedAuthority(key string, check func() error) error {
	if time.Since(c.scopeChecks[key]) <= c.accessCheckWindow() {
		return nil
	}
	if err := check(); err != nil {
		return err
	}
	c.scopeChecks[key] = time.Now()
	return nil
}
