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
	"net/url"
	"slices"
	"strings"
)

type Resource struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	ParentID string `json:"parent_id,omitempty"`
	Readable bool   `json:"readable"`
	Folder   bool   `json:"folder,omitempty"`
}
type ResourcePage struct {
	Data       []Resource `json:"data"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

// ListResources returns only selectable metadata. Endpoints and field projections
// are fixed here; selectors never become arbitrary URLs or downloadable content.
func (c *CollaborationConnector) ListResources(ctx context.Context, resource, parent, cursor string) (ResourcePage, error) {
	page := ResourcePage{Data: []Resource{}}
	if len(cursor) > 4096 || len(parent) > 512 || strings.ContainsAny(parent+cursor, "\x00\r\n") {
		return page, fmt.Errorf("%w: invalid resource cursor or parent", ErrInvalidInput)
	}
	switch c.ProviderName {
	case ProviderGoogleWorkspace:
		services := c.Services
		if len(services) == 0 {
			services = []string{"drive"}
		}
		if !slices.Contains(services, resource) {
			return page, fmt.Errorf("%w: Google service requires additional consent", ErrInvalidInput)
		}
		switch resource {
		case "drive":
			if parent == "" {
				parent = "root"
			}
			if !googleFileID(parent) {
				return page, ErrInvalidInput
			}
			q := url.Values{"q": {"'" + parent + "' in parents and trashed = false and mimeType = 'application/vnd.google-apps.folder'"}, "fields": {"nextPageToken,incompleteSearch,files(id,name)"}, "pageSize": {"100"}, "supportsAllDrives": {"true"}, "includeItemsFromAllDrives": {"true"}}
			if cursor != "" {
				q.Set("pageToken", cursor)
			}
			var result struct {
				Files      []struct{ ID, Name string } `json:"files"`
				Next       string                      `json:"nextPageToken"`
				Incomplete bool                        `json:"incompleteSearch"`
			}
			if err := c.drive().get(ctx, c.HTTPClient, "/files?"+q.Encode(), &result); err != nil {
				return page, err
			}
			if result.Incomplete {
				return page, ErrInventoryIncomplete
			}
			for _, file := range result.Files {
				page.Data = append(page.Data, Resource{ID: file.ID, Name: file.Name, ParentID: parent, Readable: true, Folder: true})
			}
			page.NextCursor = result.Next
		case "gmail":
			if cursor != "" {
				return page, ErrInvalidInput
			}
			var result struct {
				Labels []struct{ ID, Name string } `json:"labels"`
			}
			if err := c.get(ctx, "gmail", "/labels", &result); err != nil {
				return page, err
			}
			for _, label := range result.Labels {
				page.Data = append(page.Data, Resource{ID: label.ID, Name: label.Name, Readable: true})
			}
		case "calendar":
			q := url.Values{"maxResults": {"100"}, "showHidden": {"true"}}
			if cursor != "" {
				q.Set("pageToken", cursor)
			}
			var result struct {
				Items []struct{ ID, Summary, AccessRole string } `json:"items"`
				Next  string                                     `json:"nextPageToken"`
			}
			if err := c.get(ctx, "calendar", "/users/me/calendarList?"+q.Encode(), &result); err != nil {
				return page, err
			}
			for _, calendar := range result.Items {
				page.Data = append(page.Data, Resource{ID: calendar.ID, Name: calendar.Summary, Readable: calendar.AccessRole == "reader" || calendar.AccessRole == "writer" || calendar.AccessRole == "owner"})
			}
			page.NextCursor = result.Next
		default:
			return page, ErrInvalidInput
		}
	case ProviderSlack:
		q := url.Values{"types": {"public_channel,private_channel"}, "exclude_archived": {"false"}, "limit": {"100"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var result struct {
			Channels []struct {
				ID, Name string
				Member   bool `json:"is_member"`
			} `json:"channels"`
			Meta struct {
				Next string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		if err := c.get(ctx, ProviderSlack, "/conversations.list?"+q.Encode(), &result); err != nil {
			return page, err
		}
		for _, channel := range result.Channels {
			page.Data = append(page.Data, Resource{ID: channel.ID, Name: channel.Name, Readable: channel.Member})
		}
		page.NextCursor = result.Meta.Next
	case ProviderDiscord:
		if !discordID(parent) || cursor != "" {
			return page, ErrInvalidInput
		}
		var channels []discordChannel
		if err := c.get(ctx, ProviderDiscord, "/guilds/"+parent+"/channels", &channels); err != nil {
			return page, err
		}
		for _, channel := range channels {
			if channel.Type == 0 || channel.Type == 5 || channel.Type == 15 || channel.Type == 16 {
				config := CollaborationConfig{GuildID: parent, ChannelIDs: []string{channel.ID}}
				err := c.discordScope(ctx, config, channel.ID)
				page.Data = append(page.Data, Resource{ID: channel.ID, Name: channel.Name, ParentID: channel.ParentID, Readable: err == nil})
			}
		}
	default:
		return page, ErrInvalidInput
	}
	if len(page.Data) > 10000 {
		return ResourcePage{}, ErrInventoryIncomplete
	}
	return page, nil
}
