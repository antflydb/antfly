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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
)

func workspaceSelectionKey(r Request) string {
	digest := sha256.Sum256(append([]byte(r.SourceNamespace+"\x00"), r.Config...))
	return hex.EncodeToString(digest[:])
}
func refKey(resource, container, id string) string {
	return resource + "\x00" + container + "\x00" + id
}

func (c *CollaborationConnector) workspaceSelection(ctx context.Context, r Request, config CollaborationConfig) ([]collaborationRef, map[string]bool, map[string]string, bool, error) {
	tokens := map[string]string{"config": workspaceSelectionKey(r)}
	config.tokens = tokens
	baseline := c.Baseline
	delta := baseline != nil && baseline.Authoritative && baseline.ProviderState["config"] == tokens["config"] && !r.Preview && len(r.RetrySourceKeys) == 0
	removed := map[string]bool{}
	if delta {
		var refs []collaborationRef
		var err error
		switch config.Resource {
		case "gmail":
			refs, err = c.gmailDelta(ctx, config, baseline.ProviderState, tokens, removed)
		case "calendar":
			refs, err = c.calendarDelta(ctx, config, baseline.ProviderState, tokens, removed)
			if err == nil && config.Attachments {
				// Drive files can change independently of their Calendar event. Refresh
				// parents with imported attachments even when the event cursor is idle.
				seen := make(map[string]bool, len(refs))
				for _, ref := range refs {
					seen[refKey(ref.Resource, ref.Container, ref.ID)] = true
				}
				for _, item := range baseline.Objects {
					if configString(item.Metadata, "resource") != "calendar" || configString(item.Metadata, "attachment_id") == "" {
						continue
					}
					ref := collaborationRef{Resource: "calendar", Container: configString(item.Metadata, "container_id"), ID: configString(item.Metadata, "remote_id")}
					key := refKey(ref.Resource, ref.Container, ref.ID)
					if !seen[key] && !removed[key] {
						seen[key] = true
						refs = append(refs, ref)
					}
				}
			}
		default:
			delta = false
		}
		if delta && err == nil {
			return refs, removed, tokens, true, nil
		}
		var api *ProviderHTTPError
		if err != nil && !(errors.As(err, &api) && (api.Status == 404 || api.Status == 410)) && !errors.Is(err, ErrInventoryIncomplete) {
			return nil, nil, nil, false, err
		}
		// Expired change cursors require a new authoritative selection. The previous
		// publication remains the ownership baseline until replacement is verified.
		tokens = map[string]string{"config": workspaceSelectionKey(r)}
		config.tokens = tokens
	}
	if config.Resource == "gmail" {
		var profile struct {
			Account string `json:"emailAddress"`
			History string `json:"historyId"`
		}
		if err := c.get(ctx, "gmail", "/profile", &profile); err != nil {
			return nil, nil, nil, false, err
		}
		if c.Cache != nil {
			cached, found, err := c.Cache.Load(ctx, "gmail-start-anchor")
			if err != nil {
				return nil, nil, nil, false, err
			}
			if found {
				var pinned struct {
					Account string `json:"emailAddress"`
					History string `json:"historyId"`
				}
				if json.Unmarshal(cached, &pinned) != nil || pinned.Account != profile.Account {
					return nil, nil, nil, false, ErrInvalidInput
				}
				profile.History = pinned.History
			} else {
				encoded, _ := json.Marshal(profile)
				if err := c.Cache.Save(ctx, "gmail-start-anchor", encoded); err != nil {
					return nil, nil, nil, false, err
				}
			}
		}
		tokens["account"] = profile.Account
		tokens["gmail"] = profile.History
	}
	refs, err := c.workspaceRefs(ctx, config)
	return refs, nil, tokens, false, err
}
func (c *CollaborationConnector) gmailDelta(ctx context.Context, config CollaborationConfig, before, after map[string]string, removed map[string]bool) ([]collaborationRef, error) {
	account, err := c.gmailAccount(ctx)
	if err != nil {
		return nil, err
	}
	if before["account"] != "" && before["account"] != account {
		return nil, fmt.Errorf("%w: Gmail account changed", ErrInvalidInput)
	}
	if before["gmail"] == "" {
		return nil, ErrInventoryIncomplete
	}
	after["account"] = account
	refs := map[string]collaborationRef{}
	page := ""
	seen := map[string]bool{}
	for {
		q := url.Values{"startHistoryId": {before["gmail"]}, "maxResults": {"500"}}
		if page != "" {
			q.Set("pageToken", page)
		}
		type change struct {
			Message struct {
				ID string `json:"id"`
			} `json:"message"`
		}
		var result struct {
			Next      string `json:"nextPageToken"`
			HistoryID string `json:"historyId"`
			History   []struct {
				Messages []struct {
					ID string `json:"id"`
				} `json:"messages"`
				Added         []change `json:"messagesAdded"`
				Deleted       []change `json:"messagesDeleted"`
				LabelsAdded   []change `json:"labelsAdded"`
				LabelsRemoved []change `json:"labelsRemoved"`
			} `json:"history"`
		}
		if err := c.get(ctx, "gmail", "/history?"+q.Encode(), &result); err != nil {
			return nil, err
		}
		for _, entry := range result.History {
			for _, message := range entry.Messages {
				if !googleFileID(message.ID) {
					return nil, ErrInventoryIncomplete
				}
				refs[message.ID] = collaborationRef{Resource: "gmail", Container: account, ID: message.ID}
			}
			for _, changes := range [][]change{entry.Added, entry.LabelsAdded, entry.LabelsRemoved} {
				for _, change := range changes {
					if !googleFileID(change.Message.ID) {
						return nil, ErrInventoryIncomplete
					}
					refs[change.Message.ID] = collaborationRef{Resource: "gmail", Container: account, ID: change.Message.ID}
				}
			}
			for _, change := range entry.Deleted {
				if !googleFileID(change.Message.ID) {
					return nil, ErrInventoryIncomplete
				}
				removed[refKey("gmail", account, change.Message.ID)] = true
				delete(refs, change.Message.ID)
			}
		}
		if len(refs)+len(removed) > maxCollaborationSourceObjects {
			return nil, ErrInventoryIncomplete
		}
		page = result.Next
		if page == "" {
			if result.HistoryID == "" {
				return nil, ErrInventoryIncomplete
			}
			after["gmail"] = result.HistoryID
			break
		}
		if err := paginationToken(seen, page); err != nil {
			return nil, err
		}
	}
	selected := []collaborationRef{}
	for _, ref := range refs {
		if removed[refKey(ref.Resource, ref.Container, ref.ID)] {
			continue
		}
		// A label removal is a selection tombstone only after reading the message's
		// current labels under the same pinned mailbox account.
		var message struct {
			ID     string   `json:"id"`
			Labels []string `json:"labelIds"`
		}
		if err := c.get(ctx, "gmail", "/messages/"+url.PathEscape(ref.ID)+"?format=minimal", &message); err != nil {
			return nil, err
		}
		if message.ID != ref.ID {
			return nil, ErrInventoryIncomplete
		}
		matched := true
		for _, label := range config.LabelIDs {
			found := false
			for _, actual := range message.Labels {
				if label == actual {
					found = true
				}
			}
			matched = matched && found
		}
		if !matched {
			removed[refKey(ref.Resource, ref.Container, ref.ID)] = true
			continue
		}
		selected = append(selected, ref)
	}
	return selected, nil
}
func (c *CollaborationConnector) calendarDelta(ctx context.Context, config CollaborationConfig, before, after map[string]string, removed map[string]bool) ([]collaborationRef, error) {
	refs := []collaborationRef{}
	for _, selected := range config.CalendarIDs {
		id, err := c.calendarID(ctx, selected)
		if err != nil {
			return nil, err
		}
		key := "calendar:" + id
		if before[key] == "" {
			return nil, ErrInventoryIncomplete
		}
		page := ""
		seen := map[string]bool{}
		for {
			q := url.Values{"syncToken": {before[key]}, "singleEvents": {"false"}, "showDeleted": {"true"}, "maxResults": {"2500"}}
			if page != "" {
				q.Set("pageToken", page)
			}
			var result struct {
				Items []calendarEvent `json:"items"`
				Next  string          `json:"nextPageToken"`
				Token string          `json:"nextSyncToken"`
			}
			if err := c.get(ctx, "calendar", "/calendars/"+url.PathEscape(id)+"/events?"+q.Encode(), &result); err != nil {
				return nil, err
			}
			for _, event := range result.Items {
				if !googleFileID(event.ID) {
					return nil, ErrInventoryIncomplete
				}
				if event.Status == "cancelled" && event.RecurringEventID == "" {
					removed[refKey("calendar", id, event.ID)] = true
				} else {
					refs = append(refs, collaborationRef{Resource: "calendar", Container: id, ID: event.ID})
				}
				if len(refs)+len(removed) > maxCollaborationSourceObjects {
					return nil, ErrInventoryIncomplete
				}
			}
			page = result.Next
			if page == "" {
				if result.Token == "" {
					return nil, fmt.Errorf("calendar sync token missing")
				}
				after[key] = result.Token
				break
			}
			if err := paginationToken(seen, page); err != nil {
				return nil, err
			}
		}
	}
	return refs, nil
}
