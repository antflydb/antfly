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
	"fmt"
	"net/url"
	"sort"
	"strings"
)

// Reconciliation has a separate durable selection. It lists identities, then
// reads attachment metadata only for omitted children of parents still present.
// It never downloads or extracts content to establish a deletion tombstone.
func (c *CollaborationConnector) ValidateAbsentSources(ctx context.Context, r Request, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	if err := c.Validate(ctx, r); err != nil {
		return err
	}
	config, err := decodeSourceConfig[CollaborationConfig](r.Config)
	if err != nil {
		return err
	}
	if c.ProviderName == ProviderGoogleWorkspace && config.Resource == "drive" {
		return c.drive().ValidateAbsentSources(ctx, r, ids)
	}
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	encoded, _ := json.Marshal(sorted)
	fresh := *c
	fresh.Cache = ScopedAcquisitionCache(c.Cache, "absences/v1/"+c.hydrationSelection(r)+"/"+acquisitionKey(string(encoded)))
	fresh.Baseline = nil
	fresh.scopeChecks = nil
	fresh.observedScopes = nil
	var refs []collaborationRef
	switch c.ProviderName {
	case ProviderSlack:
		refs, err = fresh.slackRefs(ctx, config, nil)
	case ProviderDiscord:
		refs, err = fresh.discordRefs(ctx, config)
	case ProviderGoogleWorkspace:
		refs, err = fresh.workspaceRefs(ctx, config)
	default:
		return ErrInvalidInput
	}
	if err != nil {
		return err
	}
	present := make(map[string]collaborationRef, len(refs))
	for _, ref := range refs {
		present[collaborationItemID(c.ProviderName, r.SourceNamespace, ref)] = ref
	}
	for _, id := range sorted {
		if !ownedID(id, r.SourceNamespace) {
			return ErrInvalidInput
		}
		if _, exists := present[id]; exists {
			return ErrInventoryIncomplete
		}
		parent, attachment, child := strings.Cut(id, "/attachments/")
		if !child || !config.Attachments {
			continue
		}
		ref, exists := present[parent]
		if !exists {
			continue
		}
		attachment, err = url.PathUnescape(attachment)
		if err != nil {
			return ErrInvalidInput
		}
		current, err := fresh.currentAttachmentIDs(ctx, config, ref)
		if err != nil {
			return err
		}
		for _, actual := range current {
			if actual == attachment {
				return ErrInventoryIncomplete
			}
		}
	}
	// Resume metadata is not an authorization proof. Check every selected scope
	// again, including empty selections, before retiring owned identities.
	return fresh.ValidateInventory(ctx, r, Inventory{})
}
func collaborationItemID(provider, namespace string, ref collaborationRef) string {
	scheme := provider
	if provider == ProviderGoogleWorkspace {
		scheme = "workspace"
	}
	return scheme + "://" + namespace + "/" + ref.Resource + "/" + url.PathEscape(ref.Container) + "/" + url.PathEscape(ref.ID)
}
func (c *CollaborationConnector) currentAttachmentIDs(ctx context.Context, config CollaborationConfig, ref collaborationRef) ([]string, error) {
	if err := c.validateRef(ctx, config, ref); err != nil {
		return nil, err
	}
	encoded, _ := json.Marshal(ref)
	key := "parent-attachments/v1/" + acquisitionKey(string(encoded))
	if c.Cache != nil {
		raw, found, err := c.Cache.Load(ctx, key)
		if err != nil {
			return nil, err
		}
		if found {
			var ids []string
			if json.Unmarshal(raw, &ids) != nil || len(ids) > 1000 {
				return nil, ErrInvalidInput
			}
			return ids, nil
		}
	}
	ids := []string{}
	switch ref.Resource {
	case ProviderSlack:
		method := "conversations.history"
		q := url.Values{"channel": {ref.Container}, "oldest": {ref.ID}, "latest": {ref.ID}, "inclusive": {"true"}}
		if ref.Thread != "" {
			method = "conversations.replies"
			q.Set("ts", ref.Thread)
		}
		messages, err := c.slackPage(ctx, method, q)
		if err != nil {
			return nil, err
		}
		found := false
		for _, message := range messages {
			if message.TS == ref.ID {
				found = true
				for _, file := range message.Files {
					ids = append(ids, file.ID)
				}
			}
		}
		if !found {
			return nil, ErrInventoryIncomplete
		}
	case ProviderDiscord:
		var message discordMessage
		if err := c.get(ctx, ProviderDiscord, "/channels/"+ref.Container+"/messages/"+ref.ID, &message); err != nil {
			return nil, err
		}
		if message.ID != ref.ID || message.ChannelID != ref.Container {
			return nil, ErrInventoryIncomplete
		}
		for _, file := range message.Attachments {
			ids = append(ids, file.ID)
		}
	case "calendar":
		var event calendarEvent
		if err := c.get(ctx, "calendar", "/calendars/"+url.PathEscape(ref.Container)+"/events/"+url.PathEscape(ref.ID)+"?fields=id,attachments(fileId)", &event); err != nil {
			return nil, err
		}
		if event.ID != ref.ID {
			return nil, ErrInventoryIncomplete
		}
		for _, attachment := range event.Attachments {
			ids = append(ids, attachment.FileID)
		}
	case "gmail":
		var message gmailMessage
		// Gmail requires full format to expose the MIME tree. A partial
		// response retains only attachment identities at every supported depth.
		// The final child marker lets the walker reject deeper MIME trees.
		fields := "partId,filename,mimeType,body(attachmentId),parts(partId)"
		for depth := 0; depth < 50; depth++ {
			fields = "partId,filename,mimeType,body(attachmentId),parts(" + fields + ")"
		}
		query := "?format=full&fields=" + url.QueryEscape("id,payload("+fields+")")
		if err := c.get(ctx, "gmail", "/messages/"+url.PathEscape(ref.ID)+query, &message); err != nil {
			return nil, err
		}
		if message.ID != ref.ID {
			return nil, ErrInventoryIncomplete
		}
		var walk func(gmailPart, int) error
		walk = func(part gmailPart, depth int) error {
			if depth > 50 {
				return ErrInventoryIncomplete
			}
			if part.Filename != "" || (part.Body.AttachmentID != "" && !strings.HasPrefix(part.MIME, "text/")) {
				id := part.PartID
				if id == "" {
					id = part.Body.AttachmentID
				}
				if id == "" {
					return ErrInventoryIncomplete
				}
				ids = append(ids, id)
			}
			for _, child := range part.Parts {
				if err := walk(child, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		if err := walk(message.Payload, 0); err != nil {
			return nil, err
		}
	default:
		return nil, ErrInvalidInput
	}
	for _, id := range ids {
		if id == "" || len(id) > 512 || strings.ContainsAny(id, "/\\\x00\r\n") {
			return nil, ErrInventoryIncomplete
		}
	}
	if len(ids) > 1000 {
		return nil, fmt.Errorf("%w: attachment metadata exceeds limit", ErrInventoryIncomplete)
	}
	if c.Cache != nil {
		raw, _ := json.Marshal(ids)
		if err := c.Cache.Save(ctx, key, raw); err != nil {
			return nil, err
		}
	}
	return ids, nil
}
