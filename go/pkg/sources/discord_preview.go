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
)

// Preview samples at most eight containers and never follows an archive cursor.
// Reaching that bound without a message is inconclusive, not an empty source.
const discordPreviewContainerLimit = 8

func (c *CollaborationConnector) discordPreviewRefs(ctx context.Context, config CollaborationConfig) ([]collaborationRef, error) {
	seen := map[string]bool{}
	sampled := 0
	budgetError := func() error {
		return fmt.Errorf("%w: Discord preview sample budget exhausted; narrow the channel selection", ErrInventoryIncomplete)
	}
	sample := func(id string) (discordChannel, *collaborationRef, error) {
		if seen[id] {
			return discordChannel{}, nil, nil
		}
		if sampled >= discordPreviewContainerLimit {
			return discordChannel{}, nil, budgetError()
		}
		sampled++
		seen[id] = true
		if err := c.discordScope(ctx, config, id); err != nil {
			return discordChannel{}, nil, err
		}
		channel, err := c.discordChannel(ctx, id)
		if err != nil {
			return channel, nil, err
		}
		if channel.Type == 15 || channel.Type == 16 {
			return channel, nil, nil
		}
		var messages []discordMessage
		if err := c.get(ctx, ProviderDiscord, "/channels/"+id+"/messages?limit=1", &messages); err != nil {
			return channel, nil, err
		}
		if len(messages) == 0 {
			return channel, nil, nil
		}
		if !discordID(messages[0].ID) {
			return channel, nil, ErrInventoryIncomplete
		}
		return channel, &collaborationRef{Resource: ProviderDiscord, Container: id, ID: messages[0].ID}, nil
	}
	var active struct {
		Threads []discordChannel `json:"threads"`
	}
	activeLoaded := false
	truncated := false
	for _, id := range config.ChannelIDs {
		channel, ref, err := sample(id)
		if err != nil {
			return nil, err
		}
		if ref != nil {
			return []collaborationRef{*ref}, nil
		}
		if channel.ID == "" || (channel.Type != 0 && channel.Type != 5 && channel.Type != 15 && channel.Type != 16) || (config.IncludeThreads != nil && !*config.IncludeThreads) {
			continue
		}
		if !activeLoaded {
			if err := c.get(ctx, ProviderDiscord, "/guilds/"+config.GuildID+"/threads/active", &active); err != nil {
				return nil, err
			}
			activeLoaded = true
		}
		for _, thread := range active.Threads {
			if thread.ParentID != id {
				continue
			}
			_, ref, err := sample(thread.ID)
			if err != nil {
				return nil, err
			}
			if ref != nil {
				return []collaborationRef{*ref}, nil
			}
		}
		for _, suffix := range discordArchivePaths(channel.Type) {
			var page struct {
				Threads []discordChannel `json:"threads"`
				HasMore bool             `json:"has_more"`
			}
			q := url.Values{"limit": {"8"}}
			if err := c.get(ctx, ProviderDiscord, "/channels/"+id+"/"+suffix+"?"+q.Encode(), &page); err != nil {
				return nil, err
			}
			truncated = truncated || page.HasMore
			for _, thread := range page.Threads {
				if thread.ParentID != id {
					return nil, ErrInventoryIncomplete
				}
				_, ref, err := sample(thread.ID)
				if err != nil {
					return nil, err
				}
				if ref != nil {
					return []collaborationRef{*ref}, nil
				}
			}
		}
	}
	if truncated {
		return nil, budgetError()
	}
	return []collaborationRef{}, nil
}
