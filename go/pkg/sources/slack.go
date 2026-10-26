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
	"strconv"
	"strings"
)

type slackMessage struct {
	hasBody    bool
	TS         string `json:"ts"`
	ThreadTS   string `json:"thread_ts"`
	Text       string `json:"text"`
	User       string `json:"user"`
	BotID      string `json:"bot_id"`
	Subtype    string `json:"subtype"`
	ReplyCount int    `json:"reply_count"`
	Edited     any    `json:"edited"`
	Files      []struct {
		ID       string `json:"id"`
		Name     string `json:"name"`
		MIME     string `json:"mimetype"`
		Size     int64  `json:"size"`
		Download string `json:"url_private_download"`
		Private  string `json:"url_private"`
		External bool   `json:"is_external"`
	} `json:"files"`
}

func (m *slackMessage) UnmarshalJSON(data []byte) error {
	type plain slackMessage
	var value plain
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	*m = slackMessage(value)
	_, m.hasBody = fields["text"]
	return nil
}

func validSlackTS(ts string) bool {
	parts := strings.Split(ts, ".")
	if len(parts) != 2 || len(parts[1]) != 6 {
		return false
	}
	for _, p := range parts {
		if _, err := strconv.ParseUint(p, 10, 64); err != nil {
			return false
		}
	}
	return true
}
func (c *CollaborationConnector) slackScope(ctx context.Context, config CollaborationConfig, channel string) error {
	selected := false
	for _, id := range config.ChannelIDs {
		if id == channel {
			selected = true
		}
	}
	if !selected {
		return fmt.Errorf("Slack channel outside source selection")
	}
	var auth struct {
		TeamID string `json:"team_id"`
	}
	if err := c.get(ctx, ProviderSlack, "/auth.test", &auth); err != nil {
		return err
	}
	if auth.TeamID != config.TeamID {
		return fmt.Errorf("Slack workspace does not match connection")
	}
	var info struct {
		Channel struct {
			ID       string `json:"id"`
			IsMember bool   `json:"is_member"`
		} `json:"channel"`
	}
	if err := c.get(ctx, ProviderSlack, "/conversations.info?channel="+url.QueryEscape(channel), &info); err != nil {
		return err
	}
	if info.Channel.ID != channel || !info.Channel.IsMember {
		return fmt.Errorf("Slack source requires current channel membership")
	}
	return nil
}
func (c *CollaborationConnector) slackPage(ctx context.Context, method string, q url.Values, visitors ...func(slackMessage) error) ([]slackMessage, error) {
	sample := q.Get("_sample") == "true"
	q.Del("_sample")
	messages := []slackMessage{}
	seen := map[string]bool{}
	for {
		q.Set("limit", "100")
		if sample {
			q.Set("limit", "1")
		}
		var result struct {
			Messages []slackMessage `json:"messages"`
			HasMore  bool           `json:"has_more"`
			Limited  bool           `json:"is_limited"`
			Meta     struct {
				Next string `json:"next_cursor"`
			} `json:"response_metadata"`
		}
		if err := c.get(ctx, ProviderSlack, "/"+method+"?"+q.Encode(), &result); err != nil {
			return nil, err
		}
		if result.Limited {
			return nil, fmt.Errorf("%w: Slack history is retention limited", ErrInventoryIncomplete)
		}
		for _, message := range result.Messages {
			for _, visit := range visitors {
				if err := visit(message); err != nil {
					return nil, err
				}
			}
			if len(visitors) > 0 {
				message.Text = ""
				message.Files = nil
				message.hasBody = false
				message.Edited = nil
			}
			messages = append(messages, message)
		}
		if sample {
			if len(messages) > 1 {
				messages = messages[:1]
			}
			return messages, nil
		}
		if len(messages) > maxCollaborationSourceObjects {
			return nil, ErrInventoryIncomplete
		}
		next := result.Meta.Next
		if next == "" {
			if result.HasMore {
				return nil, ErrInventoryIncomplete
			}
			break
		}
		if err := paginationToken(seen, next); err != nil {
			return nil, err
		}
		q.Set("cursor", next)
	}
	return messages, nil
}
func (c *CollaborationConnector) slackRefs(ctx context.Context, config CollaborationConfig, visit func(collaborationRef, *slackMessage) error) ([]collaborationRef, error) {
	refs := []collaborationRef{}
	seen := map[string]bool{}
	add := func(channel string, m slackMessage) error {
		if !validSlackTS(m.TS) {
			return fmt.Errorf("invalid Slack message timestamp")
		}
		key := channel + "/" + m.TS
		if !seen[key] {
			seen[key] = true
			thread := ""
			if m.ThreadTS != "" && m.ThreadTS != m.TS {
				thread = m.ThreadTS
			}
			ref := collaborationRef{Resource: ProviderSlack, Container: channel, ID: m.TS, Thread: thread}
			if visit != nil {
				if err := visit(ref, &m); err != nil {
					return err
				}
			}
			refs = append(refs, ref)
		}
		if len(refs) > maxCollaborationSourceObjects {
			return ErrInventoryIncomplete
		}
		return nil
	}
	for _, channel := range config.ChannelIDs {
		if err := c.slackScope(ctx, config, channel); err != nil {
			return nil, err
		}
		q := url.Values{"channel": {channel}}
		if config.preview {
			q.Set("_sample", "true")
		}
		messages, err := c.slackPage(ctx, "conversations.history", q, func(m slackMessage) error {
			if m.Subtype == "message_deleted" {
				return nil
			}
			return add(channel, m)
		})
		if err != nil {
			return nil, err
		}
		for _, message := range messages {
			if message.Subtype == "message_deleted" {
				continue
			}
			if config.preview {
				return refs, nil
			}
			if message.ReplyCount > 0 && (config.IncludeThreads == nil || *config.IncludeThreads) {
				replies, err := c.slackPage(ctx, "conversations.replies", url.Values{"channel": {channel}, "ts": {message.TS}}, func(m slackMessage) error { return add(channel, m) })
				if err != nil {
					return nil, err
				}
				_ = replies
			}
		}
	}
	return refs, nil
}
func (c *CollaborationConnector) slackLoad(ctx context.Context, config CollaborationConfig, ref collaborationRef) (collaborationMaterial, error) {
	if !validSlackTS(ref.ID) {
		return collaborationMaterial{}, fmt.Errorf("invalid Slack message timestamp")
	}
	method := "conversations.history"
	q := url.Values{"channel": {ref.Container}, "oldest": {ref.ID}, "latest": {ref.ID}, "inclusive": {"true"}}
	if ref.Thread != "" {
		if !validSlackTS(ref.Thread) {
			return collaborationMaterial{}, fmt.Errorf("invalid Slack thread timestamp")
		}
		method = "conversations.replies"
		q.Set("ts", ref.Thread)
	}
	messages, err := c.slackPage(ctx, method, q)
	if err != nil {
		return collaborationMaterial{}, err
	}
	var message *slackMessage
	for i := range messages {
		if messages[i].TS == ref.ID {
			message = &messages[i]
			break
		}
	}
	if message == nil || message.Subtype == "message_deleted" {
		return collaborationMaterial{}, ErrInventoryIncomplete
	}
	return c.slackMaterial(ctx, config, ref, *message)
}
func (c *CollaborationConnector) slackMaterial(ctx context.Context, config CollaborationConfig, ref collaborationRef, message slackMessage) (collaborationMaterial, error) {
	files := []collaborationFile{}
	for _, file := range message.Files {
		if !config.Attachments {
			continue
		}
		if file.External {
			return collaborationMaterial{}, fmt.Errorf("external Slack files require an independently authorized source")
		}
		if file.ID == "" || file.Size > maxImportSourceObjectBytes {
			return collaborationMaterial{}, fmt.Errorf("invalid or oversized Slack attachment")
		}
		download := file.Download
		if download == "" {
			download = file.Private
		}
		if download == "" {
			return collaborationMaterial{}, fmt.Errorf("Slack attachment download unavailable")
		}
		files = append(files, collaborationFile{ID: file.ID, Name: file.Name, MIME: file.MIME, URL: download})
	}
	canonical := "https://app.slack.com/client/" + url.PathEscape(config.TeamID) + "/" + url.PathEscape(ref.Container) + "/thread/" + url.PathEscape(ref.Container) + "-" + strings.ReplaceAll(ref.ID, ".", "")
	var permalink struct {
		URL string `json:"permalink"`
	}
	if err := c.get(ctx, ProviderSlack, "/chat.getPermalink?"+url.Values{"channel": {ref.Container}, "message_ts": {ref.ID}}.Encode(), &permalink); err != nil {
		return collaborationMaterial{}, err
	}
	if permalink.URL != "" {
		canonical = permalink.URL
	}
	meta := map[string]any{"team_id": config.TeamID, "channel_id": ref.Container, "message_ts": ref.ID, "thread_ts": message.ThreadTS, "author_id": message.User, "bot_id": message.BotID, "edited": message.Edited}
	return collaborationMaterial{Text: []byte(message.Text), URL: canonical, Title: ref.ID, Metadata: meta, Files: files}, nil
}
