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
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Each acquisition/audit owns its authority cache. Successful metadata is
// shared only within that operation and its access-check window; a later audit
// always starts fresh. This avoids guild calls per message, thread or channel.
type discordReadPass struct {
	authorities map[string]*discordAuthority
	channels    map[string]discordCachedChannel
	active      map[string]discordActiveThreads
}
type discordCachedChannel struct {
	channel   discordChannel
	checkedAt time.Time
}
type discordActiveThreads struct {
	threads   []discordChannel
	checkedAt time.Time
}

func (c *CollaborationConnector) beginDiscordReadPass() func() {
	if c.ProviderName != ProviderDiscord || c.discordPass != nil {
		return func() {}
	}
	c.discordPass = &discordReadPass{authorities: map[string]*discordAuthority{}, channels: map[string]discordCachedChannel{}, active: map[string]discordActiveThreads{}}
	return func() { c.discordPass = nil }
}
func (c *CollaborationConnector) discordActive(ctx context.Context, guild string) ([]discordChannel, error) {
	if c.discordPass != nil {
		if entry, ok := c.discordPass.active[guild]; ok && time.Since(entry.checkedAt) < c.accessCheckWindow() {
			return entry.threads, nil
		}
	}
	started := time.Now()
	var result struct {
		Threads []discordChannel `json:"threads"`
	}
	if err := c.get(ctx, ProviderDiscord, "/guilds/"+guild+"/threads/active", &result); err != nil {
		return nil, err
	}
	if c.discordPass != nil {
		c.discordPass.active[guild] = discordActiveThreads{result.Threads, started}
	}
	return result.Threads, nil
}

type discordChannel struct {
	Name       string `json:"name"`
	ID         string `json:"id"`
	GuildID    string `json:"guild_id"`
	ParentID   string `json:"parent_id"`
	Type       int    `json:"type"`
	Overwrites []struct {
		ID    string `json:"id"`
		Type  int    `json:"type"`
		Allow string `json:"allow"`
		Deny  string `json:"deny"`
	} `json:"permission_overwrites"`
	ThreadMetadata struct {
		ArchivedAt string `json:"archive_timestamp"`
	} `json:"thread_metadata"`
}

func (c *CollaborationConnector) discordChannel(ctx context.Context, id string) (discordChannel, error) {
	if c.discordPass != nil {
		if entry, ok := c.discordPass.channels[id]; ok && time.Since(entry.checkedAt) < c.accessCheckWindow() {
			return entry.channel, nil
		}
	}
	started := time.Now()
	var channel discordChannel
	err := c.get(ctx, ProviderDiscord, "/channels/"+url.PathEscape(id), &channel)
	if err == nil && channel.ID != id {
		err = fmt.Errorf("Discord channel identity changed")
	}
	if err == nil && c.discordPass != nil {
		c.discordPass.channels[id] = discordCachedChannel{channel, started}
	}
	return channel, err
}
func (c *CollaborationConnector) discordScope(ctx context.Context, config CollaborationConfig, id string) error {
	if !discordID(id) {
		return fmt.Errorf("invalid Discord channel")
	}
	channel, err := c.discordChannel(ctx, id)
	if err != nil {
		return err
	}
	if channel.GuildID != config.GuildID {
		return fmt.Errorf("Discord guild does not match source")
	}
	selected := false
	for _, allowed := range config.ChannelIDs {
		if allowed == id || ((config.IncludeThreads == nil || *config.IncludeThreads) && channel.ParentID == allowed && (channel.Type == 10 || channel.Type == 11 || channel.Type == 12)) {
			selected = true
		}
	}
	if !selected {
		return fmt.Errorf("Discord channel outside source selection")
	}
	if channel.Type != 0 && channel.Type != 5 && channel.Type != 10 && channel.Type != 11 && channel.Type != 12 && channel.Type != 15 && channel.Type != 16 {
		return fmt.Errorf("unsupported Discord source channel type")
	}
	privateThread := channel.Type == 12

	if channel.Type == 10 || channel.Type == 11 || channel.Type == 12 {
		channel, err = c.discordChannel(ctx, channel.ParentID)
		if err != nil {
			return err
		}
		if channel.GuildID != config.GuildID {
			return fmt.Errorf("Discord parent guild changed")
		}
	}
	authority, err := c.discordAuthority(ctx, config.GuildID)
	if err != nil {
		return err
	}
	permissions, err := authority.checkedPermissions(channel)
	if err != nil {
		return err
	}
	if privateThread && permissions&8 == 0 && permissions&(1<<34) == 0 {
		var member map[string]any
		if err := c.get(ctx, ProviderDiscord, "/channels/"+id+"/thread-members/@me", &member); err != nil {
			return err
		}
	}
	return nil
}

var errDiscordAccessDenied = errors.New("Discord source access denied")

type discordAuthority struct {
	guild, botID   string
	roles          map[string]bool
	permissions    uint64
	messageContent bool
	checkedAt      time.Time
}

// Guild identity, role membership and application flags are shared by every
// channel in a resource page. Channel overwrites are evaluated locally.
func (c *CollaborationConnector) discordAuthority(ctx context.Context, guild string) (*discordAuthority, error) {
	if c.discordPass != nil {
		if authority := c.discordPass.authorities[guild]; authority != nil && time.Since(authority.checkedAt) < c.accessCheckWindow() {
			return authority, nil
		}
	}
	started := time.Now()
	var me struct {
		ID string `json:"id"`
	}
	if err := c.get(ctx, ProviderDiscord, "/users/@me", &me); err != nil {
		return nil, err
	}
	if !discordID(me.ID) {
		return nil, fmt.Errorf("Discord bot identity missing")
	}
	var member struct {
		Roles []string `json:"roles"`
	}
	if err := c.get(ctx, ProviderDiscord, "/guilds/"+guild+"/members/"+me.ID, &member); err != nil {
		return nil, err
	}
	var roles []struct {
		ID          string `json:"id"`
		Permissions string `json:"permissions"`
	}
	if err := c.get(ctx, ProviderDiscord, "/guilds/"+guild+"/roles", &roles); err != nil {
		return nil, err
	}
	memberRoles := map[string]bool{guild: true}
	for _, id := range member.Roles {
		memberRoles[id] = true
	}
	var permissions uint64
	for _, role := range roles {
		if memberRoles[role.ID] {
			bits, err := strconv.ParseUint(role.Permissions, 10, 64)
			if err != nil {
				return nil, err
			}
			permissions |= bits
		}
	}
	var app struct {
		Flags uint64 `json:"flags"`
	}
	if err := c.get(ctx, ProviderDiscord, "/oauth2/applications/@me", &app); err != nil {
		return nil, err
	}

	authority := &discordAuthority{guild: guild, botID: me.ID, roles: memberRoles, permissions: permissions, messageContent: app.Flags&((1<<18)|(1<<19)) != 0, checkedAt: started}
	if c.discordPass != nil {
		c.discordPass.authorities[guild] = authority
	}
	return authority, nil
}

func (authority *discordAuthority) check(channel discordChannel) error {
	_, err := authority.checkedPermissions(channel)
	return err
}
func (authority *discordAuthority) checkedPermissions(channel discordChannel) (uint64, error) {
	permissions := authority.permissions
	if permissions&8 == 0 {
		var deny, allow uint64
		for _, overwrite := range channel.Overwrites {
			if overwrite.Type == 0 && overwrite.ID == authority.guild {
				a, err := strconv.ParseUint(overwrite.Allow, 10, 64)
				if err != nil {
					return 0, err
				}
				d, err := strconv.ParseUint(overwrite.Deny, 10, 64)
				if err != nil {
					return 0, err
				}
				permissions = (permissions &^ d) | a
			}
		}
		for _, overwrite := range channel.Overwrites {
			if overwrite.Type == 0 && overwrite.ID != authority.guild && authority.roles[overwrite.ID] {
				a, err := strconv.ParseUint(overwrite.Allow, 10, 64)
				if err != nil {
					return 0, err
				}
				d, err := strconv.ParseUint(overwrite.Deny, 10, 64)
				if err != nil {
					return 0, err
				}
				allow |= a
				deny |= d
			}
		}
		permissions = (permissions &^ deny) | allow
		for _, overwrite := range channel.Overwrites {
			if overwrite.Type == 1 && overwrite.ID == authority.botID {
				a, err := strconv.ParseUint(overwrite.Allow, 10, 64)
				if err != nil {
					return 0, err
				}
				d, err := strconv.ParseUint(overwrite.Deny, 10, 64)
				if err != nil {
					return 0, err
				}
				permissions = (permissions &^ d) | a
			}
		}
		if permissions&(1<<10) == 0 || permissions&(1<<16) == 0 {
			return 0, fmt.Errorf("%w: VIEW_CHANNEL and READ_MESSAGE_HISTORY required", errDiscordAccessDenied)
		}
	}

	if !authority.messageContent {
		return 0, fmt.Errorf("%w: MESSAGE_CONTENT required", errDiscordAccessDenied)
	}
	return permissions, nil
}

func (c *CollaborationConnector) discordArchivePaths(ctx context.Context, guild string, parent discordChannel) ([]string, error) {
	paths := []string{"threads/archived/public"}
	// Private threads exist only under guild text channels, not forums, media,
	// or announcement channels.
	if parent.Type == 0 {
		authority, err := c.discordAuthority(ctx, guild)
		if err != nil {
			return nil, err
		}
		permissions, err := authority.checkedPermissions(parent)
		if err != nil {
			return nil, err
		}
		if permissions&(8|1<<34) != 0 {
			paths = append(paths, "threads/archived/private")
		} else {
			paths = append(paths, "users/@me/threads/archived/private")
		}
	}
	return paths, nil
}
func (c *CollaborationConnector) discordThreads(ctx context.Context, config CollaborationConfig, parent discordChannel) ([]string, error) {
	defer c.beginDiscordReadPass()()
	ids := []string{}
	active, err := c.discordActive(ctx, config.GuildID)
	if err != nil {
		return nil, err
	}
	for _, thread := range active {
		if thread.ParentID == parent.ID {
			ids = append(ids, thread.ID)
		}
	}
	paths, err := c.discordArchivePaths(ctx, config.GuildID, parent)
	if err != nil {
		return nil, err
	}
	for _, suffix := range paths {
		before := ""
		seen := map[string]bool{}
		for {
			q := url.Values{"limit": {"100"}}
			if before != "" {
				q.Set("before", before)
			}
			var result struct {
				Threads []discordChannel `json:"threads"`
				HasMore bool             `json:"has_more"`
			}
			if err := c.get(ctx, ProviderDiscord, "/channels/"+parent.ID+"/"+suffix+"?"+q.Encode(), &result); err != nil {
				return nil, err
			}
			for _, thread := range result.Threads {
				if thread.ParentID != parent.ID {
					return nil, ErrInventoryIncomplete
				}
				ids = append(ids, thread.ID)
				if len(ids) > maxCollaborationSourceObjects {
					return nil, ErrInventoryIncomplete
				}
			}
			if !result.HasMore {
				break
			}
			if len(result.Threads) == 0 {
				return nil, ErrInventoryIncomplete
			}
			last := result.Threads[len(result.Threads)-1]
			before = last.ThreadMetadata.ArchivedAt
			if strings.Contains(suffix, "@me") {
				before = last.ID
			}
			if before == "" {
				return nil, ErrInventoryIncomplete
			}
			if err := paginationToken(seen, before); err != nil {
				return nil, err
			}
		}
	}
	return ids, nil
}
func (c *CollaborationConnector) discordRefs(ctx context.Context, config CollaborationConfig) ([]collaborationRef, error) {
	defer c.beginDiscordReadPass()()
	if config.preview {
		return c.discordPreviewRefs(ctx, config)
	}
	channels := append([]string{}, config.ChannelIDs...)
	for _, id := range config.ChannelIDs {
		if err := c.discordScope(ctx, config, id); err != nil {
			return nil, err
		}
		channel, err := c.discordChannel(ctx, id)
		if err != nil {
			return nil, err
		}
		if (channel.Type == 0 || channel.Type == 5 || channel.Type == 15 || channel.Type == 16) && (config.IncludeThreads == nil || *config.IncludeThreads) {
			threads, err := c.discordThreads(ctx, config, channel)
			if err != nil {
				return nil, err
			}
			channels = append(channels, threads...)
		}
	}
	refs := []collaborationRef{}
	seenChannels := map[string]bool{}
	for _, id := range channels {
		if seenChannels[id] {
			continue
		}
		seenChannels[id] = true
		if len(seenChannels) > maxCollaborationSourceObjects {
			return nil, ErrInventoryIncomplete
		}
		if err := c.discordScope(ctx, config, id); err != nil {
			return nil, err
		}
		channel, err := c.discordChannel(ctx, id)
		if err != nil {
			return nil, err
		}
		if channel.Type == 15 || channel.Type == 16 {
			continue
		}
		before := ""
		seen := map[string]bool{}
		for {
			q := url.Values{"limit": {"100"}}
			if before != "" {
				q.Set("before", before)
			}
			var messages []discordMessage
			if err := c.get(ctx, ProviderDiscord, "/channels/"+id+"/messages?"+q.Encode(), &messages); err != nil {
				return nil, err
			}
			for _, message := range messages {
				if !discordID(message.ID) {
					return nil, ErrInventoryIncomplete
				}
				refs = append(refs, collaborationRef{Resource: ProviderDiscord, Container: id, ID: message.ID})
				if config.preview {
					return refs, nil
				}
				if len(refs) > maxCollaborationSourceObjects {
					return nil, ErrInventoryIncomplete
				}
			}
			if len(messages) == 0 {
				break
			}
			before = messages[len(messages)-1].ID
			if err := paginationToken(seen, before); err != nil {
				return nil, err
			}
		}
	}
	return refs, nil
}

type discordMessage struct {
	ID        string `json:"id"`
	ChannelID string `json:"channel_id"`
	Content   string `json:"content"`
	Timestamp string `json:"timestamp"`
	Edited    string `json:"edited_timestamp"`
	Author    struct {
		ID string `json:"id"`
	} `json:"author"`
	Reference   any `json:"message_reference"`
	Attachments []struct {
		ID   string `json:"id"`
		Name string `json:"filename"`
		MIME string `json:"content_type"`
		Size int64  `json:"size"`
		URL  string `json:"url"`
	} `json:"attachments"`
	Embeds []struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		URL         string `json:"url"`
	} `json:"embeds"`
}

func (c *CollaborationConnector) discordLoad(ctx context.Context, config CollaborationConfig, ref collaborationRef) (collaborationMaterial, error) {
	var message discordMessage
	if err := c.get(ctx, ProviderDiscord, "/channels/"+ref.Container+"/messages/"+ref.ID, &message); err != nil {
		return collaborationMaterial{}, err
	}
	if message.ID != ref.ID || message.ChannelID != ref.Container {
		return collaborationMaterial{}, fmt.Errorf("Discord message identity changed")
	}
	text := message.Content
	for _, embed := range message.Embeds {
		text += "\n" + embed.Title + "\n" + embed.Description + "\n" + embed.URL
	}
	files := []collaborationFile{}
	for _, file := range message.Attachments {
		if !config.Attachments {
			continue
		}
		if !discordID(file.ID) || file.Size > maxImportSourceObjectBytes || file.URL == "" {
			return collaborationMaterial{}, fmt.Errorf("invalid or oversized Discord attachment")
		}
		files = append(files, collaborationFile{ID: file.ID, Name: file.Name, MIME: file.MIME, URL: file.URL})
	}
	return collaborationMaterial{Text: []byte(text), URL: "https://discord.com/channels/" + config.GuildID + "/" + ref.Container + "/" + ref.ID, Title: ref.ID, Metadata: map[string]any{"guild_id": config.GuildID, "channel_id": ref.Container, "author_id": message.Author.ID, "timestamp": message.Timestamp, "edited_timestamp": message.Edited, "message_reference": message.Reference}, Files: files}, nil
}
