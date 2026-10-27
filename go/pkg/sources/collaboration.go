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
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// CollaborationConfig selects one independently shared source on a reusable
// connection. A Workspace connection can back many Drive, mail and calendar sources.
type CollaborationConfig struct {
	preview        bool
	tokens         map[string]string
	Resource       string   `json:"resource,omitempty"`
	TeamID         string   `json:"team_id,omitempty"`
	GuildID        string   `json:"guild_id,omitempty"`
	ChannelIDs     []string `json:"channel_ids,omitempty"`
	LabelIDs       []string `json:"label_ids,omitempty"`
	CalendarIDs    []string `json:"calendar_ids,omitempty"`
	FolderID       string   `json:"folder_id,omitempty"`
	SharedDriveID  string   `json:"shared_drive_id,omitempty"`
	IncludeThreads *bool    `json:"include_threads,omitempty"`
	Attachments    bool     `json:"attachments,omitempty"`
}

// CollaborationConnector owns portable acquisition only. Cloud injects current
// tenant credentials; the standalone runner supplies environment-backed clients.
// API base overrides are trusted application/test settings, never source config.
type CollaborationConnector struct {
	Baseline                                    *Inventory
	Cache                                       AcquisitionCache
	ContentStore                                AcquisitionContentStore
	retainedRef                                 string
	retainedScope                               string
	retainedBundle                              []Input
	hydratedItems                               map[string][]Item
	previewInputs                               map[string]Input
	previewSelection                            string
	ExtractorVersion                            string
	scopeChecks                                 map[string]time.Time
	calendarRoles                               map[string]string
	accessAuditWindow                           time.Duration
	accessWait                                  func(context.Context, time.Duration) error
	observedScopes                              map[string]collaborationRef
	ProviderName                                string
	HTTPClient                                  *http.Client
	AccessToken                                 func(context.Context) (string, error)
	Services                                    []string
	APIBase, GmailBase, CalendarBase, DriveBase string
	Extractor                                   AttachmentExtractor
	DownloadTransport                           http.RoundTripper
}

type collaborationRef struct {
	Resource   string `json:"resource"`
	Container  string `json:"container"`
	ID         string `json:"id"`
	Attachment string `json:"attachment,omitempty"`
	Thread     string `json:"thread,omitempty"`
}
type collaborationFile struct {
	ID, Name, MIME, URL string
	CanonicalURL        string
	Revision            string
	Data                []byte
	driveFile           bool
}
type collaborationMaterial struct {
	Text       []byte
	URL, Title string
	Metadata   map[string]any
	Files      []collaborationFile
}

type ProviderHTTPError struct {
	Provider string
	Status   int
	Code     string
	RetryAt  time.Time
}

func (e *ProviderHTTPError) Error() string {
	return fmt.Sprintf("%s source returned HTTP %d (%s)", e.Provider, e.Status, e.Code)
}
func (e *ProviderHTTPError) AntflyRetryAfter() time.Time { return e.RetryAt }
func (e *ProviderHTTPError) RateLimited() bool {
	return e.Status == 429 || (e.Provider == ProviderGoogleWorkspace && e.Status == 403 && googleRateLimitReason(e.Code))
}
func (e *ProviderHTTPError) Retryable() bool { return e.RateLimited() || e.Status >= 500 }

func (c *CollaborationConnector) Provider() string { return c.ProviderName }

// ConnectionIdentity validates a chat token without enumerating private content.
func (c *CollaborationConnector) ConnectionIdentity(ctx context.Context) (map[string]string, error) {
	switch c.ProviderName {
	case ProviderSlack:
		var auth struct {
			TeamID string `json:"team_id"`
			UserID string `json:"user_id"`
		}
		if err := c.get(ctx, ProviderSlack, "/auth.test", &auth); err != nil {
			return nil, err
		}
		if auth.TeamID == "" || auth.UserID == "" {
			return nil, fmt.Errorf("Slack connection identity missing")
		}
		return map[string]string{"team_id": auth.TeamID, "user_id": auth.UserID}, nil
	case ProviderDiscord:
		var user struct {
			ID  string `json:"id"`
			Bot bool   `json:"bot"`
		}
		if err := c.get(ctx, ProviderDiscord, "/users/@me", &user); err != nil {
			return nil, err
		}
		if !user.Bot || !discordID(user.ID) {
			return nil, fmt.Errorf("Discord source requires a bot connection")
		}
		return map[string]string{"bot_id": user.ID}, nil
	}
	return nil, fmt.Errorf("unsupported chat connection")
}
func (c *CollaborationConnector) Validate(_ context.Context, r Request) error {
	config, err := decodeSourceConfig[CollaborationConfig](r.Config)
	if err != nil {
		return err
	}
	if r.Mode != ImportModeSource {
		return fmt.Errorf("%w: source mode required", ErrInvalidInput)
	}
	validIDs := func(ids []string) bool {
		if len(ids) == 0 || len(ids) > 100 {
			return false
		}
		seen := map[string]bool{}
		for _, id := range ids {
			if id == "" || len(id) > 512 || strings.ContainsAny(id, "/\\\x00\r\n?#") || seen[id] {
				return false
			}
			seen[id] = true
		}
		return true
	}
	switch c.ProviderName {
	case ProviderSlack:
		if !googleFileID(config.TeamID) || !validIDs(config.ChannelIDs) {
			return fmt.Errorf("%w: team_id and channel_ids required", ErrInvalidInput)
		}
	case ProviderDiscord:
		if !discordID(config.GuildID) || !validIDs(config.ChannelIDs) {
			return fmt.Errorf("%w: guild_id and channel_ids required", ErrInvalidInput)
		}
		for _, id := range config.ChannelIDs {
			if !discordID(id) {
				return fmt.Errorf("%w: invalid Discord channel", ErrInvalidInput)
			}
		}
	case ProviderGoogleWorkspace:
		if config.Resource != "drive" && config.Resource != "gmail" && config.Resource != "calendar" {
			return fmt.Errorf("%w: Workspace resource must be drive, gmail or calendar", ErrInvalidInput)
		}
		services := c.Services
		if len(services) == 0 {
			services = []string{"drive"}
		}
		allowed := false
		for _, service := range services {
			if service == config.Resource {
				allowed = true
			}
		}
		if !allowed {
			return fmt.Errorf("%w: reconnect Google Workspace with %s access", ErrInvalidInput, config.Resource)
		}
		if config.Resource == "calendar" && !validIDs(config.CalendarIDs) {
			return fmt.Errorf("%w: calendar_ids required", ErrInvalidInput)
		}
		if config.Resource == "gmail" && len(config.LabelIDs) > 0 && !validIDs(config.LabelIDs) {
			return fmt.Errorf("%w: invalid label_ids", ErrInvalidInput)
		}
		if config.Resource == "drive" {
			return c.drive().Validate(context.Background(), r)
		}
	default:
		return fmt.Errorf("%w: unsupported collaboration provider", ErrInvalidInput)
	}
	return nil
}

func (c *CollaborationConnector) drive() *GoogleConnector {
	return &GoogleConnector{ProviderName: ProviderGoogleDrive, HTTPClient: c.HTTPClient, APIBase: c.DriveBase}
}
func (c *CollaborationConnector) base(resource string) string {
	if c.APIBase != "" {
		return strings.TrimRight(c.APIBase, "/")
	}
	switch resource {
	case "gmail":
		if c.GmailBase != "" {
			return c.GmailBase
		}
		return "https://gmail.googleapis.com/gmail/v1/users/me"
	case "calendar":
		if c.CalendarBase != "" {
			return c.CalendarBase
		}
		return "https://www.googleapis.com/calendar/v3"
	case ProviderSlack:
		return "https://slack.com/api"
	default:
		return "https://discord.com/api/v10"
	}
}
func (c *CollaborationConnector) get(ctx context.Context, resource, path string, out any) error {
	cacheKey := c.base(resource) + path
	if c.Cache != nil && inventoryPage(path) {
		data, found, err := c.Cache.Load(ctx, cacheKey)
		if err != nil {
			return err
		}
		if found {
			return json.Unmarshal(data, out)
		}
	}
	if c.HTTPClient == nil {
		return fmt.Errorf("authorized HTTP client required")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base(resource)+path, nil)
	if err != nil {
		return err
	}
	if c.AccessToken != nil {
		token, err := c.AccessToken(ctx)
		if err != nil {
			return err
		}
		scheme := "Bearer "
		if c.ProviderName == ProviderDiscord {
			scheme = "Bot "
		}
		req.Header.Set("Authorization", scheme+token)
	}
	client := *c.HTTPClient
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		if c.ProviderName == ProviderGoogleWorkspace {
			return googleRequestError(err)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		if resource == "gmail" || resource == "calendar" {
			api := googleResponseError(resp)
			return &ProviderHTTPError{Provider: c.ProviderName, Status: api.Status, Code: api.Reason, RetryAt: api.RetryAt}
		}
		return &ProviderHTTPError{Provider: c.ProviderName, Status: resp.StatusCode, RetryAt: providerRetryAt(resp.Header.Get("Retry-After"), time.Now())}
	}
	b, err := readBounded(resp.Body)
	if err != nil {
		return err
	}
	if resource == ProviderSlack {
		var envelope struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		if err := json.Unmarshal(b, &envelope); err != nil {
			return err
		}
		if !envelope.OK {
			return SlackAPIError(envelope.Error, providerRetryAt(resp.Header.Get("Retry-After"), time.Now()))
		}
	}
	if err := json.Unmarshal(b, out); err != nil {
		return err
	}
	if c.Cache != nil && inventoryPage(path) {
		return c.Cache.Save(ctx, cacheKey, acquisitionMetadata(resource, path, b))
	}
	return nil
}

func (c *CollaborationConnector) Enumerate(ctx context.Context, r Request) (Inventory, error) {
	// Previews are fresh, bounded and ephemeral even when the caller configured
	// durable acquisition storage for normal synchronization.
	if r.Preview {
		cache := c.Cache
		c.Cache = nil
		defer func() { c.Cache = cache }()
	}
	c.previewInputs = nil
	c.previewSelection = c.hydrationSelection(r)
	c.hydratedItems = nil
	c.retainedBundle = nil
	c.retainedRef = ""
	c.scopeChecks = nil
	c.calendarRoles = nil
	c.observedScopes = nil
	if r.SourceNamespace == "" {
		return Inventory{}, fmt.Errorf("%w: source namespace required", ErrInvalidInput)
	}
	if err := c.Validate(ctx, r); err != nil {
		return Inventory{}, err
	}
	config, _ := decodeSourceConfig[CollaborationConfig](r.Config)
	config.preview = r.Preview
	if c.ProviderName == ProviderGoogleWorkspace && config.Resource == "drive" {
		inv, err := c.drive().Enumerate(ctx, r)
		for i := range inv.Objects {
			inv.Objects[i].SourceKind = ProviderGoogleWorkspace
			inv.Objects[i].Metadata["resource"] = "drive"
			if config.Attachments {
				inv.Objects[i].Metadata["native_size"] = inv.Objects[i].Size
				inv.Objects[i].Metadata["size_is_upper_bound"] = true
				inv.Objects[i].Size = maxImportSourceObjectBytes
			}
		}
		return inv, err
	}
	var refs []collaborationRef
	var err error
	var removed map[string]bool
	var providerState map[string]string
	incremental := false
	switch c.ProviderName {
	case ProviderSlack:
		var capturedBytes int64
		var capturedObjects int
		refs, err = c.slackRefs(ctx, config, func(ref collaborationRef, message *slackMessage) error {
			if c.ContentStore == nil && !r.Preview {
				return nil
			}
			inputs, captureErr := c.hydrate(ctx, r, config, ref, message)
			if captureErr != nil {
				return captureErr
			}
			for _, input := range inputs {
				capturedBytes += input.Size
				capturedObjects++
			}
			if capturedBytes > 10<<30 || capturedObjects > maxCollaborationSourceObjects {
				return ErrInventoryIncomplete
			}
			return nil
		})
	case ProviderDiscord:
		refs, err = c.discordRefs(ctx, config)
	case ProviderGoogleWorkspace:
		refs, removed, providerState, incremental, err = c.workspaceSelection(ctx, r, config)
	}
	if err != nil {
		return Inventory{}, err
	}
	inv := Inventory{Authoritative: !r.Preview && len(r.RetrySourceKeys) == 0, BatchObjects: 1, BatchBytes: maxImportSourceObjectBytes}
	inv.ProviderState = providerState
	seen := map[string]bool{}
	var total int64
	if incremental {
		changed := map[string]bool{}
		for _, ref := range refs {
			changed[refKey(ref.Resource, ref.Container, ref.ID)] = true
		}
		for _, item := range c.Baseline.Objects {
			key := refKey(configString(item.Metadata, "resource"), configString(item.Metadata, "container_id"), configString(item.Metadata, "remote_id"))
			if removed[key] || changed[key] {
				continue
			}
			if seen[item.ID] {
				return Inventory{}, ErrInventoryIncomplete
			}
			seen[item.ID] = true
			inv.Objects = append(inv.Objects, item)
			total += item.Size
		}
	}
	for _, ref := range refs {
		items, err := c.hydrate(ctx, r, config, ref, nil)
		if err != nil {
			return Inventory{}, err
		}

		for _, input := range items {
			if seen[input.ID] {
				continue
			}
			seen[input.ID] = true
			total += input.Size
			if total > 10<<30 || len(inv.Objects) >= maxCollaborationSourceObjects {
				return Inventory{}, ErrInventoryIncomplete
			}
			inv.Objects = append(inv.Objects, input.Item)
		}
	}
	sort.Slice(inv.Objects, func(i, j int) bool { return inv.Objects[i].ID < inv.Objects[j].ID })
	if r.Preview && len(inv.Objects) > PreviewItemLimit {
		inv.Objects = inv.Objects[:PreviewItemLimit]
	}
	if len(r.RetrySourceKeys) > 0 {
		keys := exactSourceKeySet(r.RetrySourceKeys)
		var selected []Item
		for _, o := range inv.Objects {
			if _, ok := keys[o.Path]; ok {
				selected = append(selected, o)
				delete(keys, o.Path)
			}
		}
		if len(keys) > 0 {
			return Inventory{}, fmt.Errorf("%w: retry resources unavailable", ErrInvalidInput)
		}
		inv.Objects = selected
	}
	b, _ := json.Marshal(inv.Objects)
	digest := sha256.Sum256(b)
	inv.SourceRevision = hex.EncodeToString(digest[:])
	return inv, nil
}
func (c *CollaborationConnector) materialItems(ctx context.Context, r Request, config CollaborationConfig, ref collaborationRef, m collaborationMaterial) ([]Input, error) {
	if config.preview && len(m.Files) > PreviewItemLimit-1 {
		m.Files = m.Files[:PreviewItemLimit-1]
	}
	if len(m.Files) > 1000 {
		return nil, fmt.Errorf("%w: too many attachments in one parent", ErrInventoryIncomplete)
	}
	if len(m.Text) > maxImportSourceObjectBytes || !utf8.Valid(m.Text) {
		return nil, fmt.Errorf("resource text exceeds byte limit")
	}
	metadata := cloneSourceMetadata(m.Metadata)
	metadata["resource"] = ref.Resource
	metadata["container_id"] = ref.Container
	metadata["remote_id"] = ref.ID
	metadata["links"] = SourceLinks(m.Text)
	attachments := []map[string]string{}
	for _, file := range m.Files {
		attachments = append(attachments, map[string]string{"id": file.ID, "filename": file.Name, "mime_type": file.MIME})
	}
	metadata["attachments"] = attachments
	makeInput := func(ref collaborationRef, text []byte, meta map[string]any) Input {
		raw, _ := json.Marshal(ref)
		meta["acquisition"] = string(raw)
		key := ref.Resource + "/" + url.PathEscape(ref.Container) + "/" + url.PathEscape(ref.ID)
		if ref.Attachment != "" {
			key += "/attachments/" + url.PathEscape(ref.Attachment)
		}
		// Include canonical metadata so edits to links/sharing context are revisions.
		h := sha256.New()
		h.Write(text)
		b, _ := json.Marshal(meta)
		h.Write(b)
		scheme := c.ProviderName
		if scheme == ProviderGoogleWorkspace {
			scheme = "workspace"
		}
		return Input{Item: Item{ID: scheme + "://" + r.SourceNamespace + "/" + key, Path: key, Filename: m.Title, URL: m.URL, Revision: hex.EncodeToString(h.Sum(nil)), Size: int64(len(text)), SourceKind: c.ProviderName, ReadMode: sourceReadBytes, Metadata: meta}, Bytes: text}
	}
	parent := makeInput(ref, m.Text, metadata)
	items := []Input{parent}
	totalBytes := len(m.Text)
	if config.Attachments {
		for _, file := range m.Files {
			remaining := maxImportSourceObjectBytes - totalBytes
			if remaining <= 0 {
				return nil, fmt.Errorf("%w: attachment budget exhausted", ErrInvalidInput)
			}
			if file.driveFile {
				var err error
				file, err = c.workspaceDriveFile(ctx, file.ID, remaining)
				if err != nil {
					return nil, err
				}
			}
			data := file.Data
			if file.URL != "" {
				var err error
				data, err = c.downloadAttachment(ctx, file.URL)
				if err != nil {
					return nil, err
				}
			}
			if len(data) > remaining {
				return nil, fmt.Errorf("%w: attachment exceeds remaining byte budget", ErrInvalidInput)
			}
			extract := c.Extractor
			if extract == nil {
				extract = ExtractAttachmentText
			}
			text, err := extract(ctx, file.Name, file.MIME, data)
			if err != nil {
				return nil, fmt.Errorf("attachment %s: %w", file.Name, err)
			}
			totalBytes += len(data) + len(text)
			if len(text) > maxImportSourceObjectBytes || totalBytes > maxImportSourceObjectBytes || !utf8.Valid(text) {
				return nil, fmt.Errorf("extracted attachment exceeds byte limit")
			}
			meta := cloneSourceMetadata(metadata)
			meta["parent_identity"] = parent.ID
			meta["attachment_id"] = file.ID
			if file.Revision != "" {
				meta["drive_revision"] = file.Revision
			}
			meta["filename"] = file.Name
			meta["mime_type"] = file.MIME
			meta["links"] = SourceLinks(text)
			digest := sha256.Sum256(data)
			meta["attachment_sha256"] = hex.EncodeToString(digest[:])
			attachmentRef := ref
			attachmentRef.Attachment = file.ID
			input := makeInput(attachmentRef, text, meta)
			input.Filename = file.Name
			if file.CanonicalURL != "" {
				input.URL = file.CanonicalURL
			}
			items = append(items, input)
		}
	}
	return items, nil
}
func (c *CollaborationConnector) load(ctx context.Context, config CollaborationConfig, ref collaborationRef) (collaborationMaterial, error) {
	switch ref.Resource {
	case ProviderSlack:
		return c.slackLoad(ctx, config, ref)
	case ProviderDiscord:
		return c.discordLoad(ctx, config, ref)
	case "gmail":
		return c.gmailLoad(ctx, config, ref)
	case "calendar":
		return c.calendarLoad(ctx, config, ref)
	default:
		return collaborationMaterial{}, fmt.Errorf("%w: invalid resource", ErrInvalidInput)
	}
}
func (c *CollaborationConnector) Read(ctx context.Context, r Request, item Item) ([]byte, error) {
	if err := c.Validate(ctx, r); err != nil {
		return nil, err
	}
	config, _ := decodeSourceConfig[CollaborationConfig](r.Config)
	config.preview = r.Preview
	if c.ProviderName == ProviderGoogleWorkspace && config.Resource == "drive" {
		return c.readWorkspaceDrive(ctx, r, item, config.Attachments)
	}
	var ref collaborationRef
	if err := json.Unmarshal([]byte(configString(item.Metadata, "acquisition")), &ref); err != nil {
		return nil, fmt.Errorf("invalid persisted acquisition")
	}
	if r.Preview && c.previewSelection == c.hydrationSelection(r) {
		if input, ok := c.previewInputs[item.ID]; ok && input.Revision == item.Revision {
			if err := c.authorizeRetained(ctx, config, ref, item); err != nil {
				return nil, err
			}
			return input.Bytes, nil
		}
	}
	if item.ContentRef != "" && c.ContentStore != nil {
		inputs, found, err := c.retainedInputs(ctx, r, item.ContentRef)
		if err != nil {
			return nil, err
		}
		if found {
			if err := c.authorizeRetained(ctx, config, ref, item); err != nil {
				return nil, err
			}
			for _, input := range inputs {
				if input.ID == item.ID && input.Revision == item.Revision {
					return input.Bytes, nil
				}
			}
			return nil, fmt.Errorf("retained acquisition does not match pinned item")
		}
	}
	if err := c.validateRef(ctx, config, ref); err != nil {
		return nil, err
	}
	attachment := ref.Attachment
	ref.Attachment = ""
	fresh := *c
	fresh.Cache = nil
	fresh.hydratedItems = nil
	fresh.retainedRef = ""
	fresh.retainedBundle = nil
	items, err := fresh.hydrate(ctx, r, config, ref, nil)
	if err != nil {
		return nil, err
	}
	for _, input := range items {
		if input.ID == item.ID && (attachment == "" || configString(input.Metadata, "attachment_id") == attachment) {
			if input.Revision != item.Revision {
				return nil, &RevisionChangedError{Expected: item.Revision, Actual: input.Revision}
			}
			return input.Bytes, nil
		}
	}
	return nil, fmt.Errorf("resource or attachment no longer accessible")
}
func (c *CollaborationConnector) CheckAccess(ctx context.Context, r Request, item Item) error {
	if c.ContentStore != nil && item.ContentRef != "" {
		if err := c.Validate(ctx, r); err != nil {
			return err
		}
		config, _ := decodeSourceConfig[CollaborationConfig](r.Config)
		var ref collaborationRef
		if json.Unmarshal([]byte(configString(item.Metadata, "acquisition")), &ref) != nil {
			return ErrInvalidInput
		}
		return c.authorizeRetained(ctx, config, ref, item)
	}
	_, err := c.Read(ctx, r, item)
	return err
}
func (c *CollaborationConnector) ValidateInventory(ctx context.Context, r Request, inventory Inventory) error {
	c.scopeChecks = nil
	c.calendarRoles = nil
	if err := c.Validate(ctx, r); err != nil {
		return err
	}
	config, _ := decodeSourceConfig[CollaborationConfig](r.Config)
	config.preview = r.Preview
	if c.ProviderName == ProviderGoogleWorkspace && config.Resource == "drive" {
		return c.drive().ValidateInventory(ctx, r, inventory)
	}
	for _, item := range inventory.Objects {
		if item.ContentRef == "" {
			continue
		}
		var ref collaborationRef
		if json.Unmarshal([]byte(configString(item.Metadata, "acquisition")), &ref) != nil {
			return ErrInvalidInput
		}
		if err := c.authorizeRetained(ctx, config, ref, item); err != nil {
			return err
		}
	}
	switch c.ProviderName {
	case ProviderSlack:
		for _, id := range config.ChannelIDs {
			if err := c.slackScope(ctx, config, id); err != nil {
				return err
			}
		}
	case ProviderDiscord:
		for _, ref := range c.observedScopes {
			if err := c.discordScope(ctx, config, ref.Container); err != nil {
				return err
			}
		}
		for _, id := range config.ChannelIDs {
			if err := c.discordScope(ctx, config, id); err != nil {
				return err
			}
		}
	case ProviderGoogleWorkspace:
		if config.Resource == "gmail" {
			_, err := c.gmailAccount(ctx)
			if err != nil {
				return err
			}
			for _, label := range config.LabelIDs {
				var out any
				if err := c.get(ctx, "gmail", "/labels/"+url.PathEscape(label), &out); err != nil {
					return err
				}
			}
		}
		if config.Resource == "calendar" {
			for _, id := range config.CalendarIDs {
				if _, err := c.calendarID(ctx, id); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
func (c *CollaborationConnector) downloadAttachment(ctx context.Context, raw string) ([]byte, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.Port() != "" {
		return nil, fmt.Errorf("invalid attachment download URL")
	}
	allowed := false
	switch c.ProviderName {
	case ProviderSlack:
		allowed = u.Hostname() == "files.slack.com"
	case ProviderDiscord:
		allowed = u.Hostname() == "cdn.discordapp.com" || u.Hostname() == "media.discordapp.net"
	}
	if !allowed {
		return nil, fmt.Errorf("attachment host is not a provider download origin")
	}
	// Do not pass OAuth transports or provider credentials to external links.
	client := &http.Client{Transport: c.DownloadTransport, Timeout: 60 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	if c.ProviderName == ProviderSlack {
		if c.AccessToken == nil {
			return nil, fmt.Errorf("Slack token required")
		}
		token, err := c.AccessToken(ctx)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		var urlError *url.Error
		for errors.As(err, &urlError) {
			err = urlError.Err
		}
		return nil, fmt.Errorf("attachment download failed: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, &ProviderHTTPError{Provider: c.ProviderName, Status: resp.StatusCode, RetryAt: providerRetryAt(resp.Header.Get("Retry-After"), time.Now())}
	}
	return readBounded(resp.Body)
}

// SourceLink identifies related resources without granting access or dereferencing
// arbitrary URLs. Consumers must authorize the target source independently.
type SourceLink struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	URL  string `json:"url"`
}

func SourceLinks(text []byte) []SourceLink {
	links := []SourceLink{}
	seen := map[string]bool{}
	for _, word := range strings.FieldsFunc(string(text), func(r rune) bool {
		return r == '<' || r == '>' || r == '"' || r == '\'' || r == '(' || r == ')' || r == ' ' || r == '\n' || r == '\r' || r == '\t'
	}) {
		u, err := url.Parse(strings.TrimRight(word, ".,;"))
		if err != nil || u.Scheme != "https" || u.User != nil {
			continue
		}
		parts := strings.Split(strings.Trim(u.Path, "/"), "/")
		kind, id := "", ""
		switch u.Hostname() {
		case "drive.google.com":
			kind = "drive"
			id = u.Query().Get("id")
		case "docs.google.com":
			kind = "drive"
		case "calendar.google.com":
			kind = "calendar"
			id = u.Query().Get("eid")
		case "mail.google.com":
			kind = "gmail"
			id = u.Fragment
		}
		if kind == "drive" {
			for i := 0; i+1 < len(parts); i++ {
				if parts[i] == "d" {
					id = parts[i+1]
					break
				}
			}
		}
		if kind == "" || id == "" || seen[kind+":"+id] {
			continue
		}
		seen[kind+":"+id] = true
		canonical := ""
		switch kind {
		case "drive":
			if !googleFileID(id) {
				continue
			}
			canonical = "https://drive.google.com/file/d/" + url.PathEscape(id) + "/view"
		case "calendar":
			canonical = "https://calendar.google.com/calendar/event?eid=" + url.QueryEscape(id)
		case "gmail":
			canonical = "https://mail.google.com/mail/u/0/#" + url.PathEscape(id)
		}
		links = append(links, SourceLink{kind, id, canonical})
		if len(links) >= 1000 {
			break
		}
	}
	sort.Slice(links, func(i, j int) bool { return links[i].Kind+links[i].ID < links[j].Kind+links[j].ID })
	return links
}

func paginationToken(seen map[string]bool, token string) error {
	if token != "" && (seen[token] || len(seen) >= maxCollaborationSourceObjects) {
		return ErrInventoryIncomplete
	}
	seen[token] = true
	return nil
}
func discordID(id string) bool {
	if len(id) < 1 || len(id) > 20 {
		return false
	}
	for _, r := range id {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

var _ Connector = (*CollaborationConnector)(nil)

// SlackAPIError classifies Slack Web API error envelopes independently of their
// HTTP status. Unknown failures remain retryable and never imply token revocation.
func SlackAPIError(code string, retryAt time.Time) *ProviderHTTPError {
	status := 502
	switch code {
	case "ratelimited", "rate_limited":
		status = 429
		if retryAt.IsZero() {
			retryAt = time.Now().Add(time.Minute)
		}
	case "internal_error", "fatal_error", "service_unavailable", "request_timeout":
		status = 503
	case "invalid_auth", "not_authed", "token_revoked", "token_expired", "account_inactive":
		status = 401
	case "missing_scope", "no_permission", "restricted_action", "not_allowed_token_type", "enterprise_is_restricted":
		status = 403
	case "channel_not_found", "user_not_found":
		status = 404
	case "invalid_arguments", "invalid_arg_name", "invalid_array_arg", "invalid_charset", "invalid_form_data", "invalid_post_type", "missing_post_type":
		status = 400
	}
	return &ProviderHTTPError{Provider: ProviderSlack, Status: status, Code: code, RetryAt: retryAt}
}
func providerRetryAt(value string, now time.Time) time.Time {
	if seconds, err := strconv.ParseFloat(value, 64); err == nil && seconds >= 0 && seconds <= 86400 {
		return now.Add(time.Duration(seconds * float64(time.Second)))
	}
	if at, err := http.ParseTime(value); err == nil && !at.Before(now) && at.Sub(now) <= 24*time.Hour {
		return at
	}
	return time.Time{}
}
