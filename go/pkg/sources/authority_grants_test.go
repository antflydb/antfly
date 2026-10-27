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
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

func TestPermissionGrantCalendarConfidentialVisibilityRevokesPublishedDetails(t *testing.T) {
	for _, visibility := range []string{"private", "confidential"} {
		t.Run(visibility, func(t *testing.T) {
			c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar"}, APIBase: "https://fixture.test"}
			c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
				switch r.URL.Path {
				case "/users/me/calendarList/cal":
					return 200, map[string]any{"id": "cal", "accessRole": "reader"}
				case "/calendars/cal/events/event":
					return 200, map[string]any{"id": "event", "etag": "redacted-version", "visibility": visibility}
				default:
					t.Fatalf("unexpected audit request: %s", r.URL)
					return 500, nil
				}
			})
			ref, _ := json.Marshal(collaborationRef{Resource: "calendar", Container: "cal", ID: "event"})
			item := Item{ID: "workspace://job/test/calendar/cal/event", Metadata: map[string]any{"acquisition": string(ref), "details_visible": true, "etag": "published-full-details"}}
			r := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"resource":"calendar","calendar_ids":["cal"]}`)}
			err := c.AuthorizeInventory(t.Context(), r, Inventory{Objects: []Item{item}})
			require.Error(t, err, "a reader cannot renew a snapshot of previously visible private details")
		})
	}
}

func TestPermissionGrantSlackAuditRejectsMetadataOnlyGrant(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test", AccessToken: func(context.Context) (string, error) { return "metadata-only-token", nil }}
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		var body string
		switch r.URL.Path {
		case "/auth.test":
			body = `{"ok":true,"team_id":"T1","user_id":"U1"}`
		case "/conversations.info":
			body = `{"ok":true,"channel":{"id":"C1","is_member":true,"is_private":false}}`
		case "/conversations.history":
			body = `{"ok":false,"error":"missing_scope","needed":"channels:history","provided":"channels:read"}`
		default:
			t.Fatalf("unexpected request: %s", r.URL)
		}
		headers := make(http.Header)
		headers.Set("X-OAuth-Scopes", "channels:read")
		return &http.Response{StatusCode: 200, Header: headers, Request: r, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	ref, _ := json.Marshal(collaborationRef{Resource: ProviderSlack, Container: "C1", ID: "100.000001"})
	item := Item{ID: "slack://job/test/slack/C1/100.000001", Metadata: map[string]any{"acquisition": string(ref)}}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1"],"include_threads":false}`)}
	_, readErr := c.Read(t.Context(), r, item)
	var denied *ProviderHTTPError
	require.ErrorAs(t, readErr, &denied)
	require.Equal(t, 403, denied.Status, "the provider rejects content reads under the narrowed grant")
	require.Error(t, c.AuthorizeInventory(t.Context(), r, Inventory{Objects: []Item{item}}), "channel membership and metadata access alone cannot authorize retained message content")
}

func TestPermissionGrantDiscordModeratorCanEnumerateUnjoinedPrivateThread(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderDiscord, APIBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/channels/2":
			return 200, map[string]any{"id": "2", "guild_id": "1", "type": 0}
		case "/channels/10":
			return 200, map[string]any{"id": "10", "guild_id": "1", "parent_id": "2", "type": 12}
		case "/channels/10/thread-members/@me":
			return 404, map[string]any{"code": 10007, "message": "Unknown Member"}
		case "/users/@me":
			return 200, map[string]any{"id": "3"}
		case "/guilds/1/members/3":
			return 200, map[string]any{"roles": []string{"4"}}
		case "/guilds/1/roles":
			return 200, []any{map[string]any{"id": "1", "permissions": "66560"}, map[string]any{"id": "4", "permissions": "17179869184"}}
		case "/oauth2/applications/@me":
			return 200, map[string]any{"flags": uint64(1 << 19)}
		case "/guilds/1/threads/active":
			return 200, map[string]any{"threads": []any{map[string]any{"id": "10", "guild_id": "1", "parent_id": "2", "type": 12}}}
		case "/channels/2/threads/archived/public", "/channels/2/users/@me/threads/archived/private":
			return 200, map[string]any{"threads": []any{}, "has_more": false}
		case "/channels/2/messages":
			return 200, []any{}
		case "/channels/10/messages":
			if r.URL.Query().Get("before") != "" {
				return 200, []any{}
			}
			return 200, []any{map[string]any{"id": "20"}}
		default:
			t.Fatalf("unexpected request: %s", r.URL)
			return 500, nil
		}
	})
	var messages []discordMessage
	require.NoError(t, c.get(t.Context(), ProviderDiscord, "/channels/10/messages?limit=1", &messages), "moderator access does not require thread membership")
	require.Len(t, messages, 1)
	refs, err := c.discordRefs(t.Context(), CollaborationConfig{GuildID: "1", ChannelIDs: []string{"2"}})
	require.NoError(t, err, "one accessible unjoined private thread must not abort the entire selected channel inventory")
	require.Len(t, refs, 1)
}

func TestSlackLiveScopeProofMatchesSelectionAndAttachments(t *testing.T) {
	for _, tc := range []struct {
		name, scopes                 string
		private                      *bool
		attachments, bot, threadsOff bool
		status                       int
	}{
		{name: "public user", scopes: "channels:history", private: boolPointer(false)},
		{name: "private user", scopes: "groups:history", private: boolPointer(true)},
		{name: "public metadata only", scopes: "channels:read", private: boolPointer(false), status: 403},
		{name: "private history removed", scopes: "channels:history,groups:read", private: boolPointer(true), status: 403},
		{name: "attachments granted", scopes: "channels:history,files:read", private: boolPointer(false), attachments: true},
		{name: "attachments removed", scopes: "channels:history", private: boolPointer(false), attachments: true, status: 403},
		{name: "unknown channel type requires both grants", scopes: "channels:history", status: 403},
		{name: "unknown type with both grants", scopes: "channels:history,groups:history"},
		{name: "missing proof", status: 502},
		{name: "malformed proof", scopes: "channels:history,provider-secret!", status: 502},
		{name: "oversized proof", scopes: strings.Repeat("x", 17<<10), status: 502},
		{name: "bot history only", scopes: "channels:history", private: boolPointer(false), bot: true, threadsOff: true},
		{name: "bot cannot authorize user threads", scopes: "channels:history", private: boolPointer(false), bot: true, status: 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test"}
			c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
				var value any
				switch r.URL.Path {
				case "/auth.test":
					value = map[string]any{"ok": true, "team_id": "T1"}
					if tc.bot {
						value.(map[string]any)["bot_id"] = "B1"
					}
				case "/conversations.info":
					value = map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": true, "is_private": tc.private}}
				default:
					t.Fatalf("permission audit read content: %s", r.URL)
				}
				body, _ := json.Marshal(value)
				headers := make(http.Header)
				if tc.scopes != "" {
					headers.Set("X-OAuth-Scopes", tc.scopes)
				}
				return &http.Response{StatusCode: 200, Header: headers, Request: r, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
			})}
			config := CollaborationConfig{TeamID: "T1", ChannelIDs: []string{"C1"}, Attachments: tc.attachments}
			if tc.threadsOff {
				config.IncludeThreads = boolPointer(false)
			}
			raw, _ := json.Marshal(config)
			err := c.AuthorizeInventory(t.Context(), Request{Mode: ImportModeSource, Config: raw}, Inventory{})
			if tc.status == 0 {
				require.NoError(t, err)
				return
			}
			var api *ProviderHTTPError
			require.ErrorAs(t, err, &api)
			require.Equal(t, tc.status, api.Status)
			require.NotContains(t, err.Error(), "provider-secret")
		})
	}
}

func boolPointer(v bool) *bool { return &v }

func TestCalendarPrivacyNormalizationAndGrantRevocation(t *testing.T) {
	for _, visibility := range []string{"private", "confidential", "public", "default"} {
		for _, role := range []string{"reader", "writer", "owner"} {
			t.Run(visibility+"/"+role, func(t *testing.T) {
				c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar"}, APIBase: "https://fixture.test", calendarRoles: map[string]string{"cal": role}}
				c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
					if r.URL.Path == "/users/me/calendarList/cal" {
						return 200, map[string]any{"id": "cal", "accessRole": role}
					}
					require.Equal(t, "/calendars/cal/events/event", r.URL.Path)
					return 200, map[string]any{"id": "event", "etag": "v1", "visibility": visibility, "summary": "event"}
				})
				ref := collaborationRef{Resource: "calendar", Container: "cal", ID: "event"}
				material, err := c.calendarLoad(t.Context(), CollaborationConfig{}, ref)
				require.NoError(t, err)
				restricted := calendarPrivateVisibility(visibility) && role == "reader"
				require.Equal(t, !restricted, material.Metadata["details_visible"])
				raw, _ := json.Marshal(ref)
				item := Item{Metadata: map[string]any{"acquisition": string(raw), "details_visible": true, "etag": "v1"}}
				err = c.AuthorizeInventory(t.Context(), Request{Mode: ImportModeSource, Config: SourceConfig(`{"resource":"calendar","calendar_ids":["cal"]}`)}, Inventory{Objects: []Item{item}})
				if restricted {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestDiscordPrivateThreadChecksEffectiveModeratorPermissions(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		base, role, allow, deny uint64
		memberStatus            int
		expectedStatus          int
		memberCalls             int
	}{
		{name: "role moderator", base: 66560, role: 1 << 34, memberStatus: 404},
		{name: "administrator", base: 8, memberStatus: 404},
		{name: "channel moderator grant", base: 66560, allow: 1 << 34, memberStatus: 404},
		{name: "channel removes moderator grant", base: 66560, role: 1 << 34, deny: 1 << 34, memberStatus: 404, expectedStatus: 404, memberCalls: 1},
		{name: "moderator still needs history access", base: 66560, role: 1 << 34, deny: 1 << 16, memberStatus: 200, expectedStatus: 403},
		{name: "ordinary member", base: 66560, memberStatus: 200, memberCalls: 1},
		{name: "ordinary nonmember", base: 66560, memberStatus: 404, expectedStatus: 404, memberCalls: 1},
		{name: "membership throttled", base: 66560, memberStatus: 429, expectedStatus: 429, memberCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			memberCalls := 0
			c := &CollaborationConnector{ProviderName: ProviderDiscord, APIBase: "https://fixture.test"}
			c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
				switch r.URL.Path {
				case "/channels/10":
					return 200, map[string]any{"id": "10", "guild_id": "1", "parent_id": "2", "type": 12}
				case "/channels/2":
					return 200, map[string]any{"id": "2", "guild_id": "1", "type": 0, "permission_overwrites": []any{map[string]any{"id": "3", "type": 1, "allow": strconv.FormatUint(tc.allow, 10), "deny": strconv.FormatUint(tc.deny, 10)}}}
				case "/channels/10/thread-members/@me":
					memberCalls++
					return tc.memberStatus, map[string]any{}
				case "/users/@me":
					return 200, map[string]any{"id": "3"}
				case "/guilds/1/members/3":
					return 200, map[string]any{"roles": []string{"4"}}
				case "/guilds/1/roles":
					return 200, []any{map[string]any{"id": "1", "permissions": strconv.FormatUint(tc.base, 10)}, map[string]any{"id": "4", "permissions": strconv.FormatUint(tc.role, 10)}}
				case "/oauth2/applications/@me":
					return 200, map[string]any{"flags": uint64(1 << 19)}
				default:
					t.Fatalf("unexpected scope request: %s", r.URL)
					return 500, nil
				}
			})
			err := c.discordScope(t.Context(), CollaborationConfig{GuildID: "1", ChannelIDs: []string{"2"}}, "10")
			require.Equal(t, tc.memberCalls, memberCalls)
			if tc.expectedStatus == 0 {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			if tc.expectedStatus == 403 {
				require.ErrorIs(t, err, errDiscordAccessDenied)
			} else {
				var api *ProviderHTTPError
				require.ErrorAs(t, err, &api)
				require.Equal(t, tc.expectedStatus, api.Status)
			}
		})
	}
}

func TestSlackResourcePickerSeparatesMetadataAndHistoryGrants(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test"}
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		headers := make(http.Header)
		headers.Set("X-OAuth-Scopes", "channels:read,groups:read,channels:history")
		body := `{"ok":true,"channels":[{"id":"C1","name":"public","is_member":true,"is_private":false},{"id":"C2","name":"private","is_member":true,"is_private":true}]}`
		return &http.Response{StatusCode: 200, Header: headers, Request: r, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	page, err := c.ListResources(t.Context(), ProviderSlack, "", "")
	require.NoError(t, err)
	require.Len(t, page.Data, 2)
	require.True(t, page.Data[0].Readable)
	require.False(t, page.Data[1].Readable)
}
