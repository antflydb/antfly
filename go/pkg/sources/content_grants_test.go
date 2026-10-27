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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestContentGrantDiscordModeratorArchivedThread(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderDiscord, APIBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/channels/2":
			return 200, map[string]any{"id": "2", "guild_id": "1", "type": 0}
		case "/channels/10", "/channels/11":
			return 200, map[string]any{"id": strings.TrimPrefix(r.URL.Path, "/channels/"), "guild_id": "1", "parent_id": "2", "type": 12}
		case "/channels/10/messages":
			return 200, []any{map[string]any{"id": "4"}}
		case "/channels/2/messages":
			return 200, []any{}
		case "/users/@me":
			return 200, map[string]any{"id": "3"}
		case "/guilds/1/members/3":
			return 200, map[string]any{"roles": []string{}}
		case "/guilds/1/roles":
			return 200, []any{map[string]any{"id": "1", "permissions": "17179935744"}}
		case "/oauth2/applications/@me":
			return 200, map[string]any{"flags": 1 << 19}
		case "/guilds/1/threads/active", "/channels/2/threads/archived/public", "/channels/2/users/@me/threads/archived/private":
			return 200, map[string]any{"threads": []any{}, "has_more": false}
		case "/channels/2/threads/archived/private":
			if r.URL.Query().Get("before") == "" {
				return 200, map[string]any{"threads": []any{map[string]any{"id": "10", "parent_id": "2", "guild_id": "1", "type": 12, "thread_metadata": map[string]any{"archive_timestamp": "2026-10-27T00:00:00Z"}}}, "has_more": true}
			}
			require.Equal(t, "2026-10-27T00:00:00Z", r.URL.Query().Get("before"))
			return 200, map[string]any{"threads": []any{map[string]any{"id": "11", "parent_id": "2", "guild_id": "1", "type": 12}}, "has_more": false}
		default:
			t.Fatalf("unexpected request: %s", r.URL)
			return 500, nil
		}
	})
	require.NoError(t, c.discordScope(t.Context(), CollaborationConfig{GuildID: "1", ChannelIDs: []string{"2"}}, "10"), "control: moderator can read the unjoined thread")
	ids, err := c.discordThreads(t.Context(), CollaborationConfig{GuildID: "1", ChannelIDs: []string{"2"}}, discordChannel{ID: "2", GuildID: "1", Type: 0})
	require.NoError(t, err)
	require.Equal(t, []string{"10", "11"}, ids)
	sampled, err := c.discordRefs(t.Context(), CollaborationConfig{GuildID: "1", ChannelIDs: []string{"2"}, preview: true})
	require.NoError(t, err)
	require.Equal(t, []collaborationRef{{Resource: ProviderDiscord, Container: "10", ID: "4"}}, sampled)
	request := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"guild_id":"1","channel_ids":["2"]}`)}
	require.ErrorIs(t, c.ValidateAbsentSources(t.Context(), request, []string{"discord://job/test/discord/10/4"}), ErrInventoryIncomplete, "an accessible archived message must not be retired as absent")
}

func TestContentGrantGmailAttachmentPreviewBudget(t *testing.T) {
	downloads := 0
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, APIBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/messages/m":
			parts := []any{}
			for i := 0; i < 20; i++ {
				size := 1
				if i >= PreviewItemLimit-1 {
					size = maxImportSourceObjectBytes + 1
				}
				parts = append(parts, map[string]any{"partId": fmt.Sprint(i), "filename": fmt.Sprintf("file%d.txt", i), "mimeType": "text/plain", "body": map[string]any{"attachmentId": fmt.Sprint(i), "size": size}})
			}
			return 200, map[string]any{"id": "m", "payload": map[string]any{"mimeType": "multipart/mixed", "parts": parts}}
		default:
			downloads++
			return 200, map[string]any{"data": base64.RawURLEncoding.EncodeToString([]byte("x")), "size": 1}
		}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "preview/test", Preview: true, Config: SourceConfig(`{"resource":"gmail","attachments":true}`)}
	config := CollaborationConfig{Resource: "gmail", Attachments: true, preview: true}
	inputs, err := c.hydrate(t.Context(), r, config, collaborationRef{Resource: "gmail", Container: "alice@example.com", ID: "m"}, nil)
	require.NoError(t, err)
	require.Len(t, inputs, PreviewItemLimit)
	require.LessOrEqual(t, downloads, PreviewItemLimit-1, "unsampled attachments must not trigger downloads")
}

func TestContentGrantGmailMetadataGrantCannotRenewBodyLease(t *testing.T) {
	refreshes := 0
	client := fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/token":
			refreshes++
			return 200, map[string]any{"access_token": "metadata-only", "token_type": "Bearer", "expires_in": 3600, "scope": "https://www.googleapis.com/auth/gmail.metadata"}
		case "/profile":
			return 200, map[string]any{"emailAddress": "alice@example.com"}
		case "/labels/INBOX":
			return 200, map[string]any{"id": "INBOX"}
		default:
			return 403, map[string]any{"error": map[string]any{"code": 403, "errors": []any{map[string]any{"reason": "insufficientPermissions"}}}}
		}
	})
	ctx := context.WithValue(t.Context(), oauth2.HTTPClient, client)
	credentials, err := GoogleCredentialsForServices(ctx, ProviderGoogleWorkspace, `{"type":"authorized_user","client_id":"fixture","client_secret":"fixture","refresh_token":"fixture"}`, []string{"gmail"})
	require.NoError(t, err)
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, APIBase: "https://fixture.test", HTTPClient: oauth2.NewClient(ctx, credentials.TokenSource)}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"resource":"gmail","label_ids":["INBOX"]}`)}
	item := Item{Metadata: map[string]any{"acquisition": `{"resource":"gmail","container":"alice@example.com","id":"m"}`}}
	var message any
	require.Error(t, c.get(ctx, "gmail", "/messages/m?format=full", &message), "control: body reads really are forbidden")
	err = c.AuthorizeInventory(ctx, r, Inventory{Objects: []Item{item}})
	require.Positive(t, refreshes)
	require.Error(t, err, "metadata-only OAuth grant cannot authorize retained email bodies or attachments")
}

func TestGmailContentProofIsLiveBoundedAndRetryable(t *testing.T) {
	for _, status := range []int{200, 400, 403, 429, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			calls := 0
			c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, APIBase: "https://fixture.test", Cache: &FileAcquisitionCache{Directory: t.TempDir()}}
			c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
				if r.URL.Path == "/profile" {
					return 200, map[string]any{"emailAddress": "alice@example.com"}
				}
				require.Equal(t, "/messages", r.URL.Path)
				require.Equal(t, "in:anywhere", r.URL.Query().Get("q"))
				require.Equal(t, "messages/id", r.URL.Query().Get("fields"))
				require.Equal(t, "1", r.URL.Query().Get("maxResults"))
				calls++
				return status, map[string]any{}
			})
			request := Request{Mode: ImportModeSource, Config: SourceConfig(`{"resource":"gmail","permission_ttl_seconds":60}`)}
			// A selected scope proof is constant cost for arbitrarily many retained rows.
			inventory := Inventory{}
			for i := 0; i < 500; i++ {
				inventory.Objects = append(inventory.Objects, Item{Metadata: map[string]any{"acquisition": fmt.Sprintf(`{"resource":"gmail","container":"alice@example.com","id":"m%d"}`, i)}})
			}
			for audit := 0; audit < 2; audit++ {
				err := c.AuthorizeInventory(t.Context(), request, inventory)
				if status == 200 {
					require.NoError(t, err)
				} else {
					var api *ProviderHTTPError
					require.ErrorAs(t, err, &api)
					require.Equal(t, status, api.Status)
					require.Equal(t, status == 429 || status == 503, api.Retryable())
				}
			}
			require.Equal(t, 2, calls, "successful proof may be shared in an audit but must be fetched again in the next audit")
		})
	}
}

func TestGmailAttachmentsCheckBudgetBeforeAcquisition(t *testing.T) {
	for _, preview := range []bool{false, true} {
		t.Run(fmt.Sprint(preview), func(t *testing.T) {
			reads := 0
			c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, APIBase: "https://fixture.test", HTTPClient: fixtureClient(t, func(r *http.Request) (int, any) { reads++; return 200, map[string]any{} })}
			config := CollaborationConfig{Resource: "gmail", Attachments: true, preview: preview}
			ref := collaborationRef{Resource: "gmail", Container: "alice@example.com", ID: "m"}
			material := collaborationMaterial{Text: []byte("body"), Files: []collaborationFile{{ID: "f", MIME: "text/plain", gmailMessageID: "m", gmailAttachmentID: "a", size: maxImportSourceObjectBytes}}}
			_, err := c.materialItems(t.Context(), Request{SourceNamespace: "job/test"}, config, ref, material)
			require.Error(t, err)
			require.Zero(t, reads, "known oversize must fail before attachment HTTP reads")
		})
	}
}

func TestGmailFullSyncAndInvitationMetadataRemainComplete(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, APIBase: "https://fixture.test"}
	reads := 0
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		if r.URL.Path == "/messages/m" {
			parts := []any{}
			for i := 0; i < 20; i++ {
				parts = append(parts, map[string]any{"partId": fmt.Sprint(i), "filename": "invite.ics", "mimeType": "text/calendar", "body": map[string]any{"attachmentId": fmt.Sprint(i), "size": 10}})
			}
			return 200, map[string]any{"id": "m", "payload": map[string]any{"mimeType": "multipart/mixed", "parts": parts}}
		}
		reads++
		return 200, map[string]any{"data": base64.RawURLEncoding.EncodeToString([]byte("UID:event1")), "size": 10}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"resource":"gmail","attachments":true}`)}
	inputs, err := c.hydrate(t.Context(), r, CollaborationConfig{Resource: "gmail", Attachments: true}, collaborationRef{Resource: "gmail", Container: "alice@example.com", ID: "m"}, nil)
	require.NoError(t, err)
	require.Len(t, inputs, 21)
	require.Equal(t, 20, reads)
	require.Equal(t, []string{"event1"}, inputs[0].Metadata["calendar_uids"])
}

func TestDiscordAcquisitionSharesAuthorityAndChecksNextAuditFresh(t *testing.T) {
	reads := map[string]int{}
	denied := false
	c := &CollaborationConnector{ProviderName: ProviderDiscord, APIBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		reads[r.URL.Path]++
		switch r.URL.Path {
		case "/users/@me":
			return 200, map[string]any{"id": "3"}
		case "/guilds/1/members/3":
			return 200, map[string]any{"roles": []string{}}
		case "/guilds/1/roles":
			permissions := "17179935744"
			if denied {
				permissions = "0"
			}
			return 200, []any{map[string]any{"id": "1", "permissions": permissions}}
		case "/oauth2/applications/@me":
			return 200, map[string]any{"flags": 1 << 19}
		case "/guilds/1/threads/active":
			return 200, map[string]any{"threads": []any{}}
		}
		if strings.HasSuffix(r.URL.Path, "/messages") {
			return 200, []any{}
		}
		if strings.Contains(r.URL.Path, "/threads/archived/") {
			return 200, map[string]any{"threads": []any{}, "has_more": false}
		}
		if strings.HasPrefix(r.URL.Path, "/channels/") {
			return 200, map[string]any{"id": strings.TrimPrefix(r.URL.Path, "/channels/"), "guild_id": "1", "type": 0}
		}
		t.Fatalf("unexpected request: %s", r.URL)
		return 500, nil
	})
	config := CollaborationConfig{GuildID: "1"}
	for i := 10; i < 30; i++ {
		config.ChannelIDs = append(config.ChannelIDs, fmt.Sprint(i))
	}
	_, err := c.discordRefs(t.Context(), config)
	require.NoError(t, err)
	require.Equal(t, 1, reads["/guilds/1/threads/active"])
	for _, path := range []string{"/users/@me", "/guilds/1/members/3", "/guilds/1/roles", "/oauth2/applications/@me"} {
		require.Equal(t, 1, reads[path], path)
	}
	for _, id := range config.ChannelIDs {
		require.Equal(t, 1, reads["/channels/"+id])
	}
	raw, err := json.Marshal(config)
	require.NoError(t, err)
	request := Request{Mode: ImportModeSource, Config: raw}
	require.NoError(t, c.AuthorizeInventory(t.Context(), request, Inventory{}))
	denied = true
	require.ErrorIs(t, c.AuthorizeInventory(t.Context(), request, Inventory{}), errDiscordAccessDenied)
	require.Equal(t, 3, reads["/guilds/1/roles"], "each audit must fetch fresh authority")
}

func TestDiscordPassExpiresWithinLongOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := discordSamplingFixture(t, 0, func(*http.Request) (int, any) { t.Fatal("unexpected request"); return 500, nil })
		defer c.beginDiscordReadPass()()
		first, err := c.discordAuthority(t.Context(), "1")
		require.NoError(t, err)
		same, err := c.discordAuthority(t.Context(), "1")
		require.NoError(t, err)
		require.Same(t, first, same)
		time.Sleep(31 * time.Second)
		fresh, err := c.discordAuthority(t.Context(), "1")
		require.NoError(t, err)
		require.NotSame(t, first, fresh)
	})
}
