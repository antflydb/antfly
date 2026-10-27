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
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func discordSamplingFixture(t *testing.T, rootType int, handle func(*http.Request) (int, any)) *CollaborationConnector {
	t.Helper()
	c := &CollaborationConnector{ProviderName: ProviderDiscord, APIBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/channels/2":
			return 200, map[string]any{"id": "2", "guild_id": "1", "type": rootType}
		case "/users/@me":
			return 200, map[string]any{"id": "3"}
		case "/guilds/1/members/3":
			return 200, map[string]any{"roles": []string{}}
		case "/guilds/1/roles":
			return 200, []any{map[string]any{"id": "1", "permissions": "66560"}}
		case "/oauth2/applications/@me":
			return 200, map[string]any{"flags": uint64(1 << 19)}
		}
		if strings.HasPrefix(r.URL.Path, "/channels/1") && !strings.Contains(strings.TrimPrefix(r.URL.Path, "/channels/"), "/") {
			id := strings.TrimPrefix(r.URL.Path, "/channels/")
			return 200, map[string]any{"id": id, "guild_id": "1", "parent_id": "2", "type": 11}
		}
		return handle(r)
	})
	return c
}

func TestDiscordPreviewReadsMessageBeforeAnyThreadListing(t *testing.T) {
	reads := 0
	c := discordSamplingFixture(t, 0, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/channels/2/messages":
			require.Equal(t, "1", r.URL.Query().Get("limit"))
			require.Empty(t, r.URL.Query().Get("before"))
			return 200, []any{map[string]any{"id": "4"}}
		case "/channels/2/messages/4":
			reads++
			return 200, map[string]any{"id": "4", "channel_id": "2", "content": "sample", "author": map[string]any{"id": "3"}}
		default:
			t.Fatalf("preview must not enumerate threads before reading a present message: %s", r.URL)
			return 500, nil
		}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "preview/test", Preview: true, Config: SourceConfig(`{"guild_id":"1","channel_ids":["2"]}`)}
	inventory, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, inventory.Objects, 1)
	data, err := c.Read(t.Context(), r, inventory.Objects[0])
	require.NoError(t, err)
	require.Equal(t, "sample", string(data))
	require.Equal(t, 1, reads, "preview read must reuse the ephemeral normalized sample")
	require.False(t, inventory.Authoritative)
}

func TestDiscordPreviewSamplesForumAndMediaActiveThreads(t *testing.T) {
	for _, kind := range []int{15, 16} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			c := discordSamplingFixture(t, kind, func(r *http.Request) (int, any) {
				switch r.URL.Path {
				case "/guilds/1/threads/active":
					return 200, map[string]any{"threads": []any{map[string]any{"id": "100", "parent_id": "2"}}}
				case "/channels/100/messages":
					return 200, []any{map[string]any{"id": "4"}}
				default:
					t.Fatalf("unexpected archive/container request: %s", r.URL)
					return 500, nil
				}
			})
			refs, err := c.discordRefs(t.Context(), CollaborationConfig{preview: true, GuildID: "1", ChannelIDs: []string{"2"}})
			require.NoError(t, err)
			require.Equal(t, []collaborationRef{{Resource: ProviderDiscord, Container: "100", ID: "4"}}, refs)
		})
	}
}

func TestDiscordPreviewStopsAtArchiveAndContainerBudgets(t *testing.T) {
	for _, many := range []bool{false, true} {
		t.Run(fmt.Sprint(many), func(t *testing.T) {
			archives, reads := 0, 0
			c := discordSamplingFixture(t, 0, func(r *http.Request) (int, any) {
				switch {
				case r.URL.Path == "/guilds/1/threads/active":
					return 200, map[string]any{"threads": []any{}}
				case strings.HasSuffix(r.URL.Path, "/messages"):
					reads++
					return 200, []any{}
				case r.URL.Path == "/channels/2/threads/archived/public":
					archives++
					require.Empty(t, r.URL.Query().Get("before"), "preview must never follow an archive cursor")
					require.Equal(t, "8", r.URL.Query().Get("limit"))
					threads := []any{}
					n := 1
					if many {
						n = 100
					}
					for i := 0; i < n; i++ {
						threads = append(threads, map[string]any{"id": fmt.Sprint(100 + i), "parent_id": "2", "thread_metadata": map[string]any{"archive_timestamp": "2026-10-26T00:00:00Z"}})
					}
					return 200, map[string]any{"threads": threads, "has_more": true}
				case r.URL.Path == "/channels/2/users/@me/threads/archived/private":
					return 200, map[string]any{"threads": []any{}, "has_more": false}
				default:
					t.Fatalf("unexpected request: %s", r.URL)
					return 500, nil
				}
			})
			_, err := c.discordRefs(t.Context(), CollaborationConfig{preview: true, GuildID: "1", ChannelIDs: []string{"2"}})
			require.ErrorIs(t, err, ErrInventoryIncomplete, "truncated empty samples must not claim the source is empty")
			require.Equal(t, 1, archives)
			require.LessOrEqual(t, reads, discordPreviewContainerLimit)
		})
	}
}

func TestDiscordFullThreadEnumerationStillPaginates(t *testing.T) {
	for _, kind := range []int{0, 5, 15, 16} {
		t.Run(fmt.Sprint(kind), func(t *testing.T) {
			pages := 0
			c := discordSamplingFixture(t, kind, func(r *http.Request) (int, any) {
				switch r.URL.Path {
				case "/guilds/1/threads/active":
					return 200, map[string]any{"threads": []any{}}
				case "/channels/2/threads/archived/public":
					pages++
					if pages == 2 {
						require.NotEmpty(t, r.URL.Query().Get("before"))
					}
					return 200, map[string]any{"threads": []any{map[string]any{"id": fmt.Sprint(100 + pages), "parent_id": "2", "thread_metadata": map[string]any{"archive_timestamp": fmt.Sprintf("2026-10-%02dT00:00:00Z", 27-pages)}}}, "has_more": pages < 2}
				case "/channels/2/users/@me/threads/archived/private":
					require.Equal(t, 0, kind, "non-text channels cannot have private threads")
					return 200, map[string]any{"threads": []any{}, "has_more": false}
				default:
					t.Fatalf("unexpected request: %s", r.URL)
					return 500, nil
				}
			})
			ids, err := c.discordThreads(t.Context(), CollaborationConfig{GuildID: "1"}, discordChannel{ID: "2", Type: kind})
			require.NoError(t, err)
			require.Equal(t, []string{"101", "102"}, ids)
			require.Equal(t, 2, pages)
		})
	}
}
