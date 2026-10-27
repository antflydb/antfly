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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func discordResourceFixture(t *testing.T, failPath string, status int, flags uint64) (*CollaborationConnector, map[string]int, time.Time) {
	t.Helper()
	calls := map[string]int{}
	deadline := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
	c := &CollaborationConnector{ProviderName: ProviderDiscord, APIBase: "https://fixture.test"}
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		calls[r.URL.Path]++
		if r.URL.Path == failPath {
			return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{deadline.Format(http.TimeFormat)}}, Request: r, Body: io.NopCloser(strings.NewReader(`{"message":"provider-secret"}`))}, nil
		}
		var value any
		switch r.URL.Path {
		case "/guilds/1/channels":
			rows := []map[string]any{}
			for i := 0; i < 500; i++ {
				row := map[string]any{"id": fmt.Sprint(10 + i), "guild_id": "1", "type": 0, "name": fmt.Sprint(i)}
				if i%2 == 1 {
					row["permission_overwrites"] = []map[string]any{{"id": "1", "type": 0, "allow": "0", "deny": "1024"}}
				}
				rows = append(rows, row)
			}
			value = rows
		case "/users/@me":
			value = map[string]any{"id": "3"}
		case "/guilds/1/members/3":
			value = map[string]any{"roles": []string{}}
		case "/guilds/1/roles":
			value = []map[string]any{{"id": "1", "permissions": "66560"}}
		case "/oauth2/applications/@me":
			value = map[string]any{"flags": flags}
		default:
			t.Fatalf("resource browsing must not repeat per-channel authority requests: %s", r.URL)
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
	})}
	return c, calls, deadline
}

func TestDiscordResourcePageSharesAuthorityAndHonorsChannelOverwrites(t *testing.T) {
	c, calls, _ := discordResourceFixture(t, "", 0, 1<<19)
	page, err := c.ListResources(t.Context(), ProviderDiscord, "1", "")
	require.NoError(t, err)
	require.Len(t, page.Data, 500)
	for i, item := range page.Data {
		require.Equal(t, i%2 == 0, item.Readable)
	}
	require.Len(t, calls, 5)
	for _, count := range calls {
		require.Equal(t, 1, count, "guild identity and roles must be read once per page")
	}
	c, _, _ = discordResourceFixture(t, "", 0, 0)
	page, err = c.ListResources(t.Context(), ProviderDiscord, "1", "")
	require.NoError(t, err)
	for _, item := range page.Data {
		require.False(t, item.Readable, "confirmed MESSAGE_CONTENT denial remains a permission result")
	}
}

func TestDiscordResourceTransientErrorsPreserveCooldown(t *testing.T) {
	for _, path := range []string{"/guilds/1/channels", "/users/@me", "/guilds/1/members/3", "/guilds/1/roles", "/oauth2/applications/@me"} {
		for _, status := range []int{429, 503} {
			t.Run(fmt.Sprintf("%s/%d", path, status), func(t *testing.T) {
				c, _, deadline := discordResourceFixture(t, path, status, 1<<19)
				page, err := c.ListResources(t.Context(), ProviderDiscord, "1", "")
				var api *ProviderHTTPError
				require.ErrorAs(t, err, &api)
				require.Empty(t, page.Data, "do not return a successful list of unreadable resources on provider failure")
				require.Equal(t, status, api.Status)
				require.Equal(t, deadline, api.AntflyRetryAfter())
				require.True(t, api.Retryable())
				require.NotContains(t, err.Error(), "provider-secret")
			})
		}
	}
}
