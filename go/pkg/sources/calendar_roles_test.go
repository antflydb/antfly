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
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalendarRoleDiscoveryAndEmptyAudit(t *testing.T) {
	for _, tc := range []struct {
		role     string
		readable bool
	}{
		{"reader", true}, {"writerWithoutPrivateAccess", true}, {"writer", true}, {"owner", true},
		{"freeBusyReader", false}, {"none", false}, {"", false}, {"unknown", false},
	} {
		t.Run(tc.role, func(t *testing.T) {
			c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar"}, APIBase: "https://fixture.test"}
			c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
				switch r.URL.Path {
				case "/users/me/calendarList":
					return 200, map[string]any{"items": []any{map[string]any{"id": "cal", "summary": "Team", "accessRole": tc.role}}}
				case "/users/me/calendarList/cal":
					return 200, map[string]any{"id": "cal", "accessRole": tc.role}
				default:
					t.Fatalf("unexpected request: %s", r.URL)
					return 500, nil
				}
			})
			page, err := c.ListResources(t.Context(), "calendar", "", "")
			require.NoError(t, err)
			require.Len(t, page.Data, 1)
			require.Equal(t, tc.readable, page.Data[0].Readable)
			r := Request{Mode: ImportModeSource, Config: SourceConfig(`{"resource":"calendar","calendar_ids":["cal"]}`)}
			err = c.AuthorizeInventory(t.Context(), r, Inventory{})
			if tc.readable {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}
