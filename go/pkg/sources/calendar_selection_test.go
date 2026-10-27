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
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCalendarAuditResolvesEachSelectionOnce(t *testing.T) {
	const count = 20
	metadataReads, eventReads := 0, 0
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar"}, APIBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		if strings.HasPrefix(r.URL.Path, "/users/me/calendarList/") {
			metadataReads++
			return 200, map[string]any{"id": strings.TrimPrefix(r.URL.Path, "/users/me/calendarList/"), "accessRole": "reader"}
		}
		if strings.HasSuffix(r.URL.Path, "/events/event") {
			eventReads++
			return 200, map[string]any{"id": "event", "etag": "v1", "visibility": "public"}
		}
		t.Fatalf("unexpected request: %s", r.URL)
		return 500, nil
	})
	calendars := []string{}
	inventory := Inventory{}
	for i := 0; i < count; i++ {
		id := fmt.Sprintf("cal%02d", i)
		calendars = append(calendars, id)
		raw, err := json.Marshal(collaborationRef{Resource: "calendar", Container: id, ID: "event"})
		require.NoError(t, err)
		inventory.Objects = append(inventory.Objects, Item{Metadata: map[string]any{"acquisition": string(raw), "etag": "v1", "details_visible": true}})
	}
	config, err := json.Marshal(map[string]any{"resource": "calendar", "calendar_ids": calendars, "permission_ttl_seconds": 60})
	require.NoError(t, err)
	require.NoError(t, c.AuthorizeInventory(t.Context(), Request{Mode: ImportModeSource, Config: config}, inventory))
	require.Equal(t, count, eventReads)
	require.Equal(t, count, metadataReads, "each selected calendar must be resolved only once per audit")
	require.NoError(t, c.AuthorizeInventory(t.Context(), Request{Mode: ImportModeSource, Config: config}, inventory))
	require.Equal(t, 2*count, metadataReads, "the next audit must fetch fresh role proofs")
}

func TestCalendarSelectionAliasAndRevocationAreFresh(t *testing.T) {
	reads := map[string]int{}
	role := "writer"
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar"}, APIBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		reads[r.URL.Path]++
		switch r.URL.Path {
		case "/users/me/calendarList/primary":
			return 200, map[string]any{"id": "alice@example.com", "accessRole": role}
		case "/users/me/calendarList/other":
			return 200, map[string]any{"id": "other", "accessRole": "reader"}
		case "/calendars/alice@example.com/events/event":
			return 200, map[string]any{"id": "event", "visibility": "private"}
		case "/calendars/other/events/event":
			return 200, map[string]any{"id": "event", "visibility": "public"}
		default:
			t.Fatalf("unexpected request: %s", r.URL)
			return 500, nil
		}
	})
	inventory := Inventory{}
	for _, id := range []string{"alice@example.com", "other"} {
		raw, err := json.Marshal(collaborationRef{Resource: "calendar", Container: id, ID: "event"})
		require.NoError(t, err)
		inventory.Objects = append(inventory.Objects, Item{Metadata: map[string]any{"acquisition": string(raw), "details_visible": true}})
	}
	r := Request{Mode: ImportModeSource, Config: SourceConfig(`{"resource":"calendar","calendar_ids":["primary","other"]}`)}
	require.NoError(t, c.AuthorizeInventory(t.Context(), r, inventory))
	require.Equal(t, 1, reads["/users/me/calendarList/primary"])
	require.Equal(t, 1, reads["/users/me/calendarList/other"])
	role = "writerWithoutPrivateAccess"
	require.ErrorContains(t, c.AuthorizeInventory(t.Context(), r, inventory), "private Calendar event detail access revoked")
	require.Equal(t, 2, reads["/users/me/calendarList/primary"])
}

func TestCalendarSelectionProofExpiresAndDoesNotCacheFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		role := "writer"
		status := 200
		c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, APIBase: "https://fixture.test"}
		c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
			calls++
			return status, map[string]any{"id": "cal", "accessRole": role}
		})
		_, err := c.calendarID(t.Context(), "primary")
		require.NoError(t, err)
		_, err = c.calendarID(t.Context(), "primary")
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		time.Sleep(31 * time.Second)
		status = 503
		_, err = c.calendarID(t.Context(), "primary")
		require.Error(t, err)
		status = 200
		role = "writerWithoutPrivateAccess"
		_, err = c.calendarID(t.Context(), "primary")
		require.NoError(t, err)
		require.Equal(t, 3, calls, "failed refresh cannot authorize an expired proof")
		require.Equal(t, "writerWithoutPrivateAccess", c.calendarRoles["cal"])
	})
}
