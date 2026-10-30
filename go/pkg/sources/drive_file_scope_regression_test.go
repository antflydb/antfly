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
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestDriveFileScopeCannotAuthorizeOtherFiles(t *testing.T) {
	for _, kind := range []string{"drive lease", "calendar attachment lease", "cached Drive content"} {
		t.Run(kind, func(t *testing.T) {
			transport := fixtureClient(t, func(r *http.Request) (int, any) {
				switch r.URL.Path {
				case "/token":
					return 200, map[string]any{"access_token": "metadata-only-drive", "token_type": "Bearer", "expires_in": 3600, "scope": "https://www.googleapis.com/auth/drive.file https://www.googleapis.com/auth/drive.metadata.readonly https://www.googleapis.com/auth/calendar.readonly"}
				case "/files/folder":
					return 200, map[string]any{"id": "folder", "capabilities": map[string]any{"canListChildren": true}}
				case "/files/file":
					if r.URL.Query().Get("alt") == "media" {
						return 403, map[string]any{"error": map[string]any{"code": 403, "errors": []any{map[string]any{"reason": "insufficientPermissions"}}}}
					}
					return 200, map[string]any{"id": "file", "version": "1", "isAppAuthorized": false, "capabilities": map[string]any{"canDownload": true}}
				case "/users/me/calendarList/cal":
					return 200, map[string]any{"id": "cal", "accessRole": "writer"}
				case "/calendars/cal/events/event":
					return 200, map[string]any{"id": "event", "etag": "v1", "visibility": "public"}
				default:
					t.Fatalf("unexpected request: %s", r.URL)
					return 500, nil
				}
			})
			ctx := context.WithValue(t.Context(), oauth2.HTTPClient, transport)
			creds, err := GoogleCredentialsForServices(ctx, ProviderGoogleWorkspace, `{"type":"authorized_user","client_id":"fixture","client_secret":"fixture","refresh_token":"fixture"}`, []string{"drive", "calendar"})
			require.NoError(t, err)
			client := oauth2.NewClient(ctx, creds.TokenSource)
			c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"drive", "calendar"}, HTTPClient: client, APIBase: "https://fixture.test", DriveBase: "https://fixture.test"}
			var value any
			require.Error(t, c.drive().get(ctx, client, "/files/file?alt=media", &value), "control: token cannot download contents")
			request := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"resource":"drive","folder_id":"folder"}`)}
			inventory := Inventory{Objects: []Item{{Metadata: map[string]any{"file_id": "file"}}}}
			switch kind {
			case "calendar attachment lease":
				request.Config = SourceConfig(`{"resource":"calendar","calendar_ids":["cal"],"attachments":true}`)
				inventory.Objects = []Item{{Metadata: map[string]any{"acquisition": `{"resource":"calendar","container":"cal","id":"event","attachment":"file"}`, "details_visible": true, "drive_revision": "1"}}}
			case "cached Drive content":
				drive := &GoogleConnector{ProviderName: ProviderGoogleDrive, HTTPClient: client, APIBase: "https://fixture.test"}
				require.Error(t, drive.ValidateStagedSource(ctx, request, inventory.Objects[0]), "metadata scope cannot authorize cached content")
				return
			}
			require.Error(t, c.AuthorizeInventory(ctx, request, inventory), "metadata scope cannot renew retained Drive content")
		})
	}
}
