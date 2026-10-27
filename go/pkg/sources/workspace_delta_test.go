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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCalendarDeltaRefreshesIndependentlyEditedDriveAttachments(t *testing.T) {
	version, content := "1", "first"
	deleted, denied := false, false
	fullLists, deltaLists := 0, 0
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar", "drive"}, CalendarBase: "https://test/calendar", DriveBase: "https://test/drive"}
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		var value any
		status := http.StatusOK
		switch r.URL.Path {
		case "/calendar/users/me/calendarList/team":
			value = map[string]any{"id": "team", "accessRole": "reader"}
		case "/calendar/calendars/team/events":
			items := []any{}
			if r.URL.Query().Get("syncToken") == "" {
				fullLists++
				items = append(items, map[string]any{"id": "event"})
			} else {
				deltaLists++
				if deleted {
					items = append(items, map[string]any{"id": "event", "status": "cancelled"})
				}
			}
			value = map[string]any{"items": items, "nextSyncToken": "cursor"}
		case "/calendar/calendars/team/events/event":
			require.False(t, deleted, "deleted parents must not be refreshed")
			value = map[string]any{"id": "event", "etag": "unchanged-event", "summary": "Meeting", "attachments": []any{map[string]any{"fileId": "file", "title": "notes.txt", "mimeType": "text/plain"}}}
		case "/drive/files/file":
			if denied {
				status = http.StatusForbidden
				break
			}
			if r.URL.Query().Get("alt") == "media" {
				return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(content)), Request: r}, nil
			}
			value = map[string]any{"id": "file", "version": version, "size": "5", "mimeType": "text/plain", "name": "notes.txt"}
		default:
			t.Fatalf("unexpected path: %s", r.URL)
		}
		data, _ := json.Marshal(value)
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(data))), Request: r}, nil
	})}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/" + testSourceID, Config: SourceConfig(`{"resource":"calendar","calendar_ids":["team"],"attachments":true}`)}
	first, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, first.Objects, 2)
	c.Baseline = &first
	version, content = "2", "other"
	next, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, next.Objects, 2)
	require.NotEqual(t, first.SourceRevision, next.SourceRevision)
	for _, item := range next.Objects {
		text, err := c.Read(t.Context(), r, item)
		require.NoError(t, err)
		if configString(item.Metadata, "attachment_id") != "" {
			require.Equal(t, "other", string(text))
		}
	}
	c.Baseline = &next
	unchanged, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Equal(t, next.SourceRevision, unchanged.SourceRevision)
	require.Equal(t, 1, fullLists)
	require.Equal(t, 2, deltaLists)
	denied = true
	_, err = c.Enumerate(t.Context(), r)
	require.Error(t, err, "revoked Drive access must fail closed")
	deleted = true
	removed, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Empty(t, removed.Objects)
}

func TestGmailDeltaMergesNewMessagesAndValidatedTombstones(t *testing.T) {
	delta := false
	expired := false
	lists := 0
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, GmailBase: "https://test/gmail"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/gmail/profile":
			return 200, map[string]any{"emailAddress": "alice@example.com", "historyId": "10"}
		case "/gmail/labels/INBOX":
			return 200, map[string]any{"id": "INBOX"}
		case "/gmail/messages":
			lists++
			return 200, map[string]any{"messages": []any{map[string]any{"id": "old1"}, map[string]any{"id": "old2"}}}
		case "/gmail/history":
			if expired {
				return 404, nil
			}
			require.Equal(t, "10", r.URL.Query().Get("startHistoryId"))
			change := func(id string) map[string]any { return map[string]any{"message": map[string]any{"id": id}} }
			return 200, map[string]any{"historyId": "20", "history": []any{map[string]any{"messagesDeleted": []any{change("old1")}, "labelsRemoved": []any{change("old2")}, "messagesAdded": []any{change("new3")}}}}
		default:
			id := r.URL.Path[len("/gmail/messages/"):]
			labels := []string{"INBOX"}
			if delta && id == "old2" {
				labels = nil
			}
			return 200, map[string]any{"id": id, "labelIds": labels, "payload": map[string]any{"mimeType": "text/plain", "body": map[string]any{"data": base64.RawURLEncoding.EncodeToString([]byte(id + " retry"))}}}
		}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"resource":"gmail","label_ids":["INBOX"]}`)}
	first, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, first.Objects, 2)
	require.Equal(t, "10", first.ProviderState["gmail"])
	c.Baseline = &first
	delta = true
	next, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, next.Objects, 1)
	require.Equal(t, "new3", next.Objects[0].Metadata["remote_id"])
	require.Equal(t, "20", next.ProviderState["gmail"])
	require.Equal(t, 1, lists)
	expired = true
	delta = false
	_, err = c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Equal(t, 2, lists)
}

func TestLargeWorkspaceInventoryUsesChangeCursorBeyondTenThousandItems(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, GmailBase: "https://test/gmail", HTTPClient: fixtureClient(t, func(r *http.Request) (int, any) {
		if r.URL.Path == "/gmail/profile" {
			return 200, map[string]any{"emailAddress": "alice@example.com"}
		}
		require.Equal(t, "/gmail/history", r.URL.Path)
		return 200, map[string]any{"historyId": "20"}
	})}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/" + testSourceID, Config: SourceConfig(`{"resource":"gmail"}`)}
	baseline := Inventory{Authoritative: true, ProviderState: map[string]string{"config": workspaceSelectionKey(r), "account": "alice@example.com", "gmail": "10"}}
	ids := []string{}
	for i := 0; i < 10001; i++ {
		id := fmt.Sprintf("workspace://job/%s/gmail/alice@example.com/mail%d", testSourceID, i)
		ids = append(ids, id)
		baseline.Objects = append(baseline.Objects, Item{ID: id, Path: fmt.Sprintf("gmail/alice/mail%d", i), Revision: "pinned", Size: 1, SourceKind: ProviderGoogleWorkspace, Metadata: map[string]any{"resource": "gmail", "container_id": "alice@example.com", "remote_id": fmt.Sprintf("mail%d", i)}})
	}
	c.Baseline = &baseline
	next, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, next.Objects, 10001)
	require.True(t, next.Authoritative)
	_, err = NewSyncWriter(&Publisher{Endpoint: "https://example.test"}, "sources", testSourceID, ids, true)
	require.NoError(t, err)
}

func TestAcquisitionResumePinsPagesWithoutCachingBodiesOrDownloadURLs(t *testing.T) {
	firstCalls, secondCalls := 0, 0
	limited := true
	cache := &FileAcquisitionCache{Directory: t.TempDir()}
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar"}, CalendarBase: "https://test/calendar", Cache: cache}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/calendar/users/me/calendarList/team":
			return 200, map[string]any{"id": "team", "accessRole": "reader"}
		case "/calendar/calendars/team/events":
			if r.URL.Query().Get("pageToken") == "" {
				firstCalls++
				return 200, map[string]any{"items": []any{map[string]any{"id": "event1", "status": "confirmed", "description": "private body"}}, "nextPageToken": "page2"}
			}
			secondCalls++
			if limited {
				return 429, nil
			}
			return 200, map[string]any{"items": []any{}, "nextSyncToken": "cursor1"}
		case "/calendar/calendars/team/events/event1":
			return 200, map[string]any{"id": "event1", "etag": "v1", "htmlLink": "https://calendar.google.com/calendar/event?eid=event1", "summary": "retry"}
		default:
			t.Fatalf("unexpected URL %s", r.URL)
			return 404, nil
		}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"resource":"calendar","calendar_ids":["team"]}`)}
	_, err := c.Enumerate(t.Context(), r)
	require.Error(t, err)
	limited = false
	next, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, next.Objects, 1)
	require.Equal(t, 1, firstCalls)
	require.Equal(t, 2, secondCalls)
	q := url.Values{"maxResults": {"2500"}, "singleEvents": {"false"}, "showDeleted": {"true"}}
	raw, found, err := cache.Load(t.Context(), "https://test/calendar/calendars/team/events?"+q.Encode())
	require.NoError(t, err)
	require.True(t, found)
	require.NotContains(t, string(raw), "private body")
	sanitized := acquisitionMetadata(ProviderSlack, "/conversations.history?channel=C1", []byte(`{"ok":true,"messages":[{"ts":"100.000001","text":"private body","files":[{"url_private":"https://files.slack.com/a?secret=token"}]}]}`))
	require.NotContains(t, string(sanitized), "private body")
	require.NotContains(t, string(sanitized), "secret=")
}
