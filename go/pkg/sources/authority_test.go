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
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAbsenceReconciliationResumesIdentityPagesWithoutHydration(t *testing.T) {
	pages, content := t.TempDir(), t.TempDir()
	limited := false
	calls := []string{}
	windowCalls := 0
	client := fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/auth.test":
			return 200, map[string]any{"ok": true, "team_id": "T1"}
		case "/conversations.info":
			return 200, map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": true}}
		case "/conversations.history":
			cursor := r.URL.Query().Get("cursor")
			calls = append(calls, cursor)
			if limited {
				windowCalls++
				if windowCalls > 1 {
					return 429, nil
				}
			}
			ts := "100.000001"
			meta := map[string]any{"next_cursor": "next"}
			if cursor != "" {
				ts = "100.000002"
				meta = nil
			}
			return 200, map[string]any{"ok": true, "messages": []any{map[string]any{"ts": ts, "text": "private current body"}}, "response_metadata": meta}
		case "/chat.getPermalink":
			require.False(t, limited, "reconciliation must not materialize message content")
			return 200, map[string]any{"ok": true}
		default:
			t.Fatalf("unexpected request %s", r.URL)
			return 500, nil
		}
	})
	connector := func() *CollaborationConnector {
		return &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test", HTTPClient: client, Cache: &FileAcquisitionCache{Directory: pages}, ContentStore: &FileAcquisitionContentStore{Directory: content}}
	}
	r := Request{SourceNamespace: "job/test", Mode: ImportModeSource, Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1"],"include_threads":false}`)}
	_, err := connector().Enumerate(t.Context(), r)
	require.NoError(t, err)
	ids := []string{"slack://job/test/slack/C1/99.000001"}
	limited = true
	calls = nil
	require.ErrorAs(t, connector().ValidateAbsentSources(t.Context(), r, ids), new(*ProviderHTTPError))
	require.Equal(t, []string{"", "next"}, calls)
	windowCalls = 0
	calls = nil
	require.NoError(t, connector().ValidateAbsentSources(t.Context(), r, ids))
	require.Equal(t, []string{"next"}, calls)
	entries, err := os.ReadDir(pages)
	require.NoError(t, err)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(pages, entry.Name()))
		require.NoError(t, err)
		require.NotContains(t, string(data), "private current body")
	}
}

func TestAbsenceReconciliationCheckpointsOnlyChangedParentAttachmentIDs(t *testing.T) {
	pages := t.TempDir()
	used := 0
	reads := map[string]int{}
	client := fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/auth.test":
			return 200, map[string]any{"ok": true, "team_id": "T1"}
		case "/conversations.info":
			return 200, map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": true}}
		case "/conversations.history":
			used++
			if used > 1 {
				return 429, nil
			}
			id := r.URL.Query().Get("oldest")
			if id == "" {
				return 200, map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "100.000001"}, map[string]any{"ts": "100.000002"}}}
			}
			reads[id]++
			return 200, map[string]any{"ok": true, "messages": []any{map[string]any{"ts": id, "text": "private body", "files": []any{map[string]any{"id": "keep", "url_private": "https://files.slack.com/private?secret=value"}}}}}
		default:
			t.Fatalf("reconciliation must not download or extract content: %s", r.URL)
			return 500, nil
		}
	})
	r := Request{SourceNamespace: "job/test", Mode: ImportModeSource, Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1"],"include_threads":false,"attachments":true}`)}
	ids := []string{"slack://job/test/slack/C1/100.000001/attachments/removed", "slack://job/test/slack/C1/100.000002/attachments/removed"}
	for attempt := 0; attempt < 3; attempt++ {
		used = 0
		c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test", HTTPClient: client, Cache: &FileAcquisitionCache{Directory: pages}}
		err := c.ValidateAbsentSources(t.Context(), r, ids)
		if attempt < 2 {
			require.ErrorAs(t, err, new(*ProviderHTTPError))
		} else {
			require.NoError(t, err)
		}
	}
	require.Equal(t, map[string]int{"100.000001": 1, "100.000002": 1}, reads)
	entries, err := os.ReadDir(pages)
	require.NoError(t, err)
	for _, entry := range entries {
		data, err := os.ReadFile(filepath.Join(pages, entry.Name()))
		require.NoError(t, err)
		require.NotContains(t, string(data), "private body")
		require.NotContains(t, string(data), "secret=value")
	}
}

func TestCalendarRetainedContentAndPermissionOnlyAudit(t *testing.T) {
	store := &FileAcquisitionContentStore{Directory: t.TempDir()}
	denied := false
	version, etag, role := "1", "event1", "writer"
	visibility := "private"
	downloads := 0
	client := &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		var value any
		status := 200
		switch r.URL.Path {
		case "/calendar/users/me/calendarList/primary":
			value = map[string]any{"id": "alice@example.com", "accessRole": role}
		case "/calendar/calendars/alice@example.com/events":
			value = map[string]any{"items": []any{map[string]any{"id": "event"}}, "nextSyncToken": "cursor"}
		case "/calendar/calendars/alice@example.com/events/event":
			value = map[string]any{"id": "event", "etag": etag, "visibility": visibility, "summary": "Private event", "attachments": []any{map[string]any{"fileId": "file"}}}
		case "/drive/files/file":
			if r.URL.Query().Get("alt") == "media" {
				downloads++
				if denied {
					status = 403
				}
				return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("secret")), Request: r}, nil
			}
			value = map[string]any{"id": "file", "version": version, "mimeType": "text/plain", "name": "notes.txt", "size": "6", "capabilities": map[string]any{"canDownload": !denied}}
		default:
			t.Fatalf("unexpected request %s", r.URL)
		}
		b, _ := json.Marshal(value)
		return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(string(b))), Request: r}, nil
	})}
	client = withDriveScopes(client, "https://www.googleapis.com/auth/drive.readonly")
	connector := func() *CollaborationConnector {
		return &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar", "drive"}, CalendarBase: "https://fixture.test/calendar", DriveBase: "https://fixture.test/drive", HTTPClient: client, ContentStore: store}
	}
	c := connector()
	r := Request{SourceNamespace: "job/test", Mode: ImportModeSource, Config: SourceConfig(`{"resource":"calendar","calendar_ids":["primary"],"attachments":true}`)}
	inventory, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, inventory.Objects, 2)
	var attachment Item
	for _, item := range inventory.Objects {
		if configString(item.Metadata, "attachment_id") != "" {
			attachment = item
		}
	}
	denied = true
	c = connector()
	_, err = c.Read(t.Context(), r, attachment)
	require.ErrorContains(t, err, "download access revoked")
	require.Error(t, c.CheckAccess(t.Context(), r, attachment))
	require.Error(t, c.ValidateInventory(t.Context(), r, inventory))
	require.Error(t, c.AuthorizeInventory(t.Context(), r, inventory))
	require.Equal(t, 1, downloads)
	denied = false
	version = "2"
	etag = "event2"
	c = connector()
	require.NoError(t, c.AuthorizeInventory(t.Context(), r, inventory), "remote edits do not require re-ingestion to recheck authority")
	require.ErrorAs(t, c.ValidateInventory(t.Context(), r, inventory), new(*RevisionChangedError))
	require.Equal(t, 1, downloads)
	role = "reader"
	require.ErrorContains(t, connector().AuthorizeInventory(t.Context(), r, inventory), "private Calendar event detail access revoked")
}

func TestPermissionAuditRejectsUnreadableEmptySelection(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test", HTTPClient: fixtureClient(t, func(r *http.Request) (int, any) {
		if r.URL.Path == "/auth.test" {
			return 200, map[string]any{"ok": true, "team_id": "T1"}
		}
		return 200, map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": false}}
	})}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/test", Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1"]}`)}
	require.Error(t, c.AuthorizeInventory(context.Background(), r, Inventory{}))
}

func TestPermissionAuditRetainsSuccessfulScopeProofsAcrossRateLimit(t *testing.T) {
	calls := map[string]int{}
	c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/auth.test":
			return 200, map[string]any{"ok": true, "team_id": "T1"}
		case "/conversations.info":
			id := r.URL.Query().Get("channel")
			calls[id]++
			if id == "C2" && calls[id] == 1 {
				return 429, nil
			}
			return 200, map[string]any{"ok": true, "channel": map[string]any{"id": id, "is_member": true}}
		default:
			t.Fatalf("permission audit must not read content: %s", r.URL)
			return 500, nil
		}
	})
	waits := 0
	c.accessWait = func(ctx context.Context, d time.Duration) error {
		waits++
		require.Equal(t, time.Minute, d)
		return nil
	}
	err := c.AuthorizeInventory(t.Context(), Request{Mode: ImportModeSource, Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1","C2"],"include_threads":false}`)}, Inventory{})
	require.NoError(t, err)
	require.Equal(t, 1, waits)
	require.Equal(t, map[string]int{"C1": 1, "C2": 2}, calls)
	require.Zero(t, c.accessAuditWindow)
}

func TestWorkspacePermissionAuditResumesLabelChecksAfterRateLimit(t *testing.T) {
	calls := map[string]int{}
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, GmailBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		if r.URL.Path == "/messages" && r.URL.Query().Get("q") != "" {
			return 200, map[string]any{}
		}
		calls[r.URL.Path]++
		if strings.HasSuffix(r.URL.Path, "/profile") {
			return 200, map[string]any{"emailAddress": "alice@example.com"}
		}
		if strings.HasSuffix(r.URL.Path, "/labels/L2") && calls[r.URL.Path] == 1 {
			return 429, nil
		}
		require.Contains(t, r.URL.Path, "/labels/")
		return 200, map[string]any{"id": r.URL.Path}
	})
	waits := 0
	c.accessWait = func(context.Context, time.Duration) error { waits++; return nil }
	require.NoError(t, c.AuthorizeInventory(t.Context(), Request{Mode: ImportModeSource, Config: SourceConfig(`{"resource":"gmail","label_ids":["L1","L2"]}`)}, Inventory{}))
	require.Equal(t, 1, waits)
	for path, n := range calls {
		if strings.HasSuffix(path, "L2") {
			require.Equal(t, 2, n)
		} else {
			require.Equal(t, 1, n)
		}
	}
}

func TestGmailAbsenceAttachmentCheckRequestsIdentityMetadataOnly(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, GmailBase: "https://fixture.test"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		if strings.HasSuffix(r.URL.Path, "/profile") {
			return 200, map[string]any{"emailAddress": "alice@example.com"}
		}
		require.True(t, strings.HasSuffix(r.URL.Path, "/messages/m1"))
		require.Equal(t, "full", r.URL.Query().Get("format"))
		fields := r.URL.Query().Get("fields")
		require.Contains(t, fields, "payload(partId,filename,mimeType,body(attachmentId),parts(")
		require.NotContains(t, fields, "data")
		require.NotContains(t, fields, "headers")
		return 200, map[string]any{"id": "m1", "payload": map[string]any{"parts": []any{map[string]any{"partId": "1.1", "filename": "report.pdf", "mimeType": "application/pdf", "body": map[string]any{"attachmentId": "a1"}}}}}
	})
	ids, err := c.currentAttachmentIDs(t.Context(), CollaborationConfig{Resource: "gmail"}, collaborationRef{Resource: "gmail", Container: "alice@example.com", ID: "m1"})
	require.NoError(t, err)
	require.Equal(t, []string{"1.1"}, ids)
}

func TestSlackEnvelopeClassifiesTransientAndCredentialFailures(t *testing.T) {
	for _, tc := range []struct {
		code   string
		status int
		retry  bool
	}{
		{"internal_error", 503, true}, {"fatal_error", 503, true}, {"service_unavailable", 503, true}, {"unknown_future_error", 502, true}, {"ratelimited", 429, true}, {"rate_limited", 429, true},
		{"invalid_auth", 401, false}, {"token_revoked", 401, false}, {"account_inactive", 401, false}, {"missing_scope", 403, false}, {"channel_not_found", 404, false},
	} {
		t.Run(tc.code, func(t *testing.T) {
			c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test", HTTPClient: fixtureClient(t, func(r *http.Request) (int, any) { return 200, map[string]any{"ok": false, "error": tc.code} })}
			_, err := c.ConnectionIdentity(t.Context())
			var api *ProviderHTTPError
			require.ErrorAs(t, err, &api)
			require.Equal(t, tc.status, api.Status)
			require.Equal(t, tc.code, api.Code)
			require.Equal(t, tc.retry, api.Retryable())
			if tc.status == 429 {
				require.True(t, api.RetryAt.After(time.Now()))
			}
		})
	}
	at := time.Now().UTC().Add(time.Minute).Truncate(time.Second)
	require.Equal(t, at, providerRetryAt(at.Format(http.TimeFormat), time.Now()))
	require.WithinDuration(t, at, SlackAPIError("ratelimited", at).AntflyRetryAfter(), time.Second)
}

type previewForbiddenPersistence struct{ t *testing.T }

func (p previewForbiddenPersistence) Load(context.Context, string) ([]byte, bool, error) {
	p.t.Fatal("preview must not read acquisition storage")
	return nil, false, nil
}
func (p previewForbiddenPersistence) Save(context.Context, string, []byte) error {
	p.t.Fatal("preview must not persist acquisition storage")
	return nil
}

type previewDownloadTransport func(*http.Request) (*http.Response, error)

func (f previewDownloadTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestSlackPreviewReusesSampleAndAttachmentsWithoutPersistence(t *testing.T) {
	for _, persistent := range []bool{false, true} {
		t.Run(map[bool]string{false: "no-store", true: "configured-store"}[persistent], func(t *testing.T) {
			history, downloads, extractions := 0, 0, 0
			c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test", AccessToken: func(context.Context) (string, error) { return "fixture", nil }}
			if persistent {
				store := previewForbiddenPersistence{t}
				c.Cache = store
				c.ContentStore = store
			}
			c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
				switch r.URL.Path {
				case "/auth.test":
					return 200, map[string]any{"ok": true, "team_id": "T1"}
				case "/conversations.info":
					return 200, map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": true}}
				case "/conversations.history":
					history++
					require.Equal(t, 1, history, "sample hydration/read must never fetch history again")
					return 200, map[string]any{"ok": true, "messages": []any{
						map[string]any{"ts": "100.000001", "text": "sample body", "files": []any{map[string]any{"id": "F1", "name": "report.txt", "mimetype": "text/plain", "url_private": "https://files.slack.com/private/report.txt"}}},
						map[string]any{"ts": "100.000002", "text": "must not hydrate excess sample"},
					}}
				case "/chat.getPermalink":
					return 200, map[string]any{"ok": true}
				default:
					t.Fatalf("unexpected request %s", r.URL)
					return 500, nil
				}
			})
			c.DownloadTransport = previewDownloadTransport(func(r *http.Request) (*http.Response, error) {
				downloads++
				return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader("attachment body"))}, nil
			})
			c.Extractor = func(_ context.Context, _ string, _ string, b []byte) ([]byte, error) { extractions++; return b, nil }
			r := Request{Mode: ImportModeSource, SourceNamespace: "preview/test", Preview: true, Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1"],"include_threads":false,"attachments":true}`)}
			inv, err := c.Enumerate(t.Context(), r)
			require.NoError(t, err)
			require.Len(t, inv.Objects, 2)
			for _, item := range inv.Objects {
				b, err := c.Read(t.Context(), r, item)
				require.NoError(t, err)
				require.NotEmpty(t, b)
			}
			require.Equal(t, 1, history)
			require.Equal(t, 1, downloads)
			require.Equal(t, 1, extractions)
		})
	}
}
func TestAttachmentErrorsPreserveRetryAfter(t *testing.T) {
	for _, provider := range []string{ProviderSlack, ProviderDiscord} {
		for _, status := range []int{429, 503} {
			for _, dateHeader := range []bool{false, true} {
				header := "600"
				expected := time.Now().UTC().Add(10 * time.Minute).Truncate(time.Second)
				if dateHeader {
					header = expected.Format(http.TimeFormat)
				}
				c := &CollaborationConnector{ProviderName: provider, AccessToken: func(context.Context) (string, error) { return "fixture", nil }, DownloadTransport: previewDownloadTransport(func(r *http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": []string{header}}, Request: r, Body: io.NopCloser(strings.NewReader(""))}, nil
				})}
				target := "https://files.slack.com/private/report.txt"
				if provider == ProviderDiscord {
					target = "https://cdn.discordapp.com/attachments/report.txt"
				}
				_, err := c.downloadAttachment(t.Context(), target)
				var api *ProviderHTTPError
				require.ErrorAs(t, err, &api)
				require.Equal(t, status, api.Status)
				require.True(t, api.Retryable())
				var retry interface{ AntflyRetryAfter() time.Time }
				require.ErrorAs(t, err, &retry)
				require.WithinDuration(t, expected, retry.AntflyRetryAfter(), 2*time.Second)
			}
		}
	}
}
