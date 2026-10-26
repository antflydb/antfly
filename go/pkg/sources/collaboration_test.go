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
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type collaborationTransport func(*http.Request) (*http.Response, error)

func (f collaborationTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fixtureClient(t *testing.T, handler func(*http.Request) (int, any)) *http.Client {
	t.Helper()
	return &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		status, value := handler(r)
		data, err := json.Marshal(value)
		require.NoError(t, err)
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(data)), Request: r}, nil
	})}
}

func TestWorkspaceGmailAttachmentsRevisionAndAccountScope(t *testing.T) {
	account := "alice@example.com"
	body := "retry budget"
	revoked := false
	cycle := false
	client := fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/gmail/profile":
			return 200, map[string]any{"emailAddress": account}
		case "/gmail/labels/INBOX":
			return 200, map[string]any{"id": "INBOX"}
		case "/gmail/messages":
			if revoked {
				return 403, nil
			}
			next := ""
			if cycle {
				next = "repeat"
			}
			return 200, map[string]any{"messages": []any{map[string]any{"id": "mail1"}}, "nextPageToken": next}
		case "/gmail/messages/mail1":
			if revoked {
				return 403, nil
			}
			return 200, map[string]any{"id": "mail1", "threadId": "thread1", "historyId": "12", "labelIds": []string{"INBOX"}, "payload": map[string]any{"mimeType": "multipart/mixed", "headers": []any{map[string]any{"name": "Subject", "value": "Budget"}, map[string]any{"name": "Message-ID", "value": "<mail@example.com>"}}, "parts": []any{
				map[string]any{"partId": "0", "mimeType": "text/html", "body": map[string]any{"data": base64.RawURLEncoding.EncodeToString([]byte(`<p>` + body + `</p><a href="https://docs.google.com/document/d/drive1/edit">Design</a>`))}},
				map[string]any{"partId": "1", "filename": "notes.txt", "mimeType": "text/plain", "body": map[string]any{"attachmentId": "att1", "size": 16}},
			}}}
		case "/gmail/messages/mail1/attachments/att1":
			return 200, map[string]any{"data": base64.RawURLEncoding.EncodeToString([]byte("attachment retry")), "size": 16}
		default:
			t.Errorf("unexpected request %s", r.URL)
			return 404, nil
		}
	})
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}, HTTPClient: client, GmailBase: "https://fixture.test/gmail"}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"resource":"gmail","label_ids":["INBOX"],"attachments":true}`)}
	inv, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.True(t, inv.Authoritative)
	require.Len(t, inv.Objects, 2)
	var message, attachment Item
	for _, item := range inv.Objects {
		if configString(item.Metadata, "attachment_id") != "" {
			attachment = item
		} else {
			message = item
		}
	}
	require.Equal(t, "workspace://job/id/gmail/alice@example.com/mail1", message.ID)
	require.Equal(t, message.ID, attachment.Metadata["parent_identity"])
	require.Contains(t, fmt.Sprint(message.Metadata["related_resources"]), "drive1")
	data, err := c.Read(t.Context(), r, attachment)
	require.NoError(t, err)
	require.Equal(t, "attachment retry", string(data))
	data, err = c.Read(t.Context(), r, message)
	require.NoError(t, err)
	require.Contains(t, string(data), "retry budget")
	body = "changed"
	_, err = c.Read(t.Context(), r, message)
	require.ErrorAs(t, err, new(*RevisionChangedError))
	account = "bob@example.com"
	require.Error(t, c.CheckAccess(t.Context(), r, message))
	account = "alice@example.com"
	revoked = true
	require.Error(t, c.ValidateAbsentSources(t.Context(), r, []string{message.ID}))
	revoked = false
	cycle = true
	_, err = c.Enumerate(t.Context(), r)
	require.ErrorIs(t, err, ErrInventoryIncomplete)
}

func TestWorkspaceCalendarLinksRecurrenceAndRevocation(t *testing.T) {
	role := "reader"
	status := "confirmed"
	etag := "v1"
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar"}, CalendarBase: "https://fixture.test/calendar"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/calendar/users/me/calendarList/primary":
			return 200, map[string]any{"id": "alice@example.com", "accessRole": role}
		case "/calendar/calendars/alice@example.com/events":
			require.Equal(t, "false", r.URL.Query().Get("singleEvents"))
			return 200, map[string]any{"items": []any{map[string]any{"id": "event1", "status": status}}}
		case "/calendar/calendars/alice@example.com/events/event1":
			return 200, map[string]any{"id": "event1", "etag": etag, "status": status, "summary": "Budget planning", "description": `<a href="https://drive.google.com/file/d/file1/view">Plan</a>`, "htmlLink": "https://calendar.google.com/calendar/event?eid=event1", "iCalUID": "shared@example.com", "recurrence": []string{"RRULE:FREQ=WEEKLY"}, "start": map[string]any{"dateTime": "2026-10-27T10:00:00-07:00", "timeZone": "America/Los_Angeles"}, "attachments": []any{map[string]any{"fileId": "file2", "title": "Meeting notes"}}}
		default:
			t.Errorf("unexpected request %s", r.URL)
			return 404, nil
		}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"resource":"calendar","calendar_ids":["primary"]}`)}
	inv, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, inv.Objects, 1)
	require.Equal(t, "shared@example.com", inv.Objects[0].Metadata["ical_uid"])
	require.Len(t, inv.Objects[0].Metadata["related_resources"], 2)
	_, err = c.Read(t.Context(), r, inv.Objects[0])
	require.NoError(t, err)
	etag = "v2"
	_, err = c.Read(t.Context(), r, inv.Objects[0])
	require.ErrorAs(t, err, new(*RevisionChangedError))
	role = "freeBusyReader"
	require.Error(t, c.ValidateAbsentSources(t.Context(), r, []string{inv.Objects[0].ID}))
	role = "reader"
	status = "cancelled"
	require.NoError(t, c.ValidateAbsentSources(t.Context(), r, []string{inv.Objects[0].ID}))
}

func TestSlackThreadsEditsAndIncompleteHistory(t *testing.T) {
	text := "reply retry"
	limited := false
	member := true
	c := &CollaborationConnector{ProviderName: ProviderSlack, APIBase: "https://fixture.test/slack", AccessToken: func(context.Context) (string, error) { return "secret", nil }}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		require.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/slack/auth.test":
			return 200, map[string]any{"ok": true, "team_id": "T1"}
		case "/slack/conversations.info":
			return 200, map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": member}}
		case "/slack/conversations.history":
			return 200, map[string]any{"ok": true, "is_limited": limited, "messages": []any{map[string]any{"ts": "100.000001", "text": "root", "reply_count": 1}}}
		case "/slack/conversations.replies":
			require.Equal(t, "100.000001", r.URL.Query().Get("ts"))
			return 200, map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "100.000001", "text": "root"}, map[string]any{"ts": "100.000002", "thread_ts": "100.000001", "text": text}}}
		case "/slack/chat.getPermalink":
			return 200, map[string]any{"ok": true, "permalink": "https://team.slack.com/archives/C1/p" + strings.ReplaceAll(r.URL.Query().Get("message_ts"), ".", "")}
		default:
			t.Errorf("unexpected request %s", r.URL)
			return 404, nil
		}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"team_id":"T1","channel_ids":["C1"]}`)}
	inv, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, inv.Objects, 2)
	data, err := c.Read(t.Context(), r, inv.Objects[1])
	require.NoError(t, err)
	require.Equal(t, text, string(data))
	text = "edited"
	_, err = c.Read(t.Context(), r, inv.Objects[1])
	require.ErrorAs(t, err, new(*RevisionChangedError))
	limited = true
	_, err = c.Enumerate(t.Context(), r)
	require.ErrorIs(t, err, ErrInventoryIncomplete)
	limited = false
	member = false
	require.Error(t, c.CheckAccess(t.Context(), r, inv.Objects[0]))
}

func TestDiscordEmptyHistoryRequiresPermissionsAndContentIntent(t *testing.T) {
	permissions := "66560"
	flags := uint64(1 << 19)
	messages := true
	c := &CollaborationConnector{ProviderName: ProviderDiscord, APIBase: "https://fixture.test/discord"}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		switch r.URL.Path {
		case "/discord/channels/2":
			return 200, map[string]any{"id": "2", "guild_id": "1", "type": 0}
		case "/discord/users/@me":
			return 200, map[string]any{"id": "3"}
		case "/discord/guilds/1/members/3":
			return 200, map[string]any{"roles": []string{}}
		case "/discord/guilds/1/roles":
			return 200, []any{map[string]any{"id": "1", "permissions": permissions}}
		case "/discord/oauth2/applications/@me":
			return 200, map[string]any{"flags": flags}
		case "/discord/channels/2/messages":
			if !messages || r.URL.Query().Get("before") != "" {
				return 200, []any{}
			}
			return 200, []any{map[string]any{"id": "4"}}
		case "/discord/channels/2/messages/4":
			return 200, map[string]any{"id": "4", "channel_id": "2", "content": "retry budget", "timestamp": "2026-10-26T00:00:00Z", "author": map[string]any{"id": "5"}}
		default:
			t.Errorf("unexpected request %s", r.URL)
			return 404, nil
		}
	})
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"guild_id":"1","channel_ids":["2"],"include_threads":false}`)}
	inv, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	require.Len(t, inv.Objects, 1)
	data, err := c.Read(t.Context(), r, inv.Objects[0])
	require.NoError(t, err)
	require.Equal(t, "retry budget", string(data))
	messages = false
	permissions = "1024"
	require.Error(t, c.ValidateAbsentSources(t.Context(), r, []string{inv.Objects[0].ID}))
	permissions = "66560"
	flags = 0
	_, err = c.Enumerate(t.Context(), r)
	require.ErrorContains(t, err, "MESSAGE_CONTENT")
	flags = 1 << 19
	require.NoError(t, c.ValidateAbsentSources(t.Context(), r, []string{inv.Objects[0].ID}))
}

func TestAttachmentExtractionAndDownloadOrigin(t *testing.T) {
	text, err := ExtractAttachmentText(t.Context(), "hello.html", "text/html", []byte(`<p>retry <b>budget</b></p><script>secret</script>`))
	require.NoError(t, err)
	require.Equal(t, "retry budget", string(text))
	var buffer bytes.Buffer
	z := zip.NewWriter(&buffer)
	file, err := z.Create("word/document.xml")
	require.NoError(t, err)
	_, err = io.WriteString(file, `<w:document xmlns:w="word"><w:p><w:r><w:t>attachment retry</w:t></w:r></w:p></w:document>`)
	require.NoError(t, err)
	require.NoError(t, z.Close())
	text, err = ExtractAttachmentText(t.Context(), "notes.docx", "", buffer.Bytes())
	require.NoError(t, err)
	require.Contains(t, string(text), "attachment retry")
	_, err = ExtractAttachmentText(t.Context(), "notes.bin", "application/octet-stream", []byte{0, 1})
	require.Error(t, err)
	c := &CollaborationConnector{ProviderName: ProviderSlack}
	for _, url := range []string{"http://files.slack.com/a", "https://files.slack.com.attacker.test/a", "https://files.slack.com:444/a", "https://user@files.slack.com/a", "https://127.0.0.1/a"} {
		_, err := c.downloadAttachment(t.Context(), url)
		require.Error(t, err)
	}
	c.AccessToken = func(context.Context) (string, error) { return "secret", nil }
	c.DownloadTransport = collaborationTransport(func(r *http.Request) (*http.Response, error) {
		return nil, &url.Error{Op: "Get", URL: r.URL.String(), Err: context.DeadlineExceeded}
	})
	_, err = c.downloadAttachment(t.Context(), "https://files.slack.com/a?temporary=credential")
	require.Error(t, err)
	require.NotContains(t, err.Error(), "temporary=")
	links := SourceLinks([]byte("https://docs.google.com/document/d/id1/edit?access_token=secret"))
	require.Len(t, links, 1)
	require.NotContains(t, links[0].URL, "secret")
}

func TestWorkspaceServiceAdmissionAndProviderErrors(t *testing.T) {
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/id", Config: SourceConfig(`{"resource":"gmail"}`)}
	require.ErrorContains(t, c.Validate(t.Context(), r), "reconnect")
	c.Services = []string{"gmail"}
	require.NoError(t, c.Validate(t.Context(), r))
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"60"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}
	var out any
	err := c.get(t.Context(), "gmail", "/profile", &out)
	var providerErr *ProviderHTTPError
	require.ErrorAs(t, err, &providerErr)
	require.True(t, providerErr.Retryable())
	require.True(t, providerErr.RetryAt.After(time.Now().Add(50*time.Second)))
}

func TestCollaborationRunnerPublishesAttachmentsAndProtectsRevokedInventory(t *testing.T) {
	runner, _, backend := runnerFixture(t)
	attachment := true
	member := true
	content := "attachment retry"
	c := &CollaborationConnector{ProviderName: ProviderSlack, AccessToken: func(context.Context) (string, error) { return "provider-only-token", nil }}
	c.HTTPClient = fixtureClient(t, func(r *http.Request) (int, any) {
		require.Equal(t, "Bearer provider-only-token", r.Header.Get("Authorization"))
		switch r.URL.Path {
		case "/api/auth.test":
			return 200, map[string]any{"ok": true, "team_id": "T1"}
		case "/api/conversations.info":
			return 200, map[string]any{"ok": true, "channel": map[string]any{"id": "C1", "is_member": member}}
		case "/api/conversations.history":
			files := []any{}
			if attachment {
				files = append(files, map[string]any{"id": "F1", "name": "notes.txt", "mimetype": "text/plain", "size": len(content), "url_private_download": "https://files.slack.com/private/notes?temporary=secret"})
			}
			return 200, map[string]any{"ok": true, "messages": []any{map[string]any{"ts": "100.000001", "text": "message retry", "files": files}}}
		case "/api/chat.getPermalink":
			return 200, map[string]any{"ok": true, "permalink": "https://team.slack.com/archives/C1/p100000001"}
		default:
			t.Errorf("unexpected request %s", r.URL)
			return 404, nil
		}
	})
	c.DownloadTransport = collaborationTransport(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "files.slack.com", r.URL.Host)
		require.Equal(t, "Bearer provider-only-token", r.Header.Get("Authorization"))
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(content)), Request: r}, nil
	})
	runner.Profile.Connections = map[string]Connection{"chat": {Provider: ProviderSlack}}
	runner.Profile.Sources[0].Connection = "chat"
	runner.Profile.Sources[0].Config = SourceConfig(`{"team_id":"T1","channel_ids":["C1"],"include_threads":false,"attachments":true}`)
	runner.Factory = func(context.Context, Connection) (Connector, error) { return c, nil }
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	require.Len(t, backend.records, 2)
	b, _ := json.Marshal(backend.records)
	require.Contains(t, string(b), content)
	require.NotContains(t, string(b), "temporary=secret")
	require.NotContains(t, string(b), "provider-only-token")
	backend.mu.Unlock()
	if binary := os.Getenv("ANTFLY_GREP_TEST_BINARY"); binary != "" {
		command := exec.CommandContext(t.Context(), binary, "--source-endpoint", runner.Profile.Endpoint, "--source-principal", "user:self", "--source", "manuals", "--json", "retry")
		for _, entry := range os.Environ() {
			if !strings.HasPrefix(entry, "ANTFLY_GREP_") && !strings.HasPrefix(entry, "RIPGREP_CONFIG_PATH=") {
				command.Env = append(command.Env, entry)
			}
		}
		command.Env = append(command.Env, "ANTFLY_GREP_CONFIG_PATH=", "RIPGREP_CONFIG_PATH=", "ANTFLY_GREP_TABLE_TOKEN=database-test-token")
		output, err := command.CombinedOutput()
		require.NoError(t, err, string(output))
		require.Contains(t, string(output), "attachment retry")
		require.Contains(t, string(output), "message retry")
		require.Contains(t, string(output), "parent_identity")
	}
	// Repeated publication reuses canonical hashes; attachment edits replace the
	// owned attachment without altering its stable ID or leaving duplicate rows.
	require.NoError(t, runner.Sync(t.Context()))
	content = "updated attachment retry"
	require.NoError(t, runner.Sync(t.Context()))
	attachment = false
	member = false
	require.Error(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	require.Len(t, backend.records, 2)
	backend.mu.Unlock()
	member = true
	runner.Restart = true
	require.NoError(t, runner.Sync(t.Context()))
	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	backend.mu.Unlock()
}
