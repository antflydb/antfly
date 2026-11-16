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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeInvitationParentRevisionIsIndependentOfAttachmentDerivation(t *testing.T) {
	f := newWorkspaceRunnerFixture(t)
	connector, err := f.runner.Factory(t.Context(), f.runner.Profile.Connections["docs"])
	require.NoError(t, err)
	c := connector.(*CollaborationConnector)
	c.NativeAttachments = true
	base := c.HTTPClient.Transport
	invitation := []byte("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:meeting-1\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	unavailable, changed := false, false
	downloads := 0
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/attachments/") {
			downloads++
			if unavailable {
				return &http.Response{StatusCode: 503, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
			}
			raw, err := json.Marshal(map[string]any{"data": base64.RawURLEncoding.EncodeToString(invitation), "size": len(invitation)})
			require.NoError(t, err)
			return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(bytes.NewReader(raw))}, nil
		}
		response, err := base.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		if r.URL.Path == "/gmail/messages/message1" {
			raw, err := io.ReadAll(response.Body)
			response.Body.Close()
			require.NoError(t, err)
			var m map[string]any
			require.NoError(t, json.Unmarshal(raw, &m))
			if changed {
				m["historyId"] = "edited"
			}
			payload := m["payload"].(map[string]any)
			parts := payload["parts"].([]any)
			part := parts[1].(map[string]any)
			part["mimeType"] = "text/calendar"
			part["filename"] = "invite.ics"
			part["body"].(map[string]any)["size"] = len(invitation)
			payload["parts"] = append(parts, map[string]any{"partId": "2", "mimeType": "image/png", "filename": "inline.png", "body": map[string]any{"data": base64.RawURLEncoding.EncodeToString([]byte("image")), "size": 5}})
			raw, err = json.Marshal(m)
			require.NoError(t, err)
			response.Body = io.NopCloser(bytes.NewReader(raw))
		}
		return response, nil
	})}
	r := Request{Mode: ImportModeSource, SourceNamespace: "job/" + testSourceID, Config: f.runner.Profile.Sources[0].Config}
	inv, err := c.Enumerate(t.Context(), r)
	require.NoError(t, err)
	var parent, inline Item
	for _, item := range inv.Objects {
		if configString(item.Metadata, "attachment_id") == "" {
			parent = item
		}
		if configString(item.Metadata, "attachment_id") == "2" {
			inline = item
		}
	}
	require.NotEmpty(t, parent.ID)
	require.NotEmpty(t, inline.ID)
	require.Equal(t, true, parent.Metadata["calendar_invitation"])
	require.Contains(t, parent.Metadata["calendar_uids"], "meeting-1")
	require.Equal(t, parent.Revision, configString(inline.Metadata, "parent_revision"))
	downloads = 0
	unavailable = true
	data, err := c.Read(t.Context(), r, parent)
	require.NoError(t, err)
	require.Contains(t, string(data), "body")
	require.Zero(t, downloads)
	data, err = c.Read(t.Context(), r, inline)
	require.NoError(t, err)
	require.Equal(t, "image", string(data))
	require.Zero(t, downloads)
	changed = true
	_, err = c.Read(t.Context(), r, parent)
	var revision *RevisionChangedError
	require.ErrorAs(t, err, &revision)
	require.Zero(t, downloads)
}
