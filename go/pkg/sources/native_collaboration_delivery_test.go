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
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNativeCollaborationDeliveryUsesSelectedAttachment(t *testing.T) {
	f := newWorkspaceRunnerFixture(t)
	connector, err := f.runner.Factory(t.Context(), f.runner.Profile.Connections["docs"])
	require.NoError(t, err)
	c := connector.(*CollaborationConnector)
	c.NativeAttachments = true
	base := c.HTTPClient.Transport
	downloads := 0
	deleted := false
	changed := false
	brokenSibling := false
	changedBytes := false
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		if strings.Contains(r.URL.Path, "/attachments/attachment") {
			if brokenSibling && strings.HasSuffix(r.URL.Path, "/attachment3") {
				return nil, fmt.Errorf("sibling unavailable")
			}

			downloads++
			data := "notes"
			if changedBytes {
				data = "other"
			}
			body := fmt.Sprintf(`{"data":%q,"size":5}`, base64.RawURLEncoding.EncodeToString([]byte(data)))
			return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(body))}, nil
		}
		if r.URL.Path == "/gmail/messages/message1" && deleted {
			return &http.Response{StatusCode: 404, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(`{"error":"deleted"}`))}, nil
		}
		response, err := base.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		if r.URL.Path == "/gmail/messages/message1" {
			data, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				return nil, err
			}
			var message map[string]any
			require.NoError(t, json.Unmarshal(data, &message))
			if changed {
				message["historyId"] = "changed"
			}
			payload := message["payload"].(map[string]any)
			parts := payload["parts"].([]any)
			parts = parts[:1]
			for i := 1; i <= 3; i++ {
				parts = append(parts, map[string]any{"partId": fmt.Sprint(i), "mimeType": "image/png", "filename": fmt.Sprintf("original-%d.png", i), "body": map[string]any{"attachmentId": fmt.Sprintf("attachment%d", i), "size": 5}})
			}
			payload["parts"] = parts
			data, err = json.Marshal(message)
			require.NoError(t, err)
			response.Body = io.NopCloser(bytes.NewReader(data))
		}
		return response, nil
	})}
	request := Request{Mode: ImportModeSource, SourceNamespace: "job/" + testSourceID, Config: f.runner.Profile.Sources[0].Config}
	var config CollaborationConfig
	require.NoError(t, json.Unmarshal(request.Config, &config))
	inputs, err := c.hydrate(t.Context(), request, config, collaborationRef{Resource: "gmail", Container: f.account, ID: "message1"}, nil)
	require.NoError(t, err)
	var media []Input
	for _, input := range inputs {
		if UsesNativeResource(Source{}, input.Item) {
			media = append(media, input)
		}
	}
	require.Len(t, media, 3)
	t.Run("head_guard", func(t *testing.T) {
		downloads = 0
		require.NoError(t, checkMediaAccess(t.Context(), c, c, request, media[0].Item))
		recorder := httptest.NewRecorder()
		ServeMedia(recorder, httptest.NewRequest("HEAD", "https://proxy.test/item", nil), c, request, media[0].Item, fmt.Sprintf("%x", sha256.Sum256(media[0].Bytes)))
		require.Equal(t, 200, recorder.Code)
		require.Zero(t, downloads)
		t.Logf("HEAD authorization downloaded %d attachment bodies", downloads)
	})
	t.Run("sibling_fetch_amplification", func(t *testing.T) {
		downloads = 0
		for _, item := range media {
			data, acquired, err := c.AcquireMedia(t.Context(), request, item.Item)
			require.NoError(t, err)
			require.True(t, acquired)
			require.Equal(t, []byte("notes"), data)
		}
		require.Equal(t, 3, downloads)
		t.Logf("serving 3 attachments downloaded %d attachment bodies", downloads)
	})
	t.Run("http_get_and_conditional", func(t *testing.T) {
		downloads = 0
		require.NoError(t, checkMediaAccess(t.Context(), c, c, request, media[0].Item))
		recorder := httptest.NewRecorder()
		digest := fmt.Sprintf("%x", sha256.Sum256(media[0].Bytes))
		ServeMedia(recorder, httptest.NewRequest("GET", "https://proxy.test/item", nil), c, request, media[0].Item, digest)
		require.Equal(t, 200, recorder.Code)
		require.Equal(t, "notes", recorder.Body.String())
		require.Equal(t, 1, downloads)
		conditional := httptest.NewRequest("GET", "https://proxy.test/item", nil)
		conditional.Header.Set("If-None-Match", `"`+digest+`"`)
		require.NoError(t, checkMediaAccess(t.Context(), c, c, request, media[0].Item))
		recorder = httptest.NewRecorder()
		ServeMedia(recorder, conditional, c, request, media[0].Item, digest)
		require.Equal(t, 304, recorder.Code)
		require.Equal(t, 1, downloads)
	})

	t.Run("unrelated_sibling_failure", func(t *testing.T) {
		brokenSibling = true
		defer func() { brokenSibling = false }()
		downloads = 0
		data, _, err := c.AcquireMedia(t.Context(), request, media[0].Item)
		require.NoError(t, err)
		require.Equal(t, "notes", string(data))
		require.Equal(t, 1, downloads)
	})
	t.Run("parent_revision_changed", func(t *testing.T) {
		changed = true
		defer func() { changed = false }()
		downloads = 0
		require.Error(t, c.CheckMedia(t.Context(), request, media[0].Item))
		require.Zero(t, downloads)
	})
	t.Run("same_length_bytes_changed", func(t *testing.T) {
		changedBytes = true
		defer func() { changedBytes = false }()
		downloads = 0
		_, _, err := c.AcquireMedia(t.Context(), request, media[0].Item)
		require.Error(t, err)
		require.Equal(t, 1, downloads)
	})
	t.Run("deleted_head", func(t *testing.T) {
		deleted = true
		downloads = 0
		require.Error(t, c.CheckMedia(t.Context(), request, media[0].Item))
		require.Error(t, checkMediaAccess(t.Context(), c, c, request, media[0].Item))
		_, _, err := c.AcquireMedia(t.Context(), request, media[0].Item)
		require.Error(t, err)
		require.Zero(t, downloads)
	})
}
