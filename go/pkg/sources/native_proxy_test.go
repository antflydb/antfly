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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeCalendarDocumentFormat(t *testing.T) {
	mime := ""
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar", "drive"}, NativeAttachments: true, DriveBase: "https://fixture.test/drive"}
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"id":"file","version":"1","name":"slides","mimeType":"application/vnd.google-apps.presentation"}`
		if strings.HasSuffix(r.URL.Path, "/export") {
			mime = r.URL.Query().Get("mimeType")
			body = "export"
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	f, err := c.workspaceDriveFile(t.Context(), "file", maxImportSourceObjectBytes)
	if err != nil {
		t.Fatal(err)
	}
	if mime != "application/pdf" {
		t.Fatalf("native attachment exported as %s (published MIME %s)", mime, f.MIME)
	}
}
func TestNativeExportDeliveryAcquiresOnceWithoutStorage(t *testing.T) {
	data := []byte(strings.Repeat("x", 3*mediaReadWindow+1))
	exports := 0
	client := &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		body := `{"id":"file","version":"1","capabilities":{"canDownload":true}}`
		if strings.HasSuffix(r.URL.Path, "/export") {
			exports++
			body = string(data)
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(body))}, nil
	})}
	c := &GoogleConnector{ProviderName: ProviderGoogleDrive, HTTPClient: withDriveScopes(client, "https://www.googleapis.com/auth/drive.readonly"), APIBase: "https://fixture.test/drive"}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	item := Item{ID: "file", Revision: "1", Size: int64(len(data)), Metadata: map[string]any{"file_id": "file", "export_mime_type": "application/pdf", "representation_sha256": digest}}
	request := httptest.NewRequest("GET", "https://proxy.test/media", nil)
	response := httptest.NewRecorder()
	ServeMedia(response, request, c, Request{}, item, digest)
	if response.Code != 200 {
		t.Fatalf("delivery: %d %s", response.Code, response.Body.String())
	}
	if exports != 1 {
		t.Fatalf("one GET exported representation %d times", exports)
	}
	if !bytes.Equal(data, response.Body.Bytes()) {
		t.Fatal("wrong representation")
	}
	response = httptest.NewRecorder()
	ServeMedia(response, httptest.NewRequest("HEAD", "https://proxy.test/media", nil), c, Request{}, item, digest)
	if exports != 1 {
		t.Fatal("HEAD fetched body")
	}
	data[0] = 'y'
	response = httptest.NewRecorder()
	ServeMedia(response, request, c, Request{}, item, digest)
	if response.Code < 400 {
		t.Fatal("changed representation accepted")
	}
}

func TestNativeAttachmentCheckpointExcludesOriginalBytes(t *testing.T) {
	f := newWorkspaceRunnerFixture(t)
	connector, err := f.runner.Factory(t.Context(), f.runner.Profile.Connections["docs"])
	if err != nil {
		t.Fatal(err)
	}
	c := connector.(*CollaborationConnector)
	c.NativeAttachments = true
	base := c.HTTPClient.Transport
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		response, err := base.RoundTrip(r)
		if err != nil {
			return nil, err
		}
		if strings.HasSuffix(r.URL.Path, "/messages/message1") {
			raw, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				return nil, err
			}
			var message map[string]any
			if err := json.Unmarshal(raw, &message); err != nil {
				return nil, err
			}
			part := message["payload"].(map[string]any)["parts"].([]any)[1].(map[string]any)
			part["mimeType"] = "image/png"
			part["filename"] = "original.png"
			raw, err = json.Marshal(message)
			if err != nil {
				return nil, err
			}
			response.Body = io.NopCloser(bytes.NewReader(raw))
		}
		return response, nil
	})}
	directory := t.TempDir()
	c.ContentStore = &FileAcquisitionContentStore{Directory: directory}
	request := Request{Mode: ImportModeSource, SourceNamespace: "job/" + testSourceID, Config: f.runner.Profile.Sources[0].Config}
	var config CollaborationConfig
	if err := json.Unmarshal(request.Config, &config); err != nil {
		t.Fatal(err)
	}
	inputs, err := c.hydrate(t.Context(), request, config, collaborationRef{Resource: "gmail", Container: f.account, ID: "message1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, input := range inputs {
		if UsesNativeResource(Source{}, input.Item) {
			downloads := f.attachments
			if err := c.CheckMedia(t.Context(), request, input.Item); err != nil {
				t.Fatal(err)
			}
			if f.attachments != downloads {
				t.Fatal("media authorization downloaded original bytes")
			}
			found = true
			if input.ContentRef != "" {
				t.Fatal("original attachment retained in checkpoint")
			}
		}
	}
	if !found {
		t.Fatal("fixture did not acquire native media")
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		payload, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		var envelope hydrationEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatal(err)
		}
		for _, input := range envelope.Inputs {
			if UsesNativeResource(Source{}, input.Item) {
				t.Fatal("durable payload contains original attachment bytes")
			}
		}
	}
}
