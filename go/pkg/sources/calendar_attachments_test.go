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
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func calendarAttachmentFixture(t *testing.T, count int, payload string, native bool, invalidAt ...int) (*CollaborationConnector, *int, *int, []*googleCountingReader) {
	t.Helper()
	downloads, metadata := new(int), new(int)
	readers := make([]*googleCountingReader, count)
	for i := range readers {
		readers[i] = &googleCountingReader{Reader: strings.NewReader(payload)}
	}
	c := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"calendar", "drive"}, APIBase: "https://fixture.test", DriveBase: "https://fixture.test"}
	c.HTTPClient = &http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		var raw []byte
		if r.URL.Path == "/calendars/cal/events/event" {
			attachments := []map[string]string{}
			for i := 0; i < count; i++ {
				id := fmt.Sprintf("f%d", i)
				if len(invalidAt) > 0 && i == invalidAt[0] {
					id = ""
				}
				attachments = append(attachments, map[string]string{"fileId": id, "title": "notes.txt", "mimeType": "text/plain"})
			}
			raw, _ = json.Marshal(map[string]any{"id": "event", "etag": "v1", "summary": "event", "attachments": attachments})
		} else if r.URL.Query().Get("alt") == "media" || strings.HasSuffix(r.URL.Path, "/export") {
			index := *downloads
			*downloads++
			require.Less(t, index, len(readers))
			return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(readers[index])}, nil
		} else {
			*metadata++
			id := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/files/"), "/export")
			mime, size := "text/plain", fmt.Sprint(len(payload))
			if native {
				mime = "application/vnd.google-apps.document"
				size = ""
			}
			raw, _ = json.Marshal(map[string]any{"id": id, "version": "1", "name": "notes.txt", "mimeType": mime, "size": size})
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Request: r, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})}
	return c, downloads, metadata, readers
}

func TestCalendarAttachmentPreviewBoundsAcquisitionBeforeDriveRequests(t *testing.T) {
	for _, preview := range []bool{false, true} {
		t.Run(fmt.Sprint(preview), func(t *testing.T) {
			c, downloads, metadata, _ := calendarAttachmentFixture(t, 20, "body", false)
			config := CollaborationConfig{Attachments: true, preview: preview}
			ref := collaborationRef{Resource: "calendar", Container: "cal", ID: "event"}
			material, err := c.calendarLoad(t.Context(), config, ref)
			require.NoError(t, err)
			require.Zero(t, *downloads, "Calendar material must defer Drive bytes")
			require.Zero(t, *metadata, "unsampled Drive files must not incur metadata reads either")
			inputs, err := c.materialItems(t.Context(), Request{Preview: preview, SourceNamespace: "preview/test"}, config, ref, material)
			require.NoError(t, err)
			expected := 20
			if preview {
				expected = PreviewItemLimit - 1
			}
			require.Len(t, inputs, expected+1)
			require.Equal(t, expected, *downloads)
			for _, input := range inputs[1:] {
				require.Equal(t, "body", string(input.Bytes))
				require.Equal(t, "1", configString(input.Metadata, "drive_revision"))
				require.Equal(t, inputs[0].ID, configString(input.Metadata, "parent_identity"))
			}
		})
	}
}

func TestCalendarAttachmentsRespectRemainingAggregateBudget(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint(native), func(t *testing.T) {
			c, downloads, _, readers := calendarAttachmentFixture(t, 3, strings.Repeat("x", 17<<20), native)
			extracted := 0
			c.Extractor = func(context.Context, string, string, []byte) ([]byte, error) {
				extracted++
				return []byte("small"), nil
			}
			config := CollaborationConfig{Attachments: true}
			ref := collaborationRef{Resource: "calendar", Container: "cal", ID: "event"}
			material, err := c.calendarLoad(t.Context(), config, ref)
			require.NoError(t, err)
			_, err = c.materialItems(t.Context(), Request{SourceNamespace: "job/test"}, config, ref, material)
			require.ErrorIs(t, err, ErrInvalidInput)
			require.Equal(t, 1, extracted, "stop before extracting content outside the parent budget")
			expected := 1
			if native {
				expected = 2
			}
			require.Equal(t, expected, *downloads, "known binary sizes fail before download; native exports are read only within the remaining budget")
			read := 0
			for _, reader := range readers {
				read += reader.read
			}
			require.LessOrEqual(t, read, maxImportSourceObjectBytes+1)
		})
	}
}

func TestCalendarPreviewDoesNotValidateUnsampledExternalAttachments(t *testing.T) {
	for _, preview := range []bool{false, true} {
		c, downloads, _, _ := calendarAttachmentFixture(t, 20, "body", false, 8)
		config := CollaborationConfig{Attachments: true, preview: preview}
		ref := collaborationRef{Resource: "calendar", Container: "cal", ID: "event"}
		material, err := c.calendarLoad(t.Context(), config, ref)
		if !preview {
			require.Error(t, err)
			require.Zero(t, *downloads)
			continue
		}
		require.NoError(t, err)
		inputs, err := c.materialItems(t.Context(), Request{Preview: true, SourceNamespace: "preview/test"}, config, ref, material)
		require.NoError(t, err)
		require.Len(t, inputs, PreviewItemLimit)
		require.Equal(t, PreviewItemLimit-1, *downloads)
	}
}
