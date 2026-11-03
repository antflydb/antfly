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
	"github.com/stretchr/testify/require"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFactoryCancellationAndClose(t *testing.T) {
	s, fixture, _ := serviceFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	s.factory = func(ctx context.Context, _ Connection) (Connector, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-release:
			return fixture, nil
		}
	}
	response := serviceCall(s, "POST", "/v1/sync", map[string]any{})
	require.Equal(t, 202, response.Code)
	var job SyncJob
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	<-entered
	require.Equal(t, 202, serviceCall(s, "DELETE", "/v1/jobs/"+job.ID, nil).Code)
	job = waitServiceJob(t, s, job.ID)
	require.Equal(t, "cancelled", job.State)
	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not cancel construction")
	}

}

type followupTransport struct{}

func (followupTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("not called")
}
func TestCustomDefaultTransportSupported(t *testing.T) {
	s, _, _ := serviceFixture(t)
	original := http.DefaultTransport
	http.DefaultTransport = followupTransport{}
	defer func() { http.DefaultTransport = original }()
	var value any
	func() {
		defer func() { value = recover() }()
		_, _ = s.providerConnector(t.Context(), s.profile.Connections["docs"])
	}()
	t.Logf("custom default RoundTripper panic: %v", value)
	require.Nil(t, value)
}
func TestPurgeOnlyRemovesMediaFiles(t *testing.T) {
	s, _, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	parts := strings.Split(ref.URI, "/")
	id := parts[len(parts)-2]
	pin, err := s.readMediaPin(id)
	require.NoError(t, err)
	source := s.profile.Sources[0]
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+source.ID+"?purge=true", nil).Code)
	response := serviceCall(s, "POST", "/v1/sync", map[string]any{"purge_only": true})
	require.Equal(t, 202, response.Code)
	var job SyncJob
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	job = waitServiceJob(t, s, job.ID)
	require.Equal(t, "completed", job.State)
	require.Equal(t, 1, job.PurgedSourceCount)
	_, pinErr := os.Stat(s.pinPath(id))
	_, digestErr := os.Stat(s.digestPath(pin.ContentIdentity))
	t.Logf("after completed purge: descriptor retained=%v digest retained=%v", pinErr == nil, digestErr == nil)
	require.ErrorIs(t, pinErr, os.ErrNotExist)
	require.ErrorIs(t, digestErr, os.ErrNotExist)
	require.Empty(t, s.profile.Sources)
}
func TestTCPWriteStopsAtRequestDeadline(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	item := &fixture.inventory.Objects[0]
	item.Size = 8 << 20
	fixture.data[item.ID] = make([]byte, item.Size)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	done := make(chan struct{})
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Millisecond)
		defer cancel()
		defer close(done)
		s.ServeHTTP(w, r.WithContext(ctx))
	}))
	server.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateNew {
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetWriteBuffer(1024)
			}
		}
	}
	server.Start()
	defer server.Close()
	remote, err := net.ResolveTCPAddr("tcp", strings.TrimPrefix(server.URL, "http://"))
	require.NoError(t, err)
	conn, err := net.DialTCP("tcp", nil, remote)
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, conn.SetReadBuffer(1024))
	uri, err := url.Parse(ref.URI)
	require.NoError(t, err)
	_, err = fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: source.example\r\nConnection: close\r\n\r\n", uri.RequestURI())
	require.NoError(t, err)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("socket write retained media slot after cancellation")
	}
	require.Empty(t, s.fetchSlots)
}

func TestPurgePreservesSharedDigestAndPendingPins(t *testing.T) {
	s, _, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	parts := strings.Split(mediaFromBackend(t, backend).URI, "/")
	id := parts[len(parts)-2]
	pin, err := s.readMediaPin(id)
	require.NoError(t, err)
	other := pin
	other.SourceID = "00000000-0000-4000-8000-000000000002"
	other.MAC = s.pinReceiptMAC(other)
	otherID := pinIdentityID(other.mediaPinIdentity)
	require.NoError(t, writeJSON(s.pinPath(otherID), other, 1<<20))
	source := s.profile.Sources[0]
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+source.ID+"?purge=true", nil).Code)
	response := serviceCall(s, "POST", "/v1/sync", map[string]any{"purge_only": true})
	require.Equal(t, 202, response.Code)
	var job SyncJob
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Equal(t, "completed", waitServiceJob(t, s, job.ID).State)
	_, err = os.Stat(s.pinPath(id))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = s.readMediaPin(otherID)
	require.NoError(t, err)
	_, err = os.Stat(s.digestPath(pin.ContentIdentity))
	require.NoError(t, err)
	// Replaying cleanup must be safe after an interruption before intent commit.
	require.NoError(t, s.purgeMediaFiles(t.Context(), map[string]bool{source.ID: true}))
	_, err = os.Stat(s.digestPath(pin.ContentIdentity))
	require.NoError(t, err)
}

func TestGoogleOpenSourceStreamsOriginalBeforeBodyIsAvailable(t *testing.T) {
	reader, writer := io.Pipe()
	defer writer.Close()
	const size = int64(64 << 20)
	client := &http.Client{Transport: previewDownloadTransport(func(r *http.Request) (*http.Response, error) {
		require.Equal(t, "123", r.URL.Query().Get("generation"))
		require.Equal(t, "123", r.URL.Query().Get("ifGenerationMatch"))
		return &http.Response{StatusCode: 200, ContentLength: size, Header: http.Header{"X-Goog-Generation": []string{"123"}}, Body: reader}, nil
	})}
	connector := &GoogleConnector{ProviderName: ProviderGCS, HTTPClient: client}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result := make(chan io.ReadCloser, 1)
	go func() {
		body, err := connector.OpenSource(ctx, Request{Config: SourceConfig(`{"bucket":"docs"}`)}, Item{Path: "large.txt", Size: size, Revision: "123", Metadata: map[string]any{"bucket": "docs"}})
		if err != nil {
			result <- nil
			return
		}
		result <- body
	}()
	select {
	case body := <-result:
		require.NotNil(t, body)
		require.ErrorIs(t, body.Close(), io.ErrUnexpectedEOF)
	case <-ctx.Done():
		reader.Close()
		t.Fatal("OpenSource buffered the body instead of returning a stream")
	}
}

func TestStreamedDriveChecksumRequiredBeforeAcceptingBytes(t *testing.T) {
	for _, expected := range []string{"5d41402abc4b2a76b9719d911017c592", "00000000000000000000000000000000"} {
		t.Run(expected, func(t *testing.T) {
			metadataReads := 0
			client := &http.Client{Transport: previewDownloadTransport(func(r *http.Request) (*http.Response, error) {
				payload := `{"version":"7"}`
				if r.URL.Query().Get("alt") == "media" {
					payload = "hello"
				} else {
					metadataReads++
				}
				return &http.Response{StatusCode: 200, ContentLength: int64(len(payload)), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(payload))}, nil
			})}
			connector := &GoogleConnector{ProviderName: ProviderGoogleDrive, HTTPClient: client}
			body, err := connector.OpenSource(t.Context(), Request{Config: SourceConfig(`{"folder_id":"folder"}`)}, Item{Path: "hello.txt", Size: 5, Revision: "7", Metadata: map[string]any{"file_id": "doc", "md5_checksum": expected}})
			require.NoError(t, err)
			data, err := io.ReadAll(body)
			require.NoError(t, err)
			require.Equal(t, "hello", string(data))
			err = body.Close()
			if expected[0] == '0' {
				require.ErrorContains(t, err, "checksum mismatch")
				require.Equal(t, 1, metadataReads)
			} else {
				require.NoError(t, err)
				require.Equal(t, 2, metadataReads)
			}
		})
	}
}
