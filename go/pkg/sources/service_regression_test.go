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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/stretchr/testify/require"
)

type countedMedia struct {
	*fixtureConnector
	bytes atomic.Int64
	opens atomic.Int64
}
type countedBody struct {
	io.ReadCloser
	count *atomic.Int64
}

func (b countedBody) Read(p []byte) (int, error) {
	n, e := b.ReadCloser.Read(p)
	b.count.Add(int64(n))
	return n, e
}
func (c *countedMedia) OpenMedia(ctx context.Context, r Request, item Item, span MediaRange) (io.ReadCloser, error) {
	body, err := c.fixtureConnector.OpenMedia(ctx, r, item, span)
	if err != nil {
		return nil, err
	}
	c.opens.Add(1)
	return countedBody{body, &c.bytes}, nil
}
func TestMediaStreamingAndIndexedAuthorization(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	item := &fixture.inventory.Objects[0]
	item.Size = 33 << 20 // Exceeds the text hydration limit.
	data := make([]byte, item.Size)
	for i := range data {
		data[i] = byte(i)
	}
	fixture.data[item.ID] = data
	counted := &countedMedia{fixtureConnector: fixture}
	var builds atomic.Int64
	s.factory = func(context.Context, Connection) (Connector, error) { builds.Add(1); return counted, nil }
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	sum := sha256.Sum256(data)
	require.Equal(t, `"`+hex.EncodeToString(sum[:])+`"`, ref.Version)
	require.Equal(t, item.Size, counted.bytes.Load()) // Hash once, streaming.
	require.Equal(t, "completed", startServiceJob(t, s).State)
	require.Equal(t, item.Size, counted.bytes.Load()) // Receipt avoids rehashing.
	require.EqualValues(t, 1, builds.Load())
	// Hot fetches must not parse the acquisition journal.
	require.NoError(t, os.WriteFile(filepath.Join(s.runnerPath(), s.profile.Sources[0].ID+".json"), []byte("unreadable acquisition"), 0600))
	counted.bytes.Store(0)
	counted.opens.Store(0)
	require.Equal(t, 200, fetchService(s, ref, "HEAD", "").Code)
	for i := 0; i < 8; i++ {
		response := fetchService(s, ref, "GET", "bytes=2-2")
		require.Equal(t, 206, response.Code)
		require.Equal(t, []byte{2}, response.Body.Bytes())
	}
	require.EqualValues(t, 8, counted.bytes.Load())
	require.EqualValues(t, 8, counted.opens.Load())
	require.EqualValues(t, 1, builds.Load())
	changed := ref
	changed.Version = `"wrong"`
	require.Equal(t, 412, fetchService(s, changed, "GET", "").Code)
	require.EqualValues(t, 8, counted.bytes.Load())
}
func TestMultipleRevocationsRemainRepairable(t *testing.T) {
	s, _, _ := serviceFixture(t)
	original := s.profile.Connections["docs"]
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/other", original).Code)
	source := s.profile.Sources[0]
	source.ID = "22222222-2222-4222-8222-222222222222"
	source.Connection = "other"
	source.Alias = "other"
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+source.ID, source).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/docs", nil).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/other", nil).Code)
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/docs", original).Code)
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/other", original).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/docs", nil).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/other", nil).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+source.ID, nil).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+s.profile.Sources[0].ID, nil).Code)
}
func TestSlowControlBodyDoesNotBlockReads(t *testing.T) {
	s, _, _ := serviceFixture(t)
	reader, writer := io.Pipe()
	request := httptest.NewRequest("PUT", "/v1/connections/slow", reader)
	request.Header.Set("Authorization", "Bearer "+serviceTestToken)
	finished := make(chan struct{})
	go func() { defer close(finished); s.ServeHTTP(httptest.NewRecorder(), request) }()
	defer func() { _ = writer.Close(); <-finished }()
	completed := make(chan int, 1)
	go func() { completed <- serviceCall(s, "GET", "/v1/profile", nil).Code }()
	select {
	case status := <-completed:
		require.Equal(t, 200, status)
	case <-time.After(time.Second):
		t.Fatal("control body holds global lock")
	}
}
func TestProviderPreconditionMapping(t *testing.T) {
	for _, err := range []error{
		minio.ErrorResponse{StatusCode: 412, Code: "PreconditionFailed"},
		&GoogleHTTPError{Status: 412},
	} {
		response := httptest.NewRecorder()
		serveMediaError(response, mediaProviderError(err))
		require.Equal(t, 412, response.Code)
		require.Contains(t, response.Body.String(), "source_version_changed")
	}
}
func TestSyncFailureIsActionableAndSafe(t *testing.T) {
	s, _, _ := serviceFixture(t)
	s.factory = func(context.Context, Connection) (Connector, error) {
		return nil, errors.New("private-provider-secret")
	}
	job := startServiceJob(t, s)
	require.Equal(t, "failed", job.State)
	require.NotNil(t, job.Failure)
	require.Equal(t, s.profile.Sources[0].ID, job.Failure.SourceID)
	require.Equal(t, "connect", job.Failure.Phase)
	require.Equal(t, "operation_failed", job.Failure.Code)
	raw, err := os.ReadFile(s.jobPath())
	require.NoError(t, err)
	require.NotContains(t, string(raw), "private-provider-secret")
	var persisted SyncJob
	require.NoError(t, json.Unmarshal(raw, &persisted))
	require.Equal(t, job.Failure, persisted.Failure)
}
func TestExplicitPurgeDeletesOnlyOwnedRowsWithoutProvider(t *testing.T) {
	s, _, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	source := s.profile.Sources[0]
	backend.mu.Lock()
	backend.records["foreign"] = map[string]any{"title": "keep"}
	backend.mu.Unlock()
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/docs", nil).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+source.ID+"?purge=true", nil).Code)
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	require.Contains(t, backend.records, "foreign")
	backend.mu.Unlock()
	queue, err := s.readPurges()
	require.NoError(t, err)
	require.Empty(t, queue.Sources)
	_, err = os.Stat(filepath.Join(s.runnerPath(), source.ID+".json"))
	require.True(t, os.IsNotExist(err))
}
func TestAuthorityCachePublicationCannotRestoreDeletedMembership(t *testing.T) {
	s, fixture, _ := serviceFixture(t)
	source := s.profile.Sources[0]
	item := fixture.inventory.Objects[0]
	require.NoError(t, s.publishMediaAuthority(context.Background(), source, nil, &fixture.inventory))
	for i := 0; i < 100; i++ {
		s.authorities.mu.Lock()
		s.authorities.entries, s.authorities.bytes = nil, 0
		s.authorities.mu.Unlock()
		done := make(chan struct{})
		go func() { _, _ = s.mediaAuthorized(source.ID, itemMediaKey(item)); close(done) }()
		require.NoError(t, s.publishMediaAuthority(context.Background(), source, nil, &Inventory{}))
		<-done
		allowed, err := s.mediaAuthorized(source.ID, itemMediaKey(item))
		require.NoError(t, err)
		require.False(t, allowed)
		require.NoError(t, s.publishMediaAuthority(context.Background(), source, nil, &fixture.inventory))
	}
}
func TestCatalogAdvertisesMediaVersion(t *testing.T) {
	s, _, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	backend.mu.Lock()
	defer backend.mu.Unlock()
	for _, row := range backend.catalogRecords {
		raw, err := json.Marshal(row)
		require.NoError(t, err)
		require.Contains(t, string(raw), `"document_version":3`)
		require.Contains(t, string(raw), `"publication":"media"`)
	}
}
func TestControlResponseFlushDoesNotHoldStateLock(t *testing.T) {
	s, _, _ := serviceFixture(t)
	writer := &blockingResponse{entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	request := httptest.NewRequest("GET", "/v1/profile", nil)
	request.Header.Set("Authorization", "Bearer "+serviceTestToken)
	go func() { defer close(done); s.ServeHTTP(writer, request) }()
	<-writer.entered
	defer func() { close(writer.release); <-done }()
	finished := make(chan int, 1)
	go func() { finished <- serviceCall(s, "GET", "/v1/profile", nil).Code }()
	select {
	case status := <-finished:
		require.Equal(t, 200, status)
	case <-time.After(time.Second):
		t.Fatal("slow response holds state lock")
	}
}

type blockingResponse struct{ entered, release chan struct{} }

func (w *blockingResponse) Header() http.Header { return http.Header{} }
func (w *blockingResponse) WriteHeader(int)     {}
func (w *blockingResponse) Write(p []byte) (int, error) {
	close(w.entered)
	<-w.release
	return len(p), nil
}

func TestMediaServiceRejectsDriveOutsideConnectionServices(t *testing.T) {
	connector := &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, Services: []string{"gmail"}}
	_, err := publicationConnector(connector, Source{Publication: "media", Config: json.RawMessage(`{"resource":"drive","folder_id":"root"}`)})
	require.ErrorContains(t, err, "reconnect Google Workspace with drive access")
	require.False(t, strings.Contains(err.Error(), "token"))
}

func TestS3MediaRangesUseReusableTransport(t *testing.T) {
	var connections atomic.Int64
	var transferred atomic.Int64
	reject := atomic.Bool{}
	origin := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<LocationConstraint>us-east-1</LocationConstraint>`)
			return
		}
		if reject.Load() {
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(412)
			_, _ = io.WriteString(w, `<Error><Code>PreconditionFailed</Code></Error>`)
			return
		}
		require.Equal(t, "bytes=2-2", r.Header.Get("Range"))
		require.Equal(t, `"revision-1"`, r.Header.Get("If-Match"))
		w.Header().Set("Last-Modified", "Mon, 2 Jan 2006 15:04:05 GMT")
		w.Header().Set("Content-Range", "bytes 2-2/13")
		w.Header().Set("Content-Length", "1")
		w.Header().Set("ETag", `"revision-1"`)
		w.WriteHeader(206)
		_, _ = w.Write([]byte{'x'})
		transferred.Add(1)
	}))
	origin.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	origin.Start()
	defer origin.Close()
	s, _, _ := serviceFixture(t)
	t.Setenv("SOURCE_POOL_ACCESS", "access")
	t.Setenv("SOURCE_POOL_SECRET", "secret")
	s.factory = EnvironmentFactory
	connection := Connection{Provider: ProviderS3, S3Endpoint: strings.TrimPrefix(origin.URL, "http://"), S3Insecure: true, AccessKeyEnv: "SOURCE_POOL_ACCESS", SecretKeyEnv: "SOURCE_POOL_SECRET"}
	item := Item{Path: "manual.txt", Revision: "revision-1", Size: 13, Metadata: map[string]any{"bucket": "docs"}}
	request := Request{Config: json.RawMessage(`{"bucket":"docs"}`), Mode: ImportModeSource}
	var first Connector
	for i := 0; i < 8; i++ {
		connector, err := s.providerConnector(t.Context(), connection)
		require.NoError(t, err)
		if first == nil {
			first = connector
		}
		require.Same(t, first, connector)
		body, err := connector.(MediaConnector).OpenMedia(t.Context(), request, item, MediaRange{2, 1})
		require.NoError(t, err)
		data, err := io.ReadAll(body)
		require.NoError(t, err)
		require.NoError(t, body.Close())
		require.Equal(t, "x", string(data))
	}
	require.EqualValues(t, 8, transferred.Load())
	require.EqualValues(t, 1, connections.Load())
	reject.Store(true)
	_, err := first.(MediaConnector).OpenMedia(t.Context(), request, item, MediaRange{2, 1})
	var changed *RevisionChangedError
	require.ErrorAs(t, err, &changed)
	s.invalidateClients()
	second, err := s.providerConnector(t.Context(), connection)
	require.NoError(t, err)
	require.NotSame(t, first, second)
}

type revisionOnCloseMedia struct{ *fixtureConnector }

func (c revisionOnCloseMedia) OpenMedia(ctx context.Context, r Request, item Item, span MediaRange) (io.ReadCloser, error) {
	body, err := c.fixtureConnector.OpenMedia(ctx, r, item, span)
	if err != nil {
		return nil, err
	}
	return &mediaBody{body: body, remaining: span.Length, verify: func() error { return &RevisionChangedError{Expected: "one", Actual: "two"} }}, nil
}
func TestMediaWindowRejectsRevisionChangeBeforeReleasingBytes(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	s.invalidateClients()
	s.factory = func(context.Context, Connection) (Connector, error) { return revisionOnCloseMedia{fixture}, nil }
	response := fetchService(s, ref, "GET", "bytes=0-2")
	require.Equal(t, 412, response.Code)
	require.NotContains(t, response.Body.String(), "ret")
}

type blockedMediaCheck struct{ *fixtureConnector }

func (c blockedMediaCheck) CheckMedia(ctx context.Context, _ Request, _ Item) error {
	<-ctx.Done()
	return ctx.Err()
}
func TestMediaFetchHonorsRequestDeadline(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	s.invalidateClients()
	s.factory = func(context.Context, Connection) (Connector, error) { return blockedMediaCheck{fixture}, nil }
	u, err := url.Parse(ref.URI)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	request := httptest.NewRequest("GET", u.RequestURI(), nil).WithContext(ctx)
	response := httptest.NewRecorder()
	s.ServeHTTP(response, request)
	require.Equal(t, 504, response.Code)
}
func TestPurgeIntentRecoversCrashBeforeProfileRemoval(t *testing.T) {
	s, fixture, _ := serviceFixture(t)
	profile := cloneProfile(s.profile)
	source := profile.Sources[0]
	require.NoError(t, s.queuePurge(source))
	require.NoError(t, s.Close())
	reopened, err := NewService(ServiceOptions{Profile: profile, StateDir: s.directory, PublicURL: s.baseURL, AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: func(context.Context, Connection) (Connector, error) { return fixture, nil }})
	require.NoError(t, err)
	defer reopened.Close()
	require.Empty(t, reopened.profile.Sources)
	queue, err := reopened.readPurges()
	require.NoError(t, err)
	require.Len(t, queue.Sources, 1)
}

func TestMediaConditionalRequestsPrecedeRangeValidation(t *testing.T) {
	s, _, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	parsed, err := url.Parse(ref.URI)
	require.NoError(t, err)
	for _, test := range []struct {
		header, value string
		status        int
	}{
		{"If-Match", `"wrong"`, 412},
		{"If-None-Match", "W/" + ref.Version, 304},
	} {
		request := httptest.NewRequest("GET", parsed.RequestURI(), nil)
		request.Header.Set(test.header, test.value)
		request.Header.Set("Range", "bytes=1000-2000")
		response := httptest.NewRecorder()
		s.ServeHTTP(response, request)
		require.Equal(t, test.status, response.Code)
		require.Empty(t, response.Body.Bytes())
	}
}

func BenchmarkMediaAuthorityLookup(b *testing.B) {
	s, err := NewService(ServiceOptions{Profile: Profile{Version: 1, Endpoint: "https://database.example/db/v1"}, StateDir: b.TempDir(), PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey})
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	inventory := Inventory{Authoritative: true, Objects: make([]Item, 10000)}
	for i := range inventory.Objects {
		inventory.Objects[i] = Item{ID: fmt.Sprintf("object-%d", i), Revision: "revision", Size: 1024}
	}
	source := Source{ID: testSourceID}
	if err := s.publishMediaAuthority(b.Context(), source, nil, &inventory); err != nil {
		b.Fatal(err)
	}
	key := itemMediaKey(inventory.Objects[5000])
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		selected, err := s.mediaAuthorized(source.ID, key)
		if err != nil || !selected {
			b.Fatal(selected, err)
		}
	}
}

func TestMediaMultipartAndIfRangeUseExactRepresentations(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	counted := &countedMedia{fixtureConnector: fixture}
	s.factory = func(context.Context, Connection) (Connector, error) { return counted, nil }
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	counted.bytes.Store(0)
	response := fetchService(s, ref, "GET", "bytes=0-1,11-12")
	require.Equal(t, 206, response.Code)
	require.Contains(t, response.Header().Get("Content-Type"), "multipart/byteranges")
	_, params, err := mime.ParseMediaType(response.Header().Get("Content-Type"))
	require.NoError(t, err)
	reader := multipart.NewReader(response.Body, params["boundary"])
	for _, expected := range []string{"re", "t\n"} {
		part, err := reader.NextPart()
		require.NoError(t, err)
		data, err := io.ReadAll(part)
		require.NoError(t, err)
		require.Equal(t, expected, string(data))
	}
	_, err = reader.NextPart()
	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, 4, counted.bytes.Load())
	uri, err := url.Parse(ref.URI)
	require.NoError(t, err)
	request := httptest.NewRequest("GET", uri.RequestURI(), nil)
	request.Header.Set("Range", "bytes=0-0")
	request.Header.Set("If-Range", `"different"`)
	response = httptest.NewRecorder()
	s.ServeHTTP(response, request)
	require.Equal(t, 200, response.Code)
	require.Equal(t, "retry budget\n", response.Body.String())
}
