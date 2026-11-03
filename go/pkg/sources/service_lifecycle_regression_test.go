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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/stretchr/testify/require"
)

func TestPurgeJobsIgnoreUnrelatedRevokedConnections(t *testing.T) {
	for _, purgeOnly := range []bool{false, true} {
		t.Run(fmt.Sprint(purgeOnly), func(t *testing.T) {
			s, _, _ := serviceFixture(t)
			first := s.profile.Sources[0]
			connection := s.profile.Connections[first.Connection]
			require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/other", connection).Code)
			other := first
			other.ID = "22222222-2222-4222-8222-222222222222"
			other.Connection = "other"
			other.Alias = "other"
			require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+other.ID, other).Code)
			require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/other", nil).Code)
			require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+first.ID+"?purge=true", nil).Code)
			response := serviceCall(s, "POST", "/v1/sync", map[string]any{"purge_only": purgeOnly})
			require.Equal(t, 202, response.Code, response.Body.String())
			var job SyncJob
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
			job = waitServiceJob(t, s, job.ID)
			if purgeOnly {
				require.Equal(t, "completed", job.State)
				require.Equal(t, "purge", job.Operation)
			} else {
				require.Equal(t, "failed", job.State)
				require.Equal(t, "sync", job.Operation)
				require.Equal(t, other.ID, job.Failure.SourceID)
			}
			require.Equal(t, 1, job.PurgedSourceCount)
			queue, err := s.readPurges()
			require.NoError(t, err)
			require.Empty(t, queue.Sources)
			require.Len(t, s.profile.Sources, 1)
			require.Equal(t, other.ID, s.profile.Sources[0].ID)
		})
	}
}

func TestImmutableMediaStreamsAndNoOpPinsAreReused(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	item := &fixture.inventory.Objects[0]
	item.Size = 8 << 20
	fixture.data[item.ID] = make([]byte, item.Size)
	counted := &countedMedia{fixtureConnector: fixture}
	s.factory = func(context.Context, Connection) (Connector, error) { return stableCountedMedia{counted}, nil }
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	parts := strings.Split(ref.URI, "/")
	path := s.pinPath(parts[len(parts)-2])
	before, err := os.Stat(path)
	require.NoError(t, err)
	beforeRaw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	after, err := os.Stat(path)
	require.NoError(t, err)
	afterRaw, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, beforeRaw, afterRaw)
	t.Logf("unchanged pin replaced with new inode: %v", !os.SameFile(before, after))
	require.True(t, os.SameFile(before, after))
	counted.opens.Store(0)
	counted.bytes.Store(0)
	response := fetchService(s, ref, "GET", "")
	require.Equal(t, 200, response.Code)
	t.Logf("8 MiB full response provider GETs: %d; bytes: %d", counted.opens.Load(), counted.bytes.Load())
	require.EqualValues(t, 1, counted.opens.Load())
}
func TestGoogleMediaUsesOperationDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "2")
		w.Header().Set("X-Goog-Generation", "1")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "a")
		w.(http.Flusher).Flush()
		select {
		case <-r.Context().Done():
		case <-time.After(100 * time.Millisecond):
		}
		_, _ = io.WriteString(w, "b")
	}))
	defer server.Close()
	client := server.Client()
	client.Timeout = 50 * time.Millisecond
	connector := &GoogleConnector{ProviderName: ProviderGCS, HTTPClient: client, APIBase: server.URL}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	body, err := connector.OpenMedia(ctx, Request{Config: json.RawMessage(`{"bucket":"docs"}`)}, Item{Path: "file", Revision: "1", Size: 2, Metadata: map[string]any{"bucket": "docs"}}, MediaRange{0, 2})
	require.NoError(t, err)
	_, err = io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.NoError(t, ctx.Err())
	short, stop := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer stop()
	body, err = connector.OpenMedia(short, Request{Config: json.RawMessage(`{"bucket":"docs"}`)}, Item{Path: "file", Revision: "1", Size: 2, Metadata: map[string]any{"bucket": "docs"}}, MediaRange{0, 2})
	require.NoError(t, err)
	_, err = io.ReadAll(body)
	_ = body.Close()
	require.Error(t, err)
	require.ErrorIs(t, short.Err(), context.DeadlineExceeded)
}
func TestUnrelatedRegistrationPreservesInFlightFetch(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	entered, release := make(chan struct{}), make(chan struct{})
	wrapped := &authorizationGateMedia{fixtureConnector: fixture, entered: entered, release: release}
	s.factory = func(context.Context, Connection) (Connector, error) { return wrapped, nil }
	s.invalidateClients()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- fetchService(s, ref, "HEAD", "") }()
	<-entered
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/unrelated", s.profile.Connections["docs"]).Code)
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/secrets/unrelated", map[string]any{"value": "new-value"}).Code)
	source := s.profile.Sources[0]
	source.Alias = "renamed"
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+source.ID, source).Code)
	close(release)
	response := <-done
	t.Logf("existing media HEAD after unrelated connection registration: HTTP %d %s", response.Code, response.Body.String())
	require.Equal(t, 200, response.Code)
}

type authorizationGateMedia struct {
	*fixtureConnector
	entered, release chan struct{}
}

func (c *authorizationGateMedia) CheckMedia(context.Context, Request, Item) error {
	close(c.entered)
	<-c.release
	return nil
}

func TestPurgeHonorsRunnerLock(t *testing.T) {
	s, _, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	source := s.profile.Sources[0]
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+source.ID+"?purge=true", nil).Code)
	require.NoError(t, os.MkdirAll(s.runnerPath(), 0700))
	lock := flock.New(s.runnerPath() + "/runner.lock")
	locked, err := lock.TryLock()
	require.NoError(t, err)
	require.True(t, locked)
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, s.purgeSources(ctx, cloneProfile(s.profile)), context.DeadlineExceeded)
	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	backend.mu.Unlock()
	response := serviceCall(s, "POST", "/v1/sync", map[string]any{"purge_only": true})
	require.Equal(t, 202, response.Code)
	var job SyncJob
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/docs", nil).Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/secrets/revoked", nil).Code)
	// Admission is asynchronous while a separate writer owns the checkpoint.
	status := serviceCall(s, "GET", "/v1/jobs/"+job.ID, nil)
	var waiting SyncJob
	require.NoError(t, json.Unmarshal(status.Body.Bytes(), &waiting))
	require.Equal(t, "running", waiting.State)
	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	backend.mu.Unlock()
	require.NoError(t, lock.Unlock())
	job = waitServiceJob(t, s, job.ID)
	require.Equal(t, "completed", job.State)
	require.Equal(t, 1, job.PurgedSourceCount)
}

func TestDriveResumeRejectsChangedRevisionWithCachedDigest(t *testing.T) {
	runner, _, backend := runnerFixture(t)
	runner.Profile.DataPlaneTokenEnv = "SOURCE_SERVICE_TEST_DB_TOKEN"
	t.Setenv("SOURCE_SERVICE_TEST_DB_TOKEN", "database-test-token")
	runner.Profile.Connections["docs"] = Connection{Provider: ProviderGoogleWorkspace}
	runner.Profile.Sources[0].Publication = "media"
	runner.Profile.Sources[0].Config = json.RawMessage(`{"resource":"drive","folder_id":"folder"}`)
	version := "7"
	checks := 0
	changeAfterReceiptCheck := false
	client := withDriveScopes(&http.Client{Transport: collaborationTransport(func(r *http.Request) (*http.Response, error) {
		var payload string
		switch {
		case r.URL.Path == "/files" && r.URL.Query().Has("q"):
			payload = fmt.Sprintf(`{"files":[{"id":"photo","name":"photo.jpg","mimeType":"image/jpeg","size":"2","version":%q}]}`, version)
		case r.URL.Path == "/files/folder":
			payload = `{"id":"folder","mimeType":"application/vnd.google-apps.folder"}`
		case r.URL.Path == "/files/photo" && r.URL.Query().Get("alt") == "media":
			payload = "ab"
		case r.URL.Path == "/files/photo":
			current := version
			checks++
			if changeAfterReceiptCheck && checks == 2 {
				version = "8"
			}
			payload = `{"id":"photo","version":"` + current + `","capabilities":{"canDownload":true}}`
		default:
			t.Errorf("unexpected path %s", r.URL)
			payload = `{}`
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), ContentLength: int64(len(payload)), Body: io.NopCloser(strings.NewReader(payload)), Request: r}, nil
	})}, "https://www.googleapis.com/auth/drive.readonly")
	factory := func(context.Context, Connection) (Connector, error) {
		return &CollaborationConnector{ProviderName: ProviderGoogleWorkspace, HTTPClient: client, DriveBase: "https://fixture.test"}, nil
	}
	s, err := NewService(ServiceOptions{Profile: runner.Profile, StateDir: t.TempDir(), PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: factory})
	require.NoError(t, err)
	defer s.Close()
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	path := s.runnerPath() + "/" + testSourceID + ".json"
	var state runState
	require.NoError(t, loadJSON(path, &state, 256<<20))
	state.Pending = state.Published // Resume a pinned selection after interruption.
	require.NoError(t, writeJSON(path, state, 256<<20))
	checks = 0
	changeAfterReceiptCheck = true
	job := startServiceJob(t, s)
	response := fetchService(s, ref, "GET", "")
	t.Logf("Drive resume at provider revision 8: job=%s; published revision-7 URL fetch=HTTP %d", job.State, response.Code)
	require.Equal(t, "failed", job.State)
	require.Equal(t, "source_version_changed", job.Failure.Code)
	require.Equal(t, 412, response.Code)
	// A cache hit outside Runner also validates the live pinned revision.
	_, err = s.pinMedia(t.Context(), s.profile.Sources[0], Request{SourceNamespace: "job/" + testSourceID, Mode: ImportModeSource, Config: s.profile.Sources[0].Config}, state.Pending.Objects[0])
	var changed *RevisionChangedError
	require.ErrorAs(t, err, &changed)
	changeAfterReceiptCheck = false
	resumed := startServiceJob(t, s)
	require.Equal(t, "failed", resumed.State)
	require.Equal(t, "source_version_changed", resumed.Failure.Code)
	restarted := serviceCall(s, "POST", "/v1/sync", map[string]any{"restart": true})
	require.Equal(t, 202, restarted.Code)
	var next SyncJob
	require.NoError(t, json.Unmarshal(restarted.Body.Bytes(), &next))
	require.Equal(t, "completed", waitServiceJob(t, s, next.ID).State)
	fresh := mediaFromBackend(t, backend)
	require.Equal(t, 200, fetchService(s, fresh, "GET", "").Code)
}

type stableCountedMedia struct{ *countedMedia }

func (stableCountedMedia) StableMediaRanges() bool { return true }

func TestSecretGenerationRetiresOnlyReferencingClients(t *testing.T) {
	s, fixture, _ := serviceFixture(t)
	var builds atomic.Int64
	s.factory = func(context.Context, Connection) (Connector, error) { builds.Add(1); return fixture, nil }
	first := Connection{Provider: ProviderS3, SecretRefs: map[string]string{"access_key_id": "first"}}
	second := Connection{Provider: ProviderS3, SecretRefs: map[string]string{"access_key_id": "second"}}
	_, err := s.providerConnector(t.Context(), first)
	require.NoError(t, err)
	_, err = s.providerConnector(t.Context(), second)
	require.NoError(t, err)
	beforeFirst, beforeSecond := s.clientGeneration(first), s.clientGeneration(second)
	require.NoError(t, s.putSecret("first", "rotated"))
	require.NotEqual(t, beforeFirst, s.clientGeneration(first))
	require.Equal(t, beforeSecond, s.clientGeneration(second))
	_, err = s.providerConnector(t.Context(), second)
	require.NoError(t, err)
	require.EqualValues(t, 2, builds.Load())
	_, err = s.providerConnector(t.Context(), first)
	require.NoError(t, err)
	require.EqualValues(t, 3, builds.Load())
}

func TestReferencingSecretRotationFencesAuthorization(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	connection := s.profile.Connections["docs"]
	connection.SecretRefs = map[string]string{"access_key_id": "bucket.access"}
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/docs", connection).Code)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	entered, release := make(chan struct{}), make(chan struct{})
	s.factory = func(context.Context, Connection) (Connector, error) {
		return &authorizationGateMedia{fixture, entered, release}, nil
	}
	s.invalidateClients()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- fetchService(s, ref, "HEAD", "") }()
	<-entered
	require.NoError(t, s.putSecret("bucket.access", "rotated"))
	close(release)
	require.Equal(t, 410, (<-done).Code)
}

func TestS3ContiguousMediaKeepsOneProviderResponse(t *testing.T) {
	data := make([]byte, 8<<20)
	var gets atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<LocationConstraint>us-east-1</LocationConstraint>`)
			return
		}
		gets.Add(1)
		w.Header().Set("ETag", `"revision-1"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "manual", time.Unix(1, 0), bytes.NewReader(data))
	}))
	defer origin.Close()
	s, _, _ := serviceFixture(t)
	t.Setenv("SOURCE_STREAM_ACCESS", "access")
	t.Setenv("SOURCE_STREAM_SECRET", "secret")
	s.factory = EnvironmentFactory
	connection := Connection{Provider: ProviderS3, S3Endpoint: strings.TrimPrefix(origin.URL, "http://"), S3Insecure: true, AccessKeyEnv: "SOURCE_STREAM_ACCESS", SecretKeyEnv: "SOURCE_STREAM_SECRET"}
	connector, err := s.providerConnector(t.Context(), connection)
	require.NoError(t, err)
	item := Item{Path: "manual", Revision: "revision-1", Size: int64(len(data)), Metadata: map[string]any{"bucket": "docs"}}
	request := Request{Config: json.RawMessage(`{"bucket":"docs"}`), Mode: ImportModeSource}
	for _, span := range []MediaRange{{0, item.Size}, {100, item.Size - 200}} {
		t.Run(strconv.FormatInt(span.Offset, 10), func(t *testing.T) {
			gets.Store(0)
			reader := &mediaSeeker{ctx: t.Context(), connector: connector.(MediaConnector), request: request, item: item, ranges: []MediaRange{span}, offset: span.Offset}
			defer reader.Close()
			n, err := io.CopyN(io.Discard, reader, span.Length)
			require.NoError(t, err)
			require.Equal(t, span.Length, n)
			require.NoError(t, reader.Close())
			require.EqualValues(t, 1, gets.Load())
		})
	}
}

type deniedResolverAccess struct {
	*fixtureConnector
	err error
}

func (c deniedResolverAccess) CheckAccess(context.Context, Request, Item) error { return c.err }
func TestCustomMediaResolverRetainsRunnerAccessChecks(t *testing.T) {
	runner, fixture, _ := runnerFixture(t)
	runner.Profile.Sources[0].Publication = "media"
	denied := errors.New("provider access denied")
	runner.Factory = func(context.Context, Connection) (Connector, error) {
		return deniedResolverAccess{fixture, denied}, nil
	}
	called := false
	runner.MediaReference = func(context.Context, Source, Request, Item) (MediaReference, error) {
		called = true
		return MediaReference{}, nil
	}
	require.ErrorIs(t, runner.Sync(t.Context()), denied)
	require.False(t, called)
}

func TestServiceCustomConnectorRetainsAccessPolicy(t *testing.T) {
	s, fixture, _ := serviceFixture(t)
	denied := errors.New("injected connector policy denies access")
	s.factory = func(context.Context, Connection) (Connector, error) {
		return deniedResolverAccess{fixture, denied}, nil
	}
	job := startServiceJob(t, s)
	require.Equal(t, "failed", job.State)
	require.Equal(t, "verify_media", job.Failure.Phase)
}

func TestMediaGatewayRetainsInjectedAccessPolicy(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	denied := errors.New("injected connector policy denies access")
	s.factory = func(context.Context, Connection) (Connector, error) {
		return deniedResolverAccess{fixture, denied}, nil
	}
	response := fetchService(s, ref, "GET", "")
	require.Equal(t, 502, response.Code)
	require.NotContains(t, response.Body.String(), "retry budget")
	require.NotContains(t, response.Body.String(), denied.Error())
}
