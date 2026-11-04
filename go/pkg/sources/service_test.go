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
	"github.com/stretchr/testify/require"
	"io"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const serviceTestToken = "source-service-test-admin-token-32-bytes"

var serviceTestKey = []byte("source-service-test-media-signing-key-32-bytes")

func serviceCall(s *Service, method, path string, body any) *httptest.ResponseRecorder {
	raw, _ := json.Marshal(body)
	r := httptest.NewRequest(method, path, bytes.NewReader(raw))
	r.Header.Set("Authorization", "Bearer "+serviceTestToken)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func serviceFixture(t *testing.T) (*Service, *fixtureConnector, *publicationFixture) {
	t.Helper()
	runner, connector, backend := runnerFixture(t)
	runner.Profile.DataPlaneTokenEnv = "SOURCE_SERVICE_TEST_DB_TOKEN"
	t.Setenv("SOURCE_SERVICE_TEST_DB_TOKEN", "database-test-token")
	runner.Profile.Sources[0].Publication = "media"
	s, err := NewService(ServiceOptions{Profile: runner.Profile, StateDir: t.TempDir(), PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: runner.Factory})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s, connector, backend
}
func waitServiceJob(t *testing.T, s *Service, id string) SyncJob {
	t.Helper()
	var job SyncJob
	require.Eventually(t, func() bool {
		w := serviceCall(s, "GET", "/v1/jobs/"+id, nil)
		if w.Code != 200 {
			return false
		}
		_ = json.Unmarshal(w.Body.Bytes(), &job)
		return job.State != "running"
	}, 5*time.Second, time.Millisecond)
	return job
}
func startServiceJob(t *testing.T, s *Service) SyncJob {
	t.Helper()
	w := serviceCall(s, "POST", "/v1/sync", map[string]any{})
	require.Equal(t, 202, w.Code, w.Body.String())
	var job SyncJob
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &job))
	return waitServiceJob(t, s, job.ID)
}
func mediaFromBackend(t *testing.T, backend *publicationFixture) MediaReference {
	t.Helper()
	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Len(t, backend.records, 1)
	for _, row := range backend.records {
		raw, _ := json.Marshal(row)
		var doc Document
		require.NoError(t, json.Unmarshal(raw, &doc))
		require.NotNil(t, doc.Media)
		return *doc.Media
	}
	panic("unreachable")
}
func fetchService(s *Service, ref MediaReference, method, byteRange string) *httptest.ResponseRecorder {
	u, _ := url.Parse(ref.URI)
	r := httptest.NewRequest(method, u.RequestURI(), nil)
	r.Header.Set("If-Match", ref.Version)
	if byteRange != "" {
		r.Header.Set("Range", byteRange)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}
func TestServiceMediaRangesRevocationAndReconciliation(t *testing.T) {
	s, connector, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	full := fetchService(s, ref, "GET", "")
	require.Equal(t, 200, full.Code, full.Body.String())
	require.Equal(t, "retry budget\n", full.Body.String())
	require.Equal(t, ref.Version, full.Header().Get("ETag"))
	part := fetchService(s, ref, "GET", "bytes=2-5")
	require.Equal(t, 206, part.Code)
	require.Equal(t, "try ", part.Body.String())
	require.Equal(t, "bytes 2-5/13", part.Header().Get("Content-Range"))
	head := fetchService(s, ref, "HEAD", "")
	require.Equal(t, 200, head.Code)
	require.Empty(t, head.Body.String())
	require.Equal(t, "13", head.Header().Get("Content-Length"))
	invalid := ref
	invalid.Version = `"other"`
	require.Equal(t, 412, fetchService(s, invalid, "GET", "").Code)
	require.Equal(t, 416, fetchService(s, ref, "GET", "bytes=100-110").Code)
	connector.inventory.Objects = nil
	require.Equal(t, "completed", startServiceJob(t, s).State)
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	backend.mu.Lock()
	require.Empty(t, backend.records)
	backend.mu.Unlock()
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/docs", nil).Code)
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	require.Equal(t, 400, serviceCall(s, "POST", "/v1/sync", map[string]any{}).Code)
}
func TestServiceRuntimeConnectionSourceLifecycle(t *testing.T) {
	s, _, backend := serviceFixture(t)
	unauth := httptest.NewRecorder()
	s.ServeHTTP(unauth, httptest.NewRequest("GET", "/v1/profile", nil))
	require.Equal(t, 401, unauth.Code)
	require.Equal(t, 400, serviceCall(s, "PUT", "/v1/connections/new", map[string]any{"provider": "s3", "secret_key": "must-not-be-accepted"}).Code)
	connection := Connection{Provider: ProviderS3, AccessKeyEnv: "NEW_ACCESS", SecretKeyEnv: "NEW_SECRET"}
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/new", connection).Code)
	source := s.profile.Sources[0]
	source.Connection = "new"
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+source.ID, source).Code)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	require.Equal(t, 200, fetchService(s, ref, "GET", "").Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/new", nil).Code)
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/new", connection).Code)
	require.Equal(t, 200, fetchService(s, ref, "GET", "").Code)
	changedTransport := connection
	changedTransport.S3Endpoint = "another-store.example"
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/new", changedTransport).Code)
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/connections/new", connection).Code)
	require.Equal(t, 200, fetchService(s, ref, "GET", "").Code)
	source.Permissions = &Permissions{Readers: []string{"another-user"}}
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/sources/"+source.ID, source).Code)
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/sources/"+source.ID, nil).Code)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	var catalog Catalog
	require.NoError(t, loadJSON(filepath.Join(s.runnerPath(), "catalog.json"), &catalog, 32<<20))
	require.Empty(t, catalog.Sources)
}
func TestServiceRestartResumesPinnedMediaAndRecoversJobs(t *testing.T) {
	runner, connector, _ := runnerFixture(t)
	t.Setenv("SOURCE_SERVICE_TEST_DB_TOKEN", "database-test-token")
	runner.Profile.DataPlaneTokenEnv = "SOURCE_SERVICE_TEST_DB_TOKEN"
	runner.Profile.Sources[0].Publication = "media"
	dir := t.TempDir()
	options := ServiceOptions{Profile: runner.Profile, StateDir: dir, PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: runner.Factory}
	s, err := NewService(options)
	require.NoError(t, err)
	ref, err := s.pinMedia(t.Context(), runner.Profile.Sources[0], Request{}, connector.inventory.Objects[0])
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(s.runnerPath(), 0700))
	require.NoError(t, writeJSON(filepath.Join(s.runnerPath(), testSourceID+".json"), runState{Pending: &connector.inventory}, 1<<20))
	require.NoError(t, writeJSON(s.jobPath(), SyncJob{Version: 1, ID: "interrupted-job", State: "running"}, 4096))
	_, err = NewService(options)
	require.Error(t, err, "exclusive service ownership")
	require.NoError(t, s.Close())
	wrongKey := options
	wrongKey.MediaKey = []byte("another-bootstrap-key-with-32-bytes-minimum")
	_, err = NewService(wrongKey)
	require.Error(t, err)
	s, err = NewService(options)
	require.NoError(t, err)
	defer s.Close()
	require.Equal(t, 200, fetchService(s, ref, "GET", "").Code)
	job := waitServiceJob(t, s, "interrupted-job")
	require.Equal(t, "interrupted", job.State)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/connections/docs", nil).Code)
	require.NoError(t, s.Close())
	s, err = NewService(options)
	require.NoError(t, err)
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	require.NoError(t, s.Close())
}

type blockedConnector struct {
	*fixtureConnector
	started chan struct{}
}

func (c *blockedConnector) Enumerate(ctx context.Context, _ Request) (Inventory, error) {
	close(c.started)
	<-ctx.Done()
	return Inventory{}, ctx.Err()
}
func TestServiceCancellationAndSingleWriter(t *testing.T) {
	runner, fixture, _ := runnerFixture(t)
	t.Setenv("SOURCE_SERVICE_TEST_DB_TOKEN", "database-test-token")
	runner.Profile.DataPlaneTokenEnv = "SOURCE_SERVICE_TEST_DB_TOKEN"
	c := &blockedConnector{fixtureConnector: fixture, started: make(chan struct{})}
	options := ServiceOptions{Profile: runner.Profile, StateDir: t.TempDir(), PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: func(context.Context, Connection) (Connector, error) { return c, nil }}
	s, err := NewService(options)
	require.NoError(t, err)
	defer s.Close()
	w := serviceCall(s, "POST", "/v1/sync", map[string]any{})
	require.Equal(t, 202, w.Code)
	var job SyncJob
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &job))
	<-c.started
	require.Equal(t, 409, serviceCall(s, "POST", "/v1/sync", map[string]any{}).Code)
	require.Equal(t, 409, serviceCall(s, "DELETE", "/v1/sources/"+testSourceID, nil).Code)
	require.Equal(t, 202, serviceCall(s, "DELETE", "/v1/jobs/"+job.ID, nil).Code)
	require.Equal(t, "cancelled", waitServiceJob(t, s, job.ID).State)
}

type failedReadConnector struct {
	*fixtureConnector
	err error
}

func (c *failedReadConnector) OpenMedia(context.Context, Request, Item, MediaRange) (io.ReadCloser, error) {
	return nil, c.err
}
func (c *failedReadConnector) Read(context.Context, Request, Item) ([]byte, error) { return nil, c.err }
func TestServiceRejectsTamperedAndChangedMedia(t *testing.T) {
	s, fixture, backend := serviceFixture(t)
	require.Equal(t, "completed", startServiceJob(t, s).State)
	ref := mediaFromBackend(t, backend)
	r := httptest.NewRequest("GET", strings.Replace(ref.URI, "/v1/media/", "/v1/media/../", 1), nil)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	require.Equal(t, 404, w.Code)
	tampered := ref
	tampered.URI = ref.URI[:len(ref.URI)-1] + "x"
	require.Equal(t, 404, fetchService(s, tampered, "GET", "").Code)
	parts := strings.Split(ref.URI, "/")
	id := parts[len(parts)-2]
	raw, err := os.ReadFile(s.pinPath(id))
	require.NoError(t, err)
	var pin mediaPinV2
	require.NoError(t, json.Unmarshal(raw, &pin))
	pin.Item.Path = "another-object"
	require.NoError(t, writeJSON(s.pinPath(id), pin, 1<<20))
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	require.NoError(t, os.WriteFile(s.pinPath(id), raw, 0600))
	s.factory = func(context.Context, Connection) (Connector, error) {
		return &failedReadConnector{fixtureConnector: fixture, err: &RevisionChangedError{Expected: "one", Actual: "two"}}, nil
	}
	require.Equal(t, 412, fetchService(s, ref, "GET", "").Code)
	s.factory = func(context.Context, Connection) (Connector, error) {
		return &failedReadConnector{fixtureConnector: fixture, err: errors.New("private-provider-detail")}, nil
	}
	response := fetchService(s, ref, "GET", "")
	require.Equal(t, 502, response.Code)
	require.NotContains(t, response.Body.String(), "private-provider-detail")
}

func TestServiceRuntimeSecretsAreEncryptedRotatableAndWriteOnly(t *testing.T) {
	options := ServiceOptions{Profile: Profile{Version: 1, Endpoint: "https://database.example/db/v1"}, StateDir: t.TempDir(), PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey}
	s, err := NewService(options)
	require.NoError(t, err)
	defer s.Close()
	const secret = "runtime-private-secret-value"
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/secrets/bucket.access", map[string]any{"value": "access"}).Code)
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/secrets/bucket.secret", map[string]any{"value": secret}).Code)
	value, err := s.resolveSecret(t.Context(), "bucket.secret")
	require.NoError(t, err)
	require.Equal(t, secret, value)
	stored, err := os.ReadFile(s.secretPath("bucket.secret"))
	require.NoError(t, err)
	require.NotContains(t, string(stored), secret)
	require.Equal(t, 405, serviceCall(s, "GET", "/v1/secrets/bucket.secret", nil).Code)
	connection := Connection{Provider: ProviderS3, SecretRefs: map[string]string{"access_key_id": "bucket.access", "secret_access_key": "bucket.secret"}}
	response := serviceCall(s, "PUT", "/v1/connections/bucket", connection)
	require.Equal(t, 200, response.Code, response.Body.String())
	require.NotContains(t, response.Body.String(), secret)
	profile, err := os.ReadFile(s.profilePath())
	require.NoError(t, err)
	require.NotContains(t, string(profile), secret)
	require.Equal(t, 200, serviceCall(s, "PUT", "/v1/secrets/bucket.secret", map[string]any{"value": "rotated"}).Code)
	value, err = s.resolveSecret(t.Context(), "bucket.secret")
	require.NoError(t, err)
	require.Equal(t, "rotated", value)
	// AEAD binds ciphertext to its key name.
	require.NoError(t, os.WriteFile(s.secretPath("other.secret"), stored, 0600))
	_, err = s.resolveSecret(t.Context(), "other.secret")
	require.Error(t, err)
	require.NoError(t, s.Close())
	s, err = NewService(options)
	require.NoError(t, err)
	defer s.Close()
	value, err = s.resolveSecret(t.Context(), "bucket.secret")
	require.NoError(t, err)
	require.Equal(t, "rotated", value)
	require.Equal(t, 200, serviceCall(s, "DELETE", "/v1/secrets/bucket.secret", nil).Code)
	_, err = s.factory(t.Context(), connection)
	require.Error(t, err)
}

func TestCredentialFactoryRejectsAmbiguousReferences(t *testing.T) {
	factory := CredentialFactory(func(context.Context, string) (string, error) { return "secret", nil })
	_, err := factory(t.Context(), Connection{Provider: ProviderS3, AccessKeyEnv: "AWS_ACCESS", SecretRefs: map[string]string{"access_key_id": "key"}})
	require.Error(t, err)
	_, err = factory(t.Context(), Connection{Provider: ProviderS3, SecretRefs: map[string]string{"unknown": "key"}})
	require.Error(t, err)
}
