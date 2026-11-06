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
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofrs/flock"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/require"
)

// migrateFakeBucket is a tiny in-memory S3-compatible bucket: just enough GET,
// HEAD and PUT to exercise MigrateLocalCopy against a real minio.Client over
// real HTTP, without a container runtime.
type migrateFakeBucket struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    int
	failPut map[string]bool
}

func newMigrateFakeBucket(t *testing.T) (*httptest.Server, *migrateFakeBucket) {
	t.Helper()
	bucket := &migrateFakeBucket{objects: map[string][]byte{}, failPut: map[string]bool{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Has("location") {
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`)
			return
		}
		bucket.mu.Lock()
		defer bucket.mu.Unlock()
		if r.URL.Query().Get("list-type") == "2" {
			prefix := r.URL.Query().Get("prefix")
			keys := []string{}
			for k := range bucket.objects {
				if strings.HasPrefix(k, prefix) {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>testbucket</Name><KeyCount>%d</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>`, len(keys))
			for _, k := range keys {
				data := bucket.objects[k]
				var escaped bytes.Buffer
				_ = xml.EscapeText(&escaped, []byte(k))
				fmt.Fprintf(w, `<Contents><Key>%s</Key><LastModified>2026-10-23T00:00:00Z</LastModified><ETag>"%x"</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>`, escaped.String(), md5.Sum(data), len(data))
			}
			fmt.Fprint(w, `</ListBucketResult>`)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/testbucket/")
		switch r.Method {
		case http.MethodPut:
			bucket.puts++
			if bucket.failPut[key] {
				http.Error(w, "injected failure", 500)
				return
			}
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read failed", 400)
				return
			}
			bucket.objects[key] = data
			w.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(data)))
			w.WriteHeader(200)
		case http.MethodHead, http.MethodGet:
			data, ok := bucket.objects[key]
			if !ok {
				http.Header(w.Header()).Set("Content-Type", "application/xml")
				http.Error(w, `<Error><Code>NoSuchKey</Code><Message>not found</Message></Error>`, 404)
				return
			}
			w.Header().Set("ETag", fmt.Sprintf(`"%x"`, md5.Sum(data)))
			w.Header().Set("Content-Length", strconv.Itoa(len(data)))
			w.Header().Set("Last-Modified", time.Now().UTC().Format(http.TimeFormat))
			if r.Method == http.MethodGet {
				_, _ = w.Write(data)
			}
		default:
			http.Error(w, "method not allowed", 405)
		}
	}))
	t.Cleanup(server.Close)
	return server, bucket
}

func migrateFakeS3Connector(t *testing.T, server *httptest.Server) *S3Connector {
	t.Helper()
	target, err := url.Parse(server.URL)
	require.NoError(t, err)
	client, err := minio.New(target.Host, &minio.Options{Creds: credentials.NewStaticV4("access", "secret", ""), Secure: false, Region: "us-east-1"})
	require.NoError(t, err)
	return &S3Connector{Client: client}
}

func migrateLocalFixture(t *testing.T) (root string, local *LocalConnector, req Request) {
	t.Helper()
	root = t.TempDir()
	localPut(t, root, "assets", ".txt", "hello world")
	localPut(t, root, "assets", ".bin", "second object bytes")
	return root, &LocalConnector{Root: root}, localRequest(t, "assets")
}

func TestMigrateLocalCopyUploadsAndVerifiesByDigest(t *testing.T) {
	_, local, req := migrateLocalFixture(t)
	server, bucket := newMigrateFakeBucket(t)
	dest := migrateFakeS3Connector(t, server)

	result, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", false, nil)
	require.NoError(t, err)
	require.Equal(t, 2, result.Copied)
	require.Equal(t, 0, result.Skipped)
	require.Equal(t, 0, result.Failed)
	require.EqualValues(t, len("hello world")+len("second object bytes"), result.BytesCopied)
	for _, obj := range result.Objects {
		require.Equal(t, "copied", obj.Status)
		require.True(t, strings.HasPrefix(obj.DestinationKey, "migrated/objects/sha256/"))
	}
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	require.Len(t, bucket.objects, 2)
}

func TestMigrateLocalCopyDryRunTouchesNothing(t *testing.T) {
	_, local, req := migrateLocalFixture(t)
	server, bucket := newMigrateFakeBucket(t)
	dest := migrateFakeS3Connector(t, server)

	result, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", true, nil)
	require.NoError(t, err)
	require.Equal(t, 2, result.Copied)
	require.Equal(t, 0, result.Failed)
	for _, obj := range result.Objects {
		require.Equal(t, "would_copy", obj.Status)
	}
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	require.Empty(t, bucket.objects, "dry run must not write to the destination")
	require.Zero(t, bucket.puts, "dry run must never issue a PUT")
}

// TestMigrateLocalCopyResumesAfterCrash simulates a crash partway through a
// copy (one object's upload fails) and confirms a rerun uploads only what is
// still missing, leaving the already-copied object untouched.
func TestMigrateLocalCopyResumesAfterCrash(t *testing.T) {
	_, local, req := migrateLocalFixture(t)
	server, bucket := newMigrateFakeBucket(t)
	dest := migrateFakeS3Connector(t, server)

	inventory, err := local.Enumerate(t.Context(), req)
	require.NoError(t, err)
	require.Len(t, inventory.Objects, 2)
	failing, err := migrateDestinationKey("migrated", inventory.Objects[1].Filename)
	require.NoError(t, err)

	bucket.mu.Lock()
	bucket.failPut[failing] = true
	bucket.mu.Unlock()

	first, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", false, nil)
	require.Error(t, err, "a crash partway through must fail the run")
	require.Equal(t, 1, first.Copied)
	require.Equal(t, 1, first.Failed)

	// The process recovers (or the destination comes back); a rerun should
	// now succeed for the object that never landed.
	bucket.mu.Lock()
	delete(bucket.failPut, failing)
	putsAfterFirst := bucket.puts
	bucket.mu.Unlock()

	second, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, second.Failed)
	require.Equal(t, 1, second.Copied, "only the missing object should upload")
	require.Equal(t, 1, second.Skipped, "the object copied before the crash must be recognized, not re-uploaded")

	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	require.Equal(t, putsAfterFirst+1, bucket.puts, "resume must issue exactly one more PUT")
	require.Len(t, bucket.objects, 2)
}

// TestMigrateLocalCopyDetectsDigestMismatchAtDestination plants a wrong
// object at the exact destination key before copying -- same content-
// addressed name and size, different bytes -- and confirms the independent
// readback-and-hash step catches it rather than trusting the existing size
// match.
func TestMigrateLocalCopyDetectsDigestMismatchAtDestination(t *testing.T) {
	_, local, req := migrateLocalFixture(t)
	server, bucket := newMigrateFakeBucket(t)
	dest := migrateFakeS3Connector(t, server)

	inventory, err := local.Enumerate(t.Context(), req)
	require.NoError(t, err)
	var corrupted Item
	for _, item := range inventory.Objects {
		if item.Size == int64(len("hello world")) {
			corrupted = item
		}
	}
	require.NotZero(t, corrupted.Size)
	key, err := migrateDestinationKey("migrated", corrupted.Filename)
	require.NoError(t, err)
	bucket.mu.Lock()
	bucket.objects[key] = []byte("HELLO WORLD") // same size, different digest
	bucket.mu.Unlock()

	result, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", false, nil)
	require.Error(t, err)
	require.Equal(t, 1, result.Failed)
	require.Equal(t, 1, result.Copied, "the uncorrupted object must still copy")
	for _, obj := range result.Objects {
		if obj.DestinationKey == key {
			require.Equal(t, "failed", obj.Status)
			require.NotEmpty(t, obj.Error)
		}
	}
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	require.Equal(t, []byte("HELLO WORLD"), bucket.objects[key], "a failed object must never be silently overwritten")
}

// TestMigrateLocalCopySecondRunIsNoOp confirms a clean rerun uploads nothing
// new, even though it still re-verifies every object's digest.
func TestMigrateLocalCopySecondRunIsNoOp(t *testing.T) {
	_, local, req := migrateLocalFixture(t)
	server, bucket := newMigrateFakeBucket(t)
	dest := migrateFakeS3Connector(t, server)

	first, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", false, nil)
	require.NoError(t, err)
	require.Equal(t, 2, first.Copied)

	bucket.mu.Lock()
	putsAfterFirst := bucket.puts
	snapshot := map[string][]byte{}
	for k, v := range bucket.objects {
		snapshot[k] = append([]byte{}, v...)
	}
	bucket.mu.Unlock()

	second, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", false, nil)
	require.NoError(t, err)
	require.Equal(t, 0, second.Copied)
	require.Equal(t, 2, second.Skipped)
	require.Equal(t, 0, second.Failed)

	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	require.Equal(t, putsAfterFirst, bucket.puts, "a clean rerun must issue no additional PUT")
	require.Equal(t, snapshot, bucket.objects, "a clean rerun must not alter any destination object")
}

// TestMigrateLocalCopySkipsObjectsWrittenAfterEnumeration confirms that an
// object written to the local store after a migration run's enumeration
// snapshot -- as if an application upload landed concurrently with it -- is
// simply left for a later run rather than silently included (impossible once
// the snapshot is taken) or lost (it is still on local disk, untouched, and
// a rerun picks it up).
func TestMigrateLocalCopySkipsObjectsWrittenAfterEnumeration(t *testing.T) {
	root, local, req := migrateLocalFixture(t)
	server, _ := newMigrateFakeBucket(t)
	dest := migrateFakeS3Connector(t, server)

	first, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", false, nil)
	require.NoError(t, err)
	require.Len(t, first.Objects, 2, "only the objects present at this run's enumeration are touched")

	// An upload "arrives during migration": in practice this lands at any
	// point relative to a run's internal enumeration; what matters is that
	// it is never lost and never requires this run to have seen it.
	localPut(t, root, "assets", ".new", "uploaded during migration")

	second, err := MigrateLocalCopy(t.Context(), local, req, dest, "testbucket", "migrated", false, nil)
	require.NoError(t, err)
	require.Len(t, second.Objects, 3, "a later run must pick up the object left behind")
	require.Equal(t, 1, second.Copied)
	require.Equal(t, 2, second.Skipped)
}

func migrateServiceFixture(t *testing.T) (opts MigrateLocalOptions, root, stateDir string, local *LocalConnector, bucket *migrateFakeBucket, backend *publicationFixture) {
	t.Helper()
	root = t.TempDir()
	localPut(t, root, "assets", ".txt", "hello world")
	t.Setenv(LocalRootsEnv, root)

	s3Server, bucket := newMigrateFakeBucket(t)
	target, err := url.Parse(s3Server.URL)
	require.NoError(t, err)

	backend = &publicationFixture{records: map[string]map[string]any{}}
	antfly := httptest.NewServer(backend)
	t.Cleanup(antfly.Close)

	stateDir = t.TempDir()
	t.Setenv("MIGRATE_LOCAL_TEST_ADMIN_TOKEN", "migrate-local-test-admin-token-32-bytes")
	t.Setenv("MIGRATE_LOCAL_TEST_MEDIA_KEY", "migrate-local-test-media-signing-key-32b")
	t.Setenv("MIGRATE_LOCAL_TEST_DB_TOKEN", "database-test-token")
	t.Setenv("MIGRATE_LOCAL_TEST_ACCESS", "bucket-access-key")
	t.Setenv("MIGRATE_LOCAL_TEST_SECRET", "bucket-secret-key")

	profile := Profile{
		Version: 1, Endpoint: antfly.URL + "/api/v1", DataPlaneTokenEnv: "MIGRATE_LOCAL_TEST_DB_TOKEN",
		Principals: []string{"user:self"},
		Connections: map[string]Connection{
			"local":  {Provider: ProviderLocal, LocalRoot: root},
			"bucket": {Provider: ProviderS3, S3Endpoint: target.Host, S3Insecure: true, AccessKeyEnv: "MIGRATE_LOCAL_TEST_ACCESS", SecretKeyEnv: "MIGRATE_LOCAL_TEST_SECRET"},
		},
		Sources: []Source{{
			ID: testSourceID, Alias: "assets", Connection: "local", Table: "sources",
			Config: json.RawMessage(`{"prefix":"assets"}`), Permissions: &Permissions{Public: true},
		}},
	}
	opts = MigrateLocalOptions{
		Profile: profile, StateDir: stateDir, PublicURL: "https://source.example",
		AdminToken: "migrate-local-test-admin-token-32-bytes", MediaKey: []byte("migrate-local-test-media-signing-key-32b"),
		SourceID: testSourceID, FromConnection: "local", FromPrefix: "assets", ToConnection: "bucket", ToBucket: "testbucket", ToPrefix: "migrated",
	}
	return opts, root, stateDir, &LocalConnector{Root: root}, bucket, backend
}

func TestMigrateLocalRepointsCatalogAndServesMediaFromBucket(t *testing.T) {
	opts, _, _, _, bucket, backend := migrateServiceFixture(t)

	result, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err, "result: %+v", result)
	require.True(t, result.CatalogRepointed)
	require.Equal(t, 1, result.Copied)
	require.Equal(t, 1, result.ReferencesUpdated)
	require.NotEmpty(t, result.RunID)

	bucket.mu.Lock()
	require.Len(t, bucket.objects, 1)
	bucket.mu.Unlock()

	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	var doc Document
	for _, row := range backend.records {
		raw, _ := json.Marshal(row)
		require.NoError(t, json.Unmarshal(raw, &doc))
	}
	backend.mu.Unlock()
	require.Equal(t, "media", doc.Kind)
	require.NotNil(t, doc.Media)
	require.Equal(t, ProviderS3, doc.Metadata["provider"])

	// Fetch media through the new reference, served by the companion from
	// the bucket now that the catalog points there.
	service, err := NewService(ServiceOptions{Profile: opts.Profile, StateDir: opts.StateDir, PublicURL: opts.PublicURL, AdminToken: opts.AdminToken, MediaKey: opts.MediaKey})
	require.NoError(t, err)
	defer service.Close()
	u, err := url.Parse(doc.Media.URI)
	require.NoError(t, err)
	r := newLoopbackMediaRequest(t, u, doc.Media.Version)
	w := &controlResponse{header: make(http.Header)}
	service.ServeHTTP(w, r)
	require.Equal(t, 200, w.status, w.body.String())
	require.Equal(t, "hello world", w.body.String())
}

func newLoopbackMediaRequest(t *testing.T, u *url.URL, etag string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(http.MethodGet, u.RequestURI(), nil)
	require.NoError(t, err)
	r.Header.Set("If-Match", etag)
	return r
}

func TestMigrateLocalDryRunChangesNothing(t *testing.T) {
	opts, _, stateDir, _, bucket, backend := migrateServiceFixture(t)
	opts.DryRun = true

	result, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err)
	require.False(t, result.CatalogRepointed)
	require.Equal(t, 1, result.Copied)

	bucket.mu.Lock()
	require.Empty(t, bucket.objects)
	bucket.mu.Unlock()
	backend.mu.Lock()
	require.Empty(t, backend.records)
	backend.mu.Unlock()

	// Opening the service to gain its admission/lock bootstraps an ordinary,
	// unmodified copy of the profile (exactly as "serve" would on first use);
	// what matters is that the migrated source's own entry is untouched.
	raw, err := os.ReadFile(filepath.Join(stateDir, "profile.json"))
	require.NoError(t, err)
	var persisted Profile
	require.NoError(t, json.Unmarshal(raw, &persisted))
	require.Len(t, persisted.Sources, 1)
	require.Equal(t, "local", persisted.Sources[0].Connection, "dry run must not repoint the source")
}

// TestMigrateLocalRefusesWhileServeIsRunning confirms migrate-local will not
// start against a state directory that a live "serve" process already owns,
// rather than racing it.
func TestMigrateLocalRefusesWhileServeIsRunning(t *testing.T) {
	opts, _, stateDir, _, _, _ := migrateServiceFixture(t)

	lock := flock.New(filepath.Join(stateDir, "service.lock"))
	locked, err := lock.TryLock()
	require.NoError(t, err)
	require.True(t, locked)
	defer lock.Unlock()

	_, err = MigrateLocal(t.Context(), opts)
	require.Error(t, err)
}

// TestMigrateLocalSecondRunIsNoOp confirms a second full run neither copies
// anything new nor triggers another repoint sync once the catalog already
// points at the destination.
func TestMigrateLocalSecondRunIsNoOp(t *testing.T) {
	opts, _, _, _, bucket, backend := migrateServiceFixture(t)

	first, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, first.CatalogRepointed)

	bucket.mu.Lock()
	putsAfterFirst := bucket.puts
	bucket.mu.Unlock()
	backend.mu.Lock()
	recordsAfterFirst := len(backend.records)
	backend.mu.Unlock()

	second, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err)
	require.Equal(t, 0, second.Copied)
	require.Equal(t, 1, second.Skipped)
	require.True(t, second.CatalogRepointed)
	require.Empty(t, second.RunID, "an already-repointed source must not trigger another sync")

	bucket.mu.Lock()
	require.Equal(t, putsAfterFirst, bucket.puts)
	bucket.mu.Unlock()
	backend.mu.Lock()
	require.Equal(t, recordsAfterFirst, len(backend.records))
	backend.mu.Unlock()
}

// TestMigrateLocalMopUpAfterCutoverStillRepublishes confirms that running
// migrate-local again after the source already points at the bucket --
// because an object landed in the local store after the cutover -- still
// copies and republishes that straggler, rather than treating "already
// pointed" as a reason to skip it.
func TestMigrateLocalMopUpAfterCutoverStillRepublishes(t *testing.T) {
	opts, root, _, _, _, backend := migrateServiceFixture(t)

	first, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err)
	require.True(t, first.CatalogRepointed)
	backend.mu.Lock()
	recordsAfterFirst := len(backend.records)
	backend.mu.Unlock()

	// A straggler lands in the local store after the cutover.
	localPut(t, root, "assets", ".txt", "a late-arriving upload")

	second, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err)
	require.Equal(t, 1, second.Copied)
	require.True(t, second.CatalogRepointed)
	require.NotEmpty(t, second.RunID, "a mop-up run with new bytes must still resync")

	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Equal(t, recordsAfterFirst+1, len(backend.records), "the straggler must be published")
}
