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
	"crypto/md5"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
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

// migrateFakeGCSBucket is a tiny in-memory GCS JSON-API bucket: just enough
// of "GET .../o/{key}" (stat), "GET ...?alt=media" (read) and the multipart
// upload ("POST .../o?uploadType=multipart&ifGenerationMatch=0", create) to
// exercise gcsDestination against a real net/http client, without a real
// Google credential or network call.
type migrateFakeGCSBucket struct {
	mu          sync.Mutex
	objects     map[string][]byte
	generations map[string]int64
	nextGen     int64
	puts        int
	failPut     map[string]bool
}

func gcsFakeObjectKey(r *http.Request) (string, bool) {
	escaped := r.URL.EscapedPath()
	idx := strings.Index(escaped, "/o/")
	if idx < 0 {
		return "", false
	}
	key, err := url.PathUnescape(escaped[idx+3:])
	if err != nil {
		return "", false
	}
	return key, true
}

func newMigrateFakeGCSBucket(t *testing.T) (*httptest.Server, *migrateFakeGCSBucket) {
	t.Helper()
	bucket := &migrateFakeGCSBucket{objects: map[string][]byte{}, generations: map[string]int64{}, failPut: map[string]bool{}}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bucket.mu.Lock()
		defer bucket.mu.Unlock()
		if r.Method == http.MethodPost {
			bucket.puts++
			_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if err != nil {
				http.Error(w, "bad content type", 400)
				return
			}
			parts := multipart.NewReader(r.Body, params["boundary"])
			metaPart, err := parts.NextPart()
			if err != nil {
				http.Error(w, "missing metadata part", 400)
				return
			}
			metaRaw, err := io.ReadAll(metaPart)
			if err != nil {
				http.Error(w, "bad metadata part", 400)
				return
			}
			var meta struct {
				Name string `json:"name"`
			}
			if err := json.Unmarshal(metaRaw, &meta); err != nil || meta.Name == "" {
				http.Error(w, "bad metadata json", 400)
				return
			}
			mediaPart, err := parts.NextPart()
			if err != nil {
				http.Error(w, "missing media part", 400)
				return
			}
			data, err := io.ReadAll(mediaPart)
			if err != nil {
				http.Error(w, "bad media part", 400)
				return
			}
			if bucket.failPut[meta.Name] {
				http.Error(w, "injected failure", 500)
				return
			}
			if r.URL.Query().Get("ifGenerationMatch") == "0" {
				if _, exists := bucket.objects[meta.Name]; exists {
					w.WriteHeader(412)
					return
				}
			}
			bucket.objects[meta.Name] = data
			bucket.nextGen++
			bucket.generations[meta.Name] = bucket.nextGen
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"name":%q,"size":"%d","generation":"%d"}`, meta.Name, len(data), bucket.nextGen)
			return
		}
		if r.Method == http.MethodGet {
			// The listing endpoint ("GET .../o?prefix=...") has no object
			// key segment; gcsFakeObjectKey only matches "GET .../o/{key}".
			if _, ok := gcsFakeObjectKey(r); !ok {
				prefix := r.URL.Query().Get("prefix")
				keys := make([]string, 0, len(bucket.objects))
				for k := range bucket.objects {
					if strings.HasPrefix(k, prefix) {
						keys = append(keys, k)
					}
				}
				sort.Strings(keys)
				w.Header().Set("Content-Type", "application/json")
				items := make([]string, 0, len(keys))
				for _, k := range keys {
					items = append(items, fmt.Sprintf(`{"name":%q,"generation":"%d","size":"%d","contentType":"application/octet-stream"}`, k, bucket.generations[k], len(bucket.objects[k])))
				}
				fmt.Fprintf(w, `{"items":[%s]}`, strings.Join(items, ","))
				return
			}
			key, _ := gcsFakeObjectKey(r)
			data, exists := bucket.objects[key]
			if !exists {
				http.Error(w, `{"error":{"errors":[{"reason":"notFound"}]}}`, 404)
				return
			}
			if r.URL.Query().Get("alt") == "media" {
				// GoogleConnector.OpenMedia requires this header to match
				// the enumerated generation for GCS reads.
				w.Header().Set("X-Goog-Generation", fmt.Sprintf("%d", bucket.generations[key]))
				_, _ = w.Write(data)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"name":%q,"size":"%d","generation":"%d"}`, key, len(data), bucket.generations[key])
			return
		}
		http.Error(w, "not found", 404)
	}))
	t.Cleanup(server.Close)
	return server, bucket
}

// migrateTestDestination is the common surface both fake buckets expose, so
// every MigrateLocalCopy scenario test runs against S3 and GCS without
// duplicating itself.
type migrateTestDestination struct {
	kind    string
	dest    migrateDestination
	objects func() map[string][]byte
	puts    func() int
	setFail func(key string, fail bool)
	seed    func(key string, data []byte)
}

var migrateTestDestinationKinds = []string{"s3", "gcs"}

func newMigrateTestDestination(t *testing.T, kind string) migrateTestDestination {
	t.Helper()
	switch kind {
	case "s3":
		server, bucket := newMigrateFakeBucket(t)
		connector := migrateFakeS3Connector(t, server)
		dest := &s3Destination{client: connector.Client, bucket: "testbucket"}
		return migrateTestDestination{
			kind: "s3", dest: dest,
			objects: func() map[string][]byte {
				bucket.mu.Lock()
				defer bucket.mu.Unlock()
				return cloneByteMap(bucket.objects)
			},
			puts: func() int { bucket.mu.Lock(); defer bucket.mu.Unlock(); return bucket.puts },
			setFail: func(key string, fail bool) {
				bucket.mu.Lock()
				defer bucket.mu.Unlock()
				if fail {
					bucket.failPut[key] = true
				} else {
					delete(bucket.failPut, key)
				}
			},
			seed: func(key string, data []byte) { bucket.mu.Lock(); defer bucket.mu.Unlock(); bucket.objects[key] = data },
		}
	case "gcs":
		server, bucket := newMigrateFakeGCSBucket(t)
		dest := &gcsDestination{client: server.Client(), bucket: "testbucket", apiBase: server.URL, uploadBase: server.URL}
		return migrateTestDestination{
			kind: "gcs", dest: dest,
			objects: func() map[string][]byte {
				bucket.mu.Lock()
				defer bucket.mu.Unlock()
				return cloneByteMap(bucket.objects)
			},
			puts: func() int { bucket.mu.Lock(); defer bucket.mu.Unlock(); return bucket.puts },
			setFail: func(key string, fail bool) {
				bucket.mu.Lock()
				defer bucket.mu.Unlock()
				if fail {
					bucket.failPut[key] = true
				} else {
					delete(bucket.failPut, key)
				}
			},
			seed: func(key string, data []byte) { bucket.mu.Lock(); defer bucket.mu.Unlock(); bucket.objects[key] = data },
		}
	default:
		t.Fatalf("unknown migrate test destination kind %q", kind)
		return migrateTestDestination{}
	}
}

func cloneByteMap(m map[string][]byte) map[string][]byte {
	out := make(map[string][]byte, len(m))
	for k, v := range m {
		out[k] = append([]byte{}, v...)
	}
	return out
}

func migrateLocalFixture(t *testing.T) (root string, local *LocalConnector, req Request) {
	t.Helper()
	root = t.TempDir()
	localPut(t, root, "assets", ".txt", "hello world")
	localPut(t, root, "assets", ".bin", "second object bytes")
	return root, &LocalConnector{Root: root}, localRequest(t, "assets")
}

func TestMigrateLocalCopyUploadsAndVerifiesByDigest(t *testing.T) {
	for _, kind := range migrateTestDestinationKinds {
		t.Run(kind, func(t *testing.T) {
			_, local, req := migrateLocalFixture(t)
			td := newMigrateTestDestination(t, kind)

			result, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", false, nil)
			require.NoError(t, err)
			require.Equal(t, 2, result.Copied)
			require.Equal(t, 0, result.Skipped)
			require.Equal(t, 0, result.Failed)
			require.EqualValues(t, len("hello world")+len("second object bytes"), result.BytesCopied)
			for _, obj := range result.Objects {
				require.Equal(t, "copied", obj.Status)
				require.True(t, strings.HasPrefix(obj.DestinationKey, "migrated/objects/sha256/"))
			}
			require.Len(t, td.objects(), 2)
		})
	}
}

func TestMigrateLocalCopyDryRunTouchesNothing(t *testing.T) {
	for _, kind := range migrateTestDestinationKinds {
		t.Run(kind, func(t *testing.T) {
			_, local, req := migrateLocalFixture(t)
			td := newMigrateTestDestination(t, kind)

			result, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", true, nil)
			require.NoError(t, err)
			require.Equal(t, 2, result.Copied)
			require.Equal(t, 0, result.Failed)
			for _, obj := range result.Objects {
				require.Equal(t, "would_copy", obj.Status)
			}
			require.Empty(t, td.objects(), "dry run must not write to the destination")
			require.Zero(t, td.puts(), "dry run must never issue a create")
		})
	}
}

// TestMigrateLocalCopyResumesAfterCrash simulates a crash partway through a
// copy (one object's upload fails) and confirms a rerun uploads only what is
// still missing, leaving the already-copied object untouched.
func TestMigrateLocalCopyResumesAfterCrash(t *testing.T) {
	for _, kind := range migrateTestDestinationKinds {
		t.Run(kind, func(t *testing.T) {
			_, local, req := migrateLocalFixture(t)
			td := newMigrateTestDestination(t, kind)

			inventory, err := local.Enumerate(t.Context(), req)
			require.NoError(t, err)
			require.Len(t, inventory.Objects, 2)
			failing, err := migrateDestinationKey("migrated", inventory.Objects[1].Filename)
			require.NoError(t, err)
			td.setFail(failing, true)

			first, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", false, nil)
			require.Error(t, err, "a crash partway through must fail the run")
			require.Equal(t, 1, first.Copied)
			require.Equal(t, 1, first.Failed)

			// The process recovers (or the destination comes back); a rerun
			// should now succeed for the object that never landed.
			td.setFail(failing, false)
			putsAfterFirst := td.puts()

			second, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", false, nil)
			require.NoError(t, err)
			require.Equal(t, 0, second.Failed)
			require.Equal(t, 1, second.Copied, "only the missing object should upload")
			require.Equal(t, 1, second.Skipped, "the object copied before the crash must be recognized, not re-uploaded")

			require.Equal(t, putsAfterFirst+1, td.puts(), "resume must issue exactly one more create")
			require.Len(t, td.objects(), 2)
		})
	}
}

// TestMigrateLocalCopyDetectsDigestMismatchAtDestination plants a wrong
// object at the exact destination key before copying -- same content-
// addressed name and size, different bytes -- and confirms the independent
// readback-and-hash step catches it rather than trusting the existing size
// match.
func TestMigrateLocalCopyDetectsDigestMismatchAtDestination(t *testing.T) {
	for _, kind := range migrateTestDestinationKinds {
		t.Run(kind, func(t *testing.T) {
			_, local, req := migrateLocalFixture(t)
			td := newMigrateTestDestination(t, kind)

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
			td.seed(key, []byte("HELLO WORLD")) // same size, different digest

			result, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", false, nil)
			require.Error(t, err)
			require.Equal(t, 1, result.Failed)
			require.Equal(t, 1, result.Copied, "the uncorrupted object must still copy")
			for _, obj := range result.Objects {
				if obj.DestinationKey == key {
					require.Equal(t, "failed", obj.Status)
					require.NotEmpty(t, obj.Error)
				}
			}
			require.Equal(t, []byte("HELLO WORLD"), td.objects()[key], "a failed object must never be silently overwritten")
		})
	}
}

// TestMigrateLocalCopySecondRunIsNoOp confirms a clean rerun uploads nothing
// new, even though it still re-verifies every object's digest.
func TestMigrateLocalCopySecondRunIsNoOp(t *testing.T) {
	for _, kind := range migrateTestDestinationKinds {
		t.Run(kind, func(t *testing.T) {
			_, local, req := migrateLocalFixture(t)
			td := newMigrateTestDestination(t, kind)

			first, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", false, nil)
			require.NoError(t, err)
			require.Equal(t, 2, first.Copied)

			putsAfterFirst := td.puts()
			snapshot := td.objects()

			second, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", false, nil)
			require.NoError(t, err)
			require.Equal(t, 0, second.Copied)
			require.Equal(t, 2, second.Skipped)
			require.Equal(t, 0, second.Failed)

			require.Equal(t, putsAfterFirst, td.puts(), "a clean rerun must issue no additional create")
			require.Equal(t, snapshot, td.objects(), "a clean rerun must not alter any destination object")
		})
	}
}

// TestMigrateLocalCopySkipsObjectsWrittenAfterEnumeration confirms that an
// object written to the local store after a migration run's enumeration
// snapshot -- as if an application upload landed concurrently with it -- is
// simply left for a later run rather than silently included (impossible once
// the snapshot is taken) or lost (it is still on local disk, untouched, and
// a rerun picks it up).
func TestMigrateLocalCopySkipsObjectsWrittenAfterEnumeration(t *testing.T) {
	for _, kind := range migrateTestDestinationKinds {
		t.Run(kind, func(t *testing.T) {
			root, local, req := migrateLocalFixture(t)
			td := newMigrateTestDestination(t, kind)

			first, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", false, nil)
			require.NoError(t, err)
			require.Len(t, first.Objects, 2, "only the objects present at this run's enumeration are touched")

			// An upload "arrives during migration": in practice this lands at
			// any point relative to a run's internal enumeration; what
			// matters is that it is never lost and never requires this run
			// to have seen it.
			localPut(t, root, "assets", ".new", "uploaded during migration")

			second, err := MigrateLocalCopy(t.Context(), local, req, td.dest, "migrated", false, nil)
			require.NoError(t, err)
			require.Len(t, second.Objects, 3, "a later run must pick up the object left behind")
			require.Equal(t, 1, second.Copied)
			require.Equal(t, 2, second.Skipped)
		})
	}
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
		AdminToken: serviceTestToken, MediaKey: serviceTestKey,
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

// TestMigrateLocalWithNoNamespaceKeepsRowIdentity exercises the corrected
// promise through the real Runner/Publisher pipeline: a source synced
// before identity.namespace existed (this fixture's, exactly like a real
// pre-upgrade deployment) is still on the legacy, location-based key
// formula, but migrate-local's own provider change does NOT re-key it --
// it adopts exactly the namespace that reproduces this source's current
// keys (LegacyNamespace), so the row's key is identical before and after,
// even though this is also the first time the source gets a namespace. The
// PUT response and IdentityNotice both confirm no re-key happened.
func TestMigrateLocalWithNoNamespaceKeepsRowIdentity(t *testing.T) {
	opts, _, stateDir, _, _, backend := migrateServiceFixture(t)

	// Establish a baseline row while still on the local connection, exactly
	// as if this source had been running before this feature existed.
	pre, err := NewService(ServiceOptions{Profile: opts.Profile, StateDir: stateDir, PublicURL: opts.PublicURL, AdminToken: opts.AdminToken, MediaKey: opts.MediaKey})
	require.NoError(t, err)
	require.Equal(t, "completed", startServiceJob(t, pre).State)
	require.NoError(t, pre.Close())

	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	var oldKey string
	for key := range backend.records {
		oldKey = key
	}
	backend.mu.Unlock()
	require.NotEmpty(t, oldKey)

	result, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err, "result: %+v", result)
	require.True(t, result.CatalogRepointed)
	require.False(t, result.Rekeyed, "adopting the exact legacy prefix must not re-key anything")
	notice := result.IdentityNotice()
	require.Contains(t, notice, "updated in place with the same IDs")
	require.Contains(t, notice, "checksum_algorithm", "the notice must say what migrate-local cannot confirm about the destination table")
	require.NotContains(t, notice, "re-keyed")

	backend.mu.Lock()
	defer backend.mu.Unlock()
	require.Len(t, backend.records, 1, "the provider change must update the row in place, not add a second one")
	_, oldKeyStillThere := backend.records[oldKey]
	require.True(t, oldKeyStillThere, "the row's key must be unchanged by this provider change")
}

// TestMigrateLocalIdentityNoticeStatesTheChecksumAlgorithmRequirement pins
// the notice text itself: row identity surviving a provider change (the
// ordinary, non-Rekeyed case) is never the whole story, since migrate-local
// has no visibility into whether the destination table's document_extraction
// config declares checksum_algorithm -- without it, Antfly still recomputes
// derived data for a row whose location changed, even though migrate-local
// kept its ID. The notice must say so plainly, every time, rather than
// imply "rows kept their IDs" means "nothing will be recomputed."
func TestMigrateLocalIdentityNoticeStatesTheChecksumAlgorithmRequirement(t *testing.T) {
	keptIdentity := MigrateLocalResult{CatalogRepointed: true, Rekeyed: false}
	notice := keptIdentity.IdentityNotice()
	require.Contains(t, notice, "updated in place with the same IDs")
	require.Contains(t, notice, "checksum_algorithm")
	require.Contains(t, notice, "cannot see that table-side config")
	require.Contains(t, notice, "still recomputes derived data")

	rekeyed := MigrateLocalResult{CatalogRepointed: true, Rekeyed: true}
	require.Contains(t, rekeyed.IdentityNotice(), "re-keyed")

	dryRun := MigrateLocalResult{DryRun: true, CatalogRepointed: false}
	require.Empty(t, dryRun.IdentityNotice())

	notRepointed := MigrateLocalResult{CatalogRepointed: false}
	require.Empty(t, notRepointed.IdentityNotice())
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

// migrateLiveFixture builds a real *Service (the same ingredients
// migrateServiceFixture uses) and wraps it in a real httptest server, so
// live-mode tests drive it over genuine HTTP exactly like a running "serve"
// process, with no help from migrate-local holding its lock.
func migrateLiveFixture(t *testing.T) (opts MigrateLocalOptions, service *Service, root string, bucket *migrateFakeBucket, backend *publicationFixture) {
	t.Helper()
	root = t.TempDir()
	localPut(t, root, "assets", ".bin", "hello world")
	t.Setenv(LocalRootsEnv, root)

	s3Server, bucket := newMigrateFakeBucket(t)
	target, err := url.Parse(s3Server.URL)
	require.NoError(t, err)

	backend = &publicationFixture{records: map[string]map[string]any{}}
	antfly := httptest.NewServer(backend)
	t.Cleanup(antfly.Close)

	t.Setenv("MIGRATE_LOCAL_LIVE_DB_TOKEN", "database-test-token")
	t.Setenv("MIGRATE_LOCAL_LIVE_ACCESS", "bucket-access-key")
	t.Setenv("MIGRATE_LOCAL_LIVE_SECRET", "bucket-secret-key")

	profile := Profile{
		Version: 1, Endpoint: antfly.URL + "/api/v1", DataPlaneTokenEnv: "MIGRATE_LOCAL_LIVE_DB_TOKEN",
		Principals: []string{"user:self"},
		Connections: map[string]Connection{
			"local":  {Provider: ProviderLocal, LocalRoot: root},
			"bucket": {Provider: ProviderS3, S3Endpoint: target.Host, S3Insecure: true, AccessKeyEnv: "MIGRATE_LOCAL_LIVE_ACCESS", SecretKeyEnv: "MIGRATE_LOCAL_LIVE_SECRET"},
		},
		Sources: []Source{{
			ID: testSourceID, Alias: "assets", Connection: "local", Table: "sources",
			Config: json.RawMessage(`{"prefix":"assets"}`), Permissions: &Permissions{Public: true},
		}},
	}

	service, err = NewService(ServiceOptions{Profile: profile, StateDir: t.TempDir(), PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	liveServer := httptest.NewServer(service)
	t.Cleanup(liveServer.Close)

	opts = MigrateLocalOptions{
		Server: liveServer.URL, AdminToken: serviceTestToken,
		SourceID: testSourceID, FromConnection: "local", FromPrefix: "assets", ToConnection: "bucket", ToBucket: "testbucket", ToPrefix: "migrated",
	}
	return opts, service, root, bucket, backend
}

// TestMigrateLocalLiveRepointsWithoutServiceLock confirms the whole point of
// --server: migrate-local never needs the live service's own file lock, even
// though that lock really is held for this test's whole duration.
func TestMigrateLocalLiveRepointsWithoutServiceLock(t *testing.T) {
	opts, service, _, bucket, backend := migrateLiveFixture(t)

	// Sanity check: the live service really does hold its directory's lock.
	_, err := NewService(ServiceOptions{Profile: Profile{Version: 1, Endpoint: "https://database.example/db/v1"}, StateDir: service.directory, PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey})
	require.Error(t, err, "sanity check: the live service's directory must really be locked")

	result, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err, "result: %+v", result)
	require.True(t, result.CatalogRepointed)
	require.Equal(t, 1, result.Copied)
	require.NotEmpty(t, result.RunID)

	bucket.mu.Lock()
	require.Len(t, bucket.objects, 1)
	bucket.mu.Unlock()
	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	backend.mu.Unlock()
}

// TestMigrateLocalLiveFailsClearlyBeforeCopyingWhenDestinationMissing
// confirms the destination connection is checked against the live server's
// own profile, and checked before anything is copied.
func TestMigrateLocalLiveFailsClearlyBeforeCopyingWhenDestinationMissing(t *testing.T) {
	opts, _, _, bucket, _ := migrateLiveFixture(t)
	opts.ToConnection = "does-not-exist"

	_, err := MigrateLocal(t.Context(), opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does-not-exist")

	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	require.Zero(t, bucket.puts, "nothing should be copied before the destination connection is checked")
}

// TestMigrateLocalLiveRetries409ThenSucceeds simulates another run already in
// progress on the live service (its own admission, not migrate-local's) and
// confirms migrate-local waits it out instead of failing immediately.
func TestMigrateLocalLiveRetries409ThenSucceeds(t *testing.T) {
	opts, service, _, bucket, backend := migrateLiveFixture(t)

	service.mu.Lock()
	_, cancel := context.WithCancel(context.Background())
	service.cancel = cancel
	service.mu.Unlock()
	go func() {
		time.Sleep(300 * time.Millisecond)
		service.mu.Lock()
		service.cancel = nil
		service.mu.Unlock()
	}()

	start := time.Now()
	result, err := MigrateLocal(t.Context(), opts)
	require.NoError(t, err, "result: %+v", result)
	require.True(t, result.CatalogRepointed)
	require.Greater(t, time.Since(start), 250*time.Millisecond, "must have actually waited through at least one retry")

	bucket.mu.Lock()
	require.Len(t, bucket.objects, 1)
	bucket.mu.Unlock()
	backend.mu.Lock()
	require.Len(t, backend.records, 1)
	backend.mu.Unlock()
}

// TestMigrateLocalLiveGivesUpAfter409Budget confirms a run that never clears
// fails clearly instead of retrying forever.
func TestMigrateLocalLiveGivesUpAfter409Budget(t *testing.T) {
	opts, service, _, _, _ := migrateLiveFixture(t)

	original := migrateLiveRetryBudget
	migrateLiveRetryBudget = 100 * time.Millisecond
	t.Cleanup(func() { migrateLiveRetryBudget = original })

	service.mu.Lock()
	_, cancel := context.WithCancel(context.Background())
	service.cancel = cancel
	service.mu.Unlock()
	t.Cleanup(func() {
		service.mu.Lock()
		service.cancel = nil
		service.mu.Unlock()
	})

	_, err := MigrateLocal(t.Context(), opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already in progress")
}

// TestMigrateLocalPreRepointMediaReferenceGoesStaleAtRepoint drives PUT and
// POST separately (rather than through migrateRepoint) so it can fetch the
// pre-repoint reference in the window between them: local bytes are never
// deleted, and the pin itself is not pruned until the resync's cleanup runs,
// so the old reference keeps resolving right up until that point -- not
// gradually, and not because the bytes are still there (they always are).
// TestMigrateLocalPreRepointMediaReferenceGoesStaleAtRepoint drives the PUT
// (repoint) step directly, separately from any resync, to pin down exactly
// when a reference issued before repointing stops working.
//
// It does not survive until the resync's cleanup, as the earlier version of
// this test assumed: the PUT alone invalidates it immediately. A pin's
// signed id is bound to a hash of the source's own config (sourceMediaBinding
// in service_media_digest.go), which the companion recomputes and compares
// on every fetch; changing the source's Connection/Config -- exactly what
// repointing does -- changes that hash before any byte is re-copied or any
// resync runs, so the fetch returns 410 "media_revoked" right after the PUT.
// Local bytes are still never deleted; it is specifically the reference that
// stops resolving, immediately, not gradually and not because of pruning.
func TestMigrateLocalPreRepointMediaReferenceGoesStaleAtRepoint(t *testing.T) {
	_, service, root, _, backend := migrateLiveFixture(t)

	job := startServiceJob(t, service)
	require.Equal(t, "completed", job.State)
	ref := mediaFromBackend(t, backend)
	before := fetchService(service, ref, "GET", "")
	require.Equal(t, 200, before.Code, before.Body.String())
	require.Equal(t, "hello world", before.Body.String())

	entries, err := os.ReadDir(filepath.Join(root, "assets", "objects", "sha256"))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	localObjectPath := filepath.Join(root, "assets", "objects", "sha256", entries[0].Name())
	beforeInfo, err := os.Stat(localObjectPath)
	require.NoError(t, err)

	// Repoint the connection (PUT only); no resync has run yet.
	destConfig, err := json.Marshal(S3Config{Bucket: "testbucket", Prefix: "migrated"})
	require.NoError(t, err)
	updated := Source{ID: testSourceID, Alias: "assets", Connection: "bucket", Table: "sources", Config: destConfig, Permissions: &Permissions{Public: true}}
	putResp := serviceCall(service, "PUT", "/v1/sources/"+testSourceID, updated)
	require.Equal(t, 200, putResp.Code, putResp.Body.String())

	immediately := fetchService(service, ref, "GET", "")
	require.Equal(t, 410, immediately.Code, "the PUT alone -- before any resync -- must already invalidate a pre-repoint reference, via the source config binding check, not via pin pruning")

	afterPut, err := os.Stat(localObjectPath)
	require.NoError(t, err)
	require.Equal(t, beforeInfo.ModTime(), afterPut.ModTime(), "local bytes must be untouched by repointing alone")

	// The resync that follows (what migrate-local actually triggers) only
	// confirms the same thing and additionally prunes the now-dangling pin
	// file; the reference was already unusable before this ran.
	job2 := startServiceJob(t, service)
	require.Equal(t, "completed", job2.State)

	// Still 410: the handler returns the identical status whether the pin
	// file is merely mismatched (as right after the PUT, above) or now
	// missing entirely (pruned by this resync's cleanup), so a client learns
	// nothing about which has happened -- just that this reference is dead.
	after := fetchService(service, ref, "GET", "")
	require.Equal(t, 410, after.Code, "a pre-repoint reference stays rejected once its pin is pruned, with no distinct status for cleanup having run")

	afterSync, err := os.Stat(localObjectPath)
	require.NoError(t, err)
	require.Equal(t, beforeInfo.ModTime(), afterSync.ModTime(), "local bytes must still be untouched")
}

// TestMigrateLocalRepointsCatalogForGCSAndSyncsAfterward exercises the real
// repoint/resync/media-delivery machinery end to end for a GCS destination,
// against a hand-rolled fake GCS JSON-API server (no real Google credential
// or network call -- googleCredentialsWithScopes rejects any token_uri but
// Google's own, by design, so a real OAuth exchange cannot be faked here;
// this test supplies an already-authorized plain client directly instead,
// exactly like google_test.go fakes GoogleConnector for its own tests).
func TestMigrateLocalRepointsCatalogForGCSAndSyncsAfterward(t *testing.T) {
	root := t.TempDir()
	localPut(t, root, "assets", ".bin", "hello world")
	t.Setenv(LocalRootsEnv, root)

	gcsServer, bucket := newMigrateFakeGCSBucket(t)
	backend := &publicationFixture{records: map[string]map[string]any{}}
	antfly := httptest.NewServer(backend)
	t.Cleanup(antfly.Close)
	t.Setenv("MIGRATE_LOCAL_GCS_DB_TOKEN", "database-test-token")

	profile := Profile{
		Version: 1, Endpoint: antfly.URL + "/api/v1", DataPlaneTokenEnv: "MIGRATE_LOCAL_GCS_DB_TOKEN",
		Principals: []string{"user:self"},
		Connections: map[string]Connection{
			"local": {Provider: ProviderLocal, LocalRoot: root},
			"gcs":   {Provider: ProviderGCS},
		},
		Sources: []Source{{
			ID: testSourceID, Alias: "assets", Connection: "local", Table: "sources",
			Config: json.RawMessage(`{"prefix":"assets"}`), Permissions: &Permissions{Public: true},
		}},
	}
	// Reads (ordinary enumeration/access during a resync) go through this
	// fake GCS connector -- no OAuth, pointed at the same fake bucket the
	// write side uses.
	factory := func(ctx context.Context, connection Connection) (Connector, error) {
		if connection.Provider == ProviderGCS {
			return &GoogleConnector{ProviderName: ProviderGCS, APIBase: gcsServer.URL, HTTPClient: gcsServer.Client()}, nil
		}
		return EnvironmentFactory(ctx, connection)
	}
	service, err := NewService(ServiceOptions{Profile: profile, StateDir: t.TempDir(), PublicURL: "https://source.example", AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: factory})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	service.mu.Lock()
	var source Source
	for _, candidate := range service.profile.Sources {
		if candidate.ID == testSourceID {
			source = candidate
		}
	}
	service.mu.Unlock()
	require.Equal(t, testSourceID, source.ID)

	// The write side bypasses migrateGCSWriteClient/OAuth the same way --
	// an already-authorized plain client, directly.
	dest := &gcsDestination{client: gcsServer.Client(), bucket: "testbucket", apiBase: gcsServer.URL, uploadBase: gcsServer.URL}
	opts := MigrateLocalOptions{FromPrefix: "assets", ToConnection: "gcs", ToBucket: "testbucket", ToPrefix: "migrated"}
	result := MigrateLocalResult{SourceID: testSourceID, ToConnection: "gcs", Bucket: "testbucket", ToPrefix: "migrated"}

	result, err = migrateLocalRun(t.Context(), opts, result, source, &LocalConnector{Root: root}, dest, ProviderGCS, service)
	require.NoError(t, err, "result: %+v", result)
	require.True(t, result.CatalogRepointed)
	require.Equal(t, 1, result.Copied)
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
	require.Equal(t, ProviderGCS, doc.Metadata["provider"])

	// Fetch through the signed companion reference, now backed by GCS.
	u, err := url.Parse(doc.Media.URI)
	require.NoError(t, err)
	fetch := newLoopbackMediaRequest(t, u, doc.Media.Version)
	w := &controlResponse{header: make(http.Header)}
	service.ServeHTTP(w, fetch)
	require.Equal(t, 200, w.status, w.body.String())
	require.Equal(t, "hello world", w.body.String())

	// The source must sync normally afterward: an ordinary resync against
	// the now-GCS connection succeeds (proving ordinary enumeration/read
	// works against the fake, not just the one-off migration read).
	job := startServiceJob(t, service)
	require.Equal(t, "completed", job.State, "the migrated GCS source must sync normally afterward")
}
