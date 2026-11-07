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
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func localRequest(t *testing.T, prefix string) Request {
	t.Helper()
	raw, err := json.Marshal(LocalConfig{Prefix: prefix})
	require.NoError(t, err)
	return Request{SourceNamespace: "job/local", Config: raw, Mode: ImportModeSource}
}

func localPut(t *testing.T, root, prefix, ext, body string) LocalObject {
	t.Helper()
	object, err := PutLocalObject(root, prefix, ext, strings.NewReader(body))
	require.NoError(t, err)
	return object
}

func TestLocalPutIsContentAddressedAndIdempotent(t *testing.T) {
	root := t.TempDir()
	first := localPut(t, root, "uploads", ".png", "image bytes")
	sum := sha256.Sum256([]byte("image bytes"))
	require.Equal(t, hex.EncodeToString(sum[:]), first.SHA256)
	require.Equal(t, "uploads/objects/sha256/"+first.SHA256+".png", first.Path)
	before, err := os.Stat(filepath.Join(root, first.Path))
	require.NoError(t, err)
	require.Equal(t, first, localPut(t, root, "uploads", ".png", "image bytes"))
	after, err := os.Stat(filepath.Join(root, first.Path))
	require.NoError(t, err)
	require.Equal(t, before.ModTime(), after.ModTime(), "a retried put must not invalidate published records")
	entries, err := os.ReadDir(filepath.Join(root, "uploads", "objects", "sha256"))
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotContains(t, entry.Name(), ".tmp", "a retried put must leave no temporary files")
	}
	for _, invalid := range []string{"../escape", "/abs", "a/../b", `a\b`} {
		_, err := PutLocalObject(root, invalid, "", strings.NewReader("x"))
		require.ErrorIs(t, err, ErrInvalidInput, invalid)
	}
	_, err = PutLocalObject(root, "", "../x", strings.NewReader("x"))
	require.ErrorIs(t, err, ErrInvalidInput)
}

// TestLocalRelativeKeyStripsConfiguredPrefix pins that a local item's
// RelativeKey is relative to the source's own root (its configured prefix),
// not the shared store root: changing the prefix changes Item.ID (and so the
// legacy RecordKey) but must not change RelativeKey, and setting
// identity.namespace to today's provider-ID prefix must reconstruct the
// provider ID byte for byte (the invariant IdentityKey relies on).
func TestLocalRelativeKeyStripsConfiguredPrefix(t *testing.T) {
	root := t.TempDir()
	object := localPut(t, root, "team-a", ".txt", "hello")
	inventory, err := (&LocalConnector{Root: root}).Enumerate(context.Background(), localRequest(t, "team-a"))
	require.NoError(t, err)
	require.Len(t, inventory.Objects, 1)
	item := inventory.Objects[0]
	require.Equal(t, "objects/sha256/"+object.SHA256+".txt", item.RelativeKey)
	require.NotEqual(t, item.Path, item.RelativeKey, "Path keeps the configured prefix; RelativeKey strips it")

	namespace := strings.TrimSuffix(item.ID, "/"+item.RelativeKey)
	require.Equal(t, item.ID, namespace+"/"+item.RelativeKey)
}

func TestLocalPutRepairsDamagedObject(t *testing.T) {
	root := t.TempDir()
	object := localPut(t, root, "", ".bin", "original")
	path := filepath.Join(root, object.Path)
	require.NoError(t, os.WriteFile(path, []byte("corrupt!"), 0o600))
	require.Equal(t, object, localPut(t, root, "", ".bin", "original"))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "original", string(data))
	inventory, err := (&LocalConnector{Root: root}).Enumerate(context.Background(), localRequest(t, ""))
	require.NoError(t, err)
	_, err = VerifyMediaDigest(context.Background(), &LocalConnector{Root: root}, localRequest(t, ""), inventory.Objects[0])
	require.NoError(t, err)
}

func TestLocalPutConcurrentWritersKeepOnePublishedObject(t *testing.T) {
	root := t.TempDir()
	const writers = 32
	stamps := make([]time.Time, writers)
	objects := make([]LocalObject, writers)
	errs := make([]error, writers)
	var ready, done sync.WaitGroup
	start := make(chan struct{})
	for i := range writers {
		ready.Add(1)
		done.Add(1)
		go func() {
			defer done.Done()
			ready.Done()
			<-start
			objects[i], errs[i] = PutLocalObject(root, "", ".bin", strings.NewReader("same bytes"))
			if errs[i] == nil {
				info, err := os.Stat(filepath.Join(root, objects[i].Path))
				errs[i] = err
				if err == nil {
					stamps[i] = info.ModTime()
				}
			}
		}()
	}
	ready.Wait()
	close(start)
	done.Wait()
	for i := range writers {
		require.NoError(t, errs[i])
		require.Equal(t, objects[0], objects[i])
		require.True(t, stamps[0].Equal(stamps[i]), "a later writer replaced the published object")
	}
	entries, err := os.ReadDir(filepath.Dir(filepath.Join(root, objects[0].Path)))
	require.NoError(t, err)
	for _, entry := range entries {
		require.NotContains(t, entry.Name(), ".tmp", "writers must not leave temporary files")
	}
}

func TestLocalPutConcurrentRepairsConverge(t *testing.T) {
	root := t.TempDir()
	object := localPut(t, root, "", ".bin", "original")
	path := filepath.Join(root, object.Path)
	require.NoError(t, os.WriteFile(path, []byte("corrupt!"), 0o600))
	var done sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		done.Add(1)
		go func() {
			defer done.Done()
			_, errs[i] = PutLocalObject(root, "", ".bin", strings.NewReader("original"))
		}()
	}
	done.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "original", string(data))
	inventory, err := (&LocalConnector{Root: root}).Enumerate(context.Background(), localRequest(t, ""))
	require.NoError(t, err, "the lock file must not disturb enumeration")
	require.Len(t, inventory.Objects, 1)
}

func TestLocalSnapshotMissesShareOneRead(t *testing.T) {
	var cache localSnapshotCache
	var reads atomic.Int32
	release := make(chan struct{})
	read := func() ([]byte, error) {
		reads.Add(1)
		<-release
		return []byte("verified"), nil
	}
	var done sync.WaitGroup
	results := make([][]byte, 16)
	errs := make([]error, 16)
	for i := range results {
		done.Add(1)
		go func() {
			defer done.Done()
			results[i], errs[i] = cache.acquire(context.Background(), "key", 8, read)
		}()
	}
	require.Eventually(t, func() bool { return reads.Load() == 1 }, time.Second, time.Millisecond)
	time.Sleep(20 * time.Millisecond)
	close(release)
	done.Wait()
	require.EqualValues(t, 1, reads.Load())
	for i := range results {
		require.NoError(t, errs[i])
		require.Equal(t, "verified", string(results[i]))
	}
}

func TestLocalSnapshotReadsStayWithinBudgetAndHonorCancellation(t *testing.T) {
	var cache localSnapshotCache
	release := make(chan struct{})
	started := make(chan struct{})
	first := make(chan error, 1)
	go func() {
		_, err := cache.acquire(context.Background(), "a", localSnapshotBudget, func() ([]byte, error) {
			close(started)
			<-release
			return nil, errors.New("read failed")
		})
		first <- err
	}()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := cache.acquire(ctx, "b", 1, func() ([]byte, error) {
		t.Error("a read must not start beyond the budget")
		return nil, nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	waiting := make(chan []byte, 1)
	go func() {
		data, _ := cache.acquire(context.Background(), "c", 1, func() ([]byte, error) { return []byte("c"), nil })
		waiting <- data
	}()
	close(release)
	require.EqualError(t, <-first, "read failed")
	require.Equal(t, "c", string(<-waiting), "releasing the budget must wake waiters")
	_, err = cache.acquire(context.Background(), "big", localSnapshotBudget+1, nil)
	require.ErrorIs(t, err, ErrInvalidInput)
}

func TestLocalSnapshotFollowersSurviveLeaderCancellation(t *testing.T) {
	var cache localSnapshotCache
	release := make(chan struct{})
	started := make(chan struct{})
	go func() {
		_, _ = cache.acquire(context.Background(), "key", 1, func() ([]byte, error) {
			close(started)
			<-release
			return nil, context.Canceled
		})
	}()
	<-started
	result := make(chan []byte, 1)
	go func() {
		data, _ := cache.acquire(context.Background(), "key", 1, func() ([]byte, error) { return []byte("x"), nil })
		result <- data
	}()
	time.Sleep(10 * time.Millisecond)
	close(release)
	require.Equal(t, "x", string(<-result))
}

func BenchmarkLocalRangeDelivery(b *testing.B) {
	root := b.TempDir()
	object, err := PutLocalObject(root, "", ".bin", strings.NewReader(strings.Repeat("x", 32<<20)))
	require.NoError(b, err)
	c := &LocalConnector{Root: root}
	raw, _ := json.Marshal(LocalConfig{})
	request := Request{SourceNamespace: "job/local", Config: raw, Mode: ImportModeSource}
	inventory, err := c.Enumerate(context.Background(), request)
	require.NoError(b, err)
	localSnapshots.reset()
	b.ReportAllocs()
	for b.Loop() {
		data, _, err := c.AcquireMedia(context.Background(), request, inventory.Objects[0])
		require.NoError(b, err)
		require.Len(b, data, int(object.Size))
	}
}

func TestLocalDeliveryHashesWhenPinnedDigestDiffers(t *testing.T) {
	root := t.TempDir()
	object := localPut(t, root, "", ".bin", "0123456789")
	c := &LocalConnector{Root: root}
	request := localRequest(t, "")
	inventory, err := c.Enumerate(context.Background(), request)
	require.NoError(t, err)
	item := inventory.Objects[0]
	serve := func(digest string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/media", nil)
		r.Header.Set("Range", "bytes=2-5")
		w := httptest.NewRecorder()
		ServeMedia(w, r, c, request, item, digest)
		return w
	}
	ok := serve(object.SHA256)
	require.Equal(t, 206, ok.Code)
	require.Equal(t, "2345", ok.Body.String())
	wrong := serve(strings.Repeat("a", 64))
	require.NotEqual(t, 206, wrong.Code, "a pinned digest the connector did not verify must be checked")
	require.NotContains(t, wrong.Body.String(), "2345")
}

// One-byte responses through the complete delivery path, after the first read.
func BenchmarkLocalRangeResponse(b *testing.B) {
	root := b.TempDir()
	content := strings.Repeat("x", 32<<20)
	object, err := PutLocalObject(root, "", ".bin", strings.NewReader(content))
	require.NoError(b, err)
	c := &LocalConnector{Root: root}
	raw, _ := json.Marshal(LocalConfig{})
	request := Request{SourceNamespace: "job/local", Config: raw, Mode: ImportModeSource}
	inventory, err := c.Enumerate(context.Background(), request)
	require.NoError(b, err)
	localSnapshots.reset()
	serve := func() {
		r := httptest.NewRequest("GET", "/media", nil)
		r.Header.Set("Range", "bytes=5-5")
		w := httptest.NewRecorder()
		ServeMedia(w, r, c, request, inventory.Objects[0], object.SHA256)
		require.Equal(b, 206, w.Code)
	}
	serve()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		serve()
	}
}

func TestLocalEnumerateFailsClosedWhenStoreIsMissing(t *testing.T) {
	c := &LocalConnector{Root: t.TempDir()}
	_, err := c.Enumerate(context.Background(), localRequest(t, ""))
	require.Error(t, err, "a lost mount must not read as an authoritative empty inventory")

	object := localPut(t, c.Root, "", ".pdf", "report")
	inventory, err := c.Enumerate(context.Background(), localRequest(t, ""))
	require.NoError(t, err)
	require.True(t, inventory.Authoritative)
	require.Len(t, inventory.Objects, 1)
	item := inventory.Objects[0]
	require.Equal(t, object.SHA256, item.Revision)
	require.Equal(t, ProviderLocal, item.SourceKind)
	require.True(t, ownedID(item.ID, "job/local"), item.ID)
	require.NotContains(t, item.URL, c.Root, "source records must not reveal the host path")

	require.NoError(t, os.Remove(filepath.Join(c.Root, object.Path)))
	inventory, err = c.Enumerate(context.Background(), localRequest(t, ""))
	require.NoError(t, err, "an existing empty store is a real deletion")
	require.Empty(t, inventory.Objects)
}

func TestLocalEnumerateRejectsUnexpectedEntries(t *testing.T) {
	c := &LocalConnector{Root: t.TempDir()}
	object := localPut(t, c.Root, "", ".txt", "body")
	dir := filepath.Dir(filepath.Join(c.Root, object.Path))
	require.NoError(t, os.WriteFile(filepath.Join(dir, ".DS_Store"), []byte("x"), 0o600))
	inventory, err := c.Enumerate(context.Background(), localRequest(t, ""))
	require.NoError(t, err, "hidden files include put's temporary files")
	require.Len(t, inventory.Objects, 1)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o600))
	_, err = c.Enumerate(context.Background(), localRequest(t, ""))
	require.ErrorIs(t, err, ErrInvalidInput)
	require.NoError(t, os.Remove(filepath.Join(dir, "notes.txt")))

	outside := filepath.Join(t.TempDir(), "secret")
	require.NoError(t, os.WriteFile(outside, []byte("secret"), 0o600))
	require.NoError(t, os.Symlink(outside, filepath.Join(dir, strings.Repeat("a", 64))))
	_, err = c.Enumerate(context.Background(), localRequest(t, ""))
	require.ErrorIs(t, err, ErrInvalidInput, "symlinks must not be published")
}

func TestLocalMediaVerifiesPinnedContent(t *testing.T) {
	ctx := context.Background()
	c := &LocalConnector{Root: t.TempDir()}
	request := localRequest(t, "")
	object := localPut(t, c.Root, "", ".bin", "0123456789")
	inventory, err := c.Enumerate(ctx, request)
	require.NoError(t, err)
	item := inventory.Objects[0]

	digest, err := VerifyMediaDigest(ctx, c, request, item)
	require.NoError(t, err)
	require.Equal(t, object.SHA256, digest)
	data, err := c.Read(ctx, request, item)
	require.NoError(t, err)
	require.Equal(t, "0123456789", string(data))

	acquired, ok, err := c.AcquireMedia(ctx, request, item)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "0123456789", string(acquired))

	body, err := c.OpenMedia(ctx, request, item, MediaRange{Offset: 2, Length: 4})
	require.NoError(t, err)
	part, err := io.ReadAll(body)
	require.NoError(t, err)
	require.NoError(t, body.Close())
	require.Equal(t, "2345", string(part))

	body, err = c.OpenMedia(ctx, request, item, MediaRange{Offset: 2, Length: 4})
	require.NoError(t, err)
	require.Error(t, body.Close(), "an unread range must not close successfully")

	// Same size, different bytes: the name no longer matches its content.
	require.NoError(t, os.WriteFile(filepath.Join(c.Root, object.Path), []byte("9876543210"), 0o600))
	_, err = c.Read(ctx, request, item)
	var changed *RevisionChangedError
	require.True(t, errors.As(err, &changed), "%v", err)
	require.True(t, errors.As(c.CheckMedia(ctx, request, item), &changed), "access checks must notice the overwrite")

	// Restoring the old modification time defeats the cheap fence, not the digest.
	modified, err := strconv.ParseInt(configString(item.Metadata, localModifiedKey), 10, 64)
	require.NoError(t, err)
	stamp := time.Unix(0, modified)
	require.NoError(t, os.Chtimes(filepath.Join(c.Root, object.Path), stamp, stamp))
	require.NoError(t, c.CheckMedia(ctx, request, item))
	_, err = c.OpenMedia(ctx, request, item, MediaRange{Offset: 2, Length: 4})
	require.True(t, errors.As(err, &changed), "%v", err)
	// A verified snapshot still holds the published bytes; without one, the digest fails.
	cached, _, err := c.AcquireMedia(ctx, request, item)
	require.NoError(t, err)
	require.Equal(t, "0123456789", string(cached))
	localSnapshots.reset()
	_, _, err = c.AcquireMedia(ctx, request, item)
	require.True(t, errors.As(err, &changed), "%v", err)

	require.NoError(t, os.WriteFile(filepath.Join(c.Root, object.Path), []byte("short"), 0o600))
	require.True(t, errors.As(c.CheckMedia(ctx, request, item), &changed))

	require.NoError(t, os.Remove(filepath.Join(c.Root, object.Path)))
	require.True(t, errors.As(c.CheckMedia(ctx, request, item), &changed))
}

func TestLocalRejectsPinsOutsideTheStore(t *testing.T) {
	ctx := context.Background()
	c := &LocalConnector{Root: t.TempDir()}
	object := localPut(t, c.Root, "a", "", "owned by a")
	localPut(t, c.Root, "b", "", "owned by b")
	for _, item := range []Item{
		{Path: object.Path, Revision: object.SHA256, Size: object.Size},                                   // another source's prefix
		{Path: "b/objects/sha256/../../../" + object.Path, Revision: object.SHA256, Size: object.Size},    // traversal
		{Path: "b/objects/sha256/" + strings.Repeat("0", 64), Revision: strings.Repeat("1", 64), Size: 1}, // revision mismatch
	} {
		require.ErrorIs(t, c.CheckAccess(ctx, localRequest(t, "b"), item), ErrInvalidInput, item.Path)
	}
}

func TestLocalConnectionRequiresAllowlistedRoot(t *testing.T) {
	ctx := context.Background()
	factory := CredentialFactory(nil)
	base := t.TempDir()
	root := filepath.Join(base, "store")
	require.NoError(t, os.Mkdir(root, 0o700))
	connection := Connection{Provider: ProviderLocal, LocalRoot: root}

	t.Setenv(LocalRootsEnv, "")
	_, err := factory(ctx, connection)
	require.ErrorIs(t, err, ErrInvalidInput, "local connections are denied by default")

	t.Setenv(LocalRootsEnv, t.TempDir())
	_, err = factory(ctx, connection)
	require.ErrorIs(t, err, ErrInvalidInput)

	t.Setenv(LocalRootsEnv, base)
	connector, err := factory(ctx, connection)
	require.NoError(t, err)
	require.IsType(t, &LocalConnector{}, connector)

	escape := filepath.Join(base, "escape")
	require.NoError(t, os.Symlink(t.TempDir(), escape))
	_, err = factory(ctx, Connection{Provider: ProviderLocal, LocalRoot: escape})
	require.ErrorIs(t, err, ErrInvalidInput, "symlinks must not escape the allowlist")

	_, err = factory(ctx, Connection{Provider: ProviderLocal, LocalRoot: "relative"})
	require.ErrorIs(t, err, ErrInvalidInput)
	_, err = factory(ctx, Connection{Provider: ProviderLocal, LocalRoot: root, TokenEnv: "TOKEN"})
	require.ErrorIs(t, err, ErrInvalidInput)
}

// The app-facing flow: put a file, run only its source, then fetch it back.
func TestLocalUploadPublishesThroughASingleSourceRun(t *testing.T) {
	runner, _, backend := runnerFixture(t)
	root := t.TempDir()
	t.Setenv(LocalRootsEnv, root)
	t.Setenv("SOURCE_SERVICE_TEST_DB_TOKEN", "database-test-token")
	profile := runner.Profile
	profile.DataPlaneTokenEnv = "SOURCE_SERVICE_TEST_DB_TOKEN"
	profile.Connections = map[string]Connection{"docs": {Provider: ProviderLocal, LocalRoot: root}}
	config, err := json.Marshal(LocalConfig{Prefix: "uploads"})
	require.NoError(t, err)
	profile.Sources[0].Config = config
	profile.Sources[0].Publication = "media"
	s, err := NewService(ServiceOptions{Profile: profile, StateDir: t.TempDir(), PublicURL: "https://source.example.com", AdminToken: serviceTestToken, MediaKey: serviceTestKey, Factory: CredentialFactory(nil)})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })

	object := localPut(t, root, "uploads", ".png", "\x89PNG uploaded bytes")
	response := serviceCall(s, "POST", "/v1/sync", map[string]any{"source_id": profile.Sources[0].ID})
	require.Equal(t, 202, response.Code, response.Body.String())
	var job SyncJob
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &job))
	require.Equal(t, "completed", waitServiceJob(t, s, job.ID).State)

	ref := mediaFromBackend(t, backend)
	require.Equal(t, `"`+object.SHA256+`"`, ref.Version, "the version is the digest SDK callers cache by")
	require.Equal(t, "image/png", ref.MIMEType)
	full := fetchService(s, ref, "GET", "")
	require.Equal(t, 200, full.Code, full.Body.String())
	require.Equal(t, "\x89PNG uploaded bytes", full.Body.String())

	part := fetchService(s, ref, "GET", "bytes=2-5")
	require.Equal(t, 206, part.Code)
	require.Equal(t, "NG u", part.Body.String())

	// Overwriting must never serve changed bytes under the original ETag, including
	// when the modification time is restored and only the digest can notice.
	path := filepath.Join(root, object.Path)
	original := []byte("\x89PNG uploaded bytes")
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, []byte("\x89PNG CHANGED! bytes"), 0o600))
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))
	for _, byteRange := range []string{"bytes=2-5", ""} {
		// A verified snapshot can keep serving the published bytes, never the new ones.
		cached := fetchService(s, ref, "GET", byteRange)
		require.NotContains(t, cached.Body.String(), "CHANGED", byteRange)
		localSnapshots.reset()
		changed := fetchService(s, ref, "GET", byteRange)
		require.Equal(t, 412, changed.Code, byteRange)
		require.NotContains(t, changed.Body.String(), "CHANGED")
	}
	require.NoError(t, os.WriteFile(path, original, 0o600))
	require.NoError(t, os.Chtimes(path, info.ModTime(), info.ModTime()))

	// The bucket-equivalent delete: removing the object retires its row.
	require.NoError(t, os.Remove(filepath.Join(root, object.Path)))
	require.Equal(t, "completed", startServiceJob(t, s).State)
	require.Equal(t, 410, fetchService(s, ref, "GET", "").Code)
	backend.mu.Lock()
	require.Empty(t, backend.records)
	backend.mu.Unlock()
}
