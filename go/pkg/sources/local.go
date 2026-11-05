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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

// LocalRootsEnv lists the only directories a local connection may open, using
// the platform path-list separator. Unset or empty denies local connections, so
// a management token alone cannot turn the service into a filesystem reader.
const LocalRootsEnv = "ANTFLY_SOURCE_LOCAL_ROOTS"

const localObjectDir = "objects/sha256"

var localObjectName = regexp.MustCompile(`^([0-9a-f]{64})(\.[A-Za-z0-9]{1,16})?$`)

// LocalConfig selects one store beneath the connection root. Several sources can
// share a root by using different prefixes.
type LocalConfig struct {
	Prefix string `json:"prefix,omitempty"`
}

// LocalConnector serves a content-addressed directory for deployments with no
// object store. Objects live at <root>/<prefix>/objects/sha256/<digest>[.ext],
// so a name is its immutable revision and overwriting cannot go unnoticed.
type LocalConnector struct {
	Root string
}

// LocalObject identifies one object written by PutLocalObject.
type LocalObject struct {
	Path   string
	SHA256 string
	Size   int64
}

func localRootAllowed(root, allowed string) (string, error) {
	if root == "" || !filepath.IsAbs(root) {
		return "", fmt.Errorf("%w: local_root must be an absolute path", ErrInvalidInput)
	}
	resolved, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("%w: local_root is unavailable", ErrInvalidInput)
	}
	for _, candidate := range filepath.SplitList(allowed) {
		if candidate == "" {
			continue
		}
		base, err := filepath.EvalSymlinks(candidate)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(base, resolved)
		if err == nil && filepath.IsLocal(rel) {
			return resolved, nil
		}
	}
	return "", fmt.Errorf("%w: local_root is not listed in %s", ErrInvalidInput, LocalRootsEnv)
}

func localStoreDir(prefix string) (string, error) {
	if prefix == "" {
		return localObjectDir, nil
	}
	if strings.Contains(prefix, "\\") || strings.ContainsRune(prefix, 0) || !filepath.IsLocal(filepath.FromSlash(prefix)) || path.Clean(prefix) != prefix {
		return "", fmt.Errorf("%w: invalid local prefix", ErrInvalidInput)
	}
	return path.Join(prefix, localObjectDir), nil
}

func (c *LocalConnector) store(r Request) (*os.Root, string, error) {
	cfg, err := decodeSourceConfig[LocalConfig](r.Config)
	if err != nil {
		return nil, "", err
	}
	dir, err := localStoreDir(cfg.Prefix)
	if err != nil {
		return nil, "", err
	}
	root, err := os.OpenRoot(c.Root)
	if err != nil {
		return nil, "", fmt.Errorf("local store root is unavailable: %w", err)
	}
	return root, dir, nil
}

// Enumerate requires the store directory to exist. An empty listing authorizes
// deleting every owned row, so a lost mount must fail rather than read as empty.
func (c *LocalConnector) Enumerate(ctx context.Context, r Request) (Inventory, error) {
	root, dir, err := c.store(r)
	if err != nil {
		return Inventory{}, err
	}
	defer root.Close()
	entries, err := fs.ReadDir(root.FS(), dir)
	if err != nil {
		return Inventory{}, fmt.Errorf("local store is unavailable: %w", err)
	}
	out := Inventory{Authoritative: !r.Preview && len(r.RetrySourceKeys) == 0, BatchObjects: 1, BatchBytes: maxImportSourceObjectBytes}
	retry := exactSourceKeySet(r.RetrySourceKeys)
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return Inventory{}, err
		}
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}
		match := localObjectName.FindStringSubmatch(name)
		if match == nil || !entry.Type().IsRegular() {
			return Inventory{}, fmt.Errorf("%w: local store contains an unexpected entry", ErrInvalidInput)
		}
		rel := path.Join(dir, name)
		if len(r.RetrySourceKeys) > 0 {
			if _, ok := retry[rel]; !ok {
				continue
			}
			delete(retry, rel)
		}
		info, err := entry.Info()
		if err != nil {
			return Inventory{}, err
		}
		if len(out.Objects) >= maxImportSourceObjects {
			return Inventory{}, fmt.Errorf("%w: local inventory limit exceeded", ErrInvalidInput)
		}
		uri := (&url.URL{Scheme: "local", Path: "/" + rel}).String()
		out.Objects = append(out.Objects, Item{ID: "local://" + strings.Trim(r.SourceNamespace, "/") + "/" + rel, Path: rel, Filename: name, Revision: match[1], URL: uri, Size: info.Size(), SourceKind: ProviderLocal, ReadMode: sourceReadBytes, ObjectIndex: len(out.Objects) + 1, Metadata: map[string]any{"source_namespace": r.SourceNamespace, "canonical_url": uri}})
		if r.Preview {
			break
		}
	}
	if len(retry) > 0 {
		return Inventory{}, fmt.Errorf("%w: selected local retries missing", ErrInvalidInput)
	}
	return out, nil
}

func (c *LocalConnector) ValidateInventory(context.Context, Request, Inventory) error { return nil }

func (c *LocalConnector) ValidateAbsentSources(context.Context, Request, []string) error { return nil }

// open fences an item to its pinned identity. Root rejects symlinks that escape
// the store, and an interior symlink fails the regular-file check after opening.
func (c *LocalConnector) open(r Request, o Item) (*os.File, error) {
	root, dir, err := c.store(r)
	if err != nil {
		return nil, err
	}
	defer root.Close()
	match := localObjectName.FindStringSubmatch(path.Base(o.Path))
	if match == nil || path.Dir(o.Path) != dir || match[1] != o.Revision {
		return nil, fmt.Errorf("%w: pinned item is outside the local store", ErrInvalidInput)
	}
	if info, err := root.Lstat(o.Path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, &RevisionChangedError{Expected: o.Revision, Actual: "missing"}
		}
		return nil, err
	} else if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: local object is not a regular file", ErrInvalidInput)
	}
	file, err := root.Open(o.Path)
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err == nil && info.Size() != o.Size {
		err = &RevisionChangedError{Expected: o.Revision, Actual: "different size"}
	}
	if err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

func (c *LocalConnector) Read(ctx context.Context, r Request, o Item) ([]byte, error) {
	if o.Size > maxImportSourceObjectBytes {
		return nil, fmt.Errorf("%w: source object exceeds import limit", ErrInvalidInput)
	}
	if o.Size == 0 {
		// MediaRange rejects empty spans, and the empty object has one digest.
		if o.Revision != hex.EncodeToString(sha256.New().Sum(nil)) {
			return nil, &RevisionChangedError{Expected: o.Revision, Actual: "empty object"}
		}
		return []byte{}, c.CheckAccess(ctx, r, o)
	}
	body, err := c.OpenMedia(ctx, r, o, MediaRange{Length: o.Size})
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(body)
	if closeErr := body.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (c *LocalConnector) CheckAccess(_ context.Context, r Request, o Item) error {
	file, err := c.open(r, o)
	if err != nil {
		return err
	}
	return file.Close()
}

func (c *LocalConnector) CheckMedia(ctx context.Context, r Request, o Item) error {
	return c.CheckAccess(ctx, r, o)
}

func (c *LocalConnector) OpenMedia(_ context.Context, r Request, o Item, span MediaRange) (io.ReadCloser, error) {
	if err := span.validate(o.Size); err != nil {
		return nil, err
	}
	file, err := c.open(r, o)
	if err != nil {
		return nil, err
	}
	body := &localMediaBody{file: file, reader: io.NewSectionReader(file, span.Offset, span.Length), remaining: span.Length}
	if span.Offset == 0 && span.Length == o.Size {
		body.digest, body.expected = sha256.New(), o.Revision
	}
	return body, nil
}

// StableMediaRanges holds because a digest-named object has one valid content.
// A complete read verifies that digest, and ServeMedia pins it at publication.
func (*LocalConnector) StableMediaRanges() bool { return true }

// MediaContentScope binds verified digests to the resolved store directory.
func (c *LocalConnector) MediaContentScope() string { return ProviderLocal + "|" + c.Root }

type localMediaBody struct {
	file      *os.File
	reader    io.Reader
	digest    hash.Hash
	expected  string
	remaining int64
}

func (b *localMediaBody) Read(p []byte) (int, error) {
	n, err := b.reader.Read(p)
	b.remaining -= int64(n)
	if b.digest != nil && n > 0 {
		_, _ = b.digest.Write(p[:n])
	}
	return n, err
}

func (b *localMediaBody) Close() error {
	err := b.file.Close()
	if err != nil {
		return err
	}
	if b.remaining != 0 {
		return fmt.Errorf("local object ended before the requested range")
	}
	if b.digest != nil {
		if actual := hex.EncodeToString(b.digest.Sum(nil)); actual != b.expected {
			return &RevisionChangedError{Expected: b.expected, Actual: actual}
		}
	}
	return nil
}

// PutLocalObject stores content under its digest. Writing identical bytes is
// idempotent, so callers can retry an upload without creating a second object.
// ext includes its leading dot and selects the MIME type used at publication.
func PutLocalObject(root, prefix, ext string, content io.Reader) (LocalObject, error) {
	if ext != "" && !localObjectName.MatchString(strings.Repeat("0", 64)+ext) {
		return LocalObject{}, fmt.Errorf("%w: invalid local object extension", ErrInvalidInput)
	}
	dir, err := localStoreDir(prefix)
	if err != nil {
		return LocalObject{}, err
	}
	store, err := os.OpenRoot(root)
	if err != nil {
		return LocalObject{}, err
	}
	defer store.Close()
	if err := store.MkdirAll(dir, 0o700); err != nil {
		return LocalObject{}, err
	}
	temp := path.Join(dir, "."+rand.Text()+".tmp")
	file, err := store.OpenFile(temp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return LocalObject{}, err
	}
	committed := false
	defer func() {
		if !committed {
			file.Close()
			_ = store.Remove(temp)
		}
	}()
	digest := sha256.New()
	size, err := io.Copy(io.MultiWriter(file, digest), content)
	if err != nil {
		return LocalObject{}, err
	}
	if err := file.Sync(); err != nil {
		return LocalObject{}, err
	}
	if err := file.Close(); err != nil {
		return LocalObject{}, err
	}
	sum := hex.EncodeToString(digest.Sum(nil))
	rel := path.Join(dir, sum+ext)
	if err := store.Rename(temp, rel); err != nil {
		return LocalObject{}, err
	}
	committed = true
	directory, err := store.Open(dir)
	if err != nil {
		return LocalObject{}, err
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return LocalObject{}, err
	}
	return LocalObject{Path: rel, SHA256: sum, Size: size}, nil
}
