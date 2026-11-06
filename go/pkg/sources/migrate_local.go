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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/minio/minio-go/v7"
)

// MigrateLocalOptions configures a one-shot move of one local-provider
// source's content-addressed objects to a bucket, and the subsequent repoint
// of its catalog records there.
//
// This operates on the same durable state directory as "antfly-source serve"
// and reuses that service's own admission, Runner and Publisher machinery
// (the ordinary "PUT /v1/sources/{id}" and "POST /v1/sync" admin calls,
// driven in-process) rather than writing catalog or content rows a new way.
// Opening the service here takes its process-wide file lock, so this refuses
// to start -- rather than racing it -- while serve, or another migrate-local
// run, already holds the directory. Stop serve first; restart it afterward
// so it picks up the repointed connection.
type MigrateLocalOptions struct {
	// Profile seeds state the first time this state directory is used,
	// exactly like ServiceOptions.Profile; a previously persisted profile
	// always takes precedence.
	Profile    Profile
	StateDir   string
	PublicURL  string
	AdminToken string
	MediaKey   []byte
	Factory    Factory

	// SourceID names a configured source. Its connection must currently be
	// either FromConnection (not yet migrated) or ToConnection (a previous
	// run already repointed it; rerunning finds nothing new to copy and
	// skips the repoint).
	SourceID string
	// FromConnection names a configured connection with provider "local"
	// that objects are read from. This is independent of the source's own
	// current connection so a rerun after a successful migration -- which
	// repoints the source to ToConnection -- still knows where the original
	// local store is, for example to pick up a stray object that arrived
	// after the cutover.
	FromConnection string
	// FromPrefix is the local store's existing prefix (its LocalConfig
	// "prefix"), independent of whatever the source's current Config says.
	FromPrefix string
	// ToConnection names a configured connection with provider "s3" that
	// objects are copied to. It must already be present in the persisted
	// profile (register it with serve's /v1/connections admin call, or
	// include it in Profile before the state directory's first use).
	ToConnection string
	ToBucket     string
	// ToPrefix is the destination key prefix. Objects are written content-
	// addressed under it, at "<to-prefix>/objects/sha256/<digest>[.ext]",
	// the same layout the local store itself uses.
	ToPrefix string
	// DryRun reports counts and bytes without copying, writing or deleting
	// anything.
	DryRun bool
	// Progress, if set, is called once per object as the copy proceeds.
	Progress func(MigrateLocalObject)
}

// MigrateLocalObject reports what happened to one object during the copy
// pass.
type MigrateLocalObject struct {
	// Path is the object's path within the local store (e.g.
	// "objects/sha256/<digest>.ext").
	Path                   string
	Digest, DestinationKey string
	Size                   int64
	// Status is one of: copied, skipped (already present at the destination
	// and verified), would_copy, would_skip (dry run), or failed.
	Status, Error string
}

// MigrateLocalResult is the final summary of one migrate-local run.
type MigrateLocalResult struct {
	DryRun                                                   bool
	SourceID, FromConnection, ToConnection, Bucket, ToPrefix string
	Objects                                                  []MigrateLocalObject
	Copied, Skipped, Failed                                  int
	BytesCopied, BytesSkipped                                int64
	// CatalogRepointed is true once every object copied cleanly and the
	// source's connection/config were switched to the destination and
	// resynced. It is never true while DryRun or Failed > 0.
	CatalogRepointed bool
	// ReferencesUpdated approximates the number of catalog rows the repoint
	// sync now publishes against the bucket (copied plus already-verified
	// objects).
	ReferencesUpdated int
	// RunID is the underlying service sync job ID, for cross-referencing
	// serve's own run history.
	RunID string
}

// MigrateLocal copies a local source's objects to a bucket connection,
// verifies every copy by digest, and (unless DryRun or any object failed)
// repoints the source's catalog records there. It never deletes or modifies
// anything in the local store.
func MigrateLocal(ctx context.Context, opts MigrateLocalOptions) (MigrateLocalResult, error) {
	result := MigrateLocalResult{DryRun: opts.DryRun, SourceID: opts.SourceID, FromConnection: opts.FromConnection, ToConnection: opts.ToConnection, Bucket: opts.ToBucket, ToPrefix: opts.ToPrefix}
	if opts.StateDir == "" || opts.SourceID == "" || opts.FromConnection == "" || opts.ToConnection == "" || opts.ToBucket == "" {
		return result, fmt.Errorf("%w: state-dir, source, from-connection, to-connection and to-bucket are required", ErrInvalidInput)
	}
	if !canonicalID(opts.SourceID) {
		return result, fmt.Errorf("%w: source must be a UUID", ErrInvalidInput)
	}
	if _, err := localStoreDir(opts.FromPrefix); err != nil {
		return result, err
	}
	if _, err := localStoreDir(opts.ToPrefix); err != nil {
		return result, err
	}

	service, err := NewService(ServiceOptions{Profile: opts.Profile, StateDir: opts.StateDir, PublicURL: opts.PublicURL, AdminToken: opts.AdminToken, MediaKey: opts.MediaKey, Factory: opts.Factory})
	if err != nil {
		return result, fmt.Errorf("open source service state (is serve already running against this state directory?): %w", err)
	}
	defer service.Close()

	service.mu.Lock()
	var source Source
	found := false
	for _, candidate := range service.profile.Sources {
		if candidate.ID == opts.SourceID {
			source, found = candidate, true
			break
		}
	}
	var fromConnection, toConnection Connection
	var haveFrom, haveTo bool
	fromConnection, haveFrom = service.profile.Connections[opts.FromConnection]
	toConnection, haveTo = service.profile.Connections[opts.ToConnection]
	service.mu.Unlock()

	if !found {
		return result, fmt.Errorf("%w: source %s is not configured", ErrInvalidInput, opts.SourceID)
	}
	// The source's own connection is checked against both ends rather than
	// requiring it to still be local: a rerun after a successful migration
	// leaves the source pointed at ToConnection, and migrate-local must stay
	// idempotent against that -- for example to copy a stray object that
	// reached the local store after the cutover.
	if source.Connection != opts.FromConnection && source.Connection != opts.ToConnection {
		return result, fmt.Errorf("%w: source %s's connection %q is neither --from-connection %q nor --to-connection %q", ErrInvalidInput, opts.SourceID, source.Connection, opts.FromConnection, opts.ToConnection)
	}
	if !haveFrom || fromConnection.Provider != ProviderLocal {
		return result, fmt.Errorf("%w: connection %q is not configured with provider \"local\"", ErrInvalidInput, opts.FromConnection)
	}
	if !haveTo {
		return result, fmt.Errorf("%w: connection %q is not configured; register it before migrating", ErrInvalidInput, opts.ToConnection)
	}
	if toConnection.Provider != ProviderS3 {
		return result, fmt.Errorf("%w: migrate-local only supports S3-compatible destinations today (connection %q has provider %q); GCS has no upload path in this package yet", ErrInvalidInput, opts.ToConnection, toConnection.Provider)
	}

	localConnector, err := service.providerConnector(ctx, fromConnection)
	if err != nil {
		return result, fmt.Errorf("open local connection: %w", err)
	}
	local, ok := localConnector.(*LocalConnector)
	if !ok {
		return result, fmt.Errorf("connection %s did not resolve to a local store", opts.FromConnection)
	}
	destConnector, err := service.providerConnector(ctx, toConnection)
	if err != nil {
		return result, fmt.Errorf("open destination connection: %w", err)
	}
	dest, ok := destConnector.(*S3Connector)
	if !ok {
		return result, fmt.Errorf("connection %s did not resolve to an S3 client", opts.ToConnection)
	}

	localConfig, err := json.Marshal(LocalConfig{Prefix: opts.FromPrefix})
	if err != nil {
		return result, err
	}
	request := Request{SourceNamespace: "job/" + source.ID, Config: localConfig, Mode: ImportModeSource}
	copyResult, err := MigrateLocalCopy(ctx, local, request, dest, opts.ToBucket, opts.ToPrefix, opts.DryRun, opts.Progress)
	result.Objects, result.Copied, result.Skipped, result.Failed = copyResult.Objects, copyResult.Copied, copyResult.Skipped, copyResult.Failed
	result.BytesCopied, result.BytesSkipped = copyResult.BytesCopied, copyResult.BytesSkipped
	if err != nil {
		return result, err
	}
	if opts.DryRun {
		return result, nil
	}
	if result.Failed > 0 {
		return result, fmt.Errorf("migrate-local: %d of %d object(s) failed verification; the catalog was not repointed and nothing was deleted locally", result.Failed, len(result.Objects))
	}

	destConfig, err := json.Marshal(S3Config{Bucket: opts.ToBucket, Prefix: opts.ToPrefix})
	if err != nil {
		return result, err
	}
	alreadyPointed := source.Connection == opts.ToConnection && string(bytes.TrimSpace(source.Config)) == string(bytes.TrimSpace(destConfig))
	if alreadyPointed && result.Copied == 0 {
		// Already pointed at this exact destination, and this run copied no
		// new object (a clean rerun): nothing changed, so there is nothing
		// to republish.
		result.CatalogRepointed = true
		return result, nil
	}
	updated := source
	updated.Connection, updated.Config = opts.ToConnection, destConfig
	job, err := service.migrateRepoint(ctx, source.ID, updated)
	if err != nil {
		return result, err
	}
	result.CatalogRepointed = true
	result.ReferencesUpdated = result.Copied + result.Skipped
	result.RunID = job.ID
	return result, nil
}

// migrateRepoint drives the service's own admin surface in-process: it PUTs
// the reconfigured source (the same call an operator or Colony would make
// through "/sources/v1" or the legacy "/v1/sources/{id}" route) and then
// POSTs "/v1/sync" for just that source, so the resync runs through the
// ordinary Runner/Publisher/MediaReference pipeline with its usual ownership
// and publication barriers. It polls the resulting job to completion.
func (s *Service) migrateRepoint(ctx context.Context, sourceID string, updated Source) (SyncJob, error) {
	out, err := serviceAdminCall(ctx, s, http.MethodPut, "/v1/sources/"+sourceID, updated)
	if err != nil {
		return SyncJob{}, err
	}
	if out.status != http.StatusOK {
		return SyncJob{}, fmt.Errorf("repoint source %s: admin PUT failed with HTTP %d", sourceID, out.status)
	}
	out, err = serviceAdminCall(ctx, s, http.MethodPost, "/v1/sync", map[string]any{"source_id": sourceID, "restart": true})
	if err != nil {
		return SyncJob{}, err
	}
	if out.status != http.StatusAccepted {
		return SyncJob{}, fmt.Errorf("start repoint sync for %s: admin POST failed with HTTP %d", sourceID, out.status)
	}
	var job SyncJob
	if err := json.Unmarshal(out.body.Bytes(), &job); err != nil {
		return SyncJob{}, err
	}
	for job.State == "running" {
		select {
		case <-ctx.Done():
			return job, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		out, err = serviceAdminCall(ctx, s, http.MethodGet, "/v1/jobs/"+job.ID, nil)
		if err != nil {
			return job, err
		}
		if out.status != http.StatusOK {
			return job, fmt.Errorf("poll repoint job %s: admin GET failed with HTTP %d", job.ID, out.status)
		}
		if err := json.Unmarshal(out.body.Bytes(), &job); err != nil {
			return job, err
		}
	}
	if job.State != "completed" {
		if job.Failure != nil {
			return job, fmt.Errorf("repoint sync for %s ended %q: phase=%s code=%s", sourceID, job.State, job.Failure.Phase, job.Failure.Code)
		}
		return job, fmt.Errorf("repoint sync for %s ended %q: code=%s", sourceID, job.State, job.Code)
	}
	return job, nil
}

// serviceAdminCall issues one in-process admin request against a Service's
// own ServeHTTP, exactly like an HTTP client would, without opening a socket.
func serviceAdminCall(ctx context.Context, s *Service, method, target string, body any) (*controlResponse, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")
	out := &controlResponse{header: make(http.Header)}
	s.ServeHTTP(out, req)
	return out, nil
}

// MigrateLocalCopy copies every object in a local source's content-addressed
// store to an S3 bucket, preserving the "objects/sha256/<digest>[.ext]"
// layout so the digest embedded in the key -- the migration's identity --
// stays stable. It never deletes or modifies the local store.
//
// Every object this returns as copied or skipped was independently re-read
// from the destination and hashed after the write (or after finding it
// already there), so a corrupt, truncated, or pre-existing-but-wrong object
// is reported failed instead of ever being referenced. Rerunning only
// uploads what is still missing; a clean rerun uploads nothing, though it
// still re-verifies every object's digest, favoring correctness over speed
// for what is meant to be an infrequent maintenance operation.
func MigrateLocalCopy(ctx context.Context, local *LocalConnector, request Request, dest *S3Connector, bucket, prefix string, dryRun bool, progress func(MigrateLocalObject)) (MigrateLocalResult, error) {
	inventory, err := local.Enumerate(ctx, request)
	if err != nil {
		return MigrateLocalResult{}, fmt.Errorf("enumerate local store: %w", err)
	}
	result := MigrateLocalResult{DryRun: dryRun, Bucket: bucket, ToPrefix: prefix}
	for _, item := range inventory.Objects {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		obj := migrateCopyOne(ctx, local, request, item, dest, bucket, prefix, dryRun)
		result.Objects = append(result.Objects, obj)
		switch obj.Status {
		case "copied", "would_copy":
			result.Copied++
			result.BytesCopied += obj.Size
		case "skipped", "would_skip":
			result.Skipped++
			result.BytesSkipped += obj.Size
		case "failed":
			result.Failed++
		}
		if progress != nil {
			progress(obj)
		}
	}
	if result.Failed > 0 {
		return result, fmt.Errorf("%d of %d object(s) failed verification; nothing that referenced them should be published", result.Failed, len(result.Objects))
	}
	return result, nil
}

func migrateDestinationKey(prefix, filename string) (string, error) {
	dir, err := localStoreDir(prefix)
	if err != nil {
		return "", err
	}
	if filename == "" || localObjectName.FindStringSubmatch(filename) == nil {
		return "", fmt.Errorf("%w: invalid local object filename %q", ErrInvalidInput, filename)
	}
	return path.Join(dir, filename), nil
}

func migrateCopyOne(ctx context.Context, local *LocalConnector, request Request, item Item, dest *S3Connector, bucket, prefix string, dryRun bool) MigrateLocalObject {
	obj := MigrateLocalObject{Path: item.Path, Digest: item.Revision, Size: item.Size}
	key, err := migrateDestinationKey(prefix, item.Filename)
	if err != nil {
		obj.Status, obj.Error = "failed", err.Error()
		return obj
	}
	obj.DestinationKey = key
	info, statErr := dest.Client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
	exists := statErr == nil
	if statErr != nil && minio.ToErrorResponse(statErr).StatusCode != http.StatusNotFound {
		obj.Status, obj.Error = "failed", "destination stat failed"
		return obj
	}
	if exists && info.Size != item.Size {
		obj.Status, obj.Error = "failed", fmt.Sprintf("destination object already exists with a different size (%d, expected %d)", info.Size, item.Size)
		return obj
	}
	if dryRun {
		if exists {
			obj.Status = "would_skip"
		} else {
			obj.Status = "would_copy"
		}
		return obj
	}
	if !exists {
		if err := migrateUploadOne(ctx, local, request, item, dest, bucket, key); err != nil {
			obj.Status, obj.Error = "failed", err.Error()
			return obj
		}
	}
	// Verify by digest -- freshly read back from the destination -- before
	// this object is ever counted as available to reference, whether it was
	// just uploaded or was already there from an earlier run.
	if err := migrateVerifyDigest(ctx, dest, bucket, key, item.Revision, item.Size); err != nil {
		obj.Status, obj.Error = "failed", err.Error()
		return obj
	}
	if exists {
		obj.Status = "skipped"
	} else {
		obj.Status = "copied"
	}
	return obj
}

func migrateUploadOne(ctx context.Context, local *LocalConnector, request Request, item Item, dest *S3Connector, bucket, key string) error {
	if item.Size == 0 {
		if err := local.CheckAccess(ctx, request, item); err != nil {
			return fmt.Errorf("read local object: %w", err)
		}
		if _, err := dest.Client.PutObject(ctx, bucket, key, bytes.NewReader(nil), 0, minio.PutObjectOptions{ContentType: MediaMIME(item), DisableContentSha256: true}); err != nil {
			return fmt.Errorf("upload to bucket: %w", err)
		}
		return nil
	}
	body, err := local.OpenMedia(ctx, request, item, MediaRange{Length: item.Size})
	if err != nil {
		return fmt.Errorf("open local object: %w", err)
	}
	// Buffer the (size-bounded, <= maxLocalMediaBytes) object rather than
	// streaming it: a seekable *bytes.Reader lets minio-go send one ordinary
	// signed PUT instead of chunked/streaming transfer encoding, which not
	// every S3-compatible endpoint parses identically.
	data := make([]byte, item.Size)
	_, readErr := io.ReadFull(body, data)
	// Close verifies the complete local object still matched its pinned
	// digest while this read bytes from it.
	closeErr := body.Close()
	if readErr != nil {
		return fmt.Errorf("read local object: %w", readErr)
	}
	if closeErr != nil {
		return fmt.Errorf("local object changed while copying: %w", closeErr)
	}
	if _, err := dest.Client.PutObject(ctx, bucket, key, bytes.NewReader(data), item.Size, minio.PutObjectOptions{ContentType: MediaMIME(item), DisableContentSha256: true}); err != nil {
		return fmt.Errorf("upload to bucket: %w", err)
	}
	return nil
}

// migrateVerifyDigest independently re-reads the destination object and
// hashes it, rather than trusting whatever the write path (or a prior,
// unrelated writer) claims landed there.
func migrateVerifyDigest(ctx context.Context, dest *S3Connector, bucket, key, digest string, size int64) error {
	if size == 0 {
		info, err := dest.Client.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
		if err != nil {
			return fmt.Errorf("stat destination object: %w", err)
		}
		if info.Size != 0 {
			return fmt.Errorf("destination object is not empty")
		}
		return nil
	}
	obj, err := dest.Client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("read destination object: %w", err)
	}
	defer obj.Close()
	h := sha256.New()
	n, err := io.Copy(h, obj)
	if err != nil {
		return fmt.Errorf("read destination object: %w", err)
	}
	if n != size {
		return fmt.Errorf("destination object has %d bytes, expected %d", n, size)
	}
	if actual := hex.EncodeToString(h.Sum(nil)); actual != digest {
		return &RevisionChangedError{Expected: digest, Actual: actual}
	}
	return nil
}
