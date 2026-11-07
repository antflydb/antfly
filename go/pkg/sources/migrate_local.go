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
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strings"
	"time"
)

// IdentityNotice explains what this run did to row identity, for a CLI to
// print after the run completes (its outcome -- Rekeyed in particular --
// is only known once the repoint PUT has actually happened). Empty for a
// dry run or a run that never reached the catalog.
//
// Identity (IdentityKey: see sync.go) depends only on identity.namespace and
// each row's relative key, never on connection or provider, so a provider
// change by itself never has to re-key anything once a namespace reproduces
// a row's current key. A source that already has a namespace keeps it
// untouched. One that does not yet have one adopts exactly the namespace
// that reproduces its current keys under its still-current connection and
// config (LegacyNamespace, derived before the connection change below takes
// effect); --to-prefix mirrors the local store's own
// "objects/sha256/<digest>[.ext]" layout, so each object's relative key is
// the same before and after the copy. Together these mean an ordinary first
// migration never re-keys anything, even though it is also the first time
// the source gets a namespace.
//
// Row identity is not the whole story, though: whether derived data (OCR,
// chunks, embeddings) also survives depends on the destination table's own
// document_extraction config declaring checksum_algorithm for this source's
// rows. migrate-local has no visibility into that table-side config -- it
// is server/Zig configuration, not anything in a source profile -- and
// cannot confirm it either way. Without it, a location-based fingerprint still
// changes (this source's canonical URI and any etag move with the provider
// change), so Antfly still recomputes derived data for a row whose ID did
// not change.
func (r MigrateLocalResult) IdentityNotice() string {
	if r.DryRun || !r.CatalogRepointed {
		return ""
	}
	if r.Rekeyed {
		// Should not happen for an ordinary first migration: migrate-local
		// always tries to adopt the exact namespace that reproduces this
		// source's current keys. A true Rekeyed report here means the
		// server's own verification (service.go) could not confirm that the
		// adopted namespace matches this source's existing rows -- for
		// example because they disagreed on a legacy prefix, or because the
		// server could not enumerate the existing connection to check.
		return "this repoint re-keyed every row: the server could not confirm that the namespace this source adopted reproduces its existing keys, so it was treated as a re-key. This should not happen for an ordinary first migration from a source with consistent existing keys; if it keeps happening, something about this source's current rows does not match what migrate-local derived from its connection and config."
	}
	return "rows were updated in place with the same IDs. Whether derived data -- OCR, chunks, embeddings -- also survived depends on the destination table's document_extraction config declaring checksum_algorithm for this source's rows; migrate-local cannot see that table-side config and cannot confirm it either way. Without it, Antfly still recomputes derived data for a row whose location metadata (URI, etag) changed, even though its ID did not."
}

// MigrateLocalOptions configures a one-shot move of one local-provider
// source's content-addressed objects to a bucket, and the subsequent repoint
// of its catalog records there. Copying and verifying never take a service
// lock and never touch a running "serve": they only read the local store and
// write the destination bucket, so they can run for as long as a large copy
// takes while serve keeps delivering media.
//
// Repointing (the catalog PUT and resync) has two modes:
//
//   - Server set: the repoint is sent over real HTTP to that live serve
//     instance's own admin API ("PUT /v1/sources/{id}", "POST /v1/sync",
//     polled through "GET /v1/jobs/{id}"), so serve's own run admission
//     serializes it with any other run and media delivery is never
//     interrupted by this process holding a lock. A live "sync already in
//     progress" (409) is retried with bounded backoff before failing
//     clearly.
//   - Server empty: the repoint runs in-process against StateDir exactly
//     like "antfly-source serve" would, taking that directory's exclusive
//     file lock for the repoint step only. This is for a stopped service
//     (nothing is being delivered either way); it refuses -- rather than
//     racing -- if serve or another migrate-local run already holds it.
//
// Either way this reuses the ordinary Runner/Publisher/MediaReference
// pipeline and its usual ownership and publication barriers; it never writes
// catalog or content rows a new way. It never deletes or modifies anything
// in the local store.
type MigrateLocalOptions struct {
	// Profile is consulted directly for connection credentials when Server
	// is set (a live serve instance's own connections are read from its
	// "/v1/profile" admin endpoint instead). When Server is empty, Profile
	// seeds state the first time StateDir is used, exactly like
	// ServiceOptions.Profile; a previously persisted profile there always
	// takes precedence.
	Profile    Profile
	StateDir   string
	PublicURL  string
	AdminToken string
	MediaKey   []byte
	Factory    Factory

	// Server, if set, is a live "antfly-source serve" instance's base URL
	// (its --public-url, or any address that reaches the same admin API).
	// The repoint is sent there over real HTTP instead of running in-process
	// against StateDir. StateDir, PublicURL and MediaKey are not required in
	// this mode; the copy step doesn't use them either way.
	Server string

	// SourceID names a configured source. Its connection must currently be
	// either FromConnection (not yet migrated) or ToConnection (a previous
	// run already repointed it; rerunning reuses verified copies and
	// confirms publication again).
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
	// objects are copied to. It must already be present in the profile that
	// governs the repoint (the live server's, or the persisted/seeded one),
	// checked before anything is copied.
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
	// Rekeyed is the repoint PUT's own rekeys signal (see PutSourceMutation):
	// true only when this source had no identity.namespace before this run.
	// See IdentityNotice.
	Rekeyed bool
}

// MigrateLocal copies a local source's objects to a bucket connection,
// verifies every copy by digest, and (unless DryRun or any object failed)
// repoints the source's catalog records there. It never deletes or modifies
// anything in the local store. See MigrateLocalOptions for the two repoint
// modes (a live Server, or an in-process StateDir).
func MigrateLocal(ctx context.Context, opts MigrateLocalOptions) (MigrateLocalResult, error) {
	result := MigrateLocalResult{DryRun: opts.DryRun, SourceID: opts.SourceID, FromConnection: opts.FromConnection, ToConnection: opts.ToConnection, Bucket: opts.ToBucket, ToPrefix: opts.ToPrefix}
	if opts.SourceID == "" || opts.FromConnection == "" || opts.ToConnection == "" || opts.ToBucket == "" {
		return result, fmt.Errorf("%w: source, from-connection, to-connection and to-bucket are required", ErrInvalidInput)
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
	if opts.Server != "" {
		return migrateLocalLive(ctx, opts, result)
	}
	return migrateLocalStopped(ctx, opts, result)
}

// migrateRepointer sends the reconfigured source and resync through some
// admin surface's run admission, and waits for the resulting job to finish.
// rekeys is the PUT response's own signal (see PutSourceMutation): true only
// when the source had no identity.namespace before this call.
type migrateRepointer interface {
	repoint(ctx context.Context, sourceID string, updated Source) (job SyncJob, rekeys bool, err error)
}

// migrateLocalRun is the copy-then-maybe-repoint tail shared by both repoint
// modes, once each has resolved the source, its connections and a
// migrateRepointer of its own.
func migrateLocalRun(ctx context.Context, opts MigrateLocalOptions, result MigrateLocalResult, source Source, local *LocalConnector, dest migrateDestination, toProvider string, repointer migrateRepointer) (MigrateLocalResult, error) {
	// A connection can contain several independent local stores. Validate the
	// selected source before copying, including on dry runs. After cutover its
	// config is bucket-shaped, so mop-up runs keep using the original explicit
	// local prefix, as documented by MigrateLocalOptions.
	if source.Connection == opts.FromConnection {
		config, err := decodeSourceConfig[LocalConfig](source.Config)
		if err != nil {
			return result, fmt.Errorf("read selected source config: %w", err)
		}
		if config.Prefix != opts.FromPrefix {
			return result, fmt.Errorf("%w: from-prefix %q differs from selected source prefix %q", ErrInvalidInput, opts.FromPrefix, config.Prefix)
		}
	}

	localConfig, err := json.Marshal(LocalConfig{Prefix: opts.FromPrefix})
	if err != nil {
		return result, err
	}
	request := Request{SourceNamespace: "job/" + source.ID, Config: localConfig, Mode: ImportModeSource}
	copyResult, err := MigrateLocalCopy(ctx, local, request, dest, opts.ToPrefix, opts.DryRun, opts.Progress)
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

	destConfig, err := migrateDestinationConfig(toProvider, opts.ToBucket, opts.ToPrefix)
	if err != nil {
		return result, err
	}
	// Configuration alone is not a publication checkpoint: a previous run may
	// have committed PUT and then failed (or crashed) before completing sync.
	// Reconfirm publication on every invocation; verified copies are still reused.
	updated := source
	updated.Connection, updated.Config = opts.ToConnection, destConfig
	if updated.Identity.Namespace == "" {
		// Row identity (IdentityKey) depends only on namespace and each
		// row's relative key, never on connection or provider. A source that
		// has never had a namespace is still on the legacy, location-based
		// formula (RecordKey). Adopting the exact namespace that reproduces
		// its CURRENT keys (LegacyNamespace, derived from the local store
		// under its still-current connection/config, before the provider
		// change above takes effect) keeps every existing row's key, so this
		// repoint does not re-key anything even though it is also the first
		// time this source gets a namespace.
		namespace, err := migrateLegacyNamespace(ctx, local, request, source.ID)
		if err != nil {
			return result, fmt.Errorf("determine existing row identity before repointing: %w", err)
		}
		updated.Identity.Namespace = namespace
	}
	job, rekeys, err := repointer.repoint(ctx, source.ID, updated)
	if err != nil {
		return result, err
	}
	result.CatalogRepointed = true
	result.ReferencesUpdated = result.Copied + result.Skipped
	result.RunID = job.ID
	result.Rekeyed = rekeys
	return result, nil
}

// migrateLegacyNamespace enumerates the local store once more (cheap: a
// directory listing, not a copy, independent of MigrateLocalCopy's own
// enumeration) to derive the namespace that reproduces this source's
// current row keys (LegacyNamespace), so adopting it alongside this
// repoint's connection/config change does not also re-key every row. An
// empty store (nothing published under this connection/config yet) has
// nothing to preserve, so it falls back to the source's own ID, the same
// default a brand new source gets at creation.
func migrateLegacyNamespace(ctx context.Context, local *LocalConnector, request Request, sourceID string) (string, error) {
	inventory, err := local.Enumerate(ctx, request)
	if err != nil {
		return "", fmt.Errorf("enumerate local store: %w", err)
	}
	if len(inventory.Objects) == 0 {
		return sourceID, nil
	}
	return LegacyNamespace(sourceID, inventory.Objects)
}

// resolveMigrationSource finds the source and its two connections in
// profile, and checks the source's own connection against both ends, before
// anything is copied.
func resolveMigrationSource(profile Profile, opts MigrateLocalOptions) (Source, Connection, Connection, error) {
	var source Source
	found := false
	for _, candidate := range profile.Sources {
		if candidate.ID == opts.SourceID {
			source, found = candidate, true
			break
		}
	}
	if !found {
		return Source{}, Connection{}, Connection{}, fmt.Errorf("%w: source %s is not configured", ErrInvalidInput, opts.SourceID)
	}
	fromConnection, haveFrom := profile.Connections[opts.FromConnection]
	toConnection, haveTo := profile.Connections[opts.ToConnection]
	// The source's own connection is checked against both ends rather than
	// requiring it to still be local: a rerun after a successful migration
	// leaves the source pointed at ToConnection, and migrate-local must stay
	// idempotent against that -- for example to copy a stray object that
	// reached the local store after the cutover.
	if source.Connection != opts.FromConnection && source.Connection != opts.ToConnection {
		return Source{}, Connection{}, Connection{}, fmt.Errorf("%w: source %s's connection %q is neither --from-connection %q nor --to-connection %q", ErrInvalidInput, opts.SourceID, source.Connection, opts.FromConnection, opts.ToConnection)
	}
	if !haveFrom || fromConnection.Provider != ProviderLocal {
		return Source{}, Connection{}, Connection{}, fmt.Errorf("%w: connection %q is not configured with provider \"local\"", ErrInvalidInput, opts.FromConnection)
	}
	if !haveTo {
		return Source{}, Connection{}, Connection{}, fmt.Errorf("%w: connection %q is not configured; register it before migrating", ErrInvalidInput, opts.ToConnection)
	}
	if toConnection.Provider != ProviderS3 && toConnection.Provider != ProviderGCS {
		return Source{}, Connection{}, Connection{}, fmt.Errorf("%w: migrate-local only supports S3-compatible and GCS destinations (connection %q has provider %q)", ErrInvalidInput, opts.ToConnection, toConnection.Provider)
	}
	return source, fromConnection, toConnection, nil
}

// migrateDestinationConfig builds the source Config a repoint publishes,
// matching whichever connector will enumerate it afterward.
func migrateDestinationConfig(provider, bucket, prefix string) (json.RawMessage, error) {
	// Bucket prefixes are literal byte prefixes. A directory boundary keeps the
	// exact suffix identical to LocalConnector's relative key, including any
	// meaningful leading slash in a bucket object name.
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	switch provider {
	case ProviderS3:
		return json.Marshal(S3Config{Bucket: bucket, Prefix: prefix})
	case ProviderGCS:
		return json.Marshal(GoogleConfig{Bucket: bucket, Prefix: prefix})
	default:
		return nil, fmt.Errorf("%w: unsupported destination provider %q", ErrInvalidInput, provider)
	}
}

// migrateLocalStopped repoints in-process against StateDir, exactly like
// "antfly-source serve" would, holding that directory's exclusive lock for
// the whole operation. Used only when no live Server is given -- nothing is
// being delivered either way, so there is no delivery window to protect.
func migrateLocalStopped(ctx context.Context, opts MigrateLocalOptions, result MigrateLocalResult) (MigrateLocalResult, error) {
	if opts.StateDir == "" || opts.PublicURL == "" {
		return result, fmt.Errorf("%w: state-dir and public-url are required without --server", ErrInvalidInput)
	}
	service, err := NewService(ServiceOptions{Profile: opts.Profile, StateDir: opts.StateDir, PublicURL: opts.PublicURL, AdminToken: opts.AdminToken, MediaKey: opts.MediaKey, Factory: opts.Factory})
	if err != nil {
		return result, fmt.Errorf("open source service state (is serve already running against this state directory?): %w", err)
	}
	defer service.Close()

	service.mu.Lock()
	profile := cloneProfile(service.profile)
	service.mu.Unlock()

	source, fromConnection, toConnection, err := resolveMigrationSource(profile, opts)
	if err != nil {
		return result, err
	}
	localConnector, err := service.providerConnector(ctx, fromConnection)
	if err != nil {
		return result, fmt.Errorf("open local connection: %w", err)
	}
	local, ok := localConnector.(*LocalConnector)
	if !ok {
		return result, fmt.Errorf("connection %s did not resolve to a local store", opts.FromConnection)
	}
	// service.providerConnector resolves an ordinary (read-scoped) source
	// connector, fine for S3; a GCS destination instead needs its own
	// write-scoped client, built directly by migrateOpenDestination using
	// the service's own secret resolution for secret-ref connections.
	dest, err := migrateOpenDestination(ctx, service.providerConnector, toConnection, opts.ToBucket, service.resolveSecret)
	if err != nil {
		return result, fmt.Errorf("open destination connection: %w", err)
	}
	return migrateLocalRun(ctx, opts, result, source, local, dest, toConnection.Provider, service)
}

// migrateLocalLive repoints over real HTTP against a running serve instance,
// so copying and verifying never touch that process's lock and media
// delivery is never interrupted by this process holding it.
func migrateLocalLive(ctx context.Context, opts MigrateLocalOptions, result MigrateLocalResult) (MigrateLocalResult, error) {
	if err := ValidateEndpoint(opts.Server); err != nil {
		return result, fmt.Errorf("invalid --server: %w", err)
	}
	if opts.AdminToken == "" {
		return result, fmt.Errorf("%w: an admin token is required for --server", ErrInvalidInput)
	}
	admin := &liveAdminClient{baseURL: strings.TrimRight(opts.Server, "/"), token: opts.AdminToken, client: &http.Client{Timeout: 30 * time.Second}}
	profile, err := admin.getProfile(ctx)
	if err != nil {
		return result, fmt.Errorf("fetch live profile from %s: %w", opts.Server, err)
	}
	source, fromConnection, toConnection, err := resolveMigrationSource(profile, opts)
	if err != nil {
		return result, err
	}
	// The live service's own connections drive credential resolution here,
	// not Profile, since that is what the source it is about to resync
	// actually uses. Connections keyed by secret-refs (the service's own
	// encrypted secret store, rather than an environment variable) are not
	// resolvable this way; migrate-local against a live server supports
	// only env-var-referenced credentials today.
	factory := opts.Factory
	if factory == nil {
		factory = EnvironmentFactory
	}
	localConnector, err := factory(ctx, fromConnection)
	if err != nil {
		return result, fmt.Errorf("open local connection: %w", err)
	}
	local, ok := localConnector.(*LocalConnector)
	if !ok {
		return result, fmt.Errorf("connection %s did not resolve to a local store", opts.FromConnection)
	}
	// No secret-store resolver in live mode: a GCS or S3 destination
	// connection using secret-refs needs the in-process, stopped-service
	// repoint path instead.
	dest, err := migrateOpenDestination(ctx, factory, toConnection, opts.ToBucket, nil)
	if err != nil {
		return result, fmt.Errorf("open destination connection: %w", err)
	}
	return migrateLocalRun(ctx, opts, result, source, local, dest, toConnection.Provider, admin)
}

// repoint drives the service's own admin surface in-process: it PUTs the
// reconfigured source (the same call an operator or Colony would make
// through "/sources/v1" or the legacy "/v1/sources/{id}" route) and then
// POSTs "/v1/sync" for just that source, so the resync runs through the
// ordinary Runner/Publisher/MediaReference pipeline with its usual ownership
// and publication barriers. It polls the resulting job to completion.
func (s *Service) repoint(ctx context.Context, sourceID string, updated Source) (SyncJob, bool, error) {
	out, err := serviceAdminCall(ctx, s, http.MethodPut, "/v1/sources/"+sourceID, updated)
	if err != nil {
		return SyncJob{}, false, err
	}
	if out.status != http.StatusOK {
		return SyncJob{}, false, fmt.Errorf("repoint source %s: admin PUT failed with HTTP %d", sourceID, out.status)
	}
	var putResponse struct {
		Rekeys bool `json:"rekeys"`
	}
	if err := json.Unmarshal(out.body.Bytes(), &putResponse); err != nil {
		return SyncJob{}, false, err
	}
	out, err = serviceAdminCall(ctx, s, http.MethodPost, "/v1/sync", map[string]any{"source_id": sourceID, "restart": true})
	if err != nil {
		return SyncJob{}, false, err
	}
	if out.status != http.StatusAccepted {
		return SyncJob{}, false, fmt.Errorf("start repoint sync for %s: admin POST failed with HTTP %d", sourceID, out.status)
	}
	var job SyncJob
	if err := json.Unmarshal(out.body.Bytes(), &job); err != nil {
		return SyncJob{}, false, err
	}
	job, err = migrateWaitForJob(ctx, sourceID, job, 20*time.Millisecond, func(ctx context.Context) (int, []byte, error) {
		out, err := serviceAdminCall(ctx, s, http.MethodGet, "/v1/jobs/"+job.ID, nil)
		if err != nil {
			return 0, nil, err
		}
		return out.status, out.body.Bytes(), nil
	})
	return job, putResponse.Rekeys, err
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

// liveAdminClient drives a running serve instance's admin API over real
// HTTP, so its own run admission -- not this process -- serializes the
// repoint with any other run there.
type liveAdminClient struct {
	baseURL, token string
	client         *http.Client
}

// migrateLiveRetryBudget bounds how long a 409 ("a run is already in
// progress") is retried before giving up with a clear error, rather than
// retrying forever. A var, not a const, so tests can shrink it.
var migrateLiveRetryBudget = 2 * time.Minute

func (c *liveAdminClient) call(ctx context.Context, method, path string, body any) (int, []byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return 0, nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return resp.StatusCode, data, nil
}

// callSerialized retries a 409 with bounded backoff: serve's own admission
// serializes this with whatever else is running there, so this waits its
// turn instead of racing it, and fails clearly if that takes too long.
func (c *liveAdminClient) callSerialized(ctx context.Context, method, path string, body any) (int, []byte, error) {
	deadline := time.Now().Add(migrateLiveRetryBudget)
	backoff := 500 * time.Millisecond
	for {
		status, data, err := c.call(ctx, method, path, body)
		if err != nil || status != http.StatusConflict {
			return status, data, err
		}
		if time.Now().After(deadline) {
			return status, data, fmt.Errorf("live server at %s has a run already in progress; gave up retrying after %s", c.baseURL, migrateLiveRetryBudget)
		}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
	}
}

func (c *liveAdminClient) getProfile(ctx context.Context) (Profile, error) {
	status, data, err := c.call(ctx, http.MethodGet, "/v1/profile", nil)
	if err != nil {
		return Profile{}, err
	}
	if status != http.StatusOK {
		return Profile{}, fmt.Errorf("GET /v1/profile: HTTP %d", status)
	}
	var profile Profile
	if err := json.Unmarshal(data, &profile); err != nil {
		return Profile{}, err
	}
	return profile, nil
}

func (c *liveAdminClient) repoint(ctx context.Context, sourceID string, updated Source) (SyncJob, bool, error) {
	status, putBody, err := c.callSerialized(ctx, http.MethodPut, "/v1/sources/"+sourceID, updated)
	if err != nil {
		return SyncJob{}, false, err
	}
	if status != http.StatusOK {
		return SyncJob{}, false, fmt.Errorf("repoint source %s on %s: admin PUT failed with HTTP %d", sourceID, c.baseURL, status)
	}
	var putResponse struct {
		Rekeys bool `json:"rekeys"`
	}
	if err := json.Unmarshal(putBody, &putResponse); err != nil {
		return SyncJob{}, false, err
	}
	status, data, err := c.callSerialized(ctx, http.MethodPost, "/v1/sync", map[string]any{"source_id": sourceID, "restart": true})
	if err != nil {
		return SyncJob{}, false, err
	}
	if status != http.StatusAccepted {
		return SyncJob{}, false, fmt.Errorf("start repoint sync for %s on %s: admin POST failed with HTTP %d", sourceID, c.baseURL, status)
	}
	var job SyncJob
	if err := json.Unmarshal(data, &job); err != nil {
		return SyncJob{}, false, err
	}
	job, err = migrateWaitForJob(ctx, sourceID, job, 300*time.Millisecond, func(ctx context.Context) (int, []byte, error) {
		return c.call(ctx, http.MethodGet, "/v1/jobs/"+job.ID, nil)
	})
	return job, putResponse.Rekeys, err
}

// migrateWaitForJob polls poll at interval until the job it describes
// reaches a terminal state, then turns a non-"completed" outcome into an
// error. Shared by the in-process and live repoint paths, which differ only
// in how they poll and how quickly.
func migrateWaitForJob(ctx context.Context, sourceID string, job SyncJob, interval time.Duration, poll func(context.Context) (int, []byte, error)) (SyncJob, error) {
	for job.State == "running" {
		select {
		case <-ctx.Done():
			return job, ctx.Err()
		case <-time.After(interval):
		}
		status, data, err := poll(ctx)
		if err != nil {
			return job, err
		}
		if status != http.StatusOK {
			return job, fmt.Errorf("poll repoint job %s: HTTP %d", job.ID, status)
		}
		if err := json.Unmarshal(data, &job); err != nil {
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

// MigrateLocalCopy copies every object in a local source's content-addressed
// store to a migrateDestination (an S3-compatible bucket or a GCS bucket),
// preserving the "objects/sha256/<digest>[.ext]" layout so the digest
// embedded in the key -- the migration's identity -- stays stable. It never
// deletes or modifies the local store.
//
// Every object this returns as copied or skipped was independently re-read
// from the destination and hashed after the write (or after finding it
// already there), so a corrupt, truncated, or pre-existing-but-wrong object
// is reported failed instead of ever being referenced. Rerunning only
// uploads what is still missing; a clean rerun uploads nothing, though it
// still re-verifies every object's digest, favoring correctness over speed
// for what is meant to be an infrequent maintenance operation.
func MigrateLocalCopy(ctx context.Context, local *LocalConnector, request Request, dest migrateDestination, prefix string, dryRun bool, progress func(MigrateLocalObject)) (MigrateLocalResult, error) {
	inventory, err := local.Enumerate(ctx, request)
	if err != nil {
		return MigrateLocalResult{}, fmt.Errorf("enumerate local store: %w", err)
	}
	result := MigrateLocalResult{DryRun: dryRun, ToPrefix: prefix}
	for _, item := range inventory.Objects {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		obj := migrateCopyOne(ctx, local, request, item, dest, prefix, dryRun)
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

func migrateCopyOne(ctx context.Context, local *LocalConnector, request Request, item Item, dest migrateDestination, prefix string, dryRun bool) MigrateLocalObject {
	obj := MigrateLocalObject{Path: item.Path, Digest: item.Revision, Size: item.Size}
	key, err := migrateDestinationKey(prefix, item.Filename)
	if err != nil {
		obj.Status, obj.Error = "failed", err.Error()
		return obj
	}
	obj.DestinationKey = key
	size, exists, err := dest.stat(ctx, key)
	if err != nil {
		obj.Status, obj.Error = "failed", "destination stat failed"
		return obj
	}
	if exists && size != item.Size {
		obj.Status, obj.Error = "failed", fmt.Sprintf("destination object already exists with a different size (%d, expected %d)", size, item.Size)
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
		if err := migrateUploadOne(ctx, local, request, item, dest, key); err != nil {
			obj.Status, obj.Error = "failed", err.Error()
			return obj
		}
	}
	// Verify by digest -- freshly read back from the destination -- before
	// this object is ever counted as available to reference, whether it was
	// just uploaded or was already there from an earlier run.
	if err := migrateVerifyDigest(ctx, dest, key, item.Revision, item.Size); err != nil {
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

func migrateUploadOne(ctx context.Context, local *LocalConnector, request Request, item Item, dest migrateDestination, key string) error {
	data, err := migrateReadLocalObject(ctx, local, request, item)
	if err != nil {
		return err
	}
	if err := dest.create(ctx, key, data, MediaMIME(item)); err != nil {
		if errors.Is(err, errMigrateDestinationExists) {
			// A race with another writer created this key between our
			// existence check and this upload; leave it to the digest
			// verification right after (and, if that fails too, to a later
			// run) rather than treating the race itself as a failure.
			return nil
		}
		return fmt.Errorf("upload to bucket: %w", err)
	}
	return nil
}

// migrateReadLocalObject reads one local object's complete, digest-verified
// bytes (local.OpenMedia/Close verify the pinned revision), buffering it
// rather than streaming: every object here is already bounded by the local
// store's own 256 MiB object cap, and both destinations need the complete
// bytes in hand anyway (S3 to avoid chunked transfer encoding, GCS to build
// one multipart request).
func migrateReadLocalObject(ctx context.Context, local *LocalConnector, request Request, item Item) ([]byte, error) {
	if item.Size == 0 {
		// Read validates the digest of empty content as well as access. An
		// empty, truncated file must not be mistaken for a verified object.
		return local.Read(ctx, request, item)
	}
	body, err := local.OpenMedia(ctx, request, item, MediaRange{Length: item.Size})
	if err != nil {
		return nil, fmt.Errorf("open local object: %w", err)
	}
	data := make([]byte, item.Size)
	_, readErr := io.ReadFull(body, data)
	closeErr := body.Close()
	if readErr != nil {
		return nil, fmt.Errorf("read local object: %w", readErr)
	}
	if closeErr != nil {
		return nil, fmt.Errorf("local object changed while copying: %w", closeErr)
	}
	return data, nil
}

// migrateVerifyDigest independently re-reads the destination object and
// hashes it, rather than trusting whatever the write path (or a prior,
// unrelated writer) claims landed there. Both destinations are read back and
// hashed with SHA-256 the same way: GCS's own per-object checksums are
// CRC32C and MD5, neither as strong as SHA-256, so a full read-back-and-hash
// is used uniformly instead of trusting a weaker provider-native checksum on
// one side only.
func migrateVerifyDigest(ctx context.Context, dest migrateDestination, key, digest string, size int64) error {
	if size == 0 {
		statSize, exists, err := dest.stat(ctx, key)
		if err != nil {
			return fmt.Errorf("stat destination object: %w", err)
		}
		if !exists {
			return fmt.Errorf("destination object is missing")
		}
		if statSize != 0 {
			return fmt.Errorf("destination object is not empty")
		}
		sum := sha256.Sum256(nil)
		if actual := hex.EncodeToString(sum[:]); actual != digest {
			return &RevisionChangedError{Expected: digest, Actual: actual}
		}
		return nil
	}
	data, err := dest.read(ctx, key)
	if err != nil {
		return fmt.Errorf("read destination object: %w", err)
	}
	if int64(len(data)) != size {
		return fmt.Errorf("destination object has %d bytes, expected %d", len(data), size)
	}
	sum := sha256.Sum256(data)
	if actual := hex.EncodeToString(sum[:]); actual != digest {
		return &RevisionChangedError{Expected: digest, Actual: actual}
	}
	return nil
}
