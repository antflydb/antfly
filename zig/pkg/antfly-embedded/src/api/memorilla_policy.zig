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

//! Request-local latent-memory admission. This module DOES NOT authorize reads
//! or manufacture snapshot/coverage proofs. Trusted adapters supply observations
//! from the authenticated source owner, including live lifecycle checks. Never
//! deserialize these structs from a public request. No digest is a bearer grant.
//! Re-read authority and the entire input closure before every model stage and
//! before publishing a response; immutable data snapshots do not freeze grants,
//! forgetting, deletion, or expiry. The adapter owns those live reads and their
//! race/fence semantics. No cross-table serializable guarantee is implied here.
const std = @import("std");
pub const Digest = [32]u8;
pub const hard_max_inputs = 4096;
pub const maximum_id_bytes = 4096;

pub const Scope = struct {
    /// Effective authenticated tenant/database namespace, not a client label.
    namespace_binding: Digest,
    table_id: u64,
    principal: []const u8,
    /// Opaque binding of currently verified authority. An adapter may include
    /// real auth/policy revisions when available; no invented epoch is needed.
    authorization_binding: Digest,
};

pub const Identity = struct {
    producer_identity: Digest,
    recipe_identity: Digest,
    generation: u64,
    /// Exact owner-issued source cut (and any source-selection/filter identity).
    source_snapshot: Digest,
};

pub const Coverage = struct {
    identity: Identity,
    /// Counts describe the admitted source-unit domain, not physical ANN nodes
    /// or global inaccessible documents. Chunk streams must count their units.
    expected: u64,
    produced: u64,
    skipped: u64,
    failed: u64,
    pending: u64,
    enumerated: bool,
    /// Current accepted producer publication, not merely replay EOF/ready ANN.
    publication_current: bool,
};

pub const Candidate = struct {
    /// Stable artifact/unit identity, unique within this plan.
    id: []const u8,
    source_revision: u64,
    source_digest: Digest,
    artifact_source_revision: u64,
    artifact_source_digest: Digest,
    /// Exact accepted vector payload/proof, not just its parent source hash.
    artifact_digest: Digest,
    artifact_generation: u64,
    producer_identity: Digest,
    recipe_identity: Digest,
    /// Results of authoritative live source checks. Explicit values required.
    authorized: bool,
    current: bool,
    retained: bool,
    expires_at_ms: ?i64,
};

pub const Snapshot = struct {
    scope: Scope,
    identity: Identity,
    coverage: Coverage,
    candidates: []const Candidate,
};

pub const Selection = enum { complete, bounded_subset };
pub const Options = struct {
    max_inputs: usize = hard_max_inputs,
    /// The adapter selects the subset deterministically. This module never
    /// silently truncates a collection or ranks inaccessible candidates.
    selection: Selection = .complete,
};
pub const Fallback = enum { empty, incomplete_coverage, input_budget_exceeded };
pub const Plan = struct {
    scope_digest: Digest,
    identity_digest: Digest,
    input_digest: Digest,
    selection: Selection,
    selected_inputs: usize,
    eligible_inputs: u64,
    omitted_inputs: u64,
    /// A bounded subset remains labelled as a subset, even if it selects all.
    complete: bool,
    options: Options,
};
pub const Decision = union(enum) { ready: Plan, fallback: Fallback };

fn nonzero(digest: Digest) bool {
    return !std.mem.allEqual(u8, &digest, 0);
}
fn number(hash: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, &bytes, value, .little);
    hash.update(&bytes);
}
fn frame(hash: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    number(hash, value.len);
    hash.update(value);
}
fn identityDigest(identity: Identity) !Digest {
    if (identity.generation == 0 or !nonzero(identity.producer_identity) or
        !nonzero(identity.recipe_identity) or !nonzero(identity.source_snapshot)) return error.MissingSourceIdentity;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:memorilla-source:v1:");
    hash.update(&identity.producer_identity);
    hash.update(&identity.recipe_identity);
    number(&hash, identity.generation);
    hash.update(&identity.source_snapshot);
    return hash.finalResult();
}
fn scopeDigest(scope: Scope) !Digest {
    if (scope.table_id == 0 or scope.principal.len == 0 or scope.principal.len > maximum_id_bytes or
        std.mem.indexOfScalar(u8, scope.principal, 0) != null or
        !nonzero(scope.namespace_binding) or !nonzero(scope.authorization_binding)) return error.MissingAuthorityBinding;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:memorilla-authority:v1:");
    hash.update(&scope.namespace_binding);
    number(&hash, scope.table_id);
    frame(&hash, scope.principal);
    hash.update(&scope.authorization_binding);
    return hash.finalResult();
}

/// Adapters validate the live authority binding before opening raw source
/// reads; admission repeats this check when producing the immutable plan.
pub fn validateScope(scope: Scope) !void {
    _ = try scopeDigest(scope);
}

pub fn admit(snapshot: Snapshot, options: Options, now_ms: i64) !Decision {
    if (options.max_inputs == 0 or options.max_inputs > hard_max_inputs) return error.InvalidInputBudget;
    const scope = try scopeDigest(snapshot.scope);
    const identity = try identityDigest(snapshot.identity);
    if (!std.mem.eql(u8, &identity, &try identityDigest(snapshot.coverage.identity))) return error.CoverageIdentityMismatch;
    // Bound work before traversal; no fallback dispatches inputs to GPU.
    if (snapshot.candidates.len > hard_max_inputs) return .{ .fallback = .input_budget_exceeded };
    var ids: [hard_max_inputs]Digest = undefined;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly:memorilla-input-closure:v1:");
    number(&hash, snapshot.candidates.len);
    for (snapshot.candidates, 0..) |candidate, i| {
        if (!candidate.authorized) return error.SourceAccessDenied;
        if (!candidate.retained) return error.SourceUnavailable;
        if (!candidate.current) return error.SourceChanged;
        if (candidate.expires_at_ms) |expiry| if (expiry <= now_ms) return error.SourceExpired;
        if (candidate.id.len == 0 or candidate.id.len > maximum_id_bytes or
            std.mem.indexOfScalar(u8, candidate.id, 0) != null or !nonzero(candidate.source_digest) or
            !nonzero(candidate.artifact_digest)) return error.InvalidSourceInput;
        if (candidate.artifact_generation != snapshot.identity.generation or
            !std.mem.eql(u8, &candidate.producer_identity, &snapshot.identity.producer_identity) or
            !std.mem.eql(u8, &candidate.recipe_identity, &snapshot.identity.recipe_identity)) return error.ArtifactIdentityMismatch;
        if (candidate.source_revision != candidate.artifact_source_revision or
            !std.mem.eql(u8, &candidate.source_digest, &candidate.artifact_source_digest)) return error.StaleArtifact;
        // Hash then sort IDs to bound duplicate validation by N log N rather
        // than comparing long common-prefix IDs pairwise. A collision rejects.
        std.crypto.hash.sha2.Sha256.hash(candidate.id, &ids[i], .{});
        frame(&hash, candidate.id);
        number(&hash, candidate.source_revision);
        hash.update(&candidate.source_digest);
        hash.update(&candidate.artifact_digest);
        // Expiry is part of the admitted closure even before its deadline.
        hash.update(&.{@intFromBool(candidate.expires_at_ms != null)});
        if (candidate.expires_at_ms) |expiry| number(&hash, @bitCast(expiry));
    }
    const id_hashes = ids[0..snapshot.candidates.len];
    std.mem.sort(Digest, id_hashes, {}, struct {
        fn less(_: void, a: Digest, b: Digest) bool {
            return std.mem.order(u8, &a, &b) == .lt;
        }
    }.less);
    for (id_hashes, 0..) |id, i| if (i != 0 and std.mem.eql(u8, &id_hashes[i - 1], &id)) return error.DuplicateSourceInput;
    const coverage = snapshot.coverage;
    // Readiness/watermark progress alone is insufficient: strict collection
    // memory needs an accepted vector for every admitted source unit.
    if (!coverage.enumerated or !coverage.publication_current or coverage.skipped != 0 or
        coverage.failed != 0 or coverage.pending != 0 or coverage.produced != coverage.expected)
        return .{ .fallback = .incomplete_coverage };
    if (snapshot.candidates.len > coverage.produced) return error.InvalidCoverage;
    if (options.selection == .complete and snapshot.candidates.len != coverage.produced)
        return .{ .fallback = .incomplete_coverage };
    if (snapshot.candidates.len > options.max_inputs) return .{ .fallback = .input_budget_exceeded };
    if (snapshot.candidates.len == 0) return .{ .fallback = .empty };
    return .{ .ready = .{
        .scope_digest = scope,
        .identity_digest = identity,
        .input_digest = hash.finalResult(),
        .selection = options.selection,
        .selected_inputs = snapshot.candidates.len,
        .eligible_inputs = coverage.produced,
        .omitted_inputs = coverage.produced - snapshot.candidates.len,
        .complete = options.selection == .complete,
        .options = options,
    } };
}

/// Call with freshly authorized observations at generation start, every model
/// handoff, and response publication. The caller must not recycle admission's
/// booleans or bind a principal grant to the immutable data snapshot.
pub fn liveBoundary(plan: Plan, current: Snapshot, now_ms: i64) !void {
    const decision = try admit(current, plan.options, now_ms);
    const live = switch (decision) {
        .fallback => return error.CoverageChanged,
        .ready => |value| value,
    };
    if (!std.mem.eql(u8, &plan.scope_digest, &live.scope_digest)) return error.AuthorityChanged;
    if (!std.mem.eql(u8, &plan.identity_digest, &live.identity_digest)) return error.SourceSnapshotChanged;
    if (!std.mem.eql(u8, &plan.input_digest, &live.input_digest) or
        plan.selected_inputs != live.selected_inputs or plan.eligible_inputs != live.eligible_inputs or
        plan.omitted_inputs != live.omitted_inputs or plan.selection != live.selection or plan.complete != live.complete)
        return error.SourceClosureChanged;
}

fn fixture() Snapshot {
    const identity: Identity = .{ .producer_identity = @splat(2), .recipe_identity = @splat(3), .generation = 4, .source_snapshot = @splat(5) };
    return .{
        .scope = .{ .namespace_binding = @splat(1), .table_id = 7, .principal = "alice", .authorization_binding = @splat(8) },
        .identity = identity,
        .coverage = .{ .identity = identity, .expected = 1, .produced = 1, .skipped = 0, .failed = 0, .pending = 0, .enumerated = true, .publication_current = true },
        .candidates = &.{.{ .id = "doc/unit", .source_revision = 9, .source_digest = @splat(10), .artifact_source_revision = 9, .artifact_source_digest = @splat(10), .artifact_digest = @splat(11), .artifact_generation = 4, .producer_identity = @splat(2), .recipe_identity = @splat(3), .authorized = true, .current = true, .retained = true, .expires_at_ms = 100 }},
    };
}

test "complete admission and all model boundaries preserve the immutable closure" {
    const snapshot = fixture();
    const plan = (try admit(snapshot, .{}, 10)).ready;
    try std.testing.expect(plan.complete);
    try std.testing.expectEqual(@as(u64, 0), plan.omitted_inputs);
    try liveBoundary(plan, snapshot, 20);
    try liveBoundary(plan, snapshot, 99);
    try std.testing.expectError(error.SourceExpired, liveBoundary(plan, snapshot, 100));
}

test "live principal tenant policy catalog and source changes fence inference" {
    const snapshot = fixture();
    const plan = (try admit(snapshot, .{}, 10)).ready;
    var live = snapshot;
    live.scope.principal = "bob";
    try std.testing.expectError(error.AuthorityChanged, liveBoundary(plan, live, 20));
    live = snapshot;
    live.scope.namespace_binding = @splat(11);
    try std.testing.expectError(error.AuthorityChanged, liveBoundary(plan, live, 20));
    live = snapshot;
    live.scope.authorization_binding = @splat(11);
    try std.testing.expectError(error.AuthorityChanged, liveBoundary(plan, live, 20));
    live = snapshot;
    live.identity.source_snapshot = @splat(11);
    live.coverage.identity = live.identity;
    try std.testing.expectError(error.SourceSnapshotChanged, liveBoundary(plan, live, 20));
    live = snapshot;
    live.scope.authorization_binding = @splat(0);
    try std.testing.expectError(error.MissingAuthorityBinding, liveBoundary(plan, live, 20));
}

test "lifecycle and artifact binding failures reject before GPU" {
    const snapshot = fixture();
    var candidate = snapshot.candidates[0];
    var live = snapshot;
    live.candidates = (&candidate)[0..1];
    candidate.authorized = false;
    try std.testing.expectError(error.SourceAccessDenied, admit(live, .{}, 10));
    candidate = snapshot.candidates[0];
    candidate.retained = false;
    try std.testing.expectError(error.SourceUnavailable, admit(live, .{}, 10));
    candidate = snapshot.candidates[0];
    candidate.current = false;
    try std.testing.expectError(error.SourceChanged, admit(live, .{}, 10));
    candidate = snapshot.candidates[0];
    candidate.artifact_source_revision += 1;
    try std.testing.expectError(error.StaleArtifact, admit(live, .{}, 10));
    candidate = snapshot.candidates[0];
    candidate.artifact_source_digest = @splat(1);
    try std.testing.expectError(error.StaleArtifact, admit(live, .{}, 10));
    candidate = snapshot.candidates[0];
    candidate.artifact_generation += 1;
    try std.testing.expectError(error.ArtifactIdentityMismatch, admit(live, .{}, 10));
    candidate = snapshot.candidates[0];
    candidate.recipe_identity = @splat(1);
    try std.testing.expectError(error.ArtifactIdentityMismatch, admit(live, .{}, 10));
}

test "progressive and best effort publication never imply complete memory" {
    const snapshot = fixture();
    inline for (.{ "skipped", "failed", "pending" }) |name| {
        var live = snapshot;
        @field(live.coverage, name) = 1;
        try std.testing.expectEqual(Fallback.incomplete_coverage, (try admit(live, .{}, 10)).fallback);
    }
    inline for (.{ "enumerated", "publication_current" }) |name| {
        var live = snapshot;
        @field(live.coverage, name) = false;
        try std.testing.expectEqual(Fallback.incomplete_coverage, (try admit(live, .{}, 10)).fallback);
    }
    var live = snapshot;
    live.coverage.expected = 2;
    try std.testing.expectEqual(Fallback.incomplete_coverage, (try admit(live, .{}, 10)).fallback);
    live = snapshot;
    live.coverage.identity.generation += 1;
    try std.testing.expectError(error.CoverageIdentityMismatch, admit(live, .{}, 10));
}

test "subset is explicit bounded labelled and cannot conceal missing producer coverage" {
    var snapshot = fixture();
    snapshot.coverage.expected = 100;
    snapshot.coverage.produced = 100;
    try std.testing.expectEqual(Fallback.incomplete_coverage, (try admit(snapshot, .{}, 10)).fallback);
    const plan = (try admit(snapshot, .{ .selection = .bounded_subset, .max_inputs = 1 }, 10)).ready;
    try std.testing.expect(!plan.complete);
    try std.testing.expectEqual(@as(u64, 99), plan.omitted_inputs);
    try liveBoundary(plan, snapshot, 20);
    snapshot.coverage.produced = 99;
    try std.testing.expectError(error.CoverageChanged, liveBoundary(plan, snapshot, 20));
    snapshot = fixture();
    const all_subset = (try admit(snapshot, .{ .selection = .bounded_subset }, 10)).ready;
    try std.testing.expect(!all_subset.complete);
    try std.testing.expectEqual(@as(u64, 0), all_subset.omitted_inputs);
}

test "input budget duplicates empty scope and changed closure fail closed" {
    const original = fixture();
    try std.testing.expectError(error.InvalidInputBudget, admit(original, .{ .max_inputs = 0 }, 10));
    try std.testing.expectError(error.InvalidInputBudget, admit(original, .{ .max_inputs = hard_max_inputs + 1 }, 10));
    var snapshot = original;
    var rows = [_]Candidate{ original.candidates[0], original.candidates[0] };
    snapshot.candidates = &rows;
    snapshot.coverage.expected = 2;
    snapshot.coverage.produced = 2;
    try std.testing.expectError(error.DuplicateSourceInput, admit(snapshot, .{}, 10));
    rows[1].id = "doc2/unit";
    try std.testing.expectEqual(Fallback.input_budget_exceeded, (try admit(snapshot, .{ .max_inputs = 1 }, 10)).fallback);
    const plan = (try admit(snapshot, .{}, 10)).ready;
    std.mem.swap(Candidate, &rows[0], &rows[1]);
    try std.testing.expectError(error.SourceClosureChanged, liveBoundary(plan, snapshot, 10));
    snapshot = original;
    snapshot.candidates = &.{};
    snapshot.coverage.expected = 0;
    snapshot.coverage.produced = 0;
    try std.testing.expectEqual(Fallback.empty, (try admit(snapshot, .{}, 10)).fallback);
    snapshot = original;
    snapshot.scope.principal = "";
    try std.testing.expectError(error.MissingAuthorityBinding, admit(snapshot, .{}, 10));
}

test "deterministic stage simulation fences revoke deletion expiry and artifact replacement" {
    const snapshot = fixture();
    const plan = (try admit(snapshot, .{}, 10)).ready;
    // Each event happens after encoder completion and before module admission.
    // The same immutable source cut remains pinned throughout the simulation.
    inline for (.{ "revoke", "delete", "forget", "expire", "replace" }) |event| {
        var candidate = snapshot.candidates[0];
        var live = snapshot;
        live.candidates = (&candidate)[0..1];
        var model_calls: usize = 0;
        try liveBoundary(plan, live, 20);
        model_calls += 1; // admitted encoder
        var now_ms: i64 = 21;
        const expected = if (comptime std.mem.eql(u8, event, "revoke")) blk: {
            candidate.authorized = false;
            break :blk error.SourceAccessDenied;
        } else if (comptime std.mem.eql(u8, event, "delete") or std.mem.eql(u8, event, "forget")) blk: {
            candidate.retained = false;
            break :blk error.SourceUnavailable;
        } else if (comptime std.mem.eql(u8, event, "expire")) blk: {
            now_ms = 100;
            break :blk error.SourceExpired;
        } else blk: {
            candidate.artifact_digest = @splat(12);
            break :blk error.SourceClosureChanged;
        };
        var boundary_error: ?anyerror = null;
        liveBoundary(plan, live, now_ms) catch |err| {
            boundary_error = err;
        };
        if (boundary_error == null) model_calls += 1;
        try std.testing.expectEqual(@as(?anyerror, expected), boundary_error);
        try std.testing.expectEqual(@as(usize, 1), model_calls);
        // Response publication repeats the live check rather than releasing
        // encoder-derived bytes merely because a prior stage was admitted.
        try std.testing.expectError(expected, liveBoundary(plan, live, now_ms));
    }
}
