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

//! Snapshot-bound source projection. Accepted ordered provenance binds the
//! exact primary source, admitted catalog and published output independently.
//! Callers must additionally fence their index generation before ranking.
const std = @import("std");
const document = @import("../../privacy/document.zig");
const contract = @import("../../privacy/contract.zig");
const serving = @import("../../privacy/serving.zig");
const keys = @import("../internal_keys.zig");
const A = std.mem.Allocator;

pub const PolicySnapshot = struct {
    producer_json: []const u8,
    artifact_name: []const u8,
    policy: contract.Policy,
    native: bool = false,
};

/// The caller owns the allocator (normally a request arena). Do not retain
/// borrowed enrichment catalog pointers after dropping the catalog lock.
pub fn capture(db: anytype, a: A) !?PolicySnapshot {
    const manager = db.core.index_manager;
    manager.catalog_mutex.lockShared();
    const guard = manager.getEnrichment(.asset, document.state_artifact_name);
    const cfg = guard orelse for (manager.enrichments.items) |*entry| {
        if (entry.kind == .asset and std.mem.eql(u8, entry.source_field, document.source_field) and document.isArtifactName(entry.name)) break entry;
    } else {
        manager.catalog_mutex.unlockShared();
        return null;
    };
    const artifact_name = a.dupe(u8, cfg.name) catch |err| {
        manager.catalog_mutex.unlockShared();
        return err;
    };
    const raw = a.dupe(u8, cfg.producer_json) catch |err| {
        manager.catalog_mutex.unlockShared();
        return err;
    };
    manager.catalog_mutex.unlockShared();
    const parsed = try std.json.parseFromSlice(std.json.Value, a, raw, .{});
    const config = parsed.value.object.get("config") orelse return error.InvalidPrivacyProducer;
    const value = config.object.get(document.config_key) orelse return null;
    const policy = try std.json.parseFromValue(contract.Policy, a, value, .{});
    try policy.value.validate();
    return .{ .producer_json = raw, .artifact_name = if (guard != null) (if (policy.value.mode == .redact_for_search_and_ai) try document.protectedArtifactName(a, try policy.value.identity(a)) else document.artifact_name) else artifact_name, .policy = policy.value, .native = !try db.maintenanceRequiresOrderedApply() };
}

pub fn requireExpected(a: A, expected: ?[64]u8, actual: ?PolicySnapshot) !void {
    const version = expected orelse return;
    if (std.mem.allEqual(u8, &version, 0)) return;
    const current = actual orelse return error.IndexRebuilding;
    if (current.policy.mode != .redact_for_search_and_ai or !std.mem.eql(u8, &(try current.policy.identity(a)), &version)) return error.IndexRebuilding;
}

pub fn project(a: A, snapshot: PolicySnapshot, key: []const u8, source: []const u8, read: anytype) ![]u8 {
    if (snapshot.policy.mode == .monitor) return a.dupe(u8, source);
    const original = try std.json.parseFromSlice(std.json.Value, a, source, .{ .parse_numbers = false });
    const canonical = try std.json.Stringify.valueAlloc(a, original.value, .{});
    const artifact_key = try keys.artifactNamedPrefixAlloc(a, key, "asset", snapshot.artifact_name);
    var input = try @import("artifact_extraction_generation.zig").captureInput(a, read, artifact_key);
    defer input.deinit();
    const artifact = input.value orelse return error.PrivacyPending;
    try requireArtifactPublication(a, snapshot, canonical, artifact_key, artifact, input.proofKey(artifact_key), input.proofValue(), read);
    // Generation 1 denotes this immutable policy identity. Index-generation
    // readiness belongs to the ranking adapter, never to artifact metadata.
    return serving.project(a, snapshot.policy, 1, canonical, artifact, .{
        .generation = 1,
        .policy_version = try snapshot.policy.identity(a),
        .source_fingerprint = contract.fingerprint(canonical),
        .projection_fingerprint = contract.fingerprint(artifact),
        .state = .published,
    });
}

pub fn observation(a: A, snapshot: PolicySnapshot, key: []const u8, source: []const u8, read: anytype) !@import("../../privacy/status.zig").Observation {
    const original = try std.json.parseFromSlice(std.json.Value, a, source, .{ .parse_numbers = false });
    if (original.value != .object) return error.InvalidPrivacyDocument;
    for (snapshot.policy.fields) |field| {
        const value = original.value.object.get(field) orelse .null;
        if (value != .null and value != .string) return .{ .failed = true, .error_code = try a.dupe(u8, "InvalidPrivacyFieldType") };
    }
    const artifact_key = try keys.artifactNamedPrefixAlloc(a, key, "asset", snapshot.artifact_name);
    var input = try @import("artifact_extraction_generation.zig").captureInput(a, read, artifact_key);
    defer input.deinit();
    const bytes = input.value orelse return failureObservation(a, snapshot, key, source, read);
    const canonical = try std.json.Stringify.valueAlloc(a, original.value, .{});
    requireArtifactPublication(a, snapshot, canonical, artifact_key, bytes, input.proofKey(artifact_key), input.proofValue(), read) catch |err| {
        if (pending(err)) return failureObservation(a, snapshot, key, source, read);
        return err;
    };
    var observed = try @import("../../privacy/status.zig").inspect(a, snapshot.policy, source, bytes);
    if (observed.current) return observed;
    observed.deinit(a);
    return failureObservation(a, snapshot, key, source, read);
}

fn requireArtifactPublication(a: A, snapshot: PolicySnapshot, source: []const u8, artifact: []const u8, output: []const u8, proof_key: []const u8, proof_value: ?[]const u8, read: anytype) !void {
    if (snapshot.native and try @import("../source_authority.zig").maintenanceMode(read) == .local and try @import("artifact_publication.zig").authority(read) == null) {
        return @import("../../privacy/local_publication.zig").require(a, read, artifact, try snapshot.policy.identity(a), source, output);
    }
    var accepted = (try @import("artifact_producer_provenance.zig").readCurrentForArtifact(a, read, proof_key, proof_value)) orelse return error.PrivacyPublicationPending;
    defer accepted.deinit();
    if (!std.mem.eql(u8, accepted.proof.producer_name, snapshot.artifact_name)) return error.InvalidPrivacyReceipt;
}

fn failureObservation(a: A, snapshot: PolicySnapshot, key: []const u8, source: []const u8, read: anytype) !@import("../../privacy/status.zig").Observation {
    var state_key: std.ArrayListUnmanaged(u8) = .empty;
    defer state_key.deinit(a);
    try keys.appendDocumentPrefix(&state_key, a, key);
    try state_key.append(a, keys.asset_state_kind);
    try keys.appendEncodedComponent(&state_key, a, snapshot.artifact_name);
    const value = read.get(state_key.items) catch |err| if (err == error.NotFound) return .{} else return err;
    if (value.len == 0 or value[0] != '{') return .{};
    const Marker = struct { privacy_failure: struct { sequence: u64 = 0, policy_version: []const u8, source_fingerprint: []const u8, error_code: []const u8 } };
    var marker = std.json.parseFromSlice(Marker, a, value, .{}) catch return .{};
    defer marker.deinit();
    var original = try std.json.parseFromSlice(std.json.Value, a, source, .{ .parse_numbers = false });
    defer original.deinit();
    const canonical = try std.json.Stringify.valueAlloc(a, original.value, .{});
    defer a.free(canonical);
    const failure = marker.value.privacy_failure;
    if (!try @import("../../privacy/retry.zig").current(a, read, state_key.items, failure.sequence)) return .{};
    if (!std.mem.eql(u8, failure.policy_version, &(try snapshot.policy.identity(a))) or !std.mem.eql(u8, failure.source_fingerprint, &contract.fingerprint(canonical))) return .{};
    return .{ .failed = true, .error_code = try a.dupe(u8, failure.error_code) };
}

pub fn pending(err: anyerror) bool {
    return err == error.PrivacyPending or err == error.PrivacyStale or
        err == error.PrivacyInspectionFailed or err == error.PrivacyPublicationPending or
        err == error.EnrichmentSourceChanged or err == error.ArtifactPublicationPending or err == error.ArtifactCatalogDrift;
}

/// Derived bytes are safe only when accepted under the active policy-owned
/// producer graph. Orphaned pre-policy chunks/assets remain physically present
/// for GC but cannot be read through their old public IDs.
pub fn projectArtifact(a: A, snapshot: PolicySnapshot, physical: []const u8, read: anytype) ![]u8 {
    var input = try @import("artifact_extraction_generation.zig").captureInput(a, read, physical);
    defer input.deinit();
    const raw = input.value orelse return error.PrivacyPending;
    if (snapshot.native and try @import("../source_authority.zig").maintenanceMode(read) == .local and try @import("artifact_publication.zig").authority(read) == null) {
        var ref = (try @import("artifact_ids.zig").decodeArtifactRefAlloc(a, physical)) orelse return error.UnsupportedQueryRequest;
        defer ref.deinit(a);
        const root = std.mem.eql(u8, ref.name, snapshot.artifact_name);
        const version = try snapshot.policy.identity(a);
        if (!root and !std.meta.eql(document.consumerPolicyVersion(ref.name), @as(?[64]u8, version))) return error.UnsupportedQueryRequest;
        const primary = try keys.documentKeyAlloc(a, ref.document_id);
        const source = read.get(primary) catch |err| if (err == error.NotFound) return error.PrivacyStale else return err;
        if (root) return project(a, snapshot, ref.document_id, source, read);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, source, .{ .parse_numbers = false });
        const canonical = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
        try @import("../../privacy/local_publication.zig").require(a, read, physical, version, canonical, raw);
        // Independently require the current root, even when the descendant's
        // own publication is current. These bytes never grant original access.
        _ = try project(a, snapshot, ref.document_id, source, read);
        return a.dupe(u8, raw);
    }
    var proof = (try @import("artifact_producer_provenance.zig").readCurrentForArtifact(a, read, input.proofKey(physical), input.proofValue())) orelse return error.PrivacyPublicationPending;
    defer proof.deinit();
    const name = proof.proof.producer_artifact_name;
    const version = try snapshot.policy.identity(a);
    const prefix = try std.fmt.allocPrint(a, "_privacy_{s}_", .{version});
    if (std.mem.eql(u8, name, snapshot.artifact_name)) {
        if (proof.proof.sources.len != 1) return error.InvalidPrivacyReceipt;
        const key = proof.proof.sources[0].document_key;
        const primary = try keys.documentKeyAlloc(a, key);
        const source = read.get(primary) catch |err| switch (err) {
            error.NotFound => return error.PrivacyStale,
            else => return err,
        };
        return project(a, snapshot, key, source, read);
    }
    if (!std.mem.startsWith(u8, name, prefix)) return error.UnsupportedQueryRequest;
    return a.dupe(u8, raw);
}

/// Ranking uses receiver-owned producer summaries, not a corpus scan or an
/// artifact's self-reported receipt. Primary/dependency writes enroll pending
/// requirements in their own transaction; only verified native witnesses can
/// settle them. Bind those summaries and physical consumer checkpoints to the
/// same apply-shared cut before candidates, statistics or totals are read.
pub fn requirePublished(db: anytype, a: A, req: @import("types.zig").SearchRequest) !bool {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const captured = try capture(db, scratch);
    try requireExpected(scratch, req.privacy_policy_version, captured);
    const policy = captured orelse return false;
    if (policy.policy.mode == .monitor) return false;
    if (req.cancellation) |control| try control.check();
    if (req.execution_deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.DeadlineExceeded;
    const special = @import("query/projection.zig").buildLookupFieldSelectionPlan(.{ .fields = req.fields, .include_all_fields = req.include_all_fields }).special;
    if (special.all_artifacts or special.all_chunks or special.all_embeddings) return error.UnsupportedQueryRequest;
    var schema = db.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    if (schema) |view| if (view.storageMode() == .relational) return error.UnsupportedQueryRequest;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    var plan = try db.core.index_manager.acquireWritePlanSnapshot();
    defer plan.release();
    const requirements = if (plan.plan().completion_plan) |*value| value else return error.IndexRebuilding;
    const local = policy.native and try @import("../source_authority.zig").maintenanceMode(&read) == .local and try @import("artifact_publication.zig").authority(&read) == null;
    var root_found = false;
    for (requirements.nodes) |node| {
        if (node.kind != .generated and node.kind != .unit_children) continue;
        if (std.mem.eql(u8, node.artifact, policy.artifact_name)) root_found = true;
        if (!local and node.artifact.len != 0 and !try @import("artifact_producer_readiness.zig").sourceComplete(scratch, &read, plan.plan(), db.root_incarnation, node.artifact)) return error.IndexRebuilding;
        if (!local and node.embedding.len != 0 and !try @import("artifact_producer_readiness.zig").sourceComplete(scratch, &read, plan.plan(), db.root_incarnation, node.embedding)) return error.IndexRebuilding;
    }
    if (!root_found) return error.IndexRebuilding;
    inline for (.{ "text_indexes", "dense_indexes", "sparse_indexes" }) |collection| {
        for (@field(db.core.index_manager, collection).items) |*entry| {
            const name = entry.config.name;
            if (local and !try db.privacyLocalIndexSourcesComplete(scratch, &read, name, entry.config.kind, plan.plan())) return error.IndexRebuilding;
            if (db.core.index_manager.loadFailure(name) != null) return error.IndexRebuilding;
            var applied = try db.core.loadAppliedSequence(scratch, name);
            if (db.executor.appliedSequence(name)) |live| applied = @max(applied, live);
            const target = try db.managedIndexReplayTargetSequence(scratch, name, entry.config.kind, applied);
            if (applied < target) return error.IndexRebuilding;
            const checkpoint = try db.core.loadProjectionCheckpoint(scratch, name);
            if (checkpoint.status != .clean or checkpoint.config_hash != @import("types.zig").indexConfigHash(entry.config)) return error.IndexRebuilding;
        }
    }
    return true;
}
