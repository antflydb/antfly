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

//! Administrator artifact inspection for the current dense index recipe.
//! Called under DB's publication admission gates and apply-shared lock.
//! All source heads, publication guards,
//! vectors and recipe identities belong to this same immutable read cut.
const std = @import("std");
const types = @import("types.zig");
const keys = @import("../internal_keys.zig");
const ids = @import("artifact_ids.zig");
const codec = @import("enrichment/artifact_codec.zig");
const privacy = @import("privacy_read.zig");
const generation = @import("artifact_extraction_generation.zig");
const contract = @import("../../privacy/contract.zig");
const manager = @import("catalog/index_manager.zig");
const Store = @import("../docstore.zig").DocStore;
const A = std.mem.Allocator;
const scratchAllocator = @import("antfly_platform").allocator.reclaimingScratch;
const builtin = @import("builtin");
pub const Hook = struct { context: *anyopaque, run: *const fn (*anyopaque) anyerror!void };
pub var test_after_open: ?Hook = null;
pub var test_before_render: ?Hook = null;
const limits = types.dense_index_inspection_limits;
const max_vectors = limits.vectors;
const max_values = limits.float_values;
const max_source_bytes = limits.source_bytes;

const Vector = struct {
    artifact_id: []const u8,
    producer: []const u8,
    provider: []const u8,
    model: []const u8,
    producer_fingerprint: []const u8,
    source_text: []const u8,
    source_fingerprint: []const u8,
    artifact_fingerprint: []const u8,
    provider_cache_identity: []const u8,
    values: []const f32,
};
const Snapshot = struct {
    format: []const u8 = "antfly.dense-index-inspection.v1",
    index: []const u8,
    index_generation: u64,
    index_fingerprint: []const u8,
    document_id: []const u8,
    policy_version: []const u8,
    document_json: []const u8,
    document_fingerprint: []const u8,
    publication_authority: []const u8,
    index_config_hash: u64,
    checkpoint_config_hash: u64,
    checkpoint_status: []const u8,
    checkpoint_generation: u64,
    applied_sequence: u64,
    replay_target_sequence: u64,
    current: bool,
    incomplete_reason: ?[]const u8 = null,
    vectors: []const Vector = &.{},
};

/// Existing asset names take precedence. A dense index name otherwise selects
/// this versioned raw inspection; summary responses never serialize vectors.
pub fn capture(db: anytype, output: A, document: []const u8, name: []const u8) !?Captured {
    var scan_budget: @import("artifact_scan_budget.zig").Budget = .{
        .max_visits = limits.scan_visits,
        .max_bytes = limits.scan_bytes,
        .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s,
    };
    try scan_budget.check();
    // Inspection budgets apply only to this optional dense-index view. A
    // missing asset must remain absent for every otherwise legal document ID.
    const entry = db.core.index_manager.denseIndex(name) orelse return null;
    if (document.len > limits.identity_bytes or name.len > limits.identity_bytes) return error.ResourceBudgetExceeded;
    const backing = scratchAllocator(output);
    var arena = std.heap.ArenaAllocator.init(backing);
    var owned = true;
    defer if (owned) arena.deinit();
    const a = arena.allocator();
    if (try db.core.getEnrichment(a, .asset, name)) |configured| {
        var asset = configured;
        asset.deinit(a);
        return null;
    }
    const policy = (try privacy.captureWithBudget(db, a, &scan_budget)) orelse return null;
    if (policy.prepared.policy.mode != .redact_for_search_and_ai) return null;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    if (builtin.is_test) if (test_after_open) |hook| try hook.run(hook.context);
    const primary = try keys.documentKeyAlloc(a, document);
    const original = read.get(primary) catch |err| if (err == error.NotFound) return null else return err;
    if (original.len > max_source_bytes) return error.ResourceBudgetExceeded;
    // Never put originals in the inspection. Failure to obtain the currently
    // published sanitized root remains a serving failure, not an empty vector.
    const safe = try privacy.project(a, policy, document, original, &read);
    if (safe.len > max_source_bytes) return error.ResourceBudgetExceeded;
    const local = policy.native and try @import("../source_authority.zig").maintenanceMode(&read) == .local and try @import("artifact_publication.zig").authority(&read) == null;
    const checkpoint = try db.core.loadProjectionCheckpoint(a, name);
    var applied = try db.core.loadAppliedSequence(a, name);
    if (db.executor.appliedSequence(name)) |live| applied = @max(applied, live);
    const target = try db.managedIndexReplayTargetSequence(a, name, entry.config.kind, applied);
    var snapshot: Snapshot = .{
        .index = try a.dupe(u8, name),
        .index_generation = entry.config.coverage_generation,
        .index_fingerprint = try digest(a, entry.config.config_json),
        .document_id = try a.dupe(u8, document),
        .policy_version = try a.dupe(u8, &policy.prepared.version),
        .document_json = safe,
        .document_fingerprint = try digest(a, safe),
        .publication_authority = if (local) "standalone_guarded_coverage" else "ordered_causal_provenance",
        .index_config_hash = types.indexConfigHash(entry.config),
        .checkpoint_config_hash = checkpoint.config_hash,
        .checkpoint_status = @tagName(checkpoint.status),
        .checkpoint_generation = checkpoint.generation,
        .applied_sequence = applied,
        .replay_target_sequence = target,
        .current = true,
    };
    _ = privacy.requireConsumersPublishedWithBudget(db, a, .{}, &.{name}, &scan_budget) catch |err| switch (err) {
        error.IndexRebuilding => {
            snapshot.current = false;
            snapshot.incomplete_reason = "IndexRebuilding";
        },
        else => return err,
    };
    if (snapshot.current and (checkpoint.status != .clean or checkpoint.config_hash != snapshot.index_config_hash or applied < target)) {
        snapshot.current = false;
        snapshot.incomplete_reason = "ProjectionPending";
    }
    var vectors: std.ArrayList(Vector) = .empty;
    var values: usize = 0;
    var source_bytes: usize = safe.len;
    if (snapshot.current) {
        try scan_budget.consume(entry.config.config_json.len);
        const producers = try manager.denseConfigArtifactNamesAlloc(a, entry.config);
        if (producers.len > limits.producers) return error.ResourceBudgetExceeded;
        for (producers) |producer| {
            var producer_scratch = std.heap.ArenaAllocator.init(backing);
            defer producer_scratch.deinit();
            const producer_alloc = producer_scratch.allocator();
            var cfg = (try db.core.getEnrichment(producer_alloc, .embedding, producer)) orelse return error.InvalidIndexConfiguration;
            defer cfg.deinit(producer_alloc);
            if (cfg.source_artifact_name.len == 0 or cfg.embedding_input != .text or cfg.template.len != 0 or cfg.source != null or cfg.prior_context != null or cfg.neighbor_context != null or !std.mem.eql(u8, cfg.field, "text")) return null;
            if (producer.len > limits.identity_bytes or cfg.source_artifact_name.len > limits.identity_bytes or cfg.producer_json.len > limits.producer_config_bytes) return error.ResourceBudgetExceeded;
            try scan_budget.consume(cfg.producer_json.len);
            const config = try std.json.parseFromSlice(std.json.Value, producer_alloc, cfg.producer_json, .{});
            const provider_value = string(config.value, "provider");
            const model_value = string(config.value, "model");
            if (provider_value.len > limits.identity_bytes or model_value.len > limits.identity_bytes) return error.ResourceBudgetExceeded;
            const provider = try a.dupe(u8, provider_value);
            const model = try a.dupe(u8, model_value);
            const producer_fingerprint = try digest(a, cfg.producer_json);
            var chunks = try @import("artifact_chunk_cursor.zig").Cursor(Store.Txn).openNamed(producer_alloc, &read, document, cfg.source_artifact_name);
            defer chunks.close();
            while (true) {
                const chunk = switch (try chunks.poll(&scan_budget)) {
                    .end => break,
                    .yielded => {
                        if (@import("antfly_platform").time.monotonicNs() >= scan_budget.deadline_ns.?) return error.DeadlineExceeded;
                        return error.ResourceBudgetExceeded;
                    },
                    .row => |row| row,
                };
                if (vectors.items.len == max_vectors or chunk.value.len > max_source_bytes -| source_bytes) return error.ResourceBudgetExceeded;
                source_bytes += chunk.value.len;
                var proof_scratch = std.heap.ArenaAllocator.init(backing);
                defer proof_scratch.deinit();
                const transient = proof_scratch.allocator();
                const text = (try @import("../../chunking/chunk.zig").artifactTextAlloc(transient, chunk.value, cfg.field)) orelse return error.InvalidChunkArtifact;
                const key = try keys.derivedEmbeddingArtifactKeyAlloc(a, chunk.key, producer);
                // Standalone's guarded writes enroll and settle the exact
                // source generation in coverage, checked above under the same
                // apply fence. It does not mint asset privacy witnesses for
                // vectors. Ordered owners additionally need per-output causal
                // provenance; neither path treats provider bytes as authority.
                var input = try generation.captureInput(transient, &read, key);
                defer input.deinit();
                const raw = if (local)
                    input.value orelse return error.IndexRebuilding
                else
                    try privacy.projectArtifact(transient, policy, key, &read);
                const header = codec.decodeHeader(raw) catch return null;
                if (header.flags.vector_elided) return null;
                if (!header.flags.has_source_hash or header.flags.authored) return error.EnrichmentSourceChanged;
                const dims = try codec.decodeDenseEmbeddingDims(raw);
                if (dims > max_values -| values) return error.ResourceBudgetExceeded;
                values += dims;
                const vector = codec.decodeDenseEmbeddingAlloc(a, raw) catch |err| if (err == error.VectorElided) return null else return err;
                if (vector.len != entry.dims) return error.InvalidIndexConfiguration;
                var ref = (try ids.decodeArtifactRefAlloc(transient, key)) orelse return error.InvalidArgument;
                defer ref.deinit(transient);
                if (ref.unit_id) |unit| if (unit.len > limits.identity_bytes) return error.ResourceBudgetExceeded;
                if (ref.source) |source| if (source.unit_id) |unit| if (unit.len > limits.identity_bytes) return error.ResourceBudgetExceeded;
                try vectors.append(a, .{
                    .artifact_id = try ids.artifactPublicIdAlloc(a, ref),
                    .producer = producer,
                    .provider = provider,
                    .model = model,
                    .producer_fingerprint = producer_fingerprint,
                    .source_text = try a.dupe(u8, text),
                    .source_fingerprint = try digest(a, text),
                    .artifact_fingerprint = try digest(a, raw),
                    .provider_cache_identity = try std.fmt.allocPrint(a, "{x:0>16}", .{header.source_hash}),
                    .values = vector,
                });
            }
        }
        snapshot.vectors = vectors.items;
        if (vectors.items.len == 0) {
            snapshot.current = false;
            snapshot.incomplete_reason = "NoEmbeddingArtifacts";
        }
    }
    try scan_budget.check();
    owned = false;
    return .{ .arena = arena, .snapshot = snapshot };
}

/// Payload ownership is independent of DB admission. Encode only after the
/// caller has released publication gates and apply ownership.
pub const Captured = struct {
    arena: std.heap.ArenaAllocator,
    snapshot: Snapshot,
    pub fn deinit(self: *Captured) void {
        self.arena.deinit();
    }
    pub fn render(self: *const Captured, output: A) !types.DocumentArtifactManifest {
        if (builtin.is_test) if (test_before_render) |hook| try hook.run(hook.context);
        const snapshot = self.snapshot;
        var result: types.DocumentArtifactManifest = .{
            .document_id = try output.dupe(u8, snapshot.document_id),
            .artifact_name = &.{},
            .artifact_id = &.{},
            .manifest_json = &.{},
            .manifest_version = 1,
            .generation = snapshot.index_generation,
            .chunk_count = snapshot.vectors.len,
        };
        errdefer result.deinit(output);
        result.artifact_name = try output.dupe(u8, snapshot.index);
        // The index inspection has no single physical artifact ID. Individual
        // immutable member identities are carried in the versioned raw payload.
        result.manifest_json = try std.json.Stringify.valueAlloc(output, snapshot, .{});
        result.source_fingerprint = try output.dupe(u8, snapshot.document_fingerprint);
        result.content_type = try output.dupe(u8, "application/vnd.antfly.dense-index-inspection+json");
        result.route_type = try output.dupe(u8, "dense_index_inspection");
        return result;
    }
};

fn string(value: std.json.Value, field: []const u8) []const u8 {
    if (value != .object) return "";
    const found = value.object.get(field) orelse return "";
    return if (found == .string) found.string else "";
}

fn digest(a: A, bytes: []const u8) ![]const u8 {
    return a.dupe(u8, &contract.fingerprint(bytes));
}
