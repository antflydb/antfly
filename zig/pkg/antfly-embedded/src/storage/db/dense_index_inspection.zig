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
//! Called under DB's apply-shared lock. All source heads, publication guards,
//! vectors and recipe identities belong to this same immutable read cut.
const std = @import("std");
const types = @import("types.zig");
const keys = @import("../internal_keys.zig");
const ids = @import("artifact_ids.zig");
const codec = @import("enrichment/artifact_codec.zig");
const privacy = @import("privacy_read.zig");
const contract = @import("../../privacy/contract.zig");
const manager = @import("catalog/index_manager.zig");
const Store = @import("../docstore.zig").DocStore;
const A = std.mem.Allocator;
const max_vectors = 128;
const max_values = 65536;
const max_source_bytes = 1024 * 1024;

const Vector = struct {
    artifact_id: []const u8,
    producer: []const u8,
    provider: []const u8,
    model: []const u8,
    producer_fingerprint: []const u8,
    source_text: []const u8,
    source_fingerprint: []const u8,
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
    current: bool,
    incomplete_reason: ?[]const u8 = null,
    vectors: []const Vector = &.{},
};

/// Existing asset names take precedence. A dense index name otherwise selects
/// this versioned raw inspection; summary responses never serialize vectors.
pub fn inspect(db: anytype, output: A, document: []const u8, name: []const u8) !?types.DocumentArtifactManifest {
    if (document.len > 4096 or name.len > 4096) return error.ResourceBudgetExceeded;
    const entry = db.core.index_manager.denseIndex(name) orelse return null;
    var arena = std.heap.ArenaAllocator.init(output);
    defer arena.deinit();
    const a = arena.allocator();
    if (try db.core.getEnrichment(a, .asset, name)) |configured| {
        var asset = configured;
        asset.deinit(a);
        return null;
    }
    const policy = (try privacy.capture(db, a)) orelse return null;
    if (policy.prepared.policy.mode != .redact_for_search_and_ai) return null;
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    const primary = try keys.documentKeyAlloc(a, document);
    const original = read.get(primary) catch |err| if (err == error.NotFound) return null else return err;
    if (original.len > max_source_bytes) return error.ResourceBudgetExceeded;
    // Never put originals in the inspection. Failure to obtain the currently
    // published sanitized root remains a serving failure, not an empty vector.
    const safe = try privacy.project(a, policy, document, original, &read);
    if (safe.len > max_source_bytes) return error.ResourceBudgetExceeded;
    var snapshot: Snapshot = .{
        .index = name,
        .index_generation = entry.config.coverage_generation,
        .index_fingerprint = try digest(a, entry.config.config_json),
        .document_id = document,
        .policy_version = &policy.prepared.version,
        .document_json = safe,
        .document_fingerprint = try digest(a, safe),
        .current = true,
    };
    _ = privacy.requirePublished(db, a, .{}) catch |err| switch (err) {
        error.IndexRebuilding => {
            snapshot.current = false;
            snapshot.incomplete_reason = "IndexRebuilding";
        },
        else => return err,
    };
    var vectors: std.ArrayList(Vector) = .empty;
    var values: usize = 0;
    var source_bytes: usize = safe.len;
    if (snapshot.current) {
        const producers = try manager.denseConfigArtifactNamesAlloc(a, entry.config);
        if (producers.len > 16) return error.ResourceBudgetExceeded;
        for (producers) |producer| {
            var producer_scratch = std.heap.ArenaAllocator.init(output);
            defer producer_scratch.deinit();
            const producer_alloc = producer_scratch.allocator();
            var cfg = (try db.core.getEnrichment(producer_alloc, .embedding, producer)) orelse return error.InvalidIndexConfiguration;
            defer cfg.deinit(producer_alloc);
            if (cfg.source_artifact_name.len == 0 or cfg.embedding_input != .text or cfg.template.len != 0 or cfg.source != null or cfg.prior_context != null or cfg.neighbor_context != null or !std.mem.eql(u8, cfg.field, "text")) return null;
            if (producer.len > 4096 or cfg.source_artifact_name.len > 4096 or cfg.producer_json.len > max_source_bytes) return error.ResourceBudgetExceeded;
            const config = try std.json.parseFromSlice(std.json.Value, producer_alloc, cfg.producer_json, .{});
            const provider_value = string(config.value, "provider");
            const model_value = string(config.value, "model");
            if (provider_value.len > 4096 or model_value.len > 4096) return error.ResourceBudgetExceeded;
            const provider = try a.dupe(u8, provider_value);
            const model = try a.dupe(u8, model_value);
            const producer_fingerprint = try digest(a, cfg.producer_json);
            var chunks = try @import("artifact_chunk_cursor.zig").Cursor(Store.Txn).openNamed(a, &read, document, cfg.source_artifact_name);
            defer chunks.close();
            while (try chunks.next()) |chunk| {
                if (vectors.items.len == max_vectors or chunk.value.len > max_source_bytes -| source_bytes) return error.ResourceBudgetExceeded;
                source_bytes += chunk.value.len;
                var proof_scratch = std.heap.ArenaAllocator.init(output);
                defer proof_scratch.deinit();
                const transient = proof_scratch.allocator();
                const text = (try @import("../../chunking/chunk.zig").artifactTextAlloc(transient, chunk.value, cfg.field)) orelse return error.InvalidChunkArtifact;
                const key = try keys.derivedEmbeddingArtifactKeyAlloc(a, chunk.key, producer);
                const raw = try privacy.projectArtifact(transient, policy, key, &read);
                const header = codec.decodeHeader(raw) catch return null;
                if (header.flags.vector_elided) return null;
                const dims = try codec.decodeDenseEmbeddingDims(raw);
                if (dims > max_values -| values) return error.ResourceBudgetExceeded;
                values += dims;
                const vector = codec.decodeDenseEmbeddingAlloc(a, raw) catch |err| if (err == error.VectorElided) return null else return err;
                if (vector.len != entry.dims) return error.InvalidIndexConfiguration;
                var ref = (try ids.decodeArtifactRefAlloc(transient, key)) orelse return error.InvalidArgument;
                defer ref.deinit(transient);
                if (ref.unit_id) |unit| if (unit.len > 4096) return error.ResourceBudgetExceeded;
                if (ref.source) |source| if (source.unit_id) |unit| if (unit.len > 4096) return error.ResourceBudgetExceeded;
                try vectors.append(a, .{
                    .artifact_id = try ids.artifactPublicIdAlloc(a, ref),
                    .producer = producer,
                    .provider = provider,
                    .model = model,
                    .producer_fingerprint = producer_fingerprint,
                    .source_text = try a.dupe(u8, text),
                    .source_fingerprint = try digest(a, text),
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
    var result: types.DocumentArtifactManifest = .{
        .document_id = try output.dupe(u8, document),
        .artifact_name = &.{},
        .artifact_id = &.{},
        .manifest_json = &.{},
        .manifest_version = 1,
        .generation = snapshot.index_generation,
        .chunk_count = vectors.items.len,
    };
    errdefer result.deinit(output);
    result.artifact_name = try output.dupe(u8, name);
    // The index inspection has no single physical artifact ID. Individual
    // immutable member identities are carried in the versioned raw payload.
    result.manifest_json = try std.json.Stringify.valueAlloc(output, snapshot, .{});
    result.source_fingerprint = try output.dupe(u8, snapshot.document_fingerprint);
    result.content_type = try output.dupe(u8, "application/vnd.antfly.dense-index-inspection+json");
    result.route_type = try output.dupe(u8, "dense_index_inspection");
    return result;
}

fn string(value: std.json.Value, field: []const u8) []const u8 {
    if (value != .object) return "";
    const found = value.object.get(field) orelse return "";
    return if (found == .string) found.string else "";
}

fn digest(a: A, bytes: []const u8) ![]const u8 {
    return a.dupe(u8, &contract.fingerprint(bytes));
}
