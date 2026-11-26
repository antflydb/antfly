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

//! One authenticated chunk result, projected to its immutable catalog consumers.
//! Per-member acceptance is deliberately not document-wide coverage completion.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const keys = @import("../internal_keys.zig");
const manager = @import("catalog/index_manager.zig");
const derived = @import("derived/derived_types.zig");
const codec = @import("enrichment/artifact_codec.zig");
const context = @import("artifact_producer_context.zig");

/// Own the selected provider input and every causal fence before releasing the
/// read snapshot. Provider work must not keep an LSM snapshot alive.
pub const Input = struct {
    token: context.Token,
    value: ?[]const u8,
    output_key: []const u8,
    previous_output_exists: bool = false,
    effect_protocol: u16 = 18,
    /// Empty when the replay cursor proves that the member has no windows.
    old_window_outputs: ?[]const []const u8 = null,
    media_window_layout: bool = false,
    large_window_capture: bool = false,
    pub fn deinit(self: *Input) void {
        self.token.deinit();
        self.* = undefined;
    }
};

pub fn capture(alloc: std.mem.Allocator, txn: anytype, request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, plan: *const manager.IndexManager.WritePlanSnapshot, scope: []const u8) !?Input {
    const bound = (try captureSession(alloc, txn, request, plan)) orelse return null;
    return try bound.capture(alloc, scope);
}

/// Bind catalog authorization once per pinned snapshot, not once per member.
/// The transaction and request are borrowed for this session's lifetime; each
/// captured provider input owns its bytes and can outlive the snapshot.
pub fn CaptureSession(comptime Txn: type) type {
    return struct {
        txn: Txn,
        request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest,
        authority: publication.Authority,
        generation: u64,
        effect_protocol: u16,

        pub fn capture(self: @This(), alloc: std.mem.Allocator, scope: []const u8) !Input {
            return captureMember(alloc, self.txn, self.request, self.authority, self.generation, self.effect_protocol, scope);
        }
    };
}

pub fn captureSession(alloc: std.mem.Allocator, txn: anytype, request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, plan: *const manager.IndexManager.WritePlanSnapshot) !?CaptureSession(@TypeOf(txn)) {
    const authority = (try publication.authority(txn)) orelse return null;
    if ((request.kind != .dense_embedding and request.kind != .sparse_embedding) or
        request.embedding_input != .text or request.neighbor_context_json.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    const matches = for (plan.generated_templates) |template| {
        if (@import("artifact_producer_input.zig").sameDefinition(request, template)) break true;
    } else false;
    if (!matches) return error.EnrichmentSourceChanged;
    const generation = plan.coverageGeneration(request.index_name) orelse return error.ArtifactCatalogDrift;
    if (generation == 0) return error.EnrichmentSourceChanged;
    const inventory = @import("artifact_inventory.zig");
    var ordered = (try inventory.load(alloc, txn)) orelse return error.ArtifactCatalogDrift;
    defer ordered.deinit();
    const binding = ordered.value.command.binding;
    const catalogs = try inventory.catalogs(txn);
    if (binding.effect_protocol < 15 or binding.epoch != authority.epoch or
        !std.mem.eql(u8, &binding.digest, &authority.catalog_digest) or
        !std.mem.eql(u8, &ordered.value.command.namespace, &authority.namespace) or
        !std.mem.eql(u8, &catalogs.digest(), &binding.digest) or !plan.matchesArtifactInventory(catalogs)) return error.ArtifactCatalogDrift;
    return .{ .txn = txn, .request = request, .authority = authority, .generation = generation, .effect_protocol = binding.effect_protocol };
}

fn captureMember(alloc: std.mem.Allocator, txn: anytype, request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, authority: publication.Authority, generation: u64, effect_protocol: u16, scope: []const u8) !Input {
    var token: context.Token = .{
        .arena = std.heap.ArenaAllocator.init(alloc),
        .namespace = authority.namespace,
        .epoch = authority.epoch,
        .catalog_digest = authority.catalog_digest,
        .producer_kind = .index,
        .producer_name = undefined,
        .producer_generation = generation,
        .artifact_name = undefined,
        .source = undefined,
    };
    errdefer token.deinit();
    const owned = token.arena.allocator();
    // Decoded key components are scratch, not provider-lifetime state.
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const temporary = scratch.allocator();
    if (!keys.isChunkArtifactRecordKey(scope)) return error.InvalidBatchRequest;
    const decoded = (try @import("artifact_ids.zig").decodeArtifactRefAlloc(temporary, scope)) orelse return error.InvalidBatchRequest;
    if (!std.mem.eql(u8, decoded.document_id, request.doc_key)) return error.InvalidBatchRequest;
    // The pinned plan already compiled the provider/consumer relationship.
    // Do not reparse all index/enrichment JSON for every chunk of every row.
    // Final apply independently authorizes projections from the catalog.
    if (!std.mem.eql(u8, decoded.name, request.artifact_name)) return error.EnrichmentSourceChanged;
    const embedding_name = @import("enrichment/enrichment_types.zig").requestEmbeddingName(request);
    token.producer_name = try owned.dupe(u8, request.index_name);
    token.artifact_name = try owned.dupe(u8, embedding_name);
    token.producer_scope_key = try owned.dupe(u8, scope);
    token.source = publication.capturePrimarySource(owned, txn, authority.namespace, request.doc_key) catch |err| switch (err) {
        error.EnrichmentSourceChanged => try publication.capturePrimaryTombstoneSource(owned, txn, authority.namespace, request.doc_key),
        else => return err,
    };
    const output = try keys.derivedEmbeddingArtifactKeyAlloc(owned, scope, embedding_name);
    // This helper resolves selected generation membership, requires accepted
    // upstream provenance, and merges inherited sources/ordinals into token.
    const selected = try @import("artifact_asset_publication.zig").readUpstream(temporary, &token, txn, scope);
    const value = if (token.source.exists and selected != null) try owned.dupe(u8, selected.?) else null;
    const previous = txn.get(output) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    try token.observePrecondition(output, previous, try publication.artifactRevision(txn, authority.namespace, output));
    return .{ .token = token, .value = value, .output_key = output, .previous_output_exists = previous != null, .effect_protocol = effect_protocol };
}

const empty_window_prefix = "\x00\x00__artifact_publication__:empty-windows:v1:";
pub const EmptyWindowKey = [empty_window_prefix.len + 32]u8;

/// Only the physical writer certifies emptiness. A live child or scalar
/// retirement invalidates that exact member and consumer. The physical fact
/// is independent of changes to the catalog's logical configuration.
pub fn emptyWindowKey(namespace: publication.Namespace, scalar: []const u8) EmptyWindowKey {
    var key: EmptyWindowKey = undefined;
    @memcpy(key[0..empty_window_prefix.len], empty_window_prefix);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update(&namespace);
    hash.update(scalar);
    hash.final(key[empty_window_prefix.len..]);
    return key;
}

const media_embedding_recipe = @import("media_embedding_recipe.zig");

pub const Fence = struct {
    scope: []const u8,
    requires_member: bool,
    certificate: ?@import("artifact_producer_provenance.zig").ArtifactCertificate = null,
    prepared: bool = false,
    dense: bool = false,
    embedding_config: []const u8 = "",
    chunker_config: ?[]const u8 = null,
    inline_index_config: []const u8 = "",
    dimensions: u32 = 0,
    revalidate_window_plan: bool = false,
    certify_empty: ?EmptyWindowKey = null,
    /// Resolve membership and decode inherited provenance before serialized
    /// apply. Published generation rows are immutable; the head/member guards
    /// plus this exact proof reference fence preserve the prepared observation.
    pub fn bind(self: *Fence, alloc: std.mem.Allocator, txn: anytype, command: publication.Command, window_protocol: bool) !void {
        if (window_protocol and self.dense) {
            // A restored binding without the compact marker cannot rely on
            // the parent revision to fence a concurrent child insertion.
            self.revalidate_window_plan = !try @import("artifact_catalog_activation.zig").windowSetFenced(txn, .{
                .namespace = command.namespace,
                .epoch = command.authority_epoch,
                .catalog_digest = command.catalog_digest,
            });
            self.certify_empty = try self.checkWindowPlan(alloc, txn, command, !self.revalidate_window_plan);
        }
        self.prepared = false;
        self.certificate = null;
        var input = try @import("artifact_chunk_generation.zig").captureInput(alloc, txn, self.scope);
        defer input.deinit();
        if (self.requires_member and input.value == null) return error.EnrichmentSourceChanged;
        if (window_protocol and self.dense) {
            if (input.value) |payload| try self.validateFusedReceipts(alloc, command, payload);
        } else {
            var completed: ?std.AutoHashMapUnmanaged(u32, void) = null;
            defer if (completed) |*indices| indices.deinit(alloc);
            for (command.mutations) |mutation| if (mutation.value != null) {
                const base = (try keys.derivedEmbeddingBaseKeyAlloc(alloc, mutation.key)) orelse return error.InvalidBatchRequest;
                defer alloc.free(base);
                if (keys.mediaWindowIndex(base)) |index| {
                    if (completed == null) {
                        completed = .empty;
                        const metadata = try @import("../../chunking/chunk.zig").modelMetadataAlloc(alloc, input.value orelse return error.EnrichmentSourceChanged) orelse return error.InvalidBatchRequest;
                        defer metadata.deinit(alloc);
                        var results = try std.json.parseFromSlice([]@import("inference_chunker").outputs.Result, alloc, metadata.outputs_json orelse return error.InvalidBatchRequest, .{});
                        defer results.deinit();
                        for (results.value) |result| {
                            if (result.kind != .embedding or !std.mem.eql(u8, result.name, "embedding") or result.status != .complete) continue;
                            // Older single-effect wire shapes did not bind each
                            // child receipt. All new multi-effect live windows do.
                            if (command.mutations.len > 1) if (command.execution_identity) |revision| {
                                var actual: publication.Digest = undefined;
                                const decoded = std.fmt.hexToBytes(&actual, result.model_revision) catch continue;
                                if (!std.mem.eql(u8, decoded, &revision)) continue;
                            };
                            try completed.?.put(alloc, result.sub_index, {});
                        }
                    }
                    if (!completed.?.contains(index)) return error.EnrichmentSourceChanged;
                }
            };
        }
        self.certificate = if (input.proofValue() != null) try @import("artifact_producer_provenance.zig").certifyInheritedArtifact(alloc, txn, input.proofKey(self.scope), input.proofValue(), command) else null;
        self.prepared = true;
    }
    fn validateFusedReceipts(self: Fence, alloc: std.mem.Allocator, command: publication.Command, payload: []const u8) !void {
        var live: std.AutoHashMapUnmanaged(u32, void) = .empty;
        defer live.deinit(alloc);
        for (command.mutations) |mutation| {
            if (mutation.value == null) continue;
            const base = keys.derivedEmbeddingBaseKey(mutation.key) orelse return error.InvalidBatchRequest;
            const index = keys.mediaWindowIndex(base) orelse return; // A real scalar result is a different computation.
            try live.put(alloc, index, {});
        }
        const metadata = try @import("../../chunking/chunk.zig").modelMetadataAlloc(alloc, payload) orelse {
            if (live.count() != 0) return error.EnrichmentSourceChanged;
            return;
        };
        defer metadata.deinit(alloc);
        const raw = metadata.outputs_json orelse {
            if (live.count() != 0) return error.EnrichmentSourceChanged;
            return;
        };
        const publishes_windows = live.count() != 0;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const owned = arena.allocator();
        const chunker = self.chunker_config orelse if (self.inline_index_config.len != 0)
            try manager.denseGeneratorChunkerJson(owned, self.inline_index_config)
        else
            "";
        const consumer = try media_embedding_recipe.resolve(owned, self.embedding_config, chunker, self.dimensions) orelse {
            if (publishes_windows) return error.EnrichmentSourceChanged;
            return;
        };
        const O = @import("inference_chunker").outputs;
        var receipts = try std.json.parseFromSlice([]O.Result, alloc, raw, .{});
        defer receipts.deinit();
        var seen: std.AutoHashMapUnmanaged(u32, void) = .empty;
        defer seen.deinit(alloc);
        for (receipts.value) |receipt| {
            if (receipt.kind != .embedding or !std.mem.eql(u8, receipt.name, "embedding")) continue;
            // An unbound tombstone must still account for this consumer's receipts.
            const revision_matches = (command.execution_identity == null and !publishes_windows) or
                media_embedding_recipe.matchesRevision(receipt.model_revision, command.execution_identity);
            if (!revision_matches or !try consumer.matches(alloc, receipt)) {
                if (publishes_windows) return error.EnrichmentSourceChanged;
                continue;
            }
            if ((try seen.getOrPut(alloc, receipt.sub_index)).found_existing) return error.EnrichmentSourceChanged;
            switch (receipt.status) {
                .complete => if (!live.remove(receipt.sub_index)) return error.EnrichmentSourceChanged,
                .failed => if (!receipt.terminal) return error.EnrichmentSourceChanged,
                else => return error.EnrichmentSourceChanged,
            }
        }
        if (live.count() != 0) return error.EnrichmentSourceChanged;
    }

    pub fn requireCurrent(self: Fence, txn: anytype) !void {
        if (!self.prepared) return error.InvalidBatchRequest;
        if (self.certificate) |certificate| try certificate.requireCurrent(txn);
    }

    /// Validate the window plan outside the serialized writer. The scalar
    /// precondition also fences the output set: physical child writes advance
    /// the parent revision, even when the parent has no stored value.
    pub fn validateWindowPlan(self: Fence, alloc: std.mem.Allocator, txn: anytype, command: publication.Command) !void {
        _ = try self.checkWindowPlan(alloc, txn, command, false);
    }

    pub fn stageWindowWitness(self: Fence, txn: anytype) !void {
        if (self.certify_empty) |key| try txn.put(&key, "1");
    }

    fn checkWindowPlan(self: Fence, alloc: std.mem.Allocator, txn: anytype, command: publication.Command, witness_enabled: bool) !?EmptyWindowKey {
        if (!publication.isRevisionBoundChunkVectorSet(self.scope, command.producer_artifact_name, command.mutations))
            return error.EnrichmentSourceChanged;
        var scalar: ?publication.Mutation = null;
        var live_window = false;
        for (command.mutations) |effect| {
            if (effect.key[self.scope.len] == keys.derived_embedding_kind) {
                scalar = effect;
            } else live_window = live_window or effect.value != null;
        }
        const parent = scalar orelse return error.EnrichmentSourceChanged;
        const witness = emptyWindowKey(command.namespace, parent.key);
        if (witness_enabled) {
            const expected = for (command.mutation_preconditions) |guard| {
                if (std.mem.eql(u8, parent.key, guard.key)) break guard.input_position;
            } else return error.EnrichmentSourceChanged;
            if (!std.meta.eql(expected, try publication.artifactRevision(txn, command.namespace, parent.key)))
                return error.EnrichmentSourceChanged;
            const previous = txn.get(&witness) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (previous) |raw| {
                // Preview witnesses were tied to an epoch. Re-scan before
                // replacing them with the physical empty-set certificate.
                if (std.mem.eql(u8, raw, "1")) return null;
            }
        }
        const first = try keys.mediaWindowChildKeyAlloc(alloc, self.scope, 0);
        defer alloc.free(first);
        const prefix = first[0 .. first.len - @sizeOf(u32)];
        var cursor = try txn.openPhysicalCursorAdapter();
        defer cursor.close();
        cursor.setKeysOnly();
        var row = try cursor.seekAtOrAfter(first);
        var outputs: std.StringHashMapUnmanaged(void) = .empty;
        defer outputs.deinit(alloc);
        while (row) |entry| : (row = try cursor.next()) {
            if (!std.mem.startsWith(u8, entry.key, prefix)) break;
            if (!keys.matchesDerivedEmbeddingArtifactName(entry.key, command.producer_artifact_name)) continue;
            if (outputs.count() == 0) for (command.mutations) |mutation| try outputs.put(alloc, mutation.key, {});
            if (!outputs.contains(entry.key)) return error.EnrichmentSourceChanged;
        }
        // Certify live scalars only. Historical or restored tables earn the
        // first witness by scanning, and retired members need no new record.
        return if (witness_enabled and parent.value != null and !live_window) witness else null;
    }
};

pub fn prepare(alloc: std.mem.Allocator, command: publication.Command, catalogs: @import("artifact_inventory.zig").Catalogs) !publication.PreparedEffects {
    try command.validate(alloc);
    if (!std.mem.eql(u8, &command.catalog_digest, &catalogs.digest())) return error.ArtifactCatalogDrift;
    if (command.producer_kind != .index or command.mutations.len == 0 or
        !keys.isChunkArtifactRecordKey(command.producer_scope_key) or keys.mediaWindowIndex(command.producer_scope_key) != null) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const head = (try @import("artifact_chunk_manifest.zig").keyForMemberAlloc(owned, command.producer_scope_key)).?;
    head[keys.findComponentTerminator(head, 1).? + 2] = keys.producer_generation_head_kind;
    const has_head_guard = for (command.artifact_sources) |guard| {
        if (std.mem.eql(u8, guard.key, head)) break true;
    } else false;
    if (!has_head_guard) return error.InvalidBatchRequest;
    const scope = (try @import("artifact_ids.zig").decodeArtifactRefAlloc(owned, command.producer_scope_key)).?;
    const configs = try manager.deserializeCatalog(owned, catalogs.indexes);
    const producers = if (catalogs.enrichments.len == 0) &.{} else try @import("catalog/enrichment_catalog.zig").deserializeCatalog(owned, catalogs.enrichments);
    const producer = for (configs) |config| {
        if (std.mem.eql(u8, config.name, command.producer_name)) break config;
    } else return error.EnrichmentSourceChanged;
    if (keys.derivedCoverageGenerationForConfig(producer.coverage_generation, producer.config_json) != command.producer_generation or
        !try manager.consumesGeneratedChunkVector(owned, producer, producers, scope.name, command.producer_artifact_name)) return error.EnrichmentSourceChanged;
    const dims = if (producer.kind == .dense_vector) try manager.denseConfigDimensions(owned, producer) else 0;
    const model_config = for (producers) |entry| {
        if (entry.kind == .embedding and std.mem.eql(u8, entry.name, command.producer_artifact_name)) break entry.producer_json;
    } else "";
    const chunker_config = for (producers) |entry| {
        if (entry.kind == .chunk and std.mem.eql(u8, entry.name, scope.name)) break entry.chunker_json;
    } else null;
    var dense: std.ArrayList(derived.DerivedDenseEmbeddingWrite) = .empty;
    var sparse: std.ArrayList(derived.DerivedSparseEmbeddingWrite) = .empty;
    var changed: std.ArrayList([]const u8) = .empty;
    var deleted: std.ArrayList([]const u8) = .empty;
    for (command.mutations) |mutation| {
        if (mutation.family != .derived_vector or mutation.source_index != 0) return error.InvalidBatchRequest;
        const base = (try keys.derivedEmbeddingBaseKeyAlloc(owned, mutation.key)) orelse return error.InvalidBatchRequest;
        if (!std.mem.eql(u8, keys.mediaWindowParent(base), command.producer_scope_key) or !keys.matchesDerivedEmbeddingArtifactName(mutation.key, command.producer_artifact_name)) return error.InvalidBatchRequest;
        if (producer.kind != .dense_vector and keys.mediaWindowIndex(base) != null) return error.InvalidBatchRequest;
        const expected = try owned.dupe(u8, mutation.key);
        const guarded = for (command.mutation_preconditions) |guard| {
            if (std.mem.eql(u8, guard.key, expected)) break true;
        } else false;
        if (!guarded) return error.InvalidBatchRequest;
        @import("online_vector_artifacts.zig").validatePayload(mutation.value) catch return error.InvalidBatchRequest;
        if (mutation.value) |value| {
            const header = try codec.decodeHeader(value);
            if ((producer.kind == .dense_vector and header.kind != .dense_embedding) or
                (producer.kind == .sparse_vector and header.kind != .sparse_embedding)) return error.InvalidBatchRequest;
            const vector = if (producer.kind == .dense_vector) try codec.decodeDenseEmbeddingAlloc(owned, value) else &.{};
            if (producer.kind == .dense_vector and vector.len != dims) return error.InvalidBatchRequest;
            const sparse_vector = if (producer.kind == .sparse_vector) try codec.decodeSparseEmbeddingAlloc(owned, value) else null;
            for (configs) |config| {
                if (config.kind != producer.kind or !try manager.consumesGeneratedChunkVector(owned, config, producers, scope.name, command.producer_artifact_name)) continue;
                if (config.kind == .dense_vector) {
                    if (try manager.denseConfigDimensions(owned, config) != dims) continue;
                    try dense.append(owned, .{ .index_name = config.name, .parent_doc_key = scope.document_id, .doc_key = base, .artifact_key = expected, .vector = vector });
                } else try sparse.append(owned, .{ .index_name = config.name, .doc_key = base, .artifact_key = expected, .indices = sparse_vector.?.indices, .values = sparse_vector.?.values });
            }
            try changed.append(owned, expected);
        } else try deleted.append(owned, expected);
    }
    // Finish allocations before copying the arena owner. Growing the arena
    // after that copy would leave its new allocation chain unowned.
    const owned_scope = try owned.dupe(u8, command.producer_scope_key);
    return .{ .arena = arena, .chunk_vector_fence = .{ .scope = owned_scope, .requires_member = changed.items.len != 0, .dense = producer.kind == .dense_vector, .embedding_config = model_config, .chunker_config = chunker_config, .inline_index_config = producer.config_json, .dimensions = dims }, .batch = .{
        .dense_embeddings = dense.items,
        .sparse_embeddings = sparse.items,
        .changed_artifact_keys = changed.items,
        .deleted_keys = deleted.items,
    } };
}

test "ordered artifact inventory chunk vectors project shared consumers without terminal document credit" {
    try testPreparation(false);
    try testPreparation(true);
}

fn testPreparation(sparse_kind: bool) !void {
    const alloc = std.testing.allocator;
    const config_json = if (sparse_kind) "{\"field\":\"sparse\",\"embedding_name\":\"model\"}" else "{\"field\":\"dense\",\"dims\":2,\"embedding_name\":\"model\"}";
    const Config = @import("types.zig").IndexConfig;
    const configs = [_]Config{
        .{ .name = "first", .kind = if (sparse_kind) .sparse_vector else .dense_vector, .config_json = config_json, .coverage_generation = 7 },
        .{ .name = "second", .kind = if (sparse_kind) .sparse_vector else .dense_vector, .config_json = config_json, .coverage_generation = 9 },
        .{ .name = "unrelated", .kind = if (sparse_kind) .sparse_vector else .dense_vector, .config_json = if (sparse_kind) "{\"field\":\"sparse\",\"embedding_name\":\"other\"}" else "{\"field\":\"dense\",\"dims\":3,\"embedding_name\":\"model\"}", .coverage_generation = 11 },
    };
    var catalog: std.ArrayList(u8) = .empty;
    defer catalog.deinit(alloc);
    try catalog.appendSlice(alloc, "AIDX\x02\x00\x00\x00\x03\x00\x00\x00");
    for (configs) |config| {
        var length: [4]u8 = undefined;
        std.mem.writeInt(u32, &length, @intCast(config.name.len), .little);
        try catalog.appendSlice(alloc, &length);
        try catalog.appendSlice(alloc, config.name);
        try catalog.append(alloc, @backingInt(config.kind));
        std.mem.writeInt(u32, &length, @intCast(config.config_json.len), .little);
        try catalog.appendSlice(alloc, &length);
        try catalog.appendSlice(alloc, config.config_json);
        var generation: [8]u8 = undefined;
        std.mem.writeInt(u64, &generation, config.coverage_generation, .little);
        try catalog.appendSlice(alloc, &generation);
    }
    const catalogs: @import("artifact_inventory.zig").Catalogs = .{
        .indexes = catalog.items,
        .enrichments = if (sparse_kind)
            "[{\"name\":\"chunks\",\"kind\":\"chunk\",\"source_field\":\"body\"},{\"name\":\"model\",\"kind\":\"embedding\",\"source_field\":\"body\",\"source_artifact_name\":\"chunks\"}]"
        else
            "[{\"name\":\"chunks\",\"kind\":\"chunk\",\"source_field\":\"body\"},{\"name\":\"model\",\"kind\":\"embedding\",\"source_field\":\"body\",\"source_artifact_name\":\"chunks\",\"expected_dims\":2}]",
    };
    const scope = try keys.documentUnitChunkArtifactKeyAlloc(alloc, "doc", "chunks", "unit\x00\xff", 4);
    defer alloc.free(scope);
    const head = (try @import("artifact_chunk_manifest.zig").keyForMemberAlloc(alloc, scope)).?;
    defer alloc.free(head);
    head[keys.findComponentTerminator(head, 1).? + 2] = keys.producer_generation_head_kind;
    const key = try keys.derivedEmbeddingArtifactKeyAlloc(alloc, scope, "model");
    defer alloc.free(key);
    const payload = if (sparse_kind) try codec.encodeSparseEmbeddingAlloc(alloc, 3, &.{ 1, 2 }, &.{ 1, 2 }) else try codec.encodeDenseEmbeddingAlloc(alloc, 3, &.{ 1, 2 });
    defer alloc.free(payload);
    var command: publication.Command = .{ .namespace = @splat(1), .authority_epoch = 1, .catalog_digest = catalogs.digest(), .producer_name = "first", .producer_generation = 7, .producer_artifact_name = "model", .producer_scope_key = scope, .sources = &.{.{ .document_key = "doc", .content_digest = @splat(1), .timestamp = 2, .input_position = .{ .raft = .{ .term = 1, .index = 1 } } }}, .artifact_sources = &.{.{ .key = head, .content_digest = @splat(3), .input_position = null, .source_index = 0 }}, .mutation_preconditions = &.{.{ .key = key, .content_digest = null, .input_position = null, .source_index = 0 }}, .mutations = &.{.{ .family = .derived_vector, .key = key, .value = payload, .source_index = 0 }}, .publication_digest = @splat(0) };
    command.publication_digest = command.digest();
    const Check = struct {
        fn run(a: std.mem.Allocator, cmd: publication.Command, cats: @import("artifact_inventory.zig").Catalogs, sparse: bool) !void {
            var prepared = try prepare(a, cmd, cats);
            defer prepared.deinit();
            try std.testing.expectEqual(@as(usize, 0), prepared.coverage.len);
            try std.testing.expectEqual(@as(usize, 2), if (sparse) prepared.batch.sparse_embeddings.len else prepared.batch.dense_embeddings.len);
            try std.testing.expectEqualStrings(cmd.producer_scope_key, prepared.chunk_vector_fence.?.scope);
            try std.testing.expect(prepared.chunk_vector_fence.?.requires_member);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(alloc, Check.run, .{ command, catalogs, sparse_kind });
    var proof = try @import("artifact_producer_provenance.zig").fromCommand(alloc, command);
    defer proof.deinit();
    try proof.proof.requireInheritedBy(command);
    var inherited = command;
    inherited.sources = &.{ .{ .document_key = "aaa", .content_digest = @splat(4), .timestamp = 1, .input_position = null }, command.sources[0] };
    var remapped = command.artifact_sources[0];
    remapped.source_index = 1;
    inherited.artifact_sources = (&remapped)[0..1];
    try proof.proof.requireInheritedBy(inherited);
    remapped.source_index = 0;
    try std.testing.expectError(error.EnrichmentSourceChanged, proof.proof.requireInheritedBy(inherited));
    inherited = command;
    inherited.artifact_sources = &.{};
    try std.testing.expectError(error.EnrichmentSourceChanged, proof.proof.requireInheritedBy(inherited));
    inherited = command;
    inherited.authority_epoch += 1;
    try std.testing.expectError(error.ArtifactCatalogDrift, proof.proof.requireInheritedBy(inherited));
    if (!sparse_kind) {
        const child = try keys.mediaWindowKeyAlloc(alloc, scope, 1);
        defer alloc.free(child);
        const child_output = try keys.derivedEmbeddingArtifactKeyAlloc(alloc, child, "model");
        defer alloc.free(child_output);
        const effects = [_]publication.Mutation{ command.mutations[0], .{ .family = .derived_vector, .key = child_output, .value = payload, .source_index = 0 } };
        const guards = [_]publication.ArtifactSource{ command.mutation_preconditions[0], .{ .key = child_output, .content_digest = null, .input_position = null, .source_index = 0 } };
        var multi = command;
        multi.mutations = &effects;
        multi.mutation_preconditions = &guards;
        multi.publication_digest = multi.digest();
        var prepared = try prepare(alloc, multi, catalogs);
        defer prepared.deinit();
        try std.testing.expectEqual(@as(usize, 4), prepared.batch.dense_embeddings.len);
        try std.testing.expectEqual(@as(usize, 2), prepared.batch.changed_artifact_keys.len);
        try std.testing.expectEqualStrings(child, prepared.batch.dense_embeddings[2].doc_key);
        try std.testing.expectEqualStrings("doc", prepared.batch.dense_embeddings[2].parent_doc_key.?);
    }
    command.mutation_preconditions = &.{};
    command.publication_digest = command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
}

test "model aware media chunk vector windows share parent fences and project every vector" {
    try testPreparation(false);
}

test "model aware media current empty witness bypasses the child range cursor" {
    const a = std.testing.allocator;
    const Reader = struct {
        witness: EmptyWindowKey,
        raw: [1]u8,

        pub fn get(self: *const @This(), key: []const u8) anyerror![]const u8 {
            return if (std.mem.eql(u8, key, &self.witness)) &self.raw else error.NotFound;
        }

        pub fn openPhysicalCursorAdapter(_: @This()) !@import("../backend_erased.zig").Cursor {
            return error.TestUnexpectedResult;
        }
    };
    const scope = try keys.chunkArtifactKeyAlloc(a, "doc", "text", 0);
    defer a.free(scope);
    const scalar = try keys.derivedEmbeddingArtifactKeyAlloc(a, scope, "model");
    defer a.free(scalar);
    var command: publication.Command = .{
        .namespace = @splat(1),
        .authority_epoch = 2,
        .catalog_digest = @splat(3),
        .producer_name = "vectors",
        .producer_kind = .index,
        .producer_generation = 1,
        .producer_artifact_name = "model",
        .producer_scope_key = scope,
        .sources = &.{},
        .mutation_preconditions = &.{.{ .key = scalar, .content_digest = null, .input_position = null, .source_index = 0 }},
        .mutations = &.{.{ .family = .derived_vector, .key = scalar, .value = "vector", .source_index = 0 }},
        .publication_digest = @splat(0),
    };
    var reader: Reader = .{ .witness = emptyWindowKey(command.namespace, scalar), .raw = "1".* };
    command.authority_epoch += 1;
    command.catalog_digest = @splat(4);
    const fence: Fence = .{ .scope = scope, .requires_member = true, .dense = true };
    try std.testing.expect((try fence.checkWindowPlan(a, &reader, command, true)) == null);
    reader.raw[0] ^= 1;
    try std.testing.expectError(error.TestUnexpectedResult, fence.checkWindowPlan(a, &reader, command, true));
}

test "model aware media fused receipt validation covers tombstones and pending windows" {
    const a = std.testing.allocator;
    const O = @import("inference_chunker").outputs;
    const scope = try keys.chunkArtifactKeyAlloc(a, "doc", "clips", 0);
    defer a.free(scope);
    const scalar = try keys.derivedEmbeddingArtifactKeyAlloc(a, scope, "model");
    defer a.free(scalar);
    const child = try keys.mediaWindowChildKeyAlloc(a, scope, 0);
    defer a.free(child);
    const child_key = try keys.derivedEmbeddingArtifactKeyAlloc(a, child, "model");
    defer a.free(child_key);
    var effects = [_]publication.Mutation{
        .{ .family = .derived_vector, .key = scalar, .value = null, .source_index = 0 },
        .{ .family = .derived_vector, .key = child_key, .value = "vector", .source_index = 0 },
    };
    var command: publication.Command = .{
        .namespace = @splat(1),
        .authority_epoch = 2,
        .catalog_digest = @splat(3),
        .producer_name = "vectors",
        .producer_generation = 1,
        .producer_artifact_name = "model",
        .sources = &.{},
        .mutations = effects[0..1],
        .publication_digest = @splat(0),
    };
    const fence: Fence = .{
        .scope = scope,
        .requires_member = false,
        .dense = true,
        .dimensions = 2,
        .embedding_config =
        \\{"provider":"antfly","model":"model"}
        ,
        .chunker_config =
        \\{"provider":"antfly","outputs":{"boundary_model":"model","targets":[{"name":"embedding","kind":"embedding","model":"model","dimensions":2}]}}
        ,
    };
    const revision: publication.Digest = @splat(8);
    const revision_hex = std.fmt.bytesToHex(revision, .lower);
    const identity: [64]u8 = @splat('3');
    var target: O.Target = .{ .name = "embedding", .kind = .embedding, .model = "model", .dimensions = 2 };
    const matching_recipe = try target.recipeIdentity(a, &identity, &revision_hex);
    target.task_type = "RETRIEVAL_QUERY";
    const foreign_recipe = try target.recipeIdentity(a, &identity, &revision_hex);
    const completed: O.Result = .{
        .name = "embedding",
        .kind = .embedding,
        .required = true,
        .model = "model",
        .model_identity = &identity,
        .model_revision = &revision_hex,
        .recipe_id = &matching_recipe,
        .status = .complete,
    };
    var pending = completed;
    pending.sub_index = 1;
    pending.status = .failed;
    pending.code = "temporary";
    var terminal = pending;
    terminal.terminal = true;
    terminal.sub_index = 0;
    var foreign = completed;
    foreign.recipe_id = &foreign_recipe;
    const outcomes = [_][]const O.Result{ &.{completed}, &.{ completed, pending }, &.{terminal}, &.{foreign} };
    for (outcomes, 0..) |outcome, scenario| {
        const raw = try std.json.Stringify.valueAlloc(a, outcome, .{});
        defer a.free(raw);
        const Model = @import("inference_chunker").types.ModelMetadata;
        const metadata: Model = .{
            .descriptor = .{ .ordinal = 0, .input_tokens = 1, .coordinates = .{ .kind = .video } },
            .inference_id = "plan:0",
            .plan_id = "plan",
            .source_fingerprint = "source",
            .source_version = "v1",
            .model = "model",
            .model_identity = "",
            .model_revision = "",
            .task_type = "RETRIEVAL_DOCUMENT",
            .dimensions = 2,
            .policy = .{},
            .outputs_json = raw,
        };
        const payload = try std.json.Stringify.valueAlloc(a, .{ ._chunk = metadata }, .{});
        defer a.free(payload);
        command.mutations = if (scenario == 1) &effects else effects[0..1];
        command.execution_identity = revision;
        if (scenario == 0) {
            command.execution_identity = null;
            try std.testing.expectError(error.EnrichmentSourceChanged, fence.validateFusedReceipts(a, command, payload));
            command.execution_identity = revision;
        }
        if (scenario == 2 or scenario == 3) {
            try fence.validateFusedReceipts(a, command, payload);
        } else {
            try std.testing.expectError(error.EnrichmentSourceChanged, fence.validateFusedReceipts(a, command, payload));
        }
        if (scenario == 3) {
            command.mutations = &effects;
            try std.testing.expectError(error.EnrichmentSourceChanged, fence.validateFusedReceipts(a, command, payload));
        }
    }
}
