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

        pub fn capture(self: @This(), alloc: std.mem.Allocator, scope: []const u8) !Input {
            return captureMember(alloc, self.txn, self.request, self.authority, self.generation, scope);
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
    if (binding.effect_protocol != 15 or binding.epoch != authority.epoch or
        !std.mem.eql(u8, &binding.digest, &authority.catalog_digest) or
        !std.mem.eql(u8, &ordered.value.command.namespace, &authority.namespace) or
        !std.mem.eql(u8, &catalogs.digest(), &binding.digest) or !plan.matchesArtifactInventory(catalogs)) return error.ArtifactCatalogDrift;
    return .{ .txn = txn, .request = request, .authority = authority, .generation = generation };
}

fn captureMember(alloc: std.mem.Allocator, txn: anytype, request: @import("enrichment/enrichment_types.zig").GeneratedEnrichmentRequest, authority: publication.Authority, generation: u64, scope: []const u8) !Input {
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
    return .{ .token = token, .value = value, .output_key = output };
}

pub const Fence = struct {
    scope: []const u8,
    requires_member: bool,
    certificate: ?@import("artifact_producer_provenance.zig").ArtifactCertificate = null,
    prepared: bool = false,
    /// Resolve membership and decode inherited provenance before serialized
    /// apply. Published generation rows are immutable; the head/member guards
    /// plus this exact proof reference fence preserve the prepared observation.
    pub fn bind(self: *Fence, alloc: std.mem.Allocator, txn: anytype, command: publication.Command) !void {
        self.prepared = false;
        self.certificate = null;
        var input = try @import("artifact_chunk_generation.zig").captureInput(alloc, txn, self.scope);
        defer input.deinit();
        if (self.requires_member and input.value == null) return error.EnrichmentSourceChanged;
        self.certificate = if (input.proofValue() != null) try @import("artifact_producer_provenance.zig").certifyInheritedArtifact(alloc, txn, input.proofKey(self.scope), input.proofValue(), command) else null;
        self.prepared = true;
    }
    pub fn requireCurrent(self: Fence, txn: anytype) !void {
        if (!self.prepared) return error.InvalidBatchRequest;
        if (self.certificate) |certificate| try certificate.requireCurrent(txn);
    }
};

pub fn prepare(alloc: std.mem.Allocator, command: publication.Command, catalogs: @import("artifact_inventory.zig").Catalogs) !publication.PreparedEffects {
    try command.validate(alloc);
    if (!std.mem.eql(u8, &command.catalog_digest, &catalogs.digest())) return error.ArtifactCatalogDrift;
    if (command.producer_kind != .index or command.mutations.len != 1 or
        !keys.isChunkArtifactRecordKey(command.producer_scope_key) or command.mutations[0].family != .derived_vector) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const mutation = command.mutations[0];
    const expected = try keys.derivedEmbeddingArtifactKeyAlloc(owned, command.producer_scope_key, command.producer_artifact_name);
    if (!std.mem.eql(u8, expected, mutation.key)) return error.InvalidBatchRequest;
    const head = (try @import("artifact_chunk_manifest.zig").keyForMemberAlloc(owned, command.producer_scope_key)).?;
    head[keys.findComponentTerminator(head, 1).? + 2] = keys.producer_generation_head_kind;
    // Absence before the first head is an input too. A legacy member digest
    // alone cannot authorize output after a head supersedes the physical row.
    const has_head_guard = for (command.artifact_sources) |guard| {
        if (std.mem.eql(u8, guard.key, head)) break true;
    } else false;
    const has_output_guard = for (command.mutation_preconditions) |guard| {
        if (std.mem.eql(u8, guard.key, expected)) break true;
    } else false;
    if (!has_head_guard or !has_output_guard) return error.InvalidBatchRequest;
    @import("online_vector_artifacts.zig").validatePayload(mutation.value) catch return error.InvalidBatchRequest;
    const scope = (try @import("artifact_ids.zig").decodeArtifactRefAlloc(owned, command.producer_scope_key)).?;
    const configs = try manager.deserializeCatalog(owned, catalogs.indexes);
    const producers = if (catalogs.enrichments.len == 0) &.{} else try @import("catalog/enrichment_catalog.zig").deserializeCatalog(owned, catalogs.enrichments);
    const producer = for (configs) |config| {
        if (std.mem.eql(u8, config.name, command.producer_name)) break config;
    } else return error.EnrichmentSourceChanged;
    if (keys.derivedCoverageGenerationForConfig(producer.coverage_generation, producer.config_json) != command.producer_generation or
        !try manager.consumesGeneratedChunkVector(owned, producer, producers, scope.name, command.producer_artifact_name)) return error.EnrichmentSourceChanged;
    const dims = if (producer.kind == .dense_vector) try manager.denseConfigDimensions(owned, producer) else 0;
    var dense: std.ArrayList(derived.DerivedDenseEmbeddingWrite) = .empty;
    var sparse: std.ArrayList(derived.DerivedSparseEmbeddingWrite) = .empty;
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
                try dense.append(owned, .{ .index_name = config.name, .parent_doc_key = scope.document_id, .doc_key = command.producer_scope_key, .artifact_key = expected, .vector = vector });
            } else try sparse.append(owned, .{ .index_name = config.name, .doc_key = command.producer_scope_key, .artifact_key = expected, .indices = sparse_vector.?.indices, .values = sparse_vector.?.values });
        }
    }
    const artifact_keys = try owned.alloc([]const u8, 1);
    artifact_keys[0] = expected;
    const logical_scope = try owned.dupe(u8, command.producer_scope_key);
    // All replay/fence bytes must outlive the transport command buffer.
    for (dense.items) |*write| write.doc_key = logical_scope;
    for (sparse.items) |*write| write.doc_key = logical_scope;
    return .{ .arena = arena, .chunk_vector_fence = .{ .scope = logical_scope, .requires_member = mutation.value != null }, .batch = .{
        .dense_embeddings = dense.items,
        .sparse_embeddings = sparse.items,
        .changed_artifact_keys = if (mutation.value != null) artifact_keys else &.{},
        .deleted_keys = if (mutation.value == null) artifact_keys else &.{},
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
    command.mutation_preconditions = &.{};
    command.publication_digest = command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
}
