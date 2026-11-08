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

//! Owned input authority captured before provider execution. Local replay
//! positions only fence ingress; replicated commands carry tagged owner
//! positions and exact physical input digests, never a worker lease clock.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const inventory = @import("artifact_inventory.zig");
const identity = @import("doc_identity.zig");
const keys = @import("../internal_keys.zig");
const requests = @import("enrichment/enrichment_types.zig");

pub const Materializer = struct {
    ptr: *anyopaque,
    materialize: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8) anyerror![]u8,
};

pub const DocumentAuthority = struct { authority: publication.Authority, generation: u64, effect_protocol: u16, requirement: publication.Digest };
pub const TemplateAuthority = struct { effect_protocol: u16, authority: publication.Authority, requirement: *const @import("artifact_completion_plan.zig").Node };

/// Exact immutable provider definition shared by execution and completion.
/// Scope-specific callers must still establish that their scope set is closed.
pub fn authorizeTemplate(alloc: std.mem.Allocator, txn: anytype, request: requests.GeneratedEnrichmentRequest, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !?TemplateAuthority {
    const authority = (try publication.authority(txn)) orelse return null;
    return try bindTemplate(alloc, txn, request, plan, authority);
}

fn bindTemplate(alloc: std.mem.Allocator, txn: anytype, request: requests.GeneratedEnrichmentRequest, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot, authority: publication.Authority) !TemplateAuthority {
    const owner = (try @import("../source_authority.zig").load(txn)) orelse return error.ArtifactCatalogDrift;
    if (!std.mem.eql(u8, &owner.namespace, &authority.namespace)) return error.ArtifactCatalogDrift;
    const completion = if (plan.completion_plan) |*value| value else return error.ArtifactCatalogDrift;
    const requirement = try completion.providerFor(request);
    const ordinal = requirement.template orelse return error.ArtifactCatalogDrift;
    if (ordinal >= plan.generated_templates.len or !sameDefinition(request, plan.generated_templates[ordinal])) return error.EnrichmentSourceChanged;
    var ordered = (try inventory.load(alloc, txn)) orelse return error.ArtifactCatalogDrift;
    defer ordered.deinit();
    const binding = ordered.value.command.binding;
    if (binding.epoch != authority.epoch or
        !std.mem.eql(u8, &binding.digest, &authority.catalog_digest) or
        !std.mem.eql(u8, &ordered.value.command.namespace, &authority.namespace)) return error.ArtifactCatalogDrift;
    const local = try inventory.catalogs(txn);
    if (!std.mem.eql(u8, &local.digest(), &binding.digest) or !plan.matchesArtifactInventory(local)) return error.ArtifactCatalogDrift;
    // Older ordered owners cannot supply receipt-backed stream witnesses.
    // Preserve pending debt until an ordered protocol upgrade; this is not
    // evidence of catalog corruption and never authorizes legacy outputs.
    if (!binding.supportsProducerWitnesses()) return error.OnlineMergeArtifactTailsUnsupported;
    return .{ .authority = authority, .effect_protocol = binding.effect_protocol, .requirement = requirement };
}

/// Shared authorization for document-wide provider execution and completion.
/// Neither a receipt identity nor a caller-supplied request defines the scope:
/// it must be the exact immutable template from the active catalog.
pub fn authorizeDocument(alloc: std.mem.Allocator, txn: anytype, request: requests.GeneratedEnrichmentRequest, plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot) !?DocumentAuthority {
    const authority = (try publication.authority(txn)) orelse return null;
    if (request.input_kind != .document or request.embedding_input != .text or request.chunk_size != 0 or
        request.neighbor_context_json.len != 0 or request.upstream_artifact_name.len != 0 or
        (request.kind != .dense_embedding and request.kind != .sparse_embedding)) return error.OnlineMergeArtifactTailsUnsupported;
    const bound = try bindTemplate(alloc, txn, request, plan, authority);
    const requirement = bound.requirement;
    const generation = plan.coverageGeneration(request.index_name) orelse return error.ArtifactCatalogDrift;
    if (generation == 0 or requirement.scope != .document) return error.ArtifactCatalogDrift;
    return .{ .authority = authority, .generation = generation, .effect_protocol = bound.effect_protocol, .requirement = requirement.id };
}

pub const Token = struct {
    alloc: std.mem.Allocator,
    root: u128,
    namespace: publication.Namespace,
    authority_epoch: u64,
    catalog_digest: publication.Digest,
    producer_name: []u8,
    producer_generation: u64,
    artifact_name: []u8,
    source: publication.Source,
    execution_identity: ?publication.Digest = null,
    output_guard: ?publication.ArtifactSource = null,
    effect_protocol: u16 = 14,

    pub fn deinit(self: *@This()) void {
        if (self.output_guard) |guard| self.alloc.free(@constCast(guard.key));
        self.alloc.free(self.producer_name);
        self.alloc.free(self.artifact_name);
        self.alloc.free(@constCast(self.source.document_key));
        self.* = undefined;
    }

    /// Bind the optional execution identity and the output observed before work.
    /// Late results cannot replace newer publications, even without a revision.
    pub fn bindExecution(self: *@This(), txn: anytype, revision: ?publication.Digest) !void {
        // Legacy catalogs cannot encode an output precondition. Resolved
        // executions require the newer protocol; legacy unbound work retains
        // its original behavior until the catalog epoch upgrades.
        if (self.effect_protocol < 17) {
            if (revision != null) return error.OnlineMergeArtifactTailsUnsupported;
            return;
        }
        const key = try keys.embeddingArtifactKeyForDocumentAlloc(self.alloc, self.source.document_key, self.artifact_name);
        errdefer self.alloc.free(key);
        const raw = txn.get(key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const digest: ?publication.Digest = if (raw) |value| blk: {
            var result: publication.Digest = undefined;
            std.crypto.hash.sha2.Sha256.hash(value, &result, .{});
            break :blk result;
        } else null;
        const guard: publication.ArtifactSource = .{ .key = key, .content_digest = digest, .input_position = try publication.artifactRevision(txn, self.namespace, key), .source_index = 0 };
        if (self.output_guard) |old| self.alloc.free(@constCast(old.key));
        self.output_guard = guard;
        self.execution_identity = revision;
    }

    /// Values are immutable authored output; the caller retains them until
    /// serialization/enqueue completes. This method never performs IO.
    pub fn command(self: *const Token, mutations: []const publication.Mutation) publication.Command {
        var result: publication.Command = .{
            .namespace = self.namespace,
            .authority_epoch = self.authority_epoch,
            .catalog_digest = self.catalog_digest,
            .producer_name = self.producer_name,
            .producer_generation = self.producer_generation,
            .producer_artifact_name = self.artifact_name,
            .execution_identity = self.execution_identity,
            .mutation_preconditions = if (self.output_guard) |*guard| guard[0..1] else &.{},
            .sources = (&self.source)[0..1],
            .mutations = mutations,
            .publication_digest = @splat(0),
        };
        result.publication_digest = result.digest();
        return result;
    }

    /// An accepted result is canonical for this exact producer/input even if
    /// a duplicate provider invocation returned different floating values.
    /// The input recheck prevents an old receipt from crediting new work.
    pub fn authoredAccepted(self: *const Token, txn: anytype) !bool {
        const active = (try publication.authority(txn)) orelse return error.ArtifactCatalogDrift;
        if (active.epoch != self.authority_epoch or !std.mem.eql(u8, &active.namespace, &self.namespace) or
            !std.mem.eql(u8, &active.catalog_digest, &self.catalog_digest)) return error.ArtifactCatalogDrift;
        const current_catalogs = try inventory.catalogs(txn);
        if (!std.mem.eql(u8, &current_catalogs.digest(), &self.catalog_digest)) return error.ArtifactCatalogDrift;
        const artifact = try keys.embeddingArtifactKeyForDocumentAlloc(self.alloc, self.source.document_key, self.artifact_name);
        defer self.alloc.free(artifact);
        if (try @import("artifact_authored_acceptance.zig").readCurrent(txn, self.root, self.source.document_key, artifact)) |authored| {
            if (authored.matchesSource(self.source)) {
                try publication.validateSources(self.alloc, txn, self.namespace, (&self.source)[0..1]);
                return true;
            }
        }
        return false;
    }

    pub fn accepted(self: *const Token, txn: anytype) !bool {
        if (try self.authoredAccepted(txn)) return true;
        // Receipts survive output replacement. Only current causal/output
        // provenance can suppress execution for a generated result.
        var generated = (@import("artifact_producer_provenance.zig").readCurrentForSource(self.alloc, txn, self.command(&.{}), self.source) catch |err| switch (err) {
            error.EnrichmentSourceChanged => {
                // A receipt for an older input is a miss for this producer.
                // Revalidate the captured input before allowing fresh work:
                // an already-stale token still has to retry its capture.
                try publication.validateSources(self.alloc, txn, self.namespace, (&self.source)[0..1]);
                return false;
            },
            else => return err,
        }) orelse return false;
        defer generated.deinit();
        return std.meta.eql(self.execution_identity, generated.owned.proof.execution_identity);
    }
};

/// Compare every provider/planning field, excluding only row identity,
/// owner-local replay position, reprocess generation and the derived consumer list. These
/// fields cannot authorize a different generator definition. Consumer
/// projections are independently derived from the certified catalog at apply.
pub fn sameDefinition(a: requests.GeneratedEnrichmentRequest, b: requests.GeneratedEnrichmentRequest) bool {
    inline for (@typeInfo(requests.GeneratedEnrichmentRequest).@"struct".field_names, @typeInfo(requests.GeneratedEnrichmentRequest).@"struct".field_types) |reflected_name, field_type| {
        if (comptime !definitionField(reflected_name)) continue;
        const av = @field(a, reflected_name);
        const bv = @field(b, reflected_name);
        if (comptime @typeInfo(field_type) == .pointer) {
            if (!std.mem.eql(u8, av, bv)) return false;
        } else if (!std.meta.eql(av, bv)) return false;
    }
    return true;
}

fn definitionField(comptime name: []const u8) bool {
    return !std.mem.eql(u8, name, "doc_key") and !std.mem.eql(u8, name, "sequence") and !std.mem.eql(u8, name, "reprocess_generation") and !std.mem.eql(u8, name, "consumer_indexes");
}

/// Stable identity for exactly the fields used by sameDefinition. Framed
/// field names and values distinguish binary strings and future definitions;
/// a new field type must choose an explicit canonical encoding here.
pub fn definitionDigest(request: requests.GeneratedEnrichmentRequest) publication.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:producer-definition:v1:");
    inline for (@typeInfo(requests.GeneratedEnrichmentRequest).@"struct".field_names, @typeInfo(requests.GeneratedEnrichmentRequest).@"struct".field_types) |reflected_name, field_type| {
        if (comptime !definitionField(reflected_name)) continue;
        hashDefinitionBytes(&hash, reflected_name);
        const value = @field(request, reflected_name);
        switch (@typeInfo(field_type)) {
            .pointer => hashDefinitionBytes(&hash, value),
            .array => |array| if (array.child == u8) hashDefinitionBytes(&hash, &value) else @compileError("producer array needs a canonical field encoding"),
            .@"enum" => hashDefinitionBytes(&hash, @tagName(value)),
            .bool => hash.update(&.{@intFromBool(value)}),
            .int => {
                var encoded: [8]u8 = undefined;
                std.mem.writeInt(u64, &encoded, value, .little);
                hash.update(&encoded);
            },
            else => @compileError("producer definition needs a canonical field encoding"),
        }
    }
    var result: publication.Digest = undefined;
    hash.final(&result);
    return result;
}

fn hashDefinitionBytes(hash: *std.crypto.hash.Blake3, value: []const u8) void {
    var size: [8]u8 = undefined;
    std.mem.writeInt(u64, &size, value.len, .little);
    hash.update(&size);
    hash.update(value);
}

test "ordered artifact inventory producer definition identity binds every provider field" {
    const request: requests.GeneratedEnrichmentRequest = .{ .kind = .dense_embedding, .index_name = "index", .doc_key = "doc", .source_field = "body" };
    const expected = definitionDigest(request);
    inline for (@typeInfo(requests.GeneratedEnrichmentRequest).@"struct".field_names, @typeInfo(requests.GeneratedEnrichmentRequest).@"struct".field_types) |reflected_name, field_type| {
        if (comptime !definitionField(reflected_name)) continue;
        var changed = request;
        switch (@typeInfo(field_type)) {
            .pointer => @field(changed, reflected_name) = "\x00changed",
            .@"enum" => @field(changed, reflected_name) = std.meta.tags(field_type)[(@as(usize, @backingInt(@field(request, reflected_name))) + 1) % std.meta.tags(field_type).len],
            .bool => @field(changed, reflected_name) = !@field(request, reflected_name),
            .int => @field(changed, reflected_name) += 1,
            .array => @field(changed, reflected_name)[0] ^= 1,
            else => unreachable,
        }
        try std.testing.expect(!sameDefinition(request, changed));
        try std.testing.expect(!std.mem.eql(u8, &expected, &definitionDigest(changed)));
    }
    var routed = request;
    routed.doc_key = "other";
    routed.sequence = 123;
    var consumers = [_][]u8{@constCast("other-consumer")};
    routed.consumer_indexes = &consumers;
    try std.testing.expect(sameDefinition(request, routed));
    try std.testing.expectEqualDeep(expected, definitionDigest(routed));
}

/// The caller holds its catalog/apply read fence and passes a generated
/// template from that pinned write-plan generation. `txn` is the exact read
/// snapshot used for both authority and physical input. Materialization only
/// normalizes relational rows; hashes from unrelated snapshots are forbidden.
pub fn capture(
    alloc: std.mem.Allocator,
    txn: anytype,
    root: u128,
    request: requests.GeneratedEnrichmentRequest,
    plan: *const @import("catalog/index_manager.zig").IndexManager.WritePlanSnapshot,
    source_store_key: []const u8,
    observed_logical_input: []const u8,
    materializer: ?Materializer,
) !?Token {
    const authorized = (try authorizeDocument(alloc, txn, request, plan)) orelse return null;
    try @import("artifact_reprocess_intent.zig").requireEmbeddingGeneration(alloc, txn, request.doc_key, requests.requestEmbeddingName(request), request.producer_json, request.reprocess_generation);
    if (root == 0) return error.DurableRootIncarnationUnavailable;
    const authority = authorized.authority;
    const producer_generation = authorized.generation;

    const raw = txn.get(source_store_key) catch |err| switch (err) {
        error.NotFound => return error.EnrichmentSourceChanged,
        else => return err,
    };
    const logical = if (materializer) |reader| try reader.materialize(reader.ptr, alloc, source_store_key, raw) else null;
    defer if (logical) |value| alloc.free(value);
    if (!std.mem.eql(u8, logical orelse raw, observed_logical_input)) return error.EnrichmentSourceChanged;
    if (request.sequence != 0 and !keys.isGroupSourceRowKey(source_store_key)) {
        const ordinal = (try identity.lookupOrdinalTxn(alloc, txn, request.doc_key)) orelse return error.EnrichmentSourceChanged;
        const state = (try identity.lookupStateTxn(txn, ordinal)) orelse return error.InvalidDocIdentity;
        if (!state.isLive() or state.created_generation > request.sequence) return error.EnrichmentSourceChanged;
    }
    const timestamp_key = try keys.ttlKeyAlloc(alloc, request.doc_key);
    defer alloc.free(timestamp_key);
    const timestamp_raw = txn.get(timestamp_key) catch |err| switch (err) {
        error.NotFound => return error.EnrichmentSourceChanged,
        else => return err,
    };
    const timestamp = @import("../ttl.zig").writeVersion(timestamp_raw) orelse return error.EnrichmentSourceChanged;
    if (timestamp == 0) return error.EnrichmentSourceChanged;
    const producer_name = try alloc.dupe(u8, request.index_name);
    errdefer alloc.free(producer_name);
    const artifact_name = try alloc.dupe(u8, requests.requestEmbeddingName(request));
    errdefer alloc.free(artifact_name);
    const doc_key = try alloc.dupe(u8, request.doc_key);
    errdefer alloc.free(doc_key);
    var source: publication.Source = .{ .document_key = doc_key, .content_digest = undefined, .timestamp = timestamp, .input_position = try publication.inputRevision(txn, authority.namespace, request.doc_key) };
    std.crypto.hash.sha2.Sha256.hash(raw, &source.content_digest, .{});
    return .{ .effect_protocol = authorized.effect_protocol, .alloc = alloc, .root = root, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = producer_name, .producer_generation = producer_generation, .artifact_name = artifact_name, .source = source };
}

test "ordered artifact inventory producer template binds every inference field" {
    const template: requests.GeneratedEnrichmentRequest = .{ .kind = .dense_embedding, .index_name = "idx", .doc_key = "", .source_field = "text", .producer_json = "{\"model\":\"one\"}", .expected_dims = 3 };
    var request = template;
    request.doc_key = "doc";
    request.sequence = 90;
    request.reprocess_generation = 3;
    try std.testing.expect(sameDefinition(template, request));
    try std.testing.expectEqualSlices(u8, &definitionDigest(template), &definitionDigest(request));
    request.source_template = "{{other}}";
    try std.testing.expect(!sameDefinition(template, request));
    request = template;
    request.producer_json = "{\"model\":\"two\"}";
    try std.testing.expect(!sameDefinition(template, request));
    request = template;
    request.expected_dims = 4;
    try std.testing.expect(!sameDefinition(template, request));
}

test "privacy base vector source updates replace accepted absence without crediting stale input including time series" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const adapter = @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter;
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/privacy-source-update", .{tmp.sub_path});
    defer alloc.free(path);
    var db = try db_mod.DB.open(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
    defer db.close();
    try db.setSchema(.{ .version = 1, .ttl_field = "observed_at", .ttl_duration_ns = std.math.maxInt(u64) });
    try db.addIndex(.{ .name = "vectors", .kind = .dense_vector, .config_json = "{\"field\":\"body\",\"dims\":2,\"embedding_name\":\"vectors\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"vectors\"}}" });
    var catalog = try db.artifactInventoryCommand(alloc);
    defer catalog.catalogs.deinit(alloc);
    catalog.binding.effect_protocol = 15;
    try adapter.applyOrdered(&db, .{ .artifact_catalog = catalog }, .{ .term = 1, .index = 1 });
    var activate: publication.Command = .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) };
    activate.publication_digest = activate.digest();
    try adapter.applyOrdered(&db, .{ .artifact_publication = activate }, .{ .term = 1, .index = 2 });
    try adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":null,\"observed_at\":\"1969-12-31T23:59:59Z\"}" }}, .timestamp_ns = 100 }, .{ .term = 1, .index = 3 });
    var pin = try db.core.index_manager.acquireWritePlanSnapshot();
    defer pin.release();
    var request = for (pin.plan().generated_templates) |template| {
        if (template.kind == .dense_embedding) break template;
    } else return error.TestExpectedVectorProducer;
    request.doc_key = "doc";
    const stored = try keys.documentKeyAlloc(alloc, "doc");
    defer alloc.free(stored);
    const output = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc", "vectors");
    defer alloc.free(output);
    var before = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        break :blk (try capture(alloc, &read, 1, request, pin.plan(), stored, try read.get(stored), null)).?;
    };
    defer before.deinit();
    const absence = before.command(&.{.{ .family = .base_vector, .key = output, .value = null, .source_index = 0 }});
    try adapter.applyOrdered(&db, .{ .artifact_publication = absence }, .{ .term = 1, .index = 4 });
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try before.accepted(&read));
    }
    try adapter.applyOrdered(&db, .{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"text\",\"observed_at\":\"1969-12-31T23:59:59Z\"}" }}, .timestamp_ns = 200 }, .{ .term = 1, .index = 5 });
    var current = blk: {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectError(error.EnrichmentSourceChanged, before.accepted(&read));
        var token = (try capture(alloc, &read, 1, request, pin.plan(), stored, try read.get(stored), null)).?;
        errdefer token.deinit();
        try std.testing.expect(!try token.accepted(&read));
        break :blk token;
    };
    defer current.deinit();
    const payload = try @import("enrichment/artifact_codec.zig").encodeDenseEmbeddingAlloc(alloc, 1, &.{ 1, 2 });
    defer alloc.free(payload);
    const produced = current.command(&.{.{ .family = .base_vector, .key = output, .value = payload, .source_index = 0 }});
    try adapter.applyOrdered(&db, .{ .artifact_publication = produced }, .{ .term = 1, .index = 6 });
    // A delayed old result must leave the new accepted value/provenance intact.
    try adapter.applyOrdered(&db, .{ .artifact_publication = absence }, .{ .term = 1, .index = 7 });
    var read = try db.core.store.beginReadTxn();
    defer read.abort();
    try std.testing.expect(try current.accepted(&read));
    try std.testing.expectEqualSlices(u8, payload, try read.get(output));
}

test "ordered artifact inventory resolved execution refreshes vectors and fences late results" {
    const a = std.testing.allocator;
    const DB = @import("selected_root.zig").db.DB;
    const codec = @import("enrichment/artifact_codec.zig");
    var tmp = try @import("../../common/test_directory.zig").TestDirectory.init("execution-revision");
    defer tmp.cleanup();
    var db = try DB.open(a, tmp.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .executor = .{ .backend = .manual }, .start_optional_runtimes = false, .start_index_workers = false, .start_independent_maintenance = false });
    defer db.close();
    try db.addIndex(.{ .name = "semantic", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"model\"}}" });
    try db.applyOrderedCommittedMutation(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"same\"}" }}, .sync_level = .write }, .{ .term = 1, .index = 1 }, false);
    var catalog = try db.artifactInventoryCommand(a);
    defer catalog.catalogs.deinit(a);
    try std.testing.expectEqual(@as(u16, 17), catalog.binding.effect_protocol);
    {
        var txn = try db.core.store.beginWriteTxn();
        errdefer txn.abort();
        try inventory.stageOrdered(a, &txn, catalog, 1);
        try publication.stageAuthority(&txn, .{ .mode = .activate, .namespace = catalog.namespace, .authority_epoch = catalog.binding.epoch, .catalog_digest = catalog.binding.digest, .producer_name = "", .producer_generation = 0, .sources = &.{}, .mutations = &.{}, .publication_digest = @splat(0) });
        try txn.commit();
    }
    var pin = try db.core.index_manager.acquireWritePlanSnapshot();
    defer pin.release();
    const Factory = struct {
        fn make(alloc: std.mem.Allocator, owner: *DB, command: inventory.Command, generation: u64, revision: ?u8) !Token {
            var read = try owner.core.store.beginReadTxn();
            defer read.abort();
            const source = try publication.capturePrimarySource(alloc, &read, command.namespace, "doc");
            var token: Token = .{ .effect_protocol = command.binding.effect_protocol, .alloc = alloc, .root = owner.root_incarnation, .namespace = command.namespace, .authority_epoch = command.binding.epoch, .catalog_digest = command.binding.digest, .producer_name = try alloc.dupe(u8, "semantic"), .producer_generation = generation, .artifact_name = try alloc.dupe(u8, "model"), .source = source };
            errdefer token.deinit();
            try token.bindExecution(&read, if (revision) |value| @splat(value) else null);
            return token;
        }
        fn accepted(owner: *DB, token: *Token) !bool {
            var read = try owner.core.store.beginReadTxn();
            defer read.abort();
            return token.accepted(&read);
        }
    };
    const generation = pin.plan().coverageGeneration("semantic").?;
    var unbound = try Factory.make(a, &db, catalog, generation, null);
    defer unbound.deinit();
    var first = try Factory.make(a, &db, catalog, generation, 1);
    defer first.deinit();
    const key = try keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", "model");
    defer a.free(key);
    const one = try codec.encodeDenseEmbeddingAlloc(a, null, &.{ 1, 0 });
    defer a.free(one);
    const two = try codec.encodeDenseEmbeddingAlloc(a, null, &.{ 0, 1 });
    defer a.free(two);
    const first_effect = publication.Mutation{ .family = .base_vector, .key = key, .value = one, .source_index = 0 };
    const second_effect = publication.Mutation{ .family = .base_vector, .key = key, .value = two, .source_index = 0 };
    const first_command = first.command((&first_effect)[0..1]);
    try db.applyOrderedCommittedMutation(.{ .artifact_publication = first_command }, .{ .term = 1, .index = 2 }, false);
    try std.testing.expect(try Factory.accepted(&db, &first));
    var second = try Factory.make(a, &db, catalog, generation, 2);
    defer second.deinit();
    try std.testing.expect(!try Factory.accepted(&db, &second));
    const second_command = second.command((&second_effect)[0..1]);
    try std.testing.expect(!std.mem.eql(u8, &first_command.inputDigest(), &second_command.inputDigest()));
    try db.applyOrderedCommittedMutation(.{ .artifact_publication = second_command }, .{ .term = 1, .index = 3 }, false);
    try std.testing.expect(try Factory.accepted(&db, &second));
    try std.testing.expect(!try Factory.accepted(&db, &first));
    // Old execution observes the same immutable document, but its output CAS
    // cannot replace the vector accepted for the new model revision.
    try db.applyOrderedCommittedMutation(.{ .artifact_publication = first_command }, .{ .term = 1, .index = 4 }, false);
    const current = try db.core.store.get(a, key);
    defer a.free(current);
    try std.testing.expectEqualSlices(u8, two, current);
    var retry = try Factory.make(a, &db, catalog, generation, 2);
    defer retry.deinit();
    try std.testing.expect(try Factory.accepted(&db, &retry));
    try db.applyOrderedCommittedMutation(.{ .artifact_publication = retry.command((&first_effect)[0..1]) }, .{ .term = 1, .index = 5 }, false);
    const canonical = try db.core.store.get(a, key);
    defer a.free(canonical);
    try std.testing.expectEqualSlices(u8, two, canonical);
    // Missing execution identity still fences the output snapshot.
    try db.applyOrderedCommittedMutation(.{ .artifact_publication = unbound.command((&first_effect)[0..1]) }, .{ .term = 1, .index = 6 }, false);
    const after_unbound = try db.core.store.get(a, key);
    defer a.free(after_unbound);
    try std.testing.expectEqualSlices(u8, two, after_unbound);
    // Current unversioned work remains publishable. Returning to the same
    // bytes cannot make the older guard valid again (artifact revision ABA).
    var current_unbound = try Factory.make(a, &db, catalog, generation, null);
    defer current_unbound.deinit();
    try db.applyOrderedCommittedMutation(.{ .artifact_publication = current_unbound.command((&first_effect)[0..1]) }, .{ .term = 1, .index = 7 }, false);
    const unversioned = try db.core.store.get(a, key);
    defer a.free(unversioned);
    try std.testing.expectEqualSlices(u8, one, unversioned);
    try db.applyOrderedCommittedMutation(.{ .artifact_publication = second_command }, .{ .term = 1, .index = 8 }, false);
    const after_aba = try db.core.store.get(a, key);
    defer a.free(after_aba);
    try std.testing.expectEqualSlices(u8, one, after_aba);
    // Explicit ingress stays canonical even when model resolution changes.
    try db.applyOrderedCommittedMutation(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"same\",\"_embeddings\":{\"model\":[1,0]}}" }}, .sync_level = .write }, .{ .term = 1, .index = 9 }, false);
    var authored = try Factory.make(a, &db, catalog, generation, 3);
    defer authored.deinit();
    {
        var read = try db.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expect(try authored.authoredAccepted(&read));
    }
    try std.testing.expect(try Factory.accepted(&db, &authored));
}

test "ordered artifact inventory grouped sources production catalog activation upgrades and preserves protocol floors" {
    const a = std.testing.allocator;
    const DB = @import("selected_root.zig").db.DB;
    const groups = @import("group_source_contract.zig");
    const Observe = struct {
        fn binding(db: *DB) !inventory.Binding {
            var read = try db.core.store.beginReadTxn();
            defer read.abort();
            var installed = (try inventory.load(std.testing.allocator, &read)).?;
            defer installed.deinit();
            return installed.value.command.binding;
        }
        fn ready(db: *DB, definition: @import("group_source_worker.zig").Definition) !bool {
            const query = try std.json.Stringify.valueAlloc(std.testing.allocator, groups.Query{ .action = .activation, .artifact = definition.artifact, .definition = definition.definition }, .{});
            defer std.testing.allocator.free(query);
            const response = (try db.lookup(std.testing.allocator, "", .{ .group_source_query_json = query })).?;
            defer std.testing.allocator.free(response.json);
            return std.mem.eql(u8, response.json, "true");
        }
    };
    for ([_]bool{ false, true }) |legacy_first| {
        var tmp = try @import("../../common/test_directory.zig").TestDirectory.init("group-catalog-protocol");
        defer tmp.cleanup();
        var db = try DB.open(a, tmp.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 3 }, .online_source_authority = .raft, .executor = .{ .backend = .manual }, .start_optional_runtimes = false, .start_index_workers = false, .start_independent_maintenance = false });
        defer db.close();
        try db.addIndex(.{ .name = "semantic", .kind = .dense_vector, .config_json = "{\"field\":\"embedding\",\"dims\":2,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"model\"}}" });
        try db.addEnrichment(.{ .name = "turns", .kind = .group, .source = .{ .group_by = "thread_id", .order_by = "ordinal", .segment_by = "turn_id", .field = "text" } });
        const definitions = (try db.lookup(a, "", .{ .group_source_query_json = "{\"action\":\"definitions\"}" })).?;
        defer a.free(definitions.json);
        var decoded = try std.json.parseFromSlice([]@import("group_source_worker.zig").Definition, a, definitions.json, .{ .allocate = .alloc_always });
        defer decoded.deinit();
        const definition = decoded.value[0];
        var desired = try db.artifactInventoryCommand(a);
        defer desired.catalogs.deinit(a);
        try std.testing.expectEqual(@as(u16, 17), desired.binding.effect_protocol);
        const command = groups.Command{ .action = .activate_catalog, .artifact = definition.artifact, .definition = definition.definition, .namespace = desired.namespace, .backfill = .{ .route = definition.route } };
        var index: u64 = 1;
        if (legacy_first) {
            // Old log entries still reproduce protocol 16. New readiness must
            // detect the missing producer capability despite matching bytes.
            var legacy = command;
            legacy.action = .activate;
            for (0..2) |_| {
                try db.applyOrderedCommittedMutation(.{ .group_source = legacy, .sync_level = .write }, .{ .term = 1, .index = index }, false);
                index += 1;
                try std.testing.expectEqual(@as(u16, 16), (try Observe.binding(&db)).effect_protocol);
                try std.testing.expect(!try Observe.ready(&db, definition));
            }
        }
        try db.applyOrderedCommittedMutation(.{ .group_source = command, .sync_level = .write }, .{ .term = 1, .index = index }, false);
        index += 1;
        const activated = try Observe.binding(&db);
        try std.testing.expectEqual(@as(u16, 17), activated.effect_protocol);
        try std.testing.expect(try Observe.ready(&db, definition));
        // Duplicate controls must not advance the catalog epoch or invalidate
        // in-flight producer leases and authored-vector certificates.
        try db.applyOrderedCommittedMutation(.{ .group_source = command, .sync_level = .write }, .{ .term = 1, .index = index }, false);
        index += 1;
        try std.testing.expectEqual(activated, try Observe.binding(&db));
        try db.applyOrderedCommittedMutation(.{ .writes = &.{.{ .key = "doc", .value = "{\"body\":\"same\"}" }}, .sync_level = .write }, .{ .term = 1, .index = index }, false);
        index += 1;
        {
            var pin = try db.core.index_manager.acquireWritePlanSnapshot();
            defer pin.release();
            var token = blk: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                const source = try publication.capturePrimarySource(a, &read, desired.namespace, "doc");
                var result = Token{ .effect_protocol = activated.effect_protocol, .alloc = a, .root = db.root_incarnation, .namespace = desired.namespace, .authority_epoch = activated.epoch, .catalog_digest = activated.digest, .producer_name = try a.dupe(u8, "semantic"), .producer_generation = pin.plan().coverageGeneration("semantic").?, .artifact_name = try a.dupe(u8, "model"), .source = source };
                errdefer result.deinit();
                try result.bindExecution(&read, @splat(1));
                break :blk result;
            };
            defer token.deinit();
            const key = try keys.embeddingArtifactKeyForDocumentAlloc(a, "doc", "model");
            defer a.free(key);
            const vector = try @import("enrichment/artifact_codec.zig").encodeDenseEmbeddingAlloc(a, null, &.{ 1, 0 });
            defer a.free(vector);
            try db.applyOrderedCommittedMutation(.{ .artifact_publication = token.command(&.{.{ .family = .base_vector, .key = key, .value = vector, .source_index = 0 }}) }, .{ .term = 1, .index = index }, false);
            index += 1;
            const stored = try db.core.store.get(a, key);
            defer a.free(stored);
            try std.testing.expectEqualSlices(u8, vector, stored);
        }
        // Removing the protocol-17 consumer leaves a group-only catalog, but
        // neither normal inventory planning nor grouped activation may lower
        // the installed floor while reconciling the changed catalog.
        try std.testing.expect(try db.deleteIndex("semantic"));
        try std.testing.expect(!try Observe.ready(&db, definition));
        var next = try db.artifactInventoryCommand(a);
        defer next.catalogs.deinit(a);
        try std.testing.expectEqual(@as(u16, 16), try next.catalogs.requiredEffectProtocol(a));
        try std.testing.expectEqual(@as(u16, 17), next.binding.effect_protocol);
        try db.applyOrderedCommittedMutation(.{ .group_source = command, .sync_level = .write }, .{ .term = 1, .index = index }, false);
        try std.testing.expectEqual(next.binding, try Observe.binding(&db));
        try std.testing.expect(try Observe.ready(&db, definition));
    }
}
