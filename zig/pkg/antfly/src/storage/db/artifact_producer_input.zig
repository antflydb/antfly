// Copyright 2026 Antfly, Inc. Licensed under the Elastic License 2.0.
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

pub const DocumentAuthority = struct { authority: publication.Authority, generation: u64, requirement: publication.Digest };
pub const TemplateAuthority = struct { authority: publication.Authority, requirement: *const @import("artifact_completion_plan.zig").Node };

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
    if (binding.effect_protocol != 15 or binding.epoch != authority.epoch or
        !std.mem.eql(u8, &binding.digest, &authority.catalog_digest) or
        !std.mem.eql(u8, &ordered.value.command.namespace, &authority.namespace)) return error.ArtifactCatalogDrift;
    const local = try inventory.catalogs(txn);
    if (!std.mem.eql(u8, &local.digest(), &binding.digest) or !plan.matchesArtifactInventory(local)) return error.ArtifactCatalogDrift;
    return .{ .authority = authority, .requirement = requirement };
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
    return .{ .authority = authority, .generation = generation, .requirement = requirement.id };
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

    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.producer_name);
        self.alloc.free(self.artifact_name);
        self.alloc.free(@constCast(self.source.document_key));
        self.* = undefined;
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
    pub fn accepted(self: *const Token, txn: anytype) !bool {
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
        // Receipts survive output replacement. Only current causal/output
        // provenance can suppress execution for a generated result.
        var generated = (try @import("artifact_producer_provenance.zig").readCurrentForSource(self.alloc, txn, self.command(&.{}), self.source)) orelse return false;
        defer generated.deinit();
        return true;
    }
};

/// Compare every provider/planning field, excluding only row identity,
/// owner-local replay position and the derived consumer list. Those three
/// fields cannot authorize a different generator definition. Consumer
/// projections are independently derived from the certified catalog at apply.
pub fn sameDefinition(a: requests.GeneratedEnrichmentRequest, b: requests.GeneratedEnrichmentRequest) bool {
    inline for (std.meta.fields(requests.GeneratedEnrichmentRequest)) |field| {
        if (comptime !definitionField(field.name)) continue;
        const av = @field(a, field.name);
        const bv = @field(b, field.name);
        if (comptime @typeInfo(field.type) == .pointer) {
            if (!std.mem.eql(u8, av, bv)) return false;
        } else if (!std.meta.eql(av, bv)) return false;
    }
    return true;
}

fn definitionField(comptime name: []const u8) bool {
    return !std.mem.eql(u8, name, "doc_key") and !std.mem.eql(u8, name, "sequence") and !std.mem.eql(u8, name, "consumer_indexes");
}

/// Stable identity for exactly the fields used by sameDefinition. Framed
/// field names and values distinguish binary strings and future definitions;
/// a new field type must choose an explicit canonical encoding here.
pub fn definitionDigest(request: requests.GeneratedEnrichmentRequest) publication.Digest {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly:producer-definition:v1:");
    inline for (std.meta.fields(requests.GeneratedEnrichmentRequest)) |field| {
        if (comptime !definitionField(field.name)) continue;
        hashDefinitionBytes(&hash, field.name);
        const value = @field(request, field.name);
        switch (@typeInfo(field.type)) {
            .pointer => hashDefinitionBytes(&hash, value),
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
    inline for (std.meta.fields(requests.GeneratedEnrichmentRequest)) |field| {
        if (comptime !definitionField(field.name)) continue;
        var changed = request;
        switch (@typeInfo(field.type)) {
            .pointer => @field(changed, field.name) = "\x00changed",
            .@"enum" => @field(changed, field.name) = std.meta.tags(field.type)[(@as(usize, @intFromEnum(@field(request, field.name))) + 1) % std.meta.tags(field.type).len],
            .bool => @field(changed, field.name) = !@field(request, field.name),
            .int => @field(changed, field.name) += 1,
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
    if (request.sequence != 0) {
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
    if (timestamp_raw.len != 8) return error.EnrichmentSourceChanged;
    const timestamp = std.mem.readInt(u64, timestamp_raw[0..8], .little);
    if (timestamp == 0) return error.EnrichmentSourceChanged;
    const producer_name = try alloc.dupe(u8, request.index_name);
    errdefer alloc.free(producer_name);
    const artifact_name = try alloc.dupe(u8, requests.requestEmbeddingName(request));
    errdefer alloc.free(artifact_name);
    const doc_key = try alloc.dupe(u8, request.doc_key);
    errdefer alloc.free(doc_key);
    var source: publication.Source = .{ .document_key = doc_key, .content_digest = undefined, .timestamp = timestamp, .input_position = try publication.inputRevision(txn, authority.namespace, request.doc_key) };
    std.crypto.hash.sha2.Sha256.hash(raw, &source.content_digest, .{});
    return .{ .alloc = alloc, .root = root, .namespace = authority.namespace, .authority_epoch = authority.epoch, .catalog_digest = authority.catalog_digest, .producer_name = producer_name, .producer_generation = producer_generation, .artifact_name = artifact_name, .source = source };
}

test "ordered artifact inventory producer template binds every inference field" {
    const template: requests.GeneratedEnrichmentRequest = .{ .kind = .dense_embedding, .index_name = "idx", .doc_key = "", .source_field = "text", .producer_json = "{\"model\":\"one\"}", .expected_dims = 3 };
    var request = template;
    request.doc_key = "doc";
    request.sequence = 90;
    try std.testing.expect(sameDefinition(template, request));
    request.source_template = "{{other}}";
    try std.testing.expect(!sameDefinition(template, request));
    request = template;
    request.producer_json = "{\"model\":\"two\"}";
    try std.testing.expect(!sameDefinition(template, request));
    request = template;
    request.expected_dims = 4;
    try std.testing.expect(!sameDefinition(template, request));
}
