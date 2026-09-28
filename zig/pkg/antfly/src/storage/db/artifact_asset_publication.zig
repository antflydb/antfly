// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Asset providers author bytes; the ordered catalog authorizes their exact
//! output and derives durable projection work. Local skip-state is not proof.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const inventory = @import("artifact_inventory.zig");
const context = @import("artifact_producer_context.zig");
const requests = @import("enrichment/enrichment_types.zig");
const producer_input = @import("artifact_producer_input.zig");
const manager = @import("catalog/index_manager.zig");
const keys = @import("../internal_keys.zig");

pub fn authorize(alloc: std.mem.Allocator, txn: anytype, request: requests.GeneratedEnrichmentRequest, plan: *const manager.IndexManager.WritePlanSnapshot) !?producer_input.TemplateAuthority {
    if (try publication.authority(txn) == null) return null;
    if ((request.kind != .asset and request.kind != .chunk_text) or request.neighbor_context_json.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    const bound = (try producer_input.authorizeTemplate(alloc, txn, request, plan)) orelse return error.ArtifactCatalogDrift;
    if (request.kind == .asset and bound.requirement.scope != .document) return error.OnlineMergeArtifactTailsUnsupported;
    return bound;
}

pub fn capture(alloc: std.mem.Allocator, txn: anytype, request: requests.GeneratedEnrichmentRequest, plan: *const manager.IndexManager.WritePlanSnapshot, source_key: []const u8, observed: ?[]const u8, materializer: producer_input.Materializer) !?context.Token {
    const authorized = (try authorize(alloc, txn, request, plan)) orelse return null;
    const authority = authorized.authority;
    var arena = std.heap.ArenaAllocator.init(alloc);
    var arena_owned = true;
    errdefer if (arena_owned) arena.deinit();
    const owned = arena.allocator();
    const source = publication.capturePrimarySource(owned, txn, authority.namespace, request.doc_key) catch |err| switch (err) {
        error.EnrichmentSourceChanged => try publication.capturePrimaryTombstoneSource(owned, txn, authority.namespace, request.doc_key),
        else => return err,
    };
    if (source.exists) {
        const expected = observed orelse return error.EnrichmentSourceChanged;
        const raw = txn.get(source_key) catch |err| switch (err) {
            error.NotFound => return error.EnrichmentSourceChanged,
            else => return err,
        };
        const logical = try materializer.materialize(materializer.ptr, alloc, source_key, raw);
        defer alloc.free(logical);
        if (!std.mem.eql(u8, expected, logical)) return error.EnrichmentSourceChanged;
    } else if (observed != null) return error.EnrichmentSourceChanged;
    const name = if (request.artifact_name.len != 0) request.artifact_name else request.index_name;
    const owned_name = try owned.dupe(u8, name);
    var token: context.Token = .{
        .arena = arena,
        .namespace = authority.namespace,
        .epoch = authority.epoch,
        .catalog_digest = authority.catalog_digest,
        .producer_kind = .enrichment,
        .producer_name = owned_name,
        // Enrichments have no owner-local index generation. Their immutable
        // definition is identified by the authority epoch plus catalog digest.
        .producer_generation = authority.epoch,
        .artifact_name = owned_name,
        .source = source,
    };
    // From here the token owns the arena; propagate its possibly grown arena
    // state even on failure (do not deinitialize the stale local arena copy).
    arena_owned = false;
    errdefer token.deinit();
    const output_key = if (request.kind == .chunk_text)
        try @import("artifact_chunk_manifest.zig").keyAlloc(token.arena.allocator(), request.doc_key, name)
    else
        try keys.artifactNamedPrefixAlloc(token.arena.allocator(), request.doc_key, "asset", name);
    const existing = optional(txn, output_key) catch |err| return err;
    if (request.kind == .chunk_text) _ = try @import("artifact_chunk_manifest.zig").Manifest.decode(existing orelse return error.ArtifactCoverageBaselinePending);
    try token.observePrecondition(output_key, existing, try publication.artifactRevision(txn, authority.namespace, output_key));
    return token;
}

fn optional(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

/// The value consumed by a provider and its accepted causal proof are sampled
/// together. A durable but stale upstream result cannot authorize new output.
pub fn readUpstream(alloc: std.mem.Allocator, token: *context.Token, txn: anytype, key: []const u8) !?[]u8 {
    var input = try @import("artifact_extraction_generation.zig").captureInput(alloc, txn, key);
    defer input.deinit();
    const raw = input.value;
    if ((raw != null or input.head_value != null or input.require_absence_proof) and token.source.exists) {
        var proof = (try @import("artifact_producer_provenance.zig").readCurrentForArtifact(alloc, txn, input.proofKey(key), input.proofValue())) orelse return error.ArtifactPublicationPending;
        defer proof.deinit();
        try token.inheritProof(proof.proof);
    }
    try input.observe(token, txn, key);
    return if (raw) |value| try alloc.dupe(u8, value) else null;
}

pub fn prepare(alloc: std.mem.Allocator, command: publication.Command, catalogs: inventory.Catalogs) !publication.PreparedEffects {
    try command.validate(alloc);
    if (command.mode != .publish or command.producer_kind != .enrichment or
        command.producer_generation != command.authority_epoch or !std.mem.eql(u8, command.producer_name, command.producer_artifact_name)) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const configs = try @import("catalog/enrichment_catalog.zig").deserializeCatalog(owned, catalogs.enrichments);
    const config = for (configs) |value| {
        if (std.mem.eql(u8, value.name, command.producer_name)) break value;
    } else return error.EnrichmentSourceChanged;
    if (config.kind == .chunk) {
        const prepared = try @import("artifact_chunk_publication.zig").prepare(alloc, command, catalogs);
        arena.deinit();
        return prepared;
    }
    if (config.kind != .asset or command.producer_scope_key.len != 0) return error.InvalidBatchRequest;
    const producer = try @import("enrichment/asset_producer.zig").parseProducerConfig(owned, config.producer_json);
    if (producer.type == .document_extraction or config.neighbor_context_json.len != 0) return error.OnlineMergeArtifactTailsUnsupported;
    for (command.mutations) |effect| {
        const source = command.sources[effect.source_index];
        const expected = try keys.artifactNamedPrefixAlloc(owned, source.document_key, "asset", config.name);
        if (effect.family != .document_artifact or !std.mem.eql(u8, expected, effect.key)) return error.InvalidBatchRequest;
        try requireUpstream(owned, command, config, effect);
    }
    return prepareTextEffects(&arena, command, catalogs, config, configs, command.mutations, null, true);
}

pub fn requireUpstream(alloc: std.mem.Allocator, command: publication.Command, config: @import("catalog/enrichment_catalog.zig").EnrichmentConfig, effect: publication.Mutation) !void {
    const source = command.sources[effect.source_index];
    if (config.source_artifact_name.len == 0 or !source.exists) return;
    const upstream = try keys.artifactNamedPrefixAlloc(alloc, source.document_key, "asset", config.source_artifact_name);
    defer alloc.free(upstream);
    for (command.artifact_sources) |guard| {
        if (guard.source_index == effect.source_index and std.mem.eql(u8, guard.key, upstream)) {
            if (effect.value != null and guard.content_digest == null) return error.InvalidBatchRequest;
            return;
        }
    }
    return error.InvalidBatchRequest;
}

/// Shared projections consume already-authorized members, never the private
/// inventory witness. Coverage is reconciled once per owner/consumer, not once
/// per chunk; empty streams still have an owner through their manifest effect.
pub fn prepareTextEffects(arena: *std.heap.ArenaAllocator, command: publication.Command, catalogs: inventory.Catalogs, config: @import("catalog/enrichment_catalog.zig").EnrichmentConfig, configs: []const @import("catalog/enrichment_catalog.zig").EnrichmentConfig, effects: []const publication.Mutation, text_members: ?[]const bool, document_coverage: bool) !publication.PreparedEffects {
    const owned = arena.allocator();
    const indexes: []const @import("types.zig").IndexConfig = if (catalogs.indexes.len == 0) &.{} else try manager.deserializeCatalog(owned, catalogs.indexes);
    const derived = @import("derived/derived_types.zig");
    const Member = struct { name: []const u8, kind: @import("catalog/enrichment_catalog.zig").EnrichmentType };
    const Consumer = struct { name: []const u8, generation: u64, members: []const Member };
    var consumers: std.ArrayList(Consumer) = .empty;
    var targets: std.ArrayList(derived.DerivedTargetRef) = .empty;
    const include_default = try defaultTextConsumer(owned, config);
    const defaults = try owned.alloc(bool, configs.len);
    for (configs, defaults) |candidate, *default| default.* = try defaultTextConsumer(owned, candidate);
    for (indexes) |index| {
        if (index.kind != .full_text) continue;
        const text = try manager.TextArtifactConsumer.init(owned, index.config_json);
        defer text.deinit(owned);
        if (!text.consumes(config.name, include_default)) continue;
        try targets.append(owned, .{ .kind = .full_text, .index_name = index.name });
        // A unit publication projects its members but cannot settle a whole
        // document's coverage. The upstream scope census owns that boundary.
        if (!document_coverage) continue;
        var members: std.ArrayList(Member) = .empty;
        for (configs, defaults) |candidate, default| {
            if (candidate.kind == .embedding or !text.consumes(candidate.name, default)) continue;
            try members.append(owned, .{ .name = candidate.name, .kind = candidate.kind });
        }
        try consumers.append(owned, .{ .name = index.name, .generation = keys.derivedCoverageGenerationForConfig(index.coverage_generation, index.config_json), .members = members.items });
    }
    var documents: std.ArrayList(derived.DerivedDocument) = .empty;
    const changed = try owned.alloc([]const u8, effects.len);
    var coverage: std.ArrayList(publication.Coverage) = .empty;
    for (effects, changed, 0..) |effect, *changed_key, ordinal| {
        const expected = try owned.dupe(u8, effect.key);
        changed_key.* = expected;
        // Binary chunks cannot leave an old textual posting behind when a
        // producer changes MIME type. Emit a text deletion for that identity.
        const text_value = if (text_members != null and !text_members.?[ordinal]) null else effect.value;
        if (targets.items.len != 0) try documents.append(owned, .{
            .key = expected,
            .action = if (text_value != null) .upsert else .delete,
            .cleaned_value = if (text_value) |value| try owned.dupe(u8, value) else null,
            .targets = targets.items,
        });
    }
    var seen = try std.DynamicBitSetUnmanaged.initEmpty(owned, command.sources.len);
    for (command.mutations) |effect| {
        if (seen.isSet(effect.source_index)) continue;
        seen.set(effect.source_index);
        const source = command.sources[effect.source_index];
        for (consumers.items) |consumer| {
            // Compile catalog membership once per publication, not once per
            // row or sibling. Coverage remains a bounded set of point reads.
            const coverage_keys = try owned.alloc([]const u8, consumer.members.len);
            for (consumer.members, coverage_keys) |member, *key| {
                key.* = if (member.kind == .asset)
                    try keys.artifactNamedPrefixAlloc(owned, source.document_key, "asset", member.name)
                else
                    try keys.chunkArtifactKeyAlloc(owned, source.document_key, member.name, 0);
            }
            try coverage.append(owned, .{ .index_name = consumer.name, .generation = consumer.generation, .document_key = source.document_key, .artifact_keys = coverage_keys });
        }
    }
    return .{ .arena = arena.*, .batch = .{ .documents = documents.items, .changed_artifact_keys = changed }, .coverage = coverage.items, .target_hints = &.{ .enrichment, .full_text, .graph, .resolution } };
}

fn defaultTextConsumer(alloc: std.mem.Allocator, config: @import("catalog/enrichment_catalog.zig").EnrichmentConfig) !bool {
    return config.full_text_index or (config.kind == .chunk and try @import("../../chunking/types.zig").parseHasFullTextIndexFromSlice(alloc, config.chunker_json));
}
