// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Resolver input authority. Provider execution does not retain a storage
//! snapshot: every observed input is checked together at ordered publication.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const inventory = @import("artifact_inventory.zig");
const catalog = @import("catalog/resolver_catalog.zig");
const resolver_lib = @import("antfly_resolver");
const runtime = @import("resolution_runtime.zig");
const keys = @import("../internal_keys.zig");

pub const Token = @import("artifact_producer_context.zig").Token;

/// The caller reads authority, catalog and primary input in one read
/// transaction. No local replay number substitutes for owner position.
fn capture(alloc: std.mem.Allocator, txn: anytype, config: catalog.ResolverConfig, doc_key: []const u8) !?Token {
    const authority = (try publication.authority(txn)) orelse return null;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const ordered = (try inventory.load(owned, txn)) orelse return error.ArtifactCatalogDrift;
    const binding = ordered.value.command.binding;
    if (binding.effect_protocol != 15 or binding.epoch != authority.epoch or
        !std.mem.eql(u8, &binding.digest, &authority.catalog_digest) or
        !std.mem.eql(u8, &ordered.value.command.namespace, &authority.namespace)) return error.ArtifactCatalogDrift;
    const local = try inventory.catalogs(txn);
    if (!std.mem.eql(u8, &local.digest(), &binding.digest)) return error.ArtifactCatalogDrift;
    const resolvers = try catalog.deserializeCatalog(owned, local.resolvers);
    const expected = try std.json.Stringify.valueAlloc(owned, config, .{});
    const matched = for (resolvers) |resolver| {
        if (!std.mem.eql(u8, resolver.name, config.name)) continue;
        const actual = try std.json.Stringify.valueAlloc(owned, resolver, .{});
        break std.mem.eql(u8, expected, actual);
    } else false;
    if (!matched) return error.ArtifactCatalogDrift;
    const producer_name = try owned.dupe(u8, config.name);
    const artifact_name = try owned.dupe(u8, config.resolution_artifact);
    const source = publication.capturePrimarySource(owned, txn, authority.namespace, doc_key) catch |err| switch (err) {
        error.EnrichmentSourceChanged => try publication.capturePrimaryTombstoneSource(owned, txn, authority.namespace, doc_key),
        else => return err,
    };
    return .{
        .arena = arena,
        .namespace = authority.namespace,
        .epoch = authority.epoch,
        .catalog_digest = authority.catalog_digest,
        .producer_name = producer_name,
        .producer_kind = .resolver,
        .producer_generation = config.config_generation,
        .artifact_name = artifact_name,
        .source = source,
    };
}

fn setSourceScope(self: *Token, source_key: []const u8) !void {
    const alloc = self.arena.allocator();
    var parsed = (try runtime.parseSourceArtifactKeyAlloc(alloc, source_key)) orelse return error.InvalidBatchRequest;
    defer parsed.deinit(alloc);
    if (!std.mem.eql(u8, parsed.doc_key, self.source.document_key)) return error.InvalidBatchRequest;
    self.producer_scope_key = if (std.mem.eql(u8, parsed.resolution_scope_key, parsed.doc_key)) "" else try alloc.dupe(u8, source_key);
}

/// Process one replay window through the ordered owner. A successful enqueue
/// never advances the resolver watermark. Each sibling starts only after the
/// previous resolver's input-bound receipt is locally durable.
pub fn process(
    alloc: std.mem.Allocator,
    store: anytype,
    dispatcher: ?publication.Dispatcher,
    resolvers: []const catalog.ResolverConfig,
    fallback: resolver_lib.ArtifactStore,
    provider: ?resolver_lib.CandidateProvider,
    changed_keys: []const []const u8,
    candidate_source: ?runtime.CandidateSource,
    embedder: ?@import("enrichment/embedder.zig").DenseEmbedder,
) !bool {
    {
        var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
        defer read.abort();
        if (try publication.authority(&read) == null) return false;
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    var sources: std.ArrayList([]const u8) = .empty;
    for (changed_keys) |key| {
        const source_key = if (try runtime.parseSourceArtifactKeyAlloc(scratch, key) != null) key else blk: {
            const source = (try runtime.sourceExtractionKeyForResolutionKeyAlloc(scratch, resolvers, key)) orelse continue;
            // Retirement deletes do not re-drive and resurrect their output.
            if (try fallback.get(scratch, key) == null) continue;
            break :blk source;
        };
        for (sources.items) |existing| {
            if (std.mem.eql(u8, existing, source_key)) break;
        } else try sources.append(scratch, source_key);
    }
    for (sources.items) |key| {
        var parsed = (try runtime.parseSourceArtifactKeyAlloc(scratch, key)) orelse continue;
        defer parsed.deinit(scratch);
        for (resolvers) |*config| {
            if (!runtime.resolverMatchesArtifact(config, parsed.source_artifact_kind, parsed.artifact_name)) continue;
            var work_arena = std.heap.ArenaAllocator.init(alloc);
            defer work_arena.deinit();
            const work = work_arena.allocator();
            var token = blk: {
                var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
                defer read.abort();
                break :blk (try capture(alloc, &read, config.*, parsed.doc_key)) orelse return error.ArtifactCatalogDrift;
            };
            defer token.deinit();
            try setSourceScope(&token, key);
            {
                // Discover the accepted input set by stable stream identity
                // before candidate lookup or provider execution. Comparing a
                // freshly empty recorder with the receipt cannot find an
                // accepted causal read set, and would repeat inference after
                // every lost reply/restarted replay window.
                var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
                defer read.abort();
                const selector = try token.command(&.{});
                var accepted = @import("artifact_producer_provenance.zig").readCurrentForSource(alloc, &read, selector, token.source) catch |err| switch (err) {
                    error.EnrichmentSourceChanged => null,
                    else => return err,
                };
                if (accepted) |*value| {
                    value.deinit();
                    continue;
                }
            }
            const output_key = try keys.resolutionArtifactKeyAlloc(work, parsed.resolution_scope_key, config.resolution_artifact);
            var recorder: RecordingStore(@TypeOf(store.*)) = .{ .store = store, .fallback = fallback, .token = &token, .output_key = output_key, .upstream_key = key };
            const value: ?[]const u8 = if (!token.source.exists) blk: {
                // A genuine primary tombstone authorizes cleanup only. Never
                // run inference on an orphaned extraction or resurrect output.
                _ = try recorder.artifactStore().get(work, key);
                _ = try recorder.artifactStore().get(work, output_key);
                break :blk null;
            } else blk: {
                const outcome = (try runtime.processChangedExtractionWithConfig(work, resolvers, config, recorder.artifactStore(), provider, key, candidate_source, embedder, .deferred, &.{})) orelse return error.ArtifactCatalogDrift;
                break :blk switch (outcome.result) {
                    .written => outcome.resolution_value,
                    .unchanged => try recorder.artifactStore().get(work, output_key),
                    .cleared, .source_missing => null,
                };
            };
            const command = try token.command(&.{.{ .family = .resolution, .key = output_key, .value = value, .source_index = 0 }});
            try command.validate(work);
            {
                var read = try store.beginReadTxnWithBlockCacheAdmission(.transient);
                defer read.abort();
                try token.validateInputs(work, &read);
                // The accepted receipt precedes CAS validation: accepting this
                // very output necessarily changes its old-value precondition.
                if (try publication.readReceipt(&read, command, token.source) != null) continue;
                try publication.validateArtifactSources(work, &read, token.namespace, token.sources(), token.preconditions.items);
            }
            const port = dispatcher orelse return error.ArtifactCatalogDrift;
            const bytes = try @import("artifact_publication_transport_codec.zig").encodeAlloc(work, command);
            try port.submit(token.namespace, bytes);
            return error.ArtifactPublicationPending;
        }
    }
    return true;
}

/// Reads authored artifact inputs and their mutation position from the same
/// short snapshot. The resolver's own existing output is a CAS precondition,
/// not an input dependency (otherwise publishing would change its job key).
/// Metadata handoff records and candidate queries use the original adapter.
pub fn RecordingStore(comptime Store: type) type {
    return struct {
        store: *Store,
        fallback: resolver_lib.ArtifactStore,
        token: *Token,
        output_key: []const u8,
        upstream_key: []const u8,
        const Self = @This();

        pub fn artifactStore(self: *Self) resolver_lib.ArtifactStore {
            return .{ .ptr = self, .vtable = &.{ .get = get, .put = put, .delete = delete, .scan_prefix = scan, .materialize_row = materialize } };
        }

        fn get(ptr: *anyopaque, alloc: std.mem.Allocator, key: []const u8) anyerror!?[]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (!publication.guardedArtifactKey(key))
                return self.fallback.get(alloc, key);
            var txn = try self.store.beginReadTxnWithBlockCacheAdmission(.transient);
            defer txn.abort();
            var input = try @import("artifact_extraction_generation.zig").captureInput(alloc, &txn, key);
            defer input.deinit();
            const raw = input.value;
            if (std.mem.eql(u8, self.output_key, key)) {
                const position = try publication.artifactRevision(&txn, self.token.namespace, key);
                try self.token.observePrecondition(key, raw, position);
            } else {
                if (self.token.source.exists) {
                    var upstream = try @import("artifact_producer_provenance.zig").readCurrentForArtifact(alloc, &txn, input.proofKey(key), input.proofValue());
                    defer if (upstream) |*proof| proof.deinit();
                    if (upstream) |proof| {
                        try self.token.inheritProof(proof.proof);
                    } else if (input.head_value != null or input.require_absence_proof or (raw != null and std.mem.eql(u8, key, self.upstream_key))) return error.ArtifactPublicationPending;
                }
                try input.observe(self.token, &txn, key);
            }
            return if (raw) |value| try alloc.dupe(u8, value) else null;
        }

        fn put(_: *anyopaque, _: []const u8, _: []const u8) anyerror!void {
            return error.ArtifactCatalogDrift;
        }
        fn delete(_: *anyopaque, _: []const u8) anyerror!void {
            return error.ArtifactCatalogDrift;
        }
        fn scan(ptr: *anyopaque, lower: []const u8, upper: []const u8, ctx: *anyopaque, consume: *const fn (*anyopaque, []const u8, []const u8) anyerror!void) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.fallback.scanPrefix(lower, upper, ctx, consume);
        }
        fn materialize(ptr: *anyopaque, alloc: std.mem.Allocator, key: []const u8, raw: []const u8) anyerror![]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.fallback.materializeRow(alloc, key, raw);
        }
    };
}

/// Ordered apply derives replay work from validated final resolution bytes;
/// neither provider callers nor the wire supply replay payloads or hints.
pub fn prepareResolution(alloc: std.mem.Allocator, command: publication.Command, catalogs: inventory.Catalogs) !publication.PreparedEffects {
    try command.validate(alloc);
    if (command.mode != .publish or command.producer_kind != .resolver) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const configs = try catalog.deserializeCatalog(owned, catalogs.resolvers);
    const config = for (configs) |candidate| {
        if (std.mem.eql(u8, candidate.name, command.producer_name)) break candidate;
    } else return error.EnrichmentSourceChanged;
    if (config.config_generation != command.producer_generation or
        !std.mem.eql(u8, config.resolution_artifact, command.producer_artifact_name)) return error.EnrichmentSourceChanged;
    const changed = try owned.alloc([]const u8, command.mutations.len);
    for (command.mutations, 0..) |mutation, i| {
        if (mutation.family != .resolution or !keys.isResolutionArtifactKey(mutation.key)) return error.InvalidBatchRequest;
        var authorized = false;
        for (command.artifact_sources) |source| {
            if (source.source_index != mutation.source_index) continue;
            var parsed = (try runtime.parseSourceArtifactKeyAlloc(owned, source.key)) orelse continue;
            defer parsed.deinit(owned);
            if (!runtime.resolverMatchesArtifact(&config, parsed.source_artifact_kind, parsed.artifact_name)) continue;
            const expected = try keys.resolutionArtifactKeyAlloc(owned, parsed.resolution_scope_key, config.resolution_artifact);
            if (!std.mem.eql(u8, expected, mutation.key)) continue;
            if (mutation.value != null and source.content_digest == null) return error.InvalidBatchRequest;
            authorized = true;
            break;
        }
        if (!authorized) return error.InvalidBatchRequest;
        if (mutation.value) |raw| {
            var result = resolver_lib.parseResolution(owned, raw) catch return error.InvalidBatchRequest;
            defer result.deinit();
            if (result.config_generation != command.producer_generation) return error.InvalidBatchRequest;
            for (result.entities) |entity| if (!std.mem.eql(u8, entity.doc_ref.table, config.table)) return error.InvalidBatchRequest;
        }
        changed[i] = try owned.dupe(u8, mutation.key);
    }
    return .{ .arena = arena, .batch = .{ .changed_artifact_keys = changed }, .target_hints = &.{ .graph, .promotion, .resolution } };
}

/// A handoff marker certifies the atomic durable replay/outbox obligation,
/// not that graph or remote entity consumers have already completed it.
pub fn stageHandoff(alloc: std.mem.Allocator, txn: anytype, command: publication.Command) !void {
    if (command.producer_kind != .resolver) return error.InvalidBatchRequest;
    const handoff = @import("resolution_handoff.zig");
    for (command.mutations) |mutation| {
        if (mutation.family != .resolution) return error.InvalidBatchRequest;
        const marker = try handoff.keyAlloc(alloc, mutation.key);
        defer alloc.free(marker);
        if (mutation.value) |raw| {
            try txn.put(marker, &handoff.value(raw));
        } else txn.delete(marker) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }
}

test "ordered resolver input read set rejects mixed values and changed absences" {
    var token: Token = .{
        .arena = std.heap.ArenaAllocator.init(std.testing.allocator),
        .namespace = @splat(1),
        .epoch = 1,
        .catalog_digest = @splat(2),
        .producer_name = "resolver",
        .producer_kind = .resolver,
        .producer_generation = 7,
        .artifact_name = "decisions",
        .source = .{ .document_key = "doc", .content_digest = @splat(3), .timestamp = 10, .input_position = null },
    };
    defer token.deinit();
    try token.observe("source-artifact", "original", null);
    try token.observe("source-artifact", "original", null);
    try std.testing.expectEqual(@as(usize, 1), token.reads.items.len);
    try std.testing.expectError(error.EnrichmentSourceChanged, token.observe("source-artifact", "replacement", null));
    try token.observe("review-override", null, null);
    try std.testing.expectError(error.EnrichmentSourceChanged, token.observe("review-override", "new override", null));
    try token.observe("sibling", "same bytes", .{ .raft = .{ .term = 1, .index = 8 } });
    try std.testing.expectError(error.EnrichmentSourceChanged, token.observe("sibling", "same bytes", .{ .raft = .{ .term = 1, .index = 10 } }));
    try std.testing.expectEqual(@as(usize, 3), token.reads.items.len);
    try std.testing.expectEqualStrings("review-override", token.reads.items[0].key);
    try std.testing.expectEqualStrings("sibling", token.reads.items[1].key);
    const before = try token.command(&.{});
    const logical_digest = before.inputDigest();
    const full_digest = before.digest();
    try token.observePrecondition("own-output", null, null);
    const after = try token.command(&.{});
    try std.testing.expectEqualSlices(u8, &logical_digest, &after.inputDigest());
    try std.testing.expect(!std.mem.eql(u8, &full_digest, &after.digest()));
    try std.testing.expectError(error.EnrichmentSourceChanged, token.observePrecondition("own-output", "changed", null));
}

test "ordered artifact inventory resolver inherits causal sources with stable sorted ordinals" {
    const alloc = std.testing.allocator;
    var token: Token = .{
        .arena = std.heap.ArenaAllocator.init(alloc),
        .namespace = @splat(1),
        .epoch = 1,
        .catalog_digest = @splat(2),
        .producer_name = "resolver",
        .producer_kind = .resolver,
        .producer_generation = 7,
        .artifact_name = "decisions",
        .source = .{ .document_key = "middle", .content_digest = @splat(3), .timestamp = 10, .input_position = null },
    };
    defer token.deinit();
    const input_key = try keys.artifactNamedPrefixAlloc(alloc, "middle", "asset", "extraction");
    defer alloc.free(input_key);
    const neighbor_key = try keys.artifactNamedPrefixAlloc(alloc, "ahead", "asset", "context");
    defer alloc.free(neighbor_key);
    const output_key = try keys.resolutionArtifactKeyAlloc(alloc, "middle", "decisions");
    defer alloc.free(output_key);
    try token.observe(input_key, "source", null);
    try token.observePrecondition(output_key, null, null);
    const sources_value = [_]publication.Source{
        .{ .document_key = "ahead", .content_digest = @splat(4), .timestamp = 9, .input_position = null },
        token.source,
        .{ .document_key = "tail", .content_digest = @splat(5), .timestamp = 11, .input_position = null },
    };
    const upstream: publication.Command = .{
        .namespace = token.namespace,
        .authority_epoch = token.epoch,
        .catalog_digest = token.catalog_digest,
        .producer_kind = .enrichment,
        .producer_name = "extractor",
        .producer_generation = 1,
        .producer_artifact_name = "extraction",
        .sources = &sources_value,
        .artifact_sources = &.{.{ .key = neighbor_key, .content_digest = @splat(6), .input_position = null, .source_index = 0 }},
        .mutations = &.{},
        .publication_digest = @splat(7),
    };
    const proof: @import("artifact_producer_provenance.zig").Proof = .{
        .namespace = upstream.namespace,
        .authority_epoch = upstream.authority_epoch,
        .catalog_digest = upstream.catalog_digest,
        .producer_kind = upstream.producer_kind,
        .producer_name = upstream.producer_name,
        .producer_generation = upstream.producer_generation,
        .producer_artifact_name = upstream.producer_artifact_name,
        .publication_digest = upstream.publication_digest,
        .input_digest = upstream.inputDigest(),
        .sources = upstream.sources,
        .artifact_sources = upstream.artifact_sources,
        .effects = &.{.{ .family = .document_artifact, .key = input_key, .source_index = 1, .value_digest = @splat(8), .value_bytes = 6 }},
    };
    try token.inheritProof(proof);
    try token.inheritProof(proof);
    const command_value = try token.command(&.{.{ .family = .resolution, .key = output_key, .source_index = 0, .value = null }});
    try command_value.validate(alloc);
    try std.testing.expectEqualDeep(&sources_value, command_value.sources);
    try std.testing.expectEqual(@as(u32, 1), command_value.mutations[0].source_index);
    try std.testing.expectEqual(@as(u32, 1), command_value.mutation_preconditions[0].source_index);
    try std.testing.expectEqual(@as(usize, 2), command_value.artifact_sources.len);
    for (command_value.artifact_sources) |guard| {
        try std.testing.expectEqual(@as(u32, if (std.mem.eql(u8, guard.key, neighbor_key)) 0 else 1), guard.source_index);
    }
    var changed = sources_value[0];
    changed.timestamp += 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, token.inheritSources((&changed)[0..1]));
    var foreign = proof;
    foreign.authority_epoch += 1;
    try std.testing.expectError(error.ArtifactCatalogDrift, token.inheritProof(foreign));
    const AllocationFailure = struct {
        fn run(a: std.mem.Allocator, upstream_proof: @import("artifact_producer_provenance.zig").Proof, input: []const u8, output_key_value: []const u8) !void {
            var value: Token = .{
                .arena = std.heap.ArenaAllocator.init(a),
                .namespace = upstream_proof.namespace,
                .epoch = upstream_proof.authority_epoch,
                .catalog_digest = upstream_proof.catalog_digest,
                .producer_name = "resolver",
                .producer_kind = .resolver,
                .producer_generation = 7,
                .artifact_name = "decisions",
                .source = upstream_proof.sources[1],
            };
            defer value.deinit();
            try value.observe(input, "source", null);
            try value.observePrecondition(output_key_value, null, null);
            try value.inheritProof(upstream_proof);
            try value.inheritProof(upstream_proof);
            const result = try value.command(&.{.{ .family = .resolution, .key = output_key_value, .source_index = 0, .value = null }});
            try result.validate(a);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, AllocationFailure.run, .{ proof, input_key, output_key });
}

test "ordered resolver preparation binds output to exact source and configured generation" {
    const alloc = std.testing.allocator;
    const config: catalog.ResolverConfig = .{ .name = "people", .table = "entities", .source_artifact = "extraction", .resolution_artifact = "resolved", .key_template = "{{canonical_name}}", .config_generation = 7 };
    const encoded = try catalog.serializeCatalog(alloc, &.{config});
    defer alloc.free(encoded);
    const input = try keys.artifactNamedPrefixAlloc(alloc, "doc", "asset", "extraction");
    defer alloc.free(input);
    const output = try keys.resolutionArtifactKeyAlloc(alloc, "doc", "resolved");
    defer alloc.free(output);
    var mutation = publication.Mutation{ .family = .resolution, .key = output, .value = "{\"config_generation\":7,\"entities\":[]}", .source_index = 0 };
    var command: publication.Command = .{
        .namespace = @splat(1),
        .authority_epoch = 1,
        .catalog_digest = @splat(2),
        .producer_kind = .resolver,
        .producer_name = "people",
        .producer_generation = 7,
        .producer_artifact_name = "resolved",
        .sources = &.{.{ .document_key = "doc", .content_digest = @splat(3), .timestamp = 1, .input_position = null }},
        .artifact_sources = &.{.{ .key = input, .content_digest = @as(publication.Digest, @splat(4)), .input_position = null, .source_index = 0 }},
        .mutations = (&mutation)[0..1],
        .publication_digest = @splat(0),
    };
    command.publication_digest = command.digest();
    var prepared = try prepareResolution(alloc, command, .{ .resolvers = encoded });
    defer prepared.deinit();
    try std.testing.expectEqualStrings(output, prepared.batch.changed_artifact_keys[0]);
    try std.testing.expectEqual(@as(usize, 3), prepared.target_hints.?.len);
    mutation.value = "{\"config_generation\":6,\"entities\":[]}";
    command.publication_digest = command.digest();
    try std.testing.expectError(error.InvalidBatchRequest, prepareResolution(alloc, command, .{ .resolvers = encoded }));
    mutation.value = null;
    command.producer_generation = 8;
    command.publication_digest = command.digest();
    try std.testing.expectError(error.EnrichmentSourceChanged, prepareResolution(alloc, command, .{ .resolvers = encoded }));
}
