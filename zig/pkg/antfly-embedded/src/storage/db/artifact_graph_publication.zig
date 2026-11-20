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

//! Derive graph replay from authenticated physical effects. The common
//! publication transaction owns read-set validation, receipts and store writes.
const std = @import("std");
const publication = @import("artifact_publication.zig");
const inventory = @import("artifact_inventory.zig");
const manager = @import("catalog/index_manager.zig");
const graph = @import("online_graph_artifacts.zig");
const keys = @import("../internal_keys.zig");
const codec = @import("enrichment/artifact_codec.zig");
const contenders = @import("graph_edge_contender.zig");
const edges = @import("graph_edge_types.zig");

pub fn prepare(alloc: std.mem.Allocator, command: publication.Command, catalogs: inventory.Catalogs) !publication.PreparedBaseVectors {
    if (command.producer_kind != .graph) return error.InvalidBatchRequest;
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const configs = try manager.deserializeCatalog(owned, catalogs.indexes);
    const producer = for (configs) |config| {
        if (std.mem.eql(u8, config.name, command.producer_name)) break config;
    } else return error.EnrichmentSourceChanged;
    if (producer.kind != .graph or producer.coverage_generation != command.producer_generation) return error.EnrichmentSourceChanged;
    if (!try manager.graphConfigConsumesArtifact(owned, producer.config_json, command.producer_artifact_name)) return error.InvalidBatchRequest;
    const binding: inventory.Binding = .{ .epoch = command.authority_epoch, .digest = catalogs.digest(), .semantic_digest = try catalogs.semanticDigest(owned), .effect_protocol = 15 };
    var plan = try graph.Plan.init(owned, catalogs, binding, catalogs, binding);
    defer plan.deinit();
    var mutations: std.StringHashMapUnmanaged(publication.Mutation) = .empty;
    var guards: std.StringHashMapUnmanaged(void) = .empty;
    for (command.mutation_preconditions) |source| try guards.put(owned, source.key, {});
    var writes: std.ArrayList(edges.GraphEdgeWrite) = .empty;
    var deletes: std.ArrayList(edges.GraphEdgeDelete) = .empty;
    var changed: std.ArrayList([]const u8) = .empty;
    for (command.mutations) |mutation| {
        if (mutation.family != .graph or !matchesIndex(mutation.key, producer.name)) return error.InvalidBatchRequest;
        // Exact point guards protect overwritten and newly inserted keys.
        // The count guard below additionally fences prefix phantom contenders.
        if (!guards.contains(mutation.key)) return error.InvalidBatchRequest;
        const inserted = try mutations.getOrPut(owned, mutation.key);
        if (inserted.found_existing) return error.InvalidBatchRequest;
        inserted.value_ptr.* = mutation;
        // The catalog was decoded above; failures here describe authored
        // output, not persisted catalog state. Commit an invalid-output
        // receipt instead of wedging deterministic Raft replay on bad bytes.
        const validated = plan.rebind(owned, mutation.key, mutation.value) catch |err| return authoredOutputError(err);
        if (!std.mem.eql(u8, validated.key, mutation.key)) return error.InvalidBatchRequest;
        if (mutation.value) |value| if (!std.mem.eql(u8, validated.value.?, value)) return error.InvalidBatchRequest;
        try changed.append(owned, validated.key);
        if (keys.parseGraphEdgeArtifactKeyAlloc(owned, mutation.key) catch |err| return authoredOutputError(err)) |identity| {
            const source = identity.source_node orelse identity.doc_key;
            if (mutation.value) |value| {
                const edge = codec.decodeGraphEdgeAlloc(owned, value) catch |err| return authoredOutputError(err);
                try writes.append(owned, .{ .index_name = identity.index_name, .source = source, .target = identity.target_doc_key, .edge_type = identity.edge_type, .weight = edge.weight, .created_at = edge.created_at, .updated_at = edge.updated_at, .metadata_json = edge.metadata_json, .edge_id = identity.edge_id, .owner_document = if (identity.edge_id.len != 0) identity.doc_key else "", .owner = if (identity.edge_id.len == 0) identity.doc_key else "" });
            } else try deletes.append(owned, .{ .index_name = identity.index_name, .source = source, .target = identity.target_doc_key, .edge_type = identity.edge_type, .edge_id = identity.edge_id, .owner_document = if (identity.edge_id.len != 0) identity.doc_key else "", .owner = if (identity.edge_id.len == 0) identity.doc_key else "" });
        }
    }
    var coverage: std.ArrayList(publication.Coverage) = .empty;
    for (command.sources, 0..) |source, source_index| {
        const affected = for (command.mutations) |mutation| {
            if (mutation.source_index == source_index) break true;
        } else false;
        if (!affected) continue;
        const count_key = try keys.graphEdgeContenderCountKeyAlloc(owned, source.document_key, producer.name);
        if (!guards.contains(count_key)) return error.InvalidBatchRequest;
        const count_effect = mutations.get(count_key) orelse return error.InvalidBatchRequest;
        const raw = count_effect.value orelse return error.InvalidBatchRequest;
        const count = (contenders.decodeVisibleCount(raw, producer.coverage_generation) catch |err| return authoredOutputError(err)) orelse return error.InvalidBatchRequest;
        try coverage.append(owned, .{ .index_name = producer.name, .generation = producer.coverage_generation, .document_key = try owned.dupe(u8, source.document_key), .artifact_names = &.{}, .outcome = if (count == 0) .skipped else .produced });
    }
    return .{ .arena = arena, .graph_publication_digest = command.publication_digest, .batch = .{ .changed_artifact_keys = changed.items, .graph_writes = writes.items, .graph_deletes = deletes.items }, .coverage = coverage.items };
}

fn authoredOutputError(err: anyerror) anyerror {
    return switch (err) {
        error.OutOfMemory, error.ResourceBudgetExceeded => err,
        // Codec size/count limits are permanent properties of these bytes,
        // unlike a temporary allocator/admission shortage. Retrying them
        // cannot make a committed publication valid.
        error.ResourceLimitExceeded => error.InvalidBatchRequest,
        else => error.InvalidBatchRequest,
    };
}

fn matchesIndex(key: []const u8, name: []const u8) bool {
    return keys.matchesGraphEdgeIndexName(key, name) or keys.matchesGraphAssetStateIndexName(key, name) or
        keys.matchesGraphEdgeContenderIndexName(key, name) or keys.matchesGraphGlobalEdgeContenderIndexName(key, name);
}

test "ordered artifact inventory graph publication validates guards generations and logical edge ownership" {
    const alloc = std.testing.allocator;
    try std.testing.expectEqual(error.InvalidBatchRequest, authoredOutputError(error.ResourceLimitExceeded));
    try std.testing.expectEqual(error.OutOfMemory, authoredOutputError(error.OutOfMemory));
    try std.testing.expectEqual(error.ResourceBudgetExceeded, authoredOutputError(error.ResourceBudgetExceeded));
    const config = "{\"source\":{\"artifact\":\"relations\"}}";
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(alloc);
    try bytes.appendSlice(alloc, "AIDX\x02\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00g\x03");
    var size: [4]u8 = undefined;
    std.mem.writeInt(u32, &size, config.len, .little);
    try bytes.appendSlice(alloc, &size);
    try bytes.appendSlice(alloc, config);
    try bytes.appendSlice(alloc, "\x07\x00\x00\x00\x00\x00\x00\x00");
    const catalogs: inventory.Catalogs = .{ .indexes = bytes.items };
    const edge_key = try keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "owner", "g", "links", "target", "entity");
    defer alloc.free(edge_key);
    const edge = try codec.encodeGraphEdgeAlloc(alloc, null, 7, 0.5, 1, 2, "{}");
    defer alloc.free(edge);
    const count_key = try keys.graphEdgeContenderCountKeyAlloc(alloc, "owner", "g");
    defer alloc.free(count_key);
    const count = try contenders.encodeVisibleCount(7, 1);
    var command: publication.Command = .{
        .producer_kind = .graph,
        .namespace = @splat(1),
        .authority_epoch = 1,
        .catalog_digest = catalogs.digest(),
        .producer_name = "g",
        .producer_generation = 7,
        .producer_artifact_name = "relations",
        .sources = &.{.{ .document_key = "owner", .content_digest = @splat(3), .timestamp = 1, .input_position = null }},
        .mutation_preconditions = &.{ .{ .key = edge_key, .content_digest = null, .input_position = null, .source_index = 0 }, .{ .key = count_key, .content_digest = null, .input_position = null, .source_index = 0 } },
        .mutations = &.{ .{ .family = .graph, .key = edge_key, .value = edge, .source_index = 0 }, .{ .family = .graph, .key = count_key, .value = &count, .source_index = 0 } },
        .publication_digest = @splat(0),
    };
    {
        command.publication_digest = command.digest();
        var result = try prepare(alloc, command, catalogs);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.batch.graph_writes.len);
        try std.testing.expectEqualStrings("owner", result.batch.graph_writes[0].owner);
        try std.testing.expectEqualStrings("entity", result.batch.graph_writes[0].source);
        try std.testing.expect(result.coverage[0].outcome.? == .produced);
        const replacements = try result.dagReplacements("g", "links");
        try std.testing.expectEqual(@as(usize, 1), replacements.len);
        try std.testing.expectEqualStrings("entity", replacements[0].source);
        try std.testing.expectEqualStrings("target", replacements[0].targets[0]);
        var different = command;
        different.authority_epoch += 1;
        different.publication_digest = different.digest();
        try std.testing.expectError(error.InvalidBatchRequest, result.dagPrepare(different, @splat(2), "links", 1, 0, @splat(3)));
        const request = (try result.dagPrepare(command, @splat(2), "links", 1, 0, @splat(3))).?;
        try std.testing.expectEqualDeep(command.publication_digest, request.producer_versions);
        try std.testing.expectEqualDeep(replacements, request.replacements);
        const retry = (try result.dagPrepare(command, @splat(2), "links", 2, 1, @splat(3))).?;
        try std.testing.expectEqualDeep(request.attempt, retry.attempt);
        const next = (try result.dagPrepare(command, @splat(2), "links", 2, 1, @splat(4))).?;
        try std.testing.expect(!std.mem.eql(u8, &request.attempt.id, &next.attempt.id));
        try std.testing.expect((try result.dagPrepare(command, @splat(2), "unknown", 1, 0, @splat(3))) == null);
        const memory = @import("../mem_backend.zig");
        const docstore = @import("../docstore.zig");
        const dag = @import("../../graph/dag_authority.zig");
        var backend = memory.Backend.init(alloc, .{});
        defer backend.close();
        var store = try docstore.DocStore.openRuntime(alloc, try backend.runtimeStore(alloc, .{}));
        defer store.close();
        {
            var writer = try store.beginWriteTxn();
            errdefer writer.abort();
            const binding: publication.LocalDagBinding = .{ .table_identity = @splat(2), .edge_types = &.{"links"} };
            try std.testing.expectError(error.GraphConstraintNotReady, binding.stage(alloc, &writer, command));
            try dag.Storage.create(&writer, request.domain, 1);
            try std.testing.expectError(error.GraphConstraintNotReady, binding.stage(alloc, &writer, command));
            const config_json =
                \\{"edge_types":[{"name":"unconstrained","topology":"graph"},{"name":"links","topology":"dag"}]}
            ;
            try std.testing.expectError(error.InvalidDagMutation, publication.LocalDagBinding.stageFromConfig(alloc, &writer, command, @splat(2), config_json, &.{}));
            try std.testing.expectEqual(.validating, (try dag.Storage.load(&writer, request.domain)).phase);
            const activation = @import("dag_activation_contract.zig");
            const manifest = try activation.ScanManifest.init(&.{.{
                .id = request.domain,
                .roots = &.{.{
                    .namespace = command.namespace,
                    .index_name = "g",
                    .edge_type = "links",
                    .generation = command.producer_generation,
                }},
            }}, command.namespace, @splat(3));
            const fence = manifest.fence;
            var declared: std.ArrayList(u8) = .empty;
            defer declared.deinit(alloc);
            try declared.appendSlice(alloc, bytes.items[0 .. bytes.items.len - config.len - 12]);
            var config_size: [4]u8 = undefined;
            std.mem.writeInt(u32, &config_size, config_json.len, .little);
            try declared.appendSlice(alloc, &config_size);
            try declared.appendSlice(alloc, config_json);
            try declared.appendSlice(alloc, bytes.items[bytes.items.len - 8 ..]);
            const declaration: inventory.Catalogs = .{ .indexes = declared.items };
            const candidate_catalog: inventory.Command = .{
                .namespace = command.namespace,
                .catalogs = declaration,
                .binding = .{ .epoch = 1, .digest = declaration.digest(), .semantic_digest = try declaration.semanticDigest(alloc), .effect_protocol = 16 },
            };
            var scratch = std.heap.ArenaAllocator.init(alloc);
            defer scratch.deinit();
            var ordered = try inventory.planDagActivationPublication(scratch.allocator(), manifest, candidate_catalog, &.{.{
                .index_name = "g",
                .table_identity = @splat(2),
                .edge_types = &.{"links"},
            }}, 1);
            const activations = ordered.dag_activations.?[0].proofs;
            try @import("../source_authority.zig").bind(&writer, .native, command.namespace);
            var missing = ordered;
            missing.dag_activations = null;
            try std.testing.expectError(error.InvalidArtifactCatalogCommand, inventory.stageOrdered(alloc, &writer, missing, 1));
            try std.testing.expect((try inventory.load(alloc, &writer)) == null);
            try std.testing.expectEqual(.validating, (try dag.Storage.load(&writer, request.domain)).phase);
            try std.testing.expectError(error.InvalidArtifactCatalogCommand, inventory.stageOrdered(alloc, &writer, ordered, 1));
            try fence.acquire(&writer);
            var scan = try manifest.beginScanCommand(request.domain, 1);
            try scan.stageScan(alloc, &writer);
            try std.testing.expectError(error.InvalidArtifactCatalogCommand, inventory.stageOrdered(alloc, &writer, ordered, 1));
            const end = try manifest.terminalCursor(request.domain);
            scan.scan.?.root_count = null;
            scan.scan.?.page = .{ .next = &end, .replacements = &.{} };
            try scan.stageScan(alloc, &writer);
            try inventory.stageOrdered(alloc, &writer, ordered, 1);
            try inventory.stageOrdered(alloc, &writer, ordered, 1);
            ordered.dag_activations = &.{.{ .index_name = "g", .table_identity = @splat(9), .proofs = activations }};
            try std.testing.expectError(error.ArtifactCatalogEpochChanged, inventory.stageOrdered(alloc, &writer, ordered, 1));
            missing.previous = ordered.binding;
            missing.binding.epoch += 1;
            try std.testing.expectError(error.GraphConstraintBusy, inventory.stageOrdered(alloc, &writer, missing, 2));
            try fence.release(&writer);
            try inventory.stageOrdered(alloc, &writer, missing, 2);
            try binding.stage(alloc, &writer, command);
            try writer.commit();
        }
        {
            var writer = try store.beginWriteTxn();
            errdefer writer.abort();
            try result.stageLocalDag(alloc, &writer, command);
            try writer.put(edge_key, edge);
            try writer.commit();
        }
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(u64, 1), (try dag.Storage.load(&read, request.domain)).revision);
            try std.testing.expectEqualSlices(u8, edge, try read.get(edge_key));
        }
        const reverse = try keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "owner", "g", "links", "entity", "target");
        defer alloc.free(reverse);
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const effects = @import("dag_graph_effects.zig");
            const validation = @import("../../graph/dag_validation.zig");
            const Clock = struct {
                fn now(_: *anyopaque) u64 {
                    return 0;
                }
            };
            var budget: validation.Budget = .{
                .limits = .{ .requests = 1 },
                .clock = .{ .context = &read, .now = Clock.now },
            };
            const normalized = try effects.collectBoundArtifactChanges(arena.allocator(), &read, command.namespace, &.{
                .{ .key = edge_key, .before = command.producer_generation, .after = null },
                .{ .key = reverse, .before = null, .after = command.producer_generation },
            }, &budget);
            try std.testing.expectEqual(@as(usize, 1), budget.stats.requests);
            try std.testing.expectEqual(@as(usize, 2), normalized.len);
            for (normalized) |item| try std.testing.expectEqual(request.domain, item.domain);
        }
        {
            var writer = try store.beginWriteTxn();
            defer writer.abort();
            try writer.put(reverse, edge);
            try std.testing.expectError(error.GraphCycle, writer.commit());
        }
        {
            var writer = try store.beginWriteTxn();
            errdefer writer.abort();
            try writer.delete(edge_key);
            try writer.put(reverse, edge);
            try writer.commit();
        }
        {
            var read = try store.beginReadTxn();
            defer read.abort();
            try std.testing.expectEqual(@as(u64, 2), (try dag.Storage.load(&read, request.domain)).revision);
            try std.testing.expectError(error.NotFound, read.get(edge_key));
            try std.testing.expectEqualSlices(u8, edge, try read.get(reverse));
        }
    }
    const guards = command.mutation_preconditions;
    command.mutation_preconditions = guards[0..1];
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
    command.mutation_preconditions = guards;
    command.producer_generation = 8;
    try std.testing.expectError(error.EnrichmentSourceChanged, prepare(alloc, command, catalogs));
    command.producer_generation = 7;
    command.producer_artifact_name = "foreign";
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
    command.producer_artifact_name = "relations";
    var malformed_effects = [_]publication.Mutation{ command.mutations[0], command.mutations[1] };
    malformed_effects[0].value = "invalid edge bytes";
    command.mutations = &malformed_effects;
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
    malformed_effects[0].value = edge;
    const stale_count = try contenders.encodeVisibleCount(8, 1);
    malformed_effects[1].value = &stale_count;
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
}

test "ordered artifact inventory graph publication preserves explicit identities through guards and replay" {
    const alloc = std.testing.allocator;
    try std.testing.expectEqual(error.InvalidBatchRequest, authoredOutputError(error.ResourceLimitExceeded));
    try std.testing.expectEqual(error.OutOfMemory, authoredOutputError(error.OutOfMemory));
    try std.testing.expectEqual(error.ResourceBudgetExceeded, authoredOutputError(error.ResourceBudgetExceeded));
    const config = "{\"source\":{\"artifact\":\"relations\"}}";
    var bytes: std.ArrayList(u8) = .empty;
    defer bytes.deinit(alloc);
    try bytes.appendSlice(alloc, "AIDX\x02\x00\x00\x00\x01\x00\x00\x00\x01\x00\x00\x00g\x03");
    var size: [4]u8 = undefined;
    std.mem.writeInt(u32, &size, config.len, .little);
    try bytes.appendSlice(alloc, &size);
    try bytes.appendSlice(alloc, config);
    try bytes.appendSlice(alloc, "\x07\x00\x00\x00\x00\x00\x00\x00");
    const catalogs: inventory.Catalogs = .{ .indexes = bytes.items };
    const edge_key = try keys.graphRelationshipArtifactKeyAlloc(alloc, "owner", "g", "links", "target", "entity", "fact-id");
    defer alloc.free(edge_key);
    const edge = try codec.encodeGraphEdgeAlloc(alloc, null, 7, 0.5, 1, 2, "{}");
    defer alloc.free(edge);
    const count_key = try keys.graphEdgeContenderCountKeyAlloc(alloc, "owner", "g");
    defer alloc.free(count_key);
    const count = try contenders.encodeVisibleCount(7, 1);
    var command: publication.Command = .{
        .producer_kind = .graph,
        .namespace = @splat(1),
        .authority_epoch = 1,
        .catalog_digest = catalogs.digest(),
        .producer_name = "g",
        .producer_generation = 7,
        .producer_artifact_name = "relations",
        .sources = &.{.{ .document_key = "owner", .content_digest = @splat(3), .timestamp = 1, .input_position = null }},
        .mutation_preconditions = &.{ .{ .key = edge_key, .content_digest = null, .input_position = null, .source_index = 0 }, .{ .key = count_key, .content_digest = null, .input_position = null, .source_index = 0 } },
        .mutations = &.{ .{ .family = .graph, .key = edge_key, .value = edge, .source_index = 0 }, .{ .family = .graph, .key = count_key, .value = &count, .source_index = 0 } },
        .publication_digest = @splat(0),
    };
    {
        var result = try prepare(alloc, command, catalogs);
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.batch.graph_writes.len);
        try std.testing.expectEqualStrings("", result.batch.graph_writes[0].owner);
        try std.testing.expectEqualStrings("owner", result.batch.graph_writes[0].owner_document);
        try std.testing.expectEqualStrings("fact-id", result.batch.graph_writes[0].edge_id);
        try std.testing.expectEqualStrings("entity", result.batch.graph_writes[0].source);
        try std.testing.expect(result.coverage[0].outcome.? == .produced);
    }
    const guards = command.mutation_preconditions;
    command.mutation_preconditions = guards[0..1];
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
    command.mutation_preconditions = guards;
    command.producer_generation = 8;
    try std.testing.expectError(error.EnrichmentSourceChanged, prepare(alloc, command, catalogs));
    command.producer_generation = 7;
    command.producer_artifact_name = "foreign";
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
    command.producer_artifact_name = "relations";
    var malformed_effects = [_]publication.Mutation{ command.mutations[0], command.mutations[1] };
    malformed_effects[0].value = "invalid edge bytes";
    command.mutations = &malformed_effects;
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
    malformed_effects[0].value = edge;
    const stale_count = try contenders.encodeVisibleCount(8, 1);
    malformed_effects[1].value = &stale_count;
    try std.testing.expectError(error.InvalidBatchRequest, prepare(alloc, command, catalogs));
}
