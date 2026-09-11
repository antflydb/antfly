// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Metadata-only reconciliation for a pinned, unchanged external source.
//! No artifact-store or RowSource capability is accepted: this operation must
//! not hydrate remote rows or manufacture managed document snapshots.
const std = @import("std");
const Allocator = std.mem.Allocator;
const manifests = @import("../manifest/types.zig");
const publication = @import("publication_plan.zig");
const lake = @import("lake_rebuild.zig");
const metrics = @import("graph_metric_config.zig");
const metric_segment = @import("../graph_metric_segment/mod.zig");
const metric_kernel = @import("lake_graph_metric.zig");
const artifacts = @import("../artifacts/mod.zig");
const sources = @import("../search_sources.zig");

/// The caller has already established that the complete external source
/// descriptor is unchanged and protected the current HEAD with a read lease.
/// External inventory refs are deliberately omitted; the caller attaches the
/// freshly resolved inventory plan after reconciling its logical names.
pub fn reconcileAlloc(alloc: Allocator, current: manifests.Manifest, plan: publication.TablePublicationPlan) !manifests.Manifest {
    const descriptor = current.base_source orelse return error.InvalidExternalSourceManifestPlan;
    var binding = try publication.externalBindingFromSchemaJsonAlloc(alloc, current.stats.schema_json);
    defer if (binding) |*value| value.deinit(alloc);
    // Real inventory publication persists the external table ID in schema.
    // The namespace fallback is for direct library publications with no schema;
    // it is used only to compare before/after metadata, never to hydrate rows.
    const source_id = if (binding) |value| value.binding.table_id else current.namespace;
    const source: lake.LakeSourceSnapshot = switch (descriptor) {
        .external_parquet => |value| snapshot(.external_parquet, value, source_id),
        .external_iceberg => |value| snapshot(.external_iceberg, value, source_id),
        .external_lance => |value| snapshot(.external_lance, value, source_id),
        else => return error.InvalidExternalSourceManifestPlan,
    };
    var before = try lake.desiredArtifactsFromTableDefinitionAlloc(alloc, source, .{
        .table_name = current.namespace,
        .schema_json = current.stats.schema_json,
        .read_schema_json = current.stats.read_schema_json,
        .indexes_json = current.stats.indexes_json,
    });
    defer before.deinit(alloc);
    var after = try lake.desiredArtifactsFromTableDefinitionAlloc(alloc, source, .{
        .table_name = current.namespace,
        .schema_json = plan.table_definition.schema_json,
        .read_schema_json = plan.table_definition.read_schema_json,
        .indexes_json = plan.table_definition.indexes_json,
    });
    defer after.deinit(alloc);
    var published = std.ArrayListUnmanaged(lake.PublishedArtifact).empty;
    defer published.deinit(alloc);
    for (current.artifacts) |ref| {
        const previous = before.find(ref.name) orelse continue;
        if (previous.kind != ref.kind) continue;
        try published.append(alloc, .{ .name = ref.name, .binding = previous.binding, .artifact = ref });
    }
    var decisions = try lake.planAlloc(alloc, after.artifacts, published.items);
    defer decisions.deinit(alloc);
    var retained = std.ArrayListUnmanaged(manifests.ArtifactRef).empty;
    defer retained.deinit(alloc);
    var alias_names = std.ArrayListUnmanaged([]u8).empty;
    defer {
        for (alias_names.items) |name| alloc.free(name);
        alias_names.deinit(alloc);
    }
    for (current.artifacts) |ref| {
        const keep = switch (ref.kind) {
            .external_base_source, .graph_metric_segment => false,
            .text_segment, .sparse_segment, .vector_segment, .graph_segment, .algebraic_segment => blk: {
                const decision = decisions.find(ref.name) orelse break :blk false;
                if (ref.kind == .vector_segment and current.stats.policy.vector_distance_metric != plan.policy.vector_distance_metric) break :blk false;
                break :blk decision.action == .reuse;
            },
            // Source-native row storage and auxiliaries are independent of
            // individual index configuration when the source is unchanged.
            .row_fragment, .row_fragment_stats => true,
            // These auxiliary formats do not persist a per-index dependency
            // binding in the manifest. Do not infer compatibility after a
            // schema/index-definition change from their logical name alone.
            .doc_values, .stored_fields => std.mem.eql(u8, current.stats.schema_json, plan.table_definition.schema_json) and
                std.mem.eql(u8, current.stats.indexes_json, plan.table_definition.indexes_json),
            // An external generation must not acquire managed source state.
            .document_segment, .document_facts, .mutation_segment => false,
        };
        if (keep) try retained.append(alloc, ref);
    }
    // A logical graph alias does not change its normalized projection. Reuse
    // the same physical graph under the new name using the lake planner's
    // exact source/configuration binding equality.
    for (after.artifacts) |desired| {
        if (desired.kind != .graph_segment or find(retained.items, .graph_segment, desired.name) != null) continue;
        for (published.items) |previous| {
            if (previous.artifact.kind != .graph_segment or !lake.bindingsEqual(desired.binding, previous.binding)) continue;
            var alias = previous.artifact;
            alias.name = desired.name;
            try retained.append(alloc, alias);
            break;
        }
    }
    const configured_metrics = try metrics.parseIndexSpecsAlloc(alloc, plan.table_definition.indexes_json);
    defer metrics.freeIndexSpecs(alloc, configured_metrics);
    for (configured_metrics) |spec| {
        const graph = find(retained.items, .graph_segment, spec.index_name) orelse continue;
        const digest = artifacts.sha256DigestFromChecksum(graph.checksum) catch continue;
        for (spec.configs) |config| {
            const name = try metric_segment.artifactNameAlloc(alloc, spec.index_name, config.name);
            alias_names.append(alloc, name) catch |err| {
                alloc.free(name);
                return err;
            };
            // Prefer the existing logical name, then a physical alias with
            // identical graph and normalized metric configuration.
            var selected: ?manifests.ArtifactRef = null;
            for (current.artifacts) |ref| {
                if (ref.kind != .graph_metric_segment or ref.metadata_version != metric_segment.wire_version or
                    !std.mem.eql(u8, &digest, &ref.graph_metric_source_checksum) or
                    ref.graph_metric_config_fingerprint != metric_kernel.configFingerprint(config)) continue;
                selected = ref;
                if (std.mem.eql(u8, ref.name, name)) break;
            }
            if (selected) |ref| {
                var alias = ref;
                alias.name = name;
                try retained.append(alloc, alias);
            }
        }
    }

    // Clone only retained refs and new metadata, not a complete old manifest
    // followed by a second set of allocations to replace discarded fields.
    var template = current;
    template.artifacts = retained.items;
    template.stats.published_search_sources = .{};
    template.stats.policy = plan.policy;
    template.stats.schema_json = plan.table_definition.schema_json;
    template.stats.read_schema_json = plan.table_definition.read_schema_json;
    template.stats.indexes_json = plan.table_definition.indexes_json;
    var result = try manifests.cloneManifest(alloc, template);
    errdefer result.deinit(alloc);
    result.stats.published_search_sources = try filterSourcesAlloc(alloc, current.stats.published_search_sources, retained.items);
    result.stats.text_segment_count = count(retained.items, .text_segment);
    result.stats.vector_segment_count = count(retained.items, .vector_segment);
    result.stats.sparse_segment_count = count(retained.items, .sparse_segment);
    result.stats.graph_segment_count = count(retained.items, .graph_segment);
    return result;
}

fn snapshot(kind: @import("../../storage/rowsource/types.zig").SourceKind, source: manifests.ExternalBaseSource, source_id: []const u8) lake.LakeSourceSnapshot {
    return .{ .source_kind = kind, .source_id = source_id, .snapshot_id = source.snapshot_id, .schema_fingerprint = source.schema_fingerprint };
}

fn find(refs: []const manifests.ArtifactRef, kind: manifests.ArtifactKind, name: []const u8) ?manifests.ArtifactRef {
    for (refs) |ref| if (ref.kind == kind and std.mem.eql(u8, ref.name, name)) return ref;
    return null;
}

fn count(refs: []const manifests.ArtifactRef, kind: manifests.ArtifactKind) u32 {
    var result: u32 = 0;
    for (refs) |ref| if (ref.kind == kind) {
        result += 1;
    };
    return result;
}

fn hasSource(refs: []const manifests.ArtifactRef, source: sources.SearchSourceDescriptor) bool {
    const kind: manifests.ArtifactKind = switch (source) {
        .text => .text_segment,
        .vector => .vector_segment,
        .sparse => .sparse_segment,
    };
    return find(refs, kind, source.indexName()) != null;
}

fn filterSourcesAlloc(alloc: Allocator, previous: sources.PublishedSearchSources, refs: []const manifests.ArtifactRef) !sources.PublishedSearchSources {
    var items = std.ArrayListUnmanaged(sources.SearchSourceDescriptor).empty;
    defer items.deinit(alloc);
    if (previous.items) |registered| {
        for (registered) |source| if (hasSource(refs, source)) {
            try items.append(alloc, source);
        };
    } else {
        // Normalize borrowed singular descriptors before cloning. Ownership
        // enters the fixed-size registry only after its allocation succeeds.
        if (previous.text) |value| if (hasSource(refs, .{ .text = value })) {
            try items.append(alloc, .{ .text = value });
        };
        if (previous.vector) |value| if (hasSource(refs, .{ .vector = value })) {
            try items.append(alloc, .{ .vector = value });
        };
        if (previous.sparse) |value| if (hasSource(refs, .{ .sparse = value })) {
            try items.append(alloc, .{ .sparse = value });
        };
    }
    return sources.clonePublishedSearchSourcesAlloc(alloc, .{ .items = items.items });
}

/// Shared metadata-only fixture for focused tests and the opt-in benchmark.
pub const testing = struct {
    pub const indexes = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\"},\"vec\":{\"type\":\"embeddings\",\"field\":\"embedding\",\"dimension\":3},\"graph_idx\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"degree\":{\"kind\":\"degree\"},\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":20}}}}";

    pub fn fixtureAlloc(alloc: Allocator, document_count: u64) !manifests.Manifest {
        const refs = [_]manifests.ArtifactRef{
            .{ .kind = .external_base_source, .name = "docs.external-files", .artifact_id = "inventory-docs", .checksum = "a" ** 64, .byte_len = 128 },
            .{ .kind = .text_segment, .name = "body_text", .artifact_id = "sha256:" ++ "b" ** 64, .checksum = "b" ** 64, .byte_len = 128 },
            .{ .kind = .vector_segment, .name = "vec", .artifact_id = "sha256:" ++ "c" ** 64, .checksum = "c" ** 64, .byte_len = 128 },
            .{ .kind = .graph_segment, .name = "graph_idx", .artifact_id = "sha256:" ++ "a" ** 64, .checksum = "a" ** 64, .byte_len = 128, .edge_generation = 3 },
            .{ .kind = .graph_metric_segment, .name = "9:graph_idx6:degree", .artifact_id = "sha256:" ++ "d" ** 64, .checksum = "d" ** 64, .byte_len = 4096, .metadata_version = metric_segment.wire_version, .published_generation = 5, .edge_generation = 3, .computed_at_ms = 42, .graph_metric_control_len = 128, .graph_metric_routing_footer_len = 128, .graph_metric_source_checksum = @splat(0xaa) },
            .{ .kind = .graph_metric_segment, .name = "9:graph_idx4:rank", .artifact_id = "sha256:" ++ "e" ** 64, .checksum = "e" ** 64, .byte_len = 4096, .metadata_version = metric_segment.wire_version, .published_generation = 5, .edge_generation = 3, .computed_at_ms = 42, .graph_metric_control_len = 128, .graph_metric_routing_footer_len = 128, .graph_metric_source_checksum = @splat(0xaa) },
        };
        var result = try manifests.cloneManifest(alloc, .{
            .namespace = "docs",
            .version = 5,
            .built_at_ns = 42,
            .wal_start_lsn = 1,
            .wal_end_lsn = 0,
            .base_source = .{ .external_parquet = .{ .format = .parquet_prefix, .source_uri = "s3://warehouse/docs", .snapshot_id = "parquet-31", .schema_fingerprint = "schema-v3", .file_inventory_artifact = "inventory-docs" } },
            .stats = .{ .document_count = document_count, .text_segment_count = 1, .vector_segment_count = 1, .graph_segment_count = 1, .indexes_json = @constCast(indexes), .published_search_sources = .{ .text = .{ .index_name = "body_text" }, .vector = .{ .index_name = "vec", .document_source = .top_level_embedding, .embedding_name = "embedding", .distance_metric = .cosine } } },
            .artifacts = @constCast(&refs),
        });
        errdefer result.deinit(alloc);
        const specs = try metrics.parseIndexSpecsAlloc(alloc, indexes);
        defer metrics.freeIndexSpecs(alloc, specs);
        for (result.artifacts) |*ref| {
            if (ref.kind != .graph_metric_segment) continue;
            const name = try metric_segment.parseArtifactName(ref.name);
            for (specs[0].configs) |config| if (std.mem.eql(u8, config.name, name.metric_name)) {
                ref.graph_metric_config_fingerprint = metric_kernel.configFingerprint(config);
            };
        }
        return result;
    }
};

test "serverless external metadata preserves populated sidecars and selectively invalidates dependencies" {
    const a = std.testing.allocator;
    var current = try testing.fixtureAlloc(a, 16384);
    defer current.deinit(a);
    var plan: publication.TablePublicationPlan = .{ .targets = .{ .published_search_sources = .{} }, .table_definition = .{ .indexes_json = @constCast(testing.indexes), .read_schema_json = @constCast("{}") } };
    var same = try reconcileAlloc(a, current, plan);
    defer same.deinit(a);
    try std.testing.expectEqual(@as(usize, 5), same.artifacts.len);
    try std.testing.expectEqual(current.stats.document_count, same.stats.document_count);
    for (same.artifacts) |ref| {
        const prior = find(current.artifacts, ref.kind, ref.name).?;
        try std.testing.expectEqualStrings(prior.artifact_id, ref.artifact_id);
        try std.testing.expectEqual(prior.edge_generation, ref.edge_generation);
        try std.testing.expectEqual(prior.computed_at_ms, ref.computed_at_ms);
        try std.testing.expectEqual(prior.graph_metric_config_fingerprint, ref.graph_metric_config_fingerprint);
    }
    try std.testing.expectEqualStrings("embedding", same.stats.published_search_sources.findVector().?.embedding_name.?);

    const changed = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"title\"},\"graph_idx\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"degree\":{\"kind\":\"degree\"},\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":40}}}}";
    plan.table_definition.indexes_json = @constCast(changed);
    var selective = try reconcileAlloc(a, current, plan);
    defer selective.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), selective.artifacts.len);
    try std.testing.expect(find(selective.artifacts, .graph_segment, "graph_idx") != null);
    try std.testing.expect(find(selective.artifacts, .graph_metric_segment, "9:graph_idx6:degree") != null);
    try std.testing.expect(selective.stats.published_search_sources.findText() == null);
    try std.testing.expect(selective.stats.published_search_sources.findVector() == null);
    try std.testing.expectEqual(@as(u32, 0), selective.stats.text_segment_count);
    try std.testing.expectEqual(@as(u32, 0), selective.stats.vector_segment_count);

    plan.table_definition.indexes_json = @constCast("{\"graph_idx\":{\"type\":\"graph\",\"field\":\"new_edges\",\"metrics\":{\"degree\":{\"kind\":\"degree\"}}}}");
    var changed_graph = try reconcileAlloc(a, current, plan);
    defer changed_graph.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), changed_graph.artifacts.len);

    plan.table_definition.indexes_json = @constCast("{\"graph_alias\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"degree_alias\":{\"kind\":\"degree\"}}}}");
    var renamed = try reconcileAlloc(a, current, plan);
    defer renamed.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), renamed.artifacts.len);
    try std.testing.expectEqualStrings(find(current.artifacts, .graph_segment, "graph_idx").?.artifact_id, find(renamed.artifacts, .graph_segment, "graph_alias").?.artifact_id);
    try std.testing.expectEqualStrings(find(current.artifacts, .graph_metric_segment, "9:graph_idx6:degree").?.artifact_id, find(renamed.artifacts, .graph_metric_segment, "11:graph_alias12:degree_alias").?.artifact_id);
}

test "serverless external metadata allocation failures release every owned snapshot" {
    const a = std.testing.allocator;
    var current = try testing.fixtureAlloc(a, 1024);
    defer current.deinit(a);
    const Exercise = struct {
        fn run(alloc: Allocator, source: manifests.Manifest) !void {
            var result = try reconcileAlloc(alloc, source, .{
                .targets = .{ .published_search_sources = .{} },
                .table_definition = .{ .indexes_json = @constCast(testing.indexes), .schema_json = @constCast("{}"), .read_schema_json = @constCast("{}") },
            });
            defer result.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Exercise.run, .{current});
    var singular_sources = current;
    singular_sources.stats.published_search_sources.items = null;
    try std.testing.checkAllAllocationFailures(a, Exercise.run, .{singular_sources});
}
