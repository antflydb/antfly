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
const external_binding = @import("../external_source/catalog_binding.zig");

pub const NamedAction = struct {
    kind: manifests.ArtifactKind,
    name: []const u8,
    action: publication.ArtifactAction,
};

/// Owns planning storage and aliases, but borrows current artifact identities.
/// The source manifest must outlive this plan. No manifest/payload clone or
/// remote discovery is required to inspect readiness.
pub const ReconciliationPlan = struct {
    retained_refs: []manifests.ArtifactRef,
    desired_actions: []NamedAction,
    removed: []NamedAction,
    desired: lake.DesiredArtifactSet,
    alias_names: [][]u8,

    pub fn deinit(self: *ReconciliationPlan, alloc: Allocator) void {
        alloc.free(self.retained_refs);
        alloc.free(self.desired_actions);
        alloc.free(self.removed);
        self.desired.deinit(alloc);
        for (self.alias_names) |name| alloc.free(name);
        alloc.free(self.alias_names);
        self.* = undefined;
    }

    pub fn action(self: ReconciliationPlan, kind: manifests.ArtifactKind, name: []const u8) publication.ArtifactAction {
        for (self.desired_actions) |item| if (item.kind == kind and std.mem.eql(u8, item.name, name)) return item.action;
        for (self.removed) |item| if (item.kind == kind and std.mem.eql(u8, item.name, name)) return .drop;
        return .reuse;
    }

    pub fn familyAction(self: ReconciliationPlan, kind: manifests.ArtifactKind) publication.ArtifactAction {
        for (self.desired_actions) |item| if (item.kind == kind and item.action == .rebuild) return .rebuild;
        for (self.desired_actions) |item| if (item.kind == kind) return .reuse;
        for (self.removed) |item| if (item.kind == kind) return .drop;
        return .reuse;
    }

    pub fn pendingWork(self: ReconciliationPlan) bool {
        if (self.removed.len != 0) return true;
        for (self.desired_actions) |item| if (item.action == .rebuild) return true;
        return false;
    }

    pub fn hasOutstandingWork(self: ReconciliationPlan) bool {
        return self.pendingWork();
    }
};

/// Compare explicit desired work with the current immutable snapshot without
/// discovering a remote source. Changed catalog source bindings invalidate old
/// sidecars. Publication callers additionally pin and verify resolved source
/// identity; external inventory refs remain resolver-owned and are omitted.
pub fn planAlloc(alloc: Allocator, maybe_current: ?manifests.Manifest, plan: publication.TablePublicationPlan) !ReconciliationPlan {
    const current = maybe_current orelse return bootstrapPlanAlloc(alloc, plan);
    const descriptor = current.base_source orelse return bootstrapPlanAlloc(alloc, plan);
    switch (descriptor) {
        .external_parquet, .external_iceberg, .external_lance => {},
        else => return bootstrapPlanAlloc(alloc, plan),
    }
    var binding = try publication.externalBindingFromSchemaJsonAlloc(alloc, current.stats.schema_json);
    defer if (binding) |*value| value.deinit(alloc);
    var desired_binding = try publication.externalBindingFromSchemaJsonAlloc(alloc, plan.table_definition.schema_json);
    defer if (desired_binding) |*value| value.deinit(alloc);
    const compatible_source = bindingsIdentifySameSource(
        if (binding) |value| value.binding else null,
        if (desired_binding) |value| value.binding else null,
    );
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
    errdefer after.deinit(alloc);
    var published = std.ArrayListUnmanaged(lake.PublishedArtifact).empty;
    defer published.deinit(alloc);
    for (current.artifacts) |ref| {
        if (!compatible_source) break;
        const previous = before.find(ref.name) orelse continue;
        if (previous.kind != ref.kind) continue;
        try published.append(alloc, .{ .name = ref.name, .binding = previous.binding, .artifact = ref });
    }
    var decisions = try lake.planAlloc(alloc, after.artifacts, published.items);
    defer decisions.deinit(alloc);
    var retained = std.ArrayListUnmanaged(manifests.ArtifactRef).empty;
    defer retained.deinit(alloc);
    var alias_names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (alias_names.items) |name| alloc.free(name);
        alias_names.deinit(alloc);
    }
    var actions = std.ArrayListUnmanaged(NamedAction).empty;
    defer actions.deinit(alloc);
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
        if (keep and compatible_source) try retained.append(alloc, ref);
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
    for (after.artifacts) |desired| try actions.append(alloc, .{
        .kind = desired.kind,
        .name = desired.name,
        .action = if (find(retained.items, desired.kind, desired.name) != null) .reuse else .rebuild,
    });
    const configured_metrics = try metrics.parseIndexSpecsAlloc(alloc, plan.table_definition.indexes_json);
    defer metrics.freeIndexSpecs(alloc, configured_metrics);
    const reuse_rejections = try rejectionPlanUnchanged(alloc, configured_metrics, retained.items, current.artifacts);
    for (configured_metrics) |spec| {
        const graph = find(retained.items, .graph_segment, spec.index_name);
        const digest = if (graph) |ref| digest: {
            artifacts.validateSha256ArtifactIdentity(ref.artifact_id, ref.checksum) catch break :digest null;
            break :digest artifacts.sha256DigestFromChecksum(ref.checksum) catch null;
        } else null;
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
                const source_digest = digest orelse break;
                if (ref.kind != .graph_metric_segment or ref.metadata_version != metric_segment.wire_version or
                    !std.mem.eql(u8, &source_digest, &ref.graph_metric_source_checksum) or
                    ref.graph_metric_config_fingerprint != metric_kernel.configFingerprint(config)) continue;
                if (ref.graph_metric_materialization_state == .rejected and !reuse_rejections) continue;
                selected = ref;
                if (std.mem.eql(u8, ref.name, name)) break;
            }
            if (selected) |ref| {
                var alias = ref;
                alias.name = name;
                try retained.append(alloc, alias);
            }
            const current_materializer = if (selected) |ref| ref.materializer_fingerprint == metric_kernel.materializerFingerprint(.{}) and
                ref.graph_metric_control_len != 0 and ref.graph_metric_routing_footer_len != 0 else false;
            try actions.append(alloc, .{ .kind = .graph_metric_segment, .name = name, .action = if (current_materializer) .reuse else .rebuild });
        }
    }

    var removed = std.ArrayListUnmanaged(NamedAction).empty;
    defer removed.deinit(alloc);
    for (current.artifacts) |ref| {
        if (!isSidecar(ref.kind) or find(retained.items, ref.kind, ref.name) != null) continue;
        const still_desired = for (actions.items) |item| {
            if (item.kind == ref.kind and std.mem.eql(u8, item.name, ref.name)) break true;
        } else false;
        if (!still_desired) try removed.append(alloc, .{ .kind = ref.kind, .name = ref.name, .action = .drop });
    }
    const owned_retained = try retained.toOwnedSlice(alloc);
    errdefer alloc.free(owned_retained);
    const owned_actions = try actions.toOwnedSlice(alloc);
    errdefer alloc.free(owned_actions);
    const owned_removed = try removed.toOwnedSlice(alloc);
    errdefer alloc.free(owned_removed);
    const owned_aliases = try alias_names.toOwnedSlice(alloc);
    return .{ .retained_refs = owned_retained, .desired_actions = owned_actions, .removed = owned_removed, .desired = after, .alias_names = owned_aliases };
}

fn bootstrapPlanAlloc(alloc: Allocator, plan: publication.TablePublicationPlan) !ReconciliationPlan {
    var owned_binding = (try publication.externalBindingFromSchemaJsonAlloc(alloc, plan.table_definition.schema_json)) orelse return error.InvalidExternalSourceManifestPlan;
    defer owned_binding.deinit(alloc);
    const binding = owned_binding.binding;
    // This descriptor only derives declared build dependencies; it never
    // escapes as a publication or authorizes a remote read. Discovery is the
    // publication resolver's responsibility, not catalog status's.
    var desired = try lake.desiredArtifactsFromTableDefinitionAlloc(alloc, .{
        .source_kind = binding.rowSourceKind(),
        .source_id = binding.table_id,
        .snapshot_id = binding.snapshot_mode.pinnedSnapshotId() orelse "unresolved-status-snapshot",
        .schema_fingerprint = binding.schema_fingerprint,
    }, .{
        .table_name = binding.table_id,
        .schema_json = plan.table_definition.schema_json,
        .read_schema_json = plan.table_definition.read_schema_json,
        .indexes_json = plan.table_definition.indexes_json,
    });
    errdefer desired.deinit(alloc);
    var actions = std.ArrayListUnmanaged(NamedAction).empty;
    defer actions.deinit(alloc);
    for (desired.artifacts) |item| try actions.append(alloc, .{ .kind = item.kind, .name = item.name, .action = .rebuild });
    var names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (names.items) |name| alloc.free(name);
        names.deinit(alloc);
    }
    const specs = try metrics.parseIndexSpecsAlloc(alloc, plan.table_definition.indexes_json);
    defer metrics.freeIndexSpecs(alloc, specs);
    for (specs) |spec| for (spec.configs) |config| {
        try names.ensureUnusedCapacity(alloc, 1);
        const name = try metric_segment.artifactNameAlloc(alloc, spec.index_name, config.name);
        names.appendAssumeCapacity(name);
        try actions.append(alloc, .{ .kind = .graph_metric_segment, .name = name, .action = .rebuild });
    };
    const owned_actions = try actions.toOwnedSlice(alloc);
    errdefer alloc.free(owned_actions);
    const owned_names = try names.toOwnedSlice(alloc);
    return .{ .retained_refs = &.{}, .removed = &.{}, .desired_actions = owned_actions, .desired = desired, .alias_names = owned_names };
}

fn isSidecar(kind: manifests.ArtifactKind) bool {
    return switch (kind) {
        .text_segment, .vector_segment, .sparse_segment, .graph_segment, .algebraic_segment, .graph_metric_segment, .doc_values, .stored_fields => true,
        else => false,
    };
}

fn bindingsIdentifySameSource(before: ?external_binding.Binding, after: ?external_binding.Binding) bool {
    if (before == null or after == null) return before == null and after == null;
    const a = before.?;
    const b = after.?;
    if (a.format != b.format or a.write_policy != b.write_policy or
        !std.mem.eql(u8, a.table_id, b.table_id) or !std.mem.eql(u8, a.source_uri, b.source_uri) or
        !std.mem.eql(u8, a.schema_fingerprint, b.schema_fingerprint) or
        std.meta.activeTag(a.snapshot_mode) != std.meta.activeTag(b.snapshot_mode)) return false;
    return switch (a.snapshot_mode) {
        .current => true,
        .snapshot_id => |value| std.mem.eql(u8, value, b.snapshot_mode.snapshot_id),
        .object_version_digest => |value| std.mem.eql(u8, value, b.snapshot_mode.object_version_digest),
    };
}

pub fn reconcileAlloc(alloc: Allocator, current: manifests.Manifest, plan: publication.TablePublicationPlan) !manifests.Manifest {
    var reconciliation = try planAlloc(alloc, current, plan);
    defer reconciliation.deinit(alloc);

    // Clone only retained refs and new metadata, not a complete old manifest
    // followed by a second set of allocations to replace discarded fields.
    var template = current;
    template.artifacts = reconciliation.retained_refs;
    template.stats.published_search_sources = .{};
    template.stats.policy = plan.policy;
    template.stats.schema_json = plan.table_definition.schema_json;
    template.stats.read_schema_json = plan.table_definition.read_schema_json;
    template.stats.indexes_json = plan.table_definition.indexes_json;
    var result = try manifests.cloneManifest(alloc, template);
    errdefer result.deinit(alloc);
    result.stats.published_search_sources = try filterSourcesAlloc(alloc, current.stats.published_search_sources, reconciliation.retained_refs);
    result.stats.text_segment_count = count(reconciliation.retained_refs, .text_segment);
    result.stats.vector_segment_count = count(reconciliation.retained_refs, .vector_segment);
    result.stats.sparse_segment_count = count(reconciliation.retained_refs, .sparse_segment);
    result.stats.graph_segment_count = count(reconciliation.retained_refs, .graph_segment);
    return result;
}

/// A rejected computation depends on the entire admission plan, not just its
/// own source/configuration. If that witness changes, omit old rejections so
/// the next rebuild can reconsider the remaining work under the new plan.
fn rejectionPlanUnchanged(alloc: Allocator, specs: []const metrics.IndexSpec, retained: []const manifests.ArtifactRef, previous: []const manifests.ArtifactRef) !bool {
    const has_rejections = for (previous) |ref| {
        if (ref.kind == .graph_metric_segment and ref.graph_metric_materialization_state == .rejected) break true;
    } else false;
    if (!has_rejections) return false;
    var requests = std.ArrayListUnmanaged(metric_kernel.PublicationRequest).empty;
    defer requests.deinit(alloc);
    for (specs) |spec| {
        if (spec.configs.len == 0) continue;
        const graph = find(retained, .graph_segment, spec.index_name) orelse return false;
        for (spec.configs) |config| try requests.append(alloc, .{
            .graph_index_name = spec.index_name,
            .source_graph = graph,
            .config = config,
            .provenance = .{},
        });
    }
    return metric_kernel.admissionPlanUnchanged(alloc, requests.items, previous, .{});
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
            ref.materializer_fingerprint = metric_kernel.materializerFingerprint(.{});
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
    for (current.artifacts) |*ref| {
        if (ref.kind != .graph_metric_segment) continue;
        ref.materializer_fingerprint = metric_kernel.materializerFingerprint(.{});
        if (std.mem.eql(u8, ref.name, "9:graph_idx4:rank")) {
            ref.graph_metric_materialization_state = .rejected;
            ref.graph_metric_rejection_reason = .build_budget_exceeded;
        }
    }
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

test "serverless external metadata rejections retain only the complete unchanged admission plan" {
    const a = std.testing.allocator;
    var current = try testing.fixtureAlloc(a, 16384);
    defer current.deinit(a);
    const degree_name = "9:graph_idx6:degree";
    const rank_name = "9:graph_idx4:rank";
    for (current.artifacts) |*ref| {
        if (ref.kind != .graph_metric_segment) continue;
        ref.materializer_fingerprint = metric_kernel.materializerFingerprint(.{});
        if (std.mem.eql(u8, ref.name, rank_name)) {
            ref.graph_metric_materialization_state = .rejected;
            ref.graph_metric_rejection_reason = .build_budget_exceeded;
        }
    }
    var plan: publication.TablePublicationPlan = .{
        .targets = .{ .published_search_sources = .{} },
        .table_definition = .{ .indexes_json = @constCast(testing.indexes), .read_schema_json = @constCast("{}") },
    };
    var stable = try reconcileAlloc(a, current, plan);
    defer stable.deinit(a);
    const original_rejection = find(current.artifacts, .graph_metric_segment, rank_name).?;
    try std.testing.expectEqualStrings(original_rejection.artifact_id, find(stable.artifacts, .graph_metric_segment, rank_name).?.artifact_id);
    try std.testing.expect(find(stable.artifacts, .graph_metric_segment, rank_name).?.graph_metric_materialization_state == .rejected);
    // Removing the first computation must not rewrite the original [degree,
    // rank] witness into an apparently stable [rank] rejected publication.
    const only_rank = "{\"graph_idx\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":20}}}}";
    plan.table_definition.indexes_json = @constCast(only_rank);
    var removed = try reconcileAlloc(a, current, plan);
    defer removed.deinit(a);
    try std.testing.expect(find(removed.artifacts, .graph_segment, "graph_idx") != null);
    try std.testing.expect(find(removed.artifacts, .graph_metric_segment, rank_name) == null);
    // A changed sibling, or a newly configured graph with no source sidecar,
    // also changes admission even though rank's own kernel is identical.
    const changed_sibling = "{\"graph_idx\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"degree\":{\"kind\":\"pagerank\",\"max_iterations\":5},\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":20}}}}";
    const missing_graph = "{\"graph_idx\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"degree\":{\"kind\":\"degree\"},\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":20}}},\"other\":{\"type\":\"graph\",\"field\":\"other_edges\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":20}}}}";
    for ([_][]const u8{ changed_sibling, missing_graph }) |indexes| {
        plan.table_definition.indexes_json = @constCast(indexes);
        var changed = try reconcileAlloc(a, current, plan);
        defer changed.deinit(a);
        try std.testing.expect(find(changed.artifacts, .graph_metric_segment, rank_name) == null);
    }
    // Ready computations still alias normally, but a rejection cannot acquire
    // a new logical admission plan merely by renaming its reference.
    plan.table_definition.indexes_json = @constCast("{\"graph_alias\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"degree_alias\":{\"kind\":\"degree\"},\"rank_alias\":{\"kind\":\"pagerank\",\"max_iterations\":20}}}}");
    var aliases = try reconcileAlloc(a, current, plan);
    defer aliases.deinit(a);
    try std.testing.expectEqualStrings(find(current.artifacts, .graph_metric_segment, degree_name).?.artifact_id, find(aliases.artifacts, .graph_metric_segment, "11:graph_alias12:degree_alias").?.artifact_id);
    try std.testing.expect(find(aliases.artifacts, .graph_metric_segment, "11:graph_alias10:rank_alias") == null);
    plan.table_definition.indexes_json = @constCast(testing.indexes);
    for (current.artifacts) |*ref| if (std.mem.eql(u8, ref.name, rank_name)) {
        ref.materializer_fingerprint ^= 1;
    };
    var policy_changed = try reconcileAlloc(a, current, plan);
    defer policy_changed.deinit(a);
    try std.testing.expect(find(policy_changed.artifacts, .graph_metric_segment, rank_name) == null);
    try std.testing.expect(find(policy_changed.artifacts, .graph_metric_segment, degree_name) != null);
}

test "serverless external metadata removes an explicit default-named full text index without resurrecting it" {
    const a = std.testing.allocator;
    var fixture = try testing.fixtureAlloc(a, 12);
    defer fixture.deinit(a);
    var text = find(fixture.artifacts, .text_segment, "body_text").?;
    text.name = "full_text_index_v0";
    const refs = [_]manifests.ArtifactRef{text};
    var current = fixture;
    current.artifacts = @constCast(&refs);
    current.stats.indexes_json = @constCast("{\"full_text_index_v0\":{\"type\":\"full_text\"}}");
    current.stats.published_search_sources = .{ .text = .{ .index_name = "full_text_index_v0" } };
    var plan: publication.TablePublicationPlan = .{
        .targets = .{ .published_search_sources = .{} },
        .table_definition = .{ .indexes_json = current.stats.indexes_json },
    };
    var unchanged = try reconcileAlloc(a, current, plan);
    defer unchanged.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), unchanged.artifacts.len);
    try std.testing.expect(unchanged.stats.published_search_sources.findText() != null);
    plan.table_definition.indexes_json = @constCast("{}");
    var removed = try reconcileAlloc(a, current, plan);
    defer removed.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), removed.artifacts.len);
    try std.testing.expectEqual(@as(u32, 0), removed.stats.text_segment_count);
    try std.testing.expect(removed.stats.published_search_sources.findText() == null);
    var repeated = try reconcileAlloc(a, removed, plan);
    defer repeated.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), repeated.artifacts.len);
    try std.testing.expect(repeated.stats.published_search_sources.findText() == null);
}

test "serverless external metadata plan reports exact named work and converges after real removals" {
    const a = std.testing.allocator;
    var current = try testing.fixtureAlloc(a, 16384);
    defer current.deinit(a);
    var table: publication.TablePublicationPlan = .{
        .targets = .{ .published_search_sources = .{} },
        .table_definition = .{ .indexes_json = @constCast(testing.indexes) },
    };
    var stable = try planAlloc(a, current, table);
    defer stable.deinit(a);
    try std.testing.expect(!stable.pendingWork());
    try std.testing.expectEqual(publication.ArtifactAction.reuse, stable.familyAction(.document_segment));
    try std.testing.expectEqual(publication.ArtifactAction.reuse, stable.familyAction(.algebraic_segment));
    for (stable.desired_actions) |item| try std.testing.expectEqual(publication.ArtifactAction.reuse, item.action);

    table.table_definition.indexes_json = @constCast("{\"graph_alias\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"degree_alias\":{\"kind\":\"degree\"}}}}");
    var alias = try planAlloc(a, current, table);
    defer alias.deinit(a);
    try std.testing.expectEqual(publication.ArtifactAction.reuse, alias.action(.graph_segment, "graph_alias"));
    try std.testing.expectEqual(publication.ArtifactAction.reuse, alias.action(.graph_metric_segment, "11:graph_alias12:degree_alias"));
    try std.testing.expectEqual(publication.ArtifactAction.drop, alias.familyAction(.text_segment));
    try std.testing.expect(alias.pendingWork()); // Old logical names really disappear.
    var published_alias = try reconcileAlloc(a, current, table);
    defer published_alias.deinit(a);
    var converged = try planAlloc(a, published_alias, table);
    defer converged.deinit(a);
    try std.testing.expect(!converged.pendingWork());

    table.table_definition.indexes_json = @constCast("{\"graph_alias\":{\"type\":\"graph\",\"field\":\"other_edges\",\"metrics\":{\"degree_alias\":{\"kind\":\"degree\"}}}}");
    var changed = try planAlloc(a, published_alias, table);
    defer changed.deinit(a);
    try std.testing.expectEqual(publication.ArtifactAction.rebuild, changed.action(.graph_segment, "graph_alias"));
    try std.testing.expectEqual(publication.ArtifactAction.rebuild, changed.action(.graph_metric_segment, "11:graph_alias12:degree_alias"));
    try std.testing.expect(changed.pendingWork());
    try std.testing.expectEqual(@as(usize, 0), changed.removed.len);

    table.table_definition.indexes_json = @constCast("{}");
    var deleted = try reconcileAlloc(a, current, table);
    defer deleted.deinit(a);
    var empty = try planAlloc(a, deleted, table);
    defer empty.deinit(a);
    try std.testing.expect(!empty.pendingWork());
    try std.testing.expectEqual(publication.ArtifactAction.reuse, empty.familyAction(.text_segment));
    try std.testing.expectEqual(publication.ArtifactAction.reuse, empty.familyAction(.graph_segment));
}

test "serverless external metadata plan handles bootstrap source changes and terminal rejections without payload IO" {
    const a = std.testing.allocator;
    const schema = "{\"base_source\":{\"kind\":\"external\",\"table_id\":\"docs\",\"format\":\"parquet\",\"uri\":\"s3://warehouse/docs\",\"snapshot\":\"current\",\"schema_fingerprint\":\"schema-v3\"}}";
    var table: publication.TablePublicationPlan = .{
        .targets = .{ .published_search_sources = .{} },
        .table_definition = .{ .schema_json = @constCast(schema), .indexes_json = @constCast("{}") },
    };
    var empty = try planAlloc(a, null, table);
    defer empty.deinit(a);
    try std.testing.expect(!empty.pendingWork());
    table.table_definition.indexes_json = @constCast("{\"alg\":{\"type\":\"algebraic\",\"materializations\":[{\"name\":\"count_by_tenant\",\"op\":\"count\",\"group_by\":[\"tenant\"]}]}}");
    var algebraic = try planAlloc(a, null, table);
    defer algebraic.deinit(a);
    try std.testing.expect(algebraic.pendingWork());
    try std.testing.expectEqual(publication.ArtifactAction.rebuild, algebraic.action(.algebraic_segment, "alg.count_by_tenant"));

    var fixture = try testing.fixtureAlloc(a, 12);
    defer fixture.deinit(a);
    var current = fixture;
    current.stats.schema_json = @constCast(schema);
    table.table_definition.indexes_json = @constCast(testing.indexes);
    for (fixture.artifacts) |*ref| if (ref.kind == .graph_metric_segment) {
        ref.graph_metric_materialization_state = .rejected;
        ref.graph_metric_rejection_reason = .build_budget_exceeded;
    };
    var rejected = try planAlloc(a, current, table);
    defer rejected.deinit(a);
    try std.testing.expect(!rejected.pendingWork());
    for (fixture.artifacts) |*ref| if (ref.kind == .graph_metric_segment) {
        ref.graph_metric_materialization_state = .ready;
        ref.graph_metric_rejection_reason = .none;
        ref.materializer_fingerprint ^= 1;
    };
    var stale_policy = try planAlloc(a, current, table);
    defer stale_policy.deinit(a);
    try std.testing.expect(stale_policy.pendingWork());
    try std.testing.expectEqual(publication.ArtifactAction.rebuild, stale_policy.familyAction(.graph_metric_segment));
    table.table_definition.schema_json = @constCast("{\"base_source\":{\"kind\":\"external\",\"table_id\":\"docs\",\"format\":\"parquet\",\"uri\":\"s3://warehouse/replaced\",\"snapshot\":\"current\",\"schema_fingerprint\":\"schema-v3\"}}");
    var moved = try planAlloc(a, current, table);
    defer moved.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), moved.retained_refs.len);
    for (moved.desired_actions) |item| try std.testing.expectEqual(publication.ArtifactAction.rebuild, item.action);
    const Exercise = struct {
        fn run(alloc: Allocator, input: publication.TablePublicationPlan) !void {
            var result = try planAlloc(alloc, null, input);
            defer result.deinit(alloc);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Exercise.run, .{table});
}
