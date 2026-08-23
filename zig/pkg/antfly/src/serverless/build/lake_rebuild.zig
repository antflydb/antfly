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

//! Lake-native rebuild planning. This is the operator-facing dry-run layer that
//! decides whether RowSource-derived sidecars and materializations can be reused
//! for a pinned source snapshot, must be rebuilt, or should be dropped because
//! no desired binding references them anymore.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebraic_segment = @import("../algebraic_segment/mod.zig");
const artifact_store = @import("../artifacts/store.zig");
const external_source = @import("../external_source/types.zig");
const manifest_artifact = @import("../manifest/artifact_ref.zig");
const manifest_base_source = @import("../manifest/base_source.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_sidecar_algebraic = @import("lake_sidecar_algebraic.zig");
const lake_sidecar_graph = @import("lake_sidecar_graph.zig");
const graph_metric_config = @import("graph_metric_config.zig");
const graph_metric_policy = @import("graph_metric_policy.zig");
const graph_metric_segment = @import("../graph_metric_segment/mod.zig");
const lake_graph_metric = @import("lake_graph_metric.zig");
const lake_sidecar_sparse = @import("lake_sidecar_sparse.zig");
const lake_sidecar_text = @import("lake_sidecar_text.zig");
const lake_sidecar_vector = @import("lake_sidecar_vector.zig");
const lake_build_limits = @import("lake_build_limits.zig");
const lake_replay = @import("lake_replay.zig");

const default_full_text_index_name = "full_text_index_v0";
const default_chunk_embedding_index_name = "serverless_chunk";
const default_sparse_embedding_index_name = "serverless_sparse";

pub const Action = enum {
    reuse,
    rebuild,
    drop,
};

pub const DesiredArtifact = struct {
    name: []const u8,
    binding: source_binding.Binding,
    kind: manifest_artifact.ArtifactKind,
    builder_kind: ?BuilderKind = null,
    build_spec: ?BuildSpec = null,
};

pub const LakeSourceSnapshot = struct {
    source_kind: rowsource.SourceKind,
    source_id: []const u8,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,

    pub fn validate(self: LakeSourceSnapshot) !void {
        if (self.snapshot_id.len == 0) return error.InvalidLakeRebuildDesiredArtifacts;
        if (self.schema_fingerprint.len == 0) return error.InvalidLakeRebuildDesiredArtifacts;
        switch (self.source_kind) {
            .external_parquet, .external_iceberg, .external_lance => {
                if (self.source_id.len == 0) return error.InvalidLakeRebuildDesiredArtifacts;
            },
            .serverless_fragment, .relational_store, .json_materialized => {},
        }
    }
};

pub const TableIndexDefinition = struct {
    table_name: []const u8,
    schema_json: []const u8 = &.{},
    read_schema_json: []const u8 = &.{},
    indexes_json: []const u8 = &.{},
};

pub const DesiredArtifactSet = struct {
    artifacts: []DesiredArtifact,

    pub fn deinit(self: *DesiredArtifactSet, alloc: Allocator) void {
        for (self.artifacts) |*artifact| freeOwnedDesiredArtifact(alloc, artifact);
        alloc.free(self.artifacts);
        self.* = undefined;
    }

    pub fn find(self: DesiredArtifactSet, name: []const u8) ?DesiredArtifact {
        for (self.artifacts) |artifact| {
            if (std.mem.eql(u8, artifact.name, name)) return artifact;
        }
        return null;
    }
};

pub const PublishedArtifact = struct {
    name: []const u8,
    binding: source_binding.Binding,
    artifact: manifest_artifact.ArtifactRef,
};

pub const Decision = struct {
    name: []u8,
    sidecar_kind: source_binding.SidecarKind,
    action: Action,
    reason: []u8,
    artifact_id: []u8 = &.{},

    pub fn deinit(self: *Decision, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.reason);
        if (self.artifact_id.len != 0) alloc.free(self.artifact_id);
        self.* = undefined;
    }
};

pub const Plan = struct {
    decisions: []Decision,

    pub fn deinit(self: *Plan, alloc: Allocator) void {
        for (self.decisions) |*decision| decision.deinit(alloc);
        alloc.free(self.decisions);
        self.* = undefined;
    }

    pub fn find(self: Plan, name: []const u8) ?Decision {
        for (self.decisions) |decision| {
            if (std.mem.eql(u8, decision.name, name)) return decision;
        }
        return null;
    }

    pub fn anyRebuild(self: Plan) bool {
        for (self.decisions) |decision| {
            if (decision.action == .rebuild) return true;
        }
        return false;
    }
};

pub const BuilderKind = enum {
    text,
    vector,
    sparse,
    graph,
    algebraic_group_by,
    algebraic_expression,
};

pub const TextBuildSpec = struct {
    text_column: []const u8 = &.{},
    config_json: []const u8 = "{}",
};

pub const VectorBuildSpec = struct {
    vector_column: []const u8 = &.{},
    embedding_name: ?[]const u8 = null,
};

pub const SparseBuildSpec = struct {
    sparse_column: []const u8 = &.{},
};

pub const GraphBuildSpec = struct {
    graph_column: []const u8 = &.{},
};

pub const AlgebraicGroupByBuildSpec = struct {
    group_column: []const u8,
    value_column: []const u8 = &.{},
    op: algebraic_segment.AggregateOp,
};

pub const AlgebraicExpressionBuildSpec = struct {
    expressions: []const algebraic_segment.ExpressionSpec,
};

pub const BuildSpec = union(BuilderKind) {
    text: TextBuildSpec,
    vector: VectorBuildSpec,
    sparse: SparseBuildSpec,
    graph: GraphBuildSpec,
    algebraic_group_by: AlgebraicGroupByBuildSpec,
    algebraic_expression: AlgebraicExpressionBuildSpec,
};

pub const Operation = struct {
    name: []u8,
    action: Action,
    sidecar_kind: source_binding.SidecarKind,
    artifact_kind: manifest_artifact.ArtifactKind,
    builder_kind: ?BuilderKind = null,
    build_spec: ?BuildSpec = null,
    binding: source_binding.Binding,
    reason: []u8,
    artifact_id: []u8 = &.{},

    pub fn deinit(self: *Operation, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.build_spec) |*build_spec| freeOwnedBuildSpec(alloc, build_spec);
        freeOwnedBinding(alloc, self.binding);
        alloc.free(self.reason);
        if (self.artifact_id.len != 0) alloc.free(self.artifact_id);
        self.* = undefined;
    }
};

pub const OperationPlan = struct {
    operations: []Operation,

    pub fn deinit(self: *OperationPlan, alloc: Allocator) void {
        for (self.operations) |*operation| operation.deinit(alloc);
        alloc.free(self.operations);
        self.* = undefined;
    }

    pub fn find(self: OperationPlan, name: []const u8) ?Operation {
        for (self.operations) |operation| {
            if (std.mem.eql(u8, operation.name, name)) return operation;
        }
        return null;
    }
};

pub const RowSourceProvider = struct {
    ptr: *anyopaque,
    open_fn: *const fn (*anyopaque, Allocator, source_binding.Binding) anyerror!rowsource.Source,

    pub fn open(self: RowSourceProvider, alloc: Allocator, binding: source_binding.Binding) !rowsource.Source {
        return try self.open_fn(self.ptr, alloc, binding);
    }
};

pub const ExecutionOptions = struct {
    limits: lake_build_limits.Limits = .{},
};

pub const ExecutedOperation = struct {
    name: []u8,
    action: Action,
    declaration: ?sidecar_manifest.DeclaredArtifact = null,
    artifact_id: []u8 = &.{},

    pub fn deinit(self: *ExecutedOperation, alloc: Allocator) void {
        alloc.free(self.name);
        if (self.declaration) |declaration| freeOwnedDeclaration(alloc, declaration);
        if (self.artifact_id.len != 0) alloc.free(self.artifact_id);
        self.* = undefined;
    }
};

pub const ExecutionResult = struct {
    operations: []ExecutedOperation,

    pub fn deinit(self: *ExecutionResult, alloc: Allocator) void {
        for (self.operations) |*operation| operation.deinit(alloc);
        alloc.free(self.operations);
        self.* = undefined;
    }

    pub fn find(self: ExecutionResult, name: []const u8) ?ExecutedOperation {
        for (self.operations) |operation| {
            if (std.mem.eql(u8, operation.name, name)) return operation;
        }
        return null;
    }
};

pub const ReconciledManifest = struct {
    artifacts: []sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *ReconciledManifest, alloc: Allocator) void {
        for (self.artifacts) |declaration| freeOwnedDeclaration(alloc, declaration);
        alloc.free(self.artifacts);
        self.* = undefined;
    }

    pub fn manifest(self: ReconciledManifest) sidecar_manifest.Manifest {
        return .{ .artifacts = self.artifacts };
    }

    pub fn find(self: ReconciledManifest, name: []const u8) ?sidecar_manifest.DeclaredArtifact {
        return self.manifest().find(name);
    }
};

pub fn planAlloc(
    alloc: Allocator,
    desired: []const DesiredArtifact,
    published: []const PublishedArtifact,
) !Plan {
    var decisions = std.ArrayListUnmanaged(Decision).empty;
    errdefer {
        for (decisions.items) |*decision| decision.deinit(alloc);
        decisions.deinit(alloc);
    }

    for (desired) |want| {
        try want.binding.validate();
        if (want.name.len == 0) return error.InvalidLakeRebuildPlan;
        try validateDesiredArtifact(want);
        const existing = findPublished(published, want.name);
        if (existing) |got| {
            try got.binding.validate();
            if (got.artifact.artifact_id.len == 0) return error.InvalidLakeRebuildPlan;
            try validatePublishedArtifact(got);
            try decisions.ensureUnusedCapacity(alloc, 1);
            if (got.artifact.kind != want.kind) {
                decisions.appendAssumeCapacity(try makeDecision(
                    alloc,
                    want,
                    .rebuild,
                    "artifact kind changed",
                    &.{},
                ));
            } else if (!bindingsEqual(want.binding, got.binding)) {
                decisions.appendAssumeCapacity(try makeDecision(
                    alloc,
                    want,
                    .rebuild,
                    rebuildReason(want.binding, got.binding),
                    &.{},
                ));
            } else {
                decisions.appendAssumeCapacity(try makeDecision(
                    alloc,
                    want,
                    .reuse,
                    "published artifact matches source binding",
                    got.artifact.artifact_id,
                ));
            }
        } else {
            try decisions.ensureUnusedCapacity(alloc, 1);
            decisions.appendAssumeCapacity(try makeDecision(
                alloc,
                want,
                .rebuild,
                "desired artifact is missing",
                &.{},
            ));
        }
    }

    for (published) |got| {
        try got.binding.validate();
        if (got.name.len == 0) return error.InvalidLakeRebuildPlan;
        try validatePublishedArtifact(got);
        if (findDesired(desired, got.name) != null) continue;
        try decisions.ensureUnusedCapacity(alloc, 1);
        decisions.appendAssumeCapacity(try makeDropDecision(alloc, got));
    }

    std.mem.sort(Decision, decisions.items, {}, compareDecision);
    return .{ .decisions = try decisions.toOwnedSlice(alloc) };
}

pub fn desiredArtifactsFromTableDefinitionAlloc(
    alloc: Allocator,
    source: LakeSourceSnapshot,
    table: TableIndexDefinition,
) !DesiredArtifactSet {
    try source.validate();

    var parsed_indexes = try std.json.parseFromSlice(std.json.Value, alloc, if (table.indexes_json.len == 0) "{}" else table.indexes_json, .{});
    defer parsed_indexes.deinit();
    const index_root = switch (parsed_indexes.value) {
        .object => |object| object,
        else => return error.InvalidTableIndexMetadata,
    };

    var artifacts = std.ArrayListUnmanaged(DesiredArtifact).empty;
    errdefer {
        for (artifacts.items) |*artifact| freeOwnedDesiredArtifact(alloc, artifact);
        artifacts.deinit(alloc);
    }

    const text_specs = try lakeTextIndexSpecsAlloc(alloc, index_root, table.indexes_json.len != 0);
    defer freeLakeTextIndexSpecs(alloc, text_specs);
    for (text_specs) |spec| {
        const text_column = try textColumnFromIndexConfigAlloc(alloc, spec.config_json);
        defer alloc.free(text_column);
        const index_hash = try indexConfigHashAlloc(alloc, "text", spec.name, spec.config_json, &[_][]const u8{text_column});
        defer alloc.free(index_hash);
        try appendDesiredArtifactAlloc(alloc, &artifacts, source, .{
            .name = spec.name,
            .sidecar_kind = .text,
            .artifact_kind = .text_segment,
            .column_bindings = &[_][]const u8{text_column},
            .index_config_hash = index_hash,
            .build_spec = .{ .text = .{ .text_column = text_column, .config_json = spec.config_json } },
        });
    }

    const embedding_indexes = try listEmbeddingIndexesAlloc(alloc, index_root);
    defer freeEmbeddingIndexes(alloc, embedding_indexes);
    for (embedding_indexes) |source_descriptor| {
        if (source_descriptor.sparse) continue;
        const config_json = try indexConfigJsonAlloc(alloc, index_root, source_descriptor.name);
        defer alloc.free(config_json);
        const vector_column = try configuredColumnOrDefaultAlloc(
            alloc,
            index_root,
            source_descriptor.name,
            if (std.mem.eql(u8, source_descriptor.name, default_chunk_embedding_index_name)) "embedding" else source_descriptor.name,
        );
        defer alloc.free(vector_column);
        const index_hash = try indexConfigHashAlloc(alloc, "vector", source_descriptor.name, config_json, &[_][]const u8{vector_column});
        defer alloc.free(index_hash);
        try appendDesiredArtifactAlloc(alloc, &artifacts, source, .{
            .name = source_descriptor.name,
            .sidecar_kind = .vector,
            .artifact_kind = .vector_segment,
            .column_bindings = &[_][]const u8{vector_column},
            .index_config_hash = index_hash,
            .build_spec = .{ .vector = .{
                .vector_column = vector_column,
                .embedding_name = if (std.mem.eql(u8, source_descriptor.name, default_chunk_embedding_index_name)) null else source_descriptor.name,
            } },
        });
    }

    for (embedding_indexes) |source_descriptor| {
        if (!source_descriptor.sparse) continue;
        const config_json = try indexConfigJsonAlloc(alloc, index_root, source_descriptor.name);
        defer alloc.free(config_json);
        const sparse_column = try configuredColumnOrDefaultAlloc(
            alloc,
            index_root,
            source_descriptor.name,
            if (std.mem.eql(u8, source_descriptor.name, default_sparse_embedding_index_name)) "sparse_embedding" else source_descriptor.name,
        );
        defer alloc.free(sparse_column);
        const index_hash = try indexConfigHashAlloc(alloc, "sparse", source_descriptor.name, config_json, &[_][]const u8{sparse_column});
        defer alloc.free(index_hash);
        try appendDesiredArtifactAlloc(alloc, &artifacts, source, .{
            .name = source_descriptor.name,
            .sidecar_kind = .sparse,
            .artifact_kind = .sparse_segment,
            .column_bindings = &[_][]const u8{sparse_column},
            .index_config_hash = index_hash,
            .build_spec = .{ .sparse = .{ .sparse_column = sparse_column } },
        });
    }

    const graph_names = try listGraphIndexNamesAlloc(alloc, index_root);
    defer freeOwnedStrings(alloc, graph_names);
    for (graph_names) |graph_name| {
        const config_json = try graphIndexConfigJsonAlloc(alloc, index_root, graph_name);
        defer alloc.free(config_json);
        const graph_column = try configuredColumnOrDefaultAlloc(alloc, index_root, graph_name, "graph_edges");
        defer alloc.free(graph_column);
        const index_hash = try indexConfigHashAlloc(alloc, "graph", graph_name, config_json, &[_][]const u8{graph_column});
        defer alloc.free(index_hash);
        try appendDesiredArtifactAlloc(alloc, &artifacts, source, .{
            .name = graph_name,
            .sidecar_kind = .graph,
            .artifact_kind = .graph_segment,
            .column_bindings = &[_][]const u8{graph_column},
            .index_config_hash = index_hash,
            .build_spec = .{ .graph = .{ .graph_column = graph_column } },
        });
    }

    try appendAlgebraicDesiredArtifactsAlloc(alloc, &artifacts, source, index_root);

    std.mem.sort(DesiredArtifact, artifacts.items, {}, compareDesiredArtifact);
    return .{ .artifacts = try artifacts.toOwnedSlice(alloc) };
}

pub fn desiredArtifactsFromResolvedExternalSourceAlloc(
    alloc: Allocator,
    base_source: manifest_base_source.BaseSourceDescriptor,
    inventory: external_source.Inventory,
    table: TableIndexDefinition,
) !DesiredArtifactSet {
    const source = try sourceSnapshotFromResolvedExternalSource(base_source, inventory);
    return try desiredArtifactsFromTableDefinitionAlloc(alloc, source, table);
}

pub fn publishedArtifactsFromDeclarationsAlloc(
    alloc: Allocator,
    declarations: []const sidecar_manifest.DeclaredArtifact,
) ![]PublishedArtifact {
    const published = try alloc.alloc(PublishedArtifact, declarations.len);
    errdefer alloc.free(published);
    for (declarations, published) |declaration, *out| {
        try declaration.validate();
        out.* = .{
            .name = declaration.name,
            .binding = declaration.binding,
            .artifact = declaration.artifact,
        };
    }
    return published;
}

pub fn reconcileResolvedExternalSourceSidecarsAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source_provider: RowSourceProvider,
    base_source: manifest_base_source.BaseSourceDescriptor,
    inventory: external_source.Inventory,
    table: TableIndexDefinition,
    published_declarations: []const sidecar_manifest.DeclaredArtifact,
) !ReconciledManifest {
    var desired = try desiredArtifactsFromResolvedExternalSourceAlloc(alloc, base_source, inventory, table);
    defer desired.deinit(alloc);

    var base_declarations = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    defer base_declarations.deinit(alloc);
    for (published_declarations) |declaration| {
        if (declaration.binding.sidecar_kind != .graph_metric) try base_declarations.append(alloc, declaration);
    }
    const published = try publishedArtifactsFromDeclarationsAlloc(alloc, base_declarations.items);
    defer alloc.free(published);

    var operation_plan = try planOperationsAlloc(alloc, desired.artifacts, published);
    defer operation_plan.deinit(alloc);

    var executed = try executeOperationsAlloc(alloc, artifacts, source_provider, operation_plan);
    defer executed.deinit(alloc);

    var reconciled = try reconcileExecutedOperationsAlloc(alloc, published, operation_plan, executed);
    defer reconciled.deinit(alloc);
    return try appendExternalGraphMetricDeclarationsAlloc(alloc, artifacts, table.indexes_json, reconciled.artifacts, published_declarations);
}

fn appendExternalGraphMetricDeclarationsAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    indexes_json: []const u8,
    base_declarations: []const sidecar_manifest.DeclaredArtifact,
    published_declarations: []const sidecar_manifest.DeclaredArtifact,
) !ReconciledManifest {
    const specs = try graph_metric_config.parseIndexSpecsAlloc(alloc, indexes_json);
    defer graph_metric_config.freeIndexSpecs(alloc, specs);

    var declarations = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer {
        for (declarations.items) |declaration| freeOwnedDeclaration(alloc, declaration);
        declarations.deinit(alloc);
    }
    try declarations.ensureUnusedCapacity(alloc, base_declarations.len);
    for (base_declarations) |declaration| declarations.appendAssumeCapacity(try cloneDeclarationAlloc(alloc, declaration));

    const graph_metric_limits = lake_graph_metric.Limits{};
    var graph_metric_budget = graph_metric_policy.Budget{ .limits = graph_metric_limits };
    for (specs) |spec| {
        const graph_declaration = findDeclaration(base_declarations, spec.index_name) orelse continue;
        if (graph_declaration.binding.sidecar_kind != .graph or graph_declaration.artifact.kind != .graph_segment) return error.SidecarArtifactKindMismatch;

        var reusable = true;
        for (spec.configs) |config| {
            const metric_name = try graph_metric_segment.artifactNameAlloc(alloc, spec.index_name, config.name);
            defer alloc.free(metric_name);
            const existing = findDeclaration(published_declarations, metric_name) orelse {
                reusable = false;
                break;
            };
            if (existing.binding.sidecar_kind != .graph_metric or
                !source_binding.sameSourceSnapshot(existing.binding, graph_declaration.binding))
            {
                reusable = false;
                break;
            }
            const expected_hash = try graphMetricBindingHashAlloc(alloc, config, graph_declaration.artifact.artifact_id);
            defer alloc.free(expected_hash);
            if (!std.mem.eql(u8, existing.binding.index_config_hash, expected_hash) or
                !try graphMetricArtifactReusable(alloc, artifacts, existing.artifact, spec.index_name, config, graph_declaration.artifact))
            {
                reusable = false;
                break;
            }
        }

        if (reusable) {
            try declarations.ensureUnusedCapacity(alloc, spec.configs.len);
            for (spec.configs) |config| {
                const metric_name = try graph_metric_segment.artifactNameAlloc(alloc, spec.index_name, config.name);
                defer alloc.free(metric_name);
                const existing = findDeclaration(published_declarations, metric_name) orelse unreachable;
                declarations.appendAssumeCapacity(try cloneDeclarationAlloc(alloc, existing));
            }
            continue;
        }

        const built = lake_graph_metric.publishManyFromGraphArtifactWithBudgetAlloc(
            alloc,
            artifacts,
            spec.index_name,
            graph_declaration.artifact,
            spec.configs,
            .none,
            graph_metric_limits,
            &graph_metric_budget,
        ) catch |err| switch (err) {
            error.GraphMetricBuildBudgetExceeded => try lake_graph_metric.publishRejectedManyAlloc(
                alloc,
                artifacts,
                spec.index_name,
                graph_declaration.artifact,
                spec.configs,
                .none,
                .build_budget_exceeded,
                graph_metric_limits,
            ),
            else => return err,
        };
        defer {
            for (built) |artifact| lake_graph_metric.freeArtifactRef(alloc, artifact);
            alloc.free(built);
        }
        try declarations.ensureUnusedCapacity(alloc, built.len);
        for (spec.configs, built) |config, artifact| {
            declarations.appendAssumeCapacity(try graphMetricDeclarationAlloc(alloc, graph_declaration, config, artifact));
        }
    }

    const owned = try declarations.toOwnedSlice(alloc);
    errdefer {
        for (owned) |declaration| freeOwnedDeclaration(alloc, declaration);
        alloc.free(owned);
    }
    const result = ReconciledManifest{ .artifacts = owned };
    try result.manifest().validate();
    return result;
}

fn findDeclaration(
    declarations: []const sidecar_manifest.DeclaredArtifact,
    name: []const u8,
) ?sidecar_manifest.DeclaredArtifact {
    for (declarations) |declaration| if (std.mem.eql(u8, declaration.name, name)) return declaration;
    return null;
}

fn graphMetricDeclarationAlloc(
    alloc: Allocator,
    graph_declaration: sidecar_manifest.DeclaredArtifact,
    config: @import("../../graph/graph.zig").GraphMetricConfig,
    artifact: manifest_artifact.ArtifactRef,
) !sidecar_manifest.DeclaredArtifact {
    const name = try alloc.dupe(u8, artifact.name);
    errdefer alloc.free(name);
    var binding = try cloneBindingAlloc(alloc, graph_declaration.binding);
    errdefer freeOwnedBinding(alloc, binding);
    binding.sidecar_kind = .graph_metric;
    const metric_config_hash = try graphMetricBindingHashAlloc(alloc, config, graph_declaration.artifact.artifact_id);
    alloc.free(binding.index_config_hash);
    binding.index_config_hash = metric_config_hash;
    const owned_artifact = try cloneArtifactRefAlloc(alloc, artifact);
    errdefer {
        if (owned_artifact.name.len != 0) alloc.free(owned_artifact.name);
        alloc.free(owned_artifact.artifact_id);
        alloc.free(owned_artifact.checksum);
    }
    const declaration = sidecar_manifest.DeclaredArtifact{ .name = name, .binding = binding, .artifact = owned_artifact };
    try declaration.validate();
    return declaration;
}

pub fn planOperationsAlloc(
    alloc: Allocator,
    desired: []const DesiredArtifact,
    published: []const PublishedArtifact,
) !OperationPlan {
    var decision_plan = try planAlloc(alloc, desired, published);
    defer decision_plan.deinit(alloc);

    const operations = try alloc.alloc(Operation, decision_plan.decisions.len);
    errdefer alloc.free(operations);
    var initialized: usize = 0;
    errdefer {
        for (operations[0..initialized]) |*operation| operation.deinit(alloc);
    }

    for (decision_plan.decisions, operations) |decision, *operation| {
        operation.* = switch (decision.action) {
            .reuse, .rebuild => blk: {
                const want = findDesired(desired, decision.name) orelse return error.InvalidLakeRebuildPlan;
                const builder_kind = if (decision.action == .rebuild) try builderKindForDesired(want) else null;
                var build_spec = if (decision.action == .rebuild) try buildSpecForDesiredAlloc(alloc, want) else null;
                errdefer if (build_spec) |*spec| freeOwnedBuildSpec(alloc, spec);
                break :blk try makeOperation(
                    alloc,
                    decision,
                    want.binding,
                    want.kind,
                    builder_kind,
                    build_spec,
                );
            },
            .drop => blk: {
                const got = findPublished(published, decision.name) orelse return error.InvalidLakeRebuildPlan;
                break :blk try makeOperation(
                    alloc,
                    decision,
                    got.binding,
                    got.artifact.kind,
                    null,
                    null,
                );
            },
        };
        initialized += 1;
    }

    return .{ .operations = operations };
}

pub fn executeOperationsAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source_provider: RowSourceProvider,
    plan: OperationPlan,
) !ExecutionResult {
    return try executeOperationsWithOptionsAlloc(alloc, artifacts, source_provider, plan, .{});
}

pub fn executeOperationsWithOptionsAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source_provider: RowSourceProvider,
    plan: OperationPlan,
    options: ExecutionOptions,
) !ExecutionResult {
    try options.limits.validate();
    const executed = try alloc.alloc(ExecutedOperation, plan.operations.len);
    errdefer alloc.free(executed);
    const completed = try alloc.alloc(bool, plan.operations.len);
    defer alloc.free(completed);
    @memset(completed, false);
    errdefer {
        for (executed, 0..) |*operation, idx| if (completed[idx]) operation.deinit(alloc);
    }

    for (plan.operations, 0..) |operation, operation_idx| {
        if (completed[operation_idx]) continue;
        switch (operation.action) {
            .reuse => {
                executed[operation_idx] = try makeExecutedOperation(alloc, operation, null, operation.artifact_id);
                completed[operation_idx] = true;
            },
            .drop => {
                if (operation.artifact_id.len == 0) return error.InvalidLakeRebuildExecution;
                executed[operation_idx] = try makeExecutedOperation(alloc, operation, null, operation.artifact_id);
                completed[operation_idx] = true;
            },
            .rebuild => {
                const group_count = countPendingRebuildsForSnapshot(plan.operations, completed, operation.binding);
                if (group_count == 1) {
                    var source = try source_provider.open(alloc, operation.binding);
                    defer source.deinit(alloc);
                    const declaration = try executeRebuildOperationAlloc(alloc, artifacts, source, operation, options.limits);
                    errdefer freeOwnedDeclaration(alloc, declaration);
                    executed[operation_idx] = try makeExecutedOperation(alloc, operation, declaration, declaration.artifact.artifact_id);
                    completed[operation_idx] = true;
                    continue;
                }

                const merged_binding = try mergedRebuildBindingAlloc(alloc, plan.operations, completed, operation.binding);
                defer source_binding.freeOwned(alloc, merged_binding);
                var source = try source_provider.open(alloc, merged_binding);
                defer source.deinit(alloc);
                var replay = try lake_replay.Buffer.captureAlloc(alloc, source, options.limits);
                defer replay.deinit(alloc);

                for (plan.operations, 0..) |group_operation, group_idx| {
                    if (completed[group_idx] or group_operation.action != .rebuild or
                        !source_binding.sameSourceSnapshot(group_operation.binding, operation.binding)) continue;
                    var cursor = replay.cursor();
                    const declaration = try executeRebuildOperationAlloc(
                        alloc,
                        artifacts,
                        cursor.rowSource(),
                        group_operation,
                        options.limits,
                    );
                    errdefer freeOwnedDeclaration(alloc, declaration);
                    executed[group_idx] = try makeExecutedOperation(
                        alloc,
                        group_operation,
                        declaration,
                        declaration.artifact.artifact_id,
                    );
                    completed[group_idx] = true;
                }
            },
        }
    }

    return .{ .operations = executed };
}

fn countPendingRebuildsForSnapshot(
    operations: []const Operation,
    completed: []const bool,
    binding: source_binding.Binding,
) usize {
    var count: usize = 0;
    for (operations, completed) |operation, done| {
        if (!done and operation.action == .rebuild and source_binding.sameSourceSnapshot(operation.binding, binding)) count += 1;
    }
    return count;
}

fn mergedRebuildBindingAlloc(
    alloc: Allocator,
    operations: []const Operation,
    completed: []const bool,
    seed: source_binding.Binding,
) !source_binding.Binding {
    var columns = std.ArrayListUnmanaged([]const u8).empty;
    var column_kinds = std.ArrayListUnmanaged(?rowsource.ColumnKind).empty;
    defer column_kinds.deinit(alloc);
    errdefer {
        for (columns.items) |column| alloc.free(@constCast(column));
        columns.deinit(alloc);
    }
    for (operations, completed) |operation, done| {
        if (done or operation.action != .rebuild or !source_binding.sameSourceSnapshot(operation.binding, seed)) continue;
        if (operation.binding.source_kind != seed.source_kind or
            operation.binding.row_ref_kind != seed.row_ref_kind or
            !std.mem.eql(u8, operation.binding.schema_fingerprint, seed.schema_fingerprint))
        {
            return error.InvalidLakeRebuildExecution;
        }
        for (operation.binding.column_bindings, 0..) |column, binding_column_idx| {
            const kind = if (operation.binding.column_kinds.len == 0)
                null
            else
                operation.binding.column_kinds[binding_column_idx];
            var found_idx: ?usize = null;
            for (columns.items, 0..) |existing, existing_idx| {
                if (std.mem.eql(u8, existing, column)) {
                    found_idx = existing_idx;
                    break;
                }
            }
            if (found_idx) |existing_idx| {
                if (column_kinds.items[existing_idx]) |existing_kind| {
                    if (kind) |candidate_kind| {
                        if (candidate_kind != existing_kind) return error.InvalidLakeRebuildExecution;
                    }
                } else if (kind != null) {
                    column_kinds.items[existing_idx] = kind;
                }
            } else {
                const owned_column = try alloc.dupe(u8, column);
                columns.append(alloc, owned_column) catch |err| {
                    alloc.free(owned_column);
                    return err;
                };
                column_kinds.append(alloc, kind) catch |err| {
                    _ = columns.pop();
                    alloc.free(owned_column);
                    return err;
                };
            }
        }
    }

    const source_id = try alloc.dupe(u8, seed.source_id);
    errdefer alloc.free(source_id);
    const snapshot_id = try alloc.dupe(u8, seed.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, seed.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);
    const index_config_hash = try alloc.dupe(u8, "lake-rebuild-fused-v1");
    errdefer alloc.free(index_config_hash);
    const every_kind_known = for (column_kinds.items) |kind| {
        if (kind == null) break false;
    } else true;
    const owned_column_kinds = if (every_kind_known) blk: {
        const kinds = try alloc.alloc(rowsource.ColumnKind, column_kinds.items.len);
        for (column_kinds.items, kinds) |kind, *out| out.* = kind.?;
        break :blk kinds;
    } else try alloc.alloc(rowsource.ColumnKind, 0);
    errdefer alloc.free(owned_column_kinds);
    const column_bindings = try columns.toOwnedSlice(alloc);
    errdefer {
        for (column_bindings) |column| alloc.free(@constCast(column));
        alloc.free(column_bindings);
    }

    const merged = source_binding.Binding{
        .sidecar_kind = seed.sidecar_kind,
        .source_kind = seed.source_kind,
        .row_ref_kind = seed.row_ref_kind,
        .source_id = source_id,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .column_bindings = column_bindings,
        .column_kinds = owned_column_kinds,
        .index_config_hash = index_config_hash,
    };
    try merged.validate();
    return merged;
}

pub fn deleteDroppedArtifactsAfterPublishAlloc(
    artifacts: *artifact_store.ArtifactStore,
    plan: OperationPlan,
    executed: ExecutionResult,
) !void {
    for (plan.operations) |operation| {
        if (operation.action != .drop) continue;
        if (operation.artifact_id.len == 0) return error.InvalidLakeRebuildExecution;
        const result = executed.find(operation.name) orelse return error.InvalidLakeRebuildReconciliation;
        if (result.action != .drop) return error.InvalidLakeRebuildReconciliation;
        if (!std.mem.eql(u8, result.artifact_id, operation.artifact_id)) {
            return error.InvalidLakeRebuildReconciliation;
        }
        try artifacts.delete(operation.artifact_id);
    }
}

pub fn reconcileExecutedOperationsAlloc(
    alloc: Allocator,
    published: []const PublishedArtifact,
    plan: OperationPlan,
    executed: ExecutionResult,
) !ReconciledManifest {
    var declarations = std.ArrayListUnmanaged(sidecar_manifest.DeclaredArtifact).empty;
    errdefer {
        for (declarations.items) |declaration| freeOwnedDeclaration(alloc, declaration);
        declarations.deinit(alloc);
    }

    for (plan.operations) |operation| {
        const result = executed.find(operation.name) orelse return error.InvalidLakeRebuildReconciliation;
        if (result.action != operation.action) return error.InvalidLakeRebuildReconciliation;
        switch (operation.action) {
            .rebuild => {
                const declaration = result.declaration orelse return error.InvalidLakeRebuildReconciliation;
                try declaration.validate();
                if (!std.mem.eql(u8, declaration.name, operation.name)) return error.InvalidLakeRebuildReconciliation;
                if (!std.mem.eql(u8, declaration.artifact.artifact_id, result.artifact_id)) {
                    return error.InvalidLakeRebuildReconciliation;
                }
                if (!bindingsEqual(declaration.binding, operation.binding)) return error.InvalidLakeRebuildReconciliation;
                if (declaration.artifact.kind != operation.artifact_kind) return error.InvalidLakeRebuildReconciliation;
                try declarations.ensureUnusedCapacity(alloc, 1);
                declarations.appendAssumeCapacity(try cloneDeclarationAlloc(alloc, declaration));
            },
            .reuse => {
                const existing = findPublished(published, operation.name) orelse return error.InvalidLakeRebuildReconciliation;
                try existing.binding.validate();
                try validatePublishedArtifact(existing);
                if (!std.mem.eql(u8, existing.artifact.artifact_id, operation.artifact_id)) {
                    return error.InvalidLakeRebuildReconciliation;
                }
                if (!std.mem.eql(u8, result.artifact_id, operation.artifact_id)) {
                    return error.InvalidLakeRebuildReconciliation;
                }
                if (!bindingsEqual(existing.binding, operation.binding)) return error.InvalidLakeRebuildReconciliation;
                try declarations.ensureUnusedCapacity(alloc, 1);
                declarations.appendAssumeCapacity(try declarationFromPublishedAlloc(alloc, existing));
            },
            .drop => {
                const existing = findPublished(published, operation.name) orelse return error.InvalidLakeRebuildReconciliation;
                if (!std.mem.eql(u8, existing.artifact.artifact_id, operation.artifact_id)) {
                    return error.InvalidLakeRebuildReconciliation;
                }
                if (!std.mem.eql(u8, result.artifact_id, operation.artifact_id)) {
                    return error.InvalidLakeRebuildReconciliation;
                }
            },
        }
    }

    const owned = try declarations.toOwnedSlice(alloc);
    const reconciled = ReconciledManifest{ .artifacts = owned };
    try reconciled.manifest().validate();
    return reconciled;
}

fn findPublished(published: []const PublishedArtifact, name: []const u8) ?PublishedArtifact {
    for (published) |artifact| {
        if (std.mem.eql(u8, artifact.name, name)) return artifact;
    }
    return null;
}

fn findDesired(desired: []const DesiredArtifact, name: []const u8) ?DesiredArtifact {
    for (desired) |artifact| {
        if (std.mem.eql(u8, artifact.name, name)) return artifact;
    }
    return null;
}

fn sourceSnapshotFromResolvedExternalSource(
    base_source: manifest_base_source.BaseSourceDescriptor,
    inventory: external_source.Inventory,
) !LakeSourceSnapshot {
    try base_source.validate();
    try inventory.validate();

    const ResolvedExternalBase = struct {
        expected_format: external_source.Format,
        source_kind: rowsource.SourceKind,
        source: manifest_base_source.ExternalBaseSource,
    };
    const resolved: ResolvedExternalBase = switch (base_source) {
        .external_parquet => |source| .{
            .expected_format = external_source.Format.parquet,
            .source_kind = rowsource.SourceKind.external_parquet,
            .source = source,
        },
        .external_iceberg => |source| .{
            .expected_format = external_source.Format.iceberg,
            .source_kind = rowsource.SourceKind.external_iceberg,
            .source = source,
        },
        .external_lance => |source| .{
            .expected_format = external_source.Format.lance,
            .source_kind = rowsource.SourceKind.external_lance,
            .source = source,
        },
        else => return error.InvalidLakeRebuildDesiredArtifacts,
    };

    if (resolved.source.file_inventory_artifact == null) return error.InvalidLakeRebuildDesiredArtifacts;
    if (inventory.format != resolved.expected_format) return error.ExternalSourceInventoryMismatch;
    if (!std.mem.eql(u8, resolved.source.source_uri, inventory.source_uri)) return error.ExternalSourceInventoryMismatch;
    if (!std.mem.eql(u8, resolved.source.snapshot_id, inventory.snapshot_id)) return error.ExternalSourceInventoryMismatch;
    if (!std.mem.eql(u8, resolved.source.schema_fingerprint, inventory.schema_fingerprint)) return error.ExternalSourceInventoryMismatch;

    return .{
        .source_kind = resolved.source_kind,
        .source_id = inventory.source_id,
        .snapshot_id = inventory.snapshot_id,
        .schema_fingerprint = inventory.schema_fingerprint,
    };
}

const DesiredArtifactInput = struct {
    name: []const u8,
    sidecar_kind: source_binding.SidecarKind,
    artifact_kind: manifest_artifact.ArtifactKind,
    column_bindings: []const []const u8,
    column_kinds: []const rowsource.ColumnKind = &.{},
    index_config_hash: []const u8,
    build_spec: BuildSpec,
};

const LakeTextIndexSpec = struct {
    name: []u8,
    config_json: []u8,

    fn deinit(self: *LakeTextIndexSpec, alloc: Allocator) void {
        alloc.free(self.name);
        alloc.free(self.config_json);
        self.* = undefined;
    }
};

const EmbeddingIndexSpec = struct {
    name: []u8,
    sparse: bool = false,

    fn deinit(self: *EmbeddingIndexSpec, alloc: Allocator) void {
        alloc.free(self.name);
        self.* = undefined;
    }
};

fn appendDesiredArtifactAlloc(
    alloc: Allocator,
    artifacts: *std.ArrayListUnmanaged(DesiredArtifact),
    source: LakeSourceSnapshot,
    input: DesiredArtifactInput,
) !void {
    if (input.name.len == 0) return error.InvalidLakeRebuildDesiredArtifacts;
    const name = try alloc.dupe(u8, input.name);
    errdefer alloc.free(name);
    const binding = try bindingForDesiredArtifactAlloc(
        alloc,
        source,
        input.sidecar_kind,
        input.column_bindings,
        input.column_kinds,
        input.index_config_hash,
    );
    errdefer freeOwnedBinding(alloc, binding);
    var build_spec = try cloneBuildSpecAlloc(alloc, input.build_spec);
    errdefer freeOwnedBuildSpec(alloc, &build_spec);
    const artifact = DesiredArtifact{
        .name = name,
        .binding = binding,
        .kind = input.artifact_kind,
        .builder_kind = std.meta.activeTag(build_spec),
        .build_spec = build_spec,
    };
    try validateDesiredArtifact(artifact);
    try artifacts.append(alloc, artifact);
}

fn bindingForDesiredArtifactAlloc(
    alloc: Allocator,
    source: LakeSourceSnapshot,
    sidecar_kind: source_binding.SidecarKind,
    column_bindings: []const []const u8,
    column_kinds: []const rowsource.ColumnKind,
    index_config_hash: []const u8,
) !source_binding.Binding {
    if (column_kinds.len != 0 and column_kinds.len != column_bindings.len) return error.InvalidLakeRebuildDesiredArtifacts;
    const owned_columns = try alloc.alloc([]const u8, column_bindings.len);
    errdefer alloc.free(owned_columns);
    var initialized: usize = 0;
    errdefer {
        for (owned_columns[0..initialized]) |column| alloc.free(column);
    }
    for (column_bindings, 0..) |column, idx| {
        if (column.len == 0) return error.InvalidLakeRebuildDesiredArtifacts;
        owned_columns[idx] = try alloc.dupe(u8, column);
        initialized += 1;
    }
    const owned_column_kinds = try alloc.dupe(rowsource.ColumnKind, column_kinds);
    errdefer alloc.free(owned_column_kinds);

    const source_id = try alloc.dupe(u8, source.source_id);
    errdefer alloc.free(source_id);
    const snapshot_id = try alloc.dupe(u8, source.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, source.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);
    const owned_index_config_hash = try alloc.dupe(u8, index_config_hash);
    errdefer alloc.free(owned_index_config_hash);

    const binding = source_binding.Binding{
        .sidecar_kind = sidecar_kind,
        .source_kind = source.source_kind,
        .row_ref_kind = source_binding.rowRefKindForSourceKind(source.source_kind),
        .source_id = source_id,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .column_bindings = owned_columns,
        .column_kinds = owned_column_kinds,
        .index_config_hash = owned_index_config_hash,
    };
    errdefer freeOwnedBinding(alloc, binding);
    try binding.validate();
    return binding;
}

fn lakeTextIndexSpecsAlloc(alloc: Allocator, index_root: std.json.ObjectMap, allow_default: bool) ![]LakeTextIndexSpec {
    var specs = std.ArrayListUnmanaged(LakeTextIndexSpec).empty;
    errdefer {
        for (specs.items) |*spec| spec.deinit(alloc);
        specs.deinit(alloc);
    }

    var it = index_root.iterator();
    while (it.next()) |entry| {
        if (!isFullTextIndexConfig(entry.value_ptr.*)) continue;
        const config_json = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(entry.value_ptr.*, .{})});
        errdefer alloc.free(config_json);
        try specs.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .config_json = config_json,
        });
    }
    if (specs.items.len == 0 and allow_default) {
        try specs.append(alloc, .{
            .name = try alloc.dupe(u8, default_full_text_index_name),
            .config_json = try alloc.dupe(u8, "{\"type\":\"full_text\"}"),
        });
    }
    std.mem.sort(LakeTextIndexSpec, specs.items, {}, compareLakeTextIndexSpec);
    return try specs.toOwnedSlice(alloc);
}

fn freeLakeTextIndexSpecs(alloc: Allocator, specs: []LakeTextIndexSpec) void {
    for (specs) |*spec| spec.deinit(alloc);
    alloc.free(specs);
}

fn isFullTextIndexConfig(value: std.json.Value) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return true;
    return type_value == .string and std.mem.eql(u8, type_value.string, "full_text");
}

fn compareLakeTextIndexSpec(_: void, lhs: LakeTextIndexSpec, rhs: LakeTextIndexSpec) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn listEmbeddingIndexesAlloc(alloc: Allocator, index_root: std.json.ObjectMap) ![]EmbeddingIndexSpec {
    var specs = std.ArrayListUnmanaged(EmbeddingIndexSpec).empty;
    errdefer {
        for (specs.items) |*spec| spec.deinit(alloc);
        specs.deinit(alloc);
    }
    var it = index_root.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* != .object) continue;
        const type_value = entry.value_ptr.object.get("type") orelse continue;
        if (type_value != .string or !std.mem.eql(u8, type_value.string, "embeddings")) continue;
        const sparse = if (entry.value_ptr.object.get("sparse")) |value| switch (value) {
            .bool => |flag| flag,
            else => return error.InvalidTableIndexMetadata,
        } else false;
        try specs.append(alloc, .{
            .name = try alloc.dupe(u8, entry.key_ptr.*),
            .sparse = sparse,
        });
    }
    std.mem.sort(EmbeddingIndexSpec, specs.items, {}, compareEmbeddingIndexSpec);
    return try specs.toOwnedSlice(alloc);
}

fn compareEmbeddingIndexSpec(_: void, lhs: EmbeddingIndexSpec, rhs: EmbeddingIndexSpec) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn freeEmbeddingIndexes(alloc: Allocator, specs: []EmbeddingIndexSpec) void {
    for (specs) |*spec| spec.deinit(alloc);
    alloc.free(specs);
}

fn textColumnFromIndexConfigAlloc(alloc: Allocator, config_json: []const u8) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, config_json, .{});
    defer parsed.deinit();
    if (jsonStringField(parsed.value, "field")) |field| return try alloc.dupe(u8, field);
    return try alloc.dupe(u8, "text");
}

fn configuredColumnOrDefaultAlloc(
    alloc: Allocator,
    index_root: std.json.ObjectMap,
    index_name: []const u8,
    default_column: []const u8,
) ![]u8 {
    if (index_root.get(index_name)) |config| {
        if (jsonStringField(config, "field")) |field| return try alloc.dupe(u8, field);
        if (jsonStringField(config, "column")) |column| return try alloc.dupe(u8, column);
    }
    return try alloc.dupe(u8, default_column);
}

fn indexConfigJsonAlloc(
    alloc: Allocator,
    index_root: std.json.ObjectMap,
    index_name: []const u8,
) ![]u8 {
    const value = index_root.get(index_name) orelse return try alloc.dupe(u8, "{}");
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(value, .{})});
}

fn graphIndexConfigJsonAlloc(
    alloc: Allocator,
    index_root: std.json.ObjectMap,
    index_name: []const u8,
) ![]u8 {
    const value = index_root.get(index_name) orelse return try alloc.dupe(u8, "{}");
    if (value != .object) return error.InvalidTableIndexMetadata;
    var topology = std.json.ObjectMap.empty;
    defer topology.deinit(alloc);
    var it = value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "metrics")) continue;
        try topology.put(alloc, entry.key_ptr.*, entry.value_ptr.*);
    }
    return try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(std.json.Value{ .object = topology }, .{})});
}

fn graphMetricBindingHashAlloc(
    alloc: Allocator,
    config: @import("../../graph/graph.zig").GraphMetricConfig,
    graph_artifact_id: []const u8,
) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "graph-metric-v2:{x}:{x}:{s}",
        .{
            lake_graph_metric.configFingerprint(config),
            lake_graph_metric.materializerFingerprint(.{}),
            graph_artifact_id,
        },
    );
}

fn graphMetricArtifactReusable(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    ref: manifest_artifact.ArtifactRef,
    graph_index_name: []const u8,
    config: @import("../../graph/graph.zig").GraphMetricConfig,
    graph_ref: manifest_artifact.ArtifactRef,
) !bool {
    artifacts.verifyContentWithCancellationUsingAllocator(alloc, ref.artifact_id, ref.byte_len, ref.checksum, .none) catch |err| switch (err) {
        error.FileNotFound, error.InvalidArtifactId, error.ArtifactIntegrityMismatch => return false,
        else => return err,
    };
    const max_header_bytes: u64 = 1024 * 1024;
    const prefix_len: usize = @intCast(@min(ref.byte_len, max_header_bytes));
    const prefix = artifacts.getRangeAllocWithCancellationUsingAllocator(alloc, ref.artifact_id, 0, prefix_len, .none) catch |err| switch (err) {
        error.FileNotFound, error.InvalidArtifactId, error.InvalidRange => return false,
        else => return err,
    };
    defer alloc.free(prefix);
    const header = graph_metric_segment.decodeHeader(prefix) catch return false;
    return std.mem.eql(u8, header.graph_index_name, graph_index_name) and
        std.mem.eql(u8, header.metric_name, config.name) and
        header.kind == config.kind and
        header.config_fingerprint == lake_graph_metric.configFingerprint(config) and
        header.materializer_fingerprint == lake_graph_metric.materializerFingerprint(.{}) and
        std.mem.eql(u8, header.source_graph_artifact_id, graph_ref.artifact_id) and
        std.mem.eql(u8, header.source_graph_checksum, graph_ref.checksum);
}

fn indexConfigHashAlloc(
    alloc: Allocator,
    kind: []const u8,
    name: []const u8,
    config_json: []const u8,
    columns: []const []const u8,
) ![]u8 {
    var hasher = std.hash.Wyhash.init(0x1a6e_2026_51de_ca12);
    hasher.update(kind);
    hasher.update(&[_]u8{0});
    hasher.update(name);
    hasher.update(&[_]u8{0});
    hasher.update(config_json);
    for (columns) |column| {
        hasher.update(&[_]u8{0});
        hasher.update(column);
    }
    return try std.fmt.allocPrint(alloc, "wyhash64:{x}", .{hasher.final()});
}

fn jsonStringField(value: std.json.Value, field: []const u8) ?[]const u8 {
    if (value != .object) return null;
    const raw = value.object.get(field) orelse return null;
    return switch (raw) {
        .string => |text| text,
        else => null,
    };
}

fn listGraphIndexNamesAlloc(alloc: Allocator, index_root: std.json.ObjectMap) ![][]u8 {
    var names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (names.items) |name| alloc.free(name);
        names.deinit(alloc);
    }

    var it = index_root.iterator();
    while (it.next()) |entry| {
        const type_value = if (entry.value_ptr.* == .object) entry.value_ptr.object.get("type") else null;
        const is_graph = if (type_value) |value|
            value == .string and std.mem.eql(u8, value.string, "graph")
        else
            false;
        if (!is_graph) continue;
        try names.ensureUnusedCapacity(alloc, 1);
        names.appendAssumeCapacity(try alloc.dupe(u8, entry.key_ptr.*));
    }
    std.mem.sort([]u8, names.items, {}, lessString);
    return try names.toOwnedSlice(alloc);
}

fn appendAlgebraicDesiredArtifactsAlloc(
    alloc: Allocator,
    artifacts: *std.ArrayListUnmanaged(DesiredArtifact),
    source: LakeSourceSnapshot,
    index_root: std.json.ObjectMap,
) !void {
    var it = index_root.iterator();
    while (it.next()) |entry| {
        if (!isTypedIndexConfig(entry.value_ptr.*, "algebraic")) continue;
        const config_json = try indexConfigJsonAlloc(alloc, index_root, entry.key_ptr.*);
        defer alloc.free(config_json);
        const materializations = entry.value_ptr.object.get("materializations") orelse continue;
        if (materializations != .array) return error.InvalidTableIndexMetadata;
        for (materializations.array.items) |materialization| {
            try appendAlgebraicMaterializationDesiredArtifactAlloc(
                alloc,
                artifacts,
                source,
                entry.key_ptr.*,
                config_json,
                materialization,
            );
        }
    }
}

fn appendAlgebraicMaterializationDesiredArtifactAlloc(
    alloc: Allocator,
    artifacts: *std.ArrayListUnmanaged(DesiredArtifact),
    source: LakeSourceSnapshot,
    index_name: []const u8,
    config_json: []const u8,
    materialization: std.json.Value,
) !void {
    if (materialization != .object) return error.InvalidTableIndexMetadata;
    if (hasAnyJsonField(materialization, &[_][]const u8{
        "join",
        "time",
        "bucket",
        "histogram_field",
        "range_field",
        "axes",
    })) return;

    const materialization_name = jsonStringField(materialization, "name") orelse return error.InvalidTableIndexMetadata;
    if (materialization_name.len == 0) return error.InvalidTableIndexMetadata;
    const op = supportedAlgebraicOp(materialization) orelse return;
    const value_column = algebraicValueColumn(materialization);
    if (op != .count and (value_column == null or value_column.?.len == 0)) return error.InvalidTableIndexMetadata;

    const group_by = try jsonStringArrayFieldAlloc(alloc, materialization, "group_by");
    defer freeOwnedStrings(alloc, group_by);
    if (group_by.len > 1) return;

    const artifact_name = try std.fmt.allocPrint(alloc, "{s}.{s}", .{ index_name, materialization_name });
    defer alloc.free(artifact_name);

    if (group_by.len == 1) {
        var columns_buf: [2][]const u8 = undefined;
        columns_buf[0] = group_by[0];
        var column_count: usize = 1;
        if (op != .count) {
            columns_buf[1] = value_column.?;
            column_count = 2;
        }
        const columns = columns_buf[0..column_count];
        const column_kinds = algebraicGroupByColumnKinds(op);
        const index_hash = try indexConfigHashAlloc(alloc, "algebraic", artifact_name, config_json, columns);
        defer alloc.free(index_hash);
        try appendDesiredArtifactAlloc(alloc, artifacts, source, .{
            .name = artifact_name,
            .sidecar_kind = .algebraic,
            .artifact_kind = .algebraic_segment,
            .column_bindings = columns,
            .column_kinds = column_kinds,
            .index_config_hash = index_hash,
            .build_spec = .{ .algebraic_group_by = .{
                .group_column = group_by[0],
                .value_column = if (op == .count) &.{} else value_column.?,
                .op = op,
            } },
        });
        return;
    }

    var columns_buf: [1][]const u8 = undefined;
    const columns = if (op == .count) &[_][]const u8{} else blk: {
        columns_buf[0] = value_column.?;
        break :blk columns_buf[0..1];
    };
    const index_hash = try indexConfigHashAlloc(alloc, "algebraic", artifact_name, config_json, columns);
    defer alloc.free(index_hash);
    const column_kinds = algebraicExpressionColumnKinds(op);
    const expressions = [_]algebraic_segment.ExpressionSpec{.{
        .name = materialization_name,
        .value_column = if (op == .count) &.{} else value_column.?,
        .op = op,
    }};
    try appendDesiredArtifactAlloc(alloc, artifacts, source, .{
        .name = artifact_name,
        .sidecar_kind = .algebraic,
        .artifact_kind = .algebraic_segment,
        .column_bindings = columns,
        .column_kinds = column_kinds,
        .index_config_hash = index_hash,
        .build_spec = .{ .algebraic_expression = .{ .expressions = &expressions } },
    });
}

fn algebraicGroupByColumnKinds(op: algebraic_segment.AggregateOp) []const rowsource.ColumnKind {
    return switch (op) {
        .count => &[_]rowsource.ColumnKind{.bytes},
        .sum_i64, .min_i64, .max_i64, .avg_i64 => &[_]rowsource.ColumnKind{ .bytes, .i64 },
    };
}

fn algebraicExpressionColumnKinds(op: algebraic_segment.AggregateOp) []const rowsource.ColumnKind {
    return switch (op) {
        .count => &[_]rowsource.ColumnKind{},
        .sum_i64, .min_i64, .max_i64, .avg_i64 => &[_]rowsource.ColumnKind{.i64},
    };
}

fn isTypedIndexConfig(value: std.json.Value, expected_type: []const u8) bool {
    if (value != .object) return false;
    const type_value = value.object.get("type") orelse return false;
    return type_value == .string and std.mem.eql(u8, type_value.string, expected_type);
}

fn hasAnyJsonField(value: std.json.Value, fields: []const []const u8) bool {
    if (value != .object) return false;
    for (fields) |field| {
        if (value.object.get(field) != null) return true;
    }
    return false;
}

fn supportedAlgebraicOp(materialization: std.json.Value) ?algebraic_segment.AggregateOp {
    const op = jsonStringField(materialization, "op") orelse return null;
    if (std.mem.eql(u8, op, "count")) return .count;
    if (std.mem.eql(u8, op, "sum") or std.mem.eql(u8, op, "sum_i64")) return .sum_i64;
    if (std.mem.eql(u8, op, "min") or std.mem.eql(u8, op, "min_i64")) return .min_i64;
    if (std.mem.eql(u8, op, "max") or std.mem.eql(u8, op, "max_i64")) return .max_i64;
    if (std.mem.eql(u8, op, "avg") or std.mem.eql(u8, op, "avg_i64")) return .avg_i64;
    return null;
}

fn algebraicValueColumn(materialization: std.json.Value) ?[]const u8 {
    return jsonStringField(materialization, "measure") orelse jsonStringField(materialization, "value_field");
}

fn jsonStringArrayFieldAlloc(alloc: Allocator, value: std.json.Value, field: []const u8) ![][]u8 {
    if (value != .object) return error.InvalidTableIndexMetadata;
    const raw = value.object.get(field) orelse return try alloc.alloc([]u8, 0);
    if (raw != .array) return error.InvalidTableIndexMetadata;
    const out = try alloc.alloc([]u8, raw.array.items.len);
    errdefer alloc.free(out);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |item| alloc.free(item);
    }
    for (raw.array.items, 0..) |item, idx| {
        if (item != .string or item.string.len == 0) return error.InvalidTableIndexMetadata;
        out[idx] = try alloc.dupe(u8, item.string);
        initialized += 1;
    }
    return out;
}

fn validateDesiredArtifact(want: DesiredArtifact) !void {
    if (want.kind != sidecar_manifest.artifactKindForSidecarKind(want.binding.sidecar_kind)) {
        return error.SidecarArtifactKindMismatch;
    }
    if (want.builder_kind) |builder_kind| try validateBuilderKind(want.binding.sidecar_kind, builder_kind);
    if (want.build_spec) |build_spec| {
        try validateBuilderKind(want.binding.sidecar_kind, std.meta.activeTag(build_spec));
        if (want.builder_kind) |builder_kind| {
            if (builder_kind != std.meta.activeTag(build_spec)) return error.SidecarArtifactKindMismatch;
        }
    }
}

fn validatePublishedArtifact(got: PublishedArtifact) !void {
    if (got.artifact.kind != sidecar_manifest.artifactKindForSidecarKind(got.binding.sidecar_kind)) {
        return error.SidecarArtifactKindMismatch;
    }
}

fn validateBuilderKind(sidecar_kind: source_binding.SidecarKind, builder_kind: BuilderKind) !void {
    switch (builder_kind) {
        .text => if (sidecar_kind != .text) return error.SidecarArtifactKindMismatch,
        .vector => if (sidecar_kind != .vector) return error.SidecarArtifactKindMismatch,
        .sparse => if (sidecar_kind != .sparse) return error.SidecarArtifactKindMismatch,
        .graph => if (sidecar_kind != .graph) return error.SidecarArtifactKindMismatch,
        .algebraic_group_by, .algebraic_expression => if (sidecar_kind != .algebraic) {
            return error.SidecarArtifactKindMismatch;
        },
    }
}

fn builderKindForDesired(want: DesiredArtifact) !BuilderKind {
    if (want.build_spec) |build_spec| return std.meta.activeTag(build_spec);
    if (want.builder_kind) |builder_kind| {
        try validateBuilderKind(want.binding.sidecar_kind, builder_kind);
        return builder_kind;
    }
    return switch (want.binding.sidecar_kind) {
        .text => .text,
        .vector => .vector,
        .sparse => .sparse,
        .graph => .graph,
        .algebraic => error.AmbiguousLakeRebuildBuilder,
        .graph_metric => error.MissingLakeRebuildBuildSpec,
    };
}

fn buildSpecForDesiredAlloc(alloc: Allocator, want: DesiredArtifact) !BuildSpec {
    if (want.build_spec) |build_spec| return try cloneBuildSpecAlloc(alloc, build_spec);
    return switch (want.binding.sidecar_kind) {
        .text => .{ .text = .{ .text_column = try alloc.dupe(u8, try defaultBoundColumn(want.binding, 0)), .config_json = try alloc.dupe(u8, "{}") } },
        .vector => .{ .vector = .{ .vector_column = try alloc.dupe(u8, try defaultBoundColumn(want.binding, 0)) } },
        .sparse => .{ .sparse = .{ .sparse_column = try alloc.dupe(u8, try defaultBoundColumn(want.binding, 0)) } },
        .graph => .{ .graph = .{ .graph_column = try alloc.dupe(u8, try defaultBoundColumn(want.binding, 0)) } },
        .algebraic => error.MissingLakeRebuildBuildSpec,
        .graph_metric => error.MissingLakeRebuildBuildSpec,
    };
}

fn defaultBoundColumn(binding: source_binding.Binding, idx: usize) ![]const u8 {
    if (idx >= binding.column_bindings.len or binding.column_bindings[idx].len == 0) {
        return error.MissingLakeRebuildBuildSpec;
    }
    return binding.column_bindings[idx];
}

fn makeDecision(
    alloc: Allocator,
    desired: DesiredArtifact,
    action: Action,
    reason: []const u8,
    artifact_id: []const u8,
) !Decision {
    return .{
        .name = try alloc.dupe(u8, desired.name),
        .sidecar_kind = desired.binding.sidecar_kind,
        .action = action,
        .reason = try alloc.dupe(u8, reason),
        .artifact_id = if (artifact_id.len == 0) &.{} else try alloc.dupe(u8, artifact_id),
    };
}

fn makeDropDecision(alloc: Allocator, published: PublishedArtifact) !Decision {
    return .{
        .name = try alloc.dupe(u8, published.name),
        .sidecar_kind = published.binding.sidecar_kind,
        .action = .drop,
        .reason = try alloc.dupe(u8, "published artifact is no longer desired"),
        .artifact_id = try alloc.dupe(u8, published.artifact.artifact_id),
    };
}

fn makeOperation(
    alloc: Allocator,
    decision: Decision,
    binding: source_binding.Binding,
    artifact_kind: manifest_artifact.ArtifactKind,
    builder_kind: ?BuilderKind,
    build_spec: ?BuildSpec,
) !Operation {
    const name = try alloc.dupe(u8, decision.name);
    errdefer alloc.free(name);
    const owned_binding = try cloneBindingAlloc(alloc, binding);
    errdefer freeOwnedBinding(alloc, owned_binding);
    const reason = try alloc.dupe(u8, decision.reason);
    errdefer alloc.free(reason);
    const artifact_id: []u8 = if (decision.artifact_id.len == 0) &.{} else try alloc.dupe(u8, decision.artifact_id);
    errdefer if (artifact_id.len != 0) alloc.free(artifact_id);
    errdefer if (build_spec) |*spec| freeOwnedBuildSpec(alloc, spec);

    return .{
        .name = name,
        .action = decision.action,
        .sidecar_kind = binding.sidecar_kind,
        .artifact_kind = artifact_kind,
        .builder_kind = builder_kind,
        .build_spec = build_spec,
        .binding = owned_binding,
        .reason = reason,
        .artifact_id = artifact_id,
    };
}

fn makeExecutedOperation(
    alloc: Allocator,
    operation: Operation,
    declaration: ?sidecar_manifest.DeclaredArtifact,
    artifact_id: []const u8,
) !ExecutedOperation {
    const name = try alloc.dupe(u8, operation.name);
    errdefer alloc.free(name);
    const artifact_id_owned: []u8 = if (artifact_id.len == 0) &.{} else try alloc.dupe(u8, artifact_id);
    errdefer if (artifact_id_owned.len != 0) alloc.free(artifact_id_owned);
    return .{
        .name = name,
        .action = operation.action,
        .declaration = declaration,
        .artifact_id = artifact_id_owned,
    };
}

fn executeRebuildOperationAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source: rowsource.Source,
    operation: Operation,
    limits: lake_build_limits.Limits,
) !sidecar_manifest.DeclaredArtifact {
    const build_spec = operation.build_spec orelse return error.MissingLakeRebuildBuildSpec;
    return switch (build_spec) {
        .text => |spec| blk: {
            var result = try lake_sidecar_text.publishTextSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .text_column = spec.text_column,
                .config_json = spec.config_json,
                .limits = limits,
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
        .vector => |spec| blk: {
            var result = try lake_sidecar_vector.publishVectorSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .vector_column = spec.vector_column,
                .embedding_name = spec.embedding_name,
                .limits = limits,
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
        .sparse => |spec| blk: {
            var result = try lake_sidecar_sparse.publishSparseSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .sparse_column = spec.sparse_column,
                .limits = limits,
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
        .graph => |spec| blk: {
            var result = try lake_sidecar_graph.publishGraphSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .graph_column = spec.graph_column,
                .limits = limits,
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
        .algebraic_group_by => |spec| blk: {
            var result = try lake_sidecar_algebraic.publishAlgebraicGroupBySidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .group_column = spec.group_column,
                .value_column = spec.value_column,
                .op = spec.op,
                .limits = limits,
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
        .algebraic_expression => |spec| blk: {
            var result = try lake_sidecar_algebraic.publishAlgebraicExpressionSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .expressions = spec.expressions,
                .limits = limits,
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
    };
}

fn bindingsEqual(a: source_binding.Binding, b: source_binding.Binding) bool {
    return a.sidecar_kind == b.sidecar_kind and
        a.source_kind == b.source_kind and
        a.row_ref_kind == b.row_ref_kind and
        std.mem.eql(u8, a.source_id, b.source_id) and
        std.mem.eql(u8, a.snapshot_id, b.snapshot_id) and
        std.mem.eql(u8, a.schema_fingerprint, b.schema_fingerprint) and
        std.mem.eql(u8, a.index_config_hash, b.index_config_hash) and
        stringSlicesEqual(a.column_bindings, b.column_bindings) and
        source_binding.sameColumnKinds(a, b);
}

fn rebuildReason(desired: source_binding.Binding, published: source_binding.Binding) []const u8 {
    if (!source_binding.sameSourceSnapshot(desired, published)) return "source snapshot changed";
    if (!std.mem.eql(u8, desired.index_config_hash, published.index_config_hash)) return "index config changed";
    if (!stringSlicesEqual(desired.column_bindings, published.column_bindings)) return "column bindings changed";
    if (!source_binding.sameColumnKinds(desired, published)) return "column kind bindings changed";
    return "source binding changed";
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn compareDesiredArtifact(_: void, lhs: DesiredArtifact, rhs: DesiredArtifact) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

fn lessString(_: void, lhs: []const u8, rhs: []const u8) bool {
    return std.mem.lessThan(u8, lhs, rhs);
}

fn cloneBindingAlloc(alloc: Allocator, binding: source_binding.Binding) !source_binding.Binding {
    return try source_binding.cloneAlloc(alloc, binding);
}

fn cloneBuildSpecAlloc(alloc: Allocator, build_spec: BuildSpec) !BuildSpec {
    return switch (build_spec) {
        .text => |spec| .{ .text = .{
            .text_column = try alloc.dupe(u8, spec.text_column),
            .config_json = try alloc.dupe(u8, spec.config_json),
        } },
        .vector => |spec| .{ .vector = .{
            .vector_column = try alloc.dupe(u8, spec.vector_column),
            .embedding_name = if (spec.embedding_name) |embedding_name| try alloc.dupe(u8, embedding_name) else null,
        } },
        .sparse => |spec| .{ .sparse = .{
            .sparse_column = try alloc.dupe(u8, spec.sparse_column),
        } },
        .graph => |spec| .{ .graph = .{
            .graph_column = try alloc.dupe(u8, spec.graph_column),
        } },
        .algebraic_group_by => |spec| .{ .algebraic_group_by = .{
            .group_column = try alloc.dupe(u8, spec.group_column),
            .value_column = if (spec.value_column.len == 0) &.{} else try alloc.dupe(u8, spec.value_column),
            .op = spec.op,
        } },
        .algebraic_expression => |spec| blk: {
            const expressions = try alloc.alloc(algebraic_segment.ExpressionSpec, spec.expressions.len);
            errdefer alloc.free(expressions);
            var initialized: usize = 0;
            errdefer {
                for (expressions[0..initialized]) |expression| {
                    alloc.free(expression.name);
                    if (expression.value_column.len != 0) alloc.free(expression.value_column);
                }
            }
            for (spec.expressions, expressions) |expression, *out| {
                out.* = .{
                    .name = try alloc.dupe(u8, expression.name),
                    .value_column = if (expression.value_column.len == 0) &.{} else try alloc.dupe(u8, expression.value_column),
                    .op = expression.op,
                };
                initialized += 1;
            }
            break :blk .{ .algebraic_expression = .{ .expressions = expressions } };
        },
    };
}

fn freeOwnedBinding(alloc: Allocator, binding: source_binding.Binding) void {
    source_binding.freeOwned(alloc, binding);
}

fn freeOwnedDesiredArtifact(alloc: Allocator, artifact: *DesiredArtifact) void {
    alloc.free(artifact.name);
    freeOwnedBinding(alloc, artifact.binding);
    if (artifact.build_spec) |*build_spec| freeOwnedBuildSpec(alloc, build_spec);
    artifact.* = undefined;
}

fn freeOwnedStrings(alloc: Allocator, items: []const []u8) void {
    for (items) |item| alloc.free(item);
    alloc.free(items);
}

fn freeOwnedBuildSpec(alloc: Allocator, build_spec: *BuildSpec) void {
    switch (build_spec.*) {
        .text => |spec| {
            alloc.free(spec.text_column);
            alloc.free(spec.config_json);
        },
        .vector => |spec| {
            alloc.free(spec.vector_column);
            if (spec.embedding_name) |embedding_name| alloc.free(embedding_name);
        },
        .sparse => |spec| alloc.free(spec.sparse_column),
        .graph => |spec| alloc.free(spec.graph_column),
        .algebraic_group_by => |spec| {
            alloc.free(spec.group_column);
            if (spec.value_column.len != 0) alloc.free(spec.value_column);
        },
        .algebraic_expression => |spec| {
            for (spec.expressions) |expression| {
                alloc.free(expression.name);
                if (expression.value_column.len != 0) alloc.free(expression.value_column);
            }
            alloc.free(spec.expressions);
        },
    }
    build_spec.* = undefined;
}

fn cloneArtifactRefAlloc(alloc: Allocator, artifact: manifest_artifact.ArtifactRef) !manifest_artifact.ArtifactRef {
    const name: []u8 = if (artifact.name.len == 0) &.{} else try alloc.dupe(u8, artifact.name);
    errdefer if (name.len != 0) alloc.free(name);
    const artifact_id = try alloc.dupe(u8, artifact.artifact_id);
    errdefer alloc.free(artifact_id);
    const checksum = try alloc.dupe(u8, artifact.checksum);
    errdefer alloc.free(checksum);
    return .{
        .kind = artifact.kind,
        .name = name,
        .artifact_id = artifact_id,
        .byte_len = artifact.byte_len,
        .checksum = checksum,
    };
}

fn cloneDeclarationAlloc(alloc: Allocator, declaration: sidecar_manifest.DeclaredArtifact) !sidecar_manifest.DeclaredArtifact {
    try declaration.validate();
    const name = try alloc.dupe(u8, declaration.name);
    errdefer alloc.free(name);
    const binding = try cloneBindingAlloc(alloc, declaration.binding);
    errdefer freeOwnedBinding(alloc, binding);
    const artifact = try cloneArtifactRefAlloc(alloc, declaration.artifact);
    errdefer {
        if (artifact.name.len != 0) alloc.free(artifact.name);
        alloc.free(artifact.artifact_id);
        alloc.free(artifact.checksum);
    }
    return .{
        .name = name,
        .binding = binding,
        .artifact = artifact,
    };
}

fn declarationFromPublishedAlloc(alloc: Allocator, published: PublishedArtifact) !sidecar_manifest.DeclaredArtifact {
    try published.binding.validate();
    try validatePublishedArtifact(published);
    const declaration = sidecar_manifest.DeclaredArtifact{
        .name = published.name,
        .binding = published.binding,
        .artifact = published.artifact,
    };
    return try cloneDeclarationAlloc(alloc, declaration);
}

fn freeOwnedDeclaration(alloc: Allocator, declaration: sidecar_manifest.DeclaredArtifact) void {
    alloc.free(declaration.name);
    freeOwnedBinding(alloc, declaration.binding);
    if (declaration.artifact.name.len != 0) alloc.free(declaration.artifact.name);
    alloc.free(declaration.artifact.artifact_id);
    alloc.free(declaration.artifact.checksum);
}

fn compareDecision(_: void, lhs: Decision, rhs: Decision) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

const TestRowSourceProvider = struct {
    source_kind: rowsource.SourceKind,
    batches: []const rowsource.ColumnBatch,
    open_count: usize = 0,

    fn provider(self: *TestRowSourceProvider) RowSourceProvider {
        return .{
            .ptr = self,
            .open_fn = open,
        };
    }

    fn open(ptr: *anyopaque, alloc: Allocator, binding: source_binding.Binding) !rowsource.Source {
        const self: *TestRowSourceProvider = @ptrCast(@alignCast(ptr));
        if (binding.source_kind != self.source_kind) return error.SidecarSourceBindingMismatch;
        self.open_count += 1;
        const state = try alloc.create(TestRowSourceState);
        state.* = .{
            .source_kind = self.source_kind,
            .batches = self.batches,
        };
        return .{
            .kind = self.source_kind,
            .ctx = state,
            .next_batch = TestRowSourceState.next,
            .deinit_fn = TestRowSourceState.deinit,
        };
    }
};

const TestRowSourceState = struct {
    source_kind: rowsource.SourceKind,
    batches: []const rowsource.ColumnBatch,
    index: usize = 0,

    fn next(ptr: *anyopaque, alloc: Allocator) !?rowsource.ColumnBatch {
        _ = alloc;
        const self: *TestRowSourceState = @ptrCast(@alignCast(ptr));
        if (self.index >= self.batches.len) return null;
        const batch = self.batches[self.index];
        self.index += 1;
        return batch;
    }

    fn deinit(ptr: *anyopaque, alloc: Allocator) void {
        const self: *TestRowSourceState = @ptrCast(@alignCast(ptr));
        alloc.destroy(self);
    }
};

const MemoryArtifactStore = struct {
    alloc: Allocator,
    entries: std.StringArrayHashMapUnmanaged([]u8) = .empty,

    fn init(alloc: Allocator) MemoryArtifactStore {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *MemoryArtifactStore) void {
        for (self.entries.keys()) |key| self.alloc.free(key);
        for (self.entries.values()) |bytes| self.alloc.free(bytes);
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    fn artifactStore(self: *MemoryArtifactStore) artifact_store.ArtifactStore {
        return .{
            .allocator = self.alloc,
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn put(self: *MemoryArtifactStore, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(contents, &digest, .{});
        const hex = std.fmt.bytesToHex(digest, .lower);
        const artifact_id = try std.fmt.allocPrint(alloc, "{s}{s}", .{ artifact_store.sha256_artifact_id_prefix, hex });
        errdefer alloc.free(artifact_id);
        if (!self.entries.contains(artifact_id)) {
            const key = try self.alloc.dupe(u8, artifact_id);
            errdefer self.alloc.free(key);
            const bytes = try self.alloc.dupe(u8, contents);
            errdefer self.alloc.free(bytes);
            try self.entries.put(self.alloc, key, bytes);
        }
        return .{
            .artifact_id = artifact_id,
            .byte_len = @intCast(contents.len),
            .checksum = try alloc.dupe(u8, &hex),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const bytes = self.entries.get(artifact_id) orelse return error.ArtifactNotFound;
        return try alloc.dupe(u8, bytes);
    }

    fn getRangeAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const bytes = self.entries.get(artifact_id) orelse return error.ArtifactNotFound;
        if (offset > bytes.len) return error.InvalidRange;
        const start: usize = @intCast(offset);
        const end = @min(bytes.len, start + len);
        return try alloc.dupe(u8, bytes[start..end]);
    }

    fn stat(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const bytes = self.entries.get(artifact_id) orelse return error.ArtifactNotFound;
        const owned_id = try alloc.dupe(u8, artifact_id);
        errdefer alloc.free(owned_id);
        const checksum = try alloc.dupe(u8, try artifact_store.sha256ChecksumFromArtifactId(artifact_id));
        return .{
            .artifact_id = owned_id,
            .byte_len = @intCast(bytes.len),
            .checksum = checksum,
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        const index = self.entries.getIndex(artifact_id) orelse return error.ArtifactNotFound;
        const key = self.entries.keys()[index];
        const bytes = self.entries.values()[index];
        self.entries.swapRemoveAt(index);
        self.alloc.free(key);
        self.alloc.free(bytes);
    }

    const vtable: artifact_store.ArtifactStore.VTable = .{
        .deinit = erasedDeinit,
        .put = erasedPut,
        .get_alloc = erasedGetAlloc,
        .get_range_alloc = erasedGetRangeAlloc,
        .stat = erasedStat,
        .delete = erasedDelete,
    };

    fn erasedDeinit(_: Allocator, ptr: *anyopaque) void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn erasedPut(ptr: *anyopaque, alloc: Allocator, contents: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.put(alloc, contents);
    }

    fn erasedGetAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getAlloc(alloc, artifact_id);
    }

    fn erasedGetRangeAlloc(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.getRangeAlloc(alloc, artifact_id, offset, len);
    }

    fn erasedStat(ptr: *anyopaque, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        return try self.stat(alloc, artifact_id);
    }

    fn erasedDelete(ptr: *anyopaque, artifact_id: []const u8) !void {
        const self: *MemoryArtifactStore = @ptrCast(@alignCast(ptr));
        try self.delete(artifact_id);
    }
};

test "lake rebuild planner reuses matching source bindings" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .vector,
        .source_kind = .serverless_fragment,
        .row_ref_kind = .serverless,
        .source_id = "orders",
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    const desired = [_]DesiredArtifact{.{
        .name = "orders.embedding",
        .binding = binding,
        .kind = .vector_segment,
    }};
    const published = [_]PublishedArtifact{.{
        .name = "orders.embedding",
        .binding = binding,
        .artifact = .{ .kind = .vector_segment, .artifact_id = "vec-1", .byte_len = 128, .checksum = "len:128" },
    }};

    var plan = try planAlloc(alloc, &desired, &published);
    defer plan.deinit(alloc);

    try std.testing.expect(!plan.anyRebuild());
    try std.testing.expectEqual(Action.reuse, plan.find("orders.embedding").?.action);
    try std.testing.expectEqualStrings("vec-1", plan.find("orders.embedding").?.artifact_id);
}

test "lake rebuild planner rebuilds algebraic sidecars when typed bindings change" {
    const alloc = std.testing.allocator;
    const desired_binding = source_binding.Binding{
        .sidecar_kind = .algebraic,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-10",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{ "tenant", "amount" },
        .column_kinds = &[_]rowsource.ColumnKind{ .bytes, .i64 },
        .index_config_hash = "sha256:group",
    };
    var published_binding = desired_binding;
    published_binding.column_kinds = &.{};

    const desired = [_]DesiredArtifact{.{
        .name = "events.amount_by_tenant",
        .binding = desired_binding,
        .kind = .algebraic_segment,
    }};
    const published = [_]PublishedArtifact{.{
        .name = "events.amount_by_tenant",
        .binding = published_binding,
        .artifact = .{ .kind = .algebraic_segment, .artifact_id = "fold-1", .byte_len = 64, .checksum = "len:64" },
    }};

    var plan = try planAlloc(alloc, &desired, &published);
    defer plan.deinit(alloc);

    try std.testing.expect(plan.anyRebuild());
    try std.testing.expectEqual(Action.rebuild, plan.find("events.amount_by_tenant").?.action);
    try std.testing.expectEqualStrings("column kind bindings changed", plan.find("events.amount_by_tenant").?.reason);
}

test "lake rebuild desired artifacts derive from table index metadata" {
    const alloc = std.testing.allocator;
    var desired = try desiredArtifactsFromTableDefinitionAlloc(alloc, .{
        .source_kind = .external_parquet,
        .source_id = "events",
        .snapshot_id = "parquet-21",
        .schema_fingerprint = "schema-v4",
    }, .{
        .table_name = "events",
        .schema_json = "{\"version\":1}",
        .indexes_json =
        \\{
        \\  "body_text":{"type":"full_text","field":"body"},
        \\  "semantic_idx":{"type":"embeddings","field":"embedding","dimension":3},
        \\  "sparse_idx":{"type":"embeddings","field":"sparse_terms","sparse":true},
        \\  "graph_idx":{"type":"graph","field":"edges"}
        \\}
        ,
    });
    defer desired.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 4), desired.artifacts.len);
    const text = desired.find("body_text").?;
    try std.testing.expectEqual(source_binding.SidecarKind.text, text.binding.sidecar_kind);
    try std.testing.expectEqualStrings("body", text.binding.column_bindings[0]);
    try std.testing.expectEqual(BuilderKind.text, std.meta.activeTag(text.build_spec.?));
    try std.testing.expectEqualStrings("body", text.build_spec.?.text.text_column);

    const vector = desired.find("semantic_idx").?;
    try std.testing.expectEqual(source_binding.SidecarKind.vector, vector.binding.sidecar_kind);
    try std.testing.expectEqualStrings("embedding", vector.binding.column_bindings[0]);
    try std.testing.expectEqual(BuilderKind.vector, std.meta.activeTag(vector.build_spec.?));
    try std.testing.expectEqualStrings("embedding", vector.build_spec.?.vector.vector_column);
    try std.testing.expectEqualStrings("semantic_idx", vector.build_spec.?.vector.embedding_name.?);

    const sparse = desired.find("sparse_idx").?;
    try std.testing.expectEqual(source_binding.SidecarKind.sparse, sparse.binding.sidecar_kind);
    try std.testing.expectEqualStrings("sparse_terms", sparse.build_spec.?.sparse.sparse_column);

    const graph = desired.find("graph_idx").?;
    try std.testing.expectEqual(source_binding.SidecarKind.graph, graph.binding.sidecar_kind);
    try std.testing.expectEqualStrings("edges", graph.build_spec.?.graph.graph_column);
    try std.testing.expectEqualStrings("parquet-21", graph.binding.snapshot_id);

    var operations = try planOperationsAlloc(alloc, desired.artifacts, &.{});
    defer operations.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), operations.operations.len);
    try std.testing.expectEqual(BuilderKind.text, operations.find("body_text").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.vector, operations.find("semantic_idx").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.sparse, operations.find("sparse_idx").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.graph, operations.find("graph_idx").?.builder_kind.?);
}

test "serverless lake graph topology binding ignores metric-only config changes" {
    const alloc = std.testing.allocator;
    const source: LakeSourceSnapshot = .{
        .source_kind = .external_parquet,
        .source_id = "events",
        .snapshot_id = "parquet-21",
        .schema_fingerprint = "schema-v4",
    };
    var before = try desiredArtifactsFromTableDefinitionAlloc(alloc, source, .{
        .table_name = "events",
        .indexes_json = "{\"graph_idx\":{\"type\":\"graph\",\"field\":\"edges\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":20}}}}",
    });
    defer before.deinit(alloc);
    var after = try desiredArtifactsFromTableDefinitionAlloc(alloc, source, .{
        .table_name = "events",
        .indexes_json = "{\"graph_idx\":{\"type\":\"graph\",\"field\":\"edges\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\",\"max_iterations\":40}}}}",
    });
    defer after.deinit(alloc);
    try std.testing.expectEqualStrings(
        before.find("graph_idx").?.binding.index_config_hash,
        after.find("graph_idx").?.binding.index_config_hash,
    );
}

test "lake rebuild desired artifacts bind resolved external inventory identity" {
    const alloc = std.testing.allocator;
    var inventory = external_source.Inventory{
        .format = .iceberg,
        .source_id = try alloc.dupe(u8, "events-source"),
        .source_uri = try alloc.dupe(u8, "s3://warehouse/events"),
        .snapshot_id = try alloc.dupe(u8, "iceberg-99"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v9"),
        .files = try alloc.alloc(external_source.FileEntry, 0),
    };
    defer inventory.deinit(alloc);

    const base_source = manifest_base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://warehouse/events",
        .snapshot_id = "iceberg-99",
        .schema_fingerprint = "schema-v9",
        .file_inventory_artifact = "inventory-artifact",
    } };

    var desired = try desiredArtifactsFromResolvedExternalSourceAlloc(alloc, base_source, inventory, .{
        .table_name = "events",
        .indexes_json =
        \\{
        \\  "body_text":{"type":"full_text","field":"body"},
        \\  "semantic_idx":{"type":"embeddings","field":"embedding","dimension":3}
        \\}
        ,
    });
    defer desired.deinit(alloc);

    const text = desired.find("body_text").?;
    try std.testing.expectEqual(rowsource.SourceKind.external_iceberg, text.binding.source_kind);
    try std.testing.expectEqualStrings("events-source", text.binding.source_id);
    try std.testing.expectEqualStrings("iceberg-99", text.binding.snapshot_id);
    try std.testing.expectEqualStrings("schema-v9", text.binding.schema_fingerprint);
    try std.testing.expectEqual(source_binding.RowRefKind.external, text.binding.row_ref_kind);

    const vector = desired.find("semantic_idx").?;
    try std.testing.expectEqual(rowsource.SourceKind.external_iceberg, vector.binding.source_kind);
    try std.testing.expectEqualStrings("events-source", vector.binding.source_id);

    const mismatched_base_source = manifest_base_source.BaseSourceDescriptor{ .external_iceberg = .{
        .format = .iceberg,
        .source_uri = "s3://warehouse/events",
        .snapshot_id = "iceberg-stale",
        .schema_fingerprint = "schema-v9",
        .file_inventory_artifact = "inventory-artifact",
    } };
    try std.testing.expectError(
        error.ExternalSourceInventoryMismatch,
        desiredArtifactsFromResolvedExternalSourceAlloc(alloc, mismatched_base_source, inventory, .{
            .table_name = "events",
            .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\"}}",
        }),
    );
}

test "lake rebuild desired artifacts use default lake columns for named indexes" {
    const alloc = std.testing.allocator;
    var desired = try desiredArtifactsFromTableDefinitionAlloc(alloc, .{
        .source_kind = .external_iceberg,
        .source_id = "events",
        .snapshot_id = "iceberg-42",
        .schema_fingerprint = "schema-v5",
    }, .{
        .table_name = "events",
        .schema_json = "{\"version\":1}",
        .indexes_json = "{\"semantic_idx\":{\"type\":\"embeddings\",\"dimension\":3},\"sparse_idx\":{\"type\":\"embeddings\",\"sparse\":true},\"graph_idx\":{\"type\":\"graph\"}}",
    });
    defer desired.deinit(alloc);

    try std.testing.expectEqualStrings("semantic_idx", desired.find("semantic_idx").?.binding.column_bindings[0]);
    try std.testing.expectEqualStrings("sparse_idx", desired.find("sparse_idx").?.binding.column_bindings[0]);
    try std.testing.expectEqualStrings("graph_edges", desired.find("graph_idx").?.binding.column_bindings[0]);
    try std.testing.expect(desired.find("semantic_idx").?.binding.index_config_hash.len > "wyhash64:".len);
}

test "lake rebuild desired artifacts derive supported algebraic materializations" {
    const alloc = std.testing.allocator;
    var desired = try desiredArtifactsFromTableDefinitionAlloc(alloc, .{
        .source_kind = .external_iceberg,
        .source_id = "events",
        .snapshot_id = "iceberg-43",
        .schema_fingerprint = "schema-v6",
    }, .{
        .table_name = "events",
        .schema_json = "{\"version\":1}",
        .indexes_json =
        \\{
        \\  "alg":{"type":"algebraic","materializations":[
        \\    {"name":"count_by_tenant","op":"count","group_by":["tenant"]},
        \\    {"name":"sum_amount","op":"sum","measure":"amount"},
        \\    {"name":"avg_by_tenant","op":"avg","group_by":["tenant"],"measure":"amount"},
        \\    {"name":"sum_by_region_channel","op":"sum","group_by":["region","channel"],"measure":"amount"},
        \\    {"name":"joined_sum","op":"sum","join":"profiles","group_by":["region"],"measure":"amount"}
        \\  ]}
        \\}
        ,
    });
    defer desired.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 4), desired.artifacts.len);
    try std.testing.expect(desired.find(default_full_text_index_name) != null);

    const grouped = desired.find("alg.count_by_tenant").?;
    try std.testing.expectEqual(source_binding.SidecarKind.algebraic, grouped.binding.sidecar_kind);
    try std.testing.expectEqual(@as(usize, 1), grouped.binding.column_kinds.len);
    try std.testing.expectEqual(rowsource.ColumnKind.bytes, grouped.binding.column_kinds[0]);
    try std.testing.expectEqual(BuilderKind.algebraic_group_by, std.meta.activeTag(grouped.build_spec.?));
    try std.testing.expectEqualStrings("tenant", grouped.build_spec.?.algebraic_group_by.group_column);
    try std.testing.expectEqual(algebraic_segment.AggregateOp.count, grouped.build_spec.?.algebraic_group_by.op);

    const expression = desired.find("alg.sum_amount").?;
    try std.testing.expectEqual(@as(usize, 1), expression.binding.column_kinds.len);
    try std.testing.expectEqual(rowsource.ColumnKind.i64, expression.binding.column_kinds[0]);
    try std.testing.expectEqual(BuilderKind.algebraic_expression, std.meta.activeTag(expression.build_spec.?));
    try std.testing.expectEqual(@as(usize, 1), expression.build_spec.?.algebraic_expression.expressions.len);
    try std.testing.expectEqualStrings("sum_amount", expression.build_spec.?.algebraic_expression.expressions[0].name);
    try std.testing.expectEqualStrings("amount", expression.build_spec.?.algebraic_expression.expressions[0].value_column);
    try std.testing.expectEqual(algebraic_segment.AggregateOp.sum_i64, expression.build_spec.?.algebraic_expression.expressions[0].op);

    const avg_grouped = desired.find("alg.avg_by_tenant").?;
    try std.testing.expectEqual(@as(usize, 2), avg_grouped.binding.column_kinds.len);
    try std.testing.expectEqual(rowsource.ColumnKind.bytes, avg_grouped.binding.column_kinds[0]);
    try std.testing.expectEqual(rowsource.ColumnKind.i64, avg_grouped.binding.column_kinds[1]);
    try std.testing.expectEqual(BuilderKind.algebraic_group_by, std.meta.activeTag(avg_grouped.build_spec.?));
    try std.testing.expectEqualStrings("tenant", avg_grouped.build_spec.?.algebraic_group_by.group_column);
    try std.testing.expectEqualStrings("amount", avg_grouped.build_spec.?.algebraic_group_by.value_column);
    try std.testing.expectEqual(algebraic_segment.AggregateOp.avg_i64, avg_grouped.build_spec.?.algebraic_group_by.op);

    var operations = try planOperationsAlloc(alloc, desired.artifacts, &.{});
    defer operations.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 4), operations.operations.len);
    try std.testing.expectEqual(BuilderKind.algebraic_group_by, operations.find("alg.count_by_tenant").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.algebraic_expression, operations.find("alg.sum_amount").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.algebraic_group_by, operations.find("alg.avg_by_tenant").?.builder_kind.?);
}

test "lake rebuild planner rebuilds stale source snapshots and missing folds" {
    const alloc = std.testing.allocator;
    const desired_binding = source_binding.Binding{
        .sidecar_kind = .algebraic,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-9",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{ "tenant", "amount" },
        .index_config_hash = "sha256:fold",
    };
    var stale_binding = desired_binding;
    stale_binding.snapshot_id = "iceberg-8";
    const desired = [_]DesiredArtifact{
        .{ .name = "events.amount_by_tenant", .binding = desired_binding, .kind = .algebraic_segment },
        .{ .name = "events.missing_text", .binding = .{
            .sidecar_kind = .text,
            .source_kind = .external_iceberg,
            .row_ref_kind = .external,
            .source_id = "events",
            .snapshot_id = "iceberg-9",
            .schema_fingerprint = "schema-v2",
            .column_bindings = &[_][]const u8{"body"},
            .index_config_hash = "sha256:text",
        }, .kind = .text_segment },
    };
    const published = [_]PublishedArtifact{.{
        .name = "events.amount_by_tenant",
        .binding = stale_binding,
        .artifact = .{ .kind = .algebraic_segment, .artifact_id = "fold-1", .byte_len = 64, .checksum = "len:64" },
    }};

    var plan = try planAlloc(alloc, &desired, &published);
    defer plan.deinit(alloc);

    try std.testing.expect(plan.anyRebuild());
    try std.testing.expectEqual(Action.rebuild, plan.find("events.amount_by_tenant").?.action);
    try std.testing.expectEqualStrings("source snapshot changed", plan.find("events.amount_by_tenant").?.reason);
    try std.testing.expectEqual(Action.rebuild, plan.find("events.missing_text").?.action);
    try std.testing.expectEqualStrings("desired artifact is missing", plan.find("events.missing_text").?.reason);
}

test "lake rebuild planner drops undesired published artifacts" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .serverless_fragment,
        .row_ref_kind = .serverless,
        .source_id = "orders",
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"edges"},
        .index_config_hash = "sha256:graph",
    };
    const published = [_]PublishedArtifact{.{
        .name = "orders.graph_old",
        .binding = binding,
        .artifact = .{ .kind = .graph_segment, .artifact_id = "graph-1", .byte_len = 32, .checksum = "len:32" },
    }};

    var plan = try planAlloc(alloc, &.{}, &published);
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), plan.decisions.len);
    try std.testing.expectEqual(Action.drop, plan.find("orders.graph_old").?.action);
    try std.testing.expectEqualStrings("graph-1", plan.find("orders.graph_old").?.artifact_id);
}

test "lake rebuild operation planner emits executable builder operations" {
    const alloc = std.testing.allocator;
    const text_binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-10",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const vector_binding = source_binding.Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-10",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    const sparse_binding = source_binding.Binding{
        .sidecar_kind = .sparse,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-10",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"sparse_terms"},
        .index_config_hash = "sha256:sparse",
    };
    const graph_binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-10",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"edges"},
        .index_config_hash = "sha256:graph",
    };
    const algebraic_group_binding = source_binding.Binding{
        .sidecar_kind = .algebraic,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-10",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{ "tenant", "amount" },
        .index_config_hash = "sha256:group",
    };
    const algebraic_expression_binding = source_binding.Binding{
        .sidecar_kind = .algebraic,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-10",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"amount"},
        .index_config_hash = "sha256:expr",
    };
    const desired = [_]DesiredArtifact{
        .{ .name = "events.body_text", .binding = text_binding, .kind = .text_segment },
        .{ .name = "events.embedding", .binding = vector_binding, .kind = .vector_segment },
        .{ .name = "events.sparse_terms", .binding = sparse_binding, .kind = .sparse_segment },
        .{ .name = "events.graph_edges", .binding = graph_binding, .kind = .graph_segment },
        .{
            .name = "events.amount_by_tenant",
            .binding = algebraic_group_binding,
            .kind = .algebraic_segment,
            .build_spec = .{ .algebraic_group_by = .{
                .group_column = "tenant",
                .value_column = "amount",
                .op = .sum_i64,
            } },
        },
        .{
            .name = "events.amount_folds",
            .binding = algebraic_expression_binding,
            .kind = .algebraic_segment,
            .build_spec = .{ .algebraic_expression = .{
                .expressions = &[_]algebraic_segment.ExpressionSpec{
                    .{ .name = "row_count", .op = .count },
                    .{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 },
                },
            } },
        },
    };

    var plan = try planOperationsAlloc(alloc, &desired, &.{});
    defer plan.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 6), plan.operations.len);
    try std.testing.expectEqual(BuilderKind.text, plan.find("events.body_text").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.vector, plan.find("events.embedding").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.sparse, plan.find("events.sparse_terms").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.graph, plan.find("events.graph_edges").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.algebraic_group_by, plan.find("events.amount_by_tenant").?.builder_kind.?);
    try std.testing.expectEqual(BuilderKind.algebraic_expression, plan.find("events.amount_folds").?.builder_kind.?);
    try std.testing.expectEqualStrings("iceberg-10", plan.find("events.amount_folds").?.binding.snapshot_id);
}

test "lake rebuild operation planner fails closed for ambiguous algebraic rebuilds" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .algebraic,
        .source_kind = .external_iceberg,
        .row_ref_kind = .external,
        .source_id = "events",
        .snapshot_id = "iceberg-11",
        .schema_fingerprint = "schema-v2",
        .column_bindings = &[_][]const u8{"amount"},
        .index_config_hash = "sha256:expr",
    };
    const desired = [_]DesiredArtifact{.{
        .name = "events.amount_folds",
        .binding = binding,
        .kind = .algebraic_segment,
    }};

    try std.testing.expectError(error.AmbiguousLakeRebuildBuilder, planOperationsAlloc(alloc, &desired, &.{}));
}

test "lake rebuild operation executor publishes row-source sidecars" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-13",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const desired = [_]DesiredArtifact{.{
        .name = "docs.body_text",
        .binding = binding,
        .kind = .text_segment,
        .build_spec = .{ .text = .{ .text_column = "body", .config_json = "{\"case\":\"lower\"}" } },
    }};
    var plan = try planOperationsAlloc(alloc, &desired, &.{});
    defer plan.deinit(alloc);

    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{ .source_id = "docs", .snapshot_id = "parquet-13", .file_id = "file-a.parquet", .row_group_ordinal = 0, .row_ordinal = 0 } },
        .{ .external = .{ .source_id = "docs", .snapshot_id = "parquet-13", .file_id = "file-a.parquet", .row_group_ordinal = 0, .row_ordinal = 1 } },
    };
    const bodies = [_][]const u8{ "hello lake", "hello sidecar" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "body", .values = .{ .bytes = &bodies } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "docs", .snapshot_id = "parquet-13" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var source_provider = TestRowSourceProvider{ .source_kind = .external_parquet, .batches = &batches };

    var result = try executeOperationsAlloc(alloc, &artifacts, source_provider.provider(), plan);
    defer result.deinit(alloc);

    const executed = result.find("docs.body_text").?;
    try std.testing.expectEqual(Action.rebuild, executed.action);
    try std.testing.expect(executed.declaration != null);
    try std.testing.expect(std.mem.startsWith(u8, executed.artifact_id, artifact_store.sha256_artifact_id_prefix));
    const stored = try artifacts.getAlloc(executed.artifact_id);
    defer alloc.free(stored);
    try std.testing.expect(stored.len > 0);
}

test "lake rebuild operation executor opens each source snapshot once" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const body_binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-13",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:body",
    };
    const title_binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-13",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"title"},
        .index_config_hash = "sha256:title",
    };
    const desired = [_]DesiredArtifact{
        .{
            .name = "docs.body_text",
            .binding = body_binding,
            .kind = .text_segment,
            .build_spec = .{ .text = .{ .text_column = "body" } },
        },
        .{
            .name = "docs.title_text",
            .binding = title_binding,
            .kind = .text_segment,
            .build_spec = .{ .text = .{ .text_column = "title" } },
        },
    };
    var plan = try planOperationsAlloc(alloc, &desired, &.{});
    defer plan.deinit(alloc);

    const row_refs = [_]rowsource.RowRef{.{ .external = .{
        .source_id = "docs",
        .snapshot_id = "parquet-13",
        .file_id = "file-a.parquet",
        .row_group_ordinal = 0,
        .row_ordinal = 0,
    } }};
    const bodies = [_][]const u8{"lake body"};
    const titles = [_][]const u8{"lake title"};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "body", .values = .{ .bytes = &bodies } },
        .{ .name = "title", .values = .{ .bytes = &titles } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "docs", .snapshot_id = "parquet-13" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var source_provider = TestRowSourceProvider{ .source_kind = .external_parquet, .batches = &batches };

    var result = try executeOperationsAlloc(alloc, &artifacts, source_provider.provider(), plan);
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), source_provider.open_count);
    try std.testing.expect(result.find("docs.body_text").?.declaration != null);
    try std.testing.expect(result.find("docs.title_text").?.declaration != null);
}

test "lake rebuild reconciles resolved external sidecars end to end" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    var inventory = external_source.Inventory{
        .format = .parquet,
        .source_id = try alloc.dupe(u8, "docs"),
        .source_uri = try alloc.dupe(u8, "s3://warehouse/docs"),
        .snapshot_id = try alloc.dupe(u8, "parquet-31"),
        .schema_fingerprint = try alloc.dupe(u8, "schema-v3"),
        .files = try alloc.alloc(external_source.FileEntry, 0),
    };
    defer inventory.deinit(alloc);
    const base_source = manifest_base_source.BaseSourceDescriptor{ .external_parquet = .{
        .format = .parquet_prefix,
        .source_uri = "s3://warehouse/docs",
        .snapshot_id = "parquet-31",
        .schema_fingerprint = "schema-v3",
        .file_inventory_artifact = "inventory-docs",
    } };

    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{ .source_id = "docs", .snapshot_id = "parquet-31", .file_id = "file-a.parquet", .row_group_ordinal = 0, .row_ordinal = 0 } },
        .{ .external = .{ .source_id = "docs", .snapshot_id = "parquet-31", .file_id = "file-a.parquet", .row_group_ordinal = 0, .row_ordinal = 1 } },
    };
    const bodies = [_][]const u8{ "lake rebuild workflow", "sidecar reconcile" };
    const target_key = try source_binding.rowRefKeyAlloc(alloc, row_refs[1]);
    defer alloc.free(target_key);
    const first_graph = try std.fmt.allocPrint(alloc, "[{{\"target\":{f},\"edge_type\":\"cites\"}}]", .{std.json.fmt(target_key, .{})});
    defer alloc.free(first_graph);
    const graph_values = [_][]const u8{ first_graph, "[]" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "body", .values = .{ .bytes = &bodies } },
        .{ .name = "graph_edges", .values = .{ .json = &graph_values } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "docs", .snapshot_id = "parquet-31" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};
    var source_provider = TestRowSourceProvider{ .source_kind = .external_parquet, .batches = &batches };

    var reconciled = try reconcileResolvedExternalSourceSidecarsAlloc(
        alloc,
        &artifacts,
        source_provider.provider(),
        base_source,
        inventory,
        .{
            .table_name = "docs",
            .indexes_json = "{\"body_text\":{\"type\":\"full_text\",\"field\":\"body\"},\"graph_idx\":{\"type\":\"graph\",\"field\":\"graph_edges\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\"}}}}",
        },
        &.{},
    );
    defer reconciled.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), reconciled.artifacts.len);
    const declaration = reconciled.find("body_text").?;
    try std.testing.expectEqual(source_binding.SidecarKind.text, declaration.binding.sidecar_kind);
    try std.testing.expectEqual(rowsource.SourceKind.external_parquet, declaration.binding.source_kind);
    try std.testing.expectEqualStrings("docs", declaration.binding.source_id);
    try std.testing.expectEqualStrings("parquet-31", declaration.binding.snapshot_id);
    try std.testing.expectEqualStrings("schema-v3", declaration.binding.schema_fingerprint);
    try std.testing.expect(std.mem.startsWith(u8, declaration.artifact.artifact_id, artifact_store.sha256_artifact_id_prefix));
    const stored = try artifacts.getAlloc(declaration.artifact.artifact_id);
    defer alloc.free(stored);
    try std.testing.expect(stored.len > 0);

    const graph_declaration = reconciled.find("graph_idx").?;
    const metric_artifact_name = try graph_metric_segment.artifactNameAlloc(alloc, "graph_idx", "rank");
    defer alloc.free(metric_artifact_name);
    const metric_declaration = reconciled.find(metric_artifact_name).?;
    try std.testing.expectEqual(source_binding.SidecarKind.graph_metric, metric_declaration.binding.sidecar_kind);
    const metric_payload = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(
        alloc,
        metric_declaration.artifact.artifact_id,
        metric_declaration.artifact.byte_len,
        metric_declaration.artifact.checksum,
        .none,
    );
    defer alloc.free(metric_payload);
    var metric = try graph_metric_segment.decodeAlloc(alloc, metric_payload);
    defer metric.deinit(alloc);
    try std.testing.expectEqualStrings(graph_declaration.artifact.artifact_id, metric.source_graph_artifact_id);
    try std.testing.expect(metric.score(target_key) != null);
}

test "lake rebuild operation planner preserves reuse and drop artifacts" {
    const alloc = std.testing.allocator;
    const text_binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-12",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const graph_binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-12",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"edges"},
        .index_config_hash = "sha256:graph",
    };
    const desired = [_]DesiredArtifact{.{
        .name = "docs.body_text",
        .binding = text_binding,
        .kind = .text_segment,
    }};
    const published = [_]PublishedArtifact{
        .{
            .name = "docs.body_text",
            .binding = text_binding,
            .artifact = .{ .kind = .text_segment, .artifact_id = "text-1", .byte_len = 64, .checksum = "len:64" },
        },
        .{
            .name = "docs.old_graph",
            .binding = graph_binding,
            .artifact = .{ .kind = .graph_segment, .artifact_id = "graph-1", .byte_len = 32, .checksum = "len:32" },
        },
    };

    var plan = try planOperationsAlloc(alloc, &desired, &published);
    defer plan.deinit(alloc);

    const reuse = plan.find("docs.body_text").?;
    try std.testing.expectEqual(Action.reuse, reuse.action);
    try std.testing.expect(reuse.builder_kind == null);
    try std.testing.expectEqualStrings("text-1", reuse.artifact_id);

    const drop = plan.find("docs.old_graph").?;
    try std.testing.expectEqual(Action.drop, drop.action);
    try std.testing.expect(drop.builder_kind == null);
    try std.testing.expectEqualStrings("graph-1", drop.artifact_id);
}

test "lake rebuild drop cleanup waits until after manifest publication" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    var old_meta = try artifacts.put("old graph sidecar");
    defer old_meta.deinit(alloc);

    const graph_binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-12",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"edges"},
        .index_config_hash = "sha256:graph",
    };
    const published = [_]PublishedArtifact{.{
        .name = "docs.old_graph",
        .binding = graph_binding,
        .artifact = .{ .kind = .graph_segment, .name = "docs.old_graph", .artifact_id = old_meta.artifact_id, .byte_len = old_meta.byte_len, .checksum = old_meta.checksum },
    }};
    var plan = try planOperationsAlloc(alloc, &.{}, &published);
    defer plan.deinit(alloc);

    const batches = [_]rowsource.ColumnBatch{};
    var source_provider = TestRowSourceProvider{ .source_kind = .external_parquet, .batches = &batches };
    var executed = try executeOperationsAlloc(alloc, &artifacts, source_provider.provider(), plan);
    defer executed.deinit(alloc);

    const retained = try artifacts.getAlloc(old_meta.artifact_id);
    defer alloc.free(retained);
    try std.testing.expectEqualStrings("old graph sidecar", retained);

    var reconciled = try reconcileExecutedOperationsAlloc(alloc, &published, plan, executed);
    defer reconciled.deinit(alloc);
    try std.testing.expect(reconciled.find("docs.old_graph") == null);

    try deleteDroppedArtifactsAfterPublishAlloc(&artifacts, plan, executed);
    try std.testing.expectError(error.ArtifactNotFound, artifacts.getAlloc(old_meta.artifact_id));
}

test "lake rebuild operation reconciliation publishes rebuilds and reuses while omitting drops" {
    const alloc = std.testing.allocator;
    const text_binding = source_binding.Binding{
        .sidecar_kind = .text,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-12",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"body"},
        .index_config_hash = "sha256:text",
    };
    const vector_binding = source_binding.Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-12",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    const graph_binding = source_binding.Binding{
        .sidecar_kind = .graph,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-12",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"edges"},
        .index_config_hash = "sha256:graph",
    };
    const desired = [_]DesiredArtifact{
        .{
            .name = "docs.body_text",
            .binding = text_binding,
            .kind = .text_segment,
        },
        .{
            .name = "docs.embedding",
            .binding = vector_binding,
            .kind = .vector_segment,
        },
    };
    const published = [_]PublishedArtifact{
        .{
            .name = "docs.body_text",
            .binding = text_binding,
            .artifact = .{ .kind = .text_segment, .name = "docs.body_text", .artifact_id = "text-1", .byte_len = 64, .checksum = "len:64" },
        },
        .{
            .name = "docs.old_graph",
            .binding = graph_binding,
            .artifact = .{ .kind = .graph_segment, .name = "docs.old_graph", .artifact_id = "graph-1", .byte_len = 32, .checksum = "len:32" },
        },
    };
    var plan = try planOperationsAlloc(alloc, &desired, &published);
    defer plan.deinit(alloc);

    const rebuilt = sidecar_manifest.DeclaredArtifact{
        .name = "docs.embedding",
        .binding = vector_binding,
        .artifact = .{ .kind = .vector_segment, .name = "docs.embedding", .artifact_id = "vector-2", .byte_len = 128, .checksum = "len:128" },
    };
    const executed_operations = try alloc.alloc(ExecutedOperation, plan.operations.len);
    errdefer alloc.free(executed_operations);
    var initialized: usize = 0;
    errdefer {
        for (executed_operations[0..initialized]) |*operation| operation.deinit(alloc);
    }
    executed_operations[0] = try makeExecutedOperation(alloc, plan.find("docs.body_text").?, null, "text-1");
    initialized += 1;
    const rebuilt_owned = try cloneDeclarationAlloc(alloc, rebuilt);
    errdefer if (initialized < 2) freeOwnedDeclaration(alloc, rebuilt_owned);
    executed_operations[1] = try makeExecutedOperation(alloc, plan.find("docs.embedding").?, rebuilt_owned, "vector-2");
    initialized += 1;
    executed_operations[2] = try makeExecutedOperation(alloc, plan.find("docs.old_graph").?, null, "graph-1");
    initialized += 1;
    var result = ExecutionResult{ .operations = executed_operations };
    defer result.deinit(alloc);

    var reconciled = try reconcileExecutedOperationsAlloc(alloc, &published, plan, result);
    defer reconciled.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), reconciled.artifacts.len);
    try std.testing.expectEqualStrings("text-1", reconciled.find("docs.body_text").?.artifact.artifact_id);
    try std.testing.expectEqualStrings("vector-2", reconciled.find("docs.embedding").?.artifact.artifact_id);
    try std.testing.expect(reconciled.find("docs.old_graph") == null);
    try reconciled.manifest().validate();
}

test "lake rebuild operation reconciliation fails closed when rebuild declaration is missing" {
    const alloc = std.testing.allocator;
    const binding = source_binding.Binding{
        .sidecar_kind = .vector,
        .source_kind = .external_parquet,
        .row_ref_kind = .external,
        .source_id = "docs",
        .snapshot_id = "parquet-12",
        .schema_fingerprint = "schema-v1",
        .column_bindings = &[_][]const u8{"embedding"},
        .index_config_hash = "sha256:vector",
    };
    const desired = [_]DesiredArtifact{.{
        .name = "docs.embedding",
        .binding = binding,
        .kind = .vector_segment,
    }};
    var plan = try planOperationsAlloc(alloc, &desired, &.{});
    defer plan.deinit(alloc);

    const executed_operations = try alloc.alloc(ExecutedOperation, 1);
    errdefer alloc.free(executed_operations);
    executed_operations[0] = try makeExecutedOperation(alloc, plan.find("docs.embedding").?, null, "vector-2");
    var result = ExecutionResult{ .operations = executed_operations };
    defer result.deinit(alloc);

    try std.testing.expectError(
        error.InvalidLakeRebuildReconciliation,
        reconcileExecutedOperationsAlloc(alloc, &.{}, plan, result),
    );
}
