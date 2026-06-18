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
const manifest_artifact = @import("../manifest/artifact_ref.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_sidecar_algebraic = @import("lake_sidecar_algebraic.zig");
const lake_sidecar_graph = @import("lake_sidecar_graph.zig");
const lake_sidecar_sparse = @import("lake_sidecar_sparse.zig");
const lake_sidecar_text = @import("lake_sidecar_text.zig");
const lake_sidecar_vector = @import("lake_sidecar_vector.zig");

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
            if (got.artifact.kind != want.kind) {
                try decisions.append(alloc, try makeDecision(
                    alloc,
                    want,
                    .rebuild,
                    "artifact kind changed",
                    &.{},
                ));
            } else if (!bindingsEqual(want.binding, got.binding)) {
                try decisions.append(alloc, try makeDecision(
                    alloc,
                    want,
                    .rebuild,
                    rebuildReason(want.binding, got.binding),
                    &.{},
                ));
            } else {
                try decisions.append(alloc, try makeDecision(
                    alloc,
                    want,
                    .reuse,
                    "published artifact matches source binding",
                    got.artifact.artifact_id,
                ));
            }
        } else {
            try decisions.append(alloc, try makeDecision(
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
        try decisions.append(alloc, try makeDropDecision(alloc, got));
    }

    std.mem.sort(Decision, decisions.items, {}, compareDecision);
    return .{ .decisions = try decisions.toOwnedSlice(alloc) };
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
    const executed = try alloc.alloc(ExecutedOperation, plan.operations.len);
    errdefer alloc.free(executed);
    var initialized: usize = 0;
    errdefer {
        for (executed[0..initialized]) |*operation| operation.deinit(alloc);
    }

    for (plan.operations, executed) |operation, *out| {
        out.* = switch (operation.action) {
            .reuse => try makeExecutedOperation(alloc, operation, null, operation.artifact_id),
            .drop => blk: {
                if (operation.artifact_id.len == 0) return error.InvalidLakeRebuildExecution;
                try artifacts.delete(operation.artifact_id);
                break :blk try makeExecutedOperation(alloc, operation, null, operation.artifact_id);
            },
            .rebuild => blk: {
                var source = try source_provider.open(alloc, operation.binding);
                defer source.deinit(alloc);
                const declaration = try executeRebuildOperationAlloc(alloc, artifacts, source, operation);
                errdefer freeOwnedDeclaration(alloc, declaration);
                break :blk try makeExecutedOperation(alloc, operation, declaration, declaration.artifact.artifact_id);
            },
        };
        initialized += 1;
    }

    return .{ .operations = executed };
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
                try declarations.append(alloc, try cloneDeclarationAlloc(alloc, declaration));
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
                try declarations.append(alloc, try declarationFromPublishedAlloc(alloc, existing));
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
) !sidecar_manifest.DeclaredArtifact {
    const build_spec = operation.build_spec orelse return error.MissingLakeRebuildBuildSpec;
    return switch (build_spec) {
        .text => |spec| blk: {
            var result = try lake_sidecar_text.publishTextSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .text_column = spec.text_column,
                .config_json = spec.config_json,
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
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
        .sparse => |spec| blk: {
            var result = try lake_sidecar_sparse.publishSparseSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .sparse_column = spec.sparse_column,
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
        .graph => |spec| blk: {
            var result = try lake_sidecar_graph.publishGraphSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .graph_column = spec.graph_column,
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
            });
            const declaration = result.declaration;
            result = undefined;
            break :blk declaration;
        },
        .algebraic_expression => |spec| blk: {
            var result = try lake_sidecar_algebraic.publishAlgebraicExpressionSidecarFromRowSourceAlloc(alloc, artifacts, source, operation.binding, .{
                .name = operation.name,
                .expressions = spec.expressions,
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
        stringSlicesEqual(a.column_bindings, b.column_bindings);
}

fn rebuildReason(desired: source_binding.Binding, published: source_binding.Binding) []const u8 {
    if (!source_binding.sameSourceSnapshot(desired, published)) return "source snapshot changed";
    if (!std.mem.eql(u8, desired.index_config_hash, published.index_config_hash)) return "index config changed";
    if (!stringSlicesEqual(desired.column_bindings, published.column_bindings)) return "column bindings changed";
    return "source binding changed";
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn cloneBindingAlloc(alloc: Allocator, binding: source_binding.Binding) !source_binding.Binding {
    const source_id = try alloc.dupe(u8, binding.source_id);
    errdefer alloc.free(source_id);
    const snapshot_id = try alloc.dupe(u8, binding.snapshot_id);
    errdefer alloc.free(snapshot_id);
    const schema_fingerprint = try alloc.dupe(u8, binding.schema_fingerprint);
    errdefer alloc.free(schema_fingerprint);
    const index_config_hash = try alloc.dupe(u8, binding.index_config_hash);
    errdefer alloc.free(index_config_hash);
    const column_bindings = try alloc.alloc([]const u8, binding.column_bindings.len);
    errdefer alloc.free(column_bindings);
    var initialized: usize = 0;
    errdefer {
        for (column_bindings[0..initialized]) |column| alloc.free(column);
    }
    for (binding.column_bindings, 0..) |column, idx| {
        column_bindings[idx] = try alloc.dupe(u8, column);
        initialized += 1;
    }

    return .{
        .sidecar_kind = binding.sidecar_kind,
        .source_kind = binding.source_kind,
        .row_ref_kind = binding.row_ref_kind,
        .source_id = source_id,
        .snapshot_id = snapshot_id,
        .schema_fingerprint = schema_fingerprint,
        .column_bindings = column_bindings,
        .index_config_hash = index_config_hash,
    };
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
    alloc.free(binding.source_id);
    alloc.free(binding.snapshot_id);
    alloc.free(binding.schema_fingerprint);
    for (binding.column_bindings) |column| alloc.free(column);
    alloc.free(binding.column_bindings);
    alloc.free(binding.index_config_hash);
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

    fn provider(self: *TestRowSourceProvider) RowSourceProvider {
        return .{
            .ptr = self,
            .open_fn = open,
        };
    }

    fn open(ptr: *anyopaque, alloc: Allocator, binding: source_binding.Binding) !rowsource.Source {
        const self: *TestRowSourceProvider = @ptrCast(@alignCast(ptr));
        if (binding.source_kind != self.source_kind) return error.SidecarSourceBindingMismatch;
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
    next_id: usize = 0,

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
        const artifact_id = try std.fmt.allocPrint(alloc, "mem:{d}", .{self.next_id});
        errdefer alloc.free(artifact_id);
        self.next_id += 1;
        const key = try self.alloc.dupe(u8, artifact_id);
        errdefer self.alloc.free(key);
        const bytes = try self.alloc.dupe(u8, contents);
        errdefer self.alloc.free(bytes);
        try self.entries.put(self.alloc, key, bytes);
        return .{
            .artifact_id = artifact_id,
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
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
        return .{
            .artifact_id = try alloc.dupe(u8, artifact_id),
            .byte_len = @intCast(bytes.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{bytes.len}),
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
    try std.testing.expectEqualStrings("mem:0", executed.artifact_id);
    const stored = try artifacts.getAlloc(executed.artifact_id);
    defer alloc.free(stored);
    try std.testing.expect(stored.len > 0);
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
