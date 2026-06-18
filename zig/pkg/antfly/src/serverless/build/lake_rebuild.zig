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
const manifest_artifact = @import("../manifest/artifact_ref.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");

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

pub const Operation = struct {
    name: []u8,
    action: Action,
    sidecar_kind: source_binding.SidecarKind,
    artifact_kind: manifest_artifact.ArtifactKind,
    builder_kind: ?BuilderKind = null,
    binding: source_binding.Binding,
    reason: []u8,
    artifact_id: []u8 = &.{},

    pub fn deinit(self: *Operation, alloc: Allocator) void {
        alloc.free(self.name);
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
                break :blk try makeOperation(
                    alloc,
                    decision,
                    want.binding,
                    want.kind,
                    builder_kind,
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
                );
            },
        };
        initialized += 1;
    }

    return .{ .operations = operations };
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
) !Operation {
    const name = try alloc.dupe(u8, decision.name);
    errdefer alloc.free(name);
    const owned_binding = try cloneBindingAlloc(alloc, binding);
    errdefer freeOwnedBinding(alloc, owned_binding);
    const reason = try alloc.dupe(u8, decision.reason);
    errdefer alloc.free(reason);
    const artifact_id: []u8 = if (decision.artifact_id.len == 0) &.{} else try alloc.dupe(u8, decision.artifact_id);
    errdefer if (artifact_id.len != 0) alloc.free(artifact_id);

    return .{
        .name = name,
        .action = decision.action,
        .sidecar_kind = binding.sidecar_kind,
        .artifact_kind = artifact_kind,
        .builder_kind = builder_kind,
        .binding = owned_binding,
        .reason = reason,
        .artifact_id = artifact_id,
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

fn freeOwnedBinding(alloc: Allocator, binding: source_binding.Binding) void {
    alloc.free(binding.source_id);
    alloc.free(binding.snapshot_id);
    alloc.free(binding.schema_fingerprint);
    for (binding.column_bindings) |column| alloc.free(column);
    alloc.free(binding.column_bindings);
    alloc.free(binding.index_config_hash);
}

fn compareDecision(_: void, lhs: Decision, rhs: Decision) bool {
    return std.mem.lessThan(u8, lhs.name, rhs.name);
}

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
            .builder_kind = .algebraic_group_by,
        },
        .{
            .name = "events.amount_folds",
            .binding = algebraic_expression_binding,
            .kind = .algebraic_segment,
            .builder_kind = .algebraic_expression,
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
