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

//! Lake-native algebraic sidecar builders over RowSource batches.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebraic_segment = @import("../algebraic_segment/mod.zig");
const aggregate_math = algebraic_segment.aggregate_math;
const artifact_ref = @import("../manifest/artifact_ref.zig");
const artifact_store = @import("../artifacts/store.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const external_rowsource = @import("../../storage/rowsource/external.zig");
const rowsource = @import("../../storage/rowsource/types.zig");
const lake_build_limits = @import("lake_build_limits.zig");

pub const AlgebraicGroupBySidecarBuildOptions = struct {
    name: []const u8,
    group_column: []const u8,
    value_column: []const u8 = &.{},
    op: algebraic_segment.AggregateOp,
    artifact_id: []const u8 = &.{},
    limits: lake_build_limits.Limits = .{},
};

pub const AlgebraicGroupBySidecarBuildResult = struct {
    payload: []u8,
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *AlgebraicGroupBySidecarBuildResult, alloc: Allocator) void {
        alloc.free(self.payload);
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub const AlgebraicGroupBySidecarPublishResult = struct {
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *AlgebraicGroupBySidecarPublishResult, alloc: Allocator) void {
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub const AlgebraicExpressionSidecarBuildOptions = struct {
    name: []const u8,
    expressions: []const algebraic_segment.ExpressionSpec,
    artifact_id: []const u8 = &.{},
    limits: lake_build_limits.Limits = .{},
};

pub const AlgebraicExpressionSidecarBuildResult = struct {
    payload: []u8,
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *AlgebraicExpressionSidecarBuildResult, alloc: Allocator) void {
        alloc.free(self.payload);
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub const AlgebraicExpressionSidecarPublishResult = struct {
    declaration: sidecar_manifest.DeclaredArtifact,

    pub fn deinit(self: *AlgebraicExpressionSidecarPublishResult, alloc: Allocator) void {
        freeOwnedDeclaration(alloc, self.declaration);
        self.* = undefined;
    }
};

pub fn buildAlgebraicGroupBySidecarFromRowSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: AlgebraicGroupBySidecarBuildOptions,
) !AlgebraicGroupBySidecarBuildResult {
    var working_set = try lake_build_limits.WorkingSetAllocator.init(alloc, options.limits);
    return buildAlgebraicGroupBySidecarBoundedAlloc(working_set.allocator(), source, binding, options) catch |err| {
        if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarBuildBudgetExceeded;
        return err;
    };
}

fn buildAlgebraicGroupBySidecarBoundedAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: AlgebraicGroupBySidecarBuildOptions,
) !AlgebraicGroupBySidecarBuildResult {
    try validateGroupByOptions(binding, source.kind, options);
    var budget = try lake_build_limits.Budget.init(options.limits);

    var folds = std.StringHashMapUnmanaged(algebraic_segment.AggregateValue).empty;
    defer {
        var key_it = folds.keyIterator();
        while (key_it.next()) |key| alloc.free(key.*);
        folds.deinit(alloc);
    }

    while (try source.next(alloc)) |batch| {
        try budget.admitBatch(batch);
        try sidecar_manifest.validateBatchAgainstDeclaredArtifact(.{
            .name = options.name,
            .binding = binding,
            .artifact = .{
                .kind = .algebraic_segment,
                .name = options.name,
                .artifact_id = "pending",
                .byte_len = 1,
                .checksum = "pending",
            },
        }, batch);
        try appendBatchGroupBy(alloc, &folds, batch, options);
        try budget.checkRetainedItems(folds.count());
    }

    if (folds.count() == 0) return error.EmptyLakeSidecarAlgebraicSegment;

    var segment = try groupMapToSegmentAlloc(alloc, &folds, binding, options);
    defer algebraic_segment.freeSegment(alloc, &segment);

    const payload = try algebraic_segment.encodeAlloc(alloc, segment);
    errdefer alloc.free(payload);
    try budget.checkOutputBytes(payload.len);

    var declaration = try declaredArtifactAlloc(alloc, binding, options, payload.len);
    errdefer freeOwnedDeclaration(alloc, declaration);
    try declaration.validate();

    return .{
        .payload = payload,
        .declaration = declaration,
    };
}

pub fn publishAlgebraicGroupBySidecarFromRowSourceAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: AlgebraicGroupBySidecarBuildOptions,
) !AlgebraicGroupBySidecarPublishResult {
    var built = try buildAlgebraicGroupBySidecarFromRowSourceAlloc(alloc, source, binding, options);
    defer alloc.free(built.payload);
    errdefer freeOwnedDeclaration(alloc, built.declaration);

    var metadata = try artifacts.put(built.payload);
    var metadata_owned = true;
    errdefer if (metadata_owned) metadata.deinit(alloc);

    alloc.free(built.declaration.artifact.artifact_id);
    alloc.free(built.declaration.artifact.checksum);
    built.declaration.artifact.artifact_id = metadata.artifact_id;
    built.declaration.artifact.byte_len = metadata.byte_len;
    built.declaration.artifact.checksum = metadata.checksum;
    metadata_owned = false;

    try built.declaration.validate();
    return .{ .declaration = built.declaration };
}

pub fn buildAlgebraicExpressionSidecarFromRowSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: AlgebraicExpressionSidecarBuildOptions,
) !AlgebraicExpressionSidecarBuildResult {
    var working_set = try lake_build_limits.WorkingSetAllocator.init(alloc, options.limits);
    return buildAlgebraicExpressionSidecarBoundedAlloc(working_set.allocator(), source, binding, options) catch |err| {
        if (err == error.OutOfMemory and working_set.limit_exceeded) return error.LakeSidecarBuildBudgetExceeded;
        return err;
    };
}

fn buildAlgebraicExpressionSidecarBoundedAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: AlgebraicExpressionSidecarBuildOptions,
) !AlgebraicExpressionSidecarBuildResult {
    try validateExpressionOptions(binding, source.kind, options);
    var budget = try lake_build_limits.Budget.init(options.limits);
    try budget.checkRetainedItems(options.expressions.len);

    const accumulators = try alloc.alloc(ExpressionAccumulator, options.expressions.len);
    defer alloc.free(accumulators);
    for (options.expressions, accumulators) |spec, *accumulator| {
        accumulator.* = initExpressionAccumulator(spec.op);
    }

    while (try source.next(alloc)) |batch| {
        try budget.admitBatch(batch);
        try source_binding.validateBatchAgainstBinding(binding, batch);
        try appendBatchExpressions(accumulators, batch, options);
    }

    var materialization = try accumulatorsToExpressionMaterializationAlloc(alloc, accumulators, binding, options);
    defer algebraic_segment.freeExpressionMaterialization(alloc, &materialization);

    const payload = try algebraic_segment.encodeExpressionAlloc(alloc, materialization);
    errdefer alloc.free(payload);
    try budget.checkOutputBytes(payload.len);

    var declaration = try declaredArtifactForNameAlloc(
        alloc,
        binding,
        options.name,
        options.artifact_id,
        payload.len,
        "lake-algebraic-expression",
    );
    errdefer freeOwnedDeclaration(alloc, declaration);
    try declaration.validate();

    return .{
        .payload = payload,
        .declaration = declaration,
    };
}

pub fn publishAlgebraicExpressionSidecarFromRowSourceAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: AlgebraicExpressionSidecarBuildOptions,
) !AlgebraicExpressionSidecarPublishResult {
    var built = try buildAlgebraicExpressionSidecarFromRowSourceAlloc(alloc, source, binding, options);
    defer alloc.free(built.payload);
    errdefer freeOwnedDeclaration(alloc, built.declaration);

    var metadata = try artifacts.put(built.payload);
    var metadata_owned = true;
    errdefer if (metadata_owned) metadata.deinit(alloc);

    alloc.free(built.declaration.artifact.artifact_id);
    alloc.free(built.declaration.artifact.checksum);
    built.declaration.artifact.artifact_id = metadata.artifact_id;
    built.declaration.artifact.byte_len = metadata.byte_len;
    built.declaration.artifact.checksum = metadata.checksum;
    metadata_owned = false;

    try built.declaration.validate();
    return .{ .declaration = built.declaration };
}

fn validateGroupByOptions(
    binding: source_binding.Binding,
    source_kind: rowsource.SourceKind,
    options: AlgebraicGroupBySidecarBuildOptions,
) !void {
    try binding.validate();
    if (binding.sidecar_kind != .algebraic) return error.InvalidLakeSidecarAlgebraicBuildOptions;
    if (source_kind != binding.source_kind) return error.SidecarSourceBindingMismatch;
    if (options.name.len == 0 or options.group_column.len == 0) {
        return error.InvalidLakeSidecarAlgebraicBuildOptions;
    }
    if (options.op != .count and options.value_column.len == 0) {
        return error.InvalidLakeSidecarAlgebraicBuildOptions;
    }
    const expected_bindings: usize = if (options.op == .count) 1 else 2;
    if (binding.column_bindings.len != expected_bindings) return error.InvalidLakeSidecarAlgebraicBuildOptions;
    if (!std.mem.eql(u8, binding.column_bindings[0], options.group_column)) {
        return error.SidecarSourceBindingMismatch;
    }
    if (options.op != .count and !std.mem.eql(u8, binding.column_bindings[1], options.value_column)) {
        return error.SidecarSourceBindingMismatch;
    }
}

fn validateExpressionOptions(
    binding: source_binding.Binding,
    source_kind: rowsource.SourceKind,
    options: AlgebraicExpressionSidecarBuildOptions,
) !void {
    try binding.validate();
    if (binding.sidecar_kind != .algebraic) return error.InvalidLakeSidecarAlgebraicBuildOptions;
    if (source_kind != binding.source_kind) return error.SidecarSourceBindingMismatch;
    if (options.name.len == 0 or options.expressions.len == 0) {
        return error.InvalidLakeSidecarAlgebraicBuildOptions;
    }

    var expected_binding_idx: usize = 0;
    for (options.expressions, 0..) |expression, expression_idx| {
        if (expression.name.len == 0) return error.InvalidLakeSidecarAlgebraicBuildOptions;
        for (options.expressions[0..expression_idx]) |previous| {
            if (std.mem.eql(u8, previous.name, expression.name)) {
                return error.InvalidLakeSidecarAlgebraicBuildOptions;
            }
        }

        if (expression.op == .count) {
            if (expression.value_column.len != 0) return error.InvalidLakeSidecarAlgebraicBuildOptions;
            continue;
        }
        if (expression.value_column.len == 0) return error.InvalidLakeSidecarAlgebraicBuildOptions;

        var first_value_column_use = true;
        for (options.expressions[0..expression_idx]) |previous| {
            if (previous.op != .count and std.mem.eql(u8, previous.value_column, expression.value_column)) {
                first_value_column_use = false;
                break;
            }
        }
        if (first_value_column_use) {
            if (expected_binding_idx >= binding.column_bindings.len) return error.InvalidLakeSidecarAlgebraicBuildOptions;
            if (!std.mem.eql(u8, binding.column_bindings[expected_binding_idx], expression.value_column)) {
                return error.SidecarSourceBindingMismatch;
            }
            expected_binding_idx += 1;
        }
    }
    if (expected_binding_idx != binding.column_bindings.len) return error.InvalidLakeSidecarAlgebraicBuildOptions;
}

fn appendBatchGroupBy(
    alloc: Allocator,
    folds: *std.StringHashMapUnmanaged(algebraic_segment.AggregateValue),
    batch: rowsource.ColumnBatch,
    options: AlgebraicGroupBySidecarBuildOptions,
) !void {
    const group_column = batch.findColumn(options.group_column) orelse return error.RowSourceColumnNotFound;
    if (group_column.kind() != .bytes) return error.UnsupportedAlgebraicGroupColumnKind;
    const value_column = if (options.op == .count) null else batch.findColumn(options.value_column) orelse return error.RowSourceColumnNotFound;
    if (value_column) |column| {
        if (column.kind() != .i64) return error.UnsupportedAlgebraicValueColumnKind;
    }

    const group_values = group_column.values.bytes;
    for (0..batch.rowCount()) |row_idx| {
        if (group_column.nulls.isNull(row_idx)) continue;
        const key = group_values[row_idx];
        if (key.len == 0) continue;
        const next_value = (try rowAggregateValue(options.op, value_column, row_idx)) orelse continue;
        const owned_key = try alloc.dupe(u8, key);
        errdefer alloc.free(owned_key);
        const entry = try folds.getOrPut(alloc, key);
        if (!entry.found_existing) {
            entry.key_ptr.* = owned_key;
            entry.value_ptr.* = next_value;
        } else {
            alloc.free(owned_key);
            entry.value_ptr.* = try aggregate_math.combine(entry.value_ptr.*, next_value);
        }
    }
}

fn rowAggregateValue(
    op: algebraic_segment.AggregateOp,
    value_column: ?rowsource.ColumnVector,
    row_idx: usize,
) !?algebraic_segment.AggregateValue {
    return switch (op) {
        .count => .{ .count = 1 },
        .sum_i64 => .{ .sum_i64 = if (value_column.?.nulls.isNull(row_idx)) 0 else value_column.?.values.i64[row_idx] },
        .min_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .min_i64 = value_column.?.values.i64[row_idx] },
        .max_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .max_i64 = value_column.?.values.i64[row_idx] },
        .avg_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .avg_i64 = .{
            .sum_i64 = value_column.?.values.i64[row_idx],
            .count = 1,
        } },
    };
}

const ExpressionAccumulator = struct {
    value: algebraic_segment.AggregateValue,
    seen_non_null: bool,
};

fn initExpressionAccumulator(op: algebraic_segment.AggregateOp) ExpressionAccumulator {
    return .{
        .value = switch (op) {
            .count => .{ .count = 0 },
            .sum_i64 => .{ .sum_i64 = 0 },
            .min_i64 => .{ .min_i64 = 0 },
            .max_i64 => .{ .max_i64 = 0 },
            .avg_i64 => .{ .avg_i64 = .{ .sum_i64 = 0, .count = 0 } },
        },
        .seen_non_null = switch (op) {
            .count, .sum_i64 => true,
            .min_i64, .max_i64, .avg_i64 => false,
        },
    };
}

fn appendBatchExpressions(
    accumulators: []ExpressionAccumulator,
    batch: rowsource.ColumnBatch,
    options: AlgebraicExpressionSidecarBuildOptions,
) !void {
    for (options.expressions, accumulators) |expression, *accumulator| {
        if (expression.op == .count) {
            accumulator.value.count = try aggregate_math.addCount(accumulator.value.count, batch.rowCount());
            continue;
        }

        const value_column = batch.findColumn(expression.value_column) orelse return error.RowSourceColumnNotFound;
        if (value_column.kind() != .i64) return error.UnsupportedAlgebraicValueColumnKind;
        for (0..batch.rowCount()) |row_idx| {
            if (value_column.nulls.isNull(row_idx)) continue;
            const value = value_column.values.i64[row_idx];
            switch (expression.op) {
                .count => unreachable,
                .sum_i64 => accumulator.value.sum_i64 = try aggregate_math.addI64(accumulator.value.sum_i64, value),
                .avg_i64 => {
                    accumulator.value.avg_i64.sum_i64 = try aggregate_math.addI64(accumulator.value.avg_i64.sum_i64, value);
                    accumulator.value.avg_i64.count = try aggregate_math.addCount(accumulator.value.avg_i64.count, 1);
                    accumulator.seen_non_null = true;
                },
                .min_i64 => {
                    if (!accumulator.seen_non_null or value < accumulator.value.min_i64) {
                        accumulator.value.min_i64 = value;
                    }
                    accumulator.seen_non_null = true;
                },
                .max_i64 => {
                    if (!accumulator.seen_non_null or value > accumulator.value.max_i64) {
                        accumulator.value.max_i64 = value;
                    }
                    accumulator.seen_non_null = true;
                },
            }
        }
    }
}

fn groupMapToSegmentAlloc(
    alloc: Allocator,
    folds: *std.StringHashMapUnmanaged(algebraic_segment.AggregateValue),
    binding: source_binding.Binding,
    options: AlgebraicGroupBySidecarBuildOptions,
) !algebraic_segment.Segment {
    const groups = try alloc.alloc(algebraic_segment.GroupFold, folds.count());
    errdefer alloc.free(groups);
    var initialized: usize = 0;
    errdefer {
        for (groups[0..initialized]) |*group| group.deinit(alloc);
    }

    var it = folds.iterator();
    while (it.next()) |entry| {
        groups[initialized] = .{
            .key = try alloc.dupe(u8, entry.key_ptr.*),
            .value = entry.value_ptr.*,
        };
        initialized += 1;
    }
    std.mem.sort(algebraic_segment.GroupFold, groups, {}, lessGroupFold);

    var segment = algebraic_segment.Segment{
        .source = .{
            .kind = try algebraicSourceKind(binding.source_kind),
            .snapshot_id = try alloc.dupe(u8, binding.snapshot_id),
            .schema_fingerprint = try alloc.dupe(u8, binding.schema_fingerprint),
            .source_id = if (binding.source_id.len == 0) &.{} else try alloc.dupe(u8, binding.source_id),
        },
        .aggregate = .{
            .group_column = try alloc.dupe(u8, options.group_column),
            .value_column = if (options.value_column.len == 0) &.{} else try alloc.dupe(u8, options.value_column),
            .op = options.op,
            .groups = groups,
        },
    };
    errdefer segment.deinit(alloc);
    try segment.validate();
    return segment;
}

fn accumulatorsToExpressionMaterializationAlloc(
    alloc: Allocator,
    accumulators: []const ExpressionAccumulator,
    binding: source_binding.Binding,
    options: AlgebraicExpressionSidecarBuildOptions,
) !algebraic_segment.ExpressionMaterialization {
    const expressions = try alloc.alloc(algebraic_segment.ExpressionFold, options.expressions.len);
    errdefer alloc.free(expressions);
    var initialized: usize = 0;
    errdefer {
        for (expressions[0..initialized]) |*expression| expression.deinit(alloc);
    }

    for (options.expressions, accumulators, expressions) |spec, accumulator, *out| {
        switch (spec.op) {
            .count, .sum_i64 => {},
            .min_i64, .max_i64, .avg_i64 => if (!accumulator.seen_non_null) return error.EmptyAlgebraicExpressionFold,
        }

        const name = try alloc.dupe(u8, spec.name);
        errdefer alloc.free(name);
        var value_column: []u8 = &.{};
        if (spec.value_column.len != 0) value_column = try alloc.dupe(u8, spec.value_column);
        errdefer if (value_column.len != 0) alloc.free(value_column);

        out.* = .{
            .name = name,
            .value_column = value_column,
            .op = spec.op,
            .value = accumulator.value,
        };
        initialized += 1;
    }

    var materialization = algebraic_segment.ExpressionMaterialization{
        .source = .{
            .kind = try algebraicSourceKind(binding.source_kind),
            .snapshot_id = try alloc.dupe(u8, binding.snapshot_id),
            .schema_fingerprint = try alloc.dupe(u8, binding.schema_fingerprint),
            .source_id = if (binding.source_id.len == 0) &.{} else try alloc.dupe(u8, binding.source_id),
        },
        .expressions = expressions,
    };
    errdefer materialization.deinit(alloc);
    try materialization.validate();
    return materialization;
}

fn algebraicSourceKind(kind: rowsource.SourceKind) !algebraic_segment.SourceKind {
    return switch (kind) {
        .serverless_fragment => .serverless_fragment,
        .external_parquet => .external_parquet,
        .external_iceberg => .external_iceberg,
        .external_lance => .external_lance,
        .relational_store => .relational_store,
        .json_materialized => error.UnsupportedAlgebraicSourceKind,
    };
}

fn lessGroupFold(_: void, lhs: algebraic_segment.GroupFold, rhs: algebraic_segment.GroupFold) bool {
    return std.mem.lessThan(u8, lhs.key, rhs.key);
}

fn declaredArtifactAlloc(
    alloc: Allocator,
    binding: source_binding.Binding,
    options: AlgebraicGroupBySidecarBuildOptions,
    payload_len: usize,
) !sidecar_manifest.DeclaredArtifact {
    return declaredArtifactForNameAlloc(alloc, binding, options.name, options.artifact_id, payload_len, "lake-algebraic");
}

fn declaredArtifactForNameAlloc(
    alloc: Allocator,
    binding: source_binding.Binding,
    artifact_name_value: []const u8,
    explicit_artifact_id: []const u8,
    payload_len: usize,
    default_artifact_prefix: []const u8,
) !sidecar_manifest.DeclaredArtifact {
    const name = try alloc.dupe(u8, artifact_name_value);
    errdefer alloc.free(name);
    const artifact_name = try alloc.dupe(u8, artifact_name_value);
    errdefer alloc.free(artifact_name);
    const artifact_id = if (explicit_artifact_id.len == 0)
        try std.fmt.allocPrint(
            alloc,
            "{s}:{d}:{s}:{d}:{s}:{d}",
            .{ default_artifact_prefix, artifact_name_value.len, artifact_name_value, binding.snapshot_id.len, binding.snapshot_id, payload_len },
        )
    else
        try alloc.dupe(u8, explicit_artifact_id);
    errdefer alloc.free(artifact_id);
    const checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{payload_len});
    errdefer alloc.free(checksum);
    const owned_binding = try cloneBindingAlloc(alloc, binding);
    errdefer freeOwnedBinding(alloc, owned_binding);

    return .{
        .name = name,
        .binding = owned_binding,
        .artifact = artifact_ref.ArtifactRef{
            .kind = .algebraic_segment,
            .name = artifact_name,
            .artifact_id = artifact_id,
            .byte_len = @intCast(payload_len),
            .checksum = checksum,
        },
    };
}

fn cloneBindingAlloc(alloc: Allocator, binding: source_binding.Binding) !source_binding.Binding {
    return try source_binding.cloneAlloc(alloc, binding);
}

fn freeOwnedDeclaration(alloc: Allocator, declaration: sidecar_manifest.DeclaredArtifact) void {
    alloc.free(declaration.name);
    freeOwnedBinding(alloc, declaration.binding);
    if (declaration.artifact.name.len > 0) alloc.free(declaration.artifact.name);
    alloc.free(declaration.artifact.artifact_id);
    alloc.free(declaration.artifact.checksum);
}

fn freeOwnedBinding(alloc: Allocator, binding: source_binding.Binding) void {
    source_binding.freeOwned(alloc, binding);
}

const MemoryArtifactStore = struct {
    alloc: Allocator,
    bytes: ?[]u8 = null,

    fn init(alloc: Allocator) MemoryArtifactStore {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *MemoryArtifactStore) void {
        if (self.bytes) |bytes| self.alloc.free(bytes);
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
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = try self.alloc.dupe(u8, contents);
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:algebraic-sidecar"),
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        if (!std.mem.eql(u8, artifact_id, "mem:algebraic-sidecar")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return try alloc.dupe(u8, bytes);
    }

    fn getRangeAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
        const bytes = try self.getAlloc(alloc, artifact_id);
        defer alloc.free(bytes);
        if (offset > bytes.len) return error.InvalidRange;
        const start: usize = @intCast(offset);
        const end = @min(bytes.len, start + len);
        return try alloc.dupe(u8, bytes[start..end]);
    }

    fn stat(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        if (!std.mem.eql(u8, artifact_id, "mem:algebraic-sidecar")) return error.ArtifactNotFound;
        const bytes = self.bytes orelse return error.ArtifactNotFound;
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:algebraic-sidecar"),
            .byte_len = @intCast(bytes.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{bytes.len}),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        if (!std.mem.eql(u8, artifact_id, "mem:algebraic-sidecar")) return error.ArtifactNotFound;
        if (self.bytes) |bytes| self.alloc.free(bytes);
        self.bytes = null;
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

test "lake algebraic group-by sidecar builder folds external row source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "orders",
        .source_uri = "s3://bucket/warehouse/orders",
        .snapshot_id = "iceberg-234",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs_a = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const tenants_a = [_][]const u8{ "t1", "t2" };
    const amounts_a = [_]i64{ 7, 11 };
    const columns_a = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants_a } },
        .{ .name = "amount", .values = .{ .i64 = &amounts_a } },
    };
    const row_refs_b = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-b.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-b.parquet", 0, 1),
    };
    const tenants_b = [_][]const u8{ "t1", "t3" };
    const amounts_b = [_]i64{ 13, 17 };
    const columns_b = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants_b } },
        .{ .name = "amount", .values = .{ .i64 = &amounts_b } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{ .snapshot = external_binding.snapshot(), .row_refs = &row_refs_a, .columns = &columns_a },
        .{ .snapshot = external_binding.snapshot(), .row_refs = &row_refs_b, .columns = &columns_b },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .algebraic,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{ "tenant", "amount" },
        "sha256:agg:v1",
    );

    var result = try buildAlgebraicGroupBySidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
        .name = "orders.amount_by_tenant",
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
    });
    defer result.deinit(alloc);

    try result.declaration.validate();
    try sidecar_manifest.validateBatchAgainstDeclaredArtifact(result.declaration, batches[0]);

    var segment = try algebraic_segment.decodeAlloc(alloc, result.payload);
    defer algebraic_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(algebraic_segment.SourceKind.external_iceberg, segment.source.kind);
    try std.testing.expectEqualStrings("orders", segment.source.source_id);
    try std.testing.expectEqual(@as(usize, 3), segment.aggregate.groups.len);
    try std.testing.expectEqualStrings("t1", segment.aggregate.groups[0].key);
    try std.testing.expectEqual(@as(i64, 20), segment.aggregate.groups[0].value.sum_i64);
    try std.testing.expectEqualStrings("t2", segment.aggregate.groups[1].key);
    try std.testing.expectEqual(@as(i64, 11), segment.aggregate.groups[1].value.sum_i64);
    try std.testing.expectEqualStrings("t3", segment.aggregate.groups[2].key);
    try std.testing.expectEqual(@as(i64, 17), segment.aggregate.groups[2].value.sum_i64);
}

test "lake algebraic group-by sidecar publisher writes artifact store metadata into declaration" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "orders",
        .source_uri = "s3://bucket/warehouse/orders",
        .snapshot_id = "iceberg-235",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const tenants = [_][]const u8{ "t1", "t1" };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{ .snapshot = external_binding.snapshot(), .row_refs = &row_refs, .columns = &columns },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .algebraic,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"tenant"},
        "sha256:count:v1",
    );

    var result = try publishAlgebraicGroupBySidecarFromRowSourceAlloc(
        alloc,
        &artifacts,
        batch_source.rowSource(),
        binding,
        .{
            .name = "orders.count_by_tenant",
            .group_column = "tenant",
            .op = .count,
        },
    );
    defer result.deinit(alloc);

    try result.declaration.validate();
    try std.testing.expectEqualStrings("mem:algebraic-sidecar", result.declaration.artifact.artifact_id);

    const stored = try artifacts.getAlloc(result.declaration.artifact.artifact_id);
    defer alloc.free(stored);
    var segment = try algebraic_segment.decodeAlloc(alloc, stored);
    defer algebraic_segment.freeSegment(alloc, &segment);
    try std.testing.expectEqual(@as(usize, 1), segment.aggregate.groups.len);
    try std.testing.expectEqual(@as(u64, 2), segment.aggregate.groups[0].value.count);
}

test "lake algebraic group-by sidecar builder rejects stale source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "orders",
        .source_uri = "s3://bucket/warehouse/orders",
        .snapshot_id = "iceberg-236",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const tenants = [_][]const u8{"t1"};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{ .snapshot = external_binding.snapshot(), .row_refs = &row_refs, .columns = &columns },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const stale_snapshot = rowsource.SnapshotRef{ .table_id = "orders", .snapshot_id = "iceberg-235" };
    const binding = source_binding.bindingFromSnapshot(
        .algebraic,
        .external_iceberg,
        stale_snapshot,
        external_binding.schema_fingerprint,
        &[_][]const u8{"tenant"},
        "sha256:count:v1",
    );

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        buildAlgebraicGroupBySidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
            .name = "orders.count_by_tenant",
            .group_column = "tenant",
            .op = .count,
        }),
    );
}

test "lake algebraic expression sidecar builder folds external row source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "orders",
        .source_uri = "s3://bucket/warehouse/orders",
        .snapshot_id = "iceberg-237",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs_a = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const amounts_a = [_]i64{ 7, 11 };
    const columns_a = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts_a } },
    };
    const row_refs_b = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-b.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-b.parquet", 0, 1),
    };
    const amounts_b = [_]i64{ 13, 0 };
    const amount_nulls_b = [_]u8{ 0, 1 };
    const columns_b = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts_b }, .nulls = .{ .bytes = &amount_nulls_b } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{ .snapshot = external_binding.snapshot(), .row_refs = &row_refs_a, .columns = &columns_a },
        .{ .snapshot = external_binding.snapshot(), .row_refs = &row_refs_b, .columns = &columns_b },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .algebraic,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"amount"},
        "sha256:expr:v1",
    );
    const expressions = [_]algebraic_segment.ExpressionSpec{
        .{ .name = "row_count", .op = .count },
        .{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 },
        .{ .name = "amount_min", .value_column = "amount", .op = .min_i64 },
        .{ .name = "amount_max", .value_column = "amount", .op = .max_i64 },
        .{ .name = "amount_avg", .value_column = "amount", .op = .avg_i64 },
    };

    var result = try buildAlgebraicExpressionSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
        .name = "orders.amount_expression_folds",
        .expressions = &expressions,
    });
    defer result.deinit(alloc);

    try result.declaration.validate();
    try sidecar_manifest.validateBatchAgainstDeclaredArtifact(result.declaration, batches[0]);

    var materialization = try algebraic_segment.decodeExpressionAlloc(alloc, result.payload);
    defer algebraic_segment.freeExpressionMaterialization(alloc, &materialization);
    try std.testing.expectEqual(algebraic_segment.SourceKind.external_iceberg, materialization.source.kind);
    try std.testing.expectEqualStrings("orders", materialization.source.source_id);
    try std.testing.expectEqual(@as(usize, 5), materialization.expressions.len);
    try std.testing.expectEqual(@as(u64, 4), materialization.expressions[0].value.count);
    try std.testing.expectEqual(@as(i64, 31), materialization.expressions[1].value.sum_i64);
    try std.testing.expectEqual(@as(i64, 7), materialization.expressions[2].value.min_i64);
    try std.testing.expectEqual(@as(i64, 13), materialization.expressions[3].value.max_i64);
    try std.testing.expectEqual(@as(i64, 31), materialization.expressions[4].value.avg_i64.sum_i64);
    try std.testing.expectEqual(@as(u64, 3), materialization.expressions[4].value.avg_i64.count);
}

test "lake algebraic expression sidecar publisher writes artifact store metadata into declaration" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "orders",
        .source_uri = "s3://bucket/warehouse/orders",
        .snapshot_id = "iceberg-238",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 1),
    };
    const amounts = [_]i64{ 3, 5 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{ .snapshot = external_binding.snapshot(), .row_refs = &row_refs, .columns = &columns },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const binding = source_binding.bindingFromSnapshot(
        .algebraic,
        .external_iceberg,
        external_binding.snapshot(),
        external_binding.schema_fingerprint,
        &[_][]const u8{"amount"},
        "sha256:expr:v1",
    );
    const expressions = [_]algebraic_segment.ExpressionSpec{
        .{ .name = "row_count", .op = .count },
        .{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 },
    };

    var result = try publishAlgebraicExpressionSidecarFromRowSourceAlloc(
        alloc,
        &artifacts,
        batch_source.rowSource(),
        binding,
        .{
            .name = "orders.amount_expression_folds",
            .expressions = &expressions,
        },
    );
    defer result.deinit(alloc);

    try result.declaration.validate();
    try std.testing.expectEqualStrings("mem:algebraic-sidecar", result.declaration.artifact.artifact_id);

    const stored = try artifacts.getAlloc(result.declaration.artifact.artifact_id);
    defer alloc.free(stored);
    var materialization = try algebraic_segment.decodeExpressionAlloc(alloc, stored);
    defer algebraic_segment.freeExpressionMaterialization(alloc, &materialization);
    try std.testing.expectEqual(@as(usize, 2), materialization.expressions.len);
    try std.testing.expectEqual(@as(u64, 2), materialization.expressions[0].value.count);
    try std.testing.expectEqual(@as(i64, 8), materialization.expressions[1].value.sum_i64);
}

test "lake algebraic expression sidecar builder rejects stale source batches" {
    const alloc = std.testing.allocator;
    const external_binding = external_rowsource.Binding{
        .format = .iceberg,
        .source_id = "orders",
        .source_uri = "s3://bucket/warehouse/orders",
        .snapshot_id = "iceberg-239",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try external_rowsource.makeRowRef(external_binding, "file-a.parquet", 0, 0),
    };
    const amounts = [_]i64{7};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{ .snapshot = external_binding.snapshot(), .row_refs = &row_refs, .columns = &columns },
    };

    var batch_source = try external_rowsource.BatchSource.init(external_binding, &batches);
    const stale_snapshot = rowsource.SnapshotRef{ .table_id = "orders", .snapshot_id = "iceberg-238" };
    const binding = source_binding.bindingFromSnapshot(
        .algebraic,
        .external_iceberg,
        stale_snapshot,
        external_binding.schema_fingerprint,
        &[_][]const u8{"amount"},
        "sha256:expr:v1",
    );
    const expressions = [_]algebraic_segment.ExpressionSpec{
        .{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 },
    };

    try std.testing.expectError(
        error.SidecarSourceBindingMismatch,
        buildAlgebraicExpressionSidecarFromRowSourceAlloc(alloc, batch_source.rowSource(), binding, .{
            .name = "orders.amount_expression_folds",
            .expressions = &expressions,
        }),
    );
}
