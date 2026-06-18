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
const artifact_ref = @import("../manifest/artifact_ref.zig");
const artifact_store = @import("../artifacts/store.zig");
const sidecar_manifest = @import("../segment/sidecar_manifest.zig");
const source_binding = @import("../segment/source_binding.zig");
const external_rowsource = @import("../../storage/rowsource/external.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const AlgebraicGroupBySidecarBuildOptions = struct {
    name: []const u8,
    group_column: []const u8,
    value_column: []const u8 = &.{},
    op: algebraic_segment.AggregateOp,
    artifact_id: []const u8 = &.{},
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

pub fn buildAlgebraicGroupBySidecarFromRowSourceAlloc(
    alloc: Allocator,
    source: rowsource.Source,
    binding: source_binding.Binding,
    options: AlgebraicGroupBySidecarBuildOptions,
) !AlgebraicGroupBySidecarBuildResult {
    try validateGroupByOptions(binding, source.kind, options);

    var folds = std.StringHashMapUnmanaged(algebraic_segment.AggregateValue).empty;
    defer {
        var key_it = folds.keyIterator();
        while (key_it.next()) |key| alloc.free(key.*);
        folds.deinit(alloc);
    }

    while (try source.next(alloc)) |batch| {
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
    }

    if (folds.count() == 0) return error.EmptyLakeSidecarAlgebraicSegment;

    var segment = try groupMapToSegmentAlloc(alloc, &folds, binding, options);
    defer algebraic_segment.freeSegment(alloc, &segment);

    const payload = try algebraic_segment.encodeAlloc(alloc, segment);
    errdefer alloc.free(payload);

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
            entry.value_ptr.* = try combine(entry.value_ptr.*, next_value);
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
        .sum_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .sum_i64 = value_column.?.values.i64[row_idx] },
        .min_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .min_i64 = value_column.?.values.i64[row_idx] },
        .max_i64 => if (value_column.?.nulls.isNull(row_idx)) null else .{ .max_i64 = value_column.?.values.i64[row_idx] },
    };
}

fn combine(
    lhs: algebraic_segment.AggregateValue,
    rhs: algebraic_segment.AggregateValue,
) !algebraic_segment.AggregateValue {
    return switch (lhs) {
        .count => |left| .{ .count = left + rhs.count },
        .sum_i64 => |left| .{ .sum_i64 = left + rhs.sum_i64 },
        .min_i64 => |left| .{ .min_i64 = @min(left, rhs.min_i64) },
        .max_i64 => |left| .{ .max_i64 = @max(left, rhs.max_i64) },
    };
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
    const name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(name);
    const artifact_name = try alloc.dupe(u8, options.name);
    errdefer alloc.free(artifact_name);
    const artifact_id = if (options.artifact_id.len == 0)
        try std.fmt.allocPrint(
            alloc,
            "lake-algebraic:{d}:{s}:{d}:{s}:{d}",
            .{ options.name.len, options.name, binding.snapshot_id.len, binding.snapshot_id, payload_len },
        )
    else
        try alloc.dupe(u8, options.artifact_id);
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

fn freeOwnedDeclaration(alloc: Allocator, declaration: sidecar_manifest.DeclaredArtifact) void {
    alloc.free(declaration.name);
    freeOwnedBinding(alloc, declaration.binding);
    if (declaration.artifact.name.len > 0) alloc.free(declaration.artifact.name);
    alloc.free(declaration.artifact.artifact_id);
    alloc.free(declaration.artifact.checksum);
}

fn freeOwnedBinding(alloc: Allocator, binding: source_binding.Binding) void {
    alloc.free(binding.source_id);
    alloc.free(binding.snapshot_id);
    alloc.free(binding.schema_fingerprint);
    for (binding.column_bindings) |column| alloc.free(column);
    alloc.free(binding.column_bindings);
    alloc.free(binding.index_config_hash);
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
