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

//! Publication bridge for algebraic materialization artifacts.

const std = @import("std");
const Allocator = std.mem.Allocator;
const algebraic_manifest = @import("algebraic_manifest.zig");
const algebraic_segment = @import("../algebraic_segment/mod.zig");
const artifact_store = @import("../artifacts/store.zig");
const rowsource = @import("../../storage/rowsource/types.zig");

pub const PublishOptions = struct {
    source_kind: algebraic_segment.SourceKind,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    source_id: []const u8 = &.{},
    group_column: []const u8,
    value_column: []const u8 = &.{},
    op: algebraic_segment.AggregateOp,
    artifact_name: []const u8 = &.{},
};

pub const ExpressionPublishOptions = struct {
    source_kind: algebraic_segment.SourceKind,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,
    source_id: []const u8 = &.{},
    expressions: []const algebraic_segment.ExpressionSpec,
    artifact_name: []const u8 = &.{},
};

pub const PublishResult = struct {
    plan: algebraic_manifest.Plan,

    pub fn deinit(self: *PublishResult, alloc: Allocator) void {
        self.plan.deinit(alloc);
        self.* = undefined;
    }
};

pub fn publishGroupByAggregateAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    batch: rowsource.ColumnBatch,
    options: PublishOptions,
) !PublishResult {
    var segment = try algebraic_segment.buildGroupByAggregateAlloc(alloc, batch, .{
        .source_kind = options.source_kind,
        .snapshot_id = options.snapshot_id,
        .schema_fingerprint = options.schema_fingerprint,
        .source_id = options.source_id,
        .group_column = options.group_column,
        .value_column = options.value_column,
        .op = options.op,
    });
    defer segment.deinit(alloc);

    const encoded = try algebraic_segment.encodeAlloc(alloc, segment);
    defer alloc.free(encoded);

    var metadata = try artifacts.put(encoded);
    defer metadata.deinit(alloc);

    const published = [_]algebraic_manifest.PublishedArtifact{
        .{
            .artifact_id = metadata.artifact_id,
            .byte_len = metadata.byte_len,
            .checksum = metadata.checksum,
            .name = options.artifact_name,
        },
    };

    return .{
        .plan = try algebraic_manifest.planAlloc(alloc, &published),
    };
}

pub fn publishExpressionFoldsAlloc(
    alloc: Allocator,
    artifacts: *artifact_store.ArtifactStore,
    batch: rowsource.ColumnBatch,
    options: ExpressionPublishOptions,
) !PublishResult {
    var materialization = try algebraic_segment.buildExpressionFoldsAlloc(alloc, batch, .{
        .source_kind = options.source_kind,
        .snapshot_id = options.snapshot_id,
        .schema_fingerprint = options.schema_fingerprint,
        .source_id = options.source_id,
        .expressions = options.expressions,
    });
    defer materialization.deinit(alloc);

    const encoded = try algebraic_segment.encodeExpressionAlloc(alloc, materialization);
    defer alloc.free(encoded);

    var metadata = try artifacts.put(encoded);
    defer metadata.deinit(alloc);

    const published = [_]algebraic_manifest.PublishedArtifact{
        .{
            .artifact_id = metadata.artifact_id,
            .byte_len = metadata.byte_len,
            .checksum = metadata.checksum,
            .name = options.artifact_name,
        },
    };

    return .{
        .plan = try algebraic_manifest.planAlloc(alloc, &published),
    };
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
            .artifact_id = try alloc.dupe(u8, "mem:folds"),
            .byte_len = @intCast(contents.len),
            .checksum = try std.fmt.allocPrint(alloc, "len:{d}", .{contents.len}),
        };
    }

    fn getAlloc(self: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) ![]u8 {
        if (!std.mem.eql(u8, artifact_id, "mem:folds")) return error.ArtifactNotFound;
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

    fn stat(_: *MemoryArtifactStore, alloc: Allocator, artifact_id: []const u8) !artifact_store.ArtifactMetadata {
        if (!std.mem.eql(u8, artifact_id, "mem:folds")) return error.ArtifactNotFound;
        return .{
            .artifact_id = try alloc.dupe(u8, "mem:folds"),
            .byte_len = 0,
            .checksum = try alloc.dupe(u8, "len:0"),
        };
    }

    fn delete(self: *MemoryArtifactStore, artifact_id: []const u8) !void {
        if (!std.mem.eql(u8, artifact_id, "mem:folds")) return error.ArtifactNotFound;
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

test "algebraic publisher writes fold artifact and returns manifest plan" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const row_refs = [_]rowsource.RowRef{
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 0 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 1 } },
    };
    const tenants = [_][]const u8{ "t1", "t1" };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-7" },
        .row_refs = &row_refs,
        .columns = &columns,
    };

    var result = try publishGroupByAggregateAlloc(alloc, &artifacts, batch, .{
        .source_kind = .serverless_fragment,
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .source_id = "orders",
        .group_column = "tenant",
        .value_column = "amount",
        .op = .sum_i64,
        .artifact_name = "orders.amount_by_tenant",
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.plan.artifacts.len);
    try std.testing.expectEqualStrings("mem:folds", result.plan.artifacts[0].artifact_id);
    const encoded = try artifacts.getAlloc("mem:folds");
    defer alloc.free(encoded);
    try std.testing.expect(encoded.len > 0);
}

test "algebraic publisher writes serverless expression fold artifact and returns manifest plan" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const row_refs = [_]rowsource.RowRef{
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 0 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 1 } },
        .{ .serverless = .{ .fragment_id = "frag-1", .row_ordinal = 2 } },
    };
    const amounts = [_]i64{ 10, 20, 30 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "manifest-7" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const expressions = [_]algebraic_segment.ExpressionSpec{
        .{ .name = "row_count", .op = .count },
        .{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 },
    };

    var result = try publishExpressionFoldsAlloc(alloc, &artifacts, batch, .{
        .source_kind = .serverless_fragment,
        .snapshot_id = "manifest-7",
        .schema_fingerprint = "schema-v1",
        .source_id = "orders",
        .expressions = &expressions,
        .artifact_name = "orders.expression_folds",
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.plan.artifacts.len);
    try std.testing.expectEqualStrings("mem:folds", result.plan.artifacts[0].artifact_id);
    try std.testing.expectEqualStrings("orders.expression_folds", result.plan.artifacts[0].name);

    const encoded = try artifacts.getAlloc("mem:folds");
    defer alloc.free(encoded);

    var reader = try algebraic_segment.ExpressionReader.decodeAlloc(alloc, encoded);
    defer reader.deinit();
    try std.testing.expectEqual(@as(usize, 2), reader.expressionCount());
    try std.testing.expectEqual(@as(u64, 3), reader.find("row_count").?.count);
    try std.testing.expectEqual(@as(i64, 60), reader.find("amount_sum").?.sum_i64);
}

test "algebraic publisher writes expression fold artifact and returns manifest plan" {
    const alloc = std.testing.allocator;
    var memory = MemoryArtifactStore.init(alloc);
    var artifacts = memory.artifactStore();
    defer artifacts.deinit();

    const row_refs = [_]rowsource.RowRef{
        .{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-1", .file_id = "file-a", .row_group_ordinal = 0, .row_ordinal = 0 } },
        .{ .external = .{ .source_id = "events", .snapshot_id = "iceberg-1", .file_id = "file-a", .row_group_ordinal = 0, .row_ordinal = 1 } },
    };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batch = rowsource.ColumnBatch{
        .snapshot = .{ .table_id = "events", .snapshot_id = "iceberg-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    };
    const expressions = [_]algebraic_segment.ExpressionSpec{
        .{ .name = "row_count", .op = .count },
        .{ .name = "amount_sum", .value_column = "amount", .op = .sum_i64 },
    };

    var result = try publishExpressionFoldsAlloc(alloc, &artifacts, batch, .{
        .source_kind = .external_iceberg,
        .snapshot_id = "iceberg-1",
        .schema_fingerprint = "schema-v1",
        .source_id = "events",
        .expressions = &expressions,
        .artifact_name = "events.expression_folds",
    });
    defer result.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), result.plan.artifacts.len);
    try std.testing.expectEqualStrings("events.expression_folds", result.plan.artifacts[0].name);
    const encoded = try artifacts.getAlloc("mem:folds");
    defer alloc.free(encoded);
    var reader = try algebraic_segment.ExpressionReader.decodeAlloc(alloc, encoded);
    defer reader.deinit();
    try std.testing.expectEqual(@as(u64, 2), reader.find("row_count").?.count);
    try std.testing.expectEqual(@as(i64, 30), reader.find("amount_sum").?.sum_i64);
}
