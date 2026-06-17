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

//! External lake RowSource scaffolding for Parquet, Iceberg, and Lance-backed
//! tables. Concrete file readers own their decoded vectors; this adapter pins
//! source metadata and exposes validated external row batches to row plans.

const std = @import("std");
const Allocator = std.mem.Allocator;
const rowsource = @import("types.zig");

pub const Format = enum(u8) {
    parquet = 1,
    iceberg = 2,
    lance = 3,

    pub fn sourceKind(self: Format) rowsource.SourceKind {
        return switch (self) {
            .parquet => .external_parquet,
            .iceberg => .external_iceberg,
            .lance => .external_lance,
        };
    }
};

pub const Binding = struct {
    format: Format,
    source_id: []const u8,
    source_uri: []const u8,
    snapshot_id: []const u8,
    schema_fingerprint: []const u8,

    pub fn validate(self: Binding) !void {
        if (self.source_id.len == 0) return error.InvalidExternalRowSource;
        if (self.source_uri.len == 0) return error.InvalidExternalRowSource;
        if (self.snapshot_id.len == 0) return error.InvalidExternalRowSource;
        if (self.schema_fingerprint.len == 0) return error.InvalidExternalRowSource;
    }

    pub fn snapshot(self: Binding) rowsource.SnapshotRef {
        return .{
            .table_id = self.source_id,
            .snapshot_id = self.snapshot_id,
        };
    }
};

pub const FileRef = struct {
    file_id: []const u8,
    object_uri: []const u8,
    row_group_ordinal: u32,
    row_count: u64,

    pub fn validate(self: FileRef) !void {
        if (self.file_id.len == 0) return error.InvalidExternalRowSource;
        if (self.object_uri.len == 0) return error.InvalidExternalRowSource;
    }
};

pub const BatchSource = struct {
    binding: Binding,
    batches: []const rowsource.ColumnBatch,
    next_index: usize = 0,

    pub fn init(binding: Binding, batches: []const rowsource.ColumnBatch) !BatchSource {
        try binding.validate();
        for (batches) |batch| try validateExternalBatch(binding, batch);
        return .{
            .binding = binding,
            .batches = batches,
        };
    }

    pub fn rowSource(self: *BatchSource) rowsource.Source {
        return .{
            .kind = self.binding.format.sourceKind(),
            .ctx = self,
            .next_batch = nextBatch,
        };
    }

    fn nextBatch(ctx: *anyopaque, alloc: Allocator) !?rowsource.ColumnBatch {
        _ = alloc;
        const self: *BatchSource = @ptrCast(@alignCast(ctx));
        if (self.next_index >= self.batches.len) return null;
        const batch = self.batches[self.next_index];
        self.next_index += 1;
        try validateExternalBatch(self.binding, batch);
        return batch;
    }
};

pub fn validateExternalBatch(binding: Binding, batch: rowsource.ColumnBatch) !void {
    try binding.validate();
    try batch.validate();
    if (!std.mem.eql(u8, batch.snapshot.table_id, binding.source_id)) return error.InvalidExternalRowSource;
    if (!std.mem.eql(u8, batch.snapshot.snapshot_id, binding.snapshot_id)) return error.InvalidExternalRowSource;
    for (batch.row_refs) |row_ref| {
        switch (row_ref) {
            .external => |external| {
                if (!std.mem.eql(u8, external.source_id, binding.source_id)) return error.InvalidExternalRowSource;
                if (!std.mem.eql(u8, external.snapshot_id, binding.snapshot_id)) return error.InvalidExternalRowSource;
                if (external.file_id.len == 0) return error.InvalidExternalRowSource;
            },
            else => return error.InvalidExternalRowSource,
        }
    }
}

pub fn makeRowRef(
    binding: Binding,
    file_id: []const u8,
    row_group_ordinal: u32,
    row_ordinal: u64,
) !rowsource.RowRef {
    try binding.validate();
    if (file_id.len == 0) return error.InvalidExternalRowSource;
    return .{ .external = .{
        .source_id = binding.source_id,
        .snapshot_id = binding.snapshot_id,
        .file_id = file_id,
        .row_group_ordinal = row_group_ordinal,
        .row_ordinal = row_ordinal,
    } };
}

test "external batch source emits validated iceberg batches" {
    const binding = Binding{
        .format = .iceberg,
        .source_id = "events",
        .source_uri = "s3://bucket/warehouse/events",
        .snapshot_id = "iceberg-123",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{
        try makeRowRef(binding, "file-a.parquet", 0, 0),
        try makeRowRef(binding, "file-a.parquet", 0, 1),
    };
    const tenants = [_][]const u8{ "t1", "t2" };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "tenant", .values = .{ .bytes = &tenants } },
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{
        .{
            .snapshot = binding.snapshot(),
            .row_refs = &row_refs,
            .columns = &columns,
        },
    };

    var source = try BatchSource.init(binding, &batches);
    const row_source = source.rowSource();
    try std.testing.expectEqual(rowsource.SourceKind.external_iceberg, row_source.kind);
    const batch = (try row_source.next(std.testing.allocator)).?;
    try std.testing.expectEqual(@as(usize, 2), batch.rowCount());
    try std.testing.expectEqual(@as(i64, 20), batch.columns[1].values.i64[1]);
    try std.testing.expect((try row_source.next(std.testing.allocator)) == null);
}

test "external batch source rejects non-external row refs" {
    const binding = Binding{
        .format = .parquet,
        .source_id = "events",
        .source_uri = "s3://bucket/events",
        .snapshot_id = "digest-1",
        .schema_fingerprint = "schema-v1",
    };
    const row_refs = [_]rowsource.RowRef{.{ .relational_key = "row:a" }};
    const values = [_]i64{1};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &values } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = binding.snapshot(),
        .row_refs = &row_refs,
        .columns = &columns,
    }};

    try std.testing.expectError(error.InvalidExternalRowSource, BatchSource.init(binding, &batches));
}
