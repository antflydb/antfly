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

//! Local RowSource scaffolding for Antfly relational rows and materialized JSON
//! rows. Real executors can replace the batch list with cursor-backed scanners
//! while keeping the same Source contract.

const std = @import("std");
const Allocator = std.mem.Allocator;
const rowsource = @import("types.zig");

pub const BatchSource = struct {
    kind: rowsource.SourceKind,
    batches: []const rowsource.ColumnBatch,
    next_index: usize = 0,

    pub fn init(kind: rowsource.SourceKind, batches: []const rowsource.ColumnBatch) !BatchSource {
        if (kind != .relational_store and kind != .json_materialized) return error.InvalidLocalRowSource;
        for (batches) |batch| try validateLocalBatch(kind, batch);
        return .{
            .kind = kind,
            .batches = batches,
        };
    }

    pub fn rowSource(self: *BatchSource) rowsource.Source {
        return .{
            .kind = self.kind,
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
        try validateLocalBatch(self.kind, batch);
        return batch;
    }
};

pub fn relationalStoreSource(batches: []const rowsource.ColumnBatch) !BatchSource {
    return BatchSource.init(.relational_store, batches);
}

pub fn jsonMaterializedSource(batches: []const rowsource.ColumnBatch) !BatchSource {
    return BatchSource.init(.json_materialized, batches);
}

pub fn validateLocalBatch(kind: rowsource.SourceKind, batch: rowsource.ColumnBatch) !void {
    if (kind != .relational_store and kind != .json_materialized) return error.InvalidLocalRowSource;
    try batch.validate();
    for (batch.row_refs) |row_ref| {
        switch (row_ref) {
            .relational_key => |key| {
                if (key.len == 0) return error.InvalidLocalRowSource;
            },
            else => return error.InvalidLocalRowSource,
        }
    }
}

test "relational store source emits relational-key batches" {
    const row_refs = [_]rowsource.RowRef{
        .{ .relational_key = "row:a" },
        .{ .relational_key = "row:b" },
    };
    const amounts = [_]i64{ 10, 20 };
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "amount", .values = .{ .i64 = &amounts } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "orders", .snapshot_id = "lsm-7" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};

    var source = try relationalStoreSource(&batches);
    const row_source = source.rowSource();
    try std.testing.expectEqual(rowsource.SourceKind.relational_store, row_source.kind);
    const batch = (try row_source.next(std.testing.allocator)).?;
    try std.testing.expectEqual(@as(usize, 2), batch.rowCount());
    try std.testing.expectEqual(@as(i64, 20), batch.columns[0].values.i64[1]);
    try std.testing.expect((try row_source.next(std.testing.allocator)) == null);
}

test "json materialized source accepts json columns over relational keys" {
    const row_refs = [_]rowsource.RowRef{.{ .relational_key = "cte:0" }};
    const docs = [_][]const u8{"{\"ok\":true}"};
    const columns = [_]rowsource.ColumnVector{
        .{ .name = "doc", .values = .{ .json = &docs } },
    };
    const batches = [_]rowsource.ColumnBatch{.{
        .snapshot = .{ .table_id = "cte", .snapshot_id = "query-1" },
        .row_refs = &row_refs,
        .columns = &columns,
    }};

    var source = try jsonMaterializedSource(&batches);
    const batch = (try source.rowSource().next(std.testing.allocator)).?;
    try std.testing.expectEqual(rowsource.ColumnKind.json, batch.columns[0].kind());
}
