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

//! Bounded SQL pages over one pinned external snapshot. Conditions are evaluated
//! before page limits, so short filtered pages never masquerade as exhaustion.
const std = @import("std");
const catalog = @import("../sql/catalog.zig");
const scalar = @import("../sql/scalar.zig");
const rows = @import("../serverless/query/lake_rows.zig");
const serving = @import("../serverless/query/lake_serving.zig");
const operation = @import("operation.zig");
const Allocator = std.mem.Allocator;

pub fn open(alloc: Allocator, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions) !catalog.Cursor {
    const source = try alloc.create(serving.ServingSource);
    errdefer alloc.destroy(source);
    const schema: @import("../storage/schema.zig").TableSchema = .{ .storage_mode = .relational, .external_base_source = table.external_base_source };
    const normalized = try context.platformDeadline();
    source.* = try serving.ServingSource.openWithContext(alloc, schema, options, .{ .deadline_ns = normalized.deadline_ns, .cancellation = @import("../storage/object_storage.zig").CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) });
    errdefer source.deinit();
    const cursor = try openPinned(alloc, table, request, context, source);
    const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
    owner.source = source;
    return cursor;
}

pub fn openPinned(alloc: Allocator, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, source: *serving.ServingSource) !catalog.Cursor {
    if (request.include_primary_digest or request.include_document or request.index_equality != null) return error.UnsupportedSqlExecution;
    try context.ensureActive();
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    // Predicate columns must be available even when omitted from projection.
    var columns: std.ArrayList([]const u8) = .empty;
    for (request.fields) |field| try appendColumn(owned, &columns, table, field);
    for (request.conditions) |condition| try appendColumn(owned, &columns, table, condition.column);
    // COUNT(*) and identity-only scans still need one physical column to count
    // rows. The engine does not infer row counts from possibly stale metadata.
    if (columns.items.len == 0) {
        if (table.columns.len == 0) return error.InvalidSqlBackendResponse;
        try columns.append(owned, table.columns[0].path);
    }
    const schema: @import("../storage/schema.zig").TableSchema = .{ .storage_mode = .relational, .external_base_source = table.external_base_source };
    var result = source.scanner.scanAlloc(owned, schema, .{
        .projected_columns = columns.items,
        .limits = .{ .max_materialized_bytes = 32 * 1024 * 1024, .max_materialized_rows = 100_000, .max_rows_examined = 1_000_000 },
    }) catch |err| switch (err) {
        error.PreconditionFailed, error.VersionMismatch, error.ObjectNotFound => return error.ExternalLakeSnapshotMismatch,
        else => return err,
    };
    defer result.deinit(owned);
    try context.ensureActive();
    const output = try owned.alloc(catalog.Row, result.rows.len);
    for (result.rows, output) |row, *out| {
        try context.ensureActive();
        out.* = try projectedRow(owned, table, row);
    }
    const conditions = try owned.alloc(catalog.Condition, request.conditions.len);
    for (request.conditions, conditions) |condition, *out| {
        out.* = condition;
        out.column = try owned.dupe(u8, condition.column);
        const bytes = try std.json.Stringify.valueAlloc(owned, condition.value, .{});
        out.value = try std.json.parseFromSliceLeaky(std.json.Value, owned, bytes, .{ .allocate = .alloc_always, .parse_numbers = false });
    }
    std.mem.sort(catalog.Row, output, {}, struct {
        fn less(_: void, left: catalog.Row, right: catalog.Row) bool {
            return std.mem.order(u8, left.id, right.id) == .lt;
        }
    }.less);
    const owner = try alloc.create(Owner);
    errdefer alloc.destroy(owner);
    owner.* = .{ .alloc = alloc, .arena = arena, .rows = output, .context = context, .conditions = conditions, .after = if (request.after) |v| try owned.dupe(u8, v) else null, .primary_key = if (request.primary_key) |v| try owned.dupe(u8, v) else null };
    return .{ .ptr = owner, .next = Owner.next, .close = Owner.close };
}

fn appendColumn(alloc: Allocator, columns: *std.ArrayList([]const u8), table: catalog.Table, name: []const u8) !void {
    if (std.mem.eql(u8, name, "_id")) return;
    const column = try table.column(name);
    for (columns.items) |existing| if (std.mem.eql(u8, existing, column.path)) return;
    try columns.append(alloc, column.path);
}

pub fn projectedRow(alloc: Allocator, table: catalog.Table, row: rows.ProjectedRow) !catalog.Row {
    var object = std.json.ObjectMap.empty;
    var nulls: std.ArrayList(bool) = .empty;
    for (table.columns) |column| {
        const cell = row.find(column.path) orelse continue;
        const value: std.json.Value = if (cell.value) |present| switch (present) {
            .bytes => |v| .{ .string = try alloc.dupe(u8, v) },
            .json => |v| try std.json.parseFromSliceLeaky(std.json.Value, alloc, v, .{ .allocate = .alloc_always, .parse_numbers = false }),
            .i64 => |v| .{ .integer = v },
            .f64 => |v| .{ .float = v },
            .bool => |v| .{ .bool = v },
            .vector_f32 => return error.UnsupportedSqlExecution,
        } else .null;
        try object.put(alloc, column.name, value);
        try nulls.append(alloc, cell.value == null);
    }
    // RowRef includes the source snapshot and physical row ordinal. It remains
    // stable across projections, filtering and joins within that snapshot.
    const id = try std.json.Stringify.valueAlloc(alloc, row.row_ref, .{});
    return .{ .id = id, .version = 0, .value = .{ .object = object }, .sql_nulls = try nulls.toOwnedSlice(alloc) };
}

pub fn matches(row: catalog.Row, conditions: []const catalog.Condition) !bool {
    for (conditions) |condition| {
        const cell = try row.cell(condition.column);
        if (condition.op == .is_null) {
            if (!cell.sql_null) return false;
            continue;
        }
        if (condition.op == .is_not_null) {
            if (cell.sql_null) return false;
            continue;
        }
        if (cell.sql_null or condition.value == .null) return false;
        const order = try scalar.compare(cell.value, condition.value);
        const match = switch (condition.op) {
            .eq => order == .eq,
            .neq => order != .eq,
            .lt => order == .lt,
            .lte => order != .gt,
            .gt => order == .gt,
            .gte => order != .lt,
            .is_null, .is_not_null => unreachable,
        };
        if (!match) return false;
    }
    return true;
}

const Owner = struct {
    alloc: Allocator,
    arena: std.heap.ArenaAllocator,
    rows: []const catalog.Row,
    conditions: []const catalog.Condition,
    context: operation.RequestContext,
    source: ?*serving.ServingSource = null,
    after: ?[]const u8 = null,
    primary_key: ?[]const u8 = null,
    position: usize = 0,

    fn next(raw: *anyopaque, alloc: Allocator, limit: u32) !catalog.Page {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (limit == 0 or limit > 4096) return error.SqlLimitExceeded;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        var page: std.ArrayList(catalog.Row) = .empty;
        while (self.position < self.rows.len and page.items.len < limit) {
            try self.context.ensureActive();
            const row = self.rows[self.position];
            self.position += 1;
            if (self.after) |after| if (std.mem.order(u8, row.id, after) != .gt) continue;
            if (self.primary_key) |key| if (!std.mem.eql(u8, row.id, key)) continue;
            if (!try matches(row, self.conditions)) continue;
            const bytes = try std.json.Stringify.valueAlloc(a, row.value, .{});
            try page.append(a, .{ .id = try a.dupe(u8, row.id), .version = 0, .value = try std.json.parseFromSliceLeaky(std.json.Value, a, bytes, .{ .allocate = .alloc_always, .parse_numbers = false }), .sql_nulls = if (row.sql_nulls) |flags| try a.dupe(bool, flags) else null });
        }
        return .{ .rows = try page.toOwnedSlice(a), .owned_arena = arena, .after = if (self.position < self.rows.len) try a.dupe(u8, self.rows[self.position - 1].id) else null };
    }

    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.source) |source| {
            source.deinit();
            self.alloc.destroy(source);
        }
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};

test "lake SQL cursor preserves SQL null distinct from JSON null and exact integer filters" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var object = std.json.ObjectMap.empty;
    try object.put(a, "n", .{ .integer = 9007199254740993 });
    try object.put(a, "j", .null);
    try object.put(a, "missing", .null);
    const row: catalog.Row = .{ .id = "row", .version = 0, .value = .{ .object = object }, .sql_nulls = &.{ false, false, true } };
    try std.testing.expect(try matches(row, &.{.{ .column = "n", .op = .gt, .value = .{ .integer = 9007199254740992 } }}));
    try std.testing.expect(try matches(row, &.{.{ .column = "j", .op = .is_not_null }}));
    try std.testing.expect(!try matches(row, &.{.{ .column = "j", .op = .is_null }}));
    try std.testing.expect(try matches(row, &.{.{ .column = "missing", .op = .is_null }}));
    try std.testing.expect(!try matches(row, &.{.{ .column = "n", .op = .eq, .value = .null }}));
}

test "lake SQL cursor scans real Parquet with residual filtering before page limits" {
    const alloc = std.testing.allocator;
    const parquet = @import("../serverless/query/lake_parquet_rowgroup.zig");
    const storage = @import("../storage/object_storage.zig");
    var memory = storage.MemoryObjectStorage.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("bucket");
    const bytes = try parquet.buildTestSingleColumnPlainI64ParquetObjectAlloc(alloc, "amount", &.{ 1, 2, 3, 4, 5 });
    defer alloc.free(bytes);
    var initial_put = try client.putObject("bucket", "events/part.parquet", bytes, .{});
    defer initial_put.deinit(alloc);
    var inventory = try @import("../serverless/external_source/mod.zig").planParquetPrefixInventoryFromObjectStorageAlloc(alloc, .{ .client = client, .bucket = "bucket", .prefix = "events", .source_id = "events", .source_uri = "s3://bucket/events", .schema_fingerprint = "schema-v1" });
    defer inventory.deinit(alloc);
    const binding: @import("../serverless/external_source/schema_binding.zig").OwnedExternalTableBinding = .{
        .binding = .{ .table_id = "events", .format = .parquet, .source_uri = "s3://bucket/events", .schema_fingerprint = "schema-v1", .snapshot_mode = .current },
        .table_id = undefined,
        .source_uri = undefined,
        .schema_fingerprint = undefined,
    };
    const table: catalog.Table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .columns = &.{.{ .name = "amount", .path = "amount", .type = .integer }}, .external_base_source = binding };
    var source: serving.ServingSource = .{ .alloc = alloc, .store = undefined, .inventory = inventory, .scanner = serving.PinnedExternalObjectStorageLakeRowsScanner.init(inventory, client) };
    const cursor = try openPinned(alloc, table, .{ .fields = &.{"amount"}, .conditions = &.{.{ .column = "amount", .op = .gt, .value = .{ .integer = 2 } }}, .limit = 2 }, .{}, &source);
    defer cursor.close(cursor.ptr);
    // The statement has materialized a pinned snapshot. Later object mutation
    // cannot refresh one of its pages under the same cursor.
    var changed_put = try client.putObject("bucket", "events/part.parquet", "changed", .{});
    defer changed_put.deinit(alloc);
    const first = try cursor.next(cursor.ptr, alloc, 2);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 2), first.rows.len);
    try std.testing.expectEqual(std.math.Order.eq, try scalar.compare(first.rows[0].value.object.get("amount").?, .{ .integer = 3 }));
    try std.testing.expect(first.after != null);
    const last = try cursor.next(cursor.ptr, alloc, 2);
    defer last.deinit();
    try std.testing.expectEqual(@as(usize, 1), last.rows.len);
    try std.testing.expectEqual(std.math.Order.eq, try scalar.compare(last.rows[0].value.object.get("amount").?, .{ .integer = 5 }));
    try std.testing.expect(last.after == null);
}
