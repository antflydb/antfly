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
    return openWithCache(alloc, table, request, context, options, null, null);
}

pub fn openWithCache(alloc: Allocator, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions, cache: ?*@import("../serverless/query/lake_serving_cache.zig").Cache, io: ?std.Io) !catalog.Cursor {
    const source = try alloc.create(serving.ServingSource);
    errdefer alloc.destroy(source);
    const schema: @import("../storage/schema.zig").TableSchema = .{ .storage_mode = .relational, .external_base_source = table.external_base_source };
    const normalized = try context.platformDeadline();
    source.* = try serving.ServingSource.openWithContext(alloc, schema, options, .{ .io = io, .deadline_ns = normalized.deadline_ns, .cancellation = @import("../storage/object_storage.zig").CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) });
    errdefer source.deinit();
    if (cache) |shared| try source.attachCache(shared, table.external_base_source.?.binding, .{ .io = io, .deadline_ns = normalized.deadline_ns, .cancellation = @import("../storage/object_storage.zig").CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) });
    const cursor = try openPinned(alloc, table, request, context, source);
    const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
    owner.source = source;
    return cursor;
}

pub fn openPinned(alloc: Allocator, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, source: *serving.ServingSource) !catalog.Cursor {
    if (request.include_primary_digest or request.include_document or request.index_equality != null) return error.UnsupportedSqlExecution;
    try context.ensureActive();
    const binding = table.external_base_source orelse return error.InvalidSqlBackend;
    try @import("../serverless/query/lake_scan_plan.zig").validateBindingInventory(binding.binding, source.inventory);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var columns: std.ArrayList([]const u8) = .empty;
    for (request.fields) |field| try appendColumn(owned, &columns, table, field);
    for (request.conditions) |condition| try appendColumn(owned, &columns, table, condition.column);
    if (columns.items.len == 0) {
        if (table.columns.len == 0) return error.InvalidSqlBackendResponse;
        try columns.append(owned, table.columns[0].path);
    }
    const conditions = try owned.alloc(catalog.Condition, request.conditions.len);
    var pruning: std.ArrayList(@import("../serverless/query/lake_stream.zig").Predicate) = .empty;
    for (request.conditions, conditions) |condition, *out| {
        out.* = condition;
        out.column = try owned.dupe(u8, condition.column);
        const bytes = try std.json.Stringify.valueAlloc(owned, condition.value, .{});
        out.value = try std.json.parseFromSliceLeaky(std.json.Value, owned, bytes, .{ .allocate = .alloc_always, .parse_numbers = false });
        if (condition.value == .integer or condition.value == .float) out.value = condition.value;
        if (condition.op == .is_null or condition.op == .is_not_null or std.mem.eql(u8, condition.column, "_id")) continue;
        const column = try table.column(condition.column);
        const Predicate = @import("../serverless/query/lake_stream.zig").Predicate;
        const value: @FieldType(Predicate, "value") = switch (out.value) {
            .integer => |v| if (column.type == .integer or column.type == .datetime) .{ .integer = v } else continue,
            .string => |v| if (column.type == .string) .{ .bytes = v } else if (column.type == .datetime) .{ .integer = std.math.cast(i64, @import("../datetime.zig").parseRfc3339ToSignedNs(v) orelse continue) orelse continue } else continue,
            .bool => |v| if (column.type == .boolean) .{ .boolean = v } else continue,
            else => continue,
        };
        try pruning.append(owned, .{ .column = column.path, .op = std.meta.stringToEnum(Predicate.Op, @tagName(condition.op)).?, .value = value });
    }
    const normalized = try context.platformDeadline();
    var stream = try @import("../serverless/query/lake_stream.zig").Stream.init(alloc, source, columns.items, pruning.items, .{ .deadline_ns = normalized.deadline_ns, .cancellation = @import("../storage/object_storage.zig").CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) }, .{});
    errdefer stream.deinit();
    if (std.mem.startsWith(u8, binding.binding.schema_fingerprint, "parquet-schema:") or std.mem.indexOf(u8, binding.binding.schema_fingerprint, ":hash=") != null) {
        var contract: std.ArrayList(@import("../serverless/query/lake_schema.zig").Column) = .empty;
        for (table.columns) |column| {
            if (std.mem.eql(u8, column.name, "_id")) continue;
            try contract.append(owned, .{ .name = column.path, .kind = @tagName(column.type), .required = !column.nullable });
        }
        stream.schema_contract = contract.items;
    }
    const owner = try alloc.create(Owner);
    errdefer alloc.destroy(owner);
    owner.* = .{ .alloc = alloc, .arena = arena, .stream = stream, .table = table, .context = context, .conditions = conditions, .after = if (request.after) |v| try owned.dupe(u8, v) else null, .primary_key = if (request.primary_key) |v| try owned.dupe(u8, v) else null };
    return .{ .ptr = owner, .next = Owner.next, .next_columns = Owner.nextColumns, .count_rows = Owner.countRows, .close = Owner.close };
}

fn appendColumn(alloc: Allocator, columns: *std.ArrayList([]const u8), table: catalog.Table, name: []const u8) !void {
    if (std.mem.eql(u8, name, "_id")) return;
    const column = table.column(name) catch blk: {
        for (table.columns) |candidate| if (std.mem.eql(u8, candidate.path, name)) break :blk candidate;
        return error.UndefinedColumn;
    };
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
    const id = try @import("../storage/rowsource/identity.zig").allocId(alloc, row.row_ref);
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
    stream: @import("../serverless/query/lake_stream.zig").Stream,
    table: catalog.Table,
    conditions: []const catalog.Condition,
    context: operation.RequestContext,
    source: ?*serving.ServingSource = null,
    after: ?[]const u8 = null,
    primary_key: ?[]const u8 = null,
    batch: ?@import("../storage/rowsource/types.zig").ColumnBatch = null,
    position: usize = 0,
    exhausted: bool = false,

    fn countRows(raw: *anyopaque) !?u64 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.conditions.len != 0 or self.after != null or self.primary_key != null or self.batch != null) return null;
        if (self.stream.source.inventory.deleted_row_groups.len != 0) return null;
        if (self.stream.source.scanner.iceberg_delete_plan) |plan| if (plan.files.len != 0) return null;
        return self.stream.countAll() catch |err| switch (err) {
            error.PreconditionFailed, error.VersionMismatch, error.ObjectNotFound => return error.ExternalLakeSnapshotMismatch,
            else => return err,
        };
    }
    fn nextColumns(raw: *anyopaque, alloc: Allocator, limit: u32) !catalog.ColumnPage {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (limit == 0 or limit > 4096) return error.SqlLimitExceeded;
        try self.context.ensureActive();
        while (!self.exhausted) {
            if (self.batch == null or self.position == self.batch.?.rowCount()) {
                self.batch = self.stream.next() catch |err| switch (err) {
                    error.PreconditionFailed, error.VersionMismatch, error.ObjectNotFound => return error.ExternalLakeSnapshotMismatch,
                    else => return err,
                };
                self.position = 0;
                if (self.batch == null) {
                    self.exhausted = true;
                    break;
                }
            }
            const batch = self.batch.?;
            const vectors = try alloc.alloc(@import("../storage/rowsource/types.zig").ColumnVector, batch.columns.len);
            for (batch.columns, vectors) |column, *vector| {
                vector.* = column;
                for (self.table.columns) |definition| if (std.mem.eql(u8, definition.path, column.name)) {
                    vector.name = definition.name;
                    break;
                };
            }
            var view = batch;
            view.columns = vectors;
            const selected = try alloc.alloc(usize, @min(@as(usize, limit), batch.rowCount() - self.position));
            var count: usize = 0;
            var last_id: ?[]const u8 = null;
            while (self.position < batch.rowCount() and count < limit) {
                try self.context.ensureActive();
                const index = self.position;
                self.position += 1;
                if (self.stream.isDeleted(batch.row_refs[index])) continue;
                var temporary = std.heap.ArenaAllocator.init(self.alloc);
                defer temporary.deinit();
                const a = temporary.allocator();
                const one: catalog.ColumnPage = .{ .batch = view, .selection = &.{index} };
                if (self.after != null or self.primary_key != null) {
                    const id = (try one.cell(a, 0, "_id")).value.string;
                    if (self.after) |after| if (std.mem.order(u8, id, after) != .gt) continue;
                    if (self.primary_key) |key| if (!std.mem.eql(u8, id, key)) continue;
                }
                if (!try matchesColumns(one, a, self.conditions)) continue;
                selected[count] = index;
                count += 1;
            }
            if (count == 0) {
                alloc.free(selected);
                alloc.free(vectors);
                continue;
            }
            last_id = try @import("../storage/rowsource/identity.zig").allocId(alloc, batch.row_refs[selected[count - 1]]);
            const end = self.position == batch.rowCount() and (self.stream.page_cursor == null or self.stream.page_cursor.?.position == self.stream.page_cursor.?.group.row_count) and self.stream.file_index == self.stream.files.len and self.stream.group_index == self.stream.discovered.?.row_group_plan.row_groups.len;
            return .{ .batch = view, .selection = selected[0..count], .after = if (end or self.primary_key != null) null else last_id };
        }
        return .{ .batch = .{ .snapshot = .{ .table_id = self.stream.source.inventory.source_id, .snapshot_id = self.stream.source.inventory.snapshot_id }, .row_refs = &.{}, .columns = &.{} }, .selection = &.{} };
    }
    fn next(raw: *anyopaque, alloc: Allocator, limit: u32) !catalog.Page {
        const self: *Owner = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const page = try nextColumns(raw, a, limit);
        const output = try a.alloc(catalog.Row, page.selection.len);
        for (output, 0..) |*row, i| {
            var object = std.json.ObjectMap.empty;
            const nulls = try a.alloc(bool, page.batch.columns.len);
            for (page.batch.columns, nulls) |column, *flag| {
                const cell = try page.cell(a, i, column.name);
                // Clone string values only at the row API boundary.
                const value = if (cell.value == .string) std.json.Value{ .string = try a.dupe(u8, cell.value.string) } else cell.value;
                try object.put(a, column.name, value);
                flag.* = cell.sql_null;
            }
            row.* = .{ .id = try @import("../storage/rowsource/identity.zig").allocId(a, page.batch.row_refs[page.selection[i]]), .version = 0, .value = .{ .object = object }, .sql_nulls = nulls };
        }
        _ = self;
        return .{ .rows = output, .owned_arena = arena, .after = page.after };
    }
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.stream.deinit();
        if (self.source) |source| {
            source.deinit();
            self.alloc.destroy(source);
        }
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};

fn matchesColumns(page: catalog.ColumnPage, alloc: Allocator, conditions: []const catalog.Condition) !bool {
    for (conditions) |condition| {
        const cell = try page.cell(alloc, 0, condition.column);
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
            else => unreachable,
        };
        if (!match) return false;
    }
    return true;
}

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

const TestLake = struct {
    page_rows: usize = std.math.maxInt(usize),
    memory: @import("../storage/object_storage.zig").MemoryObjectStorage,
    inventory: @import("../serverless/external_source/types.zig").Inventory = undefined,
    meter: Meter = undefined,
    source: serving.ServingSource = undefined,
    table: catalog.Table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .columns = &.{.{ .name = "amount", .path = "amount", .type = .integer }}, .external_base_source = .{ .binding = .{ .table_id = "events", .format = .parquet, .source_uri = "s3://bucket/events", .schema_fingerprint = "v1" }, .table_id = undefined, .source_uri = undefined, .schema_fingerprint = undefined } },
    const storage = @import("../storage/object_storage.zig");
    const Meter = struct {
        base: storage.ObjectStorage,
        vtable: storage.ObjectStorage.VTable,
        reads: usize = 0,
        bytes: usize = 0,
        fn client(self: *Meter) storage.ObjectStorage {
            self.vtable.get_object = get;
            self.vtable.stat_object = stat;
            self.vtable.stat_object_with_options = null;
            return .{ .allocator = self.base.allocator, .ptr = self, .vtable = &self.vtable };
        }
        fn stat(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8) !storage.ObjectMetadata {
            const self: *Meter = @ptrCast(@alignCast(raw));
            var base = self.base;
            base.allocator = alloc;
            return base.statObject(bucket, key);
        }
        fn get(raw: *anyopaque, alloc: Allocator, bucket: []const u8, key: []const u8, options: storage.GetOptions) !storage.GetResult {
            const self: *Meter = @ptrCast(@alignCast(raw));
            var base = self.base;
            base.allocator = alloc;
            self.reads += 1;
            const result = try base.getObject(bucket, key, options);
            self.bytes += result.body.len;
            return result;
        }
    };
    fn populate(self: *TestLake, alloc: Allocator, count: usize, values: []const i64) !void {
        var client = self.memory.client();
        try client.makeBucket("bucket");
        const data = try @import("../serverless/query/lake_parquet_rowgroup.zig").buildTestPlainI64ParquetObjectAlloc(alloc, &.{.{ .column_id = "amount", .values = values, .field_id = 1, .write_statistics = true, .page_rows = self.page_rows }});
        defer alloc.free(data);
        for (0..count) |i| {
            const key = try std.fmt.allocPrint(alloc, "events/{d}.parquet", .{i});
            defer alloc.free(key);
            var written = try client.putObject("bucket", key, data, .{});
            written.deinit(alloc);
        }
        self.inventory = try @import("../serverless/external_source/mod.zig").planParquetPrefixInventoryFromObjectStorageAlloc(alloc, .{ .client = client, .bucket = "bucket", .prefix = "events", .source_id = "events", .source_uri = "s3://bucket/events", .schema_fingerprint = "v1" });
        self.meter = .{ .base = client, .vtable = client.vtable.* };
        self.source = .{ .alloc = alloc, .store = .{ .alloc = alloc, .client = client, .owns_client = false, .bucket = @constCast("bucket"), .prefix = @constCast("events") }, .inventory = self.inventory, .scanner = serving.PinnedExternalObjectStorageLakeRowsScanner.init(self.inventory, self.meter.client()) };
    }
    fn deinit(self: *TestLake, alloc: Allocator) void {
        if (self.source.scanner.shared_reader) |reader| alloc.destroy(reader);
        self.inventory.deinit(alloc);
        self.memory.deinit();
    }
};

test "lake SQL stream is lazy prunes pages and fences changed objects" {
    const alloc = std.testing.allocator;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(alloc) };
    try lake.populate(alloc, 3, &.{ 1, 2, 3, 4, 5 });
    defer lake.deinit(alloc);
    const cursor = try openPinned(alloc, lake.table, .{ .fields = &.{"amount"}, .limit = 1 }, .{}, &lake.source);
    defer cursor.close(cursor.ptr);
    try std.testing.expectEqual(@as(usize, 0), lake.meter.reads);
    const first = try cursor.next(cursor.ptr, alloc, 1);
    defer first.deinit();
    const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
    try std.testing.expectEqual(@as(usize, 1), owner.stream.stats.files_opened);
    try std.testing.expectEqual(@as(usize, 1), owner.stream.stats.groups_decoded);
    try std.testing.expectEqual(@as(usize, 1), first.rows.len);
    const reads = lake.meter.reads;
    const second = try cursor.next(cursor.ptr, alloc, 1);
    defer second.deinit();
    try std.testing.expectEqual(reads, lake.meter.reads);
    try std.testing.expect(std.mem.order(u8, first.rows[0].id, second.rows[0].id) == .lt);
    const pruned = try openPinned(alloc, lake.table, .{ .fields = &.{"amount"}, .conditions = &.{.{ .column = "amount", .op = .gt, .value = .{ .integer = 10 } }}, .limit = 2 }, .{}, &lake.source);
    defer pruned.close(pruned.ptr);
    const empty = try pruned.next(pruned.ptr, alloc, 2);
    defer empty.deinit();
    const pruned_owner: *Owner = @ptrCast(@alignCast(pruned.ptr));
    try std.testing.expectEqual(@as(usize, 0), pruned_owner.stream.stats.groups_decoded);
    try std.testing.expectEqual(@as(usize, 3), pruned_owner.stream.stats.groups_pruned);
    try std.testing.expectEqual(@as(usize, 0), empty.rows.len);
    try std.testing.expect(empty.after == null);
    const stale = try openPinned(alloc, lake.table, .{ .fields = &.{"amount"}, .limit = 1 }, .{}, &lake.source);
    defer stale.close(stale.ptr);
    const stale_owner: *Owner = @ptrCast(@alignCast(stale.ptr));
    const changed_file = lake.inventory.files[stale_owner.stream.files[0]];
    const object = try @import("../serverless/query/lake_range_io.zig").objectRefForExternalFileUri(changed_file);
    var client = lake.memory.client();
    var put = try client.putObject(object.bucket, object.key, "changed", .{});
    defer put.deinit(alloc);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, stale.next(stale.ptr, alloc, 1));
}

test "lake SQL shared cache reuses ranges across scans without refreshing pinned versions" {
    const alloc = std.testing.allocator;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(alloc) };
    try lake.populate(alloc, 1, &.{ 1, 2, 3 });
    defer lake.deinit(alloc);
    var cache = @import("../serverless/query/lake_serving_cache.zig").Cache.init(alloc);
    defer cache.deinit();
    try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{});
    for (0..2) |iteration| {
        const before = lake.meter.reads;
        const cursor = try openPinned(alloc, lake.table, .{ .fields = &.{"amount"}, .limit = 3 }, .{}, &lake.source);
        defer cursor.close(cursor.ptr);
        const page = try cursor.next(cursor.ptr, alloc, 3);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 3), page.rows.len);
        if (iteration == 1) try std.testing.expectEqual(before, lake.meter.reads);
    }
    try std.testing.expect(cache.snapshot().hits >= 2);
}

test "lake SQL typed stream scans a million rows with memory bounded by one row group" {
    const alloc = std.testing.allocator;
    const values = try alloc.alloc(i64, 65536);
    defer alloc.free(values);
    for (values, 0..) |*value, i| value.* = @intCast(i);
    var small_peak: usize = 0;
    for ([_]usize{ 2, 16 }) |count| {
        var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(alloc) };
        try lake.populate(alloc, count, values);
        defer lake.deinit(alloc);
        var budget: @import("../sql/memory_budget.zig") = .{ .backing = alloc, .limit = 32 * 1024 * 1024 };
        const a = budget.allocator();
        {
            const cursor = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 256 }, .{}, &lake.source);
            defer cursor.close(cursor.ptr);
            var total: usize = 0;
            var sum: i128 = 0;
            while (true) {
                var page_arena = std.heap.ArenaAllocator.init(a);
                defer page_arena.deinit();
                const page = try cursor.next_columns.?(cursor.ptr, page_arena.allocator(), 256);
                for (0..page.selection.len) |i| sum += (try page.cell(page_arena.allocator(), i, "amount")).value.integer;
                total += page.selection.len;
                if (page.after == null) break;
            }
            try std.testing.expectEqual(count * values.len, total);
            try std.testing.expectEqual(@as(i128, @intCast(count)) * 2147450880, sum);
            const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
            try std.testing.expectEqual(count, owner.stream.stats.groups_decoded);
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
        if (count == 2) small_peak = budget.peak else try std.testing.expect(budget.peak <= small_peak + 1024 * 1024);
        std.debug.print("Lake typed stream: rows={d} peak_bytes={d} physical_reads={d}\n", .{ count * values.len, budget.peak, lake.meter.reads });
    }
}

test "lake SQL metadata count avoids decoded rows and typed pages preserve null flags" {
    const alloc = std.testing.allocator;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(alloc) };
    try lake.populate(alloc, 2, &.{ 1, 2, 3, 4, 5 });
    defer lake.deinit(alloc);
    const cursor = try openPinned(alloc, lake.table, .{ .fields = &.{}, .limit = 1 }, .{}, &lake.source);
    defer cursor.close(cursor.ptr);
    try std.testing.expectEqual(@as(?u64, 10), try cursor.count_rows.?(cursor.ptr));
    const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
    try std.testing.expectEqual(@as(usize, 0), owner.stream.stats.groups_decoded);
    try std.testing.expectEqual(@as(usize, 2), owner.stream.stats.files_opened);
    const filtered = try openPinned(alloc, lake.table, .{ .fields = &.{}, .conditions = &.{.{ .column = "amount", .op = .gt, .value = .{ .integer = 3 } }}, .limit = 1 }, .{}, &lake.source);
    defer filtered.close(filtered.ptr);
    try std.testing.expect((try filtered.count_rows.?(filtered.ptr)) == null);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const page: catalog.ColumnPage = .{ .batch = .{ .snapshot = .{ .table_id = "events", .snapshot_id = "v1" }, .row_refs = &.{ .{ .relational_key = "a" }, .{ .relational_key = "b" } }, .columns = &.{ .{ .name = "json", .values = .{ .json = &.{ "null", "null" } }, .nulls = .{ .bytes = &.{ 0, 1 } } }, .{ .name = "large", .values = .{ .i64 = &.{ 9007199254740993, 9007199254740994 } } } } }, .selection = &.{ 0, 1 } };
    const json_null = try page.cell(arena.allocator(), 0, "json");
    try std.testing.expect(json_null.value == .null and !json_null.sql_null);
    try std.testing.expect((try page.cell(arena.allocator(), 1, "json")).sql_null);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try page.cell(arena.allocator(), 0, "large")).value.integer);
}

test "lake SQL typed stream applies Iceberg equality and position deletes before limits" {
    const alloc = std.testing.allocator;
    const parquet = @import("../serverless/query/lake_parquet_rowgroup.zig");
    const iceberg = @import("../serverless/query/lake_iceberg_snapshot.zig");
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(alloc) };
    try lake.populate(alloc, 1, &.{ 1, 2, 3, 4, 5 });
    defer lake.deinit(alloc);
    lake.inventory.format = .iceberg;
    lake.inventory.files[0].data_sequence_number = 5;
    lake.inventory.files[0].partition_spec_id = 0;
    lake.source.inventory = lake.inventory;
    lake.table.external_base_source.?.binding.format = .iceberg;
    const equal_bytes = try parquet.buildTestPlainI64ParquetObjectAlloc(alloc, &.{.{ .column_id = "amount", .field_id = 1, .values = &.{2} }});
    defer alloc.free(equal_bytes);
    const pos_bytes = try parquet.buildTestPlainI64AndByteArrayParquetObjectAlloc(alloc, &.{.{ .column_id = "pos", .values = &.{3} }}, &.{.{ .column_id = "file_path", .values = &.{lake.inventory.files[0].object_uri} }});
    defer alloc.free(pos_bytes);
    var client = lake.memory.client();
    var equal_put = try client.putObject("bucket", "deletes/equal.parquet", equal_bytes, .{});
    defer equal_put.deinit(alloc);
    var pos_put = try client.putObject("bucket", "deletes/pos.parquet", pos_bytes, .{});
    defer pos_put.deinit(alloc);
    var plan: iceberg.IcebergDeletePlan = .{ .files = try alloc.alloc(iceberg.IcebergDeleteFile, 2) };
    defer plan.deinit(alloc);
    plan.files[0] = .{ .content = .equality_deletes, .file_path = try alloc.dupe(u8, "s3://bucket/deletes/equal.parquet"), .file_format = try alloc.dupe(u8, "PARQUET"), .snapshot_id = 12, .data_sequence_number = 7, .file_sequence_number = 8, .record_count = 1, .file_size_in_bytes = equal_bytes.len, .equality_ids = try alloc.dupe(i32, &.{1}), .equality_columns = try alloc.alloc([]u8, 1) };
    plan.files[0].equality_columns[0] = try alloc.dupe(u8, "amount");
    plan.files[1] = .{ .content = .position_deletes, .file_path = try alloc.dupe(u8, "s3://bucket/deletes/pos.parquet"), .file_format = try alloc.dupe(u8, "PARQUET"), .snapshot_id = 12, .data_sequence_number = 7, .file_sequence_number = 9, .record_count = 1, .file_size_in_bytes = pos_bytes.len };
    lake.source.scanner.iceberg_delete_plan = plan;
    const cursor = try openPinned(alloc, lake.table, .{ .fields = &.{"amount"}, .limit = 1 }, .{}, &lake.source);
    defer cursor.close(cursor.ptr);
    try std.testing.expectEqual(@as(?u64, null), try cursor.count_rows.?(cursor.ptr));
    for ([_]i64{ 1, 3, 5 }) |expected| {
        const page = try cursor.next(cursor.ptr, alloc, 1);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqual(expected, page.rows[0].value.object.get("amount").?.integer);
    }
    const end = try cursor.next(cursor.ptr, alloc, 1);
    defer end.deinit();
    try std.testing.expectEqual(@as(usize, 0), end.rows.len);
    var mismatched = lake.table;
    mismatched.external_base_source.?.binding.schema_fingerprint = "another-schema";
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, openPinned(alloc, mismatched, .{ .fields = &.{}, .limit = 1 }, .{}, &lake.source));
}

test "lake SQL Parquet page cursor preserves row ordinals and bounds decoded row group memory" {
    const a = std.testing.allocator;
    const values = try a.alloc(i64, 8192);
    defer a.free(values);
    for (values, 0..) |*value, index| value.* = @intCast(index);
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a), .page_rows = 64 };
    try lake.populate(a, 1, values);
    defer lake.deinit(a);
    var budget: @import("../sql/memory_budget.zig") = .{ .backing = a, .limit = 1024 * 1024 };
    {
        const cursor = try openPinned(budget.allocator(), lake.table, .{ .fields = &.{"amount"}, .limit = 17 }, .{}, &lake.source);
        defer cursor.close(cursor.ptr);
        var count: usize = 0;
        while (true) {
            var scratch = std.heap.ArenaAllocator.init(budget.allocator());
            defer scratch.deinit();
            const page = try cursor.next_columns.?(cursor.ptr, scratch.allocator(), 17);
            for (0..page.selection.len) |index| {
                try std.testing.expectEqual(@as(i64, @intCast(count)), (try page.cell(scratch.allocator(), index, "amount")).value.integer);
                try std.testing.expectEqual(@as(u64, @intCast(count)), page.batch.row_refs[page.selection[index]].external.row_ordinal);
                count += 1;
            }
            if (page.after == null) break;
        }
        try std.testing.expectEqual(values.len, count);
        const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
        try std.testing.expectEqual(@as(usize, 1), owner.stream.stats.groups_decoded);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.peak < 256 * 1024);
}
