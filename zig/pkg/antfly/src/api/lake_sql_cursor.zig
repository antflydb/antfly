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
    source.* = try serving.ServingSource.openCached(alloc, schema, options, .{ .io = io, .deadline_ns = normalized.deadline_ns, .cancellation = @import("../storage/object_storage.zig").CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) }, cache);
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
    if (request.after) |id| try @import("../storage/rowsource/identity.zig").validateContinuation(id, source.inventory);
    if (request.before) |id| try @import("../storage/rowsource/identity.zig").validateContinuation(id, source.inventory);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var columns: std.ArrayList([]const u8) = .empty;
    for (request.fields) |field| try appendColumn(owned, &columns, table, field);
    for (request.conditions) |condition| try appendColumn(owned, &columns, table, condition.column);
    if (source.scanner.iceberg_delete_plan) |plan| for (plan.files) |file| for (file.equality_columns) |name| {
        // Equality columns are scan evidence even when absent from SELECT.
        var found = false;
        for (columns.items) |prior| if (std.mem.eql(u8, prior, name)) {
            found = true;
            break;
        };
        if (!found) try columns.append(owned, name);
    };
    const identity_only = columns.items.len == 0;
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
        out.value = try @import("lake_values.zig").comparisonValue(owned, out.value, column.type);
        const Predicate = @import("../serverless/query/lake_stream.zig").Predicate;
        const value: @FieldType(Predicate, "value") = switch (out.value) {
            .integer => |v| if (column.type == .integer or column.type == .datetime) .{ .integer = v } else continue,
            .string => |v| if (column.type == .string) .{ .bytes = v } else continue,
            .bool => |v| if (column.type == .boolean) .{ .boolean = v } else continue,
            else => continue,
        };
        try pruning.append(owned, .{ .column = column.path, .op = std.meta.stringToEnum(Predicate.Op, @tagName(condition.op)).?, .value = value });
    }
    const normalized = try context.platformDeadline();
    var stream = try @import("../serverless/query/lake_stream.zig").Stream.init(alloc, source, columns.items, pruning.items, .{ .deadline_ns = normalized.deadline_ns, .cancellation = @import("../storage/object_storage.zig").CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) }, .{});
    errdefer stream.deinit();
    stream.identity_only = identity_only;
    if (source.iceberg_schema != null or std.mem.startsWith(u8, binding.binding.schema_fingerprint, "parquet-schema:") or std.mem.indexOf(u8, binding.binding.schema_fingerprint, ":hash=") != null) {
        var contract: std.ArrayList(@import("../serverless/query/lake_schema.zig").Column) = .empty;
        for (table.columns) |column| {
            if (std.mem.eql(u8, column.name, "_id")) continue;
            try contract.append(owned, .{ .name = column.path, .kind = @tagName(column.type), .required = !column.nullable });
        }
        stream.schema_contract = contract.items;
    }
    if (source.iceberg_schema) |selected| {
        for (stream.schema_contract) |expected| {
            const actual = for (selected.columns) |column| {
                if (std.mem.eql(u8, column.name, expected.name)) break column;
            } else return error.ExternalLakeSchemaMismatch;
            if (!std.mem.eql(u8, actual.kind, expected.kind) or (expected.required and !actual.required)) return error.ExternalLakeSchemaMismatch;
        }
        stream.schema_contract = selected.columns;
    }
    const owner = try alloc.create(Owner);
    errdefer alloc.destroy(owner);
    const after = if (request.after) |v| try owned.dupe(u8, v) else null;
    const before = if (request.before) |v| try owned.dupe(u8, v) else null;
    const primary_key = if (request.primary_key) |v| try owned.dupe(u8, v) else null;
    owner.* = .{ .alloc = alloc, .arena = arena, .stream = stream, .table = table, .context = context, .conditions = conditions, .after = after, .before = before, .primary_key = primary_key, .mask_arena = .init(alloc) };
    var filter_columns: std.ArrayList([]const u8) = .empty;
    for (conditions) |condition| try appendColumn(owned, &filter_columns, table, condition.column);
    if (source.scanner.iceberg_delete_plan) |plan| for (plan.files) |file| for (file.equality_columns) |name| try filter_columns.append(owned, name);
    owner.stream.filter_columns = filter_columns.items;
    if (source.scanner.iceberg_delete_plan != null or conditions.len != 0 or after != null or before != null or primary_key != null)
        owner.stream.filter = .{ .ptr = owner, .any_match = Owner.anyMatch };
    var estimated_rows: u64 = 0;
    var estimated_bytes: u64 = 0;
    for (source.inventory.files) |file| {
        estimated_rows +|= file.row_count;
        estimated_bytes +|= file.byte_len;
    }
    return .{ .estimated_rows = if (source.inventory.format == .iceberg) estimated_rows else null, .estimated_bytes = estimated_bytes, .ptr = owner, .next = Owner.next, .next_columns = Owner.nextColumns, .count_rows = Owner.countRows, .set_dynamic_filter = Owner.setDynamicFilter, .split_scan = Owner.splitScan, .split_ordered = Owner.splitOrdered, .close = Owner.close };
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
    dynamic: ?*const @import("../sql/dynamic_filter.zig").Filter = null,
    started: bool = false,
    alloc: Allocator,
    arena: std.heap.ArenaAllocator,
    stream: @import("../serverless/query/lake_stream.zig").Stream,
    table: catalog.Table,
    conditions: []const catalog.Condition,
    context: operation.RequestContext,
    source: ?*serving.ServingSource = null,
    partition_source: ?*serving.ServingSource = null,
    partition_versions_owned: bool = false,
    borrowed_prepared: ?*@import("../serverless/query/lake_prepared_deletes.zig").Prepared = null,
    after: ?[]const u8 = null,
    before: ?[]const u8 = null,
    primary_key: ?[]const u8 = null,
    batch: ?@import("../storage/rowsource/types.zig").ColumnBatch = null,
    position: usize = 0,
    exhausted: bool = false,

    mask_arena: std.heap.ArenaAllocator,
    selection_mask: []bool = &.{},
    mask_file: []const u8 = &.{},
    mask_group: u32 = 0,
    mask_first: u64 = 0,
    fn selectionMask(self: *Owner, batch: @import("../storage/rowsource/types.zig").ColumnBatch) ![]const bool {
        const count = batch.rowCount();
        if (count == 0) return &.{};
        const ref = batch.row_refs[0].external;
        if (std.mem.eql(u8, self.mask_file, ref.file_id) and self.mask_group == ref.row_group_ordinal and ref.row_ordinal >= self.mask_first) {
            const start = std.math.cast(usize, ref.row_ordinal - self.mask_first) orelse return error.InvalidSqlBackendResponse;
            if (start <= self.selection_mask.len and count <= self.selection_mask.len - start) return self.selection_mask[start..][0..count];
        }
        _ = self.mask_arena.reset(.retain_capacity);
        self.selection_mask = &.{};
        self.mask_file = &.{};
        const a = self.mask_arena.allocator();
        const selected = try a.alloc(bool, count);
        @memset(selected, true);
        try self.stream.deleteMask(a, batch, selected);
        const vectors = try a.dupe(@import("../storage/rowsource/types.zig").ColumnVector, batch.columns);
        for (vectors) |*vector| for (self.table.columns) |definition| if (std.mem.eql(u8, definition.path, vector.name)) {
            vector.name = definition.name;
            break;
        };
        var view = batch;
        view.columns = vectors;
        // Column-major predicates, with dictionary entries evaluated once.
        for (self.conditions) |condition| {
            const physical = view.findColumn(condition.column);
            var dictionary: std.AutoHashMapUnmanaged(u32, bool) = .empty;
            defer dictionary.deinit(a);
            for (selected, 0..) |*keep, index| {
                if (!keep.*) continue;
                const one: catalog.ColumnPage = .{ .batch = view, .selection = &.{index} };
                if (physical != null and physical.?.values == .dictionary_bytes and !physical.?.nulls.isNull(index)) {
                    const id = physical.?.values.dictionary_bytes.indices[index];
                    if (id >= physical.?.values.dictionary_bytes.values.len) return error.InvalidSqlBackendResponse;
                    if (dictionary.get(id)) |cached| {
                        keep.* = cached;
                    } else {
                        keep.* = try matchesColumns(one, a, self.table, &.{condition});
                        try dictionary.put(a, id, keep.*);
                    }
                } else keep.* = try matchesColumns(one, a, self.table, &.{condition});
            }
        }
        if (self.dynamic) |filter| try filter.applyBatch(a, view, selected);
        if (self.after != null or self.before != null or self.primary_key != null) for (selected, 0..) |*keep, index| {
            if (!keep.*) continue;
            const id = try @import("../storage/rowsource/identity.zig").allocId(a, batch.row_refs[index]);
            if (self.after) |after| if (std.mem.order(u8, id, after) != .gt) {
                keep.* = false;
            };
            if (self.before) |before| if (std.mem.order(u8, id, before) != .lt) {
                keep.* = false;
            };
            if (self.primary_key) |key| if (!std.mem.eql(u8, id, key)) {
                keep.* = false;
            };
        };
        self.mask_file = try a.dupe(u8, ref.file_id);
        self.mask_group = ref.row_group_ordinal;
        self.mask_first = ref.row_ordinal;
        self.selection_mask = selected;
        return selected;
    }
    fn anyMatch(raw: *anyopaque, batch: @import("../storage/rowsource/types.zig").ColumnBatch) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        for (try self.selectionMask(batch)) |selected| if (selected) return true;
        return false;
    }
    fn countRows(raw: *anyopaque) !?u64 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.dynamic != null or self.conditions.len != 0 or self.after != null or self.before != null or self.primary_key != null or self.batch != null) return null;
        if (self.stream.source.inventory.deleted_row_groups.len != 0) return null;
        if (self.stream.source.scanner.iceberg_delete_plan) |plan| if (plan.files.len != 0) return null;
        return self.stream.countAll() catch |err| switch (err) {
            error.PreconditionFailed, error.VersionMismatch, error.ObjectNotFound, error.FileNotFound => return error.ExternalLakeSnapshotMismatch,
            else => return err,
        };
    }
    fn setDynamicFilter(raw: *anyopaque, filter: *const @import("../sql/dynamic_filter.zig").Filter) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.started or self.dynamic != null or !filter.sealed) return false;
        try self.context.ensureActive();
        const a = self.arena.allocator();
        var predicates: std.ArrayList(@import("../serverless/query/lake_stream.zig").Predicate) = .empty;
        try predicates.appendSlice(a, self.stream.predicates);
        for (filter.columns, filter.domains) |key, domain| {
            const column = try self.table.column(key.name);
            if (domain.minimum == null) continue;
            for ([_]bool{ true, false }) |lower| {
                const value = if (lower) domain.minimum.?.value else domain.maximum.?.value;
                const Predicate = @import("../serverless/query/lake_stream.zig").Predicate;
                const physical: @FieldType(Predicate, "value") = switch (value) {
                    .integer => |integer| if (column.type == .integer) .{ .integer = integer } else continue,
                    .string => |bytes| if (column.type == .string) .{ .bytes = bytes } else continue,
                    .bool => |boolean| if (column.type == .boolean) .{ .boolean = boolean } else continue,
                    else => continue,
                };
                try predicates.append(a, .{ .column = column.path, .op = if (lower) .gte else .lte, .value = physical });
            }
        }
        var filter_columns: std.ArrayList([]const u8) = .empty;
        try filter_columns.appendSlice(a, self.stream.filter_columns);
        var scan_columns: std.ArrayList([]const u8) = .empty;
        try scan_columns.appendSlice(a, self.stream.columns);
        for (filter.columns) |key| {
            try appendColumn(a, &filter_columns, self.table, key.name);
            try appendColumn(a, &scan_columns, self.table, key.name);
        }
        self.stream.columns = scan_columns.items;
        if (filter_columns.items.len != 0) self.stream.identity_only = false;
        self.stream.filter_columns = filter_columns.items;
        self.stream.filter = .{ .ptr = self, .any_match = anyMatch };
        self.stream.predicates = predicates.items;
        self.dynamic = filter;
        return true;
    }
    fn nextColumns(raw: *anyopaque, alloc: Allocator, limit: u32) !catalog.ColumnPage {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.started = true;
        if (self.dynamic) |filter| {
            if (filter.rows == 0) self.exhausted = true;
        }
        if (limit == 0 or limit > 4096) return error.SqlLimitExceeded;
        try self.context.ensureActive();
        while (!self.exhausted) {
            if (self.batch == null or self.position == self.batch.?.rowCount()) {
                self.batch = self.stream.next() catch |err| switch (err) {
                    error.PreconditionFailed, error.VersionMismatch, error.ObjectNotFound, error.FileNotFound => return error.ExternalLakeSnapshotMismatch,
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
            const mask = try self.selectionMask(batch);
            var count: usize = 0;
            var last_id: ?[]const u8 = null;
            while (self.position < batch.rowCount() and count < limit) {
                try self.context.ensureActive();
                const index = self.position;
                self.position += 1;
                if (!mask[index]) continue;
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
    fn splitOrdered(raw: *anyopaque, a: Allocator, maximum: usize) !?[]catalog.Cursor {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.started or self.dynamic != null or self.stream.partition_count != 1 or maximum < 2 or self.stream.source.scanner.shared_reader == null) return null;
        // Multiple files form lazy contiguous ranges. Opening a later footer
        // must not move its error ahead of an earlier successfully read prefix.
        if (self.stream.files.len >= 2) return try self.splitChildren(a, @min(maximum, self.stream.files.len), true);
        const children = (try splitScan(raw, a, maximum)) orelse return null;
        const work = self.stream.work.?;
        std.mem.sort(@import("../serverless/query/lake_stream.zig").ScanWork.Unit, work.units, self.stream.source.inventory, struct {
            fn less(inventory: @import("../serverless/external_source/types.zig").Inventory, left: @import("../serverless/query/lake_stream.zig").ScanWork.Unit, right: @import("../serverless/query/lake_stream.zig").ScanWork.Unit) bool {
                if (left.file == right.file) return left.ordinal < right.ordinal;
                const identities = @import("../storage/rowsource/identity.zig");
                const l = identities.fileDigest(inventory.source_id, inventory.snapshot_id, inventory.files[left.file].file_id);
                const r = identities.fileDigest(inventory.source_id, inventory.snapshot_id, inventory.files[right.file].file_id);
                return std.mem.order(u8, &l, &r) == .lt;
            }
        }.less);
        for (children, 0..) |child, index| {
            const owner: *Owner = @ptrCast(@alignCast(child.ptr));
            owner.stream.ordered_next = index * work.units.len / children.len;
            owner.stream.ordered_end = (index + 1) * work.units.len / children.len;
        }
        return children;
    }
    fn splitScan(raw: *anyopaque, a: Allocator, maximum: usize) !?[]catalog.Cursor {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.started or self.dynamic != null or self.stream.partition_count != 1) return null;
        const count = try self.stream.partitionCount(maximum);
        if (count < 2) return null;
        return try self.splitChildren(a, count, false);
    }
    fn cloneVersionState(a: Allocator, source: *serving.ServingSource) !void {
        const files = try a.alloc(@import("../serverless/external_source/types.zig").FileEntry, source.inventory.files.len);
        var owned: usize = 0;
        errdefer {
            for (files[0..owned]) |file| {
                a.free(file.etag);
                a.free(file.version_id);
            }
            a.free(files);
        }
        for (source.inventory.files, files) |original, *file| {
            file.* = original;
            file.etag = try a.dupe(u8, original.etag);
            errdefer a.free(file.etag);
            file.version_id = try a.dupe(u8, original.version_id);
            owned += 1;
        }
        const pinned = try a.dupe(bool, source.pinned_files);
        source.inventory.files = files;
        source.pinned_files = pinned;
        source.alloc = a;
        source.scanner.inventory = source.inventory;
    }
    fn freeVersionState(a: Allocator, source: *serving.ServingSource) void {
        for (source.inventory.files) |file| {
            a.free(file.etag);
            a.free(file.version_id);
        }
        a.free(source.inventory.files);
        a.free(source.pinned_files);
    }
    fn splitChildren(self: *Owner, a: Allocator, count: usize, ordered_files: bool) ![]catalog.Cursor {
        const cursors = try a.alloc(catalog.Cursor, count);
        errdefer a.free(cursors);
        var opened: usize = 0;
        errdefer for (cursors[0..opened]) |cursor| cursor.close(cursor.ptr);
        for (cursors, 0..) |*cursor, index| {
            const source = try a.create(serving.ServingSource);
            errdefer a.destroy(source);
            source.* = self.stream.source.*;
            if (ordered_files) try cloneVersionState(a, source);
            errdefer if (ordered_files) freeVersionState(a, source);
            const parent_reader = source.scanner.shared_reader.?;
            const reader = try a.create(@import("../serverless/query/lake_serving_cache.zig").Reader);
            errdefer a.destroy(reader);
            reader.* = .{ .cache = parent_reader.cache, .base = parent_reader.base, .scope = parent_reader.scope, .context = parent_reader.context };
            source.scanner.shared_reader = reader;
            cursor.* = try openPinned(a, self.table, .{ .fields = self.stream.columns, .conditions = self.conditions, .after = self.after, .before = self.before, .primary_key = self.primary_key, .limit = 1024 }, self.context, source);
            const child: *Owner = @ptrCast(@alignCast(cursor.ptr));
            child.partition_source = source;
            child.partition_versions_owned = ordered_files;
            child.borrowed_prepared = source.prepared_deletes;
            if (!ordered_files) child.stream.work = self.stream.work;
            child.stream.partition_index = index;
            child.stream.partition_count = count;
            opened += 1;
        }
        self.started = true;
        return cursors;
    }
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.mask_arena.deinit();
        self.stream.deinit();
        if (self.partition_source) |source| {
            if (source.prepared_deletes != self.borrowed_prepared) {
                if (source.delete_lease) |lease| lease.release() else if (source.prepared_deletes) |prepared| prepared.destroy(source.alloc);
            }
            if (self.partition_versions_owned) freeVersionState(self.alloc, source);
            self.alloc.destroy(source.scanner.shared_reader.?);
            self.alloc.destroy(source);
        }
        if (self.source) |source| {
            source.deinit();
            self.alloc.destroy(source);
        }
        self.arena.deinit();
        self.alloc.destroy(self);
    }
};

fn matchesColumns(page: catalog.ColumnPage, alloc: Allocator, table: catalog.Table, conditions: []const catalog.Condition) !bool {
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
        const kind = (try table.column(condition.column)).type;
        const stored = try @import("lake_values.zig").comparisonValue(alloc, cell.value, kind);
        const order = try scalar.compare(stored, condition.value);
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
        stats: usize = 0,
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
            self.stats += 1;
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
        const data = try @import("../serverless/query/lake_parquet_rowgroup.zig").buildTestPlainI64ParquetObjectAlloc(alloc, &.{.{ .column_id = "amount", .values = values, .field_id = 1, .write_statistics = true, .page_rows = self.page_rows }});
        defer alloc.free(data);
        try self.populateData(alloc, count, data);
    }
    fn populateData(self: *TestLake, alloc: Allocator, count: usize, data: []const u8) !void {
        var client = self.memory.client();
        try client.makeBucket("bucket");
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
        if (self.source.delete_lease) |lease| lease.release() else if (self.source.prepared_deletes) |prepared| prepared.destroy(alloc);
        if (self.source.scanner.shared_reader) |reader| alloc.destroy(reader);
        self.inventory.deinit(alloc);
        self.memory.deinit();
    }
};

test "lake SQL late projection skips rejected page payloads and preserves alignment" {
    const a = std.testing.allocator;
    var predicates: [1024]i64 = @splat(0);
    @memset(predicates[768..], 1);
    var payload: [1024]i64 = undefined;
    for (&payload, 0..) |*value, index| value.* = @intCast(index);
    const data = try @import("../serverless/query/lake_parquet_rowgroup.zig").buildTestPlainI64ParquetObjectAlloc(a, &.{
        .{ .column_id = "amount", .values = &predicates, .page_rows = 256 },
        .{ .column_id = "payload", .values = &payload, .page_rows = 128 },
    });
    defer a.free(data);
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populateData(a, 1, data);
    defer lake.deinit(a);
    lake.table.columns = &.{ .{ .name = "amount", .path = "amount", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .integer } };
    const cursor = try openPinned(a, lake.table, .{ .fields = &.{"payload"}, .conditions = &.{.{ .column = "amount", .op = .eq, .value = .{ .integer = 1 } }}, .limit = 17 }, .{}, &lake.source);
    defer cursor.close(cursor.ptr);
    var total: usize = 0;
    while (true) {
        const page = try cursor.next(cursor.ptr, a, 17);
        defer page.deinit();
        for (page.rows) |row| {
            try std.testing.expectEqual(@as(i64, @intCast(768 + total)), row.value.object.get("payload").?.integer);
            total += 1;
        }
        if (page.after == null) break;
    }
    try std.testing.expectEqual(@as(usize, 256), total);
    const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
    try std.testing.expectEqual(@as(usize, 6), owner.stream.page_cursor.?.pages_decoded);
}

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

test "lake SQL pushed number predicates retain declared coercion" {
    const a = std.testing.allocator;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 1, &.{9007199254740993});
    defer lake.deinit(a);
    lake.table.columns = &.{.{ .name = "amount", .path = "amount", .type = .number }};
    const cursor = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .conditions = &.{.{ .column = "amount", .op = .eq, .value = .{ .float = 9007199254740992.0 } }}, .limit = 1 }, .{}, &lake.source);
    defer cursor.close(cursor.ptr);
    const page = try cursor.next(cursor.ptr, a, 1);
    defer page.deinit();
    const coerced = try @import("../sql/describe.zig").coerceAlloc(a, .{ .integer = 9007199254740993 }, .number);
    try std.testing.expectEqual(std.math.Order.eq, try scalar.compare(coerced, .{ .float = 9007199254740992.0 }));
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
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
    try std.testing.expect(cache.decoded.hits >= 2);
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
    const identities = try openPinned(alloc, lake.table, .{ .fields = &.{"_id"}, .limit = 10 }, .{}, &lake.source);
    defer identities.close(identities.ptr);
    const ids = try identities.next(identities.ptr, alloc, 10);
    defer ids.deinit();
    try std.testing.expectEqual(@as(usize, 5), ids.rows.len);
    const identity_owner: *Owner = @ptrCast(@alignCast(identities.ptr));
    try std.testing.expectEqual(@as(usize, 0), identity_owner.stream.page_cursor.?.pages_decoded);
    const more_ids = try identities.next(identities.ptr, alloc, 10);
    defer more_ids.deinit();
    try std.testing.expectEqual(@as(usize, 5), more_ids.rows.len);
    try std.testing.expectEqual(@as(usize, 0), identity_owner.stream.page_cursor.?.pages_decoded);
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
    var cache = @import("../serverless/query/lake_serving_cache.zig").Cache.init(alloc);
    defer cache.deinit();
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(alloc) };
    try lake.populate(alloc, 1, &.{ 1, 2, 3, 4, 5 });
    defer lake.deinit(alloc);
    lake.inventory.format = .iceberg;
    lake.inventory.files[0].row_count = 5;
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
    try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{});
    const cursor = try openPinned(alloc, lake.table, .{ .fields = &.{"amount"}, .limit = 1 }, .{}, &lake.source);
    defer cursor.close(cursor.ptr);
    try std.testing.expectEqual(@as(?u64, null), try cursor.count_rows.?(cursor.ptr));
    const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
    for ([_]i64{ 1, 3, 5 }) |expected| {
        const page = try cursor.next(cursor.ptr, alloc, 1);
        defer page.deinit();
        // Footer offsets live only in the current request/file plan; the
        // cached immutable deletion index never grows a prefix directory.
        try std.testing.expectEqualSlices(u64, &.{0}, owner.stream.position_starts);
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqual(expected, page.rows[0].value.object.get("amount").?.integer);
    }
    const end = try cursor.next(cursor.ptr, alloc, 1);
    defer end.deinit();
    try std.testing.expectEqual(@as(usize, 0), end.rows.len);
    const prepared = lake.source.prepared_deletes.?;
    const alias = try openPinned(alloc, lake.table, .{ .fields = &.{"amount"}, .limit = 8 }, .{}, &lake.source);
    defer alias.close(alias.ptr);
    const alias_page = try alias.next(alias.ptr, alloc, 8);
    defer alias_page.deinit();
    try std.testing.expectEqual(@as(usize, 3), alias_page.rows.len);
    try std.testing.expectEqual(prepared, lake.source.prepared_deletes.?);
    try std.testing.expectEqual(@as(usize, 3), prepared.decoded_pages);
    // A second source admission reuses membership without retaining a request.
    var reopened = lake.source;
    reopened.prepared_deletes = null;
    reopened.delete_lease = null;
    defer if (reopened.delete_lease) |lease| lease.release();
    const cached_cursor = try openPinned(alloc, lake.table, .{ .fields = &.{"amount"}, .limit = 8 }, .{}, &reopened);
    defer cached_cursor.close(cached_cursor.ptr);
    const reads = lake.meter.reads;
    const cached_page = try cached_cursor.next(cached_cursor.ptr, alloc, 8);
    defer cached_page.deinit();
    try std.testing.expectEqual(@as(usize, 3), cached_page.rows.len);
    try std.testing.expectEqual(prepared, reopened.prepared_deletes.?);
    try std.testing.expectEqual(reads, lake.meter.reads);
    try std.testing.expectEqual(@as(usize, 3), prepared.decoded_pages);
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

test "lake SQL continuation validation and arena ownership survive reopening" {
    const a = std.testing.allocator;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 1, &.{ 1, 2, 3 });
    defer lake.deinit(a);
    const initial = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 1 }, .{}, &lake.source);
    defer initial.close(initial.ptr);
    const page = try initial.next(initial.ptr, a, 1);
    defer page.deinit();
    const token = page.rows[0].id;
    const Failure = struct {
        fn openClose(failing: Allocator, source: *serving.ServingSource, table: catalog.Table, after: []const u8) !void {
            const cursor = try openPinned(failing, table, .{ .fields = &.{"amount"}, .after = after, .primary_key = after, .limit = 1 }, .{}, source);
            defer cursor.close(cursor.ptr);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failure.openClose, .{ &lake.source, lake.table, token });
    const resumed = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .after = token, .limit = 1 }, .{}, &lake.source);
    defer resumed.close(resumed.ptr);
    const next = try resumed.next(resumed.ptr, a, 1);
    defer next.deinit();
    try std.testing.expectEqual(@as(i64, 2), next.rows[0].value.object.get("amount").?.integer);
    const keyed = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .primary_key = token, .limit = 1 }, .{}, &lake.source);
    defer keyed.close(keyed.ptr);
    const exact = try keyed.next(keyed.ptr, a, 1);
    defer exact.deinit();
    try std.testing.expectEqualStrings(token, exact.rows[0].id);
    const bounded = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .before = token, .limit = 1 }, .{}, &lake.source);
    defer bounded.close(bounded.ptr);
    const empty = try bounded.next(bounded.ptr, a, 1);
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.rows.len);
    // Reopening after replacing the object creates a different inventory digest.
    var client = lake.memory.client();
    const bytes = try @import("../serverless/query/lake_parquet_rowgroup.zig").buildTestSingleColumnPlainI64ParquetObjectAlloc(a, "amount", &.{ 4, 5 });
    defer a.free(bytes);
    var put = try client.putObject("bucket", "events/0.parquet", bytes, .{});
    defer put.deinit(a);
    var changed = try @import("../serverless/external_source/mod.zig").planParquetPrefixInventoryFromObjectStorageAlloc(a, .{ .client = client, .bucket = "bucket", .prefix = "events", .source_id = "events", .source_uri = "s3://bucket/events", .schema_fingerprint = "v1" });
    defer changed.deinit(a);
    var source = lake.source;
    source.inventory = changed;
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, openPinned(a, lake.table, .{ .fields = &.{"amount"}, .after = token, .limit = 1 }, .{}, &source));
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, openPinned(a, lake.table, .{ .fields = &.{"amount"}, .before = token, .limit = 1 }, .{}, &source));
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, openPinned(a, lake.table, .{ .fields = &.{"amount"}, .after = "invalid", .limit = 1 }, .{}, &lake.source));
}

test "lake SQL Iceberg field identities govern renames name reuse and pruning" {
    const a = std.testing.allocator;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 1, &.{ 1, 2, 3 });
    defer lake.deinit(a);
    const schema = @import("../serverless/query/lake_schema.zig");
    var selected = try schema.icebergSchema(a, "{\"current-schema-id\":8,\"schemas\":[{\"schema-id\":8,\"fields\":[{\"id\":1,\"name\":\"renamed\",\"required\":true,\"type\":\"long\"},{\"id\":2,\"name\":\"amount\",\"required\":false,\"type\":\"long\"}]}]}", null);
    defer selected.deinit();
    lake.source.iceberg_schema = selected;
    lake.source.inventory.format = .iceberg;
    lake.table.external_base_source.?.binding.format = .iceberg;
    lake.table.columns = &.{ .{ .name = "renamed", .path = "renamed", .type = .integer, .nullable = false }, .{ .name = "amount", .path = "amount", .type = .integer } };
    const renamed = try openPinned(a, lake.table, .{ .fields = &.{"renamed"}, .conditions = &.{.{ .column = "renamed", .op = .gt, .value = .{ .integer = 1 } }}, .limit = 3 }, .{}, &lake.source);
    defer renamed.close(renamed.ptr);
    const page = try renamed.next(renamed.ptr, a, 3);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
    try std.testing.expectEqual(@as(i64, 2), (try page.rows[0].cell("renamed")).value.integer);
    const reused = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 3 }, .{}, &lake.source);
    defer reused.close(reused.ptr);
    const missing = try reused.next(reused.ptr, a, 3);
    defer missing.deinit();
    try std.testing.expectEqual(@as(usize, 3), missing.rows.len);
    for (missing.rows) |row| try std.testing.expect((try row.cell("amount")).sql_null);
    // Missing required field IDs fail rather than reading the reused name.
    const saved = selected.columns;
    selected.columns = &.{.{ .name = "amount", .kind = "integer", .required = true, .field_id = 2 }};
    lake.source.iceberg_schema = selected;
    defer selected.columns = saved;
    lake.table.columns = &.{.{ .name = "amount", .path = "amount", .type = .integer, .nullable = false }};
    const invalid = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 3 }, .{}, &lake.source);
    defer invalid.close(invalid.ptr);
    try std.testing.expectError(error.ExternalLakeSchemaMismatch, invalid.next(invalid.ptr, a, 3));
}

test "lake SQL dynamic scan filters prune groups before decoding and filter selected cells" {
    const a = std.testing.allocator;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 3, &.{ 1, 2, 3, 4, 5 });
    defer lake.deinit(a);
    const Filter = @import("../sql/dynamic_filter.zig").Filter;
    const filter = try Filter.create(a, &.{.{ .name = "amount", .type = .integer }}, 128);
    defer filter.close();
    try filter.add(&.{scalar.Datum.json(.{ .integer = 3 })});
    filter.sealed = true;
    // Dynamic evidence must be decoded even when the original projection
    // needs metadata identities only.
    const cursor = try openPinned(a, lake.table, .{ .fields = &.{"_id"}, .limit = 2 }, .{}, &lake.source);
    defer cursor.close(cursor.ptr);
    try std.testing.expect(try cursor.set_dynamic_filter.?(cursor.ptr, filter));
    try std.testing.expectEqual(@as(?u64, null), try cursor.count_rows.?(cursor.ptr));
    var seen: usize = 0;
    while (true) {
        const page = try cursor.next(cursor.ptr, a, 2);
        defer page.deinit();
        for (page.rows) |row| try std.testing.expectEqual(@as(i64, 3), row.value.object.get("amount").?.integer);
        seen += page.rows.len;
        if (page.after == null) break;
    }
    try std.testing.expectEqual(@as(usize, 3), seen);
    try std.testing.expect(!try cursor.set_dynamic_filter.?(cursor.ptr, filter));
    const missing = try Filter.create(a, &.{.{ .name = "amount", .type = .integer }}, 128);
    defer missing.close();
    try missing.add(&.{scalar.Datum.json(.{ .integer = 100 })});
    missing.sealed = true;
    const pruned = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 2 }, .{}, &lake.source);
    defer pruned.close(pruned.ptr);
    try std.testing.expect(try pruned.set_dynamic_filter.?(pruned.ptr, missing));
    const empty = try pruned.next(pruned.ptr, a, 2);
    defer empty.deinit();
    const owner: *Owner = @ptrCast(@alignCast(pruned.ptr));
    try std.testing.expectEqual(@as(usize, 0), empty.rows.len);
    try std.testing.expectEqual(@as(usize, 3), owner.stream.stats.groups_pruned);
    try std.testing.expectEqual(@as(usize, 0), owner.stream.stats.groups_decoded);
}

test "lake SQL review evolved nullable equality column absent from older data" {
    const a = std.testing.allocator;
    const parquet = @import("../serverless/query/lake_parquet_rowgroup.zig");
    const iceberg = @import("../serverless/query/lake_iceberg_snapshot.zig");
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 1, &.{ 1, 2, 3 });
    defer lake.deinit(a);
    lake.inventory.format = .iceberg;
    lake.inventory.files[0].row_count = 3;
    lake.inventory.files[0].data_sequence_number = 1;
    lake.inventory.files[0].partition_spec_id = 0;
    lake.source.inventory = lake.inventory;
    lake.table.external_base_source.?.binding.format = .iceberg;
    var selected = try @import("../serverless/query/lake_schema.zig").icebergSchema(a, "{\"current-schema-id\":8,\"schemas\":[{\"schema-id\":8,\"fields\":[{\"id\":1,\"name\":\"amount\",\"required\":true,\"type\":\"long\"},{\"id\":2,\"name\":\"added\",\"required\":false,\"type\":\"long\"}]}]}", null);
    defer selected.deinit();
    lake.source.iceberg_schema = selected;
    lake.table.columns = &.{ .{ .name = "amount", .path = "amount", .type = .integer, .nullable = false }, .{ .name = "added", .path = "added", .type = .integer } };
    const bytes = try parquet.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "added", .field_id = 2, .values = &.{2} }});
    defer a.free(bytes);
    var client = lake.memory.client();
    var put = try client.putObject("bucket", "deletes/equal.parquet", bytes, .{});
    defer put.deinit(a);
    var plan: iceberg.IcebergDeletePlan = .{ .files = try a.alloc(iceberg.IcebergDeleteFile, 1) };
    defer plan.deinit(a);
    plan.files[0] = .{ .content = .equality_deletes, .file_path = try a.dupe(u8, "s3://bucket/deletes/equal.parquet"), .file_format = try a.dupe(u8, "PARQUET"), .snapshot_id = 12, .data_sequence_number = 2, .file_sequence_number = 3, .record_count = 1, .file_size_in_bytes = bytes.len, .equality_ids = try a.dupe(i32, &.{2}), .equality_columns = try a.alloc([]u8, 1) };
    plan.files[0].equality_columns[0] = try a.dupe(u8, "added");
    lake.source.scanner.iceberg_delete_plan = plan;
    const cursor = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 8 }, .{}, &lake.source);
    defer cursor.close(cursor.ptr);
    const page = try cursor.next(cursor.ptr, a, 8);
    defer page.deinit();
    try std.testing.expectEqual(@as(usize, 3), page.rows.len);
}

test "lake SQL shared row group tasks and exact parallel reducers match serial groups" {
    const a = std.testing.allocator;
    const Cache = @import("../serverless/query/lake_serving_cache.zig").Cache;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    var values: [2048]i64 = undefined;
    for (&values, 0..) |*value, index| value.* = @intCast(index);
    try lake.populate(a, 4, &values);
    defer lake.deinit(a);
    var cache = Cache.init(a);
    defer cache.deinit();
    try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{ .io = std.testing.io });
    const parent = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 1024 }, .{}, &lake.source);
    defer parent.close(parent.ptr);
    const parts = (try parent.split_scan.?(parent.ptr, a, 4)).?;
    defer a.free(parts);
    defer for (parts) |part| part.close(part.ptr);
    try std.testing.expectEqual(@as(usize, 4), parts.len);
    const parent_owner: *Owner = @ptrCast(@alignCast(parent.ptr));
    try std.testing.expectEqual(@as(usize, 4), parent_owner.stream.stats.files_opened);
    var total: usize = 0;
    for (parts) |part| {
        var count: usize = 0;
        while (true) {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const page = try part.next_columns.?(part.ptr, arena.allocator(), 1024);
            count += page.selection.len;
            if (page.after == null) break;
        }
        const child: *Owner = @ptrCast(@alignCast(part.ptr));
        try std.testing.expectEqual(@as(usize, 0), child.stream.stats.files_opened);
        total += count;
    }
    try std.testing.expectEqual(values.len * 4, total);
    const Backend = struct {
        lake: *TestLake,
        fn resolve(raw: *anyopaque, _: Allocator, _: @import("../sql/ast.zig").Name, _: catalog.Action) !catalog.Table {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.lake.table;
        }
        fn scan(_: *anyopaque, _: Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedStatelessScan;
        }
        fn openScan(raw: *anyopaque, alloc: Allocator, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return try openPinned(alloc, table, request, .{}, &self.lake.source);
        }
        fn mutate(_: *anyopaque, _: Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = openScan, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    var fixture: Backend = .{ .lake = &lake };
    const compiler = @import("../sql/compiler.zig");
    const runtime = @import("../sql/runtime.zig");
    for ([_][]const u8{
        "SELECT amount % 7 AS bucket, SUM(amount) AS total, COUNT(*) AS n FROM events GROUP BY amount % 7 ORDER BY bucket",
        "SELECT SUM(amount), COUNT(*) FROM events",
        "SELECT amount % 7 AS bucket, COUNT(*) AS n FROM events GROUP BY amount % 7 LIMIT 3",
    }) |sql| {
        var compiled = try compiler.compile(a, sql, .{});
        defer compiled.deinit();
        var serial = try runtime.execute(a, fixture.backend(), &compiled, &.{}, .{});
        defer serial.deinit();
        var backend = fixture.backend();
        backend.execution_io = std.testing.io;
        var parallel = try runtime.execute(a, backend, &compiled, &.{}, .{});
        defer parallel.deinit();
        try std.testing.expectEqualDeep(serial.output.rows, parallel.output.rows);
        try std.testing.expectEqualDeep(serial.output.sql_nulls, parallel.output.sql_nulls);
    }
    // High-cardinality local reducers spill and merge under shard budgets.
    var high = try compiler.compile(a, "SELECT amount, SUM(amount) FROM events GROUP BY amount ORDER BY amount LIMIT 3", .{});
    defer high.deinit();
    var backend = fixture.backend();
    backend.execution_io = std.testing.io;
    var small = try runtime.execute(a, backend, &high, &.{}, .{ .retained_bytes = 1024 * 1024 });
    defer small.deinit();
    try std.testing.expectEqual(@as(usize, 3), small.output.rows.len);
    try std.testing.expectEqualStrings("0", small.output.rows[0][0].string);
    try std.testing.expectEqualStrings("8", small.output.rows[2][1].string);
}

test "lake SQL empty independent Parquet layouts retain schema and exhaust without partitions" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        @embedFile("../serverless/query/testdata/pyarrow_empty_no_groups.parquet"),
        @embedFile("../serverless/query/testdata/pyarrow_empty_row_group.parquet"),
    }) |data| {
        var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
        try lake.populateData(a, 1, data);
        defer lake.deinit(a);
        var cache = @import("../serverless/query/lake_serving_cache.zig").Cache.init(a);
        defer cache.deinit();
        try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{ .io = std.testing.io });
        const parent = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 1024 }, .{}, &lake.source);
        defer parent.close(parent.ptr);
        try std.testing.expect((try parent.split_scan.?(parent.ptr, a, 4)) == null);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const page = try parent.next_columns.?(parent.ptr, arena.allocator(), 1024);
        try std.testing.expectEqual(@as(usize, 0), page.selection.len);
        try std.testing.expect(page.after == null);
        const count = try openPinned(a, lake.table, .{ .fields = &.{}, .limit = 1 }, .{}, &lake.source);
        defer count.close(count.ptr);
        try std.testing.expectEqual(@as(?u64, 0), try count.count_rows.?(count.ptr));
    }
}

test "lake SQL decoded pages reuse semantic identity across budgets and projection widths" {
    const a = std.testing.allocator;
    const data = try @import("../serverless/query/lake_parquet_rowgroup.zig").buildTestPlainI64ParquetObjectAlloc(a, &.{
        .{ .column_id = "amount", .values = &.{ 1, 2, 3 }, .field_id = 1 },
        .{ .column_id = "payload", .values = &.{ 4, 5, 6 }, .field_id = 2 },
    });
    defer a.free(data);
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populateData(a, 1, data);
    defer lake.deinit(a);
    lake.table.columns = &.{ .{ .name = "amount", .path = "amount", .type = .integer }, .{ .name = "payload", .path = "payload", .type = .integer } };
    var cache = @import("../serverless/query/lake_serving_cache.zig").Cache.init(a);
    defer cache.deinit();
    try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{});
    for (0..4) |iteration| {
        const cursor = try openPinned(a, lake.table, .{ .fields = if (iteration == 2) &.{ "amount", "payload" } else &.{"amount"}, .limit = 3 }, .{}, &lake.source);
        defer cursor.close(cursor.ptr);
        const owner: *Owner = @ptrCast(@alignCast(cursor.ptr));
        if (iteration == 0) {
            owner.stream.limits.max_input_bytes = 2 * 1024 * 1024;
            owner.stream.limits.max_decoded_bytes = 2 * 1024 * 1024;
        }
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        if (iteration == 3) {
            owner.stream.limits.max_decoded_bytes = 100;
            try std.testing.expectError(error.ParquetRowGroupTooLarge, cursor.next_columns.?(cursor.ptr, arena.allocator(), 3));
            continue;
        }
        const page = try cursor.next_columns.?(cursor.ptr, arena.allocator(), 3);
        try std.testing.expectEqual(@as(usize, 3), page.selection.len);
        try std.testing.expectEqual(@as(usize, if (iteration == 1) 0 else 1), owner.stream.page_cursor.?.pages_decoded);
        try std.testing.expectEqual(@as(i64, 2), (try page.cell(arena.allocator(), 1, "amount")).value.integer);
    }
}

test "lake SQL shared tasks cover single file row groups exactly once" {
    const a = std.testing.allocator;
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populateData(a, 1, @embedFile("../serverless/query/testdata/pyarrow_plain_nullable_snappy.parquet"));
    defer lake.deinit(a);
    var cache = @import("../serverless/query/lake_serving_cache.zig").Cache.init(a);
    defer cache.deinit();
    try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{});
    const parent = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 1024 }, .{}, &lake.source);
    defer parent.close(parent.ptr);
    const parts = (try parent.split_scan.?(parent.ptr, a, 4)).?;
    defer a.free(parts);
    defer for (parts) |part| part.close(part.ptr);
    const owner: *Owner = @ptrCast(@alignCast(parent.ptr));
    const work = owner.stream.work.?;
    try std.testing.expect(work.units.len > parts.len);
    try std.testing.expectEqual(@as(usize, 1), owner.stream.stats.files_opened);
    for (work.units[1..], work.units[0 .. work.units.len - 1]) |later, earlier| try std.testing.expect(later.bytes <= earlier.bytes);
    var row_count: usize = 0;
    for (parts) |part| while (true) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const page = try part.next_columns.?(part.ptr, arena.allocator(), 128);
        row_count += page.selection.len;
        if (page.after == null) break;
    };
    for (parts) |part| {
        const child: *Owner = @ptrCast(@alignCast(part.ptr));
        try std.testing.expectEqual(@as(usize, 0), child.stream.stats.files_opened);
    }
    try std.testing.expect(work.next.load(.monotonic) >= work.units.len);
    // Parent remains readable after children consume the queue.
    const parent_count = (try parent.count_rows.?(parent.ptr)).?;
    try std.testing.expectEqual(parent_count, row_count);
}

test "lake SQL lazy Iceberg versions stat only surviving files once" {
    const a = std.testing.allocator;
    const external = @import("../serverless/external_source/types.zig");
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 2, &.{ 1, 2, 3 });
    defer lake.deinit(a);
    var schema = try @import("../serverless/query/lake_schema.zig").icebergSchema(a, "{\"current-schema-id\":7,\"schemas\":[{\"schema-id\":7,\"fields\":[{\"id\":1,\"name\":\"amount\",\"required\":true,\"type\":\"long\"}]}]}", null);
    defer schema.deinit();
    lake.source.iceberg_schema = schema;
    lake.inventory.format = .iceberg;
    lake.source.inventory = lake.inventory;
    lake.table.external_base_source.?.binding.format = .iceberg;
    lake.source.lazy_versions = true;
    lake.source.pinned_files = try a.alloc(bool, 2);
    defer a.free(lake.source.pinned_files);
    @memset(lake.source.pinned_files, false);
    var zero: [8]u8 = @splat(0);
    lake.inventory.files[0].lower_bounds = try external.FieldMetric.cloneAll(a, &.{.{ .field_id = 1, .value = &zero }});
    lake.inventory.files[0].upper_bounds = try external.FieldMetric.cloneAll(a, &.{.{ .field_id = 1, .value = &zero }});
    // This file must be pruned before any HEAD or footer request.
    var client = lake.memory.client();
    try client.deleteObject("bucket", "events/0.parquet", .{});
    for (0..2) |_| {
        const cursor = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .conditions = &.{.{ .column = "amount", .op = .eq, .value = .{ .integer = 2 } }}, .limit = 8 }, .{}, &lake.source);
        defer cursor.close(cursor.ptr);
        const page = try cursor.next(cursor.ptr, a, 8);
        defer page.deinit();
        try std.testing.expectEqual(@as(usize, 1), page.rows.len);
        try std.testing.expectEqual(@as(i64, 2), page.rows[0].value.object.get("amount").?.integer);
        try std.testing.expectEqual(@as(usize, 1), lake.meter.stats);
        try std.testing.expect(!lake.source.pinned_files[0] and lake.source.pinned_files[1]);
    }
}

test "lake SQL ordered splits preserve serial identity ordering" {
    const a = std.testing.allocator;
    var cache = @import("../serverless/query/lake_serving_cache.zig").Cache.init(a);
    defer cache.deinit();
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 3, &.{ 1, 2, 3 });
    defer lake.deinit(a);
    try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{ .io = std.testing.io });
    var ids: std.ArrayList([]const u8) = .empty;
    defer {
        for (ids.items) |id| a.free(id);
        ids.deinit(a);
    }
    {
        const serial = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 4 }, .{}, &lake.source);
        defer serial.close(serial.ptr);
        while (true) {
            const page = try serial.next(serial.ptr, a, 4);
            defer page.deinit();
            for (page.rows) |row| try ids.append(a, try a.dupe(u8, row.id));
            if (page.after == null) break;
        }
    }
    const parent = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 4 }, .{}, &lake.source);
    defer parent.close(parent.ptr);
    const children = (try parent.split_ordered.?(parent.ptr, a, 2)).?;
    defer {
        for (children) |child| child.close(child.ptr);
        a.free(children);
    }
    var seen: usize = 0;
    for (children) |child| while (true) {
        const page = try child.next(child.ptr, a, 2);
        defer page.deinit();
        for (page.rows) |row| {
            try std.testing.expectEqualStrings(ids.items[seen], row.id);
            seen += 1;
        }
        if (page.after == null) break;
    };
    try std.testing.expectEqual(ids.items.len, seen);
}

test "lake SQL ordered file ranges defer later footer errors" {
    const a = std.testing.allocator;
    var cache = @import("../serverless/query/lake_serving_cache.zig").Cache.init(a);
    defer cache.deinit();
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 2, &.{ 1, 2, 3 });
    defer lake.deinit(a);
    try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{});
    const parent = try openPinned(a, lake.table, .{ .fields = &.{"amount"}, .limit = 8 }, .{}, &lake.source);
    defer parent.close(parent.ptr);
    const owner: *Owner = @ptrCast(@alignCast(parent.ptr));
    const later = lake.source.inventory.files[owner.stream.files[1]].object_uri;
    const location = try @import("../serverless/query/lake_range_io.zig").objectLocationForUri(later);
    var client = lake.memory.client();
    try client.deleteObject(location.bucket, location.key, .{});
    const children = (try parent.split_ordered.?(parent.ptr, a, 2)).?;
    defer {
        for (children) |child| child.close(child.ptr);
        a.free(children);
    }
    const first = try children[0].next(children[0].ptr, a, 8);
    defer first.deinit();
    try std.testing.expectEqual(@as(usize, 3), first.rows.len);
    try std.testing.expectEqual(@as(i64, 1), first.rows[0].value.object.get("amount").?.integer);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, children[1].next(children[1].ptr, a, 8));
}

test "lake SQL lazy ordered range ownership unwinds allocation failures" {
    const a = std.testing.allocator;
    var cache = @import("../serverless/query/lake_serving_cache.zig").Cache.init(a);
    defer cache.deinit();
    var lake: TestLake = .{ .memory = TestLake.storage.MemoryObjectStorage.init(a) };
    try lake.populate(a, 2, &.{ 1, 2, 3 });
    defer lake.deinit(a);
    try lake.source.attachCache(&cache, lake.table.external_base_source.?.binding, .{});
    const Sweep = struct {
        fn run(failing: Allocator, source: *serving.ServingSource, table: catalog.Table) !void {
            const parent = try openPinned(failing, table, .{ .fields = &.{"amount"}, .limit = 8 }, .{}, source);
            defer parent.close(parent.ptr);
            const children = (try parent.split_ordered.?(parent.ptr, failing, 2)).?;
            defer {
                for (children) |child| child.close(child.ptr);
                failing.free(children);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(a, Sweep.run, .{ &lake.source, lake.table });
}
