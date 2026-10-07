// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Leased native row-index consumption. Seek candidates in bounded windows,
//! hydrate physical vectors, then apply deletes and residuals before delivery.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const operation = local.api_operation;
const server_api = @import("http_server.zig");
const ordered = @import("lake_index_ordered_rows.zig");
const native_rows = @import("lake_index_native_rows.zig");
const Store = @import("lake_index_store.zig").Store;
const A = std.mem.Allocator;
const Owner = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    source: *local.serverless_query_lake_serving.ServingSource,
    store: Store,
    artifacts: @import("../serverless/artifacts/store.zig").ArtifactStore,
    lease: ?*@import("lake_index_reader_lease.zig").Handle = null,
    reader: ordered.Reader = undefined,
    reader_open: bool = false,
    metadata: ?@import("lake_index_decoded_metadata.zig").Owned(ordered.Root) = null,
    table: catalog.Table,
    request: catalog.Scan,
    context: operation.RequestContext,
    read_context: local.serverless_query_lake_read_context.Context,
    child: ?catalog.Cursor = null,
    child_exhausted: bool = false,
    window: std.heap.ArenaAllocator,
    cover_arena: std.heap.ArenaAllocator,
    cover_candidates: []const ordered.Reader.Entry = &.{},
    cover_candidate_offset: usize = 0,
    cursor_header: ?[local.storage_db_relational_row_cursor.identity_len]u8 = null,
    ordered_rows: []const catalog.Row = &.{},
    ordered_offset: usize = 0,
    ordered_store: ?local.sql_typed_store.Store = null,
    ordered_batch: local.sql_execution_batch.Batch = undefined,
    ordered_selection: []const usize = &.{},
    ordered_keys: []const []const u8 = &.{},
    ordered_column_offset: usize = 0,
    started: bool = false,
    dynamic: ?*const local.sql_dynamic_filter.Filter = null,
    covered: bool = false,
    covered_entries: []const ordered.Reader.Entry = &.{},
    covered_blocks: []const struct { block: local.sql_spill.ColumnarBlock, row: u16 } = &.{},
    covered_selection: []const usize = &.{},
    covered_offset: usize = 0,
    covered_batch: local.sql_execution_batch.Batch = undefined,
    fn close(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.child) |child| child.close(child.ptr);
        if (self.reader_open) self.reader.deinit();
        if (self.metadata) |owned| owned.release();
        if (self.lease) |lease| lease.deinit();
        if (self.ordered_store) |*store| store.deinit();
        self.window.deinit();
        self.cover_arena.deinit();
        self.arena.deinit();
        self.store.deinit();
        self.a.destroy(self);
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const Owner = @ptrCast(@alignCast(raw));
        self.read_context.ensureActive() catch return true;
        return false;
    }
    fn prepare(self: *Owner) !bool {
        try self.read_context.ensureActive();
        self.started = true;
        if (self.child != null and !self.child_exhausted) return true;
        if (self.child) |child| child.close(child.ptr);
        self.child = null;
        _ = self.window.reset(.retain_capacity);
        const refs = try self.reader.next(self.window.allocator(), 1024);
        if (refs.len == 0) return false;
        var request = self.request;
        request.index_equality = null;
        request.index_range = null;
        request.row_refs = refs;
        var context = self.context;
        context.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
        self.child = try local.sql_lake_cursor.openPinned(self.a, self.table, request, context, self.source);
        self.child_exhausted = false;
        if (self.dynamic) |filter| {
            if (!try self.child.?.set_dynamic_filter.?(self.child.?.ptr, filter)) return error.InvalidSqlBackendResponse;
        }
        return true;
    }
    fn setDynamicFilter(raw: *anyopaque, filter: *const local.sql_dynamic_filter.Filter) !bool {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.started or self.dynamic != null) return false;
        if (filter.failed or !filter.sealed) return error.InvalidSqlBackendResponse;
        for (filter.columns) |column| if (!covers(self.reader.root.cover, column.name)) {
            self.covered = false;
        };
        self.dynamic = filter;
        return true;
    }
    fn next(raw: *anyopaque, a: A, limit: u32) !catalog.Page {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.covered) return self.nextCoveredRows(a, limit);
        if (self.cursor_header != null) return self.nextOrdered(a, limit);
        while (try self.prepare()) {
            var page = try self.child.?.next(self.child.?.ptr, a, limit);
            self.child_exhausted = page.after == null;
            if (page.rows.len == 0) {
                page.deinit();
                continue;
            }
            if (!self.reader.exhausted and page.after == null) page.after = page.rows[page.rows.len - 1].id;
            try self.read_context.ensureActive();
            return page;
        }
        return .{ .rows = &.{} };
    }
    fn nextOrdered(self: *Owner, _: A, limit: u32) !catalog.Page {
        if (limit == 0) return error.InvalidRelationalRowsRequest;
        try self.read_context.ensureActive();
        self.started = true;
        while (self.ordered_offset == self.ordered_rows.len) {
            _ = self.window.reset(.retain_capacity);
            const a = self.window.allocator();
            const entries = try self.reader.nextEntries(a, 256);
            if (entries.len == 0) return .{ .rows = &.{} };
            const refs = try a.alloc(local.storage_rowsource_types.RowRef, entries.len);
            var ranks: std.StringHashMapUnmanaged(usize) = .empty;
            const hydrated = try a.alloc(?catalog.Row, entries.len);
            @memset(hydrated, null);
            for (entries, refs, 0..) |entry, *ref, rank| {
                ref.* = entry.ref;
                try ranks.put(a, try local.storage_rowsource_identity.allocId(a, entry.ref), rank);
            }
            var request = self.request;
            request.index_equality = null;
            request.index_range = null;
            request.row_refs = refs;
            request.primary_order = false;
            request.after = null;
            var context = self.context;
            context.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
            const child = try local.sql_lake_cursor.openPinned(self.a, self.table, request, context, self.source);
            defer child.close(child.ptr);
            if (self.dynamic) |filter| {
                if (!try child.set_dynamic_filter.?(child.ptr, filter)) return error.InvalidSqlBackendResponse;
            }
            while (true) {
                const page = try child.next(child.ptr, self.a, 256);
                defer page.deinit();
                for (page.rows) |row| {
                    const rank = ranks.get(row.id) orelse return error.InvalidNativeLakeRowIndex;
                    if (hydrated[rank] != null) return error.InvalidNativeLakeRowIndex;
                    var owned = row;
                    owned.id = try a.dupe(u8, row.id);
                    owned.value = try local.api_json_helpers.cloneJsonValue(a, row.value);
                    owned.sql_nulls = if (row.sql_nulls) |nulls| try a.dupe(bool, nulls) else null;
                    owned.index_cursor = try local.storage_db_relational_row_cursor.encode(a, self.cursor_header.?, entries[rank].key);
                    hydrated[rank] = owned;
                }
                if (page.after == null) break;
            }
            var result: std.ArrayList(catalog.Row) = .empty;
            for (hydrated) |row| if (row) |present| try result.append(a, present);
            self.ordered_rows = result.items;
            self.ordered_offset = 0;
        }
        const end = self.ordered_offset + @min(limit, self.ordered_rows.len - self.ordered_offset);
        const selected = self.ordered_rows[self.ordered_offset..end];
        self.ordered_offset = end;
        try self.read_context.ensureActive();
        return .{ .rows = selected, .after = if (self.reader.exhausted and end == self.ordered_rows.len) null else selected[selected.len - 1].index_cursor };
    }
    fn coveredCell(raw: *anyopaque, _: A, row: usize, column: usize) !local.sql_scalar.Datum {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const covered = self.covered_blocks[row];
        return covered.block.cell(covered.row, column);
    }
    fn coveredIdentity(raw: *anyopaque, row: usize, column: usize) !?u64 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        const covered = self.covered_blocks[row];
        return covered.block.dictionaryIdentity(covered.row, column, false);
    }
    fn coveredDictionary(raw: *anyopaque, a: A, column: usize) !?local.sql_execution_batch.Batch {
        const self: *Owner = @ptrCast(@alignCast(raw));
        // Each covering window belongs to exactly one authenticated block.
        const dictionary = (try self.covered_blocks[0].block.dictionaryColumn(a, column, false)) orelse return null;
        const indices = try a.alloc(u32, self.covered_blocks.len);
        for (indices, self.covered_blocks) |*id, covered| id.* = dictionary.dictionary.indices[covered.row];
        return .{ .dictionary = .{ .values = dictionary.dictionary.values, .indices = indices } };
    }
    fn nextCovered(self: *Owner, limit: u32) !catalog.ColumnPage {
        if (limit == 0) return error.InvalidRelationalRowsRequest;
        try self.read_context.ensureActive();
        self.started = true;
        while (self.covered_offset == self.covered_selection.len) {
            if (self.cover_candidate_offset == self.cover_candidates.len) {
                _ = self.window.reset(.retain_capacity);
                self.cover_candidates = try self.reader.nextEntries(self.window.allocator(), 256);
                self.cover_candidate_offset = 0;
                if (self.cover_candidates.len == 0) return .{ .selection = &.{} };
            }
            _ = self.cover_arena.reset(.retain_capacity);
            const a = self.cover_arena.allocator();
            const begin = self.cover_candidate_offset;
            const cover = self.cover_candidates[begin].cover orelse return error.InvalidNativeLakeRowIndex;
            var end = begin;
            while (end < self.cover_candidates.len) : (end += 1) {
                const candidate = self.cover_candidates[end].cover orelse return error.InvalidNativeLakeRowIndex;
                if (!std.mem.eql(u8, cover.block.artifact_id, candidate.block.artifact_id)) break;
            }
            self.covered_entries = self.cover_candidates[begin..end];
            self.cover_candidate_offset = end;
            try @import("../serverless/artifacts/store.zig").chargeReadBudget(&self.reader.remaining_reads, cover.block.byte_len);
            const bytes = try @import("lake_index_aggregate_artifact.zig").readArtifact(a, self.artifacts, cover.block, self.reader.pages.cancellation, self.reader.cached);
            const block = try local.sql_spill.decodeColumnarBlockInArena(a, bytes, @import("lake_index_aggregate_artifact.zig").max_block_bytes);
            if (block.values.len != self.reader.root.cover.len or block.keys.len != 1) return error.InvalidNativeLakeRowIndex;
            const covered = try a.alloc(@typeInfo(@TypeOf(self.covered_blocks)).pointer.child, self.covered_entries.len);
            for (self.covered_entries, covered) |entry, *position| {
                const candidate = entry.cover.?;
                if (candidate.block.byte_len != cover.block.byte_len or candidate.row >= block.count()) return error.InvalidNativeLakeRowIndex;
                const key = try block.keyCell(candidate.row, 0);
                if (key.sql_null or key.value != .string or !std.mem.eql(u8, key.value.string, entry.key)) return error.InvalidNativeLakeRowIndex;
                position.* = .{ .block = block, .row = candidate.row };
            }
            self.covered_blocks = covered;
            self.covered_batch = .{ .reader = .{ .ptr = self, .read = coveredCell, .read_identity = coveredIdentity, .read_dictionary = coveredDictionary, .count = covered.len, .width = self.reader.root.cover.len } };
            var selected: std.ArrayList(usize) = .empty;
            const mask = try a.alloc(bool, covered.len);
            @memset(mask, true);
            // Evaluate each predicate once per dictionary identity. Surviving
            // lanes alone participate, preserving short-circuit error behavior.
            for (self.request.conditions) |condition| {
                var results: std.AutoHashMapUnmanaged(u64, bool) = .empty;
                defer results.deinit(a);
                var column: ?usize = null;
                for (self.reader.root.cover, 0..) |name, i| if (std.mem.eql(u8, name, condition.column)) {
                    column = i;
                    break;
                };
                for (mask, 0..) |*keep, row| {
                    if (!keep.*) continue;
                    const one: catalog.ColumnPage = .{ .native = .{ .values = &self.covered_batch, .names = self.reader.root.cover }, .selection = &.{row} };
                    const identity = if (column) |c| try self.covered_batch.dictionaryIdentity(row, c) else null;
                    if (identity) |id| {
                        if (results.get(id)) |cached| {
                            keep.* = cached;
                            continue;
                        }
                    }
                    keep.* = try local.sql_lake_cursor.matchesColumns(one, a, self.table, &.{condition});
                    if (identity) |id| try results.put(a, id, keep.*);
                }
            }
            for (mask, 0..) |keep, row| {
                if (!keep) continue;
                const one: catalog.ColumnPage = .{ .native = .{ .values = &self.covered_batch, .names = self.reader.root.cover }, .selection = &.{row} };
                if (self.dynamic) |filter| {
                    const values = try a.alloc(local.sql_scalar.Datum, filter.columns.len);
                    defer a.free(values);
                    for (values, filter.columns) |*value, column| {
                        const cell = try one.cell(a, 0, column.name);
                        value.* = .{ .value = try local.sql_lake_values.comparisonValue(a, cell.value, column.type), .sql_null = cell.sql_null };
                    }
                    if (!try filter.contains(values)) continue;
                }
                try selected.append(a, row);
            }
            self.covered_selection = selected.items;
            self.covered_offset = 0;
        }
        const end = self.covered_offset + @min(limit, self.covered_selection.len - self.covered_offset);
        const selected = self.covered_selection[self.covered_offset..end];
        self.covered_offset = end;
        const last = self.covered_entries[selected[selected.len - 1]];
        const a = self.window.allocator();
        const after = if (self.reader.exhausted and self.cover_candidate_offset == self.cover_candidates.len and end == self.covered_selection.len) null else if (self.cursor_header) |header| try local.storage_db_relational_row_cursor.encode(a, header, last.key) else try local.storage_rowsource_identity.allocId(a, last.ref);
        try self.read_context.ensureActive();
        return .{ .native = .{ .values = &self.covered_batch, .names = self.reader.root.cover }, .selection = selected, .after = after };
    }
    fn nextCoveredRows(self: *Owner, a: A, limit: u32) !catalog.Page {
        const page = try self.nextCovered(limit);
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const out = arena.allocator();
        const result = try out.alloc(catalog.Row, page.selection.len);
        for (result, page.selection, 0..) |*row, physical, selected| {
            var object: std.json.ObjectMap = .empty;
            const nulls = try out.alloc(bool, self.request.fields.len);
            for (self.request.fields, nulls) |field, *flag| {
                const cell = try page.cell(out, selected, field);
                try object.put(out, field, try local.api_json_helpers.cloneJsonValue(out, cell.value));
                flag.* = cell.sql_null;
            }
            const entry = self.covered_entries[physical];
            row.* = .{ .id = try local.storage_rowsource_identity.allocId(out, entry.ref), .version = 0, .value = .{ .object = object }, .sql_nulls = nulls, .index_cursor = if (self.cursor_header) |header| try local.storage_db_relational_row_cursor.encode(out, header, entry.key) else null };
        }
        return .{ .rows = result, .owned_arena = arena, .after = if (page.after) |after| try out.dupe(u8, after) else null };
    }
    /// Gather physical column pages into a bounded typed window, then expose
    /// the index permutation. No document JSON round trip or global sort.
    fn nextOrderedColumns(self: *Owner, limit: u32) !catalog.ColumnPage {
        if (limit == 0) return error.InvalidRelationalRowsRequest;
        try self.read_context.ensureActive();
        self.started = true;
        while (self.ordered_column_offset == self.ordered_selection.len) {
            if (self.ordered_store) |*store| store.deinit();
            self.ordered_store = null;
            _ = self.window.reset(.retain_capacity);
            const a = self.window.allocator();
            const entries = try self.reader.nextEntries(a, 256);
            if (entries.len == 0) return .{ .selection = &.{} };
            const refs = try a.alloc(local.storage_rowsource_types.RowRef, entries.len);
            const positions = try a.alloc(?usize, entries.len);
            @memset(positions, null);
            var ranks: std.StringHashMapUnmanaged(usize) = .empty;
            for (entries, refs, 0..) |entry, *ref, rank| {
                ref.* = entry.ref;
                try ranks.put(a, try local.storage_rowsource_identity.allocId(a, entry.ref), rank);
            }
            var request = self.request;
            request.index_range = null;
            request.index_equality = null;
            request.order = &.{};
            request.row_refs = refs;
            request.after = null;
            var context = self.context;
            context.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
            const child = try local.sql_lake_cursor.openPinned(self.a, self.table, request, context, self.source);
            defer child.close(child.ptr);
            if (self.dynamic) |filter| if (!try child.set_dynamic_filter.?(child.ptr, filter)) return error.InvalidSqlBackendResponse;
            self.ordered_store = local.sql_typed_store.Store.init(self.a);
            while (true) {
                var scratch = std.heap.ArenaAllocator.init(self.a);
                defer scratch.deinit();
                const sa = scratch.allocator();
                const page = try child.next_columns.?(child.ptr, sa, 256);
                try page.validate();
                const cells = try sa.alloc(local.sql_scalar.Datum, request.fields.len);
                for (page.selection, 0..) |physical, row| {
                    if (physical >= page.batch.row_refs.len) return error.InvalidNativeLakeRowIndex;
                    const rank = ranks.get(try local.storage_rowsource_identity.allocId(sa, page.batch.row_refs[physical])) orelse return error.InvalidNativeLakeRowIndex;
                    if (positions[rank] != null) return error.InvalidNativeLakeRowIndex;
                    for (request.fields, cells) |field, *cell| {
                        const value = try page.cell(sa, row, field);
                        cell.* = .{ .value = value.value, .sql_null = value.sql_null, .patterns = value.patterns };
                    }
                    positions[rank] = try self.ordered_store.?.append(cells);
                }
                if (page.after == null) break;
            }
            var selection: std.ArrayList(usize) = .empty;
            var keys: std.ArrayList([]const u8) = .empty;
            for (positions, entries) |position, entry| if (position) |index| {
                try selection.append(a, index);
                try keys.append(a, entry.key);
            };
            self.ordered_selection = selection.items;
            self.ordered_keys = keys.items;
            self.ordered_column_offset = 0;
            self.ordered_batch = .{ .retained = .{ .store = &self.ordered_store.?, .count = self.ordered_store.?.len } };
        }
        const begin = self.ordered_column_offset;
        const end = begin + @min(limit, self.ordered_selection.len - begin);
        self.ordered_column_offset = end;
        const after = if (self.reader.exhausted and end == self.ordered_selection.len) null else try local.storage_db_relational_row_cursor.encode(self.window.allocator(), self.cursor_header.?, self.ordered_keys[end - 1]);
        try self.read_context.ensureActive();
        return .{ .native = .{ .values = &self.ordered_batch, .names = self.request.fields }, .selection = self.ordered_selection[begin..end], .after = after };
    }
    fn nextColumns(raw: *anyopaque, a: A, limit: u32) !catalog.ColumnPage {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.covered) return self.nextCovered(limit);
        if (self.request.order.len != 0 and self.cursor_header != null) return self.nextOrderedColumns(limit);
        while (try self.prepare()) {
            var page = try self.child.?.next_columns.?(self.child.?.ptr, a, limit);
            self.child_exhausted = page.after == null;
            if (page.selection.len == 0) continue;
            if (!self.reader.exhausted and page.after == null) {
                const last = page.selection[page.selection.len - 1];
                if (last >= page.batch.row_refs.len) return error.InvalidSqlBackendResponse;
                page.after = try local.storage_rowsource_identity.allocId(self.window.allocator(), page.batch.row_refs[last]);
            }
            try self.read_context.ensureActive();
            return page;
        }
        return .{ .selection = &.{} };
    }
};

pub fn openPinned(a: A, server: *server_api.ApiHttpServer, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, source: *local.serverless_query_lake_serving.ServingSource) !catalog.Cursor {
    return (try openWithPolicy(a, server, table, request, context, source, .required, null)) orelse error.ExternalLakeIndexUnavailable;
}

pub fn tryOpenAuto(a: A, server: *server_api.ApiHttpServer, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, source: *local.serverless_query_lake_serving.ServingSource) !?catalog.Cursor {
    if (request.index_range != null or request.primary_order or request.row_refs != null or request.primary_key != null) return null;
    if (request.index_equality != null) return openWithPolicy(a, server, table, request, context, source, .automatic, null);
    const definitions = table.external_indexes orelse return null;
    if (definitions.schema_json.len == 0 or server.source.lakeIndexLifecycleAuthority(context) == null) return null;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, definitions.schema_json);
    defer parsed.deinit(a);
    const indexes = (try parsed.relationalIndexDefinitions(ca)) orelse return null;
    // Enumerate usable paths, then cost authenticated range cardinalities
    // after partial-predicate proof and covering checks.
    var best: ?catalog.Cursor = null;
    errdefer if (best) |cursor| cursor.close(cursor.ptr);
    var best_cost: u64 = std.math.maxInt(u64);
    for (indexes) |index| {
        const indexed = (try chooseAccess(ca, &.{index}, request)) orelse continue;
        var cost: u64 = 0;
        const cursor = (try openWithPolicy(a, server, table, indexed, context, source, .automatic, &cost)) orelse continue;
        if (cost < best_cost) {
            if (best) |prior| prior.close(prior.ptr);
            best = cursor;
            best_cost = cost;
        } else cursor.close(cursor.ptr);
    }
    return best;
}

fn orderedBy(index: local.storage_relational_index.RelationalIndexDefinition, request: catalog.Scan, equal_count: usize) bool {
    if (request.order.len == 0 or request.order.len > index.keys.len - equal_count) return false;
    for (request.order, index.keys[equal_count..][0..request.order.len]) |order, key| {
        if (key.expression_json != null or key.collation != null or !std.mem.eql(u8, key.column, order.column) or (key.direction == .desc) != order.descending) return false;
        const nulls_first = switch (key.nulls) {
            .default => key.direction == .desc,
            .first => true,
            .last => false,
        };
        if (nulls_first != order.nulls_first) return false;
    }
    return true;
}

fn chooseAccess(a: A, indexes: []const local.storage_relational_index.RelationalIndexDefinition, request: catalog.Scan) !?catalog.Scan {
    var best: ?catalog.Scan = null;
    var best_score: usize = 0;
    for (indexes) |index| {
        var values: std.ArrayList(std.json.Value) = .empty;
        for (index.keys) |key| {
            if (key.expression_json != null or key.collation != null) break;
            const condition = for (request.conditions) |condition| {
                if (condition.op == .eq and condition.value != .null and std.mem.eql(u8, condition.column, key.column)) break condition;
            } else break;
            try values.append(a, condition.value);
        }
        const ordered_access = orderedBy(index, request, values.items.len);
        var lower: ?catalog.Scan.IndexRange.Bound = if (values.items.len != 0) .{ .values = values.items } else null;
        var upper = lower;
        var ranged = false;
        if (values.items.len < index.keys.len) {
            const key = index.keys[values.items.len];
            if (key.expression_json == null and key.collation == null) for (request.conditions) |condition| {
                if (!std.mem.eql(u8, key.column, condition.column) or condition.value == .null) continue;
                const greater = condition.op == .gt or condition.op == .gte;
                const smaller = condition.op == .lt or condition.op == .lte;
                if (!greater and !smaller) continue;
                const bound_values = try a.alloc(std.json.Value, values.items.len + 1);
                @memcpy(bound_values[0..values.items.len], values.items);
                bound_values[values.items.len] = condition.value;
                const bound: catalog.Scan.IndexRange.Bound = .{ .values = bound_values, .inclusive = condition.op == .gte or condition.op == .lte };
                if (greater != (key.direction == .desc)) lower = bound else upper = bound;
                ranged = true;
            };
        }
        if (values.items.len == 0 and !ranged and !ordered_access) continue;
        const score = values.items.len * 4 + @as(usize, @intFromBool(ranged)) * 2 + @intFromBool(ordered_access);
        if (best != null and score <= best_score) continue;
        var indexed = request;
        if (ranged or ordered_access) indexed.index_range = .{ .name = index.name, .lower = lower, .upper = upper } else indexed.index_equality = .{ .name = index.name, .values = values.items };
        best = indexed;
        best_score = score;
    }
    return best;
}

fn openWithPolicy(a: A, server: *server_api.ApiHttpServer, table: catalog.Table, request: catalog.Scan, context: operation.RequestContext, source: *local.serverless_query_lake_serving.ServingSource, policy: @import("lake_index_selection.zig").Policy, estimated_cost: ?*u64) !?catalog.Cursor {
    const index_name = if (request.index_range) |range| range.name else if (request.index_equality) |equality| equality.name else return error.ExternalLakeIndexUnavailable;
    if (request.primary_order or request.row_refs != null) return error.UnsupportedSqlExecution;
    const definitions = table.external_indexes orelse return error.ExternalLakeIndexUnavailable;
    const authority = server.source.lakeIndexLifecycleAuthority(context) orelse return if (policy == .automatic) null else error.ExternalLakeIndexUnavailable;
    const normalized = try context.platformDeadline();
    const read_context: local.serverless_query_lake_read_context.Context = .{ .io = server.embedding_provider_runtime.io, .deadline_ns = normalized.deadline_ns, .cancellation = local.storage_object_storage.CancellationToken.fromCallback(normalized.cancellation.ptr, normalized.cancellation.is_cancelled_fn) };
    var catalog_state = try local.metadata_lake_index_catalog.parse(a, definitions.catalog_json);
    defer catalog_state.deinit();
    const publication = catalog_state.value.published orelse return if (policy == .automatic) null else error.ExternalLakeIndexNotPublished;
    const store = Store.openNative(a, server.cfg.node_config, server.cfg.secret_store, true, server.cfg.deployment_mode, server.cfg.native_lake_artifact_base_dir) catch |err| {
        try context.ensureActive();
        if (policy == .automatic and err != error.OutOfMemory) return null;
        return err;
    };
    const owner = a.create(Owner) catch |err| {
        var cleanup = store;
        cleanup.deinit();
        return err;
    };
    owner.* = .{ .a = a, .arena = .init(a), .window = .init(a), .cover_arena = .init(a), .source = source, .store = store, .artifacts = undefined, .table = table, .request = request, .context = context, .read_context = read_context };
    var keep = false;
    defer if (!keep) Owner.close(owner);
    const ca = owner.arena.allocator();
    owner.lease = server.lake_reader_leases.acquire(server.embedding_provider_runtime.io, authority, table.id, publication.generation, read_context) catch |err| {
        try context.ensureActive();
        if (policy == .automatic and err != error.OutOfMemory and err != error.MetadataMutationOutcomeUnknown) return null;
        return err;
    };
    owner.read_context = owner.lease.?.readContext();
    var selected = (try @import("lake_index_selection.zig").selectCached(ca, table, source, &owner.store, owner.read_context, policy, &server.lake_read_cache)) orelse return null;
    defer selected.deinit();
    var parsed = try local.schema_mod.parseValidatedTableSchema(a, definitions.schema_json);
    defer parsed.deinit(a);
    const runtime = try local.schema_mod.deriveRuntimeTableSchema(a, parsed);
    defer local.storage_schema.freeSchema(a, runtime);
    var layout = try local.storage_db_algebraic_relational_row_codec.PhysicalLayout.init(a, runtime);
    defer layout.deinit();
    const indexes = (try parsed.relationalIndexDefinitions(ca)) orelse return error.ExternalLakeIndexUnavailable;
    const definition = for (indexes) |index| {
        if (std.mem.eql(u8, index.name, index_name)) break index;
    } else return if (policy == .automatic) null else error.ExternalLakeRowIndexNotFound;
    var predicate: ?local.storage_db_relational_index_predicate.Plan = if (definition.where.len == 0) null else try local.storage_db_relational_index_predicate.Plan.init(a, runtime, &layout, definition.where);
    defer if (predicate) |*plan| plan.deinit();
    if (predicate) |plan| {
        var query: std.ArrayList(local.storage_db_relational_predicate.Plan) = .empty;
        defer {
            for (query.items) |*condition| condition.deinit();
            query.deinit(a);
        }
        for (request.conditions) |condition| {
            const column = for (runtime.relational_columns) |column| {
                if (std.mem.eql(u8, column.name, condition.column)) break column;
            } else continue;
            const value = try local.schema_relational_checks.valueFromJson(ca, column.column_type, condition.value, true);
            var compiled = try local.storage_db_relational_predicate.Plan.init(a, runtime, &layout, .{ .column = condition.column, .op = switch (condition.op) {
                .neq => .ne,
                inline else => |tag| @field(local.storage_relational_index.RelationalCheckOp, @tagName(tag)),
            }, .value = value });
            errdefer compiled.deinit();
            try query.append(a, compiled);
        }
        if (!plan.impliedBy(query.items)) return if (policy == .automatic) null else error.ExternalLakePartialIndexNotImplied;
    }
    var tuple = try local.storage_db_relational_index_keys.TuplePlan.init(a, runtime, &layout, definition.keys);
    defer tuple.deinit();
    var lower: []const u8 = "";
    var upper: ?[]const u8 = null;
    var empty_range = false;
    if (request.index_range) |range| {
        if (range.lower) |bound| {
            const prefix = try boundKey(ca, tuple, bound.values);
            if (bound.inclusive) lower = prefix else if (try prefixSuccessor(ca, prefix)) |next_key| lower = next_key else empty_range = true;
        }
        if (range.upper) |bound| {
            const prefix = try boundKey(ca, tuple, bound.values);
            upper = if (bound.inclusive) try prefixSuccessor(ca, prefix) else prefix;
        }
    } else if (request.index_equality) |equality| {
        lower = try boundKey(ca, tuple, equality.values);
        upper = try prefixSuccessor(ca, lower);
    }
    const name = try native_rows.logicalName(ca, index_name);
    const declaration = for (selected.publication().declarations) |decl| {
        if (decl.artifact.kind == .ordered_row_index and std.mem.eql(u8, decl.name, name)) break decl;
    } else return if (policy == .automatic) null else error.ExternalLakeRowIndexNotPublished;
    owner.artifacts = owner.store.artifactStore();
    const cancel: operation.CancellationToken = .{ .ptr = owner, .is_cancelled_fn = Owner.canceled };
    owner.metadata = try @import("lake_index_decoded_metadata.zig").acquire(ordered.Root, .{ .cache = &server.lake_read_cache, .scope = owner.store.identity, .context = owner.read_context }, owner.artifacts, declaration.artifact, cancel, ordered.loadRoot);
    const root = owner.metadata.?.value.*;
    const domain = @import("lake_index_publication.zig").uploadDomainWithNamespace(table.id, owner.store.identity, selected.publication().namespace);
    if (!std.mem.eql(u8, &root.domain, &domain)) return error.InvalidNativeLakeRowIndex;
    const expected = native_rows.fingerprint(tuple, predicate, try native_rows.coverColumns(ca, definition));
    if (request.index_range) |range| {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.remote-row-cursor.publication.v1");
        hash.update(&expected);
        hash.update(declaration.artifact.artifact_id);
        hash.update(source.inventory.source_id);
        hash.update(source.inventory.snapshot_id);
        var fenced: [32]u8 = undefined;
        hash.final(&fenced);
        owner.cursor_header = local.storage_db_relational_row_cursor.identity(table.schema_version, index_name, fenced);
        if (range.after) |after| {
            const key = try local.storage_db_relational_row_cursor.decode(ca, after, owner.cursor_header.?);
            const exclusive = try prefixSuccessor(ca, key);
            if (exclusive) |next_key| {
                if (std.mem.order(u8, next_key, lower) == .gt) lower = next_key;
            } else empty_range = true;
        }
    }
    if (upper) |end| {
        if (std.mem.order(u8, lower, end) != .lt) empty_range = true;
    }
    try owner.reader.initCached(a, &owner.artifacts, root, expected, lower, upper, cancel, .{ .cache = &server.lake_read_cache, .scope = owner.store.identity, .context = owner.read_context });
    if (empty_range) owner.reader.exhausted = true;
    owner.reader_open = true;
    owner.covered = root.cover.len != 0;
    for (request.fields) |field| if (!covers(root.cover, field)) {
        owner.covered = false;
    };
    for (request.conditions) |condition| if (!covers(root.cover, condition.column)) {
        owner.covered = false;
    };
    owner.request.index_equality = null;
    var equal_count: usize = 0;
    for (definition.keys) |key| {
        const constant = for (request.conditions) |condition| {
            if (condition.op == .eq and condition.value != .null and std.mem.eql(u8, condition.column, key.column)) break true;
        } else false;
        if (!constant) break;
        equal_count += 1;
    }
    const order_satisfied = request.index_range != null and orderedBy(definition, request, equal_count);
    if (estimated_cost) |cost| {
        const candidates = if (empty_range) 0 else try owner.reader.countRange(ca, lower, upper);
        var source_rows: u64 = 0;
        for (source.inventory.files) |file| source_rows +|= file.row_count;
        source_rows = @max(source_rows, if (root.page) |page| page.records else 0);
        var physical_bytes: u64 = 0;
        var row_groups: u64 = 0;
        for (source.inventory.files) |file| {
            for (file.row_groups) |group| {
                row_groups +|= 1;
                for (group.column_chunks) |chunk| {
                    if (covers(request.fields, chunk.column_id)) physical_bytes +|= chunk.compressed_len;
                }
            }
        }
        const goal = if (rangeEnforcesConditions(definition, request, equal_count)) request.row_goal else null;
        cost.* = estimatedAccessCost(candidates, owner.covered, order_satisfied, goal, physical_bytes, row_groups);
        const scan_cost = (source_rows +| (physical_bytes / 1024)) *| @as(u64, if (order_satisfied) 2 else 1);
        if (cost.* > scan_cost and candidates != 0) return null;
    }
    keep = true;
    return .{ .order_satisfied = order_satisfied, .ptr = owner, .next = Owner.next, .next_columns = Owner.nextColumns, .set_dynamic_filter = Owner.setDynamicFilter, .close = Owner.close };
}

pub fn prefixSuccessor(a: A, prefix: []const u8) !?[]const u8 {
    var end = prefix.len;
    while (end != 0) {
        end -= 1;
        if (prefix[end] == 0xff) continue;
        const upper = try a.dupe(u8, prefix[0 .. end + 1]);
        upper[end] += 1;
        return upper;
    }
    return null;
}

fn boundKey(a: A, tuple: local.storage_db_relational_index_keys.TuplePlan, json: []const std.json.Value) ![]const u8 {
    if (json.len == 0 or json.len > tuple.keys.len) return error.InvalidRelationalIndexBound;
    const values = try a.alloc(local.storage_db_relational_index_keys.Value, json.len);
    for (values, json, tuple.keys[0..json.len]) |*value, wire, key| value.* = try local.schema_relational_checks.valueFromJson(a, key.column_type, wire, true);
    var encoded: std.ArrayList(u8) = .empty;
    _ = try tuple.appendValues(a, &encoded, values);
    return encoded.items;
}

fn covers(columns: []const []const u8, field: []const u8) bool {
    for (columns) |column| if (std.mem.eql(u8, column, field)) return true;
    return false;
}

test "external lake ordered access plans equality prefixes ranges directions and exact order proofs" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const keys: []const local.storage_relational_index.RelationalIndexKey = &.{ .{ .column = "tenant" }, .{ .column = "ts", .direction = .desc } };
    const index: local.storage_relational_index.RelationalIndexDefinition = .{ .name = "tenant_ts", .owner_kind = .table, .owner_name = "tenant_ts", .access_method = .ordered_tuple, .keys = keys };
    const request: catalog.Scan = .{ .fields = &.{"ts"}, .limit = 3, .conditions = &.{ .{ .column = "tenant", .op = .eq, .value = .{ .string = "a" } }, .{ .column = "ts", .op = .gte, .value = .{ .string = "-1" } }, .{ .column = "ts", .op = .lt, .value = .{ .string = "1" } } }, .order = &.{.{ .column = "ts", .descending = true, .nulls_first = true }} };
    const planned = (try chooseAccess(ca, &.{index}, request)).?;
    try std.testing.expect(orderedBy(index, request, 1));
    try std.testing.expectEqualStrings("1", planned.index_range.?.lower.?.values[1].string);
    try std.testing.expect(!planned.index_range.?.lower.?.inclusive);
    try std.testing.expectEqualStrings("-1", planned.index_range.?.upper.?.values[1].string);
    try std.testing.expect(planned.index_range.?.upper.?.inclusive);
    var wrong_order = request;
    wrong_order.order = &.{.{ .column = "ts", .descending = false }};
    try std.testing.expect(!orderedBy(index, wrong_order, 1));
    var wrong_nulls = request;
    wrong_nulls.order = &.{.{ .column = "ts", .descending = true, .nulls_first = false }};
    try std.testing.expect(!orderedBy(index, wrong_nulls, 1));
}

/// Relative work units. Covering rows avoid random Parquet page gathers.
fn accessCost(candidates: u64, covered: bool, ordered_access: bool) u64 {
    const row_cost = if (covered) candidates / 8 else candidates *| 8;
    return (row_cost +| 32) / @as(u64, if (ordered_access) 2 else 1);
}

// chooseAccess emits an equality prefix plus at most one lower/upper bound
// on its next key. Only that shape can consume OFFSET+LIMIT candidates without
// assuming selectivity for a residual, covered column or overwritten bound.
fn rangeEnforcesConditions(index: local.storage_relational_index.RelationalIndexDefinition, request: catalog.Scan, equal_count: usize) bool {
    for (request.conditions, 0..) |condition, ordinal| {
        const key = for (index.keys, 0..) |candidate, position| {
            if (candidate.expression_json == null and candidate.collation == null and std.mem.eql(u8, candidate.column, condition.column)) break position;
        } else return false;
        const lower = condition.op == .gt or condition.op == .gte;
        const upper = condition.op == .lt or condition.op == .lte;
        if (condition.op == .eq) {
            if (key >= equal_count) return false;
        } else if ((!lower and !upper) or key != equal_count) return false;
        for (request.conditions[0..ordinal]) |previous| {
            if (!std.mem.eql(u8, previous.column, condition.column)) continue;
            const previous_lower = previous.op == .gt or previous.op == .gte;
            const previous_upper = previous.op == .lt or previous.op == .lte;
            if (condition.op == .eq or (lower and previous_lower) or (upper and previous_upper)) return false;
        }
    }
    return true;
}

fn estimatedAccessCost(candidates: u64, covered: bool, ordered_access: bool, row_goal: ?u64, projected_bytes: u64, row_groups: u64) u64 {
    const consumed = if (ordered_access) if (row_goal) |goal| @min(candidates, goal) else candidates else candidates;
    var cost = accessCost(consumed, covered, ordered_access);
    // Random candidates may touch one decode region each until all groups
    // are visited. Unknown clustering uses this conservative upper estimate.
    if (!covered and row_groups != 0) cost +|= ((projected_bytes / row_groups) *| @min(consumed, row_groups)) / 1024;
    return cost;
}

test "external lake access costs SQL row goals only under proven order and charges projected locality" {
    const broad = estimatedAccessCost(100000, false, true, null, 10000000, 1000);
    const top_one = estimatedAccessCost(100000, false, true, 1, 10000000, 1000);
    try std.testing.expect(top_one < broad / 100);
    try std.testing.expectEqual(estimatedAccessCost(100000, false, false, null, 10000000, 1000), estimatedAccessCost(100000, false, false, 1, 10000000, 1000));
    try std.testing.expect(estimatedAccessCost(10, false, false, null, 10000000, 1) > estimatedAccessCost(10, false, false, null, 10000000, 1000));
    try std.testing.expectEqual(estimatedAccessCost(10, true, false, null, 10000000, 1), estimatedAccessCost(10, true, false, null, 10000000, 1000));
}

test "external lake access cost favors selective and covering ranges" {
    try std.testing.expect(accessCost(10, false, false) < 1000);
    try std.testing.expect(accessCost(900, false, false) > 1000);
    try std.testing.expect(accessCost(900, true, false) < 1000);
    try std.testing.expect(accessCost(900, true, true) < accessCost(900, true, false));
}

test "external lake filtered row goals require every predicate to be enforced by tuple bounds" {
    const index: local.storage_relational_index.RelationalIndexDefinition = .{ .name = "tenant_ts", .owner_kind = .table, .owner_name = "tenant_ts", .access_method = .ordered_tuple, .keys = &.{ .{ .column = "tenant" }, .{ .column = "ts" } } };
    var request: catalog.Scan = .{ .fields = &.{"ts"}, .limit = 256, .row_goal = 3, .conditions = &.{ .{ .column = "tenant", .op = .eq, .value = .{ .string = "a" } }, .{ .column = "ts", .op = .gte, .value = .{ .integer = 1 } }, .{ .column = "ts", .op = .lt, .value = .{ .integer = 10 } } } };
    try std.testing.expect(rangeEnforcesConditions(index, request, 1));
    request.conditions = &.{ .{ .column = "tenant", .op = .eq, .value = .{ .string = "a" } }, .{ .column = "covered_residual", .op = .eq, .value = .{ .integer = 1 } } };
    try std.testing.expect(!rangeEnforcesConditions(index, request, 1));
    request.conditions = &.{ .{ .column = "tenant", .op = .eq, .value = .{ .string = "a" } }, .{ .column = "ts", .op = .gte, .value = .{ .integer = 1 } }, .{ .column = "ts", .op = .gt, .value = .{ .integer = 5 } } };
    try std.testing.expect(!rangeEnforcesConditions(index, request, 1));
}

test "external lake covering batch preserves dictionary identity across reordered candidates and null domains" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Datum = local.sql_scalar.Datum;
    var cells: [32][1]Datum = undefined;
    var rows: [32]local.sql_operators.Row = undefined;
    for (&cells, &rows, 0..) |*cell, *row, i| {
        cell[0] = if (i == 0) .{} else if (i == 1) Datum.json(.null) else Datum.json(.{ .string = if (i % 2 == 0) "a repeated covering payload" else "another repeated covering payload" });
        row.* = .{ .values = cell, .keys = &.{}, .ordinal = i };
    }
    const bytes = try local.sql_spill.encodeColumnarBlockAlloc(a, &rows, 1024 * 1024);
    const block = try local.sql_spill.decodeColumnarBlockInArena(a, bytes, 1024 * 1024);
    const positions = try a.alloc(@typeInfo(@TypeOf(@as(Owner, undefined).covered_blocks)).pointer.child, 5);
    for (positions, [_]u16{ 31, 0, 1, 3, 2 }) |*position, row| position.* = .{ .block = block, .row = row };
    var owner: Owner = undefined;
    owner.covered_blocks = positions;
    const batch: local.sql_execution_batch.Batch = .{ .reader = .{ .ptr = &owner, .read = Owner.coveredCell, .read_identity = Owner.coveredIdentity, .read_dictionary = Owner.coveredDictionary, .count = positions.len, .width = 1 } };
    try std.testing.expectEqual(try batch.dictionaryIdentity(0, 0), try batch.dictionaryIdentity(3, 0));
    try std.testing.expect((try batch.dictionaryIdentity(1, 0)).? != (try batch.dictionaryIdentity(2, 0)).?);
    const selected = try batch.select(a, &.{ 2, 4, 1, 0 });
    const dictionary = (try selected.dictionaryColumn(a, 0)).?;
    for (0..selected.len()) |i| {
        const expected = try selected.cell(a, i, 0);
        const actual = try dictionary.cell(a, i, 0);
        try std.testing.expectEqual(expected.sql_null, actual.sql_null);
        try std.testing.expectEqual(std.meta.activeTag(expected.value), std.meta.activeTag(actual.value));
        if (expected.value == .string) try std.testing.expectEqualStrings(expected.value.string, actual.value.string);
    }
}
