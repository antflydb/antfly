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

//! Pull-based typed lake scans. Only one file's footer metadata, delete refs
//! and one decoded row group are retained. Opening never reads data pages.
const std = @import("std");
const external = @import("../external_source/types.zig");
const parquet = @import("lake_parquet_rowgroup.zig");
const iceberg = @import("lake_iceberg_snapshot.zig");
const serving = @import("lake_serving.zig");
const types = @import("../../storage/rowsource/types.zig");
const identity = @import("../../storage/rowsource/identity.zig");
const source_binding = @import("../segment/source_binding.zig");
const Context = @import("lake_read_context.zig").Context;
const Allocator = std.mem.Allocator;

/// Conservative pruning contract. Unsupported physical comparisons remain
/// residuals; no approximate numeric conversion can exclude matching rows.
pub const Predicate = struct {
    pub const Op = enum { eq, neq, lt, lte, gt, gte };
    column: []const u8,
    op: Op,
    value: union(enum) { integer: i64, bytes: []const u8, boolean: bool },
};
pub const Limits = struct {
    max_examined_rows: u64 = 100_000_000,
    max_decoded_bytes: usize = 32 * 1024 * 1024,
    max_input_bytes: usize = 32 * 1024 * 1024,
    max_row_group_rows: usize = 1_000_000,
};
pub const Stats = struct {
    files_opened: usize = 0,
    files_pruned: usize = 0,
    groups_decoded: usize = 0,
    groups_pruned: usize = 0,
    rows_examined: u64 = 0,
};
pub const Stream = struct {
    alloc: Allocator,
    source: *serving.ServingSource,
    columns: []const []const u8,
    identity_only: bool = false,
    predicates: []const Predicate,
    context: Context,
    limits: Limits,
    stats: Stats = .{},
    files: []usize,
    file_index: usize = 0,
    discovered: ?parquet.DiscoveredObjectRangeRowGroupPlan = null,
    group_index: usize = 0,
    current: ?parquet.OwnedBatch = null,
    page_cursor: ?@import("lake_parquet_cursor.zig").Cursor = null,
    deleted: []types.RowRef = &.{},
    schema_contract: []const @import("lake_schema.zig").Column = &.{},
    file_columns: []const []const u8 = &.{},
    file_logical_names: []const []const u8 = &.{},
    file_predicates: []const Predicate = &.{},
    filter: ?@import("lake_parquet_cursor.zig").Cursor.Filter = null,
    filter_columns: []const []const u8 = &.{},
    mapped_columns: std.ArrayList(types.ColumnVector) = .empty,
    lookahead: ?@import("../../sql/parallel_scheduler.zig").Task(anyerror!void) = null,
    lookahead_cancelled: std.atomic.Value(bool) = .init(false),
    lookahead_started: usize = 0,
    lookahead_names: []const []const u8 = &.{},
    lookahead_required: []const bool = &.{},

    fn lookaheadCanceled(raw: *const anyopaque) bool {
        const self: *const Stream = @ptrCast(@alignCast(raw));
        if (self.lookahead_cancelled.load(.acquire)) return true;
        self.context.ensureActive() catch return true;
        return false;
    }
    fn joinLookahead(self: *Stream, cancel: bool) void {
        if (cancel) self.lookahead_cancelled.store(true, .release);
        if (self.lookahead) |*task| {
            const io = self.source.scanner.shared_reader.?.context.io.?;
            if (cancel) task.cancel(io) catch {} else task.await(io) catch {};
            self.lookahead = null;
        }
    }
    fn warmGroup(self: *Stream, inventory: external.Inventory, ordinal: u32, names: []const []const u8, required: []const bool) anyerror!void {
        const reader = self.source.scanner.shared_reader.?;
        var worker_reader: @import("lake_serving_cache.zig").Reader = .{ .cache = reader.cache, .base = reader.base, .scope = reader.scope, .context = reader.context };
        const token: @import("../../storage/object_storage.zig").CancellationToken = .{ .ptr = self, .is_cancelled_fn = lookaheadCanceled };
        worker_reader.context.cancellation = token;
        worker_reader.base.cancellation = token;
        // Worker allocations never mutate a statement arena. Decoded buffers
        // belong to the bounded shared cache, and temporary work is admitted
        // by the same scheduler used by kernels, spill writes and prefetch.
        var cursor = try @import("lake_parquet_cursor.zig").Cursor.init(std.heap.page_allocator, worker_reader.reader(), inventory, inventory.files[0].file_id, ordinal, names, .{
            .max_rows = self.limits.max_row_group_rows,
            .max_input_bytes = self.limits.max_input_bytes,
            .max_decoded_bytes = self.limits.max_decoded_bytes,
            .max_struct_allocation_bytes = self.limits.max_decoded_bytes,
        });
        defer cursor.deinit();
        // Avoid recursively reserving scheduler slots from an admitted task.
        worker_reader.context.io = null;
        cursor.shared_reader = &worker_reader;
        for (cursor.columns, required) |*column, needed| column.required = needed;
        try worker_reader.context.ensureActive();
        try cursor.prepareEvidence();
        try worker_reader.context.ensureActive();
    }
    fn startLookahead(self: *Stream) void {
        if (self.identity_only) return;
        if (self.lookahead != null) return;
        const reader = self.source.scanner.shared_reader orelse return;
        const io = reader.context.io orelse return;
        const plan = self.discovered orelse return;
        const current = self.page_cursor orelse return;
        const ordinal = for (plan.row_group_plan.row_groups[self.group_index..]) |input| {
            if (groupMayMatch(plan.inventory.files[0].row_groups[input.row_group_ordinal], self.groupPredicates())) break input.row_group_ordinal;
        } else return;
        // Names and masks live in the current cursor until clearFile joins.
        const stable_names = self.alloc.alloc([]const u8, current.columns.len) catch return;
        const stable_required = self.alloc.alloc(bool, current.columns.len) catch {
            self.alloc.free(stable_names);
            return;
        };
        for (current.columns, stable_names, stable_required) |column, *name, *needed| {
            name.* = column.chunk.column_id;
            needed.* = column.required;
        }
        self.lookahead_names = stable_names;
        self.lookahead_required = stable_required;
        self.lookahead_cancelled.store(false, .release);
        self.lookahead = @import("../../sql/parallel_scheduler.zig").global().submit(io, self.limits.max_input_bytes +| self.limits.max_decoded_bytes, warmGroup, .{ self, plan.inventory, ordinal, stable_names, stable_required });
        if (self.lookahead != null) self.lookahead_started += 1 else self.freeLookaheadDescriptors();
    }
    fn freeLookaheadDescriptors(self: *Stream) void {
        self.alloc.free(self.lookahead_names);
        self.alloc.free(self.lookahead_required);
        self.lookahead_names = &.{};
        self.lookahead_required = &.{};
    }
    fn anyMatch(raw: *anyopaque, batch: types.ColumnBatch) anyerror!bool {
        const self: *Stream = @ptrCast(@alignCast(raw));
        const filter = self.filter.?;
        return filter.any_match(filter.ptr, try self.logicalBatch(batch));
    }

    fn pageMayMatch(raw: *anyopaque, chunk: external.ColumnChunk, page: @import("lake_parquet_metadata.zig").IndexedPage) bool {
        const self: *Stream = @ptrCast(@alignCast(raw));
        for (self.groupPredicates()) |predicate| {
            if (!std.mem.eql(u8, predicate.column, chunk.column_id)) continue;
            if (page.all_null) return false;
            if (chunk.logical_type.len != 0 and !std.mem.eql(u8, chunk.logical_type, "string") and !std.mem.startsWith(u8, chunk.logical_type, "int")) continue;
            const min = page.min orelse continue;
            const max = page.max orelse continue;
            const matches = switch (predicate.value) {
                .integer => |value| blk: {
                    const low = metricInteger(min) orelse continue;
                    const high = metricInteger(max) orelse continue;
                    if (low > high) continue;
                    break :blk rangeMayMatch(i64, low, high, value, predicate.op);
                },
                .bytes => |value| blk: {
                    if (std.mem.order(u8, min, max) == .gt) continue;
                    break :blk rangeMayMatch([]const u8, min, max, value, predicate.op);
                },
                .boolean => |value| blk: {
                    if (min.len != 1 or max.len != 1 or max[0] > 1 or min[0] > max[0]) continue;
                    break :blk rangeMayMatch(u8, min[0], max[0], @intFromBool(value), predicate.op);
                },
            };
            if (!matches) return false;
        }
        return true;
    }
    fn leafFor(expected: @import("lake_schema.zig").Column, leaves: []const @import("lake_parquet_metadata.zig").SchemaColumn) ?@import("lake_parquet_metadata.zig").SchemaColumn {
        for (leaves) |leaf| {
            if (if (expected.field_id) |id| leaf.field_id == id else std.mem.eql(u8, leaf.column_id, expected.name)) return leaf;
        }
        return null;
    }
    fn logicalBatch(self: *Stream, batch: types.ColumnBatch) !types.ColumnBatch {
        if (self.source.inventory.format != .iceberg or self.schema_contract.len == 0) return batch;
        self.mapped_columns.clearRetainingCapacity();
        for (batch.columns) |column| for (self.file_columns, self.file_logical_names) |physical, logical| {
            if (std.mem.eql(u8, column.name, physical)) {
                if (logical.len != 0) {
                    var mapped = column;
                    mapped.name = logical;
                    try self.mapped_columns.append(self.alloc, mapped);
                }
                break;
            }
        };
        var mapped = batch;
        mapped.columns = self.mapped_columns.items;
        return mapped;
    }
    fn groupPredicates(self: Stream) []const Predicate {
        return if (self.source.inventory.format == .iceberg) self.file_predicates else self.predicates;
    }
    fn fileMatches(self: Stream, file: external.FileEntry) bool {
        return (if (self.source.inventory.format == .iceberg) icebergFileMayMatch(file, self.schema_contract, self.predicates) else fileMayMatch(file, self.predicates)) and
            (if (self.source.partition_rules) |rules| @import("lake_partition_pruning.zig").mayMatch(rules.items, file, self.predicates) else true);
    }

    pub fn init(alloc: Allocator, source: *serving.ServingSource, columns: []const []const u8, predicates: []const Predicate, context: Context, limits: Limits) !Stream {
        try context.ensureActive();
        try source.inventory.validate();
        const files = try alloc.alloc(usize, source.inventory.files.len);
        for (files, 0..) |*index, i| index.* = i;
        std.mem.sort(usize, files, source.inventory, struct {
            fn less(inventory: external.Inventory, a: usize, b: usize) bool {
                const left = identity.fileDigest(inventory.source_id, inventory.snapshot_id, inventory.files[a].file_id);
                const right = identity.fileDigest(inventory.source_id, inventory.snapshot_id, inventory.files[b].file_id);
                return std.mem.order(u8, &left, &right) == .lt;
            }
        }.less);
        return .{ .alloc = alloc, .source = source, .columns = columns, .predicates = predicates, .context = context, .limits = limits, .files = files, .schema_contract = if (source.iceberg_schema) |schema| schema.columns else &.{} };
    }
    pub fn deinit(self: *Stream) void {
        self.clearFile();
        self.alloc.free(self.files);
        self.mapped_columns.deinit(self.alloc);
        self.* = undefined;
    }
    fn clearBatch(self: *Stream) void {
        if (self.current) |*batch| batch.deinit(self.alloc);
        self.current = null;
    }
    fn clearFile(self: *Stream) void {
        self.joinLookahead(true);
        self.freeLookaheadDescriptors();
        if (self.source.scanner.shared_reader) |reader| reader.drain(true);
        self.clearBatch();
        if (self.page_cursor) |*cursor| cursor.deinit();
        self.page_cursor = null;
        if (self.deleted.len != 0) self.alloc.free(self.deleted);
        self.deleted = &.{};
        if (self.discovered) |*plan| plan.deinit(self.alloc);
        self.discovered = null;
        for (self.file_columns) |name| self.alloc.free(name);
        self.alloc.free(self.file_columns);
        self.file_columns = &.{};
        self.alloc.free(self.file_logical_names);
        self.file_logical_names = &.{};
        self.alloc.free(self.file_predicates);
        self.file_predicates = &.{};
        self.group_index = 0;
    }
    /// The returned vectors remain valid until the next pull or close.
    pub fn next(self: *Stream) !?types.ColumnBatch {
        try self.context.ensureActive();
        self.clearBatch();
        if (self.source.scanner.shared_reader) |reader| reader.drain(false);
        while (true) {
            if (self.page_cursor) |*cursor| {
                if (try cursor.next()) |batch| {
                    if (cursor.position == cursor.group.row_count) try self.prefetchNext();
                    return try self.logicalBatch(batch);
                }
                self.joinLookahead(false);
                self.freeLookaheadDescriptors();
                cursor.deinit();
                self.page_cursor = null;
            }
            if (self.discovered) |*plan| {
                while (self.group_index < plan.row_group_plan.row_groups.len) {
                    const input = plan.row_group_plan.row_groups[self.group_index];
                    self.group_index += 1;
                    const group = for (plan.inventory.files[0].row_groups) |candidate| {
                        if (candidate.ordinal == input.row_group_ordinal) break candidate;
                    } else return error.InvalidParquetRowGroupBatch;
                    if (!groupMayMatch(group, self.groupPredicates())) {
                        self.stats.groups_pruned += 1;
                        continue;
                    }
                    if (group.row_count > self.limits.max_examined_rows -| self.stats.rows_examined) return error.LakeRowsScanBudgetExceeded;
                    self.page_cursor = try @import("lake_parquet_cursor.zig").Cursor.init(self.alloc, self.source.scanner.reader(), plan.inventory, input.file_id, input.row_group_ordinal, if (self.identity_only) &.{} else if (self.schema_contract.len != 0) self.file_columns else self.columns, .{
                        .max_rows = self.limits.max_row_group_rows,
                        .max_struct_allocation_bytes = self.limits.max_decoded_bytes,
                        .max_input_bytes = self.limits.max_input_bytes,
                        .max_decoded_bytes = self.limits.max_decoded_bytes,
                    });
                    self.stats.groups_decoded += 1;
                    self.stats.rows_examined += group.row_count;
                    try self.context.ensureActive();
                    self.page_cursor.?.shared_reader = self.source.scanner.shared_reader;
                    self.page_cursor.?.prune_ptr = self;
                    self.page_cursor.?.prune_page = pageMayMatch;
                    if (self.filter != null) {
                        var required: usize = 0;
                        for (self.page_cursor.?.columns) |*column| {
                            column.required = false;
                            var logical = column.chunk.column_id;
                            if (self.source.inventory.format == .iceberg and self.schema_contract.len != 0) {
                                for (self.file_columns, self.file_logical_names) |physical, name| if (std.mem.eql(u8, physical, logical)) {
                                    logical = @constCast(name);
                                    break;
                                };
                            }
                            for (self.filter_columns) |name| if (std.mem.eql(u8, name, logical)) {
                                column.required = true;
                                required += 1;
                                break;
                            };
                        }
                        // A driver supplies row ordinals for ID-only or missing
                        // nullable predicates; other columns remain deferred.
                        if (required == 0 and self.page_cursor.?.columns.len != 0) self.page_cursor.?.columns[0].required = true;
                        self.page_cursor.?.filter = .{ .ptr = self, .any_match = anyMatch };
                    }
                    const batch = try self.page_cursor.?.next();
                    if (batch == null) {
                        self.page_cursor.?.deinit();
                        self.page_cursor = null;
                        continue;
                    }
                    self.startLookahead();
                    if (self.page_cursor.?.position == group.row_count) try self.prefetchNext();
                    return try self.logicalBatch(batch.?);
                }
                self.clearFile();
            }
            if (self.file_index == self.files.len) return null;
            const index = self.files[self.file_index];
            self.file_index += 1;
            const file = self.source.inventory.files[index];
            if (!self.fileMatches(file)) {
                self.stats.files_pruned += 1;
                continue;
            }
            try self.loadFile(index);
        }
    }
    fn prefetchNext(self: *Stream) !void {
        const reader = self.source.scanner.shared_reader orelse return;
        if (reader.context.io == null) return;
        try self.context.ensureActive();
        self.planPrefetchNext(reader) catch {
            // Next-group/file lookahead follows the same speculative contract
            // as page prefetch; required reads validate these ranges later.
            try self.context.ensureActive();
        };
    }
    fn planPrefetchNext(self: *Stream, reader: *@import("lake_serving_cache.zig").Reader) !void {
        const plan = &self.discovered.?;
        const file = plan.inventory.files[0];
        for (plan.row_group_plan.row_groups[self.group_index..]) |input| {
            const group = for (file.row_groups) |candidate| {
                if (candidate.ordinal == input.row_group_ordinal) break candidate;
            } else continue;
            if (!groupMayMatch(group, self.groupPredicates())) continue;
            const range_io = @import("lake_range_io.zig");
            var reads: std.ArrayList(range_io.RangeRead) = .empty;
            defer reads.deinit(self.alloc);
            const object = try range_io.objectRefForExternalFileUri(file);
            for (group.column_chunks) |chunk| {
                for (if (self.schema_contract.len != 0) self.file_columns else self.columns) |column| if (std.mem.eql(u8, column, chunk.column_id)) {
                    try reads.append(self.alloc, .{ .object = object, .range = .{ .offset = chunk.file_offset, .len = @min(chunk.compressed_len, 512) }, .purpose = .parquet_column_chunk });
                    break;
                };
            }
            try reader.prefetch(reads.items);
            return;
        }
        // Next-file footer lookahead also helps single-row-group datasets.
        for (self.files[self.file_index..]) |index| {
            const upcoming = self.source.inventory.files[index];
            if (!self.fileMatches(upcoming)) continue;
            const object = try @import("lake_range_io.zig").objectRefForExternalFileUri(upcoming);
            const footer = try @import("lake_range_io.zig").planParquetFooterRead(object, 64 * 1024);
            try reader.prefetch(&.{footer});
            return;
        }
    }
    fn loadFile(self: *Stream, index: usize) !void {
        var inventory = self.source.inventory;
        inventory.files = self.source.inventory.files[index..][0..1];
        inventory.deleted_row_groups = &.{};
        if (self.schema_contract.len != 0 or self.source.scanner.shared_reader != null) {
            const lease = if (self.source.scanner.shared_reader) |reader| try reader.footer(inventory.files[0]) else null;
            defer if (lease) |borrowed| borrowed.release();
            var footer = if (lease) |borrowed| borrowed.item.payload.footer else try @import("lake_schema.zig").readFooter(self.alloc, self.source.scanner.reader(), inventory.files[0]);
            defer if (lease == null) footer.deinit(self.alloc);
            if (footer.schema_columns.len == 0) return error.ExternalLakeSchemaUnavailable;
            if (self.source.inventory.format == .iceberg) {
                for (footer.schema_columns, 0..) |leaf, i| {
                    const id = leaf.field_id orelse return error.UnsupportedIcebergSchemaEvolution;
                    for (footer.schema_columns[0..i]) |prior| if (prior.field_id == id) return error.InvalidParquetMetadata;
                }
            }
            for (self.schema_contract) |expected| {
                const leaf = leafFor(expected, footer.schema_columns);
                if (leaf) |found| {
                    if (!std.mem.eql(u8, try @import("lake_schema.zig").parquetKind(found), expected.kind) or (expected.required and found.nullable)) return error.ExternalLakeSchemaMismatch;
                } else if (expected.required) return error.ExternalLakeSchemaMismatch;
            }
            if (footer.row_count == 0) return;
            var columns: std.ArrayList([]const u8) = .empty;
            errdefer {
                for (columns.items) |name| self.alloc.free(name);
                columns.deinit(self.alloc);
            }
            var logical_names: std.ArrayList([]const u8) = .empty;
            errdefer logical_names.deinit(self.alloc);
            for (self.columns) |wanted| {
                const expected = for (self.schema_contract) |column| {
                    if (std.mem.eql(u8, column.name, wanted)) break column;
                } else if (self.schema_contract.len == 0) @as(@import("lake_schema.zig").Column, .{ .name = wanted, .kind = "", .required = false }) else return error.ExternalLakeSchemaMismatch;
                if (leafFor(expected, footer.schema_columns)) |leaf| {
                    const name = try self.alloc.dupe(u8, leaf.column_id);
                    columns.append(self.alloc, name) catch |err| {
                        self.alloc.free(name);
                        return err;
                    };
                    try logical_names.append(self.alloc, wanted);
                }
            }
            var predicates: std.ArrayList(Predicate) = .empty;
            errdefer predicates.deinit(self.alloc);
            for (self.predicates) |predicate| {
                const expected = for (self.schema_contract) |column| {
                    if (std.mem.eql(u8, column.name, predicate.column)) break column;
                } else continue;
                if (leafFor(expected, footer.schema_columns)) |leaf| {
                    // Borrow the retained projection name, never footer storage.
                    for (columns.items) |physical| if (std.mem.eql(u8, leaf.column_id, physical)) {
                        var mapped = predicate;
                        mapped.column = physical;
                        try predicates.append(self.alloc, mapped);
                        break;
                    };
                }
            }
            // A physical driver supplies row refs when every selected column
            // is absent. SQL's existing missing-cell contract returns NULL.
            if (columns.items.len == 0) {
                const name = try self.alloc.dupe(u8, footer.schema_columns[0].column_id);
                columns.append(self.alloc, name) catch |err| {
                    self.alloc.free(name);
                    return err;
                };
                try logical_names.append(self.alloc, "");
            }
            self.file_columns = try columns.toOwnedSlice(self.alloc);
            self.file_logical_names = try logical_names.toOwnedSlice(self.alloc);
            self.file_predicates = try predicates.toOwnedSlice(self.alloc);
            var enriched = try @import("lake_parquet_metadata.zig").enrichInventoryFileWithFooterAlloc(self.alloc, inventory, inventory.files[0].file_id, footer);
            errdefer enriched.deinit(self.alloc);
            const plan = try parquet.planSupportedI64ObjectRangeRowGroupsAlloc(self.alloc, enriched, self.file_columns);
            self.discovered = .{ .inventory = enriched, .row_group_plan = plan };
        } else {
            self.discovered = if (self.source.scanner.cache) |cache|
                try parquet.discoverSupportedI64ObjectRangeRowGroupsFromCachedFootersAlloc(self.alloc, self.source.scanner.reader(), cache, inventory, self.columns, 64 * 1024)
            else
                try parquet.discoverSupportedI64ObjectRangeRowGroupsFromFootersAlloc(self.alloc, self.source.scanner.reader(), inventory, self.columns, 64 * 1024);
        }
        self.stats.files_opened += 1;
        std.mem.sort(parquet.ObjectRangeRowGroupInput, self.discovered.?.row_group_plan.row_groups, {}, struct {
            fn less(_: void, a: parquet.ObjectRangeRowGroupInput, b: parquet.ObjectRangeRowGroupInput) bool {
                return a.row_group_ordinal < b.row_group_ordinal;
            }
        }.less);
        if (self.source.scanner.iceberg_delete_plan) |delete_plan| {
            if (self.source.prepared_deletes == null) self.source.prepared_deletes = try @import("lake_prepared_deletes.zig").Prepared.create(self.source.alloc, .{
                .reader = self.source.scanner.reader(),
                .client = self.source.scanner.object_reader.client,
                .cache = self.source.scanner.cache,
                .data_inventory = self.source.inventory,
                .delete_plan = delete_plan,
                .coalesce_options = self.source.scanner.coalesce_options,
                .materialization_limits = .{ .max_struct_allocation_bytes = self.limits.max_decoded_bytes, .max_input_bytes = self.limits.max_input_bytes, .max_decoded_bytes = self.limits.max_decoded_bytes },
            });
        }
    }
    pub fn countAll(self: *Stream) !?u64 {
        if (self.file_index != 0 or self.predicates.len != 0) return null;
        var total: u64 = 0;
        for (self.files) |index| {
            try self.context.ensureActive();
            try self.loadFile(index);
            if (self.discovered) |plan| total = std.math.add(u64, total, plan.inventory.files[0].row_count) catch return error.LakeRowsScanBudgetExceeded;
            self.clearFile();
        }
        self.file_index = self.files.len;
        return total;
    }
    pub fn isDeletedBatch(self: Stream, a: Allocator, batch: types.ColumnBatch, index: usize) !bool {
        if (self.isDeleted(batch.row_refs[index])) return true;
        if (self.source.prepared_deletes) |prepared|
            return prepared.matches(a, self.discovered.?.inventory.files[0], batch, index);
        return false;
    }
    pub fn isDeleted(self: Stream, ref: types.RowRef) bool {
        var low: usize = 0;
        var high: usize = self.deleted.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            if (iceberg.externalRowRefLessThan({}, self.deleted[mid], ref)) low = mid + 1 else high = mid;
        }
        if (low < self.deleted.len and source_binding.rowRefsEqual(self.deleted[low], ref)) return true;
        if (ref == .external) {
            const ordinal = ref.external.row_ordinal;
            for (self.source.inventory.deleted_row_groups) |vector| if (std.mem.eql(u8, vector.file_id, ref.external.file_id) and vector.row_group_ordinal == ref.external.row_group_ordinal) {
                var begin: usize = 0;
                var end: usize = vector.row_ordinals.len;
                while (begin < end) {
                    const mid = begin + (end - begin) / 2;
                    if (vector.row_ordinals[mid] < ordinal) begin = mid + 1 else end = mid;
                }
                if (begin < vector.row_ordinals.len and vector.row_ordinals[begin] == ordinal) return true;
            };
        }
        return false;
    }
};
fn rangeMayMatch(comptime T: type, min: T, max: T, value: T, op: Predicate.Op) bool {
    const low = if (T == []const u8) std.mem.order(u8, min, value) else std.math.order(min, value);
    const high = if (T == []const u8) std.mem.order(u8, max, value) else std.math.order(max, value);
    return switch (op) {
        .eq => low != .gt and high != .lt,
        .neq => low != .eq or high != .eq,
        .lt => low == .lt,
        .lte => low != .gt,
        .gt => high == .gt,
        .gte => high != .lt,
    };
}
pub fn groupMayMatch(group: external.RowGroup, predicates: []const Predicate) bool {
    for (predicates) |predicate| for (group.column_chunks) |chunk| {
        if (!std.mem.eql(u8, chunk.column_id, predicate.column)) continue;
        // Decimal/timestamp annotations change comparison semantics relative
        // to their physical statistics. Keep those predicates residual.
        if (chunk.logical_type.len != 0 and !std.mem.eql(u8, chunk.logical_type, "string")) continue;
        const matches = switch (predicate.value) {
            .integer => |value| if (chunk.stats_min_i64) |min| if (chunk.stats_max_i64) |max| rangeMayMatch(i64, min, max, value, predicate.op) else true else true,
            .bytes => |value| if (chunk.stats_min_bytes) |min| if (chunk.stats_max_bytes) |max| rangeMayMatch([]const u8, min, max, value, predicate.op) else true else true,
            .boolean => |value| if (chunk.stats_min_bool) |min| if (chunk.stats_max_bool) |max| rangeMayMatch(u8, @intFromBool(min), @intFromBool(max), @intFromBool(value), predicate.op) else true else true,
        };
        if (!matches) return false;
    };
    return true;
}
fn metricValue(metrics: []const external.FieldMetric, id: i32) ?[]const u8 {
    for (metrics) |metric| if (metric.field_id == id) return metric.value;
    return null;
}
fn metricInteger(bytes: []const u8) ?i64 {
    return switch (bytes.len) {
        4 => std.mem.readInt(i32, bytes[0..4], .little),
        8 => std.mem.readInt(i64, bytes[0..8], .little),
        else => null,
    };
}
pub fn icebergFileMayMatch(file: external.FileEntry, schema: []const @import("lake_schema.zig").Column, predicates: []const Predicate) bool {
    for (predicates) |predicate| {
        const column = for (schema) |candidate| {
            if (std.mem.eql(u8, candidate.name, predicate.column)) break candidate;
        } else continue;
        const id = column.field_id orelse continue;
        const min = metricValue(file.lower_bounds, id) orelse continue;
        const max = metricValue(file.upper_bounds, id) orelse continue;
        const matches = switch (predicate.value) {
            .integer => |value| blk: {
                // Timestamp and decimal bounds require their original Iceberg
                // unit/type. Unknown annotations must remain residual.
                if (!std.mem.eql(u8, column.kind, "integer")) continue;
                const low = metricInteger(min) orelse continue;
                const high = metricInteger(max) orelse continue;
                if (low > high) continue;
                break :blk rangeMayMatch(i64, low, high, value, predicate.op);
            },
            .bytes => |value| blk: {
                if (!std.mem.eql(u8, column.iceberg_type, "string") or std.mem.order(u8, min, max) == .gt) continue;
                break :blk rangeMayMatch([]const u8, min, max, value, predicate.op);
            },
            .boolean => |value| blk: {
                if (!std.mem.eql(u8, column.kind, "boolean") or min.len != 1 or max.len != 1 or max[0] > 1 or min[0] > max[0]) continue;
                break :blk rangeMayMatch(u8, min[0], max[0], @intFromBool(value), predicate.op);
            },
        };
        if (!matches) return false;
    }
    return true;
}
pub fn fileMayMatch(file: external.FileEntry, predicates: []const Predicate) bool {
    if (file.row_groups.len == 0) return true;
    for (file.row_groups) |group| if (groupMayMatch(group, predicates)) return true;
    return false;
}

test "external lake pruning preserves exact integers and unknown annotated statistics" {
    var column: external.ColumnChunk = .{ .column_id = @constCast("n"), .file_offset = 4, .compressed_len = 8, .stats_min_i64 = 9007199254740993, .stats_max_i64 = 9007199254740993 };
    var group: external.RowGroup = .{ .ordinal = 0, .row_count = 1, .column_chunks = @as([*]external.ColumnChunk, @ptrCast(&column))[0..1] };
    try std.testing.expect(groupMayMatch(group, &.{.{ .column = "n", .op = .gt, .value = .{ .integer = 9007199254740992 } }}));
    try std.testing.expect(!groupMayMatch(group, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 9007199254740992 } }}));
    column.stats_max_i64 = null;
    try std.testing.expect(groupMayMatch(group, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 0 } }}));
    column.stats_max_i64 = 9007199254740993;
    column.logical_type = @constCast("decimal");
    try std.testing.expect(groupMayMatch(group, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 0 } }}));
    group.column_chunks = &.{};
    try std.testing.expect(groupMayMatch(group, &.{.{ .column = "n", .op = .eq, .value = .{ .integer = 0 } }}));
}

test "external lake Iceberg file bounds resolve field IDs and preserve unknown decimal encodings" {
    var min: [8]u8 = undefined;
    var max: [8]u8 = undefined;
    std.mem.writeInt(i64, &min, 9007199254740993, .little);
    std.mem.writeInt(i64, &max, 9007199254740995, .little);
    var lower = [_]external.FieldMetric{.{ .field_id = 7, .value = &min }};
    var upper = [_]external.FieldMetric{.{ .field_id = 7, .value = &max }};
    const file: external.FileEntry = .{ .file_id = @constCast("f"), .object_uri = @constCast("s3://b/f"), .etag = @constCast("v1"), .byte_len = 10, .row_count = 3, .row_groups = &.{}, .lower_bounds = &lower, .upper_bounds = &upper };
    const schema = [_]@import("lake_schema.zig").Column{.{ .name = "renamed", .kind = "integer", .required = false, .field_id = 7, .iceberg_type = "long" }};
    try std.testing.expect(!icebergFileMayMatch(file, &schema, &.{.{ .column = "renamed", .op = .eq, .value = .{ .integer = 9007199254740992 } }}));
    try std.testing.expect(icebergFileMayMatch(file, &schema, &.{.{ .column = "renamed", .op = .gte, .value = .{ .integer = 9007199254740993 } }}));
    var changed = schema;
    changed[0].field_id = 8;
    try std.testing.expect(icebergFileMayMatch(file, &changed, &.{.{ .column = "renamed", .op = .eq, .value = .{ .integer = 0 } }}));
    changed[0].field_id = 7;
    changed[0].kind = "string";
    changed[0].iceberg_type = "decimal(18,2)";
    try std.testing.expect(icebergFileMayMatch(file, &changed, &.{.{ .column = "renamed", .op = .eq, .value = .{ .bytes = "0.00" } }}));
}
