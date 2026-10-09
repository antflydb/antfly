// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
    bloom_blocks_read: usize = 0,
    bloom_groups_pruned: usize = 0,
    rows_examined: u64 = 0,
};
/// Coordinator-owned immutable work list; workers claim independent row
/// groups. Large compressed groups start first to reduce straggler latency.
pub const ScanWork = struct {
    pub const FilePlan = struct {
        discovered: parquet.DiscoveredObjectRangeRowGroupPlan,
        columns: []const []const u8,
        logical_names: []const []const u8,
        predicates: []const Predicate,
        position_starts: []const u64,
        fn deinit(self: *FilePlan, a: Allocator) void {
            a.free(self.position_starts);
            self.discovered.deinit(a);
            for (self.columns) |name| a.free(name);
            a.free(self.columns);
            a.free(self.logical_names);
            a.free(self.predicates);
        }
    };
    pub const Unit = struct { file: usize, ordinal: u32, group_index: usize, bytes: u64 };
    plans: []?FilePlan,
    units: []Unit,
    next: std.atomic.Value(usize) = .init(0),
    fn claim(self: *ScanWork) ?Unit {
        const index = self.next.fetchAdd(1, .monotonic);
        return if (index < self.units.len) self.units[index] else null;
    }
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
    owns_files: bool = true,
    selection: ?@import("lake_row_selection.zig").Selection = null,
    active_file: usize = 0,
    file_index: usize = 0,
    work: ?*ScanWork = null,
    owns_work: bool = false,
    work_ordinal: ?u32 = null,
    ordered_next: ?usize = null,
    ordered_end: usize = 0,
    borrowed_plan: bool = false,
    hydration_plan: ?@import("lake_decoded_cache.zig").Lease = null,
    planning_footer: ?@import("lake_decoded_cache.zig").Lease = null,
    partition_index: usize = 0,
    partition_count: usize = 1,
    discovered: ?parquet.DiscoveredObjectRangeRowGroupPlan = null,
    group_index: usize = 0,
    position_starts: []const u64 = &.{},
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
    bloom_lookahead: [4]?@import("../../sql/parallel_scheduler.zig").Task(anyerror!void) = @splat(null),
    bloom_next_input: usize = 0,
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
        self.joinBloomLookahead(cancel);
    }
    fn joinBloomLookahead(self: *Stream, cancel: bool) void {
        if (self.source.scanner.shared_reader) |reader| if (reader.context.io) |io| for (&self.bloom_lookahead) |*task| {
            if (task.*) |*work| {
                if (cancel) work.cancel(io) catch {} else if (work.isComplete()) work.await(io) catch {} else continue;
                task.* = null;
            }
        };
    }
    fn startBloomLookahead(self: *Stream) void {
        if (self.partition_count != 1) return; // Partition workers already overlap required probes.
        const reader = self.source.scanner.shared_reader orelse return;
        const io = reader.context.io orelse return;
        const plan = self.discovered orelse return;
        self.joinBloomLookahead(false);
        self.lookahead_cancelled.store(false, .release);
        self.bloom_next_input = @max(self.bloom_next_input, self.group_index);
        var slot: usize = 0;
        while (self.bloom_next_input < plan.row_group_plan.row_groups.len) {
            while (slot < self.bloom_lookahead.len and self.bloom_lookahead[slot] != null) slot += 1;
            if (slot == self.bloom_lookahead.len) break;
            const input = plan.row_group_plan.row_groups[self.bloom_next_input];
            self.bloom_next_input += 1;
            const groups = plan.inventory.files[0].row_groups;
            if (input.row_group_ordinal >= groups.len or groups[input.row_group_ordinal].ordinal != input.row_group_ordinal) return;
            const group = groups[input.row_group_ordinal];
            if (!groupMayMatch(group, self.groupPredicates())) continue;
            const eligible = eligible: {
                for (self.groupPredicates()) |predicate| {
                    for (group.column_chunks) |chunk| if (bloomProbeUseful(chunk, predicate)) break :eligible true;
                }
                break :eligible false;
            };
            if (!eligible) continue;
            if (self.selection) |selection| if (!selection.rangeMayMatch(self.active_file, group.ordinal, 0, group.row_count)) continue;
            // Probe before the first matching group, including all-negative
            // cursor scans. A monotonic plan position avoids duplicate jobs.
            self.bloom_lookahead[slot] = @import("../../sql/parallel_scheduler.zig").global().submitTransient(io, 64 * 1024, warmBloom, .{ self, plan.inventory, input.row_group_ordinal });
            if (self.bloom_lookahead[slot] == null) return; // Required reads remain synchronous on pressure.
            slot += 1;
        }
    }
    fn warmBloom(self: *Stream, inventory: external.Inventory, ordinal: u32) anyerror!void {
        const reader = self.source.scanner.shared_reader.?;
        var worker: @import("lake_serving_cache.zig").Reader = .{ .cache = reader.cache, .base = reader.base, .scope = reader.scope, .context = reader.context };
        const token: @import("../../storage/object_storage.zig").CancellationToken = .{ .ptr = self, .is_cancelled_fn = lookaheadCanceled };
        worker.context.cancellation = token;
        worker.base.cancellation = token;
        var stats: Stats = .{};
        _ = try bloomGroupMayMatch(std.heap.page_allocator, worker.reader(), inventory.files[0], inventory.files[0].row_groups[ordinal], self.groupPredicates(), worker.context, &stats);
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
        var bloom_stats: Stats = .{};
        if (!try bloomGroupMayMatch(std.heap.page_allocator, worker_reader.reader(), inventory.files[0], inventory.files[0].row_groups[ordinal], self.groupPredicates(), worker_reader.context, &bloom_stats)) return;
        var cursor = try @import("lake_parquet_cursor.zig").Cursor.init(std.heap.page_allocator, worker_reader.reader(), inventory, inventory.files[0].file_id, ordinal, names, .{
            .max_rows = self.limits.max_row_group_rows,
            .max_input_bytes = @min(self.limits.max_input_bytes, 2 * 1024 * 1024),
            .max_decoded_bytes = @min(self.limits.max_decoded_bytes, 2 * 1024 * 1024),
            .max_struct_allocation_bytes = @min(self.limits.max_decoded_bytes, 2 * 1024 * 1024),
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
        if (self.identity_only or self.partition_count != 1) return;
        if (self.lookahead != null) return;
        const reader = self.source.scanner.shared_reader orelse return;
        const io = reader.context.io orelse return;
        const plan = self.discovered orelse return;
        const current = self.page_cursor orelse return;
        const ordinal = for (plan.row_group_plan.row_groups[self.group_index..]) |input| {
            const group = plan.inventory.files[0].row_groups[input.row_group_ordinal];
            if (self.selection) |selection| if (!selection.rangeMayMatch(self.active_file, group.ordinal, 0, group.row_count)) continue;
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
        self.lookahead = @import("../../sql/parallel_scheduler.zig").global().submitTransient(io, @min(self.limits.max_input_bytes, 2 * 1024 * 1024) +| @min(self.limits.max_decoded_bytes, 2 * 1024 * 1024), warmGroup, .{ self, plan.inventory, ordinal, stable_names, stable_required });
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
        if (self.selection) |selection| {
            const group = self.page_cursor.?.group.ordinal;
            if (page.rows != 0 and !selection.rangeMayMatch(self.active_file, group, page.first, page.rows)) return false;
        }
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
    /// Probe standard split-block Bloom filters before opening data pages.
    /// Unknown physical/logical interpretations cannot supply negative evidence.
    fn bloomMayMatch(self: *Stream, file: external.FileEntry, group: external.RowGroup) !bool {
        return bloomGroupMayMatch(self.alloc, self.source.scanner.reader(), file, group, self.groupPredicates(), self.context, &self.stats);
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
        const cached_order = source.canonicalOrder();
        if (cached_order == null) try source.inventory.validate();
        const files = cached_order orelse try serving.ServingSource.canonicalFileOrder(alloc, source.inventory);
        return .{ .alloc = alloc, .source = source, .columns = columns, .predicates = predicates, .context = context, .limits = limits, .files = files, .owns_files = cached_order == null, .schema_contract = if (source.iceberg_schema) |schema| schema.columns else &.{} };
    }
    /// A contribution scan keeps the parent snapshot and delete applicability,
    /// but schedules only one pinned object. All footer offsets remain file-local.
    pub fn restrictFile(self: *Stream, index: usize) !void {
        if (index >= self.source.inventory.files.len or self.file_index != 0 or self.discovered != null) return error.InvalidExternalLakeIndexCoverage;
        const files = try self.alloc.dupe(usize, &.{index});
        if (self.owns_files) self.alloc.free(self.files);
        self.files = files;
        self.owns_files = true;
    }
    pub fn selectRows(self: *Stream, selection: @import("lake_row_selection.zig").Selection) !void {
        if (self.file_index != 0 or self.discovered != null or self.work != null) return error.InvalidLakeCandidateReference;
        var files: std.ArrayList(usize) = .empty;
        defer files.deinit(self.alloc);
        const rank = self.source.fileRanks();
        if (!self.owns_files and rank != null and selection.blocks.len == 0) {
            for (selection.coordinates) |coordinate| {
                if (files.items.len == 0 or files.items[files.items.len - 1] != coordinate.file) try files.append(self.alloc, coordinate.file);
            }
            std.mem.sort(usize, files.items, rank.?, struct {
                fn less(ranks: []const usize, left: usize, right: usize) bool {
                    return ranks[left] < ranks[right];
                }
            }.less);
            self.stats.files_pruned += self.files.len - files.items.len;
        } else for (self.files) |file| {
            if (selection.fileMayMatch(file)) try files.append(self.alloc, file) else self.stats.files_pruned += 1;
        }
        const retained = try files.toOwnedSlice(self.alloc);
        if (self.owns_files) self.alloc.free(self.files);
        self.files = retained;
        self.owns_files = true;
        self.selection = selection;
    }
    pub fn partitionCount(self: *Stream, maximum: usize) !usize {
        if (self.files.len == 0 or maximum < 2 or self.source.scanner.shared_reader == null) return 1;
        if (self.work) |work| return @min(maximum, work.units.len);
        const planning_io = self.source.scanner.shared_reader.?.context.io orelse self.context.io;
        var units: std.ArrayList(ScanWork.Unit) = .empty;
        errdefer units.deinit(self.alloc);
        const plans = try self.alloc.alloc(?ScanWork.FilePlan, self.source.inventory.files.len);
        @memset(plans, null);
        errdefer {
            for (plans) |*plan| if (plan.*) |*owned| owned.deinit(self.alloc);
            self.alloc.free(plans);
        }
        defer self.clearFile();
        if (planning_io != null and self.selection == null and self.files.len > 1) {
            // Each planner owns version pins, its reader and temporary file
            // state. The coordinator prepares deletes after a footer survives.
            const scheduler = @import("../../sql/parallel_scheduler.zig");
            var locked: scheduler.LockedAllocator = .{ .backing = self.alloc };
            const width = @min(maximum, scheduler.global().fanout(self.files.len, self.limits.max_decoded_bytes, 1024 * 1024));
            var offset: usize = 0;
            while (offset < self.files.len) {
                const count = @min(width, self.files.len - offset);
                var jobs: [8]PlanningJob = undefined;
                var tasks: [8]?scheduler.Task(anyerror!void) = @splat(null);
                var failures: [8]?anyerror = @splat(null);
                for (jobs[0..count], 0..) |*job, lane| job.* = .{ .parent = self, .alloc = locked.allocator(), .index = self.files[offset + lane] };
                // While tasks run, every request allocation uses the same
                // lock. Join the entire wave before touching coordinator state.
                for (jobs[0..count], 0..) |*job, lane| {
                    tasks[lane] = scheduler.global().submit(planning_io.?, 1024 * 1024, PlanningJob.run, .{job});
                    if (tasks[lane] == null) PlanningJob.run(job) catch |err| {
                        failures[lane] = err;
                    };
                }
                for (tasks[0..count], 0..) |*task, lane| if (task.*) |*pending| {
                    pending.await(planning_io.?) catch |err| {
                        failures[lane] = err;
                    };
                };
                defer for (jobs[0..count]) |*job| if (job.plan) |*plan| plan.deinit(self.alloc);
                for (failures[0..count]) |failure| if (failure) |err| return err;
                for (jobs[0..count]) |*job| {
                    self.stats.files_opened += job.stats.files_opened;
                    self.stats.files_pruned += job.stats.files_pruned;
                    self.stats.bloom_blocks_read += job.stats.bloom_blocks_read;
                    self.stats.bloom_groups_pruned += job.stats.bloom_groups_pruned;
                    if (job.plan) |*plan| {
                        try self.appendPlanUnits(&units, job.index, plan);
                        plans[job.index] = plan.*;
                        job.plan = null;
                    }
                }
                offset += count;
            }
        } else {
            for (self.files) |index| {
                self.clearFile();
                if (!self.fileMatches(self.source.inventory.files[index])) continue;
                try self.loadFile(index);
                if (self.selection) |selection| try selection.validateFile(index, self.discovered.?.inventory.files[0]);
                const plan = self.takeFilePlan() orelse continue;
                plans[index] = plan;
                try self.appendPlanUnits(&units, index, &plans[index].?);
            }
        }
        const work = try self.alloc.create(ScanWork);
        errdefer self.alloc.destroy(work);
        work.* = .{ .units = try units.toOwnedSlice(self.alloc), .plans = plans };
        std.mem.sort(ScanWork.Unit, work.units, {}, struct {
            fn less(_: void, a: ScanWork.Unit, b: ScanWork.Unit) bool {
                return if (a.bytes != b.bytes) a.bytes > b.bytes else if (a.file != b.file) a.file < b.file else a.ordinal < b.ordinal;
            }
        }.less);
        self.work = work;
        self.owns_work = true;
        return @min(maximum, work.units.len);
    }
    fn takeFilePlan(self: *Stream) ?ScanWork.FilePlan {
        const discovered = self.discovered orelse return null;
        const plan: ScanWork.FilePlan = .{ .discovered = discovered, .columns = self.file_columns, .logical_names = self.file_logical_names, .predicates = self.file_predicates, .position_starts = self.position_starts };
        self.discovered = null;
        self.position_starts = &.{};
        self.file_columns = &.{};
        self.file_logical_names = &.{};
        self.file_predicates = &.{};
        return plan;
    }
    fn appendPlanUnits(self: *Stream, units: *std.ArrayList(ScanWork.Unit), index: usize, plan: *ScanWork.FilePlan) !void {
        try self.prepareDeletes();
        if (self.source.prepared_deletes) |prepared| try prepared.bindFile(plan.discovered.inventory.files[0]);
        if (plan.position_starts.len == 0) plan.position_starts = try self.positionStarts(plan.discovered.inventory.files[0]);
        for (plan.discovered.row_group_plan.row_groups, 0..) |input, group_index| {
            const group = plan.discovered.inventory.files[0].row_groups[input.row_group_ordinal];
            if (group.row_count == 0 or !groupMayMatch(group, plan.predicates)) continue;
            try units.append(self.alloc, .{ .file = index, .ordinal = input.row_group_ordinal, .group_index = group_index, .bytes = group.total_byte_len });
        }
    }
    const PlanningJob = struct {
        parent: *Stream,
        alloc: Allocator,
        index: usize,
        plan: ?ScanWork.FilePlan = null,
        stats: Stats = .{},
        fn run(job: *@This()) anyerror!void {
            const parent = job.parent;
            try parent.context.ensureActive();
            if (!parent.fileMatches(parent.source.inventory.files[job.index])) {
                job.stats.files_pruned = 1;
                return;
            }
            var source = parent.source.*;
            source.alloc = job.alloc;
            source.inventory_owned = false;
            source.scanner.iceberg_delete_plan = null;
            source.prepared_deletes = null;
            source.delete_lease = null;
            source.versions = .empty;
            defer source.clearVersions();
            if (parent.source.versions.get(job.index)) |pinned| {
                var owned = pinned;
                owned.etag = try job.alloc.dupe(u8, pinned.etag);
                errdefer job.alloc.free(owned.etag);
                owned.version_id = try job.alloc.dupe(u8, pinned.version_id);
                errdefer job.alloc.free(owned.version_id);
                try source.versions.put(job.alloc, job.index, owned);
            }
            var reader = @import("lake_serving_cache.zig").Reader{
                .cache = parent.source.scanner.shared_reader.?.cache,
                .base = parent.source.scanner.shared_reader.?.base,
                .scope = parent.source.scanner.shared_reader.?.scope,
                .context = parent.source.scanner.shared_reader.?.context,
            };
            reader.base.client.allocator = job.alloc;
            source.scanner.shared_reader = &reader;
            source.scanner.object_reader.client.allocator = job.alloc;
            var worker: Stream = .{ .alloc = job.alloc, .source = &source, .columns = parent.columns, .predicates = parent.predicates, .context = parent.context, .limits = parent.limits, .files = &.{}, .owns_files = false, .schema_contract = parent.schema_contract, .identity_only = parent.identity_only };
            defer worker.deinit();
            try worker.loadFile(job.index);
            job.plan = worker.takeFilePlan();
            job.stats = worker.stats;
        }
    };
    pub fn deinit(self: *Stream) void {
        self.clearFile();
        if (self.owns_work) if (self.work) |work| {
            for (work.plans) |*plan| if (plan.*) |*owned| owned.deinit(self.alloc);
            self.alloc.free(work.plans);
            self.alloc.free(work.units);
            self.alloc.destroy(work);
        };
        if (self.owns_files) self.alloc.free(self.files);
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
        if (!self.borrowed_plan) {
            self.alloc.free(self.position_starts);
            if (self.discovered) |*plan| plan.deinit(self.alloc);
            for (self.file_columns) |name| self.alloc.free(name);
            self.alloc.free(self.file_columns);
            self.alloc.free(self.file_logical_names);
            self.alloc.free(self.file_predicates);
        }
        if (self.hydration_plan) |lease| {
            // Delete offsets are fresh request state, never cached metadata.
            self.alloc.free(self.position_starts);
            lease.release();
            self.hydration_plan = null;
        }
        self.position_starts = &.{};
        self.borrowed_plan = false;
        self.discovered = null;
        self.file_columns = &.{};
        self.file_logical_names = &.{};
        self.file_predicates = &.{};
        self.group_index = 0;
        self.bloom_next_input = 0;
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
                    self.startBloomLookahead();
                    const input = plan.row_group_plan.row_groups[self.group_index];
                    self.group_index += 1;
                    if (self.work_ordinal != null) self.group_index = plan.row_group_plan.row_groups.len;
                    if (self.work_ordinal) |ordinal| {
                        if (input.row_group_ordinal != ordinal) continue;
                    } else if (self.partition_count > 1 and self.files.len == 1 and (self.group_index - 1) * self.partition_count / plan.row_group_plan.row_groups.len != self.partition_index) continue;
                    const groups = plan.inventory.files[0].row_groups;
                    if (input.row_group_ordinal >= groups.len or groups[input.row_group_ordinal].ordinal != input.row_group_ordinal) return error.InvalidParquetRowGroupBatch;
                    const group = groups[input.row_group_ordinal];
                    if (self.selection) |selection| if (!selection.rangeMayMatch(self.active_file, group.ordinal, 0, group.row_count)) {
                        self.stats.groups_pruned += 1;
                        continue;
                    };
                    if (!groupMayMatch(group, self.groupPredicates())) {
                        self.stats.groups_pruned += 1;
                        continue;
                    }
                    if (!try self.bloomMayMatch(plan.inventory.files[0], group)) {
                        self.stats.groups_pruned += 1;
                        self.stats.bloom_groups_pruned += 1;
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
            const index = if (self.work != null and !self.owns_work) {
                const unit = if (self.ordered_next) |position| blk: {
                    if (position == self.ordered_end) return null;
                    self.ordered_next = position + 1;
                    break :blk self.work.?.units[position];
                } else self.work.?.claim() orelse return null;
                const plan = self.work.?.plans[unit.file].?;
                self.active_file = unit.file;
                self.work_ordinal = unit.ordinal;
                self.discovered = plan.discovered;
                self.position_starts = plan.position_starts;
                self.file_columns = plan.columns;
                self.file_logical_names = plan.logical_names;
                self.file_predicates = plan.predicates;
                self.borrowed_plan = true;
                self.group_index = unit.group_index;
                continue;
            } else blk: {
                if (self.file_index == self.files.len) return null;
                const index = self.files[self.file_index];
                self.file_index += 1;
                if (self.partition_count > 1 and self.files.len > 1 and (self.file_index - 1) * self.partition_count / self.files.len != self.partition_index) continue;
                break :blk index;
            };
            const file = self.source.inventory.files[index];
            self.active_file = index;
            if (self.selection) |selection| if (!selection.fileMayMatch(index)) {
                self.stats.files_pruned += 1;
                continue;
            };
            if (!self.fileMatches(file)) {
                self.stats.files_pruned += 1;
                continue;
            }
            try self.loadFile(index);
            if (self.selection) |selection| {
                const plan = self.discovered orelse return error.InvalidLakeCandidateReference;
                try selection.validateFile(index, plan.inventory.files[0]);
            }
        }
    }
    fn prefetchNext(self: *Stream) !void {
        if (self.partition_count != 1) return;
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
            if (self.selection) |selection| if (!selection.rangeMayMatch(self.active_file, group.ordinal, 0, group.row_count)) continue;
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
            if (self.source.lazy_versions and !self.source.isFilePinned(index)) continue;
            const upcoming = self.source.fileAt(index);
            if (!self.fileMatches(upcoming)) continue;
            const object = try @import("lake_range_io.zig").objectRefForExternalFileUri(upcoming);
            const footer = try @import("lake_range_io.zig").planParquetFooterRead(object, 64 * 1024);
            try reader.prefetch(&.{footer});
            return;
        }
    }
    fn loadFile(self: *Stream, index: usize) !void {
        // Metadata and partition pruning happen before statting data objects.
        // Pin each surviving file once, before any footer or payload read.
        if (self.source.lazy_versions and !self.source.isFilePinned(index)) {
            var entry = self.source.fileAt(index);
            const sparse = !self.source.inventory_owned;
            // The provider can replace version strings before a later check
            // fails. Preserve those replacements for owned inventory teardown.
            defer if (!sparse) {
                self.source.inventory.files[index] = entry;
            };
            if (sparse) {
                entry.etag = try self.source.alloc.dupe(u8, entry.etag);
                errdefer self.source.alloc.free(entry.etag);
                entry.version_id = try self.source.alloc.dupe(u8, entry.version_id);
            }
            errdefer if (sparse) {
                self.source.alloc.free(entry.etag);
                self.source.alloc.free(entry.version_id);
            };
            var single = self.source.inventory;
            single.files = @as(*[1]@TypeOf(entry), @ptrCast(&entry));
            single.deleted_row_groups = &.{};
            if (self.source.immutable_objects and self.source.verified_files != null) {
                const verified = self.source.verified_files.?.get(entry.file_id) orelse return error.ExternalLakeSnapshotMismatch;
                if (!std.mem.eql(u8, entry.object_uri, verified.object_uri) or entry.byte_len != verified.byte_len) return error.ExternalLakeSnapshotMismatch;
                const etag = try self.source.alloc.dupe(u8, verified.etag);
                errdefer self.source.alloc.free(etag);
                const version = try self.source.alloc.dupe(u8, verified.version_id);
                self.source.alloc.free(entry.etag);
                self.source.alloc.free(entry.version_id);
                entry.etag = etag;
                entry.version_id = version;
            } else try iceberg.pinInventoryDataFileObjectVersions(self.source.alloc, self.source.scanner.object_reader.client, &single);
            if (sparse) try self.source.versions.put(self.source.alloc, index, entry) else {
                self.source.inventory.files[index] = entry;
                self.source.pinned_files[index] = true;
            }
        }
        // Candidate hydration windows share immutable projected file readers.
        // Fresh version pinning above and delete preparation below remain
        // request-owned; cached plans contain only fully owned metadata.
        if (self.selection != null and self.source.scanner.shared_reader != null) {
            const reader = self.source.scanner.shared_reader.?;
            const file = self.source.fileAt(index);
            const interpretation = try std.json.Stringify.valueAlloc(self.alloc, .{ .version = "hydration-file-plan-v1", .file = file.file_id, .format = self.source.inventory.format, .source = self.source.inventory.source_id, .source_uri = self.source.inventory.source_uri, .snapshot = self.source.inventory.snapshot_id, .schema = self.source.inventory.schema_fingerprint, .columns = self.columns, .contract = self.schema_contract, .predicates = self.predicates, .limits = self.limits }, .{});
            defer self.alloc.free(interpretation);
            const key = try reader.objectKey(self.alloc, .{ .object = try @import("lake_range_io.zig").objectRefForExternalFileUri(file), .range = .{ .offset = 0, .len = file.byte_len }, .purpose = .parquet_footer }, interpretation);
            const ready_plan = reader.cache.decoded.lookup(key);
            // Resolve dependencies before taking decoder admission, including
            // the exclusive retry lane. A loader never waits on itself.
            const footer = if (ready_plan == null) try reader.footer(file) else null;
            defer if (footer) |lease| lease.release();
            var loader = struct {
                stream: *Stream,
                footer: ?@import("lake_decoded_cache.zig").Lease,
                index: usize,
                fn clone(a: Allocator, value: anytype) !@TypeOf(value) {
                    const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
                    defer a.free(bytes);
                    return std.json.parseFromSliceLeaky(@TypeOf(value), a, bytes, .{ .allocate = .alloc_always });
                }
                fn load(raw: *anyopaque, item: *@import("lake_decoded_cache.zig").Item) !void {
                    const self_loader: *@This() = @ptrCast(@alignCast(raw));
                    const a = item.arena.allocator();
                    var worker = self_loader.stream.*;
                    worker.alloc = a;
                    worker.planning_footer = self_loader.footer;
                    worker.columns = try clone(a, worker.columns);
                    worker.schema_contract = try clone(a, worker.schema_contract);
                    worker.predicates = try clone(a, worker.predicates);
                    worker.discovered = null;
                    worker.file_columns = &.{};
                    worker.file_logical_names = &.{};
                    worker.file_predicates = &.{};
                    try worker.loadPhysicalFile(self_loader.index);
                    const plan = try a.create(HydrationPlan);
                    plan.* = .{ .discovered = worker.discovered, .columns = worker.file_columns, .logical_names = worker.file_logical_names, .predicates = worker.file_predicates };
                    item.payload = .{ .extension = plan };
                }
            }{ .stream = self, .index = index, .footer = footer };
            var load_context = self.context;
            load_context.io = reader.context.io;
            const lease = ready_plan orelse try reader.cache.decoded.acquire(key, 64 * 1024 * 1024, load_context, .{ .ptr = &loader, .load = @TypeOf(loader).load });
            self.hydration_plan = lease;
            self.borrowed_plan = true;
            const plan: *const HydrationPlan = @ptrCast(@alignCast(lease.item.payload.extension));
            self.discovered = plan.discovered;
            self.file_columns = plan.columns;
            self.file_logical_names = plan.logical_names;
            self.file_predicates = plan.predicates;
        } else try self.loadPhysicalFile(index);
        if (self.discovered == null) return;
        self.stats.files_opened += 1;
        try self.prepareDeletes();
        try self.bindPositions();
    }
    const HydrationPlan = struct {
        discovered: ?parquet.DiscoveredObjectRangeRowGroupPlan,
        columns: []const []const u8,
        logical_names: []const []const u8,
        predicates: []const Predicate,
    };
    fn loadPhysicalFile(self: *Stream, index: usize) !void {
        var file_entry = self.source.fileAt(index);
        var inventory = self.source.inventory;
        inventory.files = @as(*[1]@TypeOf(file_entry), @ptrCast(&file_entry));
        inventory.deleted_row_groups = &.{};
        if (self.schema_contract.len != 0 or self.source.scanner.shared_reader != null) {
            const lease = if (self.planning_footer) |footer| footer.retain() else if (self.source.scanner.shared_reader) |reader| try reader.footer(inventory.files[0]) else null;
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
                    const actual_kind = try @import("lake_schema.zig").parquetKind(found);
                    if (!std.mem.eql(u8, actual_kind, expected.kind) or (expected.required and found.nullable)) {
                        std.log.warn("external lake column schema mismatch column={s} expected={s} actual={s} required={} nullable={}", .{ expected.name, expected.kind, actual_kind, expected.required, found.nullable });
                        return error.ExternalLakeSchemaMismatch;
                    }
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
        std.mem.sort(parquet.ObjectRangeRowGroupInput, self.discovered.?.row_group_plan.row_groups, {}, struct {
            fn less(_: void, a: parquet.ObjectRangeRowGroupInput, b: parquet.ObjectRangeRowGroupInput) bool {
                return a.row_group_ordinal < b.row_group_ordinal;
            }
        }.less);
    }
    pub fn prepareDeletes(self: *Stream) !void {
        try self.context.ensureActive();
        if (self.source.scanner.iceberg_delete_plan) |delete_plan| {
            if (self.source.prepared_deletes == null) {
                const request: iceberg.DeleteRowRefsReadRequest = .{
                    .reader = self.source.scanner.reader(),
                    .client = self.source.scanner.object_reader.client,
                    .cache = self.source.scanner.cache,
                    .data_inventory = self.source.inventory,
                    .delete_plan = delete_plan,
                    .coalesce_options = self.source.scanner.coalesce_options,
                    .materialization_limits = .{ .max_struct_allocation_bytes = self.limits.max_decoded_bytes, .max_input_bytes = self.limits.max_input_bytes, .max_decoded_bytes = self.limits.max_decoded_bytes },
                };
                if (self.source.scanner.shared_reader) |reader| {
                    var hash = std.crypto.hash.sha2.Sha256.init(.{});
                    hash.update(&reader.scope);
                    hash.update("prepared-deletes-v1");
                    hash.update(self.source.inventory.snapshot_id);
                    const policy = try std.json.Stringify.valueAlloc(self.alloc, .{ .materialization = request.materialization_limits, .application = request.application_limits }, .{});
                    defer self.alloc.free(policy);
                    hash.update(policy);
                    // Revalidate delete object versions even on an index hit.
                    // A replaced object never inherits a cached equality set.
                    var versions = std.crypto.hash.sha2.Sha256.init(.{});
                    const index_identity = if (self.source.plan_identity) |plan_key|
                        try std.json.Stringify.valueAlloc(self.alloc, plan_key, .{})
                    else
                        try std.json.Stringify.valueAlloc(self.alloc, .{ .deletes = delete_plan, .inventory = self.source.inventory }, .{});
                    defer self.alloc.free(index_identity);
                    hash.update(index_identity);
                    var cacheable = true;
                    var delete_client = request.client.?;
                    for (delete_plan.files) |file| {
                        const location = try @import("lake_range_io.zig").objectLocationForUri(file.file_path);
                        var meta = try delete_client.statObject(location.bucket, location.key);
                        defer meta.deinit(request.client.?.allocator);
                        if (meta.content_length != file.file_size_in_bytes) return error.IcebergManifestLengthMismatch;
                        @import("lake_prepared_deletes.zig").Prepared.hashObjectVersion(&versions, file.file_path, meta.etag orelse "", meta.version_id orelse "");
                        // An unversioned provider cannot safely reuse an index.
                        if (meta.etag == null and meta.version_id == null) cacheable = false;
                    }
                    const version_key = versions.finalResult();
                    hash.update(&version_key);
                    const key = hash.finalResult();
                    const cached = if (cacheable) reader.cache.decoded.lookup(key) else null;
                    const lease = cached orelse blk: {
                        const owned = try reader.cache.decoded.create(64 * 1024 * 1024);
                        errdefer owned.release();
                        owned.item.payload = .{ .prepared = try @import("lake_prepared_deletes.zig").Prepared.create(owned.item.budget.allocator(), request) };
                        try self.context.ensureActive();
                        if (cacheable and std.mem.eql(u8, &version_key, &owned.item.payload.prepared.object_versions)) reader.cache.decoded.publish(key, owned);
                        break :blk owned;
                    };
                    self.source.delete_lease = lease;
                    self.source.prepared_deletes = lease.item.payload.prepared;
                } else self.source.prepared_deletes = try @import("lake_prepared_deletes.zig").Prepared.create(self.source.alloc, request);
            }
        }
    }
    fn bindPositions(self: *Stream) !void {
        if (self.position_starts.len == 0) self.position_starts = try self.positionStarts(self.discovered.?.inventory.files[0]);
    }
    fn positionStarts(self: *Stream, file: external.FileEntry) ![]const u64 {
        const prepared = self.source.prepared_deletes orelse return &.{};
        if (prepared.positions.count() == 0) return &.{};
        const starts = try self.alloc.alloc(u64, file.row_groups.len);
        errdefer self.alloc.free(starts);
        var total: u64 = 0;
        for (file.row_groups, starts) |group, *start| {
            start.* = total;
            total = try std.math.add(u64, total, group.row_count);
        }
        return starts;
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
    pub fn deleteMask(self: Stream, a: Allocator, batch: types.ColumnBatch, selected: []bool) !void {
        for (batch.row_refs, selected) |ref, *keep| if (keep.* and self.isDeleted(ref)) {
            keep.* = false;
        };
        if (self.source.prepared_deletes) |prepared|
            try prepared.maskPositions(a, self.discovered.?.inventory.files[0], batch, selected, self.position_starts);
    }
    pub fn isDeletedBatch(self: Stream, a: Allocator, batch: types.ColumnBatch, index: usize) !bool {
        if (self.isDeleted(batch.row_refs[index])) return true;
        if (self.source.prepared_deletes) |prepared|
            return prepared.matchesPositions(a, self.discovered.?.inventory.files[0], batch, index, self.position_starts);
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

/// Parquet split-block Bloom membership, XXH64's low word and eight salts.
fn splitBlockMayContain(bytes: []const u8, hash: u32) bool {
    if (bytes.len != 32) return true;
    const salts = [_]u32{ 0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31 };
    for (salts, 0..) |salt, lane| {
        const bit: u5 = @intCast((hash *% salt) >> 27);
        if (std.mem.readInt(u32, bytes[lane * 4 ..][0..4], .little) & (@as(u32, 1) << bit) == 0) return false;
    }
    return true;
}

test "external lake split block Bloom hash masks preserve inserted values" {
    var bytes: [32]u8 = @splat(0);
    const hash: u32 = @truncate(std.hash.XxHash64.hash(0, "present"));
    const salts = [_]u32{ 0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31 };
    try std.testing.expect(!splitBlockMayContain(&bytes, hash));
    for (salts, 0..) |salt, lane| {
        const bit: u5 = @intCast((hash *% salt) >> 27);
        std.mem.writeInt(u32, bytes[lane * 4 ..][0..4], @as(u32, 1) << bit, .little);
    }
    try std.testing.expect(splitBlockMayContain(&bytes, hash));
    try std.testing.expect(!splitBlockMayContain(&bytes, @truncate(std.hash.XxHash64.hash(0, "absent"))));
    bytes[0..4].* = @splat(0);
    try std.testing.expect(!splitBlockMayContain(&bytes, hash));
}

fn bloomProbeUseful(chunk: external.ColumnChunk, predicate: Predicate) bool {
    if (predicate.op != .eq or chunk.bloom_filter_offset == null or !std.mem.eql(u8, chunk.column_id, predicate.column)) return false;
    return switch (predicate.value) {
        .integer => |value| integer: {
            if (chunk.logical_type.len != 0 and !std.mem.startsWith(u8, chunk.logical_type, "int")) break :integer false;
            if (std.mem.eql(u8, chunk.physical_type, "int32")) {
                if (std.math.cast(i32, value) == null) break :integer false;
            } else if (!std.mem.eql(u8, chunk.physical_type, "int64")) break :integer false;
            break :integer !(chunk.stats_min_i64 != null and chunk.stats_max_i64 != null and chunk.stats_min_i64.? == value and chunk.stats_max_i64.? == value);
        },
        .bytes => |value| std.mem.eql(u8, chunk.physical_type, "byte_array") and
            (chunk.logical_type.len == 0 or std.mem.eql(u8, chunk.logical_type, "string")) and
            !(chunk.stats_min_bytes != null and chunk.stats_max_bytes != null and std.mem.eql(u8, chunk.stats_min_bytes.?, value) and std.mem.eql(u8, chunk.stats_max_bytes.?, value)),
        .boolean => false,
    };
}

fn bloomGroupMayMatch(alloc: Allocator, reader: parquet.ObjectRangeReader, file: external.FileEntry, group: external.RowGroup, predicates: []const Predicate, context: Context, stats: *Stats) !bool {
    const ranges = @import("lake_range_io.zig");
    const metadata = @import("lake_parquet_metadata.zig");
    for (predicates) |predicate| {
        if (predicate.op != .eq) continue;
        for (group.column_chunks) |chunk| {
            if (!std.mem.eql(u8, predicate.column, chunk.column_id)) continue;
            if (!bloomProbeUseful(chunk, predicate)) continue;
            const offset = chunk.bloom_filter_offset orelse continue;
            var value_bytes: [8]u8 = undefined;
            const bytes: []const u8 = switch (predicate.value) {
                .integer => |value| integer: {
                    if (chunk.logical_type.len != 0 and !std.mem.startsWith(u8, chunk.logical_type, "int")) continue;
                    if (std.mem.eql(u8, chunk.physical_type, "int32")) {
                        const narrow = std.math.cast(i32, value) orelse continue;
                        std.mem.writeInt(i32, value_bytes[0..4], narrow, .little);
                        break :integer value_bytes[0..4];
                    }
                    if (!std.mem.eql(u8, chunk.physical_type, "int64")) continue;
                    std.mem.writeInt(i64, &value_bytes, value, .little);
                    break :integer &value_bytes;
                },
                .bytes => |value| text: {
                    if (!std.mem.eql(u8, chunk.physical_type, "byte_array") or
                        (chunk.logical_type.len != 0 and !std.mem.eql(u8, chunk.logical_type, "string"))) continue;
                    break :text value;
                },
                .boolean => continue,
            };
            if (offset >= file.byte_len) continue;
            const available = if (chunk.bloom_filter_length) |len| @min(file.byte_len - offset, len) else file.byte_len - offset;
            const header_len: usize = @intCast(@min(available, 256));
            if (header_len == 0) continue;
            try context.ensureActive();
            const object = try ranges.objectRefForExternalFileUri(file);
            const header = try reader.readPlannedLease(alloc, .{ .object = object, .range = .{ .offset = offset, .len = header_len }, .purpose = .parquet_page_index });
            defer header.release();
            const decoded = metadata.parseBloomHeader(header.bytes) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => continue,
            } orelse continue;
            if (decoded.header_bytes > available or decoded.bitset_bytes > available - decoded.header_bytes) continue;
            const hash = std.hash.XxHash64.hash(0, bytes);
            const block: u64 = ((hash >> 32) * @as(u64, @intCast(decoded.bitset_bytes / 32))) >> 32;
            const relative: usize = @intCast(decoded.header_bytes + block * 32);
            stats.bloom_blocks_read += 1;
            // Small filters already fit in the bounded header lease. Reuse
            // those bytes instead of issuing another remote request.
            if (relative <= header.bytes.len and header.bytes.len - relative >= 32) {
                if (!splitBlockMayContain(header.bytes[relative..][0..32], @truncate(hash))) return false;
            } else {
                const probe = try reader.readPlannedLease(alloc, .{ .object = object, .range = .{ .offset = offset + relative, .len = 32 }, .purpose = .parquet_page_index });
                defer probe.release();
                if (!splitBlockMayContain(probe.bytes, @truncate(hash))) return false;
            }
        }
    }
    return true;
}

test "external lake Bloom pruning reads only metadata and one block with legacy length fallback" {
    const a = std.testing.allocator;
    const Fake = struct {
        bytes: [1024]u8 = @splat(0),
        reads: usize = 0,
        fn read(raw: *anyopaque, alloc: Allocator, _: []const u8, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            if (offset < 4 or offset + len > self.bytes.len) return error.TestUnexpectedResult;
            return alloc.dupe(u8, self.bytes[@intCast(offset)..][0..len]);
        }
    };
    var fake: Fake = .{};
    const header = [_]u8{ 0x15, 0x80, 0x01, 0x1c, 0x1c, 0, 0, 0x1c, 0x1c, 0, 0, 0x1c, 0x1c, 0, 0, 0 };
    @memcpy(fake.bytes[4..][0..header.len], &header);
    var encoded: [8]u8 = undefined;
    std.mem.writeInt(i64, &encoded, 42, .little);
    const hash = std.hash.XxHash64.hash(0, &encoded);
    const block: usize = @intCast(((hash >> 32) * 2) >> 32);
    const salts = [_]u32{ 0x47b6137b, 0x44974d91, 0x8824ad5b, 0xa2b7289d, 0x705495c7, 0x2df1424b, 0x9efc4947, 0x5c6bfb31 };
    for (salts, 0..) |salt, lane| {
        const bit: u5 = @intCast((@as(u32, @truncate(hash)) *% salt) >> 27);
        std.mem.writeInt(u32, fake.bytes[4 + header.len + block * 32 + lane * 4 ..][0..4], @as(u32, 1) << bit, .little);
    }
    var chunk: external.ColumnChunk = .{ .column_id = @constCast("id"), .physical_type = @constCast("int64"), .file_offset = 100, .compressed_len = 1, .bloom_filter_offset = 4, .bloom_filter_length = header.len + 64 };
    const group: external.RowGroup = .{ .ordinal = 0, .row_count = 10, .column_chunks = @as([*]external.ColumnChunk, @ptrCast(&chunk))[0..1] };
    const file: external.FileEntry = .{ .file_id = @constCast("part"), .object_uri = @constCast("s3://bucket/part"), .etag = @constCast("v1"), .byte_len = fake.bytes.len, .row_count = 10, .row_groups = &.{} };
    const reader: parquet.ObjectRangeReader = .{ .ctx = &fake, .read_range_alloc = Fake.read };
    var stats: Stats = .{};
    const present = [_]Predicate{.{ .column = "id", .op = .eq, .value = .{ .integer = 42 } }};
    const absent = [_]Predicate{.{ .column = "id", .op = .eq, .value = .{ .integer = 43 } }};
    try std.testing.expect(try bloomGroupMayMatch(a, reader, file, group, &present, .{}, &stats));
    try std.testing.expect(!try bloomGroupMayMatch(a, reader, file, group, &absent, .{}, &stats));
    chunk.bloom_filter_length = null;
    try std.testing.expect(try bloomGroupMayMatch(a, reader, file, group, &present, .{}, &stats));
    try std.testing.expect(!try bloomGroupMayMatch(a, reader, file, group, &absent, .{}, &stats));
    try std.testing.expectEqual(@as(usize, 4), stats.bloom_blocks_read);
    try std.testing.expectEqual(@as(usize, 4), fake.reads);
    // A larger filter reads only its header prefix and the chosen block.
    fake.bytes[6] = 0x08; // 512-byte bitset (zigzag varint 1024).
    chunk.bloom_filter_length = header.len + 512;
    const large_block: usize = @intCast(((hash >> 32) * 16) >> 32);
    try std.testing.expect(header.len + large_block * 32 >= 256);
    for (salts, 0..) |salt, lane| {
        const bit: u5 = @intCast((@as(u32, @truncate(hash)) *% salt) >> 27);
        std.mem.writeInt(u32, fake.bytes[4 + header.len + large_block * 32 + lane * 4 ..][0..4], @as(u32, 1) << bit, .little);
    }
    try std.testing.expect(try bloomGroupMayMatch(a, reader, file, group, &present, .{}, &stats));
    try std.testing.expectEqual(@as(usize, 6), fake.reads);

    chunk.stats_min_i64 = 42;
    chunk.stats_max_i64 = 42;
    const constant_reads = fake.reads;
    try std.testing.expect(try bloomGroupMayMatch(a, reader, file, group, &present, .{}, &stats));
    try std.testing.expectEqual(constant_reads, fake.reads);
    chunk.stats_min_i64 = null;
    chunk.stats_max_i64 = null;
    const reads = fake.reads;
    chunk.logical_type = @constCast("timestamp_nanos");
    try std.testing.expect(try bloomGroupMayMatch(a, reader, file, group, &absent, .{}, &stats));
    try std.testing.expectEqual(reads, fake.reads);
    chunk.logical_type = @constCast("");
    fake.bytes[8] = 0x2c;
    try std.testing.expect(try bloomGroupMayMatch(a, reader, file, group, &absent, .{}, &stats));
}
