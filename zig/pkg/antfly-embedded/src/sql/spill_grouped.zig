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

//! Partition exact typed states before reducing; use ordered merging for
//! order-sensitive aggregates and partitions that exceed bounded admission.
const std = @import("std");
const operators = @import("operators.zig");
const scalar = @import("scalar.zig");
const spill = @import("spill.zig");
const Datum = scalar.Datum;
const Allocator = std.mem.Allocator;
pub const Grouped = struct {
    const PartitionInput = struct { file: spill.Sequential, ordinal_base: u64, closed: bool = false };
    const Reduction = struct {
        child: Grouped,
        exact: bool,
        output: ?*@import("parallel_output.zig").Pipe = null,
        task: ?@import("parallel_scheduler.zig").Task(anyerror!void) = null,
        child_closed: bool = false,
        fn run(self: *Reduction) anyerror!void {
            defer {
                self.child.deinit();
                self.child_closed = true;
            }
            var failure: ?anyerror = null;
            self.reduceInto(self.output.?) catch |err| {
                failure = err;
            };
            self.output.?.finish(failure);
        }
        fn reduceInto(self: *Reduction, output: *@import("parallel_output.zig").Pipe) !void {
            var arena = std.heap.ArenaAllocator.init(self.child.a);
            defer arena.deinit();
            while (true) {
                _ = arena.reset(.retain_capacity);
                const result = (try self.child.nextBatch(arena.allocator(), 64, self.exact)) orelse break;
                try output.appendBatch(result.aggregates, result.keys, result.ordinals);
            }
        }
        fn close(self: *Reduction) void {
            if (self.output) |pipe| pipe.stop();
            if (self.task) |*task| {
                task.cancel(self.child.sort.manager.io) catch {};
                self.task = null;
            }
            const a = self.child.a;
            if (self.output) |pipe| pipe.close();
            if (!self.child_closed) self.child.deinit();
            a.destroy(self);
        }
    };
    a: Allocator,
    sort: spill.Sort,
    specs: []const operators.AggregateSpec,
    state_arena: std.heap.ArenaAllocator,
    read_arena: std.heap.ArenaAllocator,
    pending: ?operators.Row = null,
    output_count: usize = 0,
    partitioned: bool = false,
    partitions: [8]?spill.Sequential = @splat(null),
    /// Transferred checkpoint streams retain their existing typed block codec
    /// and extents. Global hash owners reduce all source streams together.
    partition_inputs: [8]std.ArrayList(PartitionInput) = @splat(.empty),
    partition_count: usize,
    partition_index: usize = 0,
    local: ?*operators.Grouped = null,
    fallback: ?*Grouped = null,
    bytes: usize,
    native_reduced_rows: usize = 0,
    parallel: bool = true,
    reductions: [8]?*Reduction = @splat(null),
    next_lane: usize = 0,
    reduction_lanes: usize = 0,
    reductions_started: usize = 0,
    result_view: ?@import("parallel_output.zig").Pipe.View = null,
    pub fn init(a: Allocator, manager: *spill.Manager, specs: []const operators.AggregateSpec, key_count: usize, bytes: usize) !Grouped {
        const backing = manager.allocator();
        _ = a;
        const orders = try backing.alloc(operators.Order, key_count);
        @memset(orders, .{});
        return .{ .a = backing, .sort = spill.Sort.init(backing, manager, orders, bytes / 4), .specs = specs, .state_arena = std.heap.ArenaAllocator.init(backing), .read_arena = std.heap.ArenaAllocator.init(backing), .bytes = bytes, .partition_count = std.math.clamp(bytes / (512 * 1024), 2, 8), .partitioned = bytes >= 64 * 1024 and for (specs) |spec| {
            if (!operators.exactMergeable(spec)) break false;
        } else true };
    }
    pub fn deinit(self: *Grouped) void {
        if (self.result_view) |view| view.deinit();
        for (&self.reductions) |*slot| if (slot.*) |job| {
            job.close();
            slot.* = null;
        };
        for (&self.partitions) |*file| if (file.*) |*open| open.close();
        for (&self.partition_inputs) |*inputs| {
            for (inputs.items) |*input| if (!input.closed) input.file.close();
            inputs.deinit(self.a);
        }
        if (self.local) |local| local.deinit();
        if (self.fallback) |fallback| {
            fallback.deinit();
            self.a.destroy(fallback);
        }
        self.a.free(self.sort.orders);
        self.sort.deinit();
        self.state_arena.deinit();
        self.read_arena.deinit();
    }
    pub fn canTransferPartitions(self: *const Grouped) bool {
        return self.partitioned and self.partition_index == 0 and self.reduction_lanes == 0 and self.local == null and self.fallback == null and self.output_count == 0;
    }
    /// Reserve and seal before moving ownership. Failures leave every source
    /// readable; transfer itself cannot allocate or consume source rows.
    pub fn preparePartitionTransfer(self: *Grouped, sources: []const *Grouped, bases: []const u64) !void {
        if (!self.canTransferPartitions()) return error.InvalidSqlBackendResponse;
        if (bases.len != sources.len) return error.InvalidSqlBackendResponse;
        for (sources, bases, 0..) |source, base, ordinal| {
            if (source == self) return error.InvalidSqlBackendResponse;
            for (sources[0..ordinal]) |previous| if (previous == source) return error.InvalidSqlBackendResponse;
            for (source.partition_inputs) |inputs| for (inputs.items) |input| {
                _ = try std.math.add(u64, base, input.ordinal_base);
            };
            if (!source.canTransferPartitions() or source.partition_count != self.partition_count or source.sort.manager != self.sort.manager or source.sort.orders.len != self.sort.orders.len or source.specs.len != self.specs.len) return error.InvalidSqlBackendResponse;
            for (source.specs, self.specs) |left, right| if (!std.meta.eql(left, right)) return error.InvalidSqlBackendResponse;
        }
        for (0..self.partition_count) |index| {
            var count: usize = 0;
            for (sources) |source| {
                count += @intFromBool(source.partitions[index] != null) + source.partition_inputs[index].items.len;
                if (source.partitions[index]) |*file| try file.seal();
                for (source.partition_inputs[index].items) |*input| try input.file.seal();
            }
            try self.partition_inputs[index].ensureUnusedCapacity(self.a, count);
        }
    }
    pub fn transferPartitions(self: *Grouped, source: *Grouped, ordinal_base: u64) void {
        std.debug.assert(source.canTransferPartitions() and source.partition_count == self.partition_count);
        for (0..self.partition_count) |index| {
            const target = &self.partition_inputs[index];
            if (source.partitions[index]) |file| {
                target.appendAssumeCapacity(.{ .file = file, .ordinal_base = ordinal_base });
                source.partitions[index] = null;
            }
            for (source.partition_inputs[index].items) |input| target.appendAssumeCapacity(.{ .file = input.file, .ordinal_base = ordinal_base + input.ordinal_base });
            source.partition_inputs[index].clearRetainingCapacity();
        }
    }
    pub fn partial(self: *Grouped, keys: []const Datum, states: []const operators.Aggregate, ordinal: u64) !void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        const values = try a.alloc(Datum, 1 + states.len);
        values[0] = Datum.json(.{ .bool = true });
        for (states, values[1..]) |state, *value| {
            if (state.distinct) {
                var base = try operators.Aggregate.initTyped(a, state.kind, state.input_type, state.input_element);
                defer base.deinit();
                base.distinct = true;
                value.* = try @import("aggregate_partial.zig").cell(a, base);
            } else value.* = try @import("aggregate_partial.zig").cell(a, state);
        }
        try self.append(.{ .keys = keys, .values = values, .ordinal = ordinal });
        for (states, 0..) |state, slot| if (state.distinct) {
            for (state.distinct_values.items) |entry| {
                try self.append(.{ .keys = keys, .values = &.{ Datum.json(.{ .integer = @intCast(slot) }), entry.row.row.values[0] }, .ordinal = ordinal });
            }
            if (state.kind == .pattern_set and state.patterns.?.has_null) try self.append(.{ .keys = keys, .values = &.{ Datum.json(.{ .integer = @intCast(slot) }), .{} }, .ordinal = ordinal });
        };
    }
    pub fn appendPartial(self: *Grouped, keys: []const Datum, cells: []const Datum, ordinal: u64) !void {
        if (cells.len != self.specs.len) return error.InvalidSqlSpill;
        const values = try self.a.alloc(Datum, cells.len + 1);
        defer self.a.free(values);
        values[0] = Datum.json(.{ .bool = true });
        @memcpy(values[1..], cells);
        try self.append(.{ .keys = keys, .values = values, .ordinal = ordinal });
    }
    pub fn add(self: *Grouped, keys: []const Datum, inputs: []const Datum, ordinal: u64) !void {
        const values = try self.a.alloc(Datum, inputs.len + 1);
        defer self.a.free(values);
        values[0] = Datum.json(.{ .bool = false });
        @memcpy(values[1..], inputs);
        try self.append(.{ .keys = keys, .values = values, .ordinal = ordinal });
    }
    /// Scatter bounded windows into partition-local column blocks. Hashes
    /// are semantic grouping hashes already computed by the upstream census;
    /// per-partition selections preserve input order and original ordinals.
    pub fn addBatch(self: *Grouped, keys: []const @import("execution_batch.zig").Batch, inputs: []const @import("execution_batch.zig").Batch, hashes: []const ?u64, ordinals: []const u64) !void {
        if (!self.partitioned or inputs.len != self.specs.len or hashes.len != ordinals.len) return error.InvalidSqlSpill;
        const Batch = @import("execution_batch.zig").Batch;
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        var begin: usize = 0;
        while (begin < ordinals.len) {
            try self.sort.manager.check();
            _ = scratch.reset(.retain_capacity);
            const a = scratch.allocator();
            const length = @min(@as(usize, 256), ordinals.len - begin);
            var positions: [256]usize = undefined;
            var chosen_ordinals: [256]u64 = undefined;
            const marker_indices: [256]u32 = @splat(0);
            const columns = try a.alloc(Batch, inputs.len + 1);
            columns[0] = .{ .dictionary = .{ .values = &.{Datum.json(.{ .bool = false })}, .indices = marker_indices[0..length] } };
            var all_positions: [256]usize = undefined;
            for (all_positions[0..length], begin..) |*row, i| row.* = i;
            for (inputs, columns[1..]) |*column, *part| part.* = try column.select(a, all_positions[0..length]);
            const values = try Batch.combine(a, columns, length);
            const key_columns = try a.alloc(Batch, keys.len);
            for (keys, key_columns) |*column, *part| part.* = try column.select(a, all_positions[0..length]);
            const key_batch = try Batch.combine(a, key_columns, length);
            for (0..self.partition_count) |partition| {
                var count: usize = 0;
                for (hashes[begin..][0..length], 0..) |hash, lane| {
                    if ((hash orelse return error.InvalidSqlSpill) % self.partition_count != partition) continue;
                    positions[count] = lane;
                    chosen_ordinals[count] = ordinals[begin + lane];
                    count += 1;
                }
                if (count == 0) continue;
                if (self.partitions[partition] == null) {
                    self.partitions[partition] = try spill.Sequential.init(self.sort.manager, @min(4096, self.bytes / 128));
                    self.partitions[partition].?.buffer_bytes = 512;
                }
                try self.partitions[partition].?.appendBatch(try values.select(a, positions[0..count]), try key_batch.select(a, positions[0..count]), chosen_ordinals[0..count]);
            }
            begin += length;
        }
    }
    fn append(self: *Grouped, row: operators.Row) !void {
        if (!self.partitioned) return self.sort.add(row);
        var hash = std.hash.Wyhash.init(0);
        for (row.keys) |key| {
            const value = if (key.sql_null) 0 else try scalar.semanticHashDatum(key);
            var bytes: [9]u8 = undefined;
            bytes[0] = @intFromBool(key.sql_null);
            std.mem.writeInt(u64, bytes[1..9], value, .little);
            hash.update(&bytes);
        }
        // Small reducers must not each retain eight files or launch tiny
        // nested reducers. Size useful hash fan-out from their own workspace.
        const index = hash.final() % self.partition_count;
        if (self.partitions[index] == null) {
            self.partitions[index] = try spill.Sequential.init(self.sort.manager, @min(4096, self.bytes / 128));
            self.partitions[index].?.buffer_bytes = 512;
        }
        _ = try self.partitions[index].?.append(row, spill.none);
    }
    fn nativeInputs(self: *Grouped, values: []const Datum) bool {
        if (values.len != self.specs.len) return false;
        for (values, self.specs) |value, spec| {
            if (value.sql_null or spec.kind == .count) continue;
            if (spec.input_element == .numeric and (spec.kind == .sum or spec.kind == .avg)) {
                if (value.numeric == null) return false;
                continue;
            }
            if (spec.kind == .sum and value.value != .integer) return false;
            if ((spec.kind == .bool_and or spec.kind == .bool_or) and value.value != .bool) return false;
        }
        return true;
    }
    fn readyLane(self: *Grouped) ?usize {
        for (0..self.reduction_lanes) |offset| {
            const lane = (self.next_lane + offset) % self.reduction_lanes;
            if (self.reductions[lane] != null) return lane;
        }
        return null;
    }
    fn startReductions(self: *Grouped, exact: bool) !void {
        if (self.reduction_lanes == 0) self.reduction_lanes = @import("parallel_scheduler.zig").global().fanout(self.partition_count, self.bytes, 256 * 1024);
        const workspace = self.bytes / self.reduction_lanes;
        for (self.reductions[0..self.reduction_lanes]) |*slot| {
            if (slot.* != null) continue;
            while (self.partition_index < self.partitions.len) {
                const index = self.partition_index;
                self.partition_index += 1;
                if (self.partitions[index] == null and self.partition_inputs[index].items.len == 0) continue;
                if (self.partitions[index]) |*file| try file.seal();
                const job = try self.a.create(Reduction);
                job.* = .{ .child = Grouped.init(self.a, self.sort.manager, self.specs, self.sort.orders.len, workspace) catch |err| {
                    self.a.destroy(job);
                    return err;
                }, .exact = exact };
                errdefer job.close();
                job.child.parallel = false;
                job.child.partitioned = true;
                job.child.sort.parallel_runs = false;
                job.child.partitions[0] = self.partitions[index];
                self.partitions[index] = null;
                job.child.partition_inputs[0] = self.partition_inputs[index];
                self.partition_inputs[index] = .empty;
                job.output = try @import("parallel_output.zig").Pipe.create(self.sort.manager, self.bytes / 128);
                job.task = @import("parallel_scheduler.zig").global().submit(self.sort.manager.io, workspace, Reduction.run, .{job});
                if (job.task == null) {
                    job.output.?.close();
                    job.output = null;
                } else self.reductions_started += 1;
                slot.* = job;
                break;
            }
        }
    }
    fn nextParallel(self: *Grouped, out: Allocator, exact: bool) !?operators.GroupResult {
        while (true) {
            try self.startReductions(exact);
            const lane = self.readyLane() orelse return null;
            const job = self.reductions[lane] orelse return null;
            if (job.exact != exact) return error.InvalidSqlBackendResponse;
            if (job.output) |pipe| {
                if (try pipe.nextOwned(out)) |row| {
                    self.output_count += 1;
                    return .{ .keys = row.keys, .aggregates = row.values, .ordinal = row.ordinal };
                }
                if (job.task) |*task| {
                    const result = task.await(self.sort.manager.io);
                    job.task = null;
                    try result;
                }
                if (pipe.terminal_error) |err| return err;
            } else if (try job.child.nextImpl(out, exact)) |result| {
                self.output_count += 1;
                return result;
            }
            job.close();
            self.reductions[lane] = null;
            self.next_lane = (lane + 1) % self.reduction_lanes;
        }
    }
    fn nextPartition(self: *Grouped, out: Allocator, exact: bool) anyerror!?operators.GroupResult {
        while (true) {
            if (self.local) |local| {
                if (try (if (exact) local.nextPartialResult(out) else local.nextResult(out))) |result| {
                    self.output_count += 1;
                    return result;
                }
                local.deinit();
                self.local = null;
            }
            if (self.fallback) |fallback| {
                if (try fallback.nextImpl(out, exact)) |result| {
                    self.output_count += 1;
                    return result;
                }
                fallback.deinit();
                self.a.destroy(fallback);
                self.fallback = null;
            }
            if (!try self.loadPartition()) return null;
        }
    }
    fn loadPartition(self: *Grouped) !bool {
        while (self.partition_index < self.partition_count) {
            const index = self.partition_index;
            self.partition_index += 1;
            if (self.partitions[index] == null and self.partition_inputs[index].items.len == 0) continue;
            self.local = try operators.Grouped.create(self.a, self.specs, .{ .bytes = self.bytes / 2, .groups = std.math.maxInt(usize) });
            if (self.partitions[index]) |*file| {
                try self.loadFile(file, 0);
                file.close();
                self.partitions[index] = null;
            }
            for (self.partition_inputs[index].items) |*input| {
                try self.loadFile(&input.file, input.ordinal_base);
                input.file.close();
                input.closed = true;
            }
            self.partition_inputs[index].clearRetainingCapacity();
            return true;
        }
        return false;
    }
    fn reduceNativeBlock(self: *Grouped, block: *spill.Sequential.OwnedBlock, ordinal_base: u64) !bool {
        if (self.fallback != null or block.count() < 2 or block.count() > 256 or block.width() != self.specs.len + 1) return false;
        for (self.specs) |spec| if (spec.distinct or spec.kind == .pattern_set or spec.input_element == .numeric) return false;
        for (0..block.count()) |row| {
            const marker = try block.cell(row, 0);
            if (marker.sql_null or marker.value != .bool) return error.InvalidSqlSpill;
            if (marker.value.bool) return false;
        }
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        const a = scratch.allocator();
        const Batch = @import("execution_batch.zig").Batch;
        const values = try a.create(Batch);
        values.* = try block.batch(a, false);
        const key_values = try a.create(Batch);
        key_values.* = try block.batch(a, true);
        const keys = try a.alloc(Batch, block.keyWidth());
        const inputs = try a.alloc(Batch, self.specs.len);
        for (keys, 0..) |*column, i| column.* = try key_values.project(a, i);
        for (inputs, self.specs, 0..) |*column, spec, i| {
            column.* = try values.project(a, i + 1);
            if (spec.kind == .sum or spec.kind == .avg) {
                const native = (try column.physicalColumn(a, 0)) orelse return false;
                if (native.kind != .integer) return false;
            }
        }
        const hashes = try @import("batch_hash.zig").encodedColumns(a, keys, block.count(), true);
        var ordinals: [256]u64 = undefined;
        for (ordinals[0..block.count()], 0..) |*ordinal, row| ordinal.* = try std.math.add(u64, ordinal_base, block.ordinal(row));
        const admitted = try self.local.?.tryAddEncodedColumns(keys, inputs, block.count(), .{ .hashes = hashes, .ordinals = ordinals[0..block.count()] });
        if (admitted) self.native_reduced_rows += block.count();
        return admitted;
    }
    fn loadFile(self: *Grouped, file: *spill.Sequential, ordinal_base: u64) !void {
        try file.seal();
        var offset: u64 = 0;
        while (offset < file.size) {
            try self.sort.manager.check();
            var input = try file.readInputBlock(offset);
            defer input.deinit();
            const block = input.view();
            if (try self.reduceNativeBlock(block, ordinal_base)) {
                offset += block.count();
                continue;
            }
            var values_storage: [1024]Datum = undefined;
            var keys_storage: [256]Datum = undefined;
            const values = values_storage[0..block.width()];
            const keys = keys_storage[0..block.keyWidth()];
            for (0..block.count()) |lane| {
                for (values, 0..) |*value, column| value.* = try block.cell(lane, column);
                for (keys, 0..) |*key, column| key.* = try block.keyCell(lane, column);
                const row: operators.Row = .{ .values = values, .keys = keys, .ordinal = try std.math.add(u64, ordinal_base, block.ordinal(lane)) };
                if (row.values.len == 0 or row.values[0].value != .bool) return error.InvalidSqlSpill;
                const partial_state = row.values[0].value.bool;
                const fits = if (self.fallback != null) false else if (partial_state)
                    try self.local.?.tryImportPartial(row.keys, row.values[1..], row.ordinal)
                else
                    self.nativeInputs(row.values[1..]) and try self.local.?.canRetain(row.keys, row.values[1..]);
                if (self.fallback == null and !fits) {
                    const fallback = try self.a.create(Grouped);
                    errdefer self.a.destroy(fallback);
                    fallback.* = try Grouped.init(self.a, self.sort.manager, self.specs, self.sort.orders.len, self.bytes);
                    fallback.partitioned = false;
                    fallback.sort.parallel_runs = self.parallel;
                    // Logical sorted runs share reusable extents in one
                    // anonymous file, preserving descriptor headroom for
                    // other live partitions and their merge outputs.
                    fallback.sort.shared_runs = true;
                    errdefer fallback.deinit();
                    try self.local.?.exportPartial(fallback);
                    self.local.?.deinit();
                    self.local = null;
                    self.fallback = fallback;
                }
                if (self.fallback) |fallback| {
                    try fallback.append(row);
                } else if (!partial_state) {
                    try self.local.?.addOrdered(row.keys, row.values[1..], row.ordinal);
                }
            }
            offset += block.count();
        }
    }
    pub fn nextBatch(self: *Grouped, a: Allocator, maximum: usize, exact: bool) anyerror!?operators.GroupBatch {
        if (maximum == 0) return error.InvalidSqlLimit;
        if (self.result_view) |view| {
            view.deinit();
            self.result_view = null;
        }
        if (self.partitioned and self.parallel and self.bytes >= 512 * 1024) while (true) {
            try self.startReductions(exact);
            const lane = self.readyLane() orelse return null;
            const job = self.reductions[lane] orelse return null;
            if (job.exact != exact) return error.InvalidSqlBackendResponse;
            const result = if (job.output) |pipe| blk: {
                if (try pipe.nextBatch(maximum)) |view| {
                    self.result_view = view;
                    break :blk operators.GroupBatch{ .keys = view.keys(), .aggregates = view.values(), .ordinals = view.ordinals() };
                }
                if (job.task) |*task| {
                    const status = task.await(self.sort.manager.io);
                    job.task = null;
                    try status;
                }
                if (pipe.terminal_error) |err| return err;
                break :blk null;
            } else try job.child.nextBatch(a, maximum, exact);
            if (result) |batch| {
                self.output_count += batch.ordinals.len;
                return batch;
            }
            job.close();
            self.reductions[lane] = null;
            self.next_lane = (lane + 1) % self.reduction_lanes;
        };
        if (self.partitioned) while (true) {
            if (self.local) |local| {
                if (try local.nextResultBatch(a, maximum, exact)) |batch| {
                    self.output_count += batch.ordinals.len;
                    return batch;
                }
                local.deinit();
                self.local = null;
            }
            if (self.fallback) |fallback| {
                if (try fallback.nextBatch(a, maximum, exact)) |batch| {
                    self.output_count += batch.ordinals.len;
                    return batch;
                }
                fallback.deinit();
                self.a.destroy(fallback);
                self.fallback = null;
            }
            if (!try self.loadPartition()) return null;
        };
        // The sorted skew/general-aggregate reducer owns its scalar states.
        // Preserve that boundary without forcing native reducers through it.
        const result = (try self.nextImpl(a, exact)) orelse return null;
        const keys = try a.alloc([]const Datum, 1);
        keys[0] = result.keys;
        const values = try a.alloc([]const Datum, 1);
        values[0] = result.aggregates;
        const ordinals = try a.alloc(u64, 1);
        ordinals[0] = result.ordinal;
        return .{ .keys = .{ .rows = keys }, .aggregates = .{ .rows = values }, .ordinals = ordinals };
    }
    fn same(left: []const Datum, right: []const Datum) !bool {
        if (left.len != right.len) return error.InvalidSqlSpill;
        for (left, right) |a, b| if (a.sql_null != b.sql_null or (!a.sql_null and (try scalar.compareDatums(a, b)) != .eq)) return false;
        return true;
    }
    pub fn next(self: *Grouped, out: Allocator) !?operators.GroupResult {
        return self.nextImpl(out, false);
    }
    pub fn nextPartial(self: *Grouped, out: Allocator) !?operators.GroupResult {
        return self.nextImpl(out, true);
    }
    fn nextImpl(self: *Grouped, out: Allocator, exact: bool) anyerror!?operators.GroupResult {
        if (self.partitioned) return if (self.parallel and self.bytes >= 512 * 1024) self.nextParallel(out, exact) else self.nextPartition(out, exact);
        _ = self.state_arena.reset(.free_all);
        const a = self.state_arena.allocator();
        var row = self.pending orelse (try self.sort.next(self.read_arena.allocator())) orelse return null;
        self.pending = null;
        const keys = try a.alloc(Datum, row.keys.len);
        for (row.keys, keys) |key, *copy| copy.* = try operators.cloneDatum(a, key);
        var ordinal = row.ordinal;
        const states = try a.alloc(operators.Aggregate, self.specs.len);
        var initialized: usize = 0;
        defer for (states[0..initialized]) |*state| state.deinit();
        for (states, self.specs) |*state, spec| {
            state.* = try operators.Aggregate.initTyped(self.a, spec.kind, spec.input_type, spec.input_element);
            state.distinct = exact and spec.distinct;
            initialized += 1;
        }
        const pattern_sets = try a.alloc(?*@import("pattern_spill.zig").Set, states.len);
        for (self.specs, pattern_sets) |spec, *set| set.* = if (spec.kind == .pattern_set) try @import("pattern_spill.zig").Set.create(self.sort.manager) else null;
        var distinct = spill.Sort.init(self.a, self.sort.manager, &.{ .{}, .{} }, self.sort.memory_bytes);
        defer distinct.deinit();
        var distinct_ordinal: u64 = 0;
        while (true) {
            ordinal = @min(ordinal, row.ordinal);
            if (row.values.len == 0) return error.InvalidSqlSpill;
            if (row.values[0].value == .integer) {
                if (row.values.len != 2) return error.InvalidSqlSpill;
                const slot = std.math.cast(usize, row.values[0].value.integer) orelse return error.InvalidSqlSpill;
                if (slot >= states.len) return error.InvalidSqlSpill;
                try distinct.add(.{ .keys = &.{ row.values[0], row.values[1] }, .values = &.{row.values[1]}, .ordinal = distinct_ordinal });
                distinct_ordinal += 1;
            } else if (row.values[0].value != .bool) return error.InvalidSqlSpill else if (row.values[0].value.bool) {
                if (row.values.len != 1 + states.len) return error.InvalidSqlSpill;
                for (states, row.values[1..], self.specs, 0..) |*state, value, spec, slot| {
                    var incoming = try @import("aggregate_partial.zig").decode(a, value, spec);
                    defer incoming.deinit();
                    if (spec.distinct and !exact) {
                        // Final reducers deduplicate externally, not in the
                        // in-memory membership table of a decoded partial.
                        // This also accepts complete worker partials without
                        // merging their already-reduced totals a second time.
                        for (incoming.distinct_values.items) |entry| {
                            const member = entry.row.row.values[0];
                            try distinct.add(.{ .keys = &.{ Datum.json(.{ .integer = @intCast(slot) }), member }, .values = &.{member}, .ordinal = distinct_ordinal });
                            distinct_ordinal += 1;
                        }
                        if (incoming.patterns) |patterns| if (patterns.has_null) {
                            try distinct.add(.{ .keys = &.{ Datum.json(.{ .integer = @intCast(slot) }), .{} }, .values = &.{.{}}, .ordinal = distinct_ordinal });
                            distinct_ordinal += 1;
                        };
                    } else try @import("aggregate_partial.zig").merge(state, incoming);
                }
            } else {
                if (row.values.len != states.len + 1) return error.InvalidSqlSpill;
                for (states, row.values[1..], self.specs, 0..) |*state, input, spec, slot| {
                    if (spec.distinct) {
                        if (!input.sql_null or spec.kind == .pattern_set) {
                            try distinct.add(.{ .keys = &.{ Datum.json(.{ .integer = @intCast(slot) }), input }, .values = &.{input}, .ordinal = distinct_ordinal });
                            distinct_ordinal += 1;
                        }
                    } else try state.update(input);
                }
            }
            _ = self.read_arena.reset(.free_all);
            row = (try self.sort.next(self.read_arena.allocator())) orelse break;
            if (!try same(keys, row.keys)) {
                self.pending = row;
                break;
            }
        }
        if (distinct_ordinal != 0) {
            var previous = std.heap.ArenaAllocator.init(self.a);
            defer previous.deinit();
            var current = std.heap.ArenaAllocator.init(self.a);
            defer current.deinit();
            var last: ?[]const Datum = null;
            while (true) {
                _ = current.reset(.free_all);
                const record = (try distinct.next(current.allocator())) orelse break;
                if (last) |prior| if (try same(prior, record.keys)) continue;
                const slot = std.math.cast(usize, record.keys[0].value.integer) orelse return error.InvalidSqlSpill;
                if (slot >= states.len) return error.InvalidSqlSpill;
                if (pattern_sets[slot]) |set| {
                    if (exact) try states[slot].update(record.values[0]) else try set.append(record.values[0]);
                } else try states[slot].update(record.values[0]);
                _ = previous.reset(.free_all);
                const copy = try previous.allocator().alloc(Datum, 2);
                for (record.keys, copy) |v, *cell| cell.* = try operators.cloneDatum(previous.allocator(), v);
                last = copy;
            }
        }
        const output_keys = try out.alloc(Datum, keys.len);
        for (keys, output_keys) |key, *copy| copy.* = try operators.cloneDatum(out, key);
        const aggregates = if (exact) try @import("aggregate_partial.zig").encode(out, states) else try out.alloc(Datum, states.len);
        if (!exact) for (states, @constCast(aggregates), pattern_sets) |*state, *copy, set| {
            copy.* = if (set) |patterns| .{ .patterns = &patterns.interface, .sql_null = false } else try operators.cloneDatum(out, try state.finish());
        };
        self.output_count += 1;
        return .{ .keys = output_keys, .aggregates = aggregates, .ordinal = ordinal };
    }
};

test "SQL partitioned typed aggregation reduces repeated updates and preserves ordinals" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var written: [2]u64 = undefined;
    for ([_]bool{ false, true }, 0..) |partitioned, run| {
        var budget: @import("memory_budget.zig") = .{ .backing = std.heap.page_allocator, .limit = 512 * 1024 };
        const a = budget.allocator();
        {
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false, .compression = .none };
            defer manager.deinit();
            var group = try Grouped.init(a, &manager, &.{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } }, 1, 128 * 1024);
            group.partitioned = partitioned;
            defer group.deinit();
            for (0..8192) |index| {
                const key = Datum.json(.{ .integer = @intCast(index % 128) });
                try group.add(&.{key}, &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = @intCast(index % 7) }) }, index);
            }
            var seen: [128]bool = @splat(false);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            while (true) {
                _ = scratch.reset(.retain_capacity);
                const result = (try group.next(scratch.allocator())) orelse break;
                const key: usize = @intCast(result.keys[0].value.integer);
                try std.testing.expect(!seen[key]);
                seen[key] = true;
                var sum: i64 = 0;
                for (0..64) |index| sum += @intCast((key + index * 128) % 7);
                try std.testing.expectEqual(@as(i64, 64), result.aggregates[0].value.integer);
                try std.testing.expectEqual(sum, result.aggregates[1].value.integer);
                try std.testing.expectEqual(@as(u64, key), result.ordinal);
            }
            for (seen) |found| try std.testing.expect(found);
            written[run] = manager.written_bytes;
        }
        try std.testing.expectEqual(@as(usize, 0), budget.live);
    }
    try std.testing.expect(written[1] < written[0] / 2);
    std.debug.print("SQL aggregate spill bytes: sorted={d} partitioned={d}\n", .{ written[0], written[1] });
}

test "SQL partitioned integer aggregation preserves invalid input errors" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    const a = std.testing.allocator;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .async_writes = false };
    defer manager.deinit();
    var group = try Grouped.init(a, &manager, &.{.{ .kind = .sum, .input_type = .integer }}, 1, 128 * 1024);
    defer group.deinit();
    const key = Datum.json(.{ .integer = 7 });
    try group.add(&.{key}, &.{Datum.json(.{ .integer = 1 })}, 3);
    try group.add(&.{key}, &.{Datum.json(.{ .float = 2.5 })}, 4);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.SqlTypeMismatch, group.next(arena.allocator()));
}

test "SQL full parallel partition reductions preserve exact states and close early" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]bool{ false, true }) |exact| {
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
        defer manager.deinit();
        {
            var grouped = try Grouped.init(a, &manager, &.{ .{ .kind = .count }, .{ .kind = .sum, .input_type = .integer } }, 1, 512 * 1024);
            defer grouped.deinit();
            for (0..4096) |i| {
                const key = Datum.json(.{ .integer = @intCast(i % 128) });
                try grouped.add(&.{key}, &.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = 3 }) }, i);
            }
            for (&grouped.partitions) |*file| if (file.*) |*open| try open.seal();
            const written_before = manager.written_bytes;
            var seen: [128]bool = @splat(false);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            while (true) {
                _ = arena.reset(.retain_capacity);
                const row = (try (if (exact) grouped.nextPartial(arena.allocator()) else grouped.next(arena.allocator()))) orelse break;
                const index: usize = @intCast(row.keys[0].value.integer);
                try std.testing.expect(!seen[index]);
                seen[index] = true;
                try std.testing.expectEqual(@as(u64, index), row.ordinal);
                if (exact) {
                    var sum = try @import("aggregate_partial.zig").decode(arena.allocator(), row.aggregates[1], .{ .kind = .sum, .input_type = .integer });
                    defer sum.deinit();
                    try std.testing.expectEqual(@as(i64, 96), (try sum.finish()).value.integer);
                } else {
                    try std.testing.expectEqual(@as(i64, 32), row.aggregates[0].value.integer);
                    try std.testing.expectEqual(@as(i64, 96), row.aggregates[1].value.integer);
                }
            }
            for (seen) |present| try std.testing.expect(present);
            try std.testing.expect(grouped.reductions_started > 1);
            try std.testing.expectEqual(written_before, manager.written_bytes);
        }
        try std.testing.expectEqual(@as(usize, 0), manager.files);
        // Closing after one output cancels/drains the other admitted reducer.
        {
            var grouped = try Grouped.init(a, &manager, &.{.{ .kind = .count }}, 1, 512 * 1024);
            defer grouped.deinit();
            for (0..512) |i| try grouped.add(&.{Datum.json(.{ .integer = @intCast(i) })}, &.{Datum.json(.{ .integer = 1 })}, i);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            try std.testing.expect((try grouped.next(arena.allocator())) != null);
        }
        try std.testing.expectEqual(@as(usize, 0), manager.files);
    }
}

test "SQL parallel aggregate keys remain owned after output blocks and workers close" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var output = std.heap.ArenaAllocator.init(a);
    defer output.deinit();
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(a);
    {
        var grouped = try Grouped.init(a, &manager, &.{.{ .kind = .count }}, 1, 512 * 1024);
        defer grouped.deinit();
        for (0..512) |i| {
            const name = try std.fmt.allocPrint(a, "group-{d:0>4}", .{i});
            defer a.free(name);
            try grouped.add(&.{Datum.json(.{ .string = name })}, &.{Datum.json(.{ .integer = 1 })}, i);
        }
        while (try grouped.next(output.allocator())) |row| {
            try names.append(a, row.keys[0].value.string);
            try std.testing.expectEqual(@as(i64, 1), row.aggregates[0].value.integer);
        }
    }
    try std.testing.expectEqual(@as(usize, 512), names.items.len);
    var seen: [512]bool = @splat(false);
    for (names.items) |name| {
        try std.testing.expect(std.mem.startsWith(u8, name, "group-"));
        const index = try std.fmt.parseInt(usize, name[6..], 10);
        try std.testing.expect(!seen[index]);
        seen[index] = true;
    }
    for (seen) |present| try std.testing.expect(present);
}

fn transferredPartitionScenario(a: Allocator, bytes: usize) !void {
    var context: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check };
    defer manager.deinit();
    const specs = [_]operators.AggregateSpec{ .{ .kind = .sum, .input_type = .integer }, .{ .kind = .count } };
    var sources: [3]Grouped = undefined;
    var initialized: usize = 0;
    defer for (sources[0..initialized]) |*source| source.deinit();
    for (&sources, 0..) |*source, lane| {
        source.* = try Grouped.init(a, &manager, &specs, 1, 512 * 1024);
        initialized += 1;
        for (0..17) |index| {
            const key: Datum = if (index == 0) .{} else Datum.json(.{ .integer = @intCast(index) });
            const wide: i128 = @as(i128, std.math.maxInt(i64)) * 2;
            const states = [_]operators.Aggregate{
                .{ .alloc = a, .kind = .sum, .input_type = .integer, .count = 1, .integer_sum = if (lane == 0) wide else if (lane == 1) -wide else 7 },
                .{ .alloc = a, .kind = .count, .input_type = null, .count = 1 },
            };
            try source.partial(&.{key}, &states, index);
        }
    }
    var target = try Grouped.init(a, &manager, &specs, 1, bytes);
    defer target.deinit();
    target.partition_count = 2;
    const pointers = [_]*Grouped{ &sources[0], &sources[1], &sources[2] };
    const bases = [_]u64{ 0, 1000, 2000 };
    try target.preparePartitionTransfer(&pointers, &bases);
    const written = manager.written_bytes;
    for (pointers, bases) |source, base| target.transferPartitions(source, base);
    try std.testing.expectEqual(written, manager.written_bytes);
    for (pointers) |source| for (source.partitions) |file| try std.testing.expect(file == null);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var seen: usize = 0;
    while (true) {
        _ = scratch.reset(.free_all);
        const result = (try target.next(scratch.allocator())) orelse break;
        try std.testing.expectEqual(@as(i64, 7), result.aggregates[0].value.integer);
        try std.testing.expectEqual(@as(i64, 3), result.aggregates[1].value.integer);
        const index: u64 = if (result.keys[0].sql_null) 0 else @intCast(result.keys[0].value.integer);
        try std.testing.expectEqual(index, result.ordinal);
        seen += 1;
    }
    try std.testing.expectEqual(@as(usize, 17), seen);
}
test "SQL spilled final hash owners transfer typed checkpoint extents and wide states" {
    try transferredPartitionScenario(std.testing.allocator, 1024 * 1024);
    try transferredPartitionScenario(std.testing.allocator, 64 * 1024);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, transferredPartitionScenario, .{@as(usize, 64 * 1024)});
}
