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

//! Grace hash join: sequentially partition both relations, then retain one
//! build partition. Oversized partitions repartition on remaining hash bits;
//! indistinguishable/skewed keys use bounded typed block nested loops.
const std = @import("std");
const operators = @import("operators.zig");
const spill = @import("spill.zig");
const Datum = @import("scalar.zig").Datum;
const A = std.mem.Allocator;
const Batch = @import("execution_batch.zig").Batch;
pub const Pair = struct { left: ?[]const Datum, right: ?[]const Datum, match: ?usize = null };
pub const Join = struct {
    pub const Evaluation = struct {
        condition: ?*const @import("scalar.zig").Program = null,
        parameters: []const std.json.Value = &.{},
        limits: @import("scalar.zig").EvalLimits = .{},
        left_width: usize,
        right_width: usize,
        flipped: bool = false,
        output_order: ?[]const usize = null,
    };

    // Exact summary of admitted build hashes. A split decision never needs a
    // second payload pass, and children rebuild summaries during repartition.
    const HashSummary = struct {
        any: u64 = 0,
        all: u64 = std.math.maxInt(u64),
        fn add(self: *@This(), hash: u64) void {
            self.any |= hash;
            self.all &= hash;
        }
        fn differences(self: @This()) u64 {
            return self.any ^ self.all;
        }
    };
    const Task = struct {
        hashes: HashSummary = .{},
        build: ?spill.Sequential = null,
        probes: ?spill.Sequential = null,
        used_bits: u64,
        depth: usize = 0,
        fn close(self: *Task) void {
            if (self.build) |*file| file.close();
            if (self.probes) |*file| file.close();
            self.build = null;
            self.probes = null;
        }
    };
    a: A,
    manager: *spill.Manager,
    execution: ?*@import("../common/runnable_scheduler.zig").Execution = null,
    limits: operators.HashJoin.Limits,
    partitions: usize,
    workspace_bytes: usize,
    build_hashes: [16]HashSummary = @splat(.{}),
    build_cost: [16]usize = @splat(0),
    probe_cost: [16]usize = @splat(0),
    outer_left: bool,
    outer_right: bool,
    build: [16]?spill.Sequential = @splat(null),
    probes: [16]?spill.Sequential = @splat(null),
    partition: usize = 0,
    active: ?Task = null,
    pending: [8]Task = undefined,
    pending_count: usize = 0,
    repartitions: usize = 0,
    probe_offset: u64 = 0,
    probe_input: ?spill.Sequential.InputBlock = null,
    probe_input_index: usize = 0,
    hash: ?*operators.HashJoin = null,
    skew: ?*@import("skew_join.zig").Join = null,
    skew_blocks_read: usize = 0,
    vector_residual_batches: usize = 0,
    scalar_residual_rows: usize = 0,
    probe: ?operators.HashJoin.Probe = null,
    left: ?[]const Datum = null,
    probe_matches: [256]bool = @splat(false),
    unmatched_probe: usize = 0,
    unmatched: usize = 0,
    rows: [2]usize = @splat(0),
    next_ordinals: [2]usize = @splat(0),
    finished: bool = false,
    probing_started: bool = false,
    probe_cells: []Datum = &.{},
    scalar_row: @import("execution_batch.zig").ScalarRow = .{},
    scratch: std.heap.ArenaAllocator,
    candidate: std.heap.ArenaAllocator,
    output_arena: std.heap.ArenaAllocator,
    partitions_loaded: usize = 0,
    filter: []u64,
    filtered_rows: usize = 0,
    parallel_builds: bool = false,
    evaluation: ?Evaluation = null,
    output: ?*@import("parallel_output.zig").Pipe = null,
    native_pipe_view: ?@import("parallel_output.zig").Pipe.View = null,
    native_disk_leases: std.ArrayList(*spill.Sequential.OwnedBlock) = .empty,
    output_task: ?@import("parallel_scheduler.zig").Task(anyerror!bool) = null,
    parallel_partitions_completed: usize = 0,
    prepared: [8]?*Join = @splat(null),
    preparing: [8]?@import("parallel_scheduler.zig").Task(anyerror!bool) = @splat(null),
    active_join: ?*Join = null,
    next_build_slot: usize = 0,
    build_lanes: usize = 2,
    parallel_builds_started: usize = 0,
    fn executePartition(self: *Join) anyerror!bool {
        defer self.closeInputs();
        var failure: ?anyerror = null;
        self.spoolPartition(self.output.?) catch |err| {
            failure = err;
        };
        self.output.?.finish(failure);
        return true;
    }
    /// Only this worker owns these files/hash state. Preparation detaches
    /// every receiver before handing the state back to the coordinator.
    fn bindExecution(self: *Join, execution: ?*@import("../common/runnable_scheduler.zig").Execution) void {
        self.execution = execution;
        self.limits.execution = execution;
        if (self.skew) |skew| {
            skew.execution = execution;
            if (skew.matches) |*file| file.execution = execution;
        }

        for (&self.build, &self.probes) |*build, *probe| {
            if (build.*) |*file| file.file.execution = execution;
            if (probe.*) |*file| file.file.execution = execution;
        }
        if (self.active) |*task| {
            if (task.build) |*file| file.file.execution = execution;
            if (task.probes) |*file| file.file.execution = execution;
        }
        for (self.pending[0..self.pending_count]) |*task| {
            if (task.build) |*file| file.file.execution = execution;
            if (task.probes) |*file| file.file.execution = execution;
        }
        if (self.hash) |hash| {
            hash.limits.execution = execution;
            if (hash.disk) |*file| file.execution = execution;
        }
    }
    fn executeChild(self: *Join, execution: *@import("../common/runnable_scheduler.zig").Execution) anyerror!bool {
        self.bindExecution(execution);
        defer self.bindExecution(null);
        self.output.?.producer_execution = execution;
        defer self.output.?.producer_execution = null;
        return self.executePartition();
    }
    fn prepareChild(self: *Join, execution: *@import("../common/runnable_scheduler.zig").Execution) anyerror!bool {
        self.bindExecution(execution);
        defer self.bindExecution(null);
        return self.prepare();
    }
    // Parent-owned Join retains every prepared result and closes it on errors;
    // task result is only status, with no separately transferred allocation.
    fn discardStatus(_: A, _: bool) void {}
    fn retiredOutput(args: anytype, failure: ?anyerror) void {
        if (failure != null) args[0].output.?.abort(failure);
    }
    fn spoolPartition(self: *Join, file: *@import("parallel_output.zig").Pipe) !void {
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        while (true) {
            _ = arena.reset(.{ .retain_with_limit = 64 * 1024 });
            const a = arena.allocator();
            var failure: ?anyerror = null;
            const batch = try self.nextBatch(a, 64, &failure);
            if (batch.len() != 0) {
                const view: *NativeRows = @ptrCast(@alignCast(batch.reader.ptr));
                var keys: [64][2]Datum = undefined;
                var key_rows: [64][]const Datum = undefined;
                var ordinals: [64]u64 = @splat(0);
                for (view.rows, 0..) |pair, row| {
                    keys[row] = .{ Datum.json(.{ .bool = pair.left != null }), Datum.json(.{ .bool = pair.right != null }) };
                    key_rows[row] = &keys[row];
                }
                // The queue copies a bounded cohort before the next pull can
                // release its source block or accepted native disk leases.
                try file.appendBatch(batch, .{ .rows = key_rows[0..batch.len()] }, ordinals[0..batch.len()]);
            }
            if (failure) |err| return err;
            if (batch.len() == 0) break;
        }
    }
    fn nextOutput(self: *Join) !?Pair {
        _ = self.output_arena.reset(.{ .retain_with_limit = @min(8192, self.limits.bytes / 32) });
        const row = (try self.output.?.next(self.output_arena.allocator())) orelse {
            if (self.output_task) |*task| {
                const result = task.await(self.manager.io);
                self.output_task = null;
                _ = try result;
            }
            if (self.output.?.terminal_error) |err| return err;
            return null;
        };
        const width = self.evaluation.?.left_width;
        if (row.keys.len != 2 or row.keys[0].value != .bool or row.keys[1].value != .bool or row.values.len != width + self.evaluation.?.right_width) return error.InvalidSqlSpill;
        return .{ .left = if (row.keys[0].value.bool) row.values[0..width] else null, .right = if (row.keys[1].value.bool) row.values[width..] else null };
    }
    fn readyLane(self: *Join) ?usize {
        for (0..self.build_lanes) |offset| {
            const lane = (self.next_build_slot + offset) % self.build_lanes;
            if (self.prepared[lane] != null) return lane;
        }
        return null;
    }
    fn largestPartition(self: *const Join) ?usize {
        var chosen: ?usize = null;
        var largest: usize = 0;
        for (0..self.partitions) |index| {
            if (self.build[index] == null and self.probes[index] == null) continue;
            const cost = self.build_cost[index] +| self.probe_cost[index];
            if (chosen == null or cost > largest) {
                chosen = index;
                largest = cost;
            }
        }
        return chosen;
    }
    fn finishInputs(self: *Join) !void {
        if (self.finished) return;
        for (&self.build) |*file| if (file.*) |*open| try open.seal();
        for (&self.probes) |*file| if (file.*) |*open| try open.seal();
        if (self.parallel_builds) {
            var useful: usize = 0;
            var largest: usize = 0;
            for (0..self.partitions) |index| {
                useful += @intFromBool(self.build[index] != null or self.probes[index] != null);
                largest = @max(largest, self.build_cost[index]);
            }
            var lanes = @min(self.build_lanes, @max(1, useful));
            // A prepared lookahead can coexist with the active lane. Reduce
            // fan-out before forcing a partition that fits alone to spill again.
            while (lanes > 1 and largest > self.workspace_bytes / (lanes + 1)) lanes -= 1;
            self.build_lanes = lanes;
            self.parallel_builds = useful != 0 and largest <= self.workspace_bytes / (lanes + 1);
        }
        self.limits.bytes = @max(8192, if (self.parallel_builds) self.workspace_bytes / (self.build_lanes + 1) else self.workspace_bytes);
        self.finished = true;
    }
    fn startBuilds(self: *Join) !void {
        for (self.prepared[0..self.build_lanes], self.preparing[0..self.build_lanes]) |*slot, *task| {
            if (slot.* != null) continue;
            while (self.partition < self.partitions) {
                const index = self.largestPartition() orelse {
                    self.partition = self.partitions;
                    break;
                };
                self.partition += 1;
                const child = try Join.createWithLanes(self.manager, self.limits.bytes, self.limits.rows, 0, self.outer_left, self.outer_right, 1, true);
                child.evaluation = self.evaluation;
                child.partitions = 0;
                child.finished = true;
                child.active = .{ .build = self.build[index], .probes = self.probes[index], .used_bits = self.partitions - 1, .hashes = self.build_hashes[index] };
                self.build[index] = null;
                self.probes[index] = null;
                slot.* = child;
                if (self.evaluation != null) {
                    child.output = try @import("parallel_output.zig").Pipe.create(self.manager, child.limits.bytes / 64);
                    child.output_task = if (self.manager.cpu_scope) |scope|
                        try scope.submitNotified(child.limits.bytes +| @sizeOf(Join), self.a, Join.executeChild, .{child}, discardStatus, retiredOutput)
                    else
                        self.manager.submit(child.limits.bytes, Join.executePartition, .{child});
                    if (child.output_task == null) {
                        child.output.?.close();
                        child.output = null;
                        _ = try child.prepare();
                    } else self.parallel_builds_started += 1;
                } else {
                    task.* = if (self.manager.cpu_scope) |scope|
                        try scope.submit(child.limits.bytes +| @sizeOf(Join), self.a, Join.prepareChild, .{child}, discardStatus)
                    else
                        self.manager.submit(child.limits.bytes, Join.prepare, .{child});
                    if (task.* == null) _ = try child.prepare() else self.parallel_builds_started += 1;
                }
                break;
            }
        }
    }
    fn nextParallel(self: *Join) anyerror!?Pair {
        while (true) {
            if (self.active_join) |child| {
                if (child.output != null) {
                    if (try child.nextOutput()) |pair| return pair;
                    self.parallel_partitions_completed += 1;
                } else if (try child.next()) |pair| return pair;
                self.partitions_loaded += child.partitions_loaded;
                self.repartitions += child.repartitions;
                child.close();
                self.active_join = null;
            }
            try self.startBuilds();
            const index = self.readyLane() orelse return null;
            const child = self.prepared[index] orelse return null;
            self.next_build_slot = (index + 1) % self.build_lanes;
            if (self.preparing[index]) |*task| {
                const result = task.await(self.manager.io);
                self.preparing[index] = null;
                _ = try result;
            }
            self.active_join = child;
            self.prepared[index] = null;
            // Fill the vacated lane before probing this partition. Build I/O
            // and hash admission overlap without changing residual ON handling.
            try self.startBuilds();
        }
    }
    /// Completed residual evaluation belongs to workers. Column leases bypass
    /// scalar pair reconstruction and payload cloning in downstream batches.
    /// Null means EOF or a scheduler fallback; next() handles either case.
    pub fn nextTypedBatch(self: *Join, maximum: usize) !?@import("parallel_output.zig").Pipe.View {
        if (!self.parallel_builds or self.evaluation == null) return null;
        try self.finishInputs();
        if (!self.parallel_builds) return null;
        while (true) {
            if (self.active_join) |child| {
                const pipe = child.output orelse return null;
                if (try pipe.nextBatch(maximum)) |view| return view;
                if (child.output_task) |*task| {
                    const result = task.await(self.manager.io);
                    child.output_task = null;
                    _ = try result;
                }
                if (pipe.terminal_error) |err| return err;
                self.parallel_partitions_completed += 1;
                self.partitions_loaded += child.partitions_loaded;
                self.repartitions += child.repartitions;
                child.close();
                self.active_join = null;
            }
            try self.startBuilds();
            const index = self.readyLane() orelse return null;
            const child = self.prepared[index] orelse return null;
            self.next_build_slot = (index + 1) % self.build_lanes;
            if (self.preparing[index]) |*task| {
                const result = task.await(self.manager.io);
                self.preparing[index] = null;
                _ = try result;
            }
            self.active_join = child;
            self.prepared[index] = null;
            try self.startBuilds();
        }
    }
    pub fn create(backing: A, manager: *spill.Manager, bytes: usize, rows: usize, build_bytes: u64, outer_left: bool, outer_right: bool) !*Join {
        _ = backing;
        return createWithLanes(manager, bytes, rows, build_bytes, outer_left, outer_right, null, false);
    }
    // Child workspaces are already assigned by the parent. Serial children
    // use that allowance directly rather than reserving imaginary siblings.
    fn createWithLanes(manager: *spill.Manager, bytes: usize, rows: usize, build_bytes: u64, outer_left: bool, outer_right: bool, requested_lanes: ?usize, assigned: bool) !*Join {
        const a = manager.allocator();
        const self = try a.create(Join);
        errdefer a.destroy(self);
        const filter = try a.alloc(u64, @max(16, @min(8192, bytes / 128)));
        @memset(filter, 0);
        const target = @max(@as(usize, 1), bytes / 16);
        const wanted = @min(@as(u64, 16), @max(@as(u64, 2), build_bytes / target + 1));
        const partitions = std.math.ceilPowerOfTwo(usize, @intCast(wanted)) catch unreachable;
        const lanes = requested_lanes orelse if (manager.cpu_scope != null)
            @max(1, @min(@as(usize, 8), @min(partitions, bytes / (256 * 1024))))
        else
            @import("parallel_scheduler.zig").global().fanout(partitions, bytes, 256 * 1024);
        const parallel = !assigned and bytes >= 512 * 1024 and lanes >= 2 and (manager.parallel_work or manager.cpu_scope != null);
        self.* = .{ .a = a, .manager = manager, .workspace_bytes = bytes, .build_lanes = lanes, .limits = .{ .bytes = @max(8192, if (parallel) bytes / (lanes + 1) else bytes), .rows = rows, .spill = manager }, .partitions = partitions, .outer_left = outer_left, .outer_right = outer_right, .scratch = .init(a), .candidate = .init(a), .output_arena = .init(a), .filter = filter, .parallel_builds = parallel };
        return self;
    }
    pub fn addBatch(self: *Join, build_side: bool, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum, begin: usize) !void {
        if (keys.len != batch.len() or begin > keys.len) return error.InvalidSqlBackendResponse;
        var offset = begin;
        const key_batch: @import("execution_batch.zig").Batch = .{ .rows = keys };
        while (offset < keys.len) {
            try self.manager.checkFor(self.execution);
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            const a = arena.allocator();
            const end = @min(keys.len, offset + 256);
            var selections: [16][256]usize = undefined;
            var ordinals: [16][256]u64 = undefined;
            var counts: [16]usize = @splat(0);
            for (offset..end) |index| {
                const ordinal = self.next_ordinals[@intFromBool(build_side)];
                const partition = (try self.admit(build_side, keys[index], ordinal)) orelse continue;
                const lane = counts[partition];
                selections[partition][lane] = index;
                ordinals[partition][lane] = ordinal;
                counts[partition] += 1;
                const costs = if (build_side) &self.build_cost else &self.probe_cost;
                if (costs[partition] == 0) costs[partition] = @import("typed_store.zig").columnMetadataBytes(batch.width() + keys[index].len) *| 2;
                costs[partition] +|= 128;
                for (0..batch.width()) |column| {
                    const cost = if (try batch.frozenJson(index, column)) |value| 64 +| value.bytes.len else if (try batch.encodedJson(index, column)) |text| 64 +| text.len else try @import("typed_store.zig").retainedCellBytes(try batch.cell(a, index, column));
                    costs[partition] +|= cost *| 2;
                }
                for (keys[index]) |value| costs[partition] +|= (try @import("typed_store.zig").retainedCellBytes(value)) *| 2;
            }
            for (counts[0..self.partitions], 0..) |count, partition| {
                if (count == 0) continue;
                const file = try self.inputFile(build_side, partition);
                try file.appendBatch(try batch.select(a, selections[partition][0..count]), try key_batch.select(a, selections[partition][0..count]), ordinals[partition][0..count]);
            }
            offset = end;
        }
    }
    pub fn close(self: *Join) void {
        if (self.output) |pipe| pipe.stop();
        if (self.output_task) |*task| {
            _ = task.cancel(self.manager.io) catch false;
            self.output_task = null;
        }
        for (&self.preparing, &self.prepared) |*task, *child| {
            if (task.*) |*active| {
                _ = active.cancel(self.manager.io) catch false;
                task.* = null;
            }
            if (child.*) |owner| owner.close();
            child.* = null;
        }
        if (self.active_join) |owner| owner.close();
        self.closeInputs();
        if (self.output) |pipe| pipe.close();
        self.native_disk_leases.deinit(self.a);
        self.scalar_row.deinit(self.a);
        self.scratch.deinit();
        self.candidate.deinit();
        self.output_arena.deinit();
        self.a.free(self.filter);
        self.a.destroy(self);
    }
    fn releaseNativeDiskLeases(self: *Join) void {
        for (self.native_disk_leases.items) |block| block.release();
        self.native_disk_leases.clearRetainingCapacity();
    }
    fn closeInputs(self: *Join) void {
        self.releaseNativeDiskLeases();
        if (self.native_pipe_view) |view| view.deinit();
        self.native_pipe_view = null;
        if (self.probe_input) |*block| block.deinit();
        self.probe_input = null;
        self.probe_input_index = 0;
        if (self.skew) |skew| skew.close();
        self.skew = null;
        if (self.hash) |hash| hash.deinit();
        self.hash = null;
        self.probe = null;
        for (&self.build, &self.probes) |*build, *probe| {
            if (build.*) |*file| file.close();
            if (probe.*) |*file| file.close();
            build.* = null;
            probe.* = null;
        }
        if (self.active) |*task| task.close();
        self.active = null;
        for (self.pending[0..self.pending_count]) |*task| task.close();
        self.pending_count = 0;
        self.a.free(self.probe_cells);
        self.probe_cells = &.{};
        _ = self.scratch.reset(.free_all);
        _ = self.candidate.reset(.free_all);
    }
    // The caller supplies all build rows before probe rows. This invariant
    // keeps the runtime filter complete and prevents false-negative matches.
    fn admit(self: *Join, build: bool, keys: []const Datum, ordinal: usize) !?usize {
        if (self.finished or (build and self.probing_started)) return error.InvalidSqlBackendResponse;
        if (!build) self.probing_started = true;
        const input_side = @intFromBool(build);
        self.next_ordinals[input_side] = @max(self.next_ordinals[input_side], try std.math.add(usize, ordinal, 1));
        const hash = try operators.HashJoin.keyHash(keys);
        if (hash) |value| {
            const bit = value % (self.filter.len * 64);
            const mask = @as(u64, 1) << @as(u6, @intCast(bit % 64));
            if (build) self.filter[bit / 64] |= mask else if (!self.outer_left and self.filter[bit / 64] & mask == 0) {
                self.filtered_rows += 1;
                return null;
            }
        } else if ((!build and !self.outer_left) or (build and !self.outer_right)) {
            self.filtered_rows += @intFromBool(!build);
            return null;
        }
        const side: usize = @intFromBool(build);
        if (self.rows[side] >= self.limits.rows) return error.SqlProgramLimitExceeded;
        self.rows[side] += 1;
        const partition: usize = @intCast((hash orelse 0) & (self.partitions - 1));
        if (build) self.build_hashes[partition].add(hash orelse 0);
        return partition;
    }
    fn inputFile(self: *Join, build: bool, partition: usize) !*spill.Sequential {
        const slot = if (build) &self.build[partition] else &self.probes[partition];
        if (slot.* == null) slot.* = try self.partitionFile();
        return &slot.*.?;
    }
    pub fn add(self: *Join, build: bool, values: []const Datum, keys: []const Datum, ordinal: usize) !void {
        const partition = (try self.admit(build, keys, ordinal)) orelse return;
        _ = try (try self.inputFile(build, partition)).append(.{ .values = values, .keys = keys, .ordinal = ordinal }, spill.none);
        const costs = if (build) &self.build_cost else &self.probe_cost;
        const typed = @import("typed_store.zig");
        if (costs[partition] == 0) costs[partition] = typed.columnMetadataBytes(values.len + keys.len) *| 2;
        costs[partition] +|= 128;
        for (values) |value| costs[partition] +|= (try typed.retainedCellBytes(value)) *| 2;
        for (keys) |value| costs[partition] +|= (try typed.retainedCellBytes(value)) *| 2;
    }
    pub fn accept(self: *Join, index: usize) !void {
        if (self.active_join) |child| return child.accept(index);
        if (self.skew) |skew| return skew.accept(index);
        return self.acceptProbe(index, self.probe_input_index - 1);
    }
    fn acceptProbe(self: *Join, index: usize, probe_row: usize) !void {
        if (self.probe_input == null or probe_row >= self.probe_input.?.view().count()) return error.InvalidSqlBackendResponse;
        self.probe_matches[probe_row] = true;
        if (self.outer_right) try self.hash.?.markMatched(index);
    }
    fn partitionFile(self: *Join) !spill.Sequential {
        var file = try spill.Sequential.initFor(self.manager, @min(4096, self.workspace_bytes / 512), self.execution);
        file.buffer_bytes = @max(128, @min(4096, self.workspace_bytes / 512));
        return file;
    }
    fn split(self: *Join, differences: u64) !bool {
        const parent = &self.active.?;
        const available = differences & ~parent.used_bits;
        // Identical key hashes cannot be separated. Cap depth and file count
        // independently of relation size; those cases use block nested loops.
        if (available == 0 or parent.depth == self.pending.len) return false;
        const bit = @as(u64, 1) << @as(u6, @intCast(@ctz(available)));
        var children = [_]Task{
            .{ .used_bits = parent.used_bits | bit, .depth = parent.depth + 1 },
            .{ .used_bits = parent.used_bits | bit, .depth = parent.depth + 1 },
        };
        errdefer for (&children) |*child| child.close();
        for ([_]bool{ true, false }) |build_side| {
            const source = if (build_side) &parent.build else &parent.probes;
            if (source.*) |*file| {
                file.rewind();
                var offset: u64 = 0;
                while (offset < file.size) {
                    try self.manager.checkFor(self.execution);
                    _ = self.scratch.reset(.free_all);
                    var input = try file.readInputBlockBorrowed(offset);
                    defer input.deinit();
                    const block = input.view();
                    const a = self.scratch.allocator();
                    const values = try block.batch(a, false);
                    const keys = try block.batch(a, true);
                    const selections = try a.alloc(usize, block.count());
                    const ordinals = try a.alloc(u64, block.count());
                    const key_cells = try a.alloc(Datum, block.keyWidth());
                    const sides = try a.alloc(bool, block.count());
                    for (sides, 0..) |*side, index| {
                        for (key_cells, 0..) |*cell, column| cell.* = try block.keyCell(index, column);
                        const hash = (try operators.HashJoin.keyHash(key_cells)) orelse 0;
                        side.* = hash & bit != 0;
                        if (build_side) children[@intFromBool(side.*)].hashes.add(hash);
                    }
                    for (&children, 0..) |*child, side| {
                        var count: usize = 0;
                        for (sides, 0..) |selected, index| if (selected == (side != 0)) {
                            selections[count] = index;
                            ordinals[count] = block.ordinal(index);
                            count += 1;
                        };
                        if (count == 0) continue;
                        const target = if (build_side) &child.build else &child.probes;
                        if (target.* == null) target.* = try self.partitionFile();
                        try target.*.?.appendBatch(try values.select(a, selections[0..count]), try keys.select(a, selections[0..count]), ordinals[0..count]);
                    }
                    offset += block.count();
                }
            }
        }
        parent.close();
        self.active = children[0];
        self.pending[self.pending_count] = children[1];
        self.pending_count += 1;
        self.repartitions += 1;
        return true;
    }
    fn compactSkewInput(self: *Join, source: *spill.Sequential) !void {
        var compact = try spill.Sequential.initFor(self.manager, @max(512, @min(32 * 1024, self.limits.bytes / 16)), self.execution);
        errdefer compact.close();
        compact.buffer_bytes = @max(128, @min(4096, self.limits.bytes / 64));
        source.rewind();
        var offset: u64 = 0;
        while (offset < source.size) {
            var input = try source.readInputBlockBorrowed(offset);
            defer input.deinit();
            const block = input.view();
            for (0..block.count()) |row| {
                _ = self.scratch.reset(.retain_capacity);
                _ = try compact.append(try block.row(self.scratch.allocator(), row), spill.none);
            }
            offset += block.count();
        }
        try compact.seal();
        source.close();
        source.* = compact;
    }
    fn prepare(self: *Join) anyerror!bool {
        while (self.hash == null and self.skew == null) {
            try self.manager.checkFor(self.execution);
            if (self.active == null) {
                if (self.pending_count != 0) {
                    self.pending_count -= 1;
                    self.active = self.pending[self.pending_count];
                } else {
                    if (self.partition == self.partitions) return false;
                    self.active = .{ .build = self.build[self.partition], .probes = self.probes[self.partition], .used_bits = self.partitions - 1, .hashes = self.build_hashes[self.partition] };
                    self.build[self.partition] = null;
                    self.probes[self.partition] = null;
                    self.partition += 1;
                }
            }
            var build_limits = self.limits;
            if (@import("memory_budget.zig").headroom(self.manager.alloc)) |available| {
                // Input decoding, probe scratch and the enclosing operator are
                // already live. A build's allowance must use remaining shared
                // capacity, not the original statement ceiling a second time.
                build_limits.bytes = @min(build_limits.bytes, available - available / 4);
            }
            self.hash = try operators.HashJoin.create(self.a, build_limits);
            var repartitioned = false;
            if (self.active.?.build) |*file| {
                var offset: u64 = 0;
                while (offset < file.size) {
                    try self.manager.checkFor(self.execution);
                    var input = try file.readInputBlock(offset);
                    defer input.deinit();
                    const block = input.view();
                    _ = self.scratch.reset(.retain_capacity);
                    const a = self.scratch.allocator();
                    const values = try block.batch(a, false);
                    var keys: [256][]const Datum = undefined;
                    const key_cells = try a.alloc(Datum, block.count() * block.keyWidth());
                    for (keys[0..block.count()], 0..) |*key, index| {
                        const row = key_cells[index * block.keyWidth() ..][0..block.keyWidth()];
                        for (row, 0..) |*cell, column| cell.* = try block.keyCell(index, column);
                        key.* = row;
                    }
                    const consumed = self.hash.?.addBatchUntilFull(self.a, values, keys[0..block.count()]) catch |err| {
                        if (err != error.OutOfMemory) return err;
                        // The shared statement allocator can deny growth even
                        // when the hash's local estimate fits. Typed appends
                        // may already be partial, so retire the entire hash
                        // before splitting and replaying the immutable build.
                        // No prefix is published or transferred twice.
                        self.hash.?.deinit();
                        self.hash = null;
                        _ = self.scratch.reset(.free_all);
                        file.rewind();
                        var replay: u64 = 0;
                        var all_hash_union: u64 = 0;
                        var all_hash_intersection: u64 = std.math.maxInt(u64);
                        while (replay < file.size) {
                            try self.manager.checkFor(self.execution);
                            const scanned = try file.readBatchBorrowed(replay, 256);
                            for (scanned.rows) |row| {
                                const h = (try operators.HashJoin.keyHash(row.keys)) orelse 0;
                                all_hash_union |= h;
                                all_hash_intersection &= h;
                            }
                            replay = scanned.following;
                        }
                        if (!try self.split(all_hash_union ^ all_hash_intersection)) {
                            try self.compactSkewInput(file);
                            if (self.active.?.probes) |*probes| try self.compactSkewInput(probes);
                            self.skew = try @import("skew_join.zig").Join.createFor(self.manager, self.outer_left, self.outer_right, self.execution);
                            file.rewind();
                            break;
                        }
                        repartitioned = true;
                        break;
                    };
                    offset += consumed;
                    if (consumed == block.count()) continue;
                    // Exact ingestion summaries include unread blocks. Do
                    // not decode/replay payloads merely to choose a split bit.
                    if (try self.split(self.active.?.hashes.differences())) {
                        self.hash.?.deinit();
                        self.hash = null;
                        repartitioned = true;
                        break;
                    }
                    // No useful hash split remains. Keep the typed partition
                    // and replay it per probe block, not per probe row.
                    self.hash.?.deinit();
                    self.hash = null;
                    try self.compactSkewInput(file);
                    if (self.active.?.probes) |*probes| try self.compactSkewInput(probes);
                    self.skew = try @import("skew_join.zig").Join.createFor(self.manager, self.outer_left, self.outer_right, self.execution);
                    file.rewind();
                    break;
                }
            }
            if (repartitioned) continue;
            if (self.skew != null) {
                self.partitions_loaded += 1;
                return true;
            }
            if (self.active.?.build) |*file| file.close();
            self.active.?.build = null;
            self.partitions_loaded += 1;
        }
        return true;
    }
    // Sources belong to the pinned probe cohort / build partition. A row
    // reference is immutable even as the probe cursor advances within a block.
    const NativeRow = struct {
        source: union(enum) {
            probe: *const spill.Sequential.OwnedBlock,
            build: *const @import("typed_store.zig").Store,
            values: []const Datum,
            disk: operators.HashJoin.Match.NativeMatch,
        },
        index: usize = 0,
        width: usize,
        fn cell(self: NativeRow, a: A, column: usize) !Datum {
            if (column >= self.width) return error.InvalidSqlBackendResponse;
            return switch (self.source) {
                .probe => |block| block.cell(self.index, column),
                .build => |store| store.cell(a, self.index, column),
                .disk => |native| (try native.block()).cell(native.row, column),
                .values => |values| if (column < values.len) values[column] else error.InvalidSqlBackendResponse,
            };
        }
        fn json(self: NativeRow, column: usize) !?[]const u8 {
            if (column >= self.width) return error.InvalidSqlBackendResponse;
            return switch (self.source) {
                .probe => |block| if (block.encoded) |encoded| encoded.encodedJson(self.index, column, false) else null,
                .build => |store| store.encodedJson(self.index, column),
                .disk => |native| if ((try native.block()).encoded) |encoded| encoded.encodedJson(native.row, column, false) else null,
                .values => null,
            };
        }
        fn frozen(self: NativeRow, column: usize) !?@import("execution_batch.zig").FrozenJson {
            if (column >= self.width) return error.InvalidSqlBackendResponse;
            return switch (self.source) {
                .probe => |block| if (block.encoded) |encoded| encoded.frozenJson(self.index, column, false) else null,
                .build => |store| store.frozenJson(self.index, column),
                .disk => |native| if ((try native.block()).encoded) |encoded| encoded.frozenJson(native.row, column, false) else null,
                .values => null,
            };
        }
        fn same(self: NativeRow, other: NativeRow) bool {
            return switch (self.source) {
                .probe => |block| other.source == .probe and block == other.source.probe,
                .build => |store| other.source == .build and store == other.source.build,
                else => false,
            };
        }
        fn batch(self: NativeRow, a: A) !?Batch {
            return switch (self.source) {
                .probe => |block| try @constCast(block).batch(a, false),
                .build => |store| .{ .retained = .{ .store = store, .begin = 0, .count = store.len } },
                else => null,
            };
        }
        fn row(self: NativeRow, a: A) ![]const Datum {
            const values = try a.alloc(Datum, self.width);
            for (values, 0..) |*value, column| value.* = try self.cell(a, column);
            return values;
        }
        fn retain(self: NativeRow, a: A, owner: *Join) !NativeRow {
            if (self.source == .disk) {
                const block = try self.source.disk.block();
                var pinned = false;
                for (owner.native_disk_leases.items) |existing| if (existing == block) {
                    pinned = true;
                    break;
                };
                if (!pinned) {
                    try owner.native_disk_leases.append(owner.a, block);
                    block.retain();
                }
                return .{ .source = .{ .probe = block }, .index = self.source.disk.row, .width = self.width };
            }
            // Disk-chain matches borrow the decoder until the next candidate.
            // Retain staged fallback rows, including nested payloads.
            if (self.source == .values) {
                const values = try a.alloc(Datum, self.source.values.len);
                for (values, self.source.values) |*dest, value| dest.* = try operators.cloneDatum(a, value);
                return .{ .source = .{ .values = values }, .width = values.len };
            }
            return self;
        }
        fn build(match: operators.HashJoin.Match) NativeRow {
            if (match.native) |native| return .{ .source = .{ .disk = native }, .width = native.width };
            return if (match.owner) |owner|
                .{ .source = .{ .build = &owner.value_columns }, .index = match.index, .width = owner.entries.items[match.index].width }
            else
                .{ .source = .{ .values = match.values }, .width = match.values.len };
        }
    };
    const NativePair = struct {
        left: ?NativeRow,
        right: ?NativeRow,
        match: ?usize = null,
        fn append(self: NativePair, a: A, rows: *std.ArrayList(NativePair), owner: *Join) !void {
            try rows.append(a, .{
                .left = if (self.left) |left| try left.retain(a, owner) else null,
                .right = if (self.right) |right| try right.retain(a, owner) else null,
                .match = self.match,
            });
        }
    };
    const NativeRows = struct {
        rows: []const NativePair,
        evaluation: Evaluation,
        fn locate(self: *@This(), row: usize, column: usize) !struct { source: ?NativeRow, column: usize } {
            if (row >= self.rows.len or column >= self.evaluation.left_width + self.evaluation.right_width) return error.InvalidSqlBackendResponse;
            return if (column < self.evaluation.left_width) .{ .source = self.rows[row].left, .column = column } else .{ .source = self.rows[row].right, .column = column - self.evaluation.left_width };
        }
        fn cell(raw: *anyopaque, a: A, row: usize, column: usize) !Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const location = try self.locate(row, column);
            return if (location.source) |source| source.cell(a, location.column) else .{};
        }
        fn json(raw: *anyopaque, row: usize, column: usize) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const location = try self.locate(row, column);
            return if (location.source) |source| source.json(location.column) else null;
        }
        fn frozen(raw: *anyopaque, row: usize, column: usize) !?@import("execution_batch.zig").FrozenJson {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const location = try self.locate(row, column);
            return if (location.source) |source| source.frozen(location.column) else null;
        }
        fn selectedSource(self: *@This(), a: A, column: usize, selected: ?[]const usize) !?struct { batch: Batch, column: usize, rows: []usize } {
            const count = if (selected) |indices| indices.len else self.rows.len;
            if (count == 0) return null;
            const first = try self.locate(if (selected) |indices| indices[0] else 0, column);
            const source = first.source orelse return null;
            const batch_ = (try source.batch(a)) orelse return null;
            const rows = try a.alloc(usize, count);
            var retained = false;
            defer if (!retained) a.free(rows);
            for (rows, 0..) |*row, lane| {
                const current = try self.locate(if (selected) |indices| indices[lane] else lane, column);
                const value = current.source orelse return null;
                if (current.column != first.column or !source.same(value)) return null;
                row.* = value.index;
            }
            retained = true;
            return .{ .batch = batch_, .column = first.column, .rows = rows };
        }
        fn physical(raw: *anyopaque, a: A, column: usize, selected: ?[]const usize) !?@import("execution_batch.zig").PhysicalColumn {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const source = (try self.selectedSource(a, column, selected)) orelse return null;
            return source.batch.physicalColumnSelected(a, source.column, source.rows);
        }
        fn decimal(raw: *anyopaque, a: A, column: usize, selected: ?[]const usize) !?@import("execution_batch.zig").DecimalColumn {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const source = (try self.selectedSource(a, column, selected)) orelse return null;
            return source.batch.decimalColumnSelected(a, source.column, source.rows);
        }
        fn batch(self: *@This()) Batch {
            return .{ .reader = .{ .ptr = self, .read = cell, .read_json = json, .read_frozen = frozen, .read_physical_selected = physical, .read_decimal_selected = decimal, .count = self.rows.len, .width = self.evaluation.left_width + self.evaluation.right_width } };
        }
    };
    fn nativeProbe(self: *Join) NativeRow {
        return .{ .source = .{ .probe = self.probe_input.?.view() }, .index = self.probe_input_index - 1, .width = self.probe_input.?.view().width() };
    }
    fn vectorResidualAllowed(limits: @import("scalar.zig").EvalLimits) bool {
        const defaults: @import("scalar.zig").EvalLimits = .{};
        // Native kernels retain the ordinary expression budgets. Custom scalar
        // budgets and decision execution must use the evaluator that owns them.
        return limits.steps == defaults.steps and limits.input_bytes == defaults.input_bytes and
            limits.output_bytes == defaults.output_bytes and limits.depth == defaults.depth and
            limits.decision_values == null and limits.decision_demand == null;
    }
    fn evaluateNative(self: *Join, a: A, view: *NativeRows) !bool {
        if (view.evaluation.condition) |program| {
            const cells = try self.scalar_row.prepare(self.a, view.evaluation.left_width + view.evaluation.right_width);
            for (program.required_columns) |logical| {
                if (logical >= cells.len) return error.InvalidSqlBackendResponse;
                const original = if (view.evaluation.output_order) |order| order[logical] else logical;
                const physical = if (!view.evaluation.flipped) original else if (original < view.evaluation.right_width) view.evaluation.left_width + original else original - view.evaluation.right_width;
                cells[logical] = try NativeRows.cell(view, a, 0, physical);
            }
            const accepted = try program.evaluate(a, cells, view.evaluation.parameters, view.evaluation.limits);
            if (accepted.sql_null) return false;
            if (accepted.value != .bool) return error.SqlTypeMismatch;
            if (!accepted.value.bool) return false;
        }
        return true;
    }
    /// Borrow one probe cohort and build partition until the next pull. Workers
    /// copy encoded values only at the bounded queue's ownership boundary.
    pub fn nextBatch(self: *Join, a: A, maximum: usize, failure: ?*?anyerror) anyerror!Batch {
        self.releaseNativeDiskLeases();
        if (self.native_pipe_view) |view| view.deinit();
        self.native_pipe_view = null;
        if (maximum == 0) return .{ .rows = &.{} };
        try self.finishInputs();
        if (self.parallel_builds) while (true) {
            const typed = self.nextTypedBatch(maximum) catch |err| {
                if (failure) |out| {
                    out.* = err;
                    return .{ .rows = &.{} };
                }
                return err;
            };
            if (typed) |view| {
                self.native_pipe_view = view;
                return view.values();
            }
            const child = self.active_join orelse return .{ .rows = &.{} };
            // A scheduler-declined worker keeps the same native serial path.
            const batch = try child.nextBatch(a, maximum, failure);
            if (batch.len() != 0) return batch;
            if (failure) |out| if (out.* != null) return batch;
            self.partitions_loaded += child.partitions_loaded;
            self.repartitions += child.repartitions;
            child.close();
            self.active_join = null;
        };
        while (true) {
            _ = self.candidate.reset(.{ .retain_with_limit = @min(64 * 1024, self.limits.bytes / 16) });
            const work = self.candidate.allocator();
            var candidates: std.ArrayList(NativePair) = .empty;
            var candidate_bytes: usize = 0;
            var upstream: ?anyerror = null;
            while (candidates.items.len < maximum) {
                const pair = (self.nextNativePair(candidates.items.len == 0) catch |err| {
                    upstream = err;
                    break;
                }) orelse break;
                pair.append(work, &candidates, self) catch |err| {
                    upstream = err;
                    break;
                };
                candidate_bytes +|= @sizeOf(NativePair);
                for ([_]?NativeRow{ pair.left, pair.right }) |side| if (side) |value| {
                    if (value.source == .values) {
                        for (value.source.values) |cell_| candidate_bytes +|= try @import("typed_store.zig").retainedCellBytes(cell_);
                    }
                };
                var retained_bytes = candidate_bytes;
                for (self.native_disk_leases.items) |block| retained_bytes +|= block.retainedBytes();
                if (candidates.items.len * @sizeOf(NativePair) >= self.limits.bytes / 64 or retained_bytes >= @max(4096, self.limits.bytes / 8)) break;
            }
            if (candidates.items.len == 0) {
                if (upstream) |err| {
                    if (failure) |out| out.* = err else return err;
                }
                return .{ .rows = &.{} };
            }
            var candidate: NativeRows = .{ .rows = candidates.items, .evaluation = self.evaluation.? };
            var inner: std.ArrayList(usize) = .empty;
            for (candidates.items, 0..) |pair, row| if (pair.match != null) try inner.append(work, row);
            var decisions: ?Batch = null;
            if (self.evaluation.?.condition) |program| if (inner.items.len >= 2 and vectorResidualAllowed(self.evaluation.?.limits)) {
                if (self.evaluation.?.limits.checkpoint) |check| try check(self.evaluation.?.limits.checkpoint_context);
                const source_batch = try work.create(Batch);
                source_batch.* = candidate.batch();
                const physical = try work.create(Batch);
                physical.* = try source_batch.select(work, inner.items);
                const width = physical.width();
                const ordinals = try work.alloc(usize, width);
                const types = try work.alloc(@import("scalar.zig").Type, width);
                const definitions = try work.alloc(@import("scalar.zig").Column, width);
                const names = try work.alloc([]const u8, width);
                const selection = try work.alloc(usize, inner.items.len);
                for (selection, 0..) |*row, i| row.* = i;
                for (ordinals, types, definitions, names, 0..) |*ordinal, *type_, *definition, *name, logical| {
                    const original = if (self.evaluation.?.output_order) |order| order[logical] else logical;
                    ordinal.* = if (!self.evaluation.?.flipped) original else if (original < self.evaluation.?.right_width) self.evaluation.?.left_width + original else original - self.evaluation.?.right_width;
                    type_.* = .{ .kind = .json };
                    for (program.instructions) |instruction| if (instruction.operation == .column and instruction.operation.column == logical) {
                        type_.* = instruction.type;
                        break;
                    };
                    name.* = try std.fmt.allocPrint(work, "c{d}", .{logical});
                    definition.* = .{ .name = name.*, .type = type_.kind orelse .json, .element_type = type_.element_type };
                }
                const logical_batch = try work.create(Batch);
                logical_batch.* = .{ .mapped = .{ .source = physical, .ordinals = ordinals, .types = types, .selection = selection } };
                const page: @import("catalog.zig").ColumnPage = .{ .native = .{ .values = logical_batch, .names = names }, .selection = selection };
                const outputs = @import("vector_eval.zig").evaluateColumnsEncodedMany(work, &.{program}, page, definitions, self.evaluation.?.parameters, null) catch |err| blk: {
                    if (!@import("errors.zig").isRowEvaluationError(err)) return err;
                    break :blk null;
                };
                if (outputs) |values| if (values[0]) |value| {
                    decisions = value;
                    self.vector_residual_batches += 1;
                };
            };
            var rows: std.ArrayList(NativePair) = .empty;
            var lane: usize = 0;
            for (candidates.items) |pair| {
                if (pair.match) |index| {
                    var accepted = true;
                    if (decisions) |values| {
                        const value = try values.cell(work, lane, 0);
                        if (!value.sql_null and value.value != .bool) return error.SqlTypeMismatch;
                        accepted = !value.sql_null and value.value.bool;
                    } else if (self.evaluation.?.condition != null) {
                        self.scalar_residual_rows += 1;
                        var one: NativeRows = .{ .rows = &.{pair}, .evaluation = self.evaluation.? };
                        // Scalar fallback preserves the first demanded error and prefix.
                        accepted = self.evaluateNative(work, &one) catch |err| {
                            upstream = err;
                            break;
                        };
                    }
                    lane += 1;
                    if (!accepted) continue;
                    if (self.skew) |skew| try skew.acceptAt(index, pair.left.?.index) else try self.acceptProbe(index, pair.left.?.index);
                }
                rows.append(a, pair) catch |err| {
                    upstream = err;
                    break;
                };
            }
            if (upstream) |err| {
                if (failure) |out| out.* = err else return err;
            }
            if (rows.items.len == 0 and upstream == null) {
                self.releaseNativeDiskLeases();
                continue;
            }
            if (rows.items.len == 0) return .{ .rows = &.{} };
            const view = a.create(NativeRows) catch |err| {
                if (failure) |out| {
                    out.* = err;
                    return .{ .rows = &.{} };
                }
                return err;
            };
            view.* = .{ .rows = rows.items, .evaluation = self.evaluation.? };
            return view.batch();
        }
    }

    pub fn next(self: *Join) anyerror!?Pair {
        self.releaseNativeDiskLeases();
        try self.finishInputs();
        if (self.parallel_builds) return self.nextParallel();
        _ = self.candidate.reset(.{ .retain_with_limit = @min(8192, self.limits.bytes / 32) });
        const a = self.candidate.allocator();
        const pair = try self.nextNativePair(true) orelse return null;
        return .{ .left = if (pair.left) |left| try left.row(a) else null, .right = if (pair.right) |right| try right.row(a) else null, .match = pair.match };
    }
    fn nextNativePair(self: *Join, may_advance: bool) anyerror!?NativePair {
        if (!may_advance and self.hash == null and self.skew == null) return null;
        while (try self.prepare()) {
            try self.manager.checkFor(self.execution);
            if (self.skew) |skew| {
                if (try skew.nextNative(&self.active.?.build.?, if (self.active.?.probes) |*file| file else null, may_advance)) |pair| return .{
                    .left = if (pair.left) |row| .{ .source = .{ .probe = row.block }, .index = row.index, .width = row.block.width() } else null,
                    .right = if (pair.right) |row| .{ .source = .{ .probe = row.block }, .index = row.index, .width = row.block.width() } else null,
                    .match = pair.match,
                };
                if (!may_advance) return null;
                self.skew_blocks_read += skew.blocks_read;
                skew.close();
                self.skew = null;
                self.active.?.close();
                self.active = null;
                continue;
            }
            if (self.probe) |*probe| {
                if (try probe.next()) |match| return .{ .left = self.nativeProbe(), .right = NativeRow.build(match), .match = match.index };
                self.probe = null;
            }
            // Complete residual decisions before emitting unmatched probe rows
            // or releasing their block. A cohort may span every probe in this
            // pinned block, including one-to-one hash matches.
            if (self.probe_input) |*input| if (self.probe_input_index == input.view().count()) {
                if (!may_advance and self.unmatched_probe == 0) return null;
                if (self.outer_left) while (self.unmatched_probe < input.view().count()) {
                    const row = self.unmatched_probe;
                    self.unmatched_probe += 1;
                    if (!self.probe_matches[row]) return .{ .left = .{ .source = .{ .probe = input.view() }, .index = row, .width = input.view().width() }, .right = null };
                };
                if (!may_advance) return null;
                input.deinit();
                self.probe_input = null;
            };
            if (self.active.?.probes) |*file| {
                if (self.probe_offset < file.size) {
                    if (self.probe_input == null) {
                        if (!may_advance) return null;
                        self.probe_input = try file.readInputBlockBorrowed(self.probe_offset);
                        self.probe_input_index = 0;
                        self.unmatched_probe = 0;
                        @memset(&self.probe_matches, false);
                    }
                    const block = self.probe_input.?.view();
                    if (block.count() > self.probe_matches.len) return error.InvalidSqlSpill;
                    const width = block.keyWidth();
                    if (self.probe_cells.len < width) self.probe_cells = try self.a.realloc(self.probe_cells, width);
                    const keys = self.probe_cells[0..width];
                    for (keys, 0..) |*key, column| key.* = try block.keyCell(self.probe_input_index, column);
                    self.probe_input_index += 1;
                    self.probe_offset += 1;
                    self.probe = try self.hash.?.probe(keys);
                    continue;
                }
                if (!may_advance) return null;
                file.close();
                self.active.?.probes = null;
            }
            if (self.outer_right) if (try self.hash.?.unmatched(&self.unmatched)) |match| return .{ .left = null, .right = NativeRow.build(match) };
            if (!may_advance) return null;
            self.hash.?.deinit();
            self.hash = null;
            self.active = null;
            self.unmatched = 0;
            self.probe_offset = 0;
        }
        return null;
    }
};

test "SQL partitioned join retains one partition and preserves residual outer matches" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]usize{ 64 * 1024, 512 * 1024 }) |bytes| for ([_]bool{ false, true }) |batched| {
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
        defer manager.deinit();
        const join = try Join.create(std.testing.allocator, &manager, bytes, 10000, 200000, true, true);
        defer join.close();
        if (batched) {
            var build_cells: [1000]Datum = undefined;
            var probe_cells: [1000]Datum = undefined;
            var build_rows: [1000][]const Datum = undefined;
            var probe_rows: [1000][]const Datum = undefined;
            for (&build_cells, &probe_cells, &build_rows, &probe_rows, 0..) |*build_cell, *probe_cell, *build_row, *probe_row, i| {
                build_cell.* = Datum.json(.{ .integer = @intCast(i) });
                probe_cell.* = Datum.json(.{ .integer = @intCast(i + 500) });
                build_row.* = build_cells[i..][0..1];
                probe_row.* = probe_cells[i..][0..1];
            }
            try join.addBatch(true, .{ .rows = &build_rows }, &build_rows, 0);
            try join.addBatch(false, .{ .rows = &probe_rows }, &probe_rows, 0);
        } else {
            for (0..1000) |i| {
                const key = Datum.json(.{ .integer = @intCast(i) });
                try join.add(true, &.{key}, &.{key}, i);
            }
            for (500..1500) |i| {
                const key = Datum.json(.{ .integer = @intCast(i) });
                try join.add(false, &.{key}, &.{key}, i);
            }
        }
        var matches: usize = 0;
        var lefts: usize = 0;
        var rights: usize = 0;
        while (try join.next()) |pair| {
            if (pair.match) |index| {
                // One ON residual is rejected: both sides must remain unmatched.
                if (pair.left.?[0].value.integer == 777) continue;
                try join.accept(index);
                matches += 1;
            } else if (pair.left != null) {
                lefts += 1;
            } else {
                rights += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 499), matches);
        try std.testing.expectEqual(@as(usize, 501), lefts);
        try std.testing.expectEqual(@as(usize, 501), rights);
        try std.testing.expect(join.partitions_loaded > 1);
        if (bytes >= 512 * 1024) try std.testing.expect(join.parallel_builds_started > 0);
    };
}

test "SQL partitioned join runtime filter preserves null and skew semantics" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const join = try Join.create(std.testing.allocator, &manager, 32768, 10000, 100000, false, true);
    defer join.close();
    const key = Datum.json(.{ .integer = 42 });
    for (0..100) |i| try join.add(true, &.{key}, &.{key}, i);
    try join.add(true, &.{.{}}, &.{.{}}, 100);
    for (0..17) |i| try join.add(false, &.{key}, &.{key}, i);
    for (0..100) |i| if (i != 42) {
        const missing = Datum.json(.{ .integer = @intCast(i) });
        try join.add(false, &.{missing}, &.{missing}, i + 17);
    };
    try join.add(false, &.{.{}}, &.{.{}}, 200);
    // Late build rows would make the already-used runtime filter unsound.
    try std.testing.expectError(error.InvalidSqlBackendResponse, join.add(true, &.{key}, &.{key}, 201));
    var matches: usize = 0;
    var unmatched: usize = 0;
    while (try join.next()) |pair| {
        if (pair.match) |index| {
            try join.accept(index);
            matches += 1;
        } else {
            try std.testing.expect(pair.left == null);
            try std.testing.expect(pair.right.?[0].sql_null);
            unmatched += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 1700), matches);
    try std.testing.expectEqual(@as(usize, 1), unmatched);
    try std.testing.expectEqual(@as(usize, 100), join.filtered_rows);
    try std.testing.expectEqual(@as(usize, 0), join.repartitions);
}

test "SQL partitioned join recursively splits underestimated builds within file and memory quotas" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 256 * 1024 };
    const a = budget.allocator();
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    {
        // Deliberately underestimate bytes: two initial partitions cannot fit.
        const join = try Join.create(a, &manager, 64 * 1024, 2000, 0, true, true);
        defer join.close();
        for (0..1000) |i| {
            const key = Datum.json(.{ .integer = @intCast(i) });
            try join.add(true, &.{key}, &.{key}, i);
        }
        for (950..1050) |i| {
            const key = Datum.json(.{ .integer = @intCast(i) });
            try join.add(false, &.{key}, &.{key}, i);
        }
        var matches: usize = 0;
        var lefts: usize = 0;
        var rights: usize = 0;
        while (try join.next()) |pair| {
            try std.testing.expect(manager.files <= 24);
            if (pair.match) |index| {
                if (pair.left.?[0].value.integer == 975) continue;
                try std.testing.expectEqual(pair.left.?[0].value.integer, pair.right.?[0].value.integer);
                try join.accept(index);
                matches += 1;
            } else if (pair.left != null) {
                lefts += 1;
            } else {
                rights += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 49), matches);
        try std.testing.expectEqual(@as(usize, 51), lefts);
        try std.testing.expectEqual(@as(usize, 951), rights);
        try std.testing.expect(join.repartitions > 0);
        try std.testing.expect(join.partitions_loaded > 2);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expectEqual(@as(usize, 0), manager.files);
    try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
}

test "SQL complete parallel join partitions evaluate residuals and preserve both outer sides" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]bool{ false, true }) |flipped| {
        for ([_]bool{ false, true }) |batches| {
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
            defer manager.deinit();
            var compiled = try @import("compiler.zig").compileScalar(a, "l = r AND l <> 777", .{});
            defer compiled.deinit();
            var condition = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "l", .type = .integer }, .{ .name = "r", .type = .integer } }, &.{}, .{});
            defer condition.deinit();
            const join = try Join.create(a, &manager, 512 * 1024, 10000, 200000, true, true);
            defer join.close();
            join.evaluation = .{ .condition = &condition, .left_width = 1, .right_width = 1, .flipped = flipped };
            for (0..1000) |i| {
                const key = Datum.json(.{ .integer = @intCast(i) });
                try join.add(true, &.{key}, &.{key}, i);
            }
            for (500..1500) |i| {
                const key = Datum.json(.{ .integer = @intCast(i) });
                try join.add(false, &.{key}, &.{key}, i);
            }
            for (&join.build, &join.probes) |*build, *probe| {
                if (build.*) |*file| try file.seal();
                if (probe.*) |*file| try file.seal();
            }
            const written_before = manager.written_bytes;
            var matches: usize = 0;
            var lefts: usize = 0;
            var rights: usize = 0;
            if (batches) {
                while (try join.nextTypedBatch(137)) |view| {
                    defer view.deinit();
                    for (0..view.count) |row| {
                        const left = (try view.keys().cell(a, row, 0)).value.bool;
                        const right = (try view.keys().cell(a, row, 1)).value.bool;
                        if (left and right) {
                            const l = try view.values().cell(a, row, 0);
                            const r = try view.values().cell(a, row, 1);
                            try std.testing.expectEqual(l.value.integer, r.value.integer);
                            try std.testing.expect(l.value.integer != 777);
                            matches += 1;
                        } else if (left) {
                            try std.testing.expect((try view.values().cell(a, row, 1)).sql_null);
                            lefts += 1;
                        } else {
                            try std.testing.expect((try view.values().cell(a, row, 0)).sql_null);
                            rights += 1;
                        }
                    }
                }
            } else while (try join.next()) |pair| {
                // Complete partition jobs already evaluated ON and matched markers.
                try std.testing.expect(pair.match == null);
                if (pair.left != null and pair.right != null) {
                    try std.testing.expectEqual(pair.left.?[0].value.integer, pair.right.?[0].value.integer);
                    try std.testing.expect(pair.left.?[0].value.integer != 777);
                    matches += 1;
                } else if (pair.left != null) lefts += 1 else rights += 1;
            }
            try std.testing.expectEqual(@as(usize, 499), matches);
            try std.testing.expectEqual(@as(usize, 501), lefts);
            try std.testing.expectEqual(@as(usize, 501), rights);
            try std.testing.expect(join.parallel_builds_started > 1);
            try std.testing.expect(join.parallel_partitions_completed > 1);
            try std.testing.expectEqual(written_before, manager.written_bytes);
        }
    }
}

test "SQL parallel partition joins defer errors beyond the delivered prefix" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var compiled = try @import("compiler.zig").compileScalar(a, "1 / (1 - l) > 0", .{});
    defer compiled.deinit();
    var condition = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "l", .type = .integer }, .{ .name = "r", .type = .integer } }, &.{}, .{});
    defer condition.deinit();
    {
        const join = try Join.create(a, &manager, 512 * 1024, 10, 0, false, false);
        defer join.close();
        join.evaluation = .{ .condition = &condition, .left_width = 1, .right_width = 1 };
        const key = Datum.json(.{ .integer = 42 });
        try join.add(true, &.{Datum.json(.{ .integer = 0 })}, &.{key}, 0);
        for (0..2) |i| try join.add(false, &.{Datum.json(.{ .integer = @intCast(i) })}, &.{key}, i);
        const prefix = (try join.next()).?;
        try std.testing.expectEqual(@as(i64, 0), prefix.left.?[0].value.integer);
        try std.testing.expectError(error.SqlDivisionByZero, join.next());
    }
    try std.testing.expectEqual(@as(usize, 0), manager.files);
}

test "SQL review regression oversized identical-key partition preserves forward read position" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = std.heap.page_allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const join = try Join.create(std.heap.page_allocator, &manager, 2 * 1024 * 1024, 100000, 0, false, false);
    defer join.close();
    join.parallel_builds = false;
    const key = Datum.json(.{ .integer = 42 });
    for (0..20000) |i| try join.add(true, &.{Datum.json(.{ .integer = @intCast(i) })}, &.{key}, i);
    try join.add(false, &.{key}, &.{key}, 0);
    var matches: usize = 0;
    while (try join.next()) |_| matches += 1;
    try std.testing.expectEqual(@as(usize, 20000), matches);
}

test "SQL partition join child keeps its assigned workspace and prioritizes largest work" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    for ([_]usize{ 128 * 1024, 2 * 1024 * 1024 }) |bytes| {
        const serial = try Join.createWithLanes(&manager, bytes, 10000, 0, false, false, 1, false);
        defer serial.close();
        try std.testing.expect(!serial.parallel_builds);
        try std.testing.expectEqual(bytes, serial.limits.bytes);
        try serial.finishInputs();
        try std.testing.expectEqual(bytes, serial.limits.bytes);
    }
    const child = try Join.createWithLanes(&manager, 1024 * 1024, 10000, 0, false, false, 1, true);
    defer child.close();
    try std.testing.expectEqual(@as(usize, 1024 * 1024), child.limits.bytes);
    try std.testing.expect(!child.parallel_builds);
    child.build[0] = try spill.Sequential.init(&manager, 128);
    child.build[1] = try spill.Sequential.init(&manager, 128);
    child.build_cost[0] = 100;
    child.build_cost[1] = 200;
    try std.testing.expectEqual(@as(?usize, 1), child.largestPartition());
    child.probe_cost[0] = 1000;
    try std.testing.expectEqual(@as(?usize, 0), child.largestPartition());
    child.parallel_builds = true;
    child.build_lanes = 8;
    child.build_cost[1] = 700 * 1024;
    try child.finishInputs();
    try std.testing.expect(!child.parallel_builds);
    try std.testing.expectEqual(@as(usize, 1024 * 1024), child.limits.bytes);
}

test "SQL compact probe blocks retain rows through duplicate matches and early close" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    for ([_]bool{ false, true }) |early_close| {
        const join = try Join.create(a, &manager, 256 * 1024, 10000, 0, true, false);
        defer join.close();
        for (0..8) |key_index| for (0..3) |duplicate| {
            const key = Datum.json(.{ .integer = @intCast(key_index) });
            try join.add(true, &.{ key, Datum.json(.{ .integer = @intCast(duplicate) }) }, &.{key}, key_index * 3 + duplicate);
        };
        var expected_matches: usize = 0;
        var expected_unmatched: usize = 0;
        for (0..1025) |index| {
            const key = if (index % 97 == 0) Datum{} else Datum.json(.{ .integer = @intCast(index % 8) });
            const nullable = if (index % 2 == 0) Datum{} else Datum.json(.null);
            const text = if (index % 3 == 0) "alpha\x00tail" else "beta";
            try join.add(false, &.{ Datum.json(.{ .integer = @intCast(index) }), Datum.json(.{ .string = text }), nullable }, &.{key}, index);
            if (key.sql_null) expected_unmatched += 1 else expected_matches += 3;
        }
        var matches: usize = 0;
        var unmatched: usize = 0;
        var seen: [1025]u8 = @splat(0);
        while (try join.next()) |pair| {
            const row = pair.left orelse return error.TestUnexpectedResult;
            const index: usize = @intCast(row[0].value.integer);
            try std.testing.expect(index < seen.len);
            try std.testing.expectEqualStrings(if (index % 3 == 0) "alpha\x00tail" else "beta", row[1].value.string);
            try std.testing.expectEqual(index % 2 == 0, row[2].sql_null);
            try std.testing.expect(row[2].value == .null);
            if (pair.match) |matched| {
                try std.testing.expect(index % 97 != 0);
                const right = pair.right orelse return error.TestUnexpectedResult;
                try std.testing.expectEqual(@as(i64, @intCast(index % 8)), right[0].value.integer);
                const duplicate: u3 = @intCast(right[1].value.integer);
                const mask = @as(u8, 1) << duplicate;
                try std.testing.expect(seen[index] & mask == 0);
                seen[index] |= mask;
                try join.accept(matched);
                matches += 1;
                if (early_close) {
                    // Closing while duplicate matches still borrow this probe
                    // row must release the retained physical block exactly once.
                    try std.testing.expect(join.probe_input != null);
                    break;
                }
            } else {
                try std.testing.expect(pair.right == null);
                try std.testing.expect(index % 97 == 0);
                try std.testing.expectEqual(@as(u8, 0), seen[index]);
                seen[index] = 8;
                unmatched += 1;
            }
        }
        if (!early_close) {
            try std.testing.expectEqual(expected_matches, matches);
            try std.testing.expectEqual(expected_unmatched, unmatched);
            for (seen, 0..) |mask, index| try std.testing.expectEqual(@as(u8, if (index % 97 == 0) 8 else 7), mask);
            try std.testing.expect(join.probe_input == null);
        }
    }
}

test "SQL scoped join workers preserve residual outer semantics and physically retire original failures" {
    const Runnable = @import("../common/runnable_scheduler.zig");
    const Mode = enum { prepared, complete, typed, early_close, canceled, deadline, revoked, registry_full };
    const Clock = struct {
        var now_ns: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return now_ns.load(.acquire);
        }
    };
    const Fixture = struct {
        execution: *Runnable.Execution,
        revoked: bool = false,
        invalid_callbacks: std.atomic.Value(usize) = .init(0),
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const state = self.execution.entry.?.state;
            if (state != .running and state != .stopped) {
                _ = self.invalid_callbacks.fetchAdd(1, .monotonic);
                return error.TestUnexpectedCoordinatorCallback;
            }
            try self.execution.checkpoint();
            if (self.revoked) return error.Forbidden;
        }
        fn begin(raw: *anyopaque) !void {
            try check(raw);
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.enter(.query);
            try check(raw);
        }
    };
    const a = std.testing.allocator;
    for (std.enums.values(Mode)) |mode| {
        Clock.now_ns.store(0, .release);
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1, .max_operations = if (mode == .registry_full) 2 else 32 });
        var cancel: std.atomic.Value(bool) = .init(false);
        var execution: Runnable.Execution = .{};
        try execution.start(&scheduler, .{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now, .cancellation = .{ .ptr = &cancel, .requested = Fixture.requested } }, .query);
        defer execution.finish();
        var fixture = Fixture{ .execution = &execution };
        const boundary = @import("execution_wait.zig").Boundary{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end };
        var manager = spill.Manager{ .alloc = a, .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.check, .parallel_work = false, .async_writes = false, .wait = boundary, .cpu_scope = .{ .execution = &execution, .allocator = a, .io = std.testing.io, .wait = boundary } };
        defer manager.deinit();
        var compiled = try @import("compiler.zig").compileScalar(a, "l = r AND l <> 777", .{});
        defer compiled.deinit();
        var condition = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "l", .type = .integer }, .{ .name = "r", .type = .integer } }, &.{}, .{});
        defer condition.deinit();
        {
            const join = try Join.create(a, &manager, 512 * 1024, 10000, 200000, true, true);
            defer join.close();
            if (mode != .prepared) join.evaluation = .{ .condition = &condition, .left_width = 1, .right_width = 1 };
            for (0..1000) |index| {
                const key = Datum.json(.{ .integer = @intCast(index) });
                try join.add(true, &.{key}, &.{key}, index);
            }
            for (500..1500) |index| {
                const key = Datum.json(.{ .integer = @intCast(index) });
                try join.add(false, &.{key}, &.{key}, index);
            }
            try join.finishInputs();
            try std.testing.expect(join.parallel_builds);
            try join.startBuilds();
            if (mode == .registry_full) {
                try std.testing.expectEqual(@as(usize, 0), join.parallel_builds_started);
                try std.testing.expectEqual(@as(usize, 0), execution.entry.?.children);
            } else {
                try std.testing.expect(join.parallel_builds_started >= 2);
                try std.testing.expect(execution.entry.?.children >= 2);
            }
            var health: Runnable.Execution = .{};
            try health.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
            try health.checkpoint();
            health.finish();
            switch (mode) {
                .early_close => {},
                .canceled => {
                    cancel.store(true, .release);
                    try std.testing.expectError(error.Canceled, join.next());
                },
                .deadline => {
                    Clock.now_ns.store(100, .release);
                    try std.testing.expectError(error.DeadlineExceeded, join.next());
                },
                .revoked => {
                    fixture.revoked = true;
                    try std.testing.expectError(error.Forbidden, join.next());
                },
                .prepared, .complete, .typed, .registry_full => {
                    var matches: usize = 0;
                    var lefts: usize = 0;
                    var rights: usize = 0;
                    if (mode == .typed) {
                        while (try join.nextTypedBatch(17)) |view| {
                            defer view.deinit();
                            for (0..view.count) |row| {
                                const left = (try view.keys().cell(a, row, 0)).value.bool;
                                const right = (try view.keys().cell(a, row, 1)).value.bool;
                                if (left and right) {
                                    const l = (try view.values().cell(a, row, 0)).value.integer;
                                    try std.testing.expectEqual(l, (try view.values().cell(a, row, 1)).value.integer);
                                    try std.testing.expect(l != 777);
                                    matches += 1;
                                } else if (left) {
                                    try std.testing.expect((try view.values().cell(a, row, 1)).sql_null);
                                    lefts += 1;
                                } else {
                                    try std.testing.expect((try view.values().cell(a, row, 0)).sql_null);
                                    rights += 1;
                                }
                            }
                        }
                    } else while (try join.next()) |pair| {
                        if (join.active_join) |child| if (child.output == null) {
                            try std.testing.expect(child.execution == null and child.limits.execution == null);
                            if (child.hash) |hash| {
                                try std.testing.expect(hash.limits.execution == null);
                                if (hash.disk) |file| try std.testing.expect(file.execution == null);
                            }
                        };
                        if (pair.match) |index| {
                            if (pair.left.?[0].value.integer == 777) continue;
                            try join.accept(index);
                        }
                        if (pair.left != null and pair.right != null) {
                            try std.testing.expectEqual(pair.left.?[0].value.integer, pair.right.?[0].value.integer);
                            try std.testing.expect(pair.left.?[0].value.integer != 777);
                            matches += 1;
                        } else if (pair.left != null) lefts += 1 else rights += 1;
                    }
                    try std.testing.expectEqual(@as(usize, 499), matches);
                    try std.testing.expectEqual(@as(usize, 501), lefts);
                    try std.testing.expectEqual(@as(usize, 501), rights);
                },
            }
        }
        switch (mode) {
            .prepared, .complete, .typed, .early_close, .registry_full => try execution.checkpoint(),
            .canceled, .deadline, .revoked => {},
        }
        try std.testing.expectEqual(@as(usize, 0), fixture.invalid_callbacks.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), manager.files);
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().child_operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), execution.entry.?.children);
    }
}

test "SQL scoped join preparation detaches recursive partitions and identical key disk chains before handoff" {
    const Runnable = @import("../common/runnable_scheduler.zig");
    const Fixture = struct {
        execution: *Runnable.Execution,
        invalid_callbacks: std.atomic.Value(usize) = .init(0),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.execution.entry.?.state != .running) {
                _ = self.invalid_callbacks.fetchAdd(1, .monotonic);
                return error.TestUnexpectedCoordinatorCallback;
            }
            try self.execution.checkpoint();
        }
        fn begin(raw: *anyopaque) !void {
            try check(raw);
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.enter(.query);
        }
        fn detached(join: *Join) !void {
            try std.testing.expect(join.execution == null and join.limits.execution == null);
            if (join.active) |task| {
                if (task.build) |file| try std.testing.expect(file.file.execution == null);
                if (task.probes) |file| try std.testing.expect(file.file.execution == null);
            }
            for (join.pending[0..join.pending_count]) |task| {
                if (task.build) |file| try std.testing.expect(file.file.execution == null);
                if (task.probes) |file| try std.testing.expect(file.file.execution == null);
            }
            if (join.hash) |hash| {
                try std.testing.expect(hash.limits.execution == null);
                if (hash.disk) |file| try std.testing.expect(file.execution == null);
            }
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |identical| {
        var owner = try Runnable.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1 });
        var execution: Runnable.Execution = .{};
        try execution.start(&owner, .{ .io = std.testing.io }, .query);
        defer execution.finish();
        var fixture = Fixture{ .execution = &execution };
        const boundary = @import("execution_wait.zig").Boundary{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end };
        const scope = @import("parallel_scheduler.zig").ExecutionScope{ .execution = &execution, .allocator = a, .io = std.testing.io, .wait = boundary };
        var manager = spill.Manager{ .alloc = a, .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.check, .parallel_work = false, .async_writes = false, .wait = boundary, .cpu_scope = scope };
        defer manager.deinit();
        {
            const join = try Join.createWithLanes(&manager, 16 * 1024, 10000, 0, true, true, 1, true);
            defer join.close();
            join.active = .{ .build = try join.partitionFile(), .probes = null, .used_bits = 0 };
            join.partitions = 0;
            join.finished = true;
            for (0..1000) |index| {
                const key = Datum.json(.{ .integer = if (identical) 7 else @intCast(index) });
                _ = try join.active.?.build.?.append(.{ .values = &.{key}, .keys = &.{key}, .ordinal = index }, spill.none);
                join.active.?.hashes.add((try operators.HashJoin.keyHash(&.{key})).?);
            }
            try join.active.?.build.?.seal();
            join.active.?.probes = try join.partitionFile();
            if (identical) {
                const key = Datum.json(.{ .integer = 7 });
                _ = try join.active.?.probes.?.append(.{ .values = &.{key}, .keys = &.{key}, .ordinal = 0 }, spill.none);
            } else {
                for (950..1050) |index| {
                    const key = Datum.json(.{ .integer = @intCast(index) });
                    _ = try join.active.?.probes.?.append(.{ .values = &.{key}, .keys = &.{key}, .ordinal = index }, spill.none);
                }
            }
            try join.active.?.probes.?.seal();
            var task = (try scope.submit(16 * 1024 + @sizeOf(Join), a, Join.prepareChild, .{join}, Join.discardStatus)).?;
            defer if (task.future != null) {
                _ = task.cancel(std.testing.io) catch false;
            };
            try std.testing.expect(try task.await(std.testing.io));
            try Fixture.detached(join);
            if (identical) {
                try std.testing.expect(join.skew != null);
                try std.testing.expect(join.skew.?.execution == null);
                if (join.skew.?.matches) |file| try std.testing.expect(file.execution == null);
            } else {
                try std.testing.expect(join.repartitions > 0 and join.pending_count > 0);
            }
            var matches: usize = 0;
            var lefts: usize = 0;
            var rights: usize = 0;
            while (try join.next()) |pair| {
                if (pair.match) |index| {
                    try std.testing.expectEqual(pair.left.?[0].value.integer, pair.right.?[0].value.integer);
                    try join.accept(index);
                    matches += 1;
                } else if (pair.left != null) lefts += 1 else rights += 1;
            }
            try std.testing.expectEqual(@as(usize, if (identical) 1000 else 50), matches);
            try std.testing.expectEqual(@as(usize, if (identical) 0 else 50), lefts);
            try std.testing.expectEqual(@as(usize, if (identical) 0 else 950), rights);
        }
        try std.testing.expectEqual(@as(usize, 0), fixture.invalid_callbacks.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), manager.files);
        try std.testing.expectEqual(@as(u64, 0), manager.live_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

const NativeJoinFixture = struct {
    text: []const u8 = "{\"x\":9007199254740993,\"items\":[1,2,3]}",
    frozen: ?@import("execution_batch.zig").FrozenJson = null,
    begin: usize,
    extra: bool = false,
    fn cell(raw: *anyopaque, _: A, row: usize, column: usize) !Datum {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (column == 0) return Datum.json(.{ .integer = @intCast(self.begin + row) });
        if (column == 2 and self.extra) return Datum.json(.{ .integer = 99 });
        return error.UnexpectedScalarMaterialization;
    }
    fn json(raw: *anyopaque, _: usize, column: usize) !?[]const u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (column == 1 and self.frozen == null) self.text else null;
    }
    fn tokens(raw: *anyopaque, _: usize, column: usize) !?@import("execution_batch.zig").FrozenJson {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (column == 1) self.frozen else null;
    }
    fn add(self: *@This(), a: A, join: *Join, build: bool, count: usize) !void {
        const integers = try a.alloc(Datum, count);
        defer a.free(integers);
        const keys = try a.alloc([]const Datum, count);
        defer a.free(keys);
        for (integers, keys, 0..) |*integer, *key, index| {
            integer.* = try cell(self, a, index, 0);
            key.* = @as(*[1]Datum, @ptrCast(integer));
        }
        try join.addBatch(build, .{ .reader = .{ .ptr = self, .read = cell, .read_json = json, .read_frozen = tokens, .count = count, .width = if (self.extra) 3 else 2 } }, keys, 0);
    }
};

test "SQL native partition batches preserve frozen outer rows residuals and retained ownership" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    const Fixture = struct {
        fn run(a: A, parallel: bool, flipped: bool, frozen: bool, decline_worker: bool) !void {
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            var left: NativeJoinFixture = .{ .begin = 16 };
            var right: NativeJoinFixture = .{ .begin = 0, .extra = true };
            if (frozen) {
                const root = try std.json.parseFromSliceLeaky(std.json.Value, sa, left.text, .{});
                const wire = try spill.encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{Datum.json(root)}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
                const block = try spill.decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
                defer block.deinit();
                left.frozen = (try block.frozenJson(0, 0, false)).?;
                right.frozen = left.frozen;
            }
            var compiled = try @import("compiler.zig").compileScalar(a, "l = r AND l <> 17 AND extra = 99", .{});
            defer compiled.deinit();
            const columns: []const @import("scalar.zig").Column = if (flipped) &.{ .{ .name = "r", .type = .integer }, .{ .name = "rj", .type = .json }, .{ .name = "extra", .type = .integer }, .{ .name = "l", .type = .integer }, .{ .name = "lj", .type = .json } } else &.{ .{ .name = "l", .type = .integer }, .{ .name = "lj", .type = .json }, .{ .name = "r", .type = .integer }, .{ .name = "rj", .type = .json }, .{ .name = "extra", .type = .integer } };
            var condition = try @import("scalar.zig").bind(a, compiled.expression, columns, &.{}, .{});
            defer condition.deinit();
            const join = try Join.create(a, &manager, 1 << 20, 10000, 0, true, true);
            defer join.close();
            join.parallel_builds = parallel;
            join.build_lanes = 2;
            join.evaluation = .{ .condition = &condition, .left_width = 2, .right_width = 3, .flipped = flipped };
            try right.add(a, join, true, 32);
            try left.add(a, join, false, 32);
            // Exercise the scheduler-declined branch after selecting parallel
            // lanes, without changing the process-wide scheduler's settings.
            if (decline_worker) {
                try join.finishInputs();
                const child = try Join.createWithLanes(&manager, join.limits.bytes, join.limits.rows, 0, true, true, 1, true);
                child.evaluation = join.evaluation;
                child.partitions = 0;
                child.finished = true;
                child.active = .{ .build = join.build[0], .probes = join.probes[0], .used_bits = join.partitions - 1, .hashes = join.build_hashes[0] };
                join.build[0] = null;
                join.probes[0] = null;
                join.partition = 1;
                join.active_join = child;
            }
            var matches: usize = 0;
            var lefts: usize = 0;
            var rights: usize = 0;
            var retained = @import("typed_store.zig").Store.init(a);
            defer retained.deinit();
            var page = std.heap.ArenaAllocator.init(a);
            defer page.deinit();
            while (true) {
                _ = page.reset(.free_all);
                const batch = try join.nextBatch(page.allocator(), 7, null);
                if (batch.len() == 0) break;
                if (retained.len == 0) try retained.appendBatch(batch);
                for (0..batch.len()) |row| {
                    const l = try batch.cell(page.allocator(), row, 0);
                    const r = try batch.cell(page.allocator(), row, 2);
                    const extra = try batch.cell(page.allocator(), row, 4);
                    if (r.sql_null) try std.testing.expect(extra.sql_null) else try std.testing.expectEqual(@as(i64, 99), extra.value.integer);
                    if (!l.sql_null and !r.sql_null) {
                        try std.testing.expectEqual(l.value.integer, r.value.integer);
                        try std.testing.expect(l.value.integer != 17);
                        matches += 1;
                    } else if (l.sql_null) rights += 1 else lefts += 1;
                    for ([_]usize{ 1, 3 }) |column| {
                        const missing = if (column == 1) l.sql_null else r.sql_null;
                        if (missing) {
                            try std.testing.expect((try batch.cell(page.allocator(), row, column)).sql_null);
                        } else if (try batch.frozenJson(row, column)) |value| {
                            if (frozen) try std.testing.expect(!value.lossless_text);
                        } else try std.testing.expectEqualStrings(left.text, (try batch.encodedJson(row, column)).?);
                    }
                    // Frozen forwarding must not populate the source DOM cache.
                    if (join.probe_input) |*input| if (input.view().encoded) |block| {
                        if (block.json_owner) |owner| {
                            var cached = owner.cached.valueIterator();
                            while (cached.next()) |entry| try std.testing.expect(entry.*.value == null);
                        }
                    };
                }
            }
            try std.testing.expectEqual(@as(usize, 15), matches);
            try std.testing.expectEqual(@as(usize, 17), lefts);
            try std.testing.expectEqual(@as(usize, 17), rights);
            // Retaining consumers own payloads beyond all input/partition pulls.
            for (0..retained.len) |row| {
                _ = try retained.cell(page.allocator(), row, 0);
                for ([_]usize{ 1, 3 }) |column| {
                    if (try retained.frozenJson(row, column)) |value| {
                        const decoded = try spill.materializeFrozenJson(page.allocator(), value);
                        const number = decoded.value.object.get("x").?;
                        if (frozen) try std.testing.expectEqual(@as(i64, 9007199254740993), number.integer) else try std.testing.expectEqualStrings("9007199254740993", number.number_string);
                    } else if (try retained.encodedJson(row, column)) |text| try std.testing.expectEqualStrings(left.text, text) else try std.testing.expect((try retained.cell(page.allocator(), row, column)).sql_null);
                }
            }
        }
    };
    for ([_]bool{ false, true }) |parallel| for ([_]bool{ false, true }) |flipped| for ([_]bool{ false, true }) |frozen| try Fixture.run(std.testing.allocator, parallel, flipped, frozen, false);
    try Fixture.run(std.testing.allocator, true, true, true, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ false, false, true, false });
}

test "native projection refinements benchmark native partition delivery" {
    const Measure = struct {
        const Result = struct { ns: i96, peak: usize, checksum: u64, rows: usize };
        fn now() i96 {
            if (comptime @import("builtin").link_libc and (@import("builtin").os.tag == .macos or @import("builtin").os.tag == .linux)) {
                var stamp: std.c.timespec = undefined;
                if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("thread CPU clock unavailable");
                return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
            }
            return @import("antfly_platform").time.monotonicNs();
        }
        fn check(_: *anyopaque) !void {}
        fn run(native: bool, text: []const u8) !Result {
            var budget: @import("memory_budget.zig") = .{ .backing = std.testing.allocator, .limit = 64 << 20 };
            const a = budget.allocator();
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            const join = try Join.create(a, &manager, 1 << 20, 10000, 0, false, false);
            defer join.close();
            join.parallel_builds = false;
            join.evaluation = .{ .left_width = 2, .right_width = 2 };
            var source: NativeJoinFixture = .{ .begin = 0, .text = text };
            try source.add(a, join, true, 32);
            try source.add(a, join, false, 32);
            var page = std.heap.ArenaAllocator.init(a);
            defer page.deinit();
            var hash = std.hash.Wyhash.init(0);
            var count: usize = 0;
            const start = now();
            while (true) {
                _ = page.reset(.free_all);
                const work = page.allocator();
                const batch: Batch = if (native) try join.nextBatch(work, 7, null) else blk: {
                    const pair = try join.next() orelse break;
                    const values = try work.alloc(Datum, 4);
                    @memcpy(values[0..2], pair.left.?);
                    @memcpy(values[2..4], pair.right.?);
                    if (pair.match) |index| try join.accept(index);
                    const rows = try work.alloc([]const Datum, 1);
                    rows[0] = values;
                    break :blk .{ .rows = rows };
                };
                if (batch.len() == 0) break;
                for (0..batch.len()) |row| {
                    const one = try batch.select(work, &.{row});
                    const bytes = try spill.encodeColumnarBatchVersionAlloc(a, one, &.{0}, 1 << 20, .v2);
                    defer a.free(bytes);
                    hash.update(bytes);
                    count += 1;
                }
            }
            const elapsed = now() - start;
            try std.testing.expectEqual(@as(usize, 32), count);
            return .{ .ns = elapsed, .peak = budget.peak, .checksum = hash.final(), .rows = count };
        }
    };
    var input = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer input.deinit();
    var text: std.Io.Writer.Allocating = .init(input.allocator());
    try text.writer.writeByte('[');
    for (0..256) |index| {
        if (index != 0) try text.writer.writeByte(',');
        try text.writer.writeAll("{\"x\":1,\"s\":\"nested value\"}");
    }
    try text.writer.writeByte(']');
    _ = try Measure.run(false, text.written());
    _ = try Measure.run(true, text.written());
    for (0..5) |sample| {
        const first = try Measure.run(sample % 2 != 0, text.written());
        const second = try Measure.run(sample % 2 == 0, text.written());
        const before = if (sample % 2 == 0) first else second;
        const after = if (sample % 2 == 0) second else first;
        try std.testing.expectEqual(before.checksum, after.checksum);
        try std.testing.expect(after.peak < before.peak);
        std.debug.print("NATIVE_PARTITION_DELIVERY sample={d} before_cpu_ns={d} after_cpu_ns={d} before_peak={d} after_peak={d} rows={d}\n", .{ sample, before.ns, after.ns, before.peak, after.peak, after.rows });
    }
}

test "SQL native partition batches deliver errors after a successful prefix" {
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]bool{ false, true }) |parallel| {
        const a = std.testing.allocator;
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
        defer manager.deinit();
        var compiled = try @import("compiler.zig").compileScalar(a, "1/(1-l)>0", .{});
        defer compiled.deinit();
        var condition = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "l", .type = .integer }, .{ .name = "r", .type = .integer } }, &.{}, .{});
        defer condition.deinit();
        const join = try Join.create(a, &manager, 1 << 20, 10, 0, false, false);
        defer join.close();
        join.parallel_builds = parallel;
        join.build_lanes = 2;
        join.evaluation = .{ .condition = &condition, .left_width = 1, .right_width = 1 };
        const key = Datum.json(.{ .integer = 42 });
        try join.add(true, &.{Datum.json(.{ .integer = 0 })}, &.{key}, 0);
        for (0..2) |i| try join.add(false, &.{Datum.json(.{ .integer = @intCast(i) })}, &.{key}, i);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var failure: ?anyerror = null;
        var count: usize = 0;
        while (failure == null) {
            _ = arena.reset(.free_all);
            const batch = try join.nextBatch(arena.allocator(), 7, &failure);
            for (0..batch.len()) |row| try std.testing.expectEqual(@as(i64, 0), (try batch.cell(arena.allocator(), row, 0)).value.integer);
            count += batch.len();
            if (batch.len() == 0 and failure == null) return error.ExpectedDivisionFailure;
        }
        try std.testing.expectEqual(@as(usize, 1), count);
        try std.testing.expectEqual(error.SqlDivisionByZero, failure.?);
    }
}

test "SQL native residual controls preserve cancellation and regex reuse across worker boundaries" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn cancel(_: ?*anyopaque) anyerror!void {
            return error.QueryCanceled;
        }
        fn run(a: A, parallel: bool, canceled: bool) !void {
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var compiled = try @import("compiler.zig").compileScalar(a, "l = r AND regexp_like(s, '^a')", .{});
            defer compiled.deinit();
            var condition = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "l", .type = .integer }, .{ .name = "r", .type = .integer }, .{ .name = "s", .type = .string } }, &.{}, .{});
            defer condition.deinit();
            var regex = @import("regex_execution.zig").init(a, 1 << 20);
            defer regex.deinit();
            const join = try Join.create(a, &manager, 1 << 20, 10000, 0, false, false);
            defer join.close();
            join.parallel_builds = parallel;
            join.build_lanes = 2;
            join.evaluation = .{ .condition = &condition, .left_width = 1, .right_width = 2, .limits = .{ .regex_execution = &regex, .checkpoint = if (canceled) cancel else null, .regex_checkpoint = if (canceled) cancel else null } };
            const key = Datum.json(.{ .integer = 1 });
            for (0..4) |i| {
                try join.add(true, &.{ key, Datum.json(.{ .string = "abc" }) }, &.{key}, i);
            }
            for (0..4) |i| try join.add(false, &.{key}, &.{key}, i);
            var region = std.heap.ArenaAllocator.init(a);
            defer region.deinit();
            if (canceled) {
                try std.testing.expectError(error.QueryCanceled, join.nextBatch(region.allocator(), 32, null));
            } else {
                var count: usize = 0;
                while (true) {
                    _ = region.reset(.free_all);
                    const batch = try join.nextBatch(region.allocator(), 32, null);
                    if (batch.len() == 0) break;
                    count += batch.len();
                }
                try std.testing.expectEqual(@as(usize, 16), count);
                const stats = regex.snapshot();
                try std.testing.expect(stats.compilations > 0 and stats.hits > 0);
                try std.testing.expectEqual(@as(usize, 0), stats.active);
            }
        }
    };
    for ([_]bool{ false, true }) |parallel| for ([_]bool{ false, true }) |canceled| try Fixture.run(std.testing.allocator, parallel, canceled);
}

test "SQL rejected partition candidates bound output scratch" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(builds: usize, probes: usize, cap: usize) !void {
            const a = std.testing.allocator;
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var compiled = try @import("compiler.zig").compileScalar(a, "l < r", .{});
            defer compiled.deinit();
            var program = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "l", .type = .integer }, .{ .name = "r", .type = .integer } }, &.{}, .{});
            defer program.deinit();
            const join = try Join.create(a, &manager, 1 << 20, 10000, 0, false, false);
            defer join.close();
            join.parallel_builds = false;
            join.evaluation = .{ .condition = &program, .left_width = 1, .right_width = 1 };
            const key = Datum.json(.{ .integer = 7 });
            for (0..builds) |i| try join.add(true, &.{Datum.json(.{ .integer = 0 })}, &.{key}, i);
            for (0..probes) |i| try join.add(false, &.{Datum.json(.{ .integer = 1 })}, &.{key}, i);
            var budget: @import("memory_budget.zig") = .{ .backing = a, .limit = cap };
            var page = std.heap.ArenaAllocator.init(budget.allocator());
            defer page.deinit();
            const batch = try join.nextBatch(page.allocator(), 1, null);
            {
                try std.testing.expectEqual(@as(usize, 0), batch.len());
                std.debug.print("PARTITION_REJECT matches={d} rows={d} page_peak={d} cap={d} error=none\n", .{ builds * probes, batch.len(), budget.peak, cap });
                try std.testing.expectEqual(@as(usize, 0), budget.peak);
            }
        }
    };
    try Fixture.run(8, 8, 128 << 10);
    try Fixture.run(64, 128, 128 << 10);
}

test "SQL native partition row references preserve duplicate cohorts and bounded skew fallbacks" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A, skewed: bool, output_failure: bool) !void {
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            const join = try Join.create(a, &manager, 64 << 10, 10000, 0, false, false);
            defer join.close();
            join.parallel_builds = false;
            join.evaluation = .{ .left_width = 1, .right_width = 2 };
            const key = Datum.json(.{ .integer = 7 });
            // Distinct large strings force bounded skew replay.
            var text: [2048]u8 = @splat('x');
            for (0..32) |i| {
                text[0] = @as(u8, @intCast(i)) + 'A';
                try join.add(true, &.{ Datum.json(.{ .integer = @intCast(i) }), Datum.json(.{ .string = if (skewed) &text else text[0..1] }) }, &.{key}, i);
            }
            for (0..9) |i| try join.add(false, &.{Datum.json(.{ .integer = @intCast(i) })}, &.{key}, i);
            if (output_failure) {
                var none = std.heap.FixedBufferAllocator.init(&.{});
                var failure: ?anyerror = null;
                const batch = try join.nextBatch(none.allocator(), 91, &failure);
                try std.testing.expectEqual(@as(usize, 0), batch.len());
                try std.testing.expect(failure != null);
                try std.testing.expectEqual(error.OutOfMemory, failure.?);
                try std.testing.expect(join.skew != null);
                return;
            }
            var page = std.heap.ArenaAllocator.init(a);
            defer page.deinit();
            var seen: [9][32]bool = @splat(@splat(false));
            var count: usize = 0;
            var saw_skew = false;
            while (true) {
                _ = page.reset(.free_all);
                const batch = try join.nextBatch(page.allocator(), 91, null);
                saw_skew = saw_skew or join.skew != null;
                if (batch.len() == 0) break;
                // Read only after the entire delivery page has been staged.
                // This catches mutable probe selections and transient disks.
                for (0..batch.len()) |row| {
                    const left: usize = @intCast((try batch.cell(page.allocator(), row, 0)).value.integer);
                    const right: usize = @intCast((try batch.cell(page.allocator(), row, 1)).value.integer);
                    const payload = (try batch.cell(page.allocator(), row, 2)).value.string;
                    try std.testing.expect(left < 9 and right < 32);
                    try std.testing.expect(!seen[left][right]);
                    seen[left][right] = true;
                    try std.testing.expectEqual(if (skewed) @as(usize, 2048) else 1, payload.len);
                    try std.testing.expectEqual(@as(u8, @intCast(right)) + 'A', payload[0]);
                    count += 1;
                }
            }
            try std.testing.expectEqual(@as(usize, 9 * 32), count);
            try std.testing.expectEqual(skewed, saw_skew);
        }
    };
    for ([_]bool{ false, true }) |skewed| try Fixture.run(std.testing.allocator, skewed, false);
    try Fixture.run(std.testing.allocator, true, true);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{ false, false });
}

test "SQL partitioned join block skew fallback preserves residual outer matches and bounded replay" {
    const a = std.testing.allocator;
    var context: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    const join = try Join.create(a, &manager, 32 * 1024, 2000, 0, true, true);
    defer join.close();
    const key = Datum.json(.{ .integer = 7 });
    for (0..1200) |i| try join.add(true, &.{Datum.json(.{ .integer = @intCast(i) })}, &.{key}, i);
    for (0..40) |i| try join.add(false, &.{Datum.json(.{ .integer = @intCast(i) })}, &.{key}, i);
    var accepted: usize = 0;
    var left_only: usize = 0;
    var right_only: usize = 0;
    while (try join.next()) |pair| {
        if (pair.match) |match| {
            if (pair.left.?[0].value.integer == pair.right.?[0].value.integer and pair.left.?[0].value.integer < 20) {
                try join.accept(match);
                accepted += 1;
            }
        } else if (pair.left != null) left_only += 1 else right_only += 1;
    }
    try std.testing.expectEqual(@as(usize, 20), accepted);
    try std.testing.expectEqual(@as(usize, 20), left_only);
    try std.testing.expectEqual(@as(usize, 1180), right_only);
    try std.testing.expect(join.skew_blocks_read != 0);
    try std.testing.expect(join.skew_blocks_read < 1200 * 4);
    std.debug.print("SQL skew join: candidates=48000 typed_build_block_decodes={d}\n", .{join.skew_blocks_read});
}

fn skewEarlyClose(a: A) !void {
    var context: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    var build = try spill.Sequential.init(&manager, 1024);
    defer build.close();
    var probes = try spill.Sequential.init(&manager, 1024);
    defer probes.close();
    const key = Datum.json(.{ .integer = 7 });
    for (0..3) |i| _ = try build.append(.{ .values = &.{Datum.json(.{ .integer = @intCast(i) })}, .keys = &.{key}, .ordinal = i }, spill.none);
    _ = try probes.append(.{ .values = &.{key}, .keys = &.{key}, .ordinal = 0 }, spill.none);
    try build.seal();
    try probes.seal();
    const skew = try @import("skew_join.zig").Join.create(&manager, true, true);
    defer skew.close();
    const first = (try skew.next(&build, &probes)) orelse return error.TestUnexpectedResult;
    try skew.accept(first.match orelse return error.TestUnexpectedResult);
}

test "SQL partitioned join block skew releases typed inputs and match state on early close and allocation faults" {
    try skewEarlyClose(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, skewEarlyClose, .{});
}

test "SQL parallel partition residuals use logical output permutations" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    for ([_]bool{ false, true }) |flipped| {
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
        defer manager.deinit();
        var compiled = try @import("compiler.zig").compileScalar(a, if (flipped) "rk=lk AND rv-lv = -10000" else "rk=lk AND rv-lv = 10000", .{});
        defer compiled.deinit();
        var condition = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "rk", .type = .integer }, .{ .name = "rv", .type = .integer }, .{ .name = "lk", .type = .integer }, .{ .name = "lv", .type = .integer } }, &.{}, .{});
        defer condition.deinit();
        const join = try Join.create(a, &manager, 512 * 1024, 10000, 200000, false, false);
        defer join.close();
        join.evaluation = .{ .condition = &condition, .left_width = 2, .right_width = 2, .flipped = flipped, .output_order = &.{ 2, 3, 0, 1 } };
        for (0..512) |i| {
            const key = Datum.json(.{ .integer = @intCast(i) });
            try join.add(true, &.{ key, Datum.json(.{ .integer = @intCast(i + 20000) }) }, &.{key}, i);
        }
        for (0..512) |i| {
            const key = Datum.json(.{ .integer = @intCast(i) });
            try join.add(false, &.{ key, Datum.json(.{ .integer = @intCast(i + 10000) }) }, &.{key}, i);
        }
        var count: usize = 0;
        while (try join.nextTypedBatch(137)) |view| {
            defer view.deinit();
            for (0..view.count) |row| {
                const values = view.values();
                const left = (try values.cell(a, row, 1)).value.integer;
                const right = (try values.cell(a, row, 3)).value.integer;
                try std.testing.expectEqual(@as(i64, 10000), right - left);
                count += 1;
            }
        }
        try std.testing.expectEqual(@as(usize, 512), count);
        try std.testing.expect(join.parallel_partitions_completed > 1);
    }
}

test "SQL partitioned vector residual batches preserve skew outer matches" {
    const a = std.testing.allocator;
    var context: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &context, .checkpoint = struct {
        fn check(_: *anyopaque) !void {}
    }.check, .async_writes = false };
    defer manager.deinit();
    var compiled = try @import("compiler.zig").compileScalar(a, "l = r", .{});
    defer compiled.deinit();
    var condition = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "l", .type = .integer }, .{ .name = "r", .type = .integer } }, &.{}, .{});
    defer condition.deinit();
    const join = try Join.create(a, &manager, 32 * 1024, 2000, 0, true, true);
    defer join.close();
    join.evaluation = .{ .condition = &condition, .left_width = 1, .right_width = 1 };
    const key = Datum.json(.{ .integer = 7 });
    for (0..1200) |i| try join.add(true, &.{Datum.json(.{ .integer = @intCast(i) })}, &.{key}, i);
    for (0..40) |i| try join.add(false, &.{Datum.json(.{ .integer = @intCast(i) })}, &.{key}, i);
    var accepted: usize = 0;
    var unmatched: usize = 0;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    while (true) {
        _ = arena.reset(.free_all);
        const batch = try join.nextBatch(arena.allocator(), 64, null);
        if (batch.len() == 0) break;
        for (0..batch.len()) |row| {
            const left = try batch.cell(arena.allocator(), row, 0);
            const right = try batch.cell(arena.allocator(), row, 1);
            if (left.sql_null or right.sql_null) unmatched += 1 else {
                try std.testing.expectEqual(left.value.integer, right.value.integer);
                accepted += 1;
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 40), accepted);
    try std.testing.expectEqual(@as(usize, 1160), unmatched);
    try std.testing.expect(join.skew_blocks_read > 0);
    try std.testing.expect(join.vector_residual_batches > 0);
    try std.testing.expect(join.scalar_residual_rows < 1200 * 40 / 2);
}

test "SQL partitioned join batches one-to-one probes and defers outer residual decisions" {
    const a = std.testing.allocator;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    var compiled = try @import("compiler.zig").compileScalar(a, "l < 500", .{});
    defer compiled.deinit();
    var condition = try @import("scalar.zig").bind(a, compiled.expression, &.{ .{ .name = "l", .type = .integer }, .{ .name = "r", .type = .integer } }, &.{}, .{});
    defer condition.deinit();
    const join = try Join.create(a, &manager, 256 * 1024, 10000, 200000, true, true);
    defer join.close();
    join.evaluation = .{ .condition = &condition, .left_width = 1, .right_width = 1 };
    var cells: [1000]Datum = undefined;
    var rows: [1000][]const Datum = undefined;
    for (&cells, &rows, 0..) |*cell, *row, i| {
        cell.* = Datum.json(.{ .integer = @intCast(i) });
        row.* = cells[i..][0..1];
    }
    try join.addBatch(true, .{ .rows = &rows }, &rows, 0);
    try join.addBatch(false, .{ .rows = &rows }, &rows, 0);
    var matched: usize = 0;
    var left: usize = 0;
    var right: usize = 0;
    while (true) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var failure: ?anyerror = null;
        const batch = try join.nextBatch(arena.allocator(), 64, &failure);
        if (failure) |err| return err;
        if (batch.len() == 0) break;
        for (0..batch.len()) |row| {
            const l = try batch.cell(arena.allocator(), row, 0);
            const r = try batch.cell(arena.allocator(), row, 1);
            if (l.sql_null) {
                right += 1;
                try std.testing.expect(r.value.integer >= 500);
            } else if (r.sql_null) {
                left += 1;
                try std.testing.expect(l.value.integer >= 500);
            } else {
                matched += 1;
                try std.testing.expect(l.value.integer < 500);
                try std.testing.expectEqual(l.value.integer, r.value.integer);
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 500), matched);
    try std.testing.expectEqual(@as(usize, 500), left);
    try std.testing.expectEqual(@as(usize, 500), right);
    try std.testing.expect(join.vector_residual_batches > 0);
    try std.testing.expect(join.scalar_residual_rows < 100);
}
