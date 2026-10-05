// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Grace hash join: sequentially partition both relations, then retain one
//! build partition. Oversized partitions repartition on remaining hash bits;
//! indistinguishable/skewed keys keep the bounded disk-chain fallback.
const std = @import("std");
const operators = @import("operators.zig");
const spill = @import("spill.zig");
const Datum = @import("scalar.zig").Datum;
const A = std.mem.Allocator;
pub const Pair = struct { left: ?[]const Datum, right: ?[]const Datum, match: ?usize = null };
pub const Join = struct {
    pub const Evaluation = struct {
        condition: ?*const @import("scalar.zig").Program = null,
        parameters: []const std.json.Value = &.{},
        left_width: usize,
        right_width: usize,
        flipped: bool = false,
    };

    const Task = struct {
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
    limits: operators.HashJoin.Limits,
    partitions: usize,
    workspace_bytes: usize,
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
    hash: ?*operators.HashJoin = null,
    probe: ?operators.HashJoin.Probe = null,
    left: ?[]const Datum = null,
    matched: bool = false,
    unmatched: usize = 0,
    rows: [2]usize = @splat(0),
    next_ordinals: [2]usize = @splat(0),
    finished: bool = false,
    probing_started: bool = false,
    probe_arena: std.heap.ArenaAllocator,
    scratch: std.heap.ArenaAllocator,
    candidate: std.heap.ArenaAllocator,
    output_arena: std.heap.ArenaAllocator,
    partitions_loaded: usize = 0,
    filter: []u64,
    filtered_rows: usize = 0,
    parallel_builds: bool = false,
    evaluation: ?Evaluation = null,
    output: ?*@import("parallel_output.zig").Pipe = null,
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
    fn spoolPartition(self: *Join, file: *@import("parallel_output.zig").Pipe) !void {
        const evaluation = self.evaluation.?;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        while (try self.next()) |pair| {
            _ = arena.reset(.retain_capacity);
            const a = arena.allocator();
            if (pair.match) |index| {
                if (evaluation.condition) |program| {
                    const cells = try a.alloc(Datum, evaluation.left_width + evaluation.right_width);
                    @memset(cells, .{});
                    if (pair.left) |values| @memcpy(cells[0..evaluation.left_width], values);
                    if (pair.right) |values| @memcpy(cells[evaluation.left_width..], values);
                    const logical = if (!evaluation.flipped) cells else blk: {
                        const reordered = try a.alloc(Datum, cells.len);
                        @memcpy(reordered[0..evaluation.right_width], cells[evaluation.left_width..]);
                        @memcpy(reordered[evaluation.right_width..], cells[0..evaluation.left_width]);
                        break :blk reordered;
                    };
                    const accepted = try program.evaluate(a, logical, evaluation.parameters, .{});
                    if (accepted.sql_null) continue;
                    if (accepted.value != .bool) return error.SqlTypeMismatch;
                    if (!accepted.value.bool) continue;
                }
                try self.accept(index);
            }
            try file.appendJoined(pair.left, pair.right, evaluation.left_width, evaluation.right_width);
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
            self.limits.bytes = @max(8192, if (self.parallel_builds) self.workspace_bytes / (lanes + 1) else self.workspace_bytes);
        }
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
                child.active = .{ .build = self.build[index], .probes = self.probes[index], .used_bits = self.partitions - 1 };
                self.build[index] = null;
                self.probes[index] = null;
                slot.* = child;
                if (self.evaluation != null) {
                    child.output = try @import("parallel_output.zig").Pipe.create(self.manager, child.limits.bytes / 64);
                    child.output_task = @import("parallel_scheduler.zig").global().submit(self.manager.io, child.limits.bytes, Join.executePartition, .{child});
                    if (child.output_task == null) {
                        child.output.?.close();
                        child.output = null;
                        _ = try child.prepare();
                    } else self.parallel_builds_started += 1;
                } else {
                    task.* = @import("parallel_scheduler.zig").global().submit(self.manager.io, child.limits.bytes, Join.prepare, .{child});
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
        const lanes = requested_lanes orelse @import("parallel_scheduler.zig").global().fanout(partitions, bytes, 256 * 1024);
        self.* = .{ .a = a, .manager = manager, .workspace_bytes = bytes, .build_lanes = lanes, .limits = .{ .bytes = @max(8192, if (assigned) bytes else bytes / (lanes + 1)), .rows = rows, .spill = manager }, .partitions = partitions, .outer_left = outer_left, .outer_right = outer_right, .probe_arena = .init(a), .scratch = .init(a), .candidate = .init(a), .output_arena = .init(a), .filter = filter, .parallel_builds = bytes >= 512 * 1024 and lanes >= 2 };
        return self;
    }
    pub fn addBatch(self: *Join, build_side: bool, batch: @import("execution_batch.zig").Batch, keys: []const []const Datum, begin: usize) !void {
        if (keys.len != batch.len() or begin > keys.len) return error.InvalidSqlBackendResponse;
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        for (begin..keys.len) |index| {
            _ = arena.reset(.retain_capacity);
            const ordinal = self.next_ordinals[@intFromBool(build_side)];
            try self.add(build_side, try batch.row(arena.allocator(), index), keys[index], ordinal);
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
        self.probe_arena.deinit();
        self.scratch.deinit();
        self.candidate.deinit();
        self.output_arena.deinit();
        self.a.free(self.filter);
        self.a.destroy(self);
    }
    fn closeInputs(self: *Join) void {
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
        _ = self.probe_arena.reset(.free_all);
        _ = self.scratch.reset(.free_all);
        _ = self.candidate.reset(.free_all);
    }
    // The caller supplies all build rows before probe rows. This invariant
    // keeps the runtime filter complete and prevents false-negative matches.
    pub fn add(self: *Join, build: bool, values: []const Datum, keys: []const Datum, ordinal: usize) !void {
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
                return;
            }
        } else if ((!build and !self.outer_left) or (build and !self.outer_right)) {
            self.filtered_rows += @intFromBool(!build);
            return;
        }
        const side: usize = @intFromBool(build);
        if (self.rows[side] >= self.limits.rows) return error.SqlProgramLimitExceeded;
        self.rows[side] += 1;
        const partition: usize = @intCast((hash orelse 0) & (self.partitions - 1));
        const slot = if (build) &self.build[partition] else &self.probes[partition];
        if (slot.* == null) {
            slot.* = try spill.Sequential.init(self.manager, @min(4096, self.limits.bytes / 128));
            // Both sides may keep sixteen open partition files. Reserve a
            // bounded share for their I/O buffers rather than exhausting a
            // small statement before a build partition can be loaded.
            slot.*.?.buffer_bytes = @max(128, @min(4096, self.limits.bytes / 128));
        }
        _ = try slot.*.?.append(.{ .values = values, .keys = keys, .ordinal = ordinal }, spill.none);
        const costs = if (build) &self.build_cost else &self.probe_cost;
        costs[partition] +|= @sizeOf(operators.Row);
        for (values) |value| costs[partition] +|= try operators.datumBytes(value);
        for (keys) |value| costs[partition] +|= try operators.datumBytes(value);
    }
    pub fn accept(self: *Join, index: usize) !void {
        if (self.active_join) |child| return child.accept(index);
        self.matched = true;
        if (self.outer_right) try self.hash.?.markMatched(index);
    }
    fn partitionFile(self: *Join) !spill.Sequential {
        var file = try spill.Sequential.init(self.manager, @min(4096, self.limits.bytes / 128));
        file.buffer_bytes = @max(128, @min(4096, self.limits.bytes / 128));
        return file;
    }
    fn split(self: *Join, differences: u64) !bool {
        const parent = &self.active.?;
        const available = differences & ~parent.used_bits;
        // Identical key hashes cannot be separated. Cap depth and file count
        // independently of relation size; those cases retain disk probing.
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
                var offset: u64 = 0;
                while (offset < file.size) {
                    try self.manager.check();
                    _ = self.scratch.reset(.free_all);
                    const row = try file.readBorrowed(offset);
                    const hash = (try operators.HashJoin.keyHash(row.row.keys)) orelse 0;
                    const child = &children[@intFromBool(hash & bit != 0)];
                    const target = if (build_side) &child.build else &child.probes;
                    if (target.* == null) target.* = try self.partitionFile();
                    _ = try target.*.?.append(row.row, spill.none);
                    offset = row.following;
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
    fn prepare(self: *Join) anyerror!bool {
        while (self.hash == null) {
            try self.manager.check();
            if (self.active == null) {
                if (self.pending_count != 0) {
                    self.pending_count -= 1;
                    self.active = self.pending[self.pending_count];
                } else {
                    if (self.partition == self.partitions) return false;
                    self.active = .{ .build = self.build[self.partition], .probes = self.probes[self.partition], .used_bits = self.partitions - 1 };
                    self.build[self.partition] = null;
                    self.probes[self.partition] = null;
                    self.partition += 1;
                }
            }
            self.hash = try operators.HashJoin.create(self.a, self.limits);
            var hash_union: u64 = 0;
            var hash_intersection: u64 = std.math.maxInt(u64);
            var repartitioned = false;
            if (self.active.?.build) |*file| {
                var offset: u64 = 0;
                var skew_fallback = false;
                while (offset < file.size) {
                    const block = try file.readBatchBorrowed(offset, 256);
                    var values: [256][]const Datum = undefined;
                    var keys: [256][]const Datum = undefined;
                    for (block.rows, 0..) |row, index| {
                        const h = (try operators.HashJoin.keyHash(row.keys)) orelse 0;
                        hash_union |= h;
                        hash_intersection &= h;
                        values[index] = row.values;
                        keys[index] = row.keys;
                    }
                    if (skew_fallback) {
                        for (block.rows) |row| try self.hash.?.add(row.values, row.keys);
                        offset = block.following;
                        continue;
                    }
                    const consumed = try self.hash.?.addBatchUntilFull(self.a, .{ .rows = values[0..block.rows.len] }, keys[0..block.rows.len]);
                    offset += consumed;
                    if (consumed == block.rows.len) continue;
                    // Inspect remaining hashes without copying payloads. A useful
                    // partition split wins over building a temporary disk chain.
                    var remaining = offset;
                    while (remaining < file.size) {
                        const scanned = try file.readBatchBorrowed(remaining, 256);
                        for (scanned.rows) |row| {
                            const h = (try operators.HashJoin.keyHash(row.keys)) orelse 0;
                            hash_union |= h;
                            hash_intersection &= h;
                        }
                        remaining = scanned.following;
                    }
                    if (try self.split(hash_union ^ hash_intersection)) {
                        self.hash.?.deinit();
                        self.hash = null;
                        repartitioned = true;
                        break;
                    }
                    skew_fallback = true;
                    // Restore the forward reader after lookahead. This also
                    // handles admission stopping in the middle of a block.
                    file.rewind();
                    var replay: u64 = 0;
                    while (replay < offset) {
                        const prior = try file.readBatchBorrowed(replay, @intCast(@min(256, offset - replay)));
                        replay = prior.following;
                    }
                }
            }
            if (repartitioned) continue;
            if (self.active.?.build) |*file| file.close();
            self.active.?.build = null;
            self.partitions_loaded += 1;
        }
        return true;
    }
    pub fn next(self: *Join) anyerror!?Pair {
        try self.finishInputs();
        if (self.parallel_builds) return self.nextParallel();
        _ = self.candidate.reset(.{ .retain_with_limit = @min(8192, self.limits.bytes / 32) });
        while (try self.prepare()) {
            try self.manager.check();
            if (self.probe) |*probe| {
                if (try probe.next()) |match| return .{ .left = self.left, .right = try match.materializeValues(self.candidate.allocator()), .match = match.index };
                self.probe = null;
                if (!self.matched and self.outer_left) return .{ .left = self.left, .right = null };
            }
            if (self.active.?.probes) |*file| {
                if (self.probe_offset < file.size) {
                    _ = self.probe_arena.reset(.free_all);
                    const row = try file.readBorrowed(self.probe_offset);
                    self.probe_offset = row.following;
                    self.left = row.row.values;
                    self.matched = false;
                    self.probe = try self.hash.?.probe(row.row.keys);
                    continue;
                }
                file.close();
                self.active.?.probes = null;
            }
            if (self.outer_right) if (try self.hash.?.unmatched(&self.unmatched)) |match| return .{ .left = null, .right = try match.materializeValues(self.candidate.allocator()) };
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
    for ([_]usize{ 64 * 1024, 512 * 1024 }) |bytes| {
        var dummy: u8 = 0;
        var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
        defer manager.deinit();
        const join = try Join.create(std.testing.allocator, &manager, bytes, 10000, 200000, true, true);
        defer join.close();
        for (0..1000) |i| {
            const key = Datum.json(.{ .integer = @intCast(i) });
            try join.add(true, &.{key}, &.{key}, i);
        }
        for (500..1500) |i| {
            const key = Datum.json(.{ .integer = @intCast(i) });
            try join.add(false, &.{key}, &.{key}, i);
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
    }
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
