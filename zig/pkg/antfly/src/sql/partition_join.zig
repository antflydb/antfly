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
    finished: bool = false,
    probing_started: bool = false,
    probe_arena: std.heap.ArenaAllocator,
    scratch: std.heap.ArenaAllocator,
    candidate: std.heap.ArenaAllocator,
    partitions_loaded: usize = 0,
    filter: []u64,
    filtered_rows: usize = 0,
    pub fn create(a: A, manager: *spill.Manager, bytes: usize, rows: usize, build_bytes: u64, outer_left: bool, outer_right: bool) !*Join {
        const self = try a.create(Join);
        errdefer a.destroy(self);
        const filter = try a.alloc(u64, @max(16, @min(8192, bytes / 128)));
        @memset(filter, 0);
        const target = @max(@as(usize, 1), bytes / 16);
        const wanted = @min(@as(u64, 16), @max(@as(u64, 2), build_bytes / target + 1));
        const partitions = std.math.ceilPowerOfTwo(usize, @intCast(wanted)) catch unreachable;
        self.* = .{ .a = a, .manager = manager, .limits = .{ .bytes = @max(8192, bytes / 2), .rows = rows, .spill = manager }, .partitions = partitions, .outer_left = outer_left, .outer_right = outer_right, .probe_arena = .init(a), .scratch = .init(a), .candidate = .init(a), .filter = filter };
        return self;
    }
    pub fn close(self: *Join) void {
        if (self.hash) |hash| hash.deinit();
        for (&self.build, &self.probes) |*build, *probe| {
            if (build.*) |*file| file.close();
            if (probe.*) |*file| file.close();
        }
        if (self.active) |*task| task.close();
        for (self.pending[0..self.pending_count]) |*task| task.close();
        self.probe_arena.deinit();
        self.scratch.deinit();
        self.candidate.deinit();
        self.a.free(self.filter);
        self.a.destroy(self);
    }
    // The caller supplies all build rows before probe rows. This invariant
    // keeps the runtime filter complete and prevents false-negative matches.
    pub fn add(self: *Join, build: bool, values: []const Datum, keys: []const Datum, ordinal: usize) !void {
        if (self.finished or (build and self.probing_started)) return error.InvalidSqlBackendResponse;
        if (!build) self.probing_started = true;
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
    }
    pub fn accept(self: *Join, index: usize) !void {
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
                    const row = try file.read(self.scratch.allocator(), offset);
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
    fn prepare(self: *Join) !bool {
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
            if (self.active.?.build) |*file| {
                var offset: u64 = 0;
                while (offset < file.size) {
                    _ = self.scratch.reset(.free_all);
                    const row = try file.read(self.scratch.allocator(), offset);
                    const hash = (try operators.HashJoin.keyHash(row.row.keys)) orelse 0;
                    hash_union |= hash;
                    hash_intersection &= hash;
                    try self.hash.?.add(row.row.values, row.row.keys);
                    offset = row.following;
                }
            }
            if (self.hash.?.disk != null and try self.split(hash_union ^ hash_intersection)) {
                self.hash.?.deinit();
                self.hash = null;
                continue;
            }
            if (self.active.?.build) |*file| file.close();
            self.active.?.build = null;
            self.partitions_loaded += 1;
        }
        return true;
    }
    pub fn next(self: *Join) !?Pair {
        if (!self.finished) {
            for (&self.build) |*file| if (file.*) |*open| try open.seal();
            for (&self.probes) |*file| if (file.*) |*open| try open.seal();
        }
        self.finished = true;
        _ = self.candidate.reset(.free_all);
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
                    const row = try file.read(self.probe_arena.allocator(), self.probe_offset);
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
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = std.testing.allocator, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const join = try Join.create(std.testing.allocator, &manager, 64 * 1024, 10000, 200000, true, true);
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
