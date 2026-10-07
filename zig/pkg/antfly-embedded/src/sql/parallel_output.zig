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

//! Bounded typed output between partition workers and a pull consumer.
//! Backpressure prevents eager join fan-out or aggregate delivery from adding
//! spill I/O. A terminal error follows the successfully produced prefix.
const platform = @import("antfly_platform");
const std = @import("std");

const spill = @import("spill.zig");
const operators = @import("operators.zig");
const Store = @import("typed_store.zig").Store;
const A = std.mem.Allocator;
const Batch = @import("execution_batch.zig").Batch;
const Block = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    values: Store,
    keys: Store,
    ordinals: std.ArrayList(u64) = .empty,
    bytes: usize = 0,
    failed: bool = false,
    refs: std.atomic.Value(usize) = .init(1),
    fn create(a: A) !*Block {
        const self = try a.create(Block);
        self.* = .{ .a = a, .arena = .init(a), .values = undefined, .keys = undefined };
        self.values = .init(self.arena.allocator());
        self.keys = .init(self.arena.allocator());
        return self;
    }
    fn appendBatch(self: *Block, values: Batch, keys: Batch, ordinals: []const u64) !void {
        if (self.failed or values.len() != keys.len() or values.len() != ordinals.len) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        // Reserve structural metadata first. Never publish a partially updated
        // column set after a failed allocation or normalization.
        try self.ordinals.ensureUnusedCapacity(self.arena.allocator(), ordinals.len);
        try self.values.appendBatch(values);
        try self.keys.appendBatch(keys);
        self.ordinals.appendSliceAssumeCapacity(ordinals);
        var scratch = std.heap.ArenaAllocator.init(self.a);
        defer scratch.deinit();
        for (0..values.len()) |row| {
            _ = scratch.reset(.retain_capacity);
            self.bytes +|= @sizeOf(operators.Row);
            for (0..values.width()) |column| self.bytes +|= try operators.datumBytes(try values.cell(scratch.allocator(), row, column));
            for (0..keys.width()) |column| self.bytes +|= try operators.datumBytes(try keys.cell(scratch.allocator(), row, column));
        }
    }
    fn close(self: *Block) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.a;
        self.values.deinit();
        self.keys.deinit();
        self.arena.deinit();
        a.destroy(self);
    }
};
pub const Pipe = struct {
    manager: *spill.Manager,
    slots: [2]*Block = undefined,
    queue: std.Io.Queue(*Block),
    pending: ?*Block = null,
    current: ?*Block = null,
    index: usize = 0,
    block_bytes: usize,
    terminal_error: ?anyerror = null,
    pub fn create(manager: *spill.Manager, bytes: usize) !*Pipe {
        const self = try manager.allocator().create(Pipe);
        self.* = .{ .manager = manager, .queue = undefined, .block_bytes = @max(512, @min(8192, bytes)) };
        self.queue = .init(&self.slots);
        return self;
    }
    pub const View = struct {
        block: *Block,
        begin: usize,
        count: usize,
        pub fn deinit(self: View) void {
            self.block.close();
        }
        pub fn values(self: View) Batch {
            return .{ .retained = .{ .store = &self.block.values, .begin = self.begin, .count = self.count } };
        }
        pub fn keys(self: View) Batch {
            return .{ .retained = .{ .store = &self.block.keys, .begin = self.begin, .count = self.count } };
        }
        pub fn ordinals(self: View) []const u64 {
            return self.block.ordinals.items[self.begin..][0..self.count];
        }
        pub fn ordinal(self: View, index: usize) u64 {
            return self.block.ordinals.items[self.begin + index];
        }
    };
    pub fn append(self: *Pipe, row: operators.Row) !void {
        return self.appendBatch(.{ .rows = &.{row.values} }, .{ .rows = &.{row.keys} }, &.{row.ordinal});
    }
    pub fn appendBatch(self: *Pipe, values: Batch, keys: Batch, ordinals: []const u64) !void {
        try self.manager.check();
        if (self.pending == null) self.pending = try Block.create(self.manager.allocator());
        try self.pending.?.appendBatch(values, keys, ordinals);
        if (self.pending.?.bytes >= self.block_bytes or self.pending.?.ordinals.items.len >= 64) try self.flush();
    }
    /// Join padding is written column by column; concatenated Datum rows are
    /// needed only when a residual expression actually evaluates them.
    pub fn appendJoined(self: *Pipe, left: ?[]const @import("scalar.zig").Datum, right: ?[]const @import("scalar.zig").Datum, left_width: usize, right_width: usize) !void {
        const Parts = struct {
            left: ?[]const @import("scalar.zig").Datum,
            right: ?[]const @import("scalar.zig").Datum,
            width: usize,
            fn cell(raw: *anyopaque, _: A, _: usize, column: usize) anyerror!@import("scalar.zig").Datum {
                const parts_: *@This() = @ptrCast(@alignCast(raw));
                return if (column < parts_.width) (if (parts_.left) |v| v[column] else .{}) else (if (parts_.right) |v| v[column - parts_.width] else .{});
            }
        };
        var parts: Parts = .{ .left = left, .right = right, .width = left_width };
        const Datum = @import("scalar.zig").Datum;
        const keys = [_]Datum{ Datum.json(.{ .bool = left != null }), Datum.json(.{ .bool = right != null }) };
        try self.appendBatch(.{ .reader = .{ .ptr = &parts, .read = Parts.cell, .count = 1, .width = left_width + right_width } }, .{ .rows = &.{&keys} }, &.{0});
    }
    fn flush(self: *Pipe) !void {
        const block = self.pending orelse return;
        if (block.failed or block.ordinals.items.len == 0) {
            block.close();
            self.pending = null;
            return;
        }
        // Single-element put has unambiguous ownership on cancellation.
        _ = try self.queue.put(self.manager.io, &.{block}, 1);
        self.pending = null;
    }
    pub fn finish(self: *Pipe, failure: ?anyerror) void {
        self.flush() catch |err| {
            self.terminal_error = failure orelse err;
        };
        self.terminal_error = self.terminal_error orelse failure;
        self.queue.close(self.manager.io);
    }
    /// A retained span survives producer and pipe teardown. Consumers release
    /// the span explicitly; successive pulls never invalidate another lease.
    pub fn nextBatch(self: *Pipe, maximum: usize) !?View {
        if (maximum == 0) return error.InvalidSqlLimit;
        try self.manager.check();
        if (self.current) |block| if (self.index == block.ordinals.items.len) {
            block.close();
            self.current = null;
        };
        if (self.current == null) {
            var block: [1]*Block = undefined;
            _ = self.queue.get(self.manager.io, &block, 1) catch |err| switch (err) {
                error.Closed => return null,
                else => return err,
            };
            self.current = block[0];
            self.index = 0;
        }
        const block = self.current.?;
        const begin = self.index;
        const count = @min(maximum, block.ordinals.items.len - begin);
        self.index += count;
        _ = block.refs.fetchAdd(1, .monotonic);
        return .{ .block = block, .begin = begin, .count = count };
    }
    /// Scalar callers materialize only at their expression/result boundary.
    pub fn next(self: *Pipe, a: A) !?operators.Row {
        const view = (try self.nextBatch(1)) orelse return null;
        defer view.deinit();
        return .{ .values = try view.values().row(a, 0), .keys = try view.keys().row(a, 0), .ordinal = view.ordinal(0) };
    }
    /// Aggregate APIs return caller-owned states that can outlive a block.
    pub fn nextOwned(self: *Pipe, a: A) !?operators.Row {
        const row = (try self.next(a)) orelse return null;
        for (@constCast(row.values)) |*value| value.* = try operators.cloneDatum(a, value.*);
        for (@constCast(row.keys)) |*value| value.* = try operators.cloneDatum(a, value.*);
        return row;
    }
    pub fn stop(self: *Pipe) void {
        self.queue.close(self.manager.io);
    }
    /// The producer must be joined before destruction.
    pub fn close(self: *Pipe) void {
        self.stop();
        if (self.pending) |block| block.close();
        if (self.current) |block| block.close();
        while (true) {
            var block: [1]*Block = undefined;
            const count = self.queue.getUncancelable(self.manager.io, &block, 0) catch break;
            if (count == 0) break;
            block[0].close();
        }
        self.manager.allocator().destroy(self);
    }
};

fn allocationScenario(a: A) !void {
    const Datum = @import("scalar.zig").Datum;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = platform.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const pipe = try Pipe.create(&manager, 8192);
    defer pipe.close();
    for (0..2) |i| try pipe.append(.{ .values = &.{ Datum.json(.{ .string = "owned output" }), .{}, Datum.json(.null) }, .keys = &.{Datum.json(.{ .integer = @intCast(i) })}, .ordinal = i });
    pipe.finish(null);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    for (0..2) |i| {
        const row = (try pipe.nextOwned(arena.allocator())).?;
        try std.testing.expectEqual(@as(u64, i), row.ordinal);
        try std.testing.expectEqualStrings("owned output", row.values[0].value.string);
        try std.testing.expect(row.values[1].sql_null and !row.values[2].sql_null);
    }
    try std.testing.expect((try pipe.nextOwned(arena.allocator())) == null);
    try std.testing.expect(pipe.terminal_error == null);
}
test "SQL typed output pipes reclaim partial blocks across allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationScenario, .{});
}

fn leaseScenario(a: A) !void {
    const Datum = @import("scalar.zig").Datum;
    const Hook = struct {
        fn check(_: *anyopaque) !void {}
    };
    var dummy: u8 = 0;
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
    defer manager.deinit();
    const pipe = try Pipe.create(&manager, 8192);
    var owned = true;
    defer if (owned) pipe.close();
    const vectors = [_][]const Datum{ &.{ Datum.json(.{ .string = "owned batch payload" }), .{} }, &.{ Datum.json(.null), Datum.json(.{ .integer = 9007199254740993 }) } };
    try pipe.appendBatch(.{ .vectors = .{ .values = &vectors, .count = 2 } }, .{ .vectors = .{ .values = &.{}, .count = 2 } }, &.{ 7, 11 });
    pipe.finish(null);
    const first = (try pipe.nextBatch(1)).?;
    defer first.deinit();
    const second = (try pipe.nextBatch(1)).?;
    defer second.deinit();
    pipe.close();
    owned = false;
    try std.testing.expectEqualStrings("owned batch payload", (try first.values().cell(a, 0, 0)).value.string);
    try std.testing.expect(!(try first.values().cell(a, 0, 1)).sql_null);
    try std.testing.expect((try second.values().cell(a, 0, 0)).sql_null);
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try second.values().cell(a, 0, 1)).value.integer);
    try std.testing.expectEqual(@as(u64, 11), second.ordinal(0));
}
test "SQL worker column leases survive pipe teardown and allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, leaseScenario, .{});
}
