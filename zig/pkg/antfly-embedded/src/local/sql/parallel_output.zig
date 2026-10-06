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
const std = @import("std");
const spill = @import("spill.zig");
const operators = @import("operators.zig");
const Store = @import("typed_store.zig").Store;
const A = std.mem.Allocator;
const Block = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    values: Store,
    keys: Store,
    ordinals: std.ArrayList(u64) = .empty,
    bytes: usize = 0,
    fn create(a: A) !*Block {
        const self = try a.create(Block);
        self.* = .{ .a = a, .arena = .init(a), .values = undefined, .keys = undefined };
        self.values = .init(self.arena.allocator());
        self.keys = .init(self.arena.allocator());
        return self;
    }
    fn append(self: *Block, row: operators.Row) !void {
        _ = try self.values.append(row.values);
        _ = try self.keys.append(row.keys);
        try self.ordinals.append(self.arena.allocator(), row.ordinal);
        self.bytes +|= @sizeOf(operators.Row);
        for (row.values) |value| self.bytes +|= try operators.datumBytes(value);
        for (row.keys) |value| self.bytes +|= try operators.datumBytes(value);
    }
    fn close(self: *Block) void {
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
    pub fn append(self: *Pipe, row: operators.Row) !void {
        try self.manager.check();
        if (self.pending == null) self.pending = try Block.create(self.manager.allocator());
        try self.pending.?.append(row);
        if (self.pending.?.bytes >= self.block_bytes or self.pending.?.ordinals.items.len >= 64) try self.flush();
    }
    fn flush(self: *Pipe) !void {
        const block = self.pending orelse return;
        if (block.ordinals.items.len == 0) {
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
    /// Borrowed payloads remain valid until the next block is consumed.
    pub fn next(self: *Pipe, a: A) !?operators.Row {
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
        const index = self.index;
        self.index += 1;
        return .{ .values = try block.values.row(a, index), .keys = try block.keys.row(a, index), .ordinal = block.ordinals.items[index] };
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
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = Hook.check };
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
