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
const Batch = @import("execution_batch.zig").Batch;
const Block = struct {
    a: A,
    arena: std.heap.ArenaAllocator,
    values: Store,
    keys: Store,
    ordinals: std.ArrayList(u64) = .empty,
    bytes: usize = 0,
    target: usize = 0,
    failed: bool = false,
    refs: std.atomic.Value(usize) = .init(1),
    fn create(a: A) !*Block {
        const self = try a.create(Block);
        self.* = .{ .a = a, .arena = .init(a), .values = undefined, .keys = undefined };
        self.values = .init(self.arena.allocator());
        self.keys = .init(self.arena.allocator());
        return self;
    }
    fn appendBytes(self: *Block, values: Batch, keys: Batch, count: usize, value_memo: *Store.JsonLease, key_memo: *Store.JsonLease) !usize {
        var bytes = count *| @sizeOf(u64);
        if (!self.values.initialized) bytes +|= @import("typed_store.zig").columnMetadataBytes(values.width());
        if (!self.keys.initialized) bytes +|= @import("typed_store.zig").columnMetadataBytes(keys.width());
        bytes +|= self.values.dictionaryGrowthBytes(count) *| 2;
        bytes +|= self.keys.dictionaryGrowthBytes(count) *| 2;
        bytes +|= try self.values.batchAppendBytesLeased(self.a, values, std.math.maxInt(usize), value_memo);
        bytes +|= try self.keys.batchAppendBytesLeased(self.a, keys, std.math.maxInt(usize), key_memo);
        return bytes;
    }
    fn appendBatch(self: *Block, values: Batch, keys: Batch, ordinals: []const u64, bytes: usize, value_memo: *Store.JsonLease, key_memo: *Store.JsonLease) !void {
        if (self.failed or values.len() != keys.len() or values.len() != ordinals.len) return error.InvalidSqlBackendResponse;
        errdefer self.failed = true;
        try self.ordinals.ensureUnusedCapacity(self.arena.allocator(), ordinals.len);
        try self.values.appendBatchLeased(values, value_memo);
        try self.keys.appendBatchLeased(keys, key_memo);
        self.ordinals.appendSliceAssumeCapacity(ordinals);
        self.bytes +|= bytes;
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
    // Only the producer touches this receiver; clear it before physical join.
    producer_execution: ?*@import("../common/runnable_scheduler.zig").Execution = null,
    slots: [2]*Block = undefined,
    queue: std.Io.Queue(*Block),
    pending: ?*Block = null,
    current: ?*Block = null,
    index: usize = 0,
    block_bytes: usize,
    json_hash_bytes: usize = 0,
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
        try self.manager.checkFor(self.producer_execution);
        if (values.len() != keys.len() or values.len() != ordinals.len) return error.InvalidSqlBackendResponse;
        var begin: usize = 0;
        while (begin < values.len()) {
            try self.manager.checkFor(self.producer_execution);
            if (self.pending == null) {
                self.pending = try Block.create(self.manager.allocator());
                self.pending.?.target = self.block_bytes;
            }
            const count = try self.appendCohort(values, keys, ordinals, begin);
            begin += count;
            if (begin < values.len() or self.pending.?.bytes >= self.pending.?.target or self.pending.?.ordinals.items.len >= 64) try self.flush();
        }
    }
    /// A memo is scoped to both this producer lease and destination block.
    /// Counting generations are fresh for each fitting-prefix attempt, while
    /// hash resolutions and accepted owned payloads survive within the cohort.
    fn leaseCheckpoint(raw: ?*anyopaque) !void {
        const self: *Pipe = @ptrCast(@alignCast(raw.?));
        try self.manager.checkFor(self.producer_execution);
    }
    fn appendCohort(self: *Pipe, values: Batch, keys: Batch, ordinals: []const u64, begin: usize) !usize {
        const block = self.pending.?;
        var work = std.heap.ArenaAllocator.init(self.manager.allocator());
        defer work.deinit();
        var value_memo = Store.JsonLease.initControlled(self.manager.allocator(), .{ .ptr = self, .checkpoint = leaseCheckpoint });
        defer value_memo.deinit();
        var key_memo = Store.JsonLease.initControlled(self.manager.allocator(), .{ .ptr = self, .checkpoint = leaseCheckpoint });
        defer key_memo.deinit();
        defer self.json_hash_bytes +|= value_memo.hashed_bytes +| key_memo.hashed_bytes;
        var consumed: usize = 0;
        while (begin + consumed < values.len() and block.ordinals.items.len < 64) {
            var count = @min(values.len() - begin - consumed, 64 - block.ordinals.items.len);
            while (true) {
                _ = work.reset(.retain_capacity);
                const a = work.allocator();
                const positions = try a.alloc(usize, count);
                for (positions, begin + consumed..) |*position, row| position.* = row;
                const selected = try values.select(a, positions);
                const selected_keys = try keys.select(a, positions);
                const bytes = try block.appendBytes(selected, selected_keys, count, &value_memo, &key_memo);
                if (bytes > block.target -| block.bytes) {
                    if (count > 1) {
                        count = @max(1, count / 2);
                        continue;
                    }
                    if (block.ordinals.items.len != 0) return consumed;
                    // Permit one oversized row plus a bounded metadata window;
                    // duplicates can share it, distinct payloads cannot grow it.
                    block.target = bytes +| self.block_bytes;
                }
                try block.appendBatch(selected, selected_keys, ordinals[begin + consumed ..][0..count], bytes, &value_memo, &key_memo);
                consumed += count;
                break;
            }
            if (block.bytes >= block.target) break;
        }
        return consumed;
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
        const boundary = if (self.producer_execution) |execution| spill.childWait(execution) else null;
        if (boundary) |wait| try wait.begin(wait.ptr);
        const result = self.queue.put(self.manager.io, &.{block}, 1);
        // Commit ownership before reacquisition. A denied resume must never
        // leave both the producer and the queue owning the same block.
        if (result) |_| self.pending = null else |_| {}
        if (boundary) |wait| try wait.end(wait.ptr);
        _ = try result;
    }
    pub fn finish(self: *Pipe, failure: ?anyerror) void {
        self.flush() catch |err| {
            self.terminal_error = failure orelse err;
        };
        self.terminal_error = self.terminal_error orelse failure;
        self.queue.close(self.manager.io);
    }
    fn receive(self: *Pipe) !?*Block {
        var block: [1]*Block = undefined;
        _ = self.queue.get(self.manager.io, &block, 1) catch |err| switch (err) {
            error.Closed => return null,
            else => return err,
        };
        return block[0];
    }
    fn discardReceived(block: ?*Block) void {
        if (block) |owned| owned.close();
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
            const boundary = self.manager.wait orelse if (self.manager.cpu_scope) |scope| scope.wait else null;
            const block = try @import("execution_wait.zig").runOwned(boundary, receive, .{self}, discardReceived, .{});
            errdefer if (block) |owned| owned.close();
            if (boundary != null) try self.manager.check();
            self.current = block orelse return null;
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
    /// Terminal notification must also work when a producer never activated.
    /// Its pending block remains producer-owned until the physical join.
    pub fn abort(self: *Pipe, failure: ?anyerror) void {
        self.terminal_error = self.terminal_error orelse failure;
        self.stop();
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
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, allocationScenario, .{});
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

test "SQL scoped output backpressure releases CPU and held views survive physical producer retirement" {
    const Runnable = @import("../common/runnable_scheduler.zig");
    const Fixture = struct {
        execution: *Runnable.Execution,
        pipe: *Pipe = undefined,
        producing: std.atomic.Value(usize) = .init(0),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(Runnable.State.running, self.execution.entry.?.state);
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
        fn work(self: *@This(), execution: *Runnable.Execution) anyerror!void {
            self.pipe.producer_execution = execution;
            defer self.pipe.producer_execution = null;
            var failure: ?anyerror = null;
            self.produce() catch |err| {
                failure = err;
            };
            self.pipe.finish(failure);
        }
        fn produce(self: *@This()) !void {
            for (0..8) |index| {
                self.producing.store(index + 1, .release);
                try self.pipe.append(.{ .values = &.{@import("scalar.zig").Datum.json(.{ .string = &@as([512]u8, @splat('x')) })}, .keys = &.{}, .ordinal = index });
            }
        }
        fn discard(_: A, _: void) void {}
    };
    const a = std.testing.allocator;
    var owner = try Runnable.Scheduler.init(.{ .slots = 2 });
    var execution: Runnable.Execution = .{};
    try execution.start(&owner, .{ .io = std.testing.io }, .query);
    defer execution.finish();
    var fixture = Fixture{ .execution = &execution };
    const boundary = @import("execution_wait.zig").Boundary{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end };
    var manager = spill.Manager{ .alloc = a, .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.check, .wait = boundary, .parallel_work = false };
    defer manager.deinit();
    const pipe = try Pipe.create(&manager, 512);
    var open = true;
    defer if (open) pipe.close();
    fixture.pipe = pipe;
    const scope = @import("parallel_scheduler.zig").ExecutionScope{ .execution = &execution, .allocator = a, .io = std.testing.io, .wait = boundary };
    var task = (try scope.submit(8192, a, Fixture.work, .{&fixture}, Fixture.discard)).?;
    defer if (task.future != null) {
        pipe.stop();
        _ = task.cancel(std.testing.io) catch {};
    };
    try execution.park(.resource_blocked);
    var blocked = false;
    for (0..2000) |_| {
        if (fixture.producing.load(.acquire) >= 3 and owner.stats().resource_blocked == 2) {
            blocked = true;
            break;
        }
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expect(blocked);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    var health: Runnable.Execution = .{};
    try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
    try health.checkpoint();
    health.finish();
    try execution.enter(.query);
    const held = (try pipe.nextBatch(1)).?;
    defer held.deinit();
    try std.testing.expectEqual(@as(u64, 0), held.ordinal(0));
    pipe.stop();
    _ = task.cancel(std.testing.io) catch {};
    try std.testing.expectEqual(@as(usize, 0), execution.entry.?.children);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expect(pipe.producer_execution == null);
    pipe.close();
    open = false;
    try std.testing.expectEqualStrings(&@as([512]u8, @splat('x')), (try held.values().cell(a, 0, 0)).value.string);
}

test "SQL native output pipe accounts frozen bytes without scalar access" {
    const Fixture = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const sa = source.allocator();
            const root = try std.json.parseFromSliceLeaky(std.json.Value, sa, "{\"x\":[9007199254740993,1.25]}", .{});
            const wire = try spill.encodeColumnarBlockVersionAlloc(sa, &.{.{ .values = &.{@import("scalar.zig").Datum.json(root)}, .keys = &.{}, .ordinal = 0 }}, 1 << 20, .v2);
            const block = try spill.decodeColumnarBlockForForwarding(sa, a, wire, 1 << 20);
            defer block.deinit();
            const Reader = struct {
                value: @import("execution_batch.zig").FrozenJson,
                fn cell(_: *anyopaque, _: A, _: usize, _: usize) !@import("scalar.zig").Datum {
                    return error.UnexpectedScalarMaterialization;
                }
                fn frozen(raw: *anyopaque, _: usize, _: usize) !?@import("execution_batch.zig").FrozenJson {
                    return @as(*@This(), @ptrCast(@alignCast(raw))).value;
                }
            };
            var reader: Reader = .{ .value = (try block.frozenJson(0, 0, false)).? };
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check };
            defer manager.deinit();
            const pipe = try Pipe.create(&manager, 8192);
            var live = true;
            defer if (live) pipe.close();
            try pipe.appendBatch(.{ .reader = .{ .ptr = &reader, .read = Reader.cell, .read_frozen = Reader.frozen, .count = 1, .width = 1 } }, .{ .rows = &.{&.{}} }, &.{0});
            pipe.finish(null);
            const view = (try pipe.nextBatch(1)).?;
            defer view.deinit();
            pipe.close();
            live = false;
            try std.testing.expectEqualSlices(u8, reader.value.bytes, (try view.values().frozenJson(0, 0)).?.bytes);
            const replay = try spill.encodeColumnarBatchVersionAlloc(sa, view.values(), &.{0}, 1 << 20, .v2);
            try std.testing.expectEqualSlices(u8, wire, replay);
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "SQL native worker cohorts share oversized payloads and bound distinct rows" {
    const Fixture = struct {
        text: []const u8,
        distinct: ?[]const u8 = null,
        fn check(_: *anyopaque) !void {}
        fn cell(_: *anyopaque, _: A, _: usize, _: usize) !@import("scalar.zig").Datum {
            return error.UnexpectedScalarMaterialization;
        }
        fn json(raw: *anyopaque, row: usize, _: usize) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return if (row != 0 and self.distinct != null) self.distinct.? else self.text;
        }
        fn run(a: A, distinct: bool) !void {
            var source = std.heap.ArenaAllocator.init(a);
            defer source.deinit();
            const prefix = "{\"payload\":\"";
            const text = try source.allocator().alloc(u8, prefix.len + 12000 + 2);
            @memcpy(text[0..prefix.len], prefix);
            @memset(text[prefix.len..][0..12000], 'x');
            @memcpy(text[text.len - 2 ..], "\"}");
            var fixture: @This() = .{ .text = text };
            if (distinct) {
                const second = try source.allocator().dupe(u8, fixture.text);
                second[12] = 'y';
                fixture.distinct = second;
            }
            var dummy: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &dummy, .checkpoint = check };
            defer manager.deinit();
            const pipe = try Pipe.create(&manager, 8192);
            defer pipe.close();
            const values: Batch = .{ .reader = .{ .ptr = &fixture, .read = cell, .read_json = json, .count = 64, .width = 1 } };
            const key_rows: [64][]const @import("scalar.zig").Datum = @splat(&.{});
            const ordinals: [64]u64 = @splat(0);
            if (distinct) {
                pipe.pending = try Block.create(manager.allocator());
                pipe.pending.?.target = pipe.block_bytes;
                const consumed = try pipe.appendCohort(values, .{ .rows = &key_rows }, &ordinals, 0);
                try std.testing.expectEqual(@as(usize, 1), consumed);
                try std.testing.expectEqual(@as(usize, 1), pipe.pending.?.ordinals.items.len);
                try std.testing.expect(pipe.pending.?.bytes <= pipe.pending.?.target);
                pipe.finish(null);
                const view = (try pipe.nextBatch(64)).?;
                defer view.deinit();
                try std.testing.expectEqual(@as(usize, 1), view.count);
            } else {
                try pipe.appendBatch(values, .{ .rows = &key_rows }, &ordinals);
                pipe.finish(null);
                const view = (try pipe.nextBatch(64)).?;
                defer view.deinit();
                try std.testing.expectEqual(@as(usize, 64), view.count);
                try std.testing.expectEqual(fixture.text.len, pipe.json_hash_bytes);
                for (0..64) |row| try std.testing.expectEqualStrings(fixture.text, (try view.values().encodedJson(row, 0)).?);
                try std.testing.expect((try pipe.nextBatch(64)) == null);
                if (a.ptr == std.testing.allocator.ptr) std.debug.print("NATIVE_WORKER_COHORT rows=64 payload={d} deliveries=1 hash_input={d}\n", .{ fixture.text.len, pipe.json_hash_bytes });
            }
        }
    };
    for ([_]bool{ false, true }) |distinct| {
        try Fixture.run(std.testing.allocator, distinct);
        try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{distinct});
    }
}

test "SQL native output pipe interrupts a single large leased payload before publication" {
    const Fixture = struct {
        calls: usize = 0,
        text: []const u8,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == 12) return error.OriginalPipePayloadCanceled;
        }
        fn json(raw: *anyopaque, _: usize, _: usize) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.text;
        }
        fn cell(_: *anyopaque, _: A, _: usize, _: usize) !@import("scalar.zig").Datum {
            return error.UnexpectedScalarMaterialization;
        }
    };
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 256 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    text[0] = '"';
    text[text.len - 1] = '"';
    var fixture: Fixture = .{ .text = text };
    var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &fixture, .checkpoint = Fixture.check };
    defer manager.deinit();
    const pipe = try Pipe.create(&manager, 8192);
    defer pipe.close();
    const values: Batch = .{ .reader = .{ .ptr = &fixture, .read = Fixture.cell, .read_json = Fixture.json, .count = 1, .width = 1 } };
    try std.testing.expectError(error.OriginalPipePayloadCanceled, pipe.appendBatch(values, .{ .rows = &.{&.{}} }, &.{0}));
    try std.testing.expectEqual(@as(usize, 12), fixture.calls);
    try std.testing.expectEqual(@as(usize, 0), pipe.pending.?.ordinals.items.len);
    try std.testing.expectEqual(@as(usize, 0), pipe.pending.?.values.len);
}
