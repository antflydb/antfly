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

//! Publication consumers borrow immutable spill blocks until the next record
//! or bounded window. Builders/encoders finish consuming bytes before advance;
//! no transient row or payload clone sits between sort and publication.
const std = @import("std");
const local = @import("antfly_local_sources");
const spill = local.sql_spill;
const A = std.mem.Allocator;
const RowLease = spill.Sort.RowLease;

pub const Records = struct {
    sort: *spill.Sort,
    held: ?RowLease = null,
    pub fn deinit(self: *Records) void {
        if (self.held) |lease| lease.release();
        self.held = null;
    }
    pub const Record = struct { key: []const u8, value: []const u8 };
    pub fn next(self: *Records) !?Record {
        self.deinit();
        const lease = (try self.sort.nextLeased()) orelse return null;
        self.held = lease;
        if (lease.keyWidth() != 1 or lease.width() != 1) return error.InvalidSqlSpill;
        const key = try lease.keyCell(0);
        const value = try lease.cell(0);
        if (key.sql_null or value.sql_null or key.value != .string or value.value != .string) return error.InvalidSqlSpill;
        return .{ .key = key.value.string, .value = value.value.string };
    }
};

pub const Window = struct {
    sort: *spill.Sort,
    leases: std.ArrayListUnmanaged(RowLease) = .empty,
    count: usize = 0,
    width: usize = 0,
    key_width: usize = 0,
    pub fn clear(self: *Window) void {
        for (self.leases.items) |lease| lease.release();
        self.leases.clearRetainingCapacity();
        self.count = 0;
    }
    pub fn deinit(self: *Window) void {
        self.clear();
        self.leases.deinit(local.sql_memory_budget.reclaimingAllocator(self.sort.a));
    }
    /// Keep the same row/raw-byte quantum as the owning publisher, including
    /// one indivisible oversized record. The sort owns physical block admission.
    pub fn read(self: *Window, maximum: usize, bytes: usize) !usize {
        if (maximum == 0 or maximum > 256 or bytes == 0) return error.InvalidSqlLimit;
        self.clear();
        errdefer self.clear();
        var retained: usize = 0;
        while (self.count < maximum and retained < bytes) {
            const lease = (try self.sort.nextLeased()) orelse break;
            self.leases.append(local.sql_memory_budget.reclaimingAllocator(self.sort.a), lease) catch |err| {
                lease.release();
                return err;
            };
            self.count = self.leases.items.len;
            if (self.count == 1) {
                self.width = lease.width();
                self.key_width = lease.keyWidth();
            } else if (lease.width() != self.width or lease.keyWidth() != self.key_width) return error.InvalidSqlSpill;
            for (0..self.width) |i| retained +|= try lease.cellBytes(i);
            for (0..self.key_width) |i| retained +|= try local.sql_operators.datumBytes(try lease.keyCell(i));
        }
        return self.count;
    }
    /// Only vector/scatter descriptors are allocated. All physical/frozen
    /// payloads remain pinned by this window, including dictionaries and JSON.
    pub fn values(self: *Window, a: A) !local.sql_execution_batch.Batch {
        const view = try a.create(spill.Sort.LeaseView);
        view.* = try .init(a, self.leases.items, self.width);
        return view.batch();
    }
    pub fn ordinals(self: *const Window, a: A) ![]const u64 {
        const result = try a.alloc(u64, self.count);
        for (result, self.leases.items) |*out, lease| out.* = lease.row.ordinal;
        return result;
    }
};

test "external lake leased publication records preserve resident and spilled bytes and release allocation failures" {
    const Probe = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A, memory: usize) !void {
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var sort = spill.Sort.init(a, &manager, &.{.{}}, memory);
            defer sort.deinit();
            sort.parallel_runs = false;
            for (0..8) |i| {
                var key: [32]u8 = undefined;
                const encoded = try std.fmt.bufPrint(&key, "key-{d:0>16}", .{7 - i});
                try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = encoded })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = "borrowed payload" })}, .ordinal = 7 - i });
            }
            var records: Records = .{ .sort = &sort };
            defer records.deinit();
            var seen: usize = 0;
            while (try records.next()) |record| : (seen += 1) {
                var key: [32]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&key, "key-{d:0>16}", .{seen}), record.key);
                try std.testing.expectEqualStrings("borrowed payload", record.value);
            }
            try std.testing.expectEqual(@as(usize, 8), seen);
        }
    };
    try Probe.run(std.testing.allocator, 512);
    try Probe.run(std.testing.allocator, 1 << 20);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{@as(usize, 1 << 20)});
}

test "external lake leased publication windows bound quanta preserve EOF and unwind allocation faults" {
    const Probe = struct {
        fn check(_: *anyopaque) !void {}
        fn run(a: A) !void {
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .async_writes = false };
            defer manager.deinit();
            var sort = spill.Sort.init(a, &manager, &.{.{}}, 1 << 20);
            defer sort.deinit();
            for (0..4) |i| try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .integer = @intCast(i) })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = "payload" })}, .ordinal = i });
            var window: Window = .{ .sort = &sort };
            defer window.deinit();
            try std.testing.expectError(error.InvalidSqlLimit, window.read(0, 1));
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            var seen: usize = 0;
            while (try window.read(2, 1) != 0) {
                // A single indivisible row can exceed the raw-byte quantum.
                try std.testing.expectEqual(@as(usize, 1), window.count);
                const batch = try window.values(scratch.allocator());
                try std.testing.expectEqualStrings("payload", (try batch.cell(scratch.allocator(), 0, 0)).value.string);
                try std.testing.expectEqual(@as(u64, seen), window.leases.items[0].row.ordinal);
                seen += 1;
                window.clear();
                _ = scratch.reset(.free_all);
            }
            try std.testing.expectEqual(@as(usize, 4), seen);
            try std.testing.expectEqual(@as(usize, 0), try window.read(2, 1));
        }
    };
    try Probe.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "external lake leased publication benchmark sorted record drain" {
    if (std.c.getenv("ANTFLY_LAKE_LEASED_PUBLICATION_BENCH") == null) return;
    const Probe = struct {
        const Measurement = struct { ns: i96, allocated: usize, reads: u64, written: u64 };
        fn check(_: *anyopaque) !void {}
        fn now() i96 {
            var stamp: std.c.timespec = undefined;
            if (std.c.clock_gettime(.THREAD_CPUTIME_ID, &stamp) != 0) @panic("CPU clock unavailable");
            return @as(i96, stamp.sec) * std.time.ns_per_s + stamp.nsec;
        }
        fn run(leased: bool, memory: usize) !Measurement {
            var tracking = std.testing.FailingAllocator.init(std.heap.smp_allocator, .{});
            const a = tracking.allocator();
            var marker: u8 = 0;
            var manager: spill.Manager = .{ .alloc = a, .io = std.testing.io, .context = &marker, .checkpoint = check, .compression = .none, .async_writes = false };
            defer manager.deinit();
            var sort = spill.Sort.init(a, &manager, &.{.{}}, memory);
            defer sort.deinit();
            sort.parallel_runs = false;
            var payload: [2048]u8 = @splat('p');
            for (0..1024) |i| {
                var key: [32]u8 = undefined;
                const encoded = try std.fmt.bufPrint(&key, "key-{d:0>16}", .{1023 - i});
                std.mem.writeInt(u64, payload[0..8], @intCast(1023 - i), .little);
                try sort.add(.{ .keys = &.{local.sql_scalar.Datum.json(.{ .string = encoded })}, .values = &.{local.sql_scalar.Datum.json(.{ .string = &payload })}, .ordinal = 1023 - i });
            }
            var records: Records = .{ .sort = &sort };
            defer records.deinit();
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            const allocated = tracking.allocated_bytes;
            const start = now();
            var seen: usize = 0;
            while (true) {
                _ = scratch.reset(.free_all);
                const record: Records.Record = if (leased) (try records.next()) orelse break else blk: {
                    const row = (try sort.next(scratch.allocator())) orelse break;
                    break :blk .{ .key = row.keys[0].value.string, .value = row.values[0].value.string };
                };
                var key: [32]u8 = undefined;
                try std.testing.expectEqualStrings(try std.fmt.bufPrint(&key, "key-{d:0>16}", .{seen}), record.key);
                std.mem.writeInt(u64, payload[0..8], @intCast(seen), .little);
                try std.testing.expectEqualSlices(u8, &payload, record.value);
                seen += 1;
            }
            const ns = now() - start;
            try std.testing.expectEqual(@as(usize, 1024), seen);
            return .{ .ns = ns, .allocated = tracking.allocated_bytes - allocated, .reads = manager.read_bytes, .written = manager.written_bytes };
        }
    };
    for ([_]usize{ 8 << 20, 64 << 10 }) |memory| {
        _ = try Probe.run(false, memory);
        _ = try Probe.run(true, memory);
        for (0..6) |sample| {
            const first = try Probe.run(sample % 2 != 0, memory);
            const second = try Probe.run(sample % 2 == 0, memory);
            const before = if (sample % 2 == 0) first else second;
            const after = if (sample % 2 == 0) second else first;
            std.debug.print("LEASED_PUBLICATION sample={d} memory={d} before_cpu_ns={d} after_cpu_ns={d} before_allocated={d} after_allocated={d} before_read={d} after_read={d} before_write={d} after_write={d}\n", .{ sample, memory, before.ns, after.ns, before.allocated, after.allocated, before.reads, after.reads, before.written, after.written });
        }
    }
}
