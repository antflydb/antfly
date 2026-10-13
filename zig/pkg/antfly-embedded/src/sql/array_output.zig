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

//! Shared PostgreSQL array output admission. JSONB keeps compact sizes/quoting
//! metadata and one exact maximum-path sorting buffer, never encoded payloads.
const std = @import("std");
const arrays = @import("array_value.zig");
const MemoryBudget = @import("memory_budget.zig");
const numeric = @import("numeric_value.zig");
const jsonb = @import("jsonb_text.zig");
const A = std.mem.Allocator;

pub fn needsQuotes(bytes: []const u8) bool {
    if (bytes.len == 0 or std.ascii.eqlIgnoreCase(bytes, "NULL")) return true;
    for (bytes) |byte| if (quoteByte(byte)) return true;
    return false;
}

fn quoteByte(byte: u8) bool {
    return byte == '"' or byte == '\\' or byte == '{' or byte == '}' or byte == ',' or std.ascii.isWhitespace(byte);
}

pub const Cell = struct {
    bytes: u32 = 0,
    escapes: u32 = 0,
    quoted: bool = false,
    pub fn textSize(self: Cell) usize {
        return @as(usize, self.bytes) + self.escapes + 2 * @as(usize, @intFromBool(self.quoted));
    }
};

const Probe = struct {
    cell: Cell,
    limit: usize,
    work: *arrays.Budget,
    failure: ?anyerror = null,
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },

    fn add(self: *Probe, bytes: []const u8, repeats: usize) !usize {
        const n = std.math.mul(usize, bytes.len, repeats) catch return error.SqlProgramLimitExceeded;
        if (n > self.limit -| self.cell.bytes) return error.SqlProgramLimitExceeded;
        if (n > std.math.maxInt(u32) - self.cell.bytes) return error.SqlProgramLimitExceeded;
        var offset: usize = 0;
        while (offset < bytes.len) {
            const end = offset + @min(bytes.len - offset, 256);
            try self.work.consume((end - offset) * repeats);
            for (bytes[offset..end]) |byte| {
                self.cell.quoted = self.cell.quoted or quoteByte(byte);
                if (byte == '"' or byte == '\\') self.cell.escapes += @intCast(repeats);
            }
            offset = end;
        }
        self.cell.bytes += @intCast(n);
        return n;
    }
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
        var total: usize = 0;
        for (data, 0..) |bytes, i| {
            total += self.add(bytes, if (i + 1 == data.len) splat else 1) catch |err| {
                self.failure = err;
                return error.WriteFailed;
            };
        }
        return total;
    }
};

fn sortingBytes(value: std.json.Value, work: *arrays.Budget, depth: usize) anyerror!usize {
    if (depth >= 64) return error.SqlProgramLimitExceeded;
    try work.consume(1);
    var own: usize = 0;
    var children: []const std.json.Value = &.{};
    switch (value) {
        .array => |array| children = array.items,
        .object => |object| {
            own = std.math.mul(usize, object.count(), @sizeOf(usize)) catch return error.SqlProgramLimitExceeded;
            children = object.values();
        },
        else => return 0,
    }
    var maximum: usize = 0;
    for (children) |child| maximum = @max(maximum, try sortingBytes(child, work, depth + 1));
    return std.math.add(usize, own, maximum) catch error.SqlProgramLimitExceeded;
}

/// Initialize at its final address. The memory allocator references this owner;
/// borrowed source DOM values remain immutable until output completes.
pub const Prepared = struct {
    memory: MemoryBudget,
    cells: []Cell = &.{},
    scratch: []align(@alignOf(usize)) u8 = &.{},
    format_cost: usize = 0,

    pub fn init(self: *Prepared, alloc: A, value: arrays.Value, bytes: usize, wire: usize, work: *arrays.Budget) !void {
        self.* = .{ .memory = .{ .backing = alloc, .limit = @min(bytes, MemoryBudget.headroom(alloc) orelse bytes) } };
        errdefer self.deinit();
        if (value.element_type != .jsonb or value.elements.len == 0) return;
        var sorting: usize = 0;
        var has_payload = false;
        for (value.elements) |element| {
            try work.consume(1);
            if (element.sql_null) continue;
            has_payload = true;
            sorting = @max(sorting, try sortingBytes(element.value, work, 0));
        }
        if (!has_payload) return;
        const a = self.memory.allocator();
        self.cells = try a.alloc(Cell, value.elements.len);
        self.scratch = try a.alignedAlloc(u8, .of(usize), sorting);
        const before = work.remaining;
        for (value.elements, self.cells) |element, *cell| {
            cell.* = .{};
            if (element.sql_null) continue;
            var probe: Probe = .{ .cell = .{ .quoted = element.value == .null }, .limit = wire, .work = work };
            self.writeJson(element.value, &probe.writer, wire, work) catch |err| return probe.failure orelse err;
            cell.* = probe.cell;
        }
        self.format_cost = before - work.remaining;
    }
    pub fn deinit(self: *Prepared) void {
        const a = self.memory.allocator();
        a.free(self.scratch);
        a.free(self.cells);
    }
    pub fn failure(self: *Prepared, err: anyerror) anyerror {
        return if (err == error.OutOfMemory and self.memory.isExhausted()) error.SqlProgramLimitExceeded else err;
    }
    pub fn writeJson(self: *Prepared, value: std.json.Value, writer: *std.Io.Writer, wire: usize, work: *arrays.Budget) !void {
        var fixed: std.heap.FixedBufferAllocator = .init(self.scratch);
        try jsonb.write(fixed.allocator(), value, writer, wire, work, 0);
    }
};

/// Work is charged once before publication. Emission spends that local credit
/// and forwards cancellation to the original identity, including parent chains.
pub const Emission = struct {
    owner: ?*numeric.Context,
    context: numeric.Context,
    work: arrays.Budget,

    pub fn init(self: *Emission, original: *arrays.Budget, amount: usize) !void {
        try original.consume(amount);
        self.* = .{
            .owner = original.shared,
            .context = .{ .alloc = A.failing, .remaining = amount, .checkpoint = poll, .ptr = self },
            .work = .{ .remaining = amount },
        };
        self.work.shared = &self.context;
        if (original.shared) |owner| {
            self.context.max_input_bytes = owner.max_input_bytes;
            self.context.max_output_bytes = owner.max_output_bytes;
            self.context.max_groups = owner.max_groups;
        }
    }
    fn poll(ptr: ?*anyopaque) anyerror!void {
        const self: *Emission = @ptrCast(@alignCast(ptr.?));
        if (self.owner) |owner| try owner.pollCheckpoint(256);
    }
};

/// Account output copying and poll between bounded chunks, not after a whole
/// multi-megabyte string has already reached the destination.
pub const Writer = struct {
    target: *std.Io.Writer,
    work: *arrays.Budget,
    failure: ?anyerror = null,
    buffer: [1024]u8 = undefined,
    writer: std.Io.Writer,

    pub fn init(self: *Writer, target: *std.Io.Writer, work: *arrays.Budget) void {
        self.* = .{ .target = target, .work = work, .writer = .{ .vtable = &.{ .drain = drain }, .buffer = &self.buffer } };
    }

    fn write(self: *Writer, bytes: []const u8, repeats: usize) std.Io.Writer.Error!usize {
        const count = std.math.mul(usize, bytes.len, repeats) catch return error.WriteFailed;
        if (bytes.len == 1) {
            var left = repeats;
            while (left != 0) {
                const n = @min(left, 256);
                self.work.consume(n) catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
                try self.target.splatByteAll(bytes[0], n);
                left -= n;
            }
        } else for (0..repeats) |_| {
            var offset: usize = 0;
            while (offset < bytes.len) {
                const n = @min(bytes.len - offset, 256);
                self.work.consume(n) catch |err| {
                    self.failure = err;
                    return error.WriteFailed;
                };
                try self.target.writeAll(bytes[offset..][0..n]);
                offset += n;
            }
        }
        return count;
    }
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
        _ = try self.write(w.buffered(), 1);
        w.end = 0;
        var count: usize = 0;
        for (data, 0..) |bytes, i| count += try self.write(bytes, if (i + 1 == data.len) splat else 1);
        return count;
    }
};

pub fn emissionCost(format: usize, wire: usize) !usize {
    return std.math.add(usize, format, std.math.mul(usize, wire, 2) catch return error.SqlProgramLimitExceeded) catch error.SqlProgramLimitExceeded;
}

test "SQL array wire native JSONB numeric identity survives physical storage" {
    const a = std.testing.allocator;
    for ([_]f64{ 0.1, -0.0, 1e30, @bitCast(@as(u64, 1)), std.math.floatMax(f64) }) |number| {
        const value = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{arrays.Element.json(.{ .float = number })}, .{});
        const stored = try @import("array_storage.zig").encodeAlloc(a, value, .{});
        defer a.free(stored);
        var cold = try @import("array_storage.zig").decode(a, .jsonb, stored, .{});
        defer cold.deinit();
        for ([_]bool{ false, true }) |binary| {
            var left: [4096]u8 = undefined;
            var right: [4096]u8 = undefined;
            var hot_wire = std.Io.Writer.fixed(&left);
            var cold_wire = std.Io.Writer.fixed(&right);
            if (binary) {
                try @import("array_binary.zig").encode(a, value, &hot_wire, .{});
                try @import("array_binary.zig").encode(a, cold.value, &cold_wire, .{});
                var roundtrip = try @import("array_binary.zig").decode(a, .jsonb, hot_wire.buffered(), .{});
                defer roundtrip.deinit();
                var work: arrays.Budget = .{};
                try std.testing.expectEqual(std.math.Order.eq, try value.compare(roundtrip.value, &work));
            } else {
                try @import("array_text.zig").encode(a, value, &hot_wire, .{});
                try @import("array_text.zig").encode(a, cold.value, &cold_wire, .{});
                var roundtrip = try @import("array_text.zig").decode(a, .jsonb, hot_wire.buffered(), .{});
                defer roundtrip.deinit();
                var work: arrays.Budget = .{};
                try std.testing.expectEqual(std.math.Order.eq, try value.compare(roundtrip.value, &work));
            }
            try std.testing.expectEqualSlices(u8, hot_wire.buffered(), cold_wire.buffered());
        }
    }
}

test "SQL array wire primitive output scales with zero preparation allocations" {
    const a = std.testing.allocator;
    const cells = try a.alloc(arrays.Element, 4096);
    defer a.free(cells);
    const buffer = try a.alloc(u8, 4096 * 8 + 64);
    defer a.free(buffer);
    for ([_]bool{ false, true }) |binary| {
        var previous: u64 = 0;
        for ([_]usize{ 1024, 4096 }) |count| {
            for (cells[0..count], 0..) |*cell, i| cell.* = arrays.Element.json(.{ .bool = i % 2 == 0 });
            const value = try arrays.Value.init(.boolean, &.{.{ .length = @intCast(count) }}, cells[0..count], .{});
            var writer = std.Io.Writer.fixed(buffer);
            var context: numeric.Context = .{ .alloc = A.failing };
            const started = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
            if (binary) try @import("array_binary.zig").encode(A.failing, value, &writer, .{ .context = &context }) else try @import("array_text.zig").encode(A.failing, value, &writer, .{ .context = &context });
            const elapsed = std.Io.Clock.now(.awake, std.testing.io).nanoseconds - started;
            const cost = 8 * 1024 * 1024 - context.remaining;
            if (previous != 0) try std.testing.expect(cost <= previous * 5);
            previous = cost;
            try std.testing.expectEqual(if (binary) 20 + count * 5 else 1 + count * 2, writer.end);
            std.debug.print("array output debug: binary={} cells={d} bytes={d} work={d} preparation_allocations=0 elapsed_ns={d}\n", .{ binary, count, writer.end, cost, elapsed });
        }
    }
}

test "SQL array wire binary input keeps shared quota cancellation and genuine OOM distinct" {
    const a = std.testing.allocator;
    const binary = @import("array_binary.zig");
    const payload: [1024]u8 = @splat('x');
    const value = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{arrays.Element.json(.{ .string = &payload })}, .{});
    var bytes: [4096]u8 = undefined;
    var output_writer = std.Io.Writer.fixed(&bytes);
    try binary.encode(a, value, &output_writer, .{});
    const source = output_writer.buffered();
    var limited: numeric.Context = .{ .alloc = a, .remaining = source.len - 1 };
    try std.testing.expectError(error.SqlProgramLimitExceeded, binary.decode(a, .jsonb, source, .{ .context = &limited }));
    try std.testing.expect(limited.failure.? == error.SqlProgramLimitExceeded);
    var genuine: numeric.Context = .{ .alloc = A.failing };
    try std.testing.expectError(error.OutOfMemory, binary.decode(A.failing, .jsonb, source, .{ .context = &genuine }));
    try std.testing.expect(genuine.failure == null);
    const Counter = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        fn poll(ptr: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.calls == self.stop) return error.QueryCanceled;
        }
    };
    var observed: Counter = .{};
    var parent: numeric.Context = .{ .alloc = a, .checkpoint = Counter.poll, .ptr = &observed };
    var decoded = try binary.decode(a, .jsonb, source, .{ .context = &parent });
    decoded.deinit();
    try std.testing.expect(observed.calls > 1);
    for (1..observed.calls + 1) |stop| {
        var counter: Counter = .{ .stop = stop };
        parent = .{ .alloc = a, .checkpoint = Counter.poll, .ptr = &counter };
        try std.testing.expectError(error.QueryCanceled, binary.decode(a, .jsonb, source, .{ .context = &parent }));
        try std.testing.expect(parent.failure.? == error.QueryCanceled);
        const before = parent.remaining;
        try std.testing.expectError(error.QueryCanceled, binary.decode(A.failing, .jsonb, source, .{ .context = &parent }));
        try std.testing.expectEqual(before, parent.remaining);
    }
}

test "SQL array wire preparation retains only metadata and exact maximum-path scratch" {
    const a = std.testing.allocator;
    const text = try a.alloc(u8, 1024 * 1024);
    defer a.free(text);
    @memset(text, 'x');
    const value = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{arrays.Element.json(.{ .string = text })}, .{ .work = 16 * 1024 * 1024 });
    var work: arrays.Budget = .{ .remaining = 16 * 1024 * 1024 };
    var prepared: Prepared = undefined;
    try prepared.init(a, value, 64, 2 * 1024 * 1024, &work);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(usize, 0), prepared.scratch.len);
    try std.testing.expectEqual(@as(usize, @sizeOf(Cell)), prepared.memory.peak);
    try std.testing.expectEqual(@as(usize, text.len + 6), prepared.cells[0].textSize());
    const source = "{\"outer\":{\"nested\":{\"b\":1.20,\"a\":2}},\"tail\":[{\"a\":1,\"b\":2,\"c\":3}]}";
    var parsed = try std.json.parseFromSlice(std.json.Value, a, source, .{ .parse_numbers = false });
    defer parsed.deinit();
    const nested = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{arrays.Element.json(parsed.value)}, .{});
    work = .{};
    var tree: Prepared = undefined;
    try tree.init(a, nested, 64, 1024, &work);
    defer tree.deinit();
    try std.testing.expectEqual(@as(usize, 5 * @sizeOf(usize)), tree.scratch.len);
    try std.testing.expectEqual(@as(usize, @sizeOf(Cell) + 5 * @sizeOf(usize)), tree.memory.peak);
    var buffer: [1024]u8 = undefined;
    var writer = std.Io.Writer.fixed(&buffer);
    try tree.writeJson(parsed.value, &writer, 1024, &work);
    const nulls = try arrays.Value.init(.jsonb, &.{.{ .length = 3 }}, &.{ .{}, .{}, .{} }, .{});
    work = .{};
    var empty: Prepared = undefined;
    try empty.init(A.failing, nulls, 0, 1024, &work);
    defer empty.deinit();
    try std.testing.expectEqual(@as(usize, 0), empty.memory.peak);
}

test "SQL array wire prepaid emission preserves parent quota and cancellation identity" {
    const Cancel = struct {
        polls: usize = 0,
        stop: usize = 3,
        fn poll(ptr: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.polls += 1;
            if (self.polls == self.stop) return error.QueryCanceled;
        }
    };
    var cancel: Cancel = .{};
    var parent: numeric.Context = .{ .alloc = A.failing, .remaining = 4096, .checkpoint = Cancel.poll, .ptr = &cancel };
    var child: numeric.Context = .{ .alloc = A.failing, .remaining = 4096, .parent = &parent, .max_groups = 16, .max_output_bytes = 32 };
    var original: arrays.Budget = .{ .remaining = 4096, .shared = &child };
    var emission: Emission = undefined;
    try emission.init(&original, 1024);
    try std.testing.expectEqual(@as(u64, 3072), parent.remaining);
    try std.testing.expectEqual(@as(u64, 3072), child.remaining);
    try std.testing.expectEqual(@as(usize, 3072), original.remaining);
    try std.testing.expectEqual(child.max_output_bytes, emission.context.max_output_bytes);
    try std.testing.expectEqual(child.max_groups, emission.context.max_groups);
    var buffer: [512]u8 = undefined;
    var target = std.Io.Writer.fixed(&buffer);
    var writer: Writer = undefined;
    writer.init(&target, &emission.work);
    const bytes: [512]u8 = @splat('x');
    try writer.writer.writeAll(&bytes);
    try std.testing.expectError(error.WriteFailed, writer.writer.flush());
    try std.testing.expect(writer.failure.? == error.QueryCanceled);
    try std.testing.expectEqual(@as(usize, 256), target.end);
    try std.testing.expect(parent.failure.? == error.QueryCanceled and child.failure.? == error.QueryCanceled);
    try std.testing.expectEqual(@as(u64, 3072), parent.remaining);
    try std.testing.expectEqual(@as(u64, 3072), child.remaining);
    try std.testing.expectEqual(@as(usize, 3072), original.remaining);
}

test "SQL array wire output admits faults and quotas before publication" {
    const a = std.testing.allocator;
    var source = try @import("array_text.zig").decode(a, .jsonb, "[-2:0]={\"{\\\"long\\\":1.20,\\\"a\\\":[0.00,2]}\",\"null\",NULL}", .{});
    defer source.deinit();
    const Faults = struct {
        fn run(alloc: A, value: arrays.Value, binary: bool) !void {
            var buffer: [1024]u8 = undefined;
            var writer = std.Io.Writer.fixed(&buffer);
            var context: numeric.Context = .{ .alloc = alloc };
            const result = if (binary)
                @import("array_binary.zig").encode(alloc, value, &writer, .{ .context = &context })
            else
                @import("array_text.zig").encode(alloc, value, &writer, .{ .context = &context });
            result catch |err| {
                try std.testing.expectEqual(@as(usize, 0), writer.end);
                try std.testing.expect(context.failure == null);
                return err;
            };
            try std.testing.expect(writer.end > 0);
        }
    };
    for ([_]bool{ false, true }) |binary| {
        try std.testing.checkAllAllocationFailures(a, Faults.run, .{ source.value, binary });
        var measured: numeric.Context = .{ .alloc = a };
        var sink: std.Io.Writer.Discarding = .init(&.{});
        if (binary) try @import("array_binary.zig").encode(a, source.value, &sink.writer, .{ .context = &measured }) else try @import("array_text.zig").encode(a, source.value, &sink.writer, .{ .context = &measured });
        const cost = 8 * 1024 * 1024 - measured.remaining;
        var limited: numeric.Context = .{ .alloc = a, .remaining = cost - 1 };
        var buffer: [1024]u8 = undefined;
        var writer = std.Io.Writer.fixed(&buffer);
        if (binary) try std.testing.expectError(error.SqlProgramLimitExceeded, @import("array_binary.zig").encode(a, source.value, &writer, .{ .context = &limited })) else try std.testing.expectError(error.SqlProgramLimitExceeded, @import("array_text.zig").encode(a, source.value, &writer, .{ .context = &limited }));
        try std.testing.expectEqual(@as(usize, 0), writer.end);
        try std.testing.expect(limited.failure.? == error.SqlProgramLimitExceeded);
        var memory: MemoryBudget = .{ .backing = a, .limit = 1 };
        limited = .{ .alloc = a };
        if (binary) try std.testing.expectError(error.SqlProgramLimitExceeded, @import("array_binary.zig").encode(memory.allocator(), source.value, &writer, .{ .context = &limited })) else try std.testing.expectError(error.SqlProgramLimitExceeded, @import("array_text.zig").encode(memory.allocator(), source.value, &writer, .{ .context = &limited }));
        try std.testing.expectEqual(@as(usize, 0), writer.end);
        try std.testing.expect(limited.failure.? == error.SqlProgramLimitExceeded);
        try std.testing.expectEqual(@as(usize, 0), memory.live);
    }
}

test "SQL array wire output cancels every phase without resetting shared work" {
    const a = std.testing.allocator;
    const payload: [1024]u8 = @splat('x');
    const value = try arrays.Value.init(.jsonb, &.{.{ .length = 1 }}, &.{arrays.Element.json(.{ .string = &payload })}, .{});
    const Counter = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        fn poll(ptr: ?*anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.calls += 1;
            if (self.calls == self.stop) return error.QueryCanceled;
        }
    };
    for ([_]bool{ false, true }) |binary| {
        var observed: Counter = .{};
        var context: numeric.Context = .{ .alloc = a, .checkpoint = Counter.poll, .ptr = &observed };
        var sink: std.Io.Writer.Discarding = .init(&.{});
        if (binary) try @import("array_binary.zig").encode(a, value, &sink.writer, .{ .context = &context }) else try @import("array_text.zig").encode(a, value, &sink.writer, .{ .context = &context });
        try std.testing.expect(observed.calls > 4);
        for (1..observed.calls + 1) |stop| {
            var counter: Counter = .{ .stop = stop };
            context = .{ .alloc = a, .checkpoint = Counter.poll, .ptr = &counter };
            var buffer: [4096]u8 = undefined;
            var writer = std.Io.Writer.fixed(&buffer);
            if (binary) try std.testing.expectError(error.QueryCanceled, @import("array_binary.zig").encode(a, value, &writer, .{ .context = &context })) else try std.testing.expectError(error.QueryCanceled, @import("array_text.zig").encode(a, value, &writer, .{ .context = &context }));
            try std.testing.expect(context.failure.? == error.QueryCanceled);
            if (stop == 1) try std.testing.expectEqual(@as(u64, 8 * 1024 * 1024), context.remaining) else try std.testing.expect(context.remaining < 8 * 1024 * 1024);
            const remaining = context.remaining;
            if (binary) try std.testing.expectError(error.QueryCanceled, @import("array_binary.zig").encode(A.failing, value, &writer, .{ .context = &context })) else try std.testing.expectError(error.QueryCanceled, @import("array_text.zig").encode(A.failing, value, &writer, .{ .context = &context }));
            try std.testing.expectEqual(remaining, context.remaining);
        }
    }
}
