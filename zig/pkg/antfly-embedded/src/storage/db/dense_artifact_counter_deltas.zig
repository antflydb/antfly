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

//! Owned ordered dense-artifact coverage adjustments. Counter state belongs
//! to the final writer, not to the preparation read set. This is transient
//! native state; the existing count and repair-bootstrap encodings are retained.
const std = @import("std");
const admission = @import("../../common/read_admission.zig");
const A = std.mem.Allocator;
pub const count_prefix = "\x00\x00__metadata__:dense_artifact_target_count:";
pub const bootstrap_prefix = "\x00\x00__metadata__:dense_artifact_counter_bootstrap:";
pub const bootstrap_magic = "AFDCB001";
pub const bootstrap_bytes = bootstrap_magic.len + 2 * @sizeOf(u128) + @sizeOf(i64);

pub const Bootstrap = struct {
    repair_id: u128,
    attempt_id: u128,
    delta: i64 = 0,
    pub fn decode(raw: []const u8) !Bootstrap {
        if (raw.len != bootstrap_bytes or !std.mem.eql(u8, raw[0..bootstrap_magic.len], bootstrap_magic)) return error.InvalidDenseArtifactCounterBootstrap;
        return .{ .repair_id = std.mem.readInt(u128, raw[8..24], .little), .attempt_id = std.mem.readInt(u128, raw[24..40], .little), .delta = std.mem.readInt(i64, raw[40..48], .little) };
    }
    pub fn encode(self: Bootstrap) [bootstrap_bytes]u8 {
        var raw: [bootstrap_bytes]u8 = undefined;
        @memcpy(raw[0..8], bootstrap_magic);
        std.mem.writeInt(u128, raw[8..24], self.repair_id, .little);
        std.mem.writeInt(u128, raw[24..40], self.attempt_id, .little);
        std.mem.writeInt(i64, raw[40..48], self.delta, .little);
        return raw;
    }
};
/// Composition of ordered saturating additions. The interval is the image of
/// [0,maxInt(u64)]; signed prefix extrema preserve bootstrap overflow at every
/// intermediate step. Storage and final-writer work are constant per target.
pub const Adjustment = struct {
    bias: i128 = 0,
    minimum_prefix: i128 = 0,
    maximum_prefix: i128 = 0,
    lower: u64 = 0,
    upper: u64 = std.math.maxInt(u64),
    fn append(self: *Adjustment, step: i64) !void {
        if (step == std.math.minInt(i64)) return error.DenseArtifactCounterBootstrapOverflow;
        var next = self.*;
        next.bias = std.math.add(i128, next.bias, step) catch return error.DenseArtifactCounterBootstrapOverflow;
        next.minimum_prefix = @min(next.minimum_prefix, next.bias);
        next.maximum_prefix = @max(next.maximum_prefix, next.bias);
        next.lower = shift(next.lower, step);
        next.upper = shift(next.upper, step);
        self.* = next;
    }
    fn shift(value: u64, step: i64) u64 {
        return if (step >= 0) value +| @as(u64, @intCast(step)) else value -| @as(u64, @intCast(-step));
    }
    pub fn count(self: Adjustment, value: u64) u64 {
        const shifted = std.math.add(i128, value, self.bias) catch return if (self.bias > 0) self.upper else self.lower;
        return @intCast(std.math.clamp(shifted, @as(i128, self.lower), @as(i128, self.upper)));
    }
    pub fn bootstrapDelta(self: Adjustment, value: i64) !i64 {
        const low = std.math.add(i128, value, self.minimum_prefix) catch return error.DenseArtifactCounterBootstrapOverflow;
        const high = std.math.add(i128, value, self.maximum_prefix) catch return error.DenseArtifactCounterBootstrapOverflow;
        if (low < std.math.minInt(i64) or high > std.math.maxInt(i64)) return error.DenseArtifactCounterBootstrapOverflow;
        return @intCast(@as(i128, value) + self.bias);
    }
};
pub const Entry = struct {
    name: []u8,
    counter: []u8,
    bootstrap: []u8,
    adjustment: Adjustment = .{},
    fn deinit(self: *Entry, a: A) void {
        a.free(self.name);
        a.free(self.counter);
        a.free(self.bootstrap);
    }
};
pub const Program = struct {
    allocator: A,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    by_name: std.StringHashMapUnmanaged(usize) = .empty,
    pub fn init(a: A) Program {
        return .{ .allocator = a };
    }
    pub fn deinit(self: *Program) void {
        for (self.entries.items) |*entry| entry.deinit(self.allocator);
        self.entries.deinit(self.allocator);
        self.by_name.deinit(self.allocator);
        self.* = undefined;
    }
    pub fn append(self: *Program, name: []const u8, step: i64, original: ?admission.Port) !void {
        if (original) |owner| try owner.checkpoint();
        if (step == 0) return;
        if (step == std.math.minInt(i64)) return error.DenseArtifactCounterBootstrapOverflow;
        if (self.by_name.get(name)) |index| {
            try self.entries.items[index].adjustment.append(step);
            return;
        }
        const a = self.allocator;
        var entry: Entry = .{ .name = try copy(a, "", name, original), .counter = undefined, .bootstrap = undefined };
        errdefer a.free(entry.name);
        entry.counter = try copy(a, count_prefix, name, original);
        errdefer a.free(entry.counter);
        entry.bootstrap = try copy(a, bootstrap_prefix, name, original);
        errdefer a.free(entry.bootstrap);
        try entry.adjustment.append(step);
        try self.entries.ensureUnusedCapacity(a, 1);
        try self.by_name.put(a, entry.name, self.entries.items.len);
        self.entries.appendAssumeCapacity(entry);
    }
    /// Caller owns this uncommitted writer and aborts on any error. No source
    /// authority or participant callback is needed, including control-only and
    /// inactive-authority batches. The program can be retried after an abort.
    pub fn apply(self: *const Program, writer: anytype, original: ?admission.Port) !void {
        var trace: admission.RejectionTrace = .{};
        self.applyInner(writer, trace.bind(original)) catch |cause| return trace.failure orelse cause;
        if (trace.failure) |cause| return cause;
    }
    fn applyInner(self: *const Program, writer: anytype, original: ?admission.Port) !void {
        for (self.entries.items) |entry| {
            if (original) |owner| try owner.checkpoint();
            const current = writer.get(entry.counter) catch |cause| if (cause == error.NotFound) null else return cause;
            if (current) |raw| {
                if (raw.len != 8) return error.InvalidDenseArtifactTargetCounter;
                const count = entry.adjustment.count(std.mem.readInt(u64, raw[0..8], .little));
                var encoded: [8]u8 = undefined;
                std.mem.writeInt(u64, &encoded, count, .little);
                try writer.put(entry.counter, &encoded);
            } else {
                const raw = writer.get(entry.bootstrap) catch |cause| if (cause == error.NotFound) continue else return cause;
                var state = try Bootstrap.decode(raw);
                state.delta = try entry.adjustment.bootstrapDelta(state.delta);
                try writer.put(entry.bootstrap, &state.encode());
            }
        }
        if (original) |owner| try owner.checkpoint();
    }
};
fn copy(a: A, prefix: []const u8, name: []const u8, original: ?admission.Port) ![]u8 {
    const len = std.math.add(usize, prefix.len, name.len) catch return error.TransactionTooLarge;
    const raw = try a.alloc(u8, len);
    errdefer a.free(raw);
    @memcpy(raw[0..prefix.len], prefix);
    var offset: usize = 0;
    while (offset < name.len) {
        if (original) |owner| try owner.checkpoint();
        const end = offset + @min(name.len - offset, 4096);
        @memcpy(raw[prefix.len + offset .. prefix.len + end], name[offset..end]);
        offset = end;
    }
    return raw;
}

const TestWriter = struct {
    values: std.StringHashMap([]u8),
    fn init(a: A) TestWriter {
        return .{ .values = .init(a) };
    }
    fn deinit(self: *TestWriter) void {
        var values = self.values.valueIterator();
        while (values.next()) |value| self.values.allocator.free(value.*);
        self.values.deinit();
    }
    pub fn get(self: *TestWriter, key: []const u8) ![]const u8 {
        return self.values.get(key) orelse error.NotFound;
    }
    pub fn put(self: *TestWriter, key: []const u8, raw: []const u8) !void {
        const copy_value = try self.values.allocator.dupe(u8, raw);
        errdefer self.values.allocator.free(copy_value);
        const entry = try self.values.getOrPut(key);
        if (entry.found_existing) self.values.allocator.free(entry.value_ptr.*);
        entry.value_ptr.* = copy_value;
    }
    fn number(self: *TestWriter, key: []const u8, value: u64) !void {
        var raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &raw, value, .little);
        try self.put(key, &raw);
    }
    fn readNumber(self: *TestWriter, key: []const u8) !u64 {
        return std.mem.readInt(u64, (try self.get(key))[0..8], .little);
    }
};

test "dense artifact counter deltas fold the current writer view and preserve ordered saturation and bootstrap identity" {
    const a = std.testing.allocator;
    var program = Program.init(a);
    defer program.deinit();
    try program.append("vector", -1, null);
    try program.append("vector", 1, null);
    const entry = program.entries.items[0];
    var writer = TestWriter.init(a);
    defer writer.deinit();
    // An unavailable counter stays unavailable; preparation never invents one.
    try program.apply(&writer, null);
    try std.testing.expectError(error.NotFound, writer.get(entry.counter));
    try writer.number(entry.counter, 0);
    try program.apply(&writer, null);
    try std.testing.expectEqual(@as(u64, 1), try writer.readNumber(entry.counter));
    try writer.number(entry.counter, std.math.maxInt(u64));
    try program.apply(&writer, null);
    try std.testing.expectEqual(std.math.maxInt(u64), try writer.readNumber(entry.counter));
    // Repair can complete between preparation and commit. A newly installed
    // count wins over the older bootstrap record without rebinding the plan.
    var insert = Program.init(a);
    defer insert.deinit();
    try insert.append("repair", 1, null);
    const repair = insert.entries.items[0];
    const initial: Bootstrap = .{ .repair_id = 17, .attempt_id = 23, .delta = -3 };
    try writer.put(repair.bootstrap, &initial.encode());
    try insert.apply(&writer, null);
    var expected = initial;
    expected.delta += 1;
    try std.testing.expectEqualDeep(expected, try Bootstrap.decode(try writer.get(repair.bootstrap)));
    try writer.number(repair.counter, 41);
    try insert.apply(&writer, null);
    try std.testing.expectEqual(@as(u64, 42), try writer.readNumber(repair.counter));
    try std.testing.expectEqualDeep(expected, try Bootstrap.decode(try writer.get(repair.bootstrap)));
}

test "dense artifact counter deltas reject malformed counters bootstrap state and signed overflow" {
    const a = std.testing.allocator;
    var program = Program.init(a);
    defer program.deinit();
    try program.append("vector", 1, null);
    const entry = program.entries.items[0];
    var writer = TestWriter.init(a);
    defer writer.deinit();
    try writer.put(entry.bootstrap, "invalid");
    try std.testing.expectError(error.InvalidDenseArtifactCounterBootstrap, program.apply(&writer, null));
    const full: Bootstrap = .{ .repair_id = 17, .attempt_id = 23, .delta = std.math.maxInt(i64) };
    try writer.put(entry.bootstrap, &full.encode());
    try std.testing.expectError(error.DenseArtifactCounterBootstrapOverflow, program.apply(&writer, null));
    try std.testing.expectEqualDeep(full, try Bootstrap.decode(try writer.get(entry.bootstrap)));
    try writer.put(entry.counter, "bad");
    try std.testing.expectError(error.InvalidDenseArtifactTargetCounter, program.apply(&writer, null));
    try std.testing.expectError(error.DenseArtifactCounterBootstrapOverflow, program.append("invalid", std.math.minInt(i64), null));
}

test "dense artifact counter deltas own every allocation and preserve original checkpoint errors" {
    const Checks = struct {
        calls: usize = 0,
        stop: usize = std.math.maxInt(usize),
        cause: anyerror = error.DeadlineExceeded,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls == self.stop) return self.cause;
        }
        fn run(a: A, checks: ?*@This()) !void {
            var source: admission.Payload = .{ .ptr = checks orelse undefined, .vtable = &.{ .checkpoint = check } };
            const original: ?admission.Port = if (checks != null) .init(&source) else null;
            var program = Program.init(a);
            defer program.deinit();
            try program.append("counter", -1, original);
            try program.append("counter", 1, original);
            try program.append("bootstrap", -1, original);
            try program.append("missing", 1, original);
            var writer = TestWriter.init(std.testing.allocator);
            defer writer.deinit();
            try writer.number(program.entries.items[0].counter, 7);
            try writer.put(program.entries.items[1].bootstrap, &(Bootstrap{ .repair_id = 1, .attempt_id = 2 }).encode());
            try program.apply(&writer, original);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Checks.run, .{@as(?*Checks, null)});
    var baseline: Checks = .{};
    try Checks.run(std.testing.allocator, &baseline);
    for ([_]anyerror{ error.NotFound, error.OutOfMemory, error.DeadlineExceeded }) |cause| for (1..baseline.calls + 1) |stop| {
        var checks: Checks = .{ .stop = stop, .cause = cause };
        try std.testing.expectError(cause, Checks.run(std.testing.allocator, &checks));
    };
}

test "dense artifact counter deltas compact composition matches ordered reference arithmetic at both integer boundaries" {
    for (0..256) |mask| {
        var adjustment: Adjustment = .{};
        var steps: [8]i64 = undefined;
        for (&steps, 0..) |*step, bit| {
            step.* = if (mask & (@as(usize, 1) << @intCast(bit)) == 0) -3 else 5;
            try adjustment.append(step.*);
        }
        for ([_]u64{ 0, 1, 17, std.math.maxInt(u64) / 2, std.math.maxInt(u64) - 1, std.math.maxInt(u64) }) |initial| {
            var reference = initial;
            for (steps) |step| reference = Adjustment.shift(reference, step);
            try std.testing.expectEqual(reference, adjustment.count(initial));
        }
        for ([_]i64{ std.math.minInt(i64), std.math.minInt(i64) + 1, 0, std.math.maxInt(i64) - 1, std.math.maxInt(i64) }) |initial| {
            var reference = initial;
            var overflow = false;
            for (steps) |step| {
                reference = std.math.add(i64, reference, step) catch {
                    overflow = true;
                    break;
                };
            }
            if (overflow) try std.testing.expectError(error.DenseArtifactCounterBootstrapOverflow, adjustment.bootstrapDelta(initial)) else try std.testing.expectEqual(reference, try adjustment.bootstrapDelta(initial));
        }
    }
    // A net-zero adjustment can still overflow an intermediate bootstrap step.
    var zero: Adjustment = .{};
    try zero.append(1);
    try zero.append(-1);
    try std.testing.expectEqual(@as(i128, 0), zero.bias);
    try std.testing.expectError(error.DenseArtifactCounterBootstrapOverflow, zero.bootstrapDelta(std.math.maxInt(i64)));
}
