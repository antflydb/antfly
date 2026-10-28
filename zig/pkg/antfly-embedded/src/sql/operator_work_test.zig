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

const std = @import("std");
const Work = @import("operator_work.zig");
const scalar = @import("scalar.zig");
const Datum = scalar.Datum;
const Probe = struct {
    calls: usize = 0,
    stop_after: usize = std.math.maxInt(usize),
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.calls += 1;
        if (self.calls >= self.stop_after) return error.OwnerCanceled;
    }
    fn control(self: *@This()) Work.Control {
        return .{ .ptr = self, .checkpoint = check };
    }
};

test "SQL grouping work byte boundaries preserve hashes ordering and original failures" {
    const text: [65536]u8 = @splat('q');
    for ([_]usize{ 0, 1, 47, 48, 49, 255, 256, 257, 4095, 4096, 4097, text.len }) |length| {
        var probe: Probe = .{};
        try std.testing.expectEqual(std.hash.Wyhash.hash(17, text[0..length]), try Work.hashBytes(probe.control(), 17, text[0..length]));
        try std.testing.expectEqual(std.math.Order.eq, try Work.orderBytes(probe.control(), text[0..length], text[0..length]));
        try std.testing.expect(probe.calls > 0);
    }
    var probe: Probe = .{ .stop_after = 3 };
    try std.testing.expectError(error.OwnerCanceled, Work.hashBytes(probe.control(), 0, &text));
    probe = .{ .stop_after = 4 };
    try std.testing.expectError(error.OwnerCanceled, Work.equalBytes(probe.control(), &text, &text));
}

test "SQL grouping work typed hashes and comparisons preserve null numeric array and JSON domains" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    var numeric_context: @import("numeric_value.zig").Context = .{ .alloc = alloc };
    var number = try @import("numeric_value.zig").parse(&numeric_context, "123456789012345678901234567890.00100");
    defer number.deinit();
    const text: [16384]u8 = @splat('x');
    const array = try @import("array_value.zig").Value.init(.text, &.{.{ .length = 3, .lower = -2 }}, &.{ Datum.json(.{ .string = &text }), .{}, Datum.json(.{ .string = "tail" }) }, .{});
    var object: std.json.ObjectMap = .empty;
    try object.put(alloc, &text, .{ .integer = 1 });
    const values = [_]Datum{
        .{},                               Datum.json(.null),                           Datum.json(.{ .integer = 9007199254740993 }),
        Datum.json(.{ .float = 1.5 }),     Datum.json(.{ .float = std.math.nan(f64) }), Datum.json(.{ .float = std.math.inf(f64) }),
        Datum.json(.{ .string = &text }),  Datum.typedNumeric(&number.value),           Datum.typedArray(&array),
        Datum.json(.{ .object = object }),
    };
    for (values) |value| {
        var probe: Probe = .{};
        try std.testing.expectEqual(try scalar.semanticHashDatum(value), try scalar.semanticHashDatumControlled(value, probe.control()));
        try std.testing.expectEqual(try scalar.compareDatums(value, value), try scalar.compareDatumsControlled(value, value, probe.control()));
        try std.testing.expect(probe.calls > 0);
        probe = .{ .stop_after = 1 };
        try std.testing.expectError(error.OwnerCanceled, scalar.semanticHashDatumControlled(value, probe.control()));
        probe = .{ .stop_after = 1 };
        try std.testing.expectError(error.OwnerCanceled, scalar.compareDatumsControlled(value, value, probe.control()));
    }
}

test "SQL grouping work JSON object lookups poll wide keys without changing charged work" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const left_key: [16384]u8 = @splat('k');
    const right_key = try arena.allocator().dupe(u8, &left_key);
    var left: std.json.ObjectMap = .empty;
    var right: std.json.ObjectMap = .empty;
    try left.put(arena.allocator(), &left_key, .{ .integer = 1 });
    try right.put(arena.allocator(), right_key, .{ .integer = 1 });
    const json = @import("json_order.zig");
    var legacy: json.Budget = .{};
    try std.testing.expectEqual(std.math.Order.eq, try json.compare(.{ .object = left }, .{ .object = right }, &legacy, 0));
    var probe: Probe = .{};
    var parent: @import("numeric_value.zig").Context = .{ .alloc = arena.allocator(), .remaining = std.math.maxInt(u64), .ptr = &probe, .checkpoint = Probe.check };
    var controlled: json.Budget = .{ .shared = &parent };
    try std.testing.expectEqual(std.math.Order.eq, try json.compare(.{ .object = left }, .{ .object = right }, &controlled, 0));
    try std.testing.expectEqual(legacy.remaining, controlled.remaining);
    try std.testing.expect(probe.calls > 4);
    for ([_]usize{ 4, 20, 100 }) |stop_after| {
        probe = .{ .stop_after = stop_after };
        parent.failure = null;
        parent.since_poll = 256;
        controlled = .{ .shared = &parent };
        try std.testing.expectError(error.OwnerCanceled, json.compare(.{ .object = left }, .{ .object = right }, &controlled, 0));
    }
    legacy = .{ .remaining = 8 };
    parent = .{ .alloc = arena.allocator(), .remaining = std.math.maxInt(u64), .ptr = &probe, .checkpoint = Probe.check };
    probe = .{};
    controlled = .{ .remaining = 8, .shared = &parent };
    try std.testing.expectError(error.SqlProgramLimitExceeded, json.compare(.{ .object = left }, .{ .object = right }, &legacy, 0));
    try std.testing.expectError(error.SqlProgramLimitExceeded, json.compare(.{ .object = left }, .{ .object = right }, &controlled, 0));
}

fn batches(a: std.mem.Allocator) !void {
    const hash = @import("batch_hash.zig");
    const Batch = @import("execution_batch.zig").Batch;
    const text: [4097]u8 = @splat('t');
    const values = [_]Datum{ Datum.json(.{ .string = &text }), .{}, Datum.json(.null), Datum.json(.{ .integer = 5 }) };
    var indices: [64]u32 = undefined;
    var cells: [64]Datum = undefined;
    var rows: [64][]const Datum = undefined;
    for (&indices, &cells, 0..) |*index, *cell, i| {
        index.* = @intCast(i % values.len);
        cell.* = values[index.*];
    }
    for (&rows, &cells) |*row, *cell| row.* = @as(*[1]Datum, @ptrCast(cell));
    const encoded: Batch = .{ .dictionary = .{ .values = &values, .indices = &indices } };
    for ([_]bool{ false, true }) |grouped| {
        const expected = try hash.columns(a, &.{&cells}, cells.len, grouped);
        defer a.free(expected);
        var probe: Probe = .{};
        const plain = try hash.columnsControlled(a, &.{&cells}, cells.len, grouped, probe.control());
        defer a.free(plain);
        const columnar = try hash.encodedColumnsControlled(a, &.{encoded}, cells.len, grouped, probe.control());
        defer a.free(columnar);
        const row_hashes = try hash.rowsControlled(a, &rows, grouped, probe.control());
        defer a.free(row_hashes);
        try std.testing.expectEqualDeep(expected, plain);
        try std.testing.expectEqualDeep(expected, columnar);
        try std.testing.expectEqualDeep(expected, row_hashes);
    }
}
test "SQL grouping work batch memo and dictionary paths preserve hashes and unwind allocation faults" {
    try batches(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, batches, .{});
    const encoded: @import("execution_batch.zig").Batch = .{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 1 })}, .indices = &@as([1024]u32, @splat(0)) } };
    var probe: Probe = .{ .stop_after = 32 };
    try std.testing.expectError(error.OwnerCanceled, @import("batch_hash.zig").encodedColumnsControlled(std.testing.allocator, &.{encoded}, 1024, true, probe.control()));
}

test "SQL grouping work retained key equality polls wide text and typed payloads" {
    const a = std.testing.allocator;
    const text: [32768]u8 = @splat('e');
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    _ = try store.append(&.{Datum.json(.{ .string = &text })});
    var probe: Probe = .{};
    try std.testing.expect(try store.equalControlled(a, 0, &.{Datum.json(.{ .string = &text })}, true, probe.control()));
    probe = .{ .stop_after = 6 };
    try std.testing.expectError(error.OwnerCanceled, store.equalControlled(a, 0, &.{Datum.json(.{ .string = &text })}, true, probe.control()));
    try std.testing.expect(try store.equal(a, 0, &.{Datum.json(.{ .string = &text })}, true));
}

test "SQL grouping work failures stop before group publication and retain ordinary cleanup" {
    const Groups = @import("operators.zig").Grouped;
    const a = std.testing.allocator;
    const text: [32768]u8 = @splat('g');
    const key = Datum.json(.{ .string = &text });
    for ([_]bool{ false, true }) |encoded| {
        var probe: Probe = .{ .stop_after = 5 };
        const group = try Groups.create(a, &.{.{ .kind = .count }}, .{ .control = probe.control() });
        defer group.deinit();
        const result = if (encoded)
            group.addEncodedColumns(&.{.{ .dictionary = .{ .values = &.{key}, .indices = &.{0} } }}, &.{.{ .dictionary = .{ .values = &.{Datum.json(.{ .integer = 1 })}, .indices = &.{0} } }}, 1)
        else
            group.addColumns(&.{&.{key}}, &.{&.{Datum.json(.{ .integer = 1 })}}, 1);
        try std.testing.expectError(error.OwnerCanceled, result);
        try std.testing.expectEqual(@as(u64, 0), group.rows_seen);
        try std.testing.expectEqual(@as(usize, 0), group.groups.items.len);
    }
    var probe: Probe = .{};
    const group = try Groups.create(a, &.{.{ .kind = .count }}, .{ .control = probe.control() });
    defer group.deinit();
    try group.add(&.{key}, &.{Datum.json(.{ .integer = 1 })});
    probe = .{ .stop_after = 4 };
    try std.testing.expectError(error.OwnerCanceled, group.add(&.{key}, &.{Datum.json(.{ .integer = 1 })}));
    try std.testing.expectEqual(@as(u64, 1), group.rows_seen);
    try std.testing.expectEqual(@as(usize, 1), group.groups.items.len);
}

test "SQL grouping work retained numeric and array comparisons preserve owned values during cancellation" {
    const a = std.testing.allocator;
    const numeric_text: [8192]u8 = @splat('3');
    var context: @import("numeric_value.zig").Context = .{ .alloc = a };
    var number = try @import("numeric_value.zig").parse(&context, &numeric_text);
    defer number.deinit();
    const text: [16384]u8 = @splat('a');
    const array = try @import("array_value.zig").Value.init(.text, &.{.{ .length = 1, .lower = -2 }}, &.{Datum.json(.{ .string = &text })}, .{});
    for ([_]Datum{ Datum.typedNumeric(&number.value), Datum.typedArray(&array) }) |value| {
        var store = @import("typed_store.zig").Store.init(a);
        defer store.deinit();
        _ = try store.append(&.{value});
        var probe: Probe = .{};
        try std.testing.expect(try store.equalControlled(a, 0, &.{value}, true, probe.control()));
        try std.testing.expect(probe.calls >= 8);
        probe = .{ .stop_after = 8 };
        try std.testing.expectError(error.OwnerCanceled, store.equalControlled(a, 0, &.{value}, true, probe.control()));
        try std.testing.expect(try store.equal(a, 0, &.{value}, true));
    }
}

test "SQL grouping work yields original CPU to a queued write during wide-key memo and equality work" {
    const Runnable = @import("../common/runnable_scheduler.zig");
    const Clock = struct {
        var tick: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return tick.load(.acquire);
        }
    };
    const Owner = struct {
        execution: *Runnable.Execution,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = Clock.tick.fetchAdd(std.time.ns_per_ms, .acq_rel);
            try self.execution.checkpoint();
        }
    };
    const Peer = struct {
        fn run(scheduler: *Runnable.Scheduler, entry: *Runnable.Entry, progressed: *std.atomic.Value(bool)) !void {
            defer scheduler.finish(entry);
            try scheduler.wait(entry);
            progressed.store(true, .release);
        }
    };
    Clock.tick.store(0, .release);
    const a = std.testing.allocator;
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 2, .checkpoint_budget = 1, .quantum_ms = 1, .max_wait_ms = 5000 });
    defer scheduler.close();
    var execution: Runnable.Execution = .{};
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 10 * std.time.ns_per_s, .retained_bytes = 72 }, .query);
    defer execution.finish();
    var peer: Runnable.Entry = .{ .options = .{ .io = std.testing.io, .native_now_ns = Clock.now, .allow_wait = true }, .class = .write, .bytes = 72 };
    try scheduler.submit(&peer);
    defer scheduler.finish(&peer);
    try std.testing.expectEqual(Runnable.State.queued, scheduler.state(&peer));
    var progressed: std.atomic.Value(bool) = .init(false);
    var future = try std.testing.io.concurrent(Peer.run, .{ &scheduler, &peer, &progressed });
    defer future.cancel(std.testing.io) catch {};
    var owner: Owner = .{ .execution = &execution };
    const group = try @import("operators.zig").Grouped.create(a, &.{.{ .kind = .count }}, .{ .coordinator_scope = .{ .execution = &execution, .allocator = a, .check = .{ .context = &owner, .check = Owner.check } } });
    var live = true;
    defer if (live) group.deinit();
    const text: [65536]u8 = @splat('p');
    const key = Datum.json(.{ .string = &text });
    try group.addColumns(&.{&.{ key, key }}, &.{&.{ Datum.json(.{ .integer = 1 }), Datum.json(.{ .integer = 1 }) }}, 2);
    try std.testing.expect(progressed.load(.acquire));
    try future.await(std.testing.io);
    try std.testing.expectEqual(@as(u64, 2), group.rows_seen);
    try std.testing.expectEqual(@as(usize, 1), group.groups.items.len);
    try std.testing.expect(scheduler.stats().yields > 0);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 72), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(?u64, 10 * std.time.ns_per_s), execution.entry.?.options.deadline_ns);
    group.deinit();
    live = false;
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "SQL grouping work retained copies preserve owned JSON numeric array and stored estimates" {
    const a = std.testing.allocator;
    const operators = @import("operators.zig");
    const layout = @import("../common/sql_array_layout.zig");
    var bytes: [25 + 16384]u8 = @splat(0);
    bytes[0] = 1;
    bytes[1] = 1;
    std.mem.writeInt(u32, bytes[4..8], 1, .little);
    std.mem.writeInt(u32, bytes[8..12], 1, .little);
    std.mem.writeInt(i32, bytes[12..16], -2, .little);
    std.mem.writeInt(u32, bytes[21..25], 16384, .little);
    @memset(bytes[25..], 'v');
    const view = try layout.View.open(.text, &bytes, .{});
    const stored: @import("array_value.zig").Value = .{ .element_type = .text, .dimensions = &.{.{ .length = 1, .lower = -2 }}, .stored = &view };
    const text: [16384]u8 = @splat('x');
    var numeric_context: @import("numeric_value.zig").Context = .{ .alloc = a };
    var number = try @import("numeric_value.zig").parse(&numeric_context, "12345678901234567890.100");
    defer number.deinit();
    const array = try @import("array_value.zig").Value.init(.text, &.{.{ .length = 1 }}, &.{Datum.json(.{ .string = &text })}, .{});
    var source_arena = std.heap.ArenaAllocator.init(a);
    defer source_arena.deinit();
    var object: std.json.ObjectMap = .empty;
    for (0..40) |index| {
        const key = try std.fmt.allocPrint(source_arena.allocator(), "{d}:{s}", .{ index, text });
        try object.put(source_arena.allocator(), key, .{ .string = &text });
    }
    for ([_]Datum{ .{}, Datum.json(.{ .string = &text }), Datum.typedNumeric(&number.value), Datum.typedArray(&array), Datum.typedArray(&stored), Datum.json(.{ .object = object }) }) |value| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var probe: Probe = .{};
        try std.testing.expectEqual(try operators.datumBytes(value), try operators.datumBytesWithControl(value, probe.control()));
        const copied = try operators.cloneDatumControlled(arena.allocator(), value, probe.control());
        try std.testing.expectEqual(try scalar.semanticHashDatum(value), try scalar.semanticHashDatum(copied));
        try std.testing.expectEqual(try operators.datumBytes(value), try operators.datumBytes(copied));
        if (value.array) |original| if (original.stored) |original_view| {
            try std.testing.expect(copied.array.?.stored.?.bytes.ptr != original_view.bytes.ptr);
        };
        if (value.value == .object) {
            for (object.keys()) |key| try std.testing.expectEqualStrings(&text, copied.value.object.get(key).?.string);
        }
        probe = .{ .stop_after = 1 };
        try std.testing.expectError(error.OwnerCanceled, operators.datumBytesWithControl(value, probe.control()));
    }
    for ([_]Datum{ Datum.json(.{ .string = &text }), Datum.typedArray(&stored), Datum.json(.{ .object = object }) }) |value| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        var probe: Probe = .{ .stop_after = 4 };
        try std.testing.expectError(error.OwnerCanceled, operators.cloneDatumControlled(arena.allocator(), value, probe.control()));
    }
}

fn retainedDictionary(a: std.mem.Allocator) !void {
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    var probe: Probe = .{};
    // Preserve repeated dictionary sharing, then exercise the high-cardinality
    // transition and appends after the representation becomes flat.
    for (0..600) |index| {
        var bytes: [96]u8 = @splat('x');
        _ = try std.fmt.bufPrint(bytes[0..8], "{d:0>8}", .{index % 300});
        const value = Datum.json(.{ .string = &bytes });
        _ = try store.appendControlled(&.{value}, probe.control());
        try std.testing.expect(try store.equal(a, index, &.{value}, true));
        try std.testing.expectEqual(try store.appendBytes(&.{value}), try store.appendBytesControlled(&.{value}, probe.control()));
    }
    const first_id = (try store.dictionaryId(0, 0)).?;
    try std.testing.expectEqual(first_id, (try store.dictionaryId(300, 0)).?);
    for (300..700) |index| {
        var bytes: [96]u8 = @splat('x');
        _ = try std.fmt.bufPrint(bytes[0..8], "{d:0>8}", .{index});
        _ = try store.appendControlled(&.{Datum.json(.{ .string = &bytes })}, probe.control());
    }
    try std.testing.expectEqual(@as(?u64, null), try store.dictionaryId(store.len - 1, 0));
}
test "SQL grouping work retained dictionary growth preserves sharing flat fallback and allocation cleanup" {
    try retainedDictionary(std.testing.allocator);
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(fixed.allocator(), retainedDictionary, .{});
}

test "SQL grouping work retained admission and physical copy preserve cancellation without publishing a row" {
    const a = std.testing.allocator;
    const text: [32768]u8 = @splat('x');
    const value = Datum.json(.{ .string = &text });
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    _ = try store.append(&.{value});
    var probe: Probe = .{ .stop_after = 4 };
    try std.testing.expectError(error.OwnerCanceled, store.appendBytesControlled(&.{value}, probe.control()));
    try std.testing.expect(!store.failed);
    try std.testing.expectEqual(@as(usize, 1), store.len);
    for ([_]usize{ 4, 14 }) |stop_after| {
        var fresh = @import("typed_store.zig").Store.init(a);
        defer fresh.deinit();
        probe = .{ .stop_after = stop_after };
        try std.testing.expectError(error.OwnerCanceled, fresh.appendControlled(&.{value}, probe.control()));
        try std.testing.expectEqual(@as(usize, 0), fresh.len);
        try std.testing.expect(fresh.failed);
    }
}

fn retainedTypedCopies(a: std.mem.Allocator) !void {
    var input = std.heap.ArenaAllocator.init(a);
    defer input.deinit();
    const text: [4097]u8 = @splat('x');
    const array = try @import("array_value.zig").Value.init(.text, &.{.{ .length = 2 }}, &.{ Datum.json(.{ .string = &text }), .{} }, .{});
    var context: @import("numeric_value.zig").Context = .{ .alloc = input.allocator() };
    var number = try @import("numeric_value.zig").parse(&context, "123456789012345678901234567890.0001");
    defer number.deinit();
    var object: std.json.ObjectMap = .empty;
    try object.put(input.allocator(), &text, .{ .string = &text });
    var store = @import("typed_store.zig").Store.init(a);
    defer store.deinit();
    var probe: Probe = .{};
    const values = [_]Datum{ Datum.typedArray(&array), Datum.typedNumeric(&number.value), Datum.json(.{ .object = object }) };
    _ = try store.appendControlled(&values, probe.control());
    for (values, 0..) |value, column| {
        try std.testing.expectEqual(try scalar.semanticHashDatum(value), try scalar.semanticHashDatum(try store.cell(input.allocator(), 0, column)));
    }
}
test "SQL grouping work retained typed copies unwind every allocation failure" {
    try retainedTypedCopies(std.testing.allocator);
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(fixed.allocator(), retainedTypedCopies, .{});
}

test "SQL grouping work exact reducers retain work budgets and atomic state on original cancellation" {
    const a = std.testing.allocator;
    const numeric = @import("numeric_value.zig");
    const Numeric = @import("numeric_aggregate.zig");
    const digits: [2048]u16 = @splat(1);
    const wide: numeric.Value = .{ .digits = &digits, .weight = digits.len - 1 };
    const one: numeric.Value = .{ .digits = &.{1} };
    var legacy: Numeric.Reducer = .{ .alloc = a };
    defer legacy.deinit();
    var controlled: Numeric.Reducer = .{ .alloc = a };
    defer controlled.deinit();
    var probe: Probe = .{};
    for ([_]numeric.Value{ one, wide, wide }) |value| {
        try legacy.add(value);
        try controlled.addControlled(value, probe.control());
        try std.testing.expectEqualDeep(legacy.state.buckets.items, controlled.state.buckets.items);
        try std.testing.expectEqual(legacy.remaining, controlled.remaining);
    }
    const result = (try controlled.finishControlled(false, probe.control())).?;
    const expected = (try legacy.finish(false)).?;
    try std.testing.expectEqual(try scalar.semanticHashDatum(Datum.typedNumeric(expected)), try scalar.semanticHashDatum(Datum.typedNumeric(result)));
    try std.testing.expectEqual(legacy.remaining, controlled.remaining);
    const before = try controlled.clone(a);
    var saved = before;
    defer saved.deinit();
    for ([_]bool{ false, true }) |merge| {
        probe = .{ .stop_after = 4 };
        const outcome = if (merge) controlled.mergeControlled(legacy.state, probe.control()) else controlled.addControlled(wide, probe.control());
        try std.testing.expectError(error.OwnerCanceled, outcome);
        try std.testing.expectEqual(saved.state.count, controlled.state.count);
        try std.testing.expectEqual(saved.state.weight, controlled.state.weight);
        try std.testing.expectEqual(saved.state.scale, controlled.state.scale);
        try std.testing.expectEqualSlices(i128, saved.state.buckets.items, controlled.state.buckets.items);
        try std.testing.expectEqual(result, (try controlled.finish(false)).?);
    }
    probe = .{ .stop_after = 4 };
    try std.testing.expectError(error.OwnerCanceled, saved.cloneControlled(a, probe.control()));
    var fresh = try saved.clone(a);
    defer fresh.deinit();
    probe = .{ .stop_after = 4 };
    try std.testing.expectError(error.OwnerCanceled, fresh.finishControlled(false, probe.control()));
    try std.testing.expect(fresh.result == null);
    probe = .{};
    _ = try fresh.finishControlled(false, probe.control());
    probe = .{ .stop_after = 1 };
    try std.testing.expectError(error.OwnerCanceled, fresh.finishControlled(false, probe.control()));
    var no_work: Numeric.Reducer = .{ .alloc = a, .remaining = 0 };
    defer no_work.deinit();
    probe = .{};
    try std.testing.expectError(error.SqlProgramLimitExceeded, no_work.addControlled(one, probe.control()));
    try std.testing.expectEqual(@as(u64, 0), no_work.state.count);
}

test "SQL grouping work aggregate extrema and DISTINCT poll owned keys without changing results" {
    const a = std.testing.allocator;
    const Operators = @import("operators.zig");
    const Column = @import("aggregate_state.zig").Column;
    var first: [32768]u8 = @splat('x');
    var second = first;
    first[first.len - 1] = 'a';
    second[second.len - 1] = 'b';
    for ([_]bool{ false, true }) |dynamic| {
        var column = Column.init(.{ .kind = .min, .input_type = if (dynamic) .json else .string });
        defer column.clear(a);
        try column.append(a);
        var probe: Probe = .{};
        try column.updateControlled(a, 0, Datum.json(.{ .string = &second }), probe.control());
        probe = .{ .stop_after = 8 };
        try std.testing.expectError(error.OwnerCanceled, column.updateControlled(a, 0, Datum.json(.{ .string = &first }), probe.control()));
        try std.testing.expectEqualStrings(&second, (try column.finish(0)).value.string);
        probe = .{};
        try column.updateControlled(a, 0, Datum.json(.{ .string = &first }), probe.control());
        try std.testing.expectEqualStrings(&first, (try column.finishControlled(0, probe.control())).value.string);
    }
    var state = try Operators.Aggregate.init(a, .count, .string);
    defer state.deinit();
    state.distinct = true;
    var probe: Probe = .{};
    try state.updateControlled(Datum.json(.{ .string = &first }), probe.control());
    try state.updateControlled(Datum.json(.{ .string = &first }), probe.control());
    try std.testing.expectEqual(@as(u64, 1), state.count);
    probe = .{ .stop_after = 6 };
    try std.testing.expectError(error.OwnerCanceled, state.updateControlled(Datum.json(.{ .string = &first }), probe.control()));
    try std.testing.expectEqual(@as(u64, 1), state.count);
    probe = .{};
    try state.updateControlled(Datum.json(.{ .string = &second }), probe.control());
    try std.testing.expectEqual(@as(u64, 2), state.count);
}

test "SQL grouping work typed aggregate batches preserve ordered states and cancel within updates" {
    const a = std.testing.allocator;
    const Column = @import("aggregate_state.zig").Column;
    const specs = [_]@import("operators.zig").AggregateSpec{
        .{ .kind = .count, .input_type = .integer },
        .{ .kind = .sum, .input_type = .integer },
        .{ .kind = .avg, .input_type = .number },
        .{ .kind = .bool_and, .input_type = .boolean },
    };
    var ids: [64]usize = @splat(0);
    var indices: [64]u32 = undefined;
    var values: [64]Datum = undefined;
    for (specs) |spec| for ([_]bool{ false, true }) |encoded| {
        const dictionary = [_]Datum{ .{}, Datum.json(if (spec.input_type == .boolean) .{ .bool = true } else .{ .integer = 9007199254740993 }), Datum.json(if (spec.input_type == .boolean) .{ .bool = false } else .{ .integer = -7 }) };
        for (&indices, &values, 0..) |*index, *value, i| {
            index.* = @intCast(i % dictionary.len);
            value.* = dictionary[index.*];
        }
        var column = Column.init(spec);
        defer column.clear(a);
        var baseline = Column.init(spec);
        defer baseline.clear(a);
        try column.append(a);
        try baseline.append(a);
        var probe: Probe = .{};
        if (encoded) try column.updateEncodedControlled(a, &ids, .{ .dictionary = .{ .values = &dictionary, .indices = &indices } }, probe.control()) else try column.updateBatchControlled(a, &ids, &values, probe.control());
        for (values) |value| try baseline.update(a, 0, value);
        try std.testing.expectEqualDeep(try baseline.finish(0), try column.finishControlled(0, probe.control()));
        probe = .{ .stop_after = 8 };
        const outcome = if (encoded) column.updateEncodedControlled(a, &ids, .{ .dictionary = .{ .values = &dictionary, .indices = &indices } }, probe.control()) else column.updateBatchControlled(a, &ids, &values, probe.control());
        try std.testing.expectError(error.OwnerCanceled, outcome);
    };
}

test "SQL grouping work global four lane reductions preserve results and stop before publication" {
    const a = std.testing.allocator;
    const Grouped = @import("operators.zig").Grouped;
    const specs = [_]@import("operators.zig").AggregateSpec{ .{ .kind = .count, .input_type = .integer }, .{ .kind = .sum, .input_type = .integer } };
    const value = Datum.json(.{ .integer = 9007199254740993 });
    const column: [64]Datum = @splat(value);
    const row = [_]Datum{ value, value };
    const rows: [64][]const Datum = @splat(&row);
    const Poll = struct {
        group: ?*Grouped = null,
        mutating_checks: usize = 0,
        stop: usize = std.math.maxInt(usize),
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.group.?.rows_seen != 0) {
                self.mutating_checks += 1;
                if (self.mutating_checks == self.stop) return error.OriginalAggregateCanceled;
            }
        }
    };
    for ([_]bool{ false, true }) |columns| for ([_]bool{ false, true }) |cancel| {
        var poll: Poll = .{ .stop = if (cancel) 10 else std.math.maxInt(usize) };
        const grouped = try Grouped.create(a, &specs, .{ .control = .{ .ptr = &poll, .checkpoint = Poll.check } });
        defer grouped.deinit();
        poll.group = grouped;
        const outcome = if (columns) grouped.addGlobalColumns(&.{ &column, &column }, column.len) else grouped.addGlobalBatch(&rows);
        if (cancel) {
            try std.testing.expectError(error.OriginalAggregateCanceled, outcome);
            try std.testing.expect(grouped.failed);
            try std.testing.expectEqual(@as(u64, 1), grouped.rows_seen);
            try std.testing.expectError(error.InvalidSqlBackendResponse, grouped.resultAt(a, 0));
        } else {
            try outcome;
            const reference = try Grouped.create(a, &specs, .{});
            defer reference.deinit();
            for (rows) |input| try reference.add(&.{}, input);
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            try std.testing.expectEqualDeep((try reference.resultAt(arena.allocator(), 0)).aggregates, (try grouped.resultAt(arena.allocator(), 0)).aggregates);
            try std.testing.expect(poll.mutating_checks > 10);
        }
    };
}

fn controlledAggregateCopies(a: std.mem.Allocator) !void {
    const operators = @import("operators.zig");
    const Column = @import("aggregate_state.zig").Column;
    const numeric = @import("numeric_value.zig");
    const digits: [512]u16 = @splat(2);
    const value: numeric.Value = .{ .digits = &digits, .weight = digits.len - 1 };
    var probe: Probe = .{};
    var source = try operators.Aggregate.initTyped(a, .sum, .number, .numeric);
    defer source.deinit();
    try source.updateControlled(Datum.typedNumeric(&value), probe.control());
    var destination = try operators.Aggregate.initTyped(a, .sum, .number, .numeric);
    defer destination.deinit();
    try @import("aggregate_partial.zig").mergeControlled(&destination, source, probe.control());
    const expected = try source.finishControlled(probe.control());
    try std.testing.expectEqual(try scalar.semanticHashDatum(expected), try scalar.semanticHashDatum(try destination.finishControlled(probe.control())));
    var column = Column.init(.{ .kind = .sum, .input_type = .number, .input_element = .numeric });
    defer column.clear(a);
    try column.appendControlled(a, probe.control());
    try column.mergeExactControlled(a, 0, source, probe.control());
    var snapshot = try column.snapshotControlled(a, 0, probe.control());
    defer snapshot.deinit();
    try std.testing.expectEqual(try scalar.semanticHashDatum(expected), try scalar.semanticHashDatum(try snapshot.finishControlled(probe.control())));
    var cloned = try source.numeric.?.cloneControlled(a, probe.control());
    defer cloned.deinit();
    try std.testing.expectEqual(source.numeric.?.remaining, cloned.remaining);
    try std.testing.expect(cloned.state.buckets.items.ptr != source.numeric.?.state.buckets.items.ptr);
}
test "SQL grouping work numeric state merge clone snapshot and publication unwind allocation failures" {
    try controlledAggregateCopies(std.testing.allocator);
    var fixed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(fixed.allocator(), controlledAggregateCopies, .{});
}

test "SQL grouping work pattern state merge retains DISTINCT null ownership and original cancellation" {
    const a = std.testing.allocator;
    const operators = @import("operators.zig");
    const partial = @import("aggregate_partial.zig");
    const text: [16384]u8 = @splat('x');
    var source = try operators.Aggregate.init(a, .pattern_set, .string);
    defer source.deinit();
    source.distinct = true;
    var probe: Probe = .{};
    try source.updateControlled(.{}, probe.control());
    try source.updateControlled(Datum.json(.{ .string = &text }), probe.control());
    try source.updateControlled(Datum.json(.{ .string = "tail" }), probe.control());
    for ([_]bool{ false, true }) |cancel| {
        var state = try operators.Aggregate.init(a, .pattern_set, .string);
        defer state.deinit();
        state.distinct = true;
        probe = .{ .stop_after = if (cancel) 8 else std.math.maxInt(usize) };
        const outcome = partial.mergeControlled(&state, source, probe.control());
        if (cancel) {
            try std.testing.expectError(error.OwnerCanceled, outcome);
        } else {
            try outcome;
            try partial.mergeControlled(&state, source, probe.control());
            const result = try state.finishControlled(probe.control());
            try std.testing.expectEqual(@as(u64, 2), state.count);
            try std.testing.expectEqual(@as(usize, 3), result.value.array.items.len);
            try std.testing.expect(result.value.array.items[0] == .null);
            try std.testing.expectEqualStrings(&text, result.value.array.items[1].string);
        }
    }
}

test "SQL grouping work partial merge cancellation prevents publishing an incomplete destination" {
    const a = std.testing.allocator;
    const Grouped = @import("operators.zig").Grouped;
    const specs = [_]@import("operators.zig").AggregateSpec{.{ .kind = .count, .input_type = .integer }};
    const source = try Grouped.create(a, &specs, .{});
    defer source.deinit();
    for (0..4) |index| try source.add(&.{Datum.json(.{ .integer = @intCast(index) })}, &.{Datum.json(.{ .integer = 1 })});
    const Poll = struct {
        group: ?*Grouped = null,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.group.?.groups.items.len >= 2) return error.OriginalMergeCanceled;
        }
    };
    var poll: Poll = .{};
    const destination = try Grouped.create(a, &specs, .{ .control = .{ .ptr = &poll, .checkpoint = Poll.check } });
    defer destination.deinit();
    poll.group = destination;
    try std.testing.expectError(error.OriginalMergeCanceled, destination.mergeExact(source, 0));
    try std.testing.expectEqual(@as(usize, 2), destination.groups.items.len);
    try std.testing.expect(destination.failed);
    try std.testing.expectError(error.InvalidSqlBackendResponse, destination.resultAt(a, 0));
    try std.testing.expectError(error.InvalidSqlBackendResponse, destination.nextResult(a));
    try std.testing.expectError(error.InvalidSqlBackendResponse, destination.nextResultBatch(a, 2, false));
    try std.testing.expectError(error.InvalidSqlBackendResponse, destination.nextTypedPartialResult(a));
    try std.testing.expectError(error.InvalidSqlBackendResponse, destination.nextPartialResult(a));
    try std.testing.expectError(error.InvalidSqlBackendResponse, destination.finish(a));
    // The failed guard must precede any use of the external receiver's fields.
    var external: @import("spill_grouped.zig").Grouped = undefined;
    try std.testing.expectError(error.InvalidSqlBackendResponse, destination.exportPartial(&external));
    const clean = try Grouped.create(a, &specs, .{});
    defer clean.deinit();
    try std.testing.expectError(error.InvalidSqlBackendResponse, clean.mergeExact(destination, 0));
    try std.testing.expectEqual(@as(usize, 0), clean.groups.items.len);
    try std.testing.expect(!clean.failed);
    try std.testing.expectEqual(@as(u64, 4), source.rows_seen);
    try std.testing.expect(!source.failed);
}

test "SQL grouping work partial read failure retires its cursor before another publication" {
    const a = std.testing.allocator;
    const Grouped = @import("operators.zig").Grouped;
    const Poll = struct {
        canceled: bool = false,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled) return error.OriginalPartialCanceled;
        }
    };
    for ([_]bool{ false, true }) |typed| {
        var poll: Poll = .{};
        const grouped = try Grouped.create(a, &.{.{ .kind = .count }}, .{ .control = .{ .ptr = &poll, .checkpoint = Poll.check } });
        defer grouped.deinit();
        for (0..3) |index| try grouped.add(&.{Datum.json(.{ .integer = @intCast(index) })}, &.{Datum.json(.{ .integer = 1 })});
        var output = std.heap.ArenaAllocator.init(a);
        defer output.deinit();
        poll.canceled = true;
        const outcome = if (typed) grouped.nextTypedPartialResult(output.allocator()) else grouped.nextPartialResult(output.allocator());
        try std.testing.expectError(error.OriginalPartialCanceled, outcome);
        try std.testing.expect(grouped.failed);
        poll.canceled = false;
        try std.testing.expectError(error.InvalidSqlBackendResponse, grouped.nextTypedPartialResult(a));
        try std.testing.expectError(error.InvalidSqlBackendResponse, grouped.nextPartialResult(a));
        try std.testing.expectError(error.InvalidSqlBackendResponse, grouped.resultAt(a, 0));
    }
}

test "SQL grouping work prepared hash bundle preserves legacy hashes without memo heap allocations" {
    const a = std.testing.allocator;
    const hash = @import("batch_hash.zig");
    const Prepared = hash.GroupedScratch;
    const text: [4097]u8 = @splat('x');
    const source = [_]Datum{ .{}, Datum.json(.null), Datum.json(.{ .string = &text }), Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .float = 1.5 }) };
    const other = [_]Datum{ Datum.json(.{ .integer = 0 }), Datum.json(.{ .string = "other-null-domain" }), Datum.json(.{ .string = "different text" }), Datum.json(.{ .integer = -7 }), Datum.json(.{ .float = std.math.nan(f64) }) };
    var cells: [64]Datum = undefined;
    var other_cells: [64]Datum = undefined;
    var indices: [64]u32 = undefined;
    for (&cells, &other_cells, &indices, 0..) |*cell, *other_cell, *index, i| {
        index.* = @intCast(i % source.len);
        cell.* = source[index.*];
        other_cell.* = other[index.*];
    }
    const bytes = Prepared.requiredBytes(cells.len, false).?;
    try std.testing.expect(bytes > cells.len * @sizeOf(usize));
    const buffer = try a.alignedAlloc(u8, Prepared.alignment, bytes);
    defer a.free(buffer);
    var prepared = try Prepared.init(buffer, cells.len, false);
    var denied = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    for ([_]bool{ false, true }) |grouped| {
        const expected = try hash.columns(a, &.{ &cells, &other_cells }, cells.len, grouped);
        defer a.free(expected);
        var probe: Probe = .{};
        try std.testing.expectEqualSlices(?u64, expected, try prepared.columns(&.{ &cells, &other_cells }, grouped, probe.control()));
        const column: @import("execution_batch.zig").Batch = .{ .dictionary = .{ .values = &source, .indices = &indices } };
        const other_column: @import("execution_batch.zig").Batch = .{ .dictionary = .{ .values = &other, .indices = &indices } };
        try std.testing.expectEqualSlices(?u64, expected, try prepared.encodedColumns(denied.allocator(), &.{ column, other_column }, grouped, probe.control()));
        try std.testing.expectEqual(@as(usize, 0), denied.alloc_index);
        probe = .{ .stop_after = 20 };
        try std.testing.expectError(error.OwnerCanceled, prepared.columns(&.{&cells}, grouped, probe.control()));
    }
    const identified_bytes = Prepared.requiredBytes(cells.len, true).?;
    const identified_buffer = try a.alignedAlloc(u8, Prepared.alignment, identified_bytes);
    defer a.free(identified_buffer);
    const identified = try Prepared.init(identified_buffer, cells.len, true);
    try std.testing.expectEqual(cells.len, identified.ids.len);
    try std.testing.expectEqual(@as(usize, 0), identified.states.len);
    try std.testing.expectEqual(@as(usize, 0), identified.memo.len);
    try std.testing.expectEqual(@as(?usize, null), Prepared.requiredBytes(4097, false));
}

test "SQL grouping work prepared memo fills its maximum row domain and retains exact keys" {
    const a = std.testing.allocator;
    const Prepared = @import("batch_hash.zig").GroupedScratch;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const cells = try arena.allocator().alloc(Datum, 4096);
    for (cells, 0..) |*cell, index| cell.* = Datum.json(.{ .string = try std.fmt.allocPrint(arena.allocator(), "key-{d}", .{index}) });
    const buffer = try a.alignedAlloc(u8, Prepared.alignment, Prepared.requiredBytes(cells.len, false).?);
    defer a.free(buffer);
    var prepared = try Prepared.init(buffer, cells.len, false);
    var probe: Probe = .{};
    const actual = try prepared.columns(&.{cells}, true, probe.control());
    const expected = try @import("batch_hash.zig").columns(a, &.{cells}, cells.len, true);
    defer a.free(expected);
    try std.testing.expectEqualSlices(?u64, expected, actual);
}

test "SQL grouping work optional bundle failure falls back but original OOM callbacks never do" {
    const a = std.testing.allocator;
    const Grouped = @import("operators.zig").Grouped;
    const count = 64;
    const bytes = Grouped.batchScratchBytes(count, false).?;
    const Fault = enum { physical, control, decode };
    const Filter = struct {
        backing: std.mem.Allocator,
        bytes: usize,
        fault: Fault,
        live: bool = false,
        denied: usize = 0,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (len == self.bytes) {
                if (self.fault == .physical and self.denied == 0) {
                    self.denied += 1;
                    return null;
                }
                const out = self.backing.rawAlloc(len, alignment, ret) orelse return null;
                self.live = true;
                return out;
            }
            return self.backing.rawAlloc(len, alignment, ret);
        }
        fn free(raw: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (memory.len == self.bytes) self.live = false;
            self.backing.rawFree(memory, alignment, ret);
        }
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.fault == .control and self.live) return error.OutOfMemory;
        }
        fn read(raw: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !Datum {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fault == .decode and self.live) return error.OutOfMemory;
            return Datum.json(.{ .integer = 9007199254740993 });
        }
    };
    const values: [count]Datum = @splat(Datum.json(.{ .integer = 9007199254740993 }));
    const specs = [_]@import("operators.zig").AggregateSpec{ .{ .kind = .count, .input_type = .integer }, .{ .kind = .sum, .input_type = .integer } };
    for ([_]bool{ false, true }) |encoded| for (std.enums.values(Fault)) |fault| {
        if (fault == .decode and !encoded) continue;
        var filter: Filter = .{ .backing = a, .bytes = bytes, .fault = fault };
        const grouped = try Grouped.create(filter.allocator(), &specs, .{ .control = .{ .ptr = &filter, .checkpoint = Filter.check } });
        defer grouped.deinit();
        const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &filter, .read = Filter.read, .count = count, .width = 1 } };
        const outcome = if (encoded) grouped.addEncodedColumns(&.{batch}, &.{ batch, batch }, count) else grouped.addColumns(&.{&values}, &.{ &values, &values }, count);
        if (fault == .physical) {
            try outcome;
            try std.testing.expectEqual(@as(usize, 1), filter.denied);
            try std.testing.expect(!grouped.failed);
            try std.testing.expectEqual(@as(u64, count), grouped.rows_seen);
            var output = std.heap.ArenaAllocator.init(a);
            defer output.deinit();
            const result = try grouped.resultAt(output.allocator(), 0);
            try std.testing.expectEqual(@as(i64, count), result.aggregates[0].value.integer);
            try std.testing.expectEqual(@as(i64, 9007199254740993 * count), result.aggregates[1].value.integer);
        } else {
            try std.testing.expectError(error.OutOfMemory, outcome);
            try std.testing.expect(grouped.failed);
            try std.testing.expectEqual(@as(u64, 0), grouped.rows_seen);
            try std.testing.expectEqual(@as(usize, 0), filter.denied);
        }
        try std.testing.expect(!filter.live);
    };
}

test "SQL grouping work oversized accelerator batches keep the serial path and original ordinals" {
    const a = std.testing.allocator;
    const Grouped = @import("operators.zig").Grouped;
    const scheduling = @import("parallel_scheduler.zig");
    const Runnable = @import("../common/runnable_scheduler.zig");
    const count = 4097;
    const key = Datum.json(.{ .integer = 7 });
    const value = Datum.json(.{ .integer = 1 });
    const values = try a.alloc(Datum, count);
    defer a.free(values);
    @memset(values, value);
    const keys = try a.alloc(Datum, count);
    defer a.free(keys);
    @memset(keys, key);
    const indices = try a.alloc(u32, count);
    defer a.free(indices);
    @memset(indices, 0);
    const ordinals = try a.alloc(u64, count);
    defer a.free(ordinals);
    for (ordinals, 0..) |*ordinal, index| ordinal.* = count - index;
    const one_hash = try @import("batch_hash.zig").columns(a, &.{&.{key}}, 1, true);
    defer a.free(one_hash);
    const hashes = try a.alloc(?u64, count);
    defer a.free(hashes);
    @memset(hashes, one_hash[0]);
    const Source = struct {
        checks: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
        }
        fn acquire(_: *anyopaque, _: usize) !?scheduling.ScratchLease {
            return error.UnexpectedOversizedAcceleratorReservation;
        }
    };
    for ([_]u8{ 0, 1, 2 }) |mode| {
        var source: Source = .{};
        var execution: Runnable.Execution = .{};
        const scope: scheduling.CoordinatorScope = .{ .execution = &execution, .allocator = a, .check = .{ .context = &source, .check = Source.check }, .scratch = .{ .context = &source, .acquire = Source.acquire } };
        const grouped = try Grouped.create(a, &.{.{ .kind = .count }}, .{ .coordinator_scope = scope });
        defer grouped.deinit();
        if (mode == 0) {
            try grouped.addColumns(&.{keys}, &.{values}, count);
        } else {
            const key_batch: @import("execution_batch.zig").Batch = .{ .dictionary = .{ .values = &.{key}, .indices = indices } };
            const value_batch: @import("execution_batch.zig").Batch = .{ .dictionary = .{ .values = &.{value}, .indices = indices } };
            try grouped.addEncodedColumnsIdentified(&.{key_batch}, &.{value_batch}, count, if (mode == 2) .{ .hashes = hashes, .ordinals = ordinals } else null);
        }
        var output = std.heap.ArenaAllocator.init(a);
        defer output.deinit();
        const result = try grouped.resultAt(output.allocator(), 0);
        try std.testing.expectEqual(@as(i64, count), result.aggregates[0].value.integer);
        try std.testing.expectEqual(@as(u64, if (mode == 2) count else 0), result.ordinal);
        try std.testing.expect(grouped.source_ordinal == null);
        try std.testing.expect(source.checks > count);
    }
}

test "SQL grouping work avoids duplicate scalar entry polls and preserves every wide value cancellation" {
    const values = [_]Datum{ .{}, Datum.json(.null), Datum.json(.{ .bool = true }), Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .float = 1.5 }) };
    for (values) |value| {
        var probe: Probe = .{};
        try std.testing.expectEqual(try scalar.semanticHashDatum(value), try scalar.semanticHashDatumControlled(value, probe.control()));
        try std.testing.expectEqual(try scalar.compareDatums(value, value), try scalar.compareDatumsControlled(value, value, probe.control()));
        // Fixed-size cells need entry and completion checks, with no extra
        // scheduler transition just to initialize a nested work meter.
        try std.testing.expect(probe.calls <= 4);
        probe = .{ .stop_after = 2 };
        try std.testing.expectError(error.OwnerCanceled, scalar.semanticHashDatumControlled(value, probe.control()));
        probe = .{ .stop_after = 2 };
        try std.testing.expectError(error.OwnerCanceled, scalar.compareDatumsControlled(value, value, probe.control()));
    }
    const left: [16384]u8 = @splat('w');
    var right: [16384]u8 = undefined;
    @memcpy(&right, &left);
    const value = Datum.json(.{ .string = &left });
    const other = Datum.json(.{ .string = &right });
    var hash_probe: Probe = .{};
    var compare_probe: Probe = .{};
    try std.testing.expectEqual(try scalar.semanticHashDatum(value), try scalar.semanticHashDatumControlled(value, hash_probe.control()));
    try std.testing.expectEqual(std.math.Order.eq, try scalar.compareDatumsControlled(value, other, compare_probe.control()));
    try std.testing.expect(hash_probe.calls > 4);
    try std.testing.expect(compare_probe.calls > 4);
    for (1..hash_probe.calls + 1) |stop| {
        var probe: Probe = .{ .stop_after = stop };
        try std.testing.expectError(error.OwnerCanceled, scalar.semanticHashDatumControlled(value, probe.control()));
    }
    for (1..compare_probe.calls + 1) |stop| {
        var probe: Probe = .{ .stop_after = stop };
        try std.testing.expectError(error.OwnerCanceled, scalar.compareDatumsControlled(value, other, probe.control()));
    }
}

test "SQL grouping work shares clone quanta across object fields and retires every canceled partial copy" {
    const operators = @import("operators.zig");
    const a = std.testing.allocator;
    var source = std.heap.ArenaAllocator.init(a);
    defer source.deinit();
    var fields: std.json.ObjectMap = .empty;
    for (0..256) |index| {
        const key = try std.fmt.allocPrint(source.allocator(), "k{d}", .{index});
        try fields.put(source.allocator(), key, .{ .string = "small" });
    }
    const input = Datum.json(.{ .object = fields });
    var copied_region = std.heap.ArenaAllocator.init(a);
    defer copied_region.deinit();
    var small: Probe = .{};
    const copy = try operators.cloneDatumControlled(copied_region.allocator(), input, small.control());
    try std.testing.expectEqual(try scalar.semanticHashDatum(input), try scalar.semanticHashDatum(copy));
    // Work scales by bounded byte/node quanta, rather than key count times
    // the number of nested copy/hash wrappers.
    try std.testing.expect(small.calls <= 10);
    const wide = try source.allocator().alloc(u8, 16385);
    @memset(wide, 'z');
    try fields.put(source.allocator(), wide, .{ .string = wide });
    const large = Datum.json(.{ .object = fields });
    var probe: Probe = .{};
    const large_copy = try operators.cloneDatumControlled(copied_region.allocator(), large, probe.control());
    try std.testing.expectEqual(try scalar.semanticHashDatum(large), try scalar.semanticHashDatum(large_copy));
    try std.testing.expect(probe.calls > small.calls + 8);
    for (1..probe.calls + 1) |stop| {
        var region = std.heap.ArenaAllocator.init(a);
        defer region.deinit();
        var canceled: Probe = .{ .stop_after = stop };
        try std.testing.expectError(error.OwnerCanceled, operators.cloneDatumControlled(region.allocator(), large, canceled.control()));
    }
    try std.testing.expectEqualStrings("small", fields.get("k0").?.string);
}

test "SQL grouping work metadata scopes share nested budgets and preserve direct byte checks and denied retries" {
    var probe: Probe = .{};
    var scope: Work.MetadataScope = .{ .original = probe.control() };
    const control = scope.view();
    try Work.poll(control);
    var nested: Work.MetadataScope = .{ .original = control };
    for (0..64) |_| try Work.metadataPoll(nested.view());
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try Work.metadataPoll(control);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    // A wide byte operation cannot borrow any of the remaining metadata
    // credit. It checks the original owner before touching its bytes.
    probe.stop_after = 3;
    const wide_bytes: [4097]u8 = @splat(1);
    try std.testing.expectError(error.OwnerCanceled, Work.hashBytes(control, 0, &wide_bytes));
    probe = .{};
    scope.remaining = 0;
    probe.stop_after = 1;
    try std.testing.expectError(error.OwnerCanceled, Work.metadataPoll(control));
    try std.testing.expectError(error.OwnerCanceled, Work.metadataPoll(control));
    try std.testing.expectEqual(@as(usize, 0), scope.remaining);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    // Insufficient partial credit must also be lost on denial. A caller
    // cannot switch from a wide chunk to a smaller header visit and continue.
    probe = .{ .stop_after = 1 };
    scope.remaining = 17;
    const name: [64]u8 = @splat('n');
    try std.testing.expectError(error.OwnerCanceled, Work.equalMetadataBytes(control, &name, &name));
    try std.testing.expectEqual(@as(usize, 0), scope.remaining);
    try std.testing.expectError(error.OwnerCanceled, Work.metadataPoll(control));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    // Ordinary callers without a scope keep their original per-call checks.
    probe = .{};
    try Work.metadataPoll(probe.control());
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
}

test "SQL grouping work metadata names retain exact comparison and every chunk cancellation" {
    const left: [4097]u8 = @splat('n');
    var right = left;
    var probe: Probe = .{};
    var scope: Work.MetadataScope = .{ .original = probe.control() };
    try Work.poll(scope.view());
    try std.testing.expect(try Work.equalMetadataBytes(scope.view(), &left, &right));
    const checks = probe.calls;
    try std.testing.expect(checks > 60);
    for (1..checks + 1) |stop| {
        var canceled: Probe = .{ .stop_after = stop };
        var current: Work.MetadataScope = .{ .original = canceled.control() };
        const result = blk: {
            Work.poll(current.view()) catch |err| break :blk @as(anyerror!bool, err);
            break :blk Work.equalMetadataBytes(current.view(), &left, &right);
        };
        try std.testing.expectError(error.OwnerCanceled, result);
    }
    probe = .{};
    scope.remaining = 64;
    right[right.len - 1] = 'x';
    try std.testing.expect(!try Work.equalMetadataBytes(scope.view(), &left, &right));
}

test "SQL grouping work batch metadata admission preserves selections and retires every denied prefix" {
    const Store = @import("typed_store.zig").Store;
    const Batch = @import("execution_batch.zig").Batch;
    const cells = [_]Datum{ .{}, Datum.json(.{ .integer = 7 }) };
    const columns: [65][]const Datum = @splat(&cells);
    const batch: Batch = .{ .vectors = .{ .values = &columns, .count = 2 } };
    var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer scratch.deinit();
    const selected = try batch.select(scratch.allocator(), &.{ 1, 0 });
    try std.testing.expect(selected.reader.read_frozen_controlled != null);
    try std.testing.expect(selected.reader.read_json_controlled != null);
    try std.testing.expect(selected.reader.read_dictionary_controlled != null);
    const Scenario = struct {
        fn run(a: std.mem.Allocator, input: Batch, stop: usize) !usize {
            var store = Store.init(a);
            defer store.deinit();
            var probe: Probe = .{ .stop_after = stop };
            var lease = Store.JsonLease.initControlled(a, probe.control());
            defer lease.deinit();
            const estimate = try store.batchAppendBytesLeased(a, input, std.math.maxInt(usize), &lease);
            try std.testing.expect(estimate > 0);
            try store.appendBatchLeased(input, &lease);
            try std.testing.expectEqual(@as(usize, 2), store.len);
            for (0..input.width()) |column| {
                try std.testing.expectEqual(@as(i64, 7), (try store.cell(a, 0, column)).value.integer);
                try std.testing.expect((try store.cell(a, 1, column)).sql_null);
            }
            return probe.calls;
        }
    };
    const checks = try Scenario.run(std.testing.allocator, selected, std.math.maxInt(usize));
    // Even the capability and selection layers together use fewer owner
    // checks than there are columns, while every denial still unwinds.
    std.debug.print("SQL metadata admission: columns={} rows=2 owner_checks={}\n", .{ columns.len, checks });
    try std.testing.expect(checks > 4 and checks < columns.len);
    for (1..checks + 1) |stop| {
        try std.testing.expectError(error.OwnerCanceled, Scenario.run(std.testing.allocator, selected, stop));
    }
}

test "SQL grouping work tiny byte kernels spend metadata bytes without changing hashes or denial" {
    const bytes: [64]u8 = @splat('h');
    for (0..bytes.len + 1) |len| {
        var probe: Probe = .{};
        var scope: Work.MetadataScope = .{ .original = probe.control() };
        try Work.poll(scope.view());
        try std.testing.expectEqual(std.hash.Wyhash.hash(19, bytes[0..len]), try Work.hashBytes(scope.view(), 19, bytes[0..len]));
        try std.testing.expect(try Work.equalBytes(scope.view(), bytes[0..len], bytes[0..len]));
        scope.remaining = 0;
        probe.stop_after = probe.calls + 1;
        try std.testing.expectError(error.OwnerCanceled, Work.hashBytes(scope.view(), 19, bytes[0..len]));
        try std.testing.expectEqual(@as(usize, 0), scope.remaining);
    }
}

test "SQL grouping work unknown reader capabilities retain owner and provider failure boundaries" {
    const Source = struct {
        calls: usize = 0,
        fn deny(raw: *anyopaque) error{InvalidSqlBackendResponse} {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.InvalidSqlBackendResponse;
        }
        fn read(raw: *anyopaque, _: std.mem.Allocator, _: usize, _: usize) !Datum {
            return deny(raw);
        }
        fn controlled(raw: *anyopaque, _: std.mem.Allocator, _: usize, _: usize, _: ?Work.Control) !Datum {
            return deny(raw);
        }
        fn dictionary(raw: *anyopaque, _: std.mem.Allocator, _: usize) !?@import("execution_batch.zig").Batch {
            return deny(raw);
        }
        fn decimal(raw: *anyopaque, _: std.mem.Allocator, _: usize, _: ?[]const usize) !?@import("execution_batch.zig").DecimalColumn {
            return deny(raw);
        }
        fn json(raw: *anyopaque, _: usize, _: usize) !?[]const u8 {
            return deny(raw);
        }
        fn frozen(raw: *anyopaque, _: usize, _: usize) !?@import("execution_batch.zig").FrozenJson {
            return deny(raw);
        }
        fn identity(raw: *anyopaque, _: usize, _: usize) !?u64 {
            return deny(raw);
        }
    };
    const Op = enum { cell, dictionary, decimal, json, frozen, identity };
    const Invoke = struct {
        fn call(op: Op, batch: @import("execution_batch.zig").Batch, control: ?Work.Control) !void {
            const a = std.testing.allocator;
            switch (op) {
                .cell => {
                    _ = try batch.cellControlled(a, 0, 0, control);
                },
                .dictionary => {
                    _ = try batch.dictionaryColumnControlled(a, 0, control);
                },
                .decimal => {
                    _ = try batch.decimalColumnSelectedControlled(a, 0, null, control);
                },
                .json => {
                    _ = try batch.encodedJsonControlled(0, 0, control);
                },
                .frozen => {
                    _ = try batch.frozenJsonControlled(0, 0, control);
                },
                .identity => {
                    _ = try batch.dictionaryIdentityControlled(0, 0, control);
                },
            }
        }
    };
    var source: Source = .{};
    const batch: @import("execution_batch.zig").Batch = .{ .reader = .{ .ptr = &source, .count = 1, .width = 1, .read = Source.read, .read_controlled = Source.controlled, .read_dictionary = Source.dictionary, .read_decimal_selected = Source.decimal, .read_json = Source.json, .read_frozen = Source.frozen, .read_identity = Source.identity } };
    for (std.enums.values(Op)) |op| {
        source.calls = 0;
        var probe: Probe = .{ .stop_after = 1 };
        var scope: Work.MetadataScope = .{ .original = probe.control() };
        try std.testing.expectError(error.OwnerCanceled, Invoke.call(op, batch, scope.view()));
        try std.testing.expectEqual(@as(usize, 0), source.calls);
        probe = .{};
        scope.remaining = 64;
        try std.testing.expectError(error.InvalidSqlBackendResponse, Invoke.call(op, batch, scope.view()));
        try std.testing.expectEqual(@as(usize, 1), source.calls);
    }
}

test "SQL grouping work fixed numeric read checks its owner outside the shared arena mutex" {
    const Store = @import("typed_store.zig").Store;
    const a = std.testing.allocator;
    var context: @import("numeric_value.zig").Context = .{ .alloc = a };
    var number = try @import("numeric_value.zig").parse(&context, "12.00");
    defer number.deinit();
    var store = Store.init(a);
    defer store.deinit();
    _ = try store.append(&.{Datum.typedNumeric(&number.value)});
    try std.testing.expect(store.columns[0].fixed != null);
    const Owner = struct {
        store: *Store,
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (!self.store.reader_mutex.tryLock()) return error.OwnerCanceled;
            self.store.reader_mutex.unlock();
        }
    };
    var owner: Owner = .{ .store = &store };
    var scope: Work.MetadataScope = .{ .original = .{ .ptr = &owner, .checkpoint = Owner.check } };
    const value = try store.cellControlled(a, 0, 0, scope.view());
    try std.testing.expectEqual(try scalar.semanticHashDatum(Datum.typedNumeric(&number.value)), try scalar.semanticHashDatum(value));
    try std.testing.expectEqual(@as(usize, 1), owner.calls);
}

test "SQL grouping work retained buffer growth keeps its original owner through copy and fill" {
    const a = std.testing.allocator;
    const Fixture = struct {
        fn grow(a0: std.mem.Allocator, stop_after: usize) !usize {
            var values: std.ArrayList(u64) = .empty;
            defer values.deinit(a0);
            try values.resize(a0, 2048);
            for (values.items, 0..) |*value, i| value.* = i;
            const old_ptr = values.items.ptr;
            const old_capacity = values.capacity;
            var owner: Probe = .{ .stop_after = stop_after };
            Work.ensureTotalCapacity(u64, a0, &values, old_capacity + 1, owner.control()) catch |err| {
                try std.testing.expectEqual(old_ptr, values.items.ptr);
                try std.testing.expectEqual(old_capacity, values.capacity);
                try std.testing.expectEqual(@as(usize, 2048), values.items.len);
                for (values.items, 0..) |value, i| try std.testing.expectEqual(i, value);
                return err;
            };
            for (values.items, 0..) |value, i| try std.testing.expectEqual(i, value);
            try Work.resizeFill(u64, a0, &values, old_capacity + 1, 42, owner.control());
            for (values.items[2048..]) |value| try std.testing.expectEqual(@as(u64, 42), value);
            return owner.calls;
        }
        fn allocationFailures(a0: std.mem.Allocator) !void {
            _ = try grow(a0, std.math.maxInt(usize));
        }
    };
    const count = try Fixture.grow(a, std.math.maxInt(usize));
    try std.testing.expect(count > 4);
    for (1..count + 1) |at| try std.testing.expectError(error.OwnerCanceled, Fixture.grow(a, at));
    try std.testing.checkAllAllocationFailures(a, Fixture.allocationFailures, .{});
}

test "SQL grouping work column widening retains values and stops at every original checkpoint" {
    const Store = @import("typed_store.zig").Store;
    const Kind = enum { integer, fixed, null_prefix };
    const Fixture = struct {
        fn exercise(a: std.mem.Allocator, kind: Kind, at: usize) !usize {
            var context: @import("numeric_value.zig").Context = .{ .alloc = a };
            var number = try @import("numeric_value.zig").parse(&context, "12.3400");
            defer number.deinit();
            var store = Store.init(a);
            defer store.deinit();
            const rows = 65;
            for (0..rows) |i| {
                const value: Datum = if (kind == .null_prefix or i % 3 == 0) .{} else switch (kind) {
                    .integer => Datum.json(.{ .integer = @intCast(i) }),
                    .fixed => Datum.typedNumeric(&number.value),
                    .null_prefix => unreachable,
                };
                _ = try store.append(&.{value});
            }
            const prior_fixed = store.columns[0].fixed;
            const tail = Datum.json(.{ .string = "widens the retained column" });
            var owner: Probe = .{ .stop_after = at };
            _ = store.appendControlled(&.{tail}, owner.control()) catch |err| {
                try std.testing.expectEqual(@as(usize, rows), store.len);
                if (owner.calls == 1) {
                    // Entry denial precedes every mutation and the failure guard.
                    try std.testing.expect(!store.failed);
                    _ = try store.append(&.{tail});
                } else {
                    try std.testing.expect(store.failed);
                    try std.testing.expectError(error.InvalidSqlBackendResponse, store.append(&.{tail}));
                }
                return err;
            };
            try std.testing.expectEqual(@as(usize, rows + 1), store.len);
            if (prior_fixed) |fixed| try std.testing.expectEqual(@as(usize, 0), fixed.cells.count());
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            for (0..rows) |i| {
                const actual = try store.cell(scratch.allocator(), i, 0);
                if (kind == .null_prefix or i % 3 == 0) {
                    try std.testing.expect(actual.sql_null);
                } else switch (kind) {
                    .integer => try std.testing.expectEqual(@as(i64, @intCast(i)), actual.value.integer),
                    .fixed => {
                        try std.testing.expectEqual(try scalar.semanticHashDatum(Datum.typedNumeric(&number.value)), try scalar.semanticHashDatum(actual));
                        try std.testing.expectEqual(number.value.scale, actual.numeric.?.scale);
                    },
                    .null_prefix => unreachable,
                }
            }
            try std.testing.expectEqualStrings(tail.value.string, (try store.cell(scratch.allocator(), rows, 0)).value.string);
            return owner.calls;
        }
        fn allocationFailures(a: std.mem.Allocator, kind: Kind) !void {
            _ = try exercise(a, kind, std.math.maxInt(usize));
        }
    };
    for (std.enums.values(Kind)) |kind| {
        const count = try Fixture.exercise(std.testing.allocator, kind, std.math.maxInt(usize));
        if (kind != .null_prefix) try std.testing.expect(count > 65);
        for (1..count + 1) |at| try std.testing.expectError(error.OwnerCanceled, Fixture.exercise(std.testing.allocator, kind, at));
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.allocationFailures, .{kind});
    }
}
