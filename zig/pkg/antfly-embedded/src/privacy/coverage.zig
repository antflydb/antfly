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

//! Durable, bounded background coverage. Each published result describes an
//! observation interval, not an authorization proof or a cross-shard snapshot.
const std = @import("std");
const A = std.mem.Allocator;
const Value = std.json.Value;

pub const Counts = struct {
    total: u64 = 0,
    current: u64 = 0,
    pending: u64 = 0,
    failed_known: u64 = 0,
    with_findings: u64 = 0,
    protected_ready: u64 = 0,
    protected_blocked: u64 = 0,
    categories: Value = .{ .object = .empty },
    fields: Value = .{ .object = .empty },
    failures: Value = .null,
};
pub const Snapshot = struct {
    counts: Counts,
    started_at_millis: i64,
    completed_at_millis: i64,
};
pub const State = struct {
    snapshot: ?Snapshot = null,
    pending: Counts = .{},
    cursor: ?[]const u8 = null,
    started_at_millis: i64 = 0,
    updated_at_millis: i64 = 0,
    retry_after_millis: i64 = 0,
    page_size: u32 = 100,
};
const Page = struct {
    available: bool,
    total: ?u64 = 0,
    current: ?u64 = 0,
    pending: ?u64 = 0,
    failed_known: ?u64 = 0,
    with_findings: ?u64 = 0,
    protected_ready: ?u64 = 0,
    protected_blocked: ?u64 = 0,
    categories: Value = .{ .object = .empty },
    fields: Value = .{ .object = .empty },
    failures: Value = .null,
    next_key: ?[]const u8 = null,
};

/// Failure counts are complete; optional examples have independent row and
/// byte limits so large identifiers cannot inflate every metadata CAS.
pub const max_failure_detail_bytes: usize = 1024 * 1024;
pub fn failureDetailSize(entry: anytype) !usize {
    var counter: std.Io.Writer.Discarding = .init(&.{});
    try std.json.Stringify.value(entry, .{}, &counter.writer);
    return std.math.cast(usize, counter.fullCount()) orelse error.Overflow;
}
pub fn appendFailures(a: A, prior: Value, incoming: Value) !Value {
    var output: std.array_list.Managed(Value) = .init(a);
    var bytes: usize = 2;
    for ([_]Value{ prior, incoming }) |entries| {
        if (entries != .array) continue;
        for (entries.array.items) |entry| {
            if (output.items.len == 100) break;
            if (entry == .object) if (entry.object.get("key")) |key| {
                if (key == .string and key.string.len > max_failure_detail_bytes) continue;
            };
            const size = try failureDetailSize(entry);
            if (size +| bytes +| 1 > max_failure_detail_bytes) continue;
            try output.append(entry);
            bytes += size + 1;
        }
    }
    return .{ .array = output };
}

/// Allocations belong to the caller's pass arena, which must live until the
/// metadata CAS serializes this state. Failed pages never advance the cursor.
pub fn accept(a: A, state: *State, raw: []const u8, now: i64) !void {
    const page = (try std.json.parseFromSlice(Page, a, raw, .{ .ignore_unknown_fields = true, .allocate = .alloc_always })).value;
    state.updated_at_millis = now;
    if (!page.available) {
        state.page_size = @max(1, state.page_size / 2);
        state.retry_after_millis = now + 2000;
        return;
    }
    if (state.cursor == null) {
        state.pending = .{};
        if (state.started_at_millis == 0) state.started_at_millis = now;
    }
    inline for (.{ "total", "current", "pending", "failed_known", "with_findings", "protected_ready", "protected_blocked" }) |name| {
        @field(state.pending, name) = try std.math.add(u64, @field(state.pending, name), @field(page, name) orelse return error.InvalidPrivacyReceipt);
    }
    inline for (.{ "categories", "fields" }) |name| {
        const source = @field(page, name);
        if (source != .object) return error.InvalidPrivacyReceipt;
        // Clone before mutation: persisted JSON values can borrow the metadata
        // parser's arena, whose allocator must never be mixed with this pass.
        var target: Value = .{ .object = .empty };
        var prior = @field(state.pending, name).object.iterator();
        while (prior.next()) |entry| try target.object.put(a, entry.key_ptr.*, entry.value_ptr.*);
        var entries = source.object.iterator();
        while (entries.next()) |entry| {
            const count = try std.json.parseFromValue(u64, a, entry.value_ptr.*, .{});
            const old = if (target.object.get(entry.key_ptr.*)) |value| (try std.json.parseFromValue(u64, a, value, .{})).value else 0;
            try target.object.put(a, entry.key_ptr.*, .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{try std.math.add(u64, old, count.value)}) });
        }
        @field(state.pending, name) = target;
    }
    state.pending.failures = try appendFailures(a, state.pending.failures, page.failures);
    state.cursor = page.next_key;
    state.retry_after_millis = 0;
    if (state.cursor == null) {
        state.snapshot = .{ .counts = state.pending, .started_at_millis = state.started_at_millis, .completed_at_millis = now };
        state.pending = .{};
        state.started_at_millis = 0;
        state.retry_after_millis = now + 5000;
        state.page_size = 100;
    }
}

pub fn response(a: A, state: State) ![]u8 {
    const snapshot = state.snapshot orelse return std.json.Stringify.valueAlloc(a, .{
        .available = false,
        .refreshing = true,
        .total = @as(?u64, null),
        .current = @as(?u64, null),
        .pending = @as(?u64, null),
        .failed = @as(?u64, null),
        .with_findings = @as(?u64, null),
        .protected_ready = @as(?u64, null),
        .protected_blocked = @as(?u64, null),
    }, .{});
    const counts = snapshot.counts;
    return std.json.Stringify.valueAlloc(a, .{
        .available = true,
        .refreshing = state.started_at_millis != 0,
        .started_at_millis = snapshot.started_at_millis,
        .completed_at_millis = snapshot.completed_at_millis,
        .total = counts.total,
        .current = counts.current,
        .pending = counts.pending,
        .failed = if (counts.pending == 0) @as(?u64, counts.failed_known) else null,
        .with_findings = counts.with_findings,
        .protected_ready = counts.protected_ready,
        .protected_blocked = counts.protected_blocked,
        .categories = counts.categories,
        .fields = counts.fields,
        .failed_known = counts.failed_known,
        .failures = counts.failures,
    }, .{});
}

test "privacy coverage resumes pages, retains completed snapshots, and bounds slow pages" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var state: State = .{};
    try accept(a, &state, "{\"available\":true,\"total\":100,\"current\":80,\"pending\":20,\"categories\":{\"email\":4},\"fields\":{\"body\":4},\"next_key\":\"last\"}", 100);
    try std.testing.expectEqualStrings("last", state.cursor.?);
    try std.testing.expect(state.snapshot == null);
    try accept(a, &state, "{\"available\":false,\"total\":null,\"current\":null,\"pending\":null,\"failed\":null,\"with_findings\":null,\"protected_ready\":null,\"protected_blocked\":null}", 200);
    try std.testing.expectEqualStrings("last", state.cursor.?);
    try std.testing.expectEqual(@as(u32, 50), state.page_size);
    const partial = try std.json.Stringify.valueAlloc(a, state, .{});
    state = (try std.json.parseFromSlice(State, a, partial, .{})).value;
    try std.testing.expectEqualStrings("last", state.cursor.?);
    try std.testing.expectEqual(@as(u64, 100), state.pending.total);
    try accept(a, &state, "{\"available\":true,\"total\":2,\"current\":2,\"categories\":{\"email\":1},\"fields\":{\"body\":1}}", 300);
    try std.testing.expectEqual(@as(u64, 102), state.snapshot.?.counts.total);
    try std.testing.expectEqual(@as(u64, 82), state.snapshot.?.counts.current);
    const raw = try response(a, state);
    const result = (try std.json.parseFromSlice(Value, a, raw, .{})).value.object;
    try std.testing.expectEqual(@as(i64, 5), result.get("categories").?.object.get("email").?.integer);
    try std.testing.expect(result.get("failed").? == .null);
    try std.testing.expectEqual(@as(i64, 100), result.get("started_at_millis").?.integer);
    // Persist/reload between pages, as happens on restart and leader transfer.
    const persisted = try std.json.Stringify.valueAlloc(a, state, .{});
    state = (try std.json.parseFromSlice(State, a, persisted, .{})).value;
    try accept(a, &state, "{\"available\":true,\"total\":1,\"current\":1,\"next_key\":\"next\"}", 6000);
    try std.testing.expectEqual(@as(u64, 102), state.snapshot.?.counts.total);
    try accept(a, &state, "{\"available\":true}", 6100);
    try std.testing.expectEqual(@as(u64, 1), state.snapshot.?.counts.total);
    try std.testing.expectEqual(@as(u64, 0), state.snapshot.?.counts.pending);
}

test "privacy coverage bounds failure examples across pages including JSON escaping" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const huge = try a.alloc(u8, max_failure_detail_bytes + 1);
    @memset(huge, 'x');
    const escaped = try a.alloc(u8, 200000);
    @memset(escaped, 0);
    const raw = try std.json.Stringify.valueAlloc(a, .{ .available = true, .total = 3, .failed_known = 3, .next_key = "next", .failures = .{ .{ .key = huge, .error_code = "failed" }, .{ .key = escaped, .error_code = "failed" }, .{ .key = "small", .error_code = "failed" } } }, .{});
    var state: State = .{};
    try accept(a, &state, raw, 1);
    try std.testing.expectEqual(@as(usize, 1), state.pending.failures.array.items.len);
    try accept(a, &state, "{\"available\":true,\"total\":1,\"failed_known\":1,\"failures\":[{\"key\":\"another\",\"error_code\":\"failed\"}]}", 2);
    try std.testing.expectEqual(@as(u64, 4), state.snapshot.?.counts.failed_known);
    try std.testing.expectEqual(@as(usize, 2), state.snapshot.?.counts.failures.array.items.len);
    try std.testing.expect(try failureDetailSize(state.snapshot.?.counts.failures) <= max_failure_detail_bytes);
}

test "privacy coverage rejects null counts on an available page" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var state: State = .{};
    try std.testing.expectError(error.InvalidPrivacyReceipt, accept(arena.allocator(), &state, "{\"available\":true,\"total\":null}", 200));
    try std.testing.expect(state.snapshot == null);
}
