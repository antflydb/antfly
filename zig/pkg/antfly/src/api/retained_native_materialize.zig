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

//! Reclaim each native wire page after copying its owned rows. The same live
//! budget admits assembly, parsing, serialization and the outgoing reply copy.
const std = @import("std");
const local = @import("antfly_local_sources");
const series = local.storage_retained_series;
const read = @import("retained_dataset_read.zig");
const Budget = @import("antfly_platform").allocator.BoundedAllocator;
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;

pub fn collect(backing: A, output: A, reader: anytype, request: read.NativeQuery, spec: series.Spec, now: i128, context: Context) ![]u8 {
    return collectLimited(backing, output, reader, request, spec, now, context, read.max_result_bytes);
}
fn collectLimited(backing: A, output: A, reader: anytype, request: read.NativeQuery, spec: series.Spec, now: i128, context: Context, limit: usize) ![]u8 {
    var memory: Budget = .{ .backing = backing, .limit = limit };
    defer std.debug.assert(memory.live == 0);
    return collectBudgeted(output, &memory, reader, request, spec, now, context) catch |err| {
        return if (err == error.OutOfMemory and memory.denied) error.DatasetPageBudgetExceeded else err;
    };
}
fn collectBudgeted(output: A, memory: *Budget, reader: anytype, request: read.NativeQuery, spec: series.Spec, now: i128, context: Context) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer arena.deinit();
    const a = arena.allocator();
    var page_arena = std.heap.ArenaAllocator.init(memory.allocator());
    defer page_arena.deinit();
    var rows: std.ArrayList(series.Bucket) = .empty;
    var position: ?series.Cursor = null;
    var root: []const u8 = "";
    var observation: ?read.Observation = null;
    var admitted_ns = now;
    var next: ?local.retained_dataset_types.RowKey = null;
    var encoded_bytes: usize = 0;
    while (true) {
        try context.ensureActive();
        _ = page_arena.reset(.free_all);
        const temp = page_arena.allocator();
        const raw = try reader.nextJson(temp, @intCast(@min(128, request.limit - @as(u32, @intCast(rows.items.len)))), context);
        if (raw.len > read.max_result_bytes - encoded_bytes) return error.DatasetPageBudgetExceeded;
        encoded_bytes += raw.len;
        const page = try std.json.parseFromSliceLeaky(struct { root_incarnation: []const u8, page: series.Page }, temp, raw, .{});
        if (position) |prior| {
            if (!std.meta.eql(prior, page.page.cursor) or !std.mem.eql(u8, root, page.root_incarnation) or
                observation.?.source_sequence != page.page.source_sequence or observation.?.admitted_ns != try read.Observation.retentionBoundary(spec, page.page.admitted_ns)) return error.RetainedSeriesResetRequired;
        } else {
            position = page.page.cursor;
            root = try a.dupe(u8, page.root_incarnation);
            observation = .{ .root = try std.fmt.parseInt(u128, root, 10), .cursor = page.page.cursor, .source_sequence = page.page.source_sequence, .admitted_ns = try read.Observation.retentionBoundary(spec, page.page.admitted_ns) };
            admitted_ns = @max(now, page.page.admitted_ns);
        }
        if (page.page.rows.len > request.limit - rows.items.len) return error.DatasetPageBudgetExceeded;
        for (page.page.rows) |row| try rows.append(a, .{ .start_ns = row.start_ns, .dimensions = try a.dupe(u8, row.dimensions), .rows = row.rows, .metrics = try series.ownMetrics(a, row.metrics) });
        if (page.page.after == null) break;
        if (page.page.rows.len == 0) return error.RetainedSeriesCorrupt;
        if (rows.items.len == request.limit) {
            if (!request.allow_more) return error.DatasetPageBudgetExceeded;
            next = local.storage_retained_series_archive.key(rows.items[rows.items.len - 1]);
            break;
        }
    }
    page_arena.deinit();
    page_arena = std.heap.ArenaAllocator.init(memory.allocator());
    try context.ensureActive();
    const bytes = try std.json.Stringify.valueAlloc(a, read.NativeReply{ .rows = rows.items, .root_incarnation = root, .cursor = position orelse return error.RetainedSeriesNotReady, .next = next, .admitted_ns = admitted_ns, .observation = observation }, .{});
    // The outgoing allocator need not be this budget (an HTTP request arena or
    // another ABI). Reserve its overlap logically before making that copy.
    if (bytes.len > memory.limit - memory.live) return error.DatasetPageBudgetExceeded;
    return output.dupe(u8, bytes);
}

const TestReader = struct {
    remaining: usize = 4096,
    calls: usize = 0,
    wide: bool = false,
    change_cut: bool = false,
    pub fn nextJson(self: *@This(), a: A, limit: u16, _: Context) ![]u8 {
        self.calls += 1;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temp = arena.allocator();
        const count = @min(limit, self.remaining);
        const rows = try temp.alloc(series.Bucket, count);
        for (rows) |*row| {
            const metrics = try temp.alloc(series.Metric, if (self.wide) 32 else 1);
            @memset(metrics, .{ .count = 1 });
            row.* = .{ .start_ns = @intCast(self.calls), .dimensions = "[]", .rows = 1, .metrics = metrics };
        }
        self.remaining -= count;
        return std.json.Stringify.valueAlloc(a, .{ .root_incarnation = "1", .page = series.Page{ .rows = rows, .after = if (self.remaining == 0) null else "more", .cursor = .{ .definition = @splat(2), .partition = @splat(3), .incarnation = 1, .sequence = 1 }, .source_sequence = if (self.change_cut and self.calls > 1) 2 else 1, .admitted_ns = 100 } }, .{});
    }
};
test "retained owner native controls materialization bounds live memory and preserves owned pages" {
    const a = std.testing.allocator;
    const spec: series.Spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 10, .retention_ns = 1000, .laws = &.{.count} };
    const request: read.NativeQuery = .{ .scope = .{ .resource = 1, .generation = 1, .plan = @splat(4), .group = 1, .revision = 1 }, .start_ns = "0", .end_ns = "10000", .limit = 4096 };
    var reader: TestReader = .{};
    const bytes = try collect(a, a, &reader, request, spec, 100, .{});
    defer a.free(bytes);
    var result = try std.json.parseFromSlice(read.NativeReply, a, bytes, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 32), reader.calls);
    try std.testing.expectEqual(@as(usize, 4096), result.value.rows.len);
    try std.testing.expectEqualStrings("[]", result.value.rows[0].dimensions);
    try std.testing.expectEqual(@as(i64, 1), result.value.rows[0].metrics[0].count);
    var wide: TestReader = .{ .wide = true };
    try std.testing.expectError(error.DatasetPageBudgetExceeded, collectLimited(a, a, &wide, request, spec, 100, .{}, 256 * 1024));
    var unavailable = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var no_memory: TestReader = .{};
    try std.testing.expectError(error.OutOfMemory, collect(unavailable.allocator(), a, &no_memory, request, spec, 100, .{}));
    var outgoing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var no_reply: TestReader = .{};
    try std.testing.expectError(error.OutOfMemory, collect(a, outgoing.allocator(), &no_reply, request, spec, 100, .{}));
    var changed: TestReader = .{ .change_cut = true };
    try std.testing.expectError(error.RetainedSeriesResetRequired, collect(a, a, &changed, request, spec, 100, .{}));
    var canceled = std.atomic.Value(bool).init(true);
    var cancel_reader: TestReader = .{};
    try std.testing.expectError(error.Canceled, collect(a, a, &cancel_reader, request, spec, 100, .{ .cancellation = .fromAtomic(&canceled) }));
    try std.testing.expectEqual(@as(usize, 0), cancel_reader.calls);
}
