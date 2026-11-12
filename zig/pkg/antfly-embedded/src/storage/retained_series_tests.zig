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
const series = @import("retained_series.zig");
const a = std.testing.allocator;
const Entry = struct { key: []const u8, value: []const u8 };
// A transactional test store: aborted working copies never reach the durable map.
const Store = struct {
    rows: std.StringHashMap([]const u8),
    bucket_writes: usize = 0,
    ordered_seeks: usize = 0,
    fn init() Store {
        return .{ .rows = std.StringHashMap([]const u8).init(a) };
    }
    fn deinit(self: *Store) void {
        var it = self.rows.iterator();
        while (it.next()) |e| {
            a.free(e.key_ptr.*);
            a.free(e.value_ptr.*);
        }
        self.rows.deinit();
    }
    fn begin(self: *Store) !Store {
        var copy = Store.init();
        errdefer copy.deinit();
        var it = self.rows.iterator();
        while (it.next()) |e| try copy.put(e.key_ptr.*, e.value_ptr.*);
        return copy;
    }
    fn commit(self: *Store, working: *Store) void {
        self.deinit();
        self.* = working.*;
        working.* = Store.init();
    }
    pub fn get(self: *Store, key: []const u8) anyerror![]const u8 {
        return self.rows.get(key) orelse error.NotFound;
    }
    pub fn put(self: *Store, key: []const u8, value: []const u8) !void {
        if (std.mem.indexOf(u8, key, ":bucket:") != null and std.mem.indexOf(u8, key, ":support:") == null) self.bucket_writes += 1;
        const k = try a.dupe(u8, key);
        errdefer a.free(k);
        const v = try a.dupe(u8, value);
        errdefer a.free(v);
        const result = try self.rows.getOrPut(k);
        if (result.found_existing) {
            a.free(k);
            a.free(result.value_ptr.*);
        }
        result.value_ptr.* = v;
    }
    pub fn delete(self: *Store, key: []const u8) !void {
        if (self.rows.fetchRemove(key)) |e| {
            a.free(e.key);
            a.free(e.value);
        }
    }
    pub fn openCursor(self: *Store) !Scan {
        const entries = try a.alloc(Entry, self.rows.count());
        var it = self.rows.iterator();
        var i: usize = 0;
        while (it.next()) |e| : (i += 1) entries[i] = .{ .key = e.key_ptr.*, .value = e.value_ptr.* };
        std.mem.sort(Entry, entries, {}, struct {
            fn less(_: void, l: Entry, r: Entry) bool {
                return std.mem.order(u8, l.key, r.key) == .lt;
            }
        }.less);
        return .{ .entries = entries, .owner = self };
    }
};
const Scan = struct {
    entries: []const Entry,
    owner: *Store,
    position: usize = 0,
    pub fn close(self: *Scan) void {
        a.free(self.entries);
    }
    pub fn seekAtOrAfter(self: *Scan, key: []const u8) !?Entry {
        self.owner.ordered_seeks += 1;
        for (self.entries, 0..) |e, i| if (std.mem.order(u8, e.key, key) != .lt) {
            self.position = i;
            return e;
        };
        return null;
    }
    pub fn next(self: *Scan) !?Entry {
        self.position += 1;
        if (self.position >= self.entries.len) return null;
        return self.entries[self.position];
    }
    pub fn seekAtOrBefore(self: *Scan, key: []const u8) !?Entry {
        self.owner.ordered_seeks += 1;
        var i = self.entries.len;
        while (i > 0) {
            i -= 1;
            const e = self.entries[i];
            if (std.mem.order(u8, e.key, key) != .gt) return e;
        }
        return null;
    }
};
fn engine() series.Engine {
    return .{ .alloc = a, .spec = .{ .id = @splat(1), .generation = 1, .definition = @splat(2), .source_partition = @splat(3), .source_incarnation = 1, .interval_ns = 10, .mutable_for_ns = 100, .laws = &.{ .count, .sum, .avg, .min, .max } } };
}
fn sample(time: i64, values: []const ?f64) series.Row {
    return .{ .timestamp_ns = time, .dimensions = "[\"host-a\"]", .values = values };
}
fn ready(e: series.Engine, s: *Store) !void {
    try e.install(s, 0, 0, -1000);
    try e.backfill(s, "", &.{}, true);
}
fn latest(e: series.Engine, arena: std.mem.Allocator, s: *Store, c: series.Cursor) !series.Bucket {
    const frames = try e.poll(arena, s, c, 256);
    return frames[frames.len - 1].edits[0].row.?;
}
test "retained series correction maintains exact extrema and weighted average" {
    var s = Store.init();
    defer s.deinit();
    const e = engine();
    try ready(e, &s);
    const cut = try e.snapshotCursor(&s);
    const v1 = [_]?f64{ null, 3, 3, 3, 3 };
    const v2 = [_]?f64{ null, 9, 9, 9, 9 };
    const v3 = [_]?f64{ null, -7, -7, -7, -7 };
    try e.apply(&s, 1, 5, .mutation, &.{ .{ .key = "a", .after = sample(1, &v1) }, .{ .key = "b", .after = sample(2, &v2) } });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var b = try latest(e, arena.allocator(), &s, cut);
    try std.testing.expectEqual(@as(i64, 2), b.rows);
    try std.testing.expectEqual(@as(?f64, 6), b.metrics[2].value(.avg));
    try std.testing.expectEqual(@as(?f64, 3), b.metrics[3].value(.min));
    try std.testing.expectEqual(@as(?f64, 9), b.metrics[4].value(.max));
    try e.apply(&s, 2, 6, .mutation, &.{.{ .key = "b", .before = sample(2, &v2), .after = sample(2, &v3) }});
    b = try latest(e, arena.allocator(), &s, cut);
    try std.testing.expectEqual(@as(?f64, -2), b.metrics[2].value(.avg));
    try std.testing.expectEqual(@as(?f64, -7), b.metrics[3].value(.min));
    try std.testing.expectEqual(@as(?f64, 3), b.metrics[4].value(.max));
    try e.apply(&s, 3, 7, .mutation, &.{.{ .key = "b", .before = sample(2, &v3) }});
    b = try latest(e, arena.allocator(), &s, cut);
    try std.testing.expectEqual(@as(?f64, 3), b.metrics[2].value(.avg));
    const merged = try series.Metric.merge(.{ .sum = 100, .count = 10 }, a, .{ .sum = 0, .count = 1 });
    try std.testing.expectApproxEqAbs(@as(f64, 100.0 / 11.0), merged.value(.avg).?, 1e-12);
}
test "retained series expiration cannot outrun backfill or correction finality" {
    var s = Store.init();
    defer s.deinit();
    const e = engine();
    try e.install(&s, 0, 0, -1000);
    const values = [_]?f64{ null, 2, 2, 2, 2 };
    const change = series.Change{ .key = "a", .before = sample(1, &values) };
    try std.testing.expectError(error.RetainedSeriesNotReady, e.apply(&s, 1, 200, .expiration, &.{change}));
    try e.backfill(&s, "", &.{.{ .key = "a", .after = sample(1, &values) }}, true);
    const cut = try e.snapshotCursor(&s);
    try std.testing.expectError(error.RetainedSeriesCorrectionWindow, e.apply(&s, 1, 109, .expiration, &.{change}));
    try e.apply(&s, 1, 110, .expiration, &.{change});
    // Physical expiration produces no aggregate removal feed entry.
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectEqual(@as(usize, 0), (try e.poll(arena.allocator(), &s, cut, 10)).len);
    try std.testing.expectError(error.RetainedSeriesCorrectionWindow, e.apply(&s, 2, 1, .mutation, &.{change}));
    try std.testing.expectError(error.RetainedSeriesSequenceMismatch, e.apply(&s, 1, 110, .expiration, &.{change}));
}
test "retained series backfill interleaving does not double count or resurrect deletes" {
    var s = Store.init();
    defer s.deinit();
    const e = engine();
    try e.install(&s, 0, 0, -1000);
    const v1 = [_]?f64{ null, 2, 2, 2, 2 };
    const v2 = [_]?f64{ null, 8, 8, 8, 8 };
    try e.apply(&s, 1, 5, .mutation, &.{ .{ .key = "b", .before = sample(1, &v1), .after = sample(1, &v2) }, .{ .key = "c", .before = sample(1, &v1) } });
    try e.backfill(&s, "", &.{.{ .key = "a", .after = sample(1, &v1) }}, false);
    try std.testing.expectError(error.RetainedSeriesNotReady, e.snapshotCursor(&s));
    try std.testing.expectError(error.RetainedSeriesBackfillFence, e.backfill(&s, "", &.{}, true));
    try e.backfill(&s, "a", &.{.{ .key = "b", .after = sample(1, &v2) }}, true);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const b = try latest(e, arena.allocator(), &s, .{ .definition = e.spec.definition, .partition = e.spec.source_partition, .incarnation = 1, .sequence = 0 });
    try std.testing.expectEqual(@as(i64, 2), b.rows);
    try std.testing.expectEqual(@as(?f64, 5), b.metrics[2].value(.avg));
}
test "retained series failed source transaction leaves no progress or events" {
    var durable = Store.init();
    defer durable.deinit();
    const e = engine();
    try ready(e, &durable);
    var working = try durable.begin();
    defer working.deinit();
    const cut = try e.snapshotCursor(&durable);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    try e.apply(&working, 1, 5, .mutation, &.{.{ .key = "a", .after = sample(1, &values) }});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectEqual(@as(usize, 0), (try e.poll(arena.allocator(), &durable, cut, 10)).len);
    durable.commit(&working);
    try std.testing.expectEqual(@as(usize, 1), (try e.poll(arena.allocator(), &durable, cut, 10)).len);
}
test "retained series bounded feed forces reset rather than pinning raw retention" {
    var s = Store.init();
    defer s.deinit();
    const e = engine();
    try ready(e, &s);
    const cut = try e.snapshotCursor(&s);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    for (0..series.max_feed_frames + 1) |i| {
        var key: [16]u8 = undefined;
        const k = try std.fmt.bufPrint(&key, "{d}", .{i});
        try e.apply(&s, i + 1, 5, .mutation, &.{.{ .key = k, .after = sample(1, &values) }});
    }
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.RetainedSeriesResetRequired, e.poll(arena.allocator(), &s, cut, 10));
    var changed = cut;
    changed.definition = @splat(42);
    try std.testing.expectError(error.RetainedSeriesResetRequired, e.poll(arena.allocator(), &s, changed, 10));
    const current = try e.snapshotCursor(&s);
    try std.testing.expectEqual(@as(usize, 0), (try e.poll(arena.allocator(), &s, current, 10)).len);
}
test "retained series signed buckets and null aggregate inputs" {
    const e = engine();
    try std.testing.expectEqual(@as(i128, -10), try e.spec.bucket(-1));
    var shifted = e.spec;
    shifted.origin_ns = 3;
    try std.testing.expectEqual(@as(i128, -7), try shifted.bucket(-1));
    var s = Store.init();
    defer s.deinit();
    try ready(e, &s);
    const cut = try e.snapshotCursor(&s);
    const values = [_]?f64{ null, null, null, null, null };
    try e.apply(&s, 1, 5, .mutation, &.{.{ .key = "a", .after = sample(1, &values) }});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const b = try latest(e, arena.allocator(), &s, cut);
    try std.testing.expectEqual(@as(?f64, 1), b.metrics[0].value(.count));
    for (e.spec.laws[1..], b.metrics[1..]) |op, m| try std.testing.expectEqual(@as(?f64, null), m.value(op));
}

test "retained series indexed range pagination and temporary marker collection" {
    var s = Store.init();
    defer s.deinit();
    const e = engine();
    try e.install(&s, 0, 0, -1000);
    const v = [_]?f64{ null, 1, 1, 1, 1 };
    try e.backfill(&s, "", &.{ .{ .key = "a", .after = sample(-1, &v) }, .{ .key = "b", .after = sample(11, &v) }, .{ .key = "c", .after = sample(22, &v) } }, true);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const first = try e.scan(alloc, &s, -10, 30, 0, null, 1);
    try std.testing.expectEqual(@as(i128, -10), first.rows[0].start_ns);
    try std.testing.expect(first.after != null);
    const second = try e.scan(alloc, &s, -10, 30, 0, first.after, 1);
    try std.testing.expectEqual(@as(i128, 10), second.rows[0].start_ns);
    const third = try e.scan(alloc, &s, -10, 30, 0, second.after, 1);
    try std.testing.expectEqual(@as(i128, 20), third.rows[0].start_ns);
    try std.testing.expectEqual(@as(?[]const u8, null), third.after);
    try std.testing.expectEqual(first.cursor, third.cursor);
    try std.testing.expectError(error.RetainedSeriesResolutionMismatch, e.scan(alloc, &s, -1, 30, 0, null, 1));
    try std.testing.expectEqual(@as(usize, 2), try e.cleanupBackfill(&s, 2));
    try std.testing.expectEqual(@as(usize, 1), try e.cleanupBackfill(&s, 2));
    try std.testing.expectEqual(@as(usize, 0), try e.cleanupBackfill(&s, 2));
}
test "retained series definition and partition fence stale readers" {
    var s = Store.init();
    defer s.deinit();
    const e = engine();
    try ready(e, &s);
    const cut = try e.snapshotCursor(&s);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var other = cut;
    other.partition = @splat(91);
    try std.testing.expectError(error.RetainedSeriesResetRequired, e.poll(arena.allocator(), &s, other, 1));
    var wrong = e;
    wrong.spec.interval_ns = 20;
    try std.testing.expectError(error.RetainedSeriesDefinitionMismatch, wrong.snapshotCursor(&s));
    try std.testing.expectError(error.RetainedSeriesAlreadyExists, e.install(&s, 0, 0, -1000));
}

test "retained series idle ticks bound support cleanup and emit retention removals" {
    var s = Store.init();
    defer s.deinit();
    var e = engine();
    e.spec.retention_ns = 200;
    try ready(e, &s);
    const v = [_]?f64{ null, 2, 2, 2, 2 };
    try e.apply(&s, 1, 5, .mutation, &.{.{ .key = "a", .after = sample(1, &v) }});
    const cut = try e.snapshotCursor(&s);
    try std.testing.expectEqual(@as(usize, 1), try e.tick(&s, 110, 1));
    try std.testing.expectEqual(@as(usize, 1), try e.tick(&s, 1, 1));
    try std.testing.expectEqual(@as(usize, 0), try e.tick(&s, 1, 1));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var page = try e.scan(arena.allocator(), &s, 0, 10, 110, null, 2);
    try std.testing.expectEqual(@as(?f64, 2), page.rows[0].metrics[3].value(.min));
    try std.testing.expectEqual(@as(usize, 1), try e.tick(&s, 210, 1));
    const frames = try e.poll(arena.allocator(), &s, cut, 10);
    try std.testing.expectEqual(@as(usize, 1), frames.len);
    try std.testing.expectEqual(@as(?series.Bucket, null), frames[0].edits[0].row);
    try std.testing.expectError(error.RetainedSeriesHistoryExpired, e.scan(arena.allocator(), &s, 0, 10, 210, null, 2));
    page = try e.scan(arena.allocator(), &s, 10, 20, 210, null, 2);
    try std.testing.expectEqual(@as(usize, 0), page.rows.len);
    // Raw evidence older than the summary's own logical retention has no hold.
    try e.apply(&s, 2, 210, .expiration, &.{.{ .key = "a", .before = sample(1, &v) }});
}

fn allocationScenario(allocator: std.mem.Allocator) !void {
    var store = Store.init();
    defer store.deinit();
    var e = engine();
    e.alloc = allocator;
    e.spec.retention_ns = 200;
    try e.install(&store, 0, 0, -1000);
    const v = [_]?f64{ null, 3, 3, 3, 3 };
    try e.backfill(&store, "", &.{.{ .key = "a", .after = sample(1, &v) }}, true);
    const cut = try e.snapshotCursor(&store);
    try e.apply(&store, 1, 6, .mutation, &.{.{ .key = "b", .after = sample(1, &v) }});
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    _ = try e.scan(arena.allocator(), &store, 0, 10, 0, null, 1);
    _ = try e.poll(arena.allocator(), &store, cut, 1);
    _ = try e.cleanupBackfill(&store, 1);
    _ = try e.tick(&store, 210, 10);
}
test "retained series allocation failures release staging and serving memory" {
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), allocationScenario, .{});
}
test "retained series count projection preserves SQL integers beyond double precision" {
    const metric = series.Metric{ .count = 9_007_199_254_740_993 };
    try std.testing.expectEqual(@as(i64, 9_007_199_254_740_993), (try metric.finalize(.count)).integer);
}
test "retained series large source commands split into bounded feed frames" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const cut = try e.snapshotCursor(&store);
    const v = [_]?f64{ null, 3, 3, 3, 3 };
    var changes: [20]series.Change = undefined;
    var keys: [20][8]u8 = undefined;
    for (&changes, 0..) |*c, i| {
        c.* = .{ .key = try std.fmt.bufPrint(&keys[i], "{d}", .{i}), .after = sample(@intCast(i * 10), &v) };
    }
    try e.apply(&store, 1, 0, .mutation, &changes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const frames = try e.poll(arena.allocator(), &store, cut, 10);
    try std.testing.expectEqual(@as(usize, 3), frames.len);
    var count: usize = 0;
    for (frames, 0..) |frame, i| {
        try std.testing.expectEqual(@as(u64, i + 1), frame.sequence);
        try std.testing.expectEqual(@as(u64, 1), frame.source_sequence);
        try std.testing.expect(frame.edits.len <= 8);
        count += frame.edits.len;
    }
    try std.testing.expectEqual(@as(usize, 20), count);
}

test "retained series does not claim unavailable pre-activation history as empty" {
    var s = Store.init();
    defer s.deinit();
    const e = engine();
    try e.install(&s, 0, 0, 0);
    const v = [_]?f64{ null, 2, 2, 2, 2 };
    try e.backfill(&s, "", &.{ .{ .key = "a", .after = sample(-1, &v) }, .{ .key = "b", .after = sample(1, &v) } }, true);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectError(error.RetainedSeriesCoverageMissing, e.scan(arena.allocator(), &s, -10, 10, 0, null, 2));
    const page = try e.scan(arena.allocator(), &s, 0, 10, 0, null, 2);
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqual(@as(i64, 1), page.rows[0].rows);
    try e.apply(&s, 1, 0, .expiration, &.{.{ .key = "a", .before = sample(-1, &v) }});
}

test "retained series wide accumulation delays result overflow until projection" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const cut = try e.snapshotCursor(&store);
    const large = std.math.floatMax(f64);
    const input = [_]?f64{ null, large, large, large, large };
    try e.apply(&store, 1, 1, .mutation, &.{ .{ .key = "a", .after = sample(1, &input) }, .{ .key = "b", .after = sample(1, &input) } });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var b = try latest(e, arena.allocator(), &store, cut);
    try std.testing.expectError(error.RetainedSeriesNumericOverflow, b.metrics[1].finalize(.sum));
    try std.testing.expectEqual(@as(?f64, large), b.metrics[2].value(.avg));
    try e.apply(&store, 2, 2, .mutation, &.{.{ .key = "a", .before = sample(1, &input) }});
    b = try latest(e, arena.allocator(), &store, cut);
    try std.testing.expectEqual(large, (try b.metrics[1].finalize(.sum)).number);
}
test "retained series unchanged contributions do not reopen finalized buckets" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const values = [_]?f64{ null, 2, 2, 2, 2 };
    try e.apply(&store, 1, 1, .mutation, &.{.{ .key = "a", .after = sample(1, &values) }});
    const cut = try e.snapshotCursor(&store);
    _ = try e.tick(&store, 110, 100);
    // A non-aggregate source-field change or timestamp adjustment inside the
    // same bucket advances source progress without retract/add numerical drift.
    try e.apply(&store, 2, 111, .mutation, &.{.{ .key = "a", .before = sample(1, &values), .after = sample(2, &values) }});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectEqual(@as(usize, 0), (try e.poll(arena.allocator(), &store, cut, 10)).len);
    const progress = try e.status(arena.allocator(), &store);
    try std.testing.expectEqual(@as(u64, 2), progress.source_sequence);
}

test "retained series temporal domain matches shared SQL bins outside signed 64 bit epoch" {
    var e = engine();
    e.spec.interval_ns = std.time.ns_per_s;
    const ancient = @import("../datetime.zig").parseRfc3339ToSignedNs("0001-01-01T00:00:00Z").?;
    const future = @import("../datetime.zig").parseRfc3339ToSignedNs("9999-12-31T23:59:59Z").?;
    try std.testing.expectEqual(ancient, try e.spec.bucket(ancient));
    try std.testing.expectEqual(future, try e.spec.bucket(future));
    e.spec.origin_ns = std.math.minInt(i128);
    try std.testing.expectError(error.RetainedSeriesTimeOverflow, e.spec.bucket(std.math.maxInt(i128)));
}

test "retained series count field counts nonnull inputs independently of row cardinality" {
    var store = Store.init();
    defer store.deinit();
    var e = engine();
    e.spec.laws = &.{ .count, .count };
    e.spec.count_nonnull = &.{ false, true };
    try ready(e, &store);
    const cut = try e.snapshotCursor(&store);
    const missing = [_]?f64{ null, null };
    const present = [_]?f64{ null, 1 };
    try e.apply(&store, 1, 1, .mutation, &.{ .{ .key = "a", .after = sample(1, &missing) }, .{ .key = "b", .after = sample(1, &present) } });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var b = try latest(e, arena.allocator(), &store, cut);
    try std.testing.expectEqual(@as(i64, 2), (try b.metrics[0].finalize(.count)).integer);
    try std.testing.expectEqual(@as(i64, 1), (try b.metrics[1].finalize(.count)).integer);
    try e.apply(&store, 2, 2, .mutation, &.{.{ .key = "b", .before = sample(1, &present), .after = sample(1, &missing) }});
    b = try latest(e, arena.allocator(), &store, cut);
    try std.testing.expectEqual(@as(i64, 2), b.rows);
    try std.testing.expectEqual(@as(i64, 0), (try b.metrics[1].finalize(.count)).integer);
}

test "retained series hot bucket work scales with affected buckets rather than observation count" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const cut = try e.snapshotCursor(&store);
    const v = [_]?f64{ null, 3, 3, 3, 3 };
    var changes: [100]series.Change = undefined;
    var keys: [100][8]u8 = undefined;
    for (&changes, 0..) |*change, i| change.* = .{ .key = try std.fmt.bufPrint(&keys[i], "{d}", .{i}), .after = sample(1, &v) };
    try e.apply(&store, 1, 1, .mutation, &changes);
    try std.testing.expectEqual(@as(usize, 1), store.bucket_writes);
    try std.testing.expectEqual(@as(usize, 2), store.ordered_seeks);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const bucket = try latest(e, arena.allocator(), &store, cut);
    try std.testing.expectEqual(@as(i64, 100), bucket.rows);
    try std.testing.expectEqual(@as(?f64, 300), bucket.metrics[1].value(.sum));
    try std.testing.expectEqual(@as(?f64, 3), bucket.metrics[3].value(.min));
}

test "retained series expired history stops holding writes and TTL before physical cleanup or backfill" {
    var store = Store.init();
    defer store.deinit();
    var e = engine();
    e.spec.retention_ns = 200;
    try ready(e, &store);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    const old = sample(1, &values);
    const current = sample(211, &values);
    try e.apply(&store, 1, 5, .mutation, &.{.{ .key = "a", .after = old }});
    // No tick has deleted the old physical bucket yet.
    try e.apply(&store, 2, 211, .mutation, &.{.{ .key = "a", .before = old, .after = current }});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const page = try e.scan(arena.allocator(), &store, 210, 220, 211, null, 10);
    try std.testing.expectEqual(@as(i64, 1), page.rows[0].rows);
    try e.apply(&store, 3, 211, .mutation, &.{.{ .key = "a", .before = current, .after = old }});
    const cleared = try e.scan(arena.allocator(), &store, 210, 220, 211, null, 10);
    try std.testing.expectEqual(@as(usize, 0), cleared.rows.len);
    try e.apply(&store, 4, 211, .mutation, &.{.{ .key = "a", .before = old }});
    // Retained finalized history continues to reject changes.
    try std.testing.expectError(error.RetainedSeriesCorrectionWindow, e.apply(&store, 5, 211, .mutation, &.{.{ .key = "b", .after = sample(101, &values) }}));
    var other = Store.init();
    defer other.deinit();
    try e.install(&other, 0, 211, -1000);
    const progress = try e.status(arena.allocator(), &other);
    try std.testing.expect(try e.expirationReady(arena.allocator(), &other, progress, old, 211));
    try e.apply(&other, 1, 211, .expiration, &.{.{ .key = "a", .before = old }});
    try e.backfill(&other, "", &.{.{ .key = "b", .after = old }}, true);
    try std.testing.expectEqual(@as(u64, 0), (try e.snapshotCursor(&other)).sequence);
}

test "retained series replay rejects corrupted bucket identities and row states" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const cut = try e.snapshotCursor(&store);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    try e.apply(&store, 1, 5, .mutation, &.{.{ .key = "a", .after = sample(1, &values) }});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const frames = try e.poll(temp, &store, cut, 1);
    const frame_key = key: {
        var it = store.rows.keyIterator();
        while (it.next()) |k| if (std.mem.indexOf(u8, k.*, ":feed:") != null) break :key try temp.dupe(u8, k.*);
        return error.TestUnexpectedResult;
    };
    const edits = try temp.dupe(series.Edit, frames[0].edits);
    var broken = frames[0];
    broken.edits = edits;
    edits[0].key = "other-dataset-key";
    try store.put(frame_key, try std.json.Stringify.valueAlloc(temp, broken, .{}));
    try std.testing.expectError(error.RetainedSeriesCorrupt, e.poll(temp, &store, cut, 1));
    edits[0] = frames[0].edits[0];
    edits[0].row.?.rows = -1;
    try store.put(frame_key, try std.json.Stringify.valueAlloc(temp, broken, .{}));
    try std.testing.expectError(error.RetainedSeriesCorrupt, e.poll(temp, &store, cut, 1));
}

test "retained series prepared corrections retain extrema past finality and retire holds atomically" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const old = [_]?f64{ null, 3, 3, 3, 3 };
    const updated = [_]?f64{ null, 9, 9, 9, 9 };
    const changes = [_]series.Change{.{ .key = "a", .before = sample(1, &old), .after = sample(1, &updated) }};
    try e.apply(&store, 1, 5, .mutation, &.{.{ .key = "a", .after = sample(1, &old) }});
    try e.prepare(&store, @splat(1), 109, &changes);
    // An exact retry preserves admission; changing the value does not obtain
    // a new correction exception merely because its bucket already has a hold.
    try e.prepare(&store, @splat(1), 120, &changes);
    var rejected = try store.begin();
    defer rejected.deinit();
    try std.testing.expectError(error.RetainedSeriesCorrectionWindow, e.prepare(&rejected, @splat(1), 120, &.{ .{ .key = "a", .before = sample(1, &old), .after = sample(1, &old) }, .{ .key = "b", .after = sample(1, &updated) } }));
    try std.testing.expectEqual(@as(usize, 0), try e.tick(&store, 120, 1024));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const progress = try e.status(temp, &store);
    try std.testing.expect(!try e.expirationReady(temp, &store, progress, sample(1, &old), 120));
    const before = try e.scan(temp, &store, 0, 10, 120, null, 10);
    try std.testing.expectEqual(@as(?f64, 3), before.rows[0].metrics[3].value(.min));
    try e.applyPrepared(&store, @splat(1), 2, 120, .mutation, &changes);
    const after = try e.scan(temp, &store, 0, 10, 120, null, 10);
    try std.testing.expectEqual(@as(?f64, 9), after.rows[0].metrics[3].value(.min));
    try std.testing.expectEqual(@as(?f64, 9), after.rows[0].metrics[4].value(.max));
    try std.testing.expectEqual(@as(usize, 0), try e.releasePrepared(&store, @splat(1)));
    try std.testing.expectEqual(@as(usize, 2), try e.tick(&store, 120, 1024));
    try std.testing.expect(try e.expirationReady(temp, &store, try e.status(temp, &store), sample(1, &updated), 120));
    try std.testing.expectError(error.RetainedSeriesCorrectionWindow, e.apply(&store, 3, 121, .mutation, &.{.{ .key = "a", .before = sample(1, &updated), .after = sample(1, &old) }}));
}

test "retained series held buckets do not starve other finalized support and abort releases only its references" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const old = [_]?f64{ null, 3, 3, 3, 3 };
    const updated = [_]?f64{ null, 9, 9, 9, 9 };
    try e.apply(&store, 1, 5, .mutation, &.{
        .{ .key = "a", .after = sample(1, &old) },
        .{ .key = "b", .after = sample(1, &old) },
        .{ .key = "later", .after = sample(11, &old) },
    });
    try e.prepare(&store, @splat(1), 109, &.{.{ .key = "a", .before = sample(1, &old), .after = sample(1, &updated) }});
    try e.prepare(&store, @splat(2), 109, &.{.{ .key = "b", .before = sample(1, &old), .after = sample(1, &updated) }});
    // A physical work budget of one visits the hold once, then continues from
    // its durable cursor rather than revisiting all documents in that bucket.
    try std.testing.expectEqual(@as(usize, 0), try e.tick(&store, 120, 1));
    try std.testing.expectEqual(@as(usize, 1), try e.tick(&store, 120, 1));
    try std.testing.expectEqual(@as(usize, 1), try e.tick(&store, 120, 1));
    try std.testing.expectEqual(@as(usize, 1), try e.releasePrepared(&store, @splat(1)));
    try std.testing.expectEqual(@as(usize, 0), try e.tick(&store, 120, 1024));
    try std.testing.expectEqual(@as(usize, 1), try e.releasePrepared(&store, @splat(2)));
    try std.testing.expectEqual(@as(usize, 4), try e.tick(&store, 120, 1024));
}

test "retained series prepared contribution proofs cannot authorize different rows or values" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const old = [_]?f64{ null, 3, 3, 3, 3 };
    const updated = [_]?f64{ null, 9, 9, 9, 9 };
    try e.apply(&store, 1, 5, .mutation, &.{.{ .key = "a", .after = sample(1, &old) }});
    try e.prepare(&store, @splat(1), 109, &.{.{ .key = "a", .before = sample(1, &old), .after = sample(1, &updated) }});
    var wrong = try store.begin();
    defer wrong.deinit();
    try std.testing.expectError(error.RetainedSeriesReservationMismatch, e.applyPrepared(&wrong, @splat(1), 2, 120, .mutation, &.{.{ .key = "a", .before = sample(1, &old) }}));
    try std.testing.expectError(error.RetainedSeriesReservationMismatch, e.applyPrepared(&wrong, @splat(2), 2, 120, .mutation, &.{.{ .key = "a", .before = sample(1, &old), .after = sample(1, &updated) }}));
    try std.testing.expectEqual(@as(usize, 1), try e.releasePrepared(&store, @splat(1)));
}

test "retained series freeze proof refuses prepared inserts in otherwise empty coverage" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    try e.prepare(&store, @splat(1), 109, &.{.{ .key = "not-yet-visible", .after = sample(1, &values) }});
    _ = try e.tick(&store, 120, 1024);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const range = @import("../retained_dataset_types.zig").Range{ .start_ns = "0", .end_ns = "10" };
    try std.testing.expectError(error.RetainedSeriesPreparedHold, e.requireClosedSpan(temp, &store, range, null));
    try std.testing.expectEqual(@as(usize, 1), try e.releasePrepared(&store, @splat(1)));
    const cut = try e.requireClosedSpan(temp, &store, range, null);
    try std.testing.expectEqual(@as(u64, 0), cut.source_sequence);
    try std.testing.expectEqual(@as(i128, 120), cut.admitted_ns);
}

fn preparedAllocationScenario(allocator: std.mem.Allocator) !void {
    var store = Store.init();
    defer store.deinit();
    var e = engine();
    e.alloc = allocator;
    try ready(e, &store);
    const old = [_]?f64{ null, 3, 3, 3, 3 };
    const updated = [_]?f64{ null, 9, 9, 9, 9 };
    try e.apply(&store, 1, 5, .mutation, &.{.{ .key = "a", .after = sample(1, &old) }});
    const changes = [_]series.Change{.{ .key = "a", .before = sample(1, &old), .after = sample(1, &updated) }};
    try e.prepare(&store, @splat(1), 109, &changes);
    _ = try e.tick(&store, 120, 1);
    try e.applyPrepared(&store, @splat(1), 2, 120, .mutation, &changes);
    _ = try e.tick(&store, 120, 1024);
}

test "retained series prepared reservations release all memory on admission and resolution allocation failures" {
    var no_resize = std.testing.FailingAllocator.init(a, .{ .resize_fail_index = 0 });
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), preparedAllocationScenario, .{});
}

test "retained series prepared admission clock cannot reopen finalized history on a lower-clock commit" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const old = [_]?f64{ null, 3, 3, 3, 3 };
    const updated = [_]?f64{ null, 9, 9, 9, 9 };
    const changes = [_]series.Change{.{ .key = "a", .before = sample(1, &old), .after = sample(1, &updated) }};
    try e.apply(&store, 1, 5, .mutation, &.{.{ .key = "a", .after = sample(1, &old) }});
    try e.prepare(&store, @splat(1), 109, &changes);
    try e.prepare(&store, @splat(1), 120, &changes);
    try e.applyPrepared(&store, @splat(1), 2, 6, .mutation, &changes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    try std.testing.expectEqual(@as(i128, 120), (try e.status(arena.allocator(), &store)).admitted_ns);
    try std.testing.expectError(error.RetainedSeriesCorrectionWindow, e.apply(&store, 3, 7, .mutation, &.{.{ .key = "a", .before = sample(1, &updated), .after = sample(1, &old) }}));
}

test "retained series prepared expiration preserves history alongside ordinary cascade effects" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    try e.apply(&store, 1, 5, .mutation, &.{.{ .key = "expired", .after = sample(1, &values) }});
    // A TTL delete is safe only after its contribution is finalized.
    const expiration = series.Change{ .key = "expired", .before = sample(1, &values), .cause = .expiration };
    try std.testing.expectError(error.RetainedSeriesNotReady, e.prepare(&store, @splat(1), 109, &.{expiration}));
    const changes = [_]series.Change{ expiration, .{ .key = "survivor", .after = sample(121, &values) } };
    try e.prepare(&store, @splat(1), 120, &changes);
    // Retry cannot turn an admitted physical expiration into a retraction.
    try std.testing.expectError(error.RetainedSeriesReservationMismatch, e.prepare(&store, @splat(1), 120, &.{.{ .key = "expired", .before = sample(1, &values) }}));
    // The resolution command need not trust or retransmit per-row causes.
    try e.applyPrepared(&store, @splat(1), 2, 120, .mutation, &.{
        .{ .key = "expired", .before = sample(1, &values) },
        .{ .key = "survivor", .after = sample(121, &values) },
    });
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const page = try e.scan(arena.allocator(), &store, 0, 130, 120, null, 10);
    try std.testing.expectEqual(@as(usize, 2), page.rows.len);
    try std.testing.expectEqual(@as(i64, 1), page.rows[0].rows);
    try std.testing.expectEqual(@as(?f64, 3), page.rows[0].metrics[3].value(.min));
    try std.testing.expectEqual(@as(i64, 1), page.rows[1].rows);
    try std.testing.expectEqual(@as(usize, 0), try e.releasePrepared(&store, @splat(1)));
}

test "retained series generation closure drains even no-op prepares and finalizes future observations" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    const row = sample(1001, &values);
    try e.apply(&store, 1, 1, .mutation, &.{.{ .key = "future", .after = row }});
    try e.prepare(&store, @splat(1), 1, &.{.{ .key = "future", .before = row, .after = row }});
    try std.testing.expectError(error.RetainedSeriesPreparedHold, e.close(&store, 5));
    try std.testing.expectEqual(@as(usize, 1), try e.releasePrepared(&store, @splat(1)));
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const range = @import("../retained_dataset_types.zig").Range{ .start_ns = "1000", .end_ns = "1010" };
    try std.testing.expectError(error.RetainedSeriesNotFinal, e.requireClosedSpan(temp, &store, range, null));
    try e.close(&store, 5);
    try e.close(&store, 999);
    const cut = try e.requireClosedSpan(temp, &store, range, null);
    try std.testing.expect(cut.source_closed);
    try std.testing.expectEqual(@as(i128, 5), cut.admitted_ns);
    try std.testing.expectEqual(@as(u64, 1), cut.source_sequence);
    try std.testing.expectError(error.RetainedSeriesSourceClosed, e.apply(&store, 2, 6, .mutation, &.{.{ .key = "future", .before = row }}));
    try std.testing.expectError(error.RetainedSeriesSourceClosed, e.prepare(&store, @splat(2), 6, &.{.{ .key = "new", .after = row }}));
    const progress = try e.status(temp, &store);
    try std.testing.expect(try e.expirationReady(temp, &store, progress, row, 5));
    // Closed state belongs to the immutable export until revocation purges it.
    const closure = try progress.closureCut();
    try std.testing.expectEqual(@as(i128, 5), closure.admitted_ns);
    try std.testing.expectEqual(@as(usize, 0), try e.tick(&store, 6, 1024));
    const after_tick = try e.status(temp, &store);
    try std.testing.expectEqual(@as(i128, 5), after_tick.admitted_ns);
    try std.testing.expectEqualDeep(closure, try after_tick.closureCut());
    const page = try e.scan(temp, &store, 1000, 1010, 6, null, 10);
    try std.testing.expectEqual(@as(i64, 1), page.rows[0].rows);
    try std.testing.expectEqual(@as(?f64, 3), page.rows[0].metrics[3].value(.min));
    try std.testing.expectEqual(@as(u64, 1), (try e.status(temp, &store)).source_sequence);
}

test "retained series cancellation drains prepares without certifying unfinished backfill" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try e.install(&store, 0, 1, 0);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    const row = sample(1, &values);
    try e.prepare(&store, @splat(1), 1, &.{.{ .key = "new", .after = row }});
    try std.testing.expectError(error.RetainedSeriesPreparedHold, e.revoke(&store, 2));
    _ = try e.releasePrepared(&store, @splat(1));
    try e.revoke(&store, 3);
    try e.revoke(&store, 99);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const p = try e.status(arena.allocator(), &store);
    try std.testing.expect(p.closed and p.revoked and !p.ready);
    try std.testing.expectEqual(@as(i128, 3), p.admitted_ns);
    try std.testing.expectError(error.RetainedSeriesSourceRevoked, e.close(&store, 4));
    try std.testing.expectError(error.RetainedSeriesSourceRevoked, e.snapshotCursor(&store));
    try std.testing.expectError(error.RetainedSeriesSourceRevoked, e.requireClosedSpan(arena.allocator(), &store, .{ .start_ns = "0", .end_ns = "10" }, null));
    try std.testing.expectError(error.RetainedSeriesSourceClosed, e.apply(&store, 1, 4, .mutation, &.{.{ .key = "late", .after = row }}));
    var planner: @import("retained_series_maintenance.zig").Planner = .{};
    try std.testing.expectEqual(@as(?@import("retained_series_maintenance.zig").Task, null), try planner.next(&.{.{ .spec = e.spec, .progress = p }}, 100));
}

test "retained series close admission drains old votes without accepting new contributions" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const values = [_]?f64{ null, 3, 3, 3, 3 };
    const row = sample(1, &values);
    const changes = [_]series.Change{.{ .key = "old", .after = row }};
    try e.prepare(&store, @splat(1), 1, &changes);
    try e.beginClose(&store, 2);
    try e.beginClose(&store, 99);
    try e.prepare(&store, @splat(1), 2, &changes);
    try std.testing.expectError(error.RetainedSeriesSourceDraining, e.prepare(&store, @splat(2), 2, &changes));
    try std.testing.expectError(error.RetainedSeriesSourceDraining, e.apply(&store, 1, 2, .mutation, &changes));
    try std.testing.expectError(error.RetainedSeriesPreparedHold, e.close(&store, 2));
    // A decided transaction keeps its vote even after admission is closed.
    try e.applyPrepared(&store, @splat(1), 1, 3, .mutation, &changes);
    try e.close(&store, 4);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const p = try e.status(arena.allocator(), &store);
    try std.testing.expect(p.closed and !p.draining and !p.revoked);
    const page = try e.scan(arena.allocator(), &store, 0, 10, 4, null, 10);
    try std.testing.expectEqual(@as(i64, 1), page.rows[0].rows);
}

test "retained series exact dimensions seek across cardinality and resume by public row key" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const values = [_]?f64{ null, 1, 1, 1, 1 };
    for (0..200) |i| {
        const dimension = try std.fmt.allocPrint(a, "[{d}]", .{i});
        defer a.free(dimension);
        const key = try std.fmt.allocPrint(a, "row{d}", .{i});
        defer a.free(key);
        try e.apply(&store, i + 1, 1, .mutation, &.{.{ .key = key, .after = .{ .timestamp_ns = 1, .dimensions = dimension, .values = &values } }});
    }
    try e.apply(&store, 201, 11, .mutation, &.{.{ .key = "second", .after = .{ .timestamp_ns = 11, .dimensions = "[42]", .values = &values } }});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    store.ordered_seeks = 0;
    const first = try e.scanWhere(arena.allocator(), &store, 0, 20, 11, null, 1, "[42]", null);
    try std.testing.expectEqual(@as(usize, 1), first.rows.len);
    try std.testing.expectEqualStrings("[42]", first.rows[0].dimensions);
    try std.testing.expect(first.after != null);
    const key = @import("retained_series_archive.zig").key(first.rows[0]);
    const second = try e.scanWhere(arena.allocator(), &store, 0, 20, 11, null, 1, "[42]", key);
    try std.testing.expectEqual(@as(usize, 1), second.rows.len);
    try std.testing.expectEqual(@as(i128, 10), second.rows[0].start_ns);
    try std.testing.expectEqual(@as(?[]const u8, null), second.after);
    const absent = try e.scanWhere(arena.allocator(), &store, 0, 20, 11, null, 1, "[999]", null);
    try std.testing.expectEqual(@as(usize, 0), absent.rows.len);
    try std.testing.expect(store.ordered_seeks < 20);
}

test "retained series read admission is durable across rollback and does not alter replay sequence" {
    var store = Store.init();
    defer store.deinit();
    var e = engine();
    e.spec.retention_ns = 1000;
    try ready(e, &store);
    const values = [_]?f64{ null, 1, 1, 1, 1 };
    try e.apply(&store, 1, 1, .mutation, &.{.{ .key = "row", .after = sample(1, &values) }});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const before = try e.snapshotCursor(&store);
    try e.admitRetentionClock(&store, 1100);
    try std.testing.expect(std.meta.eql(before, try e.snapshotCursor(&store)));
    try std.testing.expectError(error.RetainedSeriesHistoryExpired, e.scan(arena.allocator(), &store, 0, 10, 1100, null, 10));
    try e.admitRetentionClock(&store, 1000);
    try std.testing.expectError(error.RetainedSeriesHistoryExpired, e.scan(arena.allocator(), &store, 0, 10, 1000, null, 10));
}

test "retained series closed export preserves its frozen coverage after live clock admission" {
    var store = Store.init();
    defer store.deinit();
    var e = engine();
    e.spec.retention_ns = 1000;
    try ready(e, &store);
    const values = [_]?f64{ null, 1, 1, 1, 1 };
    try e.apply(&store, 1, 1, .mutation, &.{.{ .key = "row", .after = sample(1, &values) }});
    try e.close(&store, 100);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const alloc = arena.allocator();
    const closed = try (try e.status(alloc, &store)).closureCut();
    const first = if (try e.spec.expired(closed.complete_from_ns, closed.admitted_ns)) try e.spec.bucket(closed.admitted_ns - e.spec.retention_ns) else closed.complete_from_ns;
    const request: @import("retained_series_read_contract.zig").ArchiveStart = .{ .sequence = closed.sequence, .complete_from_ns = closed.complete_from_ns, .closed_at_ns = closed.admitted_ns, .first_key = @import("../retained_dataset_types.zig").rowKey(first, @splat(0)) };
    const before = try e.scanArchive(alloc, &store, request, null, 10);
    try std.testing.expectEqual(@as(usize, 1), before.rows.len);
    try e.admitRetentionClock(&store, 110);
    const live = try e.scan(alloc, &store, 0, 10, 110, null, 10);
    try std.testing.expectEqual(@as(usize, 1), live.rows.len);
    const after = try e.scanArchive(alloc, &store, request, null, 10);
    try std.testing.expectEqual(@as(usize, 1), after.rows.len);
    try std.testing.expectEqual(@as(i128, 110), after.admitted_ns);
    try std.testing.expectEqualDeep(closed, try (try e.status(alloc, &store)).closureCut());
    try e.beginArchiveHandoff(&store, 120);
    try e.beginArchiveHandoff(&store, 130);
    try std.testing.expectError(error.RetainedSeriesSourceDraining, e.admitRetentionClock(&store, 140));
    _ = try e.tick(&store, 150, 10);
    const frozen = try e.status(alloc, &store);
    try std.testing.expect(frozen.archive_handoff);
    try std.testing.expectEqual(@as(i128, 120), frozen.admitted_ns);
    const final_page = try e.scanArchive(alloc, &store, request, null, 10);
    try std.testing.expectEqual(@as(i128, 120), final_page.admitted_ns);
    try std.testing.expectEqual(@as(usize, 1), final_page.rows.len);
}

test "retained series exact numeric state survives correction and archive merge" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    const large = [_]?f64{ null, 0x1p120, 0x1p120, 0x1p120, 0x1p120 };
    const small = [_]?f64{ null, 1, 1, 1, 1 };
    try e.apply(&store, 1, 1, .mutation, &.{ .{ .key = "large", .after = sample(1, &large) }, .{ .key = "small", .after = sample(1, &small) } });
    try e.apply(&store, 2, 2, .mutation, &.{.{ .key = "large", .before = sample(1, &large) }});
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const page = try e.scan(temp, &store, 0, 10, 2, null, 10);
    const row = page.rows[0];
    try std.testing.expectEqual(@as(f64, 1), (try row.metrics[1].finalize(.sum)).number);
    try std.testing.expectEqual(@as(f64, 1), (try row.metrics[2].finalize(.avg)).number);
    const archive = @import("retained_series_archive.zig");
    const bytes = try archive.encode(temp, e.spec, .{ .source_sequence = 2, .source_closed = true, .admitted_ns = 2, .complete_from_ns = -1000, .span = try @import("../retained_dataset_types.zig").KeySpan.fromRange(.{ .start_ns = "0", .end_ns = "10" }) }, &.{row});
    const decoded = try archive.decode(temp, e.spec, bytes);
    const merged = try row.metrics[1].merge(temp, decoded.rows[0].metrics[1]);
    try std.testing.expectEqual(@as(f64, 2), (try merged.finalize(.sum)).number);
    try std.testing.expectEqual(@as(f64, 1), (try merged.finalize(.avg)).number);
    const mixed = try row.metrics[1].merge(temp, .{ .count = 1, .sum = 2 });
    try std.testing.expect(mixed.exact_sum == null);
    try std.testing.expectEqual(@as(f64, 3), (try mixed.finalize(.sum)).number);
    try e.close(&store, 3);
    try std.testing.expectEqual(@as(usize, 0), try e.tick(&store, std.math.maxInt(u64), 10));
    const frozen = try e.scanArchive(temp, &store, .{ .sequence = 2, .complete_from_ns = -1000, .closed_at_ns = 3, .first_key = @import("../retained_dataset_types.zig").rowKey(0, @splat(0)) }, null, 10);
    try std.testing.expectEqual(@as(usize, 1), frozen.rows.len);
}

test "retained series diagnostics never advance retention or source authority" {
    var store = Store.init();
    defer store.deinit();
    const e = engine();
    try ready(e, &store);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const before = try e.status(arena.allocator(), &store);
    try e.recordDiagnostic(&store, "InvalidRetainedSeriesRow", 100);
    try e.recordDiagnostic(&store, null, 90);
    const failed = try e.status(arena.allocator(), &store);
    try std.testing.expectEqualStrings("InvalidRetainedSeriesRow", failed.last_error.?);
    try std.testing.expectEqual(before.admitted_ns, failed.admitted_ns);
    try std.testing.expectEqual(before.source_sequence, failed.source_sequence);
    try std.testing.expectEqual(before.feed_latest, failed.feed_latest);
    try e.recordDiagnostic(&store, null, 110);
    try std.testing.expectEqual(@as(?[]const u8, null), (try e.status(arena.allocator(), &store)).last_error);
}

test "retained series exact query reduction allocates once across partition cancellation" {
    const big = try (try series.ExactSum.input(0x1p1023)).encode(a);
    defer a.free(big);
    const tiny = try (try series.ExactSum.input(0x1p-1074)).encode(a);
    defer a.free(tiny);
    const negative = try (try series.ExactSum.input(-0x1p1023)).encode(a);
    defer a.free(negative);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
    var reducer = try series.MetricReducer.init(failing.allocator(), .{});
    defer reducer.deinit(failing.allocator());
    for (0..256) |_| {
        try reducer.add(failing.allocator(), .{ .count = 1, .exact_sum = big });
        try reducer.add(failing.allocator(), .{ .count = 1, .exact_sum = tiny });
        try reducer.add(failing.allocator(), .{ .count = 1, .exact_sum = negative });
    }
    const metric = try reducer.finish(a);
    defer a.free(metric.exact_sum.?);
    try std.testing.expectEqual(@as(i64, 768), metric.count);
    try std.testing.expectEqual(@as(f64, 0x1p-1066), (try metric.finalize(.sum)).number);
}

test "retained series wide numeric batches flush one bucket inside a fixed allocation ceiling" {
    var store = Store.init();
    defer store.deinit();
    const Budget = @import("antfly_platform").allocator.BoundedAllocator;
    var memory: Budget = .{ .backing = a, .limit = 2 * 1024 * 1024 };
    defer std.debug.assert(memory.live == 0);
    var e = engine();
    e.alloc = memory.allocator();
    const laws: [32]series.Law = @splat(.avg);
    e.spec.laws = &laws;
    try ready(e, &store);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const temp = arena.allocator();
    const changes = try temp.alloc(series.Change, 1024);
    const tiny_value: f64 = @bitCast(@as(u64, 1));
    const tiny: [32]?f64 = @splat(tiny_value);
    const large: [32]?f64 = @splat(std.math.floatMax(f64));
    for (changes, 0..) |*change, i| change.* = .{ .key = try std.fmt.allocPrint(temp, "row-{d}", .{i}), .after = .{ .timestamp_ns = 1, .dimensions = "[]", .values = if (i == 0) &tiny else &large } };
    store.bucket_writes = 0;
    try e.apply(&store, 1, 1, .mutation, changes);
    try std.testing.expectEqual(@as(usize, 1), store.bucket_writes);
    try std.testing.expect(memory.peak <= 2 * 1024 * 1024);
    const page = try e.scan(temp, &store, 0, 10, 1, null, 10);
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    try std.testing.expectEqual(@as(i64, 1024), page.rows[0].rows);
    for (page.rows[0].metrics) |metric| {
        try std.testing.expect(metric.exact_sum.?.len > 500);
        try std.testing.expectEqual(@as(i64, 1024), metric.count);
        const average = (try metric.finalize(.avg)).number;
        try std.testing.expect(std.math.isFinite(average));
        try std.testing.expect(average > 1e308);
    }
}
