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

//! Production builder/edit/GC comparison, with independent exact-key oracles.
const std = @import("std");
const before = @import("before");
const after = @import("after");
fn run(comptime T: type, name: []const u8, sample: usize) !void {
    const a = std.heap.smp_allocator;
    const width = 131000;
    const count = 512;
    const keys = try a.alloc([width]u8, count);
    defer a.free(keys);
    for (keys, 0..) |*key, i| {
        @memset(key, 'w');
        std.mem.writeInt(u64, key[width - 8 ..], i, .big);
    }
    var memory: T.testing.MemoryStore = .{ .alloc = a };
    defer memory.deinit();
    var store = memory.store();
    store.prefix_keys = true;
    var tracked = std.testing.FailingAllocator.init(a, .{});
    const ca = tracked.allocator();
    const build_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    const original = block: {
        var builder: T.SortedBuilder = .{ .alloc = ca, .store = store, .target_bytes = T.bulk_target_page_bytes };
        defer builder.deinit();
        for (keys) |*key| try builder.add(key, "v");
        break :block (try builder.finish()).?;
    };
    const build_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds - build_start;
    try std.testing.expectEqual(tracked.allocated_bytes, tracked.freed_bytes);
    const build_allocated = tracked.allocated_bytes;
    const build_pages = memory.pages.count();
    const build_bytes = memory.written_bytes;
    const edit_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    const updated = (try T.apply(ca, store, original, &.{ .{ .key = &keys[7], .value = null }, .{ .key = &keys[300], .value = "updated" } })).?;
    const edit_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds - edit_start;
    const edit_allocated = tracked.allocated_bytes - build_allocated;
    try std.testing.expectEqual(tracked.allocated_bytes, tracked.freed_bytes);
    try std.testing.expectEqual(@as(u64, count - 1), updated.records);
    {
        var cursor = try T.Cursor.init(a, store, updated, "", null);
        defer cursor.deinit();
        for (keys, 0..) |*key, i| {
            if (i == 7) continue;
            const row = (try cursor.nextView()).?;
            try std.testing.expectEqual(.eq, row.key.order(T.KeyView.from(key)));
            try std.testing.expectEqualStrings(if (i == 300) "updated" else "v", row.value);
        }
        try std.testing.expect(try cursor.nextView() == null);
    }
    const Visitor = struct {
        rows: usize = 0,
        pub fn skip(_: *@This(), _: T.Ref) !bool {
            return false;
        }
        pub fn visit(_: *@This(), _: T.Ref) !void {}
        pub fn visitRecord(self: *@This(), _: T.Ref, key: []const u8, _: []const u8) !void {
            try std.testing.expectEqual(@as(usize, width), key.len);
            self.rows += 1;
        }
    };
    const gc_start = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    const gc_before = tracked.allocated_bytes;
    var visitor: Visitor = .{};
    try T.walkPostOrder(ca, store, updated, &visitor, false);
    const gc_ns = std.Io.Clock.awake.now(std.testing.io).nanoseconds - gc_start;
    try std.testing.expectEqual(@as(usize, count - 1), visitor.rows);
    try std.testing.expectEqual(tracked.allocated_bytes, tracked.freed_bytes);
    std.debug.print("sample={} variant={s} build_ns={} build_allocated_bytes={} build_pages={} build_written_bytes={} height={} edit_ns={} edit_allocated_bytes={} gc_ns={} gc_allocated_bytes={}\n", .{ sample, name, build_ns, build_allocated, build_pages, build_bytes, original.height, edit_ns, edit_allocated, gc_ns, tracked.allocated_bytes - gc_before });
}
test "compact_write_production_comparison" {
    for (0..6) |sample| {
        if (sample % 2 == 0) {
            try run(before, "before", sample);
            try run(after, "after", sample);
        } else {
            try run(after, "after", sample);
            try run(before, "before", sample);
        }
    }
}
