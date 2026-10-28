// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! A numeric catalog sweep. New high IDs cannot move a sweep's upper fence;
//! failed attempts advance so one slow table cannot block its siblings.
const std = @import("std");

pub const tables_per_pass = 8;
pub const groups_per_pass = 8;

pub const Cursor = struct {
    active: bool = false,
    after: ?u64 = null,
    through: u64 = 0,
    epoch: u64 = 0,
    pending: bool = false,

    pub fn begin(self: *Cursor, ids: []const u64, epoch: u64) void {
        if (self.active) return;
        self.* = .{ .active = true, .through = maximum(ids), .epoch = epoch };
    }

    pub fn next(self: *const Cursor, ids: []const u64) ?u64 {
        var selected: ?u64 = null;
        for (ids) |id| {
            if (id > self.through) continue;
            if (self.after) |after| if (id <= after) continue;
            if (selected == null or id < selected.?) selected = id;
        }
        return selected;
    }

    pub fn attempted(self: *Cursor, id: u64) void {
        std.debug.assert(self.active and id <= self.through);
        std.debug.assert(self.after == null or id > self.after.?);
        self.after = id;
    }

    pub const Result = struct { more_pages: bool, repeat: bool, changed: bool = false };

    pub fn finish(self: *Cursor, ids: []const u64, epoch: u64) Result {
        if (self.next(ids) != null) return .{ .more_pages = true, .repeat = true };
        const changed = self.epoch != epoch or maximum(ids) > self.through;
        const result: Result = .{ .more_pages = false, .repeat = self.pending or changed, .changed = changed };
        self.* = .{};
        return result;
    }

    fn maximum(ids: []const u64) u64 {
        var max: u64 = 0;
        for (ids) |id| max = @max(max, id);
        return max;
    }
};

test "runtime status refresh cursor freezes a sweep under growing catalog IDs" {
    var cursor: Cursor = .{};
    cursor.begin(&.{ 3, 1, 2 }, 7);
    for ([_]u64{ 1, 2, 3 }) |expected| {
        const next = cursor.next(&.{ 10, 3, 2, 1, 9 }).?;
        try std.testing.expectEqual(expected, next);
        cursor.attempted(next);
    }
    const done = cursor.finish(&.{ 1, 2, 3, 9, 10 }, 8);
    try std.testing.expect(!done.more_pages and done.repeat and done.changed);
    cursor.begin(&.{ 1, 2, 3, 9, 10 }, 8);
    try std.testing.expectEqual(@as(?u64, 1), cursor.next(&.{ 10, 1 }));
}

test "runtime status refresh cursor carries pending work and skips failed or deleted attempts" {
    var cursor: Cursor = .{};
    cursor.begin(&.{ 1, 2, 3 }, 1);
    cursor.attempted(1); // Its observation may fail; the next pass serves 2.
    cursor.pending = true;
    try std.testing.expect(cursor.finish(&.{ 1, 2, 3 }, 1).more_pages);
    cursor.begin(&.{ 1, 2, 3, 4 }, 2); // Cannot renew this sweep's fence.
    try std.testing.expectEqual(@as(?u64, 2), cursor.next(&.{ 1, 2, 3, 4 }));
    cursor.attempted(2);
    const done = cursor.finish(&.{ 1, 2, 4 }, 2); // 3 was deleted.
    try std.testing.expect(!done.more_pages and done.repeat);
}

test "runtime status refresh cursor handles empty zero and maximum IDs without overflow" {
    var cursor: Cursor = .{};
    cursor.begin(&.{}, 0);
    try std.testing.expect(!cursor.finish(&.{}, 0).repeat);
    cursor.begin(&.{ 0, std.math.maxInt(u64) }, 1);
    cursor.attempted(cursor.next(&.{ 0, std.math.maxInt(u64) }).?);
    try std.testing.expectEqual(std.math.maxInt(u64), cursor.next(&.{std.math.maxInt(u64)}).?);
    cursor.attempted(std.math.maxInt(u64));
    try std.testing.expect(!cursor.finish(&.{ 0, std.math.maxInt(u64) }, 1).repeat);
}
