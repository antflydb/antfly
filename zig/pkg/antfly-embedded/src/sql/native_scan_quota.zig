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

//! Logical native work is independent of physical cursor and worker fragments.
const std = @import("std");
pub const Quota = struct {
    capacity: usize,
    row_limit: usize,
    page_limit: usize,
    rows: std.atomic.Value(usize) = .init(0),
    empty: std.atomic.Value(usize) = .init(0),
    pub fn admit(self: *Quota, count: usize, terminal: bool) !void {
        if (count == 0 and terminal) return;
        // Mutate before reading either counter. Sequential consistency makes
        // at least the last concurrent admission observe both row and empty
        // progress, including when each alone would fit the final credit.
        if (count != 0) {
            _ = std.math.add(usize, self.rows.fetchAdd(count, .seq_cst), count) catch return error.SqlProgramLimitExceeded;
        } else {
            _ = std.math.add(usize, self.empty.fetchAdd(1, .seq_cst), 1) catch return error.SqlProgramLimitExceeded;
        }
        const rows = self.rows.load(.seq_cst);
        const empty = self.empty.load(.seq_cst);
        const pages = rows / self.capacity +| @intFromBool(rows % self.capacity != 0) +| empty;
        if (rows > self.row_limit or pages > self.page_limit) return error.SqlProgramLimitExceeded;
    }
};

test "SQL native aggregate quota ignores fragmentation but charges empty progress" {
    var quota: Quota = .{ .capacity = 4096, .row_limit = 8192, .page_limit = 2 };
    for (0..128) |_| try quota.admit(64, false);
    try quota.admit(0, true);
    try std.testing.expectError(error.SqlProgramLimitExceeded, quota.admit(0, false));
    var empty: Quota = .{ .capacity = 4096, .row_limit = 8192, .page_limit = 2 };
    try empty.admit(0, false);
    try empty.admit(0, false);
    try std.testing.expectError(error.SqlProgramLimitExceeded, empty.admit(0, false));
}

test "SQL native aggregate quota reconciles concurrent rows and empty progress" {
    const Worker = struct {
        fn run(quota: *Quota, ready: *std.Io.Event, count: usize) bool {
            ready.wait(std.testing.io) catch return true;
            quota.admit(count, false) catch return true;
            return false;
        }
    };
    for (0..32) |_| {
        var quota: Quota = .{ .capacity = 4096, .row_limit = 8192, .page_limit = 1 };
        var ready: std.Io.Event = .unset;
        var first = try std.testing.io.concurrent(Worker.run, .{ &quota, &ready, @as(usize, 1) });
        var second = std.testing.io.concurrent(Worker.run, .{ &quota, &ready, @as(usize, 0) }) catch |err| {
            ready.set(std.testing.io);
            _ = first.await(std.testing.io);
            return err;
        };
        ready.set(std.testing.io);
        const row_failed = first.await(std.testing.io);
        const empty_failed = second.await(std.testing.io);
        try std.testing.expect(row_failed or empty_failed);
    }
}
