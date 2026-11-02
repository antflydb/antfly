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

//! Fixed scheduling metadata for independently funded online owner preparation.
const std = @import("std");
pub const capacity = 64;
pub const max_table_name_bytes = 255;
pub const initial_workspace_bytes = 64 * 1024;
pub const Request = struct {
    group_id: u64,
    name: [max_table_name_bytes]u8 = undefined,
    name_len: u8,
    deadline_ns: u64,
    workspace_bytes: usize = initial_workspace_bytes,
    retry_delay_ms: u32 = 2,
    pub fn table(self: *const Request) []const u8 {
        return self.name[0..self.name_len];
    }
    fn matches(self: *const Request, group: u64, name: []const u8) bool {
        return self.group_id == group and std.mem.eql(u8, self.table(), name);
    }
};

/// External synchronization belongs to the source registry. No allocator,
/// borrowed descriptor, foreground execution or I/O object enters this queue.
pub const Queue = struct {
    items: [capacity]Request = undefined,
    head: usize = 0,
    len: usize = 0,
    active: ?Request = null,
    pub fn push(self: *Queue, group: u64, name: []const u8, deadline_ns: u64) !bool {
        if (name.len == 0 or name.len > max_table_name_bytes) return error.InvalidArgument;
        if (self.active) |*request| if (request.matches(group, name)) return false;
        for (0..self.len) |i| if (self.items[(self.head + i) % capacity].matches(group, name)) return false;
        if (self.len + @intFromBool(self.active != null) == capacity) return error.StorageReadTemporarilyUnavailable;
        var request: Request = .{ .group_id = group, .name_len = @intCast(name.len), .deadline_ns = deadline_ns };
        @memcpy(request.name[0..name.len], name);
        self.append(request);
        return true;
    }
    fn append(self: *Queue, request: Request) void {
        std.debug.assert(self.len < capacity);
        self.items[(self.head + self.len) % capacity] = request;
        self.len += 1;
    }
    pub fn take(self: *Queue, now_ns: u64) ?Request {
        std.debug.assert(self.active == null);
        while (self.len != 0) {
            const request = self.items[self.head];
            self.head = (self.head + 1) % capacity;
            self.len -= 1;
            if (now_ns >= request.deadline_ns) continue;
            self.active = request;
            return request;
        }
        return null;
    }
    pub fn finish(self: *Queue, retry: bool, grow: bool, now_ns: u64) void {
        var request = self.active orelse return;
        self.active = null;
        if (!retry or now_ns >= request.deadline_ns) return;
        if (grow) {
            request.workspace_bytes = std.math.mul(usize, request.workspace_bytes, 2) catch return;
        } else request.retry_delay_ms = @min(request.retry_delay_ms *| 2, 1000);
        self.append(request);
    }
};

test "online owner preparation queue owns bounded names and coalesces without extending authority" {
    var queue: Queue = .{};
    var name = [_]u8{ 'a', 'b' };
    try std.testing.expect(try queue.push(7, &name, 100));
    @memset(&name, 'x');
    try std.testing.expect(!try queue.push(7, "ab", 900));
    const first = queue.take(1).?;
    try std.testing.expectEqualStrings("ab", first.table());
    try std.testing.expectEqual(@as(u64, 100), first.deadline_ns);
    try std.testing.expect(!try queue.push(7, "ab", 900));
    queue.finish(true, true, 2);
    const second = queue.take(3).?;
    try std.testing.expectEqual(@as(u64, 100), second.deadline_ns);
    try std.testing.expectEqual(@as(usize, 2 * initial_workspace_bytes), second.workspace_bytes);
    queue.finish(true, false, 4);
    const retried = queue.take(5).?;
    try std.testing.expectEqual(@as(u64, 100), retried.deadline_ns);
    try std.testing.expectEqual(@as(u32, 4), retried.retry_delay_ms);
    queue.finish(true, false, 100);
    try std.testing.expectEqual(@as(usize, 0), queue.len);
    try std.testing.expectEqual(null, queue.active);
}

test "online owner preparation queue enforces aggregate active capacity and preserves FIFO across wraparound" {
    var queue: Queue = .{};
    for (0..capacity) |i| try std.testing.expect(try queue.push(i, "rows", 1000));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, queue.push(999, "rows", 1000));
    try std.testing.expectEqual(@as(u64, 0), queue.take(1).?.group_id);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, queue.push(999, "rows", 1000));
    queue.finish(false, false, 2);
    try std.testing.expect(try queue.push(999, "rows", 1000));
    for (1..capacity) |i| {
        try std.testing.expectEqual(@as(u64, i), queue.take(2).?.group_id);
        queue.finish(false, false, 2);
    }
    try std.testing.expectEqual(@as(u64, 999), queue.take(2).?.group_id);
    queue.finish(false, false, 2);
    for (0..1000) |i| {
        try std.testing.expect(try queue.push(i, "a", 1000));
        try std.testing.expect(try queue.push(i, "b", 1000));
        try std.testing.expectEqualStrings("a", queue.take(2).?.table());
        queue.finish(false, false, 2);
        try std.testing.expectEqualStrings("b", queue.take(2).?.table());
        queue.finish(false, false, 2);
    }
}

test "online owner preparation queue drops expired work and rejects invalid metadata without allocation" {
    var queue: Queue = .{};
    try std.testing.expectError(error.InvalidArgument, queue.push(7, "", 10));
    const too_long: [256]u8 = @splat('x');
    try std.testing.expectError(error.InvalidArgument, queue.push(7, &too_long, 10));
    try std.testing.expect(try queue.push(7, "old", 10));
    try std.testing.expect(try queue.push(8, "new", 20));
    const request = queue.take(10).?;
    try std.testing.expectEqual(@as(u64, 8), request.group_id);
    queue.finish(true, true, 20);
    try std.testing.expectEqual(null, queue.take(21));
    try std.testing.expectEqual(@as(usize, 0), queue.len);
}
