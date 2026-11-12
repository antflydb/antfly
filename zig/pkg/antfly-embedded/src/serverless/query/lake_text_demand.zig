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

//! Bounded, process-local hints for optional warming. They grant no authority:
//! consumers resolve each index against the currently authenticated directory.
const std = @import("std");
pub const History = struct {
    pub const max_blocks = 16;
    const Entry = struct {
        key: [32]u8,
        blocks: [max_blocks]u32 = undefined,
        len: usize = 0,
        touched: u64 = 0,
    };
    entries: [64]?Entry = @splat(null),
    mutex: std.atomic.Mutex = .unlocked,
    tick: u64 = 0,

    pub fn identity(scope: [32]u8, directory: []const u8) [32]u8 {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.text-demand.v1");
        hash.update(&scope);
        hash.update(directory);
        var key: [32]u8 = undefined;
        hash.final(&key);
        return key;
    }
    pub fn record(self: *History, key: [32]u8, block: usize) void {
        const index = std.math.cast(u32, block) orelse return;
        // Optional hints must never contend with a demand read.
        if (!self.mutex.tryLock()) return;
        defer self.mutex.unlock();
        self.tick +|= 1;
        var slot: usize = 0;
        var oldest: u64 = std.math.maxInt(u64);
        for (&self.entries, 0..) |*item, i| {
            if (item.*) |*entry| {
                if (std.mem.eql(u8, &entry.key, &key)) {
                    slot = i;
                    break;
                }
                if (entry.touched < oldest) {
                    oldest = entry.touched;
                    slot = i;
                }
            } else {
                oldest = 0;
                slot = i;
            }
        }
        if (self.entries[slot] == null or !std.mem.eql(u8, &self.entries[slot].?.key, &key)) self.entries[slot] = .{ .key = key };
        const entry = &self.entries[slot].?;
        var end: usize = @min(entry.len, max_blocks - 1);
        for (entry.blocks[0..entry.len], 0..) |prior, i| if (prior == index) {
            end = i;
            break;
        };
        std.mem.copyBackwards(u32, entry.blocks[1..][0..end], entry.blocks[0..end]);
        entry.blocks[0] = index;
        entry.len = @min(entry.len + @intFromBool(end == entry.len), max_blocks);
        entry.touched = self.tick;
    }
    pub fn recent(self: *History, key: [32]u8, out: *[max_blocks]u32) []const u32 {
        if (!self.mutex.tryLock()) return &.{};
        defer self.mutex.unlock();
        for (self.entries) |item| if (item) |entry| if (std.mem.eql(u8, &entry.key, &key)) {
            @memcpy(out[0..entry.len], entry.blocks[0..entry.len]);
            return out[0..entry.len];
        };
        return &.{};
    }
};
test "text demand hints bound recency and isolate credential and directory identities" {
    var history: History = .{};
    const key = History.identity(@splat(1), "directory-a");
    for (0..30) |i| history.record(key, i);
    var blocks: [History.max_blocks]u32 = undefined;
    try std.testing.expectEqual(@as(usize, 16), history.recent(key, &blocks).len);
    try std.testing.expectEqual(@as(u32, 29), blocks[0]);
    history.record(key, 20);
    try std.testing.expectEqual(@as(u32, 20), history.recent(key, &blocks)[0]);
    try std.testing.expectEqual(@as(u32, 29), blocks[1]);
    try std.testing.expectEqual(@as(usize, 0), history.recent(History.identity(@splat(2), "directory-a"), &blocks).len);
    try std.testing.expectEqual(@as(usize, 0), history.recent(History.identity(@splat(1), "directory-b"), &blocks).len);
    for (0..100) |i| history.record(History.identity(@splat(3), std.mem.asBytes(&i)), i);
    try std.testing.expectEqual(@as(usize, 0), history.recent(key, &blocks).len);
}
