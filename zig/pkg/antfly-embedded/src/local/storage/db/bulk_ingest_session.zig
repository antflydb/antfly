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
const types = @import("types.zig");
/// Direct writes use bulk store admission while this session is active.
/// The legacy coalescing statistics remain wire-compatible; staging is unused.
pub const State = struct {
    active: bool = false,
    active_session: std.atomic.Value(u8) = .init(0),
    pub fn begin(self: *State) void {
        self.active = true;
        self.active_session.store(1, .monotonic);
    }
    pub fn finish(self: *State) void {
        self.active = false;
        self.active_session.store(0, .monotonic);
    }
    pub fn snapshot(self: *const State) types.BulkCoalescingStats {
        return .{ .active_session = self.active_session.load(.monotonic) != 0 };
    }
};
test "bulk session retains active admission and legacy statistics" {
    var state: State = .{};
    try std.testing.expect(!state.snapshot().active_session);
    state.begin();
    try std.testing.expect(state.active and state.snapshot().active_session);
    try std.testing.expectEqual(@as(u64, 0), state.snapshot().staged_keys);
    try std.testing.expectEqual(@as(u64, 0), state.snapshot().flush_calls);
    state.finish();
    try std.testing.expect(!state.active and !state.snapshot().active_session);
}

/// Owned identity proof scratch. Eligibility and durable summary publication
/// remain with the mutation coordinator under its apply fence.
pub const IdentityScratch = struct {
    enabled: bool = false,
    trusted: @import("doc_identity.zig").AllNewTrustedState = .{},
    seen: std.StringHashMapUnmanaged(void) = .empty,
    pub fn clearSeen(self: *IdentityScratch, alloc: std.mem.Allocator) void {
        var it = self.seen.keyIterator();
        while (it.next()) |key| alloc.free(@constCast(key.*));
        self.seen.clearRetainingCapacity();
    }
    pub fn reset(self: *IdentityScratch, alloc: std.mem.Allocator) void {
        self.enabled = false;
        self.trusted.deinit(alloc);
        self.clearSeen(alloc);
    }
    pub fn deinit(self: *IdentityScratch, alloc: std.mem.Allocator) void {
        self.reset(alloc);
        self.seen.deinit(alloc);
    }
    pub fn remember(self: *IdentityScratch, alloc: std.mem.Allocator, ids: []const []const u8) !bool {
        var batch = std.StringHashMapUnmanaged(void).empty;
        defer batch.deinit(alloc);
        for (ids) |id| {
            if (self.seen.contains(id) or batch.contains(id)) return false;
            try batch.put(alloc, id, {});
        }
        // Failure invalidates the proof rather than retaining a partial batch.
        errdefer self.reset(alloc);
        try self.seen.ensureUnusedCapacity(alloc, @intCast(ids.len));
        for (ids) |id| self.seen.putAssumeCapacity(try alloc.dupe(u8, id), {});
        return true;
    }
};

test "bulk identity scratch detects duplicate batches and resets owned keys" {
    const alloc = std.testing.allocator;
    var scratch: IdentityScratch = .{};
    defer scratch.deinit(alloc);
    scratch.enabled = true;
    try std.testing.expect(!try scratch.remember(alloc, &.{ "a", "a" }));
    try std.testing.expectEqual(@as(u32, 0), scratch.seen.count());
    try std.testing.expect(try scratch.remember(alloc, &.{ "a", "b" }));
    try std.testing.expect(!try scratch.remember(alloc, &.{"b"}));
    scratch.reset(alloc);
    try std.testing.expect(!scratch.enabled);
    try std.testing.expect(try scratch.remember(alloc, &.{"a"}));
}

test "bulk identity scratch invalidates partial proof on allocation failure" {
    const F = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var scratch: IdentityScratch = .{};
            defer scratch.deinit(alloc);
            scratch.enabled = true;
            _ = scratch.remember(alloc, &.{ "a", "b", "c" }) catch |err| {
                try std.testing.expect(!scratch.enabled or scratch.seen.count() == 0);
                return err;
            };
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}
