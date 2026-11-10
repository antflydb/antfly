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

//! Optional immutable-generation hint. The physical query-pin guard serializes
//! access; source/replay admission fences validate the current state separately.
const std = @import("std");
const Request = @import("native_query_cut_contract.zig").Request;
pub const Proof = struct { sequence: u64, digest: [32]u8, manifest_checksum: [32]u8 };
pub const State = struct {
    arena: std.heap.ArenaAllocator,
    indexes: ?[]const []const u8,
    id: [64]u8,
    table_id: u64,
    expires_ms: u64,
    sequence: u64,
    digest: [32]u8,
    manifest_checksum: [32]u8,

    pub fn init(a: std.mem.Allocator, request: Request, committed: Proof) !State {
        try request.validate(@import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms);
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const indexes = if (request.indexes) |names| copied: {
            const owned = try arena.allocator().alloc([]const u8, names.len);
            for (names, owned) |name, *item| item.* = try arena.allocator().dupe(u8, name);
            break :copied owned;
        } else null;
        return .{ .arena = arena, .indexes = indexes, .id = request.id[0..64].*, .table_id = request.table_id, .expires_ms = request.expires_ms, .sequence = committed.sequence, .digest = committed.digest, .manifest_checksum = committed.manifest_checksum };
    }
    pub fn deinit(self: *State) void {
        self.arena.deinit();
    }
    pub fn matchesSelection(self: State, request: Request) bool {
        if ((self.indexes == null) != (request.indexes == null)) return false;
        const left = self.indexes orelse return true;
        const right = request.indexes.?;
        if (left.len != right.len) return false;
        for (left, right) |x, y| if (!std.mem.eql(u8, x, y)) return false;
        return true;
    }
    pub fn proof(self: State) Proof {
        return .{ .sequence = self.sequence, .digest = self.digest, .manifest_checksum = self.manifest_checksum };
    }
    pub fn asRequest(self: *const State) Request {
        return .{ .id = &self.id, .table_id = self.table_id, .expires_ms = self.expires_ms, .create = true, .indexes = self.indexes };
    }
    pub fn matchesIndexes(self: State, indexes: ?[]const []const u8) bool {
        var request = self.asRequest();
        request.indexes = indexes;
        return self.matchesSelection(request);
    }
    pub fn asRequestWithIndexes(self: *const State, indexes: ?[]const []const u8) Request {
        std.debug.assert(self.matchesIndexes(indexes));
        var request = self.asRequest();
        request.indexes = indexes;
        return request;
    }
    pub fn matches(self: State, digest: [32]u8, now_ms: u64) bool {
        return self.expires_ms > now_ms and std.mem.eql(u8, &self.digest, &digest);
    }
};

test "external lake native reuse rejects state and expiry changes" {
    const id: [64]u8 = @splat('a');
    var state = try State.init(std.testing.allocator, .{ .id = &id, .table_id = 7, .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 300000, .create = true }, .{ .sequence = 123, .digest = @splat(42), .manifest_checksum = @splat(1) });
    defer state.deinit();
    try std.testing.expect(state.matches(@splat(42), state.expires_ms - 1));
    try std.testing.expect(!state.matches(@splat(43), state.expires_ms - 1));
    try std.testing.expect(!state.matches(@splat(42), state.expires_ms));
    try std.testing.expectEqualStrings(&id, state.asRequest().id);
}

test "external lake native reuse owns and preserves index selection" {
    const id: [64]u8 = @splat('b');
    var name = "body_text".*;
    const request: Request = .{ .id = &id, .table_id = 7, .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 300000, .create = true, .indexes = &.{&name} };
    var state = try State.init(std.testing.allocator, request, .{ .sequence = 1, .digest = @splat(1), .manifest_checksum = @splat(2) });
    defer state.deinit();
    @memset(&name, 'x');
    try std.testing.expectEqualStrings("body_text", state.asRequest().indexes.?[0]);
    var expected = request;
    expected.indexes = &.{"body_text"};
    try std.testing.expect(state.matchesSelection(expected));
    expected.indexes = null;
    try std.testing.expect(!state.matchesSelection(expected));
    expected.indexes = &.{"other"};
    try std.testing.expect(!state.matchesSelection(expected));
}

test "time series native reuse binds exact index selection" {
    const id: [64]u8 = @splat('a');
    var state = try State.init(std.testing.allocator, .{ .id = &id, .table_id = 7, .expires_ms = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms + 300000, .indexes = &.{ "a", "bc" } }, .{ .sequence = 1, .digest = @splat(1), .manifest_checksum = @splat(2) });
    defer state.deinit();
    try std.testing.expect(state.matchesIndexes(&.{ "a", "bc" }));
    try std.testing.expect(!state.matchesIndexes(null));
    try std.testing.expect(!state.matchesIndexes(&.{}));
    try std.testing.expect(!state.matchesIndexes(&.{ "ab", "c" }));
    try std.testing.expect(!state.matchesIndexes(&.{ "bc", "a" }));
    try std.testing.expect(state.asRequestWithIndexes(&.{ "a", "bc" }).indexesEqual(.{ .id = &id, .table_id = 7, .expires_ms = state.expires_ms, .indexes = &.{ "a", "bc" } }));
}
