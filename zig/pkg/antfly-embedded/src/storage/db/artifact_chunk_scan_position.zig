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

//! Receiver-local cursor position, not an input or completion certificate.
//! Both merge inputs must resume: a member key alone loses empty-head and
//! ignored-legacy progress. Empty keys denote exhausted inputs.
const std = @import("std");
pub const max_key_bytes = 1024 * 1024;
pub const max_encoded_bytes = 49 + 2 * max_key_bytes;
pub const Position = struct {
    head: []const u8,
    legacy: []const u8,
    ordinal: u32 = 0,
    members_open: bool = false,
    advance_legacy: bool = false,

    fn valid(self: Position) bool {
        return self.head.len <= max_key_bytes and self.legacy.len <= max_key_bytes and
            (self.head.len != 0 or (!self.members_open and self.ordinal == 0)) and
            (self.members_open or self.ordinal == 0) and (self.legacy.len != 0 or !self.advance_legacy);
    }
    pub fn encodeAlloc(self: Position, alloc: std.mem.Allocator) ![]u8 {
        if (!self.valid()) return error.InvalidBatchRequest;
        const raw = try alloc.alloc(u8, 49 + self.head.len + self.legacy.len);
        @memcpy(raw[0..4], "ALC1");
        std.mem.writeInt(u32, raw[4..8], @intCast(self.head.len), .little);
        std.mem.writeInt(u32, raw[8..12], @intCast(self.legacy.len), .little);
        std.mem.writeInt(u32, raw[12..16], self.ordinal, .little);
        raw[16] = @as(u8, @intFromBool(self.members_open)) | (@as(u8, @intFromBool(self.advance_legacy)) << 1);
        @memcpy(raw[17 .. 17 + self.head.len], self.head);
        @memcpy(raw[17 + self.head.len .. raw.len - 32], self.legacy);
        std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], raw[raw.len - 32 ..][0..32], .{});
        return raw;
    }
    pub fn decode(raw: []const u8) !Position {
        if (raw.len < 49 or raw.len > max_encoded_bytes or !std.mem.eql(u8, raw[0..4], "ALC1") or raw[16] > 3) return error.ArtifactCatalogCorrupt;
        const head_len: usize = std.mem.readInt(u32, raw[4..8], .little);
        const legacy_len: usize = std.mem.readInt(u32, raw[8..12], .little);
        if (head_len > max_key_bytes or legacy_len > max_key_bytes or raw.len != 49 + head_len + legacy_len) return error.ArtifactCatalogCorrupt;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw[0 .. raw.len - 32], &digest, .{});
        if (!std.mem.eql(u8, &digest, raw[raw.len - 32 ..])) return error.ArtifactCatalogCorrupt;
        const result: Position = .{ .head = raw[17 .. 17 + head_len], .legacy = raw[17 + head_len .. raw.len - 32], .ordinal = std.mem.readInt(u32, raw[12..16], .little), .members_open = raw[16] & 1 != 0, .advance_legacy = raw[16] & 2 != 0 };
        if (!result.valid()) return error.ArtifactCatalogCorrupt;
        return result;
    }
    fn orderKey(a: []const u8, b: []const u8) std.math.Order {
        if (a.len == 0) return if (b.len == 0) .eq else .gt;
        if (b.len == 0) return .lt;
        return std.mem.order(u8, a, b);
    }
    /// Neither merge input may rewind, and at least one must move forward.
    pub fn advances(self: Position, old: Position) bool {
        var head = orderKey(self.head, old.head);
        if (head == .eq) head = std.math.order(@as(u64, self.ordinal) * 2 + @intFromBool(self.members_open), @as(u64, old.ordinal) * 2 + @intFromBool(old.members_open));
        var legacy = orderKey(self.legacy, old.legacy);
        if (legacy == .eq) legacy = std.math.order(@intFromBool(self.advance_legacy), @intFromBool(old.advance_legacy));
        return head != .lt and legacy != .lt and (head == .gt or legacy == .gt);
    }
};

test "ordered artifact inventory logical scan positions own binary merge progress and reject rewind" {
    const alloc = std.testing.allocator;
    const position: Position = .{ .head = "head\x00\xff", .legacy = "legacy\x80", .members_open = true, .ordinal = 3 };
    const Check = struct {
        fn run(a: std.mem.Allocator, value: Position) !void {
            const raw = try value.encodeAlloc(a);
            defer a.free(raw);
            try std.testing.expectEqualDeep(value, try Position.decode(raw));
            for (0..raw.len) |index| {
                raw[index] ^= 1;
                try std.testing.expectError(error.ArtifactCatalogCorrupt, Position.decode(raw));
                raw[index] ^= 1;
            }
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{position});
    try std.testing.expect(!position.advances(position));
    var next = position;
    next.ordinal += 1;
    try std.testing.expect(next.advances(position));
    try std.testing.expect(!position.advances(next));
    next.legacy = "earlier";
    try std.testing.expect(!next.advances(position));
    next = position;
    next.advance_legacy = true;
    try std.testing.expect(next.advances(position));
    next = .{ .head = "", .legacy = "" };
    try std.testing.expect(next.advances(position));
    try std.testing.expect(!position.advances(next));
    next.members_open = true;
    try std.testing.expectError(error.InvalidBatchRequest, next.encodeAlloc(alloc));
}
