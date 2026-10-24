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

//! Portable per-incarnation visibility fences. A token is not authorization:
//! the host binds the authenticated physical table incarnation before use.
const std = @import("std");
pub const Token = struct {
    scope: [32]u8,
    lsn: u64,
    pub const text_len = 82;
    pub fn init(incarnation: []const u8, lsn: u64) Token {
        var scope: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(incarnation, &scope, .{});
        return .{ .scope = scope, .lsn = lsn };
    }
    pub fn encode(self: Token) [text_len]u8 {
        var raw: [41]u8 = undefined;
        raw[0] = 1;
        @memcpy(raw[1..33], &self.scope);
        std.mem.writeInt(u64, raw[33..41], self.lsn, .big);
        return std.fmt.bytesToHex(raw, .lower);
    }
    pub fn decode(text: []const u8) !Token {
        if (text.len != text_len) return error.InvalidVisibilityToken;
        var raw: [41]u8 = undefined;
        _ = std.fmt.hexToBytes(&raw, text) catch return error.InvalidVisibilityToken;
        if (raw[0] != 1) return error.InvalidVisibilityToken;
        return .{ .scope = raw[1..33].*, .lsn = std.mem.readInt(u64, raw[33..41], .big) };
    }
    pub fn require(self: Token, incarnation: []const u8, visible_lsn: u64) !void {
        const bound = init(incarnation, visible_lsn);
        if (!std.mem.eql(u8, &self.scope, &bound.scope)) return error.VisibilityIncarnationMismatch;
        if (self.lsn > visible_lsn) return error.WalVisibilityUnavailable;
    }
};

pub const Cut = struct {
    published_lsn: u64,
    visible_lsn: u64,
    pub fn capture(incarnation: []const u8, published_lsn: u64, committed_lsn: u64, required: ?Token) !Cut {
        if (published_lsn > committed_lsn) return error.InvalidWal;
        if (required) |token| try token.require(incarnation, committed_lsn);
        return .{ .published_lsn = published_lsn, .visible_lsn = committed_lsn };
    }
};

test "serverless visibility tokens bind an incarnation and a committed prefix without JS number precision loss" {
    const token = Token.init("tenant/table/incarnation-1", 9007199254740993);
    const decoded = try Token.decode(&token.encode());
    try std.testing.expectEqual(token.lsn, decoded.lsn);
    try decoded.require("tenant/table/incarnation-1", token.lsn);
    try std.testing.expectError(error.VisibilityIncarnationMismatch, decoded.require("tenant/table/incarnation-2", token.lsn));
    try std.testing.expectError(error.WalVisibilityUnavailable, decoded.require("tenant/table/incarnation-1", token.lsn - 1));
    try std.testing.expectError(error.InvalidVisibilityToken, Token.decode("garbage"));
}

/// An opaque resume position, not an authorization credential. The host still
/// authorizes every page and acquires the referenced generation's read lease.
pub const Cursor = struct {
    scope: [32]u8,
    query: [32]u8,
    generation: u64,
    cutoff: u64,
    after: []const u8,
    const header_bytes = 85;
    const max_id_bytes = 256 * 1024;
    pub fn encode(self: Cursor, alloc: std.mem.Allocator) ![]u8 {
        if (self.after.len > max_id_bytes) return error.InvalidQueryCursor;
        const raw = try alloc.alloc(u8, header_bytes + self.after.len);
        defer alloc.free(raw);
        raw[0] = 1;
        @memcpy(raw[1..33], &self.scope);
        @memcpy(raw[33..65], &self.query);
        std.mem.writeInt(u64, raw[65..73], self.generation, .big);
        std.mem.writeInt(u64, raw[73..81], self.cutoff, .big);
        std.mem.writeInt(u32, raw[81..85], @intCast(self.after.len), .big);
        @memcpy(raw[85..], self.after);
        const encoder = std.base64.url_safe_no_pad.Encoder;
        const text = try alloc.alloc(u8, encoder.calcSize(raw.len));
        _ = encoder.encode(text, raw);
        return text;
    }
    /// Returned after-key borrows raw_storage, which the caller frees once.
    pub fn decode(text: []const u8, alloc: std.mem.Allocator, incarnation: []const u8, query: [32]u8) !struct { cursor: Cursor, raw_storage: []u8 } {
        const decoder = std.base64.url_safe_no_pad.Decoder;
        const len = decoder.calcSizeForSlice(text) catch return error.InvalidQueryCursor;
        if (len < header_bytes or len > header_bytes + max_id_bytes) return error.InvalidQueryCursor;
        const raw = try alloc.alloc(u8, len);
        errdefer alloc.free(raw);
        decoder.decode(raw, text) catch return error.InvalidQueryCursor;
        if (raw[0] != 1 or std.mem.readInt(u32, raw[81..85], .big) != len - header_bytes) return error.InvalidQueryCursor;
        const expected = Token.init(incarnation, 0);
        if (!std.mem.eql(u8, raw[1..33], &expected.scope) or !std.mem.eql(u8, raw[33..65], &query)) return error.QueryCursorBindingMismatch;
        return .{ .cursor = .{ .scope = raw[1..33].*, .query = query, .generation = std.mem.readInt(u64, raw[65..73], .big), .cutoff = std.mem.readInt(u64, raw[73..81], .big), .after = raw[85..] }, .raw_storage = raw };
    }
};

test "serverless query cursors bind incarnation, query and an exact generation/cutoff" {
    const a = std.testing.allocator;
    const token = Token.init("table/1", 99);
    const cursor = Cursor{ .scope = token.scope, .query = @splat(1), .generation = 7, .cutoff = 99, .after = "document-z" };
    const encoded = try cursor.encode(a);
    defer a.free(encoded);
    const decoded = try Cursor.decode(encoded, a, "table/1", @splat(1));
    defer a.free(decoded.raw_storage);
    try std.testing.expectEqual(@as(u64, 7), decoded.cursor.generation);
    try std.testing.expectEqualStrings("document-z", decoded.cursor.after);
    try std.testing.expectError(error.QueryCursorBindingMismatch, Cursor.decode(encoded, a, "table/2", @splat(1)));
    try std.testing.expectError(error.QueryCursorBindingMismatch, Cursor.decode(encoded, a, "table/1", @splat(2)));
}
