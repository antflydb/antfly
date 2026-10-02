// Copyright 2026 Antfly, Inc.
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

const std = @import("std");
const Allocator = std.mem.Allocator;
const token = @import("token.zig");

pub const Symbol = struct {
    hash: token.Hash128,
    canonical: []const u8,
};

pub const id_len = 16;
pub const Id = [id_len]u8;

pub fn id(canonical: []const u8) Id {
    const hash = token.hash128(canonical);
    var out: Id = undefined;
    std.mem.writeInt(u64, out[0..8], hash.hi, .big);
    std.mem.writeInt(u64, out[8..16], hash.lo, .big);
    return out;
}

pub fn idAlloc(alloc: Allocator, canonical: []const u8) ![]u8 {
    const raw = id(canonical);
    return try alloc.dupe(u8, raw[0..]);
}

pub fn idTextAlloc(alloc: Allocator, id_bytes: []const u8) ![]u8 {
    if (id_bytes.len != id_len) return error.InvalidSymbolId;
    const out = try alloc.alloc(u8, id_bytes.len * 2);
    const alphabet = "0123456789abcdef";
    for (id_bytes, 0..) |byte, i| {
        out[i * 2] = alphabet[byte >> 4];
        out[i * 2 + 1] = alphabet[byte & 0x0f];
    }
    return out;
}

pub fn idFromTextAlloc(alloc: Allocator, text: []const u8) ![]u8 {
    if (text.len != id_len * 2) return error.InvalidSymbolId;
    const out = try alloc.alloc(u8, id_len);
    errdefer alloc.free(out);
    var i: usize = 0;
    while (i < id_len) : (i += 1) {
        out[i] = (try hexValue(text[i * 2]) << 4) | try hexValue(text[i * 2 + 1]);
    }
    return out;
}

fn hexValue(byte: u8) !u8 {
    return switch (byte) {
        '0'...'9' => byte - '0',
        'a'...'f' => byte - 'a' + 10,
        'A'...'F' => byte - 'A' + 10,
        else => error.InvalidSymbolId,
    };
}

pub fn hashTextAlloc(alloc: Allocator, canonical: []const u8) ![]u8 {
    const hash = token.hash128(canonical);
    return try std.fmt.allocPrint(alloc, "{x:0>16}{x:0>16}", .{ hash.hi, hash.lo });
}

pub fn fromCanonical(canonical: []const u8) Symbol {
    return .{
        .hash = token.hash128(canonical),
        .canonical = canonical,
    };
}

pub fn shardMergeKeyAlloc(alloc: Allocator, canonical: []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{ "sym", canonical });
}

pub fn shardMergeKeyFromResolvedSymbolAlloc(alloc: Allocator, local_id: []const u8, canonical: []const u8) ![]u8 {
    if (local_id.len != id_len) return error.InvalidSymbolId;
    return try shardMergeKeyAlloc(alloc, canonical);
}

test "symbol hashes preserve canonical lookup identity" {
    const alloc = std.testing.allocator;
    const a = try token.canonicalTupleAlloc(alloc, &.{ "s", "alice" });
    defer alloc.free(a);
    const b = try token.canonicalTupleAlloc(alloc, &.{ "s", "bob" });
    defer alloc.free(b);

    const a_hash = try hashTextAlloc(alloc, a);
    defer alloc.free(a_hash);
    const a_hash_again = try hashTextAlloc(alloc, a);
    defer alloc.free(a_hash_again);
    const b_hash = try hashTextAlloc(alloc, b);
    defer alloc.free(b_hash);

    try std.testing.expectEqualStrings(a_hash, a_hash_again);
    try std.testing.expect(!std.mem.eql(u8, a_hash, b_hash));
}

test "symbol ids are stable binary hash coordinates" {
    const alloc = std.testing.allocator;
    const a = try token.canonicalTupleAlloc(alloc, &.{ "s", "alice" });
    defer alloc.free(a);
    const b = try token.canonicalTupleAlloc(alloc, &.{ "s", "bob" });
    defer alloc.free(b);

    const a_id = id(a);
    const a_id_again = id(a);
    const b_id = id(b);

    try std.testing.expectEqualSlices(u8, a_id[0..], a_id_again[0..]);
    try std.testing.expect(!std.mem.eql(u8, a_id[0..], b_id[0..]));
    try std.testing.expectEqual(@as(usize, id_len), a_id.len);

    const text = try idTextAlloc(alloc, a_id[0..]);
    defer alloc.free(text);
    try std.testing.expectEqual(@as(usize, id_len * 2), text.len);
    const decoded = try idFromTextAlloc(alloc, text);
    defer alloc.free(decoded);
    try std.testing.expectEqualSlices(u8, a_id[0..], decoded);
}

test "symbol shard merge keys use canonical token semantics" {
    const alloc = std.testing.allocator;
    const canonical = try token.canonicalTupleAlloc(alloc, &.{ "s", "alice" });
    defer alloc.free(canonical);
    const local_id = id(canonical);

    const merge_key = try shardMergeKeyAlloc(alloc, canonical);
    defer alloc.free(merge_key);
    const merge_key_again = try shardMergeKeyAlloc(alloc, canonical);
    defer alloc.free(merge_key_again);
    const local_id_text = try idTextAlloc(alloc, local_id[0..]);
    defer alloc.free(local_id_text);

    try std.testing.expectEqualStrings(merge_key, merge_key_again);
    try std.testing.expect(std.mem.indexOf(u8, merge_key, canonical) != null);
    try std.testing.expect(std.mem.indexOf(u8, merge_key, local_id_text) == null);
}

test "symbol shard merge keys ignore shard local id assignment" {
    const alloc = std.testing.allocator;
    const canonical = try token.canonicalTupleAlloc(alloc, &.{ "s", "alice" });
    defer alloc.free(canonical);
    const local_a = [_]u8{1} ** id_len;
    const local_b = [_]u8{2} ** id_len;

    const merge_a = try shardMergeKeyFromResolvedSymbolAlloc(alloc, local_a[0..], canonical);
    defer alloc.free(merge_a);
    const merge_b = try shardMergeKeyFromResolvedSymbolAlloc(alloc, local_b[0..], canonical);
    defer alloc.free(merge_b);

    try std.testing.expectEqualStrings(merge_a, merge_b);
}
