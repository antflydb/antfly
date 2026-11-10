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

//! Large opaque transactions use the same authenticated page namespace and
//! upload epoch as their WAL record. No second log or untracked blob lifetime.
const std = @import("std");
const tree = @import("../graph_segment/page_tree.zig");
const codec = @import("record_codec.zig");
const A = std.mem.Allocator;
const Hash = std.crypto.hash.sha2.Sha256;
pub const max_bytes = 32 * 1024 * 1024;
const chunk_bytes = 256 * 1024;
const magic = "AFWPCH01";
pub const Reference = struct { version: u8 = 1, bytes: u32, sha256: [32]u8, root: tree.Ref };

pub fn reference(a: A, encoded: []const u8) !Reference {
    const parsed = try std.json.parseFromSlice(Reference, a, encoded, .{});
    defer parsed.deinit();
    const ref = parsed.value;
    if (ref.version != 1 or ref.bytes == 0 or ref.bytes > max_bytes or ref.root.records != (@as(usize, ref.bytes) + chunk_bytes - 1) / chunk_bytes) return error.InvalidWalPayload;
    try ref.root.validate();
    return ref;
}

pub fn store(a: A, pages: tree.Store, bytes: []const u8) ![]u8 {
    if (bytes.len == 0 or bytes.len > max_bytes) return error.WalRecordTooLarge;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    const count = (bytes.len + chunk_bytes - 1) / chunk_bytes;
    const changes = try ca.alloc(tree.Mutation, count);
    for (changes, 0..) |*change, i| {
        try pages.check(pages.ptr);
        const key = try ca.alloc(u8, 8);
        std.mem.writeInt(u64, key[0..8], i, .big);
        const start = i * chunk_bytes;
        const end = @min(bytes.len, start + chunk_bytes);
        const value = try ca.alloc(u8, magic.len + end - start);
        @memcpy(value[0..magic.len], magic);
        @memcpy(value[magic.len..], bytes[start..end]);
        change.* = .{ .key = key, .value = value };
    }
    const root = (try tree.apply(a, pages, null, changes)) orelse return error.InvalidWalPayload;
    var digest: [32]u8 = undefined;
    Hash.hash(bytes, &digest, .{});
    return std.json.Stringify.valueAlloc(a, Reference{ .bytes = @intCast(bytes.len), .sha256 = digest, .root = root }, .{});
}

pub fn load(a: A, pages: tree.Store, encoded: []const u8, limit: usize) ![]u8 {
    const ref = try reference(a, encoded);
    if (ref.bytes > limit) return error.WalReadBudgetExceeded;
    const out = try a.alloc(u8, ref.bytes);
    errdefer a.free(out);
    var cursor = try tree.Cursor.init(a, pages, ref.root, "", null);
    defer cursor.deinit();
    var position: usize = 0;
    var ordinal: u64 = 0;
    while (try cursor.next()) |entry| {
        try pages.check(pages.ptr);
        if (entry.key.len != 8 or std.mem.readInt(u64, entry.key[0..8], .big) != ordinal or !std.mem.startsWith(u8, entry.value, magic)) return error.InvalidWalPayload;
        const chunk = entry.value[magic.len..];
        const expected = @min(chunk_bytes, out.len - position);
        if (chunk.len != expected or expected == 0) return error.InvalidWalPayload;
        @memcpy(out[position..][0..chunk.len], chunk);
        position += chunk.len;
        ordinal += 1;
    }
    if (position != out.len) return error.InvalidWalPayload;
    var digest: [32]u8 = undefined;
    Hash.hash(out, &digest, .{});
    if (!std.mem.eql(u8, &digest, &ref.sha256)) return error.ArtifactIntegrityMismatch;
    return out;
}

/// All collectors must traverse both structural edges and external record
/// payload roots. Chunk leaves themselves carry no further references.
pub fn traceChild(a: A, key: tree.KeyView, entry: tree.DecodedEntry) !?tree.Ref {
    if (entry.child) |ref| return ref.*;
    // Only record LSNs can contain payload descriptors. Reconstruct this one
    // fixed-size logical key, never an entire page of compressed receipt IDs.
    if (key.len() != 8) return null;
    var lsn: [8]u8 = undefined;
    key.copy(&lsn);
    return child(a, &lsn, entry.value);
}

/// Payload roots occur only in actual WAL record leaves. Chunk values carry
/// distinct fixed magic and cannot be mistaken for WAL records.
pub fn child(a: A, key: []const u8, value: []const u8) !?tree.Ref {
    if (key.len != 8) return null;
    const record = codec.parseRecord(value, 0) catch return null;
    if (record.next_cursor != value.len or record.lsn != std.mem.readInt(u64, key[0..8], .big) or !record.external_payload) return null;
    return (try reference(a, record.payload)).root;
}
