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

//! Binary batch wire format for `antfly_db_batch_wire`, alongside the JSON
//! `antfly_db_batch_json` path in `api/batch.zig`. Same wire-codec shape as
//! `capi/search_wire.zig` (fixed-width little-endian header, length-prefixed
//! byte strings), but scoped to exactly what a batch needs: writes, deletes,
//! transforms and the sync-level option. Graph writes/deletes stay JSON-only.
//!
//! Format (all integers little-endian, fixed width):
//!   magic: u32 = 0x41464257 ("AFBW")
//!   version: u16 = 1 or 2
//!   sync_level: u8 (0=propose 1=write 2=full_text 3=enrichments 4=full_index)
//!   reserved: u8 = 0
//!   write_count: u32
//!   delete_count: u32
//!   write_count * { key_len: u32, key: [key_len]u8, value_len: u32, value: [value_len]u8 }
//!   delete_count * { key_len: u32, key: [key_len]u8 }
//!   version 2 only: transforms_len: u32, transforms: [transforms_len]u8
//!
//! Version 2 transforms are the public batch `transforms` JSON array, with
//! per-transform `expected_version`, parsed by the same codec as the JSON
//! entry point so both accept exactly the same transforms (including
//! artifact-seed paths and `_seed_conflict`). A zero length means none.
//!
//! `decodeBatchRequest` is zero-copy: every write/delete key and write value
//! borrows directly from the caller's `buf` rather than being duplicated.
//! That is safe under the same ABI contract `antfly_slice` already documents
//! ("valid for the duration of the call"): the borrowed bytes are read-only
//! input to one synchronous batch commit and are never retained past it --
//! the storage layer copies any value it keeps. Only the `writes`/`deletes`
//! index arrays themselves are allocated, so `DecodedBatch.deinit` frees
//! exactly those two arrays and nothing per-entry.
const std = @import("std");
const mutation = @import("../storage/db/mutation_values.zig");
const batch_api = @import("../api/batch.zig");

pub const magic: u32 = 0x41464257; // "AFBW"
pub const version: u16 = 1;
/// Adds the transforms section.
pub const transforms_version: u16 = 2;

pub const DecodedBatch = struct {
    writes: []mutation.BatchWrite = &.{},
    deletes: [][]const u8 = &.{},
    sync_level: mutation.SyncLevel = .propose,
    /// Version 2 transforms, parsed (and owned) by the public batch codec.
    transforms: ?batch_api.OwnedBatchRequest = null,

    pub fn deinit(self: *DecodedBatch, alloc: std.mem.Allocator) void {
        if (self.writes.len != 0) alloc.free(self.writes);
        if (self.deletes.len != 0) alloc.free(self.deletes);
        if (self.transforms) |*owned| owned.deinit(alloc);
        self.* = undefined;
    }

    pub fn asRequest(self: DecodedBatch) mutation.PublicBatchRequest {
        return .{
            .writes = self.writes,
            .deletes = self.deletes,
            .transforms = if (self.transforms) |owned| owned.transforms else &.{},
            .sync_level = self.sync_level,
        };
    }

    /// Per-transform `expected_version` preconditions.
    pub fn expectedVersions(self: DecodedBatch) []const batch_api.ExpectedVersion {
        return if (self.transforms) |owned| owned.expected_versions else &.{};
    }
};

pub fn decodeBatchRequest(alloc: std.mem.Allocator, buf: []const u8) !DecodedBatch {
    var reader = Reader{ .buf = buf };
    const wire_version = try reader.expectMagicAndVersion();
    const sync_level = syncLevelFromTag(try reader.readInt(u8)) orelse return error.InvalidArgument;
    _ = try reader.readInt(u8); // reserved
    const write_count = try reader.readInt(u32);
    const delete_count = try reader.readInt(u32);
    // A write needs at least its two length prefixes and a delete its one,
    // so counts whose combined minimum encoding exceeds the bytes left can
    // never be satisfied; reject before allocating the index arrays instead
    // of trusting a caller-controlled count.
    const minimum_bytes = @as(u64, write_count) * 8 + @as(u64, delete_count) * 4;
    if (minimum_bytes > buf.len - reader.pos) return error.InvalidArgument;

    const writes = try alloc.alloc(mutation.BatchWrite, write_count);
    errdefer alloc.free(writes);
    for (writes) |*write| {
        const key = try reader.readBytes(try reader.readInt(u32));
        const value = try reader.readBytes(try reader.readInt(u32));
        if (batch_api.isReservedControlKey(key)) return error.InvalidArgument;
        write.* = .{ .key = key, .value = value };
    }

    const deletes = try alloc.alloc([]const u8, delete_count);
    errdefer alloc.free(deletes);
    for (deletes) |*del| {
        const key = try reader.readBytes(try reader.readInt(u32));
        if (batch_api.isReservedControlKey(key)) return error.InvalidArgument;
        del.* = key;
    }
    var transforms: ?batch_api.OwnedBatchRequest = null;
    errdefer if (transforms) |*owned| owned.deinit(alloc);
    if (wire_version == transforms_version) {
        const json = try reader.readBytes(try reader.readInt(u32));
        if (json.len != 0) {
            const body = try std.mem.concat(alloc, u8, &.{ "{\"transforms\":", json, "}" });
            defer alloc.free(body);
            transforms = batch_api.parseBatchRequest(alloc, body) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return error.InvalidArgument,
            };
        }
    }
    if (!reader.done()) return error.InvalidArgument;
    return .{ .writes = writes, .deletes = deletes, .sync_level = sync_level, .transforms = transforms };
}

fn syncLevelFromTag(tag: u8) ?mutation.SyncLevel {
    return switch (tag) {
        0 => .propose,
        1 => .write,
        2 => .full_text,
        3 => .enrichments,
        4 => .full_index,
        else => null,
    };
}

pub fn syncLevelTag(level: mutation.SyncLevel) u8 {
    return switch (level) {
        .propose => 0,
        .write => 1,
        .full_text => 2,
        .enrichments => 3,
        .full_index => 4,
    };
}

const Reader = struct {
    buf: []const u8,
    pos: usize = 0,

    fn expectMagicAndVersion(self: *Reader) !u16 {
        if (try self.readInt(u32) != magic) return error.InvalidArgument;
        const found = try self.readInt(u16);
        if (found != version and found != transforms_version) return error.InvalidArgument;
        return found;
    }

    fn readBytes(self: *Reader, len: usize) ![]const u8 {
        if (self.pos + len > self.buf.len) return error.InvalidArgument;
        defer self.pos += len;
        return self.buf[self.pos..][0..len];
    }

    fn readInt(self: *Reader, comptime T: type) !T {
        const bytes = try self.readBytes(@sizeOf(T));
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .little);
    }

    fn done(self: *Reader) bool {
        return self.pos == self.buf.len;
    }
};

fn writeInt(buf: []u8, cursor: *usize, comptime T: type, value: T) void {
    std.mem.writeInt(T, buf[cursor.*..][0..@sizeOf(T)], value, .little);
    cursor.* += @sizeOf(T);
}

/// Test/bench helper: encode writes/deletes into the wire format this module
/// decodes. Not used by the C API itself (callers build this buffer directly).
pub fn encodeBatchRequestAlloc(
    alloc: std.mem.Allocator,
    writes: []const mutation.BatchWrite,
    deletes: []const []const u8,
    sync_level: mutation.SyncLevel,
) ![]u8 {
    var total: usize = 4 + 2 + 1 + 1 + 4 + 4;
    for (writes) |write| total += 4 + write.key.len + 4 + write.value.len;
    for (deletes) |key| total += 4 + key.len;
    const out = try alloc.alloc(u8, total);
    errdefer alloc.free(out);
    var cursor: usize = 0;
    writeInt(out, &cursor, u32, magic);
    writeInt(out, &cursor, u16, version);
    writeInt(out, &cursor, u8, syncLevelTag(sync_level));
    writeInt(out, &cursor, u8, 0);
    writeInt(out, &cursor, u32, @intCast(writes.len));
    writeInt(out, &cursor, u32, @intCast(deletes.len));
    for (writes) |write| {
        writeInt(out, &cursor, u32, @intCast(write.key.len));
        @memcpy(out[cursor..][0..write.key.len], write.key);
        cursor += write.key.len;
        writeInt(out, &cursor, u32, @intCast(write.value.len));
        @memcpy(out[cursor..][0..write.value.len], write.value);
        cursor += write.value.len;
    }
    for (deletes) |key| {
        writeInt(out, &cursor, u32, @intCast(key.len));
        @memcpy(out[cursor..][0..key.len], key);
        cursor += key.len;
    }
    std.debug.assert(cursor == total);
    return out;
}

/// Test/bench helper: the version 2 encoding, with `transforms_json` (the
/// public batch `transforms` array) after the deletes.
pub fn encodeBatchRequestWithTransformsAlloc(
    alloc: std.mem.Allocator,
    writes: []const mutation.BatchWrite,
    deletes: []const []const u8,
    transforms_json: []const u8,
    sync_level: mutation.SyncLevel,
) ![]u8 {
    const v1 = try encodeBatchRequestAlloc(alloc, writes, deletes, sync_level);
    defer alloc.free(v1);
    const out = try alloc.alloc(u8, v1.len + 4 + transforms_json.len);
    @memcpy(out[0..v1.len], v1);
    std.mem.writeInt(u16, out[4..6], transforms_version, .little);
    var cursor = v1.len;
    writeInt(out, &cursor, u32, @intCast(transforms_json.len));
    @memcpy(out[cursor..][0..transforms_json.len], transforms_json);
    return out;
}

test "batch wire version 2 carries transforms with expected versions and seed paths" {
    const alloc = std.testing.allocator;
    const writes = [_]mutation.BatchWrite{.{ .key = "doc:a", .value = "{\"x\":1}" }};
    const transforms =
        \\[{"key":"doc:b","expected_version":"7","operations":[{"op":"$set","path":"_artifacts.summary","value":{"value":"s"}}]},
        \\ {"key":"doc:c","operations":[{"op":"$inc","path":"$.n","value":1}]}]
    ;
    const buf = try encodeBatchRequestWithTransformsAlloc(alloc, &writes, &.{}, transforms, .write);
    defer alloc.free(buf);
    var decoded = try decodeBatchRequest(alloc, buf);
    defer decoded.deinit(alloc);
    const req = decoded.asRequest();
    try std.testing.expectEqual(@as(usize, 1), req.writes.len);
    try std.testing.expectEqual(@as(usize, 2), req.transforms.len);
    try std.testing.expectEqualStrings("_artifacts.summary", req.transforms[0].operations[0].path);
    try std.testing.expectEqual(@as(usize, 1), decoded.expectedVersions().len);
    try std.testing.expectEqual(@as(u64, 7), decoded.expectedVersions()[0].version);

    // The same transform rules as the JSON entry point: a seed-only
    // transform without `expected_version` is rejected.
    const unbound = try encodeBatchRequestWithTransformsAlloc(alloc, &.{}, &.{},
        \\[{"key":"doc:b","operations":[{"op":"$set","path":"_artifacts.summary","value":{"value":"s"}}]}]
    , .write);
    defer alloc.free(unbound);
    try std.testing.expectError(error.InvalidArgument, decodeBatchRequest(alloc, unbound));
    // An empty section is no transforms; a truncated one is malformed.
    const none = try encodeBatchRequestWithTransformsAlloc(alloc, &writes, &.{}, "", .write);
    defer alloc.free(none);
    var plain = try decodeBatchRequest(alloc, none);
    defer plain.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), plain.asRequest().transforms.len);
    try std.testing.expectError(error.InvalidArgument, decodeBatchRequest(alloc, buf[0 .. buf.len - 1]));
}

test "batch wire round-trips writes, deletes and sync level" {
    const alloc = std.testing.allocator;
    const writes = [_]mutation.BatchWrite{
        .{ .key = "doc:a", .value = "{\"x\":1}" },
        .{ .key = "doc:b", .value = "{\"x\":2}" },
    };
    const deletes = [_][]const u8{"doc:c"};
    const buf = try encodeBatchRequestAlloc(alloc, &writes, &deletes, .full_index);
    defer alloc.free(buf);

    var decoded = try decodeBatchRequest(alloc, buf);
    defer decoded.deinit(alloc);
    try std.testing.expectEqual(mutation.SyncLevel.full_index, decoded.sync_level);
    try std.testing.expectEqual(@as(usize, 2), decoded.writes.len);
    try std.testing.expectEqualStrings("doc:a", decoded.writes[0].key);
    try std.testing.expectEqualStrings("{\"x\":1}", decoded.writes[0].value);
    try std.testing.expectEqualStrings("doc:b", decoded.writes[1].key);
    try std.testing.expectEqualStrings("{\"x\":2}", decoded.writes[1].value);
    try std.testing.expectEqual(@as(usize, 1), decoded.deletes.len);
    try std.testing.expectEqualStrings("doc:c", decoded.deletes[0]);

    // Decoded byte slices borrow the input buffer (zero-copy): the key's
    // backing pointer is exactly the input buffer's, offset past the
    // header (16 bytes) and its own key_len prefix (4 bytes) -- not a copy.
    const probe = try decodeBatchRequest(alloc, buf);
    try std.testing.expectEqual(buf[16 + 4 ..].ptr, probe.writes[0].key.ptr);
    alloc.free(probe.writes);
    alloc.free(probe.deletes);
}

test "batch wire rejects counts larger than the body could encode before allocating" {
    // Header plus one write's worth of bytes, claiming a million writes.
    // magic u32, version u16, sync u8, reserved u8, write and delete counts.
    var buf: [16 + 8]u8 = @splat(0);
    std.mem.writeInt(u32, buf[0..4], magic, .little);
    std.mem.writeInt(u16, buf[4..6], version, .little);
    buf[6] = syncLevelTag(.write);
    std.mem.writeInt(u32, buf[8..12], 1_000_000, .little);
    // A failing allocator: any index-array allocation fails the test.
    try std.testing.expectError(error.InvalidArgument, decodeBatchRequest(std.testing.failing_allocator, &buf));
    // Both counts count: 1 write (8 bytes) plus 1 delete (4 bytes) > 8.
    std.mem.writeInt(u32, buf[8..12], 1, .little);
    std.mem.writeInt(u32, buf[12..16], 1, .little);
    try std.testing.expectError(error.InvalidArgument, decodeBatchRequest(std.testing.failing_allocator, &buf));
}

test "batch wire rejects a bad magic, version, truncated body and reserved keys" {
    const alloc = std.testing.allocator;
    const writes = [_]mutation.BatchWrite{.{ .key = "doc:a", .value = "{}" }};
    const buf = try encodeBatchRequestAlloc(alloc, &writes, &.{}, .write);
    defer alloc.free(buf);

    var bad_magic = try alloc.dupe(u8, buf);
    defer alloc.free(bad_magic);
    bad_magic[0] = ~bad_magic[0];
    try std.testing.expectError(error.InvalidArgument, decodeBatchRequest(alloc, bad_magic));

    var bad_version = try alloc.dupe(u8, buf);
    defer alloc.free(bad_version);
    bad_version[4] = 99;
    try std.testing.expectError(error.InvalidArgument, decodeBatchRequest(alloc, bad_version));

    try std.testing.expectError(error.InvalidArgument, decodeBatchRequest(alloc, buf[0 .. buf.len - 1]));

    const reserved_writes = [_]mutation.BatchWrite{.{ .key = "\x00\x00__metadata__:graph_owner_replay:v1:x", .value = "{}" }};
    const reserved_buf = try encodeBatchRequestAlloc(alloc, &reserved_writes, &.{}, .write);
    defer alloc.free(reserved_buf);
    try std.testing.expectError(error.InvalidArgument, decodeBatchRequest(alloc, reserved_buf));
}
