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

//! Fixed receiver-local slots bound abandoned spool space by the largest row
//! actually received, not by the number of attempts. A new attempt overwrites
//! slots; their transfer header prevents mixing old and new row contents.
const std = @import("std");
const pages = @import("merge_page_contract.zig");
const types = @import("types.zig");
const KV = @import("../docstore.zig").KVPair;
pub const manifest_key = "\x00\x00__metadata__:raftmerge:chunks";
pub const slot_prefix = "\x00\x00__metadata__:raftmerge:chunk:";

pub const NativeReader = struct {
    txn: *@import("../docstore.zig").DocStore.Txn,
    pub fn get(self: *@This(), key: []const u8) ![]const u8 {
        return self.txn.get(key);
    }
    pub fn copyChunk(self: *@This(), key: []const u8, transfer: pages.Digest, destination: []u8) !void {
        if (self.txn.read) |*read| {
            var fork = try read.forkRead();
            defer fork.abort();
            try copyChecked(try fork.get(key), transfer, destination);
        } else try copyChecked(try self.txn.get(key), transfer, destination);
    }
};

pub fn copyChecked(stored: []const u8, transfer: pages.Digest, destination: []u8) !void {
    try validateSlot(stored);
    if (stored.len != 64 + destination.len or !std.mem.eql(u8, stored[0..32], &transfer)) return error.InvalidMergePage;
    @memcpy(destination, stored[64..]);
}

pub fn validateSlot(stored: []const u8) !void {
    if (stored.len != 64 + pages.chunk_bytes or !std.mem.eql(u8, &checksum(stored[64..]), stored[32..64])) return error.InvalidMergePage;
}

pub fn checksum(value: []const u8) pages.Digest {
    var result: pages.Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(value, &result, .{});
    return result;
}

pub fn slotKey(alloc: std.mem.Allocator, slot: u32) ![]u8 {
    const result = try alloc.alloc(u8, slot_prefix.len + 4);
    @memcpy(result[0..slot_prefix.len], slot_prefix);
    std.mem.writeInt(u32, result[slot_prefix.len..][0..4], slot, .big);
    return result;
}

pub fn slotCount(raw: []const u8) !u32 {
    if (raw.len != 4) return error.InvalidMergePage;
    const count = std.mem.readInt(u32, raw[0..4], .little);
    if (count > (std.math.divCeil(u64, pages.max_row_bytes, pages.chunk_bytes) catch unreachable)) return error.InvalidMergePage;
    return count;
}

pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    request: types.BatchRequest,
    expected_progress: ?pages.Digest = null,
    writes: []const KV = &.{},
    deletes: []const []const u8 = &.{},

    pub fn init(alloc: std.mem.Allocator, request: types.BatchRequest) Prepared {
        return .{ .arena = std.heap.ArenaAllocator.init(alloc), .request = request };
    }
    pub fn deinit(self: *Prepared) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Reader.get returns snapshot-owned bytes. The caller commits these effects
/// only with the exact progress digest below and its normal page state fold.
/// A final row is allocated once under the caller's preparation budget; chunks
/// are read one at a time, never accumulated into a second whole-row buffer.
pub fn prepare(alloc: std.mem.Allocator, reader: anytype, request: types.BatchRequest, cancellation: types.CancellationToken) !Prepared {
    try pages.validateRequest(request);
    var result = Prepared.init(alloc, request);
    errdefer result.deinit();
    const owned = result.arena.allocator();
    const chunk = request.merge_page.?.chunk orelse return error.InvalidMergePage;
    const raw_progress = try reader.get(pages.key);
    var progress = try pages.decode(owned, raw_progress);
    defer progress.deinit();
    switch (try pages.plan(progress.value, request)) {
        .replay => return result,
        .apply => {},
    }
    result.expected_progress = checksum(raw_progress);
    const raw_manifest = reader.get(manifest_key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
    const allocated_slots: u32 = if (raw_manifest) |raw| try slotCount(raw) else 0;
    const slot: u32 = @intCast(chunk.offset / pages.chunk_bytes);
    const transfer = pages.transferDigest(request);
    try cancellation.check();
    if (!chunk.complete()) {
        const encoded = try owned.alloc(u8, 64 + chunk.data.len);
        @memcpy(encoded[0..32], &transfer);
        @memcpy(encoded[32..64], &chunk.chunk_digest);
        @memcpy(encoded[64..], chunk.data);
        const manifest = try owned.alloc(u8, 4);
        std.mem.writeInt(u32, manifest[0..4], @max(allocated_slots, slot + 1), .little);
        const writes = try owned.alloc(KV, 2);
        writes[0] = .{ .key = try slotKey(owned, slot), .value = encoded };
        writes[1] = .{ .key = manifest_key, .value = manifest };
        result.writes = writes;
    } else {
        const row = try owned.alloc(u8, std.math.cast(usize, chunk.total_bytes) orelse return error.TransactionTooLarge);
        var index: u32 = 0;
        while (index < slot) : (index += 1) {
            try cancellation.check();
            const offset = @as(usize, index) * pages.chunk_bytes;
            try reader.copyChunk(try slotKey(owned, index), transfer, row[offset..][0..pages.chunk_bytes]);
        }
        @memcpy(row[@intCast(chunk.offset)..], chunk.data);
        if (!std.mem.eql(u8, &checksum(row), &chunk.row_digest)) return error.InvalidMergePage;
        const writes = try owned.alloc(types.BatchWrite, 1);
        writes[0] = .{ .key = chunk.row_key, .value = row };
        result.request.writes = writes;
        const deletes = try owned.alloc([]const u8, @as(usize, allocated_slots) + 1);
        for (deletes[0..allocated_slots], 0..) |*key, i| key.* = try slotKey(owned, @intCast(i));
        deletes[allocated_slots] = manifest_key;
        result.deletes = deletes;
    }
    try cancellation.check();
    return result;
}
