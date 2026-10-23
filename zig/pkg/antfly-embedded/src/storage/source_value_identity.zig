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

//! Canonical JSON value identities shared by storage publication and delivery.
const std = @import("std");
const V = std.json.Value;
const HashWriter = struct {
    hash: std.crypto.hash.sha2.Sha256 = .init(.{}),
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },
    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *@This() = @alignCast(@fieldParentPtr("writer", w));
        var count: usize = 0;
        for (data[0 .. data.len - 1]) |bytes| {
            self.hash.update(bytes);
            count += bytes.len;
        }
        for (0..splat) |_| {
            const bytes = data[data.len - 1];
            self.hash.update(bytes);
            count += bytes.len;
        }
        return count;
    }
};
pub fn identity(value: V) ![68]u8 {
    var out: HashWriter = .{};
    out.hash.update("antfly.source.value.v1\x00");
    try std.json.Stringify.value(value, .{}, &out.writer);
    try out.writer.flush();
    var digest: [32]u8 = undefined;
    out.hash.final(&digest);
    return "sv1-".* ++ std.fmt.bytesToHex(digest, .lower);
}

const projection = @import("document_projection.zig");
const prefix = "\x00\x00__metadata__:source_value_identity_v1:";
pub const Key = [prefix.len + 32]u8;
// Disposable proofs must not survive a canonicalizer/compiler change. The
// public identity remains sv1; a mismatching producer ABI forces full-value
// delivery and bounded backfill instead of trusting an older implementation.
const canonical_abi: [32]u8 = abi: {
    @setEvalBranchQuota(2_000_000);
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(@import("builtin").zig_version_string);
    hash.update(@embedFile("source_value_identity.zig"));
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    break :abi digest;
};
const header_bytes = 111;
const record_bytes = 34;
pub const maximum_bytes = header_bytes + projection.maximum_fields * record_bytes;
pub fn keyForRevision(key: anytype) Key {
    var result: Key = undefined;
    @memcpy(result[0..prefix.len], prefix);
    @memcpy(result[prefix.len..], key[key.len - 32 ..]);
    return result;
}
/// Separate bounded metadata preserves the existing path directory's coverage
/// and admission budget. Leaf ranges are disjoint, so canonical hashing adds
/// linear write work; arrays and opaque subtrees retain full-value fallback.
pub fn build(a: std.mem.Allocator, body: []const u8, kind: u8, digest: [32]u8, directory: []const u8) !?[]u8 {
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    const temp = arena.allocator();
    const proof = .{ .bytes = @as(u64, body.len), .kind = kind, .digest = digest };
    const paths = (try projection.Directory.open(temp, directory, proof)) orelse return null;
    if (paths.version < 2) return null;
    const Record = struct { index: u16, digest: [32]u8 };
    var records: [projection.maximum_fields]Record = undefined;
    var count: usize = 0;
    for (paths.fields) |field| {
        if (field.kind != .leaf) continue;
        const raw = body[@intCast(field.offset)..][0..@intCast(field.length)];
        var parsed = std.json.parseFromSlice(V, temp, raw, .{}) catch |err| {
            if (err == error.OutOfMemory) return err;
            continue;
        };
        defer parsed.deinit();
        const token = try identity(parsed.value);
        records[count].index = field.index;
        _ = try std.fmt.hexToBytes(&records[count].digest, token[4..]);
        count += 1;
    }
    const output = try a.alloc(u8, header_bytes + count * record_bytes);
    @memcpy(output[0..4], "SVI1");
    std.mem.writeInt(u64, output[4..12], body.len, .little);
    @memcpy(output[12..44], &digest);
    output[44] = kind;
    std.crypto.hash.sha2.Sha256.hash(directory, output[45..77], .{});
    std.mem.writeInt(u16, output[77..79], @intCast(count), .little);
    @memcpy(output[79..111], &canonical_abi);
    for (records[0..count], 0..) |record, i| {
        const offset = header_bytes + i * record_bytes;
        std.mem.writeInt(u16, output[offset..][0..2], record.index, .little);
        @memcpy(output[offset + 2 ..][0..32], &record.digest);
    }
    return output;
}
pub const Certificate = struct {
    records: []const u8,
    pub fn open(raw: []const u8, directory: []const u8, proof: anytype) ?Certificate {
        if (raw.len < header_bytes or raw.len > maximum_bytes or !std.mem.eql(u8, raw[0..4], "SVI1")) return null;
        if (!std.mem.eql(u8, raw[79..111], &canonical_abi)) return null;
        if (std.mem.readInt(u64, raw[4..12], .little) != proof.bytes or raw[44] != proof.kind or !std.mem.eql(u8, raw[12..44], &proof.digest)) return null;
        var directory_digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(directory, &directory_digest, .{});
        if (!std.mem.eql(u8, raw[45..77], &directory_digest)) return null;
        const count = std.mem.readInt(u16, raw[77..79], .little);
        if (count > projection.maximum_fields or raw.len != header_bytes + @as(usize, count) * record_bytes) return null;
        var prior: ?u16 = null;
        for (0..count) |i| {
            const index = std.mem.readInt(u16, raw[header_bytes + i * record_bytes ..][0..2], .little);
            if (index >= projection.maximum_fields or (prior != null and index <= prior.?)) return null;
            prior = index;
        }
        return .{ .records = raw[header_bytes..] };
    }
    pub fn find(self: Certificate, index: u16) ?[32]u8 {
        var low: usize = 0;
        var high = self.records.len / record_bytes;
        while (low < high) {
            const mid = low + (high - low) / 2;
            const record = self.records[mid * record_bytes ..][0..record_bytes];
            const found = std.mem.readInt(u16, record[0..2], .little);
            if (found < index) low = mid + 1 else if (found > index) high = mid else return record[2..34].*;
        }
        return null;
    }
};
