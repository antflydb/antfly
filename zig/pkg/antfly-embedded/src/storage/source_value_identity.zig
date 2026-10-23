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
const domain = "antfly.source.value.v1\x00";
fn digestValue(value: V) ![32]u8 {
    var out: HashWriter = .{};
    out.hash.update(domain);
    try std.json.Stringify.value(value, .{}, &out.writer);
    try out.writer.flush();
    var digest: [32]u8 = undefined;
    out.hash.final(&digest);
    return digest;
}
pub fn identity(value: V) ![68]u8 {
    return "sv1-".* ++ std.fmt.bytesToHex(try digestValue(value), .lower);
}
/// Caller has already strictly validated the scalar JSON. Plain UTF-8 strings
/// are their own canonical encoding: hash borrowed bytes without allocating or
/// reserializing. Numbers and escaped strings use the typed reference.
/// Keep this fallback rather than guessing about floating-point normalization.
pub fn validatedScalarDigest(a: std.mem.Allocator, raw: []const u8) !?[32]u8 {
    const value = std.mem.trim(u8, raw, " \t\r\n");
    const plain = value.len >= 2 and value[0] == '"' and value[value.len - 1] == '"' and
        std.mem.indexOfScalar(u8, value, '\\') == null and std.unicode.utf8ValidateSlice(value);
    if (plain or std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "false") or std.mem.eql(u8, value, "null")) {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(domain);
        hash.update(value);
        var digest: [32]u8 = undefined;
        hash.final(&digest);
        return digest;
    }
    var parsed = std.json.parseFromSlice(V, a, value, .{}) catch |err| {
        if (err == error.OutOfMemory) return err;
        return null;
    };
    defer parsed.deinit();
    return try digestValue(parsed.value);
}
const projection = @import("document_projection.zig");
const prefix = "\x00\x00__metadata__:source_value_identity_v1:";
pub const Key = [prefix.len + 32]u8;
// Semantic ABI, deliberately independent of compiler version, comments, tests,
// lookup algorithms and directory layout. Bump ONLY when typed canonical JSON
// semantics change; golden vectors gate compiler upgrades. Unknown versions
// safely fall back and backfill. Public identities remain opaque sv1 tokens.
const canonical_abi: [32]u8 = abi: {
    @setEvalBranchQuota(100_000);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("antfly.source.canonical-json/1", &digest, .{});
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
    var collector: Collector = .{ .a = temp };
    for (paths.fields) |field| {
        if (field.kind == .leaf) try Collector.visit(&collector, field.index, body[@intCast(field.offset)..][0..@intCast(field.length)]);
    }
    return try collector.encode(a, body.len, kind, digest, directory);
}
const Record = struct { index: u16, digest: [32]u8 };
const Collector = struct {
    a: std.mem.Allocator,
    records: [projection.maximum_fields]Record = undefined,
    count: usize = 0,
    fn visit(raw: *anyopaque, index: u16, value: []const u8) !void {
        const self: *Collector = @ptrCast(@alignCast(raw));
        const digest = (try validatedScalarDigest(self.a, value)) orelse return;
        self.records[self.count] = .{ .index = index, .digest = digest };
        self.count += 1;
    }
    fn encode(self: *const Collector, a: std.mem.Allocator, bytes: usize, kind: u8, digest: [32]u8, directory: []const u8) ![]u8 {
        const count = self.count;
        const output = try a.alloc(u8, header_bytes + count * record_bytes);
        @memcpy(output[0..4], "SVI1");
        std.mem.writeInt(u64, output[4..12], bytes, .little);
        @memcpy(output[12..44], &digest);
        output[44] = kind;
        std.crypto.hash.sha2.Sha256.hash(directory, output[45..77], .{});
        std.mem.writeInt(u16, output[77..79], @intCast(count), .little);
        @memcpy(output[79..111], &canonical_abi);
        for (self.records[0..count], 0..) |record, i| {
            const offset = header_bytes + i * record_bytes;
            std.mem.writeInt(u16, output[offset..][0..2], record.index, .little);
            @memcpy(output[offset + 2 ..][0..32], &record.digest);
        }
        return output;
    }
};
/// Atomic writers share the directory walk. The two disposable formats retain
/// independent budgets; failed/unsupported directories never publish partial facts.
pub const Publication = struct { directory: ?[]u8, identities: ?[]u8 };
pub fn buildAuthority(a: std.mem.Allocator, body: []const u8, kind: u8, digest: [32]u8) !Publication {
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(a));
    defer arena.deinit();
    var collector: Collector = .{ .a = arena.allocator() };
    const directory = try projection.buildAuthorityObserved(a, body, kind, digest, .{ .ctx = &collector, .visit = Collector.visit });
    errdefer if (directory) |value| a.free(value);
    const identities = if (directory) |paths| if (std.mem.startsWith(u8, paths, "DPD2"))
        try collector.encode(a, body.len, kind, digest, paths)
    else
        null else null;
    return .{ .directory = directory, .identities = identities };
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
