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
const Sha256 = @import("antfly_hash").Sha256;
const artifact_codec = @import("enrichment/artifact_codec.zig");

const magic = "GET2";
const encoded_len = magic.len + 8 + 32;

/// A retired source revision must not regain a fresh TTL when an unchanged
/// producer asset is replayed. The digest excludes the server-assigned TTL
/// timestamp so replay of the same content has the same source revision.
pub const Tombstone = struct {
    deadline_ns: u64,
    source_digest: [32]u8,

    pub fn encode(self: Tombstone) [encoded_len]u8 {
        var out: [encoded_len]u8 = undefined;
        @memcpy(out[0..magic.len], magic);
        std.mem.writeInt(u64, out[magic.len..][0..8], self.deadline_ns, .big);
        @memcpy(out[magic.len + 8 ..], &self.source_digest);
        return out;
    }

    pub fn decode(raw: []const u8) !Tombstone {
        if (raw.len != encoded_len or !std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidGraphEdgeTtlTombstone;
        return .{
            .deadline_ns = std.mem.readInt(u64, raw[magic.len..][0..8], .big),
            .source_digest = raw[magic.len + 8 ..][0..32].*,
        };
    }
};

pub fn sourceDigest(alloc: std.mem.Allocator, raw: []const u8) ![32]u8 {
    var edge = try artifact_codec.decodeGraphEdgeAlloc(alloc, raw);
    defer edge.deinit(alloc);
    var sha = Sha256.init(.{});
    // Incarnation is carried by the tombstone key. Keeping the source
    // revision digest independent of it lets a merge rebind the key to the
    // receiver generation without reviving an unchanged producer asset.
    var fixed: [3 * 8]u8 = undefined;
    std.mem.writeInt(u64, fixed[0..8], @bitCast(edge.weight), .big);
    std.mem.writeInt(u64, fixed[8..16], edge.created_at, .big);
    std.mem.writeInt(u64, fixed[16..24], edge.updated_at, .big);
    sha.update(&fixed);
    sha.update(edge.metadata_json);
    var digest: [32]u8 = undefined;
    sha.final(&digest);
    return digest;
}

test "graph edge ttl tombstone survives codec round-trip and ignores replay timestamp" {
    const alloc = std.testing.allocator;
    const first = try artifact_codec.encodeGraphEdgeWithTtlAlloc(alloc, null, 7, 2, 3, 4, 10, "{\"x\":1}");
    defer alloc.free(first);
    const replay = try artifact_codec.encodeGraphEdgeWithTtlAlloc(alloc, null, 7, 2, 3, 4, 99, "{\"x\":1}");
    defer alloc.free(replay);
    const changed = try artifact_codec.encodeGraphEdgeWithTtlAlloc(alloc, null, 7, 2, 3, 4, 99, "{\"x\":2}");
    defer alloc.free(changed);
    const rebound = try artifact_codec.encodeGraphEdgeWithTtlAlloc(alloc, null, 9, 2, 3, 4, 99, "{\"x\":1}");
    defer alloc.free(rebound);
    const digest = try sourceDigest(alloc, first);
    const replay_digest = try sourceDigest(alloc, replay);
    const rebound_digest = try sourceDigest(alloc, rebound);
    const changed_digest = try sourceDigest(alloc, changed);
    try std.testing.expectEqualSlices(u8, &digest, &replay_digest);
    try std.testing.expectEqualSlices(u8, &digest, &rebound_digest);
    try std.testing.expect(!std.mem.eql(u8, &digest, &changed_digest));
    const encoded = (Tombstone{ .deadline_ns = 55, .source_digest = digest }).encode();
    const decoded = try Tombstone.decode(&encoded);
    try std.testing.expectEqual(@as(u64, 55), decoded.deadline_ns);
    try std.testing.expectEqualSlices(u8, &digest, &decoded.source_digest);
    try std.testing.expectError(error.InvalidGraphEdgeTtlTombstone, Tombstone.decode(encoded[0 .. encoded.len - 1]));
}
