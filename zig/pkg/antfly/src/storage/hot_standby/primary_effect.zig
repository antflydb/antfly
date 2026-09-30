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

//! Versioned authoritative primary effects plus disposable projection replay.
//! Older derived-effect readers reject the distinct magic rather than silently
//! dropping primary state. Payload bytes are borrowed; only slice tables allocate.
const std = @import("std");
const journal = @import("../db/derived/change_journal.zig");
const magic = "HPE1";
const retirement_magic = "HPE2";
const expiration = @import("../db/graph_edge_ttl_expiration.zig");
pub const GraphRetirement = struct { candidate: expiration.Candidate, source_digest: [32]u8 };
const header_len = 16;
pub const Write = struct { key: []const u8, value: []const u8 };
pub const Decoded = struct {
    alloc: std.mem.Allocator,
    writes: []Write,
    deletes: [][]const u8,
    replay: []const u8,
    graph_retirement: ?GraphRetirement = null,
    pub fn deinit(self: *@This()) void {
        self.alloc.free(self.writes);
        self.alloc.free(self.deletes);
        self.* = undefined;
    }
};
pub fn isPrimaryEffect(raw: []const u8) bool {
    return raw.len >= 4 and (std.mem.eql(u8, raw[0..4], magic) or std.mem.eql(u8, raw[0..4], retirement_magic));
}
fn appendBytes(alloc: std.mem.Allocator, out: *std.ArrayList(u8), bytes: []const u8) !void {
    const len = std.math.cast(u32, bytes.len) orelse return error.PrimaryEffectTooLarge;
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, len, .little);
    try out.appendSlice(alloc, &buf);
    try out.appendSlice(alloc, bytes);
}
pub fn encodeAlloc(alloc: std.mem.Allocator, writes: anytype, deletes: []const []const u8, replay: []const u8) ![]u8 {
    if (!journal.looksLikeBinaryRecord(replay)) return error.UnsupportedDerivedEffectPayload;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    var header: [header_len]u8 = undefined;
    @memcpy(header[0..4], magic);
    std.mem.writeInt(u32, header[4..8], std.math.cast(u32, writes.len) orelse return error.PrimaryEffectTooLarge, .little);
    std.mem.writeInt(u32, header[8..12], std.math.cast(u32, deletes.len) orelse return error.PrimaryEffectTooLarge, .little);
    std.mem.writeInt(u32, header[12..16], std.math.cast(u32, replay.len) orelse return error.PrimaryEffectTooLarge, .little);
    try out.appendSlice(alloc, &header);
    for (writes) |write| {
        try appendBytes(alloc, &out, write.key);
        try appendBytes(alloc, &out, write.value);
    }
    for (deletes) |key| try appendBytes(alloc, &out, key);
    try out.appendSlice(alloc, replay);
    return out.toOwnedSlice(alloc);
}
/// Retire a source revision without copying replay-dependent counts/winners.
/// HPE2 is intentionally distinct so an HPE1 reader cannot apply it as raw KV.
pub fn encodeGraphRetirementAlloc(alloc: std.mem.Allocator, retirement: GraphRetirement, replay: []const u8) ![]u8 {
    if (!journal.looksLikeBinaryRecord(replay)) return error.UnsupportedDerivedEffectPayload;
    const candidate = try expiration.encodeAlloc(alloc, retirement.candidate);
    defer alloc.free(candidate);
    const retirement_len = std.math.add(u32, std.math.cast(u32, candidate.len) orelse return error.PrimaryEffectTooLarge, 32) catch return error.PrimaryEffectTooLarge;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(alloc);
    var header: [20]u8 = @splat(0);
    @memcpy(header[0..4], retirement_magic);
    std.mem.writeInt(u32, header[12..16], std.math.cast(u32, replay.len) orelse return error.PrimaryEffectTooLarge, .little);
    std.mem.writeInt(u32, header[16..20], retirement_len, .little);
    try out.appendSlice(alloc, &header);
    try out.appendSlice(alloc, candidate);
    try out.appendSlice(alloc, &retirement.source_digest);
    try out.appendSlice(alloc, replay);
    return out.toOwnedSlice(alloc);
}

fn take(raw: []const u8, offset: *usize) ![]const u8 {
    if (raw.len - offset.* < 4) return error.InvalidPrimaryEffect;
    const len = std.mem.readInt(u32, raw[offset.*..][0..4], .little);
    offset.* += 4;
    if (len > raw.len - offset.*) return error.InvalidPrimaryEffect;
    const bytes = raw[offset.*..][0..len];
    offset.* += len;
    return bytes;
}
pub fn decode(alloc: std.mem.Allocator, raw: []const u8) !Decoded {
    if (raw.len < header_len or !isPrimaryEffect(raw)) return error.InvalidPrimaryEffect;
    if (std.mem.eql(u8, raw[0..4], retirement_magic)) {
        if (raw.len < 20 or std.mem.readInt(u32, raw[4..8], .little) != 0 or std.mem.readInt(u32, raw[8..12], .little) != 0) return error.InvalidPrimaryEffect;
        const replay_len = std.mem.readInt(u32, raw[12..16], .little);
        const retirement_len = std.mem.readInt(u32, raw[16..20], .little);
        if (retirement_len < 32 or retirement_len > raw.len - 20 or replay_len != raw.len - 20 - retirement_len) return error.InvalidPrimaryEffect;
        const candidate = expiration.decode(raw[20 .. 20 + retirement_len - 32]) catch return error.InvalidPrimaryEffect;
        const replay = raw[20 + retirement_len ..];
        if (!journal.looksLikeBinaryRecord(replay)) return error.InvalidPrimaryEffect;
        const writes = try alloc.alloc(Write, 0);
        errdefer alloc.free(writes);
        return .{ .alloc = alloc, .writes = writes, .deletes = try alloc.alloc([]const u8, 0), .replay = replay, .graph_retirement = .{ .candidate = candidate, .source_digest = raw[20 + retirement_len - 32 ..][0..32].* } };
    }
    const write_count = std.mem.readInt(u32, raw[4..8], .little);
    const delete_count = std.mem.readInt(u32, raw[8..12], .little);
    const replay_len = std.mem.readInt(u32, raw[12..16], .little);
    // Reject impossible counts before allocating from untrusted headers.
    const bytes = raw.len - header_len;
    if (write_count > bytes / 8 or delete_count > bytes / 4 or replay_len > bytes) return error.InvalidPrimaryEffect;
    const writes = try alloc.alloc(Write, write_count);
    errdefer alloc.free(writes);
    const deletes = try alloc.alloc([]const u8, delete_count);
    errdefer alloc.free(deletes);
    var offset: usize = header_len;
    for (writes) |*write| write.* = .{ .key = try take(raw, &offset), .value = try take(raw, &offset) };
    for (deletes) |*key| key.* = try take(raw, &offset);
    if (raw.len - offset != replay_len or !journal.looksLikeBinaryRecord(raw[offset..])) return error.InvalidPrimaryEffect;
    return .{ .alloc = alloc, .writes = writes, .deletes = deletes, .replay = raw[offset..] };
}

test "HA primary effect preserves binary writes and rejects truncated effects" {
    const alloc = std.testing.allocator;
    const replay = try journal.encodeRecord(alloc, .{ .sequence = 7, .changed_artifact_keys = &.{"edge"} });
    defer alloc.free(replay);
    const writes = [_]Write{.{ .key = &.{ 0, 255 }, .value = &.{ 128, 0 } }};
    const raw = try encodeAlloc(alloc, &writes, &.{"gone"}, replay);
    defer alloc.free(raw);
    var decoded = try decode(alloc, raw);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, writes[0].key, decoded.writes[0].key);
    try std.testing.expectEqualSlices(u8, writes[0].value, decoded.writes[0].value);
    try std.testing.expectEqualSlices(u8, "gone", decoded.deletes[0]);
    try std.testing.expectEqualSlices(u8, replay, decoded.replay);
    try std.testing.expectError(error.InvalidPrimaryEffect, decode(alloc, raw[0 .. raw.len - 1]));
    // Previous standbys must fail closed on the new envelope.
    try std.testing.expect(!journal.looksLikeBinaryRecord(raw));
    try std.testing.expectError(error.InvalidPrimaryEffect, decode(alloc, "HPE2"));
}

test "HA primary effect graph retirement round trips without copying derived state" {
    const alloc = std.testing.allocator;
    const keys = @import("../internal_keys.zig");
    const edge = try keys.graphEdgeArtifactKeyAlloc(alloc, "owner\x00binary", "g", "links", "doc:b");
    defer alloc.free(edge);
    const replay = try journal.encodeRecord(alloc, .{ .sequence = 7, .changed_artifact_keys = &.{edge} });
    defer alloc.free(replay);
    const retirement = GraphRetirement{ .candidate = .{ .index_name = "g", .generation = 7, .edge_key = edge, .state_key = "state\x00binary", .source_priority = 2, .deadline_ns = 1000, .contender_digest = @splat(4) }, .source_digest = @splat(5) };
    const raw = try encodeGraphRetirementAlloc(alloc, retirement, replay);
    defer alloc.free(raw);
    var decoded = try decode(alloc, raw);
    defer decoded.deinit();
    try std.testing.expect(isPrimaryEffect(raw));
    try std.testing.expectEqual(@as(usize, 0), decoded.writes.len);
    try std.testing.expectEqual(@as(usize, 0), decoded.deletes.len);
    const op = decoded.graph_retirement.?;
    try std.testing.expectEqualSlices(u8, edge, op.candidate.edge_key);
    try std.testing.expectEqualSlices(u8, retirement.candidate.state_key, op.candidate.state_key);
    try std.testing.expectEqualSlices(u8, &retirement.source_digest, &op.source_digest);
    try std.testing.expectEqual(retirement.candidate.generation, op.candidate.generation);
    try std.testing.expectEqual(retirement.candidate.deadline_ns, op.candidate.deadline_ns);
    try std.testing.expectEqualSlices(u8, replay, decoded.replay);
    try std.testing.expect(!journal.looksLikeBinaryRecord(raw));
    try std.testing.expectError(error.InvalidPrimaryEffect, decode(alloc, raw[0 .. raw.len - 1]));
    const malformed = try alloc.dupe(u8, raw);
    defer alloc.free(malformed);
    malformed[4] = 1;
    try std.testing.expectError(error.InvalidPrimaryEffect, decode(alloc, malformed));
}
