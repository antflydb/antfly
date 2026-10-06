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

const std = @import("std");
const internal_keys = @import("../internal_keys.zig");

const magic = "GEX1";
const header_len = magic.len + 8 + 4 + 8 + 32 + 3 * 4;
const direct_magic = "GEX2";
const direct_header_len = direct_magic.len + 8 + 8 + 32 + 2 * 4;

/// Borrowed view of one due source contribution. The digest binds the exact
/// contender afterimage; generation, source identity, and deadline are checked
/// again by the primary conditional mutation.
pub const Candidate = struct {
    index_name: []const u8,
    generation: u64,
    edge_key: []const u8,
    state_key: []const u8,
    source_priority: usize,
    deadline_ns: u64,
    contender_digest: [32]u8,
};

/// Direct graph artifacts have no producer source state. Their exact stored
/// afterimage is the conditional deletion identity.
pub const DirectCandidate = struct {
    index_name: []const u8,
    generation: u64,
    artifact_key: []const u8,
    deadline_ns: u64,
    artifact_digest: [32]u8,
};

pub const Due = union(enum) {
    source: Candidate,
    direct: DirectCandidate,
};

pub fn directIndexKeyAlloc(alloc: std.mem.Allocator, deadline_ns: u64, artifact_key: []const u8) ![]u8 {
    if (deadline_ns == 0 or !internal_keys.isGraphEdgeArtifactKey(artifact_key)) return error.InvalidGraphTtlCandidate;
    const prefix = &internal_keys.graph_edge_expiration_index_prefix;
    const out = try alloc.alloc(u8, prefix.len + 8 + artifact_key.len);
    @memcpy(out[0..prefix.len], prefix);
    std.mem.writeInt(u64, out[prefix.len..][0..8], deadline_ns, .big);
    @memcpy(out[prefix.len + 8 ..], artifact_key);
    return out;
}

pub fn indexKeyAlloc(alloc: std.mem.Allocator, deadline_ns: u64, contender_key: []const u8) ![]u8 {
    if (deadline_ns == 0 or !internal_keys.isGraphGlobalEdgeContenderKey(contender_key)) return error.InvalidGraphTtlCandidate;
    const prefix = &internal_keys.graph_edge_expiration_index_prefix;
    const out = try alloc.alloc(u8, prefix.len + 8 + contender_key.len);
    @memcpy(out[0..prefix.len], prefix);
    std.mem.writeInt(u64, out[prefix.len..][0..8], deadline_ns, .big);
    @memcpy(out[prefix.len + 8 ..], contender_key);
    return out;
}

pub fn deadlineFromKey(key: []const u8) !u64 {
    const prefix = &internal_keys.graph_edge_expiration_index_prefix;
    if (key.len <= prefix.len + 8 or !std.mem.startsWith(u8, key, prefix) or
        (!internal_keys.isGraphGlobalEdgeContenderKey(key[prefix.len + 8 ..]) and
            !internal_keys.isGraphEdgeArtifactKey(key[prefix.len + 8 ..])))
        return error.InvalidGraphTtlCandidate;
    const deadline = std.mem.readInt(u64, key[prefix.len..][0..8], .big);
    if (deadline == 0) return error.InvalidGraphTtlCandidate;
    return deadline;
}

pub fn encodeDirectAlloc(alloc: std.mem.Allocator, candidate: DirectCandidate) ![]u8 {
    if (candidate.generation == 0 or candidate.deadline_ns == 0 or
        candidate.index_name.len > std.math.maxInt(u32) or
        candidate.artifact_key.len > std.math.maxInt(u32) or
        !internal_keys.matchesGraphEdgeIndexName(candidate.artifact_key, candidate.index_name))
        return error.InvalidGraphTtlCandidate;
    const payload_len = std.math.add(usize, candidate.index_name.len, candidate.artifact_key.len) catch return error.ResourceLimitExceeded;
    const length = std.math.add(usize, direct_header_len, payload_len) catch return error.ResourceLimitExceeded;
    const out = try alloc.alloc(u8, length);
    @memcpy(out[0..direct_magic.len], direct_magic);
    var pos: usize = direct_magic.len;
    std.mem.writeInt(u64, out[pos..][0..8], candidate.generation, .big);
    pos += 8;
    std.mem.writeInt(u64, out[pos..][0..8], candidate.deadline_ns, .big);
    pos += 8;
    @memcpy(out[pos..][0..32], &candidate.artifact_digest);
    pos += 32;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(candidate.index_name.len), .big);
    pos += 4;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(candidate.artifact_key.len), .big);
    pos += 4;
    @memcpy(out[pos..][0..candidate.index_name.len], candidate.index_name);
    pos += candidate.index_name.len;
    @memcpy(out[pos..][0..candidate.artifact_key.len], candidate.artifact_key);
    return out;
}

pub fn decodeDirect(raw: []const u8) !DirectCandidate {
    if (raw.len < direct_header_len or !std.mem.eql(u8, raw[0..direct_magic.len], direct_magic)) return error.InvalidGraphTtlCandidate;
    var pos: usize = direct_magic.len;
    const generation = std.mem.readInt(u64, raw[pos..][0..8], .big);
    pos += 8;
    const deadline_ns = std.mem.readInt(u64, raw[pos..][0..8], .big);
    pos += 8;
    const digest = raw[pos..][0..32].*;
    pos += 32;
    const index_len = std.mem.readInt(u32, raw[pos..][0..4], .big);
    pos += 4;
    const artifact_len = std.mem.readInt(u32, raw[pos..][0..4], .big);
    pos += 4;
    const index_end = std.math.add(usize, pos, @intCast(index_len)) catch return error.InvalidGraphTtlCandidate;
    const artifact_end = std.math.add(usize, index_end, @intCast(artifact_len)) catch return error.InvalidGraphTtlCandidate;
    if (artifact_end != raw.len) return error.InvalidGraphTtlCandidate;
    const candidate: DirectCandidate = .{
        .index_name = raw[pos..index_end],
        .generation = generation,
        .artifact_key = raw[index_end..artifact_end],
        .deadline_ns = deadline_ns,
        .artifact_digest = digest,
    };
    if (generation == 0 or deadline_ns == 0 or
        !internal_keys.matchesGraphEdgeIndexName(candidate.artifact_key, candidate.index_name))
        return error.InvalidGraphTtlCandidate;
    return candidate;
}

pub fn decodeDue(raw: []const u8) !Due {
    if (raw.len < 4) return error.InvalidGraphTtlCandidate;
    if (std.mem.eql(u8, raw[0..4], magic)) return .{ .source = try decode(raw) };
    if (std.mem.eql(u8, raw[0..4], direct_magic)) return .{ .direct = try decodeDirect(raw) };
    return error.InvalidGraphTtlCandidate;
}

pub fn encodeAlloc(alloc: std.mem.Allocator, candidate: Candidate) ![]u8 {
    if (candidate.generation == 0 or candidate.deadline_ns == 0 or
        candidate.source_priority > std.math.maxInt(u32) or
        candidate.index_name.len > std.math.maxInt(u32) or
        candidate.edge_key.len > std.math.maxInt(u32) or
        candidate.state_key.len > std.math.maxInt(u32) or
        !internal_keys.matchesGraphEdgeIndexName(candidate.edge_key, candidate.index_name))
        return error.InvalidGraphTtlCandidate;
    var length = std.math.add(usize, header_len, candidate.index_name.len) catch return error.ResourceLimitExceeded;
    length = std.math.add(usize, length, candidate.edge_key.len) catch return error.ResourceLimitExceeded;
    length = std.math.add(usize, length, candidate.state_key.len) catch return error.ResourceLimitExceeded;
    const out = try alloc.alloc(u8, length);
    @memcpy(out[0..magic.len], magic);
    var pos: usize = magic.len;
    std.mem.writeInt(u64, out[pos..][0..8], candidate.generation, .big);
    pos += 8;
    std.mem.writeInt(u32, out[pos..][0..4], @intCast(candidate.source_priority), .big);
    pos += 4;
    std.mem.writeInt(u64, out[pos..][0..8], candidate.deadline_ns, .big);
    pos += 8;
    @memcpy(out[pos..][0..32], &candidate.contender_digest);
    pos += 32;
    for ([_]usize{ candidate.index_name.len, candidate.edge_key.len, candidate.state_key.len }) |part_len| {
        std.mem.writeInt(u32, out[pos..][0..4], @intCast(part_len), .big);
        pos += 4;
    }
    for ([_][]const u8{ candidate.index_name, candidate.edge_key, candidate.state_key }) |part| {
        @memcpy(out[pos..][0..part.len], part);
        pos += part.len;
    }
    return out;
}

pub fn decode(raw: []const u8) !Candidate {
    if (raw.len < header_len or !std.mem.eql(u8, raw[0..magic.len], magic)) return error.InvalidGraphTtlCandidate;
    var pos: usize = magic.len;
    const generation = std.mem.readInt(u64, raw[pos..][0..8], .big);
    pos += 8;
    const source_priority = std.mem.readInt(u32, raw[pos..][0..4], .big);
    pos += 4;
    const deadline_ns = std.mem.readInt(u64, raw[pos..][0..8], .big);
    pos += 8;
    const digest = raw[pos..][0..32].*;
    pos += 32;
    const index_len = std.mem.readInt(u32, raw[pos..][0..4], .big);
    pos += 4;
    const edge_len = std.mem.readInt(u32, raw[pos..][0..4], .big);
    pos += 4;
    const state_len = std.mem.readInt(u32, raw[pos..][0..4], .big);
    pos += 4;
    const total = std.math.add(usize, pos, @intCast(index_len)) catch return error.InvalidGraphTtlCandidate;
    const edge_end = std.math.add(usize, total, @intCast(edge_len)) catch return error.InvalidGraphTtlCandidate;
    const state_end = std.math.add(usize, edge_end, @intCast(state_len)) catch return error.InvalidGraphTtlCandidate;
    if (state_end != raw.len) return error.InvalidGraphTtlCandidate;
    const candidate = Candidate{
        .index_name = raw[pos..total],
        .generation = generation,
        .edge_key = raw[total..edge_end],
        .state_key = raw[edge_end..state_end],
        .source_priority = source_priority,
        .deadline_ns = deadline_ns,
        .contender_digest = digest,
    };
    if (generation == 0 or deadline_ns == 0 or
        !internal_keys.matchesGraphEdgeIndexName(candidate.edge_key, candidate.index_name))
        return error.InvalidGraphTtlCandidate;
    return candidate;
}

test "graph ttl due keys sort by deadline and candidate codec binds source" {
    const alloc = std.testing.allocator;
    const edge = try internal_keys.graphEdgeArtifactKeyWithSourceAlloc(alloc, "doc:a", "g", "links", "doc:b", "doc:a");
    defer alloc.free(edge);
    const contender = try internal_keys.graphGlobalEdgeContenderKeyAlloc(alloc, "g", 7, edge, 0, "source");
    defer alloc.free(contender);
    const first_key = try indexKeyAlloc(alloc, 100, contender);
    defer alloc.free(first_key);
    const second_key = try indexKeyAlloc(alloc, 200, contender);
    defer alloc.free(second_key);
    try std.testing.expect(std.mem.order(u8, first_key, second_key) == .lt);
    try std.testing.expectEqual(@as(u64, 100), try deadlineFromKey(first_key));
    const candidate = Candidate{ .index_name = "g", .generation = 7, .edge_key = edge, .state_key = "source", .source_priority = 0, .deadline_ns = 100, .contender_digest = @splat(3) };
    const encoded = try encodeAlloc(alloc, candidate);
    defer alloc.free(encoded);
    const decoded = try decode(encoded);
    try std.testing.expectEqualStrings(candidate.index_name, decoded.index_name);
    try std.testing.expectEqualStrings(candidate.edge_key, decoded.edge_key);
    try std.testing.expectEqualStrings(candidate.state_key, decoded.state_key);
    try std.testing.expectEqual(candidate.deadline_ns, decoded.deadline_ns);
    try std.testing.expectEqualSlices(u8, &candidate.contender_digest, &decoded.contender_digest);
    try std.testing.expectError(error.InvalidGraphTtlCandidate, decode(encoded[0 .. encoded.len - 1]));
}

test "graph ttl direct due codec authenticates artifact identity" {
    const alloc = std.testing.allocator;
    const artifact = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "g", "links", "doc:b");
    defer alloc.free(artifact);
    const key = try directIndexKeyAlloc(alloc, 123, artifact);
    defer alloc.free(key);
    try std.testing.expectEqual(@as(u64, 123), try deadlineFromKey(key));
    const candidate: DirectCandidate = .{
        .index_name = "g",
        .generation = 7,
        .artifact_key = artifact,
        .deadline_ns = 123,
        .artifact_digest = @splat(5),
    };
    const value = try encodeDirectAlloc(alloc, candidate);
    defer alloc.free(value);
    const due = try decodeDue(value);
    const decoded = due.direct;
    try std.testing.expectEqualStrings(candidate.index_name, decoded.index_name);
    try std.testing.expectEqualStrings(candidate.artifact_key, decoded.artifact_key);
    try std.testing.expectEqual(candidate.generation, decoded.generation);
    try std.testing.expectEqual(candidate.deadline_ns, decoded.deadline_ns);
    try std.testing.expectEqualSlices(u8, &candidate.artifact_digest, &decoded.artifact_digest);
    try std.testing.expectError(error.InvalidGraphTtlCandidate, decodeDirect(value[0 .. value.len - 1]));
}
