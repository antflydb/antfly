// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Authoritative cleanup job incarnations, independent of local directories.
const std = @import("std");
const keys = @import("internal_keys.zig");
pub const Guard = struct {
    endpoint: []const u8,
    generation: u64,
    kind: enum { endpoint, owner_replay } = .endpoint,
    checkpoint_digest: [32]u8 = @as([32]u8, @splat(0)),
};
const magic = "GEC2";
pub fn matchesKey(key: []const u8, endpoint: []const u8) bool {
    if (!std.mem.startsWith(u8, key, keys.graph_endpoint_cleanup_prefix) or key.len != keys.graph_endpoint_cleanup_prefix.len + 64) return false;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(endpoint, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, key[keys.graph_endpoint_cleanup_prefix.len..], &hex);
}
pub fn decode(key: []const u8, value: []const u8) !Guard {
    // Legacy values are arbitrary endpoint bytes; test their hash first so
    // an endpoint beginning with the version magic cannot be misinterpreted.
    if (matchesKey(key, value)) return .{ .endpoint = value, .generation = 0 };
    if (value.len < 12 or !std.mem.startsWith(u8, value, magic)) return error.InvalidGraphSegment;
    const generation = std.mem.readInt(u64, value[4..12], .little);
    if (generation == 0 or !matchesKey(key, value[12..])) return error.InvalidGraphSegment;
    return .{ .endpoint = value[12..], .generation = generation };
}
pub fn encodeAlloc(alloc: std.mem.Allocator, endpoint: []const u8, generation: u64) ![]u8 {
    if (generation == 0) return error.InvalidGraphSegment;
    const value = try alloc.alloc(u8, 12 + endpoint.len);
    @memcpy(value[0..4], magic);
    std.mem.writeInt(u64, value[4..12], generation, .little);
    @memcpy(value[12..], endpoint);
    return value;
}

/// Each input kind has its own prefix, avoiding scans of projected edges or
/// embeddings when a producing document has a large artifact history.
pub const OwnerPhase = enum(u8) { retirements = 0, inputs = 1, chunks = 2, resolutions = 3 };
pub const OwnerJob = struct {
    owner: []const u8,
    generation: u64,
    phase: OwnerPhase = .retirements,
    cursor: []const u8 = "",
};

pub fn checkpointDigest(value: []const u8) [32]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(value, &digest, .{});
    return digest;
}

pub fn ownerJobKeyAlloc(alloc: std.mem.Allocator, owner: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(owner, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.mem.concat(alloc, u8, &.{ keys.graph_owner_replay_prefix, &hex });
}

pub fn matchesOwnerJobKey(key: []const u8, owner: []const u8) bool {
    if (!keys.isGraphOwnerReplayJobKey(key)) return false;
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(owner, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    return std.mem.eql(u8, key[keys.graph_owner_replay_prefix.len..], &hex);
}

pub fn encodeOwnerJobAlloc(alloc: std.mem.Allocator, job: OwnerJob) ![]u8 {
    if (job.generation == 0 or job.owner.len > std.math.maxInt(u32) or job.cursor.len > std.math.maxInt(u32)) return error.InvalidGraphSegment;
    const value = try alloc.alloc(u8, 21 + job.owner.len + job.cursor.len);
    @memcpy(value[0..4], "GOR3");
    std.mem.writeInt(u64, value[4..12], job.generation, .little);
    value[12] = @backingInt(job.phase);
    std.mem.writeInt(u32, value[13..17], @intCast(job.owner.len), .little);
    std.mem.writeInt(u32, value[17..21], @intCast(job.cursor.len), .little);
    @memcpy(value[21..][0..job.owner.len], job.owner);
    @memcpy(value[21 + job.owner.len ..], job.cursor);
    return value;
}

pub fn decodeOwnerJob(key: []const u8, value: []const u8) !OwnerJob {
    if (value.len < 21 or !std.mem.eql(u8, value[0..4], "GOR3")) return error.InvalidGraphSegment;
    const generation = std.mem.readInt(u64, value[4..12], .little);
    const phase: OwnerPhase = switch (value[12]) {
        0 => .retirements,
        1 => .inputs,
        2 => .chunks,
        3 => .resolutions,
        else => return error.InvalidGraphSegment,
    };
    const owner_len: usize = std.mem.readInt(u32, value[13..17], .little);
    const cursor_len: usize = std.mem.readInt(u32, value[17..21], .little);
    if (generation == 0 or owner_len > value.len - 21 or cursor_len != value.len - 21 - owner_len) return error.InvalidGraphSegment;
    const owner = value[21..][0..owner_len];
    if (!matchesOwnerJobKey(key, owner)) return error.InvalidGraphSegment;
    const cursor = value[21 + owner_len ..];
    return .{ .owner = owner, .generation = generation, .phase = phase, .cursor = cursor };
}

pub fn guardKeyAlloc(alloc: std.mem.Allocator, guard: Guard) ![]u8 {
    return if (guard.kind == .endpoint) keys.graphEndpointCleanupKeyAlloc(alloc, guard.endpoint) else ownerJobKeyAlloc(alloc, guard.endpoint);
}

pub fn guardMatches(guard: Guard, key: []const u8, value: []const u8) !bool {
    if (guard.kind == .endpoint) return (try decode(key, value)).generation == guard.generation;
    const job = try decodeOwnerJob(key, value);
    return job.generation == guard.generation and std.mem.eql(u8, &checkpointDigest(value), &guard.checkpoint_digest);
}

pub fn retirementValue(generation: u64) [12]u8 {
    std.debug.assert(generation != 0);
    var value: [12]u8 = undefined;
    @memcpy(value[0..4], "GRT2");
    std.mem.writeInt(u64, value[4..12], generation, .little);
    return value;
}

pub fn retirementGeneration(value: []const u8) !u64 {
    if (std.mem.eql(u8, value, "1")) return 0;
    if (value.len != 12 or !std.mem.eql(u8, value[0..4], "GRT2")) return error.InvalidGraphRetirement;
    const generation = std.mem.readInt(u64, value[4..12], .little);
    if (generation == 0) return error.InvalidGraphRetirement;
    return generation;
}

pub fn ownedBy(alloc: std.mem.Allocator, key: []const u8, owner: []const u8) !bool {
    if (key.len < 4 or key[0] != keys.user_namespace) return false;
    const end = keys.findComponentTerminator(key, 1) orelse return false;
    const decoded = try keys.decodeBodyAlloc(alloc, key[1..end]);
    defer alloc.free(decoded);
    return std.mem.eql(u8, decoded, owner);
}

pub fn isReplayInput(key: []const u8) bool {
    return keys.isAssetArtifactKey(key) or keys.isChunkArtifactRecordKey(key) or keys.isResolutionArtifactKey(key);
}
