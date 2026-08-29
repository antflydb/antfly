// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");
const raft_engine = @import("raft_engine");

pub const protocol_version: u32 = 2;
/// Generation of the isolated `/raft/v2/snapshot/*` HTTP routing contract.
/// This is deliberately separate from the snapshot wire format version.
pub const http_route_version: u32 = 1;
pub const digest_len = std.crypto.hash.sha2.Sha256.digest_length;
pub const max_manifest_bytes: usize = 256 * 1024;
pub const max_members_per_set: usize = 16 * 1024;
const magic = "AFSNAP2\x00";

/// The v2 wire format keeps its original scalar identity fields for rolling
/// compatibility. Their meaning is explicit here: `from == 0` is an artifact
/// publication and `to` is the node that owns the artifact; a non-zero `from`
/// is a live Raft snapshot delivery. New code must branch on this purpose
/// instead of treating the zero sentinel as a live Raft node id.
pub const Purpose = union(enum) {
    live_install: struct {
        from: u64,
        to: u64,
        term: u64,
    },
    bootstrap_artifact: struct {
        owner_node_id: u64,
    },
};

pub const Manifest = struct {
    group_id: u64,
    from: u64,
    to: u64,
    request_term: u64,
    metadata: raft_engine.core.types.SnapshotMetadata,
    data_len: u64,
    digest: [digest_len]u8,

    pub fn purpose(self: @This()) Purpose {
        if (self.from == 0) {
            return .{ .bootstrap_artifact = .{ .owner_node_id = self.to } };
        }
        return .{ .live_install = .{
            .from = self.from,
            .to = self.to,
            .term = self.request_term,
        } };
    }

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.metadata.deinit(alloc);
        self.* = undefined;
    }

    pub fn clone(self: @This(), alloc: std.mem.Allocator) !Manifest {
        return .{
            .group_id = self.group_id,
            .from = self.from,
            .to = self.to,
            .request_term = self.request_term,
            .metadata = try self.metadata.clone(alloc),
            .data_len = self.data_len,
            .digest = self.digest,
        };
    }
};

pub fn digest(bytes: []const u8) [digest_len]u8 {
    var value: [digest_len]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &value, .{});
    return value;
}

pub fn encode(alloc: std.mem.Allocator, manifest: Manifest) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.ensureTotalCapacity(alloc, 128);
    try out.appendSlice(alloc, magic);
    try appendInt(u32, alloc, &out, protocol_version);
    try appendInt(u64, alloc, &out, manifest.group_id);
    try appendInt(u64, alloc, &out, manifest.from);
    try appendInt(u64, alloc, &out, manifest.to);
    try appendInt(u64, alloc, &out, manifest.request_term);
    try appendInt(u64, alloc, &out, manifest.metadata.index);
    try appendInt(u64, alloc, &out, manifest.metadata.term);
    try encodeNodeList(alloc, &out, manifest.metadata.conf_state.voters);
    try encodeNodeList(alloc, &out, manifest.metadata.conf_state.voters_outgoing);
    try encodeNodeList(alloc, &out, manifest.metadata.conf_state.learners);
    try encodeNodeList(alloc, &out, manifest.metadata.conf_state.learners_next);
    try out.append(alloc, @intFromBool(manifest.metadata.conf_state.auto_leave));
    try appendInt(u64, alloc, &out, manifest.data_len);
    try out.appendSlice(alloc, &manifest.digest);
    var manifest_digest: [digest_len]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(out.items, &manifest_digest, .{});
    try out.appendSlice(alloc, &manifest_digest);
    if (out.items.len > max_manifest_bytes) return error.SnapshotManifestTooLarge;
    return try out.toOwnedSlice(alloc);
}

pub fn decode(alloc: std.mem.Allocator, bytes: []const u8) !Manifest {
    if (bytes.len > max_manifest_bytes or bytes.len < magic.len or
        !std.mem.eql(u8, bytes[0..magic.len], magic)) return error.InvalidSnapshotManifest;
    var cursor: usize = magic.len;
    if (try readInt(u32, bytes, &cursor) != protocol_version) return error.UnsupportedSnapshotTransferProtocol;
    const group_id = try readInt(u64, bytes, &cursor);
    const from = try readInt(u64, bytes, &cursor);
    const to = try readInt(u64, bytes, &cursor);
    const request_term = try readInt(u64, bytes, &cursor);
    const index = try readInt(u64, bytes, &cursor);
    const term = try readInt(u64, bytes, &cursor);
    var conf_state: raft_engine.core.types.ConfState = .{};
    errdefer conf_state.deinit(alloc);
    conf_state.voters = try decodeNodeList(alloc, bytes, &cursor);
    conf_state.voters_outgoing = try decodeNodeList(alloc, bytes, &cursor);
    conf_state.learners = try decodeNodeList(alloc, bytes, &cursor);
    conf_state.learners_next = try decodeNodeList(alloc, bytes, &cursor);
    if (cursor >= bytes.len) return error.InvalidSnapshotManifest;
    conf_state.auto_leave = switch (bytes[cursor]) {
        0 => false,
        1 => true,
        else => return error.InvalidSnapshotManifest,
    };
    cursor += 1;
    const data_len = try readInt(u64, bytes, &cursor);
    if (cursor + digest_len * 2 != bytes.len) return error.InvalidSnapshotManifest;
    var expected_digest: [digest_len]u8 = undefined;
    @memcpy(&expected_digest, bytes[cursor .. cursor + digest_len]);
    const checksum_offset = cursor + digest_len;
    var actual_manifest_digest: [digest_len]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[0..checksum_offset], &actual_manifest_digest, .{});
    if (!std.mem.eql(u8, &actual_manifest_digest, bytes[checksum_offset..]))
        return error.SnapshotManifestChecksumMismatch;
    return .{
        .group_id = group_id,
        .from = from,
        .to = to,
        .request_term = request_term,
        .metadata = .{ .index = index, .term = term, .conf_state = conf_state },
        .data_len = data_len,
        .digest = expected_digest,
    };
}

fn appendInt(comptime T: type, alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), value: T) !void {
    var encoded: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &encoded, value, .little);
    try out.appendSlice(alloc, &encoded);
}

fn encodeNodeList(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), nodes: []const u64) !void {
    if (nodes.len > max_members_per_set) return error.SnapshotManifestMembershipTooLarge;
    try appendInt(u32, alloc, out, @intCast(nodes.len));
    for (nodes) |node| try appendInt(u64, alloc, out, node);
}

fn readInt(comptime T: type, bytes: []const u8, cursor: *usize) !T {
    if (cursor.* + @sizeOf(T) > bytes.len) return error.InvalidSnapshotManifest;
    var encoded: [@sizeOf(T)]u8 = undefined;
    @memcpy(&encoded, bytes[cursor.* .. cursor.* + @sizeOf(T)]);
    cursor.* += @sizeOf(T);
    return std.mem.readInt(T, &encoded, .little);
}

fn decodeNodeList(alloc: std.mem.Allocator, bytes: []const u8, cursor: *usize) ![]u64 {
    const count = try readInt(u32, bytes, cursor);
    if (count > max_members_per_set) return error.InvalidSnapshotManifest;
    const nodes = try alloc.alloc(u64, count);
    errdefer alloc.free(nodes);
    for (nodes) |*node| node.* = try readInt(u64, bytes, cursor);
    return nodes;
}

test "snapshot transfer manifest round trips with integrity metadata" {
    var voters = [_]u64{ 1, 2, 3 };
    const original: Manifest = .{
        .group_id = 9,
        .from = 1,
        .to = 2,
        .request_term = 7,
        .metadata = .{ .index = 11, .term = 6, .conf_state = .{ .voters = &voters } },
        .data_len = 5,
        .digest = digest("hello"),
    };
    const encoded = try encode(std.testing.allocator, original);
    defer std.testing.allocator.free(encoded);
    var decoded = try decode(std.testing.allocator, encoded);
    defer decoded.deinit(std.testing.allocator);
    try std.testing.expectEqual(original.group_id, decoded.group_id);
    try std.testing.expectEqual(original.data_len, decoded.data_len);
    try std.testing.expectEqualSlices(u64, &voters, decoded.metadata.conf_state.voters);
    try std.testing.expectEqualSlices(u8, &original.digest, &decoded.digest);

    const corrupt = try std.testing.allocator.dupe(u8, encoded);
    defer std.testing.allocator.free(corrupt);
    corrupt[magic.len + @sizeOf(u32)] ^= 1;
    try std.testing.expectError(
        error.SnapshotManifestChecksumMismatch,
        decode(std.testing.allocator, corrupt),
    );
}

test "snapshot transfer purpose distinguishes durable artifacts from live delivery" {
    const artifact: Manifest = .{
        .group_id = 9,
        .from = 0,
        .to = 7,
        .request_term = 0,
        .metadata = .{},
        .data_len = 0,
        .digest = digest(""),
    };
    switch (artifact.purpose()) {
        .bootstrap_artifact => |purpose| try std.testing.expectEqual(@as(u64, 7), purpose.owner_node_id),
        .live_install => return error.TestUnexpectedResult,
    }

    var live = artifact;
    live.from = 3;
    live.to = 4;
    live.request_term = 11;
    switch (live.purpose()) {
        .live_install => |purpose| {
            try std.testing.expectEqual(@as(u64, 3), purpose.from);
            try std.testing.expectEqual(@as(u64, 4), purpose.to);
            try std.testing.expectEqual(@as(u64, 11), purpose.term);
        },
        .bootstrap_artifact => return error.TestUnexpectedResult,
    }
}
