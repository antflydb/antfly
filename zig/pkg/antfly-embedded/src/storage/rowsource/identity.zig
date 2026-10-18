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

//! Opaque, versioned external identities ordered by file, row group and row.
//! The digest binds identity to the immutable source snapshot; fixed-width
//! ordinals allow ordered pagination without sorting or retaining row values.
const std = @import("std");
const types = @import("types.zig");
pub fn fileDigest(source: []const u8, snapshot: []const u8, file: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    for ([_][]const u8{ source, snapshot, file }) |part| {
        var len: [8]u8 = undefined;
        std.mem.writeInt(u64, &len, part.len, .little);
        hash.update(&len);
        hash.update(part);
    }
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    return digest;
}
/// New public identities bind a snapshot separately from stable file navigation.
/// The full SHA-256 domains are retained; a snapshot change rewrites no file key.
pub fn navigationDigest(version: u8, source: []const u8, snapshot: []const u8, file: []const u8) [32]u8 {
    return fileDigest(source, if (version == 2) "" else snapshot, file);
}
pub const Parts = struct { version: u8, file: []const u8, digest: []const u8, snapshot: ?[]const u8, group: u32, row: u64 };
pub fn parse(id: []const u8) !Parts {
    const modern = id.len == 161 and std.mem.startsWith(u8, id, "lake3:");
    const legacy = id.len == 96 and (std.mem.startsWith(u8, id, "lake1:") or std.mem.startsWith(u8, id, "lake2:"));
    if (!modern and !legacy) return error.ExternalLakeSnapshotMismatch;
    const end: usize = if (modern) 135 else 70;
    if (id[end] != ':' or id[end + 9] != ':' or (modern and id[70] != ':')) return error.ExternalLakeSnapshotMismatch;
    for (id[6..], 6..) |byte, offset| {
        if (offset == end or offset == end + 9 or (modern and offset == 70)) continue;
        if (!((byte >= '0' and byte <= '9') or (byte >= 'a' and byte <= 'f'))) return error.ExternalLakeSnapshotMismatch;
    }
    return .{ .version = if (modern) 2 else 1, .file = id[6..end], .digest = if (modern) id[71..135] else id[6..70], .snapshot = if (modern) id[6..70] else null, .group = std.fmt.parseUnsigned(u32, id[end + 1 ..][0..8], 16) catch return error.ExternalLakeSnapshotMismatch, .row = std.fmt.parseUnsigned(u64, id[end + 10 ..][0..16], 16) catch return error.ExternalLakeSnapshotMismatch };
}
pub fn publicFile(id: []const u8) ![]const u8 {
    return (try parse(id)).file;
}
pub fn isPublic(id: []const u8) bool {
    return std.mem.startsWith(u8, id, "lake1:") or std.mem.startsWith(u8, id, "lake3:");
}
pub fn validateSnapshot(parts: Parts, source: []const u8, snapshot: []const u8) !void {
    if (parts.snapshot) |proof| if (!std.mem.eql(u8, proof, &std.fmt.bytesToHex(fileDigest(source, snapshot, ""), .lower))) return error.ExternalLakeSnapshotMismatch;
}
pub fn publicFileAlloc(a: std.mem.Allocator, version: u8, source: []const u8, snapshot: []const u8, file: []const u8) ![]u8 {
    const digest = std.fmt.bytesToHex(navigationDigest(version, source, snapshot, file), .lower);
    return if (version == 2) std.fmt.allocPrint(a, "{s}:{s}", .{ std.fmt.bytesToHex(fileDigest(source, snapshot, ""), .lower), digest }) else a.dupe(u8, &digest);
}
pub fn allocId(alloc: std.mem.Allocator, ref: types.RowRef) ![]u8 {
    return switch (ref) {
        .relational_key => |key| try alloc.dupe(u8, key),
        .serverless => try std.json.Stringify.valueAlloc(alloc, ref, .{}),
        .external => |external| if (external.identity_version == 2)
            try std.fmt.allocPrint(alloc, "lake3:{s}:{s}:{x:0>8}:{x:0>16}", .{ std.fmt.bytesToHex(fileDigest(external.source_id, external.snapshot_id, ""), .lower), std.fmt.bytesToHex(navigationDigest(2, external.source_id, external.snapshot_id, external.file_id), .lower), external.row_group_ordinal, external.row_ordinal })
        else
            try std.fmt.allocPrint(alloc, "lake1:{s}:{x:0>8}:{x:0>16}", .{ std.fmt.bytesToHex(fileDigest(external.source_id, external.snapshot_id, external.file_id), .lower), external.row_group_ordinal, external.row_ordinal }),
    };
}
test "external lake identities sort numerically and bind the source snapshot" {
    const alloc = std.testing.allocator;
    var ref: types.RowRef = .{ .external = .{ .source_id = "events", .snapshot_id = "v1", .file_id = "a", .row_group_ordinal = 1, .row_ordinal = 2 } };
    const a = try allocId(alloc, ref);
    defer alloc.free(a);
    ref.external.row_ordinal = 10;
    const b = try allocId(alloc, ref);
    defer alloc.free(b);
    try std.testing.expect(std.mem.order(u8, a, b) == .lt);
    ref.external.snapshot_id = "v2";
    const changed = try allocId(alloc, ref);
    defer alloc.free(changed);
    try std.testing.expect(!std.mem.eql(u8, b, changed));
}

/// Accept only identities issued for a file in this exact source snapshot.
/// The existing lake1 digest includes source, snapshot and file identity, so
/// validation preserves published IDs without introducing an unbound token.
pub fn validateContinuation(id: []const u8, inventory: @import("../../serverless/external_source/types.zig").Inventory) !void {
    if (!isPublic(id)) return error.ExternalLakeSnapshotMismatch;
    const parts = try parse(id);
    try validateSnapshot(parts, inventory.source_id, inventory.snapshot_id);
    if (parts.version != inventory.row_identity_version) return error.ExternalLakeSnapshotMismatch;
    for (inventory.files) |file| {
        const digest = std.fmt.bytesToHex(navigationDigest(parts.version, inventory.source_id, inventory.snapshot_id, file.file_id), .lower);
        if (std.mem.eql(u8, parts.digest, &digest)) return;
    }
    return error.ExternalLakeSnapshotMismatch;
}

test "external lake stable navigation keeps full snapshot proofs and rejects stale continuations" {
    const a = std.testing.allocator;
    var ref: types.RowRef = .{ .external = .{ .identity_version = 2, .source_id = "source", .snapshot_id = "before", .file_id = "file", .row_group_ordinal = 7, .row_ordinal = 9007199254740993 } };
    const old = try allocId(a, ref);
    defer a.free(old);
    const parts = try parse(old);
    try std.testing.expectEqual(@as(usize, 161), old.len);
    try std.testing.expectEqual(@as(u64, 9007199254740993), parts.row);
    try validateSnapshot(parts, "source", "before");
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, validateSnapshot(parts, "source", "after"));
    ref.external.snapshot_id = "after";
    const fresh = try allocId(a, ref);
    defer a.free(fresh);
    try std.testing.expectEqualStrings(parts.digest, (try parse(fresh)).digest);
    try std.testing.expect(!std.mem.eql(u8, old, fresh));
    const file = try publicFileAlloc(a, 2, "source", "after", "file");
    defer a.free(file);
    try std.testing.expectEqualStrings(file, try publicFile(fresh));
    ref.external.identity_version = 1;
    const legacy = try allocId(a, ref);
    defer a.free(legacy);
    try std.testing.expectEqual(@as(usize, 96), legacy.len);
    try std.testing.expectEqual(@as(u8, 1), (try parse(legacy)).version);
}
