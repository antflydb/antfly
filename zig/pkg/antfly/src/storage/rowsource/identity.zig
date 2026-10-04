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
pub fn allocId(alloc: std.mem.Allocator, ref: types.RowRef) ![]u8 {
    return switch (ref) {
        .relational_key => |key| try alloc.dupe(u8, key),
        .serverless => try std.json.Stringify.valueAlloc(alloc, ref, .{}),
        .external => |external| try std.fmt.allocPrint(alloc, "lake1:{s}:{x:0>8}:{x:0>16}", .{ std.fmt.bytesToHex(fileDigest(external.source_id, external.snapshot_id, external.file_id), .lower), external.row_group_ordinal, external.row_ordinal }),
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
