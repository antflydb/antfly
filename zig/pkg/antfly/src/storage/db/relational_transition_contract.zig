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

//! Bounded, data-only requests against an already leased physical owner.
const topology = @import("relational_integrity_topology_contract.zig");
const handoff = @import("relational_integrity_handoff_contract.zig");

pub const Request = union(enum) {
    identity: void,
    status: void,
    manifest: struct {
        source: topology.Fence,
        destination: topology.Fence,
        lower: []const u8,
        upper: []const u8,
        primary_sequence: u64,
    },
    page: struct { manifest: handoff.Manifest, progress: handoff.Progress },
};

test "pure transition contract encodes binary owner and continuation proofs" {
    const std = @import("std");
    const alloc = std.testing.allocator;
    const fence: topology.Fence = .{
        .transition_id = 8,
        .attempt = 9,
        .peer_group_id = 2,
        .owner_group_id = 1,
        .role = .split_source,
        .namespace = .{ .table_id = 5, .shard_id = 1, .range_id = 1 },
        .catalog_digest = @splat(255),
    };
    const request: Request = .{ .page = .{ .manifest = .{
        .source = fence,
        .destination = fence,
        .lower = "\x00\xff",
        .upper = "\xff\xff",
        .source_range_start = "",
        .source_range_end = "",
        .catalog_bytes = "\x00\xffcatalog",
        .activation_bytes = "\xffproof",
        .primary_sequence = 12,
    }, .progress = .{ .manifest_digest = @splat(254), .cursor = "\xff\x00cursor" } } };
    const encoded = try handoff.encode(alloc, request);
    defer alloc.free(encoded);
    var parsed = try std.json.parseFromSlice(Request, alloc, encoded, .{});
    defer parsed.deinit();
    try std.testing.expect(parsed.value.page.manifest.source.eql(fence));
    try std.testing.expectEqualStrings(request.page.manifest.catalog_bytes, parsed.value.page.manifest.catalog_bytes);
    try std.testing.expectEqualStrings(request.page.progress.cursor, parsed.value.page.progress.cursor);
}
