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

//! Regression: fresh qualified repeated binding advances the root cut without
//! replacing an admitted attempt or rewriting the original generation binding.
const std = @import("std");
const scope = @import("source_scope.zig");
const ordered = @import("source_scope_ordered.zig");
const completion = @import("transaction_completion.zig");
test "source scope repeated binding advances metadata floor and preserves pending work" {
    const owner: completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = @splat(1), .table_binding = @splat(8), .root_generation = 0, .group_id = 2, .topology_epoch = 1 };
    const root = try scope.Basis.fromOwner(owner);
    const spec: scope.Spec = .{ .actor_table_id = 7, .source_id = @splat(2), .generation = 1, .binding = @splat(3), .checkpoint_owner = root };
    var header: scope.Header = .{ .root = root };
    var metadata_cut: scope.MetadataCut = .{ .group_id = 1, .incarnation = owner.metadata_incarnation, .index = 10 };
    var record = try header.bind(null, spec, metadata_cut, 3);
    try record.enter(.{ .reference = .{ .domain = spec.domain(), .binding = spec.binding, .generation = 1, .token = 1, .sequence = 4 }, .transaction_id = @splat(4), .source_purpose = @splat(5), .admission_digest = @splat(6) }, root, root);
    const original = record;
    metadata_cut.index = 11;
    const refreshed = try header.bind(record, spec, metadata_cut, 99);
    try std.testing.expectEqual(@as(u64, 11), header.metadata_floor);
    try std.testing.expectEqual(@as(u16, 1), header.domains);
    try std.testing.expectEqualDeep(original, refreshed);
    try refreshed.validate();
    var another = spec;
    another.source_id[0] ^= 1;
    var stale = metadata_cut;
    stale.index = 10;
    try std.testing.expectError(error.SourceCheckpointConflict, header.bind(null, another, stale, 0));
    const Writer = struct {
        bytes: ?[]u8 = null,
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try std.testing.expectEqualStrings(ordered.receipt_key, key);
            if (self.bytes) |old| std.testing.allocator.free(old);
            self.bytes = try std.testing.allocator.dupe(u8, value);
        }
    };
    var writer: Writer = .{};
    defer if (writer.bytes) |bytes| std.testing.allocator.free(bytes);
    const entry: scope.Cut = .{ .term = 1, .index = 21 };
    const command: ordered.control.Command = .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .spec = spec, .minimum_metadata_index = 10, .action = .bind }, .metadata_cut = metadata_cut };
    try ordered.stage(std.testing.allocator, &writer, command, entry, .{ .applied = .{ .version = 1, .owner = owner, .domain = spec.domain(), .cut = entry, .header = header, .record = refreshed } });
    try std.testing.expect(writer.bytes != null);
}
