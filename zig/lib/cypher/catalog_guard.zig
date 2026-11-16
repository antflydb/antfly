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
const catalog = @import("catalog.zig");

/// A source-owned catalog epoch, never comparable across manager lifetimes.
pub const Epoch = struct { owner: usize, generation: u64 };

/// Graph declarations can map many relationship types onto one physical index.
/// Cache only successful identity checks, fenced on both sides by the source's
/// catalog epoch. Policy, cancellation and deadline checks remain independent.
pub const Guard = struct {
    proofs: []const Proof,
    validated: ?Epoch = null,
    const Proof = struct { index: []const u8, identity: catalog.IndexIdentity };

    pub fn init(a: std.mem.Allocator, graph: catalog.Graph, table: []const u8) !Guard {
        var proofs: std.ArrayList(Proof) = .empty;
        errdefer proofs.deinit(a);
        var unique: std.StringHashMapUnmanaged(catalog.IndexIdentity) = .empty;
        defer unique.deinit(a);
        for (graph.relationships) |mapping| {
            if (!std.mem.eql(u8, graph.label(mapping.source_label).?.table, table)) continue;
            const expected = mapping.identity orelse continue;
            const slot = try unique.getOrPut(a, mapping.index);
            if (slot.found_existing) {
                if (!std.meta.eql(slot.value_ptr.*, expected)) return error.CatalogGenerationChanged;
            } else {
                slot.value_ptr.* = expected;
                try proofs.append(a, .{ .index = mapping.index, .identity = expected });
            }
        }
        return .{ .proofs = try proofs.toOwnedSlice(a) };
    }
    pub fn validate(self: *Guard, a: std.mem.Allocator, source: anytype) !void {
        if (self.proofs.len == 0) return;
        const epoch = source.epoch();
        if (self.validated) |prior| if (std.meta.eql(prior, epoch)) return;
        // Do not leave a prior success reusable after any failed validation.
        self.validated = null;
        for (self.proofs) |proof| {
            const actual = (try source.identity(a, proof.index)) orelse return error.CatalogGenerationChanged;
            if (!std.meta.eql(proof.identity, actual)) return error.CatalogGenerationChanged;
        }
        if (!std.meta.eql(epoch, source.epoch())) return error.CatalogGenerationChanged;
        self.validated = epoch;
    }
};

test "catalog proof cache deduplicates indexes and fences replacement and failed validation" {
    const Source = struct {
        generation: u64 = 1,
        owner: usize = 1,
        reads: usize = 0,
        current: ?catalog.IndexIdentity = .{ .incarnation = 7, .config_hash = 9 },
        change_during_read: bool = false,
        pub fn epoch(self: *@This()) Epoch {
            return .{ .owner = self.owner, .generation = self.generation };
        }
        pub fn identity(self: *@This(), _: std.mem.Allocator, _: []const u8) !?catalog.IndexIdentity {
            self.reads += 1;
            if (self.change_during_read) self.generation += 1;
            return self.current;
        }
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{
        .{ .name = "A", .index = "links", .source_label = "D", .target_label = "D", .stored_type = "a", .identity = .{ .incarnation = 7, .config_hash = 9 } },
        .{ .name = "B", .index = "links", .source_label = "D", .target_label = "D", .stored_type = "b", .identity = .{ .incarnation = 7, .config_hash = 9 } },
    } };
    var guard = try Guard.init(arena.allocator(), graph, "docs");
    try std.testing.expectEqual(@as(usize, 1), guard.proofs.len);
    var source: Source = .{};
    for (0..400) |_| try guard.validate(a, &source);
    try std.testing.expectEqual(@as(usize, 1), source.reads);
    source.generation += 1; // Unrelated catalog change: revalidate, then reuse.
    try guard.validate(a, &source);
    try std.testing.expectEqual(@as(usize, 2), source.reads);
    source.owner += 1; // Same numeric revision in a different owner.
    try guard.validate(a, &source);
    try std.testing.expectEqual(@as(usize, 3), source.reads);
    source.generation += 1;
    source.current = null;
    try std.testing.expectError(error.CatalogGenerationChanged, guard.validate(a, &source));
    source.current = .{ .incarnation = 8, .config_hash = 9 };
    try std.testing.expectError(error.CatalogGenerationChanged, guard.validate(a, &source));
    source.current = .{ .incarnation = 7, .config_hash = 10 };
    try std.testing.expectError(error.CatalogGenerationChanged, guard.validate(a, &source));
    source.current = .{ .incarnation = 7, .config_hash = 9 };
    source.change_during_read = true;
    try std.testing.expectError(error.CatalogGenerationChanged, guard.validate(a, &source));
    source.change_during_read = false;
    try guard.validate(a, &source);
}
