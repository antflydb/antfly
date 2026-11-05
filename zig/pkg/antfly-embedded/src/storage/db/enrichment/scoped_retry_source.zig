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
const revisions = @import("../../document_mutation_revision.zig");
const identities = @import("../doc_identity.zig");
const keys = @import("../../internal_keys.zig");
pub const Stamp = @import("scoped_retries.zig").SourceStamp;
pub const Observation = struct { stamp: ?Stamp = null, logical_digest: ?[32]u8 = null, primary_read: bool = false };

/// All observations share one read snapshot. Primary bytes are needed only
/// for old/unsupported metadata or to qualify a relational input on failure.
/// Derived writes may advance the revision while preserving primary content.
pub fn observe(a: std.mem.Allocator, txn: anytype, physical: []const u8, document: []const u8, previous: ?Stamp, logical: bool, materializer: anytype) !Observation {
    var incarnation: u64 = 0;
    if (try identities.lookupOrdinalTxn(a, txn, document)) |ordinal| {
        const state = (try identities.lookupStateTxn(txn, ordinal)) orelse return error.InvalidDocIdentity;
        if (!state.isLive()) return .{};
        incarnation = state.created_generation;
    }
    const key = revisions.keyForPhysical(physical) orelse return error.InvalidEnrichmentState;
    const revision = try revisions.load(txn, key);
    if (!logical) if (previous) |old| {
        if (revision != 0 and revision == old.mutation_revision and incarnation == old.incarnation) return .{ .stamp = old };
    };
    if (try revisions.loadContent(txn, physical)) |content| {
        const stamp = Stamp{ .mutation_revision = revision, .incarnation = incarnation, .digest = content.digest };
        if (!logical) return .{ .stamp = stamp };
        if (keys.isPrimaryDocumentKey(physical)) return .{ .stamp = stamp, .logical_digest = content.digest };
        // A relational content authority hashes its packed row. Verify the
        // request's logical JSON only when initially capturing its failure.
    }
    const raw = txn.get(physical) catch |err| switch (err) {
        error.NotFound => return .{},
        else => return err,
    };
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(raw, &digest, .{});
    const stamp = Stamp{ .mutation_revision = revision, .incarnation = incarnation, .digest = digest };
    if (!logical) return .{ .stamp = stamp, .primary_read = true };
    const value = try materializer.materialize(a, physical, raw);
    defer a.free(value);
    var logical_digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(value, &logical_digest, .{});
    return .{ .stamp = stamp, .logical_digest = logical_digest, .primary_read = true };
}

test "durable enrichment scoped retry validates metadata without reading unchanged primary bodies" {
    const a = std.testing.allocator;
    const physical = try keys.documentKeyAlloc(a, "doc");
    defer a.free(physical);
    const mutation_key = revisions.keyForPhysical(physical).?;
    const Fixture = struct {
        physical: []const u8,
        mutation_key: revisions.Key,
        revision: [8]u8 = undefined,
        content: [49]u8 = undefined,
        body: []const u8 = "old primary",
        reads: usize = 0,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            if (std.mem.eql(u8, key, &self.mutation_key)) return &self.revision;
            if (std.mem.eql(u8, key, &revisions.contentKey(self.mutation_key))) return &self.content;
            if (std.mem.eql(u8, key, self.physical)) {
                self.reads += 1;
                return self.body;
            }
            return error.NotFound;
        }
        pub fn materialize(_: @This(), alloc: std.mem.Allocator, _: []const u8, raw: []const u8) ![]u8 {
            return alloc.dupe(u8, raw);
        }
        fn write(self: *@This(), revision: u64) void {
            std.mem.writeInt(u64, &self.revision, revision, .little);
            std.mem.writeInt(u64, self.content[0..8], revision, .little);
            std.mem.writeInt(u64, self.content[8..16], self.body.len, .little);
            self.content[16] = self.physical[self.physical.len - 1];
            std.crypto.hash.sha2.Sha256.hash(self.body, self.content[17..49], .{});
        }
    };
    var fixture = Fixture{ .physical = physical, .mutation_key = mutation_key };
    fixture.write(1);
    const first = try observe(a, &fixture, physical, "doc", null, true, fixture);
    try std.testing.expectEqual(@as(usize, 0), fixture.reads);
    try std.testing.expect(first.logical_digest != null);
    for (0..128) |_| {
        const current = try observe(a, &fixture, physical, "doc", first.stamp, false, fixture);
        try std.testing.expect(first.stamp.?.sameInput(current.stamp.?));
    }
    try std.testing.expectEqual(@as(usize, 0), fixture.reads);
    // Derived mutations keep an authoritative primary digest at a new revision.
    fixture.write(2);
    const derived = try observe(a, &fixture, physical, "doc", first.stamp, false, fixture);
    try std.testing.expect(first.stamp.?.sameInput(derived.stamp.?));
    try std.testing.expectEqual(@as(usize, 0), fixture.reads);
    fixture.body = "replaced primary";
    fixture.write(3);
    const changed = try observe(a, &fixture, physical, "doc", first.stamp, false, fixture);
    try std.testing.expect(!first.stamp.?.sameInput(changed.stamp.?));
    // An old writer advances only the compatibility revision. Read its bytes
    // once, then reuse the resulting observation until another mutation.
    fixture.body = "old writer primary";
    std.mem.writeInt(u64, &fixture.revision, 4, .little);
    const fallback = try observe(a, &fixture, physical, "doc", changed.stamp, false, fixture);
    try std.testing.expect(fallback.primary_read);
    try std.testing.expectEqual(@as(usize, 1), fixture.reads);
    _ = try observe(a, &fixture, physical, "doc", fallback.stamp, false, fixture);
    try std.testing.expectEqual(@as(usize, 1), fixture.reads);
    fixture.body = "further old writer change";
    std.mem.writeInt(u64, &fixture.revision, 5, .little);
    const later = try observe(a, &fixture, physical, "doc", fallback.stamp, false, fixture);
    try std.testing.expect(!later.stamp.?.sameInput(fallback.stamp.?));
    try std.testing.expectEqual(@as(usize, 2), fixture.reads);
}
