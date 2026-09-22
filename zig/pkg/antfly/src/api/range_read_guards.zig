// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Server-authored, owner-routed snapshot observations. Native counter keys
//! must never be routed as ordinary document keys. Admission callbacks/clocks
//! are process-local and are deliberately stripped from durable ownership.
const std = @import("std");
const metadata = @import("../metadata/api.zig");
pub const Proof = @import("../storage/range_protection.zig").Proof;
pub const OwnerRangeProof = struct { fence: metadata.CatalogRouteFence, proofs: []const Proof };
pub const max_owners = 4096;
pub const max_proofs = 16384;

pub const Owned = struct {
    arena: std.heap.ArenaAllocator,
    value: []const OwnerRangeProof,
    pub fn deinit(self: *Owned) void {
        self.arena.deinit();
        self.* = undefined;
    }
};

fn sameOwner(a: metadata.CatalogRouteFence, b: metadata.CatalogRouteFence) bool {
    return a.protocol == b.protocol and a.metadata_group_id == b.metadata_group_id and
        std.meta.eql(a.metadata_incarnation, b.metadata_incarnation) and
        a.table_id == b.table_id and a.topology_epoch == b.topology_epoch and std.meta.eql(a.route, b.route);
}

pub fn validate(input: []const OwnerRangeProof) !void {
    if (input.len > max_owners) return error.TransactionTooLarge;
    var count: usize = 0;
    for (input) |owner| {
        try owner.fence.validate();
        if (owner.fence.metadata_incarnation == null or owner.proofs.len == 0 or owner.proofs.len > 257) return error.InvalidTransactionSessionRecord;
        count += owner.proofs.len;
        if (count > max_proofs) return error.TransactionTooLarge;
        for (owner.proofs) |proof| if (proof.bucket > 256) return error.InvalidTransactionSessionRecord;
    }
}

/// Later statements cannot replace an earlier observation with a new value:
/// staged SQL writes have not touched physical counters yet. Detect a changed
/// range before exposing that statement's buffered results.
pub fn merge(alloc: std.mem.Allocator, previous: []const OwnerRangeProof, incoming: []const OwnerRangeProof) !Owned {
    try validate(previous);
    try validate(incoming);
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const Entry = struct { fence: metadata.CatalogRouteFence, proofs: std.AutoHashMapUnmanaged(u16, ?u64) = .empty };
    var entries: std.ArrayList(Entry) = .empty;
    var owners: std.AutoHashMapUnmanaged(u64, usize) = .empty;
    var count: usize = 0;
    for ([_][]const OwnerRangeProof{ previous, incoming }) |input| for (input) |owner| {
        const slot = try owners.getOrPut(owned, owner.fence.route.group_id);
        if (!slot.found_existing) {
            if (entries.items.len == max_owners) return error.TransactionTooLarge;
            slot.value_ptr.* = entries.items.len;
            var fence = owner.fence;
            fence.admission_deadline_ns = null;
            fence.admission_deadline_io = null;
            fence.admission_cancellation = .none;
            try entries.append(owned, .{ .fence = fence });
        }
        const entry = &entries.items[slot.value_ptr.*];
        if (!sameOwner(entry.fence, owner.fence)) return error.CatalogGenerationChanged;
        entry.fence.catalog_revision = @max(entry.fence.catalog_revision, owner.fence.catalog_revision);
        for (owner.proofs) |proof| {
            const observed = try entry.proofs.getOrPut(owned, proof.bucket);
            if (observed.found_existing) {
                if (observed.value_ptr.* != proof.generation) return error.SqlWriteConflict;
            } else {
                count += 1;
                if (count > max_proofs) return error.TransactionTooLarge;
                observed.value_ptr.* = proof.generation;
            }
        }
    };
    const result = try owned.alloc(OwnerRangeProof, entries.items.len);
    for (entries.items, result) |*entry, *out| {
        const proofs = try owned.alloc(Proof, entry.proofs.count());
        var iterator = entry.proofs.iterator();
        for (proofs) |*proof| {
            const value = iterator.next().?;
            proof.* = .{ .bucket = value.key_ptr.*, .generation = value.value_ptr.* };
        }
        std.mem.sort(Proof, proofs, {}, struct {
            fn less(_: void, a: Proof, b: Proof) bool {
                return a.bucket < b.bucket;
            }
        }.less);
        out.* = .{ .fence = entry.fence, .proofs = proofs };
    }
    std.mem.sort(OwnerRangeProof, result, {}, struct {
        fn less(_: void, a: OwnerRangeProof, b: OwnerRangeProof) bool {
            return a.fence.route.group_id < b.fence.route.group_id;
        }
    }.less);
    return .{ .arena = arena, .value = result };
}

test "distributed txn SQL range observations retain first snapshot and reject owner or generation changes" {
    const Case = struct {
        fn run(alloc: std.mem.Allocator) !void {
            const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .catalog_revision = 2, .table_id = 3, .topology_epoch = 4, .route = .{ .group_id = 5, .range_id = 6, .identity_namespace = .{ .table_id = 3, .shard_id = 5, .range_id = 6 } }, .admission_deadline_ns = 99 };
            const first = OwnerRangeProof{ .fence = fence, .proofs = &.{ .{ .bucket = 2, .generation = null }, .{ .bucket = 1, .generation = 7 } } };
            var initial = try merge(alloc, &.{}, &.{first});
            defer initial.deinit();
            var again = try merge(alloc, initial.value, &.{first});
            defer again.deinit();
            try std.testing.expectEqual(@as(usize, 2), again.value[0].proofs.len);
            try std.testing.expectEqual(@as(u16, 1), again.value[0].proofs[0].bucket);
            try std.testing.expectEqual(null, again.value[0].fence.admission_deadline_ns);
            var changed = first;
            changed.proofs = &.{.{ .bucket = 1, .generation = 8 }};
            if (merge(alloc, initial.value, &.{changed})) |unexpected| {
                var value = unexpected;
                value.deinit();
                return error.TestExpectedError;
            } else |err| if (err != error.SqlWriteConflict) return err;
            changed = first;
            changed.fence.table_id = 9;
            changed.fence.route.identity_namespace.table_id = 9;
            if (merge(alloc, initial.value, &.{changed})) |unexpected| {
                var value = unexpected;
                value.deinit();
                return error.TestExpectedError;
            } else |err| if (err != error.CatalogGenerationChanged) return err;
            const bytes = try std.json.Stringify.valueAlloc(alloc, again.value, .{});
            defer alloc.free(bytes);
            var decoded = try std.json.parseFromSlice([]const OwnerRangeProof, alloc, bytes, .{});
            defer decoded.deinit();
            try validate(decoded.value);
            try std.testing.expectEqual(null, decoded.value[0].proofs[1].generation);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}
