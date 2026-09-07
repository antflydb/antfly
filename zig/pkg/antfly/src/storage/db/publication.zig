// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (https://www.elastic.co/licensing/elastic-license).

const std = @import("std");

// These epochs order local writable owners, not remote nodes or durable
// incarnations. Neither epochs nor stamps cross the status wire protocol.
var next_owner_epoch: std.atomic.Value(u64) = .init(1);

pub fn allocateOwnerEpoch() u64 {
    const epoch = next_owner_epoch.fetchAdd(1, .monotonic);
    std.debug.assert(epoch != 0 and epoch != std.math.maxInt(u64));
    return epoch;
}

/// A coherent owner observation of one payload. The enclosing index supplies
/// the exact name/kind/incarnation/config/root identity. Serving and coverage
/// have separate stamps because neither may borrow the other's replay proof.
/// `recovered_through` is issued only after resident admission and checkpoint
/// validation, never just because a new owner exists.
pub const Stamp = struct {
    owner_epoch: u64,
    revision: u64,
    applied_through: u64,
    recovered_through: ?u64 = null,

    pub const Order = enum { older, identical, newer, unproven };

    pub fn order(incoming: Stamp, cached: Stamp) Order {
        if (incoming.owner_epoch == cached.owner_epoch) {
            if (incoming.revision < cached.revision) return .older;
            if (incoming.revision == cached.revision) return .identical;
            // New observations cannot claim a smaller applied prefix. This
            // is an ordering check, not a cardinality comparison.
            return if (incoming.applied_through >= cached.applied_through) .newer else .unproven;
        }
        if (incoming.owner_epoch < cached.owner_epoch) return .older;
        const recovered = incoming.recovered_through orelse return .unproven;
        return if (recovered >= cached.applied_through and incoming.applied_through >= recovered)
            .newer
        else
            .unproven;
    }
};

test "publication stamps order revisions and require recovery across owners" {
    const original = Stamp{ .owner_epoch = 1, .revision = 3, .applied_through = 7 };
    var next = original;
    try std.testing.expectEqual(Stamp.Order.identical, next.order(original));
    next.revision = 4;
    try std.testing.expectEqual(Stamp.Order.newer, next.order(original));
    next.applied_through = 6;
    try std.testing.expectEqual(Stamp.Order.unproven, next.order(original));
    next = .{ .owner_epoch = 2, .revision = 1, .applied_through = 8 };
    try std.testing.expectEqual(Stamp.Order.unproven, next.order(original));
    next.recovered_through = 6;
    try std.testing.expectEqual(Stamp.Order.unproven, next.order(original));
    next.recovered_through = 7;
    try std.testing.expectEqual(Stamp.Order.newer, next.order(original));
    try std.testing.expectEqual(Stamp.Order.older, original.order(next));
}
