// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable allocation-slot provenance embedded in a retained physical Lease.
//! This epoch identifies the slot's owner, not a content/write generation.
const std = @import("std");
const ownership = @import("storage_ownership.zig");

pub const Slot = struct {
    allocation: u32,
    epoch: u64,
    byte_offset: usize = 0,
    byte_size: usize,
    backing: ownership.Identity,

    pub fn init(allocation: u32, epoch: u64, requested: usize, retained: usize, backing: ownership.Span) !Slot {
        if (epoch == 0 or requested == 0 or requested > retained) return error.InvalidAllocationSlot;
        if (backing.access != .writable or backing.ownership != .allocation) return error.ReadOnlyAllocationSlot;
        const extent = backing.requested_bytes orelse backing.backing_bytes orelse return error.UnknownAllocationSlotExtent;
        if (retained > extent) return error.InvalidAllocationSlot;
        return .{ .allocation = allocation, .epoch = epoch, .byte_size = requested, .backing = backing.identity };
    }
    pub fn eql(self: Slot, other: Slot) bool {
        return self.allocation == other.allocation and self.epoch == other.epoch and self.byte_offset == other.byte_offset and self.byte_size == other.byte_size and std.meta.eql(self.backing, other.backing);
    }
    /// The token points into the Lease's stable storage. Neither a Frame nor
    /// the cloned plan is retained/dereferenced by a published view.
    pub fn token(self: *const Slot) *const ownership.AllocationSlotAuthority {
        return @ptrCast(self);
    }
    pub fn region(self: *const Slot, allocation: u32, epoch: u64, bytes: usize) !ownership.WritableRegion {
        if (allocation != self.allocation or epoch != self.epoch or bytes == 0 or bytes > self.byte_size) return error.InvalidAllocationSlot;
        return .{ .authority = self.token(), .epoch = epoch, .backing = self.backing, .byte_offset = self.byte_offset, .byte_size = bytes };
    }
    pub fn accepts(self: *const Slot, allocation: u32, epoch: u64, bytes: usize, value: ownership.WritableRegion) bool {
        return allocation == self.allocation and epoch == self.epoch and bytes != 0 and bytes <= self.byte_size and value.authority == self.token() and value.epoch == self.epoch and value.byte_offset == self.byte_offset and value.byte_size >= bytes and value.byte_size <= self.byte_size and std.meta.eql(value.backing, self.backing);
    }
};

const Canonical = struct {
    span: ?ownership.Span = null,
    fn emit(raw: *anyopaque, report: ownership.Report) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const value = switch (report) {
            .backing => |span| span,
            .plane => return,
        };
        if (self.span) |old| {
            if (!std.meta.eql(old.identity, value.identity) or old.domain != value.domain or old.access != value.access or old.ownership != value.ownership or old.requested_bytes != value.requested_bytes or old.backing_bytes != value.backing_bytes) return error.AmbiguousAllocationSlot;
        } else self.span = value;
    }
};

/// Ask the actual allocating provider for its authoritative full span. No
/// pointer-tail arithmetic, logical-numel estimate, allocation or submission.
pub fn canonicalBacking(backend: *const @import("../ops/ops.zig").ComputeBackend, tensor: @import("../ops/ops.zig").CT) !ownership.Span {
    var result = Canonical{};
    const functions = backend.vtable.kernels orelse return error.UnknownAllocationSlotExtent;
    if (!functions.accepts(backend.vtable)) return error.UnknownAllocationSlotExtent;
    const query = functions.storageOwnership orelse return error.UnknownAllocationSlotExtent;
    if (try query(backend.ptr, tensor, .{ .context = &result, .visit = Canonical.emit }) != .complete) return error.UnknownAllocationSlotExtent;
    return result.span orelse error.UnknownAllocationSlotExtent;
}

test "slot authority bounds declared writable range independently of a larger backing" {
    const backing = ownership.Span{ .identity = .{ .kind = .host, .base = 4096 }, .domain = .host, .access = .writable, .requested_bytes = 256, .backing_bytes = 512 };
    var slot = try Slot.init(3, 9, 64, 256, backing);
    const allowed = try slot.region(3, 9, 32);
    try std.testing.expect(slot.accepts(3, 9, 32, allowed));
    try std.testing.expectError(error.InvalidAllocationSlot, slot.region(3, 9, 65));
    try std.testing.expectError(error.InvalidAllocationSlot, slot.region(3, 10, 32));
    var forged = allowed;
    forged.byte_size = 256;
    try std.testing.expect(!slot.accepts(3, 9, 32, forged));
    var other = try Slot.init(3, 9, 64, 256, backing);
    try std.testing.expect(!other.accepts(3, 9, 32, allowed));
    const read_only = ownership.Span{ .identity = backing.identity, .domain = .host, .access = .read_only, .requested_bytes = 256, .backing_bytes = 512 };
    try std.testing.expectError(error.ReadOnlyAllocationSlot, Slot.init(3, 9, 64, 256, read_only));
}
