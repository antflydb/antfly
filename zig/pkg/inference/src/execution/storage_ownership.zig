const std = @import("std");
const admission = @import("storage_admission.zig");

/// Stable live backing identity. Host base addresses are process-wide; CUDA
/// pointers are namespaced by the allocating context; Metal uses the original
/// MTLBuffer identity. Views must resolve their original base, never an offset.
pub const Identity = struct {
    kind: enum { host, cuda, metal },
    context: ?*anyopaque = null,
    base: usize,
};

pub const Domain = enum { host, backend };
pub const Access = enum { writable, read_only, unknown };
pub const Span = struct {
    identity: Identity,
    domain: Domain,
    /// Read-only is an owner fact (for physical literals, Lease.constant),
    /// inherited by every alias. Unknown must never authorize a write.
    access: Access = .unknown,
    /// Complete retained buffer range, not the logical tensor/view length.
    requested_bytes: ?usize,
    backing_bytes: ?usize,
    charged: ?struct {
        identity: admission.Identity,
        /// Exact owned quota; a failed conservative shrink may keep more than
        /// the measured span. Coverage credit is capped to retained span size.
        bytes: usize,
    } = null,
    ownership: enum { allocation, borrowed_model, unknown } = .allocation,
};

/// Owned by the executable allocation-slot planner. A retained backing span
/// or an interior logical view cannot mint this authority.
pub const AllocationSlotAuthority = opaque {};
pub const WritableRegion = struct {
    authority: *const AllocationSlotAuthority,
    epoch: u64,
    backing: Identity,
    byte_offset: usize,
    /// Exclusive writable extent of this slot, not the whole backing tail.
    byte_size: usize,
};
pub const Plane = struct {
    /// Physical shape/dtype/layout; logical graph dtype may differ. Reuse
    /// TensorDesc for packed quant, reduced mirrors and observed residency.
    descriptor: @import("../graph/backend_contracts.zig").TensorDesc,
    /// Exact backend physical packing profile, namespaced by implementation.
    /// Null is unknown, not the dense/default profile.
    layout_key: ?u64 = null,
    /// A pinned immutable owner may report a stable lifetime epoch; mutable
    /// representation changes must advance it. Null prevents cache proof.
    storage_epoch: ?u64 = null,
    role: enum { primary, bf16_mirror, mean, inverse_std, packed_weight, host_mirror },
    /// Null describes a not-yet-allocated planned plane. An observed actual
    /// binding requires a canonical backing identity before ownership credit.
    backing: ?Identity = null,
    byte_offset: usize,
    /// Minimum alignment of the allocation BASE, not this view's address.
    /// Planned facts come from the accepted physical allocator policy; unknown
    /// observed/custom storage stays at one. Consumers account for byte_offset.
    base_alignment: u32 = 1,
    logical_bytes: usize,
    /// Initialized readable data for the selected physical representation.
    readable_bytes: usize,
    /// Extra padded rows are writable only under allocation-slot authority.
    /// Readback and normal consumers retain logical_bytes and logical shape.
    writable: ?WritableRegion = null,
};
pub const Report = union(enum) { backing: Span, plane: Plane };

pub const Coverage = enum { complete, unknown };
/// Stream cold metadata instead of returning a large fixed multi-span struct.
/// Providers allocate nothing and submit no device work. The caller may dedup
/// many layer/KV views and can propagate its own metadata allocation failures.
pub const Visitor = struct {
    context: *anyopaque,
    visit: *const fn (*anyopaque, Report) anyerror!void,
    pub fn emit(self: Visitor, report: Report) !void {
        return self.visit(self.context, report);
    }
};

/// Suggested replacement for the existing physicalStorageBytes callback:
/// storageOwnership(ctx,tensor,Visitor) anyerror!Coverage. A common byte-only
/// projection preserves Ledger use while one provider owns all backing facts.
pub const BackingBytes = struct {
    value: usize = 0,
    known: bool = true,
    fn add(raw: *anyopaque, report: Report) !void {
        const span = switch (report) {
            .backing => |span| span,
            .plane => return,
        };
        const self: *@This() = @ptrCast(@alignCast(raw));
        const bytes = span.backing_bytes orelse {
            self.known = false;
            return;
        };
        self.value = try std.math.add(usize, self.value, bytes);
    }
    pub fn visitor(self: *@This()) Visitor {
        return .{ .context = self, .visit = add };
    }
};

/// Cold whole-result collector. The caller owns its allocator and pins every
/// tensor/provider while collecting; this stores no borrowed tensor pointers.
pub const Retained = struct {
    allocator: std.mem.Allocator,
    spans: std.AutoHashMapUnmanaged(Identity, Span) = .empty,
    complete: bool = true,

    pub fn deinit(self: *Retained) void {
        self.spans.deinit(self.allocator);
    }
    pub fn visitor(self: *Retained) Visitor {
        return .{ .context = self, .visit = add };
    }
    pub fn noteCoverage(self: *Retained, coverage: Coverage) void {
        if (coverage != .complete) self.complete = false;
    }
    fn add(raw: *anyopaque, report: Report) !void {
        const self: *Retained = @ptrCast(@alignCast(raw));
        const span = switch (report) {
            .backing => |span| span,
            .plane => return,
        };
        const entry = try self.spans.getOrPut(self.allocator, span.identity);
        if (!entry.found_existing) {
            entry.value_ptr.* = span;
            return;
        }
        const old = entry.value_ptr;
        // Distinct views must resolve the same allocating owner's full span.
        // Conflicting facts cannot silently shrink a retained-output quota.
        if (old.domain != span.domain or old.requested_bytes != span.requested_bytes or old.backing_bytes != span.backing_bytes or old.access != span.access or old.ownership != span.ownership)
            return error.InconsistentStorageOwnership;
        if (old.charged) |charge| {
            const repeated = span.charged orelse return error.InconsistentStorageOwnership;
            if (!charge.identity.eql(repeated.identity) or charge.bytes != repeated.bytes)
                return error.InconsistentStorageOwnership;
        } else if (span.charged != null) return error.InconsistentStorageOwnership;
    }
    /// Exact uncovered full retained storage, not a sum of logical KV views.
    /// Existing same-controller owners continue to carry their own quota.
    /// Unknown storage preserves the caller's compatibility reservation.
    pub const Uncovered = struct { host: usize, backend: usize };
    pub fn uncovered(self: *const Retained, identity: admission.Identity) !?Uncovered {
        if (!self.complete) return null;
        var result = Uncovered{ .host = 0, .backend = 0 };
        var it = self.spans.valueIterator();
        while (it.next()) |span| {
            const bytes = span.backing_bytes orelse span.requested_bytes orelse return null;
            if (span.ownership == .unknown or (span.ownership == .borrowed_model and span.charged == null)) return null;
            const covered = if (span.charged) |charge|
                (if (charge.identity.eql(identity)) @min(bytes, charge.bytes) else 0)
            else
                0;
            const amount = bytes - covered;
            switch (span.domain) {
                .host => result.host = try std.math.add(usize, result.host, amount),
                .backend => result.backend = try std.math.add(usize, result.backend, amount),
            }
        }
        return result;
    }
};

/// Cold publication guard. Unknown storage cannot hide a known read-only
/// retained owner. The full requested span bounds layout-preserving copies.
pub const ReadOnly = struct {
    value: bool = false,
    largest_requested: usize = 0,
    fn add(raw: *anyopaque, report: Report) !void {
        const self: *ReadOnly = @ptrCast(@alignCast(raw));
        switch (report) {
            .backing => |span| {
                if (span.access == .read_only) self.value = true;
                self.largest_requested = @max(self.largest_requested, span.requested_bytes orelse 0);
            },
            .plane => {},
        }
    }
    pub fn visitor(self: *ReadOnly) Visitor {
        return .{ .context = self, .visit = add };
    }
};
