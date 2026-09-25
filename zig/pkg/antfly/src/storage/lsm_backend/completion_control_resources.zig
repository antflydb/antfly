// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Physical resources for one transaction's control lifecycle. Construction is
//! ordinary, fallible admission work. Capturing another proposal never consumes
//! the monotonic publication span or reacquires ordinary memory/WAL admission.
//! This module supplies backing, not consensus or durable-restoration authority.
//! The caller must hold native transaction/DB serialization through every
//! method and candidate callback. `busy` rejects reentrancy, not concurrent use.
const std = @import("std");
const domains = @import("completion_allocator.zig");
const resources = @import("../resource_manager.zig");
const protocol = @import("../../common/completion_entry_protocol.zig");
const accepted_guard = @import("completion_control_accepted.zig");

pub const Limits = struct {
    publication_bytes: usize,
    wal_bytes: u64,
    proposal_bytes: usize = protocol.max_wire_bytes,
    accepted_bytes: usize = accepted_guard.max_bytes,
};

pub const Resources = struct {
    control: *domains.RecyclingScratch,
    publication_domain: *domains.RecyclingScratch,
    publication: *domains.PublicationReservation,
    /// Pool-owned, shared, and heap-stable through destruction of this owner.
    compiler: *domains.CompilerWorkspace,
    txn_id: [16]u8,
    proposal: []u8,
    accepted: []u8,
    accepted_len: usize = 0,
    busy: bool = false,
    wal_credit: u64 = 0,
    wal_pin: resources.ObserverMetadataPin,

    pub fn create(backing: std.mem.Allocator, manager: *resources.ResourceManager, compiler: *domains.CompilerWorkspace, txn_id: [16]u8, limits: Limits) !*Resources {
        if (limits.publication_bytes == 0 or limits.wal_bytes == 0 or limits.proposal_bytes == 0 or limits.proposal_bytes > protocol.max_wire_bytes or
            limits.accepted_bytes == 0 or limits.accepted_bytes > accepted_guard.max_bytes)
            return error.UnsupportedCompletionProfile;
        const footprint = domains.RecyclingScratch.allocationFootprint;
        const control_bytes = try std.math.add(usize, try std.math.add(usize, try footprint(@sizeOf(Resources), @alignOf(Resources)), try footprint(limits.proposal_bytes, 1)), try footprint(limits.accepted_bytes, 1));
        const control = try domains.RecyclingScratch.create(backing, manager, control_bytes);
        errdefer control.retire();
        const allocator = control.allocator();
        const self = try allocator.create(Resources);
        errdefer allocator.destroy(self);
        const proposal = try allocator.alloc(u8, limits.proposal_bytes);
        errdefer allocator.free(proposal);
        const accepted = try allocator.alloc(u8, limits.accepted_bytes);
        errdefer allocator.free(accepted);
        const publication_domain = try domains.RecyclingScratch.create(backing, manager, try domains.PublicationReservation.backingFootprint(limits.publication_bytes));
        errdefer publication_domain.retire();
        const publication = try domains.PublicationReservation.create(publication_domain, limits.publication_bytes);
        errdefer publication.finish();
        self.* = .{
            .control = control,
            .publication_domain = publication_domain,
            .publication = publication,
            .compiler = compiler,
            .txn_id = txn_id,
            .proposal = proposal,
            .accepted = accepted,
            .wal_pin = undefined,
        };
        self.wal_pin = try manager.pinObserverMetadata(.lsm_wal_retention, &self.wal_credit);
        errdefer self.wal_pin.release() catch unreachable;
        try manager.adjustUsage(.lsm_wal_retention, &self.wal_credit, limits.wal_bytes);
        return self;
    }

    /// Capture uses the critical compiler scope, then releases it BEFORE native
    /// proposal admission can reenter that workspace. The candidate callback
    /// borrows the separate proposal buffer synchronously; it must copy/consume
    /// those bytes before returning. No borrowed bytes may escape the callback.
    /// Capture itself leaves publication capacity and WAL credit untouched.
    /// Durable admission/publication consume those through their separate paths.
    pub fn withCandidate(self: *Resources, comptime T: type, compile_context: anytype, comptime compile: anytype, candidate_context: anytype, comptime candidate: anytype) !T {
        if (self.busy) return error.CompletionReservationBusy;
        self.busy = true;
        defer self.busy = false;
        const Capture = struct {
            owner: *Resources,
            input: @TypeOf(compile_context),

            fn run(context: @This(), alloc: std.mem.Allocator) !usize {
                const wire = try compile(context.input, alloc);
                defer alloc.free(wire);
                if (wire.len == 0 or wire.len > context.owner.proposal.len) return error.CompletionPlanCapacityExceeded;
                @memcpy(context.owner.proposal[0..wire.len], wire);
                return wire.len;
            }
        };
        const length = try self.compiler.withCompletion(usize, Capture{ .owner = self, .input = compile_context }, Capture.run);
        return candidate(candidate_context, @as([]const u8, self.proposal[0..length]));
    }

    /// Must precede the WAL append attempt. Once transferred, failed/uncertain
    /// I/O retains the charge at the backend; dropping this owner does not
    /// pretend those bytes were durably checkpointed or canceled.
    pub fn chargeWal(self: *Resources, tracked_backend_bytes: *u64, bytes: u64) !void {
        if (bytes > self.wal_credit) return error.CompletionPlanCapacityExceeded;
        const next = std.math.add(u64, tracked_backend_bytes.*, bytes) catch return error.CompletionPlanCapacityExceeded;
        try self.wal_pin.manager.transferUsage(.lsm_wal_retention, &self.wal_credit, self.wal_credit - bytes, tracked_backend_bytes, next);
    }

    /// Only a durable terminal checkpoint, authoritative log cancellation, or
    /// quiesced process teardown may release the owning handle. Reader-held
    /// publication nodes retain their allocation domain after this returns.
    pub fn destroy(self: *Resources) void {
        std.debug.assert(!self.busy);
        self.wal_pin.manager.observeUsage(.lsm_wal_retention, &self.wal_credit, 0);
        self.wal_pin.release() catch unreachable;
        self.publication.finish();
        self.publication_domain.retire();
        const control = self.control;
        const allocator = control.allocator();
        allocator.free(self.proposal);
        allocator.free(self.accepted);
        allocator.destroy(self);
        control.retire();
    }
};

test "workload admission completion compiler control resources preserve capacity across canceled captures" {
    const alloc = std.testing.allocator;
    var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    var backing = std.testing.FailingAllocator.init(alloc, .{});
    var compiler = try domains.CompilerWorkspace.init(backing.allocator(), &manager, 64 * 1024);
    defer compiler.deinit() catch unreachable;
    const held = try Resources.create(backing.allocator(), &manager, &compiler, @splat(19), .{ .publication_bytes = 64 * 1024, .wal_bytes = 64 * 1024, .proposal_bytes = 4096 });
    var destroyed = false;
    defer if (!destroyed) held.destroy();
    var tracked_backend_bytes: u64 = 0;
    var backend_pin = try manager.pinObserverMetadata(.lsm_wal_retention, &tracked_backend_bytes);
    defer backend_pin.release() catch unreachable;
    defer manager.observeUsage(.lsm_wal_retention, &tracked_backend_bytes, 0);
    const available = held.publication.remainingBytes();
    const credit = held.wal_credit;
    backing.fail_index = backing.alloc_index;
    backing.resize_fail_index = backing.resize_index;
    manager.memory.budget.hard_limit_bytes = 1;
    compiler.generation = std.math.maxInt(u64);
    const Callbacks = struct {
        fn compile(size: usize, scratch: std.mem.Allocator) ![]u8 {
            const wire = try scratch.alloc(u8, size);
            @memset(wire, 0x51);
            return wire;
        }
        fn overwriteScratch(_: void, scratch: std.mem.Allocator) !void {
            const bytes = try scratch.alloc(u8, 8192);
            defer scratch.free(bytes);
            @memset(bytes, 0xa9);
        }
        fn cancel(owner: *Resources, wire: []const u8) !void {
            try std.testing.expectEqual(@as(usize, 4096), wire.len);
            try std.testing.expect(std.mem.allEqual(u8, wire, 0x51));
            // Native admission may use the compiler again once capture ends.
            try owner.compiler.withCompletion(void, {}, overwriteScratch);
            try std.testing.expect(std.mem.allEqual(u8, wire, 0x51));
            try std.testing.expectError(error.CompletionReservationBusy, owner.withCandidate(usize, @as(usize, 1), compile, {}, accept));
            return error.NotProposed;
        }
        fn accept(_: void, wire: []const u8) !usize {
            return wire.len;
        }
    };
    for (0..256) |_| {
        try std.testing.expectError(error.NotProposed, held.withCandidate(void, @as(usize, 4096), Callbacks.compile, held, Callbacks.cancel));
        try std.testing.expectEqual(available, held.publication.remainingBytes());
        try std.testing.expectEqual(credit, held.wal_credit);
    }
    try std.testing.expectError(error.CompletionPlanCapacityExceeded, held.withCandidate(usize, @as(usize, 4097), Callbacks.compile, {}, Callbacks.accept));
    try std.testing.expectEqual(@as(usize, 4096), try held.withCandidate(usize, @as(usize, 4096), Callbacks.compile, {}, Callbacks.accept));
    try std.testing.expectError(error.CompletionPlanCapacityExceeded, held.chargeWal(&tracked_backend_bytes, credit + 1));
    try std.testing.expectEqual(credit, held.wal_credit);
    var overflow: u64 = std.math.maxInt(u64);
    try std.testing.expectError(error.CompletionPlanCapacityExceeded, held.chargeWal(&overflow, 1));
    try std.testing.expectEqual(credit, held.wal_credit);
    try held.chargeWal(&tracked_backend_bytes, 1024);
    try std.testing.expectEqual(@as(u64, 1024), tracked_backend_bytes);
    const publication_allocator = held.publication.allocator();
    const reader = try publication_allocator.alloc(u8, 32 * 1024);
    @memset(reader, 0x73);
    held.destroy();
    destroyed = true;
    try std.testing.expectEqual(@as(u64, 1024), tracked_backend_bytes);
    try std.testing.expect(std.mem.allEqual(u8, reader, 0x73));
    publication_allocator.free(reader);
    try std.testing.expect(!backing.has_induced_failure);
}

test "workload admission completion compiler control resource construction unwinds failed allocations" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var manager = resources.ResourceManager.init(.{ .identity_allocator = alloc });
            defer manager.deinit(alloc);
            var compiler = try domains.CompilerWorkspace.init(alloc, &manager, 4096);
            defer compiler.deinit() catch unreachable;
            const before = manager.snapshot().memory.used_bytes;
            const held = Resources.create(alloc, &manager, &compiler, @splat(21), .{
                .publication_bytes = 4096,
                .wal_bytes = 4096,
                .proposal_bytes = 512,
            }) catch |err| {
                try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
                return err;
            };
            held.destroy();
            try std.testing.expectEqual(before, manager.snapshot().memory.used_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
