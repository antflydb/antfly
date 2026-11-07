// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold existing-root receiving custody. This owns no Model, provider, SDK,
//! Io, caller allocator or fresh-storage credit. Only process metadata and
//! independently pinned root uses may survive actual execution teardown.
const std = @import("std");
const platform = @import("antfly_platform");
const roles = @import("../backends/metal_workspace_roles.zig");
const workspace = @import("../backends/metal_workspace_ownership.zig");
const retirement = @import("resource_retirement.zig");
const sink = @import("retirement_sink.zig");
const Cleanup = @import("cleanup_context.zig").Context;

pub const Owner = struct {
    allocator: std.mem.Allocator,
    runtime_address: usize,
    expected: roles.Snapshot,
    validation: roles.Snapshot,
    final_owned: roles.Snapshot,
    roots: []*workspace.Root,
    bank: workspace.UseBank,
    receiver: workspace.ReceiverUses,
    node: retirement.Node = undefined,
    retired: bool = false,

    /// Every fallible allocation precedes the Snapshot ownership move. The
    /// supplied snapshot has canonical pure pins acquired under actual source
    /// exclusion. The caller releases its temporary target OUTSIDE all lanes.
    pub fn prepareTake(expected: *roles.Snapshot, runtime_address: usize, epoch: *const sink.Epoch) !*Owner {
        return prepareTakeWithAllocator(platform.allocator.processAllocator(platform.allocator.concurrentFallback()), expected, runtime_address, epoch) catch |err| {
            // Snapshot metadata pins do not promise a live SDK creator. A
            // successful peer retirement/shrink can win before beginUse.
            // Only these actual pre-entry races permit a full cold restart.
            if (preEntryRace(err)) return error.WorkspaceReceiptChangedBeforeExecution;
            return err;
        };
    }
    fn preEntryRace(err: anyerror) bool {
        return err == error.BackingAlreadyRetired or err == error.BackingClosing or err == error.ResourceTemporarilyUnavailable;
    }
    fn prepareTakeWithAllocator(a: std.mem.Allocator, expected: *roles.Snapshot, runtime_address: usize, epoch: *const sink.Epoch) !*Owner {
        if (runtime_address == 0 or !expected.captured) return error.InvalidWorkspaceCensus;
        if (try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        const self = try a.create(Owner);
        errdefer a.destroy(self);
        var validation = try roles.Snapshot.prepare(expected.entries.len);
        errdefer validation.deinitOutsideExecution();
        // Old-only scopes cannot create modern roots. New unmanaged pools do
        // not enlarge captureOwned's tagged-only bound or earn any new credit.
        var root_count: usize = 0;
        for (expected.entries[0..expected.count]) |entry| if (entry.root != null) {
            root_count += 1;
        };
        var final_owned = try roles.Snapshot.prepare(@max(root_count, 1));
        errdefer final_owned.deinitOutsideExecution();
        const roots = try a.alloc(*workspace.Root, root_count);
        errdefer a.free(roots);
        const selected = try expected.copyRoots(roots);
        var bank = try workspace.UseBank.prepareExact(selected.len);
        errdefer bank.deinitOutsideExecution();
        try bank.bindOutsideExecution(selected, epoch);
        self.* = .{ .allocator = a, .runtime_address = runtime_address, .expected = expected.*, .validation = validation, .final_owned = final_owned, .roots = roots, .bank = bank, .receiver = undefined };
        self.receiver = .{ .existing = &self.bank }; // final address, no Phase
        expected.* = undefined; // caller transfers only after full success
        return self;
    }

    /// Synchronous actual receiver only. Caller supplies its NEW Engine token
    /// through Context; this object never infers a numerical operation or B.
    pub fn receiverUses(self: *Owner) !*workspace.ReceiverUses {
        if (self.retired) return error.WorkspaceReceiptRetired;
        return &self.receiver;
    }

    pub fn retireWithCleanup(self: *Owner, current: Cleanup) !void {
        if (self.retired) return error.WorkspaceReceiptRetired;
        if (self.receiver.bound_operation != null or self.receiver.allocations != null) return error.WorkspaceReceiverStillBound;
        if (current.boundary) |epoch| {
            if (try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
            const boundary = try epoch.boundary();
            self.retired = true;
            errdefer self.retired = false;
            self.node = .{ .context = self, .release_fn = destroyNode };
            try boundary.register(boundary.context, boundary.nonce, &self.node);
            return; // no self access after independently owned publication
        }
        if (!current.outside_execution) return error.MissingWorkspaceCleanupEpoch;
        self.retired = true;
        destroyNode(self);
    }
    fn destroyNode(raw: *anyopaque) callconv(.c) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        std.debug.assert(self.retired and self.receiver.bound_operation == null and self.receiver.allocations == null);
        // Queued use nodes hold independent cell references. They may drain
        // before or after this metadata node; neither path borrows this Owner.
        self.bank.deinitOutsideExecution();
        self.expected.deinitOutsideExecution();
        self.validation.deinitOutsideExecution();
        self.final_owned.deinitOutsideExecution();
        const a = self.allocator;
        a.free(self.roots);
        a.destroy(self);
    }
};

fn receiptMetadataCase(a: std.mem.Allocator) !void {
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("storage_admission.zig");
    const Case = struct {
        a: std.atomic.Mutex = .unlocked,
        b: std.atomic.Mutex = .unlocked,
        unsafe_callback: bool = false,
        metadata_deaths: usize = 0,
        callbacks: usize = 0,
        fn check(self: *@This()) void {
            self.callbacks += 1;
            if (self.a.tryLock()) self.a.unlock() else self.unsafe_callback = true;
            if (self.b.tryLock()) self.b.unlock() else self.unsafe_callback = true;
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            return 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn dead(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.metadata_deaths += 1;
        }
    };
    var probe: Case = .{};
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Case.reserve, .retain = Case.retain, .release = Case.release });
    var reservation: ?storage.Reservation = .{ .identity = .{ .controller = &controller, .backend_class = .gpu }, .domain = .backend, .leases = .{ try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null } };
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try retirement.Charge.createReservation(&reservation, &domain);
    var root: workspace.Root = undefined;
    root.init(charge, .{ .context = &probe, .release = Case.dead });
    const sdk_pin = root.backing.retainMetadataPin();
    var sdk_live = true;
    defer if (sdk_live) sdk_pin.release(sdk_pin.context);
    var reference = try sink.Ref.create();
    defer reference.release();
    var source = try reference.enter();
    defer source.finish();
    var source_epoch = try source.retainEpoch();
    defer source_epoch.release();
    var creator_live = true;
    defer if (creator_live) root.releaseCreator(.{ .boundary = &source_epoch }) catch @panic("pure receipt test creator cleanup failed");
    var expected = try roles.Snapshot.prepare(1);
    var expected_live = true;
    defer if (expected_live) expected.deinitOutsideExecution();
    expected.entries[0] = .{ .buffer_identity = 0x1000, .root = &root, .operations = &workspace.RootOperations.canonical, .bytes = 64, .birth = root.identity, .tagged = 1, .metadata_retained = 1 };
    expected.metadata[0] = root.retainSnapshot();
    expected.count = 1;
    expected.generation = 1;
    expected.captured = true;
    const owner = try Owner.prepareTakeWithAllocator(a, &expected, 0x1000, &source_epoch);
    expected_live = false;
    var owner_live = true;
    defer if (owner_live) owner.retireWithCleanup(.{ .outside_execution = true }) catch @panic("pure receipt metadata cleanup failed");
    try std.testing.expectEqual(@as(usize, 1), owner.bank.count);
    try std.testing.expectEqual(@as(usize, 1), owner.bank.entries.len);
    try std.testing.expect((try owner.receiverUses()).allocations == null);
    owner.receiver.allocations = @ptrFromInt(@alignOf(workspace.Phase));
    try std.testing.expectError(error.WorkspaceReceiverStillBound, owner.retireWithCleanup(.{ .boundary = &source_epoch }));
    owner.receiver.allocations = null;
    var consumer = try reference.enter();
    defer consumer.finish();
    var consumer_epoch = try consumer.retainEpoch();
    defer consumer_epoch.release();
    try std.testing.expect(probe.a.tryLock());
    var a_owned = true;
    defer if (a_owned) probe.a.unlock();
    try std.testing.expect(probe.b.tryLock());
    var b_owned = true;
    defer if (b_owned) probe.b.unlock();
    try root.releaseCreator(.{ .boundary = &source_epoch });
    creator_live = false;
    sdk_pin.release(sdk_pin.context);
    sdk_live = false; // pure SDK-death witness only
    try owner.retireWithCleanup(.{ .boundary = &consumer_epoch });
    owner_live = false; // consumes before a callback-capable final boundary
    probe.a.unlock();
    a_owned = false;
    source.finish();
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), probe.metadata_deaths);
    probe.b.unlock();
    b_owned = false;
    consumer.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), probe.metadata_deaths);
    try std.testing.expect(!probe.unsafe_callback);
}

test "existing workspace receipt moves pure metadata once and keeps quota until actual foreign cleanup B" {
    try receiptMetadataCase(std.testing.allocator);
}

test "existing workspace receipt own header and root-array OOM preserve input snapshot and unwind outside gates" {
    // Snapshot/UseBank/Fence factories retain their independent shared tests;
    // this enumerates ONLY this holder's newly owned allocation boundaries.
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, receiptMetadataCase, .{});
}

test "existing Root retired after snapshot refuses before ownership move as a cold pre-entry race" {
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("storage_admission.zig");
    const Dead = struct {
        fn release(_: *anyopaque) void {}
    };
    var marker: u8 = 0;
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var reservation: ?storage.Reservation = .{ .identity = .{ .controller = &controller, .backend_class = .gpu }, .domain = .backend, .leases = .{ try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null } };
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try retirement.Charge.createReservation(&reservation, &domain);
    var root: workspace.Root = undefined;
    root.init(charge, .{ .context = &marker, .release = Dead.release });
    var expected = try roles.Snapshot.prepare(1);
    var expected_live = true;
    defer if (expected_live) expected.deinitOutsideExecution();
    expected.entries[0] = .{ .buffer_identity = 0x1000, .root = &root, .operations = &workspace.RootOperations.canonical, .bytes = 64, .birth = root.identity, .tagged = 1, .metadata_retained = 1 };
    expected.metadata[0] = root.retainSnapshot();
    expected.count = 1;
    expected.generation = 1;
    expected.captured = true;
    try root.releaseCreator(.{ .outside_execution = true });
    // Snapshot keeps the pure metadata/quota pin, even after SDK/creator
    // retirement. Failed preparation must leave that exact pin untouched.
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    var ref = try sink.Ref.create();
    defer ref.release();
    var guard = try ref.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try std.testing.expectError(error.WorkspaceReceiptChangedBeforeExecution, Owner.prepareTake(&expected, 0x1000, &epoch));
    try std.testing.expect(expected.captured and expected.count == 1 and expected.entries[0].root == &root);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    expected.deinitOutsideExecution();
    expected_live = false;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expect(Owner.preEntryRace(error.BackingAlreadyRetired));
    try std.testing.expect(Owner.preEntryRace(error.BackingClosing));
    try std.testing.expect(Owner.preEntryRace(error.ResourceTemporarilyUnavailable));
    try std.testing.expect(!Owner.preEntryRace(error.OutOfMemory));
    try std.testing.expect(!Owner.preEntryRace(error.ForeignWorkspaceUse));
    try std.testing.expect(!Owner.preEntryRace(error.SealedWorkspaceCleanupEpoch));
}
