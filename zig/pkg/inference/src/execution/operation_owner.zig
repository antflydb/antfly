// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const gate_mod = @import("operation_gate.zig");
const operation = @import("backend_operation.zig");
const kernel = @import("kernel_route.zig");
const retirement_sink = @import("retirement_sink.zig");

/// Embedded provider metadata. Initialize only once the provider has its final
/// address. No field borrows a caller stack/allocator; checked provider teardown
/// retires every real command-buffer handler before destroying these records.
pub const Owner = struct {
    initialized: std.atomic.Value(u8) = .init(0),
    gate: gate_mod.Gate = .{},
    records: [gate_mod.Gate.capacity]Record = @splat(.{}),
    pub const Record = struct {
        // These are initialized once, before any capability is published.
        mutex: std.atomic.Mutex = .unlocked,
        gate: *gate_mod.Gate = undefined,
        index: usize = 0,
        ticket: ?gate_mod.Gate.Ticket = null,
        provider: *anyopaque = undefined,
        source: kernel.Implementation = undefined,
        mode: operation.Mode = .legacy_unknown_frame,
        entered: bool = false,
        cleanup_epoch: ?retirement_sink.Epoch = null,
        pub const Snapshot = struct {
            ticket: gate_mod.Gate.Ticket,
            provider: *anyopaque,
            source: kernel.Implementation,
            mode: operation.Mode,
            entered: bool,
        };
        fn lock(self: *Record) void {
            while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        }
        /// Stale Borrowed capabilities never read rewritten plain metadata.
        /// The version/ref proof is inspected while publication/retirement of
        /// this exact record is excluded. No driver call runs under this lock.
        pub fn snapshot(self: *Record, nonce: u64) !Snapshot {
            self.lock();
            defer self.mutex.unlock();
            return self.snapshotLocked(nonce);
        }
        fn snapshotLocked(self: *Record, nonce: u64) !Snapshot {
            try self.gate.validateNonce(self.index, nonce);
            const ticket = self.ticket orelse return error.BackendOperationConsumed;
            if (ticket.nonce != nonce) return error.BackendOperationConsumed;
            return .{ .ticket = ticket, .provider = self.provider, .source = self.source, .mode = self.mode, .entered = self.entered };
        }
        /// Only pure metadata validators may use this transition. Providers
        /// requiring driver or external-frame checks keep those outside this
        /// lock and publish entered only after those checks succeed.
        pub fn beginMetadata(self: *Record, nonce: u64, request: operation.Request, comptime validate: anytype) !void {
            self.lock();
            defer self.mutex.unlock();
            const snapshot_value = try self.snapshotLocked(nonce);
            try validate(&snapshot_value, request);
            try snapshot_value.ticket.begin();
            self.entered = true;
        }
        /// Bind only an explicitly supplied current outer owner. The Guard was
        /// entered before the relevant model/provider gates and is sealed by
        /// that owner after unlocking them; the Record never seals it itself.
        pub fn bindCleanupEpoch(self: *Record, nonce: u64, epoch: *const retirement_sink.Epoch) !void {
            var retained = try epoch.retain();
            var moved = false;
            defer if (!moved) retained.release();
            self.lock();
            defer self.mutex.unlock();
            const current = try self.snapshotLocked(nonce);
            if (!current.entered or !try current.ticket.ownsGate()) return error.BackendOperationInactive;
            if (current.mode == .metadata) return error.BackendOperationCleanupUnavailable;
            if (try retained.isSealed()) return error.RetirementEpochSealed;
            if (self.cleanup_epoch) |existing| {
                if (existing.record != retained.record or existing.nonce != retained.nonce) return error.BackendOperationCleanupChanged;
                return;
            }
            self.cleanup_epoch = retained;
            moved = true;
        }
        /// The independent returned pin names exactly this live operation's
        /// boundary. Raw/unqualified operations return null, never a task's or
        /// provider's unrelated currently active epoch.
        pub fn retainCleanupEpoch(self: *Record, nonce: u64) !?retirement_sink.Epoch {
            self.lock();
            defer self.mutex.unlock();
            const current = try self.snapshotLocked(nonce);
            if (!current.entered or !try current.ticket.ownsGate()) return error.BackendOperationInactive;
            if (current.mode == .metadata) return null;
            const epoch = self.cleanup_epoch orelse return null;
            if (try epoch.isSealed()) return error.RetirementEpochSealed;
            return try epoch.retain();
        }
        pub fn setEntered(self: *Record, nonce: u64, entered: bool) !void {
            self.lock();
            defer self.mutex.unlock();
            try self.gate.validateNonce(self.index, nonce);
            self.entered = entered;
        }
        /// Commit terminal host metadata in one critical section. The owner
        /// has already supplied any required SDK completion proof; releasing
        /// this atomic gate invokes no driver or user callback.
        pub fn finishMetadata(self: *Record, nonce: u64) !void {
            self.lock();
            defer self.mutex.unlock();
            try self.finishMetadataLocked(nonce);
        }
        /// Successful finish also consumes the host reference. Independent
        /// command-buffer tickets remain live until their own completion.
        pub fn finishAndRelease(self: *Record, nonce: u64) !void {
            self.lock();
            if (self.cleanup_epoch != null) {
                @branchHint(.unlikely);
                return self.finishAndReleaseWithCleanupLocked(nonce);
            }
            defer self.mutex.unlock();
            try self.finishMetadataLocked(nonce);
            var ticket = self.ticket.?;
            try ticket.release();
        }
        noinline fn finishAndReleaseWithCleanupLocked(self: *Record, nonce: u64) !void {
            var detached: ?retirement_sink.Epoch = null;
            defer if (detached) |*epoch| epoch.release();
            defer self.mutex.unlock();
            try self.finishMetadataLocked(nonce);
            var ticket = self.ticket.?;
            const epoch = self.cleanup_epoch.?;
            self.cleanup_epoch = null;
            ticket.release() catch |err| {
                self.cleanup_epoch = epoch;
                return err;
            };
            detached = epoch;
        }
        fn finishMetadataLocked(self: *Record, nonce: u64) !void {
            try self.gate.validateNonce(self.index, nonce);
            const ticket = self.ticket orelse return error.BackendOperationConsumed;
            if (ticket.nonce != nonce) return error.BackendOperationConsumed;
            if (try ticket.ownsGate()) try ticket.end();
            self.entered = false;
        }
        pub fn release(self: *Record, nonce: u64) !void {
            self.lock();
            if (self.cleanup_epoch != null) {
                @branchHint(.unlikely);
                return self.releaseWithCleanupLocked(nonce);
            }
            defer self.mutex.unlock();
            try self.gate.validateNonce(self.index, nonce);
            var ticket = self.ticket orelse return error.BackendOperationConsumed;
            if (ticket.nonce != nonce) return error.BackendOperationConsumed;
            // Leave the non-owning descriptor until its next publication.
            // In particular, don't overwrite fields AFTER nonce becomes free.
            // Independent real-CB refs can continue to validate the same nonce.
            try ticket.release();
        }
        noinline fn releaseWithCleanupLocked(self: *Record, nonce: u64) !void {
            var detached: ?retirement_sink.Epoch = null;
            defer if (detached) |*epoch| epoch.release();
            defer self.mutex.unlock();
            try self.gate.validateNonce(self.index, nonce);
            var ticket = self.ticket orelse return error.BackendOperationConsumed;
            if (ticket.nonce != nonce) return error.BackendOperationConsumed;
            // Detach before Ticket.release can publish a reusable nonce.
            const epoch = self.cleanup_epoch.?;
            self.cleanup_epoch = null;
            ticket.release() catch |err| {
                self.cleanup_epoch = epoch;
                return err;
            };
            detached = epoch;
        }
    };
    pub const Prepared = struct { record: *Record, nonce: u64 };
    fn ensureInitialized(self: *Owner) void {
        if (self.initialized.load(.acquire) == 2) return;
        if (self.initialized.cmpxchgStrong(0, 1, .acq_rel, .acquire) == null) {
            for (&self.records, 0..) |*value, index| {
                value.gate = &self.gate;
                value.index = index;
            }
            self.initialized.store(2, .release);
        } else while (self.initialized.load(.acquire) != 2) std.atomic.spinLoopHint();
    }
    pub fn prepare(self: *Owner, provider: *anyopaque, request: operation.Request) !Prepared {
        self.ensureInitialized();
        var ticket = try self.gate.prepare();
        errdefer ticket.release() catch unreachable;
        const value = &self.records[ticket.index];
        value.lock();
        defer value.mutex.unlock();
        // The Gate reservation excludes another publisher. A stale observer
        // checks the NEW nonce under this mutex before touching these fields.
        std.debug.assert(value.cleanup_epoch == null);
        value.ticket = ticket;
        value.provider = provider;
        value.source = request.implementation;
        value.mode = request.mode;
        value.entered = false;
        return .{ .record = value, .nonce = ticket.nonce };
    }
    pub fn assertIdle(self: *const Owner) void {
        self.gate.assertIdle();
    }
};

test "provider operation terminal metadata preserves retained tickets and rejects stale transitions" {
    var owner = Owner{};
    defer owner.assertIdle();
    var marker: u8 = 0;
    const request = operation.Request{ .implementation = .{ .context = &marker, .table = &marker }, .program = null, .mode = .managed };
    const prepared = try owner.prepare(&marker, request);
    const snapshot = try prepared.record.snapshot(prepared.nonce);
    var completion = try snapshot.ticket.retain();
    defer if (completion.held) completion.release() catch @panic("completion ticket cleanup failed");
    var host_live = true;
    defer if (host_live) prepared.record.release(prepared.nonce) catch @panic("host ticket cleanup failed");
    defer prepared.record.finishMetadata(prepared.nonce) catch {};
    try snapshot.ticket.begin();
    try prepared.record.setEntered(prepared.nonce, true);
    try std.testing.expectError(error.BackendOperationConsumed, prepared.record.finishMetadata(prepared.nonce + 1));
    try std.testing.expect((try prepared.record.snapshot(prepared.nonce)).entered);
    try std.testing.expect(try snapshot.ticket.ownsGate());
    try prepared.record.finishMetadata(prepared.nonce);
    try std.testing.expect(!(try prepared.record.snapshot(prepared.nonce)).entered);
    try std.testing.expect(!try completion.ownsGate());

    const next = try owner.prepare(&marker, request);
    defer next.record.release(next.nonce) catch @panic("next ticket cleanup failed");
    defer next.record.finishMetadata(next.nonce) catch {};
    try std.testing.expect(next.record != prepared.record);
    const next_snapshot = try next.record.snapshot(next.nonce);
    try next_snapshot.ticket.begin();
    try next.record.setEntered(next.nonce, true);
    try next.record.finishMetadata(next.nonce);

    try prepared.record.release(prepared.nonce);
    host_live = false;
    try completion.validate();
    try completion.release();
    try std.testing.expectError(error.BackendOperationConsumed, prepared.record.snapshot(prepared.nonce));
}

test "provider operation record publication rejects concurrent stale capabilities without plain metadata races" {
    var owner = Owner{};
    defer owner.assertIdle();
    var first_marker: u8 = 0;
    var next_marker: u8 = 0;
    const first = try owner.prepare(&first_marker, .{ .implementation = .{ .context = &first_marker, .table = &first_marker }, .program = null, .mode = .metadata });
    try first.record.release(first.nonce);
    const Work = struct {
        owner: *Owner,
        stale: *Owner.Record,
        nonce: u64,
        marker: *u8,
        failed: std.atomic.Value(bool) = .init(false),
        fn read(self: *@This()) void {
            for (0..4096) |_| {
                if (self.stale.snapshot(self.nonce)) |_| {
                    self.failed.store(true, .release);
                } else |err| {
                    if (err != error.BackendOperationConsumed) self.failed.store(true, .release);
                }
            }
        }
        fn publish(self: *@This()) void {
            for (0..2048) |_| {
                const value = self.owner.prepare(self.marker, .{ .implementation = .{ .context = self.marker, .table = self.marker }, .program = null, .mode = .managed }) catch {
                    self.failed.store(true, .release);
                    return;
                };
                value.record.release(value.nonce) catch {
                    self.failed.store(true, .release);
                    return;
                };
            }
        }
    };
    var work = Work{ .owner = &owner, .stale = first.record, .nonce = first.nonce, .marker = &next_marker };
    const reader1 = try std.Thread.spawn(.{}, Work.read, .{&work});
    var joined1 = false;
    defer if (!joined1) reader1.join();
    const reader2 = try std.Thread.spawn(.{}, Work.read, .{&work});
    var joined2 = false;
    defer if (!joined2) reader2.join();
    const writer = try std.Thread.spawn(.{}, Work.publish, .{&work});
    writer.join();
    reader1.join();
    joined1 = true;
    reader2.join();
    joined2 = true;
    // Reader loops are bounded, so failure paths cannot leave parked threads.
    try std.testing.expect(!work.failed.load(.acquire));
}

test "provider metadata transitions reject changed requests and consume only the host ticket" {
    var owner = Owner{};
    defer owner.assertIdle();
    var marker: u8 = 0;
    var other: u8 = 0;
    const request = operation.Request{ .implementation = .{ .context = &marker, .table = &marker }, .program = null, .mode = .managed };
    const prepared = try owner.prepare(&marker, request);
    var host_live = true;
    defer if (host_live) prepared.record.release(prepared.nonce) catch @panic("host cleanup failed");
    defer if (host_live) prepared.record.finishMetadata(prepared.nonce) catch {};
    const Validator = struct {
        fn check(value: *const Owner.Record.Snapshot, requested: operation.Request) !void {
            if (value.source.context != requested.implementation.context or value.source.table != requested.implementation.table or !operation.allowsMode(value.mode, requested.mode)) return error.BackendOperationChanged;
        }
    };
    var changed = request;
    changed.implementation.context = &other;
    try std.testing.expectError(error.BackendOperationChanged, prepared.record.beginMetadata(prepared.nonce, changed, Validator.check));
    const before = try prepared.record.snapshot(prepared.nonce);
    try std.testing.expect(!before.entered and !try before.ticket.ownsGate());
    var independent = try before.ticket.retain();
    defer if (independent.held) independent.release() catch @panic("completion cleanup failed");
    try prepared.record.beginMetadata(prepared.nonce, request, Validator.check);
    try std.testing.expect((try prepared.record.snapshot(prepared.nonce)).entered);
    try prepared.record.finishAndRelease(prepared.nonce);
    host_live = false;
    try independent.validate();
    try std.testing.expect(!try independent.ownsGate());
    try std.testing.expect(!(try prepared.record.snapshot(prepared.nonce)).entered);
    try independent.release();
    try std.testing.expectError(error.BackendOperationConsumed, prepared.record.snapshot(prepared.nonce));
}

/// Static cold bridge shared by canonical provider adapters.
pub const cleanup_epoch_services = operation.CleanupEpochServices{
    .bind = bindCleanupEpoch,
    .retain = retainCleanupEpoch,
};
fn bindCleanupEpoch(raw: *anyopaque, nonce: u64, epoch: *const retirement_sink.Epoch) !void {
    const record: *Owner.Record = @ptrCast(@alignCast(raw));
    try record.bindCleanupEpoch(nonce, epoch);
}
fn retainCleanupEpoch(raw: *anyopaque, nonce: u64) !?retirement_sink.Epoch {
    const record: *Owner.Record = @ptrCast(@alignCast(raw));
    return record.retainCleanupEpoch(nonce);
}
