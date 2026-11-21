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
const gate_mod = @import("operation_gate.zig");
const operation = @import("backend_operation.zig");
const kernel = @import("kernel_route.zig");
const retirement_sink = @import("retirement_sink.zig");

/// Embedded provider metadata. Initialize only once the provider has its final
/// address. No field borrows a caller stack/allocator; checked provider teardown
/// retires every real command-buffer handler before destroying these records.
pub const Owner = struct {
    /// Exact inline scope field in the canonical managed Metal Compute. The
    /// shared provider metadata endpoint charges this singleton once and lives
    /// no shorter than that Compute. This is only same-thread metadata/index
    /// exclusion evidence, never numerical or cleanup-boundary authority.
    pub const MetadataIndexScope = struct {
        token: ?operation.Borrowed = null,
        previous: ?*MetadataIndexScope = null,
    };

    // Null for the unchanged embedded Native/CUDA owner. Metal's independently
    // allocated metadata has a provider root and explicit B-use storage pins.
    storage_allocator: ?std.mem.Allocator = null,
    storage_references: std.atomic.Value(usize) = .init(1),
    storage_closed: std.atomic.Value(bool) = .init(false),
    storage_mutex: std.atomic.Mutex = .unlocked,
    storage_backing: ?@import("../runtime/tier/cache.zig").PhysicalBacking = null,
    close_epoch: ?retirement_sink.Epoch = null,
    close_node: @import("resource_retirement.zig").Node = undefined,
    initialized: std.atomic.Value(u8) = .init(0),
    gate: gate_mod.Gate = .{},
    records: [gate_mod.Gate.capacity]Record = @splat(.{}),
    pub const Record = struct {
        // These are initialized once, before any capability is published.
        mutex: std.atomic.Mutex = .unlocked,
        owner: *Owner = undefined,
        host_released: bool = true,
        gate: *gate_mod.Gate = undefined,
        index: usize = 0,
        ticket: ?gate_mod.Gate.Ticket = null,
        provider: *anyopaque = undefined,
        source: kernel.Implementation = undefined,
        mode: operation.Mode = .legacy_unknown_frame,
        entered: bool = false,
        // Intrusive stable Frame nodes, registered only under an entered
        // managed owner. Generic metadata finish never publishes completion.
        terminal_observers: ?*operation.TerminalEvidence = null,
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
            if (self.owner.storage_closed.load(.acquire)) return error.BackendOperationConsumed;
            try self.gate.validateNonce(self.index, nonce);
            const ticket = self.ticket orelse return error.BackendOperationConsumed;
            if (ticket.nonce != nonce) return error.BackendOperationConsumed;
            return .{ .ticket = ticket, .provider = self.provider, .source = self.source, .mode = self.mode, .entered = self.entered };
        }
        /// Pins the actual record array independently of the provider body.
        /// Call while this operation is still entered, before taking its Ticket.
        /// Only providers with actual completed SDK work call this. Publication
        /// occurs after finishMetadata has ended the exact operation gate.
        pub fn publishTerminal(self: *Record, nonce: u64) !void {
            while (true) {
                self.lock();
                self.gate.validateNonce(self.index, nonce) catch |err| {
                    self.mutex.unlock();
                    return err;
                };
                const ticket = self.ticket orelse {
                    self.mutex.unlock();
                    return error.BackendOperationConsumed;
                };
                if (ticket.nonce != nonce or self.mode != .managed or self.entered or (ticket.ownsGate() catch true)) {
                    self.mutex.unlock();
                    return error.BackendOperationInactive;
                }
                const node = self.terminal_observers orelse {
                    self.mutex.unlock();
                    return;
                };
                // Destruction owns node then record. Never block in the
                // reverse order: release record so unlink can make progress.
                if (!node.mutex.tryLock()) {
                    self.mutex.unlock();
                    std.atomic.spinLoopHint();
                    continue;
                }
                const source = node.implementation orelse unreachable;
                std.debug.assert(node.context == @as(*anyopaque, @ptrCast(self)) and node.nonce == nonce and source.context == self.source.context and source.table == self.source.table);
                self.terminal_observers = node.next;
                node.next = null;
                node.context = null;
                node.detach = null;
                node.terminal = true;
                var retained = ticket;
                const owner = self.owner;
                self.mutex.unlock();
                node.mutex.unlock();
                // Host lease still keeps the record/provider root alive. Only
                // metadata references leave here, outside both metadata locks.
                try retained.release();
                owner.releaseStorage();
            }
        }
        pub fn bindTerminalEvidence(self: *Record, nonce: u64, implementation: kernel.Implementation, node: *operation.TerminalEvidence) !void {
            node.lock();
            defer node.mutex.unlock();
            if (node.context != null or node.detach != null or node.implementation != null) return error.InvalidOperationTerminalEvidence;
            self.lock();
            defer self.mutex.unlock();
            const value = try self.snapshotLocked(nonce);
            if (value.source.context != implementation.context or value.source.table != implementation.table or value.mode != .managed) return error.BackendOperationChanged;
            if (!value.entered or !try value.ticket.ownsGate()) return error.BackendOperationInactive;
            if (self.owner.storage_allocator == null) return error.BackendOperationStorageUnavailable;
            _ = try value.ticket.retain();
            const old = self.owner.storage_references.fetchAdd(1, .monotonic);
            std.debug.assert(old != 0 and old != std.math.maxInt(usize));
            node.context = self;
            node.nonce = nonce;
            node.implementation = implementation;
            node.terminal = false;
            node.detach = detachTerminal;
            node.next = self.terminal_observers;
            self.terminal_observers = node;
        }
        fn detachTerminal(node: *operation.TerminalEvidence) void {
            // Node mutex is held by deinit, and its own Ticket+StoragePin
            // keep this exact record/nonce alive until unlink completes.
            const self: *Record = @ptrCast(@alignCast(node.context.?));
            self.lock();
            self.gate.validateNonce(self.index, node.nonce) catch @panic("stale terminal observer");
            var link = &self.terminal_observers;
            while (link.* != node) {
                const current = link.* orelse @panic("missing terminal observer");
                link = &current.next;
            }
            link.* = node.next;
            var ticket = self.ticket orelse @panic("missing terminal ticket");
            std.debug.assert(ticket.nonce == node.nonce);
            const owner = self.owner;
            node.context = null;
            node.next = null;
            node.detach = null;
            node.implementation = null;
            node.nonce = 0;
            node.terminal = false;
            self.mutex.unlock();
            node.mutex.unlock();
            ticket.release() catch @panic("terminal observer ticket release");
            owner.releaseStorage();
        }

        pub fn retainStorage(self: *Record, nonce: u64) !StoragePin {
            self.lock();
            defer self.mutex.unlock();
            const value = try self.snapshotLocked(nonce);
            if (!value.entered or !try value.ticket.ownsGate()) return error.BackendOperationInactive;
            if (self.owner.storage_allocator == null) return error.BackendOperationStorageUnavailable;
            const old = self.owner.storage_references.fetchAdd(1, .monotonic);
            std.debug.assert(old != 0 and old != std.math.maxInt(usize));
            return .{ .owner = self.owner };
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
            self.host_released = true;
            ticket.release() catch |err| {
                self.host_released = false;
                return err;
            };
        }
        noinline fn finishAndReleaseWithCleanupLocked(self: *Record, nonce: u64) !void {
            var detached: ?retirement_sink.Epoch = null;
            defer if (detached) |*epoch| epoch.release();
            defer self.mutex.unlock();
            try self.finishMetadataLocked(nonce);
            var ticket = self.ticket.?;
            const epoch = self.cleanup_epoch.?;
            self.cleanup_epoch = null;
            self.host_released = true;
            ticket.release() catch |err| {
                self.host_released = false;
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
            self.host_released = true;
            ticket.release() catch |err| {
                self.host_released = false;
                return err;
            };
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
            self.host_released = true;
            ticket.release() catch |err| {
                self.host_released = false;
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
                value.owner = self;
                value.gate = &self.gate;
                value.index = index;
            }
            self.initialized.store(2, .release);
        } else while (self.initialized.load(.acquire) != 2) std.atomic.spinLoopHint();
    }
    pub fn prepare(self: *Owner, provider: *anyopaque, request: operation.Request) !Prepared {
        self.ensureInitialized();
        if (self.storage_closed.load(.acquire)) return error.BackendOperationConsumed;
        var ticket = try self.gate.prepare();
        errdefer ticket.release() catch unreachable;
        const value = &self.records[ticket.index];
        value.lock();
        defer value.mutex.unlock();
        // The Gate reservation excludes another publisher. A stale observer
        // checks the NEW nonce under this mutex before touching these fields.
        std.debug.assert(value.cleanup_epoch == null);
        value.host_released = false;
        value.ticket = ticket;
        value.provider = provider;
        value.source = request.implementation;
        value.mode = request.mode;
        value.entered = false;
        std.debug.assert(value.terminal_observers == null);
        return .{ .record = value, .nonce = ticket.nonce };
    }
    pub const StoragePin = struct {
        owner: ?*Owner,
        pub fn release(self: *StoragePin) void {
            const owner = self.owner orelse return;
            self.owner = null;
            owner.releaseStorage();
        }
    };
    pub fn retainedStorageBytes() usize {
        // This new native service lives in Runtime, which dies no later than
        // this independently retained metadata endpoint. Charge it ONCE with
        // this singleton rather than once per immutable packed entry.
        const Native = struct {
            extern fn antfly_metal_packed_use_service_metadata_bytes() usize;
        };
        const service = if (comptime @import("build_options").enable_metal) Native.antfly_metal_packed_use_service_metadata_bytes() else 0;
        const project = std.math.add(usize, @sizeOf(Owner), @sizeOf(MetadataIndexScope)) catch @panic("operation metadata size overflow");
        return std.math.add(usize, project, service) catch @panic("operation metadata size overflow");
    }
    pub fn createRetained(allocator: std.mem.Allocator) !*Owner {
        const result = try allocator.create(Owner);
        result.* = .{ .storage_allocator = allocator };
        return result;
    }
    pub fn hasStorageBacking(self: *Owner) bool {
        while (!self.storage_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.storage_mutex.unlock();
        return self.storage_backing != null;
    }
    /// Transfer exactly one shared HOST component before the first packed use.
    /// Caller has already funded it outside all Model/provider gates.
    pub fn attachStorageBacking(self: *Owner, backing: *?@import("../runtime/tier/cache.zig").PhysicalBacking) !void {
        while (!self.storage_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.storage_mutex.unlock();
        if (self.storage_allocator == null or self.storage_closed.load(.acquire)) return error.BackendOperationConsumed;
        if (self.storage_backing != null) return error.BackendOperationStorageAlreadyFunded;
        const value = backing.* orelse return error.MissingCacheRetirementDomain;
        if (value.tier != .host or value.bytes != retainedStorageBytes()) return error.InvalidOperationStorageBacking;
        self.storage_backing = value;
        backing.* = null;
    }
    /// Provider close may leave only independent completed-host metadata uses.
    /// This never consumes the provider root; releaseClosedStorage runs after
    /// every provider gate/lock has ended and the provider body is retired.
    /// As with provider teardown, the caller holds the actual last-provider
    /// lifetime exclusion: no new host prepare can race this close scan.
    pub fn closeRetained(self: *Owner) !void {
        if (self.storage_allocator == null) return error.BackendOperationStorageUnavailable;
        self.ensureInitialized();
        for (&self.records) |*record| {
            record.lock();
            const released = record.host_released;
            const entered = record.entered;
            record.mutex.unlock();
            if (!released or entered) return error.BackendOperationStillPrepared;
        }
        self.storage_closed.store(true, .release);
    }
    /// Retain the singleton metadata allocation through the actual provider
    /// close B even if no packed entry ever reached successful publication.
    pub fn holdStorageUntilCleanup(self: *Owner, current: @import("cleanup_context.zig").Context) !void {
        const epoch = current.boundary orelse {
            if (!current.outside_execution) return error.BackendOperationCleanupUnavailable;
            return;
        };
        var retained = try epoch.retain();
        var moved = false;
        defer if (!moved) retained.release();
        while (!self.storage_mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.storage_mutex.unlock();
        if (self.close_epoch) |existing| {
            if (existing.record != retained.record or existing.nonce != retained.nonce) return error.BackendOperationCleanupChanged;
            return;
        }
        const boundary = try retained.boundaryBeforeSeal();
        self.close_node = .{ .context = self, .release_fn = closeBoundaryEnded };
        const old = self.storage_references.fetchAdd(1, .monotonic);
        std.debug.assert(old != 0 and old != std.math.maxInt(usize));
        self.close_epoch = retained;
        boundary.register(boundary.context, boundary.nonce, &self.close_node) catch |err| {
            self.close_epoch = null;
            _ = self.storage_references.fetchSub(1, .monotonic);
            return err;
        };
        moved = true;
    }
    fn closeBoundaryEnded(raw: *anyopaque) callconv(.c) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        while (!self.storage_mutex.tryLock()) std.atomic.spinLoopHint();
        var epoch = self.close_epoch.?;
        self.close_epoch = null;
        self.storage_mutex.unlock();
        // No destroy/refund callback while holding the metadata mutex.
        epoch.release();
        self.releaseStorage();
    }
    pub fn releaseClosedStorage(self: *Owner) void {
        std.debug.assert(self.storage_closed.load(.acquire));
        self.releaseStorage();
    }
    fn releaseStorage(self: *Owner) void {
        if (self.storage_references.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.storage_closed.load(.acquire));
        self.assertIdle();
        const allocator = self.storage_allocator.?;
        const backing = self.storage_backing;
        allocator.destroy(self);
        // Payload and the actual record/array allocation are gone BEFORE the
        // shared singleton HOST component can refund; never a Model/provider pin.
        if (backing) |value| value.releaseAfterMetadataDeathOutsideExecution();
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

test "retained Metal operation storage survives host close until independent ticket pin drains" {
    const a = std.testing.allocator;
    const owner = try Owner.createRetained(a);
    var root_live = true;
    defer if (root_live) {
        owner.closeRetained() catch unreachable;
        owner.releaseClosedStorage();
    };
    var marker: u8 = 0;
    const request = operation.Request{ .implementation = .{ .context = &marker, .table = &marker }, .program = null, .mode = .managed };
    const prepared = try owner.prepare(&marker, request);
    var host_live = true;
    defer if (host_live) prepared.record.finishAndRelease(prepared.nonce) catch unreachable;
    try std.testing.expectError(error.BackendOperationStillPrepared, owner.closeRetained());
    const snapshot_value = try prepared.record.snapshot(prepared.nonce);
    try snapshot_value.ticket.begin();
    try prepared.record.setEntered(prepared.nonce, true);
    var storage = try prepared.record.retainStorage(prepared.nonce);
    defer storage.release();
    var completion = try snapshot_value.ticket.retain();
    defer if (completion.held) completion.release() catch unreachable;
    try std.testing.expectError(error.BackendOperationConsumed, prepared.record.retainStorage(prepared.nonce + 1));
    try prepared.record.finishAndRelease(prepared.nonce);
    host_live = false;
    try owner.closeRetained();
    owner.releaseClosedStorage();
    root_live = false;
    // No provider/Model remains. Ticket points into the independently pinned
    // storage, and closed metadata rejects every provider-facing capability.
    try completion.validate();
    try std.testing.expectError(error.BackendOperationConsumed, prepared.record.snapshot(prepared.nonce));
    try completion.release();
    storage.release();
}

test "embedded operation metadata rejects retained storage without changing raw host lifecycle" {
    var owner: Owner = .{};
    defer owner.assertIdle();
    var marker: u8 = 0;
    const prepared = try owner.prepare(&marker, .{ .implementation = .{ .context = &marker, .table = &marker }, .program = null, .mode = .managed });
    defer prepared.record.finishAndRelease(prepared.nonce) catch unreachable;
    const snapshot_value = try prepared.record.snapshot(prepared.nonce);
    try snapshot_value.ticket.begin();
    try prepared.record.setEntered(prepared.nonce, true);
    try std.testing.expectError(error.BackendOperationStorageUnavailable, prepared.record.retainStorage(prepared.nonce));
}

test "retained raw operation storage close waits actual B even without a published packed entry" {
    var sink = try retirement_sink.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    const owner = try Owner.createRetained(std.testing.allocator);
    try owner.holdStorageUntilCleanup(.{ .boundary = &epoch });
    try owner.holdStorageUntilCleanup(.{ .boundary = &epoch });
    try std.testing.expectEqual(@as(usize, 2), owner.storage_references.load(.acquire));
    try owner.closeRetained();
    owner.releaseClosedStorage();
    try std.testing.expectEqual(@as(usize, 1), owner.storage_references.load(.acquire));
    try std.testing.expect(owner.storage_closed.load(.acquire));
    guard.finish(); // Last pure metadata allocation dies here, outside B.
}

test "retained operation storage rejects sealed close boundary without consuming metadata root" {
    var sink = try retirement_sink.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    guard.finish();
    const owner = try Owner.createRetained(std.testing.allocator);
    defer {
        owner.closeRetained() catch unreachable;
        owner.releaseClosedStorage();
    }
    try std.testing.expectError(error.RetirementEpochSealed, owner.holdStorageUntilCleanup(.{ .boundary = &epoch }));
    try std.testing.expect(owner.close_epoch == null);
    try std.testing.expectEqual(@as(usize, 1), owner.storage_references.load(.acquire));
}

const TerminalEvidenceFixture = struct {
    fn run(a: std.mem.Allocator) !void {
        // Only the cold Owner header may allocate; all receiving/terminal
        // transitions run with the next actual allocation forced to fail.
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 1 });
        const owner = try Owner.createRetained(failing.allocator());
        var root_live = true;
        defer if (root_live) {
            owner.closeRetained() catch unreachable;
            owner.releaseClosedStorage();
        };
        var marker: u8 = 0;
        var foreign: u8 = 0;
        const implementation = kernel.Implementation{ .context = &marker, .table = &marker };
        const request = operation.Request{ .implementation = implementation, .program = null, .mode = .managed };
        // Distinct stable Frame-sized nodes retain terminal facts, not the
        // bounded 64 operation records. Exercise more than that actual limit.
        var nodes: [gate_mod.Gate.capacity + 17]operation.TerminalEvidence = @splat(.{});
        defer for (&nodes) |*node| node.deinit();
        for (&nodes) |*node| {
            const prepared = try owner.prepare(&marker, request);
            var host_live = true;
            defer if (host_live) {
                prepared.record.finishMetadata(prepared.nonce) catch unreachable;
                prepared.record.release(prepared.nonce) catch unreachable;
            };
            try std.testing.expectError(error.BackendOperationInactive, prepared.record.bindTerminalEvidence(prepared.nonce, implementation, node));
            const snapshot = try prepared.record.snapshot(prepared.nonce);
            try snapshot.ticket.begin();
            try prepared.record.setEntered(prepared.nonce, true);
            try std.testing.expectError(error.BackendOperationChanged, prepared.record.bindTerminalEvidence(prepared.nonce, .{ .context = &foreign, .table = &marker }, node));
            try std.testing.expectError(error.BackendOperationConsumed, prepared.record.bindTerminalEvidence(prepared.nonce + 1, implementation, node));
            try prepared.record.bindTerminalEvidence(prepared.nonce, implementation, node);
            try std.testing.expectError(error.PhysicalCompletionUnavailable, node.validateTerminal(implementation));
            try std.testing.expectError(error.BackendOperationInactive, prepared.record.publishTerminal(prepared.nonce));
            // Represents the provider's actual successful wait/cancel proof:
            // finish metadata, publish to stable receivers, then release host.
            try prepared.record.finishMetadata(prepared.nonce);
            try prepared.record.publishTerminal(prepared.nonce);
            try prepared.record.release(prepared.nonce);
            host_live = false;
            try std.testing.expectEqual(@as(usize, 1), owner.storage_references.load(.acquire));
            try node.validateTerminal(implementation);
            try std.testing.expect(node.context == null and node.detach == null);
        }
        try owner.closeRetained();
        owner.releaseClosedStorage();
        root_live = false;
        for (&nodes) |*node| {
            try node.validateTerminal(implementation);
            try std.testing.expectError(error.BackendOperationChanged, node.validateTerminal(.{ .context = &marker, .table = &foreign }));
        }
    }
};
test "managed operation terminal latches release bounded records across more than 64 stable frames and provider close" {
    try TerminalEvidenceFixture.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, TerminalEvidenceFixture.run, .{});
}

test "unlinked mixed work and generic metadata finish never receive terminal proof" {
    const owner = try Owner.createRetained(std.testing.allocator);
    defer {
        owner.closeRetained() catch unreachable;
        owner.releaseClosedStorage();
    }
    var marker: u8 = 0;
    const implementation = kernel.Implementation{ .context = &marker, .table = &marker };
    const prepared = try owner.prepare(&marker, .{ .implementation = implementation, .program = null, .mode = .managed });
    const snapshot = try prepared.record.snapshot(prepared.nonce);
    try snapshot.ticket.begin();
    try prepared.record.setEntered(prepared.nonce, true);
    var pending = operation.TerminalEvidence{};
    defer pending.deinit();
    var detached = operation.TerminalEvidence{};
    defer detached.deinit();
    try prepared.record.bindTerminalEvidence(prepared.nonce, implementation, &pending);
    try prepared.record.bindTerminalEvidence(prepared.nonce, implementation, &detached);
    // A real primitive/transfer invalidates this Frame receiver before writes;
    // later terminal publication cannot touch the unlinked node's address.
    detached.deinit();
    try prepared.record.finishMetadata(prepared.nonce);
    try std.testing.expectError(error.PhysicalCompletionUnavailable, pending.validateTerminal(implementation));
    try std.testing.expectError(error.PhysicalCompletionUnavailable, detached.validateTerminal(implementation));
    try prepared.record.publishTerminal(prepared.nonce);
    try prepared.record.release(prepared.nonce);
    try pending.validateTerminal(implementation);
    try std.testing.expectError(error.PhysicalCompletionUnavailable, detached.validateTerminal(implementation));
}

test "failed unproven host finish stays unknown after provider close and receiver detach is metadata only" {
    const owner = try Owner.createRetained(std.testing.allocator);
    var marker: u8 = 0;
    const implementation = kernel.Implementation{ .context = &marker, .table = &marker };
    const prepared = try owner.prepare(&marker, .{ .implementation = implementation, .program = null, .mode = .managed });
    const snapshot = try prepared.record.snapshot(prepared.nonce);
    try snapshot.ticket.begin();
    try prepared.record.setEntered(prepared.nonce, true);
    var node = operation.TerminalEvidence{};
    // Generic cleanup has no provider terminal proof. The live receiver pins
    // only metadata through close and must remain unknown, not guess success.
    try prepared.record.bindTerminalEvidence(prepared.nonce, implementation, &node);
    try prepared.record.finishAndRelease(prepared.nonce);
    try owner.closeRetained();
    owner.releaseClosedStorage();
    defer node.deinit();
    try std.testing.expectError(error.PhysicalCompletionUnavailable, node.validateTerminal(implementation));
}
