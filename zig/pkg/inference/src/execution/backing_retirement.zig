// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Independent quota for one actual physical backing. SDK death witnesses
//! retain Charge directly; this owner supplies its exact CURRENT cleanup gate
//! plus optional deferred conservative-padding refund. Neither epoch substitutes
//! for the other, and every external quota callback runs outside metadata locks.
const std = @import("std");
const retirement = @import("resource_retirement.zig");
const sink = @import("retirement_sink.zig");
const platform = @import("antfly_platform");

/// One physical plane owns its own quota. Shrink work pins only metadata;
/// it never substitutes its earlier epoch for final physical cleanup authority.
pub const BackingOwner = struct {
    references: std.atomic.Value(usize) = .init(1), // physical cleanup plus shrink metadata
    mutex: std.atomic.Mutex = .unlocked,
    metadata: Metadata,
    charge: *retirement.Charge,
    admitted_bytes: std.atomic.Value(usize) = .init(0),
    node: retirement.Node,
    shrink_node: retirement.Node,
    cleanup_epoch: ?sink.Epoch = null,
    shrink_epoch: ?sink.Epoch = null,
    measured_bytes: usize = 0,
    physical_released: bool = false,
    physical_finished: bool = false,
    shrink_ready: bool = false,
    measured: bool = false,
    active_uses: usize = 0,
    shrinking: bool = false,

    pub const Metadata = struct {
        context: *anyopaque,
        /// Called only after every physical/shrink metadata pin is gone. A
        /// cold owner slab may retain its own invocation pin independently.
        release: *const fn (*anyopaque) void,
    };

    /// Initialize only at its final process-stable address, before any SDK
    /// allocation or numerical gate. No caller-local Node pointer can escape.
    pub fn init(self: *BackingOwner, charge: *retirement.Charge, metadata: Metadata) void {
        self.* = .{ .charge = charge, .admitted_bytes = .init(charge.storageBytes() orelse @panic("physical owner requires storage quota")), .metadata = metadata, .node = .{ .context = self, .release_fn = finish }, .shrink_node = .{ .context = self, .release_fn = finishShrink } };
    }

    /// Cold construction/pre-admission cancellation only. The caller proves
    /// this owner has never materialized or published SDK/physical storage.
    pub fn discardUnpublished(self: *BackingOwner) void {
        std.debug.assert(self.references.load(.acquire) == 1 and !self.measured and !self.physical_released and self.shrink_epoch == null and self.cleanup_epoch == null);
        self.dropMetadata();
    }

    /// Identity/domain stay immutable. Readers never race a deferred
    /// padding refund's private mutation of underlying exactly-once leases.
    pub fn admittedBytes(self: *const BackingOwner) usize {
        return self.admitted_bytes.load(.acquire);
    }
    pub fn admittedAmounts(self: *const BackingOwner) @import("../runtime/tier/memory.zig").AdmissionAmounts {
        return self.charge.storageAmountsForBytes(self.admittedBytes());
    }

    /// A concrete SDK witness may outlive the final logical cache/view ref.
    /// Its Group retains only this pure metadata pin, and releases it from
    /// the existing CPU collector AFTER actual SDK payload death. No SDK
    /// deallocation callback destroys metadata or invokes a quota supplier.
    pub fn retainMetadataPin(self: *BackingOwner) Metadata {
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        return .{ .context = self, .release = releaseMetadataPin };
    }
    fn releaseMetadataPin(raw: *anyopaque) void {
        const self: *BackingOwner = @ptrCast(@alignCast(raw));
        self.dropMetadata();
    }

    fn lock(self: *BackingOwner) void {
        platform.sync.lockYielding(&self.mutex);
    }
    fn dropMetadata(self: *BackingOwner) void {
        const refs = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(refs != 0);
        if (refs != 1) return;
        const charge = self.charge;
        const metadata = self.metadata;
        charge.release();
        metadata.release(metadata.context);
    }
    /// Cold actual-owner/scope preparation only, BEFORE numerical ownership.
    /// A new use cannot race an outside padding callback into an owned lane.
    /// No SDK capture, supplier callback, Epoch array, or metadata allocation.
    pub fn beginUseOutsideExecution(self: *BackingOwner) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.physical_released) return error.BackingAlreadyRetired;
        if (self.shrinking) return error.ResourceTemporarilyUnavailable;
        try self.retainUseLocked();
    }
    /// A concrete cold-prepared root may establish its initial use BEFORE
    /// first SDK allocation/publication. No measurement/shrink exists yet,
    /// so this infallible-capacity path invokes no supplier under the owner.
    pub fn beginInitialUseBeforePublication(self: *BackingOwner) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.measured or self.physical_released or self.active_uses != 0 or self.shrinking)
            return error.BackingAlreadyPublished;
        try self.retainUseLocked();
    }
    fn retainUseLocked(self: *BackingOwner) !void {
        if (self.active_uses == std.math.maxInt(usize)) return error.BackingUseReferenceOverflow;
        var references = self.references.load(.monotonic);
        while (true) {
            if (references == 0 or references == std.math.maxInt(usize)) return error.BackingUseReferenceOverflow;
            references = self.references.cmpxchgWeak(references, references + 1, .monotonic, .monotonic) orelse break;
        }
        self.active_uses += 1;
    }
    /// Each use ends ONLY after its own actual current B sealed, or cold
    /// cancellation outside every lane. A use pins both final credit AND any
    /// pending padding refund; a plain Charge ref would protect only the first.
    pub fn endUseOutsideExecution(self: *BackingOwner) void {
        self.lock();
        std.debug.assert(self.active_uses != 0);
        self.active_uses -= 1;
        const shrink = self.takeShrinkLocked();
        self.mutex.unlock();
        if (shrink) self.runShrinkOutsideExecution();
        self.dropMetadata(); // this independently prepared use's pure pin
    }
    fn takeShrinkLocked(self: *BackingOwner) bool {
        if (!self.shrink_ready or self.shrinking or self.active_uses != 0 or
            (self.physical_released and !self.physical_finished)) return false;
        self.shrink_ready = false;
        self.shrinking = true;
        return true;
    }
    fn runShrinkOutsideExecution(self: *BackingOwner) void {
        // The original shrink metadata ref spans the callback and all state
        // publication. Never call supplier retain under this owner's mutex.
        self.charge.shrinkStorageBytes(self.measured_bytes);
        const remaining = self.charge.storageBytes().?;
        self.lock();
        std.debug.assert(self.shrinking and self.active_uses == 0);
        self.admitted_bytes.store(remaining, .release);
        self.shrinking = false;
        self.mutex.unlock();
        self.dropMetadata(); // one exact deferred shrink pin, consumed once
    }
    fn finishShrink(raw: *anyopaque) callconv(.c) void {
        const self: *BackingOwner = @ptrCast(@alignCast(raw));
        self.lock();
        var epoch = self.shrink_epoch.?;
        self.shrink_epoch = null;
        self.shrink_ready = true;
        const shrink = self.takeShrinkLocked();
        self.mutex.unlock();
        if (shrink) self.runShrinkOutsideExecution();
        epoch.release();
    }
    /// Actual SDK spans can be below conservative bounds. Prepare one shrink
    /// node before numerical publication and pin its own exact growth epoch.
    /// The pin is metadata only; it cannot authorize final physical retirement.
    pub fn measure(self: *BackingOwner, cleanup: *const sink.Epoch, actual: usize) void {
        const bound = self.charge.storageBytes() orelse @panic("physical owner lacks storage quota");
        if (actual > bound) @panic("physical backing exceeds admitted span");
        self.lock();
        std.debug.assert(!self.measured and !self.physical_released);
        self.measured = true;
        if (actual == bound) {
            self.mutex.unlock();
            return;
        }
        self.measured_bytes = actual;
        self.shrink_epoch = cleanup.retain() catch platform.inference_process_supervisor.restartWorker();
        _ = self.references.fetchAdd(1, .monotonic);
        const boundary = self.shrink_epoch.?.boundary() catch platform.inference_process_supervisor.restartWorker();
        self.mutex.unlock();
        boundary.register(boundary.context, boundary.nonce, &self.shrink_node) catch platform.inference_process_supervisor.restartWorker();
    }
    fn finish(raw: *anyopaque) callconv(.c) void {
        const self: *BackingOwner = @ptrCast(@alignCast(raw));
        self.lock();
        std.debug.assert(self.physical_released and !self.physical_finished);
        self.physical_finished = true;
        var epoch = self.cleanup_epoch.?;
        self.cleanup_epoch = null;
        const shrink = self.takeShrinkLocked();
        self.mutex.unlock();
        if (shrink) self.runShrinkOutsideExecution();
        self.dropMetadata(); // actual physical cleanup's pin
        epoch.release();
    }
    /// Explicit outside-execution final destruction, after SDK witness
    /// publication. This is never selected merely because boundary is null.
    /// A still-pending shrink retains its own earlier pin; physical cleanup
    /// obtains no authority from that earlier epoch.
    pub fn releaseOutsideExecution(self: *BackingOwner) void {
        self.lock();
        std.debug.assert(!self.physical_released);
        self.physical_released = true;
        self.physical_finished = true;
        const shrink = self.takeShrinkLocked();
        self.mutex.unlock();
        if (shrink) self.runShrinkOutsideExecution();
        self.dropMetadata();
    }

    /// Called only after actual CPU/CUDA release or Metal buffer-witness publication.
    /// ALWAYS record the actual CURRENT cleanup epoch, even when an earlier
    /// shrink callback still pins this metadata. No fallible retirement work.
    pub fn release(self: *BackingOwner, cleanup: *const sink.Epoch) void {
        const held = cleanup.retain() catch platform.inference_process_supervisor.restartWorker();
        const boundary = held.boundary() catch platform.inference_process_supervisor.restartWorker();
        self.lock();
        std.debug.assert(!self.physical_released);
        self.physical_released = true;
        self.cleanup_epoch = held;
        self.mutex.unlock();
        boundary.register(boundary.context, boundary.nonce, &self.node) catch platform.inference_process_supervisor.restartWorker();
        // A sealed epoch can execute finish synchronously; do not read self.
    }
};

// Exercise the same cold cells/owner barrier through the existing lifecycle root.
test {
    _ = @import("../backends/metal_workspace_fence.zig");
}
