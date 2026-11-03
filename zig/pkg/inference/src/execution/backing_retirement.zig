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
    fn finishShrink(raw: *anyopaque) callconv(.c) void {
        const self: *BackingOwner = @ptrCast(@alignCast(raw));
        self.lock();
        var epoch = self.shrink_epoch.?;
        self.shrink_epoch = null;
        // A physical cleanup under a newer epoch already owns the plane.
        // Its gate must end before even a conservative-padding refund runs.
        if (self.physical_released and !self.physical_finished) {
            self.shrink_ready = true;
            self.mutex.unlock();
            epoch.release();
            return; // B's callback owns this shrink metadata pin now.
        }
        self.mutex.unlock();
        self.charge.shrinkStorageBytes(self.measured_bytes);
        self.admitted_bytes.store(self.charge.storageBytes().?, .release);
        self.dropMetadata();
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
        const shrink = self.shrink_ready;
        self.shrink_ready = false;
        self.mutex.unlock();
        if (shrink) {
            self.charge.shrinkStorageBytes(self.measured_bytes);
            self.admitted_bytes.store(self.charge.storageBytes().?, .release);
            self.dropMetadata(); // deferred shrink metadata pin
        }
        self.dropMetadata(); // actual physical cleanup's pin
        epoch.release();
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
