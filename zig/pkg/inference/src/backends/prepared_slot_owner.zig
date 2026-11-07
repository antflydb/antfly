// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cache owns actual prepared payload+charge. Frozen selectors own metadata;
//! invocations/captures pin payload until checked completion, not output life.
const std = @import("std");

pub const Owner = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    metadata_references: usize = 1,
    payload_pins: usize = 0,
    retiring: bool = false,
    retired: bool = false,
    published: bool = false,
    epoch: u64,
    slot: usize,
    // No Session back-reference: the Session can itself cache selectors.
    resource: struct {
        context: *anyopaque,
        bytes: usize,
        release_retired: *const fn (*const Owner) void,
    },

    pub fn create(allocator: std.mem.Allocator, slot: usize, epoch: u64, resource: @FieldType(Owner, "resource")) !*Owner {
        if (epoch == 0 or resource.bytes == 0) return error.InvalidPreparedSlotOwner;
        const self = try allocator.create(Owner);
        self.* = .{ .allocator = allocator, .slot = slot, .epoch = epoch, .resource = resource };
        return self;
    }
    pub fn publish(self: *Owner) void {
        std.debug.assert(!self.published and !self.retired and self.metadata_references == 1 and self.payload_pins == 0);
        self.published = true;
    }
    pub fn discardUnpublished(self: *Owner) void {
        std.debug.assert(!self.published and !self.retired and self.metadata_references == 1 and self.payload_pins == 0);
        self.allocator.destroy(self);
    }

    /// Cache lookup-to-ref acquisition uses its existing mutation domain.
    /// This keeps metadata alive after payload eviction without holding bytes.
    pub fn profile(self: *Owner) ?Profile {
        // This mutex covers counters/flags only, never allocation or SDK work.
        // Contention is not a cold/unready payload fact for variant caching.
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (!self.published or self.retiring or self.retired) return null;
        self.metadata_references += 1;
        return .{ .owner = self, .epoch = self.epoch };
    }
    pub const Profile = struct {
        owner: *Owner,
        epoch: u64,
        pub fn live(self: Profile) bool {
            while (!self.owner.mutex.tryLock()) std.atomic.spinLoopHint();
            defer self.owner.mutex.unlock();
            return self.owner.epoch == self.epoch and self.owner.published and !self.owner.retiring and !self.owner.retired;
        }
        pub fn release(self: Profile) void {
            const owner = self.owner;
            while (!owner.mutex.tryLock()) std.atomic.spinLoopHint();
            std.debug.assert(owner.epoch == self.epoch and owner.metadata_references != 0);
            owner.metadata_references -= 1;
            const destroy = owner.metadata_references == 0;
            if (destroy) std.debug.assert(owner.retired and owner.payload_pins == 0);
            owner.mutex.unlock();
            if (destroy) owner.allocator.destroy(owner);
        }
        /// Stabilize actual bytes BEFORE admission; transfer to Frame through
        /// completion, or capture through eviction/drain. Never an output pin.
        pub fn pin(self: Profile) ?Pin {
            return self.owner.tryPin(self.epoch);
        }
    };

    pub fn tryPin(self: *Owner, epoch: u64) ?Pin {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        if (!self.published or self.retiring or self.retired or self.epoch != epoch) return null;
        self.metadata_references += 1;
        self.payload_pins += 1;
        return .{ .owner = self, .epoch = epoch };
    }
    pub const Pin = struct {
        owner: *Owner,
        epoch: u64,
        /// Each actual command buffer owns an independent payload pin. A
        /// successful bridge registration invokes this exactly once; captures
        /// retain an independent pin until their checked eviction/drain.
        pub fn retain(self: Pin) Pin {
            retainSubmittedUse(self.owner);
            return self;
        }
        pub fn valid(self: Pin) bool {
            while (!self.owner.mutex.tryLock()) std.atomic.spinLoopHint();
            defer self.owner.mutex.unlock();
            return self.owner.epoch == self.epoch and self.owner.published and !self.owner.retiring and !self.owner.retired and self.owner.payload_pins != 0;
        }
        pub fn release(self: Pin) void {
            while (!self.owner.mutex.tryLock()) std.atomic.spinLoopHint();
            defer self.owner.mutex.unlock();
            std.debug.assert(self.owner.epoch == self.epoch and !self.owner.retiring and !self.owner.retired and self.owner.payload_pins != 0 and self.owner.metadata_references > 1);
            self.owner.payload_pins -= 1;
            self.owner.metadata_references -= 1;
        }
    };

    /// C completion bridge callbacks. The caller's original acquired Pin
    /// guarantees the owner remains valid through registration. Completion
    /// performs metadata-only refcount work, never driver operations.
    pub fn retainSubmittedUse(raw: *anyopaque) callconv(.c) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        std.debug.assert(self.published and !self.retiring and !self.retired and self.payload_pins != 0);
        self.metadata_references += 1;
        self.payload_pins += 1;
    }
    pub fn releaseSubmittedUse(raw: *anyopaque) callconv(.c) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        (Pin{ .owner = self, .epoch = self.epoch }).release();
    }

    pub fn retirementInProgress(self: *Owner) bool {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        return self.retiring and self.payload_pins == 0;
    }
    /// Idle selector metadata is not a payload pin. Active/armed invocations
    /// exclude destructive setPurgeableEmpty BEFORE entering the bridge.
    pub fn beginRetirement(self: *Owner) ?Retirement {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        if (!self.published or self.retiring or self.retired or self.payload_pins != 0) return null;
        self.retiring = true;
        return .{ .owner = self };
    }
    pub const Retirement = struct {
        owner: *Owner,
        /// Only a proved decline before teardown can reopen this payload.
        /// Uncertain driver errors retain the charge and fail closed.
        pub fn abortBeforeTeardown(self: Retirement) void {
            while (!self.owner.mutex.tryLock()) std.atomic.spinLoopHint();
            defer self.owner.mutex.unlock();
            std.debug.assert(self.owner.retiring and self.owner.payload_pins == 0);
            self.owner.retiring = false;
        }
        /// Payload teardown and cache removal precede actual quota release.
        /// Surviving selectors keep only retired metadata, which cannot match.
        pub fn commitRetiredPayload(self: Retirement) void {
            const owner = self.owner;
            std.debug.assert(owner.retiring and owner.payload_pins == 0);
            owner.resource.release_retired(owner);
            while (!owner.mutex.tryLock()) std.atomic.spinLoopHint();
            owner.retired = true;
            owner.retiring = false;
            owner.published = false;
            owner.metadata_references -= 1;
            const destroy = owner.metadata_references == 0;
            owner.mutex.unlock();
            if (destroy) owner.allocator.destroy(owner);
        }
    };
};
