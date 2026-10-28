// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Bounded, prepublished ownership for independent cleanup. The registry and
//! its thread-safe, charged allocator outlive every reservation. Callers pin
//! the complete callback/execution/I/O/allocator parent chain before reserve
//! returns, and publish resources only after reservation succeeds.
const std = @import("std");
const abi = @import("antfly_runtime_abi").runnable_abi;
const cleanup = @import("runnable_cleanup_task.zig");

pub const Registry = struct {
    allocator: std.mem.Allocator,
    limit: usize,
    mutex: std.atomic.Mutex = .unlocked,
    closing: bool = false,
    used: usize = 0,
    head: ?*Slot = null,

    fn lock(self: *Registry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub const Callbacks = struct {
        /// Acquire all parent pins; roll back partial acquisition on error.
        retain: *const fn (*anyopaque) anyerror!void,
        /// Release pins last, after task and registry metadata physically free.
        release: *const fn (*anyopaque) void,
        cleanup: *const fn (*anyopaque, abi.Borrowed, std.Io) anyerror!void,
        /// Physically retire remaining resources after the future joins. On
        /// error, retain their ownership and all pins. This may be retried only
        /// through explicit retireFailedOne, so partial retirement is resumable.
        retire: *const fn (*anyopaque) anyerror!void,
    };
    const Slot = struct {
        registry: *Registry,
        next: ?*Slot = null,
        context: *anyopaque,
        callbacks: Callbacks,
        task: cleanup.Task,
        published: bool = false,
        state: enum { foreground, supervised, joining, failed, retirement_requested, retiring } = .foreground,
        cleanup_failure: ?anyerror = null,
        retirement_failure: ?anyerror = null,
    };
    pub const Failure = struct { cleanup: ?anyerror, retirement: ?anyerror };
    pub const Progress = union(enum) { pending, retired, failed: Failure };

    /// A move-only foreground handle. The slot is already authoritative and
    /// charged. Transfer needs no allocator, new worker or new parent pin.
    pub const Reservation = struct {
        slot: ?*Slot,

        pub fn publish(self: *Reservation) !void {
            const slot = self.slot orelse return error.InvalidRunnableTransition;
            if (slot.published) return error.InvalidRunnableTransition;
            slot.published = true;
        }
        pub fn start(self: *Reservation) !void {
            const slot = self.slot orelse return error.InvalidRunnableTransition;
            if (!slot.published) return error.InvalidRunnableTransition;
            try slot.task.start();
        }
        pub fn waitUntil(self: *Reservation, deadline: std.Io.Clock.Timestamp) !void {
            const slot = self.slot orelse return error.InvalidRunnableTransition;
            try slot.task.waitUntil(deadline);
        }
        pub fn transfer(self: *Reservation) !void {
            const slot = self.slot orelse return error.InvalidRunnableTransition;
            if (!slot.task.started) return error.InvalidRunnableTransition;
            const registry = slot.registry;
            registry.lock();
            defer registry.mutex.unlock();
            slot.state = .supervised;
            self.slot = null;
        }
        /// Only setup rollback before any resource publication. Once published,
        /// cleanup must start and transfer, even after a foreground timeout.
        pub fn abort(self: *Reservation) !void {
            const slot = self.slot orelse return error.InvalidRunnableTransition;
            if (slot.published or slot.task.started) return error.InvalidRunnableTransition;
            const registry = slot.registry;
            registry.lock();
            slot.state = .retiring;
            registry.mutex.unlock();
            self.slot = null;
            registry.releaseSlot(slot);
        }
    };

    pub fn reserve(self: *Registry, io: std.Io, borrow: abi.Borrowed, options: *const abi.Cleanup, context: *anyopaque, callbacks: Callbacks) !Reservation {
        self.lock();
        if (self.closing or self.used >= self.limit) {
            self.mutex.unlock();
            return error.CleanupCapacityUnavailable;
        }
        self.used += 1;
        self.mutex.unlock();
        errdefer {
            self.lock();
            self.used -= 1;
            self.mutex.unlock();
        }
        try callbacks.retain(context);
        errdefer callbacks.release(context);
        const slot = try self.allocator.create(Slot);
        errdefer self.allocator.destroy(slot);
        var task = try cleanup.Task.reserve(self.allocator, io, borrow, options, context, callbacks.cleanup);
        errdefer task.deinit();
        slot.* = .{ .registry = self, .context = context, .callbacks = callbacks, .task = task };
        self.lock();
        defer self.mutex.unlock();
        // A reserve already in flight must not slip past shutdown publication.
        if (self.closing) return error.CleanupCapacityUnavailable;
        slot.next = self.head;
        self.head = slot;
        return .{ .slot = slot };
    }

    /// Claim one callback that has completed. Joining is a physical fence and
    /// can still block; this function does not promise bounded shutdown. Failed
    /// cleanup remains in the registry without automatic replay or retirement.
    pub fn pollOne(self: *Registry) Progress {
        self.lock();
        var candidate = self.head;
        while (candidate) |slot| : (candidate = slot.next) {
            if (slot.state != .supervised) continue;
            switch (slot.task.outcome()) {
                .pending => continue,
                else => {},
            }
            slot.state = .joining;
            break;
        }
        self.mutex.unlock();
        const slot = candidate orelse return .pending;
        slot.task.join() catch |err| {
            self.lock();
            slot.cleanup_failure = err;
            slot.state = .failed;
            self.mutex.unlock();
            return .{ .failed = .{ .cleanup = err, .retirement = null } };
        };
        return self.retire(slot);
    }

    /// Explicit physical retirement of a joined failure; never reruns cleanup.
    /// The retire callback must account for any resources left by partial work.
    pub fn retireFailedOne(self: *Registry) Progress {
        self.lock();
        var candidate = self.head;
        while (candidate) |slot| : (candidate = slot.next) {
            if (slot.state != .failed) continue;
            slot.state = .retiring;
            break;
        }
        self.mutex.unlock();
        return self.retire(candidate orelse return .pending);
    }

    /// Claim exactly one already joined failed slot. A request needs no new
    /// metadata and cannot silently merge with another request or attach to a
    /// future failure. The reserved reaper performs the physical attempt.
    pub fn requestFailedRetirement(self: *Registry) !void {
        self.lock();
        defer self.mutex.unlock();
        var candidate = self.head;
        while (candidate) |slot| : (candidate = slot.next) {
            if (slot.state != .failed) continue;
            slot.state = .retirement_requested;
            return;
        }
        return error.CleanupNotFailed;
    }

    pub fn retireRequestedOne(self: *Registry) Progress {
        self.lock();
        var candidate = self.head;
        while (candidate) |slot| : (candidate = slot.next) {
            if (slot.state != .retirement_requested) continue;
            slot.state = .retiring;
            break;
        }
        self.mutex.unlock();
        return self.retire(candidate orelse return .pending);
    }

    fn retire(self: *Registry, slot: *Slot) Progress {
        slot.callbacks.retire(slot.context) catch |err| {
            self.lock();
            slot.retirement_failure = err;
            slot.state = .failed;
            const failure: Failure = .{ .cleanup = slot.cleanup_failure, .retirement = err };
            self.mutex.unlock();
            return .{ .failed = failure };
        };
        self.releaseSlot(slot);
        return .retired;
    }

    fn releaseSlot(self: *Registry, slot: *Slot) void {
        const callbacks = slot.callbacks;
        const context = slot.context;
        // Keep the registry capacity held throughout physical metadata free
        // and final parent release. No callback or allocator access follows it.
        slot.task.deinit();
        self.lock();
        var link = &self.head;
        while (link.* != slot) link = &link.*.?.next;
        link.* = slot.next;
        self.mutex.unlock();
        self.allocator.destroy(slot);
        callbacks.release(context);
        self.lock();
        self.used -= 1;
        self.mutex.unlock();
    }

    pub const Snapshot = struct {
        capacity: usize,
        retained: usize,
        foreground: usize = 0,
        supervised: usize = 0,
        joining: usize = 0,
        failed: usize = 0,
        retirement_requested: usize = 0,
        retiring: usize = 0,
        /// Reservations being constructed or final metadata/pins being freed
        /// remain charged even while they are absent from the linked list.
        in_flight: usize = 0,
        closing: bool,
    };

    pub fn snapshot(self: *Registry) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        var result: Snapshot = .{ .capacity = self.limit, .retained = self.used, .closing = self.closing };
        var linked: usize = 0;
        var cursor = self.head;
        while (cursor) |slot| : (cursor = slot.next) {
            linked += 1;
            switch (slot.state) {
                .foreground => result.foreground += 1,
                .supervised => result.supervised += 1,
                .joining => result.joining += 1,
                .failed => result.failed += 1,
                .retirement_requested => result.retirement_requested += 1,
                .retiring => result.retiring += 1,
            }
        }
        std.debug.assert(linked <= result.retained);
        result.in_flight = result.retained - linked;
        return result;
    }

    pub fn failures(self: *Registry) usize {
        self.lock();
        defer self.mutex.unlock();
        var count: usize = 0;
        var cursor = self.head;
        while (cursor) |slot| : (cursor = slot.next) {
            if (slot.cleanup_failure != null or slot.retirement_failure != null) count += 1;
        }
        return count;
    }

    pub fn retained(self: *Registry) usize {
        self.lock();
        defer self.mutex.unlock();
        return self.used;
    }
    pub fn close(self: *Registry) void {
        self.lock();
        defer self.mutex.unlock();
        self.closing = true;
    }
    /// Shutdown cannot discard live tasks or failures, or their parent pins.
    pub fn deinit(self: *Registry) !void {
        self.lock();
        defer self.mutex.unlock();
        if (!self.closing or self.used != 0) return error.CleanupPending;
        std.debug.assert(self.head == null);
    }
};

/// One preadmitted reaper on an executor independent of gated cleanup children.
/// The API keeps this runtime, both worker leases, allocators and service parents
/// alive through listener drain and this physical join. Failures stay charged
/// until an explicit retirement request; shutdown cannot discard them.
pub const Runtime = struct {
    registry: Registry,
    io: std.Io,
    future: std.Io.Future(void),
    stopping: std.atomic.Value(bool) = .init(false),
    joined: bool = false,

    pub fn create(allocator: std.mem.Allocator, reaper_io: std.Io, limit: usize) !*Runtime {
        if (limit == 0) return error.CleanupCapacityUnavailable;
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        self.* = .{ .registry = .{ .allocator = allocator, .limit = limit }, .io = reaper_io, .future = undefined };
        self.future = try reaper_io.concurrent(maintain, .{self});
        return self;
    }

    pub fn close(self: *Runtime) void {
        self.registry.close();
        self.stopping.store(true, .release);
    }

    /// Request one explicit physical failure retirement on the reserved reaper.
    /// This does not replay the cleanup callback or renew its expired budget.
    pub fn requestFailureRetirement(self: *Runtime) !void {
        try self.registry.requestFailedRetirement();
    }

    /// Physical shutdown fence, deliberately unbounded while physical cleanup
    /// or an explicitly retained failure still owns resources or parent pins.
    pub fn join(self: *Runtime) void {
        std.debug.assert(!self.joined);
        const io = self.io;
        const protection = io.swapCancelProtection(.blocked);
        defer _ = io.swapCancelProtection(protection);
        self.close();
        self.future.await(io);
        self.joined = true;
    }

    /// The publisher fences snapshot readers before freeing this stable owner.
    /// Its mutex must not be held across the potentially unbounded join.
    pub fn destroyJoined(self: *Runtime) void {
        std.debug.assert(self.joined);
        const allocator = self.registry.allocator;
        self.registry.deinit() catch unreachable;
        allocator.destroy(self);
    }

    pub fn deinit(self: *Runtime) void {
        self.join();
        self.destroyJoined();
    }

    fn maintain(self: *Runtime) void {
        const protection = self.io.swapCancelProtection(.blocked);
        defer _ = self.io.swapCancelProtection(protection);
        while (true) {
            for (0..32) |_| switch (self.registry.pollOne()) {
                .pending => break,
                else => {},
            };
            for (0..32) |_| switch (self.registry.retireRequestedOne()) {
                .pending => break,
                else => {},
            };
            if (self.stopping.load(.acquire) and self.registry.retained() == 0) return;
            self.io.sleep(.fromMilliseconds(1), .awake) catch unreachable;
        }
    }
};
