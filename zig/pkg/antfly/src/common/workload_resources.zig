// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Fixed resource envelopes and explicit ownership for the execution scheduler.
//! This ledger accounts credits; allocating memory still requires the storage
//! resource manager's reservation. It never executes an operator or waits while
//! holding a partial resource bundle. Owners must keep the ledger address stable.
const std = @import("std");

pub const Lane = enum { bounded_read, general_read, analytical_read, write, background, control, recovery, transition };
pub const lane_count = @typeInfo(Lane).@"enum".fields.len;
pub const Kind = enum { request, queue, runnable, retained_state, resume_queue, local_io, remote_attempt, recovery };

/// queued_bytes references already charged retained_bytes; the two dimensions
/// are different ceilings, never two charges to the physical memory total.
pub const Bundle = struct {
    handles: u64 = 0,
    requests: u64 = 0,
    queued: u64 = 0,
    queued_bytes: u64 = 0,
    runnable: u64 = 0,
    retained_bytes: u64 = 0,
    io: u64 = 0,
    remote_attempts: u64 = 0,
    recovery_obligations: u64 = 0,

    pub fn add(a: Bundle, b: Bundle) !Bundle {
        var out: Bundle = .{};
        inline for (std.meta.fields(Bundle)) |field|
            @field(out, field.name) = std.math.add(u64, @field(a, field.name), @field(b, field.name)) catch return error.ResourceRequestTooLarge;
        return out;
    }

    fn sub(a: Bundle, b: Bundle) Bundle {
        var out: Bundle = .{};
        inline for (std.meta.fields(Bundle)) |field| {
            std.debug.assert(@field(a, field.name) >= @field(b, field.name));
            @field(out, field.name) = @field(a, field.name) - @field(b, field.name);
        }
        return out;
    }

    pub fn fits(a: Bundle, ceiling: Bundle) bool {
        inline for (std.meta.fields(Bundle)) |field|
            if (@field(a, field.name) > @field(ceiling, field.name)) return false;
        return true;
    }

    fn maximum(a: Bundle, b: Bundle) Bundle {
        var out: Bundle = .{};
        inline for (std.meta.fields(Bundle)) |field|
            @field(out, field.name) = @max(@field(a, field.name), @field(b, field.name));
        return out;
    }

    fn minimum(a: Bundle, b: Bundle) Bundle {
        var out: Bundle = .{};
        inline for (std.meta.fields(Bundle)) |field|
            @field(out, field.name) = @min(@field(a, field.name), @field(b, field.name));
        return out;
    }
};

pub const LanePolicy = struct { floor: Bundle = .{}, ceiling: Bundle = .{} };
pub const Policy = struct {
    total: Bundle,
    lanes: [lane_count]LanePolicy,

    pub fn validate(self: Policy) !void {
        var floors: Bundle = .{};
        for (self.lanes) |lane| {
            if (!lane.floor.fits(lane.ceiling) or !lane.ceiling.fits(self.total)) return error.InvalidPolicy;
            floors = floors.add(lane.floor) catch return error.InvalidPolicy;
        }
        if (!floors.fits(self.total)) return error.InvalidPolicy;
    }

    /// Maximum possible bundle for one lane with every other lane at its floor.
    pub fn maximumFor(self: Policy, lane: Lane) Bundle {
        var available = self.total;
        for (self.lanes, 0..) |other, i| {
            if (i != @intFromEnum(lane)) available = available.sub(other.floor);
        }
        return Bundle.minimum(available, self.lanes[@intFromEnum(lane)].ceiling);
    }
};

const Handle = struct { index: usize, generation: u64 };
const Entry = struct {
    generation: u64 = 0,
    live: bool = false,
    kind: Kind = .request,
    lane: Lane = .general_read,
    bundle: Bundle = .{},
    parent: ?Handle = null,
    children: usize = 0,
    next_free: ?usize = null,
};

pub fn Lease(comptime kind: Kind) type {
    return struct {
        owner: *Ledger,
        handle: Handle,

        /// Copied/stale handles cannot decrement another owner's counters.
        /// A request cannot retire while it still owns child work/state.
        pub fn release(self: *@This()) !void {
            try self.owner.release(self.handle, kind);
        }

        pub fn grow(self: *@This(), additional: Bundle) !void {
            try self.owner.grow(self.handle, kind, additional);
        }

        pub fn reserved(self: *const @This()) !Bundle {
            self.owner.lock();
            defer self.owner.mutex.unlock();
            return (try self.owner.entry(self.handle, kind)).bundle;
        }

        /// At an audited handoff, replace queue/runnable/state ownership without
        /// an uncharged interval or an extra metadata slot. `bundle` describes
        /// the complete new ownership, including all state still retained.
        pub fn exchange(self: *@This(), comptime next: Kind, bundle: Bundle) !Lease(next) {
            if (!((kind == .queue and next == .runnable) or
                (kind == .runnable and next == .retained_state) or
                (kind == .retained_state and (next == .runnable or next == .resume_queue)) or
                (kind == .resume_queue and (next == .runnable or next == .retained_state))))
                @compileError("invalid workload ownership handoff");
            const handle = try self.owner.exchange(self.handle, kind, next, bundle);
            self.handle.generation = 0;
            return .{ .owner = self.owner, .handle = handle };
        }

        /// Transfer this independently owned state/attempt to a longer-lived
        /// owner. Call only after the destination has accepted actual ownership.
        pub fn detach(self: *@This(), destination: Lane) !@This() {
            if (kind == .request or kind == .runnable or kind == .queue or kind == .resume_queue)
                @compileError("request execution cannot detach from its lifetime owner");
            const handle = try self.owner.detach(self.handle, kind, destination);
            self.handle.generation = 0;
            return .{ .owner = self.owner, .handle = handle };
        }
    };
}

pub const RequestLease = Lease(.request);
pub const QueueLease = Lease(.queue);
pub const RunnableLease = Lease(.runnable);
pub const RetainedStateLease = Lease(.retained_state);
/// A wakeup reuses its pre-reserved continuation metadata and byte credits.
/// Ordinary start-queue exhaustion cannot prevent an existing owner resuming.
pub const ResumeQueueLease = Lease(.resume_queue);
pub const LocalIoLease = Lease(.local_io);
pub const RemoteAttemptLease = Lease(.remote_attempt);
pub const RecoveryLease = Lease(.recovery);

pub const Ledger = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    policy: Policy,
    entries: []Entry,
    free_head: ?usize,
    used: [lane_count]Bundle = @splat(.{}),
    total: Bundle = .{},

    pub fn init(allocator: std.mem.Allocator, policy: Policy) !Ledger {
        try policy.validate();
        const count = std.math.cast(usize, policy.total.handles) orelse return error.InvalidPolicy;
        const entries = try allocator.alloc(Entry, count);
        for (entries, 0..) |*slot, i| slot.* = .{ .next_free = if (i + 1 < count) i + 1 else null };
        return .{ .allocator = allocator, .policy = policy, .entries = entries, .free_head = if (count == 0) null else 0 };
    }

    pub fn deinit(self: *Ledger) void {
        std.debug.assert(self.total.handles == 0);
        self.allocator.free(self.entries);
    }

    fn lock(self: *Ledger) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn entry(self: *Ledger, handle: Handle, kind: Kind) !*Entry {
        if (handle.index >= self.entries.len) return error.LeaseRetired;
        const result = &self.entries[handle.index];
        if (!result.live or result.generation != handle.generation) return error.LeaseRetired;
        if (result.kind != kind) return error.InvalidLease;
        return result;
    }

    fn fits(self: *Ledger, use: [lane_count]Bundle) bool {
        var reserved: Bundle = .{};
        var existing: Bundle = .{};
        var actual: Bundle = .{};
        for (use, self.policy.lanes, self.used) |lane, policy, current| {
            // Reductions do not revoke existing state or block an unrelated
            // cleanup dimension that still fits. No overdrawn dimension grows.
            if (!lane.fits(Bundle.maximum(policy.ceiling, current))) return false;
            reserved = reserved.add(Bundle.maximum(lane, policy.floor)) catch return false;
            existing = existing.add(Bundle.maximum(current, policy.floor)) catch return false;
            actual = actual.add(lane) catch return false;
        }
        return reserved.fits(Bundle.maximum(self.policy.total, existing)) and
            actual.fits(Bundle.maximum(self.policy.total, self.total));
    }

    fn charge(self: *Ledger, lane: Lane, bundle: Bundle) !void {
        if (!bundle.fits(self.policy.maximumFor(lane))) return error.ResourceRequestTooLarge;
        var proposed = self.used;
        proposed[@intFromEnum(lane)] = try proposed[@intFromEnum(lane)].add(bundle);
        if (!self.fits(proposed)) return error.ResourceTemporarilyUnavailable;
        self.used = proposed;
        self.total = try self.total.add(bundle);
    }

    fn uncharge(self: *Ledger, lane: Lane, bundle: Bundle) void {
        self.used[@intFromEnum(lane)] = self.used[@intFromEnum(lane)].sub(bundle);
        self.total = self.total.sub(bundle);
    }

    fn create(self: *Ledger, comptime kind: Kind, lane: Lane, requested: Bundle, parent: ?Handle) !Lease(kind) {
        // One fixed metadata entry is part of every grant, including cleanup
        // and helper grants. Bulk cannot consume protected metadata capacity.
        if (requested.handles != 0) return error.InvalidLease;
        try validateBundle(kind, requested);
        var bundle = requested;
        bundle.handles = 1;
        const index = self.free_head orelse return error.ResourceTemporarilyUnavailable;
        try self.charge(lane, bundle);
        const slot = &self.entries[index];
        self.free_head = slot.next_free;
        const generation = slot.generation + 1;
        slot.* = .{ .live = true, .kind = kind, .lane = lane, .bundle = bundle, .parent = parent, .generation = generation };
        if (parent) |handle| (try self.entry(handle, .request)).children += 1;
        return .{ .owner = self, .handle = .{ .index = index, .generation = generation } };
    }

    pub fn admit(self: *Ledger, lane: Lane, retained_bytes: u64) !RequestLease {
        self.lock();
        defer self.mutex.unlock();
        return self.create(.request, lane, .{ .requests = 1, .retained_bytes = retained_bytes }, null);
    }

    /// Initial/resume execution obtains its complete incremental bundle in one
    /// operation. A continuation's existing state stays in its state lease.
    pub fn acquire(self: *Ledger, comptime kind: Kind, parent: *const RequestLease, bundle: Bundle) !Lease(kind) {
        if (kind == .request) @compileError("use admit for a request lifetime");
        self.lock();
        defer self.mutex.unlock();
        if (parent.owner != self) return error.InvalidLease;
        const owner = try self.entry(parent.handle, .request);
        if (kind == .runnable and owner.lane == .transition) return error.TransitionCannotExecute;
        var required = bundle;
        required.handles = 1;
        required.retained_bytes = std.math.add(u64, required.retained_bytes, owner.bundle.retained_bytes) catch return error.ResourceRequestTooLarge;
        for (self.entries) |child| {
            // Live helpers/I/O can finish independently; their occupancy is
            // contention. Pinned state stays while this request waits.
            if (child.live and (child.kind == .retained_state or child.kind == .resume_queue) and child.parent != null and std.meta.eql(child.parent.?, parent.handle))
                required.retained_bytes = std.math.add(u64, required.retained_bytes, child.bundle.retained_bytes) catch return error.ResourceRequestTooLarge;
        }
        if (!required.fits(self.policy.maximumFor(owner.lane))) return error.ResourceRequestTooLarge;
        return self.create(kind, owner.lane, bundle, parent.handle);
    }

    pub fn requestLane(self: *Ledger, request: *const RequestLease) !Lane {
        if (request.owner != self) return error.InvalidLease;
        self.lock();
        defer self.mutex.unlock();
        return (try self.entry(request.handle, .request)).lane;
    }

    pub fn validateContinuation(self: *Ledger, request: *const RequestLease, continuation: *const RetainedStateLease) !void {
        if (request.owner != self or continuation.owner != self) return error.InvalidLease;
        self.lock();
        defer self.mutex.unlock();
        _ = try self.entry(request.handle, .request);
        const state = try self.entry(continuation.handle, .retained_state);
        if (state.parent == null or !std.meta.eql(state.parent.?, request.handle)) return error.InvalidLease;
    }

    fn release(self: *Ledger, handle: Handle, kind: Kind) !void {
        self.lock();
        defer self.mutex.unlock();
        const owned = self.entry(handle, kind) catch |err| switch (err) {
            error.LeaseRetired => return,
            else => return err,
        };
        if (owned.children != 0) return error.OwnedWorkStillLive;
        if (owned.parent) |parent| (try self.entry(parent, .request)).children -= 1;
        self.uncharge(owned.lane, owned.bundle);
        owned.live = false;
        // Retire an exhausted generation permanently rather than wrapping into
        // an old copied handle. This cannot fabricate new metadata capacity.
        if (owned.generation != std.math.maxInt(u64)) {
            owned.next_free = self.free_head;
            self.free_head = handle.index;
        }
    }

    fn grow(self: *Ledger, handle: Handle, kind: Kind, additional: Bundle) !void {
        if (additional.handles != 0) return error.InvalidLease;
        self.lock();
        defer self.mutex.unlock();
        const owned = try self.entry(handle, kind);
        const next = try owned.bundle.add(additional);
        var payload = next;
        payload.handles = 0;
        try validateBundle(kind, payload);
        if (!next.fits(self.policy.maximumFor(owned.lane))) return error.ResourceRequestTooLarge;
        try self.charge(owned.lane, additional);
        owned.bundle = next;
    }

    fn detach(self: *Ledger, handle: Handle, kind: Kind, destination: Lane) !Handle {
        self.lock();
        defer self.mutex.unlock();
        const owned = try self.entry(handle, kind);
        if (owned.generation == std.math.maxInt(u64)) return error.GenerationExhausted;
        var proposed = self.used;
        proposed[@intFromEnum(owned.lane)] = proposed[@intFromEnum(owned.lane)].sub(owned.bundle);
        proposed[@intFromEnum(destination)] = try proposed[@intFromEnum(destination)].add(owned.bundle);
        if (!self.fits(proposed)) return error.ResourceTemporarilyUnavailable;
        if (owned.parent) |parent| (try self.entry(parent, .request)).children -= 1;
        owned.parent = null;
        owned.lane = destination;
        owned.generation += 1;
        self.used = proposed;
        return .{ .index = handle.index, .generation = owned.generation };
    }

    fn exchange(self: *Ledger, handle: Handle, kind: Kind, next: Kind, requested: Bundle) !Handle {
        if (requested.handles != 0) return error.InvalidLease;
        try validateBundle(next, requested);
        self.lock();
        defer self.mutex.unlock();
        const owned = try self.entry(handle, kind);
        if (owned.generation == std.math.maxInt(u64)) return error.GenerationExhausted;
        if (next == .runnable and owned.lane == .transition) return error.TransitionCannotExecute;
        // No handoff silently drops live state. An operator can explicitly
        // release credits at its verified suspension boundary after freeing it.
        if ((kind == .retained_state or kind == .resume_queue) and requested.retained_bytes < owned.bundle.retained_bytes)
            return error.RetainedStateStillLive;
        var bundle = requested;
        bundle.handles = 1;
        var required = bundle;
        if (owned.parent) |parent| {
            required.retained_bytes = std.math.add(u64, required.retained_bytes, (try self.entry(parent, .request)).bundle.retained_bytes) catch return error.ResourceRequestTooLarge;
            for (self.entries, 0..) |other, i| {
                if (i != handle.index and other.live and (other.kind == .retained_state or other.kind == .resume_queue) and other.parent != null and std.meta.eql(other.parent.?, parent))
                    required.retained_bytes = std.math.add(u64, required.retained_bytes, other.bundle.retained_bytes) catch return error.ResourceRequestTooLarge;
            }
        }
        if (!bundle.fits(owned.bundle) and !required.fits(self.policy.maximumFor(owned.lane))) return error.ResourceRequestTooLarge;
        var proposed = self.used;
        proposed[@intFromEnum(owned.lane)] = try proposed[@intFromEnum(owned.lane)].sub(owned.bundle).add(bundle);
        if (!self.fits(proposed)) return error.ResourceTemporarilyUnavailable;
        self.total = try self.total.sub(owned.bundle).add(bundle);
        self.used = proposed;
        owned.bundle = bundle;
        owned.kind = next;
        owned.generation += 1;
        return .{ .index = handle.index, .generation = owned.generation };
    }

    /// Atomic demotion/resume transfers every child charge with its request.
    /// Live helpers/runnable work prohibit transfer, even when the parent yielded.
    pub fn transfer(self: *Ledger, request: *const RequestLease, destination: Lane) !void {
        self.lock();
        defer self.mutex.unlock();
        if (request.owner != self) return error.InvalidLease;
        const owner = try self.entry(request.handle, .request);
        var bundle = owner.bundle;
        for (self.entries) |child| {
            if (!child.live or child.parent == null or !std.meta.eql(child.parent.?, request.handle)) continue;
            if (child.kind == .runnable or child.kind == .resume_queue or child.kind == .queue) return error.ExecutionStillLive;
            bundle = try bundle.add(child.bundle);
        }
        var proposed = self.used;
        proposed[@intFromEnum(owner.lane)] = proposed[@intFromEnum(owner.lane)].sub(bundle);
        proposed[@intFromEnum(destination)] = try proposed[@intFromEnum(destination)].add(bundle);
        if (!self.fits(proposed)) return error.ResourceTemporarilyUnavailable;
        for (self.entries) |*child| {
            if (child.live and child.parent != null and std.meta.eql(child.parent.?, request.handle)) child.lane = destination;
        }
        owner.lane = destination;
        self.used = proposed;
    }

    /// Running ownership survives reductions; only future growth/grants stop.
    pub fn reconfigure(self: *Ledger, policy: Policy) !void {
        try policy.validate();
        if (policy.total.handles > self.entries.len) return error.InvalidPolicy;
        self.lock();
        defer self.mutex.unlock();
        self.policy = policy;
    }

    pub const Stats = struct { total: Bundle, lanes: [lane_count]Bundle, policy: Policy };
    pub fn snapshot(self: *Ledger) Stats {
        self.lock();
        defer self.mutex.unlock();
        return .{ .total = self.total, .lanes = self.used, .policy = self.policy };
    }
};

fn validateBundle(kind: Kind, bundle: Bundle) !void {
    var remaining = bundle;
    remaining.retained_bytes = 0;
    switch (kind) {
        .request => {
            if (remaining.requests != 1) return error.InvalidLease;
            remaining.requests = 0;
        },
        .queue => {
            if (remaining.queued != 1 or bundle.retained_bytes != 0) return error.InvalidLease;
            remaining.queued = 0;
            remaining.queued_bytes = 0;
        },
        .runnable => {
            if (remaining.runnable == 0) return error.InvalidLease;
            remaining.runnable = 0;
            remaining.io = 0; // incremental minimum working-set bundle
        },
        .retained_state, .resume_queue => {},
        .local_io => {
            if (remaining.io == 0) return error.InvalidLease;
            remaining.io = 0;
        },
        .remote_attempt => {
            if (remaining.remote_attempts != 1) return error.InvalidLease;
            remaining.remote_attempts = 0;
        },
        .recovery => {
            if (remaining.recovery_obligations != 1) return error.InvalidLease;
            remaining.recovery_obligations = 0;
        },
    }
    if (!remaining.fits(.{})) return error.InvalidLease;
}

fn testPolicy() Policy {
    const total: Bundle = .{ .handles = 32, .requests = 8, .queued = 8, .queued_bytes = 100, .runnable = 4, .retained_bytes = 100, .io = 4, .remote_attempts = 4, .recovery_obligations = 4 };
    var lanes: [lane_count]LanePolicy = @splat(.{ .ceiling = total });
    lanes[@intFromEnum(Lane.bounded_read)].floor = .{ .handles = 3, .requests = 1, .runnable = 1, .retained_bytes = 10 };
    lanes[@intFromEnum(Lane.control)].floor = .{ .handles = 3, .requests = 1, .runnable = 1, .retained_bytes = 10 };
    lanes[@intFromEnum(Lane.transition)].floor = .{ .handles = 3, .requests = 1, .retained_bytes = 20 };
    return .{ .total = total, .lanes = lanes };
}

test "workload admission resource floors protect memory execution and metadata together" {
    var ledger = try Ledger.init(std.testing.allocator, testPolicy());
    defer ledger.deinit();
    var bulk = try ledger.admit(.general_read, 60);
    defer bulk.release() catch unreachable;
    var compute = try ledger.acquire(.runnable, &bulk, .{ .runnable = 2 });
    defer compute.release() catch unreachable;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ledger.acquire(.runnable, &bulk, .{ .runnable = 1 }));
    try std.testing.expectError(error.ResourceRequestTooLarge, bulk.grow(.{ .retained_bytes = 1 }));
    var bounded = try ledger.admit(.bounded_read, 10);
    defer bounded.release() catch unreachable;
    var bounded_run = try ledger.acquire(.runnable, &bounded, .{ .runnable = 1 });
    defer bounded_run.release() catch unreachable;
    var control = try ledger.admit(.control, 10);
    defer control.release() catch unreachable;
    var cleanup = try ledger.acquire(.runnable, &control, .{ .runnable = 1 });
    defer cleanup.release() catch unreachable;
    try std.testing.expectEqual(@as(u64, 4), ledger.snapshot().total.runnable);
}

test "workload admission state survives suspension and copied handles cannot release reused slots" {
    var ledger = try Ledger.init(std.testing.allocator, testPolicy());
    defer ledger.deinit();
    var request = try ledger.admit(.general_read, 1);
    var state = try ledger.acquire(.retained_state, &request, .{ .retained_bytes = 20 });
    var compute = try ledger.acquire(.runnable, &request, .{ .runnable = 1, .io = 1 });
    var stale = compute;
    try std.testing.expectError(error.OwnedWorkStillLive, request.release());
    try compute.release();
    try std.testing.expectEqual(@as(u64, 21), ledger.snapshot().total.retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().total.runnable);
    var resumed = try ledger.acquire(.runnable, &request, .{ .runnable = 1 });
    try stale.release();
    try std.testing.expectEqual(@as(u64, 1), ledger.snapshot().total.runnable);
    try resumed.release();
    try state.release();
    try request.release();
    try request.release();
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().total.handles);
}

test "workload admission atomic demotion and detached recovery keep one owner" {
    var policy = testPolicy();
    policy.lanes[@intFromEnum(Lane.general_read)].ceiling.retained_bytes = 20;
    var ledger = try Ledger.init(std.testing.allocator, policy);
    defer ledger.deinit();
    var blocker = try ledger.admit(.general_read, 20);
    var request = try ledger.admit(.bounded_read, 5);
    var retained = try ledger.acquire(.retained_state, &request, .{ .retained_bytes = 5 });
    var compute = try ledger.acquire(.runnable, &request, .{ .runnable = 1 });
    try std.testing.expectError(error.ExecutionStillLive, ledger.transfer(&request, .general_read));
    try compute.release();
    const before = ledger.snapshot().total;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ledger.transfer(&request, .general_read));
    try ledger.transfer(&request, .transition);
    try std.testing.expectEqualDeep(before, ledger.snapshot().total);
    try std.testing.expectError(error.TransitionCannotExecute, ledger.acquire(.runnable, &request, .{ .runnable = 1 }));
    try blocker.release();
    try ledger.transfer(&request, .general_read);
    var recovery = try ledger.acquire(.recovery, &request, .{ .recovery_obligations = 1, .retained_bytes = 1 });
    // A real caller persists the durable decision/handoff before detaching.
    var old_recovery_owner = recovery;
    recovery = try recovery.detach(.recovery);
    try old_recovery_owner.release();
    try retained.release();
    try request.release();
    try std.testing.expectEqual(@as(u64, 1), ledger.snapshot().total.recovery_obligations);
    try recovery.release();
}

test "workload admission policy reductions retain live reservations and deny growth" {
    var policy = testPolicy();
    var ledger = try Ledger.init(std.testing.allocator, policy);
    defer ledger.deinit();
    var request = try ledger.admit(.general_read, 50);
    policy.lanes[@intFromEnum(Lane.general_read)].ceiling.retained_bytes = 20;
    try ledger.reconfigure(policy);
    try std.testing.expectEqual(@as(u64, 50), ledger.snapshot().total.retained_bytes);
    try std.testing.expectError(error.ResourceRequestTooLarge, request.grow(.{ .retained_bytes = 1 }));
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ledger.admit(.general_read, 1));
    try request.release();
    var next = try ledger.admit(.general_read, 20);
    try next.release();
}

test "workload admission queue grant and suspension atomically reuse bounded metadata" {
    var policy = testPolicy();
    policy.lanes[@intFromEnum(Lane.general_read)].ceiling.handles = 3;
    var ledger = try Ledger.init(std.testing.allocator, policy);
    defer ledger.deinit();
    var request = try ledger.admit(.general_read, 5);
    var snapshot = try ledger.acquire(.retained_state, &request, .{ .retained_bytes = 20 });
    var queued = try ledger.acquire(.queue, &request, .{ .queued = 1, .queued_bytes = 25 });
    var stale_queue = queued;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ledger.acquire(.runnable, &request, .{ .runnable = 1 }));
    var running = try queued.exchange(.runnable, .{ .runnable = 1, .retained_bytes = 5 });
    try stale_queue.release();
    try std.testing.expectEqual(@as(u64, 1), ledger.snapshot().total.runnable);
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().total.queued_bytes);
    var continuation = try running.exchange(.retained_state, .{ .retained_bytes = 5 });
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().total.runnable);
    try std.testing.expectEqual(@as(u64, 30), ledger.snapshot().total.retained_bytes);
    var resumed = try continuation.exchange(.runnable, .{ .runnable = 1, .retained_bytes = 5 });
    try resumed.release();
    try snapshot.release();
    try request.release();
}

test "workload admission impossible continuation cannot wait on its own retained state" {
    var ledger = try Ledger.init(std.testing.allocator, testPolicy());
    defer ledger.deinit();
    var request = try ledger.admit(.general_read, 10);
    defer request.release() catch unreachable;
    var continuation = try ledger.acquire(.retained_state, &request, .{ .retained_bytes = 40 });
    defer continuation.release() catch unreachable;
    // The 60-byte maximum applies to the whole request, not just the new grant.
    try std.testing.expectError(error.ResourceRequestTooLarge, continuation.exchange(.runnable, .{ .runnable = 1, .retained_bytes = 51 }));
    try std.testing.expectEqual(@as(u64, 50), ledger.snapshot().total.retained_bytes);
}
