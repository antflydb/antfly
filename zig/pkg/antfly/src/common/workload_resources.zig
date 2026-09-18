// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Fixed resource envelopes and explicit ownership for the execution scheduler.
//! This ledger accounts credits; allocating memory still requires the storage
//! resource manager's reservation. It never executes an operator or waits while
//! holding a partial resource bundle. Owners must keep the ledger address stable.
const std = @import("std");

pub const Lane = enum { bounded_read, general_read, analytical_read, write, background, control, recovery, transition };
pub const lane_count = @typeInfo(Lane).@"enum".fields.len;
pub const Kind = enum { request, queue, runnable, retained_state, resume_queue, local_io, remote_attempt, recovery, transition_ticket };

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

    /// Subtract an already-validated owned or reserved sub-bundle.
    pub fn sub(a: Bundle, b: Bundle) Bundle {
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
    first_child: ?usize = null,
    previous_sibling: ?usize = null,
    next_sibling: ?usize = null,
    transition_ticket: ?Handle = null,
    transition_maximum: Bundle = .{},
    service_units: u64 = 0,
    executed: bool = false,
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

        /// Return unused memory credits only after actual storage is released.
        pub fn shrinkRetained(self: *@This(), bytes: u64) !void {
            self.owner.lock();
            defer self.owner.mutex.unlock();
            const owned = try self.owner.entry(self.handle, kind);
            if (bytes > owned.bundle.retained_bytes) return error.InvalidLease;
            owned.bundle.retained_bytes -= bytes;
            self.owner.uncharge(owned.lane, .{ .retained_bytes = bytes });
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
            if (kind == .request or kind == .runnable or kind == .queue or kind == .resume_queue or kind == .transition_ticket)
                @compileError("request execution cannot detach from its lifetime owner");
            const handle = try self.owner.detach(self.handle, kind, destination);
            self.handle.generation = 0;
            return .{ .owner = self.owner, .handle = handle };
        }

        /// Keep the current lane while detaching, atomically with a concurrent
        /// request demotion. A separate lane lookup could resurrect old charges.
        pub fn detachCurrent(self: *@This()) !@This() {
            if (kind == .request or kind == .runnable or kind == .queue or kind == .resume_queue or kind == .transition_ticket)
                @compileError("request execution cannot detach from its lifetime owner");
            const handle = try self.owner.detach(self.handle, kind, null);
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
/// Future transition capacity, not another physical-memory charge.
pub const TransitionTicket = Lease(.transition_ticket);
pub const max_service_units: u64 = 1_048_576;

pub const Ledger = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    policy: Policy,
    entries: []Entry,
    free_head: ?usize,
    used: [lane_count]Bundle = @splat(.{}),
    total: Bundle = .{},
    transition_reserved: Bundle = .{},

    pub fn init(allocator: std.mem.Allocator, policy: Policy) !Ledger {
        try policy.validate();
        const count = std.math.cast(usize, policy.total.handles) orelse return error.InvalidPolicy;
        const entries = try allocator.alloc(Entry, count);
        for (entries, 0..) |*slot, i| slot.* = .{ .next_free = if (i + 1 < count) i + 1 else null };
        return .{ .allocator = allocator, .policy = policy, .entries = entries, .free_head = if (count == 0) null else 0 };
    }

    pub fn deinit(self: *Ledger) void {
        std.debug.assert(self.total.handles == 0);
        std.debug.assert(self.transition_reserved.fits(.{}));
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
        return self.fitsWithTickets(use, self.transition_reserved);
    }

    fn fitsWithTickets(self: *Ledger, use: [lane_count]Bundle, tickets: Bundle) bool {
        if (!tickets.fits(.{})) {
            const future = use[@intFromEnum(Lane.transition)].add(tickets) catch return false;
            if (!future.fits(self.policy.lanes[@intFromEnum(Lane.transition)].floor)) return false;
        }
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

    fn unlinkChild(self: *Ledger, owned: *Entry) void {
        if (owned.parent) |parent| {
            const owner = self.entry(parent, .request) catch unreachable;
            if (owned.previous_sibling) |previous| self.entries[previous].next_sibling = owned.next_sibling else owner.first_child = owned.next_sibling;
            if (owned.next_sibling) |next| self.entries[next].previous_sibling = owned.previous_sibling;
            owner.children -= 1;
        }
        owned.previous_sibling = null;
        owned.next_sibling = null;
    }

    fn moveChildren(self: *Ledger, owner: *Entry, destination: Lane) void {
        var next = owner.first_child;
        while (next) |index| {
            self.entries[index].lane = destination;
            next = self.entries[index].next_sibling;
        }
    }

    fn stateFootprint(bundle: Bundle) Bundle {
        return .{ .handles = bundle.handles, .requests = bundle.requests, .retained_bytes = bundle.retained_bytes };
    }

    // Every allocation/growth checks the prepaid maximum before callers obtain
    // storage. Helpers may consume less at quiescence; including them here is
    // conservative and keeps the guaranteed demotion footprint bounded.
    fn checkTransitionGrowth(self: *Ledger, parent: Handle, replace: ?usize, replacement: Bundle) !void {
        const owner = try self.entry(parent, .request);
        const ticket_handle = owner.transition_ticket orelse return;
        const ticket = try self.entry(ticket_handle, .transition_ticket);
        var footprint = stateFootprint(if (replace == parent.index) replacement else owner.bundle);
        var next = owner.first_child;
        while (next) |index| {
            const child = self.entries[index];
            next = child.next_sibling;
            if (child.kind == .transition_ticket) continue;
            footprint = try footprint.add(stateFootprint(if (replace == index) replacement else child.bundle));
        }
        if (replace == null) footprint = try footprint.add(stateFootprint(replacement));
        if (!footprint.fits(ticket.transition_maximum)) return error.TransitionReservationExceeded;
    }

    /// Reserve before the first protected execution. The maximum includes the
    /// request and every future retained child/continuation, but not this ticket.
    /// Capacity is carved from the transition floor, not charged as live bytes.
    pub fn reserveTransition(self: *Ledger, request: *const RequestLease, maximum: Bundle) !TransitionTicket {
        if (maximum.requests != 1 or maximum.handles < 2 or !std.meta.eql(maximum, stateFootprint(maximum))) return error.InvalidLease;
        self.lock();
        defer self.mutex.unlock();
        if (request.owner != self) return error.InvalidLease;
        const owner = try self.entry(request.handle, .request);
        if (owner.lane != .bounded_read or owner.transition_ticket != null or owner.executed) return error.InvalidLease;
        var footprint = owner.bundle;
        var next = owner.first_child;
        while (next) |index| {
            const child = self.entries[index];
            next = child.next_sibling;
            if (child.kind != .retained_state) return error.ExecutionStillLive;
            footprint = try footprint.add(child.bundle);
        }
        if (!footprint.fits(maximum)) return error.TransitionReservationExceeded;
        const reserved = try self.transition_reserved.add(maximum);
        const future = try self.used[@intFromEnum(Lane.transition)].add(reserved);
        if (!future.fits(self.policy.lanes[@intFromEnum(Lane.transition)].floor)) return error.ResourceTemporarilyUnavailable;
        const ticket = try self.create(.transition_ticket, .bounded_read, .{}, request.handle);
        self.entries[ticket.handle.index].transition_maximum = maximum;
        owner.transition_ticket = ticket.handle;
        self.transition_reserved = reserved;
        return ticket;
    }

    pub fn recordService(self: *Ledger, request: *const RequestLease, units: u64) !u64 {
        self.lock();
        defer self.mutex.unlock();
        if (request.owner != self) return error.InvalidLease;
        const owner = try self.entry(request.handle, .request);
        owner.service_units = @min(max_service_units, owner.service_units +| units);
        return owner.service_units;
    }

    pub fn serviceDebt(self: *Ledger, request: *const RequestLease) !u64 {
        self.lock();
        defer self.mutex.unlock();
        if (request.owner != self) return error.InvalidLease;
        return (try self.entry(request.handle, .request)).service_units;
    }

    pub const Demotion = struct { state: RetainedStateLease, lane: Lane, service_units: u64 };

    /// One atomic boundary: no yielded-but-still-protected interval and no
    /// allocation. On failure all supplied handles remain usable. All helpers,
    /// I/O and remote attempts must have quiesced before calling this method.
    pub fn demoteAtYield(self: *Ledger, request: *const RequestLease, running: *RunnableLease, ticket: *TransitionTicket, retained_bytes: u64, measured_units: u64) !Demotion {
        self.lock();
        defer self.mutex.unlock();
        if (request.owner != self or running.owner != self or ticket.owner != self) return error.InvalidLease;
        const owner = try self.entry(request.handle, .request);
        const active = try self.entry(running.handle, .runnable);
        const reservation = try self.entry(ticket.handle, .transition_ticket);
        if (owner.lane != .bounded_read or !std.meta.eql(owner.transition_ticket, @as(?Handle, ticket.handle)) or
            !std.meta.eql(active.parent, @as(?Handle, request.handle)) or !std.meta.eql(reservation.parent, @as(?Handle, request.handle))) return error.InvalidLease;
        if (active.generation == std.math.maxInt(u64)) return error.GenerationExhausted;
        if (retained_bytes > active.bundle.retained_bytes) return error.TransitionReservationExceeded;
        const state_bundle: Bundle = .{ .handles = 1, .retained_bytes = retained_bytes };
        var before = owner.bundle;
        var after = owner.bundle;
        var next = owner.first_child;
        while (next) |index| {
            const child = self.entries[index];
            next = child.next_sibling;
            before = try before.add(child.bundle);
            if (index == ticket.handle.index) continue;
            if (index == running.handle.index) {
                after = try after.add(state_bundle);
            } else {
                if (child.kind != .retained_state) return error.ExecutionStillLive;
                after = try after.add(child.bundle);
            }
        }
        if (!after.fits(reservation.transition_maximum)) return error.TransitionReservationExceeded;
        const pending = self.transition_reserved.sub(reservation.transition_maximum);
        var proposed = self.used;
        proposed[@intFromEnum(Lane.bounded_read)] = proposed[@intFromEnum(Lane.bounded_read)].sub(before);
        proposed[@intFromEnum(Lane.general_read)] = try proposed[@intFromEnum(Lane.general_read)].add(after);
        var destination: Lane = .general_read;
        if (!self.fitsWithTickets(proposed, pending)) {
            destination = .transition;
            proposed = self.used;
            proposed[@intFromEnum(Lane.bounded_read)] = proposed[@intFromEnum(Lane.bounded_read)].sub(before);
            proposed[@intFromEnum(Lane.transition)] = try proposed[@intFromEnum(Lane.transition)].add(after);
            // The ticket guarantees this complete destination bundle, including
            // its metadata. Capacity cannot disappear between reserve and yield.
            if (!self.fitsWithTickets(proposed, pending)) return error.ResourceTemporarilyUnavailable;
        }
        self.unlinkChild(reservation);
        reservation.live = false;
        if (reservation.generation != std.math.maxInt(u64)) {
            reservation.next_free = self.free_head;
            self.free_head = ticket.handle.index;
        }
        owner.transition_ticket = null;
        owner.lane = destination;
        owner.service_units = @min(max_service_units, owner.service_units +| measured_units);
        self.moveChildren(owner, destination);
        active.kind = .retained_state;
        active.bundle = state_bundle;
        active.generation += 1;
        self.used = proposed;
        self.total = try self.total.sub(before).add(after);
        self.transition_reserved = pending;
        running.handle.generation = 0;
        ticket.handle.generation = 0;
        return .{ .state = .{ .owner = self, .handle = .{ .index = running.handle.index, .generation = active.generation } }, .lane = destination, .service_units = owner.service_units };
    }

    /// Transfer parked state only together with its complete general-lane
    /// execution grant. A failed grant cannot strand it between accounting lanes.
    pub fn resumeDemoted(self: *Ledger, comptime kind: Kind, request: *const RequestLease, state: *Lease(kind), minimum: Bundle) !RunnableLease {
        if (kind != .retained_state and kind != .resume_queue) @compileError("invalid continuation kind");
        try validateBundle(.runnable, minimum);
        if (minimum.handles != 0) return error.InvalidLease;
        self.lock();
        defer self.mutex.unlock();
        if (request.owner != self or state.owner != self) return error.InvalidLease;
        const owner = try self.entry(request.handle, .request);
        const held = try self.entry(state.handle, kind);
        if ((owner.lane != .transition and owner.lane != .general_read) or !std.meta.eql(held.parent, @as(?Handle, request.handle))) return error.InvalidLease;
        if (held.generation == std.math.maxInt(u64)) return error.GenerationExhausted;
        if (minimum.retained_bytes < held.bundle.retained_bytes) return error.RetainedStateStillLive;
        var bundle = minimum;
        bundle.handles = 1;
        var before = owner.bundle;
        var after = owner.bundle;
        var next = owner.first_child;
        while (next) |index| {
            const child = self.entries[index];
            next = child.next_sibling;
            if (index != state.handle.index and child.kind != .retained_state) return error.ExecutionStillLive;
            before = try before.add(child.bundle);
            after = try after.add(if (index == state.handle.index) bundle else child.bundle);
        }
        if (!after.fits(self.policy.maximumFor(.general_read))) return error.ResourceRequestTooLarge;
        var proposed = self.used;
        proposed[@intFromEnum(owner.lane)] = proposed[@intFromEnum(owner.lane)].sub(before);
        proposed[@intFromEnum(Lane.general_read)] = try proposed[@intFromEnum(Lane.general_read)].add(after);
        if (!self.fits(proposed)) return error.ResourceTemporarilyUnavailable;
        owner.lane = .general_read;
        self.moveChildren(owner, .general_read);
        held.kind = .runnable;
        held.bundle = bundle;
        held.generation += 1;
        self.used = proposed;
        self.total = try self.total.sub(before).add(after);
        const handle: Handle = .{ .index = state.handle.index, .generation = held.generation };
        state.handle.generation = 0;
        return .{ .owner = self, .handle = handle };
    }

    fn create(self: *Ledger, comptime kind: Kind, lane: Lane, requested: Bundle, parent: ?Handle) !Lease(kind) {
        // One fixed metadata entry is part of every grant, including cleanup
        // and helper grants. Bulk cannot consume protected metadata capacity.
        if (requested.handles != 0) return error.InvalidLease;
        try validateBundle(kind, requested);
        var bundle = requested;
        bundle.handles = 1;
        const index = self.free_head orelse return error.ResourceTemporarilyUnavailable;
        if (parent) |handle| if (kind != .transition_ticket) try self.checkTransitionGrowth(handle, null, bundle);
        try self.charge(lane, bundle);
        const slot = &self.entries[index];
        self.free_head = slot.next_free;
        const generation = slot.generation + 1;
        slot.* = .{ .live = true, .kind = kind, .lane = lane, .bundle = bundle, .parent = parent, .generation = generation };
        if (parent) |handle| {
            const owner = try self.entry(handle, .request);
            slot.next_sibling = owner.first_child;
            if (owner.first_child) |first| self.entries[first].previous_sibling = index;
            owner.first_child = index;
            owner.children += 1;
            if (kind == .runnable) owner.executed = true;
        }
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
        if (kind == .request or kind == .transition_ticket) @compileError("use explicit admission/reservation for lifetime owners");
        self.lock();
        defer self.mutex.unlock();
        if (parent.owner != self) return error.InvalidLease;
        const owner = try self.entry(parent.handle, .request);
        if (owner.lane == .transition and kind != .retained_state) return error.TransitionCannotExecute;
        var required = bundle;
        required.handles = 1;
        required.retained_bytes = std.math.add(u64, required.retained_bytes, owner.bundle.retained_bytes) catch return error.ResourceRequestTooLarge;
        var child_index = owner.first_child;
        while (child_index) |index| {
            const child = self.entries[index];
            child_index = child.next_sibling;
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
        if (kind == .transition_ticket) {
            self.transition_reserved = self.transition_reserved.sub(owned.transition_maximum);
            (try self.entry(owned.parent.?, .request)).transition_ticket = null;
        }
        self.unlinkChild(owned);
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
        if (kind == .request) {
            try self.checkTransitionGrowth(handle, handle.index, next);
        } else if (owned.parent) |parent| try self.checkTransitionGrowth(parent, handle.index, next);
        try self.charge(owned.lane, additional);
        owned.bundle = next;
    }

    fn detach(self: *Ledger, handle: Handle, kind: Kind, requested_destination: ?Lane) !Handle {
        self.lock();
        defer self.mutex.unlock();
        const owned = try self.entry(handle, kind);
        const destination = requested_destination orelse owned.lane;
        if (owned.generation == std.math.maxInt(u64)) return error.GenerationExhausted;
        var proposed = self.used;
        proposed[@intFromEnum(owned.lane)] = proposed[@intFromEnum(owned.lane)].sub(owned.bundle);
        proposed[@intFromEnum(destination)] = try proposed[@intFromEnum(destination)].add(owned.bundle);
        if (!self.fits(proposed)) return error.ResourceTemporarilyUnavailable;
        self.unlinkChild(owned);
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
            var child_index = (try self.entry(parent, .request)).first_child;
            while (child_index) |i| {
                const other = self.entries[i];
                child_index = other.next_sibling;
                if (i != handle.index and other.live and (other.kind == .retained_state or other.kind == .resume_queue) and other.parent != null and std.meta.eql(other.parent.?, parent))
                    required.retained_bytes = std.math.add(u64, required.retained_bytes, other.bundle.retained_bytes) catch return error.ResourceRequestTooLarge;
            }
        }
        if (owned.parent) |parent| try self.checkTransitionGrowth(parent, handle.index, bundle);
        if (!bundle.fits(owned.bundle) and !required.fits(self.policy.maximumFor(owned.lane))) return error.ResourceRequestTooLarge;
        var proposed = self.used;
        proposed[@intFromEnum(owned.lane)] = try proposed[@intFromEnum(owned.lane)].sub(owned.bundle).add(bundle);
        if (!self.fits(proposed)) return error.ResourceTemporarilyUnavailable;
        self.total = try self.total.sub(owned.bundle).add(bundle);
        self.used = proposed;
        owned.bundle = bundle;
        owned.kind = next;
        if (next == .runnable) if (owned.parent) |parent| {
            (try self.entry(parent, .request)).executed = true;
        };
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
        if (owner.transition_ticket != null) return error.TransitionReservationStillLive;
        var child_index = owner.first_child;
        while (child_index) |index| {
            const child = self.entries[index];
            child_index = child.next_sibling;
            if (child.kind == .runnable or child.kind == .resume_queue or child.kind == .queue) return error.ExecutionStillLive;
            bundle = try bundle.add(child.bundle);
        }
        var proposed = self.used;
        proposed[@intFromEnum(owner.lane)] = proposed[@intFromEnum(owner.lane)].sub(bundle);
        proposed[@intFromEnum(destination)] = try proposed[@intFromEnum(destination)].add(bundle);
        if (!self.fits(proposed)) return error.ResourceTemporarilyUnavailable;
        self.moveChildren(owner, destination);
        owner.lane = destination;
        self.used = proposed;
    }

    /// Running ownership survives reductions; only future growth/grants stop.
    pub fn reconfigure(self: *Ledger, policy: Policy) !void {
        try policy.validate();
        if (policy.total.handles > self.entries.len) return error.InvalidPolicy;
        self.lock();
        defer self.mutex.unlock();
        if (!self.transition_reserved.fits(.{})) {
            const guaranteed = try self.used[@intFromEnum(Lane.transition)].add(self.transition_reserved);
            if (!guaranteed.fits(policy.lanes[@intFromEnum(Lane.transition)].floor)) return error.TransitionReservationStillLive;
        }
        self.policy = policy;
    }

    pub const Stats = struct { total: Bundle, lanes: [lane_count]Bundle, policy: Policy, transition_reserved: Bundle };
    pub fn snapshot(self: *Ledger) Stats {
        self.lock();
        defer self.mutex.unlock();
        return .{ .total = self.total, .lanes = self.used, .policy = self.policy, .transition_reserved = self.transition_reserved };
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
        .transition_ticket => if (bundle.retained_bytes != 0) return error.InvalidLease,
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

test "workload admission transition tickets reserve whole footprints without duplicate live bytes" {
    var policy = testPolicy();
    policy.lanes[@intFromEnum(Lane.general_read)].ceiling.retained_bytes = 50;
    var ledger = try Ledger.init(std.testing.allocator, policy);
    defer ledger.deinit();
    var blocker = try ledger.admit(.general_read, 50);
    var request = try ledger.admit(.bounded_read, 5);
    var ticket = try ledger.reserveTransition(&request, .{ .handles = 3, .requests = 1, .retained_bytes = 10 });
    var stale_ticket = ticket;
    try std.testing.expectEqual(@as(u64, 55), ledger.snapshot().total.retained_bytes);
    try std.testing.expectEqual(@as(u64, 10), ledger.snapshot().transition_reserved.retained_bytes);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ledger.admit(.transition, 1));
    var reduced = policy;
    reduced.lanes[@intFromEnum(Lane.transition)].floor.requests = 0;
    try std.testing.expectError(error.TransitionReservationStillLive, ledger.reconfigure(reduced));
    var running = try ledger.acquire(.runnable, &request, .{ .runnable = 1, .retained_bytes = 5 });
    try std.testing.expectError(error.TransitionReservationExceeded, running.grow(.{ .retained_bytes = 1 }));
    var io = try ledger.acquire(.local_io, &request, .{ .io = 1 });
    const live = ledger.snapshot();
    try std.testing.expectError(error.ExecutionStillLive, ledger.demoteAtYield(&request, &running, &ticket, 5, 7));
    try std.testing.expectEqualDeep(live, ledger.snapshot());
    try io.release();
    const before = ledger.snapshot().total;
    var demoted = try ledger.demoteAtYield(&request, &running, &ticket, 5, 7);
    try std.testing.expectEqual(Lane.transition, demoted.lane);
    try std.testing.expectEqual(@as(u64, 7), demoted.service_units);
    const after = ledger.snapshot();
    try std.testing.expectEqual(before.retained_bytes, after.total.retained_bytes);
    try std.testing.expectEqual(before.requests, after.total.requests);
    try std.testing.expectEqual(before.handles - 1, after.total.handles);
    try std.testing.expectEqual(@as(u64, 0), after.total.runnable);
    try std.testing.expectEqualDeep(Bundle{}, after.transition_reserved);
    try stale_ticket.release();
    try std.testing.expectEqualDeep(after, ledger.snapshot());
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ledger.resumeDemoted(.retained_state, &request, &demoted.state, .{ .runnable = 1, .retained_bytes = 5 }));
    try std.testing.expectEqual(Lane.transition, try ledger.requestLane(&request));
    try std.testing.expectError(error.TransitionCannotExecute, ledger.acquire(.local_io, &request, .{ .io = 1 }));
    try blocker.release();
    var resumed = try ledger.resumeDemoted(.retained_state, &request, &demoted.state, .{ .runnable = 1, .retained_bytes = 5 });
    try std.testing.expectEqual(Lane.general_read, try ledger.requestLane(&request));
    try std.testing.expectEqual(@as(u64, 7), try ledger.serviceDebt(&request));
    try resumed.release();
    try request.release();
}

test "workload admission transition cancellation releases reservation and rejects stale reused tickets" {
    var ledger = try Ledger.init(std.testing.allocator, testPolicy());
    defer ledger.deinit();
    var request = try ledger.admit(.bounded_read, 5);
    var ticket = try ledger.reserveTransition(&request, .{ .handles = 2, .requests = 1, .retained_bytes = 10 });
    var stale = ticket;
    try std.testing.expectError(error.OwnedWorkStillLive, request.release());
    try ticket.release();
    var next = try ledger.reserveTransition(&request, .{ .handles = 2, .requests = 1, .retained_bytes = 10 });
    try stale.release();
    try std.testing.expectEqual(@as(u64, 1), ledger.snapshot().transition_reserved.requests);
    var running = try ledger.acquire(.runnable, &request, .{ .runnable = 1, .retained_bytes = 5 });
    try std.testing.expectError(error.LeaseRetired, ledger.demoteAtYield(&request, &running, &stale, 5, 1));
    var result = try ledger.demoteAtYield(&request, &running, &next, 5, 1);
    try std.testing.expectEqual(Lane.general_read, result.lane);
    try result.state.release();
    try request.release();
    try std.testing.expectEqualDeep(Bundle{}, ledger.snapshot().total);
}

test "workload admission multiple transition reservations cannot steal parked or future capacity" {
    var policy = testPolicy();
    policy.lanes[@intFromEnum(Lane.transition)].floor = .{ .handles = 4, .requests = 2, .retained_bytes = 20 };
    var ledger = try Ledger.init(std.testing.allocator, policy);
    defer ledger.deinit();
    var first = try ledger.admit(.bounded_read, 5);
    var second = try ledger.admit(.bounded_read, 5);
    var third = try ledger.admit(.bounded_read, 5);
    var one = try ledger.reserveTransition(&first, .{ .handles = 2, .requests = 1, .retained_bytes = 10 });
    var two = try ledger.reserveTransition(&second, .{ .handles = 2, .requests = 1, .retained_bytes = 10 });
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ledger.reserveTransition(&third, .{ .handles = 2, .requests = 1, .retained_bytes = 5 }));
    try std.testing.expectEqual(@as(u64, 15), ledger.snapshot().total.retained_bytes);
    try one.release();
    var parked = try ledger.admit(.transition, 10);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, ledger.reserveTransition(&third, .{ .handles = 2, .requests = 1, .retained_bytes = 5 }));
    try two.release();
    try parked.release();
    var executed = try ledger.acquire(.runnable, &third, .{ .runnable = 1 });
    try executed.release();
    try std.testing.expectError(error.InvalidLease, ledger.reserveTransition(&third, .{ .handles = 2, .requests = 1, .retained_bytes = 10 }));
    try first.release();
    try second.release();
    try third.release();
}

test "workload admission concurrent ticket cancellation and demotion conserve one owner" {
    var ledger = try Ledger.init(std.testing.allocator, testPolicy());
    defer ledger.deinit();
    const Race = struct {
        ledger: *Ledger,
        request: RequestLease,
        running: RunnableLease,
        ticket: TransitionTicket,
        cancellation_ticket: TransitionTicket,
        start: std.atomic.Value(bool) = .init(false),
        result: ?Ledger.Demotion = null,
        failure: ?anyerror = null,
        fn demote(self: *@This()) void {
            while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
            self.result = self.ledger.demoteAtYield(&self.request, &self.running, &self.ticket, 5, 1) catch |err| {
                self.failure = err;
                return;
            };
        }
        fn cancel(self: *@This()) void {
            while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
            self.cancellation_ticket.release() catch unreachable;
        }
    };
    for (0..32) |_| {
        var request = try ledger.admit(.bounded_read, 5);
        const ticket = try ledger.reserveTransition(&request, .{ .handles = 2, .requests = 1, .retained_bytes = 10 });
        var running = try ledger.acquire(.runnable, &request, .{ .runnable = 1, .retained_bytes = 5 });
        var race: Race = .{ .ledger = &ledger, .request = request, .running = running, .ticket = ticket, .cancellation_ticket = ticket };
        const demoter = try std.Thread.spawn(.{}, Race.demote, .{&race});
        const canceller = try std.Thread.spawn(.{}, Race.cancel, .{&race});
        race.start.store(true, .release);
        demoter.join();
        canceller.join();
        try std.testing.expectEqual(@as(u64, 10), ledger.snapshot().total.retained_bytes);
        if (race.result) |*result| {
            try std.testing.expect(race.failure == null);
            try result.state.release();
        } else try std.testing.expectEqual(error.LeaseRetired, race.failure.?);
        try running.release();
        try request.release();
        try std.testing.expectEqualDeep(Bundle{}, ledger.snapshot().total);
        try std.testing.expectEqualDeep(Bundle{}, ledger.snapshot().transition_reserved);
    }
}
