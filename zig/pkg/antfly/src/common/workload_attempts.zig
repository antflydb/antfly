// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Coordinator-side remote ownership. Membership supplies a durable, fenced
//! generation and authenticates retirement evidence. A restarted coordinator
//! cannot dispatch to a destination until the prior generation is known fenced
//! and quiescent there. No timer, disconnect, or health check retires a charge.
//! This mechanism requires a worker protocol adapter before production use.
const std = @import("std");
const resources = @import("workload_resources.zig");

pub const AttemptId = struct {
    coordinator: u64,
    generation: u64,
    sequence: u64,
    operation: u128,
    destination: u64,
    worker_incarnation: u64,
    worker_namespace: u128 = 0,
};

/// Versioned remaining-duration contract. Neither endpoint serializes a local
/// monotonic epoch. Network delay means receiver expiry can be later than the
/// sender deadline; only acknowledged quiescence can reconcile that uncertainty.
pub const Envelope = struct {
    version: u16 = 1,
    attempt: AttemptId,
    remaining_ns: u64,

    pub fn receiveDeadline(self: Envelope, received_ns: u64, worker_max_ns: u64) !u64 {
        if (self.version != 1) return error.UnsupportedAttemptProtocol;
        if (self.remaining_ns == 0 or worker_max_ns == 0) return error.DeadlineExceeded;
        return received_ns +| @min(self.remaining_ns, worker_max_ns);
    }
};

pub const DestinationPolicy = struct { id: u64, max_attempts: u32, max_bytes: u64 };
pub const FenceEvidence = struct {
    destination: u64,
    worker_incarnation: u64,
    worker_namespace: u128 = 0,
    fenced_through: u64,
    quiesced_through: u64,
};

pub const Attempts = struct {
    allocator: std.mem.Allocator,
    ledger: *resources.Ledger,
    mutex: std.atomic.Mutex = .unlocked,
    coordinator: u64,
    generation: u64,
    sequence: u64 = 0,
    destinations: []Destination,
    entries: []Entry,
    total_bytes: u64 = 0,
    max_bytes: u64,

    const Destination = struct {
        policy: DestinationPolicy,
        incarnation: u64 = 0,
        namespace: u128 = 0,
        ready: bool = false,
        fenced_through: u64 = 0,
        active: u32 = 0,
        bytes: u64 = 0,
    };
    const Entry = struct {
        live: bool = false,
        id: AttemptId = undefined,
        lease: resources.RemoteAttemptLease = undefined,
        bytes: u64 = 0,
        started_ns: u64 = 0,
        deadline_ns: u64 = 0,
        uncertain: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator, ledger: *resources.Ledger, coordinator: u64, generation: u64, policies: []const DestinationPolicy, max_attempts: usize, max_bytes: u64) !Attempts {
        if (coordinator == 0 or generation == 0 or max_attempts == 0 or max_bytes < @sizeOf(Entry)) return error.InvalidPolicy;
        const destinations = try allocator.alloc(Destination, policies.len);
        errdefer allocator.free(destinations);
        for (policies, 0..) |policy, i| {
            if (policy.id == 0 or policy.max_attempts == 0 or policy.max_bytes < @sizeOf(Entry)) return error.InvalidPolicy;
            for (policies[0..i]) |prior| if (prior.id == policy.id) return error.InvalidPolicy;
            destinations[i] = .{ .policy = policy };
        }
        const entries = try allocator.alloc(Entry, max_attempts);
        @memset(entries, .{});
        return .{ .allocator = allocator, .ledger = ledger, .coordinator = coordinator, .generation = generation, .destinations = destinations, .entries = entries, .max_bytes = max_bytes };
    }

    pub fn deinit(self: *Attempts) void {
        // A process teardown must first durably arrange generation fencing for
        // restart; ordinary deinit cannot silently discard uncertain ownership.
        for (self.entries) |entry| std.debug.assert(!entry.live);
        self.allocator.free(self.entries);
        self.allocator.free(self.destinations);
    }

    fn lock(self: *Attempts) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn destination(self: *Attempts, id: u64) !*Destination {
        for (self.destinations) |*value| if (value.policy.id == id) return value;
        return error.UnknownDestination;
    }

    /// Called only with authenticated membership/worker evidence. A first-ever
    /// generation also needs evidence: an empty local map proves nothing about
    /// a previous owner at a worker. Unsupported peers remain unavailable here.
    pub fn openDestination(self: *Attempts, evidence: FenceEvidence) !void {
        self.lock();
        defer self.mutex.unlock();
        const target = try self.destination(evidence.destination);
        if (target.active != 0) return error.AttemptsStillLive;
        if (target.fenced_through >= self.generation or evidence.worker_incarnation == 0 or evidence.fenced_through < self.generation - 1 or
            evidence.quiesced_through < self.generation - 1 or evidence.fenced_through >= self.generation)
            return error.FencingRequired;
        target.incarnation = evidence.worker_incarnation;
        target.namespace = evidence.worker_namespace;
        target.fenced_through = @max(target.fenced_through, evidence.fenced_through);
        target.ready = true;
    }

    /// Reserve uncertainty storage before sending. Distinct retries need a new
    /// identity and another reservation; retransmission keeps this same envelope
    /// and requires worker deduplication by the full AttemptId.
    pub fn begin(self: *Attempts, request: *const resources.RequestLease, destination_id: u64, operation: u128, retained_bytes: u64, now_ns: u64, deadline_ns: u64) !Envelope {
        if (now_ns >= deadline_ns) return error.DeadlineExceeded;
        self.lock();
        defer self.mutex.unlock();
        const target = try self.destination(destination_id);
        if (!target.ready) return error.FencingRequired;
        const bytes = std.math.add(u64, retained_bytes, @sizeOf(Entry)) catch return error.ResourceRequestTooLarge;
        if (bytes > target.policy.max_bytes or bytes > self.max_bytes) return error.ResourceRequestTooLarge;
        if (target.active >= target.policy.max_attempts or bytes > target.policy.max_bytes -| target.bytes or
            bytes > self.max_bytes -| self.total_bytes) return error.AttemptCapacityExhausted;
        const slot = for (self.entries) |*entry| {
            if (!entry.live) break entry;
        } else return error.AttemptCapacityExhausted;
        if (self.sequence == std.math.maxInt(u64)) return error.GenerationExhausted;
        const lease = try self.ledger.acquire(.remote_attempt, request, .{ .remote_attempts = 1, .retained_bytes = bytes });
        self.sequence += 1;
        const id: AttemptId = .{ .coordinator = self.coordinator, .generation = self.generation, .sequence = self.sequence, .operation = operation, .destination = destination_id, .worker_incarnation = target.incarnation, .worker_namespace = target.namespace };
        slot.* = .{ .live = true, .id = id, .lease = lease, .bytes = bytes, .started_ns = now_ns, .deadline_ns = deadline_ns };
        target.active += 1;
        target.bytes += bytes;
        self.total_bytes += bytes;
        return .{ .attempt = id, .remaining_ns = deadline_ns - now_ns };
    }

    /// Rebuild the wire budget at actual dispatch, including retransmission.
    /// A delayed sender must not reuse the duration captured by `begin`.
    pub fn dispatchEnvelope(self: *Attempts, id: AttemptId, now_ns: u64) !Envelope {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |entry| {
            if (!entry.live or !std.meta.eql(entry.id, id)) continue;
            if (now_ns >= entry.deadline_ns) return error.DeadlineExceeded;
            if (!(try self.destination(id.destination)).ready) return error.FencingRequired;
            return .{ .attempt = id, .remaining_ns = entry.deadline_ns - now_ns };
        }
        return error.AttemptRetired;
    }

    /// Transport loss is not remote retirement. This transfers the already
    /// reserved reconciliation owner and lets the local parent retire safely.
    pub fn markUncertain(self: *Attempts, id: AttemptId) !bool {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |*entry| {
            if (!entry.live or !std.meta.eql(entry.id, id)) continue;
            if (entry.uncertain) return true;
            entry.lease = try entry.lease.detachCurrent();
            entry.uncertain = true;
            return true;
        }
        return false;
    }

    fn retire(self: *Attempts, entry: *Entry) void {
        entry.lease.release() catch unreachable;
        const target = self.destination(entry.id.destination) catch unreachable;
        target.active -= 1;
        target.bytes -= entry.bytes;
        self.total_bytes -= entry.bytes;
        entry.live = false;
    }

    /// A terminal response must certify underlying quiescence or an accounted
    /// recovery handoff. An HTTP error alone is not such evidence.
    pub fn terminal(self: *Attempts, id: AttemptId) bool {
        self.lock();
        defer self.mutex.unlock();
        for (self.entries) |*entry| {
            if (entry.live and std.meta.eql(entry.id, id)) {
                self.retire(entry);
                return true;
            }
        }
        return false; // duplicate/stale responses cannot recreate ownership
    }

    pub fn reconcileFence(self: *Attempts, evidence: FenceEvidence) !void {
        self.lock();
        defer self.mutex.unlock();
        const target = try self.destination(evidence.destination);
        if (evidence.worker_incarnation != target.incarnation or evidence.worker_namespace != target.namespace or evidence.fenced_through < self.generation or evidence.quiesced_through < self.generation)
            return error.FencingRequired;
        target.ready = false; // delayed dispatch of this generation is rejected
        target.fenced_through = @max(target.fenced_through, evidence.fenced_through);
        for (self.entries) |*entry| if (entry.live and entry.id.destination == evidence.destination) self.retire(entry);
    }

    pub const Stats = struct { active: usize = 0, uncertain: usize = 0, bytes: u64 = 0, oldest_uncertain_ns: u64 = 0, fenced_destinations: usize = 0 };
    pub fn stats(self: *Attempts, now_ns: u64) Stats {
        self.lock();
        defer self.mutex.unlock();
        var result: Stats = .{ .bytes = self.total_bytes };
        for (self.entries) |entry| {
            if (!entry.live) continue;
            result.active += 1;
            if (entry.uncertain) {
                result.uncertain += 1;
                result.oldest_uncertain_ns = @max(result.oldest_uncertain_ns, now_ns -| entry.started_ns);
            }
        }
        for (self.destinations) |target| if (!target.ready) {
            result.fenced_destinations += 1;
        };
        return result;
    }
};

test "workload admission remote uncertainty survives deadlines and isolates unhealthy destinations" {
    const total: resources.Bundle = .{ .handles = 8, .requests = 2, .retained_bytes = 4096, .remote_attempts = 4 };
    var ledger = try resources.Ledger.init(std.testing.allocator, .{ .total = total, .lanes = @splat(.{ .ceiling = total }) });
    defer ledger.deinit();
    var attempts = try Attempts.init(std.testing.allocator, &ledger, 1, 2, &.{ .{ .id = 7, .max_attempts = 1, .max_bytes = 1024 }, .{ .id = 8, .max_attempts = 2, .max_bytes = 2048 } }, 3, 3072);
    defer attempts.deinit();
    var request = try ledger.admit(.general_read, 0);
    try std.testing.expectError(error.FencingRequired, attempts.begin(&request, 7, 100, 0, 0, 100));
    for ([_]u64{ 7, 8 }) |id| try attempts.openDestination(.{ .destination = id, .worker_incarnation = 3, .fenced_through = 1, .quiesced_through = 1 });
    const first = try attempts.begin(&request, 7, 100, 0, 0, 100);
    try std.testing.expectEqual(@as(u64, 10), (try attempts.dispatchEnvelope(first.attempt, 90)).remaining_ns);
    try std.testing.expectError(error.DeadlineExceeded, attempts.dispatchEnvelope(first.attempt, 100));
    try std.testing.expectError(error.OwnedWorkStillLive, request.release());
    try std.testing.expect(try attempts.markUncertain(first.attempt));
    try request.release();
    try std.testing.expectEqual(@as(usize, 1), attempts.stats(10000).uncertain);
    var next = try ledger.admit(.general_read, 0);
    defer next.release() catch unreachable;
    try std.testing.expectError(error.AttemptCapacityExhausted, attempts.begin(&next, 7, 100, 0, 10000, 10100));
    const healthy = try attempts.begin(&next, 8, 100, 0, 10000, 10100);
    try std.testing.expect(attempts.terminal(healthy.attempt));
    try std.testing.expect(!attempts.terminal(healthy.attempt));
    try std.testing.expectError(error.FencingRequired, attempts.reconcileFence(.{ .destination = 7, .worker_incarnation = 4, .fenced_through = 2, .quiesced_through = 2 }));
    try attempts.reconcileFence(.{ .destination = 7, .worker_incarnation = 3, .fenced_through = 2, .quiesced_through = 2 });
    try std.testing.expect(!attempts.terminal(first.attempt));
    try std.testing.expectEqual(@as(usize, 0), attempts.stats(20000).active);
    try std.testing.expectError(error.FencingRequired, attempts.begin(&next, 7, 100, 0, 20000, 20100));
    try std.testing.expectError(error.FencingRequired, attempts.openDestination(.{ .destination = 7, .worker_incarnation = 3, .fenced_through = 1, .quiesced_through = 1 }));
}

test "workload admission wire budgets are durations and unsupported peers cannot extend them" {
    var envelope: Envelope = .{ .attempt = undefined, .remaining_ns = 100 };
    try std.testing.expectEqual(@as(u64, 5050), try envelope.receiveDeadline(5000, 50));
    envelope.version = 2;
    try std.testing.expectError(error.UnsupportedAttemptProtocol, envelope.receiveDeadline(5000, 50));
    envelope.version = 1;
    envelope.remaining_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, envelope.receiveDeadline(5000, 50));
}
