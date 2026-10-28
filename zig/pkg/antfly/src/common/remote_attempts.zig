// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Allocation-free remote ownership state machines over caller-owned storage.
//! Transport loss is not a terminal outcome. Fence acknowledgements must come
//! from authenticated workers after durable fencing and physical quiescence.
//! Wire authentication, membership and persistence belong to the integrating
//! owner; these state machines alone do not establish distributed safety.
const std = @import("std");

pub const Id = struct {
    coordinator: u128,
    generation: u64,
    operation: u128,
    sequence: u64,

    pub fn eql(a: Id, b: Id) bool {
        return a.coordinator == b.coordinator and a.generation == b.generation and a.operation == b.operation and a.sequence == b.sequence;
    }
};
pub const FenceAck = struct {
    coordinator: u128,
    generation: u64,
    worker_incarnation: u128,
};
pub const State = enum { reserved, dispatched, uncertain };
/// Move-only physical parent reference. Release runs outside the ledger lock
/// after trustworthy retirement, while the slot and budget remain charged.
pub const Retention = struct {
    context: *anyopaque,
    index: usize,
    id: Id,
    release_fn: *const fn (*anyopaque, usize, Id) void,
    cancellation: ?*const std.atomic.Value(bool) = null,
    secondary_cancellation: ?*const std.atomic.Value(bool) = null,
    deadline_ns: ?u64 = null,

    pub fn check(self: Retention, now_ns: ?u64) !void {
        if (now_ns) |now| if (self.deadline_ns) |deadline| if (now >= deadline) return error.DeadlineExceeded;
        if (self.cancellation) |signal| if (signal.load(.acquire)) return error.Canceled;
        if (self.secondary_cancellation) |signal| if (signal.load(.acquire)) return error.Canceled;
    }

    pub fn release(self: Retention) void {
        self.release_fn(self.context, self.index, self.id);
    }
};
pub const Attempt = struct {
    id: Id,
    destination: usize,
    bytes: usize,
    state: State = .reserved,
    abandoned: bool = false,
    parent: ?Retention = null,
    retiring: bool = false,
};
pub const Destination = struct {
    worker_incarnation: u128,
    max_attempts: usize,
    max_bytes: usize,
    attempts: usize = 0,
    bytes: usize = 0,
    ready: bool = false,
};
pub const Snapshot = struct { attempts: usize, bytes: usize, uncertain: usize, startup_fences_remaining: usize, generation_fenced: bool = false, observed_generation: u64 = 0, queued: usize = 0, queued_bytes: usize = 0 };

pub const Coordinator = struct {
    coordinator: u128,
    generation: u64,
    next_sequence: u64 = 1,
    reconciler_bound: bool = false,
    generation_fenced: bool = false,
    observed_generation: u64 = 0,
    slots: []?Attempt,
    destinations: []Destination,
    max_bytes: usize,
    attempts: usize = 0,
    bytes: usize = 0,
    startup_fences_remaining: usize,
    mutex: std.atomic.Mutex = .unlocked,
    waiting: ?*Waiter = null,
    waiting_count: usize = 0,
    waiting_bytes: usize = 0,

    /// Synchronous intrusive wait metadata. The borrower must stay alive until
    /// endWait; no ID, destination credit or publication exists while queued.
    pub const Waiter = struct {
        owner: ?*Coordinator = null,
        next: ?*Waiter = null,
        destination: usize = 0,
        operation: u128 = 0,
        generation: u64 = 0,
        bytes: usize = 0,
    };
    const maximum_waiters = 64;

    /// Fresh process state is closed until EVERY configured destination fences
    /// the previous generation. Opening just one would forget other old work
    /// from the aggregate budget. Membership must include all prior destinations.
    pub fn init(coordinator: u128, generation: u64, slots: []?Attempt, destinations: []Destination, max_bytes: usize) !Coordinator {
        if (coordinator == 0 or generation == 0 or slots.len == 0 or destinations.len == 0 or max_bytes == 0) return error.InvalidRemoteAttemptConfig;
        for (destinations) |destination| if (destination.worker_incarnation == 0 or destination.max_attempts == 0 or destination.max_bytes == 0) return error.InvalidRemoteAttemptConfig;
        for (destinations, 0..) |destination, index| {
            for (destinations[0..index]) |prior| if (prior.worker_incarnation == destination.worker_incarnation) return error.InvalidRemoteAttemptConfig;
        }
        @memset(slots, null);
        for (destinations) |*destination| {
            destination.attempts = 0;
            destination.bytes = 0;
            destination.ready = false;
        }
        return .{ .coordinator = coordinator, .generation = generation, .slots = slots, .destinations = destinations, .max_bytes = max_bytes, .startup_fences_remaining = destinations.len };
    }

    fn lock(self: *Coordinator) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn find(self: *Coordinator, id: Id) ?*?Attempt {
        for (self.slots) |*slot| if (slot.*) |attempt| if (attempt.id.eql(id)) return slot;
        return null;
    }
    fn retire(self: *Coordinator, slot: *?Attempt) void {
        if (slot.*.?.retiring) return;
        if (slot.*.?.parent != null) {
            slot.*.?.retiring = true;
            return;
        }
        self.finishRetirement(slot);
    }
    fn finishRetirement(self: *Coordinator, slot: *?Attempt) void {
        const attempt = slot.*.?;
        const destination = &self.destinations[attempt.destination];
        destination.attempts -= 1;
        destination.bytes -= attempt.bytes;
        self.attempts -= 1;
        self.bytes -= attempt.bytes;
        slot.* = null;
    }
    fn drainRetired(self: *Coordinator) void {
        while (true) {
            self.lock();
            const selected = for (self.slots, 0..) |*slot, index| {
                if (slot.*) |*attempt| if (attempt.retiring and attempt.parent != null) {
                    const parent = attempt.parent.?;
                    attempt.parent = null;
                    break .{ .index = index, .id = attempt.id, .parent = parent };
                };
            } else {
                self.mutex.unlock();
                return;
            };
            self.mutex.unlock();
            selected.parent.release();
            self.lock();
            const slot = &self.slots[selected.index];
            std.debug.assert(slot.* != null and slot.*.?.retiring and slot.*.?.id.eql(selected.id));
            self.finishRetirement(slot);
            self.mutex.unlock();
        }
    }
    /// Transfer before publication. Failure leaves ownership with the caller;
    /// success clears it. This never allocates or invokes a callback.
    pub fn attachParent(self: *Coordinator, id: Id, parent: *?Retention) !void {
        self.lock();
        defer self.mutex.unlock();
        const slot = self.find(id) orelse return error.InvalidRemoteAttempt;
        if (slot.*.?.state != .reserved or slot.*.?.retiring or slot.*.?.parent != null or parent.* == null) return error.InvalidRemoteAttempt;
        slot.*.?.parent = parent.*;
        parent.* = null;
    }
    pub fn hasRetainedParents(self: *Coordinator) bool {
        self.lock();
        defer self.mutex.unlock();
        for (self.slots) |slot| if (slot) |attempt| if (attempt.parent != null or attempt.retiring) return true;
        return false;
    }
    pub fn checkParent(self: *Coordinator, id: Id, now_ns: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        const slot = self.find(id) orelse return error.InvalidRemoteAttempt;
        if (slot.*.?.retiring) return error.InvalidRemoteAttempt;
        if (slot.*.?.parent) |parent| try parent.check(now_ns);
    }

    /// Bind one operational cursor/worker. Never let independent supervisors
    /// reset reconciliation fairness or advance the same ledger concurrently.
    pub fn bindReconciler(self: *Coordinator) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.reconciler_bound) return error.InvalidRunnableTransition;
        self.reconciler_bound = true;
    }
    pub fn unbindReconciler(self: *Coordinator) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.reconciler_bound);
        self.reconciler_bound = false;
    }

    /// Reserves both the remote allowance and its fixed reconciliation slot.
    /// Retransmit the returned ID; reserve again only for a distinct attempt.
    pub fn reserve(self: *Coordinator, destination_index: usize, operation: u128, bytes: usize) !Id {
        self.lock();
        defer self.mutex.unlock();
        return self.reserveLocked(destination_index, operation, bytes, null);
    }

    fn reserveLocked(self: *Coordinator, destination_index: usize, operation: u128, bytes: usize, waiter: ?*Waiter) !Id {
        if (destination_index >= self.destinations.len or operation == 0 or bytes == 0) return error.InvalidRemoteAttempt;
        const destination = &self.destinations[destination_index];
        if (self.generation_fenced) return error.RemoteGenerationFenced;
        if (self.startup_fences_remaining != 0 or !destination.ready) return error.RemoteGenerationNotReady;
        if (self.next_sequence == std.math.maxInt(u64)) return error.RemoteGenerationExhausted;
        if (waiter == null and self.firstWaiting(destination_index) != null) return error.RemoteAttemptCapacity;
        // Preserve shared slot/byte headroom for the oldest destination whose
        // own allowance can grant it. Other peers may use genuinely spare
        // headroom, but cannot repeatedly consume a scarce shared grant or
        // prevent a larger older allowance from accumulating.
        if (self.firstEligibleWaiting()) |older| if (older != waiter) {
            if (older.destination == destination_index or self.slots.len - self.attempts <= 1 or
                bytes > self.max_bytes - self.bytes or self.max_bytes - self.bytes - bytes < older.bytes)
                return error.RemoteAttemptCapacity;
        };
        if (self.attempts == self.slots.len or bytes > self.max_bytes - self.bytes or destination.attempts == destination.max_attempts or bytes > destination.max_bytes - destination.bytes) return error.RemoteAttemptCapacity;
        for (self.slots) |*slot| if (slot.* == null) {
            const id = Id{ .coordinator = self.coordinator, .generation = self.generation, .operation = operation, .sequence = self.next_sequence };
            self.next_sequence += 1;
            slot.* = .{ .id = id, .destination = destination_index, .bytes = bytes };
            destination.attempts += 1;
            destination.bytes += bytes;
            self.attempts += 1;
            self.bytes += bytes;
            return id;
        };
        unreachable;
    }

    fn firstWaiting(self: *Coordinator, destination: usize) ?*Waiter {
        var current = self.waiting;
        while (current) |waiter| : (current = waiter.next) {
            if (waiter.destination == destination) return waiter;
        }
        return null;
    }
    fn firstEligibleWaiting(self: *Coordinator) ?*Waiter {
        var current = self.waiting;
        while (current) |waiter| : (current = waiter.next) {
            if (waiter.generation != self.generation or self.firstWaiting(waiter.destination) != waiter) continue;
            const target = self.destinations[waiter.destination];
            if (target.ready and target.attempts < target.max_attempts and waiter.bytes <= target.max_bytes - target.bytes) return waiter;
        }
        return null;
    }
    /// Bound both the node queue and each destination before retaining a wait.
    /// A failed destination cannot place waiters ahead of a healthy peer.
    pub fn beginWait(self: *Coordinator, waiter: *Waiter, destination: usize, operation: u128, bytes: usize) !void {
        self.lock();
        defer self.mutex.unlock();
        if (waiter.owner != null or destination >= self.destinations.len or operation == 0 or bytes == 0) return error.InvalidRemoteAttempt;
        const target = &self.destinations[destination];
        if (self.generation_fenced) return error.RemoteGenerationFenced;
        if (self.startup_fences_remaining != 0 or !target.ready) return error.RemoteGenerationNotReady;
        if (bytes > self.max_bytes or bytes > target.max_bytes or self.waiting_count >= @min(self.slots.len, maximum_waiters)) return error.RemoteAttemptCapacity;
        const retained = std.math.add(usize, self.waiting_bytes, bytes) catch return error.RemoteAttemptCapacity;
        var peer_waiters: usize = 0;
        var tail = &self.waiting;
        while (tail.*) |current| {
            peer_waiters += @intFromBool(current.destination == destination);
            tail = &current.next;
        }
        if (peer_waiters >= @min(target.max_attempts, maximum_waiters)) return error.RemoteAttemptCapacity;
        waiter.* = .{ .owner = self, .destination = destination, .operation = operation, .generation = self.generation, .bytes = bytes };
        tail.* = waiter;
        self.waiting_count += 1;
        self.waiting_bytes = retained;
    }
    /// Only a destination's oldest waiter may consume its returned credits.
    /// Shared contention also preserves priority across eligible destinations.
    pub fn reserveWaiting(self: *Coordinator, waiter: *Waiter) !?Id {
        self.lock();
        defer self.mutex.unlock();
        if (waiter.owner != self) return error.InvalidRemoteAttempt;
        if (self.generation_fenced or waiter.generation != self.generation) return error.RemoteGenerationFenced;
        if (self.startup_fences_remaining != 0 or !self.destinations[waiter.destination].ready) return error.RemoteGenerationNotReady;
        if (self.firstWaiting(waiter.destination) != waiter) return null;
        const id = self.reserveLocked(waiter.destination, waiter.operation, waiter.bytes, waiter) catch |err| {
            if (err == error.RemoteAttemptCapacity) return null;
            return err;
        };
        self.removeWaiting(waiter);
        return id;
    }
    fn removeWaiting(self: *Coordinator, waiter: *Waiter) void {
        var link = &self.waiting;
        while (link.*) |current| {
            if (current == waiter) {
                link.* = waiter.next;
                self.waiting_count -= 1;
                self.waiting_bytes -= waiter.bytes;
                waiter.* = .{};
                return;
            }
            link = &current.next;
        }
        unreachable;
    }
    /// Cancellation/deadline removes only local queue metadata, never an
    /// existing published attempt or its uncertain destination charge.
    pub fn endWait(self: *Coordinator, waiter: *Waiter) void {
        self.lock();
        defer self.mutex.unlock();
        if (waiter.owner == null) return;
        std.debug.assert(waiter.owner == self);
        self.removeWaiting(waiter);
    }

    /// Snapshot the destination for durable pre-publication history. Dispatch
    /// must still revalidate after I/O; this does not grant publication itself.
    pub fn reservedDestination(self: *Coordinator, id: Id) !u128 {
        self.lock();
        defer self.mutex.unlock();
        if (self.generation_fenced) return error.RemoteGenerationFenced;
        const slot = self.find(id) orelse return error.InvalidRemoteAttempt;
        if (slot.*.?.state != .reserved or slot.*.?.retiring or id.generation != self.generation or !self.destinations[slot.*.?.destination].ready) return error.InvalidRemoteAttempt;
        return self.destinations[slot.*.?.destination].worker_incarnation;
    }

    /// Call BEFORE publishing any bytes to a transport. After this point local
    /// teardown cannot prove non-execution, even if no response was received.
    pub fn dispatch(self: *Coordinator, id: Id) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.generation_fenced) return error.RemoteGenerationFenced;
        const slot = self.find(id) orelse return error.InvalidRemoteAttempt;
        if (slot.*.?.state != .reserved or slot.*.?.retiring or !self.destinations[slot.*.?.destination].ready or id.generation != self.generation) return error.InvalidRemoteAttempt;
        if (slot.*.?.parent) |parent| try parent.check(null);
        slot.*.?.state = .dispatched;
    }

    /// This transfer cannot allocate or fail for a dispatched attempt. Local
    /// request/transport state can retire independently of this retained record.
    pub fn transportLost(self: *Coordinator, id: Id) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.find(id)) |slot| if (slot.*.?.state != .reserved and !slot.*.?.retiring) {
            slot.*.?.state = .uncertain;
        };
    }
    pub fn abortUnpublished(self: *Coordinator, id: Id) !void {
        defer self.drainRetired();
        self.lock();
        defer self.mutex.unlock();
        const slot = self.find(id) orelse return error.InvalidRemoteAttempt;
        if (slot.*.?.state != .reserved) return error.RemoteOutcomeUncertain;
        self.retire(slot);
    }

    /// Only a trusted terminal outcome proving physical retirement may call
    /// this. A timeout, closed socket or health failure is not such evidence.
    /// Unknown/duplicate responses never create ownership or release another ID.
    pub fn terminal(self: *Coordinator, id: Id, worker_incarnation: u128) !bool {
        defer self.drainRetired();
        self.lock();
        defer self.mutex.unlock();
        const slot = self.find(id) orelse return false;
        if (slot.*.?.retiring) return false;
        if (self.destinations[slot.*.?.destination].worker_incarnation != worker_incarnation or slot.*.?.state == .reserved) return error.InvalidRemoteAttempt;
        self.retire(slot);
        return true;
    }

    /// Runtime generation change retains every old charge until reconciliation.
    pub fn beginGeneration(self: *Coordinator, generation: u64) !void {
        defer self.drainRetired();
        self.lock();
        defer self.mutex.unlock();
        if (generation <= @max(self.generation, self.observed_generation)) return error.InvalidRemoteGeneration;
        self.generation = generation;
        self.generation_fenced = false;
        self.next_sequence = 1;
        // A higher peer floor can be discovered before startup completes.
        // Every prior acknowledgment belongs to the old epoch; repeat ALL of
        // them rather than opening after only the remaining destination replies.
        if (self.startup_fences_remaining != 0) self.startup_fences_remaining = self.destinations.len;
        for (self.destinations) |*destination| destination.ready = false;
        // Unpublished reservations can retire; no remote observer owns them.
        for (self.slots) |*slot| if (slot.*) |attempt| if (attempt.state == .reserved) self.retire(slot);
    }
    pub fn acknowledgeFence(self: *Coordinator, destination_index: usize, ack: FenceAck) !void {
        defer self.drainRetired();
        self.lock();
        defer self.mutex.unlock();
        if (destination_index >= self.destinations.len) return error.InvalidRemoteAttempt;
        const destination = &self.destinations[destination_index];
        if (ack.coordinator != self.coordinator or ack.generation != self.generation or ack.worker_incarnation != destination.worker_incarnation) return error.InvalidRemoteFence;
        if (destination.ready) return;
        for (self.slots) |*slot| if (slot.*) |attempt| if (attempt.destination == destination_index) self.retire(slot);
        destination.ready = true;
        if (self.startup_fences_remaining != 0) self.startup_fences_remaining -= 1;
    }
    /// Validate before any durable reply-side mutation. IDs never alias a reused
    /// slot; final retirement still rechecks under the ledger lock after I/O.
    pub fn validateEvidenceDestination(self: *Coordinator, destination_index: usize, id: Id, worker_incarnation: u128) !void {
        self.lock();
        defer self.mutex.unlock();
        if (destination_index >= self.destinations.len or id.coordinator != self.coordinator or
            self.destinations[destination_index].worker_incarnation != worker_incarnation) return error.InvalidRemoteFence;
        if (self.find(id)) |slot| if (slot.*.?.destination != destination_index) return error.InvalidRemoteAttempt;
    }

    /// Authenticate the reply before calling. Destination validation, generation
    /// observation and exact-attempt retirement share one mutation boundary.
    /// Other destinations' uncertainty remains charged.
    pub fn reconcileFenced(self: *Coordinator, destination_index: usize, id: Id, worker_incarnation: u128, generation: u64) !bool {
        defer self.drainRetired();
        self.lock();
        defer self.mutex.unlock();
        if (destination_index >= self.destinations.len or id.coordinator != self.coordinator or generation <= id.generation or
            self.destinations[destination_index].worker_incarnation != worker_incarnation) return error.InvalidRemoteFence;
        const slot = self.find(id);
        if (slot) |value| {
            if (value.*.?.destination != destination_index or value.*.?.state == .reserved) return error.InvalidRemoteAttempt;
        }
        self.observed_generation = @max(self.observed_generation, generation);
        if (generation > self.generation) self.generation_fenced = true;
        if (slot) |value| {
            if (value.*.?.retiring) return false;
            self.retire(value);
            return true;
        }
        return false;
    }
    /// Only the owning operation may abandon an attempt. Transport loss and
    /// response destruction alone cannot revoke retained snapshots or output.
    /// Keep the allowance until the worker proves physical termination.
    pub fn abandon(self: *Coordinator, id: Id) !bool {
        self.lock();
        defer self.mutex.unlock();
        const slot = self.find(id) orelse return false;
        if (slot.*.?.retiring) return false;
        if (slot.*.?.state == .reserved) return error.InvalidRemoteAttempt;
        slot.*.?.abandoned = true;
        return true;
    }

    pub const ReconcileAction = struct { destination: usize, id: Id, kind: enum { fence, status, cancel } };
    /// Fixed ring: destination fences, destination generation probes, then
    /// published attempts. A probe sequence can never be allocated by reserve.
    pub fn reconciliationWidth(self: *Coordinator) usize {
        return self.destinations.len * 2 + self.slots.len;
    }
    pub fn reconciliationAction(self: *Coordinator, position: usize) ?ReconcileAction {
        return self.reconciliationActionAt(position, null);
    }
    pub fn reconciliationActionAt(self: *Coordinator, position: usize, now_ns: ?u64) ?ReconcileAction {
        self.lock();
        defer self.mutex.unlock();
        const count = self.destinations.len;
        if (position < count * 2) {
            const index = position % count;
            if (self.destinations[index].ready) return null;
            const probe = position >= count;
            return .{ .destination = index, .kind = if (probe) .status else .fence, .id = .{ .coordinator = self.coordinator, .generation = self.generation, .operation = if (probe) 1 else 0, .sequence = if (probe) std.math.maxInt(u64) else 0 } };
        }
        const index = position - count * 2;
        if (index >= self.slots.len) return null;
        const attempt = if (self.slots[index]) |*value| value else return null;
        if (attempt.state == .reserved or attempt.retiring) return null;
        // The slot pins these atomic signals until retirement. No borrowed
        // request callback or lock-taking callout executes under this mutex.
        if (attempt.parent) |parent| parent.check(now_ns) catch {
            attempt.abandoned = true;
        };
        return .{ .destination = attempt.destination, .id = attempt.id, .kind = if (attempt.abandoned) .cancel else .status };
    }

    /// A signed closed-generation reply forbids dispatch but proves no physical
    /// retirement. Keep every charge until separate terminal/fence evidence.
    pub fn observeClosedGeneration(self: *Coordinator, coordinator: u128, generation: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        if (coordinator != self.coordinator or generation == 0) return error.InvalidRemoteFence;
        self.observed_generation = @max(self.observed_generation, generation);
        if (generation >= self.generation) self.generation_fenced = true;
    }

    pub const GenerationState = struct { coordinator: u128, generation: u64, observed_generation: u64, generation_fenced: bool };
    pub fn generationState(self: *Coordinator) GenerationState {
        self.lock();
        defer self.mutex.unlock();
        return .{ .coordinator = self.coordinator, .generation = self.generation, .observed_generation = self.observed_generation, .generation_fenced = self.generation_fenced };
    }

    pub fn snapshot(self: *Coordinator) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        var uncertain: usize = 0;
        for (self.slots) |slot| if (slot) |attempt| {
            uncertain += @intFromBool(attempt.state == .uncertain);
        };
        return .{ .attempts = self.attempts, .bytes = self.bytes, .uncertain = uncertain, .startup_fences_remaining = self.startup_fences_remaining, .generation_fenced = self.generation_fenced, .observed_generation = self.observed_generation, .queued = self.waiting_count, .queued_bytes = self.waiting_bytes };
    }
};

pub const WorkerRecord = struct { id: Id, active: bool, canceled: bool = false, cancellation_tombstone: bool = false, binding: [32]u8 = @splat(0) };
pub const Admit = enum { start, duplicate_active, duplicate_terminal };

/// One coordinator's worker registry. Its bounded duplicate window rejects old
/// IDs rather than forgetting them and executing delayed duplicates again.
/// A bounded membership owner must retain this gate and its persisted floor;
/// dropping/recreating it from a health check is forbidden.
pub const WorkerGate = struct {
    coordinator: u128,
    worker_incarnation: u128,
    generation: u64,
    records: []?WorkerRecord,
    floor: u64 = 1,
    accepting: bool = false,
    fence_started: bool = false,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(coordinator: u128, worker_incarnation: u128, persisted_generation: u64, records: []?WorkerRecord) !WorkerGate {
        if (coordinator == 0 or worker_incarnation == 0 or records.len == 0) return error.InvalidRemoteAttemptConfig;
        @memset(records, null);
        return .{ .coordinator = coordinator, .worker_incarnation = worker_incarnation, .generation = persisted_generation, .records = records };
    }
    fn lock(self: *WorkerGate) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn beginFence(self: *WorkerGate, generation: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        if (generation == self.generation and self.fence_started) return;
        if (generation <= self.generation) return error.InvalidRemoteGeneration;
        self.generation = generation;
        self.fence_started = true;
        self.accepting = false;
    }
    /// The integrating owner first persists this exact new generation floor.
    /// An acknowledgement additionally requires every old attempt to quiesce.
    /// Persistence alone never retires still-running physical work.
    pub fn acknowledgeFence(self: *WorkerGate, persisted_generation: u64) !FenceAck {
        self.lock();
        defer self.mutex.unlock();
        if (!self.fence_started or persisted_generation != self.generation or self.generation == 0) return error.InvalidRemoteFence;
        if (self.accepting) return .{ .coordinator = self.coordinator, .generation = self.generation, .worker_incarnation = self.worker_incarnation };
        for (self.records) |record| if (record) |value| if (value.active) return error.RemoteFencePending;
        if (!self.accepting) {
            @memset(self.records, null);
            self.floor = 1;
            self.accepting = true;
        }
        return .{ .coordinator = self.coordinator, .generation = self.generation, .worker_incarnation = self.worker_incarnation };
    }
    pub fn admit(self: *WorkerGate, id: Id) !Admit {
        return self.admitBound(id, @splat(0));
    }
    pub fn admitBound(self: *WorkerGate, id: Id, binding: [32]u8) !Admit {
        self.lock();
        defer self.mutex.unlock();
        if (!self.accepting or id.coordinator != self.coordinator or id.generation != self.generation or id.sequence == 0 or id.sequence < self.floor) return error.RemoteGenerationFenced;
        for (self.records) |record| if (record) |value| {
            if (value.id.generation == id.generation and value.id.sequence == id.sequence) {
                if (!value.id.eql(id) or (!value.cancellation_tombstone and !std.mem.eql(u8, &value.binding, &binding))) return error.InvalidRemoteAttempt;
                return if (value.active) .duplicate_active else .duplicate_terminal;
            }
        };
        const new_floor = if (id.sequence >= self.records.len) id.sequence - self.records.len + 1 else 1;
        // Never evict an active operation to make a later attempt fit.
        for (self.records) |record| if (record) |value| if (value.active and value.id.sequence < new_floor) return error.RemoteAttemptCapacity;
        if (new_floor > self.floor) {
            self.floor = new_floor;
            for (self.records) |*record| if (record.*) |value| if (value.id.sequence < new_floor) {
                record.* = null;
            };
        }
        for (self.records) |*record| if (record.* == null) {
            record.* = .{ .id = id, .active = true, .binding = binding };
            return .start;
        };
        return error.RemoteAttemptCapacity;
    }
    /// Suppress a delayed dispatch even when cancellation arrives first. A
    /// cancellation tombstone has no read-body binding: it permits no execution.
    /// Active records stay active until the physical owner calls finish.
    pub fn cancel(self: *WorkerGate, id: Id) !void {
        self.lock();
        defer self.mutex.unlock();
        if (id.coordinator != self.coordinator or id.operation == 0 or id.sequence == 0 or id.sequence == std.math.maxInt(u64)) return error.InvalidRemoteAttempt;
        for (self.records) |*record| if (record.*) |*value| {
            if (value.id.generation == id.generation and value.id.sequence == id.sequence) {
                if (!value.id.eql(id)) return error.InvalidRemoteAttempt;
                value.canceled = true;
                return;
            }
        };
        // Closed epochs cannot execute. Evidence still distinguishes a closed
        // gate from an acknowledged, physically quiescent higher generation.
        if (id.generation < self.generation or (!self.accepting and id.generation == self.generation)) return;
        if (id.generation != self.generation or !self.accepting) return error.InvalidRemoteGeneration;
        if (id.sequence < self.floor) return;
        const new_floor = if (id.sequence >= self.records.len) id.sequence - self.records.len + 1 else 1;
        for (self.records) |record| if (record) |value| if (value.active and value.id.sequence < new_floor) return error.RemoteAttemptCapacity;
        if (new_floor > self.floor) {
            self.floor = new_floor;
            for (self.records) |*record| if (record.*) |value| if (value.id.sequence < new_floor) {
                record.* = null;
            };
        }
        for (self.records) |*record| if (record.* == null) {
            record.* = .{ .id = id, .active = false, .canceled = true, .cancellation_tombstone = true };
            return;
        };
        return error.RemoteAttemptCapacity;
    }
    pub const Status = enum { unknown, active, terminal, fenced, closed };
    pub const Evidence = struct { status: Status, generation: u64 };
    pub fn status(self: *WorkerGate, id: Id) Status {
        return self.evidence(id).status;
    }
    pub fn evidence(self: *WorkerGate, id: Id) Evidence {
        self.lock();
        defer self.mutex.unlock();
        const state: Status = state: {
            if (id.coordinator != self.coordinator) break :state .unknown;
            for (self.records) |record| if (record) |value| if (value.id.eql(id)) break :state if (value.active) .active else .terminal;
            if (!self.accepting and self.generation != 0 and id.generation <= self.generation) break :state .closed;
            if (self.accepting and id.generation < self.generation) break :state .fenced;
            if (self.accepting and id.generation == self.generation and id.sequence != 0 and id.sequence < self.floor) break :state .terminal;
            break :state .unknown;
        };
        return .{ .status = state, .generation = self.generation };
    }
    pub fn executable(self: *WorkerGate, id: Id) bool {
        self.lock();
        defer self.mutex.unlock();
        if (!self.accepting or id.generation != self.generation) return false;
        for (self.records) |record| if (record) |value| if (value.id.eql(id)) return value.active and !value.canceled;
        return false;
    }

    /// Call only after native children, snapshots and response obligations
    /// actually retire. Old-generation work can finish while a fence is pending.
    pub fn finish(self: *WorkerGate, id: Id) bool {
        self.lock();
        defer self.mutex.unlock();
        for (self.records) |*record| if (record.*) |*value| if (value.id.eql(id)) {
            const was_active = value.active;
            value.active = false;
            return was_active;
        };
        return false;
    }
};

const testing = std.testing;
fn ready(ledger: *Coordinator, destination: usize) !void {
    try ledger.acknowledgeFence(destination, .{ .coordinator = ledger.coordinator, .generation = ledger.generation, .worker_incarnation = ledger.destinations[destination].worker_incarnation });
}

test "remote attempts retain uncertain count and bytes while healthy destinations progress" {
    var slots: [3]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 100 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 100 },
    };
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 160);
    try ready(&ledger, 0);
    try testing.expectError(error.RemoteGenerationNotReady, ledger.reserve(0, 71, 40));
    try ready(&ledger, 1);
    const first = try ledger.reserve(0, 71, 40);
    try ledger.dispatch(first);
    ledger.transportLost(first);
    ledger.transportLost(first);
    const retry = try ledger.reserve(0, 71, 60);
    try ledger.dispatch(retry);
    ledger.transportLost(retry);
    try testing.expectError(error.RemoteAttemptCapacity, ledger.reserve(0, 71, 1));
    try testing.expectError(error.RemoteOutcomeUncertain, ledger.abortUnpublished(first));
    const healthy = try ledger.reserve(1, 71, 60);
    try ledger.dispatch(healthy);
    try testing.expectEqual(Snapshot{ .attempts = 3, .bytes = 160, .uncertain = 2, .startup_fences_remaining = 0 }, ledger.snapshot());
    try testing.expect(try ledger.terminal(healthy, 22));
    try testing.expectEqual(@as(usize, 100), ledger.snapshot().bytes);
    try testing.expectError(error.InvalidRemoteAttempt, ledger.terminal(first, 22));
    try testing.expect(try ledger.terminal(first, 11));
    try testing.expect(try ledger.terminal(retry, 11));
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}

test "remote attempts late duplicate replies cannot release a reused slot" {
    var slots: [1]?Attempt = undefined;
    var destinations = [_]Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 100 }};
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 100);
    try ready(&ledger, 0);
    const first = try ledger.reserve(0, 71, 100);
    try testing.expectError(error.InvalidRemoteAttempt, ledger.terminal(first, 11));
    try ledger.dispatch(first);
    try testing.expect(try ledger.terminal(first, 11));
    const second = try ledger.reserve(0, 71, 100);
    try ledger.dispatch(second);
    try testing.expect(!first.eql(second));
    try testing.expect(!try ledger.terminal(first, 11));
    ledger.transportLost(first);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().uncertain);
    try testing.expectEqual(@as(usize, 100), ledger.snapshot().bytes);
    try testing.expect(try ledger.terminal(second, 11));
    const unpublished = try ledger.reserve(0, 71, 100);
    try ledger.abortUnpublished(unpublished);
    try testing.expectError(error.InvalidRemoteAttempt, ledger.dispatch(unpublished));
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}

test "remote worker fencing waits for physical completion and rejects delayed old dispatch" {
    var records: [2]?WorkerRecord = undefined;
    var worker = try WorkerGate.init(1, 11, 0, &records);
    const old = Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 1 };
    try testing.expectError(error.InvalidRemoteFence, worker.acknowledgeFence(0));
    try worker.beginFence(1);
    _ = try worker.acknowledgeFence(1);
    try testing.expectEqual(Admit.start, try worker.admit(old));
    try testing.expectEqual(Admit.duplicate_active, try worker.admit(old));
    // Duplicate fence requests cannot reset or abandon current-generation work.
    try worker.beginFence(1);
    _ = try worker.acknowledgeFence(1);
    try worker.beginFence(2);
    try testing.expectError(error.RemoteGenerationFenced, worker.admit(old));
    try testing.expectError(error.InvalidRemoteFence, worker.acknowledgeFence(1));
    try testing.expectError(error.RemoteFencePending, worker.acknowledgeFence(2));
    try testing.expect(worker.finish(old));
    try testing.expect(!worker.finish(old));
    const ack = try worker.acknowledgeFence(2);
    try testing.expectEqual(@as(u64, 2), ack.generation);
    try testing.expectError(error.RemoteGenerationFenced, worker.admit(old));
    const next = Id{ .coordinator = 1, .generation = 2, .operation = 71, .sequence = 1 };
    try testing.expectEqual(Admit.start, try worker.admit(next));
    try testing.expect(worker.finish(next));
    try testing.expectEqual(Admit.duplicate_terminal, try worker.admit(next));
}

test "remote worker duplicate window never evicts active work or reruns forgotten replies" {
    var records: [2]?WorkerRecord = undefined;
    var worker = try WorkerGate.init(1, 11, 0, &records);
    try worker.beginFence(1);
    _ = try worker.acknowledgeFence(1);
    const first = Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 1 };
    const third = Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 3 };
    try testing.expectEqual(Admit.start, try worker.admit(first));
    try testing.expectError(error.RemoteAttemptCapacity, worker.admit(third));
    try testing.expect(worker.finish(first));
    try testing.expectEqual(Admit.start, try worker.admit(third));
    try testing.expectError(error.RemoteGenerationFenced, worker.admit(first));
    // An out-of-order request inside the retained window can still start once.
    const second = Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 2 };
    try testing.expectEqual(Admit.start, try worker.admit(second));
    try testing.expectEqual(Admit.duplicate_active, try worker.admit(third));
    try testing.expect(worker.finish(third));
    try testing.expect(worker.finish(second));
}

test "remote generation advance retains old charges until matching authenticated fence" {
    var slots: [3]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 100 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 100 },
    };
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 100);
    try ready(&ledger, 0);
    try ready(&ledger, 1);
    const old = try ledger.reserve(0, 71, 60);
    try ledger.dispatch(old);
    ledger.transportLost(old);
    _ = try ledger.reserve(1, 71, 30);
    try ledger.beginGeneration(2);
    try testing.expectEqual(@as(usize, 60), ledger.snapshot().bytes);
    try testing.expectError(error.RemoteGenerationNotReady, ledger.reserve(1, 71, 1));
    try testing.expectError(error.InvalidRemoteFence, ledger.acknowledgeFence(0, .{ .coordinator = 1, .generation = 1, .worker_incarnation = 11 }));
    try testing.expectError(error.InvalidRemoteFence, ledger.acknowledgeFence(0, .{ .coordinator = 1, .generation = 2, .worker_incarnation = 99 }));
    try ready(&ledger, 1);
    try testing.expectError(error.RemoteAttemptCapacity, ledger.reserve(1, 71, 41));
    const healthy = try ledger.reserve(1, 71, 40);
    try ledger.dispatch(healthy);
    try ready(&ledger, 0);
    try testing.expectEqual(@as(usize, 40), ledger.snapshot().bytes);
    try testing.expect(!try ledger.terminal(old, 11));
    // Duplicate fence acknowledgements must not retire new generation work.
    try ready(&ledger, 1);
    try testing.expectEqual(@as(usize, 40), ledger.snapshot().bytes);
    try testing.expect(try ledger.terminal(healthy, 22));
}

test "remote restart requires every destination fence proof before dispatch" {
    var slots: [2]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 100 },
        .{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 100 },
    };
    var ledger = try Coordinator.init(1, 2, &slots, &destinations, 100);
    var records: [2]?WorkerRecord = undefined;
    var worker = try WorkerGate.init(1, 11, 1, &records);
    try testing.expectError(error.InvalidRemoteFence, worker.acknowledgeFence(1));
    try testing.expectError(error.InvalidRemoteGeneration, worker.beginFence(1));
    try worker.beginFence(2);
    const ack = try worker.acknowledgeFence(2);
    try ledger.acknowledgeFence(0, ack);
    try testing.expectError(error.RemoteGenerationNotReady, ledger.reserve(0, 71, 1));
    try testing.expectError(error.RemoteGenerationFenced, worker.admit(.{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 900 }));
    try ready(&ledger, 1);
    const next = try ledger.reserve(0, 71, 100);
    try ledger.dispatch(next);
    try testing.expectEqual(Admit.start, try worker.admit(next));
    try testing.expect(worker.finish(next));
    try testing.expect(try ledger.terminal(next, 11));
}

test "remote attempt identity and generation exhaustion cannot reset ownership" {
    var records: [2]?WorkerRecord = undefined;
    var worker = try WorkerGate.init(1, 11, 0, &records);
    try worker.beginFence(1);
    _ = try worker.acknowledgeFence(1);
    const first = Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 1 };
    try testing.expectEqual(Admit.start, try worker.admit(first));
    var forged = first;
    forged.operation = 72;
    try testing.expectError(error.InvalidRemoteAttempt, worker.admit(forged));
    try testing.expect(!worker.finish(forged));
    try testing.expect(worker.finish(first));
    try testing.expectError(error.InvalidRemoteAttempt, worker.admit(forged));
    var slots: [2]?Attempt = undefined;
    var destinations = [_]Destination{.{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 100 }};
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 100);
    try ready(&ledger, 0);
    try testing.expectError(error.InvalidRemoteAttempt, ledger.reserve(0, 0, 1));
    const live = try ledger.reserve(0, 71, 50);
    try ledger.dispatch(live);
    ledger.next_sequence = std.math.maxInt(u64);
    try testing.expectError(error.RemoteGenerationExhausted, ledger.reserve(0, 72, 1));
    try testing.expectEqual(@as(usize, 50), ledger.snapshot().bytes);
    try ledger.beginGeneration(2);
    try testing.expectError(error.RemoteGenerationNotReady, ledger.reserve(0, 72, 1));
    try testing.expectEqual(@as(usize, 50), ledger.snapshot().bytes);
    try ready(&ledger, 0);
    const next = try ledger.reserve(0, 72, 100);
    try testing.expectEqual(@as(u64, 1), next.sequence);
    try testing.expectEqual(@as(u64, 2), next.generation);
    try ledger.abortUnpublished(next);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}

test "remote worker attempt cannot be rebound to different request content" {
    var records: [2]?WorkerRecord = undefined;
    var worker = try WorkerGate.init(1, 11, 0, &records);
    try worker.beginFence(1);
    _ = try worker.acknowledgeFence(1);
    const id = Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 1 };
    try testing.expectEqual(Admit.start, try worker.admitBound(id, @splat(1)));
    try testing.expectError(error.InvalidRemoteAttempt, worker.admitBound(id, @splat(2)));
    try testing.expectEqual(Admit.duplicate_active, try worker.admitBound(id, @splat(1)));
    try testing.expect(worker.finish(id));
    try testing.expectError(error.InvalidRemoteAttempt, worker.admitBound(id, @splat(2)));
    try testing.expectEqual(Admit.duplicate_terminal, try worker.admitBound(id, @splat(1)));
}

test "remote startup advancement repeats every fence and reconciliation skips unpublished work" {
    var slots: [2]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 100 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 100 },
    };
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 100);
    try ready(&ledger, 0);
    try testing.expectEqual(@as(usize, 1), ledger.snapshot().startup_fences_remaining);
    try ledger.beginGeneration(2);
    try testing.expectEqual(@as(usize, 2), ledger.snapshot().startup_fences_remaining);
    try testing.expectError(error.InvalidRemoteFence, ledger.acknowledgeFence(1, .{ .coordinator = 1, .generation = 1, .worker_incarnation = 22 }));
    try ready(&ledger, 1);
    try testing.expectError(error.RemoteGenerationNotReady, ledger.reserve(1, 71, 10));
    const fence = ledger.reconciliationAction(0).?;
    try testing.expectEqual(@as(u64, 2), fence.id.generation);
    try testing.expectEqual(@as(usize, 0), fence.destination);
    const probe = ledger.reconciliationAction(2).?;
    try testing.expectEqual(std.math.maxInt(u64), probe.id.sequence);
    try ready(&ledger, 0);
    const unpublished = try ledger.reserve(0, 71, 10);
    try testing.expectError(error.InvalidRemoteAttempt, ledger.abandon(unpublished));
    const published = try ledger.reserve(1, 72, 10);
    try ledger.dispatch(published);
    try testing.expect(ledger.reconciliationAction(4) == null);
    try testing.expect(ledger.reconciliationAction(5).?.id.eql(published));
    try testing.expectEqual(@as(usize, 6), ledger.reconciliationWidth());
    try testing.expectEqual(.status, ledger.reconciliationAction(5).?.kind);
    try testing.expect(try ledger.abandon(published));
    try testing.expectEqual(.cancel, ledger.reconciliationAction(5).?.kind);
    try testing.expectEqual(@as(usize, 20), ledger.snapshot().bytes);
}

test "remote cancellation tombstones delayed dispatch without evicting active work" {
    var records: [2]?WorkerRecord = undefined;
    var worker = try WorkerGate.init(1, 11, 0, &records);
    try worker.beginFence(1);
    _ = try worker.acknowledgeFence(1);
    const first = Id{ .coordinator = 1, .generation = 1, .operation = 71, .sequence = 1 };
    var next = first;
    next.sequence = 2;
    try testing.expectEqual(Admit.start, try worker.admitBound(first, @splat(1)));
    try worker.cancel(next);
    try testing.expect(worker.executable(first));
    try worker.cancel(first);
    try testing.expect(!worker.executable(first));
    try testing.expectEqual(WorkerGate.Status.active, worker.status(first));
    try testing.expectEqual(Admit.duplicate_active, try worker.admitBound(first, @splat(1)));
    try testing.expectError(error.InvalidRemoteAttempt, worker.admitBound(first, @splat(2)));
    var forged = first;
    forged.operation += 1;
    try testing.expectError(error.InvalidRemoteAttempt, worker.cancel(forged));
    try worker.cancel(next);
    try testing.expectEqual(WorkerGate.Status.terminal, worker.status(next));
    try testing.expectEqual(Admit.duplicate_terminal, try worker.admitBound(next, @splat(2)));
    var third = next;
    third.sequence = 3;
    try testing.expectError(error.RemoteAttemptCapacity, worker.cancel(third));
    try testing.expectEqual(@as(u64, 1), worker.floor);
    try testing.expect(worker.finish(first));
    try worker.cancel(third);
    try testing.expectEqual(WorkerGate.Status.terminal, worker.status(first));
    try testing.expectError(error.RemoteGenerationFenced, worker.admit(first));
    // The reserved generation-probe sequence must never shift the window.
    third.sequence = std.math.maxInt(u64);
    try testing.expectError(error.InvalidRemoteAttempt, worker.cancel(third));
    try worker.beginFence(2);
    try worker.cancel(next);
    try testing.expectEqual(WorkerGate.Status.terminal, worker.status(next));
    _ = try worker.acknowledgeFence(2);
    try worker.cancel(next);
    try testing.expectEqual(WorkerGate.Status.fenced, worker.status(next));
}

test "remote coordinator parent release retains its slot budget and permits reentrant observation" {
    var slots: [1]?Attempt = undefined;
    var destinations = [_]Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 1024 }};
    var ledger = try Coordinator.init(7, 1, &slots, &destinations, 1024);
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 11 });
    const Probe = struct {
        ledger: *Coordinator,
        releases: usize = 0,
        fn release(raw: *anyopaque, _: usize, id: Id) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // Observing/reentering the ledger cannot deadlock, and capacity
            // cannot be reused until this physical parent reference is released.
            std.debug.assert(self.ledger.snapshot().attempts == 1);
            std.debug.assert(self.ledger.hasRetainedParents());
            std.debug.assert(!(self.ledger.terminal(id, 11) catch unreachable));
            self.ledger.transportLost(id);
            std.debug.assert(self.ledger.reconciliationAction(2) == null);
            _ = self.ledger.reserve(0, 99, 1) catch |err| {
                std.debug.assert(err == error.RemoteAttemptCapacity);
                self.releases += 1;
                return;
            };
            unreachable;
        }
    };
    var probe = Probe{ .ledger = &ledger };
    const id = try ledger.reserve(0, 71, 1024);
    var parent: ?Retention = .{ .context = &probe, .index = 0, .id = id, .release_fn = Probe.release };
    try ledger.attachParent(id, &parent);
    try std.testing.expect(parent == null);
    try ledger.dispatch(id);
    ledger.transportLost(id);
    try std.testing.expect(try ledger.abandon(id));
    try std.testing.expectEqual(@as(usize, 0), probe.releases);
    try std.testing.expectError(error.InvalidRemoteAttempt, ledger.terminal(id, 22));
    try std.testing.expect(try ledger.terminal(id, 11));
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expect(!ledger.hasRetainedParents());
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().bytes);
    try std.testing.expect(!(try ledger.terminal(id, 11)));
}

test "remote coordinator parent references survive epoch changes and retire only their destination" {
    const Probe = struct {
        releases: usize = 0,
        fn release(raw: *anyopaque, _: usize, _: Id) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    var probes: [2]Probe = @splat(.{});
    var slots: [2]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 1024 },
        .{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 1024 },
    };
    var ledger = try Coordinator.init(7, 1, &slots, &destinations, 2048);
    for (destinations, 0..) |destination, index| try ledger.acknowledgeFence(index, .{ .coordinator = 7, .generation = 1, .worker_incarnation = destination.worker_incarnation });
    for (&probes, 0..) |*probe, index| {
        const id = try ledger.reserve(index, index + 1, 1024);
        var parent: ?Retention = .{ .context = probe, .index = 0, .id = id, .release_fn = Probe.release };
        try ledger.attachParent(id, &parent);
        try ledger.dispatch(id);
        ledger.transportLost(id);
    }
    try ledger.beginGeneration(2);
    try std.testing.expectEqual(@as(usize, 0), probes[0].releases + probes[1].releases);
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 2, .worker_incarnation = 11 });
    try std.testing.expectEqual(@as(usize, 1), probes[0].releases);
    try std.testing.expectEqual(@as(usize, 0), probes[1].releases);
    try std.testing.expectEqual(@as(usize, 1024), ledger.snapshot().bytes);
    try ledger.acknowledgeFence(1, .{ .coordinator = 7, .generation = 2, .worker_incarnation = 22 });
    try std.testing.expectEqual(@as(usize, 1), probes[1].releases);
    const unpublished = try ledger.reserve(0, 3, 1024);
    var parent: ?Retention = .{ .context = &probes[0], .index = 0, .id = unpublished, .release_fn = Probe.release };
    try ledger.attachParent(unpublished, &parent);
    try ledger.abortUnpublished(unpublished);
    try std.testing.expectEqual(@as(usize, 2), probes[0].releases);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}

test "remote coordinator failed parent transfer and unpublished epoch cleanup preserve ownership" {
    const Probe = struct {
        releases: usize = 0,
        fn release(raw: *anyopaque, _: usize, _: Id) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    var probe: Probe = .{};
    var slots: [1]?Attempt = undefined;
    var destinations = [_]Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 1024 }};
    var ledger = try Coordinator.init(7, 1, &slots, &destinations, 1024);
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 11 });
    const id = try ledger.reserve(0, 71, 1024);
    var wrong = id;
    wrong.operation += 1;
    var parent: ?Retention = .{ .context = &probe, .index = 0, .id = id, .release_fn = Probe.release };
    try std.testing.expectError(error.InvalidRemoteAttempt, ledger.attachParent(wrong, &parent));
    try std.testing.expect(parent != null);
    try ledger.attachParent(id, &parent);
    try ledger.beginGeneration(2); // Unpublished work has no remote observer.
    try std.testing.expect(parent == null);
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expect(!ledger.hasRetainedParents());
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 2, .worker_incarnation = 11 });
    const published = try ledger.reserve(0, 72, 1024);
    parent = .{ .context = &probe, .index = 0, .id = published, .release_fn = Probe.release };
    try ledger.attachParent(published, &parent);
    try ledger.dispatch(published);
    var untransferred: ?Retention = .{ .context = &probe, .index = 0, .id = published, .release_fn = Probe.release };
    try std.testing.expectError(error.InvalidRemoteAttempt, ledger.attachParent(published, &untransferred));
    try std.testing.expect(untransferred != null);
    // Caller still owns a failed transfer; it must release that separate hold.
    untransferred.?.release();
    untransferred = null;
    try std.testing.expectEqual(@as(usize, 2), probe.releases);
    try std.testing.expect(try ledger.reconcileFenced(0, published, 11, 3));
    try std.testing.expectEqual(@as(usize, 3), probe.releases);
    try std.testing.expect(!(try ledger.reconcileFenced(0, published, 11, 3)));
}

test "remote coordinator automatically cancels descendants after parent expiry or owned cancellation" {
    const Probe = struct {
        releases: usize = 0,
        fn release(raw: *anyopaque, _: usize, _: Id) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    for ([_]enum { primary, secondary, deadline }{ .primary, .secondary, .deadline }) |cause| {
        var probe: Probe = .{};
        var first: std.atomic.Value(bool) = .init(false);
        var second: std.atomic.Value(bool) = .init(false);
        var slots: [1]?Attempt = undefined;
        var destinations = [_]Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 1024 }};
        var ledger = try Coordinator.init(7, 1, &slots, &destinations, 1024);
        try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 11 });
        const id = try ledger.reserve(0, 71, 1024);
        var parent: ?Retention = .{ .context = &probe, .index = 0, .id = id, .release_fn = Probe.release, .cancellation = &first, .secondary_cancellation = &second, .deadline_ns = 100 };
        try ledger.attachParent(id, &parent);
        try ledger.checkParent(id, 50);
        try ledger.dispatch(id);
        try std.testing.expectEqual(Coordinator.ReconcileAction{ .destination = 0, .id = id, .kind = .status }, ledger.reconciliationActionAt(2, 50).?);
        switch (cause) {
            .primary => first.store(true, .release),
            .secondary => second.store(true, .release),
            .deadline => {},
        }
        try std.testing.expectEqual(Coordinator.ReconcileAction{ .destination = 0, .id = id, .kind = .cancel }, ledger.reconciliationActionAt(2, if (cause == .deadline) 100 else 50).?);
        try std.testing.expectEqual(@as(usize, 0), probe.releases);
        try std.testing.expectEqual(@as(usize, 1024), ledger.snapshot().bytes);
        try std.testing.expect(try ledger.terminal(id, 11));
        try std.testing.expectEqual(@as(usize, 1), probe.releases);
    }
}

test "remote coordinator cannot publish a retained child after cancellation during durable preparation" {
    const Probe = struct {
        releases: usize = 0,
        fn release(raw: *anyopaque, _: usize, _: Id) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
        }
    };
    var probe: Probe = .{};
    var stopped: std.atomic.Value(bool) = .init(false);
    var slots: [1]?Attempt = undefined;
    var destinations = [_]Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 1024 }};
    var ledger = try Coordinator.init(7, 1, &slots, &destinations, 1024);
    try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = 1, .worker_incarnation = 11 });
    const id = try ledger.reserve(0, 71, 1024);
    var parent: ?Retention = .{ .context = &probe, .index = 0, .id = id, .release_fn = Probe.release, .cancellation = &stopped, .deadline_ns = 100 };
    try ledger.attachParent(id, &parent);
    try std.testing.expectError(error.DeadlineExceeded, ledger.checkParent(id, 100));
    stopped.store(true, .release);
    try std.testing.expectError(error.Canceled, ledger.dispatch(id));
    try ledger.abortUnpublished(id);
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().bytes);
}

test "remote credit waiters preserve destination FIFO without blocking healthy peers" {
    var slots: [4]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 20 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 20 },
    };
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 40);
    try ready(&ledger, 0);
    try ready(&ledger, 1);
    const a = try ledger.reserve(0, 1, 10);
    const b = try ledger.reserve(0, 2, 10);
    const sequence = ledger.next_sequence;
    var first: Coordinator.Waiter = .{};
    var second: Coordinator.Waiter = .{};
    try ledger.beginWait(&first, 0, 3, 10);
    defer ledger.endWait(&first);
    try ledger.beginWait(&second, 0, 4, 10);
    defer ledger.endWait(&second);
    try testing.expectEqual(sequence, ledger.next_sequence);
    try testing.expectEqual(@as(usize, 2), ledger.snapshot().queued);
    try ledger.abortUnpublished(a);
    try testing.expect((try ledger.reserveWaiting(&second)) == null);
    try testing.expectError(error.RemoteAttemptCapacity, ledger.reserve(0, 5, 10));
    const healthy = try ledger.reserve(1, 6, 10);
    try ledger.abortUnpublished(healthy);
    const selected_first = (try ledger.reserveWaiting(&first)).?;
    try ledger.abortUnpublished(b);
    const selected_second = (try ledger.reserveWaiting(&second)).?;
    try ledger.abortUnpublished(selected_first);
    try ledger.abortUnpublished(selected_second);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued_bytes);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}

test "remote credit queue is bounded and cancellation promotes the next waiter without publication" {
    var slots: [4]?Attempt = undefined;
    var destinations = [_]Destination{.{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 20 }};
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 20);
    try ready(&ledger, 0);
    const held = try ledger.reserve(0, 1, 20);
    var first: Coordinator.Waiter = .{};
    var second: Coordinator.Waiter = .{};
    var overflow: Coordinator.Waiter = .{};
    try ledger.beginWait(&first, 0, 2, 10);
    try ledger.beginWait(&second, 0, 3, 10);
    try testing.expectError(error.RemoteAttemptCapacity, ledger.beginWait(&overflow, 0, 4, 10));
    const sequence = ledger.next_sequence;
    ledger.endWait(&first);
    try testing.expectEqual(sequence, ledger.next_sequence);
    try testing.expectEqual(@as(usize, 1), ledger.snapshot().attempts);
    try ledger.abortUnpublished(held);
    const selected = (try ledger.reserveWaiting(&second)).?;
    try ledger.abortUnpublished(selected);
    ledger.endWait(&second);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued_bytes);
    try testing.expectError(error.RemoteAttemptCapacity, ledger.beginWait(&overflow, 0, 5, 21));
    try ledger.beginWait(&overflow, 0, 6, 10);
    try ledger.observeClosedGeneration(1, 1);
    try testing.expectError(error.RemoteGenerationFenced, ledger.reserveWaiting(&overflow));
    ledger.endWait(&overflow);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
}

test "remote credit waiters cannot migrate into a replacement generation" {
    var slots: [1]?Attempt = undefined;
    var destinations = [_]Destination{.{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 20 }};
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 20);
    try ready(&ledger, 0);
    var waiter: Coordinator.Waiter = .{};
    try ledger.beginWait(&waiter, 0, 7, 10);
    defer ledger.endWait(&waiter);
    try ledger.beginGeneration(2);
    try ready(&ledger, 0);
    try testing.expectError(error.RemoteGenerationFenced, ledger.reserveWaiting(&waiter));
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
    ledger.endWait(&waiter);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
}

test "remote credit shared capacity honors oldest eligible destination before younger waiters" {
    var slots: [2]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 40 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 40 },
    };
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 40);
    try ready(&ledger, 0);
    try ready(&ledger, 1);
    const held1 = try ledger.reserve(1, 1, 10);
    const held2 = try ledger.reserve(1, 2, 10);
    var older: Coordinator.Waiter = .{};
    var younger: Coordinator.Waiter = .{};
    try ledger.beginWait(&older, 0, 3, 10);
    defer ledger.endWait(&older);
    try ledger.beginWait(&younger, 1, 4, 10);
    defer ledger.endWait(&younger);
    try ledger.abortUnpublished(held1);
    const sequence = ledger.next_sequence;
    try testing.expect((try ledger.reserveWaiting(&younger)) == null);
    try testing.expectEqual(sequence, ledger.next_sequence);
    const first = (try ledger.reserveWaiting(&older)).?;
    try ledger.abortUnpublished(held2);
    const second = (try ledger.reserveWaiting(&younger)).?;
    try ledger.abortUnpublished(first);
    try ledger.abortUnpublished(second);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}

test "remote credit shared byte headroom accumulates for an older eligible destination" {
    var slots: [3]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 30 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 30 },
    };
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 30);
    try ready(&ledger, 0);
    try ready(&ledger, 1);
    const held1 = try ledger.reserve(1, 1, 15);
    const held2 = try ledger.reserve(1, 2, 15);
    var older: Coordinator.Waiter = .{};
    try ledger.beginWait(&older, 0, 3, 30);
    defer ledger.endWait(&older);
    try ledger.abortUnpublished(held1);
    const sequence = ledger.next_sequence;
    try testing.expectError(error.RemoteAttemptCapacity, ledger.reserve(1, 4, 10));
    try testing.expectEqual(sequence, ledger.next_sequence);
    try testing.expect((try ledger.reserveWaiting(&older)) == null);
    try ledger.abortUnpublished(held2);
    try testing.expectError(error.RemoteAttemptCapacity, ledger.reserve(1, 5, 10));
    const first = (try ledger.reserveWaiting(&older)).?;
    try ledger.abortUnpublished(first);
    const next = try ledger.reserve(1, 6, 10);
    try ledger.abortUnpublished(next);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}

test "remote credit shared priority bypasses a destination blocked by its own retained allowance" {
    var slots: [2]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 10 },
        .{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 10 },
    };
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 20);
    try ready(&ledger, 0);
    try ready(&ledger, 1);
    const bad = try ledger.reserve(0, 1, 10);
    const occupied = try ledger.reserve(1, 2, 10);
    var older: Coordinator.Waiter = .{};
    var younger: Coordinator.Waiter = .{};
    try ledger.beginWait(&older, 0, 3, 10);
    defer ledger.endWait(&older);
    try ledger.abortUnpublished(occupied);
    const fresh = try ledger.reserve(1, 4, 10);
    try ledger.abortUnpublished(fresh);
    try ledger.beginWait(&younger, 1, 5, 10);
    defer ledger.endWait(&younger);
    const healthy = (try ledger.reserveWaiting(&younger)).?;
    try ledger.abortUnpublished(healthy);
    try testing.expect((try ledger.reserveWaiting(&older)) == null);
    try ledger.abortUnpublished(bad);
    const recovered = (try ledger.reserveWaiting(&older)).?;
    try ledger.abortUnpublished(recovered);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}

test "remote credit shared priority cancellation promotes another destination without publication" {
    var slots: [2]?Attempt = undefined;
    var destinations = [_]Destination{
        .{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 20 },
        .{ .worker_incarnation = 22, .max_attempts = 2, .max_bytes = 20 },
    };
    var ledger = try Coordinator.init(1, 1, &slots, &destinations, 20);
    try ready(&ledger, 0);
    try ready(&ledger, 1);
    const held1 = try ledger.reserve(1, 1, 10);
    const held2 = try ledger.reserve(1, 2, 10);
    var older: Coordinator.Waiter = .{};
    var younger: Coordinator.Waiter = .{};
    try ledger.beginWait(&older, 0, 3, 10);
    defer ledger.endWait(&older);
    try ledger.beginWait(&younger, 1, 4, 10);
    defer ledger.endWait(&younger);
    try ledger.abortUnpublished(held1);
    try testing.expect((try ledger.reserveWaiting(&younger)) == null);
    const sequence = ledger.next_sequence;
    ledger.endWait(&older);
    try testing.expectEqual(sequence, ledger.next_sequence);
    const next = (try ledger.reserveWaiting(&younger)).?;
    try ledger.abortUnpublished(next);
    try ledger.abortUnpublished(held2);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().queued);
    try testing.expectEqual(@as(usize, 0), ledger.snapshot().attempts);
}
