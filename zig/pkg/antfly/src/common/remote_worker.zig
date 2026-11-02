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

//! Stable bounded physical owners for authenticated remote reads. Membership
//! bindings must come from the authoritative API admission adapter. Handlers
//! must retain resources before publication and release them AFTER physical
//! joins/destruction. Handles may move before publication, but must not be copied.
const std = @import("std");
const attempts = @import("remote_attempts.zig");
const wire = @import("remote_attempt_wire.zig");
const membership = @import("remote_membership.zig");
const fence_store = @import("remote_fence_store.zig");

pub const Limits = struct {
    coordinators: usize,
    attempts: usize,
    per_coordinator_attempts: usize,
    duplicate_window: usize,
    bytes: usize,
    per_coordinator_bytes: usize,
    resource_references: usize,
    controls: usize = 2,
    pub fn validate(self: Limits, capacity: usize) !void {
        if (self.coordinators == 0 or self.coordinators > capacity or self.attempts == 0 or
            self.per_coordinator_attempts == 0 or self.per_coordinator_attempts > self.attempts or
            self.duplicate_window < self.per_coordinator_attempts or self.bytes == 0 or
            self.per_coordinator_bytes == 0 or self.per_coordinator_bytes > self.bytes or
            self.resource_references == 0 or self.controls == 0) return error.InvalidRemoteAttemptConfig;
        _ = std.math.mul(usize, self.coordinators, self.duplicate_window) catch return error.InvalidRemoteAttemptConfig;
        _ = std.math.mul(usize, self.attempts, self.resource_references) catch return error.InvalidRemoteAttemptConfig;
    }
};
const Phase = enum { empty, loading, ready };
const Coordinator = struct { id: u128 = 0, phase: Phase = .empty, gate: ?attempts.WorkerGate = null, count: usize = 0, bytes: usize = 0 };
const Entry = struct {
    id: attempts.Id,
    coordinator: usize,
    bytes: usize,
    deadline_ns: u64,
    references: usize = 1,
    closing: bool = false,
    cause: ?anyerror = null,
    canceled: std.atomic.Value(bool) = .init(false),
};
pub const Rejection = enum { capacity, deadline };
pub const Begin = union(enum) { started: Lease, duplicate_active, terminal, rejected: Rejection };
pub const Snapshot = struct { attempts: usize, bytes: usize, references: usize, controls: usize };
/// Borrowed bootstrap authority. It must prove that prior external work is
/// reconciled/fenced, with publication stopped; a health check is insufficient.
pub const RecoveryCheck = struct { context: *const anyopaque, quiescent: *const fn (*const anyopaque) bool };

pub const Registry = struct {
    allocator: std.mem.Allocator,
    store: *fence_store.Store,
    local: membership.Local,
    limits: Limits,
    now_ns: *const fn () u64,
    coordinators: []Coordinator,
    records: []?attempts.WorkerRecord,
    entries: []?Entry,
    count: usize = 0,
    bytes: usize = 0,
    controls: usize = 0,
    admission_closed: bool = false,
    recovery_complete: bool = false,
    mutex: std.atomic.Mutex = .unlocked,

    /// Heap backing only; Registry itself is embedded in its node owner.
    pub fn metadataBytes(limits: Limits) !u64 {
        try limits.validate(65535);
        const coordinators = try std.math.mul(u64, limits.coordinators, @sizeOf(Coordinator));
        const records = try std.math.mul(u64, try std.math.mul(u64, limits.coordinators, limits.duplicate_window), @sizeOf(?attempts.WorkerRecord));
        const entries = try std.math.mul(u64, limits.attempts, @sizeOf(?Entry));
        return std.math.add(u64, coordinators, try std.math.add(u64, records, entries));
    }

    /// Allocations use the caller's accounted owner. No dispatch allocates
    /// registry storage; ready coordinator records are never evicted on health.
    pub fn init(allocator: std.mem.Allocator, store: *fence_store.Store, local: membership.Local, limits: Limits, now_ns: *const fn () u64) !Registry {
        _ = try membership.coordinatorIdentity(local.metadata_incarnation, local.node_id);
        if (local.root_incarnation != store.worker_incarnation or local.store_id == 0 or local.reporter_incarnation == 0) return error.InvalidRemoteAttemptConfig;
        try limits.validate(store.floors.len);
        const record_count = limits.coordinators * limits.duplicate_window;
        const coordinators = try allocator.alloc(Coordinator, limits.coordinators);
        errdefer allocator.free(coordinators);
        for (coordinators) |*value| value.* = .{};
        const records = try allocator.alloc(?attempts.WorkerRecord, record_count);
        errdefer allocator.free(records);
        @memset(records, null);
        const entries = try allocator.alloc(?Entry, limits.attempts);
        @memset(entries, null);
        return .{ .allocator = allocator, .store = store, .local = local, .limits = limits, .now_ns = now_ns, .coordinators = coordinators, .records = records, .entries = entries };
    }
    fn lock(self: *Registry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn deinit(self: *Registry) !void {
        self.lock();
        if (self.count != 0 or self.controls != 0) {
            self.mutex.unlock();
            return error.RemoteWorkerBusy;
        }
        self.admission_closed = true;
        self.mutex.unlock();
        // The owning supervisor must also join all entry/status/control calls.
        self.allocator.free(self.entries);
        self.allocator.free(self.records);
        self.allocator.free(self.coordinators);
        self.* = undefined;
    }
    pub fn completeRecovery(self: *Registry, proof: RecoveryCheck) !void {
        // The bootstrap owner holds its publication barrier across this call.
        if (!proof.quiescent(proof.context)) return error.RemoteWorkerRecoveryPending;
        self.lock();
        defer self.mutex.unlock();
        self.recovery_complete = true;
    }
    pub fn recoveryComplete(self: *Registry) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.recovery_complete;
    }
    fn requireRecovery(self: *Registry) !void {
        self.lock();
        defer self.mutex.unlock();
        if (!self.recovery_complete) return error.RemoteWorkerRecoveryPending;
    }
    fn validate(self: *Registry, verified: wire.Verified, binding: membership.Binding, kind: wire.Kind) !void {
        try verified.context.validate();
        if (verified.context.kind != kind or verified.context.worker_incarnation != self.local.root_incarnation or
            binding.worker_node_id != self.local.node_id or binding.store_id != self.local.store_id or
            binding.root_incarnation != self.local.root_incarnation or binding.reporter_incarnation != self.local.reporter_incarnation or
            try membership.coordinatorIdentity(self.local.metadata_incarnation, binding.coordinator_node_id) != verified.context.id.coordinator) return error.InvalidRemoteMembership;
    }
    fn gateFor(self: *Registry, id: u128, create: bool) !usize {
        return self.gateForWithWait(id, create, null);
    }
    fn gateForWithWait(self: *Registry, id: u128, create: bool, wait: ?fence_store.Wait) !usize {
        self.lock();
        var empty: ?usize = null;
        for (self.coordinators, 0..) |*coordinator, index| {
            if (coordinator.phase != .empty and coordinator.id == id) {
                const phase = coordinator.phase;
                self.mutex.unlock();
                if (phase != .ready) return error.RemoteControlCapacity;
                return index;
            }
            if (coordinator.phase == .empty and empty == null) empty = index;
        }
        if (!create or empty == null) {
            self.mutex.unlock();
            return if (create) error.RemoteFenceRegistryFull else error.RemoteGenerationNotReady;
        }
        const index = empty.?;
        self.coordinators[index] = .{ .id = id, .phase = .loading };
        self.mutex.unlock();
        // No registry lock spans storage I/O or storage-owner mutex waits.
        const start = index * self.limits.duplicate_window;
        const gate = self.store.withWait(wait).restoreGate(id, self.records[start..][0..self.limits.duplicate_window]) catch |err| {
            self.lock();
            self.coordinators[index] = .{};
            self.mutex.unlock();
            return err;
        };
        self.lock();
        self.coordinators[index].gate = gate;
        self.coordinators[index].phase = .ready;
        self.mutex.unlock();
        return index;
    }
    fn entry(self: *Registry, index: usize, id: attempts.Id) ?*Entry {
        if (index >= self.entries.len) return null;
        if (self.entries[index]) |*value| if (value.id.eql(id)) return value;
        return null;
    }
    fn stop(value: *Entry, cause: anyerror) void {
        if (value.cause == null) value.cause = cause;
        value.canceled.store(true, .release);
    }
    fn check(self: *Registry, value: *Entry) !void {
        if (value.cause) |cause| return cause;
        if (value.closing) return error.Canceled;
        if (self.now_ns() >= value.deadline_ns) stop(value, error.DeadlineExceeded) else if (!self.coordinators[value.coordinator].gate.?.executable(value.id)) stop(value, error.RemoteGenerationFenced);
        if (value.cause) |cause| return cause;
    }
    pub fn begin(self: *Registry, verified: wire.Verified, binding: membership.Binding) !Begin {
        return self.beginWithWait(verified, binding, null);
    }
    pub fn beginWithWait(self: *Registry, verified: wire.Verified, binding: membership.Binding, wait: ?fence_store.Wait) !Begin {
        try self.validate(verified, binding, .read);
        try self.requireRecovery();
        const coordinator_index = try self.gateForWithWait(verified.context.id.coordinator, false, wait);
        // No registry mutex spans filesystem I/O. Durable publication history
        // must precede gate admission and every physical execution owner.
        try self.store.withWait(wait).markPublicationStarted();
        self.lock();
        defer self.mutex.unlock();
        if (self.admission_closed) return error.RemoteWorkerClosing;
        const coordinator = &self.coordinators[coordinator_index];
        const gate = &coordinator.gate.?;
        switch (try gate.admitBound(verified.context.id, verified.context.request_digest)) {
            .duplicate_terminal => return .terminal,
            .duplicate_active => {
                for (self.entries) |*slot| if (slot.*) |*value| if (value.id.eql(verified.context.id)) {
                    value.deadline_ns = @min(value.deadline_ns, verified.local_deadline_ns);
                };
                return .duplicate_active;
            },
            .start => {},
        }
        const bytes = std.math.cast(usize, verified.context.charged_bytes);
        const rejection: ?Rejection = if (self.now_ns() >= verified.local_deadline_ns) .deadline else if (bytes == null or self.count == self.entries.len or coordinator.count == self.limits.per_coordinator_attempts or bytes.? > self.limits.bytes - self.bytes or bytes.? > self.limits.per_coordinator_bytes - coordinator.bytes) .capacity else null;
        if (rejection) |reason| {
            // Preserve a terminal tombstone even for rejected publication, so
            // a delayed duplicate cannot start after capacity/drain changes.
            std.debug.assert(gate.finish(verified.context.id));
            return .{ .rejected = reason };
        }
        for (self.entries, 0..) |*slot, index| if (slot.* == null) {
            slot.* = .{ .id = verified.context.id, .coordinator = coordinator_index, .bytes = bytes.?, .deadline_ns = verified.local_deadline_ns };
            self.count += 1;
            self.bytes += bytes.?;
            coordinator.count += 1;
            coordinator.bytes += bytes.?;
            return .{ .started = .{ .registry = self, .index = index, .id = verified.context.id } };
        };
        unreachable;
    }
    pub fn fence(self: *Registry, verified: wire.Verified, binding: membership.Binding) !attempts.FenceAck {
        return self.fenceWithWait(verified, binding, null);
    }
    pub fn fenceWithWait(self: *Registry, verified: wire.Verified, binding: membership.Binding, wait: ?fence_store.Wait) !attempts.FenceAck {
        try self.validate(verified, binding, .fence);
        try self.requireRecovery();
        if (self.now_ns() >= verified.local_deadline_ns) return error.DeadlineExceeded;
        self.lock();
        if (self.controls == self.limits.controls) {
            self.mutex.unlock();
            return error.RemoteControlCapacity;
        }
        self.controls += 1;
        self.mutex.unlock();
        defer {
            self.lock();
            self.controls -= 1;
            self.mutex.unlock();
        }
        const index = try self.gateForWithWait(verified.context.id.coordinator, true, wait);
        const gate = &self.coordinators[index].gate.?;
        try gate.beginFence(verified.context.id.generation);
        self.lock();
        for (self.entries) |*slot| if (slot.*) |*value| if (value.coordinator == index and value.id.generation < verified.context.id.generation) stop(value, error.RemoteGenerationFenced);
        self.mutex.unlock();
        return self.store.withWait(wait).fence(gate, verified.context.id.generation);
    }
    pub fn status(self: *Registry, verified: wire.Verified, binding: membership.Binding) !attempts.WorkerGate.Status {
        return (try self.statusEvidence(verified, binding)).status;
    }
    pub fn statusEvidence(self: *Registry, verified: wire.Verified, binding: membership.Binding) !attempts.WorkerGate.Evidence {
        return self.statusEvidenceWithWait(verified, binding, null);
    }
    pub fn statusEvidenceWithWait(self: *Registry, verified: wire.Verified, binding: membership.Binding, wait: ?fence_store.Wait) !attempts.WorkerGate.Evidence {
        try self.validate(verified, binding, .status);
        if (self.now_ns() >= verified.local_deadline_ns) return error.DeadlineExceeded;
        const index = self.gateForWithWait(verified.context.id.coordinator, true, wait) catch |err| switch (err) {
            error.RemoteGenerationNotReady => return .{ .status = .unknown, .generation = 0 },
            else => return err,
        };
        return self.coordinators[index].gate.?.evidence(verified.context.id);
    }
    pub fn cancelWithWait(self: *Registry, verified: wire.Verified, binding: membership.Binding, wait: ?fence_store.Wait) !attempts.WorkerGate.Evidence {
        try self.validate(verified, binding, .cancel);
        try self.requireRecovery();
        if (self.now_ns() >= verified.local_deadline_ns) return error.DeadlineExceeded;
        const index = try self.gateForWithWait(verified.context.id.coordinator, true, wait);
        self.lock();
        defer self.mutex.unlock();
        const gate = &self.coordinators[index].gate.?;
        try gate.cancel(verified.context.id);
        for (self.entries) |*slot| if (slot.*) |*value| if (value.id.eql(verified.context.id)) stop(value, error.Canceled);
        return gate.evidence(verified.context.id);
    }
    pub fn closeAdmission(self: *Registry) void {
        self.lock();
        defer self.mutex.unlock();
        self.admission_closed = true;
    }
    pub fn snapshot(self: *Registry) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        var references: usize = 0;
        for (self.entries) |slot| if (slot) |value| {
            references += value.references;
        };
        return .{ .attempts = self.count, .bytes = self.bytes, .references = references, .controls = self.controls };
    }
    fn retire(self: *Registry, index: usize, value: *Entry) void {
        std.debug.assert(value.closing and value.references == 0);
        const coordinator = &self.coordinators[value.coordinator];
        std.debug.assert(coordinator.gate.?.finish(value.id));
        coordinator.count -= 1;
        coordinator.bytes -= value.bytes;
        self.count -= 1;
        self.bytes -= value.bytes;
        self.entries[index] = null;
    }
    fn retain(self: *Registry, index: usize, id: attempts.Id) !Resource {
        self.lock();
        defer self.mutex.unlock();
        const value = self.entry(index, id) orelse return error.InvalidRemoteAttempt;
        try self.check(value);
        if (value.references == self.limits.resource_references) return error.RemoteResourceCapacity;
        value.references += 1;
        return .{ .registry = self, .index = index, .id = id };
    }
};

pub const Lease = struct {
    registry: *Registry,
    index: usize,
    id: attempts.Id,
    live: bool = true,
    pub fn retainResource(self: *Lease) !Resource {
        if (!self.live) return error.Canceled;
        return self.registry.retain(self.index, self.id);
    }
    /// A descendant's reconciliation slot owns this reference after publication.
    /// Losing the request/response cannot make the parent physically terminal.
    pub fn retainRemoteChild(self: *Lease) !attempts.Retention {
        var resource = try self.retainResource();
        return resource.intoRetention();
    }
    pub fn check(self: *Lease) !void {
        if (!self.live) return error.Canceled;
        self.registry.lock();
        defer self.registry.mutex.unlock();
        const value = self.registry.entry(self.index, self.id) orelse return error.InvalidRemoteAttempt;
        try self.registry.check(value);
    }
    /// Apply the outcome of a fresh authoritative membership read at a verified
    /// resume boundary. Any rejection stops work without releasing its charge.
    pub fn revalidate(self: *Lease, result: anyerror!membership.Binding) !void {
        if (!self.live) return error.Canceled;
        self.registry.lock();
        defer self.registry.mutex.unlock();
        const value = self.registry.entry(self.index, self.id) orelse return error.InvalidRemoteAttempt;
        try self.registry.check(value);
        const binding = result catch |err| {
            Registry.stop(value, err);
            return self.registry.check(value);
        };
        const coordinator = membership.coordinatorIdentity(self.registry.local.metadata_incarnation, binding.coordinator_node_id) catch {
            Registry.stop(value, error.RemoteMembershipChanged);
            return self.registry.check(value);
        };
        if (binding.worker_node_id != self.registry.local.node_id or binding.store_id != self.registry.local.store_id or binding.root_incarnation != self.registry.local.root_incarnation or binding.reporter_incarnation != self.registry.local.reporter_incarnation or coordinator != self.id.coordinator) Registry.stop(value, error.RemoteMembershipChanged);
        try self.registry.check(value);
    }
    /// Borrow this exact platform-monotonic bound when constructing backend
    /// context. It can only tighten on retransmission, never refresh.
    pub fn deadlineNs(self: *Lease) !u64 {
        if (!self.live) return error.Canceled;
        self.registry.lock();
        defer self.registry.mutex.unlock();
        const value = self.registry.entry(self.index, self.id) orelse return error.InvalidRemoteAttempt;
        try self.registry.check(value);
        return value.deadline_ns;
    }
    pub fn cancellationSignal(self: *Lease) !*const std.atomic.Value(bool) {
        if (!self.live) return error.Canceled;
        self.registry.lock();
        defer self.registry.mutex.unlock();
        const value = self.registry.entry(self.index, self.id) orelse return error.InvalidRemoteAttempt;
        return &value.canceled;
    }
    pub fn close(self: *Lease) void {
        if (!self.live) return;
        self.live = false;
        self.registry.lock();
        defer self.registry.mutex.unlock();
        const value = self.registry.entry(self.index, self.id) orelse return;
        if (value.closing) return;
        value.closing = true;
        value.canceled.store(true, .release);
        value.references -= 1;
        if (value.references == 0) self.registry.retire(self.index, value);
    }
};
pub const Resource = struct {
    registry: *Registry,
    index: usize,
    id: attempts.Id,
    live: bool = true,
    pub fn intoRetention(self: *Resource) !attempts.Retention {
        if (!self.live) return error.Canceled;
        self.registry.lock();
        defer self.registry.mutex.unlock();
        const entry = self.registry.entry(self.index, self.id) orelse return error.InvalidRemoteAttempt;
        self.live = false;
        return .{ .context = self.registry, .index = self.index, .id = self.id, .release_fn = releaseRetained, .cancellation = &entry.canceled, .deadline_ns = entry.deadline_ns };
    }
    fn releaseRetained(raw: *anyopaque, index: usize, id: attempts.Id) void {
        var resource: Resource = .{ .registry = @ptrCast(@alignCast(raw)), .index = index, .id = id };
        resource.release();
    }
    /// Only after the referenced buffer/snapshot/task physically retires.
    pub fn release(self: *Resource) void {
        if (!self.live) return;
        self.live = false;
        self.registry.lock();
        defer self.registry.mutex.unlock();
        const value = self.registry.entry(self.index, self.id) orelse return;
        std.debug.assert(value.references > 0);
        value.references -= 1;
        if (value.references == 0) self.registry.retire(self.index, value);
    }
};

const test_local: membership.Local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
const test_limits: Limits = .{ .coordinators = 2, .attempts = 2, .per_coordinator_attempts = 1, .duplicate_window = 8, .bytes = 16 << 10, .per_coordinator_bytes = 8 << 10, .resource_references = 2 };
const TestClock = struct {
    var tick: u64 = 100;
    fn now() u64 {
        return tick;
    }
};
fn testRecovered(registry: *Registry) !void {
    const Ready = struct {
        fn quiescent(_: *const anyopaque) bool {
            return true;
        }
    };
    try registry.completeRecovery(.{ .context = &test_limits, .quiescent = Ready.quiescent });
}
fn testBinding(node: u64) membership.Binding {
    return .{ .coordinator_node_id = node, .worker_node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
}
fn testRequest(node: u64, generation: u64, sequence: u64, kind: wire.Kind) !wire.Verified {
    return .{ .context = .{
        .kind = kind,
        .id = .{ .coordinator = try membership.coordinatorIdentity(test_local.metadata_incarnation, node), .generation = generation, .operation = if (kind == .fence) 0 else 71, .sequence = if (kind == .fence) 0 else sequence },
        .worker_incarnation = 11,
        .remaining_ns = 1000,
        .charged_bytes = if (kind == .read) 8 << 10 else 0,
        .request_digest = @splat(7),
    }, .local_deadline_ns = TestClock.tick + 1000 };
}

test "remote worker holds physical buffer ownership through close fence and final release" {
    TestClock.tick = 100;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try fence_store.Store.create(std.testing.allocator, std.testing.io, tmp.dir, 11, 2);
    defer store.deinit();
    var registry = try Registry.init(std.testing.allocator, &store, test_local, test_limits, TestClock.now);
    defer registry.deinit() catch unreachable;
    try testRecovered(&registry);
    _ = try registry.fence(try testRequest(7, 1, 0, .fence), testBinding(7));
    try std.testing.expect(!(try store.initializationState()).publication_started);
    var lease = (try registry.begin(try testRequest(7, 1, 1, .read), testBinding(7))).started;
    try std.testing.expect((try store.initializationState()).publication_started);
    defer lease.close();
    const signal = try lease.cancellationSignal();
    var held = try lease.retainResource();
    defer held.release();
    try std.testing.expectError(error.RemoteResourceCapacity, lease.retainResource());
    const bytes = try std.testing.allocator.dupe(u8, "physical output remains owned after logical detach");
    var bytes_live = true;
    defer if (bytes_live) std.testing.allocator.free(bytes);
    lease.close();
    try std.testing.expect(signal.load(.acquire));
    try std.testing.expectError(error.RemoteWorkerBusy, registry.deinit());
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try registry.status(try testRequest(7, 1, 1, .status), testBinding(7)));
    try std.testing.expectEqual(@as(usize, 8 << 10), registry.snapshot().bytes);
    try std.testing.expectEqual(@as(usize, 1), registry.snapshot().references);
    try std.testing.expectError(error.RemoteFencePending, registry.fence(try testRequest(7, 2, 0, .fence), testBinding(7)));
    try std.testing.expectEqualStrings("physical output remains owned after logical detach", bytes);
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try registry.status(try testRequest(7, 1, 1, .status), testBinding(7)));
    std.testing.allocator.free(bytes);
    bytes_live = false;
    held.release();
    held.release();
    try std.testing.expectEqual(@as(usize, 0), registry.snapshot().bytes);
    _ = try registry.fence(try testRequest(7, 2, 0, .fence), testBinding(7));
    try std.testing.expectEqual(attempts.WorkerGate.Status.fenced, try registry.status(try testRequest(7, 1, 1, .status), testBinding(7)));
    try std.testing.expectError(error.RemoteGenerationFenced, registry.begin(try testRequest(7, 1, 1, .read), testBinding(7)));
}

test "remote worker rejected attempts cannot start later and other coordinators progress" {
    TestClock.tick = 100;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try fence_store.Store.create(std.testing.allocator, std.testing.io, tmp.dir, 11, 2);
    defer store.deinit();
    var registry = try Registry.init(std.testing.allocator, &store, test_local, test_limits, TestClock.now);
    defer registry.deinit() catch unreachable;
    try testRecovered(&registry);
    _ = try registry.fence(try testRequest(7, 1, 0, .fence), testBinding(7));
    _ = try registry.fence(try testRequest(8, 1, 0, .fence), testBinding(8));
    var first = (try registry.begin(try testRequest(7, 1, 1, .read), testBinding(7))).started;
    defer first.close();
    const denied = try testRequest(7, 1, 2, .read);
    try std.testing.expectEqual(Rejection.capacity, (try registry.begin(denied, testBinding(7))).rejected);
    var healthy = (try registry.begin(try testRequest(8, 1, 1, .read), testBinding(8))).started;
    defer healthy.close();
    try std.testing.expectEqual(@as(usize, 2), registry.snapshot().attempts);
    first.close();
    try std.testing.expect(try registry.begin(denied, testBinding(7)) == .terminal);
    var expired = try testRequest(7, 1, 3, .read);
    expired.local_deadline_ns = 99;
    try std.testing.expectEqual(Rejection.deadline, (try registry.begin(expired, testBinding(7))).rejected);
    expired.local_deadline_ns = 1000;
    try std.testing.expect(try registry.begin(expired, testBinding(7)) == .terminal);
    registry.closeAdmission();
    try std.testing.expectError(error.RemoteWorkerClosing, registry.begin(try testRequest(7, 1, 4, .read), testBinding(7)));
    healthy.close();
    _ = try registry.fence(try testRequest(7, 2, 0, .fence), testBinding(7));
}

test "remote worker preserves original deadline first cause and failed resume charges" {
    TestClock.tick = 100;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try fence_store.Store.create(std.testing.allocator, std.testing.io, tmp.dir, 11, 2);
    defer store.deinit();
    var registry = try Registry.init(std.testing.allocator, &store, test_local, test_limits, TestClock.now);
    defer registry.deinit() catch unreachable;
    try testRecovered(&registry);
    _ = try registry.fence(try testRequest(7, 1, 0, .fence), testBinding(7));
    var request = try testRequest(7, 1, 1, .read);
    request.local_deadline_ns = 200;
    var lease = (try registry.begin(request, testBinding(7))).started;
    defer lease.close();
    request.local_deadline_ns = 1000;
    try std.testing.expect(try registry.begin(request, testBinding(7)) == .duplicate_active);
    TestClock.tick = 200;
    try std.testing.expectError(error.DeadlineExceeded, lease.check());
    try std.testing.expectError(error.DeadlineExceeded, lease.revalidate(error.RemoteMembershipChanged));
    try std.testing.expectEqual(@as(usize, 8 << 10), registry.snapshot().bytes);
    lease.close();
    var second = (try registry.begin(try testRequest(7, 1, 2, .read), testBinding(7))).started;
    defer second.close();
    const signal = try second.cancellationSignal();
    var changed = testBinding(7);
    changed.reporter_incarnation += 1;
    try std.testing.expectError(error.RemoteMembershipChanged, second.revalidate(changed));
    try std.testing.expect(signal.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), registry.snapshot().attempts);
    try std.testing.expectError(error.RemoteFencePending, registry.fence(try testRequest(7, 2, 0, .fence), testBinding(7)));
    try std.testing.expectError(error.RemoteMembershipChanged, second.check());
    second.close();
    _ = try registry.fence(try testRequest(7, 2, 0, .fence), testBinding(7));
}

test "remote worker startup allocation failures leave durable owner usable" {
    TestClock.tick = 100;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try fence_store.Store.create(std.testing.allocator, std.testing.io, tmp.dir, 11, 2);
    defer store.deinit();
    const Harness = struct {
        fn run(allocator: std.mem.Allocator, durable: *fence_store.Store) !void {
            var registry = try Registry.init(allocator, durable, test_local, test_limits, TestClock.now);
            defer registry.deinit() catch unreachable;
            try std.testing.expectEqual(@as(usize, 0), registry.snapshot().attempts);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.run, .{&store});
    try std.testing.expectEqual(@as(u64, 0), try store.floor(try membership.coordinatorIdentity(test_local.metadata_incarnation, 7)));
}

test "remote worker startup cannot fence before external recovery owner releases its barrier" {
    TestClock.tick = 100;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try fence_store.Store.create(std.testing.allocator, std.testing.io, tmp.dir, 11, 2);
    defer store.deinit();
    var registry = try Registry.init(std.testing.allocator, &store, test_local, test_limits, TestClock.now);
    defer registry.deinit() catch unreachable;
    const External = struct {
        pending: bool = true,
        fn quiescent(raw: *const anyopaque) bool {
            return !@as(*const @This(), @ptrCast(@alignCast(raw))).pending;
        }
    };
    var external: External = .{};
    const proof = RecoveryCheck{ .context = &external, .quiescent = External.quiescent };
    try std.testing.expectError(error.RemoteWorkerRecoveryPending, registry.completeRecovery(proof));
    try std.testing.expectError(error.RemoteWorkerRecoveryPending, registry.fence(try testRequest(7, 1, 0, .fence), testBinding(7)));
    try std.testing.expectError(error.RemoteWorkerRecoveryPending, registry.begin(try testRequest(7, 1, 1, .read), testBinding(7)));
    try std.testing.expectEqual(attempts.WorkerGate.Status.unknown, try registry.status(try testRequest(7, 1, 1, .status), testBinding(7)));
    try std.testing.expectEqual(@as(u64, 0), try store.floor(try membership.coordinatorIdentity(test_local.metadata_incarnation, 7)));
    external.pending = false;
    try registry.completeRecovery(proof);
    _ = try registry.fence(try testRequest(7, 1, 0, .fence), testBinding(7));
    var lease = (try registry.begin(try testRequest(7, 1, 1, .read), testBinding(7))).started;
    lease.close();
}

test "remote worker cannot admit physical work before durable publication history" {
    const Fault = struct {
        var original: *const fn (?*anyopaque, std.Io.File) std.Io.File.SyncError!void = undefined;
        var calls: usize = 0;
        var fail_at: usize = 0;
        fn sync(userdata: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            calls += 1;
            if (calls == fail_at) return error.InputOutput;
            return original(userdata, file);
        }
    };
    const io = std.testing.io;
    for ([_]usize{ 1, 2 }) |fail_at| {
        TestClock.tick = 100;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        var store = try fence_store.Store.create(std.testing.allocator, io, tmp.dir, 11, 2);
        {
            var registry = try Registry.init(std.testing.allocator, &store, test_local, test_limits, TestClock.now);
            defer registry.deinit() catch unreachable;
            try testRecovered(&registry);
            _ = try registry.fence(try testRequest(7, 1, 0, .fence), testBinding(7));
            var vtable = io.vtable.*;
            Fault.original = vtable.fileSync;
            Fault.calls = 0;
            Fault.fail_at = fail_at;
            vtable.fileSync = Fault.sync;
            store.io = .{ .userdata = io.userdata, .vtable = &vtable };
            try std.testing.expectError(error.InputOutput, registry.begin(try testRequest(7, 1, 1, .read), testBinding(7)));
            try std.testing.expectEqual(@as(usize, 0), registry.snapshot().attempts);
            try std.testing.expectError(error.RemoteFenceStoreUncertain, store.initializationState());
            try std.testing.expectError(error.RemoteFenceStoreUncertain, registry.begin(try testRequest(7, 1, 1, .read), testBinding(7)));
            store.io = io;
        }
        store.deinit();
        store = try fence_store.Store.open(std.testing.allocator, io, tmp.dir, 11, 2);
        defer store.deinit();
        // Failure before rename left no work admitted; failure after rename
        // retained conservative publication history even without any work.
        try std.testing.expectEqual(fail_at == 2, (try store.initializationState()).publication_started);
    }
}

test "remote worker parent stays physically active while a descendant outcome is uncertain" {
    TestClock.tick = 100;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try fence_store.Store.create(std.testing.allocator, std.testing.io, tmp.dir, 11, 2);
    defer store.deinit();
    var registry = try Registry.init(std.testing.allocator, &store, test_local, test_limits, TestClock.now);
    defer registry.deinit() catch unreachable;
    try testRecovered(&registry);
    _ = try registry.fence(try testRequest(7, 1, 0, .fence), testBinding(7));
    var lease = (try registry.begin(try testRequest(7, 1, 1, .read), testBinding(7))).started;
    defer lease.close();
    var parent: ?attempts.Retention = try lease.retainRemoteChild();
    defer if (parent) |retained| retained.release();
    var slots: [1]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{.{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 1024 }};
    var ledger = try attempts.Coordinator.init(9, 1, &slots, &destinations, 1024);
    try ledger.acknowledgeFence(0, .{ .coordinator = 9, .generation = 1, .worker_incarnation = 22 });
    const descendant = try ledger.reserve(0, 71, 1024);
    try ledger.attachParent(descendant, &parent);
    try ledger.dispatch(descendant);
    ledger.transportLost(descendant);
    lease.close();
    try std.testing.expectEqual(@as(usize, 1), registry.snapshot().references);
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try registry.status(try testRequest(7, 1, 1, .status), testBinding(7)));
    try std.testing.expectError(error.RemoteFencePending, registry.fence(try testRequest(7, 2, 0, .fence), testBinding(7)));
    try std.testing.expectEqual(@as(usize, 8192), registry.snapshot().bytes);
    try std.testing.expect(try ledger.abandon(descendant));
    try std.testing.expectEqual(@as(usize, 1), registry.snapshot().references);
    try std.testing.expect(try ledger.terminal(descendant, 22));
    try std.testing.expectEqual(@as(usize, 0), registry.snapshot().bytes);
    _ = try registry.fence(try testRequest(7, 2, 0, .fence), testBinding(7));
}
