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

//! One joined control consumer per coordinator. The owning API must retain the
//! borrowed ledger, store, targets and executor until this worker has joined.
const std = @import("std");
const client_mod = @import("remote_attempt_client.zig");
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const clock = @import("antfly_platform").time;
const http = @import("../raft/transport/http_common.zig");
const Wait = @import("antfly_runtime_abi").execution_wait.Boundary;

pub const Policy = struct {
    interval_ms: u32 = 100,
    pass_duration_ns: u64 = 100 * std.time.ns_per_ms,
    rpc_timeout_ns: u64 = 50 * std.time.ns_per_ms,
    max_rpc_calls: usize = 4,
    max_scanned: usize = 64,

    pub fn validate(self: Policy) !void {
        if (self.interval_ms == 0 or self.interval_ms > 60_000 or self.pass_duration_ns == 0 or
            self.pass_duration_ns > std.time.ns_per_s or self.rpc_timeout_ns == 0 or
            self.rpc_timeout_ns > self.pass_duration_ns or self.max_rpc_calls == 0 or
            self.max_rpc_calls > 64 or self.max_scanned == 0 or self.max_scanned > 4096) return error.InvalidArgument;
    }
};
pub const Snapshot = struct {
    passes: u64,
    failed_passes: u64,
    generation_advances: u64,
    discovery_passes: u64,
    discovery_failures: u64,
    last_discovery_destination: ?usize,
    last: client_mod.ReconcileReport,
    stopping: bool,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    scheduler: *runnable.Scheduler,
    baseline: runnable.Scheduler.ControlBaseline,
    client: client_mod.Client,
    policy: Policy,
    cursor: client_mod.ReconcileCursor = .{},
    discovery_next: bool = false,
    discovery_destination: usize = 0,
    future: ?std.Io.Future(void) = null,
    stopping: std.atomic.Value(bool) = .init(false),
    cancellation: http.RequestCancellation = .{},
    joined: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    passes: u64 = 0,
    failed_passes: u64 = 0,
    generation_advances: u64 = 0,
    discovery_passes: u64 = 0,
    discovery_failures: u64 = 0,
    last_discovery_destination: ?usize = null,
    last: client_mod.ReconcileReport = .{},

    /// `io` must come from an independently reserved physical worker lease.
    /// Persistent metadata and maximum per-pass scratch remain charged while
    /// idle. Fixed consumers collectively leave cleanup and health capacity.
    /// Failure publishes no worker or binding.
    pub fn create(allocator: std.mem.Allocator, io: std.Io, scheduler: *runnable.Scheduler, client: client_mod.Client, policy: Policy) !*Runtime {
        try policy.validate();
        if (@min(@as(usize, 4), scheduler.config.max_operations / 2) < 2 or
            client.control_wait != null) return error.InvalidArgument;
        var baseline = scheduler.reserveControlBaseline(reservationBytes()) catch |err| return if (err == error.SchedulerClosed) err else error.CleanupCapacityUnavailable;
        errdefer baseline.release();
        try client.ledger.bindReconciler();
        errdefer client.ledger.unbindReconciler();
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator, .io = io, .scheduler = scheduler, .baseline = baseline, .client = client, .policy = policy };
        self.future = try io.concurrent(run, .{self});
        return self;
    }
    pub fn reservationBytes() u64 {
        return @sizeOf(Runtime) + @sizeOf(Round) + @sizeOf(std.heap.FixedBufferAllocator) + @sizeOf(@import("remote_endpoint_source.zig").Source.Control) + @sizeOf(?@import("remote_control_admission.zig").Gate.Lease) + @sizeOf(http.RequestCancellation);
    }
    pub fn reservedBytes(self: *const Runtime) u64 {
        return self.baseline.bytes;
    }
    fn lock(self: *Runtime) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    const control_gate = @import("remote_control_admission.zig").Gate;
    const Round = struct {
        runtime: *Runtime,
        deadline_ns: u64,
        execution: runnable.Execution = .{},
        scratch: [64 * 1024]u8 = undefined,
        fn canceled(raw: *const anyopaque) bool {
            const self: *const Round = @ptrCast(@alignCast(raw));
            return self.runtime.stopping.load(.acquire);
        }
        fn check(raw: *anyopaque) !void {
            const self: *Round = @ptrCast(@alignCast(raw));
            if (self.runtime.stopping.load(.acquire)) return error.Canceled;
            if (clock.monotonicNs() >= self.deadline_ns) return error.DeadlineExceeded;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *Round = @ptrCast(@alignCast(raw));
            try check(raw);
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *Round = @ptrCast(@alignCast(raw));
            try check(raw);
            try self.execution.enterWithCheck(.control, .{ .context = self, .check = check });
        }
        fn wait(self: *Round) Wait {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
    };
    fn pass(self: *Runtime) !void {
        const work_ns = @min(self.policy.pass_duration_ns, 250 * std.time.ns_per_ms);
        const started = clock.monotonicNs();
        const parent_deadline = started +| control_gate.max_wait_ns +| work_ns;
        var waiting: http.RequestCancellation = .{ .borrowed = &self.stopping, .execution_deadline_ns = parent_deadline };
        var permit = if (self.client.control_admission) |gate|
            try gate.acquireIdle(.{ .deadline_ns = started +| control_gate.max_wait_ns, .cancellation = &waiting })
        else
            null;
        defer if (permit) |*held| held.release();
        var round = Round{ .runtime = self, .deadline_ns = @min(parent_deadline, clock.monotonicNs() +| work_ns) };
        defer round.execution.finish();
        try round.execution.startCleanupRoot(self.scheduler, .{
            .io = self.io,
            .native_now_ns = clock.monotonicNs,
            .deadline_ns = round.deadline_ns,
            .cancellation = .{ .ptr = &round, .requested = Round.canceled },
            .retained_bytes = 0,
            .allow_wait = false,
        });
        var scratch = std.heap.FixedBufferAllocator.init(&round.scratch);
        var client = self.client;
        client.control_permit = if (permit) |*held| held.borrow() else null;
        var endpoint_lease = if (client.endpoint_source) |source| try source.current() else null;
        defer if (endpoint_lease) |*lease| lease.release();
        if (endpoint_lease) |*lease| client = client.withEndpointView(lease.view());
        client.control_allocator = scratch.allocator();
        client.control_wait = round.wait();
        client.now_ns = clock.monotonicNs;
        // A failed or slow probe cannot consume the next reconciliation turn.
        // One destination per discovery turn bounds scanning, RPC and scratch;
        // advance the cursor before I/O so an unavailable peer cannot monopolize it.
        if (client.capabilities != null and client.targets.len != 0) {
            const discovery_turn = self.discovery_next;
            self.discovery_next = !discovery_turn;
            if (discovery_turn) return self.discover(&client, &round);
        }
        const report = try client.reconcilePass(&self.cursor, .{
            .max_rpc_calls = self.policy.max_rpc_calls,
            .max_scanned = self.policy.max_scanned,
            .rpc_timeout_ns = self.policy.rpc_timeout_ns,
            .deadline_ns = round.deadline_ns,
            .cancellation = &self.cancellation,
        });
        self.lock();
        self.passes +|= 1;
        self.last = report;
        self.mutex.unlock();
        if (report.needs_generation_advance) {
            // At most one durable advance per timed pass; preserve every old
            // published charge and repeat destination fencing on later passes.
            try Round.check(&round);
            try client.advanceGeneration();
            self.lock();
            self.generation_advances +|= 1;
            self.mutex.unlock();
        }
    }
    fn discover(self: *Runtime, client: *client_mod.Client, round: *Round) !void {
        const cache = client.capabilities.?;
        const index = self.discovery_destination % client.targets.len;
        self.discovery_destination = (index + 1) % client.targets.len;
        self.lock();
        self.discovery_passes +|= 1;
        self.last_discovery_destination = index;
        self.mutex.unlock();
        errdefer {
            self.lock();
            self.discovery_failures +|= 1;
            self.mutex.unlock();
        }
        const authority = client.targets[index].authority;
        var probe = try cache.begin(index, authority);
        defer probe.abort() catch unreachable;
        // Invalidation preserves everEnabled: after first activation, failure
        // cannot restore unowned legacy dispatch. Existing resource copies keep
        // their original authority and cleanup capability.
        var invalidate = true;
        errdefer if (invalidate) cache.invalidate(index, authority) catch {};
        const deadline = @min(round.deadline_ns, clock.monotonicNs() +| self.policy.rpc_timeout_ns);
        var delivery: http.RequestDeliveryTracker = .{};
        const observation = client.probeCapabilitiesTracked(index, deadline, &self.cancellation, &delivery) catch |err| {
            // A busy local control connection gives no new information about
            // the enrolled destination. Preserve its last verified families
            // only when the executor proves this probe was never sent.
            if (err == error.OutboundCapacityUnavailable and delivery.load() == .not_sent)
                invalidate = false;
            return err;
        };
        try Round.check(round);
        try probe.publish(observation);
    }
    fn run(self: *Runtime) void {
        while (!self.stopping.load(.acquire)) {
            self.pass() catch {
                if (!self.stopping.load(.acquire)) {
                    self.lock();
                    self.failed_passes +|= 1;
                    self.mutex.unlock();
                }
            };
            // Only the persistent byte reservation survives idle; no operation,
            // CPU grant or per-attempt task is held.
            self.io.sleep(.fromMilliseconds(self.policy.interval_ms), .awake) catch return;
        }
    }
    pub fn snapshot(self: *Runtime) Snapshot {
        self.lock();
        defer self.mutex.unlock();
        return .{ .passes = self.passes, .failed_passes = self.failed_passes, .generation_advances = self.generation_advances, .discovery_passes = self.discovery_passes, .discovery_failures = self.discovery_failures, .last_discovery_destination = self.last_discovery_destination, .last = self.last, .stopping = self.stopping.load(.acquire) };
    }
    pub fn close(self: *Runtime) void {
        self.stopping.store(true, .release);
        self.cancellation.cancel();
    }
    pub fn join(self: *Runtime) void {
        if (self.joined) return;
        self.close();
        self.future.?.cancel(self.io);
        self.future = null;
        self.client.ledger.unbindReconciler();
        self.joined = true;
    }
    pub fn deinit(self: *Runtime) void {
        self.join();
        const allocator = self.allocator;
        var baseline = self.baseline;
        allocator.destroy(self);
        baseline.release();
    }
};

test "remote attempt client discovery rotates unavailable peers without starving reconciliation at one RPC per pass" {
    const attempts = @import("../common/remote_attempts.zig");
    const durable = @import("../common/remote_fence_store.zig");
    const publication = @import("remote_capability_publication.zig").Cache;
    const replies = @import("../common/remote_attempt_reply.zig");
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try durable.Store.create(a, std.testing.io, tmp.dir, 77, 2);
    defer store.deinit();
    var slots: [2]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{
        .{ .worker_incarnation = 11, .max_attempts = 1, .max_bytes = 4096 },
        .{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 4096 },
    };
    var ledger = try store.startCoordinator(17, 0, &slots, &destinations, 8192);
    const first: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = @splat(7) };
    var second = first;
    second.local.node_id = 10;
    second.local.store_id = 14;
    second.local.root_incarnation = 22;
    const targets = [_]client_mod.Target{ .{ .base_uri = "http://worker-a", .authority = first }, .{ .base_uri = "http://worker-b", .authority = second } };
    var scheduler = try runnable.Scheduler.init(.{ .slots = 2, .control_bytes = 1 << 20, .read_isolation = false });
    defer scheduler.close();
    const cache = try publication.create(a, &scheduler, &.{ first, second }, clock.monotonicNs);
    defer {
        cache.close();
        cache.deinit() catch unreachable;
    }
    const Transport = struct {
        calls: usize = 0,
        probes: usize = 0,
        fn execute(raw: *anyopaque, _: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const is_probe = std.mem.endsWith(u8, request.uri, "/capabilities");
            try std.testing.expectEqual(self.calls % 2 == 1, is_probe);
            try std.testing.expect(request.execution_wait != null);
            try std.testing.expect(request.cancellation.?.execution_deadline_ns.? > clock.monotonicNs());
            if (is_probe) {
                const destination: usize = if (std.mem.startsWith(u8, request.uri, "http://worker-a/")) 0 else 1;
                try std.testing.expectEqual(self.probes % 2, destination);
                self.probes += 1;
            }
            self.calls += 1;
            return error.ConnectionResetByPeer;
        }
    };
    var transport: Transport = .{};
    const executor: http.RequestExecutor = .{ .ptr = &transport, .clock_io = .init(&std.testing.io), .vtable = &.{ .execute = Transport.execute } };
    // Drive the production pass synchronously to assert exact bounded turns;
    // the native/imported periodic fixture covers the real worker and join.
    var runtime: Runtime = .{ .allocator = a, .io = std.testing.io, .scheduler = &scheduler, .baseline = try scheduler.reserveControlBaseline(Runtime.reservationBytes()), .client = .{ .ledger = &ledger, .store = &store, .targets = &targets, .signing = .{ .secret = "0123456789abcdef0123456789abcdef", .issuer = "cluster" }, .read_allocator = a, .control_allocator = a, .read_executor = executor, .control_executor = executor, .capabilities = cache }, .policy = .{ .max_rpc_calls = 1, .max_scanned = 4 } };
    defer runtime.baseline.release();
    for (0..12) |_| runtime.pass() catch |err| {
        try std.testing.expectEqual(error.ConnectionResetByPeer, err);
    };
    try std.testing.expectEqual(@as(usize, 12), transport.calls);
    try std.testing.expectEqual(@as(usize, 6), transport.probes);
    try std.testing.expectEqual(@as(u64, 6), runtime.snapshot().passes);
    try std.testing.expectEqual(@as(u64, 6), runtime.snapshot().discovery_passes);
    try std.testing.expectEqual(@as(u64, 6), runtime.snapshot().discovery_failures);
    try std.testing.expectEqual(@as(?usize, 1), runtime.snapshot().last_discovery_destination);
    try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(cache.reservedBytes() + runtime.reservedBytes(), scheduler.stats().retained_bytes);
}

test "remote attempt client discovery preserves eligibility only on proven unsent control saturation" {
    const attempts = @import("../common/remote_attempts.zig");
    const durable = @import("../common/remote_fence_store.zig");
    const publication = @import("remote_capability_publication.zig").Cache;
    const replies = @import("../common/remote_attempt_reply.zig");
    const caps = @import("../common/remote_read_capabilities.zig");
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var store = try durable.Store.create(a, std.testing.io, tmp.dir, 77, 1);
    defer store.deinit();
    var slots: [2]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{.{ .worker_incarnation = 11, .max_attempts = 2, .max_bytes = 8192 }};
    var ledger = try store.startCoordinator(17, 0, &slots, &destinations, 8192);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const authority: replies.Authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 }, .public_key = pair.public_key.toBytes() };
    const signer = try replies.Signer.init(authority.local, authority.public_key, @splat(7));
    const targets = [_]client_mod.Target{.{ .base_uri = "http://127.0.0.1:1", .authority = authority }};
    var scheduler = try runnable.Scheduler.init(.{ .slots = 2, .control_bytes = 1 << 20, .read_isolation = false });
    defer scheduler.close();
    const cache = try publication.create(a, &scheduler, &.{authority}, clock.monotonicNs);
    defer {
        cache.close();
        cache.deinit() catch unreachable;
    }
    const Transport = struct {
        delivery: http.RequestDeliveryTracker.State = .unknown,
        failure: ?anyerror = error.OutboundCapacityUnavailable,
        families: caps.Families = .{},
        forge_signature: bool = false,
        cancel_parent: ?*http.RequestCancellation = null,
        ledger: *attempts.Coordinator,
        signer: *const replies.Signer,
        calls: usize = 0,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (request.delivery_tracker) |tracker| switch (self.delivery) {
                .unknown => {},
                .not_sent => tracker.markNotSent(),
                .may_have_been_sent => tracker.markMayHaveBeenSent(),
            };
            if (self.failure) |failure| return failure;
            const wire = @import("../common/remote_attempt_wire.zig");
            const generation = self.ledger.generationState();
            const context: wire.Context = .{ .kind = .capabilities, .id = .{ .coordinator = generation.coordinator, .generation = generation.generation, .operation = 0, .sequence = 0 }, .worker_incarnation = self.signer.local.root_incarnation, .remaining_ns = 1, .charged_bytes = 0, .request_digest = wire.requestDigest("POST", @import("../internal/routes.zig").remote_attempt_capabilities, request.body) };
            var reply = try caps.Reply.sign(self.signer, context, self.families);
            if (self.forge_signature) reply.signature[0] ^= 1;
            const encoded = reply.encode();
            if (self.cancel_parent) |parent| parent.cancel();
            return .{ .status = 200, .body = try alloc.dupe(u8, &encoded) };
        }
    };
    var transport: Transport = .{ .ledger = &ledger, .signer = &signer };
    const executor: http.RequestExecutor = .{ .ptr = &transport, .clock_io = .init(&std.testing.io), .vtable = &.{ .execute = Transport.execute } };
    var runtime: Runtime = .{ .allocator = a, .io = std.testing.io, .scheduler = &scheduler, .baseline = try scheduler.reserveControlBaseline(Runtime.reservationBytes()), .client = .{ .ledger = &ledger, .store = &store, .targets = &targets, .signing = .{ .secret = "0123456789abcdef0123456789abcdef", .issuer = "cluster" }, .read_allocator = a, .control_allocator = a, .read_executor = executor, .control_executor = executor, .capabilities = cache }, .policy = .{ .pass_duration_ns = std.time.ns_per_s, .rpc_timeout_ns = std.time.ns_per_s } };
    defer runtime.baseline.release();
    const Case = struct { delivery: http.RequestDeliveryTracker.State, failure: anyerror, retained: bool };
    const cases = [_]Case{
        .{ .delivery = .not_sent, .failure = error.OutboundCapacityUnavailable, .retained = true },
        .{ .delivery = .unknown, .failure = error.OutboundCapacityUnavailable, .retained = false },
        .{ .delivery = .may_have_been_sent, .failure = error.OutboundCapacityUnavailable, .retained = false },
        .{ .delivery = .not_sent, .failure = error.ConnectionResetByPeer, .retained = false },
        .{ .delivery = .may_have_been_sent, .failure = error.InvalidRemoteReplySignature, .retained = false },
    };
    for (cases) |case| {
        var seed = try cache.begin(0, authority);
        try seed.publish(.{ .authority = authority, .families = .{ .group_query = true, .retained_read = true }, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s });
        transport.delivery = case.delivery;
        transport.failure = case.failure;
        const before = transport.calls;
        var round: Runtime.Round = .{ .runtime = &runtime, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        try std.testing.expectError(case.failure, runtime.discover(&runtime.client, &round));
        try std.testing.expectEqual(before + 1, transport.calls);
        const current = try cache.snapshot(0, authority);
        try std.testing.expectEqual(case.retained, current.group_query);
        try std.testing.expectEqual(case.retained, current.retained_read);
        try std.testing.expect(cache.everEnabled(false) and cache.everEnabled(true));
        // Every ticket has returned; no retry or data-attempt publication.
        var after = try cache.begin(0, authority);
        try after.abort();
        try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().bytes);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    }
    // Exercise the production HTTP executor and its actual fail-fast outbound
    // admission too. Hold its only endpoint lease: the discovery attempt must
    // refuse locally before connect/send, without consuming another charge.
    {
        var seed = try cache.begin(0, authority);
        try seed.publish(.{ .authority = authority, .families = .{ .group_query = true, .retained_read = true }, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s });
        var outbound = @import("httpx").OutboundAdmission.init(a, std.testing.io, .{ .max_active = 1, .max_per_endpoint = 1, .max_waiters = 0, .max_wait_bytes = 0, .max_wait_ms = 0 });
        defer outbound.deinit();
        var occupied = try outbound.acquire("127.0.0.1", 1, .{});
        defer occupied.release();
        var physical: @import("../common/http/std_http_executor.zig").StdHttpExecutor = undefined;
        try physical.initInPlaceFallible(a, .{ .io_concurrent_limit = 8, .pool_max_connections = 1, .pool_max_per_host = 1, .pool_max_waiters = 0, .pool_max_wait_bytes = 0, .pool_max_wait_ms = 0, .outbound_admission = &outbound });
        defer physical.deinit();
        var actual_client = runtime.client;
        actual_client.control_executor = physical.executor();
        var round: Runtime.Round = .{ .runtime = &runtime, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        try std.testing.expectError(error.OutboundCapacityUnavailable, runtime.discover(&actual_client, &round));
        try std.testing.expectEqualDeep(caps.Families{ .group_query = true, .retained_read = true }, try cache.snapshot(0, authority));
        try std.testing.expectEqual(@as(usize, 1), outbound.snapshot().active);
        try std.testing.expectEqual(@as(usize, 0), outbound.snapshot().waiting);
        try std.testing.expectEqual(@as(usize, 0), ledger.snapshot().bytes);
    }
    // Genuine verification failures and cancellation after a signed reply
    // still withdraw eligibility; a previous grant cannot bypass these fences.
    transport.failure = null;
    transport.delivery = .may_have_been_sent;
    transport.families = .{ .group_query = true, .retained_read = true };
    for ([_]bool{ true, false }) |forged| {
        var previous = try cache.begin(0, authority);
        try previous.publish(.{ .authority = authority, .families = transport.families, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s });
        transport.forge_signature = forged;
        transport.cancel_parent = if (forged) null else &runtime.cancellation;
        var round: Runtime.Round = .{ .runtime = &runtime, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        try std.testing.expectError(if (forged) error.InvalidRemoteReplySignature else error.Canceled, runtime.discover(&runtime.client, &round));
        try std.testing.expectEqualDeep(caps.Families{}, try cache.snapshot(0, authority));
        runtime.cancellation.cancelled.store(false, .release);
    }
    transport.forge_signature = false;
    transport.cancel_parent = null;
    // Local saturation cannot create a grant when none was published.
    transport.failure = error.OutboundCapacityUnavailable;
    transport.delivery = .not_sent;
    {
        var round: Runtime.Round = .{ .runtime = &runtime, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        try std.testing.expectError(error.OutboundCapacityUnavailable, runtime.discover(&runtime.client, &round));
        try std.testing.expectEqualDeep(caps.Families{}, try cache.snapshot(0, authority));
    }
    // Fresh signed absence still revokes eligibility, and a later fresh signed
    // observation can grant it again. Neither observation clears the latch.
    var seed = try cache.begin(0, authority);
    try seed.publish(.{ .authority = authority, .families = .{ .group_query = true, .retained_read = true }, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s });
    transport.failure = null;
    transport.delivery = .may_have_been_sent;
    for ([_]bool{ false, true }) |enabled| {
        transport.families = .{ .group_query = enabled, .retained_read = enabled };
        var round: Runtime.Round = .{ .runtime = &runtime, .deadline_ns = clock.monotonicNs() + std.time.ns_per_s };
        try runtime.discover(&runtime.client, &round);
        try std.testing.expectEqualDeep(transport.families, try cache.snapshot(0, authority));
        try std.testing.expect(cache.everEnabled(false) and cache.everEnabled(true));
    }
}
