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

//! Stable ownership from admitted internal-query work through buffered output.
//! Join every physical child before handoff; response retirement frees its body
//! before releasing this owner and the allocator group backing that body.
const std = @import("std");
const sources = @import("antfly_local_sources");
const scheduler = sources.common_request_admission.runnable_scheduler;
const operation = sources.api_operation;
const memory = @import("../common/read_memory.zig");
const httpx = @import("httpx");
const runtime = @import("antfly_runtime_abi");
const worker = @import("../common/remote_worker.zig");
const attempts = @import("../common/remote_attempts.zig");
const wire = @import("../common/remote_attempt_wire.zig");
const membership = @import("../common/remote_membership.zig");

pub const MembershipAuthority = struct {
    context: *anyopaque,
    refresh: *const fn (*anyopaque, wire.Verified, operation.RequestContext, membership.Local) anyerror!membership.Binding,
};

pub const Owner = struct {
    backing: std.mem.Allocator,
    node: ?*scheduler.NodeOwner,
    pool: ?*memory.Pool,
    context: operation.RequestContext,
    original_cancellation: ?operation.CancellationToken,
    cancellation_mutex: std.atomic.Mutex = .unlocked,
    stopped: std.atomic.Value(bool) = .init(false),
    remote_signal: ?*const std.atomic.Value(bool) = null,
    remote_lease: ?worker.Lease = null,
    remote_verified: ?wire.Verified = null,
    remote_prepared: ?wire.Verified = null,
    retained_input_bytes: usize = 0,
    membership_authority: ?MembershipAuthority = null,
    fallback: @import("../common/allocation_budget.zig").Shared,
    response_bound: bool = false,
    remote_children_started: std.atomic.Value(bool) = .init(false),
    remote_dispatch: ?@import("retained_query_dispatch.zig").Dispatcher = null,
    execution: scheduler.Execution = .{},
    group: ?memory.Group = null,
    read: @import("runnable_local_read.zig").Read = undefined,
    team: ?runtime.runnable_abi.Team = null,
    references: std.atomic.Value(usize) = .init(1),

    pub fn create(backing: std.mem.Allocator, node: ?*scheduler.NodeOwner, pool: ?*memory.Pool, input: operation.RequestContext) !*Owner {
        const context = try input.platformDeadline();
        try context.ensureActive();
        if (node) |owner| try owner.retain();
        errdefer if (node) |owner| owner.release();
        const self = try backing.create(Owner);
        self.* = .{ .backing = backing, .node = node, .pool = pool, .context = context, .original_cancellation = context.cancellation, .fallback = .{ .child = backing, .limit = std.math.maxInt(usize) } };
        self.context.cancellation = .{ .ptr = self, .is_cancelled_fn = canceled };
        return self;
    }
    fn lockCancellation(self: *Owner) void {
        while (!self.cancellation_mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *Owner = @ptrCast(@alignCast(@constCast(raw)));
        self.lockCancellation();
        defer self.cancellation_mutex.unlock();
        if (self.stopped.load(.acquire)) return true;
        if (self.remote_signal) |signal| if (signal.load(.acquire)) return true;
        if (self.original_cancellation) |original| if (original.isCancelled()) {
            self.stopped.store(true, .release);
            return true;
        };
        return false;
    }
    fn detachIngress(self: *Owner) void {
        self.lockCancellation();
        defer self.cancellation_mutex.unlock();
        self.original_cancellation = null;
        self.stopped.store(true, .release);
    }
    pub fn start(self: *Owner, server: anytype, io: std.Io, input_bytes: usize) !void {
        return self.startClass(server, io, input_bytes, .query);
    }
    pub fn startClass(self: *Owner, server: anytype, io: std.Io, input_bytes: usize, class: scheduler.Class) !void {
        try self.context.ensureActive();
        self.retained_input_bytes = input_bytes;
        try server.startSqlRunnable(&self.execution, .{
            .io = io,
            .native_now_ns = @import("antfly_platform").time.monotonicNs,
            .deadline_ns = self.context.deadline_ns,
            .cancellation = .{ .ptr = &self.context.cancellation, .requested = struct {
                fn requested(raw: *const anyopaque) bool {
                    return @as(*const operation.CancellationToken, @ptrCast(@alignCast(raw))).isCancelled();
                }
            }.requested },
            .retained_bytes = @as(u64, input_bytes) +| @sizeOf(Owner),
        }, class);
        if (self.pool) |pool| self.group = try memory.Group.init(pool, .general, pool.limits.bytes[0]);
        if (self.execution.entry != null) {
            self.read = .{ .execution = &self.execution, .allocator = self.allocator() };
            self.team = try self.read.team();
            self.context.execution_team = &self.team.?;
        }
    }
    /// Installed only by trusted native bootstrap. Copies in physical children
    /// borrow this stable owner; only publication moves an independently held ref.
    pub fn bindRemoteQueries(self: *Owner, client: @import("remote_attempt_client.zig").Client) !void {
        if (!client.reads_ready) return error.DistributedQueryUnavailable;
        if (self.team == null or self.remote_dispatch != null) return error.InvalidRunnableTransition;
        self.remote_dispatch = try @import("retained_query_dispatch.zig").Dispatcher.init(client, self, remoteDeadline, retainDispatchParent);
        self.context.execution_remote_query = self.remote_dispatch.?.executor();
    }
    fn remoteDeadline(raw: *anyopaque) !?u64 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        return self.context.deadline_ns;
    }
    fn retainDispatchParent(raw: *anyopaque, deadline: u64) !attempts.Retention {
        const self: *Owner = @ptrCast(@alignCast(raw));
        var parent = try self.retainQueryParent();
        parent.deadline_ns = if (parent.deadline_ns) |original| @min(original, deadline) else deadline;
        return parent;
    }
    fn retainQueryParent(self: *Owner) !attempts.Retention {
        if (self.remote_lease != null) return self.retainRemoteChild();
        try self.context.ensureActive();
        self.remote_children_started.store(true, .release);
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        return .{ .context = self, .index = 0, .id = .{ .coordinator = 0, .generation = 0, .operation = 0, .sequence = 0 }, .release_fn = releaseQueryParent, .cancellation = &self.stopped, .deadline_ns = self.context.deadline_ns };
    }
    fn releaseQueryParent(raw: *anyopaque, _: usize, _: attempts.Id) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.releaseReference();
    }
    pub fn allocator(self: *Owner) std.mem.Allocator {
        return if (self.group) |*group| group.allocator() else self.fallback.allocator();
    }
    pub const RemoteAdmission = enum { started, duplicate_active, terminal, capacity, deadline };
    /// Bound authoritative membership scratch before its first allocation.
    /// Input remains physically retained outside the query allocator and must
    /// be deducted from the same signed allowance. Reuse cannot refresh a receipt.
    pub fn prepareRemote(self: *Owner, verified: wire.Verified) !void {
        if (self.remote_lease != null or self.team == null) return error.InvalidRunnableTransition;
        if (self.remote_prepared) |prepared| {
            if (!std.meta.eql(prepared, verified)) return error.InvalidRunnableTransition;
            return;
        }
        const bytes = std.math.cast(usize, verified.context.charged_bytes) orelse return error.InvalidRemoteAttempt;
        const retained = std.math.add(usize, @sizeOf(Owner), self.retained_input_bytes) catch return error.InvalidRemoteAttempt;
        if (bytes <= retained) return error.InvalidRemoteAttempt;
        if (self.group) |*group| {
            if (group.allocation_started or group.used != 0) return error.InvalidRunnableTransition;
            group.limit = @min(group.limit, bytes - retained);
        } else {
            if (self.fallback.stats().used_bytes != 0) return error.InvalidRunnableTransition;
            self.fallback.limit = bytes - retained;
        }
        try self.tightenDeadline(verified.local_deadline_ns);
        self.remote_prepared = verified;
    }
    /// Verified context and membership must come from the trusted ingress cut.
    /// Admit before query allocations, snapshots or physical children exist.
    /// Prepared membership scratch must already have been freed.
    pub fn admitRemote(self: *Owner, registry: *worker.Registry, verified: wire.Verified, binding: membership.Binding, authority: MembershipAuthority) !RemoteAdmission {
        try self.prepareRemote(verified);
        if (self.group) |*group| {
            if (group.used != 0) return error.InvalidRunnableTransition;
        } else if (self.fallback.stats().used_bytes != 0) return error.InvalidRunnableTransition;
        var wait: @import("runnable_fanout_owner.zig").RemoteWait = .{ .borrow = self.team.?.borrow };
        const admission = try registry.beginWithWait(verified, binding, wait.boundary());
        switch (admission) {
            .started => |lease| {
                self.remote_lease = lease;
                errdefer {
                    self.remote_lease.?.close();
                    self.remote_lease = null;
                    self.remote_signal = null;
                    self.read.validation = null;
                    self.stopped.store(true, .release);
                }
                self.remote_signal = try self.remote_lease.?.cancellationSignal();
                self.remote_verified = verified;
                self.membership_authority = authority;
                self.read.validation = .{ .context = self, .check = revalidateRemote };
                try self.read.validateCurrent();
                return .started;
            },
            .duplicate_active => return .duplicate_active,
            .terminal => return .terminal,
            .rejected => |reason| return if (reason == .capacity) .capacity else .deadline,
        }
    }
    fn revalidateRemote(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        var lease = self.remote_lease orelse return error.InvalidRunnableTransition;
        const authority = self.membership_authority orelse return error.InvalidRunnableTransition;
        var team: runtime.runnable_abi.Team = .{ .borrow = borrow, .maximum_slots = borrow.cpu_slots, .allocator = self.allocator() };
        var request = self.context;
        request.execution_team = &team;
        try lease.revalidate(authority.refresh(authority.context, self.remote_verified.?, request, lease.registry.local));
        try self.context.ensureActive();
    }
    /// Borrow only during this authenticated request's native calls. Published
    /// receiver resources acquire the composite child hold, which also pins
    /// this query heap and execution after response/ingress retirement.
    pub fn retainedReadParent(self: *Owner) !?@import("retained_read_owner.zig").RequestParent {
        if (self.remote_lease == null) return null;
        const id = try checkRetainedReadParent(self);
        return .{ .ptr = self, .id = id, .check = checkRetainedReadParent, .retain = retainRetainedReadParent };
    }
    fn checkRetainedReadParent(raw: *anyopaque) !attempts.Id {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.context.ensureActive();
        const lease = if (self.remote_lease) |*value| value else return error.InvalidRunnableTransition;
        try lease.check();
        return lease.id;
    }
    fn retainRetainedReadParent(raw: *anyopaque) !attempts.Retention {
        const self: *Owner = @ptrCast(@alignCast(raw));
        _ = try checkRetainedReadParent(self);
        return self.retainRemoteChild();
    }

    /// Composite hold pins worker authority, query heap and runnable metadata.
    /// Its cancellation pointers are owned atomics, never HTTP-context callbacks.
    pub fn retainRemoteChild(self: *Owner) !attempts.Retention {
        try self.context.ensureActive();
        const lease = if (self.remote_lease) |*value| value else return error.InvalidRunnableTransition;
        const resource = try lease.retainRemoteChild();
        self.remote_children_started.store(true, .release);
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        return .{ .context = self, .index = resource.index, .id = resource.id, .release_fn = releaseChild, .cancellation = &self.stopped, .secondary_cancellation = self.remote_signal, .deadline_ns = self.context.deadline_ns };
    }
    fn releaseChild(raw: *anyopaque, index: usize, id: attempts.Id) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        var resource: worker.Resource = .{ .registry = self.remote_lease.?.registry, .index = index, .id = id };
        resource.release();
        self.releaseReference();
    }
    pub fn tightenDeadline(self: *Owner, deadline: ?u64) !void {
        if (deadline) |value| {
            const tightened = @min(self.context.deadline_ns orelse value, value);
            if (self.remote_children_started.load(.acquire) and tightened != self.context.deadline_ns) return error.InvalidRunnableTransition;
            self.context.deadline_ns = tightened;
            try self.execution.tightenDeadline(self.context.deadline_ns.?);
        }
        try self.context.ensureActive();
    }
    pub fn parkOutput(self: *Owner) void {
        const entry = if (self.execution.entry) |*value| value else return;
        std.debug.assert(entry.children == 0);
        const owner = entry.scheduler orelse return;
        if (owner.state(entry) == .running) self.execution.park(.output_blocked) catch {};
    }
    pub fn retainResponse(self: *Owner, response: *httpx.Response) !void {
        if (response.retirement != null or self.response_bound) return error.InvalidRunnableTransition;
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        response.retirement = .{ .ptr = self, .release = releaseOpaque };
        self.response_bound = true;
    }
    fn releaseOpaque(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.detachIngress();
        self.releaseReference();
    }
    pub fn release(self: *Owner) void {
        if (!self.response_bound) self.detachIngress();
        self.releaseReference();
    }
    fn releaseReference(self: *Owner) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous != 1) return;
        if (self.group) |*group| group.deinit();
        std.debug.assert(self.fallback.stats().used_bytes == 0);
        if (self.remote_dispatch) |*dispatch| dispatch.deinit();
        self.execution.finish();
        var lease = self.remote_lease;
        const node = self.node;
        const backing = self.backing;
        backing.destroy(self);
        if (node) |owner| owner.release();
        if (lease) |*value| value.close();
    }
};

const TestServer = struct {
    scheduler: scheduler.Scheduler,
    pub fn startSqlRunnable(self: *@This(), execution: *scheduler.Execution, options: sources.common_bounded_admission.Options, class: scheduler.Class) !void {
        try execution.start(&self.scheduler, options, class);
    }
};

test "internal query owner retains physical output memory after worker CPU is parked" {
    for ([_]enum { success, cancel, shutdown }{ .success, .cancel, .shutdown }) |outcome| {
        const alloc = std.testing.allocator;
        var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
        defer server.scheduler.close();
        var pool = try memory.Pool.init(alloc, null, .{ .bytes = .{ 4096, 1024, 1024 }, .groups = .{ 2, 2, 2 } });
        defer pool.deinit();
        var canceled: std.atomic.Value(bool) = .init(false);
        const owner = try Owner.create(alloc, null, &pool, .{ .cancellation = .fromAtomic(&canceled) });
        var initial = true;
        defer if (initial) owner.release();
        try owner.start(&server, std.testing.io, 23);
        const team = owner.context.execution_team.?;
        const checkpoint = team.borrow.checkpoint(team.borrow.context);
        if (!checkpoint.isOk()) return runtime.error_abi.errorFromStatus(checkpoint);
        var response = try httpx.Response.fromText(owner.allocator(), 200, "retained query output");
        var live = true;
        defer if (live) response.deinit();
        owner.parkOutput();
        try owner.retainResponse(&response);
        owner.release();
        initial = false;
        try std.testing.expectEqual(@as(usize, 1), server.scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), pool.stats().groups[0]);
        try std.testing.expect(pool.stats().used[0] > 0);
        switch (outcome) {
            .success => {},
            .cancel => canceled.store(true, .release),
            .shutdown => server.scheduler.close(),
        }
        try std.testing.expectEqualStrings("retained query output", response.body.?);
        response.deinit();
        live = false;
        try std.testing.expectEqual(@as(usize, 0), pool.stats().used[0]);
        try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().operations);
    }
}

test "internal query owner decoded timeout is anchored before queueing and cannot extend authority" {
    const alloc = std.testing.allocator;
    var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
    defer server.scheduler.close();
    const owner = try Owner.create(alloc, null, null, .{});
    defer owner.release();
    try owner.start(&server, std.testing.io, 23);
    const deadline = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
    try owner.tightenDeadline(deadline);
    try owner.tightenDeadline(deadline + std.time.ns_per_s);
    try std.testing.expectEqual(deadline, owner.context.deadline_ns.?);
    try std.testing.expectEqual(deadline, owner.execution.entry.?.options.deadline_ns.?);
    const decoded = try sources.api_query_contract.queryExecutionDeadlineNsFromBodyAt(alloc, "{\"timeout_ms\":1}", 0);
    // A body timeout decoded after admission still includes time already spent
    // queued; it never starts a new millisecond budget at worker execution.
    try std.testing.expect(decoded != null);
    try std.testing.expectError(error.DeadlineExceeded, owner.tightenDeadline(decoded));
}

test "internal query owner allocation failure cleans up both admission and its heap group" {
    const Failure = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
            defer server.scheduler.close();
            var pool = try memory.Pool.init(alloc, null, .{ .bytes = .{ 4096, 1024, 1024 }, .groups = .{ 2, 2, 2 } });
            defer pool.deinit();
            const owner = try Owner.create(alloc, null, &pool, .{});
            defer owner.release();
            try owner.start(&server, std.testing.io, 23);
            var response = try httpx.Response.fromText(owner.allocator(), 200, "bounded physical output");
            defer response.deinit();
            owner.parkOutput();
            try owner.retainResponse(&response);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Failure.run, .{});
}

test "internal query owner output backpressure bounds operations while control can still run" {
    const alloc = std.testing.allocator;
    var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2, .max_operations = 4 }) };
    defer server.scheduler.close();
    var pool = try memory.Pool.init(alloc, null, .{ .bytes = .{ 16384, 1024, 1024 }, .groups = .{ 4, 2, 2 } });
    defer pool.deinit();
    var outputs: [4]?httpx.Response = @splat(null);
    defer for (&outputs) |*output| if (output.*) |*response| response.deinit();
    var rejected = false;
    for (&outputs) |*output| {
        const owner = try Owner.create(alloc, null, &pool, .{});
        defer owner.release();
        owner.start(&server, std.testing.io, 23) catch {
            rejected = true;
            break;
        };
        var response = try httpx.Response.fromText(owner.allocator(), 200, "slow receiver");
        errdefer response.deinit();
        owner.parkOutput();
        try owner.retainResponse(&response);
        output.* = response;
    }
    try std.testing.expect(rejected);
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().runnable_slots);
    var control: scheduler.Execution = .{};
    defer control.finish();
    try control.start(&server.scheduler, .{ .io = std.testing.io, .retained_bytes = 7 }, .control);
    control.finish();
    for (&outputs) |*output| if (output.*) |*response| {
        response.deinit();
        output.* = null;
    };
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().operations);
}

const RemoteFixture = struct {
    tmp: std.testing.TmpDir,
    store: @import("../common/remote_fence_store.zig").Store,
    registry: worker.Registry,
    validations: usize,
    validation_failure: ?anyerror,
    validation_server: ?*TestServer,
    metadata_children: bool,
    last_borrow: ?*anyopaque,
    replacement_binding: ?membership.Binding,
    deny_reentry: ?*std.atomic.Value(bool),
    fn authority(self: *@This()) MembershipAuthority {
        return .{ .context = self, .refresh = refresh };
    }
    fn refresh(raw: *anyopaque, verified: wire.Verified, request_: operation.RequestContext, expected: membership.Local) !membership.Binding {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try request_.ensureActive();
        try std.testing.expectEqualDeep(local, expected);
        try std.testing.expect(request_.deadline_ns.? <= verified.local_deadline_ns);
        const team = request_.execution_team orelse return error.TestMissingRunnable;
        self.validations += 1;
        self.last_borrow = team.borrow.context;
        var metadata_child: ?runtime.runnable_abi.Child = null;
        defer if (metadata_child) |*child| child.deinit();
        if (self.metadata_children) {
            const io = runtime.native_abi.IoBorrow.init(&std.testing.io);
            var child: runtime.runnable_abi.Child = undefined;
            try team.borrow.reserveChild(&.{ .io = &io, .retained_bytes = 7, .out_child = &child });
            metadata_child = child;
        }
        try checked(team.borrow.park(team.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
        if (self.validation_server) |server| {
            try std.testing.expect(server.scheduler.stats().resource_blocked >= 1);
            var control: scheduler.Execution = .{};
            defer control.finish();
            try control.start(&server.scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 7 }, .control);
        }
        if (metadata_child) |*child| {
            try child.activate();
            try checked(child.borrow.checkpoint(child.borrow.context));
            child.deinit();
            metadata_child = null;
        }
        try checked(team.borrow.enter(team.borrow.context, @backingInt(runtime.runnable_abi.Class.query)));
        if (self.deny_reentry) |signal| signal.store(true, .release);
        if (self.validation_failure) |err| return err;
        return self.replacement_binding orelse binding();
    }
    fn checked(status: runtime.error_abi.Status) !void {
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
    }
    const local: membership.Local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
    fn now() u64 {
        return @import("antfly_platform").time.monotonicNs();
    }
    fn fresh(_: *const anyopaque) bool {
        return true;
    }
    fn init(self: *@This()) !void {
        self.validations = 0;
        self.validation_failure = null;
        self.validation_server = null;
        self.metadata_children = false;
        self.last_borrow = null;
        self.replacement_binding = null;
        self.deny_reentry = null;
        self.tmp = std.testing.tmpDir(.{});
        errdefer self.tmp.cleanup();
        self.store = try @import("../common/remote_fence_store.zig").Store.create(std.testing.allocator, std.testing.io, self.tmp.dir, 11, 2);
        errdefer self.store.deinit();
        self.registry = try worker.Registry.init(std.testing.allocator, &self.store, local, .{ .coordinators = 2, .attempts = 4, .per_coordinator_attempts = 4, .duplicate_window = 8, .bytes = 1 << 20, .per_coordinator_bytes = 1 << 20, .resource_references = 8 }, now);
        errdefer self.registry.deinit() catch unreachable;
        // Only this just-created empty fixture supplies a fresh recovery proof.
        try self.registry.completeRecovery(.{ .context = self, .quiescent = fresh });
        _ = try self.registry.fence(try request(.fence, 0), binding());
    }
    fn deinit(self: *@This()) void {
        self.registry.deinit() catch unreachable;
        self.store.deinit();
        self.tmp.cleanup();
    }
    fn binding() membership.Binding {
        return .{ .coordinator_node_id = 7, .worker_node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
    }
    fn request(kind: wire.Kind, bytes: u64) !wire.Verified {
        return .{ .context = .{ .kind = kind, .id = .{ .coordinator = try membership.coordinatorIdentity(local.metadata_incarnation, 7), .generation = 1, .operation = if (kind == .fence) 0 else 71, .sequence = if (kind == .fence) 0 else 1 }, .worker_incarnation = 11, .remaining_ns = 5 * std.time.ns_per_s, .charged_bytes = bytes, .request_digest = @splat(7) }, .local_deadline_ns = now() + 5 * std.time.ns_per_s };
    }
};

test "internal query owner remote lease and query heap survive response retirement until descendants finish" {
    const alloc = std.testing.allocator;
    var fixture: RemoteFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
    defer server.scheduler.close();
    var pool = try memory.Pool.init(alloc, null, .{ .bytes = .{ 65536, 1024, 1024 }, .groups = .{ 3, 2, 2 } });
    defer pool.deinit();
    const Probe = struct {
        live: bool = true,
        calls: usize = 0,
        fn requested(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            std.debug.assert(self.live);
            self.calls += 1;
            return false;
        }
    };
    var probe: Probe = .{};
    const owner = try Owner.create(alloc, null, &pool, .{ .cancellation = .{ .ptr = &probe, .is_cancelled_fn = Probe.requested } });
    var ingress = true;
    defer if (ingress) owner.release();
    try owner.start(&server, std.testing.io, 23);
    const verified = try RemoteFixture.request(.read, 32768);
    try std.testing.expectEqual(Owner.RemoteAdmission.started, try owner.admitRemote(&fixture.registry, verified, RemoteFixture.binding(), fixture.authority()));
    var parent: ?attempts.Retention = try owner.retainRemoteChild();
    defer if (parent) |retained| retained.release();
    var slots: [1]?attempts.Attempt = undefined;
    var destinations = [_]attempts.Destination{.{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 1024 }};
    var ledger = try attempts.Coordinator.init(9, 1, &slots, &destinations, 1024);
    try ledger.acknowledgeFence(0, .{ .coordinator = 9, .generation = 1, .worker_incarnation = 22 });
    const child = try ledger.reserve(0, 72, 1024);
    try ledger.attachParent(child, &parent);
    try ledger.dispatch(child);
    ledger.transportLost(child);
    var response = try httpx.Response.fromText(owner.allocator(), 200, "query output remains physically owned");
    var output = true;
    defer if (output) response.deinit();
    owner.parkOutput();
    try owner.retainResponse(&response);
    const surviving_context = owner.context;
    owner.release();
    ingress = false;
    response.deinit();
    output = false;
    probe.live = false; // Simulates HTTP-context destruction after its response.
    const calls = probe.calls;
    try std.testing.expect(surviving_context.cancellation.isCancelled());
    try std.testing.expectEqual(calls, probe.calls);
    try std.testing.expectEqual(@as(usize, 1), pool.stats().groups[0]);
    try std.testing.expectEqual(@as(usize, 1), server.scheduler.stats().operations);
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try fixture.registry.status(try RemoteFixture.request(.status, 0), RemoteFixture.binding()));
    try std.testing.expectEqual(attempts.Coordinator.ReconcileAction{ .destination = 0, .id = child, .kind = .cancel }, ledger.reconciliationActionAt(2, RemoteFixture.now()).?);
    try std.testing.expectEqual(@as(usize, 1024), ledger.snapshot().bytes);
    try std.testing.expect(try ledger.terminal(child, 22));
    try std.testing.expectEqual(@as(usize, 0), pool.stats().groups[0]);
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), fixture.registry.snapshot().bytes);
    try std.testing.expectEqual(attempts.WorkerGate.Status.terminal, try fixture.registry.status(try RemoteFixture.request(.status, 0), RemoteFixture.binding()));
}

test "internal query owner remote admission enforces signed heap allowance with or without a read pool" {
    for ([_]bool{ false, true }) |pooled| {
        var fixture: RemoteFixture = undefined;
        try fixture.init();
        defer fixture.deinit();
        var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
        defer server.scheduler.close();
        var pool = try memory.Pool.init(std.testing.allocator, null, .{ .bytes = .{ 65536, 1024, 1024 }, .groups = .{ 3, 2, 2 } });
        defer pool.deinit();
        const owner = try Owner.create(std.testing.allocator, null, if (pooled) &pool else null, .{});
        defer owner.release();
        try owner.start(&server, std.testing.io, 23);
        const verified = try RemoteFixture.request(.read, @sizeOf(Owner) + 1024);
        try std.testing.expectEqual(Owner.RemoteAdmission.started, try owner.admitRemote(&fixture.registry, verified, RemoteFixture.binding(), fixture.authority()));
        // The 23 retained input bytes consume the same signed allowance.
        try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 1002));
        try std.testing.expectEqual(@as(usize, 1001), if (owner.group) |group| group.limit else owner.fallback.limit);
        const within_budget = try owner.allocator().alloc(u8, 900);
        owner.allocator().free(within_budget);
        const bytes = try owner.allocator().dupe(u8, "bounded remote scratch");
        owner.allocator().free(bytes);
        _ = try fixture.registry.cancelWithWait(try RemoteFixture.request(.cancel, 0), RemoteFixture.binding(), null);
        try std.testing.expectError(error.Canceled, owner.context.ensureActive());
        try std.testing.expectError(error.Canceled, owner.retainRemoteChild());
        try std.testing.expectEqual(@as(usize, 1), fixture.registry.snapshot().attempts);
    }
}

test "internal query owner duplicate remote admission never creates another physical lease" {
    var fixture: RemoteFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
    defer server.scheduler.close();
    const first = try Owner.create(std.testing.allocator, null, null, .{});
    var first_live = true;
    defer if (first_live) first.release();
    try first.start(&server, std.testing.io, 23);
    const verified = try RemoteFixture.request(.read, 32768);
    try std.testing.expectEqual(Owner.RemoteAdmission.started, try first.admitRemote(&fixture.registry, verified, RemoteFixture.binding(), fixture.authority()));
    var child: ?attempts.Retention = try first.retainRemoteChild();
    defer if (child) |retained| retained.release();
    try std.testing.expectError(error.InvalidRunnableTransition, first.tightenDeadline(verified.local_deadline_ns - 1));
    child.?.release();
    child = null;
    first.parkOutput();
    {
        const duplicate = try Owner.create(std.testing.allocator, null, null, .{});
        defer duplicate.release();
        try duplicate.start(&server, std.testing.io, 23);
        try std.testing.expectEqual(Owner.RemoteAdmission.duplicate_active, try duplicate.admitRemote(&fixture.registry, verified, RemoteFixture.binding(), fixture.authority()));
        try std.testing.expect(duplicate.remote_lease == null);
        try std.testing.expectEqual(@as(usize, 1), fixture.registry.snapshot().attempts);
    }
    first.release();
    first_live = false;
    const terminal = try Owner.create(std.testing.allocator, null, null, .{});
    defer terminal.release();
    try terminal.start(&server, std.testing.io, 23);
    try std.testing.expectEqual(Owner.RemoteAdmission.terminal, try terminal.admitRemote(&fixture.registry, verified, RemoteFixture.binding(), fixture.authority()));
    try std.testing.expectEqual(@as(usize, 0), fixture.registry.snapshot().attempts);
}

test "internal query owner revalidates membership after quantum and IO resumes without recursive metadata validation" {
    var fixture: RemoteFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2, .max_operations = 16, .checkpoint_budget = 1 }) };
    defer server.scheduler.close();
    fixture.validation_server = &server;
    fixture.metadata_children = true;
    const owner = try Owner.create(std.testing.allocator, null, null, .{});
    defer owner.release();
    try owner.start(&server, std.testing.io, 23);
    const receipt = try RemoteFixture.request(.read, 65536);
    _ = try owner.admitRemote(&fixture.registry, receipt, RemoteFixture.binding(), fixture.authority());
    try std.testing.expectEqual(@as(usize, 1), fixture.validations);
    const borrow = owner.team.?.borrow;
    try RemoteFixture.checked(borrow.checkpoint(borrow.context));
    try std.testing.expectEqual(@as(usize, 2), fixture.validations);
    try RemoteFixture.checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    try RemoteFixture.checked(borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.query)));
    try std.testing.expectEqual(@as(usize, 3), fixture.validations);
    const bytes = try owner.allocator().dupe(u8, "snapshot remains owned after membership revocation");
    defer owner.allocator().free(bytes);
    var replacement = RemoteFixture.binding();
    replacement.reporter_incarnation += 1;
    fixture.replacement_binding = replacement;
    try RemoteFixture.checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    try std.testing.expectError(error.RemoteMembershipChanged, RemoteFixture.checked(borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.query))));
    try std.testing.expectEqual(@as(usize, 4), fixture.validations);
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 65536), fixture.registry.snapshot().bytes);
    try std.testing.expectEqualStrings("snapshot remains owned after membership revocation", bytes);
    try std.testing.expectError(error.RemoteMembershipChanged, RemoteFixture.checked(borrow.checkpoint(borrow.context)));
    try std.testing.expectError(error.RemoteMembershipChanged, borrow.checkLifetime());
    try std.testing.expectEqual(@as(usize, 4), fixture.validations);
    var control: scheduler.Execution = .{};
    defer control.finish();
    try control.start(&server.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
}

test "internal query owner physical children and grandchildren independently validate membership" {
    var fixture: RemoteFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2, .max_operations = 16 }) };
    defer server.scheduler.close();
    fixture.validation_server = &server;
    const owner = try Owner.create(std.testing.allocator, null, null, .{});
    defer owner.release();
    try owner.start(&server, std.testing.io, 23);
    _ = try owner.admitRemote(&fixture.registry, try RemoteFixture.request(.read, 65536), RemoteFixture.binding(), fixture.authority());
    const borrow = owner.team.?.borrow;
    const io = runtime.native_abi.IoBorrow.init(&std.testing.io);
    var child: runtime.runnable_abi.Child = undefined;
    try borrow.reserveChild(&.{ .io = &io, .retained_bytes = 7, .out_child = &child });
    var child_live = true;
    defer if (child_live) child.deinit();
    try RemoteFixture.checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    const before = fixture.validations;
    try child.activate();
    try std.testing.expectEqual(before + 1, fixture.validations);
    try std.testing.expectEqual(child.borrow.context, fixture.last_borrow.?);
    var nested: runtime.runnable_abi.Child = undefined;
    try child.borrow.reserveChild(&.{ .io = &io, .retained_bytes = 7, .out_child = &nested });
    defer nested.deinit();
    try RemoteFixture.checked(child.borrow.park(child.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    const nested_before = fixture.validations;
    fixture.validation_failure = error.RemoteMembershipUnavailable;
    try std.testing.expectError(error.RemoteMembershipUnavailable, nested.activate());
    try std.testing.expectEqual(nested_before + 1, fixture.validations);
    try std.testing.expectEqual(nested.borrow.context, fixture.last_borrow.?);
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), fixture.registry.snapshot().attempts);
    nested.deinit();
    child.deinit();
    child_live = false;
}

test "internal query owner metadata validation preserves the caller CPU return restriction" {
    var fixture: RemoteFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
    defer server.scheduler.close();
    const owner = try Owner.create(std.testing.allocator, null, null, .{});
    defer owner.release();
    try owner.start(&server, std.testing.io, 23);
    _ = try owner.admitRemote(&fixture.registry, try RemoteFixture.request(.read, 32768), RemoteFixture.binding(), fixture.authority());
    var deny: std.atomic.Value(bool) = .init(false);
    fixture.deny_reentry = &deny;
    const Restriction = struct {
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const signal: *std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
            return if (signal.load(.acquire)) runtime.error_abi.statusFromError(error.Forbidden) else .ok;
        }
    };
    const borrow = owner.team.?.borrow;
    try RemoteFixture.checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    try std.testing.expectError(error.Forbidden, borrow.enterChecked(.query, &.{ .context = &deny, .check = Restriction.check }));
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), fixture.registry.snapshot().attempts);
    try std.testing.expectError(error.Forbidden, RemoteFixture.checked(borrow.checkpoint(borrow.context)));
}

test "internal query owner membership rejection fences retained descendants until physical join" {
    var fixture: RemoteFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2, .max_operations = 16 }) };
    defer server.scheduler.close();
    const owner = try Owner.create(std.testing.allocator, null, null, .{});
    defer owner.release();
    try owner.start(&server, std.testing.io, 23);
    _ = try owner.admitRemote(&fixture.registry, try RemoteFixture.request(.read, 65536), RemoteFixture.binding(), fixture.authority());
    const borrow = owner.team.?.borrow;
    const io = runtime.native_abi.IoBorrow.init(&std.testing.io);
    var child: runtime.runnable_abi.Child = undefined;
    try borrow.reserveChild(&.{ .io = &io, .retained_bytes = 7, .out_child = &child });
    defer child.deinit();
    try RemoteFixture.checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    try child.activate();
    try RemoteFixture.checked(child.borrow.park(child.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    const retained = server.scheduler.stats();
    fixture.validation_failure = error.RemoteMembershipChanged;
    try std.testing.expectError(error.RemoteMembershipChanged, RemoteFixture.checked(borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.query))));
    const validations = fixture.validations;
    // Membership revocation latches the shared lease cancellation signal.
    // Restoring metadata cannot resurrect a child of the rejected parent.
    fixture.validation_failure = null;
    try std.testing.expectError(error.Canceled, RemoteFixture.checked(child.borrow.enter(child.borrow.context, @backingInt(runtime.runnable_abi.Class.query))));
    try std.testing.expectError(error.Canceled, child.borrow.checkLifetime());
    try std.testing.expectEqual(validations, fixture.validations);
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained.operations, server.scheduler.stats().operations);
    try std.testing.expectEqual(retained.retained_bytes, server.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 65536), fixture.registry.snapshot().bytes);
    child.deinit();
    try std.testing.expectEqual(retained.operations - 1, server.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), fixture.registry.snapshot().attempts);
}

test "internal query owner prepares bounded authority scratch without refreshing or publishing the lease" {
    for ([_]bool{ false, true }) |pooled| {
        var fixture: RemoteFixture = undefined;
        try fixture.init();
        defer fixture.deinit();
        var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
        defer server.scheduler.close();
        var pool = try memory.Pool.init(std.testing.allocator, null, .{ .bytes = .{ 65536, 1024, 1024 }, .groups = .{ 3, 2, 2 } });
        defer pool.deinit();
        const owner = try Owner.create(std.testing.allocator, null, if (pooled) &pool else null, .{});
        defer owner.release();
        try owner.start(&server, std.testing.io, 23);
        const receipt = try RemoteFixture.request(.read, @sizeOf(Owner) + 23 + 1024);
        try owner.prepareRemote(receipt);
        try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, 1025));
        {
            const scratch = try owner.allocator().alloc(u8, 900);
            defer owner.allocator().free(scratch);
            try std.testing.expectEqual(@as(usize, 0), fixture.registry.snapshot().attempts);
            try std.testing.expectError(error.InvalidRunnableTransition, owner.admitRemote(&fixture.registry, receipt, RemoteFixture.binding(), fixture.authority()));
        }
        var refreshed = receipt;
        refreshed.local_deadline_ns += 1;
        try std.testing.expectError(error.InvalidRunnableTransition, owner.prepareRemote(refreshed));
        try std.testing.expectEqual(Owner.RemoteAdmission.started, try owner.admitRemote(&fixture.registry, receipt, RemoteFixture.binding(), fixture.authority()));
        try std.testing.expectEqual(@as(usize, 1), fixture.registry.snapshot().attempts);
    }
}

test "internal query owner trusted dispatch retains local and inbound parents across lost remote replies" {
    const protocol_mod = @import("remote_attempt_client.zig");
    const http = @import("../raft/transport/http_common.zig");
    const a = std.testing.allocator;
    const Mode = enum { lost, invalid_reply, success };
    const Lost = struct {
        mode: Mode,
        retained_read: bool,
        calls: usize = 0,
        fn execute(raw: *anyopaque, alloc: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.retained_read) {
                var envelope = try @import("owned_retained_read.zig").decode(std.testing.allocator, request.body);
                defer envelope.deinit();
                try std.testing.expectEqualStrings("{\"operation\":\"capture\",\"schema_version\":5}", envelope.value.request);
                try std.testing.expectEqualStrings("{}", envelope.value.route_fence);
                try std.testing.expect(request.header(@import("../metadata/api.zig").catalog_route_fence_header) == null);
            } else {
                var envelope = try @import("owned_group_query.zig").decode(std.testing.allocator, request.body);
                defer envelope.deinit();
                try std.testing.expectEqualStrings("{}", envelope.value.query);
            }
            try std.testing.expect(request.execution_remote_query == null);
            if (self.mode == .lost) return error.ConnectionResetByPeer;
            if (self.retained_read) return @import("http_route_helpers.zig").textResponseWithHeaders(alloc, 200, "{}", &.{.{ .name = @import("../metadata/api.zig").catalog_route_fence_ack_header, .value = if (self.mode == .invalid_reply) "invalid" else @import("../metadata/api.zig").catalog_route_fence_ack_value }});
            return @import("http_route_helpers.zig").textResponseWithHeaders(alloc, 200, "{\"responses\":[]}", &.{.{ .name = sources.api_query.QueryResponse.identity_read_generation_header, .value = if (self.mode == .invalid_reply) "invalid" else "42" }});
        }
    };
    for ([_]bool{ false, true }) |retained_read| {
        for ([_]bool{ false, true }) |inbound| {
            for ([_]Mode{ .lost, .invalid_reply, .success }) |mode| {
                var fixture: RemoteFixture = undefined;
                try fixture.init();
                defer fixture.deinit();
                var tmp = std.testing.tmpDir(.{});
                defer tmp.cleanup();
                var store = try @import("../common/remote_fence_store.zig").Store.create(a, std.testing.io, tmp.dir, 11, 2);
                defer store.deinit();
                var slots: [1]?attempts.Attempt = undefined;
                var destinations = [_]attempts.Destination{.{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 65536 }};
                var ledger = try store.startCoordinator(7, 0, &slots, &destinations, 65536);
                try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = ledger.generationState().generation, .worker_incarnation = 22 });
                const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
                const targets = [_]protocol_mod.Target{.{ .base_uri = "http://worker", .owned_reads_v1 = true, .owned_retained_reads_v1 = true, .authority = .{ .local = .{ .metadata_incarnation = RemoteFixture.local.metadata_incarnation, .node_id = 10, .store_id = 14, .root_incarnation = 22, .reporter_incarnation = 18 }, .public_key = pair.public_key.toBytes() } }};
                var lost: Lost = .{ .mode = mode, .retained_read = retained_read };
                const executor: http.RequestExecutor = .{ .ptr = &lost, .vtable = &.{ .execute = Lost.execute } };
                const client: protocol_mod.Client = .{ .ledger = &ledger, .store = &store, .targets = &targets, .signing = .{ .secret = "owned-query-parent-service-secret-32-bytes", .issuer = "cluster" }, .read_allocator = a, .control_allocator = a, .read_executor = executor, .control_executor = executor };
                var server = TestServer{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
                defer server.scheduler.close();
                var pool = try memory.Pool.init(a, null, .{ .bytes = .{ 1 << 20, 1024, 1024 }, .groups = .{ 3, 2, 2 } });
                defer pool.deinit();
                const owner = try Owner.create(a, null, &pool, .{ .deadline_ns = RemoteFixture.now() + 5 * std.time.ns_per_s });
                var ingress = true;
                defer if (ingress) owner.release();
                try owner.start(&server, std.testing.io, 23);
                var recovering = client;
                recovering.reads_ready = false;
                try std.testing.expectError(error.DistributedQueryUnavailable, owner.bindRemoteQueries(recovering));
                try std.testing.expect(owner.remote_dispatch == null);
                try std.testing.expect(owner.context.execution_remote_query == null);
                try owner.bindRemoteQueries(client);
                if (inbound) _ = try owner.admitRemote(&fixture.registry, try RemoteFixture.request(.read, 65536), RemoteFixture.binding(), fixture.authority());
                const alloc = owner.allocator();
                {
                    const planning: @import("internal_query_operations.zig").Context = .{ .catalog = .{ .ptr = &fixture }, .query_router = .{ .ptr = &fixture, .route_query_to_read_schema = undefined } };
                    var planned = try planning.planQueryWithContext(alloc, "docs", "{}", owner.context);
                    defer planned.deinit(alloc);
                    try std.testing.expectEqual(owner.context.execution_remote_query.?.ptr, planned.req.execution_remote_query.?.ptr);
                }
                const scratch = try alloc.dupe(u8, "retained query heap");
                var scratch_live = true;
                defer if (scratch_live) alloc.free(scratch);
                const authority = owner.context.execution_remote_query.?;
                var wait: @import("runnable_fanout_owner.zig").RemoteWait = .{ .borrow = owner.team.?.borrow };
                try std.testing.expectError(error.InvalidRemoteAttemptTarget, authority.execute(alloc, .{ .method = .POST, .uri = "http://worker.evil/internal/v1/groups/7/tables/docs/query", .body = "{}" }));
                try std.testing.expectEqual(@as(usize, 0), lost.calls);
                const request: http.HttpRequest = .{ .method = .POST, .uri = if (retained_read) "http://worker/internal/v1/groups/7/tables/docs/retained-read" else "http://worker/internal/v1/groups/7/tables/docs/query", .body = if (retained_read) "{\"operation\":\"capture\",\"schema_version\":5}" else "{}", .headers = if (retained_read) &.{.{ .name = @import("../metadata/api.zig").catalog_route_fence_header, .value = "{}" }} else &.{}, .execution_wait = wait.boundary() };
                if (mode == .success) {
                    var response = try authority.execute(alloc, request);
                    defer response.deinit(alloc);
                    try std.testing.expectEqualStrings(if (retained_read) "{}" else "{\"responses\":[]}", response.body);
                } else try std.testing.expectError(error.RemoteOutcomeUncertain, authority.execute(alloc, request));
                try std.testing.expectEqual(@as(usize, 1), lost.calls);
                try std.testing.expectEqual(@as(usize, 1), ledger.snapshot().attempts);
                try std.testing.expectError(error.InvalidRunnableTransition, owner.tightenDeadline(1));
                const id = slots[0].?.id;
                owner.parkOutput();
                owner.release();
                ingress = false;
                try std.testing.expectEqual(@as(usize, 1), server.scheduler.stats().operations);
                try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().runnable_slots);
                try std.testing.expectEqual(@as(usize, @intFromBool(inbound)), fixture.registry.snapshot().attempts);
                try std.testing.expectEqualStrings("retained query heap", scratch);
                try std.testing.expect(ledger.reconciliationActionAt(2, RemoteFixture.now()).?.kind == .cancel);
                alloc.free(scratch);
                scratch_live = false;
                try std.testing.expect(try ledger.terminal(id, 22));
                try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().operations);
                try std.testing.expectEqual(@as(usize, 0), fixture.registry.snapshot().attempts);
                try std.testing.expectEqual(@as(usize, 0), pool.stats().used[0]);
            }
        }
    }
}

test "internal query owner retained receiver pins admitted heap and runnable after response retirement" {
    const receiver = @import("../storage/retained_read_registry.zig");
    const alloc = std.testing.allocator;
    var fixture: RemoteFixture = undefined;
    try fixture.init();
    defer fixture.deinit();
    var server: TestServer = .{ .scheduler = try scheduler.Scheduler.init(.{ .slots = 2 }) };
    defer server.scheduler.close();
    var pool = try memory.Pool.init(alloc, null, .{ .bytes = .{ 65536, 1024, 1024 }, .groups = .{ 3, 2, 2 } });
    defer pool.deinit();
    var tokens = try receiver.Registry.init(alloc, std.testing.io, 99, 2, 2, 10 * std.time.ns_per_s);
    defer tokens.deinit();
    const Probe = struct {
        live: bool = true,
        calls: usize = 0,
        fn canceled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            std.debug.assert(self.live);
            self.calls += 1;
            return false;
        }
    };
    const Buffer = struct {
        allocator: std.mem.Allocator,
        owner: *Owner,
        bytes: []u8,
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.owner.allocator().free(self.bytes);
            const allocator = self.allocator;
            allocator.destroy(self);
        }
    };
    var probe: Probe = .{};
    const owner = try Owner.create(alloc, null, &pool, .{ .cancellation = .{ .ptr = &probe, .is_cancelled_fn = Probe.canceled } });
    var ingress_live = true;
    defer if (ingress_live) owner.release();
    try owner.start(&server, std.testing.io, 23);
    try std.testing.expect((try owner.retainedReadParent()) == null);
    const verified = try RemoteFixture.request(.read, 32768);
    try std.testing.expectEqual(Owner.RemoteAdmission.started, try owner.admitRemote(&fixture.registry, verified, RemoteFixture.binding(), fixture.authority()));
    const parent = (try owner.retainedReadParent()).?;
    var retained: ?attempts.Retention = try parent.retain(parent.ptr);
    defer if (retained) |value| value.release();
    const buffer = try alloc.create(Buffer);
    buffer.* = .{ .allocator = alloc, .owner = owner, .bytes = try owner.allocator().dupe(u8, "retained receiver data survives the HTTP response") };
    const scope: receiver.Scope = .{ .principal = @splat(1), .authorization_revision = 1, .table_id = 2, .group_id = 3, .topology_revision = 4, .schema_version = 5 };
    const now = @import("antfly_platform").time.monotonicNs();
    const token = try tokens.insert(scope, 1, now, now + 5 * std.time.ns_per_s, .{ .ptr = buffer, .close = Buffer.close, .kind = .snapshot, .retention = retained });
    retained = null;
    defer tokens.close(token, scope) catch {};
    var response = try httpx.Response.fromText(owner.allocator(), 200, "response ownership has a distinct retirement fence");
    var output_live = true;
    defer if (output_live) response.deinit();
    owner.parkOutput();
    try owner.retainResponse(&response);
    owner.release();
    ingress_live = false;
    response.deinit();
    output_live = false;
    probe.live = false;
    const calls = probe.calls;
    try std.testing.expectError(error.Canceled, parent.check(parent.ptr));
    try std.testing.expectEqual(calls, probe.calls);
    try std.testing.expectEqualStrings("retained receiver data survives the HTTP response", buffer.bytes);
    try std.testing.expectEqual(@as(usize, 1), pool.stats().groups[0]);
    try std.testing.expect(pool.stats().total_used > 0);
    try std.testing.expectEqual(@as(usize, 1), server.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().runnable_slots);
    var health: scheduler.Execution = .{};
    try health.start(&server.scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
    health.finish();
    try std.testing.expectEqual(attempts.WorkerGate.Status.active, try fixture.registry.status(try RemoteFixture.request(.status, 0), RemoteFixture.binding()));
    try std.testing.expectEqual(receiver.Retirement.retired, try tokens.closeAcknowledged(token, scope));
    try std.testing.expectEqual(@as(usize, 0), pool.stats().groups[0]);
    try std.testing.expectEqual(@as(usize, 0), pool.stats().total_used);
    try std.testing.expectEqual(@as(usize, 0), server.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), fixture.registry.snapshot().bytes);
    try std.testing.expectEqual(attempts.WorkerGate.Status.terminal, try fixture.registry.status(try RemoteFixture.request(.status, 0), RemoteFixture.binding()));
}
