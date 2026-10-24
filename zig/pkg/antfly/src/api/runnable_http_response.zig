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

const std = @import("std");
const httpx = @import("httpx");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const RequestCancellation = @import("antfly_local_sources").common_http_http_common.RequestCancellation;

/// Stable request authority retained through buffered HTTP output. Both server
/// protocols destroy the context after their synchronous response writer and
/// response allocation retire, including write/reset/disconnect failures.
pub const Owner = struct {
    allocator: std.mem.Allocator,
    cancellation: RequestCancellation,
    execution: scheduler.Execution = .{},
    transferred: bool = false,
    node: ?*scheduler.NodeOwner = null,
    references: std.atomic.Value(usize) = .init(1),
    ingress_retired: std.atomic.Value(bool) = .init(false),
    original: ?RequestCancellation = null,
    cancellation_mutex: std.atomic.Mutex = .unlocked,
    remote_dispatch: ?@import("retained_query_dispatch.zig").Dispatcher = null,

    const context_key = "antfly.query-response-owner";

    pub fn create(allocator: std.mem.Allocator, original: RequestCancellation) !*Owner {
        const self = try allocator.create(Owner);
        self.* = .{ .allocator = allocator, .cancellation = .{ .execution_deadline_ns = original.execution_deadline_ns }, .original = original };
        self.cancellation.borrowed_context = self;
        self.cancellation.borrowed_is_cancelled = originalCanceled;
        return self;
    }

    pub fn createForServer(server: anytype, original: RequestCancellation) !*Owner {
        const self = try create(server.alloc, original);
        errdefer self.allocator.destroy(self);
        if (server.runnable_owner) |node| {
            try node.retain();
            self.node = node;
        }
        return self;
    }
    fn lock(self: *Owner) void {
        while (!self.cancellation_mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn originalCanceled(raw: *const anyopaque) bool {
        const self: *Owner = @ptrCast(@alignCast(@constCast(raw)));
        self.lock();
        defer self.cancellation_mutex.unlock();
        if (self.original) |*original| {
            if (original.isCancelled()) {
                self.cancellation.cancel();
                return true;
            }
            return false;
        }
        return true;
    }
    pub fn bindRemoteQueries(self: *Owner, client: @import("remote_attempt_client.zig").Client) !void {
        if (self.execution.entry == null or self.remote_dispatch != null) return error.InvalidRunnableTransition;
        self.remote_dispatch = try @import("retained_query_dispatch.zig").Dispatcher.init(client, self, remoteDeadline, retainParent);
    }
    pub fn remoteQueries(self: *Owner) ?@import("../raft/transport/http_common.zig").RequestExecutor {
        return if (self.remote_dispatch) |*dispatch| dispatch.executor() else null;
    }
    fn remoteDeadline(raw: *anyopaque) !?u64 {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.cancellation.isCancelled()) return error.Canceled;
        try self.execution.checkLifetime();
        return if (self.execution.entry) |*entry| entry.options.deadline_ns else null;
    }
    fn retainParent(raw: *anyopaque, deadline: u64) !@import("../common/remote_attempts.zig").Retention {
        const self: *Owner = @ptrCast(@alignCast(raw));
        _ = try remoteDeadline(self);
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0 and previous != std.math.maxInt(usize));
        return .{ .context = self, .index = 0, .id = .{ .coordinator = 0, .generation = 0, .operation = 0, .sequence = 0 }, .release_fn = releaseParent, .cancellation = &self.cancellation.cancelled, .deadline_ns = deadline };
    }
    fn releaseParent(raw: *anyopaque, _: usize, _: @import("../common/remote_attempts.zig").Id) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.releaseReference();
    }
    fn releaseReference(self: *Owner) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous != 1) return;
        const allocator = self.allocator;
        const node = self.node;
        if (self.remote_dispatch) |*dispatch| dispatch.deinit();
        self.execution.finish();
        allocator.destroy(self);
        if (node) |owner| owner.release();
    }

    pub fn releaseUnlessTransferred(self: *Owner) void {
        if (!self.transferred) destroy(self);
    }

    pub fn retainUntilContextRetires(self: *Owner, ctx: *httpx.Context) !void {
        if (self.execution.entry == null) return;
        if (self.transferred or ctx.getData(context_key) != null) return error.InvalidRunnableTransition;
        try self.execution.park(.output_blocked);
        try ctx.setData(context_key, self, destroy);
        self.transferred = true;
    }

    fn destroy(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        if (self.ingress_retired.swap(true, .acq_rel)) return;
        self.lock();
        self.cancellation.cancel();
        self.original = null;
        self.cancellation_mutex.unlock();
        self.releaseReference();
    }
};

test "HTTP response owner retains original authority through context retirement" {
    const a = std.testing.allocator;
    for ([_]enum { success, cancel, expire, shutdown }{ .success, .cancel, .expire, .shutdown }) |outcome| {
        var scheduler_owner = try scheduler.Scheduler.init(.{ .slots = 2 });
        var request = try httpx.Request.init(a, .POST, "http://localhost/db/v1/query");
        defer request.deinit();
        var ctx = httpx.Context.init(a, std.testing.io, &request);
        var context_live = true;
        defer if (context_live) ctx.deinit();
        const Clock = struct {
            var now_ns: u64 = 10;
            fn now() u64 {
                return now_ns;
            }
        };
        Clock.now_ns = 10;
        const owner = try Owner.create(a, .{ .execution_deadline_ns = 30 });
        var transferred = false;
        defer if (!transferred) owner.releaseUnlessTransferred();
        try owner.execution.start(&scheduler_owner, .{
            .io = std.testing.io,
            .native_now_ns = Clock.now,
            .deadline_ns = owner.cancellation.execution_deadline_ns,
            .retained_bytes = 23,
            .cancellation = .{ .ptr = &owner.cancellation, .requested = struct {
                fn requested(raw: *const anyopaque) bool {
                    return @as(*const RequestCancellation, @ptrCast(@alignCast(raw))).isCancelled();
                }
            }.requested },
        }, .query);
        try owner.execution.park(.resource_blocked);
        try owner.execution.enter(.inference);
        try owner.retainUntilContextRetires(&ctx);
        transferred = true;
        // The handler's normal scope retirement no longer owns this capsule.
        owner.releaseUnlessTransferred();
        try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().output_blocked);
        try std.testing.expectEqual(@as(u64, 23), scheduler_owner.stats().retained_bytes);
        try std.testing.expectEqual(scheduler.Class.inference, try owner.execution.retainedClass());
        var control: scheduler.Execution = .{};
        try control.start(&scheduler_owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
        try control.checkpoint();
        control.finish();
        const grants = scheduler_owner.stats().grants;
        switch (outcome) {
            .success => try owner.execution.checkLifetime(),
            .cancel => {
                owner.cancellation.cancel();
                try std.testing.expectError(error.Canceled, owner.execution.checkLifetime());
            },
            .expire => {
                Clock.now_ns = 31;
                try std.testing.expectError(error.DeadlineExceeded, owner.execution.checkLifetime());
            },
            .shutdown => {
                scheduler_owner.close();
                try std.testing.expectError(error.SchedulerClosed, owner.execution.checkLifetime());
            },
        }
        try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler_owner.stats().retained_bytes);
        try std.testing.expectEqual(grants, scheduler_owner.stats().grants);
        ctx.deinit();
        context_live = false;
        try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler_owner.stats().retained_bytes);
    }
}

test "HTTP response owner context allocation failure retires the untransferred root" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            var scheduler_owner = try scheduler.Scheduler.init(.{ .slots = 2 });
            defer std.debug.assert(scheduler_owner.stats().operations == 0 and scheduler_owner.stats().retained_bytes == 0);
            var request = try httpx.Request.init(std.testing.allocator, .POST, "http://localhost/db/v1/query");
            defer request.deinit();
            const owner = try Owner.create(a, .{});
            var ctx = httpx.Context.init(a, std.testing.io, &request);
            defer ctx.deinit();
            defer owner.releaseUnlessTransferred();
            try owner.execution.start(&scheduler_owner, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
            try owner.retainUntilContextRetires(&ctx);
            try std.testing.expectError(error.InvalidRunnableTransition, owner.retainUntilContextRetires(&ctx));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "HTTP response owner retains published remote parents after HTTP cancellation context retires" {
    for ([_]u64{ 17, 18 }) |replacement_reporter| {
        for ([_]bool{ false, true }) |saturated| {
            const a = std.testing.allocator;
            const http = @import("../raft/transport/http_common.zig");
            const attempts = @import("../common/remote_attempts.zig");
            const client_mod = @import("remote_attempt_client.zig");
            const Probe = struct {
                live: bool = true,
                checks: usize = 0,
                last_uri: [256]u8 = undefined,
                last_uri_len: usize = 0,
                fn canceled(raw: *const anyopaque) bool {
                    const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
                    std.debug.assert(self.live);
                    self.checks += 1;
                    return false;
                }
                fn execute(raw: *anyopaque, _: std.mem.Allocator, request: http.HttpRequest) !http.HttpResponse {
                    const self: *@This() = @ptrCast(@alignCast(raw));
                    if (request.uri.len > self.last_uri.len) return error.InvalidArgument;
                    @memcpy(self.last_uri[0..request.uri.len], request.uri);
                    self.last_uri_len = request.uri.len;
                    return error.ConnectionResetByPeer;
                }
            };
            var probe: Probe = .{};
            var tmp = std.testing.tmpDir(.{});
            defer tmp.cleanup();
            var store = try @import("../common/remote_fence_store.zig").Store.create(a, std.testing.io, tmp.dir, 11, 2);
            defer store.deinit();
            var slots: [1]?attempts.Attempt = undefined;
            var destinations = [_]attempts.Destination{.{ .worker_incarnation = 22, .max_attempts = 1, .max_bytes = 65536 }};
            var ledger = try store.startCoordinator(7, 0, &slots, &destinations, 65536);
            try ledger.acknowledgeFence(0, .{ .coordinator = 7, .generation = ledger.generationState().generation, .worker_incarnation = 22 });
            const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
            const targets = [_]client_mod.Target{.{ .base_uri = "http://worker", .owned_reads_v1 = true, .authority = .{ .local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 22, .reporter_incarnation = 17 }, .public_key = pair.public_key.toBytes() } }};
            const executor: http.RequestExecutor = .{ .ptr = &probe, .vtable = &.{ .execute = Probe.execute } };
            var client: client_mod.Client = .{ .ledger = &ledger, .store = &store, .targets = &targets, .signing = .{ .secret = "public-query-parent-service-secret-32-bytes", .issuer = "cluster" }, .read_allocator = a, .control_allocator = a, .read_executor = executor, .control_executor = executor };
            var node = try scheduler.NodeOwner.init(.{ .slots = 2 });
            defer node.deinit();
            const scheduler_owner = &node.scheduler;
            const descriptors = [_]struct { local: @TypeOf(targets[0].authority.local), base_uri: []const u8 }{.{ .local = targets[0].authority.local, .base_uri = targets[0].base_uri }};
            const view = try @import("remote_endpoint_view.zig").View.create(a, scheduler_owner, &descriptors, &.{targets[0].authority});
            var view_owner_live = true;
            defer if (view_owner_live) view.release();
            view.targets[0].owned_reads_v1 = true;
            view.targets[0].owned_retained_reads_v1 = true;
            client.targets = view.targets;
            client.endpoint_view = view;
            const Source = @import("remote_endpoint_source.zig").Source;
            const source = try Source.create(a, scheduler_owner, view);
            var source_live = true;
            defer if (source_live) {
                source.close();
                source.deinit() catch unreachable;
            };
            client.endpoint_source = source;
            const view_bytes = view.reservedBytes();
            const Server = struct { alloc: std.mem.Allocator, runnable_owner: ?*scheduler.NodeOwner };
            var server: Server = .{ .alloc = a, .runnable_owner = &node };
            var request = try httpx.Request.init(a, .POST, "http://localhost/db/v1/query");
            defer request.deinit();
            var ctx = httpx.Context.init(a, std.testing.io, &request);
            var context_live = true;
            defer if (context_live) ctx.deinit();
            const owner = try Owner.createForServer(&server, .{ .borrowed_context = &probe, .borrowed_is_cancelled = Probe.canceled });
            var transferred = false;
            defer if (!transferred) owner.releaseUnlessTransferred();
            const now = @import("antfly_platform").time.monotonicNs;
            const deadline = now() + 5 * std.time.ns_per_s;
            try owner.execution.start(scheduler_owner, .{ .io = std.testing.io, .native_now_ns = now, .deadline_ns = deadline, .retained_bytes = 23 }, .query);
            try owner.bindRemoteQueries(client);
            try std.testing.expectEqual(@as(usize, 3), view.referenceCount());
            view_owner_live = false;
            view.release();
            try std.testing.expectError(error.RemoteOutcomeUncertain, owner.remoteQueries().?.execute(a, .{ .method = .POST, .uri = "http://worker/internal/v1/groups/7/tables/docs/retained-read", .body = "{\"operation\":\"capture\",\"schema_version\":1,\"connection\":7}", .headers = &.{.{ .name = @import("../metadata/api.zig").catalog_route_fence_header, .value = "{}" }} }));
            const id = slots[0].?.id;
            var replacement_authority = targets[0].authority;
            replacement_authority.local.reporter_incarnation = replacement_reporter;
            const replacement_descriptors = [_]struct { local: @TypeOf(replacement_authority.local), base_uri: []const u8 }{.{ .local = replacement_authority.local, .base_uri = "http://replacement" }};
            var replacement_bytes: u64 = 0;
            var pressure: ?scheduler.Scheduler.ControlBaseline = null;
            defer if (pressure) |*reserved| reserved.release();
            var ticket = try source.current();
            defer ticket.release();
            if (saturated) {
                pressure = try scheduler_owner.reserveControlBaseline(scheduler_owner.config.control_bytes / 4 - scheduler_owner.stats().control_retained_bytes);
                _ = try source.acceptSnapshot(ticket.revision, &replacement_descriptors, &.{replacement_authority}, &.{true});
                try std.testing.expectError(error.CleanupCapacityUnavailable, @import("remote_endpoint_view.zig").View.create(a, scheduler_owner, &replacement_descriptors, &.{replacement_authority}));
            } else {
                const replacement = try @import("remote_endpoint_view.zig").View.create(a, scheduler_owner, &replacement_descriptors, &.{replacement_authority});
                defer replacement.release();
                replacement_bytes = replacement.reservedBytes();
                try source.publish(ticket.revision, replacement);
            }
            ticket.release();
            var original = try source.find(id.operation);
            defer original.release();
            try std.testing.expectEqual(@as(u64, 17), original.view().targets[0].authority.local.reporter_incarnation);
            original.release();
            try owner.retainUntilContextRetires(&ctx);
            transferred = true;
            ctx.deinit();
            context_live = false;
            probe.live = false;
            try std.testing.expectEqual(@as(usize, 1), node.borrowers);
            try std.testing.expectError(error.InvalidRunnableTransition, node.tryDeinit());
            const checks = probe.checks;
            try std.testing.expect(owner.cancellation.isCancelled());
            try std.testing.expectEqual(checks, probe.checks);
            try std.testing.expectEqual(@as(usize, 1), scheduler_owner.stats().operations);
            try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 65536), ledger.snapshot().bytes);
            try std.testing.expectEqual(@as(usize, if (saturated) 2 else 1), view.referenceCount());
            const expected_before = if (saturated) scheduler_owner.config.control_bytes / 4 else view_bytes + replacement_bytes + (try Source.reservationBytes(1)) + @sizeOf(Source.Binding);
            try std.testing.expectEqual(expected_before, scheduler_owner.stats().control_retained_bytes);
            try std.testing.expectError(error.ConnectionResetByPeer, client.status(0, id, now() + std.time.ns_per_s));
            try std.testing.expectEqualStrings(if (replacement_reporter == 17) "http://replacement/internal/v1/attempt/status" else "http://worker/internal/v1/attempt/status", probe.last_uri[0..probe.last_uri_len]);
            try std.testing.expectError(error.ConnectionResetByPeer, client.cleanupRetainedRead(0, id, now() + std.time.ns_per_s, .{ .method = .POST, .uri = "/internal/v1/groups/7/tables/docs/retained-read", .body = "{\"operation\":\"cancel\",\"schema_version\":1,\"connection\":7}" }, "{}"));
            try std.testing.expectEqualStrings(if (replacement_reporter == 17) "http://replacement/internal/v1/groups/7/tables/docs/owned-retained-read/22/17" else "http://worker/internal/v1/groups/7/tables/docs/owned-retained-read/22/17", probe.last_uri[0..probe.last_uri_len]);
            try std.testing.expectEqual(@as(usize, 65536), ledger.snapshot().bytes);
            try std.testing.expect(ledger.reconciliationActionAt(2, now()).?.kind == .cancel);
            try std.testing.expect(try ledger.terminal(id, 22));
            try std.testing.expectEqual(@as(usize, 0), scheduler_owner.stats().operations);
            try std.testing.expectEqual(@as(usize, 0), node.borrowers);
            const expected_after = if (saturated) scheduler_owner.config.control_bytes / 4 - @sizeOf(Source.Binding) else replacement_bytes + (try Source.reservationBytes(1));
            try std.testing.expectEqual(expected_after, scheduler_owner.stats().control_retained_bytes);
            if (pressure) |*reserved| reserved.release();
            try std.testing.expectEqual((if (saturated) view_bytes else replacement_bytes) + (try Source.reservationBytes(1)), scheduler_owner.stats().control_retained_bytes);
            source.close();
            source_live = false;
            try source.deinit();
            try std.testing.expectEqual(@as(u64, 0), scheduler_owner.stats().control_retained_bytes);
        }
    }
}
