// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Durable snapshot pins. Admission publishes a monotonic deadline before its
//! second retirement check. GC publishes retirement before reading deadlines.
//! Thus every admitted reader either protects the snapshot or fails closed.
const std = @import("std");
const local = @import("antfly_local_sources");
const objectstore = @import("objectstore");
const platform = @import("antfly_platform");
const catalog = local.serverless_external_source_mod.lake_catalog;
const Context = catalog.types.Context;
const A = std.mem.Allocator;
const Operation = @import("provider_operation.zig").Operation;
const Rejection = @import("antfly_cancellation").Rejection;
pub const duration_ns = 2 * std.time.ns_per_min;
pub const grace_ns = 30 * std.time.ns_per_s;
pub fn namespace(a: A, prefix: []const u8, binding: local.serverless_external_source_catalog_binding.Binding, uuid: []const u8) ![]u8 {
    const identity = try std.json.Stringify.valueAlloc(a, .{ binding.source_uri, uuid }, .{});
    defer a.free(identity);
    return std.fmt.allocPrint(a, "{s}{s}lake-readers/{s}", .{ prefix, if (prefix.len == 0) "" else "/", catalog.types.digestHex(identity) });
}
pub const Store = struct {
    client: objectstore.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: Context = .{},
    fn key(self: Store, a: A, kind: []const u8, snapshot: []const u8) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/snapshots/{s}/{s}", .{ self.prefix, kind, catalog.types.digestHex(snapshot) });
    }
    fn get(self: Store, a: A, kind: []const u8, snapshot: []const u8) !?objectstore.GetResult {
        var client = self.client;
        const path = try self.key(a, kind, snapshot);
        defer a.free(path);
        const operation: Operation = .{ .context = self.context };
        return operation.get(&client, self.bucket, path, .{ .max_response_bytes = 1024 }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => null,
            else => return err,
        };
    }
    pub fn deadline(self: Store, a: A, snapshot: []const u8) !u64 {
        var value = (try self.get(a, "pins", snapshot)) orelse return 0;
        defer value.deinit(self.client.allocator);
        return std.fmt.parseInt(u64, value.body, 10);
    }
    pub fn acquire(self: Store, a: A, snapshot: []const u8, now: u64) !u64 {
        return self.acquireFor(a, snapshot, now, duration_ns);
    }
    /// Durable maintenance keeps its input alive between bounded turns.
    /// Abandoned work expires; retirement still wins the final admission race.
    pub fn acquireFor(self: Store, a: A, snapshot: []const u8, now: u64, duration: u64) !u64 {
        if (duration == 0 or duration > std.time.ns_per_day) return error.LakeSnapshotReadLeaseExpired;
        if (now == 0 or snapshot.len == 0) return error.LakeSnapshotReadLeaseExpired;
        for (0..16) |_| {
            try self.context.ensureActive();
            if (try self.get(a, "retired", snapshot)) |value| {
                var proof = value;
                proof.deinit(self.client.allocator);
                return error.LakeSnapshotRetired;
            }
            var previous = try self.get(a, "pins", snapshot);
            defer if (previous) |*value| value.deinit(self.client.allocator);
            const old = if (previous) |value| try std.fmt.parseInt(u64, value.body, 10) else 0;
            const desired = try std.math.add(u64, now, duration);
            const deadline_ns = @max(old, desired);
            if (deadline_ns != old) {
                const bytes = try std.fmt.allocPrint(a, "{d}", .{deadline_ns});
                defer a.free(bytes);
                const path = try self.key(a, "pins", snapshot);
                defer a.free(path);
                var client = self.client;
                const operation: Operation = .{ .context = self.context };
                var result = operation.put(&client, self.bucket, path, bytes, .{ .if_none_match = previous == null, .if_match_etag = if (previous) |value| value.metadata.etag orelse return error.MissingObjectEtag else null }) catch |err| switch (err) {
                    error.PreconditionFailed, error.ObjectAlreadyExists => continue,
                    else => return err,
                };
                result.deinit(client.allocator);
            }
            if (try self.get(a, "retired", snapshot)) |value| {
                var proof = value;
                proof.deinit(self.client.allocator);
                return error.LakeSnapshotRetired;
            }
            // A future deadline from another host never extends local authority
            // beyond the bounded duration of this process's own acquisition.
            return @min(deadline_ns, desired);
        }
        return error.LakeSnapshotReadLeaseContended;
    }
    pub fn retire(self: Store, a: A, snapshot: []const u8, now: u64) !bool {
        if (now == 0) return false;
        if ((try self.deadline(a, snapshot)) +| grace_ns >= now) return false;
        const path = try self.key(a, "retired", snapshot);
        defer a.free(path);
        var client = self.client;
        const operation: Operation = .{ .context = self.context };
        var result = operation.put(&client, self.bucket, path, snapshot, .{ .if_none_match = true }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => null,
            else => return err,
        };
        defer if (result) |*value| value.deinit(client.allocator);
        // Pin publication races with retirement only before the reader's
        // final check. A race may retain extra data, never admit an unsafe read.
        return (try self.deadline(a, snapshot)) +| grace_ns < now;
    }
};
pub const Owner = struct {
    a: A,
    opened: local.serverless_object_store_support.OpenedObjectStore,
    prefix: []u8,
    snapshot: []u8,
    parent: Context,
    io: std.Io,
    unix_deadline: std.atomic.Value(u64) = .init(0),
    authority_deadline: std.atomic.Value(u64) = .init(0),
    refusal: Rejection = .{},
    stopping: std.atomic.Value(bool) = .init(false),
    heartbeat: ?std.Io.Future(void) = null,
    heartbeat_interval_ns: u64 = 30 * std.time.ns_per_s,
    service: ?*@import("lake_renewal_service.zig").Service = null,
    link: @import("lake_renewal_service.zig").Service.Link = .{},
    pub fn checkSource(self: *const Owner) !void {
        if (self.stopping.load(.acquire)) return error.Canceled;
        try self.parent.checkLifetime();
        const deadline_ns = self.unix_deadline.load(.acquire);
        if (deadline_ns != 0) {
            const unix = platform.time.realtimeNs();
            const authority = platform.time.authorityNs();
            if (unix == 0 or authority == 0 or unix >= deadline_ns or authority >= self.authority_deadline.load(.acquire)) return error.LakeSnapshotReadLeaseExpired;
        }
    }
    fn fence(raw: *anyopaque) !void {
        try @as(*Owner, @ptrCast(@alignCast(raw))).checkSource();
    }
    fn backgroundContext(self: *Owner) Context {
        // This background task must never borrow the foreground task's wait
        // or checkpoint hooks. Admitted background renewal must supply its
        // own execution; original source authority remains in fence.
        return .{ .io = self.io, .deadline_ns = self.parent.deadline_ns, .authority = .{ .ptr = self, .check = fence } };
    }
    fn renew(self: *Owner, context: Context) !void {
        try self.checkSource();
        const now = platform.time.realtimeNs();
        const authority = platform.time.authorityNs();
        if (now == 0 or authority == 0) return error.LakeSnapshotReadLeaseExpired;
        var operation_context = context;
        // Bound one provider attempt without extending the reader's original
        // lifetime. The source fence rechecks that lifetime through every wait.
        const ceiling = platform.time.monotonicNs() +| 10 * std.time.ns_per_s;
        operation_context.deadline_ns = @min(ceiling, @min(context.deadline_ns orelse ceiling, self.parent.deadline_ns orelse ceiling));
        operation_context.authority = .{ .ptr = self, .check = fence };
        const store: Store = .{ .client = self.opened.client, .bucket = self.opened.bucket, .prefix = self.prefix, .context = operation_context };
        const deadline_ns = try store.acquire(self.a, self.snapshot, now);
        // The old rights must still be live when the response is accepted.
        // A delayed heartbeat cannot reacquire an expired snapshot.
        try self.checkSource();
        self.unix_deadline.store(deadline_ns, .release);
        self.authority_deadline.store(authority +| @min(duration_ns, deadline_ns -| now), .release);
    }
    pub fn start(self: *Owner) !local.serverless_lake_host.SnapshotPin {
        try self.checkSource();
        if (self.service == null and (self.parent.execution != null or self.parent.wait != null)) return error.SnapshotRenewalServiceRequired;
        try self.renew(self.parent);
        if (self.service) |service| {
            try service.attach(self);
        } else {
            self.heartbeat = self.io.concurrent(run, .{self}) catch |err| {
                try self.checkSource();
                return err;
            };
        }
        return .{ .ptr = self, .check = check, .deinit = destroy };
    }
    fn run(self: *Owner) void {
        while (!self.stopping.load(.acquire)) {
            self.io.sleep(.fromNanoseconds(@intCast(self.heartbeat_interval_ns)), .awake) catch |err| {
                if (!self.stopping.load(.acquire)) _ = @intFromError(self.refusal.remember(err));
                return;
            };
            self.heartbeatOnce() catch return;
        }
    }
    fn heartbeatOnce(self: *Owner) !void {
        return self.heartbeatFor(self.backgroundContext());
    }
    pub fn heartbeatFor(self: *Owner, context: Context) !void {
        try self.refusal.check();
        self.renew(context) catch |err| {
            const cause = blk: {
                self.checkSource() catch |original| break :blk original;
                if (err == error.LakeSnapshotRetired) break :blk err;
                // Preserve the existing unavailable-reader contract for a
                // provider failure. Raw transport errors must not turn a
                // failed heartbeat into a different query/fallback policy.
                break :blk error.LakeSnapshotReadLeaseExpired;
            };
            if (!self.stopping.load(.acquire)) _ = @intFromError(self.refusal.remember(cause));
            return cause;
        };
    }
    fn check(raw: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.refusal.check();
        try self.checkSource();
        if (self.service) |service| try service.checkOwner(self);
    }
    pub fn destroy(raw: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(raw));
        self.stopping.store(true, .release);
        if (self.service) |service| service.detach(self);
        if (self.heartbeat) |*future| future.cancel(self.io);
        self.opened.deinit();
        self.a.free(self.prefix);
        self.a.free(self.snapshot);
        self.a.destroy(self);
    }
};
test "external lake snapshot retirement fences new admission and protects durable readers" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    const store: Store = .{ .client = memory.client(), .bucket = "lake", .prefix = "identity" };
    const now: u64 = 10 * duration_ns;
    const deadline_ns = try store.acquire(a, "100", now);
    try std.testing.expect(!try store.retire(a, "100", now));
    try std.testing.expect(!try store.retire(a, "100", deadline_ns + grace_ns));
    try std.testing.expect(try store.retire(a, "100", deadline_ns + grace_ns + 1));
    try std.testing.expectError(error.LakeSnapshotRetired, store.acquire(a, "100", deadline_ns + grace_ns + 1));
    try std.testing.expect(try store.retire(a, "101", now));
    try std.testing.expectError(error.LakeSnapshotRetired, store.acquire(a, "101", now));
}

test "external lake snapshot heartbeat preserves source bounds and passive checks through real provider waits" {
    const Runnable = local.common_request_admission.runnable_scheduler;
    const Mode = enum { success, canceled, deadline, expired, cleanup, provider_failure, shutdown };
    const Fixture = struct {
        threaded: std.Io.Threaded,
        execution: ?*Runnable.Execution = null,
        scheduler: ?*Runnable.Scheduler = null,
        armed: std.atomic.Value(bool) = .init(false),
        entered: std.atomic.Value(bool) = .init(false),
        checkpoint_armed: std.atomic.Value(bool) = .init(false),
        checkpoint_calls: std.atomic.Value(usize) = .init(0),
        initial_loans: std.atomic.Value(usize) = .init(0),
        release: std.Io.Event = .unset,
        destroyed: std.atomic.Value(bool) = .init(false),
        fail_sync: std.atomic.Value(bool) = .init(false),
        cleanup_progress: std.atomic.Value(bool) = .init(false),
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.checkpoint_calls.fetchAdd(1, .acq_rel);
            if (self.checkpoint_armed.load(.acquire)) return error.WatchdogEnteredForegroundCheckpoint;
        }
        fn destroy(self: *@This(), owner: *Owner) void {
            Owner.destroy(owner);
            self.destroyed.store(true, .release);
        }
        fn shutdown(self: *@This(), service: *@import("lake_renewal_service.zig").Service) void {
            service.deinit();
            self.destroyed.store(true, .release);
        }
        fn releaseForCleanup(self: *@This(), owner: *Owner) !void {
            const io = self.threaded.io();
            defer self.release.set(io);
            const until = platform.time.monotonicNs() + std.time.ns_per_s;
            while (true) {
                platform.sync.lockYielding(&self.scheduler.?.mutex);
                const blocked = self.execution.?.entry.?.state == .resource_blocked;
                self.scheduler.?.mutex.unlock();
                if (owner.stopping.load(.acquire) and blocked) break;
                if (platform.time.monotonicNs() >= until) return error.CleanupDidNotLendReaderCpu;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            var query: Runnable.Execution = .{};
            defer query.finish();
            try query.startWithSlots(self.scheduler.?, .{ .io = io, .allow_wait = false }, .query, 2);
            var health: Runnable.Execution = .{};
            defer health.finish();
            try health.start(self.scheduler.?, .{ .io = io, .allow_wait = false }, .control);
            health.finish();
            query.finish();
            var pending_execution: Runnable.Execution = .{};
            defer pending_execution.finish();
            try pending_execution.start(self.scheduler.?, .{ .io = io, .retained_bytes = 4096 }, .query);
            var pending: Owner = .{
                .a = std.testing.allocator,
                .opened = owner.opened,
                .prefix = owner.prefix,
                .snapshot = owner.snapshot,
                .parent = (Context{ .io = io, .deadline_ns = platform.time.monotonicNs() + 50 * std.time.ns_per_ms }).withExecution(&pending_execution),
                .io = io,
                .service = owner.service,
                .unix_deadline = .init(platform.time.realtimeNs() + duration_ns),
                .authority_deadline = .init(platform.time.authorityNs() + duration_ns),
            };
            try std.testing.expectError(error.DeadlineExceeded, owner.service.?.attach(&pending));
            self.cleanup_progress.store(true, .release);
        }
        fn sync(raw: ?*anyopaque, file: std.Io.File) std.Io.File.SyncError!void {
            const threaded: *std.Io.Threaded = @ptrCast(@alignCast(raw.?));
            const self: *@This() = @fieldParentPtr("threaded", threaded);
            if (self.execution) |execution| {
                const scheduler = self.scheduler.?;
                platform.sync.lockYielding(&scheduler.mutex);
                const blocked = execution.entry.?.state == .resource_blocked;
                scheduler.mutex.unlock();
                if (blocked) _ = self.initial_loans.fetchAdd(1, .acq_rel);
            }
            if (self.armed.swap(false, .acq_rel)) {
                self.entered.store(true, .release);
                self.release.waitUncancelable(threaded.io());
            }
            if (self.fail_sync.swap(false, .acq_rel)) return error.Unexpected;
            return threaded.io().vtable.fileSync(raw, file);
        }
    };
    const a = std.testing.allocator;
    for (std.enums.values(Mode)) |mode| for ([_]bool{ false, true }) |borrowed_owner| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
        defer a.free(root);
        var fixture: Fixture = .{ .threaded = .init(a, .{}) };
        defer fixture.threaded.deinit();
        var io = fixture.threaded.io();
        var vtable = io.vtable.*;
        vtable.fileSync = Fixture.sync;
        io.vtable = &vtable;
        var filesystem = try objectstore.FilesystemClient.initWithIo(a, root, io);
        defer filesystem.deinit();
        var scheduler = try Runnable.Scheduler.init(.{ .slots = 3 });
        defer scheduler.close();
        var execution: Runnable.Execution = .{};
        defer execution.finish();
        try execution.startWithSlots(&scheduler, .{ .io = io, .retained_bytes = 4096 }, .query, 2);
        fixture.execution = &execution;
        fixture.scheduler = &scheduler;
        var read: @import("../api/runnable_local_read.zig").Read = .{ .execution = &execution, .allocator = a };
        const team = try read.team();
        var bridge = try @import("../api/runnable_native_borrow.zig").Bridge.init(&team.borrow, a);
        var borrowed: Runnable.Execution = .{};
        defer borrowed.finish();
        if (borrowed_owner) try bridge.bind(&borrowed);
        const reader_execution = if (borrowed_owner) &borrowed else &execution;
        var service: @import("lake_renewal_service.zig").Service = .{ .interval_ns = 10 * std.time.ns_per_ms };
        service.configure(io, &scheduler);
        defer service.deinit();
        var canceled = std.atomic.Value(bool).init(false);
        const deadline = platform.time.monotonicNs() + if (mode == .deadline) @as(u64, 400 * std.time.ns_per_ms) else 5 * std.time.ns_per_s;
        const owner = try a.create(Owner);
        owner.* = .{
            .a = a,
            .opened = try local.serverless_object_store_support.OpenedObjectStore.initWithClient(a, filesystem.client(), "pins", ""),
            .prefix = try a.dupe(u8, "identity"),
            .snapshot = try a.dupe(u8, "42"),
            .io = io,
            .service = &service,
            .parent = (Context{ .io = io, .deadline_ns = deadline, .cancellation = .fromAtomic(&canceled), .additional_checkpoint = .{ .ptr = &fixture, .check = Fixture.checkpoint } }).withExecution(reader_execution),
        };
        const pin = try owner.start();
        var pin_owned = true;
        defer if (pin_owned) pin.deinit(pin.ptr);
        const initial = owner.unix_deadline.load(.acquire);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_services);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().control_children);
        try std.testing.expect(owner.heartbeat == null);
        try std.testing.expect(fixture.initial_loans.load(.acquire) > 0);
        fixture.checkpoint_armed.store(true, .release);
        const checkpoints = fixture.checkpoint_calls.load(.acquire);
        // Run the real periodic worker with a short interval and a physical
        // sync barrier; its ownership and join are the production path.
        fixture.armed.store(true, .release);
        defer fixture.release.set(io);
        const until = platform.time.monotonicNs() + 2 * std.time.ns_per_s;
        while (!fixture.entered.load(.acquire)) {
            try pin.check(pin.ptr);
            if (platform.time.monotonicNs() >= until) return error.SnapshotHeartbeatDidNotEnterProvider;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        for (0..64) |_| try pin.check(pin.ptr);
        try std.testing.expectEqual(checkpoints, fixture.checkpoint_calls.load(.acquire));
        try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
        try execution.park(.resource_blocked);
        var query: Runnable.Execution = .{};
        defer query.finish();
        try query.startWithSlots(&scheduler, .{ .io = io, .allow_wait = false }, .query, 2);
        var health: Runnable.Execution = .{};
        defer health.finish();
        try health.start(&scheduler, .{ .io = io, .allow_wait = false }, .control);
        health.finish();
        if (mode == .canceled) canceled.store(true, .release);
        if (mode == .provider_failure) fixture.fail_sync.store(true, .release);
        if (mode == .expired) owner.authority_deadline.store(platform.time.authorityNs(), .release);
        if (mode == .deadline) while (platform.time.monotonicNs() < deadline) try io.sleep(.fromMilliseconds(1), .awake);
        var destruction: ?std.Io.Future(void) = null;
        defer if (destruction) |*future| future.cancel(io);
        if (mode == .cleanup) {
            query.finish();
            try execution.enter(.query);
            var helper = try io.concurrent(Fixture.releaseForCleanup, .{ &fixture, owner });
            defer _ = helper.cancel(io) catch {};
            pin.deinit(pin.ptr);
            pin_owned = false;
            try helper.await(io);
            try std.testing.expect(fixture.cleanup_progress.load(.acquire));
            try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
        }
        if (mode == .shutdown) {
            destruction = try io.concurrent(Fixture.shutdown, .{ &fixture, &service });
            try io.sleep(.fromMilliseconds(10), .awake);
            try std.testing.expect(!fixture.destroyed.load(.acquire));
        }
        fixture.release.set(io);
        if (mode == .shutdown) {
            destruction.?.await(io);
            destruction = null;
            try std.testing.expect(fixture.destroyed.load(.acquire));
            try std.testing.expectError(error.LakeSnapshotReadLeaseExpired, pin.check(pin.ptr));
            pin.deinit(pin.ptr);
            pin_owned = false;
        } else if (mode != .cleanup) {
            while (true) {
                if (mode == .success and owner.unix_deadline.load(.acquire) > initial) break;
                owner.refusal.check() catch break;
                if (platform.time.monotonicNs() >= until) return error.SnapshotHeartbeatDidNotFinish;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            switch (mode) {
                .success => {
                    try pin.check(pin.ptr);
                    const store: Store = .{ .client = owner.opened.client, .bucket = owner.opened.bucket, .prefix = owner.prefix };
                    const current = owner.unix_deadline.load(.acquire);
                    try std.testing.expect(try store.retire(a, owner.snapshot, current + grace_ns + duration_ns));
                    // The reader may retain old rights, but renewal must replay
                    // retirement precisely and never publish another deadline.
                    while (true) {
                        owner.refusal.check() catch |err| {
                            try std.testing.expectEqual(error.LakeSnapshotRetired, err);
                            break;
                        };
                        if (platform.time.monotonicNs() >= until) return error.SnapshotRetirementWasNotObserved;
                        try io.sleep(.fromMilliseconds(1), .awake);
                    }
                    try std.testing.expectError(error.LakeSnapshotRetired, pin.check(pin.ptr));
                    const refused = owner.unix_deadline.load(.acquire);
                    try std.testing.expectError(error.LakeSnapshotRetired, owner.heartbeatOnce());
                    try std.testing.expectEqual(refused, owner.unix_deadline.load(.acquire));
                },
                .canceled => try std.testing.expectError(error.Canceled, pin.check(pin.ptr)),
                .deadline => try std.testing.expectError(error.DeadlineExceeded, pin.check(pin.ptr)),
                .expired, .provider_failure => {
                    try std.testing.expectError(error.LakeSnapshotReadLeaseExpired, pin.check(pin.ptr));
                    try std.testing.expectEqual(initial, owner.unix_deadline.load(.acquire));
                },
                .cleanup, .shutdown => unreachable,
            }
            pin.deinit(pin.ptr);
            pin_owned = false;
        }
        query.finish();
        borrowed.finish();
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

fn fixtureOwner(a: A, client: objectstore.Client, context: Context, service: *@import("lake_renewal_service.zig").Service, snapshot: []const u8) !*Owner {
    var opened = try local.serverless_object_store_support.OpenedObjectStore.initWithClient(a, client, "pins", "");
    errdefer opened.deinit();
    const prefix = try a.dupe(u8, "shared");
    errdefer a.free(prefix);
    const id = try a.dupe(u8, snapshot);
    errdefer a.free(id);
    const owner = try a.create(Owner);
    errdefer a.destroy(owner);
    owner.* = .{ .a = a, .opened = opened, .prefix = prefix, .snapshot = id, .parent = context, .io = context.io.?, .service = service };
    _ = try owner.start();
    return owner;
}

test "external lake snapshot service shares bounded execution across readers and isolates refusal" {
    const Runnable = local.common_request_admission.runnable_scheduler;
    const Service = @import("lake_renewal_service.zig").Service;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    var filesystem = try objectstore.FilesystemClient.initWithIo(a, root, std.testing.io);
    defer filesystem.deinit();
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3 });
    defer scheduler.close();
    var service: Service = .{ .interval_ns = 5 * std.time.ns_per_ms };
    service.configure(std.testing.io, &scheduler);
    defer service.deinit();
    var executions: [8]Runnable.Execution = @splat(.{});
    defer for (&executions) |*execution| execution.finish();
    var flags: [Service.limit]std.atomic.Value(bool) = @splat(.init(false));
    var owners: [Service.limit]?*Owner = @splat(null);
    defer for (&owners) |*owner| if (owner.*) |live| Owner.destroy(live);
    const deadline = platform.time.monotonicNs() + 10 * std.time.ns_per_s;
    for (&owners, 0..) |*owner, index| {
        const execution = &executions[index % executions.len];
        if (index < executions.len) try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 4096 }, .query) else try execution.enter(.query);
        var buffer: [32]u8 = undefined;
        const id = try std.fmt.bufPrint(&buffer, "snapshot-{d}", .{index});
        owner.* = try fixtureOwner(a, filesystem.client(), (Context{ .io = std.testing.io, .deadline_ns = deadline, .cancellation = .fromAtomic(&flags[index]) }).withExecution(execution), &service, id);
        try std.testing.expect(owner.*.?.heartbeat == null);
        try execution.park(.resource_blocked);
    }
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_services);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().control_children);
    try executions[0].enter(.query);
    const overflow = (Context{ .io = std.testing.io, .deadline_ns = deadline }).withExecution(&executions[0]);
    try std.testing.expectError(error.SchedulerFull, fixtureOwner(a, filesystem.client(), overflow, &service, "overflow"));
    try executions[0].park(.resource_blocked);
    var health: Runnable.Execution = .{};
    defer health.finish();
    try health.start(&scheduler, .{ .io = std.testing.io, .deadline_ns = deadline }, .control);
    health.finish();
    const healthy = owners[owners.len - 1].?;
    const before = healthy.unix_deadline.load(.acquire);
    flags[0].store(true, .release);
    while (true) {
        const refused = if (owners[0].?.refusal.check()) |_| false else |_| true;
        if (refused and healthy.unix_deadline.load(.acquire) > before) break;
        if (platform.time.monotonicNs() >= deadline) return error.SharedSnapshotRenewalDidNotProgress;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectError(error.Canceled, Owner.check(owners[0].?));
    try Owner.check(healthy);
    for (&owners) |*owner| {
        Owner.destroy(owner.*.?);
        owner.* = null;
    }
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().control_services);
    try std.testing.expectEqual(@as(usize, 0), service.count);
    for (&executions) |*execution| execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "external lake snapshot service recovers its execution without reviving failed readers" {
    const Runnable = local.common_request_admission.runnable_scheduler;
    const Service = @import("lake_renewal_service.zig").Service;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(root);
    var filesystem = try objectstore.FilesystemClient.initWithIo(a, root, std.testing.io);
    defer filesystem.deinit();
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 3, .max_wait_ms = 50 });
    defer scheduler.close();
    var service: Service = .{ .interval_ns = 5 * std.time.ns_per_ms };
    service.configure(std.testing.io, &scheduler);
    defer service.deinit();
    var execution: Runnable.Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io }, .query);
    const context = (Context{ .io = std.testing.io, .deadline_ns = platform.time.monotonicNs() + 5 * std.time.ns_per_s }).withExecution(&execution);
    const old = try fixtureOwner(a, filesystem.client(), context, &service, "old");
    var old_owned = true;
    defer if (old_owned) Owner.destroy(old);
    try execution.park(.resource_blocked);
    var health: Runnable.Execution = .{};
    defer health.finish();
    try health.start(&scheduler, .{ .io = std.testing.io }, .control);
    const until = platform.time.monotonicNs() + 3 * std.time.ns_per_s;
    while (!service.failed.load(.acquire)) {
        if (platform.time.monotonicNs() >= until) return error.ServiceDidNotObserveDeniedControl;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectError(error.LakeSnapshotReadLeaseExpired, Owner.check(old));
    health.finish();
    var next_execution: Runnable.Execution = .{};
    defer next_execution.finish();
    try next_execution.start(&scheduler, .{ .io = std.testing.io }, .query);
    const next_context = (Context{ .io = std.testing.io, .deadline_ns = context.deadline_ns }).withExecution(&next_execution);
    const next = try fixtureOwner(a, filesystem.client(), next_context, &service, "new");
    var next_owned = true;
    defer if (next_owned) Owner.destroy(next);
    const initial = next.unix_deadline.load(.acquire);
    try next_execution.park(.resource_blocked);
    while (next.unix_deadline.load(.acquire) <= initial) {
        try Owner.check(next);
        if (platform.time.monotonicNs() >= until) return error.ServiceDidNotRecover;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectError(error.LakeSnapshotReadLeaseExpired, Owner.check(old));
    try Owner.check(next);
    Owner.destroy(old);
    old_owned = false;
    Owner.destroy(next);
    next_owned = false;
    execution.finish();
    next_execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}
