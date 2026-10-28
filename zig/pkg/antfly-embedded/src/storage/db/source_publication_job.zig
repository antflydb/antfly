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

//! One supervised immutable-export job per native owner. RPC deadlines govern
//! scheduling/polling, never the lifetime of an already admitted export.
const std = @import("std");
const builtin = @import("builtin");
const DB = @import("db.zig").DB;
const Scope = @import("online_source_contract.zig").Scope;
const Certificate = @import("../source_snapshot.zig").Certificate;
const Cancellation = @import("types.zig").CancellationToken;
const pin = @import("source_pin.zig");

pub const Gate = struct { entered: std.Io.Event = .unset, proceed: std.Io.Event = .unset, cleanup: ?*std.Io.Event = null };
pub const Job = struct {
    mutex: std.Io.Mutex = .init,
    future: ?std.Io.Future(void) = null,
    scope: ?Scope = null,
    canceled: std.atomic.Value(bool) = .init(false),
    completed: std.Io.Event = .unset,
    failure: ?anyerror = null,
    closing: bool = false,
    stop_started: bool = false,
    stopped: bool = false,
    stop_changed: std.Io.Condition = .init,
    stop_generation: std.atomic.Value(u64) = .init(0),
    in_flight: std.atomic.Value(u32) = .init(0),
    test_gate: if (builtin.is_test) ?*Gate else void = if (builtin.is_test) null else {},
    test_admission_gate: if (builtin.is_test) ?*Gate else void = if (builtin.is_test) null else {},

    /// Signal only: release must never join an exporter or wait for its file
    /// lock. The short scope mutex is never held across export/filesystem I/O.
    pub fn cancelScope(self: *Job, io: std.Io, scope: Scope) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.scope) |active| if (std.meta.eql(active, scope)) self.canceled.store(true, .release);
    }

    fn finishStop(self: *Job, io: std.Io, complete: bool) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        self.stop_started = false;
        if (complete) self.stopped = true;
        _ = self.stop_generation.fetchAdd(1, .release);
        self.stop_changed.broadcast(io);
    }
    /// Compatibility join requires quiesced cleanup ownership. Followers
    /// observe physical completion or take over an abandoned controlled join.
    pub fn stop(self: *Job, io: std.Io) void {
        while (true) {
            self.mutex.lockUncancelable(io);
            self.closing = true;
            self.canceled.store(true, .release);
            if (self.stopped) {
                self.mutex.unlock(io);
                return;
            }
            if (self.stop_started) {
                self.stop_changed.waitUncancelable(io, &self.mutex);
                self.mutex.unlock(io);
                continue;
            }
            self.stop_started = true;
            var future = self.future;
            self.mutex.unlock(io);
            if (future) |*task| task.cancel(io);
            self.mutex.lockUncancelable(io);
            self.future = null;
            self.mutex.unlock(io);
            while (self.in_flight.load(.acquire) != 0) io.sleep(.fromMilliseconds(1), .awake) catch {};
            self.finishStop(io, true);
            return;
        }
    }
    pub fn stopWithAdmission(self: *Job, backend: *@import("../background_runtime.zig").BackendRuntime, original: @import("../../common/read_admission.zig").Port) !void {
        const wait = @import("../../common/cleanup_wait.zig");
        const io = backend.io() orelse backend.filesystemIo() orelse std.Options.debug_io;
        while (true) {
            try wait.lockIo(&self.mutex, io, original);
            self.closing = true;
            self.canceled.store(true, .release);
            if (self.stopped) {
                self.mutex.unlock(io);
                return;
            }
            if (self.stop_started) {
                const Progress = struct {
                    generation: *std.atomic.Value(u64),
                    previous: u64,
                    fn ready(raw: *anyopaque) anyerror!bool {
                        const value: *@This() = @ptrCast(@alignCast(raw));
                        return value.generation.load(.acquire) != value.previous;
                    }
                };
                var progress: Progress = .{ .generation = &self.stop_generation, .previous = self.stop_generation.load(.acquire) };
                self.mutex.unlock(io);
                try wait.until(io, original, &progress, Progress.ready, null);
                continue;
            }
            self.stop_started = true;
            var future = self.future;
            self.mutex.unlock(io);
            errdefer self.finishStop(io, false);
            if (future) |*task| {
                const Ready = struct {
                    fn get(raw: *anyopaque) anyerror!bool {
                        const value: *Job = @ptrCast(@alignCast(raw));
                        return value.completed.isSet();
                    }
                };
                try wait.until(io, original, self, Ready.get, &self.completed);
                // The worker publishes completed under this mutex. Reacquire
                // after CPU reentry to observe its final DB/context access.
                try wait.lockIo(&self.mutex, io, original);
                self.mutex.unlock(io);
                task.await(io);
                self.mutex.lockUncancelable(io);
                self.future = null;
                self.mutex.unlock(io);
            }
            try @import("../background_runtime.zig").waitForDirectAdmissionsWithAdmission(backend, &self.in_flight, original);
            self.finishStop(io, true);
            return;
        }
    }

    pub fn poll(self: *Job, db: *DB, scope: Scope, cancellation: Cancellation) !?Certificate {
        try cancellation.check();
        if (!db.stable_address) return error.UnsupportedOperation;
        const io = db.backend_runtime.io() orelse return error.BackendRuntimeIoUnavailable;
        try self.mutex.lock(io);
        if (self.closing) {
            self.mutex.unlock(io);
            return error.Canceled;
        }
        if (self.in_flight.load(.monotonic) == std.math.maxInt(u32)) {
            self.mutex.unlock(io);
            return error.ResourceLimitExceeded;
        }
        _ = self.in_flight.fetchAdd(1, .acquire);
        self.mutex.unlock(io);
        defer _ = self.in_flight.fetchSub(1, .release);
        if (builtin.is_test) if (self.test_admission_gate) |gate| {
            gate.entered.set(io);
            gate.proceed.waitUncancelable(io);
        };
        // Reuse already verified durable publication across request retries
        // and owner restarts. This is a compact stat/receipt read, not export.
        if (try pin.publicationCertificateIfPresent(db, scope, cancellation)) |certificate| return certificate;
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        if (self.closing) return error.Canceled;
        if (self.future) |*task| {
            if (!self.completed.isSet()) {
                if (!std.meta.eql(self.scope.?, scope)) return error.OnlineSourcePinPending;
                return null;
            }
            // Completion is published after the worker releases all export
            // locks. Joining here cannot wait for a running table export.
            task.await(io);
            self.future = null;
            if (std.meta.eql(self.scope.?, scope)) if (self.failure) |err| {
                self.failure = null;
                return err;
            };
        }
        self.scope = scope;
        self.failure = null;
        self.canceled.store(false, .release);
        self.completed.reset();
        self.future = io.concurrent(run, .{ self, db, scope, io }) catch return error.OnlineSourcePinPending;
        return null;
    }

    fn run(self: *Job, db: *DB, scope: Scope, io: std.Io) void {
        const failure: ?anyerror = work(self, db, scope, io) catch |err| err;
        if (builtin.is_test) if (self.test_gate) |gate| if (gate.cleanup) |cleanup| cleanup.waitUncancelable(io);
        self.mutex.lockUncancelable(io);
        self.failure = failure;
        self.completed.set(io);
        self.mutex.unlock(io);
    }

    fn work(self: *Job, db: *DB, scope: Scope, io: std.Io) !?anyerror {
        if (builtin.is_test) if (self.test_gate) |gate| {
            gate.entered.set(io);
            while (!gate.proceed.isSet()) {
                try Cancellation.fromAtomic(&self.canceled).check();
                try std.Io.sleep(io, .fromMilliseconds(1), .awake);
            }
        };
        _ = try pin.preparePublication(db, scope, Cancellation.fromAtomic(&self.canceled));
        return null;
    }
};

test "relational index system source publication job survives polling deadlines reuses receipts and joins on close" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/publisher", .{tmp.sub_path});
    defer alloc.free(path);
    const options: @import("db.zig").OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var scope: Scope = undefined;
    var certificate: Certificate = undefined;
    {
        const db = try DB.openOwned(alloc, path, options);
        defer db.closeOwned();
        try db.setSchemaJson(alloc, "{}");
        try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"original\":true}" }} });
        const identity = try db.relationalTopologyIdentity();
        scope = .{ .fence = .{ .admission_epoch = identity.next_epoch, .transition_id = 7, .attempt = 1, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = identity.namespace, .catalog_digest = identity.catalog_digest }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
        try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 });
        var gate: Gate = .{};
        db.local_execution.source_publication.test_gate = &gate;
        try std.testing.expect(try db.local_execution.source_publication.poll(db, scope, .none) == null);
        try gate.entered.wait(io);
        const task = db.local_execution.source_publication.future.?.any_future;
        for (0..3) |_| try std.testing.expect(try db.local_execution.source_publication.poll(db, scope, .none) == null);
        const canceled: std.atomic.Value(bool) = .init(true);
        try std.testing.expectError(error.Canceled, db.local_execution.source_publication.poll(db, scope, Cancellation.fromAtomic(&canceled)));
        try std.testing.expect(!db.local_execution.source_publication.completed.isSet());
        try std.testing.expect(task == db.local_execution.source_publication.future.?.any_future);
        gate.proceed.set(io);
        try db.local_execution.source_publication.completed.wait(io);
        certificate = (try db.local_execution.source_publication.poll(db, scope, .none)).?;
        try std.testing.expectEqual(@as(u64, 1), certificate.cut.applied_index);
        try std.testing.expect(certificate.eql((try db.local_execution.source_publication.poll(db, scope, .none)).?));
    }
    {
        const db = try DB.openOwned(alloc, path, options);
        var open = true;
        defer if (open) db.closeOwned();
        // Restart discovers the durable completed receipt without scheduling
        // another exporter or recapturing the live primary.
        try std.testing.expect(certificate.eql((try db.local_execution.source_publication.poll(db, scope, .none)).?));
        try std.testing.expect(db.local_execution.source_publication.future == null);
        scope.consumer_epoch = 2;
        scope.fence.transition_id = 8;
        scope.copy_attempt.sequence = 2;
        try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 2 });
        var gate: Gate = .{};
        db.local_execution.source_publication.test_gate = &gate;
        try std.testing.expect(try db.local_execution.source_publication.poll(db, scope, .none) == null);
        try gate.entered.wait(io);
        // Gate remains closed. Teardown must cancel the independent I/O task
        // and join it before releasing its DB, allocator, or filesystem.
        db.closeOwned();
        open = false;
    }
    {
        const db = try DB.openOwned(alloc, path, options);
        defer db.closeOwned();
        try std.testing.expect(try db.local_execution.source_publication.poll(db, scope, .none) == null);
        try db.local_execution.source_publication.completed.wait(io);
        const retried = (try db.local_execution.source_publication.poll(db, scope, .none)).?;
        try std.testing.expectEqual(@as(u64, 2), retried.cut.applied_index);
        const old_scope = scope;
        scope.consumer_epoch = 3;
        scope.fence.transition_id = 9;
        scope.copy_attempt.sequence = 3;
        try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 3 });
        var gate: Gate = .{};
        db.local_execution.source_publication.test_gate = &gate;
        try std.testing.expect(try db.local_execution.source_publication.poll(db, scope, .none) == null);
        try gate.entered.wait(io);
        db.local_execution.source_publication.cancelScope(io, old_scope);
        try std.testing.expect(!db.local_execution.source_publication.canceled.load(.acquire));
        // Replicated release signals only the matching exporter. It returns
        // without opening its gate or joining the job; the worker observes
        // the cancellation independently and relinquishes its pin resources.
        try @import("antfly_server_test_sources").local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .online_source = .{ .release = scope } }, .{ .term = 1, .index = 4 });
        try std.testing.expect(db.local_execution.source_publication.canceled.load(.acquire));
        try db.local_execution.source_publication.completed.wait(io);
        try std.testing.expectEqual(error.Canceled, db.local_execution.source_publication.failure.?);
        try std.testing.expectEqual(@import("online_source.zig").Phase.released, (try db.onlineSourceStatus(scope)).phase);
    }
}

test "controlled owner source publication stop preserves exporter and polling borrows after original failure" {
    const admission = @import("../../common/read_admission.zig");
    const Case = enum { success, begin_failure, end_failure, lifetime_in_wait };
    for ([_]bool{ false, true }) |direct_poll| for (std.enums.values(Case)) |case| {
        const alloc = std.testing.allocator;
        var directory = std.testing.tmpDir(.{});
        defer directory.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/controlled-export", .{directory.sub_path});
        defer alloc.free(path);
        const db = try DB.openOwned(alloc, path, .{ .start_index_workers = false, .start_optional_runtimes = false, .start_independent_maintenance = false });
        defer db.closeOwned();
        const io = db.backend_runtime.io().?;
        const scope: Scope = .{ .fence = .{ .admission_epoch = 1, .transition_id = 7, .attempt = 1, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .catalog_digest = @splat(0) }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
        var gate: Gate = .{};
        var cleanup: std.Io.Event = .unset;
        var job: Job = .{};
        const Poller = struct {
            job: *Job,
            db: *DB,
            scope: Scope,
            fn run(self: *@This()) void {
                _ = self.job.poll(self.db, self.scope, .none) catch return;
            }
        };
        var poller: Poller = .{ .job = &job, .db = db, .scope = scope };
        var thread: ?std.Thread = null;
        defer {
            gate.proceed.set(io);
            cleanup.set(io);
            if (thread) |active| active.join();
            job.stop(io);
        }
        if (direct_poll) {
            job.test_admission_gate = &gate;
            thread = try std.Thread.spawn(.{}, Poller.run, .{&poller});
        } else {
            gate.cleanup = &cleanup;
            job.test_gate = &gate;
            job.scope = scope;
            job.future = try io.concurrent(Job.run, .{ &job, db, scope, io });
        }
        try gate.entered.wait(io);
        const Control = struct {
            gate: *Gate,
            cleanup: *std.Io.Event,
            io: std.Io,
            case: Case,
            begins: usize = 0,
            ends: usize = 0,
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.case == .lifetime_in_wait and self.begins != 0) return error.DeadlineExceeded;
            }
            fn deadline(_: *anyopaque) !?u64 {
                return 1000;
            }
            fn release(self: *@This()) void {
                self.gate.proceed.set(self.io);
                self.cleanup.set(self.io);
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                if (self.case == .begin_failure) return error.NotFound;
                if (self.case != .lifetime_in_wait) self.release();
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                self.release();
                if (self.case == .end_failure) return error.Canceled;
            }
        };
        var control: Control = .{ .gate = &gate, .cleanup = &cleanup, .io = io, .case = case };
        var payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const result = job.stopWithAdmission(db.backend_runtime, .init(&payload));
        if (case == .success) try result else {
            try std.testing.expectError(switch (case) {
                .begin_failure => error.NotFound,
                .end_failure => error.Canceled,
                .lifetime_in_wait => error.DeadlineExceeded,
                .success => unreachable,
            }, result);
            try std.testing.expect(!job.stopped and !job.stop_started);
            if (!direct_poll) try std.testing.expect(job.future != null);
            control.release();
            job.stop(io);
        }
        if (thread) |active| active.join();
        thread = null;
        try std.testing.expect(job.future == null and job.stopped);
        try std.testing.expectEqual(@as(u32, 0), job.in_flight.load(.acquire));
        try std.testing.expect(!db.isClosed());
        try std.testing.expectError(error.Canceled, job.poll(db, scope, .none));
        try std.testing.expectEqual(@as(usize, 1), control.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), control.ends);
    };
}
