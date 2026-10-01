// Copyright 2026 Antfly, Inc.
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

//! One supervised immutable-export job per native owner. RPC deadlines govern
//! scheduling/polling, never the lifetime of an already admitted export.
const std = @import("std");
const builtin = @import("builtin");
const DB = @import("db.zig").DB;
const Scope = @import("online_source_contract.zig").Scope;
const Certificate = @import("../source_snapshot.zig").Certificate;
const Cancellation = @import("types.zig").CancellationToken;
const pin = @import("source_pin.zig");

pub const Gate = struct { entered: std.Io.Event = .unset, proceed: std.Io.Event = .unset };
pub const Job = struct {
    mutex: std.Io.Mutex = .init,
    future: ?std.Io.Future(void) = null,
    scope: ?Scope = null,
    canceled: std.atomic.Value(bool) = .init(false),
    completed: std.Io.Event = .unset,
    failure: ?anyerror = null,
    closing: bool = false,
    test_gate: if (builtin.is_test) ?*Gate else void = if (builtin.is_test) null else {},

    /// Signal only: release must never join an exporter or wait for its file
    /// lock. The short scope mutex is never held across export/filesystem I/O.
    pub fn cancelScope(self: *Job, io: std.Io, scope: Scope) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        if (self.scope) |active| if (std.meta.eql(active, scope)) self.canceled.store(true, .release);
    }

    /// DB.close calls this before releasing any owner/runtime/store resource.
    pub fn stop(self: *Job, io: std.Io) void {
        self.mutex.lockUncancelable(io);
        self.closing = true;
        self.canceled.store(true, .release);
        var future = self.future;
        self.future = null;
        self.mutex.unlock(io);
        if (future) |*task| task.cancel(io);
    }

    pub fn poll(self: *Job, db: *DB, scope: Scope, cancellation: Cancellation) !?Certificate {
        try cancellation.check();
        if (!db.stable_address) return error.UnsupportedOperation;
        const io = db.backend_runtime.io() orelse return error.BackendRuntimeIoUnavailable;
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
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 });
        var gate: Gate = .{};
        db.source_publication.test_gate = &gate;
        try std.testing.expect(try db.source_publication.poll(db, scope, .none) == null);
        try gate.entered.wait(io);
        const task = db.source_publication.future.?.any_future;
        for (0..3) |_| try std.testing.expect(try db.source_publication.poll(db, scope, .none) == null);
        const canceled: std.atomic.Value(bool) = .init(true);
        try std.testing.expectError(error.Canceled, db.source_publication.poll(db, scope, Cancellation.fromAtomic(&canceled)));
        try std.testing.expect(!db.source_publication.completed.isSet());
        try std.testing.expect(task == db.source_publication.future.?.any_future);
        gate.proceed.set(io);
        try db.source_publication.completed.wait(io);
        certificate = (try db.source_publication.poll(db, scope, .none)).?;
        try std.testing.expectEqual(@as(u64, 1), certificate.cut.applied_index);
        try std.testing.expect(certificate.eql((try db.source_publication.poll(db, scope, .none)).?));
    }
    {
        const db = try DB.openOwned(alloc, path, options);
        var open = true;
        defer if (open) db.closeOwned();
        // Restart discovers the durable completed receipt without scheduling
        // another exporter or recapturing the live primary.
        try std.testing.expect(certificate.eql((try db.source_publication.poll(db, scope, .none)).?));
        try std.testing.expect(db.source_publication.future == null);
        scope.consumer_epoch = 2;
        scope.fence.transition_id = 8;
        scope.copy_attempt.sequence = 2;
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 2 });
        var gate: Gate = .{};
        db.source_publication.test_gate = &gate;
        try std.testing.expect(try db.source_publication.poll(db, scope, .none) == null);
        try gate.entered.wait(io);
        // Gate remains closed. Teardown must cancel the independent I/O task
        // and join it before releasing its DB, allocator, or filesystem.
        db.closeOwned();
        open = false;
    }
    {
        const db = try DB.openOwned(alloc, path, options);
        defer db.closeOwned();
        try std.testing.expect(try db.source_publication.poll(db, scope, .none) == null);
        try db.source_publication.completed.wait(io);
        const retried = (try db.source_publication.poll(db, scope, .none)).?;
        try std.testing.expectEqual(@as(u64, 2), retried.cut.applied_index);
        const old_scope = scope;
        scope.consumer_epoch = 3;
        scope.fence.transition_id = 9;
        scope.copy_attempt.sequence = 3;
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 3 });
        var gate: Gate = .{};
        db.source_publication.test_gate = &gate;
        try std.testing.expect(try db.source_publication.poll(db, scope, .none) == null);
        try gate.entered.wait(io);
        db.source_publication.cancelScope(io, old_scope);
        try std.testing.expect(!db.source_publication.canceled.load(.acquire));
        // Replicated release signals only the matching exporter. It returns
        // without opening its gate or joining the job; the worker observes
        // the cancellation independently and relinquishes its pin resources.
        try db.batchRaftReplicatedApply(.{ .online_source = .{ .release = scope } }, .{ .term = 1, .index = 4 });
        try std.testing.expect(db.source_publication.canceled.load(.acquire));
        try db.source_publication.completed.wait(io);
        try std.testing.expectEqual(error.Canceled, db.source_publication.failure.?);
        try std.testing.expectEqual(@import("online_source.zig").Phase.released, (try db.onlineSourceStatus(scope)).phase);
    }
}
