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

//! Direct source owner with the ordered server adapter used by publication tests.
pub const antfly_sources = @import("source_owner_physical.zig");
pub const local_test_sources = @import("local_test_sources.zig");
pub const consumer_tests_only = true;
test {
    _ = @import("antfly_local_sources").storage_db_source_publication_job;
    _ = @import("antfly_local_sources").storage_db_online_merge_io;
}

test "controlled owner source publication stop lets competing cleanup resume an interrupted join" {
    const std = @import("std");
    const local = @import("antfly_local_sources");
    const publication = local.storage_db_source_publication_job;
    const admission = local.common_read_admission;
    const DB = local.storage_db_db.DB;
    const alloc = std.testing.allocator;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/competing-export", .{directory.sub_path});
    defer alloc.free(path);
    const db = try DB.openOwned(alloc, path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_index_workers = false, .start_optional_runtimes = false, .start_independent_maintenance = false });
    defer db.closeOwned();
    const io = db.backend_runtime.io().?;
    try db.setSchemaJson(alloc, "{}");
    try db.batch(.{ .writes = &.{.{ .key = "a", .value = "{}" }} });
    const identity = try db.relationalTopologyIdentity();
    const scope: local.storage_db_online_source_contract.Scope = .{ .fence = .{ .admission_epoch = identity.next_epoch, .transition_id = 7, .attempt = 1, .owner_group_id = 2, .peer_group_id = 3, .role = .merge_source, .namespace = identity.namespace, .catalog_digest = identity.catalog_digest }, .receiver_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .consumer_epoch = 1, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    try local_test_sources.storage_server_db_adapter.applyOrdered(&db, .{ .online_source = .{ .admit = .{ .scope = scope } } }, .{ .term = 1, .index = 1 });
    var cleanup: std.Io.Event = .unset;
    var gate: publication.Gate = .{ .cleanup = &cleanup };
    const job = &db.local_execution.source_publication;
    job.test_gate = &gate;
    const F = struct {
        job: *publication.Job,
        io: std.Io,
        thread: ?std.Thread = null,
        fn stop(self: *@This()) void {
            self.job.stop(self.io);
        }
        fn check(_: *anyopaque) !void {}
        fn deadline(_: *anyopaque) !?u64 {
            return 1000;
        }
        fn begin(raw: *anyopaque) !void {
            const value: *@This() = @ptrCast(@alignCast(raw));
            value.thread = try std.Thread.spawn(.{}, stop, .{value});
            const until = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
            while (value.job.stop_changed.state.load(.acquire).waiters == 0) {
                if (@import("antfly_platform").time.monotonicNs() >= until) return error.TestUnexpectedResult;
                @import("antfly_platform").time.yieldNow();
            }
            return error.NotFound;
        }
        fn end(_: *anyopaque) !void {
            @panic("failed begin must not reenter");
        }
    };
    var f: F = .{ .job = job, .io = io };
    defer {
        cleanup.set(io);
        gate.proceed.set(io);
        if (f.thread) |active| active.join();
        job.stop(io);
    }
    try std.testing.expect(try job.poll(db, scope, .none) == null);
    try gate.entered.wait(io);
    var payload: admission.Payload = .{ .ptr = &f, .vtable = &.{ .checkpoint = F.check, .check_lifetime = F.check, .deadline_ns = F.deadline }, .execution_wait = .{ .ptr = &f, .begin = F.begin, .end = F.end } };
    try std.testing.expectError(error.NotFound, job.stopWithAdmission(db.backend_runtime, .init(&payload)));
    job.mutex.lockUncancelable(io);
    const retained = !job.stopped and job.future != null;
    job.mutex.unlock(io);
    try std.testing.expect(retained);
    cleanup.set(io);
    f.thread.?.join();
    f.thread = null;
    try std.testing.expect(job.stopped and job.future == null);
    try std.testing.expectEqual(@as(u64, 2), job.stop_generation.load(.acquire));
    try std.testing.expect(!db.isClosed());
}
