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

pub const antfly_sources = struct {};
pub const consumer_tests_only = true;
test {
    _ = @import("storage/db/enrichment/enrichment_runtime.zig");
    _ = @import("storage/db/enrichment_runtime_owner.zig");
    _ = @import("storage/db/propagation_lifecycle.zig");
    _ = @import("storage/db/derived/runtime_types.zig");
    _ = @import("storage/db/derived/derived_executor.zig");
    _ = @import("storage/db/dense_finalization_admission.zig");
    _ = @import("storage/db/derived/io_threaded_runtime.zig");
    _ = @import("storage/db/maintenance/text_merge_runtime.zig");
}

test "controlled derived finalization retains transferred session before callback admission" {
    const std = @import("std");
    const Runtime = @import("storage/db/derived/io_threaded_runtime.zig").DerivedRuntime;
    const runtime_types = @import("storage/db/derived/runtime_types.zig");
    const derived_types = @import("storage/db/derived/derived_types.zig");
    const ManagedIndex = @import("storage/db/catalog/index_manager.zig").ManagedIndexRef;
    const admission = @import("common/read_admission.zig");
    const Worker = @typeInfo(@typeInfo(@FieldType(@FieldType(Runtime, "workers"), "items")).pointer.child).pointer.child;
    const Probe = struct {
        worker: *Worker = undefined,
        reject: bool = true,
        finishes: usize = 0,
        persists: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.reject and self.worker.finalization.started) return error.NotFound;
        }
        fn deadline(_: *anyopaque) !?u64 {
            return 1000;
        }
        fn apply(_: *anyopaque, _: derived_types.DerivedBatch, _: ManagedIndex, _: runtime_types.CatchUpSessionToken) !bool {
            return true;
        }
        fn truncate(_: *anyopaque, _: u64) !void {}
        fn persist(raw: *anyopaque, _: []const u8, _: u64, _: bool) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.persists += 1;
            return true;
        }
        fn finish(raw: *anyopaque, _: ManagedIndex, token: runtime_types.CatchUpSessionToken, _: u64, _: bool) !runtime_types.CatchUpFinishResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(u64, 7), token.value);
            self.finishes += 1;
            return .{};
        }
    };
    for ([_]bool{ false, true }) |physical_retry| {
        var probe: Probe = .{};
        const alloc = std.testing.allocator;
        var runtime = try Runtime.init(alloc, undefined, &probe, Probe.apply, Probe.persist, Probe.truncate, null, Probe.finish, null, null, null);
        var live = true;
        defer if (live) runtime.deinit();
        const worker = try alloc.create(Worker);
        const name = try alloc.dupe(u8, "transferred");
        worker.* = .{ .runtime = &runtime, .name = name, .kind = .{ .name = name, .kind = .dense_vector }, .applied_sequence = 1, .persisted_sequence = 0, .target_sequence = 1, .catch_up_open = true, .catch_up_token = .{ .value = 7 } };
        try runtime.workers.append(alloc, worker);
        probe.worker = worker;
        var payload: admission.Payload = .{ .ptr = &probe, .vtable = &.{ .checkpoint = Probe.check, .check_lifetime = Probe.check, .deadline_ns = Probe.deadline }, .execution_wait = .{ .ptr = &probe, .begin = Probe.check, .end = Probe.check } };
        try std.testing.expectError(error.NotFound, runtime.finalizeWithAdmission(.init(&payload)));
        try std.testing.expect(worker.finalization.started and !worker.finalization.finish_done and !worker.catch_up_open);
        try std.testing.expect(worker.finalization.had_session and worker.finalization.token.value == 7);
        try std.testing.expectEqual(@as(usize, 0), probe.finishes);
        try std.testing.expectEqual(@as(usize, 0), probe.persists);
        probe.reject = false;
        if (!physical_retry) try runtime.finalizeWithAdmission(.init(&payload));
        runtime.deinit();
        live = false;
        try std.testing.expectEqual(@as(usize, 1), probe.finishes);
        try std.testing.expectEqual(@as(usize, 1), probe.persists);
    }
}
