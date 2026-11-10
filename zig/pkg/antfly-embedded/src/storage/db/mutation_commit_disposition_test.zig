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

//! Real durable effects distinguish precommit retry from publication failure.
const std = @import("std");
const DB = @import("db.zig").DB;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;

test "prepared commit disposition never retries a committed increment for retry-shaped or resource errors" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("prepared-commit-disposition");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    const Observer = struct {
        calls: usize = 0,
        failure: anyerror,
        committed: *bool,
        fn observe(raw: *anyopaque, _: []const u8) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.committed.*);
            self.calls += 1;
            return self.failure;
        }
    };
    for ([_]anyerror{ error.PreparedGenerationChanged, error.PreparedReadSetChanged, error.OutOfMemory, error.Canceled }) |failure| {
        const key = @errorName(failure);
        try db.batch(.{ .writes = &.{.{ .key = key, .value = "{\"count\":0}" }}, .sync_level = .write });
        var committed = false;
        var observer: Observer = .{ .failure = failure, .committed = &committed };
        const expected = if (failure == error.Canceled) error.CommitVisibilityNotSatisfied else failure;
        try std.testing.expectError(expected, db.batchInternal(.{ .transforms = &.{.{ .key = key, .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} }}, .sync_level = .write }, null, .{ .primary_committed = &committed, .committed_batch_effects_observer = .{ .ptr = &observer, .apply = Observer.observe } }));
        try std.testing.expect(committed);
        try std.testing.expectEqual(@as(usize, 1), observer.calls);
        const stored = (try db.get(a, key)).?;
        defer a.free(stored);
        try std.testing.expectEqualStrings("{\"count\":1}", stored);
    }
    // Every publication failure released the apply/replication/snapshot leases.
    try db.batch(.{ .writes = &.{.{ .key = "after", .value = "{}" }}, .sync_level = .write });
}

test "prepared commit disposition allocation faults preserve one durable increment and release partial preparation" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("prepared-commit-allocation");
    defer directory.cleanup();
    var db = try DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    var precommit_faults: usize = 0;
    const Exercise = struct {
        fn run(faults: std.mem.Allocator, database: *DB, observed: *usize) !void {
            const base = std.testing.allocator;
            try database.batch(.{ .writes = &.{.{ .key = "counter", .value = "{\"count\":0}" }}, .sync_level = .write });
            var allocator: @import("execution_resources.zig").PreparedRowAllocator = .{ .child = faults, .io = std.testing.io };
            var committed = false;
            database.batchInternalWithPreparationAllocator(.{ .transforms = &.{.{ .key = "counter", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} }}, .sync_level = .write }, null, .{ .primary_committed = &committed }, &allocator) catch |err| {
                if (err != error.OutOfMemory) return err;
                if (!committed) observed.* += 1;
                const stored = (try database.get(base, "counter")).?;
                defer base.free(stored);
                try std.testing.expectEqualStrings(if (committed) "{\"count\":1}" else "{\"count\":0}", stored);
                // Use the ordinary allocator after the fault to detect retained
                // admission/fences, independently of the failed allocator.
                try database.batch(.{ .writes = &.{.{ .key = "after-fault", .value = "{}" }}, .sync_level = .write });
                return err;
            };
            try std.testing.expect(committed);
            const stored = (try database.get(base, "counter")).?;
            defer base.free(stored);
            try std.testing.expectEqualStrings("{\"count\":1}", stored);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Exercise.run, .{ &db, &precommit_faults });
    try std.testing.expect(precommit_faults > 0);
}

test "prepared commit disposition durable receipt prevents replay after publication failure and reopen" {
    const a = std.testing.allocator;
    var directory = try TestDirectory.init("prepared-commit-restart");
    defer directory.cleanup();
    const request: @import("types.zig").BatchRequest = .{ .transforms = &.{.{ .key = "counter", .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }} }}, .sync_level = .write };
    const identity: @import("types.zig").OrderedApplyReceipt = .{ .term = 1, .index = 1 };
    {
        var db = try DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer db.close();
        try db.batch(.{ .writes = &.{.{ .key = "counter", .value = "{\"count\":0}" }}, .sync_level = .write });
        const Failure = struct {
            fn observe(_: *anyopaque, _: []const u8) anyerror!void {
                return error.PreparedReadSetChanged;
            }
        };
        var context: u8 = 0;
        var committed = false;
        try std.testing.expectError(error.PreparedReadSetChanged, db.batchInternal(request, null, .{ .primary_committed = &committed, .ordered_apply_receipt = identity, .committed_batch_effects_observer = .{ .ptr = &context, .apply = Failure.observe } }));
        try std.testing.expect(committed);
    }
    var reopened = try DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer reopened.close();
    // A restart supplies the original committed entry, not a new user attempt.
    try reopened.applyOrderedCommittedMutation(request, identity, false);
    const stored = (try reopened.get(a, "counter")).?;
    defer a.free(stored);
    try std.testing.expectEqualStrings("{\"count\":1}", stored);
    try reopened.batch(.{ .writes = &.{.{ .key = "after-reopen", .value = "{}" }}, .sync_level = .write });
}
