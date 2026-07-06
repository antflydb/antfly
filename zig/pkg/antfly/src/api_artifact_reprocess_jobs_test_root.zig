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

const std = @import("std");

const artifact_reprocess_jobs = @import("api/artifact_reprocess_jobs.zig");
const repair_jobs = @import("api/repair_jobs.zig");
const db_mod = @import("storage/db/mod.zig");

test {
    std.testing.refAllDecls(artifact_reprocess_jobs);
    std.testing.refAllDecls(repair_jobs);
}

test "repair job store starts and records a pass" {
    const alloc = std.testing.allocator;
    var store = repair_jobs.Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", .{ .target = "artifact", .limit = 2 });
    defer alloc.free(started);
    var parsed_start = try std.json.parseFromSlice(repair_jobs.JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_start.deinit();

    const begin = try store.beginAdvance(alloc, parsed_start.value);
    defer alloc.free(begin.encoded);
    try std.testing.expect(begin.started);
    var parsed_running = try std.json.parseFromSlice(repair_jobs.JobState, alloc, begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_running.deinit();
    try std.testing.expectEqual(@as(u64, 1), parsed_running.value.attempt_id);

    try store.heartbeatRunning(alloc, parsed_running.value.job_id, parsed_running.value.attempt_id);
    const after_heartbeat = (try store.loadJobAlloc(alloc, parsed_running.value.job_id)).?;
    defer alloc.free(after_heartbeat);
    var parsed_after_heartbeat = try std.json.parseFromSlice(repair_jobs.JobState, alloc, after_heartbeat, .{ .ignore_unknown_fields = true });
    defer parsed_after_heartbeat.deinit();
    try std.testing.expectEqual(parsed_running.value.attempt_id, parsed_after_heartbeat.value.attempt_id);

    var pass: db_mod.types.ArtifactRepairResult = .{
        .scanned = 2,
        .repaired = 2,
        .limit = 2,
        .has_more = true,
        .debt_remaining = true,
        .next_cursor = try alloc.dupe(u8, "cursor-1"),
    };
    defer pass.deinit(alloc);

    const updated = try store.recordPass(alloc, parsed_running.value, pass);
    defer alloc.free(updated);
    var parsed_update = try std.json.parseFromSlice(repair_jobs.JobState, alloc, updated, .{ .ignore_unknown_fields = true });
    defer parsed_update.deinit();
    try std.testing.expectEqualStrings("queued", parsed_update.value.phase);
    try std.testing.expectEqualStrings("cursor-1", parsed_update.value.cursor.?);
    try std.testing.expectEqual(@as(u64, 2), parsed_update.value.result.repaired);
}
