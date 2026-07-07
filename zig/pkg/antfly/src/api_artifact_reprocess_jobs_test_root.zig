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
const http_client = @import("api/http_client.zig");
const http_internal_group_write_routes = @import("api/http_internal_group_write_routes.zig");
const repair_jobs = @import("api/repair_jobs.zig");
const db_mod = @import("storage/db/mod.zig");

test {
    std.testing.refAllDecls(artifact_reprocess_jobs);
    std.testing.refAllDecls(http_client);
    std.testing.refAllDecls(http_internal_group_write_routes);
    std.testing.refAllDecls(repair_jobs);
}

test "api http client maps remote repair cancel unavailable" {
    try http_client.expectGroupArtifactRepairRunMapsCancelUnavailableForTest();
}

test "internal group artifact repair rejects callback token without cancel executor" {
    try http_internal_group_write_routes.expectRejectsCallbackTokenWithoutCancelExecutorForTest();
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

test "repair job store applies running cancellation at pass boundary" {
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

    const cancelling = try store.requestCancel(alloc, parsed_running.value);
    defer alloc.free(cancelling);
    var parsed_cancelling = try std.json.parseFromSlice(repair_jobs.JobState, alloc, cancelling, .{ .ignore_unknown_fields = true });
    defer parsed_cancelling.deinit();
    try std.testing.expectEqualStrings("running", parsed_cancelling.value.phase);
    try std.testing.expect(parsed_cancelling.value.cancel_requested);

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
    try std.testing.expectEqualStrings("cancelled", parsed_update.value.phase);
    try std.testing.expectEqualStrings("stopped", parsed_update.value.repair_status);
    try std.testing.expect(parsed_update.value.cancel_requested);
    try std.testing.expectEqualStrings("cancel_requested", parsed_update.value.last_error.?);
    try std.testing.expectEqual(@as(u64, 2), parsed_update.value.result.repaired);
}

test "repair job store records cancel requested across stale queued token" {
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

    const cancelling = try store.requestCancel(alloc, parsed_start.value);
    defer alloc.free(cancelling);
    var parsed_cancelling = try std.json.parseFromSlice(repair_jobs.JobState, alloc, cancelling, .{ .ignore_unknown_fields = true });
    defer parsed_cancelling.deinit();
    try std.testing.expectEqualStrings("running", parsed_cancelling.value.phase);
    try std.testing.expect(parsed_cancelling.value.cancel_requested);
    try std.testing.expectEqualStrings("cancel_requested", parsed_cancelling.value.last_error.?);
}

test "artifact reprocess job store applies running cancellation at pass boundary" {
    const alloc = std.testing.allocator;
    var store = artifact_reprocess_jobs.Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", "document_units_v1", .{ .limit = 2 });
    defer alloc.free(started);
    var parsed_start = try std.json.parseFromSlice(artifact_reprocess_jobs.JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_start.deinit();

    const begin = try store.beginAdvance(alloc, parsed_start.value);
    defer alloc.free(begin.encoded);
    try std.testing.expect(begin.started);
    var parsed_running = try std.json.parseFromSlice(artifact_reprocess_jobs.JobState, alloc, begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_running.deinit();

    const cancelling = try store.requestCancel(alloc, parsed_running.value);
    defer alloc.free(cancelling);
    var parsed_cancelling = try std.json.parseFromSlice(artifact_reprocess_jobs.JobState, alloc, cancelling, .{ .ignore_unknown_fields = true });
    defer parsed_cancelling.deinit();
    try std.testing.expectEqualStrings("running", parsed_cancelling.value.phase);
    try std.testing.expect(parsed_cancelling.value.cancel_requested);

    const pass = db_mod.types.DocumentArtifactTableReprocessResult{
        .scanned = 2,
        .reprocessed = 2,
        .limit = 2,
    };
    const updated = try store.recordPass(alloc, parsed_running.value, pass);
    defer alloc.free(updated);
    var parsed_update = try std.json.parseFromSlice(artifact_reprocess_jobs.JobState, alloc, updated, .{ .ignore_unknown_fields = true });
    defer parsed_update.deinit();
    try std.testing.expectEqualStrings("cancelled", parsed_update.value.phase);
    try std.testing.expectEqualStrings("stopped", parsed_update.value.reprocess_status);
    try std.testing.expect(parsed_update.value.cancel_requested);
    try std.testing.expectEqualStrings("cancel_requested", parsed_update.value.last_error.?);
    try std.testing.expectEqual(@as(usize, 2), parsed_update.value.reprocessed);
}

test "artifact reprocess job store records cancel requested across stale queued token" {
    const alloc = std.testing.allocator;
    var store = artifact_reprocess_jobs.Store.init(alloc, .{});
    defer store.deinit();

    const started = try store.startJob(alloc, "docs", "document_units_v1", .{ .limit = 2 });
    defer alloc.free(started);
    var parsed_start = try std.json.parseFromSlice(artifact_reprocess_jobs.JobState, alloc, started, .{ .ignore_unknown_fields = true });
    defer parsed_start.deinit();

    const begin = try store.beginAdvance(alloc, parsed_start.value);
    defer alloc.free(begin.encoded);
    try std.testing.expect(begin.started);

    const cancelling = try store.requestCancel(alloc, parsed_start.value);
    defer alloc.free(cancelling);
    var parsed_cancelling = try std.json.parseFromSlice(artifact_reprocess_jobs.JobState, alloc, cancelling, .{ .ignore_unknown_fields = true });
    defer parsed_cancelling.deinit();
    try std.testing.expectEqualStrings("running", parsed_cancelling.value.phase);
    try std.testing.expect(parsed_cancelling.value.cancel_requested);
    try std.testing.expectEqualStrings("cancel_requested", parsed_cancelling.value.last_error.?);
}

test "repair job store does not expire future live running heartbeat" {
    const alloc = std.testing.allocator;
    var store = repair_jobs.Store.init(alloc, .{});
    defer store.deinit();

    const now_ms = repair_jobs.nowMillis();
    const encoded = try repair_jobs.encodeState(alloc, .{
        .job_id = 7001,
        .attempt_id = 4,
        .table_name = "docs",
        .phase = repair_jobs.phaseString(.running),
        .repair_status = "in_progress",
        .target = "artifact",
        .limit = 10,
        .result = .{ .limit = 10 },
        .created_at_millis = now_ms,
        .last_updated_at_millis = now_ms + 60_000,
        .expires_at_millis = now_ms + 120_000,
    });
    defer alloc.free(encoded);
    try store.storeEncodedForTest(7001, encoded);
    var parsed = try std.json.parseFromSlice(repair_jobs.JobState, alloc, encoded, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();

    const begin = try store.beginAdvance(alloc, parsed.value);
    defer alloc.free(begin.encoded);
    try std.testing.expect(!begin.started);
    var parsed_begin = try std.json.parseFromSlice(repair_jobs.JobState, alloc, begin.encoded, .{ .ignore_unknown_fields = true });
    defer parsed_begin.deinit();
    try std.testing.expectEqual(@as(u64, 4), parsed_begin.value.attempt_id);
    try std.testing.expectEqualStrings("running", parsed_begin.value.phase);
}
