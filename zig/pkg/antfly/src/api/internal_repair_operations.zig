// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral operations used by internal repair coordination.

const std = @import("std");
const operation = @import("operation.zig");
const repair_jobs = @import("repair_jobs.zig");

pub const Error = operation.ApiError;

pub const CancelStateInput = struct {
    table_name: []const u8,
    job_id: u64,
    attempt_id: u64,
};

pub const CancelState = struct {
    cancel_requested: bool,
};

pub const Operations = struct {
    store: ?*repair_jobs.Store,

    pub fn cancelState(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        input: CancelStateInput,
    ) Error!CancelState {
        try request.ensureActive();
        const store = self.store orelse return error.NotFound;
        const encoded = (store.loadJobAlloc(alloc, input.job_id) catch return error.Internal) orelse
            return error.NotFound;
        defer alloc.free(encoded);
        var parsed = std.json.parseFromSlice(
            repair_jobs.JobState,
            alloc,
            encoded,
            .{ .ignore_unknown_fields = true },
        ) catch return error.Internal;
        defer parsed.deinit();
        if (!std.mem.eql(u8, parsed.value.table_name, input.table_name)) return error.NotFound;
        return .{ .cancel_requested = parsed.value.cancel_requested or
            repair_jobs.isTerminalPhase(parsed.value.phase) or
            parsed.value.attempt_id != input.attempt_id };
    }
};

test "repair cancellation state is callable without an HTTP request" {
    const alloc = std.testing.allocator;
    var store = repair_jobs.Store.init(alloc, .{});
    defer store.deinit();
    const encoded = try store.startJob(alloc, "documents", .{});
    defer alloc.free(encoded);
    var parsed = try std.json.parseFromSlice(repair_jobs.JobState, alloc, encoded, .{});
    defer parsed.deinit();

    const operations = Operations{ .store = &store };
    const active = try operations.cancelState(alloc, .{}, .{
        .table_name = "documents",
        .job_id = parsed.value.job_id,
        .attempt_id = parsed.value.attempt_id,
    });
    try std.testing.expect(!active.cancel_requested);

    const stale_attempt = try operations.cancelState(alloc, .{}, .{
        .table_name = "documents",
        .job_id = parsed.value.job_id,
        .attempt_id = parsed.value.attempt_id + 1,
    });
    try std.testing.expect(stale_attempt.cancel_requested);
    try std.testing.expectError(error.NotFound, operations.cancelState(alloc, .{}, .{
        .table_name = "another-table",
        .job_id = parsed.value.job_id,
        .attempt_id = parsed.value.attempt_id,
    }));
}
