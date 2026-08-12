// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral operations for internal distributed-join coordination.

const std = @import("std");
const distributed_join = @import("distributed_join.zig");
const operation = @import("operation.zig");

pub const Error = operation.ApiError;

pub const JobState = struct {
    parsed: std.json.Parsed(distributed_join.EncodedJoinJobState),

    pub fn deinit(self: *JobState) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub const Operations = struct {
    job_store: *distributed_join.JoinJobStore,

    pub fn jobState(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        job_id: u64,
    ) Error!JobState {
        try request.ensureActive();
        const encoded = (self.job_store.loadJoinJobStateSnapshot(alloc, job_id) catch return error.Internal) orelse
            return error.NotFound;
        defer alloc.free(encoded);
        const parsed = std.json.parseFromSlice(
            distributed_join.EncodedJoinJobState,
            alloc,
            encoded,
            .{ .allocate = .alloc_always, .ignore_unknown_fields = true },
        ) catch return error.Internal;
        return .{ .parsed = parsed };
    }
};

test "internal join job state is callable without an HTTP request" {
    const alloc = std.testing.allocator;
    var store = distributed_join.JoinJobStore.init(alloc, .{});
    defer store.deinit();
    const operations = Operations{ .job_store = &store };
    try std.testing.expectError(error.NotFound, operations.jobState(alloc, .{}, 7));

    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, operations.jobState(alloc, .{
        .cancellation = operation.CancellationToken.fromAtomic(&canceled),
    }, 7));
}
