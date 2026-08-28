// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Transport-neutral operations for internal distributed-join coordination.

const std = @import("std");
const distributed_join = @import("distributed_join.zig");
const operation = @import("operation.zig");
const table_reads = @import("table_read_source.zig");

pub const Error = operation.ApiError || error{
    InvalidQueryRequest,
    UnsupportedQueryRequest,
    Timeout,
    TopologyChanged,
    DocIdentityNamespaceMismatch,
};

pub const JobState = struct {
    parsed: std.json.Parsed(distributed_join.EncodedJoinJobState),

    pub fn deinit(self: *JobState) void {
        self.parsed.deinit();
        self.* = undefined;
    }
};

pub const Operations = struct {
    job_store: *distributed_join.JoinJobStore,
    join_context: ?distributed_join.JoinContext = null,
    reads: ?table_reads.TableReadSource = null,

    fn mapExecutionError(err: anyerror) Error {
        return switch (err) {
            error.InvalidQueryRequest => error.InvalidQueryRequest,
            error.UnsupportedQueryRequest => error.UnsupportedQueryRequest,
            error.TableNotFound, error.UnknownGroup => error.NotFound,
            error.Timeout => error.Timeout,
            error.TopologyChanged => error.TopologyChanged,
            error.DocIdentityNamespaceMismatch => error.DocIdentityNamespaceMismatch,
            else => error.Internal,
        };
    }

    fn executionDependencies(self: Operations) Error!struct {
        context: distributed_join.JoinContext,
        reads: table_reads.TableReadSource,
    } {
        const reads = self.reads orelse return error.NotFound;
        const context = self.join_context orelse return error.Unavailable;
        return .{
            .context = context,
            .reads = reads,
        };
    }

    pub fn jobState(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        job_id: u64,
    ) Error!JobState {
        try request.ensureActive();
        if (self.join_context) |context| self.job_store.setContext(context);
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

    pub fn finalize(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: distributed_join.JoinFinalizeRequest,
    ) Error!distributed_join.JoinPartitionExecutionResult {
        try request.ensureActive();
        const deps = try self.executionDependencies();
        return distributed_join.executeJoinFinalizeWorkerLocalTyped(
            deps.context,
            self.job_store,
            alloc,
            deps.reads,
            group_id,
            table_name,
            input,
        ) catch |err| return mapExecutionError(err);
    }

    pub fn rows(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: distributed_join.JoinRowsRequest,
    ) Error![]std.json.Value {
        try request.ensureActive();
        const deps = try self.executionDependencies();
        return distributed_join.executeJoinRowsLocalTyped(
            deps.context,
            alloc,
            deps.reads,
            group_id,
            table_name,
            input,
        ) catch |err| return mapExecutionError(err);
    }

    pub fn unmatched(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: distributed_join.JoinUnmatchedRequest,
    ) Error!distributed_join.EncodedJoinUnmatchedResponse {
        try request.ensureActive();
        const deps = try self.executionDependencies();
        return distributed_join.executeJoinUnmatchedLocalTyped(
            deps.context,
            alloc,
            deps.reads,
            group_id,
            table_name,
            input,
        ) catch |err| return mapExecutionError(err);
    }

    pub fn partition(
        self: Operations,
        alloc: std.mem.Allocator,
        request: operation.RequestContext,
        group_id: u64,
        table_name: []const u8,
        input: distributed_join.JoinPartitionRequest,
    ) Error!distributed_join.JoinPartitionExecutionResult {
        try request.ensureActive();
        const deps = try self.executionDependencies();
        return distributed_join.executeJoinPartitionWorkerLocalTyped(
            deps.context,
            self.job_store,
            alloc,
            deps.reads,
            group_id,
            table_name,
            input,
        ) catch |err| return mapExecutionError(err);
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

    var rows_request = try distributed_join.parseJoinRowsRequest(
        alloc,
        "{\"join\":{\"right_table\":\"documents\",\"on\":{\"left_field\":\"customer_id\",\"right_field\":\"_id\"}}}",
    );
    defer rows_request.deinit(alloc);
    try std.testing.expectError(error.NotFound, operations.rows(
        alloc,
        .{},
        7,
        "documents",
        rows_request,
    ));
}
