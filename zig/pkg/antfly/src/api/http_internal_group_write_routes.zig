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
const batch_api = @import("batch.zig");
const db_mod = @import("../storage/db/mod.zig");
const http_client = @import("http_client.zig");
const http_common = @import("../raft/transport/http_common.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const internal_batch_forwarding = @import("internal_batch_forwarding.zig");
const internal_group_operations = @import("internal_group_operations.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const metadata_mod = @import("../metadata/domain.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const raft_mod = @import("../raft/mod.zig");
const repair_jobs = @import("repair_jobs.zig");
const routes = @import("http_routes.zig");
const table_writes = @import("table_write_source.zig");
const platform_time = @import("antfly_platform").time;

pub const BatchValidator = struct {
    ptr: *anyopaque,
    validate: *const fn (ptr: *anyopaque, table_name: []const u8, writes: []const db_mod.types.BatchWrite) anyerror!void,

    fn run(self: BatchValidator, table_name: []const u8, writes: []const db_mod.types.BatchWrite) !void {
        return try self.validate(self.ptr, table_name, writes);
    }
};

pub const RoutedRaftBatchWriter = struct {
    ptr: *anyopaque,
    write: *const fn (
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
        forwarding: internal_batch_forwarding.Context,
        cancellation: ?*const http_common.RequestCancellation,
    ) anyerror!?void,

    fn run(
        self: RoutedRaftBatchWriter,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_mod.types.BatchRequest,
        forwarding: internal_batch_forwarding.Context,
        cancellation: ?*const http_common.RequestCancellation,
    ) !?void {
        return try self.write(self.ptr, alloc, group_id, table_name, req, forwarding, cancellation);
    }
};

pub const Context = struct {
    alloc: std.mem.Allocator,
    shard_ops: ?raft_mod.ShardOperationAdapter,
    shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    writes: ?table_writes.TableWriteSource,
    routed_raft_batch_writer: ?RoutedRaftBatchWriter = null,
    repair_job_store: ?*repair_jobs.Store = null,
    repair_cancel_executor: ?http_common.RequestExecutor = null,
    batch_validator: BatchValidator,
};

fn raftBatchOutcomeResponse(
    alloc: std.mem.Allocator,
    status: u16,
    body: []const u8,
    outcome: []const u8,
) !http_common.HttpResponse {
    return try http_route_helpers.textResponseWithHeaders(alloc, status, body, &.{.{
        .name = internal_batch_forwarding.outcome_header,
        .value = outcome,
    }});
}

const RepairJobCancelProbe = struct {
    alloc: std.mem.Allocator,
    store: *repair_jobs.Store,
    job_id: u64,
    attempt_id: u64,
    cached_requested: std.atomic.Value(bool) = .init(false),
    last_check_ns: std.atomic.Value(u64) = .init(0),

    const check_interval_ns: u64 = 100 * std.time.ns_per_ms;

    fn check(ptr: *anyopaque) bool {
        const self: *RepairJobCancelProbe = @ptrCast(@alignCast(ptr));
        if (self.cached_requested.load(.acquire)) return true;
        const now_ns = platform_time.monotonicNs();
        const last_ns = self.last_check_ns.load(.acquire);
        if (last_ns != 0 and now_ns -| last_ns < check_interval_ns) return false;
        self.last_check_ns.store(now_ns, .release);

        const encoded = self.store.loadJobAlloc(self.alloc, self.job_id) catch return false;
        defer if (encoded) |buf| self.alloc.free(buf);
        const body = encoded orelse {
            self.cached_requested.store(true, .release);
            return true;
        };
        var parsed = std.json.parseFromSlice(repair_jobs.JobState, self.alloc, body, .{ .ignore_unknown_fields = true }) catch return false;
        defer parsed.deinit();
        const state = parsed.value;
        const requested = state.cancel_requested or
            repair_jobs.isTerminalPhase(state.phase) or
            state.attempt_id != self.attempt_id;
        if (requested) self.cached_requested.store(true, .release);
        return requested;
    }
};

const RemoteRepairJobCancelProbe = struct {
    alloc: std.mem.Allocator,
    executor: http_common.RequestExecutor,
    base_uri: []const u8,
    table_name: []const u8,
    job_id: u64,
    attempt_id: u64,
    cached_requested: std.atomic.Value(bool) = .init(false),
    last_check_ns: std.atomic.Value(u64) = .init(0),

    const check_interval_ns: u64 = 100 * std.time.ns_per_ms;

    fn check(ptr: *anyopaque) bool {
        const self: *RemoteRepairJobCancelProbe = @ptrCast(@alignCast(ptr));
        if (self.cached_requested.load(.acquire)) return true;
        const now_ns = platform_time.monotonicNs();
        const last_ns = self.last_check_ns.load(.acquire);
        if (last_ns != 0 and now_ns -| last_ns < check_interval_ns) return false;
        self.last_check_ns.store(now_ns, .release);

        var client = http_client.ApiHttpClient.init(self.alloc, self.executor);
        const requested = client.fetchTableRepairCancelRequested(
            self.base_uri,
            self.table_name,
            self.job_id,
            self.attempt_id,
        ) catch return false;
        if (requested) self.cached_requested.store(true, .release);
        return requested;
    }
};

pub fn handle(ctx: Context, req: http_common.HttpRequest, path: []const u8) !?http_common.HttpResponse {
    if (req.method != .POST) return null;

    if (routes.Routes.matchGroupRoutedBatch(path)) |batch_route| {
        const forwarding = internal_batch_forwarding.parse(req) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid raft batch forwarding headers");
        };
        if (forwarding == null) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "missing raft batch forwarding headers");
        }
        var batch_req = batch_api.parseInternalBatchRequest(ctx.alloc, req.body) catch |err| switch (err) {
            error.InvalidBatchRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid batch request"),
            error.ValueTooLong => return try http_route_helpers.textResponse(ctx.alloc, 413, "value too large"),
            else => return err,
        };
        defer batch_req.deinit(ctx.alloc);
        ctx.batch_validator.run(batch_route.table_name, batch_req.req.writes) catch |err| switch (err) {
            error.InvalidBatchRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid batch request"),
            else => return err,
        };

        _ = ((if (ctx.routed_raft_batch_writer) |writer|
            writer.run(ctx.alloc, batch_route.group_id, batch_route.table_name, batch_req.req, forwarding.?, req.cancellation)
        else
            return try raftBatchOutcomeResponse(
                ctx.alloc,
                503,
                "routed raft batch unavailable",
                internal_batch_forwarding.outcome_not_proposed_v1,
            )) catch |err| switch (err) {
            error.InvalidBatchRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid batch request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.RaftBatchWriteOutcomeUnknown => {
                // A generic 5xx retry policy could duplicate a transform that
                // already committed. Conflict is deliberately non-retryable;
                // the body preserves the machine-readable outcome class.
                return try raftBatchOutcomeResponse(
                    ctx.alloc,
                    409,
                    "write outcome unknown",
                    internal_batch_forwarding.outcome_unknown_v1,
                );
            },
            error.LeaderUnavailable, error.GroupLeaderUnavailable, error.MetadataSnapshotUnavailable => {
                return try raftBatchOutcomeResponse(
                    ctx.alloc,
                    503,
                    "group leader unavailable",
                    internal_batch_forwarding.outcome_not_proposed_v1,
                );
            },
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const result = batch_req.result();
        const response: metadata_openapi.BatchResponse = .{
            .inserted = result.inserted,
            .deleted = result.deleted,
            .transformed = result.transformed,
        };
        return try http_route_helpers.jsonResponseWithStatus(ctx.alloc, 201, response);
    }
    if (routes.Routes.matchGroupTableArtifactRepair(path)) |repair_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(db_mod.types.ArtifactRepairListRequest, ctx.alloc, if (req.body.len > 0) req.body else "{}", .{}) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid artifact repair list request");
        };
        defer parsed.deinit();
        var result = (writes.listArtifactRepairIssuesGroupLocal(
            ctx.alloc,
            repair_route.group_id,
            repair_route.table_name,
            parsed.value,
        ) catch |err| switch (err) {
            error.InvalidArgument => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid artifact repair list request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer result.deinit(ctx.alloc);
        return try http_route_helpers.jsonResponse(ctx.alloc, result);
    }
    if (routes.Routes.matchGroupTableArtifactRepairRun(path)) |repair_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(db_mod.types.ArtifactRepairRunRequest, ctx.alloc, if (req.body.len > 0) req.body else "{}", .{}) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid artifact repair request");
        };
        defer parsed.deinit();
        if (parsed.value.repair_job_id != null or parsed.value.repair_attempt_id != null) {
            const job_id = parsed.value.repair_job_id orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid repair cancel token");
            const attempt_id = parsed.value.repair_attempt_id orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid repair cancel token");
            if (parsed.value.repair_cancel_base_uri) |base_uri| {
                const executor = ctx.repair_cancel_executor orelse return try http_route_helpers.textResponse(ctx.alloc, 503, "repair cancel unavailable");
                var probe = RemoteRepairJobCancelProbe{
                    .alloc = ctx.alloc,
                    .executor = executor,
                    .base_uri = base_uri,
                    .table_name = repair_route.table_name,
                    .job_id = job_id,
                    .attempt_id = attempt_id,
                };
                var result = (writes.repairArtifactIssuesGroupLocalControlled(
                    ctx.alloc,
                    repair_route.group_id,
                    repair_route.table_name,
                    parsed.value,
                    .{
                        .cancel_check = .{
                            .ptr = &probe,
                            .is_requested = RemoteRepairJobCancelProbe.check,
                        },
                    },
                ) catch |err| switch (err) {
                    error.Canceled => return try http_route_helpers.textResponse(ctx.alloc, 409, "repair cancelled"),
                    error.InvalidArgument => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid artifact repair request"),
                    error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
                    error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
                    error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
                    else => return err,
                }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
                defer result.deinit(ctx.alloc);
                return try http_route_helpers.jsonResponseWithStatus(ctx.alloc, 202, result);
            } else {
                const store = ctx.repair_job_store orelse return try http_route_helpers.textResponse(ctx.alloc, 503, "repair cancel unavailable");
                var probe = RepairJobCancelProbe{
                    .alloc = ctx.alloc,
                    .store = store,
                    .job_id = job_id,
                    .attempt_id = attempt_id,
                };
                var result = (writes.repairArtifactIssuesGroupLocalControlled(
                    ctx.alloc,
                    repair_route.group_id,
                    repair_route.table_name,
                    parsed.value,
                    .{
                        .cancel_check = .{
                            .ptr = &probe,
                            .is_requested = RepairJobCancelProbe.check,
                        },
                    },
                ) catch |err| switch (err) {
                    error.Canceled => return try http_route_helpers.textResponse(ctx.alloc, 409, "repair cancelled"),
                    error.InvalidArgument => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid artifact repair request"),
                    error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
                    error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
                    error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
                    else => return err,
                }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
                defer result.deinit(ctx.alloc);
                return try http_route_helpers.jsonResponseWithStatus(ctx.alloc, 202, result);
            }
        }
        var result = (writes.repairArtifactIssuesGroupLocalControlled(
            ctx.alloc,
            repair_route.group_id,
            repair_route.table_name,
            parsed.value,
            .{},
        ) catch |err| switch (err) {
            error.Canceled => return try http_route_helpers.textResponse(ctx.alloc, 409, "repair cancelled"),
            error.InvalidArgument => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid artifact repair request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer result.deinit(ctx.alloc);
        return try http_route_helpers.jsonResponseWithStatus(ctx.alloc, 202, result);
    }
    if (routes.Routes.matchGroupTableArtifactReprocess(path)) |artifact_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const Request = struct {
            from_key: []const u8 = "",
            to_key: []const u8 = "",
            limit: u32 = 100,
            shard_cursors: []const db_mod.types.DocumentArtifactReprocessShardResume = &.{},
        };
        const FailureResponse = struct {
            key: []const u8,
            error_code: []const u8,
        };
        const ShardCursorResponse = struct {
            group_id: ?u64,
            next_key: []const u8,
            scanned: usize,
            reprocessed: usize,
            skipped: usize,
            failed: usize,
            limit: u32,
        };
        const Response = struct {
            reprocess: []const u8,
            reprocess_status: []const u8,
            artifact_name: []const u8,
            scanned: usize,
            reprocessed: usize,
            skipped: usize,
            failed: usize,
            limit: u32,
            next_key: ?[]const u8,
            pending_shards: usize,
            failures: []const FailureResponse,
            shard_cursors: []const ShardCursorResponse,
        };
        const decoded_artifact_name = try http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.alloc, artifact_route.artifact_name);
        defer ctx.alloc.free(decoded_artifact_name);
        var parsed = std.json.parseFromSlice(Request, ctx.alloc, if (req.body.len > 0) req.body else "{}", .{}) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid document artifact reprocess request");
        };
        defer parsed.deinit();
        var result = (writes.reprocessDocumentArtifactRangeGroupLocal(
            ctx.alloc,
            artifact_route.group_id,
            artifact_route.table_name,
            decoded_artifact_name,
            .{
                .from_key = parsed.value.from_key,
                .to_key = parsed.value.to_key,
                .limit = parsed.value.limit,
                .shard_cursors = parsed.value.shard_cursors,
            },
        ) catch |err| switch (err) {
            error.InvalidBatchRequest, error.InvalidArgument => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid document artifact reprocess request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer result.deinit(ctx.alloc);
        const failures = try ctx.alloc.alloc(FailureResponse, result.failures.len);
        defer ctx.alloc.free(failures);
        for (result.failures, failures) |failure, *out| {
            out.* = .{ .key = failure.key, .error_code = failure.error_code };
        }
        const shard_cursors = try ctx.alloc.alloc(ShardCursorResponse, result.shard_cursors.len);
        defer ctx.alloc.free(shard_cursors);
        for (result.shard_cursors, shard_cursors) |cursor, *out| {
            out.* = .{
                .group_id = cursor.group_id,
                .next_key = cursor.next_key,
                .scanned = cursor.scanned,
                .reprocessed = cursor.reprocessed,
                .skipped = cursor.skipped,
                .failed = cursor.failed,
                .limit = cursor.limit,
            };
        }
        const pending_shards = if (result.shard_cursors.len > 0)
            result.shard_cursors.len
        else if (result.next_key != null)
            @as(usize, 1)
        else
            @as(usize, 0);
        return try http_route_helpers.jsonResponseWithStatus(ctx.alloc, 202, Response{
            .reprocess = "triggered",
            .reprocess_status = if (pending_shards == 0) "complete" else "in_progress",
            .artifact_name = decoded_artifact_name,
            .scanned = result.scanned,
            .reprocessed = result.reprocessed,
            .skipped = result.skipped,
            .failed = result.failed,
            .limit = result.limit,
            .next_key = result.next_key,
            .pending_shards = pending_shards,
            .failures = failures,
            .shard_cursors = shard_cursors,
        });
    }
    return null;
}

const EncodedTransitionAction = struct {
    kind: enum {
        prepare_split_source,
        start_split_source,
        bootstrap_split_destination,
        catch_up_split_destination,
        finalize_split_source,
        rollback_split,
        accept_merge_receiver,
        catch_up_merge_receiver,
        finalize_merge,
        rollback_merge,
    },
    transition_id: u64,
    attempt_epoch: u64 = 0,
    source_group_id: ?u64 = null,
    destination_group_id: ?u64 = null,
    donor_group_id: ?u64 = null,
    receiver_group_id: ?u64 = null,
    allow_doc_identity_reassignment: bool = false,
    split_key: ?[]const u8 = null,
    source_range_end: ?[]const u8 = null,
    table_contract: metadata_transition_state.TransitionTableContract = .{},
};

const test_transition_table_contract: metadata_transition_state.TransitionTableContract = .{
    .table_id = 7,
    .table_name = "docs",
    .schema_json = "",
    .indexes_json = "{}",
    .source_identity = .{ .shard_id = 7, .range_id = 7 },
    .target_identity = .{ .shard_id = 7, .range_id = 7 },
};

fn requiredTransitionGroupId(value: ?u64) !u64 {
    const group_id = value orelse return error.InvalidTransitionActionRequest;
    if (group_id == 0) return error.InvalidTransitionActionRequest;
    return group_id;
}

pub fn parseSplitTransitionRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_transition_state.SplitTransitionRecord {
    var parsed = try std.json.parseFromSlice(metadata_transition_state.SplitTransitionRecord, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.transition_id == 0 or parsed.value.attempt_epoch == 0 or
        parsed.value.source_group_id == 0 or parsed.value.destination_group_id == 0)
    {
        return error.InvalidTransitionActionRequest;
    }
    try parsed.value.table_contract.validateForSplit();
    const split_key = if (parsed.value.split_key) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (split_key) |value| alloc.free(value);
    const source_range_end = if (parsed.value.source_range_end) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (source_range_end) |value| alloc.free(value);
    const rollback_reason = if (parsed.value.rollback_reason) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (rollback_reason) |value| alloc.free(value);
    const table_contract = try parsed.value.table_contract.clone(alloc);
    return .{
        .transition_id = parsed.value.transition_id,
        .attempt_epoch = parsed.value.attempt_epoch,
        .source_group_id = parsed.value.source_group_id,
        .destination_group_id = parsed.value.destination_group_id,
        .phase = parsed.value.phase,
        .split_key = split_key,
        .source_range_end = source_range_end,
        .rollback_reason = rollback_reason,
        .table_contract = table_contract,
    };
}

pub fn parseMergeTransitionRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_transition_state.MergeTransitionRecord {
    var parsed = try std.json.parseFromSlice(metadata_transition_state.MergeTransitionRecord, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    if (parsed.value.transition_id == 0 or parsed.value.donor_group_id == 0 or
        parsed.value.receiver_group_id == 0)
    {
        return error.InvalidTransitionActionRequest;
    }
    try parsed.value.table_contract.validateForMerge(
        parsed.value.allow_doc_identity_reassignment,
    );
    const rollback_reason = if (parsed.value.rollback_reason) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (rollback_reason) |value| alloc.free(value);
    const table_contract = try parsed.value.table_contract.clone(alloc);
    return .{
        .transition_id = parsed.value.transition_id,
        .donor_group_id = parsed.value.donor_group_id,
        .receiver_group_id = parsed.value.receiver_group_id,
        .phase = parsed.value.phase,
        .rollback_reason = rollback_reason,
        .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
        .table_contract = table_contract,
    };
}

pub fn parseTransitionAction(alloc: std.mem.Allocator, body: []const u8) !metadata_mod.TransitionAction {
    var parsed = try std.json.parseFromSlice(EncodedTransitionAction, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    switch (parsed.value.kind) {
        .prepare_split_source,
        .start_split_source,
        .bootstrap_split_destination,
        .catch_up_split_destination,
        .finalize_split_source,
        .rollback_split,
        => if (parsed.value.attempt_epoch == 0) return error.InvalidTransitionActionRequest,
        else => {},
    }
    if (parsed.value.transition_id == 0)
        return error.InvalidTransitionActionRequest;
    switch (parsed.value.kind) {
        .prepare_split_source,
        .start_split_source,
        .bootstrap_split_destination,
        .catch_up_split_destination,
        .finalize_split_source,
        .rollback_split,
        => try parsed.value.table_contract.validateForSplit(),
        .accept_merge_receiver,
        .catch_up_merge_receiver,
        .finalize_merge,
        .rollback_merge,
        => try parsed.value.table_contract.validateForMerge(
            parsed.value.allow_doc_identity_reassignment,
        ),
    }
    return switch (parsed.value.kind) {
        .prepare_split_source => try parsePrepareSplitTransitionAction(
            alloc,
            parsed.value,
        ),
        .start_split_source => .{
            .start_split_source = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .bootstrap_split_destination => .{
            .bootstrap_split_destination = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .catch_up_split_destination => .{
            .catch_up_split_destination = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .finalize_split_source => .{
            .finalize_split_source = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .rollback_split => .{
            .rollback_split = .{
                .transition_id = parsed.value.transition_id,
                .attempt_epoch = parsed.value.attempt_epoch,
                .source_group_id = try requiredTransitionGroupId(parsed.value.source_group_id),
                .destination_group_id = try requiredTransitionGroupId(parsed.value.destination_group_id),
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .accept_merge_receiver => .{
            .accept_merge_receiver = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = try requiredTransitionGroupId(parsed.value.donor_group_id),
                .receiver_group_id = try requiredTransitionGroupId(parsed.value.receiver_group_id),
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .catch_up_merge_receiver => .{
            .catch_up_merge_receiver = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = try requiredTransitionGroupId(parsed.value.donor_group_id),
                .receiver_group_id = try requiredTransitionGroupId(parsed.value.receiver_group_id),
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .finalize_merge => .{
            .finalize_merge = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = try requiredTransitionGroupId(parsed.value.donor_group_id),
                .receiver_group_id = try requiredTransitionGroupId(parsed.value.receiver_group_id),
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
        .rollback_merge => .{
            .rollback_merge = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = try requiredTransitionGroupId(parsed.value.donor_group_id),
                .receiver_group_id = try requiredTransitionGroupId(parsed.value.receiver_group_id),
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
                .table_contract = try parsed.value.table_contract.clone(alloc),
            },
        },
    };
}

fn parsePrepareSplitTransitionAction(
    alloc: std.mem.Allocator,
    encoded: EncodedTransitionAction,
) !metadata_mod.TransitionAction {
    const source_group_id = try requiredTransitionGroupId(encoded.source_group_id);
    const destination_group_id = try requiredTransitionGroupId(
        encoded.destination_group_id,
    );
    const raw_split_key = encoded.split_key orelse
        return error.InvalidTransitionActionRequest;
    if (raw_split_key.len == 0) return error.InvalidTransitionActionRequest;
    const split_key = try alloc.dupe(u8, raw_split_key);
    errdefer alloc.free(split_key);
    const source_range_end = if (encoded.source_range_end) |value|
        try alloc.dupe(u8, value)
    else
        null;
    errdefer if (source_range_end) |value| alloc.free(value);
    const table_contract = try encoded.table_contract.clone(alloc);
    return .{
        .prepare_split_source = .{
            .transition_id = encoded.transition_id,
            .attempt_epoch = encoded.attempt_epoch,
            .source_group_id = source_group_id,
            .destination_group_id = destination_group_id,
            .split_key = split_key,
            .source_range_end = source_range_end,
            .table_contract = table_contract,
        },
    };
}

pub fn freeSplitTransitionRecordOwned(alloc: std.mem.Allocator, record: *metadata_transition_state.SplitTransitionRecord) void {
    if (record.split_key) |value| alloc.free(value);
    if (record.source_range_end) |value| alloc.free(value);
    if (record.rollback_reason) |value| alloc.free(value);
    record.table_contract.deinitOwned(alloc);
    record.* = undefined;
}

pub fn freeMergeTransitionRecordOwned(alloc: std.mem.Allocator, record: *metadata_transition_state.MergeTransitionRecord) void {
    if (record.rollback_reason) |value| alloc.free(value);
    record.table_contract.deinitOwned(alloc);
    record.* = undefined;
}

test "legacy internal write dispatcher no longer handles unrouted batch requests" {
    const alloc = std.testing.allocator;

    const resp = try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/batch",
        .body = "{\"inserts\":[]}",
    }, "/internal/v1/groups/7/tables/docs/batch");
    try std.testing.expect(resp == null);

    var input = try batch_api.parseInternalBatchRequest(alloc, "{}");
    defer input.deinit(alloc);
    const operations = internal_group_operations.Operations{
        .reads = null,
        .shard_db_adapter = null,
        .writes = TestWriteSource.source(),
        .batch_validator = .{ .ptr = undefined, .validate_fn = TestWriteSource.validateBatch },
    };
    try std.testing.expectError(error.NotFound, operations.batch(
        alloc,
        .{},
        7,
        "docs",
        input.req,
    ));
}

test "internal group write route dispatches bounded raft forwarding context" {
    const Capture = struct {
        forwarding: ?internal_batch_forwarding.Context = null,
        failure: ?anyerror = null,

        fn write(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            _: db_mod.types.BatchRequest,
            forwarding: internal_batch_forwarding.Context,
            cancellation: ?*const http_common.RequestCancellation,
        ) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(cancellation != null);
            self.forwarding = forwarding;
            if (self.failure) |err| return err;
            return {};
        }
    };

    const headers = [_]http_common.RequestHeader{
        .{ .name = internal_batch_forwarding.remaining_ms_header, .value = "425" },
        .{ .name = internal_batch_forwarding.forwards_remaining_header, .value = "1" },
        .{ .name = internal_batch_forwarding.campaign_allowed_header, .value = "false" },
    };
    var capture = Capture{};
    var cancellation = http_common.RequestCancellation{};
    var resp = (try handle(.{
        .alloc = std.testing.allocator,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .routed_raft_batch_writer = .{ .ptr = &capture, .write = Capture.write },
        .batch_validator = TestWriteSource.batchValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/batch-routed-v1",
        .headers = &headers,
        .body = "{}",
        .cancellation = &cancellation,
    }, "/internal/v1/groups/7/tables/docs/batch-routed-v1")).?;
    defer resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expectEqual(@as(u32, 425), capture.forwarding.?.remaining_ms);
    try std.testing.expectEqual(@as(u8, 1), capture.forwarding.?.forwards_remaining);
    try std.testing.expect(!capture.forwarding.?.campaign_allowed);

    capture.forwarding = null;
    var missing_headers_resp = (try handle(.{
        .alloc = std.testing.allocator,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .routed_raft_batch_writer = .{ .ptr = &capture, .write = Capture.write },
        .batch_validator = TestWriteSource.batchValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/batch-routed-v1",
        .body = "{}",
    }, "/internal/v1/groups/7/tables/docs/batch-routed-v1")).?;
    defer missing_headers_resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 400), missing_headers_resp.status);
    try std.testing.expect(capture.forwarding == null);

    capture.failure = error.RaftBatchWriteOutcomeUnknown;
    var unknown_resp = (try handle(.{
        .alloc = std.testing.allocator,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .routed_raft_batch_writer = .{ .ptr = &capture, .write = Capture.write },
        .batch_validator = TestWriteSource.batchValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/batch-routed-v1",
        .headers = &headers,
        .body = "{}",
        .cancellation = &cancellation,
    }, "/internal/v1/groups/7/tables/docs/batch-routed-v1")).?;
    defer unknown_resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 409), unknown_resp.status);
    try std.testing.expectEqualStrings("write outcome unknown", unknown_resp.body);
    try std.testing.expectEqualStrings(
        internal_batch_forwarding.outcome_unknown_v1,
        unknown_resp.header(internal_batch_forwarding.outcome_header).?,
    );

    capture.failure = error.LeaderUnavailable;
    var unavailable_resp = (try handle(.{
        .alloc = std.testing.allocator,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .routed_raft_batch_writer = .{ .ptr = &capture, .write = Capture.write },
        .batch_validator = TestWriteSource.batchValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/batch-routed-v1",
        .headers = &headers,
        .body = "{}",
        .cancellation = &cancellation,
    }, "/internal/v1/groups/7/tables/docs/batch-routed-v1")).?;
    defer unavailable_resp.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(u16, 503), unavailable_resp.status);
    try std.testing.expectEqualStrings(
        internal_batch_forwarding.outcome_not_proposed_v1,
        unavailable_resp.header(internal_batch_forwarding.outcome_header).?,
    );
}

test "legacy internal group write dispatcher does not handle transactions" {
    const alloc = std.testing.allocator;

    try std.testing.expect((try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/txn-status",
        .body = "{}",
    }, "/internal/v1/groups/7/tables/docs/txn-status")) == null);

    const operations = internal_group_operations.Operations{
        .reads = null,
        .shard_db_adapter = null,
        .writes = TestWriteSource.source(),
    };
    try std.testing.expectEqual(
        db_mod.types.TxnStatus.pending,
        try operations.txnStatus(alloc, .{}, 7, "docs", .{0} ** 16),
    );
}

test "typed internal group operations update document artifact child range placement" {
    const alloc = std.testing.allocator;
    const operations = internal_group_operations.Operations{
        .reads = null,
        .shard_db_adapter = null,
        .writes = TestWriteSource.source(),
    };
    try operations.updateDocumentArtifactChildRangePlacement(alloc, .{}, 7, "docs", "doc/a", "document_units_v1", .{
        .range_id = "range:000000",
        .placement = "remote",
        .owner_group_id = 7002,
        .placement_generation = 3,
        .route_status = "remote_committed",
        .split_eligible = true,
    });
}

test "typed internal group operations apply document artifact child range batch" {
    const alloc = std.testing.allocator;
    const artifact_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc/a", "document_units_v1", "page:000001");
    defer alloc.free(artifact_key);
    const writes = [_]db_mod.types.BatchWrite{.{
        .key = artifact_key,
        .value = "{\"_parent_doc_key\":\"doc/a\",\"_artifact_name\":\"document_units_v1\",\"unit_id\":\"page:000001\",\"text\":\"alpha\"}",
    }};
    const operations = internal_group_operations.Operations{
        .reads = null,
        .shard_db_adapter = null,
        .writes = TestWriteSource.source(),
    };
    try std.testing.expectEqual(@as(u64, 44), try operations.applyDocumentArtifactChildRangeBatch(
        alloc,
        .{},
        7,
        "docs",
        "doc/a",
        "document_units_v1",
        .{ .artifact_writes = writes[0..], .sync_level = .write },
    ));
}

pub fn expectRejectsCallbackTokenWithoutCancelExecutorForTest() !void {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/repair/run",
        .body = "{\"target\":\"index\",\"index_name\":\"semantic\",\"repair_job_id\":42,\"repair_attempt_id\":3,\"repair_cancel_base_uri\":\"http://node-a\"}",
    }, "/internal/v1/groups/7/tables/docs/repair/run")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 503), resp.status);
    try std.testing.expectEqualStrings("repair cancel unavailable", resp.body);
}

test "internal group artifact repair rejects callback token without cancel executor" {
    try expectRejectsCallbackTokenWithoutCancelExecutorForTest();
}

test "typed internal group operations reject mismatched shard execute requests" {
    const alloc = std.testing.allocator;
    const body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(
        EncodedTransitionAction{
            .kind = .prepare_split_source,
            .transition_id = 1,
            .attempt_epoch = 1,
            .source_group_id = 8,
            .destination_group_id = 9,
            .split_key = "doc:m",
            .table_contract = test_transition_table_contract,
        },
        .{},
    )});
    defer alloc.free(body);

    var action = try parseTransitionAction(alloc, body);
    defer freeTransitionActionOwned(alloc, &action);
    const operations = internal_group_operations.Operations{
        .reads = null,
        .shard_db_adapter = null,
        .shard_ops = TestShardOps.adapter(),
    };
    try std.testing.expectError(error.InvalidArgument, operations.executeTransition(.{}, 7, action));
}

test "internal group write routes allow source-hosted split destination actions" {
    const action = metadata_mod.TransitionAction{ .bootstrap_split_destination = .{
        .transition_id = 1,
        .attempt_epoch = 1,
        .source_group_id = 7,
        .destination_group_id = 8,
        .table_contract = test_transition_table_contract,
    } };
    try std.testing.expect(transitionActionMatchesRouteGroup(action, 7));
    try std.testing.expect(transitionActionMatchesRouteGroup(action, 8));
    try std.testing.expect(!transitionActionMatchesRouteGroup(action, 9));
}

test "internal group write routes parse merge doc identity reassignment action flag" {
    const alloc = std.testing.allocator;
    const body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(
        EncodedTransitionAction{
            .kind = .catch_up_merge_receiver,
            .transition_id = 4,
            .donor_group_id = 10,
            .receiver_group_id = 9,
            .allow_doc_identity_reassignment = true,
            .table_contract = test_transition_table_contract,
        },
        .{},
    )});
    defer alloc.free(body);
    var action = try parseTransitionAction(alloc, body);
    defer freeTransitionActionOwned(alloc, &action);

    try std.testing.expect(action == .catch_up_merge_receiver);
    try std.testing.expect(action.catch_up_merge_receiver.allow_doc_identity_reassignment);
    try std.testing.expect(action.catch_up_merge_receiver.table_contract.eql(
        test_transition_table_contract,
    ));
}

test "internal group write routes reject incomplete transition contracts" {
    const alloc = std.testing.allocator;
    try std.testing.expectError(
        error.InvalidTransitionTableContract,
        parseTransitionAction(alloc,
            \\{"kind":"catch_up_merge_receiver","transition_id":4,"donor_group_id":10,"receiver_group_id":9}
        ),
    );
    try std.testing.expectError(
        error.InvalidTransitionActionRequest,
        parseTransitionAction(alloc,
            \\{"kind":"prepare_split_source","transition_id":4,"attempt_epoch":1,"source_group_id":10,"destination_group_id":9,"split_key":"","table_contract":{"table_id":7,"table_name":"docs","schema_json":"","indexes_json":"{}","source_identity":{"shard_id":7,"range_id":7},"target_identity":{"shard_id":7,"range_id":7}}}
        ),
    );
    try std.testing.expectError(
        error.InvalidTransitionTableContract,
        parseTransitionAction(alloc,
            \\{"kind":"catch_up_merge_receiver","transition_id":4,"donor_group_id":10,"receiver_group_id":9,"table_contract":{"table_id":7,"table_name":"docs","schema_json":"","indexes_json":"{}","source_identity":{"shard_id":7,"range_id":7},"target_identity":{"shard_id":8,"range_id":8}}}
        ),
    );
    try std.testing.expectError(
        error.InvalidTransitionTableContract,
        parseTransitionAction(alloc,
            \\{"kind":"prepare_split_source","transition_id":4,"attempt_epoch":1,"source_group_id":10,"destination_group_id":9,"split_key":"doc:m","table_contract":{"table_id":7,"table_name":"docs","schema_json":"","indexes_json":"{}","source_identity":{"shard_id":7,"range_id":7},"target_identity":{"shard_id":8,"range_id":8}}}
        ),
    );
}

test "typed internal group operations preserve shard doc identity conflicts" {
    const alloc = std.testing.allocator;
    const ConflictShardOps = struct {
        fn adapter() raft_mod.ShardOperationAdapter {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .observe_split = observeSplit,
                    .observe_merge = observeMerge,
                    .prepare_split_source = prepareSplitSource,
                    .start_split_source = startSplitSource,
                    .bootstrap_split_destination = bootstrapSplitDestination,
                    .catch_up_split_destination = catchUpSplitDestination,
                    .finalize_split_source = finalizeSplitSource,
                    .rollback_split = rollbackSplit,
                    .accept_merge_receiver = acceptMergeReceiver,
                    .catch_up_merge_receiver = catchUpMergeReceiver,
                    .finalize_merge = finalizeMerge,
                    .rollback_merge = rollbackMerge,
                },
            };
        }

        fn observeSplit(_: *anyopaque, _: u64, _: metadata_transition_state.SplitTransitionRecord) !metadata_transition_state.SplitObservation {
            return error.DocIdentityNamespaceMismatch;
        }

        fn observeMerge(_: *anyopaque, _: u64, _: metadata_transition_state.MergeTransitionRecord) !metadata_transition_state.MergeObservation {
            return error.DocIdentityNamespaceMismatch;
        }

        fn prepareSplitSource(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .prepare_split_source).type) !void {
            unreachable;
        }

        fn startSplitSource(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .start_split_source).type) !void {
            unreachable;
        }

        fn bootstrapSplitDestination(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .bootstrap_split_destination).type) !void {
            unreachable;
        }

        fn catchUpSplitDestination(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .catch_up_split_destination).type) !void {
            unreachable;
        }

        fn finalizeSplitSource(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .finalize_split_source).type) !void {
            unreachable;
        }

        fn rollbackSplit(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .rollback_split).type) !void {
            unreachable;
        }

        fn acceptMergeReceiver(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .accept_merge_receiver).type) !void {
            unreachable;
        }

        fn catchUpMergeReceiver(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .catch_up_merge_receiver).type) !void {
            unreachable;
        }

        fn finalizeMerge(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .finalize_merge).type) !void {
            return error.DocIdentityNamespaceMismatch;
        }

        fn rollbackMerge(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .rollback_merge).type) !void {
            unreachable;
        }
    };

    const operations = internal_group_operations.Operations{
        .reads = null,
        .shard_db_adapter = null,
        .shard_ops = ConflictShardOps.adapter(),
    };
    var split = try parseSplitTransitionRecord(alloc, "{\"transition_id\":1,\"attempt_epoch\":1,\"source_group_id\":7,\"destination_group_id\":8,\"split_key\":\"doc:m\",\"table_contract\":{\"table_id\":7,\"table_name\":\"docs\",\"schema_json\":\"\",\"indexes_json\":\"{}\",\"source_identity\":{\"shard_id\":7,\"range_id\":7},\"target_identity\":{\"shard_id\":7,\"range_id\":7}}}");
    defer freeSplitTransitionRecordOwned(alloc, &split);
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, operations.observeSplit(.{}, 7, split));

    var merge = try parseMergeTransitionRecord(alloc, "{\"transition_id\":2,\"donor_group_id\":8,\"receiver_group_id\":7,\"allow_doc_identity_reassignment\":true,\"table_contract\":{\"table_id\":7,\"table_name\":\"docs\",\"schema_json\":\"\",\"indexes_json\":\"{}\",\"source_identity\":{\"shard_id\":7,\"range_id\":7},\"target_identity\":{\"shard_id\":7,\"range_id\":7}}}");
    defer freeMergeTransitionRecordOwned(alloc, &merge);
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, operations.observeMerge(.{}, 7, merge));

    var action = try parseTransitionAction(alloc, "{\"kind\":\"finalize_merge\",\"transition_id\":3,\"donor_group_id\":8,\"receiver_group_id\":7,\"allow_doc_identity_reassignment\":true,\"table_contract\":{\"table_id\":7,\"table_name\":\"docs\",\"schema_json\":\"\",\"indexes_json\":\"{}\",\"source_identity\":{\"shard_id\":7,\"range_id\":7},\"target_identity\":{\"shard_id\":7,\"range_id\":7}}}");
    defer freeTransitionActionOwned(alloc, &action);
    try std.testing.expectError(error.DocIdentityNamespaceMismatch, operations.executeTransition(.{}, 7, action));
}

const TestWriteSource = struct {
    fn source() table_writes.TableWriteSource {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .batch = batch,
                .batch_group_local = batchGroupLocal,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_status_group_local = txnStatusGroupLocal,
                .update_document_artifact_child_range_placement_group_local = updateDocumentArtifactChildRangePlacementGroupLocal,
                .apply_document_artifact_child_range_batch_group_local = applyDocumentArtifactChildRangeBatchGroupLocal,
            },
        };
    }

    fn batchValidator() BatchValidator {
        return .{
            .ptr = undefined,
            .validate = validateBatch,
        };
    }

    fn validateBatch(_: *anyopaque, _: []const u8, _: []const db_mod.types.BatchWrite) !void {}

    fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
        return null;
    }

    fn batchGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.BatchRequest) !?void {
        return null;
    }

    fn txnBeginGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: bool, _: []const []const u8) !?void {
        return null;
    }

    fn txnPrepareGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) !?void {
        return null;
    }

    fn txnResolveGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64, _: u64, _: db_mod.types.SyncLevel) !?void {
        return null;
    }

    fn txnStatusGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !?db_mod.types.TxnStatus {
        return .pending;
    }

    fn updateDocumentArtifactChildRangePlacementGroupLocal(
        _: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_mod.types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        if (group_id != 7) return null;
        if (!std.mem.eql(u8, table_name, "docs")) return null;
        if (!std.mem.eql(u8, doc_key, "doc/a")) return false;
        if (!std.mem.eql(u8, artifact_name, "document_units_v1")) return false;
        if (!std.mem.eql(u8, update.range_id, "range:000000")) return false;
        if (!std.mem.eql(u8, update.placement, "remote")) return false;
        if (update.owner_group_id != 7002) return false;
        if (update.placement_generation != 3) return false;
        if (update.route_status == null or !std.mem.eql(u8, update.route_status.?, "remote_committed")) return false;
        if (update.split_eligible != true) return false;
        return true;
    }

    fn applyDocumentArtifactChildRangeBatchGroupLocal(
        _: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
    ) !?u64 {
        if (group_id != 7) return null;
        if (!std.mem.eql(u8, table_name, "docs")) return null;
        if (!std.mem.eql(u8, doc_key, "doc/a")) return null;
        if (!std.mem.eql(u8, artifact_name, "document_units_v1")) return null;
        if (child_batch.artifact_writes.len != 1) return error.InvalidBatchRequest;
        if (child_batch.artifact_delete_keys.len != 0) return error.InvalidBatchRequest;
        if (child_batch.sync_level != .write) return error.InvalidBatchRequest;
        return 44;
    }
};

const TestShardOps = struct {
    fn adapter() raft_mod.ShardOperationAdapter {
        return .{
            .ptr = undefined,
            .vtable = &.{
                .observe_split = observeSplit,
                .observe_merge = observeMerge,
                .prepare_split_source = prepareSplitSource,
                .start_split_source = startSplitSource,
                .bootstrap_split_destination = bootstrapSplitDestination,
                .catch_up_split_destination = catchUpSplitDestination,
                .finalize_split_source = finalizeSplitSource,
                .rollback_split = rollbackSplit,
                .accept_merge_receiver = acceptMergeReceiver,
                .catch_up_merge_receiver = catchUpMergeReceiver,
                .finalize_merge = finalizeMerge,
                .rollback_merge = rollbackMerge,
            },
        };
    }

    fn observeSplit(_: *anyopaque, _: u64, _: metadata_transition_state.SplitTransitionRecord) !metadata_transition_state.SplitObservation {
        unreachable;
    }

    fn observeMerge(_: *anyopaque, _: u64, _: metadata_transition_state.MergeTransitionRecord) !metadata_transition_state.MergeObservation {
        unreachable;
    }

    fn prepareSplitSource(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .prepare_split_source).type) !void {
        unreachable;
    }

    fn startSplitSource(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .start_split_source).type) !void {
        unreachable;
    }

    fn bootstrapSplitDestination(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .bootstrap_split_destination).type) !void {
        unreachable;
    }

    fn catchUpSplitDestination(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .catch_up_split_destination).type) !void {
        unreachable;
    }

    fn finalizeSplitSource(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .finalize_split_source).type) !void {
        unreachable;
    }

    fn rollbackSplit(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .rollback_split).type) !void {
        unreachable;
    }

    fn acceptMergeReceiver(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .accept_merge_receiver).type) !void {
        unreachable;
    }

    fn catchUpMergeReceiver(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .catch_up_merge_receiver).type) !void {
        unreachable;
    }

    fn finalizeMerge(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .finalize_merge).type) !void {
        unreachable;
    }

    fn rollbackMerge(_: *anyopaque, _: u64, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .rollback_merge).type) !void {
        unreachable;
    }
};

pub fn freeTransitionActionOwned(alloc: std.mem.Allocator, action: *metadata_mod.TransitionAction) void {
    const table_contract: ?metadata_transition_state.TransitionTableContract = switch (action.*) {
        .none => null,
        inline else => |op| op.table_contract,
    };
    switch (action.*) {
        .prepare_split_source => |op| {
            alloc.free(op.split_key);
            if (op.source_range_end) |value| alloc.free(value);
        },
        else => {},
    }
    if (table_contract) |contract| {
        var owned = contract;
        owned.deinitOwned(alloc);
    }
    action.* = undefined;
}

fn transitionActionMatchesRouteGroup(action: metadata_mod.TransitionAction, group_id: u64) bool {
    return switch (action) {
        .none => group_id == 0,
        .prepare_split_source => |op| group_id == op.source_group_id,
        .start_split_source => |op| group_id == op.source_group_id,
        // During local split handoff the source node owns the destination DB
        // bootstrap until the new range is committed, so internal routing may
        // address these destination actions through either split group.
        .bootstrap_split_destination => |op| group_id == op.source_group_id or group_id == op.destination_group_id,
        .catch_up_split_destination => |op| group_id == op.source_group_id or group_id == op.destination_group_id,
        .finalize_split_source => |op| group_id == op.source_group_id,
        .rollback_split => |op| group_id == op.source_group_id,
        .accept_merge_receiver => |op| group_id == op.receiver_group_id,
        .catch_up_merge_receiver => |op| group_id == op.receiver_group_id,
        .finalize_merge => |op| group_id == op.receiver_group_id,
        .rollback_merge => |op| group_id == op.receiver_group_id,
    };
}
