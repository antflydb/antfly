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
const distributed_txn = @import("distributed_txn.zig");
const http_client = @import("http_client.zig");
const http_common = @import("../raft/transport/http_common.zig");
const http_route_helpers = @import("http_route_helpers.zig");
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

pub const TxnValidator = struct {
    ptr: *anyopaque,
    validate: *const fn (ptr: *anyopaque, table_name: []const u8, writes: []const db_mod.types.TransactionWrite) anyerror!void,

    fn run(self: TxnValidator, table_name: []const u8, writes: []const db_mod.types.TransactionWrite) !void {
        return try self.validate(self.ptr, table_name, writes);
    }
};

pub const Context = struct {
    alloc: std.mem.Allocator,
    shard_ops: ?raft_mod.ShardOperationAdapter,
    shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    writes: ?table_writes.TableWriteSource,
    repair_job_store: ?*repair_jobs.Store = null,
    repair_cancel_executor: ?http_common.RequestExecutor = null,
    batch_validator: BatchValidator,
    txn_validator: TxnValidator,
};

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

const CorruptEmbeddingArtifactRequest = struct {
    doc_key: []const u8,
    index_name: []const u8,
};

const DocumentArtifactChildKeyPrefixes = struct {
    unit: []u8,
    chunk: []u8,

    fn deinit(self: *DocumentArtifactChildKeyPrefixes, alloc: std.mem.Allocator) void {
        alloc.free(self.unit);
        alloc.free(self.chunk);
        self.* = undefined;
    }
};

fn documentArtifactChildKeyPrefixesAlloc(
    alloc: std.mem.Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
) !DocumentArtifactChildKeyPrefixes {
    var unit = std.ArrayListUnmanaged(u8).empty;
    errdefer unit.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&unit, alloc, doc_key);
    try unit.append(alloc, internal_keys.artifact_kind);
    try internal_keys.appendEncodedComponent(&unit, alloc, "asset");
    try internal_keys.appendEncodedComponent(&unit, alloc, artifact_name);
    try unit.append(alloc, internal_keys.document_unit_record_kind);

    var chunk = std.ArrayListUnmanaged(u8).empty;
    errdefer chunk.deinit(alloc);
    try internal_keys.appendDocumentPrefix(&chunk, alloc, doc_key);
    try chunk.append(alloc, internal_keys.artifact_kind);
    try internal_keys.appendEncodedComponent(&chunk, alloc, "chunk");
    try internal_keys.appendEncodedComponent(&chunk, alloc, artifact_name);
    try chunk.append(alloc, internal_keys.document_unit_record_kind);

    const owned_unit = try unit.toOwnedSlice(alloc);
    errdefer alloc.free(owned_unit);
    const owned_chunk = try chunk.toOwnedSlice(alloc);
    return .{
        .unit = owned_unit,
        .chunk = owned_chunk,
    };
}

fn documentArtifactChildKeyMatches(prefixes: DocumentArtifactChildKeyPrefixes, key: []const u8) bool {
    return std.mem.startsWith(u8, key, prefixes.unit) or std.mem.startsWith(u8, key, prefixes.chunk);
}

fn validateDocumentArtifactChildRangeBatchScope(
    alloc: std.mem.Allocator,
    doc_key: []const u8,
    artifact_name: []const u8,
    child_batch: db_mod.DocumentArtifactChildRangeApplyBatch,
) !void {
    var prefixes = try documentArtifactChildKeyPrefixesAlloc(alloc, doc_key, artifact_name);
    defer prefixes.deinit(alloc);

    for (child_batch.artifact_writes) |write| {
        if (!documentArtifactChildKeyMatches(prefixes, write.key)) return error.InvalidBatchRequest;
    }
    for (child_batch.artifact_delete_keys) |key| {
        if (!documentArtifactChildKeyMatches(prefixes, key)) return error.InvalidBatchRequest;
    }
    for (child_batch.documents) |doc| {
        if (!documentArtifactChildKeyMatches(prefixes, doc.key)) return error.InvalidBatchRequest;
    }
    for (child_batch.dense_embeddings) |embedding| {
        if (embedding.artifact_key) |artifact_key| {
            if (!documentArtifactChildKeyMatches(prefixes, artifact_key)) return error.InvalidBatchRequest;
        }
    }
    for (child_batch.sparse_embeddings) |embedding| {
        if (embedding.artifact_key) |artifact_key| {
            if (!documentArtifactChildKeyMatches(prefixes, artifact_key)) return error.InvalidBatchRequest;
        }
    }
}

pub fn handle(ctx: Context, req: http_common.HttpRequest, path: []const u8) !?http_common.HttpResponse {
    if (req.method == .GET) {
        if (routes.Routes.matchGroupDbMedianKey(path)) |route| {
            const adapter = ctx.shard_db_adapter orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
            const median_key = adapter.fetchMedianKey(ctx.alloc, route.group_id) catch |err| switch (err) {
                error.UnknownGroup => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
                error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
                else => return err,
            };
            defer if (median_key) |value| ctx.alloc.free(value);
            return try http_route_helpers.jsonResponse(ctx.alloc, .{ .median_key = median_key });
        }
    }

    if (req.method != .POST) return null;

    if (routes.Routes.matchInternalTableCorruptEmbeddingArtifact(path)) |route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(CorruptEmbeddingArtifactRequest, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
        }) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid corrupt embedding artifact request");
        };
        defer parsed.deinit();
        _ = (writes.corruptEmbeddingArtifact(ctx.alloc, route.table_name, parsed.value.doc_key, parsed.value.index_name) catch |err| switch (err) {
            error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, struct {}{});
    }

    if (routes.Routes.matchGroupShardObserveSplit(path)) |route| {
        const ops = ctx.shard_ops orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var record = parseSplitTransitionRecord(ctx.alloc, req.body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid split transition request");
        };
        defer freeSplitTransitionRecordOwned(ctx.alloc, &record);
        if (route.group_id != record.source_group_id and route.group_id != record.destination_group_id) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "group does not match transition");
        }
        var observation = ops.observeSplit(record) catch |err| switch (err) {
            error.UnknownGroup, error.UnknownSplitRuntime, error.MissingSplitRuntime => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.LeaderUnavailable,
            error.GroupLeaderUnavailable,
            error.SplitSourceProjectionNotReady,
            error.DurableRootIncarnationUnavailable,
            error.AutoBulkIngestBusy,
            error.ApplyStoreGroupRetired,
            error.ApplyStoreShuttingDown,
            => return try http_route_helpers.textResponse(ctx.alloc, 503, "group leader unavailable"),
            else => return err,
        };
        if (route.group_id == record.source_group_id) observation.source_local_leader = true;
        if (route.group_id == record.destination_group_id) observation.destination_local_leader = true;
        return try http_route_helpers.jsonResponse(ctx.alloc, observation);
    }
    if (routes.Routes.matchGroupShardObserveMerge(path)) |route| {
        const ops = ctx.shard_ops orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var record = parseMergeTransitionRecord(ctx.alloc, req.body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid merge transition request");
        };
        defer freeMergeTransitionRecordOwned(ctx.alloc, &record);
        if (route.group_id != record.donor_group_id and route.group_id != record.receiver_group_id) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "group does not match transition");
        }
        var observation = ops.observeMerge(record) catch |err| switch (err) {
            error.UnknownGroup, error.UnknownMergeRuntime, error.MissingMergeRuntime => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.LeaderUnavailable, error.GroupLeaderUnavailable => return try http_route_helpers.textResponse(ctx.alloc, 503, "group leader unavailable"),
            else => return err,
        };
        if (route.group_id == record.donor_group_id) observation.donor_local_leader = true;
        if (route.group_id == record.receiver_group_id) observation.receiver_local_leader = true;
        return try http_route_helpers.jsonResponse(ctx.alloc, observation);
    }
    if (routes.Routes.matchGroupShardExecute(path)) |route| {
        const ops = ctx.shard_ops orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var action = parseTransitionAction(ctx.alloc, req.body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transition action request");
        };
        defer freeTransitionActionOwned(ctx.alloc, &action);
        if (!transitionActionMatchesRouteGroup(action, route.group_id)) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "group does not match transition action");
        }
        ops.execute(action) catch |err| switch (err) {
            error.UnknownGroup, error.UnknownSplitRuntime, error.UnknownMergeRuntime, error.MissingSplitRuntime, error.MissingMergeRuntime => {
                return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
            },
            error.TopologyChanged => return try http_route_helpers.textResponse(ctx.alloc, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.LeaderUnavailable,
            error.GroupLeaderUnavailable,
            error.MetadataSnapshotUnavailable,
            error.SplitSourceProjectionNotReady,
            error.SplitSourceProjectionAdvanced,
            error.DurableRootIncarnationUnavailable,
            error.AutoBulkIngestBusy,
            error.ApplyStoreGroupRetired,
            error.ApplyStoreShuttingDown,
            error.BackgroundOwnerClosing,
            error.BackgroundOwnerClosed,
            error.TransitionOperationBusy,
            => {
                return try http_route_helpers.textResponse(ctx.alloc, 503, "group leader unavailable");
            },
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            else => return err,
        };
        return try http_route_helpers.jsonResponse(ctx.alloc, struct {}{});
    }

    if (routes.Routes.matchGroupBatch(path)) |batch_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
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

        _ = (writes.batchGroupLocal(ctx.alloc, batch_route.group_id, batch_route.table_name, batch_req.req) catch |err| switch (err) {
            error.InvalidBatchRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid batch request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.LeaderUnavailable, error.GroupLeaderUnavailable, error.MetadataSnapshotUnavailable => {
                return try http_route_helpers.textResponse(ctx.alloc, 503, "group leader unavailable");
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
    if (routes.Routes.matchGroupDocumentArtifactPlacementUpdate(path)) |artifact_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.alloc, artifact_route.key);
        defer ctx.alloc.free(decoded_key);
        const decoded_artifact_name = try http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.alloc, artifact_route.artifact_name);
        defer ctx.alloc.free(decoded_artifact_name);
        var parsed = std.json.parseFromSlice(db_mod.types.DocumentArtifactChildRangePlacementUpdate, ctx.alloc, req.body, .{}) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid document artifact placement request");
        };
        defer parsed.deinit();
        const handled = (writes.updateDocumentArtifactChildRangePlacementGroupLocal(
            ctx.alloc,
            artifact_route.group_id,
            artifact_route.table_name,
            decoded_key,
            decoded_artifact_name,
            parsed.value,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest, error.InvalidArgument => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid document artifact placement request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        if (!handled) return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, .{ .placement = "updated" });
    }
    if (routes.Routes.matchGroupDocumentArtifactChildRangeBatch(path)) |artifact_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.alloc, artifact_route.key);
        defer ctx.alloc.free(decoded_key);
        const decoded_artifact_name = try http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.alloc, artifact_route.artifact_name);
        defer ctx.alloc.free(decoded_artifact_name);
        var parsed = std.json.parseFromSlice(db_mod.DocumentArtifactChildRangeApplyBatch, ctx.alloc, req.body, .{}) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid document artifact child range batch request");
        };
        defer parsed.deinit();
        validateDocumentArtifactChildRangeBatchScope(ctx.alloc, decoded_key, decoded_artifact_name, parsed.value) catch |err| switch (err) {
            error.InvalidBatchRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid document artifact child range batch request"),
            else => return err,
        };
        const sequence = (writes.applyDocumentArtifactChildRangeBatchGroupLocal(
            ctx.alloc,
            artifact_route.group_id,
            artifact_route.table_name,
            decoded_key,
            decoded_artifact_name,
            parsed.value,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest, error.InvalidArgument => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid document artifact child range batch request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, .{ .sequence = sequence });
    }
    if (routes.Routes.matchGroupDocumentArtifactReprocess(path)) |artifact_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const decoded_key = try http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.alloc, artifact_route.key);
        defer ctx.alloc.free(decoded_key);
        const decoded_artifact_name = try http_route_helpers.decodePercentEncodedPathComponentAlloc(ctx.alloc, artifact_route.artifact_name);
        defer ctx.alloc.free(decoded_artifact_name);
        const handled = (writes.reprocessDocumentArtifactGroupLocal(
            ctx.alloc,
            artifact_route.group_id,
            artifact_route.table_name,
            decoded_key,
            decoded_artifact_name,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest, error.InvalidArgument => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid document artifact reprocess request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound, error.NotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        if (!handled) return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, .{ .reprocess = "triggered" });
    }
    if (routes.Routes.matchGroupTxnBegin(path)) |txn_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var txn_req = distributed_txn.parseTxnBeginRequest(ctx.alloc, req.body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transaction request");
        };
        defer distributed_txn.freeTxnBeginRequest(ctx.alloc, &txn_req);
        _ = (writes.txnBeginGroupLocal(
            ctx.alloc,
            txn_route.group_id,
            txn_route.table_name,
            txn_req.txn_id,
            txn_req.begin_timestamp,
            txn_req.topology_epoch,
            txn_req.participants,
        ) catch |err| switch (err) {
            error.InvalidBatchRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transaction request"),
            error.TopologyChanged => return try http_route_helpers.textResponse(ctx.alloc, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TxnNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, struct {}{});
    }
    if (routes.Routes.matchGroupTxnPrepare(path)) |txn_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var txn_req = distributed_txn.parseTxnPrepareRequest(ctx.alloc, req.body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transaction request");
        };
        defer distributed_txn.freeTxnPrepareRequest(ctx.alloc, &txn_req);
        ctx.txn_validator.run(txn_route.table_name, txn_req.req.writes) catch |err| switch (err) {
            error.InvalidBatchRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transaction request"),
            else => return err,
        };
        _ = (writes.txnPrepareGroupLocal(
            ctx.alloc,
            txn_route.group_id,
            txn_route.table_name,
            txn_req.txn_id,
            txn_req.topology_epoch,
            txn_req.req,
        ) catch |err| switch (err) {
            error.TopologyChanged => return try http_route_helpers.textResponse(ctx.alloc, 409, "topology changed"),
            error.VersionConflict, error.IntentConflict => return try http_route_helpers.textResponse(ctx.alloc, 409, "transaction conflict"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TxnNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, struct {}{});
    }
    if (routes.Routes.matchGroupTxnResolve(path)) |txn_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const txn_req = distributed_txn.parseTxnResolveRequest(ctx.alloc, req.body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transaction request");
        };
        _ = (writes.txnResolveGroupLocal(
            ctx.alloc,
            txn_route.group_id,
            txn_route.table_name,
            txn_req.txn_id,
            txn_req.status,
            txn_req.commit_version,
        ) catch |err| switch (err) {
            error.DecisionConflict => return try http_route_helpers.textResponse(ctx.alloc, 409, "decision conflict"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TxnNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, struct {}{});
    }
    if (routes.Routes.matchGroupTxnStatus(path)) |txn_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const txn_id = distributed_txn.parseTxnStatusRequest(ctx.alloc, req.body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transaction request");
        };
        const status = (writes.txnStatusGroupLocal(
            ctx.alloc,
            txn_route.group_id,
            txn_route.table_name,
            txn_id,
        ) catch |err| switch (err) {
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TxnNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, distributed_txn.TxnStatusResponse{ .status = status });
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

fn parseSplitTransitionRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_transition_state.SplitTransitionRecord {
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

fn parseMergeTransitionRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_transition_state.MergeTransitionRecord {
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

fn parseTransitionAction(alloc: std.mem.Allocator, body: []const u8) !metadata_mod.TransitionAction {
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

fn freeSplitTransitionRecordOwned(alloc: std.mem.Allocator, record: *metadata_transition_state.SplitTransitionRecord) void {
    if (record.split_key) |value| alloc.free(value);
    if (record.source_range_end) |value| alloc.free(value);
    if (record.rollback_reason) |value| alloc.free(value);
    record.table_contract.deinitOwned(alloc);
    record.* = undefined;
}

fn freeMergeTransitionRecordOwned(alloc: std.mem.Allocator, record: *metadata_transition_state.MergeTransitionRecord) void {
    if (record.rollback_reason) |value| alloc.free(value);
    record.table_contract.deinitOwned(alloc);
    record.* = undefined;
}

test "internal group write routes validate batch requests" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/batch",
        .body = "{\"inserts\":[]}",
    }, "/internal/v1/groups/7/tables/docs/batch")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("invalid batch request", resp.body);
}

test "internal group write routes validate transaction status requests" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/txn-status",
        .body = "{}",
    }, "/internal/v1/groups/7/tables/docs/txn-status")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("invalid transaction request", resp.body);
}

test "internal group write routes update document artifact child range placement" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:placement",
        .body = "{\"range_id\":\"range:000000\",\"placement\":\"remote\",\"owner_group_id\":7002,\"placement_generation\":3,\"route_status\":\"remote_committed\",\"split_eligible\":true}",
    }, "/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:placement")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"placement\":\"updated\"") != null);
}

test "internal group write routes apply document artifact child range batch" {
    const alloc = std.testing.allocator;
    const artifact_key = try internal_keys.documentUnitArtifactKeyAlloc(alloc, "doc/a", "document_units_v1", "page:000001");
    defer alloc.free(artifact_key);
    const writes = [_]db_mod.types.BatchWrite{.{
        .key = artifact_key,
        .value = "{\"_parent_doc_key\":\"doc/a\",\"_artifact_name\":\"document_units_v1\",\"unit_id\":\"page:000001\",\"text\":\"alpha\"}",
    }};
    const body = try std.json.Stringify.valueAlloc(alloc, db_mod.DocumentArtifactChildRangeApplyBatch{
        .artifact_writes = writes[0..],
        .sync_level = .write,
    }, .{});
    defer alloc.free(body);

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:child-range-batch",
        .body = body,
    }, "/internal/v1/groups/7/tables/docs/documents/doc%2Fa/artifacts/document_units_v1:child-range-batch")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"sequence\":44") != null);
}

pub fn expectRejectsCallbackTokenWithoutCancelExecutorForTest() !void {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
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

test "internal group write routes reject mismatched shard execute requests" {
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

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = TestShardOps.adapter(),
        .writes = null,
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/shard-ops/execute",
        .body = body,
    }, "/internal/v1/groups/7/shard-ops/execute")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("group does not match transition action", resp.body);
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

test "internal group write routes map shard doc identity mismatch to conflict" {
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

    const ctx: Context = .{
        .alloc = alloc,
        .shard_ops = ConflictShardOps.adapter(),
        .writes = null,
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    };

    var split_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/shard-ops/observe-split",
        .body = "{\"transition_id\":1,\"attempt_epoch\":1,\"source_group_id\":7,\"destination_group_id\":8,\"split_key\":\"doc:m\",\"table_contract\":{\"table_id\":7,\"table_name\":\"docs\",\"schema_json\":\"\",\"indexes_json\":\"{}\",\"source_identity\":{\"shard_id\":7,\"range_id\":7},\"target_identity\":{\"shard_id\":7,\"range_id\":7}}}",
    }, "/internal/v1/groups/7/shard-ops/observe-split")).?;
    defer split_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), split_resp.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", split_resp.body);

    var merge_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/shard-ops/observe-merge",
        .body = "{\"transition_id\":2,\"donor_group_id\":8,\"receiver_group_id\":7,\"allow_doc_identity_reassignment\":true,\"table_contract\":{\"table_id\":7,\"table_name\":\"docs\",\"schema_json\":\"\",\"indexes_json\":\"{}\",\"source_identity\":{\"shard_id\":7,\"range_id\":7},\"target_identity\":{\"shard_id\":7,\"range_id\":7}}}",
    }, "/internal/v1/groups/7/shard-ops/observe-merge")).?;
    defer merge_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), merge_resp.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", merge_resp.body);

    var execute_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/shard-ops/execute",
        .body = "{\"kind\":\"finalize_merge\",\"transition_id\":3,\"donor_group_id\":8,\"receiver_group_id\":7,\"allow_doc_identity_reassignment\":true,\"table_contract\":{\"table_id\":7,\"table_name\":\"docs\",\"schema_json\":\"\",\"indexes_json\":\"{}\",\"source_identity\":{\"shard_id\":7,\"range_id\":7},\"target_identity\":{\"shard_id\":7,\"range_id\":7}}}",
    }, "/internal/v1/groups/7/shard-ops/execute")).?;
    defer execute_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), execute_resp.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", execute_resp.body);
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

    fn txnValidator() TxnValidator {
        return .{
            .ptr = undefined,
            .validate = validateTxn,
        };
    }

    fn validateBatch(_: *anyopaque, _: []const u8, _: []const db_mod.types.BatchWrite) !void {}

    fn validateTxn(_: *anyopaque, _: []const u8, _: []const db_mod.types.TransactionWrite) !void {}

    fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
        return null;
    }

    fn batchGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.BatchRequest) !?void {
        return null;
    }

    fn txnBeginGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: u64, _: []const []const u8) !?void {
        return null;
    }

    fn txnPrepareGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: u64, _: db_mod.types.TransactionIntentRequest) !?void {
        return null;
    }

    fn txnResolveGroupLocal(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId, _: db_mod.types.TxnStatus, _: u64) !?void {
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

fn freeTransitionActionOwned(alloc: std.mem.Allocator, action: *metadata_mod.TransitionAction) void {
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
