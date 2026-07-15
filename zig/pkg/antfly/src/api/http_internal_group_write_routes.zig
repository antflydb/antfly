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
const metadata_mod = @import("../metadata/mod.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const raft_mod = @import("../raft/mod.zig");
const repair_jobs = @import("repair_jobs.zig");
const routes = @import("http_routes.zig");
const relational_rows_api = @import("relational_rows.zig");
const schema_api = @import("../schema/mod.zig");
const storage_schema = @import("../storage/schema.zig");
const table_writes = @import("table_writes.zig");
const platform_time = @import("../platform/time.zig");

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

const ForeignKeyIntegrityRequestWire = struct {
    action: ?[]const u8 = null,
    phase: ?[]const u8 = null,
    constraint_name: ?[]const u8 = null,
    doc_key: ?[]const u8 = null,
    lower_doc_key: ?[]const u8 = null,
    upper_doc_key: ?[]const u8 = null,
    violation_limit: ?usize = null,
    job_id: ?[]const u8 = null,
    claim_key: ?[]const u8 = null,
    worker_id: ?[]const u8 = null,
    lease_ms: ?u64 = null,
    max_work_units: ?usize = null,
};

const UniqueIntegrityRequestWire = struct {
    action: ?[]const u8 = null,
    lower_doc_key: ?[]const u8 = null,
    upper_doc_key: ?[]const u8 = null,
};

const ForeignKeyActionJobRequestWire = struct {
    job_id: ?[]const u8 = null,
    action: ?[]const u8 = null,
    worker_id: ?[]const u8 = null,
    constraint_name: ?[]const u8 = null,
    parent_table: ?[]const u8 = null,
    parent_key: ?[]const u8 = null,
    updated_parent_key: ?[]const u8 = null,
    page_limit: ?usize = null,
    cascade_depth: ?u32 = null,
    cascade_max_depth: ?u32 = null,
    lease_ms: ?u64 = null,
    schedule_only: bool = false,
    requeue_only: bool = false,
};

const ForeignKeyActionScheduleRequestWire = struct {
    schedule_id: ?[]const u8 = null,
    action_job_id: ?[]const u8 = null,
    action: ?[]const u8 = null,
    worker_id: ?[]const u8 = null,
    constraint_name: ?[]const u8 = null,
    parent_table: ?[]const u8 = null,
    parent_key: ?[]const u8 = null,
    updated_parent_key: ?[]const u8 = null,
    page_limit: ?usize = null,
    scheduled_groups: ?u64 = null,
    requeue_only: bool = false,
};

const RowsMutationSourceStageWire = struct {
    schema_json: []const u8,
    request_body: []const u8,
    topology_epoch: u64 = 0,
    planned_stage: ?[]const u8 = null,
    autocommit: bool = true,
    sync_level: ?[]const u8 = null,
};

const RowsMutationSourceCollectWire = struct {
    schema_json: []const u8,
    request_body: []const u8,
    topology_epoch: u64 = 0,
};

const RowsJoinedMutationSourceStageWire = struct {
    target_schema_json: []const u8,
    source_schema_json: []const u8,
    request_body: []const u8,
    topology_epoch: u64 = 0,
    source_rows: []const []const u8 = &.{},
    planned_stage: ?[]const u8 = null,
    autocommit: bool = true,
    sync_level: ?[]const u8 = null,
};

const RowsJoinedMutationSourceCollectWire = struct {
    target_schema_json: []const u8,
    source_schema_json: []const u8,
    request_body: []const u8,
    topology_epoch: u64 = 0,
};

const RowsJoinedMutationSourceInputsWire = struct {
    target_schema_json: []const u8,
    source_schema_json: []const u8,
    request_body: []const u8,
    topology_epoch: u64 = 0,
};

fn parseForeignKeyIntegrityAction(value: ?[]const u8) !table_writes.ForeignKeyIntegrityAction {
    const text = value orelse "validate";
    if (enumTokenEql(text, "plan")) return .plan;
    if (enumTokenEql(text, "validate")) return .validate;
    if (enumTokenEql(text, "dry_run")) return .dry_run;
    if (enumTokenEql(text, "repair")) return .repair;
    if (enumTokenEql(text, "list")) return .list;
    if (enumTokenEql(text, "explain_delete")) return .explain_delete;
    if (enumTokenEql(text, "progress")) return .progress;
    return error.InvalidForeignKeyIntegrityRequest;
}

fn parseUniqueIntegrityAction(value: ?[]const u8) !table_writes.UniqueConstraintIntegrityAction {
    const text = value orelse "validate";
    if (enumTokenEql(text, "validate")) return .validate;
    if (enumTokenEql(text, "dry_run")) return .dry_run;
    if (enumTokenEql(text, "repair")) return .repair;
    if (enumTokenEql(text, "progress")) return .progress;
    return error.InvalidUniqueIntegrityRequest;
}

fn enumTokenEql(actual: []const u8, expected: []const u8) bool {
    var actual_index: usize = 0;
    var expected_index: usize = 0;
    while (true) {
        while (actual_index < actual.len and enumTokenSeparator(actual[actual_index])) actual_index += 1;
        while (expected_index < expected.len and enumTokenSeparator(expected[expected_index])) expected_index += 1;
        if (actual_index == actual.len or expected_index == expected.len) break;
        if (std.ascii.toLower(actual[actual_index]) != std.ascii.toLower(expected[expected_index])) return false;
        actual_index += 1;
        expected_index += 1;
    }
    while (actual_index < actual.len and enumTokenSeparator(actual[actual_index])) actual_index += 1;
    while (expected_index < expected.len and enumTokenSeparator(expected[expected_index])) expected_index += 1;
    return actual_index == actual.len and expected_index == expected.len;
}

fn enumTokenSeparator(ch: u8) bool {
    return ch == ' ' or ch == '_' or ch == '-';
}

fn runtimeSchemaFromJsonAlloc(alloc: std.mem.Allocator, schema_json: []const u8) !storage_schema.TableSchema {
    var parsed = schema_api.parseValidatedTableSchema(alloc, schema_json) catch return error.InvalidRowsMutationSourceRequest;
    defer parsed.deinit(alloc);
    return schema_api.deriveRuntimeTableSchema(alloc, parsed) catch return error.InvalidRowsMutationSourceRequest;
}

fn parseSyncLevel(text: ?[]const u8) !db_mod.types.SyncLevel {
    return db_mod.types.parsePublicSyncLevelText(text orelse "write") orelse error.InvalidRowsMutationSourceRequest;
}

pub fn handle(ctx: Context, req: http_common.HttpRequest, path: []const u8) !?http_common.HttpResponse {
    return handleImpl(ctx, req, path) catch |err| switch (err) {
        error.NotLeader, error.LeaderUnavailable, error.ReadUnavailable, error.WriteUnavailable => return try http_route_helpers.textResponse(ctx.alloc, 503, "leader unavailable"),
        else => return err,
    };
}

fn handleImpl(ctx: Context, req: http_common.HttpRequest, path: []const u8) !?http_common.HttpResponse {
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
            error.UnsupportedOperation => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            else => return err,
        };
        return try http_route_helpers.jsonResponse(ctx.alloc, struct {}{});
    }

    if (routes.Routes.matchGroupBatch(path)) |batch_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var batch_req = batch_api.parseBatchRequest(ctx.alloc, req.body) catch |err| switch (err) {
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
            error.InvalidBatchRequest, error.ForeignKeyViolation, error.UniqueConstraintViolation => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid batch request"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
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
    if (routes.Routes.matchGroupForeignKeyIntegrity(path)) |fk_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(ForeignKeyIntegrityRequestWire, ctx.alloc, if (req.body.len == 0) "{}" else req.body, .{
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key integrity request");
        defer parsed.deinit();
        const action = parseForeignKeyIntegrityAction(parsed.value.action) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key integrity request");
        };
        const violation_limit = @min(parsed.value.violation_limit orelse 100, 10_000);
        if (action == .explain_delete and (parsed.value.doc_key == null or parsed.value.doc_key.?.len == 0)) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "foreign key delete explain requires doc_key");
        }
        const lower_doc_key = if (action == .explain_delete) parsed.value.doc_key.? else parsed.value.lower_doc_key orelse "";
        const upper_doc_key = if (action == .explain_delete) "" else parsed.value.upper_doc_key orelse "";
        if (parsed.value.claim_key) |claim_key| {
            const worker_id = parsed.value.worker_id orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "foreign key integrity claim requires worker_id");
            if (claim_key.len == 0 or worker_id.len == 0) {
                return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key integrity claim");
            }
            if (parsed.value.job_id) |job_id| {
                if (job_id.len == 0) return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key integrity claim");
            }
            var result = (writes.foreignKeyIntegrityWorkUnitGroupLocal(
                ctx.alloc,
                fk_route.group_id,
                fk_route.table_name,
                action,
                parsed.value.phase orelse "child_range",
                parsed.value.job_id,
                claim_key,
                worker_id,
                parsed.value.lease_ms orelse 60_000,
                @max(1, parsed.value.max_work_units orelse 1),
                parsed.value.constraint_name,
                lower_doc_key,
                upper_doc_key,
                violation_limit,
            ) catch |err| switch (err) {
                error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
                error.InvalidForeignKeyIntegrityRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key integrity request"),
                error.ForeignKeyIntegrityClaimBusy => return try http_route_helpers.textResponse(ctx.alloc, 409, "foreign key integrity claim busy"),
                error.ForeignKeyNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "foreign key not found"),
                error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
            defer result.deinit(ctx.alloc);
            return try http_route_helpers.jsonResponse(ctx.alloc, result);
        }
        var result = (writes.foreignKeyIntegrityGroupLocal(
            ctx.alloc,
            fk_route.group_id,
            fk_route.table_name,
            action,
            parsed.value.constraint_name,
            lower_doc_key,
            upper_doc_key,
            violation_limit,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.ForeignKeyNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "foreign key not found"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer result.deinit(ctx.alloc);
        return try http_route_helpers.jsonResponse(ctx.alloc, result);
    }
    if (routes.Routes.matchGroupUniqueIntegrity(path)) |unique_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(UniqueIntegrityRequestWire, ctx.alloc, if (req.body.len == 0) "{}" else req.body, .{
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid unique integrity request");
        defer parsed.deinit();
        const action = parseUniqueIntegrityAction(parsed.value.action) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid unique integrity request");
        };
        var result = (writes.uniqueConstraintIntegrityGroupLocal(
            ctx.alloc,
            unique_route.group_id,
            unique_route.table_name,
            action,
            parsed.value.lower_doc_key orelse "",
            parsed.value.upper_doc_key orelse "",
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer result.deinit(ctx.alloc);
        return try http_route_helpers.jsonResponse(ctx.alloc, result);
    }
    if (routes.Routes.matchGroupGraphMetricMaintenance(path)) |graph_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        const body = (writes.graphMetricMaintenanceGroupLocal(
            ctx.alloc,
            graph_route.group_id,
            graph_route.table_name,
            req.body,
        ) catch |err| switch (err) {
            error.InvalidGraphMetricRuntimeConfig => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid graph metric runtime config"),
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return .{
            .status = 200,
            .content_type = try ctx.alloc.dupe(u8, "application/json"),
            .body = body,
        };
    }
    if (routes.Routes.matchGroupSecondaryIndexRebuild(path)) |rebuild_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(table_writes.SecondaryIndexRebuildGroupRequest, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid secondary index rebuild request");
        defer parsed.deinit();
        if (parsed.value.worker_id.len == 0 or parsed.value.lease_ms == 0) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid secondary index rebuild request");
        }
        const result = (writes.secondaryIndexRebuildGroupLocal(
            ctx.alloc,
            rebuild_route.group_id,
            rebuild_route.table_name,
            parsed.value.record,
            parsed.value.worker_id,
            parsed.value.lease_ms,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.InvalidSecondaryIndexRebuildRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid secondary index rebuild request"),
            error.SecondaryIndexRebuildRangeClaimBusy => return try http_route_helpers.textResponse(ctx.alloc, 409, "secondary index rebuild claim busy"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, result);
    }
    if (routes.Routes.matchGroupRelationalColumnBackedIndexRepair(path)) |repair_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(table_writes.RelationalColumnBackedIndexRepairGroupRequest, ctx.alloc, if (req.body.len == 0) "{}" else req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid relational index repair request");
        defer parsed.deinit();
        const result = (writes.relationalColumnBackedIndexRepairGroupLocal(
            ctx.alloc,
            repair_route.group_id,
            repair_route.table_name,
            parsed.value.lower_doc_key,
            parsed.value.upper_doc_key,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, result);
    }
    if (routes.Routes.matchGroupSchemaRewrite(path)) |rewrite_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(table_writes.SchemaRewriteGroupRequest, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid schema rewrite request");
        defer parsed.deinit();
        if (parsed.value.worker_id.len == 0 or parsed.value.lease_ms == 0 or parsed.value.record.group_id != rewrite_route.group_id) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid schema rewrite request");
        }
        const result = (writes.schemaRewriteGroupLocal(
            ctx.alloc,
            rewrite_route.group_id,
            rewrite_route.table_name,
            parsed.value.record,
            parsed.value.worker_id,
            parsed.value.lease_ms,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.InvalidSchemaRewriteJob, error.InvalidSchemaRewriteJobRange, error.InvalidSchemaRewriteExpression => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid schema rewrite request"),
            error.SchemaRewriteJobClaimBusy => return try http_route_helpers.textResponse(ctx.alloc, 409, "schema rewrite claim busy"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, result);
    }
    if (routes.Routes.matchGroupTableEmptying(path)) |emptying_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(table_writes.TableEmptyingGroupRequest, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid table emptying request");
        defer parsed.deinit();
        if (parsed.value.worker_id.len == 0 or parsed.value.lease_ms == 0 or parsed.value.record.group_id != emptying_route.group_id) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid table emptying request");
        }
        const result = (writes.tableEmptyingGroupLocal(
            ctx.alloc,
            emptying_route.group_id,
            emptying_route.table_name,
            parsed.value.record,
            parsed.value.worker_id,
            parsed.value.lease_ms,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.InvalidTableEmptyingJob, error.InvalidTableEmptyingJobLease => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid table emptying request"),
            error.TableEmptyingJobClaimBusy => return try http_route_helpers.textResponse(ctx.alloc, 409, "table emptying claim busy"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        return try http_route_helpers.jsonResponse(ctx.alloc, result);
    }
    if (routes.Routes.matchGroupForeignKeyRefChildren(path)) |fk_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var scan_req = distributed_txn.parseForeignKeyRefChildrenRequest(ctx.alloc, if (req.body.len == 0) "{}" else req.body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key ref children request");
        };
        defer distributed_txn.freeForeignKeyRefChildrenRequest(ctx.alloc, &scan_req);
        const limit = @min(scan_req.limit, 10_000);
        var page = (writes.foreignKeyRefChildrenPageGroupLocal(
            ctx.alloc,
            fk_route.group_id,
            fk_route.table_name,
            scan_req.constraint_name,
            scan_req.parent_table,
            scan_req.parent_key,
            scan_req.start_after_child_table,
            scan_req.start_after_child_key,
            limit,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.ForeignKeyNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "foreign key not found"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            error.ForeignKeyActionLimitExceeded => return try http_route_helpers.textResponse(ctx.alloc, 409, "foreign key action limit exceeded"),
            error.ForeignKeyViolation => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key ref children request"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer table_writes.freeForeignKeyRefChildrenPage(ctx.alloc, &page);
        const body = try distributed_txn.encodeForeignKeyRefChildrenResponse(ctx.alloc, .{
            .children = page.children,
            .complete = page.complete,
            .next_child_table = page.next_child_table,
            .next_child_key = page.next_child_key,
        });
        errdefer ctx.alloc.free(body);
        return .{
            .status = 200,
            .content_type = try ctx.alloc.dupe(u8, "application/json"),
            .body = body,
        };
    }
    if (routes.Routes.matchGroupForeignKeyActionJob(path)) |fk_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(ForeignKeyActionJobRequestWire, ctx.alloc, if (req.body.len == 0) "{}" else req.body, .{
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        defer parsed.deinit();
        const job_id = parsed.value.job_id orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        const action = parsed.value.action orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        const worker_id = parsed.value.worker_id orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        const constraint_name = parsed.value.constraint_name orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        const parent_table = parsed.value.parent_table orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        const parent_key = parsed.value.parent_key orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        if (job_id.len == 0 or action.len == 0 or worker_id.len == 0 or constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        }
        if (parsed.value.schedule_only and parsed.value.requeue_only) {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
        }
        const maybe_status = if (parsed.value.schedule_only) blk: {
            const cascade_depth = parsed.value.cascade_depth orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
            const cascade_max_depth = parsed.value.cascade_max_depth orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
            if (cascade_max_depth == 0 or cascade_depth > cascade_max_depth) {
                return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
            }
            break :blk writes.foreignKeyActionJobGroupLocalSchedule(
                ctx.alloc,
                fk_route.group_id,
                fk_route.table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                parsed.value.updated_parent_key,
                @min(parsed.value.page_limit orelse 1024, 10_000),
                cascade_depth,
                cascade_max_depth,
            );
        } else if (parsed.value.requeue_only)
            writes.foreignKeyActionJobGroupLocalRequeue(
                ctx.alloc,
                fk_route.group_id,
                fk_route.table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                parsed.value.updated_parent_key,
                @min(parsed.value.page_limit orelse 1024, 10_000),
            )
        else blk: {
            const cascade_depth = parsed.value.cascade_depth orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
            const cascade_max_depth = parsed.value.cascade_max_depth orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
            if (cascade_max_depth == 0 or cascade_depth > cascade_max_depth) {
                return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request");
            }
            break :blk writes.foreignKeyActionJobGroupLocal(
                ctx.alloc,
                fk_route.group_id,
                fk_route.table_name,
                job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                parsed.value.updated_parent_key,
                @min(parsed.value.page_limit orelse 1024, 10_000),
                parsed.value.lease_ms orelse 60_000,
                cascade_depth,
                cascade_max_depth,
            );
        };
        var status = (maybe_status catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.InvalidForeignKeyActionJob, error.ForeignKeyViolation => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action job request"),
            error.ForeignKeyIntegrityClaimBusy => return try http_route_helpers.textResponse(ctx.alloc, 409, "foreign key action job claim busy"),
            error.ForeignKeyNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "foreign key not found"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer status.deinit(ctx.alloc);
        return try http_route_helpers.jsonResponse(ctx.alloc, status);
    }
    if (routes.Routes.matchGroupForeignKeyActionJobProgress(path)) |fk_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var progress = (writes.foreignKeyActionJobGroupLocalProgress(
            ctx.alloc,
            fk_route.group_id,
            fk_route.table_name,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer progress.deinit(ctx.alloc);
        return try http_route_helpers.jsonResponse(ctx.alloc, progress);
    }
    if (routes.Routes.matchGroupForeignKeyActionScheduleProgress(path)) |fk_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var progress = (writes.foreignKeyActionScheduleGroupLocalProgress(
            ctx.alloc,
            fk_route.group_id,
            fk_route.table_name,
        ) catch |err| switch (err) {
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer progress.deinit(ctx.alloc);
        return try http_route_helpers.jsonResponse(ctx.alloc, progress);
    }
    if (routes.Routes.matchGroupForeignKeyActionSchedule(path)) |fk_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(ForeignKeyActionScheduleRequestWire, ctx.alloc, if (req.body.len == 0) "{}" else req.body, .{
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
        defer parsed.deinit();
        const schedule_id = parsed.value.schedule_id orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
        if (schedule_id.len == 0) return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
        var status = if (parsed.value.requeue_only) blk: {
            const action_job_id = parsed.value.action_job_id orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
            const action = parsed.value.action orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
            const worker_id = parsed.value.worker_id orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
            const constraint_name = parsed.value.constraint_name orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
            const parent_table = parsed.value.parent_table orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
            const parent_key = parsed.value.parent_key orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
            const page_limit = parsed.value.page_limit orelse return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
            if (action_job_id.len == 0 or action.len == 0 or worker_id.len == 0 or constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0 or page_limit == 0) {
                return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request");
            }
            break :blk (writes.foreignKeyActionScheduleGroupLocalRequeue(
                ctx.alloc,
                fk_route.group_id,
                fk_route.table_name,
                schedule_id,
                action_job_id,
                action,
                worker_id,
                constraint_name,
                parent_table,
                parent_key,
                parsed.value.updated_parent_key,
                page_limit,
            ) catch |err| switch (err) {
                error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
                error.InvalidForeignKeyActionJob => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request"),
                error.UnknownGroup, error.TableNotFound, error.ForeignKeyActionScheduleNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        } else blk: {
            break :blk (writes.foreignKeyActionScheduleGroupLocalMarkSeeded(
                ctx.alloc,
                fk_route.group_id,
                fk_route.table_name,
                schedule_id,
                parsed.value.scheduled_groups orelse 0,
            ) catch |err| switch (err) {
                error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
                error.InvalidForeignKeyActionJob => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid foreign key action schedule request"),
                error.UnknownGroup, error.TableNotFound, error.ForeignKeyActionScheduleNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
                else => return err,
            }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        };
        defer status.deinit(ctx.alloc);
        return try http_route_helpers.jsonResponse(ctx.alloc, status);
    }
    if (routes.Routes.matchGroupRowsMutationSourceCollect(path)) |mutation_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(RowsMutationSourceCollectWire, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        defer parsed.deinit();

        const schema = runtimeSchemaFromJsonAlloc(ctx.alloc, parsed.value.schema_json) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer storage_schema.freeSchema(ctx.alloc, schema);

        var rows_req = relational_rows_api.parseRowsMutationSourceRequest(ctx.alloc, parsed.value.request_body, schema) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer rows_req.deinit(ctx.alloc);

        const response_body = (writes.rowsMutationSourceCollectGroupLocal(ctx.alloc, mutation_route.group_id, mutation_route.table_name, parsed.value.topology_epoch, schema, rows_req.req) catch |err| switch (err) {
            error.InvalidArgument, error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request"),
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.TopologyChanged => return try http_route_helpers.textResponse(ctx.alloc, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer ctx.alloc.free(response_body);
        return .{
            .status = 200,
            .content_type = try ctx.alloc.dupe(u8, "application/json"),
            .body = try ctx.alloc.dupe(u8, response_body),
        };
    }
    if (routes.Routes.matchGroupRowsMutationSourceStage(path)) |mutation_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(RowsMutationSourceStageWire, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        defer parsed.deinit();

        const sync_level = if (parsed.value.autocommit) parseSyncLevel(parsed.value.sync_level) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        } else db_mod.types.SyncLevel.write;
        const schema = runtimeSchemaFromJsonAlloc(ctx.alloc, parsed.value.schema_json) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer storage_schema.freeSchema(ctx.alloc, schema);

        var rows_req = relational_rows_api.parseRowsMutationSourceRequest(ctx.alloc, parsed.value.request_body, schema) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer rows_req.deinit(ctx.alloc);

        var planned_stage = if (parsed.value.planned_stage) |planned_body| relational_rows_api.parseRowsMutationSourcePlannedStageAlloc(ctx.alloc, planned_body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        } else null;
        defer if (planned_stage) |*stage| stage.deinit(ctx.alloc);
        if (planned_stage != null and parsed.value.autocommit) return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");

        var result = ((if (planned_stage) |stage|
            writes.rowsMutationSourceStagePlannedGroupLocal(ctx.alloc, mutation_route.group_id, mutation_route.table_name, parsed.value.topology_epoch, schema, rows_req.req, stage.matched, stage.candidates)
        else if (parsed.value.autocommit)
            writes.mutateRowsFromSourceAutocommit(ctx.alloc, mutation_route.table_name, schema, rows_req.req, sync_level)
        else
            writes.mutateRowsFromSource(ctx.alloc, mutation_route.table_name, schema, rows_req.req)) catch |err| switch (err) {
            error.InvalidArgument, error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request"),
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.VersionConflict, error.IntentConflict, error.DecisionConflict, error.TxnNotFound, error.InvalidTxnRecord => return try http_route_helpers.textResponse(ctx.alloc, 409, "version conflict"),
            error.TopologyChanged => return try http_route_helpers.textResponse(ctx.alloc, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer result.deinit(ctx.alloc);

        const response_body = try relational_rows_api.encodeRowsMutationSourceResponseAlloc(ctx.alloc, result);
        defer ctx.alloc.free(response_body);
        return .{
            .status = 200,
            .content_type = try ctx.alloc.dupe(u8, "application/json"),
            .body = try ctx.alloc.dupe(u8, response_body),
        };
    }
    if (routes.Routes.matchGroupRowsJoinedMutationSourceCollect(path)) |mutation_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(RowsJoinedMutationSourceCollectWire, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        defer parsed.deinit();

        const target_schema = runtimeSchemaFromJsonAlloc(ctx.alloc, parsed.value.target_schema_json) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer storage_schema.freeSchema(ctx.alloc, target_schema);
        const source_schema = runtimeSchemaFromJsonAlloc(ctx.alloc, parsed.value.source_schema_json) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer storage_schema.freeSchema(ctx.alloc, source_schema);

        var rows_req = relational_rows_api.parseRowsJoinedMutationSourceRequestWithSchemas(ctx.alloc, parsed.value.request_body, target_schema, source_schema) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer rows_req.deinit(ctx.alloc);

        const response_body = (writes.rowsJoinedMutationSourceCollectGroupLocal(ctx.alloc, mutation_route.group_id, mutation_route.table_name, parsed.value.topology_epoch, target_schema, rows_req.req) catch |err| switch (err) {
            error.InvalidArgument, error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request"),
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.TopologyChanged => return try http_route_helpers.textResponse(ctx.alloc, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer ctx.alloc.free(response_body);
        return .{
            .status = 200,
            .content_type = try ctx.alloc.dupe(u8, "application/json"),
            .body = try ctx.alloc.dupe(u8, response_body),
        };
    }
    if (routes.Routes.matchGroupRowsJoinedMutationSourceInputs(path)) |mutation_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(RowsJoinedMutationSourceInputsWire, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        defer parsed.deinit();

        const target_schema = runtimeSchemaFromJsonAlloc(ctx.alloc, parsed.value.target_schema_json) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer storage_schema.freeSchema(ctx.alloc, target_schema);
        const source_schema = runtimeSchemaFromJsonAlloc(ctx.alloc, parsed.value.source_schema_json) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer storage_schema.freeSchema(ctx.alloc, source_schema);

        var rows_req = relational_rows_api.parseRowsJoinedMutationSourceRequestWithSchemas(ctx.alloc, parsed.value.request_body, target_schema, source_schema) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer rows_req.deinit(ctx.alloc);

        const response_body = (writes.rowsJoinedMutationSourceInputsGroupLocal(ctx.alloc, mutation_route.group_id, mutation_route.table_name, parsed.value.topology_epoch, source_schema, rows_req.req) catch |err| switch (err) {
            error.InvalidArgument, error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request"),
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.TopologyChanged => return try http_route_helpers.textResponse(ctx.alloc, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer ctx.alloc.free(response_body);
        return .{
            .status = 200,
            .content_type = try ctx.alloc.dupe(u8, "application/json"),
            .body = try ctx.alloc.dupe(u8, response_body),
        };
    }
    if (routes.Routes.matchGroupRowsJoinedMutationSourceStage(path)) |mutation_route| {
        const writes = ctx.writes orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        var parsed = std.json.parseFromSlice(RowsJoinedMutationSourceStageWire, ctx.alloc, req.body, .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = true,
        }) catch return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        defer parsed.deinit();

        const sync_level = if (parsed.value.autocommit) parseSyncLevel(parsed.value.sync_level) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        } else db_mod.types.SyncLevel.write;
        const target_schema = runtimeSchemaFromJsonAlloc(ctx.alloc, parsed.value.target_schema_json) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer storage_schema.freeSchema(ctx.alloc, target_schema);
        const source_schema = runtimeSchemaFromJsonAlloc(ctx.alloc, parsed.value.source_schema_json) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer storage_schema.freeSchema(ctx.alloc, source_schema);

        var rows_req = relational_rows_api.parseRowsJoinedMutationSourceRequestWithSchemas(ctx.alloc, parsed.value.request_body, target_schema, source_schema) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        };
        defer rows_req.deinit(ctx.alloc);

        var planned_stage = if (parsed.value.planned_stage) |planned_body| relational_rows_api.parseRowsJoinedMutationSourcePlannedStageAlloc(ctx.alloc, planned_body) catch {
            return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");
        } else null;
        defer if (planned_stage) |*stage| stage.deinit(ctx.alloc);
        if (planned_stage != null and parsed.value.autocommit) return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request");

        var result = ((if (planned_stage) |stage|
            writes.rowsJoinedMutationSourceStagePlannedGroupLocal(ctx.alloc, mutation_route.group_id, mutation_route.table_name, parsed.value.topology_epoch, target_schema, source_schema, rows_req.req, stage.matched, stage.candidates)
        else if (parsed.value.autocommit)
            writes.mutateRowsJoinedFromSourceRowsAutocommit(ctx.alloc, mutation_route.table_name, target_schema, source_schema, rows_req.req, parsed.value.source_rows, sync_level)
        else
            writes.mutateRowsJoinedFromSourceRows(ctx.alloc, mutation_route.table_name, target_schema, source_schema, rows_req.req, parsed.value.source_rows)) catch |err| switch (err) {
            error.InvalidArgument, error.InvalidQueryRequest, error.UnsupportedQueryRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid rows mutation source request"),
            error.UnsupportedOperation, error.ReadOnly => return try http_route_helpers.textResponse(ctx.alloc, 405, "method not allowed"),
            error.VersionConflict, error.IntentConflict, error.DecisionConflict, error.TxnNotFound, error.InvalidTxnRecord => return try http_route_helpers.textResponse(ctx.alloc, 409, "version conflict"),
            error.TopologyChanged => return try http_route_helpers.textResponse(ctx.alloc, 409, "topology changed"),
            error.DocIdentityNamespaceMismatch => return try http_route_helpers.textResponse(ctx.alloc, 409, "doc identity namespace mismatch"),
            error.UnknownGroup, error.TableNotFound => return try http_route_helpers.textResponse(ctx.alloc, 404, "not found"),
            else => return err,
        }) orelse return try http_route_helpers.textResponse(ctx.alloc, 404, "not found");
        defer result.deinit(ctx.alloc);

        const response_body = try relational_rows_api.encodeRowsMutationSourceResponseAlloc(ctx.alloc, result);
        defer ctx.alloc.free(response_body);
        return .{
            .status = 200,
            .content_type = try ctx.alloc.dupe(u8, "application/json"),
            .body = try ctx.alloc.dupe(u8, response_body),
        };
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
            error.InvalidBatchRequest, error.ForeignKeyViolation, error.UniqueConstraintViolation => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transaction request"),
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
            error.InvalidBatchRequest, error.ForeignKeyViolation, error.UniqueConstraintViolation => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid transaction request"),
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
    source_group_id: ?u64 = null,
    destination_group_id: ?u64 = null,
    donor_group_id: ?u64 = null,
    receiver_group_id: ?u64 = null,
    allow_doc_identity_reassignment: bool = false,
    split_key: ?[]const u8 = null,
    source_range_end: ?[]const u8 = null,
};

fn parseSplitTransitionRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_transition_state.SplitTransitionRecord {
    var parsed = try std.json.parseFromSlice(metadata_transition_state.SplitTransitionRecord, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    return .{
        .transition_id = parsed.value.transition_id,
        .source_group_id = parsed.value.source_group_id,
        .destination_group_id = parsed.value.destination_group_id,
        .phase = parsed.value.phase,
        .split_key = if (parsed.value.split_key) |value| try alloc.dupe(u8, value) else null,
        .source_range_end = if (parsed.value.source_range_end) |value| try alloc.dupe(u8, value) else null,
        .rollback_reason = if (parsed.value.rollback_reason) |value| try alloc.dupe(u8, value) else null,
    };
}

fn parseMergeTransitionRecord(alloc: std.mem.Allocator, body: []const u8) !metadata_transition_state.MergeTransitionRecord {
    var parsed = try std.json.parseFromSlice(metadata_transition_state.MergeTransitionRecord, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    return .{
        .transition_id = parsed.value.transition_id,
        .donor_group_id = parsed.value.donor_group_id,
        .receiver_group_id = parsed.value.receiver_group_id,
        .phase = parsed.value.phase,
        .rollback_reason = if (parsed.value.rollback_reason) |value| try alloc.dupe(u8, value) else null,
        .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
    };
}

fn parseTransitionAction(alloc: std.mem.Allocator, body: []const u8) !metadata_mod.TransitionAction {
    var parsed = try std.json.parseFromSlice(EncodedTransitionAction, alloc, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    return switch (parsed.value.kind) {
        .prepare_split_source => .{
            .prepare_split_source = .{
                .transition_id = parsed.value.transition_id,
                .source_group_id = parsed.value.source_group_id orelse return error.InvalidTransitionActionRequest,
                .destination_group_id = parsed.value.destination_group_id orelse return error.InvalidTransitionActionRequest,
                .split_key = try alloc.dupe(u8, parsed.value.split_key orelse return error.InvalidTransitionActionRequest),
                .source_range_end = if (parsed.value.source_range_end) |value| try alloc.dupe(u8, value) else null,
            },
        },
        .start_split_source => .{
            .start_split_source = .{
                .transition_id = parsed.value.transition_id,
                .source_group_id = parsed.value.source_group_id orelse return error.InvalidTransitionActionRequest,
                .destination_group_id = parsed.value.destination_group_id orelse return error.InvalidTransitionActionRequest,
            },
        },
        .bootstrap_split_destination => .{
            .bootstrap_split_destination = .{
                .transition_id = parsed.value.transition_id,
                .source_group_id = parsed.value.source_group_id orelse return error.InvalidTransitionActionRequest,
                .destination_group_id = parsed.value.destination_group_id orelse return error.InvalidTransitionActionRequest,
            },
        },
        .catch_up_split_destination => .{
            .catch_up_split_destination = .{
                .transition_id = parsed.value.transition_id,
                .source_group_id = parsed.value.source_group_id orelse return error.InvalidTransitionActionRequest,
                .destination_group_id = parsed.value.destination_group_id orelse return error.InvalidTransitionActionRequest,
            },
        },
        .finalize_split_source => .{
            .finalize_split_source = .{
                .transition_id = parsed.value.transition_id,
                .source_group_id = parsed.value.source_group_id orelse return error.InvalidTransitionActionRequest,
                .destination_group_id = parsed.value.destination_group_id orelse return error.InvalidTransitionActionRequest,
            },
        },
        .rollback_split => .{
            .rollback_split = .{
                .transition_id = parsed.value.transition_id,
                .source_group_id = parsed.value.source_group_id orelse return error.InvalidTransitionActionRequest,
                .destination_group_id = parsed.value.destination_group_id orelse return error.InvalidTransitionActionRequest,
            },
        },
        .accept_merge_receiver => .{
            .accept_merge_receiver = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = parsed.value.donor_group_id orelse return error.InvalidTransitionActionRequest,
                .receiver_group_id = parsed.value.receiver_group_id orelse return error.InvalidTransitionActionRequest,
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
            },
        },
        .catch_up_merge_receiver => .{
            .catch_up_merge_receiver = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = parsed.value.donor_group_id orelse return error.InvalidTransitionActionRequest,
                .receiver_group_id = parsed.value.receiver_group_id orelse return error.InvalidTransitionActionRequest,
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
            },
        },
        .finalize_merge => .{
            .finalize_merge = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = parsed.value.donor_group_id orelse return error.InvalidTransitionActionRequest,
                .receiver_group_id = parsed.value.receiver_group_id orelse return error.InvalidTransitionActionRequest,
                .allow_doc_identity_reassignment = parsed.value.allow_doc_identity_reassignment,
            },
        },
        .rollback_merge => .{
            .rollback_merge = .{
                .transition_id = parsed.value.transition_id,
                .donor_group_id = parsed.value.donor_group_id orelse return error.InvalidTransitionActionRequest,
                .receiver_group_id = parsed.value.receiver_group_id orelse return error.InvalidTransitionActionRequest,
            },
        },
    };
}

fn freeSplitTransitionRecordOwned(alloc: std.mem.Allocator, record: *metadata_transition_state.SplitTransitionRecord) void {
    if (record.split_key) |value| alloc.free(value);
    if (record.source_range_end) |value| alloc.free(value);
    if (record.rollback_reason) |value| alloc.free(value);
    record.* = undefined;
}

fn freeMergeTransitionRecordOwned(alloc: std.mem.Allocator, record: *metadata_transition_state.MergeTransitionRecord) void {
    if (record.rollback_reason) |value| alloc.free(value);
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

test "internal group write routes forward batch identity rewrites" {
    const alloc = std.testing.allocator;

    const CaptureWriteSource = struct {
        calls: usize = 0,
        group_id: u64 = 0,
        table_name: ?[]u8 = null,
        old_key: ?[]u8 = null,
        new_key: ?[]u8 = null,
        value: ?[]u8 = null,

        fn deinit(self: *@This(), alloc_inner: std.mem.Allocator) void {
            if (self.table_name) |value| alloc_inner.free(value);
            if (self.old_key) |value| alloc_inner.free(value);
            if (self.new_key) |value| alloc_inner.free(value);
            if (self.value) |value| alloc_inner.free(value);
        }

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .batch_group_local = batchGroupLocal,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return null;
        }

        fn batchGroupLocal(ptr: *anyopaque, alloc_inner: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_mod.types.BatchRequest) !?void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 0), req.writes.len);
            try std.testing.expectEqual(@as(usize, 0), req.deletes.len);
            try std.testing.expectEqual(@as(usize, 1), req.relational_identity_rewrites.len);
            try std.testing.expectEqual(@as(usize, 0), req.transforms.len);
            self.calls += 1;
            self.group_id = group_id;
            self.table_name = try alloc_inner.dupe(u8, table_name);
            self.old_key = try alloc_inner.dupe(u8, req.relational_identity_rewrites[0].old_key);
            self.new_key = try alloc_inner.dupe(u8, req.relational_identity_rewrites[0].new_key);
            self.value = try alloc_inner.dupe(u8, req.relational_identity_rewrites[0].value);
            return {};
        }
    };

    var capture = CaptureWriteSource{};
    defer capture.deinit(alloc);
    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = capture.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/batch",
        .body = "{\"relational_identity_rewrites\":[{\"old_key\":\"doc:a\",\"new_key\":\"doc:b\",\"value\":{\"id\":\"b\",\"status\":\"renamed\"}}],\"sync_level\":\"write\"}",
    }, "/internal/v1/groups/7/tables/docs/batch")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 201), resp.status);
    try std.testing.expectEqual(@as(usize, 1), capture.calls);
    try std.testing.expectEqual(@as(u64, 7), capture.group_id);
    try std.testing.expectEqualStrings("docs", capture.table_name.?);
    try std.testing.expectEqualStrings("doc:a", capture.old_key.?);
    try std.testing.expectEqualStrings("doc:b", capture.new_key.?);
    try std.testing.expect(std.mem.indexOf(u8, capture.value.?, "\"renamed\"") != null);
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

test "internal group write routes expose foreign key integrity" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/foreign-key-integrity",
        .body = "{\"action\":\"list\",\"violation_limit\":3}",
    }, "/internal/v1/groups/7/tables/docs/foreign-key-integrity")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    var parsed = try std.json.parseFromSlice(table_writes.ForeignKeyIntegrityResult, alloc, resp.body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(table_writes.ForeignKeyIntegrityAction.list, parsed.value.action);
    try std.testing.expectEqual(@as(u64, 7), parsed.value.groups[0].group_id);
    try std.testing.expectEqual(@as(usize, 3), parsed.value.violation_limit);

    var dry_run_resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/foreign-key-integrity",
        .body = "{\"action\":\"DRY RUN\"}",
    }, "/internal/v1/groups/7/tables/docs/foreign-key-integrity")).?;
    defer dry_run_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), dry_run_resp.status);
    var dry_run_parsed = try std.json.parseFromSlice(table_writes.ForeignKeyIntegrityResult, alloc, dry_run_resp.body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer dry_run_parsed.deinit();
    try std.testing.expectEqual(table_writes.ForeignKeyIntegrityAction.dry_run, dry_run_parsed.value.action);

    var claim_resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/foreign-key-integrity",
        .body = "{\"action\":\"validate\",\"job_id\":\"job:internal-fk\",\"claim_key\":\"fk:claim:7:docs:a:z\",\"worker_id\":\"worker:1\",\"lease_ms\":1000,\"max_work_units\":4,\"lower_doc_key\":\"a\",\"upper_doc_key\":\"z\"}",
    }, "/internal/v1/groups/7/tables/docs/foreign-key-integrity")).?;
    defer claim_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), claim_resp.status);
    var claim_parsed = try std.json.parseFromSlice(table_writes.ForeignKeyIntegrityResult, alloc, claim_resp.body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer claim_parsed.deinit();
    try std.testing.expectEqual(table_writes.ForeignKeyIntegrityAction.validate, claim_parsed.value.action);
    try std.testing.expect(claim_parsed.value.complete);
    try std.testing.expectEqual(@as(usize, 1), claim_parsed.value.work_claims.len);
    try std.testing.expectEqualStrings("fk:claim:7:docs:a:z", claim_parsed.value.work_claims[0].claim_key);
    try std.testing.expectEqualStrings("worker:1", claim_parsed.value.work_claims[0].worker_id);
    try std.testing.expectEqualStrings("a", claim_parsed.value.work_claims[0].lower_doc_key);
    try std.testing.expectEqualStrings("z", claim_parsed.value.work_claims[0].upper_doc_key);
    try std.testing.expectEqual(@as(u32, 4), claim_parsed.value.work_claims[0].attempts);
}

test "internal group write routes expose foreign key action job requeue" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/foreign-key-action-job",
        .body = "{\"job_id\":\"fk-action:set-null:7\",\"action\":\"set_null\",\"worker_id\":\"worker:retry\",\"constraint_name\":\"orders_customer_id_fkey\",\"parent_table\":\"customers\",\"parent_key\":\"customer:7\",\"page_limit\":12,\"requeue_only\":true}",
    }, "/internal/v1/groups/7/tables/docs/foreign-key-action-job")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    var parsed = try std.json.parseFromSlice(table_writes.ForeignKeyActionJobStatus, alloc, resp.body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 7), parsed.value.group_id);
    try std.testing.expectEqualStrings("fk-action:set-null:7", parsed.value.job_id);
    try std.testing.expectEqualStrings("set_null", parsed.value.action);
    try std.testing.expectEqualStrings("worker:retry", parsed.value.worker_id);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.value.constraint_name);
    try std.testing.expectEqualStrings("customers", parsed.value.parent_table);
    try std.testing.expectEqualStrings("customer:7", parsed.value.parent_key);
    try std.testing.expectEqual(@as(usize, 12), parsed.value.page_limit);
    try std.testing.expectEqualStrings("pending", parsed.value.status);
    try std.testing.expectEqual(@as(u64, 5), parsed.value.applied_children);
    try std.testing.expectEqual(@as(u64, 3), parsed.value.failure_count);
    try std.testing.expectEqual(@as(u64, 101), parsed.value.first_failed_at_ns.?);
    try std.testing.expectEqual(@as(u64, 202), parsed.value.last_failed_at_ns.?);
    try std.testing.expectEqual(@as(u64, 2), parsed.value.requeue_count);
    try std.testing.expectEqual(@as(u64, 303), parsed.value.last_requeued_at_ns.?);
    try std.testing.expectEqual(@as(u32, 4), parsed.value.cascade_depth);
    try std.testing.expectEqual(@as(u32, 9), parsed.value.cascade_max_depth);
    try std.testing.expectEqualStrings("row", parsed.value.next_child_table.?);
    try std.testing.expectEqualStrings("order:cursor", parsed.value.next_child_key.?);

    var claimed_resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/foreign-key-action-job",
        .body = "{\"job_id\":\"fk-action:cascade:7\",\"action\":\"cascade\",\"worker_id\":\"worker:claim\",\"constraint_name\":\"orders_customer_id_fkey\",\"parent_table\":\"customers\",\"parent_key\":\"customer:7\",\"page_limit\":12,\"lease_ms\":30,\"cascade_depth\":2,\"cascade_max_depth\":8}",
    }, "/internal/v1/groups/7/tables/docs/foreign-key-action-job")).?;
    defer claimed_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), claimed_resp.status);
    var claimed_parsed = try std.json.parseFromSlice(table_writes.ForeignKeyActionJobStatus, alloc, claimed_resp.body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer claimed_parsed.deinit();
    try std.testing.expectEqualStrings("claimed", claimed_parsed.value.status);
    try std.testing.expectEqual(@as(u32, 2), claimed_parsed.value.cascade_depth);
    try std.testing.expectEqual(@as(u32, 8), claimed_parsed.value.cascade_max_depth);

    var schedule_resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/foreign-key-action-job",
        .body = "{\"job_id\":\"fk-action:cascade:7\",\"action\":\"cascade\",\"worker_id\":\"worker:schedule\",\"constraint_name\":\"orders_customer_id_fkey\",\"parent_table\":\"customers\",\"parent_key\":\"customer:7\",\"page_limit\":12,\"cascade_depth\":3,\"cascade_max_depth\":9,\"schedule_only\":true}",
    }, "/internal/v1/groups/7/tables/docs/foreign-key-action-job")).?;
    defer schedule_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), schedule_resp.status);
    var schedule_parsed = try std.json.parseFromSlice(table_writes.ForeignKeyActionJobStatus, alloc, schedule_resp.body, .{
        .allocate = .alloc_always,
        .ignore_unknown_fields = true,
    });
    defer schedule_parsed.deinit();
    try std.testing.expectEqualStrings("scheduled", schedule_parsed.value.status);
    try std.testing.expectEqual(@as(u32, 3), schedule_parsed.value.cascade_depth);
    try std.testing.expectEqual(@as(u32, 9), schedule_parsed.value.cascade_max_depth);

    var missing_lineage_resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/foreign-key-action-job",
        .body = "{\"job_id\":\"fk-action:cascade:7\",\"action\":\"cascade\",\"worker_id\":\"worker:schedule\",\"constraint_name\":\"orders_customer_id_fkey\",\"parent_table\":\"customers\",\"parent_key\":\"customer:7\",\"page_limit\":12,\"schedule_only\":true}",
    }, "/internal/v1/groups/7/tables/docs/foreign-key-action-job")).?;
    defer missing_lineage_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), missing_lineage_resp.status);
    try std.testing.expectEqualStrings("invalid foreign key action job request", missing_lineage_resp.body);

    var invalid_resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/foreign-key-action-job",
        .body = "{\"job_id\":\"fk-action:set-null:7\",\"action\":\"set_null\",\"worker_id\":\"worker:retry\",\"constraint_name\":\"orders_customer_id_fkey\",\"parent_table\":\"customers\",\"parent_key\":\"customer:7\",\"schedule_only\":true,\"requeue_only\":true}",
    }, "/internal/v1/groups/7/tables/docs/foreign-key-action-job")).?;
    defer invalid_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), invalid_resp.status);
    try std.testing.expectEqualStrings("invalid foreign key action job request", invalid_resp.body);
}

test "internal group write routes expose unique integrity" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/unique-integrity",
        .body = "{\"action\":\"REPAIR\"}",
    }, "/internal/v1/groups/7/tables/docs/unique-integrity")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    var parsed = try std.json.parseFromSlice(table_writes.UniqueConstraintIntegrityResult, alloc, resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(table_writes.UniqueConstraintIntegrityAction.repair, parsed.value.action);
    try std.testing.expectEqual(@as(u64, 7), parsed.value.groups[0].group_id);

    const unavailable_bodies = [_][]const u8{
        "{\"action\":\"validate\",\"lower_doc_key\":\"leader\"}",
        "{\"action\":\"validate\",\"lower_doc_key\":\"write\"}",
    };
    for (unavailable_bodies) |body| {
        var unavailable_resp = (try handle(.{
            .alloc = alloc,
            .shard_ops = null,
            .writes = TestWriteSource.source(),
            .batch_validator = TestWriteSource.batchValidator(),
            .txn_validator = TestWriteSource.txnValidator(),
        }, .{
            .method = .POST,
            .uri = "/internal/v1/groups/7/tables/docs/unique-integrity",
            .body = body,
        }, "/internal/v1/groups/7/tables/docs/unique-integrity")).?;
        defer unavailable_resp.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 503), unavailable_resp.status);
        try std.testing.expectEqualStrings("leader unavailable", unavailable_resp.body);
    }
}

test "internal group write routes expose relational column backed index repair" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/relational-column-backed-index-repair",
        .body = "{\"lower_doc_key\":\"doc:a\",\"upper_doc_key\":\"doc:z\"}",
    }, "/internal/v1/groups/7/tables/docs/relational-column-backed-index-repair")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    var parsed = try std.json.parseFromSlice(db_mod.relational_store.ColumnBackedIndexRepairReport, alloc, resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 5), parsed.value.scanned_rows);
    try std.testing.expectEqual(@as(u64, 4), parsed.value.indexed_rows);
    try std.testing.expectEqual(@as(u64, 3), parsed.value.deleted_orphan_entries);
    try std.testing.expectEqual(@as(u64, 2), parsed.value.written_entries);
}

test "internal group write routes expose table emptying" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = null,
        .writes = TestWriteSource.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/table-emptying",
        .body = "{\"record\":{\"job_id\":99,\"table_id\":11,\"group_id\":7,\"schema_generation\":3,\"affected_table_ids\":[11],\"restart_identity\":true,\"cascade\":false,\"state\":\"declared\"},\"worker_id\":\"worker:empty\",\"lease_ms\":1000}",
    }, "/internal/v1/groups/7/tables/docs/table-emptying")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    var parsed = try std.json.parseFromSlice(table_writes.TableEmptyingWorkerResult, alloc, resp.body, .{
        .ignore_unknown_fields = true,
    });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u64, 7), parsed.value.group_id);
    try std.testing.expectEqual(@as(u64, 11), parsed.value.table_id);
    try std.testing.expectEqual(@as(u64, 99), parsed.value.job_id);
    try std.testing.expect(parsed.value.claimed);
    try std.testing.expect(parsed.value.completed);
    try std.testing.expectEqual(@as(u64, 4), parsed.value.result.matched);
    try std.testing.expectEqual(@as(u64, 4), parsed.value.result.staged);
}

test "internal group write routes stage rows mutation source envelopes" {
    const alloc = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"source_id":{"type":"keyword"},"quantity":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const MutationSource = struct {
        const route_topology_epoch: u64 = 4242;

        calls: usize = 0,
        mutation_collect_calls: usize = 0,
        mutation_planned_stage_calls: usize = 0,
        joined_calls: usize = 0,
        planned_stage_calls: usize = 0,
        collect_calls: usize = 0,
        inputs_calls: usize = 0,

        fn source(self: *@This()) table_writes.TableWriteSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .batch = batch,
                    .mutate_rows_from_source = mutateRowsFromSource,
                    .mutate_rows_from_source_autocommit = mutateRowsFromSourceAutocommit,
                    .rows_mutation_source_collect_group_local = rowsMutationSourceCollectGroupLocal,
                    .rows_mutation_source_stage_planned_group_local = rowsMutationSourceStagePlannedGroupLocal,
                    .mutate_rows_joined_from_source_rows = mutateRowsJoinedFromSourceRows,
                    .mutate_rows_joined_from_source_rows_autocommit = mutateRowsJoinedFromSourceRowsAutocommit,
                    .rows_joined_mutation_source_collect_group_local = rowsJoinedMutationSourceCollectGroupLocal,
                    .rows_joined_mutation_source_inputs_group_local = rowsJoinedMutationSourceInputsGroupLocal,
                    .rows_joined_mutation_source_stage_planned_group_local = rowsJoinedMutationSourceStagePlannedGroupLocal,
                },
            };
        }

        fn batch(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db_mod.types.BatchRequest) !?void {
            return null;
        }

        fn mutationResult(alloc_inner: std.mem.Allocator, row: []const u8) !db_mod.types.RelationalRowsMutationSourceResult {
            const returning = try alloc_inner.alloc([]const u8, 1);
            errdefer alloc_inner.free(returning);
            returning[0] = try alloc_inner.dupe(u8, row);
            return .{
                .matched = 2,
                .staged = 1,
                .returning_rows = returning,
            };
        }

        fn mutationParticipantResult(alloc_inner: std.mem.Allocator) !db_mod.types.RelationalRowsMutationSourceResult {
            var result = try mutationResult(alloc_inner, "{\"id\":\"a\",\"status\":\"claimed\"}");
            errdefer result.deinit(alloc_inner);
            const predicates = try alloc_inner.alloc(db_mod.types.TransactionVersionPredicate, 1);
            errdefer alloc_inner.free(predicates);
            predicates[0] = .{
                .key = try alloc_inner.dupe(u8, "row:a"),
                .expected_version = 41,
            };
            const preimages = try alloc_inner.alloc(db_mod.types.TransactionWrite, 1);
            errdefer alloc_inner.free(preimages);
            preimages[0] = .{
                .key = try alloc_inner.dupe(u8, "row:a"),
                .value = try alloc_inner.dupe(u8, "{\"id\":\"a\",\"status\":\"ready\"}"),
            };
            const deletes = try alloc_inner.alloc([]const u8, 1);
            errdefer alloc_inner.free(deletes);
            deletes[0] = try alloc_inner.dupe(u8, "row:old");
            const ops = try alloc_inner.alloc(db_mod.types.TransformOp, 1);
            errdefer alloc_inner.free(ops);
            ops[0] = .{
                .op = .set,
                .path = try alloc_inner.dupe(u8, "status"),
                .value_json = try alloc_inner.dupe(u8, "\"claimed\""),
            };
            const transforms = try alloc_inner.alloc(db_mod.types.DocumentTransform, 1);
            errdefer alloc_inner.free(transforms);
            transforms[0] = .{
                .key = try alloc_inner.dupe(u8, "row:a"),
                .operations = ops,
            };
            result.participant_predicates = predicates;
            result.participant_preimages = preimages;
            result.participant_deletes = deletes;
            result.participant_transforms = transforms;
            return result;
        }

        fn mutateRowsFromSource(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
            schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsMutationSourceRequest,
        ) !?db_mod.types.RelationalRowsMutationSourceResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, req.kind);
            try std.testing.expect(req.source.row_claim != null);
            return try mutationParticipantResult(alloc_inner);
        }

        fn mutateRowsFromSourceAutocommit(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
            schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsMutationSourceRequest,
            sync_level: db_mod.types.SyncLevel,
        ) !?db_mod.types.RelationalRowsMutationSourceResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            switch (sync_level) {
                .propose => return error.NotLeader,
                .full_text => return error.LeaderUnavailable,
                .enrichments => return error.WriteUnavailable,
                else => {},
            }
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, sync_level);
            try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, req.kind);
            try std.testing.expectEqual(@as(usize, 1), req.source.predicates.len);
            try std.testing.expectEqualStrings("status", req.source.predicates[0].field);
            try std.testing.expectEqual(@as(usize, 1), req.operations.len);
            try std.testing.expectEqualStrings("status", req.operations[0].path);
            return try mutationResult(alloc_inner, "{\"id\":\"a\",\"status\":\"claimed\"}");
        }

        fn rowsMutationSourceCollectGroupLocal(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            topology_epoch: u64,
            schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsMutationSourceRequest,
        ) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.mutation_collect_calls += 1;
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            if (topology_epoch == route_topology_epoch + 1) return error.ReadUnavailable;
            try std.testing.expectEqual(route_topology_epoch, topology_epoch);
            try std.testing.expect(schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, req.kind);
            return try alloc_inner.dupe(u8, "{\"candidates\":[{\"doc_key\":\"row:a\",\"json\":{\"id\":\"a\",\"status\":\"ready\"},\"version\":9,\"ordinal\":0,\"group_id\":7,\"order_keys\":[]}]}");
        }

        fn rowsMutationSourceStagePlannedGroupLocal(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            topology_epoch: u64,
            schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsMutationSourceRequest,
            matched: u32,
            candidates: []const db_mod.DB.RelationalRowsMutationSourceCandidate,
        ) !?db_mod.types.RelationalRowsMutationSourceResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.mutation_planned_stage_calls += 1;
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(route_topology_epoch, topology_epoch);
            try std.testing.expect(schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, req.kind);
            try std.testing.expectEqual(@as(u32, 2), matched);
            try std.testing.expectEqual(@as(usize, 1), candidates.len);
            try std.testing.expectEqualStrings("row:a", candidates[0].doc_key);
            try std.testing.expectEqual(@as(u64, 7), candidates[0].group_id);
            return try mutationParticipantResult(alloc_inner);
        }

        fn mutateRowsJoinedFromSourceRowsAutocommit(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
            target_schema: storage_schema.TableSchema,
            source_schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
            source_rows: []const []const u8,
            sync_level: db_mod.types.SyncLevel,
        ) !?db_mod.types.RelationalRowsMutationSourceResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.joined_calls += 1;
            switch (sync_level) {
                .propose => return error.NotLeader,
                .full_text => return error.LeaderUnavailable,
                .enrichments => return error.WriteUnavailable,
                else => {},
            }
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(target_schema.primary_key != null);
            try std.testing.expect(source_schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.SyncLevel.write, sync_level);
            try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, req.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, req.target_side);
            try std.testing.expectEqual(@as(usize, 1), source_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}", source_rows[0]);
            return try mutationResult(alloc_inner, "{\"id\":\"a\",\"status\":\"synced\"}");
        }

        fn mutateRowsJoinedFromSourceRows(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            table_name: []const u8,
            target_schema: storage_schema.TableSchema,
            source_schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
            source_rows: []const []const u8,
        ) !?db_mod.types.RelationalRowsMutationSourceResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.joined_calls += 1;
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(target_schema.primary_key != null);
            try std.testing.expect(source_schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, req.kind);
            try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, req.target_side);
            try std.testing.expectEqual(@as(usize, 1), source_rows.len);
            try std.testing.expectEqualStrings("{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}", source_rows[0]);
            return try mutationParticipantResult(alloc_inner);
        }

        fn rowsJoinedMutationSourceStagePlannedGroupLocal(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            topology_epoch: u64,
            target_schema: storage_schema.TableSchema,
            source_schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
            matched: u32,
            candidates: []const db_mod.DB.RelationalRowsJoinedMutationSourceCandidate,
        ) !?db_mod.types.RelationalRowsMutationSourceResult {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.planned_stage_calls += 1;
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(route_topology_epoch, topology_epoch);
            try std.testing.expect(target_schema.primary_key != null);
            try std.testing.expect(source_schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsMutationKind.update, req.kind);
            try std.testing.expectEqual(@as(u32, 2), matched);
            try std.testing.expectEqual(@as(usize, 1), candidates.len);
            try std.testing.expectEqualStrings("row:a", candidates[0].target.doc_key);
            try std.testing.expectEqual(@as(u64, 7), candidates[0].target.group_id);
            try std.testing.expectEqualStrings("{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}", candidates[0].source_json);
            return try mutationParticipantResult(alloc_inner);
        }

        fn rowsJoinedMutationSourceCollectGroupLocal(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            topology_epoch: u64,
            target_schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        ) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.collect_calls += 1;
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(route_topology_epoch, topology_epoch);
            try std.testing.expect(target_schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, req.target_side);
            return try alloc_inner.dupe(u8, "{\"candidates\":[{\"doc_key\":\"row:a\",\"json\":{\"id\":\"a\"},\"version\":9,\"ordinal\":0,\"group_id\":7,\"order_keys\":[]}]}");
        }

        fn rowsJoinedMutationSourceInputsGroupLocal(
            ptr: *anyopaque,
            alloc_inner: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            topology_epoch: u64,
            source_schema: storage_schema.TableSchema,
            req: db_mod.types.RelationalRowsJoinedMutationSourceRequest,
        ) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.inputs_calls += 1;
            try std.testing.expectEqual(@as(u64, 7), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            if (topology_epoch == route_topology_epoch + 1) return error.ReadUnavailable;
            try std.testing.expectEqual(route_topology_epoch, topology_epoch);
            try std.testing.expect(source_schema.primary_key != null);
            try std.testing.expectEqual(db_mod.types.RelationalRowsJoinProjectionSide.left, req.target_side);
            return try alloc_inner.dupe(u8, "{\"total\":1,\"rows\":[{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}]}");
        }
    };

    var source = MutationSource{};
    const ctx: Context = .{
        .alloc = alloc,
        .shard_ops = null,
        .writes = source.source(),
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    };

    const mutation_body =
        \\{"op":"update","source":{"where":{"field":"status","op":"eq","value":"ready"},"row_claim":{"mode":"for_update","owner_id":"session:mutation","transaction_id":"00112233445566778899aabbccddeeff"}},"patch":{"status":"claimed"},"returning":["id","status"]}
    ;
    const collect_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .schema_json = schema_json,
        .request_body = mutation_body,
        .topology_epoch = MutationSource.route_topology_epoch,
    }, .{})});
    defer alloc.free(collect_body);

    var collect_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/mutation-source/collect",
        .body = collect_body,
    }, "/internal/v1/groups/7/tables/docs/rows/mutation-source/collect")).?;
    defer collect_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), collect_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.mutation_collect_calls);
    try std.testing.expectEqualStrings("{\"candidates\":[{\"doc_key\":\"row:a\",\"json\":{\"id\":\"a\",\"status\":\"ready\"},\"version\":9,\"ordinal\":0,\"group_id\":7,\"order_keys\":[]}]}", collect_resp.body);

    const stage_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .schema_json = schema_json,
        .request_body = mutation_body,
        .sync_level = "write",
    }, .{})});
    defer alloc.free(stage_body);

    var resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/mutation-source/stage",
        .body = stage_body,
    }, "/internal/v1/groups/7/tables/docs/rows/mutation-source/stage")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.calls);
    try std.testing.expectEqualStrings("{\"matched\":2,\"staged\":1,\"returning\":[{\"id\":\"a\",\"status\":\"claimed\"}]}", resp.body);

    const txn_stage_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .schema_json = schema_json,
        .request_body = mutation_body,
        .autocommit = false,
    }, .{})});
    defer alloc.free(txn_stage_body);

    var txn_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/mutation-source/stage",
        .body = txn_stage_body,
    }, "/internal/v1/groups/7/tables/docs/rows/mutation-source/stage")).?;
    defer txn_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), txn_resp.status);
    try std.testing.expectEqual(@as(usize, 2), source.calls);
    var parsed_txn_resp = try relational_rows_api.parseRowsMutationSourceResponseAlloc(alloc, txn_resp.body);
    defer parsed_txn_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), parsed_txn_resp.matched);
    try std.testing.expectEqual(@as(usize, 1), parsed_txn_resp.participant_predicates.len);
    try std.testing.expectEqualStrings("row:a", parsed_txn_resp.participant_predicates[0].key);
    try std.testing.expectEqual(@as(u64, 41), parsed_txn_resp.participant_predicates[0].expected_version);
    try std.testing.expectEqual(@as(usize, 1), parsed_txn_resp.participant_preimages.len);
    try std.testing.expectEqualStrings("{\"id\":\"a\",\"status\":\"ready\"}", parsed_txn_resp.participant_preimages[0].value);
    try std.testing.expectEqual(@as(usize, 1), parsed_txn_resp.participant_deletes.len);
    try std.testing.expectEqualStrings("row:old", parsed_txn_resp.participant_deletes[0]);
    try std.testing.expectEqual(@as(usize, 1), parsed_txn_resp.participant_transforms.len);
    try std.testing.expectEqualStrings("\"claimed\"", parsed_txn_resp.participant_transforms[0].operations[0].value_json.?);

    var planned_mutation_candidates = [_]db_mod.DB.RelationalRowsMutationSourceCandidate{.{
        .doc_key = @constCast("row:a"),
        .json = @constCast("{\"id\":\"a\",\"status\":\"ready\"}"),
        .version = 9,
        .ordinal = 0,
        .group_id = 7,
    }};
    const planned_mutation_stage = try relational_rows_api.encodeRowsMutationSourcePlannedStageAlloc(alloc, 2, planned_mutation_candidates[0..]);
    defer alloc.free(planned_mutation_stage);
    const planned_stage_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .schema_json = schema_json,
        .request_body = mutation_body,
        .topology_epoch = MutationSource.route_topology_epoch,
        .planned_stage = planned_mutation_stage,
        .autocommit = false,
    }, .{})});
    defer alloc.free(planned_stage_body);

    var planned_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/mutation-source/stage",
        .body = planned_stage_body,
    }, "/internal/v1/groups/7/tables/docs/rows/mutation-source/stage")).?;
    defer planned_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), planned_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.mutation_planned_stage_calls);
    var parsed_planned_resp = try relational_rows_api.parseRowsMutationSourceResponseAlloc(alloc, planned_resp.body);
    defer parsed_planned_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), parsed_planned_resp.matched);
    try std.testing.expectEqual(@as(usize, 1), parsed_planned_resp.participant_predicates.len);
    try std.testing.expectEqualStrings("row:a", parsed_planned_resp.participant_predicates[0].key);

    const joined_body =
        \\{"op":"update","target_side":"left","join":{"left":{"where":{"field":"status","op":"eq","value":"ready"},"row_claim":{"mode":"for_update","owner_id":"session:joined","transaction_id":"00112233445566778899aabbccddeeff"}},"right":{"where":{"field":"status","op":"eq","value":"source"}},"on":[{"left_field":"source_id","right_field":"id"}]},"patch":{"status":"synced"},"returning":["id","status"]}
    ;
    const joined_collect_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .target_schema_json = schema_json,
        .source_schema_json = schema_json,
        .request_body = joined_body,
        .topology_epoch = MutationSource.route_topology_epoch,
    }, .{})});
    defer alloc.free(joined_collect_body);

    var joined_collect_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/collect",
        .body = joined_collect_body,
    }, "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/collect")).?;
    defer joined_collect_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), joined_collect_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.collect_calls);
    try std.testing.expectEqualStrings("{\"candidates\":[{\"doc_key\":\"row:a\",\"json\":{\"id\":\"a\"},\"version\":9,\"ordinal\":0,\"group_id\":7,\"order_keys\":[]}]}", joined_collect_resp.body);

    const joined_inputs_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .target_schema_json = schema_json,
        .source_schema_json = schema_json,
        .request_body = joined_body,
        .topology_epoch = MutationSource.route_topology_epoch,
    }, .{})});
    defer alloc.free(joined_inputs_body);

    var joined_inputs_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/inputs",
        .body = joined_inputs_body,
    }, "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/inputs")).?;
    defer joined_inputs_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), joined_inputs_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.inputs_calls);
    try std.testing.expectEqualStrings("{\"total\":1,\"rows\":[{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}]}", joined_inputs_resp.body);

    const joined_stage_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .target_schema_json = schema_json,
        .source_schema_json = schema_json,
        .request_body = joined_body,
        .source_rows = &[_][]const u8{"{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}"},
        .sync_level = "write",
    }, .{})});
    defer alloc.free(joined_stage_body);

    var joined_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage",
        .body = joined_stage_body,
    }, "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage")).?;
    defer joined_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), joined_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.joined_calls);
    try std.testing.expectEqualStrings("{\"matched\":2,\"staged\":1,\"returning\":[{\"id\":\"a\",\"status\":\"synced\"}]}", joined_resp.body);

    const joined_txn_stage_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .target_schema_json = schema_json,
        .source_schema_json = schema_json,
        .request_body = joined_body,
        .source_rows = &[_][]const u8{"{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}"},
        .autocommit = false,
    }, .{})});
    defer alloc.free(joined_txn_stage_body);

    var joined_txn_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage",
        .body = joined_txn_stage_body,
    }, "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage")).?;
    defer joined_txn_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), joined_txn_resp.status);
    try std.testing.expectEqual(@as(usize, 2), source.joined_calls);
    var parsed_joined_txn_resp = try relational_rows_api.parseRowsMutationSourceResponseAlloc(alloc, joined_txn_resp.body);
    defer parsed_joined_txn_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), parsed_joined_txn_resp.matched);
    try std.testing.expectEqual(@as(usize, 1), parsed_joined_txn_resp.participant_predicates.len);
    try std.testing.expectEqualStrings("row:a", parsed_joined_txn_resp.participant_predicates[0].key);

    var planned_candidates = [_]db_mod.DB.RelationalRowsJoinedMutationSourceCandidate{.{
        .target = .{
            .doc_key = @constCast("row:a"),
            .json = @constCast("{\"id\":\"a\"}"),
            .version = 9,
            .ordinal = 0,
            .group_id = 7,
        },
        .source_json = @constCast("{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}"),
    }};
    const planned_stage = try relational_rows_api.encodeRowsJoinedMutationSourcePlannedStageAlloc(alloc, 2, planned_candidates[0..]);
    defer alloc.free(planned_stage);
    const joined_planned_stage_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .target_schema_json = schema_json,
        .source_schema_json = schema_json,
        .request_body = joined_body,
        .topology_epoch = MutationSource.route_topology_epoch,
        .planned_stage = planned_stage,
        .autocommit = false,
    }, .{})});
    defer alloc.free(joined_planned_stage_body);

    var joined_planned_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage",
        .body = joined_planned_stage_body,
    }, "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage")).?;
    defer joined_planned_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 200), joined_planned_resp.status);
    try std.testing.expectEqual(@as(usize, 1), source.planned_stage_calls);
    var parsed_joined_planned_resp = try relational_rows_api.parseRowsMutationSourceResponseAlloc(alloc, joined_planned_resp.body);
    defer parsed_joined_planned_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), parsed_joined_planned_resp.matched);
    try std.testing.expectEqual(@as(usize, 1), parsed_joined_planned_resp.participant_predicates.len);
    try std.testing.expectEqualStrings("row:a", parsed_joined_planned_resp.participant_predicates[0].key);

    const read_unavailable_collect_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .schema_json = schema_json,
        .request_body = mutation_body,
        .topology_epoch = MutationSource.route_topology_epoch + 1,
    }, .{})});
    defer alloc.free(read_unavailable_collect_body);

    var read_unavailable_collect_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/mutation-source/collect",
        .body = read_unavailable_collect_body,
    }, "/internal/v1/groups/7/tables/docs/rows/mutation-source/collect")).?;
    defer read_unavailable_collect_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 503), read_unavailable_collect_resp.status);
    try std.testing.expectEqualStrings("leader unavailable", read_unavailable_collect_resp.body);

    const read_unavailable_inputs_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
        .target_schema_json = schema_json,
        .source_schema_json = schema_json,
        .request_body = joined_body,
        .topology_epoch = MutationSource.route_topology_epoch + 1,
    }, .{})});
    defer alloc.free(read_unavailable_inputs_body);

    var read_unavailable_inputs_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/inputs",
        .body = read_unavailable_inputs_body,
    }, "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/inputs")).?;
    defer read_unavailable_inputs_resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 503), read_unavailable_inputs_resp.status);
    try std.testing.expectEqualStrings("leader unavailable", read_unavailable_inputs_resp.body);

    const UnavailableCase = struct {
        sync_level: []const u8,
    };
    const unavailable_cases = [_]UnavailableCase{
        .{ .sync_level = "propose" },
        .{ .sync_level = "query" },
        .{ .sync_level = "enrichments" },
    };
    for (unavailable_cases) |case| {
        const unavailable_stage_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
            .schema_json = schema_json,
            .request_body = mutation_body,
            .sync_level = case.sync_level,
        }, .{})});
        defer alloc.free(unavailable_stage_body);

        var unavailable_resp = (try handle(ctx, .{
            .method = .POST,
            .uri = "/internal/v1/groups/7/tables/docs/rows/mutation-source/stage",
            .body = unavailable_stage_body,
        }, "/internal/v1/groups/7/tables/docs/rows/mutation-source/stage")).?;
        defer unavailable_resp.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 503), unavailable_resp.status);
        try std.testing.expectEqualStrings("leader unavailable", unavailable_resp.body);

        const joined_unavailable_stage_body = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(.{
            .target_schema_json = schema_json,
            .source_schema_json = schema_json,
            .request_body = joined_body,
            .source_rows = &[_][]const u8{"{\"id\":\"src:1\",\"status\":\"source\",\"quantity\":3}"},
            .sync_level = case.sync_level,
        }, .{})});
        defer alloc.free(joined_unavailable_stage_body);

        var joined_unavailable_resp = (try handle(ctx, .{
            .method = .POST,
            .uri = "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage",
            .body = joined_unavailable_stage_body,
        }, "/internal/v1/groups/7/tables/docs/rows/joined-mutation-source/stage")).?;
        defer joined_unavailable_resp.deinit(alloc);

        try std.testing.expectEqual(@as(u16, 503), joined_unavailable_resp.status);
        try std.testing.expectEqualStrings("leader unavailable", joined_unavailable_resp.body);
    }
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

test "internal group write routes reject mismatched shard execute requests" {
    const alloc = std.testing.allocator;

    var resp = (try handle(.{
        .alloc = alloc,
        .shard_ops = TestShardOps.adapter(),
        .writes = null,
        .batch_validator = TestWriteSource.batchValidator(),
        .txn_validator = TestWriteSource.txnValidator(),
    }, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/shard-ops/execute",
        .body = "{\"kind\":\"prepare_split_source\",\"transition_id\":1,\"source_group_id\":8,\"destination_group_id\":9,\"split_key\":\"doc:m\"}",
    }, "/internal/v1/groups/7/shard-ops/execute")).?;
    defer resp.deinit(alloc);

    try std.testing.expectEqual(@as(u16, 400), resp.status);
    try std.testing.expectEqualStrings("group does not match transition action", resp.body);
}

test "internal group write routes allow source-hosted split destination actions" {
    const action = metadata_mod.TransitionAction{ .bootstrap_split_destination = .{
        .transition_id = 1,
        .source_group_id = 7,
        .destination_group_id = 8,
    } };
    try std.testing.expect(transitionActionMatchesRouteGroup(action, 7));
    try std.testing.expect(transitionActionMatchesRouteGroup(action, 8));
    try std.testing.expect(!transitionActionMatchesRouteGroup(action, 9));
}

test "internal group write routes parse merge doc identity reassignment action flag" {
    const alloc = std.testing.allocator;
    var action = try parseTransitionAction(alloc,
        \\{"kind":"catch_up_merge_receiver","transition_id":4,"donor_group_id":10,"receiver_group_id":9,"allow_doc_identity_reassignment":true}
    );
    defer freeTransitionActionOwned(alloc, &action);

    try std.testing.expect(action == .catch_up_merge_receiver);
    try std.testing.expect(action.catch_up_merge_receiver.allow_doc_identity_reassignment);
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

        fn observeSplit(_: *anyopaque, _: metadata_transition_state.SplitTransitionRecord) !metadata_transition_state.SplitObservation {
            return error.DocIdentityNamespaceMismatch;
        }

        fn observeMerge(_: *anyopaque, _: metadata_transition_state.MergeTransitionRecord) !metadata_transition_state.MergeObservation {
            return error.DocIdentityNamespaceMismatch;
        }

        fn prepareSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .prepare_split_source).type) !void {
            unreachable;
        }

        fn startSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .start_split_source).type) !void {
            unreachable;
        }

        fn bootstrapSplitDestination(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .bootstrap_split_destination).type) !void {
            unreachable;
        }

        fn catchUpSplitDestination(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .catch_up_split_destination).type) !void {
            unreachable;
        }

        fn finalizeSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .finalize_split_source).type) !void {
            unreachable;
        }

        fn rollbackSplit(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .rollback_split).type) !void {
            unreachable;
        }

        fn acceptMergeReceiver(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .accept_merge_receiver).type) !void {
            unreachable;
        }

        fn catchUpMergeReceiver(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .catch_up_merge_receiver).type) !void {
            unreachable;
        }

        fn finalizeMerge(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .finalize_merge).type) !void {
            return error.DocIdentityNamespaceMismatch;
        }

        fn rollbackMerge(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .rollback_merge).type) !void {
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
        .body = "{\"transition_id\":1,\"source_group_id\":7,\"destination_group_id\":8,\"split_key\":\"doc:m\"}",
    }, "/internal/v1/groups/7/shard-ops/observe-split")).?;
    defer split_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), split_resp.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", split_resp.body);

    var merge_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/shard-ops/observe-merge",
        .body = "{\"transition_id\":2,\"donor_group_id\":8,\"receiver_group_id\":7}",
    }, "/internal/v1/groups/7/shard-ops/observe-merge")).?;
    defer merge_resp.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), merge_resp.status);
    try std.testing.expectEqualStrings("doc identity namespace mismatch", merge_resp.body);

    var execute_resp = (try handle(ctx, .{
        .method = .POST,
        .uri = "/internal/v1/groups/7/shard-ops/execute",
        .body = "{\"kind\":\"finalize_merge\",\"transition_id\":3,\"donor_group_id\":8,\"receiver_group_id\":7,\"allow_doc_identity_reassignment\":true}",
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
                .foreign_key_integrity_group_local = foreignKeyIntegrityGroupLocal,
                .foreign_key_integrity_work_unit_group_local = foreignKeyIntegrityWorkUnitGroupLocal,
                .foreign_key_action_job_group_local = foreignKeyActionJobGroupLocal,
                .foreign_key_action_job_group_local_schedule = foreignKeyActionJobGroupLocalSchedule,
                .foreign_key_action_job_group_local_requeue = foreignKeyActionJobGroupLocalRequeue,
                .unique_constraint_integrity_group_local = uniqueConstraintIntegrityGroupLocal,
                .relational_column_backed_index_repair_group_local = relationalColumnBackedIndexRepairGroupLocal,
                .table_emptying_group_local = tableEmptyingGroupLocal,
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

    fn foreignKeyIntegrityGroupLocal(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        _: []const u8,
        action: table_writes.ForeignKeyIntegrityAction,
        _: ?[]const u8,
        _: []const u8,
        _: []const u8,
        violation_limit: usize,
    ) !?table_writes.ForeignKeyIntegrityResult {
        const groups = try alloc.alloc(table_writes.ForeignKeyIntegrityGroupReport, 1);
        groups[0] = .{ .group_id = group_id, .report = .{} };
        return .{
            .action = action,
            .valid = true,
            .complete = true,
            .violation_limit = violation_limit,
            .violations_truncated = false,
            .report = .{},
            .groups = groups,
            .violations = &.{},
        };
    }

    fn foreignKeyIntegrityWorkUnitGroupLocal(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        _: []const u8,
        action: table_writes.ForeignKeyIntegrityAction,
        phase: []const u8,
        job_id: ?[]const u8,
        claim_key: []const u8,
        worker_id: []const u8,
        lease_ms: u64,
        max_work_units: usize,
        _: ?[]const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
        violation_limit: usize,
    ) !?table_writes.ForeignKeyIntegrityResult {
        const groups = try alloc.alloc(table_writes.ForeignKeyIntegrityGroupReport, 1);
        errdefer alloc.free(groups);
        groups[0] = .{ .group_id = group_id, .report = .{} };

        const work_claims = try alloc.alloc(table_writes.ForeignKeyIntegrityWorkClaim, 1);
        errdefer alloc.free(work_claims);
        work_claims[0] = .{
            .claim_key = try alloc.dupe(u8, claim_key),
            .worker_id = &.{},
            .group_id = group_id,
            .phase = try alloc.dupe(u8, phase),
            .planned_action = try alloc.dupe(u8, @tagName(action)),
            .constraint_name = null,
            .lower_doc_key = try alloc.dupe(u8, lower_doc_key),
            .upper_doc_key = try alloc.dupe(u8, upper_doc_key),
            .claimed_at_ns = 1,
            .lease_until_ns = 1 + lease_ms * std.time.ns_per_ms,
            .attempts = @intCast(max_work_units),
        };
        errdefer {
            if (work_claims[0].claim_key.len > 0) alloc.free(work_claims[0].claim_key);
            if (work_claims[0].worker_id.len > 0) alloc.free(work_claims[0].worker_id);
            if (work_claims[0].phase.len > 0) alloc.free(work_claims[0].phase);
            if (work_claims[0].planned_action.len > 0) alloc.free(work_claims[0].planned_action);
            if (work_claims[0].constraint_name) |value| alloc.free(value);
            if (work_claims[0].lower_doc_key.len > 0) alloc.free(work_claims[0].lower_doc_key);
            if (work_claims[0].upper_doc_key.len > 0) alloc.free(work_claims[0].upper_doc_key);
        }
        work_claims[0].worker_id = try alloc.dupe(u8, worker_id);

        return .{
            .action = action,
            .valid = true,
            .complete = job_id != null,
            .violation_limit = violation_limit,
            .violations_truncated = false,
            .report = .{},
            .groups = groups,
            .work_claims = work_claims,
            .violations = &.{},
        };
    }

    fn testForeignKeyActionJobStatus(
        alloc: std.mem.Allocator,
        group_id: u64,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        status: []const u8,
    ) !table_writes.ForeignKeyActionJobStatus {
        return .{
            .group_id = group_id,
            .job_id = try alloc.dupe(u8, job_id),
            .action = try alloc.dupe(u8, action),
            .worker_id = try alloc.dupe(u8, worker_id),
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .parent_table = try alloc.dupe(u8, parent_table),
            .parent_key = try alloc.dupe(u8, parent_key),
            .updated_parent_key = if (updated_parent_key) |value| try alloc.dupe(u8, value) else null,
            .page_limit = page_limit,
            .status = try alloc.dupe(u8, status),
            .created_at_ns = 1,
            .updated_at_ns = 2,
            .claimed_at_ns = if (std.mem.eql(u8, status, "claimed")) 2 else 0,
            .lease_until_ns = if (std.mem.eql(u8, status, "claimed")) 3 else 0,
            .attempts = if (std.mem.eql(u8, status, "claimed")) 2 else 1,
            .completed = false,
            .applied_children = if (std.mem.eql(u8, status, "pending")) 5 else 0,
            .failure_count = if (std.mem.eql(u8, status, "pending")) 3 else 0,
            .first_failed_at_ns = if (std.mem.eql(u8, status, "pending")) 101 else null,
            .last_failed_at_ns = if (std.mem.eql(u8, status, "pending")) 202 else null,
            .requeue_count = if (std.mem.eql(u8, status, "pending")) 2 else 0,
            .last_requeued_at_ns = if (std.mem.eql(u8, status, "pending")) 303 else null,
            .cascade_depth = if (std.mem.eql(u8, status, "pending")) 4 else 0,
            .cascade_max_depth = if (std.mem.eql(u8, status, "pending")) 9 else 64,
            .next_child_table = if (std.mem.eql(u8, status, "pending")) try alloc.dupe(u8, "row") else null,
            .next_child_key = if (std.mem.eql(u8, status, "pending")) try alloc.dupe(u8, "order:cursor") else null,
            .last_error = null,
        };
    }

    fn foreignKeyActionJobGroupLocal(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        _: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        _: u64,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?table_writes.ForeignKeyActionJobStatus {
        var status = try testForeignKeyActionJobStatus(alloc, group_id, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, "claimed");
        status.cascade_depth = cascade_depth;
        status.cascade_max_depth = cascade_max_depth;
        return status;
    }

    fn foreignKeyActionJobGroupLocalSchedule(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        _: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
        cascade_depth: u32,
        cascade_max_depth: u32,
    ) !?table_writes.ForeignKeyActionJobStatus {
        var status = try testForeignKeyActionJobStatus(alloc, group_id, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, "scheduled");
        status.cascade_depth = cascade_depth;
        status.cascade_max_depth = cascade_max_depth;
        return status;
    }

    fn foreignKeyActionJobGroupLocalRequeue(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        _: []const u8,
        job_id: []const u8,
        action: []const u8,
        worker_id: []const u8,
        constraint_name: []const u8,
        parent_table: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        page_limit: usize,
    ) !?table_writes.ForeignKeyActionJobStatus {
        return try testForeignKeyActionJobStatus(alloc, group_id, job_id, action, worker_id, constraint_name, parent_table, parent_key, updated_parent_key, page_limit, "pending");
    }

    fn uniqueConstraintIntegrityGroupLocal(
        _: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        _: []const u8,
        action: table_writes.UniqueConstraintIntegrityAction,
        lower_doc_key: []const u8,
        _: []const u8,
    ) !?table_writes.UniqueConstraintIntegrityResult {
        if (std.mem.eql(u8, lower_doc_key, "leader")) return error.LeaderUnavailable;
        if (std.mem.eql(u8, lower_doc_key, "write")) return error.WriteUnavailable;
        const groups = try alloc.alloc(table_writes.UniqueConstraintIntegrityGroupReport, 1);
        groups[0] = .{ .group_id = group_id, .report = .{} };
        return .{
            .action = action,
            .valid = true,
            .complete = true,
            .report = .{},
            .groups = groups,
        };
    }

    fn relationalColumnBackedIndexRepairGroupLocal(
        _: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        lower_doc_key: []const u8,
        upper_doc_key: []const u8,
    ) !?db_mod.relational_store.ColumnBackedIndexRepairReport {
        try std.testing.expectEqual(@as(u64, 7), group_id);
        try std.testing.expectEqualStrings("docs", table_name);
        try std.testing.expectEqualStrings("doc:a", lower_doc_key);
        try std.testing.expectEqualStrings("doc:z", upper_doc_key);
        return .{
            .scanned_rows = 5,
            .indexed_rows = 4,
            .deleted_orphan_entries = 3,
            .written_entries = 2,
        };
    }

    fn tableEmptyingGroupLocal(
        _: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        _: []const u8,
        record: metadata_table_manager.TableEmptyingJobRecord,
        worker_id: []const u8,
        lease_ms: u64,
    ) !?table_writes.TableEmptyingWorkerResult {
        if (worker_id.len == 0 or lease_ms == 0) return error.InvalidTableEmptyingJobLease;
        if (record.group_id != group_id) return error.InvalidTableEmptyingJob;
        return .{
            .group_id = group_id,
            .table_id = record.table_id,
            .job_id = record.job_id,
            .claimed = true,
            .completed = true,
            .result = .{
                .matched = 4,
                .staged = 4,
            },
        };
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

    fn observeSplit(_: *anyopaque, _: metadata_transition_state.SplitTransitionRecord) !metadata_transition_state.SplitObservation {
        unreachable;
    }

    fn observeMerge(_: *anyopaque, _: metadata_transition_state.MergeTransitionRecord) !metadata_transition_state.MergeObservation {
        unreachable;
    }

    fn prepareSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .prepare_split_source).type) !void {
        unreachable;
    }

    fn startSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .start_split_source).type) !void {
        unreachable;
    }

    fn bootstrapSplitDestination(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .bootstrap_split_destination).type) !void {
        unreachable;
    }

    fn catchUpSplitDestination(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .catch_up_split_destination).type) !void {
        unreachable;
    }

    fn finalizeSplitSource(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .finalize_split_source).type) !void {
        unreachable;
    }

    fn rollbackSplit(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .rollback_split).type) !void {
        unreachable;
    }

    fn acceptMergeReceiver(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .accept_merge_receiver).type) !void {
        unreachable;
    }

    fn catchUpMergeReceiver(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .catch_up_merge_receiver).type) !void {
        unreachable;
    }

    fn finalizeMerge(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .finalize_merge).type) !void {
        unreachable;
    }

    fn rollbackMerge(_: *anyopaque, _: std.meta.fieldInfo(metadata_mod.TransitionAction, .rollback_merge).type) !void {
        unreachable;
    }
};

fn freeTransitionActionOwned(alloc: std.mem.Allocator, action: *metadata_mod.TransitionAction) void {
    switch (action.*) {
        .prepare_split_source => |op| {
            alloc.free(op.split_key);
            if (op.source_range_end) |value| alloc.free(value);
        },
        else => {},
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
