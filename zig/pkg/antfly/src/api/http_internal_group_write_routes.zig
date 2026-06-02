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
const http_common = @import("../raft/transport/http_common.zig");
const http_route_helpers = @import("http_route_helpers.zig");
const metadata_mod = @import("../metadata/mod.zig");
const metadata_transition_state = @import("../metadata/transition_state.zig");
const metadata_openapi = @import("antfly_metadata_openapi");
const raft_mod = @import("../raft/mod.zig");
const routes = @import("http_routes.zig");
const table_writes = @import("table_writes.zig");

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
    batch_validator: BatchValidator,
    txn_validator: TxnValidator,
};

const CorruptEmbeddingArtifactRequest = struct {
    doc_key: []const u8,
    index_name: []const u8,
};

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
            error.InvalidBatchRequest => return try http_route_helpers.textResponse(ctx.alloc, 400, "invalid batch request"),
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
