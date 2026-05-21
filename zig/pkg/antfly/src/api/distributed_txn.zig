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
const db_mod = @import("../storage/db/mod.zig");
const transactions_mod = @import("../storage/transactions.zig");
const tracing = @import("../tracing/antfly_trace_writer.zig");
const http_common = @import("../raft/transport/http_common.zig");
const http_client_mod = @import("http_client.zig");
const table_catalog = @import("table_catalog.zig");
const table_router = @import("table_router.zig");
const table_writes = @import("table_writes.zig");

pub const table_participant_prefix = "table:";
const table_participant_v2_prefix = "table2:";
pub const group_participant_marker = ":group:";

pub const TxnBeginRequest = struct {
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    topology_epoch: u64 = 0,
    participants: []const []const u8,
};

pub const TxnPrepareRequest = struct {
    txn_id: db_mod.types.TxnId,
    topology_epoch: u64 = 0,
    req: db_mod.types.TransactionIntentRequest,
};

pub const TxnResolveRequest = struct {
    txn_id: db_mod.types.TxnId,
    status: db_mod.types.TxnStatus,
    commit_version: u64,
};

pub const TxnStatusResponse = struct {
    status: db_mod.types.TxnStatus,
};

pub const TableCommitRequest = struct {
    table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite = &.{},
    deletes: []const []const u8 = &.{},
    transforms: []const db_mod.types.DocumentTransform = &.{},
    predicates: []const db_mod.types.TransactionVersionPredicate = &.{},
};

pub const CommitConflict = struct {
    table_name: []const u8,
    key: []const u8,
    message: []const u8,
    group_id: ?u64 = null,
    phase: ?ParticipantPhase = null,
};

pub const ParticipantPhase = enum {
    begin,
    prepare,
    resolve,
};

pub const CommitOutcome = union(enum) {
    committed: ExecuteResult,
    conflict: CommitConflict,
};

pub const ParticipantWorker = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        begin_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnBeginRequest,
        ) anyerror!void,
        prepare_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnPrepareRequest,
        ) anyerror!void,
        resolve_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: TxnResolveRequest,
        ) anyerror!void,
        status_group: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            txn_id: db_mod.types.TxnId,
        ) anyerror!db_mod.types.TxnStatus,
    };

    pub fn beginGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
        try self.vtable.begin_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn prepareGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
        try self.vtable.prepare_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn resolveGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
        try self.vtable.resolve_group(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn statusGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        return try self.vtable.status_group(self.ptr, alloc, group_id, table_name, txn_id);
    }
};

pub const RecoveryResolver = struct {
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    owner_id: []const u8 = "api",
    lease_owned: bool = false,
    interval_ms: u64 = 10,
    cutoff_ns: u64 = 5 * std.time.ns_per_min,

    pub fn config(self: *const RecoveryResolver) db_mod.transaction_runtime.Config {
        return .{
            .enabled = true,
            .lease_owned = self.lease_owned,
            .owner_id = self.owner_id,
            .interval_ms = self.interval_ms,
            .cutoff_ns = self.cutoff_ns,
            .resolver_ctx = @constCast(self),
            .resolve_participant_fn = resolve,
        };
    }

    fn resolve(
        ctx_ptr: *anyopaque,
        txn_id: db_mod.types.TxnId,
        participant: []const u8,
        status: db_mod.types.TxnStatus,
        commit_version: u64,
    ) !void {
        const self: *RecoveryResolver = @ptrCast(@alignCast(ctx_ptr));
        try resolveParticipant(self.alloc, self.worker, participant, txn_id, status, commit_version);
    }
};

pub const HostedParticipantWorker = struct {
    catalog: table_catalog.CatalogSource,
    router: table_router.HostedGroupRouter,
    writes: table_writes.TableWriteSource,
    executor: http_common.RequestExecutor,

    pub fn init(
        catalog: table_catalog.CatalogSource,
        router: table_router.HostedGroupRouter,
        writes: table_writes.TableWriteSource,
        executor: http_common.RequestExecutor,
    ) HostedParticipantWorker {
        return .{
            .catalog = catalog,
            .router = router,
            .writes = writes,
            .executor = executor,
        };
    }

    pub fn worker(self: *HostedParticipantWorker) ParticipantWorker {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_group = beginGroup,
                .prepare_group = prepareGroup,
                .resolve_group = resolveGroup,
                .status_group = statusGroup,
            },
        };
    }

    fn beginGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        switch (route) {
            .local => _ = (try self.writes.txnBeginGroupLocal(alloc, group_id, table_name, req.txn_id, req.begin_timestamp, req.topology_epoch, req.participants)) orelse return error.UnknownGroup,
            .remote => |remote| {
                var client = http_client_mod.ApiHttpClient.init(alloc, self.executor);
                const body = try encodeTxnBeginRequest(alloc, req);
                defer alloc.free(body);
                var response = try client.fetchGroupTxnBegin(remote.base_uri, group_id, table_name, body);
                response.deinit(alloc);
            },
        }
    }

    fn prepareGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        switch (route) {
            .local => _ = (try self.writes.txnPrepareGroupLocal(alloc, group_id, table_name, req.txn_id, req.topology_epoch, req.req)) orelse return error.UnknownGroup,
            .remote => |remote| {
                var client = http_client_mod.ApiHttpClient.init(alloc, self.executor);
                const body = try encodeTxnPrepareRequest(alloc, req);
                defer alloc.free(body);
                var response = try client.fetchGroupTxnPrepare(remote.base_uri, group_id, table_name, body);
                response.deinit(alloc);
            },
        }
    }

    fn resolveGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        switch (route) {
            .local => _ = (try self.writes.txnResolveGroupLocal(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version)) orelse return error.UnknownGroup,
            .remote => |remote| {
                var client = http_client_mod.ApiHttpClient.init(alloc, self.executor);
                const body = try encodeTxnResolveRequest(alloc, req);
                defer alloc.free(body);
                var response = try client.fetchGroupTxnResolve(remote.base_uri, group_id, table_name, body);
                response.deinit(alloc);
            },
        }
    }

    fn statusGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        return switch (route) {
            .local => (try self.writes.txnStatusGroupLocal(alloc, group_id, table_name, txn_id)) orelse error.UnknownGroup,
            .remote => |remote| blk: {
                var client = http_client_mod.ApiHttpClient.init(alloc, self.executor);
                const body = try encodeTxnStatusRequest(alloc, txn_id);
                defer alloc.free(body);
                var response = try client.fetchGroupTxnStatus(remote.base_uri, group_id, table_name, body);
                defer response.deinit(alloc);
                const parsed = try parseTxnStatusResponse(alloc, response.body);
                break :blk parsed.status;
            },
        };
    }
};

pub const LocalTableWriteParticipantWorker = struct {
    writes: table_writes.TableWriteSource,

    pub fn init(writes: table_writes.TableWriteSource) LocalTableWriteParticipantWorker {
        return .{ .writes = writes };
    }

    pub fn worker(self: *LocalTableWriteParticipantWorker) ParticipantWorker {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_group = beginGroup,
                .prepare_group = prepareGroup,
                .resolve_group = resolveGroup,
                .status_group = statusGroup,
            },
        };
    }

    fn beginGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try self.writes.txnBeginGroupLocal(alloc, group_id, table_name, req.txn_id, req.begin_timestamp, req.topology_epoch, req.participants)) orelse return error.UnknownGroup;
    }

    fn prepareGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try self.writes.txnPrepareGroupLocal(alloc, group_id, table_name, req.txn_id, req.topology_epoch, req.req)) orelse return error.UnknownGroup;
    }

    fn resolveGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        _ = (try self.writes.txnResolveGroupLocal(alloc, group_id, table_name, req.txn_id, req.status, req.commit_version)) orelse return error.UnknownGroup;
    }

    fn statusGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, txn_id: db_mod.types.TxnId) !db_mod.types.TxnStatus {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        return (try self.writes.txnStatusGroupLocal(alloc, group_id, table_name, txn_id)) orelse error.UnknownGroup;
    }
};

pub const ExecuteResult = struct {
    participant_count: usize,
};

pub fn executeCrossGroup(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    table_name: []const u8,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    req: db_mod.types.TransactionIntentRequest,
    trace_writer: ?tracing.AntflyTraceWriter,
) !ExecuteResult {
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);
    var grouped = std.ArrayListUnmanaged(GroupTxn).empty;
    defer {
        for (grouped.items) |*group| group.deinit(alloc);
        grouped.deinit(alloc);
    }

    for (req.writes) |write| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, write.key, topology_epoch)) orelse return error.UnknownGroup;
        const group = try ensureGroupTxn(alloc, &grouped, group_id);
        try group.writes.append(alloc, write);
    }
    for (req.deletes) |key| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, key, topology_epoch)) orelse return error.UnknownGroup;
        const group = try ensureGroupTxn(alloc, &grouped, group_id);
        try group.deletes.append(alloc, key);
    }
    for (req.predicates) |predicate| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, predicate.key, topology_epoch)) orelse return error.UnknownGroup;
        const group = try ensureGroupTxn(alloc, &grouped, group_id);
        try group.predicates.append(alloc, predicate);
    }
    for (req.transforms) |transform| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, transform.key, topology_epoch)) orelse return error.UnknownGroup;
        const group = try ensureGroupTxn(alloc, &grouped, group_id);
        try group.transforms.append(alloc, transform);
    }

    var participants = try alloc.alloc([]const u8, grouped.items.len);
    defer {
        for (participants) |participant| alloc.free(@constCast(participant));
        alloc.free(participants);
    }
    for (grouped.items, 0..) |group, i| {
        participants[i] = try participantIdForGroup(alloc, table_name, group.group_id);
    }

    var begun = std.ArrayListUnmanaged(u64).empty;
    defer begun.deinit(alloc);

    errdefer {
        if (trace_writer) |tw| {
            tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
        }
        abortBegunParticipants(alloc, worker, table_name, txn_id, commit_version, begun.items) catch {};
    }

    for (grouped.items) |group| {
        try worker.beginGroup(alloc, group.group_id, table_name, .{
            .txn_id = txn_id,
            .begin_timestamp = begin_timestamp,
            .participants = participants,
        });
        try begun.append(alloc, group.group_id);
    }

    for (grouped.items) |group| {
        try worker.prepareGroup(alloc, group.group_id, table_name, .{
            .txn_id = txn_id,
            .req = .{
                .writes = group.writes.items,
                .deletes = group.deletes.items,
                .transforms = group.transforms.items,
                .predicates = group.predicates.items,
            },
        });
    }

    for (grouped.items) |group| {
        try worker.resolveGroup(alloc, group.group_id, table_name, .{
            .txn_id = txn_id,
            .status = .committed,
            .commit_version = commit_version,
        });
    }

    if (trace_writer) |tw| {
        tw.traceEvent(&.{ .name = "CommitTransaction", .txn_id = txn_id, .shard_id = "", .timestamp = commit_version });
    }

    return .{ .participant_count = grouped.items.len };
}

pub fn executeMultiTableCommit(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    tables: []const TableCommitRequest,
    trace_writer: ?tracing.AntflyTraceWriter,
) !CommitOutcome {
    var attempt: usize = 0;
    while (attempt < 2) : (attempt += 1) {
        return executeMultiTableCommitOnce(alloc, catalog, worker, txn_id, begin_timestamp, commit_version, tables, attempt > 0, trace_writer) catch |err| switch (err) {
            error.TopologyChanged, error.UnknownGroup => if (attempt == 0) continue else return err,
            else => return err,
        };
    }
    unreachable;
}

fn executeMultiTableCommitOnce(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    tables: []const TableCommitRequest,
    surface_unavailable_conflict: bool,
    trace_writer: ?tracing.AntflyTraceWriter,
) !CommitOutcome {
    var participants = std.ArrayListUnmanaged(ParticipantTxn).empty;
    defer {
        for (participants.items) |*participant| participant.deinit(alloc);
        participants.deinit(alloc);
    }

    for (tables) |table| {
        const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table.table_name);
        if (topology_epoch == 0) return error.TableNotFound;

        for (table.writes) |write| {
            const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table.table_name, write.key, topology_epoch)) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.writes.append(alloc, write);
        }
        for (table.deletes) |key| {
            const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table.table_name, key, topology_epoch)) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.deletes.append(alloc, key);
        }
        for (table.predicates) |predicate| {
            const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table.table_name, predicate.key, topology_epoch)) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.predicates.append(alloc, predicate);
        }
        for (table.transforms) |transform| {
            const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table.table_name, transform.key, topology_epoch)) orelse return error.UnknownGroup;
            const participant = try ensureParticipantTxn(alloc, &participants, table.table_name, group_id, topology_epoch);
            try participant.transforms.append(alloc, transform);
        }
    }

    const participant_ids = try alloc.alloc([]const u8, participants.items.len);
    defer {
        for (participant_ids) |participant_id| alloc.free(@constCast(participant_id));
        alloc.free(participant_ids);
    }
    for (participants.items, 0..) |participant, i| {
        participant_ids[i] = try participantIdForGroup(alloc, participant.table_name, participant.group_id);
    }

    var begun = std.ArrayListUnmanaged(ParticipantRef).empty;
    defer begun.deinit(alloc);

    errdefer {
        if (trace_writer) |tw| {
            tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
        }
        abortBegunRefs(alloc, worker, txn_id, commit_version, begun.items) catch {};
    }

    for (participants.items) |participant| {
        worker.beginGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .begin_timestamp = begin_timestamp,
            .topology_epoch = participant.topology_epoch,
            .participants = participant_ids,
        }) catch |err| switch (err) {
            error.UnknownGroup => {
                if (surface_unavailable_conflict) {
                    return .{ .conflict = try participantUnavailableConflict(alloc, participant, .begin) };
                }
                return err;
            },
            else => return err,
        };
        try begun.append(alloc, .{ .table_name = participant.table_name, .group_id = participant.group_id });
    }

    for (participants.items) |participant| {
        worker.prepareGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .topology_epoch = participant.topology_epoch,
            .req = .{
                .writes = participant.writes.items,
                .deletes = participant.deletes.items,
                .transforms = participant.transforms.items,
                .predicates = participant.predicates.items,
            },
        }) catch |err| switch (err) {
            error.IntentConflict, error.VersionConflict => {
                if (trace_writer) |tw| {
                    tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
                }
                try abortBegunRefs(alloc, worker, txn_id, commit_version, begun.items);
                return .{ .conflict = participantConflict(participant) };
            },
            error.UnknownGroup => {
                if (surface_unavailable_conflict) {
                    if (trace_writer) |tw| {
                        tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
                    }
                    try abortBegunRefs(alloc, worker, txn_id, commit_version, begun.items);
                    return .{ .conflict = try participantUnavailableConflict(alloc, participant, .prepare) };
                }
                return err;
            },
            else => return err,
        };
    }

    for (participants.items) |participant| {
        worker.resolveGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .status = .committed,
            .commit_version = commit_version,
        }) catch |err| switch (err) {
            error.DecisionConflict => {
                if (trace_writer) |tw| {
                    tw.traceEvent(&.{
                        .name = "ResolveDecisionConflict",
                        .txn_id = txn_id,
                        .shard_id = "",
                        .timestamp = commit_version,
                        .reason = "participant decision conflict",
                    });
                }
                return .{ .conflict = participantDecisionConflict(participant, .resolve) };
            },
            error.TxnNotFound, error.InvalidTxnRecord => {
                if (trace_writer) |tw| {
                    tw.traceEvent(&.{
                        .name = "ResolveTornTransactionState",
                        .txn_id = txn_id,
                        .shard_id = "",
                        .timestamp = commit_version,
                        .reason = "participant transaction state missing",
                    });
                }
                return .{ .conflict = participantTornStateConflict(participant, .resolve) };
            },
            error.UnknownGroup => {
                if (surface_unavailable_conflict) {
                    if (trace_writer) |tw| {
                        tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
                    }
                    try abortBegunRefs(alloc, worker, txn_id, commit_version, begun.items);
                    return .{ .conflict = try participantUnavailableConflict(alloc, participant, .resolve) };
                }
                return err;
            },
            else => return err,
        };
    }

    if (trace_writer) |tw| {
        tw.traceEvent(&.{ .name = "CommitTransaction", .txn_id = txn_id, .shard_id = "", .timestamp = commit_version });
    }

    return .{ .committed = .{ .participant_count = participants.items.len } };
}

fn abortBegunParticipants(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    table_name: []const u8,
    txn_id: db_mod.types.TxnId,
    timestamp: u64,
    groups: []const u64,
) !void {
    for (groups) |group_id| {
        worker.resolveGroup(alloc, group_id, table_name, .{
            .txn_id = txn_id,
            .status = .aborted,
            .commit_version = timestamp,
        }) catch {};
    }
}

const GroupTxn = struct {
    group_id: u64,
    writes: std.ArrayListUnmanaged(db_mod.types.TransactionWrite) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,
    transforms: std.ArrayListUnmanaged(db_mod.types.DocumentTransform) = .empty,
    predicates: std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate) = .empty,

    fn deinit(self: *GroupTxn, alloc: std.mem.Allocator) void {
        self.writes.deinit(alloc);
        self.deletes.deinit(alloc);
        self.transforms.deinit(alloc);
        self.predicates.deinit(alloc);
        self.* = undefined;
    }
};

const ParticipantTxn = struct {
    table_name: []const u8,
    group_id: u64,
    topology_epoch: u64,
    writes: std.ArrayListUnmanaged(db_mod.types.TransactionWrite) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,
    transforms: std.ArrayListUnmanaged(db_mod.types.DocumentTransform) = .empty,
    predicates: std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate) = .empty,

    fn deinit(self: *ParticipantTxn, alloc: std.mem.Allocator) void {
        self.writes.deinit(alloc);
        self.deletes.deinit(alloc);
        self.transforms.deinit(alloc);
        self.predicates.deinit(alloc);
        self.* = undefined;
    }
};

fn ensureGroupTxn(alloc: std.mem.Allocator, grouped: *std.ArrayListUnmanaged(GroupTxn), group_id: u64) !*GroupTxn {
    for (grouped.items) |*group| {
        if (group.group_id == group_id) return group;
    }
    try grouped.append(alloc, .{ .group_id = group_id });
    return &grouped.items[grouped.items.len - 1];
}

fn ensureParticipantTxn(
    alloc: std.mem.Allocator,
    grouped: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    group_id: u64,
    topology_epoch: u64,
) !*ParticipantTxn {
    for (grouped.items) |*participant| {
        if (participant.group_id == group_id and std.mem.eql(u8, participant.table_name, table_name)) {
            if (participant.topology_epoch != topology_epoch) return error.TopologyChanged;
            return participant;
        }
    }
    try grouped.append(alloc, .{ .table_name = table_name, .group_id = group_id, .topology_epoch = topology_epoch });
    return &grouped.items[grouped.items.len - 1];
}

pub fn participantIdForGroup(alloc: std.mem.Allocator, table_name: []const u8, group_id: u64) ![]u8 {
    if (table_name.len > std.math.maxInt(u32)) return error.TableNameTooLong;
    return try std.fmt.allocPrint(alloc, "{s}{x:0>8}:{s}:{d}", .{ table_participant_v2_prefix, table_name.len, table_name, group_id });
}

pub const ParticipantRef = struct {
    table_name: []const u8,
    group_id: u64,
};

pub fn parseParticipantRef(participant: []const u8) ?ParticipantRef {
    if (std.mem.startsWith(u8, participant, table_participant_v2_prefix)) {
        const body = participant[table_participant_v2_prefix.len..];
        if (body.len < 9 or body[8] != ':') return null;
        const table_name_len = std.fmt.parseUnsigned(u32, body[0..8], 16) catch return null;
        const table_start: usize = 9;
        const group_separator = table_start + @as(usize, table_name_len);
        if (body.len <= group_separator or body[group_separator] != ':') return null;
        const table_name = body[table_start..group_separator];
        if (table_name.len == 0) return null;
        const group_id = std.fmt.parseUnsigned(u64, body[group_separator + 1 ..], 10) catch return null;
        return .{ .table_name = table_name, .group_id = group_id };
    }

    if (!std.mem.startsWith(u8, participant, table_participant_prefix)) return null;
    const rest = participant[table_participant_prefix.len..];
    const marker_index = std.mem.indexOf(u8, rest, group_participant_marker) orelse return null;
    const table_name = rest[0..marker_index];
    if (table_name.len == 0) return null;
    const group_id = std.fmt.parseUnsigned(u64, rest[marker_index + group_participant_marker.len ..], 10) catch return null;
    return .{ .table_name = table_name, .group_id = group_id };
}

test "distributed txn participant ids preserve embedded group markers" {
    const alloc = std.testing.allocator;

    const table_name = "docs:group:shadow";
    const participant = try participantIdForGroup(alloc, table_name, 42);
    defer alloc.free(participant);

    const parsed = parseParticipantRef(participant) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(table_name, parsed.table_name);
    try std.testing.expectEqual(@as(u64, 42), parsed.group_id);

    const legacy = parseParticipantRef("table:docs:group:42") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("docs", legacy.table_name);
    try std.testing.expectEqual(@as(u64, 42), legacy.group_id);
}

pub fn resolveParticipant(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    participant: []const u8,
    txn_id: db_mod.types.TxnId,
    status: db_mod.types.TxnStatus,
    commit_version: u64,
) !void {
    const ref = parseParticipantRef(participant) orelse return error.InvalidParticipant;
    try worker.resolveGroup(alloc, ref.group_id, ref.table_name, .{
        .txn_id = txn_id,
        .status = status,
        .commit_version = commit_version,
    });
}

pub fn encodeTxnIdHex(txn_id: db_mod.types.TxnId) [32]u8 {
    var out: [32]u8 = undefined;
    const hex = "0123456789abcdef";
    for (txn_id, 0..) |byte, i| {
        out[i * 2] = hex[byte >> 4];
        out[i * 2 + 1] = hex[byte & 0x0f];
    }
    return out;
}

pub fn parseTxnIdHex(text: []const u8) !db_mod.types.TxnId {
    if (text.len != 32) return error.InvalidTxnId;
    var out: db_mod.types.TxnId = undefined;
    for (0..16) |i| {
        out[i] = try std.fmt.parseInt(u8, text[i * 2 ..][0..2], 16);
    }
    return out;
}

pub fn encodeTxnBeginRequest(alloc: std.mem.Allocator, req: TxnBeginRequest) ![]u8 {
    const txn_hex = encodeTxnIdHex(req.txn_id);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"txn_id\":\"");
    try out.appendSlice(alloc, &txn_hex);
    try out.appendSlice(alloc, "\",\"begin_timestamp\":");
    const begin_timestamp = try std.fmt.allocPrint(alloc, "{d}", .{req.begin_timestamp});
    defer alloc.free(begin_timestamp);
    try out.appendSlice(alloc, begin_timestamp);
    try out.appendSlice(alloc, ",\"topology_epoch\":");
    const epoch = try std.fmt.allocPrint(alloc, "{d}", .{req.topology_epoch});
    defer alloc.free(epoch);
    try out.appendSlice(alloc, epoch);
    try out.appendSlice(alloc, ",\"participants\":[");
    for (req.participants, 0..) |participant, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(participant, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn encodeTxnPrepareRequest(alloc: std.mem.Allocator, req: TxnPrepareRequest) ![]u8 {
    const txn_hex = encodeTxnIdHex(req.txn_id);
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"txn_id\":\"");
    try out.appendSlice(alloc, &txn_hex);
    try out.appendSlice(alloc, "\",\"topology_epoch\":");
    const epoch = try std.fmt.allocPrint(alloc, "{d}", .{req.topology_epoch});
    defer alloc.free(epoch);
    try out.appendSlice(alloc, epoch);
    try out.appendSlice(alloc, ",\"writes\":[");
    for (req.req.writes, 0..) |write, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"key\":{f},\"value\":{s}}}",
            .{ std.json.fmt(write.key, .{}), write.value },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"deletes\":[");
    for (req.req.deletes, 0..) |key, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(key, .{})});
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"transforms\":[");
    for (req.req.transforms, 0..) |transform, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded_key = try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(transform.key, .{})});
        defer alloc.free(encoded_key);
        try out.appendSlice(alloc, "{\"key\":");
        try out.appendSlice(alloc, encoded_key);
        try out.appendSlice(alloc, ",\"operations\":[");
        for (transform.operations, 0..) |op, op_index| {
            if (op_index > 0) try out.append(alloc, ',');
            const encoded_op = try std.fmt.allocPrint(
                alloc,
                "{{\"op\":{f},\"path\":{f}",
                .{ std.json.fmt(db_mod.transform.transformOpText(op.op), .{}), std.json.fmt(op.path, .{}) },
            );
            defer alloc.free(encoded_op);
            try out.appendSlice(alloc, encoded_op);
            if (op.value_json) |value_json| {
                try out.appendSlice(alloc, ",\"value\":");
                try out.appendSlice(alloc, value_json);
            }
            try out.append(alloc, '}');
        }
        try out.append(alloc, ']');
        if (transform.upsert) try out.appendSlice(alloc, ",\"upsert\":true");
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "],\"predicates\":[");
    for (req.req.predicates, 0..) |predicate, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"key\":{f},\"expected_version\":{d}}}",
            .{ std.json.fmt(predicate.key, .{}), predicate.expected_version },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

pub fn encodeTxnResolveRequest(alloc: std.mem.Allocator, req: TxnResolveRequest) ![]u8 {
    const txn_hex = encodeTxnIdHex(req.txn_id);
    const status_text = switch (req.status) {
        .pending => "pending",
        .committed => "committed",
        .aborted => "aborted",
    };
    return try std.fmt.allocPrint(
        alloc,
        "{{\"txn_id\":\"{s}\",\"status\":\"{s}\",\"commit_version\":{d}}}",
        .{ &txn_hex, status_text, req.commit_version },
    );
}

pub fn encodeTxnStatusRequest(alloc: std.mem.Allocator, txn_id: db_mod.types.TxnId) ![]u8 {
    const txn_hex = encodeTxnIdHex(txn_id);
    return try std.fmt.allocPrint(alloc, "{{\"txn_id\":\"{s}\"}}", .{&txn_hex});
}

pub fn encodeTxnStatusResponse(alloc: std.mem.Allocator, response: TxnStatusResponse) ![]u8 {
    const status_text = switch (response.status) {
        .pending => "pending",
        .committed => "committed",
        .aborted => "aborted",
    };
    return try std.fmt.allocPrint(alloc, "{{\"status\":\"{s}\"}}", .{status_text});
}

pub fn parseTxnBeginRequest(alloc: std.mem.Allocator, body: []const u8) !TxnBeginRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const txn_id = try parseTxnIdHex(requireString(obj, "txn_id"));
    const begin_timestamp = requireInteger(obj, "begin_timestamp");
    const participants_value = obj.get("participants") orelse return error.InvalidTxnRequest;
    const participants = switch (participants_value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc([]const u8, participants.items.len);
    errdefer alloc.free(out);
    for (participants.items, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, switch (item) {
            .string => |s| s,
            else => return error.InvalidTxnRequest,
        });
    }
    return .{ .txn_id = txn_id, .begin_timestamp = begin_timestamp, .topology_epoch = requireInteger(obj, "topology_epoch"), .participants = out };
}

pub fn freeTxnBeginRequest(alloc: std.mem.Allocator, req: *TxnBeginRequest) void {
    for (req.participants) |participant| alloc.free(@constCast(participant));
    if (req.participants.len > 0) alloc.free(req.participants);
    req.* = undefined;
}

pub fn parseTxnPrepareRequest(alloc: std.mem.Allocator, body: []const u8) !TxnPrepareRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const txn_id = try parseTxnIdHex(requireString(obj, "txn_id"));
    const writes = try parseTxnWrites(alloc, obj.get("writes") orelse return error.InvalidTxnRequest);
    errdefer if (writes.len > 0) alloc.free(writes);
    const deletes = try parseTxnDeletes(alloc, obj.get("deletes") orelse return error.InvalidTxnRequest);
    errdefer if (deletes.len > 0) alloc.free(deletes);
    const transforms = try parseTxnTransforms(alloc, obj.get("transforms") orelse return error.InvalidTxnRequest);
    errdefer freeTxnTransforms(alloc, transforms);
    const predicates = try parseTxnPredicates(alloc, obj.get("predicates") orelse return error.InvalidTxnRequest);
    errdefer if (predicates.len > 0) alloc.free(predicates);
    return .{
        .txn_id = txn_id,
        .topology_epoch = requireInteger(obj, "topology_epoch"),
        .req = .{
            .writes = writes,
            .deletes = deletes,
            .transforms = transforms,
            .predicates = predicates,
        },
    };
}

pub fn freeTxnPrepareRequest(alloc: std.mem.Allocator, req: *TxnPrepareRequest) void {
    for (req.req.writes) |write| {
        alloc.free(@constCast(write.key));
        alloc.free(@constCast(write.value));
    }
    if (req.req.writes.len > 0) alloc.free(req.req.writes);
    for (req.req.deletes) |key| alloc.free(@constCast(key));
    if (req.req.deletes.len > 0) alloc.free(req.req.deletes);
    freeTxnTransforms(alloc, req.req.transforms);
    for (req.req.predicates) |predicate| alloc.free(@constCast(predicate.key));
    if (req.req.predicates.len > 0) alloc.free(req.req.predicates);
    req.* = undefined;
}

pub fn parseTxnResolveRequest(alloc: std.mem.Allocator, body: []const u8) !TxnResolveRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    return .{
        .txn_id = try parseTxnIdHex(requireString(obj, "txn_id")),
        .status = parseTxnStatus(requireString(obj, "status")) orelse return error.InvalidTxnRequest,
        .commit_version = requireInteger(obj, "commit_version"),
    };
}

pub fn parseTxnStatusRequest(alloc: std.mem.Allocator, body: []const u8) !db_mod.types.TxnId {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    return try parseTxnIdHex(requireString(obj, "txn_id"));
}

pub fn parseTxnStatusResponse(alloc: std.mem.Allocator, body: []const u8) !TxnStatusResponse {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    return .{ .status = parseTxnStatus(requireString(obj, "status")) orelse return error.InvalidTxnRequest };
}

fn parseTxnWrites(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.TransactionWrite {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.TransactionWrite, arr.items.len);
    for (arr.items, 0..) |item, i| {
        const obj = switch (item) {
            .object => |obj| obj,
            else => return error.InvalidTxnRequest,
        };
        out[i] = .{
            .key = try alloc.dupe(u8, requireString(obj, "key")),
            .value = blk: {
                const raw_value = obj.get("value") orelse return error.InvalidTxnRequest;
                break :blk try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(raw_value, .{})});
            },
        };
    }
    return out;
}

fn parseTxnDeletes(alloc: std.mem.Allocator, value: std.json.Value) ![]const []const u8 {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc([]const u8, arr.items.len);
    for (arr.items, 0..) |item, i| {
        out[i] = try alloc.dupe(u8, switch (item) {
            .string => |s| s,
            else => return error.InvalidTxnRequest,
        });
    }
    return out;
}

fn parseTxnTransforms(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.DocumentTransform {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.DocumentTransform, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |transform| {
            alloc.free(@constCast(transform.key));
            for (transform.operations) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (transform.operations.len > 0) alloc.free(@constCast(transform.operations));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |obj| obj,
            else => return error.InvalidTxnRequest,
        };
        const key = requireString(obj, "key");
        if (key.len == 0) return error.InvalidTxnRequest;
        const operations_value = obj.get("operations") orelse return error.InvalidTxnRequest;
        const operations_arr = switch (operations_value) {
            .array => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        var ops = try alloc.alloc(db_mod.types.TransformOp, operations_arr.items.len);
        var ops_initialized: usize = 0;
        errdefer {
            for (ops[0..ops_initialized]) |op| {
                alloc.free(@constCast(op.path));
                if (op.value_json) |value_json| alloc.free(@constCast(value_json));
            }
            if (ops.len > 0) alloc.free(ops);
        }
        for (operations_arr.items, 0..) |op_item, i| {
            const op_obj = switch (op_item) {
                .object => |inner| inner,
                else => return error.InvalidTxnRequest,
            };
            const op_text = requireString(op_obj, "op");
            const path = requireString(op_obj, "path");
            if (op_text.len == 0 or path.len == 0) return error.InvalidTxnRequest;
            ops[i] = .{
                .op = parseTransformOpType(op_text) orelse return error.InvalidTxnRequest,
                .path = try alloc.dupe(u8, path),
                .value_json = if (op_obj.get("value")) |raw_value| try std.fmt.allocPrint(alloc, "{f}", .{std.json.fmt(raw_value, .{})}) else null,
            };
            ops_initialized += 1;
        }
        out[initialized] = .{
            .key = try alloc.dupe(u8, key),
            .operations = ops,
            .upsert = if (obj.get("upsert")) |upsert_value| switch (upsert_value) {
                .bool => |flag| flag,
                .null => false,
                else => return error.InvalidTxnRequest,
            } else false,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnTransforms(alloc: std.mem.Allocator, transforms: []const db_mod.types.DocumentTransform) void {
    for (transforms) |transform| {
        alloc.free(@constCast(transform.key));
        for (transform.operations) |op| {
            alloc.free(@constCast(op.path));
            if (op.value_json) |value_json| alloc.free(@constCast(value_json));
        }
        if (transform.operations.len > 0) alloc.free(@constCast(transform.operations));
    }
    if (transforms.len > 0) alloc.free(@constCast(transforms));
}

fn parseTransformOpType(text: []const u8) ?db_mod.types.TransformOpType {
    if (std.mem.eql(u8, text, "$set")) return .set;
    if (std.mem.eql(u8, text, "$unset")) return .unset;
    if (std.mem.eql(u8, text, "$inc")) return .inc;
    if (std.mem.eql(u8, text, "$push")) return .push;
    if (std.mem.eql(u8, text, "$pull")) return .pull;
    if (std.mem.eql(u8, text, "$addToSet")) return .add_to_set;
    if (std.mem.eql(u8, text, "$pop")) return .pop;
    if (std.mem.eql(u8, text, "$mul")) return .mul;
    if (std.mem.eql(u8, text, "$min")) return .min;
    if (std.mem.eql(u8, text, "$max")) return .max;
    if (std.mem.eql(u8, text, "$currentDate")) return .current_date;
    if (std.mem.eql(u8, text, "$rename")) return .rename;
    return null;
}

fn parseTxnPredicates(alloc: std.mem.Allocator, value: std.json.Value) ![]db_mod.types.TransactionVersionPredicate {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.TransactionVersionPredicate, arr.items.len);
    for (arr.items, 0..) |item, i| {
        const obj = switch (item) {
            .object => |obj| obj,
            else => return error.InvalidTxnRequest,
        };
        out[i] = .{
            .key = try alloc.dupe(u8, requireString(obj, "key")),
            .expected_version = requireInteger(obj, "expected_version"),
        };
    }
    return out;
}

fn requireString(obj: std.json.ObjectMap, key: []const u8) []const u8 {
    const value = obj.get(key) orelse return "";
    return switch (value) {
        .string => |s| s,
        else => "",
    };
}

test "txn prepare parser preserves raw JSON object values" {
    const alloc = std.testing.allocator;
    const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
    const body = try encodeTxnPrepareRequest(alloc, .{
        .txn_id = txn_id,
        .topology_epoch = 7,
        .req = .{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
        },
    });
    defer alloc.free(body);

    var parsed = try parseTxnPrepareRequest(alloc, body);
    defer freeTxnPrepareRequest(alloc, &parsed);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.writes.len);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", parsed.req.writes[0].value);
}

test "txn prepare parser round-trips transforms" {
    const alloc = std.testing.allocator;
    const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
    const body = try encodeTxnPrepareRequest(alloc, .{
        .txn_id = txn_id,
        .topology_epoch = 7,
        .req = .{
            .transforms = &.{.{
                .key = "doc:a",
                .operations = &.{
                    .{ .op = .set, .path = "status", .value_json = "\"updated\"" },
                    .{ .op = .max, .path = "version", .value_json = "3" },
                },
                .upsert = true,
            }},
        },
    });
    defer alloc.free(body);

    var parsed = try parseTxnPrepareRequest(alloc, body);
    defer freeTxnPrepareRequest(alloc, &parsed);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.transforms.len);
    try std.testing.expect(parsed.req.transforms[0].upsert);
    try std.testing.expectEqualStrings("doc:a", parsed.req.transforms[0].key);
    try std.testing.expectEqual(db_mod.types.TransformOpType.set, parsed.req.transforms[0].operations[0].op);
    try std.testing.expectEqualStrings("\"updated\"", parsed.req.transforms[0].operations[0].value_json.?);
}

fn requireInteger(obj: std.json.ObjectMap, key: []const u8) u64 {
    const value = obj.get(key) orelse return 0;
    return switch (value) {
        .integer => |i| @intCast(i),
        else => 0,
    };
}

fn parseTxnStatus(text: []const u8) ?db_mod.types.TxnStatus {
    if (std.mem.eql(u8, text, "pending")) return .pending;
    if (std.mem.eql(u8, text, "committed")) return .committed;
    if (std.mem.eql(u8, text, "aborted")) return .aborted;
    return null;
}

fn abortBegunRefs(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    timestamp: u64,
    refs: []const ParticipantRef,
) !void {
    for (refs) |ref| {
        worker.resolveGroup(alloc, ref.group_id, ref.table_name, .{
            .txn_id = txn_id,
            .status = .aborted,
            .commit_version = timestamp,
        }) catch {};
    }
}

fn participantConflict(participant: ParticipantTxn) CommitConflict {
    if (participant.predicates.items.len > 0) {
        return .{
            .table_name = participant.table_name,
            .key = participant.predicates.items[0].key,
            .message = "version conflict",
            .group_id = participant.group_id,
            .phase = .prepare,
        };
    }
    if (participant.writes.items.len > 0) {
        return .{
            .table_name = participant.table_name,
            .key = participant.writes.items[0].key,
            .message = "intent conflict",
            .group_id = participant.group_id,
            .phase = .prepare,
        };
    }
    if (participant.deletes.items.len > 0) {
        return .{
            .table_name = participant.table_name,
            .key = participant.deletes.items[0],
            .message = "intent conflict",
            .group_id = participant.group_id,
            .phase = .prepare,
        };
    }
    return .{
        .table_name = participant.table_name,
        .key = "",
        .message = "transaction conflict",
        .group_id = participant.group_id,
        .phase = .prepare,
    };
}

fn participantUnavailableConflict(alloc: std.mem.Allocator, participant: ParticipantTxn, phase: ParticipantPhase) !CommitConflict {
    _ = alloc;
    return .{
        .table_name = participant.table_name,
        .key = "",
        .message = "participant unavailable",
        .group_id = participant.group_id,
        .phase = phase,
    };
}

fn participantDecisionConflict(participant: ParticipantTxn, phase: ParticipantPhase) CommitConflict {
    return .{
        .table_name = participant.table_name,
        .key = "",
        .message = "decision conflict",
        .group_id = participant.group_id,
        .phase = phase,
    };
}

fn participantTornStateConflict(participant: ParticipantTxn, phase: ParticipantPhase) CommitConflict {
    return .{
        .table_name = participant.table_name,
        .key = "",
        .message = "transaction state missing",
        .group_id = participant.group_id,
        .phase = phase,
    };
}

test "distributed txn coordinator groups by range and commits all participants" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const metadata_table_manager = @import("../metadata/table_manager.zig");
            const raft_reconciler = @import("../raft/reconciler.zig");
            const metadata_transition_state = @import("../metadata/transition_state.zig");
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        begins: std.ArrayListUnmanaged(u64) = .empty,
        prepares: std.ArrayListUnmanaged(u64) = .empty,
        resolves: std.ArrayListUnmanaged(struct { group_id: u64, status: db_mod.types.TxnStatus }) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.begins.deinit(alloc);
            self.prepares.deinit(alloc);
            self.resolves.deinit(alloc);
        }

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
            try self.begins.append(std.testing.allocator, group_id);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(req.req.writes.len + req.req.deletes.len + req.req.predicates.len > 0);
            try self.prepares.append(std.testing.allocator, group_id);
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.resolves.append(std.testing.allocator, .{ .group_id = group_id, .status = req.status });
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    defer recorder.deinit(std.testing.allocator);
    const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
    const result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "docs",
        txn_id,
        10_000,
        10_001,
        .{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
            },
            .predicates = &.{
                .{ .key = "doc:a", .expected_version = 1 },
                .{ .key = "doc:z", .expected_version = 2 },
            },
        },
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.participant_count);
    try std.testing.expectEqual(@as(usize, 2), recorder.begins.items.len);
    try std.testing.expectEqual(@as(usize, 2), recorder.prepares.items.len);
    try std.testing.expectEqual(@as(usize, 2), recorder.resolves.items.len);
    for (recorder.resolves.items) |resolved| try std.testing.expectEqual(db_mod.types.TxnStatus.committed, resolved.status);
}

test "distributed txn coordinator aborts begun participants on prepare failure" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const metadata_table_manager = @import("../metadata/table_manager.zig");
            const raft_reconciler = @import("../raft/reconciler.zig");
            const metadata_transition_state = @import("../metadata/transition_state.zig");
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        resolves: std.ArrayListUnmanaged(db_mod.types.TxnStatus) = .empty,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.resolves.deinit(alloc);
        }

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}

        fn prepare(_: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, _: TxnPrepareRequest) !void {
            if (group_id == 7002) return error.IntentConflict;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.resolves.append(std.testing.allocator, req.status);
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    defer recorder.deinit(std.testing.allocator);
    const txn_id = try parseTxnIdHex("ffeeddccbbaa99887766554433221100");
    try std.testing.expectError(error.IntentConflict, executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "docs",
        txn_id,
        10_000,
        10_001,
        .{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"a\"}" },
                .{ .key = "doc:z", .value = "{\"title\":\"z\"}" },
            },
        },
        null,
    ));
    try std.testing.expectEqual(@as(usize, 2), recorder.resolves.items.len);
    for (recorder.resolves.items) |status| try std.testing.expectEqual(db_mod.types.TxnStatus.aborted, status);
}

test "distributed txn coordinator retries once on topology change" {
    const FakeCatalog = struct {
        call_count: usize = 0,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(ptr: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.call_count += 1;
            const metadata_table_manager = @import("../metadata/table_manager.zig");
            const raft_reconciler = @import("../raft/reconciler.zig");
            const metadata_transition_state = @import("../metadata/transition_state.zig");
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = if (self.call_count <= 2)
                    @constCast((&[_]metadata_table_manager.RangeRecord{
                        .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                        .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    })[0..])
                else
                    @constCast((&[_]metadata_table_manager.RangeRecord{
                        .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:n" },
                        .{ .group_id = 7002, .table_id = 7, .start_key = "doc:n", .end_key = null },
                    })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        prepare_calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
            if (self.prepare_calls == 1) {
                try std.testing.expect(req.topology_epoch != 0);
                return error.TopologyChanged;
            }
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var catalog = FakeCatalog{};
    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("11112222333344445555666677778888");
    const outcome = try executeMultiTableCommit(
        std.testing.allocator,
        catalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:z", .value = "{\"title\":\"z\"}" }},
        }},
        null,
    );
    try std.testing.expect(outcome == .committed);
    try std.testing.expectEqual(@as(usize, 2), recorder.prepare_calls);
}

test "distributed txn coordinator stops after single topology retry" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const metadata_table_manager = @import("../metadata/table_manager.zig");
            const raft_reconciler = @import("../raft/reconciler.zig");
            const metadata_transition_state = @import("../metadata/transition_state.zig");
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        fn worker() ParticipantWorker {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.TopologyChanged;
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    const txn_id = try parseTxnIdHex("99990000111122223333444455556666");
    try std.testing.expectError(error.TopologyChanged, executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        Recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"a\"}" }},
        }},
        null,
    ));
}

test "distributed txn coordinator surfaces participant group on repeated unknown-group failure" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const metadata_table_manager = @import("../metadata/table_manager.zig");
            const raft_reconciler = @import("../raft/reconciler.zig");
            const metadata_transition_state = @import("../metadata/transition_state.zig");
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        begin_calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_calls += 1;
            return error.UnknownGroup;
        }

        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("aaaabbbbccccddddeeeeffff00001111");
    const outcome = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"a\"}" }},
        }},
        null,
    );
    try std.testing.expect(outcome == .conflict);
    try std.testing.expectEqualStrings("participant unavailable", outcome.conflict.message);
    try std.testing.expectEqualStrings("docs", outcome.conflict.table_name);
    try std.testing.expectEqual(@as(?u64, 7001), outcome.conflict.group_id);
    try std.testing.expectEqual(.begin, outcome.conflict.phase.?);
    try std.testing.expectEqual(@as(usize, 2), recorder.begin_calls);
}

test "distributed txn coordinator surfaces resolve decision conflicts deterministically" {
    const FakeCatalog = struct {
        fn iface() table_catalog.CatalogSource {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn adminSnapshot(_: *anyopaque) !@import("../metadata/api.zig").AdminSnapshot {
            const metadata_table_manager = @import("../metadata/table_manager.zig");
            const raft_reconciler = @import("../raft/reconciler.zig");
            const metadata_transition_state = @import("../metadata/transition_state.zig");
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{.{ .table_id = 7, .name = "docs", .placement_role = "data" }})[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        begin_calls: usize = 0,
        prepare_calls: usize = 0,
        resolve_calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_calls += 1;
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.resolve_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            return error.DecisionConflict;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("11112222333344445555666677778888");
    const outcome = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"a\"}" }},
        }},
        null,
    );
    try std.testing.expect(outcome == .conflict);
    try std.testing.expectEqualStrings("decision conflict", outcome.conflict.message);
    try std.testing.expectEqualStrings("docs", outcome.conflict.table_name);
    try std.testing.expectEqual(@as(?u64, 7001), outcome.conflict.group_id);
    try std.testing.expectEqual(.resolve, outcome.conflict.phase.?);
    try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.resolve_calls);
}

test "db transaction recovery runtime resolves table-group participants through distributed txn resolver" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/distributed-txn-recovery-db", .{tmp.sub_path});
    defer alloc.free(path);

    const Recorder = struct {
        calls: usize = 0,
        committed_calls: usize = 0,
        aborted_calls: usize = 0,
        last_group_id: u64 = 0,
        last_status: ?db_mod.types.TxnStatus = null,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            self.calls += 1;
            self.last_group_id = group_id;
            self.last_status = req.status;
            switch (req.status) {
                .committed => self.committed_calls += 1,
                .aborted => self.aborted_calls += 1,
                else => {},
            }
        }
    };

    var recorder = Recorder{};
    var resolver = RecoveryResolver{
        .alloc = alloc,
        .worker = recorder.worker(),
        .lease_owned = true,
        .interval_ms = 250,
    };
    var db = try db_mod.DB.open(alloc, path, .{
        .transaction_recovery = resolver.config(),
    });
    defer db.close();

    const participant = try participantIdForGroup(alloc, "docs", 77);
    defer alloc.free(participant);
    const txn_id = try db.beginTransactionWithParticipants(1_000, &.{participant});
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:recover", .value = "{\"title\":\"value\"}" }},
    });
    try db.resolveTransactionIntents(txn_id, .committed, 2_000);

    var attempts: usize = 0;
    while (attempts < 200) : (attempts += 1) {
        const status = db.getTransactionStatus(txn_id);
        if (status) |_| {} else |err| {
            if (err == transactions_mod.TxnError.TxnNotFound) break;
            return err;
        }
        sleepNs(5 * std.time.ns_per_ms);
    }

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.transaction_recovery.notification_attempts > 0);
    try std.testing.expect(stats.transaction_recovery.notification_successes > 0);
    try std.testing.expect(recorder.calls > 0);
    try std.testing.expectEqual(@as(u64, 77), recorder.last_group_id);
    try std.testing.expect(recorder.committed_calls > 0);
    try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
}

fn sleepNs(duration_ns: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(duration_ns / std.time.ns_per_s),
        .nsec = @intCast(duration_ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

test "db one-shot transaction recovery resolves table-group participants through distributed txn resolver" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/distributed-txn-recovery-once-db", .{tmp.sub_path});
    defer alloc.free(path);

    const Recorder = struct {
        calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(u64, 88), group_id);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            self.calls += 1;
        }
    };

    var recorder = Recorder{};
    var resolver = RecoveryResolver{
        .alloc = alloc,
        .worker = recorder.worker(),
        .lease_owned = true,
    };
    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const participant = try participantIdForGroup(alloc, "docs", 88);
    defer alloc.free(participant);
    const txn_id = try db.beginTransactionWithParticipants(1_000, &.{participant});
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:recover-once", .value = "{\"title\":\"value\"}" }},
    });
    try db.resolveTransactionIntents(txn_id, .committed, 2_000);

    const stats = try db.runTransactionRecoveryOnce(resolver.config());
    try std.testing.expect(stats.notification_attempts > 0);
    try std.testing.expect(stats.notification_successes > 0);
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, db.getTransactionStatus(txn_id));
}

test "db one-shot transaction recovery does not auto-abort fresh pending transactions by default" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/distributed-txn-recovery-fresh-pending-db", .{tmp.sub_path});
    defer alloc.free(path);

    const Recorder = struct {
        calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    var resolver = RecoveryResolver{
        .alloc = alloc,
        .worker = recorder.worker(),
        .lease_owned = true,
    };
    var db = try db_mod.DB.open(alloc, path, .{});
    defer db.close();

    const participant = try participantIdForGroup(alloc, "docs", 99);
    defer alloc.free(participant);
    const txn_id = try db.beginTransactionWithParticipants(1_000, &.{participant});
    try db.writeTransaction(txn_id, .{
        .writes = &.{.{ .key = "doc:fresh-pending", .value = "{\"title\":\"value\"}" }},
    });

    const stats = try db.runTransactionRecoveryOnce(resolver.config());
    try std.testing.expectEqual(@as(u64, 0), stats.notification_attempts);
    try std.testing.expectEqual(@as(u64, 0), stats.auto_aborted);
    try std.testing.expectEqual(@as(usize, 0), recorder.calls);
    try std.testing.expectEqual(db_mod.types.TxnStatus.pending, try db.getTransactionStatus(txn_id));
}
