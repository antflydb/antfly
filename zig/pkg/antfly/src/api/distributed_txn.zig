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
const table_catalog = @import("../metadata/catalog/routing.zig");
const table_router = @import("table_router.zig");
const table_reads = @import("table_reads.zig");
const table_writes = @import("table_writes.zig");
const schema_mod = @import("../schema/mod.zig");
const storage_schema = @import("../storage/schema.zig");
const relational_store = @import("../storage/db/relational_store.zig");
const document_mapper = @import("../storage/db/document_mapper.zig");
const platform_clock = @import("../platform/clock.zig");

const table_participant_v2_prefix = "table2:";

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

pub const ForeignKeyRefChildrenRequest = struct {
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    limit: usize = 4096,
    start_after_child_table: ?[]const u8 = null,
    start_after_child_key: ?[]const u8 = null,
};

pub const ForeignKeyRefChildrenResponse = struct {
    children: []const db_mod.types.ForeignKeyRefChild,
    complete: bool = true,
    next_child_table: ?[]const u8 = null,
    next_child_key: ?[]const u8 = null,
};

pub const ForeignKeyDeleteExplain = struct {
    parent_group_id: u64,
    routed_owner_group_count: usize = 0,
    plan: relational_store.ForeignKeyDeletePlan,
};

pub const ForeignKeyActionPageExecution = struct {
    applied_children: usize = 0,
    complete: bool = true,
    next_child_table: ?[]u8 = null,
    next_child_key: ?[]u8 = null,
    participant_count: usize = 0,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.next_child_table) |value| alloc.free(value);
        if (self.next_child_key) |value| alloc.free(value);
        self.* = undefined;
    }
};

pub const TableCommitRequest = struct {
    table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite = &.{},
    deletes: []const []const u8 = &.{},
    transforms: []const db_mod.types.DocumentTransform = &.{},
    predicates: []const db_mod.types.TransactionVersionPredicate = &.{},
    preimages: []const db_mod.types.TransactionWrite = &.{},
    foreign_key_constraint_timing_overrides: []const db_mod.types.ForeignKeyConstraintTimingOverride = &.{},
};

const TableForeignKeyConstraintTimingOverrides = struct {
    table_name: []const u8,
    overrides: []const db_mod.types.ForeignKeyConstraintTimingOverride,
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
        lookup_group: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            key: []const u8,
        ) anyerror!?table_reads.LookupResponse = null,
        foreign_key_ref_children_group: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) anyerror![]db_mod.types.ForeignKeyRefChild = null,
        foreign_key_ref_children_page_group: ?*const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) anyerror!db_mod.types.ForeignKeyRefChildrenPage = null,
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

    pub fn lookupGroup(self: ParticipantWorker, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
        const fn_ptr = self.vtable.lookup_group orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, key);
    }

    pub fn foreignKeyRefChildrenGroup(
        self: ParticipantWorker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: ForeignKeyRefChildrenRequest,
    ) ![]db_mod.types.ForeignKeyRefChild {
        const fn_ptr = self.vtable.foreign_key_ref_children_group orelse return error.UnsupportedOperation;
        return try fn_ptr(self.ptr, alloc, group_id, table_name, req);
    }

    pub fn foreignKeyRefChildrenPageGroup(
        self: ParticipantWorker,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: ForeignKeyRefChildrenRequest,
    ) !db_mod.types.ForeignKeyRefChildrenPage {
        if (self.vtable.foreign_key_ref_children_page_group) |fn_ptr| {
            return try fn_ptr(self.ptr, alloc, group_id, table_name, req);
        }
        if (req.start_after_child_table != null or req.start_after_child_key != null) return error.UnsupportedOperation;
        const children = try self.foreignKeyRefChildrenGroup(alloc, group_id, table_name, req);
        return .{
            .children = @constCast(children),
            .complete = true,
        };
    }
};

pub const RecoveryResolver = struct {
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    owner_id: []const u8 = "api",
    lease_owned: bool = false,
    interval_ms: u64 = 10,
    cutoff_ns: u64 = 5 * std.time.ns_per_min,
    clock: platform_clock.Clock = platform_clock.Clock.real(),

    pub fn config(self: *const RecoveryResolver) db_mod.transaction_runtime.Config {
        return .{
            .enabled = true,
            .lease_owned = self.lease_owned,
            .owner_id = self.owner_id,
            .interval_ms = self.interval_ms,
            .cutoff_ns = self.cutoff_ns,
            .clock = self.clock,
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
    reads: ?table_reads.TableReadSource = null,
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

    pub fn withReads(self: *HostedParticipantWorker, reads: table_reads.TableReadSource) *HostedParticipantWorker {
        self.reads = reads;
        return self;
    }

    pub fn worker(self: *HostedParticipantWorker) ParticipantWorker {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_group = beginGroup,
                .prepare_group = prepareGroup,
                .resolve_group = resolveGroup,
                .status_group = statusGroup,
                .lookup_group = lookupGroup,
                .foreign_key_ref_children_group = foreignKeyRefChildrenGroup,
                .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
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

    fn lookupGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        return switch (route) {
            .local => blk: {
                const reads = self.reads orelse return error.UnsupportedOperation;
                break :blk try reads.lookupGroupLocal(alloc, group_id, table_name, key, .{}, .read_index);
            },
            .remote => |remote| blk: {
                var client = http_client_mod.ApiHttpClient.init(alloc, self.executor);
                var response = client.fetchGroupLookup(remote.base_uri, group_id, table_name, key, null) catch |err| switch (err) {
                    error.NotFound => return null,
                    else => return err,
                };
                defer response.deinit(alloc);
                const version_text = response.version orelse return error.UnsupportedOperation;
                break :blk .{
                    .json = try alloc.dupe(u8, response.body),
                    .version = try std.fmt.parseUnsigned(u64, version_text, 10),
                };
            },
        };
    }

    fn foreignKeyRefChildrenGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: ForeignKeyRefChildrenRequest) ![]db_mod.types.ForeignKeyRefChild {
        var page = try foreignKeyRefChildrenPageGroup(ptr, alloc, group_id, table_name, req);
        errdefer table_writes.freeForeignKeyRefChildrenPage(alloc, &page);
        if (!page.complete) return error.ForeignKeyActionLimitExceeded;
        const children = page.children;
        page.children = &.{};
        table_writes.freeForeignKeyRefChildrenPage(alloc, &page);
        return children;
    }

    fn foreignKeyRefChildrenPageGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: ForeignKeyRefChildrenRequest) !db_mod.types.ForeignKeyRefChildrenPage {
        const self: *HostedParticipantWorker = @ptrCast(@alignCast(ptr));
        var route = (try table_router.resolveGroupRoute(alloc, self.catalog, self.router, group_id, .prefer_leader)) orelse return error.UnknownGroup;
        defer route.deinit(alloc);
        return switch (route) {
            .local => (try self.writes.foreignKeyRefChildrenPageGroupLocal(alloc, group_id, table_name, req.constraint_name, req.parent_table, req.parent_key, req.start_after_child_table, req.start_after_child_key, req.limit)) orelse error.UnknownGroup,
            .remote => |remote| blk: {
                var client = http_client_mod.ApiHttpClient.init(alloc, self.executor);
                const body = try encodeForeignKeyRefChildrenRequest(alloc, req);
                defer alloc.free(body);
                var response = try client.fetchGroupForeignKeyRefChildren(remote.base_uri, group_id, table_name, body);
                defer response.deinit(alloc);
                var parsed = try parseForeignKeyRefChildrenResponse(alloc, response.body);
                errdefer freeForeignKeyRefChildrenResponse(alloc, &parsed);
                const page: db_mod.types.ForeignKeyRefChildrenPage = .{
                    .children = @constCast(parsed.children),
                    .complete = parsed.complete,
                    .next_child_table = parsed.next_child_table,
                    .next_child_key = parsed.next_child_key,
                };
                parsed.children = &.{};
                parsed.next_child_table = null;
                parsed.next_child_key = null;
                freeForeignKeyRefChildrenResponse(alloc, &parsed);
                break :blk page;
            },
        };
    }
};

pub const LocalTableWriteParticipantWorker = struct {
    writes: table_writes.TableWriteSource,
    reads: ?table_reads.TableReadSource = null,

    pub fn init(writes: table_writes.TableWriteSource) LocalTableWriteParticipantWorker {
        return .{ .writes = writes };
    }

    pub fn withReads(self: *LocalTableWriteParticipantWorker, reads: table_reads.TableReadSource) *LocalTableWriteParticipantWorker {
        self.reads = reads;
        return self;
    }

    pub fn worker(self: *LocalTableWriteParticipantWorker) ParticipantWorker {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_group = beginGroup,
                .prepare_group = prepareGroup,
                .resolve_group = resolveGroup,
                .status_group = statusGroup,
                .lookup_group = lookupGroup,
                .foreign_key_ref_children_group = foreignKeyRefChildrenGroup,
                .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
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

    fn lookupGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        const reads = self.reads orelse return error.UnsupportedOperation;
        return try reads.lookupGroupLocal(alloc, group_id, table_name, key, .{}, .read_index);
    }

    fn foreignKeyRefChildrenGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: ForeignKeyRefChildrenRequest) ![]db_mod.types.ForeignKeyRefChild {
        var page = try foreignKeyRefChildrenPageGroup(ptr, alloc, group_id, table_name, req);
        errdefer table_writes.freeForeignKeyRefChildrenPage(alloc, &page);
        if (!page.complete) return error.ForeignKeyActionLimitExceeded;
        const children = page.children;
        page.children = &.{};
        table_writes.freeForeignKeyRefChildrenPage(alloc, &page);
        return children;
    }

    fn foreignKeyRefChildrenPageGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: ForeignKeyRefChildrenRequest) !db_mod.types.ForeignKeyRefChildrenPage {
        const self: *LocalTableWriteParticipantWorker = @ptrCast(@alignCast(ptr));
        return (try self.writes.foreignKeyRefChildrenPageGroupLocal(alloc, group_id, table_name, req.constraint_name, req.parent_table, req.parent_key, req.start_after_child_table, req.start_after_child_key, req.limit)) orelse error.UnknownGroup;
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
    var participants = std.ArrayListUnmanaged(ParticipantTxn).empty;
    defer {
        for (participants.items) |*participant| participant.deinit(alloc);
        participants.deinit(alloc);
    }
    const constraint_timing_overrides = [_]TableForeignKeyConstraintTimingOverrides{.{
        .table_name = table_name,
        .overrides = req.foreign_key_constraint_timing_overrides,
    }};

    try rejectUnsupportedDistributedForeignKeyTransforms(alloc, catalog, table_name, req.transforms);

    for (req.writes) |write| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, write.key, topology_epoch)) orelse return error.UnknownGroup;
        const participant = try ensureParticipantTxn(alloc, &participants, table_name, group_id, topology_epoch);
        try participant.writes.append(alloc, write);
    }
    for (req.deletes) |key| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, key, topology_epoch)) orelse return error.UnknownGroup;
        const participant = try ensureParticipantTxn(alloc, &participants, table_name, group_id, topology_epoch);
        try participant.deletes.append(alloc, key);
    }
    for (req.predicates) |predicate| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, predicate.key, topology_epoch)) orelse return error.UnknownGroup;
        const participant = try ensureParticipantTxn(alloc, &participants, table_name, group_id, topology_epoch);
        try participant.predicates.append(alloc, predicate);
    }
    for (req.transforms) |transform| {
        const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, transform.key, topology_epoch)) orelse return error.UnknownGroup;
        const participant = try ensureParticipantTxn(alloc, &participants, table_name, group_id, topology_epoch);
        try participant.transforms.append(alloc, transform);
    }
    try addForeignKeyParentParticipants(alloc, catalog, worker, &participants, table_name, req.writes, req.transforms, req.predicates, &constraint_timing_overrides);
    try addForeignKeyTransformParticipants(alloc, catalog, worker, &participants, table_name, req.writes, req.deletes, req.transforms, req.predicates, &constraint_timing_overrides);
    try addForeignKeyChildDeleteParticipants(alloc, catalog, worker, &participants, table_name, req.deletes, req.transforms, req.predicates);
    try addForeignKeyParentUpdateParticipants(alloc, catalog, worker, &participants, table_name, req.writes, req.deletes, req.transforms, req.predicates, &constraint_timing_overrides);
    try addForeignKeyParentDeleteParticipants(
        alloc,
        catalog,
        worker,
        &participants,
        table_name,
        req.writes,
        &.{},
        req.deletes,
        req.predicates,
        &constraint_timing_overrides,
        0,
        foreign_key_action_default_cascade_max_depth,
    );
    try addUniqueConstraintOwnerParticipants(alloc, catalog, worker, &participants, table_name, req.writes, req.deletes, req.transforms, req.predicates);
    try applyForeignKeyConstraintTimingOverridesToParticipants(alloc, &participants, &constraint_timing_overrides);

    var participant_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (participant_ids.items) |participant_id| alloc.free(@constCast(participant_id));
        participant_ids.deinit(alloc);
    }
    try participant_ids.ensureTotalCapacity(alloc, participants.items.len);
    for (participants.items) |participant| {
        const participant_id = try participantIdForGroup(alloc, participant.table_name, participant.group_id);
        participant_ids.appendAssumeCapacity(participant_id);
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
        try worker.beginGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .begin_timestamp = begin_timestamp,
            .topology_epoch = participant.topology_epoch,
            .participants = participant_ids.items,
        });
        try begun.append(alloc, .{ .table_name = participant.table_name, .group_id = participant.group_id });
    }

    for (participants.items) |participant| {
        try worker.prepareGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .topology_epoch = participant.topology_epoch,
            .req = .{
                .writes = participant.writes.items,
                .deletes = participant.deletes.items,
                .transforms = participant.transforms.items,
                .predicates = participant.predicates.items,
                .foreign_key_parent_checks = participant.foreign_key_parent_checks.items,
                .foreign_key_parent_delete_checks = participant.foreign_key_parent_delete_checks.items,
                .foreign_key_conflict_checks = participant.foreign_key_conflict_checks.items,
                .foreign_key_set_null_children = participant.foreign_key_set_null_children.items,
                .foreign_key_cascade_children = participant.foreign_key_cascade_children.items,
                .foreign_key_action_schedules = participant.foreign_key_action_schedules.items,
                .foreign_key_ref_writes = participant.foreign_key_ref_writes.items,
                .foreign_key_ref_deletes = participant.foreign_key_ref_deletes.items,
                .foreign_key_externalized_parent_checks = participant.foreign_key_externalized_parent_checks.items,
                .unique_constraint_writes = participant.unique_constraint_writes.items,
                .unique_constraint_deletes = participant.unique_constraint_deletes.items,
                .foreign_key_constraint_timing_overrides = participant.foreign_key_constraint_timing_overrides.items,
            },
        });
    }

    for (participants.items) |participant| {
        try worker.resolveGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .status = .committed,
            .commit_version = commit_version,
        });
    }

    if (trace_writer) |tw| {
        tw.traceEvent(&.{ .name = "CommitTransaction", .txn_id = txn_id, .shard_id = "", .timestamp = commit_version });
    }

    return .{ .participant_count = participants.items.len };
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

pub fn executeForeignKeyActionPage(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    child_table_name: []const u8,
    owner_group_id: u64,
    action: []const u8,
    constraint_name: []const u8,
    parent_table_name: []const u8,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8,
    start_after_child_table: ?[]const u8,
    start_after_child_key: ?[]const u8,
    page_limit: usize,
    cascade_depth: u32,
    cascade_max_depth: u32,
    trace_writer: ?tracing.AntflyTraceWriter,
) !ForeignKeyActionPageExecution {
    if (page_limit == 0 or cascade_max_depth == 0 or cascade_depth > cascade_max_depth) return error.InvalidTxnRequest;
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, child_table_name)) orelse return error.TableNotFound;
    defer alloc.free(schema_json);
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational) return error.UnsupportedOperation;

    const foreign_key = findForeignKeyByName(runtime_schema.foreign_keys, constraint_name) orelse return error.UnsupportedOperation;
    if (!foreignKeyIsEnforced(foreign_key)) return error.UnsupportedOperation;
    const parent_catalog_table_name = foreignKeyParentCatalogTableName(child_table_name, runtime_schema.default_type, foreign_key);
    const parent_ref_table_name = foreign_key.parent_table;
    if (!std.mem.eql(u8, parent_catalog_table_name, parent_table_name) and !std.mem.eql(u8, parent_ref_table_name, parent_table_name)) return error.UnsupportedOperation;
    const canonical_action = foreignKeyActionPageCanonicalAction(action) orelse return error.UnsupportedOperation;
    if (std.mem.eql(u8, canonical_action, "set_null")) {
        if (foreign_key.on_delete != .set_null) return error.UnsupportedOperation;
    } else if (std.mem.eql(u8, canonical_action, "cascade")) {
        if (foreign_key.on_delete != .cascade) return error.UnsupportedOperation;
    } else if (std.mem.eql(u8, canonical_action, "update_set_null")) {
        if (foreign_key.on_update != .set_null) return error.UnsupportedOperation;
    } else if (std.mem.eql(u8, canonical_action, "update_cascade")) {
        if (foreign_key.on_update != .cascade or updated_parent_key == null) return error.UnsupportedOperation;
    } else {
        return error.UnsupportedOperation;
    }

    var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        foreign_key.name,
        parent_catalog_table_name,
        parent_key,
    );
    defer resolution.deinit(alloc);
    if (!resolution.configured) return error.UnsupportedOperation;
    var owner_group_found = false;
    for (resolution.groups) |group_id| {
        if (group_id == owner_group_id) {
            owner_group_found = true;
            break;
        }
    }
    if (!owner_group_found) return error.TopologyChanged;

    var page = try worker.foreignKeyRefChildrenPageGroup(alloc, owner_group_id, child_table_name, .{
        .constraint_name = constraint_name,
        .parent_table = parent_ref_table_name,
        .parent_key = parent_key,
        .limit = page_limit,
        .start_after_child_table = start_after_child_table,
        .start_after_child_key = start_after_child_key,
    });
    defer table_writes.freeForeignKeyRefChildrenPage(alloc, &page);

    var out = ForeignKeyActionPageExecution{
        .applied_children = page.children.len,
        .complete = page.complete,
    };
    errdefer out.deinit(alloc);
    if (!page.complete) {
        const next_child_table = page.next_child_table orelse return error.InvalidTxnRequest;
        const next_child_key = page.next_child_key orelse return error.InvalidTxnRequest;
        out.next_child_table = try alloc.dupe(u8, next_child_table);
        out.next_child_key = try alloc.dupe(u8, next_child_key);
    }

    if (page.children.len == 0) return out;

    var participants = std.ArrayListUnmanaged(ParticipantTxn).empty;
    defer {
        for (participants.items) |*participant| participant.deinit(alloc);
        participants.deinit(alloc);
    }
    const child_topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, child_table_name);
    if (child_topology_epoch == 0) return error.TableNotFound;
    try routeForeignKeyRefOwnerChildActionPage(.{ .route_actions = .{
        .alloc = alloc,
        .catalog = catalog,
        .participants = &participants,
        .child_table_name = child_table_name,
        .child_runtime_table = runtime_schema.default_type,
        .foreign_key = foreign_key,
        .action = canonical_action,
        .parent_key = parent_key,
        .updated_parent_key = updated_parent_key,
        .owner_group_id = owner_group_id,
        .owner_topology_epoch = resolution.topology_epoch,
        .child_topology_epoch = child_topology_epoch,
    } }, page.children);
    if (foreignKeyActionPageUpdatesChildren(canonical_action)) {
        try addForeignKeyUpdateActionPageDownstreamParticipants(
            alloc,
            catalog,
            worker,
            &participants,
            child_table_name,
            runtime_schema,
            foreign_key,
            canonical_action,
            parent_key,
            updated_parent_key,
            child_topology_epoch,
            page.children,
        );
    }
    if (std.mem.eql(u8, canonical_action, "cascade")) {
        if (cascade_depth >= cascade_max_depth and try tableHasMutatingForeignKeyDeleteDependents(alloc, catalog, child_table_name)) {
            return error.ForeignKeyCascadeDepthLimit;
        }
        const cascade_parent_keys = try alloc.alloc([]const u8, page.children.len);
        defer alloc.free(cascade_parent_keys);
        for (page.children, 0..) |child, i| cascade_parent_keys[i] = child.child_key;
        try addForeignKeyParentDeleteParticipants(
            alloc,
            catalog,
            worker,
            &participants,
            child_table_name,
            &.{},
            &.{},
            cascade_parent_keys,
            &.{},
            &.{},
            cascade_depth +| 1,
            cascade_max_depth,
        );
    }

    out.participant_count = (try executeParticipantTxns(
        alloc,
        worker,
        txn_id,
        begin_timestamp,
        commit_version,
        participants.items,
        trace_writer,
    )).participant_count;
    return out;
}

fn foreignKeyActionPageUpdatesChildren(action: []const u8) bool {
    return std.mem.eql(u8, action, "set_null") or
        std.mem.eql(u8, action, "update_set_null") or
        std.mem.eql(u8, action, "update_cascade");
}

fn addForeignKeyUpdateActionPageDownstreamParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    child_table_name: []const u8,
    child_runtime_schema: storage_schema.TableSchema,
    foreign_key: storage_schema.ForeignKey,
    action: []const u8,
    old_parent_key: []const u8,
    updated_parent_key: ?[]const u8,
    child_topology_epoch: u64,
    children: []const db_mod.types.ForeignKeyRefChild,
) !void {
    for (children) |child| {
        const child_group_id = (try table_catalog.resolveGroupForKeyPinned(
            alloc,
            catalog,
            child_table_name,
            child.child_key,
            child_topology_epoch,
        )) orelse return error.UnknownGroup;
        const child_participant = try ensureParticipantTxn(
            alloc,
            participants,
            child_table_name,
            child_group_id,
            child_topology_epoch,
        );
        var lookup = (try worker.lookupGroup(alloc, child_group_id, child_table_name, child.child_key)) orelse continue;
        defer lookup.deinit(alloc);
        try appendInjectedVersionPredicate(alloc, child_participant, child.child_key, lookup.version);

        const current_parent = (try foreignKeyParentReferenceFromJsonAlloc(
            alloc,
            catalog,
            child_table_name,
            child_runtime_schema.default_type,
            child_runtime_schema.relational_columns,
            foreign_key,
            lookup.json,
        )) orelse continue;
        defer alloc.free(current_parent);
        if (!std.mem.eql(u8, current_parent, old_parent_key)) continue;

        const child_row = try document_mapper.buildRelationalRowValueAlloc(alloc, lookup.json, child_runtime_schema.relational_columns);
        defer alloc.free(child_row);
        const rewritten_row = if (std.mem.eql(u8, action, "update_cascade")) blk: {
            const next_parent_key = updated_parent_key orelse return error.UnsupportedOperation;
            break :blk try relational_store.relationalRowWithForeignKeyColumnsFromParentKeyAlloc(alloc, child_row, next_parent_key, foreign_key);
        } else if (std.mem.eql(u8, action, "set_null") or std.mem.eql(u8, action, "update_set_null")) blk: {
            break :blk try relational_store.relationalRowWithoutColumnsAlloc(alloc, child_row, foreign_key.child_columns);
        } else return error.UnsupportedOperation;
        defer alloc.free(rewritten_row);
        const rewritten_json = try document_mapper.materializeRelationalRowValueAlloc(alloc, rewritten_row);
        defer alloc.free(rewritten_json);

        try addUniqueConstraintOwnerMutationsForWrite(
            alloc,
            catalog,
            participants,
            child_table_name,
            child_runtime_schema,
            child_runtime_schema.unique_constraints,
            child.child_key,
            lookup.json,
            rewritten_json,
        );
        try addForeignKeyParentUpdateParticipantsForWrite(
            alloc,
            catalog,
            participants,
            child_table_name,
            child_runtime_schema,
            lookup.json,
            rewritten_json,
            &.{},
        );
    }
}

fn executeParticipantTxns(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    txn_id: db_mod.types.TxnId,
    begin_timestamp: u64,
    commit_version: u64,
    participants: []const ParticipantTxn,
    trace_writer: ?tracing.AntflyTraceWriter,
) !ExecuteResult {
    var participant_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (participant_ids.items) |participant_id| alloc.free(@constCast(participant_id));
        participant_ids.deinit(alloc);
    }
    try participant_ids.ensureTotalCapacity(alloc, participants.len);
    for (participants) |participant| {
        const participant_id = try participantIdForGroup(alloc, participant.table_name, participant.group_id);
        participant_ids.appendAssumeCapacity(participant_id);
    }

    var begun = std.ArrayListUnmanaged(ParticipantRef).empty;
    defer begun.deinit(alloc);

    errdefer {
        if (trace_writer) |tw| {
            tw.traceEvent(&.{ .name = "AbortTransaction", .txn_id = txn_id, .shard_id = "" });
        }
        abortBegunRefs(alloc, worker, txn_id, commit_version, begun.items) catch {};
    }

    for (participants) |participant| {
        try worker.beginGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .begin_timestamp = begin_timestamp,
            .topology_epoch = participant.topology_epoch,
            .participants = participant_ids.items,
        });
        try begun.append(alloc, .{ .table_name = participant.table_name, .group_id = participant.group_id });
    }

    for (participants) |participant| {
        try worker.prepareGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .topology_epoch = participant.topology_epoch,
            .req = .{
                .writes = participant.writes.items,
                .deletes = participant.deletes.items,
                .transforms = participant.transforms.items,
                .predicates = participant.predicates.items,
                .foreign_key_parent_checks = participant.foreign_key_parent_checks.items,
                .foreign_key_parent_delete_checks = participant.foreign_key_parent_delete_checks.items,
                .foreign_key_conflict_checks = participant.foreign_key_conflict_checks.items,
                .foreign_key_set_null_children = participant.foreign_key_set_null_children.items,
                .foreign_key_cascade_children = participant.foreign_key_cascade_children.items,
                .foreign_key_action_schedules = participant.foreign_key_action_schedules.items,
                .foreign_key_ref_writes = participant.foreign_key_ref_writes.items,
                .foreign_key_ref_deletes = participant.foreign_key_ref_deletes.items,
                .foreign_key_externalized_parent_checks = participant.foreign_key_externalized_parent_checks.items,
                .unique_constraint_writes = participant.unique_constraint_writes.items,
                .unique_constraint_deletes = participant.unique_constraint_deletes.items,
                .foreign_key_constraint_timing_overrides = participant.foreign_key_constraint_timing_overrides.items,
            },
        });
    }

    for (participants) |participant| {
        try worker.resolveGroup(alloc, participant.group_id, participant.table_name, .{
            .txn_id = txn_id,
            .status = .committed,
            .commit_version = commit_version,
        });
    }

    if (trace_writer) |tw| {
        tw.traceEvent(&.{ .name = "CommitTransaction", .txn_id = txn_id, .shard_id = "", .timestamp = commit_version });
    }

    return .{ .participant_count = participants.len };
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
    const constraint_timing_overrides = try alloc.alloc(TableForeignKeyConstraintTimingOverrides, tables.len);
    defer alloc.free(constraint_timing_overrides);
    for (tables, 0..) |table, i| {
        constraint_timing_overrides[i] = .{
            .table_name = table.table_name,
            .overrides = table.foreign_key_constraint_timing_overrides,
        };
    }

    for (tables) |table| {
        const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table.table_name);
        if (topology_epoch == 0) return error.TableNotFound;

        try rejectUnsupportedDistributedForeignKeyTransforms(alloc, catalog, table.table_name, table.transforms);

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
        try addForeignKeyParentParticipants(alloc, catalog, worker, &participants, table.table_name, table.writes, table.transforms, table.predicates, constraint_timing_overrides);
        try addForeignKeyTransformParticipants(alloc, catalog, worker, &participants, table.table_name, table.writes, table.deletes, table.transforms, table.predicates, constraint_timing_overrides);
        try addForeignKeyChildDeleteParticipants(alloc, catalog, worker, &participants, table.table_name, table.deletes, table.transforms, table.predicates);
        try addForeignKeyParentUpdateParticipants(alloc, catalog, worker, &participants, table.table_name, table.writes, table.deletes, table.transforms, table.predicates, constraint_timing_overrides);
        try addForeignKeyParentDeleteParticipants(
            alloc,
            catalog,
            worker,
            &participants,
            table.table_name,
            table.writes,
            table.preimages,
            table.deletes,
            table.predicates,
            constraint_timing_overrides,
            0,
            foreign_key_action_default_cascade_max_depth,
        );
        try addUniqueConstraintOwnerParticipants(alloc, catalog, worker, &participants, table.table_name, table.writes, table.deletes, table.transforms, table.predicates);
    }
    try applyForeignKeyConstraintTimingOverridesToParticipants(alloc, &participants, constraint_timing_overrides);

    var participant_ids = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (participant_ids.items) |participant_id| alloc.free(@constCast(participant_id));
        participant_ids.deinit(alloc);
    }
    try participant_ids.ensureTotalCapacity(alloc, participants.items.len);
    for (participants.items) |participant| {
        const participant_id = try participantIdForGroup(alloc, participant.table_name, participant.group_id);
        participant_ids.appendAssumeCapacity(participant_id);
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
            .participants = participant_ids.items,
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
                .foreign_key_parent_checks = participant.foreign_key_parent_checks.items,
                .foreign_key_parent_delete_checks = participant.foreign_key_parent_delete_checks.items,
                .foreign_key_conflict_checks = participant.foreign_key_conflict_checks.items,
                .foreign_key_set_null_children = participant.foreign_key_set_null_children.items,
                .foreign_key_cascade_children = participant.foreign_key_cascade_children.items,
                .foreign_key_action_schedules = participant.foreign_key_action_schedules.items,
                .foreign_key_ref_writes = participant.foreign_key_ref_writes.items,
                .foreign_key_ref_deletes = participant.foreign_key_ref_deletes.items,
                .foreign_key_externalized_parent_checks = participant.foreign_key_externalized_parent_checks.items,
                .unique_constraint_writes = participant.unique_constraint_writes.items,
                .unique_constraint_deletes = participant.unique_constraint_deletes.items,
                .foreign_key_constraint_timing_overrides = participant.foreign_key_constraint_timing_overrides.items,
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

pub fn explainRoutedForeignKeyParentDelete(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    parent_table_name: []const u8,
    constraint_name: ?[]const u8,
    parent_key: []const u8,
) !?ForeignKeyDeleteExplain {
    const parent_group_id = (try table_catalog.resolveGroupForKey(alloc, catalog, parent_table_name, parent_key)) orelse return null;
    var parent_lookup = try worker.lookupGroup(alloc, parent_group_id, parent_table_name, parent_key);
    if (parent_lookup == null) return null;
    defer parent_lookup.?.deinit(alloc);

    var plan = relational_store.ForeignKeyDeletePlan{
        .exists = true,
        .allowed = true,
        .planned_row_deletes = 1,
    };

    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);

    var routed_owner_group_count: usize = 0;
    var saw_routed_constraint = false;
    var saw_unrouted_constraint = false;

    for (snapshot.tables) |table| {
        if (table.schema_json.len == 0) continue;
        var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, table.schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer storage_schema.freeSchema(alloc, runtime_schema);
        if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) continue;

        for (runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforcedImmediate(foreign_key)) continue;
            if (constraint_name) |name| {
                if (!std.mem.eql(u8, foreign_key.name, name)) continue;
            }
            if (!std.mem.eql(u8, foreignKeyParentCatalogTableName(table.name, runtime_schema.default_type, foreign_key), parent_table_name)) continue;
            if (!foreignKeySupportsDistributedParentDeleteCheck(foreign_key) and !foreignKeySupportsRoutedUniqueParentDeleteCheck(foreign_key)) return error.UnsupportedOperation;

            if (foreignKeyReferencesPrimaryKey(foreign_key)) {
                const routed = try explainRoutedPrimaryKeyForeignKeyParentDelete(
                    alloc,
                    catalog,
                    worker,
                    &plan,
                    &routed_owner_group_count,
                    table.name,
                    runtime_schema.default_type,
                    foreign_key,
                    parent_key,
                );
                if (routed) {
                    saw_routed_constraint = true;
                } else {
                    saw_unrouted_constraint = true;
                }
            } else {
                const routed = try explainRoutedUniqueForeignKeyParentDelete(
                    alloc,
                    catalog,
                    worker,
                    &plan,
                    &routed_owner_group_count,
                    table.name,
                    runtime_schema.default_type,
                    foreign_key,
                    parent_table_name,
                    parent_lookup.?.json,
                );
                if (routed) {
                    saw_routed_constraint = true;
                } else {
                    saw_unrouted_constraint = true;
                }
            }
        }
    }

    if (!saw_routed_constraint) return null;
    if (saw_unrouted_constraint) return error.UnsupportedOperation;
    return .{
        .parent_group_id = parent_group_id,
        .routed_owner_group_count = routed_owner_group_count,
        .plan = plan,
    };
}

fn explainRoutedPrimaryKeyForeignKeyParentDelete(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    plan: *relational_store.ForeignKeyDeletePlan,
    routed_owner_group_count: *usize,
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
) !bool {
    const parent_table_name = foreignKeyParentCatalogTableName(child_table_name, child_runtime_table, foreign_key);
    var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        foreign_key.name,
        parent_table_name,
        parent_key,
    );
    defer resolution.deinit(alloc);
    if (!resolution.configured) return false;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    try explainRoutedForeignKeyRefOwnerChildren(
        alloc,
        worker,
        plan,
        routed_owner_group_count,
        child_table_name,
        child_runtime_table,
        foreign_key,
        parent_key,
        resolution.groups,
    );
    return true;
}

fn explainRoutedUniqueForeignKeyParentDelete(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    plan: *relational_store.ForeignKeyDeletePlan,
    routed_owner_group_count: *usize,
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    parent_table_name: []const u8,
    parent_json: []const u8,
) !bool {
    if (foreignKeyReferencesPrimaryKey(foreign_key)) return false;
    const parent_schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, parent_table_name)) orelse return error.TableNotFound;
    defer alloc.free(parent_schema_json);
    var parsed_parent_schema = try schema_mod.parseValidatedTableSchema(alloc, parent_schema_json);
    defer parsed_parent_schema.deinit(alloc);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_parent_schema);
    defer storage_schema.freeSchema(alloc, parent_runtime_schema);
    if (parent_runtime_schema.storage_mode != .relational) return error.UnsupportedOperation;
    const parent_constraint = findParentTupleConstraintByColumns(parent_runtime_schema, foreign_key.parent_columns) orelse return error.UnsupportedOperation;
    const parent_row = try document_mapper.buildRelationalRowValueAlloc(alloc, parent_json, parent_runtime_schema.relational_columns);
    defer alloc.free(parent_row);
    const encoded_parent = try parentTupleValueAlloc(alloc, parent_row, parent_runtime_schema, parent_constraint);
    defer alloc.free(encoded_parent);

    var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        foreign_key.name,
        parent_table_name,
        encoded_parent,
    );
    defer resolution.deinit(alloc);
    if (!resolution.configured) return false;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    try explainRoutedForeignKeyRefOwnerChildren(
        alloc,
        worker,
        plan,
        routed_owner_group_count,
        child_table_name,
        child_runtime_table,
        foreign_key,
        encoded_parent,
        resolution.groups,
    );
    return true;
}

fn explainRoutedForeignKeyRefOwnerChildren(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    plan: *relational_store.ForeignKeyDeletePlan,
    routed_owner_group_count: *usize,
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    owner_groups: []const u64,
) !void {
    if (!foreignKeyDeleteActionSupported(foreign_key)) return error.UnsupportedOperation;
    routed_owner_group_count.* +|= owner_groups.len;
    for (owner_groups) |group_id| {
        try forEachForeignKeyRefOwnerChildPage(alloc, worker, group_id, child_table_name, foreign_key.name, foreign_key.parent_table, parent_key, .{
            .ctx = .{ .explain = .{
                .plan = plan,
                .child_runtime_table = child_runtime_table,
                .on_delete = foreign_key.on_delete,
            } },
            .callback = explainRoutedForeignKeyRefOwnerChildPage,
        });
    }
}

const foreign_key_ref_owner_page_limit: usize = 4096;

const ForeignKeyRefOwnerPageCallback = struct {
    ctx: Context,
    callback: *const fn (ctx: Context, children: []const db_mod.types.ForeignKeyRefChild) anyerror!void,

    const Context = union(enum) {
        explain: ExplainContext,
        route_actions: RouteActionsContext,
    };

    const ExplainContext = struct {
        plan: *relational_store.ForeignKeyDeletePlan,
        child_runtime_table: []const u8,
        on_delete: storage_schema.ForeignKeyAction,
    };

    const RouteActionsContext = struct {
        alloc: std.mem.Allocator,
        catalog: table_catalog.CatalogSource,
        participants: *std.ArrayListUnmanaged(ParticipantTxn),
        child_table_name: []const u8,
        child_runtime_table: []const u8,
        foreign_key: storage_schema.ForeignKey,
        action: []const u8,
        parent_key: []const u8,
        updated_parent_key: ?[]const u8,
        owner_group_id: u64,
        owner_topology_epoch: u64,
        child_topology_epoch: u64,
    };
};

fn forEachForeignKeyRefOwnerChildPage(
    alloc: std.mem.Allocator,
    worker: ParticipantWorker,
    group_id: u64,
    child_table_name: []const u8,
    constraint_name: []const u8,
    parent_table_name: []const u8,
    parent_key: []const u8,
    callback: ForeignKeyRefOwnerPageCallback,
) !void {
    var start_after_child_table: ?[]u8 = null;
    var start_after_child_key: ?[]u8 = null;
    defer {
        if (start_after_child_table) |value| alloc.free(value);
        if (start_after_child_key) |value| alloc.free(value);
    }
    while (true) {
        var page = try worker.foreignKeyRefChildrenPageGroup(alloc, group_id, child_table_name, .{
            .constraint_name = constraint_name,
            .parent_table = parent_table_name,
            .parent_key = parent_key,
            .limit = foreign_key_ref_owner_page_limit,
            .start_after_child_table = start_after_child_table,
            .start_after_child_key = start_after_child_key,
        });
        defer table_writes.freeForeignKeyRefChildrenPage(alloc, &page);

        try callback.callback(callback.ctx, page.children);

        if (page.complete) break;
        const next_child_table = page.next_child_table orelse return error.InvalidTxnRequest;
        const next_child_key = page.next_child_key orelse return error.InvalidTxnRequest;
        const next = blk: {
            const table_copy = try alloc.dupe(u8, next_child_table);
            errdefer alloc.free(table_copy);
            const key_copy = try alloc.dupe(u8, next_child_key);
            break :blk .{ .table = table_copy, .key = key_copy };
        };
        if (start_after_child_table) |value| alloc.free(value);
        if (start_after_child_key) |value| alloc.free(value);
        start_after_child_table = next.table;
        start_after_child_key = next.key;
    }
}

fn explainRoutedForeignKeyRefOwnerChildPage(
    ctx: ForeignKeyRefOwnerPageCallback.Context,
    children: []const db_mod.types.ForeignKeyRefChild,
) !void {
    const explain = switch (ctx) {
        .explain => |value| value,
        else => return error.InvalidTxnRequest,
    };
    for (children) |child| {
        if (!std.mem.eql(u8, child.child_table, explain.child_runtime_table)) return error.TopologyChanged;
    }
    switch (explain.on_delete) {
        .restrict, .no_action => if (children.len > 0) {
            explain.plan.allowed = false;
            if (explain.plan.block_reason == .none) explain.plan.block_reason = .restrict;
        },
        .set_null => {
            explain.plan.planned_set_null_updates +|= @intCast(children.len);
            explain.plan.planned_writes +|= @intCast(children.len);
        },
        .cascade => {
            explain.plan.planned_cascade_deletes +|= @intCast(children.len);
            explain.plan.planned_row_deletes +|= @intCast(children.len);
        },
    }
}

fn routeForeignKeyRefOwnerChildActionPage(
    ctx: ForeignKeyRefOwnerPageCallback.Context,
    children: []const db_mod.types.ForeignKeyRefChild,
) !void {
    const route = switch (ctx) {
        .route_actions => |value| value,
        else => return error.InvalidTxnRequest,
    };
    for (children) |child| {
        if (!std.mem.eql(u8, child.child_table, route.child_runtime_table)) return error.TopologyChanged;
        const owner_for_child = try ensureParticipantTxn(
            route.alloc,
            route.participants,
            route.child_table_name,
            route.owner_group_id,
            route.owner_topology_epoch,
        );
        try appendForeignKeyRefMutation(route.alloc, &owner_for_child.foreign_key_ref_deletes, route.foreign_key, route.child_runtime_table, child.child_key, route.parent_key);

        const child_group_id = (try table_catalog.resolveGroupForKeyPinned(
            route.alloc,
            route.catalog,
            route.child_table_name,
            child.child_key,
            route.child_topology_epoch,
        )) orelse return error.UnknownGroup;
        const child_participant = try ensureParticipantTxn(
            route.alloc,
            route.participants,
            route.child_table_name,
            child_group_id,
            route.child_topology_epoch,
        );
        if (std.mem.eql(u8, route.action, "set_null")) {
            try appendForeignKeySetNullChildAction(route.alloc, child_participant, route.foreign_key, route.parent_key, child.child_key, .delete);
        } else if (std.mem.eql(u8, route.action, "update_set_null")) {
            try appendForeignKeySetNullChildAction(route.alloc, child_participant, route.foreign_key, route.parent_key, child.child_key, .update);
        } else if (std.mem.eql(u8, route.action, "cascade")) {
            try appendForeignKeyCascadeChildAction(route.alloc, child_participant, route.foreign_key, route.parent_key, null, child.child_key, .delete);
        } else if (std.mem.eql(u8, route.action, "update_cascade")) {
            const updated_parent_key = route.updated_parent_key orelse return error.UnsupportedOperation;
            try appendForeignKeyCascadeChildAction(route.alloc, child_participant, route.foreign_key, route.parent_key, updated_parent_key, child.child_key, .update);
        } else {
            return error.UnsupportedOperation;
        }
    }
}

const ParticipantTxn = struct {
    table_name: []u8,
    group_id: u64,
    topology_epoch: u64,
    writes: std.ArrayListUnmanaged(db_mod.types.TransactionWrite) = .empty,
    deletes: std.ArrayListUnmanaged([]const u8) = .empty,
    transforms: std.ArrayListUnmanaged(db_mod.types.DocumentTransform) = .empty,
    predicates: std.ArrayListUnmanaged(db_mod.types.TransactionVersionPredicate) = .empty,
    owned_predicate_keys: std.ArrayListUnmanaged([]u8) = .empty,
    foreign_key_parent_checks: std.ArrayListUnmanaged(db_mod.types.ForeignKeyParentCheck) = .empty,
    foreign_key_parent_delete_checks: std.ArrayListUnmanaged(db_mod.types.ForeignKeyParentDeleteCheck) = .empty,
    foreign_key_conflict_checks: std.ArrayListUnmanaged(db_mod.types.ForeignKeyConflictCheck) = .empty,
    foreign_key_set_null_children: std.ArrayListUnmanaged(db_mod.types.ForeignKeySetNullChildAction) = .empty,
    foreign_key_cascade_children: std.ArrayListUnmanaged(db_mod.types.ForeignKeyCascadeChildAction) = .empty,
    foreign_key_action_schedules: std.ArrayListUnmanaged(db_mod.types.ForeignKeyActionScheduleMutation) = .empty,
    foreign_key_ref_writes: std.ArrayListUnmanaged(db_mod.types.ForeignKeyRefMutation) = .empty,
    foreign_key_ref_deletes: std.ArrayListUnmanaged(db_mod.types.ForeignKeyRefMutation) = .empty,
    foreign_key_externalized_parent_checks: std.ArrayListUnmanaged(db_mod.types.ForeignKeyParentCheck) = .empty,
    unique_constraint_writes: std.ArrayListUnmanaged(db_mod.types.UniqueConstraintMutation) = .empty,
    unique_constraint_deletes: std.ArrayListUnmanaged(db_mod.types.UniqueConstraintMutation) = .empty,
    foreign_key_constraint_timing_overrides: std.ArrayListUnmanaged(db_mod.types.ForeignKeyConstraintTimingOverride) = .empty,

    fn deinit(self: *ParticipantTxn, alloc: std.mem.Allocator) void {
        alloc.free(self.table_name);
        for (self.foreign_key_parent_checks.items) |check| freeForeignKeyParentCheckFields(alloc, check);
        self.foreign_key_parent_checks.deinit(alloc);
        for (self.foreign_key_parent_delete_checks.items) |check| {
            alloc.free(@constCast(check.constraint_name));
            alloc.free(@constCast(check.parent_table));
            alloc.free(@constCast(check.parent_key));
        }
        self.foreign_key_parent_delete_checks.deinit(alloc);
        for (self.foreign_key_conflict_checks.items) |check| {
            alloc.free(@constCast(check.constraint_name));
            alloc.free(@constCast(check.parent_table));
            alloc.free(@constCast(check.parent_key));
        }
        self.foreign_key_conflict_checks.deinit(alloc);
        for (self.foreign_key_set_null_children.items) |action| {
            alloc.free(@constCast(action.constraint_name));
            alloc.free(@constCast(action.parent_table));
            alloc.free(@constCast(action.parent_key));
            alloc.free(@constCast(action.child_key));
        }
        self.foreign_key_set_null_children.deinit(alloc);
        for (self.foreign_key_cascade_children.items) |action| {
            alloc.free(@constCast(action.constraint_name));
            alloc.free(@constCast(action.parent_table));
            alloc.free(@constCast(action.parent_key));
            if (action.updated_parent_key) |value| alloc.free(@constCast(value));
            alloc.free(@constCast(action.child_key));
        }
        self.foreign_key_cascade_children.deinit(alloc);
        for (self.foreign_key_action_schedules.items) |schedule| {
            freeForeignKeyActionScheduleMutationFields(alloc, schedule);
        }
        self.foreign_key_action_schedules.deinit(alloc);
        for (self.foreign_key_ref_writes.items) |mutation| freeForeignKeyRefMutationFields(alloc, mutation);
        self.foreign_key_ref_writes.deinit(alloc);
        for (self.foreign_key_ref_deletes.items) |mutation| freeForeignKeyRefMutationFields(alloc, mutation);
        self.foreign_key_ref_deletes.deinit(alloc);
        for (self.foreign_key_externalized_parent_checks.items) |check| freeForeignKeyParentCheckFields(alloc, check);
        self.foreign_key_externalized_parent_checks.deinit(alloc);
        for (self.unique_constraint_writes.items) |mutation| freeUniqueConstraintMutationFields(alloc, mutation);
        self.unique_constraint_writes.deinit(alloc);
        for (self.unique_constraint_deletes.items) |mutation| freeUniqueConstraintMutationFields(alloc, mutation);
        self.unique_constraint_deletes.deinit(alloc);
        for (self.foreign_key_constraint_timing_overrides.items) |override| freeForeignKeyConstraintTimingOverrideFields(alloc, override);
        self.foreign_key_constraint_timing_overrides.deinit(alloc);
        self.writes.deinit(alloc);
        self.deletes.deinit(alloc);
        self.transforms.deinit(alloc);
        for (self.owned_predicate_keys.items) |key| alloc.free(key);
        self.owned_predicate_keys.deinit(alloc);
        self.predicates.deinit(alloc);
        self.* = undefined;
    }
};

fn freeForeignKeyParentCheckFields(alloc: std.mem.Allocator, check: db_mod.types.ForeignKeyParentCheck) void {
    alloc.free(@constCast(check.constraint_name));
    alloc.free(@constCast(check.child_table));
    alloc.free(@constCast(check.child_key));
    alloc.free(@constCast(check.parent_table));
    alloc.free(@constCast(check.parent_key));
    if (check.parent_constraint_name) |name| alloc.free(@constCast(name));
    if (check.child_period_start_json) |json| alloc.free(@constCast(json));
    if (check.child_period_end_json) |json| alloc.free(@constCast(json));
}

fn freeForeignKeyConstraintTimingOverrideFields(alloc: std.mem.Allocator, override: db_mod.types.ForeignKeyConstraintTimingOverride) void {
    alloc.free(@constCast(override.constraint_name));
}

fn freeForeignKeyRefMutationFields(alloc: std.mem.Allocator, mutation: db_mod.types.ForeignKeyRefMutation) void {
    alloc.free(@constCast(mutation.constraint_name));
    alloc.free(@constCast(mutation.parent_table));
    alloc.free(@constCast(mutation.parent_key));
    alloc.free(@constCast(mutation.child_table));
    alloc.free(@constCast(mutation.child_key));
}

fn freeForeignKeyActionScheduleMutationFields(alloc: std.mem.Allocator, schedule: db_mod.types.ForeignKeyActionScheduleMutation) void {
    alloc.free(@constCast(schedule.schedule_id));
    alloc.free(@constCast(schedule.action_job_id));
    alloc.free(@constCast(schedule.action));
    alloc.free(@constCast(schedule.worker_id));
    alloc.free(@constCast(schedule.constraint_name));
    alloc.free(@constCast(schedule.parent_table));
    alloc.free(@constCast(schedule.parent_key));
    if (schedule.updated_parent_key) |value| alloc.free(@constCast(value));
}

fn freeUniqueConstraintMutationFields(alloc: std.mem.Allocator, mutation: db_mod.types.UniqueConstraintMutation) void {
    alloc.free(@constCast(mutation.constraint_name));
    alloc.free(@constCast(mutation.encoded_value));
    alloc.free(@constCast(mutation.owner_key));
    if (mutation.temporal_start) |value| alloc.free(@constCast(value));
    if (mutation.temporal_end) |value| alloc.free(@constCast(value));
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
    const owned_table_name = try alloc.dupe(u8, table_name);
    errdefer alloc.free(owned_table_name);
    try grouped.append(alloc, .{ .table_name = owned_table_name, .group_id = group_id, .topology_epoch = topology_epoch });
    return &grouped.items[grouped.items.len - 1];
}

fn applyForeignKeyConstraintTimingOverridesToParticipants(
    alloc: std.mem.Allocator,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    override_tables: []const TableForeignKeyConstraintTimingOverrides,
) !void {
    for (participants.items) |*participant| {
        for (override_tables) |table_overrides| {
            if (!std.mem.eql(u8, participant.table_name, table_overrides.table_name)) continue;
            for (table_overrides.overrides) |override| {
                try appendForeignKeyConstraintTimingOverride(alloc, participant, override);
            }
        }
    }
}

fn appendForeignKeyConstraintTimingOverride(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    override: db_mod.types.ForeignKeyConstraintTimingOverride,
) !void {
    if (override.constraint_name.len == 0) return error.InvalidTxnRequest;
    for (participant.foreign_key_constraint_timing_overrides.items) |existing| {
        if (std.mem.eql(u8, existing.constraint_name, override.constraint_name)) {
            if (existing.timing != override.timing) return error.InvalidTxnRequest;
            return;
        }
    }
    const constraint_name = try alloc.dupe(u8, override.constraint_name);
    errdefer alloc.free(constraint_name);
    try participant.foreign_key_constraint_timing_overrides.append(alloc, .{
        .constraint_name = constraint_name,
        .timing = override.timing,
    });
}

fn effectiveForeignKeyParentCheckTiming(
    child_table_name: []const u8,
    foreign_key: storage_schema.ForeignKey,
    override_tables: []const TableForeignKeyConstraintTimingOverrides,
) db_mod.types.ForeignKeyParentCheck.Timing {
    for (override_tables) |table_overrides| {
        if (!std.mem.eql(u8, child_table_name, table_overrides.table_name)) continue;
        for (table_overrides.overrides) |override| {
            if (std.mem.eql(u8, foreign_key.name, override.constraint_name)) return override.timing;
        }
    }
    return foreignKeyParentCheckTiming(foreign_key);
}

fn appendInjectedVersionPredicate(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    key: []const u8,
    expected_version: u64,
) !void {
    for (participant.predicates.items) |predicate| {
        if (std.mem.eql(u8, predicate.key, key)) {
            if (predicate.expected_version != expected_version) return error.VersionConflict;
            return;
        }
    }
    const owned_key = try alloc.dupe(u8, key);
    errdefer alloc.free(owned_key);
    try participant.predicates.append(alloc, .{ .key = owned_key, .expected_version = expected_version });
    try participant.owned_predicate_keys.append(alloc, owned_key);
}

fn foreignKeyChildParticipantForKey(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    child_table_name: []const u8,
    child_key: []const u8,
) !*ParticipantTxn {
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, child_table_name);
    if (topology_epoch == 0) return error.TableNotFound;
    const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, child_table_name, child_key, topology_epoch)) orelse return error.UnknownGroup;
    return try ensureParticipantTxn(alloc, participants, child_table_name, group_id, topology_epoch);
}

fn addForeignKeyParentParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite,
    transforms: []const db_mod.types.DocumentTransform,
    predicates: []const db_mod.types.TransactionVersionPredicate,
    constraint_timing_overrides: []const TableForeignKeyConstraintTimingOverrides,
) !void {
    if (writes.len == 0) return;
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) return;

    for (writes) |write| {
        if (try keyHasForeignKeyReferenceTransform(alloc, runtime_schema.foreign_keys, write.key, transforms)) continue;
        for (runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            const maybe_parent_key = try foreignKeyParentReferenceFromJsonAlloc(alloc, catalog, table_name, runtime_schema.default_type, runtime_schema.relational_columns, foreign_key, write.value);
            defer if (maybe_parent_key) |parent_key| alloc.free(parent_key);
            if (maybe_parent_key) |parent_key| {
                var child_period_bounds = try foreignKeyChildPeriodBoundsFromJsonAlloc(alloc, runtime_schema.periods, foreign_key, write.value);
                defer child_period_bounds.deinit(alloc);
                try addForeignKeyParentCheckParticipant(alloc, catalog, participants, table_name, runtime_schema.default_type, foreign_key, write.key, parent_key, child_period_bounds.start_json, child_period_bounds.end_json, effectiveForeignKeyParentCheckTiming(table_name, foreign_key, constraint_timing_overrides));
            }

            if (!try foreignKeyRefOwnersConfiguredForConstraint(alloc, catalog, table_name, runtime_schema.default_type, foreign_key, maybe_parent_key orelse "")) continue;
            if (writeHasInsertOnlyPredicate(predicates, write.key)) {
                if (maybe_parent_key) |parent_key| {
                    try addForeignKeyRefOwnerWriteParticipant(alloc, catalog, participants, table_name, runtime_schema.default_type, foreign_key, write.key, parent_key);
                }
                continue;
            }

            var old_row = try lookupWriteRowForConstraintProof(alloc, catalog, worker, participants, table_name, write.key, predicates);
            defer if (old_row) |*row| row.deinit(alloc);
            const maybe_old_parent_key = if (old_row) |old| try foreignKeyParentReferenceFromJsonAlloc(alloc, catalog, table_name, runtime_schema.default_type, runtime_schema.relational_columns, foreign_key, old.json) else null;
            defer if (maybe_old_parent_key) |old_parent_key| alloc.free(old_parent_key);

            if (maybe_old_parent_key) |old_parent_key| {
                if (maybe_parent_key == null or !std.mem.eql(u8, old_parent_key, maybe_parent_key.?)) {
                    try addForeignKeyRefOwnerDeleteParticipant(alloc, catalog, participants, table_name, runtime_schema.default_type, foreign_key, write.key, old_parent_key);
                }
            }
            if (maybe_parent_key) |parent_key| {
                if (maybe_old_parent_key == null or !std.mem.eql(u8, maybe_old_parent_key.?, parent_key)) {
                    try addForeignKeyRefOwnerWriteParticipant(alloc, catalog, participants, table_name, runtime_schema.default_type, foreign_key, write.key, parent_key);
                }
            }
        }
    }
}

fn writeHasInsertOnlyPredicate(predicates: []const db_mod.types.TransactionVersionPredicate, key: []const u8) bool {
    for (predicates) |predicate| {
        if (predicate.expected_version == 0 and std.mem.eql(u8, predicate.key, key)) return true;
    }
    return false;
}

fn findVersionPredicate(predicates: []const db_mod.types.TransactionVersionPredicate, key: []const u8) ?u64 {
    for (predicates) |predicate| {
        if (std.mem.eql(u8, predicate.key, key)) return predicate.expected_version;
    }
    return null;
}

fn findWriteValueForKey(writes: []const db_mod.types.TransactionWrite, key: []const u8) ?[]const u8 {
    var value: ?[]const u8 = null;
    for (writes) |write| {
        if (std.mem.eql(u8, write.key, key)) value = write.value;
    }
    return value;
}

fn deleteContainsKey(deletes: []const []const u8, key: []const u8) bool {
    for (deletes) |delete_key| {
        if (std.mem.eql(u8, delete_key, key)) return true;
    }
    return false;
}

fn transformKeySeenBefore(transforms: []const db_mod.types.DocumentTransform, index: usize) bool {
    const key = transforms[index].key;
    for (transforms[0..index]) |previous| {
        if (std.mem.eql(u8, previous.key, key)) return true;
    }
    return false;
}

fn keyHasForeignKeyReferenceTransform(
    alloc: std.mem.Allocator,
    foreign_keys: []const storage_schema.ForeignKey,
    key: []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !bool {
    for (transforms) |transform| {
        if (!std.mem.eql(u8, transform.key, key)) continue;
        for (foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (try foreignKeyTransformTouchesReference(alloc, foreign_key, transform)) return true;
        }
    }
    return false;
}

fn keyTouchesForeignKeyReferenceTransform(
    alloc: std.mem.Allocator,
    foreign_key: storage_schema.ForeignKey,
    key: []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !bool {
    for (transforms) |transform| {
        if (!std.mem.eql(u8, transform.key, key)) continue;
        if (try foreignKeyTransformTouchesReference(alloc, foreign_key, transform)) return true;
    }
    return false;
}

fn lookupVersionedChildRowForForeignKeyPlanning(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    table_name: []const u8,
    key: []const u8,
    predicates: []const db_mod.types.TransactionVersionPredicate,
) !?table_reads.LookupResponse {
    const expected_version = findVersionPredicate(predicates, key) orelse return error.UnsupportedOperation;
    if (expected_version == 0) return null;
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);
    if (topology_epoch == 0) return error.TableNotFound;
    const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, key, topology_epoch)) orelse return error.UnknownGroup;
    var row = (try worker.lookupGroup(alloc, group_id, table_name, key)) orelse return error.VersionConflict;
    errdefer row.deinit(alloc);
    if (row.version != expected_version) return error.VersionConflict;
    return row;
}

fn lookupDeleteRowForConstraintProof(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    key: []const u8,
    predicates: []const db_mod.types.TransactionVersionPredicate,
) !?table_reads.LookupResponse {
    if (findVersionPredicate(predicates, key) != null) {
        return try lookupVersionedChildRowForForeignKeyPlanning(alloc, catalog, worker, table_name, key, predicates);
    }
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);
    if (topology_epoch == 0) return error.TableNotFound;
    const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, key, topology_epoch)) orelse return error.UnknownGroup;
    var row = (try worker.lookupGroup(alloc, group_id, table_name, key)) orelse return null;
    errdefer row.deinit(alloc);
    const participant = try ensureParticipantTxn(alloc, participants, table_name, group_id, topology_epoch);
    try appendInjectedVersionPredicate(alloc, participant, key, row.version);
    return row;
}

fn lookupWriteRowForConstraintProof(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    key: []const u8,
    predicates: []const db_mod.types.TransactionVersionPredicate,
) !?table_reads.LookupResponse {
    if (findVersionPredicate(predicates, key) != null) {
        return try lookupVersionedChildRowForForeignKeyPlanning(alloc, catalog, worker, table_name, key, predicates);
    }
    const topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, table_name);
    if (topology_epoch == 0) return error.TableNotFound;
    const group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, table_name, key, topology_epoch)) orelse return error.UnknownGroup;
    const participant = try ensureParticipantTxn(alloc, participants, table_name, group_id, topology_epoch);
    var row = (try worker.lookupGroup(alloc, group_id, table_name, key)) orelse {
        try appendInjectedVersionPredicate(alloc, participant, key, 0);
        return null;
    };
    errdefer row.deinit(alloc);
    try appendInjectedVersionPredicate(alloc, participant, key, row.version);
    return row;
}

fn foreignKeyRefOwnersConfiguredForConstraint(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
) !bool {
    const parent_table_name = foreignKeyParentCatalogTableName(child_table_name, child_runtime_table, foreign_key);
    var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        foreign_key.name,
        parent_table_name,
        parent_key,
    );
    defer resolution.deinit(alloc);
    return resolution.configured;
}

fn addForeignKeyTransformParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
    predicates: []const db_mod.types.TransactionVersionPredicate,
    constraint_timing_overrides: []const TableForeignKeyConstraintTimingOverrides,
) !void {
    if (transforms.len == 0) return;
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) return;

    for (transforms, 0..) |transform, transform_index| {
        if (transformKeySeenBefore(transforms, transform_index)) continue;
        if (!try keyHasForeignKeyReferenceTransform(alloc, runtime_schema.foreign_keys, transform.key, transforms)) continue;

        var old_row = try lookupWriteRowForConstraintProof(alloc, catalog, worker, participants, table_name, transform.key, predicates);
        defer if (old_row) |*row| row.deinit(alloc);

        var final_owned: ?[]u8 = null;
        defer if (final_owned) |body| alloc.free(body);
        var final_json: ?[]const u8 = blk: {
            if (deleteContainsKey(deletes, transform.key)) break :blk null;
            if (findWriteValueForKey(writes, transform.key)) |write_value| break :blk write_value;
            if (old_row) |row| break :blk row.json;
            break :blk null;
        };
        for (transforms) |candidate| {
            if (!std.mem.eql(u8, candidate.key, transform.key)) continue;
            const resolved = try db_mod.transform.resolveDocumentTransform(alloc, final_json, candidate) orelse continue;
            if (final_owned) |previous| alloc.free(previous);
            final_owned = resolved;
            final_json = resolved;
        }
        if (final_json == null and old_row == null) continue;

        for (runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!try keyTouchesForeignKeyReferenceTransform(alloc, foreign_key, transform.key, transforms)) continue;

            const maybe_old_parent_key = if (old_row) |row| try foreignKeyParentReferenceFromJsonAlloc(alloc, catalog, table_name, runtime_schema.default_type, runtime_schema.relational_columns, foreign_key, row.json) else null;
            defer if (maybe_old_parent_key) |old_parent_key| alloc.free(old_parent_key);
            const maybe_new_parent_key = if (final_json) |body| try foreignKeyParentReferenceFromJsonAlloc(alloc, catalog, table_name, runtime_schema.default_type, runtime_schema.relational_columns, foreign_key, body) else null;
            defer if (maybe_new_parent_key) |new_parent_key| alloc.free(new_parent_key);

            if (maybe_new_parent_key) |parent_key| {
                var child_period_bounds = try foreignKeyChildPeriodBoundsFromJsonAlloc(alloc, runtime_schema.periods, foreign_key, final_json.?);
                defer child_period_bounds.deinit(alloc);
                try addForeignKeyParentCheckParticipant(alloc, catalog, participants, table_name, runtime_schema.default_type, foreign_key, transform.key, parent_key, child_period_bounds.start_json, child_period_bounds.end_json, effectiveForeignKeyParentCheckTiming(table_name, foreign_key, constraint_timing_overrides));
            }

            const owner_configured = (maybe_old_parent_key != null and try foreignKeyRefOwnersConfiguredForConstraint(alloc, catalog, table_name, runtime_schema.default_type, foreign_key, maybe_old_parent_key.?)) or
                (maybe_new_parent_key != null and try foreignKeyRefOwnersConfiguredForConstraint(alloc, catalog, table_name, runtime_schema.default_type, foreign_key, maybe_new_parent_key.?));
            if (!owner_configured) continue;

            if (maybe_old_parent_key) |old_parent_key| {
                if (maybe_new_parent_key == null or !std.mem.eql(u8, old_parent_key, maybe_new_parent_key.?)) {
                    try addForeignKeyRefOwnerDeleteParticipant(alloc, catalog, participants, table_name, runtime_schema.default_type, foreign_key, transform.key, old_parent_key);
                }
            }
            if (maybe_new_parent_key) |new_parent_key| {
                if (maybe_old_parent_key == null or !std.mem.eql(u8, maybe_old_parent_key.?, new_parent_key)) {
                    try addForeignKeyRefOwnerWriteParticipant(alloc, catalog, participants, table_name, runtime_schema.default_type, foreign_key, transform.key, new_parent_key);
                }
            }
        }
    }
}

fn addForeignKeyParentUpdateParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    parent_table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
    predicates: []const db_mod.types.TransactionVersionPredicate,
    constraint_timing_overrides: []const TableForeignKeyConstraintTimingOverrides,
) !void {
    if (writes.len == 0 and transforms.len == 0) return;
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, parent_table_name)) orelse return;
    defer alloc.free(schema_json);
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, parent_runtime_schema);
    if (parent_runtime_schema.storage_mode != .relational or (parent_runtime_schema.primary_key == null and parent_runtime_schema.unique_constraints.len == 0)) return;
    if (!try catalogHasForeignKeysReferencingParentUniqueConstraints(
        alloc,
        catalog,
        parent_table_name,
        parent_runtime_schema.primary_key,
        parent_runtime_schema.unique_constraints,
    )) return;

    for (writes) |write| {
        if (deleteContainsKey(deletes, write.key)) continue;
        if (try keyHasRuntimeOwnerConstraintTransform(alloc, parent_runtime_schema, write.key, transforms)) continue;
        var old_row = try lookupWriteRowForConstraintProof(alloc, catalog, worker, participants, parent_table_name, write.key, predicates);
        defer if (old_row) |*row| row.deinit(alloc);
        const old = old_row orelse continue;
        try addForeignKeyParentUpdateParticipantsForWrite(
            alloc,
            catalog,
            participants,
            parent_table_name,
            parent_runtime_schema,
            old.json,
            write.value,
            constraint_timing_overrides,
        );
    }

    for (transforms, 0..) |transform, transform_index| {
        if (transformKeySeenBefore(transforms, transform_index)) continue;
        if (!try keyHasRuntimeOwnerConstraintTransform(alloc, parent_runtime_schema, transform.key, transforms)) continue;

        var old_row = try lookupWriteRowForConstraintProof(alloc, catalog, worker, participants, parent_table_name, transform.key, predicates);
        defer if (old_row) |*row| row.deinit(alloc);
        const old = old_row orelse continue;

        var final_owned: ?[]u8 = null;
        defer if (final_owned) |body| alloc.free(body);
        var final_json: ?[]const u8 = blk: {
            if (deleteContainsKey(deletes, transform.key)) break :blk null;
            if (findWriteValueForKey(writes, transform.key)) |write_value| break :blk write_value;
            break :blk old.json;
        };
        for (transforms) |candidate| {
            if (!std.mem.eql(u8, candidate.key, transform.key)) continue;
            const resolved = try db_mod.transform.resolveDocumentTransform(alloc, final_json, candidate) orelse continue;
            if (final_owned) |previous| alloc.free(previous);
            final_owned = resolved;
            final_json = resolved;
        }
        const final = final_json orelse continue;
        try addForeignKeyParentUpdateParticipantsForWrite(
            alloc,
            catalog,
            participants,
            parent_table_name,
            parent_runtime_schema,
            old.json,
            final,
            constraint_timing_overrides,
        );
    }
}

fn catalogHasForeignKeysReferencingParentUniqueConstraints(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    parent_table_name: []const u8,
    parent_primary_key: ?storage_schema.PrimaryKey,
    parent_constraints: []const storage_schema.UniqueConstraint,
) !bool {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    for (snapshot.tables) |table| {
        if (table.schema_json.len == 0) continue;
        var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, table.schema_json);
        defer parsed_schema.deinit(alloc);
        const child_runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer storage_schema.freeSchema(alloc, child_runtime_schema);
        if (child_runtime_schema.storage_mode != .relational or child_runtime_schema.foreign_keys.len == 0) continue;
        for (child_runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!std.mem.eql(u8, foreignKeyParentCatalogTableName(table.name, child_runtime_schema.default_type, foreign_key), parent_table_name)) continue;
            if (parent_primary_key) |primary_key| {
                if (stringSlicesEqual(foreign_key.parent_columns, primary_key.columns)) return true;
            }
            for (parent_constraints) |constraint| {
                if (!uniqueConstraintCanBackForeignKey(constraint)) continue;
                if (stringSlicesEqual(foreign_key.parent_columns, constraint.columns)) return true;
            }
        }
    }
    return false;
}

fn addForeignKeyParentUpdateParticipantsForWrite(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    parent_table_name: []const u8,
    parent_runtime_schema: storage_schema.TableSchema,
    old_json: []const u8,
    new_json: []const u8,
    constraint_timing_overrides: []const TableForeignKeyConstraintTimingOverrides,
) !void {
    const old_row = try document_mapper.buildRelationalRowValueAlloc(alloc, old_json, parent_runtime_schema.relational_columns);
    defer alloc.free(old_row);
    const new_row = try document_mapper.buildRelationalRowValueAlloc(alloc, new_json, parent_runtime_schema.relational_columns);
    defer alloc.free(new_row);

    if (parent_runtime_schema.primary_key) |primary_key| {
        const constraint = primaryKeyAsUniqueConstraint(primary_key);
        const old_value = try relational_store.primaryKeyTupleValueAlloc(alloc, old_row, primary_key);
        defer alloc.free(old_value);
        const new_value = try relational_store.primaryKeyTupleValueAlloc(alloc, new_row, primary_key);
        defer alloc.free(new_value);
        if (!std.mem.eql(u8, old_value, new_value)) {
            try addForeignKeyParentUpdateParticipantsForUniqueValue(
                alloc,
                catalog,
                participants,
                parent_table_name,
                constraint,
                old_value,
                new_value,
                constraint_timing_overrides,
            );
        }
    }

    for (parent_runtime_schema.unique_constraints) |constraint| {
        if (!uniqueConstraintCanBackForeignKey(constraint)) continue;
        const old_value = try relational_store.uniqueConstraintTupleValueWithColumnsAlloc(alloc, old_row, constraint, parent_runtime_schema.relational_columns);
        defer if (old_value) |value| alloc.free(value);
        const new_value = try relational_store.uniqueConstraintTupleValueWithColumnsAlloc(alloc, new_row, constraint, parent_runtime_schema.relational_columns);
        defer if (new_value) |value| alloc.free(value);
        if (old_value == null) continue;
        if (new_value != null and std.mem.eql(u8, old_value.?, new_value.?)) continue;
        try addForeignKeyParentUpdateParticipantsForUniqueValue(
            alloc,
            catalog,
            participants,
            parent_table_name,
            constraint,
            old_value.?,
            new_value,
            constraint_timing_overrides,
        );
    }
}

fn addForeignKeyParentUpdateParticipantsForUniqueValue(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    parent_table_name: []const u8,
    parent_constraint: storage_schema.UniqueConstraint,
    old_parent_value: []const u8,
    new_parent_value: ?[]const u8,
    constraint_timing_overrides: []const TableForeignKeyConstraintTimingOverrides,
) !void {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    for (snapshot.tables) |table| {
        if (table.schema_json.len == 0) continue;
        var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, table.schema_json);
        defer parsed_schema.deinit(alloc);
        const child_runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer storage_schema.freeSchema(alloc, child_runtime_schema);
        if (child_runtime_schema.storage_mode != .relational or child_runtime_schema.foreign_keys.len == 0) continue;

        for (child_runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!std.mem.eql(u8, foreignKeyParentCatalogTableName(table.name, child_runtime_schema.default_type, foreign_key), parent_table_name)) continue;
            if (!uniqueConstraintCanBackForeignKey(parent_constraint)) continue;
            if (!stringSlicesEqual(foreign_key.parent_columns, parent_constraint.columns)) continue;
            if (!foreignKeyUpdateActionSupported(foreign_key)) return error.UnsupportedOperation;

            var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
                alloc,
                catalog,
                table.name,
                foreign_key.name,
                parent_table_name,
                old_parent_value,
            );
            defer resolution.deinit(alloc);
            if (!resolution.configured) return error.UnsupportedOperation;
            if (resolution.groups.len == 0) return error.UnknownGroup;
            for (resolution.groups) |group_id| {
                const owner_participant = try ensureParticipantTxn(alloc, participants, table.name, group_id, resolution.topology_epoch);
                if (foreignKeyUpdateActionRestricts(foreign_key)) {
                    try appendForeignKeyParentUpdateCheck(alloc, owner_participant, foreign_key, old_parent_value, effectiveForeignKeyParentCheckTiming(table.name, foreign_key, constraint_timing_overrides));
                } else {
                    try appendForeignKeyConflictCheck(alloc, owner_participant, foreign_key, old_parent_value);
                    try appendForeignKeyActionScheduleMutationForUpdate(
                        alloc,
                        owner_participant,
                        table.name,
                        foreign_key,
                        old_parent_value,
                        new_parent_value,
                        group_id,
                    );
                }
            }
        }
    }
}

fn appendForeignKeyParentCheck(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    foreign_key: storage_schema.ForeignKey,
    child_table: []const u8,
    child_key: []const u8,
    parent_key: []const u8,
    parent_constraint_name: ?[]const u8,
    child_period_start_json: ?[]const u8,
    child_period_end_json: ?[]const u8,
    timing: db_mod.types.ForeignKeyParentCheck.Timing,
) !void {
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const child_table_owned = try alloc.dupe(u8, child_table);
    errdefer alloc.free(child_table_owned);
    const child_key_owned = try alloc.dupe(u8, child_key);
    errdefer alloc.free(child_key_owned);
    const parent_table_owned = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table_owned);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    const parent_constraint_name_owned = if (parent_constraint_name) |name| try alloc.dupe(u8, name) else null;
    errdefer if (parent_constraint_name_owned) |name| alloc.free(name);
    const child_period_start_owned = if (child_period_start_json) |json| try alloc.dupe(u8, json) else null;
    errdefer if (child_period_start_owned) |json| alloc.free(json);
    const child_period_end_owned = if (child_period_end_json) |json| try alloc.dupe(u8, json) else null;
    errdefer if (child_period_end_owned) |json| alloc.free(json);
    try participant.foreign_key_parent_checks.append(alloc, .{
        .constraint_name = constraint_name,
        .child_table = child_table_owned,
        .child_key = child_key_owned,
        .parent_table = parent_table_owned,
        .parent_key = parent_key_owned,
        .parent_constraint_name = parent_constraint_name_owned,
        .child_period_start_json = child_period_start_owned,
        .child_period_end_json = child_period_end_owned,
        .timing = timing,
    });
}

fn appendForeignKeyExternalizedParentCheck(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    foreign_key: storage_schema.ForeignKey,
    child_table: []const u8,
    child_key: []const u8,
    parent_key: []const u8,
    parent_constraint_name: ?[]const u8,
    child_period_start_json: ?[]const u8,
    child_period_end_json: ?[]const u8,
    timing: db_mod.types.ForeignKeyParentCheck.Timing,
) !void {
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const child_table_owned = try alloc.dupe(u8, child_table);
    errdefer alloc.free(child_table_owned);
    const child_key_owned = try alloc.dupe(u8, child_key);
    errdefer alloc.free(child_key_owned);
    const parent_table_owned = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table_owned);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    const parent_constraint_name_owned = if (parent_constraint_name) |name| try alloc.dupe(u8, name) else null;
    errdefer if (parent_constraint_name_owned) |name| alloc.free(name);
    const child_period_start_owned = if (child_period_start_json) |json| try alloc.dupe(u8, json) else null;
    errdefer if (child_period_start_owned) |json| alloc.free(json);
    const child_period_end_owned = if (child_period_end_json) |json| try alloc.dupe(u8, json) else null;
    errdefer if (child_period_end_owned) |json| alloc.free(json);
    try participant.foreign_key_externalized_parent_checks.append(alloc, .{
        .constraint_name = constraint_name,
        .child_table = child_table_owned,
        .child_key = child_key_owned,
        .parent_table = parent_table_owned,
        .parent_key = parent_key_owned,
        .parent_constraint_name = parent_constraint_name_owned,
        .child_period_start_json = child_period_start_owned,
        .child_period_end_json = child_period_end_owned,
        .timing = timing,
    });
}

fn foreignKeyParentCheckTiming(foreign_key: storage_schema.ForeignKey) db_mod.types.ForeignKeyParentCheck.Timing {
    return switch (foreign_key.timing) {
        .immediate => .immediate,
        .deferred => .deferred,
    };
}

fn addForeignKeyRefOwnerWriteParticipant(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    child_key: []const u8,
    parent_key: []const u8,
) !void {
    const parent_table_name = foreignKeyParentCatalogTableName(child_table_name, child_runtime_table, foreign_key);
    var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        foreign_key.name,
        parent_table_name,
        parent_key,
    );
    defer resolution.deinit(alloc);
    if (!resolution.configured) return;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    if (resolution.groups.len != 1) return error.TopologyChanged;
    const participant = try ensureParticipantTxn(alloc, participants, child_table_name, resolution.groups[0], resolution.topology_epoch);
    try appendForeignKeyRefMutation(alloc, &participant.foreign_key_ref_writes, foreign_key, child_runtime_table, child_key, parent_key);
}

fn addForeignKeyRefOwnerDeleteParticipant(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    child_key: []const u8,
    parent_key: []const u8,
) !void {
    const parent_table_name = foreignKeyParentCatalogTableName(child_table_name, child_runtime_table, foreign_key);
    var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        foreign_key.name,
        parent_table_name,
        parent_key,
    );
    defer resolution.deinit(alloc);
    if (!resolution.configured) return;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    if (resolution.groups.len != 1) return error.TopologyChanged;
    const participant = try ensureParticipantTxn(alloc, participants, child_table_name, resolution.groups[0], resolution.topology_epoch);
    try appendForeignKeyRefMutation(alloc, &participant.foreign_key_ref_deletes, foreign_key, child_runtime_table, child_key, parent_key);
}

fn appendForeignKeyRefMutation(
    alloc: std.mem.Allocator,
    mutations: *std.ArrayListUnmanaged(db_mod.types.ForeignKeyRefMutation),
    foreign_key: storage_schema.ForeignKey,
    child_runtime_table: []const u8,
    child_key: []const u8,
    parent_key: []const u8,
) !void {
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const parent_table_owned = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table_owned);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    const child_table_owned = try alloc.dupe(u8, child_runtime_table);
    errdefer alloc.free(child_table_owned);
    const child_key_owned = try alloc.dupe(u8, child_key);
    errdefer alloc.free(child_key_owned);
    try mutations.append(alloc, .{
        .constraint_name = constraint_name,
        .parent_table = parent_table_owned,
        .parent_key = parent_key_owned,
        .child_table = child_table_owned,
        .child_key = child_key_owned,
    });
}

fn addForeignKeyChildDeleteParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
    predicates: []const db_mod.types.TransactionVersionPredicate,
) !void {
    if (deletes.len == 0) return;
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) return;

    for (deletes) |key| {
        if (try keyHasForeignKeyReferenceTransform(alloc, runtime_schema.foreign_keys, key, transforms)) continue;
        for (runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!try foreignKeyRefOwnersConfiguredForConstraint(alloc, catalog, table_name, runtime_schema.default_type, foreign_key, "")) continue;

            var old_row = try lookupDeleteRowForConstraintProof(alloc, catalog, worker, participants, table_name, key, predicates);
            defer if (old_row) |*row| row.deinit(alloc);
            const old = old_row orelse continue;
            const maybe_old_parent_key = try foreignKeyParentReferenceFromJsonAlloc(alloc, catalog, table_name, runtime_schema.default_type, runtime_schema.relational_columns, foreign_key, old.json);
            defer if (maybe_old_parent_key) |old_parent_key| alloc.free(old_parent_key);
            if (maybe_old_parent_key) |old_parent_key| {
                try addForeignKeyRefOwnerDeleteParticipant(alloc, catalog, participants, table_name, runtime_schema.default_type, foreign_key, key, old_parent_key);
            }
        }
    }
}

fn addForeignKeyParentDeleteParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    parent_table_name: []const u8,
    parent_writes: []const db_mod.types.TransactionWrite,
    parent_preimages: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    predicates: []const db_mod.types.TransactionVersionPredicate,
    constraint_timing_overrides: []const TableForeignKeyConstraintTimingOverrides,
    cascade_depth: u32,
    cascade_max_depth: u32,
) !void {
    if (deletes.len == 0) return;
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    for (snapshot.tables) |table| {
        if (table.schema_json.len == 0) continue;
        var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, table.schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer storage_schema.freeSchema(alloc, runtime_schema);
        if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) continue;

        var has_supported_parent_delete_check = false;
        for (runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!std.mem.eql(u8, foreignKeyParentCatalogTableName(table.name, runtime_schema.default_type, foreign_key), parent_table_name)) continue;
            if (!foreignKeySupportsDistributedParentDeleteCheck(foreign_key) and !foreignKeySupportsRoutedUniqueParentDeleteCheck(foreign_key)) return error.UnsupportedOperation;
            has_supported_parent_delete_check = true;
        }
        if (!has_supported_parent_delete_check) continue;

        for (runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!std.mem.eql(u8, foreignKeyParentCatalogTableName(table.name, runtime_schema.default_type, foreign_key), parent_table_name)) continue;
            for (deletes) |parent_key| {
                if (try temporalParentDeleteCoveredByTransactionWrites(
                    alloc,
                    catalog,
                    worker,
                    participants,
                    parent_table_name,
                    parent_key,
                    parent_writes,
                    parent_preimages,
                    predicates,
                    foreign_key,
                )) continue;
                if (foreignKeyReferencesPrimaryKey(foreign_key)) {
                    if (!foreignKeySupportsDistributedParentDeleteCheck(foreign_key)) return error.UnsupportedOperation;
                    if (try addRoutedForeignKeyParentDeleteParticipants(alloc, catalog, worker, participants, table.name, runtime_schema.default_type, foreign_key, parent_key, constraint_timing_overrides, cascade_depth, cascade_max_depth)) continue;
                } else {
                    if (!foreignKeySupportsRoutedUniqueParentDeleteCheck(foreign_key)) return error.UnsupportedOperation;
                    if (try addRoutedUniqueForeignKeyParentDeleteParticipants(alloc, catalog, worker, participants, table.name, runtime_schema.default_type, foreign_key, parent_table_name, parent_key, predicates, constraint_timing_overrides, cascade_depth, cascade_max_depth)) continue;
                }
                return error.UnsupportedOperation;
            }
        }
    }
}

fn temporalParentDeleteCoveredByTransactionWrites(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    parent_table_name: []const u8,
    parent_doc_key: []const u8,
    parent_writes: []const db_mod.types.TransactionWrite,
    parent_preimages: []const db_mod.types.TransactionWrite,
    predicates: []const db_mod.types.TransactionVersionPredicate,
    foreign_key: storage_schema.ForeignKey,
) !bool {
    _ = worker;
    _ = participants;
    _ = predicates;
    const parent_period_name = foreign_key.parent_period orelse return false;
    if (parent_writes.len == 0) return false;

    const parent_schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, parent_table_name)) orelse return error.TableNotFound;
    defer alloc.free(parent_schema_json);
    var parsed_parent_schema = try schema_mod.parseValidatedTableSchema(alloc, parent_schema_json);
    defer parsed_parent_schema.deinit(alloc);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_parent_schema);
    defer storage_schema.freeSchema(alloc, parent_runtime_schema);
    if (parent_runtime_schema.storage_mode != .relational) return error.UnsupportedOperation;
    _ = relationalPeriodByName(parent_runtime_schema.periods, parent_period_name) orelse return error.UnsupportedOperation;
    const parent_constraint = findParentTupleConstraintByColumns(parent_runtime_schema, foreign_key.parent_columns) orelse return error.UnsupportedOperation;

    const old_parent_json = findWriteValueForKey(parent_preimages, parent_doc_key) orelse return false;
    var old_span = try temporalUniquePeriodSpanBytesFromJsonAlloc(alloc, parent_runtime_schema, old_parent_json, parent_period_name);
    defer old_span.deinit(alloc);
    const old_row = try document_mapper.buildRelationalRowValueAlloc(alloc, old_parent_json, parent_runtime_schema.relational_columns);
    defer alloc.free(old_row);
    const old_parent_tuple = try parentTupleValueAlloc(alloc, old_row, parent_runtime_schema, parent_constraint);
    defer alloc.free(old_parent_tuple);

    var spans = std.ArrayListUnmanaged(TemporalUniquePeriodSpanBytes).empty;
    defer {
        for (spans.items) |*span| span.deinit(alloc);
        spans.deinit(alloc);
    }

    for (parent_writes) |write| {
        const row = document_mapper.buildRelationalRowValueAlloc(alloc, write.value, parent_runtime_schema.relational_columns) catch continue;
        defer alloc.free(row);
        const tuple = (parentTupleValueAlloc(alloc, row, parent_runtime_schema, parent_constraint) catch continue);
        defer alloc.free(tuple);
        if (!std.mem.eql(u8, old_parent_tuple, tuple)) continue;
        var span = temporalUniquePeriodSpanBytesFromJsonAlloc(alloc, parent_runtime_schema, write.value, parent_period_name) catch continue;
        errdefer span.deinit(alloc);
        try spans.append(alloc, span);
    }
    if (spans.items.len == 0) return false;

    std.mem.sort(TemporalUniquePeriodSpanBytes, spans.items, {}, temporalUniquePeriodSpanLessThan);
    var covered_end = old_span.start;
    for (spans.items) |span| {
        if ((try relational_store.temporalPeriodBoundBytesOrder(span.end, covered_end)) != .gt) continue;
        if ((try relational_store.temporalPeriodBoundBytesOrder(span.start, covered_end)) == .gt) return false;
        if ((try relational_store.temporalPeriodBoundBytesOrder(span.end, covered_end)) == .gt) covered_end = span.end;
        if ((try relational_store.temporalPeriodBoundBytesOrder(covered_end, old_span.end)) != .lt) return true;
    }
    return false;
}

fn temporalUniquePeriodSpanLessThan(_: void, left: TemporalUniquePeriodSpanBytes, right: TemporalUniquePeriodSpanBytes) bool {
    return (relational_store.temporalPeriodBoundBytesOrder(left.start, right.start) catch .gt) == .lt;
}

fn tableHasMutatingForeignKeyDeleteDependents(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    parent_table_name: []const u8,
) !bool {
    var snapshot = try catalog.adminSnapshot();
    defer catalog.freeAdminSnapshot(&snapshot);
    for (snapshot.tables) |table| {
        if (table.schema_json.len == 0) continue;
        var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, table.schema_json);
        defer parsed_schema.deinit(alloc);
        const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
        defer storage_schema.freeSchema(alloc, runtime_schema);
        if (runtime_schema.storage_mode != .relational or runtime_schema.foreign_keys.len == 0) continue;
        for (runtime_schema.foreign_keys) |foreign_key| {
            if (!foreignKeyIsEnforced(foreign_key)) continue;
            if (!std.mem.eql(u8, foreignKeyParentCatalogTableName(table.name, runtime_schema.default_type, foreign_key), parent_table_name)) continue;
            if (foreign_key.on_delete == .cascade or foreign_key.on_delete == .set_null) return true;
        }
    }
    return false;
}

fn addRoutedForeignKeyParentDeleteParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    constraint_timing_overrides: []const TableForeignKeyConstraintTimingOverrides,
    cascade_depth: u32,
    cascade_max_depth: u32,
) !bool {
    _ = worker;
    const parent_table_name = foreignKeyParentCatalogTableName(child_table_name, child_runtime_table, foreign_key);
    var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        foreign_key.name,
        parent_table_name,
        parent_key,
    );
    defer resolution.deinit(alloc);
    if (!resolution.configured) return false;
    if (!foreignKeyDeleteActionSupported(foreign_key)) return error.UnsupportedOperation;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    for (resolution.groups) |group_id| {
        if (foreignKeyDeleteActionRestricts(foreign_key)) {
            const participant = try ensureParticipantTxn(alloc, participants, child_table_name, group_id, resolution.topology_epoch);
            try appendForeignKeyParentDeleteCheck(alloc, participant, foreign_key, parent_key, effectiveForeignKeyParentCheckTiming(child_table_name, foreign_key, constraint_timing_overrides));
        } else {
            const owner_participant = try ensureParticipantTxn(alloc, participants, child_table_name, group_id, resolution.topology_epoch);
            try appendForeignKeyConflictCheck(alloc, owner_participant, foreign_key, parent_key);
        }
    }
    if (foreign_key.on_delete == .set_null or foreign_key.on_delete == .cascade) {
        for (resolution.groups) |group_id| {
            const scheduler_participant = try ensureParticipantTxn(alloc, participants, child_table_name, group_id, resolution.topology_epoch);
            try appendForeignKeyActionScheduleMutation(alloc, scheduler_participant, child_table_name, foreign_key, parent_key, group_id, cascade_depth, cascade_max_depth);
        }
    }
    return true;
}

fn addRoutedUniqueForeignKeyParentDeleteParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    parent_table_name: []const u8,
    parent_doc_key: []const u8,
    predicates: []const db_mod.types.TransactionVersionPredicate,
    constraint_timing_overrides: []const TableForeignKeyConstraintTimingOverrides,
    cascade_depth: u32,
    cascade_max_depth: u32,
) !bool {
    _ = child_runtime_table;
    if (foreignKeyReferencesPrimaryKey(foreign_key)) return false;
    if (!foreignKeyDeleteActionSupported(foreign_key)) return error.UnsupportedOperation;

    const parent_schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, parent_table_name)) orelse return error.TableNotFound;
    defer alloc.free(parent_schema_json);
    var parsed_parent_schema = try schema_mod.parseValidatedTableSchema(alloc, parent_schema_json);
    defer parsed_parent_schema.deinit(alloc);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_parent_schema);
    defer storage_schema.freeSchema(alloc, parent_runtime_schema);
    if (parent_runtime_schema.storage_mode != .relational) return error.UnsupportedOperation;
    const parent_constraint = findParentTupleConstraintByColumns(parent_runtime_schema, foreign_key.parent_columns) orelse return error.UnsupportedOperation;

    var parent_row_lookup = try lookupDeleteRowForConstraintProof(alloc, catalog, worker, participants, parent_table_name, parent_doc_key, predicates);
    defer if (parent_row_lookup) |*row| row.deinit(alloc);
    const parent_lookup = parent_row_lookup orelse return true;
    const parent_row = try document_mapper.buildRelationalRowValueAlloc(alloc, parent_lookup.json, parent_runtime_schema.relational_columns);
    defer alloc.free(parent_row);
    const encoded_parent = try parentTupleValueAlloc(alloc, parent_row, parent_runtime_schema, parent_constraint);
    defer alloc.free(encoded_parent);

    var resolution = try table_catalog.resolveForeignKeyRefOwnerGroups(
        alloc,
        catalog,
        child_table_name,
        foreign_key.name,
        parent_table_name,
        encoded_parent,
    );
    defer resolution.deinit(alloc);
    if (!resolution.configured) return false;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    for (resolution.groups) |group_id| {
        if (foreignKeyDeleteActionRestricts(foreign_key)) {
            const owner_participant = try ensureParticipantTxn(alloc, participants, child_table_name, group_id, resolution.topology_epoch);
            try appendForeignKeyParentDeleteCheck(alloc, owner_participant, foreign_key, encoded_parent, effectiveForeignKeyParentCheckTiming(child_table_name, foreign_key, constraint_timing_overrides));
            continue;
        }

        const owner_participant = try ensureParticipantTxn(alloc, participants, child_table_name, group_id, resolution.topology_epoch);
        try appendForeignKeyConflictCheck(alloc, owner_participant, foreign_key, encoded_parent);
    }
    if (foreign_key.on_delete == .set_null or foreign_key.on_delete == .cascade) {
        for (resolution.groups) |group_id| {
            const scheduler_participant = try ensureParticipantTxn(alloc, participants, child_table_name, group_id, resolution.topology_epoch);
            try appendForeignKeyActionScheduleMutation(alloc, scheduler_participant, child_table_name, foreign_key, encoded_parent, group_id, cascade_depth, cascade_max_depth);
        }
    }
    return true;
}

fn freeForeignKeyRefChildren(alloc: std.mem.Allocator, children: []const db_mod.types.ForeignKeyRefChild) void {
    for (children) |child| {
        alloc.free(@constCast(child.child_table));
        alloc.free(@constCast(child.child_key));
    }
    if (children.len > 0) alloc.free(@constCast(children));
}

fn rejectUnsupportedDistributedForeignKeyTransforms(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !void {
    if (transforms.len == 0) return;
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational) return;
    for (runtime_schema.foreign_keys) |foreign_key| {
        if (!foreignKeyIsEnforcedImmediate(foreign_key)) continue;
        for (transforms) |transform| {
            const touches_reference = try foreignKeyTransformTouchesReference(alloc, foreign_key, transform);
            if (touches_reference and !foreignKeySupportsDistributedParentCheck(foreign_key) and !try foreignKeyReferencesUniqueParent(alloc, catalog, table_name, runtime_schema.default_type, foreign_key)) return error.UnsupportedOperation;
        }
    }
}

fn foreignKeyTransformTouchesReference(
    alloc: std.mem.Allocator,
    foreign_key: storage_schema.ForeignKey,
    transform: db_mod.types.DocumentTransform,
) !bool {
    for (transform.operations) |op| {
        if (foreignKeyPathTouchesReference(foreign_key, op.path)) return true;
        if (op.op == .rename) {
            const value_json = op.value_json orelse return true;
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return true;
            defer parsed.deinit();
            const target_path = switch (parsed.value) {
                .string => |text| text,
                else => return true,
            };
            if (foreignKeyPathTouchesReference(foreign_key, target_path)) return true;
        }
    }
    return false;
}

fn foreignKeyPathTouchesReference(foreign_key: storage_schema.ForeignKey, raw_path: []const u8) bool {
    return columnPathTouchesAny(raw_path, foreign_key.child_columns);
}

fn columnPathTouchesAny(raw_path: []const u8, columns: []const []const u8) bool {
    const path = normalizeTransformPathView(raw_path) orelse return true;
    for (columns) |column| {
        if (jsonPathsOverlap(path, normalizeTransformPathView(column) orelse return true)) return true;
    }
    return false;
}

fn columnPathTouches(raw_path: []const u8, column: []const u8) bool {
    const path = normalizeTransformPathView(raw_path) orelse return true;
    return jsonPathsOverlap(path, normalizeTransformPathView(column) orelse return true);
}

fn normalizeTransformPathView(path: []const u8) ?[]const u8 {
    if (path.len == 0) return null;
    if (path[0] != '$') return path;
    if (path.len == 1) return null;
    if (path[1] != '.') return null;
    if (path.len == 2) return null;
    return path[2..];
}

fn jsonPathsOverlap(a: []const u8, b: []const u8) bool {
    if (std.mem.eql(u8, a, b)) return true;
    if (a.len < b.len) {
        return b[a.len] == '.' and std.mem.eql(u8, a, b[0..a.len]);
    }
    if (b.len < a.len) {
        return a[b.len] == '.' and std.mem.eql(u8, b, a[0..b.len]);
    }
    return false;
}

fn addUniqueConstraintOwnerParticipants(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    worker: ParticipantWorker,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    writes: []const db_mod.types.TransactionWrite,
    deletes: []const []const u8,
    transforms: []const db_mod.types.DocumentTransform,
    predicates: []const db_mod.types.TransactionVersionPredicate,
) !void {
    if (writes.len == 0 and deletes.len == 0 and transforms.len == 0) return;
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, table_name)) orelse return;
    defer alloc.free(schema_json);
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational or (runtime_schema.primary_key == null and runtime_schema.unique_constraints.len == 0)) return;
    const owner_constraints = try runtimeOwnerConstraintsAlloc(alloc, runtime_schema);
    defer alloc.free(owner_constraints);
    if (!try uniqueConstraintOwnerTopologyConfigured(alloc, catalog, table_name, owner_constraints)) return;

    for (writes) |write| {
        if (try keyHasUniqueConstraintTransform(alloc, owner_constraints, write.key, transforms)) continue;
        var old_row = try lookupWriteRowForConstraintProof(alloc, catalog, worker, participants, table_name, write.key, predicates);
        defer if (old_row) |*row| row.deinit(alloc);
        try addUniqueConstraintOwnerMutationsForWrite(alloc, catalog, participants, table_name, runtime_schema, owner_constraints, write.key, if (old_row) |row| row.json else null, write.value);
    }
    for (deletes) |key| {
        if (findWriteValueForKey(writes, key) != null) continue;
        if (try keyHasUniqueConstraintTransform(alloc, owner_constraints, key, transforms)) continue;
        var old_row = try lookupDeleteRowForConstraintProof(alloc, catalog, worker, participants, table_name, key, predicates);
        defer if (old_row) |*row| row.deinit(alloc);
        const old = old_row orelse continue;
        try addUniqueConstraintOwnerMutationsForWrite(alloc, catalog, participants, table_name, runtime_schema, owner_constraints, key, old.json, null);
    }
    for (transforms, 0..) |transform, transform_index| {
        if (transformKeySeenBefore(transforms, transform_index)) continue;
        if (!try keyHasUniqueConstraintTransform(alloc, owner_constraints, transform.key, transforms)) continue;

        var old_row = try lookupWriteRowForConstraintProof(alloc, catalog, worker, participants, table_name, transform.key, predicates);
        defer if (old_row) |*row| row.deinit(alloc);

        var final_owned: ?[]u8 = null;
        defer if (final_owned) |body| alloc.free(body);
        var final_json: ?[]const u8 = blk: {
            if (deleteContainsKey(deletes, transform.key)) break :blk null;
            if (findWriteValueForKey(writes, transform.key)) |write_value| break :blk write_value;
            if (old_row) |row| break :blk row.json;
            break :blk null;
        };
        for (transforms) |candidate| {
            if (!std.mem.eql(u8, candidate.key, transform.key)) continue;
            const resolved = try db_mod.transform.resolveDocumentTransform(alloc, final_json, candidate) orelse continue;
            if (final_owned) |previous| alloc.free(previous);
            final_owned = resolved;
            final_json = resolved;
        }
        if (final_json == null and old_row == null) continue;

        try addUniqueConstraintOwnerMutationsForWrite(alloc, catalog, participants, table_name, runtime_schema, owner_constraints, transform.key, if (old_row) |row| row.json else null, final_json);
    }
}

fn uniqueConstraintOwnerTopologyConfigured(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    table_name: []const u8,
    constraints: []const storage_schema.UniqueConstraint,
) !bool {
    for (constraints) |constraint| {
        var resolution = try table_catalog.resolveUniqueConstraintOwnerGroups(alloc, catalog, table_name, constraint.name, "");
        defer resolution.deinit(alloc);
        if (resolution.configured) return true;
    }
    return false;
}

fn transformTouchesUniqueConstraint(
    alloc: std.mem.Allocator,
    constraints: []const storage_schema.UniqueConstraint,
    transform: db_mod.types.DocumentTransform,
) !bool {
    for (constraints) |constraint| {
        for (transform.operations) |op| {
            if (constraintMetadataTouchesPath(constraint, op.path)) return true;
            if (op.op == .rename) {
                const value_json = op.value_json orelse return true;
                var parsed = std.json.parseFromSlice(std.json.Value, alloc, value_json, .{}) catch return true;
                defer parsed.deinit();
                const target_path = switch (parsed.value) {
                    .string => |text| text,
                    else => return true,
                };
                if (constraintMetadataTouchesPath(constraint, target_path)) return true;
            }
        }
    }
    return false;
}

fn constraintMetadataTouchesPath(constraint: storage_schema.UniqueConstraint, path: []const u8) bool {
    if (columnPathTouchesAny(path, constraint.columns)) return true;
    for (constraint.expressions) |expression| {
        switch (expression.op) {
            .lower, .upper, .md5 => if (columnPathTouches(path, expression.field)) return true,
            .expression => if (expression.expression) |row_expression| {
                if (rowExpressionTouchesPath(row_expression, path)) return true;
            },
        }
    }
    for (constraint.where) |predicate| {
        if (columnPathTouches(path, predicate.field)) return true;
    }
    return false;
}

fn rowExpressionTouchesPath(expression: storage_schema.RelationalRowsExpression, path: []const u8) bool {
    if (expression.kind == .field and expression.field_source == .row and columnPathTouches(path, expression.field)) return true;
    for (expression.operands) |operand| {
        if (rowExpressionTouchesPath(operand, path)) return true;
    }
    for (expression.case_branches) |branch| {
        if (rowExpressionConditionTouchesPath(branch.when, path)) return true;
        if (rowExpressionTouchesPath(branch.then, path)) return true;
    }
    for (expression.case_else) |fallback| {
        if (rowExpressionTouchesPath(fallback, path)) return true;
    }
    return false;
}

fn rowExpressionConditionTouchesPath(condition: storage_schema.RelationalRowsExpressionCondition, path: []const u8) bool {
    if (rowExpressionTouchesPath(condition.lhs, path)) return true;
    for (condition.rhs) |rhs| {
        if (rowExpressionTouchesPath(rhs, path)) return true;
    }
    return false;
}

fn keyHasRuntimeOwnerConstraintTransform(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    key: []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !bool {
    const owner_constraints = try runtimeOwnerConstraintsAlloc(alloc, runtime_schema);
    defer alloc.free(owner_constraints);
    return try keyHasUniqueConstraintTransform(alloc, owner_constraints, key, transforms);
}

fn keyHasUniqueConstraintTransform(
    alloc: std.mem.Allocator,
    constraints: []const storage_schema.UniqueConstraint,
    key: []const u8,
    transforms: []const db_mod.types.DocumentTransform,
) !bool {
    for (transforms) |transform| {
        if (!std.mem.eql(u8, transform.key, key)) continue;
        if (try transformTouchesUniqueConstraint(alloc, constraints, transform)) return true;
    }
    return false;
}

fn runtimeOwnerConstraintsAlloc(alloc: std.mem.Allocator, runtime_schema: storage_schema.TableSchema) ![]storage_schema.UniqueConstraint {
    const extra: usize = if (runtime_schema.primary_key != null) 1 else 0;
    const constraints = try alloc.alloc(storage_schema.UniqueConstraint, runtime_schema.unique_constraints.len + extra);
    var index: usize = 0;
    if (runtime_schema.primary_key) |primary_key| {
        constraints[index] = primaryKeyAsUniqueConstraint(primary_key);
        index += 1;
    }
    for (runtime_schema.unique_constraints) |constraint| {
        constraints[index] = constraint;
        index += 1;
    }
    return constraints;
}

fn addUniqueConstraintOwnerMutationsForWrite(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    runtime_schema: storage_schema.TableSchema,
    constraints: []const storage_schema.UniqueConstraint,
    owner_key: []const u8,
    old_json: ?[]const u8,
    new_json: ?[]const u8,
) !void {
    const old_row = if (old_json) |json| try document_mapper.buildRelationalRowValueAlloc(alloc, json, runtime_schema.relational_columns) else null;
    defer if (old_row) |row| alloc.free(row);
    const new_row = if (new_json) |json| try document_mapper.buildRelationalRowValueAlloc(alloc, json, runtime_schema.relational_columns) else null;
    defer if (new_row) |row| alloc.free(row);

    for (constraints) |constraint| {
        const old_value = if (old_row) |row| try relational_store.uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, constraint, runtime_schema.relational_columns) else null;
        defer if (old_value) |value| alloc.free(value);
        const new_value = if (new_row) |row| try relational_store.uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, constraint, runtime_schema.relational_columns) else null;
        defer if (new_value) |value| alloc.free(value);
        var old_span = if (old_json) |json| if (constraint.without_overlaps_period) |period_name|
            try temporalUniquePeriodSpanBytesFromJsonAlloc(alloc, runtime_schema, json, period_name)
        else
            null else null;
        defer if (old_span) |*span| span.deinit(alloc);
        var new_span = if (new_json) |json| if (constraint.without_overlaps_period) |period_name|
            try temporalUniquePeriodSpanBytesFromJsonAlloc(alloc, runtime_schema, json, period_name)
        else
            null else null;
        defer if (new_span) |*span| span.deinit(alloc);
        if (old_value != null and new_value != null and std.mem.eql(u8, old_value.?, new_value.?) and temporalUniqueSpansEqual(old_span, new_span)) continue;
        if (old_value) |value| {
            if (!try addUniqueConstraintOwnerDeleteParticipant(alloc, catalog, participants, table_name, constraint, value, old_span, owner_key)) return error.UnsupportedOperation;
        }
        if (new_value) |value| {
            if (!try addUniqueConstraintOwnerWriteParticipant(alloc, catalog, participants, table_name, constraint, value, new_span, owner_key)) return error.UnsupportedOperation;
        }
    }
}

const TemporalUniquePeriodSpanBytes = struct {
    start: []const u8,
    end: []const u8,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(@constCast(self.start));
        alloc.free(@constCast(self.end));
        self.* = undefined;
    }
};

fn temporalUniquePeriodSpanBytesFromJsonAlloc(
    alloc: std.mem.Allocator,
    runtime_schema: storage_schema.TableSchema,
    row_json: []const u8,
    period_name: []const u8,
) !TemporalUniquePeriodSpanBytes {
    const period = relationalPeriodByName(runtime_schema.periods, period_name) orelse return error.InvalidTxnRequest;
    const start_column = findRelationalColumn(runtime_schema.relational_columns, period.start_column) orelse return error.InvalidTxnRequest;
    const end_column = findRelationalColumn(runtime_schema.relational_columns, period.end_column) orelse return error.InvalidTxnRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, row_json, .{}) catch return error.InvalidTxnRequest;
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidTxnRequest;
    const start_json = if (parsed.value.object.get(period.start_column)) |value|
        try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false })
    else
        null;
    defer if (start_json) |json| alloc.free(json);
    const end_json = if (parsed.value.object.get(period.end_column)) |value|
        try std.json.Stringify.valueAlloc(alloc, value, .{ .emit_null_optional_fields = false })
    else
        null;
    defer if (end_json) |json| alloc.free(json);
    const start = try relational_store.temporalPeriodStartBoundBytesFromJsonAlloc(alloc, start_json, start_column);
    errdefer alloc.free(start);
    const end = try relational_store.temporalPeriodEndBoundBytesFromJsonAlloc(alloc, end_json, end_column);
    errdefer alloc.free(end);
    if (!(relational_store.temporalPeriodSpanBytesValid(start, end) catch return error.InvalidTxnRequest)) return error.InvalidTxnRequest;
    return .{ .start = start, .end = end };
}

fn findRelationalColumn(columns: []const storage_schema.RelationalColumn, name: []const u8) ?storage_schema.RelationalColumn {
    for (columns) |column| {
        if (std.mem.eql(u8, column.name, name)) return column;
    }
    return null;
}

fn temporalUniqueSpansEqual(left: ?TemporalUniquePeriodSpanBytes, right: ?TemporalUniquePeriodSpanBytes) bool {
    if (left == null or right == null) return left == null and right == null;
    return std.mem.eql(u8, left.?.start, right.?.start) and std.mem.eql(u8, left.?.end, right.?.end);
}

fn addUniqueConstraintOwnerWriteParticipant(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    constraint: storage_schema.UniqueConstraint,
    encoded_value: []const u8,
    temporal_span: ?TemporalUniquePeriodSpanBytes,
    owner_key: []const u8,
) !bool {
    var resolution = try table_catalog.resolveUniqueConstraintOwnerGroups(alloc, catalog, table_name, constraint.name, encoded_value);
    defer resolution.deinit(alloc);
    if (!resolution.configured) return false;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    if (resolution.groups.len != 1) return error.TopologyChanged;
    const participant = try ensureParticipantTxn(alloc, participants, table_name, resolution.groups[0], resolution.topology_epoch);
    try appendUniqueConstraintMutation(alloc, &participant.unique_constraint_writes, constraint.name, encoded_value, temporal_span, owner_key);
    return true;
}

fn addUniqueConstraintOwnerDeleteParticipant(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    table_name: []const u8,
    constraint: storage_schema.UniqueConstraint,
    encoded_value: []const u8,
    temporal_span: ?TemporalUniquePeriodSpanBytes,
    owner_key: []const u8,
) !bool {
    var resolution = try table_catalog.resolveUniqueConstraintOwnerGroups(alloc, catalog, table_name, constraint.name, encoded_value);
    defer resolution.deinit(alloc);
    if (!resolution.configured) return false;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    if (resolution.groups.len != 1) return error.TopologyChanged;
    const participant = try ensureParticipantTxn(alloc, participants, table_name, resolution.groups[0], resolution.topology_epoch);
    try appendUniqueConstraintMutation(alloc, &participant.unique_constraint_deletes, constraint.name, encoded_value, temporal_span, owner_key);
    return true;
}

fn appendUniqueConstraintMutation(
    alloc: std.mem.Allocator,
    mutations: *std.ArrayListUnmanaged(db_mod.types.UniqueConstraintMutation),
    constraint_name: []const u8,
    encoded_value: []const u8,
    temporal_span: ?TemporalUniquePeriodSpanBytes,
    owner_key: []const u8,
) !void {
    const constraint_name_owned = try alloc.dupe(u8, constraint_name);
    errdefer alloc.free(constraint_name_owned);
    const encoded_value_owned = try alloc.dupe(u8, encoded_value);
    errdefer alloc.free(encoded_value_owned);
    const owner_key_owned = try alloc.dupe(u8, owner_key);
    errdefer alloc.free(owner_key_owned);
    const temporal_start_owned = if (temporal_span) |span| try alloc.dupe(u8, span.start) else null;
    errdefer if (temporal_start_owned) |value| alloc.free(value);
    const temporal_end_owned = if (temporal_span) |span| try alloc.dupe(u8, span.end) else null;
    errdefer if (temporal_end_owned) |value| alloc.free(value);
    try mutations.append(alloc, .{
        .constraint_name = constraint_name_owned,
        .encoded_value = encoded_value_owned,
        .owner_key = owner_key_owned,
        .temporal_start = temporal_start_owned,
        .temporal_end = temporal_end_owned,
    });
}

fn foreignKeyIsEnforcedImmediate(foreign_key: storage_schema.ForeignKey) bool {
    return foreign_key.validation_state == .enforced and foreign_key.timing == .immediate;
}

fn findForeignKeyByName(foreign_keys: []const storage_schema.ForeignKey, name: []const u8) ?storage_schema.ForeignKey {
    for (foreign_keys) |foreign_key| {
        if (std.mem.eql(u8, foreign_key.name, name)) return foreign_key;
    }
    return null;
}

fn foreignKeyIsEnforced(foreign_key: storage_schema.ForeignKey) bool {
    return foreign_key.validation_state == .enforced;
}

fn foreignKeySupportsDistributedParentCheck(foreign_key: storage_schema.ForeignKey) bool {
    if (!foreignKeyIsEnforced(foreign_key)) return false;
    if (foreign_key.child_columns.len != 1 or foreign_key.parent_columns.len != 1) return false;
    return std.mem.eql(u8, foreign_key.parent_columns[0], "_id");
}

fn foreignKeyReferencesPrimaryKey(foreign_key: storage_schema.ForeignKey) bool {
    return foreign_key.parent_columns.len == 1 and std.mem.eql(u8, foreign_key.parent_columns[0], "_id");
}

fn foreignKeyParentCatalogTableName(
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
) []const u8 {
    if (std.mem.eql(u8, foreign_key.parent_table, child_runtime_table)) return child_table_name;
    return foreign_key.parent_table;
}

fn foreignKeyReferencesUniqueParent(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
) !bool {
    if (foreignKeyReferencesPrimaryKey(foreign_key)) return false;
    const parent_constraint_name = try foreignKeyParentUniqueConstraintNameAlloc(alloc, catalog, child_table_name, child_runtime_table, foreign_key);
    defer if (parent_constraint_name) |name| alloc.free(name);
    return parent_constraint_name != null;
}

fn foreignKeyParentUniqueConstraintNameAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
) !?[]u8 {
    if (foreignKeyReferencesPrimaryKey(foreign_key)) return null;
    const parent_table_name = foreignKeyParentCatalogTableName(child_table_name, child_runtime_table, foreign_key);
    const schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, parent_table_name)) orelse return null;
    defer alloc.free(schema_json);
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);
    if (runtime_schema.storage_mode != .relational) return null;
    if (runtime_schema.primary_key) |primary_key| {
        if (stringSlicesEqual(primary_key.columns, foreign_key.parent_columns)) {
            return try alloc.dupe(u8, relational_store.primary_key_constraint_name);
        }
    }
    for (runtime_schema.unique_constraints) |constraint| {
        if (!uniqueConstraintCanBackForeignKey(constraint)) continue;
        if (stringSlicesEqual(constraint.columns, foreign_key.parent_columns)) return try alloc.dupe(u8, constraint.name);
    }
    return null;
}

fn findParentTupleConstraintByColumns(runtime_schema: storage_schema.TableSchema, columns: []const []const u8) ?storage_schema.UniqueConstraint {
    if (runtime_schema.primary_key) |primary_key| {
        if (stringSlicesEqual(primary_key.columns, columns)) return primaryKeyAsUniqueConstraint(primary_key);
    }
    return findUniqueConstraintByColumns(runtime_schema.unique_constraints, columns);
}

fn primaryKeyAsUniqueConstraint(primary_key: storage_schema.PrimaryKey) storage_schema.UniqueConstraint {
    return .{
        .name = relational_store.primary_key_constraint_name,
        .columns = primary_key.columns,
        .without_overlaps_period = primary_key.without_overlaps_period,
    };
}

fn parentTupleValueAlloc(
    alloc: std.mem.Allocator,
    row: []const u8,
    runtime_schema: storage_schema.TableSchema,
    constraint: storage_schema.UniqueConstraint,
) ![]u8 {
    if (runtime_schema.primary_key) |primary_key| {
        if (std.mem.eql(u8, constraint.name, relational_store.primary_key_constraint_name)) {
            return try relational_store.primaryKeyTupleValueAlloc(alloc, row, primary_key);
        }
    }
    return (try relational_store.uniqueConstraintTupleValueWithColumnsAlloc(alloc, row, constraint, runtime_schema.relational_columns)) orelse return error.UnsupportedOperation;
}

fn stringSlicesEqual(a: []const []const u8, b: []const []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |left, right| {
        if (!std.mem.eql(u8, left, right)) return false;
    }
    return true;
}

fn addForeignKeyParentCheckParticipant(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    participants: *std.ArrayListUnmanaged(ParticipantTxn),
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    foreign_key: storage_schema.ForeignKey,
    child_key: []const u8,
    parent_key: []const u8,
    child_period_start_json: ?[]const u8,
    child_period_end_json: ?[]const u8,
    timing: db_mod.types.ForeignKeyParentCheck.Timing,
) !void {
    const parent_table_name = foreignKeyParentCatalogTableName(child_table_name, child_runtime_table, foreign_key);
    if (foreignKeyReferencesPrimaryKey(foreign_key)) {
        const parent_topology_epoch = try table_catalog.topologyEpoch(alloc, catalog, parent_table_name);
        if (parent_topology_epoch == 0) return error.TableNotFound;
        const parent_group_id = (try table_catalog.resolveGroupForKeyPinned(alloc, catalog, parent_table_name, parent_key, parent_topology_epoch)) orelse return error.UnknownGroup;
        const parent_participant = try ensureParticipantTxn(alloc, participants, parent_table_name, parent_group_id, parent_topology_epoch);
        try appendForeignKeyParentCheck(alloc, parent_participant, foreign_key, child_table_name, child_key, parent_key, null, child_period_start_json, child_period_end_json, timing);
        const child_participant = try foreignKeyChildParticipantForKey(alloc, catalog, participants, child_table_name, child_key);
        try appendForeignKeyExternalizedParentCheck(alloc, child_participant, foreign_key, child_runtime_table, child_key, parent_key, null, child_period_start_json, child_period_end_json, timing);
        return;
    }

    const parent_constraint_name = (try foreignKeyParentUniqueConstraintNameAlloc(alloc, catalog, child_table_name, child_runtime_table, foreign_key)) orelse return error.UnsupportedOperation;
    defer alloc.free(parent_constraint_name);
    var resolution = try table_catalog.resolveUniqueConstraintOwnerGroups(alloc, catalog, parent_table_name, parent_constraint_name, parent_key);
    defer resolution.deinit(alloc);
    if (!resolution.configured) return error.UnsupportedOperation;
    if (resolution.groups.len == 0) return error.UnknownGroup;
    if (resolution.groups.len != 1) return error.TopologyChanged;
    const parent_participant = try ensureParticipantTxn(alloc, participants, parent_table_name, resolution.groups[0], resolution.topology_epoch);
    try appendForeignKeyParentCheck(alloc, parent_participant, foreign_key, child_table_name, child_key, parent_key, parent_constraint_name, child_period_start_json, child_period_end_json, timing);
    const child_participant = try foreignKeyChildParticipantForKey(alloc, catalog, participants, child_table_name, child_key);
    try appendForeignKeyExternalizedParentCheck(alloc, child_participant, foreign_key, child_runtime_table, child_key, parent_key, parent_constraint_name, child_period_start_json, child_period_end_json, timing);
}

fn foreignKeySupportsDistributedParentDeleteCheck(foreign_key: storage_schema.ForeignKey) bool {
    if (!foreignKeyIsEnforced(foreign_key)) return false;
    if (!foreignKeyDeleteActionSupported(foreign_key)) return false;
    return foreignKeyReferencesPrimaryKey(foreign_key);
}

fn foreignKeySupportsRoutedUniqueParentDeleteCheck(foreign_key: storage_schema.ForeignKey) bool {
    if (!foreignKeyIsEnforced(foreign_key)) return false;
    if (foreignKeyReferencesPrimaryKey(foreign_key)) return false;
    return foreignKeyDeleteActionSupported(foreign_key);
}

fn foreignKeyDeleteActionSupported(foreign_key: storage_schema.ForeignKey) bool {
    return foreignKeyDeleteActionRestricts(foreign_key) or foreign_key.on_delete == .set_null or foreign_key.on_delete == .cascade;
}

fn foreignKeyDeleteActionRestricts(foreign_key: storage_schema.ForeignKey) bool {
    return foreign_key.on_delete == .restrict or foreign_key.on_delete == .no_action;
}

fn foreignKeyUpdateActionRestricts(foreign_key: storage_schema.ForeignKey) bool {
    return foreign_key.on_update == .restrict or foreign_key.on_update == .no_action;
}

fn foreignKeyUpdateActionSupported(foreign_key: storage_schema.ForeignKey) bool {
    return foreignKeyUpdateActionRestricts(foreign_key) or foreign_key.on_update == .set_null or foreign_key.on_update == .cascade;
}

fn findUniqueConstraintByColumns(constraints: []const storage_schema.UniqueConstraint, columns: []const []const u8) ?storage_schema.UniqueConstraint {
    for (constraints) |constraint| {
        if (!uniqueConstraintCanBackForeignKey(constraint)) continue;
        if (stringSlicesEqual(constraint.columns, columns)) return constraint;
    }
    return null;
}

fn uniqueConstraintCanBackForeignKey(constraint: storage_schema.UniqueConstraint) bool {
    return constraint.where.len == 0 and constraint.expressions.len == 0;
}

fn appendForeignKeyParentDeleteCheck(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    timing: db_mod.types.ForeignKeyParentCheck.Timing,
) !void {
    try appendForeignKeyParentAbsenceCheck(alloc, participant, foreign_key, parent_key, timing, .delete);
}

fn appendForeignKeyParentUpdateCheck(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    timing: db_mod.types.ForeignKeyParentCheck.Timing,
) !void {
    try appendForeignKeyParentAbsenceCheck(alloc, participant, foreign_key, parent_key, timing, .update);
}

fn appendForeignKeyParentAbsenceCheck(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    timing: db_mod.types.ForeignKeyParentCheck.Timing,
    operation: db_mod.types.ForeignKeyParentDeleteCheck.Operation,
) !void {
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const parent_table_owned = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table_owned);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    try participant.foreign_key_parent_delete_checks.append(alloc, .{
        .constraint_name = constraint_name,
        .parent_table = parent_table_owned,
        .parent_key = parent_key_owned,
        .timing = timing,
        .operation = operation,
    });
}

fn appendForeignKeyConflictCheck(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
) !void {
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const parent_table_owned = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table_owned);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    try participant.foreign_key_conflict_checks.append(alloc, .{
        .constraint_name = constraint_name,
        .parent_table = parent_table_owned,
        .parent_key = parent_key_owned,
    });
}

fn appendForeignKeySetNullChildAction(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    child_key: []const u8,
    operation: db_mod.types.ForeignKeySetNullChildAction.Operation,
) !void {
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const parent_table_owned = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table_owned);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    const child_key_owned = try alloc.dupe(u8, child_key);
    errdefer alloc.free(child_key_owned);
    try participant.foreign_key_set_null_children.append(alloc, .{
        .constraint_name = constraint_name,
        .parent_table = parent_table_owned,
        .parent_key = parent_key_owned,
        .child_key = child_key_owned,
        .operation = operation,
    });
}

fn appendForeignKeyCascadeChildAction(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8,
    child_key: []const u8,
    operation: db_mod.types.ForeignKeyCascadeChildAction.Operation,
) !void {
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const parent_table_owned = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table_owned);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    const child_key_owned = try alloc.dupe(u8, child_key);
    errdefer alloc.free(child_key_owned);
    const updated_parent_key_owned = if (updated_parent_key) |value| try alloc.dupe(u8, value) else null;
    errdefer if (updated_parent_key_owned) |value| alloc.free(value);
    try participant.foreign_key_cascade_children.append(alloc, .{
        .constraint_name = constraint_name,
        .parent_table = parent_table_owned,
        .parent_key = parent_key_owned,
        .child_key = child_key_owned,
        .updated_parent_key = updated_parent_key_owned,
        .operation = operation,
    });
}

const foreign_key_action_schedule_worker_id = "txn-coordinator";
const foreign_key_action_schedule_page_limit: usize = 1024;
const foreign_key_action_default_cascade_max_depth: u32 = 64;

fn appendForeignKeyActionScheduleMutation(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    child_table_name: []const u8,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    scheduler_group_id: u64,
    cascade_depth: u32,
    cascade_max_depth: u32,
) !void {
    const action = switch (foreign_key.on_delete) {
        .set_null => "set_null",
        .cascade => "cascade",
        else => return error.UnsupportedOperation,
    };
    try appendForeignKeyActionScheduleMutationForAction(alloc, participant, child_table_name, foreign_key, action, parent_key, null, scheduler_group_id, cascade_depth, cascade_max_depth);
}

fn appendForeignKeyActionScheduleMutationForUpdate(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    child_table_name: []const u8,
    foreign_key: storage_schema.ForeignKey,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8,
    scheduler_group_id: u64,
) !void {
    const action = switch (foreign_key.on_update) {
        .set_null => "update_set_null",
        .cascade => blk: {
            if (updated_parent_key == null) return error.UnsupportedOperation;
            break :blk "update_cascade";
        },
        else => return error.UnsupportedOperation,
    };
    try appendForeignKeyActionScheduleMutationForAction(alloc, participant, child_table_name, foreign_key, action, parent_key, updated_parent_key, scheduler_group_id, 0, foreign_key_action_default_cascade_max_depth);
}

fn appendForeignKeyActionScheduleMutationForAction(
    alloc: std.mem.Allocator,
    participant: *ParticipantTxn,
    child_table_name: []const u8,
    foreign_key: storage_schema.ForeignKey,
    action: []const u8,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8,
    scheduler_group_id: u64,
    cascade_depth: u32,
    cascade_max_depth: u32,
) !void {
    const action_job_id = try foreignKeyActionJobIdAlloc(alloc, action, child_table_name, foreign_key.name, foreign_key.parent_table, parent_key, updated_parent_key, cascade_depth, cascade_max_depth);
    errdefer alloc.free(action_job_id);
    const schedule_id = try foreignKeyActionScheduleIdAlloc(alloc, action, child_table_name, foreign_key.name, foreign_key.parent_table, parent_key, updated_parent_key, scheduler_group_id, cascade_depth, cascade_max_depth);
    errdefer alloc.free(schedule_id);
    const action_owned = try alloc.dupe(u8, action);
    errdefer alloc.free(action_owned);
    const worker_id = try alloc.dupe(u8, foreign_key_action_schedule_worker_id);
    errdefer alloc.free(worker_id);
    const constraint_name = try alloc.dupe(u8, foreign_key.name);
    errdefer alloc.free(constraint_name);
    const parent_table = try alloc.dupe(u8, foreign_key.parent_table);
    errdefer alloc.free(parent_table);
    const parent_key_owned = try alloc.dupe(u8, parent_key);
    errdefer alloc.free(parent_key_owned);
    const updated_parent_key_owned = if (updated_parent_key) |value| try alloc.dupe(u8, value) else null;
    errdefer if (updated_parent_key_owned) |value| alloc.free(value);
    try participant.foreign_key_action_schedules.append(alloc, .{
        .schedule_id = schedule_id,
        .action_job_id = action_job_id,
        .action = action_owned,
        .worker_id = worker_id,
        .constraint_name = constraint_name,
        .parent_table = parent_table,
        .parent_key = parent_key_owned,
        .updated_parent_key = updated_parent_key_owned,
        .page_limit = foreign_key_action_schedule_page_limit,
        .cascade_depth = cascade_depth,
        .cascade_max_depth = cascade_max_depth,
    });
}

fn foreignKeyActionJobIdAlloc(
    alloc: std.mem.Allocator,
    action: []const u8,
    child_table_name: []const u8,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8,
    cascade_depth: u32,
    cascade_max_depth: u32,
) ![]u8 {
    const replacement_key = updated_parent_key orelse "";
    return try std.fmt.allocPrint(alloc, "fk-action:v4:{d}:{d}:{d}:{s}:{d}:{s}:{d}:{s}:{d}:{s}:{d}:{s}:{d}:{s}", .{
        cascade_depth,
        cascade_max_depth,
        action.len,
        action,
        child_table_name.len,
        child_table_name,
        constraint_name.len,
        constraint_name,
        parent_table.len,
        parent_table,
        parent_key.len,
        parent_key,
        replacement_key.len,
        replacement_key,
    });
}

fn foreignKeyActionScheduleIdAlloc(
    alloc: std.mem.Allocator,
    action: []const u8,
    child_table_name: []const u8,
    constraint_name: []const u8,
    parent_table: []const u8,
    parent_key: []const u8,
    updated_parent_key: ?[]const u8,
    scheduler_group_id: u64,
    cascade_depth: u32,
    cascade_max_depth: u32,
) ![]u8 {
    const replacement_key = updated_parent_key orelse "";
    return try std.fmt.allocPrint(alloc, "fk-action-schedule:v4:{d}:{d}:{d}:{s}:{d}:{s}:{d}:{s}:{d}:{s}:{d}:{s}:{d}:{s}:{d}", .{
        cascade_depth,
        cascade_max_depth,
        action.len,
        action,
        child_table_name.len,
        child_table_name,
        constraint_name.len,
        constraint_name,
        parent_table.len,
        parent_table,
        parent_key.len,
        parent_key,
        replacement_key.len,
        replacement_key,
        scheduler_group_id,
    });
}

fn foreignKeyParentReferenceFromJsonAlloc(
    alloc: std.mem.Allocator,
    catalog: table_catalog.CatalogSource,
    child_table_name: []const u8,
    child_runtime_table: []const u8,
    columns: []const storage_schema.RelationalColumn,
    foreign_key: storage_schema.ForeignKey,
    value: []const u8,
) !?[]u8 {
    if (foreign_key.validation_state != .enforced) return null;
    const row = try document_mapper.buildRelationalRowValueAlloc(alloc, value, columns);
    defer alloc.free(row);
    if (foreignKeyReferencesPrimaryKey(foreign_key)) {
        return try relational_store.foreignKeyReferenceValueAlloc(alloc, row, foreign_key);
    }

    const parent_table_name = foreignKeyParentCatalogTableName(child_table_name, child_runtime_table, foreign_key);
    const parent_schema_json = (try table_catalog.tableSchemaJsonAlloc(alloc, catalog, parent_table_name)) orelse return error.TableNotFound;
    defer alloc.free(parent_schema_json);
    var parsed_parent_schema = try schema_mod.parseValidatedTableSchema(alloc, parent_schema_json);
    defer parsed_parent_schema.deinit(alloc);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_parent_schema);
    defer storage_schema.freeSchema(alloc, parent_runtime_schema);
    if (parent_runtime_schema.storage_mode != .relational) return error.UnsupportedOperation;
    return try relational_store.foreignKeyReferenceValueWithColumnsAlloc(
        alloc,
        row,
        foreign_key,
        parent_runtime_schema.primary_key,
        parent_runtime_schema.relational_columns,
    );
}

const ForeignKeyChildPeriodBounds = struct {
    start_json: ?[]const u8 = null,
    end_json: ?[]const u8 = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.start_json) |json| alloc.free(@constCast(json));
        if (self.end_json) |json| alloc.free(@constCast(json));
        self.* = .{};
    }
};

fn foreignKeyChildPeriodBoundsFromJsonAlloc(
    alloc: std.mem.Allocator,
    periods: []const storage_schema.RelationalPeriod,
    foreign_key: storage_schema.ForeignKey,
    value: []const u8,
) !ForeignKeyChildPeriodBounds {
    const period_name = foreign_key.child_period orelse return .{};
    const period = relationalPeriodByName(periods, period_name) orelse return error.InvalidTxnRequest;
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, value, .{}) catch return error.InvalidTxnRequest;
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |object| object,
        else => return error.InvalidTxnRequest,
    };
    const start_value = obj.get(period.start_column) orelse return error.InvalidTxnRequest;
    const end_value = obj.get(period.end_column) orelse return error.InvalidTxnRequest;
    const start_json = try std.json.Stringify.valueAlloc(alloc, start_value, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(start_json);
    const end_json = try std.json.Stringify.valueAlloc(alloc, end_value, .{ .emit_null_optional_fields = false });
    errdefer alloc.free(end_json);
    return .{ .start_json = start_json, .end_json = end_json };
}

fn relationalPeriodByName(periods: []const storage_schema.RelationalPeriod, name: []const u8) ?storage_schema.RelationalPeriod {
    for (periods) |period| {
        if (std.mem.eql(u8, period.name, name)) return period;
    }
    return null;
}

test "foreign key action schedule ids include the mutating action" {
    const alloc = std.testing.allocator;

    const set_null_job_id = try foreignKeyActionJobIdAlloc(
        alloc,
        "set_null",
        "orders",
        "orders_customer_id_fkey",
        "customers",
        "customer:hot",
        null,
        0,
        foreign_key_action_default_cascade_max_depth,
    );
    defer alloc.free(set_null_job_id);
    const cascade_job_id = try foreignKeyActionJobIdAlloc(
        alloc,
        "cascade",
        "orders",
        "orders_customer_id_fkey",
        "customers",
        "customer:hot",
        null,
        0,
        foreign_key_action_default_cascade_max_depth,
    );
    defer alloc.free(cascade_job_id);
    try std.testing.expect(!std.mem.eql(u8, set_null_job_id, cascade_job_id));
    try std.testing.expect(std.mem.indexOf(u8, set_null_job_id, "set_null") != null);
    try std.testing.expect(std.mem.indexOf(u8, cascade_job_id, "cascade") != null);

    const set_null_schedule_id = try foreignKeyActionScheduleIdAlloc(
        alloc,
        "set_null",
        "orders",
        "orders_customer_id_fkey",
        "customers",
        "customer:hot",
        null,
        9001,
        0,
        foreign_key_action_default_cascade_max_depth,
    );
    defer alloc.free(set_null_schedule_id);
    const cascade_schedule_id = try foreignKeyActionScheduleIdAlloc(
        alloc,
        "cascade",
        "orders",
        "orders_customer_id_fkey",
        "customers",
        "customer:hot",
        null,
        9001,
        0,
        foreign_key_action_default_cascade_max_depth,
    );
    defer alloc.free(cascade_schedule_id);
    try std.testing.expect(!std.mem.eql(u8, set_null_schedule_id, cascade_schedule_id));
    try std.testing.expect(std.mem.indexOf(u8, set_null_schedule_id, "set_null") != null);
    try std.testing.expect(std.mem.indexOf(u8, cascade_schedule_id, "cascade") != null);
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
    if (!std.mem.startsWith(u8, participant, table_participant_v2_prefix)) return null;
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

test "distributed txn participant ids preserve embedded group markers" {
    const alloc = std.testing.allocator;

    const table_name = "docs:group:shadow";
    const participant = try participantIdForGroup(alloc, table_name, 42);
    defer alloc.free(participant);

    const parsed = parseParticipantRef(participant) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(table_name, parsed.table_name);
    try std.testing.expectEqual(@as(u64, 42), parsed.group_id);

    try std.testing.expect(parseParticipantRef("table:docs:group:42") == null);
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
    try out.appendSlice(alloc, "],\"foreign_key_parent_checks\":[");
    for (req.req.foreign_key_parent_checks, 0..) |check, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeForeignKeyParentCheck(alloc, check);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"foreign_key_parent_delete_checks\":[");
    for (req.req.foreign_key_parent_delete_checks, 0..) |check, i| {
        if (i > 0) try out.append(alloc, ',');
        const timing = switch (check.timing) {
            .immediate => "immediate",
            .deferred => "deferred",
        };
        const operation = switch (check.operation) {
            .delete => "delete",
            .update => "update",
        };
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"constraint_name\":{f},\"parent_table\":{f},\"parent_key\":{f},\"timing\":{f},\"operation\":{f}}}",
            .{
                std.json.fmt(check.constraint_name, .{}),
                std.json.fmt(check.parent_table, .{}),
                std.json.fmt(check.parent_key, .{}),
                std.json.fmt(timing, .{}),
                std.json.fmt(operation, .{}),
            },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"foreign_key_conflict_checks\":[");
    for (req.req.foreign_key_conflict_checks, 0..) |check, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"constraint_name\":{f},\"parent_table\":{f},\"parent_key\":{f}}}",
            .{
                std.json.fmt(check.constraint_name, .{}),
                std.json.fmt(check.parent_table, .{}),
                std.json.fmt(check.parent_key, .{}),
            },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"foreign_key_set_null_children\":[");
    for (req.req.foreign_key_set_null_children, 0..) |action, i| {
        if (i > 0) try out.append(alloc, ',');
        const operation = switch (action.operation) {
            .delete => "delete",
            .update => "update",
        };
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"constraint_name\":{f},\"parent_table\":{f},\"parent_key\":{f},\"child_key\":{f},\"operation\":{f}}}",
            .{
                std.json.fmt(action.constraint_name, .{}),
                std.json.fmt(action.parent_table, .{}),
                std.json.fmt(action.parent_key, .{}),
                std.json.fmt(action.child_key, .{}),
                std.json.fmt(operation, .{}),
            },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"foreign_key_cascade_children\":[");
    for (req.req.foreign_key_cascade_children, 0..) |action, i| {
        if (i > 0) try out.append(alloc, ',');
        const operation = switch (action.operation) {
            .delete => "delete",
            .update => "update",
        };
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"constraint_name\":{f},\"parent_table\":{f},\"parent_key\":{f},\"child_key\":{f},\"operation\":{f}",
            .{
                std.json.fmt(action.constraint_name, .{}),
                std.json.fmt(action.parent_table, .{}),
                std.json.fmt(action.parent_key, .{}),
                std.json.fmt(action.child_key, .{}),
                std.json.fmt(operation, .{}),
            },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
        if (action.updated_parent_key) |updated_parent_key| {
            const replacement = try std.fmt.allocPrint(
                alloc,
                ",\"updated_parent_key\":{f}",
                .{std.json.fmt(updated_parent_key, .{})},
            );
            defer alloc.free(replacement);
            try out.appendSlice(alloc, replacement);
        }
        try out.append(alloc, '}');
    }
    try out.appendSlice(alloc, "],\"foreign_key_action_schedules\":[");
    for (req.req.foreign_key_action_schedules, 0..) |schedule, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"schedule_id\":{f},\"action_job_id\":{f},\"action\":{f},\"worker_id\":{f},\"constraint_name\":{f},\"parent_table\":{f},\"parent_key\":{f}",
            .{
                std.json.fmt(schedule.schedule_id, .{}),
                std.json.fmt(schedule.action_job_id, .{}),
                std.json.fmt(schedule.action, .{}),
                std.json.fmt(schedule.worker_id, .{}),
                std.json.fmt(schedule.constraint_name, .{}),
                std.json.fmt(schedule.parent_table, .{}),
                std.json.fmt(schedule.parent_key, .{}),
            },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
        if (schedule.updated_parent_key) |updated_parent_key| {
            const replacement = try std.fmt.allocPrint(
                alloc,
                ",\"updated_parent_key\":{f}",
                .{std.json.fmt(updated_parent_key, .{})},
            );
            defer alloc.free(replacement);
            try out.appendSlice(alloc, replacement);
        }
        const suffix = try std.fmt.allocPrint(
            alloc,
            ",\"page_limit\":{d},\"cascade_depth\":{d},\"cascade_max_depth\":{d}}}",
            .{ schedule.page_limit, schedule.cascade_depth, schedule.cascade_max_depth },
        );
        defer alloc.free(suffix);
        try out.appendSlice(alloc, suffix);
    }
    try out.appendSlice(alloc, "],\"foreign_key_ref_writes\":[");
    for (req.req.foreign_key_ref_writes, 0..) |mutation, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeForeignKeyRefMutation(alloc, mutation);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"foreign_key_ref_deletes\":[");
    for (req.req.foreign_key_ref_deletes, 0..) |mutation, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeForeignKeyRefMutation(alloc, mutation);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"foreign_key_externalized_parent_checks\":[");
    for (req.req.foreign_key_externalized_parent_checks, 0..) |check, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeForeignKeyParentCheck(alloc, check);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"foreign_key_constraint_timing_overrides\":[");
    for (req.req.foreign_key_constraint_timing_overrides, 0..) |override, i| {
        if (i > 0) try out.append(alloc, ',');
        const timing = switch (override.timing) {
            .immediate => "immediate",
            .deferred => "deferred",
        };
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"constraint_name\":{f},\"timing\":{f}}}",
            .{
                std.json.fmt(override.constraint_name, .{}),
                std.json.fmt(timing, .{}),
            },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"unique_constraint_writes\":[");
    for (req.req.unique_constraint_writes, 0..) |mutation, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeUniqueConstraintMutation(alloc, mutation);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "],\"unique_constraint_deletes\":[");
    for (req.req.unique_constraint_deletes, 0..) |mutation, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try encodeUniqueConstraintMutation(alloc, mutation);
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

fn encodeForeignKeyParentCheck(alloc: std.mem.Allocator, check: db_mod.types.ForeignKeyParentCheck) ![]u8 {
    const timing = switch (check.timing) {
        .immediate => "immediate",
        .deferred => "deferred",
    };
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const encoded = try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"child_table\":{f},\"child_key\":{f},\"parent_table\":{f},\"parent_key\":{f},\"timing\":{f}",
        .{
            std.json.fmt(check.constraint_name, .{}),
            std.json.fmt(check.child_table, .{}),
            std.json.fmt(check.child_key, .{}),
            std.json.fmt(check.parent_table, .{}),
            std.json.fmt(check.parent_key, .{}),
            std.json.fmt(timing, .{}),
        },
    );
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
    if (check.parent_constraint_name) |name| {
        const encoded_name = try std.fmt.allocPrint(alloc, ",\"parent_constraint_name\":{f}", .{std.json.fmt(name, .{})});
        defer alloc.free(encoded_name);
        try out.appendSlice(alloc, encoded_name);
    }
    if (check.child_period_start_json) |json| {
        const encoded_start = try std.fmt.allocPrint(alloc, ",\"child_period_start\":{s}", .{json});
        defer alloc.free(encoded_start);
        try out.appendSlice(alloc, encoded_start);
    }
    if (check.child_period_end_json) |json| {
        const encoded_end = try std.fmt.allocPrint(alloc, ",\"child_period_end\":{s}", .{json});
        defer alloc.free(encoded_end);
        try out.appendSlice(alloc, encoded_end);
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

fn encodeForeignKeyRefMutation(alloc: std.mem.Allocator, mutation: db_mod.types.ForeignKeyRefMutation) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"parent_table\":{f},\"parent_key\":{f},\"child_table\":{f},\"child_key\":{f}}}",
        .{
            std.json.fmt(mutation.constraint_name, .{}),
            std.json.fmt(mutation.parent_table, .{}),
            std.json.fmt(mutation.parent_key, .{}),
            std.json.fmt(mutation.child_table, .{}),
            std.json.fmt(mutation.child_key, .{}),
        },
    );
}

fn encodeUniqueConstraintMutation(alloc: std.mem.Allocator, mutation: db_mod.types.UniqueConstraintMutation) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const encoded = try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"encoded_value\":{f},\"owner_key\":{f}",
        .{
            std.json.fmt(mutation.constraint_name, .{}),
            std.json.fmt(mutation.encoded_value, .{}),
            std.json.fmt(mutation.owner_key, .{}),
        },
    );
    defer alloc.free(encoded);
    try out.appendSlice(alloc, encoded);
    if (mutation.temporal_start) |start| {
        const encoded_start = try std.fmt.allocPrint(alloc, ",\"temporal_start\":{f}", .{std.json.fmt(start, .{})});
        defer alloc.free(encoded_start);
        try out.appendSlice(alloc, encoded_start);
    }
    if (mutation.temporal_end) |end| {
        const encoded_end = try std.fmt.allocPrint(alloc, ",\"temporal_end\":{f}", .{std.json.fmt(end, .{})});
        defer alloc.free(encoded_end);
        try out.appendSlice(alloc, encoded_end);
    }
    try out.append(alloc, '}');
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

pub fn encodeForeignKeyRefChildrenRequest(alloc: std.mem.Allocator, req: ForeignKeyRefChildrenRequest) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    const prefix = try std.fmt.allocPrint(
        alloc,
        "{{\"constraint_name\":{f},\"parent_table\":{f},\"parent_key\":{f},\"limit\":{d}",
        .{ std.json.fmt(req.constraint_name, .{}), std.json.fmt(req.parent_table, .{}), std.json.fmt(req.parent_key, .{}), req.limit },
    );
    defer alloc.free(prefix);
    try out.appendSlice(alloc, prefix);
    if (req.start_after_child_table) |cursor_table| {
        const cursor_key = req.start_after_child_key orelse return error.InvalidTxnRequest;
        const cursor = try std.fmt.allocPrint(
            alloc,
            ",\"start_after_child_table\":{f},\"start_after_child_key\":{f}",
            .{ std.json.fmt(cursor_table, .{}), std.json.fmt(cursor_key, .{}) },
        );
        defer alloc.free(cursor);
        try out.appendSlice(alloc, cursor);
    } else if (req.start_after_child_key != null) {
        return error.InvalidTxnRequest;
    }
    try out.append(alloc, '}');
    return try out.toOwnedSlice(alloc);
}

pub fn encodeForeignKeyRefChildrenResponse(alloc: std.mem.Allocator, response: ForeignKeyRefChildrenResponse) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"children\":[");
    for (response.children, 0..) |child, i| {
        if (i > 0) try out.append(alloc, ',');
        const encoded = try std.fmt.allocPrint(
            alloc,
            "{{\"child_table\":{f},\"child_key\":{f}}}",
            .{ std.json.fmt(child.child_table, .{}), std.json.fmt(child.child_key, .{}) },
        );
        defer alloc.free(encoded);
        try out.appendSlice(alloc, encoded);
    }
    const suffix = try std.fmt.allocPrint(
        alloc,
        "],\"complete\":{s}",
        .{if (response.complete) "true" else "false"},
    );
    defer alloc.free(suffix);
    try out.appendSlice(alloc, suffix);
    if (response.next_child_table) |next_table| {
        const next_key = response.next_child_key orelse return error.InvalidTxnRequest;
        const cursor = try std.fmt.allocPrint(
            alloc,
            ",\"next_child_table\":{f},\"next_child_key\":{f}",
            .{ std.json.fmt(next_table, .{}), std.json.fmt(next_key, .{}) },
        );
        defer alloc.free(cursor);
        try out.appendSlice(alloc, cursor);
    } else if (response.next_child_key != null) {
        return error.InvalidTxnRequest;
    }
    try out.appendSlice(alloc, "}");
    return try out.toOwnedSlice(alloc);
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
    const foreign_key_parent_checks = if (obj.get("foreign_key_parent_checks")) |checks_value|
        try parseTxnForeignKeyParentChecks(alloc, checks_value)
    else
        &.{};
    errdefer freeTxnForeignKeyParentChecks(alloc, foreign_key_parent_checks);
    const foreign_key_parent_delete_checks = if (obj.get("foreign_key_parent_delete_checks")) |checks_value|
        try parseTxnForeignKeyParentDeleteChecks(alloc, checks_value)
    else
        &.{};
    errdefer freeTxnForeignKeyParentDeleteChecks(alloc, foreign_key_parent_delete_checks);
    const foreign_key_conflict_checks = if (obj.get("foreign_key_conflict_checks")) |checks_value|
        try parseTxnForeignKeyConflictChecks(alloc, checks_value)
    else
        &.{};
    errdefer freeTxnForeignKeyConflictChecks(alloc, foreign_key_conflict_checks);
    const foreign_key_set_null_children = if (obj.get("foreign_key_set_null_children")) |actions_value|
        try parseTxnForeignKeySetNullChildren(alloc, actions_value)
    else
        &.{};
    errdefer freeTxnForeignKeySetNullChildren(alloc, foreign_key_set_null_children);
    const foreign_key_cascade_children = if (obj.get("foreign_key_cascade_children")) |actions_value|
        try parseTxnForeignKeyCascadeChildren(alloc, actions_value)
    else
        &.{};
    errdefer freeTxnForeignKeyCascadeChildren(alloc, foreign_key_cascade_children);
    const foreign_key_action_schedules = if (obj.get("foreign_key_action_schedules")) |schedules_value|
        try parseTxnForeignKeyActionSchedules(alloc, schedules_value)
    else
        &.{};
    errdefer freeTxnForeignKeyActionSchedules(alloc, foreign_key_action_schedules);
    const foreign_key_ref_writes = if (obj.get("foreign_key_ref_writes")) |mutations_value|
        try parseTxnForeignKeyRefMutations(alloc, mutations_value)
    else
        &.{};
    errdefer freeTxnForeignKeyRefMutations(alloc, foreign_key_ref_writes);
    const foreign_key_ref_deletes = if (obj.get("foreign_key_ref_deletes")) |mutations_value|
        try parseTxnForeignKeyRefMutations(alloc, mutations_value)
    else
        &.{};
    errdefer freeTxnForeignKeyRefMutations(alloc, foreign_key_ref_deletes);
    const foreign_key_externalized_parent_checks = if (obj.get("foreign_key_externalized_parent_checks")) |checks_value|
        try parseTxnForeignKeyParentChecks(alloc, checks_value)
    else
        &.{};
    errdefer freeTxnForeignKeyParentChecks(alloc, foreign_key_externalized_parent_checks);
    const foreign_key_constraint_timing_overrides = if (obj.get("foreign_key_constraint_timing_overrides")) |overrides_value|
        try parseTxnForeignKeyConstraintTimingOverrides(alloc, overrides_value)
    else
        &.{};
    errdefer freeTxnForeignKeyConstraintTimingOverrides(alloc, foreign_key_constraint_timing_overrides);
    const unique_constraint_writes = if (obj.get("unique_constraint_writes")) |mutations_value|
        try parseTxnUniqueConstraintMutations(alloc, mutations_value)
    else
        &.{};
    errdefer freeTxnUniqueConstraintMutations(alloc, unique_constraint_writes);
    const unique_constraint_deletes = if (obj.get("unique_constraint_deletes")) |mutations_value|
        try parseTxnUniqueConstraintMutations(alloc, mutations_value)
    else
        &.{};
    errdefer freeTxnUniqueConstraintMutations(alloc, unique_constraint_deletes);
    return .{
        .txn_id = txn_id,
        .topology_epoch = requireInteger(obj, "topology_epoch"),
        .req = .{
            .writes = writes,
            .deletes = deletes,
            .transforms = transforms,
            .predicates = predicates,
            .foreign_key_parent_checks = foreign_key_parent_checks,
            .foreign_key_parent_delete_checks = foreign_key_parent_delete_checks,
            .foreign_key_conflict_checks = foreign_key_conflict_checks,
            .foreign_key_set_null_children = foreign_key_set_null_children,
            .foreign_key_cascade_children = foreign_key_cascade_children,
            .foreign_key_action_schedules = foreign_key_action_schedules,
            .foreign_key_ref_writes = foreign_key_ref_writes,
            .foreign_key_ref_deletes = foreign_key_ref_deletes,
            .foreign_key_externalized_parent_checks = foreign_key_externalized_parent_checks,
            .foreign_key_constraint_timing_overrides = foreign_key_constraint_timing_overrides,
            .unique_constraint_writes = unique_constraint_writes,
            .unique_constraint_deletes = unique_constraint_deletes,
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
    freeTxnForeignKeyParentChecks(alloc, req.req.foreign_key_parent_checks);
    freeTxnForeignKeyParentDeleteChecks(alloc, req.req.foreign_key_parent_delete_checks);
    freeTxnForeignKeyConflictChecks(alloc, req.req.foreign_key_conflict_checks);
    freeTxnForeignKeySetNullChildren(alloc, req.req.foreign_key_set_null_children);
    freeTxnForeignKeyCascadeChildren(alloc, req.req.foreign_key_cascade_children);
    freeTxnForeignKeyActionSchedules(alloc, req.req.foreign_key_action_schedules);
    freeTxnForeignKeyRefMutations(alloc, req.req.foreign_key_ref_writes);
    freeTxnForeignKeyRefMutations(alloc, req.req.foreign_key_ref_deletes);
    freeTxnForeignKeyParentChecks(alloc, req.req.foreign_key_externalized_parent_checks);
    freeTxnForeignKeyConstraintTimingOverrides(alloc, req.req.foreign_key_constraint_timing_overrides);
    freeTxnUniqueConstraintMutations(alloc, req.req.unique_constraint_writes);
    freeTxnUniqueConstraintMutations(alloc, req.req.unique_constraint_deletes);
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

pub fn parseForeignKeyRefChildrenRequest(alloc: std.mem.Allocator, body: []const u8) !ForeignKeyRefChildrenRequest {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const constraint_name = try alloc.dupe(u8, requireString(obj, "constraint_name"));
    errdefer alloc.free(constraint_name);
    const parent_table = try alloc.dupe(u8, requireString(obj, "parent_table"));
    errdefer alloc.free(parent_table);
    const parent_key = try alloc.dupe(u8, requireString(obj, "parent_key"));
    errdefer alloc.free(parent_key);
    const start_after_child_table = if (obj.get("start_after_child_table")) |value| blk: {
        const table = switch (value) {
            .string => |s| s,
            .null => break :blk null,
            else => return error.InvalidTxnRequest,
        };
        break :blk try alloc.dupe(u8, table);
    } else null;
    errdefer if (start_after_child_table) |value| alloc.free(value);
    const start_after_child_key = if (obj.get("start_after_child_key")) |value| blk: {
        const key = switch (value) {
            .string => |s| s,
            .null => break :blk null,
            else => return error.InvalidTxnRequest,
        };
        break :blk try alloc.dupe(u8, key);
    } else null;
    errdefer if (start_after_child_key) |value| alloc.free(value);
    if ((start_after_child_table == null) != (start_after_child_key == null)) return error.InvalidTxnRequest;
    return .{
        .constraint_name = constraint_name,
        .parent_table = parent_table,
        .parent_key = parent_key,
        .limit = if (obj.get("limit")) |limit_value| switch (limit_value) {
            .integer => |value| if (value < 0) return error.InvalidTxnRequest else @intCast(value),
            else => return error.InvalidTxnRequest,
        } else 4096,
        .start_after_child_table = start_after_child_table,
        .start_after_child_key = start_after_child_key,
    };
}

pub fn freeForeignKeyRefChildrenRequest(alloc: std.mem.Allocator, req: *ForeignKeyRefChildrenRequest) void {
    alloc.free(@constCast(req.constraint_name));
    alloc.free(@constCast(req.parent_table));
    alloc.free(@constCast(req.parent_key));
    if (req.start_after_child_table) |value| alloc.free(@constCast(value));
    if (req.start_after_child_key) |value| alloc.free(@constCast(value));
    req.* = undefined;
}

pub fn parseForeignKeyRefChildrenResponse(alloc: std.mem.Allocator, body: []const u8) !ForeignKeyRefChildrenResponse {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, body, .{});
    defer parsed.deinit();
    const obj = switch (parsed.value) {
        .object => |obj| obj,
        else => return error.InvalidTxnRequest,
    };
    const children_value = obj.get("children") orelse return error.InvalidTxnRequest;
    const children_array = switch (children_value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, children_array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (children[0..initialized]) |child| {
            alloc.free(@constCast(child.child_table));
            alloc.free(@constCast(child.child_key));
        }
        if (children.len > 0) alloc.free(children);
    }
    for (children_array.items, 0..) |item, i| {
        const child_obj = switch (item) {
            .object => |value| value,
            else => return error.InvalidTxnRequest,
        };
        children[i] = .{
            .child_table = try alloc.dupe(u8, requireString(child_obj, "child_table")),
            .child_key = try alloc.dupe(u8, requireString(child_obj, "child_key")),
        };
        initialized += 1;
    }
    const next_child_table = if (obj.get("next_child_table")) |value| blk: {
        const table = switch (value) {
            .string => |s| s,
            .null => break :blk null,
            else => return error.InvalidTxnRequest,
        };
        break :blk try alloc.dupe(u8, table);
    } else null;
    errdefer if (next_child_table) |value| alloc.free(value);
    const next_child_key = if (obj.get("next_child_key")) |value| blk: {
        const key = switch (value) {
            .string => |s| s,
            .null => break :blk null,
            else => return error.InvalidTxnRequest,
        };
        break :blk try alloc.dupe(u8, key);
    } else null;
    errdefer if (next_child_key) |value| alloc.free(value);
    if ((next_child_table == null) != (next_child_key == null)) return error.InvalidTxnRequest;
    return .{
        .children = children,
        .complete = if (obj.get("complete")) |value| switch (value) {
            .bool => |flag| flag,
            else => return error.InvalidTxnRequest,
        } else true,
        .next_child_table = next_child_table,
        .next_child_key = next_child_key,
    };
}

pub fn freeForeignKeyRefChildrenResponse(alloc: std.mem.Allocator, response: *ForeignKeyRefChildrenResponse) void {
    for (response.children) |child| {
        alloc.free(@constCast(child.child_table));
        alloc.free(@constCast(child.child_key));
    }
    if (response.children.len > 0) alloc.free(response.children);
    if (response.next_child_table) |value| alloc.free(@constCast(value));
    if (response.next_child_key) |value| alloc.free(@constCast(value));
    response.* = undefined;
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
    if (std.mem.eql(u8, text, "$setOnInsert")) return .set_on_insert;
    if (std.mem.eql(u8, text, "$set_on_insert")) return .set_on_insert;
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

fn parseTxnForeignKeyParentChecks(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.ForeignKeyParentCheck {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.ForeignKeyParentCheck, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |check| {
            alloc.free(@constCast(check.constraint_name));
            alloc.free(@constCast(check.child_table));
            alloc.free(@constCast(check.child_key));
            alloc.free(@constCast(check.parent_table));
            alloc.free(@constCast(check.parent_key));
            if (check.parent_constraint_name) |name| alloc.free(@constCast(name));
            if (check.child_period_start_json) |json| alloc.free(@constCast(json));
            if (check.child_period_end_json) |json| alloc.free(@constCast(json));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const constraint_name = requireString(obj, "constraint_name");
        const child_table = requireString(obj, "child_table");
        const child_key = requireString(obj, "child_key");
        const parent_table = requireString(obj, "parent_table");
        const parent_key = requireString(obj, "parent_key");
        const parent_constraint_name = if (obj.get("parent_constraint_name")) |field_value| switch (field_value) {
            .string => |name| name,
            .null => null,
            else => return error.InvalidTxnRequest,
        } else null;
        const child_period_start_json = if (obj.get("child_period_start")) |field_value|
            try std.json.Stringify.valueAlloc(alloc, field_value, .{ .emit_null_optional_fields = false })
        else
            null;
        errdefer if (child_period_start_json) |json| alloc.free(json);
        const child_period_end_json = if (obj.get("child_period_end")) |field_value|
            try std.json.Stringify.valueAlloc(alloc, field_value, .{ .emit_null_optional_fields = false })
        else
            null;
        errdefer if (child_period_end_json) |json| alloc.free(json);
        const timing: db_mod.types.ForeignKeyParentCheck.Timing = if (obj.get("timing")) |field_value| switch (field_value) {
            .string => |name| parseForeignKeyTimingNameForConstraint(name, constraint_name) orelse return error.InvalidTxnRequest,
            else => return error.InvalidTxnRequest,
        } else .immediate;
        if (constraint_name.len == 0 or child_table.len == 0 or child_key.len == 0 or parent_table.len == 0 or parent_key.len == 0) return error.InvalidTxnRequest;
        if (parent_constraint_name) |name| if (name.len == 0) return error.InvalidTxnRequest;
        if ((child_period_start_json == null) != (child_period_end_json == null)) return error.InvalidTxnRequest;
        out[initialized] = .{
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .child_table = try alloc.dupe(u8, child_table),
            .child_key = try alloc.dupe(u8, child_key),
            .parent_table = try alloc.dupe(u8, parent_table),
            .parent_key = try alloc.dupe(u8, parent_key),
            .parent_constraint_name = if (parent_constraint_name) |name| try alloc.dupe(u8, name) else null,
            .child_period_start_json = child_period_start_json,
            .child_period_end_json = child_period_end_json,
            .timing = timing,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnForeignKeyParentChecks(alloc: std.mem.Allocator, checks: []const db_mod.types.ForeignKeyParentCheck) void {
    for (checks) |check| {
        alloc.free(@constCast(check.constraint_name));
        alloc.free(@constCast(check.child_table));
        alloc.free(@constCast(check.child_key));
        alloc.free(@constCast(check.parent_table));
        alloc.free(@constCast(check.parent_key));
        if (check.parent_constraint_name) |name| alloc.free(@constCast(name));
        if (check.child_period_start_json) |json| alloc.free(@constCast(json));
        if (check.child_period_end_json) |json| alloc.free(@constCast(json));
    }
    if (checks.len > 0) alloc.free(@constCast(checks));
}

fn parseTxnForeignKeyConstraintTimingOverrides(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.ForeignKeyConstraintTimingOverride {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.ForeignKeyConstraintTimingOverride, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |override| alloc.free(@constCast(override.constraint_name));
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const constraint_name = requireString(obj, "constraint_name");
        const timing: db_mod.types.ForeignKeyParentCheck.Timing = if (obj.get("timing")) |field_value| switch (field_value) {
            .string => |name| parseForeignKeyTimingNameForConstraint(name, constraint_name) orelse return error.InvalidTxnRequest,
            else => return error.InvalidTxnRequest,
        } else return error.InvalidTxnRequest;
        if (constraint_name.len == 0) return error.InvalidTxnRequest;
        out[initialized] = .{
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .timing = timing,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnForeignKeyConstraintTimingOverrides(alloc: std.mem.Allocator, overrides: []const db_mod.types.ForeignKeyConstraintTimingOverride) void {
    for (overrides) |override| alloc.free(@constCast(override.constraint_name));
    if (overrides.len > 0) alloc.free(@constCast(overrides));
}

fn parseTxnForeignKeyParentDeleteChecks(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.ForeignKeyParentDeleteCheck {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.ForeignKeyParentDeleteCheck, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |check| {
            alloc.free(@constCast(check.constraint_name));
            alloc.free(@constCast(check.parent_table));
            alloc.free(@constCast(check.parent_key));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const constraint_name = requireString(obj, "constraint_name");
        const parent_table = requireString(obj, "parent_table");
        const parent_key = requireString(obj, "parent_key");
        const timing: db_mod.types.ForeignKeyParentCheck.Timing = if (obj.get("timing")) |field_value| switch (field_value) {
            .string => |name| parseForeignKeyTimingNameForConstraint(name, constraint_name) orelse return error.InvalidTxnRequest,
            else => return error.InvalidTxnRequest,
        } else .immediate;
        const operation: db_mod.types.ForeignKeyParentDeleteCheck.Operation = if (obj.get("operation")) |field_value| switch (field_value) {
            .string => |name| parseForeignKeyMutationOperation(name) orelse return error.InvalidTxnRequest,
            else => return error.InvalidTxnRequest,
        } else .delete;
        if (constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0) return error.InvalidTxnRequest;
        out[initialized] = .{
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .parent_table = try alloc.dupe(u8, parent_table),
            .parent_key = try alloc.dupe(u8, parent_key),
            .timing = timing,
            .operation = operation,
        };
        initialized += 1;
    }
    return out;
}

fn parseForeignKeyTimingName(name: []const u8) ?db_mod.types.ForeignKeyParentCheck.Timing {
    return parseForeignKeyTimingNameForConstraint(name, null);
}

fn parseForeignKeyTimingNameForConstraint(name: []const u8, expected_constraint_name: ?[]const u8) ?db_mod.types.ForeignKeyParentCheck.Timing {
    if (enumTokenEql(name, "immediate") or
        enumTokenEql(name, "initially_immediate") or
        enumTokenEql(name, "set_constraint_immediate") or
        enumTokenEql(name, "set_constraints_immediate") or
        enumTokenEql(name, "set_constraints_all_immediate"))
    {
        return .immediate;
    }
    if (enumTokenEql(name, "deferred") or
        enumTokenEql(name, "initially_deferred") or
        enumTokenEql(name, "set_constraint_deferred") or
        enumTokenEql(name, "set_constraints_deferred") or
        enumTokenEql(name, "set_constraints_all_deferred"))
    {
        return .deferred;
    }
    if (parseSetConstraintsTimingName(name, expected_constraint_name)) |timing| return timing;
    return null;
}

fn parseSetConstraintsTimingName(name: []const u8, expected_constraint_name: ?[]const u8) ?db_mod.types.ForeignKeyParentCheck.Timing {
    const statement = trimSqlStatementTerminator(name) orelse return null;
    var index: usize = 0;
    const set_token = nextEnumTokenSpan(statement, &index) orelse return null;
    if (!asciiTokenEql(set_token.text, "set")) return null;
    const constraints_token = nextEnumTokenSpan(statement, &index) orelse return null;
    if (!asciiTokenEql(constraints_token.text, "constraint") and !asciiTokenEql(constraints_token.text, "constraints")) return null;

    const target_start = constraints_token.end;
    var last_token: ?EnumTokenSpan = null;
    while (nextEnumTokenSpan(statement, &index)) |token| {
        last_token = token;
    }
    const timing_token = last_token orelse return null;
    const timing = if (asciiTokenEql(timing_token.text, "immediate"))
        db_mod.types.ForeignKeyParentCheck.Timing.immediate
    else if (asciiTokenEql(timing_token.text, "deferred"))
        db_mod.types.ForeignKeyParentCheck.Timing.deferred
    else
        return null;
    if (expected_constraint_name) |expected| {
        const target = trimEnumTokenSeparators(statement[target_start..timing_token.start]);
        if (!setConstraintsTargetMatchesExpected(target, expected)) return null;
    }
    return timing;
}

fn trimSqlStatementTerminator(name: []const u8) ?[]const u8 {
    var statement = std.mem.trim(u8, name, &std.ascii.whitespace);
    if (statement.len > 0 and statement[statement.len - 1] == ';') {
        statement = std.mem.trim(u8, statement[0 .. statement.len - 1], &std.ascii.whitespace);
    }
    if (std.mem.indexOfScalar(u8, statement, ';') != null) return null;
    return statement;
}

fn setConstraintsTargetMatchesExpected(target: []const u8, expected: []const u8) bool {
    if (target.len == 0) return true;
    if (enumTokenEql(target, "all")) return true;

    var saw_target = false;
    var target_index: usize = 0;
    while (nextSetConstraintsTargetPart(target, &target_index)) |part| {
        const trimmed = trimEnumTokenSeparators(part);
        if (trimmed.len == 0) return false;
        saw_target = true;
        if (setConstraintsTargetIdentifierMatchesExpected(trimmed, expected)) return true;
    }
    return !saw_target;
}

fn nextSetConstraintsTargetPart(target: []const u8, index: *usize) ?[]const u8 {
    if (index.* > target.len) return null;
    if (index.* == target.len) {
        index.* = target.len + 1;
        return "";
    }
    const start = index.*;
    var in_quote = false;
    while (index.* < target.len) {
        const ch = target[index.*];
        if (ch == '"') {
            if (in_quote and index.* + 1 < target.len and target[index.* + 1] == '"') {
                index.* += 2;
                continue;
            }
            in_quote = !in_quote;
            index.* += 1;
            continue;
        }
        if (!in_quote and ch == ',') {
            const end = index.*;
            index.* += 1;
            return target[start..end];
        }
        index.* += 1;
    }
    index.* = target.len + 1;
    return target[start..target.len];
}

fn setConstraintsTargetIdentifierMatchesExpected(target: []const u8, expected: []const u8) bool {
    if (enumTokenEql(target, expected)) return true;
    return quotedSqlIdentifierEql(target, expected);
}

fn quotedSqlIdentifierEql(target: []const u8, expected: []const u8) bool {
    if (target.len < 2 or target[0] != '"') return false;
    var target_index: usize = 1;
    var expected_index: usize = 0;
    while (target_index < target.len) {
        const ch = target[target_index];
        if (ch == '"') {
            if (target_index + 1 < target.len and target[target_index + 1] == '"') {
                if (expected_index == expected.len or expected[expected_index] != '"') return false;
                expected_index += 1;
                target_index += 2;
                continue;
            }
            return target_index + 1 == target.len and expected_index == expected.len;
        }
        if (expected_index == expected.len or expected[expected_index] != ch) return false;
        expected_index += 1;
        target_index += 1;
    }
    return false;
}

const EnumTokenSpan = struct {
    text: []const u8,
    start: usize,
    end: usize,
};

fn nextEnumTokenSpan(text: []const u8, index: *usize) ?EnumTokenSpan {
    while (index.* < text.len and enumTokenSeparator(text[index.*])) index.* += 1;
    if (index.* == text.len) return null;
    const start = index.*;
    while (index.* < text.len and !enumTokenSeparator(text[index.*])) index.* += 1;
    return .{
        .text = text[start..index.*],
        .start = start,
        .end = index.*,
    };
}

fn trimEnumTokenSeparators(text: []const u8) []const u8 {
    var start: usize = 0;
    var end: usize = text.len;
    while (start < end and enumTokenSeparator(text[start])) start += 1;
    while (end > start and enumTokenSeparator(text[end - 1])) end -= 1;
    return text[start..end];
}

fn foreignKeyActionPageCanonicalAction(action: []const u8) ?[]const u8 {
    if (enumTokenEql(action, "set_null") or enumTokenEql(action, "delete_set_null") or enumTokenEql(action, "on_delete_set_null")) return "set_null";
    if (enumTokenEql(action, "cascade") or enumTokenEql(action, "delete_cascade") or enumTokenEql(action, "on_delete_cascade")) return "cascade";
    if (enumTokenEql(action, "update_set_null") or enumTokenEql(action, "on_update_set_null")) return "update_set_null";
    if (enumTokenEql(action, "update_cascade") or enumTokenEql(action, "on_update_cascade")) return "update_cascade";
    return null;
}

fn parseForeignKeyMutationOperation(name: []const u8) ?db_mod.types.ForeignKeyParentDeleteCheck.Operation {
    if (enumTokenEql(name, "delete") or
        enumTokenEql(name, "parent_delete") or
        enumTokenEql(name, "on_delete") or
        enumTokenEql(name, "on_delete_check"))
    {
        return .delete;
    }
    if (enumTokenEql(name, "update") or
        enumTokenEql(name, "parent_update") or
        enumTokenEql(name, "on_update") or
        enumTokenEql(name, "on_update_check"))
    {
        return .update;
    }
    return null;
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

fn asciiTokenEql(actual: []const u8, expected: []const u8) bool {
    if (actual.len != expected.len) return false;
    for (actual, expected) |actual_ch, expected_ch| {
        if (std.ascii.toLower(actual_ch) != std.ascii.toLower(expected_ch)) return false;
    }
    return true;
}

fn enumTokenSeparator(ch: u8) bool {
    return std.ascii.isWhitespace(ch) or ch == '_' or ch == '-';
}

test "txn foreign key timing parser accepts SQL spellings" {
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingName("INITIALLY DEFERRED").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingName("initially-deferred").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.immediate, parseForeignKeyTimingName("initially immediate").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingName("SET CONSTRAINTS DEFERRED").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.immediate, parseForeignKeyTimingName("set-constraints-all-immediate").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingName("SET CONSTRAINTS orders_customer_id_fkey DEFERRED").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.immediate, parseForeignKeyTimingName("set-constraint-orders-customer-id-fkey-immediate").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS orders_customer_id_fkey DEFERRED", "orders_customer_id_fkey").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingNameForConstraint("SET\tCONSTRAINTS\torders_customer_id_fkey\tDEFERRED;", "orders_customer_id_fkey").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.immediate, parseForeignKeyTimingNameForConstraint(" SET CONSTRAINTS ALL IMMEDIATE ; ", "orders_customer_id_fkey").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.immediate, parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS ALL IMMEDIATE", "orders_customer_id_fkey").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS other_customer_id_fkey, orders_customer_id_fkey DEFERRED", "orders_customer_id_fkey").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS \"orders_customer_id_fkey\" DEFERRED", "orders_customer_id_fkey").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.immediate, parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS other_customer_id_fkey, \"orders_customer_id_fkey\" IMMEDIATE", "orders_customer_id_fkey").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS \"orders_customer\"\"id_fkey\" DEFERRED", "orders_customer\"id_fkey").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS other_customer_id_fkey, \"orders,customer_fkey\" DEFERRED", "orders,customer_fkey").?);
    try std.testing.expect(parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS other_customer_id_fkey DEFERRED", "orders_customer_id_fkey") == null);
    try std.testing.expect(parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS other_customer_id_fkey, third_customer_id_fkey DEFERRED", "orders_customer_id_fkey") == null);
    try std.testing.expect(parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS other_customer_id_fkey, DEFERRED", "orders_customer_id_fkey") == null);
    try std.testing.expect(parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS \"Orders_Customer_Id_Fkey\" DEFERRED", "orders_customer_id_fkey") == null);
    try std.testing.expect(parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS \"orders_customer_id_fkey DEFERRED", "orders_customer_id_fkey") == null);
    try std.testing.expect(parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS orders_customer_id_fkey; DEFERRED", "orders_customer_id_fkey") == null);
    try std.testing.expect(parseForeignKeyTimingNameForConstraint("SET CONSTRAINTS orders_customer_id_fkey DEFERRED;;", "orders_customer_id_fkey") == null);
    try std.testing.expect(parseForeignKeyTimingName("SET CONSTRAINTS orders_customer_id_fkey") == null);
    try std.testing.expect(parseForeignKeyTimingName("eventually") == null);
}

test "txn foreign key mutation operation parser accepts SQL spellings" {
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentDeleteCheck.Operation.delete, parseForeignKeyMutationOperation("DELETE").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentDeleteCheck.Operation.delete, parseForeignKeyMutationOperation("parent-delete").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentDeleteCheck.Operation.delete, parseForeignKeyMutationOperation("ON DELETE").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentDeleteCheck.Operation.update, parseForeignKeyMutationOperation("UPDATE").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentDeleteCheck.Operation.update, parseForeignKeyMutationOperation("parent_update").?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentDeleteCheck.Operation.update, parseForeignKeyMutationOperation("on update check").?);
    try std.testing.expect(parseForeignKeyMutationOperation("merge") == null);
}

fn freeTxnForeignKeyParentDeleteChecks(alloc: std.mem.Allocator, checks: []const db_mod.types.ForeignKeyParentDeleteCheck) void {
    for (checks) |check| {
        alloc.free(@constCast(check.constraint_name));
        alloc.free(@constCast(check.parent_table));
        alloc.free(@constCast(check.parent_key));
    }
    if (checks.len > 0) alloc.free(@constCast(checks));
}

fn parseTxnForeignKeyConflictChecks(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.ForeignKeyConflictCheck {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.ForeignKeyConflictCheck, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |check| {
            alloc.free(@constCast(check.constraint_name));
            alloc.free(@constCast(check.parent_table));
            alloc.free(@constCast(check.parent_key));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const constraint_name = requireString(obj, "constraint_name");
        const parent_table = requireString(obj, "parent_table");
        const parent_key = requireString(obj, "parent_key");
        if (constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0) return error.InvalidTxnRequest;
        out[initialized] = .{
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .parent_table = try alloc.dupe(u8, parent_table),
            .parent_key = try alloc.dupe(u8, parent_key),
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnForeignKeyConflictChecks(alloc: std.mem.Allocator, checks: []const db_mod.types.ForeignKeyConflictCheck) void {
    for (checks) |check| {
        alloc.free(@constCast(check.constraint_name));
        alloc.free(@constCast(check.parent_table));
        alloc.free(@constCast(check.parent_key));
    }
    if (checks.len > 0) alloc.free(@constCast(checks));
}

fn parseTxnForeignKeySetNullChildren(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.ForeignKeySetNullChildAction {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.ForeignKeySetNullChildAction, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |action| {
            alloc.free(@constCast(action.constraint_name));
            alloc.free(@constCast(action.parent_table));
            alloc.free(@constCast(action.parent_key));
            alloc.free(@constCast(action.child_key));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const constraint_name = requireString(obj, "constraint_name");
        const parent_table = requireString(obj, "parent_table");
        const parent_key = requireString(obj, "parent_key");
        const child_key = requireString(obj, "child_key");
        const operation: db_mod.types.ForeignKeySetNullChildAction.Operation = if (obj.get("operation")) |field_value| switch (field_value) {
            .string => |text| switch (parseForeignKeyMutationOperation(text) orelse return error.InvalidTxnRequest) {
                .delete => .delete,
                .update => .update,
            },
            else => return error.InvalidTxnRequest,
        } else .delete;
        if (constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0 or child_key.len == 0) return error.InvalidTxnRequest;
        out[initialized] = .{
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .parent_table = try alloc.dupe(u8, parent_table),
            .parent_key = try alloc.dupe(u8, parent_key),
            .child_key = try alloc.dupe(u8, child_key),
            .operation = operation,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnForeignKeySetNullChildren(alloc: std.mem.Allocator, actions: []const db_mod.types.ForeignKeySetNullChildAction) void {
    for (actions) |action| {
        alloc.free(@constCast(action.constraint_name));
        alloc.free(@constCast(action.parent_table));
        alloc.free(@constCast(action.parent_key));
        alloc.free(@constCast(action.child_key));
    }
    if (actions.len > 0) alloc.free(@constCast(actions));
}

fn parseTxnForeignKeyCascadeChildren(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.ForeignKeyCascadeChildAction {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.ForeignKeyCascadeChildAction, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |action| {
            alloc.free(@constCast(action.constraint_name));
            alloc.free(@constCast(action.parent_table));
            alloc.free(@constCast(action.parent_key));
            alloc.free(@constCast(action.child_key));
            if (action.updated_parent_key) |updated_key| alloc.free(@constCast(updated_key));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const constraint_name = requireString(obj, "constraint_name");
        const parent_table = requireString(obj, "parent_table");
        const parent_key = requireString(obj, "parent_key");
        const child_key = requireString(obj, "child_key");
        const updated_parent_key = if (obj.get("updated_parent_key")) |field_value| switch (field_value) {
            .string => |text| text,
            else => return error.InvalidTxnRequest,
        } else null;
        const operation: db_mod.types.ForeignKeyCascadeChildAction.Operation = if (obj.get("operation")) |field_value| switch (field_value) {
            .string => |text| switch (parseForeignKeyMutationOperation(text) orelse return error.InvalidTxnRequest) {
                .delete => .delete,
                .update => .update,
            },
            else => return error.InvalidTxnRequest,
        } else .delete;
        if (constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0 or child_key.len == 0) return error.InvalidTxnRequest;
        if (operation == .update and (updated_parent_key == null or updated_parent_key.?.len == 0)) return error.InvalidTxnRequest;
        if (operation == .delete and updated_parent_key != null) return error.InvalidTxnRequest;
        out[initialized] = .{
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .parent_table = try alloc.dupe(u8, parent_table),
            .parent_key = try alloc.dupe(u8, parent_key),
            .child_key = try alloc.dupe(u8, child_key),
            .updated_parent_key = if (updated_parent_key) |updated_key| try alloc.dupe(u8, updated_key) else null,
            .operation = operation,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnForeignKeyCascadeChildren(alloc: std.mem.Allocator, actions: []const db_mod.types.ForeignKeyCascadeChildAction) void {
    for (actions) |action| {
        alloc.free(@constCast(action.constraint_name));
        alloc.free(@constCast(action.parent_table));
        alloc.free(@constCast(action.parent_key));
        alloc.free(@constCast(action.child_key));
        if (action.updated_parent_key) |value| alloc.free(@constCast(value));
    }
    if (actions.len > 0) alloc.free(@constCast(actions));
}

fn parseTxnForeignKeyActionSchedules(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.ForeignKeyActionScheduleMutation {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.ForeignKeyActionScheduleMutation, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |schedule| {
            freeForeignKeyActionScheduleMutationFields(alloc, schedule);
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const schedule_id = requireString(obj, "schedule_id");
        const action_job_id = requireString(obj, "action_job_id");
        const action = requireString(obj, "action");
        const worker_id = requireString(obj, "worker_id");
        const constraint_name = requireString(obj, "constraint_name");
        const parent_table = requireString(obj, "parent_table");
        const parent_key = requireString(obj, "parent_key");
        const updated_parent_key = if (obj.get("updated_parent_key")) |field_value| switch (field_value) {
            .string => |text| text,
            else => return error.InvalidTxnRequest,
        } else null;
        const page_limit = requireInteger(obj, "page_limit");
        const cascade_depth = requireInteger(obj, "cascade_depth");
        const cascade_max_depth = requireInteger(obj, "cascade_max_depth");
        if (schedule_id.len == 0 or action_job_id.len == 0 or action.len == 0 or worker_id.len == 0 or constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0 or page_limit == 0) return error.InvalidTxnRequest;
        if (updated_parent_key != null and updated_parent_key.?.len == 0) return error.InvalidTxnRequest;
        if (cascade_max_depth == 0 or cascade_depth > cascade_max_depth or cascade_depth > std.math.maxInt(u32) or cascade_max_depth > std.math.maxInt(u32)) return error.InvalidTxnRequest;
        out[initialized] = .{
            .schedule_id = try alloc.dupe(u8, schedule_id),
            .action_job_id = try alloc.dupe(u8, action_job_id),
            .action = try alloc.dupe(u8, action),
            .worker_id = try alloc.dupe(u8, worker_id),
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .parent_table = try alloc.dupe(u8, parent_table),
            .parent_key = try alloc.dupe(u8, parent_key),
            .updated_parent_key = if (updated_parent_key) |updated_key| try alloc.dupe(u8, updated_key) else null,
            .page_limit = @intCast(page_limit),
            .cascade_depth = @intCast(cascade_depth),
            .cascade_max_depth = @intCast(cascade_max_depth),
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnForeignKeyActionSchedules(alloc: std.mem.Allocator, schedules: []const db_mod.types.ForeignKeyActionScheduleMutation) void {
    for (schedules) |schedule| {
        freeForeignKeyActionScheduleMutationFields(alloc, schedule);
    }
    if (schedules.len > 0) alloc.free(@constCast(schedules));
}

fn parseTxnForeignKeyRefMutations(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.ForeignKeyRefMutation {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.ForeignKeyRefMutation, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |mutation| {
            alloc.free(@constCast(mutation.constraint_name));
            alloc.free(@constCast(mutation.parent_table));
            alloc.free(@constCast(mutation.parent_key));
            alloc.free(@constCast(mutation.child_table));
            alloc.free(@constCast(mutation.child_key));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const constraint_name = requireString(obj, "constraint_name");
        const parent_table = requireString(obj, "parent_table");
        const parent_key = requireString(obj, "parent_key");
        const child_table = requireString(obj, "child_table");
        const child_key = requireString(obj, "child_key");
        if (constraint_name.len == 0 or parent_table.len == 0 or parent_key.len == 0 or child_table.len == 0 or child_key.len == 0) return error.InvalidTxnRequest;
        out[initialized] = .{
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .parent_table = try alloc.dupe(u8, parent_table),
            .parent_key = try alloc.dupe(u8, parent_key),
            .child_table = try alloc.dupe(u8, child_table),
            .child_key = try alloc.dupe(u8, child_key),
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnForeignKeyRefMutations(alloc: std.mem.Allocator, mutations: []const db_mod.types.ForeignKeyRefMutation) void {
    for (mutations) |mutation| {
        alloc.free(@constCast(mutation.constraint_name));
        alloc.free(@constCast(mutation.parent_table));
        alloc.free(@constCast(mutation.parent_key));
        alloc.free(@constCast(mutation.child_table));
        alloc.free(@constCast(mutation.child_key));
    }
    if (mutations.len > 0) alloc.free(@constCast(mutations));
}

fn optionalString(obj: std.json.ObjectMap, key: []const u8) ?[]const u8 {
    const value = obj.get(key) orelse return null;
    return switch (value) {
        .string => |s| s,
        else => null,
    };
}

fn parseTxnUniqueConstraintMutations(alloc: std.mem.Allocator, value: std.json.Value) ![]const db_mod.types.UniqueConstraintMutation {
    const arr = switch (value) {
        .array => |arr| arr,
        else => return error.InvalidTxnRequest,
    };
    var out = try alloc.alloc(db_mod.types.UniqueConstraintMutation, arr.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |mutation| {
            alloc.free(@constCast(mutation.constraint_name));
            alloc.free(@constCast(mutation.encoded_value));
            alloc.free(@constCast(mutation.owner_key));
            if (mutation.temporal_start) |start| alloc.free(@constCast(start));
            if (mutation.temporal_end) |end| alloc.free(@constCast(end));
        }
        alloc.free(out);
    }
    for (arr.items) |item| {
        const obj = switch (item) {
            .object => |inner| inner,
            else => return error.InvalidTxnRequest,
        };
        const constraint_name = requireString(obj, "constraint_name");
        const encoded_value = requireString(obj, "encoded_value");
        const owner_key = requireString(obj, "owner_key");
        const temporal_start = optionalString(obj, "temporal_start");
        const temporal_end = optionalString(obj, "temporal_end");
        if (constraint_name.len == 0 or encoded_value.len == 0 or owner_key.len == 0) return error.InvalidTxnRequest;
        if ((temporal_start == null) != (temporal_end == null)) return error.InvalidTxnRequest;
        out[initialized] = .{
            .constraint_name = try alloc.dupe(u8, constraint_name),
            .encoded_value = try alloc.dupe(u8, encoded_value),
            .owner_key = try alloc.dupe(u8, owner_key),
            .temporal_start = if (temporal_start) |start| try alloc.dupe(u8, start) else null,
            .temporal_end = if (temporal_end) |end| try alloc.dupe(u8, end) else null,
        };
        initialized += 1;
    }
    return out;
}

fn freeTxnUniqueConstraintMutations(alloc: std.mem.Allocator, mutations: []const db_mod.types.UniqueConstraintMutation) void {
    for (mutations) |mutation| {
        alloc.free(@constCast(mutation.constraint_name));
        alloc.free(@constCast(mutation.encoded_value));
        alloc.free(@constCast(mutation.owner_key));
        if (mutation.temporal_start) |start| alloc.free(@constCast(start));
        if (mutation.temporal_end) |end| alloc.free(@constCast(end));
    }
    if (mutations.len > 0) alloc.free(@constCast(mutations));
}

fn requireString(obj: std.json.ObjectMap, key: []const u8) []const u8 {
    const value = obj.get(key) orelse return "";
    return switch (value) {
        .string => |s| s,
        else => "",
    };
}

test "foreign key ref children request and response round-trip cursors" {
    const alloc = std.testing.allocator;

    const req_body = try encodeForeignKeyRefChildrenRequest(alloc, .{
        .constraint_name = "orders_customer_id_fkey",
        .parent_table = "customers",
        .parent_key = "customer:page",
        .limit = 2,
        .start_after_child_table = "row",
        .start_after_child_key = "order:b",
    });
    defer alloc.free(req_body);

    var parsed_req = try parseForeignKeyRefChildrenRequest(alloc, req_body);
    defer freeForeignKeyRefChildrenRequest(alloc, &parsed_req);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed_req.constraint_name);
    try std.testing.expectEqualStrings("customers", parsed_req.parent_table);
    try std.testing.expectEqualStrings("customer:page", parsed_req.parent_key);
    try std.testing.expectEqual(@as(usize, 2), parsed_req.limit);
    try std.testing.expectEqualStrings("row", parsed_req.start_after_child_table.?);
    try std.testing.expectEqualStrings("order:b", parsed_req.start_after_child_key.?);
    try std.testing.expectError(error.InvalidTxnRequest, parseForeignKeyRefChildrenRequest(alloc,
        \\{"constraint_name":"orders_customer_id_fkey","parent_table":"customers","parent_key":"customer:page","start_after_child_table":"row"}
    ));

    const children = [_]db_mod.types.ForeignKeyRefChild{
        .{ .child_table = "row", .child_key = "order:c" },
    };
    const response_body = try encodeForeignKeyRefChildrenResponse(alloc, .{
        .children = children[0..],
        .complete = false,
        .next_child_table = "row",
        .next_child_key = "order:c",
    });
    defer alloc.free(response_body);

    var parsed_response = try parseForeignKeyRefChildrenResponse(alloc, response_body);
    defer freeForeignKeyRefChildrenResponse(alloc, &parsed_response);
    try std.testing.expect(!parsed_response.complete);
    try std.testing.expectEqual(@as(usize, 1), parsed_response.children.len);
    try std.testing.expectEqualStrings("row", parsed_response.children[0].child_table);
    try std.testing.expectEqualStrings("order:c", parsed_response.children[0].child_key);
    try std.testing.expectEqualStrings("row", parsed_response.next_child_table.?);
    try std.testing.expectEqualStrings("order:c", parsed_response.next_child_key.?);
    try std.testing.expectError(error.InvalidTxnRequest, parseForeignKeyRefChildrenResponse(alloc,
        \\{"children":[],"complete":false,"next_child_table":"row"}
    ));
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

test "txn prepare parser round-trips constraint participant intents" {
    const alloc = std.testing.allocator;
    const txn_id = try parseTxnIdHex("00112233445566778899aabbccddeeff");
    const body = try encodeTxnPrepareRequest(alloc, .{
        .txn_id = txn_id,
        .topology_epoch = 7,
        .req = .{
            .foreign_key_parent_checks = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .child_table = "orders",
                .child_key = "order:1",
                .parent_table = "customers",
                .parent_key = "customer:1",
                .parent_constraint_name = "customers_id_key",
                .child_period_start_json = "10",
                .child_period_end_json = "20",
                .timing = .deferred,
            }},
            .foreign_key_parent_delete_checks = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:2",
            }},
            .foreign_key_conflict_checks = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:5",
            }},
            .foreign_key_set_null_children = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:6",
                .child_key = "order:6",
            }},
            .foreign_key_cascade_children = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:7",
                .child_key = "order:7",
            }},
            .foreign_key_action_schedules = &.{.{
                .schedule_id = "fk-action-schedule:orders:customers:customer:8",
                .action_job_id = "fk-action:orders:customers:customer:8",
                .action = "cascade",
                .worker_id = "txn-coordinator",
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:8",
                .page_limit = 512,
                .cascade_depth = 3,
                .cascade_max_depth = 9,
            }},
            .foreign_key_ref_writes = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:3",
                .child_table = "orders",
                .child_key = "order:3",
            }},
            .foreign_key_ref_deletes = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .parent_table = "customers",
                .parent_key = "customer:4",
                .child_table = "orders",
                .child_key = "order:4",
            }},
            .foreign_key_externalized_parent_checks = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .child_table = "orders",
                .child_key = "order:9",
                .parent_table = "customers",
                .parent_key = "customer:9",
                .timing = .deferred,
            }},
            .foreign_key_constraint_timing_overrides = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .timing = .immediate,
            }},
            .unique_constraint_writes = &.{.{
                .constraint_name = "users_email_key",
                .encoded_value = "email:ada",
                .owner_key = "user:1",
            }},
            .unique_constraint_deletes = &.{.{
                .constraint_name = "users_email_key",
                .encoded_value = "email:grace",
                .owner_key = "user:2",
            }},
        },
    });
    defer alloc.free(body);

    var parsed = try parseTxnPrepareRequest(alloc, body);
    defer freeTxnPrepareRequest(alloc, &parsed);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_parent_checks.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_parent_checks[0].constraint_name);
    try std.testing.expectEqualStrings("orders", parsed.req.foreign_key_parent_checks[0].child_table);
    try std.testing.expectEqualStrings("order:1", parsed.req.foreign_key_parent_checks[0].child_key);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_parent_checks[0].parent_table);
    try std.testing.expectEqualStrings("customer:1", parsed.req.foreign_key_parent_checks[0].parent_key);
    try std.testing.expectEqualStrings("customers_id_key", parsed.req.foreign_key_parent_checks[0].parent_constraint_name.?);
    try std.testing.expectEqualStrings("10", parsed.req.foreign_key_parent_checks[0].child_period_start_json.?);
    try std.testing.expectEqualStrings("20", parsed.req.foreign_key_parent_checks[0].child_period_end_json.?);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parsed.req.foreign_key_parent_checks[0].timing);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_parent_delete_checks.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_parent_delete_checks[0].constraint_name);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_parent_delete_checks[0].parent_table);
    try std.testing.expectEqualStrings("customer:2", parsed.req.foreign_key_parent_delete_checks[0].parent_key);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.immediate, parsed.req.foreign_key_parent_delete_checks[0].timing);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_conflict_checks.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_conflict_checks[0].constraint_name);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_conflict_checks[0].parent_table);
    try std.testing.expectEqualStrings("customer:5", parsed.req.foreign_key_conflict_checks[0].parent_key);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_set_null_children.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_set_null_children[0].constraint_name);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_set_null_children[0].parent_table);
    try std.testing.expectEqualStrings("customer:6", parsed.req.foreign_key_set_null_children[0].parent_key);
    try std.testing.expectEqualStrings("order:6", parsed.req.foreign_key_set_null_children[0].child_key);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_cascade_children.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_cascade_children[0].constraint_name);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_cascade_children[0].parent_table);
    try std.testing.expectEqualStrings("customer:7", parsed.req.foreign_key_cascade_children[0].parent_key);
    try std.testing.expectEqualStrings("order:7", parsed.req.foreign_key_cascade_children[0].child_key);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_action_schedules.len);
    try std.testing.expectEqualStrings("fk-action-schedule:orders:customers:customer:8", parsed.req.foreign_key_action_schedules[0].schedule_id);
    try std.testing.expectEqualStrings("fk-action:orders:customers:customer:8", parsed.req.foreign_key_action_schedules[0].action_job_id);
    try std.testing.expectEqualStrings("cascade", parsed.req.foreign_key_action_schedules[0].action);
    try std.testing.expectEqualStrings("txn-coordinator", parsed.req.foreign_key_action_schedules[0].worker_id);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_action_schedules[0].constraint_name);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_action_schedules[0].parent_table);
    try std.testing.expectEqualStrings("customer:8", parsed.req.foreign_key_action_schedules[0].parent_key);
    try std.testing.expectEqual(@as(usize, 512), parsed.req.foreign_key_action_schedules[0].page_limit);
    try std.testing.expectEqual(@as(u32, 3), parsed.req.foreign_key_action_schedules[0].cascade_depth);
    try std.testing.expectEqual(@as(u32, 9), parsed.req.foreign_key_action_schedules[0].cascade_max_depth);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_ref_writes.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_ref_writes[0].constraint_name);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_ref_writes[0].parent_table);
    try std.testing.expectEqualStrings("customer:3", parsed.req.foreign_key_ref_writes[0].parent_key);
    try std.testing.expectEqualStrings("orders", parsed.req.foreign_key_ref_writes[0].child_table);
    try std.testing.expectEqualStrings("order:3", parsed.req.foreign_key_ref_writes[0].child_key);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_ref_deletes.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_ref_deletes[0].constraint_name);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_ref_deletes[0].parent_table);
    try std.testing.expectEqualStrings("customer:4", parsed.req.foreign_key_ref_deletes[0].parent_key);
    try std.testing.expectEqualStrings("orders", parsed.req.foreign_key_ref_deletes[0].child_table);
    try std.testing.expectEqualStrings("order:4", parsed.req.foreign_key_ref_deletes[0].child_key);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_externalized_parent_checks.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_externalized_parent_checks[0].constraint_name);
    try std.testing.expectEqualStrings("orders", parsed.req.foreign_key_externalized_parent_checks[0].child_table);
    try std.testing.expectEqualStrings("order:9", parsed.req.foreign_key_externalized_parent_checks[0].child_key);
    try std.testing.expectEqualStrings("customers", parsed.req.foreign_key_externalized_parent_checks[0].parent_table);
    try std.testing.expectEqualStrings("customer:9", parsed.req.foreign_key_externalized_parent_checks[0].parent_key);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parsed.req.foreign_key_externalized_parent_checks[0].timing);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_constraint_timing_overrides.len);
    try std.testing.expectEqualStrings("orders_customer_id_fkey", parsed.req.foreign_key_constraint_timing_overrides[0].constraint_name);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.immediate, parsed.req.foreign_key_constraint_timing_overrides[0].timing);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.unique_constraint_writes.len);
    try std.testing.expectEqualStrings("users_email_key", parsed.req.unique_constraint_writes[0].constraint_name);
    try std.testing.expectEqualStrings("email:ada", parsed.req.unique_constraint_writes[0].encoded_value);
    try std.testing.expectEqualStrings("user:1", parsed.req.unique_constraint_writes[0].owner_key);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.unique_constraint_deletes.len);
    try std.testing.expectEqualStrings("users_email_key", parsed.req.unique_constraint_deletes[0].constraint_name);
    try std.testing.expectEqualStrings("email:grace", parsed.req.unique_constraint_deletes[0].encoded_value);
    try std.testing.expectEqualStrings("user:2", parsed.req.unique_constraint_deletes[0].owner_key);
    try expectTxnPrepareSqlStyleForeignKeyOperationSpellings(alloc);
}

fn expectTxnPrepareSqlStyleForeignKeyOperationSpellings(alloc: std.mem.Allocator) !void {
    var parsed = try parseTxnPrepareRequest(alloc,
        \\{
        \\  "txn_id":"00112233445566778899aabbccddeeff",
        \\  "topology_epoch":7,
        \\  "writes":[],
        \\  "deletes":[],
        \\  "transforms":[],
        \\  "predicates":[],
        \\  "foreign_key_parent_delete_checks":[{
        \\    "constraint_name":"orders_customer_id_fkey",
        \\    "parent_table":"customers",
        \\    "parent_key":"customer:1",
        \\    "operation":"ON UPDATE",
        \\    "timing":"SET CONSTRAINTS ALL DEFERRED"
        \\  }],
        \\  "foreign_key_set_null_children":[{
        \\    "constraint_name":"orders_customer_id_fkey",
        \\    "parent_table":"customers",
        \\    "parent_key":"customer:2",
        \\    "child_key":"order:2",
        \\    "operation":"parent-delete"
        \\  }],
        \\  "foreign_key_cascade_children":[{
        \\    "constraint_name":"orders_customer_id_fkey",
        \\    "parent_table":"customers",
        \\    "parent_key":"customer:3",
        \\    "child_key":"order:3",
        \\    "operation":"on update check",
        \\    "updated_parent_key":"customer:4"
        \\  }],
        \\  "foreign_key_constraint_timing_overrides":[{
        \\    "constraint_name":"orders_customer_id_fkey",
        \\    "timing":"SET CONSTRAINTS other_customer_id_fkey, \"orders_customer_id_fkey\" DEFERRED"
        \\  }]
        \\}
    );
    defer freeTxnPrepareRequest(alloc, &parsed);

    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_parent_delete_checks.len);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentDeleteCheck.Operation.update, parsed.req.foreign_key_parent_delete_checks[0].operation);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parsed.req.foreign_key_parent_delete_checks[0].timing);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_set_null_children.len);
    try std.testing.expectEqual(db_mod.types.ForeignKeySetNullChildAction.Operation.delete, parsed.req.foreign_key_set_null_children[0].operation);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_cascade_children.len);
    try std.testing.expectEqual(db_mod.types.ForeignKeyCascadeChildAction.Operation.update, parsed.req.foreign_key_cascade_children[0].operation);
    try std.testing.expectEqualStrings("customer:4", parsed.req.foreign_key_cascade_children[0].updated_parent_key.?);
    try std.testing.expectEqual(@as(usize, 1), parsed.req.foreign_key_constraint_timing_overrides.len);
    try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, parsed.req.foreign_key_constraint_timing_overrides[0].timing);

    try std.testing.expectError(error.InvalidTxnRequest, parseTxnPrepareRequest(alloc,
        \\{
        \\  "txn_id":"00112233445566778899aabbccddeeff",
        \\  "topology_epoch":7,
        \\  "writes":[],
        \\  "deletes":[],
        \\  "transforms":[],
        \\  "predicates":[],
        \\  "foreign_key_constraint_timing_overrides":[{
        \\    "constraint_name":"orders_customer_id_fkey",
        \\    "timing":"SET CONSTRAINTS other_customer_id_fkey DEFERRED"
        \\  }]
        \\}
    ));
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
            try std.testing.expectEqual(@as(usize, 3), req.participants.len);
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

test "distributed txn coordinator registers foreign key parent participants" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
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
        prepared_empty_parent: bool = false,

        fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            self.begins.deinit(alloc);
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

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
            } else if (group_id == 8002) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.deletes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.transforms.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.predicates.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_parent_checks[0].constraint_name);
                try std.testing.expectEqualStrings("docs", req.req.foreign_key_parent_checks[0].child_table);
                try std.testing.expectEqualStrings("doc:a-order", req.req.foreign_key_parent_checks[0].child_key);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_parent_checks[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_parent_checks[0].parent_key);
                self.prepared_empty_parent = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    defer recorder.deinit(std.testing.allocator);
    const txn_id = try parseTxnIdHex("11112222333344445555666677778888");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:z-customer\"}" }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expectEqual(@as(usize, 2), recorder.begins.items.len);
    try std.testing.expect(recorder.prepared_empty_parent);
}

test "distributed txn coordinator externalizes deferred foreign key parent checks exactly" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","timing":"deferred"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
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
        prepared_child: bool = false,
        prepared_parent: bool = false,
        expected_timing: db_mod.types.ForeignKeyParentCheck.Timing = .deferred,
        expect_child_override: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_externalized_parent_checks.len);
                const check = req.req.foreign_key_externalized_parent_checks[0];
                try std.testing.expectEqualStrings("orders_customer_id_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("row", check.child_table);
                try std.testing.expectEqualStrings("doc:a-order", check.child_key);
                try std.testing.expectEqualStrings("customers", check.parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", check.parent_key);
                try std.testing.expectEqual(self.expected_timing, check.timing);
                if (self.expect_child_override) {
                    try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_constraint_timing_overrides.len);
                    try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_constraint_timing_overrides[0].constraint_name);
                    try std.testing.expectEqual(self.expected_timing, req.req.foreign_key_constraint_timing_overrides[0].timing);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_constraint_timing_overrides.len);
                }
                self.prepared_child = true;
            } else if (group_id == 8002) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_constraint_timing_overrides.len);
                const check = req.req.foreign_key_parent_checks[0];
                try std.testing.expectEqualStrings("orders_customer_id_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("docs", check.child_table);
                try std.testing.expectEqualStrings("doc:a-order", check.child_key);
                try std.testing.expectEqualStrings("customers", check.parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", check.parent_key);
                try std.testing.expectEqual(self.expected_timing, check.timing);
                self.prepared_parent = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("11112222333344445555666677779999");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:z-customer\"}" }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.prepared_child);
    try std.testing.expect(recorder.prepared_parent);

    var immediate_recorder = Recorder{
        .expected_timing = .immediate,
        .expect_child_override = true,
    };
    const immediate_txn_id = try parseTxnIdHex("1111222233334444555566667777999a");
    const immediate_result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        immediate_recorder.worker(),
        immediate_txn_id,
        10_002,
        10_003,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:z-customer\"}" }},
            .foreign_key_constraint_timing_overrides = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .timing = .immediate,
            }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), immediate_result.committed.participant_count);
    try std.testing.expect(immediate_recorder.prepared_child);
    try std.testing.expect(immediate_recorder.prepared_parent);

    const deferrable_initially_immediate_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","timing":"immediate","deferrable":true}]}
    ;
    const DeferrableImmediateCatalog = struct {
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = deferrable_initially_immediate_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
                })[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    var deferred_override_recorder = Recorder{
        .expected_timing = .deferred,
        .expect_child_override = true,
    };
    const deferred_override_txn_id = try parseTxnIdHex("1111222233334444555566667777999b");
    const deferred_override_result = try executeMultiTableCommit(
        std.testing.allocator,
        DeferrableImmediateCatalog.iface(),
        deferred_override_recorder.worker(),
        deferred_override_txn_id,
        10_004,
        10_005,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:z-customer\"}" }},
            .foreign_key_constraint_timing_overrides = &.{.{
                .constraint_name = "orders_customer_id_fkey",
                .timing = .deferred,
            }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), deferred_override_result.committed.participant_count);
    try std.testing.expect(deferred_override_recorder.prepared_child);
    try std.testing.expect(deferred_override_recorder.prepared_parent);
}

test "single-table distributed txn coordinator registers foreign key parent groups" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
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
        prepared_empty_parent: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
            } else if (group_id == 8002) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.deletes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.transforms.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.predicates.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_parent_checks[0].constraint_name);
                try std.testing.expectEqualStrings("docs", req.req.foreign_key_parent_checks[0].child_table);
                try std.testing.expectEqualStrings("doc:a-order", req.req.foreign_key_parent_checks[0].child_key);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_parent_checks[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_parent_checks[0].parent_key);
                self.prepared_empty_parent = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("22223333444455556666777788889999");
    const result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "docs",
        txn_id,
        10_000,
        10_001,
        .{ .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:z-customer\"}" }} },
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.participant_count);
    try std.testing.expect(recorder.prepared_empty_parent);
}

test "distributed txn coordinator routes foreign key child writes through ref owners when configured" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        prepared_child: bool = false,
        prepared_parent: bool = false,
        prepared_owner: bool = false,
        lookup_calls: usize = 0,

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

        fn workerWithLookup(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookupMissing,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 3), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("doc:a-order", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 0), req.req.predicates[0].expected_version);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_externalized_parent_checks.len);
                self.prepared_child = true;
            } else if (group_id == 8001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_externalized_parent_checks.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_parent_checks[0].constraint_name);
                try std.testing.expectEqualStrings("docs", req.req.foreign_key_parent_checks[0].child_table);
                try std.testing.expectEqualStrings("doc:a-order", req.req.foreign_key_parent_checks[0].child_key);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_parent_checks[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_parent_checks[0].parent_key);
                self.prepared_parent = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_ref_writes.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_ref_writes[0].constraint_name);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_ref_writes[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_ref_writes[0].parent_key);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_ref_writes[0].child_table);
                try std.testing.expectEqualStrings("doc:a-order", req.req.foreign_key_ref_writes[0].child_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_externalized_parent_checks.len);
                self.prepared_owner = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookupMissing(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:a-order", key);
            return null;
        }
    };

    var unversioned_recorder = Recorder{};
    const unversioned_result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        unversioned_recorder.workerWithLookup(),
        try parseTxnIdHex("22223333444455556666777788889998"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:z-customer\"}" }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), unversioned_result.committed.participant_count);
    try std.testing.expectEqual(@as(usize, 1), unversioned_recorder.lookup_calls);
    try std.testing.expect(unversioned_recorder.prepared_child);
    try std.testing.expect(unversioned_recorder.prepared_parent);
    try std.testing.expect(unversioned_recorder.prepared_owner);

    var delete_recorder = Recorder{};
    try std.testing.expectError(error.UnsupportedOperation, executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        delete_recorder.worker(),
        try parseTxnIdHex("22223333444455556666777788889997"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .deletes = &.{"doc:a-order"},
        }},
        null,
    ));
    try std.testing.expect(!delete_recorder.prepared_child);
    try std.testing.expect(!delete_recorder.prepared_parent);
    try std.testing.expect(!delete_recorder.prepared_owner);

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("22223333444455556666777788889999");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:z-customer\"}" }},
            .predicates = &.{.{ .key = "doc:a-order", .expected_version = 0 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), result.committed.participant_count);
    try std.testing.expect(recorder.prepared_child);
    try std.testing.expect(recorder.prepared_parent);
    try std.testing.expect(recorder.prepared_owner);
}

test "distributed txn coordinator fails closed for transitional foreign key ref owner ranges" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                    .state = metadata_table_manager.foreign_key_ref_range_rebuilding,
                }})[0..]),
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
            return error.UnexpectedWorkerCall;
        }

        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }
    };

    var recorder = Recorder{};
    try std.testing.expectError(error.UnknownGroup, executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("22223333444455556666777788889996"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:z-customer\"}" }},
            .predicates = &.{.{ .key = "doc:a-order", .expected_version = 0 }},
        }},
        null,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);
}

test "distributed txn coordinator routes old and new foreign key refs with versioned child rows" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Mode = enum { update, delete };
    const Recorder = struct {
        mode: Mode,
        lookup_calls: usize = 0,
        prepared_child: bool = false,
        prepared_parent: bool = false,
        prepared_owner: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(if (self.mode == .update) @as(usize, 3) else @as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("doc:a-order", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 7), req.req.predicates[0].expected_version);
                if (self.mode == .update) {
                    try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                    try std.testing.expectEqual(@as(usize, 0), req.req.deletes.len);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                    try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                }
                self.prepared_child = true;
            } else if (group_id == 8001) {
                if (self.mode != .update) return error.UnexpectedGroup;
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqualStrings("cust:new", req.req.foreign_key_parent_checks[0].parent_key);
                self.prepared_parent = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqualStrings("cust:old", req.req.foreign_key_ref_deletes[0].parent_key);
                try std.testing.expectEqualStrings("doc:a-order", req.req.foreign_key_ref_deletes[0].child_key);
                if (self.mode == .update) {
                    try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_ref_writes.len);
                    try std.testing.expectEqualStrings("cust:new", req.req.foreign_key_ref_writes[0].parent_key);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_writes.len);
                }
                self.prepared_owner = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:a-order", key);
            return .{
                .json = try alloc.dupe(u8, "{\"customer_id\":\"cust:old\"}"),
                .version = 7,
            };
        }
    };

    var update_recorder = Recorder{ .mode = .update };
    const update_result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        update_recorder.worker(),
        try parseTxnIdHex("22223333444455556666777788889996"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"customer_id\":\"cust:new\"}" }},
            .predicates = &.{.{ .key = "doc:a-order", .expected_version = 7 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), update_result.committed.participant_count);
    try std.testing.expectEqual(@as(usize, 1), update_recorder.lookup_calls);
    try std.testing.expect(update_recorder.prepared_child);
    try std.testing.expect(update_recorder.prepared_parent);
    try std.testing.expect(update_recorder.prepared_owner);

    var delete_recorder = Recorder{ .mode = .delete };
    const delete_result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        delete_recorder.worker(),
        try parseTxnIdHex("22223333444455556666777788889995"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .deletes = &.{"doc:a-order"},
            .predicates = &.{.{ .key = "doc:a-order", .expected_version = 7 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), delete_result.committed.participant_count);
    try std.testing.expectEqual(@as(usize, 1), delete_recorder.lookup_calls);
    try std.testing.expect(delete_recorder.prepared_child);
    try std.testing.expect(!delete_recorder.prepared_parent);
    try std.testing.expect(delete_recorder.prepared_owner);
}

test "distributed txn coordinator routes unique-touching transforms with row proofs" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed_schema.deinit(std.testing.allocator);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parsed_schema);
    defer storage_schema.freeSchema(std.testing.allocator, runtime_schema);
    const old_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(old_row);
    const old_value = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, old_row, runtime_schema.unique_constraints[0]);
    defer if (old_value) |value| std.testing.allocator.free(value);
    const old_encoded_value = old_value orelse unreachable;
    const grace_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"grace@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(grace_row);
    const grace_value = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, grace_row, runtime_schema.unique_constraints[0]);
    defer if (grace_value) |value| std.testing.allocator.free(value);
    const grace_encoded_value = grace_value orelse unreachable;
    const katherine_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"katherine@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(katherine_row);
    const katherine_value = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, katherine_row, runtime_schema.unique_constraints[0]);
    defer if (katherine_value) |value| std.testing.allocator.free(value);
    const katherine_encoded_value = katherine_value orelse unreachable;
    const final_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"final@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(final_row);
    const final_value = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, final_row, runtime_schema.unique_constraints[0]);
    defer if (final_value) |value| std.testing.allocator.free(value);
    const final_encoded_value = final_value orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "users", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "user:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "user:m", .end_key = null },
                })[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 7,
                    .constraint_name = "users_email_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9001,
                    .topology_epoch = 1,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Mode = enum { existing, upsert, write_transform };
    const Recorder = struct {
        mode: Mode,
        expected_old_encoded_value: ?[]const u8 = null,
        expected_new_encoded_value: []const u8,
        expected_version: u64,
        lookup_calls: usize = 0,
        prepared_row: bool = false,
        prepared_owner: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            if (group_id == 7002) {
                try std.testing.expectEqual(@as(usize, 1), req.req.transforms.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqual(self.expected_version, req.req.predicates[0].expected_version);
                if (self.mode == .write_transform) {
                    try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                    try std.testing.expectEqualStrings("user:x", req.req.writes[0].key);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                }
                self.prepared_row = true;
            } else if (group_id == 9001) {
                if (self.expected_old_encoded_value) |old_encoded| {
                    try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                    try std.testing.expectEqualStrings(old_encoded, req.req.unique_constraint_deletes[0].encoded_value);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.unique_constraint_deletes.len);
                }
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_writes.len);
                try std.testing.expectEqualStrings(self.expected_new_encoded_value, req.req.unique_constraint_writes[0].encoded_value);
                self.prepared_owner = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            try std.testing.expectEqual(@as(u64, 7002), group_id);
            try std.testing.expectEqualStrings("users", table_name);
            switch (self.mode) {
                .existing => {
                    try std.testing.expectEqualStrings("user:z", key);
                    return .{
                        .json = try alloc.dupe(u8, "{\"email\":\"ada@example.test\"}"),
                        .version = 7,
                    };
                },
                .upsert => {
                    try std.testing.expectEqualStrings("user:y", key);
                    return null;
                },
                .write_transform => {
                    try std.testing.expectEqualStrings("user:x", key);
                    return null;
                },
            }
        }
    };

    const txn_id = try parseTxnIdHex("1234567890abcdef1234567890abcdef");
    const set_grace = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "email",
        .value_json = "\"grace@example.test\"",
    }};
    var existing_recorder = Recorder{
        .mode = .existing,
        .expected_old_encoded_value = old_encoded_value,
        .expected_new_encoded_value = grace_encoded_value,
        .expected_version = 7,
    };
    const existing_result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        existing_recorder.worker(),
        "users",
        txn_id,
        10_200,
        10_201,
        .{ .transforms = &.{.{
            .key = "user:z",
            .operations = set_grace[0..],
        }} },
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), existing_result.participant_count);
    try std.testing.expectEqual(@as(usize, 1), existing_recorder.lookup_calls);
    try std.testing.expect(existing_recorder.prepared_row);
    try std.testing.expect(existing_recorder.prepared_owner);

    const set_katherine = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "email",
        .value_json = "\"katherine@example.test\"",
    }};
    var upsert_recorder = Recorder{
        .mode = .upsert,
        .expected_new_encoded_value = katherine_encoded_value,
        .expected_version = 0,
    };
    const upsert_result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        upsert_recorder.worker(),
        try parseTxnIdHex("1234567890abcdef1234567890abcdee"),
        10_300,
        10_301,
        &.{.{
            .table_name = "users",
            .transforms = &.{.{
                .key = "user:y",
                .operations = set_katherine[0..],
                .upsert = true,
            }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), upsert_result.committed.participant_count);
    try std.testing.expectEqual(@as(usize, 1), upsert_recorder.lookup_calls);
    try std.testing.expect(upsert_recorder.prepared_row);
    try std.testing.expect(upsert_recorder.prepared_owner);

    const set_final = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "email",
        .value_json = "\"final@example.test\"",
    }};
    var write_transform_recorder = Recorder{
        .mode = .write_transform,
        .expected_new_encoded_value = final_encoded_value,
        .expected_version = 0,
    };
    const write_transform_result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        write_transform_recorder.worker(),
        "users",
        try parseTxnIdHex("1234567890abcdef1234567890abcded"),
        10_400,
        10_401,
        .{
            .writes = &.{.{ .key = "user:x", .value = "{\"email\":\"initial@example.test\"}" }},
            .transforms = &.{.{
                .key = "user:x",
                .operations = set_final[0..],
            }},
        },
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), write_transform_result.participant_count);
    try std.testing.expectEqual(@as(usize, 1), write_transform_recorder.lookup_calls);
    try std.testing.expect(write_transform_recorder.prepared_row);
    try std.testing.expect(write_transform_recorder.prepared_owner);
}

test "distributed txn coordinator routes unique constraint writes through owner ranges" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword","collation":"antfly.case_insensitive"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed_schema.deinit(std.testing.allocator);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parsed_schema);
    defer storage_schema.freeSchema(std.testing.allocator, runtime_schema);
    const expected_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(expected_row);
    const expected_value = try relational_store.uniqueConstraintTupleValueWithColumnsAlloc(std.testing.allocator, expected_row, runtime_schema.unique_constraints[0], runtime_schema.relational_columns);
    defer if (expected_value) |value| std.testing.allocator.free(value);
    const expected_encoded_value = expected_value orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "users", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "user:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "user:m", .end_key = null },
                })[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 7,
                    .constraint_name = "users_email_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9001,
                    .topology_epoch = 1,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        const Mode = enum { write, delete };

        mode: Mode = .write,
        expected_encoded_value: []const u8,
        prepared_row: bool = false,
        prepared_owner: bool = false,
        lookup_calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            if (group_id == 7002) {
                if (self.mode == .write) {
                    try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                    try std.testing.expectEqual(@as(usize, 0), req.req.deletes.len);
                    try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                    try std.testing.expectEqual(@as(u64, 0), req.req.predicates[0].expected_version);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                    try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                    try std.testing.expectEqualStrings("user:z", req.req.deletes[0]);
                    try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                    try std.testing.expectEqualStrings("user:z", req.req.predicates[0].key);
                    try std.testing.expectEqual(@as(u64, 5), req.req.predicates[0].expected_version);
                }
                self.prepared_row = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                if (self.mode == .write) {
                    try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_writes.len);
                    try std.testing.expectEqual(@as(usize, 0), req.req.unique_constraint_deletes.len);
                    try std.testing.expectEqualStrings("users_email_key", req.req.unique_constraint_writes[0].constraint_name);
                    try std.testing.expectEqualStrings(self.expected_encoded_value, req.req.unique_constraint_writes[0].encoded_value);
                    try std.testing.expectEqualStrings("user:z", req.req.unique_constraint_writes[0].owner_key);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.unique_constraint_writes.len);
                    try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                    try std.testing.expectEqualStrings("users_email_key", req.req.unique_constraint_deletes[0].constraint_name);
                    try std.testing.expectEqualStrings(self.expected_encoded_value, req.req.unique_constraint_deletes[0].encoded_value);
                    try std.testing.expectEqualStrings("user:z", req.req.unique_constraint_deletes[0].owner_key);
                }
                self.prepared_owner = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            try std.testing.expectEqual(@as(u64, 7002), group_id);
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqualStrings("user:z", key);
            if (self.mode == .write) return null;
            return .{
                .json = try alloc.dupe(u8, "{\"email\":\"ada@example.test\"}"),
                .version = 5,
            };
        }
    };

    var recorder = Recorder{ .mode = .write, .expected_encoded_value = expected_encoded_value };
    const txn_id = try parseTxnIdHex("41414141414141414141414141414141");
    const result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "users",
        txn_id,
        30_000,
        30_001,
        .{
            .writes = &.{.{ .key = "user:z", .value = "{\"email\":\"Ada@Example.Test\"}" }},
        },
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.participant_count);
    try std.testing.expect(recorder.prepared_row);
    try std.testing.expect(recorder.prepared_owner);
    try std.testing.expectEqual(@as(usize, 1), recorder.lookup_calls);

    var delete_recorder = Recorder{ .mode = .delete, .expected_encoded_value = expected_encoded_value };
    const delete_result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        delete_recorder.worker(),
        "users",
        try parseTxnIdHex("41414141414141414141414141414142"),
        30_100,
        30_101,
        .{ .deletes = &.{"user:z"} },
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), delete_result.participant_count);
    try std.testing.expect(delete_recorder.prepared_row);
    try std.testing.expect(delete_recorder.prepared_owner);
    try std.testing.expectEqual(@as(usize, 1), delete_recorder.lookup_calls);
}

test "distributed txn coordinator routes unique owner handoff with row version proofs" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed_schema.deinit(std.testing.allocator);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parsed_schema);
    defer storage_schema.freeSchema(std.testing.allocator, runtime_schema);
    const old_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(old_row);
    const old_value = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, old_row, runtime_schema.unique_constraints[0]);
    defer if (old_value) |value| std.testing.allocator.free(value);
    const old_encoded_value = old_value orelse unreachable;
    const new_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"grace@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(new_row);
    const new_value = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, new_row, runtime_schema.unique_constraints[0]);
    defer if (new_value) |value| std.testing.allocator.free(value);
    const new_encoded_value = new_value orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "users", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "user:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "user:m", .end_key = null },
                })[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 7,
                    .constraint_name = "users_email_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9001,
                    .topology_epoch = 1,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        old_encoded_value: []const u8,
        new_encoded_value: []const u8,
        prepared_owner: bool = false,
        lookup_calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("users", table_name);
            if (group_id == 7002) {
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("user:z", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 7), req.req.predicates[0].expected_version);
            } else if (group_id == 9001) {
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                try std.testing.expectEqualStrings(self.old_encoded_value, req.req.unique_constraint_deletes[0].encoded_value);
                try std.testing.expectEqualStrings("user:z", req.req.unique_constraint_deletes[0].owner_key);
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_writes.len);
                try std.testing.expectEqualStrings(self.new_encoded_value, req.req.unique_constraint_writes[0].encoded_value);
                try std.testing.expectEqualStrings("user:z", req.req.unique_constraint_writes[0].owner_key);
                self.prepared_owner = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            try std.testing.expectEqual(@as(u64, 7002), group_id);
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqualStrings("user:z", key);
            return .{
                .json = try alloc.dupe(u8, "{\"email\":\"ada@example.test\"}"),
                .version = 7,
            };
        }
    };

    var recorder = Recorder{ .old_encoded_value = old_encoded_value, .new_encoded_value = new_encoded_value };
    const txn_id = try parseTxnIdHex("42424242424242424242424242424242");
    const result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "users",
        txn_id,
        31_000,
        31_001,
        .{
            .writes = &.{.{ .key = "user:z", .value = "{\"email\":\"grace@example.test\"}" }},
            .predicates = &.{.{ .key = "user:z", .expected_version = 7 }},
        },
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.participant_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.lookup_calls);
    try std.testing.expect(recorder.prepared_owner);

    var unversioned_recorder = Recorder{ .old_encoded_value = old_encoded_value, .new_encoded_value = new_encoded_value };
    const unversioned_result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        unversioned_recorder.worker(),
        "users",
        try parseTxnIdHex("42424242424242424242424242424243"),
        31_100,
        31_101,
        .{ .writes = &.{.{ .key = "user:z", .value = "{\"email\":\"grace@example.test\"}" }} },
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), unversioned_result.participant_count);
    try std.testing.expectEqual(@as(usize, 1), unversioned_recorder.lookup_calls);
    try std.testing.expect(unversioned_recorder.prepared_owner);
}

test "distributed txn coordinator allows non-unique transforms on multi-range unique tables" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"status":{"type":"keyword"},"count":{"type":"numeric"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "users", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "user:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "user:m", .end_key = null },
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

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqual(@as(usize, 1), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
            try std.testing.expectEqual(@as(usize, 0), req.req.deletes.len);
            try std.testing.expectEqual(@as(usize, 1), req.req.transforms.len);
            try std.testing.expectEqualStrings("user:a", req.req.transforms[0].key);
            try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_checks.len);
            try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_delete_checks.len);
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.resolve_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }
    };

    const update_status = [_]db_mod.types.TransformOp{
        .{ .op = .set, .path = "status", .value_json = "\"active\"" },
        .{ .op = .inc, .path = "count", .value_json = "1" },
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("fedcba9876543210fedcba9876543210");
    const result = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "users",
        txn_id,
        20_000,
        20_001,
        .{ .transforms = &.{.{
            .key = "user:a",
            .operations = update_status[0..],
        }} },
        null,
    );
    try std.testing.expectEqual(@as(usize, 1), result.participant_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.resolve_calls);
}

test "distributed txn coordinator allows single-range unique writes to use local enforcement" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "users", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
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
        prepared: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqual(@as(usize, 1), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("users", table_name);
            try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
            try std.testing.expectEqualStrings("user:a", req.req.writes[0].key);
            self.prepared = true;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("234567890abcdef1234567890abcdef1");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "users",
            .writes = &.{.{ .key = "user:a", .value = "{\"email\":\"ada@example.test\"}" }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 1), result.committed.participant_count);
    try std.testing.expect(recorder.prepared);
}

test "distributed txn coordinator rejects non-primary foreign key parent writes without unique owner topology" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"restrict"}],"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
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
            return error.UnexpectedWorkerCall;
        }

        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("aaaabbbbccccddddeeeeffff00001111");
    try std.testing.expectError(error.UnsupportedOperation, executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"ref_email\":\"ada@example.test\"}" }},
        }},
        null,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);
}

test "distributed txn coordinator rejects partial match full composite foreign key writes before prepare" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"email":{"type":"keyword"},"customer_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"users_customer_tenant_email_fkey","columns":["tenant_id","customer_email"],"references":{"table":"row","columns":["tenant_id","email"]},"match":"full"}],"unique_constraints":[{"name":"users_tenant_email_key","columns":["tenant_id","email"]}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
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
            return error.UnexpectedWorkerCall;
        }

        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("aaaabbbbccccddddeeeeffff00001112");
    try std.testing.expectError(error.ForeignKeyViolation, executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:partial", .value = "{\"tenant_id\":\"tenant:1\"}" }},
        }},
        null,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);
}

test "distributed txn coordinator routes foreign key checks through unique owner ranges" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"restrict"}],"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed_schema.deinit(std.testing.allocator);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parsed_schema);
    defer storage_schema.freeSchema(std.testing.allocator, runtime_schema);
    const row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"ref_email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(row);
    const expected_parent = try relational_store.foreignKeyReferenceValueAlloc(std.testing.allocator, row, runtime_schema.foreign_keys[0]);
    defer if (expected_parent) |value| std.testing.allocator.free(value);
    const expected_parent_key = expected_parent orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{
                    .{ .table_id = 7, .constraint_name = "users_email_key", .start_encoded_value = "", .end_encoded_value = null, .group_id = 9001 },
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
        expected_parent_key: []const u8,
        row_prepared: bool = false,
        unique_parent_prepared: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            if (group_id == 7001) {
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_externalized_parent_checks.len);
                self.row_prepared = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_externalized_parent_checks.len);
                const check = req.req.foreign_key_parent_checks[0];
                try std.testing.expectEqualStrings("users_ref_email_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("docs", check.child_table);
                try std.testing.expectEqualStrings("doc:a-order", check.child_key);
                try std.testing.expectEqualStrings("row", check.parent_table);
                try std.testing.expectEqualStrings(self.expected_parent_key, check.parent_key);
                try std.testing.expect(check.parent_constraint_name != null);
                try std.testing.expectEqualStrings("users_email_key", check.parent_constraint_name.?);
                self.unique_parent_prepared = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const txn_id = try parseTxnIdHex("bbbbccccddddeeeeffff000011112222");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "doc:a-order", .value = "{\"ref_email\":\"ada@example.test\"}" }},
            .predicates = &.{.{ .key = "doc:a-order", .expected_version = 0 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.row_prepared);
    try std.testing.expect(recorder.unique_parent_prepared);
}

test "distributed txn coordinator routes cross-table foreign key checks through parent unique owner ranges" {
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_email_fkey","columns":["customer_email"],"references":{"table":"customers","columns":["email"]},"on_delete":"restrict"}]}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"customers","enforce_types":true,"document_schemas":{"customers":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"customers_email_key","columns":["email"]}]}
    ;
    var parent_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, customers_schema_json);
    defer parent_schema.deinit(std.testing.allocator);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parent_schema);
    defer storage_schema.freeSchema(std.testing.allocator, parent_runtime_schema);
    const parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\"}", parent_runtime_schema.relational_columns);
    defer std.testing.allocator.free(parent_row);
    const expected_parent = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, parent_row, parent_runtime_schema.unique_constraints[0]);
    defer if (expected_parent) |value| std.testing.allocator.free(value);
    const expected_parent_key = expected_parent orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = orders_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{
                    .{ .table_id = 8, .constraint_name = "customers_email_key", .start_encoded_value = "", .end_encoded_value = null, .group_id = 9001 },
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
        expected_parent_key: []const u8,
        row_prepared: bool = false,
        unique_parent_prepared: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_externalized_parent_checks.len);
                self.row_prepared = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_externalized_parent_checks.len);
                const check = req.req.foreign_key_parent_checks[0];
                try std.testing.expectEqualStrings("orders_customer_email_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("docs", check.child_table);
                try std.testing.expectEqualStrings("order:a", check.child_key);
                try std.testing.expectEqualStrings("customers", check.parent_table);
                try std.testing.expectEqualStrings(self.expected_parent_key, check.parent_key);
                try std.testing.expect(check.parent_constraint_name != null);
                try std.testing.expectEqualStrings("customers_email_key", check.parent_constraint_name.?);
                self.unique_parent_prepared = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("aaaabbbbccccddddeeeeffff00001111"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "order:a", .value = "{\"customer_email\":\"ada@example.test\"}" }},
            .predicates = &.{.{ .key = "order:a", .expected_version = 0 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.row_prepared);
    try std.testing.expect(recorder.unique_parent_prepared);
}

test "distributed txn coordinator routes unique foreign key parent updates through ref owners" {
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_email_fkey","columns":["customer_email"],"references":{"table":"customers","columns":["email"]},"on_delete":"set_null","on_update":"restrict"}]}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"customers","enforce_types":true,"document_schemas":{"customers":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"customers_email_key","columns":["email"]}]}
    ;
    var parent_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, customers_schema_json);
    defer parent_schema.deinit(std.testing.allocator);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parent_schema);
    defer storage_schema.freeSchema(std.testing.allocator, parent_runtime_schema);
    const old_parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\"}", parent_runtime_schema.relational_columns);
    defer std.testing.allocator.free(old_parent_row);
    const expected_old_parent = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, old_parent_row, parent_runtime_schema.unique_constraints[0]);
    defer if (expected_old_parent) |value| std.testing.allocator.free(value);
    const expected_old_parent_key = expected_old_parent orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_email_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 44,
                }})[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 8,
                    .constraint_name = "customers_email_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9002,
                    .topology_epoch = 45,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        expected_old_parent_key: []const u8,
        parent_prepared: bool = false,
        ref_owner_prepared: bool = false,
        lookup_calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 3), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 8001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                try std.testing.expectEqualStrings("customer:ada", req.req.writes[0].key);
                self.parent_prepared = true;
            } else if (group_id == 9002) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_writes.len);
                try std.testing.expectEqualStrings("customers_email_key", req.req.unique_constraint_deletes[0].constraint_name);
                try std.testing.expectEqualStrings("customers_email_key", req.req.unique_constraint_writes[0].constraint_name);
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("orders", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_action_schedules.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_delete_checks.len);
                const check = req.req.foreign_key_parent_delete_checks[0];
                try std.testing.expectEqual(db_mod.types.ForeignKeyParentDeleteCheck.Operation.update, check.operation);
                try std.testing.expectEqualStrings("orders_customer_email_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("customers", check.parent_table);
                try std.testing.expectEqualStrings(self.expected_old_parent_key, check.parent_key);
                self.ref_owner_prepared = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            try std.testing.expectEqual(@as(u64, 8001), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            try std.testing.expectEqualStrings("customer:ada", key);
            return .{
                .json = try alloc.dupe(u8, "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}"),
                .version = 5,
            };
        }
    };

    var recorder = Recorder{ .expected_old_parent_key = expected_old_parent_key };
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("aaaabbbbccccddddeeeeffff00004444"),
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .writes = &.{.{ .key = "customer:ada", .value = "{\"email\":\"grace@example.test\",\"name\":\"Ada\"}" }},
            .predicates = &.{.{ .key = "customer:ada", .expected_version = 5 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), result.committed.participant_count);
    try std.testing.expectEqual(@as(usize, 2), recorder.lookup_calls);
    try std.testing.expect(recorder.parent_prepared);
    try std.testing.expect(recorder.ref_owner_prepared);
}

test "distributed txn coordinator schedules mutating unique foreign key parent updates through ref owners" {
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_email_fkey","columns":["customer_email"],"references":{"table":"customers","columns":["email"]},"on_update":"set_null"}]}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"customers","enforce_types":true,"document_schemas":{"customers":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"customers_email_key","columns":["email"]}]}
    ;
    var parent_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, customers_schema_json);
    defer parent_schema.deinit(std.testing.allocator);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parent_schema);
    defer storage_schema.freeSchema(std.testing.allocator, parent_runtime_schema);
    const old_parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}", parent_runtime_schema.relational_columns);
    defer std.testing.allocator.free(old_parent_row);
    const expected_old_parent = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, old_parent_row, parent_runtime_schema.unique_constraints[0]);
    defer if (expected_old_parent) |value| std.testing.allocator.free(value);
    const expected_old_parent_key = expected_old_parent orelse unreachable;
    const new_parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"grace@example.test\",\"name\":\"Ada\"}", parent_runtime_schema.relational_columns);
    defer std.testing.allocator.free(new_parent_row);
    const expected_new_parent = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, new_parent_row, parent_runtime_schema.unique_constraints[0]);
    defer if (expected_new_parent) |value| std.testing.allocator.free(value);
    const expected_new_parent_key = expected_new_parent orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_email_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                }})[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 8,
                    .constraint_name = "customers_email_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9002,
                    .topology_epoch = 45,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        expected_old_parent_key: []const u8,
        expected_new_parent_key: []const u8,
        parent_prepared: bool = false,
        ref_owner_prepared: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 8001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                self.parent_prepared = true;
            } else if (group_id == 9002) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_writes.len);
                try std.testing.expectEqualStrings("customers_email_key", req.req.unique_constraint_deletes[0].constraint_name);
                try std.testing.expectEqualStrings("customers_email_key", req.req.unique_constraint_writes[0].constraint_name);
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("orders", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_delete_checks.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_conflict_checks.len);
                try std.testing.expectEqualStrings("orders_customer_email_fkey", req.req.foreign_key_conflict_checks[0].constraint_name);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_conflict_checks[0].parent_table);
                try std.testing.expectEqualStrings(self.expected_old_parent_key, req.req.foreign_key_conflict_checks[0].parent_key);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_action_schedules.len);
                const schedule = req.req.foreign_key_action_schedules[0];
                try std.testing.expectEqualStrings("update_set_null", schedule.action);
                try std.testing.expectEqualStrings("txn-coordinator", schedule.worker_id);
                try std.testing.expectEqualStrings("orders_customer_email_fkey", schedule.constraint_name);
                try std.testing.expectEqualStrings("customers", schedule.parent_table);
                try std.testing.expectEqualStrings(self.expected_old_parent_key, schedule.parent_key);
                try std.testing.expect(schedule.updated_parent_key != null);
                try std.testing.expectEqualStrings(self.expected_new_parent_key, schedule.updated_parent_key.?);
                try std.testing.expectEqual(@as(usize, 1024), schedule.page_limit);
                self.ref_owner_prepared = true;
            } else return error.UnexpectedGroup;
        }
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}
        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 8001), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            try std.testing.expectEqualStrings("customer:ada", key);
            return .{
                .json = try alloc.dupe(u8, "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}"),
                .version = 5,
            };
        }
    };

    var recorder = Recorder{
        .expected_old_parent_key = expected_old_parent_key,
        .expected_new_parent_key = expected_new_parent_key,
    };
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("aaaabbbbccccddddeeeeffff00005555"),
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .writes = &.{.{ .key = "customer:ada", .value = "{\"email\":\"grace@example.test\",\"name\":\"Ada\"}" }},
            .predicates = &.{.{ .key = "customer:ada", .expected_version = 5 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), result.committed.participant_count);
    try std.testing.expect(recorder.parent_prepared);
    try std.testing.expect(recorder.ref_owner_prepared);
}

test "distributed txn coordinator routes cross-table composite foreign key checks through parent unique owner ranges" {
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_region":{"type":"keyword"},"customer_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_identity_fkey","columns":["customer_region","customer_email"],"references":{"table":"customers","columns":["region","email"]},"on_delete":"restrict"}]}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"customers","enforce_types":true,"document_schemas":{"customers":{"schema":{"type":"object","properties":{"region":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"customers_region_email_key","columns":["region","email"]}]}
    ;
    var parent_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, customers_schema_json);
    defer parent_schema.deinit(std.testing.allocator);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parent_schema);
    defer storage_schema.freeSchema(std.testing.allocator, parent_runtime_schema);
    const parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"region\":\"us-east\",\"email\":\"ada@example.test\"}", parent_runtime_schema.relational_columns);
    defer std.testing.allocator.free(parent_row);
    const expected_parent = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, parent_row, parent_runtime_schema.unique_constraints[0]);
    defer if (expected_parent) |value| std.testing.allocator.free(value);
    const expected_parent_key = expected_parent orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = orders_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{
                    .{ .table_id = 8, .constraint_name = "customers_region_email_key", .start_encoded_value = "", .end_encoded_value = null, .group_id = 9001 },
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
        expected_parent_key: []const u8,
        row_prepared: bool = false,
        unique_parent_prepared: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_externalized_parent_checks.len);
                self.row_prepared = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_externalized_parent_checks.len);
                const check = req.req.foreign_key_parent_checks[0];
                try std.testing.expectEqualStrings("orders_customer_identity_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("docs", check.child_table);
                try std.testing.expectEqualStrings("order:a", check.child_key);
                try std.testing.expectEqualStrings("customers", check.parent_table);
                try std.testing.expectEqualStrings(self.expected_parent_key, check.parent_key);
                try std.testing.expect(check.parent_constraint_name != null);
                try std.testing.expectEqualStrings("customers_region_email_key", check.parent_constraint_name.?);
                self.unique_parent_prepared = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("aaaabbbbccccddddeeeeffff00002222"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "order:a", .value = "{\"customer_region\":\"us-east\",\"customer_email\":\"ada@example.test\"}" }},
            .predicates = &.{.{ .key = "order:a", .expected_version = 0 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.row_prepared);
    try std.testing.expect(recorder.unique_parent_prepared);
}

test "distributed txn coordinator routes cross-table composite foreign key checks through parent primary key owner ranges" {
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_region":{"type":"keyword"},"customer_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_identity_fkey","columns":["customer_region","customer_email"],"references":{"table":"customers","columns":["region","email"]},"on_delete":"restrict"}]}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"customers","enforce_types":true,"document_schemas":{"customers":{"schema":{"type":"object","properties":{"region":{"type":"keyword"},"email":{"type":"keyword"},"name":{"type":"keyword"}},"required":["region","email"],"additionalProperties":false}}},"primary_key":{"columns":["region","email"]}}
    ;
    var parent_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, customers_schema_json);
    defer parent_schema.deinit(std.testing.allocator);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parent_schema);
    defer storage_schema.freeSchema(std.testing.allocator, parent_runtime_schema);
    const parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"region\":\"us-east\",\"email\":\"ada@example.test\"}", parent_runtime_schema.relational_columns);
    defer std.testing.allocator.free(parent_row);
    const expected_parent_key = try relational_store.primaryKeyTupleValueAlloc(std.testing.allocator, parent_row, parent_runtime_schema.primary_key.?);
    defer std.testing.allocator.free(expected_parent_key);

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = orders_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{
                    .{ .table_id = 8, .constraint_name = relational_store.primary_key_constraint_name, .start_encoded_value = "", .end_encoded_value = null, .group_id = 9001 },
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
        expected_parent_key: []const u8,
        row_prepared: bool = false,
        primary_parent_prepared: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_externalized_parent_checks.len);
                self.row_prepared = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_externalized_parent_checks.len);
                const check = req.req.foreign_key_parent_checks[0];
                try std.testing.expectEqualStrings("orders_customer_identity_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("docs", check.child_table);
                try std.testing.expectEqualStrings("order:a", check.child_key);
                try std.testing.expectEqualStrings("customers", check.parent_table);
                try std.testing.expectEqualStrings(self.expected_parent_key, check.parent_key);
                try std.testing.expect(check.parent_constraint_name != null);
                try std.testing.expectEqualStrings(relational_store.primary_key_constraint_name, check.parent_constraint_name.?);
                self.primary_parent_prepared = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("aaaabbbbccccddddeeeeffff00002223"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .writes = &.{.{ .key = "order:a", .value = "{\"customer_region\":\"us-east\",\"customer_email\":\"ada@example.test\"}" }},
            .predicates = &.{.{ .key = "order:a", .expected_version = 0 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.row_prepared);
    try std.testing.expect(recorder.primary_parent_prepared);
}

test "distributed txn coordinator routes unique foreign key parent deletes through ref owners" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"restrict"}],"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
    ;
    var parsed_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed_schema.deinit(std.testing.allocator);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parsed_schema);
    defer storage_schema.freeSchema(std.testing.allocator, runtime_schema);
    const parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(parent_row);
    const expected_parent = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, parent_row, runtime_schema.unique_constraints[0]);
    defer if (expected_parent) |value| std.testing.allocator.free(value);
    const expected_parent_key = expected_parent orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "users_ref_email_fkey",
                    .parent_table_id = 7,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 7,
                    .constraint_name = "users_email_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9002,
                    .topology_epoch = 43,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        expected_parent_key: []const u8,
        row_prepared: bool = false,
        owner_prepared: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 3), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            if (group_id == 7001) {
                try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                try std.testing.expectEqualStrings("user:parent", req.req.deletes[0]);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("user:parent", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 5), req.req.predicates[0].expected_version);
                self.row_prepared = true;
            } else if (group_id == 9002) {
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.unique_constraint_writes.len);
                try std.testing.expectEqualStrings("users_email_key", req.req.unique_constraint_deletes[0].constraint_name);
            } else if (group_id == 9001) {
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_delete_checks.len);
                const check = req.req.foreign_key_parent_delete_checks[0];
                try std.testing.expectEqualStrings("users_ref_email_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("row", check.parent_table);
                try std.testing.expectEqualStrings(self.expected_parent_key, check.parent_key);
                self.owner_prepared = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("user:parent", key);
            return .{
                .json = try alloc.dupe(u8, "{\"email\":\"ada@example.test\"}"),
                .version = 5,
            };
        }
    };

    var unversioned_recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const unversioned_result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        unversioned_recorder.worker(),
        try parseTxnIdHex("ccccddddeeeeffff0000111122223333"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .deletes = &.{"user:parent"},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), unversioned_result.committed.participant_count);
    try std.testing.expect(unversioned_recorder.row_prepared);
    try std.testing.expect(unversioned_recorder.owner_prepared);

    var recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("ccccddddeeeeffff0000111122224444"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .deletes = &.{"user:parent"},
            .predicates = &.{.{ .key = "user:parent", .expected_version = 5 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), result.committed.participant_count);
    try std.testing.expect(recorder.row_prepared);
    try std.testing.expect(recorder.owner_prepared);
}

test "distributed txn coordinator routes cross-table unique foreign key parent deletes through ref owners" {
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_email_fkey","columns":["customer_email"],"references":{"table":"customers","columns":["email"]},"on_delete":"restrict"}]}
    ;
    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"customers","enforce_types":true,"document_schemas":{"customers":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"name":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"customers_email_key","columns":["email"]}]}
    ;
    var parsed_parent = try schema_mod.parseValidatedTableSchema(std.testing.allocator, customers_schema_json);
    defer parsed_parent.deinit(std.testing.allocator);
    const parent_runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parsed_parent);
    defer storage_schema.freeSchema(std.testing.allocator, parent_runtime_schema);
    const parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}", parent_runtime_schema.relational_columns);
    defer std.testing.allocator.free(parent_row);
    const expected_parent = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, parent_row, parent_runtime_schema.unique_constraints[0]);
    defer if (expected_parent) |value| std.testing.allocator.free(value);
    const expected_parent_key = expected_parent orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = orders_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_email_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 8,
                    .constraint_name = "customers_email_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9002,
                    .topology_epoch = 43,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        expected_parent_key: []const u8,
        parent_prepared: bool = false,
        owner_prepared: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 3), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 8001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                try std.testing.expectEqualStrings("customer:ada", req.req.deletes[0]);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("customer:ada", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 8), req.req.predicates[0].expected_version);
                self.parent_prepared = true;
            } else if (group_id == 9002) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.unique_constraint_writes.len);
                try std.testing.expectEqualStrings("customers_email_key", req.req.unique_constraint_deletes[0].constraint_name);
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_delete_checks.len);
                const check = req.req.foreign_key_parent_delete_checks[0];
                try std.testing.expectEqualStrings("orders_customer_email_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("customers", check.parent_table);
                try std.testing.expectEqualStrings(self.expected_parent_key, check.parent_key);
                self.owner_prepared = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 8001), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            try std.testing.expectEqualStrings("customer:ada", key);
            return .{
                .json = try alloc.dupe(u8, "{\"email\":\"ada@example.test\",\"name\":\"Ada\"}"),
                .version = 8,
            };
        }
    };

    var unversioned_recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const unversioned_result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        unversioned_recorder.worker(),
        try parseTxnIdHex("dddd0000111122223333444455556666"),
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"customer:ada"},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), unversioned_result.committed.participant_count);
    try std.testing.expect(unversioned_recorder.parent_prepared);
    try std.testing.expect(unversioned_recorder.owner_prepared);

    var recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("dddd0000111122223333444455557777"),
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"customer:ada"},
            .predicates = &.{.{ .key = "customer:ada", .expected_version = 8 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), result.committed.participant_count);
    try std.testing.expect(recorder.parent_prepared);
    try std.testing.expect(recorder.owner_prepared);
}

test "distributed txn coordinator routes unique foreign key set-null parent deletes through ref owners" {
    try runUniqueForeignKeyParentDeleteActionRoutingTest(.set_null);
}

test "distributed txn coordinator routes unique foreign key cascade parent deletes through ref owners" {
    try runUniqueForeignKeyParentDeleteActionRoutingTest(.cascade);
}

test "distributed txn explain routes restrict parent deletes through ref owners" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                    .foreign_key_ref_children_group = foreignKeyRefChildrenGroup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 8001), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            try std.testing.expectEqualStrings("cust:ada", key);
            return .{ .json = try alloc.dupe(u8, "{}"), .version = 3 };
        }

        fn foreignKeyRefChildrenGroup(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) ![]db_mod.types.ForeignKeyRefChild {
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("orders_customer_id_fkey", req.constraint_name);
            try std.testing.expectEqualStrings("customers", req.parent_table);
            try std.testing.expectEqualStrings("cust:ada", req.parent_key);
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
            children[0] = .{
                .child_table = try alloc.dupe(u8, "row"),
                .child_key = try alloc.dupe(u8, "order:1"),
            };
            return children;
        }
    };

    var recorder = Recorder{};
    const explain = (try explainRoutedForeignKeyParentDelete(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "customers",
        null,
        "cust:ada",
    )) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 8001), explain.parent_group_id);
    try std.testing.expectEqual(@as(usize, 1), explain.routed_owner_group_count);
    try std.testing.expect(explain.plan.exists);
    try std.testing.expect(!explain.plan.allowed);
    try std.testing.expectEqual(relational_store.ForeignKeyDeletePlanBlockReason.restrict, explain.plan.block_reason);
}

test "distributed txn explain fails closed on incomplete routed ref owner scans" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                    .foreign_key_ref_children_group = foreignKeyRefChildrenGroup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {}
        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {}
        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 8001), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            try std.testing.expectEqualStrings("cust:ada", key);
            return .{ .json = try alloc.dupe(u8, "{}"), .version = 3 };
        }

        fn foreignKeyRefChildrenGroup(
            _: *anyopaque,
            _: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) ![]db_mod.types.ForeignKeyRefChild {
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("orders_customer_id_fkey", req.constraint_name);
            return error.ForeignKeyActionLimitExceeded;
        }
    };

    var recorder = Recorder{};
    try std.testing.expectError(error.ForeignKeyActionLimitExceeded, explainRoutedForeignKeyParentDelete(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "customers",
        null,
        "cust:ada",
    ));
}

fn runUniqueForeignKeyParentDeleteActionRoutingTest(comptime action: enum { set_null, cascade }) !void {
    const schema_json = switch (action) {
        .set_null =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"set_null"}],"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
        ,
        .cascade =>
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"email":{"type":"keyword"},"ref_email":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"users_ref_email_fkey","columns":["ref_email"],"references":{"table":"row","columns":["email"]},"on_delete":"cascade"}],"unique_constraints":[{"name":"users_email_key","columns":["email"]}]}
        ,
    };
    const is_set_null = action == .set_null;
    var parsed_schema = try schema_mod.parseValidatedTableSchema(std.testing.allocator, schema_json);
    defer parsed_schema.deinit(std.testing.allocator);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(std.testing.allocator, parsed_schema);
    defer storage_schema.freeSchema(std.testing.allocator, runtime_schema);
    const parent_row = try document_mapper.buildRelationalRowValueAlloc(std.testing.allocator, "{\"email\":\"ada@example.test\"}", runtime_schema.relational_columns);
    defer std.testing.allocator.free(parent_row);
    const expected_parent = try relational_store.uniqueConstraintTupleValueAlloc(std.testing.allocator, parent_row, runtime_schema.unique_constraints[0]);
    defer if (expected_parent) |value| std.testing.allocator.free(value);
    const expected_parent_key = expected_parent orelse unreachable;

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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "user:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "user:m", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{
                    .{
                        .child_table_id = 7,
                        .constraint_name = "users_ref_email_fkey",
                        .parent_table_id = 7,
                        .start_parent_key = "",
                        .end_parent_key = null,
                        .group_id = 9001,
                        .topology_epoch = 42,
                    },
                    .{
                        .child_table_id = 7,
                        .constraint_name = "users_ref_email_fkey",
                        .parent_table_id = 7,
                        .start_parent_key = "",
                        .end_parent_key = null,
                        .group_id = 9003,
                        .topology_epoch = 44,
                    },
                })[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 7,
                    .constraint_name = "users_email_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9002,
                    .topology_epoch = 43,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        expected_parent_key: []const u8,
        parent_prepared: bool = false,
        owner_9001_prepared: bool = false,
        owner_9003_prepared: bool = false,
        unique_owner_prepared: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                    .foreign_key_ref_children_group = foreignKeyRefChildrenGroup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 4), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            if (group_id == 7002) {
                try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                try std.testing.expectEqualStrings("user:parent", req.req.deletes[0]);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("user:parent", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 5), req.req.predicates[0].expected_version);
                self.parent_prepared = true;
            } else if (group_id == 9001 or group_id == 9003) {
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_delete_checks.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_conflict_checks.len);
                try std.testing.expectEqualStrings("users_ref_email_fkey", req.req.foreign_key_conflict_checks[0].constraint_name);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_conflict_checks[0].parent_table);
                try std.testing.expectEqualStrings(self.expected_parent_key, req.req.foreign_key_conflict_checks[0].parent_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_set_null_children.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_cascade_children.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_action_schedules.len);
                try std.testing.expectEqualStrings(if (is_set_null) "set_null" else "cascade", req.req.foreign_key_action_schedules[0].action);
                try std.testing.expectEqualStrings("txn-coordinator", req.req.foreign_key_action_schedules[0].worker_id);
                try std.testing.expectEqualStrings("users_ref_email_fkey", req.req.foreign_key_action_schedules[0].constraint_name);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_action_schedules[0].parent_table);
                try std.testing.expectEqualStrings(self.expected_parent_key, req.req.foreign_key_action_schedules[0].parent_key);
                try std.testing.expectEqual(@as(usize, 1024), req.req.foreign_key_action_schedules[0].page_limit);
                if (group_id == 9001) {
                    self.owner_9001_prepared = true;
                } else {
                    self.owner_9003_prepared = true;
                }
            } else if (group_id == 9002) {
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                try std.testing.expectEqualStrings("users_email_key", req.req.unique_constraint_deletes[0].constraint_name);
                try std.testing.expectEqualStrings(self.expected_parent_key, req.req.unique_constraint_deletes[0].encoded_value);
                try std.testing.expectEqualStrings("user:parent", req.req.unique_constraint_deletes[0].owner_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.unique_constraint_writes.len);
                self.unique_owner_prepared = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7002), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("user:parent", key);
            return .{
                .json = try alloc.dupe(u8, "{\"email\":\"ada@example.test\"}"),
                .version = 5,
            };
        }

        fn foreignKeyRefChildrenGroup(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) ![]db_mod.types.ForeignKeyRefChild {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("users_ref_email_fkey", req.constraint_name);
            try std.testing.expectEqualStrings("row", req.parent_table);
            try std.testing.expectEqualStrings(self.expected_parent_key, req.parent_key);
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
            errdefer alloc.free(children);
            children[0] = .{
                .child_table = try alloc.dupe(u8, "row"),
                .child_key = try alloc.dupe(u8, "user:child"),
            };
            return children;
        }
    };

    var recorder = Recorder{ .expected_parent_key = expected_parent_key };
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex(if (is_set_null) "dddd0000111122223333444455558888" else "dddd0000111122223333444455559999"),
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .deletes = &.{"user:parent"},
            .predicates = &.{.{ .key = "user:parent", .expected_version = 5 }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 4), result.committed.participant_count);
    try std.testing.expect(recorder.parent_prepared);
    try std.testing.expect(recorder.owner_9001_prepared);
    try std.testing.expect(recorder.owner_9003_prepared);
    try std.testing.expect(recorder.unique_owner_prepared);
}

test "distributed txn coordinator routes foreign key reference transforms with final-value planning" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
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
        lookup_calls: usize = 0,
        prepared_child_transform: bool = false,
        prepared_parent_check: bool = false,
        prepared_owner_mutations: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_calls += 1;
            try std.testing.expectEqual(@as(usize, 3), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
            if (group_id == 7001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.transforms.len);
                const key = req.req.transforms[0].key;
                try std.testing.expect(
                    std.mem.eql(u8, "doc:a-order", key) or
                        std.mem.eql(u8, "doc:b-order", key) or
                        std.mem.eql(u8, "doc:c-order", key) or
                        std.mem.eql(u8, "doc:d-order", key),
                );
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings(key, req.req.predicates[0].key);
                const expected_version: u64 = if (std.mem.eql(u8, "doc:a-order", key))
                    7
                else if (std.mem.eql(u8, "doc:d-order", key))
                    8
                else
                    0;
                try std.testing.expectEqual(expected_version, req.req.predicates[0].expected_version);
                if (std.mem.eql(u8, "doc:c-order", key)) {
                    try std.testing.expectEqual(@as(usize, 1), req.req.writes.len);
                    try std.testing.expectEqualStrings("doc:c-order", req.req.writes[0].key);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
                }
                if (std.mem.eql(u8, "doc:d-order", key)) {
                    try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                    try std.testing.expectEqualStrings("doc:d-order", req.req.deletes[0]);
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.deletes.len);
                }
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_writes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_deletes.len);
                self.prepared_child_transform = true;
            } else if (group_id == 8001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_checks.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_parent_checks[0].constraint_name);
                try std.testing.expectEqualStrings("docs", req.req.foreign_key_parent_checks[0].child_table);
                try std.testing.expect(
                    std.mem.eql(u8, "doc:a-order", req.req.foreign_key_parent_checks[0].child_key) or
                        std.mem.eql(u8, "doc:b-order", req.req.foreign_key_parent_checks[0].child_key) or
                        std.mem.eql(u8, "doc:c-order", req.req.foreign_key_parent_checks[0].child_key) or
                        std.mem.eql(u8, "doc:d-order", req.req.foreign_key_parent_checks[0].child_key),
                );
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_parent_checks[0].parent_table);
                try std.testing.expect(
                    std.mem.eql(u8, "cust:z-customer", req.req.foreign_key_parent_checks[0].parent_key) or
                        std.mem.eql(u8, "cust:y-customer", req.req.foreign_key_parent_checks[0].parent_key) or
                        std.mem.eql(u8, "cust:c-final", req.req.foreign_key_parent_checks[0].parent_key) or
                        std.mem.eql(u8, "cust:d-final", req.req.foreign_key_parent_checks[0].parent_key),
                );
                self.prepared_parent_check = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_ref_writes.len);
                if (req.req.foreign_key_ref_deletes.len == 1) {
                    try std.testing.expect(
                        std.mem.eql(u8, "cust:a-customer", req.req.foreign_key_ref_deletes[0].parent_key) or
                            std.mem.eql(u8, "cust:d-old", req.req.foreign_key_ref_deletes[0].parent_key),
                    );
                    try std.testing.expect(
                        std.mem.eql(u8, "cust:z-customer", req.req.foreign_key_ref_writes[0].parent_key) or
                            std.mem.eql(u8, "cust:d-final", req.req.foreign_key_ref_writes[0].parent_key),
                    );
                    try std.testing.expect(
                        std.mem.eql(u8, "doc:a-order", req.req.foreign_key_ref_writes[0].child_key) or
                            std.mem.eql(u8, "doc:d-order", req.req.foreign_key_ref_writes[0].child_key),
                    );
                } else {
                    try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_deletes.len);
                    try std.testing.expect(
                        std.mem.eql(u8, "cust:y-customer", req.req.foreign_key_ref_writes[0].parent_key) or
                            std.mem.eql(u8, "cust:c-final", req.req.foreign_key_ref_writes[0].parent_key),
                    );
                    try std.testing.expect(
                        std.mem.eql(u8, "doc:b-order", req.req.foreign_key_ref_writes[0].child_key) or
                            std.mem.eql(u8, "doc:c-order", req.req.foreign_key_ref_writes[0].child_key),
                    );
                }
                try std.testing.expectEqualStrings("row", req.req.foreign_key_ref_writes[0].child_table);
                self.prepared_owner_mutations = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.resolve_calls += 1;
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }

        fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            if (std.mem.eql(u8, "doc:a-order", key)) {
                return .{
                    .json = try alloc.dupe(u8, "{\"customer_id\":\"cust:a-customer\",\"status\":\"open\"}"),
                    .version = 7,
                };
            }
            try std.testing.expectEqualStrings("doc:d-order", key);
            return .{
                .json = try alloc.dupe(u8, "{\"customer_id\":\"cust:d-old\",\"status\":\"open\"}"),
                .version = 8,
            };
        }
    };

    const set_customer = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "customer_id",
        .value_json = "\"cust:z-customer\"",
    }};
    const upsert_customer = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "customer_id",
        .value_json = "\"cust:y-customer\"",
    }};
    const set_customer_final = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "customer_id",
        .value_json = "\"cust:c-final\"",
    }};
    const upsert_customer_after_delete = [_]db_mod.types.TransformOp{.{
        .op = .set,
        .path = "customer_id",
        .value_json = "\"cust:d-final\"",
    }};
    const txn_id = try parseTxnIdHex("abcdefabcdefabcdefabcdefabcdefab");
    var unversioned_recorder = Recorder{};
    const unversioned_outcome = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        unversioned_recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .transforms = &.{.{
                .key = "doc:a-order",
                .operations = set_customer[0..],
            }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), unversioned_outcome.committed.participant_count);
    try std.testing.expectEqual(@as(usize, 3), unversioned_recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 3), unversioned_recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 3), unversioned_recorder.resolve_calls);
    try std.testing.expectEqual(@as(usize, 1), unversioned_recorder.lookup_calls);

    var recorder = Recorder{};
    const outcome = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "docs",
        txn_id,
        10_100,
        10_101,
        .{
            .transforms = &.{.{
                .key = "doc:a-order",
                .operations = set_customer[0..],
            }},
            .predicates = &.{.{ .key = "doc:a-order", .expected_version = 7 }},
        },
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), outcome.participant_count);
    try std.testing.expectEqual(@as(usize, 3), recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 3), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 3), recorder.resolve_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.lookup_calls);
    try std.testing.expect(recorder.prepared_child_transform);
    try std.testing.expect(recorder.prepared_parent_check);
    try std.testing.expect(recorder.prepared_owner_mutations);

    const upsert_outcome = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "docs",
        txn_id,
        10_200,
        10_201,
        .{
            .transforms = &.{.{
                .key = "doc:b-order",
                .operations = upsert_customer[0..],
                .upsert = true,
            }},
            .predicates = &.{.{ .key = "doc:b-order", .expected_version = 0 }},
        },
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), upsert_outcome.participant_count);
    try std.testing.expectEqual(@as(usize, 6), recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 6), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 6), recorder.resolve_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.lookup_calls);

    const write_transform_outcome = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "docs",
        txn_id,
        10_300,
        10_301,
        .{
            .writes = &.{.{ .key = "doc:c-order", .value = "{\"customer_id\":\"cust:c-initial\",\"status\":\"draft\"}" }},
            .transforms = &.{.{
                .key = "doc:c-order",
                .operations = set_customer_final[0..],
            }},
            .predicates = &.{.{ .key = "doc:c-order", .expected_version = 0 }},
        },
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), write_transform_outcome.participant_count);
    try std.testing.expectEqual(@as(usize, 9), recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 9), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 9), recorder.resolve_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.lookup_calls);

    const delete_transform_outcome = try executeCrossGroup(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        "docs",
        txn_id,
        10_400,
        10_401,
        .{
            .deletes = &.{"doc:d-order"},
            .transforms = &.{.{
                .key = "doc:d-order",
                .operations = upsert_customer_after_delete[0..],
                .upsert = true,
            }},
            .predicates = &.{.{ .key = "doc:d-order", .expected_version = 8 }},
        },
        null,
    );
    try std.testing.expectEqual(@as(usize, 3), delete_transform_outcome.participant_count);
    try std.testing.expectEqual(@as(usize, 12), recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 12), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 12), recorder.resolve_calls);
    try std.testing.expectEqual(@as(usize, 2), recorder.lookup_calls);
}

test "distributed txn coordinator allows non-reference transforms on foreign key tables" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"},"status":{"type":"keyword"},"count":{"type":"numeric"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
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

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(usize, 1), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(@as(usize, 0), req.req.writes.len);
            try std.testing.expectEqual(@as(usize, 0), req.req.deletes.len);
            try std.testing.expectEqual(@as(usize, 1), req.req.transforms.len);
            try std.testing.expectEqualStrings("doc:a-order", req.req.transforms[0].key);
            try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_checks.len);
            try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_delete_checks.len);
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.resolve_calls += 1;
            try std.testing.expectEqual(@as(u64, 7001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }
    };

    const update_status = [_]db_mod.types.TransformOp{
        .{ .op = .set, .path = "status", .value_json = "\"paid\"" },
        .{ .op = .inc, .path = "count", .value_json = "1" },
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("abcdefabcdefabcdefabcdefabcdefac");
    const outcome = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "docs",
            .transforms = &.{.{
                .key = "doc:a-order",
                .operations = update_status[0..],
            }},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 1), outcome.committed.participant_count);
    try std.testing.expectEqual(@as(usize, 1), recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.prepare_calls);
    try std.testing.expectEqual(@as(usize, 1), recorder.resolve_calls);
}

test "distributed txn coordinator fails closed without foreign key ref owner parent delete ranges" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
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
            return error.UnexpectedWorkerCall;
        }

        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("3333444455556666777788889999aaaa");
    try std.testing.expectError(error.UnsupportedOperation, executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"cust:z-customer"},
        }},
        null,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);
}

test "distributed txn coordinator routes foreign key parent deletes through ref owners when configured" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        prepared_parent_delete: bool = false,
        prepared_owner_check: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 8001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.deletes[0]);
                self.prepared_parent_delete = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_delete_checks.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_parent_delete_checks[0].constraint_name);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_parent_delete_checks[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_parent_delete_checks[0].parent_key);
                self.prepared_owner_check = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("3333444455556666777788889999aaab");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"cust:z-customer"},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.prepared_parent_delete);
    try std.testing.expect(recorder.prepared_owner_check);
}

test "distributed txn coordinator routes deferred foreign key parent deletes through ref owners when configured" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict","timing":"deferred"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        prepared_parent_delete: bool = false,
        prepared_owner_check: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 8001) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.deletes[0]);
                self.prepared_parent_delete = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_parent_delete_checks.len);
                const check = req.req.foreign_key_parent_delete_checks[0];
                try std.testing.expectEqualStrings("orders_customer_id_fkey", check.constraint_name);
                try std.testing.expectEqualStrings("customers", check.parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", check.parent_key);
                try std.testing.expectEqual(db_mod.types.ForeignKeyParentCheck.Timing.deferred, check.timing);
                self.prepared_owner_check = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("3333444455556666777788889999aabb");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"cust:z-customer"},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.prepared_parent_delete);
    try std.testing.expect(recorder.prepared_owner_check);
}

test "distributed txn coordinator fails closed for transitional foreign key ref owner parent deletes" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                    .state = metadata_table_manager.foreign_key_ref_range_rebuilding,
                }})[0..]),
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
            return error.UnexpectedWorkerCall;
        }

        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }
    };

    var recorder = Recorder{};
    try std.testing.expectError(error.UnknownGroup, executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        try parseTxnIdHex("3333444455556666777788889999aaaf"),
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"cust:z-customer"},
        }},
        null,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);
}

test "distributed txn coordinator ignores unrelated foreign key child tables for parent delete planning" {
    const unrelated_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"account_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"docs_account_id_fkey","columns":["account_id"],"references":{"table":"accounts","columns":["_id"]},"on_delete":"restrict"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = unrelated_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
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
        prepared_parent_delete: bool = false,

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

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 1), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 8002), group_id);
            try std.testing.expectEqualStrings("customers", table_name);
            try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
            try std.testing.expectEqualStrings("cust:z-customer", req.req.deletes[0]);
            try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_delete_checks.len);
            self.prepared_parent_delete = true;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("4567890abcdef1234567890abcdef123");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"cust:z-customer"},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 1), result.committed.participant_count);
    try std.testing.expect(recorder.prepared_parent_delete);
}

test "distributed txn coordinator routes distributed foreign key set-null actions across child ranges" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        prepared_parent_delete: bool = false,
        prepared_owner_conflict: bool = false,
        owner_page_calls: u8 = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 8002) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.deletes[0]);
                self.prepared_parent_delete = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_delete_checks.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_conflict_checks.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_conflict_checks[0].constraint_name);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_conflict_checks[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_conflict_checks[0].parent_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_set_null_children.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_cascade_children.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_action_schedules.len);
                try std.testing.expectEqualStrings("set_null", req.req.foreign_key_action_schedules[0].action);
                try std.testing.expectEqualStrings("txn-coordinator", req.req.foreign_key_action_schedules[0].worker_id);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_action_schedules[0].constraint_name);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_action_schedules[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_action_schedules[0].parent_key);
                try std.testing.expectEqual(@as(usize, 1024), req.req.foreign_key_action_schedules[0].page_limit);
                self.prepared_owner_conflict = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn foreignKeyRefChildrenPageGroup(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) !db_mod.types.ForeignKeyRefChildrenPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("orders_customer_id_fkey", req.constraint_name);
            try std.testing.expectEqualStrings("customers", req.parent_table);
            try std.testing.expectEqualStrings("cust:z-customer", req.parent_key);

            self.owner_page_calls += 1;
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
            errdefer alloc.free(children);
            if (self.owner_page_calls == 1) {
                try std.testing.expect(req.start_after_child_table == null);
                try std.testing.expect(req.start_after_child_key == null);
                children[0] = try makeRefChild(alloc, "doc:a-order");
                errdefer freeRefChild(alloc, children[0]);
                const cursor = blk: {
                    const table = try alloc.dupe(u8, "row");
                    errdefer alloc.free(table);
                    const key = try alloc.dupe(u8, "doc:a-order");
                    break :blk .{ .table = table, .key = key };
                };
                return .{
                    .children = children,
                    .complete = false,
                    .next_child_table = cursor.table,
                    .next_child_key = cursor.key,
                };
            }
            try std.testing.expectEqual(@as(u8, 2), self.owner_page_calls);
            try std.testing.expectEqualStrings("row", req.start_after_child_table orelse return error.TestUnexpectedResult);
            try std.testing.expectEqualStrings("doc:a-order", req.start_after_child_key orelse return error.TestUnexpectedResult);
            children[0] = try makeRefChild(alloc, "doc:z-order");
            return .{
                .children = children,
                .complete = true,
            };
        }

        fn makeRefChild(alloc: std.mem.Allocator, child_key: []const u8) !db_mod.types.ForeignKeyRefChild {
            const child_table = try alloc.dupe(u8, "row");
            errdefer alloc.free(child_table);
            const key = try alloc.dupe(u8, child_key);
            return .{
                .child_table = child_table,
                .child_key = key,
            };
        }

        fn freeRefChild(alloc: std.mem.Allocator, child: db_mod.types.ForeignKeyRefChild) void {
            alloc.free(@constCast(child.child_table));
            alloc.free(@constCast(child.child_key));
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("bbbbccccddddeeeeffff000011112222");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"cust:z-customer"},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.prepared_parent_delete);
    try std.testing.expectEqual(@as(u8, 0), recorder.owner_page_calls);
    try std.testing.expect(recorder.prepared_owner_conflict);
}

test "foreign key action page executes owner ref cleanup and child mutation through routed participants" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        began: usize = 0,
        resolved: usize = 0,
        prepared_owner: bool = false,
        prepared_child: bool = false,
        paged_owner: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                    .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(group_id == 9001 or group_id == 7002);
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
            self.began += 1;
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            if (group_id == 9001) {
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_ref_deletes[0].constraint_name);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_ref_deletes[0].child_table);
                try std.testing.expectEqualStrings("doc:z-order", req.req.foreign_key_ref_deletes[0].child_key);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_ref_deletes[0].parent_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_set_null_children.len);
                self.prepared_owner = true;
            } else if (group_id == 7002) {
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_set_null_children.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_set_null_children[0].constraint_name);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_set_null_children[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_set_null_children[0].parent_key);
                try std.testing.expectEqualStrings("doc:z-order", req.req.foreign_key_set_null_children[0].child_key);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("doc:z-order", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 88), req.req.predicates[0].expected_version);
                self.prepared_child = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(group_id == 9001 or group_id == 7002);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            self.resolved += 1;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7002), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:z-order", key);
            return .{
                .json = try alloc.dupe(u8, "{\"customer_id\":\"cust:z-customer\"}"),
                .version = 88,
            };
        }

        fn foreignKeyRefChildrenPageGroup(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) !db_mod.types.ForeignKeyRefChildrenPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("orders_customer_id_fkey", req.constraint_name);
            try std.testing.expectEqualStrings("customers", req.parent_table);
            try std.testing.expectEqualStrings("cust:z-customer", req.parent_key);
            try std.testing.expect(req.start_after_child_table == null);
            try std.testing.expect(req.start_after_child_key == null);

            self.paged_owner = true;
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
            errdefer alloc.free(children);
            children[0] = .{
                .child_table = try alloc.dupe(u8, "row"),
                .child_key = try alloc.dupe(u8, "doc:z-order"),
            };
            return .{
                .children = children,
                .complete = true,
            };
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("abcdef00112233445566778899aabbcc");
    var execution = try executeForeignKeyActionPage(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        11_000,
        11_001,
        "docs",
        9001,
        "ON DELETE SET NULL",
        "orders_customer_id_fkey",
        "customers",
        "cust:z-customer",
        null,
        null,
        null,
        10,
        0,
        foreign_key_action_default_cascade_max_depth,
        null,
    );
    defer execution.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), execution.applied_children);
    try std.testing.expect(execution.complete);
    try std.testing.expectEqual(@as(usize, 2), execution.participant_count);
    try std.testing.expect(recorder.paged_owner);
    try std.testing.expectEqual(@as(usize, 2), recorder.began);
    try std.testing.expect(recorder.prepared_owner);
    try std.testing.expect(recorder.prepared_child);
    try std.testing.expectEqual(@as(usize, 2), recorder.resolved);
}

test "foreign key action page fails closed for transitional ref owner topology" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 43,
                    .state = metadata_table_manager.foreign_key_ref_range_splitting,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        page_calls: usize = 0,
        begin_calls: usize = 0,
        prepare_calls: usize = 0,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                    .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.begin_calls += 1;
            return error.TestUnexpectedResult;
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
            return error.TestUnexpectedResult;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: []const u8) !?table_reads.LookupResponse {
            return error.TestUnexpectedResult;
        }

        fn foreignKeyRefChildrenPageGroup(
            ptr: *anyopaque,
            _: std.mem.Allocator,
            _: u64,
            _: []const u8,
            _: ForeignKeyRefChildrenRequest,
        ) !db_mod.types.ForeignKeyRefChildrenPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.page_calls += 1;
            return error.TestUnexpectedResult;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("abcdef00112233445566778899aabbcd");
    try std.testing.expectError(error.TopologyChanged, executeForeignKeyActionPage(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        11_100,
        11_101,
        "docs",
        9001,
        "ON DELETE SET NULL",
        "orders_customer_id_fkey",
        "customers",
        "cust:z-customer",
        null,
        null,
        null,
        10,
        0,
        foreign_key_action_default_cascade_max_depth,
        null,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.page_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.prepare_calls);
}

test "foreign key action page routes update cascade child mutations with replacement parent key" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"unique_constraints":[{"name":"orders_customer_id_key","columns":["customer_id"]}],"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_update":"cascade"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .unique_constraint_ranges = @constCast((&[_]metadata_table_manager.UniqueConstraintRangeRecord{.{
                    .table_id = 7,
                    .constraint_name = "orders_customer_id_key",
                    .start_encoded_value = "",
                    .end_encoded_value = null,
                    .group_id = 9101,
                    .topology_epoch = 9,
                    .state = metadata_table_manager.unique_constraint_range_active,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        began: usize = 0,
        resolved: usize = 0,
        prepared_owner: bool = false,
        prepared_child: bool = false,
        paged_owner: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                    .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(group_id == 9001 or group_id == 7002 or group_id == 9101);
            try std.testing.expectEqual(@as(usize, 3), req.participants.len);
            self.began += 1;
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            if (group_id == 9001) {
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_ref_deletes[0].constraint_name);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_ref_deletes[0].child_table);
                try std.testing.expectEqualStrings("doc:z-order", req.req.foreign_key_ref_deletes[0].child_key);
                try std.testing.expectEqualStrings("cust:old", req.req.foreign_key_ref_deletes[0].parent_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_cascade_children.len);
                self.prepared_owner = true;
            } else if (group_id == 7002) {
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_cascade_children.len);
                const action = req.req.foreign_key_cascade_children[0];
                try std.testing.expectEqual(db_mod.types.ForeignKeyCascadeChildAction.Operation.update, action.operation);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", action.constraint_name);
                try std.testing.expectEqualStrings("customers", action.parent_table);
                try std.testing.expectEqualStrings("cust:old", action.parent_key);
                try std.testing.expectEqualStrings("cust:new", action.updated_parent_key orelse return error.TestUnexpectedResult);
                try std.testing.expectEqualStrings("doc:z-order", action.child_key);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("doc:z-order", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 77), req.req.predicates[0].expected_version);
                self.prepared_child = true;
            } else if (group_id == 9101) {
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_deletes.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.unique_constraint_writes.len);
                try std.testing.expectEqualStrings("orders_customer_id_key", req.req.unique_constraint_deletes[0].constraint_name);
                try std.testing.expectEqualStrings("orders_customer_id_key", req.req.unique_constraint_writes[0].constraint_name);
                try std.testing.expectEqualStrings("doc:z-order", req.req.unique_constraint_deletes[0].owner_key);
                try std.testing.expectEqualStrings("doc:z-order", req.req.unique_constraint_writes[0].owner_key);
                try std.testing.expect(req.req.unique_constraint_deletes[0].encoded_value.len > 0);
                try std.testing.expect(req.req.unique_constraint_writes[0].encoded_value.len > 0);
                try std.testing.expect(!std.mem.eql(u8, req.req.unique_constraint_deletes[0].encoded_value, req.req.unique_constraint_writes[0].encoded_value));
            } else return error.UnexpectedGroup;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(group_id == 9001 or group_id == 7002 or group_id == 9101);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            self.resolved += 1;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7002), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:z-order", key);
            return .{
                .json = try alloc.dupe(u8, "{\"customer_id\":\"cust:old\"}"),
                .version = 77,
            };
        }

        fn foreignKeyRefChildrenPageGroup(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) !db_mod.types.ForeignKeyRefChildrenPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("orders_customer_id_fkey", req.constraint_name);
            try std.testing.expectEqualStrings("customers", req.parent_table);
            try std.testing.expectEqualStrings("cust:old", req.parent_key);
            try std.testing.expect(req.start_after_child_table == null);
            try std.testing.expect(req.start_after_child_key == null);

            self.paged_owner = true;
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
            errdefer alloc.free(children);
            children[0] = .{
                .child_table = try alloc.dupe(u8, "row"),
                .child_key = try alloc.dupe(u8, "doc:z-order"),
            };
            return .{
                .children = children,
                .complete = true,
            };
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("abcdef00112233445566778899aabbd1");
    var execution = try executeForeignKeyActionPage(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        11_000,
        11_001,
        "docs",
        9001,
        "ON UPDATE CASCADE",
        "orders_customer_id_fkey",
        "customers",
        "cust:old",
        "cust:new",
        null,
        null,
        10,
        0,
        foreign_key_action_default_cascade_max_depth,
        null,
    );
    defer execution.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), execution.applied_children);
    try std.testing.expect(execution.complete);
    try std.testing.expectEqual(@as(usize, 3), execution.participant_count);
    try std.testing.expect(recorder.paged_owner);
    try std.testing.expectEqual(@as(usize, 3), recorder.began);
    try std.testing.expect(recorder.prepared_owner);
    try std.testing.expect(recorder.prepared_child);
    try std.testing.expectEqual(@as(usize, 3), recorder.resolved);
}

test "foreign key action page schedules recursive cascade work for deleted children" {
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"cascade"}]}
    ;
    const line_items_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"order_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"line_items_order_id_fkey","columns":["order_id"],"references":{"table":"orders","columns":["_id"]},"on_delete":"cascade"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                    .{ .table_id = 9, .name = "line_items", .schema_json = line_items_schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "order:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "order:m", .end_key = null },
                    .{ .group_id = 7101, .table_id = 9, .start_key = "", .end_key = "line:m" },
                    .{ .group_id = 7102, .table_id = 9, .start_key = "line:m", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{
                    .{
                        .child_table_id = 7,
                        .constraint_name = "orders_customer_id_fkey",
                        .parent_table_id = 8,
                        .start_parent_key = "",
                        .end_parent_key = null,
                        .group_id = 9001,
                        .topology_epoch = 42,
                    },
                    .{
                        .child_table_id = 9,
                        .constraint_name = "line_items_order_id_fkey",
                        .parent_table_id = 7,
                        .start_parent_key = "",
                        .end_parent_key = null,
                        .group_id = 9101,
                        .topology_epoch = 43,
                    },
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
        began: usize = 0,
        resolved: usize = 0,
        prepared_order_owner: bool = false,
        prepared_order_child: bool = false,
        prepared_line_item_owner: bool = false,
        paged_order_owner: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(group_id == 9001 or group_id == 7002 or group_id == 9101);
            try std.testing.expectEqual(@as(usize, 3), req.participants.len);
            self.began += 1;
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 9001) {
                try std.testing.expectEqualStrings("orders", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_ref_deletes[0].constraint_name);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_ref_deletes[0].child_table);
                try std.testing.expectEqualStrings("order:z", req.req.foreign_key_ref_deletes[0].child_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_action_schedules.len);
                self.prepared_order_owner = true;
            } else if (group_id == 7002) {
                try std.testing.expectEqualStrings("orders", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_cascade_children.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_cascade_children[0].constraint_name);
                try std.testing.expectEqualStrings("order:z", req.req.foreign_key_cascade_children[0].child_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_action_schedules.len);
                self.prepared_order_child = true;
            } else if (group_id == 9101) {
                try std.testing.expectEqualStrings("line_items", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_conflict_checks.len);
                try std.testing.expectEqualStrings("line_items_order_id_fkey", req.req.foreign_key_conflict_checks[0].constraint_name);
                try std.testing.expectEqualStrings("orders", req.req.foreign_key_conflict_checks[0].parent_table);
                try std.testing.expectEqualStrings("order:z", req.req.foreign_key_conflict_checks[0].parent_key);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_action_schedules.len);
                const schedule = req.req.foreign_key_action_schedules[0];
                try std.testing.expectEqualStrings("cascade", schedule.action);
                try std.testing.expectEqualStrings("line_items_order_id_fkey", schedule.constraint_name);
                try std.testing.expectEqualStrings("orders", schedule.parent_table);
                try std.testing.expectEqualStrings("order:z", schedule.parent_key);
                try std.testing.expectEqual(@as(u32, 1), schedule.cascade_depth);
                try std.testing.expectEqual(foreign_key_action_default_cascade_max_depth, schedule.cascade_max_depth);
                self.prepared_line_item_owner = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(group_id == 9001 or group_id == 7002 or group_id == 9101);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            self.resolved += 1;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn foreignKeyRefChildrenPageGroup(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) !db_mod.types.ForeignKeyRefChildrenPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("orders", table_name);
            try std.testing.expectEqualStrings("orders_customer_id_fkey", req.constraint_name);
            try std.testing.expectEqualStrings("customers", req.parent_table);
            try std.testing.expectEqualStrings("customer:recursive", req.parent_key);
            self.paged_order_owner = true;
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
            errdefer alloc.free(children);
            children[0] = .{
                .child_table = try alloc.dupe(u8, "row"),
                .child_key = try alloc.dupe(u8, "order:z"),
            };
            return .{ .children = children, .complete = true };
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("abcdef00112233445566778899aabbd2");
    var execution = try executeForeignKeyActionPage(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        12_500,
        12_501,
        "orders",
        9001,
        "cascade",
        "orders_customer_id_fkey",
        "customers",
        "customer:recursive",
        null,
        null,
        null,
        10,
        0,
        foreign_key_action_default_cascade_max_depth,
        null,
    );
    defer execution.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), execution.applied_children);
    try std.testing.expect(execution.complete);
    try std.testing.expectEqual(@as(usize, 3), execution.participant_count);
    try std.testing.expect(recorder.paged_order_owner);
    try std.testing.expect(recorder.prepared_order_owner);
    try std.testing.expect(recorder.prepared_order_child);
    try std.testing.expect(recorder.prepared_line_item_owner);
    try std.testing.expectEqual(@as(usize, 3), recorder.began);
    try std.testing.expectEqual(@as(usize, 3), recorder.resolved);

    var capped_recorder = Recorder{};
    try std.testing.expectError(error.ForeignKeyCascadeDepthLimit, executeForeignKeyActionPage(
        std.testing.allocator,
        FakeCatalog.iface(),
        capped_recorder.worker(),
        try parseTxnIdHex("abcdef00112233445566778899aabbd3"),
        12_600,
        12_601,
        "orders",
        9001,
        "cascade",
        "orders_customer_id_fkey",
        "customers",
        "customer:recursive",
        null,
        null,
        null,
        10,
        1,
        1,
        null,
    ));
    try std.testing.expect(capped_recorder.paged_order_owner);
    try std.testing.expectEqual(@as(usize, 0), capped_recorder.began);
}

test "foreign key action page accepts same table runtime parent identity for durable schedules" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"manager_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"row_manager_id_fkey","columns":["manager_id"],"references":{"table":"row","columns":["_id"]},"on_delete":"set_null"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "row_manager_id_fkey",
                    .parent_table_id = 7,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        began: usize = 0,
        resolved: usize = 0,
        prepared_owner: bool = false,
        prepared_child: bool = false,
        paged_owner: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                    .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(group_id == 9001 or group_id == 7002);
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
            self.began += 1;
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            if (group_id == 9001) {
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqualStrings("row_manager_id_fkey", req.req.foreign_key_ref_deletes[0].constraint_name);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_ref_deletes[0].parent_table);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_ref_deletes[0].child_table);
                try std.testing.expectEqualStrings("doc:z-child", req.req.foreign_key_ref_deletes[0].child_key);
                try std.testing.expectEqualStrings("doc:parent", req.req.foreign_key_ref_deletes[0].parent_key);
                self.prepared_owner = true;
            } else if (group_id == 7002) {
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_set_null_children.len);
                try std.testing.expectEqualStrings("row_manager_id_fkey", req.req.foreign_key_set_null_children[0].constraint_name);
                try std.testing.expectEqualStrings("row", req.req.foreign_key_set_null_children[0].parent_table);
                try std.testing.expectEqualStrings("doc:parent", req.req.foreign_key_set_null_children[0].parent_key);
                try std.testing.expectEqualStrings("doc:z-child", req.req.foreign_key_set_null_children[0].child_key);
                try std.testing.expectEqual(@as(usize, 1), req.req.predicates.len);
                try std.testing.expectEqualStrings("doc:z-child", req.req.predicates[0].key);
                try std.testing.expectEqual(@as(u64, 99), req.req.predicates[0].expected_version);
                self.prepared_child = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expect(group_id == 9001 or group_id == 7002);
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            self.resolved += 1;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(_: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            try std.testing.expectEqual(@as(u64, 7002), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("doc:z-child", key);
            return .{
                .json = try alloc.dupe(u8, "{\"manager_id\":\"doc:parent\"}"),
                .version = 99,
            };
        }

        fn foreignKeyRefChildrenPageGroup(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) !db_mod.types.ForeignKeyRefChildrenPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("row_manager_id_fkey", req.constraint_name);
            try std.testing.expectEqualStrings("row", req.parent_table);
            try std.testing.expectEqualStrings("doc:parent", req.parent_key);

            self.paged_owner = true;
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
            errdefer alloc.free(children);
            children[0] = .{
                .child_table = try alloc.dupe(u8, "row"),
                .child_key = try alloc.dupe(u8, "doc:z-child"),
            };
            return .{
                .children = children,
                .complete = true,
            };
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("abcdef00112233445566778899aabbee");
    var execution = try executeForeignKeyActionPage(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        12_000,
        12_001,
        "docs",
        9001,
        "set_null",
        "row_manager_id_fkey",
        "row",
        "doc:parent",
        null,
        null,
        null,
        10,
        0,
        foreign_key_action_default_cascade_max_depth,
        null,
    );
    defer execution.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), execution.applied_children);
    try std.testing.expect(execution.complete);
    try std.testing.expectEqual(@as(usize, 2), execution.participant_count);
    try std.testing.expect(recorder.paged_owner);
    try std.testing.expectEqual(@as(usize, 2), recorder.began);
    try std.testing.expect(recorder.prepared_owner);
    try std.testing.expect(recorder.prepared_child);
    try std.testing.expectEqual(@as(usize, 2), recorder.resolved);
}

test "distributed txn relational identity workload mixes owner topology churn and actions" {
    const metadata_table_manager = @import("../metadata/table_manager.zig");
    const metadata_api = @import("../metadata/api.zig");
    const raft_reconciler = @import("../raft/reconciler.zig");
    const metadata_transition_state = @import("../metadata/transition_state.zig");

    const customers_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"email":{"type":"keyword"}},"required":["tenant_id","customer_id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id","customer_id"]},"unique_constraints":[{"name":"customers_tenant_email_key","columns":["tenant_id","email"]}]}
    ;
    const orders_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"order_id":{"type":"keyword"},"customer_id":{"type":"keyword"},"customer_email":{"type":"keyword"},"nullable_customer_id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["tenant_id","order_id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id","order_id"]},"foreign_keys":[{"name":"orders_customer_pk_fkey","columns":["tenant_id","customer_id"],"references":{"table":"customers","columns":["tenant_id","customer_id"]},"on_delete":"restrict"},{"name":"orders_customer_email_fkey","columns":["tenant_id","customer_email"],"references":{"table":"customers","columns":["tenant_id","email"]},"on_delete":"restrict"},{"name":"orders_nullable_customer_fkey","columns":["nullable_customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"set_null"}]}
    ;
    const line_items_schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"tenant_id":{"type":"keyword"},"order_id":{"type":"keyword"},"line_id":{"type":"keyword"}},"required":["tenant_id","order_id","line_id"],"additionalProperties":false}}},"primary_key":{"columns":["tenant_id","order_id","line_id"]},"foreign_keys":[{"name":"lines_order_fkey","columns":["tenant_id","order_id"],"references":{"table":"orders","columns":["tenant_id","order_id"]},"on_delete":"cascade"}]}
    ;

    const TopologyPhase = enum {
        active_single,
        fk_ref_splitting,
        fk_ref_split_active,
        unique_splitting,
        unique_split_active,
        action_ref_merging,
        child_owner_moved,
    };

    const Catalog = struct {
        tables: [3]metadata_table_manager.TableRecord = undefined,
        ranges: [4]metadata_table_manager.RangeRecord = undefined,
        fk_ranges: [6]metadata_table_manager.ForeignKeyReferenceRangeRecord = undefined,
        fk_range_count: usize = 0,
        unique_ranges: [6]metadata_table_manager.UniqueConstraintRangeRecord = undefined,
        unique_range_count: usize = 0,

        fn iface(self: *@This()) table_catalog.CatalogSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .admin_snapshot = adminSnapshot,
                    .free_admin_snapshot = freeAdminSnapshot,
                },
            };
        }

        fn configure(self: *@This(), phase: TopologyPhase) void {
            self.tables = .{
                .{ .table_id = 8, .name = "customers", .schema_json = customers_schema_json, .placement_role = "data" },
                .{ .table_id = 7, .name = "orders", .schema_json = orders_schema_json, .placement_role = "data" },
                .{ .table_id = 9, .name = "line_items", .schema_json = line_items_schema_json, .placement_role = "data" },
            };
            self.ranges = .{
                .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = null },
                .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "order:m" },
                .{ .group_id = if (phase == .child_owner_moved) 7003 else 7002, .table_id = 7, .start_key = "order:m", .end_key = null },
                .{ .group_id = 7101, .table_id = 9, .start_key = "", .end_key = null },
            };

            self.fk_range_count = 0;
            self.addForeignKeyRange("orders_customer_pk_fkey", 7, 8, 9001, "", null, if (phase == .fk_ref_splitting) metadata_table_manager.foreign_key_ref_range_splitting else metadata_table_manager.foreign_key_ref_range_active);
            if (phase == .fk_ref_split_active) {
                self.fk_ranges[0].end_parent_key = "\xff";
                self.addForeignKeyRange("orders_customer_pk_fkey", 7, 8, 9002, "\xff", null, metadata_table_manager.foreign_key_ref_range_active);
            }
            self.addForeignKeyRange("orders_customer_email_fkey", 7, 8, 9003, "", null, metadata_table_manager.foreign_key_ref_range_active);
            self.addForeignKeyRange("orders_nullable_customer_fkey", 7, 8, 9004, "", null, if (phase == .action_ref_merging) metadata_table_manager.foreign_key_ref_range_merging else metadata_table_manager.foreign_key_ref_range_active);
            self.addForeignKeyRange("lines_order_fkey", 9, 7, 9005, "", null, metadata_table_manager.foreign_key_ref_range_active);

            self.unique_range_count = 0;
            self.addUniqueRange(8, relational_store.primary_key_constraint_name, 9111, "", null, metadata_table_manager.unique_constraint_range_active);
            self.addUniqueRange(8, "customers_tenant_email_key", 9101, "", null, if (phase == .unique_splitting) metadata_table_manager.unique_constraint_range_splitting else metadata_table_manager.unique_constraint_range_active);
            if (phase == .unique_split_active) {
                self.unique_ranges[1].end_encoded_value = "\xff";
                self.addUniqueRange(8, "customers_tenant_email_key", 9102, "\xff", null, metadata_table_manager.unique_constraint_range_active);
            }
            self.addUniqueRange(7, relational_store.primary_key_constraint_name, 9201, "", null, metadata_table_manager.unique_constraint_range_active);
            self.addUniqueRange(9, relational_store.primary_key_constraint_name, 9301, "", null, metadata_table_manager.unique_constraint_range_active);
        }

        fn addForeignKeyRange(
            self: *@This(),
            constraint_name: []const u8,
            child_table_id: u64,
            parent_table_id: u64,
            group_id: u64,
            start_parent_key: []const u8,
            end_parent_key: ?[]const u8,
            state: []const u8,
        ) void {
            self.fk_ranges[self.fk_range_count] = .{
                .child_table_id = child_table_id,
                .constraint_name = constraint_name,
                .parent_table_id = parent_table_id,
                .start_parent_key = start_parent_key,
                .end_parent_key = end_parent_key,
                .group_id = group_id,
                .topology_epoch = group_id,
                .state = state,
            };
            self.fk_range_count += 1;
        }

        fn addUniqueRange(
            self: *@This(),
            table_id: u64,
            constraint_name: []const u8,
            group_id: u64,
            start_encoded_value: []const u8,
            end_encoded_value: ?[]const u8,
            state: []const u8,
        ) void {
            self.unique_ranges[self.unique_range_count] = .{
                .table_id = table_id,
                .constraint_name = constraint_name,
                .start_encoded_value = start_encoded_value,
                .end_encoded_value = end_encoded_value,
                .group_id = group_id,
                .topology_epoch = group_id,
                .state = state,
            };
            self.unique_range_count += 1;
        }

        fn adminSnapshot(ptr: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .status = .{ .metadata_group_id = 1, .metrics = .{} },
                .tables = self.tables[0..],
                .ranges = self.ranges[0..],
                .foreign_key_ref_ranges = self.fk_ranges[0..self.fk_range_count],
                .unique_constraint_ranges = self.unique_ranges[0..self.unique_range_count],
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };

    const Recorder = struct {
        begin_calls: usize = 0,
        prepare_calls: usize = 0,
        resolve_calls: usize = 0,
        lookup_calls: usize = 0,
        page_calls: usize = 0,
        parent_checks: usize = 0,
        externalized_checks: usize = 0,
        ref_writes: usize = 0,
        ref_deletes: usize = 0,
        unique_writes: usize = 0,
        action_schedules: usize = 0,
        set_null_children: usize = 0,
        cascade_children: usize = 0,
        expected_order_z_group: u64 = 7002,

        fn reset(self: *@This()) void {
            self.* = .{};
        }

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .lookup_group = lookup,
                    .foreign_key_ref_children_page_group = foreignKeyRefChildrenPageGroup,
                },
            };
        }

        fn begin(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(req.participants.len > 0);
            self.begin_calls += 1;
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, _: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.prepare_calls += 1;
            self.parent_checks += req.req.foreign_key_parent_checks.len;
            self.externalized_checks += req.req.foreign_key_externalized_parent_checks.len;
            self.ref_writes += req.req.foreign_key_ref_writes.len;
            self.ref_deletes += req.req.foreign_key_ref_deletes.len;
            self.unique_writes += req.req.unique_constraint_writes.len;
            self.action_schedules += req.req.foreign_key_action_schedules.len;
            self.set_null_children += req.req.foreign_key_set_null_children.len;
            self.cascade_children += req.req.foreign_key_cascade_children.len;
            if (req.req.foreign_key_ref_writes.len != 0 or req.req.foreign_key_ref_deletes.len != 0 or req.req.foreign_key_action_schedules.len != 0) {
                try std.testing.expect(group_id == 9001 or group_id == 9002 or group_id == 9003 or group_id == 9004 or group_id == 9005);
            }
            if (req.req.unique_constraint_writes.len != 0 or req.req.unique_constraint_deletes.len != 0) {
                try std.testing.expect(group_id == 9101 or group_id == 9102 or group_id == 9111 or group_id == 9201 or group_id == 9301);
            }
            if (req.req.foreign_key_set_null_children.len != 0) {
                try std.testing.expectEqual(self.expected_order_z_group, group_id);
            }
        }

        fn resolve(ptr: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnResolveRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            self.resolve_calls += 1;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn lookup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8) !?table_reads.LookupResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lookup_calls += 1;
            try std.testing.expectEqual(self.expected_order_z_group, group_id);
            try std.testing.expectEqualStrings("orders", table_name);
            try std.testing.expectEqualStrings("order:z", key);
            return .{
                .json = try alloc.dupe(u8, "{\"tenant_id\":\"t1\",\"order_id\":\"order:z\",\"customer_id\":\"customer:a\",\"customer_email\":\"ada@example.test\",\"nullable_customer_id\":\"customer:b\",\"status\":\"open\"}"),
                .version = 88,
            };
        }

        fn foreignKeyRefChildrenPageGroup(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) !db_mod.types.ForeignKeyRefChildrenPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.page_calls += 1;
            if (group_id == 9004) {
                try std.testing.expectEqualStrings("orders", table_name);
                try std.testing.expectEqualStrings("orders_nullable_customer_fkey", req.constraint_name);
                const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
                children[0] = .{ .child_table = try alloc.dupe(u8, "row"), .child_key = try alloc.dupe(u8, "order:z") };
                return .{ .children = children, .complete = true };
            }
            try std.testing.expectEqual(@as(u64, 9005), group_id);
            try std.testing.expectEqualStrings("line_items", table_name);
            try std.testing.expectEqualStrings("lines_order_fkey", req.constraint_name);
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 1);
            children[0] = .{ .child_table = try alloc.dupe(u8, "row"), .child_key = try alloc.dupe(u8, "line:z") };
            return .{ .children = children, .complete = true };
        }
    };

    var catalog = Catalog{};
    catalog.configure(.active_single);
    var recorder = Recorder{};
    const write_order = TableCommitRequest{
        .table_name = "orders",
        .writes = &.{.{ .key = "order:a", .value = "{\"tenant_id\":\"t1\",\"order_id\":\"order:a\",\"customer_id\":\"customer:a\",\"customer_email\":\"ada@example.test\"}" }},
        .predicates = &.{.{ .key = "order:a", .expected_version = 0 }},
    };

    const active_write = try executeMultiTableCommit(std.testing.allocator, catalog.iface(), recorder.worker(), try parseTxnIdHex("11111111222222223333333344444441"), 10_000, 10_001, &.{write_order}, null);
    try std.testing.expect(active_write == .committed);
    try std.testing.expect(recorder.parent_checks >= 2);
    try std.testing.expect(recorder.externalized_checks >= 2);
    try std.testing.expect(recorder.ref_writes >= 2);
    try std.testing.expect(recorder.unique_writes >= 1);

    catalog.configure(.fk_ref_splitting);
    recorder.reset();
    try std.testing.expectError(error.UnknownGroup, executeMultiTableCommit(std.testing.allocator, catalog.iface(), recorder.worker(), try parseTxnIdHex("11111111222222223333333344444442"), 10_010, 10_011, &.{write_order}, null));
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);

    catalog.configure(.fk_ref_split_active);
    recorder.reset();
    const split_write = try executeMultiTableCommit(std.testing.allocator, catalog.iface(), recorder.worker(), try parseTxnIdHex("11111111222222223333333344444443"), 10_020, 10_021, &.{write_order}, null);
    try std.testing.expect(split_write == .committed);
    try std.testing.expect(recorder.ref_writes >= 2);

    catalog.configure(.unique_splitting);
    recorder.reset();
    try std.testing.expectError(error.UnknownGroup, executeMultiTableCommit(std.testing.allocator, catalog.iface(), recorder.worker(), try parseTxnIdHex("11111111222222223333333344444444"), 10_030, 10_031, &.{write_order}, null));
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);

    catalog.configure(.unique_split_active);
    recorder.reset();
    const unique_split_write = try executeMultiTableCommit(std.testing.allocator, catalog.iface(), recorder.worker(), try parseTxnIdHex("11111111222222223333333344444445"), 10_040, 10_041, &.{write_order}, null);
    try std.testing.expect(unique_split_write == .committed);
    try std.testing.expect(recorder.parent_checks >= 2);
    try std.testing.expect(recorder.ref_writes >= 2);

    catalog.configure(.action_ref_merging);
    recorder.reset();
    try std.testing.expectError(error.TopologyChanged, executeForeignKeyActionPage(std.testing.allocator, catalog.iface(), recorder.worker(), try parseTxnIdHex("11111111222222223333333344444446"), 10_050, 10_051, "orders", 9004, "set_null", "orders_nullable_customer_fkey", "customers", "customer:b", null, null, null, 4, 0, foreign_key_action_default_cascade_max_depth, null));
    try std.testing.expectEqual(@as(usize, 0), recorder.page_calls);
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);

    catalog.configure(.child_owner_moved);
    recorder.reset();
    recorder.expected_order_z_group = 7003;
    var set_null_execution = try executeForeignKeyActionPage(std.testing.allocator, catalog.iface(), recorder.worker(), try parseTxnIdHex("11111111222222223333333344444447"), 10_060, 10_061, "orders", 9004, "set_null", "orders_nullable_customer_fkey", "customers", "customer:b", null, null, null, 4, 0, foreign_key_action_default_cascade_max_depth, null);
    defer set_null_execution.deinit(std.testing.allocator);
    try std.testing.expect(set_null_execution.complete);
    try std.testing.expectEqual(@as(usize, 1), set_null_execution.applied_children);
    try std.testing.expect(recorder.lookup_calls >= 1);
    try std.testing.expect(recorder.set_null_children >= 1);
    try std.testing.expect(recorder.ref_deletes >= 1);

    recorder.reset();
    var cascade_execution = try executeForeignKeyActionPage(std.testing.allocator, catalog.iface(), recorder.worker(), try parseTxnIdHex("11111111222222223333333344444448"), 10_070, 10_071, "line_items", 9005, "cascade", "lines_order_fkey", "orders", "order:z", null, null, null, 4, 0, foreign_key_action_default_cascade_max_depth, null);
    defer cascade_execution.deinit(std.testing.allocator);
    try std.testing.expect(cascade_execution.complete);
    try std.testing.expectEqual(@as(usize, 1), cascade_execution.applied_children);
    try std.testing.expect(recorder.cascade_children >= 1);
    try std.testing.expect(recorder.ref_deletes >= 1);
}

test "distributed txn coordinator routes distributed foreign key cascade actions across child ranges" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"cascade"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
                })[0..]),
                .foreign_key_ref_ranges = @constCast((&[_]metadata_table_manager.ForeignKeyReferenceRangeRecord{.{
                    .child_table_id = 7,
                    .constraint_name = "orders_customer_id_fkey",
                    .parent_table_id = 8,
                    .start_parent_key = "",
                    .end_parent_key = null,
                    .group_id = 9001,
                    .topology_epoch = 42,
                }})[0..]),
                .stores = @constCast((&[_]metadata_table_manager.StoreRecord{})[0..]),
                .placement_intents = @constCast((&[_]raft_reconciler.PlacementIntent{})[0..]),
                .split_transitions = @constCast((&[_]metadata_transition_state.SplitTransitionRecord{})[0..]),
                .merge_transitions = @constCast((&[_]metadata_transition_state.MergeTransitionRecord{})[0..]),
            };
        }

        fn freeAdminSnapshot(_: *anyopaque, _: *@import("../metadata/api.zig").AdminSnapshot) void {}
    };

    const Recorder = struct {
        prepared_parent_delete: bool = false,
        prepared_owner_conflict: bool = false,

        fn worker(self: *@This()) ParticipantWorker {
            return .{
                .ptr = self,
                .vtable = &.{
                    .begin_group = begin,
                    .prepare_group = prepare,
                    .resolve_group = resolve,
                    .status_group = status,
                    .foreign_key_ref_children_group = foreignKeyRefChildrenGroup,
                },
            };
        }

        fn begin(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, req: TxnBeginRequest) !void {
            try std.testing.expectEqual(@as(usize, 2), req.participants.len);
        }

        fn prepare(ptr: *anyopaque, _: std.mem.Allocator, group_id: u64, table_name: []const u8, req: TxnPrepareRequest) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (group_id == 8002) {
                try std.testing.expectEqualStrings("customers", table_name);
                try std.testing.expectEqual(@as(usize, 1), req.req.deletes.len);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.deletes[0]);
                self.prepared_parent_delete = true;
            } else if (group_id == 9001) {
                try std.testing.expectEqualStrings("docs", table_name);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_parent_delete_checks.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_conflict_checks.len);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_conflict_checks[0].constraint_name);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_conflict_checks[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_conflict_checks[0].parent_key);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_ref_deletes.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_set_null_children.len);
                try std.testing.expectEqual(@as(usize, 0), req.req.foreign_key_cascade_children.len);
                try std.testing.expectEqual(@as(usize, 1), req.req.foreign_key_action_schedules.len);
                try std.testing.expectEqualStrings("cascade", req.req.foreign_key_action_schedules[0].action);
                try std.testing.expectEqualStrings("txn-coordinator", req.req.foreign_key_action_schedules[0].worker_id);
                try std.testing.expectEqualStrings("orders_customer_id_fkey", req.req.foreign_key_action_schedules[0].constraint_name);
                try std.testing.expectEqualStrings("customers", req.req.foreign_key_action_schedules[0].parent_table);
                try std.testing.expectEqualStrings("cust:z-customer", req.req.foreign_key_action_schedules[0].parent_key);
                try std.testing.expectEqual(@as(usize, 1024), req.req.foreign_key_action_schedules[0].page_limit);
                self.prepared_owner_conflict = true;
            } else return error.UnexpectedGroup;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {}

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return .pending;
        }

        fn foreignKeyRefChildrenGroup(
            _: *anyopaque,
            alloc: std.mem.Allocator,
            group_id: u64,
            table_name: []const u8,
            req: ForeignKeyRefChildrenRequest,
        ) ![]db_mod.types.ForeignKeyRefChild {
            try std.testing.expectEqual(@as(u64, 9001), group_id);
            try std.testing.expectEqualStrings("docs", table_name);
            try std.testing.expectEqualStrings("orders_customer_id_fkey", req.constraint_name);
            try std.testing.expectEqualStrings("customers", req.parent_table);
            try std.testing.expectEqualStrings("cust:z-customer", req.parent_key);
            const children = try alloc.alloc(db_mod.types.ForeignKeyRefChild, 2);
            errdefer alloc.free(children);
            children[0] = .{
                .child_table = try alloc.dupe(u8, "row"),
                .child_key = try alloc.dupe(u8, "doc:a-order"),
            };
            errdefer {
                alloc.free(@constCast(children[0].child_table));
                alloc.free(@constCast(children[0].child_key));
            }
            children[1] = .{
                .child_table = try alloc.dupe(u8, "row"),
                .child_key = try alloc.dupe(u8, "doc:z-order"),
            };
            return children;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("bbbbccccddddeeeeffff000011112224");
    const result = try executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"cust:z-customer"},
        }},
        null,
    );
    try std.testing.expectEqual(@as(usize, 2), result.committed.participant_count);
    try std.testing.expect(recorder.prepared_parent_delete);
    try std.testing.expect(recorder.prepared_owner_conflict);
}

test "distributed txn coordinator rejects distributed foreign key cascade actions without ref owner topology" {
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"customer_id":{"type":"keyword"}},"additionalProperties":false}}},"foreign_keys":[{"name":"orders_customer_id_fkey","columns":["customer_id"],"references":{"table":"customers","columns":["_id"]},"on_delete":"cascade"}]}
    ;
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
                .tables = @constCast((&[_]metadata_table_manager.TableRecord{
                    .{ .table_id = 7, .name = "docs", .schema_json = schema_json, .placement_role = "data" },
                    .{ .table_id = 8, .name = "customers", .placement_role = "data" },
                })[0..]),
                .ranges = @constCast((&[_]metadata_table_manager.RangeRecord{
                    .{ .group_id = 7001, .table_id = 7, .start_key = "", .end_key = "doc:m" },
                    .{ .group_id = 7002, .table_id = 7, .start_key = "doc:m", .end_key = null },
                    .{ .group_id = 8001, .table_id = 8, .start_key = "", .end_key = "cust:m" },
                    .{ .group_id = 8002, .table_id = 8, .start_key = "cust:m", .end_key = null },
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
            return error.UnexpectedWorkerCall;
        }

        fn prepare(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnPrepareRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn resolve(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: TxnResolveRequest) !void {
            return error.UnexpectedWorkerCall;
        }

        fn status(_: *anyopaque, _: std.mem.Allocator, _: u64, _: []const u8, _: db_mod.types.TxnId) !db_mod.types.TxnStatus {
            return error.UnexpectedWorkerCall;
        }
    };

    var recorder = Recorder{};
    const txn_id = try parseTxnIdHex("bbbbccccddddeeeeffff000011112223");
    try std.testing.expectError(error.UnsupportedOperation, executeMultiTableCommit(
        std.testing.allocator,
        FakeCatalog.iface(),
        recorder.worker(),
        txn_id,
        10_000,
        10_001,
        &.{.{
            .table_name = "customers",
            .deletes = &.{"cust:z-customer"},
        }},
        null,
    ));
    try std.testing.expectEqual(@as(usize, 0), recorder.begin_calls);
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

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(5 * std.time.ns_per_min);

    var recorder = Recorder{};
    var resolver = RecoveryResolver{
        .alloc = alloc,
        .worker = recorder.worker(),
        .lease_owned = true,
        .interval_ms = 250,
        .clock = manual_clock.clock(),
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
    manual_clock.setRealtimeNs(5 * std.time.ns_per_min + 10_000);

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

test "db transaction recovery resolves committed mutation-source participant after reopen" {
    const alloc = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/distributed-mutation-source-participant-reopen", .{tmp.sub_path});
    defer alloc.free(path);

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    var parsed_schema = try schema_mod.parseValidatedTableSchema(alloc, schema_json);
    defer parsed_schema.deinit(alloc);
    const runtime_schema = try schema_mod.deriveRuntimeTableSchema(alloc, parsed_schema);
    defer storage_schema.freeSchema(alloc, runtime_schema);

    const participant = try participantIdForGroup(alloc, "docs", 77);
    defer alloc.free(participant);
    const txn_id = blk: {
        var db = try db_mod.DB.open(alloc, path, .{});
        defer db.close();
        try db.setSchema(runtime_schema);

        try db.batch(.{
            .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"a\",\"status\":\"ready\"}" }},
            .timestamp_ns = 1_000,
        });

        const staged_txn = try db.beginTransactionWithParticipants(2_000, &.{participant});
        const predicates = [_]storage_schema.RelationalCheck{.{
            .name = "",
            .field = "status",
            .op = .eq,
            .value_json = "\"ready\"",
        }};
        const operations = [_]db_mod.types.TransformOp{.{
            .op = .set,
            .path = "status",
            .value_json = "\"done\"",
        }};
        var staged = try db.mutateRelationalRowsFromSource(alloc, runtime_schema, .{
            .kind = .update,
            .source = .{
                .predicates = predicates[0..],
                .row_claim = .{
                    .mode = .for_update,
                    .owner_id = "session:reopen-participant",
                    .lease_ms = 60_000,
                    .txn_id = staged_txn,
                },
            },
            .operations = operations[0..],
        });
        defer staged.deinit(alloc);
        try std.testing.expectEqual(@as(u32, 1), staged.matched);
        try std.testing.expectEqual(@as(u32, 1), staged.staged);

        try db.resolveTransactionIntents(staged_txn, .committed, 2_100);
        break :blk staged_txn;
    };

    const Recorder = struct {
        calls: usize = 0,
        last_group_id: u64 = 0,
        last_status: ?db_mod.types.TxnStatus = null,
        last_commit_version: u64 = 0,

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
            try std.testing.expectEqual(db_mod.types.TxnStatus.committed, req.status);
            self.calls += 1;
            self.last_group_id = group_id;
            self.last_status = req.status;
            self.last_commit_version = req.commit_version;
        }
    };

    var recorder = Recorder{};
    var resolver = RecoveryResolver{
        .alloc = alloc,
        .worker = recorder.worker(),
        .lease_owned = true,
    };
    var reopened = try db_mod.DB.open(alloc, path, .{});
    defer reopened.close();
    try reopened.setSchema(runtime_schema);

    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, try reopened.getTransactionStatus(txn_id));
    const unresolved_before = try reopened.getUnresolvedTransactionParticipants(alloc, txn_id);
    defer transactions_mod.freeParticipantList(alloc, unresolved_before);
    try std.testing.expectEqual(@as(usize, 1), unresolved_before.len);
    try std.testing.expectEqualStrings(participant, unresolved_before[0]);

    var visible_before = (try reopened.lookup(alloc, "row:a", .{})) orelse return error.TestUnexpectedResult;
    defer visible_before.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, visible_before.json, "\"status\":\"done\"") != null);

    const stats = try reopened.runTransactionRecoveryOnce(resolver.config());
    try std.testing.expect(stats.notification_attempts > 0);
    try std.testing.expect(stats.notification_successes > 0);
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
    try std.testing.expectEqual(@as(u64, 77), recorder.last_group_id);
    try std.testing.expectEqual(db_mod.types.TxnStatus.committed, recorder.last_status.?);
    try std.testing.expectEqual(@as(u64, 2_100), recorder.last_commit_version);
    try std.testing.expectError(transactions_mod.TxnError.TxnNotFound, reopened.getTransactionStatus(txn_id));

    var final_row = (try reopened.lookup(alloc, "row:a", .{})) orelse return error.TestUnexpectedResult;
    defer final_row.deinit(alloc);
    try std.testing.expect(std.mem.indexOf(u8, final_row.json, "\"status\":\"done\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, final_row.json, "\"status\":\"lost\"") == null);

    const again = try reopened.runTransactionRecoveryOnce(resolver.config());
    try std.testing.expectEqual(@as(u64, 0), again.notification_attempts);
    try std.testing.expectEqual(@as(usize, 1), recorder.calls);
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

    var manual_clock = platform_clock.ManualClock{};
    manual_clock.setRealtimeNs(5 * std.time.ns_per_min);

    var recorder = Recorder{};
    var resolver = RecoveryResolver{
        .alloc = alloc,
        .worker = recorder.worker(),
        .lease_owned = true,
        .clock = manual_clock.clock(),
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
