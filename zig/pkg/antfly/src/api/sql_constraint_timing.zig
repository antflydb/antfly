// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! SET CONSTRAINTS changes durable session validation timing, never commit
//! authority. Publishing IMMEDIATE atomically validates the pending overlay.
const std = @import("std");
const http = @import("http_server.zig");
const sessions = @import("transactions.zig");
const sql = @import("../sql/session.zig");
const integrity = @import("relational_integrity_commit.zig");
const native = @import("../storage/relational_index.zig");
const domain = @import("../system_catalog/domain.zig");
const operation = @import("operation.zig");

pub fn set(server: *http.ApiHttpServer, alloc: std.mem.Allocator, identity: ?http.AuthenticatedIdentity, context: operation.RequestContext, scope: sql.Scope, lookup_namespace: []const u8, id: sql.Id, names: []const []const u8, deferred: bool) !void {
    try context.ensureActive();
    if (names.len > 256) return error.SqlLimitExceeded;
    if (try server.txn_sessions.principalAccess(alloc, id, http.transactionPrincipal(identity)) != .allowed) return error.SqlTransactionNotActive;
    const lease = server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
    defer lease.release();
    var state = (try server.txn_sessions.getSqlState(alloc, id)) orelse return error.SqlTransactionNotActive;
    defer state.deinit(alloc);
    if (state.execution_started or state.terminal != null) return error.SqlTransactionOutcomeUnknown;
    if (state.metadata.failed) return error.SqlTransactionAborted;
    if (state.owner_node_id != server.localSessionNodeId()) return error.SessionLeaseLost;
    if (!std.mem.eql(u8, state.metadata.database, scope.database) or !std.mem.eql(u8, state.metadata.namespace, scope.namespace)) return error.SqlTransactionNotActive;
    try server.txn_sessions.validateSqlLease(alloc, id, state.owner_node_id);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const modes: []const native.ConstraintTiming = if (names.len == 0) &[_]native.ConstraintTiming{.{ .deferred = deferred }} else blk: {
        var tables: std.ArrayList(@import("../metadata/table_manager.zig").TableRecord) = .empty;
        var request: domain.TableList = .{ .database = scope.database, .namespace = lookup_namespace, .limit = 128 };
        while (true) {
            try context.ensureActive();
            const bytes = try server.source.systemCatalog(a, context, .{ .list_tables = request });
            const listing = try std.json.parseFromSliceLeaky(@import("../system_catalog/projection.zig").TableListing, a, bytes, .{ .allocate = .alloc_always });
            if (request.revision) |revision| if (revision != listing.revision) return error.CatalogGenerationChanged;
            request.revision = listing.revision;
            request.legacy_membership = listing.legacy_membership;
            for (listing.entries) |entry| {
                const target: domain.Target = .{ .database = scope.database, .namespace = lookup_namespace, .table = entry.name };
                const resource = try target.resourceNameAlloc(a);
                if (identity) |authenticated| if (!http.permissionsAllow(authenticated.permissions, .table, resource, .write)) continue;
                if (tables.items.len >= 4096) return error.SqlLimitExceeded;
                try tables.append(a, entry.table);
            }
            const next = listing.next_after orelse break;
            if (request.after) |previous| if (std.mem.eql(u8, previous, next) and request.after_table_id == listing.next_table_id) return error.InvalidIntegrityContinuation;
            request.after = next;
            request.after_table_id = listing.next_table_id;
        }
        break :blk try integrity.resolveConstraintTiming(a, server.table_reads orelse return error.IntegrityCatalogUnavailable, tables.items, names, deferred, context);
    };
    var validation = Validation{ .server = server, .identity = identity, .context = context, .modes = modes, .immediate = !deferred };
    const empty: sessions.OwnedTransactionCommitRequest = .{};
    _ = (try server.txn_sessions.stageValidated(server.alloc, id, &empty, .{ .ptr = &validation, .validate = Validation.apply })) orelse return error.SqlTransactionNotActive;
}

const Validation = struct {
    server: *http.ApiHttpServer,
    identity: ?http.AuthenticatedIdentity,
    context: operation.RequestContext,
    modes: []const native.ConstraintTiming,
    immediate: bool,

    fn apply(raw: *anyopaque, alloc: std.mem.Allocator, _: ?*const sessions.OwnedTransactionCommitRequest, candidate: *sessions.OwnedTransactionCommitRequest, _: *const sessions.OwnedTransactionCommitRequest) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        for (self.modes) |mode| try candidate.setConstraintTiming(alloc, mode);
        if (!self.immediate or candidate.tables.len == 0) return;
        if (!try self.server.transactionRequestAuthorized(self.identity, candidate.*)) return error.Forbidden;
        var snapshot = (try self.server.source.adminSnapshot()) orelse return error.IntegrityCatalogUnavailable;
        defer self.server.source.freeAdminSnapshot(&snapshot);
        const requests = try candidate.distributedTables(alloc);
        defer alloc.free(requests);
        var checked = try integrity.validateConstraintTiming(alloc, self.server.table_reads orelse return error.IntegrityCatalogUnavailable, snapshot.tables, requests, self.context, candidate.constraint_timing.items);
        defer checked.deinit();
        // Retain original row observations discovered by retroactive checks.
        try @import("relational_session_statement.zig").apply(alloc, candidate, checked.tables);
    }
};
