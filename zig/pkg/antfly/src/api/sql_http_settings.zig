// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Durable HTTP SQL setting commands. One attached session owns each overlay;
//! the current scoped catalog remains the authority for every read and write.
const std = @import("std");
const execution = @import("sql_execution.zig");
const http = @import("http_server.zig");
const settings = @import("../sql/setting_catalog.zig");
const runtime = @import("../sql/runtime.zig");
const commands = @import("../pgwire/session_commands.zig");
const distributed = @import("distributed_txn.zig");

pub const Command = union(enum) { catalog: commands.CatalogSetting, reset_all };

pub fn execute(alloc: std.mem.Allocator, adapter: *execution.Adapter, command: Command, parameters: []const std.json.Value) !runtime.Result {
    if (parameters.len != 0) return error.InvalidSqlParameters;
    const session_alloc = adapter.server.alloc;
    const encoded = adapter.session_id orelse return error.SqlTransactionNotActive;
    const id = distributed.parseTxnIdHex(encoded) catch return error.SqlTransactionNotActive;
    const principal = http.transactionPrincipal(adapter.identity.*);
    if (try adapter.server.txn_sessions.principalAccess(session_alloc, id, principal) != .allowed) return error.SqlTransactionNotActive;
    const lease = adapter.server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
    defer lease.release();
    var state = (try adapter.server.txn_sessions.getSqlState(session_alloc, id)) orelse return error.SqlTransactionNotActive;
    defer state.deinit(session_alloc);
    if (state.metadata.failed) return error.SqlTransactionAborted;
    if (state.owner_node_id != adapter.server.localSessionNodeId()) return error.SessionLeaseLost;
    try adapter.server.txn_sessions.validateSqlLease(session_alloc, id, state.owner_node_id);
    if ((!adapter.inherit_session_database and !std.mem.eql(u8, adapter.database, state.metadata.database)) or
        (!adapter.inherit_session_namespace and !std.mem.eql(u8, adapter.namespace, state.metadata.namespace))) return error.SqlTransactionNotActive;
    const previous_database = adapter.database;
    const previous_namespace = adapter.namespace;
    const previous_overlay = adapter.setting_overlay;
    defer {
        adapter.database = previous_database;
        adapter.namespace = previous_namespace;
        adapter.setting_overlay = previous_overlay;
    }
    adapter.database = state.metadata.database;
    adapter.namespace = state.metadata.namespace;
    adapter.setting_overlay = state.setting_active.items;
    adapter.transaction_status = .in_transaction;
    adapter.result_session_id = std.fmt.bytesToHex(id, .lower);
    if (command == .reset_all) {
        try adapter.server.txn_sessions.resetAllSqlSettings(session_alloc, id, principal);
        return runtime.Result.empty(alloc, "RESET");
    }
    const capture = adapter.settingCapture().?;
    return switch (command.catalog) {
        .set => |value| blk: {
            // Parse against the same authoritative catalog capture used for
            // the durable mutation; a preliminary read doubles remote work
            // and can race a definition change before publication.
            try adapter.server.txn_sessions.setSqlSettingRaw(session_alloc, id, principal, capture.owner, value.name, value.value, value.local);
            break :blk runtime.Result.empty(alloc, "SET");
        },
        .reset => |name| blk: {
            try adapter.server.txn_sessions.resetSqlSetting(session_alloc, id, principal, capture.owner, name);
            break :blk runtime.Result.empty(alloc, "RESET");
        },
        .reset_local => |name| blk: {
            try adapter.server.txn_sessions.resetLocalSqlSetting(session_alloc, id, principal, capture.owner, name);
            break :blk runtime.Result.empty(alloc, "SET");
        },
        .show => |name| blk: {
            var view = try settings.View.capture(session_alloc, capture.owner, capture.scope, capture.overlay);
            defer view.deinit();
            const resolved = try view.resolve(name);
            var numeric_buffer: [32]u8 = undefined;
            const rendered = switch (resolved.value) {
                .string => |value| value,
                .integer => |value| try std.fmt.bufPrint(&numeric_buffer, "{d}", .{value}),
                .boolean => |value| if (value) "on" else "off",
            };
            break :blk runtime.Result.singleText(alloc, "SHOW", name, rendered);
        },
    };
}
