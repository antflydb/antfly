// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Durable HTTP SQL setting commands. One attached session owns each overlay;
//! the current scoped catalog remains the authority for every read and write.
const std = @import("std");
const execution = @import("sql_execution.zig");
const http = @import("http_server.zig");
const settings = @import("../sql/setting_catalog.zig");
const runtime = @import("../sql/runtime.zig");
const commands = @import("../pgwire/session_commands.zig");
const distributed = @import("distributed_txn.zig");
const connections = @import("sql_connections.zig");
const platform_time = @import("antfly_platform").time;

pub const Command = union(enum) { catalog: commands.CatalogSetting, reset_all, discard_all };

pub fn execute(alloc: std.mem.Allocator, adapter: *execution.Adapter, command: Command, parameters: []const std.json.Value) !runtime.Result {
    if (parameters.len != 0) return error.InvalidSqlParameters;
    if (command == .discard_all and adapter.session_id != null) return error.ActiveSqlTransaction;
    if (adapter.connection_id != null and adapter.session_id == null)
        return executeConnection(alloc, adapter, command);
    const session_alloc = adapter.server.alloc;
    const encoded = adapter.session_id orelse return error.SqlTransactionNotActive;
    const id = distributed.parseTxnIdHex(encoded) catch return error.SqlTransactionNotActive;
    const principal = http.transactionPrincipal(adapter.identity.*);
    if (try adapter.server.txn_sessions.principalAccess(session_alloc, id, principal) != .allowed) return error.SqlTransactionNotActive;
    const lease = adapter.server.txn_sessions.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
    defer lease.release();
    var state = (try adapter.server.txn_sessions.getSqlState(session_alloc, id)) orelse return error.SqlTransactionNotActive;
    defer state.deinit(session_alloc);
    if (state.terminal_abort) return error.SqlTransactionAborted;
    if (!std.meta.eql(state.connection_id, adapter.connection_id)) return error.SqlConnectionNotFound;
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
    if (command == .discard_all) return error.ActiveSqlTransaction;
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

fn executeConnection(alloc: std.mem.Allocator, adapter: *execution.Adapter, command: Command) !runtime.Result {
    const id = adapter.connection_id.?;
    const expected_revision = adapter.connection_revision orelse return error.SqlConnectionChanged;
    const store = adapter.server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
    const principal = http.transactionPrincipal(adapter.identity.*) orelse "";
    const owner_node_id = adapter.server.localSessionNodeId();
    const now_ms = platform_time.realtimeNs() / std.time.ns_per_ms;
    if (command == .discard_all) {
        _ = try connections.mutate(store, .{ .discard = id }, principal, owner_node_id, now_ms);
        return runtime.Result.empty(alloc, "DISCARD ALL");
    }
    if (command == .reset_all) {
        _ = try connections.mutate(store, .{ .overlay = .{ .id = id, .expected_revision = expected_revision, .entries = &.{} } }, principal, owner_node_id, now_ms);
        return runtime.Result.empty(alloc, "RESET");
    }
    const setting = command.catalog;
    if (setting == .reset_local or (setting == .set and setting.set.local)) return error.NoActiveSqlTransaction;
    const capture = adapter.settingCapture().?;
    var view = try settings.View.capture(alloc, capture.owner, capture.scope, capture.overlay);
    defer view.deinit();
    if (setting == .show) {
        const name = setting.show;
        const resolved = try view.resolve(name);
        var numeric_buffer: [32]u8 = undefined;
        const rendered = switch (resolved.value) {
            .string => |value| value,
            .integer => |value| try std.fmt.bufPrint(&numeric_buffer, "{d}", .{value}),
            .boolean => |value| if (value) "on" else "off",
        };
        return runtime.Result.singleText(alloc, "SHOW", name, rendered);
    }
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const scratch = arena.allocator();
    const old = adapter.setting_overlay;
    if (old.len >= connections.max_overlay_entries and setting == .set) return error.SettingLimitExceeded;
    const entries = try scratch.alloc(settings.OverlayEntry, old.len + 1);
    var count: usize = 0;
    var identity: settings.Identity = undefined;
    var replacement: ?settings.OverlayEntry = null;
    switch (setting) {
        .set => |value| {
            const definition = try view.writable(value.name);
            identity = definition.identity;
            replacement = .{ .identity = identity, .value = try settings.parseValue(definition.kind, value.value) };
        },
        .reset => |name| identity = (try view.writable(name)).identity,
        .show, .reset_local => unreachable,
    }
    var replaced = false;
    for (old) |entry| {
        if (entry.identity.id == identity.id) {
            if (replacement) |next| {
                entries[count] = next;
                count += 1;
                replaced = true;
            }
        } else {
            entries[count] = entry;
            count += 1;
        }
    }
    if (replacement) |next| if (!replaced) {
        entries[count] = next;
        count += 1;
    };
    _ = try connections.mutate(store, .{ .overlay = .{ .id = id, .expected_revision = expected_revision, .entries = entries[0..count] } }, principal, owner_node_id, now_ms);
    return runtime.Result.empty(alloc, if (setting == .set) "SET" else "RESET");
}
