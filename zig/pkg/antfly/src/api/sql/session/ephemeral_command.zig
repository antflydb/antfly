// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Stateless document version requests borrow connected command authority.
//! Recovery may finish control delivery, never execute the body again.
const std = @import("std");
const local = @import("antfly_local_sources");
const execution = @import("../execution/execution.zig");
const commands = @import("command_execution.zig");
const connections = @import("connections.zig");
const http = @import("../../http_server.zig");

pub fn needed(root: *const execution.Adapter, statement: local.sql_ast.Statement) bool {
    return local.sql_ast.returningWriteVersion(statement) and eligible(root);
}

fn eligible(root: *const execution.Adapter) bool {
    return root.connection_id == null and root.session_id == null and root.active_transaction == null and root.context.sql_command_fence == null and
        root.server.source.vtable.supports_sql_command_ownership;
}

pub fn execute(root: *execution.Adapter, a: std.mem.Allocator, compiled: *const local.sql_compiler.Compiled, parameters: []const std.json.Value, limits: local.sql_runtime.Limits, backend: ?local.sql_catalog.Backend) anyerror!local.sql_runtime.Result {
    try root.context.ensureActive();
    const server = root.server;
    const store = server.txn_sessions.durable orelse return error.SqlConnectionUnavailable;
    const io = server.sharedApiIo() orelse return error.SqlConnectionUnavailable;
    const principal = http.transactionPrincipal(root.identity.*) orelse "";
    const node = server.localSessionNodeId();
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    var random: [16]u8 = undefined;
    try io.randomSecure(&random);
    const id = @import("../execution/resource_routing.zig").encode(node, random);
    const instance = try server.sql_connection_admission.executionInstance(io);
    _ = try connections.mutate(store, .{ .create = .{
        .id = id,
        .wire_epoch = instance,
        .principal = principal,
        .owner_node_id = node,
        .expires_at_ms = now +| @import("connection_record.zig").ttl_ms,
        .database = root.database,
        .namespace = root.namespace,
        .baseline = root.setting_baseline orelse &.{},
        .principal_instance = try http.sqlPrincipalInstance(server, root.identity.*),
    } }, principal, node, now);
    var native: commands.Native = .{ .server = server, .context = root.context };
    const controller = native.controller(id, root.identity.*) catch |err| {
        _ = connections.mutate(store, .{ .close = id }, principal, node, now) catch {};
        return err;
    };
    // Exact native retirement precedes deleting its connection. Cancellation or
    // uncertain decisions leave both durable owners for the existing upkeep.
    defer {
        retire(server, store, controller, id, principal, node);
    }
    var admission = try server.sql_connection_admission.acquire(server.alloc, io, .init(id, principal));
    defer admission.release(io);
    var command: commands.Request = .{};
    var command_finished = false;
    defer if (!command_finished) command.finish() catch {};
    if (!try command.start(server, root.context, root.identity.*, &admission, .{ .execution_nonce = random, .statement_kind = @tagName(compiled.statement), .parameters = parameters, .database = root.database, .namespace = root.namespace })) return error.SqlConnectionUnavailable;
    var connection = try connections.load(store, a, id, principal, node, now);
    defer connection.deinit();
    const previous_context = root.context;
    const previous_connection = root.connection_id;
    const previous_generation = root.connection_generation;
    const previous_revision = root.connection_revision;
    const previous_baseline = root.setting_baseline;
    defer {
        root.context = previous_context;
        root.connection_id = previous_connection;
        root.connection_generation = previous_generation;
        root.connection_revision = previous_revision;
        root.setting_baseline = previous_baseline;
    }
    root.context = command.attach(root.context);
    root.connection_id = id;
    root.connection_generation = connection.value.generation;
    root.connection_revision = connection.value.revision;
    root.setting_baseline = connection.value.baseline;
    var result = root.execute(a, compiled, parameters, limits, backend) catch |err| {
        _ = command.failed(root.identity.*, err) catch true;
        return err;
    };
    errdefer result.deinit();
    try command.finish();
    command_finished = true;
    return result;
}

fn retire(server: *http.ApiHttpServer, store: *@import("../../transactions.zig").DurableSessionStore, controller: commands.Controller, id: [32]u8, principal: []const u8, node: u64) void {
    var enrolled = @import("command_producer.zig").load(store, server.alloc, id) catch return;
    defer if (enrolled) |*owned| owned.deinit();
    if (enrolled != null) {
        controller.retire(server.alloc) catch return;
        controller.retireAndWait(server.alloc) catch return;
    }
    _ = connections.mutate(store, .{ .close = id }, principal, node, @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms) catch {};
}
