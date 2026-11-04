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

//! Notifications are owned SQL intents, never a request-side delivery effect.
const std = @import("std");
const local = @import("antfly_local_sources");
const execution = @import("sql_execution.zig");
const producer = @import("sql_command_producer.zig");
const authority = @import("../system_catalog/lock_authority.zig");
/// A native PID lease is shared across API nodes. Wire backends reserve their
/// advertised identity at startup; HTTP identities can retry collisions before
/// any user notification intent has been staged.
pub fn bindIdentity(root: *execution.Adapter, a: std.mem.Allocator) !?i32 {
    try root.context.ensureActive();
    const frame = try root.lockingFrame();
    const now = @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
    try frame.requireExecuting(a, now);
    var native: producer.NativePort = .{ .server = root.server, .context = root.context };
    const port = native.port();
    const bytes = try port.call(port.ptr, a, .{ .sql_lock_read = .{ .incarnation = frame.fence.backend.incarnation, .standalone = frame.fence.standalone, .actor = frame.actor, .database = frame.fence.database, .backend = frame.fence.backend } });
    defer a.free(bytes);
    var cut = try std.json.parseFromSlice(authority.Snapshot, a, bytes, .{});
    defer cut.deinit();
    if (!cut.value.notification_capable) return null;
    if (cut.value.group != frame.fence.group or !std.mem.eql(u8, &cut.value.incarnation, &frame.fence.backend.incarnation) or cut.value.standalone != frame.fence.standalone) return error.CatalogGenerationChanged;
    var connection = try @import("sql_connections.zig").loadWithCommand(frame.source, a, frame.connection, frame.principal, root.server.localSessionNodeId(), now, root.context.sql_command_fence);
    defer connection.deinit();
    var pid: ?i32 = null;
    if (cut.value.notification_binding) |binding| {
        if (!binding.backend.eql(frame.fence.backend) or !std.mem.eql(u8, &binding.connection, &frame.connection) or binding.generation != connection.value.generation) return error.CatalogGenerationChanged;
        pid = binding.sender_pid;
    } else {
        for (0..8) |_| {
            var nonce: [16]u8 = undefined;
            try root.server.embedding_provider_runtime.io.randomSecure(&nonce);
            const candidate = connection.value.notification_pid orelse if (root.backend_pid > 0) root.backend_pid else @as(i32, @intCast(1 + std.mem.readInt(u32, nonce[0..4], .little) % std.math.maxInt(i32)));
            _ = port.submit(a, .{ .admission = .{ .head = cut.value.head, .nonce = nonce }, .incarnation = frame.fence.backend.incarnation, .standalone = frame.fence.standalone, .actor = frame.actor, .database = frame.fence.database, .backend = frame.fence.backend, .operation = .{ .notification_bind = .{ .command = frame.fence.command, .connection = frame.connection, .generation = connection.value.generation, .sender_pid = candidate } } }) catch |err| {
                if (err == error.SqlBackendPidInUse and root.backend_pid == 0 and connection.value.notification_pid == null) continue;
                return err;
            };
            pid = candidate;
            break;
        }
    }
    const accepted = pid orelse return error.SqlBackendPidInUse;
    if (root.backend_pid > 0 and accepted != root.backend_pid) return error.SqlBackendPidInUse;
    _ = try @import("sql_connections.zig").mutateWithCommand(frame.source, .{ .notification_identity = .{ .id = frame.connection, .generation = connection.value.generation, .sender_pid = accepted } }, frame.principal, root.server.localSessionNodeId(), now, root.context.sql_command_fence);
    return accepted;
}
pub fn stage(raw: *anyopaque, a: std.mem.Allocator, command: local.sql_ast.notifications.Command) anyerror!void {
    const root: *execution.Adapter = @ptrCast(@alignCast(raw));
    if (try bindIdentity(root, a) == null) return error.TableTopologyProtocolUpgradeRequired;
    const frame = try root.lockingFrame();
    if (command == .unlisten or command == .unlisten_all) {
        var connection = try @import("sql_connections.zig").loadWithCommand(frame.source, a, frame.connection, frame.principal, root.server.localSessionNodeId(), @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms, root.context.sql_command_fence);
        defer connection.deinit();
        if (!connection.value.notification_listen_seen) {
            var has_registration = false;
            if (root.effectTransaction()) |active| {
                var state = (try root.server.txn_sessions.getSqlState(a, active)) orelse return error.SqlTransactionNotActive;
                defer state.deinit(a);
                for (state.metadata.notifications.commands.items) |pending| if (pending != .notify) {
                    has_registration = true;
                    break;
                };
            }
            if (!has_registration) return;
        }
    }
    const id = try root.lockingReadTransaction();
    try root.server.txn_sessions.stageSqlNotification(root.server.alloc, id, frame.principal, command);
    if (command == .listen) if (root.notification_interest) |interest| {
        interest.* = true;
    };
}
