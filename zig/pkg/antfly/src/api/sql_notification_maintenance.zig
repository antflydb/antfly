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

//! Leader-owned bounded reclamation. No SQL or connection replay is involved.
//! The catalog work index survives sender disconnect and process replacement.
const std = @import("std");
const Server = @import("http_server.zig").ApiHttpServer;
const locks = @import("../system_catalog/lock_authority.zig");
const journal = @import("../system_catalog/sql_notifications.zig");
const A = std.mem.Allocator;
pub const Cursor = struct {
    after: ?[32]u8 = null,
    productive: bool = false,
    pub fn accept(self: *Cursor, work: journal.Work) bool {
        self.after = work.next_after;
        if (work.scope != null) {
            self.productive = true;
            return true;
        }
        if (work.next_after != null) return true;
        const again = self.productive;
        self.productive = false;
        return again;
    }
};
pub fn owns(server: *Server) bool {
    if (server.cfg.prepared_transaction_leadership) |leadership| return leadership.isLocalLeader(server.cfg.prepared_transaction_group_id);
    return server.cfg.deployment_mode == .standalone;
}
pub fn advance(server: *Server, cursor: *Cursor) !bool {
    if (!owns(server)) return false;
    var arena = std.heap.ArenaAllocator.init(server.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const context: @import("antfly_local_sources").api_operation.RequestContext = .{ .setting_admin = true, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| 250 * std.time.ns_per_ms };
    const actor: locks.prepared.Actor = .{ .session_user = .bootstrap, .credential = "" };
    const bytes = try server.source.systemCatalog(a, context, .{ .sql_lock_read = .{ .incarnation = @splat(0), .standalone = server.cfg.deployment_mode == .standalone, .actor = actor, .database = 0, .notification_work = .{ .after = cursor.after } } });
    const cut = try std.json.parseFromSliceLeaky(locks.Snapshot, a, bytes, .{});
    if (cut.group != server.cfg.prepared_transaction_group_id) return error.CatalogGenerationChanged;
    if (!cut.notification_capable) return false;
    const work = cut.notification_work orelse return error.InvalidSqlBackendResponse;
    const scope = work.scope orelse return cursor.accept(work);
    if (scope.group != cut.group or !std.mem.eql(u8, &scope.incarnation, &cut.incarnation)) return error.InvalidSqlBackendResponse;
    if (!owns(server)) return false;
    var nonce: [16]u8 = undefined;
    try server.embedding_provider_runtime.io.randomSecure(&nonce);
    const request: locks.Request = .{ .admission = .{ .head = cut.head, .nonce = nonce }, .incarnation = cut.incarnation, .standalone = cut.standalone, .actor = actor, .database = scope.database, .backend = .{ .incarnation = cut.incarnation, .id = @splat(1), .fence = 1 }, .operation = .{ .notification_gc = .{ .scope = scope, .steps = 4 } } };
    const encoded = try server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request });
    const outcome = try std.json.parseFromSliceLeaky(locks.outcomes.Outcome, a, encoded, .{});
    _ = try outcome.result();
    return cursor.accept(work);
}

test "SQL notifications maintenance cursors keep productive rounds moving and sleep only after a fully idle round" {
    var cursor: Cursor = .{};
    const scope: journal.Scope = .{ .group = 1, .database = 1, .incarnation = @splat(1) };
    try std.testing.expect(cursor.accept(.{ .scope = scope, .next_after = @splat(2) }));
    try std.testing.expect(cursor.accept(.{}));
    try std.testing.expect(cursor.after == null);
    try std.testing.expect(!cursor.accept(.{}));
    // Multiple pages of pinned scopes make bounded progress, then sleep.
    try std.testing.expect(cursor.accept(.{ .next_after = @splat(3) }));
    try std.testing.expect(cursor.accept(.{ .next_after = @splat(4) }));
    try std.testing.expect(!cursor.accept(.{}));
}
