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

//! One leader-owned ancestor quantum. Durable root and child identities own
//! every effect; discovery never recaptures a mutable database/schema name.
const std = @import("std");
const local = @import("antfly_local_sources");
const scopes = @import("../metadata/scope_retirement_ledger.zig");
const wire = @import("../metadata/scope_retirement_wire.zig");
const roots = @import("../metadata/table_retirement_ledger.zig");
const locks = @import("../system_catalog/lock_authority.zig");
const Server = @import("http_server.zig").ApiHttpServer;

pub fn step(server: *Server, after: u64, context: local.api_operation.RequestContext) !u64 {
    const group = server.cfg.prepared_transaction_group_id;
    if (server.cfg.prepared_transaction_leadership) |leadership| {
        if (!leadership.isLocalLeader(group)) return after;
    } else if (server.cfg.deployment_mode != .standalone) return after;
    var arena = std.heap.ArenaAllocator.init(server.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const work = (try std.json.parseFromSliceLeaky(?scopes.Work, a, try server.source.systemCatalog(a, context, .{ .scope_retirement_work = after }), .{})) orelse return 0;
    const head = work.head;
    try head.validate();
    if (head.lookup.group != group) return error.InvalidScopeRetirement;
    var nonce: [16]u8 = undefined;
    const io = server.sharedApiIo() orelse return error.MissingBackendRuntimeIo;
    try io.randomSecure(&nonce);
    const operation: @FieldType(wire.Request, "operation") = switch (head.progress.phase) {
        .draining_connections => blk: {
            if (head.force) {
                const actor: locks.prepared.Actor = .{ .session_user = .bootstrap, .credential = "" };
                const cut = try std.json.parseFromSliceLeaky(locks.Snapshot, a, try server.source.systemCatalog(a, context, .{ .sql_lock_read = .{
                    .incarnation = head.metadata_incarnation,
                    .standalone = server.cfg.deployment_mode == .standalone,
                    .actor = actor,
                    .database = head.lookup.reference.database,
                    .database_work = .{ .after = work.source_after, .wrap = true },
                } }), .{});
                if (cut.scope) |source| {
                    // The common lock supervisor owns data/transaction ACK
                    // drainage. Re-fencing a dead owner neither speeds it up
                    // nor grants permission to terminate a privileged owner.
                    if (source.state != .active) return head.sequence;
                    const request: locks.Request = .{
                        .scope_retirement = .{ .root = head.lookup, .expected_after = work.source_after },
                        .admission = .{ .nonce = nonce, .head = cut.head },
                        .incarnation = cut.incarnation,
                        .standalone = cut.standalone,
                        .database = head.lookup.reference.database,
                        .actor = actor,
                        .backend = source.registration.lease.backend,
                        .operation = .{ .fence = .close },
                    };
                    const outcome = try std.json.parseFromSliceLeaky(locks.outcomes.Outcome, a, try server.source.systemCatalog(a, context, .{ .sql_lock_mutate = request }), .{});
                    _ = try outcome.result();
                    return head.sequence;
                }
            }
            break :blk .{ .drain = .{ .lookup = head.lookup, .expected_revision = head.progress.revision } };
        },
        .retiring_tables => blk: {
            const child = work.child orelse return error.InvalidScopeRetirement;
            const lookup: roots.Lookup = .{ .group = child.lookup.group, .table = child.table, .id = child.id };
            const receipts = @import("../metadata/table_retirement_receipt.zig");
            const expected: receipts.Request = .{ .lookup = lookup, .effect = .{ .scope_admission = child } };
            const admitted = try std.json.parseFromSliceLeaky(receipts.Observation, a, try server.source.systemCatalog(a, context, .{ .table_retirement_receipt = expected }), .{});
            try admitted.requireFor(expected);
            if (!admitted.accepted) {
                const command = try (roots.Command{ .scope_begin = child }).encodeAlloc(a);
                _ = try server.source.systemCatalog(a, context, .{ .table_retirement_submit = command });
                return head.sequence;
            }
            const bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_owner_work = lookup });
            const current = try std.json.parseFromSliceLeaky(roots.Work, a, bytes, .{});
            if (!std.meta.eql(current.head.scope.parent, @as(?scopes.Child, child))) return error.InvalidScopeRetirement;
            if (current.head.phase != .physical_ready) return head.sequence;
            break :blk .{ .child_ready = .{ .lookup = head.lookup, .expected_revision = head.progress.revision, .ordinal = child.ordinal } };
        },
        .publishing_catalog => .{ .publish = .{ .lookup = head.lookup, .expected_revision = head.progress.revision } },
        .published, .canceled => return head.sequence,
    };
    const response = try std.json.parseFromSliceLeaky(wire.Observation, a, try server.source.systemCatalog(a, context, .{ .scope_retirement_submit = .{ .admission = .{ .nonce = nonce, .head = 0 }, .operation = operation } }), .{});
    if (response.outcome) |outcome| if (outcome.result == .rejected) switch (outcome.result.rejected) {
        .ScopeRetirementChanged, .SqlDatabaseInUse, .CatalogGenerationChanged, .CatalogPublicationProofPending => {},
        else => return outcome.result.rejected.toError(),
    };
    return head.sequence;
}
