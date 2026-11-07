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
    return stepInternal(server, after, context, null);
}

/// Standalone has no separate metadata API maintenance issuer. Its closing
/// source continues only this retained TEMP ancestor after broad maintenance
/// joins. The same metadata-leader gate and native journal govern each step.
pub fn stepRetainedTemporary(server: *Server, expected: scopes.Head, context: local.api_operation.RequestContext) !void {
    try expected.validate();
    if (expected.temporary_owner == null) return error.InvalidScopeRetirement;
    _ = try stepInternal(server, expected.sequence - 1, context, expected);
}

fn stepInternal(server: *Server, after: u64, context: local.api_operation.RequestContext, retained_head: ?scopes.Head) !u64 {
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
    if (retained_head) |retained| {
        if (head.sequence < retained.sequence) return error.InvalidScopeRetirement;
        // A completed ancestor can leave the active feed before this read.
        // Reobserve its durable outcome instead of executing a later root.
        if (head.sequence > retained.sequence) return after;
        if (!std.meta.eql(head.lookup, retained.lookup) or !std.mem.eql(u8, &head.metadata_incarnation, &retained.metadata_incarnation) or !std.mem.eql(u8, &head.target_digest, &retained.target_digest) or !head.creator.eql(retained.creator) or head.force != retained.force or head.temporary_owner == null or !head.temporary_owner.?.eql(retained.temporary_owner.?)) return error.InvalidScopeRetirement;
    }
    var nonce: [16]u8 = undefined;
    const io = server.sharedApiIo() orelse return error.MissingBackendRuntimeIo;
    try io.randomSecure(&nonce);
    const operation: @FieldType(wire.Request, "operation") = switch (head.progress.phase) {
        .draining_connections => blk: {
            // Lost native images have no surviving physical owner from which
            // to solicit drainage ACKs. The metadata leader records one exact
            // closed-family loss checkpoint per quantum before normal scope
            // drainage; retries retain the immutable child identity.
            if (head.native_loss_requested) {
                const completed = if (head.native_losses) |losses| losses.count else 0;
                if (completed < head.lookup.reference.tables) break :blk .{ .source_lost = .{
                    .lookup = head.lookup,
                    .expected_revision = head.progress.revision,
                    .ordinal = completed,
                } };
            }
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

test "SQL relation lifetime standalone cleanup keeps metadata leadership and exact retained TEMP ancestor authority" {
    const a = std.testing.allocator;
    const reference: @import("antfly_local_sources").system_catalog_domain.scope_retirement.Reference = .{ .id = @splat(1), .digest = @splat(2), .database = 1, .root = 2, .kind = .namespace, .tables = 0, .resources = 0 };
    const head: scopes.Head = .{
        .lookup = .{ .group = 2188, .reference = reference },
        .metadata_incarnation = @splat(3),
        .creator = .bootstrap,
        .target_digest = @splat(4),
        .temporary_owner = .{ .connection_id = @splat(5), .wire_epoch = @splat(6), .execution_instance = @splat(7), .connection_generation = 1, .principal_digest = @splat(8) },
        .sequence = 1,
        .progress = .{ .reference = reference },
    };
    try head.validate();
    const Spy = struct {
        head: scopes.Head,
        calls: usize = 0,
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.TestUnexpectedStatusRead;
        }
        fn catalog(raw: *anyopaque, alloc: std.mem.Allocator, _: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (call != .scope_retirement_work or call.scope_retirement_work != 0) return error.TestUnexpectedRetirementEffect;
            return std.json.Stringify.valueAlloc(alloc, @as(?scopes.Work, .{ .head = self.head, .child = null }), .{});
        }
        fn notLeader(_: *anyopaque, group: u64) bool {
            std.debug.assert(group == 2188);
            return false;
        }
    };
    var spy: Spy = .{ .head = head };
    var server = Server.init(a, .{ .deployment_mode = .distributed, .prepared_transaction_group_id = 2188 }, .{ .ptr = &spy, .vtable = &.{ .status = Spy.status, .system_catalog = Spy.catalog } }, null, null);
    defer server.deinit();
    const context: local.api_operation.RequestContext = .{ .setting_admin = true, .schema_validation_authority = true, .fk_generation_publication_authority = true };
    try stepRetainedTemporary(&server, head, context);
    try std.testing.expectEqual(@as(usize, 0), spy.calls);
    server.cfg.deployment_mode = .standalone;
    server.cfg.prepared_transaction_leadership = .{ .ptr = &spy, .vtable = &.{ .is_local_leader = Spy.notLeader } };
    try stepRetainedTemporary(&server, head, context);
    try std.testing.expectEqual(@as(usize, 0), spy.calls);
    server.cfg.prepared_transaction_leadership = null;
    spy.head.sequence = 2;
    spy.head.lookup.reference.id = @splat(9);
    spy.head.progress.reference = spy.head.lookup.reference;
    try spy.head.validate();
    // Completion may replace this feed item with a later unrelated ancestor.
    // It is observed only; this caller cannot execute that other root.
    try stepRetainedTemporary(&server, head, context);
    try std.testing.expectEqual(@as(usize, 1), spy.calls);
    spy.head = head;
    spy.head.temporary_owner.?.connection_generation += 1;
    try spy.head.validate();
    try std.testing.expectError(error.InvalidScopeRetirement, stepRetainedTemporary(&server, head, context));
    spy.head = head;
    spy.head.metadata_incarnation[0] ^= 1;
    try spy.head.validate();
    try std.testing.expectError(error.InvalidScopeRetirement, stepRetainedTemporary(&server, head, context));
    spy.head = head;
    spy.head.target_digest[0] ^= 1;
    try spy.head.validate();
    try std.testing.expectError(error.InvalidScopeRetirement, stepRetainedTemporary(&server, head, context));
    var ordinary = head;
    ordinary.temporary_owner = null;
    try ordinary.validate();
    const calls = spy.calls;
    try std.testing.expectError(error.InvalidScopeRetirement, stepRetainedTemporary(&server, ordinary, context));
    try std.testing.expectEqual(calls, spy.calls);
}
