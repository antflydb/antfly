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

//! Native data-lock transport. Only private owner ingress can construct these
//! controls. Each accepting leader replaces caller-supplied metadata cuts with
//! a fresh linearizable observation before it appends an ordinary owner entry.
const std = @import("std");
const local = @import("antfly_local_sources");
const native = local.storage_pessimistic_lock_control;
const authority = @import("../system_catalog/lock_authority.zig");
const Server = @import("http_server.zig").ApiHttpServer;
const operation = local.api_operation;
const A = std.mem.Allocator;

pub fn authorize(server: *Server, a: A, context: operation.RequestContext, group: u64, table: []const u8, input: native.Command) !native.Command {
    try input.validate();
    if (group == 0 or input.admission.binding.namespace.group != group) return error.IdentityNamespaceMismatch;
    var result = input;
    // Prepared commands are authorized by the receiving native frozen 2PC
    // reference. They cannot depend on a surviving connection or metadata IO.
    if (input.operation == .prepare or input.operation == .resolve) {
        result.checked = null;
        return result;
    }
    var control_context = context;
    control_context.setting_admin = true;
    control_context.sql_command_fence = null;
    control_context.sql_completion = null;
    control_context.sql_completion_submission = null;
    try control_context.ensureActive();
    const source = input.admission.source;
    const raw = try server.source.systemCatalog(a, control_context, .{ .sql_lock_read = .{
        .incarnation = source.lease.backend.incarnation,
        .standalone = source.standalone,
        .actor = .{ .session_user = .bootstrap, .credential = "" },
        .database = source.lease.database,
        .backend = source.lease.backend,
        .participant = .{ .generation = input.admission.generation },
    } });
    defer a.free(raw);
    if (raw.len > 32768) return error.InvalidSqlBackendResponse;
    const cut = try std.json.parseFromSliceLeaky(authority.Snapshot, a, raw, .{ .allocate = .alloc_always });
    if (cut.group != source.group or cut.standalone != source.standalone or !std.mem.eql(u8, &cut.incarnation, &source.lease.backend.incarnation)) return error.LockOwnerChanged;
    const enrolled = cut.participant orelse return error.LockOwnerChanged;
    if (!std.mem.eql(u8, enrolled.spec.table_name, table) or enrolled.spec.route.route.group_id != group) return error.IdentityNamespaceMismatch;
    result.checked = .{ .source = cut.scope orelse return error.LockOwnerChanged, .participant = enrolled, .admitted_at_ms = @intCast(@max(1, @divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_ms))) };
    try result.checked.?.validate(input.admission);
    if (input.operation != .close) try result.checked.?.requireExecuting(input.admission);
    return result;
}

pub fn submit(server: *Server, a: A, context: operation.RequestContext, table: []const u8, input: native.Command) !void {
    const group = input.admission.binding.namespace.group;
    const checked = try authorize(server, a, context, group, table, input);
    const request: local.storage_db_types.BatchRequest = .{ .sql_lock = checked, .sync_level = .write };
    if (server.cfg.routed_raft_batch_writer) |writer| {
        const forwarding = @import("internal_batch_forwarding.zig");
        // The data leader independently authorizes this strict private shape.
        // Phase-two delivery cannot depend on a mutable catalog revision.
        _ = (try writer.write(a, .transaction, group, table, request, .{
            .remaining_ms = @min((try context.remainingTimeoutMs()) orelse forwarding.max_remaining_ms, forwarding.max_remaining_ms),
            .forwards_remaining = forwarding.max_forwards,
            .campaign_allowed = false,
        }, context)) orelse return error.GroupLeaderUnavailable;
    } else {
        const writes = server.table_writes orelse return error.UnsupportedOperation;
        _ = (try writes.batchGroupLocal(a, group, table, request)) orelse return error.GroupLeaderUnavailable;
    }
}

pub fn observe(server: *Server, a: A, context: operation.RequestContext, table: []const u8, query: native.Query) !native.Snapshot {
    try context.ensureActive();
    const raw = try std.json.Stringify.valueAlloc(a, query, .{});
    defer a.free(raw);
    const reads = server.table_reads orelse return error.UnsupportedOperation;
    var response = (try reads.lookupGroupLocal(a, query.admission.binding.namespace.group, table, "", .{ .sql_lock_query_json = raw }, .read_index)) orelse return error.GroupLeaderUnavailable;
    defer response.deinit(a);
    if (response.json.len > 65536) return error.InvalidSqlBackendResponse;
    return std.json.parseFromSliceLeaky(native.Snapshot, a, response.json, .{ .allocate = .alloc_always });
}

/// One native mutation or one metadata acknowledgement per pass. This is also
/// called by the existing source supervisor after a lost client; no SQL body,
/// selected row image, or in-memory participant cache is needed for recovery.
pub fn beforeMetadataMutation(server: *Server, backing: A, context: operation.RequestContext, request: authority.Request) !bool {
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const finishing = request.operation == .finish;
    const retiring = request.operation == .release or request.operation == .release_unadmitted or request.operation == .close_step;
    if (!finishing and !retiring) return true;
    var private = context;
    private.setting_admin = true;
    private.sql_command_fence = null;
    private.sql_completion = null;
    private.sql_completion_submission = null;
    const query: authority.Query = .{ .incarnation = request.incarnation, .standalone = request.standalone, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = request.database, .backend = request.backend };
    const raw = try server.source.systemCatalog(a, private, .{ .sql_lock_read = query });
    defer a.free(raw);
    const root = try std.json.parseFromSliceLeaky(authority.Snapshot, a, raw, .{ .allocate = .alloc_always });
    const scope = root.scope orelse return true;
    const directory = scope.data orelse return true;
    if (directory.count() == 0 or (finishing and directory.open == 0)) return true;
    // Resolution first records its independently checked terminal intent in
    // metadata. Only a subsequent pass may release native transaction grants.
    if (retiring and scope.state == .active and !scope.releasing) return true;
    for (directory.slots) |generation| {
        if (generation == 0) continue;
        var selection = query;
        selection.participant = .{ .generation = generation };
        const part_raw = try server.source.systemCatalog(a, private, .{ .sql_lock_read = selection });
        defer a.free(part_raw);
        const selected = try std.json.parseFromSliceLeaky(authority.Snapshot, a, part_raw, .{ .allocate = .alloc_always });
        const part = selected.participant orelse return error.InvalidSqlBackendResponse;
        if (finishing and !part.command_open) continue;
        if (finishing and !std.meta.eql(part.admission.parent_command, request.operation.finish)) return error.LockEffectChanged;
        const state = try observe(server, a, private, part.spec.table_name, .{ .admission = part.admission });
        if (finishing) {
            if (state.scope == null) {
                try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .begin });
                return false;
            }
            if (state.owner) |owner| if (owner.waiting) |effect| {
                try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .{ .cancel_wait = effect } });
                return false;
            };
            if (!std.meta.eql(state.scope.?.completed, @as(?authority.scopes.Command, try part.admission.command()))) {
                try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .finish });
                return false;
            }
        } else if (state.scope == null or state.scope.?.state != .retired) {
            try submit(server, a, private, part.spec.table_name, .{ .admission = part.admission, .operation = .close });
            return false;
        }
        var nonce: [16]u8 = undefined;
        try server.embedding_provider_runtime.io.randomSecure(&nonce);
        var ack = request;
        ack.admission = .{ .head = selected.head, .nonce = nonce };
        ack.actor = query.actor;
        ack.operation = if (finishing)
            .{ .data_ack = .{ .generation = generation, .command = part.admission.parent_command, .native = state } }
        else
            .{ .data_retire = .{ .generation = generation, .native = state } };
        const acknowledged = try server.source.systemCatalog(a, private, .{ .sql_lock_mutate = ack });
        defer a.free(acknowledged);
        const outcome = try std.json.parseFromSliceLeaky(?authority.outcomes.Outcome, a, acknowledged, .{});
        _ = try (outcome orelse return error.MetadataMutationOutcomeUnknown).result();
        return false;
    }
    return true;
}
