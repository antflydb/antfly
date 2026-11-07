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

//! One native effect per lock occurrence. A request-scoped cache amortizes
//! metadata enrollment and native effect-sequence discovery across a row scan;
//! ownership and recovery remain entirely in durable metadata/data journals.
const std = @import("std");
const local = @import("antfly_local_sources");
const catalog = local.sql_catalog;
const ast = local.sql_ast;
const locks = local.storage_pessimistic_lock_contract;
const ledger = local.storage_pessimistic_lock_ledger;
const scopes = local.storage_pessimistic_lock_authority;
const transport = @import("sql_data_locks.zig");
const authority = @import("../system_catalog/lock_authority.zig");
const producer = @import("sql_command_producer.zig");
const Root = @import("sql_execution.zig").Adapter;
const A = std.mem.Allocator;
const Native = local.storage_pessimistic_lock_control;
const TableCatalog = @import("table_catalog.zig");

pub const Owner = struct {
    arena: std.heap.ArenaAllocator,
    root: *Root,
    participants: std.ArrayList(*Participant) = .empty,
    tables: std.AutoHashMapUnmanaged(u64, struct { schema_version: u32, modes: u8 }) = .empty,
    const Participant = struct { value: scopes.Data.Participant, next_effect: u64, table_modes: u8 = 0, range: local.storage_db_types.ByteRange };
    pub fn deinit(self: *Owner) void {
        const a = self.arena.child_allocator;
        self.arena.deinit();
        a.destroy(self);
    }
    fn init(root: *Root) !*Owner {
        if (root.data_lock_owner) |owner| return owner;
        const result = try root.server.alloc.create(Owner);
        result.* = .{ .root = root, .arena = std.heap.ArenaAllocator.init(root.server.alloc) };
        root.data_lock_owner = result;
        return result;
    }
    fn nonce(self: *Owner) ![16]u8 {
        var result: [16]u8 = undefined;
        try self.root.server.embedding_provider_runtime.io.randomSecure(&result);
        return result;
    }
    fn checkpoint(self: *Owner) !void {
        try self.root.context.ensureActive();
        const frame = try self.root.lockingFrame();
        try frame.requireExecuting(self.root.server.alloc, @intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_ms)));
    }
    fn pause(self: *Owner) !void {
        try self.checkpoint();
        try self.root.server.embedding_provider_runtime.io.sleep(.fromMilliseconds(2), .awake);
        try self.checkpoint();
    }
    fn observeRoot(self: *Owner, a: A, spec: scopes.Data.Spec, id: [16]u8) !authority.Snapshot {
        const frame = try self.root.lockingFrame();
        var port: producer.NativePort = .{ .server = self.root.server, .context = self.root.context };
        const bytes = try port.port().call(port.port().ptr, a, .{ .sql_lock_read = .{
            .incarnation = frame.fence.backend.incarnation,
            .standalone = frame.fence.standalone,
            .actor = frame.actor,
            .database = frame.fence.database,
            .backend = frame.fence.backend,
            .participant = .{ .route = .{ .spec = spec, .transaction = id } },
        } });
        defer a.free(bytes);
        return std.json.parseFromSliceLeaky(authority.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    }
    fn refreshRoute(self: *Owner, a: A, previous: scopes.Data.Spec) !scopes.Data.Spec {
        var routing = (try TableCatalog.authoritativeTableRoutingSnapshot(a, self.root.server.catalogSource(), previous.table_name, self.root.context.deadline_ns)) orelse return error.TableNotFound;
        defer routing.deinit(a);
        if (routing.table_id != previous.route.table_id or routing.topology_epoch != previous.route.topology_epoch) return error.CatalogGenerationChanged;
        for (routing.ranges) |range| {
            if (range.group_id != previous.route.route.group_id) continue;
            const route = routing.resolveRouteForKey(range.start_key) orelse return error.CatalogGenerationChanged;
            if (!std.meta.eql(route, previous.route.route)) return error.CatalogGenerationChanged;
            var result = previous;
            result.route = routing.fenceForRoute(route);
            result.route.native_participant = previous.route.native_participant;
            const routing_source = self.root.server.catalogSource();
            try routing_source.pinNativeParticipant(a, &result.route, routing_source.deadlineFrom(.{ .deadline_ns = self.root.context.deadline_ns, .io = self.root.context.deadline_io }));
            return result;
        }
        return error.CatalogGenerationChanged;
    }
    fn enroll(self: *Owner, initial_spec: scopes.Data.Spec, range: local.storage_db_types.ByteRange) !*Participant {
        var spec = initial_spec;
        try self.checkpoint();
        const routing_source = self.root.server.catalogSource();
        const deadline = routing_source.deadlineFrom(.{ .deadline_ns = self.root.context.deadline_ns, .io = self.root.context.deadline_io });
        try routing_source.pinNativeParticipant(self.arena.allocator(), &spec.route, deadline);
        for (self.participants.items) |item| if (item.value.spec.route.route.group_id == spec.route.route.group_id) {
            if (item.value.spec.route.table_id != spec.route.table_id or item.value.spec.schema_version != spec.schema_version or item.value.spec.route.topology_epoch != spec.route.topology_epoch) return error.LockOwnerChanged;
            return item;
        };
        var stage: enum { transaction, metadata_read, metadata_admit, metadata_confirm, native_admit, native_read } = .transaction;
        errdefer std.log.warn("native SQL lock enrollment rejected stage={s} metadata_group={} data_group={} table_id={} revision={}", .{ @tagName(stage), spec.route.metadata_group_id, spec.route.route.group_id, spec.route.table_id, spec.route.catalog_revision });
        const id = try self.root.lockingReadTransaction();
        const a = self.arena.allocator();
        const frame = try self.root.lockingFrame();
        var port: producer.NativePort = .{ .server = self.root.server, .context = self.root.context };
        stage = .metadata_read;
        var cut = try self.observeRoot(a, spec, id);
        if (cut.participant == null or !std.meta.eql(cut.participant.?.admission.parent_command, frame.fence.command)) {
            var request: authority.Request = .{ .admission = .{ .head = cut.head, .nonce = try self.nonce() }, .incarnation = frame.fence.backend.incarnation, .standalone = frame.fence.standalone, .actor = frame.actor, .database = frame.fence.database, .backend = frame.fence.backend, .operation = .{ .data_enroll = .{ .command = frame.fence.command, .transaction = id, .spec = spec } } };
            stage = .metadata_admit;
            var refreshes: u8 = 0;
            while (true) {
                try self.checkpoint();
                _ = port.port().submit(a, request) catch |err| switch (err) {
                    error.MetadataMutationOutcomeUnknown => {
                        try self.pause();
                        continue;
                    },
                    error.CatalogGenerationChanged => {
                        // The retained outcome proves this control was rejected.
                        // Refresh only its route capability, never SQL or row
                        // evaluation. Schema/identity/membership changes still
                        // reject the whole statement against its original bind.
                        if (refreshes == 2) return err;
                        const fresh = try self.refreshRoute(a, spec);
                        if (fresh.route.catalog_revision == spec.route.catalog_revision) return err;
                        spec = fresh;
                        cut = try self.observeRoot(a, spec, id);
                        request.admission = .{ .head = cut.head, .nonce = try self.nonce() };
                        request.operation.data_enroll.spec = spec;
                        refreshes += 1;
                        continue;
                    },
                    else => return err,
                };
                break;
            }
            stage = .metadata_confirm;
            cut = try self.observeRoot(a, spec, id);
        }
        const enrolled = cut.participant orelse return error.LockOwnerChanged;
        if (!std.meta.eql(enrolled.admission.parent_command, frame.fence.command) or !enrolled.command_open) return error.LockOwnerChanged;
        stage = .native_admit;
        try transport.submit(self.root.server, a, self.root.context, spec.table_name, .{ .admission = enrolled.admission, .operation = .begin });
        stage = .native_read;
        const native = try transport.observe(self.root.server, a, self.root.context, spec.table_name, .{ .admission = enrolled.admission });
        const owner = native.owner orelse return error.InvalidSqlBackendResponse;
        if (owner.state != .active or owner.job != null or owner.waiting != null) return error.LockOwnerBusy;
        const result = try a.create(Participant);
        result.* = .{ .value = enrolled, .next_effect = owner.next_effect, .range = .{ .start = try a.dupe(u8, range.start), .end = try a.dupe(u8, range.end) } };
        try self.participants.append(a, result);
        return result;
    }
    fn effect(self: *Owner, part: *Participant, action: ledger.Action) !ledger.Effect {
        return .{ .backend = (try part.value.admission.registration()).lease.backend, .sequence = part.next_effect, .nonce = try self.nonce(), .action = action };
    }
    fn acquireTable(self: *Owner, part: *Participant, mode: locks.TableMode, wait: ast.LockWait) !void {
        const bit = @as(u8, 1) << @as(u3, @intCast(@backingInt(mode)));
        if (part.table_modes & bit != 0) return;
        const acquisition: ledger.Acquisition = .{ .resource = try part.value.admission.binding.table(), .mode = .{ .table = mode }, .scope = .{ .transaction = .{ .id = part.value.admission.transaction, .subtransaction = self.root.active_subtransaction } } };
        const effect_value = try self.effect(part, if (wait == .block) .{ .acquire = acquisition } else .{ .try_acquire = acquisition });
        const request: Native.Command = .{ .admission = part.value.admission, .operation = .{ .acquire = effect_value } };
        var submit_needed = true;
        var waited_at: ?ledger.ResourceStamp = null;
        while (true) {
            try self.checkpoint();
            var scratch = std.heap.ArenaAllocator.init(self.root.server.alloc);
            defer scratch.deinit();
            const a = scratch.allocator();
            if (submit_needed) try transport.submit(self.root.server, a, self.root.context, part.value.spec.table_name, request);
            const cut = try transport.observe(self.root.server, a, self.root.context, part.value.spec.table_name, .{ .admission = request.admission, .effect = effect_value });
            const receipt = cut.receipt orelse return error.InvalidSqlBackendResponse;
            try receipt.requireFor(request.admission.binding.namespace, effect_value);
            switch (receipt.outcome) {
                .pending => {
                    // Re-propose only when a holder/queue revision changes.
                    // Read-only observations still see cancellation/deadlock
                    // receipts, without filling Raft with identical waiters.
                    submit_needed = !submit_needed and !std.meta.eql(waited_at, cut.resource);
                    waited_at = cut.resource;
                    if (!submit_needed) try self.pause();
                },
                .granted => {
                    part.next_effect += 1;
                    part.table_modes |= bit;
                    return;
                },
                .contended => {
                    part.next_effect += 1;
                    return error.SqlLockNotAvailable;
                },
                .deadlocked => return error.SqlDeadlockDetected,
                .cancelled => return error.Canceled,
                else => return error.InvalidSqlBackendResponse,
            }
        }
    }
};

pub fn explicit(raw: *anyopaque) !void {
    const root: *Root = @ptrCast(@alignCast(raw));
    if (root.active_transaction == null) return error.SqlTransactionNotActive;
}
pub fn authorizeRows(raw: *anyopaque, a: A, target: catalog.Table) !void {
    const root: *Root = @ptrCast(@alignCast(raw));
    var authorized = target;
    authorized.sql_required_privileges = local.system_catalog_privileges.Access.bit(.select) | local.system_catalog_privileges.Access.bit(.update);
    try @import("sql_authorization_runtime.zig").authorize(root, a, authorized);
}
pub fn authorize(raw: *anyopaque, a: A, target: catalog.Table, mode: locks.TableMode) !void {
    const root: *Root = @ptrCast(@alignCast(raw));
    var authorized = target;
    // PostgreSQL permits any mode with MAINTAIN/UPDATE/DELETE/TRUNCATE;
    // INSERT permits ROW EXCLUSIVE and weaker, SELECT only ACCESS SHARE.
    for ([_]local.system_catalog_privileges.Privilege{ .maintain, .update, .delete, .truncate, .insert, .select }) |privilege| {
        const applicable = switch (privilege) {
            .select => mode == .access_share,
            .insert => mode == .access_share or mode == .row_share or mode == .row_exclusive,
            else => true,
        };
        if (applicable) {
            authorized.sql_required_privileges = local.system_catalog_privileges.Access.bit(privilege);
            @import("sql_authorization_runtime.zig").authorize(root, a, authorized) catch |err| switch (err) {
                error.Forbidden => continue,
                else => return err,
            };
            return;
        }
    }
    return error.Forbidden;
}
pub fn table(raw: *anyopaque, _: A, target: catalog.Table, mode: locks.TableMode, wait: ast.LockWait) !void {
    const root: *Root = @ptrCast(@alignCast(raw));
    if (mode != .access_share) _ = try root.lockingTransaction();
    const owner = try Owner.init(root);
    const a = owner.arena.allocator();
    const bit = @as(u8, 1) << @as(u3, @intCast(@backingInt(mode)));
    if (owner.tables.getPtr(target.id)) |held| {
        if (held.schema_version != target.schema_version) return error.CatalogGenerationChanged;
        if (held.modes & bit != 0) return;
        // The first successful acquisition enrolled every native range and
        // pins its membership until retirement. Reuse those exact admissions
        // when another operator in this command needs a stronger table mode.
        for (owner.participants.items) |part| if (part.value.spec.route.table_id == target.id) try owner.acquireTable(part, mode, wait);
        held.modes |= bit;
        return;
    }
    var routing = (try TableCatalog.tableRoutingSnapshotForWrite(a, root.server.catalogSource(), target.physical_name, root.context.deadline_ns)) orelse return error.TableNotFound;
    defer routing.deinit(a);
    if (routing.table_id != target.id) return error.CatalogGenerationChanged;
    // Stable range order is shared with write routing. Every range is enrolled
    // before its first native grant, pinning membership through retirement.
    for (routing.ranges) |range| {
        const route = routing.resolveRouteForKey(range.start_key) orelse return error.TopologyChanged;
        const spec: scopes.Data.Spec = .{ .table_name = target.physical_name, .route = routing.fenceForRoute(route), .schema_version = if (target.schema_json.len == 0) null else target.schema_version };
        try owner.acquireTable(try owner.enroll(spec, .{ .start = range.start_key, .end = range.end_key orelse "" }), mode, wait);
    }
    try owner.tables.put(a, target.id, .{ .schema_version = target.schema_version, .modes = bit });
}

pub fn row(raw: *anyopaque, a: A, target: catalog.Table, identity: local.sql_locking.Identity, captured: catalog.Row, mode: locks.RowMode, wait: ast.LockWait, fields: []const []const u8) !?catalog.Row {
    const root: *Root = @ptrCast(@alignCast(raw));
    const owner = try Owner.init(root);
    const part = for (owner.participants.items) |item| {
        if (item.value.spec.route.table_id == target.id and item.range.contains(identity.key)) break item;
    } else return error.LockOwnerNotRegistered;
    var current = captured;
    var witness: local.storage_pessimistic_lock_row.Witness = .{ .version = identity.version, .digest = identity.digest orelse return error.InvalidSqlBackendResponse };
    while (true) {
        const acquisition: ledger.Acquisition = .{ .resource = try part.value.admission.binding.row(identity.key), .mode = .{ .row = mode }, .scope = .{ .transaction = .{ .id = part.value.admission.transaction, .subtransaction = root.active_subtransaction } } };
        const effect_value = try owner.effect(part, .{ .acquire = acquisition });
        const request: Native.Command = .{ .admission = part.value.admission, .operation = .{ .row = .{ .effect = effect_value, .key = identity.key, .witness = witness, .wait = if (wait == .block) .block else .try_lock } } };
        const row_request = request.rowRequest(request.operation.row);
        var waited_for_write = false;
        var submit_needed = true;
        var waited_at: ?ledger.ResourceStamp = null;
        while (true) {
            try owner.checkpoint();
            var scratch = std.heap.ArenaAllocator.init(root.server.alloc);
            defer scratch.deinit();
            const tmp = scratch.allocator();
            if (submit_needed) try transport.submit(root.server, tmp, root.context, part.value.spec.table_name, request);
            const cut = try transport.observe(root.server, tmp, root.context, part.value.spec.table_name, .{ .admission = request.admission, .row_request = row_request });
            const result = cut.row_result orelse return error.InvalidSqlBackendResponse;
            try result.receipt.requireFor(request.admission.binding.namespace, effect_value);
            if (result.receipt.outcome == .pending) {
                submit_needed = !submit_needed and !std.meta.eql(waited_at, cut.resource);
                waited_at = cut.resource;
                if (!submit_needed) try owner.pause();
                continue;
            }
            part.next_effect += 1;
            if (result.receipt.outcome == .deadlocked) return error.SqlDeadlockDetected;
            switch (result.status) {
                .matched => if (result.receipt.outcome == .granted) return current else return error.Canceled,
                .absent => return null,
                .contended => if (wait == .skip_locked) return null else return error.SqlLockNotAvailable,
                .changed => break,
                .write_pending => {
                    if (wait != .block) return if (wait == .skip_locked) null else error.SqlLockNotAvailable;
                    // Waiting on a write intent needs read-only point probes,
                    // not repeated WAL proposals or replaying a false effect.
                    while (true) {
                        try owner.pause();
                        var poll = std.heap.ArenaAllocator.init(root.server.alloc);
                        defer poll.deinit();
                        const observed = try transport.observe(root.server, poll.allocator(), root.context, part.value.spec.table_name, .{ .admission = request.admission, .intent_key = identity.key });
                        if (observed.intent_owner == null) break;
                    }
                    waited_for_write = true;
                    break;
                },
            }
        }
        const read_committed = try root.lockingReadCommitted();
        if (!waited_for_write and !read_committed) return error.SqlSerializationFailure;
        current = (try root.lockingCurrentRow(a, target, identity.key, fields)) orelse {
            if (!read_committed) return error.SqlSerializationFailure;
            return null;
        };
        const refreshed: local.storage_pessimistic_lock_row.Witness = .{ .version = current.version, .digest = current.expected_content_digest orelse return error.InvalidSqlBackendResponse };
        if (!read_committed and !std.meta.eql(witness, refreshed)) return error.SqlSerializationFailure;
        witness = refreshed;
    }
}
