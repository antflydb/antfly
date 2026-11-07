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

//! Single-attempt admission and exact durable completion. Once a begin may
//! have crossed the proposal boundary, neither cancellation nor transport loss
//! permits replaying it. Only immutable job observations resolve uncertainty.
const std = @import("std");
const root = @import("../metadata/table_retirement_ledger.zig");
const operation = @import("antfly_local_sources").api_operation;
const Server = @import("http_server.zig").ApiHttpServer;

/// One durable metadata/owner effect per quantum. No volatile plan, name
/// recapture or accepted-receipt cache is needed after failover or restart.
pub fn step(server: *Server, after: u64, context: operation.RequestContext) !u64 {
    return stepInternal(server, after, context, false);
}

pub const TestDriver = if (@import("builtin").is_test) struct {
    /// Interrupt after real owner execution and signed observation, before its
    /// metadata ACK. The ordinary durable discovery feed must recover it cold.
    pub fn loseOwnerReply(server: *Server, after: u64, context: operation.RequestContext) !u64 {
        return stepInternal(server, after, context, true);
    }
} else struct {};

fn stepInternal(server: *Server, after: u64, context: operation.RequestContext, comptime lose_owner_reply: bool) !u64 {
    var arena = std.heap.ArenaAllocator.init(server.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const page_bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_queue = after });
    const page = try std.json.parseFromSliceLeaky(@import("../metadata/table_retirement_work.zig").Page, a, page_bytes, .{});
    const lookup = page.lookup orelse return page.next;
    return stepLookup(server, a, lookup, page.next, context, lose_owner_reply, null);
}

/// A source shutting down can own a retained TEMP scope whose ancestor runs
/// on metadata. Drive only its already admitted physical child while ordinary
/// session maintenance is joined; never discover an unrelated table job.
pub fn stepScopeChild(server: *Server, child: @import("../metadata/scope_retirement_ledger.zig").Child, context: operation.RequestContext) !void {
    try child.validate();
    var arena = std.heap.ArenaAllocator.init(server.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const lookup: root.Lookup = .{ .group = child.lookup.group, .table = child.table, .id = child.id };
    const receipts = @import("../metadata/table_retirement_receipt.zig");
    const expected: receipts.Request = .{ .lookup = lookup, .effect = .{ .scope_admission = child } };
    const bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_receipt = expected });
    const admitted = try std.json.parseFromSliceLeaky(receipts.Observation, a, bytes, .{});
    try admitted.requireFor(expected);
    if (!admitted.accepted) return;
    _ = try stepLookup(server, a, lookup, 0, context, false, child);
}

fn stepLookup(server: *Server, a: std.mem.Allocator, lookup: root.Lookup, next: u64, context: operation.RequestContext, comptime lose_owner_reply: bool, expected_parent: ?@import("../metadata/scope_retirement_ledger.zig").Child) !u64 {
    const work_bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_owner_work = lookup });
    const work = try std.json.parseFromSliceLeaky(root.Work, a, work_bytes, .{});
    try work.head.validate();
    if (!std.meta.eql(work.head.lookup, lookup)) return error.InvalidCatalogRecord;
    if (expected_parent) |parent| if (!std.meta.eql(work.head.scope.parent, @as(?@import("../metadata/scope_retirement_ledger.zig").Child, parent))) return error.InvalidCatalogRecord;
    if (work.head.scope.parent != null and work.head.phase == .physical_ready) return next;
    var command: root.Command = undefined;
    switch (work.head.phase) {
        .reserved, .physical_ready => {
            const bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_effects = lookup });
            var effects = try @import("../metadata/table_retirement_manifest.zig").Owned.parse(a, bytes);
            defer effects.deinit();
            if (!std.meta.eql(try effects.value.reference(), work.head.effects) or !std.mem.eql(u8, &effects.value.metadata_incarnation, &work.head.incarnation)) return error.InvalidCatalogRecord;
            if (effects.value.root_ranges.len == 0) return error.GenerationAdmissionPending;
            // Elect a stable data-owner coordinator for preparation. Every
            // mutation still revalidates its metadata/data Raft leadership.
            if (!try @import("group_work_ownership.zig").mayCoordinate(server.cfg.relational_maintenance_leadership, a, effects.value.root.table_id, effects.value.root_ranges[0].group_id, effects.value.root.sql_persistence, .retirement, context)) return next;
            if (work.head.phase == .reserved) {
                command = .{ .seal = .{ .lookup = lookup, .expected_revision = work.head.revision, .plan = try @import("fk_generation_plan_builder.zig").buildReservedTableRetirement(server, a, context, work.head, effects.value) } };
            } else if (effects.value.survivors.len == 0) {
                command = .{ .publish = .{ .lookup = lookup, .expected_revision = work.head.revision } };
            } else {
                var transport: @import("schema_validation_owner_transport.zig").Transport = .{ .server = server, .alloc = a };
                defer transport.deinit();
                const io = server.sharedApiIo() orelse return error.MissingBackendRuntimeIo;
                var prepared = try @import("schema_validation_plan_builder.zig").buildTableRetirement(&transport, a, io, context, work.head, effects.value, if (server.cfg.deployment_mode == .standalone) .native else .raft);
                defer prepared.deinit();
                const accepted = try @import("schema_validation_admission.zig").beginWhenReady(server.source, a, io, context, prepared.command);
                switch (accepted) {
                    .reply => {},
                    .rejected => |err| return err,
                }
                return next;
            }
        },
        .retiring => {
            const owner = work.owner orelse return error.InvalidCatalogRecord;
            // Scheduling is selected from the admitted immutable preimage,
            // never a fresh table name or an unrelated Raft group leader.
            const bytes = try server.source.systemCatalog(a, context, .{ .table_retirement_effects = lookup });
            var effects = try @import("../metadata/table_retirement_manifest.zig").Owned.parse(a, bytes);
            defer effects.deinit();
            if (!std.meta.eql(try effects.value.reference(), work.head.effects) or !std.mem.eql(u8, &effects.value.metadata_incarnation, &work.head.incarnation)) return error.InvalidCatalogRecord;
            const captured = try capturedOwner(effects.value, owner.table, owner.group);
            if (!try @import("group_work_ownership.zig").mayCoordinate(server.cfg.relational_maintenance_leadership, a, owner.table, owner.group, captured.table.sql_persistence, .retirement, context)) return next;
            const step_kind: root.physical.Step = @fromBackingInt(@backingInt(work.head.physical.?.phase));
            const proof: root.physical.Evidence = if (step_kind == .source_fenced)
                .{ .source = server.executeCapturedFkGenerationSource(a, captured, .{ .plan_id = lookup.id, .child_table_id = lookup.table, .child_table_name = work.root_name, .child_group_id = owner.group, .action = .fence }, context) catch |err| switch (err) {
                    error.TransactionTopologyBusy => {
                        if (work.head.scope.parent != null and captured.table.sql_persistence != null and captured.table.sql_persistence.?.kind == .temporary)
                            _ = try @import("sql_data_locks.zig").abandonedPrivateQuantum(server, a, context, work.head);
                        return next;
                    },
                    else => return err,
                } }
            else
                .{ .parent = try server.executeCapturedFkGenerationParent(a, captured, .{ .plan_id = lookup.id, .child_table_id = lookup.table, .child_table_name = work.root_name, .parent_table_id = owner.table, .parent_group_id = owner.group, .action = switch (step_kind) {
                    .parent_staged => .stage,
                    .parent_activated => .activate,
                    .parent_acknowledged, .parent_coverage => .acknowledge,
                    else => unreachable,
                } }, context) };
            if (step_kind == .source_fenced and work.head.scope.parent != null and captured.table.sql_persistence != null and captured.table.sql_persistence.?.kind == .temporary and !try @import("sql_data_locks.zig").abandonedPrivateQuantum(server, a, context, work.head)) return next;
            if (lose_owner_reply) return error.InjectedRetirementOwnerReplyLoss;
            command = .{ .ack = .{ .lookup = lookup, .expected_revision = work.head.revision, .ordinal = owner.ordinal, .step = step_kind, .evidence = proof } };
        },
        .validating_survivors, .published, .canceled => return next,
    }
    const bytes = try command.encodeAlloc(a);
    _ = try server.source.systemCatalog(a, context, .{ .table_retirement_submit = bytes });
    return next;
}

fn capturedOwner(manifest: @import("../metadata/table_retirement_manifest.zig").Manifest, table: u64, group: u64) !@import("table_retirement_preparation.zig").Selection {
    if (manifest.root.table_id == table) {
        for (manifest.root_ranges) |range| if (range.group_id == group and range.table_id == table) return .{ .table = manifest.root, .range = range };
    }
    for (manifest.external_parents) |parent| if (parent.table.table_id == table) {
        for (parent.ranges) |range| if (range.group_id == group and range.table_id == table) return .{ .table = parent.table, .range = range };
    };
    for (manifest.view_backings) |backing| if (backing.table.table_id == table) {
        for (backing.ranges) |range| if (range.group_id == group and range.table_id == table) return .{ .table = backing.table, .range = range };
    };
    for (manifest.survivors) |survivor| if (survivor.before.table_id == table) {
        for (manifest.survivor_ranges) |range| if (range.group_id == group and range.table_id == table) return .{ .table = survivor.before, .range = range };
    };
    return error.InvalidCatalogRecord;
}

test "SQL relation lifetime scoped cleanup drives only an admitted exact child and rejects substituted custody before owner effects" {
    const scopes = @import("../metadata/scope_retirement_ledger.zig");
    const receipts = @import("../metadata/table_retirement_receipt.zig");
    const a = std.testing.allocator;
    const lookup: scopes.Lookup = .{ .group = 2188, .reference = .{ .id = @splat(1), .digest = @splat(2), .database = 1, .root = 2, .kind = .namespace, .tables = 1, .resources = 1 } };
    const child: scopes.Child = .{ .lookup = lookup, .ordinal = 0, .table = 3, .id = try lookup.reference.childId(3), .definition = @splat(4), .ranges = @splat(5) };
    try child.validate();
    const head: root.Head = .{
        .lookup = .{ .group = lookup.group, .table = child.table, .id = child.id },
        .incarnation = "11111111111111111111111111111111".*,
        .effects = .{ .digest = @splat(6), .root_table = child.table, .survivors = 0, .counters = 0, .views = 0 },
        .scope = .{ .database = 1, .namespace = 2, .creator = .bootstrap, .parent = child },
        .participant_count = 1,
        .range_count = 1,
        .participants_digest = @splat(7),
    };
    try head.validate();
    const Spy = struct {
        child: scopes.Child,
        head: root.Head,
        work_head: root.Head,
        accepted: bool = false,
        work_reads: usize = 0,
        fn status(_: *anyopaque) anyerror!@import("../metadata/api.zig").MetadataStatus {
            return error.TestUnexpectedStatusRead;
        }
        fn catalog(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return switch (call) {
                .table_retirement_receipt => |request| blk: {
                    try std.testing.expectEqualDeep(self.child, request.effect.scope_admission);
                    const observed: receipts.Observation = .{ .request = request, .head = if (self.accepted) self.head else null, .accepted = self.accepted };
                    break :blk std.json.Stringify.valueAlloc(alloc, observed, .{});
                },
                .table_retirement_owner_work => |selected| blk: {
                    self.work_reads += 1;
                    try std.testing.expectEqualDeep(self.head.lookup, selected);
                    break :blk std.json.Stringify.valueAlloc(alloc, root.Work{ .head = self.work_head, .owner = null }, .{});
                },
                // Queue discovery, effects preparation and owner execution
                // are never allowed from these nonadmitted/mismatched cuts.
                else => error.TestUnexpectedRetirementEffect,
            };
        }
    };
    var substituted = head;
    substituted.scope.parent.?.definition[0] ^= 1;
    try substituted.validate();
    var spy: Spy = .{ .child = child, .head = head, .work_head = substituted };
    var server = Server.init(a, .{}, .{ .ptr = &spy, .vtable = &.{ .status = Spy.status, .system_catalog = Spy.catalog } }, null, null);
    defer server.deinit();
    try stepScopeChild(&server, child, .{ .setting_admin = true, .schema_validation_authority = true, .fk_generation_publication_authority = true });
    try std.testing.expectEqual(@as(usize, 0), spy.work_reads);
    spy.accepted = true;
    try std.testing.expectError(error.InvalidCatalogRecord, stepScopeChild(&server, child, .{ .setting_admin = true, .schema_validation_authority = true, .fk_generation_publication_authority = true }));
    try std.testing.expectEqual(@as(usize, 1), spy.work_reads);
}

test "SQL relation lifetime retirement delivery preserves the exact captured native owner and range without fresh Raft lookup" {
    const records = @import("antfly_local_sources").common_topology_records;
    const manifests = @import("../metadata/table_retirement_manifest.zig");
    const a = std.testing.allocator;
    const schema = "{\"version\":1}";
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(schema, &digest, .{});
    const descriptor: records.RelationPersistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(3), .data_generation = 1 };
    const root_table: records.TableRecord = .{ .table_id = 7, .name = "table:retiring", .schema_json = schema, .description = "retained root preimage", .sql_persistence = descriptor };
    const parent_table: records.TableRecord = .{ .table_id = 8, .name = "table:parent", .schema_json = schema, .description = "retained parent preimage", .sql_persistence = descriptor };
    const root_range: records.RangeRecord = .{ .table_id = 7, .group_id = 72, .doc_identity_shard_id = 172, .doc_identity_range_id = 173, .start_key = "a", .end_key = "z" };
    const parent_range: records.RangeRecord = .{ .table_id = 8, .group_id = 82, .doc_identity_shard_id = 182, .doc_identity_range_id = 183, .start_key = "b", .end_key = "y" };
    // Selection operates only on an already parsed immutable Manifest; it
    // neither creates admission nor substitutes a current routing snapshot.
    const manifest: manifests.Manifest = .{
        .metadata_group_id = 2188,
        .metadata_incarnation = @splat('1'),
        .guard = .{ .target = .{ .name = "retiring" }, .logical_table = "retiring", .owner = .{ .table_id = 7, .schema_version = 1, .schema_digest = digest, .kind = .table }, .incarnation = @splat(0x11) },
        .root = root_table,
        .root_ranges = &.{root_range},
        .incoming = .{},
        .selection = .{ .relations = &.{.{ .table_id = 7, .physical_name = "table:retiring" }} },
        .counters = &.{},
        .survivors = &.{},
        .dependent_views = &.{},
        .view_backings = &.{},
        .survivor_ranges = &.{},
        .outgoing_parents = &.{8},
        .external_parents = &.{.{ .table = parent_table, .ranges = &.{parent_range} }},
        .view_edges = &.{},
    };
    const owned_bytes = try std.json.Stringify.valueAlloc(a, manifest, .{});
    defer a.free(owned_bytes);
    var retained = try std.json.parseFromSlice(manifests.Manifest, a, owned_bytes, .{});
    defer retained.deinit();
    const selected_root = try capturedOwner(retained.value, 7, 72);
    try std.testing.expectEqualDeep(root_table, selected_root.table);
    try std.testing.expectEqualDeep(root_range, selected_root.range);
    const selected_parent = try capturedOwner(retained.value, 8, 82);
    try std.testing.expectEqualDeep(parent_table, selected_parent.table);
    try std.testing.expectEqualDeep(parent_range, selected_parent.range);
    try std.testing.expectError(error.InvalidCatalogRecord, capturedOwner(retained.value, 7, 172));
    try std.testing.expectError(error.InvalidCatalogRecord, capturedOwner(retained.value, 7, 82));
    try std.testing.expectError(error.InvalidCatalogRecord, capturedOwner(retained.value, 8, 72));
    try std.testing.expectError(error.InvalidCatalogRecord, capturedOwner(retained.value, 9, 82));
}
