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

//! Public SQL producer for immutable namespace-owned type publication. Native
//! catalog transactions retain the outcome; ambiguity only permits observation.
const std = @import("std");
const local = @import("antfly_local_sources");
const types = @import("../system_catalog/named_types.zig");
const catalog = local.sql_catalog;
const execution = @import("sql_execution.zig");
const calls = @import("sql_routine_execution.zig");
const recovery = @import("sql_type_recovery.zig");
const common = @import("sql_catalog_recovery.zig");
const A = std.mem.Allocator;

pub fn snapshot(root: *execution.Adapter, a: A, request: types.SnapshotRequest) !types.Snapshot {
    try root.context.ensureActive();
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    if (!root.server.source.vtable.supports_sql_types) return error.UnsupportedSqlExecution;
    var private = root.context;
    private.setting_admin = true;
    const bytes = try root.server.source.systemCatalog(a, private, .{ .sql_type_snapshot = request });
    defer a.free(bytes);
    const cut = try std.json.parseFromSliceLeaky(types.Snapshot, a, bytes, .{ .allocate = .alloc_always });
    try cut.validate(a, request);
    return cut;
}
pub fn execute(root: *execution.Adapter, allocators: catalog.DdlAllocators, ddl: local.sql_ast.CatalogDdl) !catalog.DdlOutcome {
    if (ddl.named_type_kind == .domain and ddl.action == .alter_schema) {
        const definition = ddl.domain_definition orelse return error.InvalidSqlSyntax;
        if (definition != .set_not_null or !definition.set_not_null) return error.UnsupportedSqlExecution;
    }
    if (ddl.kind != .named_type or (ddl.action != .create and ddl.action != .alter_schema and ddl.action != .drop and ddl.action != .comment)) return error.UnsupportedSqlExecution;
    if (root.active_transaction != null or !root.server.source.vtable.supports_sql_types) return error.UnsupportedSqlExecution;
    if (root.server.cfg.auth_enabled and root.identity.* == null) return error.Forbidden;
    var region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer region.deinit();
    const a = region.allocator();
    const targets = calls.targets(root, a, ddl.name) catch |err| switch (err) {
        error.SqlUndefinedFunction => return error.SqlNamedTypeNotFound,
        else => return err,
    };
    const cut = try snapshot(root, a, .{ .targets = targets, .expected_revision = root.revision, .check_dependents = ddl.action == .alter_schema or (ddl.action == .drop and ddl.cascade) });
    root.revision = cut.revision;
    const creating = ddl.action == .create;
    const selected: ?usize = if (creating) scope: {
        for (cut.scopes, 0..) |candidate, i| if (candidate.namespace_id != null) break :scope i;
        break :scope null;
    } else if (cut.found) |found| found.scope else null;
    const index = selected orelse {
        if (ddl.action == .drop and ddl.conditional) {
            if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .message = try std.fmt.allocPrint(a, "type \"{s}\" does not exist, skipping", .{ddl.name.table}) });
            return .{};
        }
        if (!creating) return error.SqlNamedTypeNotFound;
        return if (cut.scopes[0].database_exists) error.NamespaceNotFound else error.DatabaseNotFound;
    };
    const scope = cut.scopes[index];
    try recovery.authorize(root.server, root.identity.*, a, scope.target);
    const previous: ?types.records.Record = if (cut.found) |found| if (found.scope == index) found.record else null else null;
    if (creating and previous != null) return error.SqlDuplicateType;
    if (!creating and ddl.named_type_kind == .domain and previous != null and previous.?.definition != .domain) return error.SqlWrongObjectType;
    var prepared_domain: ?local.sql_catalog.domain_definitions.Prepared = null;
    defer if (prepared_domain) |*prepared| prepared.deinit();
    if (creating and ddl.named_type_kind == .domain) prepared_domain = local.sql_catalog.domain_definitions.Prepared.init(a, ddl.name.table, (ddl.domain_definition orelse return error.InvalidSqlSyntax).create) catch |err| return if (err == error.SqlDomainBaseRequiresRegistry) error.UnsupportedSqlExecution else err;
    const expected: ?types.Identity = if (previous) |record| .{ .id = record.identity.id, .generation = record.identity.generation } else null;
    const operation: @FieldType(types.Mutation, "operation") = switch (ddl.action) {
        .create => if (prepared_domain) |prepared| .{ .create_domain = .{ .definition = prepared.definition, .owner = try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled) } } else .{ .create_enum = .{ .labels = (ddl.enum_definition orelse return error.InvalidSqlSyntax).create, .owner = try @import("sql_view_ddl.zig").currentOwner(root.identity.*, root.server.cfg.auth_enabled) } },
        .alter_schema => add: {
            if (ddl.named_type_kind == .domain) {
                const record = previous orelse return error.SqlNamedTypeNotFound;
                if (record.definition != .domain) return error.SqlWrongObjectType;
                if (record.definition.domain.not_null) return .{};
                break :add .{ .set_domain_not_null = .{ .expected = expected.? } };
            }
            const change = (ddl.enum_definition orelse return error.InvalidSqlSyntax).add;
            const record = previous orelse return error.SqlNamedTypeNotFound;
            if (record.definition != .labels) return error.SqlWrongObjectType;
            var view = try types.records.View.init(a, record.definition);
            defer view.deinit(a);
            if (view.find(change.label) != null) {
                if (!change.if_not_exists) return error.SqlDuplicateEnumLabel;
                if (root.notice_sink) |sink| try sink.emit(sink.ptr, .{ .code = "42710", .message = try std.fmt.allocPrint(a, "enum label \"{s}\" already exists, skipping", .{change.label}) });
                return .{};
            }
            break :add .{ .add_enum = .{ .expected = expected.?, .label = change.label, .placement = change.placement, .if_not_exists = change.if_not_exists } };
        },
        .drop => .{ .drop = .{ .expected = expected, .if_exists = ddl.conditional, .cascade = ddl.cascade } },
        .comment => .{ .comment = .{ .expected = expected orelse return error.SqlNamedTypeNotFound, .description = ddl.comment_value } },
        else => unreachable,
    };
    const Notice = catalog.notices.Notice;
    const conditional_notice: ?Notice = switch (operation) {
        .add_enum => |add| if (add.if_not_exists) .{ .code = "42710", .message = try std.fmt.allocPrint(a, "enum label \"{s}\" already exists, skipping", .{add.label}) } else null,
        .drop => |drop| if (drop.if_exists) .{ .message = try std.fmt.allocPrint(a, "type \"{s}\" does not exist, skipping", .{ddl.name.table}) } else null,
        else => null,
    };
    if (conditional_notice) |notice| if (root.notice_sink) |sink| if (sink.reserve) |reserve| try reserve(sink.ptr, 1, notice.message.len);
    var nonce: [16]u8 = undefined;
    try root.server.embedding_provider_runtime.io.randomSecure(&nonce);
    const request: types.Mutation = .{ .admission = .{ .nonce = nonce, .head = cut.head }, .target = scope.target, .namespace_id = scope.namespace_id.?, .incarnation = cut.incarnation, .operation = operation };
    try request.validate();
    if (operation == .drop and operation.drop.cascade and cut.has_dependents)
        return @import("sql_type_retirement.zig").execute(root, allocators, cut, request);
    const observation = try types.observation(a, request);
    var receipt = try recovery.newReceipt(allocators.result, a, observation, cut.revision);
    errdefer receipt.deinit(allocators.result);
    if ((operation == .add_enum or operation == .set_domain_not_null) and cut.has_dependents) return beginTypeEpoch(root, allocators, cut, request, receipt);
    var private = root.context;
    private.setting_admin = true;
    try root.context.ensureActive();
    const reply = root.server.source.systemCatalog(a, private, .{ .sql_type_mutate = request }) catch null;
    var outcome = if (reply) |bytes| recovery.decodeOutcome(a, bytes, observation, cut.revision) else null;
    if (outcome == null) outcome = recovery.observe(root.server, root.identity.*, root.context, a, observation, cut.revision);
    if (outcome) |known| {
        const result = try known.result();
        if (result.no_op) if (conditional_notice) |notice| if (root.notice_sink) |sink| try sink.emit(sink.ptr, notice);
        common.complete(&receipt, known);
        return .{ .catalog_receipt = receipt };
    }
    return .{ .mutation_outcome = null, .catalog_receipt = receipt };
}

fn beginTypeEpoch(root: *execution.Adapter, allocators: catalog.DdlAllocators, cut: types.Snapshot, request: types.Mutation, receipt: catalog.CatalogOperationReceipt) !catalog.DdlOutcome {
    const io = root.server.sharedApiIo() orelse return error.UnsupportedSqlExecution;
    const selected = cut.found orelse return error.SqlNamedTypeNotFound;
    // Capture ranges after authorization, using immutable type identity.
    // Unrelated catalog changes may commute; preparation and the accepting
    // writer independently rejoin the exact type generation and physical cut.
    var private = try root.context.platformDeadline();
    private.setting_admin = true;
    private.schema_validation_authority = true;
    // Hashing can allocate: finish it before the proposal boundary so OOM
    // cannot discard the already-owned recovery receipt after admission.
    const observation = try types.observation(allocators.scratch, request);
    {
        var region = std.heap.ArenaAllocator.init(allocators.scratch);
        defer region.deinit();
        const captured = try snapshot(root, region.allocator(), .{ .targets = &.{}, .reference = .{ .id = selected.record.identity.id, .namespace_id = selected.record.identity.namespace_id }, .include_dependents = true, .prepare_activation = true });
        var transport: @import("schema_validation_owner_transport.zig").Transport = .{ .server = root.server, .alloc = allocators.scratch };
        defer transport.deinit();
        var owned = try @import("schema_validation_plan_builder.zig").buildType(&transport, allocators.scratch, io, private, captured, request);
        defer owned.deinit();
        try root.context.ensureActive();
        // Share the ordinary schema admission boundary. Only explicit
        // pre-proposal report lag may retry this same signed immutable plan;
        // an uncertain proposal is observed through the original nonce.
        const admission = @import("schema_validation_admission.zig");
        const attempt: ?admission.BeginAttempt = admission.beginWhenReady(root.server.source, allocators.scratch, io, private, owned.command) catch |err| switch (err) {
            error.MetadataReplicationPending => return error.CatalogPublicationProofPending,
            error.Forbidden, error.CatalogGenerationChanged, error.GenerationPublicationChanged, error.MetadataIncarnationMismatch, error.TableTopologyProtocolUpgradeRequired, error.CatalogOccupancyNotReady, error.CatalogPublicationProofPending, error.UnsupportedNativeIndexOwner, error.RowPolicyUnsupported => return err,
            else => uncertain: {
                std.log.warn("SQL named type epoch admission unresolved err={s}", .{@errorName(err)});
                break :uncertain null;
            },
        };
        if (attempt) |value| switch (value) {
            .reply => |bytes| allocators.scratch.free(bytes),
            .rejected => |err| return err,
        };
    }
    // Release all schemas, transport frames and preparation state before
    // waiting. Completion retains only the compact nonce and result receipt.
    // A dropped reply never resubmits the begin.
    return awaitOutcome(root, allocators, observation, cut.revision, receipt);
}

/// Poll only the exact retained outcome. Unknown delivery never resubmits DDL.
pub fn awaitOutcome(root: *execution.Adapter, allocators: catalog.DdlAllocators, observation: types.OutcomeRequest, revision: u64, receipt: catalog.CatalogOperationReceipt) !catalog.DdlOutcome {
    // Admission has already crossed the durable boundary. Expiry or loss of
    // the local polling runtime must return the owned receipt, never turn an
    // accepted/uncertain mutation into an ordinary deadline error.
    var private = root.context.platformDeadline() catch return retainedOutcome(receipt);
    const io = root.server.sharedApiIo() orelse return retainedOutcome(receipt);
    private.setting_admin = true;
    var result = receipt;
    var observation_region = std.heap.ArenaAllocator.init(allocators.scratch);
    defer observation_region.deinit();
    if (allocators.completion == .bounded) private.deadline_ns = @min(private.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 2 * std.time.ns_per_s);
    while (true) {
        _ = observation_region.reset(.retain_capacity);
        if (recovery.observe(root.server, root.identity.*, private, observation_region.allocator(), observation, revision)) |known| {
            common.complete(&result, known);
            if (known != .pending) {
                _ = try known.result();
                return .{ .catalog_receipt = result };
            }
        } else break;
        private.ensureActive() catch break;
        io.sleep(.fromMilliseconds(20), .awake) catch break;
    }
    return retainedOutcome(result);
}
fn retainedOutcome(receipt: catalog.CatalogOperationReceipt) catalog.DdlOutcome {
    return .{ .mutation_outcome = if (receipt.state == .pending) .committed_pending else null, .catalog_receipt = receipt };
}

test "SQL named type completion preserves admitted receipts when the request expires before polling" {
    var root: execution.Adapter = undefined;
    root.context = .{ .deadline_ns = 0 };
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    for ([_]catalog.CatalogOperationReceipt.State{ .pending, .unknown }) |state| {
        const receipt: catalog.CatalogOperationReceipt = .{ .kind = .type, .operation = .drop, .database = "default", .namespace = "public", .name = "status", .recovery_token = "owned-before-admission", .state = state };
        // Server and observation are deliberately unreachable: cancellation
        // cannot trigger another RPC, allocation or unsafe mutation replay.
        const outcome = try awaitOutcome(&root, .{ .scratch = failing.allocator(), .result = failing.allocator() }, undefined, 1, receipt);
        try std.testing.expectEqualDeep(receipt, outcome.catalog_receipt.?);
        var completed = receipt;
        completed.object_id = 17;
        common.complete(&completed, .{ .success = .{ .revision = 2, .identity = null, .removed = 1 } });
        try std.testing.expectEqual(.committed, completed.state);
        try std.testing.expect(completed.object_id == null and completed.generation == null);
        try std.testing.expectEqual(@as(?catalog.MutationOutcome, if (state == .pending) .committed_pending else null), outcome.mutation_outcome);
    }
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    try std.testing.expect(!failing.has_induced_failure);
}
