// Copyright 2026 Antfly, Inc.
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

//! Transport-independent system catalog admission, capability fencing, and
//! exact receipt completion. Logical and physical table identities are separate.
const std = @import("std");
const domain = @import("domain.zig");
const storage = @import("../metadata/storage/raft_apply_store.zig");
const protocol = @import("../metadata/topology_protocol.zig");
const operation = @import("../api/operation.zig");
const tables_api = @import("../api/tables.zig");
const indexes_api = @import("../api/indexes.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const table_manager = @import("../metadata/table_manager.zig");

pub const Request = domain.Request;

pub fn mutate(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, request: Request) !void {
    try context.ensureActive();
    if (request.mutation.table_id != 0 or request.mutation.storage_name.len != 0) return error.InvalidCatalogMutation;
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.system_catalog_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const admission = try store.systemCatalogAdmission(a, svc.metadata_group_id, request.mutation);
    var command: storage.SystemCatalogCommand = .{ .expected_revision = admission.meta.revision, .mutation = request.mutation };
    if (request.mutation.action == .create and request.mutation.kind == .table) {
        const json = request.create_table_json orelse return error.InvalidCatalogMutation;
        var req = try tables_api.parseStoredCreateTableRequest(a, json);
        const storage_name = request.physical_name orelse try std.fmt.allocPrint(a, "table:{d}", .{admission.meta.next_id});
        if (!std.mem.startsWith(u8, storage_name, "table:") or storage_name.len > 1024) return error.InvalidCatalogMutation;
        req.indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(a, storage_name, req.indexes_json orelse tables_api.default_indexes_json, tables_api.effectiveSchemaJson(req.schema_json));
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(a, req.indexes_json.?);
        try managed_embedder.validateEmbeddingProducerOwnershipJson(a, req.indexes_json.?);
        var table = tables_api.deriveTableRecord(storage_name, req);
        const policy = admission.placement_policy;
        try policy.validate();
        if (policy.placement_role) |role| table.placement_role = role;
        if (policy.desired_replica_count) |count| table.desired_replica_count = count;
        if (req.num_shards == null) if (policy.min_ranges) |count| {
            table.min_ranges = count;
        };
        const generation = try svc.captureTableCreateGeneration(a, table.table_id);
        const ranges = try tables_api.deriveInitialRangesForGeneration(a, table, generation);
        command.mutation.table_id = table.table_id;
        command.mutation.storage_name = storage_name;
        command.topology = .{ .create = .{ .expected_transition_generation = generation, .table = table, .ranges = ranges } };
    } else if (request.create_table_json != null) return error.InvalidCatalogMutation;
    if (request.mutation.action == .set_tablespace and request.mutation.kind == .table) {
        const target: domain.Target = .{ .database = request.mutation.database, .namespace = request.mutation.namespace, .table = request.mutation.name };
        const current = (try store.resolveSystemCatalogTable(a, svc.metadata_group_id, target)) orelse return error.TableNotFound;
        const policy = admission.placement_policy;
        var replacement = current;
        replacement.placement_role = policy.placement_role orelse "data";
        replacement.desired_replica_count = policy.desired_replica_count orelse 3;
        replacement.min_ranges = policy.min_ranges orelse 1;
        command.placement_update = .{ .expected = current, .replacement = replacement };
    }
    try store.validateSystemCatalog(svc.metadata_group_id, command);
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_system_catalog = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const observed = store.systemCatalogMeta(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.last_command, &expected_hash)) return error.MetadataMutationOutcomeUnknown;
    if (command.topology) |topology| svc.verifyTableCreateProjection(a, topology.create.table, topology.create.ranges) catch return error.MetadataMutationOutcomeUnknown;
}

pub fn snapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext) ![]u8 {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var snapshot = try store.systemCatalogSnapshot(alloc, svc.metadata_group_id);
    defer snapshot.deinit();
    return std.json.Stringify.valueAlloc(alloc, snapshot.value, .{});
}

pub fn resolve(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, target: domain.Target) !?table_manager.TableRecord {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target);
}

pub fn call(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, input: domain.Call) ![]u8 {
    return switch (input) {
        .list_tables => |request| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const result = try store.listSystemCatalogTables(alloc, svc.metadata_group_id, request);
            errdefer alloc.free(result);
            try context.ensureActive();
            break :blk result;
        },
        .export_snapshot => blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.exportSystemCatalog(alloc, svc.metadata_group_id);
        },
        .read => |request| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            break :blk store.systemCatalogRead(alloc, svc.metadata_group_id, request);
        },
        .query_definition => |name| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const definition = try store.queryTableDefinition(alloc, svc.metadata_group_id, name);
            defer if (definition) |value| value.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, definition, .{});
        },
        .snapshot => snapshotJson(svc, alloc, context),
        .resolve => |target| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const table = try store.resolveSystemCatalogIdentity(alloc, svc.metadata_group_id, target);
            defer if (table) |value| value.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, table, .{});
        },
        .resolve_many => |request| blk: {
            try svc.ensureLinearizableReadWithContext(context);
            const store = svc.projectedStore() orelse return error.MissingMetadataStore;
            const result = try store.resolveSystemCatalogIdentities(alloc, svc.metadata_group_id, request);
            defer result.deinit(alloc);
            break :blk std.json.Stringify.valueAlloc(alloc, result, .{});
        },
        .mutate => |request| blk: {
            try mutate(svc, alloc, context, request);
            break :blk alloc.dupe(u8, "{}");
        },
    };
}

/// Publish the restored physical incarnation and its logical binding together.
/// Job retries are accepted only when both projections match exactly.
pub fn restore(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, target: domain.Target, table: table_manager.TableRecord, source_ranges: []const table_manager.RangeRecord) !void {
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.system_catalog_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const meta = try store.systemCatalogMeta(alloc, svc.metadata_group_id);
    const admission = try svc.captureTableRestoreAdmission(alloc, table);
    const ranges = try @import("../metadata/table_topology_mutations.zig").deriveRestoreDestinationRanges(alloc, table, source_ranges, admission.incarnation_generation);
    defer {
        for (ranges) |range| table_manager.freeRange(alloc, range);
        alloc.free(ranges);
    }
    if (admission.already_applied) {
        const bound = (try store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target)) orelse return error.MetadataMutationOutcomeUnknown;
        defer table_manager.freeTable(alloc, bound);
        if (bound.table_id != table.table_id) return error.TableAlreadyExists;
        return svc.verifyTableCreateProjection(alloc, table, ranges);
    }
    const command: storage.SystemCatalogCommand = .{
        .expected_revision = meta.revision,
        .mutation = .{ .action = .create, .kind = .table, .database = target.database, .namespace = target.namespace, .name = target.table, .table_id = table.table_id, .storage_name = table.name },
        .topology = .{ .create = .{ .expected_transition_generation = admission.expected_transition_generation, .table = table, .ranges = ranges } },
    };
    try store.validateSystemCatalog(svc.metadata_group_id, command);
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_system_catalog = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const bound = (store.resolveSystemCatalogTable(alloc, svc.metadata_group_id, target) catch return error.MetadataMutationOutcomeUnknown) orelse return error.MetadataMutationOutcomeUnknown;
    defer table_manager.freeTable(alloc, bound);
    if (bound.table_id != table.table_id) return error.MetadataMutationOutcomeUnknown;
    svc.verifyTableCreateProjection(alloc, table, ranges) catch return error.MetadataMutationOutcomeUnknown;
}
