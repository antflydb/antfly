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

//! Transport-independent native catalog admission, capability fencing, and
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
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.native_catalog_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var snapshot = try store.nativeCatalogSnapshot(alloc, svc.metadata_group_id);
    defer snapshot.deinit();
    var command: storage.NativeCatalogCommand = .{ .expected_revision = snapshot.meta.revision, .mutation = request.mutation };
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    if (request.mutation.action == .create and request.mutation.kind == .table) {
        const json = request.create_table_json orelse return error.InvalidCatalogMutation;
        var req = try tables_api.parseStoredCreateTableRequest(a, json);
        const storage_name = request.physical_name orelse try std.fmt.allocPrint(a, "table:{d}", .{snapshot.meta.next_id});
        if (!std.mem.startsWith(u8, storage_name, "table:") or storage_name.len > 512) return error.InvalidCatalogMutation;
        req.indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(a, storage_name, req.indexes_json orelse tables_api.default_indexes_json, tables_api.effectiveSchemaJson(req.schema_json));
        try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(a, req.indexes_json.?);
        try managed_embedder.validateEmbeddingProducerOwnershipJson(a, req.indexes_json.?);
        var table = tables_api.deriveTableRecord(storage_name, req);
        const namespace = try snapshot.value.namespaceFor(request.mutation.database, request.mutation.namespace);
        const explicit = if (request.mutation.tablespace) |name| (snapshot.value.find(.tablespace, 0, name) orelse return error.TablespaceNotFound).id else 0;
        if (try snapshot.value.effectiveTablespace(namespace, explicit)) |tablespace| {
            try tablespace.placement_policy.validate();
            if (tablespace.placement_policy.placement_role) |role| table.placement_role = role;
            if (tablespace.placement_policy.desired_replica_count) |count| table.desired_replica_count = count;
            if (req.num_shards == null) if (tablespace.placement_policy.min_ranges) |count| {
                table.min_ranges = count;
            };
        }
        const generation = try svc.captureTableCreateGeneration(a, table.table_id);
        const ranges = try tables_api.deriveInitialRangesForGeneration(a, table, generation);
        command.mutation.table_id = table.table_id;
        command.mutation.storage_name = storage_name;
        command.topology = .{ .create = .{ .expected_transition_generation = generation, .table = table, .ranges = ranges } };
    } else if (request.create_table_json != null) return error.InvalidCatalogMutation;
    if (request.mutation.action == .set_tablespace and request.mutation.kind == .table) {
        const target: domain.Target = .{ .database = request.mutation.database, .namespace = request.mutation.namespace, .table = request.mutation.name };
        const current = (try store.resolveNativeCatalogTable(a, svc.metadata_group_id, target)) orelse return error.TableNotFound;
        const namespace = try snapshot.value.namespaceFor(target.database, target.namespace);
        const explicit = if (request.mutation.tablespace) |name| (snapshot.value.find(.tablespace, 0, name) orelse return error.TablespaceNotFound).id else 0;
        const policy = if (try snapshot.value.effectiveTablespace(namespace, explicit)) |tablespace| tablespace.placement_policy else domain.PlacementPolicy{};
        var replacement = current;
        replacement.placement_role = policy.placement_role orelse "data";
        replacement.desired_replica_count = policy.desired_replica_count orelse 3;
        replacement.min_ranges = policy.min_ranges orelse 1;
        command.placement_update = .{ .expected = current, .replacement = replacement };
    }
    try store.validateNativeCatalog(svc.metadata_group_id, command);
    const bytes = try std.json.Stringify.valueAlloc(a, command, .{});
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_native_catalog = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    var observed = store.nativeCatalogSnapshot(alloc, svc.metadata_group_id) catch return error.MetadataMutationOutcomeUnknown;
    defer observed.deinit();
    var expected_hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected_hash, .{});
    if (observed.meta.revision != command.expected_revision + 1 or !std.mem.eql(u8, &observed.meta.last_command, &expected_hash)) return error.MetadataMutationOutcomeUnknown;
    if (command.topology) |topology| svc.verifyTableCreateProjection(a, topology.create.table, topology.create.ranges) catch return error.MetadataMutationOutcomeUnknown;
}

pub fn snapshotJson(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext) ![]u8 {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var snapshot = try store.nativeCatalogSnapshot(alloc, svc.metadata_group_id);
    defer snapshot.deinit();
    return std.json.Stringify.valueAlloc(alloc, snapshot.value, .{});
}

pub fn resolve(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, target: domain.Target) !?table_manager.TableRecord {
    try svc.ensureLinearizableReadWithContext(context);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return store.resolveNativeCatalogTable(alloc, svc.metadata_group_id, target);
}

pub fn call(svc: anytype, alloc: std.mem.Allocator, context: operation.RequestContext, input: domain.Call) ![]u8 {
    return switch (input) {
        .snapshot => snapshotJson(svc, alloc, context),
        .resolve => |target| blk: {
            const table = try resolve(svc, alloc, context, target);
            defer if (table) |value| table_manager.freeTable(alloc, value);
            break :blk std.json.Stringify.valueAlloc(alloc, table, .{});
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
    const readiness = try svc.ensureTableTopologyProtocolReadyWithContext(context, protocol.native_catalog_version);
    svc.lockCatalogMutation();
    defer svc.unlockCatalogMutation();
    try svc.ensureLinearizableReadWithContext(context);
    try svc.validateTableTopologyProtocolReadinessWithContext(context, readiness);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    var snapshot = try store.nativeCatalogSnapshot(alloc, svc.metadata_group_id);
    defer snapshot.deinit();
    const admission = try svc.captureTableRestoreAdmission(alloc, table);
    const ranges = try @import("../metadata/table_topology_mutations.zig").deriveRestoreDestinationRanges(alloc, table, source_ranges, admission.incarnation_generation);
    defer {
        for (ranges) |range| table_manager.freeRange(alloc, range);
        alloc.free(ranges);
    }
    if (admission.already_applied) {
        const bound = (try store.resolveNativeCatalogTable(alloc, svc.metadata_group_id, target)) orelse return error.MetadataMutationOutcomeUnknown;
        defer table_manager.freeTable(alloc, bound);
        if (bound.table_id != table.table_id) return error.TableAlreadyExists;
        return svc.verifyTableCreateProjection(alloc, table, ranges);
    }
    const command: storage.NativeCatalogCommand = .{
        .expected_revision = snapshot.meta.revision,
        .mutation = .{ .action = .create, .kind = .table, .database = target.database, .namespace = target.namespace, .name = target.table, .table_id = table.table_id, .storage_name = table.name },
        .topology = .{ .create = .{ .expected_transition_generation = admission.expected_transition_generation, .table = table, .ranges = ranges } },
    };
    try store.validateNativeCatalog(svc.metadata_group_id, command);
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > domain.max_command_bytes) return error.CatalogCommandTooLarge;
    try context.ensureActive();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_native_catalog = bytes });
    svc.waitForTransitionAppliedWithContext(receipt, context) catch return error.MetadataMutationOutcomeUnknown;
    const bound = (store.resolveNativeCatalogTable(alloc, svc.metadata_group_id, target) catch return error.MetadataMutationOutcomeUnknown) orelse return error.MetadataMutationOutcomeUnknown;
    defer table_manager.freeTable(alloc, bound);
    if (bound.table_id != table.table_id) return error.MetadataMutationOutcomeUnknown;
    svc.verifyTableCreateProjection(alloc, table, ranges) catch return error.MetadataMutationOutcomeUnknown;
}
