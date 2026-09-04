// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at
//
// https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations.

//! Atomic table-topology mutations shared by the public leader path and
//! authenticated forwarding endpoint.

const std = @import("std");
const operation = @import("../api/operation.zig");
const indexes_api = @import("../api/indexes.zig");
const tables_api = @import("../api/tables.zig");
const group_ids = @import("../common/group_ids.zig");
const managed_embedder = @import("../inference/managed_embedder.zig");
const metadata_authority = @import("authority.zig");
const metadata_table_manager = @import("table_manager.zig");
const topology_protocol = @import("topology_protocol.zig");

fn afterAdmission(err: anyerror) anyerror {
    // Once mutation admission begins, a local failure cannot prove that none
    // of its effects committed. Fail closed so callers never blindly replay.
    std.log.warn("metadata topology mutation outcome became ambiguous after admission err={s}", .{@errorName(err)});
    return error.MetadataMutationOutcomeUnknown;
}

fn lockTableCatalogMutation(svc: anytype, table_name: []const u8) void {
    const Service = @TypeOf(svc.*);
    if (comptime @hasDecl(Service, "lockTableCatalogMutation")) {
        svc.lockTableCatalogMutation(table_name);
    } else {
        // Keep transport test doubles and embedders source compatible. Real
        // metadata services provide the narrower per-table shared lane.
        svc.lockCatalogMutation();
    }
}

fn unlockTableCatalogMutation(svc: anytype, table_name: []const u8) void {
    const Service = @TypeOf(svc.*);
    if (comptime @hasDecl(Service, "unlockTableCatalogMutation")) {
        svc.unlockTableCatalogMutation(table_name);
    } else {
        svc.unlockCatalogMutation();
    }
}

pub const DropResult = topology_protocol.DropResult;

fn deriveRestoreDestinationRanges(
    alloc: std.mem.Allocator,
    table: metadata_table_manager.TableRecord,
    source_ranges: []const metadata_table_manager.RangeRecord,
    incarnation_generation: u64,
) ![]metadata_table_manager.RangeRecord {
    const destination = try alloc.alloc(metadata_table_manager.RangeRecord, source_ranges.len);
    var initialized: usize = 0;
    errdefer {
        for (destination[0..initialized]) |record|
            metadata_table_manager.freeRange(alloc, record);
        alloc.free(destination);
    }
    for (source_ranges, 0..) |source, index| {
        destination[index] = try metadata_table_manager.cloneRange(alloc, source);
        initialized += 1;
        // Legacy range records encode the document-identity namespace by
        // leaving these fields zero and falling back to the physical group
        // and range IDs. A restore intentionally creates fresh physical group
        // IDs, so materialize the source namespace before changing group_id.
        // Otherwise the restored DB retains the old namespace while metadata
        // silently starts expecting the new group ID, permanently fencing the
        // shard as DocIdentityNamespaceMismatch.
        destination[index].doc_identity_shard_id = metadata_table_manager.rangeDocIdentityShardId(source);
        destination[index].doc_identity_range_id = metadata_table_manager.rangeDocIdentityRangeId(source);
        destination[index].group_id = try tables_api.deriveInitialRangeGroupIdForGeneration(
            table.name,
            @intCast(index),
            @intCast(source_ranges.len),
            incarnation_generation,
        );
    }
    return destination;
}

test "restore preserves implicit source document identity across a new physical incarnation" {
    const alloc = std.testing.allocator;
    const table = metadata_table_manager.TableRecord{
        .table_id = 17,
        .name = "docs",
    };
    const source = [_]metadata_table_manager.RangeRecord{.{
        .group_id = 7001,
        .range_id = 7002,
        .table_id = table.table_id,
        .start_key = "",
    }};
    const destination = try deriveRestoreDestinationRanges(alloc, table, &source, 9);
    defer {
        for (destination) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(destination);
    }

    try std.testing.expect(destination[0].group_id != source[0].group_id);
    try std.testing.expectEqual(source[0].group_id, destination[0].doc_identity_shard_id);
    try std.testing.expectEqual(source[0].range_id, destination[0].doc_identity_range_id);
}

pub fn create(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
    req: tables_api.CreateTableRequest,
) !void {
    try request.ensureActive();
    // Schema-derived algebraic definitions are a public transport shape, not a
    // durable catalog shape. Normalize at the transport-neutral admission
    // boundary so embedded, HTTP-local, and forwarded callers cannot persist
    // different definitions for the same request.
    var normalized_req = req;
    const expanded_indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(
        alloc,
        table_name,
        req.indexes_json orelse tables_api.default_indexes_json,
        tables_api.effectiveSchemaJson(req.schema_json),
    );
    defer alloc.free(expanded_indexes_json);
    normalized_req.indexes_json = expanded_indexes_json;
    try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, expanded_indexes_json);
    try managed_embedder.validateEmbeddingProducerOwnershipJson(alloc, expanded_indexes_json);

    // Decoder capability probes perform remote I/O. Complete them before
    // entering the catalog lane, then revalidate their term/membership token
    // under the lane immediately before deriving the admission snapshot.
    const protocol_readiness = try svc.ensureTableTopologyProtocolReadyWithContext(
        request,
        topology_protocol.atomic_table_topology_version,
    );
    lockTableCatalogMutation(svc, table_name);
    var catalog_locked = true;
    defer if (catalog_locked) unlockTableCatalogMutation(svc, table_name);
    try request.ensureActive();
    const table = tables_api.deriveTableRecord(table_name, normalized_req);
    // Read the durable fence on the leader while holding the catalog mutation
    // lock. Its generation is both the apply precondition and the storage
    // incarnation salt, so a recreate cannot reuse paths owned by an earlier
    // drop even when post-commit cleanup is delayed or the caller crashes.
    try svc.ensureLinearizableReadWithContext(request);
    try svc.validateTableTopologyProtocolReadinessWithContext(request, protocol_readiness);
    const transition_generation = try svc.captureTableCreateGeneration(alloc, table.table_id);
    const ranges = try tables_api.deriveInitialRangesForGeneration(
        alloc,
        table,
        transition_generation,
    );
    defer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }

    // Everything through this call is provably pre-admission. In particular,
    // preserve NotLeader so the routing owner can safely rediscover the leader.
    try request.ensureActive();
    const receipt = svc.proposeTransitionCommandWithReceipt(.{
        .apply_table_topology = .{ .create = .{
            .expected_transition_generation = transition_generation,
            .table = table,
            .ranges = ranges,
        } },
    }) catch |err| {
        if (metadata_authority.isMutationNotAdmittedError(err)) return error.NotLeader;
        if (err == error.MetadataTopologyCommandTooLarge) return error.CreateTableRequestTooLarge;
        return err;
    };
    svc.waitForTransitionAppliedWithContext(receipt, request) catch |err|
        return afterAdmission(err);

    svc.verifyTableCreateProjection(alloc, table, ranges) catch |err| switch (err) {
        error.TableAlreadyExists => return err,
        else => return afterAdmission(err),
    };
    unlockTableCatalogMutation(svc, table_name);
    catalog_locked = false;
}

/// Atomically publishes a backup manifest as one fresh physical table
/// incarnation. Admission accepts only an absent destination or an exact retry
/// of this generation; every conflicting row is rejected before the tag-50
/// command is proposed.
pub fn restore(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table: metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
) !void {
    try request.ensureActive();
    if (table.table_id == 0 or table.name.len == 0 or
        ranges.len == 0 or ranges.len > topology_protocol.max_initial_ranges or
        ranges.len != @as(usize, table.min_ranges))
        return error.InvalidTableTopologyMutation;
    try indexes_api.validateArtifactEnrichmentsForTableIndexesJson(alloc, table.indexes_json);
    try managed_embedder.validateEmbeddingProducerOwnershipJson(alloc, table.indexes_json);
    metadata_table_manager.validateCompleteKeyspaceRanges(ranges) catch
        return error.InvalidTableTopologyMutation;
    var unique_groups = std.AutoHashMapUnmanaged(u64, void).empty;
    defer unique_groups.deinit(alloc);
    try unique_groups.ensureTotalCapacity(alloc, @intCast(ranges.len));
    for (ranges) |range| {
        group_ids.requireDataGroupId(range.group_id) catch
            return error.InvalidTableTopologyMutation;
        if (range.table_id != table.table_id or unique_groups.contains(range.group_id))
            return error.InvalidTableTopologyMutation;
        unique_groups.putAssumeCapacity(range.group_id, {});
    }
    const protocol_readiness = try svc.ensureTableTopologyProtocolReadyWithContext(
        request,
        topology_protocol.atomic_table_topology_version,
    );
    lockTableCatalogMutation(svc, table.name);
    var catalog_locked = true;
    defer if (catalog_locked) unlockTableCatalogMutation(svc, table.name);
    try request.ensureActive();
    try svc.ensureLinearizableReadWithContext(request);
    try svc.validateTableTopologyProtocolReadinessWithContext(request, protocol_readiness);
    const admission = try svc.captureTableRestoreAdmission(
        alloc,
        table,
    );
    const destination_ranges = try deriveRestoreDestinationRanges(
        alloc,
        table,
        ranges,
        admission.incarnation_generation,
    );
    defer {
        for (destination_ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(destination_ranges);
    }

    // A retry after an acknowledged or ambiguous commit observes the advanced
    // fence. Re-derive the preceding incarnation and require an exact catalog
    // projection instead of appending another no-op Raft entry.
    if (admission.already_applied) {
        try svc.verifyTableCreateProjection(alloc, table, destination_ranges);
        unlockTableCatalogMutation(svc, table.name);
        catalog_locked = false;
        return;
    }

    try request.ensureActive();
    const receipt = svc.proposeTransitionCommandWithReceipt(.{
        .apply_table_topology = .{ .create = .{
            .expected_transition_generation = admission.expected_transition_generation,
            .table = table,
            .ranges = destination_ranges,
        } },
    }) catch |err| {
        if (metadata_authority.isMutationNotAdmittedError(err)) return error.NotLeader;
        return err;
    };
    svc.waitForTransitionAppliedWithContext(receipt, request) catch |err|
        return afterAdmission(err);
    svc.verifyTableCreateProjection(alloc, table, destination_ranges) catch |err| switch (err) {
        error.TableAlreadyExists => return err,
        else => return afterAdmission(err),
    };
    unlockTableCatalogMutation(svc, table.name);
    catalog_locked = false;
}

pub fn drop(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
) !DropResult {
    try request.ensureActive();
    const protocol_readiness = try svc.ensureTableTopologyProtocolReadyWithContext(
        request,
        topology_protocol.atomic_table_topology_version,
    );
    lockTableCatalogMutation(svc, table_name);
    var catalog_locked = true;
    defer if (catalog_locked) unlockTableCatalogMutation(svc, table_name);
    try request.ensureActive();
    try svc.ensureLinearizableReadWithContext(request);
    try svc.validateTableTopologyProtocolReadinessWithContext(request, protocol_readiness);
    var admission = try svc.captureTableDropAdmission(alloc, table_name);
    defer admission.deinit(alloc);

    try request.ensureActive();
    const receipt = svc.proposeTransitionCommandWithReceipt(.{
        .apply_table_topology = .{ .drop = .{
            .table_id = admission.table_id,
            .expected_name = admission.expected_name,
            .expected_transition_generation = admission.expected_transition_generation,
            .range_contract = .{ .membership = admission.range_membership },
        } },
    }) catch |err| {
        if (metadata_authority.isMutationNotAdmittedError(err)) return error.NotLeader;
        return err;
    };
    svc.waitForTransitionAppliedWithContext(receipt, request) catch |err|
        return afterAdmission(err);

    svc.verifyTableDropProjection(alloc, admission.table_id) catch |err| switch (err) {
        error.TableTransitionActive => return err,
        else => return afterAdmission(err),
    };
    unlockTableCatalogMutation(svc, table_name);
    catalog_locked = false;
    const result = DropResult{
        .table_id = admission.table_id,
        .expected_transition_generation = admission.expected_transition_generation,
        .group_ids = admission.range_group_ids,
    };
    admission.range_group_ids = &.{};
    return result;
}
