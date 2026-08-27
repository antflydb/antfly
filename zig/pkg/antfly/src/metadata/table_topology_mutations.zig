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

//! Exact, single-entry table-topology mutations shared by the public leader
//! path and the authenticated forwarding endpoint.

const std = @import("std");
const operation = @import("../api/operation.zig");
const tables_api = @import("../api/tables.zig");
const metadata_authority = @import("authority.zig");
const metadata_service = @import("service.zig");
const metadata_table_manager = @import("table_manager.zig");
const topology_protocol = @import("topology_protocol.zig");

fn afterAdmission(_: anyerror) anyerror {
    // Once Raft returned a receipt, no local failure can prove that the entry
    // was not committed. Fail closed so callers never blindly replay it.
    return error.MetadataMutationOutcomeUnknown;
}

fn findRangeByGroupId(
    ranges: []const metadata_table_manager.RangeRecord,
    group_id: u64,
) ?*const metadata_table_manager.RangeRecord {
    for (ranges) |*record| {
        if (record.group_id == group_id) return record;
    }
    return null;
}

fn runPostCommitControlRound(
    svc: *metadata_service.MetadataHttpService,
    operation_name: []const u8,
    table_name: []const u8,
) void {
    svc.runControlRoundOnly() catch |err| {
        // The exact receipt and projection check already proved the metadata
        // outcome. Background reconciliation remains responsible for
        // convergence; do not mislabel a committed mutation as ambiguous.
        std.log.warn(
            "table topology {s} committed; immediate reconciliation deferred table={s} err={s}",
            .{ operation_name, table_name, @errorName(err) },
        );
    };
}

pub const DropResult = topology_protocol.DropResult;

pub fn create(
    svc: *metadata_service.MetadataHttpService,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
    req: tables_api.CreateTableRequest,
) !void {
    svc.lockCatalogMutation();
    var catalog_locked = true;
    defer if (catalog_locked) svc.unlockCatalogMutation();
    try request.ensureActive();
    try svc.ensureTableTopologyProtocolReadyWithContext(request);
    const table = tables_api.deriveTableRecord(table_name, req);
    const ranges = try tables_api.deriveInitialRanges(alloc, table);
    defer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }

    // Everything through this call is provably pre-admission. In particular,
    // preserve NotLeader so the routing owner can safely rediscover the leader.
    try request.ensureActive();
    const receipt = svc.proposeTransitionCommandWithReceipt(.{
        .apply_table_topology = .{ .create = .{
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

    var snapshot = svc.adminSnapshot() catch |err| return afterAdmission(err);
    defer svc.freeAdminSnapshot(&snapshot);
    const projected = tables_api.findTableByName(&snapshot, table_name) orelse
        return error.MetadataMutationOutcomeUnknown;
    if (!metadata_table_manager.tableDefinitionsEqual(projected.*, table))
        return error.TableAlreadyExists;
    for (ranges) |expected| {
        const projected_range = findRangeByGroupId(snapshot.ranges, expected.group_id) orelse
            return error.MetadataMutationOutcomeUnknown;
        if (!metadata_table_manager.rangeRecordsEqual(projected_range.*, expected))
            return error.TableAlreadyExists;
    }
    svc.unlockCatalogMutation();
    catalog_locked = false;
    runPostCommitControlRound(svc, "create", table_name);
}

pub fn drop(
    svc: *metadata_service.MetadataHttpService,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
) !DropResult {
    svc.lockCatalogMutation();
    var catalog_locked = true;
    defer if (catalog_locked) svc.unlockCatalogMutation();
    try request.ensureActive();
    try svc.ensureTableTopologyProtocolReadyWithContext(request);
    try svc.ensureLinearizableReadWithContext(request);
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

    var projected = svc.adminSnapshot() catch |err| return afterAdmission(err);
    defer svc.freeAdminSnapshot(&projected);
    if (tables_api.findTableByName(&projected, table_name) != null)
        return error.TableTransitionActive;
    svc.unlockCatalogMutation();
    catalog_locked = false;
    runPostCommitControlRound(svc, "drop", table_name);
    const result = DropResult{
        .table_id = admission.table_id,
        .expected_transition_generation = admission.expected_transition_generation,
        .group_ids = admission.range_group_ids,
    };
    admission.range_group_ids = &.{};
    return result;
}
