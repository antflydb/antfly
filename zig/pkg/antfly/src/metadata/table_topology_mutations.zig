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
const metadata_api = @import("api.zig");
const metadata_authority = @import("authority.zig");
const metadata_service = @import("service.zig");
const metadata_table_manager = @import("table_manager.zig");

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

fn extensionOwnsTableScopedObject(
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
) bool {
    for (snapshot.extension_members) |member| {
        const member_table = if (member.table_name.len != 0)
            member.table_name
        else if (member.scope.kind == .table)
            member.scope.table_name
        else
            continue;
        if (std.mem.eql(u8, member_table, table_name)) return true;
    }
    return false;
}

pub fn create(
    svc: *metadata_service.MetadataHttpService,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
    req: tables_api.CreateTableRequest,
) !void {
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
    svc.runControlRoundOnly() catch |err| return afterAdmission(err);
}

pub fn drop(
    svc: *metadata_service.MetadataHttpService,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
) !void {
    try request.ensureActive();
    try svc.ensureTableTopologyProtocolReadyWithContext(request);
    try svc.ensureLinearizableReadWithContext(request);
    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);
    const table = tables_api.findTableByName(&snapshot, table_name) orelse return error.TableNotFound;
    if (extensionOwnsTableScopedObject(&snapshot, table_name)) return error.ExtensionOwnedObject;
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    const fence = try store.getTableTransitionFence(svc.metadata_group_id, table.table_id);
    if (fence.active()) return error.TableTransitionActive;

    var range_count: usize = 0;
    for (snapshot.ranges) |record| {
        if (record.table_id == table.table_id) range_count += 1;
    }
    const range_group_ids = try alloc.alloc(u64, range_count);
    defer alloc.free(range_group_ids);
    var range_index: usize = 0;
    for (snapshot.ranges) |record| {
        if (record.table_id != table.table_id) continue;
        range_group_ids[range_index] = record.group_id;
        range_index += 1;
    }
    std.sort.pdq(u64, range_group_ids, {}, std.sort.asc(u64));

    try request.ensureActive();
    const receipt = svc.proposeTransitionCommandWithReceipt(.{
        .apply_table_topology = .{ .drop = .{
            .table_id = table.table_id,
            .expected_name = table.name,
            .expected_transition_generation = fence.generation,
            .range_group_ids = range_group_ids,
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
    svc.runControlRoundOnly() catch |err| return afterAdmission(err);
}
