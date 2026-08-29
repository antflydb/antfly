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

//! Table-topology mutations shared by the public leader path and authenticated
//! forwarding endpoint, with an explicit decoder-first rollout boundary.

const std = @import("std");
const operation = @import("../api/operation.zig");
const tables_api = @import("../api/tables.zig");
const metadata_authority = @import("authority.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_table_workflow = @import("table_workflow.zig");
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

fn lockTableWorkflowMutation(svc: anytype, table_name: []const u8) bool {
    const Service = @TypeOf(svc.*);
    if (comptime @hasDecl(Service, "lockTableWorkflowMutation")) {
        svc.lockTableWorkflowMutation(table_name);
        return true;
    }
    // The legacy control loop still takes the exclusive catalog lock. Test
    // doubles without a dedicated same-table lane retain that serialization.
    return false;
}

fn unlockTableWorkflowMutation(svc: anytype, table_name: []const u8, locked: bool) void {
    if (!locked) return;
    svc.unlockTableWorkflowMutation(table_name);
}

fn verifyCompatibleTableDropProjection(
    svc: anytype,
    alloc: std.mem.Allocator,
    table_id: u64,
    range_group_ids: []const u64,
) !void {
    try svc.verifyTableDropProjection(alloc, table_id);
    const Service = @TypeOf(svc.*);
    if (comptime @hasDecl(Service, "verifyTableDropRangesProjection"))
        try svc.verifyTableDropRangesProjection(alloc, range_group_ids);
}

pub const DropResult = topology_protocol.DropResult;

pub fn create(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
    req: tables_api.CreateTableRequest,
) !void {
    return switch (topology_protocol.atomic_table_topology_rollout) {
        .decoder_only => createCompatible(svc, alloc, request, table_name, req),
        .enabled => createAtomic(svc, alloc, request, table_name, req),
    };
}

/// Stage-A production path for the decoder rollout. It deliberately emits
/// only transition tags understood by the predecessor release. The per-table
/// lane prevents local DDL races; failures after reconciliation begins are
/// resolved by observing the exact projection and otherwise reported as
/// ambiguous so forwarding never blindly replays a partially applied plan.
fn createCompatible(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
    req: tables_api.CreateTableRequest,
) !void {
    var normalized_req = req;
    const expanded_indexes_json = try tables_api.expandSchemaDerivedAlgebraicIndexesAlloc(
        alloc,
        table_name,
        req.indexes_json orelse tables_api.default_indexes_json,
        tables_api.effectiveSchemaJson(req.schema_json),
    );
    defer alloc.free(expanded_indexes_json);
    normalized_req.indexes_json = expanded_indexes_json;

    const lane_locked = lockTableWorkflowMutation(svc, table_name);
    defer unlockTableWorkflowMutation(svc, table_name, lane_locked);
    try request.ensureActive();
    try svc.ensureLinearizableReadWithContext(request);
    const table = tables_api.deriveTableRecord(table_name, normalized_req);
    // Generation-salted identities require the atomic drop's durable fence.
    // Retain the predecessor identity derivation throughout decoder-only
    // rollout so mixed-version replicas project identical legacy commands.
    const ranges = try tables_api.deriveInitialRanges(alloc, table);
    defer {
        for (ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(ranges);
    }

    var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
    defer workflow.deinit();
    _ = workflow.createTableWithRanges(svc, table, ranges) catch |err| {
        svc.ensureLinearizableReadWithContext(request) catch return afterAdmission(err);
        svc.verifyTableCreateProjection(alloc, table, ranges) catch |verify_err| switch (verify_err) {
            error.TableAlreadyExists => return verify_err,
            else => return afterAdmission(err),
        };
        return;
    };
    svc.verifyTableCreateProjection(alloc, table, ranges) catch |err| switch (err) {
        error.TableAlreadyExists => return err,
        else => return afterAdmission(err),
    };
}

fn createAtomic(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
    req: tables_api.CreateTableRequest,
) !void {
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

pub fn drop(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
) !DropResult {
    return switch (topology_protocol.atomic_table_topology_rollout) {
        .decoder_only => dropCompatible(svc, alloc, request, table_name),
        .enabled => dropAtomic(svc, alloc, request, table_name),
    };
}

fn dropCompatible(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
) !DropResult {
    const lane_locked = lockTableWorkflowMutation(svc, table_name);
    defer unlockTableWorkflowMutation(svc, table_name, lane_locked);
    try request.ensureActive();
    try svc.ensureLinearizableReadWithContext(request);
    var admission = try svc.captureTableDropAdmission(alloc, table_name);
    defer admission.deinit(alloc);

    var workflow = metadata_table_workflow.TableWorkflow.init(alloc);
    defer workflow.deinit();
    if (workflow.dropTable(svc, admission.table_id)) |_| {} else |err| {
        svc.ensureLinearizableReadWithContext(request) catch return afterAdmission(err);
        verifyCompatibleTableDropProjection(
            svc,
            alloc,
            admission.table_id,
            admission.range_group_ids,
        ) catch
            return afterAdmission(err);
    }
    verifyCompatibleTableDropProjection(
        svc,
        alloc,
        admission.table_id,
        admission.range_group_ids,
    ) catch |err|
        return afterAdmission(err);

    const result = DropResult{
        .table_id = admission.table_id,
        .expected_transition_generation = admission.expected_transition_generation,
        .group_ids = admission.range_group_ids,
    };
    admission.range_group_ids = &.{};
    return result;
}

fn dropAtomic(
    svc: anytype,
    alloc: std.mem.Allocator,
    request: operation.RequestContext,
    table_name: []const u8,
) !DropResult {
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
