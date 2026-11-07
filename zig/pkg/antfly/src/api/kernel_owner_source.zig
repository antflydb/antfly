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

//! Provisioned group-local read/write adapters for the compiled storage owner.
//! Distributed sources retain routing, admission, consistency, aggregation,
//! and lifecycle; this source owns only coarse physical operations.

const server_coordinated_ttl = @import("../storage/server_coordinated_ttl.zig");
const std = @import("std");
const request_operation = @import("antfly_local_sources").api_operation;
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;
const abi = @import("kernel_owner_abi");
const kernel_error_identity = @import("kernel_error_identity");
const client = @import("../storage/kernel_owner_client.zig");
const data_apply_client = @import("../storage/data_raft_apply_client.zig");
const descriptor_contract = @import("../storage/kernel_owner_descriptor.zig");
const backend_types = @import("antfly_local_sources").storage_backend_types;
const db_types = @import("antfly_local_sources").storage_db_types;
const runtime_callbacks = @import("antfly_local_sources").storage_db_runtime_callbacks;
const replication_contract = @import("antfly_local_sources").storage_db_replication_contract;
const document_artifact_child_range = @import("antfly_local_sources").storage_db_document_artifact_child_range;
const text_memory = @import("antfly_local_sources").storage_db_text_memory_stats;
const replication_effects = @import("antfly_local_sources").storage_db_replication_effects;
const hot_standby_replication_record = @import("antfly_local_sources").storage_db_replication_record;
const runtime_preflight = @import("antfly_local_sources").storage_db_runtime_preflight;
const metadata_api = @import("../metadata/api.zig");
const metadata_domain = @import("../metadata/domain.zig");
const backup_contract = @import("antfly_local_sources").api_backup_contract;
const distributed_graph = @import("distributed_graph.zig");
const query_response = @import("antfly_local_sources").api_query_response;
const runtime_status = @import("antfly_local_sources").api_runtime_status;
const restore_state_contract = @import("../storage/restore_state_contract.zig");
const read_gate = @import("../raft/read_gate.zig");
const feature_reads = @import("../raft/feature_reads.zig");
const table_catalog = @import("table_catalog.zig");
const table_read_source = @import("antfly_local_sources").api_table_read_source;
const table_reads = @import("antfly_local_sources").api_local_query_contract;
const storage_snapshot_source = @import("storage_snapshot_source.zig");
const storage_maintenance_source = @import("storage_maintenance_source.zig");
const table_write_source = @import("antfly_local_sources").api_table_write_source;
const table_writes = @import("antfly_source_root").antfly_sources.table_writes;
const transaction_recovery_source = @import("transaction_recovery_source.zig");
const common_config = @import("antfly_local_sources").common_config;
const native_index_control = @import("antfly_local_sources").storage_db_relational_index_owner_control;
const raft_index_control = @import("antfly_local_sources").storage_db_relational_index_raft_contract;
const snapshot_retention = @import("../raft/storage/snapshot_retention_source.zig");
const index_owner_job = @import("antfly_local_sources").storage_db_relational_index_owner_job;
const scraping = @import("antfly_scraping");

/// Compiled owner operations use the platform monotonic clock. In particular on
/// Darwin that clock is not std.Io's awake clock. Translate the remaining
/// budget once, before both cold owner acquisition and the compiled boundary.
fn platformDeadlineContext(context: request_operation.RequestContext) !request_operation.RequestContext {
    return context.platformDeadline();
}

/// Both terminal operations must check the accepted generation on their
/// applying lease before calling the native boundary, never after fence lift.
fn requireAcceptedNativeRootGeneration(actual: u64, expected: u64) !void {
    if (expected == 0 or try indexRootEpoch(actual) != expected) return error.PrivateIndexRootRecoveryRequired;
}

fn indexRootEpoch(selection: u64) !u64 {
    return @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(selection) catch error.PrivateIndexRootRecoveryRequired;
}

test "native index owner adapter rejects cancellation deadline and foreign group before acquisition" {
    // Deliberately uninitialized: all failures must precede catalog/owner access.
    var source: ProvisionedKernelOwnerSource = undefined;
    const identity: native_index_control.Identity = .{
        .id = @splat(1),
        .plan_digest = @splat(2),
        .owner_digest = @splat(3),
        .ordinal = 0,
        .namespace = .{ .table_id = 101, .shard_id = 102, .range_id = 103 },
        .owner_group_id = 7,
    };
    var signal = std.atomic.Value(bool).init(true);
    const cancelled: request_operation.RequestContext = .{
        .cancellation = request_operation.CancellationToken.fromAtomic(&signal),
    };
    const expired: request_operation.RequestContext = .{ .deadline_ns = 0 };
    inline for (.{ cancelled, expired }) |context| {
        const expected = if (context.deadline_ns != null) error.DeadlineExceeded else error.Canceled;
        try std.testing.expectError(expected, source.raftIndexOwnerObservationAtRoot(std.testing.allocator, 7, "rows", identity, context));
        try std.testing.expectError(expected, source.acquireRaftIndexOwnerSession(7, "rows", identity, context));
        try std.testing.expectError(expected, source.raftIndexOwnerPreparationAtRoot(std.testing.allocator, 7, "rows", undefined, "manifest", context));
        try std.testing.expectError(expected, source.nativeIndexOwnerState(std.testing.allocator, 7, "rows", identity, context));
        try std.testing.expectError(expected, source.nativeSchemaOwnerObservationAtRoot(7, "rows", undefined, context));
        try std.testing.expectError(expected, source.attestSchemaPreparationAtRoot(7, "rows", undefined, undefined, undefined, undefined, context));
        try std.testing.expectError(expected, source.captureSchemaPreparationAtRoot(std.testing.failing_allocator, 7, "rows", undefined, "", "", .{ .not_null = &.{} }, undefined, undefined, context));
        try std.testing.expectError(expected, source.attestNativeSchemaOwnerAtRoot(7, "rows", undefined, undefined, undefined, context));
        try std.testing.expectError(expected, source.raftSchemaOwnerObservationAtRoot(7, "rows", undefined, context));
        try std.testing.expectError(expected, source.stepSchemaOwnerAuditAtRoot(7, "rows", undefined, "", undefined, .{}, context));
        try std.testing.expectError(expected, source.attestRaftSchemaOwnerAtRoot(7, "rows", undefined, undefined, undefined, context));
        try std.testing.expectError(expected, source.applyNativeSchemaOwnerControlAtRoot(7, "rows", undefined, 1, std.time.ns_per_s, undefined, context));
        try std.testing.expectError(expected, source.readNativeIndexClaimSourceAtRoot(std.testing.allocator, 7, "rows", identity, context));
        try std.testing.expectError(expected, source.nativeIndexOwnerReceipt(std.testing.allocator, 7, "rows", .{ .identity = identity, .effect = .fenced }, context));
        try std.testing.expectError(expected, source.nativeIndexOwnerReceiptAtRoot(std.testing.allocator, 7, "rows", .{ .identity = identity, .effect = .fenced }, context));
        try std.testing.expectError(expected, source.admitNativeIndexOwner(std.testing.allocator, 7, "rows", undefined, "manifest", context));
        try std.testing.expectError(expected, source.applyNativeIndexOwnerControl(std.testing.allocator, 7, "rows", undefined, context));
        try std.testing.expectError(expected, source.applyNativeIndexOwnerControlAtRoot(std.testing.allocator, 7, "rows", undefined, context));
        try std.testing.expectError(expected, source.applyNativeIndexOwnerClaims(std.testing.allocator, 7, "rows", undefined, context));
        try std.testing.expectError(expected, source.applyNativeIndexOwnerClaimsAtRoot(std.testing.allocator, 7, "rows", undefined, context));
        try std.testing.expectError(expected, source.consumeNativeIndexSourceAtRoot(std.testing.allocator, 7, "rows", undefined, context));
        try std.testing.expectError(expected, source.stepNativeIndexOwnerInstallationAtRoot(std.testing.allocator, 7, "rows", undefined, 1, context));
        try std.testing.expectError(expected, source.applyNativeIndexOwnerReleaseAtRoot(std.testing.allocator, 7, "rows", undefined, 1, 1, context));
        try std.testing.expectError(expected, source.completeNativeIndexOwnerCancellationAtRoot(std.testing.allocator, 7, "rows", identity, 1, context));
        try std.testing.expectError(expected, source.applyNativeIndexOwnerCancellationReleaseAtRoot(std.testing.allocator, 7, "rows", undefined, 1, 1, context));
        try std.testing.expectError(expected, source.cancelNativeIndexOwnerIntent(std.testing.allocator, 7, "rows", undefined, context));
        try std.testing.expectError(expected, source.cleanupNativeIndexOwner(std.testing.allocator, 7, "rows", identity, context));
        try std.testing.expectError(expected, source.collectNativeIndexOwnerGarbage(std.testing.allocator, 7, "rows", identity, context));
        try std.testing.expectError(expected, source.collectNativeIndexOwnerClaimGarbage(std.testing.allocator, 7, "rows", identity, context));
        try std.testing.expectError(expected, source.admitCanceledNativeIndexOwner(std.testing.allocator, 7, "rows", undefined, "manifest", context));
        try std.testing.expectError(expected, source.applyCanceledNativeIndexOwnerControl(std.testing.allocator, 7, "rows", undefined, context));
        try std.testing.expectError(expected, source.nativeIndexOwnerCancellationState(std.testing.allocator, 7, "rows", identity, context));
    }
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, source.nativeIndexOwnerState(std.testing.allocator, 8, "rows", identity, .{}));
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, source.raftIndexOwnerObservationAtRoot(std.testing.allocator, 8, "rows", identity, .{}));
    try std.testing.expectError(error.InvalidPrivateIndexInputs, source.admitNativeIndexOwner(std.testing.allocator, 7, "rows", undefined, "", .{}));
}

test "distributed txn native lookup read-index rejects leader loss before storage execution" {
    const Barrier = struct {
        calls: usize = 0,
        fn wait(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.NotLeader;
        }
    };
    var barrier: Barrier = .{};
    // The gate must fail before touching any owner/catalog/storage fields.
    var source: ProvisionedKernelOwnerSource = undefined;
    source.read_safety_barrier = .{ .ptr = &barrier, .vtable = &.{ .wait_read_safe = Barrier.wait } };
    const reader = source.readSource();
    try std.testing.expect(reader.strict_read_index_absence);
    try std.testing.expectError(error.NotLeader, reader.lookupGroupLocal(std.testing.allocator, 7, "rows", "missing", .{}, .read_index));
    try std.testing.expectEqual(@as(usize, 1), barrier.calls);
    try source.prepareLookupRead(7, "missing", .{}, .stale);
    try std.testing.expectEqual(@as(usize, 1), barrier.calls);
}

test "SQL retained native scan rejects leader loss without stale retry" {
    const Barrier = struct {
        calls: usize = 0,
        fn wait(ptr: *anyopaque, _: u64, _: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.NotLeader;
        }
    };
    var barrier: Barrier = .{};
    var source: ProvisionedKernelOwnerSource = undefined;
    source.read_safety_barrier = .{ .ptr = &barrier, .vtable = &.{ .wait_read_safe = Barrier.wait } };
    try std.testing.expectError(error.NotLeader, source.prepareRetainedScanRead(7, "", "", .{}, .read_index));
    try std.testing.expectEqual(@as(usize, 1), barrier.calls);
    // Already-prepared local routing explicitly requests stale; only that
    // caller-provided consistency may omit an additional read-index barrier.
    try source.prepareRetainedScanRead(7, "", "", .{}, .stale);
    try std.testing.expectEqual(@as(usize, 1), barrier.calls);
}

test "source owner deadlines normalize executor clock epochs without extending budgets" {
    const FakeClock = struct {
        fn now(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const value: *const u64 = @ptrCast(@alignCast(raw.?));
            return .{ .nanoseconds = value.* };
        }
    };
    var clock_now: u64 = 10;
    var vtable = std.testing.io.vtable.*;
    vtable.now = FakeClock.now;
    const io: std.Io = .{ .userdata = &clock_now, .vtable = &vtable };
    const context: request_operation.RequestContext = .{ .deadline_ns = 10 + std.time.ns_per_s, .deadline_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&io) };
    const before = platform_time.monotonicNs();
    const normalized = try platformDeadlineContext(context);
    const after = platform_time.monotonicNs();
    try std.testing.expect(normalized.deadline_io == null);
    try std.testing.expect(normalized.deadline_ns.? >= before + std.time.ns_per_s);
    try std.testing.expect(normalized.deadline_ns.? <= after + std.time.ns_per_s);
    try normalized.ensureActive();
    clock_now = context.deadline_ns.?;
    try std.testing.expectError(error.DeadlineExceeded, platformDeadlineContext(context));
    try std.testing.expect((try platformDeadlineContext(.{})).deadline_ns == null);
    const native_context: request_operation.RequestContext = .{ .deadline_ns = after + std.time.ns_per_s };
    try std.testing.expectEqual(native_context.deadline_ns, (try platformDeadlineContext(native_context)).deadline_ns);
}

test "source owner routed admission preserves the fence clock" {
    const Fixture = struct {
        now: u64,
        io: std.Io = undefined,
        calls: usize = 0,
        fn clock(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return .{ .nanoseconds = self.now };
        }
        fn resolve(raw: *anyopaque, _: std.mem.Allocator, _: []const u8, _: table_catalog.RouteQuery, deadline: ?u64) !table_catalog.RouteResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try table_catalog.RoutingBudget.initIo(deadline, self.io).checkpoint();
            try std.testing.expectEqual(self.now + std.time.ns_per_s, deadline.?);
            self.calls += 1;
            return .not_found;
        }
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.TestUnexpectedResult;
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    var vtable = std.testing.io.vtable.*;
    vtable.now = Fixture.clock;
    var request: Fixture = .{ .now = 10 };
    request.io = .{ .userdata = &request, .vtable = &vtable };
    var catalog: Fixture = .{ .now = 1000 * std.time.ns_per_s };
    catalog.io = .{ .userdata = &catalog, .vtable = &vtable };
    var source: ProvisionedKernelOwnerSource = undefined;
    source.catalog = .{ .ptr = &catalog, .io = @import("antfly_runtime_abi").io_abi.Borrow.init(&catalog.io), .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.free, .validate_route = Fixture.resolve } };
    const fence: metadata_api.CatalogRouteFence = .{
        .metadata_group_id = 1,
        .catalog_revision = 1,
        .table_id = 1,
        .topology_epoch = 1,
        .route = .{ .group_id = 2, .range_id = 2, .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 } },
        .admission_deadline_ns = request.now + std.time.ns_per_s,
        .admission_deadline_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&request.io),
    };
    try std.testing.expectError(error.TopologyChanged, source.validateRoutedRead(std.testing.allocator, fence, 2, "rows"));
    try std.testing.expectEqual(@as(usize, 1), catalog.calls);
    try std.testing.expectError(error.TopologyChanged, ProvisionedKernelOwnerSource.openStatementSnapshotRouted(&source, std.testing.allocator, fence, 2, "rows", .read_index, null, fence.admission_deadline_ns));
    try std.testing.expectEqual(@as(usize, 2), catalog.calls);
    request.now = fence.admission_deadline_ns.?;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, source.validateRoutedRead(std.testing.allocator, fence, 2, "rows"));
    try std.testing.expectEqual(@as(usize, 2), catalog.calls);
}

test "source owner lookup and descriptor admission preserve one cancellable catalog budget" {
    const Fixture = struct {
        request_now: u64 = 10,
        catalog_now: u64 = 1000 * std.time.ns_per_s,
        canceled: std.atomic.Value(bool) = .init(false),
        mode: enum { confirm, cancel, expire } = .confirm,
        captures: usize = 0,
        confirmations: usize = 0,
        fn now(ptr: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const value: *const u64 = @ptrCast(@alignCast(ptr.?));
            return .{ .nanoseconds = value.* };
        }
        fn capture(ptr: *anyopaque, _: []const u8, deadline: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(?u64, 1001 * std.time.ns_per_s), deadline);
            self.captures += 1;
            const elapsed: u64 = if (self.mode == .expire) std.time.ns_per_s else 250 * std.time.ns_per_ms;
            self.request_now += elapsed;
            self.catalog_now += elapsed;
            if (self.mode == .cancel) self.canceled.store(true, .release);
            return .{ .metadata_group_id = 1, .catalog_revision = 1, .tables = &.{}, .ranges = &.{} };
        }
        fn confirm(ptr: *anyopaque, _: []const u8, deadline: ?u64) !metadata_api.CatalogRoutingSnapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            // The eventual miss used 250 ms. Confirmation keeps the original
            // absolute deadline, rather than receiving a fresh second.
            try std.testing.expectEqual(@as(?u64, 1001 * std.time.ns_per_s), deadline);
            self.confirmations += 1;
            return error.DescriptorProbeComplete;
        }
        fn freeRouting(_: *anyopaque, _: *metadata_api.CatalogRoutingSnapshot) void {}
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnbudgetedDescriptorFallback;
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    var vtable = std.testing.io.vtable.*;
    vtable.now = Fixture.now;
    for ([_]@FieldType(Fixture, "mode"){ .confirm, .cancel, .expire }) |mode| {
        for ([_]bool{ false, true }) |lookup| {
            var fixture: Fixture = .{ .mode = mode };
            const request_io: std.Io = .{ .userdata = &fixture.request_now, .vtable = &vtable };
            const catalog_io: std.Io = .{ .userdata = &fixture.catalog_now, .vtable = &vtable };
            var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "unused", .{ .ptr = &fixture, .io = @import("antfly_runtime_abi").io_abi.Borrow.init(&catalog_io), .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.free, .table_routing_snapshot = Fixture.capture, .linearizable_table_routing_snapshot = Fixture.confirm, .free_routing_snapshot = Fixture.freeRouting } }, read_gate.alreadyReadSafeBarrier());
            const opts: db_types.LookupOptions = .{ .execution_deadline_ns = fixture.request_now + std.time.ns_per_s, .execution_io = @import("antfly_runtime_abi").io_abi.Borrow.init(&request_io), .cancellation = .fromAtomic(&fixture.canceled) };
            const expected: anyerror = switch (mode) {
                .confirm => error.DescriptorProbeComplete,
                .cancel => error.Canceled,
                .expire => error.CatalogRoutingSnapshotTimeout,
            };
            if (lookup) {
                try std.testing.expectError(expected, ProvisionedKernelOwnerSource.lookupGroupLocal(&source, std.testing.allocator, 7, "rows", "a", opts, .stale));
            } else {
                try std.testing.expectError(expected, source.acquireWithControls(7, "rows", .from(opts)));
            }
            try std.testing.expectEqual(@as(usize, 1), fixture.captures);
            try std.testing.expectEqual(@as(usize, if (mode == .confirm) 1 else 0), fixture.confirmations);
        }
    }
}

test "source owner fenced descriptor disappearance is availability not absence" {
    const Fixture = struct {
        fn resolve(_: *anyopaque, alloc: std.mem.Allocator, _: []const u8, _: table_catalog.RouteQuery, _: ?u64) !table_catalog.RouteResult {
            const groups = try alloc.alloc(table_catalog.CatalogGroupRoute, 1);
            groups[0] = .{ .group_id = 7, .range_id = 7, .identity_namespace = .{ .table_id = 1, .shard_id = 7, .range_id = 7 } };
            return .{ .found = .{ .metadata_group_id = 1, .metadata_incarnation = null, .catalog_revision = 1, .table_id = 1, .topology_epoch = 1, .groups = groups } };
        }
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &.{}, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    var fixture: u8 = 0;
    var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "unused", .{ .ptr = &fixture, .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.free, .validate_route = Fixture.resolve } }, read_gate.alreadyReadSafeBarrier());
    const fence: metadata_api.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 1, .topology_epoch = 1, .route = .{ .group_id = 7, .range_id = 7, .identity_namespace = .{ .table_id = 1, .shard_id = 7, .range_id = 7 } } };
    // The authenticated public route exists. A later descriptor projection
    // cannot certify that its row is absent without admitting an owner.
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, ProvisionedKernelOwnerSource.lookupGroupLocalRouted(&source, std.testing.allocator, fence, 7, "rows", "a", .{}, .read_index));
    // An unfenced physical lookup still identifies a genuinely missing table.
    try std.testing.expectError(error.TableNotFound, ProvisionedKernelOwnerSource.lookupGroupLocal(&source, std.testing.allocator, 7, "rows", "a", .{}, .read_index));
}

test "SQL relation lifetime draining admits only exact retained external host control" {
    const Fixture = struct {
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogRead;
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    var fixture: u8 = 0;
    var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "unused", .{ .ptr = &fixture, .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.free } }, read_gate.alreadyReadSafeBarrier());
    const native: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 7, .owner_store_id = 11, .generation_instance = @splat(8), .data_generation = 1 };
    const route: metadata_api.CatalogRouteFence = .{ .metadata_group_id = 1, .catalog_revision = 1, .table_id = 1, .topology_epoch = 1, .route = .{ .group_id = 7, .range_id = 7, .identity_namespace = .{ .table_id = 1, .shard_id = 7, .range_id = 7 }, .execution = .{ .native_local = native } } };
    const owner: @import("antfly_local_sources").storage_transactions.ExternalDecisionOwner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 100 };
    source.sql_persistence_host = .{ .node_id = 7, .store_id = 11, .execution_instance = @splat(8), .generation_instance = @splat(8), .data_generation = 1 };
    source.sql_persistence_ready = true;
    try source.validateNativeRoute(route);
    source.sql_persistence_ready = false;
    source.sql_persistence_draining = true;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.validateNativeRoute(route));
    try source.validateNativeDecisionHost(route, owner);
    var replaced = route;
    replaced.route.execution.native_local.owner_node_id += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.validateNativeDecisionHost(replaced, owner));
    replaced = route;
    replaced.route.execution.native_local.owner_store_id += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.validateNativeDecisionHost(replaced, owner));
    source.sql_persistence_draining = false;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.validateNativeDecisionHost(route, owner));
}

pub const ProvisionedKernelOwnerSource = struct {
    native_terminal_authority: ?struct {
        ptr: *anyopaque,
        observe_fn: *const fn (*anyopaque, std.mem.Allocator, []const u8, metadata_api.CatalogRouteFence, [16]u8, @import("../system_catalog/prepared_transactions.zig").Owner, request_operation.RequestContext) anyerror!@import("../system_catalog/prepared_transactions.zig").Decision,
        complete_ack_fn: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, metadata_api.CatalogRouteFence, [16]u8, @import("../system_catalog/prepared_transactions.zig").Owner, []const []const u8, request_operation.RequestContext) anyerror!void = null,
    } = null,
    native_lock_authority: ?struct {
        ptr: *anyopaque,
        authorize_fn: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, @import("antfly_local_sources").storage_pessimistic_lock_control.Command, request_operation.RequestContext) anyerror!@import("antfly_local_sources").storage_pessimistic_lock_control.Command,
    } = null,

    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    catalog: table_catalog.CatalogSource,
    read_safety_barrier: read_gate.ReadSafetyBarrier,
    /// Selected once by the hosting runtime before any owner is opened.
    online_source_authority: @import("antfly_local_sources").storage_db_online_source_contract.Authority = .raft,
    /// Actual local data-process admission. The SQL connection issuer may be
    /// another node and is independently bound in Descriptor.owner.
    sql_persistence_host: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Context = null,
    sql_persistence_boot: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Boot = null,
    sql_persistence_ready: bool = false,
    sql_persistence_draining: bool = false,
    sql_persistence_drain_cursor: usize = 0,
    /// Pending births enroll before their fresh metadata read. Root removal
    /// therefore cannot miss a producer between proof capture and owner open.
    native_initializers: std.AutoHashMapUnmanaged(u64, u32) = .empty,
    sql_preparation_secret: ?[]const u8 = null,
    sql_preparation_issuer: ?[]const u8 = null,
    row_policy_authority_secret: ?[]const u8 = null,
    row_policy_authority_issuer: ?[]const u8 = null,
    group_visible_root_generation: ?table_reads.GroupVisibleRootGenerationSource = null,
    transaction_recovery_source: ?transaction_recovery_source.Source = null,
    restore_descriptor_recovery: ?RestoreDescriptorRecovery = null,
    document_child_range_dispatch_source: ?table_write_source.TableWriteSource = null,
    resolution_candidate_source: ?runtime_callbacks.CandidateSource = null,
    coordinated_ttl: ?server_coordinated_ttl.Port = null,
    artifact_publications: ?@import("../storage/artifact_publication_dispatch.zig").Port = null,
    entity_sink: ?runtime_callbacks.EntitySink = null,
    runtime_status_cache: ?*runtime_status.TableRuntimeSnapshotCache = null,
    native_migration_policy: ?runtime_callbacks.DenseNativeMigrationPolicySource = null,
    promotion_leadership_source: ?table_writes.PromotionLeadershipSource = null,
    replication_write_gate: ?replication_contract.WriteGate = null,
    hot_standby_async_mirror: ?replication_contract.AsyncEffectMirror = null,
    remote_content: ?*const scraping.RemoteContentConfig = null,
    remote_content_configured: bool = false,
    secret_store: ?*anyopaque = null,
    context: client.Context = .{},
    owns_context: bool = true,
    context_init_mutex: std.atomic.Mutex = .unlocked,
    mutex: std.atomic.Mutex = .unlocked,
    quiescing: bool = false,
    shutdown_abandoned: bool = false,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,
    publications: std.ArrayListUnmanaged(*PendingPublication) = .empty,
    owner_cache_hits: std.atomic.Value(u64) = .init(0),
    native_owner_diagnostics: std.atomic.Value(u32) = .init(0),
    owner_cache_misses: std.atomic.Value(u64) = .init(0),
    /// Only the control worker may open or close an owner on behalf of Raft
    /// apply. The bounded queue holds owned, exact committed descriptors.
    apply_control_io: ?std.Io = null,
    apply_control_future: ?std.Io.Future(void) = null,
    apply_control_started: std.atomic.Value(bool) = .init(false),
    apply_control_wake: std.Io.Event = .unset,
    apply_control_pending: std.ArrayListUnmanaged(*ApplyOpen) = .empty,
    apply_control_active: ?*ApplyOpen = null,
    apply_control_stopping: bool = false,
    schema_entry_store: ?*@import("../storage/data_raft_apply_client.zig").RaftApplyStore = null,
    schema_snapshot_retention: ?snapshot_retention.Source = null,
    schema_apply: @import("../data/schema_apply_queue.zig").Queue(PreparedRaftSchemaControl) = .{},
    schema_control_shutdown: std.atomic.Value(bool) = .init(false),
    apply_ready_wake: ?ApplyReadyWake = null,
    apply_ready_wake_inflight: usize = 0,
    test_apply_control_hooks: ?ApplyControlTestHooks = null,

    const ApplyControlTestHooks = struct {
        ptr: *anyopaque,
        before_open: ?*const fn (*anyopaque) void = null,
        before_close: ?*const fn (*anyopaque) void = null,
    };

    pub const ApplyReadyWake = struct {
        ptr: *anyopaque,
        notify_fn: *const fn (*anyopaque) void,

        fn notify(self: ApplyReadyWake) void {
            self.notify_fn(self.ptr);
        }
    };

    const apply_control_max_pending = 64;
    const ApplyOpen = struct {
        group_id: u64,
        table_name: []u8,
        path: []u8,
        descriptor: descriptor_contract.Descriptor,
        retry_delay_ms: u32 = 2,
        next_log_ns: u64 = 0,

        fn init(alloc: std.mem.Allocator, replica_root_dir: []const u8, group_id: u64, table_name: []const u8, descriptor: descriptor_contract.Descriptor) !*ApplyOpen {
            const pending = try alloc.create(ApplyOpen);
            errdefer alloc.destroy(pending);
            const owned_name = try alloc.dupe(u8, table_name);
            errdefer alloc.free(owned_name);
            const owned_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ replica_root_dir, group_id });
            errdefer alloc.free(owned_path);
            const schema = try alloc.dupe(u8, descriptor.schema_json);
            errdefer alloc.free(schema);
            const indexes = try alloc.dupe(u8, descriptor.indexes_json);
            errdefer alloc.free(indexes);
            const restore_bootstrap = try alloc.dupe(u8, descriptor.restore_bootstrap_json);
            errdefer alloc.free(restore_bootstrap);
            const child_bootstrap = try alloc.dupe(u8, descriptor.initial_child_bootstrap_json);
            errdefer alloc.free(child_bootstrap);
            const range = try descriptor_contract.cloneInitialRange(alloc, descriptor.initial_range);
            errdefer descriptor_contract.freeInitialRange(alloc, range);
            var restore = if (descriptor.restore) |identity| try identity.clone(alloc) else null;
            errdefer if (restore) |*identity| identity.deinit(alloc);
            pending.* = .{
                .group_id = group_id,
                .table_name = owned_name,
                .path = owned_path,
                .descriptor = .{
                    .lsm_root_generation = descriptor.lsm_root_generation,
                    .identity = descriptor.identity,
                    .schema_json = schema,
                    .indexes_json = indexes,
                    .restore_bootstrap_json = restore_bootstrap,
                    .initial_child_bootstrap_json = child_bootstrap,
                    .initial_range = range,
                    .restore_cancel_recovery = descriptor.restore_cancel_recovery,
                    .restore_ha_replay = descriptor.restore_ha_replay,
                    .table_storage = descriptor.table_storage,
                    .sql_persistence = descriptor.sql_persistence,
                    .restore = restore,
                },
            };
            return pending;
        }

        pub fn deinit(self: *ApplyOpen, alloc: std.mem.Allocator) void {
            alloc.free(self.table_name);
            alloc.free(self.path);
            alloc.free(self.descriptor.schema_json);
            alloc.free(self.descriptor.indexes_json);
            alloc.free(self.descriptor.restore_bootstrap_json);
            alloc.free(self.descriptor.initial_child_bootstrap_json);
            descriptor_contract.freeInitialRange(alloc, self.descriptor.initial_range);
            if (self.descriptor.restore) |*identity| identity.deinit(alloc);
            alloc.destroy(self);
        }
    };

    const PendingPublication = struct {
        group_id: u64,
        table_name: []u8,
    };

    const Identity = descriptor_contract.Identity;

    pub const CacheStats = struct {
        hit_count: u64 = 0,
        miss_count: u64 = 0,
    };

    pub const LoadedDescriptor = struct {
        path: []u8,
        schema_json: []u8,
        indexes_json: []u8,
        table_storage: ?@import("antfly_local_sources").common_table_storage.Settings = null,
        generation: u64,
        sql_persistence: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor = null,
        initial_range: ?db_types.ByteRange = null,
        identity: descriptor_contract.Identity,
        restore: ?@import("../storage/restore_identity.zig").Identity = null,
        initial_child_bootstrap_json: ?[]u8 = null,

        pub fn view(self: *const LoadedDescriptor) descriptor_contract.Descriptor {
            return .{
                .lsm_root_generation = self.generation,
                .identity = self.identity,
                .schema_json = self.schema_json,
                .indexes_json = self.indexes_json,
                .table_storage = self.table_storage,
                .sql_persistence = self.sql_persistence,
                .initial_range = self.initial_range,
                .restore = self.restore,
                .initial_child_bootstrap_json = self.initial_child_bootstrap_json orelse "",
            };
        }

        pub fn deinit(self: *LoadedDescriptor, alloc: std.mem.Allocator) void {
            alloc.free(self.path);
            alloc.free(self.schema_json);
            alloc.free(self.indexes_json);
            descriptor_contract.freeInitialRange(alloc, self.initial_range);
            if (self.restore) |*identity| identity.deinit(alloc);
            if (self.initial_child_bootstrap_json) |value| alloc.free(value);
            self.* = undefined;
        }
    };

    const LeaseAdmission = enum { shared, exclusive, exclusive_if_idle };

    const Entry = struct {
        group_id: u64,
        table_name: []u8,
        generation: u64,
        identity: Identity,
        schema_json: []u8,
        indexes_json: []u8,
        restore_bootstrap_json: []u8,
        initial_child_bootstrap_json: []u8,
        initial_range: ?db_types.ByteRange = null,
        restore_cancel_recovery: bool = false,
        restore_ha_replay: bool = false,
        table_storage: ?@import("antfly_local_sources").common_table_storage.Settings = null,
        restore: ?@import("../storage/restore_identity.zig").Identity = null,
        sql_persistence: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor = null,
        owner: client.Owner,
        /// A Raft entry's pinned descriptor did not authorize current-catalog
        /// reconciliation. The next ordinary acquisition must reopen against
        /// the catalog, even when the descriptor bytes happen to match.
        opened_for_historical_apply: bool = false,
        // Exact descriptor/target proof, owned by this physical generation.
        // Shared repair steps may reuse it until a structural follow-up is due.
        repair_target: ?[]u8 = null,
        repair_configuration: ?abi.ReconcileResult = null,
        active_users: usize = 0,
        /// A source/dual FK fence permits exact topology control to reopen the
        /// durable owner, but forbids public reads until a whole-table catalog
        /// reconcile completes after the fence is released.
        catalog_deferred: bool = false,
        /// Apply drops its pin without waiting for the registry mutex: a cold
        /// open can hold that mutex while recovery reenters data Raft.
        pending_apply_releases: std.atomic.Value(usize) = .init(0),
        /// Foreground admission or durable background debt owns residency.
        /// Status and maintenance leases only borrow it until their release.
        resident: bool = false,
        transient_retirement_pending: bool = false,
        /// Writer preference for structural reconciliation. Once an exclusive
        /// caller observes live readers, new observational/foreground readers
        /// must stop entering so the existing leases can drain.
        exclusive_pending: bool = false,
        exclusive_active: bool = false,
        /// A catalog-only reopen closes new admission while admitted readers
        /// finish on the still-current physical root. Physical retirement must
        /// not invalidate their observations before this drain completes.
        catalog_drain_pending: bool = false,
        retired: bool = false,
        close_failure_reported: bool = false,
        close_failure: ?anyerror = null,
        close_retry_at_ns: u64 = 0,
        close_retry_delay_ns: u64 = 10 * std.time.ns_per_ms,
        closing: bool = false,
        bulk_ingest_active: std.atomic.Value(bool) = .init(false),
    };

    const Lease = struct {
        source: *ProvisionedKernelOwnerSource,
        entry: *Entry,
        exclusive: bool = false,
        apply_only: bool = false,
        active: bool = true,

        fn owner(self: *Lease) *client.Owner {
            return &self.entry.owner;
        }

        /// Extend an already-admitted read capability without entering the
        /// owner admission queue again while its statement fence is held.
        fn cloneRead(self: *Lease) Lease {
            lock(&self.source.mutex);
            defer self.source.mutex.unlock();
            std.debug.assert(self.active and !self.exclusive);
            self.entry.active_users += 1;
            return .{ .source = self.source, .entry = self.entry };
        }

        fn downgrade(self: *Lease) void {
            lock(&self.source.mutex);
            defer self.source.mutex.unlock();
            std.debug.assert(self.active and self.exclusive and self.entry.active_users == 1);
            self.entry.exclusive_active = false;
            self.exclusive = false;
        }

        fn retireAfterConfigurationFailure(self: *Lease) void {
            lock(&self.source.mutex);
            self.entry.retired = true;
            self.source.mutex.unlock();
        }

        fn requestTransientRetirement(self: *Lease) void {
            lock(&self.source.mutex);
            defer self.source.mutex.unlock();
            if (!self.entry.resident) self.entry.transient_retirement_pending = true;
        }

        fn retain(self: *Lease) void {
            lock(&self.source.mutex);
            defer self.source.mutex.unlock();
            self.entry.resident = true;
            self.entry.transient_retirement_pending = false;
        }

        pub fn deinit(self: *Lease) void {
            if (!self.active) return;
            self.source.release(self.entry, self.exclusive, self.apply_only);
            self.active = false;
        }
    };

    pub fn init(
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        catalog: table_catalog.CatalogSource,
        read_safety_barrier: read_gate.ReadSafetyBarrier,
    ) ProvisionedKernelOwnerSource {
        return .{
            .alloc = alloc,
            .replica_root_dir = replica_root_dir,
            .catalog = catalog,
            .read_safety_barrier = read_safety_barrier,
        };
    }

    /// Called on the hosting control path before linked data-Raft apply starts.
    /// The borrowed lane is independent of Raft progress and is joined before
    /// its worker lease is released.
    pub fn startApplyControl(self: *ProvisionedKernelOwnerSource, io: std.Io) !void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.apply_control_future != null) return;
        if (self.quiescing) return error.Canceled;
        // Apply admission never grows the queue under the Raft mutex.
        try self.apply_control_pending.ensureTotalCapacity(self.alloc, apply_control_max_pending);
        self.apply_control_io = io;
        self.schema_control_shutdown.store(false, .release);
        errdefer self.apply_control_io = null;
        self.apply_control_future = try io.concurrent(applyControlMain, .{self});
        self.apply_control_started.store(true, .release);
    }

    pub fn setApplyReadyWake(self: *ProvisionedKernelOwnerSource, wake: ?ApplyReadyWake) void {
        while (true) {
            lock(&self.mutex);
            self.apply_ready_wake = wake;
            const drained = wake != null or self.apply_ready_wake_inflight == 0;
            self.mutex.unlock();
            if (drained) return;
            platform_time.yieldBriefly();
        }
    }

    /// Publish the control worker's stop without joining it. A deployment
    /// sharing one cooperative scheduler must wake all owners before driving
    /// their tasks to completion and reclaiming the sources.
    pub fn beginApplyControlShutdown(self: *ProvisionedKernelOwnerSource) void {
        self.schema_control_shutdown.store(true, .release);
        lock(&self.mutex);
        self.apply_control_stopping = true;
        self.apply_control_started.store(false, .release);
        const io = self.apply_control_io;
        if (io) |control_io| self.apply_control_wake.set(control_io);
        self.mutex.unlock();
    }

    fn stopApplyControl(self: *ProvisionedKernelOwnerSource) void {
        self.beginApplyControlShutdown();
        const io = self.apply_control_io;
        if (self.apply_control_future) |*future| future.await(io.?);
        self.apply_control_future = null;
        self.schema_apply.deinit();
        // Keep the borrowed Io value stable until source destruction: an
        // apply-only release that observed started=true before shutdown may
        // still load it while stop waits for attached apply work to drain.
        self.apply_ready_wake = null;
        for (self.apply_control_pending.items) |pending| pending.deinit(self.alloc);
        self.apply_control_pending.deinit(self.alloc);
        self.apply_control_pending = .empty;
    }

    fn applyControlMain(self: *ProvisionedKernelOwnerSource) void {
        const io = self.apply_control_io.?;
        var schema_host: SchemaApplyHost = .{ .source = self };
        var schema_turn = true;
        while (true) {
            if (schema_turn) {
                schema_turn = false;
                if (self.schema_apply.step(&schema_host, platform_time.monotonicNs()) == .worked) continue;
            }
            lock(&self.mutex);
            self.apply_control_wake.reset();
            self.reconcileApplyReleasesLocked();
            if (self.apply_control_stopping) {
                self.mutex.unlock();
                return;
            }
            const close_now = platform_time.monotonicNs();
            var next_close_retry_ns: ?u64 = null;
            const retired_index = for (self.entries.items, 0..) |entry, index| {
                if (!entry.retired or entry.closing or entry.active_users != 0) continue;
                if (entry.close_retry_at_ns <= close_now) break index;
                next_close_retry_ns = @min(next_close_retry_ns orelse entry.close_retry_at_ns, entry.close_retry_at_ns);
            } else null;
            if (retired_index) |index| {
                self.destroyEntryAtIndexLocked(index);
                self.mutex.unlock();
                continue;
            }
            const pending: ?*ApplyOpen = if (self.apply_control_pending.items.len != 0)
                self.apply_control_pending.orderedRemove(0)
            else
                null;
            self.apply_control_active = pending;
            self.mutex.unlock();
            const request = pending orelse {
                switch (self.schema_apply.step(&schema_host, platform_time.monotonicNs())) {
                    .worked => continue,
                    .pending => {
                        const now = platform_time.monotonicNs();
                        const delay = @min(self.schema_apply.retryDelayNs(now), if (next_close_retry_ns) |retry_at| retry_at -| now else std.math.maxInt(u64));
                        self.apply_control_wake.waitTimeout(io, .{ .duration = .{
                            .raw = .fromNanoseconds(@intCast(delay)),
                            .clock = .awake,
                        } }) catch {};
                    },
                    .idle => if (next_close_retry_ns) |retry_at| {
                        self.apply_control_wake.waitTimeout(io, .{ .duration = .{
                            .raw = .fromNanoseconds(@intCast(retry_at -| platform_time.monotonicNs())),
                            .clock = .awake,
                        } }) catch {};
                    } else self.apply_control_wake.wait(io) catch {},
                }
                continue;
            };
            schema_turn = true;
            if (@import("builtin").is_test) if (self.test_apply_control_hooks) |hooks| {
                if (hooks.before_open) |before_open| before_open(hooks.ptr);
            };
            const opened = blk: {
                var lease = self.acquireDescriptorOnce(request.group_id, request.table_name, request.path, request.descriptor, .shared, .resident, .{ .historical_raft_apply = true }) catch |err| {
                    switch (err) {
                        error.StorageKernelOwnerTransitionRequired,
                        error.StorageReadTemporarilyUnavailable,
                        error.StorageBusy,
                        => request.retry_delay_ms = 2,
                        error.Canceled => {},
                        else => {
                            const now_ns = platform_time.monotonicNs();
                            if (now_ns >= request.next_log_ns) {
                                std.log.warn("data raft owner control deferred group_id={} table={s} err={s}", .{ request.group_id, request.table_name, @errorName(err) });
                                request.next_log_ns = now_ns +| std.time.ns_per_s;
                            }
                            request.retry_delay_ms = @min(request.retry_delay_ms *| 2, 1_000);
                        },
                    }
                    break :blk false;
                };
                lease.deinit();
                break :blk true;
            };
            if (opened) {
                lock(&self.mutex);
                self.apply_control_active = null;
                const wake = self.apply_ready_wake;
                if (wake != null) self.apply_ready_wake_inflight += 1;
                self.mutex.unlock();
                request.deinit(self.alloc);
                if (wake) |callback| {
                    callback.notify();
                    lock(&self.mutex);
                    self.apply_ready_wake_inflight -= 1;
                    self.mutex.unlock();
                }
                continue;
            }
            lock(&self.mutex);
            self.apply_control_active = null;
            if (self.apply_control_stopping) {
                self.mutex.unlock();
                request.deinit(self.alloc);
                return;
            }
            // A queued group owns one slot until its exact descriptor opens;
            // retrying a transition does not allocate or grow the backlog.
            self.apply_control_pending.appendAssumeCapacity(request);
            self.mutex.unlock();
            io.sleep(.fromMilliseconds(request.retry_delay_ms), .awake) catch {};
        }
    }

    pub fn withGroupVisibleRootGeneration(
        self: *ProvisionedKernelOwnerSource,
        source: ?table_reads.GroupVisibleRootGenerationSource,
    ) *ProvisionedKernelOwnerSource {
        self.group_visible_root_generation = source;
        return self;
    }

    pub fn withReadSafetyBarrier(
        self: *ProvisionedKernelOwnerSource,
        read_safety_barrier: read_gate.ReadSafetyBarrier,
    ) *ProvisionedKernelOwnerSource {
        self.read_safety_barrier = read_safety_barrier;
        return self;
    }

    pub fn withTransactionRecoverySource(
        self: *ProvisionedKernelOwnerSource,
        source: ?transaction_recovery_source.Source,
    ) *ProvisionedKernelOwnerSource {
        self.transaction_recovery_source = source;
        return self;
    }

    /// Generated child-range artifacts are routed by the distributed table
    /// source while the physical owner retains the durable outbox. The source
    /// is borrowed for synchronous batch calls and is never retained by the
    /// compiled provider.
    pub fn withDocumentChildRangeDispatchSource(
        self: *ProvisionedKernelOwnerSource,
        source: table_write_source.TableWriteSource,
    ) *ProvisionedKernelOwnerSource {
        self.document_child_range_dispatch_source = source;
        return self;
    }

    /// Runtime callbacks are retained by every compiled owner and therefore
    /// must be installed before the first owner is opened.
    pub fn withRuntimeHooks(
        self: *ProvisionedKernelOwnerSource,
        candidate_source: ?runtime_callbacks.CandidateSource,
        entity_sink: ?runtime_callbacks.EntitySink,
        leadership_source: ?table_writes.PromotionLeadershipSource,
    ) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.resolution_candidate_source = candidate_source;
        self.entity_sink = entity_sink;
        self.promotion_leadership_source = leadership_source;
        return self;
    }

    /// HA policy stays in distributed control. The compiled owner performs the
    /// physical commit; this adapter fences before it and appends the exact
    /// coarse batch plus its provider-produced derived effect only after that
    /// commit succeeds.
    pub fn withHotStandbyControls(
        self: *ProvisionedKernelOwnerSource,
        gate: ?replication_contract.WriteGate,
        mirror: ?replication_contract.AsyncEffectMirror,
    ) *ProvisionedKernelOwnerSource {
        self.replication_write_gate = gate;
        self.hot_standby_async_mirror = mirror;
        return self;
    }

    pub fn withSecretStore(self: *ProvisionedKernelOwnerSource, store: ?*anyopaque) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.secret_store = store;
        self.remote_content_configured = false;
        return self;
    }

    pub fn withRemoteContent(
        self: *ProvisionedKernelOwnerSource,
        remote_content: ?*const scraping.RemoteContentConfig,
    ) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.remote_content = remote_content;
        self.remote_content_configured = false;
        return self;
    }

    fn ensureContextConfigured(self: *ProvisionedKernelOwnerSource) !void {
        // Cold hidden-owner reads can arrive concurrently with one another or
        // with an owner acquisition. Context creation and configuration must
        // publish as one operation before any caller uses the handle.
        lock(&self.context_init_mutex);
        defer self.context_init_mutex.unlock();
        try self.context.ensure();
        if (self.remote_content_configured) return;
        try self.context.configureRemoteContent(self.remote_content);
        try self.context.configureSecrets(self.secret_store);
        self.remote_content_configured = true;
    }

    pub fn withStorageContextHandle(
        self: *ProvisionedKernelOwnerSource,
        handle: ?*anyopaque,
    ) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        std.debug.assert(self.context.handle == null);
        self.context.handle = handle;
        self.owns_context = false;
        // A borrowed process context must be fully configured before any
        // system store or table owner acquires it. Reconfiguring it lazily
        // here would race those existing owners and correctly return busy.
        self.remote_content_configured = true;
        return self;
    }

    /// Call only after every attached read/write source has drained. Owner
    /// closure is deliberately centralized here so one live DB serves both
    /// operation families for its full group lifecycle.
    pub fn deinit(self: *ProvisionedKernelOwnerSource) void {
        self.stopApplyControl();
        lock(&self.mutex);
        self.reconcileApplyReleasesLocked();
        for (self.entries.items) |entry| {
            std.debug.assert(entry.active_users == 0 and !entry.closing);
            entry.retired = true;
        }
        self.drainRetiredLocked(null, null);
        // Final destruction is the sole forced-abandon path. Native context
        // debt remains dirty, so this can never publish a clean boot proof.
        while (self.entries.items.len != 0) self.destroyEntryAtIndexLockedMode(0, true);
        std.debug.assert(self.publications.items.len == 0);
        self.publications.deinit(self.alloc);
        std.debug.assert(self.native_initializers.count() == 0);
        self.native_initializers.deinit(self.alloc);
        self.entries.deinit(self.alloc);
        self.entries = .empty;
        self.mutex.unlock();
        if (self.owns_context) self.context.deinit();
    }

    /// Close admission and join every DB-owned worker while its Raft,
    /// candidate, sink, and provider callback contexts are still alive.
    /// Attached request/apply sources must already be stopped. Keep the
    /// registry and context valid until their ordinary final deinit.
    pub const QuiesceOutcome = enum { drained, abandoned };

    pub fn quiesce(self: *ProvisionedKernelOwnerSource, io: std.Io) !void {
        _ = try self.quiesceUntil(io, null);
    }

    /// The deadline limits waiting for debt, never the lifetime of a borrowed
    /// owner. At expiry only idle registered owners may enter the existing
    /// final-destruction path; native debt remains dirty in its context and
    /// durable journal. The caller must not publish a clean Boot afterward.
    pub fn quiesceWithDeadline(self: *ProvisionedKernelOwnerSource, io: std.Io, deadline: @import("../common/runtime_lifecycle.zig").ShutdownDeadline) !QuiesceOutcome {
        return self.quiesceUntil(io, deadline);
    }

    fn quiesceUntil(self: *ProvisionedKernelOwnerSource, io: std.Io, deadline: ?@import("../common/runtime_lifecycle.zig").ShutdownDeadline) !QuiesceOutcome {
        self.stopApplyControl();
        while (true) {
            const outcome: ?QuiesceOutcome = blk: {
                lock(&self.mutex);
                defer self.mutex.unlock();
                self.reconcileApplyReleasesLocked();
                self.quiescing = true;
                for (self.entries.items) |entry| entry.retired = true;
                self.drainRetiredLocked(null, null);
                if (self.entries.items.len == 0 and self.native_initializers.count() == 0 and self.publications.items.len == 0)
                    break :blk if (self.shutdown_abandoned) .abandoned else .drained;
                if (deadline) |limit| if (limit.expired()) {
                    if (self.native_initializers.count() != 0 or self.publications.items.len != 0) return error.ShutdownTimeout;
                    for (self.entries.items) |entry| {
                        if (entry.active_users != 0 or entry.exclusive_active or entry.exclusive_pending or entry.closing)
                            return error.ShutdownTimeout;
                    }
                    // Admission remains closed while destruction temporarily
                    // unlocks the registry to join the owner's own workers.
                    self.shutdown_abandoned = true;
                    while (self.entries.items.len != 0) self.destroyEntryAtIndexLockedMode(0, true);
                    break :blk .abandoned;
                };
                break :blk null;
            };
            if (outcome) |finished| return finished;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    pub fn readSource(self: *ProvisionedKernelOwnerSource) table_read_source.TableReadSource {
        return .{
            .ptr = self,
            .strict_read_index_absence = true,
            .remote_statement_fences_safe = self.read_safety_barrier.vtable.capture_frozen != null and self.read_safety_barrier.vtable.validate_frozen != null,
            .supports_sql_range_guards = true,
            .vtable = &.{
                .lookup = unsupportedTopLevelLookup,
                .scan = unsupportedTopLevelScan,
                .open_relational_read_group_local_routed = openRelationalReadRouted,
                .try_statement_read_fence_group_local_routed = tryStatementReadFenceRouted,
                .open_relational_statement_snapshot_group_local_routed = openStatementSnapshotRouted,
                .query = unsupportedTopLevelQuery,
                .preflight_query_group_local = preflightQueryGroupLocal,
                .preflight_query_group_local_routed = preflightQueryGroupLocalRouted,
                .lookup_group_local = lookupGroupLocal,
                .lookup_group_local_routed = lookupGroupLocalRouted,
                .scan_group_local_stream = scanGroupLocalStream,
                .scan_group_local = scanGroupLocal,
                .scan_group_local_routed_stream = scanGroupLocalRoutedStream,
                .scan_group_local_routed = scanGroupLocalRouted,
                .query_group_local = queryGroupLocal,
                .query_group_local_routed = queryGroupLocalRouted,
                .search_result_group_local = searchResultGroupLocal,
                .search_result_group_local_routed = searchResultGroupLocalRouted,
                .text_stats_group_local = textStatsGroupLocal,
                .text_stats_group_local_routed = textStatsGroupLocalRouted,
                .algebraic_partials_group_local = algebraicPartialsGroupLocal,
                .algebraic_partials_group_local_routed = algebraicPartialsGroupLocalRouted,
                .graph_expand_group_local = graphExpandGroupLocal,
                .graph_expand_group_local_routed = graphExpandGroupLocalRouted,
                .graph_hydrate_group_local = graphHydrateGroupLocal,
                .graph_hydrate_group_local_routed = graphHydrateGroupLocalRouted,
                .graph_edges_group_local = graphEdgesGroupLocal,
                .graph_edges_group_local_routed = graphEdgesGroupLocalRouted,
                .observed_dynamic_field_capability_sets = observedDynamicFieldCapabilitySets,
                .document_artifact_manifest_group_local = documentArtifactManifestGroupLocal,
                .document_artifact_manifest_group_local_routed = documentArtifactManifestGroupLocalRouted,
                .document_artifact_manifests_group_local = documentArtifactManifestsGroupLocal,
                .document_artifact_manifests_group_local_routed = documentArtifactManifestsGroupLocalRouted,
            },
        };
    }

    pub fn writeSource(self: *ProvisionedKernelOwnerSource) table_write_source.TableWriteSource {
        return .{
            .ptr = self,
            .prepares_rows_atomically = true,
            .vtable = &.{
                .batch = unsupportedTopLevelBatch,
                .batch_group_local = batchGroupLocal,
                .batch_group_local_routed = batchGroupLocalRouted,
                .replicated_batch_group_local = replicatedBatchGroupLocal,
                .backup_table_group_local = backupTableGroupLocal,
                .backup_pin_control = backupPinControl,
                .txn_begin_group_local = txnBeginGroupLocal,
                .txn_begin_group_local_with_pre_decision_context = txnBeginGroupLocalWithPreDecisionContext,
                .txn_prepare_group_local = txnPrepareGroupLocal,
                .txn_prepare_group_local_with_pre_decision_context = txnPrepareGroupLocalWithPreDecisionContext,
                .txn_resolve_group_local = txnResolveGroupLocal,
                .txn_resolve_group_local_with_request = txnResolveGroupLocalWithRequest,
                .txn_status_group_local = txnStatusGroupLocal,
                .txn_status_group_local_with_request = txnStatusGroupLocalWithRequest,
                .txn_acknowledge_group_local = txnAcknowledgeGroupLocal,
                .begin_bulk_ingest_group_local = beginBulkIngestGroupLocal,
                .finish_bulk_ingest_group_local = finishBulkIngestGroupLocal,
                .abort_bulk_ingest_group_local = abortBulkIngestGroupLocal,
                .corrupt_embedding_artifact_group_local = corruptEmbeddingArtifactGroupLocal,
                .reprocess_document_artifact_group_local = reprocessDocumentArtifactGroupLocal,
                .reprocess_document_artifact_range_group_local = reprocessDocumentArtifactRangeGroupLocal,
                .list_artifact_repair_issues_group_local = listArtifactRepairIssuesGroupLocal,
                .vector_migration_group_local = vectorMigrationGroupLocal,
                .sql_maintenance_group = sqlMaintenanceGroup,
                .sql_maintenance_group_routed = sqlMaintenanceGroupRouted,
                .sql_maintenance_group_local_routed = sqlMaintenanceGroupRouted,
                .sql_maintenance_group_local = sqlMaintenanceGroupLocal,
                .graph_metric_maintenance_group_local = graphMetricMaintenanceGroupLocal,
                .resolver_operation_group_local = resolverOperationGroupLocal,
                .repair_artifact_issues_group_local = repairArtifactIssuesGroupLocal,
                .repair_artifact_issues_group_local_controlled = repairArtifactIssuesGroupLocalControlled,
                .update_document_artifact_child_range_placement_group_local = updateDocumentArtifactChildRangePlacementGroupLocal,
                .apply_document_artifact_child_range_batch_group_local = applyDocumentArtifactChildRangeBatchGroupLocal,
                .local_runtime_statuses = localRuntimeStatuses,
                .text_memory_attribution_stats_best_effort = textMemoryAttributionStatsBestEffort,
                .preflight_write_admission_group_local = preflightWriteAdmissionGroupLocal,
                .preflight_grouped_source_batch_group_local = preflightGroupedSourceBatchGroupLocal,
                .prepare_hot_standby_seed_snapshot_group_local = prepareHotStandbySeedSnapshotGroupLocal,
                .capture_hot_standby_seed_snapshot_group_local = captureHotStandbySeedSnapshotGroupLocal,
                .find_median_key_group_local = findMedianKeyGroupLocal,
                .reconcile_table_group_local = reconcileTableGroupLocal,
                .reconcile_table_group_local_transient = reconcileTableGroupLocalTransient,
                .retire_table_group_local = retireTableGroupLocal,
                .reconcile_table_group_local_observed = reconcileTableGroupLocalObserved,
                .local_runtime_status_group_local = localRuntimeStatusGroupLocal,
            },
        };
    }

    pub fn captureNativeRaftSnapshot(self: *ProvisionedKernelOwnerSource, group_id: u64, applied_index: u64) !*anyopaque {
        // Keep the owner and its runtime alive across deferred materialization.
        // The compiled capture destroys its pin before releasing this lease.
        var catalog = try self.catalog.adminSnapshot();
        const table_name = blk: {
            defer self.catalog.freeAdminSnapshot(&catalog);
            const range = metadata_domain.findAdminRange(&catalog, group_id) orelse return error.UnknownGroup;
            const table = metadata_domain.findAdminTable(&catalog, range.table_id) orelse return error.TableNotFound;
            break :blk try self.alloc.dupe(u8, table.name);
        };
        defer self.alloc.free(table_name);
        const lease = try self.alloc.create(Lease);
        errdefer self.alloc.destroy(lease);
        // Snapshot the persisted native catalog at this committed cut; an
        // active schema fence may deliberately precede metadata publication.
        lease.* = try self.acquireWithControls(group_id, table_name, .{
            .allow_deferred_catalog = true,
            .historical_raft_apply = self.online_source_authority == .raft,
            .durable_catalog = self.online_source_authority == .raft,
        });
        errdefer lease.deinit();
        var capture = try lease.owner().captureNativeRaftSnapshot(group_id, applied_index);
        errdefer capture.deinit();
        try capture.bindLease(lease, struct {
            fn release(ptr: ?*anyopaque) callconv(.c) void {
                const held: *Lease = @ptrCast(@alignCast(ptr.?));
                const alloc = held.source.alloc;
                held.deinit();
                alloc.destroy(held);
            }
        }.release);
        return capture.handle orelse unreachable;
    }

    pub fn snapshotSource(self: *ProvisionedKernelOwnerSource) storage_snapshot_source.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .begin_publication = beginPublication,
                .end_publication = endPublication,
                .prepare = prepareSnapshot,
                .prepare_restore = prepareRestore,
                .reconcile_restore = reconcileRestore,
                .repair_published_restore = repairPublishedRestore,
                .promote = promoteSnapshot,
                .publish_prepared = publishPreparedSnapshot,
                .commit = commitSnapshot,
                .rollback = rollbackSnapshot,
                .destroy = destroySnapshot,
            },
        };
    }

    pub fn maintenanceSource(self: *ProvisionedKernelOwnerSource) storage_maintenance_source.Source {
        return .{
            .ptr = self,
            .vtable = &.{
                .run_lsm_round = runLsmMaintenanceRound,
                .run_dense_posting_round = runDensePostingMaintenanceRound,
                .publish_dense_checkpoints = publishDenseCheckpoints,
                .run_vector_block_round = runVectorBlockRound,
                .snapshot = maintenanceSnapshot,
                .publish_runtime_statuses = publishRuntimeStatuses,
            },
        };
    }

    /// Process-owner reuse replaces the legacy read/write cache split. Report
    /// one shared acquisition counter to both compatibility metric names until
    /// those public metrics are renamed around the owner model.
    pub fn cacheStats(self: *const ProvisionedKernelOwnerSource) CacheStats {
        return .{
            .hit_count = self.owner_cache_hits.load(.monotonic),
            .miss_count = self.owner_cache_misses.load(.monotonic),
        };
    }

    pub fn contextMetrics(self: *ProvisionedKernelOwnerSource) !abi.ContextMetricsResult {
        try self.ensureContextConfigured();
        return try self.context.metrics();
    }

    /// Native storage process identity, independent of the API SQL session
    /// issuer. Catalog descriptors and request bodies cannot choose this epoch.
    pub fn nativeExecutionInstance(self: *ProvisionedKernelOwnerSource) ![16]u8 {
        const value = (try self.contextMetrics()).execution_instance;
        if (std.mem.allEqual(u8, &value, 0)) return error.SqlRelationOwnerChanged;
        return value;
    }

    /// Initialization is proved by the native primary, then durably closed
    /// before signing. It is not an attestation of a metadata descriptor.
    pub const RecoveryDescriptor = struct {
        group_id: u64,
        table_name: []u8,
        loaded: LoadedDescriptor,
        pub fn deinit(self: *@This(), a: std.mem.Allocator) void {
            self.loaded.deinit(a);
            a.free(self.table_name);
            self.* = undefined;
        }
    };

    /// Indexed immutable navigation, not name-based rediscovery or a full
    /// catalog scan. The producer subsequently rejoins its SignedBinding and
    /// current Boot at native/metadata boundaries before publishing anything.
    pub fn loadDescriptorByTableIdWithDeadline(self: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, table_id: u64, deadline_ns: ?u64) !RecoveryDescriptor {
        if (table_id == 0) return error.InvalidSqlRelationPersistence;
        const acquire_generation = self.catalog.vtable.acquire_routing_generation orelse return error.CatalogRoutingUnavailable;
        const generation = try acquire_generation(self.catalog.ptr, self.catalog.deadlineFrom(.{ .deadline_ns = deadline_ns }), false);
        defer generation.release();
        const indexed = &generation.indexed;
        const index = indexed.table_id_indexes.get(table_id) orelse return error.TableNotFound;
        const table = indexed.snapshot.value.tables[index];
        const ranges = indexed.table_range_refs.get(table_id) orelse return error.IdentityNamespaceMismatch;
        if (table.sql_persistence == null or ranges.len != 1 or ranges[0].table_id != table_id) return error.InvalidSqlRelationPersistence;
        const name = try a.dupe(u8, table.name);
        errdefer a.free(name);
        var loaded = try self.loadDescriptorWithDeadline(a, ranges[0].group_id, name, deadline_ns);
        errdefer loaded.deinit(a);
        if (loaded.identity.table_id != table_id or loaded.sql_persistence == null or !loaded.sql_persistence.?.eql(table.sql_persistence.?)) return error.SqlRelationOwnerChanged;
        return .{ .group_id = ranges[0].group_id, .table_name = name, .loaded = loaded };
    }

    pub fn initializedRelationBindingAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, metadata_group: u64, group: u64, table_name: []const u8, boot: @import("../metadata/sql_relation_boots.zig").Record, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !@import("../metadata/sql_relation_attestation.zig").SignedBinding {
        try context.ensureActive();
        const normalized = try platformDeadlineContext(context);
        var descriptor = try self.loadDescriptorWithDeadline(alloc, group, table_name, normalized.deadline_ns);
        defer descriptor.deinit(alloc);
        return self.initializedRelationBindingForDescriptorAtRoot(alloc, metadata_group, group, table_name, descriptor.path, descriptor.view(), null, boot, root, context);
    }

    /// Hidden targets use the immutable already-admitted job descriptor; the
    /// metadata receipt receiver separately rejoins this exact plan and scope.
    pub fn initializedRelationBindingForDescriptorAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, metadata_group: u64, group: u64, table_name: []const u8, path: []const u8, descriptor: descriptor_contract.Descriptor, creation_scope: ?@import("../metadata/sql_relation_attestation.zig").CreationScope, boot: @import("../metadata/sql_relation_boots.zig").Record, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !@import("../metadata/sql_relation_attestation.zig").SignedBinding {
        try context.ensureActive();
        try self.beginNativeInitialization(group, table_name);
        defer self.endNativeInitialization(group);
        const normalized = try platformDeadlineContext(context);
        try boot.validate();
        var uninitialized: ?ReadControls = null;
        const no_ack_observed = try self.catalog.sqlRelationBinding(alloc, .{ .node = boot.identity.node_id, .binding_table = descriptor.identity.table_id, .creation_scope = creation_scope }, normalized.deadline_ns);
        if (!no_ack_observed.registration_current or no_ack_observed.record == null or
            no_ack_observed.record.?.generation != boot.generation or
            !std.mem.eql(u8, &no_ack_observed.record.?.instance, &boot.instance)) return error.SqlRelationOwnerChanged;
        if (no_ack_observed.uninitialized) |permit| {
            const p = descriptor.sql_persistence orelse return error.InvalidSqlRelationPersistence;
            try permit.validateFor(p);
            if (permit.metadata_group_id != metadata_group or permit.boot_generation != boot.generation or
                permit.table_id != descriptor.identity.table_id or permit.shard_id != descriptor.identity.shard_id or
                permit.range_id != descriptor.identity.range_id) return error.SqlRelationOwnerChanged;
            uninitialized = .{ .native_uninitialized = .{
                .host = .{ .node_id = p.owner_node_id, .store_id = p.owner_store_id, .execution_instance = boot.instance, .generation_instance = p.generation_instance, .data_generation = p.data_generation },
                .boot = .{ .instance = boot.instance, .unclean_restart = boot.unclean_restart, .initializing = true, .uninitialized = permit },
            } };
        }
        if (boot.phase != .ready and uninitialized == null) return error.SqlRelationRecoveryIncomplete;
        const actual = try self.nativeExecutionInstance();
        if (!std.mem.eql(u8, &actual, &boot.instance)) return error.SqlRelationOwnerChanged;
        const lifetime = descriptor.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        if (lifetime.owner_node_id != boot.identity.node_id or lifetime.owner_store_id != boot.identity.store_id or
            (lifetime.kind == .temporary and !std.mem.eql(u8, &lifetime.generation_instance, &actual))) return error.SqlRelationOwnerChanged;
        var lease = try self.acquireDescriptorWithMode(group, table_name, path, descriptor, true, .transient, .{
            .native_initialization = true,
            .native_uninitialized = if (uninitialized) |control| control.native_uninitialized else null,
            .execution_deadline_ns = normalized.deadline_ns,
            .cancellation = normalized.cancellation,
        });
        defer lease.deinit();
        const identity = lease.entry.identity;
        const binding = try lease.owner().nativeRelationBinding(alloc, table_name);
        try binding.validate();
        if (binding.has_user_data or binding.has_transaction_state) return error.SqlRelationRecoveryIncomplete;
        if (uninitialized) |control| try control.native_uninitialized.?.boot.uninitialized.?.requireBinding(binding);
        if (!binding.descriptor.eql(lifetime) or binding.table_id != identity.table_id or binding.shard_id != identity.shard_id or binding.range_id != identity.range_id)
            return error.SqlRelationOwnerChanged;
        // Exclusive admission prevents an in-flight row mutation from racing
        // the observation. Retirement closes this exact handle fallibly.
        const entry = lease.entry;
        lock(&self.mutex);
        entry.resident = false;
        entry.retired = true;
        self.mutex.unlock();
        lease.deinit();
        lock(&self.mutex);
        const retained = for (self.entries.items) |candidate| {
            if (candidate == entry) break true;
        } else false;
        self.mutex.unlock();
        if (retained) return error.SqlRelationOwnerUnavailable;
        try context.ensureActive();
        if (!std.mem.eql(u8, &actual, &try self.nativeExecutionInstance())) return error.SqlRelationOwnerChanged;
        return try @import("../metadata/sql_relation_attestation.zig").SignedBinding.sign(.{
            .kind = .initialized,
            .metadata_group_id = metadata_group,
            .boot_generation = boot.generation,
            .identity = boot.identity,
            .reporter_incarnation = boot.reporter_incarnation,
            .binding = binding,
            .creation_scope = creation_scope,
        }, root);
    }

    /// Reopen the exact durably initialized image through a binding-only
    /// control capability. Failed flush/close remains counted in this registry;
    /// no publication can mistake a missing initialized root for an empty one.
    pub fn adoptedRelationBindingForDescriptorAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, metadata_group: u64, group: u64, table_name: []const u8, path: []const u8, descriptor: descriptor_contract.Descriptor, before: @import("../metadata/sql_relation_attestation.zig").SignedBinding, boot: @import("../metadata/sql_relation_boots.zig").Record, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !@import("../metadata/sql_relation_attestation.zig").SignedBinding {
        const P = @import("antfly_local_sources").common_topology_records.RelationPersistence;
        try context.ensureActive();
        try self.beginNativeInitialization(group, table_name);
        defer self.endNativeInitialization(group);
        const normalized = try platformDeadlineContext(context);
        try before.verify();
        try boot.validate();
        const actual = try self.nativeExecutionInstance();
        const lifetime = descriptor.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        if (boot.phase != .recovering or boot.unclean_restart or lifetime.kind != .unlogged or
            metadata_group != before.metadata_group_id or !before.binding.descriptor.eql(lifetime) or
            !std.mem.eql(u8, &actual, &boot.instance) or !std.mem.eql(u8, &try before.identity.digest(), &try boot.identity.digest()) or
            boot.generation <= before.boot_generation or before.binding.table_id != descriptor.identity.table_id or
            before.binding.shard_id != descriptor.identity.shard_id or before.binding.range_id != descriptor.identity.range_id)
            return error.SqlRelationOwnerChanged;
        const guard: P.CleanAdoption = .{ .before = lifetime, .metadata_group_id = metadata_group, .boot_generation = boot.generation, .table_id = before.binding.table_id, .shard_id = before.binding.shard_id, .range_id = before.binding.range_id, .physical_root_incarnation = before.binding.physical_root_incarnation };
        var lease = try self.acquireDescriptorWithMode(group, table_name, path, descriptor, true, .transient, .{
            .native_adoption = .{ .host = .{ .node_id = lifetime.owner_node_id, .store_id = lifetime.owner_store_id, .execution_instance = actual, .generation_instance = lifetime.generation_instance, .data_generation = lifetime.data_generation }, .boot = .{ .instance = actual, .unclean_restart = false, .adoption = guard } },
            // Binding-only recovery deliberately omits mutable resolver/index
            // reconciliation. Its CAPI capability permits only the signed
            // native binding observation, never deferred-catalog row reads.
            .allow_deferred_catalog = true,
            .execution_deadline_ns = normalized.deadline_ns,
            .cancellation = normalized.cancellation,
        });
        defer lease.deinit();
        const binding = try lease.owner().nativeRelationBinding(alloc, table_name);
        const entry = lease.entry;
        lock(&self.mutex);
        entry.resident = false;
        entry.retired = true;
        self.mutex.unlock();
        lease.deinit();
        lock(&self.mutex);
        const retained = for (self.entries.items) |candidate| {
            if (candidate == entry) break true;
        } else false;
        self.mutex.unlock();
        if (retained) return error.SqlRelationOwnerUnavailable;
        try context.ensureActive();
        if (!std.mem.eql(u8, &actual, &try self.nativeExecutionInstance())) return error.SqlRelationOwnerChanged;
        const proof = try @import("../metadata/sql_relation_attestation.zig").SignedBinding.sign(.{
            .kind = .adopted,
            .metadata_group_id = metadata_group,
            .boot_generation = boot.generation,
            .identity = boot.identity,
            .reporter_incarnation = boot.reporter_incarnation,
            .binding = binding,
            .creation_scope = before.creation_scope,
            .previous_descriptor_fingerprint = try lifetime.fingerprint(),
            .previous_binding_digest = try before.digest(),
        }, root);
        try @import("../metadata/sql_relation_lifetimes.zig").requireAdoption(before, proof);
        return proof;
    }

    pub fn attestRelationBoot(self: *ProvisionedKernelOwnerSource, request: @import("../metadata/sql_relation_boot_wire.zig").Request, root: @import("antfly_local_sources").storage_db_root_signing_identity.State) !@import("../metadata/sql_relation_boot_wire.zig").Request {
        const actual = try self.nativeExecutionInstance();
        if (!std.mem.eql(u8, &actual, &request.native_instance)) return error.SqlRelationOwnerChanged;
        if (request.command == .begin_drain) {
            lock(&self.mutex);
            self.sql_persistence_ready = false;
            self.sql_persistence_draining = true;
            self.mutex.unlock();
        }
        const native = try self.context.relationLifetime(if (request.command == .begin_drain) .begin_drain else .observe, actual);
        if (request.command == .finish_drain) {
            if (std.mem.allEqual(u8, &native.clean_digest, 0) or !std.mem.eql(u8, &native.clean_digest, &request.clean_digest))
                return error.SqlRelationRecoveryIncomplete;
        } else if (request.command != .begin_drain and native.admission_closed != 0) return error.SqlRelationOwnerChanged;
        return try request.sign(root);
    }

    /// Call only after the metadata boot worker has proved the current
    /// enrolled reporter and completed the bounded old-owner recovery cut.
    pub fn admitRelationHost(self: *ProvisionedKernelOwnerSource, host: @import("antfly_local_sources").common_topology_records.RelationPersistence.Context, boot: @import("antfly_local_sources").common_topology_records.RelationPersistence.Boot) !void {
        try boot.validate();
        const actual = try self.nativeExecutionInstance();
        if (host.standby or host.connection != null or !std.mem.eql(u8, &host.execution_instance, &actual) or !std.mem.eql(u8, &boot.instance, &actual))
            return error.SqlRelationOwnerChanged;
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        self.sql_persistence_host = host;
        self.sql_persistence_boot = boot;
        self.sql_persistence_ready = true;
        self.sql_persistence_draining = false;
    }

    /// One bounded shutdown slice. Native closed-admission and owner proof
    /// counts, rather than this facade's cache size, determine completion.
    pub fn drainRelationHosts(self: *ProvisionedKernelOwnerSource) !abi.RelationLifetimeContextResult {
        const actual = try self.nativeExecutionInstance();
        _ = try self.context.relationLifetime(.begin_drain, actual);
        lock(&self.mutex);
        self.sql_persistence_ready = false;
        self.sql_persistence_draining = true;
        var visited: usize = 0;
        const visit_limit = @min(self.entries.items.len, 128);
        while (visited < visit_limit and self.entries.items.len != 0) : (visited += 1) {
            if (self.sql_persistence_drain_cursor >= self.entries.items.len) self.sql_persistence_drain_cursor = 0;
            const index = self.sql_persistence_drain_cursor;
            const entry = self.entries.items[index];
            if (entry.sql_persistence != null and entry.active_users == 0 and !entry.exclusive_active and !entry.closing) {
                const before_close = self.entries.items.len;
                self.destroyEntryAtIndexLocked(index);
                if (self.entries.items.len >= before_close) self.sql_persistence_drain_cursor += 1;
            } else self.sql_persistence_drain_cursor += 1;
        }
        self.mutex.unlock();
        return try self.context.relationLifetime(.observe, actual);
    }

    pub fn storageContextHandle(self: *ProvisionedKernelOwnerSource) !?*anyopaque {
        try self.ensureContextConfigured();
        return self.context.handle;
    }

    /// Read one durable restore marker through the compiled storage owner.
    /// The returned wire value is fully owned by `alloc` and contains no DB
    /// implementation types.
    pub fn restoreState(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?restore_state_contract.State {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, descriptor.path, descriptor.view(), false, .transient, .{});
        defer lease.deinit();
        defer lease.requestTransientRetirement();
        var response = (try lease.owner().restoreStateJson(table_name)) orelse return null;
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(
            restore_state_contract.State,
            alloc,
            response.bytes(),
            .{},
        );
        defer parsed.deinit();
        return try parsed.value.cloneAlloc(alloc);
    }

    /// Run one bounded projection reconciliation while borrowing the same
    /// resident physical owner used by table reads and writes.
    /// Holds a generation for an admitted transition without exposing its DB.
    pub const TransitionLease = struct {
        lease: Lease,

        pub fn deinit(self: *TransitionLease) void {
            self.lease.deinit();
        }

        pub fn reconcile(self: *TransitionLease, apply_store: *data_apply_client.RaftApplyStore, alloc: std.mem.Allocator, expected: ?data_apply_client.AppliedDataBatch) !data_apply_client.RaftApplyStore.ReconcileResult {
            return try apply_store.reconcileAuthoritativeOwner(alloc, self.lease.owner().handle, self.lease.entry.group_id, expected, false, 256, 2 * 1024 * 1024);
        }

        pub fn mergeArtifactsPage(self: *TransitionLease, alloc: std.mem.Allocator, range: db_types.ByteRange, after_key: ?[]const u8) ![]db_types.BatchWrite {
            var response: abi.OwnedBytes = .{};
            try @import("kernel_error_identity").statusToError(abi.antfly_storage_owner_merge_artifacts_page(self.lease.owner().handle, &.{
                .table_name = .fromSlice(self.lease.entry.table_name),
                .range_start = .fromSlice(range.start),
                .range_end = .fromSlice(range.end),
                .after_key = .fromSlice(after_key orelse ""),
            }, &response));
            defer abi.antfly_storage_owner_buffer_destroy(&response);
            var page = try @import("../storage/data_raft_projection_wire.zig").decodeGroupStatePageAlloc(alloc, response.slice());
            errdefer page.deinit(alloc);
            const rows = try alloc.alloc(db_types.BatchWrite, page.entries.len);
            for (page.entries, 0..) |entry, i| rows[i] = .{ .key = entry.key, .value = entry.value };
            alloc.free(page.entries);
            return rows;
        }

        pub fn mergeCleanupKeysPage(self: *TransitionLease, alloc: std.mem.Allocator, range: db_types.ByteRange, after_key: ?[]const u8) ![]db_types.BatchWrite {
            var response: abi.OwnedBytes = .{};
            try @import("kernel_error_identity").statusToError(abi.antfly_storage_owner_merge_cleanup_keys_page(self.lease.owner().handle, &.{
                .table_name = .fromSlice(self.lease.entry.table_name),
                .range_start = .fromSlice(range.start),
                .range_end = .fromSlice(range.end),
                .after_key = .fromSlice(after_key orelse ""),
            }, &response));
            defer abi.antfly_storage_owner_buffer_destroy(&response);
            var page = try @import("../storage/data_raft_projection_wire.zig").decodeGroupStatePageAlloc(alloc, response.slice());
            errdefer page.deinit(alloc);
            const rows = try alloc.alloc(db_types.BatchWrite, page.entries.len);
            for (page.entries, 0..) |entry, i| rows[i] = .{ .key = entry.key, .value = entry.value };
            alloc.free(page.entries);
            return rows;
        }

        fn relationalRead(self: *TransitionLease, comptime T: type, alloc: std.mem.Allocator, request: @import("antfly_local_sources").storage_db_relational_transition_contract.Request) !T {
            var encoded: std.Io.Writer.Allocating = .init(alloc);
            defer encoded.deinit();
            var json: std.json.Stringify = .{ .writer = &encoded.writer };
            try @import("antfly_local_sources").storage_db_relational_integrity_json.write(request, &json);
            var response: abi.OwnedBytes = .{};
            try kernel_error_identity.statusToError(abi.antfly_storage_owner_relational_transition_read(self.lease.owner().handle, &.{
                .table_name = .fromSlice(self.lease.entry.table_name),
                .request_json = .fromSlice(encoded.written()),
            }, &response));
            defer abi.antfly_storage_owner_buffer_destroy(&response);
            // Handoff pages/manifests borrow the caller's bounded request
            // arena, exactly like their native counterparts. Never retain ABI
            // response bytes after the owner releases its output buffer.
            return std.json.parseFromSliceLeaky(T, alloc, response.slice(), .{ .allocate = .alloc_always });
        }

        /// Read bounded lifecycle metadata from this exact transition owner.
        /// The caller establishes its Raft read barrier before acquiring the
        /// lease. In particular, an unpublished split destination must not be
        /// re-resolved through the public table catalog here.
        pub fn readRelationalTopologyJson(self: *TransitionLease, alloc: std.mem.Allocator, mode: []const u8) ![]u8 {
            const Mode = @FieldType(@import("../raft/shard_ops.zig").TopologyReadRequest, "mode");
            _ = std.meta.stringToEnum(Mode, mode) orelse return error.InvalidArgument;
            const control = try std.json.Stringify.valueAlloc(alloc, .{ .mode = mode }, .{});
            defer alloc.free(control);
            const request = try table_reads.encodeStorageKernelLookupRequest(alloc, "", .{ .relational_topology_json = control });
            defer alloc.free(request);
            var response = try self.lease.owner().lookupJson(self.lease.entry.table_name, request);
            defer response.deinit();
            return alloc.dupe(u8, response.bytes());
        }

        pub fn relationalTopologyStatus(self: *TransitionLease) !@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Status {
            var arena = std.heap.ArenaAllocator.init(self.lease.source.alloc);
            defer arena.deinit();
            return self.relationalRead(@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Status, arena.allocator(), .{ .status = {} });
        }

        pub fn relationalHandoffManifest(self: *TransitionLease, alloc: std.mem.Allocator, source: @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence, destination: @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence, lower: []const u8, upper: []const u8, primary_sequence: u64) !@import("antfly_local_sources").storage_db_relational_integrity_handoff_contract.Manifest {
            return self.relationalRead(@import("antfly_local_sources").storage_db_relational_integrity_handoff_contract.Manifest, alloc, .{ .manifest = .{ .source = source, .destination = destination, .lower = lower, .upper = upper, .primary_sequence = primary_sequence } });
        }

        pub fn relationalHandoffPage(self: *TransitionLease, alloc: std.mem.Allocator, manifest: @import("antfly_local_sources").storage_db_relational_integrity_handoff_contract.Manifest, progress: @import("antfly_local_sources").storage_db_relational_integrity_handoff_contract.Progress) !@import("antfly_local_sources").storage_db_relational_integrity_handoff_contract.Page {
            return self.relationalRead(@import("antfly_local_sources").storage_db_relational_integrity_handoff_contract.Page, alloc, .{ .page = .{ .manifest = manifest, .progress = progress } });
        }
    };

    pub fn leaseTransitionOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: descriptor_contract.Descriptor) !TransitionLease {
        const path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer self.alloc.free(path);
        return .{ .lease = try self.acquireDescriptor(group_id, table_name, path, descriptor) };
    }

    pub fn reconcileDataRaftProjection(
        self: *ProvisionedKernelOwnerSource,
        apply_store: *data_apply_client.RaftApplyStore,
        work_alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        expected: ?data_apply_client.AppliedDataBatch,
        capture_handoff: bool,
        max_page_entries: usize,
        max_page_bytes: usize,
    ) !data_apply_client.RaftApplyStore.ReconcileResult {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return try apply_store.reconcileAuthoritativeOwner(
            work_alloc,
            lease.owner().handle,
            group_id,
            expected,
            capture_handoff,
            max_page_entries,
            max_page_bytes,
        );
    }

    /// Borrow both resident group owners for one complete local split/merge
    /// phase. Acquisition is globally ordered so inverse group pairs cannot
    /// deadlock, while argument order remains source/destination or
    /// donor/receiver at the compiled ABI.
    pub fn runLocalTransition(
        self: *ProvisionedKernelOwnerSource,
        apply_store: ?*data_apply_client.RaftApplyStore,
        primary_group_id: u64,
        secondary_group_id: u64,
        table_name: []const u8,
        request: client.LocalTransitionRequest,
    ) !client.LocalTransitionResult {
        if (primary_group_id == secondary_group_id or
            request.primary_group_id != primary_group_id or
            request.secondary_group_id != secondary_group_id or
            !std.mem.eql(u8, request.table_name.slice(), table_name))
        {
            return error.InvalidTransitionRequest;
        }

        var primary_lease: ?Lease = null;
        defer if (primary_lease) |*lease| lease.deinit();
        var secondary_lease: ?Lease = null;
        defer if (secondary_lease) |*lease| lease.deinit();
        const primary_path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            primary_group_id,
        });
        defer self.alloc.free(primary_path);
        const secondary_path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            secondary_group_id,
        });
        defer self.alloc.free(secondary_path);
        const primary_descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = self.visibleRootGeneration(primary_group_id),
            .identity = .{
                .table_id = request.table_id,
                .shard_id = request.source_identity_shard_id,
                .range_id = request.source_identity_range_id,
            },
            .schema_json = request.schema_json.slice(),
            .indexes_json = request.indexes_json.slice(),
        };
        const secondary_descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = self.visibleRootGeneration(secondary_group_id),
            .identity = .{
                .table_id = request.table_id,
                .shard_id = request.target_identity_shard_id,
                .range_id = request.target_identity_range_id,
            },
            .schema_json = request.schema_json.slice(),
            .indexes_json = request.indexes_json.slice(),
        };
        if (primary_group_id < secondary_group_id) {
            primary_lease = try self.acquireDescriptor(
                primary_group_id,
                table_name,
                primary_path,
                primary_descriptor,
            );
            secondary_lease = try self.acquireDescriptor(
                secondary_group_id,
                table_name,
                secondary_path,
                secondary_descriptor,
            );
        } else {
            secondary_lease = try self.acquireDescriptor(
                secondary_group_id,
                table_name,
                secondary_path,
                secondary_descriptor,
            );
            primary_lease = try self.acquireDescriptor(
                primary_group_id,
                table_name,
                primary_path,
                primary_descriptor,
            );
        }
        return try primary_lease.?.owner().localTransition(
            secondary_lease.?.owner(),
            if (apply_store) |store| store.handle else null,
            request,
        );
    }

    pub fn retireAll(self: *ProvisionedKernelOwnerSource) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const count = self.entries.items.len;
        for (self.entries.items) |entry| entry.retired = true;
        self.drainRetiredLocked(null, null);
        return count;
    }

    /// Existing leases keep their owner alive; retirement prevents admission
    /// while close drains storage workers outside the registry mutex.
    pub fn retireTable(self: *ProvisionedKernelOwnerSource, table_name: []const u8) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (!std.mem.eql(u8, entry.table_name, table_name)) continue;
            count += 1;
            entry.retired = true;
        }
        self.drainRetiredLocked(null, table_name);
        return count;
    }

    /// True when `incoming` carries a lower durable schema version than
    /// `current`. Table schemas persist a monotonic `version`; the storage
    /// kernel rejects opening a lower one as `SchemaVersionRegression`, so a
    /// caller presenting it can only be stale. Unversioned or unparseable
    /// schemas compare as "not older" and keep the conservative drain.
    fn schemaVersionRegresses(current: []const u8, incoming: []const u8) bool {
        const current_version = schemaVersionFromJson(current) orelse return false;
        const incoming_version = schemaVersionFromJson(incoming) orelse return false;
        return incoming_version < current_version;
    }

    fn schemaVersionFromJson(schema_json: []const u8) ?u32 {
        var scanner = std.json.Scanner.initCompleteInput(std.heap.page_allocator, schema_json);
        defer scanner.deinit();
        if ((scanner.next() catch return null) != .object_begin) return null;
        var depth: usize = 0;
        while (true) {
            const token = scanner.next() catch return null;
            switch (token) {
                .object_begin, .array_begin => depth += 1,
                .object_end, .array_end => {
                    if (depth == 0) return null;
                    depth -= 1;
                },
                .end_of_document => return null,
                .string => |key| if (depth == 0 and std.mem.eql(u8, key, "version")) {
                    const value = scanner.next() catch return null;
                    return switch (value) {
                        .number => |digits| std.fmt.parseInt(u32, digits, 10) catch null,
                        else => null,
                    };
                } else {
                    // Skip the value that follows this key or array element.
                    if (depth == 0) scanner.skipValue() catch return null;
                },
                else => {},
            }
        }
    }

    fn publicationPendingLocked(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) bool {
        for (self.publications.items) |publication| {
            if (publication.group_id == group_id and std.mem.eql(u8, publication.table_name, table_name)) return true;
        }
        return false;
    }

    fn beginNativeInitialization(self: *ProvisionedKernelOwnerSource, group: u64, table: []const u8) !void {
        if (group == 0 or table.len == 0) return error.InvalidArgument;
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        if (self.publicationPendingLocked(group, table)) return error.StorageReadTemporarilyUnavailable;
        const slot = try self.native_initializers.getOrPut(self.alloc, group);
        if (!slot.found_existing) slot.value_ptr.* = 0;
        slot.value_ptr.* = std.math.add(u32, slot.value_ptr.*, 1) catch return error.ResourceBudgetExceeded;
    }

    fn endNativeInitialization(self: *ProvisionedKernelOwnerSource, group: u64) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        const count = self.native_initializers.getPtr(group) orelse unreachable;
        std.debug.assert(count.* != 0);
        count.* -= 1;
        if (count.* == 0) _ = self.native_initializers.remove(group);
    }

    fn registerPublication(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) !*PendingPublication {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        if (self.publicationPendingLocked(group_id, table_name)) return error.StorageBusy;
        const publication = try self.alloc.create(PendingPublication);
        errdefer self.alloc.destroy(publication);
        publication.* = .{ .group_id = group_id, .table_name = try self.alloc.dupe(u8, table_name) };
        errdefer self.alloc.free(publication.table_name);
        try self.publications.append(self.alloc, publication);
        // Close admission before observing users or dropping the registry lock.
        // The gate outlives the last Entry, including an initially cold group.
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and std.mem.eql(u8, entry.table_name, table_name)) entry.retired = true;
        }
        return publication;
    }

    fn publicationDrained(self: *ProvisionedKernelOwnerSource, publication: *PendingPublication) bool {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.native_initializers.contains(publication.group_id)) return false;
        self.drainRetiredLocked(publication.group_id, publication.table_name);
        for (self.entries.items) |entry| {
            // Closing entries remain registered while owner workers drain.
            if (entry.group_id == publication.group_id and std.mem.eql(u8, entry.table_name, publication.table_name)) return false;
        }
        return true;
    }

    fn beginPublication(ptr: *anyopaque, request: storage_snapshot_source.PublicationRequest) !*anyopaque {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try request.cancellation.check();
        const publication = try self.registerPublication(request.group_id, request.table_name);
        errdefer endPublication(ptr, publication);
        // Admission is closed before invalidating work. Every physical-root
        // publisher (restore as well as Raft snapshots) must drain schema
        // tickets through the same control worker, not wait on their pins.
        self.discardSchemaApplyForRootReplacement(request.group_id);
        const deadline = std.Io.Clock.awake.now(request.io).nanoseconds + request.drain_timeout_ns;
        while (true) {
            try request.cancellation.check();
            if (self.publicationDrained(publication)) {
                try request.cancellation.check();
                return publication;
            }
            if (std.Io.Clock.awake.now(request.io).nanoseconds >= deadline) return error.StorageBusy;
            // Borrow the operation's I/O: cancellation and simulated time must
            // remain on the same runtime as the work whose leases are draining.
            try request.io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    fn endPublication(ptr: *anyopaque, handle: *anyopaque) void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const publication: *PendingPublication = @ptrCast(@alignCast(handle));
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.publications.items, 0..) |candidate, index| {
            if (candidate != publication) continue;
            _ = self.publications.orderedRemove(index);
            self.alloc.free(publication.table_name);
            self.alloc.destroy(publication);
            return;
        }
        unreachable;
    }

    fn drainRetiredLocked(self: *ProvisionedKernelOwnerSource, group_id: ?u64, table_name: ?[]const u8) void {
        self.reconcileApplyReleasesLocked();
        while (true) {
            const index = for (self.entries.items, 0..) |entry, i| {
                if (!entry.retired or entry.closing or entry.active_users != 0) continue;
                if (group_id) |id| if (entry.group_id != id) continue;
                if (table_name) |name| if (!std.mem.eql(u8, entry.table_name, name)) continue;
                break i;
            } else return;
            const before_close = self.entries.items.len;
            self.destroyEntryAtIndexLocked(index);
            if (self.entries.items.len >= before_close) return;
        }
    }

    fn retireTableGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.retireGroupAndWait(group_id, table_name);
        return {};
    }

    fn captureHotStandbySeedSnapshotGroupLocal(ptr: *anyopaque, group_id: u64, table_name: []const u8, token: []const u8, destination: []const u8) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        // Preparation opened the owner before the exclusive HA freeze. Never
        // resolve catalog metadata or open a competing owner inside that freeze.
        var lease = (try self.acquireIfPresent(group_id, table_name)) orelse return error.StorageKernelOwnerUnavailable;
        defer lease.deinit();
        try lease.owner().captureHotStandbySeedSnapshot(table_name, token, destination);
        return {};
    }

    fn prepareHotStandbySeedSnapshotGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        deadline_ns: u64,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = self.acquire(group_id, table_name) catch |err| {
            std.log.warn("HA seed owner acquisition failed group_id={d} err={s}", .{ group_id, @errorName(err) });
            return err;
        };
        defer lease.deinit();
        try lease.owner().prepareHotStandbySeedSnapshot(table_name, deadline_ns);
        return {};
    }

    /// Prevent new admissions to every resident generation for a dropped
    /// group, then wait for already-admitted work to release its leases before
    /// the caller moves or deletes the physical root.
    fn retireGroupAndWait(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !void {
        if (group_id == 0) return error.InvalidArgument;
        var wait_io_impl = std.Io.Threaded.init(self.alloc, .{});
        defer wait_io_impl.deinit();
        const wait_io = wait_io_impl.io();
        const deadline_ns = platform_time.monotonicNs() +| 5 * std.time.ns_per_s;
        const name_filter: ?[]const u8 = if (table_name.len == 0) null else table_name;
        while (true) {
            var active = false;
            {
                lock(&self.mutex);
                defer self.mutex.unlock();
                for (self.entries.items) |entry| {
                    if (entry.group_id == group_id and (name_filter == null or std.mem.eql(u8, entry.table_name, name_filter.?))) entry.retired = true;
                }
                self.drainRetiredLocked(group_id, name_filter);
                for (self.entries.items) |entry| {
                    if (entry.group_id == group_id and (name_filter == null or std.mem.eql(u8, entry.table_name, name_filter.?))) active = true;
                }
            }
            if (!active) return;
            if (platform_time.monotonicNs() >= deadline_ns) return error.StorageBusy;
            try wait_io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
        }
    }

    fn prepareSnapshot(
        ptr: *anyopaque,
        request: storage_snapshot_source.PrepareRequest,
    ) !*anyopaque {
        _ = ptr;
        const snapshot = try client.Snapshot.prepare(.{
            .path = .fromSlice(request.path),
            .table_name = .fromSlice(request.table_name),
            .group_id = request.group_id,
            .lsm_root_generation = request.lsm_root_generation,
            .identity_table_id = request.identity.table_id,
            .identity_shard_id = request.identity.shard_id,
            .identity_range_id = request.identity.range_id,
            .schema_json = .fromSlice(request.schema_json),
            .indexes_json = .fromSlice(request.indexes_json),
            .encoded_snapshot = .fromSlice(request.encoded_snapshot),
            .projection_store = request.projection_store,
            .expected_applied_index = request.expected_applied_index,
        });
        return snapshot.handle orelse error.StorageKernelFailure;
    }

    const EncodedRestoreRequest = struct {
        alloc: std.mem.Allocator,
        manifest_json: []u8,
        request: abi.RestorePrepareRequest,
        cancellation: db_types.CancellationToken,

        fn cancelled(ptr: ?*anyopaque) callconv(.c) u8 {
            const token: *const db_types.CancellationToken = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(token.isCancelled());
        }

        fn bindCancellation(self: *EncodedRestoreRequest) void {
            self.request.cancellation_ctx = &self.cancellation;
            self.request.cancellation_fn = cancelled;
        }

        pub fn deinit(self: *EncodedRestoreRequest) void {
            self.alloc.free(self.manifest_json);
            self.* = undefined;
        }
    };

    fn encodeRestoreRequest(
        self: *ProvisionedKernelOwnerSource,
        request: storage_snapshot_source.RestoreRequest,
    ) !EncodedRestoreRequest {
        const manifest_json = try std.json.Stringify.valueAlloc(self.alloc, request.manifest.*, .{
            .emit_null_optional_fields = false,
        });
        return .{
            .alloc = self.alloc,
            .manifest_json = manifest_json,
            .cancellation = request.cancellation,
            .request = .{
                .path = .fromSlice(request.path),
                .table_name = .fromSlice(request.table_name),
                .group_id = request.group_id,
                .lsm_root_generation = request.lsm_root_generation,
                .has_identity_namespace = @intFromBool(request.identity != null),
                .identity_table_id = if (request.identity) |identity| identity.table_id else 0,
                .identity_shard_id = if (request.identity) |identity| identity.shard_id else 0,
                .identity_range_id = if (request.identity) |identity| identity.range_id else 0,
                .backup_root = .fromSlice(request.backup_root),
                .backup_id = .fromSlice(request.manifest.backup_id),
                .artifact_backup_id = .fromSlice(request.artifact_backup_id),
                .source_identity = .fromSlice(request.source_identity),
                .snapshot_path = .fromSlice(request.shard.snapshot_path),
                .expected_artifact_size_bytes = request.shard.artifact_size_bytes,
                .expected_artifact_sha256 = .fromSlice(request.shard.artifact_sha256),
                .expected_native_manifest_size_bytes = request.shard.native_manifest_size_bytes,
                .expected_native_manifest_sha256 = .fromSlice(request.shard.native_manifest_sha256),
                .manifest_json = .fromSlice(manifest_json),
            },
        };
    }

    fn prepareRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !storage_snapshot_source.RestorePreparation {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        encoded.bindCancellation();
        return switch (try client.Snapshot.prepareRestore(encoded.request)) {
            .prepared => |snapshot| .{ .prepared = .{
                .source = self.snapshotSource(),
                .handle = snapshot.handle orelse return error.StorageKernelFailure,
            } },
            .already_imported => .already_imported,
        };
    }

    fn reconcileRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        encoded.bindCancellation();
        try client.Snapshot.reconcileRestore(encoded.request);
    }

    fn repairPublishedRestore(
        ptr: *anyopaque,
        request: storage_snapshot_source.RestoreRequest,
    ) !void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var encoded = try self.encodeRestoreRequest(request);
        defer encoded.deinit();
        encoded.bindCancellation();
        var lease = try self.acquire(request.group_id, request.table_name);
        defer lease.deinit();
        try lease.owner().repairRestore(&encoded.request);
    }

    fn promoteSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.promote();
    }

    fn publishPreparedSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        const durability_uncertain = try snapshot.publishPrepared();
        // The compiled storage context owns the caches used by every resident
        // table owner. The control-only caller cannot invalidate them through
        // its legacy DB-cache path, so make the physical publication boundary
        // explicit before a new owner can open the replacement generation.
        try self.ensureContextConfigured();
        try self.context.invalidateCaches();
        return durability_uncertain;
    }

    fn commitSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.commit();
    }

    fn rollbackSnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) !void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        try snapshot.rollback();
    }

    fn destroySnapshot(ptr: *anyopaque, snapshot_handle: *anyopaque) void {
        _ = ptr;
        var snapshot = client.Snapshot{ .handle = snapshot_handle };
        snapshot.deinit();
    }

    /// Apply one already-committed local Raft batch without consulting the
    /// catalog from the apply thread. The descriptor is part of the replicated
    /// envelope, so every replica opens the same generation and identity.
    pub fn applyPreparedReplicatedBatchGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        req: db_types.BatchRequest,
    ) !void {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{
            self.replica_root_dir,
            group_id,
        });
        defer alloc.free(path);
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor);
        defer lease.deinit();
        var response = try lease.owner().replicatedBatchJson(table_name, request_json);
        defer response.deinit();
    }

    /// Apply one exact committed Raft entry. The provider persists the log
    /// identity in the same physical batch as the mutation, making retries
    /// after an apply-watermark crash safe across the compiled boundary.
    pub fn applyPreparedReplicatedBatchGroupLocalAtRaftEntry(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        req: db_types.BatchRequest,
        raft_term: u64,
        raft_index: u64,
    ) !void {
        // The Raft progress driver owns the committed entry and its retry
        // checkpoint. Yield admission conflicts to it immediately: waiting for
        // another owner lease here stalls unrelated groups and can deadlock a
        // maintenance callback waiting for this same progress driver.
        var lease = if (!@import("builtin").is_test or self.apply_control_started.load(.acquire))
            try self.acquireApplyOnly(group_id, table_name, descriptor)
        else blk: {
            // Direct owner tests and non-hosted callers have no progress
            // driver or borrowed control executor. Hosted Raft always binds
            // the control worker before attaching the state machine.
            const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
            defer alloc.free(path);
            break :blk self.acquireDescriptorOnce(group_id, table_name, path, descriptor, .shared, .resident, .{ .historical_raft_apply = true }) catch |err| switch (err) {
                error.StorageKernelOwnerTransitionRequired => return error.StorageBusy,
                else => return err,
            };
        };
        defer lease.deinit();
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var response = try lease.owner().replicatedBatchAtRaftEntryJson(
            table_name,
            request_json,
            raft_term,
            raft_index,
        );
        defer response.deinit();
    }

    pub fn waitForCurrentSyncGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        sync_level: db_types.SyncLevel,
    ) !void {
        return try self.waitForCurrentSyncGroupLocalWithCancellation(group_id, table_name, sync_level, .none);
    }

    pub fn waitForCurrentSyncGroupLocalWithCancellation(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        sync_level: db_types.SyncLevel,
        cancellation: db_types.CancellationToken,
    ) !void {
        switch (sync_level) {
            .propose, .write => return,
            .full_text, .enrichments, .full_index => {},
        }
        const owner_sync_level: abi.SyncLevel = switch (sync_level) {
            .propose => .propose,
            .write => .write,
            .full_text => .full_text,
            .enrichments => .enrichments,
            .full_index => .full_index,
        };
        var target: abi.SyncTarget = .{};
        const Slice = struct {
            outer: db_types.CancellationToken,
            until: u64,
            fn cancelled(ptr: *const anyopaque) bool {
                const slice: *const @This() = @ptrCast(@alignCast(ptr));
                return slice.outer.isCancelled() or platform_time.monotonicNs() >= slice.until;
            }
        };
        while (true) {
            try cancellation.check();
            if (target.captured != 0 and platform_time.monotonicNs() >= target.deadline_ns) return error.Timeout;
            const complete = blk: {
                // Owner transitions may be prerequisites of derived progress.
                // Keep a fixed replay target, but never retain this lease for
                // the entire visibility wait and block those transitions.
                var lease = try self.acquire(group_id, table_name);
                defer lease.deinit();
                const slice = Slice{ .outer = cancellation, .until = @min(platform_time.monotonicNs() +| 50 * std.time.ns_per_ms, if (target.captured != 0) target.deadline_ns else std.math.maxInt(u64)) };
                lease.owner().waitForSyncTargetWithCancellation(table_name, owner_sync_level, &target, .{ .ptr = &slice, .is_cancelled_fn = Slice.cancelled }) catch |err| switch (err) {
                    // The storage owner preserves enrichment cancellation's
                    // typed identity across the native boundary. An expired
                    // local slice must yield the lease and resume; only the
                    // outer request token may cancel the whole barrier.
                    error.Canceled, error.EnrichmentWaitCanceled => break :blk false,
                    else => return err,
                };
                break :blk true;
            };
            if (complete) return;
            try cancellation.check();
            platform_time.sleepNs(std.time.ns_per_ms);
        }
    }

    pub fn applyHotStandbyReplicationRecordGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        record: hot_standby_replication_record.RecordView,
    ) !void {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().applyHotStandbyReplicationRecord(table_name, .{
            .record_kind = @backingInt(record.kind),
            .payload_codec = @backingInt(record.payload_codec),
            .flags = record.flags,
            .cluster_id = record.cluster_id,
            .shard_id = record.shard_id,
            .table_id = record.table_id,
            .timeline_id = record.timeline_id,
            .epoch = record.epoch,
            .lsn = record.lsn,
            .previous_lsn = record.previous_lsn,
            .commit_timestamp_ns = record.commit_timestamp_ns,
            .payload = record.payload,
        });
    }

    /// Read the persisted private bootstrap without asking the public catalog
    /// to invent a route for an unpublished owner. Warm reads pin the exact
    /// current generation; cold reads stay inside the compiled storage owner.
    pub fn inspectUnpublishedNativeImage(self: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, group: u64, expected: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor) !@import("antfly_local_sources").common_topology_records.RelationPersistence.UnpublishedImageInspection {
        try self.ensureContextConfigured();
        const path = try std.fmt.allocPrint(a, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group });
        defer a.free(path);
        const encoded = try std.json.Stringify.valueAlloc(a, expected, .{});
        defer a.free(encoded);
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(null, &.{ .operation = .inspect_native_root, .context = self.context.handle, .path = .fromSlice(path), .native_inspection_json = .fromSlice(encoded) }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
        var result = try std.json.parseFromSlice(@import("antfly_local_sources").common_topology_records.RelationPersistence.UnpublishedImageInspection, a, output.slice(), .{});
        defer result.deinit();
        return result.value;
    }

    /// This is a private replacement producer, not a recovery-capable user
    /// owner open. Source publication excludes all old owners and cold openers
    /// before the fresh registered metadata attempt is observed.
    pub fn replaceNativeEmptyImageAtRoot(self: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, metadata_group: u64, group: u64, table: []const u8, path: []const u8, descriptor: descriptor_contract.Descriptor, creation_scope: @import("../metadata/sql_relation_attestation.zig").CreationScope, expected_boot: @import("../metadata/sql_relation_boots.zig").Record, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, io: std.Io, context: request_operation.RequestContext) !@import("../metadata/sql_relation_attestation.zig").SignedBinding {
        const P = @import("antfly_local_sources").common_topology_records.RelationPersistence;
        const restore = @import("antfly_local_sources").storage_db_restore_staging_contract;
        try context.ensureActive();
        try self.ensureContextConfigured();
        const normalized = try platformDeadlineContext(context);
        var publication = try self.snapshotSource().beginPublication(.{ .group_id = group, .table_name = table, .io = io, .drain_timeout_ns = 5 * std.time.ns_per_s, .cancellation = normalized.cancellation });
        defer publication.deinit();
        // Enroll this producer only AFTER the publication drain, otherwise it
        // would wait on its own count. Shutdown must still join this disk work.
        lock(&self.mutex);
        if (self.quiescing) {
            self.mutex.unlock();
            return error.Canceled;
        }
        const slot = self.native_initializers.getOrPut(self.alloc, group) catch |err| {
            self.mutex.unlock();
            return err;
        };
        if (!slot.found_existing) slot.value_ptr.* = 0;
        slot.value_ptr.* = std.math.add(u32, slot.value_ptr.*, 1) catch {
            self.mutex.unlock();
            return error.SqlProgramLimitExceeded;
        };
        self.mutex.unlock();
        defer self.endNativeInitialization(group);
        const observed = try self.catalog.sqlRelationBinding(a, .{ .node = expected_boot.node_id, .binding_table = descriptor.identity.table_id, .creation_scope = creation_scope }, normalized.deadline_ns);
        const boot = observed.record orelse return error.SqlRelationOwnerChanged;
        const attempt = observed.image_attempt orelse return error.SqlRelationRecoveryIncomplete;
        const admission = observed.empty_target orelse return error.SqlRelationRecoveryIncomplete;
        const lifetime = descriptor.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        const actual = try self.nativeExecutionInstance();
        if (!observed.registration_current or !std.meta.eql(boot, expected_boot) or boot.phase != .recovering or !boot.unclean_restart or
            !std.mem.eql(u8, &actual, &boot.instance) or attempt.boot.generation != boot.generation or !std.mem.eql(u8, &attempt.boot.instance, &actual) or
            attempt.initialized != null or !std.meta.eql(attempt.scope, creation_scope) or !attempt.previous_initialized.binding.descriptor.eql(lifetime)) return error.SqlRelationOwnerChanged;
        try attempt.validate();
        var bootstrap = try std.json.parseFromSlice(restore.OwnerBootstrap, a, descriptor.restore_bootstrap_json, .{});
        defer bootstrap.deinit();
        try admission.require(bootstrap.value.scope, lifetime, .{ .table_id = descriptor.identity.table_id, .shard_id = descriptor.identity.shard_id, .range_id = descriptor.identity.range_id }, observed.attempt);
        const permit: P.UninitializedAdmission = .{ .metadata_group_id = metadata_group, .boot_generation = boot.generation, .table_id = descriptor.identity.table_id, .shard_id = descriptor.identity.shard_id, .range_id = descriptor.identity.range_id, .descriptor_fingerprint = try lifetime.fingerprint(), .schema_digest = bootstrap.value.scope.target_schema_digest };
        const encoded = try std.json.Stringify.valueAlloc(a, .{ .descriptor = lifetime, .host = P.Context{ .node_id = lifetime.owner_node_id, .store_id = lifetime.owner_store_id, .execution_instance = actual, .generation_instance = lifetime.generation_instance, .data_generation = lifetime.data_generation }, .boot = P.Boot{ .instance = actual, .unclean_restart = true, .initializing = true, .uninitialized = permit }, .bootstrap = bootstrap.value, .attempt_number = attempt.number, .attempt_digest = try attempt.digest(), .previous_root = attempt.previous_initialized.binding.physical_root_incarnation }, .{});
        defer a.free(encoded);
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(null, &.{ .operation = .replace_native_empty, .context = self.context.handle, .path = .fromSlice(path), .native_image_json = .fromSlice(encoded) }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
        var captured = try std.json.parseFromSlice(P.NativeBinding, a, output.slice(), .{});
        defer captured.deinit();
        const signed = try @import("../metadata/sql_relation_attestation.zig").SignedBinding.sign(.{ .kind = .initialized, .metadata_group_id = metadata_group, .boot_generation = boot.generation, .identity = boot.identity, .reporter_incarnation = boot.reporter_incarnation, .binding = captured.value, .creation_scope = creation_scope }, root);
        try attempt.requireInitialized(signed);
        return signed;
    }

    pub fn readHotStandbyHiddenOwnerBootstrap(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_id: u64) !?std.json.Parsed(@import("antfly_local_sources").storage_db_restore_staging_contract.OwnerBootstrap) {
        try self.ensureContextConfigured();
        const generation = self.visibleRootGeneration(group_id);
        var resident: ?Lease = blk: {
            lock(&self.mutex);
            defer self.mutex.unlock();
            for (self.entries.items) |entry| {
                if (entry.group_id != group_id or entry.generation != generation or entry.retired or entry.closing) continue;
                if (!tryReserveEntryLeaseLocked(entry, .shared)) return error.StorageReadTemporarilyUnavailable;
                break :blk .{ .source = self, .entry = entry };
            }
            break :blk null;
        };
        defer if (resident) |*lease| lease.deinit();
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(if (resident) |*lease| lease.owner().handle else null, &.{ .operation = .read_bootstrap, .context = self.context.handle, .path = .fromSlice(path), .table_id = table_id }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
        if (generation != self.visibleRootGeneration(group_id)) return error.StorageKernelOwnerTransitionRequired;
        if (output.len == 0) return null;
        return try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_restore_staging_contract.OwnerBootstrap, alloc, output.slice(), .{ .allocate = .alloc_always });
    }

    pub fn readHiddenInitialChildRecord(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_id: u64,
    ) !?@import("antfly_local_sources").storage_db_relational_initial_child_publication.Record {
        try self.ensureContextConfigured();
        const generation = self.visibleRootGeneration(group_id);
        var resident: ?Lease = blk: {
            lock(&self.mutex);
            defer self.mutex.unlock();
            for (self.entries.items) |entry| {
                if (entry.group_id != group_id or entry.generation != generation or entry.retired or entry.closing) continue;
                if (!tryReserveEntryLeaseLocked(entry, .shared)) return error.StorageReadTemporarilyUnavailable;
                break :blk .{ .source = self, .entry = entry };
            }
            break :blk null;
        };
        defer if (resident) |*lease| lease.deinit();
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(if (resident) |*lease| lease.owner().handle else null, &.{ .operation = .read_initial_child, .context = self.context.handle, .path = .fromSlice(path), .table_id = table_id }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
        if (output.len == 0) return null;
        if (generation != self.visibleRootGeneration(group_id)) return error.InitialChildPublicationChanged;
        var parsed = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_relational_initial_child_publication.Record, alloc, output.slice(), .{ .ignore_unknown_fields = false });
        defer parsed.deinit();
        try parsed.value.validate();
        return parsed.value;
    }

    /// Trusted local retirement preflight only. Reads the cold AICH without
    /// assuming metadata's current table ID; the caller must still compare
    /// the exact ticket before any unlink or ACK.
    pub fn readInitialChildRetirementRecord(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
    ) !?@import("antfly_local_sources").storage_db_relational_initial_child_publication.Record {
        return self.readHiddenInitialChildRecord(alloc, group_id, 0);
    }

    pub fn cancelColdInitialChildRetirementAtPath(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_path: []const u8, expected: @import("antfly_local_sources").storage_db_relational_initial_child_publication.Record, cancel_revision: u64) !void {
        try self.ensureContextConfigured();
        const path = try std.fs.path.join(alloc, &.{ group_path, "table-db" });
        defer alloc.free(path);
        const payload = try std.json.Stringify.valueAlloc(alloc, .{ .expected = expected, .cancel_revision = cancel_revision }, .{});
        defer alloc.free(payload);
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(null, &.{ .operation = .cancel_initial_child_retirement, .context = self.context.handle, .path = .fromSlice(path), .table_id = expected.namespace.table_id, .snapshot_token = .fromSlice(payload) }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
    }

    /// The retirement worker holds the placement lease and drains all owners
    /// before its final check. A renamed root must be read at its exact trash
    /// path after a crash; resolving by group ID could inspect a replacement.
    pub fn readColdInitialChildRetirementRecordAtPath(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_path: []const u8,
    ) !?@import("antfly_local_sources").storage_db_relational_initial_child_publication.Record {
        try self.ensureContextConfigured();
        const path = try std.fs.path.join(alloc, &.{ group_path, "table-db" });
        defer alloc.free(path);
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(null, &.{
            .operation = .read_initial_child,
            .context = self.context.handle,
            .path = .fromSlice(path),
            .table_id = 0,
        }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
        if (output.len == 0) return null;
        var parsed = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_relational_initial_child_publication.Record, alloc, output.slice(), .{ .ignore_unknown_fields = false });
        defer parsed.deinit();
        try parsed.value.validate();
        return parsed.value;
    }

    pub fn captureHotStandbySeedHiddenReplicaSnapshot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, scope: [32]u8, snapshot_token: []const u8, destination_root: []const u8) !void {
        var descriptor = (try self.cachedRestoreDescriptor(alloc, group_id, table_name, scope)) orelse return error.RestoreStagingScopeChanged;
        defer descriptor.deinit(alloc);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor.view());
        defer lease.deinit();
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(lease.owner().handle, &.{ .operation = .capture_snapshot, .table_name = .fromSlice(table_name), .table_id = descriptor.descriptor.identity.table_id, .scope = scope, .snapshot_token = .fromSlice(snapshot_token), .destination_root = .fromSlice(destination_root) }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
    }

    pub fn captureHotStandbySeedReplicaSnapshot(self: *ProvisionedKernelOwnerSource, table_name: []const u8, group_id: u64, snapshot_token: []const u8, destination_root: []const u8) !void {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(lease.owner().handle, &.{ .operation = .capture_public_snapshot, .table_name = .fromSlice(table_name), .table_id = lease.entry.identity.table_id, .snapshot_token = .fromSlice(snapshot_token), .destination_root = .fromSlice(destination_root) }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
    }

    /// Hidden initial FK owners are already resident from the authenticated
    /// private placement projection. Public catalog lookup must not be used
    /// before the final metadata publication CAS.
    pub fn captureHotStandbySeedInitialChildReplicaSnapshot(self: *ProvisionedKernelOwnerSource, table_name: []const u8, group_id: u64, snapshot_token: []const u8, destination_root: []const u8) !void {
        var lease = try self.acquirePreparedOwner(group_id, table_name);
        defer lease.deinit();
        if (lease.entry.initial_child_bootstrap_json.len == 0) return error.InvalidInitialChildPublication;
        var output: abi.OwnedBytes = .{};
        try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(lease.owner().handle, &.{ .operation = .capture_public_snapshot, .table_name = .fromSlice(table_name), .table_id = lease.entry.identity.table_id, .snapshot_token = .fromSlice(snapshot_token), .destination_root = .fromSlice(destination_root) }, &output));
        defer abi.antfly_storage_owner_buffer_destroy(&output);
    }

    pub fn applyHotStandbyHiddenOwnerRecord(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: [32]u8, record: hot_standby_replication_record.RecordView) !void {
        var descriptor = (try self.cachedRestoreDescriptor(alloc, group_id, table_name, scope)) orelse return error.RestoreStagingScopeChanged;
        defer descriptor.deinit(alloc);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor.view());
        defer lease.deinit();
        try lease.owner().applyHotStandbyReplicationRecord(table_name, .{
            .record_kind = @backingInt(record.kind),
            .payload_codec = @backingInt(record.payload_codec),
            .flags = record.flags,
            .cluster_id = record.cluster_id,
            .shard_id = record.shard_id,
            .table_id = record.table_id,
            .timeline_id = record.timeline_id,
            .epoch = record.epoch,
            .lsn = record.lsn,
            .previous_lsn = record.previous_lsn,
            .commit_timestamp_ns = record.commit_timestamp_ns,
            .payload = record.payload,
        });
    }

    pub fn applyHotStandbyInitialChildOwnerRecord(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, record: hot_standby_replication_record.RecordView) !void {
        var lease = try self.acquirePreparedOwner(group_id, table_name);
        defer lease.deinit();
        if (lease.entry.initial_child_bootstrap_json.len == 0 or lease.entry.identity.table_id != record.table_id or
            lease.entry.identity.shard_id != record.shard_id) return error.InitialChildPublicationChanged;
        try lease.owner().applyHotStandbyReplicationRecord(table_name, .{
            .record_kind = @backingInt(record.kind),
            .payload_codec = @backingInt(record.payload_codec),
            .flags = record.flags,
            .cluster_id = record.cluster_id,
            .shard_id = record.shard_id,
            .table_id = record.table_id,
            .timeline_id = record.timeline_id,
            .epoch = record.epoch,
            .lsn = record.lsn,
            .previous_lsn = record.previous_lsn,
            .commit_timestamp_ns = record.commit_timestamp_ns,
            .payload = record.payload,
        });
    }

    const BackupShardWire = struct {
        group_id: u64,
        range_id: u64 = 0,
        doc_identity_shard_id: u64 = 0,
        doc_identity_range_id: u64 = 0,
        split_attempt_epoch: u64 = 0,
        start_key: []const u8,
        end_key: ?[]const u8 = null,
        snapshot_path: []const u8,
        artifact_size_bytes: u64 = 0,
        artifact_sha256: []const u8 = "",
        native_manifest_size_bytes: u64 = 0,
        native_manifest_sha256: []const u8 = "",
        accepted_generation_summary: []const backup_contract.SourceGenerationAdmissionSummaryEntry = &.{},
        accepted_generation_summary_digest: ?[32]u8 = null,
    };

    fn backupTableGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        plan: backup_contract.TableBackupPlan,
    ) !?[]backup_contract.ShardSnapshot {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try plan.ensureActive();
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        const cohort_json = if (plan.relational_cohort_fence) |fence| try std.json.Stringify.valueAlloc(alloc, fence, .{}) else null;
        defer if (cohort_json) |json| alloc.free(json);
        const handle = try backup_contract.sealedHandleForGroup(plan.sealed_handles, group_id);
        const handle_json = if (handle) |sealed| try std.json.Stringify.valueAlloc(alloc, sealed.handle, .{}) else null;
        defer if (handle_json) |json| alloc.free(json);
        var cancellation = plan.cancellation;
        var response = try lease.owner().backupWithControl(.{
            .table_name = .fromSlice(table_name),
            .backup_root = .fromSlice(plan.backup_root),
            .backup_id = .fromSlice(plan.backup_id),
            .format = @backingInt(switch (plan.format) {
                .native => abi.BackupFormat.native,
                .portable => abi.BackupFormat.portable,
            }),
            .cohort_json = .fromSlice(cohort_json orelse ""),
            .sealed_handle_json = .fromSlice(handle_json orelse ""),
            .execution_deadline_ns = plan.deadline_ns orelse 0,
            .has_execution_deadline = @intFromBool(plan.deadline_ns != null),
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        });
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(
            []BackupShardWire,
            alloc,
            response.bytes(),
            .{},
        );
        defer parsed.deinit();
        if (parsed.value.len != 1 or parsed.value[0].group_id != group_id)
            return error.StorageKernelFailure;
        const shards = try alloc.alloc(backup_contract.ShardSnapshot, parsed.value.len);
        var initialized: usize = 0;
        errdefer {
            for (shards[0..initialized]) |shard| shard.deinit(alloc);
            alloc.free(shards);
        }
        for (parsed.value, 0..) |shard, i| {
            const start_key = try alloc.dupe(u8, shard.start_key);
            errdefer alloc.free(start_key);
            const end_key = if (shard.end_key) |value| try alloc.dupe(u8, value) else null;
            errdefer if (end_key) |value| alloc.free(value);
            const snapshot_path = try alloc.dupe(u8, shard.snapshot_path);
            errdefer alloc.free(snapshot_path);
            const artifact_sha256 = if (shard.artifact_sha256.len > 0)
                try alloc.dupe(u8, shard.artifact_sha256)
            else
                "";
            errdefer if (artifact_sha256.len > 0) alloc.free(@constCast(artifact_sha256));
            const native_manifest_sha256 = if (shard.native_manifest_sha256.len > 0)
                try alloc.dupe(u8, shard.native_manifest_sha256)
            else
                "";
            errdefer if (native_manifest_sha256.len > 0) alloc.free(@constCast(native_manifest_sha256));
            const accepted_generation_summary = try table_writes.cloneAcceptedGenerationSummary(alloc, shard.accepted_generation_summary);
            errdefer {
                for (accepted_generation_summary) |entry| {
                    alloc.free(entry.child_table_name);
                    alloc.free(entry.constraint_name);
                }
                if (accepted_generation_summary.len != 0) alloc.free(@constCast(accepted_generation_summary));
            }
            shards[i] = .{
                .group_id = shard.group_id,
                .range_id = shard.range_id,
                .doc_identity_shard_id = shard.doc_identity_shard_id,
                .doc_identity_range_id = shard.doc_identity_range_id,
                .split_attempt_epoch = shard.split_attempt_epoch,
                .start_key = start_key,
                .end_key = end_key,
                .snapshot_path = snapshot_path,
                .artifact_size_bytes = shard.artifact_size_bytes,
                .artifact_sha256 = artifact_sha256,
                .native_manifest_size_bytes = shard.native_manifest_size_bytes,
                .native_manifest_sha256 = native_manifest_sha256,
                .accepted_generation_summary = accepted_generation_summary,
                .accepted_generation_summary_digest = shard.accepted_generation_summary_digest,
            };
            initialized += 1;
        }
        return shards;
    }

    fn backupPinControl(ptr: *anyopaque, alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, input: @import("antfly_local_sources").storage_db_native_backup_seal_contract.Request, control: backup_contract.BackupOperationControl) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try control.ensureActive();
        const json = try std.json.Stringify.valueAlloc(alloc, input, .{});
        defer alloc.free(json);
        var cancellation = control.cancellation;
        const native_control: abi.ControlledJsonOperationRequest = .{
            .table_name = .fromSlice(table_name),
            .request_json = .fromSlice(json),
            .execution_deadline_ns = control.deadline_ns,
            .has_execution_deadline = 1,
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        };
        if (input != .seal) {
            try self.ensureContextConfigured();
            var response = try self.context.reclaimBackupPinJson(.{ .control = native_control, .replica_root = .fromSlice(self.replica_root_dir), .group_id = group_id });
            defer response.deinit();
            return try alloc.dupe(u8, response.bytes());
        }
        var lease = try self.acquireWithControls(group_id, table_name, .{ .execution_deadline_ns = control.deadline_ns, .cancellation = control.cancellation });
        defer lease.deinit();
        var response = try lease.owner().backupPinControlJson(native_control);
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    /// Private lifecycle bridge: materialize from the admitted durable pin,
    /// then return its certificate for a separate replicated publication CAS.
    pub fn prepareOnlineSourcePublication(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: @import("antfly_local_sources").storage_db_online_source_contract.Scope, context: @import("antfly_local_sources").api_operation.RequestContext) !@import("antfly_local_sources").storage_source_snapshot.Certificate {
        try context.ensureActive();
        try scope.validate();
        if (scope.fence.owner_group_id != group_id) return error.OnlineSourceScopeChanged;
        const json = try std.json.Stringify.valueAlloc(alloc, scope, .{});
        defer alloc.free(json);
        const native_context = try platformDeadlineContext(context);
        var cancellation = context.cancellation;
        var lease = try self.acquireWithControls(group_id, table_name, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation = cancellation });
        defer lease.deinit();
        try context.ensureActive();
        var response = try lease.owner().prepareSourcePinPublicationJson(.{ .table_name = .fromSlice(table_name), .request_json = .fromSlice(json), .execution_deadline_ns = native_context.deadline_ns orelse 0, .has_execution_deadline = @intFromBool(native_context.deadline_ns != null), .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(@import("antfly_local_sources").storage_source_snapshot.Certificate, alloc, response.bytes(), .{});
        defer parsed.deinit();
        _ = try parsed.value.encode();
        if (!parsed.value.cut.namespace.eql(scope.fence.namespace)) return error.SourceSnapshotCutMismatch;
        try context.ensureActive();
        return parsed.value;
    }

    pub fn onlineSourceArtifact(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request: @import("antfly_local_sources").storage_db_source_artifact_transfer.Request, context: @import("antfly_local_sources").api_operation.RequestContext) ![]u8 {
        try context.ensureActive();
        try request.scope().validate();
        if (request.scope().fence.owner_group_id != group_id) return error.OnlineSourceScopeChanged;
        const json = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(json);
        if (json.len > 2 * 1024 * 1024) return error.InvalidSourceSnapshot;
        const native_context = try platformDeadlineContext(context);
        var cancellation = context.cancellation;
        var lease = try self.acquireWithControls(group_id, table_name, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation = cancellation });
        defer lease.deinit();
        try context.ensureActive();
        var response = try lease.owner().sourceArtifactJson(.{ .table_name = .fromSlice(table_name), .request_json = .fromSlice(json), .execution_deadline_ns = native_context.deadline_ns orelse 0, .has_execution_deadline = @intFromBool(native_context.deadline_ns != null), .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        try context.ensureActive();
        return alloc.dupe(u8, response.bytes());
    }

    pub fn onlineMergeIo(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request: @import("antfly_local_sources").storage_db_online_merge_io_contract.Request, context: @import("antfly_local_sources").api_operation.RequestContext) ![]u8 {
        try context.ensureActive();
        try request.validate();
        if (request.ownerGroup() != group_id) return error.OnlineSourceScopeChanged;
        // An immutable artifact may be served by another donor replica. Its
        // inner published-ledger scope/certificate is the authority, not a
        // new current read cut. All row/status operations require read-index.
        if (request.operation != .artifact) try feature_reads.FeatureReads.init(self.read_safety_barrier).prepareLookupWithConsistency(group_id, "", .{
            .execution_deadline_ns = context.deadline_ns,
            .execution_io = context.deadline_io,
            .cancellation = context.cancellation,
        }, .read_index);
        const json = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(json);
        if (json.len > @import("online_merge_io.zig").contract.max_request_bytes) return error.InvalidMergePage;
        const native_context = try platformDeadlineContext(context);
        var cancellation = context.cancellation;
        // Lifecycle I/O addresses the durable physical owner, which outlives
        // its serving route after cutover. Borrow the already admitted owner
        // without re-resolving that removed route. The read-index above and
        // the native owner's exact namespace/published-ledger scope checks
        // still authorize every operation; ordinary query opens retain their
        // catalog descriptor checks. Cold owners are prepared independently
        // by placement recovery, never by an unbounded admin read here.
        var lease = self.acquirePreparedOwner(group_id, table_name) catch
            try self.acquireWithControls(group_id, table_name, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation = cancellation });
        defer lease.deinit();
        try context.ensureActive();
        var response = try lease.owner().onlineMergeIoJson(.{ .table_name = .fromSlice(table_name), .request_json = .fromSlice(json), .execution_deadline_ns = native_context.deadline_ns orelse 0, .has_execution_deadline = @intFromBool(native_context.deadline_ns != null), .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        try context.ensureActive();
        return alloc.dupe(u8, response.bytes());
    }

    pub fn ownerCountForTest(self: *ProvisionedKernelOwnerSource) usize {
        lock(&self.mutex);
        defer self.mutex.unlock();
        return self.entries.items.len;
    }

    /// Pre-open the same resident owner used by reads and writes. Warmup must
    /// not create a second status-only DB in the distributed compilation unit;
    /// acquiring and releasing the owner performs descriptor validation
    /// without opening a second DB in distributed code.
    pub fn warmTableGroup(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !void {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, descriptor.path, descriptor.view(), false, .transient, .{});
        defer lease.deinit();
        // Warmup can overlap startup catch-up or foreground admission. Retire
        // only a transient owner, while the lease still pins it. Borrowing
        // observers drain before close; foreground adoption retains it.
        // Never retire by group after
        // releasing the lease: that can close another operation's owner.
        lease.requestTransientRetirement();
    }

    /// Prepare a cold lifecycle owner on the control lane and retain it for
    /// the next bounded merge turn. Unlike speculative startup warmup, that
    /// turn needs the owner to survive after this lease drains. Descriptor
    /// validation still uses the current catalog/lifecycle contract; ordinary
    /// replica retirement and source teardown retain ownership of closing it.
    pub fn prepareOnlineMergeOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) !void {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, descriptor.path, descriptor.view(), false, .resident, .{});
        defer lease.deinit();
    }

    /// Apply the latest catalog schema/index contract to the already-resident
    /// physical group owner, or open that owner with the contract when absent.
    /// Routing and catalog selection remain in distributed control.
    pub fn reconcileTableGroup(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !abi.ReconcileResult {
        return try self.reconcileTableGroupStep(group_id, table_name, null, false);
    }

    /// Advance at most one durable index-repair intent in addition to the
    /// desired-state pass. Node scheduling decides when to request that work.
    pub fn reconcileTableGroupStep(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !abi.ReconcileResult {
        return try self.reconcileTableGroupStepWithRetention(
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
            true,
            null,
        );
    }

    /// A terminal schema owner cannot acknowledge public serving readiness
    /// while its registry still carries a fence-deferred catalog. Reuse the
    /// full catalog reconciler under this control turn's deadline, retaining
    /// the ready owner rather than waiting for an unrelated index repair.
    pub fn reconcileServingCatalogAtRoot(self: *ProvisionedKernelOwnerSource, group: u64, table: []const u8, context: request_operation.RequestContext) !abi.ReconcileResult {
        try context.ensureActive();
        return self.reconcileTableGroupStepWithRetention(group, table, null, false, true, context);
    }

    fn reconcileTableGroupStepWithRetention(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
        retain_cold_owner: bool,
        context: ?request_operation.RequestContext,
    ) !abi.ReconcileResult {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        return self.reconcileDescriptorCatalog(group_id, table_name, descriptor.path, descriptor.view(), target_index_name, advance_index_repair, retain_cold_owner, context);
    }

    fn reconcileDescriptorCatalog(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
        retain_cold_owner: bool,
        context: ?request_operation.RequestContext,
    ) !abi.ReconcileResult {
        var lease = if (context) |request| control: {
            const normalized = try platformDeadlineContext(request);
            // Serving reconciliation is a control-round inspection, not an
            // admitted structural writer. Yield without closing foreground
            // admission when a retained statement already pins this owner.
            break :control (try self.tryAcquireServingCatalogOwner(group_id, table_name, path, descriptor, normalized)) orelse return .{ .state = .busy };
        } else (try self.acquireDescriptorForReconcile(
            group_id,
            table_name,
            path,
            descriptor,
            retain_cold_owner or advance_index_repair,
            if (retain_cold_owner) .resident else .transient,
        )) orelse return .{ .state = .busy };
        defer lease.deinit();
        errdefer lease.requestTransientRetirement();
        const result = lease.owner().reconcile(
            table_name,
            descriptor.schema_json,
            descriptor.indexes_json,
            target_index_name,
            advance_index_repair,
        ) catch |err| {
            lease.retireAfterConfigurationFailure();
            return err;
        };
        self.noteWholeCatalogReconciled(lease.entry, target_index_name, result.state);
        if (!retain_cold_owner) lease.requestTransientRetirement();
        return result;
    }

    fn noteWholeCatalogReconciled(self: *ProvisionedKernelOwnerSource, entry: *Entry, target_index_name: ?[]const u8, state: abi.ReconcileState) void {
        // A named repair cannot prove the full catalog contract, and busy or
        // degraded outcomes cannot release public admission. The periodic
        // full-table pass retries after the physical fence lifts.
        if (target_index_name != null or (state != .complete and state != .repair_pending)) return;
        lock(&self.mutex);
        entry.catalog_deferred = false;
        self.mutex.unlock();
    }

    fn reconcileTableGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?table_write_source.LocalStructuralReconcileResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const result = try self.reconcileTableGroupStep(
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
        );
        return localStructuralReconcileResult(result);
    }

    fn localStructuralReconcileResult(
        result: abi.ReconcileResult,
    ) table_write_source.LocalStructuralReconcileResult {
        return .{
            .state = switch (result.state) {
                .complete => .complete,
                .repair_pending => .repair_pending,
                .busy => .busy,
                .degraded => .degraded,
                .restore_repair_pending => .restore_repair_pending,
            },
            .indexes_added = result.indexes_added,
            .indexes_removed = result.indexes_removed,
            .indexes_pending = result.indexes_pending,
            .repair_discovered = result.repair_discovered,
            .repair_attempted = result.repair_attempted,
            .repair_repaired = result.repair_repaired,
            .repair_remaining = result.repair_remaining,
            .repair_terminal = result.repair_terminal,
            .repair_paused = result.repair_paused,
            .repair_busy = result.repair_busy,
            .repair_disk_waits = result.repair_disk_waits,
            .next_retry_at_ms = result.next_retry_at_ms,
            .restore_repair_attempted = result.restore_repair_attempted,
            .restore_repair_progressed = result.restore_repair_progressed,
            .restore_repair_pending = result.restore_repair_pending,
        };
    }

    fn reconcileTableGroupLocalTransient(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
    ) !?table_write_source.LocalStructuralReconcileResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const result = try self.reconcileTableGroupStepWithRetention(
            group_id,
            table_name,
            target_index_name,
            advance_index_repair,
            false,
            null,
        );
        return localStructuralReconcileResult(result);
    }

    const RepairControlsBridge = struct {
        options: db_types.ArtifactRepairRunOptions,
        deadline_ns: u64 = 0,

        fn cancelled(ptr: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(self.options.cancelled());
        }
        fn yieldRequested(ptr: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(if (self.options.yield_check) |check| check.requested() else platform_time.monotonicNs() >= self.deadline_ns);
        }
        fn activationAllowed(ptr: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            return @intFromBool(if (self.options.activation_check) |check| check.current() catch false else true);
        }
        fn wire(self: *@This()) abi.RepairControls {
            self.deadline_ns = platform_time.monotonicNs() +| 50 * std.time.ns_per_ms;
            return .{
                .context = self,
                .cancelled = cancelled,
                .yield_requested = yieldRequested,
                .activation_allowed = activationAllowed,
                .owner_epoch = self.options.owner_epoch,
                .capacity_domain_lo = @truncate(self.options.capacity_domain_id),
                .capacity_domain_hi = @truncate(self.options.capacity_domain_id >> 64),
                .estimated_candidate_bytes = self.options.estimated_candidate_bytes,
                .max_activation_gap_sequences = self.options.max_activation_gap_sequences,
                .max_convergence_rounds = self.options.max_convergence_rounds,
                .max_activation_pause_ms = self.options.max_activation_pause_ms,
            };
        }
    };

    fn reconcileTableGroupLocalObserved(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        target_index_name: ?[]const u8,
        advance_index_repair: bool,
        repair_options: db_types.ArtifactRepairRunOptions,
        retain_cold_owner: bool,
    ) !?table_write_source.LocalStructuralReconcileObservation {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (repair_options.cancelled()) return error.Canceled;
        if (repair_options.yield_check) |check| if (check.requested()) return .{ .result = .{ .state = .busy } };
        var descriptor = try self.loadDescriptorWithDeadline(self.alloc, group_id, table_name, repair_options.admission_deadline_ns);
        defer descriptor.deinit(self.alloc);
        if (repair_options.cancelled()) return error.Canceled;
        if (repair_options.yield_check) |check| if (check.requested()) return .{ .result = .{ .state = .busy } };
        const configured = if (advance_index_repair)
            self.tryAcquireConfiguredRepair(group_id, table_name, descriptor.view(), target_index_name)
        else
            null;
        var lease = if (configured) |ready| ready.lease else (try self.acquireDescriptorForReconcile(
            group_id,
            table_name,
            descriptor.path,
            descriptor.view(),
            // Only an explicit structural caller may install writer preference.
            // A scheduled repair returns busy immediately behind live leases.
            retain_cold_owner,
            if (retain_cold_owner) .resident else .transient,
        )) orelse return .{ .result = .{ .state = .busy } };
        defer lease.deinit();
        errdefer lease.requestTransientRetirement();

        var result = if (configured) |ready| ready.result else lease.owner().reconcile(
            table_name,
            descriptor.schema_json,
            descriptor.indexes_json,
            target_index_name,
            false,
        ) catch |err| {
            lease.retireAfterConfigurationFailure();
            return err;
        };
        if (configured == null) self.noteWholeCatalogReconciled(lease.entry, target_index_name, result.state);
        if (advance_index_repair and result.restore_repair_pending == 0) {
            if (configured == null) {
                const owned_target = if (target_index_name) |target| try self.alloc.dupe(u8, target) else null;
                lock(&self.mutex);
                if (lease.entry.repair_target) |old| self.alloc.free(old);
                lease.entry.repair_target = owned_target;
                lease.entry.repair_configuration = result;
                self.mutex.unlock();
            }
            if (lease.exclusive) lease.downgrade();
            var controls = RepairControlsBridge{ .options = repair_options };
            const repair = try lease.owner().repairIndex(table_name, target_index_name, controls.wire());
            const added = result.indexes_added;
            const removed = result.indexes_removed;
            const pending = result.indexes_pending;
            result = repair;
            result.indexes_added = added;
            result.indexes_removed = removed;
            result.indexes_pending = pending;
            if (repair.state == .complete and pending != 0) {
                // Admission may have left cleanup/activation debt. Verify it
                // under a new nonblocking structural lease on the next pass.
                lock(&self.mutex);
                lease.entry.repair_configuration = null;
                self.mutex.unlock();
                result.state = .busy;
            }
        }
        var response = lease.owner().runtimeStatusJson(table_name) catch |err| switch (err) {
            // Runtime status is deliberately best effort and returns busy
            // rather than waiting behind a concurrent Raft apply writer. The
            // structural reconcile above is already authoritative, so retain
            // its result and let the periodic status refresher observe the
            // owner after apply releases its writer guard.
            error.StorageBusy => null,
            else => return err,
        };
        defer if (response) |*value| value.deinit();
        var observed: ?runtime_status.LocalTableRuntimeStatus = if (response) |*value| observed: {
            var parsed = try std.json.parseFromSlice(
                runtime_status.LocalTableRuntimeStatus,
                alloc,
                value.bytes(),
                .{},
            );
            defer parsed.deinit();
            var status = try parsed.value.clone(alloc);
            status.group_id = group_id;
            // Retain the provider's source-target proof; replacing metadata
            // with defaults would erase the sequence sampled with these stats.
            status.metadata.updated_at_ns = platform_time.monotonicNs();
            status.metadata.source = .live_writer_publish;
            status.metadata.freshness = .fresh;
            status.metadata.lsm_root_generation = lease.entry.generation;
            break :observed status;
        } else null;
        errdefer if (observed) |*status| status.deinit(alloc);
        const retain_for_background_work = if (observed) |status|
            runtimeStatusNeedsResidentOwner(status)
        else
            // The status probe lost a best-effort race with Raft apply. Keep
            // this otherwise-cold owner resident so the periodic refresher can
            // publish the exact generation proof once the writer guard drains.
            true;

        // A transient startup inspection normally gives the cold owner back
        // immediately. Managed enrichment and index catch-up are different:
        // their retry scheduler lives inside that owner, so retiring it here
        // strands durable work until an unrelated foreground request happens
        // to reopen the group. Keep only owners with observed background debt;
        // idle groups preserve the bounded transient-open contract.
        if (retain_for_background_work) lease.retain() else lease.requestTransientRetirement();
        return .{
            .result = localStructuralReconcileResult(result),
            .runtime_status = observed,
        };
    }

    const ConfiguredRepair = struct { lease: Lease, result: abi.ReconcileResult };

    fn tryAcquireConfiguredRepair(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: descriptor_contract.Descriptor, target: ?[]const u8) ?ConfiguredRepair {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.closing or entry.retired or entry.generation != descriptor.lsm_root_generation or
                !entry.identity.eql(descriptor.identity) or !std.mem.eql(u8, entry.schema_json, descriptor.schema_json) or
                !std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) or !std.meta.eql(entry.table_storage, descriptor.table_storage) or
                !std.meta.eql(entry.sql_persistence, descriptor.sql_persistence)) return null;
            const result = entry.repair_configuration orelse return null;
            if ((target == null) != (entry.repair_target == null)) return null;
            if (target) |name| if (!std.mem.eql(u8, entry.repair_target.?, name)) return null;
            if (!tryReserveEntryLeaseLocked(entry, .shared)) return null;
            return .{ .lease = .{ .source = self, .entry = entry }, .result = result };
        }
        return null;
    }

    fn runtimeStatusNeedsResidentOwner(status: runtime_status.LocalTableRuntimeStatus) bool {
        const enrichment = status.stats.enrichment;
        if (enrichment.retrying or
            enrichment.target_sequence > enrichment.applied_sequence or
            enrichment.active_embed_batch_items != 0)
        {
            return true;
        }
        if (status.stats.async_indexing.startup.active or
            status.stats.async_indexing.dense_catch_up.active or
            status.stats.async_indexing.bulk_coalescing.active_session)
        {
            return true;
        }
        for (status.stats.indexes) |index| {
            if (index.backfill_active or
                index.catch_up_active or
                index.replay_catch_up_required or
                index.replay_target_sequence > index.replay_applied_sequence)
            {
                return true;
            }
        }
        return false;
    }

    fn preflightGroupedSourceBatchGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_types.BatchRequest) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        if (!try lease.owner().preflightWriteAdmissionJson(table_name, "")) return {};
        const encoded = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(encoded);
        _ = try lease.owner().preflightWriteAdmissionJson(table_name, encoded);
        return {};
    }

    fn preflightWriteAdmissionGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().preflightWriteAdmission(table_name);
        return {};
    }

    fn findMedianKeyGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = (try lease.owner().findMedianKey(table_name)) orelse return null;
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    fn visibleRootGeneration(self: *const ProvisionedKernelOwnerSource, group_id: u64) u64 {
        return if (self.group_visible_root_generation) |source|
            source.visibleRootGenerationForGroup(group_id)
        else
            table_reads.backend_current_root_generation;
    }

    fn lock(mutex: *std.atomic.Mutex) void {
        platform_sync.lockYielding(mutex);
    }

    fn destroyEntryAtIndexLocked(self: *ProvisionedKernelOwnerSource, index: usize) void {
        self.destroyEntryAtIndexLockedMode(index, false);
    }

    fn destroyEntryAtIndexLockedMode(self: *ProvisionedKernelOwnerSource, index: usize, force_abandon: bool) void {
        const entry = self.entries.items[index];
        std.debug.assert(entry.active_users == 0 and !entry.exclusive_active and !entry.closing);
        if (!force_abandon and entry.close_retry_at_ns > platform_time.monotonicNs()) return;
        entry.retired = true;
        entry.closing = true;
        // Keep the closing owner registered until all storage work has drained.
        // A concurrent open or cleanup must not mistake a removed pointer for
        // permission to reopen, move, or delete the same physical root.
        self.mutex.unlock();
        if (@import("builtin").is_test) if (self.test_apply_control_hooks) |hooks| {
            if (hooks.before_close) |before_close| before_close(hooks.ptr);
        };
        if (entry.sql_persistence != null) {
            entry.owner.deinitFallible() catch |err| {
                if (force_abandon) {
                    std.log.warn("volatile owner forcibly abandoned during source destruction group_id={} err={s}", .{ entry.group_id, @errorName(err) });
                    entry.owner.deinit();
                } else {
                    lock(&self.mutex);
                    entry.closing = false;
                    entry.close_failure = err;
                    if (!entry.close_failure_reported) std.log.warn("volatile owner close retained for retry group_id={} err={s}", .{ entry.group_id, @errorName(err) });
                    entry.close_failure_reported = true;
                    entry.close_retry_at_ns = platform_time.monotonicNs() +| entry.close_retry_delay_ns;
                    entry.close_retry_delay_ns = @min(entry.close_retry_delay_ns *| 2, std.time.ns_per_s);
                    return;
                }
            };
        } else entry.owner.deinit();
        lock(&self.mutex);
        for (self.entries.items, 0..) |candidate, current_index| {
            if (candidate == entry) {
                _ = self.entries.orderedRemove(current_index);
                break;
            }
        } else unreachable;
        self.alloc.free(entry.table_name);
        self.alloc.free(entry.schema_json);
        self.alloc.free(entry.indexes_json);
        self.alloc.free(entry.restore_bootstrap_json);
        self.alloc.free(entry.initial_child_bootstrap_json);
        descriptor_contract.freeInitialRange(self.alloc, entry.initial_range);
        if (entry.restore) |*identity| identity.deinit(self.alloc);
        if (entry.repair_target) |target| self.alloc.free(target);
        self.alloc.destroy(entry);
    }

    fn release(self: *ProvisionedKernelOwnerSource, entry: *Entry, exclusive: bool, apply_only: bool) void {
        if (apply_only) {
            std.debug.assert(!exclusive);
            // The active_users pin keeps entry alive until the control worker
            // (or final quiesce) accounts for this atomic release.
            _ = entry.pending_apply_releases.fetchAdd(1, .release);
            if (self.apply_control_started.load(.acquire)) self.apply_control_wake.set(self.apply_control_io.?);
            return;
        }
        lock(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(entry.active_users > 0);
        if (exclusive) {
            std.debug.assert(entry.exclusive_active);
            std.debug.assert(entry.active_users == 1);
            entry.exclusive_active = false;
        }
        entry.active_users -= 1;
        if (entry.active_users == 0 and (entry.catalog_drain_pending or
            (entry.transient_retirement_pending and !entry.resident)))
            entry.retired = true;
        if (!entry.retired or entry.active_users != 0) return;
        for (self.entries.items, 0..) |candidate, index| {
            if (candidate != entry) continue;
            self.destroyEntryAtIndexLocked(index);
            return;
        }
        unreachable;
    }

    fn reconcileApplyReleasesLocked(self: *ProvisionedKernelOwnerSource) void {
        for (self.entries.items) |entry| {
            const released = entry.pending_apply_releases.swap(0, .acq_rel);
            std.debug.assert(released <= entry.active_users);
            entry.active_users -= released;
            if (entry.active_users == 0 and (entry.catalog_drain_pending or
                (entry.transient_retirement_pending and !entry.resident)))
                entry.retired = true;
        }
    }

    fn snapshotOwnerLeases(
        self: *ProvisionedKernelOwnerSource,
        best_effort: bool,
        skip_bulk_ingest: bool,
    ) !?[]Lease {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();

        var count: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.retired or entry.catalog_drain_pending or entry.transient_retirement_pending or entry.exclusive_pending or entry.exclusive_active or (skip_bulk_ingest and entry.bulk_ingest_active.load(.acquire))) continue;
            count += 1;
        }
        const leases = try self.alloc.alloc(Lease, count);
        var initialized: usize = 0;
        for (self.entries.items) |entry| {
            if (entry.retired or entry.catalog_drain_pending or entry.transient_retirement_pending or entry.exclusive_pending or entry.exclusive_active or (skip_bulk_ingest and entry.bulk_ingest_active.load(.acquire))) continue;
            entry.active_users += 1;
            leases[initialized] = .{ .source = self, .entry = entry };
            initialized += 1;
        }
        std.debug.assert(initialized == count);
        return leases;
    }

    fn releaseMaintenanceLeases(self: *ProvisionedKernelOwnerSource, leases: []Lease) void {
        for (leases) |*lease| lease.deinit();
        self.alloc.free(leases);
    }

    fn maintenanceEntryLimit(self: *ProvisionedKernelOwnerSource, best_effort: bool) ?usize {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();
        return self.entries.items.len;
    }

    /// Maintenance must never pin unrelated owners across a slow storage step.
    /// The cursor may skip an entry removed during the round; the next round
    /// will see it if it is still resident. Newly appended entries wait too.
    fn nextMaintenanceLease(
        self: *ProvisionedKernelOwnerSource,
        cursor: *usize,
        limit: usize,
        best_effort: bool,
        skip_bulk_ingest: bool,
    ) ?Lease {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();
        while (cursor.* < limit and cursor.* < self.entries.items.len) {
            const entry = self.entries.items[cursor.*];
            cursor.* += 1;
            if (entry.retired or entry.catalog_drain_pending or entry.transient_retirement_pending or entry.exclusive_pending or
                entry.exclusive_active or (skip_bulk_ingest and entry.bulk_ingest_active.load(.acquire))) continue;
            entry.active_users += 1;
            return .{ .source = self, .entry = entry };
        }
        return null;
    }

    fn selectedMaintenanceLease(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        generation: u64,
        best_effort: bool,
    ) ?Lease {
        if (best_effort) {
            if (!self.mutex.tryLock()) return null;
        } else {
            lock(&self.mutex);
        }
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or entry.generation != generation or
                !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.retired or entry.catalog_drain_pending or entry.transient_retirement_pending or entry.exclusive_pending or
                entry.exclusive_active or entry.bulk_ingest_active.load(.acquire)) return null;
            entry.active_users += 1;
            return .{ .source = self, .entry = entry };
        }
        return null;
    }

    fn runLsmMaintenanceRound(
        ptr: *anyopaque,
        best_effort: bool,
    ) !storage_maintenance_source.RoundResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(best_effort) orelse return .{};
        var cursor: usize = 0;
        var selected_name: ?[]u8 = null;
        defer if (selected_name) |name| self.alloc.free(name);
        var selected_group_id: u64 = 0;
        var selected_generation: u64 = 0;
        var selected_score: u64 = 0;
        var selected_due = false;
        while (self.nextMaintenanceLease(&cursor, limit, best_effort, true)) |borrowed| {
            var lease = borrowed;
            const status = lease.owner().maintenance(
                lease.entry.table_name,
                if (best_effort) .inspect_best_effort else .inspect,
            ) catch |err| {
                lease.deinit();
                if (best_effort) continue;
                return err;
            };
            const due = status.has_next_wake_delay != 0 and status.next_wake_delay_ns == 0;
            if (due or status.maintenance_score != 0) {
                if (selected_name == null or
                    (due and !selected_due) or
                    (due == selected_due and status.maintenance_score > selected_score))
                {
                    const name = self.alloc.dupe(u8, lease.entry.table_name) catch |err| {
                        lease.deinit();
                        return err;
                    };
                    if (selected_name) |previous| self.alloc.free(previous);
                    selected_name = name;
                    selected_group_id = lease.entry.group_id;
                    selected_generation = lease.entry.generation;
                    selected_score = status.maintenance_score;
                    selected_due = due;
                }
            }
            lease.deinit();
        }
        const name = selected_name orelse return .{};
        var selected = self.selectedMaintenanceLease(selected_group_id, name, selected_generation, best_effort) orelse return .{};
        defer selected.deinit();
        const lease = &selected;
        const result = try lease.owner().maintenance(
            lease.entry.table_name,
            if (best_effort) .lsm_step_best_effort else .lsm_step,
        );
        return .{
            .progressed = result.progressed != 0,
            .group_id = lease.entry.group_id,
        };
    }

    fn runDensePostingMaintenanceRound(ptr: *anyopaque) !@import("storage_maintenance_source.zig").PostingRefreshProgress {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(true) orelse return .{ .pending = true };
        var cursor: usize = 0;

        var total: @import("storage_maintenance_source.zig").PostingRefreshProgress = .{};
        while (self.nextMaintenanceLease(&cursor, limit, true, true)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            const result = lease.owner().maintenance(
                lease.entry.table_name,
                .dense_posting_idle,
            ) catch |err| {
                std.log.warn("storage owner dense posting maintenance failed table={s} group_id={d} err={s}", .{
                    lease.entry.table_name,
                    lease.entry.group_id,
                    @errorName(err),
                });
                total.pending = true;
                continue;
            };
            total.repaired +|= @intCast(result.dense_steps);
            total.scanned +|= @intCast(result.dense_scanned);
            total.pending = total.pending or result.deferred != 0 or result.busy != 0;
        }
        if (cursor < limit) total.pending = true;
        return total;
    }

    fn targetAdvanced(
        ptr: ?*anyopaque,
        table_name: abi.BorrowedBytes,
        group_id: u64,
        sequence: u64,
        has_sequence: u8,
        identities_json: abi.BorrowedBytes,
    ) callconv(.c) void {
        const cache: *runtime_status.TableRuntimeSnapshotCache = @ptrCast(@alignCast(ptr orelse return));
        const target_sequence: ?u64 = if (has_sequence != 0) sequence else null;
        if (identities_json.len == 0) {
            cache.markGroupTargetObservationPending(table_name.slice(), group_id, target_sequence);
            return;
        }
        var identities = std.json.parseFromSlice(
            []db_types.IndexTargetVisibility,
            cache.alloc,
            identities_json.slice(),
            .{ .ignore_unknown_fields = true },
        ) catch {
            cache.markGroupTargetObservationPending(table_name.slice(), group_id, target_sequence);
            return;
        };
        defer identities.deinit();
        cache.markIndexTargetsObservationPending(table_name.slice(), group_id, identities.value, sequence);
    }

    pub fn withRuntimeStatusCache(self: *ProvisionedKernelOwnerSource, cache: *runtime_status.TableRuntimeSnapshotCache) *ProvisionedKernelOwnerSource {
        self.runtime_status_cache = cache;
        return self;
    }

    fn publishRuntimeStatuses(ptr: *anyopaque) void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (self.runtime_status_cache == null) return;
        const limit = self.maintenanceEntryLimit(true) orelse return;
        var cursor: usize = 0;
        while (self.nextMaintenanceLease(&cursor, limit, true, true)) |borrowed| {
            var lease = borrowed;
            self.refreshMaintenanceStatus(&lease);
            lease.deinit();
        }
    }

    fn refreshMaintenanceStatus(self: *ProvisionedKernelOwnerSource, lease: *Lease) void {
        const cache = self.runtime_status_cache orelse return;
        // Capture the table fence before observing the pinned generation.
        const token = cache.capturePublicationToken(lease.entry.table_name) catch return;
        var response = lease.owner().runtimeStatusJson(lease.entry.table_name) catch return;
        defer response.deinit();
        var parsed = std.json.parseFromSlice(runtime_status.LocalTableRuntimeStatus, self.alloc, response.bytes(), .{}) catch return;
        defer parsed.deinit();
        _ = cache.publishGroups(token, lease.entry.table_name, &.{parsed.value}) catch return;
    }

    fn publishDenseCheckpoints(ptr: *anyopaque) !db_types.NativePublicationResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(true) orelse return .{ .busy = true };
        var cursor: usize = 0;
        var combined: db_types.NativePublicationResult = .{};
        while (self.nextMaintenanceLease(&cursor, limit, true, true)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            const result = try lease.owner().maintenance(lease.entry.table_name, .publish_dense_checkpoints);
            if (result.published != 0) self.refreshMaintenanceStatus(&lease);
            combined.published += @intCast(result.published);
            combined.busy = combined.busy or result.busy != 0;
            combined.deferred = combined.deferred or result.deferred != 0;
        }
        if (cursor < limit) combined.busy = true;
        return combined;
    }

    fn runVectorBlockRound(ptr: *anyopaque) !usize {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(true) orelse return 0;
        var cursor: usize = 0;
        var steps: usize = 0;
        while (self.nextMaintenanceLease(&cursor, limit, true, true)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            self.refreshMaintenanceStatus(&lease);
            defer self.refreshMaintenanceStatus(&lease);
            const result = try lease.owner().maintenance(lease.entry.table_name, .vector_block_idle);
            steps += @intCast(result.dense_steps);
        }
        return steps;
    }

    fn maintenanceSnapshot(
        ptr: *anyopaque,
        best_effort: bool,
    ) !storage_maintenance_source.Snapshot {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(best_effort) orelse return .{};
        var cursor: usize = 0;
        var result = storage_maintenance_source.Snapshot{};
        while (self.nextMaintenanceLease(&cursor, limit, best_effort, true)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            result.owner_count += 1;
            const status = lease.owner().maintenance(
                lease.entry.table_name,
                if (best_effort) .inspect_best_effort else .inspect,
            ) catch continue;
            result.maintenance_score = @max(result.maintenance_score, status.maintenance_score);
            if (status.has_next_wake_delay != 0) {
                result.next_wake_delay_ns = if (result.next_wake_delay_ns) |current|
                    @min(current, status.next_wake_delay_ns)
                else
                    status.next_wake_delay_ns;
            }
        }
        return result;
    }

    pub fn loadDescriptor(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !LoadedDescriptor {
        return self.loadDescriptorWithDeadline(alloc, group_id, table_name, null);
    }

    /// Only an exact metadata-authorized hidden child may obtain a Raft
    /// descriptor without a public catalog route. The bootstrap travels in
    /// the committed entry so follower apply uses the same private identity.
    pub fn loadInitialChildDescriptor(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        expected: @import("antfly_local_sources").storage_db_relational_initial_child_publication.Bootstrap,
    ) !LoadedDescriptor {
        try expected.validate();
        var lease = try self.acquirePreparedOwner(group_id, table_name);
        defer lease.deinit();
        const entry = lease.entry;
        if (entry.initial_child_bootstrap_json.len == 0 or
            entry.identity.table_id != expected.namespace.table_id or
            entry.identity.shard_id != expected.namespace.shard_id or
            entry.identity.range_id != expected.namespace.range_id)
            return error.InitialChildPublicationChanged;
        var stored = std.json.parseFromSlice(@TypeOf(expected), alloc, entry.initial_child_bootstrap_json, .{ .ignore_unknown_fields = false }) catch return error.InvalidInitialChildPublication;
        defer stored.deinit();
        if (!stored.value.eql(expected)) return error.InitialChildPublicationChanged;
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        errdefer alloc.free(path);
        const schema_json = try alloc.dupe(u8, entry.schema_json);
        errdefer alloc.free(schema_json);
        const indexes_json = try alloc.dupe(u8, entry.indexes_json);
        errdefer alloc.free(indexes_json);
        const initial_range = try descriptor_contract.cloneInitialRange(alloc, entry.initial_range);
        errdefer descriptor_contract.freeInitialRange(alloc, initial_range);
        const bootstrap_json = try alloc.dupe(u8, entry.initial_child_bootstrap_json);
        return .{
            .path = path,
            .schema_json = schema_json,
            .indexes_json = indexes_json,
            .table_storage = entry.table_storage,
            .sql_persistence = entry.sql_persistence,
            .generation = entry.generation,
            .initial_range = initial_range,
            .identity = entry.identity,
            .initial_child_bootstrap_json = bootstrap_json,
        };
    }

    pub fn loadDescriptorWithDeadline(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        deadline_ns: ?u64,
    ) !LoadedDescriptor {
        return self.loadDescriptorWithBudget(alloc, group_id, table_name, self.catalog.budget(deadline_ns));
    }

    fn loadDescriptorWithBudget(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        budget: table_catalog.RoutingBudget,
    ) !LoadedDescriptor {
        var projection = (try table_catalog.tableGroupDescriptorProjectionControlled(
            alloc,
            self.catalog,
            table_name,
            group_id,
            budget,
        )) orelse return error.TableNotFound;
        errdefer projection.deinit(alloc);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        return .{
            .path = path,
            .schema_json = projection.schema_json,
            .indexes_json = projection.indexes_json,
            .table_storage = projection.table_storage,
            .sql_persistence = projection.sql_persistence,
            .initial_range = projection.initial_range,
            .restore = projection.restore,
            .generation = self.visibleRootGeneration(group_id),
            .identity = .{
                .table_id = projection.table_id,
                .shard_id = projection.doc_identity_shard_id,
                .range_id = projection.doc_identity_range_id,
            },
        };
    }

    const ReadControls = struct {
        native_route: ?metadata_api.CatalogRouteFence = null,
        native_lock_cleanup: bool = false,
        native_decision_owner: ?@import("antfly_local_sources").storage_transactions.ExternalDecisionOwner = null,
        native_initialized: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.NativeBinding = null,
        native_initialization: bool = false,
        /// Metadata-derived never-acknowledged admission. Unlike reset this
        /// carries no fabricated old physical UUID or transaction outcome.
        native_uninitialized: ?struct {
            host: @import("antfly_local_sources").common_topology_records.RelationPersistence.Context,
            boot: @import("antfly_local_sources").common_topology_records.RelationPersistence.Boot,
        } = null,
        /// Only an already initialized current hidden empty image can borrow
        /// a recovering host. New/replacement birth uses separate admission.
        native_empty_admission: ?@import("../metadata/sql_relation_empty_target.zig").Admission = null,
        native_empty_host: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.Context = null,
        native_adoption: ?struct {
            boot: @import("antfly_local_sources").common_topology_records.RelationPersistence.Boot,
            host: @import("antfly_local_sources").common_topology_records.RelationPersistence.Context,
        } = null,
        native_boot_observed: bool = false,
        execution_deadline_ns: ?u64 = null,
        execution_io: ?@import("antfly_runtime_abi").io_abi.Borrow = null,
        cancellation: ?db_types.CancellationToken = null,
        historical_raft_apply: bool = false,
        durable_catalog: bool = false,
        /// Schema installation retains its deferred serving fence. Reads and
        /// progress-only controls may borrow a configured current owner.
        require_historical_owner: bool = false,
        allow_deferred_catalog: bool = false,

        fn from(req: anytype) ReadControls {
            return .{ .execution_deadline_ns = req.execution_deadline_ns, .execution_io = if (@hasField(@TypeOf(req), "execution_io")) req.execution_io else null, .cancellation = req.cancellation };
        }

        fn withRoute(self: @This(), route: metadata_api.CatalogRouteFence) @This() {
            var controls = self;
            controls.native_route = route;
            return controls;
        }

        fn catalogBudget(self: ReadControls, catalog: table_catalog.CatalogSource) table_catalog.RoutingBudget {
            var budget = catalog.budget(catalog.deadlineFrom(.{ .deadline_ns = self.execution_deadline_ns, .io = self.execution_io }));
            budget.cancellation = self.cancellation orelse .none;
            return budget;
        }

        fn check(self: ReadControls) !void {
            const context: request_operation.RequestContext = .{
                .deadline_ns = self.execution_deadline_ns,
                .deadline_io = self.execution_io,
                .cancellation = self.cancellation orelse .none,
            };
            context.ensureActive() catch |err| return switch (err) {
                error.Canceled => error.Cancelled,
                error.DeadlineExceeded => error.Timeout,
                else => err,
            };
        }
    };

    const NativeOwnerFailure = enum { route_absent, route_descriptor, host_not_ready, host_missing, route_kind, host_descriptor, boot_binding, initialized_binding, routed_batch };
    fn nativeOwnerFailure(self: *ProvisionedKernelOwnerSource, stage: NativeOwnerFailure, group: u64, table: u64, detail: u64, _: ?*std.builtin.StackTrace) anyerror {
        if (comptime @import("builtin").mode == .debug) {
            const bit = @as(u32, 1) << @backingInt(stage);
            if ((self.native_owner_diagnostics.fetchOr(bit, .monotonic) & bit) == 0) {
                std.debug.print("native owner rejection stage={s} group={d} table={d} mismatch=0x{x}\n", .{ @tagName(stage), group, table, detail });
            }
        }
        return error.SqlRelationOwnerChanged;
    }

    fn nativeHostMismatch(descriptor: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor, context: @import("antfly_local_sources").common_topology_records.RelationPersistence.Context) u64 {
        // node, store, zero execution identity, standby, generation birth,
        // data generation, TEMP execution epoch. No owner credential bytes.
        return @as(u64, @intFromBool(context.node_id != descriptor.owner_node_id)) |
            (@as(u64, @intFromBool(context.store_id != descriptor.owner_store_id)) << 1) |
            (@as(u64, @intFromBool(std.mem.allEqual(u8, &context.execution_instance, 0))) << 2) |
            (@as(u64, @intFromBool(context.standby)) << 3) |
            (@as(u64, @intFromBool(!std.mem.eql(u8, &context.generation_instance, &descriptor.generation_instance))) << 4) |
            (@as(u64, @intFromBool(context.data_generation != descriptor.data_generation)) << 5) |
            (@as(u64, @intFromBool(descriptor.kind == .temporary and !std.mem.eql(u8, &context.execution_instance, &descriptor.generation_instance))) << 6);
    }

    fn acquire(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !Lease {
        return self.acquireWithControls(group_id, table_name, .{});
    }

    fn acquireWithControls(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        input: ReadControls,
    ) !Lease {
        var controls = input;
        try controls.check();
        var descriptor = try self.loadDescriptorWithBudget(self.alloc, group_id, table_name, controls.catalogBudget(self.catalog));
        defer descriptor.deinit(self.alloc);
        try controls.check();
        var view = descriptor.view();
        if (view.sql_persistence) |native| {
            // Public/raw callers cannot borrow a native owner merely by
            // knowing its physical name, even on a stale-read path. Receiver
            // code creates this process-local authority only after route,
            // current Boot and signed connection admission have succeeded.
            const admitted = controls.native_route orelse return self.nativeOwnerFailure(.route_absent, group_id, view.identity.table_id, 0, null);
            if (admitted.route.execution != .native_local or !native.eql(admitted.route.execution.native_local)) return self.nativeOwnerFailure(.route_descriptor, group_id, view.identity.table_id, 0, null);
            if (controls.native_decision_owner) |owner| try self.validateNativeDecisionHost(admitted, owner) else try self.validateNativeRoute(admitted);
            if (!controls.native_boot_observed) {
                controls.native_initialized = try self.currentNativeInitialized(admitted, controls.native_decision_owner != null);
                controls.native_boot_observed = true;
            }
        }

        if (controls.native_decision_owner != null) {
            if (try self.acquireRetainedNativeDecisionDebt(group_id, table_name, view)) |retained| return retained;
        }

        if (controls.durable_catalog) {
            // Routing still supplies namespace, restore admission and local
            // generation. Persisted schema/index/runtime settings own the cut;
            // never reconcile a prospective metadata schema while observing it.
            view.schema_json = "";
            view.indexes_json = "";
            view.table_storage = null;
            view.initial_range = null;
        }
        return self.acquireDescriptorWithMode(group_id, table_name, descriptor.path, view, false, .resident, controls);
    }

    /// Private metadata-job controls carry their own independently validated
    /// namespace/purpose. They do not borrow a SQL connection's row authority.
    /// Public/raw reads must continue through acquireWithControls above.
    fn acquireControlWithControls(self: *ProvisionedKernelOwnerSource, group: u64, table: []const u8, input: ReadControls) !Lease {
        try input.check();
        var descriptor = try self.loadDescriptorWithBudget(self.alloc, group, table, input.catalogBudget(self.catalog));
        defer descriptor.deinit(self.alloc);
        if (descriptor.sql_persistence == null) return self.acquireWithControls(group, table, input);
        var controls = input;
        controls.historical_raft_apply = false;
        var view = descriptor.view();
        if (controls.durable_catalog) {
            view.schema_json = "";
            view.indexes_json = "";
            view.table_storage = null;
            view.initial_range = null;
        }
        return self.acquireDescriptorWithMode(group, table, descriptor.path, view, false, .resident, controls);
    }

    /// Only an immutable phase-two owner may finish debt on this exact live
    /// handle. An I/O-failed/changed root is never revived, nor is the entry
    /// unretired for new user work. The native manager checks its stored owner.
    fn acquireRetainedNativeDecisionDebt(self: *ProvisionedKernelOwnerSource, group: u64, table: []const u8, descriptor: descriptor_contract.Descriptor) !?Lease {
        if (descriptor.sql_persistence == null) return null;
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        for (self.entries.items) |entry| {
            if (entry.group_id != group or !std.mem.eql(u8, entry.table_name, table)) continue;
            if (!entry.retired) return null;
            const close_failure = entry.close_failure orelse return error.SqlRelationOwnerUnavailable;
            if (entry.closing or close_failure != @as(anyerror, error.SqlRelationRecoveryIncomplete) or
                !exactApplyDescriptor(entry, descriptor)) return error.SqlRelationOwnerUnavailable;
            if (!tryReserveEntryLeaseLocked(entry, .shared)) return error.StorageBusy;
            return .{ .source = self, .entry = entry };
        }
        return null;
    }

    fn transactionRecoveryStatus(err: anyerror) abi.Status {
        return kernel_error_identity.statusFromError(err);
    }

    const CandidateConsumerBridge = struct {
        ctx: ?*anyopaque,
        consume: abi.ResolutionCandidateConsumeFn,

        fn consumeCandidate(
            ptr: *anyopaque,
            entity_key: []const u8,
            value: []const u8,
        ) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try kernel_error_identity.statusToError(self.consume(
                self.ctx,
                .fromSlice(entity_key),
                .fromSlice(value),
            ));
        }
    };

    fn resolutionCandidateGet(
        ptr: ?*anyopaque,
        table: abi.BorrowedBytes,
        key: abi.BorrowedBytes,
        consume_ctx: ?*anyopaque,
        consume: abi.ResolutionCandidateConsumeFn,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.resolution_candidate_source orelse return .invalid_argument;
        const value = source.get(self.alloc, table.slice(), key.slice()) catch |err|
            return kernel_error_identity.statusFromError(err);
        const bytes = value orelse return .not_found;
        defer self.alloc.free(bytes);
        return consume(consume_ctx, key, .fromSlice(bytes));
    }

    fn resolutionCandidateScanPrefix(
        ptr: ?*anyopaque,
        table: abi.BorrowedBytes,
        prefix: abi.BorrowedBytes,
        limit: u64,
        consume_ctx: ?*anyopaque,
        consume: abi.ResolutionCandidateConsumeFn,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.resolution_candidate_source orelse return .invalid_argument;
        var bridge = CandidateConsumerBridge{ .ctx = consume_ctx, .consume = consume };
        source.scanPrefix(
            self.alloc,
            table.slice(),
            prefix.slice(),
            .{ .limit = @intCast(@min(limit, std.math.maxInt(usize))) },
            &bridge,
            CandidateConsumerBridge.consumeCandidate,
        ) catch |err| return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn resolutionCandidateNearest(
        ptr: ?*anyopaque,
        table: abi.BorrowedBytes,
        index_name: abi.BorrowedBytes,
        embedding_ptr: ?[*]const f32,
        embedding_len: u64,
        k: u64,
        consume_ctx: ?*anyopaque,
        consume: abi.ResolutionCandidateConsumeFn,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.resolution_candidate_source orelse return .invalid_argument;
        if (embedding_len > 0 and embedding_ptr == null) return .invalid_argument;
        const embedding = if (embedding_len == 0)
            &.{}
        else
            embedding_ptr.?[0..@intCast(embedding_len)];
        var bridge = CandidateConsumerBridge{ .ctx = consume_ctx, .consume = consume };
        source.nearest(
            self.alloc,
            table.slice(),
            .{
                .index_name = index_name.slice(),
                .embedding = embedding,
                .k = @intCast(@min(k, std.math.maxInt(usize))),
            },
            &bridge,
            CandidateConsumerBridge.consumeCandidate,
        ) catch |err| return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn entitySupportsCuration(ptr: ?*anyopaque, supported: *u8) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const sink = self.entity_sink orelse return .invalid_argument;
        supported.* = @intFromBool(sink.supportsCuration() catch |err| return kernel_error_identity.statusFromError(err));
        return .ok;
    }

    fn entityUpsert(
        ptr: ?*anyopaque,
        table: abi.BorrowedBytes,
        key: abi.BorrowedBytes,
        doc_json: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const sink = self.entity_sink orelse return .invalid_argument;
        sink.upsert(self.alloc, table.slice(), key.slice(), doc_json.slice()) catch |err|
            return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn entityUpsertBatch(
        ptr: ?*anyopaque,
        entries_ptr: ?[*]const abi.EntityUpsert,
        entry_count: u64,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const sink = self.entity_sink orelse return .invalid_argument;
        if (entry_count > 0 and entries_ptr == null) return .invalid_argument;
        const encoded = if (entry_count == 0) &.{} else entries_ptr.?[0..@intCast(entry_count)];
        const entries = self.alloc.alloc(runtime_callbacks.EntityUpsert, encoded.len) catch return .out_of_memory;
        defer self.alloc.free(entries);
        for (encoded, entries) |source, *destination| destination.* = .{
            .table = source.table.slice(),
            .storage_table = if (source.storage_table.slice().len == 0) null else source.storage_table.slice(),
            .key = source.key.slice(),
            .doc_json = source.doc_json.slice(),
            .delete = source.delete != 0,
            .skip_if_missing = source.skip_if_missing != 0,
        };
        sink.upsertBatch(self.alloc, entries) catch |err|
            return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn promotionOwner(
        ptr: ?*anyopaque,
        group_id: u64,
    ) callconv(.c) u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return 0));
        const source = self.promotion_leadership_source orelse return 1;
        return @intFromBool(source.isLocalLeader(group_id));
    }

    pub fn withNativeMigrationPolicy(self: *ProvisionedKernelOwnerSource, policy: runtime_callbacks.DenseNativeMigrationPolicySource) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.native_migration_policy = policy;
        return self;
    }

    fn nativeAuthorityPermitted(ptr: ?*const anyopaque) callconv(.c) u8 {
        const self: *const ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr.?));
        return @intFromBool(self.native_migration_policy.?.authorityPermitted());
    }

    fn runtimeHooksConfig(self: *ProvisionedKernelOwnerSource) abi.RuntimeHooksConfig {
        return .{
            .artifact_publication_ctx = if (self.artifact_publications != null) self else null,
            .artifact_publication_enqueue_fn = if (self.artifact_publications != null) enqueueArtifactPublication else null,
            .coordinated_ttl_ctx = if (self.coordinated_ttl != null) self else null,
            .coordinated_ttl_enqueue_fn = if (self.coordinated_ttl != null) enqueueCoordinatedTtl else null,
            .native_authority_ctx = if (self.native_migration_policy != null) self else null,
            .native_authority_fn = if (self.native_migration_policy != null) nativeAuthorityPermitted else null,
            .resolution_candidates = if (self.resolution_candidate_source) |*source| .{
                .native_view = .fromSlice(std.mem.asBytes(source)),
                .native_view_contract = .fromSlice(runtime_callbacks.CandidateSource.nativeContractBytes()),
                .callback_ctx = self,
                .get_fn = resolutionCandidateGet,
                .scan_prefix_fn = resolutionCandidateScanPrefix,
                .nearest_fn = resolutionCandidateNearest,
            } else .{},
            .entity_sink = if (self.entity_sink != null) .{
                .callback_ctx = self,
                .upsert_fn = entityUpsert,
                .upsert_batch_fn = entityUpsertBatch,
                .supports_curation_fn = entitySupportsCuration,
            } else .{},
            .promotion_owner_ctx = if (self.promotion_leadership_source != null) self else null,
            .promotion_owner_fn = if (self.promotion_leadership_source != null) promotionOwner else null,
        };
    }

    pub fn withCoordinatedTtl(self: *ProvisionedKernelOwnerSource, port: server_coordinated_ttl.Port) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.coordinated_ttl = port;
        return self;
    }

    pub fn withArtifactPublications(self: *ProvisionedKernelOwnerSource, port: @import("../storage/artifact_publication_dispatch.zig").Port) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.entries.items.len == 0);
        self.artifact_publications = port;
        return self;
    }

    fn enqueueArtifactPublication(ptr: ?*anyopaque, group_id: u64, namespace: *const [24]u8, command: abi.BorrowedBytes) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const port = self.artifact_publications orelse return .invalid_argument;
        if (command.len == 0 or command.ptr == null) return .invalid_argument;
        port.enqueue(group_id, namespace.*, command.slice()) catch |err| return kernel_error_identity.statusFromError(err);
        return .ok;
    }

    fn enqueueCoordinatedTtl(ptr: ?*anyopaque, request: *const abi.CoordinatedTtlRequest) callconv(.c) u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return 1));
        const port = self.coordinated_ttl orelse return 1;
        if (request.candidate_count > abi.coordinated_ttl_page_capacity or
            (request.candidate_count != 0 and request.candidates == null)) return 1;
        var candidates: [abi.coordinated_ttl_page_capacity]server_coordinated_ttl.Candidate = undefined;
        for (candidates[0..request.candidate_count], 0..) |*dest, index| {
            const source = request.candidates.?[index];
            if (source.key.len != 0 and source.key.ptr == null) return 1;
            dest.* = .{ .key = source.key.slice(), .row_version = source.row_version, .ttl_timestamp_ns = source.ttl_timestamp_ns, .expected_content_digest = source.expected_content_digest };
        }
        if (request.ttl_field.len != 0 and request.ttl_field.ptr == null) return 1;
        _ = port.expire(.{ .table_id = request.table_id, .group_id = request.group_id, .schema_version = request.schema_version, .ttl_duration_ns = request.ttl_duration_ns, .ttl_field = request.ttl_field.slice(), .observed_at_unix_ns = request.observed_at_unix_ns, .grace_period_ns = request.grace_period_ns, .candidates = candidates[0..request.candidate_count] }) catch return 1;
        return 0;
    }

    fn transactionRecoveryResolve(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        participant: abi.BorrowedBytes,
        status: abi.TxnStatus,
        commit_version: u64,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.transaction_recovery_source orelse return .invalid_argument;
        source.resolve(
            txn_id.bytes,
            participant.slice(),
            switch (status) {
                .pending => return .invalid_argument,
                .committed => .committed,
                .aborted => .aborted,
            },
            commit_version,
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryResolveExternal(ptr: ?*anyopaque, txn_id: *const abi.TxnId, participant: abi.BorrowedBytes, status: abi.TxnStatus, version: u64, owner_bytes: abi.BorrowedBytes) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.transaction_recovery_source orelse return .invalid_argument;
        const Owner = @import("antfly_local_sources").storage_transactions.ExternalDecisionOwner;
        if (owner_bytes.len != Owner.encoded_len or owner_bytes.ptr == null) return .invalid_external_decision_owner;
        if (participant.len != 0 and participant.ptr == null) return .invalid_argument;
        const owner = Owner.decode(owner_bytes.slice()) catch |err| return transactionRecoveryStatus(err);
        source.resolveExternal(txn_id.bytes, participant.slice(), switch (status) {
            .pending => return .invalid_argument,
            .committed => .committed,
            .aborted => .aborted,
        }, version, owner) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryOwns(
        ptr: ?*anyopaque,
        owner_participant: abi.BorrowedBytes,
    ) callconv(.c) u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return 0));
        const source = self.transaction_recovery_source orelse return 0;
        return @intFromBool(source.owns(owner_participant.slice()));
    }

    fn transactionRecoveryAcknowledge(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        owner_participant: abi.BorrowedBytes,
        participant: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.transaction_recovery_source orelse return .invalid_argument;
        source.acknowledge(
            txn_id.bytes,
            owner_participant.slice(),
            participant.slice(),
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryAcknowledgeMany(ptr: ?*anyopaque, txn_id: *const abi.TxnId, owner_participant: abi.BorrowedBytes, participants_ptr: ?[*]const abi.BorrowedBytes, participants_len: usize) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.transaction_recovery_source orelse return .invalid_argument;
        if (participants_len == 0 or participants_len > 64) return .invalid_argument;
        const bytes = (participants_ptr orelse return .invalid_argument)[0..participants_len];
        var members: [64][]const u8 = undefined;
        for (bytes, members[0..participants_len]) |item, *member| member.* = item.slice();
        source.acknowledgeMany(txn_id.bytes, owner_participant.slice(), members[0..participants_len]) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryCleanup(
        ptr: ?*anyopaque,
        txn_id: *const abi.TxnId,
        owner_participant: abi.BorrowedBytes,
        cutoff_timestamp: u64,
        retained_cutoff_timestamp: u64,
    ) callconv(.c) abi.Status {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        const source = self.transaction_recovery_source orelse return .invalid_argument;
        source.cleanup(
            txn_id.bytes,
            owner_participant.slice(),
            cutoff_timestamp,
            retained_cutoff_timestamp,
        ) catch |err| return transactionRecoveryStatus(err);
        return .ok;
    }

    fn transactionRecoveryConfig(self: *ProvisionedKernelOwnerSource) abi.TransactionRecoveryConfig {
        const source = self.transaction_recovery_source orelse return .{};
        const options = source.options();
        if (!options.enabled) return .{};
        return .{
            .enabled = 1,
            .lease_owned = @intFromBool(options.lease_owned),
            .replicated_metadata = @intFromBool(options.replicated_metadata),
            .interval_ms = options.interval_ms,
            .cutoff_ns = options.cutoff_ns,
            .callback_ctx = self,
            .owner_id = .fromSlice(options.owner_id),
            .resolve_participant_fn = transactionRecoveryResolve,
            .resolve_external_participant_fn = transactionRecoveryResolveExternal,
            .owns_recovery_fn = if (options.replicated_metadata) transactionRecoveryOwns else null,
            .acknowledge_participant_fn = if (options.replicated_metadata) transactionRecoveryAcknowledge else null,
            .acknowledge_participants_fn = if (options.replicated_metadata) transactionRecoveryAcknowledgeMany else null,
            .cleanup_transaction_fn = if (options.replicated_metadata) transactionRecoveryCleanup else null,
        };
    }

    fn acquireDescriptor(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
    ) !Lease {
        return try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor, false, .resident, .{});
    }

    pub const RestoreOwnerOptions = struct {
        secret_store: ?*anyopaque = null,
        node_config: ?*const anyopaque = null,
        source_byte_budget: u64 = 1024 * 1024,
    };

    pub const OwnedRestoreDescriptor = struct {
        descriptor: descriptor_contract.Descriptor,

        pub fn view(self: *const OwnedRestoreDescriptor) descriptor_contract.Descriptor {
            return self.descriptor;
        }

        pub fn deinit(self: *OwnedRestoreDescriptor, alloc: std.mem.Allocator) void {
            alloc.free(self.descriptor.schema_json);
            alloc.free(self.descriptor.indexes_json);
            alloc.free(self.descriptor.restore_bootstrap_json);
            descriptor_contract.freeInitialRange(alloc, self.descriptor.initial_range);
            self.* = undefined;
        }
    };

    pub const RestoreDescriptorUse = enum { read, mutate, resolve };
    pub const RestoreDescriptorRecovery = struct {
        const VTable = struct { recover: @FieldType(RestoreDescriptorRecovery, "recover_fn") };
        const Boundary = @import("antfly_local_sources").runtime_callback_abi.Boundary(VTable);
        ptr: *anyopaque,
        recover_fn: *const fn (*anyopaque, std.mem.Allocator, u64, []const u8, [32]u8, [16]u8, RestoreDescriptorUse, @import("antfly_local_sources").api_operation.RequestContext) anyerror!OwnedRestoreDescriptor,
        boundary_dispatch: Boundary.Dispatch = Boundary.local_dispatch,

        fn recover(self: @This(), alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: [32]u8, plan_id: [16]u8, use: RestoreDescriptorUse, context: @import("antfly_local_sources").api_operation.RequestContext) !OwnedRestoreDescriptor {
            return Boundary.call("recover", self.boundary_dispatch, self.recover_fn, .{ self.ptr, alloc, group_id, table_name, scope, plan_id, use, context });
        }
    };

    pub fn withRestoreDescriptorRecovery(self: *ProvisionedKernelOwnerSource, recovery: RestoreDescriptorRecovery) *ProvisionedKernelOwnerSource {
        self.restore_descriptor_recovery = recovery;
        return self;
    }

    /// Cold hidden-owner recovery is bounded by its explicit plan identity.
    /// Only the host's authoritative restore-plan callback can supply a missing
    /// descriptor; ordinary named-table discovery is never an alternative.
    pub fn resolveRestoreDescriptor(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: [32]u8, plan_id: ?[16]u8, use: RestoreDescriptorUse, context: @import("antfly_local_sources").api_operation.RequestContext) !OwnedRestoreDescriptor {
        try context.ensureActive();
        var descriptor = if (try self.cachedRestoreDescriptor(alloc, group_id, table_name, scope)) |cached| cached else blk: {
            const plan = plan_id orelse return error.RestoreStagingScopeChanged;
            if (std.mem.allEqual(u8, &plan, 0)) return error.RestoreStagingScopeChanged;
            const recovery = self.restore_descriptor_recovery orelse return error.RestoreStagingScopeChanged;
            break :blk try recovery.recover(alloc, group_id, table_name, scope, plan, use, context);
        };
        errdefer descriptor.deinit(alloc);
        var parsed = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_restore_staging_contract.OwnerBootstrap, alloc, descriptor.descriptor.restore_bootstrap_json, .{});
        defer parsed.deinit();
        try parsed.value.validate();
        const namespace = parsed.value.scope.target_namespace;
        if (!std.mem.eql(u8, &scope, &parsed.value.scope.digest()) or
            !std.mem.eql(u8, table_name, parsed.value.table_name) or namespace.shard_id != group_id or
            namespace.table_id != descriptor.descriptor.identity.table_id or namespace.range_id != descriptor.descriptor.identity.range_id or
            descriptor.descriptor.identity.shard_id != group_id) return error.RestoreStagingScopeChanged;
        // The root can advance after the immutable plan descriptor is read.
        // Re-resolve it on retry; this is not a changed restore scope.
        if (descriptor.descriptor.lsm_root_generation != self.visibleRootGeneration(group_id)) return error.StorageKernelOwnerTransitionRequired;
        if (plan_id) |plan| if (!std.mem.eql(u8, &plan, &parsed.value.scope.plan_id)) return error.RestoreStagingScopeChanged;
        try context.ensureActive();
        return descriptor;
    }

    pub fn cachedRestoreDescriptor(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, scope: [32]u8) !?OwnedRestoreDescriptor {
        const generation = self.visibleRootGeneration(group_id);
        var pinned: Lease = blk: {
            lock(&self.mutex);
            defer self.mutex.unlock();
            for (self.entries.items) |entry| {
                if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name) or entry.retired or entry.closing or entry.generation != generation or entry.restore_bootstrap_json.len == 0) continue;
                if (!tryReserveEntryLeaseLocked(entry, .shared)) return error.StorageReadTemporarilyUnavailable;
                break :blk .{ .source = self, .entry = entry };
            }
            return null;
        };
        defer pinned.deinit();
        const entry = pinned.entry;
        {
            var bootstrap = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_restore_staging_contract.OwnerBootstrap, alloc, entry.restore_bootstrap_json, .{});
            defer bootstrap.deinit();
            if (!std.mem.eql(u8, &scope, &bootstrap.value.scope.digest())) return error.RestoreStagingScopeChanged;
            const schema_json = try alloc.dupe(u8, entry.schema_json);
            errdefer alloc.free(schema_json);
            const indexes_json = try alloc.dupe(u8, entry.indexes_json);
            errdefer alloc.free(indexes_json);
            const restore_bootstrap_json = try alloc.dupe(u8, entry.restore_bootstrap_json);
            errdefer alloc.free(restore_bootstrap_json);
            const initial_range = try descriptor_contract.cloneInitialRange(alloc, entry.initial_range);
            return .{ .descriptor = .{
                .lsm_root_generation = entry.generation,
                .identity = entry.identity,
                .schema_json = schema_json,
                .indexes_json = indexes_json,
                .restore_bootstrap_json = restore_bootstrap_json,
                .restore_cancel_recovery = entry.restore_cancel_recovery,
                .restore_ha_replay = entry.restore_ha_replay,
                .table_storage = entry.table_storage,
                .sql_persistence = entry.sql_persistence,
                .initial_range = initial_range,
            } };
        }
    }

    /// The caller supplies an authoritative private descriptor, never a public
    /// name lookup. Opening and all physical work remain in the compiled owner.
    pub fn primeRestoreOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: descriptor_contract.Descriptor) !void {
        if (descriptor.restore_bootstrap_json.len == 0) return error.RestoreStagingScopeChanged;
        if (descriptor.lsm_root_generation != self.visibleRootGeneration(group_id)) return error.StorageKernelOwnerTransitionRequired;
        const path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer self.alloc.free(path);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor);
        defer lease.deinit();
    }

    pub fn primeInitialChildOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: descriptor_contract.Descriptor) !void {
        if (descriptor.initial_child_bootstrap_json.len == 0 or descriptor.restore_bootstrap_json.len != 0 or
            descriptor.lsm_root_generation != self.visibleRootGeneration(group_id)) return error.InvalidInitialChildPublication;
        const path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer self.alloc.free(path);
        var lease = try self.acquireDescriptor(group_id, table_name, path, descriptor);
        defer lease.deinit();
    }

    /// Retire only the exact private owner named by a terminal metadata
    /// cancellation. A public owner (or a different hidden plan) is never
    /// evicted by a delayed supervisor round. Active leases drain normally;
    /// callers retry until this returns true.
    pub fn retireCanceledInitialChildOwner(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        expected: @import("antfly_local_sources").storage_db_relational_initial_child_publication.Bootstrap,
    ) !bool {
        try expected.validate();
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.initial_child_bootstrap_json.len == 0 or
                entry.identity.table_id != expected.namespace.table_id or
                entry.identity.shard_id != expected.namespace.shard_id or
                entry.identity.range_id != expected.namespace.range_id)
                return error.InitialChildPublicationChanged;
            var stored = std.json.parseFromSlice(@TypeOf(expected), self.alloc, entry.initial_child_bootstrap_json, .{ .ignore_unknown_fields = false }) catch return error.InvalidInitialChildPublication;
            defer stored.deinit();
            if (!stored.value.eql(expected)) return error.InitialChildPublicationChanged;
            entry.retired = true;
        }
        self.drainRetiredLocked(group_id, table_name);
        for (self.entries.items) |entry| {
            if (entry.group_id == group_id and std.mem.eql(u8, entry.table_name, table_name)) return false;
        }
        return true;
    }

    /// A not-yet-published child has no catalog route. Only the exact hidden
    /// descriptor admitted by metadata may read its private control record;
    /// ordinary group reads must continue through the public route fence.
    pub fn lookupInitialChildPrivate(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        expected: @import("antfly_local_sources").storage_db_relational_initial_child_publication.Bootstrap,
        opts: db_types.LookupOptions,
    ) !?table_read_source.LookupResponse {
        try expected.validate();
        if (!std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"initial_child_preflight\"}") and
            !std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"initial_child_publication\"}"))
            return error.InvalidInitialChildPublication;
        try self.prepareLookupRead(group_id, "", opts, .stale);
        var lease = try self.acquirePreparedOwner(group_id, table_name);
        defer lease.deinit();
        if (lease.entry.initial_child_bootstrap_json.len == 0 or
            lease.entry.identity.table_id != expected.namespace.table_id or
            lease.entry.identity.shard_id != expected.namespace.shard_id or
            lease.entry.identity.range_id != expected.namespace.range_id)
            return error.InitialChildPublicationChanged;
        var stored = std.json.parseFromSlice(@TypeOf(expected), alloc, lease.entry.initial_child_bootstrap_json, .{ .ignore_unknown_fields = false }) catch return error.InvalidInitialChildPublication;
        defer stored.deinit();
        if (!stored.value.eql(expected)) return error.InitialChildPublicationChanged;
        const request_json = try table_reads.encodeStorageKernelLookupRequest(alloc, "", opts);
        defer alloc.free(request_json);
        var response = lease.owner().lookupJson(table_name, request_json) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()), .version = response.version(), .expected_content_digest = response.expectedContentDigest() };
    }

    /// Pin the exact physical owner for the whole private operation. This is
    /// a storage adapter, not metadata authorization: callers must independently
    /// obtain publication permission before dispatching a mutation here.
    fn acquireIndexOwner(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        identity: raft_index_control.Identity,
        context: request_operation.RequestContext,
    ) !Lease {
        try context.ensureActive();
        try identity.validate();
        if (identity.owner_group_id != group_id) return error.PrivateIndexOwnerJobChanged;
        return self.acquireBoundOwner(group_id, table_name, identity.namespace, context);
    }

    /// Physical scope is shared across private control families; a schema job
    /// must not impersonate an index identity just to pin its real owner.
    fn acquireBoundOwner(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        namespace: @import("antfly_local_sources").storage_db_doc_identity.Namespace,
        context: request_operation.RequestContext,
    ) !Lease {
        try context.ensureActive();
        const normalized = try platformDeadlineContext(context);
        var lease = try self.acquireControlWithControls(group_id, table_name, .{
            .allow_deferred_catalog = true,
            .execution_deadline_ns = normalized.deadline_ns,
            .cancellation = normalized.cancellation,
        });
        errdefer lease.deinit();
        if (lease.entry.identity.table_id != namespace.table_id or
            lease.entry.identity.shard_id != namespace.shard_id or
            lease.entry.identity.range_id != namespace.range_id)
            return error.IdentityNamespaceMismatch;
        try context.ensureActive();
        return lease;
    }

    /// Observe an existing schema owner without reconciling metadata over its
    /// live validation fence. Ordinary table/index provisioning keeps its
    /// explicit descriptor; these private schema operations require a real root.
    fn acquireSchemaBoundOwner(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        namespace: @import("antfly_local_sources").storage_db_doc_identity.Namespace,
        context: request_operation.RequestContext,
    ) !Lease {
        return self.acquireSchemaBoundOwnerMode(group_id, table_name, namespace, false, context);
    }

    fn acquireSchemaBoundOwnerMode(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        namespace: @import("antfly_local_sources").storage_db_doc_identity.Namespace,
        require_historical_owner: bool,
        context: request_operation.RequestContext,
    ) !Lease {
        if (self.online_source_authority != .raft) return self.acquireBoundOwner(group_id, table_name, namespace, context);
        try context.ensureActive();
        const normalized = try platformDeadlineContext(context);
        var lease = self.acquireControlWithControls(group_id, table_name, .{
            .allow_deferred_catalog = true,
            .historical_raft_apply = true,
            .durable_catalog = true,
            .require_historical_owner = require_historical_owner,
            .execution_deadline_ns = normalized.deadline_ns,
            .cancellation = normalized.cancellation,
        }) catch |err| switch (err) {
            // A present physical directory without its committed table
            // catalog is no more authoritative than a missing directory.
            error.FileNotFound, error.NotFound => return error.SchemaValidationRuntimeRecoveryRequired,
            else => return err,
        };
        errdefer lease.deinit();
        if (lease.entry.identity.table_id != namespace.table_id or
            lease.entry.identity.shard_id != namespace.shard_id or
            lease.entry.identity.range_id != namespace.range_id) return error.IdentityNamespaceMismatch;
        try context.ensureActive();
        return lease;
    }

    const SchemaAttestation = @import("../metadata/schema_validation_attestation.zig");
    pub const RootedNativeSchemaOwnerObservation = SchemaAttestation.RootedObservation;

    pub const SchemaPreparationSigner = struct {
        identity: @import("../metadata/store_root_enrollment.zig").Identity,
        reporter_incarnation: u64,
        authority: @import("../metadata/store_root_attestation.zig").Authority,
        native_pin: ?@import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin = null,
    };

    /// Candidate compilation and the actual physical cut share one applying
    /// lease. Quorum certification waits after release, then rechecks the root
    /// before signing. The receiver obtains signer enrollment/authority
    /// independently from metadata; this method grants neither admission nor
    /// replacement-root adoption. Metadata repeats that check on durable begin.
    pub fn attestSchemaPreparationAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.PreparationDescriptor, inputs: []const u8, signer: SchemaPreparationSigner, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !SchemaAttestation.SignedPreparation {
        try context.ensureActive();
        try descriptor.validate();
        try signer.identity.validate();
        if (descriptor.fence.owner_group_id != group_id or (descriptor.authority == .native) != (signer.authority == .native) or signer.reporter_incarnation == 0)
            return error.SchemaValidationJobChanged;
        if (signer.authority == .native) {
            try (@import("../metadata/store_root_attestation.zig").NativeView{ .identity = signer.identity, .reporter_incarnation = signer.reporter_incarnation, .binding_digest = signer.authority.native, .native_pin = signer.native_pin }).validate(signer.identity.metadata_incarnation);
        }
        const cut = capture: {
            var lease = try self.acquireSchemaBoundOwner(group_id, table_name, descriptor.fence.namespace, context);
            defer lease.deinit();
            break :capture try self.captureSchemaPreparationAtLease(&lease, table_name, descriptor, inputs, context);
        };
        return self.certifySchemaPreparation(cut, table_name, signer, root, context);
    }

    const SchemaPreparationCut = struct {
        value: @FieldType(SchemaAttestation.SignedPreparation, "value"),
        generation: u64,
    };

    fn captureSchemaPreparationAtLease(self: *ProvisionedKernelOwnerSource, lease: *Lease, table_name: []const u8, descriptor: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.PreparationDescriptor, inputs: []const u8, context: request_operation.RequestContext) !SchemaPreparationCut {
        const normalized = try platformDeadlineContext(context);
        var cancellation = normalized.cancellation;
        const observed = try lease.owner().schemaOwnerPreparation(table_name, descriptor, inputs, .{
            .execution_deadline_ns = normalized.deadline_ns,
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        });
        const generation = @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(lease.entry.generation) catch return error.SchemaValidationRuntimeRecoveryRequired;
        try context.ensureActive();
        lock(&self.mutex);
        const retired = lease.entry.retired or lease.entry.closing;
        self.mutex.unlock();
        const visible = try @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(self.visibleRootGeneration(descriptor.fence.owner_group_id));
        if (retired or visible != generation) return error.SchemaValidationRuntimeRecoveryRequired;
        return .{ .value = observed, .generation = generation };
    }

    fn certifySchemaPreparation(self: *ProvisionedKernelOwnerSource, cut: SchemaPreparationCut, table_name: []const u8, signer: SchemaPreparationSigner, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !SchemaAttestation.SignedPreparation {
        const normalized = try platformDeadlineContext(context);
        const group = cut.value.binding.fence.owner_group_id;
        // No owner lease survives into this wait: completing the quorum prefix
        // may require an apply writer to acquire that same physical owner.
        // The immutable captured cut remains valid while concurrent rows advance.
        const completed: ?@import("antfly_local_sources").storage_read_consistency.ReadSafetyBarrier.FrozenProof = if (cut.value.binding.authority == .raft) prefix: {
            const capture = self.read_safety_barrier.vtable.capture_frozen orelse return error.ReadSafetyBarrierUnavailable;
            const certify = self.read_safety_barrier.vtable.certify_completed_prefix orelse return error.ReadSafetyBarrierUnavailable;
            const captured = try capture(self.read_safety_barrier.ptr, group);
            const actual_index = if (cut.value.position) |position| actual: {
                if (position != .raft) return error.InvalidControlReceiptPosition;
                break :actual position.raft.index;
            } else 0;
            break :prefix try certify(self.read_safety_barrier.ptr, group, captured.term, @max(1, @max(actual_index, captured.applied_index)), normalized.deadline_ns, normalized.cancellation);
        } else null;
        // A replacement during the wait invalidates this capture; ordinary row
        // churn does not. Reacquire only for this short identity check.
        const current = try self.schemaOwnerStateAtRoot(group, table_name, context);
        if (current.root_generation != cut.generation or current.state.physical_root_incarnation != cut.value.physical_root_incarnation or
            !current.state.namespace.eql(cut.value.binding.fence.namespace)) return error.SchemaValidationRuntimeRecoveryRequired;
        if (signer.native_pin) |pin| {
            if (pin.physical_root_incarnation != cut.value.physical_root_incarnation or
                pin.table_id != current.state.namespace.table_id or pin.shard_id != current.state.namespace.shard_id or pin.range_id != current.state.namespace.range_id) return error.SchemaValidationRuntimeRecoveryRequired;
        }
        const signed = try SchemaAttestation.SignedPreparation.signPinned(cut.value, signer.identity, signer.reporter_incarnation, signer.authority, cut.generation, completed, signer.native_pin, root);
        try context.ensureActive();
        return signed;
    }

    pub const SchemaPreparationIntent = struct {
        id: [16]u8,
        provisional_plan: [32]u8,
        transition_id: u64,
        namespace: @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace,
    };

    pub const CapturedSchemaPreparation = struct {
        alloc: std.mem.Allocator,
        inputs: []u8,
        signed: SchemaAttestation.SignedPreparation,

        pub fn deinit(self: *@This()) void {
            self.alloc.free(self.inputs);
            self.* = undefined;
        }
    };

    /// Read-only pre-admission capture. The physical catalog and topology come
    /// from this exact owner lease, never the coordinator's compiled guesses.
    /// Native preparation independently rechecks the resulting ASVI against a
    /// retained cut before signing. A concurrent catalog change rejects before
    /// admission; it cannot silently produce a different candidate or fence.
    pub fn captureSchemaPreparationAtRoot(self: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, group_id: u64, table_name: []const u8, intent: SchemaPreparationIntent, before_json: []const u8, after_json: []const u8, selection: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.InputSelection, signer: SchemaPreparationSigner, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !CapturedSchemaPreparation {
        const local = @import("antfly_local_sources");
        const contract = local.storage_db_relational_schema_owner_contract;
        try context.ensureActive();
        try signer.identity.validate();
        if (intent.transition_id == 0 or std.mem.allEqual(u8, &intent.id, 0) or std.mem.allEqual(u8, &intent.provisional_plan, 0) or
            before_json.len == 0 or before_json.len > contract.max_schema or after_json.len == 0 or after_json.len > contract.max_schema or signer.reporter_incarnation == 0) return error.InvalidSchemaValidationJob;
        if (signer.authority == .native) {
            if (signer.native_pin) |pin| {
                if (!std.mem.eql(u8, &try @import("../metadata/sql_relation_attestation.zig").nativeAuthorityDigest(signer.identity, pin), &signer.authority.native)) return error.SchemaValidationJobChanged;
            } else {
                const binding: @import("../metadata/standalone_native_owner.zig").Binding = .{ .metadata_incarnation = signer.identity.metadata_incarnation, .node_id = signer.identity.node_id, .store_id = signer.identity.store_id, .root_incarnation = signer.identity.root_incarnation };
                if (!std.mem.eql(u8, &try binding.digest(), &signer.authority.native)) return error.SchemaValidationJobChanged;
            }
        }
        try selection.validate();
        if (selection == .foreign_keys and !std.mem.eql(u8, selection.foreign_keys.table_name, table_name)) return error.SchemaValidationJobChanged;
        const column_bytes = try selection.encodePayload(a);
        defer a.free(column_bytes);
        const captured = capture: {
            var lease = try self.acquireSchemaBoundOwner(group_id, table_name, intent.namespace, context);
            defer lease.deinit();
            if (signer.native_pin) |pin| {
                const descriptor = lease.entry.sql_persistence orelse return error.SchemaValidationJobChanged;
                if (pin.table_id != intent.namespace.table_id or pin.shard_id != intent.namespace.shard_id or pin.range_id != intent.namespace.range_id or
                    pin.data_generation != descriptor.data_generation) return error.SchemaValidationJobChanged;
            }
            const normalized = try platformDeadlineContext(context);
            const identity_request = try table_reads.encodeStorageKernelLookupRequest(a, "", .{ .relational_topology_json = "{\"mode\":\"identity\"}", .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
            defer a.free(identity_request);
            var identity_bytes = try lease.owner().lookupJson(table_name, identity_request);
            defer identity_bytes.deinit();
            if (identity_bytes.bytes().len == 0 or identity_bytes.bytes().len > 8192) return error.InvalidBoundaryFailureIdentity;
            var identity = try std.json.parseFromSlice(local.storage_db_relational_integrity_topology_contract.Identity, a, identity_bytes.bytes(), .{ .ignore_unknown_fields = false });
            defer identity.deinit();
            if (!identity.value.namespace.eql(intent.namespace) or identity.value.next_epoch == 0) return error.IdentityNamespaceMismatch;
            const catalog_request = try table_reads.encodeStorageKernelLookupRequest(a, "", .{ .relational_integrity_catalog = true, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
            defer a.free(catalog_request);
            var catalog_bytes = try lease.owner().lookupJson(table_name, catalog_request);
            defer catalog_bytes.deinit();
            const max_catalog = local.storage_db_relational_integrity_catalog.max_catalog_bytes;
            if (catalog_bytes.bytes().len == 0 or catalog_bytes.bytes().len > std.base64.standard.Encoder.calcSize(max_catalog) + 1024) return error.InvalidBoundaryFailureIdentity;
            // Reuse the existing owner read contract locally. Only raw ASVI leaves
            // this adapter; the existing catalog JSON/base64 wrapper does not cross
            // the preparation transport or enter the durable metadata command.
            const Catalog = struct { catalog: []const u8, schema_version: u32, table_id: []const u8 };
            var catalog = try std.json.parseFromSlice(Catalog, a, catalog_bytes.bytes(), .{ .ignore_unknown_fields = false });
            defer catalog.deinit();
            var table_id: [20]u8 = undefined;
            if (!std.mem.eql(u8, catalog.value.table_id, try std.fmt.bufPrint(&table_id, "{d}", .{intent.namespace.table_id}))) return error.IdentityNamespaceMismatch;
            const length = std.base64.standard.Decoder.calcSizeForSlice(catalog.value.catalog) catch return error.InvalidBoundaryFailureIdentity;
            if (length == 0 or length > max_catalog) return error.InvalidBoundaryFailureIdentity;
            const physical = try a.alloc(u8, length);
            defer a.free(physical);
            std.base64.standard.Decoder.decode(physical, catalog.value.catalog) catch return error.InvalidBoundaryFailureIdentity;
            if (!std.mem.eql(u8, &contract.hash(physical), &identity.value.catalog_digest)) return error.IntegrityCatalogChanged;
            const inputs = try contract.encodeInputs(a, .{ before_json, after_json, physical, column_bytes });
            errdefer a.free(inputs);
            const descriptor: contract.PreparationDescriptor = .{ .purpose = selection.purpose(), .id = intent.id, .provisional_plan = intent.provisional_plan, .authority = if (signer.authority == .native) .native else .raft, .fence = .{ .role = .schema_validation, .transition_id = intent.transition_id, .attempt = 1, .admission_epoch = identity.value.next_epoch, .owner_group_id = group_id, .peer_group_id = group_id, .namespace = intent.namespace, .catalog_digest = identity.value.catalog_digest } };
            try descriptor.validate();
            const cut = try self.captureSchemaPreparationAtLease(&lease, table_name, descriptor, inputs, context);
            break :capture .{ .inputs = inputs, .cut = cut };
        };
        errdefer a.free(captured.inputs);
        const signed = try self.certifySchemaPreparation(captured.cut, table_name, signer, root, context);
        return .{ .alloc = a, .inputs = captured.inputs, .signed = signed };
    }

    /// Zero-progress root recovery uses real native candidate preparation plus
    /// an independently completed shared Raft prefix. No job header, data-entry
    /// index or row-audit evidence is invented for a pre-begin replica.
    pub fn attestSchemaBootstrapAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, descriptor: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.PreparationDescriptor, inputs: []const u8, expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !@import("../metadata/schema_validation_bootstrap.zig").CertifiedPreparation {
        try context.ensureActive();
        try expected.validate();
        if (descriptor.authority != .raft or expected.group_id != group_id) return error.SchemaValidationJobChanged;
        const prepared = try self.attestSchemaPreparationAtRoot(group_id, table_name, descriptor, inputs, .{
            .identity = expected.identity,
            .reporter_incarnation = expected.reporter_incarnation,
            .authority = .{ .raft = expected.replica_id },
        }, root, context);
        try expected.requireBinding(prepared.value.binding);
        if (prepared.value.physical_root_incarnation != expected.physical_root_incarnation or
            prepared.proof.root_generation != expected.root_generation) return error.SchemaValidationRuntimeRecoveryRequired;
        // Initial admission and root recovery share the same certified
        // physical preparation. Do not issue a second quorum read here.
        const completed = prepared.completed orelse return error.ReadSafetyBarrierUnavailable;
        // A new leader term or a report ahead of this captured prefix needs a
        // fresh scope, not a permanently invalid recovery receipt.
        if (completed.term > expected.reported_term or completed.applied_index < expected.reported_applied_index)
            return error.IndexOwnerReceiptChanged;
        const visible = try @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(self.visibleRootGeneration(group_id));
        if (visible != expected.root_generation) return error.SchemaValidationRuntimeRecoveryRequired;
        const signed = try @import("../metadata/schema_validation_bootstrap.zig").CertifiedPreparation.sign(prepared, completed, expected, root);
        try context.ensureActive();
        return signed;
    }

    /// Facts and generation come from the same applying lease. The receiver
    /// still independently obtains metadata permission, enrollment and current
    /// native ownership; this observation is not a signed publication receipt.
    pub fn nativeSchemaOwnerObservationAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding, context: request_operation.RequestContext) !RootedNativeSchemaOwnerObservation {
        try context.ensureActive();
        try binding.validate();
        if (binding.authority != .native or binding.fence.owner_group_id != group_id) return error.SchemaValidationJobChanged;
        var lease = try self.acquireSchemaBoundOwner(group_id, table_name, binding.fence.namespace, context);
        defer lease.deinit();
        return observeSchemaAtLease(&lease, table_name, binding, context);
    }

    /// The receiver supplies a scope fetched independently from metadata and
    /// its already-loaded private root key, never from an ordinary request.
    /// Possession/signature is not enrollment or permission to publish. The
    /// metadata ACK transaction must independently revalidate both authorities.
    pub fn attestNativeSchemaOwnerAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding, expected: SchemaAttestation.NativeScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !SchemaAttestation.Result {
        try context.ensureActive();
        try expected.requireBinding(binding);
        if (binding.fence.owner_group_id != group_id) return error.SchemaValidationJobChanged;
        var lease = try self.acquireSchemaBoundOwner(group_id, table_name, binding.fence.namespace, context);
        defer lease.deinit();
        const observed = try observeSchemaAtLease(&lease, table_name, binding, context);
        try expected.require(observed);
        if (observed.observation.header == null) return error.SchemaValidationJobRequired;
        if (observed.observation.replication_pending) return .{ .pending = observed };
        try context.ensureActive();
        const signed = try SchemaAttestation.SignedNativeObservation.sign(observed, expected, root);
        try context.ensureActive();
        return .{ .signed = signed };
    }

    /// Callers retain this exact lease through signing, so a later route
    /// lookup cannot substitute the provenance of another physical owner.
    fn observeSchemaAtLease(lease: *Lease, table_name: []const u8, binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding, context: request_operation.RequestContext) !RootedNativeSchemaOwnerObservation {
        const normalized = try platformDeadlineContext(context);
        var cancellation = normalized.cancellation;
        const result = try lease.owner().nativeSchemaOwnerObservation(table_name, binding, .{
            .execution_deadline_ns = normalized.deadline_ns,
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        });
        try context.ensureActive();
        return .{ .root_generation = @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(lease.entry.generation) catch return error.SchemaValidationRuntimeRecoveryRequired, .observation = result };
    }

    pub const RootedNativeSchemaOwnerOutcome = struct {
        root_generation: u64,
        outcome: client.Owner.NativeSchemaOwnerOutcome,
    };

    pub const SchemaOwnerRoot = struct { root_generation: u64, physical_root_incarnation: u128 };

    /// Native control worker only. The receiving service independently checks
    /// metadata/root/cohort permission. Keep the same bound physical owner
    /// across off-lock preparation, atomic apply and response construction.
    pub fn applyNativeSchemaOwnerControlAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, command: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Command, records: u32, time_ns: u64, expected: SchemaOwnerRoot, context: request_operation.RequestContext) !RootedNativeSchemaOwnerOutcome {
        try context.ensureActive();
        try command.binding.validate();
        if (command.binding.authority != .native or command.binding.fence.owner_group_id != group_id) return error.SchemaValidationJobChanged;
        var lease = try self.acquireSchemaBoundOwner(group_id, table_name, command.binding.fence.namespace, context);
        defer lease.deinit();
        const generation = @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(lease.entry.generation) catch return error.SchemaValidationRuntimeRecoveryRequired;
        const actual = try self.schemaStateAtLease(&lease, table_name, context);
        if (expected.root_generation == 0 or expected.physical_root_incarnation == 0 or
            generation != expected.root_generation or actual.state.physical_root_incarnation != expected.physical_root_incarnation)
            return error.SchemaValidationRuntimeRecoveryRequired;
        const normalized = try platformDeadlineContext(context);
        var cancellation = normalized.cancellation;
        const result = try lease.owner().nativeSchemaOwnerControl(table_name, command, records, time_ns, .{
            .execution_deadline_ns = normalized.deadline_ns,
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        });
        // No fallible post-commit deadline/cancellation check. A durable
        // effect (including explicit pending mirror debt) remains observable.
        return .{ .root_generation = generation, .outcome = result };
    }

    pub fn raftSchemaOwnerObservationAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding, context: request_operation.RequestContext) !SchemaAttestation.RootedRaftObservation {
        var session = try self.acquireRaftSchemaOwnerSession(group_id, table_name, binding, context);
        defer session.deinit();
        return session.initial;
    }

    /// A preparation/observation lease is not held across ordered apply: apply
    /// may need to retire the resident descriptor. Reacquisition must prove
    /// BOTH the serving generation and the physical root retained at admission.
    /// Neither routing nor an equal schema digest authorizes root adoption.
    pub const RootedSchemaOwnerAudit = struct {
        root_generation: u64,
        result: @import("antfly_local_sources").storage_db_relational_schema_root_contract.AuditResult,
    };

    pub const RootedSchemaOwnerState = struct {
        root_generation: u64,
        state: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.OwnerState,
    };

    /// Private recovery discovery on the actual bound owner, including before
    /// the caller knows a job binding. This is not metadata/execute authority.
    pub fn schemaOwnerStateAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, context: request_operation.RequestContext) !RootedSchemaOwnerState {
        try context.ensureActive();
        const normalized = try platformDeadlineContext(context);
        var lease = try self.acquireControlWithControls(group_id, table_name, .{ .allow_deferred_catalog = true, .historical_raft_apply = self.online_source_authority == .raft, .durable_catalog = self.online_source_authority == .raft, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        defer lease.deinit();
        return self.schemaStateAtLease(&lease, table_name, context);
    }

    fn schemaStateAtLease(self: *ProvisionedKernelOwnerSource, lease: *Lease, table_name: []const u8, context: request_operation.RequestContext) !RootedSchemaOwnerState {
        const normalized = try platformDeadlineContext(context);
        const request = try table_reads.encodeStorageKernelLookupRequest(self.alloc, "", .{ .relational_topology_json = "{\"mode\":\"schema_owner\"}", .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        defer self.alloc.free(request);
        var response = try lease.owner().lookupJson(table_name, request);
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_relational_schema_owner_contract.OwnerState, self.alloc, response.bytes(), .{ .ignore_unknown_fields = false });
        defer parsed.deinit();
        const state = parsed.value;
        if (state.physical_root_incarnation == 0 or state.namespace.table_id != lease.entry.identity.table_id or
            state.namespace.shard_id != lease.entry.identity.shard_id or state.namespace.range_id != lease.entry.identity.range_id) return error.IdentityNamespaceMismatch;
        if (state.header) |header| try header.validate();
        try context.ensureActive();
        return .{ .root_generation = try @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(lease.entry.generation), .state = state };
    }

    /// A single physical lease owns the generation and native bounded audit.
    /// The checkpoint/signing directory comes from this provisioned source,
    /// never from a coordinator request. Enrollment is checked independently.
    pub fn stepSchemaOwnerAuditAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding, inputs: []const u8, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, budget: @import("antfly_local_sources").storage_db_relational_schema_root_contract.AuditBudget, context: request_operation.RequestContext) !RootedSchemaOwnerAudit {
        try context.ensureActive();
        try binding.validate();
        try budget.validate();
        if (binding.fence.owner_group_id != group_id) return error.SchemaValidationJobChanged;
        var lease = try self.acquireSchemaBoundOwner(group_id, table_name, binding.fence.namespace, context);
        defer lease.deinit();
        const normalized = try platformDeadlineContext(context);
        var cancellation = normalized.cancellation;
        const result = try lease.owner().stepSchemaOwnerAudit(table_name, binding, inputs, .{ .dir = self.replica_root_dir, .incarnation = root.root_incarnation, .public_key = root.public_key }, budget, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        const generation = try @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(lease.entry.generation);
        try context.ensureActive();
        return .{ .root_generation = generation, .result = result };
    }

    pub const RaftSchemaOwnerSession = struct {
        lease: Lease,
        table_name: []const u8,
        binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding,
        context: request_operation.RequestContext,
        initial: SchemaAttestation.RootedRaftObservation,

        pub fn deinit(self: *@This()) void {
            self.lease.deinit();
            self.* = undefined;
        }

        pub fn releaseForApply(self: *@This()) void {
            self.lease.deinit();
        }

        pub fn reacquireAfterApply(self: *@This()) !void {
            return self.reacquireOwner(false);
        }

        fn reacquireOwner(self: *@This(), require_historical_owner: bool) !void {
            std.debug.assert(!self.lease.active);
            // Admission/cancellation may still bound the owner open. Once a
            // committed effect is found, recoverCommitted reads it independently
            // of the initiating request's cancellation, on this exact lease.
            const source = self.lease.source;
            try self.context.ensureActive();
            var next = try source.acquireSchemaBoundOwnerMode(self.binding.fence.owner_group_id, self.table_name, self.binding.fence.namespace, require_historical_owner, self.context);
            errdefer next.deinit();
            // Qualify physical identity BEFORE interpreting its domain/job.
            // A replacement may not yet carry this job or source authority;
            // neither absence nor a foreign phase can relabel it as our owner.
            const actual = try source.schemaStateAtLease(&next, self.table_name, self.context);
            if (actual.root_generation != self.initial.root_generation or
                actual.state.physical_root_incarnation != self.initial.observation.physical_root_incarnation)
                return error.SchemaValidationRuntimeRecoveryRequired;
            _ = try observeRaftSchemaAtLease(&next, self.table_name, self.binding, self.context);
            self.lease = next;
        }

        pub fn observe(self: *@This()) !SchemaAttestation.RootedRaftObservation {
            return self.observeWithContext(self.context);
        }

        /// Read-only recovery after known commit. This never proposes a command
        /// or adopts a root, and cannot suppress an existing effect merely because
        /// its initiating request expired while ordered application completed.
        pub fn recoverCommitted(self: *@This()) !SchemaAttestation.RootedRaftObservation {
            return self.observeWithContext(.{});
        }

        /// Query-only producer preparation stays on this exact physical lease.
        /// The returned bytes own their storage and grant no apply authority.
        pub fn prepareProposal(self: *@This(), action: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Action, records: u32, time_ns: u64) !client.Owner.SchemaProposal {
            try self.context.ensureActive();
            std.debug.assert(self.lease.active);
            const normalized = try platformDeadlineContext(self.context);
            var cancellation = normalized.cancellation;
            var proposal = try self.lease.owner().prepareSchemaProposal(self.table_name, self.binding, action, records, time_ns, .{
                .execution_deadline_ns = normalized.deadline_ns,
                .cancellation_ctx = &cancellation,
                .cancellation_fn = cancellationTokenRequested,
            });
            errdefer proposal.deinit();
            if (proposal.physical_root_incarnation != self.initial.observation.physical_root_incarnation or
                try @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(proposal.root_generation) != self.initial.root_generation)
                return error.SchemaValidationRuntimeRecoveryRequired;
            return proposal;
        }

        /// Control-executor preparation for an actual committed entry, never a
        /// client-supplied term/index. The ticket owns a separate lease, so the
        /// caller can release this session without closing its prepared owner.
        /// Parsing and destruction stay off the Raft progress lane. Borrow the
        /// actual ASVR entry rather than re-encoding its potentially large ASVI
        /// payload. The kernel owns its preparation before this call returns.
        pub fn prepareCommitted(self: *@This(), bytes: []const u8, term: u64, index: u64) !PreparedRaftSchemaControl {
            try self.context.ensureActive();
            const command = try @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Command.decodeRaft(bytes);
            if (!std.meta.eql(command.binding, self.binding)) return error.SchemaValidationJobChanged;
            if (command.action == .install) try self.requireSchemaInstallOwner();
            const normalized = try platformDeadlineContext(self.context);
            var cancellation = normalized.cancellation;
            var lease = self.lease.cloneRead();
            errdefer lease.deinit();
            var prepared = try lease.owner().prepareRaftSchemaOwnerControl(self.table_name, bytes, term, index, .{
                .execution_deadline_ns = normalized.deadline_ns,
                .cancellation_ctx = &cancellation,
                .cancellation_fn = cancellationTokenRequested,
            });
            errdefer prepared.deinit();
            if (prepared.physical_root_incarnation != self.initial.observation.physical_root_incarnation or
                try @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(prepared.root_generation) != self.initial.root_generation)
                return error.SchemaValidationRuntimeRecoveryRequired;
            return .{ .lease = lease, .prepared = prepared, .table_name = lease.entry.table_name, .binding = self.binding, .root_generation = self.initial.root_generation, .physical_root_incarnation = self.initial.observation.physical_root_incarnation };
        }

        /// Reads and progress-only controls may share a configured owner.
        /// Installing a new physical schema requires the replay owner whose
        /// public catalog stays deferred until post-publication reconciliation.
        fn requireSchemaInstallOwner(self: *@This()) !void {
            if (!self.lease.active) return error.SchemaValidationRuntimeRecoveryRequired;
            if (self.lease.entry.opened_for_historical_apply) return;
            self.lease.deinit();
            try self.reacquireOwner(true);
        }

        fn observeWithContext(self: *@This(), context: request_operation.RequestContext) !SchemaAttestation.RootedRaftObservation {
            std.debug.assert(self.lease.active);
            const observed = try observeRaftSchemaAtLease(&self.lease, self.table_name, self.binding, context);
            if (observed.root_generation != self.initial.root_generation or
                observed.observation.physical_root_incarnation != self.initial.observation.physical_root_incarnation)
                return error.SchemaValidationRuntimeRecoveryRequired;
            return observed;
        }

        pub const SnapshotParent = struct {
            signed: SchemaAttestation.SignedSnapshotParent,
            source_header: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Header,
            retained: snapshot_retention.RetainedSnapshot,

            pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
                // Memory ownership is independent of the durable reference.
                // Only a separately authorized lifecycle decision may unlink it.
                self.retained.deinit(alloc);
                self.* = undefined;
            }
        };

        /// The private retention callback reads the exact published checkpoint;
        /// requests cannot supply origin, header, footer or completed counters.
        /// This returns source evidence, NOT metadata admission/adoption.
        pub fn attestSnapshotParent(self: *@This(), alloc: std.mem.Allocator, request: snapshot_retention.Request, expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State) !SnapshotParent {
            return self.attestSnapshotParentMode(alloc, request, expected, root, .retain, null);
        }

        /// Reopen the exact durable reference after producer recovery, never
        /// repin a newer CURRENT image or silently replace a missing reference.
        /// The same live-root, manifest and quorum-completion checks still apply.
        pub fn recoverSnapshotParent(self: *@This(), alloc: std.mem.Allocator, request: snapshot_retention.Request, expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State) !SnapshotParent {
            return self.attestSnapshotParentMode(alloc, request, expected, root, .recover, null);
        }

        /// The producer must already have a durable intent. A reserved intent
        /// may finish its first pin if none exists; a captured intent may ONLY
        /// reopen its old reference. Never use this to regenerate a prepared
        /// command/grant after a possible metadata submission.
        pub fn captureSnapshotParent(self: *@This(), alloc: std.mem.Allocator, request: snapshot_retention.Request, expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, captured: ?snapshot_retention.CapturedPayload, observer: snapshot_retention.CaptureObserver) !SnapshotParent {
            return self.attestSnapshotParentMode(alloc, request, expected, root, if (captured) |value| .{ .recover_captured = value } else .recover_or_retain, observer);
        }

        const SnapshotCaptureMode = union(enum) { retain, recover, recover_or_retain, recover_captured: snapshot_retention.CapturedPayload };

        fn attestSnapshotParentMode(self: *@This(), alloc: std.mem.Allocator, request: snapshot_retention.Request, expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, mode: SnapshotCaptureMode, observer: ?snapshot_retention.CaptureObserver) !SnapshotParent {
            try self.context.ensureActive();
            if (!self.lease.active) return error.SchemaValidationRuntimeRecoveryRequired;
            try request.validate();
            try expected.requireBinding(self.binding);
            if (request.group_id != self.binding.fence.owner_group_id) return error.SchemaValidationJobChanged;
            if (self.initial.root_generation != expected.root_generation or
                self.initial.observation.physical_root_incarnation != expected.physical_root_incarnation) return error.SchemaValidationRuntimeRecoveryRequired;
            const source = self.lease.source;
            const retention = source.schema_snapshot_retention orelse return error.NativeSnapshotRequired;
            const barrier = source.read_safety_barrier;
            const certify = barrier.vtable.certify_completed_prefix orelse return error.ReadSafetyBarrierUnavailable;
            if (request.term > expected.reported_term or request.index > expected.reported_applied_index) return error.InvalidControlReceiptPosition;
            var retained = switch (mode) {
                .retain => try retention.retainPublished(alloc, request, self.context),
                .recover => try retention.recoverRetained(alloc, request, self.context),
                .recover_captured => |captured| try retention.recoverCaptured(alloc, request, captured, self.context),
                .recover_or_retain => retention.recoverRetained(alloc, request, self.context) catch |err| switch (err) {
                    error.FileNotFound => try retention.retainPublished(alloc, request, self.context),
                    else => return err,
                },
            };
            errdefer retained.deinit(alloc);
            if (retained.identity.index != request.index or retained.identity.term != request.term or
                !std.mem.eql(u8, &retained.reference, &request.reference) or
                retained.application_manifest.len > 64 * 1024) return error.InvalidSnapshot;
            if (retained.application_manifest.len == 0) return error.NativeSnapshotRequired;
            var arena = std.heap.ArenaAllocator.init(alloc);
            defer arena.deinit();
            const wire = @import("antfly_local_sources").storage_db_native_raft_snapshot_contract;
            const witness = try std.json.parseFromSliceLeaky(wire.MaterializationWitness, arena.allocator(), retained.application_manifest, .{ .ignore_unknown_fields = false, .allocate = .alloc_always, .max_value_len = 64 * 1024 });
            try witness.requireFor(request.group_id, request.index, request.term, retained.payload_len);
            const header = witness.stream.identity.schema_header orelse return error.SchemaValidationJobRequired;
            if (!std.meta.eql(header.binding, self.binding) or witness.stream.identity.origin.?.physical_root_incarnation != expected.physical_root_incarnation)
                return error.SchemaValidationRuntimeRecoveryRequired;
            if (observer) |capture| try capture.record(capture.ptr, retained);
            const normalized = try platformDeadlineContext(self.context);
            const completed = try certify(barrier.ptr, request.group_id, expected.reported_term, @max(request.index, expected.reported_applied_index), normalized.deadline_ns, normalized.cancellation);
            if (completed.incarnation == 0 or completed.term != expected.reported_term or completed.applied_index < request.index or
                completed.applied_index < expected.reported_applied_index) return error.InvalidControlReceiptPosition;
            // Validate the ACTUAL newer completion proof. Never fabricate a
            // proof at the older image cut to make it equal the snapshot index.
            const signed = try SchemaAttestation.SignedSnapshotParent.sign(.{
                .binding = self.binding,
                .origin = witness.stream.identity.origin.?,
                .stream_digest = witness.stream.footer_digest,
                .through_index = witness.stream.identity.through_index,
                .completed = completed,
            }, expected.identity, expected.reporter_incarnation, expected.replica_id, expected.root_generation, root);
            lock(&source.mutex);
            const retired = self.lease.entry.retired or self.lease.entry.closing;
            source.mutex.unlock();
            const generation = @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(source.visibleRootGeneration(request.group_id)) catch return error.SchemaValidationRuntimeRecoveryRequired;
            if (retired or generation != expected.root_generation) return error.SchemaValidationRuntimeRecoveryRequired;
            try self.context.ensureActive();
            return .{ .signed = signed, .source_header = header, .retained = retained };
        }

        /// Sign only facts read on the retained owner. The receiver obtains the
        /// expected scope/enrollment independently; metadata revalidates the ACK.
        pub fn attest(self: *@This(), expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State) !SchemaAttestation.SignedRaftObservation {
            return self.attestMode(expected, root, false);
        }

        /// Replacement-root producer only: even an exact native/data cut
        /// needs an independently quorum-certified SHARED apply prefix. The
        /// ordinary observation fast path is not root-adoption evidence.
        pub fn attestCompleted(self: *@This(), expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State) !SchemaAttestation.SignedRaftObservation {
            return self.attestMode(expected, root, true);
        }

        fn attestMode(self: *@This(), expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, require_completed: bool) !SchemaAttestation.SignedRaftObservation {
            try self.context.ensureActive();
            try expected.requireBinding(self.binding);
            const source = self.lease.source;
            var observed = try self.observe();
            if (require_completed) {
                const barrier = source.read_safety_barrier;
                const certify = barrier.vtable.certify_completed_prefix orelse return error.ReadSafetyBarrierUnavailable;
                const applied = observed.observation.applied orelse return error.InvalidControlReceiptPosition;
                const normalized = try platformDeadlineContext(self.context);
                const completed = try certify(barrier.ptr, self.binding.fence.owner_group_id, expected.reported_term, @max(applied.index, expected.reported_applied_index), normalized.deadline_ns, normalized.cancellation);
                observed = try self.observe();
                observed.completed = completed;
            }
            // Preserve the strict actual-data fast path. Only a gap caused by
            // non-data entries needs a separately quorum-validated prefix.
            if (!require_completed) if (observed.observation.applied) |applied| if (applied.index < expected.reported_applied_index) {
                const barrier = source.read_safety_barrier;
                const capture = barrier.vtable.capture_frozen;
                const validate = barrier.vtable.validate_frozen;
                if ((capture == null) != (validate == null)) return error.ReadSafetyBarrierUnavailable;
                if (capture) |capture_fn| {
                    const proof = try capture_fn(barrier.ptr, self.binding.fence.owner_group_id);
                    observed = try self.observe();
                    const normalized = try platformDeadlineContext(self.context);
                    try validate.?(barrier.ptr, self.binding.fence.owner_group_id, proof, normalized.deadline_ns, normalized.cancellation);
                    observed.completed = proof;
                }
            };
            // A retained lease keeps memory alive but does not make a retired
            // physical owner current. Never sign after a root replacement.
            lock(&source.mutex);
            const retired = self.lease.entry.retired or self.lease.entry.closing;
            source.mutex.unlock();
            const visible_generation = @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(source.visibleRootGeneration(self.binding.fence.owner_group_id)) catch return error.SchemaValidationRuntimeRecoveryRequired;
            if (retired or visible_generation != observed.root_generation)
                return error.SchemaValidationRuntimeRecoveryRequired;
            const signed = try SchemaAttestation.SignedRaftObservation.sign(observed, expected, root);
            try self.context.ensureActive();
            return signed;
        }
    };

    pub const PreparedRaftSchemaControl = struct {
        lease: Lease,
        prepared: client.RaftSchemaPreparation,
        table_name: []const u8,
        binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding,
        root_generation: u64,
        physical_root_incarnation: u128,

        /// Control executor only: drain the ticket before releasing its owner.
        pub fn deinit(self: *@This()) void {
            self.prepared.deinit();
            self.lease.deinit();
            self.* = undefined;
        }

        /// Progress-lane operation. The kernel rechecks its exact prepared
        /// owner and committed term/index; no cold open, encoding or signing.
        pub fn apply(self: *@This(), options: client.Owner.QueryOptions) !void {
            try self.prepared.apply(options);
        }

        /// Read-only control-lane recovery on the applying owner. A successful
        /// apply is not erased by a later initiating-request cancellation.
        pub fn observeCommitted(self: *@This()) !SchemaAttestation.RootedRaftObservation {
            const observed = try observeRaftSchemaAtLease(&self.lease, self.table_name, self.binding, .{});
            if (observed.root_generation != self.root_generation or observed.observation.physical_root_incarnation != self.physical_root_incarnation)
                return error.SchemaValidationRuntimeRecoveryRequired;
            return observed;
        }
    };

    /// Bind before the shared control worker starts. The raw fold commits first;
    /// its exact-entry projection is read off-progress, never a public catalog.
    pub fn withSchemaEntryStore(self: *ProvisionedKernelOwnerSource, store: ?*@import("../storage/data_raft_apply_client.zig").RaftApplyStore) *ProvisionedKernelOwnerSource {
        std.debug.assert(self.apply_control_future == null);
        self.schema_entry_store = store;
        return self;
    }

    /// Trusted committed-entry lane only. This copies three integers into a
    /// fixed pool or borrows an already-prepared ticket; no log payload copy.
    pub fn applyPreparedSchemaEntry(self: *ProvisionedKernelOwnerSource, group: u64, term: u64, index: u64) !void {
        if (!self.apply_control_started.load(.acquire) or self.schema_entry_store == null) return error.RaftApplyWriterUnavailable;
        defer self.apply_control_wake.set(self.apply_control_io.?);
        return self.schema_apply.applyOrEnqueue(.{ .group = group, .term = term, .index = index });
    }

    pub fn retireSchemaApplyGroup(self: *ProvisionedKernelOwnerSource, group: u64) void {
        self.schema_apply.cancelGroup(group);
        if (self.apply_control_started.load(.acquire)) self.apply_control_wake.set(self.apply_control_io.?);
    }

    pub fn discardSchemaApplyForRootReplacement(self: *ProvisionedKernelOwnerSource, group: u64) void {
        self.schema_apply.discardForRootReplacement(group);
        if (self.apply_control_started.load(.acquire)) self.apply_control_wake.set(self.apply_control_io.?);
    }

    const SchemaApplyHost = struct {
        source: *ProvisionedKernelOwnerSource,

        pub fn prepare(self: *@This(), key: @import("../data/schema_apply_queue.zig").Key, canceled: *const std.atomic.Value(bool)) !PreparedRaftSchemaControl {
            return self.source.prepareSchemaEntry(key, canceled);
        }
        pub fn notify(self: *@This()) void {
            const source = self.source;
            lock(&source.mutex);
            const wake = source.apply_ready_wake;
            if (wake != null) source.apply_ready_wake_inflight += 1;
            source.mutex.unlock();
            if (wake) |callback| {
                callback.notify();
                lock(&source.mutex);
                source.apply_ready_wake_inflight -= 1;
                source.mutex.unlock();
            }
        }
    };

    fn prepareSchemaEntry(self: *ProvisionedKernelOwnerSource, key: @import("../data/schema_apply_queue.zig").Key, canceled: *const std.atomic.Value(bool)) !PreparedRaftSchemaControl {
        const Cancel = struct {
            local: *const std.atomic.Value(bool),
            stopping: *const std.atomic.Value(bool),
            fn check(ptr: *const anyopaque) bool {
                const value: *const @This() = @ptrCast(@alignCast(ptr));
                return value.local.load(.acquire) or value.stopping.load(.acquire);
            }
        };
        const cancel: Cancel = .{ .local = canceled, .stopping = &self.schema_control_shutdown };
        const context: request_operation.RequestContext = .{ .cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancel.check } };
        try context.ensureActive();
        const store = self.schema_entry_store orelse return error.StorageKernelOwnerUnavailable;
        const log = @import("../data/schema_control_log.zig");
        var retained = store.committedNormalEntry(key.group, key.term, key.index, log.max_wire_bytes) catch |err| switch (err) {
            error.NotFound => return error.SchemaValidationRuntimeRecoveryRequired,
            else => return err,
        };
        defer retained.deinit();
        var decoded = try log.decode(self.alloc, retained.bytes());
        defer decoded.deinit();
        const binding = decoded.command.binding;
        if (self.online_source_authority != .raft or binding.authority != .raft or binding.fence.owner_group_id != key.group)
            return error.SchemaValidationJobChanged;
        // The envelope's generation was local to the submitting replica.
        // Ordered replay after snapshot publication pins this destination's
        // current generation instead; namespace and immutable command guards
        // still validate against its actual persisted schema/job state.
        const generation = self.visibleRootGeneration(key.group);
        const path = try std.fmt.allocPrint(self.alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, key.group });
        defer self.alloc.free(path);
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = generation,
            .identity = .{ .table_id = binding.fence.namespace.table_id, .shard_id = binding.fence.namespace.shard_id, .range_id = binding.fence.namespace.range_id },
        };
        var lease = try self.acquireDescriptorOnce(key.group, decoded.table(), path, descriptor, .shared, .resident, .{
            .historical_raft_apply = true,
            .durable_catalog = true,
            .require_historical_owner = decoded.command.action == .install,
            .cancellation = context.cancellation,
        });
        errdefer lease.deinit();
        var cancellation = context.cancellation;
        var prepared = try lease.owner().prepareRaftSchemaOwnerControl(lease.entry.table_name, decoded.command_bytes, key.term, key.index, .{
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        });
        errdefer prepared.deinit();
        if (prepared.root_generation != generation or self.visibleRootGeneration(key.group) != generation)
            return error.SchemaValidationRuntimeRecoveryRequired;
        return .{ .lease = lease, .prepared = prepared, .table_name = lease.entry.table_name, .binding = binding, .root_generation = try @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(prepared.root_generation), .physical_root_incarnation = prepared.physical_root_incarnation };
    }

    pub fn acquireRaftSchemaOwnerSession(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding, context: request_operation.RequestContext) !RaftSchemaOwnerSession {
        try context.ensureActive();
        try binding.validate();
        if (binding.authority != .raft or binding.fence.owner_group_id != group_id) return error.SchemaValidationJobChanged;
        var lease = try self.acquireSchemaBoundOwner(group_id, table_name, binding.fence.namespace, context);
        errdefer lease.deinit();
        const initial = try observeRaftSchemaAtLease(&lease, table_name, binding, context);
        return .{ .lease = lease, .table_name = table_name, .binding = binding, .context = context, .initial = initial };
    }

    /// Pin one physical owner until the observation is signed. Metadata must
    /// independently recheck enrollment/placement and the accepted phase; this
    /// read-only replica evidence does not authorize a Raft proposal or install.
    pub fn attestRaftSchemaOwnerAtRoot(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding, expected: SchemaAttestation.RaftScope, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !SchemaAttestation.SignedRaftObservation {
        try context.ensureActive();
        try expected.requireBinding(binding);
        var session = try self.acquireRaftSchemaOwnerSession(group_id, table_name, binding, context);
        defer session.deinit();
        return session.attest(expected, root);
    }

    fn observeRaftSchemaAtLease(lease: *Lease, table_name: []const u8, binding: @import("antfly_local_sources").storage_db_relational_schema_owner_contract.Binding, context: request_operation.RequestContext) !SchemaAttestation.RootedRaftObservation {
        const normalized = try platformDeadlineContext(context);
        var cancellation = normalized.cancellation;
        const result = try lease.owner().raftSchemaOwnerObservation(table_name, binding, .{
            .execution_deadline_ns = normalized.deadline_ns,
            .cancellation_ctx = &cancellation,
            .cancellation_fn = cancellationTokenRequested,
        });
        try context.ensureActive();
        return .{ .root_generation = @import("antfly_local_sources").storage_db_root_signing_identity.attestedRootGeneration(lease.entry.generation) catch return error.SchemaValidationRuntimeRecoveryRequired, .observation = result };
    }

    pub const RootedRaftIndexOwnerObservation = struct {
        identity: raft_index_control.Identity,
        root_generation: u64,
        observation: raft_index_control.Observation,
    };

    /// Retain the exact physical owner across preparation, ordered proposal,
    /// receipt recovery and signing. A second route lookup is not provenance.
    pub const RaftIndexOwnerSession = struct {
        lease: Lease,
        root_generation: u64,
        table_name: []const u8,
        identity: raft_index_control.Identity,
        context: request_operation.RequestContext,

        pub fn deinit(self: *@This()) void {
            self.lease.deinit();
            self.* = undefined;
        }

        /// A committed descriptor can require retiring this resident owner.
        /// Never pin its preparation lease while waiting for ordered apply.
        pub fn releaseForApply(self: *@This()) void {
            self.lease.deinit();
        }

        /// Rejoin only the same physical root, never relabel a replacement
        /// root's observation with the preparation epoch. Failed reacquisition
        /// leaves the session released and safe for its caller's defer.
        pub fn reacquireAfterApply(self: *@This()) !void {
            std.debug.assert(!self.lease.active);
            var next = try self.lease.source.acquireRaftIndexOwnerSession(self.identity.owner_group_id, self.table_name, self.identity, self.context);
            errdefer next.deinit();
            if (next.root_generation != self.root_generation) return error.PrivateIndexRootRecoveryRequired;
            self.lease = next.lease;
        }

        pub fn observe(self: *@This(), alloc: std.mem.Allocator) !RootedRaftIndexOwnerObservation {
            return self.observeWithContext(alloc, self.context);
        }

        /// Once ordered apply succeeded, request cancellation must not hide
        /// its durable outcome. This performs only the bounded retained-cut
        /// lookup on the lease already held, with no routing or metadata IO.
        pub fn recoverCommitted(self: *@This(), alloc: std.mem.Allocator) !RootedRaftIndexOwnerObservation {
            return self.observeWithContext(alloc, .{});
        }

        fn observeWithContext(self: *@This(), alloc: std.mem.Allocator, context: request_operation.RequestContext) !RootedRaftIndexOwnerObservation {
            const normalized = try platformDeadlineContext(context);
            var cancellation = normalized.cancellation;
            const result = try self.lease.owner().raftIndexOwnerObservation(alloc, self.table_name, self.identity, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
            try context.ensureActive();
            return .{ .identity = self.identity, .root_generation = self.root_generation, .observation = result };
        }

        pub fn claims(self: *@This(), alloc: std.mem.Allocator, command: native_index_control.ClaimCommand, recover_committed: bool) !std.json.Parsed(?native_index_control.DeliveredClaims) {
            const b = command.binding;
            const identity = self.identity;
            if (!std.mem.eql(u8, &b.id, &identity.id) or !std.mem.eql(u8, &b.plan_digest, &identity.plan_digest) or
                !std.mem.eql(u8, &b.owner_digest, &identity.owner_digest) or b.ordinal != identity.ordinal or
                b.fence.owner_group_id != identity.owner_group_id or !b.fence.namespace.eql(identity.namespace)) return error.PrivateIndexOwnerJobChanged;
            const normalized = if (recover_committed) request_operation.RequestContext{} else try platformDeadlineContext(self.context);
            var cancellation = normalized.cancellation;
            return self.lease.owner().raftIndexClaims(alloc, self.table_name, command, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        }

        pub fn claimSource(self: *@This(), alloc: std.mem.Allocator) !std.json.Parsed(native_index_control.SourceWire) {
            return self.sourceProjected(alloc, false, false);
        }

        pub fn sourceProjected(self: *@This(), alloc: std.mem.Allocator, consumed_only: bool, recover_committed: bool) !std.json.Parsed(native_index_control.SourceWire) {
            const normalized = if (recover_committed) request_operation.RequestContext{} else try platformDeadlineContext(self.context);
            var cancellation = normalized.cancellation;
            return self.lease.owner().raftIndexSourceProjected(alloc, self.table_name, self.identity, consumed_only, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        }

        pub fn build(self: *@This(), alloc: std.mem.Allocator, request: raft_index_control.BuildRequest, prepare_page: bool) !std.json.Parsed(raft_index_control.BuildPreparation) {
            if (!std.meta.eql(request.identity, self.identity)) return error.PrivateIndexOwnerJobChanged;
            const normalized = if (prepare_page) try platformDeadlineContext(self.context) else request_operation.RequestContext{};
            var cancellation = normalized.cancellation;
            return self.lease.owner().raftIndexOwnerBuild(alloc, self.table_name, request, prepare_page, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        }

        pub fn lifecycle(self: *@This(), alloc: std.mem.Allocator, request: raft_index_control.LifecycleRequest, prepare_work: bool, root: ?native_index_control.AuditRoot) !std.json.Parsed(raft_index_control.LifecyclePreparation) {
            const b = request.binding();
            const identity: raft_index_control.Identity = .{ .id = b.id, .plan_digest = b.plan_digest, .owner_digest = b.owner_digest, .ordinal = b.ordinal, .namespace = b.fence.namespace, .owner_group_id = b.fence.owner_group_id };
            if (!std.meta.eql(identity, self.identity)) return error.PrivateIndexOwnerJobChanged;
            const normalized = if (prepare_work) try platformDeadlineContext(self.context) else request_operation.RequestContext{};
            var cancellation = normalized.cancellation;
            return self.lease.owner().raftIndexOwnerLifecycle(alloc, self.table_name, request, prepare_work, root, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        }

        pub fn rootGeneration(self: *const @This()) u64 {
            return self.root_generation;
        }

        pub fn cancelStep(self: *@This(), alloc: std.mem.Allocator, request: raft_index_control.CancellationRequest, prepare_work: bool) !std.json.Parsed(raft_index_control.CancellationPreparation) {
            if (!std.meta.eql(request.identity(), self.identity)) return error.PrivateIndexOwnerJobChanged;
            const normalized = if (prepare_work) try platformDeadlineContext(self.context) else request_operation.RequestContext{};
            var cancellation = normalized.cancellation;
            return self.lease.owner().raftIndexOwnerCancellation(alloc, self.table_name, request, prepare_work, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        }

        pub fn audit(self: *@This(), alloc: std.mem.Allocator, request: raft_index_control.BuildRequest, root: native_index_control.AuditRoot, recover_committed: bool) !std.json.Parsed(raft_index_control.BuildPreparation) {
            if (!std.meta.eql(request.identity, self.identity)) return error.PrivateIndexOwnerJobChanged;
            try root.validate();
            const normalized = if (recover_committed) request_operation.RequestContext{} else try platformDeadlineContext(self.context);
            var cancellation = normalized.cancellation;
            return self.lease.owner().raftIndexOwnerStep(alloc, self.table_name, request, 2, root, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        }

        pub fn prepare(self: *@This(), alloc: std.mem.Allocator, header: raft_index_control.Admission, manifest: []const u8) !raft_index_control.AdmissionPreparation {
            try header.validate();
            const identity = self.identity;
            if (!std.mem.eql(u8, &header.id, &identity.id) or !std.mem.eql(u8, &header.plan_digest, &identity.plan_digest) or
                !std.mem.eql(u8, &header.owner_digest, &identity.owner_digest) or header.ordinal != identity.ordinal or
                header.fence.owner_group_id != identity.owner_group_id or !header.fence.namespace.eql(identity.namespace)) return error.PrivateIndexOwnerJobChanged;
            if (manifest.len == 0 or manifest.len > raft_index_control.AdmissionPreparation.max_manifest_bytes) return error.InvalidPrivateIndexInputs;
            const normalized = try platformDeadlineContext(self.context);
            var cancellation = normalized.cancellation;
            return self.lease.owner().raftIndexOwnerAdmissionPreparation(alloc, self.table_name, header, manifest, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        }
    };

    pub fn acquireRaftIndexOwnerSession(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8, identity: raft_index_control.Identity, context: request_operation.RequestContext) !RaftIndexOwnerSession {
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        errdefer lease.deinit();
        return .{ .lease = lease, .root_generation = try indexRootEpoch(lease.entry.generation), .table_name = table_name, .identity = identity, .context = context };
    }

    /// Root provenance belongs to the exact leased owner that returned these
    /// facts, not a subsequent route lookup. This is not enrollment, leadership,
    /// metadata permission or a signature. The receiver supplies those checks.
    pub fn raftIndexOwnerObservationAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, identity: raft_index_control.Identity, context: request_operation.RequestContext) !RootedRaftIndexOwnerObservation {
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        const normalized = try platformDeadlineContext(context);
        var cancellation = normalized.cancellation;
        const result = try lease.owner().raftIndexOwnerObservation(alloc, table_name, identity, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        return .{ .identity = identity, .root_generation = try indexRootEpoch(lease.entry.generation), .observation = result };
    }

    pub const RootedRaftIndexOwnerPreparation = struct {
        root_generation: u64,
        preparation: raft_index_control.AdmissionPreparation,
    };

    /// Advisory compilation is pinned to an actual owner root. No manifest
    /// transport copy, catalog mutation or authorization is performed here.
    pub fn raftIndexOwnerPreparationAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, header: raft_index_control.Admission, manifest: []const u8, context: request_operation.RequestContext) !RootedRaftIndexOwnerPreparation {
        try context.ensureActive();
        try header.validate();
        if (manifest.len == 0 or manifest.len > raft_index_control.AdmissionPreparation.max_manifest_bytes) return error.InvalidPrivateIndexInputs;
        var lease = try self.acquireIndexOwner(group_id, table_name, .{ .id = header.id, .plan_digest = header.plan_digest, .owner_digest = header.owner_digest, .ordinal = header.ordinal, .namespace = header.fence.namespace, .owner_group_id = header.fence.owner_group_id }, context);
        defer lease.deinit();
        const normalized = try platformDeadlineContext(context);
        var cancellation = normalized.cancellation;
        const result = try lease.owner().raftIndexOwnerAdmissionPreparation(alloc, table_name, header, manifest, .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .preparation = result };
    }

    /// Claim bytes and expected binding are not a remote capability. The
    /// authenticated receiver independently obtains current metadata permission
    /// before calling this leased physical adapter. Kernel apply rechecks the
    /// durable job, actual receiving range and cancellation in its transaction.
    /// Retain the physical root from the lease that committed these claims.
    /// This is provenance, not enrollment or a signed publication receipt.
    pub const RootedNativeIndexOwnerClaims = struct {
        root_generation: u64,
        outcome: std.json.Parsed(native_index_control.ClaimOutcome),

        pub fn deinit(self: *RootedNativeIndexOwnerClaims) void {
            self.outcome.deinit();
            self.* = undefined;
        }
    };

    pub const RootedNativeIndexSourceConsumption = struct {
        root_generation: u64,
        outcome: std.json.Parsed(native_index_control.ConsumeOutcome),
        pub fn deinit(self: *RootedNativeIndexSourceConsumption) void {
            self.outcome.deinit();
            self.* = undefined;
        }
    };

    pub const RootedNativeIndexOwnerBuild = struct {
        root_generation: u64,
        outcome: std.json.Parsed(native_index_control.BuildResponse),
    };

    pub const RootedNativeIndexClaimSource = struct {
        root_generation: u64,
        outcome: native_index_control.SourceRead,
        wire: std.json.Parsed(native_index_control.SourceWire),
        pub fn deinit(self: *RootedNativeIndexClaimSource) void {
            self.outcome.deinit();
            self.wire.deinit();
            self.* = undefined;
        }
    };

    pub const RootedNativeIndexOwnerInstallation = struct {
        root_generation: u64,
        outcome: std.json.Parsed(native_index_control.InstallationResponse),
        pub fn deinit(self: *@This()) void {
            self.outcome.deinit();
            self.* = undefined;
        }
    };

    /// Metadata permission is checked independently by the receiving server.
    /// Physical checks and installation use one applying lease; root generation
    /// is never reconstructed from routing after that lease has been released.
    pub fn stepNativeIndexOwnerInstallationAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request: native_index_control.InstallationRequest, audit_root_incarnation: u128, context: request_operation.RequestContext) !RootedNativeIndexOwnerInstallation {
        try context.ensureActive();
        try request.validate();
        const binding = request.intent.validation.command.binding;
        const identity: native_index_control.Identity = .{ .id = binding.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = binding.fence.owner_group_id };
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(encoded);
        if (encoded.len > 32 * 1024) return error.TransactionTooLarge;
        const root: native_index_control.AuditRoot = .{ .root_dir = self.replica_root_dir, .root_incarnation = audit_root_incarnation };
        try root.validate();
        const root_scope = try std.json.Stringify.valueAlloc(alloc, root, .{});
        defer alloc.free(root_scope);
        const native_context = try platformDeadlineContext(context);
        var cancellation = native_context.cancellation;
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerInstallationScopedJson(table_name, encoded, root_scope, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        if (response.bytes().len > native_index_control.max_response_bytes) return error.InvalidPrivateIndexNativeReceipt;
        var result = try std.json.parseFromSlice(native_index_control.InstallationResponse, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer result.deinit();
        try result.value.validateFor(request);
        if (result.value.physicalAudit().audit.root_incarnation != audit_root_incarnation) return error.InvalidPrivateIndexNativeReceipt;
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = result };
    }

    pub const RootedNativeIndexOwnerRelease = struct {
        root_generation: u64,
        outcome: std.json.Parsed(native_index_control.ReleaseOutcome),
        pub fn deinit(self: *@This()) void {
            self.outcome.deinit();
            self.* = undefined;
        }
    };

    /// One applying lease binds both the terminal effect and its root evidence.
    /// Exact retries use the durable terminal record, not retired job state.
    pub fn applyNativeIndexOwnerReleaseAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, intent: @FieldType(native_index_control.ReleaseOutcome, "intent"), expected_root_generation: u64, signing_root_incarnation: u128, context: request_operation.RequestContext) !RootedNativeIndexOwnerRelease {
        try context.ensureActive();
        try intent.validate();
        if (expected_root_generation == 0) return error.PrivateIndexRootRecoveryRequired;
        const binding = intent.installed.command.validation.command.binding;
        const identity: native_index_control.Identity = .{ .id = binding.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = binding.fence.owner_group_id };
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        try requireAcceptedNativeRootGeneration(lease.entry.generation, expected_root_generation);
        const encoded = try std.json.Stringify.valueAlloc(alloc, intent, .{});
        defer alloc.free(encoded);
        if (encoded.len > 32 * 1024) return error.TransactionTooLarge;
        const root: native_index_control.AuditRoot = .{ .root_dir = self.replica_root_dir, .root_incarnation = signing_root_incarnation };
        try root.validate();
        const scope = try std.json.Stringify.valueAlloc(alloc, root, .{});
        defer alloc.free(scope);
        const native_context = try platformDeadlineContext(context);
        var cancellation = native_context.cancellation;
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerReleaseScopedJson(table_name, encoded, scope, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        if (response.bytes().len > native_index_control.max_response_bytes) return error.InvalidPrivateIndexNativeReceipt;
        var result = try std.json.parseFromSlice(native_index_control.ReleaseOutcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer result.deinit();
        try result.value.validate();
        if (!std.meta.eql(result.value.intent, intent)) return error.InvalidPrivateIndexNativeReceipt;
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = result };
    }

    pub const RootedNativeIndexOwnerCancellationCompletion = struct {
        root_generation: u64,
        outcome: std.json.Parsed(native_index_control.CancellationCompletionOutcome),
        pub fn deinit(self: *@This()) void {
            self.outcome.deinit();
            self.* = undefined;
        }
    };

    pub const RootedNativeIndexOwnerCancellationRelease = struct {
        root_generation: u64,
        outcome: std.json.Parsed(native_index_control.CancellationReleaseOutcome),
        pub fn deinit(self: *@This()) void {
            self.outcome.deinit();
            self.* = undefined;
        }
    };

    /// A fresh metadata grant selects the expected physical generation. Check
    /// it on the SAME applying lease before invoking the irreversible effect;
    /// a post-effect signing check cannot protect a replacement root's fence.
    pub fn applyNativeIndexOwnerCancellationReleaseAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, intent: @FieldType(native_index_control.CancellationReleaseOutcome, "intent"), expected_root_generation: u64, signing_root_incarnation: u128, context: request_operation.RequestContext) !RootedNativeIndexOwnerCancellationRelease {
        try context.ensureActive();
        try intent.validate();
        if (expected_root_generation == 0) return error.PrivateIndexRootRecoveryRequired;
        const binding = intent.canceled.binding;
        const identity: native_index_control.Identity = .{ .id = binding.id, .plan_digest = binding.plan_digest, .owner_digest = binding.owner_digest, .ordinal = binding.ordinal, .namespace = binding.fence.namespace, .owner_group_id = binding.fence.owner_group_id };
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        try requireAcceptedNativeRootGeneration(lease.entry.generation, expected_root_generation);
        const encoded = try std.json.Stringify.valueAlloc(alloc, intent, .{});
        defer alloc.free(encoded);
        if (encoded.len > 32 * 1024) return error.TransactionTooLarge;
        const root: native_index_control.AuditRoot = .{ .root_dir = self.replica_root_dir, .root_incarnation = signing_root_incarnation };
        try root.validate();
        const scope = try std.json.Stringify.valueAlloc(alloc, root, .{});
        defer alloc.free(scope);
        const native_context = try platformDeadlineContext(context);
        var cancellation = native_context.cancellation;
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerCancellationReleaseScopedJson(table_name, encoded, scope, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        if (response.bytes().len == 0 or response.bytes().len > native_index_control.max_response_bytes) return error.InvalidPrivateIndexNativeReceipt;
        var result = try std.json.parseFromSlice(native_index_control.CancellationReleaseOutcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer result.deinit();
        try result.value.validate();
        if (!std.meta.eql(result.value.intent, intent)) return error.InvalidPrivateIndexNativeReceipt;
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = result };
    }

    /// The applying lease supplies the observed generation. No physical job,
    /// cleanup certificate, native cut or root path enters the remote body.
    pub fn completeNativeIndexOwnerCancellationAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, identity: native_index_control.Identity, signing_root_incarnation: u128, context: request_operation.RequestContext) !RootedNativeIndexOwnerCancellationCompletion {
        try context.ensureActive();
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, identity, .{});
        defer alloc.free(encoded);
        const root: native_index_control.AuditRoot = .{ .root_dir = self.replica_root_dir, .root_incarnation = signing_root_incarnation };
        try root.validate();
        const scope = try std.json.Stringify.valueAlloc(alloc, root, .{});
        defer alloc.free(scope);
        const native_context = try platformDeadlineContext(context);
        var cancellation = native_context.cancellation;
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerCancellationCompletionScopedJson(table_name, encoded, scope, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        if (response.bytes().len == 0 or response.bytes().len > native_index_control.max_response_bytes) return error.InvalidPrivateIndexNativeReceipt;
        var result = try std.json.parseFromSlice(native_index_control.CancellationCompletionOutcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer result.deinit();
        try result.value.validate();
        const binding = result.value.binding;
        if (!std.mem.eql(u8, &binding.id, &identity.id) or !std.mem.eql(u8, &binding.plan_digest, &identity.plan_digest) or
            !std.mem.eql(u8, &binding.owner_digest, &identity.owner_digest) or binding.ordinal != identity.ordinal or
            binding.fence.owner_group_id != identity.owner_group_id or !binding.fence.namespace.eql(identity.namespace)) return error.PrivateIndexOwnerJobChanged;
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = result };
    }

    /// Source bytes and root generation come from one pinned compiled owner.
    /// No route/status reread, schema compiler or caller-selected cut is used.
    pub fn readNativeIndexClaimSourceAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, identity: native_index_control.Identity, context: request_operation.RequestContext) !RootedNativeIndexClaimSource {
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, identity, .{});
        defer alloc.free(encoded);
        const native_context = try platformDeadlineContext(context);
        var cancellation = native_context.cancellation;
        try context.ensureActive();
        var response = try lease.owner().nativeIndexClaimSourceJson(table_name, encoded, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        if (response.bytes().len > native_index_control.SourceWire.max_wire_bytes) return error.InvalidPrivateIndexClaimInventory;
        var parsed = try std.json.parseFromSlice(native_index_control.SourceWire, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false, .max_value_len = native_index_control.SourceWire.max_payload_b64 });
        errdefer parsed.deinit();
        var result = try parsed.value.decode(alloc, identity);
        errdefer result.deinit();
        try context.ensureActive();
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = result, .wire = parsed };
    }

    pub fn stepNativeIndexOwnerBuildAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request: native_index_control.BuildRequest, audit_root_incarnation: ?u128, context: request_operation.RequestContext) !RootedNativeIndexOwnerBuild {
        try context.ensureActive();
        try request.validate();
        var lease = try self.acquireIndexOwner(group_id, table_name, request.identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(encoded);
        if (encoded.len > 32 * 1024) return error.TransactionTooLarge;
        const native_context = try platformDeadlineContext(context);
        var cancellation = native_context.cancellation;
        try context.ensureActive();
        const root_scope = if (request.target_expectation != null) scope: {
            const value: native_index_control.AuditRoot = .{ .root_dir = self.replica_root_dir, .root_incarnation = audit_root_incarnation orelse return error.NativeIndexSigningUnavailable };
            try value.validate();
            break :scope try std.json.Stringify.valueAlloc(alloc, value, .{});
        } else null;
        defer if (root_scope) |bytes| alloc.free(bytes);
        var response = try lease.owner().nativeIndexOwnerBuildScopedJson(table_name, encoded, root_scope orelse "", .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        if (response.bytes().len > native_index_control.max_response_bytes) return error.InvalidPrivateIndexNativeReceipt;
        var result = try std.json.parseFromSlice(native_index_control.BuildResponse, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer result.deinit();
        try result.value.validateFor(request);
        if (result.value == .audit and result.value.audit.audit.root_incarnation != audit_root_incarnation) return error.InvalidPrivateIndexNativeReceipt;
        if (result.value == .validated and result.value.validated.physical.audit.root_incarnation != audit_root_incarnation) return error.InvalidPrivateIndexNativeReceipt;
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = result };
    }

    pub fn applyNativeIndexOwnerClaimsAtRoot(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        command: native_index_control.ClaimCommand,
        context: request_operation.RequestContext,
    ) !RootedNativeIndexOwnerClaims {
        try context.ensureActive();
        try command.validate();
        const binding = command.binding;
        var lease = try self.acquireIndexOwner(group_id, table_name, .{
            .id = binding.id,
            .plan_digest = binding.plan_digest,
            .owner_digest = binding.owner_digest,
            .ordinal = binding.ordinal,
            .namespace = binding.fence.namespace,
            .owner_group_id = binding.fence.owner_group_id,
        }, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, command, .{});
        defer alloc.free(encoded);
        if (encoded.len > native_index_control.max_claim_wire_bytes) return error.TransactionTooLarge;
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerClaimsJson(table_name, encoded);
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(native_index_control.ClaimOutcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer parsed.deinit();
        try parsed.value.validateFor(command);
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = parsed };
    }

    /// Uses the generation of the actual applying lease, not a later lookup.
    /// The receiver must independently authenticate metadata acceptance first.
    pub fn consumeNativeIndexSourceAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, command: native_index_control.ConsumeCommand, context: request_operation.RequestContext) !RootedNativeIndexSourceConsumption {
        try context.ensureActive();
        try command.validate();
        const binding = command.binding;
        var lease = try self.acquireIndexOwner(group_id, table_name, .{
            .id = binding.id,
            .plan_digest = binding.plan_digest,
            .owner_digest = binding.owner_digest,
            .ordinal = binding.ordinal,
            .namespace = binding.fence.namespace,
            .owner_group_id = binding.fence.owner_group_id,
        }, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, command, .{});
        defer alloc.free(encoded);
        if (encoded.len > 32 * 1024) return error.TransactionTooLarge;
        const native_context = try platformDeadlineContext(context);
        var cancellation = native_context.cancellation;
        try context.ensureActive();
        var response = try lease.owner().nativeIndexSourceConsumptionJson(table_name, encoded, .{ .execution_deadline_ns = native_context.deadline_ns, .cancellation_ctx = &cancellation, .cancellation_fn = cancellationTokenRequested });
        defer response.deinit();
        if (response.bytes().len > 32 * 1024) return error.InvalidPrivateIndexNativeReceipt;
        var parsed = try std.json.parseFromSlice(native_index_control.ConsumeOutcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer parsed.deinit();
        try parsed.value.validateFor(command);
        // Preserve committed/pending results even if ingress was canceled.
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = parsed };
    }

    pub fn applyNativeIndexOwnerClaims(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        command: native_index_control.ClaimCommand,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.ClaimOutcome) {
        // Transfer the owned result without reconstructing its physical root
        // from another lookup. Signing callers must use the rooted operation.
        return (try self.applyNativeIndexOwnerClaimsAtRoot(alloc, group_id, table_name, command, context)).outcome;
    }

    pub fn nativeIndexOwnerState(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        identity: native_index_control.Identity,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(?index_owner_job.Binding) {
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, identity, .{});
        defer alloc.free(encoded);
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerStateJson(table_name, encoded);
        defer response.deinit();
        return std.json.parseFromSlice(?index_owner_job.Binding, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    }

    pub fn nativeIndexOwnerCancellationState(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        identity: native_index_control.Identity,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(index_owner_job.CancellationState) {
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, identity, .{});
        defer alloc.free(encoded);
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerCancellationStateJson(table_name, encoded);
        defer response.deinit();
        return std.json.parseFromSlice(index_owner_job.CancellationState, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    }

    /// The generation is observed from the same pinned physical owner that
    /// recovered the cut, never from a later routing/status lookup. This is not
    /// an attestation: pending outcomes still carry no publishable receipt, and
    /// enrollment, replica placement and signing remain the receiver's duties.
    pub const RootedNativeIndexOwnerReceipt = struct {
        root_generation: u64,
        outcome: std.json.Parsed(?native_index_control.Outcome),

        pub fn deinit(self: *RootedNativeIndexOwnerReceipt) void {
            self.outcome.deinit();
            self.* = undefined;
        }
    };

    pub fn nativeIndexOwnerReceiptAtRoot(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: native_index_control.ReceiptRequest,
        context: request_operation.RequestContext,
    ) !RootedNativeIndexOwnerReceipt {
        var lease = try self.acquireIndexOwner(group_id, table_name, request.identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(encoded);
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerReceiptJson(table_name, encoded);
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(?native_index_control.Outcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer parsed.deinit();
        if (parsed.value) |value| try value.validateRecovery(request);
        return .{
            .root_generation = try indexRootEpoch(lease.entry.generation),
            .outcome = parsed,
        };
    }

    pub fn nativeIndexOwnerReceipt(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: native_index_control.ReceiptRequest,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(?native_index_control.Outcome) {
        // Transfer the parsed response; this observational caller does not
        // sign it and must not reconstruct a generation from another lookup.
        return (try self.nativeIndexOwnerReceiptAtRoot(alloc, group_id, table_name, request, context)).outcome;
    }

    /// Queue shared GC at the leased physical owner, not a canceled ACK.
    pub fn cleanupNativeIndexOwner(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        identity: native_index_control.Identity,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.CleanupOutcome) {
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, identity, .{});
        defer alloc.free(encoded);
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerCleanupJson(table_name, encoded);
        defer response.deinit();
        return std.json.parseFromSlice(native_index_control.CleanupOutcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    }

    /// One bounded derived-artifact page at the exact leased physical owner.
    /// A committed page is not converted into a request-cancellation failure.
    pub fn collectNativeIndexOwnerGarbage(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        identity: native_index_control.Identity,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.CleanupMaintenanceOutcome) {
        return self.collectNativeIndexOwnerDerivedGarbage(alloc, group_id, table_name, identity, context, false);
    }

    pub fn collectNativeIndexOwnerClaimGarbage(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        identity: native_index_control.Identity,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.ClaimCleanupMaintenanceOutcome) {
        return self.collectNativeIndexOwnerDerivedGarbage(alloc, group_id, table_name, identity, context, true);
    }

    fn collectNativeIndexOwnerDerivedGarbage(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        identity: native_index_control.Identity,
        context: request_operation.RequestContext,
        comptime claims: bool,
    ) !std.json.Parsed(if (claims) native_index_control.ClaimCleanupMaintenanceOutcome else native_index_control.CleanupMaintenanceOutcome) {
        const Outcome = if (claims) native_index_control.ClaimCleanupMaintenanceOutcome else native_index_control.CleanupMaintenanceOutcome;
        var lease = try self.acquireIndexOwner(group_id, table_name, identity, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, identity, .{});
        defer alloc.free(encoded);
        try context.ensureActive();
        var response = if (claims) try lease.owner().nativeIndexOwnerClaimGcJson(table_name, encoded) else try lease.owner().nativeIndexOwnerGcJson(table_name, encoded);
        defer response.deinit();
        return std.json.parseFromSlice(Outcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    }

    /// Persist intent at the leased physical owner; this is not a canceled ACK.
    pub fn cancelNativeIndexOwnerIntent(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        intent: native_index_control.Admission,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.CancellationOutcome) {
        try context.ensureActive();
        try intent.validate();
        var lease = try self.acquireIndexOwner(group_id, table_name, .{
            .id = intent.id,
            .plan_digest = intent.plan_digest,
            .owner_digest = intent.owner_digest,
            .ordinal = intent.ordinal,
            .namespace = intent.fence.namespace,
            .owner_group_id = intent.fence.owner_group_id,
        }, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, intent, .{});
        defer alloc.free(encoded);
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerCancellationJson(table_name, encoded);
        defer response.deinit();
        // Cancellation observed after commit must not erase durable progress.
        return std.json.parseFromSlice(native_index_control.CancellationOutcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    }

    pub fn admitNativeIndexOwner(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        admission: native_index_control.Admission,
        manifest: []const u8,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.Outcome) {
        return self.admitNativeIndexOwnerImpl(alloc, group_id, table_name, admission, manifest, context, false);
    }

    pub fn admitCanceledNativeIndexOwner(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        admission: native_index_control.Admission,
        manifest: []const u8,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.CleanupOutcome) {
        return self.admitNativeIndexOwnerImpl(alloc, group_id, table_name, admission, manifest, context, true);
    }

    fn admitNativeIndexOwnerImpl(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        admission: native_index_control.Admission,
        manifest: []const u8,
        context: request_operation.RequestContext,
        comptime canceling: bool,
    ) !std.json.Parsed(if (canceling) native_index_control.CleanupOutcome else native_index_control.Outcome) {
        try context.ensureActive();
        if (manifest.len == 0 or manifest.len > native_index_control.Prepared.max_manifest_bytes)
            return error.InvalidPrivateIndexInputs;
        var lease = try self.acquireIndexOwner(group_id, table_name, .{
            .id = admission.id,
            .plan_digest = admission.plan_digest,
            .owner_digest = admission.owner_digest,
            .ordinal = admission.ordinal,
            .namespace = admission.fence.namespace,
            .owner_group_id = admission.fence.owner_group_id,
        }, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, admission, .{});
        defer alloc.free(encoded);
        try context.ensureActive();
        var response = if (canceling)
            try lease.owner().nativeIndexOwnerCancellationAdmissionJson(table_name, encoded, manifest)
        else
            try lease.owner().nativeIndexOwnerAdmissionJson(table_name, encoded, manifest);
        defer response.deinit();
        // A committed pending outcome must survive cancellation observed after
        // the ABI returns. Do not turn durable success into a retry-looking error.
        return std.json.parseFromSlice(if (canceling) native_index_control.CleanupOutcome else native_index_control.Outcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
    }

    /// Keep the applying lease's root with its owned committed outcome. A
    /// subsequent status lookup may already refer to a replacement root.
    pub const RootedNativeIndexOwnerControl = RootedNativeControl(false);

    fn RootedNativeControl(comptime canceling: bool) type {
        return struct {
            root_generation: u64,
            outcome: std.json.Parsed(if (canceling) native_index_control.CleanupOutcome else native_index_control.Outcome),

            pub fn deinit(self: *@This()) void {
                self.outcome.deinit();
                self.* = undefined;
            }
        };
    }

    pub fn applyNativeIndexOwnerControlAtRoot(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        command: native_index_control.Command,
        context: request_operation.RequestContext,
    ) !RootedNativeIndexOwnerControl {
        return self.applyNativeIndexOwnerControlImpl(alloc, group_id, table_name, command, context, false);
    }

    pub fn applyNativeIndexOwnerControl(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        command: native_index_control.Command,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.Outcome) {
        return (try self.applyNativeIndexOwnerControlAtRoot(alloc, group_id, table_name, command, context)).outcome;
    }

    pub fn applyCanceledNativeIndexOwnerControl(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        command: native_index_control.Command,
        context: request_operation.RequestContext,
    ) !std.json.Parsed(native_index_control.CleanupOutcome) {
        return (try self.applyNativeIndexOwnerControlImpl(alloc, group_id, table_name, command, context, true)).outcome;
    }

    fn applyNativeIndexOwnerControlImpl(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        command: native_index_control.Command,
        context: request_operation.RequestContext,
        comptime canceling: bool,
    ) !RootedNativeControl(canceling) {
        try context.ensureActive();
        try command.validate();
        const operation: abi.NativeIndexOwnerOperation = switch (command.effect) {
            .fenced => if (canceling) .cancel_fence else .fence,
            .reserved => if (canceling) .cancel_reserve else .reserve,
            else => return error.InvalidPrivateIndexOwnerJob,
        };
        var lease = try self.acquireIndexOwner(group_id, table_name, .{
            .id = command.binding.id,
            .plan_digest = command.binding.plan_digest,
            .owner_digest = command.binding.owner_digest,
            .ordinal = command.binding.ordinal,
            .namespace = command.binding.fence.namespace,
            .owner_group_id = command.binding.fence.owner_group_id,
        }, context);
        defer lease.deinit();
        const encoded = try std.json.Stringify.valueAlloc(alloc, command, .{});
        defer alloc.free(encoded);
        try context.ensureActive();
        var response = try lease.owner().nativeIndexOwnerControlJson(table_name, operation, encoded);
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(if (canceling) native_index_control.CleanupOutcome else native_index_control.Outcome, alloc, response.bytes(), .{ .allocate = .alloc_always, .ignore_unknown_fields = false });
        errdefer parsed.deinit();
        if (!canceling) try parsed.value.validateFor(command);
        return .{ .root_generation = try indexRootEpoch(lease.entry.generation), .outcome = parsed };
    }

    pub const NativeFkRead = enum {
        identity,
        catalog,
        publication,
        coverage,
        status,
        retirement_status,
        public_schema,
        initial_child_preflight,
        pub fn options(self: NativeFkRead, context: request_operation.RequestContext) db_types.LookupOptions {
            var result: db_types.LookupOptions = .{ .execution_deadline_ns = context.deadline_ns, .cancellation = context.cancellation };
            switch (self) {
                .identity => result.relational_topology_json = "{\"mode\":\"identity\"}",
                .catalog => {
                    result.relational_integrity_catalog = true;
                    result.fk_generation_source_control = true;
                },
                .publication => result.relational_topology_json = "{\"mode\":\"generation_publication\"}",
                .coverage => {
                    result.relational_activation_json = "{\"mode\":\"status\"}";
                    result.fk_generation_source_control = true;
                },
                .status => {
                    result.relational_topology_json = "{\"mode\":\"status\"}";
                    result.fk_generation_source_control = true;
                },
                .retirement_status => {
                    result.relational_topology_json = "{\"mode\":\"retirement_status\"}";
                    result.fk_generation_source_control = true;
                },
                .public_schema => {
                    result.relational_topology_json = "{\"mode\":\"public_schema\"}";
                    result.fk_generation_source_control = true;
                },
                .initial_child_preflight => result.relational_topology_json = "{\"mode\":\"initial_child_preflight\"}",
            }
            return result;
        }
    };

    /// Only fixed metadata-control observations are exposed, never arbitrary
    /// row keys/projections. The caller separately holds the job permission.
    pub fn lookupNativeFkControl(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, kind: NativeFkRead, context: request_operation.RequestContext) !?table_read_source.LookupResponse {
        const normalized = try platformDeadlineContext(context);
        var lease = try self.acquireControlWithControls(group, table, .{ .allow_deferred_catalog = true, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        defer lease.deinit();
        if (lease.entry.sql_persistence == null) return error.InvalidSqlRelationPersistence;
        const encoded = try table_reads.encodeStorageKernelLookupRequest(alloc, "", kind.options(normalized));
        defer alloc.free(encoded);
        var response = lease.owner().lookupJson(table, encoded) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()), .version = response.version(), .expected_content_digest = response.expectedContentDigest() };
    }

    pub const NativeRetirementCaptureExpected = struct {
        namespace: @import("antfly_local_sources").storage_db_doc_identity.Namespace,
        descriptor: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor,
        schema_json: []const u8,
        indexes_json: []const u8,
    };
    pub const NativeRetirementCapture = struct {
        identity: @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity,
        catalog_json: []u8,
        native_proof: NativeGroupProof,
        pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
            alloc.free(self.catalog_json);
            self.* = undefined;
        }
    };

    /// The receiver must independently rejoin the reserved retirement Head,
    /// parent Child and immutable Manifest before invoking this private port.
    /// This grants no row access and accepts no keys or caller-selected lookup
    /// controls. The path and physical root are selected by the current source.
    ///
    /// A retained owner prevents image replacement throughout the fixed reads.
    /// Identity/catalog/identity are separate native read transactions: raw
    /// catalog hashing and the monotone topology epoch rejoin their evidence;
    /// Boot/initialized/physical Pin are checked again after capture. The final
    /// retirement seal/install still revalidates the returned topology fence.
    pub fn captureNativeRetirementPreparation(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, expected: NativeRetirementCaptureExpected, context: request_operation.RequestContext) !NativeRetirementCapture {
        if (!context.setting_admin or !context.schema_validation_authority or !context.fk_generation_publication_authority) return error.Forbidden;
        try context.ensureActive();
        try expected.descriptor.validate();
        const max_metadata = @import("../metadata/table_retirement_manifest.zig").max_bytes;
        if (expected.schema_json.len == 0 or expected.schema_json.len > max_metadata or expected.indexes_json.len > max_metadata or table.len > 1024) return error.InvalidTableRetirementManifest;
        if (group == 0 or table.len == 0 or expected.namespace.table_id == 0 or expected.namespace.shard_id == 0 or expected.namespace.range_id == 0) return error.IdentityNamespaceMismatch;
        const normalized = try platformDeadlineContext(context);
        var captured = try self.loadDescriptorWithDeadline(alloc, group, table, normalized.deadline_ns);
        defer captured.deinit(alloc);
        if (captured.sql_persistence == null or !captured.sql_persistence.?.eql(expected.descriptor) or
            captured.identity.table_id != expected.namespace.table_id or captured.identity.shard_id != expected.namespace.shard_id or captured.identity.range_id != expected.namespace.range_id or
            !std.mem.eql(u8, captured.schema_json, expected.schema_json) or !std.mem.eql(u8, captured.indexes_json, expected.indexes_json)) return error.SqlRelationOwnerChanged;
        const controls: ReadControls = .{ .native_boot_observed = true, .allow_deferred_catalog = true, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation };
        const proof = try self.nativeProofForDescriptor(captured.view(), controls);
        var admitted = controls;
        admitted.native_initialized = proof.initialized;
        var lease = try self.acquireDescriptorWithMode(group, table, captured.path, captured.view(), false, .resident, admitted);
        defer lease.deinit();
        const persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
        const pin = try persistence.PhysicalPin.fromBinding(proof.initialized);
        const physical = try lease.owner().nativeRelationBinding(alloc, table);
        if (!pin.eql(try persistence.PhysicalPin.fromBinding(physical)) or !physical.descriptor.eql(expected.descriptor) or
            !std.mem.eql(u8, &physical.native_instance, &proof.boot.instance)) return error.SqlRelationOwnerChanged;
        const Reader = struct {
            fn identity(a: std.mem.Allocator, held: *Lease, name: []const u8, ctx: request_operation.RequestContext) !@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity {
                const body = try table_reads.encodeStorageKernelLookupRequest(a, "", NativeFkRead.identity.options(ctx));
                defer a.free(body);
                var response = try held.owner().lookupJson(name, body);
                defer response.deinit();
                if (response.bytes().len == 0 or response.bytes().len > 8192) return error.InvalidIntegrityCatalog;
                var parsed = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Identity, a, response.bytes(), .{});
                defer parsed.deinit();
                return parsed.value;
            }
        };
        const before = try Reader.identity(alloc, &lease, table, normalized);
        if (!before.namespace.eql(expected.namespace) or before.next_epoch == 0 or !before.catalog_checks_identity_supported) return error.IdentityNamespaceMismatch;
        const body = try table_reads.encodeStorageKernelLookupRequest(alloc, "", NativeFkRead.catalog.options(normalized));
        defer alloc.free(body);
        var response = try lease.owner().lookupJson(table, body);
        defer response.deinit();
        const max_catalog = @import("antfly_local_sources").storage_db_relational_integrity_catalog.max_catalog_bytes;
        if (response.bytes().len == 0 or response.bytes().len > std.base64.standard.Encoder.calcSize(max_catalog) + 1024) return error.InvalidIntegrityCatalog;
        const Envelope = struct { catalog: []const u8, schema_version: u32, table_id: []const u8 };
        var parsed = try std.json.parseFromSlice(Envelope, alloc, response.bytes(), .{});
        defer parsed.deinit();
        const length = std.base64.standard.Decoder.calcSizeForSlice(parsed.value.catalog) catch return error.InvalidIntegrityCatalog;
        if (length == 0 or length > max_catalog or (std.fmt.parseInt(u64, parsed.value.table_id, 10) catch return error.InvalidIntegrityCatalog) != expected.namespace.table_id) return error.InvalidIntegrityCatalog;
        var schema = try @import("antfly_local_sources").schema_mod.parseValidatedTableSchema(alloc, expected.schema_json);
        defer schema.deinit(alloc);
        if (parsed.value.schema_version != schema.version) return error.IntegrityCatalogChanged;
        const raw = try alloc.alloc(u8, length);
        defer alloc.free(raw);
        std.base64.standard.Decoder.decode(raw, parsed.value.catalog) catch return error.InvalidIntegrityCatalog;
        var digest: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(raw, &digest, .{});
        if (!std.mem.eql(u8, &digest, &before.catalog_digest)) return error.IntegrityCatalogChanged;
        const after = try Reader.identity(alloc, &lease, table, normalized);
        if (!std.meta.eql(before, after)) return error.IntegrityCatalogChanged;
        const current_proof = try self.nativeProofForDescriptor(captured.view(), admitted);
        const current_physical = try lease.owner().nativeRelationBinding(alloc, table);
        if (!std.meta.eql(proof.boot, current_proof.boot) or !std.meta.eql(proof.initialized, current_proof.initialized) or
            !pin.eql(try persistence.PhysicalPin.fromBinding(current_physical)) or !current_physical.descriptor.eql(expected.descriptor) or
            !std.mem.eql(u8, &current_physical.native_instance, &proof.boot.instance)) return error.SqlRelationOwnerChanged;
        var current_descriptor = try self.loadDescriptorWithDeadline(alloc, group, table, normalized.deadline_ns);
        defer current_descriptor.deinit(alloc);
        if (!exactApplyDescriptor(lease.entry, current_descriptor.view())) return error.SqlRelationOwnerChanged;
        try context.ensureActive();
        return .{ .identity = before, .catalog_json = try alloc.dupe(u8, response.bytes()), .native_proof = current_proof };
    }

    pub fn lookupNativeFkIndexStatus(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, request: @import("antfly_local_sources").storage_db_relational_index_status_contract.Request, context: request_operation.RequestContext) !?table_read_source.LookupResponse {
        const normalized = try platformDeadlineContext(context);
        var lease = try self.acquireControlWithControls(group, table, .{ .allow_deferred_catalog = true, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        defer lease.deinit();
        if (lease.entry.sql_persistence == null) return error.InvalidSqlRelationPersistence;
        const status_json = try std.json.Stringify.valueAlloc(alloc, request, .{});
        defer alloc.free(status_json);
        const encoded = try table_reads.encodeStorageKernelLookupRequest(alloc, "", .{ .relational_index_status_json = status_json, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        defer alloc.free(encoded);
        var response = lease.owner().lookupJson(table, encoded) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()), .version = response.version(), .expected_content_digest = response.expectedContentDigest() };
    }

    /// Re-attest the durable native receipt after reply loss or clean same-root
    /// adoption. The actual effect is not executed by this signing operation.
    pub fn attestNativeFkReceipt(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, input: anytype, expected: @import("antfly_local_sources").storage_db_relational_integrity_generation_admission.AppliedReceipt, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !@TypeOf(input) {
        const supplied_group = if (comptime @hasField(@TypeOf(input), "parent_group_id")) input.parent_group_id else input.child_group_id;
        if (supplied_group != group) return error.IntegrityTopologyChanged;
        const normalized = try platformDeadlineContext(context);
        var captured = try self.loadDescriptorWithDeadline(alloc, group, table, normalized.deadline_ns);
        defer captured.deinit(alloc);
        var lease = try self.acquireControlWithControls(group, table, .{ .allow_deferred_catalog = true, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        defer lease.deinit();
        const proof = try self.nativeProofForDescriptor(captured.view(), .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        return attestNativeFkReceiptOnLease(alloc, &lease, proof, input, expected, root, normalized);
    }

    fn attestNativeFkReceiptOnLease(alloc: std.mem.Allocator, lease: *Lease, proof: NativeGroupProof, input: anytype, expected: @import("antfly_local_sources").storage_db_relational_integrity_generation_admission.AppliedReceipt, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, normalized: request_operation.RequestContext) !@TypeOf(input) {
        const contract = @import("relational_fk_generation_publication.zig");
        const admission = @import("antfly_local_sources").storage_db_relational_integrity_generation_admission;
        const table = lease.entry.table_name;
        const descriptor = lease.entry.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        const actual_binding = try lease.owner().nativeRelationBinding(alloc, table);
        const actual_pin = try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(actual_binding);
        if (proof.signing.native_pin == null or !proof.signing.native_pin.?.eql(actual_pin) or !actual_binding.descriptor.eql(descriptor) or !std.mem.eql(u8, &actual_binding.native_instance, &proof.boot.instance)) return error.SqlRelationOwnerChanged;
        const encoded = try table_reads.encodeStorageKernelLookupRequest(alloc, "", NativeFkRead.publication.options(normalized));
        defer alloc.free(encoded);
        var response = try lease.owner().lookupJson(table, encoded);
        defer response.deinit();
        var status = try std.json.parseFromSlice(admission.OwnerStatus, alloc, response.bytes(), .{});
        defer status.deinit();
        const actual = if (comptime @hasField(@TypeOf(input), "parent_group_id")) switch (input.action) {
            .stage => status.value.staged_receipt,
            .activate => status.value.activation_receipt,
            .acknowledge => status.value.acknowledged_receipt,
            .cancel => status.value.cancel_receipt,
        } else switch (input.action) {
            .fence => status.value.source_fence_receipt,
            .install => status.value.source_install_receipt,
            .cancel => status.value.source_cancel_receipt,
        };
        const observed = actual orelse return error.GenerationAdmissionPending;
        if (!std.meta.eql(expected, observed) or observed.native_position == null or !std.meta.eql(input.native_position, observed.native_position) or input.applied_term != 0 or input.applied_index != 0) return error.GenerationAdmissionChanged;
        try (try observed.position()).requireNamespace(.{ .table_id = lease.entry.identity.table_id, .shard_id = lease.entry.identity.shard_id, .range_id = lease.entry.identity.range_id });
        var result = input;
        result.native_descriptor_fingerprint = try descriptor.fingerprint();
        result.native_boot = .{ .generation = proof.boot.generation, .instance = proof.boot.instance };
        const template: contract.NativeProof = .{ .identity = proof.signing.identity, .reporter_incarnation = proof.signing.reporter_incarnation, .authority = .{ .native = proof.signing.binding_digest }, .native_pin = proof.signing.native_pin, .root_generation = try indexRootEpoch(lease.entry.generation), .signature = @splat(0) };
        result.native_proof = try template.sign(root, result.digest());
        try contract.requireCapturedPosition(result, descriptor, .{ .table_id = lease.entry.identity.table_id, .shard_id = lease.entry.identity.shard_id, .range_id = lease.entry.identity.range_id });
        return result;
    }

    pub const NativeRetirementScope = struct {
        lease: Lease,
        proof: NativeGroupProof,
        plan_id: [16]u8,
        fence: @import("antfly_local_sources").storage_db_relational_integrity_topology_contract.Fence,

        pub fn deinit(self: *@This()) void {
            self.lease.deinit();
        }
        pub fn lookup(self: *@This(), a: std.mem.Allocator, kind: NativeFkRead, context: request_operation.RequestContext) !?table_read_source.LookupResponse {
            const normalized = try platformDeadlineContext(context);
            if (kind == .retirement_status) {
                try normalized.ensureActive();
                // This private process-local read is not a public lookup option.
                // The receiver rejoins this exact native image and stored fence
                // before deriving its internal retirement-debt authority.
                var response = try self.lease.owner().nativeRetirementStatusJson(a, self.lease.entry.table_name, .{
                    .fence = self.fence,
                    .descriptor = self.proof.initialized.descriptor,
                    .pin = try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(self.proof.initialized),
                    .native_instance = self.proof.boot.instance,
                });
                defer response.deinit();
                try normalized.ensureActive();
                // Fixed metadata has no row version or primary row digest.
                return .{ .json = try a.dupe(u8, response.bytes()), .version = 0 };
            }
            const body = try table_reads.encodeStorageKernelLookupRequest(a, "", kind.options(normalized));
            defer a.free(body);
            var response = try self.lease.owner().lookupJson(self.lease.entry.table_name, body);
            defer response.deinit();
            return .{ .json = try a.dupe(u8, response.bytes()), .version = response.version(), .expected_content_digest = response.expectedContentDigest() };
        }
        pub fn apply(self: *@This(), a: std.mem.Allocator, request: db_types.BatchRequest, context: request_operation.RequestContext) !void {
            try context.ensureActive();
            const topology = @import("antfly_local_sources").storage_db_relational_integrity_topology_contract;
            try topology.validateSqlDropBatch(request);
            const command = request.relational_topology orelse return error.InvalidBatchRequest;
            if (command.action != .begin or !command.sql_drop_control or !command.fence.eql(self.fence)) return error.GenerationAdmissionChanged;
            const body = try table_writes.encodeStorageKernelBatchRequest(a, request);
            defer a.free(body);
            var response = try self.lease.owner().nativeFkGenerationControlJson(self.lease.entry.table_name, body);
            defer response.deinit();
        }
        pub fn attest(self: *@This(), a: std.mem.Allocator, input: @import("relational_fk_generation_publication.zig").SourceReceipt, expected: @import("antfly_local_sources").storage_db_relational_integrity_generation_admission.AppliedReceipt, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, context: request_operation.RequestContext) !@import("relational_fk_generation_publication.zig").SourceReceipt {
            if (input.action != .fence or !std.mem.eql(u8, &input.plan_id, &self.plan_id) or input.child_group_id != self.lease.entry.group_id or input.child_table_id != self.lease.entry.identity.table_id) return error.GenerationAdmissionChanged;
            return attestNativeFkReceiptOnLease(a, &self.lease, self.proof, input, expected, root, try platformDeadlineContext(context));
        }
    };

    fn requireNativeRetirementSelection(group: u64, table: []const u8, request: @import("relational_fk_generation_publication.zig").SourceRequest, decision: @import("../metadata/fk_generation_publication.zig").SourceDecision) !@import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor {
        try request.validate(group);
        const selected = decision.child_before.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        try selected.validate();
        if (!decision.sql_drop_control or decision.phase != .fencing_child or request.action != .fence or decision.metadata_group_id == 0 or
            !@import("antfly_local_sources").metadata_incarnation.isValid(decision.metadata_incarnation) or decision.metadata_epoch == 0 or
            std.mem.allEqual(u8, &decision.plan_digest, 0) or request.child_table_id != decision.child_before.table_id or
            decision.child_range.group_id != group or decision.fence.owner_group_id != group or
            decision.fence.transition_id != std.mem.readInt(u64, request.plan_id[0..8], .little) or decision.fence.attempt != std.mem.readInt(u64, request.plan_id[8..16], .little) or
            !std.mem.eql(u8, table, request.child_table_name) or !std.mem.eql(u8, table, decision.child_before.name) or !decision.fence.namespace.eql(.{ .table_id = request.child_table_id, .shard_id = @import("../metadata/table_manager.zig").rangeDocIdentityShardId(decision.child_range), .range_id = @import("../metadata/table_manager.zig").rangeDocIdentityRangeId(decision.child_range) })) return error.GenerationAdmissionChanged;
        switch (decision.fence.role) {
            .document_retirement_source, .child_generation_source, .child_generation_dual => {},
            else => return error.GenerationAdmissionChanged,
        }
        return selected;
    }

    /// The caller has just fetched this exact immutable job through the
    /// authenticated source-decision RPC. Capture Boot/Pin BEFORE leasing an
    /// owner, then hold that same existing image through all fixed controls.
    pub fn openNativeRetirementScope(self: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, group: u64, table: []const u8, request: @import("relational_fk_generation_publication.zig").SourceRequest, decision: @import("../metadata/fk_generation_publication.zig").SourceDecision, context: request_operation.RequestContext) !NativeRetirementScope {
        const selected = try requireNativeRetirementSelection(group, table, request, decision);
        const normalized = try platformDeadlineContext(context);
        var captured = try self.loadDescriptorWithDeadline(a, group, table, normalized.deadline_ns);
        defer captured.deinit(a);
        if (captured.sql_persistence == null or !captured.sql_persistence.?.eql(selected) or
            captured.identity.table_id != decision.fence.namespace.table_id or captured.identity.shard_id != decision.fence.namespace.shard_id or captured.identity.range_id != decision.fence.namespace.range_id) return error.SqlRelationOwnerChanged;
        const observed = try self.catalog.sqlRelationBinding(a, .{ .node = selected.owner_node_id, .binding_table = captured.identity.table_id }, normalized.deadline_ns);
        const boot = observed.record orelse return error.SqlRelationOwnerUnavailable;
        try boot.validate();
        if (!observed.registration_current or (boot.phase != .ready and boot.phase != .draining)) return error.SqlRelationOwnerChanged;
        const initialized = observed.initialized orelse return error.SqlRelationRecoveryIncomplete;
        try initialized.verify();
        const binding = initialized.binding;
        const pin = try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(binding);
        if (!binding.descriptor.eql(selected) or binding.table_id != captured.identity.table_id or binding.shard_id != captured.identity.shard_id or binding.range_id != captured.identity.range_id or
            initialized.metadata_group_id != decision.metadata_group_id or
            !std.mem.eql(u8, &initialized.identity.metadata_incarnation, &decision.metadata_incarnation) or boot.identity.store_id != selected.owner_store_id or
            !std.mem.eql(u8, &binding.native_instance, &boot.instance) or !std.mem.eql(u8, &boot.instance, &try self.nativeExecutionInstance()) or
            initialized.boot_generation != boot.generation or initialized.reporter_incarnation != boot.reporter_incarnation or !std.mem.eql(u8, &try initialized.identity.digest(), &try boot.identity.digest())) return error.SqlRelationOwnerChanged;
        lock(&self.mutex);
        const host = self.sql_persistence_host;
        const ready = self.sql_persistence_ready;
        const draining = self.sql_persistence_draining;
        self.mutex.unlock();
        if ((!ready and boot.phase == .ready) or (!draining and boot.phase == .draining)) return error.SqlRelationOwnerChanged;
        var actual = host orelse return error.SqlRelationOwnerChanged;
        actual.generation_instance = selected.generation_instance;
        actual.data_generation = selected.data_generation;
        actual.connection = null;
        if (!std.mem.eql(u8, &actual.execution_instance, &boot.instance)) return error.SqlRelationOwnerChanged;
        try selected.requireHost(actual);
        const signing: @import("../metadata/relational_index_publication.zig").NativeSigningView = .{ .identity = boot.identity, .reporter_incarnation = boot.reporter_incarnation, .native_pin = pin, .binding_digest = try @import("../metadata/sql_relation_attestation.zig").nativeAuthorityDigest(boot.identity, pin) };
        try signing.validate(boot.identity.metadata_incarnation);
        var lease = if (boot.phase == .draining)
            try self.acquireExistingNativeRetirement(group, table, captured.view())
        else
            try self.acquireDescriptorWithMode(group, table, captured.path, captured.view(), false, .resident, .{ .native_initialized = binding, .native_boot_observed = true, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        errdefer lease.deinit();
        const physical = try lease.owner().nativeRelationBinding(a, table);
        if (!pin.eql(try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(physical)) or
            !std.mem.eql(u8, &physical.native_instance, &boot.instance)) return error.SqlRelationOwnerChanged;
        return .{ .lease = lease, .proof = .{ .boot = boot, .initialized = binding, .signing = signing, .metadata_group_id = initialized.metadata_group_id }, .plan_id = request.plan_id, .fence = decision.fence };
    }

    fn acquireExistingNativeRetirement(self: *ProvisionedKernelOwnerSource, group: u64, table: []const u8, descriptor: descriptor_contract.Descriptor) !Lease {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        for (self.entries.items) |entry| {
            if (entry.group_id != group or !std.mem.eql(u8, entry.table_name, table)) continue;
            if (!exactApplyDescriptor(entry, descriptor) or entry.closing) return error.SqlRelationOwnerUnavailable;
            if (entry.retired) {
                const reason = entry.close_failure orelse return error.SqlRelationOwnerUnavailable;
                if (reason != @as(anyerror, error.SqlRelationRecoveryIncomplete)) return error.SqlRelationOwnerUnavailable;
            }
            if (!tryReserveEntryLeaseLocked(entry, .shared)) return error.StorageBusy;
            return .{ .source = self, .entry = entry };
        }
        // Drain never opens, recreates, un-retires or marks an image clean.
        return error.SqlRelationOwnerUnavailable;
    }

    pub fn applyFkGenerationNative(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !void {
        const command = req.relational_topology orelse return error.InvalidBatchRequest;
        if (command.fence.owner_group_id != group_id) return error.IntegrityTopologyChanged;
        var lease = try self.acquireControlWithControls(group_id, table_name, .{ .allow_deferred_catalog = true });
        defer lease.deinit();
        if (lease.entry.identity.table_id != command.fence.namespace.table_id or
            lease.entry.identity.shard_id != command.fence.namespace.shard_id or
            lease.entry.identity.range_id != command.fence.namespace.range_id) return error.IntegrityTopologyChanged;
        const encoded = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(encoded);
        var response = try lease.owner().nativeFkGenerationControlJson(table_name, encoded);
        defer response.deinit();
    }

    pub fn applyInitialChildNative(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        expected: @import("antfly_local_sources").storage_db_relational_initial_child_publication.Bootstrap,
        req: db_types.BatchRequest,
        operation_index: u64,
    ) !void {
        try expected.validate();
        var lease = try self.acquirePreparedOwner(group_id, table_name);
        defer lease.deinit();
        if (lease.entry.initial_child_bootstrap_json.len == 0 or
            lease.entry.identity.table_id != expected.namespace.table_id or
            lease.entry.identity.shard_id != expected.namespace.shard_id or
            lease.entry.identity.range_id != expected.namespace.range_id)
            return error.InitialChildPublicationChanged;
        var stored = std.json.parseFromSlice(@TypeOf(expected), alloc, lease.entry.initial_child_bootstrap_json, .{ .ignore_unknown_fields = false }) catch return error.InvalidInitialChildPublication;
        defer stored.deinit();
        if (!stored.value.eql(expected)) return error.InitialChildPublicationChanged;
        const encoded = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(encoded);
        var response = try lease.owner().nativeInitialChildControlJson(table_name, encoded, operation_index);
        defer response.deinit();
    }

    pub fn restoreOwnerControl(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        input: @import("restore_owner.zig").Request,
        proposer: ?@import("restore_owner.zig").Proposer,
        options: RestoreOwnerOptions,
        request: @import("antfly_local_sources").api_operation.RequestContext,
    ) !@import("restore_owner.zig").Response {
        try request.ensureActive();
        try input.validate(group_id);
        if (descriptor.restore_bootstrap_json.len == 0) return error.RestoreStagingScopeChanged;
        if (descriptor.lsm_root_generation != self.visibleRootGeneration(group_id)) return error.StorageKernelOwnerTransitionRequired;
        var bootstrap = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_restore_staging_contract.OwnerBootstrap, alloc, descriptor.restore_bootstrap_json, .{});
        defer bootstrap.deinit();
        if (!std.mem.eql(u8, &bootstrap.value.scope.digest(), &input.scope.digest())) return error.RestoreStagingScopeChanged;
        if (input.action == .install_generation_admissions) {
            const expected = bootstrap.value.generation_admission orelse return error.RestoreSourceProofMissing;
            const command = input.generation_admissions.?;
            if (!std.mem.eql(u8, &expected.source_summary_digest, &command.source_summary_digest) or
                !std.mem.eql(u8, &expected.expected_receipt_digest, &try @import("antfly_local_sources").storage_db_restore_staging_contract.admissionReceiptDigest(command)))
                return error.RestoreStagingScopeChanged;
        }
        if (input.action == .install_native_empty) {
            const expected = bootstrap.value.native_empty_source orelse return error.RestoreSourceProofMissing;
            const command = input.native_empty_install.?;
            if (!std.meta.eql(expected.origin, command.origin) or
                !std.mem.eql(u8, &expected.expected_install_digest, &try command.digest())) return error.RestoreStagingScopeChanged;
        }
        var prepared = try self.prepareRestoreOwnerControl(alloc, group_id, table_name, descriptor, input, options, request);
        defer prepared.deinit();
        const encoded = prepared.value.batch_json orelse return prepared.value.response;
        var batch = try @import("antfly_local_sources").api_batch.parseInternalBatchRequest(alloc, encoded);
        defer batch.deinit(alloc);
        const scope = batch.req.restore_staging_scope orelse return error.RestoreStagingScopeChanged;
        if (!std.mem.eql(u8, &scope, &input.scope.digest()) or batch.req.restore_staging == null) return error.RestoreStagingScopeChanged;
        // No owner lease crosses the proposal callback: Raft apply must acquire
        // this same owner, and the returned receipt must reflect that commit.
        if (proposer) |replicated| {
            try replicated.submit(batch.req, request);
        } else {
            _ = try self.batchGroupLocalWithDescriptor(alloc, group_id, table_name, batch.req, descriptor);
        }
        const followup = if (input.action == .publish or input.action == .cancel or input.action == .install_generation_admissions or input.action == .install_native_empty) input else input.statusRead();
        var committed = try self.prepareRestoreOwnerControl(alloc, group_id, table_name, descriptor, followup, options, request);
        defer committed.deinit();
        if (committed.value.batch_json != null) return error.RestoreStagingScopeChanged;
        committed.value.response.source_next_offset = prepared.value.response.source_next_offset;
        return committed.value.response;
    }

    pub fn attestNativeRestoreAcceptance(self: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, group: u64, table: []const u8, descriptor: descriptor_contract.Descriptor, input: @import("restore_owner.zig").Request, expected: @import("antfly_local_sources").storage_db_native_empty_generation.Receipt, root: @import("antfly_local_sources").storage_db_root_signing_identity.State, options: RestoreOwnerOptions, context: request_operation.RequestContext) !@import("../metadata/native_restore_acceptance.zig").Acceptance {
        const contract = @import("../metadata/native_restore_acceptance.zig");
        const persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
        if (input.action != .install_native_empty or descriptor.sql_persistence == null) return error.InvalidRestoreStagingCommand;
        const normalized = try platformDeadlineContext(context);
        var controls: ReadControls = .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation };
        const captured = try self.nativeProofForDescriptor(descriptor, controls);
        const attempt = captured.attempt orelse return error.SqlRelationRecoveryIncomplete;
        // Reuse this exact fresh metadata cut for the retained owner open,
        // rather than issuing a second independent Boot/attempt RPC.
        controls.native_initialized = captured.initialized;
        controls.native_boot_observed = true;
        if (captured.boot.phase == .recovering) {
            controls.native_empty_admission = captured.empty_target orelse return error.SqlRelationRecoveryIncomplete;
            const native = descriptor.sql_persistence.?;
            controls.native_empty_host = .{ .node_id = native.owner_node_id, .store_id = native.owner_store_id, .execution_instance = captured.boot.instance, .generation_instance = native.generation_instance, .data_generation = native.data_generation };
        }
        const path = try std.fmt.allocPrint(a, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group });
        defer a.free(path);
        var lease = try self.acquireDescriptorWithMode(group, table, path, descriptor, false, .resident, controls);
        defer lease.deinit();
        // Preparing an existing install is a verified read. Missing effects
        // would produce a batch; signing never executes that batch or retries it.
        var actual = try prepareRestoreOwnerControlOnLease(a, &lease, table, input, options, normalized);
        defer actual.deinit();
        if (actual.value.batch_json != null) return error.RestoreValidationPending;
        const installed = actual.value.response.native_empty_commit orelse return error.RestoreValidationPending;
        if (!std.meta.eql(installed, expected)) return error.RestoreStagingScopeChanged;
        const binding = try lease.owner().nativeRelationBinding(a, table);
        if (binding.has_user_data or binding.has_transaction_state) return error.RestoreSourceProofMissing;
        const pin = try persistence.PhysicalPin.fromBinding(binding);
        if (captured.signing.native_pin == null or !captured.signing.native_pin.?.eql(pin) or
            !binding.descriptor.eql(descriptor.sql_persistence.?) or !std.mem.eql(u8, &binding.native_instance, &captured.boot.instance)) return error.SqlRelationOwnerChanged;
        var result: contract.Acceptance = .{
            .metadata_group_id = captured.metadata_group_id,
            .group_id = group,
            .range_id = descriptor.identity.range_id,
            .plan_digest = input.scope.plan_digest,
            .attempt = attempt,
            .initialized_digest = try captured.initialized.digest(),
            .descriptor_fingerprint = try binding.descriptor.fingerprint(),
            .boot_generation = captured.boot.generation,
            .native_instance = captured.boot.instance,
            .installed = installed,
            .proof = .{ .identity = captured.signing.identity, .reporter_incarnation = captured.signing.reporter_incarnation, .authority = .{ .native = captured.signing.binding_digest }, .native_pin = pin, .root_generation = try indexRootEpoch(lease.entry.generation), .signature = @splat(0) },
        };
        result.proof = try result.proof.sign(root, try result.digest());
        try result.requireLogical(group, input.scope.plan_digest, input.scope.digest(), input.native_empty_install.?, binding.descriptor, input.scope.target_namespace);
        try result.requireInitialized(captured.initialized, captured.boot.generation, captured.boot.instance, attempt);
        return result;
    }

    fn prepareRestoreOwnerControl(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
        input: @import("restore_owner.zig").Request,
        options: RestoreOwnerOptions,
        request: @import("antfly_local_sources").api_operation.RequestContext,
    ) !std.json.Parsed(@import("restore_owner.zig").Prepared) {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor, false, .resident, .{ .execution_deadline_ns = request.deadline_ns, .execution_io = request.deadline_io, .cancellation = request.cancellation });
        defer lease.deinit();
        return prepareRestoreOwnerControlOnLease(alloc, &lease, table_name, input, options, request);
    }

    fn prepareRestoreOwnerControlOnLease(alloc: std.mem.Allocator, lease: *Lease, table_name: []const u8, input: @import("restore_owner.zig").Request, options: RestoreOwnerOptions, request: request_operation.RequestContext) !std.json.Parsed(@import("restore_owner.zig").Prepared) {
        const json = try std.json.Stringify.valueAlloc(alloc, input, .{});
        defer alloc.free(json);
        var cancellation = request.cancellation;
        const native_context = try platformDeadlineContext(request);
        var result = try lease.owner().restoreControlJson(.{
            .control = .{
                .table_name = .fromSlice(table_name),
                .request_json = .fromSlice(json),
                .execution_deadline_ns = native_context.deadline_ns orelse 0,
                .has_execution_deadline = @intFromBool(native_context.deadline_ns != null),
                .cancellation_ctx = &cancellation,
                .cancellation_fn = cancellationTokenRequested,
            },
            .source_byte_budget = options.source_byte_budget,
            .secret_store = options.secret_store,
            .node_config = options.node_config,
        });
        defer result.deinit();
        return std.json.parseFromSlice(@import("restore_owner.zig").Prepared, alloc, result.bytes(), .{ .allocate = .alloc_always });
    }

    /// Lease only an already-resident owner whose complete catalog descriptor
    /// still matches. Observability uses this path so a cold status read never
    /// opens storage or discards query-warmed physical coverage.
    fn acquireIfPresent(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) !?Lease {
        var descriptor = try self.loadDescriptor(self.alloc, group_id, table_name);
        defer descriptor.deinit(self.alloc);
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.retired or
                entry.generation != descriptor.generation or
                !entry.identity.eql(descriptor.identity) or
                !std.mem.eql(u8, entry.schema_json, descriptor.schema_json) or
                !std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) or
                !descriptor_contract.initialRangesEqual(entry.initial_range, descriptor.initial_range) or
                !std.meta.eql(entry.sql_persistence, descriptor.sql_persistence) or
                !std.meta.eql(entry.table_storage, descriptor.table_storage))
            {
                return null;
            }
            return try self.borrowEntryLocked(entry);
        }
        return null;
    }

    /// The registry mutex and a validated descriptor pin this entry. Borrowing
    /// must neither adopt residency nor admit new work after transient cleanup.
    fn borrowEntryLocked(self: *ProvisionedKernelOwnerSource, entry: *Entry) !Lease {
        if (entry.retired or entry.closing or entry.transient_retirement_pending or !tryReserveEntryLeaseLocked(entry, .shared))
            return error.StorageReadTemporarilyUnavailable;
        _ = self.owner_cache_hits.fetchAdd(1, .monotonic);
        return .{ .source = self, .entry = entry };
    }

    const Residency = enum { transient, resident };

    fn acquireDescriptorExclusive(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        residency: Residency,
    ) !Lease {
        return try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor, true, residency, .{});
    }

    fn acquireDescriptorForReconcile(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        wait_for_readers: bool,
        residency: Residency,
    ) !?Lease {
        if (wait_for_readers) return try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor, true, residency, .{ .allow_deferred_catalog = true });
        // Periodic inspection must yield to admitted foreground/maintenance
        // leases. Queueing a writer here closes foreground admission while an
        // existing lease may itself be waiting for a long derived-index apply.
        // Return busy without installing that gate; the startup scheduler
        // retains the inspection debt and retries. Explicit structural changes
        // and admitted repair work keep their writer-preference contract.
        return self.acquireDescriptorOnce(group_id, table_name, path, descriptor, .exclusive_if_idle, residency, .{ .allow_deferred_catalog = true }) catch |err| switch (err) {
            error.StorageKernelOwnerTransitionRequired, error.StorageKernelOwnerStaleDescriptor => null,
            else => return err,
        };
    }

    pub const NativeGroupProof = struct {
        boot: @import("../metadata/sql_relation_boots.zig").Record,
        initialized: @import("antfly_local_sources").common_topology_records.RelationPersistence.NativeBinding,
        signing: @import("../metadata/relational_index_publication.zig").NativeSigningView,
        metadata_group_id: u64,
        attempt: ?@import("../metadata/native_restore_acceptance.zig").Attempt = null,
        empty_target: ?@import("../metadata/sql_relation_empty_target.zig").Admission = null,
    };

    fn initializedForDescriptor(self: *ProvisionedKernelOwnerSource, descriptor: descriptor_contract.Descriptor, controls: ReadControls) !@import("antfly_local_sources").common_topology_records.RelationPersistence.NativeBinding {
        return (try self.nativeProofForDescriptor(descriptor, controls)).initialized;
    }

    fn requireHiddenEmptyBoot(descriptor: descriptor_contract.Descriptor, scope: @import("antfly_local_sources").storage_db_restore_staging_contract.Scope, observed: @import("../metadata/sql_relation_boots.zig").Observation) !@import("../metadata/sql_relation_boots.zig").Record {
        const boot = observed.record orelse return error.SqlRelationOwnerUnavailable;
        try boot.validate();
        if (!observed.registration_current) return error.SqlRelationOwnerChanged;
        if (boot.phase == .ready) return observed.requireReady();
        if (boot.phase != .recovering) return error.SqlRelationRecoveryIncomplete;
        const native = descriptor.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        const empty = observed.empty_target orelse return error.SqlRelationRecoveryIncomplete;
        const initialized = observed.initialized orelse return error.SqlRelationRecoveryIncomplete;
        if (empty.metadata_group_id != initialized.metadata_group_id) return error.SqlRelationOwnerChanged;
        try empty.require(scope, native, .{ .table_id = descriptor.identity.table_id, .shard_id = descriptor.identity.shard_id, .range_id = descriptor.identity.range_id }, observed.attempt);
        // The remainder of nativeProofForDescriptor independently authenticates
        // the actual registered process/initialized image and retained Pin.
        return boot;
    }

    /// Recovery is availability only after authenticating the enrolled current
    /// process. A foreign/stale reporter or TEMP execution epoch stays fenced.
    fn requireCurrentNativeBoot(self: *ProvisionedKernelOwnerSource, native: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor, observed: @import("../metadata/sql_relation_boots.zig").Observation) !@import("../metadata/sql_relation_boots.zig").Record {
        try native.validate();
        const boot = observed.record orelse return error.SqlRelationOwnerUnavailable;
        try boot.validate();
        const actual = try self.nativeExecutionInstance();
        if (!observed.registration_current or boot.node_id != native.owner_node_id or boot.identity.store_id != native.owner_store_id or
            !std.mem.eql(u8, &boot.instance, &actual) or
            (native.kind == .temporary and !std.mem.eql(u8, &native.generation_instance, &actual))) return error.SqlRelationOwnerChanged;
        return boot;
    }

    fn nativeProofForDescriptor(self: *ProvisionedKernelOwnerSource, descriptor: descriptor_contract.Descriptor, controls: ReadControls) !NativeGroupProof {
        const native = descriptor.sql_persistence orelse return error.InvalidSqlRelationPersistence;
        var region = std.heap.ArenaAllocator.init(self.alloc);
        defer region.deinit();
        var creation_scope: ?@import("../metadata/sql_relation_attestation.zig").CreationScope = null;
        var restore_scope: ?@import("antfly_local_sources").storage_db_restore_staging_contract.Scope = null;
        if (descriptor.restore_bootstrap_json.len != 0) {
            const bootstrap = try std.json.parseFromSliceLeaky(@import("antfly_local_sources").storage_db_restore_staging_contract.OwnerBootstrap, region.allocator(), descriptor.restore_bootstrap_json, .{ .allocate = .alloc_always });
            restore_scope = bootstrap.scope;
            creation_scope = .{ .plan_id = bootstrap.scope.plan_id, .plan_digest = bootstrap.scope.plan_digest };
        }
        const observed = try self.catalog.sqlRelationBinding(self.alloc, .{ .node = native.owner_node_id, .binding_table = descriptor.identity.table_id, .creation_scope = creation_scope }, controls.execution_deadline_ns);
        const current_boot = try self.requireCurrentNativeBoot(native, observed);
        const boot = if (restore_scope != null and restore_scope.?.native_empty_origin != null)
            try requireHiddenEmptyBoot(descriptor, restore_scope.?, observed)
        else switch (current_boot.phase) {
            .ready => current_boot,
            .recovering => return error.SqlRelationRecoveryIncomplete,
            .draining, .clean => return error.SqlRelationOwnerChanged,
        };
        const signed = observed.initialized orelse return error.SqlRelationRecoveryIncomplete;
        try signed.verify();
        if (boot.identity.store_id != native.owner_store_id or !std.mem.eql(u8, &boot.instance, &signed.binding.native_instance) or !std.mem.eql(u8, &boot.instance, &try self.nativeExecutionInstance()) or
            !signed.binding.descriptor.eql(native) or signed.binding.table_id != descriptor.identity.table_id or
            signed.binding.shard_id != descriptor.identity.shard_id or signed.binding.range_id != descriptor.identity.range_id or
            (creation_scope != null and !std.meta.eql(signed.creation_scope, creation_scope)) or signed.boot_generation != boot.generation or signed.reporter_incarnation != boot.reporter_incarnation or
            !std.mem.eql(u8, &try signed.identity.digest(), &try boot.identity.digest())) return error.SqlRelationOwnerChanged;
        lock(&self.mutex);
        const host = self.sql_persistence_host;
        const ready = self.sql_persistence_ready;
        self.mutex.unlock();
        const recovering_empty = boot.phase == .recovering and restore_scope != null and restore_scope.?.native_empty_origin != null;
        if (!ready and !recovering_empty) return error.SqlRelationRecoveryIncomplete;
        var actual = if (recovering_empty)
            @import("antfly_local_sources").common_topology_records.RelationPersistence.Context{ .node_id = boot.identity.node_id, .store_id = boot.identity.store_id, .execution_instance = boot.instance, .generation_instance = native.generation_instance, .data_generation = native.data_generation }
        else
            host orelse return error.SqlRelationOwnerChanged;
        actual.generation_instance = native.generation_instance;
        actual.data_generation = native.data_generation;
        actual.connection = null;
        try native.requireHost(actual);
        const pin = try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(signed.binding);
        const signing: @import("../metadata/relational_index_publication.zig").NativeSigningView = .{ .identity = boot.identity, .reporter_incarnation = boot.reporter_incarnation, .native_pin = pin, .binding_digest = try @import("../metadata/sql_relation_attestation.zig").nativeAuthorityDigest(boot.identity, pin) };
        try signing.validate(boot.identity.metadata_incarnation);
        const attempt: ?@import("../metadata/native_restore_acceptance.zig").Attempt = if (creation_scope != null) captured: {
            const current = observed.attempt orelse return error.SqlRelationRecoveryIncomplete;
            break :captured .{ .number = current.number, .digest = current.digest };
        } else null;
        return .{ .boot = boot, .initialized = signed.binding, .signing = signing, .metadata_group_id = signed.metadata_group_id, .attempt = attempt, .empty_target = if (recovering_empty) observed.empty_target else null };
    }

    /// Per-group execution selection for private metadata control. This does
    /// not grant a SQL principal access to rows: ordinary user reads/writes
    /// still enter the routed Owner admission path.
    /// Elect only the actual current local process for restartable native
    /// work. Recovering/draining election grants no open or mutation rights;
    /// retirement receivers must still verify their immutable job authority.
    pub fn mayCoordinateNativeMaintenance(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, selection: @import("group_work_ownership.zig").NativeSelection, local_node: u64, local_store: u64, context: request_operation.RequestContext) !bool {
        try context.ensureActive();
        const normalized = try platformDeadlineContext(context);
        const native = selection.descriptor;
        try native.validate();
        lock(&self.mutex);
        const host = self.sql_persistence_host;
        const serving = self.sql_persistence_ready;
        self.mutex.unlock();
        if (local_node != native.owner_node_id or local_store != native.owner_store_id) return false;
        const actual_instance = try self.nativeExecutionInstance();
        const observed = try self.catalog.sqlRelationBinding(alloc, .{ .node = native.owner_node_id, .binding_table = selection.table_id }, normalized.deadline_ns);
        const current = observed.record orelse return false;
        try current.validate();
        if (!observed.registration_current or current.identity.node_id != local_node or current.identity.store_id != local_store or
            !std.mem.eql(u8, &current.instance, &actual_instance)) return false;
        if (selection.purpose == .retirement) return current.phase == .ready or current.phase == .draining or current.phase == .recovering;
        if (current.phase != .ready or !serving) return false;
        var actual_host = host orelse return false;
        if (!std.mem.eql(u8, &actual_host.execution_instance, &actual_instance)) return false;
        actual_host.generation_instance = native.generation_instance;
        actual_host.data_generation = native.data_generation;
        try native.requireHost(actual_host);
        const initialized = observed.initialized orelse return false;
        try initialized.verify();
        return initialized.binding.descriptor.eql(native) and initialized.binding.table_id == selection.table_id and
            std.mem.eql(u8, &initialized.binding.native_instance, &actual_instance) and initialized.boot_generation == current.generation and
            initialized.reporter_incarnation == current.reporter_incarnation and
            std.mem.eql(u8, &try initialized.identity.digest(), &try current.identity.digest());
    }

    pub fn nativeGroupAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, context: request_operation.RequestContext) !bool {
        return (try self.nativeGroupProofAtRoot(alloc, group, table, context)) != null;
    }

    pub fn nativeGroupProofAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, context: request_operation.RequestContext) !?NativeGroupProof {
        try context.ensureActive();
        const normalized = try platformDeadlineContext(context);
        var descriptor = try self.loadDescriptorWithDeadline(alloc, group, table, normalized.deadline_ns);
        defer descriptor.deinit(alloc);
        if (descriptor.sql_persistence == null) return null;
        return try self.nativeProofForDescriptor(descriptor.view(), .{ .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
    }

    fn tryAcquireServingCatalogOwner(self: *ProvisionedKernelOwnerSource, group: u64, table: []const u8, path: []const u8, descriptor: descriptor_contract.Descriptor, context: request_operation.RequestContext) !?Lease {
        const controls = try self.prepareDescriptorControls(descriptor, .{
            .allow_deferred_catalog = true,
            .execution_deadline_ns = context.deadline_ns,
            .cancellation = context.cancellation,
        });
        return self.acquireDescriptorOnce(group, table, path, descriptor, .exclusive_if_idle, .resident, controls) catch |err| switch (err) {
            error.StorageKernelOwnerTransitionRequired, error.StorageKernelOwnerStaleDescriptor => null,
            else => return err,
        };
    }

    fn prepareDescriptorControls(self: *ProvisionedKernelOwnerSource, descriptor: descriptor_contract.Descriptor, input: ReadControls) !ReadControls {
        var controls = input;
        try controls.check();
        // Private schema/index/restore controls already hold their metadata
        // purpose. They still need the same initialized-image proof as user
        // opens. Fetch it before entering the owner registry mutex.
        if (descriptor.sql_persistence != null and controls.native_initialized == null and !controls.native_initialization and controls.native_adoption == null) {
            const proof = try self.nativeProofForDescriptor(descriptor, controls);
            controls.native_initialized = proof.initialized;
            controls.native_boot_observed = true;
            if (proof.boot.phase == .recovering) {
                controls.native_empty_admission = proof.empty_target orelse return error.SqlRelationRecoveryIncomplete;
                const native = descriptor.sql_persistence.?;
                controls.native_empty_host = .{ .node_id = native.owner_node_id, .store_id = native.owner_store_id, .execution_instance = proof.boot.instance, .generation_instance = native.generation_instance, .data_generation = native.data_generation };
            }
        }
        return controls;
    }

    fn acquireDescriptorWithMode(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        exclusive: bool,
        residency: Residency,
        input: ReadControls,
    ) !Lease {
        const controls = try self.prepareDescriptorControls(descriptor, input);
        var lease = self.acquireDescriptorOnce(group_id, table_name, path, descriptor, if (exclusive) .exclusive else .shared, residency, controls) catch |err| switch (err) {
            error.StorageKernelOwnerStaleDescriptor => return err,
            error.StorageKernelOwnerTransitionRequired => try self.acquireDescriptorAfterTransition(
                group_id,
                table_name,
                path,
                descriptor,
                exclusive,
                residency,
                controls,
            ),
            else => return err,
        };
        errdefer lease.deinit();
        if (lease.entry.catalog_deferred and !controls.allow_deferred_catalog and !controls.historical_raft_apply)
            return error.StorageReadTemporarilyUnavailable;
        try controls.check();
        return lease;
    }

    fn acquireDescriptorAfterTransition(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        exclusive: bool,
        residency: Residency,
        controls: ReadControls,
    ) !Lease {
        errdefer if (exclusive) self.clearExclusivePending(group_id, table_name);
        var wait_io_impl = std.Io.Threaded.init(self.alloc, .{});
        defer wait_io_impl.deinit();
        const wait_io = wait_io_impl.io();
        const deadline_ns = platform_time.monotonicNs() +| 5 * std.time.ns_per_s;
        while (true) {
            try controls.check();
            try wait_io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
            try controls.check();
            return self.acquireDescriptorOnce(group_id, table_name, path, descriptor, if (exclusive) .exclusive else .shared, residency, controls) catch |err| switch (err) {
                error.StorageKernelOwnerStaleDescriptor => return err,
                error.StorageKernelOwnerTransitionRequired => {
                    try controls.check();
                    if (platform_time.monotonicNs() >= deadline_ns) return error.StorageBusy;
                    continue;
                },
                else => return err,
            };
        }
    }

    fn clearExclusivePending(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
    ) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!entry.exclusive_active) entry.exclusive_pending = false;
        }
    }

    fn tryReserveEntryLeaseLocked(entry: *Entry, admission: LeaseAdmission) bool {
        const exclusive = admission != .shared;
        if (entry.catalog_drain_pending or entry.exclusive_active or (admission != .exclusive and entry.exclusive_pending)) return false;
        if (exclusive and entry.active_users != 0) {
            if (admission == .exclusive) entry.exclusive_pending = true;
            return false;
        }
        entry.active_users += 1;
        if (exclusive) {
            entry.exclusive_pending = false;
            entry.exclusive_active = true;
        }
        return true;
    }

    fn exactApplyDescriptor(entry: *const Entry, descriptor: descriptor_contract.Descriptor) bool {
        const restore_matches = restoreBindingMatches(entry.restore, descriptor.restore, false);
        return entry.generation == descriptor.lsm_root_generation and
            entry.identity.eql(descriptor.identity) and
            restore_matches and
            std.mem.eql(u8, entry.schema_json, descriptor.schema_json) and
            std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) and
            std.mem.eql(u8, entry.restore_bootstrap_json, descriptor.restore_bootstrap_json) and
            std.mem.eql(u8, entry.initial_child_bootstrap_json, descriptor.initial_child_bootstrap_json) and
            descriptor_contract.initialRangesEqual(entry.initial_range, descriptor.initial_range) and
            entry.restore_cancel_recovery == descriptor.restore_cancel_recovery and
            entry.restore_ha_replay == descriptor.restore_ha_replay and
            std.meta.eql(entry.sql_persistence, descriptor.sql_persistence) and
            std.meta.eql(entry.table_storage, descriptor.table_storage);
    }

    fn restoreBindingMatches(
        admitted: ?@import("../storage/restore_identity.zig").Identity,
        expected: ?@import("../storage/restore_identity.zig").Identity,
        allow_inherited_binding: bool,
    ) bool {
        if (expected) |identity| return if (admitted) |actual| actual.eql(identity) else false;
        return allow_inherited_binding or admitted == null;
    }

    /// An apply callback is invoked under the data-Raft mutex. It can only
    /// borrow an already-open exact owner; all cold opens, stale retirement,
    /// and recovery-worker joins belong to the independent control executor.
    fn acquireApplyOnly(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
    ) !Lease {
        // Native-local owners are never data-Raft participants, including
        // warm owners that happen to share this registry.
        if (descriptor.sql_persistence != null) return error.SqlRelationOwnerChanged;
        if (!self.mutex.tryLock()) return error.RaftApplyWriterUnavailable;
        const initial = self.tryAcquireWarmApplyLocked(group_id, table_name, descriptor);
        const may_queue = !self.quiescing and !self.apply_control_stopping and
            !self.publicationPendingLocked(group_id, table_name) and self.apply_control_io != null and
            !self.applyRequestPendingLocked(group_id, table_name) and
            self.apply_control_pending.items.len + @as(usize, @intFromBool(self.apply_control_active != null)) < apply_control_max_pending;
        self.mutex.unlock();
        if (initial) |lease| return lease;
        if (!may_queue) return error.RaftApplyWriterUnavailable;
        // A committed descriptor may carry large schema and index JSON. Clone
        // it outside the owner registry mutex, then recheck the exact binding
        // before enqueueing; another control worker may have opened it meanwhile.
        const pending = ApplyOpen.init(self.alloc, self.replica_root_dir, group_id, table_name, descriptor) catch
            return error.RaftApplyWriterUnavailable;
        var pending_owned = true;
        defer if (pending_owned) pending.deinit(self.alloc);
        if (!self.mutex.tryLock()) return error.RaftApplyWriterUnavailable;
        defer self.mutex.unlock();
        if (self.quiescing or self.apply_control_stopping or
            self.publicationPendingLocked(group_id, table_name)) return error.RaftApplyWriterUnavailable;
        if (self.tryAcquireWarmApplyLocked(group_id, table_name, descriptor)) |lease| return lease;
        if (self.apply_control_io == null or self.applyRequestPendingLocked(group_id, table_name) or
            self.apply_control_pending.items.len + @as(usize, @intFromBool(self.apply_control_active != null)) >= apply_control_max_pending)
            return error.RaftApplyWriterUnavailable;
        self.apply_control_pending.appendAssumeCapacity(pending);
        pending_owned = false;
        self.apply_control_wake.set(self.apply_control_io.?);
        return error.RaftApplyWriterUnavailable;
    }

    fn tryAcquireWarmApplyLocked(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        descriptor: descriptor_contract.Descriptor,
    ) ?Lease {
        if (self.quiescing or self.apply_control_stopping or
            self.publicationPendingLocked(group_id, table_name)) return null;
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (!entry.retired and !entry.closing and exactApplyDescriptor(entry, descriptor) and
                tryReserveEntryLeaseLocked(entry, .shared))
            {
                entry.resident = true;
                entry.transient_retirement_pending = false;
                _ = self.owner_cache_hits.fetchAdd(1, .monotonic);
                return .{ .source = self, .entry = entry, .apply_only = true };
            }
        }
        return null;
    }

    fn applyRequestPendingLocked(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) bool {
        // The committed log is ordered per group. Keep one exact descriptor
        // request for that group; later entries retry after it is installed.
        if (self.apply_control_active) |active| {
            if (active.group_id == group_id and std.mem.eql(u8, active.table_name, table_name)) return true;
        }
        for (self.apply_control_pending.items) |pending| {
            if (pending.group_id == group_id and std.mem.eql(u8, pending.table_name, table_name)) return true;
        }
        return false;
    }

    fn acquireDescriptorOnce(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        path: []const u8,
        descriptor: descriptor_contract.Descriptor,
        admission: LeaseAdmission,
        residency: Residency,
        controls: ReadControls,
    ) !Lease {
        if (descriptor.sql_persistence != null and controls.historical_raft_apply) return error.InvalidSqlRelationPersistence;
        const exclusive = admission != .shared;
        if (!self.mutex.tryLock()) return error.StorageKernelOwnerTransitionRequired;
        defer self.mutex.unlock();
        if (self.quiescing) return error.Canceled;
        if (descriptor.sql_persistence) |native| if (controls.native_adoption == null and controls.native_empty_host == null and controls.native_uninitialized == null and !self.sql_persistence_ready and
            !(self.sql_persistence_draining and (controls.native_decision_owner != null or controls.native_lock_cleanup)))
        {
            if (self.sql_persistence_draining) return error.SqlRelationOwnerChanged;
            var actual = self.sql_persistence_host orelse return error.SqlRelationOwnerUnavailable;
            actual.generation_instance = native.generation_instance;
            actual.data_generation = native.data_generation;
            try native.requireHost(actual);
            return error.SqlRelationRecoveryIncomplete;
        };
        // Return to the caller rather than waiting with a descriptor captured
        // before publication; a retry must acquire the new catalog descriptor.
        if (self.publicationPendingLocked(group_id, table_name)) return error.StorageReadTemporarilyUnavailable;
        var stale_index: ?usize = null;
        for (self.entries.items, 0..) |entry, index| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            // A cached owner opened before restore intent must drain as well.
            // Compare the admitted binding in memory; warm hits need no marker I/O.
            // Ordinary descriptor acquisition may inherit an admitted restore
            // binding. Historical committed apply must open the exact binding
            // or apply-only retry can never borrow what the worker marked ready.
            const restore_matches = restoreBindingMatches(entry.restore, descriptor.restore, !controls.historical_raft_apply);
            // Ordinary callers must refresh an older descriptor before they
            // can disturb the current owner, even when that owner is idle.
            // Committed Raft entries are different: historical apply opens
            // the entry's schema without downgrading the durable catalog.
            if (!controls.historical_raft_apply and entry.identity.eql(descriptor.identity) and restore_matches and
                !std.mem.eql(u8, entry.schema_json, descriptor.schema_json) and
                schemaVersionRegresses(entry.schema_json, descriptor.schema_json))
            {
                return error.StorageKernelOwnerStaleDescriptor;
            }
            if (entry.closing) return error.StorageKernelOwnerTransitionRequired;
            if (entry.close_failure_reported and entry.close_retry_at_ns > platform_time.monotonicNs()) return error.SqlRelationOwnerUnavailable;
            if (entry.retired or entry.generation != descriptor.lsm_root_generation or !entry.identity.eql(descriptor.identity) or !restore_matches) {
                entry.retired = true;
                if (entry.active_users == 0) {
                    stale_index = index;
                    break;
                }
                return error.StorageKernelOwnerTransitionRequired;
            }
            if (entry.catalog_deferred and !entry.opened_for_historical_apply and !controls.allow_deferred_catalog and !controls.historical_raft_apply)
                return error.StorageReadTemporarilyUnavailable;
            const control_binding_changed = !std.mem.eql(u8, entry.restore_bootstrap_json, descriptor.restore_bootstrap_json) or
                !std.meta.eql(entry.sql_persistence, descriptor.sql_persistence) or
                !std.mem.eql(u8, entry.initial_child_bootstrap_json, descriptor.initial_child_bootstrap_json) or
                entry.restore_cancel_recovery != descriptor.restore_cancel_recovery or
                entry.restore_ha_replay != descriptor.restore_ha_replay;
            if ((!controls.historical_raft_apply and entry.opened_for_historical_apply) or
                (controls.require_historical_owner and !entry.opened_for_historical_apply) or
                (!controls.durable_catalog and (!std.mem.eql(u8, entry.schema_json, descriptor.schema_json) or
                    !std.mem.eql(u8, entry.indexes_json, descriptor.indexes_json) or
                    !std.meta.eql(entry.table_storage, descriptor.table_storage) or
                    !descriptor_contract.initialRangesEqual(entry.initial_range, descriptor.initial_range))) or control_binding_changed)
            {
                // Catalog definition changes do not necessarily publish a new
                // physical root generation. An API lease is not the only DB
                // activity: index reconciliation may have handed durable work
                // to the owner's background runtime before releasing its
                // lease. Retire the idle owner so close drains that work, then
                // reopen with the new exact descriptor. Live configure here
                // would race the old descriptor's DB-owned maintenance.
                if (entry.active_users != 0) {
                    // Close admission without revoking the current physical
                    // root: admitted readers may still need to sign their cut.
                    // Retire at the last release, then reopen from a fresh
                    // descriptor. Periodic inspection never starts this drain.
                    if (admission != .exclusive_if_idle and
                        (controls.historical_raft_apply or !schemaVersionRegresses(entry.schema_json, descriptor.schema_json)))
                    {
                        if (control_binding_changed) entry.retired = true else entry.catalog_drain_pending = true;
                    }
                    return error.StorageKernelOwnerTransitionRequired;
                }
                entry.retired = true;
                stale_index = index;
                break;
            }
            if (!tryReserveEntryLeaseLocked(entry, admission)) {
                const Trace = struct {
                    var reported: std.atomic.Value(bool) = .init(false);
                };
                if (@import("builtin").mode == .debug and admission == .shared and entry.sql_persistence != null and !Trace.reported.swap(true, .monotonic))
                    std.debug.print("native shared lease blocked group={} users={} drain={} retired={} exclusive_pending={} exclusive_active={}\n", .{ group_id, entry.active_users, entry.catalog_drain_pending, entry.retired, entry.exclusive_pending, entry.exclusive_active });
                return error.StorageKernelOwnerTransitionRequired;
            }
            if (residency == .resident) {
                entry.resident = true;
                entry.transient_retirement_pending = false;
            }
            _ = self.owner_cache_hits.fetchAdd(1, .monotonic);
            return .{ .source = self, .entry = entry, .exclusive = exclusive };
        }
        if (stale_index) |index| {
            const before_close = self.entries.items.len;
            self.destroyEntryAtIndexLocked(index);
            if (self.entries.items.len >= before_close) return error.SqlRelationOwnerUnavailable;
            // Closing releases the registry mutex. Recheck the descriptor on
            // retry in case another caller installed its replacement.
            return error.StorageKernelOwnerTransitionRequired;
        }

        try self.entries.ensureUnusedCapacity(self.alloc, 1);
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        const owned_schema_json = try self.alloc.dupe(u8, descriptor.schema_json);
        errdefer self.alloc.free(owned_schema_json);
        const owned_indexes_json = try self.alloc.dupe(u8, descriptor.indexes_json);
        errdefer self.alloc.free(owned_indexes_json);
        const owned_restore_bootstrap_json = try self.alloc.dupe(u8, descriptor.restore_bootstrap_json);
        errdefer self.alloc.free(owned_restore_bootstrap_json);
        const owned_initial_child_bootstrap_json = try self.alloc.dupe(u8, descriptor.initial_child_bootstrap_json);
        errdefer self.alloc.free(owned_initial_child_bootstrap_json);
        const owned_initial_range = try descriptor_contract.cloneInitialRange(self.alloc, descriptor.initial_range);
        errdefer descriptor_contract.freeInitialRange(self.alloc, owned_initial_range);
        var owned_restore = if (descriptor.restore) |identity| try identity.clone(self.alloc) else null;
        errdefer if (owned_restore) |*identity| identity.deinit(self.alloc);
        const entry = try self.alloc.create(Entry);
        errdefer self.alloc.destroy(entry);
        try self.ensureContextConfigured();
        var cancellation = controls.cancellation orelse db_types.CancellationToken.none;
        const native_context = try platformDeadlineContext(.{
            .deadline_ns = controls.execution_deadline_ns,
            .deadline_io = controls.execution_io,
            .cancellation = cancellation,
        });
        var catalog_deferred: u8 = 0;
        var lifetime_json: []const u8 = "";
        defer if (lifetime_json.len != 0) self.alloc.free(lifetime_json);
        var lifetime_context_json: []const u8 = "";
        defer if (lifetime_context_json.len != 0) self.alloc.free(lifetime_context_json);
        var lifetime_boot_json: []const u8 = "";
        defer if (lifetime_boot_json.len != 0) self.alloc.free(lifetime_boot_json);
        if (descriptor.sql_persistence) |lifetime| {
            if (controls.historical_raft_apply or controls.durable_catalog) return error.InvalidSqlRelationPersistence;
            if (controls.native_empty_host) |recovering| {
                if (controls.native_adoption != null or controls.native_initialization or !controls.native_boot_observed or controls.native_initialized == null or
                    controls.native_empty_admission == null or descriptor.restore_bootstrap_json.len == 0) return error.InvalidSqlRelationPersistence;
                var bootstrap = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_restore_staging_contract.OwnerBootstrap, self.alloc, descriptor.restore_bootstrap_json, .{});
                defer bootstrap.deinit();
                const empty = controls.native_empty_admission.?;
                try empty.require(bootstrap.value.scope, lifetime, .{ .table_id = descriptor.identity.table_id, .shard_id = descriptor.identity.shard_id, .range_id = descriptor.identity.range_id }, @as(?@import("../metadata/sql_relation_empty_target.zig").Attempt, empty.attempt));
                if (!std.mem.eql(u8, &recovering.execution_instance, &controls.native_initialized.?.native_instance)) return error.SqlRelationOwnerChanged;
            }
            if (controls.native_uninitialized != null and (!controls.native_initialization or controls.native_adoption != null or controls.native_empty_host != null)) return error.InvalidSqlRelationPersistence;
            var host = if (controls.native_uninitialized) |initial| initial.host else if (controls.native_empty_host) |reset| reset else if (controls.native_adoption) |recovery| recovery.host else self.sql_persistence_host orelse return error.SqlRelationOwnerChanged;
            var boot = if (controls.native_uninitialized) |initial| initial.boot else if (controls.native_empty_host) |reset| @import("antfly_local_sources").common_topology_records.RelationPersistence.Boot{ .instance = reset.execution_instance, .unclean_restart = true, .initializing = true } else if (controls.native_adoption) |recovery| recovery.boot else self.sql_persistence_boot orelse return error.SqlRelationOwnerChanged;
            boot.initialized = controls.native_initialized;
            boot.initializing = controls.native_initialization;
            if (boot.initialized) |binding| if (!binding.descriptor.eql(lifetime) or binding.table_id != descriptor.identity.table_id or
                binding.shard_id != descriptor.identity.shard_id or binding.range_id != descriptor.identity.range_id) return error.SqlRelationOwnerChanged;
            host.connection = null;
            host.generation_instance = lifetime.generation_instance;
            host.data_generation = lifetime.data_generation;
            lifetime.requireHost(host) catch |err| {
                if (err == error.SqlRelationOwnerChanged) return self.nativeOwnerFailure(.host_descriptor, group_id, descriptor.identity.table_id, if (comptime @import("builtin").mode == .debug) nativeHostMismatch(lifetime, host) else 0, @errorReturnTrace());
                return err;
            };
            try boot.validate();
            const actual_instance = try self.nativeExecutionInstance();
            if (!std.mem.eql(u8, &boot.instance, &host.execution_instance) or
                !std.mem.eql(u8, &actual_instance, &host.execution_instance)) return error.SqlRelationOwnerChanged;
            lifetime_json = try std.json.Stringify.valueAlloc(self.alloc, lifetime, .{});
            lifetime_context_json = try std.json.Stringify.valueAlloc(self.alloc, host, .{});
            lifetime_boot_json = try std.json.Stringify.valueAlloc(self.alloc, boot, .{});
        }
        var owner = client.Owner.open(.{
            .context = self.context.handle,
            .path = abi.BorrowedBytes.fromSlice(path),
            .table_name = abi.BorrowedBytes.fromSlice(table_name),
            .group_id = group_id,
            .lsm_root_generation = descriptor.lsm_root_generation,
            .has_identity_namespace = 1,
            .identity_table_id = descriptor.identity.table_id,
            .identity_shard_id = descriptor.identity.shard_id,
            .identity_range_id = descriptor.identity.range_id,
            .schema_json = .fromSlice(if (controls.native_adoption != null) "" else descriptor.schema_json),
            .indexes_json = .fromSlice(if (controls.native_adoption != null) "" else descriptor.indexes_json),
            .sql_persistence_json = .fromSlice(lifetime_json),
            .sql_persistence_context_json = .fromSlice(lifetime_context_json),
            .persistence_boot_json = .fromSlice(lifetime_boot_json),
            .restore_bootstrap_json = .fromSlice(if (controls.native_adoption != null) "" else descriptor.restore_bootstrap_json),
            .initial_child_bootstrap_json = .fromSlice(if (controls.native_adoption != null) "" else descriptor.initial_child_bootstrap_json),
            .restore_cancel_recovery = @intFromBool(controls.native_adoption == null and descriptor.restore_cancel_recovery),
            .restore_ha_replay = @intFromBool(controls.native_adoption == null and descriptor.restore_ha_replay),
            .online_source_authority = if (descriptor.sql_persistence != null) 2 else @backingInt(self.online_source_authority),
            .sql_preparation_secret = .fromSlice(self.sql_preparation_secret orelse ""),
            .sql_preparation_issuer = .fromSlice(self.sql_preparation_issuer orelse ""),
            .row_policy_authority_secret = .fromSlice(self.row_policy_authority_secret orelse ""),
            .row_policy_authority_issuer = .fromSlice(self.row_policy_authority_issuer orelse ""),
            .historical_raft_apply = @intFromBool(controls.historical_raft_apply),
            .catalog_mode = if (controls.native_adoption != null) .lifetime_recovery else if (controls.durable_catalog) .durable_only else .descriptor,
            .owner_catalog_deferred_out = &catalog_deferred,
            .dense_embedding_storage = if (controls.native_adoption != null) .persisted else if (descriptor.table_storage) |settings| switch (settings.dense_embeddings) {
                .primary_lsm => .primary_lsm,
                .vector_store => .vector_store,
            } else .persisted,
            .target_observer = if (self.runtime_status_cache) |cache| .{
                .ctx = cache,
                .notify = targetAdvanced,
            } else .{},
            .transaction_recovery = self.transactionRecoveryConfig(),
            .runtime_hooks = self.runtimeHooksConfig(),
            .has_initial_range = @intFromBool(controls.native_adoption == null and descriptor.initial_range != null),
            .initial_range_start = .fromSlice(if (controls.native_adoption != null) "" else if (descriptor.initial_range) |range| range.start else ""),
            .initial_range_end = .fromSlice(if (controls.native_adoption != null) "" else if (descriptor.initial_range) |range| range.end else ""),
            .initial_range_control = .{
                .execution_deadline_ns = native_context.deadline_ns orelse 0,
                .has_execution_deadline = @intFromBool(native_context.deadline_ns != null),
                .cancellation_ctx = &cancellation,
                .cancellation_fn = cancellationTokenRequested,
            },
            .restore = if (controls.native_adoption != null) .{} else if (descriptor.restore) |identity| .{
                .required = 1,
                .backup_id = .fromSlice(identity.backup_id),
                .location = .fromSlice(identity.location),
                .snapshot_path = .fromSlice(identity.snapshot_path),
                .artifact_sha256 = .fromSlice(identity.artifact_sha256),
                .native_manifest_size_bytes = identity.native_manifest_size_bytes,
                .native_manifest_sha256 = .fromSlice(identity.native_manifest_sha256),
            } else .{},
        }) catch |err| {
            return err;
        };
        errdefer owner.deinit();
        entry.* = .{
            .group_id = group_id,
            .table_name = owned_table_name,
            .generation = descriptor.lsm_root_generation,
            .identity = descriptor.identity,
            .schema_json = owned_schema_json,
            .indexes_json = owned_indexes_json,
            .restore_bootstrap_json = owned_restore_bootstrap_json,
            .initial_child_bootstrap_json = owned_initial_child_bootstrap_json,
            .initial_range = owned_initial_range,
            .restore_cancel_recovery = descriptor.restore_cancel_recovery,
            .restore_ha_replay = descriptor.restore_ha_replay,
            .table_storage = descriptor.table_storage,
            .sql_persistence = descriptor.sql_persistence,
            .restore = owned_restore,
            .owner = owner,
            .opened_for_historical_apply = controls.historical_raft_apply,
            .catalog_deferred = catalog_deferred != 0,
            .active_users = 1,
            .resident = residency == .resident,
            .exclusive_pending = false,
            .exclusive_active = exclusive,
        };
        self.entries.appendAssumeCapacity(entry);
        _ = self.owner_cache_misses.fetchAdd(1, .monotonic);
        return .{ .source = self, .entry = entry, .exclusive = exclusive };
    }

    fn prepareQueryRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.read_safety_barrier);
        reads.prepareSearchWithConsistency(group_id, req, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareSearchWithConsistency(group_id, req, .stale),
            else => return err,
        };
    }

    fn prepareLookupRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.read_safety_barrier);
        reads.prepareLookupWithConsistency(group_id, key, opts, consistency) catch |err| switch (err) {
            // Read-index null is an authoritative absence proof. Never
            // manufacture one by downgrading a failed leader read to stale.
            error.NotLeader => if (consistency != .leader_lease)
                return err
            else
                return try reads.prepareLookupWithConsistency(group_id, key, opts, .stale),
            else => return err,
        };
    }

    fn prepareScanRead(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !void {
        const reads = feature_reads.FeatureReads.init(self.read_safety_barrier);
        reads.prepareScanWithConsistency(group_id, from_key, to_key, opts, consistency) catch |err| switch (err) {
            error.NotLeader => if (consistency == .stale)
                return err
            else
                return try reads.prepareScanWithConsistency(group_id, from_key, to_key, opts, .stale),
            else => return err,
        };
    }

    fn prepareGraphExpandRead(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !void {
        for (req.frontier) |item| {
            const search_req = try distributed_graph.frontierItemToSearchRequest(alloc, req, item);
            defer distributed_graph.freeExpandSearchRequest(alloc, search_req);
            try self.prepareQueryRead(group_id, search_req, consistency);
        }
    }

    fn executeQuery(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        raw_search_result: bool,
        route: ?metadata_api.CatalogRouteFence,
    ) !client.QueryResponse {
        try table_reads.checkQueryDeadline(req);
        // Routed exact-ID hydration reaches this physical boundary before
        // encoding private document authority. A coordinator may leave the
        // owner unspecified, but an already scoped request cannot change it.
        const scoped = try table_reads.bindDocumentAuthorityToGroup(req, group_id);
        if (!nativeReadRoute(route)) try self.prepareQueryRead(group_id, scoped, consistency);
        const request_json = try @import("antfly_local_sources").api_local_query_contract.encodeStorageKernelQueryRequestForExecution(alloc, scoped, raw_search_result);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.from(scoped), route));
        defer lease.deinit();
        try table_reads.checkQueryDeadline(req);
        var cancellation = req.cancellation;
        var execution = @import("antfly_local_sources").storage_local_query_controls.executionOptions(scoped);
        execution.raw_search_result = @intFromBool(raw_search_result);
        var response = try lease.owner().queryJsonWithOptions(table_name, request_json, .{
            .execution_deadline_ns = req.execution_deadline_ns,
            .cancellation_ctx = if (cancellation != null) @ptrCast(&cancellation.?) else null,
            .cancellation_fn = if (cancellation != null) cancellationTokenRequested else null,
            .execution = execution,
        });
        errdefer response.deinit();
        try table_reads.checkQueryDeadline(req);
        return response;
    }

    fn unsupportedTopLevelLookup(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: db_types.LookupOptions,
        _: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelScan(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: []const u8,
        _: []const u8,
        _: db_types.ScanOptions,
        _: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelQuery(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_types.SearchRequest,
        _: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn unsupportedTopLevelBatch(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: []const u8,
        _: db_types.BatchRequest,
    ) !?void {
        return error.UnsupportedStorageKernelTopLevelOperation;
    }

    fn validateNativeRoute(self: *ProvisionedKernelOwnerSource, route: metadata_api.CatalogRouteFence) !void {
        if (route.route.execution != .native_local) return self.nativeOwnerFailure(.route_kind, route.route.group_id, route.table_id, 0, null);
        const descriptor = route.route.execution.native_local;
        lock(&self.mutex);
        const ready = self.sql_persistence_ready;
        const draining = self.sql_persistence_draining;
        const host = self.sql_persistence_host;
        self.mutex.unlock();
        // No host has been admitted during early startup: its absence proves
        // availability debt, not a changed owner. It grants no read capability.
        // A known host is checked below before interpreting its readiness gate.
        var context = host orelse {
            try descriptor.requireConnectionOwner(route.admission_sql_relation_owner);
            if (!ready and !draining) return error.SqlRelationOwnerUnavailable;
            return self.nativeOwnerFailure(.host_missing, route.route.group_id, route.table_id, 0, null);
        };
        context.generation_instance = descriptor.generation_instance;
        context.data_generation = descriptor.data_generation;
        context.connection = route.admission_sql_relation_owner;
        descriptor.requireOwner(context) catch |err| {
            if (err == error.SqlRelationOwnerChanged) return self.nativeOwnerFailure(.host_descriptor, route.route.group_id, route.table_id, if (comptime @import("builtin").mode == .debug) nativeHostMismatch(descriptor, context) else 0, @errorReturnTrace());
            return err;
        };
        if (!ready) {
            if (draining) return self.nativeOwnerFailure(.host_not_ready, route.route.group_id, route.table_id, 0, null);
            return error.SqlRelationRecoveryIncomplete;
        }
    }

    fn validateNativeDecisionHost(self: *ProvisionedKernelOwnerSource, route: metadata_api.CatalogRouteFence, owner: @import("antfly_local_sources").storage_transactions.ExternalDecisionOwner) !void {
        try owner.validate();
        if (route.route.execution != .native_local) return error.SqlRelationOwnerChanged;
        const descriptor = route.route.execution.native_local;
        lock(&self.mutex);
        const admitted = self.sql_persistence_ready or self.sql_persistence_draining;
        const host = self.sql_persistence_host;
        self.mutex.unlock();
        if (!admitted) return error.SqlRelationOwnerChanged;
        var context = host orelse return error.SqlRelationOwnerChanged;
        context.generation_instance = descriptor.generation_instance;
        context.data_generation = descriptor.data_generation;
        context.connection = route.admission_sql_relation_owner;
        try descriptor.requireHost(context);
    }

    fn validateCurrentNativeBoot(self: *ProvisionedKernelOwnerSource, route: metadata_api.CatalogRouteFence) !void {
        if (route.route.execution != .native_local) return;
        const descriptor = route.route.execution.native_local;
        // A startup admission is not an ownership lease. Rejoin the current
        // metadata registration before native execution; a partitioned old
        // process must not keep serving after a successor has claimed Boot.
        const observed = try self.catalog.sqlRelationBoot(self.alloc, descriptor.owner_node_id, self.catalog.routeFenceDeadline(route));
        const current = try observed.requireReady();
        if (current.identity.store_id != descriptor.owner_store_id or
            !std.mem.eql(u8, &current.instance, &try self.nativeExecutionInstance()))
            return error.SqlRelationOwnerChanged;
    }

    const NativeControlAdmission = struct {
        draining: bool = false,
        route: metadata_api.CatalogRouteFence,
        initialized: @import("antfly_local_sources").common_topology_records.RelationPersistence.NativeBinding,
    };

    fn currentNativeInitialized(self: *ProvisionedKernelOwnerSource, route: metadata_api.CatalogRouteFence, allow_draining: bool) !@import("antfly_local_sources").common_topology_records.RelationPersistence.NativeBinding {
        const descriptor = route.route.execution.native_local;
        const observed = try self.catalog.sqlRelationBinding(self.alloc, .{ .node = descriptor.owner_node_id, .binding_table = route.table_id }, self.catalog.routeFenceDeadline(route));
        const boot = try self.requireCurrentNativeBoot(descriptor, observed);
        if (boot.phase != .ready and !(allow_draining and boot.phase == .draining)) {
            if (boot.phase == .recovering) return error.SqlRelationRecoveryIncomplete;
            return self.nativeOwnerFailure(.boot_binding, route.route.group_id, route.table_id, 2, null);
        }
        const initialized = observed.initialized orelse return error.SqlRelationRecoveryIncomplete;
        try initialized.verify();
        const binding = initialized.binding;
        if (!std.mem.eql(u8, &binding.native_instance, &boot.instance) or !binding.descriptor.eql(descriptor) or binding.table_id != route.table_id or
            binding.shard_id != route.route.identity_namespace.shard_id or binding.range_id != route.route.identity_namespace.range_id or
            initialized.metadata_group_id != route.metadata_group_id or initialized.boot_generation != boot.generation or
            initialized.reporter_incarnation != boot.reporter_incarnation or !std.mem.eql(u8, &try initialized.identity.digest(), &try boot.identity.digest()))
        {
            var mismatch: u64 = 0;
            if (comptime @import("builtin").mode == .debug) {
                // process, Descriptor, table, shard, range, metadata group,
                // boot generation, reporter incarnation, enrolled root/realm.
                const initialized_identity = initialized.identity.digest() catch @as([32]u8, @splat(0));
                const boot_identity = boot.identity.digest() catch @as([32]u8, @splat(0));
                mismatch = @as(u64, @intFromBool(!std.mem.eql(u8, &binding.native_instance, &boot.instance))) |
                    (@as(u64, @intFromBool(!binding.descriptor.eql(descriptor))) << 1) |
                    (@as(u64, @intFromBool(binding.table_id != route.table_id)) << 2) |
                    (@as(u64, @intFromBool(binding.shard_id != route.route.identity_namespace.shard_id)) << 3) |
                    (@as(u64, @intFromBool(binding.range_id != route.route.identity_namespace.range_id)) << 4) |
                    (@as(u64, @intFromBool(initialized.metadata_group_id != route.metadata_group_id)) << 5) |
                    (@as(u64, @intFromBool(initialized.boot_generation != boot.generation)) << 6) |
                    (@as(u64, @intFromBool(initialized.reporter_incarnation != boot.reporter_incarnation)) << 7) |
                    (@as(u64, @intFromBool(!std.mem.eql(u8, &initialized_identity, &boot_identity))) << 8);
            }
            return self.nativeOwnerFailure(.initialized_binding, route.route.group_id, route.table_id, mismatch, null);
        }
        return binding;
    }

    /// A durable participant follows an initialized image, never a fresh SQL
    /// name. Only phase two may rejoin its same root after a clean process
    /// restart; every new user vote retains its captured execution epoch.
    fn validateNativeDecisionRoute(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, route: metadata_api.CatalogRouteFence, group: u64, _: []const u8, owner: @import("antfly_local_sources").storage_transactions.ExternalDecisionOwner) !NativeControlAdmission {
        try route.validate();
        try owner.validate();
        if (route.route.group_id != group or route.route.execution != .native_local) return error.TopologyChanged;
        const pin = route.native_participant orelse return error.SqlRelationRecoveryIncomplete;
        try route.admission_cancellation.check();
        const original = route.route.execution.native_local;
        const observed = try self.catalog.sqlRelationBinding(alloc, .{ .node = original.owner_node_id, .binding_table = route.table_id }, self.catalog.routeFenceDeadline(route));
        const boot = observed.record orelse return error.SqlRelationOwnerUnavailable;
        try boot.validate();
        if (!observed.registration_current or (boot.phase != .ready and boot.phase != .draining)) return error.SqlRelationOwnerChanged;
        const proof = observed.initialized orelse return error.SqlRelationRecoveryIncomplete;
        try proof.verify();
        const binding = proof.binding;
        const current = binding.descriptor;
        const current_pin = try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(binding);
        if (!pin.eql(current_pin) or !current.eql(original) or
            current.owner_node_id != boot.node_id or current.owner_store_id != boot.identity.store_id or !std.mem.eql(u8, &binding.native_instance, &boot.instance) or !std.mem.eql(u8, &boot.instance, &try self.nativeExecutionInstance()) or
            proof.metadata_group_id != route.metadata_group_id or proof.boot_generation != boot.generation or proof.reporter_incarnation != boot.reporter_incarnation or
            !std.mem.eql(u8, &try proof.identity.digest(), &try boot.identity.digest())) return error.SqlRelationOwnerChanged;
        var admitted = route;
        admitted.route.execution = .{ .native_local = current };
        admitted.admission_relation_minter = null;
        admitted.admission_sql_relation_owner = null;
        try self.validateNativeDecisionHost(admitted, owner);
        return .{ .route = admitted, .initialized = binding, .draining = boot.phase == .draining };
    }

    fn nativeLostDecision(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, route: metadata_api.CatalogRouteFence, group: u64, table: []const u8, txn: [16]u8, owner: @import("../system_catalog/prepared_transactions.zig").Owner, context: request_operation.RequestContext) !?@import("../system_catalog/prepared_transactions.zig").Decision {
        if (route.route.group_id != group) return error.TopologyChanged;
        const recovery = @import("sql_native_recovery.zig");
        _ = (try recovery.retiredGeneration(alloc, self.catalog, route, context)) orelse return null;
        const source = self.native_terminal_authority orelse return error.CommitDecisionUnknown;
        return try source.observe_fn(source.ptr, alloc, table, route, txn, owner, context);
    }

    fn completeLostNativeControl(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, route: metadata_api.CatalogRouteFence, group: u64, table: []const u8, req: db_types.BatchRequest, context: request_operation.RequestContext) !bool {
        const owner = req.externalDecisionControlOwner() orelse return false;
        const mutation = req.transaction.?;
        const txn = switch (mutation) {
            .prepare => |value| value.txn_id,
            .resolve => |value| value.txn_id,
            .acknowledge => |value| value.txn_id,
            .acknowledge_many => |value| value.txn_id,
            else => return false,
        };
        const decision = (try self.nativeLostDecision(alloc, route, group, table, txn, owner, context)) orelse return false;
        switch (mutation) {
            .prepare => if (decision.status != .committed) return error.ExternalTransactionDecisionChanged,
            .resolve => |request| if (request.commit_version != decision.version or request.status != (if (decision.status == .committed) db_types.TxnStatus.committed else .aborted)) return error.ExternalTransactionDecisionChanged,
            .acknowledge, .acknowledge_many => {},
            else => return false,
        }
        return true;
    }

    fn nativeRecoveryAdmissionFailure(err: anyerror) bool {
        return err == error.SqlRelationOwnerChanged or err == error.SqlRelationRecoveryIncomplete or err == error.SqlRelationOwnerUnavailable;
    }

    fn nativeResolutionOwner(req: db_types.BatchRequest) ?@import("antfly_local_sources").storage_transactions.ExternalDecisionOwner {
        return req.externalDecisionControlOwner();
    }

    fn validateRoutedRead(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, fence: metadata_api.CatalogRouteFence, group_id: u64, table_name: []const u8) !void {
        try self.validateAdmittedRoutedRead(alloc, fence, group_id, table_name);
    }

    fn validateAdmittedRoutedRead(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
    ) !void {
        try fence.validate();
        if (fence.route.group_id != group_id) return error.TopologyChanged;
        try fence.admission_cancellation.check();
        if (fence.route.execution == .native_local) try self.validateNativeRoute(fence);
        try table_catalog.validateCatalogRouteFenceUntil(
            alloc,
            self.catalog,
            table_name,
            fence,
            self.catalog.routeFenceDeadline(fence),
        );
        try fence.admission_cancellation.check();
    }

    fn ownerLookupResultAlloc(a: std.mem.Allocator, response: anytype, opts: db_types.LookupOptions) !table_read_source.LookupResponse {
        if (opts.include_json_null_fields) {
            const decoded = try table_reads.StorageKernelTypedLookup.decode(a, response.bytes());
            return .{ .json = decoded.json, .version = response.version(), .expected_content_digest = response.expectedContentDigest(), .json_null_fields = decoded.json_null_fields };
        }
        return .{ .json = try a.dupe(u8, response.bytes()), .version = response.version(), .expected_content_digest = response.expectedContentDigest() };
    }

    fn lookupGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (opts.row_policy_receipt != null) try validateRowPolicyReceiptLookupOptions(key, opts);
        if (std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_identity\"}") and consistency != .read_index) return error.RestoreStagingScopeChanged;
        if (opts.restore_staging_scope != null) return self.lookupRestoreStaging(alloc, group_id, table_name, key, opts, fence);
        if (std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_identity\"}")) return error.RestoreStagingScopeChanged;
        if (fence.route.execution == .native_local and opts.sql_lock_query_json.len != 0) {
            try fence.validate();
            try fence.admission_cancellation.check();
            if (fence.route.group_id != group_id) return error.IdentityNamespaceMismatch;
            const private = try @import("native_sql_lock_read.zig").isPrivateControl(alloc, key, opts, fence);
            if (!private) try self.validateRoutedRead(alloc, fence, group_id, table_name);
            // Canonical metadata authorization, current Boot/Pin and the
            // sealed native participant are checked by this existing reader.
            return (try self.lookupNativeSqlLock(alloc, group_id, table_name, key, opts)) orelse error.SqlRelationOwnerChanged;
        }
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        if (fence.route.execution == .native_local) {
            const request_json = try table_reads.encodeStorageKernelLookupRequest(alloc, key, opts);
            defer alloc.free(request_json);
            var lease = try self.acquireWithControls(group_id, table_name, ReadControls.from(opts).withRoute(fence));
            defer lease.deinit();
            const admitted = lease.entry.sql_persistence orelse return error.SqlRelationOwnerChanged;
            if (!admitted.eql(fence.route.execution.native_local)) return error.SqlRelationOwnerChanged;
            var response = lease.owner().lookupJson(table_name, request_json) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer response.deinit();
            if (opts.include_primary_digest and !table_reads.integrityLookupMode(opts) and response.expectedContentDigest() == null) return error.InvalidResponse;
            return try ownerLookupResultAlloc(alloc, response, opts);
        }
        if (std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_install\"}")) {
            if (!publishedHandoffReceiptReadCertified(key, opts, consistency))
                return error.RestoreStagingScopeChanged;
            // Publication retires the hidden staging descriptor. Reopen the
            // current public owner under its authenticated catalog fence. The
            // wrapper completed the strict read-index barrier before holding
            // read admission; repeating it here could deadlock with apply.
            const encoded = try table_reads.encodeStorageKernelLookupRequest(alloc, key, opts);
            defer alloc.free(encoded);
            var lease = try self.acquireWithControls(group_id, table_name, ReadControls.from(opts).withRoute(fence));
            defer lease.deinit();
            var response = lease.owner().lookupJson(table_name, encoded) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer response.deinit();
            return try ownerLookupResultAlloc(alloc, response, opts);
        }
        // The route was authenticated above. A missing descriptor during
        // owner admission is a topology/availability race, never a row miss.
        return lookupGroupLocal(ptr, alloc, group_id, table_name, key, opts, consistency) catch |err| switch (err) {
            error.TableNotFound => error.StorageReadTemporarilyUnavailable,
            else => err,
        };
    }

    fn publishedHandoffReceiptReadCertified(key: []const u8, opts: db_types.LookupOptions, consistency: read_gate.ReadConsistency) bool {
        return key.len == 0 and opts.restore_staging_scope == null and opts.restore_staging_plan_id == null and
            std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_install\"}") and
            opts.generation_handoff_install_read_index_certified and consistency == .stale;
    }

    const RetainedRelationalRead = struct {
        alloc: std.mem.Allocator,
        lease: Lease,
        view: table_read_source.RelationalReadView,

        fn next(ptr: *anyopaque, alloc: std.mem.Allocator, limit: u32) !table_read_source.RelationalReadView.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.view.next(alloc, limit);
        }
        fn normalize(ptr: *anyopaque, alloc: std.mem.Allocator, writes: []const db_types.BatchWrite) ![]db_types.BatchWrite {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.view.normalize(alloc, writes);
        }
        fn rangeProofs(ptr: *anyopaque, alloc: std.mem.Allocator) ![]@import("antfly_local_sources").storage_range_protection.Proof {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.view.rangeProofs(alloc);
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.view.deinit();
            self.lease.deinit();
            self.alloc.destroy(self);
        }
    };

    const RetainedStatementFence = struct {
        alloc: std.mem.Allocator,
        source: *ProvisionedKernelOwnerSource,
        owner: Lease,
        native: table_read_source.StatementReadFence,
        route: metadata_api.CatalogRouteFence,
        group: u64,
        table: []const u8,

        frozen_proof: ?read_gate.ReadSafetyBarrier.FrozenProof = null,

        fn nativeSequence(ptr: *anyopaque) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try validateLocal(self);
            return self.native.nativeSequence();
        }
        pub fn validate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try validateLocal(self);
            if (self.frozen_proof) |proof| {
                if (self.route.route.execution == .native_local) {
                    try proof.requireNative(self.route.route.execution.native_local, try self.source.nativeExecutionInstance());
                    return;
                }
                const barrier = self.source.read_safety_barrier;
                const validate_proof = barrier.vtable.validate_frozen orelse return error.SqlStatementSnapshotRequired;
                try validate_proof(barrier.ptr, self.group, proof, self.route.admission_deadline_ns, self.route.admission_cancellation);
            }
        }
        fn validateLocal(self: *@This()) !void {
            try self.native.validate();
            try self.source.validateAdmittedRoutedRead(self.alloc, self.route, self.group, self.table);
        }
        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.native.deinit();
            self.owner.deinit();
            self.alloc.free(self.table);
            self.alloc.destroy(self);
        }
        fn open(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: db_types.ScanOptions) !table_read_source.RelationalReadView {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try validateLocal(self);
            var owner = self.owner.cloneRead();
            errdefer owner.deinit();
            const view = try self.native.open(alloc, from, to, opts);
            errdefer view.deinit();
            try validateLocal(self);
            const retained = try alloc.create(RetainedRelationalRead);
            retained.* = .{ .alloc = alloc, .lease = owner, .view = view };
            return .{ .text_filter_digest = view.text_filter_digest, .ptr = retained, .vtable = &.{ .next = RetainedRelationalRead.next, .close = RetainedRelationalRead.close, .normalize = RetainedRelationalRead.normalize, .range_proofs = RetainedRelationalRead.rangeProofs } };
        }

        fn captureSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
            return captureSnapshotWithGraphs(ptr, alloc, &.{});
        }
        fn captureSnapshotWithGraphs(ptr: *anyopaque, alloc: std.mem.Allocator, indexes: []const []const u8) !@import("antfly_local_sources").storage_statement_read_fence.Snapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const frozen_proof = self.frozen_proof orelse return error.SqlStatementSnapshotRequired;
            try validate(self);
            const native = try self.native.captureSnapshotWithGraphs(alloc, indexes);
            errdefer native.deinit();
            try validate(self);
            var owner = self.owner.cloneRead();
            errdefer owner.deinit();
            const name = try alloc.dupe(u8, self.table);
            errdefer alloc.free(name);
            // The short capture's cancellation/deadline borrows die when the
            // mutation fence is released. The immutable snapshot retains only
            // owner identity; each delayed cursor brings its own controls.
            var retained_route = self.route;
            retained_route.admission_cancellation = .none;
            retained_route.admission_deadline_ns = null;
            retained_route.admission_deadline_io = null;
            const snapshot = try alloc.create(RoutedStatementSnapshot);
            snapshot.* = .{ .alloc = alloc, .source = self.source, .owner = owner, .native = native, .route = retained_route, .group = self.group, .table = name, .cancellation = null, .deadline_ns = null, .frozen_proof = frozen_proof };
            return .{ .ptr = snapshot, .vtable = &.{ .open = RoutedStatementSnapshot.openNative, .graph_page = RoutedStatementSnapshot.graphPage, .release = RoutedStatementSnapshot.close } };
        }
    };

    const RoutedStatementSnapshot = struct {
        alloc: std.mem.Allocator,
        source: *ProvisionedKernelOwnerSource,
        owner: Lease,
        native: @import("antfly_local_sources").storage_statement_read_fence.Snapshot,
        route: metadata_api.CatalogRouteFence,
        group: u64,
        table: []u8,
        cancellation: ?@import("antfly_cancellation").CancellationToken,
        deadline_ns: ?u64,
        frozen_proof: read_gate.ReadSafetyBarrier.FrozenProof,

        fn check(self: *@This(), request: db_types.ScanOptions) !void {
            if (self.cancellation) |token| try token.check();
            const now = platform_time.monotonicNs();
            if (self.deadline_ns) |deadline| if (now >= deadline) return error.DeadlineExceeded;
            if (request.cancellation) |token| try token.check();
            const deadline = @min(request.execution_deadline_ns orelse std.math.maxInt(u64), now +| 5 * std.time.ns_per_s);
            var route = self.route;
            route.admission_deadline_ns = deadline;
            route.admission_cancellation = request.cancellation orelse .none;
            if (route.route.execution == .native_local) {
                try self.source.validateRoutedRead(self.alloc, route, self.group, self.table);
                try self.source.validateNativeRoute(route);
                try self.frozen_proof.requireNative(route.route.execution.native_local, try self.source.nativeExecutionInstance());
                return;
            }
            const certify = self.source.read_safety_barrier.vtable.certify_completed_prefix orelse return error.SqlStatementSnapshotRequired;
            var budget = self.source.catalog.budget(self.source.catalog.deadlineFrom(.{ .deadline_ns = deadline }));
            while (true) {
                if (request.cancellation) |token| try token.check();
                budget.checkpoint() catch {
                    if (request.execution_deadline_ns) |requested| if (platform_time.monotonicNs() >= requested) return error.DeadlineExceeded;
                    return error.ReadUnavailable;
                };
                // A historical root holds no mutation fence. Retry only pure
                // authority checks which could not obtain the Raft mutex or
                // a completed quorum prefix. Never replace the retained root,
                // replay a page, or retry a changed term/incarnation/topology.
                self.source.validateRoutedRead(self.alloc, route, self.group, self.table) catch |err| {
                    if (err != error.ReadUnavailable) return err;
                    budget.sleepNs(std.time.ns_per_ms) catch return error.ReadUnavailable;
                    continue;
                };
                const completed = certify(self.source.read_safety_barrier.ptr, self.group, self.frozen_proof.term, self.frozen_proof.applied_index, deadline, request.cancellation orelse .none) catch |err| {
                    if (err != error.ReadUnavailable) return err;
                    budget.sleepNs(std.time.ns_per_ms) catch return error.ReadUnavailable;
                    continue;
                };
                try self.frozen_proof.validateRetained(completed);
                return;
            }
        }
        fn open(ptr: *anyopaque, alloc: std.mem.Allocator, input: table_read_source.RelationalStatementScan) !table_read_source.RelationalReadView {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.check(input.opts);
            if (!std.mem.eql(u8, input.table, self.table)) return error.SqlStatementSnapshotRequired;
            var opts = input.opts;
            if (self.cancellation) |token| opts.cancellation = token;
            if (self.deadline_ns) |deadline| opts.execution_deadline_ns = if (opts.execution_deadline_ns) |prior| @min(prior, deadline) else deadline;
            var lease = self.owner.cloneRead();
            errdefer lease.deinit();
            const view = try self.native.open(alloc, input.from, input.to, opts);
            errdefer view.deinit();
            try self.check(input.opts);
            const retained = try alloc.create(RetainedRelationalRead);
            retained.* = .{ .alloc = alloc, .lease = lease, .view = view };
            return .{ .text_filter_digest = view.text_filter_digest, .ptr = retained, .vtable = &.{ .next = RetainedRelationalRead.next, .close = RetainedRelationalRead.close, .normalize = RetainedRelationalRead.normalize, .range_proofs = RetainedRelationalRead.rangeProofs } };
        }
        fn graphPage(ptr: *anyopaque, alloc: std.mem.Allocator, request: @import("antfly_local_sources").storage_statement_read_fence.GraphRead.Request) !@import("antfly_local_sources").storage_statement_read_fence.GraphRead.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const controls: db_types.ScanOptions = .{ .cancellation = request.cancellation, .execution_deadline_ns = request.execution_deadline_ns };
            try self.check(controls);
            var page = try self.native.graphPage(alloc, request);
            errdefer page.deinit(alloc);
            try self.check(controls);
            return page;
        }
        fn openNative(ptr: *anyopaque, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: db_types.ScanOptions) !table_read_source.RelationalReadView {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return open(ptr, alloc, .{ .table = self.table, .from = from, .to = to, .opts = opts });
        }
        fn openGuarded(ptr: *anyopaque, alloc: std.mem.Allocator, input: table_read_source.RelationalStatementScan) !table_read_source.RelationalStatementSnapshot.GuardedRead {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var guarded = input;
            guarded.opts.include_range_proofs = true;
            const view = try open(ptr, alloc, guarded);
            errdefer view.deinit();
            const proofs = try view.rangeProofs(alloc);
            errdefer alloc.free(proofs);
            if (proofs.len == 0) return error.SqlRangeTrackingRequired;
            const owners = try alloc.alloc(@import("antfly_local_sources").api_range_read_guards.OwnerRangeProof, 1);
            owners[0] = .{ .fence = self.route, .proofs = proofs };
            return .{ .view = view, .owner_proofs = owners };
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.native.deinit();
            self.owner.deinit();
            self.alloc.free(self.table);
            self.alloc.destroy(self);
        }
    };

    fn openStatementSnapshotRouted(ptr: *anyopaque, alloc: std.mem.Allocator, route: metadata_api.CatalogRouteFence, group: u64, table: []const u8, consistency: read_gate.ReadConsistency, cancellation: ?@import("antfly_cancellation").CancellationToken, deadline_ns: ?u64) !table_read_source.RelationalStatementSnapshot {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var scoped = route;
        if (cancellation) |token| scoped.admission_cancellation = token;
        if (deadline_ns) |deadline| scoped.admission_deadline_ns = if (scoped.admission_deadline_ns) |prior| @min(prior, deadline) else deadline;
        const opts: db_types.ScanOptions = .{ .cancellation = cancellation, .execution_deadline_ns = scoped.admission_deadline_ns };
        const fence = (try tryStatementReadFenceRouted(ptr, alloc, scoped, group, table, opts, consistency)) orelse return error.Backpressured;
        defer fence.deinit();
        const held: *RetainedStatementFence = @ptrCast(@alignCast(fence.ptr));
        const frozen_proof = held.frozen_proof orelse return error.SqlStatementSnapshotRequired;
        const native = try held.native.captureSnapshot(alloc);
        errdefer native.deinit();
        try RetainedStatementFence.validate(held);
        var owner = held.owner.cloneRead();
        errdefer owner.deinit();
        const name = try alloc.dupe(u8, table);
        errdefer alloc.free(name);
        const snapshot = try alloc.create(RoutedStatementSnapshot);
        snapshot.* = .{ .alloc = alloc, .source = self, .owner = owner, .native = native, .route = scoped, .group = group, .table = name, .cancellation = cancellation, .deadline_ns = scoped.admission_deadline_ns, .frozen_proof = frozen_proof };
        return .{ .ptr = snapshot, .vtable = &.{ .open = RoutedStatementSnapshot.open, .open_guarded = RoutedStatementSnapshot.openGuarded, .close = RoutedStatementSnapshot.close } };
    }

    fn tryStatementReadFenceRouted(ptr: *anyopaque, alloc: std.mem.Allocator, route: metadata_api.CatalogRouteFence, group: u64, table: []const u8, opts: db_types.ScanOptions, consistency: read_gate.ReadConsistency) !?table_read_source.StatementReadFence {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, route, group, table);
        if (route.route.execution == .native_local) try self.validateNativeRoute(route) else try self.prepareRetainedScanRead(group, "", "", opts, consistency);
        var owner = try self.acquireWithControls(group, table, ReadControls.from(opts).withRoute(route));
        errdefer owner.deinit();
        const provider = try @import("../storage/relational_read_provider.zig").acquire(owner.owner().handle);
        const native = (try provider.tryFence(alloc, table, opts)) orelse {
            owner.deinit();
            return null;
        };
        errdefer native.deinit();
        const retained = try alloc.create(RetainedStatementFence);
        errdefer alloc.destroy(retained);
        retained.* = .{ .alloc = alloc, .source = self, .owner = owner, .native = native, .route = route, .group = group, .table = try alloc.dupe(u8, table) };
        errdefer alloc.free(retained.table);
        if (route.route.execution == .native_local) {
            const descriptor = owner.entry.sql_persistence orelse return error.SqlRelationOwnerChanged;
            if (!descriptor.eql(route.route.execution.native_local)) return error.SqlRelationOwnerChanged;
            retained.frozen_proof = try read_gate.ReadSafetyBarrier.FrozenProof.fromNative(descriptor, try self.nativeExecutionInstance(), try native.nativeSequence());
        } else if (self.read_safety_barrier.vtable.capture_frozen) |capture_proof|
            retained.frozen_proof = try capture_proof(self.read_safety_barrier.ptr, group);
        try RetainedStatementFence.validateLocal(retained);
        return .{ .ptr = retained, .vtable = &.{ .validate = RetainedStatementFence.validate, .native_sequence = RetainedStatementFence.nativeSequence, .open = RetainedStatementFence.open, .capture_snapshot = RetainedStatementFence.captureSnapshot, .capture_snapshot_with_graphs = RetainedStatementFence.captureSnapshotWithGraphs, .release = RetainedStatementFence.release } };
    }

    fn prepareRetainedScanRead(self: *ProvisionedKernelOwnerSource, group: u64, from: []const u8, to: []const u8, opts: db_types.ScanOptions, consistency: read_gate.ReadConsistency) !void {
        // Retained SQL reads promise the requested statement snapshot. Leader
        // loss must be retried by routing, never weakened to a stale snapshot.
        try feature_reads.FeatureReads.init(self.read_safety_barrier).prepareScanWithConsistency(group, from, to, opts, consistency);
    }

    fn openRelationalReadRouted(ptr: *anyopaque, alloc: std.mem.Allocator, fence: metadata_api.CatalogRouteFence, group: u64, table: []const u8, from: []const u8, to: []const u8, opts: db_types.ScanOptions, consistency: read_gate.ReadConsistency) !?table_read_source.RelationalReadView {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group, table);
        if (fence.route.execution == .raft) try self.prepareRetainedScanRead(group, from, to, opts, consistency);
        var lease = try self.acquireWithControls(group, table, ReadControls.from(opts).withRoute(fence));
        errdefer lease.deinit();
        if (fence.route.execution == .native_local) {
            const descriptor = lease.entry.sql_persistence orelse return error.SqlRelationOwnerChanged;
            if (!descriptor.eql(fence.route.execution.native_local)) return error.SqlRelationOwnerChanged;
        }

        const provider = try @import("../storage/relational_read_provider.zig").acquire(lease.owner().handle);
        const view = try provider.open(alloc, table, from, to, opts);
        errdefer view.deinit();
        // Close the admission race before publishing the retained snapshot.
        // The physical owner lease then fences retirement for its lifetime.
        try self.validateRoutedRead(alloc, fence, group, table);
        const retained = try alloc.create(RetainedRelationalRead);
        retained.* = .{ .alloc = alloc, .lease = lease, .view = view };
        return .{ .text_filter_digest = view.text_filter_digest, .ptr = retained, .vtable = &.{ .next = RetainedRelationalRead.next, .close = RetainedRelationalRead.close, .normalize = RetainedRelationalRead.normalize, .range_proofs = RetainedRelationalRead.rangeProofs } };
    }

    fn scanGroupLocalRoutedStream(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
        sink: table_read_source.ScanStreamSink,
    ) !bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try scanGroupLocalStreamAdmitted(ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency, sink, fence);
    }

    fn scanGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try scanGroupLocalAdmitted(ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency, fence);
    }

    fn documentArtifactManifestGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifest {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try documentArtifactManifestGroupLocalAdmitted(ptr, alloc, group_id, table_name, doc_key, artifact_name, consistency, fence);
    }

    fn documentArtifactManifestsGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifestList {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try documentArtifactManifestsGroupLocalAdmitted(ptr, alloc, group_id, table_name, doc_key, consistency, fence);
    }

    fn preflightQueryGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
    ) !?runtime_preflight.RuntimePreflightSummary {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try preflightQueryGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, max_work, fence);
    }

    fn queryGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try queryGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, fence);
    }

    fn searchResultGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.SearchResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try searchResultGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, fence);
    }

    fn textStatsGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try textStatsGroupLocalAdmitted(ptr, alloc, group_id, table_name, body, fence);
    }

    fn algebraicPartialsGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try algebraicPartialsGroupLocalAdmitted(ptr, alloc, group_id, table_name, body, fence);
    }

    fn graphExpandGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try graphExpandGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, fence);
    }

    fn graphHydrateGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try graphHydrateGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, fence);
    }

    fn graphEdgesGroupLocalRouted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        fence: metadata_api.CatalogRouteFence,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.validateRoutedRead(alloc, fence, group_id, table_name);
        return try graphEdgesGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, fence);
    }

    fn fkGenerationSourceDeferredLookupAllowed(key: []const u8, opts: db_types.LookupOptions, consistency: read_gate.ReadConsistency) bool {
        if (!opts.fk_generation_source_control or !opts.fk_generation_source_read_index_certified or
            key.len != 0 or consistency != .stale or
            opts.relational_integrity_action or opts.relational_integrity_jobs_json.len != 0 or
            (opts.sql_lock_query_json.len != 0 or opts.group_source_query_json.len != 0) or opts.relational_index_status_json.len != 0 or
            opts.row_policy_receipt != null or opts.restore_staging_scope != null or
            opts.include_primary_digest or opts.fields.len != 0) return false;
        // Install completion must inspect durable reference coverage while
        // serving admission still waits for index/schema reconciliation. This
        // is the exact certified private status observation, never a row read.
        if (opts.relational_activation_json.len != 0)
            return !opts.relational_integrity_catalog and opts.relational_topology_json.len == 0 and
                std.mem.eql(u8, opts.relational_activation_json, "{\"mode\":\"status\"}");
        if (opts.relational_integrity_catalog)
            return opts.relational_topology_json.len == 0;
        return std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"public_schema\"}") or
            std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"status\"}");
    }

    fn validateRowPolicyReceiptLookupOptions(key: []const u8, opts: db_types.LookupOptions) !void {
        if (key.len != 0 or opts.row_policy_receipt == null) return error.InvalidRowPolicyReceipt;
        const defaults: db_types.LookupOptions = .{};
        inline for (@typeInfo(db_types.LookupOptions).@"struct".field_names, @typeInfo(db_types.LookupOptions).@"struct".field_types) |name, T| {
            if (comptime !std.mem.eql(u8, name, "row_policy_receipt") and !std.mem.eql(u8, name, "execution_deadline_ns") and !std.mem.eql(u8, name, "execution_io") and !std.mem.eql(u8, name, "cancellation")) {
                if (comptime @typeInfo(T) == .pointer and @typeInfo(T).pointer.size == .slice) {
                    if (@field(opts, name).len != 0) return error.InvalidRowPolicyReceipt;
                } else if (!std.meta.eql(@field(opts, name), @field(defaults, name))) return error.InvalidRowPolicyReceipt;
            }
        }
    }

    fn validateNativeLockLookupOptions(opts: db_types.LookupOptions) !void {
        try @import("native_sql_lock_read.zig").validateOptions(opts);
    }

    fn lookupNativeSqlLock(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, key: []const u8, opts: db_types.LookupOptions) !?table_read_source.LookupResponse {
        if (key.len != 0) return error.InvalidLockScope;
        try validateNativeLockLookupOptions(opts);
        var descriptor = try self.loadDescriptorWithBudget(alloc, group, table, ReadControls.from(opts).catalogBudget(self.catalog));
        defer descriptor.deinit(alloc);
        const native = descriptor.sql_persistence orelse return null;
        var query = try std.json.parseFromSlice(@import("antfly_local_sources").storage_pessimistic_lock_control.Query, alloc, opts.sql_lock_query_json, .{});
        defer query.deinit();
        try query.value.validate();
        if (query.value.admission.binding.namespace.group != group or (query.value.admission.binding.identity.table_id != descriptor.identity.table_id or query.value.admission.binding.identity.shard_id != descriptor.identity.shard_id or query.value.admission.binding.identity.range_id != descriptor.identity.range_id)) return error.IdentityNamespaceMismatch;
        var controls: ReadControls = undefined;
        if (query.value.native_control) |control| {
            try control.validate(query.value.admission);
            controls = try self.frozenNativeLockControls(alloc, group, table, control, false);
            if (!controls.native_route.?.route.execution.native_local.eql(native)) return error.SqlRelationOwnerChanged;
        } else {
            const command = try self.authorizeNativeLock(alloc, group, table, .{ .admission = query.value.admission, .retirement_control = query.value.retirement_control, .operation = .close }, .{ .deadline_ns = opts.execution_deadline_ns, .deadline_io = opts.execution_io, .cancellation = opts.cancellation orelse .none });
            const route = if (command.retirement_control) |retirement| retirement.route else (command.checked orelse return error.LockOwnerChanged).participant.spec.route;
            if (query.value.retirement_control != null) {
                query.value.admission = command.admission;
                query.value.retirement_control = command.retirement_control;
            }
            if (route.route.execution != .native_local or !route.route.execution.native_local.eql(native)) return error.SqlRelationOwnerChanged;
            const binding = try self.currentNativeInitialized(route, true);
            controls = ReadControls.from(opts);
            controls.native_lock_cleanup = true;
            controls.native_initialized = binding;
            controls.native_boot_observed = true;
        }
        var lease = if (controls.native_decision_owner != null) try self.acquireWithControls(group, table, controls) else (try self.acquireRetainedNativeDecisionDebt(group, table, descriptor.view())) orelse try self.acquireDescriptorWithMode(group, table, descriptor.path, descriptor.view(), false, .resident, controls);
        defer lease.deinit();
        var admitted_opts = opts;
        const canonical_query = try std.json.Stringify.valueAlloc(alloc, query.value, .{});
        defer alloc.free(canonical_query);
        admitted_opts.sql_lock_query_json = canonical_query;
        const body = try table_reads.encodeStorageKernelLookupRequest(alloc, key, admitted_opts);
        defer alloc.free(body);
        var result = try lease.owner().lookupJson(table, body);
        defer result.deinit();
        return .{ .json = try alloc.dupe(u8, result.bytes()), .version = result.version(), .expected_content_digest = result.expectedContentDigest() };
    }

    fn lookupGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        key: []const u8,
        opts: db_types.LookupOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.LookupResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (opts.row_policy_receipt != null) try validateRowPolicyReceiptLookupOptions(key, opts);
        if (opts.sql_lock_query_json.len != 0) {
            if (try self.lookupNativeSqlLock(alloc, group_id, table_name, key, opts)) |result| return result;
        }
        if (std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_identity\"}") and consistency != .read_index) return error.RestoreStagingScopeChanged;
        if (opts.restore_staging_scope != null) return self.lookupRestoreStaging(alloc, group_id, table_name, key, opts, null);
        // Unscoped handoff receipts belong exclusively to the published,
        // catalog-fenced route above, never a direct hidden-owner lookup.
        if (std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_install\"}") or
            std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_identity\"}"))
            return error.RestoreStagingScopeChanged;
        try self.prepareLookupRead(group_id, key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelLookupRequest(alloc, key, opts);
        defer alloc.free(request_json);
        var controls = ReadControls.from(opts);
        controls.durable_catalog = opts.row_policy_receipt != null;
        // Closed receipt reads observe the persisted Raft cut, just like the
        // other durable-only controls. The CABI requires historical authority
        // for this mode; prospective catalog schema hints remain blank.
        controls.historical_raft_apply = controls.durable_catalog and self.online_source_authority == .raft;
        controls.allow_deferred_catalog = opts.row_policy_receipt != null or std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"identity\"}") or
            std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_publication\"}") or
            fkGenerationSourceDeferredLookupAllowed(key, opts, consistency);
        try controls.check();
        var lease = if (std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"initial_child_preflight\"}") or
            std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"initial_child_publication\"}"))
            try self.acquirePreparedOwner(group_id, table_name)
        else
            try self.acquireWithControls(group_id, table_name, controls);
        defer lease.deinit();
        try controls.check();
        var response = lease.owner().lookupJson(table_name, request_json) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        if (opts.include_primary_digest and !table_reads.integrityLookupMode(opts) and response.expectedContentDigest() == null) return error.InvalidResponse;
        return try ownerLookupResultAlloc(alloc, response, opts);
    }

    fn lookupRestoreStaging(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, key: []const u8, opts: db_types.LookupOptions, fence: ?metadata_api.CatalogRouteFence) !?table_read_source.LookupResponse {
        const scope = opts.restore_staging_scope orelse return error.RestoreStagingScopeChanged;
        const handoff_identity = std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_identity\"}");
        const handoff_receipt = handoff_identity or std.mem.eql(u8, opts.relational_topology_json, "{\"mode\":\"generation_handoff_install\"}");
        if (handoff_identity and (fence != null or opts.include_primary_digest or opts.relational_integrity_catalog or opts.relational_integrity_action or
            opts.relational_integrity_jobs_json.len != 0 or (opts.sql_lock_query_json.len != 0 or opts.group_source_query_json.len != 0) or opts.relational_index_status_json.len != 0 or
            opts.relational_activation_json.len != 0 or opts.fields.len != 0 or opts.generation_handoff_install_read_index_certified)) return error.RestoreStagingScopeChanged;
        if (handoff_receipt and (opts.restore_staging_plan_id == null or key.len != 0)) return error.RestoreStagingScopeChanged;
        var descriptor = try self.resolveRestoreDescriptor(alloc, group_id, table_name, scope, opts.restore_staging_plan_id, if (handoff_receipt) .resolve else .read, .{ .deadline_ns = opts.execution_deadline_ns, .deadline_io = opts.execution_io, .cancellation = opts.cancellation orelse .none });
        defer descriptor.deinit(alloc);
        if (handoff_receipt) {
            var parsed = try std.json.parseFromSlice(@import("antfly_local_sources").storage_db_restore_staging_contract.OwnerBootstrap, alloc, descriptor.descriptor.restore_bootstrap_json, .{});
            defer parsed.deinit();
            try parsed.value.validate();
            if (parsed.value.empty_generation_handoff == null)
                return error.RestoreStagingScopeChanged;
        }
        const identity = descriptor.view().identity;
        if (identity.shard_id != group_id) return error.RestoreStagingScopeChanged;
        if (fence) |expected| {
            try expected.validate();
            try expected.admission_cancellation.check();
            if (expected.table_id != identity.table_id or expected.route.group_id != group_id or expected.route.range_id != identity.range_id or
                expected.route.identity_namespace.table_id != identity.table_id or expected.route.identity_namespace.shard_id != identity.shard_id or expected.route.identity_namespace.range_id != identity.range_id) return error.RestoreStagingScopeChanged;
        }
        // Hidden reads certify UNIQUE/FK activation. Never use the ordinary
        // observational NotLeader -> stale fallback for those proofs.
        try feature_reads.FeatureReads.init(self.read_safety_barrier).prepareLookupWithConsistency(group_id, key, opts, .read_index);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor.view(), false, .resident, ReadControls.from(opts));
        defer lease.deinit();
        const encoded = try table_reads.encodeStorageKernelLookupRequest(alloc, key, opts);
        defer alloc.free(encoded);
        var response = lease.owner().lookupJson(table_name, encoded) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        if (opts.include_primary_digest and !table_reads.integrityLookupMode(opts) and response.expectedContentDigest() == null) return error.InvalidResponse;
        return try ownerLookupResultAlloc(alloc, response, opts);
    }

    fn nativeReadRoute(route: ?metadata_api.CatalogRouteFence) bool {
        return if (route) |captured| captured.route.execution == .native_local else false;
    }

    fn controlsForReadRoute(input: ReadControls, route: ?metadata_api.CatalogRouteFence) ReadControls {
        var controls = input;
        if (route) |captured| if (captured.route.execution == .native_local) {
            controls.native_route = captured;
            if (controls.execution_deadline_ns == null) controls.execution_deadline_ns = captured.admission_deadline_ns;
            if (controls.cancellation == null) controls.cancellation = captured.admission_cancellation;
        };
        return controls;
    }

    fn scanGroupLocalStream(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
        sink: table_read_source.ScanStreamSink,
    ) !bool {
        return scanGroupLocalStreamAdmitted(ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency, sink, null);
    }

    fn scanGroupLocalStreamAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
        sink: table_read_source.ScanStreamSink,
        route: ?metadata_api.CatalogRouteFence,
    ) !bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (!nativeReadRoute(route)) try self.prepareScanRead(group_id, from_key, to_key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelScanRequest(alloc, from_key, to_key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.from(opts), route));
        defer lease.deinit();
        var cancellation = opts.cancellation;
        try lease.owner().scanStreamWithOptions(table_name, request_json, sink, .{
            .execution_deadline_ns = opts.execution_deadline_ns,
            .cancellation_ctx = if (cancellation != null) @ptrCast(&cancellation.?) else null,
            .cancellation_fn = if (cancellation != null) cancellationTokenRequested else null,
        });
        return true;
    }

    fn scanGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
    ) !?table_read_source.ScanResponse {
        return scanGroupLocalAdmitted(ptr, alloc, group_id, table_name, from_key, to_key, opts, consistency, null);
    }

    fn scanGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        from_key: []const u8,
        to_key: []const u8,
        opts: db_types.ScanOptions,
        consistency: read_gate.ReadConsistency,
        route: ?metadata_api.CatalogRouteFence,
    ) !?table_read_source.ScanResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (!nativeReadRoute(route)) try self.prepareScanRead(group_id, from_key, to_key, opts, consistency);
        const request_json = try table_reads.encodeStorageKernelScanRequest(alloc, from_key, to_key, opts);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.from(opts), route));
        defer lease.deinit();
        var cancellation = opts.cancellation;
        var response = try lease.owner().scanNdjsonWithOptions(table_name, request_json, .{
            .execution_deadline_ns = opts.execution_deadline_ns,
            .cancellation_ctx = if (cancellation != null) @ptrCast(&cancellation.?) else null,
            .cancellation_fn = if (cancellation != null) cancellationTokenRequested else null,
        });
        defer response.deinit();
        return .{ .ndjson = try alloc.dupe(u8, response.bytes()) };
    }

    fn documentArtifactManifestGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifest {
        return documentArtifactManifestGroupLocalAdmitted(ptr, alloc, group_id, table_name, doc_key, artifact_name, consistency, null);
    }

    fn documentArtifactManifestGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        consistency: read_gate.ReadConsistency,
        route: ?metadata_api.CatalogRouteFence,
    ) !?db_types.DocumentArtifactManifest {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (!nativeReadRoute(route)) try self.prepareLookupRead(group_id, doc_key, .{}, consistency);
        const request_json = try table_reads.encodeStorageKernelDocumentArtifactManifestRequest(
            alloc,
            doc_key,
            artifact_name,
        );
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.{}, route));
        defer lease.deinit();
        var response = lease.owner().documentArtifactManifestJson(
            table_name,
            request_json,
        ) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        defer response.deinit();
        return try table_reads.parseStorageKernelDocumentArtifactManifestResponse(
            alloc,
            response.bytes(),
        );
    }

    fn documentArtifactManifestsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.DocumentArtifactManifestList {
        return documentArtifactManifestsGroupLocalAdmitted(ptr, alloc, group_id, table_name, doc_key, consistency, null);
    }

    fn documentArtifactManifestsGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        consistency: read_gate.ReadConsistency,
        route: ?metadata_api.CatalogRouteFence,
    ) !?db_types.DocumentArtifactManifestList {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (!nativeReadRoute(route)) try self.prepareLookupRead(group_id, doc_key, .{}, consistency);
        const request_json = try table_reads.encodeStorageKernelDocumentArtifactManifestsRequest(
            alloc,
            doc_key,
        );
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.{}, route));
        defer lease.deinit();
        var response = try lease.owner().documentArtifactManifestsJson(
            table_name,
            request_json,
        );
        defer response.deinit();
        return try table_reads.parseStorageKernelDocumentArtifactManifestsResponse(
            alloc,
            response.bytes(),
        );
    }

    fn preflightQueryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
    ) !?runtime_preflight.RuntimePreflightSummary {
        return preflightQueryGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, max_work, null);
    }

    fn preflightQueryGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        max_work: u32,
        route: ?metadata_api.CatalogRouteFence,
    ) !?runtime_preflight.RuntimePreflightSummary {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (!nativeReadRoute(route)) try self.prepareQueryRead(group_id, req, consistency);
        const request_json = try table_reads.encodeStorageKernelPreflightRequest(alloc, req, max_work);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.from(req), route));
        defer lease.deinit();
        var response = try lease.owner().preflightJson(table_name, request_json);
        defer response.deinit();
        var summary = try table_reads.parseStorageKernelPreflightSummary(alloc, response.bytes());
        table_reads.annotateVectorWorkerPreflight(alloc, &summary, req);
        return summary;
    }

    fn acquirePreparedOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) !Lease {
        const generation = self.visibleRootGeneration(group_id);
        if (!self.mutex.tryLock()) return error.RaftApplyWriterUnavailable;
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name)) continue;
            if (entry.sql_persistence != null) return error.SqlRelationOwnerChanged;
            if (entry.retired or entry.closing or entry.generation != generation or
                !tryReserveEntryLeaseLocked(entry, .shared)) return error.RaftApplyWriterUnavailable;
            entry.resident = true;
            entry.transient_retirement_pending = false;
            return .{ .source = self, .entry = entry, .exclusive = false };
        }
        return error.RaftApplyWriterUnavailable;
    }

    fn replicatedBatchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
        metadata_prepared: bool,
        entry: ?db_types.OrderedApplyReceipt,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = if (metadata_prepared)
            try self.acquirePreparedOwner(group_id, table_name)
        else
            try self.acquire(group_id, table_name);
        defer lease.deinit();
        const encoded = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(encoded);
        var response = if (entry) |identity|
            try lease.owner().replicatedBatchAtRaftEntryJson(table_name, encoded, identity.term, identity.index)
        else
            try lease.owner().replicatedBatchJson(table_name, encoded);
        defer response.deinit();
        return {};
    }

    fn authorizeNativeLock(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, command: @import("antfly_local_sources").storage_pessimistic_lock_control.Command, context: request_operation.RequestContext) !@import("antfly_local_sources").storage_pessimistic_lock_control.Command {
        const source = self.native_lock_authority orelse return error.LockOwnerChanged;
        return source.authorize_fn(source.ptr, alloc, group, table, command, context);
    }

    fn nativeLockCleanup(command: @import("antfly_local_sources").storage_pessimistic_lock_control.Command) bool {
        return switch (command.operation) {
            .close, .finish, .cancel, .cancel_wait => true,
            else => false,
        };
    }

    /// Only private lock transport can call this after obtaining a fresh
    /// metadata Checked cut. No row mutation, user ownership, or arbitrary
    /// batch field is admitted through this control lane.
    pub fn applyNativeSqlLock(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, req: db_types.BatchRequest) !bool {
        const control = @import("antfly_local_sources").storage_pessimistic_lock_control;
        try control.validateBatch(req);
        if (req.sql_lock.?.native_control) |frozen_control|
            return self.applyFrozenNativeSqlLock(alloc, group, table, req, frozen_control);
        const supplied_route = if (req.sql_lock.?.retirement_control) |retirement| retirement.route else if (req.sql_lock.?.checked) |checked| checked.participant.spec.route else return false;
        if (supplied_route.route.execution != .native_local) return false;
        const command = try self.authorizeNativeLock(alloc, group, table, req.sql_lock.?, .{});
        const part = if (command.retirement_control) |retirement| retirement.participant else checked: {
            const checked = command.checked orelse return error.LockOwnerChanged;
            try checked.validate(command.admission);
            break :checked checked.participant;
        };
        const route = part.spec.route;
        if (route.route.execution != .native_local) return false;
        if (route.route.group_id != group or !std.mem.eql(u8, part.spec.table_name, table)) return error.IdentityNamespaceMismatch;
        var descriptor = try self.loadDescriptorWithDeadline(alloc, group, table, self.catalog.routeFenceDeadline(route));
        defer descriptor.deinit(alloc);
        const native = descriptor.sql_persistence orelse return error.SqlRelationOwnerChanged;
        if (!native.eql(route.route.execution.native_local) or (descriptor.identity.table_id != command.admission.binding.identity.table_id or descriptor.identity.shard_id != command.admission.binding.identity.shard_id or descriptor.identity.range_id != command.admission.binding.identity.range_id)) return error.SqlRelationOwnerChanged;
        const cleanup = nativeLockCleanup(command);
        const binding = try self.currentNativeInitialized(route, cleanup);
        const controls: ReadControls = .{ .native_lock_cleanup = cleanup, .native_initialized = binding, .native_boot_observed = true, .execution_deadline_ns = self.catalog.routeFenceDeadline(route), .cancellation = route.admission_cancellation };
        try controls.check();
        var lease = if (cleanup) (try self.acquireRetainedNativeDecisionDebt(group, table, descriptor.view())) orelse
            try self.acquireDescriptorWithMode(group, table, descriptor.path, descriptor.view(), false, .resident, controls) else try self.acquireDescriptorWithMode(group, table, descriptor.path, descriptor.view(), false, .resident, controls);
        defer lease.deinit();
        var admitted = req;
        admitted.sql_lock = command;
        const body = try table_writes.encodeStorageKernelBatchRequest(alloc, admitted);
        defer alloc.free(body);
        var result = try lease.owner().batchJson(table, body);
        defer result.deinit();
        return true;
    }

    fn frozenNativeLockControls(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, control: @import("antfly_local_sources").storage_pessimistic_lock_control.NativeControl, require_ready: bool) !ReadControls {
        const admitted = try self.validateNativeDecisionRoute(alloc, control.route, group, table, control.owner);
        if (require_ready and admitted.draining) return error.SqlRelationOwnerChanged;
        return .{ .native_route = admitted.route, .native_decision_owner = control.owner, .native_initialized = admitted.initialized, .native_boot_observed = true, .execution_deadline_ns = self.catalog.routeFenceDeadline(control.route), .cancellation = control.route.admission_cancellation };
    }

    fn applyFrozenNativeSqlLock(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, req: db_types.BatchRequest, control: @import("antfly_local_sources").storage_pessimistic_lock_control.NativeControl) !bool {
        const command = req.sql_lock.?;
        try command.validate();
        const controls = try self.frozenNativeLockControls(alloc, group, table, control, command.operation == .prepare);
        var lease = try self.acquireWithControls(group, table, controls);
        defer lease.deinit();
        var admitted = req;
        if (command.operation == .resolve) {
            const query = try std.json.Stringify.valueAlloc(alloc, @import("antfly_local_sources").storage_pessimistic_lock_control.Query{ .admission = command.admission, .native_control = control }, .{});
            defer alloc.free(query);
            const probe = try table_reads.encodeStorageKernelLookupRequest(alloc, "", .{ .sql_lock_query_json = query });
            defer alloc.free(probe);
            if (lease.owner().lookupJson(table, probe)) |value| {
                var observed = value;
                observed.deinit();
            } else |err| {
                if (err != error.ExternalTransactionDecisionChanged) return err;
                // Normal sealed retries need no metadata IO. A missing Ref
                // can be adopted only after the actual decision home proves
                // this exact immutable participant and terminal ABORT.
                admitted.sql_lock = try self.authorizeNativeLock(alloc, group, table, command, .{ .deadline_ns = controls.execution_deadline_ns, .deadline_io = controls.execution_io, .cancellation = controls.cancellation orelse .none });
            }
        }
        const body = try table_writes.encodeStorageKernelBatchRequest(alloc, admitted);
        defer alloc.free(body);
        // The native controller independently rejoins its stored frozen Ref
        // and external decision. No executing metadata directory or user
        // credential may be fabricated to finish a prepared participant.
        var result = try lease.owner().batchJson(table, body);
        defer result.deinit();
        return true;
    }

    fn batchGroupLocalRouted(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, req: db_types.BatchRequest, route: metadata_api.CatalogRouteFence) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (route.route.execution != .native_local) return batchGroupLocal(ptr, alloc, group, table, req);
        self.applyTransactionGroupLocalRouted(alloc, group, table, req, .{ .cancellation = route.admission_cancellation }, route) catch |err| {
            if (err == error.SqlRelationOwnerChanged) return self.nativeOwnerFailure(.routed_batch, group, route.table_id, 0, @errorReturnTrace());
            return err;
        };
        return {};
    }

    fn batchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (req.sql_lock != null) {
            if (try self.applyNativeSqlLock(alloc, group_id, table_name, req)) return {};
        }
        if (req.restore_staging_scope) |scope| {
            var descriptor = try self.resolveRestoreDescriptor(alloc, group_id, table_name, scope, req.restore_staging_plan_id, restoreDescriptorUseForBatch(req), .{});
            defer descriptor.deinit(alloc);
            return self.batchGroupLocalWithDescriptor(alloc, group_id, table_name, req, descriptor.view());
        }
        return self.batchGroupLocalWithDescriptor(alloc, group_id, table_name, req, null);
    }

    fn batchGroupLocalWithDescriptor(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
        descriptor: ?descriptor_contract.Descriptor,
    ) !?void {
        try @import("sql_graph_vote.zig").require(alloc, self.catalog, table_name, req, null);
        if (self.replication_write_gate) |gate| try gate.check();
        var replication_mutation = if (self.hot_standby_async_mirror) |mirror|
            if (mirror.mutation_barrier) |barrier| barrier.acquireShared() else null
        else
            null;
        defer if (replication_mutation) |*lease| lease.release();
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        const private_path = if (descriptor != null) try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id }) else null;
        defer if (private_path) |path| alloc.free(path);
        var lease = if (descriptor) |owned| try self.acquireDescriptor(group_id, table_name, private_path.?, owned) else try self.acquire(group_id, table_name);
        defer lease.deinit();
        const logged = lease.entry.sql_persistence == null;
        if (logged) try self.preflightHotStandbyMirrorSyncCommit();
        if (descriptor == null and (req.writes.len != 0 or req.transforms.len != 0)) _ = try lease.owner().preflightWriteAdmissionJson(table_name, request_json);
        if (req.transaction != null and req.restore_staging_scope != null) {
            // Private activation uses the batch route to retain exact Plan
            // authority for every transaction phase, including abort/retry.
            // The ordinary document batch handler does not execute transaction
            // controls. Reuse the native transaction dispatcher (no Raft entry
            // or synthetic watermark); the descriptor above and the native
            // scoped transaction APIs independently validate the hidden owner.
            if (descriptor == null) return error.RestoreStagingScopeChanged;
            var response = try lease.owner().replicatedBatchJson(table_name, request_json);
            defer response.deinit();
            return {};
        }
        var callback_error_relay: kernel_error_identity.CallbackErrorRelay = .{};
        var committed_effects_context = CommittedBatchEffectsContext{
            .source = self,
            .request = req,
            .identity = lease.entry.identity,
            .error_relay = &callback_error_relay,
        };
        var dispatch_context = if (self.document_child_range_dispatch_source) |source|
            DocumentChildRangeDispatchContext{
                .alloc = alloc,
                .source = source,
                .table_name = table_name,
            }
        else
            null;
        var response: client.Response = .{};
        const callback_status = lease.owner().batchJsonWithCallbacksStatus(
            table_name,
            request_json,
            if (dispatch_context) |*context| context else null,
            if (dispatch_context != null) dispatchDocumentChildRange else null,
            if (logged) &committed_effects_context else null,
            if (logged) committedBatchEffects else null,
            &response,
        );
        defer response.deinit();
        try callback_error_relay.finish(callback_status);
        return {};
    }

    fn preflightHotStandbyMirrorSyncCommit(self: *ProvisionedKernelOwnerSource) !void {
        const mirror = self.hot_standby_async_mirror orelse return;
        try mirror.publisher.preflightRecordingDecision(mirror);
    }

    const CommittedBatchEffectsContext = struct {
        source: *ProvisionedKernelOwnerSource,
        request: db_types.BatchRequest,
        identity: Identity,
        error_relay: *kernel_error_identity.CallbackErrorRelay,
    };

    fn committedBatchEffects(
        ptr: ?*anyopaque,
        replay_payload: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const context: *CommittedBatchEffectsContext = @ptrCast(@alignCast(ptr orelse
            return .invalid_argument));
        context.source.mirrorReplicationBatchMutationCommit(context.request, context.identity) catch |err|
            return context.error_relay.capture(err);
        if (replay_payload.len != 0) {
            context.source.mirrorReplicationReplayPayloadCommit(replay_payload.slice(), context.identity) catch |err|
                return context.error_relay.capture(err);
        }
        return .ok;
    }

    fn mirrorReplicationBatchMutationCommit(
        self: *ProvisionedKernelOwnerSource,
        req: db_types.BatchRequest,
        identity: Identity,
    ) !void {
        const mirror = self.hot_standby_async_mirror orelse return;
        const transition_mutex = mirror.transition_mutex;
        if (transition_mutex) |mutex| lock(mutex);
        var transition_locked = transition_mutex != null;
        defer if (transition_locked) transition_mutex.?.unlock();

        const payload = replication_effects.encodeBatchMutationRequestAlloc(self.alloc, req) catch |err| {
            noteReplicationMirrorFailure(mirror, err);
            if (mirror.requirements.synchronous) return err;
            return;
        };
        defer self.alloc.free(payload);
        const lsn = mirror.publisher.publish(mirror, .batch, payload, .{
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
        }) catch |err| {
            noteReplicationMirrorFailure(mirror, err);
            if (mirror.requirements.synchronous) return err;
            return;
        };
        mirror.notePublished(lsn);

        if (transition_mutex) |mutex| {
            mutex.unlock();
            transition_locked = false;
        }
        try evaluateReplicationMirrorCommitGate(mirror, lsn);
        if (transition_mutex) |mutex| {
            lock(mutex);
            transition_locked = true;
        }
        if (self.replication_write_gate) |gate| try gate.check();
    }

    fn mirrorReplicationReplayPayloadCommit(
        self: *ProvisionedKernelOwnerSource,
        replay_payload: []const u8,
        identity: Identity,
    ) !void {
        const mirror = self.hot_standby_async_mirror orelse return;
        const transition_mutex = mirror.transition_mutex;
        if (transition_mutex) |mutex| lock(mutex);
        var transition_locked = transition_mutex != null;
        defer if (transition_locked) transition_mutex.?.unlock();

        const lsn = mirror.publisher.publish(mirror, .replay, replay_payload, .{
            .shard_id = identity.shard_id,
            .table_id = identity.table_id,
        }) catch |err| {
            noteReplicationMirrorFailure(mirror, err);
            if (mirror.requirements.synchronous) return err;
            return;
        };
        mirror.notePublished(lsn);

        if (transition_mutex) |mutex| {
            mutex.unlock();
            transition_locked = false;
        }
        try evaluateReplicationMirrorCommitGate(mirror, lsn);
        if (transition_mutex) |mutex| {
            lock(mutex);
            transition_locked = true;
        }
        if (self.replication_write_gate) |gate| try gate.check();
    }

    fn evaluateReplicationMirrorCommitGate(mirror: replication_contract.AsyncEffectMirror, lsn: u64) !void {
        try mirror.publisher.complete(mirror, lsn);
    }

    fn noteReplicationMirrorFailure(mirror: replication_contract.AsyncEffectMirror, err: anyerror) void {
        mirror.noteFailure();
        std.log.warn("failed to mirror compiled-owner commit into HA stream: {s}", .{@errorName(err)});
    }

    const DocumentChildRangeDispatchContext = struct {
        alloc: std.mem.Allocator,
        source: table_write_source.TableWriteSource,
        table_name: []const u8,
    };

    fn dispatchDocumentChildRange(
        ptr: ?*anyopaque,
        owner_group_id: u64,
        request_json: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const context: *DocumentChildRangeDispatchContext = @ptrCast(@alignCast(ptr orelse
            return .invalid_argument));
        var parsed = std.json.parseFromSlice(
            table_writes.StorageKernelArtifactChildRangeBatchRequest,
            context.alloc,
            request_json.slice(),
            .{ .allocate = .alloc_always },
        ) catch |err| return documentChildRangeDispatchStatusFromError(err);
        defer parsed.deinit();
        const sequence = context.source.applyDocumentArtifactChildRangeBatch(
            context.alloc,
            owner_group_id,
            context.table_name,
            parsed.value.doc_key,
            parsed.value.artifact_name,
            parsed.value.batch,
        ) catch |err| return documentChildRangeDispatchStatusFromError(err);
        if (sequence == null) return .not_found;
        return .ok;
    }

    fn documentChildRangeDispatchStatusFromError(err: anyerror) abi.Status {
        return kernel_error_identity.statusFromError(err);
    }

    fn vectorMigrationGroupLocal(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, request_json: []const u8) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = lease.owner().vectorMigrationJson(table_name, request_json) catch |err| {
            if (err == error.VectorMigrationRecoveryRequired or err == error.VectorPayloadStorePoisoned)
                lease.retireAfterConfigurationFailure();
            return err;
        };
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    fn executeArtifactOperation(
        self: *ProvisionedKernelOwnerSource,
        group_id: u64,
        table_name: []const u8,
        operation: abi.ArtifactOperation,
        request_json: []const u8,
        cancellation_ctx: ?*anyopaque,
        cancellation_fn: ?abi.CancellationCheckFn,
        defer_durable_index_repair_execution: bool,
    ) !client.Response {
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        return try lease.owner().artifactOperationJson(
            table_name,
            operation,
            request_json,
            cancellation_ctx,
            cancellation_fn,
            defer_durable_index_repair_execution,
        );
    }

    fn corruptEmbeddingArtifactGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        index_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelEmbeddingCorruptionRequest(
            alloc,
            doc_key,
            index_name,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .corrupt_embedding,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        if (!try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes())) return error.NotFound;
        return {};
    }

    fn reprocessDocumentArtifactGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
    ) !?bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactDocumentRequest(
            alloc,
            doc_key,
            artifact_name,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .reprocess_document,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes());
    }

    fn reprocessDocumentArtifactRangeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        artifact_name: []const u8,
        request: db_types.DocumentArtifactTableReprocessRequest,
    ) !?db_types.DocumentArtifactTableReprocessResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactRangeRequest(
            alloc,
            artifact_name,
            request,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .reprocess_document_range,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelDocumentArtifactTableReprocessResult(
            alloc,
            response.bytes(),
        );
    }

    fn listArtifactRepairIssuesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairListRequest,
    ) !?db_types.ArtifactRepairListResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactRepairListRequest(alloc, request);
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .list_repair_issues,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelArtifactRepairListResult(alloc, response.bytes());
    }

    const ArtifactRepairCancellation = struct {
        check: db_types.RepairCancelCheck,

        fn requested(ctx: ?*anyopaque) callconv(.c) u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx orelse return 0));
            return @intFromBool(self.check.requested());
        }
    };

    fn validateArtifactRepairControls(options: db_types.ArtifactRepairRunOptions) !void {
        const defaults = db_types.ArtifactRepairRunOptions{};
        if (options.yield_check != null or
            options.activation_check != null or
            options.capacity_source != null or
            options.capacity_check != null or
            options.owner_epoch != defaults.owner_epoch or
            options.max_activation_gap_sequences != defaults.max_activation_gap_sequences or
            options.max_convergence_rounds != defaults.max_convergence_rounds or
            options.max_activation_pause_ms != defaults.max_activation_pause_ms or
            options.estimated_candidate_bytes != defaults.estimated_candidate_bytes or
            options.planned_disk_bytes != defaults.planned_disk_bytes or
            options.capacity_domain_id != defaults.capacity_domain_id or
            !std.meta.eql(options.capacity_observation, defaults.capacity_observation))
        {
            return error.UnsupportedStorageKernelRepairControls;
        }
    }

    fn repairArtifactIssuesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairRunRequest,
    ) !?db_types.ArtifactRepairResult {
        return try repairArtifactIssuesGroupLocalControlled(
            ptr,
            alloc,
            group_id,
            table_name,
            request,
            .{},
        );
    }

    fn repairArtifactIssuesGroupLocalControlled(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        request: db_types.ArtifactRepairRunRequest,
        options: db_types.ArtifactRepairRunOptions,
    ) !?db_types.ArtifactRepairResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try validateArtifactRepairControls(options);
        if (options.cancelled()) return error.Canceled;
        const request_json = try table_writes.encodeStorageKernelArtifactRepairRequest(alloc, request);
        defer alloc.free(request_json);
        var cancellation: ?ArtifactRepairCancellation = if (options.cancel_check) |check|
            .{ .check = check }
        else
            null;
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .repair_issues,
            request_json,
            if (cancellation) |*value| value else null,
            if (cancellation != null) ArtifactRepairCancellation.requested else null,
            options.defer_durable_index_repair_execution,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelArtifactRepairResult(alloc, response.bytes());
    }

    fn updateDocumentArtifactChildRangePlacementGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        update: db_types.DocumentArtifactChildRangePlacementUpdate,
    ) !?bool {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactPlacementRequest(
            alloc,
            doc_key,
            artifact_name,
            update,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .update_child_range_placement,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelHandledResponse(alloc, response.bytes());
    }

    fn applyDocumentArtifactChildRangeBatchGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        doc_key: []const u8,
        artifact_name: []const u8,
        batch: document_artifact_child_range.ApplyBatch,
    ) !?u64 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const request_json = try table_writes.encodeStorageKernelArtifactChildRangeBatchRequest(
            alloc,
            doc_key,
            artifact_name,
            batch,
        );
        defer alloc.free(request_json);
        var response = try self.executeArtifactOperation(
            group_id,
            table_name,
            .apply_child_range_batch,
            request_json,
            null,
            null,
            false,
        );
        defer response.deinit();
        return try table_writes.parseStorageKernelSequenceResponse(alloc, response.bytes());
    }

    fn applyTransactionGroupLocal(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
    ) !void {
        return self.applyTransactionGroupLocalWithContext(alloc, group_id, table_name, req, .{});
    }

    fn applyTransactionGroupLocalWithContext(
        self: *ProvisionedKernelOwnerSource,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.BatchRequest,
        context: request_operation.RequestContext,
    ) !void {
        return self.applyTransactionGroupLocalRouted(alloc, group_id, table_name, req, context, null);
    }

    fn applyTransactionGroupLocalRouted(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_types.BatchRequest, context: request_operation.RequestContext, route: ?metadata_api.CatalogRouteFence) !void {
        try @import("sql_graph_vote.zig").require(alloc, self.catalog, table_name, req, context.deadline_ns);
        var admitted_route = route;
        var native_admission: ?NativeControlAdmission = null;
        if (route) |fence| {
            if (fence.route.execution == .native_local and nativeResolutionOwner(req) != null) {
                native_admission = self.validateNativeDecisionRoute(alloc, fence, group_id, table_name, nativeResolutionOwner(req).?) catch |err| {
                    if (nativeRecoveryAdmissionFailure(err) and try self.completeLostNativeControl(alloc, fence, group_id, table_name, req, context)) return;
                    return err;
                };
                admitted_route = native_admission.?.route;
            } else try self.validateRoutedRead(alloc, fence, group_id, table_name);
        }
        try context.ensureActive();
        const request_json = try table_writes.encodeStorageKernelBatchRequest(alloc, req);
        defer alloc.free(request_json);
        var lease = try self.acquireTransactionOwner(alloc, group_id, table_name, req, context, admitted_route, native_admission);
        defer lease.deinit();
        try context.ensureActive();
        try self.applyCapturedTransactionOwner(alloc, &lease, table_name, req, context, route, native_admission, request_json);
    }

    // The caller retains the actual owner lease and has freshly checked the
    // native image/Boot when native_admission is present. This shared dispatch
    // also keeps the CABI failure and final-ACK fallback under one testable cut.
    fn applyCapturedTransactionOwner(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, lease: *Lease, table_name: []const u8, req: db_types.BatchRequest, context: request_operation.RequestContext, route: ?metadata_api.CatalogRouteFence, native_admission: ?NativeControlAdmission, request_json: []const u8) !void {
        // A genuine pre-BEGIN ABORT retains its real local singleton. The
        // metadata coordinator may own a global manifest without ever having
        // installed a local global roster. Inspect actual native custody;
        // never infer this distinction from a transport error or absent reply.
        if (native_admission) |admission| if (req.transaction) |mutation| if (mutation == .acknowledge_many and mutation.acknowledge_many.participants.len > 1) {
            const ack = mutation.acknowledge_many;
            const owner = ack.external_decision_owner orelse return error.ExternalTransactionDecisionChanged;
            const pin = admission.route.native_participant orelse return error.SqlRelationRecoveryIncomplete;
            var retained: ?client.Owner.ExternalRetention = lease.owner().externalTransactionRetention(alloc, table_name, admission.route.route.group_id, ack.txn_id, owner, pin) catch |err| snapshot: {
                if (err == error.TxnNotFound) break :snapshot null;
                return err;
            };
            defer if (retained) |*state| state.deinit();
            if (retained) |state| if (!state.coordinator and state.status == .aborted and !state.prepared and state.intents_resolved and state.participants.value.len == 1) {
                const local_id = try @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroupScoped(alloc, table_name, admission.route.route.group_id, null, null);
                defer alloc.free(local_id);
                if (!std.mem.eql(u8, state.participants.value[0], local_id)) return error.InvalidParticipant;
                var selected = false;
                for (ack.participants) |member| if (std.mem.eql(u8, member, local_id)) {
                    selected = true;
                };
                if (!selected) return error.InvalidParticipant;
                const authority = self.native_terminal_authority orelse return error.CommitDecisionUnknown;
                const completed = authority.complete_ack_fn orelse return error.CommitDecisionUnknown;
                // This existing fresh canonical check also rejects a peer
                // attempting to acknowledge another globally enrolled member.
                try completed(authority.ptr, alloc, table_name, admission.route, ack.txn_id, owner, ack.participants, context);
                var local_ack = req;
                local_ack.transaction.?.acknowledge_many.participants = state.participants.value;
                const encoded = try table_writes.encodeStorageKernelBatchRequest(alloc, local_ack);
                defer alloc.free(encoded);
                var result = lease.owner().replicatedBatchJson(table_name, encoded) catch |err| {
                    if (err != error.CommitDecisionUnknown) return err;
                    // A concurrent legitimate GC may retire the already
                    // ACKed record after our retained-state observation.
                    // Reuse the existing fresh completed-manifest proof;
                    // absence alone never turns this into success.
                    try completed(authority.ptr, alloc, table_name, admission.route, ack.txn_id, owner, ack.participants, context);
                    return;
                };
                result.deinit();
                return;
            };
        };
        var response = if (req.transaction != null)
            // Reuse the private transaction dispatcher without an ordered
            // receipt; it invokes the live DB transaction admission paths.
            lease.owner().replicatedBatchJson(table_name, request_json) catch |err| {
                if (err != error.CommitDecisionUnknown or native_admission == null) return err;
                const mutation = req.transaction.?;
                const txn = switch (mutation) {
                    .acknowledge => |v| v.txn_id,
                    .acknowledge_many => |v| v.txn_id,
                    else => return err,
                };
                const owner = req.externalDecisionControlOwner() orelse return err;
                var single: [1][]const u8 = undefined;
                const members = switch (mutation) {
                    .acknowledge => |v| blk: {
                        single[0] = v.participant;
                        break :blk single[0..];
                    },
                    .acknowledge_many => |v| v.participants,
                    else => unreachable,
                };
                const authority = self.native_terminal_authority orelse return err;
                const completed = authority.complete_ack_fn orelse return err;
                // The retained lease and fresh native admission still pin this
                // exact image. Only a completed metadata manifest can discharge
                // lost retention ACK replies after native terminal metadata GC.
                try completed(authority.ptr, alloc, table_name, route.?, txn, owner, members, context);
                try context.ensureActive();
                return;
            }
        else
            try lease.owner().batchJson(table_name, request_json);
        defer response.deinit();
    }

    fn acquireHiddenTransactionOwner(self: *ProvisionedKernelOwnerSource, group_id: u64, table_name: []const u8) !?Lease {
        const generation = self.visibleRootGeneration(group_id);
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items) |entry| {
            if (entry.group_id != group_id or !std.mem.eql(u8, entry.table_name, table_name) or entry.restore_bootstrap_json.len == 0) continue;
            if (entry.retired or entry.closing or entry.generation != generation or !tryReserveEntryLeaseLocked(entry, .shared)) return error.RaftApplyWriterUnavailable;
            return .{ .source = self, .entry = entry };
        }
        return null;
    }

    pub fn restoreDescriptorUseForBatch(req: db_types.BatchRequest) RestoreDescriptorUse {
        if (req.restore_staging) |control| switch (control) {
            .finish => return .resolve,
            else => {},
        };
        // Empty-generation admission is installed after old-owner cutover,
        // while the target is still private. This is a bounded Plan-bound
        // recovery control, not a new import mutation; the storage apply
        // validates the hidden bootstrap, exact scope, and mapped receipt.
        if (req.relational_topology) |command| {
            if (command.action == .install_generation_handoff) return .resolve;
        }
        if (req.transaction) |txn| switch (txn) {
            .resolve, .acknowledge, .acknowledge_many, .cleanup => return .resolve,
            else => {},
        };
        return .mutate;
    }

    fn acquireTransactionOwner(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, req: db_types.BatchRequest, context: request_operation.RequestContext, route: ?metadata_api.CatalogRouteFence, admission: ?NativeControlAdmission) !Lease {
        if (req.restore_staging_scope) |scope| {
            var descriptor = try self.resolveRestoreDescriptor(alloc, group_id, table_name, scope, req.restore_staging_plan_id, restoreDescriptorUseForBatch(req), context);
            defer descriptor.deinit(alloc);
            const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
            defer alloc.free(path);
            return self.acquireDescriptorWithMode(group_id, table_name, path, descriptor.view(), false, .resident, .{ .execution_deadline_ns = context.deadline_ns, .execution_io = context.deadline_io, .cancellation = context.cancellation });
        }
        if (req.transaction) |txn| switch (txn) {
            .resolve, .acknowledge, .acknowledge_many => if (try self.acquireHiddenTransactionOwner(group_id, table_name)) |lease| return lease,
            else => {},
        };
        var controls: ReadControls = .{ .execution_deadline_ns = context.deadline_ns, .execution_io = context.deadline_io, .cancellation = context.cancellation };
        if (route) |fence| controls.native_route = fence;
        controls.native_decision_owner = nativeResolutionOwner(req);
        if (admission) |verified| {
            controls.native_initialized = verified.initialized;
            controls.native_boot_observed = true;
        }
        return self.acquireWithControls(group_id, table_name, controls);
    }

    fn txnBeginGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        retain_terminal: bool,
        participants: []const []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .transaction = .{ .begin = .{
                .txn_id = txn_id,
                .begin_timestamp = begin_timestamp,
                .created_at_ns = platform_time.realtimeNs(),
                .topology_epoch = topology_epoch,
                .retain_terminal = retain_terminal,
                .participants = participants,
            } },
        });
        return {};
    }

    fn txnBeginGroupLocalWithPreDecisionContext(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        begin_timestamp: u64,
        topology_epoch: u64,
        retain_terminal: bool,
        participants: []const []const u8,
        context: @import("antfly_local_sources").api_distributed_txn_contract.PreDecisionContext,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const active: request_operation.RequestContext = .{ .deadline_ns = context.deadline_ns, .deadline_io = context.deadline_io, .cancellation = context.cancellation };
        try active.ensureActive();
        if (context.restore_staging_scope == null and context.restore_staging_plan_id != null) return error.RestoreStagingScopeChanged;
        try self.applyTransactionGroupLocalRouted(alloc, group_id, table_name, .{
            .restore_staging_scope = context.restore_staging_scope,
            .restore_staging_plan_id = context.restore_staging_plan_id,
            .transaction = .{ .begin = .{
                .txn_id = txn_id,
                .begin_timestamp = begin_timestamp,
                .created_at_ns = platform_time.realtimeNs(),
                .topology_epoch = topology_epoch,
                .retain_terminal = retain_terminal or context.external_decision_owner != null,
                .external_decision_owner = context.external_decision_owner,
                .sql_locks = context.sql_locks,
                .participants = participants,
            } },
        }, active, context.route_fence);
        return {};
    }

    fn txnPrepareGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        topology_epoch: u64,
        req: db_types.TransactionIntentRequest,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (req.relational_index_maintenance) |command| if (command.owner_group_id != group_id) return error.PreparedGenerationChanged;
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, req.asBatch(txn_id, topology_epoch));
        return {};
    }

    fn txnPrepareGroupLocalWithPreDecisionContext(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, txn_id: db_types.TxnId, topology_epoch: u64, req: db_types.TransactionIntentRequest, context: @import("antfly_local_sources").api_distributed_txn_contract.PreDecisionContext) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (req.relational_index_maintenance) |command| if (command.owner_group_id != group) return error.PreparedGenerationChanged;
        try self.applyTransactionGroupLocalRouted(alloc, group, table, req.asBatch(txn_id, topology_epoch), .{ .deadline_ns = context.deadline_ns, .deadline_io = context.deadline_io, .cancellation = context.cancellation }, context.route_fence);
        return {};
    }

    fn txnResolveGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        status: db_types.TxnStatus,
        commit_version: u64,
        _: u64,
        sync_level: db_types.SyncLevel,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .sync_level = sync_level,
            .transaction = .{ .resolve = .{
                .txn_id = txn_id,
                .status = status,
                .commit_version = commit_version,
            } },
        });
        return {};
    }

    fn txnResolveGroupLocalWithRequest(ptr: *anyopaque, alloc: std.mem.Allocator, group: u64, table: []const u8, request: @import("antfly_local_sources").api_distributed_txn_contract.TxnResolveRequest, cancellation: db_types.CancellationToken) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocalRouted(alloc, group, table, .{
            .restore_staging_scope = request.restore_staging_scope,
            .restore_staging_plan_id = request.restore_staging_plan_id,
            .sync_level = request.sync_level,
            .transaction = .{ .resolve = .{ .txn_id = request.txn_id, .status = request.status, .commit_version = request.commit_version, .external_decision_owner = request.external_decision_owner } },
        }, .{ .cancellation = cancellation }, request.route_fence);
        return {};
    }

    fn txnStatusGroupLocal(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
    ) !?db_types.TxnStatus {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = (try self.acquireHiddenTransactionOwner(group_id, table_name)) orelse try self.acquire(group_id, table_name);
        defer lease.deinit();
        return switch (try lease.owner().transactionStatus(table_name, txn_id)) {
            .pending => .pending,
            .committed => .committed,
            .aborted => .aborted,
        };
    }

    fn txnStatusGroupLocalWithRequest(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: @import("antfly_local_sources").api_distributed_txn_contract.TxnStatusRequest,
        context: request_operation.RequestContext,
    ) !?db_types.TxnStatus {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        if (req.route_fence) |route| {
            if (route.route.execution != .native_local or req.restore_staging_scope != null) return error.InvalidTxnRequest;
            const owner = req.external_decision_owner orelse return error.InvalidExternalDecisionOwner;
            try context.ensureActive();
            const admission = self.validateNativeDecisionRoute(alloc, route, group_id, table_name, owner) catch |err| {
                if (nativeRecoveryAdmissionFailure(err)) if (try self.nativeLostDecision(alloc, route, group_id, table_name, req.txn_id, owner, context)) |decision| return if (decision.status == .committed) .committed else .aborted;
                return err;
            };
            var controls = ReadControls.withRoute(.{ .execution_deadline_ns = context.deadline_ns, .execution_io = context.deadline_io, .cancellation = context.cancellation }, admission.route);
            controls.native_decision_owner = owner;
            controls.native_initialized = admission.initialized;
            controls.native_boot_observed = true;
            var lease = try self.acquireWithControls(group_id, table_name, controls);
            defer lease.deinit();
            try context.ensureActive();
            return switch (try lease.owner().externalTransactionStatus(table_name, req.txn_id, owner)) {
                .pending => .pending,
                .committed => .committed,
                .aborted => .aborted,
            };
        }
        if (req.external_decision_owner != null) return error.InvalidTxnRequest;
        const scope = req.restore_staging_scope orelse return error.RestoreStagingScopeChanged;
        var descriptor = try self.resolveRestoreDescriptor(alloc, group_id, table_name, scope, req.restore_staging_plan_id, .resolve, context);
        defer descriptor.deinit(alloc);
        // A coordinator may recover a durable decision after publication or
        // cancellation, but it must never turn an uncertain read into an abort.
        try feature_reads.FeatureReads.init(self.read_safety_barrier).prepareLookupWithConsistency(group_id, "", .{
            .execution_deadline_ns = context.deadline_ns,
            .execution_io = context.deadline_io,
            .cancellation = context.cancellation,
        }, .read_index);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ self.replica_root_dir, group_id });
        defer alloc.free(path);
        var lease = try self.acquireDescriptorWithMode(group_id, table_name, path, descriptor.view(), false, .resident, .{ .execution_deadline_ns = context.deadline_ns, .execution_io = context.deadline_io, .cancellation = context.cancellation });
        defer lease.deinit();
        try context.ensureActive();
        return switch (try lease.owner().transactionStatus(table_name, req.txn_id)) {
            .pending => .pending,
            .committed => .committed,
            .aborted => .aborted,
        };
    }

    fn txnAcknowledgeGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        txn_id: db_types.TxnId,
        participant: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try self.applyTransactionGroupLocal(alloc, group_id, table_name, .{
            .transaction = .{ .acknowledge = .{
                .txn_id = txn_id,
                .participant = participant,
            } },
        });
        return {};
    }

    fn beginBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        try lease.owner().beginBulkIngest(table_name);
        lease.entry.bulk_ingest_active.store(true, .release);
        return {};
    }

    const BulkFinishCallbacks = struct {
        options: backend_types.BulkIngestFinishOptions,
        error_relay: kernel_error_identity.CallbackErrorRelay = .{},

        fn progress(
            ctx: ?*anyopaque,
            value: *const abi.BulkProgress,
        ) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            const callback = self.options.progress_fn orelse return;
            callback(self.options.progress_ctx.?, .{
                .phase = switch (value.phase) {
                    .begin => .begin,
                    .split => .split,
                    .publish => .publish,
                    .complete => .complete,
                },
                .publish_window = value.publish_window,
                .split_steps = value.split_steps,
                .deferred_leaf_splits = value.deferred_leaf_splits,
                .elapsed_ns = value.elapsed_ns,
            });
        }

        fn admission(ctx: ?*anyopaque) callconv(.c) abi.Status {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.options.checkAdmission() catch |err| return self.error_relay.capture(err);
            return .ok;
        }
    };

    pub fn validateBulkCallbackIdentityForTest() !void {
        const Admission = struct {
            fn fail(_: *anyopaque) !void {
                return error.TestBulkAdmissionIdentity;
            }
        };
        var admission_context: u8 = 0;
        var callbacks = BulkFinishCallbacks{ .options = .{
            .admission_ctx = &admission_context,
            .admission_fn = Admission.fail,
        } };
        const callback_status = BulkFinishCallbacks.admission(&callbacks);

        // Model the real provider adapter: callback status becomes a Zig
        // error inside storage, then becomes a status again at the exported
        // operation boundary. The consumer relay must still win.
        const provider_status = blk: {
            kernel_error_identity.statusToError(callback_status) catch |err| {
                break :blk kernel_error_identity.statusFromError(err);
            };
            break :blk abi.Status.ok;
        };
        try std.testing.expectEqual(abi.Status.storage_kernel_callback_failed, provider_status);
        try std.testing.expectError(
            error.TestBulkAdmissionIdentity,
            callbacks.error_relay.finish(provider_status),
        );
    }

    fn finishBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
        options: backend_types.BulkIngestFinishOptions,
    ) !?void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var callbacks = BulkFinishCallbacks{ .options = options };
        const request = abi.BulkFinishRequest{
            .compact = @intFromBool(options.compact),
            .flush = @intFromBool(options.flush),
            .has_max_deferred_l0_runs = @intFromBool(options.max_deferred_l0_runs != null),
            .has_max_foreground_compaction_input_bytes = @intFromBool(options.max_foreground_compaction_input_bytes != null),
            .has_max_foreground_compaction_ns = @intFromBool(options.max_foreground_compaction_ns != null),
            .has_max_deferred_hbc_leaf_splits_per_publish = @intFromBool(options.max_deferred_hbc_leaf_splits_per_publish != null),
            .has_max_deferred_hbc_leaf_split_members_per_publish = @intFromBool(options.max_deferred_hbc_leaf_split_members_per_publish != null),
            .has_bulk_rebuild_hbc_leaf_min_members = @intFromBool(options.bulk_rebuild_hbc_leaf_min_members != null),
            .table_name = .fromSlice(table_name),
            .max_deferred_l0_runs = @intCast(options.max_deferred_l0_runs orelse 0),
            .max_foreground_compaction_steps = @intCast(options.max_foreground_compaction_steps),
            .max_foreground_compaction_input_bytes = options.max_foreground_compaction_input_bytes orelse 0,
            .max_foreground_compaction_ns = options.max_foreground_compaction_ns orelse 0,
            .max_deferred_hbc_leaf_splits_per_publish = @intCast(options.max_deferred_hbc_leaf_splits_per_publish orelse 0),
            .max_deferred_hbc_leaf_split_members_per_publish = @intCast(options.max_deferred_hbc_leaf_split_members_per_publish orelse 0),
            .bulk_rebuild_hbc_leaf_min_members = @intCast(options.bulk_rebuild_hbc_leaf_min_members orelse 0),
            .callback_ctx = &callbacks,
            .progress_fn = if (options.progress_fn != null and options.progress_ctx != null)
                BulkFinishCallbacks.progress
            else
                null,
            .admission_fn = if (options.admission_fn != null and options.admission_ctx != null)
                BulkFinishCallbacks.admission
            else
                null,
        };
        const status = lease.owner().finishBulkIngestStatus(&request);
        try callbacks.error_relay.finish(status);
        lease.entry.bulk_ingest_active.store(false, .release);
        return {};
    }

    fn abortBulkIngestGroupLocal(
        ptr: *anyopaque,
        group_id: u64,
        table_name: []const u8,
    ) void {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = self.acquire(group_id, table_name) catch |err| {
            std.log.warn("storage owner bulk abort acquire failed table={s} group_id={d} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return;
        };
        defer lease.deinit();
        lease.owner().abortBulkIngest(table_name) catch |err| {
            std.log.warn("storage owner bulk abort failed table={s} group_id={d} err={s}", .{
                table_name,
                group_id,
                @errorName(err),
            });
            return;
        };
        lease.entry.bulk_ingest_active.store(false, .release);
    }

    fn localRuntimeStatuses(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatuses {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        var items = std.ArrayListUnmanaged(runtime_status.LocalTableRuntimeStatus).empty;
        errdefer {
            for (items.items) |*item| item.deinit(alloc);
            items.deinit(alloc);
        }
        for (group_ids) |group_id| {
            // Each group is an independent best-effort observation. One cold
            // or busy owner must not discard facts sampled from its siblings.
            var status = (localRuntimeStatusGroupLocal(self, alloc, group_id, table_name) catch |err| switch (err) {
                error.StorageBusy, error.StorageReadTemporarilyUnavailable => continue,
                else => return err,
            }) orelse continue;
            errdefer status.deinit(alloc);
            try items.append(alloc, status);
        }
        if (items.items.len == 0) {
            items.deinit(alloc);
            return null;
        }
        return .{ .items = try items.toOwnedSlice(alloc) };
    }

    /// Capture one complete owner observation from its real lease. Native
    /// metadata publication must not walk every table/range or open a second DB.
    pub fn runtimeStatusGroupAtRoot(self: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, context: request_operation.RequestContext) !runtime_status.LocalTableRuntimeStatus {
        try context.ensureActive();
        const normalized = try platformDeadlineContext(context);
        var lease = try self.acquireControlWithControls(group, table, .{ .allow_deferred_catalog = true, .execution_deadline_ns = normalized.deadline_ns, .cancellation = normalized.cancellation });
        defer lease.deinit();
        var response = try lease.owner().runtimeStatusJson(table);
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(runtime_status.LocalTableRuntimeStatus, alloc, response.bytes(), .{});
        defer parsed.deinit();
        var observed = try parsed.value.clone(alloc);
        errdefer observed.deinit(alloc);
        observed.group_id = group;
        observed.metadata.lsm_root_generation = lease.entry.generation;
        try context.ensureActive();
        return observed;
    }

    fn localRuntimeStatusGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
    ) !?runtime_status.LocalTableRuntimeStatus {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = (try self.acquireIfPresent(group_id, table_name)) orelse return null;
        defer lease.deinit();
        var response = try lease.owner().runtimeStatusJson(table_name);
        defer response.deinit();
        var parsed = try std.json.parseFromSlice(
            runtime_status.LocalTableRuntimeStatus,
            alloc,
            response.bytes(),
            .{},
        );
        defer parsed.deinit();
        var observed = try parsed.value.clone(alloc);
        observed.group_id = group_id;
        observed.metadata.lsm_root_generation = lease.entry.generation;
        return observed;
    }

    const ObservationCancellationDispatch = struct {
        token: db_types.CancellationToken,

        fn requested(ptr: ?*anyopaque) callconv(.c) u8 {
            const self: *const ObservationCancellationDispatch = @ptrCast(@alignCast(ptr orelse return 0));
            return @intFromBool(self.token.isCancelled());
        }
    };

    fn observedDynamicFieldCapabilitySets(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        table_name: []const u8,
        observation: table_reads.DynamicFieldObservationQuery,
    ) !?[]table_reads.ObservedDynamicFieldCapabilitySet {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const group_ids = try table_catalog.resolveGroupsForSpan(
            alloc,
            self.catalog,
            table_name,
            "",
            "",
        );
        defer alloc.free(group_ids);
        if (group_ids.len == 0) return null;

        const request_json = try table_reads.encodeStorageKernelDynamicFieldObservationRequest(alloc, observation);
        defer alloc.free(request_json);
        var cancellation = ObservationCancellationDispatch{
            .token = observation.cancellation orelse .none,
        };
        var merged = std.ArrayListUnmanaged(table_reads.ObservedDynamicFieldCapabilitySet).empty;
        errdefer {
            for (merged.items) |*set| set.deinit(alloc);
            merged.deinit(alloc);
        }

        for (group_ids) |group_id| {
            // Observation is best effort and must not map a cold text index.
            // Query admission owns the warm-and-retry protocol and installs a
            // resident owner whose validated coverage remains visible to the
            // subsequent status read.
            var lease = (try self.acquireIfPresent(group_id, table_name)) orelse
                return error.StorageReadTemporarilyUnavailable;
            defer lease.deinit();
            var response = try lease.owner().observedDynamicFieldCapabilitySetsJson(
                table_name,
                request_json,
                observation.execution_deadline_ns,
                if (observation.cancellation != null) @ptrCast(&cancellation) else null,
                if (observation.cancellation != null) ObservationCancellationDispatch.requested else null,
            );
            defer response.deinit();
            var parsed = try std.json.parseFromSlice(
                []table_reads.ObservedDynamicFieldCapabilitySet,
                alloc,
                response.bytes(),
                .{},
            );
            defer parsed.deinit();
            for (parsed.value) |set| try table_reads.mergeObservedDynamicFieldCapabilitySet(alloc, &merged, set);
        }
        return try merged.toOwnedSlice(alloc);
    }

    fn textMemoryAttributionStatsBestEffort(
        ptr: *anyopaque,
    ) text_memory.TextMemoryAttributionStats {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        const limit = self.maintenanceEntryLimit(true) orelse return .{};
        var cursor: usize = 0;
        var result: text_memory.TextMemoryAttributionStats = .{};
        while (self.nextMaintenanceLease(&cursor, limit, true, false)) |borrowed| {
            var lease = borrowed;
            defer lease.deinit();
            var response = lease.owner().textMemoryJson(lease.entry.table_name) catch continue;
            defer response.deinit();
            var parsed = std.json.parseFromSlice(
                text_memory.TextMemoryAttributionStats,
                self.alloc,
                response.bytes(),
                .{},
            ) catch continue;
            defer parsed.deinit();
            result.accumulate(parsed.value);
        }
        return result;
    }

    fn sqlMaintenanceGroup(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64, table_name: []const u8, body: []const u8, cancellation: db_types.CancellationToken) !?[]u8 {
        try cancellation.check();
        return sqlMaintenanceGroupLocal(ptr, alloc, group_id, table_name, body);
    }

    fn sqlMaintenanceGroupRouted(ptr: *anyopaque, alloc: std.mem.Allocator, route: metadata_api.CatalogRouteFence, group_id: u64, table_name: []const u8, body: []const u8, cancellation: db_types.CancellationToken) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try cancellation.check();
        var scoped = route;
        scoped.admission_cancellation = cancellation;
        try self.validateRoutedRead(alloc, scoped, group_id, table_name);
        var lease = try self.acquireWithControls(group_id, table_name, .{ .native_route = scoped, .execution_deadline_ns = scoped.admission_deadline_ns, .execution_io = scoped.admission_deadline_io, .cancellation = cancellation });
        defer lease.deinit();
        var response = try lease.owner().sqlMaintenanceJson(table_name, body);
        defer response.deinit();
        try cancellation.check();
        return try alloc.dupe(u8, response.bytes());
    }

    fn sqlMaintenanceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().sqlMaintenanceJson(table_name, body);
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    fn graphMetricMaintenanceGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().graphMetricMaintenanceJson(table_name, body);
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    fn resolverOperationGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?[]u8 {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquire(group_id, table_name);
        defer lease.deinit();
        var response = try lease.owner().resolverOperationJson(table_name, body);
        defer response.deinit();
        return try alloc.dupe(u8, response.bytes());
    }

    fn textStatsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        return textStatsGroupLocalAdmitted(ptr, alloc, group_id, table_name, body, null);
    }

    fn textStatsGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        route: ?metadata_api.CatalogRouteFence,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.{}, route));
        defer lease.deinit();
        var response = try lease.owner().textStatsJson(table_name, body);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn algebraicPartialsGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
    ) !?query_response.QueryResponse {
        return algebraicPartialsGroupLocalAdmitted(ptr, alloc, group_id, table_name, body, null);
    }

    fn algebraicPartialsGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        body: []const u8,
        route: ?metadata_api.CatalogRouteFence,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.{}, route));
        defer lease.deinit();
        var response = try lease.owner().algebraicPartialsJson(table_name, body);
        defer response.deinit();
        return .{ .json = try alloc.dupe(u8, response.bytes()) };
    }

    fn graphExpandGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphExpandResponse {
        return graphExpandGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, null);
    }

    fn graphExpandGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphExpandRequest,
        consistency: read_gate.ReadConsistency,
        route: ?metadata_api.CatalogRouteFence,
    ) !?distributed_graph.GraphExpandResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        if (!nativeReadRoute(route)) try self.prepareGraphExpandRead(alloc, group_id, controlled, consistency);
        const request_json = try distributed_graph.encodeGraphExpandRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.from(controlled), route));
        defer lease.deinit();
        var cancellation = req.cancellation;
        var response = try lease.owner().graphExpandJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (cancellation != null) @ptrCast(&cancellation.?) else null,
            if (cancellation != null) cancellationTokenRequested else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphExpandResponse(alloc, response.bytes());
    }

    fn graphHydrateGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphHydrateResponse {
        return graphHydrateGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, null);
    }

    fn graphHydrateGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphHydrateRequest,
        consistency: read_gate.ReadConsistency,
        route: ?metadata_api.CatalogRouteFence,
    ) !?distributed_graph.GraphHydrateResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        if (!nativeReadRoute(route)) try self.prepareQueryRead(group_id, table_reads.graphHydrateSearchRequest(controlled), consistency);
        const request_json = try distributed_graph.encodeGraphHydrateRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.from(controlled), route));
        defer lease.deinit();
        var cancellation = req.cancellation;
        var response = try lease.owner().graphHydrateJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (cancellation != null) @ptrCast(&cancellation.?) else null,
            if (cancellation != null) cancellationTokenRequested else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphHydrateResponse(alloc, response.bytes());
    }

    fn graphEdgesGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: read_gate.ReadConsistency,
    ) !?distributed_graph.GraphEdgesResponse {
        return graphEdgesGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, null);
    }

    fn graphEdgesGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: distributed_graph.GraphEdgesRequest,
        consistency: read_gate.ReadConsistency,
        route: ?metadata_api.CatalogRouteFence,
    ) !?distributed_graph.GraphEdgesResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        try table_catalog.validateTopologyEpoch(alloc, self.catalog, table_name, req.topology_epoch);
        var controlled = req;
        controlled.topology_epoch = 0;
        controlled.execution_deadline_ns = req.execution_deadline_ns orelse distributed_graph.executionDeadlineFromTimeoutMs(req.timeout_ms);
        if (!nativeReadRoute(route)) try self.prepareLookupRead(group_id, req.key, .{}, consistency);
        const request_json = try distributed_graph.encodeGraphEdgesRequest(alloc, controlled);
        defer alloc.free(request_json);
        var lease = try self.acquireWithControls(group_id, table_name, controlsForReadRoute(.from(controlled), route));
        defer lease.deinit();
        var cancellation = req.cancellation;
        var response = try lease.owner().graphEdgesJson(
            table_name,
            request_json,
            controlled.execution_deadline_ns,
            if (cancellation != null) @ptrCast(&cancellation.?) else null,
            if (cancellation != null) cancellationTokenRequested else null,
        );
        defer response.deinit();
        return try distributed_graph.parseGraphEdgesResponse(alloc, response.bytes());
    }

    fn cancellationTokenRequested(ctx: ?*anyopaque) callconv(.c) u8 {
        const token: *const db_types.CancellationToken = @ptrCast(@alignCast(ctx orelse return 0));
        return @intFromBool(token.isCancelled());
    }

    fn queryGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?query_response.QueryResponse {
        return queryGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, null);
    }

    fn queryGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        route: ?metadata_api.CatalogRouteFence,
    ) !?query_response.QueryResponse {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        var response = try self.executeQuery(alloc, group_id, table_name, req, consistency, false, route);
        defer response.deinit();
        return .{
            .json = try alloc.dupe(u8, response.bytes()),
            .identity_read_generation = response.identityReadGeneration(),
        };
    }

    fn searchResultGroupLocal(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
    ) !?db_types.SearchResult {
        return searchResultGroupLocalAdmitted(ptr, alloc, group_id, table_name, req, consistency, null);
    }

    fn searchResultGroupLocalAdmitted(
        ptr: *anyopaque,
        alloc: std.mem.Allocator,
        group_id: u64,
        table_name: []const u8,
        req: db_types.SearchRequest,
        consistency: read_gate.ReadConsistency,
        route: ?metadata_api.CatalogRouteFence,
    ) !?db_types.SearchResult {
        const self: *ProvisionedKernelOwnerSource = @ptrCast(@alignCast(ptr));
        // This is a raw shard phase. The coordinator owns its aggregation;
        // queryGroupLocal instead requests a complete local response.
        var response = try self.executeQuery(alloc, group_id, table_name, req, consistency, true, route);
        defer response.deinit();
        var result = try table_reads.parseStorageKernelSearchResult(alloc, response.bytes());
        errdefer result.deinit();
        try table_reads.restoreDocumentAuthority(alloc, alloc, group_id, req.document_authority_mode, &result);
        result.identity_read_generation = response.identityReadGeneration();
        if (req.identity_read_generation) |expected| {
            if (result.identity_read_generation != expected)
                return error.IdentityReadGenerationChanged;
        }
        return result;
    }
};

test "SQL relation lifetime retirement borrows only existing debt image during drain and cannot publish clean early" {
    try qualifyNativeDebtRetirement(false);
}

test "SQL relation lifetime bounded shutdown abandons idle native debt without clean proof or cold journal deletion" {
    try qualifyNativeDebtRetirement(true);
}

fn qualifyNativeDebtRetirement(abandon_at_deadline: bool) !void {
    const services = @import("../storage/kernel_runtime_services.zig");
    const P = @import("antfly_local_sources").common_topology_records.RelationPersistence;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(parent);
    const path = try std.fmt.allocPrint(a, "{s}/draining-debt", .{parent});
    defer a.free(path);
    var io = std.testing.io;
    const borrow = services.executor.Borrow.init(&io);
    var context: client.Context = .{};
    try std.testing.expectEqual(abi.Status.ok, services.antfly_storage_context_create_with_runtime(&.{ .io = &borrow }, &context.handle));
    defer context.deinit();
    const instance = (try context.metrics()).execution_instance;
    var source = ProvisionedKernelOwnerSource.init(a, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    _ = source.withStorageContextHandle(context.handle);
    defer source.deinit();
    try source.admitRelationHost(.{ .node_id = 9, .store_id = 11, .execution_instance = instance, .generation_instance = instance, .data_generation = 1 }, .{ .instance = instance, .unclean_restart = false });
    const descriptor: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 7, .shard_id = 172, .range_id = 272 },
        .sql_persistence = P.Descriptor{ .kind = .temporary, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = instance, .data_generation = 1, .owner = .{ .issuer_node_id = 9, .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = instance, .connection_generation = 1, .principal_digest = @splat(3) } },
    };
    var initialize = try source.acquireDescriptorWithMode(172, "rows", path, descriptor, true, .transient, .{ .native_initialization = true });
    const binding = try initialize.owner().nativeRelationBinding(a, "rows");
    initialize.requestTransientRetirement();
    initialize.deinit();
    var lease = try source.acquireDescriptorWithMode(172, "rows", path, descriptor, false, .transient, .{ .native_initialized = binding });
    defer lease.deinit();
    const now = platform_time.realtimeNs();
    const external_owner: @import("antfly_local_sources").storage_transactions.ExternalDecisionOwner = .{
        .metadata_incarnation = @splat('a'),
        .operation_id = @splat(1),
        .admission_digest = @splat(2),
        .expires_at_unix_ns = now + std.time.ns_per_min,
    };
    const txn_id = "01010101010101010101010101010101";
    // The real dispatcher requires the exact table-scoped retained participant,
    // shared by BEGIN and its terminal acknowledgement.
    const participant = try @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroup(a, "rows", 172);
    defer a.free(participant);
    const begin = try std.json.Stringify.valueAlloc(a, .{ ._transaction = .{
        .phase = "external_begin_v1",
        .txn_id = txn_id,
        .begin_timestamp = now,
        .created_at_ns = now,
        .topology_epoch = @as(u64, 0),
        .retain_terminal = true,
        .participants = &.{participant},
        .external_decision_owner = external_owner,
    }, .sync_level = "write" }, .{});
    defer a.free(begin);
    var begun = try lease.owner().replicatedBatchJson("rows", begin);
    begun.deinit();
    lease.requestTransientRetirement();
    lease.deinit();
    try std.testing.expectEqual(@as(usize, 1), source.entries.items.len);
    try std.testing.expectEqual(@as(anyerror, error.SqlRelationRecoveryIncomplete), source.entries.items[0].close_failure.?);
    const pending = try source.drainRelationHosts();
    try std.testing.expectEqual(@as(u64, 1), pending.live_owners);
    try std.testing.expect(std.mem.allEqual(u8, &pending.clean_digest, 0));
    if (abandon_at_deadline) {
        const deadline = @import("../common/runtime_lifecycle.zig").ShutdownDeadline.afterMillisecondsWithIo(std.testing.io, 0);
        try std.testing.expectEqual(ProvisionedKernelOwnerSource.QuiesceOutcome.abandoned, try source.quiesceWithDeadline(std.testing.io, deadline));
        try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
        const dirty = try context.relationLifetime(.observe, instance);
        try std.testing.expectEqual(@as(u64, 0), dirty.live_owners);
        try std.testing.expect(dirty.failed_closes != 0);
        try std.testing.expect(std.mem.allEqual(u8, &dirty.clean_digest, 0));
        // Even an idempotent second shutdown must preserve abandonment, not
        // turn an empty registry into permission to publish a clean Boot.
        try std.testing.expectEqual(ProvisionedKernelOwnerSource.QuiesceOutcome.abandoned, try source.quiesceWithDeadline(std.testing.io, deadline));
        var cold_context: client.Context = .{};
        try cold_context.ensure();
        defer cold_context.deinit();
        const inspected = try inspectShutdownNativeImage(a, &cold_context, path, descriptor.sql_persistence.?);
        try std.testing.expect(inspected.root_present and inspected.primary_present and inspected.catalog_present);
        try std.testing.expectEqual(binding.physical_root_incarnation, inspected.physical_root_incarnation.?);
        try std.testing.expect(inspected.descriptor.?.eql(descriptor.sql_persistence.?));
        try std.testing.expect(inspected.has_transaction_state == true);
        return;
    }
    var retained = try source.acquireExistingNativeRetirement(172, "rows", descriptor);
    defer retained.deinit();
    const captured = try retained.owner().nativeRelationBinding(a, "rows");
    try std.testing.expectEqual(binding.physical_root_incarnation, captured.physical_root_incarnation);
    var wrong = descriptor;
    wrong.identity.range_id += 1;
    try std.testing.expectError(error.SqlRelationOwnerUnavailable, source.acquireExistingNativeRetirement(172, "rows", wrong));
    try std.testing.expectError(error.SqlRelationOwnerUnavailable, source.acquireExistingNativeRetirement(173, "rows", descriptor));
    try std.testing.expectEqual(@as(usize, 1), source.entries.items.len);
    const resolve = try std.json.Stringify.valueAlloc(a, .{ ._transaction = .{
        .phase = "external_resolve_v1",
        .txn_id = txn_id,
        .status = "aborted",
        .commit_version = now + 1,
        .external_decision_owner = external_owner,
    }, .sync_level = "write" }, .{});
    defer a.free(resolve);
    var resolved = try retained.owner().replicatedBatchJson("rows", resolve);
    resolved.deinit();
    const ack = try std.json.Stringify.valueAlloc(a, .{ ._transaction = .{
        .phase = "external_acknowledge_v1",
        .txn_id = txn_id,
        .participant = participant,
        .external_decision_owner = external_owner,
    }, .sync_level = "write" }, .{});
    defer a.free(ack);
    var acknowledged = try retained.owner().replicatedBatchJson("rows", ack);
    acknowledged.deinit();
    // Even after debt is discharged, this genuine held maintenance lease keeps
    // the global clean marker unavailable until the entire source control ends.
    const held = try source.drainRelationHosts();
    try std.testing.expectEqual(@as(u64, 1), held.live_owners);
    try std.testing.expect(std.mem.allEqual(u8, &held.clean_digest, 0));
    retained.deinit();
    if (source.entries.items.len != 0) source.entries.items[0].close_retry_at_ns = 0;
    const clean = try source.drainRelationHosts();
    try std.testing.expectEqual(@as(u64, 0), clean.live_owners);
    try std.testing.expect(!std.mem.allEqual(u8, &clean.clean_digest, 0));
    try std.testing.expectError(error.SqlRelationOwnerUnavailable, source.acquireExistingNativeRetirement(172, "rows", descriptor));
}

test "volatile source failed close retains its owner and bounds foreground and drain retries" {
    try qualifyVolatileFailedClose(false);
}

test "SQL relation lifetime bounded shutdown refuses producers and live leases before abandoning an actual failed close" {
    try qualifyVolatileFailedClose(true);
}

fn inspectShutdownNativeImage(a: std.mem.Allocator, context: *client.Context, path: []const u8, descriptor: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor) !@import("antfly_local_sources").common_topology_records.RelationPersistence.UnpublishedImageInspection {
    const encoded = try std.json.Stringify.valueAlloc(a, @as(?@TypeOf(descriptor), descriptor), .{});
    defer a.free(encoded);
    var output: abi.OwnedBytes = .{};
    try kernel_error_identity.statusToError(abi.antfly_storage_owner_hidden_restore_json(null, &.{ .operation = .inspect_native_root, .context = context.handle, .path = .fromSlice(path), .native_inspection_json = .fromSlice(encoded) }, &output));
    defer abi.antfly_storage_owner_buffer_destroy(&output);
    var inspected = try std.json.parseFromSlice(@import("antfly_local_sources").common_topology_records.RelationPersistence.UnpublishedImageInspection, a, output.slice(), .{});
    defer inspected.deinit();
    return inspected.value;
}

fn qualifyVolatileFailedClose(abandon_at_deadline: bool) !void {
    const services = @import("../storage/kernel_runtime_services.zig");
    const P = @import("antfly_local_sources").common_topology_records.RelationPersistence;
    const Fault = struct {
        var armed: std.atomic.Value(bool) = .init(false);
        var failures: std.atomic.Value(u32) = .init(0);
        fn createFile(userdata: ?*anyopaque, dir: std.Io.Dir, name: []const u8, options: std.Io.Dir.CreateFileOptions) std.Io.File.OpenError!std.Io.File {
            if (armed.load(.acquire)) {
                _ = failures.fetchAdd(1, .monotonic);
                return error.NoSpaceLeft;
            }
            return std.testing.io.vtable.dirCreateFile(userdata, dir, name, options);
        }
    };
    Fault.armed.store(false, .release);
    Fault.failures.store(0, .release);
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(parent);
    const path = try std.fmt.allocPrint(a, "{s}/source-volatile", .{parent});
    defer a.free(path);
    var io_vtable = std.testing.io.vtable.*;
    io_vtable.dirCreateFile = Fault.createFile;
    var io = std.testing.io;
    io.vtable = &io_vtable;
    const borrow = services.executor.Borrow.init(&io);
    var context: client.Context = .{};
    try std.testing.expectEqual(abi.Status.ok, services.antfly_storage_context_create_with_runtime(&.{ .io = &borrow }, &context.handle));
    defer context.deinit();
    const instance = (try context.metrics()).execution_instance;
    var source = ProvisionedKernelOwnerSource.init(a, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    _ = source.withStorageContextHandle(context.handle);
    defer {
        Fault.armed.store(false, .release);
        source.deinit();
    }
    try source.admitRelationHost(.{ .node_id = 9, .store_id = 11, .execution_instance = instance, .generation_instance = instance, .data_generation = 1 }, .{ .instance = instance, .unclean_restart = false });
    const descriptor: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 7, .shard_id = 172, .range_id = 272 },
        .sql_persistence = P.Descriptor{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = instance, .data_generation = 1 },
    };
    const Probe = struct {
        count: u32 = 0,
        source: *ProvisionedKernelOwnerSource,
        attempt_publication: bool = false,
        publication_error: ?anyerror = null,
        fn beforeClose(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.count += 1;
            if (!self.attempt_publication) return;
            // Owner destruction drops the registry lock to join workers.
            // A late physical publisher must still see closed admission.
            var publication = self.source.snapshotSource().beginPublication(.{ .group_id = 175, .table_name = "late-publisher", .io = std.testing.io, .drain_timeout_ns = 0 }) catch |err| {
                self.publication_error = err;
                return;
            };
            publication.deinit();
        }
    };
    var probe: Probe = .{ .source = &source };
    source.test_apply_control_hooks = .{ .ptr = &probe, .before_close = Probe.beforeClose };
    // Establish the real durable primary binding before any row operation.
    var initialize = try source.acquireDescriptorWithMode(172, "rows", path, descriptor, true, .transient, .{ .native_initialization = true });
    const initial_binding = try initialize.owner().nativeRelationBinding(a, "rows");
    initialize.requestTransientRetirement();
    initialize.deinit();
    probe.count = 0;
    const controls: ProvisionedKernelOwnerSource.ReadControls = .{ .native_initialized = initial_binding };
    var lease = try source.acquireDescriptorWithMode(172, "rows", path, descriptor, false, .transient, controls);
    defer lease.deinit();
    var written = try lease.owner().batchJson("rows", "{\"inserts\":{\"a\":{\"id\":1}},\"sync_level\":\"write\"}");
    written.deinit();
    if (abandon_at_deadline) {
        // All three outstanding lifetimes veto abandonment independently.
        // The live lease is a real compiled owner; no handle may be freed.
        try source.beginNativeInitialization(173, "cold");
        var initializer_active = true;
        defer if (initializer_active) source.endNativeInitialization(173);
        var publication = try source.snapshotSource().beginPublication(.{ .group_id = 174, .table_name = "publishing", .io = std.testing.io });
        defer publication.deinit();
        const expired = @import("../common/runtime_lifecycle.zig").ShutdownDeadline.afterMillisecondsWithIo(std.testing.io, 0);
        try std.testing.expectError(error.ShutdownTimeout, source.quiesceWithDeadline(std.testing.io, expired));
        try std.testing.expectEqual(@as(usize, 1), source.entries.items.len);
        try std.testing.expectEqual(initial_binding.physical_root_incarnation, (try lease.owner().nativeRelationBinding(a, "rows")).physical_root_incarnation);
        source.endNativeInitialization(173);
        initializer_active = false;
        try std.testing.expectError(error.ShutdownTimeout, source.quiesceWithDeadline(std.testing.io, expired));
        publication.deinit();
        try std.testing.expectError(error.ShutdownTimeout, source.quiesceWithDeadline(std.testing.io, expired));
        try std.testing.expect(!source.shutdown_abandoned);
        Fault.armed.store(true, .release);
        lease.requestTransientRetirement();
        lease.deinit();
        try std.testing.expectEqual(@as(usize, 1), source.entries.items.len);
        try std.testing.expect(source.entries.items[0].close_failure_reported);
        source.entries.items[0].close_retry_at_ns = std.math.maxInt(u64);
        const elapsed_start = std.Io.Timestamp.now(std.testing.io, .awake);
        const deadline = @import("../common/runtime_lifecycle.zig").ShutdownDeadline.afterMillisecondsWithIo(std.testing.io, 5);
        probe.attempt_publication = true;
        try std.testing.expectEqual(ProvisionedKernelOwnerSource.QuiesceOutcome.abandoned, try source.quiesceWithDeadline(std.testing.io, deadline));
        try std.testing.expectEqual(@as(anyerror, error.Canceled), probe.publication_error orelse return error.PublicationEnteredDuringShutdown);
        try std.testing.expectEqual(@as(usize, 0), source.publications.items.len);
        try std.testing.expectError(error.Canceled, source.snapshotSource().beginPublication(.{ .group_id = 175, .table_name = "late-publisher", .io = std.testing.io }));
        try std.testing.expect(deadline.expired());
        try std.testing.expect(std.Io.Timestamp.now(std.testing.io, .awake).toNanoseconds() >= elapsed_start.toNanoseconds());
        try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
        const dirty = try context.relationLifetime(.begin_drain, instance);
        try std.testing.expect(dirty.failed_closes != 0);
        try std.testing.expect(std.mem.allEqual(u8, &dirty.clean_digest, 0));
        Fault.armed.store(false, .release);
        var cold_context: client.Context = .{};
        try cold_context.ensure();
        defer cold_context.deinit();
        const inspected = try inspectShutdownNativeImage(a, &cold_context, path, descriptor.sql_persistence.?);
        try std.testing.expect(inspected.root_present and inspected.primary_present and inspected.catalog_present);
        try std.testing.expectEqual(initial_binding.physical_root_incarnation, inspected.physical_root_incarnation.?);
        try std.testing.expect(inspected.descriptor.?.eql(descriptor.sql_persistence.?));
        return;
    }
    Fault.armed.store(true, .release);
    lease.requestTransientRetirement();
    lease.deinit();
    try std.testing.expectEqual(@as(usize, 1), source.entries.items.len);
    try std.testing.expect(source.entries.items[0].close_failure_reported);
    try std.testing.expectEqual(@as(u32, 1), probe.count);
    try std.testing.expectError(error.SqlRelationOwnerUnavailable, source.acquireExistingNativeRetirement(172, "rows", descriptor));
    source.entries.items[0].close_retry_at_ns = std.math.maxInt(u64);
    const failed_before = Fault.failures.load(.acquire);
    try std.testing.expect(failed_before > 0);
    try std.testing.expectError(error.SqlRelationOwnerUnavailable, source.acquireDescriptorWithMode(172, "rows", path, descriptor, false, .transient, controls));
    try std.testing.expectEqual(failed_before, Fault.failures.load(.acquire));
    source.entries.items[0].close_retry_at_ns = 0;
    const pending = try source.drainRelationHosts();
    try std.testing.expectEqual(@as(u64, 1), pending.live_owners);
    try std.testing.expectEqual(@as(usize, 1), source.entries.items.len);
    try std.testing.expectEqual(@as(u32, 2), probe.count);
    try std.testing.expect(Fault.failures.load(.acquire) > failed_before);
    Fault.armed.store(false, .release);
    source.entries.items[0].close_retry_at_ns = 0;
    const clean = try source.drainRelationHosts();
    try std.testing.expectEqual(@as(u32, 3), probe.count);
    try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
    try std.testing.expectEqual(@as(u64, 0), clean.live_owners);
    try std.testing.expectEqual(@as(u64, 0), clean.failed_closes);
    try std.testing.expect(!std.mem.allEqual(u8, &clean.clean_digest, 0));

    // Exercise the real registry factory on a new process context. Mutable
    // descriptor configuration must never reach its binding-only recovery open.
    var next_context: client.Context = .{};
    try next_context.ensure();
    defer next_context.deinit();
    const next_instance = (try next_context.metrics()).execution_instance;
    var next_source = ProvisionedKernelOwnerSource.init(a, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    _ = next_source.withStorageContextHandle(next_context.handle);
    defer next_source.deinit();
    const host: P.Context = .{ .node_id = 9, .store_id = 11, .execution_instance = next_instance, .generation_instance = instance, .data_generation = 1 };
    const recovery_boot: P.Boot = .{ .instance = next_instance, .unclean_restart = false, .adoption = .{
        .before = descriptor.sql_persistence.?,
        .metadata_group_id = 1,
        .boot_generation = 2,
        .table_id = 7,
        .shard_id = 172,
        .range_id = 272,
        .physical_root_incarnation = initial_binding.physical_root_incarnation,
    } };
    var mutable_descriptor = descriptor;
    mutable_descriptor.schema_json = "not a schema";
    mutable_descriptor.indexes_json = "not an index";
    mutable_descriptor.restore_bootstrap_json = "not a bootstrap";
    mutable_descriptor.initial_child_bootstrap_json = "not a bootstrap";
    mutable_descriptor.table_storage = .{ .dense_embeddings = .primary_lsm };
    mutable_descriptor.initial_range = .{ .start = "left", .end = "right" };
    var recovery = try next_source.acquireDescriptorWithMode(172, "rows", path, mutable_descriptor, true, .transient, .{
        .native_adoption = .{ .host = host, .boot = recovery_boot },
        .allow_deferred_catalog = true,
    });
    const adopted_binding = try recovery.owner().nativeRelationBinding(a, "rows");
    try std.testing.expect(adopted_binding.descriptor.eql(descriptor.sql_persistence.?));
    try std.testing.expectEqual(next_instance, adopted_binding.native_instance);
    try std.testing.expectEqual(initial_binding.physical_root_incarnation, adopted_binding.physical_root_incarnation);
    try std.testing.expectError(error.InvalidArgument, recovery.owner().lookupJson("rows", "{\"key\":\"a\"}"));
    recovery.requestTransientRetirement();
    recovery.deinit();
    try std.testing.expectEqual(@as(usize, 0), next_source.entries.items.len);
    try next_source.admitRelationHost(host, .{ .instance = next_instance, .unclean_restart = false });
    var reopened = try next_source.acquireDescriptorWithMode(172, "rows", path, descriptor, false, .transient, .{ .native_initialized = adopted_binding });
    defer reopened.deinit();
    var row = try reopened.owner().lookupJson("rows", "{\"key\":\"a\"}");
    defer row.deinit();
    try std.testing.expect(std.mem.indexOf(u8, row.bytes(), "1") != null);
}

test "compiled owner coordinated ttl admission preserves exact observations and pressure" {
    const ttl = @import("../storage/server_coordinated_ttl.zig");
    const Fake = struct {
        calls: usize = 0,
        pressure: bool = false,
        fn enqueue(ptr: *anyopaque, request: ttl.Request) !u32 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.pressure) return error.CoordinatedTtlBackpressure;
            try std.testing.expectEqual(@as(u64, 17), request.table_id);
            try std.testing.expectEqual(@as(u64, 23), request.group_id);
            try std.testing.expectEqual(@as(u32, 3), request.schema_version);
            try std.testing.expectEqualStrings("expires", request.ttl_field);
            try std.testing.expectEqual(@as(usize, 1), request.candidates.len);
            try std.testing.expectEqualStrings("\x00\xffkey", request.candidates[0].key);
            try std.testing.expectEqual(@as(u64, 99), request.candidates[0].row_version);
            try std.testing.expectEqual(@as(u64, 44), request.candidates[0].ttl_timestamp_ns);
            try std.testing.expectEqual(@as([32]u8, @splat(0xa7)), request.candidates[0].expected_content_digest);
            self.calls += 1;
            return 0;
        }
    };
    var fake = Fake{};
    var source: ProvisionedKernelOwnerSource = undefined;
    source.coordinated_ttl = .{ .ptr = &fake, .expire_fn = Fake.enqueue };
    const candidates = [_]abi.CoordinatedTtlCandidate{.{ .key = .fromSlice("\x00\xffkey"), .row_version = 99, .ttl_timestamp_ns = 44, .expected_content_digest = @splat(0xa7) }};
    var request = abi.CoordinatedTtlRequest{ .table_id = 17, .group_id = 23, .schema_version = 3, .ttl_duration_ns = 100, .ttl_field = .fromSlice("expires"), .observed_at_unix_ns = 200, .grace_period_ns = 1, .candidates = &candidates, .candidate_count = 1 };
    try std.testing.expectEqual(@as(u8, 0), ProvisionedKernelOwnerSource.enqueueCoordinatedTtl(&source, &request));
    fake.pressure = true;
    try std.testing.expectEqual(@as(u8, 1), ProvisionedKernelOwnerSource.enqueueCoordinatedTtl(&source, &request));
    fake.pressure = false;
    request.candidate_count = abi.coordinated_ttl_page_capacity + 1;
    try std.testing.expectEqual(@as(u8, 1), ProvisionedKernelOwnerSource.enqueueCoordinatedTtl(&source, &request));
    request.candidate_count = 1;
    request.candidates = null;
    try std.testing.expectEqual(@as(u8, 1), ProvisionedKernelOwnerSource.enqueueCoordinatedTtl(&source, &request));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
}

test "storage owner quiesce drains leases and promotion callbacks before context destruction" {
    const alloc = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("owner-quiesce");
    defer directory.cleanup();
    const path = std.mem.span(directory.path().ptr);
    var source = ProvisionedKernelOwnerSource.init(alloc, path, table_catalog.emptyCatalogSource(), read_gate.unavailableReadSafetyBarrier());
    defer source.deinit();
    const Callback = struct {
        entered: std.atomic.Value(bool) = .init(false),
        released: std.atomic.Value(bool) = .init(false),
        fn isLeader(ptr: *anyopaque, _: u64) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.released.load(.acquire)) std.testing.io.sleep(.fromMilliseconds(1), .awake) catch {};
            return false;
        }
    };
    var callback = Callback{};
    // Release on any failed assertion before source.deinit joins the worker.
    defer callback.released.store(true, .release);
    _ = source.withRuntimeHooks(null, null, .{ .ptr = &callback, .vtable = &.{ .is_local_leader = Callback.isLeader } });
    const descriptor = descriptor_contract.Descriptor{
        .lsm_root_generation = 0,
        .identity = .{ .table_id = 7, .shard_id = 7001, .range_id = 7001 },
        .indexes_json =
        \\{"relations_graph":{"type":"graph","source":{"artifact":"relations_v1","path":"$.relations[*]","format":"extraction_relation"},"artifact":{"name":"relations_v1","kind":"asset","source":{"type":"field","value":"relations"},"content_type":"application/json"},"resolvers":[{"name":"kg","table":"entities","source_artifact":"relations_v1","resolution_artifact":"resolution_v1","key_template":"{{ lower _entity.label }}/{{ slug _entity.text }}","config_generation":1,"_antfly_destination_authorization_v1":{"principal":"service:auth-disabled","signature":"auth-disabled","destinations":["entities"]}}]}}
        ,
    };
    var lease = try source.acquireDescriptor(7001, "docs", path, descriptor);
    var lease_active = true;
    defer if (lease_active) lease.deinit();
    errdefer callback.released.store(true, .release);
    var response = try lease.owner().batchJson("docs",
        \\{"inserts":{"a":{"relations":{"entities":[{"id":"e0","label":"person","text":"Ada"}]}}},"sync_level":"write"}
    );
    response.deinit();
    const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!callback.entered.load(.acquire)) {
        if (platform_time.monotonicNs() >= deadline) return error.PromotionCallbackDidNotStart;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    const Shutdown = struct {
        source: *ProvisionedKernelOwnerSource,
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            self.source.quiesce(std.testing.io) catch |err| {
                self.err = err;
            };
            self.done.store(true, .release);
        }
    };
    var shutdown = Shutdown{ .source = &source };
    var shutdown_task = try std.testing.io.concurrent(Shutdown.run, .{&shutdown});
    defer {
        callback.released.store(true, .release);
        if (lease_active) {
            lease.deinit();
            lease_active = false;
        }
        shutdown_task.await(std.testing.io);
    }
    while (true) {
        ProvisionedKernelOwnerSource.lock(&source.mutex);
        const quiescing = source.quiescing;
        source.mutex.unlock();
        if (quiescing) break;
        if (platform_time.monotonicNs() >= deadline) return error.QuiesceDidNotStart;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!shutdown.done.load(.acquire));
    try std.testing.expectError(error.Canceled, source.acquireDescriptor(7001, "docs", path, descriptor));
    // Closing the last lease must wait for the autonomous callback as well.
    var release_task = try std.testing.io.concurrent(ProvisionedKernelOwnerSource.Lease.deinit, .{&lease});
    lease_active = false;
    defer {
        callback.released.store(true, .release);
        release_task.await(std.testing.io);
    }
    while (true) {
        ProvisionedKernelOwnerSource.lock(&source.mutex);
        const closing = source.entries.items.len == 1 and source.entries.items[0].closing;
        source.mutex.unlock();
        if (closing) break;
        if (platform_time.monotonicNs() >= deadline) return error.OwnerCloseDidNotStart;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!shutdown.done.load(.acquire));
    callback.released.store(true, .release);
    while (!shutdown.done.load(.acquire)) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    if (shutdown.err) |err| return err;
    try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());
    try source.quiesce(std.testing.io);
    try std.testing.expectError(error.Canceled, source.acquireDescriptor(7001, "docs", path, descriptor));
}

// Direct compiled-owner fixtures have no retained-Ready Raft driver. A
// physical writer yield retries only the identical committed entry; semantic
// errors and the tests' explicit registry-admission negatives remain unchanged.
fn applyCommittedSourceFixtureEntry(source: *ProvisionedKernelOwnerSource, alloc: std.mem.Allocator, group: u64, table: []const u8, descriptor: descriptor_contract.Descriptor, req: db_types.BatchRequest, term: u64, index: u64) !void {
    const deadline = platform_time.monotonicNs() +| 2 * std.time.ns_per_s;
    while (true) {
        source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, group, table, descriptor, req, term, index) catch |err| switch (err) {
            error.RaftApplyWriterUnavailable => {
                if (platform_time.monotonicNs() >= deadline) return err;
                try std.testing.io.sleep(.fromMilliseconds(2), .awake);
                continue;
            },
            else => return err,
        };
        return;
    }
}

test "committed owner apply yields admission conflicts and retries the exact entry once" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    for ([_]enum { registry, exclusive, publication }{ .registry, .exclusive, .publication }) |history| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer alloc.free(root);
        const path = try std.fmt.allocPrint(alloc, "{s}/group-1/table-db", .{root});
        defer alloc.free(path);
        var source = Source.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        };
        try applyCommittedSourceFixtureEntry(&source, alloc, 1, "docs", descriptor, .{
            .writes = &.{.{ .key = "doc:counter", .value = "{\"count\":0}" }},
        }, 1, 1);
        const increment: db_types.BatchRequest = .{ .transforms = &.{.{
            .key = "doc:counter",
            .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
        }} };
        {
            var exclusive: ?Source.Lease = null;
            var publication: ?*Source.PendingPublication = null;
            switch (history) {
                .registry => Source.lock(&source.mutex),
                .exclusive => exclusive = try source.acquireDescriptorExclusive(1, "docs", path, descriptor, .resident),
                .publication => publication = try source.registerPublication(1, "docs"),
            }
            defer switch (history) {
                .registry => source.mutex.unlock(),
                .exclusive => exclusive.?.deinit(),
                .publication => Source.endPublication(&source, publication.?),
            };
            const started = platform_time.monotonicNs();
            try std.testing.expectError(
                if (history == .publication) error.StorageReadTemporarilyUnavailable else error.StorageBusy,
                source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", descriptor, increment, 1, 2),
            );
            // The gate stays held by this test. Waiting for the five-second
            // foreground admission timeout would strand the Raft progress lane.
            try std.testing.expect(platform_time.monotonicNs() - started < std.time.ns_per_s);
        }
        // Publication may first retire the old owner. Bounded progress retries
        // reopen it without abandoning the original term/index identity.
        for (0..4) |_| {
            applyCommittedSourceFixtureEntry(&source, alloc, 1, "docs", descriptor, increment, 1, 2) catch |err| switch (err) {
                error.StorageBusy => continue,
                else => return err,
            };
            break;
        } else return error.TestOwnerAdmissionDidNotRecover;
        try applyCommittedSourceFixtureEntry(&source, alloc, 1, "docs", descriptor, increment, 1, 2);
        var reader = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer reader.deinit();
        var value = try reader.owner().lookupJson("docs", "{\"key\":\"doc:counter\",\"include_all_fields\":true}");
        defer value.deinit();
        try std.testing.expect(std.mem.indexOf(u8, value.bytes(), "\"count\":1") != null);
    }
}

test "committed owner apply never opens or closes storage under the raft apply lock" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-1/table-db", .{root});
    defer alloc.free(path);
    var source = Source.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    const Gate = struct {
        io: std.Io,
        mode: std.atomic.Value(u8) = .init(0),
        cold_entered: std.Io.Event = .unset,
        cold_release: std.Io.Event = .unset,
        close_entered: std.Io.Event = .unset,
        close_release: std.Io.Event = .unset,
        raft_mutex: std.atomic.Mutex = .unlocked,

        fn beforeOpen(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.mode.load(.acquire) != 0) return;
            self.cold_entered.set(self.io);
            self.cold_release.waitUncancelable(self.io);
        }

        fn beforeClose(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.mode.load(.acquire) != 1) return;
            self.close_entered.set(self.io);
            self.close_release.waitUncancelable(self.io);
            // A recovery worker may propose through this mutex while owner
            // close joins it. The control executor can wait; Raft apply cannot.
            platform_sync.lockYielding(&self.raft_mutex);
            self.raft_mutex.unlock();
        }

        fn applyCommitted(self: *@This(), owner_source: *Source, descriptor: descriptor_contract.Descriptor, batch: db_types.BatchRequest, index: u64) !void {
            const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
            while (true) {
                owner_source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(std.testing.allocator, 1, "docs", descriptor, batch, 1, index) catch |err| switch (err) {
                    error.RaftApplyWriterUnavailable => {
                        if (platform_time.monotonicNs() >= deadline) return error.TestOwnerDidNotAdmit;
                        try self.io.sleep(.fromMilliseconds(1), .awake);
                        continue;
                    },
                    else => return err,
                };
                return;
            }
        }
    };
    var gate = Gate{ .io = io };
    defer {
        gate.cold_release.set(io);
        gate.close_release.set(io);
    }
    source.test_apply_control_hooks = .{ .ptr = &gate, .before_open = Gate.beforeOpen, .before_close = Gate.beforeClose };
    try source.startApplyControl(io);
    const first: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        .schema_json = "{\"version\":0}",
    };
    const second: descriptor_contract.Descriptor = .{
        .lsm_root_generation = first.lsm_root_generation,
        .identity = first.identity,
        .schema_json = "{\"version\":1}",
    };
    const first_batch: db_types.BatchRequest = .{ .writes = &.{.{ .key = "doc:a", .value = "{\"count\":0}" }} };
    const second_batch: db_types.BatchRequest = .{ .transforms = &.{.{
        .key = "doc:a",
        .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
    }} };
    // A bounded admission rejection need not enqueue an open: the registry
    // itself may be busy. Force that history, then retry the same committed
    // entry just as the progress driver does until the control worker enters.
    {
        Source.lock(&source.mutex);
        defer source.mutex.unlock();
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", first, first_batch, 1, 1));
        try std.testing.expectEqual(@as(usize, 0), source.apply_control_pending.items.len);
    }
    const cold_deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!gate.cold_entered.isSet()) {
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", first, first_batch, 1, 1));
        if (platform_time.monotonicNs() >= cold_deadline) return error.TestColdOwnerDidNotQueue;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    // The worker is held before physical open; a repeated committed apply
    // remains a bounded retry and cannot itself open the owner.
    try std.testing.expectError(error.RaftApplyWriterUnavailable, source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", first, first_batch, 1, 1));
    gate.cold_release.set(io);
    try gate.applyCommitted(&source, first, first_batch, 1);
    // A different group's cold open can hold the owner registry while a
    // recovery callback awaits Raft progress. The already-admitted apply
    // lease must be releasable even in that exact lock order.
    const lease_deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    var warm_lease = while (true) {
        const lease = source.acquireApplyOnly(1, "docs", first) catch |err| {
            try std.testing.expect(err == error.RaftApplyWriterUnavailable);
            if (platform_time.monotonicNs() >= lease_deadline) return error.TestWarmOwnerDidNotAdmit;
            try io.sleep(.fromMilliseconds(1), .awake);
            continue;
        };
        break lease;
    };
    const Release = struct {
        lease: *Source.Lease,
        io: std.Io,
        done: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            self.lease.deinit();
            self.done.set(self.io);
        }
    };
    var release = Release{ .lease = &warm_lease, .io = io };
    Source.lock(&source.mutex);
    var registry_held = true;
    defer if (registry_held) source.mutex.unlock();
    var release_task = try io.concurrent(Release.run, .{&release});
    defer release_task.await(io);
    try release.done.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    source.mutex.unlock();
    registry_held = false;
    gate.mode.store(1, .release);
    const Apply = struct {
        source: *Source,
        descriptor: descriptor_contract.Descriptor,
        batch: db_types.BatchRequest,
        gate: *Gate,
        done: std.Io.Event = .unset,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(std.testing.allocator, 1, "docs", self.descriptor, self.batch, 1, 2) catch |err| {
                self.failure = err;
            };
            self.done.set(self.gate.io);
        }
    };
    var apply = Apply{ .source = &source, .descriptor = second, .batch = second_batch, .gate = &gate };
    platform_sync.lockYielding(&gate.raft_mutex);
    var raft_mutex_held = true;
    errdefer if (raft_mutex_held) gate.raft_mutex.unlock();
    var task = try io.concurrent(Apply.run, .{&apply});
    defer {
        if (raft_mutex_held) {
            gate.raft_mutex.unlock();
            raft_mutex_held = false;
        }
        gate.close_release.set(io);
        task.await(io);
    }
    try apply.done.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    try std.testing.expectEqual(@as(?anyerror, error.RaftApplyWriterUnavailable), apply.failure);
    // Retirement admission is also a retry, not proof that this attempt
    // queued the descriptor. Keep Raft's mutex held while advancing the exact
    // entry; physical close still belongs to the independent control worker.
    const close_deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (!gate.close_entered.isSet()) {
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", second, second_batch, 1, 2));
        if (platform_time.monotonicNs() >= close_deadline) return error.TestRetiredOwnerDidNotQueue;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    gate.raft_mutex.unlock();
    raft_mutex_held = false;
    gate.close_release.set(io);
    try gate.applyCommitted(&source, second, second_batch, 2);
    // Idempotency uses the same admission contract: the successful apply wakes
    // release reconciliation, which can briefly contend with this retry.
    try gate.applyCommitted(&source, second, second_batch, 2);
    var lease = try source.acquireDescriptorOnce(1, "docs", path, second, .shared, .resident, .{ .historical_raft_apply = true });
    defer lease.deinit();
    var value = try lease.owner().lookupJson("docs", "{\"key\":\"doc:a\",\"include_all_fields\":true}");
    defer value.deinit();
    try std.testing.expect(std.mem.indexOf(u8, value.bytes(), "\"count\":1") != null);
}

test "owner shutdown joins recovery close while raft progress remains available" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    var source = ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    const Gate = struct {
        io: std.Io,
        raft_mutex: std.atomic.Mutex = .unlocked,
        close_entered: std.Io.Event = .unset,
        close_release: std.Io.Event = .unset,
        fn beforeClose(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.close_entered.set(self.io);
            self.close_release.waitUncancelable(self.io);
            // Simulate transaction recovery reentering the Raft proposal
            // path while the owner close joins that recovery worker.
            platform_sync.lockYielding(&self.raft_mutex);
            self.raft_mutex.unlock();
        }
    };
    var gate = Gate{ .io = io };
    defer gate.close_release.set(io);
    source.test_apply_control_hooks = .{ .ptr = &gate, .before_close = Gate.beforeClose };
    try source.startApplyControl(io);
    const first: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        .schema_json = "{\"version\":0}",
    };
    var second = first;
    second.schema_json = "{\"version\":1}";
    const batch: db_types.BatchRequest = .{ .writes = &.{.{ .key = "doc:a", .value = "{}" }} };
    const deadline = platform_time.monotonicNs() + 10 * std.time.ns_per_s;
    while (true) {
        source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", first, batch, 1, 1) catch |err| switch (err) {
            error.RaftApplyWriterUnavailable => {
                if (platform_time.monotonicNs() >= deadline) return error.TestColdOwnerDidNotOpen;
                try io.sleep(.fromMilliseconds(1), .awake);
                continue;
            },
            else => return err,
        };
        break;
    }
    platform_sync.lockYielding(&gate.raft_mutex);
    var raft_held = true;
    errdefer if (raft_held) gate.raft_mutex.unlock();
    try std.testing.expectError(error.RaftApplyWriterUnavailable, source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", second, batch, 1, 2));
    try gate.close_entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    const Shutdown = struct {
        source: *ProvisionedKernelOwnerSource,
        io: std.Io,
        entered: std.Io.Event = .unset,
        done: std.Io.Event = .unset,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            self.entered.set(self.io);
            self.source.quiesce(self.io) catch |err| {
                self.failure = err;
            };
            self.done.set(self.io);
        }
    };
    var shutdown = Shutdown{ .source = &source, .io = io };
    var task = try io.concurrent(Shutdown.run, .{&shutdown});
    defer {
        if (raft_held) {
            gate.raft_mutex.unlock();
            raft_held = false;
        }
        gate.close_release.set(io);
        task.await(io);
    }
    try shutdown.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    // The shutdown join is waiting, but the Raft mutex can still be released
    // by its live progress driver; the recovery callback then exits.
    gate.raft_mutex.unlock();
    raft_held = false;
    gate.close_release.set(io);
    try shutdown.done.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    if (shutdown.failure) |err| return err;
}

test "historical apply requires the exact restore binding" {
    const source = ProvisionedKernelOwnerSource;
    const restored: @import("../storage/restore_identity.zig").Identity = .{
        .backup_id = "backup",
        .location = "local",
        .snapshot_path = "snapshot",
        .artifact_sha256 = "sha",
    };
    try std.testing.expect(source.restoreBindingMatches(restored, null, true));
    try std.testing.expect(!source.restoreBindingMatches(restored, null, false));
    try std.testing.expect(source.restoreBindingMatches(restored, restored, false));
    try std.testing.expect(!source.restoreBindingMatches(null, restored, false));
}

test "committed catch-up retains newer durable schema across an older pinned descriptor" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-1/table-db", .{root});
    defer alloc.free(path);
    var source = ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    try source.startApplyControl(io);
    const Apply = struct {
        fn retry(source_ptr: *ProvisionedKernelOwnerSource, descriptor: descriptor_contract.Descriptor, batch: db_types.BatchRequest, index: u64) !void {
            const deadline = platform_time.monotonicNs() + 10 * std.time.ns_per_s;
            while (true) {
                source_ptr.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(std.testing.allocator, 1, "docs", descriptor, batch, 1, index) catch |err| switch (err) {
                    error.RaftApplyWriterUnavailable => {
                        if (platform_time.monotonicNs() >= deadline) return error.TestOwnerAdmissionDidNotRecover;
                        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                        continue;
                    },
                    else => return err,
                };
                return;
            }
        }
    };
    const old: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        .schema_json = "{\"version\":0}",
    };
    const current: descriptor_contract.Descriptor = .{
        .lsm_root_generation = old.lsm_root_generation,
        .identity = old.identity,
        .schema_json = "{\"version\":1}",
    };
    const first_batch: db_types.BatchRequest = .{
        .writes = &.{.{ .key = "doc:first", .value = "{\"title\":\"first\"}" }},
    };
    try std.testing.expectError(error.RaftApplyWriterUnavailable, source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(alloc, 1, "docs", old, first_batch, 1, 1));
    try Apply.retry(&source, old, first_batch, 1);
    {
        var lease = try source.acquireDescriptor(1, "docs", path, current);
        lease.deinit();
    }
    // An old Raft entry can be retried after the metadata schema advances.
    // Its already-applied marker prevents duplicate mutation, and historical
    // admission must preserve the newer durable catalog.
    try Apply.retry(&source, old, .{
        .writes = &.{.{ .key = "doc:first", .value = "{\"title\":\"duplicate\"}" }},
    }, 1);
    try Apply.retry(&source, old, .{
        .writes = &.{.{ .key = "doc:second", .value = "{\"title\":\"second\",\"count\":0}" }},
    }, 2);
    const increment: db_types.BatchRequest = .{ .transforms = &.{.{
        .key = "doc:second",
        .operations = &.{.{ .op = .inc, .path = "count", .value_json = "1" }},
    }} };
    try Apply.retry(&source, old, increment, 3);
    try Apply.retry(&source, old, increment, 3);
    var lease = try source.acquireDescriptor(1, "docs", path, current);
    defer lease.deinit();
    var first = try lease.owner().lookupJson("docs", "{\"key\":\"doc:first\"}");
    defer first.deinit();
    try std.testing.expect(std.mem.indexOf(u8, first.bytes(), "duplicate") == null);
    var second = try lease.owner().lookupJson("docs", "{\"key\":\"doc:second\"}");
    defer second.deinit();
    try @import("antfly-json").testing.expectSubsetJsonText(alloc, "{\"title\":\"second\",\"count\":1}", second.bytes());
}

test "committed relational catch-up applies an older pinned schema version" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-1/table-db", .{root});
    defer alloc.free(path);
    var source = ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    const old: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        .schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}",
    };
    var current = old;
    current.schema_json = "{\"version\":2,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"relational_indexes\":[{\"name\":\"id_idx\",\"keys\":[{\"column\":\"id\"}]}],\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"},\"extra\":{\"type\":\"string\"}},\"additionalProperties\":false}}}}";
    try applyCommittedSourceFixtureEntry(&source, alloc, 1, "docs", old, .{
        .writes = &.{.{ .key = "doc:first", .value = "{\"id\":1}" }},
        .relational_schema_version = 1,
    }, 1, 1);
    var lease = try source.acquireDescriptor(1, "docs", path, current);
    lease.deinit();
    for (0..4) |_| {
        applyCommittedSourceFixtureEntry(&source, alloc, 1, "docs", old, .{
            .writes = &.{.{ .key = "doc:second", .value = "{\"id\":2}" }},
            .relational_schema_version = 1,
        }, 1, 2) catch |err| switch (err) {
            error.StorageBusy => continue,
            else => return err,
        };
        break;
    } else return error.TestOwnerAdmissionDidNotRecover;
    var reader = try source.acquireDescriptor(1, "docs", path, current);
    defer reader.deinit();
    var second = try reader.owner().lookupJson("docs", "{\"key\":\"doc:second\",\"include_all_fields\":true}");
    defer second.deinit();
    try @import("antfly-json").testing.expectSubsetJsonText(alloc, "{\"id\":2}", second.bytes());
}

test "hidden initial child descriptor requires exact private bootstrap" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    var source = ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    const Bootstrap = @import("antfly_local_sources").storage_db_relational_initial_child_publication.Bootstrap;
    const expected: Bootstrap = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .namespace = .{ .table_id = 7, .shard_id = 8, .range_id = 9 },
        .schema_version = 1,
        .schema_digest = @splat(3),
        .public_schema_json_digest = @splat(4),
        .catalog_digest = @splat(5),
    };
    const bootstrap_json = try std.json.Stringify.valueAlloc(alloc, expected, .{});
    defer alloc.free(bootstrap_json);
    try source.primeInitialChildOwner(1, "hidden", .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 7, .shard_id = 8, .range_id = 9 },
        .initial_range = .{ .start = "", .end = "" },
        .initial_child_bootstrap_json = bootstrap_json,
    });
    try std.testing.expectError(error.TableNotFound, source.loadDescriptor(alloc, 1, "hidden"));
    var loaded = try source.loadInitialChildDescriptor(alloc, 1, "hidden", expected);
    defer loaded.deinit(alloc);
    try std.testing.expectEqualStrings(bootstrap_json, loaded.view().initial_child_bootstrap_json);
    try std.testing.expect(loaded.view().identity.eql(.{ .table_id = 7, .shard_id = 8, .range_id = 9 }));
    var wrong = expected;
    wrong.plan_id = @splat(6);
    try std.testing.expectError(error.InitialChildPublicationChanged, source.loadInitialChildDescriptor(alloc, 1, "hidden", wrong));
}

test "committed catch-up does not reconcile an older index-only descriptor" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-1/table-db", .{root});
    defer alloc.free(path);
    var source = ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    const old: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        .schema_json = "{\"version\":0}",
        .indexes_json = "{}",
    };
    const current: descriptor_contract.Descriptor = .{
        .lsm_root_generation = old.lsm_root_generation,
        .identity = old.identity,
        .schema_json = old.schema_json,
        .indexes_json = "{\"new_idx\":{\"type\":\"full_text\"}}",
    };
    try applyCommittedSourceFixtureEntry(&source, alloc, 1, "docs", old, .{
        .writes = &.{.{ .key = "doc:first", .value = "{\"title\":\"first\"}" }},
    }, 1, 1);
    {
        var lease = try source.acquireDescriptor(1, "docs", path, current);
        lease.deinit();
    }
    for (0..4) |_| {
        applyCommittedSourceFixtureEntry(&source, alloc, 1, "docs", old, .{
            .writes = &.{.{ .key = "doc:second", .value = "{\"title\":\"second\"}" }},
        }, 1, 2) catch |err| switch (err) {
            error.StorageBusy => continue,
            else => return err,
        };
        break;
    } else return error.TestOwnerAdmissionDidNotRecover;
    var lease = try source.acquireDescriptorOnce(1, "docs", path, old, .shared, .resident, .{ .historical_raft_apply = true });
    defer lease.deinit();
    // Reconciliation would have to add this index again if historical open
    // retired it. Querying the existing owner avoids an intervening reopen.
    const reconciled = try lease.owner().reconcile("docs", current.schema_json, current.indexes_json, null, false);
    try std.testing.expectEqual(@as(u64, 0), reconciled.indexes_added);
    try std.testing.expectEqual(@as(u64, 0), reconciled.indexes_removed);
    var second = try lease.owner().lookupJson("docs", "{\"key\":\"doc:second\"}");
    defer second.deinit();
    try std.testing.expect(std.mem.indexOf(u8, second.bytes(), "second") != null);
}

test "current catalog acquisition replaces a replay-only owner with the same descriptor" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const path = try std.fmt.allocPrint(alloc, "{s}/group-1/table-db", .{root});
    defer alloc.free(path);
    const descriptor: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        .initial_range = .{ .start = "", .end = "" },
        .schema_json = "{\"version\":0}",
        .indexes_json =
        \\{"search":{"type":"full_text","artifact_name":"chunks","enrichments":[{"name":"assets","kind":"asset","field":"url","content_type":"application/json","producer_json":"{\"type\":\"document_extraction\",\"config\":{}}"},{"name":"chunks","kind":"chunk","source_artifact_name":"assets","field":"text","chunk_size":128}]}}
        ,
    };
    {
        var initial = ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer initial.deinit();
        try applyCommittedSourceFixtureEntry(&initial, alloc, 1, "docs", descriptor, .{
            .writes = &.{.{ .key = "doc:first", .value = "{\"title\":\"first\"}" }},
        }, 1, 1);
    }
    var source = ProvisionedKernelOwnerSource.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    try applyCommittedSourceFixtureEntry(&source, alloc, 1, "docs", descriptor, .{
        .writes = &.{.{ .key = "doc:second", .value = "{\"title\":\"second\"}" }},
    }, 1, 2);
    try std.testing.expectEqual(@as(u64, 1), source.cacheStats().miss_count);
    {
        var replay = try source.acquireDescriptorOnce(1, "docs", path, descriptor, .shared, .resident, .{ .historical_raft_apply = true });
        defer replay.deinit();
        const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
        while (true) {
            var status = replay.owner().runtimeStatusJson("docs") catch |err| switch (err) {
                error.StorageBusy => {
                    if (platform_time.monotonicNs() >= deadline) return error.TestEnrichmentRuntimeNotObservable;
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                    continue;
                },
                else => return err,
            };
            defer status.deinit();
            var parsed = try std.json.parseFromSlice(std.json.Value, alloc, status.bytes(), .{});
            defer parsed.deinit();
            const enrichment = parsed.value.object.get("stats").?.object.get("enrichment").?.object;
            if (enrichment.get("enabled").?.bool and enrichment.get("worker_started").?.bool) break;
            if (platform_time.monotonicNs() >= deadline) return error.TestEnrichmentRuntimeNotStarted;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        // The current catalog requests a reopen while a real replay reader
        // is pinned. Close new admission without revoking its physical cut.
        try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, source.acquireDescriptorOnce(1, "docs", path, descriptor, .shared, .resident, .{}));
        try std.testing.expect(replay.entry.catalog_drain_pending and !replay.entry.retired);
        var admitted = try replay.owner().lookupJson("docs", "{\"key\":\"doc:second\"}");
        defer admitted.deinit();
        try std.testing.expect(std.mem.indexOf(u8, admitted.bytes(), "second") != null);
    }
    // The ordinary last-reader release drains and closes the replay runtime.
    var current = try source.acquireDescriptor(1, "docs", path, descriptor);
    defer current.deinit();
    try std.testing.expectEqual(@as(u64, 2), source.cacheStats().miss_count);
    var second = try current.owner().lookupJson("docs", "{\"key\":\"doc:second\"}");
    defer second.deinit();
    try std.testing.expect(std.mem.indexOf(u8, second.bytes(), "second") != null);
    // Durable control reads omit prospective schema/range hints. Borrow the
    // configured physical owner instead of revoking it or reopening its DB.
    var durable_descriptor = descriptor;
    durable_descriptor.schema_json = "";
    durable_descriptor.indexes_json = "";
    durable_descriptor.initial_range = null;
    var durable = try source.acquireDescriptorOnce(1, "docs", path, durable_descriptor, .shared, .resident, .{
        .historical_raft_apply = true,
        .durable_catalog = true,
        .allow_deferred_catalog = true,
    });
    defer durable.deinit();
    try std.testing.expect(durable.entry == current.entry);
    try std.testing.expect(!current.entry.retired and !current.entry.catalog_drain_pending);
    try std.testing.expectEqual(@as(u64, 2), source.cacheStats().miss_count);
    const apply_controls: ProvisionedKernelOwnerSource.ReadControls = .{
        .historical_raft_apply = true,
        .durable_catalog = true,
        .require_historical_owner = true,
        .allow_deferred_catalog = true,
    };
    try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, source.acquireDescriptorOnce(1, "docs", path, durable_descriptor, .shared, .resident, apply_controls));
    try std.testing.expect(current.entry.catalog_drain_pending and !current.entry.retired);
    durable.deinit();
    current.deinit();
    var applying = try source.acquireDescriptorWithMode(1, "docs", path, durable_descriptor, false, .resident, apply_controls);
    defer applying.deinit();
    try std.testing.expect(applying.entry.opened_for_historical_apply and applying.entry.catalog_deferred);
    try std.testing.expectEqual(@as(u64, 3), source.cacheStats().miss_count);
}

test "pending exclusive storage owner lease blocks new readers until drain" {
    var entry: ProvisionedKernelOwnerSource.Entry = undefined;
    entry.active_users = 1;
    entry.exclusive_pending = false;
    entry.exclusive_active = false;

    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive));
    try std.testing.expect(entry.exclusive_pending);
    try std.testing.expectEqual(@as(usize, 1), entry.active_users);

    // Observational status reads arriving after the writer must not starve it.
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .shared));
    try std.testing.expectEqual(@as(usize, 1), entry.active_users);

    // Once the original reader drains, the waiting exclusive lease wins and
    // clears the pending gate while its active gate remains authoritative.
    entry.active_users = 0;
    try std.testing.expect(ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive));
    try std.testing.expect(!entry.exclusive_pending);
    try std.testing.expect(entry.exclusive_active);
    try std.testing.expectEqual(@as(usize, 1), entry.active_users);
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .shared));
}

test "background storage owner lease inspection yields without gating readers" {
    var entry: ProvisionedKernelOwnerSource.Entry = undefined;
    entry.active_users = 1;
    entry.exclusive_pending = false;
    entry.exclusive_active = false;

    // A long-lived query or maintenance lease must not turn periodic
    // inspection into a barrier for later foreground requests.
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive_if_idle));
    try std.testing.expect(!entry.exclusive_pending and !entry.exclusive_active);
    try std.testing.expect(ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .shared));
    try std.testing.expectEqual(@as(usize, 2), entry.active_users);

    // Inspection can run once admitted users drain, with the same exclusion
    // while actually reconciling. It cannot jump an explicit structural waiter.
    entry.active_users = 0;
    entry.exclusive_pending = true;
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive_if_idle));
    try std.testing.expect(entry.exclusive_pending);
    entry.exclusive_pending = false;
    try std.testing.expect(ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .exclusive_if_idle));
    try std.testing.expect(entry.exclusive_active and !entry.exclusive_pending);
    try std.testing.expect(!ProvisionedKernelOwnerSource.tryReserveEntryLeaseLocked(&entry, .shared));
}

test "transient storage owner retirement drains borrowers and permits foreground adoption" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    for ([_]enum { observation_finished, observation_held, maintenance_held, foreground_adoption, prepared_adoption }{ .observation_finished, .observation_held, .maintenance_held, .foreground_adoption, .prepared_adoption }) |history| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
        defer alloc.free(path);
        var source = Source.init(alloc, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        };
        var transient = try source.acquireDescriptorWithMode(1, "docs", path, descriptor, false, .transient, .{});
        defer transient.deinit();
        const original = transient.entry;
        var observation: ?Source.Lease = null;
        defer if (observation) |*lease| lease.deinit();
        var maintenance: ?[]Source.Lease = null;
        defer if (maintenance) |leases| source.releaseMaintenanceLeases(leases);
        if (history == .maintenance_held) {
            maintenance = (try source.snapshotOwnerLeases(false, false)).?;
            try std.testing.expectEqual(@as(usize, 1), maintenance.?.len);
        } else {
            Source.lock(&source.mutex);
            observation = source.borrowEntryLocked(original) catch |err| {
                source.mutex.unlock();
                return err;
            };
            source.mutex.unlock();
            if (history == .observation_finished) observation.?.deinit();
        }
        try std.testing.expect(!original.resident);
        transient.requestTransientRetirement();
        transient.deinit();
        if (history == .observation_finished) {
            try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());
        } else {
            try std.testing.expectEqual(@as(usize, 1), source.ownerCountForTest());
            try std.testing.expect(original.transient_retirement_pending);
            // Once cleanup begins, new observational work cannot starve drain.
            Source.lock(&source.mutex);
            const refused = source.borrowEntryLocked(original);
            source.mutex.unlock();
            try std.testing.expectError(error.StorageReadTemporarilyUnavailable, refused);
            const excluded = (try source.snapshotOwnerLeases(false, false)).?;
            defer source.releaseMaintenanceLeases(excluded);
            try std.testing.expectEqual(@as(usize, 0), excluded.len);
            if (history == .foreground_adoption or history == .prepared_adoption) {
                var foreground = if (history == .prepared_adoption) try source.acquirePreparedOwner(1, "docs") else try source.acquireDescriptor(1, "docs", path, descriptor);
                defer foreground.deinit();
                try std.testing.expectEqual(original, foreground.entry);
                try std.testing.expect(original.resident);
                try std.testing.expect(!original.transient_retirement_pending);
                foreground.deinit();
            }
            if (observation) |*lease| lease.deinit();
            if (maintenance) |leases| {
                source.releaseMaintenanceLeases(leases);
                maintenance = null;
            }
            try std.testing.expectEqual(@as(usize, if (history == .foreground_adoption or history == .prepared_adoption) 1 else 0), source.ownerCountForTest());
        }
        // A finished transient lease cannot retire a later replacement owner.
        var resident = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer resident.deinit();
        const misses = source.cacheStats().miss_count;
        transient.deinit();
        try std.testing.expectEqual(@as(usize, 1), source.ownerCountForTest());
        try std.testing.expectEqual(@as(u64, if (history == .foreground_adoption or history == .prepared_adoption) 1 else 2), misses);
    }
}

test "storage repair lease downgrade admits readers while fencing configuration" {
    const Source = ProvisionedKernelOwnerSource;
    var source: Source = undefined;
    source.mutex = .unlocked;
    var entry: Source.Entry = undefined;
    entry.active_users = 1;
    entry.exclusive_active = true;
    entry.exclusive_pending = false;
    var lease = Source.Lease{ .source = &source, .entry = &entry, .exclusive = true };
    try std.testing.expect(!Source.tryReserveEntryLeaseLocked(&entry, .shared));
    lease.downgrade();
    try std.testing.expect(!lease.exclusive);
    try std.testing.expect(Source.tryReserveEntryLeaseLocked(&entry, .shared));
    try std.testing.expectEqual(@as(usize, 2), entry.active_users);
    try std.testing.expect(!Source.tryReserveEntryLeaseLocked(&entry, .exclusive));
}

test "owner descriptor changes close admission before draining existing readers" {
    const Source = ProvisionedKernelOwnerSource;
    for ([_]Source.LeaseAdmission{ .shared, .exclusive, .exclusive_if_idle }) |admission| {
        var source = Source.init(std.testing.allocator, "/unused", table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.entries.deinit(std.testing.allocator);
        var entry: Source.Entry = .{
            .group_id = 1,
            .table_name = @constCast("docs"),
            .generation = 7,
            .identity = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .schema_json = @constCast("old schema"),
            .indexes_json = @constCast("{}"),
            .restore_bootstrap_json = @constCast(""),
            .initial_child_bootstrap_json = @constCast(""),
            .owner = undefined,
            .active_users = 1,
            .resident = true,
        };
        try source.entries.append(std.testing.allocator, &entry);
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = entry.generation,
            .identity = entry.identity,
            .schema_json = "new schema",
            .indexes_json = entry.indexes_json,
        };
        try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, source.acquireDescriptorOnce(1, "docs", "/unused", descriptor, admission, .resident, .{}));
        // Scheduled inspection yields without interrupting foreground work.
        // An admitted change must prevent observers from extending the drain.
        try std.testing.expectEqual(admission != .exclusive_if_idle, entry.catalog_drain_pending);
        // An admitted reader's physical-root observation remains truthful
        // until its lease ends; descriptor drain is not root revocation.
        try std.testing.expect(!entry.retired and !entry.closing);
        if (admission != .exclusive_if_idle) {
            try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.borrowEntryLocked(&entry));
            var old_descriptor = descriptor;
            old_descriptor.schema_json = entry.schema_json;
            try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, source.acquireDescriptorOnce(1, "docs", "/unused", old_descriptor, .shared, .resident, .{}));
        }
        try std.testing.expectEqual(@as(usize, 1), entry.active_users);
        entry.pending_apply_releases.store(1, .release);
        source.reconcileApplyReleasesLocked();
        try std.testing.expectEqual(@as(usize, 0), entry.active_users);
        try std.testing.expectEqual(admission != .exclusive_if_idle, entry.retired);
        // Private bootstrap authority changes still revoke immediately. This
        // is distinct from a catalog-only reopen on the same physical root.
        entry.active_users = 1;
        entry.retired = false;
        entry.catalog_drain_pending = false;
        var bootstrap_descriptor = descriptor;
        bootstrap_descriptor.schema_json = entry.schema_json;
        bootstrap_descriptor.restore_bootstrap_json = "changed private bootstrap";
        try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, source.acquireDescriptorOnce(1, "docs", "/unused", bootstrap_descriptor, admission, .resident, .{}));
        try std.testing.expectEqual(admission != .exclusive_if_idle, entry.retired);
        try std.testing.expect(!entry.catalog_drain_pending);
    }
}

test "published handoff receipt requires local strict-barrier certificate and routed stale execution" {
    const Source = ProvisionedKernelOwnerSource;
    const receipt: db_types.LookupOptions = .{
        .relational_topology_json = "{\"mode\":\"generation_handoff_install\"}",
        .generation_handoff_install_read_index_certified = true,
    };
    try std.testing.expect(Source.publishedHandoffReceiptReadCertified("", receipt, .stale));
    try std.testing.expect(!Source.publishedHandoffReceiptReadCertified("", receipt, .read_index));
    try std.testing.expect(!Source.publishedHandoffReceiptReadCertified("row", receipt, .stale));
    var uncertified = receipt;
    uncertified.generation_handoff_install_read_index_certified = false;
    try std.testing.expect(!Source.publishedHandoffReceiptReadCertified("", uncertified, .stale));
    var hidden = receipt;
    hidden.restore_staging_scope = @splat(1);
    try std.testing.expect(!Source.publishedHandoffReceiptReadCertified("", hidden, .stale));
    var planned = receipt;
    planned.restore_staging_plan_id = @splat(2);
    try std.testing.expect(!Source.publishedHandoffReceiptReadCertified("", planned, .stale));
}

test "fence-deferred owner preserves control admission without exposing stale public catalog" {
    const Source = ProvisionedKernelOwnerSource;
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("", .{ .relational_integrity_catalog = true }, .stale));
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("", .{ .relational_integrity_catalog = true, .fk_generation_source_control = true }, .stale));
    try std.testing.expect(Source.fkGenerationSourceDeferredLookupAllowed("", .{ .relational_integrity_catalog = true, .fk_generation_source_control = true, .fk_generation_source_read_index_certified = true }, .stale));
    try std.testing.expect(Source.fkGenerationSourceDeferredLookupAllowed("", .{ .relational_topology_json = "{\"mode\":\"public_schema\"}", .fk_generation_source_control = true, .fk_generation_source_read_index_certified = true }, .stale));
    try std.testing.expect(Source.fkGenerationSourceDeferredLookupAllowed("", .{ .relational_topology_json = "{\"mode\":\"status\"}", .fk_generation_source_control = true, .fk_generation_source_read_index_certified = true }, .stale));
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("row", .{ .relational_integrity_catalog = true, .fk_generation_source_control = true, .fk_generation_source_read_index_certified = true }, .stale));
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("", .{ .relational_integrity_catalog = true, .fk_generation_source_control = true, .fk_generation_source_read_index_certified = true }, .read_index));
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("", .{ .relational_topology_json = "{\"mode\":\"other\"}", .fk_generation_source_control = true, .fk_generation_source_read_index_certified = true }, .stale));
    const activation: db_types.LookupOptions = .{ .relational_activation_json = "{\"mode\":\"status\"}", .fk_generation_source_control = true, .fk_generation_source_read_index_certified = true };
    try std.testing.expect(Source.fkGenerationSourceDeferredLookupAllowed("", activation, .stale));
    var uncertified_activation = activation;
    uncertified_activation.fk_generation_source_read_index_certified = false;
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("", uncertified_activation, .stale));
    var mixed_activation = activation;
    mixed_activation.relational_integrity_catalog = true;
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("", mixed_activation, .stale));
    mixed_activation = activation;
    mixed_activation.relational_topology_json = "{\"mode\":\"status\"}";
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("", mixed_activation, .stale));
    mixed_activation = activation;
    mixed_activation.relational_activation_json = "{\"mode\":\"other\"}";
    try std.testing.expect(!Source.fkGenerationSourceDeferredLookupAllowed("", mixed_activation, .stale));
    var source = Source.init(std.testing.allocator, "/unused", table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.entries.deinit(std.testing.allocator);
    var entry: Source.Entry = .{
        .group_id = 1,
        .table_name = @constCast("docs"),
        .generation = 7,
        .identity = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
        .schema_json = @constCast("{\"version\":1}"),
        .indexes_json = @constCast("{}"),
        .restore_bootstrap_json = @constCast(""),
        .initial_child_bootstrap_json = @constCast(""),
        .owner = undefined,
        .active_users = 1,
        .resident = true,
        .catalog_deferred = true,
    };
    try source.entries.append(std.testing.allocator, &entry);
    const exact: descriptor_contract.Descriptor = .{
        .lsm_root_generation = entry.generation,
        .identity = entry.identity,
        .schema_json = entry.schema_json,
        .indexes_json = entry.indexes_json,
    };
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptorOnce(1, "docs", "/unused", exact, .shared, .resident, .{}));
    try std.testing.expect(!entry.retired);
    var control = try source.acquireDescriptorOnce(1, "docs", "/unused", exact, .shared, .resident, .{ .allow_deferred_catalog = true });
    try std.testing.expectEqual(@as(usize, 2), entry.active_users);
    control.deinit();
    const successor: descriptor_contract.Descriptor = .{
        .lsm_root_generation = entry.generation,
        .identity = entry.identity,
        .schema_json = "{\"version\":2}",
        .indexes_json = entry.indexes_json,
    };
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptorOnce(1, "docs", "/unused", successor, .shared, .resident, .{}));
    try std.testing.expect(!entry.retired);
    source.noteWholeCatalogReconciled(&entry, "named", .complete);
    try std.testing.expect(entry.catalog_deferred);
    source.noteWholeCatalogReconciled(&entry, null, .busy);
    source.noteWholeCatalogReconciled(&entry, null, .degraded);
    try std.testing.expect(entry.catalog_deferred);
    source.noteWholeCatalogReconciled(&entry, null, .complete);
    try std.testing.expect(!entry.catalog_deferred);
    entry.catalog_deferred = true;
    var new_root = exact;
    new_root.lsm_root_generation += 1;
    try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, source.acquireDescriptorOnce(1, "docs", "/unused", new_root, .shared, .resident, .{}));
    try std.testing.expect(entry.retired);
}

test "storage owner rejects prior ABI before reading deferred-output field" {
    var owner: ?*anyopaque = @ptrFromInt(1);
    const old_request: abi.OpenRequest = .{
        .version = 68,
        .path = .{ .ptr = @ptrFromInt(1), .len = 1 },
        .owner_catalog_deferred_out = @ptrFromInt(1),
    };
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_open(&old_request, &owner));
    try std.testing.expect(owner == null);
}

test "owner descriptor changes do not retire a live owner for an older schema version" {
    const Source = ProvisionedKernelOwnerSource;
    try std.testing.expectEqual(@as(?u32, 7), Source.schemaVersionFromJson("{\"default_type\":\"_default\",\"types\":{\"a\":{\"version\":1}},\"version\":7}"));
    try std.testing.expectEqual(@as(?u32, null), Source.schemaVersionFromJson("old schema"));
    try std.testing.expect(Source.schemaVersionRegresses("{\"version\":7}", "{\"version\":6}"));
    try std.testing.expect(!Source.schemaVersionRegresses("{\"version\":7}", "{\"version\":8}"));
    try std.testing.expect(!Source.schemaVersionRegresses("{\"version\":7}", "{\"version\":7}"));
    try std.testing.expect(!Source.schemaVersionRegresses("old schema", "{\"version\":1}"));

    for ([_]Source.LeaseAdmission{ .shared, .exclusive }) |admission| {
        var source = Source.init(std.testing.allocator, "/unused", table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.entries.deinit(std.testing.allocator);
        var entry: Source.Entry = .{
            .group_id = 1,
            .table_name = @constCast("docs"),
            .generation = 7,
            .identity = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
            .schema_json = @constCast("{\"version\":7,\"default_type\":\"_default\"}"),
            .indexes_json = @constCast("{}"),
            .restore_bootstrap_json = @constCast(""),
            .initial_child_bootstrap_json = @constCast(""),
            .owner = undefined,
            .active_users = 1,
            .resident = true,
        };
        try source.entries.append(std.testing.allocator, &entry);
        var descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = entry.generation,
            .identity = entry.identity,
            .schema_json = "{\"version\":6,\"default_type\":\"_default\"}",
            .indexes_json = entry.indexes_json,
        };
        // A stale caller (schema captured before the current publication)
        // is turned away without closing admission for current readers.
        try std.testing.expectError(error.StorageKernelOwnerStaleDescriptor, source.acquireDescriptorOnce(1, "docs", "/unused", descriptor, admission, .resident, .{}));
        try std.testing.expect(!entry.retired);
        try std.testing.expectError(error.StorageKernelOwnerStaleDescriptor, source.acquireDescriptorWithMode(1, "docs", "/unused", descriptor, admission == .exclusive, .resident, .{}));
        try std.testing.expect(!entry.retired);
        // A newer schema still closes admission so the drain can complete.
        descriptor.schema_json = "{\"version\":8,\"default_type\":\"_default\"}";
        try std.testing.expectError(error.StorageKernelOwnerTransitionRequired, source.acquireDescriptorOnce(1, "docs", "/unused", descriptor, admission, .resident, .{}));
        try std.testing.expect(entry.catalog_drain_pending and !entry.retired);
        try std.testing.expectEqual(@as(usize, 1), entry.active_users);
    }

    var source = Source.init(std.testing.allocator, "/unused", table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.entries.deinit(std.testing.allocator);
    var entry: Source.Entry = .{
        .group_id = 1,
        .table_name = @constCast("docs"),
        .generation = 7,
        .identity = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
        .schema_json = @constCast("{\"version\":7}"),
        .indexes_json = @constCast("{}"),
        .restore_bootstrap_json = @constCast(""),
        .initial_child_bootstrap_json = @constCast(""),
        .owner = undefined,
        .active_users = 0,
        .resident = true,
    };
    try source.entries.append(std.testing.allocator, &entry);
    const stale: descriptor_contract.Descriptor = .{
        .lsm_root_generation = entry.generation,
        .identity = entry.identity,
        .schema_json = "{\"version\":6}",
        .indexes_json = entry.indexes_json,
    };
    try std.testing.expectError(error.StorageKernelOwnerStaleDescriptor, source.acquireDescriptorOnce(1, "docs", "/unused", stale, .shared, .resident, .{}));
    try std.testing.expectError(error.StorageKernelOwnerStaleDescriptor, source.acquireDescriptor(1, "docs", "/unused", stale));
    try std.testing.expect(!entry.retired);
    try std.testing.expectEqual(@as(usize, 0), entry.active_users);
}

test "scheduled repair admission yields to readers and reuses exact configured generation" {
    const Source = ProvisionedKernelOwnerSource;
    var source = Source.init(std.testing.allocator, "/unused", table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.entries.deinit(std.testing.allocator);
    var entry: Source.Entry = .{
        .group_id = 1,
        .table_name = @constCast("docs"),
        .generation = 7,
        .identity = .{ .table_id = 1, .shard_id = 2, .range_id = 3 },
        .schema_json = @constCast("schema"),
        .indexes_json = @constCast("indexes"),
        .restore_bootstrap_json = @constCast(""),
        .initial_child_bootstrap_json = @constCast(""),
        .owner = undefined,
        .active_users = 1,
        .resident = true,
    };
    try source.entries.append(std.testing.allocator, &entry);
    const descriptor: descriptor_contract.Descriptor = .{
        .lsm_root_generation = 7,
        .identity = entry.identity,
        .schema_json = entry.schema_json,
        .indexes_json = entry.indexes_json,
    };
    try std.testing.expect((try source.acquireDescriptorForReconcile(1, "docs", "/unused", descriptor, false, .transient)) == null);
    try std.testing.expect(!entry.exclusive_pending);
    try std.testing.expect(Source.tryReserveEntryLeaseLocked(&entry, .shared));
    entry.active_users -= 1;
    entry.repair_target = @constCast("text");
    entry.repair_configuration = .{ .state = .busy, .repair_remaining = 1 };
    var repair = source.tryAcquireConfiguredRepair(1, "docs", descriptor, "text").?;
    try std.testing.expect(!repair.lease.exclusive);
    try std.testing.expectEqual(@as(usize, 2), entry.active_users);
    repair.lease.deinit();
    try std.testing.expect(source.tryAcquireConfiguredRepair(1, "docs", descriptor, "other") == null);
    var changed = descriptor;
    changed.schema_json = "new schema";
    try std.testing.expect(source.tryAcquireConfiguredRepair(1, "docs", changed, "text") == null);
    entry.repair_target = null;
    var ordinary = source.tryAcquireConfiguredRepair(1, "docs", descriptor, null).?;
    try std.testing.expect(!ordinary.lease.exclusive);
    ordinary.lease.deinit();
    changed = descriptor;
    changed.lsm_root_generation += 1;
    try std.testing.expect(source.tryAcquireConfiguredRepair(1, "docs", changed, null) == null);
}

// The fake clock controls only the publication wait. Owners below use the real
// compiled storage kernel, including worker shutdown and registry removal.
const PublicationWaitTest = struct {
    now_ns: i96 = 0,
    sleeps: usize = 0,
    lease: ?*ProvisionedKernelOwnerSource.Lease = null,
    cancel: ?*std.atomic.Value(bool) = null,

    fn now(ptr: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
        const self: *@This() = @ptrCast(@alignCast(ptr.?));
        return .{ .nanoseconds = self.now_ns };
    }

    fn sleep(ptr: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
        const self: *@This() = @ptrCast(@alignCast(ptr.?));
        self.sleeps += 1;
        self.now_ns += std.time.ns_per_ms;
        if (self.lease) |lease| {
            lease.deinit();
            self.lease = null;
        }
        if (self.cancel) |signal| signal.store(true, .release);
    }

    fn io(self: *@This(), vtable: *std.Io.VTable) std.Io {
        vtable.* = std.testing.io.vtable.*;
        vtable.now = now;
        vtable.sleep = sleep;
        return .{ .userdata = self, .vtable = vtable };
    }
};

test "publication drains existing readers status and maintenance before reopening admission" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    for ([_]enum { reader, status, maintenance }{ .reader, .status, .maintenance }) |history| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
        defer alloc.free(path);
        var source = Source.init(alloc, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        };
        var original = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer original.deinit();
        var held: Source.Lease = undefined;
        var maintenance: ?[]Source.Lease = null;
        defer if (maintenance) |leases| source.releaseMaintenanceLeases(leases);
        switch (history) {
            .reader => held = try source.acquireDescriptor(1, "docs", path, descriptor),
            .status => {
                Source.lock(&source.mutex);
                held = source.borrowEntryLocked(original.entry) catch |err| {
                    source.mutex.unlock();
                    return err;
                };
                source.mutex.unlock();
            },
            .maintenance => {
                maintenance = (try source.snapshotOwnerLeases(false, false)).?;
                try std.testing.expectEqual(@as(usize, 1), maintenance.?.len);
            },
        }
        const borrower = if (maintenance) |leases| &leases[0] else &held;
        defer borrower.deinit();
        original.deinit();
        // Force publication to see a live user, then release it at the first
        // cooperative wait. The old implementation returned StorageBusy here.
        var wait = PublicationWaitTest{ .lease = borrower };
        var vtable: std.Io.VTable = undefined;
        var publication = try source.snapshotSource().beginPublication(.{
            .io = wait.io(&vtable),
            .group_id = 1,
            .table_name = "docs",
        });
        defer publication.deinit();
        try std.testing.expectEqual(@as(usize, 1), wait.sleeps);
        try std.testing.expectEqual(@as(usize, 0), source.ownerCountForTest());
        // With no Entry remaining, the independent gate still excludes open
        // and prepared apply until the publisher completes commit/rollback.
        try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptor(1, "docs", path, descriptor));
        try std.testing.expectError(error.RaftApplyWriterUnavailable, source.acquirePreparedOwner(1, "docs"));
        try std.testing.expectError(error.StorageBusy, source.snapshotSource().beginPublication(.{
            .io = wait.io(&vtable),
            .group_id = 1,
            .table_name = "docs",
        }));
        const excluded = (try source.snapshotOwnerLeases(false, false)).?;
        defer source.releaseMaintenanceLeases(excluded);
        try std.testing.expectEqual(@as(usize, 0), excluded.len);
        publication.deinit();
        var replacement = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer replacement.deinit();
        try std.testing.expectEqual(@as(u64, 2), source.cacheStats().miss_count);
    }
}

test "maintenance on one owner does not pin another owner against publication" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    var source = Source.init(alloc, root, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();

    for ([_]struct { group_id: u64, name: []const u8 }{
        .{ .group_id = 1, .name = "busy" },
        .{ .group_id = 2, .name = "restoring" },
    }) |owner| {
        const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ root, owner.group_id });
        defer alloc.free(path);
        var lease = try source.acquireDescriptor(owner.group_id, owner.name, path, .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = owner.group_id, .shard_id = owner.group_id, .range_id = owner.group_id },
        });
        lease.deinit();
    }

    const limit = source.maintenanceEntryLimit(false).?;
    var cursor: usize = 0;
    var slow_maintenance = source.nextMaintenanceLease(&cursor, limit, false, true).?;
    defer slow_maintenance.deinit();
    try std.testing.expectEqual(@as(u64, 1), slow_maintenance.entry.group_id);

    var wait = PublicationWaitTest{};
    var vtable: std.Io.VTable = undefined;
    var publication = try source.snapshotSource().beginPublication(.{
        .io = wait.io(&vtable),
        .group_id = 2,
        .table_name = "restoring",
        .drain_timeout_ns = std.time.ns_per_ms,
    });
    defer publication.deinit();
    try std.testing.expectEqual(@as(usize, 0), wait.sleeps);
    try std.testing.expectEqual(@as(u64, 1), slow_maintenance.entry.group_id);
}

test "publication cancellation and timeout release admission without invalidating borrowers" {
    const alloc = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    for ([_]bool{ false, true }) |cancel| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
        defer alloc.free(path);
        var source = Source.init(alloc, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{
            .lsm_root_generation = table_reads.backend_current_root_generation,
            .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        };
        var borrower = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer borrower.deinit();
        var signal = std.atomic.Value(bool).init(false);
        var wait = PublicationWaitTest{ .cancel = if (cancel) &signal else null };
        var vtable: std.Io.VTable = undefined;
        try std.testing.expectError(if (cancel) error.Canceled else error.StorageBusy, source.snapshotSource().beginPublication(.{
            .io = wait.io(&vtable),
            .group_id = 1,
            .table_name = "docs",
            .cancellation = .fromAtomic(&signal),
            .drain_timeout_ns = std.time.ns_per_ms,
        }));
        try std.testing.expectEqual(@as(usize, 1), wait.sleeps);
        try std.testing.expectEqual(@as(usize, 0), source.publications.items.len);
        try std.testing.expectEqual(@as(usize, 1), source.ownerCountForTest());
        try std.testing.expectEqual(@as(usize, 1), borrower.entry.active_users);
        borrower.deinit();
        var replacement = try source.acquireDescriptor(1, "docs", path, descriptor);
        defer replacement.deinit();
    }
}

test "owner recovery bulk callback preserves bounded identities and uncertain status" {
    const Recorder = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn options(_: *anyopaque) transaction_recovery_source.Options {
            return .{};
        }
        fn owns(_: *anyopaque, _: []const u8) bool {
            return true;
        }
        fn resolve(_: *anyopaque, _: @import("antfly_local_sources").storage_db_types.TxnId, _: []const u8, _: @import("antfly_local_sources").storage_db_types.TxnStatus, _: u64) !void {}
        fn single(_: *anyopaque, _: @import("antfly_local_sources").storage_db_types.TxnId, _: []const u8, _: []const u8) !void {
            return error.TestUnexpectedSingle;
        }
        fn cleanup(_: *anyopaque, _: @import("antfly_local_sources").storage_db_types.TxnId, _: []const u8, _: u64, _: u64) !void {}
        fn many(ptr: *anyopaque, txn: @import("antfly_local_sources").storage_db_types.TxnId, owner: []const u8, participants: []const []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            try std.testing.expectEqual(@as([16]u8, @splat(4)), txn);
            try std.testing.expectEqualStrings("owner", owner);
            try std.testing.expectEqual(@as(usize, 2), participants.len);
            try std.testing.expectEqualStrings("first", participants[0]);
            try std.testing.expectEqualStrings("second", participants[1]);
            if (self.failure) |err| return err;
        }
    };
    var recorder: Recorder = .{};
    var owner: ProvisionedKernelOwnerSource = undefined;
    owner.transaction_recovery_source = .{ .ptr = &recorder, .vtable = &.{ .options = Recorder.options, .owns = Recorder.owns, .resolve = Recorder.resolve, .acknowledge = Recorder.single, .acknowledge_many = Recorder.many, .cleanup = Recorder.cleanup } };
    const txn: abi.TxnId = .{ .bytes = @splat(4) };
    const participants = [_]abi.BorrowedBytes{ .fromSlice("first"), .fromSlice("second") };
    try std.testing.expectEqual(abi.Status.ok, ProvisionedKernelOwnerSource.transactionRecoveryAcknowledgeMany(&owner, &txn, .fromSlice("owner"), &participants, participants.len));
    for ([_]anyerror{ error.UnsupportedOperation, error.UnsupportedRaftBatchProtocolVersion, error.RaftBatchWriteOutcomeUnknown }) |err| {
        recorder.failure = err;
        try std.testing.expectEqual(kernel_error_identity.statusFromError(err), ProvisionedKernelOwnerSource.transactionRecoveryAcknowledgeMany(&owner, &txn, .fromSlice("owner"), &participants, participants.len));
    }
    try std.testing.expectEqual(abi.Status.invalid_argument, ProvisionedKernelOwnerSource.transactionRecoveryAcknowledgeMany(&owner, &txn, .fromSlice("owner"), &participants, 65));
    try std.testing.expectEqual(abi.Status.invalid_argument, ProvisionedKernelOwnerSource.transactionRecoveryAcknowledgeMany(&owner, &txn, .fromSlice("owner"), null, 2));
    try std.testing.expectEqual(@as(usize, 4), recorder.calls);
}

test "storage owner entity sink bridge preserves curation capability and missing-row policy" {
    const Fixture = struct {
        supported: bool = true,
        seen: bool = false,
        fn supports(ptr: *anyopaque) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.supported;
        }
        fn upsert(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8) anyerror!void {}
        fn batch(ptr: *anyopaque, _: std.mem.Allocator, entries: []const runtime_callbacks.EntityUpsert) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 1), entries.len);
            try std.testing.expect(entries[0].skip_if_missing);
            try std.testing.expectEqualStrings("table:7", entries[0].storage_table.?);
            self.seen = true;
        }
    };
    var fixture: Fixture = .{};
    var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "/unused", table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    _ = source.withRuntimeHooks(null, .{ .ptr = &fixture, .vtable = &.{ .upsert = Fixture.upsert, .upsert_batch = Fixture.batch, .supports_curation = Fixture.supports } }, null);
    const hooks = source.runtimeHooksConfig();
    var supported: u8 = 0;
    try std.testing.expectEqual(abi.Status.ok, hooks.entity_sink.supports_curation_fn.?(hooks.entity_sink.callback_ctx, &supported));
    try std.testing.expectEqual(@as(u8, 1), supported);
    fixture.supported = false;
    try std.testing.expectEqual(abi.Status.ok, hooks.entity_sink.supports_curation_fn.?(hooks.entity_sink.callback_ctx, &supported));
    try std.testing.expectEqual(@as(u8, 0), supported);
    const entries = [_]abi.EntityUpsert{.{ .table = .fromSlice("entities"), .storage_table = .fromSlice("table:7"), .key = .fromSlice("entity/retired"), .doc_json = .fromSlice("{}"), .skip_if_missing = 1 }};
    try std.testing.expectEqual(abi.Status.ok, hooks.entity_sink.upsert_batch_fn.?(hooks.entity_sink.callback_ctx, &entries, entries.len));
    try std.testing.expect(fixture.seen);
}

test "raft batch round trips external transaction recovery authority through native callback statuses" {
    const Owner = @import("antfly_local_sources").storage_transactions.ExternalDecisionOwner;
    const expected: Owner = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const Probe = struct {
        calls: usize = 0,
        failure: ?anyerror = null,
        fn options(_: *anyopaque) transaction_recovery_source.Options {
            return .{ .enabled = true };
        }
        fn owns(_: *anyopaque, _: []const u8) bool {
            return true;
        }
        fn ordinary(_: *anyopaque, _: db_types.TxnId, _: []const u8, _: db_types.TxnStatus, _: u64) !void {
            return error.UnexpectedOrdinaryResolution;
        }
        fn ack(_: *anyopaque, _: db_types.TxnId, _: []const u8, _: []const u8) !void {}
        fn cleanup(_: *anyopaque, _: db_types.TxnId, _: []const u8, _: u64, _: u64) !void {}
        fn external(ptr: *anyopaque, txn: db_types.TxnId, peer: []const u8, status: db_types.TxnStatus, version: u64, owner: Owner) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as([16]u8, @splat(4)), txn);
            try std.testing.expectEqualStrings("peer", peer);
            try std.testing.expectEqual(db_types.TxnStatus.committed, status);
            try std.testing.expectEqual(@as(u64, 20), version);
            if (!expected.eql(owner)) return error.ExternalTransactionDecisionChanged;
            self.calls += 1;
            if (self.failure) |err| return err;
        }
        const full: transaction_recovery_source.Source.VTable = .{ .options = options, .owns = owns, .resolve = ordinary, .resolve_external = external, .acknowledge = ack, .cleanup = cleanup };
        const old: transaction_recovery_source.Source.VTable = .{ .options = options, .owns = owns, .resolve = ordinary, .acknowledge = ack, .cleanup = cleanup };
    };
    var probe: Probe = .{};
    var control: ProvisionedKernelOwnerSource = undefined;
    control.transaction_recovery_source = .{ .ptr = &probe, .vtable = &Probe.full };
    var config = control.transactionRecoveryConfig();
    const callback = @import("../storage/transaction_recovery_callback.zig").resolveExternal;
    try callback(config, @splat(4), "peer", .committed, 20, expected);
    for ([_]anyerror{ error.RaftBatchWriteOutcomeUnknown, error.Timeout }) |err| {
        probe.failure = err;
        try std.testing.expectError(err, callback(config, @splat(4), "peer", .committed, 20, expected));
    }
    var changed = expected;
    changed.operation_id[0] ^= 1;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, callback(config, @splat(4), "peer", .committed, 20, changed));
    control.transaction_recovery_source = .{ .ptr = &probe, .vtable = &Probe.old };
    try std.testing.expectError(error.ExternalTransactionDecisionRequired, callback(config, @splat(4), "peer", .committed, 20, expected));
    const txn: abi.TxnId = .{ .bytes = @splat(4) };
    const encoded = try expected.encode();
    try std.testing.expectEqual(abi.Status.invalid_external_decision_owner, ProvisionedKernelOwnerSource.transactionRecoveryResolveExternal(&control, &txn, .fromSlice("peer"), .committed, 20, .fromSlice(encoded[0..79])));
    try std.testing.expectEqual(abi.Status.invalid_external_decision_owner, ProvisionedKernelOwnerSource.transactionRecoveryResolveExternal(&control, &txn, .fromSlice("peer"), .committed, 20, .{ .len = Owner.encoded_len }));
    config.resolve_external_participant_fn = null;
    try std.testing.expectError(error.ExternalTransactionDecisionRequired, callback(config, @splat(4), "peer", .committed, 20, expected));
    try std.testing.expectEqual(@as(usize, 3), probe.calls);
}

test "SQL relation lifetime native lock observation cannot borrow row lookup privileges" {
    try ProvisionedKernelOwnerSource.validateNativeLockLookupOptions(.{ .sql_lock_query_json = "{}" });
    try std.testing.expectError(error.InvalidLockScope, ProvisionedKernelOwnerSource.validateNativeLockLookupOptions(.{ .sql_lock_query_json = "{}", .fields = &.{"private"} }));
    try std.testing.expectError(error.InvalidLockScope, ProvisionedKernelOwnerSource.validateNativeLockLookupOptions(.{ .sql_lock_query_json = "{}", .include_primary_digest = true }));
}

test "SQL relation lifetime root publication drains initialization before fresh proof capture and prevents cold owner recreation" {
    const Fixture = struct {
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogRead;
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    var fixture: u8 = 0;
    var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "unused", .{ .ptr = &fixture, .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.free } }, read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    // Both producers enrolled before any metadata/physical work. There are no
    // owner Entries yet, precisely the cold-open race normal lease counts miss.
    try source.beginNativeInitialization(7, "rows");
    try source.beginNativeInitialization(7, "rows");
    const publication = try source.registerPublication(7, "rows");
    try std.testing.expect(!source.publicationDrained(publication));
    source.endNativeInitialization(7);
    try std.testing.expect(!source.publicationDrained(publication));
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.beginNativeInitialization(7, "rows"));
    source.endNativeInitialization(7);
    try std.testing.expect(source.publicationDrained(publication));
    ProvisionedKernelOwnerSource.endPublication(&source, publication);
    // No permanent tombstone is necessary: fresh initialization must now get
    // a new metadata cut, which refuses the canceled job before owner open.
    try source.beginNativeInitialization(7, "rows");
    source.endNativeInitialization(7);
}

test "SQL relation lifetime quiesce waits for enrolled cold initialization before context teardown" {
    const Fixture = struct {
        fn admin(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedCatalogRead;
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
    };
    var fixture: u8 = 0;
    var source = ProvisionedKernelOwnerSource.init(std.testing.allocator, "unused", .{ .ptr = &fixture, .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = Fixture.free } }, read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    try source.beginNativeInitialization(7, "rows");
    var enrolled = true;
    defer if (enrolled) source.endNativeInitialization(7);
    const Shutdown = struct {
        source: *ProvisionedKernelOwnerSource,
        done: std.atomic.Value(bool) = .init(false),
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            self.source.quiesce(std.testing.io) catch |err| {
                self.err = err;
            };
            self.done.store(true, .release);
        }
    };
    var shutdown: Shutdown = .{ .source = &source };
    var task = try std.testing.io.concurrent(Shutdown.run, .{&shutdown});
    defer {
        if (enrolled) {
            source.endNativeInitialization(7);
            enrolled = false;
        }
        task.await(std.testing.io);
    }
    const deadline = platform_time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        ProvisionedKernelOwnerSource.lock(&source.mutex);
        const quiescing = source.quiescing;
        source.mutex.unlock();
        if (quiescing) break;
        if (platform_time.monotonicNs() >= deadline) return error.QuiesceDidNotStart;
        try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!shutdown.done.load(.acquire));
    try std.testing.expectError(error.Canceled, source.beginNativeInitialization(8, "rows"));
    source.endNativeInitialization(7);
    enrolled = false;
    task.await(std.testing.io);
    try std.testing.expect(shutdown.done.load(.acquire));
    try std.testing.expect(shutdown.err == null);
}

test "SQL relation lifetime serving reconciliation yields to retained owners without closing foreground admission" {
    const a = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/serving-owner", .{tmp.sub_path});
    defer a.free(path);
    var source = Source.init(a, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    defer source.deinit();
    const descriptor: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 },
        .schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}",
    };
    var reader = try source.acquireDescriptorWithMode(1, "docs", path, descriptor, false, .resident, .{});
    defer reader.deinit();
    const entry = reader.entry;
    const context: request_operation.RequestContext = .{ .deadline_ns = platform_time.monotonicNs() +| std.time.ns_per_s };
    const pending = try source.reconcileDescriptorCatalog(1, "docs", path, descriptor, null, false, true, context);
    try std.testing.expectEqual(abi.ReconcileState.busy, pending.state);
    try std.testing.expect(!entry.exclusive_pending and !entry.exclusive_active);
    try std.testing.expectError(error.DeadlineExceeded, source.reconcileDescriptorCatalog(1, "docs", path, descriptor, null, false, true, .{ .deadline_ns = platform_time.monotonicNs() -| 1 }));
    var canceled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, source.reconcileDescriptorCatalog(1, "docs", path, descriptor, null, false, true, .{ .cancellation = .fromAtomic(&canceled) }));
    try std.testing.expect(!entry.exclusive_pending and !entry.exclusive_active);
    var next = try source.acquireDescriptorWithMode(1, "docs", path, descriptor, false, .resident, .{});
    defer next.deinit();
    try std.testing.expect(next.entry == entry);
    const catalog_request = try table_reads.encodeStorageKernelLookupRequest(a, "", .{ .relational_integrity_catalog = true });
    defer a.free(catalog_request);
    var catalog_read = try next.owner().lookupJson("docs", catalog_request);
    defer catalog_read.deinit();
    var catalog = try std.json.parseFromSlice(struct { catalog: []const u8, schema_version: u32, table_id: []const u8 }, a, catalog_read.bytes(), .{ .ignore_unknown_fields = false });
    defer catalog.deinit();
    try std.testing.expectEqual(@as(u32, 1), catalog.value.schema_version);
    try std.testing.expectEqualStrings("1", catalog.value.table_id);
    try std.testing.expect(catalog.value.catalog.len != 0);
    next.deinit();
    // A genuinely admitted structural writer retains its preference; this
    // periodic caller cannot clear or jump that independently owned wait.
    Source.lock(&source.mutex);
    const reserved = Source.tryReserveEntryLeaseLocked(entry, .exclusive);
    source.mutex.unlock();
    try std.testing.expect(!reserved and entry.exclusive_pending);
    try std.testing.expect((try source.tryAcquireServingCatalogOwner(1, "docs", path, descriptor, context)) == null);
    try std.testing.expect(entry.exclusive_pending);
    source.clearExclusivePending(1, "docs");
    reader.deinit();
    const installed = try source.reconcileDescriptorCatalog(1, "docs", path, descriptor, null, false, true, context);
    try std.testing.expectEqual(abi.ReconcileState.complete, installed.state);
    var retained = try source.acquireDescriptorWithMode(1, "docs", path, descriptor, false, .resident, .{});
    defer retained.deinit();
    try std.testing.expect(retained.entry == entry);
}

test "SQL relation lifetime terminal ordinary sessions release actual compiled owner cuts while retaining durable decision debt" {
    const a = std.testing.allocator;
    const Source = ProvisionedKernelOwnerSource;
    const sessions = @import("transactions.zig");
    const shared = @import("sql_retained_snapshot.zig");
    const reads = @import("antfly_local_sources").api_table_read_source;
    const memory = @import("antfly_local_sources").storage_mem_backend;
    for ([_]bool{ false, true }) |abort| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/terminal-owner", .{tmp.sub_path});
        defer a.free(path);
        var source = Source.init(a, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
        defer source.deinit();
        const descriptor: descriptor_contract.Descriptor = .{ .lsm_root_generation = table_reads.backend_current_root_generation, .identity = .{ .table_id = 1, .shard_id = 1, .range_id = 1 }, .schema_json = "{\"version\":1,\"storage_mode\":\"document\"}" };
        var reader = try source.acquireDescriptorWithMode(1, "docs", path, descriptor, false, .resident, .{});
        defer reader.deinit();
        const entry = reader.entry;
        var inserted = try reader.owner().batchJson("docs", "{\"inserts\":{\"a\":{\"v\":1}},\"sync_level\":\"write\"}");
        inserted.deinit();
        var backend = memory.Backend.init(a, .{});
        defer backend.close();
        var store = try backend.runtimeStore(a, .{ .name = "system/terminal-cut" });
        defer store.deinit();
        var durable = sessions.DurableSessionStore.initRuntime(a, &store);
        var registry = sessions.SessionRegistry.init(&durable);
        defer registry.deinit(a);
        const info = try registry.beginForPrincipal(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = .repeatable_read, .mode = .read_write } }, 7, "alice");
        const Held = struct {
            lease: *Source.Lease,
            registry: *sessions.SessionRegistry,
            id: [16]u8,
            closes: usize = 0,
            outside_locks: bool = false,
            fn open(_: *anyopaque, _: std.mem.Allocator, _: reads.RelationalStatementScan) !reads.RelationalReadView {
                return error.TestUnexpectedResult;
            }
            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.closes += 1;
                if (self.registry.mutex.inner.tryLock()) {
                    self.registry.mutex.inner.unlock();
                    if (self.registry.tryAcquireCommitExecution(self.id)) |execution| {
                        self.outside_locks = true;
                        execution.release();
                    }
                }
                self.lease.deinit();
            }
        };
        var held: Held = .{ .lease = &reader, .registry = &registry, .id = info.txn_id };
        const cut = try shared.Shared.create(a, .{ .ptr = &held, .vtable = &.{ .open = Held.open, .close = Held.close } }, &registry.snapshot_retirement);
        var cut_owned = true;
        defer if (cut_owned) cut.release();
        try registry.installNativeSqlCut(a, info.txn_id, cut);
        cut.release();
        cut_owned = false;
        var request = try sessions.parseCommitRequest(a, "{\"read_set\":[],\"tables\":{\"docs\":{\"inserts\":{\"a\":{\"v\":2}}}}}");
        defer request.deinit(a);
        var sealed = (try registry.cloneCommitRequest(a, info.txn_id, &request)) orelse return error.TestExpectedCommitPlan;
        sealed.deinit(a);
        const tables = try request.distributedTables(a);
        defer tables.deinit();
        var plan = (try registry.sealExecutionPlan(a, info.txn_id, tables.tables)) orelse return error.TestExpectedCommitPlan;
        plan.deinit();
        const context: request_operation.RequestContext = .{ .deadline_ns = platform_time.monotonicNs() +| std.time.ns_per_s };
        try std.testing.expectEqual(@as(usize, 1), entry.active_users);
        try std.testing.expect((try source.tryAcquireServingCatalogOwner(1, "docs", path, descriptor, context)) == null);
        const execution = registry.tryAcquireCommitExecution(info.txn_id) orelse return error.TestExpectedExecutionLease;
        var execution_active = true;
        defer if (execution_active) execution.release();
        // A failed local terminal publication retains the exact live owner.
        durable.fail_writes_for_test = true;
        if (abort) {
            try std.testing.expectError(error.InjectedSessionStoreFailure, registry.recordTerminalAbort(a, info.txn_id));
        } else {
            try std.testing.expectError(error.InjectedSessionStoreFailure, registry.recordTerminalCommit(a, info.txn_id, .committed_recovery_pending, 1, "docs"));
        }
        durable.fail_writes_for_test = false;
        registry.snapshot_retirement.drain();
        try std.testing.expectEqual(@as(usize, 0), held.closes);
        try std.testing.expectEqual(@as(usize, 1), entry.active_users);
        if (abort) {
            _ = (try registry.recordTerminalAbort(a, info.txn_id)) orelse return error.TestExpectedTerminal;
        } else {
            var committed = try reader.owner().batchJson("docs", "{\"inserts\":{\"a\":{\"v\":2}},\"sync_level\":\"write\"}");
            committed.deinit();
            _ = (try registry.recordTerminalCommit(a, info.txn_id, .committed_recovery_pending, 1, "docs")) orelse return error.TestExpectedTerminal;
        }
        // Terminal recording only retires the cut. Its real owner closes
        // after the coordinator drops execution ownership, never under locks.
        try std.testing.expectEqual(@as(usize, 0), held.closes);
        execution.release();
        execution_active = false;
        try std.testing.expectEqual(@as(usize, 1), held.closes);
        try std.testing.expect(held.outside_locks);
        try std.testing.expectEqual(@as(usize, 0), entry.active_users);
        var retained = (try registry.getExecutionPlan(a, info.txn_id)) orelse return error.TestExpectedCommitPlan;
        defer retained.deinit();
        try std.testing.expectEqualStrings("docs", retained.value[0].table_name);
        if (!abort) {
            var terminal = (try registry.getTerminalCommit(a, info.txn_id)) orelse return error.TestExpectedTerminal;
            defer terminal.deinit(a);
            try std.testing.expectEqual(sessions.TerminalCommitStatus.committed_recovery_pending, terminal.status);
            try std.testing.expect(!terminal.coordinator_acknowledged);
        }
        var serving = (try source.tryAcquireServingCatalogOwner(1, "docs", path, descriptor, context)) orelse return error.TestExpectedLease;
        defer serving.deinit();
        const reconciled = try serving.owner().reconcile("docs", "{\"version\":2,\"storage_mode\":\"document\"}", "{}", null, false);
        try std.testing.expectEqual(abi.ReconcileState.complete, reconciled.state);
    }
}

test "SQL relation lifetime initialized native owner reports actual source while catalog admission remains deferred" {
    const services = @import("../storage/kernel_runtime_services.zig");
    const P = @import("antfly_local_sources").common_topology_records.RelationPersistence;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const parent = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(parent);
    const path = try std.fmt.allocPrint(a, "{s}/deferred-native-report", .{parent});
    defer a.free(path);
    var io = std.testing.io;
    const borrow = services.executor.Borrow.init(&io);
    var context: client.Context = .{};
    try std.testing.expectEqual(abi.Status.ok, services.antfly_storage_context_create_with_runtime(&.{ .io = &borrow }, &context.handle));
    defer context.deinit();
    const instance = (try context.metrics()).execution_instance;
    var source = ProvisionedKernelOwnerSource.init(a, path, table_catalog.emptyCatalogSource(), read_gate.alreadyReadSafeBarrier());
    _ = source.withStorageContextHandle(context.handle);
    defer source.deinit();
    try source.admitRelationHost(.{ .node_id = 9, .store_id = 11, .execution_instance = instance, .generation_instance = instance, .data_generation = 1 }, .{ .instance = instance, .unclean_restart = false });
    const descriptor: descriptor_contract.Descriptor = .{
        .lsm_root_generation = table_reads.backend_current_root_generation,
        .identity = .{ .table_id = 7, .shard_id = 172, .range_id = 272 },
        .sql_persistence = P.Descriptor{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = instance, .data_generation = 1 },
    };
    var initialize = try source.acquireDescriptorWithMode(172, "rows", path, descriptor, true, .transient, .{ .native_initialization = true });
    const binding = try initialize.owner().nativeRelationBinding(a, "rows");
    initialize.requestTransientRetirement();
    initialize.deinit();
    const boots = @import("../metadata/sql_relation_boots.zig");
    const attestation = @import("../metadata/sql_relation_attestation.zig");
    const ownership = @import("group_work_ownership.zig");
    const root = try @import("antfly_local_sources").storage_db_root_signing_identity.loadOrCreate(a, std.testing.io, path);
    const identity: @import("../metadata/store_root_enrollment.zig").Identity = .{ .metadata_incarnation = @splat('a'), .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key };
    const Observation = struct {
        value: boots.Observation,
        fn read(raw: *anyopaque, _: std.mem.Allocator, query: boots.Query, _: ?u64) !boots.Observation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (query.node != 9 or query.binding_table != 7) return error.SqlRelationOwnerChanged;
            return self.value;
        }
    };
    var observed: Observation = .{ .value = .{
        .record = .{ .node_id = 9, .identity = identity, .reporter_incarnation = 1, .generation = 1, .instance = instance, .claim_digest = @splat(1), .unclean_restart = false, .phase = .ready },
        .registration_current = true,
        .initialized = try attestation.SignedBinding.sign(.{ .kind = .initialized, .metadata_group_id = 1, .boot_generation = 1, .identity = identity, .reporter_incarnation = 1, .binding = binding }, root),
    } };
    var metadata_callbacks = table_catalog.emptyCatalogSource().vtable.*;
    metadata_callbacks.sql_relation_boot = Observation.read;
    source.catalog = .{ .ptr = &observed, .vtable = &metadata_callbacks };
    const Scheduler = struct {
        owner: *ProvisionedKernelOwnerSource,
        raft_calls: usize = 0,
        fn raft(raw: *anyopaque, _: u64) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.raft_calls += 1;
            return false;
        }
        fn native(raw: *anyopaque, alloc: std.mem.Allocator, selection: ownership.NativeSelection, input: request_operation.RequestContext) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.owner.mayCoordinateNativeMaintenance(alloc, selection, 9, 11, input);
        }
    };
    var scheduler: Scheduler = .{ .owner = &source };
    const leadership: ownership.Source = .{ .ptr = &scheduler, .vtable = &.{ .is_local_leader = Scheduler.raft, .native_local = Scheduler.native } };
    try std.testing.expect(try ownership.mayCoordinate(leadership, a, 7, 172, descriptor.sql_persistence, .serving, .{}));
    observed.value.registration_current = false;
    try std.testing.expect(!try ownership.mayCoordinate(leadership, a, 7, 172, descriptor.sql_persistence, .serving, .{}));
    observed.value.registration_current = true;
    observed.value.record.?.generation += 1;
    try std.testing.expect(!try ownership.mayCoordinate(leadership, a, 7, 172, descriptor.sql_persistence, .serving, .{}));
    observed.value.record.?.generation -= 1;
    const previous_instance = observed.value.record.?.instance;
    observed.value.record.?.instance = @splat(99);
    try std.testing.expect(!try ownership.mayCoordinate(leadership, a, 7, 172, descriptor.sql_persistence, .serving, .{}));
    observed.value.record.?.instance = previous_instance;
    var other_generation = descriptor.sql_persistence.?;
    other_generation.data_generation += 1;
    try std.testing.expect(!try ownership.mayCoordinate(leadership, a, 7, 172, other_generation, .serving, .{}));
    observed.value.record.?.phase = .draining;
    try std.testing.expect(!try ownership.mayCoordinate(leadership, a, 7, 172, descriptor.sql_persistence, .serving, .{}));
    // Scheduling retirement during drain is distinct from serving or any
    // permission to open/read/apply the protected native owner.
    try std.testing.expect(try ownership.mayCoordinate(leadership, a, 7, 172, descriptor.sql_persistence, .retirement, .{}));
    observed.value.record.?.phase = .ready;
    try std.testing.expect(try ownership.mayCoordinate(leadership, a, 7, 172, descriptor.sql_persistence, .serving, .{}));
    try std.testing.expectEqual(@as(usize, 0), scheduler.raft_calls);
    try std.testing.expect(!try ownership.mayCoordinate(leadership, a, 7, 172, null, .serving, .{}));
    try std.testing.expectEqual(@as(usize, 1), scheduler.raft_calls);
    var current = try source.acquireDescriptorWithMode(172, "rows", path, descriptor, false, .resident, .{ .native_initialized = binding });
    defer current.deinit();
    try qualifyNativeCollectedAck(&source, &current, descriptor.sql_persistence.?, binding, identity.metadata_incarnation);
    try qualifyNativeMissingCoordinatorAck(&source, &current, descriptor.sql_persistence.?, binding, identity.metadata_incarnation);
    try qualifyNativeDirectoryRetirement(&source, &current, descriptor.sql_persistence.?, binding, identity.metadata_incarnation);
    const entry = current.entry;
    // This is the real process-local admission state installed by a fenced
    // catalog open. The physical owner and its initialized image stay intact.
    ProvisionedKernelOwnerSource.lock(&source.mutex);
    entry.catalog_deferred = true;
    source.mutex.unlock();
    current.deinit();
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptorWithMode(172, "rows", path, descriptor, false, .resident, .{ .native_initialized = binding }));
    var reporting = try source.acquireDescriptorWithMode(172, "rows", path, descriptor, false, .resident, .{ .native_initialized = binding, .allow_deferred_catalog = true });
    defer reporting.deinit();
    var response = try reporting.owner().runtimeStatusJson("rows");
    defer response.deinit();
    var parsed = try std.json.parseFromSlice(runtime_status.LocalTableRuntimeStatus, a, response.bytes(), .{});
    defer parsed.deinit();
    const actual = parsed.value.metadata.source_authority orelse return error.IndexOwnerReceiptChanged;
    try std.testing.expectEqual(.native, actual.kind);
    const namespace = @import("antfly_local_sources").storage_db_online_source_contract.namespaceBytes(.{ .table_id = binding.table_id, .shard_id = binding.shard_id, .range_id = binding.range_id });
    try std.testing.expectEqualSlices(u8, &namespace, &actual.namespace);
    const physical = try reporting.owner().nativeRelationBinding(a, "rows");
    try std.testing.expect((try P.PhysicalPin.fromBinding(binding)).eql(try P.PhysicalPin.fromBinding(physical)));
    try std.testing.expect(entry.catalog_deferred);
    reporting.deinit();
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptorWithMode(172, "rows", path, descriptor, false, .resident, .{ .native_initialized = binding }));
}

fn qualifyNativeCollectedAck(source: *ProvisionedKernelOwnerSource, lease: *ProvisionedKernelOwnerSource.Lease, descriptor: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor, initialized: @import("antfly_local_sources").common_topology_records.RelationPersistence.NativeBinding, incarnation: [32]u8) !void {
    const registry = @import("../system_catalog/prepared_transactions.zig");
    const P = @import("antfly_local_sources").common_topology_records.RelationPersistence;
    const a = std.testing.allocator;
    const now = platform_time.realtimeNs();
    const route: metadata_api.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .catalog_revision = 2, .table_id = 7, .topology_epoch = 7, .route = .{ .group_id = 172, .range_id = 272, .identity_namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .execution = .{ .native_local = descriptor } }, .native_participant = try P.PhysicalPin.fromBinding(initialized) };
    const manifest = [_]registry.Participant{
        .{ .table_name = "coordinator", .route = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .catalog_revision = 2, .table_id = 8, .topology_epoch = 7, .route = .{ .group_id = 173, .range_id = 273, .identity_namespace = .{ .table_id = 8, .shard_id = 173, .range_id = 273 } } } },
        .{ .table_name = "rows", .route = route },
    };
    const identity: registry.Identity = .{ .gid = "collected-native-ack", .database_id = 1, .principal = .bootstrap, .txn_id = @splat(61), .operation_id = @splat(62), .metadata_incarnation = incarnation, .begin_timestamp = now - 2, .expires_at_unix_ns = now - 1, .plan_digest = @splat(63), .manifest_digest = try registry.manifestDigest(a, &manifest, incarnation) };
    const owner = try identity.owner(a);
    const metadata = struct {
        record: registry.Record,
        realm: [16]u8,
        calls: usize = 0,
        pub fn systemCatalog(self: *@This(), allocator: std.mem.Allocator, _: request_operation.RequestContext, _: anytype) ![]u8 {
            return std.json.Stringify.valueAlloc(allocator, registry.Snapshot{ .incarnation = self.realm, .head = 1, .active = 1, .records = &.{self.record} }, .{});
        }
        fn observe(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: metadata_api.CatalogRouteFence, _: [16]u8, _: registry.Owner, _: request_operation.RequestContext) !registry.Decision {
            return error.UnexpectedDecisionRead;
        }
        fn complete(raw: *anyopaque, allocator: std.mem.Allocator, table: []const u8, fence: metadata_api.CatalogRouteFence, txn: [16]u8, selected: registry.Owner, members: []const []const u8, context: request_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try @import("sql_native_recovery.zig").requireCompletedRetention(allocator, self, context, table, fence, txn, selected, members);
        }
    };
    var realm: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&realm, &incarnation);
    var canonical: metadata = .{ .record = .{ .identity = identity, .participants = &manifest, .phase = .resolved, .decision = .{ .status = .aborted, .version = now }, .resolutions = 3 }, .realm = realm };
    const previous = source.native_terminal_authority;
    source.native_terminal_authority = .{ .ptr = &canonical, .observe_fn = metadata.observe, .complete_ack_fn = metadata.complete };
    defer source.native_terminal_authority = previous;
    const admission = try source.validateNativeDecisionRoute(a, route, 172, "rows", owner);
    const participant = try @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroup(a, "rows", 172);
    defer a.free(participant);
    const abort: db_types.BatchRequest = .{ .transaction = .{ .resolve = .{ .txn_id = identity.txn_id, .status = .aborted, .commit_version = now, .external_decision_owner = owner } }, .sync_level = .write };
    const abort_json = try table_writes.encodeStorageKernelBatchRequest(a, abort);
    defer a.free(abort_json);
    try source.applyCapturedTransactionOwner(a, lease, "rows", abort, .{}, route, admission, abort_json);
    const ack: db_types.BatchRequest = .{ .transaction = .{ .acknowledge = .{ .txn_id = identity.txn_id, .participant = participant, .external_decision_owner = owner } }, .sync_level = .write };
    const ack_json = try table_writes.encodeStorageKernelBatchRequest(a, ack);
    defer a.free(ack_json);
    try source.applyCapturedTransactionOwner(a, lease, "rows", ack, .{}, route, admission, ack_json);
    try std.testing.expectEqual(@as(usize, 0), canonical.calls);
    const cleanup: db_types.BatchRequest = .{ .transaction = .{ .cleanup = .{ .txn_id = identity.txn_id, .cutoff_timestamp = now, .retained_cutoff_timestamp = now } }, .sync_level = .write };
    const cleanup_json = try table_writes.encodeStorageKernelBatchRequest(a, cleanup);
    defer a.free(cleanup_json);
    var reclaimed = try lease.owner().replicatedBatchJson("rows", cleanup_json);
    reclaimed.deinit();
    try std.testing.expectError(error.TxnNotFound, lease.owner().externalTransactionStatus("rows", identity.txn_id, owner));
    canonical.record.phase = .decided;
    canonical.record.resolutions = 0;
    try std.testing.expectError(error.CommitDecisionUnknown, source.applyCapturedTransactionOwner(a, lease, "rows", ack, .{}, route, admission, ack_json));
    try std.testing.expectEqual(.decided, canonical.record.phase);
    try std.testing.expectEqual(@as(usize, 1), canonical.calls);
    canonical.record.phase = .resolved;
    canonical.record.resolutions = 3;
    try source.applyCapturedTransactionOwner(a, lease, "rows", ack, .{}, route, admission, ack_json);
    try std.testing.expectEqual(@as(usize, 2), canonical.calls);
    const foreign_id = try @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroup(a, "coordinator", 173);
    defer a.free(foreign_id);
    var foreign_ack = ack;
    foreign_ack.transaction.?.acknowledge.participant = foreign_id;
    const foreign_ack_json = try table_writes.encodeStorageKernelBatchRequest(a, foreign_ack);
    defer a.free(foreign_ack_json);
    // Enlisted globally is insufficient: this actual peer owns only rows:172.
    try std.testing.expectError(error.InvalidParticipant, source.applyCapturedTransactionOwner(a, lease, "rows", foreign_ack, .{}, route, admission, foreign_ack_json));
    try std.testing.expectEqual(@as(usize, 3), canonical.calls);
    // Missing native primary resolution stays an error even when metadata has
    // completed. The fallback discharges retention ACKs exclusively.
    var missing_commit = abort;
    missing_commit.transaction.?.resolve.status = .committed;
    const missing_commit_json = try table_writes.encodeStorageKernelBatchRequest(a, missing_commit);
    defer a.free(missing_commit_json);
    try std.testing.expectError(error.TxnNotFound, source.applyCapturedTransactionOwner(a, lease, "rows", missing_commit, .{}, route, admission, missing_commit_json));
    try std.testing.expectEqual(@as(usize, 3), canonical.calls);
}

fn qualifyNativeMissingCoordinatorAck(source: *ProvisionedKernelOwnerSource, lease: *ProvisionedKernelOwnerSource.Lease, descriptor: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor, initialized: @import("antfly_local_sources").common_topology_records.RelationPersistence.NativeBinding, incarnation: [32]u8) !void {
    const P = @import("../system_catalog/prepared_transactions.zig");
    const a = std.testing.allocator;
    const now = platform_time.realtimeNs();
    const pin = try @import("antfly_local_sources").common_topology_records.RelationPersistence.PhysicalPin.fromBinding(initialized);
    const route: metadata_api.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .catalog_revision = 2, .table_id = 7, .topology_epoch = 7, .route = .{ .group_id = 172, .range_id = 272, .identity_namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .execution = .{ .native_local = descriptor } }, .native_participant = pin };
    const manifest = [_]P.Participant{
        .{ .table_name = "rows", .route = route },
        .{ .table_name = "peer", .route = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .catalog_revision = 2, .table_id = 8, .topology_epoch = 7, .route = .{ .group_id = 173, .range_id = 273, .identity_namespace = .{ .table_id = 8, .shard_id = 173, .range_id = 273 } } } },
    };
    const identity: P.Identity = .{ .gid = "missing-native-coordinator", .database_id = 1, .principal = .bootstrap, .txn_id = @splat(91), .operation_id = @splat(92), .metadata_incarnation = incarnation, .begin_timestamp = now - 2, .expires_at_unix_ns = now - 1, .plan_digest = @splat(93), .manifest_digest = try P.manifestDigest(a, &manifest, incarnation) };
    const owner = try identity.owner(a);
    const Authority = struct {
        record: P.Record,
        realm: [16]u8,
        calls: usize = 0,
        reclaim: ?*client.Owner = null,
        cutoff: u64 = 0,
        pub fn systemCatalog(self: *@This(), allocator: std.mem.Allocator, _: request_operation.RequestContext, _: anytype) ![]u8 {
            return std.json.Stringify.valueAlloc(allocator, P.Snapshot{ .incarnation = self.realm, .head = 1, .active = 1, .records = &.{self.record} }, .{});
        }
        fn observe(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: metadata_api.CatalogRouteFence, _: [16]u8, _: P.Owner, _: request_operation.RequestContext) !P.Decision {
            return error.UnexpectedDecisionRead;
        }
        fn complete(raw: *anyopaque, allocator: std.mem.Allocator, table: []const u8, fence: metadata_api.CatalogRouteFence, id: [16]u8, held: P.Owner, members: []const []const u8, context: request_operation.RequestContext) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try @import("sql_native_recovery.zig").requireCompletedRetention(allocator, self, context, table, fence, id, held, members);
            if (self.reclaim) |value| {
                self.reclaim = null;
                const cleanup: db_types.BatchRequest = .{ .transaction = .{ .cleanup = .{ .txn_id = id, .cutoff_timestamp = self.cutoff, .retained_cutoff_timestamp = self.cutoff } }, .sync_level = .write };
                const encoded = try table_writes.encodeStorageKernelBatchRequest(allocator, cleanup);
                defer allocator.free(encoded);
                var retired = try value.replicatedBatchJson(table, encoded);
                retired.deinit();
            }
        }
    };
    var realm: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&realm, &incarnation);
    var canonical: Authority = .{ .record = .{ .identity = identity, .participants = &manifest, .phase = .resolved, .decision = .{ .status = .aborted, .version = now }, .resolutions = 3 }, .realm = realm };
    const previous = source.native_terminal_authority;
    source.native_terminal_authority = .{ .ptr = &canonical, .observe_fn = Authority.observe, .complete_ack_fn = Authority.complete };
    defer source.native_terminal_authority = previous;
    const admission = try source.validateNativeDecisionRoute(a, route, 172, "rows", owner);
    const local_id = try @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroup(a, "rows", 172);
    defer a.free(local_id);
    const peer = try @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroup(a, "peer", 173);
    defer a.free(peer);
    const abort: db_types.BatchRequest = .{ .transaction = .{ .resolve = .{ .txn_id = identity.txn_id, .status = .aborted, .commit_version = now, .external_decision_owner = owner } }, .sync_level = .write };
    const abort_json = try table_writes.encodeStorageKernelBatchRequest(a, abort);
    defer a.free(abort_json);
    try source.applyCapturedTransactionOwner(a, lease, "rows", abort, .{}, route, admission, abort_json);
    {
        var state = try lease.owner().externalTransactionRetention(a, "rows", 172, identity.txn_id, owner, pin);
        defer state.deinit();
        try std.testing.expect(!state.coordinator and state.status == .aborted and state.intents_resolved);
        try std.testing.expectEqual(@as(usize, 1), state.participants.value.len);
        try std.testing.expectEqualStrings(local_id, state.participants.value[0]);
    }
    var changed = owner;
    changed.operation_id = @splat(94);
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, lease.owner().externalTransactionRetention(a, "rows", 172, identity.txn_id, changed, pin));
    var wrong = pin;
    wrong.data_generation += 1;
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, lease.owner().externalTransactionRetention(a, "rows", 172, identity.txn_id, owner, wrong));
    try std.testing.expectError(error.ExternalTransactionDecisionChanged, lease.owner().externalTransactionRetention(a, "rows", 72, identity.txn_id, owner, pin));
    const LegacyStatus = extern struct {
        version: u32 = abi.abi_version - 1,
        status: abi.TxnStatus = .pending,
        guard: [64]u8 = @splat(0xA5),
        alignment: u64 = 0,
    };
    var previous_status: LegacyStatus = .{};
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_transaction_status(lease.owner().handle, &.{ .version = abi.abi_version - 1 }, @ptrCast(&previous_status)));
    try std.testing.expectEqual(abi.abi_version - 1, previous_status.version);
    try std.testing.expect(std.mem.allEqual(u8, &previous_status.guard, 0xA5));
    const ack: db_types.BatchRequest = .{ .transaction = .{ .acknowledge_many = .{ .txn_id = identity.txn_id, .participants = &.{ local_id, peer }, .external_decision_owner = owner } }, .sync_level = .write };
    const encoded = try table_writes.encodeStorageKernelBatchRequest(a, ack);
    defer a.free(encoded);
    // The real CABI/core refuses this roster before the Source can select its
    // authenticated exact local custody. No foreign member is added locally.
    try std.testing.expectError(error.InvalidParticipant, lease.owner().replicatedBatchJson("rows", encoded));
    canonical.record.phase = .decided;
    canonical.record.resolutions = 0;
    try std.testing.expectError(error.CommitDecisionUnknown, source.applyCapturedTransactionOwner(a, lease, "rows", ack, .{}, route, admission, encoded));
    canonical.record.phase = .resolved;
    canonical.record.resolutions = 3;
    try source.applyCapturedTransactionOwner(a, lease, "rows", ack, .{}, route, admission, encoded);
    try source.applyCapturedTransactionOwner(a, lease, "rows", ack, .{}, route, admission, encoded);
    var retained = try lease.owner().externalTransactionRetention(a, "rows", 172, identity.txn_id, owner, pin);
    defer retained.deinit();
    try std.testing.expectEqual(@as(usize, 1), retained.participants.value.len);
    try std.testing.expectEqualStrings(local_id, retained.participants.value[0]);
    var unrelated = ack;
    unrelated.transaction.?.acknowledge_many.participants = &.{ local_id, "outsider" };
    const unrelated_json = try table_writes.encodeStorageKernelBatchRequest(a, unrelated);
    defer a.free(unrelated_json);
    try std.testing.expectError(error.InvalidParticipant, source.applyCapturedTransactionOwner(a, lease, "rows", unrelated, .{}, route, admission, unrelated_json));
    // Reclaim after actual CABI status, before the selected local ACK. Only
    // fresh completed-manifest proof may discharge this lost-reply window.
    canonical.reclaim = lease.owner();
    canonical.cutoff = now;
    try source.applyCapturedTransactionOwner(a, lease, "rows", ack, .{}, route, admission, encoded);
    try std.testing.expectError(error.TxnNotFound, lease.owner().externalTransactionStatus("rows", identity.txn_id, owner));
    try source.applyCapturedTransactionOwner(a, lease, "rows", ack, .{}, route, admission, encoded);
    // A normally begun coordinator retains its full actual global roster;
    // it must take the original all-member ACK path, not the singleton lane.
    var begun_identity = identity;
    begun_identity.gid = "begun-native-coordinator";
    begun_identity.txn_id = @splat(95);
    begun_identity.operation_id = @splat(96);
    begun_identity.expires_at_unix_ns = now + std.time.ns_per_min;
    const begun_owner = try begun_identity.owner(a);
    const begin: db_types.BatchRequest = .{ .transaction = .{ .begin = .{ .txn_id = begun_identity.txn_id, .begin_timestamp = begun_identity.begin_timestamp, .created_at_ns = now, .topology_epoch = 7, .retain_terminal = true, .participants = &.{ local_id, peer }, .external_decision_owner = begun_owner } }, .sync_level = .write };
    const begin_json = try table_writes.encodeStorageKernelBatchRequest(a, begin);
    defer a.free(begin_json);
    var started = try lease.owner().replicatedBatchJson("rows", begin_json);
    started.deinit();
    var begun_abort = abort;
    begun_abort.transaction.?.resolve.txn_id = begun_identity.txn_id;
    begun_abort.transaction.?.resolve.external_decision_owner = begun_owner;
    const begun_abort_json = try table_writes.encodeStorageKernelBatchRequest(a, begun_abort);
    defer a.free(begun_abort_json);
    var resolved = try lease.owner().replicatedBatchJson("rows", begun_abort_json);
    resolved.deinit();
    var begun = try lease.owner().externalTransactionRetention(a, "rows", 172, begun_identity.txn_id, begun_owner, pin);
    defer begun.deinit();
    try std.testing.expect(begun.coordinator and begun.participants.value.len == 2);
    var begun_ack = ack;
    begun_ack.transaction.?.acknowledge_many.txn_id = begun_identity.txn_id;
    begun_ack.transaction.?.acknowledge_many.external_decision_owner = begun_owner;
    const begun_ack_json = try table_writes.encodeStorageKernelBatchRequest(a, begun_ack);
    defer a.free(begun_ack_json);
    const callback_calls = canonical.calls;
    const begun_admission = try source.validateNativeDecisionRoute(a, route, 172, "rows", begun_owner);
    try source.applyCapturedTransactionOwner(a, lease, "rows", begun_ack, .{}, route, begun_admission, begun_ack_json);
    try std.testing.expectEqual(callback_calls, canonical.calls);
}

/// Qualifies real native CABI terminal observation plus the existing metadata
/// writer primitives. Context.enrollment models the accepting catalog cut,
/// using the actual initialized image Pin; this is not a catalog-admission test.
fn qualifyNativeDirectoryRetirement(source: *ProvisionedKernelOwnerSource, lease: *ProvisionedKernelOwnerSource.Lease, descriptor: @import("antfly_local_sources").common_topology_records.RelationPersistence.Descriptor, initialized: @import("antfly_local_sources").common_topology_records.RelationPersistence.NativeBinding, incarnation: [32]u8) !void {
    const local = @import("antfly_local_sources");
    const a = std.testing.allocator;
    const native_route: metadata_api.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .catalog_revision = 2, .table_id = 7, .topology_epoch = 7, .route = .{ .group_id = 172, .range_id = 272, .identity_namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .execution = .{ .native_local = descriptor } }, .native_participant = try local.common_topology_records.RelationPersistence.PhysicalPin.fromBinding(initialized) };
    _ = try qualifyDirectoryRetirement(lease.owner(), source.replica_root_dir, native_route, initialized.schema_version, incarnation, false);
    const path = try std.fmt.allocPrint(a, "{s}-ordered-owner", .{source.replica_root_dir});
    defer a.free(path);
    const physical: descriptor_contract.Descriptor = .{ .lsm_root_generation = table_reads.backend_current_root_generation, .identity = .{ .table_id = 8, .shard_id = 273, .range_id = 373 }, .schema_json = "{\"version\":1,\"storage_mode\":\"document\"}" };
    var raft = try source.acquireDescriptorWithMode(173, "rows", path, physical, true, .transient, .{});
    defer raft.deinit();
    const raft_route: metadata_api.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .catalog_revision = 2, .table_id = 8, .topology_epoch = 7, .route = .{ .group_id = 173, .range_id = 373, .identity_namespace = .{ .table_id = 8, .shard_id = 273, .range_id = 373 } } };
    const query = try qualifyDirectoryRetirement(raft.owner(), path, raft_route, 1, incarnation, true);
    raft.requestTransientRetirement();
    raft.deinit();
    var cold = try source.acquireDescriptorWithMode(173, "rows", path, physical, false, .transient, .{});
    defer cold.deinit();
    const raw = try std.json.Stringify.valueAlloc(a, query, .{});
    defer a.free(raw);
    const lookup = try local.api_local_query_contract.encodeStorageKernelLookupRequest(a, "", .{ .sql_lock_query_json = raw });
    defer a.free(lookup);
    var observed = try cold.owner().lookupJson("rows", lookup);
    defer observed.deinit();
    var state = try std.json.parseFromSlice(local.storage_pessimistic_lock_control.Snapshot, a, observed.bytes(), .{});
    defer state.deinit();
    try std.testing.expect(state.value.frozen.?.retired and state.value.scope == null and state.value.owner == null);
    var wrong_group = query;
    wrong_group.admission.binding.namespace.group += 1;
    const changed = try std.json.Stringify.valueAlloc(a, wrong_group, .{});
    defer a.free(changed);
    const wrong_lookup = try local.api_local_query_contract.encodeStorageKernelLookupRequest(a, "", .{ .sql_lock_query_json = changed });
    defer a.free(wrong_lookup);
    try std.testing.expectError(error.IdentityNamespaceMismatch, cold.owner().lookupJson("rows", wrong_lookup));
}

fn qualifyDirectoryRetirement(physical: *client.Owner, root_path: []const u8, route: metadata_api.CatalogRouteFence, schema_version: u32, incarnation: [32]u8, ordered: bool) !@import("antfly_local_sources").storage_pessimistic_lock_control.Query {
    const local = @import("antfly_local_sources");
    const L = @import("../system_catalog/lock_authority.zig");
    const P = L.prepared;
    const locks = local.storage_pessimistic_lock_control;
    const DocStore = local.storage_docstore.DocStore;
    const backing = std.testing.allocator;
    var region = std.heap.ArenaAllocator.init(backing);
    defer region.deinit();
    const a = region.allocator();
    const path = try std.fmt.allocPrintSentinel(a, "{s}-metadata-retirement", .{root_path}, 0);
    var metadata = try DocStore.open(backing, path, .{});
    defer metadata.close();
    var realm: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&realm, &incarnation);
    const ns: L.scopes.ledger.Namespace = .{ .home = .metadata, .group = 1, .incarnation = realm };
    const backend: L.locks.Backend = .{ .incarnation = realm, .id = @splat(71), .fence = 1 };
    const command: L.scopes.Command = .{ .sequence = 1, .nonce = @splat(72), .executor = @splat(73) };
    const now = platform_time.realtimeNs();
    const registration: L.scopes.Registration = .{ .group = 1, .lease = .{ .backend = backend, .database = 1, .node = 9, .principal = try L.principalDigest(a, .bootstrap), .expires_at_ms = now / std.time.ns_per_ms + 60000 }, .connection = @splat('a'), .nonce = @splat(74), .standalone = false };
    const spec: L.scopes.Data.Spec = .{ .table_name = "rows", .route = route, .schema_version = schema_version };
    const base_context: L.Context = .{ .a = a, .actor = .{ .session_user = .bootstrap, .credential = "" }, .database = 1, .standalone = false, .maintenance = true, .enrollment = spec };
    var identity: P.Identity = .{ .kind = .session_commit, .gid = "", .database_id = 1, .principal = .bootstrap, .txn_id = @splat(75), .operation_id = @splat(76), .metadata_incarnation = incarnation, .begin_timestamp = now, .expires_at_unix_ns = now + 5 * std.time.ns_per_min, .plan_digest = @splat(77), .manifest_digest = undefined, .session = .{ .owner_node_id = 9, .connection_id = registration.connection }, .lock_scope = .{ .backend = backend, .command = command } };
    var participant: L.scopes.Data.Participant = undefined;
    var manifest: [1]P.Participant = undefined;
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        try L.scopes.register(a, &txn, ns, registration, base_context);
        _ = try L.scopes.begin(a, &txn, ns, backend, command, base_context);
        try L.scopes.bindTransaction(a, &txn, ns, backend, command, identity.txn_id, base_context);
        participant = try L.scopes.Data.enroll(a, &txn, ns, backend, command, identity.txn_id, spec, base_context);
        var admitted = participant.admission;
        admitted.purpose = .resolution;
        admitted.serial += 1;
        manifest = .{.{ .table_name = "rows", .route = route, .schema_version = schema_version, .sql_locks = admitted }};
        identity.manifest_digest = try P.manifestDigest(a, &manifest, incarnation);
        const plan = try P.plan(a, &txn, 1, 1, .{ .admission = .{ .head = 1, .nonce = identity.operation_id }, .incarnation = realm, .standalone = false, .database_id = 1, .actor = base_context.actor, .operation = .{ .admit = .{ .identity = identity, .participants = &manifest, .admitted_at_ns = now } } });
        try plan.apply(&txn);
        const owner = try identity.owner(a);
        try L.scopes.seal(a, &txn, ns, backend, command, .{ .operation = owner.operation_id, .admission = owner.admission_digest }, base_context);
        try std.testing.expectEqual(@as(u64, 1), try L.scopes.Data.tableCount(&txn, ns, route.table_id));
        try txn.commit();
    }
    const H = struct {
        fn advance(a1: std.mem.Allocator, txn: anytype, selected: P.Identity, nonce: u8, action: P.Action) !void {
            const current = (try P.readOperation(a1, txn, 1, selected.operation_id)).?;
            const planned = try P.plan(a1, txn, 1, current.revision, .{ .admission = .{ .head = current.revision, .nonce = @splat(nonce) }, .incarnation = realmFrom(selected.metadata_incarnation), .standalone = false, .database_id = 1, .actor = .{ .session_user = .bootstrap, .credential = "" }, .operation = .{ .advance = .{ .owner = try selected.owner(a1), .expected_revision = current.revision, .action = action } } });
            try planned.apply(txn);
        }
        fn realmFrom(text: [32]u8) [16]u8 {
            var result: [16]u8 = undefined;
            _ = std.fmt.hexToBytes(&result, &text) catch unreachable;
            return result;
        }
    };
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        try H.advance(a, &txn, identity, 79, .{ .abort = now + 1 });
        try txn.commit();
    }
    const owner = try identity.owner(a);
    const ref = (try manifest[0].lockReference(identity, a)).?;
    const txn_hex = std.fmt.bytesToHex(identity.txn_id, .lower);
    const abort = try std.json.Stringify.valueAlloc(a, .{ ._transaction = .{ .phase = "external_resolve_v1", .txn_id = &txn_hex, .status = "aborted", .commit_version = now + 1, .external_decision_owner = owner }, .sync_level = "write" }, .{});
    var aborted = if (ordered) try physical.replicatedBatchAtRaftEntryJson("rows", abort, 1, 1) else try physical.replicatedBatchJson("rows", abort);
    aborted.deinit();
    const control: ?locks.NativeControl = if (ordered) null else .{ .route = route, .owner = owner, .decision = ref.decision };
    const native_resolve = try local.api_batch.encodeOwnerBatchRequest(a, .{ .sql_lock = .{ .admission = ref.admission, .native_control = control, .operation = .{ .resolve = ref.decision } }, .sync_level = .write });
    var retired = if (ordered) try physical.replicatedBatchAtRaftEntryJson("rows", native_resolve, 1, 2) else try physical.batchJson("rows", native_resolve);
    retired.deinit();
    const query = try std.json.Stringify.valueAlloc(a, locks.Query{ .admission = ref.admission, .native_control = control }, .{});
    const lookup = try local.api_local_query_contract.encodeStorageKernelLookupRequest(a, "", .{ .sql_lock_query_json = query });
    var observed = try physical.lookupJson("rows", lookup);
    defer observed.deinit();
    const proof = try std.json.parseFromSliceLeaky(locks.Snapshot, a, observed.bytes(), .{ .allocate = .alloc_always });
    try std.testing.expect(proof.scope == null and proof.owner == null and proof.prepared_owner == null and proof.frozen.?.retired);
    var context = base_context;
    context.data_proof = proof;
    {
        var txn = try metadata.beginWriteTxn();
        defer txn.abort();
        // The actual canonical ABORT precedes native delivery. Its terminal
        // Ref still cannot detach metadata custody before primary ACKs have
        // durably advanced the record from decided to resolved.
        try std.testing.expectError(error.LockOwnerBusy, L.scopes.Data.retire(a, &txn, ns, backend, participant.admission.generation, context));
        try std.testing.expectEqual(@as(u64, 1), try L.scopes.Data.tableCount(&txn, ns, route.table_id));
    }
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        try H.advance(a, &txn, identity, 80, .{ .resolve = 1 });
        try txn.commit();
    }
    metadata.close();
    metadata = try DocStore.open(backing, path, .{});
    {
        var txn = try metadata.beginWriteTxn();
        defer txn.abort();
        const root = (try L.scopes.read(a, &txn, ns, backend)).?;
        var pending = context;
        pending.data_proof.?.frozen = null;
        try std.testing.expectError(error.LockOwnerBusy, pending.requireDataRetirement(&txn, ns, root, participant));
        var foreign = participant;
        if (ordered) foreign.spec.route.route.group_id += 1 else foreign.spec.route.native_participant.?.physical_root_incarnation ^= 1;
        try std.testing.expectError(error.InvalidLockScope, context.requireDataRetirement(&txn, ns, root, foreign));
        var wrong = context;
        wrong.data_proof.?.frozen.?.ref.decision.admission[0] ^= 1;
        try std.testing.expectError(error.InvalidLockScope, wrong.requireDataRetirement(&txn, ns, root, participant));
        try std.testing.expectEqual(@as(u64, 1), try L.scopes.Data.tableCount(&txn, ns, route.table_id));
    }
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        try L.scopes.Data.retire(a, &txn, ns, backend, participant.admission.generation, context);
        try L.scopes.Data.retire(a, &txn, ns, backend, participant.admission.generation, context);
        try std.testing.expectEqual(@as(u64, 0), try L.scopes.Data.tableCount(&txn, ns, route.table_id));
        try H.advance(a, &txn, identity, 81, .{ .native_locks_released = 1 });
        try txn.commit();
    }
    metadata.close();
    metadata = try DocStore.open(backing, path, .{});
    var released = false;
    for (0..8) |_| {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        released = try L.scopes.releaseTransaction(a, &txn, ns, backend, .{ .id = identity.txn_id, .decision = ref.decision }, base_context);
        try txn.commit();
        if (released) break;
    }
    try std.testing.expect(released);
    {
        var txn = try metadata.beginWriteTxn();
        errdefer txn.abort();
        const root = (try L.scopes.read(a, &txn, ns, backend)).?;
        try std.testing.expect(root.transaction == null and root.data.?.count() == 0 and root.data.?.open == 0);
        var next = command;
        next.sequence += 1;
        next.nonce = @splat(82);
        _ = try L.scopes.begin(a, &txn, ns, backend, next, base_context);
        try H.advance(a, &txn, identity, 83, .handoff);
        try txn.commit();
    }
    const participant_id = try local.api_local_transaction_contract.participantIdForGroup(a, "rows", route.route.group_id);
    const ack_request: db_types.BatchRequest = .{
        .transaction = .{
            .acknowledge = .{
                .txn_id = identity.txn_id,
                .participant = participant_id,
                // Native retention ACKs carry the exact external owner. Raft's
                // existing ordinary ACK persists the consensus receipt instead.
                .external_decision_owner = if (ordered) null else owner,
            },
        },
        .sync_level = .write,
    };
    const ack = try local.api_batch.encodeOwnerBatchRequest(a, ack_request);
    if (ordered) {
        var wrong_dialect = ack_request;
        wrong_dialect.transaction.?.acknowledge.external_decision_owner = owner;
        const native_ack = try local.api_batch.encodeOwnerBatchRequest(a, wrong_dialect);
        try std.testing.expectError(error.InvalidBatchRequest, physical.replicatedBatchAtRaftEntryJson("rows", native_ack, 1, 3));
    }
    var acknowledged = if (ordered) try physical.replicatedBatchAtRaftEntryJson("rows", ack, 1, 3) else try physical.replicatedBatchJson("rows", ack);
    acknowledged.deinit();
    if (ordered) {
        // A rejected native dialect must leave the ordered position usable,
        // and replay of the genuine ACK remains idempotent before cold open.
        var replayed = try physical.replicatedBatchAtRaftEntryJson("rows", ack, 1, 3);
        replayed.deinit();
    }
    return .{ .admission = ref.admission, .native_control = control };
}

test "row-policy coupled schema deferred receipt lookup rejects mixed selectors before owner acquisition" {
    const Source = ProvisionedKernelOwnerSource;
    const receipt: db_types.LookupOptions.RowPolicyReceipt = .{ .generation = 2, .phase = .serving_install };
    try Source.validateRowPolicyReceiptLookupOptions("", .{ .row_policy_receipt = receipt, .execution_deadline_ns = 123 });
    try std.testing.expectError(error.InvalidRowPolicyReceipt, Source.validateRowPolicyReceiptLookupOptions("row", .{ .row_policy_receipt = receipt }));
    try std.testing.expectError(error.InvalidRowPolicyReceipt, Source.validateRowPolicyReceiptLookupOptions("", .{}));
    const mixed = [_]db_types.LookupOptions{
        .{ .row_policy_receipt = receipt, .include_primary_digest = true },
        .{ .row_policy_receipt = receipt, .include_json_null_fields = true },
        .{ .row_policy_receipt = receipt, .fields = &.{"id"} },
        .{ .row_policy_receipt = receipt, .include_all_fields = false },
        .{ .row_policy_receipt = receipt, .output_exact = true },
        .{ .row_policy_receipt = receipt, .row_policy_principal_proof = "proof" },
        .{ .row_policy_receipt = receipt, .row_policy_database = "main" },
        .{ .row_policy_receipt = receipt, .restore_staging_scope = @splat(1) },
        .{ .row_policy_receipt = receipt, .restore_staging_plan_id = @splat(1) },
        .{ .row_policy_receipt = receipt, .relational_integrity_catalog = true },
        .{ .row_policy_receipt = receipt, .relational_integrity_action = true },
        .{ .row_policy_receipt = receipt, .relational_integrity_jobs_json = "[]" },
        .{ .row_policy_receipt = receipt, .relational_activation_json = "{}" },
        .{ .row_policy_receipt = receipt, .relational_index_status_json = "{}" },
        .{ .row_policy_receipt = receipt, .relational_topology_json = "{}" },
        .{ .row_policy_receipt = receipt, .group_source_query_json = "{}" },
        .{ .row_policy_receipt = receipt, .sql_lock_query_json = "{}" },
        .{ .row_policy_receipt = receipt, .fk_generation_source_control = true },
        .{ .row_policy_receipt = receipt, .fk_generation_source_read_index_certified = true },
        .{ .row_policy_receipt = receipt, .generation_handoff_install_read_index_certified = true },
    };
    for (mixed) |opts| try std.testing.expectError(error.InvalidRowPolicyReceipt, Source.validateRowPolicyReceiptLookupOptions("", opts));
    // The production dispatch validates before it dereferences Source or
    // reaches an owner/metadata provider. This is not merely a helper test.
    var inert: u8 align(@alignOf(Source)) = 0;
    try std.testing.expectError(error.InvalidRowPolicyReceipt, Source.lookupGroupLocal(&inert, std.testing.allocator, 17, "rows", "", mixed[0], .stale));
    // Mixed receipt selectors reject before consuming the routed fence.
    for (mixed) |opts| try std.testing.expectError(error.InvalidRowPolicyReceipt, Source.lookupGroupLocalRouted(&inert, std.testing.allocator, undefined, 17, "rows", "", opts, .stale));
}

test "row-policy coupled Source CAPI desired descriptor preserves predecessor until exact ordered install" {
    const local = @import("antfly_local_sources");
    const policies = local.system_catalog_policies;
    const policy_bundle = local.storage_db_row_policy_bundle;
    const schema = local.storage_schema;
    const authority = local.usermgr_row_policy_authority;
    const before_json =
        \\{"version":5,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const after_json =
        \\{"version":6,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}}}
    ;
    const secret = "source-pair-real-owner-secret-32-bytes";
    const issuer = "source-pair-owner-test";
    const group: u64 = 17;
    const Helper = struct {
        fn reconcileServing(source: *ProvisionedKernelOwnerSource) !void {
            // An idle receipt-only owner may need one turn to retire before
            // the canonical serving descriptor can be acquired.
            for (0..8) |_| {
                switch ((try source.reconcileServingCatalogAtRoot(group, "rows", .{})).state) {
                    .complete => return,
                    .busy => continue,
                    else => return error.TestUnexpectedReconcileState,
                }
            }
            return error.TestServingReconcileDidNotComplete;
        }
        fn digest(a: std.mem.Allocator, json: []const u8) ![32]u8 {
            var public = try local.schema_mod.parseValidatedTableSchema(a, json);
            defer public.deinit(a);
            const native = try local.schema_mod.deriveRuntimeTableSchema(a, public);
            defer schema.freeSchema(a, native);
            const encoded_schema = try schema.serializeSchema(a, native);
            defer a.free(encoded_schema);
            var result: [32]u8 = undefined;
            std.crypto.hash.Blake3.hash(encoded_schema, &result, .{});
            return result;
        }
        fn record(json: []const u8, read: []const u8) [32]u8 {
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update(json);
            hash.update(&.{0});
            hash.update(read);
            hash.update(&.{0});
            hash.update("");
            hash.update(&.{0});
            hash.update("0");
            var result: [32]u8 = undefined;
            hash.final(&result);
            return result;
        }
        fn pair(a: std.mem.Allocator) !policies.SchemaPair {
            return .{ .before_schema_json = before_json, .after_schema_json = after_json, .before_schema_digest = try digest(a, before_json), .after_schema_digest = try digest(a, after_json), .before_record_digest = record(before_json, ""), .staged_record_digest = record(after_json, before_json), .after_record_digest = record(after_json, ""), .before_policy_generation = 1, .before_catalog_epoch = 2 };
        }
        fn request(a: std.mem.Allocator, generation: u64, phase: policies.Publication.Phase) !policies.InstallRequest {
            return .{ .table_id = 7, .expected_generation = generation, .expected_catalog_epoch = generation + 1, .expected_phase = phase, .owner_group_id = group, .expected_descriptor_digest = try (policies.OwnerDescriptor{ .table_id = 7, .group_id = group, .shard_id = 8, .range_id = 9, .schema_version = if (generation == 1) 5 else 6, .schema_digest = try digest(a, if (generation == 1) before_json else after_json), .range_start = "", .range_end = "" }).digest() };
        }
        fn bytes(a: std.mem.Allocator, generation: u64, phase: policies.Publication.Phase) ![]u8 {
            const record_: policies.Record = .{ .id = 1, .generation = generation, .table_id = 7, .schema_version = if (generation == 1) 5 else 6, .schema_digest = try digest(a, if (generation == 1) before_json else after_json), .name = "visible", .commands = .{ .select = true }, .roles = &.{"PUBLIC"}, .using = .{ .instructions = &.{.{ .type = .{ .kind = .boolean }, .operation = .{ .literal = .{ .bool = true } } }}, .root = 0 } };
            return std.json.Stringify.valueAlloc(a, policies.InstallSnapshot{ .table_id = 7, .schema_version = record_.schema_version, .schema_digest = record_.schema_digest, .policy_generation = generation, .catalog_epoch = generation + 1, .phase = phase, .schema_pair = if (generation == 1) null else try pair(a), .records = &.{record_}, .settings = &.{} }, .{});
        }
        fn bind(source: *ProvisionedKernelOwnerSource) void {
            source.row_policy_authority_secret = secret;
            source.row_policy_authority_issuer = issuer;
        }
        fn requireDurableSchema(source: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, version: u32, expected: []const u8) !void {
            // Inspect the real compiled owner's durable cut without supplying
            // a prospective schema hint or releasing deferred user admission.
            var lease = try source.acquireWithControls(group, "rows", .{ .durable_catalog = true, .historical_raft_apply = true, .allow_deferred_catalog = true });
            defer lease.deinit();
            const clock = @import("antfly_platform").time;
            const deadline = clock.monotonicNs() +| 2 * std.time.ns_per_s;
            var status = while (true) {
                const response = lease.owner().runtimeStatusJson("rows") catch |err| switch (err) {
                    error.StorageBusy => {
                        if (clock.monotonicNs() >= deadline) return err;
                        try std.testing.io.sleep(.fromMilliseconds(2), .awake);
                        continue;
                    },
                    else => return err,
                };
                break response;
            };
            defer status.deinit();
            var parsed = try std.json.parseFromSlice(struct { stats: struct { schema_epoch: u32 } }, a, status.bytes(), .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            try std.testing.expectEqual(version, parsed.value.stats.schema_epoch);
            const request_json = try table_reads.encodeStorageKernelLookupRequest(a, "", .{ .relational_topology_json = "{\"mode\":\"public_schema\"}" });
            defer a.free(request_json);
            var public = try lease.owner().lookupJson("rows", request_json);
            defer public.deinit();
            var decoded = try std.json.parseFromSlice([]const u8, a, public.bytes(), .{});
            defer decoded.deinit();
            try std.testing.expectEqualStrings(expected, decoded.value);
        }
        fn observe(source: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, generation: u64, phase: policies.Publication.Phase) !?policy_bundle.Receipt {
            var response = (try ProvisionedKernelOwnerSource.lookupGroupLocal(source, a, group, "rows", "", .{ .row_policy_receipt = .{ .generation = generation, .phase = phase } }, .read_index)) orelse return null;
            defer response.deinit(a);
            var parsed = try std.json.parseFromSlice(policy_bundle.Receipt, a, response.json, .{});
            defer parsed.deinit();
            return parsed.value;
        }
        fn applyCommitted(source: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, descriptor: descriptor_contract.Descriptor, req: db_types.BatchRequest, index: u64) !void {
            // This direct fixture has no MultiRaft retained-Ready driver.
            // The production retained-Ready driver also defers owner-cache
            // transition StorageBusy (data/runtime.zig). Replay that admission
            // yield and writer contention with the exact same body and cut.
            const clock = @import("antfly_platform").time;
            const deadline = clock.monotonicNs() +| 2 * std.time.ns_per_s;
            while (true) {
                source.applyPreparedReplicatedBatchGroupLocalAtRaftEntry(a, group, "rows", descriptor, req, 3, index) catch |err| switch (err) {
                    error.RaftApplyWriterUnavailable, error.StorageBusy => {
                        if (clock.monotonicNs() >= deadline) return err;
                        try std.testing.io.sleep(.fromMilliseconds(2), .awake);
                        continue;
                    },
                    else => return err,
                };
                return;
            }
        }
        fn apply(source: *ProvisionedKernelOwnerSource, a: std.mem.Allocator, descriptor: descriptor_contract.Descriptor, generation: u64, phase: policies.Publication.Phase, index: u64) !void {
            const raw = try bytes(a, generation, phase);
            defer a.free(raw);
            const input = try request(a, generation, phase);
            // Real compiled owner and exact ordered component cut. This does
            // not claim an external metadata all-owner publication or ACK.
            try applyCommitted(source, a, descriptor, .{ .row_policy_publication = input, .row_policy_install_bundle = raw, .sync_level = .write }, index);
            const receipt = (try observe(source, a, generation, phase)) orelse return error.TestMissingActualReceipt;
            var digest_: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(raw, &digest_, .{});
            try std.testing.expectEqual(generation, receipt.generation);
            try std.testing.expectEqual(phase, receipt.phase);
            try std.testing.expectEqual(@as(u64, 3), receipt.applied_term);
            try std.testing.expectEqual(index, receipt.applied_index);
            try std.testing.expectEqualSlices(u8, &digest_, &receipt.bundle_digest);
            try std.testing.expectEqualSlices(u8, &input.expected_descriptor_digest, &receipt.descriptor_digest);
        }
    };
    const Fixture = struct {
        tables: [1]@import("../metadata/table_manager.zig").TableRecord = .{.{ .table_id = 7, .name = "rows", .schema_json = before_json, .indexes_json = "" }},
        ranges: [1]@import("../metadata/table_manager.zig").RangeRecord = .{.{ .table_id = 7, .group_id = group, .range_id = 9, .doc_identity_shard_id = 8, .doc_identity_range_id = 9, .start_key = "", .end_key = null }},
        fn admin(raw: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.tables, .ranges = &self.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn catalog(self: *@This()) table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{ .admin_snapshot = admin, .free_admin_snapshot = free } };
        }
    };
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/pair-source", .{tmp.sub_path});
    defer a.free(root);
    const path = try std.fmt.allocPrint(a, "{s}/group-{d}/table-db", .{ root, group });
    defer a.free(path);
    var fixture: Fixture = .{};
    var source = ProvisionedKernelOwnerSource.init(a, root, fixture.catalog(), read_gate.alreadyReadSafeBarrier());
    var live = true;
    defer if (live) source.deinit();
    Helper.bind(&source);
    const before: descriptor_contract.Descriptor = .{ .lsm_root_generation = table_reads.backend_current_root_generation, .identity = .{ .table_id = 7, .shard_id = 8, .range_id = 9 }, .initial_range = .{ .start = "", .end = "" }, .schema_json = before_json, .indexes_json = "" };
    var after = before;
    after.schema_json = after_json;
    try Helper.applyCommitted(&source, a, before, .{ .writes = &.{.{ .key = "a", .value = "{\"id\":\"a\"}" }}, .relational_schema_version = 5, .sync_level = .write }, 10);
    try Helper.apply(&source, a, before, 1, .pending_install, 11);
    try Helper.apply(&source, a, before, 1, .serving_install, 12);
    // A value-free receipt observes durable custody but deliberately leaves
    // public admission deferred. Drive the existing serving reconciler only
    // after the real final receipt, rather than clearing its flag in a fixture.
    try Helper.reconcileServing(&source);
    {
        var predecessor = try source.acquireDescriptor(group, "rows", path, before);
        defer predecessor.deinit();
        const token = try authority.sign(a, secret, issuer, .{ .principal = "alice", .roles = &.{"PUBLIC"}, .auth_revision = 1 }, .{ .table_id = 7, .table = "rows", .database = "main", .policy_generation = 1, .catalog_epoch = 2 }, @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
        defer a.free(token);
        const encoded = try table_reads.encodeStorageKernelLookupRequest(a, "a", .{ .row_policy_principal_proof = token, .row_policy_database = "main" });
        defer a.free(encoded);
        var old_row = try predecessor.owner().lookupJson("rows", encoded);
        defer old_row.deinit();
        try std.testing.expect(std.mem.indexOf(u8, old_row.bytes(), "\"id\":\"a\"") != null);
    }
    // Exact production integration gap: desired metadata schema6 arrives
    // while the real compiled owner still serves installed policy1/schema5.
    fixture.tables[0].schema_json = after_json;
    try std.testing.expect((try Helper.observe(&source, a, 1, .serving_install)) != null);
    try std.testing.expect((try Helper.observe(&source, a, 2, .pending_install)) == null);
    try std.testing.expectError(error.InvalidRowPolicyReceipt, ProvisionedKernelOwnerSource.lookupGroupLocal(&source, a, group, "rows", "", .{ .row_policy_receipt = .{ .generation = 1, .phase = .serving_install }, .include_primary_digest = true }, .read_index));
    // A future descriptor must not publish its native schema before a
    // candidate exists, even when its predecessor's policy is already active.
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptor(group, "rows", path, after));
    try Helper.requireDurableSchema(&source, a, 5, before_json);
    source.deinit();
    live = false;
    source = ProvisionedKernelOwnerSource.init(a, root, fixture.catalog(), read_gate.alreadyReadSafeBarrier());
    live = true;
    Helper.bind(&source);
    try std.testing.expect((try Helper.observe(&source, a, 1, .serving_install)) != null);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptor(group, "rows", path, after));
    try Helper.requireDurableSchema(&source, a, 5, before_json);
    var changed_layout = before;
    changed_layout.schema_json =
        \\{"version":5,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"required":["id"],"additionalProperties":false}}}}
    ;
    // Cached desired6 rejects an older descriptor before opening the DB.
    try std.testing.expectError(error.StorageKernelOwnerStaleDescriptor, source.acquireDescriptor(group, "rows", path, changed_layout));
    try Helper.requireDurableSchema(&source, a, 5, before_json);
    // With no cached6 descriptor, the same-version layout mismatch reaches
    // the genuine cold native policy/schema pin instead of the cache guard.
    source.deinit();
    live = false;
    fixture.tables[0].schema_json = changed_layout.schema_json;
    source = ProvisionedKernelOwnerSource.init(a, root, fixture.catalog(), read_gate.alreadyReadSafeBarrier());
    live = true;
    Helper.bind(&source);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptor(group, "rows", path, changed_layout));
    fixture.tables[0].schema_json = after_json;
    try Helper.requireDurableSchema(&source, a, 5, before_json);
    try std.testing.expect((try Helper.observe(&source, a, 1, .serving_install)) != null);
    try std.testing.expect((try Helper.observe(&source, a, 2, .pending_install)) == null);
    try Helper.apply(&source, a, after, 2, .pending_install, 20);
    // Candidate receipt is real, but it does not authorize a normal new-schema
    // reader. Closed receipt controls remain available across cold acquisition.
    source.deinit();
    live = false;
    source = ProvisionedKernelOwnerSource.init(a, root, fixture.catalog(), read_gate.alreadyReadSafeBarrier());
    live = true;
    Helper.bind(&source);
    const pending = (try Helper.observe(&source, a, 2, .pending_install)).?;
    try std.testing.expectEqual(@as(u64, 20), pending.applied_index);
    try std.testing.expect((try Helper.observe(&source, a, 1, .serving_install)) != null);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptor(group, "rows", path, after));
    // That deferred attempt still caches desired6. An ordinary5 descriptor
    // is now a cache regression, independently of the pending Pair gate.
    try std.testing.expectError(error.StorageKernelOwnerStaleDescriptor, source.acquireDescriptor(group, "rows", path, before));
    try Helper.requireDurableSchema(&source, a, 5, before_json);
    source.deinit();
    live = false;
    source = ProvisionedKernelOwnerSource.init(a, root, fixture.catalog(), read_gate.alreadyReadSafeBarrier());
    live = true;
    Helper.bind(&source);
    // With no cached6, predecessor5 reaches the actual pending Pair fence.
    // Neither its real policy1 receipt nor the candidate2 receipt opens rows.
    try std.testing.expect((try Helper.observe(&source, a, 2, .pending_install)) != null);
    try std.testing.expect((try Helper.observe(&source, a, 1, .serving_install)) != null);
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, source.acquireDescriptor(group, "rows", path, before));
    try Helper.requireDurableSchema(&source, a, 5, before_json);
    try std.testing.expectEqual(abi.ReconcileState.busy, (try source.reconcileServingCatalogAtRoot(group, "rows", .{})).state);
    try Helper.apply(&source, a, after, 2, .serving_install, 21);
    // A lost reply reuses the exact same committed entry and receipt, never
    // rebuilds a candidate or evaluates a user routine.
    try Helper.apply(&source, a, after, 2, .serving_install, 21);
    source.deinit();
    live = false;
    source = ProvisionedKernelOwnerSource.init(a, root, fixture.catalog(), read_gate.alreadyReadSafeBarrier());
    live = true;
    Helper.bind(&source);
    const final = (try Helper.observe(&source, a, 2, .serving_install)).?;
    try std.testing.expectEqual(@as(u64, 21), final.applied_index);
    try Helper.requireDurableSchema(&source, a, 6, after_json);
    try Helper.reconcileServing(&source);
    var current = try source.acquireDescriptor(group, "rows", path, after);
    defer current.deinit();
    const proof = try authority.sign(a, secret, issuer, .{ .principal = "alice", .roles = &.{"PUBLIC"}, .auth_revision = 1 }, .{ .table_id = 7, .table = "rows", .database = "main", .policy_generation = 2, .catalog_epoch = 3 }, @intCast(@divFloor(platform_time.realtimeNs(), std.time.ns_per_s)));
    defer a.free(proof);
    const lookup = try table_reads.encodeStorageKernelLookupRequest(a, "a", .{ .row_policy_principal_proof = proof, .row_policy_database = "main" });
    defer a.free(lookup);
    var row = try current.owner().lookupJson("rows", lookup);
    defer row.deinit();
    try std.testing.expect(std.mem.indexOf(u8, row.bytes(), "\"id\":\"a\"") != null);
    current.deinit();
    try std.testing.expectError(error.StorageKernelOwnerStaleDescriptor, source.acquireDescriptorOnce(group, "rows", path, before, .shared, .resident, .{}));
}

test "SQL relation lifetime native retirement preparation captures only exact initialized image and catalog across cold owner reopen" {
    const local = @import("antfly_local_sources");
    const P = local.common_topology_records.RelationPersistence;
    const boots = @import("../metadata/sql_relation_boots.zig");
    const attestation = @import("../metadata/sql_relation_attestation.zig");
    const services = @import("../storage/kernel_runtime_services.zig");
    const a = std.testing.allocator;
    const schema_json = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"keyword\"}},\"additionalProperties\":false}}}}";
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/retirement-capture", .{tmp.sub_path});
    defer a.free(root_path);
    const owner_path = try std.fmt.allocPrint(a, "{s}/group-72/table-db", .{root_path});
    defer a.free(owner_path);
    var io = std.testing.io;
    const borrow = services.executor.Borrow.init(&io);
    var context: client.Context = .{};
    try std.testing.expectEqual(abi.Status.ok, services.antfly_storage_context_create_with_runtime(&.{ .io = &borrow }, &context.handle));
    defer context.deinit();
    const instance = (try context.metrics()).execution_instance;
    const native: P.Descriptor = .{ .kind = .temporary, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = instance, .data_generation = 1, .owner = .{ .issuer_node_id = 9, .connection_id = @splat(1), .wire_epoch = @splat(2), .execution_instance = instance, .connection_generation = 1, .principal_digest = @splat(3) } };
    const Fixture = struct {
        tables: [1]@import("../metadata/table_manager.zig").TableRecord,
        ranges: [1]@import("../metadata/table_manager.zig").RangeRecord = .{.{ .table_id = 7, .group_id = 72, .range_id = 272, .doc_identity_shard_id = 172, .doc_identity_range_id = 272, .start_key = "", .end_key = null }},
        observation: boots.Observation = .{ .record = null, .registration_current = false },
        boot_reads: usize = 0,
        change_boot_on_read: ?usize = null,
        fn admin(raw: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.tables, .ranges = &self.ranges, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn boot(raw: *anyopaque, _: std.mem.Allocator, query: boots.Query, _: ?u64) !boots.Observation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (query.node != 9 or query.binding_table != 7) return error.SqlRelationOwnerChanged;
            self.boot_reads += 1;
            if (self.change_boot_on_read == self.boot_reads) self.observation.record.?.generation += 1;
            return self.observation;
        }
        fn catalog(self: *@This()) table_catalog.CatalogSource {
            return .{ .ptr = self, .vtable = &.{ .admin_snapshot = admin, .free_admin_snapshot = free, .sql_relation_boot = boot } };
        }
        fn attach(self: *@This(), source: *ProvisionedKernelOwnerSource, handle: *anyopaque, process: [16]u8) !void {
            _ = self;
            _ = source.withStorageContextHandle(handle);
            try source.admitRelationHost(.{ .node_id = 9, .store_id = 11, .execution_instance = process, .generation_instance = process, .data_generation = 1 }, .{ .instance = process, .unclean_restart = false });
        }
    };
    var fixture: Fixture = .{ .tables = .{.{ .table_id = 7, .name = "rows", .schema_json = schema_json, .indexes_json = "{}", .sql_persistence = native }} };
    var source = ProvisionedKernelOwnerSource.init(a, root_path, fixture.catalog(), read_gate.alreadyReadSafeBarrier());
    var live = true;
    defer if (live) source.deinit();
    try fixture.attach(&source, context.handle.?, instance);
    const descriptor: descriptor_contract.Descriptor = .{ .lsm_root_generation = table_reads.backend_current_root_generation, .identity = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .schema_json = schema_json, .indexes_json = "{}", .sql_persistence = native, .initial_range = .{ .start = "", .end = "" } };
    var initializing = try source.acquireDescriptorWithMode(72, "rows", owner_path, descriptor, true, .resident, .{ .native_initialization = true });
    const binding = try initializing.owner().nativeRelationBinding(a, "rows");
    initializing.deinit();
    const root = try local.storage_db_root_signing_identity.loadOrCreate(a, io, root_path);
    const identity: @import("../metadata/store_root_enrollment.zig").Identity = .{ .metadata_incarnation = @splat('b'), .node_id = 9, .store_id = 11, .root_incarnation = root.root_incarnation, .public_key = root.public_key };
    fixture.observation = .{ .record = .{ .node_id = 9, .identity = identity, .reporter_incarnation = 1, .generation = 1, .instance = instance, .claim_digest = @splat(8), .unclean_restart = false, .phase = .ready }, .registration_current = true };
    const expected: ProvisionedKernelOwnerSource.NativeRetirementCaptureExpected = .{ .namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .descriptor = native, .schema_json = schema_json, .indexes_json = "{}" };
    // These component tests model genuine committed metadata observations:
    // the initialized binding is signed by the actual compiled owner root.
    // They do not manufacture a SQL Owner grant or claim manifest permission.
    const request_context: request_operation.RequestContext = .{ .setting_admin = true, .schema_validation_authority = true, .fk_generation_publication_authority = true };
    try std.testing.expectError(error.Forbidden, source.captureNativeRetirementPreparation(a, 72, "rows", expected, .{}));
    try std.testing.expectError(error.SqlRelationRecoveryIncomplete, source.captureNativeRetirementPreparation(a, 72, "rows", expected, request_context));
    fixture.observation.initialized = try attestation.SignedBinding.sign(.{ .kind = .initialized, .metadata_group_id = 1, .boot_generation = 1, .identity = identity, .reporter_incarnation = 1, .binding = binding }, root);
    // Current Boot recovery and the local host-ready gate are availability;
    // current registration/process and exact TEMP ownership remain mandatory.
    const route: metadata_api.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = identity.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 72, .range_id = 272, .identity_namespace = .{ .table_id = expected.namespace.table_id, .shard_id = expected.namespace.shard_id, .range_id = expected.namespace.range_id }, .execution = .{ .native_local = native } }, .native_participant = try P.PhysicalPin.fromBinding(binding), .admission_sql_relation_owner = native.owner };
    source.sql_persistence_ready = false;
    try std.testing.expectError(error.SqlRelationRecoveryIncomplete, source.validateNativeRoute(route));
    const admitted_host = source.sql_persistence_host;
    source.sql_persistence_host = null;
    try std.testing.expectError(error.SqlRelationOwnerUnavailable, source.validateNativeRoute(route));
    source.sql_persistence_host = admitted_host;
    var foreign_route = route;
    foreign_route.route.execution.native_local.owner_store_id += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.validateNativeRoute(foreign_route));
    foreign_route = route;
    foreign_route.route.execution.native_local.generation_instance[0] ^= 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.validateNativeRoute(foreign_route));
    foreign_route = route;
    foreign_route.admission_sql_relation_owner.?.connection_id[0] ^= 1;
    try std.testing.expectError(error.SqlTemporaryRelationNotOwned, source.validateNativeRoute(foreign_route));
    fixture.observation.record.?.phase = .recovering;
    try std.testing.expectError(error.SqlRelationRecoveryIncomplete, source.currentNativeInitialized(route, false));
    try std.testing.expectError(error.SqlRelationRecoveryIncomplete, source.nativeProofForDescriptor(descriptor, .{}));
    fixture.observation.registration_current = false;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.currentNativeInitialized(route, false));
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.nativeProofForDescriptor(descriptor, .{}));
    fixture.observation.registration_current = true;
    fixture.observation.record.?.instance[0] ^= 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.currentNativeInitialized(route, false));
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.nativeProofForDescriptor(descriptor, .{}));
    fixture.observation.record.?.instance[0] ^= 1;
    fixture.observation.record.?.phase = .ready;
    source.sql_persistence_ready = true;
    try source.validateNativeRoute(route);
    _ = try source.currentNativeInitialized(route, false);
    _ = try source.nativeProofForDescriptor(descriptor, .{});
    {
        var captured = try source.captureNativeRetirementPreparation(a, 72, "rows", expected, request_context);
        defer captured.deinit(a);
        try std.testing.expect(captured.identity.namespace.eql(expected.namespace));
        try std.testing.expect(captured.catalog_json.len != 0);
        try std.testing.expectEqual(@as(u64, 1), captured.native_proof.boot.generation);
        try std.testing.expectEqualDeep(binding, captured.native_proof.initialized);
    }
    var wrong = expected;
    wrong.namespace.shard_id += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.captureNativeRetirementPreparation(a, 72, "rows", wrong, request_context));
    wrong = expected;
    wrong.descriptor.data_generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.captureNativeRetirementPreparation(a, 72, "rows", wrong, request_context));
    wrong = expected;
    wrong.indexes_json = "";
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.captureNativeRetirementPreparation(a, 72, "rows", wrong, request_context));
    const good_initialized = fixture.observation.initialized;
    var wrong_image = binding;
    wrong_image.physical_root_incarnation ^= 1;
    fixture.observation.initialized = try attestation.SignedBinding.sign(.{ .kind = .initialized, .metadata_group_id = 1, .boot_generation = 1, .identity = identity, .reporter_incarnation = 1, .binding = wrong_image }, root);
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.captureNativeRetirementPreparation(a, 72, "rows", expected, request_context));
    fixture.observation.initialized = good_initialized;
    // The second canonical Boot read must reject a changed owner even after
    // fixed catalog reads succeeded on the retained physical handle.
    fixture.change_boot_on_read = fixture.boot_reads + 2;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.captureNativeRetirementPreparation(a, 72, "rows", expected, request_context));
    fixture.change_boot_on_read = null;
    fixture.observation.record.?.generation = 1;
    fixture.observation.registration_current = false;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.captureNativeRetirementPreparation(a, 72, "rows", expected, request_context));
    fixture.observation.registration_current = true;
    fixture.observation.record.?.generation += 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.captureNativeRetirementPreparation(a, 72, "rows", expected, request_context));
    fixture.observation.record.?.generation -= 1;
    fixture.observation.record.?.instance[0] ^= 1;
    try std.testing.expectError(error.SqlRelationOwnerChanged, source.captureNativeRetirementPreparation(a, 72, "rows", expected, request_context));
    fixture.observation.record.?.instance[0] ^= 1;
    try std.testing.expectError(error.TableNotFound, source.captureNativeRetirementPreparation(a, 73, "rows", expected, request_context));
    var expired_context = request_context;
    expired_context.deadline_ns = platform_time.monotonicNs() -| 1;
    try std.testing.expectError(error.DeadlineExceeded, source.captureNativeRetirementPreparation(a, 72, "rows", expected, expired_context));
    source.deinit();
    live = false;
    source = ProvisionedKernelOwnerSource.init(a, root_path, fixture.catalog(), read_gate.alreadyReadSafeBarrier());
    live = true;
    try fixture.attach(&source, context.handle.?, instance);
    var cold = try source.captureNativeRetirementPreparation(a, 72, "rows", expected, request_context);
    defer cold.deinit(a);
    try std.testing.expectEqualDeep(binding, cold.native_proof.initialized);
    {
        // Exercise the Source dispatch through an actual initialized CABI
        // owner after cold reopen. This component installs a real fence; the
        // mounted campaign supplies the metadata job/manifest authority.
        const topology = local.storage_db_relational_integrity_topology_contract;
        const fence: topology.Fence = .{ .transition_id = 71, .attempt = 72, .admission_epoch = cold.identity.next_epoch, .owner_group_id = 72, .peer_group_id = 72, .role = .child_generation_source, .namespace = expected.namespace, .catalog_digest = cold.identity.catalog_digest };
        var scope: ProvisionedKernelOwnerSource.NativeRetirementScope = .{
            .lease = try source.acquireControlWithControls(72, "rows", .{ .allow_deferred_catalog = true }),
            .proof = cold.native_proof,
            .plan_id = @splat(1),
            .fence = fence,
        };
        defer scope.deinit();
        try scope.apply(a, .{ .relational_topology = .{ .action = .begin, .fence = fence, .sql_drop_control = true } }, request_context);
        const ordinary = try table_reads.encodeStorageKernelLookupRequest(a, "", ProvisionedKernelOwnerSource.NativeFkRead.retirement_status.options(request_context));
        defer a.free(ordinary);
        try std.testing.expectError(error.InvalidSqlRelationPersistence, scope.lease.owner().lookupJson("rows", ordinary));
        var response = (try scope.lookup(a, .retirement_status, request_context)).?;
        defer response.deinit(a);
        var status = try std.json.parseFromSlice(topology.Status, a, response.json, .{});
        defer status.deinit();
        try std.testing.expect(status.value.fence.?.eql(fence));
        try std.testing.expect(status.value.drained);
        var duplicate = (try scope.lookup(a, .retirement_status, request_context)).?;
        defer duplicate.deinit(a);
        try std.testing.expectEqualStrings(response.json, duplicate.json);
    }
    // Even cold private capture does not turn a bare public TEMP read into
    // an owning SQL connection's row authority.
    try std.testing.expectError(error.SqlRelationOwnerChanged, ProvisionedKernelOwnerSource.lookupGroupLocal(&source, a, 72, "rows", "", .{ .relational_integrity_catalog = true }, .read_index));
}
