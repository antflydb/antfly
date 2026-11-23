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

//! Authoritative catalog capture for frozen completion owner admission.
//! The caller holds its table activity lease through proposal and supplies a
//! descriptor from the actual owner source. This establishes no leader lease.
const std = @import("std");
const catalog_api = @import("table_catalog.zig");
const metadata = @import("../metadata/api.zig");
const tables = @import("../metadata/table_manager.zig");
const descriptor_contract = @import("../storage/kernel_owner_descriptor.zig");
const Context = @import("antfly_local_sources").api_operation.RequestContext;
pub const completion = @import("antfly_local_sources").api_local_table_writes.table_write_source.history_resolution_wire.completion;

pub fn capture(a: std.mem.Allocator, catalog: catalog_api.CatalogSource, table_name: []const u8, fence: metadata.CatalogRouteFence, descriptor: descriptor_contract.Descriptor, context: Context) !completion.RoutedOwner {
    try context.ensureActive();
    try fence.validate();
    if (table_name.len == 0 or table_name.len > 1024) return error.InvalidCatalogRouteFence;
    if (descriptor.restore != null or descriptor.restore_bootstrap_json.len != 0 or descriptor.initial_child_bootstrap_json.len != 0) return error.ChangefeedCapabilityUnsupported;
    var budget = catalog.budget(null);
    budget.deadline_ns = @min(budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io }) orelse std.math.maxInt(u64), budget.nowNs() +| 5 * std.time.ns_per_s);
    if (catalog.routeFenceDeadline(fence)) |deadline| budget.deadline_ns = @min(budget.deadline_ns.?, deadline);
    const Token = @import("antfly_cancellation").CancellationToken;
    const Pair = struct {
        first: Token,
        second: Token,
        fn check(ptr: *const anyopaque) !void {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            try self.first.check();
            try self.second.check();
        }
        fn cancelled(ptr: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            return self.first.isCancelled() or self.second.isCancelled();
        }
    };
    const cancellation: Pair = .{ .first = context.cancellation, .second = fence.admission_cancellation };
    budget.cancellation = .{ .ptr = &cancellation, .check_fn = Pair.check, .is_cancelled_fn = Pair.cancelled };
    var session = try catalog_api.RoutingSession.initWithBudget(a, catalog, budget);
    defer session.deinit();
    try budget.checkpoint();
    const snapshot = session.snapshot.value;
    if (snapshot.catalog_revision < fence.catalog_revision) return error.CatalogRoutingUnavailable;
    if (snapshot.metadata_group_id != fence.metadata_group_id or !std.meta.eql(snapshot.metadata_incarnation, fence.metadata_incarnation)) return error.TopologyChanged;
    const incarnation = snapshot.metadata_incarnation orelse return error.ChangefeedCapabilityUnsupported;
    const view = try session.tableRoutingView(table_name, budget);
    if (view.table_id != fence.table_id or view.topology_epoch != fence.topology_epoch) return error.TopologyChanged;
    const range = try session.rangeForGroup(view.table_id, fence.route.group_id, budget);
    const identity: metadata.CatalogIdentityNamespace = .{ .table_id = view.table_id, .shard_id = tables.rangeDocIdentityShardId(range.*), .range_id = tables.rangeDocIdentityRangeId(range.*) };
    if (range.group_id != fence.route.group_id or identity.range_id != fence.route.range_id or !std.meta.eql(identity, fence.route.identity_namespace)) return error.TopologyChanged;
    if (descriptor.identity.table_id != identity.table_id or descriptor.identity.shard_id != identity.shard_id or descriptor.identity.range_id != identity.range_id) return error.HistoryResynchronizationRequired;
    const table = snapshot.tables[session.table_id_indexes.get(view.table_id) orelse return error.TopologyChanged];
    if (table.storage.engine != .local) return error.ChangefeedCapabilityUnsupported;
    if (table.routing_storage_binding == null) return error.ChangefeedCapabilityUnsupported;
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], identity.table_id, .big);
    std.mem.writeInt(u64, namespace[8..16], identity.shard_id, .big);
    std.mem.writeInt(u64, namespace[16..24], identity.range_id, .big);
    const result: completion.RoutedOwner = .{ .metadata_group_id = snapshot.metadata_group_id, .metadata_incarnation = incarnation, .namespace = namespace, .table_binding = @import("../metadata/changefeed_control.zig").tableBinding(table), .root_generation = descriptor.lsm_root_generation, .group_id = range.group_id, .topology_epoch = view.topology_epoch };
    try result.validate();
    try budget.checkpoint();
    try context.ensureActive();
    return result;
}

pub fn require(a: std.mem.Allocator, catalog: catalog_api.CatalogSource, table_name: []const u8, fence: metadata.CatalogRouteFence, descriptor: descriptor_contract.Descriptor, expected: completion.RoutedOwner, context: Context) !void {
    try expected.validate();
    const current = try capture(a, catalog, table_name, fence, descriptor, context);
    if (!std.mem.eql(u8, &current.digest(), &expected.digest())) return error.TopologyChanged;
}

test "completion live owner capture uses authoritative catalog and rejects replacement descriptors" {
    const a = std.testing.allocator;
    var records = [_]tables.TableRecord{.{ .table_id = 7, .name = "docs", .schema_json = "{}" }};
    records[0].routing_storage_binding = @import("antfly_local_sources").common_topology_records.tableStorageBinding(records[0]);
    const ranges = [_]tables.RangeRecord{.{ .table_id = 7, .group_id = 9, .range_id = 11, .start_key = "", .end_key = null, .doc_identity_shard_id = 9, .doc_identity_range_id = 11 }};
    const generation = try catalog_api.RoutingGeneration.create(a, .{ .metadata_group_id = 1, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .catalog_revision = 10, .tables = @constCast(&records), .ranges = @constCast(&ranges) }, .{});
    defer generation.release();
    const Fixture = struct {
        generation: *catalog_api.RoutingGeneration,
        captures: usize = 0,
        cancel_after: ?*std.atomic.Value(bool) = null,
        fn acquire(ptr: *anyopaque, budget: catalog_api.RoutingBudget, authoritative: bool) !*catalog_api.RoutingGeneration {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try budget.checkpoint();
            try std.testing.expect(authoritative);
            self.captures += 1;
            if (self.cancel_after) |cancel| cancel.store(true, .release);
            self.generation.retain();
            return self.generation;
        }
        fn admin(_: *anyopaque) !metadata.AdminSnapshot {
            return error.TestUnexpectedAdminFallback;
        }
    };
    var fixture: Fixture = .{ .generation = generation };
    const catalog: catalog_api.CatalogSource = .{ .ptr = &fixture, .vtable = &.{ .admin_snapshot = Fixture.admin, .free_admin_snapshot = undefined, .acquire_routing_generation = Fixture.acquire } };
    const view = try generation.indexed.tableRoutingView("docs", .{});
    var fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = generation.indexed.snapshot.value.metadata_incarnation, .catalog_revision = 9, .table_id = 7, .topology_epoch = view.topology_epoch, .route = .{ .group_id = 9, .range_id = 11, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 11 } } };
    var descriptor: descriptor_contract.Descriptor = .{ .lsm_root_generation = 2, .identity = .{ .table_id = 7, .shard_id = 9, .range_id = 11 } };
    const owner = try capture(a, catalog, "docs", fence, descriptor, .{});
    try std.testing.expectEqual(@as(u64, 2), owner.root_generation);
    try require(a, catalog, "docs", fence, descriptor, owner, .{});
    descriptor.lsm_root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, require(a, catalog, "docs", fence, descriptor, owner, .{}));
    descriptor.lsm_root_generation -= 1;
    descriptor.identity.range_id += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, capture(a, catalog, "docs", fence, descriptor, .{}));
    descriptor.identity.range_id -= 1;
    fence.catalog_revision = 11;
    try std.testing.expectError(error.CatalogRoutingUnavailable, capture(a, catalog, "docs", fence, descriptor, .{}));
    fence.catalog_revision = 9;
    fence.topology_epoch += 1;
    try std.testing.expectError(error.TopologyChanged, capture(a, catalog, "docs", fence, descriptor, .{}));
    var cancelled = std.atomic.Value(bool).init(true);
    const count = fixture.captures;
    try std.testing.expectError(error.Canceled, capture(a, catalog, "docs", fence, descriptor, .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) }));
    try std.testing.expectEqual(count, fixture.captures);
    cancelled.store(false, .release);
    fence.topology_epoch -= 1;
    fence.admission_cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled);
    fixture.cancel_after = &cancelled;
    try std.testing.expectError(error.Canceled, capture(a, catalog, "docs", fence, descriptor, .{}));
    try std.testing.expectEqual(count + 1, fixture.captures);
    fixture.cancel_after = null;
    cancelled.store(false, .release);
    fence.admission_deadline_ns = 0;
    const before_deadline = fixture.captures;
    try std.testing.expectError(error.CatalogRoutingSnapshotTimeout, capture(a, catalog, "docs", fence, descriptor, .{}));
    try std.testing.expectEqual(before_deadline, fixture.captures);
    fence.admission_deadline_ns = null;
    var replacement_records = records;
    replacement_records[0].object_storage_generation += 1;
    replacement_records[0].routing_storage_binding = null;
    replacement_records[0].routing_storage_binding = @import("antfly_local_sources").common_topology_records.tableStorageBinding(replacement_records[0]);
    const replacement = try catalog_api.RoutingGeneration.create(a, .{ .metadata_group_id = 1, .metadata_incarnation = fence.metadata_incarnation, .catalog_revision = 11, .tables = &replacement_records, .ranges = @constCast(&ranges) }, .{});
    defer replacement.release();
    fixture.generation = replacement;
    fence.topology_epoch = (try replacement.indexed.tableRoutingView("docs", .{})).topology_epoch;
    try std.testing.expectError(error.TopologyChanged, require(a, catalog, "docs", fence, descriptor, owner, .{}));
    const missing_incarnation = try catalog_api.RoutingGeneration.create(a, .{ .metadata_group_id = 1, .catalog_revision = 12, .tables = @constCast(&records), .ranges = @constCast(&ranges) }, .{});
    defer missing_incarnation.release();
    fixture.generation = missing_incarnation;
    fence.metadata_incarnation = null;
    fence.topology_epoch = (try missing_incarnation.indexed.tableRoutingView("docs", .{})).topology_epoch;
    try std.testing.expectError(error.ChangefeedCapabilityUnsupported, capture(a, catalog, "docs", fence, descriptor, .{}));
}
