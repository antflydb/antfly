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

const std = @import("std");
const table_manager = @import("table_manager.zig");

pub const StoreObservation = table_manager.StoreStatusReport;
pub const PlacementStatusTag = enum {
    preferred,
    constrained,
    overloaded,
    excluded,
};

pub const PlacementStatus = struct {
    tag: PlacementStatusTag,
    priority: u8,
    retain_current: bool,
};

pub fn applyObservation(
    existing: table_manager.StoreRecord,
    observation: StoreObservation,
) table_manager.StoreRecord {
    var updated = existing;
    updated.live = observation.live;
    updated.health_class = observation.health_class;
    updated.capacity_bytes = observation.capacity_bytes;
    updated.available_bytes = observation.available_bytes;
    updated.lease_pressure = observation.lease_pressure;
    updated.read_load = observation.read_load;
    updated.write_load = observation.write_load;
    updated.active_backfills = observation.active_backfills;
    updated.backfill_progress_millis = observation.backfill_progress_millis;
    updated.group_statuses = observation.group_statuses;
    updated.runtime_statuses = observation.runtime_statuses;
    return updated;
}

pub fn applyObservations(
    records: []table_manager.StoreRecord,
    observations: []const StoreObservation,
) !usize {
    var applied: usize = 0;
    for (observations) |observation| {
        const index = findStoreIndex(records, observation.store_id) orelse return error.UnknownStore;
        records[index] = applyObservation(records[index], observation);
        applied += 1;
    }
    return applied;
}

pub fn applyObservationsOwned(
    alloc: std.mem.Allocator,
    records: []table_manager.StoreRecord,
    observations: []const StoreObservation,
) !usize {
    var applied: usize = 0;
    for (observations) |observation| {
        const index = findStoreIndex(records, observation.store_id) orelse return error.UnknownStore;
        if (!observationChangesRecord(records[index], observation)) {
            applied += 1;
            continue;
        }
        alloc.free(records[index].health_class);
        records[index].health_class = try alloc.dupe(u8, observation.health_class);
        records[index].live = observation.live;
        records[index].capacity_bytes = observation.capacity_bytes;
        records[index].available_bytes = observation.available_bytes;
        records[index].lease_pressure = observation.lease_pressure;
        records[index].read_load = observation.read_load;
        records[index].write_load = observation.write_load;
        records[index].active_backfills = observation.active_backfills;
        records[index].backfill_progress_millis = observation.backfill_progress_millis;
        const next_group_statuses = try table_manager.cloneGroupStatuses(alloc, observation.group_statuses);
        errdefer table_manager.freeGroupStatuses(alloc, next_group_statuses);
        const next_runtime_statuses = try table_manager.cloneRuntimeGroupStatusReports(alloc, observation.runtime_statuses);
        table_manager.freeGroupStatuses(alloc, records[index].group_statuses);
        table_manager.freeRuntimeGroupStatusReports(alloc, records[index].runtime_statuses);
        records[index].group_statuses = next_group_statuses;
        records[index].runtime_statuses = next_runtime_statuses;
        applied += 1;
    }
    return applied;
}

pub fn findStoreIndex(records: []const table_manager.StoreRecord, store_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.store_id == store_id) return i;
    }
    return null;
}

pub fn observationChangesRecord(
    existing: table_manager.StoreRecord,
    observation: StoreObservation,
) bool {
    return existing.live != observation.live or
        !std.mem.eql(u8, existing.health_class, observation.health_class) or
        existing.capacity_bytes != observation.capacity_bytes or
        existing.available_bytes != observation.available_bytes or
        existing.lease_pressure != observation.lease_pressure or
        existing.read_load != observation.read_load or
        existing.write_load != observation.write_load or
        existing.active_backfills != observation.active_backfills or
        existing.backfill_progress_millis != observation.backfill_progress_millis or
        !groupStatusesEqual(existing.group_statuses, observation.group_statuses) or
        !runtimeStatusesEqual(existing.runtime_statuses, observation.runtime_statuses);
}

fn groupStatusesEqual(
    lhs: []const table_manager.GroupStatusReport,
    rhs: []const table_manager.GroupStatusReport,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!groupStatusEqual(left, right)) return false;
    }
    return true;
}

fn groupStatusEqual(
    lhs: table_manager.GroupStatusReport,
    rhs: table_manager.GroupStatusReport,
) bool {
    return lhs.group_id == rhs.group_id and
        lhs.doc_count == rhs.doc_count and
        lhs.disk_bytes == rhs.disk_bytes and
        lhs.empty == rhs.empty and
        lhs.created_at_millis == rhs.created_at_millis and
        lhs.updated_at_millis == rhs.updated_at_millis and
        lhs.local_leader == rhs.local_leader and
        lhs.local_voter == rhs.local_voter and
        lhs.voter_count == rhs.voter_count and
        lhs.joint_consensus == rhs.joint_consensus and
        lhs.transition_pending == rhs.transition_pending and
        lhs.replay_required == rhs.replay_required and
        lhs.replay_caught_up == rhs.replay_caught_up and
        lhs.cutover_ready == rhs.cutover_ready and
        lhs.reads_ready_after_cutover == rhs.reads_ready_after_cutover;
}

fn runtimeStatusesEqual(
    lhs: []const table_manager.RuntimeGroupStatusReport,
    rhs: []const table_manager.RuntimeGroupStatusReport,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!runtimeStatusEqual(left, right)) return false;
    }
    return true;
}

fn runtimeStatusEqual(
    lhs: table_manager.RuntimeGroupStatusReport,
    rhs: table_manager.RuntimeGroupStatusReport,
) bool {
    if (lhs.table_id != rhs.table_id or
        !std.mem.eql(u8, lhs.table_name, rhs.table_name) or
        lhs.group_id != rhs.group_id or
        lhs.store_id != rhs.store_id or
        lhs.node_id != rhs.node_id or
        lhs.updated_at_ns != rhs.updated_at_ns or
        !std.mem.eql(u8, lhs.source, rhs.source) or
        !std.mem.eql(u8, lhs.freshness, rhs.freshness) or
        lhs.topology_generation != rhs.topology_generation or
        lhs.lsm_root_generation != rhs.lsm_root_generation or
        lhs.status_generation != rhs.status_generation or
        lhs.doc_count != rhs.doc_count or
        lhs.disk_bytes != rhs.disk_bytes or
        lhs.created_at_millis != rhs.created_at_millis or
        lhs.index_count != rhs.index_count or
        lhs.enrichment_enabled != rhs.enrichment_enabled or
        lhs.enrichment_target_sequence != rhs.enrichment_target_sequence or
        lhs.enrichment_applied_sequence != rhs.enrichment_applied_sequence or
        lhs.enrichment_retrying != rhs.enrichment_retrying or
        lhs.enrichment_worker_failed != rhs.enrichment_worker_failed or
        lhs.async_indexing_active != rhs.async_indexing_active or
        lhs.async_startup_active != rhs.async_startup_active or
        lhs.async_dense_catch_up_active != rhs.async_dense_catch_up_active or
        lhs.async_bulk_coalescing_active != rhs.async_bulk_coalescing_active or
        lhs.indexes.len != rhs.indexes.len)
    {
        return false;
    }
    for (lhs.indexes, rhs.indexes) |left, right| {
        if (!std.mem.eql(u8, left.name, right.name) or
            !std.mem.eql(u8, left.kind, right.kind) or
            left.doc_count != right.doc_count or
            left.term_count != right.term_count or
            left.edge_count != right.edge_count or
            left.node_count != right.node_count or
            left.root_node != right.root_node or
            left.backfill_active != right.backfill_active or
            left.backfill_progress_millis != right.backfill_progress_millis or
            left.replay_applied_sequence != right.replay_applied_sequence or
            left.replay_target_sequence != right.replay_target_sequence or
            left.replay_catch_up_required != right.replay_catch_up_required)
        {
            return false;
        }
    }
    return true;
}

pub fn classifyStore(record: table_manager.StoreRecord) PlacementStatus {
    if (record.drain_requested) return .{ .tag = .excluded, .priority = 255, .retain_current = false };
    if (!record.live) return .{ .tag = .excluded, .priority = 255, .retain_current = false };
    if (std.mem.eql(u8, record.health_class, "draining")) return .{ .tag = .excluded, .priority = 255, .retain_current = false };
    if (record.available_bytes == 0 and record.capacity_bytes > 0) return .{ .tag = .excluded, .priority = 255, .retain_current = false };
    if (std.mem.eql(u8, record.health_class, "degraded")) {
        return .{ .tag = .constrained, .priority = 1, .retain_current = true };
    }

    const pressure = combinedPressure(record.lease_pressure, record.read_load, record.write_load);
    if (pressure >= overloadPressureThreshold()) {
        return .{ .tag = .overloaded, .priority = 2, .retain_current = false };
    }
    if (pressure >= constrainedPressureThreshold()) {
        return .{ .tag = .constrained, .priority = 1, .retain_current = true };
    }
    return .{ .tag = .preferred, .priority = 0, .retain_current = true };
}

pub fn combinedPressure(lease_pressure: u32, read_load: u32, write_load: u32) u64 {
    return @as(u64, lease_pressure) * 4 + @as(u64, read_load) + @as(u64, write_load) * 2;
}

pub fn constrainedPressureThreshold() u64 {
    return 280;
}

pub fn overloadPressureThreshold() u64 {
    return 600;
}

test "store observer applies a single observation without losing placement attributes" {
    const existing: table_manager.StoreRecord = .{
        .store_id = 11,
        .node_id = 1,
        .role = "data",
        .health_class = "healthy",
        .failure_domain = "rack-a",
        .live = true,
        .drain_requested = true,
        .capacity_bytes = 1024,
        .available_bytes = 800,
        .lease_pressure = 15,
        .read_load = 20,
        .write_load = 10,
        .active_backfills = 0,
        .backfill_progress_millis = 1000,
    };

    const updated = applyObservation(existing, .{
        .store_id = 11,
        .live = false,
        .health_class = "degraded",
        .capacity_bytes = 2048,
        .available_bytes = 0,
        .lease_pressure = 90,
        .read_load = 200,
        .write_load = 120,
        .active_backfills = 2,
        .backfill_progress_millis = 375,
    });

    try std.testing.expectEqual(@as(u64, 11), updated.store_id);
    try std.testing.expectEqual(@as(u64, 1), updated.node_id);
    try std.testing.expect(std.mem.eql(u8, updated.role, "data"));
    try std.testing.expect(std.mem.eql(u8, updated.failure_domain, "rack-a"));
    try std.testing.expectEqual(false, updated.live);
    try std.testing.expect(updated.drain_requested);
    try std.testing.expect(std.mem.eql(u8, updated.health_class, "degraded"));
    try std.testing.expectEqual(@as(u64, 2048), updated.capacity_bytes);
    try std.testing.expectEqual(@as(u64, 0), updated.available_bytes);
    try std.testing.expectEqual(@as(u32, 90), updated.lease_pressure);
    try std.testing.expectEqual(@as(u32, 200), updated.read_load);
    try std.testing.expectEqual(@as(u32, 120), updated.write_load);
    try std.testing.expectEqual(@as(u32, 2), updated.active_backfills);
    try std.testing.expectEqual(@as(u16, 375), updated.backfill_progress_millis);
}

test "store observer keeps drain intent across healthy observations" {
    var records = [_]table_manager.StoreRecord{.{
        .store_id = 31,
        .node_id = 3,
        .role = "data",
        .health_class = "draining",
        .live = true,
        .drain_requested = true,
        .capacity_bytes = 1024,
        .available_bytes = 100,
    }};

    try std.testing.expectEqual(@as(usize, 1), try applyObservations(&records, &.{.{
        .store_id = 31,
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 900,
    }}));

    try std.testing.expect(records[0].drain_requested);
    try std.testing.expectEqual(PlacementStatusTag.excluded, classifyStore(records[0]).tag);
}

test "store observer applies multiple observations in place" {
    var records = [_]table_manager.StoreRecord{
        .{
            .store_id = 21,
            .node_id = 1,
            .role = "data",
            .failure_domain = "rack-a",
            .live = true,
            .capacity_bytes = 1024,
            .available_bytes = 900,
            .lease_pressure = 10,
            .read_load = 10,
            .write_load = 5,
            .active_backfills = 0,
            .backfill_progress_millis = 1000,
        },
        .{
            .store_id = 22,
            .node_id = 2,
            .role = "data",
            .failure_domain = "rack-b",
            .live = true,
            .capacity_bytes = 1024,
            .available_bytes = 850,
            .lease_pressure = 15,
            .read_load = 20,
            .write_load = 10,
            .active_backfills = 0,
            .backfill_progress_millis = 1000,
        },
    };

    try std.testing.expectEqual(@as(usize, 2), try applyObservations(&records, &.{
        .{ .store_id = 21, .live = false, .health_class = "degraded", .capacity_bytes = 1024, .available_bytes = 0, .lease_pressure = 95, .read_load = 140, .write_load = 110, .active_backfills = 1, .backfill_progress_millis = 200 },
        .{ .store_id = 22, .live = true, .health_class = "healthy", .capacity_bytes = 2048, .available_bytes = 1200, .lease_pressure = 5, .read_load = 15, .write_load = 8, .active_backfills = 0, .backfill_progress_millis = 1000 },
    }));

    try std.testing.expectEqual(false, records[0].live);
    try std.testing.expect(std.mem.eql(u8, records[0].health_class, "degraded"));
    try std.testing.expect(std.mem.eql(u8, records[0].failure_domain, "rack-a"));
    try std.testing.expectEqual(@as(u64, 0), records[0].available_bytes);
    try std.testing.expectEqual(@as(u32, 95), records[0].lease_pressure);
    try std.testing.expectEqual(@as(u32, 140), records[0].read_load);
    try std.testing.expectEqual(@as(u32, 110), records[0].write_load);
    try std.testing.expectEqual(@as(u32, 1), records[0].active_backfills);
    try std.testing.expectEqual(@as(u16, 200), records[0].backfill_progress_millis);

    try std.testing.expectEqual(true, records[1].live);
    try std.testing.expect(std.mem.eql(u8, records[1].health_class, "healthy"));
    try std.testing.expect(std.mem.eql(u8, records[1].failure_domain, "rack-b"));
    try std.testing.expectEqual(@as(u64, 2048), records[1].capacity_bytes);
    try std.testing.expectEqual(@as(u64, 1200), records[1].available_bytes);
    try std.testing.expectEqual(@as(u32, 5), records[1].lease_pressure);
    try std.testing.expectEqual(@as(u32, 15), records[1].read_load);
    try std.testing.expectEqual(@as(u32, 8), records[1].write_load);
    try std.testing.expectEqual(@as(u32, 0), records[1].active_backfills);
    try std.testing.expectEqual(@as(u16, 1000), records[1].backfill_progress_millis);
}

test "store observer classifies placement status from health and pressure" {
    const preferred = classifyStore(.{
        .store_id = 1,
        .node_id = 1,
        .role = "data",
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 900,
        .lease_pressure = 10,
        .read_load = 15,
        .write_load = 8,
    });
    try std.testing.expectEqual(PlacementStatusTag.preferred, preferred.tag);
    try std.testing.expectEqual(@as(u8, 0), preferred.priority);
    try std.testing.expect(preferred.retain_current);

    const constrained = classifyStore(.{
        .store_id = 2,
        .node_id = 2,
        .role = "data",
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 800,
        .lease_pressure = 60,
        .read_load = 20,
        .write_load = 10,
    });
    try std.testing.expectEqual(PlacementStatusTag.constrained, constrained.tag);
    try std.testing.expect(constrained.retain_current);

    const overloaded = classifyStore(.{
        .store_id = 3,
        .node_id = 3,
        .role = "data",
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 850,
        .lease_pressure = 95,
        .read_load = 200,
        .write_load = 140,
    });
    try std.testing.expectEqual(PlacementStatusTag.overloaded, overloaded.tag);
    try std.testing.expect(!overloaded.retain_current);

    const excluded = classifyStore(.{
        .store_id = 4,
        .node_id = 4,
        .role = "data",
        .live = false,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 900,
    });
    try std.testing.expectEqual(PlacementStatusTag.excluded, excluded.tag);
    try std.testing.expect(!excluded.retain_current);
}
