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

const status_heartbeat_refresh_ms: u64 = 30 * std.time.ms_per_s;
const runtime_status_heartbeat_refresh_ns: u64 = 30 * std.time.ns_per_s;

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
    if (existing.reporter_incarnation != 0 and
        observation.reporter_incarnation == existing.reporter_incarnation)
    {
        updated.status_generation = observation.status_generation;
        updated.artifact_sources_protocol_version = observation.artifact_sources_protocol_version;
    }
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
    return try applyObservationsOwnedWithRepairStatus(alloc, records, observations, true);
}

pub fn applyObservationsOwnedWithRepairStatus(
    alloc: std.mem.Allocator,
    records: []table_manager.StoreRecord,
    observations: []const StoreObservation,
    include_repair_status: bool,
) !usize {
    var applied: usize = 0;
    for (observations) |observation| {
        const index = findStoreIndex(records, observation.store_id) orelse return error.UnknownStore;
        if (!observationChangesRecordWithRepairStatus(records[index], observation, include_repair_status)) {
            applied += 1;
            continue;
        }
        alloc.free(records[index].health_class);
        records[index].health_class = try alloc.dupe(u8, observation.health_class);
        if (records[index].reporter_incarnation != 0 and
            observation.reporter_incarnation == records[index].reporter_incarnation)
        {
            records[index].status_generation = observation.status_generation;
            records[index].artifact_sources_protocol_version = observation.artifact_sources_protocol_version;
        }
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
        if (!include_repair_status) {
            preserveCommittedRuntimeRepairStatus(records[index].runtime_statuses, next_runtime_statuses);
        }
        table_manager.freeGroupStatuses(alloc, records[index].group_statuses);
        table_manager.freeRuntimeGroupStatusReports(alloc, records[index].runtime_statuses);
        records[index].group_statuses = next_group_statuses;
        records[index].runtime_statuses = next_runtime_statuses;
        applied += 1;
    }
    return applied;
}

/// A capability probe that is pending or temporarily unavailable must not turn
/// an ordinary heartbeat into deletion of repair facts that were already
/// committed with the newer codec. New repair facts remain suppressed until
/// activation, while facts for a different index incarnation fail closed.
fn preserveCommittedRuntimeRepairStatus(
    existing: []const table_manager.RuntimeGroupStatusReport,
    next: []table_manager.RuntimeGroupStatusReport,
) void {
    for (next) |*next_runtime| {
        const prior_runtime = findRuntimeRepairIdentity(existing, next_runtime.*);
        for (next_runtime.indexes) |*next_index| {
            next_index.repair_status = null;
            next_index.repair_active_generation_serviceable = false;
            const prior = prior_runtime orelse continue;
            const prior_index = findRuntimeIndexRepairIdentity(prior.indexes, next_index.*) orelse continue;
            next_index.repair_status = prior_index.repair_status;
            next_index.repair_active_generation_serviceable =
                prior_index.repair_status != null and prior_index.repair_active_generation_serviceable;
        }
    }
}

fn findRuntimeRepairIdentity(
    statuses: []const table_manager.RuntimeGroupStatusReport,
    target: table_manager.RuntimeGroupStatusReport,
) ?table_manager.RuntimeGroupStatusReport {
    for (statuses) |status| {
        if (status.table_id == target.table_id and
            status.group_id == target.group_id and
            status.store_id == target.store_id and
            status.node_id == target.node_id)
        {
            return status;
        }
    }
    return null;
}

fn findRuntimeIndexRepairIdentity(
    indexes: []const table_manager.RuntimeIndexStatusReport,
    target: table_manager.RuntimeIndexStatusReport,
) ?table_manager.RuntimeIndexStatusReport {
    for (indexes) |index| {
        if (std.mem.eql(u8, index.name, target.name) and
            std.mem.eql(u8, index.kind, target.kind) and
            index.coverage_generation == target.coverage_generation and
            index.coverage_config_hash == target.coverage_config_hash)
        {
            return index;
        }
    }
    return null;
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
    return observationChangesRecordWithRepairStatus(existing, observation, true);
}

pub fn observationChangesRecordWithRepairStatus(
    existing: table_manager.StoreRecord,
    observation: StoreObservation,
    include_repair_status: bool,
) bool {
    const repair_facts_equal = !include_repair_status or
        runtimeRepairFactsEqual(existing.runtime_statuses, observation.runtime_statuses);
    // Once registration establishes an incarnation, reports from a prior
    // process can never mutate the store projection. Generations order full
    // snapshots within the active process; equal generations remain useful
    // for cheap heartbeats that overlay live Raft facts.
    if (existing.reporter_incarnation != 0) {
        if (observation.reporter_incarnation != existing.reporter_incarnation) return false;
        if (observation.status_generation < existing.status_generation) return false;
        if (include_repair_status and
            observation.status_generation == existing.status_generation and
            !repair_facts_equal) return false;
    } else if (!repair_facts_equal and
        !legacyRepairTransitionCausallySupersedes(existing.runtime_statuses, observation.runtime_statuses))
    {
        return false;
    }

    // Until the v13 codec is activated, absence or an incomplete replacement
    // identity is not authoritative for an already-committed repair identity.
    // A legacy or transient heartbeat can omit an index (or its whole runtime
    // group), and replacing the owned snapshot in that case would erase the
    // only admission-safety fact. Treat the heartbeat as unchanged so callers
    // neither replace the projection nor propose the deletion. A same-name
    // index with a complete new materialization identity remains authoritative.
    if (observationLacksAuthoritativeCommittedRepairIdentity(existing, observation)) return false;

    return existing.live != observation.live or
        !std.mem.eql(u8, existing.health_class, observation.health_class) or
        (existing.reporter_incarnation != 0 and
            existing.status_generation != observation.status_generation) or
        existing.artifact_sources_protocol_version != observation.artifact_sources_protocol_version or
        existing.capacity_bytes != observation.capacity_bytes or
        existing.available_bytes != observation.available_bytes or
        existing.lease_pressure != observation.lease_pressure or
        existing.read_load != observation.read_load or
        existing.write_load != observation.write_load or
        existing.active_backfills != observation.active_backfills or
        existing.backfill_progress_millis != observation.backfill_progress_millis or
        !groupStatusesEqual(existing.group_statuses, observation.group_statuses) or
        !runtimeStatusesEqual(existing.runtime_statuses, observation.runtime_statuses, include_repair_status);
}

fn observationLacksAuthoritativeCommittedRepairIdentity(
    existing: table_manager.StoreRecord,
    observation: StoreObservation,
) bool {
    for (existing.runtime_statuses) |prior_runtime| {
        var has_committed_repair = false;
        for (prior_runtime.indexes) |prior_index| {
            if (prior_index.repair_status != null) {
                has_committed_repair = true;
                break;
            }
        }
        // Activation may be unknown because another store has a repair fact;
        // keep the common repair-free store path linear in its own index count.
        if (!has_committed_repair) continue;

        const next_runtime = findRuntimeRepairIdentity(observation.runtime_statuses, prior_runtime) orelse {
            if (!storeObservationCausallySupersedes(existing, observation, null, null)) return true;
            continue;
        };
        for (prior_runtime.indexes) |prior_index| {
            if (prior_index.repair_status == null) continue;
            var replacement: ?table_manager.RuntimeIndexStatusReport = null;
            for (next_runtime.indexes) |next_index| {
                if (std.mem.eql(u8, prior_index.name, next_index.name)) {
                    replacement = next_index;
                    break;
                }
            }
            const next_index = replacement orelse {
                if (!storeObservationCausallySupersedes(existing, observation, prior_runtime, next_runtime)) return true;
                continue;
            };
            // A kind change is only an explicit catalog replacement once the
            // producer supplies a complete identity. Legacy reports may omit
            // kind entirely, and startup/degraded snapshots deliberately
            // publish unknown identities; neither may gain deletion authority
            // from values that happen not to match the committed incarnation.
            const same_materialization = std.mem.eql(u8, prior_index.kind, next_index.kind) and
                prior_index.coverage_generation == next_index.coverage_generation and
                prior_index.coverage_config_hash == next_index.coverage_config_hash;
            if (!same_materialization and
                (!next_index.coverage_identity_ready or
                    !storeObservationCausallySupersedes(existing, observation, prior_runtime, next_runtime))) return true;
        }
    }
    return false;
}

fn storeObservationCausallySupersedes(
    existing: table_manager.StoreRecord,
    observation: StoreObservation,
    prior_runtime: ?table_manager.RuntimeGroupStatusReport,
    next_runtime: ?table_manager.RuntimeGroupStatusReport,
) bool {
    if (existing.reporter_incarnation != 0) {
        return observation.reporter_incarnation == existing.reporter_incarnation and
            observation.status_generation > existing.status_generation;
    }
    // Legacy rolling-upgrade reporters have no process authority. Retain the
    // conservative realtime fallback only when both corresponding runtime
    // observations exist; absence can never prove a deletion.
    const prior = prior_runtime orelse return false;
    const next = next_runtime orelse return false;
    return prior.updated_at_ns != 0 and next.updated_at_ns > prior.updated_at_ns;
}

fn runtimeRepairFactsEqual(
    lhs: []const table_manager.RuntimeGroupStatusReport,
    rhs: []const table_manager.RuntimeGroupStatusReport,
) bool {
    return runtimeRepairFactsContained(lhs, rhs) and runtimeRepairFactsContained(rhs, lhs);
}

fn runtimeRepairFactsContained(
    expected: []const table_manager.RuntimeGroupStatusReport,
    actual: []const table_manager.RuntimeGroupStatusReport,
) bool {
    for (expected) |expected_runtime| {
        for (expected_runtime.indexes) |expected_index| {
            const expected_status = expected_index.repair_status orelse continue;
            const actual_runtime = findRuntimeRepairIdentity(actual, expected_runtime) orelse return false;
            const actual_index = findRuntimeIndexRepairIdentity(actual_runtime.indexes, expected_index) orelse return false;
            if (actual_index.repair_status != expected_status or
                actual_index.repair_active_generation_serviceable !=
                    expected_index.repair_active_generation_serviceable) return false;
        }
    }
    return true;
}

fn runtimeRepairFactsForGroupContained(
    expected: table_manager.RuntimeGroupStatusReport,
    actual: table_manager.RuntimeGroupStatusReport,
) bool {
    for (expected.indexes) |expected_index| {
        const expected_status = expected_index.repair_status orelse continue;
        const actual_index = findRuntimeIndexRepairIdentity(actual.indexes, expected_index) orelse return false;
        if (actual_index.repair_status != expected_status or
            actual_index.repair_active_generation_serviceable !=
                expected_index.repair_active_generation_serviceable) return false;
    }
    return true;
}

fn legacyRepairTransitionCausallySupersedes(
    existing: []const table_manager.RuntimeGroupStatusReport,
    observation: []const table_manager.RuntimeGroupStatusReport,
) bool {
    for (existing) |prior_runtime| {
        const next_runtime = findRuntimeRepairIdentity(observation, prior_runtime) orelse {
            for (prior_runtime.indexes) |index| if (index.repair_status != null) return false;
            continue;
        };
        // Publishing an additional repair fact cannot erase or rewrite any
        // committed admission-safety fact, so legacy reporters do not need a
        // timestamp proof for that monotonic transition. Removal or mutation
        // still requires a causally newer complete observation below.
        if (runtimeRepairFactsForGroupContained(prior_runtime, next_runtime)) continue;
        if (prior_runtime.updated_at_ns == 0 or
            next_runtime.updated_at_ns <= prior_runtime.updated_at_ns) return false;
    }
    // A genuinely new runtime group has no committed predecessor to protect;
    // monotonic publications on matching groups were accepted above.
    return true;
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
        lhs.relocation_generation == rhs.relocation_generation and
        lhs.raft_applied_index == rhs.raft_applied_index and
        lhs.raft_term == rhs.raft_term and
        lhs.raft_membership_index == rhs.raft_membership_index and
        lhs.doc_count == rhs.doc_count and
        lhs.disk_bytes == rhs.disk_bytes and
        lhs.disk_bytes_known == rhs.disk_bytes_known and
        lhs.empty == rhs.empty and
        lhs.created_at_millis == rhs.created_at_millis and
        timestampMillisCoalesced(lhs.updated_at_millis, rhs.updated_at_millis) and
        lhs.observed_reallocation_request_id == rhs.observed_reallocation_request_id and
        lhs.local_leader == rhs.local_leader and
        lhs.local_voter == rhs.local_voter and
        lhs.voter_count == rhs.voter_count and
        lhs.voter_set_known == rhs.voter_set_known and
        std.mem.eql(u8, &lhs.voter_set_fingerprint, &rhs.voter_set_fingerprint) and
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
    include_repair_status: bool,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!runtimeStatusEqual(left, right, include_repair_status)) return false;
    }
    return true;
}

fn runtimeStatusEqual(
    lhs: table_manager.RuntimeGroupStatusReport,
    rhs: table_manager.RuntimeGroupStatusReport,
    include_repair_status: bool,
) bool {
    if (lhs.table_id != rhs.table_id or
        !std.mem.eql(u8, lhs.table_name, rhs.table_name) or
        lhs.group_id != rhs.group_id or
        lhs.store_id != rhs.store_id or
        lhs.node_id != rhs.node_id or
        !timestampNanosCoalesced(lhs.updated_at_ns, rhs.updated_at_ns) or
        !std.mem.eql(u8, lhs.source, rhs.source) or
        !std.mem.eql(u8, lhs.freshness, rhs.freshness) or
        lhs.topology_generation != rhs.topology_generation or
        lhs.lsm_root_generation != rhs.lsm_root_generation or
        lhs.status_generation != rhs.status_generation or
        lhs.doc_count != rhs.doc_count or
        lhs.disk_bytes != rhs.disk_bytes or
        lhs.disk_bytes_known != rhs.disk_bytes_known or
        lhs.created_at_millis != rhs.created_at_millis or
        lhs.index_count != rhs.index_count or
        !runtimeEnrichmentStatusEqual(lhs.enrichment, rhs.enrichment) or
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
            !optionalStringsEqual(left.load_error, right.load_error) or
            left.doc_count != right.doc_count or
            left.term_count != right.term_count or
            left.edge_count != right.edge_count or
            left.node_count != right.node_count or
            left.root_node != right.root_node or
            left.coverage_produced_count != right.coverage_produced_count or
            left.coverage_skipped_count != right.coverage_skipped_count or
            left.coverage_terminal_failed_count != right.coverage_terminal_failed_count or
            left.coverage_generation != right.coverage_generation or
            left.coverage_config_hash != right.coverage_config_hash or
            left.coverage_identity_ready != right.coverage_identity_ready or
            left.coverage_summary_ready != right.coverage_summary_ready or
            left.backfill_active != right.backfill_active or
            left.backfill_progress_millis != right.backfill_progress_millis or
            left.replay_applied_sequence != right.replay_applied_sequence or
            left.replay_target_sequence != right.replay_target_sequence or
            left.replay_catch_up_required != right.replay_catch_up_required or
            !runtimeIndexSourceReplayEqual(left.source_replay, right.source_replay) or
            (include_repair_status and (left.repair_status != right.repair_status or
                left.repair_active_generation_serviceable != right.repair_active_generation_serviceable)))
        {
            return false;
        }
    }
    return true;
}

fn runtimeIndexSourceReplayEqual(
    lhs: []const table_manager.RuntimeIndexSourceReplayStatusReport,
    rhs: []const table_manager.RuntimeIndexSourceReplayStatusReport,
) bool {
    if (lhs.len != rhs.len) return false;
    for (lhs, rhs) |left, right| {
        if (!std.mem.eql(u8, left.artifact_name, right.artifact_name) or
            left.published_sequence != right.published_sequence or
            left.target_sequence != right.target_sequence) return false;
    }
    return true;
}

fn optionalStringsEqual(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null or rhs == null) return lhs == null and rhs == null;
    return std.mem.eql(u8, lhs.?, rhs.?);
}

fn runtimeEnrichmentStatusEqual(
    lhs: table_manager.RuntimeEnrichmentStatusReport,
    rhs: table_manager.RuntimeEnrichmentStatusReport,
) bool {
    inline for (std.meta.fields(table_manager.RuntimeEnrichmentStatusReport)) |field| {
        if (comptime std.mem.eql(u8, field.name, "projection_checkpoint_status")) {
            if (!std.mem.eql(u8, @field(lhs, field.name), @field(rhs, field.name))) return false;
        } else if (@field(lhs, field.name) != @field(rhs, field.name)) {
            return false;
        }
    }
    return true;
}

fn timestampMillisCoalesced(lhs: u64, rhs: u64) bool {
    const delta = if (lhs > rhs) lhs - rhs else rhs - lhs;
    return delta < status_heartbeat_refresh_ms;
}

fn timestampNanosCoalesced(lhs: u64, rhs: u64) bool {
    const delta = if (lhs > rhs) lhs - rhs else rhs - lhs;
    return delta < runtime_status_heartbeat_refresh_ns;
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

test "store observer coalesces status heartbeat timestamps" {
    var existing_groups = [_]table_manager.GroupStatusReport{.{
        .group_id = 101,
        .doc_count = 7,
        .disk_bytes = 4096,
        .empty = false,
        .created_at_millis = 100,
        .updated_at_millis = 1_000,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 3,
    }};
    var fresh_groups = [_]table_manager.GroupStatusReport{.{
        .group_id = 101,
        .doc_count = 7,
        .disk_bytes = 4096,
        .empty = false,
        .created_at_millis = 100,
        .updated_at_millis = 1_000 + 5 * std.time.ms_per_s,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 3,
    }};
    var stale_groups = [_]table_manager.GroupStatusReport{.{
        .group_id = 101,
        .doc_count = 7,
        .disk_bytes = 4096,
        .empty = false,
        .created_at_millis = 100,
        .updated_at_millis = 1_000 + 31 * std.time.ms_per_s,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 3,
    }};

    const existing = table_manager.StoreRecord{
        .store_id = 21,
        .node_id = 1,
        .role = "data",
        .failure_domain = "rack-a",
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 900,
        .group_statuses = existing_groups[0..],
    };
    var observation = StoreObservation{
        .store_id = 21,
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 900,
        .group_statuses = fresh_groups[0..],
    };
    try std.testing.expect(!observationChangesRecord(existing, observation));

    // Causal acknowledgements are state transitions, not heartbeats. They
    // must bypass timestamp coalescing even when every storage fact is stable.
    fresh_groups[0].observed_reallocation_request_id = 0x1234;
    try std.testing.expect(observationChangesRecord(existing, observation));
    fresh_groups[0].observed_reallocation_request_id = 0;

    observation.group_statuses = stale_groups[0..];
    try std.testing.expect(observationChangesRecord(existing, observation));
}

test "store observer can ignore unactivated repair fields without hiding other changes" {
    var existing_indexes = [_]table_manager.RuntimeIndexStatusReport{.{
        .name = "visual_idx",
        .kind = "dense_vector",
        .doc_count = 10,
    }};
    var observed_indexes = existing_indexes;
    observed_indexes[0].repair_status = .rebuilding;
    observed_indexes[0].repair_active_generation_serviceable = true;
    var existing_runtime = [_]table_manager.RuntimeGroupStatusReport{.{
        .table_id = 1,
        .table_name = "products",
        .group_id = 2,
        .store_id = 3,
        .node_id = 4,
        .source = "runtime",
        .freshness = "fresh",
        .indexes = existing_indexes[0..],
    }};
    var observed_runtime = existing_runtime;
    observed_runtime[0].indexes = observed_indexes[0..];
    const existing = table_manager.StoreRecord{
        .store_id = 3,
        .node_id = 4,
        .runtime_statuses = existing_runtime[0..],
    };
    const observation = StoreObservation{
        .store_id = 3,
        .runtime_statuses = observed_runtime[0..],
    };

    try std.testing.expect(observationChangesRecord(existing, observation));
    try std.testing.expect(!observationChangesRecordWithRepairStatus(existing, observation, false));
    observed_indexes[0].doc_count += 1;
    try std.testing.expect(observationChangesRecordWithRepairStatus(existing, observation, false));
}

test "store observer fences repair transitions by registered reporter incarnation and generation" {
    var existing_indexes = [_]table_manager.RuntimeIndexStatusReport{.{
        .name = "visual_idx",
        .kind = "dense_vector",
        .coverage_generation = 7,
        .coverage_config_hash = 8,
        .coverage_identity_ready = true,
        .repair_status = .rebuilding,
        .repair_active_generation_serviceable = true,
    }};
    var next_indexes = existing_indexes;
    next_indexes[0].repair_status = .failed;
    next_indexes[0].repair_active_generation_serviceable = false;
    var existing_runtime = [_]table_manager.RuntimeGroupStatusReport{.{
        .table_id = 1,
        .group_id = 2,
        .store_id = 3,
        .node_id = 4,
        .updated_at_ns = 500,
        .indexes = existing_indexes[0..],
    }};
    var next_runtime = existing_runtime;
    next_runtime[0].updated_at_ns = 1;
    next_runtime[0].indexes = next_indexes[0..];
    const existing: table_manager.StoreRecord = .{
        .store_id = 3,
        .node_id = 4,
        .reporter_incarnation = 0x1111,
        .status_generation = 8,
        .runtime_statuses = existing_runtime[0..],
    };

    var observation: StoreObservation = .{
        .store_id = 3,
        .reporter_incarnation = 0x2222,
        .status_generation = 100,
        .runtime_statuses = next_runtime[0..],
    };
    try std.testing.expect(!observationChangesRecordWithRepairStatus(existing, observation, true));

    observation.reporter_incarnation = existing.reporter_incarnation;
    observation.status_generation = existing.status_generation - 1;
    try std.testing.expect(!observationChangesRecordWithRepairStatus(existing, observation, true));

    observation.status_generation = existing.status_generation;
    try std.testing.expect(!observationChangesRecordWithRepairStatus(existing, observation, true));

    // Generation, rather than incomparable wall time, authorizes the active
    // process to publish the transition and to delete an omitted repair fact.
    observation.status_generation = existing.status_generation + 1;
    try std.testing.expect(observationChangesRecordWithRepairStatus(existing, observation, true));
    observation.runtime_statuses = &.{};
    try std.testing.expect(observationChangesRecordWithRepairStatus(existing, observation, true));

    var legacy_existing = existing;
    legacy_existing.reporter_incarnation = 0;
    legacy_existing.status_generation = 0;
    observation.reporter_incarnation = 0;
    observation.status_generation = 0;
    observation.runtime_statuses = next_runtime[0..];
    try std.testing.expect(!observationChangesRecordWithRepairStatus(legacy_existing, observation, true));
    next_runtime[0].updated_at_ns = existing_runtime[0].updated_at_ns + 1;
    try std.testing.expect(observationChangesRecordWithRepairStatus(legacy_existing, observation, true));
}

test "store observer preserves committed repair facts while capability is unknown" {
    var existing_indexes = [_]table_manager.RuntimeIndexStatusReport{.{
        .name = "visual_idx",
        .kind = "dense_vector",
        .doc_count = 10,
        .coverage_generation = 7,
        .coverage_config_hash = 8,
        .coverage_identity_ready = true,
        .repair_status = .rebuilding,
        .repair_active_generation_serviceable = true,
    }};
    var existing_runtime = [_]table_manager.RuntimeGroupStatusReport{.{
        .table_id = 1,
        .table_name = "products",
        .group_id = 2,
        .store_id = 3,
        .node_id = 4,
        .updated_at_ns = 200 * std.time.ns_per_ms,
        .status_generation = 10,
        .source = "runtime",
        .freshness = "fresh",
        .indexes = existing_indexes[0..],
    }};
    var records = [_]table_manager.StoreRecord{try table_manager.cloneStore(std.testing.allocator, .{
        .store_id = 3,
        .node_id = 4,
        .runtime_statuses = existing_runtime[0..],
    })};
    defer table_manager.freeStore(std.testing.allocator, records[0]);

    var observed_indexes = existing_indexes;
    observed_indexes[0].doc_count = 11;
    observed_indexes[0].repair_status = .failed;
    observed_indexes[0].repair_active_generation_serviceable = false;
    var observed_runtime = existing_runtime;
    observed_runtime[0].indexes = observed_indexes[0..];
    const observation = StoreObservation{
        .store_id = 3,
        .runtime_statuses = observed_runtime[0..],
    };

    try std.testing.expectEqual(
        @as(usize, 1),
        try applyObservationsOwnedWithRepairStatus(std.testing.allocator, &records, &.{observation}, false),
    );
    try std.testing.expectEqual(@as(u64, 11), records[0].runtime_statuses[0].indexes[0].doc_count);
    try std.testing.expectEqual(
        table_manager.IndexRepairStatus.rebuilding,
        records[0].runtime_statuses[0].indexes[0].repair_status.?,
    );
    try std.testing.expect(records[0].runtime_statuses[0].indexes[0].repair_active_generation_serviceable);

    // Missing or changed kind without a complete identity is not proof of a
    // catalog replacement. This covers legacy JSON, where kind defaults empty.
    observed_indexes[0].doc_count = 12;
    observed_indexes[0].kind = "";
    observed_indexes[0].coverage_identity_ready = false;
    try std.testing.expectEqual(
        @as(usize, 1),
        try applyObservationsOwnedWithRepairStatus(std.testing.allocator, &records, &.{observation}, false),
    );
    try std.testing.expectEqual(@as(u64, 11), records[0].runtime_statuses[0].indexes[0].doc_count);
    try std.testing.expectEqualStrings("dense_vector", records[0].runtime_statuses[0].indexes[0].kind);
    try std.testing.expectEqual(
        table_manager.IndexRepairStatus.rebuilding,
        records[0].runtime_statuses[0].indexes[0].repair_status.?,
    );

    observed_indexes[0].kind = "sparse_vector";
    try std.testing.expectEqual(
        @as(usize, 1),
        try applyObservationsOwnedWithRepairStatus(std.testing.allocator, &records, &.{observation}, false),
    );
    try std.testing.expectEqualStrings("dense_vector", records[0].runtime_statuses[0].indexes[0].kind);
    try std.testing.expectEqual(
        table_manager.IndexRepairStatus.rebuilding,
        records[0].runtime_statuses[0].indexes[0].repair_status.?,
    );

    // A different but incomplete materialization identity has no authority to
    // erase the committed repair fact.
    observed_indexes[0].kind = "dense_vector";
    observed_indexes[0].doc_count = 13;
    observed_indexes[0].coverage_generation = 9;
    observed_indexes[0].coverage_config_hash = 0;
    observed_indexes[0].coverage_identity_ready = false;
    try std.testing.expectEqual(
        @as(usize, 1),
        try applyObservationsOwnedWithRepairStatus(std.testing.allocator, &records, &.{observation}, false),
    );
    try std.testing.expectEqual(@as(u64, 11), records[0].runtime_statuses[0].indexes[0].doc_count);
    try std.testing.expectEqual(
        table_manager.IndexRepairStatus.rebuilding,
        records[0].runtime_statuses[0].indexes[0].repair_status.?,
    );
    try std.testing.expect(records[0].runtime_statuses[0].indexes[0].repair_active_generation_serviceable);

    // Missing causal metadata on either side is ambiguous and must fail closed.
    observed_indexes[0].coverage_identity_ready = true;
    observed_runtime[0].updated_at_ns = 0;
    try std.testing.expectEqual(
        @as(usize, 1),
        try applyObservationsOwnedWithRepairStatus(std.testing.allocator, &records, &.{observation}, false),
    );
    try std.testing.expectEqual(@as(u64, 11), records[0].runtime_statuses[0].indexes[0].doc_count);
    try std.testing.expectEqual(
        table_manager.IndexRepairStatus.rebuilding,
        records[0].runtime_statuses[0].indexes[0].repair_status.?,
    );

    observed_runtime[0].updated_at_ns = 300 * std.time.ns_per_ms;
    records[0].runtime_statuses[0].updated_at_ns = 0;
    try std.testing.expectEqual(
        @as(usize, 1),
        try applyObservationsOwnedWithRepairStatus(std.testing.allocator, &records, &.{observation}, false),
    );
    try std.testing.expectEqual(@as(u64, 11), records[0].runtime_statuses[0].indexes[0].doc_count);
    try std.testing.expectEqual(
        table_manager.IndexRepairStatus.rebuilding,
        records[0].runtime_statuses[0].indexes[0].repair_status.?,
    );
    records[0].runtime_statuses[0].updated_at_ns = 200 * std.time.ns_per_ms;

    // A complete but older cached identity still has no deletion authority.
    // Producer generations can reset, so even a larger generation does not
    // override a regressed cross-process timestamp.
    observed_runtime[0].updated_at_ns = 100 * std.time.ns_per_ms;
    observed_runtime[0].status_generation = 99;
    try std.testing.expectEqual(
        @as(usize, 1),
        try applyObservationsOwnedWithRepairStatus(std.testing.allocator, &records, &.{observation}, false),
    );
    try std.testing.expectEqual(@as(u64, 11), records[0].runtime_statuses[0].indexes[0].doc_count);
    try std.testing.expectEqual(
        table_manager.IndexRepairStatus.rebuilding,
        records[0].runtime_statuses[0].indexes[0].repair_status.?,
    );

    // Once the producer supplies a complete, newer replacement identity, it
    // must not inherit repair state from the retired materialization. A lower
    // process-local generation remains valid after a producer restart.
    observed_runtime[0].updated_at_ns = 300 * std.time.ns_per_ms;
    observed_runtime[0].status_generation = 1;
    try std.testing.expectEqual(
        @as(usize, 1),
        try applyObservationsOwnedWithRepairStatus(std.testing.allocator, &records, &.{observation}, false),
    );
    try std.testing.expectEqual(@as(u64, 13), records[0].runtime_statuses[0].indexes[0].doc_count);
    try std.testing.expect(records[0].runtime_statuses[0].indexes[0].repair_status == null);
    try std.testing.expect(!records[0].runtime_statuses[0].indexes[0].repair_active_generation_serviceable);
}

test "metadata store observer detects exact voter set changes at a stable count" {
    const original_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 103 }, null);
    const changed_fingerprint = table_manager.voterSetFingerprint(&.{ 101, 102, 104 }, null);
    var existing_groups = [_]table_manager.GroupStatusReport{.{
        .group_id = 101,
        .local_voter = true,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = original_fingerprint,
    }};
    var changed_groups = [_]table_manager.GroupStatusReport{.{
        .group_id = 101,
        .local_voter = true,
        .voter_count = 3,
        .voter_set_known = true,
        .voter_set_fingerprint = changed_fingerprint,
    }};
    const existing = table_manager.StoreRecord{
        .store_id = 21,
        .node_id = 101,
        .role = "data",
        .group_statuses = existing_groups[0..],
    };
    const observation = StoreObservation{
        .store_id = 21,
        .group_statuses = changed_groups[0..],
    };

    try std.testing.expect(observationChangesRecord(existing, observation));
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
