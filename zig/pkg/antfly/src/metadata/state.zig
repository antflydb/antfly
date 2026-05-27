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
const raft_reconciler = @import("../raft/reconciler.zig");
const metadata_reconciler = @import("reconciler.zig");
const reallocation_request = @import("reallocation_request.zig");
const metadata_store_observer = @import("store_observer.zig");
const metadata_table_manager = @import("table_manager.zig");
const transition_state = @import("transition_state.zig");
const data = @import("../data/mod.zig");

pub const CandidatePlacementInfo = struct {
    node_id: u64,
    store_id: u64 = 0,
    role: []const u8,
    failure_domain: []const u8,
    priority: u8 = 0,
    status_tag: metadata_store_observer.PlacementStatusTag = .preferred,
    available_bytes: u64 = 0,
    lease_pressure: u32 = 0,
    read_load: u32 = 0,
    write_load: u32 = 0,
    retain_current: bool = true,
};

pub const CapturedCurrentState = struct {
    current: metadata_reconciler.CurrentMetadataState,
    placement_intents: []raft_reconciler.PlacementIntent,
    tables: []metadata_table_manager.TableRecord,
    ranges: []metadata_table_manager.RangeRecord,
    restore_progresses: []metadata_table_manager.RestoreProgressRecord,
    schema_progresses: []metadata_table_manager.SchemaProgressRecord,
    merged_group_statuses: []metadata_reconciler.MergedGroupStatus,
    split_observations: []metadata_reconciler.SplitRuntimeObservation,
    merge_observations: []metadata_reconciler.MergeRuntimeObservation,

    pub fn deinit(self: *CapturedCurrentState, alloc: std.mem.Allocator) void {
        for (self.placement_intents) |intent| {
            var record = intent.record;
            record.deinit(alloc);
            if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
        }
        alloc.free(self.placement_intents);
        for (self.tables) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(self.tables);
        for (self.ranges) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(self.ranges);
        for (self.restore_progresses) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
        alloc.free(self.restore_progresses);
        alloc.free(self.schema_progresses);
        alloc.free(self.merged_group_statuses);
        alloc.free(self.split_observations);
        alloc.free(self.merge_observations);
        self.* = undefined;
    }
};

pub const MetadataState = struct {
    alloc: std.mem.Allocator,
    desired: metadata_table_manager.TableManager,
    projected: metadata_table_manager.TableManager,
    placement_candidate_node_ids: []u64 = &.{},
    projected_node_ids: []u64 = &.{},
    projected_store_node_ids: []u64 = &.{},
    projected_store_candidates: []CandidatePlacementInfo = &.{},
    projected_store_topology_present: bool = false,
    committed_nodes: std.ArrayListUnmanaged(metadata_table_manager.NodeRecord) = .empty,
    committed_stores: std.ArrayListUnmanaged(metadata_table_manager.StoreRecord) = .empty,
    committed_splits: std.ArrayListUnmanaged(transition_state.SplitTransitionRecord) = .empty,
    committed_merges: std.ArrayListUnmanaged(transition_state.MergeTransitionRecord) = .empty,

    pub fn init(alloc: std.mem.Allocator) MetadataState {
        return .{
            .alloc = alloc,
            .desired = metadata_table_manager.TableManager.init(alloc),
            .projected = metadata_table_manager.TableManager.init(alloc),
        };
    }

    pub fn deinit(self: *MetadataState) void {
        self.clearCommitted();
        self.committed_splits.deinit(self.alloc);
        self.committed_merges.deinit(self.alloc);
        self.clearCommittedNodes();
        self.committed_nodes.deinit(self.alloc);
        self.clearCommittedStores();
        self.committed_stores.deinit(self.alloc);
        if (self.placement_candidate_node_ids.len > 0) self.alloc.free(self.placement_candidate_node_ids);
        if (self.projected_node_ids.len > 0) self.alloc.free(self.projected_node_ids);
        if (self.projected_store_node_ids.len > 0) self.alloc.free(self.projected_store_node_ids);
        if (self.projected_store_candidates.len > 0) {
            for (self.projected_store_candidates) |candidate| {
                self.alloc.free(candidate.role);
                self.alloc.free(candidate.failure_domain);
            }
            self.alloc.free(self.projected_store_candidates);
        }
        self.desired.deinit();
        self.projected.deinit();
        self.* = undefined;
    }

    pub fn tableManager(self: *MetadataState) *metadata_table_manager.TableManager {
        return &self.desired;
    }

    pub fn projectedTableManager(self: *MetadataState) *metadata_table_manager.TableManager {
        return &self.projected;
    }

    pub fn setPlacementCandidates(self: *MetadataState, candidate_node_ids: []const u64) !void {
        const owned = try self.alloc.dupe(u64, candidate_node_ids);
        if (self.placement_candidate_node_ids.len > 0) self.alloc.free(self.placement_candidate_node_ids);
        self.placement_candidate_node_ids = owned;
    }

    pub fn placementCandidates(self: *const MetadataState) []const u64 {
        if (self.placement_candidate_node_ids.len > 0) return self.placement_candidate_node_ids;
        if (self.projected_store_topology_present) return self.projected_store_node_ids;
        return self.projected_node_ids;
    }

    pub fn placementCandidateInfo(self: *const MetadataState) []const CandidatePlacementInfo {
        return self.projected_store_candidates;
    }

    pub fn seedDesiredFromProjected(self: *MetadataState) !void {
        const tables = try self.projected.listTables(self.alloc);
        defer self.projected.freeTables(self.alloc, tables);
        const ranges = try self.projected.listRanges(self.alloc);
        defer self.projected.freeRanges(self.alloc, ranges);
        try self.desired.replaceTopology(tables, ranges);
    }

    pub fn syncProjected(self: *MetadataState, service: anytype) !void {
        const projected_tables = try service.listProjectedTables(self.alloc);
        defer service.freeProjectedTables(self.alloc, projected_tables);
        const projected_ranges = try service.listProjectedRanges(self.alloc);
        defer service.freeProjectedRanges(self.alloc, projected_ranges);
        const projected_nodes = try listProjectedNodes(self, service);
        defer freeProjectedNodes(self, service, projected_nodes);
        const projected_stores = try listProjectedStores(self, service);
        defer freeProjectedStores(self, service, projected_stores);
        const split_records = try service.listProjectedSplitTransitions(self.alloc);
        defer service.freeProjectedSplitTransitions(self.alloc, split_records);
        const merge_records = try service.listProjectedMergeTransitions(self.alloc);
        defer service.freeProjectedMergeTransitions(self.alloc, merge_records);

        _ = try self.projected.replaceProjectedTopology(projected_tables, projected_ranges);
        self.clearCommitted();
        self.clearCommittedNodes();
        self.clearCommittedStores();
        if (self.projected_node_ids.len > 0) {
            self.alloc.free(self.projected_node_ids);
            self.projected_node_ids = &.{};
        }
        if (self.projected_store_node_ids.len > 0) {
            self.alloc.free(self.projected_store_node_ids);
            self.projected_store_node_ids = &.{};
        }
        self.projected_store_topology_present = false;
        if (self.projected_store_candidates.len > 0) {
            for (self.projected_store_candidates) |candidate| {
                self.alloc.free(candidate.role);
                self.alloc.free(candidate.failure_domain);
            }
            self.alloc.free(self.projected_store_candidates);
            self.projected_store_candidates = &.{};
        }
        var projected_node_ids = std.ArrayListUnmanaged(u64).empty;
        errdefer {
            projected_node_ids.deinit(self.alloc);
        }
        for (projected_nodes) |record| {
            try self.committed_nodes.append(self.alloc, try metadata_table_manager.cloneNode(self.alloc, record));
            if (metadata_table_manager.nodeLifecycleActive(record.lifecycle)) {
                try projected_node_ids.append(self.alloc, record.node_id);
            }
        }
        self.projected_node_ids = try projected_node_ids.toOwnedSlice(self.alloc);
        errdefer {
            if (self.projected_node_ids.len > 0) self.alloc.free(self.projected_node_ids);
            self.projected_node_ids = &.{};
        }
        const derived_store_candidates = try deriveStoreCandidateInfo(self.alloc, projected_nodes, projected_stores);
        errdefer {
            for (derived_store_candidates) |candidate| {
                self.alloc.free(candidate.role);
                self.alloc.free(candidate.failure_domain);
            }
            self.alloc.free(derived_store_candidates);
        }
        const derived_store_node_ids = try self.alloc.alloc(u64, derived_store_candidates.len);
        errdefer {
            self.alloc.free(derived_store_node_ids);
        }
        for (derived_store_candidates, 0..) |candidate, i| derived_store_node_ids[i] = candidate.node_id;
        for (projected_stores) |record| {
            try self.committed_stores.append(self.alloc, try metadata_table_manager.cloneStore(self.alloc, record));
        }
        for (split_records) |record| try self.committed_splits.append(self.alloc, try cloneSplitRecord(self.alloc, record));
        for (merge_records) |record| try self.committed_merges.append(self.alloc, try cloneMergeRecord(self.alloc, record));
        self.projected_store_candidates = derived_store_candidates;
        self.projected_store_node_ids = derived_store_node_ids;
        self.projected_store_topology_present = projected_stores.len > 0;
    }

    pub fn captureCurrent(self: *MetadataState, service: anytype) !CapturedCurrentState {
        const placement_intents = try service.listProjectedPlacementIntents(self.alloc);
        errdefer service.freeProjectedPlacementIntents(self.alloc, placement_intents);
        const tables = try self.projected.listTables(self.alloc);
        errdefer self.projected.freeTables(self.alloc, tables);
        const ranges = try self.projected.listRanges(self.alloc);
        errdefer self.projected.freeRanges(self.alloc, ranges);
        const restore_progresses = try listProjectedRestoreProgress(self, service);
        errdefer freeProjectedRestoreProgress(self, service, restore_progresses);
        const schema_progresses = try listProjectedSchemaProgress(self, service);
        errdefer freeProjectedSchemaProgress(self, service, schema_progresses);
        const split_observations = try self.alloc.alloc(metadata_reconciler.SplitRuntimeObservation, self.committed_splits.items.len);
        errdefer self.alloc.free(split_observations);
        const merge_observations = try self.alloc.alloc(metadata_reconciler.MergeRuntimeObservation, self.committed_merges.items.len);
        errdefer self.alloc.free(merge_observations);
        const projected_reallocation_request = try getProjectedReallocationRequest(service);

        for (self.committed_splits.items, 0..) |record, i| {
            split_observations[i] = .{
                .transition_id = record.transition_id,
                .observation = (service.observeSplitTransition(record.transition_id) catch |err| blk: {
                    std.log.warn("split transition observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                    break :blk null;
                }) orelse defaultSplitObservation(),
            };
        }
        for (self.committed_merges.items, 0..) |record, i| {
            merge_observations[i] = .{
                .transition_id = record.transition_id,
                .observation = (service.observeMergeTransition(record.transition_id) catch |err| blk: {
                    std.log.warn("merge transition observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
                    break :blk null;
                }) orelse defaultMergeObservation(record),
            };
        }
        const merged_group_statuses = try mergeHealthyGroupStatuses(
            self.alloc,
            tables,
            ranges,
            placement_intents,
            restore_progresses,
            self.committed_stores.items,
            self.committed_splits.items,
            self.committed_merges.items,
            split_observations,
            merge_observations,
        );
        errdefer freeMergedGroupStatuses(self.alloc, merged_group_statuses);

        return .{
            .current = .{
                .placement_intents = placement_intents,
                .tables = tables,
                .ranges = ranges,
                .stores = self.committed_stores.items,
                .merged_group_statuses = merged_group_statuses,
                .restore_progresses = restore_progresses,
                .reallocate_requested = projected_reallocation_request != null,
                .schema_progresses = schema_progresses,
                .split_transitions = self.committed_splits.items,
                .merge_transitions = self.committed_merges.items,
                .split_observations = split_observations,
                .merge_observations = merge_observations,
            },
            .placement_intents = placement_intents,
            .tables = tables,
            .ranges = ranges,
            .restore_progresses = restore_progresses,
            .schema_progresses = schema_progresses,
            .merged_group_statuses = merged_group_statuses,
            .split_observations = split_observations,
            .merge_observations = merge_observations,
        };
    }

    fn clearCommitted(self: *MetadataState) void {
        for (self.committed_splits.items) |record| metadata_table_manager.freeSplitTransitionRecord(self.alloc, record);
        self.committed_splits.clearRetainingCapacity();
        for (self.committed_merges.items) |record| metadata_table_manager.freeMergeTransitionRecord(self.alloc, record);
        self.committed_merges.clearRetainingCapacity();
    }

    fn clearCommittedNodes(self: *MetadataState) void {
        for (self.committed_nodes.items) |record| metadata_table_manager.freeNode(self.alloc, record);
        self.committed_nodes.clearRetainingCapacity();
    }

    fn clearCommittedStores(self: *MetadataState) void {
        for (self.committed_stores.items) |record| metadata_table_manager.freeStore(self.alloc, record);
        self.committed_stores.clearRetainingCapacity();
    }
};

fn getProjectedReallocationRequest(service: anytype) !?reallocation_request.ReallocationRequestRecord {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "getProjectedReallocationRequest")) {
        return try service.getProjectedReallocationRequest();
    }
    return null;
}

fn listProjectedNodes(self: *MetadataState, service: anytype) ![]metadata_table_manager.NodeRecord {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "listProjectedNodes")) {
        return try service.listProjectedNodes(self.alloc);
    }
    return try self.alloc.alloc(metadata_table_manager.NodeRecord, 0);
}

fn freeProjectedNodes(self: *MetadataState, service: anytype, records: []metadata_table_manager.NodeRecord) void {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "freeProjectedNodes")) {
        service.freeProjectedNodes(self.alloc, records);
        return;
    }
    self.alloc.free(records);
}

fn listProjectedStores(self: *MetadataState, service: anytype) ![]metadata_table_manager.StoreRecord {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "listProjectedStores")) {
        return try service.listProjectedStores(self.alloc);
    }
    return try self.alloc.alloc(metadata_table_manager.StoreRecord, 0);
}

fn freeProjectedStores(self: *MetadataState, service: anytype, records: []metadata_table_manager.StoreRecord) void {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "freeProjectedStores")) {
        service.freeProjectedStores(self.alloc, records);
        return;
    }
    self.alloc.free(records);
}

fn listProjectedRestoreProgress(self: *MetadataState, service: anytype) ![]metadata_table_manager.RestoreProgressRecord {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "listProjectedRestoreProgress")) {
        return try service.listProjectedRestoreProgress(self.alloc);
    }
    return try self.alloc.alloc(metadata_table_manager.RestoreProgressRecord, 0);
}

fn freeProjectedRestoreProgress(self: *MetadataState, service: anytype, records: []metadata_table_manager.RestoreProgressRecord) void {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "freeProjectedRestoreProgress")) {
        service.freeProjectedRestoreProgress(self.alloc, records);
        return;
    }
    self.alloc.free(records);
}

fn listProjectedSchemaProgress(self: *MetadataState, service: anytype) ![]metadata_table_manager.SchemaProgressRecord {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "listProjectedSchemaProgress")) {
        return try service.listProjectedSchemaProgress(self.alloc);
    }
    return try self.alloc.alloc(metadata_table_manager.SchemaProgressRecord, 0);
}

fn freeProjectedSchemaProgress(self: *MetadataState, service: anytype, records: []metadata_table_manager.SchemaProgressRecord) void {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };
    if (@hasDecl(ServiceDeclType, "freeProjectedSchemaProgress")) {
        service.freeProjectedSchemaProgress(self.alloc, records);
        return;
    }
    self.alloc.free(records);
}

fn deriveStoreCandidateInfo(
    alloc: std.mem.Allocator,
    nodes: []const metadata_table_manager.NodeRecord,
    stores: []const metadata_table_manager.StoreRecord,
) ![]CandidatePlacementInfo {
    const Candidate = struct {
        node_id: u64,
        store_id: u64,
        role: []const u8,
        health_priority: u8,
        available_bytes: u64,
        failure_domain: []const u8,
        status_tag: metadata_store_observer.PlacementStatusTag,
        lease_pressure: u32,
        read_load: u32,
        write_load: u32,
        retain_current: bool,
    };

    var candidates = std.ArrayListUnmanaged(Candidate).empty;
    errdefer {
        for (candidates.items) |candidate| {
            alloc.free(candidate.role);
            alloc.free(candidate.failure_domain);
        }
        candidates.deinit(alloc);
    }

    for (stores) |record| {
        if (!storeNodeAcceptsPlacement(nodes, record.node_id)) continue;
        const placement_status = metadata_store_observer.classifyStore(record);
        if (placement_status.tag == .excluded) continue;
        const health_priority = placement_status.priority;

        var existing: ?*Candidate = null;
        for (candidates.items) |*candidate| {
            if (candidate.node_id == record.node_id) {
                existing = candidate;
                break;
            }
        }
        if (existing) |candidate| {
            if (health_priority < candidate.health_priority) {
                candidate.health_priority = health_priority;
                candidate.available_bytes = record.available_bytes;
                candidate.lease_pressure = record.lease_pressure;
                candidate.read_load = record.read_load;
                candidate.write_load = record.write_load;
                candidate.status_tag = placement_status.tag;
                candidate.retain_current = placement_status.retain_current;
                candidate.store_id = record.store_id;
                alloc.free(candidate.role);
                candidate.role = try alloc.dupe(u8, record.role);
                alloc.free(candidate.failure_domain);
                candidate.failure_domain = try alloc.dupe(u8, record.failure_domain);
            } else if (health_priority == candidate.health_priority and storePressureLess(record, candidate.*)) {
                candidate.available_bytes = record.available_bytes;
                candidate.lease_pressure = record.lease_pressure;
                candidate.read_load = record.read_load;
                candidate.write_load = record.write_load;
                candidate.status_tag = placement_status.tag;
                candidate.retain_current = placement_status.retain_current;
                alloc.free(candidate.role);
                candidate.role = try alloc.dupe(u8, record.role);
                alloc.free(candidate.failure_domain);
                candidate.failure_domain = try alloc.dupe(u8, record.failure_domain);
            }
            continue;
        }
        try candidates.append(alloc, .{
            .node_id = record.node_id,
            .store_id = record.store_id,
            .role = try alloc.dupe(u8, record.role),
            .health_priority = health_priority,
            .available_bytes = record.available_bytes,
            .failure_domain = try alloc.dupe(u8, record.failure_domain),
            .status_tag = placement_status.tag,
            .lease_pressure = record.lease_pressure,
            .read_load = record.read_load,
            .write_load = record.write_load,
            .retain_current = placement_status.retain_current,
        });
    }

    std.mem.sort(Candidate, candidates.items, {}, struct {
        fn lessThan(_: void, a: Candidate, b: Candidate) bool {
            if (a.health_priority != b.health_priority) return a.health_priority < b.health_priority;
            const a_pressure = metadata_store_observer.combinedPressure(a.lease_pressure, a.read_load, a.write_load);
            const b_pressure = metadata_store_observer.combinedPressure(b.lease_pressure, b.read_load, b.write_load);
            if (a_pressure != b_pressure) return a_pressure < b_pressure;
            if (a.available_bytes == b.available_bytes) return a.node_id < b.node_id;
            return a.available_bytes > b.available_bytes;
        }
    }.lessThan);

    const out = try alloc.alloc(CandidatePlacementInfo, candidates.items.len);
    for (candidates.items, 0..) |candidate, i| out[i] = .{
        .node_id = candidate.node_id,
        .store_id = candidate.store_id,
        .role = candidate.role,
        .failure_domain = candidate.failure_domain,
        .priority = candidate.health_priority,
        .status_tag = candidate.status_tag,
        .available_bytes = candidate.available_bytes,
        .lease_pressure = candidate.lease_pressure,
        .read_load = candidate.read_load,
        .write_load = candidate.write_load,
        .retain_current = candidate.retain_current,
    };
    candidates.deinit(alloc);
    return out;
}

fn storeNodeAcceptsPlacement(nodes: []const metadata_table_manager.NodeRecord, node_id: u64) bool {
    for (nodes) |node| {
        if (node.node_id != node_id) continue;
        return metadata_table_manager.nodeLifecycleActive(node.lifecycle);
    }
    return true;
}

fn storePressureLess(record: metadata_table_manager.StoreRecord, candidate: anytype) bool {
    const record_pressure = metadata_store_observer.combinedPressure(record.lease_pressure, record.read_load, record.write_load);
    const candidate_pressure = metadata_store_observer.combinedPressure(candidate.lease_pressure, candidate.read_load, candidate.write_load);
    if (record_pressure != candidate_pressure) return record_pressure < candidate_pressure;
    if (record.available_bytes != candidate.available_bytes) return record.available_bytes > candidate.available_bytes;
    return record.node_id < candidate.node_id;
}

const GroupStatusMergeCandidate = struct {
    store_id: u64,
    report: metadata_table_manager.GroupStatusReport,
};

const GroupStatusMergeState = struct {
    group_id: u64,
    latest: ?GroupStatusMergeCandidate = null,
    latest_leader: ?GroupStatusMergeCandidate = null,
    ambiguous_leader: bool = false,
    observed_voter_count: ?u16 = null,
    ambiguous_voter_count: bool = false,
    healthy_voter_reports: u16 = 0,
    joint_consensus: bool = false,
    transition_pending: bool = false,
    replay_required: bool = false,
    replay_caught_up: bool = false,
    cutover_ready: bool = false,
    reads_ready_after_cutover: bool = false,
    doc_identity_reassignment_active: bool = false,
    last_voter_store_id: u64 = 0,
    doc_identity: metadata_table_manager.RuntimeDocIdentityStatusReport = .{},
    doc_identity_namespace_conflict: bool = false,
};

pub fn mergeHealthyGroupStatuses(
    alloc: std.mem.Allocator,
    tables: []const metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    placement_intents: []const raft_reconciler.PlacementIntent,
    restore_progresses: []const metadata_table_manager.RestoreProgressRecord,
    stores: []const metadata_table_manager.StoreRecord,
    split_transitions: []const transition_state.SplitTransitionRecord,
    merge_transitions: []const transition_state.MergeTransitionRecord,
    split_observations: []const metadata_reconciler.SplitRuntimeObservation,
    merge_observations: []const metadata_reconciler.MergeRuntimeObservation,
) ![]metadata_reconciler.MergedGroupStatus {
    var states = std.ArrayListUnmanaged(GroupStatusMergeState).empty;
    defer states.deinit(alloc);
    var indexes = std.AutoHashMapUnmanaged(u64, usize).empty;
    defer indexes.deinit(alloc);

    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;

        for (store.group_statuses) |group_status| {
            const entry = try indexes.getOrPut(alloc, group_status.group_id);
            if (!entry.found_existing) {
                entry.value_ptr.* = states.items.len;
                try states.append(alloc, .{ .group_id = group_status.group_id });
            }
            var state = &states.items[entry.value_ptr.*];
            const candidate: GroupStatusMergeCandidate = .{
                .store_id = store.store_id,
                .report = group_status,
            };
            if (state.latest == null or moreCompleteGroupStatus(group_status, state.latest.?.report)) {
                state.latest = candidate;
            }
            if (group_status.local_voter and state.last_voter_store_id != store.store_id) {
                state.healthy_voter_reports +|= 1;
                state.last_voter_store_id = store.store_id;
            }
            if (group_status.voter_count > 0) {
                if (state.observed_voter_count) |existing| {
                    if (existing != group_status.voter_count) state.ambiguous_voter_count = true;
                } else {
                    state.observed_voter_count = group_status.voter_count;
                }
            }
            state.joint_consensus = state.joint_consensus or group_status.joint_consensus;
            state.transition_pending = state.transition_pending or group_status.transition_pending;
            state.replay_required = state.replay_required or group_status.replay_required;
            state.replay_caught_up = state.replay_caught_up or group_status.replay_caught_up;
            state.cutover_ready = state.cutover_ready or group_status.cutover_ready;
            state.reads_ready_after_cutover = state.reads_ready_after_cutover or group_status.reads_ready_after_cutover;
            if (group_status.local_leader) {
                if (state.latest_leader) |existing| {
                    if (group_status.updated_at_millis > existing.report.updated_at_millis) {
                        state.latest_leader = candidate;
                        state.ambiguous_leader = false;
                    } else if (group_status.updated_at_millis == existing.report.updated_at_millis and existing.store_id != store.store_id) {
                        state.ambiguous_leader = true;
                    }
                } else {
                    state.latest_leader = candidate;
                    state.ambiguous_leader = false;
                }
            }
        }

        for (store.runtime_statuses) |runtime_status| {
            if (!storeHasPlacement(placement_intents, runtime_status.group_id, store.node_id)) continue;
            if (runtime_status.doc_count == 0 and runtime_status.disk_bytes == 0 and !runtimeDocIdentityHasFacts(runtime_status.doc_identity)) continue;
            const entry = try indexes.getOrPut(alloc, runtime_status.group_id);
            if (!entry.found_existing) {
                entry.value_ptr.* = states.items.len;
                try states.append(alloc, .{ .group_id = runtime_status.group_id });
            }
            var state = &states.items[entry.value_ptr.*];
            const updated_at_millis = @divTrunc(runtime_status.updated_at_ns, std.time.ns_per_ms);
            const candidate: GroupStatusMergeCandidate = .{
                .store_id = store.store_id,
                .report = .{
                    .group_id = runtime_status.group_id,
                    .doc_count = runtime_status.doc_count,
                    .disk_bytes = runtime_status.disk_bytes,
                    .empty = runtime_status.doc_count == 0 and runtime_status.disk_bytes == 0,
                    .created_at_millis = runtime_status.created_at_millis,
                    .updated_at_millis = updated_at_millis,
                    .local_voter = true,
                    .voter_count = countPlacementIntents(placement_intents, runtime_status.group_id),
                },
            };
            if (state.latest == null or moreCompleteGroupStatus(candidate.report, state.latest.?.report)) {
                state.latest = candidate;
            }
            if (state.last_voter_store_id != store.store_id) {
                state.healthy_voter_reports +|= 1;
                state.last_voter_store_id = store.store_id;
            }
            const voter_count = countPlacementIntents(placement_intents, runtime_status.group_id);
            if (voter_count > 0) {
                if (state.observed_voter_count) |existing| {
                    if (existing != voter_count) state.ambiguous_voter_count = true;
                } else {
                    state.observed_voter_count = voter_count;
                }
            }
            mergeRuntimeDocIdentity(state, runtime_status.doc_identity);
        }
    }

    const merged = try alloc.alloc(metadata_reconciler.MergedGroupStatus, states.items.len);
    errdefer freeMergedGroupStatuses(alloc, merged);
    for (states.items, 0..) |state, i| {
        const latest = state.latest orelse unreachable;
        const base = latest.report;
        merged[i] = .{
            .group_id = base.group_id,
            .doc_count = base.doc_count,
            .disk_bytes = base.disk_bytes,
            .empty = base.empty,
            .created_at_millis = base.created_at_millis,
            .updated_at_millis = base.updated_at_millis,
            .leader_known = false,
            .leader_store_id = 0,
            .voter_count_known = state.observed_voter_count != null and !state.ambiguous_voter_count,
            .voter_count = if (state.observed_voter_count) |count| count else 0,
            .healthy_voter_reports = state.healthy_voter_reports,
            .joint_consensus = state.joint_consensus,
            .readiness_from_leader = false,
            .transition_pending = state.transition_pending,
            .replay_required = state.replay_required,
            .replay_caught_up = state.replay_caught_up,
            .cutover_ready = state.cutover_ready,
            .reads_ready_after_cutover = state.reads_ready_after_cutover,
            .doc_identity_reassignment_active = state.doc_identity_reassignment_active,
            .doc_identity_lifecycle = metadata_reconciler.doc_identity_lifecycle_unknown,
            .doc_identity = state.doc_identity,
            .doc_identity_namespace_conflict = state.doc_identity_namespace_conflict,
        };
        if (!state.ambiguous_leader) {
            if (state.latest_leader) |leader| {
                merged[i].leader_known = true;
                merged[i].leader_store_id = leader.store_id;
                merged[i].readiness_from_leader = true;
                merged[i].transition_pending = leader.report.transition_pending;
                merged[i].replay_required = leader.report.replay_required;
                merged[i].replay_caught_up = leader.report.replay_caught_up;
                merged[i].cutover_ready = leader.report.cutover_ready;
                merged[i].reads_ready_after_cutover = leader.report.reads_ready_after_cutover;
            }
        }
    }
    overlayDocIdentityNamespaceExpectations(merged, ranges);
    overlayTransitionObservationReadiness(merged, split_transitions, merge_transitions, split_observations, merge_observations);
    overlayRestoreReadiness(merged, tables, ranges, placement_intents, restore_progresses);
    refreshDocIdentityLifecycles(merged);
    return merged;
}

pub fn freeMergedGroupStatuses(alloc: std.mem.Allocator, statuses: []metadata_reconciler.MergedGroupStatus) void {
    alloc.free(statuses);
}

fn mergeRuntimeDocIdentity(
    state: *GroupStatusMergeState,
    incoming: metadata_table_manager.RuntimeDocIdentityStatusReport,
) void {
    if (!runtimeDocIdentityHasFacts(incoming)) return;
    if (!runtimeDocIdentityHasFacts(state.doc_identity)) {
        state.doc_identity = incoming;
        return;
    }
    if (runtimeDocIdentityHasOrdinalRows(state.doc_identity) and runtimeDocIdentityHasOrdinalRows(incoming) and
        !runtimeDocIdentitySameNamespace(state.doc_identity, incoming))
    {
        state.doc_identity_namespace_conflict = true;
    }
    state.doc_identity.rebuild_required = state.doc_identity.rebuild_required or incoming.rebuild_required;
    state.doc_identity.ordinal_capacity_exhausted = state.doc_identity.ordinal_capacity_exhausted or incoming.ordinal_capacity_exhausted;
    state.doc_identity.complete = state.doc_identity.complete and incoming.complete;
    state.doc_identity.allocated_ordinals = @max(state.doc_identity.allocated_ordinals, incoming.allocated_ordinals);
    state.doc_identity.state_rows = @max(state.doc_identity.state_rows, incoming.state_rows);
    state.doc_identity.live_ordinals = @max(state.doc_identity.live_ordinals, incoming.live_ordinals);
    state.doc_identity.tombstone_ordinals = @max(state.doc_identity.tombstone_ordinals, incoming.tombstone_ordinals);
    state.doc_identity.primary_docs_missing_ordinals = @max(state.doc_identity.primary_docs_missing_ordinals, incoming.primary_docs_missing_ordinals);
    state.doc_identity.primary_docs_missing_identity_state = @max(state.doc_identity.primary_docs_missing_identity_state, incoming.primary_docs_missing_identity_state);
    state.doc_identity.primary_docs_with_tombstone_ordinals = @max(state.doc_identity.primary_docs_with_tombstone_ordinals, incoming.primary_docs_with_tombstone_ordinals);
}

fn runtimeDocIdentityHasFacts(stats: metadata_table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.namespace_table_id != 0 or
        stats.namespace_shard_id != 0 or
        stats.namespace_range_id != 0 or
        stats.next_ordinal != 1 or
        stats.allocated_ordinals != 0 or
        stats.ordinal_capacity_remaining != 0 or
        stats.ordinal_capacity_exhausted or
        stats.rebuild_required or
        stats.state_rows != 0 or
        stats.live_ordinals != 0 or
        stats.tombstone_ordinals != 0 or
        stats.min_created_generation != 0 or
        stats.max_created_generation != 0 or
        stats.min_deleted_generation != 0 or
        stats.max_deleted_generation != 0 or
        stats.scanned_primary_docs != 0 or
        stats.primary_docs_missing_ordinals != 0 or
        stats.primary_docs_missing_identity_state != 0 or
        stats.primary_docs_with_tombstone_ordinals != 0 or
        stats.complete;
}

fn runtimeDocIdentityHasOrdinalRows(stats: metadata_table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.next_ordinal != 1 or
        stats.allocated_ordinals != 0 or
        stats.state_rows != 0 or
        stats.live_ordinals != 0 or
        stats.tombstone_ordinals != 0;
}

fn runtimeDocIdentityHasNamespace(stats: metadata_table_manager.RuntimeDocIdentityStatusReport) bool {
    return stats.namespace_table_id != 0 or
        stats.namespace_shard_id != 0 or
        stats.namespace_range_id != 0;
}

fn runtimeDocIdentitySameNamespace(
    left: metadata_table_manager.RuntimeDocIdentityStatusReport,
    right: metadata_table_manager.RuntimeDocIdentityStatusReport,
) bool {
    return left.namespace_table_id == right.namespace_table_id and
        left.namespace_shard_id == right.namespace_shard_id and
        left.namespace_range_id == right.namespace_range_id;
}

fn overlayDocIdentityNamespaceExpectations(
    merged: []metadata_reconciler.MergedGroupStatus,
    ranges: []const metadata_table_manager.RangeRecord,
) void {
    for (merged) |*status| {
        const range = findRangeForGroup(ranges, status.group_id) orelse continue;
        markDocIdentityRebuildRequiredOnNamespaceMismatch(status, range);
    }
}

fn markDocIdentityRebuildRequiredOnNamespaceMismatch(
    status: *metadata_reconciler.MergedGroupStatus,
    range: metadata_table_manager.RangeRecord,
) void {
    if (!runtimeDocIdentityHasNamespace(status.doc_identity)) return;
    if (status.doc_identity.namespace_table_id == range.table_id and
        status.doc_identity.namespace_shard_id == metadata_table_manager.rangeDocIdentityShardId(range) and
        status.doc_identity.namespace_range_id == metadata_table_manager.rangeDocIdentityRangeId(range)) return;
    status.doc_identity.rebuild_required = true;
}

fn refreshDocIdentityLifecycles(merged: []metadata_reconciler.MergedGroupStatus) void {
    for (merged) |*status| metadata_reconciler.refreshDocIdentityLifecycle(status);
}

fn storeHasPlacement(placements: []const raft_reconciler.PlacementIntent, group_id: u64, node_id: u64) bool {
    for (placements) |intent| {
        if (intent.record.group_id == group_id and intent.record.local_node_id == node_id) return true;
    }
    return false;
}

fn countPlacementIntents(placements: []const raft_reconciler.PlacementIntent, group_id: u64) u16 {
    var count: u16 = 0;
    for (placements) |intent| {
        if (intent.record.group_id == group_id) count +|= 1;
    }
    return count;
}

fn moreCompleteGroupStatus(candidate: metadata_table_manager.GroupStatusReport, current: metadata_table_manager.GroupStatusReport) bool {
    if (candidate.doc_count != current.doc_count) return candidate.doc_count > current.doc_count;
    if (candidate.disk_bytes != current.disk_bytes) return candidate.disk_bytes > current.disk_bytes;
    return candidate.updated_at_millis >= current.updated_at_millis;
}

fn overlayTransitionObservationReadiness(
    merged: []metadata_reconciler.MergedGroupStatus,
    split_transitions: []const transition_state.SplitTransitionRecord,
    merge_transitions: []const transition_state.MergeTransitionRecord,
    split_observations: []const metadata_reconciler.SplitRuntimeObservation,
    merge_observations: []const metadata_reconciler.MergeRuntimeObservation,
) void {
    for (split_transitions) |record| {
        const observation = findSplitObservation(split_observations, record.transition_id) orelse continue;
        if (observation.observation.source_local_leader) {
            if (findMergedGroupStatus(merged, record.source_group_id)) |status| {
                applySplitObservationReadiness(status, observation.observation);
            }
        }
        if (observation.observation.destination_local_leader) {
            if (findMergedGroupStatus(merged, record.destination_group_id)) |status| {
                applySplitObservationReadiness(status, observation.observation);
            }
        }
    }

    for (merge_transitions) |record| {
        const observation = findMergeObservation(merge_observations, record.transition_id) orelse continue;
        if (observation.observation.donor_local_leader) {
            if (findMergedGroupStatus(merged, record.donor_group_id)) |status| {
                applyMergeObservationReadiness(status, observation.observation.donor);
            }
        }
        if (observation.observation.receiver_local_leader) {
            if (findMergedGroupStatus(merged, record.receiver_group_id)) |status| {
                applyMergeObservationReadiness(status, observation.observation.receiver);
            }
        }
    }
}

fn overlayRestoreReadiness(
    merged: []metadata_reconciler.MergedGroupStatus,
    tables: []const metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    placement_intents: []const raft_reconciler.PlacementIntent,
    restore_progresses: []const metadata_table_manager.RestoreProgressRecord,
) void {
    for (merged) |*status| {
        const range = findRangeForGroup(ranges, status.group_id) orelse continue;
        const table = findTableForId(tables, range.table_id) orelse continue;
        const restore_backup_id = restoreBackupIdForRange(range, table) orelse continue;
        status.restore_pending = groupRestorePending(table.table_id, restore_backup_id, placement_intents, restore_progresses, status.group_id);
    }
}

fn groupRestorePending(
    table_id: u64,
    restore_backup_id: []const u8,
    placement_intents: []const raft_reconciler.PlacementIntent,
    restore_progresses: []const metadata_table_manager.RestoreProgressRecord,
    group_id: u64,
) bool {
    var expected: usize = 0;
    var restored: usize = 0;
    for (placement_intents) |intent| {
        if (intent.record.group_id != group_id) continue;
        expected += 1;
        if (findRestoreProgress(restore_progresses, table_id, intent.record.local_node_id, group_id, restore_backup_id)) |progress| {
            if (!progress.runtime_repair_complete) continue;
            restored += 1;
        }
    }
    return expected > 0 and restored < expected;
}

fn restoreBackupIdForRange(
    range: metadata_table_manager.RangeRecord,
    table: metadata_table_manager.TableRecord,
) ?[]const u8 {
    if (range.restore_backup_id.len > 0) return range.restore_backup_id;
    if (table.restore_backup_id.len > 0) return table.restore_backup_id;
    return null;
}

fn findRangeForGroup(
    ranges: []const metadata_table_manager.RangeRecord,
    group_id: u64,
) ?metadata_table_manager.RangeRecord {
    for (ranges) |range| {
        if (range.group_id == group_id) return range;
    }
    return null;
}

fn findTableForId(
    tables: []const metadata_table_manager.TableRecord,
    table_id: u64,
) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn findRestoreProgress(
    records: []const metadata_table_manager.RestoreProgressRecord,
    table_id: u64,
    node_id: u64,
    group_id: u64,
    backup_id: []const u8,
) ?metadata_table_manager.RestoreProgressRecord {
    for (records) |record| {
        if (record.table_id != table_id) continue;
        if (record.node_id != node_id) continue;
        if (record.group_id != group_id) continue;
        if (!std.mem.eql(u8, record.backup_id, backup_id)) continue;
        return record;
    }
    return null;
}

fn findMergedGroupStatus(
    merged: []metadata_reconciler.MergedGroupStatus,
    group_id: u64,
) ?*metadata_reconciler.MergedGroupStatus {
    for (merged) |*status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn findSplitObservation(
    observations: []const metadata_reconciler.SplitRuntimeObservation,
    transition_id: u64,
) ?metadata_reconciler.SplitRuntimeObservation {
    for (observations) |observation| {
        if (observation.transition_id == transition_id) return observation;
    }
    return null;
}

fn findMergeObservation(
    observations: []const metadata_reconciler.MergeRuntimeObservation,
    transition_id: u64,
) ?metadata_reconciler.MergeRuntimeObservation {
    for (observations) |observation| {
        if (observation.transition_id == transition_id) return observation;
    }
    return null;
}

fn applySplitObservationReadiness(
    status: *metadata_reconciler.MergedGroupStatus,
    observation: transition_state.SplitObservation,
) void {
    status.readiness_from_leader = true;
    status.transition_pending = observation.status.phase != .finalized and observation.status.phase != .rolled_back;
    status.replay_required = observation.status.replay_required;
    status.replay_caught_up = observation.status.replay_caught_up;
    status.cutover_ready = observation.status.cutover_ready;
    status.reads_ready_after_cutover = observation.status.destination_ready_for_reads;
}

fn applyMergeObservationReadiness(
    status: *metadata_reconciler.MergedGroupStatus,
    observation: data.MergeTransitionStatus,
) void {
    status.readiness_from_leader = true;
    status.transition_pending = observation.phase != .finalized and observation.phase != .rolled_back;
    status.replay_required = observation.replay_required;
    status.replay_caught_up = observation.replay_caught_up;
    status.cutover_ready = observation.cutover_ready;
    status.reads_ready_after_cutover = observation.receiver_ready_for_reads;
    status.doc_identity_reassignment_active = status.doc_identity_reassignment_active or observation.allow_doc_identity_reassignment;
}

fn cloneSplitRecord(alloc: std.mem.Allocator, record: transition_state.SplitTransitionRecord) !transition_state.SplitTransitionRecord {
    return .{
        .transition_id = record.transition_id,
        .source_group_id = record.source_group_id,
        .destination_group_id = record.destination_group_id,
        .phase = record.phase,
        .split_key = if (record.split_key) |value| try alloc.dupe(u8, value) else null,
        .source_range_end = if (record.source_range_end) |value| try alloc.dupe(u8, value) else null,
        .rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null,
    };
}

fn cloneMergeRecord(alloc: std.mem.Allocator, record: transition_state.MergeTransitionRecord) !transition_state.MergeTransitionRecord {
    return .{
        .transition_id = record.transition_id,
        .donor_group_id = record.donor_group_id,
        .receiver_group_id = record.receiver_group_id,
        .phase = record.phase,
        .rollback_reason = if (record.rollback_reason) |value| try alloc.dupe(u8, value) else null,
        .allow_doc_identity_reassignment = record.allow_doc_identity_reassignment,
    };
}

fn defaultSplitObservation() transition_state.SplitObservation {
    return .{
        .status = .{
            .phase = .prepare,
            .source_split_phase = .prepare,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .destination_ready_for_reads = false,
            .source_delta_sequence = 0,
            .dest_delta_sequence = 0,
        },
    };
}

fn defaultMergeObservation(record: transition_state.MergeTransitionRecord) transition_state.MergeObservation {
    return .{
        .donor = .{
            .phase = .prepare,
            .donor_group_id = record.donor_group_id,
            .receiver_group_id = record.receiver_group_id,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
        .receiver = .{
            .phase = .prepare,
            .donor_group_id = record.donor_group_id,
            .receiver_group_id = record.receiver_group_id,
            .receiver_accepts_donor_range = false,
            .bootstrapped = false,
            .replay_required = false,
            .replay_caught_up = false,
            .cutover_ready = false,
            .receiver_ready_for_reads = false,
            .donor_delta_sequence = 0,
            .receiver_delta_sequence = 0,
        },
    };
}

test "metadata state captures committed transitions and observations" {
    const FakeService = struct {
        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            const out = try alloc.alloc(metadata_table_manager.TableRecord, 1);
            out[0] = .{
                .table_id = 7,
                .name = try alloc.dupe(u8, "docs"),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try alloc.alloc(metadata_table_manager.RangeRecord, 1);
            out[0] = .{ .group_id = 71, .table_id = 7, .start_key = try alloc.dupe(u8, "doc:a"), .end_key = try alloc.dupe(u8, "doc:z") };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
            for (records) |record| {
                alloc.free(record.start_key);
                if (record.end_key) |end| alloc.free(end);
            }
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            const out = try alloc.alloc(transition_state.SplitTransitionRecord, 1);
            out[0] = .{
                .transition_id = 1,
                .source_group_id = 11,
                .destination_group_id = 12,
                .phase = .prepare,
                .split_key = try alloc.dupe(u8, "doc:m"),
            };
            return out;
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            for (records) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            const out = try alloc.alloc(transition_state.MergeTransitionRecord, 1);
            out[0] = .{
                .transition_id = 2,
                .donor_group_id = 12,
                .receiver_group_id = 11,
                .phase = .prepare,
            };
            return out;
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            for (records) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
            alloc.free(records);
        }

        pub fn observeSplitTransition(_: *@This(), transition_id: u64) !?transition_state.SplitObservation {
            _ = transition_id;
            return .{
                .status = .{
                    .phase = .prepare,
                    .source_split_phase = .prepare,
                    .bootstrapped = false,
                    .replay_required = false,
                    .replay_caught_up = false,
                    .cutover_ready = false,
                    .destination_ready_for_reads = false,
                    .source_delta_sequence = 0,
                    .dest_delta_sequence = 0,
                },
            };
        }

        pub fn observeMergeTransition(_: *@This(), transition_id: u64) !?transition_state.MergeObservation {
            return defaultMergeObservation(.{
                .transition_id = transition_id,
                .donor_group_id = 12,
                .receiver_group_id = 11,
                .phase = .prepare,
            });
        }
    };

    var state = MetadataState.init(std.testing.allocator);
    defer state.deinit();
    var fake = FakeService{};
    try state.syncProjected(&fake);
    var current = try state.captureCurrent(&fake);
    defer current.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), current.current.placement_intents.len);
    try std.testing.expectEqual(@as(usize, 1), current.current.split_transitions.len);
    try std.testing.expectEqual(@as(usize, 1), current.current.merge_transitions.len);
    try std.testing.expectEqual(@as(usize, 1), current.current.split_observations.len);
    try std.testing.expectEqual(@as(usize, 1), current.current.merge_observations.len);
    const tables = try state.projectedTableManager().listTables(std.testing.allocator);
    defer state.projectedTableManager().freeTables(std.testing.allocator, tables);
    const ranges = try state.projectedTableManager().listRanges(std.testing.allocator);
    defer state.projectedTableManager().freeRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
}

test "metadata state skips orphan projected ranges during projected sync" {
    const FakeService = struct {
        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            const out = try alloc.alloc(metadata_table_manager.TableRecord, 1);
            out[0] = .{
                .table_id = 7,
                .name = try alloc.dupe(u8, "docs"),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return out;
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try alloc.alloc(metadata_table_manager.RangeRecord, 2);
            out[0] = .{ .group_id = 71, .table_id = 7, .start_key = try alloc.dupe(u8, "doc:a"), .end_key = try alloc.dupe(u8, "doc:z") };
            out[1] = .{ .group_id = 99, .table_id = 99, .start_key = try alloc.dupe(u8, "ghost:a"), .end_key = try alloc.dupe(u8, "ghost:z") };
            return out;
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
            for (records) |record| {
                alloc.free(record.start_key);
                if (record.end_key) |end| alloc.free(end);
            }
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            return try alloc.alloc(transition_state.SplitTransitionRecord, 0);
        }

        pub fn freeProjectedSplitTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            return try alloc.alloc(transition_state.MergeTransitionRecord, 0);
        }

        pub fn freeProjectedMergeTransitions(_: *@This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            alloc.free(records);
        }
    };

    var state = MetadataState.init(std.testing.allocator);
    defer state.deinit();
    var fake = FakeService{};

    try state.syncProjected(&fake);

    const tables = try state.projectedTableManager().listTables(std.testing.allocator);
    defer state.projectedTableManager().freeTables(std.testing.allocator, tables);
    const ranges = try state.projectedTableManager().listRanges(std.testing.allocator);
    defer state.projectedTableManager().freeRanges(std.testing.allocator, ranges);

    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 7), ranges[0].table_id);
    try std.testing.expectEqual(@as(u64, 71), ranges[0].group_id);
}

test "metadata state prefers healthy stores before degraded stores" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{ .store_id = 1, .node_id = 1, .role = "data", .health_class = "degraded", .live = true, .capacity_bytes = 1024, .available_bytes = 900 },
        .{ .store_id = 2, .node_id = 2, .role = "data", .health_class = "healthy", .live = true, .capacity_bytes = 1024, .available_bytes = 700 },
        .{ .store_id = 3, .node_id = 3, .role = "data", .health_class = "healthy", .live = true, .capacity_bytes = 1024, .available_bytes = 650 },
        .{ .store_id = 4, .node_id = 4, .role = "data", .health_class = "draining", .live = true, .capacity_bytes = 1024, .available_bytes = 999 },
    };

    const candidates = try deriveStoreCandidateInfo(std.testing.allocator, &.{}, &stores);
    defer {
        for (candidates) |candidate| {
            std.testing.allocator.free(candidate.role);
            std.testing.allocator.free(candidate.failure_domain);
        }
        std.testing.allocator.free(candidates);
    }

    try std.testing.expectEqual(@as(usize, 3), candidates.len);
    try std.testing.expectEqual(@as(u64, 2), candidates[0].node_id);
    try std.testing.expectEqual(@as(u64, 3), candidates[1].node_id);
    try std.testing.expectEqual(@as(u64, 1), candidates[2].node_id);
    try std.testing.expect(candidates[0].retain_current);
    try std.testing.expect(candidates[2].retain_current);
}

test "metadata state prefers lower pressure healthy stores before higher capacity ones" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{ .store_id = 1, .node_id = 1, .role = "data", .health_class = "healthy", .live = true, .capacity_bytes = 1024, .available_bytes = 950, .lease_pressure = 95, .read_load = 200, .write_load = 120 },
        .{ .store_id = 2, .node_id = 2, .role = "data", .health_class = "healthy", .live = true, .capacity_bytes = 1024, .available_bytes = 800, .lease_pressure = 10, .read_load = 20, .write_load = 12 },
        .{ .store_id = 3, .node_id = 3, .role = "data", .health_class = "healthy", .live = true, .capacity_bytes = 1024, .available_bytes = 780, .lease_pressure = 12, .read_load = 24, .write_load = 15 },
    };

    const candidates = try deriveStoreCandidateInfo(std.testing.allocator, &.{}, &stores);
    defer {
        for (candidates) |candidate| {
            std.testing.allocator.free(candidate.role);
            std.testing.allocator.free(candidate.failure_domain);
        }
        std.testing.allocator.free(candidates);
    }

    try std.testing.expectEqual(@as(usize, 3), candidates.len);
    try std.testing.expectEqual(@as(u64, 2), candidates[0].node_id);
    try std.testing.expectEqual(@as(u64, 3), candidates[1].node_id);
    try std.testing.expectEqual(@as(u64, 1), candidates[2].node_id);
}

test "metadata state excludes stores on draining nodes" {
    const nodes = [_]metadata_table_manager.NodeRecord{
        .{ .node_id = 1, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_active },
        .{ .node_id = 2, .role = "data", .lifecycle = metadata_table_manager.node_lifecycle_draining },
    };
    const stores = [_]metadata_table_manager.StoreRecord{
        .{ .store_id = 1, .node_id = 1, .role = "data", .health_class = "healthy", .live = true },
        .{ .store_id = 2, .node_id = 2, .role = "data", .health_class = "healthy", .live = true },
    };

    const candidates = try deriveStoreCandidateInfo(std.testing.allocator, &nodes, &stores);
    defer {
        for (candidates) |candidate| {
            std.testing.allocator.free(candidate.role);
            std.testing.allocator.free(candidate.failure_domain);
        }
        std.testing.allocator.free(candidates);
    }

    try std.testing.expectEqual(@as(usize, 1), candidates.len);
    try std.testing.expectEqual(@as(u64, 1), candidates[0].node_id);
}

test "metadata state merges healthy group status and prefers leader readiness" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7001,
                    .doc_count = 30,
                    .disk_bytes = 300,
                    .empty = false,
                    .updated_at_millis = 100,
                    .local_leader = true,
                    .transition_pending = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .reads_ready_after_cutover = true,
                },
            })[0..]),
        },
        .{
            .store_id = 12,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7001,
                    .doc_count = 31,
                    .disk_bytes = 333,
                    .empty = false,
                    .updated_at_millis = 200,
                    .local_leader = false,
                    .transition_pending = false,
                    .replay_required = false,
                    .replay_caught_up = false,
                    .cutover_ready = false,
                    .reads_ready_after_cutover = false,
                },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &.{}, &.{}, &.{}, &stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expectEqual(@as(u64, 7001), merged[0].group_id);
    try std.testing.expectEqual(@as(u64, 31), merged[0].doc_count);
    try std.testing.expectEqual(@as(u64, 333), merged[0].disk_bytes);
    try std.testing.expectEqual(@as(u64, 200), merged[0].updated_at_millis);
    try std.testing.expect(merged[0].leader_known);
    try std.testing.expectEqual(@as(u64, 11), merged[0].leader_store_id);
    try std.testing.expect(merged[0].readiness_from_leader);
    try std.testing.expect(merged[0].transition_pending);
    try std.testing.expect(merged[0].replay_required);
    try std.testing.expect(merged[0].replay_caught_up);
    try std.testing.expect(merged[0].cutover_ready);
    try std.testing.expect(merged[0].reads_ready_after_cutover);
}

test "metadata state merges runtime document identity facts into group status" {
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 7021, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 11 },
    };
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .runtime_statuses = @constCast((&[_]metadata_table_manager.RuntimeGroupStatusReport{
                .{
                    .table_id = 70,
                    .table_name = "docs",
                    .group_id = 7021,
                    .doc_identity = .{
                        .namespace_table_id = 70,
                        .namespace_shard_id = 7021,
                        .namespace_range_id = 9001,
                        .next_ordinal = 12,
                        .allocated_ordinals = 11,
                        .live_ordinals = 10,
                        .tombstone_ordinals = 1,
                        .complete = true,
                    },
                },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &.{}, &placements, &.{}, &stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expectEqual(@as(u64, 7021), merged[0].group_id);
    try std.testing.expectEqual(@as(u64, 70), merged[0].doc_identity.namespace_table_id);
    try std.testing.expectEqual(@as(u64, 7021), merged[0].doc_identity.namespace_shard_id);
    try std.testing.expectEqual(@as(u64, 9001), merged[0].doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u32, 12), merged[0].doc_identity.next_ordinal);
    try std.testing.expectEqual(@as(u64, 11), merged[0].doc_identity.allocated_ordinals);
    try std.testing.expect(!merged[0].doc_identity_namespace_conflict);
    try std.testing.expectEqualStrings(metadata_reconciler.doc_identity_lifecycle_ready, merged[0].doc_identity_lifecycle);
}

test "metadata state marks doc identity rebuild required on range namespace mismatch" {
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 7031,
            .range_id = 9101,
            .table_id = 70,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 7032,
            .range_id = 9102,
            .table_id = 70,
            .start_key = "doc:m",
            .end_key = "doc:z",
        },
        .{
            .group_id = 7033,
            .range_id = 9103,
            .table_id = 70,
            .start_key = "doc:z",
            .end_key = null,
        },
    };
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 7031, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 11 },
        .{ .record = .{ .group_id = 7032, .replica_id = 2, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 11 },
        .{ .record = .{ .group_id = 7033, .replica_id = 3, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 11 },
    };
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .runtime_statuses = @constCast((&[_]metadata_table_manager.RuntimeGroupStatusReport{
                .{
                    .table_id = 70,
                    .table_name = "docs",
                    .group_id = 7031,
                    .doc_identity = .{
                        .namespace_table_id = 70,
                        .namespace_shard_id = 7031,
                        .namespace_range_id = 9101,
                        .next_ordinal = 12,
                        .allocated_ordinals = 11,
                    },
                },
                .{
                    .table_id = 70,
                    .table_name = "docs",
                    .group_id = 7032,
                    .doc_identity = .{
                        .namespace_table_id = 70,
                        .namespace_shard_id = 7032,
                        .namespace_range_id = 8001,
                        .next_ordinal = 8,
                        .allocated_ordinals = 7,
                    },
                },
                .{
                    .table_id = 70,
                    .table_name = "docs",
                    .group_id = 7033,
                    .doc_identity = .{
                        .namespace_table_id = 70,
                        .namespace_shard_id = 9999,
                        .namespace_range_id = 9999,
                        .complete = true,
                    },
                },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &ranges, &placements, &.{}, &stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 3), merged.len);
    const first = findMergedGroupStatus(merged, 7031).?;
    const second = findMergedGroupStatus(merged, 7032).?;
    const third = findMergedGroupStatus(merged, 7033).?;
    try std.testing.expect(!first.doc_identity.rebuild_required);
    try std.testing.expect(second.doc_identity.rebuild_required);
    try std.testing.expect(third.doc_identity.rebuild_required);
    try std.testing.expectEqualStrings(metadata_reconciler.doc_identity_lifecycle_rebuild_required, second.doc_identity_lifecycle);
    try std.testing.expectEqualStrings(metadata_reconciler.doc_identity_lifecycle_rebuild_required, third.doc_identity_lifecycle);

    const preserved_split_ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 7031,
            .range_id = 9101,
            .table_id = 70,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 7032,
            .range_id = 9102,
            .table_id = 70,
            .start_key = "doc:m",
            .end_key = "doc:z",
            .doc_identity_shard_id = 7031,
            .doc_identity_range_id = 9101,
        },
    };
    const preserved_split_stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .runtime_statuses = @constCast((&[_]metadata_table_manager.RuntimeGroupStatusReport{
                .{
                    .table_id = 70,
                    .table_name = "docs",
                    .group_id = 7032,
                    .doc_identity = .{
                        .namespace_table_id = 70,
                        .namespace_shard_id = 7031,
                        .namespace_range_id = 9101,
                        .next_ordinal = 8,
                        .allocated_ordinals = 7,
                    },
                },
            })[0..]),
        },
    };
    const preserved_split_merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &preserved_split_ranges, &placements, &.{}, &preserved_split_stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, preserved_split_merged);

    try std.testing.expectEqual(@as(usize, 1), preserved_split_merged.len);
    try std.testing.expect(!preserved_split_merged[0].doc_identity.rebuild_required);
    try std.testing.expectEqualStrings(metadata_reconciler.doc_identity_lifecycle_preserving, preserved_split_merged[0].doc_identity_lifecycle);
}

test "metadata state classifies mixed-version doc identity lifecycle reports" {
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{
            .group_id = 7041,
            .range_id = 9201,
            .table_id = 70,
            .start_key = "doc:a",
            .end_key = "doc:m",
        },
        .{
            .group_id = 7042,
            .range_id = 9202,
            .table_id = 70,
            .start_key = "doc:m",
            .end_key = "doc:t",
            .doc_identity_shard_id = 7041,
            .doc_identity_range_id = 9201,
        },
        .{
            .group_id = 7043,
            .range_id = 9203,
            .table_id = 70,
            .start_key = "doc:t",
            .end_key = null,
        },
    };
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 7041, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 11 },
        .{ .record = .{ .group_id = 7042, .replica_id = 2, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 11 },
        .{ .record = .{ .group_id = 7043, .replica_id = 3, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 11 },
    };
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7041,
                    .updated_at_millis = 10,
                    .local_voter = true,
                    .voter_count = 1,
                },
            })[0..]),
            .runtime_statuses = @constCast((&[_]metadata_table_manager.RuntimeGroupStatusReport{
                .{
                    .table_id = 70,
                    .table_name = "docs",
                    .group_id = 7042,
                    .doc_identity = .{
                        .namespace_table_id = 70,
                        .namespace_shard_id = 7041,
                        .namespace_range_id = 9201,
                    },
                },
                .{
                    .table_id = 70,
                    .table_name = "docs",
                    .group_id = 7043,
                    .doc_identity = .{
                        .namespace_table_id = 70,
                        .namespace_shard_id = 9999,
                        .namespace_range_id = 9999,
                        .complete = true,
                    },
                },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &ranges, &placements, &.{}, &stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 3), merged.len);
    const old_node = findMergedGroupStatus(merged, 7041).?;
    const preserved_split = findMergedGroupStatus(merged, 7042).?;
    const stale_namespace = findMergedGroupStatus(merged, 7043).?;

    try std.testing.expect(!old_node.doc_identity.rebuild_required);
    try std.testing.expectEqualStrings(metadata_reconciler.doc_identity_lifecycle_unknown, old_node.doc_identity_lifecycle);

    try std.testing.expect(!preserved_split.doc_identity.rebuild_required);
    try std.testing.expectEqualStrings(metadata_reconciler.doc_identity_lifecycle_preserving, preserved_split.doc_identity_lifecycle);

    try std.testing.expect(stale_namespace.doc_identity.rebuild_required);
    try std.testing.expectEqualStrings(metadata_reconciler.doc_identity_lifecycle_rebuild_required, stale_namespace.doc_identity_lifecycle);
}

test "metadata state prefers leader-qualified transition observation over follower heartbeat readiness" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 21,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7102,
                    .doc_count = 20,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = 300,
                    .local_leader = false,
                    .transition_pending = false,
                    .replay_required = false,
                    .replay_caught_up = false,
                    .cutover_ready = false,
                    .reads_ready_after_cutover = false,
                },
            })[0..]),
        },
    };

    const split_transitions = [_]transition_state.SplitTransitionRecord{
        .{
            .transition_id = 9101,
            .source_group_id = 7101,
            .destination_group_id = 7102,
            .phase = .bootstrap_peer,
        },
    };
    const split_observations = [_]metadata_reconciler.SplitRuntimeObservation{
        .{
            .transition_id = 9101,
            .observation = .{
                .status = .{
                    .phase = .cutover_ready,
                    .source_split_phase = .splitting,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 4,
                    .dest_delta_sequence = 4,
                },
                .destination_local_leader = true,
            },
        },
    };

    const merged = try mergeHealthyGroupStatuses(
        std.testing.allocator,
        &.{},
        &.{},
        &.{},
        &.{},
        &stores,
        &split_transitions,
        &.{},
        &split_observations,
        &.{},
    );
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expect(merged[0].readiness_from_leader);
    try std.testing.expect(merged[0].transition_pending);
    try std.testing.expect(merged[0].replay_required);
    try std.testing.expect(merged[0].replay_caught_up);
    try std.testing.expect(merged[0].cutover_ready);
    try std.testing.expect(merged[0].reads_ready_after_cutover);
}

test "metadata state carries merge doc identity reassignment observation" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 22,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7104,
                    .doc_count = 20,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = 300,
                    .local_leader = false,
                },
            })[0..]),
        },
    };

    const merge_transitions = [_]transition_state.MergeTransitionRecord{
        .{
            .transition_id = 9102,
            .donor_group_id = 7103,
            .receiver_group_id = 7104,
            .phase = .replay_deltas,
            .allow_doc_identity_reassignment = true,
        },
    };
    const receiver_status: data.MergeTransitionStatus = .{
        .phase = .cutover_ready,
        .donor_group_id = 7103,
        .receiver_group_id = 7104,
        .receiver_accepts_donor_range = true,
        .bootstrapped = true,
        .replay_required = true,
        .replay_caught_up = true,
        .cutover_ready = true,
        .receiver_ready_for_reads = true,
        .donor_delta_sequence = 4,
        .receiver_delta_sequence = 4,
        .allow_doc_identity_reassignment = true,
    };
    const merge_observations = [_]metadata_reconciler.MergeRuntimeObservation{
        .{
            .transition_id = 9102,
            .observation = .{
                .donor = receiver_status,
                .receiver = receiver_status,
                .receiver_local_leader = true,
            },
        },
    };

    const merged = try mergeHealthyGroupStatuses(
        std.testing.allocator,
        &.{},
        &.{},
        &.{},
        &.{},
        &stores,
        &.{},
        &merge_transitions,
        &.{},
        &merge_observations,
    );
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expect(merged[0].readiness_from_leader);
    try std.testing.expect(merged[0].doc_identity_reassignment_active);
    try std.testing.expectEqualStrings(metadata_reconciler.doc_identity_lifecycle_reassigning, merged[0].doc_identity_lifecycle);
}

test "metadata state conservatively aggregates readiness across healthy peers without leader truth" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 24,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7103,
                    .doc_count = 20,
                    .disk_bytes = 200,
                    .empty = false,
                    .updated_at_millis = 100,
                    .local_leader = false,
                    .transition_pending = true,
                    .replay_required = true,
                    .replay_caught_up = false,
                    .cutover_ready = false,
                    .reads_ready_after_cutover = false,
                },
            })[0..]),
        },
        .{
            .store_id = 25,
            .node_id = 3,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7103,
                    .doc_count = 21,
                    .disk_bytes = 210,
                    .empty = false,
                    .updated_at_millis = 300,
                    .local_leader = false,
                    .transition_pending = false,
                    .replay_required = false,
                    .replay_caught_up = false,
                    .cutover_ready = false,
                    .reads_ready_after_cutover = false,
                },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &.{}, &.{}, &.{}, &stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expectEqual(@as(u64, 300), merged[0].updated_at_millis);
    try std.testing.expect(!merged[0].leader_known);
    try std.testing.expect(!merged[0].readiness_from_leader);
    try std.testing.expect(merged[0].transition_pending);
    try std.testing.expect(merged[0].replay_required);
    try std.testing.expect(!merged[0].replay_caught_up);
    try std.testing.expect(!merged[0].cutover_ready);
    try std.testing.expect(!merged[0].reads_ready_after_cutover);
}

test "metadata state tracks authoritative voter count when healthy peers agree" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 26,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7104,
                    .updated_at_millis = 100,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
        .{
            .store_id = 27,
            .node_id = 3,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7104,
                    .updated_at_millis = 101,
                    .local_voter = true,
                    .voter_count = 2,
                },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &.{}, &.{}, &.{}, &stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expect(merged[0].voter_count_known);
    try std.testing.expectEqual(@as(u16, 2), merged[0].voter_count);
    try std.testing.expectEqual(@as(u16, 2), merged[0].healthy_voter_reports);
}

test "metadata state marks restore-pending groups as not yet ready" {
    const tables = [_]metadata_table_manager.TableRecord{
        .{
            .table_id = 88,
            .name = "docs",
            .placement_role = "data",
            .restore_backup_id = "snap1",
            .restore_location = "file:///tmp/backups",
        },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 8801, .table_id = 88, .start_key = "", .end_key = null },
    };
    const placement_intents = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 8801, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .peer_node_ids = &.{2} },
        .{ .record = .{ .group_id = 8801, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .peer_node_ids = &.{1} },
    };
    const restore_progresses = [_]metadata_table_manager.RestoreProgressRecord{
        .{ .table_id = 88, .node_id = 1, .group_id = 8801, .backup_id = "snap1" },
    };
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 11,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{ .group_id = 8801, .updated_at_millis = 10, .local_leader = true, .local_voter = true, .voter_count = 2 },
            })[0..]),
        },
        .{
            .store_id = 12,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{ .group_id = 8801, .updated_at_millis = 9, .local_voter = true, .voter_count = 2 },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(
        std.testing.allocator,
        &tables,
        &ranges,
        &placement_intents,
        &restore_progresses,
        &stores,
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expect(merged[0].restore_pending);
}

test "metadata state prefers freshest leader report over stale conflicting leader report" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 31,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7201,
                    .doc_count = 40,
                    .disk_bytes = 400,
                    .empty = false,
                    .updated_at_millis = 100,
                    .local_leader = true,
                    .transition_pending = true,
                    .replay_required = true,
                },
            })[0..]),
        },
        .{
            .store_id = 32,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7201,
                    .doc_count = 41,
                    .disk_bytes = 410,
                    .empty = false,
                    .updated_at_millis = 200,
                    .local_leader = true,
                    .transition_pending = false,
                    .replay_required = false,
                    .replay_caught_up = false,
                    .cutover_ready = false,
                    .reads_ready_after_cutover = false,
                },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &.{}, &.{}, &.{}, &stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expect(merged[0].leader_known);
    try std.testing.expectEqual(@as(u64, 32), merged[0].leader_store_id);
    try std.testing.expectEqual(@as(u64, 200), merged[0].updated_at_millis);
    try std.testing.expect(!merged[0].transition_pending);
    try std.testing.expect(!merged[0].replay_required);
}

test "metadata state keeps leader unknown when freshest leader reports conflict" {
    const stores = [_]metadata_table_manager.StoreRecord{
        .{
            .store_id = 41,
            .node_id = 1,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7301,
                    .doc_count = 50,
                    .disk_bytes = 500,
                    .empty = false,
                    .updated_at_millis = 300,
                    .local_leader = true,
                },
            })[0..]),
        },
        .{
            .store_id = 42,
            .node_id = 2,
            .role = "data",
            .health_class = "healthy",
            .live = true,
            .group_statuses = @constCast((&[_]metadata_table_manager.GroupStatusReport{
                .{
                    .group_id = 7301,
                    .doc_count = 51,
                    .disk_bytes = 510,
                    .empty = false,
                    .updated_at_millis = 300,
                    .local_leader = true,
                },
            })[0..]),
        },
    };

    const merged = try mergeHealthyGroupStatuses(std.testing.allocator, &.{}, &.{}, &.{}, &.{}, &stores, &.{}, &.{}, &.{}, &.{});
    defer freeMergedGroupStatuses(std.testing.allocator, merged);

    try std.testing.expectEqual(@as(usize, 1), merged.len);
    try std.testing.expect(!merged[0].leader_known);
    try std.testing.expectEqual(@as(u64, 0), merged[0].leader_store_id);
    try std.testing.expect(!merged[0].readiness_from_leader);
}
