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
const platform_time = @import("antfly_platform").time;
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;
const metadata_state = @import("state.zig");
const metadata_reconciler = @import("reconciler.zig");
const extension_domain = @import("../extensions/mod.zig");
const table_manager = @import("table_manager.zig");
const raft_host = @import("../raft/host.zig");
const raft_reconciler = @import("../raft/reconciler.zig");
const raft_service = @import("../raft/service.zig");
const transition_state = @import("transition_state.zig");
const metadata_incarnation = @import("incarnation.zig");
const reallocation_request = @import("reallocation_request.zig");

pub const MetadataClusterIncarnation = metadata_incarnation.MetadataClusterIncarnation;
pub const MetadataRaftVoterSetFingerprint = [table_manager.voter_set_fingerprint_len * 2]u8;

/// Allocation-free subset of `/status` used by rolling-upgrade admission
/// probes. Keeping this separate from MetadataStatus avoids parsing and
/// retaining unrelated status strings on every table DDL operation.
pub const TableTopologyProtocolStatus = struct {
    metadata_group_id: u64,
    table_topology_protocol_version: u16 = 0,
    metadata_incarnation: ?MetadataClusterIncarnation = null,
    metadata_raft_local_node_id: u64 = 0,
};

pub const MetadataStatus = struct {
    metadata_group_id: u64,
    /// Zero means the peer predates the causal reallocation barrier.
    reallocation_barrier_protocol_version: u16 = 0,
    /// Zero means the peer cannot decode atomic table-topology transitions.
    table_topology_protocol_version: u16 = 0,
    /// Maximum embedded runtime-status record version this replica can apply;
    /// zero means the peer predates rolling-safe format negotiation.
    runtime_status_record_version: u16 = 0,
    /// Highest runtime-status record version durably activated for this
    /// metadata incarnation. Zero means activation has not committed yet.
    runtime_status_protocol_activated_version: u16 = 0,
    /// Highest version the current metadata membership can safely commit now.
    /// This may lead activation by one command and can be lower than this
    /// process's current codec during a rolling upgrade.
    runtime_status_protocol_ready_version: u16 = 0,
    /// Whether this replica currently has one capability probe in flight.
    runtime_status_protocol_probe_in_flight: bool = false,
    /// Consecutive failed probes since the last successful probe or durable
    /// activation. This is rate-limited by exponential backoff.
    runtime_status_protocol_probe_failures: u32 = 0,
    metadata_incarnation: ?MetadataClusterIncarnation = null,
    metadata_epoch: u64 = 0,
    metadata_raft_local_node_id: u64 = 0,
    metadata_raft_role: []const u8 = "absent",
    metadata_raft_leader_id: ?u64 = null,
    metadata_raft_term: u64 = 0,
    metadata_raft_commit_index: u64 = 0,
    metadata_raft_local_voter: bool = false,
    metadata_raft_voter_count: usize = 0,
    metadata_raft_voter_set_fingerprint: ?MetadataRaftVoterSetFingerprint = null,
    metadata_raft_joint_consensus: bool = false,
    /// Includes both stable and staged learners (`learners_next`).
    metadata_raft_learner_count: usize = 0,
    metadata_raft_election_elapsed: u32 = 0,
    metadata_raft_randomized_election_timeout: u32 = 0,
    metadata_raft_votes_granted: usize = 0,
    metadata_raft_votes_rejected: usize = 0,
    metadata_raft_votes_unknown: usize = 0,
    metadata_raft_inbound_message_enqueues: usize = 0,
    metadata_raft_inbound_message_drains: usize = 0,
    metadata_raft_pending_inbound_messages: usize = 0,
    metadata_raft_transport_sent_frames: usize = 0,
    metadata_raft_transport_send_failures: usize = 0,
    metadata_raft_transport_retries_scheduled: usize = 0,
    metadata_raft_transport_retries_exhausted: usize = 0,
    metadata_raft_transport_retried_successes: usize = 0,
    metadata_raft_transport_peer_refreshes: usize = 0,
    metadata_raft_transport_peer_routes: usize = 0,
    metadata_raft_transport_served_groups: usize = 0,
    metadata_raft_transport_pending_retries: usize = 0,
    metrics: raft_service.ManagedServiceMetrics,
    reconcile_lease_enabled: bool = false,
    reconcile_lease_owner_node_id: u64 = 0,
    reconcile_lease_expires_at_ms: u64 = 0,
    reconcile_lease_held_by_local: bool = false,
    reconcile_lease_acquisition_count: u64 = 0,
    reconcile_lease_acquire_failures: u64 = 0,
    reconcile_lease_lost_leases: u64 = 0,
    reconcile_lease_last_acquired_ms: u64 = 0,
    projected_tables: usize = 0,
    projected_tables_with_replication_sources: usize = 0,
    projected_replication_sources: usize = 0,
    projected_replication_source_statuses: usize = 0,
    projected_replication_source_statuses_exact_cutover: usize = 0,
    projected_replication_source_statuses_non_exact_cutover: usize = 0,
    projected_replication_source_statuses_reseed_recommended: usize = 0,
    projected_replication_source_statuses_exported_snapshot: usize = 0,
    projected_replication_source_statuses_slot_first: usize = 0,
    projected_replication_source_statuses_slot_resumed: usize = 0,
    projected_replication_source_statuses_snapshot: usize = 0,
    projected_replication_source_statuses_cutover_prepared: usize = 0,
    projected_replication_source_statuses_streaming: usize = 0,
    projected_replication_source_statuses_failed: usize = 0,
    projected_replication_source_statuses_with_last_error: usize = 0,
    projected_replication_source_statuses_slot_missing_failed: usize = 0,
    projected_replication_source_statuses_retryable_failed: usize = 0,
    projected_replication_source_statuses_terminal_failed: usize = 0,
    projected_replication_source_statuses_with_consecutive_failures: usize = 0,
    projected_replication_source_statuses_with_success_timestamp: usize = 0,
    projected_replication_source_statuses_with_change_timestamp: usize = 0,
    projected_replication_source_consecutive_failures_total: u64 = 0,
    projected_replication_source_consecutive_failures_max: u64 = 0,
    projected_replication_source_lag_records_total: u64 = 0,
    projected_replication_source_lag_records_max: u64 = 0,
    projected_replication_source_lag_millis_total: u64 = 0,
    projected_replication_source_lag_millis_max: u64 = 0,
    projected_replication_source_observed_lag_millis_total: u64 = 0,
    projected_replication_source_observed_lag_millis_max: u64 = 0,
    projected_replication_source_statuses_with_source_commit_timestamp: usize = 0,
    projected_replication_source_last_success_at_ms_max: u64 = 0,
    projected_replication_source_last_source_commit_at_ms_max: u64 = 0,
    projected_replication_source_last_change_applied_at_ms_max: u64 = 0,
    projected_extension_packages: usize = 0,
    projected_installed_extensions: usize = 0,
    projected_extension_members: usize = 0,
    projected_extension_dependencies: usize = 0,
    projected_ranges: usize = 0,
    projected_stores: usize = 0,
    projected_placement_intents: usize = 0,
    projected_snapshot_bootstrap_intents: usize = 0,
    projected_backup_restore_bootstrap_intents: usize = 0,
    projected_shuffle_join_leases: usize = 0,
    projected_restore_progress: usize = 0,
    projected_split_transitions: usize = 0,
    projected_merge_transitions: usize = 0,
    projected_doc_identity_lifecycle_unknown: usize = 0,
    projected_doc_identity_lifecycle_preserving: usize = 0,
    projected_doc_identity_lifecycle_reassigning: usize = 0,
    projected_doc_identity_lifecycle_rebuild_required: usize = 0,
    projected_doc_identity_lifecycle_ready: usize = 0,
    preferred_stores: usize = 0,
    constrained_stores: usize = 0,
    overloaded_stores: usize = 0,
    excluded_stores: usize = 0,
    backfill_stores: usize = 0,
    active_backfills: usize = 0,
    placement_upserts: usize = 0,
    placement_removals: usize = 0,
    repair_placement_groups: usize = 0,
    rebalance_placement_groups: usize = 0,
};

pub const MetadataHead = struct {
    metadata_group_id: u64,
    metadata_incarnation: ?MetadataClusterIncarnation = null,
    metadata_epoch: u64 = 0,
};

pub const CatalogIdentity = struct {
    metadata_group_id: u64,
    metadata_incarnation: MetadataClusterIncarnation,
};

pub const catalog_routing_protocol_current: u16 = 2;

/// Wire capabilities are negotiated before a data node chooses its routing
/// adapter. Absence of this document identifies the N-1 legacy protocol.
pub const MetadataCapabilities = struct {
    catalog_routing_protocol_min: u16 = catalog_routing_protocol_current,
    catalog_routing_protocol_max: u16 = catalog_routing_protocol_current,

    pub fn supportsCatalogRouting(self: @This(), version: u16) bool {
        return version >= self.catalog_routing_protocol_min and
            version <= self.catalog_routing_protocol_max;
    }
};

/// Compact, constant-size runtime topology consumed by safety monitors.
/// Keep this separate from MetadataStatus so frequent topology probes never
/// walk or clone the projected catalog.
pub const MetadataRuntimeTopology = struct {
    metadata_group_id: u64,
    metadata_incarnation: ?MetadataClusterIncarnation = null,
    metadata_raft_local_node_id: u64 = 0,
    metadata_raft_role: []const u8 = "absent",
    metadata_raft_leader_id: ?u64 = null,
    metadata_raft_term: u64 = 0,
    metadata_raft_local_voter: bool = false,
    metadata_raft_voter_count: usize = 0,
    metadata_raft_voter_set_fingerprint: ?MetadataRaftVoterSetFingerprint = null,
    metadata_raft_joint_consensus: bool = false,
    /// Includes both stable and staged learners (`learners_next`).
    metadata_raft_learner_count: usize = 0,
};

pub const ReplicationSourceActionHint = struct {
    table_id: u64,
    table_name: []u8,
    source_ordinal: u32,
    action: []const u8,
    reason: []const u8,
    reseed_exact_cutover_path: []u8,
};

pub const AdminSnapshot = struct {
    status: MetadataStatus,
    reallocation_request: ?reallocation_request.ReallocationRequestRecord = null,
    tables: []table_manager.TableRecord,
    ranges: []table_manager.RangeRecord,
    nodes: []table_manager.NodeRecord = &.{},
    stores: []table_manager.StoreRecord,
    placement_intents: []raft_reconciler.PlacementIntent,
    shuffle_join_leases: []table_manager.ShuffleJoinLeaseRecord = &.{},
    local_bootstrap_statuses: []raft_host.BootstrapStatus = &.{},
    restore_progresses: []table_manager.RestoreProgressRecord = &.{},
    replication_source_statuses: []table_manager.ReplicationSourceStatusRecord = &.{},
    replication_source_action_hints: []ReplicationSourceActionHint = &.{},
    extension_packages: []extension_domain.PackageManifest = &.{},
    installed_extensions: []extension_domain.InstalledExtension = &.{},
    extension_members: []extension_domain.ExtensionMember = &.{},
    extension_dependencies: []extension_domain.ExtensionDependency = &.{},
    split_transitions: []transition_state.SplitTransitionRecord,
    merge_transitions: []transition_state.MergeTransitionRecord,
    split_observations: []transition_state.SplitObservationRecord = &.{},
    merge_observations: []transition_state.MergeObservationRecord = &.{},
    merged_group_statuses: []metadata_reconciler.MergedGroupStatus = &.{},
};

/// Atomic projected catalog view used only for table-to-group routing. It
/// includes the authority and range identity needed to consume a route, while
/// excluding operational and reconciliation status so it cannot accidentally
/// become an administrative snapshot.
pub const CatalogRoutingSnapshot = struct {
    /// Authority and revision are captured with the projection. Remote callers
    /// must validate the authority before using any route from this snapshot.
    metadata_group_id: u64 = 0,
    metadata_incarnation: ?MetadataClusterIncarnation = null,
    catalog_revision: u64 = 0,
    /// Durable authority-scoped publication observation used to wait for a
    /// newer routing projection. It is safe to compare across replicas and
    /// process restarts.
    change_token: CatalogRoutingChangeToken = .{},
    tables: []table_manager.TableRecord,
    ranges: []table_manager.RangeRecord,
};

pub const CatalogRoutingChangeToken = struct {
    metadata_group_id: u64 = 0,
    metadata_incarnation: ?MetadataClusterIncarnation = null,
    revision: u64 = 0,

    pub fn fromSnapshot(snapshot: CatalogRoutingSnapshot) @This() {
        return .{
            .metadata_group_id = snapshot.metadata_group_id,
            .metadata_incarnation = snapshot.metadata_incarnation,
            .revision = snapshot.catalog_revision,
        };
    }

    pub fn authorityEql(a: @This(), b: @This()) bool {
        return a.metadata_group_id == b.metadata_group_id and
            std.meta.eql(a.metadata_incarnation, b.metadata_incarnation);
    }

    pub fn eql(a: @This(), b: @This()) bool {
        return a.authorityEql(b) and a.revision == b.revision;
    }
};

pub const CatalogRoutingChangeRequest = struct {
    observed_token: CatalogRoutingChangeToken,
    /// Terminal absence is valid only when the server completes a
    /// linearizable confirmation after its bounded watch. Short failover
    /// probes leave this false and treat an unchanged replica as retryable.
    confirm_absence: bool = false,
};

pub const CatalogRoutingChangeResult = struct {
    token: CatalogRoutingChangeToken,
    disposition: ?Disposition = null,
    /// Retained while old and new metadata/data nodes may coexist.
    changed: bool = false,

    pub const Disposition = enum {
        advanced,
        unchanged,
        authority_changed,
        replica_behind,
    };

    pub fn effectiveDisposition(self: @This()) Disposition {
        return self.disposition orelse if (self.changed) .advanced else .unchanged;
    }
};

pub const CatalogRouteSelector = enum {
    table,
    all_ranges,
    key,
    span,
    group,
};

pub const CatalogRouteQuery = struct {
    table_name: []const u8,
    selector: CatalogRouteSelector,
    key: []const u8 = "",
    from_key: []const u8 = "",
    to_key: []const u8 = "",
    group_id: u64 = 0,
};

pub const CatalogIdentityNamespace = struct {
    table_id: u64,
    shard_id: u64,
    range_id: u64,
};

pub const CatalogGroupRoute = struct {
    group_id: u64,
    range_id: u64,
    identity_namespace: CatalogIdentityNamespace,
};

pub const catalog_route_fence_protocol_current: u16 = 1;
pub const catalog_route_fence_header = "X-Antfly-Catalog-Route-Fence";
pub const catalog_route_fence_ack_header = "X-Antfly-Catalog-Route-Fence-Ack";
pub const catalog_route_fence_ack_value = "1";
pub const catalog_route_deadline_ms_header = "X-Antfly-Catalog-Route-Deadline-Ms";
pub const catalog_route_default_deadline_ms: u32 = 5_000;
pub const catalog_route_max_deadline_ms: u32 = 30_000;

/// Immutable authority and identity carried with every first-party
/// group-local read. The receiver validates this against its compact routing
/// projection before opening storage, so an independently cached admin
/// snapshot can never select a different table generation.
pub const CatalogRouteFence = struct {
    protocol: u16 = catalog_route_fence_protocol_current,
    metadata_group_id: u64,
    metadata_incarnation: ?MetadataClusterIncarnation = null,
    catalog_revision: u64,
    table_id: u64,
    topology_epoch: u64,
    route: CatalogGroupRoute,
    /// Receiver-local admission context. These fields are intentionally
    /// excluded from the wire representation: monotonic clocks and borrowed
    /// cancellation callbacks are process-local capabilities.
    admission_deadline_ns: ?u64 = null,
    admission_cancellation: CancellationToken = .none,

    const Wire = struct {
        protocol: u16 = catalog_route_fence_protocol_current,
        metadata_group_id: u64,
        metadata_incarnation: ?MetadataClusterIncarnation = null,
        catalog_revision: u64,
        table_id: u64,
        topology_epoch: u64,
        route: CatalogGroupRoute,
    };

    fn wire(self: @This()) Wire {
        return .{
            .protocol = self.protocol,
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .catalog_revision = self.catalog_revision,
            .table_id = self.table_id,
            .topology_epoch = self.topology_epoch,
            .route = self.route,
        };
    }

    pub fn jsonStringify(self: @This(), jw: anytype) !void {
        try jw.write(self.wire());
    }

    pub fn jsonParse(alloc: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !@This() {
        const value = try std.json.innerParse(Wire, alloc, source, options);
        return .{
            .protocol = value.protocol,
            .metadata_group_id = value.metadata_group_id,
            .metadata_incarnation = value.metadata_incarnation,
            .catalog_revision = value.catalog_revision,
            .table_id = value.table_id,
            .topology_epoch = value.topology_epoch,
            .route = value.route,
        };
    }

    pub fn validate(self: @This()) !void {
        if (self.protocol != catalog_route_fence_protocol_current) return error.UnsupportedCatalogRouteFence;
        if (self.metadata_group_id == 0 or self.table_id == 0 or self.route.group_id == 0) return error.InvalidCatalogRouteFence;
        if (self.route.identity_namespace.table_id != self.table_id) return error.InvalidCatalogRouteFence;
    }
};

pub const CatalogRoutePlan = struct {
    metadata_group_id: u64,
    metadata_incarnation: ?MetadataClusterIncarnation,
    catalog_revision: u64,
    table_id: u64,
    topology_epoch: u64,
    groups: []CatalogGroupRoute,

    pub fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        alloc.free(self.groups);
        self.* = undefined;
    }
};

pub const CatalogRouteResolveRequest = struct {
    query: CatalogRouteQuery,
};

pub const CatalogRouteResolveResult = struct {
    disposition: Disposition,
    token: CatalogRoutingChangeToken = .{},
    plan: ?CatalogRoutePlan = null,

    pub const Disposition = enum {
        found,
        not_found,
        timed_out,
        authority_changed,
    };
};

/// Compact catalog values consumed while staging one data-Raft generation.
/// Metadata replicas compare this after a linearizable read so the network and
/// allocation cost is independent of the cluster-wide catalog size.
pub const CatalogPublicationContract = struct {
    metadata_group_id: u64,
    metadata_incarnation: MetadataClusterIncarnation,
    table_id: u64,
    table_name: []const u8,
    schema_json: []const u8,
    indexes_json: []const u8,
    range: table_manager.RangeRecord,

    pub fn matches(self: @This(), snapshot: *const AdminSnapshot) bool {
        return self.matchesProjection(
            snapshot.status.metadata_group_id,
            snapshot.status.metadata_incarnation,
            snapshot.tables,
            snapshot.ranges,
        );
    }

    pub fn matchesProjection(
        self: @This(),
        metadata_group_id: u64,
        incarnation_value: ?MetadataClusterIncarnation,
        tables: []const table_manager.TableRecord,
        ranges: []const table_manager.RangeRecord,
    ) bool {
        if (metadata_group_id != self.metadata_group_id) return false;
        const incarnation = incarnation_value orelse return false;
        if (!std.mem.eql(u8, &incarnation, &self.metadata_incarnation)) return false;
        var table_match = false;
        for (tables) |table| {
            if (table.table_id != self.table_id) continue;
            table_match = std.mem.eql(u8, self.table_name, table.name) and
                std.mem.eql(u8, self.schema_json, table.schema_json) and
                std.mem.eql(u8, self.indexes_json, table.indexes_json);
            break;
        }
        if (!table_match) return false;
        for (ranges) |range| {
            if (range.group_id != self.range.group_id) continue;
            return range.table_id == self.table_id and table_manager.rangeRecordsEqual(self.range, range);
        }
        return false;
    }
};

/// Exact authorization request for deleting local storage paths left behind by
/// a committed table drop. Group ownership is the destructive-action fence;
/// table-name presence is returned separately so a replacement incarnation can
/// coexist while cleanup reclaims only the old groups.
pub const CatalogGroupRetirementContract = struct {
    metadata_group_id: u64,
    metadata_incarnation: MetadataClusterIncarnation,
    table_name: []const u8,
    group_ids: []const u64,
};

pub const CatalogGroupRetirementValidation = struct {
    group_ids_unowned: bool,
    table_name_absent: bool,
};

pub const CatalogTableTopology = struct {
    range_count: u64,
    digest: [std.crypto.hash.sha2.Sha256.digest_length]u8,
};

/// Non-owning indexes over a stable catalog projection. Building the index is
/// linear in the projection size; publication checks are constant time.
pub const CatalogProjectionIndex = struct {
    table_indexes: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    table_name_indexes: std.StringHashMapUnmanaged(usize) = .empty,
    range_indexes: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    table_topologies: std.AutoHashMapUnmanaged(u64, CatalogTableTopology) = .empty,

    pub fn init(
        alloc: std.mem.Allocator,
        tables: []const table_manager.TableRecord,
        ranges: []const table_manager.RangeRecord,
    ) !CatalogProjectionIndex {
        return try initUntil(alloc, tables, ranges, null);
    }

    pub fn initUntil(
        alloc: std.mem.Allocator,
        tables: []const table_manager.TableRecord,
        ranges: []const table_manager.RangeRecord,
        deadline_ns: ?u64,
    ) !CatalogProjectionIndex {
        try catalogProjectionCheckpoint(deadline_ns, 0);
        var self: CatalogProjectionIndex = .{};
        errdefer self.deinit(alloc);
        try self.table_indexes.ensureTotalCapacity(alloc, @intCast(tables.len));
        try self.table_name_indexes.ensureTotalCapacity(alloc, @intCast(tables.len));
        try self.range_indexes.ensureTotalCapacity(alloc, @intCast(ranges.len));
        try self.table_topologies.ensureTotalCapacity(alloc, @intCast(tables.len));

        for (tables, 0..) |table, index| {
            try catalogProjectionCheckpoint(deadline_ns, index);
            if (self.table_indexes.contains(table.table_id)) return error.InvalidCatalogProjection;
            self.table_indexes.putAssumeCapacity(table.table_id, index);
            self.table_name_indexes.putAssumeCapacity(table.name, index);
            self.table_topologies.putAssumeCapacity(table.table_id, .{
                .range_count = 0,
                .digest = [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length,
            });
        }
        for (ranges, 0..) |range, index| {
            try catalogProjectionCheckpoint(deadline_ns, index);
            if (self.range_indexes.contains(range.group_id)) return error.InvalidCatalogProjection;
            self.range_indexes.putAssumeCapacity(range.group_id, index);
            // Partial indexes intentionally ignore ranges for tables outside
            // the supplied projection; the full reader validates ownership.
            const topology = self.table_topologies.getPtr(range.table_id) orelse continue;
            topology.range_count += 1;
            addCatalogTopologyDigest(&topology.digest, catalogRangeTopologyDigest(range));
        }
        var iterator = self.table_topologies.iterator();
        var topology_index: usize = 0;
        while (iterator.next()) |entry| {
            try catalogProjectionCheckpoint(deadline_ns, topology_index);
            entry.value_ptr.digest = finalizeCatalogTableTopology(
                entry.key_ptr.*,
                entry.value_ptr.range_count,
                entry.value_ptr.digest,
            );
            topology_index += 1;
        }
        try catalogProjectionDeadline(deadline_ns);
        return self;
    }

    pub fn deinit(self: *CatalogProjectionIndex, alloc: std.mem.Allocator) void {
        self.table_indexes.deinit(alloc);
        self.table_name_indexes.deinit(alloc);
        self.range_indexes.deinit(alloc);
        self.table_topologies.deinit(alloc);
        self.* = .{};
    }

    pub fn matchesPublication(
        self: *const CatalogProjectionIndex,
        contract: CatalogPublicationContract,
        metadata_group_id: u64,
        incarnation_value: ?MetadataClusterIncarnation,
        tables: []const table_manager.TableRecord,
        ranges: []const table_manager.RangeRecord,
    ) bool {
        if (!catalogIdentityMatches(contract.metadata_group_id, contract.metadata_incarnation, metadata_group_id, incarnation_value)) return false;
        const table_index = self.table_indexes.get(contract.table_id) orelse return false;
        const table = &tables[table_index];
        if (!catalogTableDefinitionMatches(contract.table_name, contract.schema_json, contract.indexes_json, table)) return false;
        const range_index = self.range_indexes.get(contract.range.group_id) orelse return false;
        const range = &ranges[range_index];
        return range.table_id == contract.table_id and table_manager.rangeRecordsEqual(contract.range, range.*);
    }

    pub fn matchesTablePublication(
        self: *const CatalogProjectionIndex,
        contract: CatalogTablePublicationContract,
        metadata_group_id: u64,
        incarnation_value: ?MetadataClusterIncarnation,
        tables: []const table_manager.TableRecord,
    ) bool {
        if (!catalogIdentityMatches(contract.metadata_group_id, contract.metadata_incarnation, metadata_group_id, incarnation_value)) return false;
        const table_index = self.table_indexes.get(contract.table_id) orelse return false;
        if (!catalogTableDefinitionMatches(contract.table_name, contract.schema_json, contract.indexes_json, &tables[table_index])) return false;
        const topology = self.table_topologies.get(contract.table_id) orelse return false;
        return topology.range_count == contract.topology.range_count and
            std.crypto.timing_safe.eql(@TypeOf(topology.digest), topology.digest, contract.topology.digest);
    }

    pub fn validateGroupRetirement(
        self: *const CatalogProjectionIndex,
        contract: CatalogGroupRetirementContract,
        metadata_group_id: u64,
        incarnation_value: ?MetadataClusterIncarnation,
    ) CatalogGroupRetirementValidation {
        if (!catalogIdentityMatches(
            contract.metadata_group_id,
            contract.metadata_incarnation,
            metadata_group_id,
            incarnation_value,
        )) return .{ .group_ids_unowned = false, .table_name_absent = false };

        for (contract.group_ids) |group_id| {
            if (self.range_indexes.contains(group_id)) {
                return .{
                    .group_ids_unowned = false,
                    .table_name_absent = !self.table_name_indexes.contains(contract.table_name),
                };
            }
        }
        return .{
            .group_ids_unowned = true,
            .table_name_absent = !self.table_name_indexes.contains(contract.table_name),
        };
    }
};

fn catalogProjectionCheckpoint(deadline_ns: ?u64, index: usize) !void {
    if (index % 64 == 0) try catalogProjectionDeadline(deadline_ns);
}

fn catalogProjectionDeadline(deadline_ns: ?u64) !void {
    if (deadline_ns) |deadline| {
        if (platform_time.monotonicNs() >= deadline) return error.CatalogRoutingSnapshotTimeout;
    }
}

fn catalogIdentityMatches(
    expected_group_id: u64,
    expected_incarnation: MetadataClusterIncarnation,
    actual_group_id: u64,
    actual_incarnation_value: ?MetadataClusterIncarnation,
) bool {
    if (actual_group_id != expected_group_id) return false;
    const actual_incarnation = actual_incarnation_value orelse return false;
    return std.mem.eql(u8, &actual_incarnation, &expected_incarnation);
}

fn catalogTableDefinitionMatches(
    expected_name: []const u8,
    expected_schema_json: []const u8,
    expected_indexes_json: []const u8,
    actual: *const table_manager.TableRecord,
) bool {
    return std.mem.eql(u8, expected_name, actual.name) and
        std.mem.eql(u8, expected_schema_json, actual.schema_json) and
        std.mem.eql(u8, expected_indexes_json, actual.indexes_json);
}

/// Compact whole-table fence used after a linearizable metadata read. The
/// digest is an order-independent SHA-256 multiset accumulator over every
/// range field, so validation does not depend on projection iteration order.
pub const CatalogTablePublicationContract = struct {
    metadata_group_id: u64,
    metadata_incarnation: MetadataClusterIncarnation,
    table_id: u64,
    table_name: []const u8,
    schema_json: []const u8,
    indexes_json: []const u8,
    topology: CatalogTableTopology,

    pub fn matches(self: @This(), snapshot: *const AdminSnapshot) bool {
        return self.matchesProjection(
            snapshot.status.metadata_group_id,
            snapshot.status.metadata_incarnation,
            snapshot.tables,
            snapshot.ranges,
        );
    }

    pub fn matchesProjection(
        self: @This(),
        metadata_group_id: u64,
        incarnation_value: ?MetadataClusterIncarnation,
        tables: []const table_manager.TableRecord,
        ranges: []const table_manager.RangeRecord,
    ) bool {
        if (metadata_group_id != self.metadata_group_id) return false;
        const incarnation = incarnation_value orelse return false;
        if (!std.mem.eql(u8, &incarnation, &self.metadata_incarnation)) return false;
        var table_match = false;
        for (tables) |table| {
            if (table.table_id != self.table_id) continue;
            table_match = std.mem.eql(u8, self.table_name, table.name) and
                std.mem.eql(u8, self.schema_json, table.schema_json) and
                std.mem.eql(u8, self.indexes_json, table.indexes_json);
            break;
        }
        if (!table_match) return false;
        const actual = catalogTableTopology(self.table_id, ranges);
        return actual.range_count == self.topology.range_count and
            std.crypto.timing_safe.eql(@TypeOf(actual.digest), actual.digest, self.topology.digest);
    }
};

pub fn catalogTableTopology(table_id: u64, ranges: []const table_manager.RangeRecord) CatalogTableTopology {
    return catalogTableTopologyResolution(table_id, ranges).topology;
}

pub const CatalogTableTopologyResolution = struct {
    topology: CatalogTableTopology,
    single_group_id: ?u64,
};

/// Computes the order-independent topology fence and the sole storage group in
/// one catalog pass. `single_group_id` is null for both empty and multi-range
/// tables; callers distinguish those cases with `topology.range_count`.
pub fn catalogTableTopologyResolution(
    table_id: u64,
    ranges: []const table_manager.RangeRecord,
) CatalogTableTopologyResolution {
    var accumulator = [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length;
    var range_count: u64 = 0;
    var single_group_id: ?u64 = null;
    for (ranges) |range| {
        if (range.table_id != table_id) continue;
        range_count += 1;
        if (range_count == 1) {
            single_group_id = range.group_id;
        } else {
            single_group_id = null;
        }
        addCatalogTopologyDigest(&accumulator, catalogRangeTopologyDigest(range));
    }

    return .{
        .topology = .{
            .range_count = range_count,
            .digest = finalizeCatalogTableTopology(table_id, range_count, accumulator),
        },
        .single_group_id = single_group_id,
    };
}

fn catalogRangeTopologyDigest(range: table_manager.RangeRecord) [std.crypto.hash.sha2.Sha256.digest_length]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hashCatalogTopologyU64(&hasher, range.group_id);
    hashCatalogTopologyU64(&hasher, range.range_id);
    hashCatalogTopologyU64(&hasher, range.table_id);
    hashCatalogTopologyBytes(&hasher, range.start_key);
    if (range.end_key) |end_key| {
        hasher.update(&.{1});
        hashCatalogTopologyBytes(&hasher, end_key);
    } else {
        hasher.update(&.{0});
    }
    hashCatalogTopologyU64(&hasher, range.doc_identity_shard_id);
    hashCatalogTopologyU64(&hasher, range.doc_identity_range_id);
    hashCatalogTopologyU64(&hasher, range.split_attempt_epoch);
    hashCatalogTopologyBytes(&hasher, range.restore_backup_id);
    hashCatalogTopologyBytes(&hasher, range.restore_artifact_backup_id);
    hashCatalogTopologyBytes(&hasher, range.restore_location);
    hashCatalogTopologyBytes(&hasher, range.restore_snapshot_path);
    hashCatalogTopologyBytes(&hasher, range.restore_connection);
    hasher.update(std.mem.asBytes(&range.restore_artifact_size_bytes));
    hashCatalogTopologyBytes(&hasher, range.restore_artifact_sha256);
    hasher.update(std.mem.asBytes(&range.restore_native_manifest_size_bytes));
    hashCatalogTopologyBytes(&hasher, range.restore_native_manifest_sha256);
    hasher.update(&range.completed_restore_fingerprint);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn finalizeCatalogTableTopology(
    table_id: u64,
    range_count: u64,
    accumulator: [std.crypto.hash.sha2.Sha256.digest_length]u8,
) [std.crypto.hash.sha2.Sha256.digest_length]u8 {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-catalog-table-topology-v1");
    hashCatalogTopologyU64(&hasher, table_id);
    hashCatalogTopologyU64(&hasher, range_count);
    hasher.update(&accumulator);
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn hashCatalogTopologyU64(hasher: *std.crypto.hash.sha2.Sha256, value: u64) void {
    var encoded: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &encoded, value, .little);
    hasher.update(&encoded);
}

fn hashCatalogTopologyBytes(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    hashCatalogTopologyU64(hasher, @intCast(value.len));
    hasher.update(value);
}

fn addCatalogTopologyDigest(
    accumulator: *[std.crypto.hash.sha2.Sha256.digest_length]u8,
    digest: [std.crypto.hash.sha2.Sha256.digest_length]u8,
) void {
    var carry: u16 = 0;
    for (0..accumulator.len) |offset| {
        const index = accumulator.len - 1 - offset;
        const sum: u16 = @as(u16, accumulator[index]) + @as(u16, digest[index]) + carry;
        accumulator[index] = @truncate(sum);
        carry = sum >> 8;
    }
}

test "catalog table topology is order independent and detects range mutation" {
    const incarnation: MetadataClusterIncarnation = "11111111111111111111111111111111".*;
    const ranges = [_]table_manager.RangeRecord{
        .{ .group_id = 11, .range_id = 21, .table_id = 7, .start_key = "", .end_key = "m" },
        .{ .group_id = 12, .range_id = 22, .table_id = 7, .start_key = "m", .end_key = null },
        .{ .group_id = 13, .range_id = 23, .table_id = 8, .start_key = "", .end_key = null },
    };
    const reordered = [_]table_manager.RangeRecord{ ranges[2], ranges[1], ranges[0] };
    const expected = catalogTableTopology(7, &ranges);
    const actual = catalogTableTopology(7, &reordered);
    try std.testing.expectEqual(expected.range_count, actual.range_count);
    try std.testing.expectEqualSlices(u8, &expected.digest, &actual.digest);

    var changed = ranges;
    changed[1].doc_identity_range_id = 99;
    const changed_topology = catalogTableTopology(7, &changed);
    try std.testing.expect(!std.mem.eql(u8, &expected.digest, &changed_topology.digest));

    var snapshot = AdminSnapshot{
        .status = .{ .metadata_group_id = 1, .metadata_incarnation = incarnation, .metrics = .{} },
        .tables = @constCast((&[_]table_manager.TableRecord{.{
            .table_id = 7,
            .name = "docs",
            .schema_json = "{\"version\":1}",
            .indexes_json = "{}",
        }})[0..]),
        .ranges = @constCast(ranges[0..]),
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    const contract = CatalogTablePublicationContract{
        .metadata_group_id = 1,
        .metadata_incarnation = incarnation,
        .table_id = 7,
        .table_name = "docs",
        .schema_json = "{\"version\":1}",
        .indexes_json = "{}",
        .topology = expected,
    };
    try std.testing.expect(contract.matches(&snapshot));

    var projection_index = try CatalogProjectionIndex.init(std.testing.allocator, snapshot.tables, snapshot.ranges);
    defer projection_index.deinit(std.testing.allocator);
    try std.testing.expect(projection_index.matchesTablePublication(
        contract,
        snapshot.status.metadata_group_id,
        snapshot.status.metadata_incarnation,
        snapshot.tables,
    ));
    try std.testing.expect(projection_index.matchesPublication(.{
        .metadata_group_id = 1,
        .metadata_incarnation = incarnation,
        .table_id = 7,
        .table_name = "docs",
        .schema_json = "{\"version\":1}",
        .indexes_json = "{}",
        .range = ranges[1],
    }, snapshot.status.metadata_group_id, snapshot.status.metadata_incarnation, snapshot.tables, snapshot.ranges));

    const retirement_contract = CatalogGroupRetirementContract{
        .metadata_group_id = 1,
        .metadata_incarnation = incarnation,
        .table_name = "retired-docs",
        .group_ids = &.{ 41, 12 },
    };
    const owned_validation = projection_index.validateGroupRetirement(
        retirement_contract,
        snapshot.status.metadata_group_id,
        snapshot.status.metadata_incarnation,
    );
    try std.testing.expect(!owned_validation.group_ids_unowned);
    try std.testing.expect(owned_validation.table_name_absent);

    const retired_validation = projection_index.validateGroupRetirement(.{
        .metadata_group_id = 1,
        .metadata_incarnation = incarnation,
        .table_name = "docs",
        .group_ids = &.{41},
    }, snapshot.status.metadata_group_id, snapshot.status.metadata_incarnation);
    try std.testing.expect(retired_validation.group_ids_unowned);
    try std.testing.expect(!retired_validation.table_name_absent);

    const wrong_cluster = projection_index.validateGroupRetirement(
        retirement_contract,
        2,
        snapshot.status.metadata_incarnation,
    );
    try std.testing.expect(!wrong_cluster.group_ids_unowned);
    try std.testing.expect(!wrong_cluster.table_name_absent);

    snapshot.ranges = @constCast(changed[0..]);
    try std.testing.expect(!contract.matches(&snapshot));
}

pub fn captureSnapshot(alloc: std.mem.Allocator, source: anytype) !AdminSnapshot {
    const SourceType = @TypeOf(source);
    const SourceDeclType = switch (@typeInfo(SourceType)) {
        .pointer => |pointer| pointer.child,
        else => SourceType,
    };
    var snapshot: AdminSnapshot = .{
        .status = if (@hasDecl(SourceDeclType, "metadataStatus")) try source.metadataStatus() else try source.status(),
        .tables = &.{},
        .ranges = &.{},
        .stores = &.{},
        .placement_intents = &.{},
        .split_transitions = &.{},
        .merge_transitions = &.{},
    };
    errdefer freeSnapshot(alloc, source, &snapshot);
    if (@hasDecl(SourceDeclType, "getProjectedReallocationRequest")) {
        snapshot.reallocation_request = try source.getProjectedReallocationRequest();
    }
    snapshot.tables = try source.listProjectedTables(alloc);
    snapshot.ranges = try source.listProjectedRanges(alloc);
    if (@hasDecl(SourceDeclType, "listProjectedNodes")) {
        snapshot.nodes = try source.listProjectedNodes(alloc);
    }
    snapshot.stores = try source.listProjectedStores(alloc);
    snapshot.placement_intents = try source.listProjectedPlacementIntents(alloc);
    if (@hasDecl(SourceDeclType, "listProjectedShuffleJoinLeases")) {
        snapshot.shuffle_join_leases = try source.listProjectedShuffleJoinLeases(alloc);
    }
    if (@hasDecl(SourceDeclType, "listLocalBootstrapStatuses")) {
        snapshot.local_bootstrap_statuses = try source.listLocalBootstrapStatuses(alloc);
    }
    if (@hasDecl(SourceDeclType, "listProjectedRestoreProgress")) {
        snapshot.restore_progresses = try source.listProjectedRestoreProgress(alloc);
    }
    if (@hasDecl(SourceDeclType, "listProjectedReplicationSourceStatuses")) {
        snapshot.replication_source_statuses = try source.listProjectedReplicationSourceStatuses(alloc);
    }
    if (@hasDecl(SourceDeclType, "listProjectedExtensionPackages")) {
        snapshot.extension_packages = try source.listProjectedExtensionPackages(alloc);
    }
    if (@hasDecl(SourceDeclType, "listProjectedInstalledExtensions")) {
        snapshot.installed_extensions = try source.listProjectedInstalledExtensions(alloc);
    }
    if (@hasDecl(SourceDeclType, "listProjectedExtensionMembers")) {
        snapshot.extension_members = try source.listProjectedExtensionMembers(alloc);
    }
    if (@hasDecl(SourceDeclType, "listProjectedExtensionDependencies")) {
        snapshot.extension_dependencies = try source.listProjectedExtensionDependencies(alloc);
    }
    snapshot.replication_source_action_hints = try deriveReplicationSourceActionHints(alloc, snapshot.tables, snapshot.replication_source_statuses);
    snapshot.split_transitions = try source.listProjectedSplitTransitions(alloc);
    snapshot.merge_transitions = try source.listProjectedMergeTransitions(alloc);
    if (@hasDecl(SourceDeclType, "observeSplitTransition")) {
        snapshot.split_observations = try captureSplitObservations(alloc, source, snapshot.split_transitions);
    }
    if (@hasDecl(SourceDeclType, "observeMergeTransition")) {
        snapshot.merge_observations = try captureMergeObservations(alloc, source, snapshot.merge_transitions);
    }
    const merged_split_observations = try cloneSplitRuntimeObservations(alloc, snapshot.split_observations);
    defer if (merged_split_observations.len > 0) alloc.free(merged_split_observations);
    const merged_merge_observations = try cloneMergeRuntimeObservations(alloc, snapshot.merge_observations);
    defer if (merged_merge_observations.len > 0) alloc.free(merged_merge_observations);
    snapshot.merged_group_statuses = try metadata_state.mergeHealthyGroupStatuses(
        alloc,
        snapshot.tables,
        snapshot.ranges,
        snapshot.placement_intents,
        snapshot.restore_progresses,
        snapshot.stores,
        snapshot.split_transitions,
        snapshot.merge_transitions,
        merged_split_observations,
        merged_merge_observations,
    );
    return snapshot;
}

pub fn freeSnapshot(alloc: std.mem.Allocator, source: anytype, snapshot: *AdminSnapshot) void {
    const SourceType = @TypeOf(source);
    const SourceDeclType = switch (@typeInfo(SourceType)) {
        .pointer => |pointer| pointer.child,
        else => SourceType,
    };
    source.freeProjectedTables(alloc, snapshot.tables);
    source.freeProjectedRanges(alloc, snapshot.ranges);
    if (@hasDecl(SourceDeclType, "freeProjectedNodes") and snapshot.nodes.len > 0) {
        source.freeProjectedNodes(alloc, snapshot.nodes);
    }
    source.freeProjectedStores(alloc, snapshot.stores);
    source.freeProjectedPlacementIntents(alloc, snapshot.placement_intents);
    if (@hasDecl(SourceDeclType, "freeProjectedShuffleJoinLeases") and snapshot.shuffle_join_leases.len > 0) {
        source.freeProjectedShuffleJoinLeases(alloc, snapshot.shuffle_join_leases);
    }
    if (@hasDecl(SourceDeclType, "freeLocalBootstrapStatuses") and snapshot.local_bootstrap_statuses.len > 0) {
        source.freeLocalBootstrapStatuses(alloc, snapshot.local_bootstrap_statuses);
    }
    if (@hasDecl(SourceDeclType, "freeProjectedRestoreProgress") and snapshot.restore_progresses.len > 0) {
        source.freeProjectedRestoreProgress(alloc, snapshot.restore_progresses);
    }
    if (@hasDecl(SourceDeclType, "freeProjectedReplicationSourceStatuses") and snapshot.replication_source_statuses.len > 0) {
        source.freeProjectedReplicationSourceStatuses(alloc, snapshot.replication_source_statuses);
    }
    if (@hasDecl(SourceDeclType, "freeProjectedExtensionPackages") and snapshot.extension_packages.len > 0) {
        source.freeProjectedExtensionPackages(alloc, snapshot.extension_packages);
    }
    if (@hasDecl(SourceDeclType, "freeProjectedInstalledExtensions") and snapshot.installed_extensions.len > 0) {
        source.freeProjectedInstalledExtensions(alloc, snapshot.installed_extensions);
    }
    if (@hasDecl(SourceDeclType, "freeProjectedExtensionMembers") and snapshot.extension_members.len > 0) {
        source.freeProjectedExtensionMembers(alloc, snapshot.extension_members);
    }
    if (@hasDecl(SourceDeclType, "freeProjectedExtensionDependencies") and snapshot.extension_dependencies.len > 0) {
        source.freeProjectedExtensionDependencies(alloc, snapshot.extension_dependencies);
    }
    freeReplicationSourceActionHints(alloc, snapshot.replication_source_action_hints);
    source.freeProjectedSplitTransitions(alloc, snapshot.split_transitions);
    source.freeProjectedMergeTransitions(alloc, snapshot.merge_transitions);
    if (snapshot.split_observations.len > 0) alloc.free(snapshot.split_observations);
    if (snapshot.merge_observations.len > 0) alloc.free(snapshot.merge_observations);
    if (snapshot.merged_group_statuses.len > 0) metadata_state.freeMergedGroupStatuses(alloc, snapshot.merged_group_statuses);
    snapshot.* = undefined;
}

pub fn deriveReplicationSourceActionHints(
    alloc: std.mem.Allocator,
    tables: []const table_manager.TableRecord,
    statuses: []const table_manager.ReplicationSourceStatusRecord,
) ![]ReplicationSourceActionHint {
    var out = std.ArrayListUnmanaged(ReplicationSourceActionHint).empty;
    errdefer freeReplicationSourceActionHints(alloc, out.items);
    for (statuses) |status| {
        const reason = replicationSourceReseedReason(status) orelse continue;
        const table_name = findTableNameById(tables, status.table_id) orelse continue;
        const path = try std.fmt.allocPrint(
            alloc,
            "/internal/v1/tables/{s}/replication-sources/{d}/reseed-exact-cutover",
            .{ table_name, status.source_ordinal },
        );
        errdefer alloc.free(path);
        try out.append(alloc, .{
            .table_id = status.table_id,
            .table_name = try alloc.dupe(u8, table_name),
            .source_ordinal = status.source_ordinal,
            .action = "reseed_exact_cutover",
            .reason = reason,
            .reseed_exact_cutover_path = path,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn freeReplicationSourceActionHints(alloc: std.mem.Allocator, hints: []ReplicationSourceActionHint) void {
    for (hints) |hint| {
        alloc.free(hint.table_name);
        alloc.free(hint.reseed_exact_cutover_path);
    }
    if (hints.len > 0) alloc.free(hints);
}

fn replicationSourceReseedReason(status: table_manager.ReplicationSourceStatusRecord) ?[]const u8 {
    if (!std.mem.eql(u8, status.source_kind, "postgres")) return null;
    if (std.mem.eql(u8, status.cutover_mode, "slot_resumed")) return "existing_slot_non_exact_cutover";
    if (std.mem.eql(u8, status.last_error, "ReplicationExactCutoverRequired")) return "exact_cutover_required_rejected";
    return null;
}

fn findTableNameById(tables: []const table_manager.TableRecord, table_id: u64) ?[]const u8 {
    for (tables) |table| {
        if (table.table_id == table_id) return table.name;
    }
    return null;
}

pub fn captureSplitObservations(
    alloc: std.mem.Allocator,
    source: anytype,
    split_transitions: []const transition_state.SplitTransitionRecord,
) ![]transition_state.SplitObservationRecord {
    var out = std.ArrayListUnmanaged(transition_state.SplitObservationRecord).empty;
    errdefer out.deinit(alloc);
    for (split_transitions) |record| {
        const observation = (source.observeSplitTransition(record.transition_id) catch |err| {
            std.log.warn("split transition snapshot observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
            continue;
        }) orelse continue;
        try out.append(alloc, .{
            .transition_id = record.transition_id,
            .observation = observation,
        });
    }
    return try out.toOwnedSlice(alloc);
}

pub fn captureMergeObservations(
    alloc: std.mem.Allocator,
    source: anytype,
    merge_transitions: []const transition_state.MergeTransitionRecord,
) ![]transition_state.MergeObservationRecord {
    var out = std.ArrayListUnmanaged(transition_state.MergeObservationRecord).empty;
    errdefer out.deinit(alloc);
    for (merge_transitions) |record| {
        const observation = (source.observeMergeTransition(record.transition_id) catch |err| {
            std.log.warn("merge transition snapshot observation failed transition_id={d} err={s}", .{ record.transition_id, @errorName(err) });
            continue;
        }) orelse continue;
        try out.append(alloc, .{
            .transition_id = record.transition_id,
            .observation = observation,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn cloneSplitRuntimeObservations(
    alloc: std.mem.Allocator,
    records: []const transition_state.SplitObservationRecord,
) ![]metadata_reconciler.SplitRuntimeObservation {
    const out = try alloc.alloc(metadata_reconciler.SplitRuntimeObservation, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .observation = record.observation,
        };
    }
    return out;
}

fn cloneMergeRuntimeObservations(
    alloc: std.mem.Allocator,
    records: []const transition_state.MergeObservationRecord,
) ![]metadata_reconciler.MergeRuntimeObservation {
    const out = try alloc.alloc(metadata_reconciler.MergeRuntimeObservation, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| {
        out[i] = .{
            .transition_id = record.transition_id,
            .observation = record.observation,
        };
    }
    return out;
}

test "metadata admin snapshot captures projected metadata state" {
    const FakeSource = struct {
        fn status(_: @This()) !MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_tables = 1,
                .projected_tables_with_replication_sources = 1,
                .projected_replication_sources = 2,
                .projected_replication_source_statuses = 1,
                .projected_replication_source_statuses_exact_cutover = 1,
                .projected_replication_source_statuses_non_exact_cutover = 0,
                .projected_replication_source_statuses_reseed_recommended = 0,
                .projected_replication_source_statuses_exported_snapshot = 1,
                .projected_replication_source_statuses_slot_first = 0,
                .projected_replication_source_statuses_slot_resumed = 0,
                .projected_replication_source_statuses_snapshot = 1,
                .projected_replication_source_statuses_cutover_prepared = 0,
                .projected_replication_source_statuses_streaming = 0,
                .projected_replication_source_statuses_failed = 0,
                .projected_replication_source_statuses_with_last_error = 0,
                .projected_replication_source_statuses_slot_missing_failed = 0,
                .projected_replication_source_statuses_retryable_failed = 0,
                .projected_replication_source_statuses_terminal_failed = 0,
                .projected_replication_source_statuses_with_consecutive_failures = 1,
                .projected_replication_source_statuses_with_success_timestamp = 1,
                .projected_replication_source_statuses_with_change_timestamp = 1,
                .projected_replication_source_consecutive_failures_total = 2,
                .projected_replication_source_consecutive_failures_max = 2,
                .projected_replication_source_lag_records_total = 12,
                .projected_replication_source_lag_records_max = 12,
                .projected_replication_source_lag_millis_total = 34,
                .projected_replication_source_lag_millis_max = 34,
                .projected_replication_source_observed_lag_millis_total = 56,
                .projected_replication_source_observed_lag_millis_max = 56,
                .projected_replication_source_statuses_with_source_commit_timestamp = 1,
                .projected_replication_source_last_success_at_ms_max = 1234,
                .projected_replication_source_last_source_commit_at_ms_max = 1200,
                .projected_replication_source_last_change_applied_at_ms_max = 1235,
                .projected_ranges = 1,
                .projected_stores = 1,
                .projected_placement_intents = 1,
                .projected_restore_progress = 1,
                .projected_split_transitions = 1,
                .projected_merge_transitions = 1,
            };
        }

        fn listProjectedTables(_: @This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const records = try alloc.alloc(table_manager.TableRecord, 1);
            records[0] = .{
                .table_id = 1,
                .name = try alloc.dupe(u8, "docs"),
                .replication_sources_json = try alloc.dupe(u8, "[{\"type\":\"postgres\"},{\"type\":\"postgres\"}]"),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return records;
        }

        fn freeProjectedTables(_: @This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.replication_sources_json);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        fn listProjectedRanges(_: @This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            const records = try alloc.alloc(table_manager.RangeRecord, 1);
            records[0] = .{ .group_id = 10, .table_id = 1, .start_key = try alloc.dupe(u8, "doc:a"), .end_key = try alloc.dupe(u8, "doc:z") };
            return records;
        }

        fn freeProjectedRanges(_: @This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            for (records) |record| {
                alloc.free(record.start_key);
                if (record.end_key) |end_key| alloc.free(end_key);
            }
            alloc.free(records);
        }

        fn listProjectedStores(_: @This(), alloc: std.mem.Allocator) ![]table_manager.StoreRecord {
            const records = try alloc.alloc(table_manager.StoreRecord, 1);
            const group_statuses = try alloc.alloc(table_manager.GroupStatusReport, 1);
            group_statuses[0] = .{
                .group_id = 10,
                .doc_count = 5,
                .disk_bytes = 128,
                .empty = false,
                .created_at_millis = 7,
                .updated_at_millis = 11,
                .local_leader = true,
                .transition_pending = true,
                .replay_required = true,
                .replay_caught_up = true,
                .cutover_ready = true,
                .reads_ready_after_cutover = true,
            };
            records[0] = .{
                .store_id = 11,
                .node_id = 1,
                .role = try alloc.dupe(u8, "data"),
                .health_class = try alloc.dupe(u8, "healthy"),
                .failure_domain = try alloc.dupe(u8, "rack-a"),
                .group_statuses = group_statuses,
            };
            return records;
        }

        fn freeProjectedStores(_: @This(), alloc: std.mem.Allocator, records: []table_manager.StoreRecord) void {
            for (records) |record| table_manager.freeStore(alloc, record);
            alloc.free(records);
        }

        fn listProjectedPlacementIntents(_: @This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            const intents = try alloc.alloc(raft_reconciler.PlacementIntent, 1);
            intents[0] = .{
                .record = .{ .group_id = 10, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted },
                .peer_node_ids = try alloc.dupe(u64, &.{2}),
            };
            return intents;
        }

        fn freeProjectedPlacementIntents(_: @This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            for (intents) |intent| alloc.free(intent.peer_node_ids);
            alloc.free(intents);
        }

        fn listLocalBootstrapStatuses(_: @This(), alloc: std.mem.Allocator) ![]raft_host.BootstrapStatus {
            const statuses = try alloc.alloc(raft_host.BootstrapStatus, 1);
            statuses[0] = .{
                .group_id = 10,
                .kind = .backup_db_snapshot_restore,
                .phase = .failed,
                .attempts = 2,
                .last_updated_at_millis = 1234,
                .last_error = try alloc.dupe(u8, "InvalidBackupLocation"),
                .backup_id = try alloc.dupe(u8, "snap1"),
                .snapshot_path = try alloc.dupe(u8, "snap1/groups/10"),
            };
            return statuses;
        }

        fn freeLocalBootstrapStatuses(_: @This(), alloc: std.mem.Allocator, statuses: []raft_host.BootstrapStatus) void {
            for (statuses) |bootstrap_status| {
                if (bootstrap_status.last_error) |msg| alloc.free(msg);
                if (bootstrap_status.backup_id) |value| alloc.free(value);
                if (bootstrap_status.snapshot_path) |value| alloc.free(value);
            }
            alloc.free(statuses);
        }

        fn listProjectedRestoreProgress(_: @This(), alloc: std.mem.Allocator) ![]table_manager.RestoreProgressRecord {
            const records = try alloc.alloc(table_manager.RestoreProgressRecord, 1);
            records[0] = .{
                .table_id = 1,
                .node_id = 1,
                .group_id = 10,
                .backup_id = try alloc.dupe(u8, "snap1"),
            };
            return records;
        }

        fn freeProjectedRestoreProgress(_: @This(), alloc: std.mem.Allocator, records: []table_manager.RestoreProgressRecord) void {
            for (records) |record| table_manager.freeRestoreProgress(alloc, record);
            alloc.free(records);
        }

        fn listProjectedReplicationSourceStatuses(_: @This(), alloc: std.mem.Allocator) ![]table_manager.ReplicationSourceStatusRecord {
            const records = try alloc.alloc(table_manager.ReplicationSourceStatusRecord, 1);
            records[0] = .{
                .table_id = 1,
                .source_ordinal = 0,
                .source_kind = try alloc.dupe(u8, "postgres"),
                .external_table = try alloc.dupe(u8, "users"),
                .cutover_mode = try alloc.dupe(u8, "exported_snapshot"),
                .slot_name = try alloc.dupe(u8, "antfly_postgres_users_docs"),
                .publication_name = try alloc.dupe(u8, "antfly_pub_postgres_users_docs"),
                .phase = try alloc.dupe(u8, "snapshot"),
                .checkpoint = try alloc.dupe(u8, "lsn:0/16B6A50"),
                .snapshot_offset = 2,
                .prepared_checkpoint = try alloc.dupe(u8, "lsn:0/16B6A50"),
                .stream_checkpoint = try alloc.dupe(u8, "lsn:0/16B6A50"),
                .last_error = try alloc.dupe(u8, ""),
                .failure_class = try alloc.dupe(u8, ""),
                .lag_records = 12,
                .lag_millis = 34,
                .consecutive_failures = 2,
                .last_source_commit_at_ms = 1200,
                .last_success_at_ms = 1234,
                .last_change_applied_at_ms = 1235,
                .updated_at_ms = 555,
            };
            return records;
        }

        fn freeProjectedReplicationSourceStatuses(_: @This(), alloc: std.mem.Allocator, records: []table_manager.ReplicationSourceStatusRecord) void {
            for (records) |record| table_manager.freeReplicationSourceStatus(alloc, record);
            alloc.free(records);
        }

        fn listProjectedSplitTransitions(_: @This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            const records = try alloc.alloc(transition_state.SplitTransitionRecord, 1);
            records[0] = .{
                .transition_id = 9001,
                .attempt_epoch = 1,
                .source_group_id = 10,
                .destination_group_id = 11,
                .split_key = try alloc.dupe(u8, "doc:m"),
            };
            return records;
        }

        fn freeProjectedSplitTransitions(_: @This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            for (records) |record| {
                if (record.split_key) |split_key| alloc.free(split_key);
                if (record.rollback_reason) |reason| alloc.free(reason);
            }
            alloc.free(records);
        }

        fn listProjectedMergeTransitions(_: @This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            const records = try alloc.alloc(transition_state.MergeTransitionRecord, 1);
            records[0] = .{ .transition_id = 9002, .donor_group_id = 11, .receiver_group_id = 10 };
            return records;
        }

        fn freeProjectedMergeTransitions(_: @This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            for (records) |record| if (record.rollback_reason) |reason| alloc.free(reason);
            alloc.free(records);
        }

        fn observeSplitTransition(_: @This(), transition_id: u64) !?transition_state.SplitObservation {
            if (transition_id != 9001) return null;
            return .{
                .status = .{
                    .phase = .cutover_ready,
                    .source_split_phase = .splitting,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .destination_ready_for_reads = true,
                    .source_delta_sequence = 3,
                    .dest_delta_sequence = 3,
                },
            };
        }

        fn observeMergeTransition(_: @This(), transition_id: u64) !?transition_state.MergeObservation {
            if (transition_id != 9002) return null;
            return .{
                .donor = .{
                    .phase = .cutover_ready,
                    .donor_group_id = 11,
                    .receiver_group_id = 10,
                    .receiver_accepts_donor_range = true,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .receiver_ready_for_reads = true,
                    .donor_delta_sequence = 4,
                    .receiver_delta_sequence = 4,
                    .allow_doc_identity_reassignment = true,
                },
                .receiver = .{
                    .phase = .cutover_ready,
                    .donor_group_id = 11,
                    .receiver_group_id = 10,
                    .receiver_accepts_donor_range = true,
                    .bootstrapped = true,
                    .replay_required = true,
                    .replay_caught_up = true,
                    .cutover_ready = true,
                    .receiver_ready_for_reads = true,
                    .donor_delta_sequence = 4,
                    .receiver_delta_sequence = 4,
                    .allow_doc_identity_reassignment = true,
                },
                .receiver_local_leader = true,
            };
        }
    };

    const source = FakeSource{};
    var snapshot = try captureSnapshot(std.testing.allocator, source);
    defer freeSnapshot(std.testing.allocator, source, &snapshot);

    try std.testing.expectEqual(@as(u64, 77), snapshot.status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_tables_with_replication_sources);
    try std.testing.expectEqual(@as(usize, 2), snapshot.status.projected_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_exact_cutover);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_non_exact_cutover);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_reseed_recommended);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_exported_snapshot);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_slot_first);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_slot_resumed);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_snapshot);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_cutover_prepared);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_streaming);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_failed);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_with_last_error);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_slot_missing_failed);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_retryable_failed);
    try std.testing.expectEqual(@as(usize, 0), snapshot.status.projected_replication_source_statuses_terminal_failed);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_with_consecutive_failures);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_with_success_timestamp);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_with_change_timestamp);
    try std.testing.expectEqual(@as(u64, 2), snapshot.status.projected_replication_source_consecutive_failures_total);
    try std.testing.expectEqual(@as(u64, 2), snapshot.status.projected_replication_source_consecutive_failures_max);
    try std.testing.expectEqual(@as(u64, 12), snapshot.status.projected_replication_source_lag_records_total);
    try std.testing.expectEqual(@as(u64, 12), snapshot.status.projected_replication_source_lag_records_max);
    try std.testing.expectEqual(@as(u64, 34), snapshot.status.projected_replication_source_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 34), snapshot.status.projected_replication_source_lag_millis_max);
    try std.testing.expectEqual(@as(u64, 56), snapshot.status.projected_replication_source_observed_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 56), snapshot.status.projected_replication_source_observed_lag_millis_max);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.projected_replication_source_statuses_with_source_commit_timestamp);
    try std.testing.expectEqual(@as(u64, 1234), snapshot.status.projected_replication_source_last_success_at_ms_max);
    try std.testing.expectEqual(@as(u64, 1200), snapshot.status.projected_replication_source_last_source_commit_at_ms_max);
    try std.testing.expectEqual(@as(u64, 1235), snapshot.status.projected_replication_source_last_change_applied_at_ms_max);
    try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.ranges.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.stores.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.placement_intents.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.local_bootstrap_statuses.len);
    try std.testing.expectEqual(@as(u64, 10), snapshot.local_bootstrap_statuses[0].group_id);
    try std.testing.expectEqual(.failed, snapshot.local_bootstrap_statuses[0].phase);
    try std.testing.expectEqual(@as(u64, 1234), snapshot.local_bootstrap_statuses[0].last_updated_at_millis);
    try std.testing.expectEqualStrings("snap1", snapshot.local_bootstrap_statuses[0].backup_id orelse return error.TestExpectedEqual);
    try std.testing.expectEqualStrings("snap1/groups/10", snapshot.local_bootstrap_statuses[0].snapshot_path orelse return error.TestExpectedEqual);
    try std.testing.expectEqual(@as(usize, 1), snapshot.restore_progresses.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.replication_source_statuses.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.replication_source_action_hints.len);
    try std.testing.expectEqualStrings("postgres", snapshot.replication_source_statuses[0].source_kind);
    try std.testing.expectEqualStrings("users", snapshot.replication_source_statuses[0].external_table);
    try std.testing.expectEqualStrings("exported_snapshot", snapshot.replication_source_statuses[0].cutover_mode);
    try std.testing.expectEqualStrings("antfly_postgres_users_docs", snapshot.replication_source_statuses[0].slot_name);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_docs", snapshot.replication_source_statuses[0].publication_name);
    try std.testing.expectEqualStrings("snapshot", snapshot.replication_source_statuses[0].phase);
    try std.testing.expectEqual(@as(u64, 2), snapshot.replication_source_statuses[0].snapshot_offset);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", snapshot.replication_source_statuses[0].prepared_checkpoint);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", snapshot.replication_source_statuses[0].stream_checkpoint);
    try std.testing.expectEqualStrings("", snapshot.replication_source_statuses[0].failure_class);
    try std.testing.expectEqual(@as(u64, 34), snapshot.replication_source_statuses[0].lag_millis);
    try std.testing.expectEqual(@as(u64, 2), snapshot.replication_source_statuses[0].consecutive_failures);
    try std.testing.expectEqual(@as(u64, 1200), snapshot.replication_source_statuses[0].last_source_commit_at_ms);
    try std.testing.expectEqual(@as(u64, 1234), snapshot.replication_source_statuses[0].last_success_at_ms);
    try std.testing.expectEqual(@as(u64, 1235), snapshot.replication_source_statuses[0].last_change_applied_at_ms);
    try std.testing.expectEqual(@as(usize, 1), snapshot.split_transitions.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.merge_transitions.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.split_observations.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.merge_observations.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.merged_group_statuses.len);
    try std.testing.expectEqual(@as(u64, 10), snapshot.merged_group_statuses[0].group_id);
    try std.testing.expect(snapshot.merged_group_statuses[0].leader_known);
    try std.testing.expectEqual(@as(u64, 11), snapshot.merged_group_statuses[0].leader_store_id);
    try std.testing.expect(snapshot.merged_group_statuses[0].doc_identity_reassignment_active);
}

test "metadata admin snapshot derives replication source action hints for reseed" {
    const FakeSource = struct {
        fn status(_: @This()) !MetadataStatus {
            return .{
                .metadata_group_id = 77,
                .metrics = .{},
                .projected_tables = 1,
                .projected_replication_source_statuses = 1,
                .projected_replication_source_statuses_exact_cutover = 0,
                .projected_replication_source_statuses_non_exact_cutover = 1,
                .projected_replication_source_statuses_reseed_recommended = 1,
            };
        }

        fn listProjectedTables(_: @This(), alloc: std.mem.Allocator) ![]table_manager.TableRecord {
            const records = try alloc.alloc(table_manager.TableRecord, 1);
            records[0] = .{
                .table_id = 9,
                .name = try alloc.dupe(u8, "docs"),
                .replication_sources_json = try alloc.dupe(u8, "[{\"type\":\"postgres\"}]"),
                .placement_role = try alloc.dupe(u8, "data"),
            };
            return records;
        }

        fn freeProjectedTables(_: @This(), alloc: std.mem.Allocator, records: []table_manager.TableRecord) void {
            for (records) |record| {
                alloc.free(record.name);
                alloc.free(record.replication_sources_json);
                alloc.free(record.placement_role);
            }
            alloc.free(records);
        }

        fn listProjectedRanges(_: @This(), alloc: std.mem.Allocator) ![]table_manager.RangeRecord {
            const records = try alloc.alloc(table_manager.RangeRecord, 0);
            return records;
        }

        fn freeProjectedRanges(_: @This(), alloc: std.mem.Allocator, records: []table_manager.RangeRecord) void {
            alloc.free(records);
        }

        fn listProjectedStores(_: @This(), alloc: std.mem.Allocator) ![]table_manager.StoreRecord {
            const records = try alloc.alloc(table_manager.StoreRecord, 0);
            return records;
        }

        fn freeProjectedStores(_: @This(), alloc: std.mem.Allocator, records: []table_manager.StoreRecord) void {
            alloc.free(records);
        }

        fn listProjectedPlacementIntents(_: @This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            const records = try alloc.alloc(raft_reconciler.PlacementIntent, 0);
            return records;
        }

        fn freeProjectedPlacementIntents(_: @This(), alloc: std.mem.Allocator, records: []raft_reconciler.PlacementIntent) void {
            alloc.free(records);
        }

        fn listProjectedReplicationSourceStatuses(_: @This(), alloc: std.mem.Allocator) ![]table_manager.ReplicationSourceStatusRecord {
            const records = try alloc.alloc(table_manager.ReplicationSourceStatusRecord, 1);
            records[0] = .{
                .table_id = 9,
                .source_ordinal = 0,
                .source_kind = try alloc.dupe(u8, "postgres"),
                .external_table = try alloc.dupe(u8, "users"),
                .cutover_mode = try alloc.dupe(u8, "slot_resumed"),
                .slot_name = try alloc.dupe(u8, "slot_old"),
                .publication_name = try alloc.dupe(u8, "pub_old"),
                .phase = try alloc.dupe(u8, "streaming"),
            };
            return records;
        }

        fn freeProjectedReplicationSourceStatuses(_: @This(), alloc: std.mem.Allocator, records: []table_manager.ReplicationSourceStatusRecord) void {
            for (records) |record| table_manager.freeReplicationSourceStatus(alloc, record);
            alloc.free(records);
        }

        fn listProjectedSplitTransitions(_: @This(), alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
            const records = try alloc.alloc(transition_state.SplitTransitionRecord, 0);
            return records;
        }

        fn freeProjectedSplitTransitions(_: @This(), alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
            alloc.free(records);
        }

        fn listProjectedMergeTransitions(_: @This(), alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
            const records = try alloc.alloc(transition_state.MergeTransitionRecord, 0);
            return records;
        }

        fn freeProjectedMergeTransitions(_: @This(), alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
            alloc.free(records);
        }
    };

    var snapshot = try captureSnapshot(std.testing.allocator, FakeSource{});
    defer freeSnapshot(std.testing.allocator, FakeSource{}, &snapshot);

    try std.testing.expectEqual(@as(usize, 1), snapshot.replication_source_action_hints.len);
    try std.testing.expectEqualStrings("reseed_exact_cutover", snapshot.replication_source_action_hints[0].action);
    try std.testing.expectEqualStrings("existing_slot_non_exact_cutover", snapshot.replication_source_action_hints[0].reason);
    try std.testing.expectEqualStrings("docs", snapshot.replication_source_action_hints[0].table_name);
    try std.testing.expect(std.mem.indexOf(u8, snapshot.replication_source_action_hints[0].reseed_exact_cutover_path, "/internal/v1/tables/docs/replication-sources/0/reseed-exact-cutover") != null);
}
