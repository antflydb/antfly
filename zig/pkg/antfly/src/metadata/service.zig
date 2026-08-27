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

const builtin = @import("builtin");
const std = @import("std");
const fs_paths = @import("../common/fs_paths.zig");
const common_secrets = @import("../common/secrets.zig");
const metadata_mod = @import("domain.zig");
const extension_domain = @import("../extensions/mod.zig");
const metadata_api = @import("api.zig");
const metadata_http_client = @import("http_client.zig");
const raft_engine = @import("raft_engine");
const metadata_control_loop = @import("control_loop.zig");
const metadata_reconcile_lease = @import("reconcile_lease.zig");
const metadata_reallocation_request = @import("reallocation_request.zig");
const metadata_runtime_status_protocol = @import("runtime_status_protocol.zig");
const metadata_reconciler = @import("reconciler.zig");
const metadata_replication_backfill = @import("replication_backfill.zig");
const metadata_state = @import("state.zig");
const metadata_table_provisioner = @import("table_provisioner.zig");
const metadata_store_observer = @import("store_observer.zig");
const metadata_table_manager = @import("table_manager.zig");
const metadata_table_workflow = @import("table_workflow.zig");
const metadata_storage = @import("storage/mod.zig");
const platform_clock = @import("antfly_platform").clock;
const process_memory_mod = @import("antfly_platform").process_memory;
const platform_time = @import("antfly_platform").time;
const raft_reconciler = @import("../raft/reconciler.zig");
const transition_state = @import("transition_state.zig");
const raft_catalog = @import("../raft/catalog.zig");
const raft_host = @import("../raft/host.zig");
const raft_managed_host = @import("../raft/managed_host.zig");
const raft_service = @import("../raft/service.zig");
const raft_transition_service = @import("../raft/transition_service.zig");
const raft_state_machine = @import("../raft/state_machine/mod.zig");
const http_common = @import("../raft/transport/http_common.zig");
const api_table_catalog = @import("../api/table_catalog.zig");
const api_operation = @import("../api/operation.zig");
const api_table_router = @import("../api/table_router.zig");
const api_table_writes = @import("../api/table_writes.zig");
const stored_destination_authorization = @import("../api/stored_destination_authorization.zig");
const db_mod = @import("../storage/db/mod.zig");
const backend_runtime_mod = @import("../storage/background_runtime.zig");
const backfill_state_mod = @import("../storage/db/backfill_state.zig");
const internal_keys = @import("../storage/internal_keys.zig");
const foreign_mod = @import("../foreign/mod.zig");

const cdc_replication_round_interval_ms: u64 = 1_000;
pub const metadata_run_round_slow_threshold_ns: u64 = std.time.ns_per_s;
const metadata_run_round_slow_phase_threshold_ns: u64 = 500 * std.time.ns_per_ms;
const metadata_run_round_trace_max_phases: usize = 32;
const linearizable_metadata_read_prefix = "metadata:linearizable-read:";
const linearizable_metadata_read_timeout_ns: u64 = 5 * std.time.ns_per_s;
const linearizable_metadata_read_retry_ns: u64 = 50 * std.time.ns_per_ms;
const metadata_proposal_driver_wait_ns: u64 = std.time.ns_per_ms;
const metadata_proposal_passive_wait_ns: u64 = 25 * std.time.ns_per_ms;
const reallocation_protocol_probe_timeout_ns: u64 = 5 * std.time.ns_per_s;
const runtime_status_protocol_probe_min_backoff_ns: u64 = 5 * std.time.ns_per_s;
const runtime_status_protocol_probe_max_backoff_ns: u64 = 60 * std.time.ns_per_s;

pub const AdminSnapshotFence = struct {
    metadata_group_id: u64,
    metadata_incarnation: ?metadata_api.MetadataClusterIncarnation,
    metadata_raft_term: u64,
    metadata_raft_commit_index: u64,
    metadata_raft_applied_index: u64,
    projection_epoch: u64,
    catalog_epoch: u64,
    placement_epoch: u64,
    reconcile_lease_epoch: u64,
    transition_epoch: u64,
    projected_core_epoch: u64 = 0,
    transition_readiness_epoch: u64 = 0,
};

pub const MetadataProposalReceipt = struct {
    term: u64,
    index: u64,
};

const MetadataProposalApplyObservation = enum {
    pending,
    applied,
    superseded,
};

fn acceptedMetadataProposalIndex(accepted_index: ?u64, dispatch_error: ?anyerror) !u64 {
    if (accepted_index) |index| return index;
    if (dispatch_error) |err| return err;
    return error.NotLeader;
}

fn observeMetadataProposalApply(
    status: ?raft_engine.core.Status,
    applied_entry_term: ?u64,
    receipt: MetadataProposalReceipt,
) MetadataProposalApplyObservation {
    const raft_status = status orelse return .superseded;
    if (raft_status.applied_index >= receipt.index) {
        return if (applied_entry_term != null and applied_entry_term.? == receipt.term)
            .applied
        else
            .superseded;
    }
    const still_receipt_leader = raft_status.soft.role == .leader and
        raft_status.soft.leader_id != null and
        raft_status.soft.leader_id.? == raft_status.id and
        raft_status.hard.current_term == receipt.term;
    return if (still_receipt_leader) .pending else .superseded;
}

test "metadata proposal receipt requires the accepted term at the applied index" {
    const receipt: MetadataProposalReceipt = .{ .term = 3, .index = 9 };
    var status: raft_engine.core.Status = .{
        .id = 1,
        .group_id = 1,
        .soft = .{ .role = .leader, .leader_id = 1 },
        .hard = .{ .current_term = 3, .commit_index = 9 },
        .conf_state = .{},
        .applied_index = 8,
    };

    try std.testing.expectEqual(.pending, observeMetadataProposalApply(status, null, receipt));
    status.applied_index = 9;
    try std.testing.expectEqual(.applied, observeMetadataProposalApply(status, 3, receipt));
    try std.testing.expectEqual(.superseded, observeMetadataProposalApply(status, 4, receipt));
    try std.testing.expectEqual(.superseded, observeMetadataProposalApply(status, null, receipt));

    status.applied_index = 8;
    status.soft = .{ .role = .follower, .leader_id = 2 };
    status.hard.current_term = 4;
    try std.testing.expectEqual(.superseded, observeMetadataProposalApply(status, null, receipt));
}

test "metadata proposal receipt survives a post-acceptance dispatch failure" {
    try std.testing.expectEqual(
        @as(u64, 17),
        try acceptedMetadataProposalIndex(17, error.OutOfMemory),
    );
    try std.testing.expectError(
        error.OutOfMemory,
        acceptedMetadataProposalIndex(null, error.OutOfMemory),
    );
    try std.testing.expectError(
        error.NotLeader,
        acceptedMetadataProposalIndex(null, null),
    );
}

pub fn sameAdminSnapshotFence(before: AdminSnapshotFence, after: AdminSnapshotFence) bool {
    return before.metadata_group_id == after.metadata_group_id and
        std.meta.eql(before.metadata_incarnation, after.metadata_incarnation) and
        before.metadata_raft_term == after.metadata_raft_term and
        before.metadata_raft_commit_index == after.metadata_raft_commit_index and
        before.metadata_raft_applied_index == after.metadata_raft_applied_index and
        before.projection_epoch == after.projection_epoch and
        before.catalog_epoch == after.catalog_epoch and
        before.placement_epoch == after.placement_epoch and
        before.reconcile_lease_epoch == after.reconcile_lease_epoch and
        before.transition_epoch == after.transition_epoch and
        before.projected_core_epoch == after.projected_core_epoch and
        before.transition_readiness_epoch == after.transition_readiness_epoch;
}

/// Return the exact owned snapshot covered by the linearizable read barrier.
/// The before/after fence makes the multi-collection capture coherent without
/// holding the Raft runtime lock while a potentially large snapshot is cloned.
pub fn coherentLinearizableAdminSnapshot(
    comptime Service: type,
    svc: *Service,
    request: api_operation.RequestContext,
) !metadata_api.AdminSnapshot {
    try svc.ensureLinearizableReadWithContext(request);
    for (0..3) |_| {
        try request.ensureActive();
        const before = try svc.adminSnapshotFence();
        var snapshot = try svc.adminSnapshot();
        errdefer svc.freeAdminSnapshot(&snapshot);
        try request.ensureActive();
        const after = try svc.adminSnapshotFence();
        if (sameAdminSnapshotFence(before, after)) return snapshot;
        svc.freeAdminSnapshot(&snapshot);
    }
    return error.MetadataLinearizableReadTimeout;
}

fn logMetadataRunRoundPhase(name: []const u8, elapsed_ns: u64) void {
    if (elapsed_ns > metadata_run_round_slow_phase_threshold_ns) {
        std.log.warn("metadata runRound phase slow phase={s} elapsed_ms={d}", .{
            name,
            @divTrunc(elapsed_ns, std.time.ns_per_ms),
        });
    }
}

fn logMetadataRaftRoundDiagnostics(round: raft_engine.runtime.multi_raft.HostRound) void {
    if (round.elapsed_ns <= metadata_run_round_slow_phase_threshold_ns) return;
    const ready = round.slowest_ready_group;
    std.log.warn(
        "metadata raft round slow elapsed_ms={d} inbound_ms={d} tick_ms={d} drain_ready_ms={d} drain_scan_ms={d} persist_begin_ms={d} persist_finish_ms={d} outbox_drain_ms={d} apply_flush_ms={d} transport_flush_ms={d} transport_advance_ms={d} ticked_groups={d} processed_groups={d} processed_ready_steps={d} virtual_round={d} virtual_time_ms={d} ready_group_id={d} ready_group_ms={d}",
        .{
            @divTrunc(round.elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.inbound_drain_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.tick_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.drain_ready_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.drain_ready_scan_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.persist_batch_begin_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.persist_batch_finish_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.outbox_drain_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.apply_flush_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.transport_flush_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(round.transport_advance_elapsed_ns, std.time.ns_per_ms),
            round.ticked_groups,
            round.processed_groups,
            round.processed_ready_steps,
            round.virtual_round,
            round.virtual_time_ms,
            ready.group_id,
            @divTrunc(ready.elapsed_ns, std.time.ns_per_ms),
        },
    );
    std.log.warn(
        "metadata raft ready slow group_id={d} ready_build_ms={d} ready_backpressure_ms={d} ready_capacity_ms={d} ready_snapshot_throttle_ms={d} ready_persist_ms={d} ready_async_ms={d} ready_clone_messages_ms={d} ready_enqueue_apply_ms={d} ready_async_loop_ms={d} ready_outbox_append_ms={d} ready_advance_ms={d} ready_inline_apply_flush_ms={d} ready_inline_outbox_drain_ms={d} ready_inline_transport_flush_ms={d}",
        .{
            ready.group_id,
            @divTrunc(ready.ready_build_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.backpressure_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.capacity_check_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.snapshot_throttle_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.persist_ready_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.async_ready_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.clone_messages_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.enqueue_apply_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.async_message_loop_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.outbox_append_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.raft_advance_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.inline_apply_flush_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.inline_outbox_drain_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(ready.inline_transport_flush_elapsed_ns, std.time.ns_per_ms),
        },
    );
    const persist = ready.persist_ready_detail;
    std.log.warn(
        "metadata raft ready persist detail group_id={d} skipped_no_durable_state={} used_batch={} used_group_storage={} storage_apply_ms={d} encode_ms={d} wal_append_ms={d} wal_wait_ms={d} wal_coalesce_ms={d} wal_txn_open_ms={d} wal_put_ms={d} wal_commit_ms={d} wal_physical_commits={d} wal_inner_segment_syncs={d} wal_inner_index_syncs={d} wal_post_commit_segment_syncs={d} wal_post_commit_index_syncs={d} encoded_bytes={d} replay_debt_records={d} replay_debt_bytes={d}",
        .{
            ready.group_id,
            persist.skipped_no_durable_state,
            persist.used_batch,
            persist.used_group_storage,
            @divTrunc(persist.storage_apply_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(persist.encode_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(persist.wal_append_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(persist.wal_wait_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(persist.wal_coalesce_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(persist.wal_txn_open_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(persist.wal_put_elapsed_ns, std.time.ns_per_ms),
            @divTrunc(persist.wal_commit_elapsed_ns, std.time.ns_per_ms),
            persist.wal_physical_commits,
            persist.wal_inner_segment_syncs,
            persist.wal_inner_index_syncs,
            persist.wal_post_commit_segment_syncs,
            persist.wal_post_commit_index_syncs,
            persist.encoded_bytes,
            persist.delta_records_since_checkpoint,
            persist.delta_bytes_since_checkpoint,
        },
    );
    std.log.warn(
        "metadata raft ready pressure group_id={d} ready_messages={d} ready_message_bytes={d} ready_committed_entries={d} ready_committed_bytes={d} ready_unstable_entries={d} ready_unstable_bytes={d} ready_read_states={d} ready_has_snapshot={} ready_snapshot_bytes={d} ready_async_storage={} ready_processed={} ready_denied_backpressure={} ready_denied_transport_capacity={} ready_denied_apply_capacity={} ready_denied_snapshot_throttle={} ready_has_more={}",
        .{
            ready.group_id,
            ready.message_count,
            ready.message_bytes,
            ready.committed_entries,
            ready.committed_entry_bytes,
            ready.unstable_entries,
            ready.unstable_entry_bytes,
            ready.read_states,
            ready.has_snapshot,
            ready.snapshot_bytes,
            ready.async_storage_writes,
            ready.processed,
            ready.denied_by_backpressure,
            ready.denied_by_transport_capacity,
            ready.denied_by_apply_capacity,
            ready.denied_by_snapshot_throttle,
            ready.has_more_ready,
        },
    );
}

const MetadataRaftDiagnosticsSnapshot = struct {
    pending_updates: usize = 0,
    node_id: u64 = 0,
    role: []const u8 = "unknown",
    has_leader: bool = false,
    leader_id: u64 = 0,
    term: u64 = 0,
    commit_index: u64 = 0,
    applied_index: u64 = 0,
    last_index: u64 = 0,
    election_elapsed: u64 = 0,
    last_runtime_round: ?raft_engine.runtime.multi_raft.HostRound = null,
};

const MetadataRunRoundTrace = struct {
    const Phase = struct {
        name: []const u8,
        elapsed_ns: u64,
    };

    start_ns: u64,
    phases: [metadata_run_round_trace_max_phases]Phase = undefined,
    phase_count: usize = 0,
    dropped_phases: usize = 0,

    fn init() MetadataRunRoundTrace {
        return .{ .start_ns = platform_time.monotonicNs() };
    }

    fn recordSince(self: *MetadataRunRoundTrace, name: []const u8, start_ns: u64) void {
        const elapsed_ns = platform_time.monotonicNs() -| start_ns;
        if (self.phase_count < self.phases.len) {
            self.phases[self.phase_count] = .{ .name = name, .elapsed_ns = elapsed_ns };
            self.phase_count += 1;
        } else {
            self.dropped_phases += 1;
        }
        logMetadataRunRoundPhase(name, elapsed_ns);
    }

    fn logIfSlow(self: *const MetadataRunRoundTrace) void {
        const total_elapsed_ns = platform_time.monotonicNs() -| self.start_ns;
        if (total_elapsed_ns <= metadata_run_round_slow_threshold_ns) return;

        const top = self.topPhaseIndexes();
        const top0 = phaseOrEmpty(self, top[0]);
        const top1 = phaseOrEmpty(self, top[1]);
        const top2 = phaseOrEmpty(self, top[2]);
        std.log.warn(
            "metadata runRound phase summary slow elapsed_ms={d} phases={d} dropped_phases={d} top1_phase={s} top1_ms={d} top2_phase={s} top2_ms={d} top3_phase={s} top3_ms={d}",
            .{
                @divTrunc(total_elapsed_ns, std.time.ns_per_ms),
                self.phase_count,
                self.dropped_phases,
                top0.name,
                @divTrunc(top0.elapsed_ns, std.time.ns_per_ms),
                top1.name,
                @divTrunc(top1.elapsed_ns, std.time.ns_per_ms),
                top2.name,
                @divTrunc(top2.elapsed_ns, std.time.ns_per_ms),
            },
        );
    }

    fn topPhaseIndexes(self: *const MetadataRunRoundTrace) [3]?usize {
        var top: [3]?usize = .{ null, null, null };
        for (self.phases[0..self.phase_count], 0..) |phase, idx| {
            var insert_at: ?usize = null;
            for (top, 0..) |existing, top_idx| {
                if (existing == null or phase.elapsed_ns > self.phases[existing.?].elapsed_ns) {
                    insert_at = top_idx;
                    break;
                }
            }
            if (insert_at) |slot| {
                var move_idx: usize = top.len - 1;
                while (move_idx > slot) : (move_idx -= 1) {
                    top[move_idx] = top[move_idx - 1];
                }
                top[slot] = idx;
            }
        }
        return top;
    }

    fn phaseOrEmpty(self: *const MetadataRunRoundTrace, idx: ?usize) Phase {
        return if (idx) |value| self.phases[value] else .{ .name = "-", .elapsed_ns = 0 };
    }
};

const MetadataProposalProgressDriver = struct {
    lane: std.atomic.Mutex = .unlocked,
    wake_epoch: std.atomic.Value(u32) = .init(0),

    const Lease = struct {
        driver: *MetadataProposalProgressDriver,

        fn deinit(self: *Lease) void {
            self.driver.lane.unlock();
            self.driver.notifyWaiters();
            self.* = undefined;
        }
    };

    fn tryAcquire(self: *MetadataProposalProgressDriver) ?Lease {
        if (!self.lane.tryLock()) return null;
        return .{ .driver = self };
    }

    fn currentEpoch(self: *const MetadataProposalProgressDriver) u32 {
        return self.wake_epoch.load(.acquire);
    }

    fn waitForHandoff(self: *MetadataProposalProgressDriver, observed_epoch: u32, timeout_ns: u64) void {
        if (self.wake_epoch.load(.acquire) != observed_epoch) return;
        std.Io.futexWaitTimeout(
            std.Options.debug_io,
            u32,
            &self.wake_epoch.raw,
            observed_epoch,
            .{ .duration = .{
                .clock = .awake,
                .raw = .fromNanoseconds(@intCast(timeout_ns)),
            } },
        ) catch return;
    }

    fn notifyWaiters(self: *MetadataProposalProgressDriver) void {
        _ = self.wake_epoch.fetchAdd(1, .release);
        std.Io.futexWake(std.Options.debug_io, u32, &self.wake_epoch.raw, std.math.maxInt(u32));
    }
};

test "metadata proposal receipt progress uses a single transferable driver" {
    var driver = MetadataProposalProgressDriver{};
    var first = driver.tryAcquire() orelse return error.TestExpectedEqual;
    try std.testing.expect(driver.tryAcquire() == null);
    const observed_epoch = driver.currentEpoch();
    first.deinit();
    try std.testing.expect(driver.currentEpoch() != observed_epoch);

    var next = driver.tryAcquire() orelse return error.TestExpectedEqual;
    next.deinit();
}

const LifecycleSignal = struct {
    alloc: std.mem.Allocator,
    mutex: std.Io.Mutex = .init,
    wake_epoch: std.atomic.Value(u32) = .init(0),
    epoch: u64 = 0,
    table_epochs: std.StringHashMapUnmanaged(u64) = .empty,

    const Snapshot = struct {
        global_epoch: u64,
        table_name: ?[]const u8 = null,
        table_epoch: u64 = 0,
    };

    fn init(alloc: std.mem.Allocator) LifecycleSignal {
        return .{ .alloc = alloc };
    }

    fn current(self: *const LifecycleSignal) u32 {
        const mutable: *LifecycleSignal = @constCast(self);
        mutable.lock();
        defer mutable.unlock();
        return @intCast(mutable.epoch);
    }

    fn currentEpoch(self: *const LifecycleSignal) u64 {
        const mutable: *LifecycleSignal = @constCast(self);
        mutable.lock();
        defer mutable.unlock();
        return mutable.epoch;
    }

    fn snapshot(self: *LifecycleSignal, table_name: ?[]const u8) Snapshot {
        self.lock();
        defer self.unlock();
        return .{
            .global_epoch = self.epoch,
            .table_name = table_name,
            .table_epoch = if (table_name) |name| self.table_epochs.get(name) orelse 0 else 0,
        };
    }

    fn notify(self: *LifecycleSignal, table_name: ?[]const u8) void {
        self.lock();
        self.epoch +%= 1;
        if (table_name) |name| {
            const owned_name = self.table_epochs.getKey(name) orelse blk: {
                const duped = self.alloc.dupe(u8, name) catch {
                    self.unlockAndWake();
                    return;
                };
                self.table_epochs.put(self.alloc, duped, 0) catch {
                    self.alloc.free(duped);
                    self.unlockAndWake();
                    return;
                };
                break :blk self.table_epochs.getKey(name).?;
            };
            const current_epoch = self.table_epochs.get(owned_name) orelse 0;
            self.table_epochs.putAssumeCapacity(owned_name, current_epoch +% 1);
        }
        self.unlockAndWake();
    }

    fn wait(self: *LifecycleSignal, observed: Snapshot, timeout_ns: u64) void {
        const start_ns = platform_time.monotonicNs();
        while (true) {
            self.lock();
            if (self.changedLocked(observed)) {
                self.unlock();
                return;
            }
            const wake_epoch = self.wake_epoch.load(.acquire);
            self.unlock();

            const elapsed_ns = platform_time.monotonicNs() -| start_ns;
            if (elapsed_ns >= timeout_ns) return;
            const remaining_ns = timeout_ns - elapsed_ns;
            std.Io.futexWaitTimeout(
                std.Options.debug_io,
                u32,
                &self.wake_epoch.raw,
                wake_epoch,
                .{ .duration = .{
                    .clock = .awake,
                    .raw = .fromNanoseconds(@intCast(remaining_ns)),
                } },
            ) catch return;
        }
    }

    fn changedLocked(self: *const LifecycleSignal, observed: Snapshot) bool {
        if (self.epoch != observed.global_epoch) return true;
        if (observed.table_name) |name| {
            return (self.table_epochs.get(name) orelse 0) != observed.table_epoch;
        }
        return false;
    }

    fn deinit(self: *LifecycleSignal) void {
        var it = self.table_epochs.iterator();
        while (it.next()) |entry| self.alloc.free(entry.key_ptr.*);
        self.table_epochs.deinit(self.alloc);
        self.* = undefined;
    }

    fn lock(self: *LifecycleSignal) void {
        self.mutex.lockUncancelable(std.Options.debug_io);
    }

    fn unlock(self: *LifecycleSignal) void {
        self.mutex.unlock(std.Options.debug_io);
    }

    fn unlockAndWake(self: *LifecycleSignal) void {
        self.unlock();
        _ = self.wake_epoch.fetchAdd(1, .release);
        std.Io.futexWake(std.Options.debug_io, u32, &self.wake_epoch.raw, std.math.maxInt(u32));
    }
};

pub const LifecycleReconcileHook = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        run: *const fn (ptr: *anyopaque) anyerror!void,
    };

    pub fn run(self: LifecycleReconcileHook) !void {
        try self.vtable.run(self.ptr);
    }
};

pub const LocalReplicaRootReconcileHook = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const Request = struct {
        metadata_group_id: u64,
        group_ids: []const u64,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
    };

    pub const VTable = struct {
        run: *const fn (ptr: *anyopaque, request: Request) anyerror!metadata_table_provisioner.ProvisionSummary,
    };

    pub fn run(self: LocalReplicaRootReconcileHook, request: Request) !metadata_table_provisioner.ProvisionSummary {
        return try self.vtable.run(self.ptr, request);
    }
};

pub const LocalReplicaRootReconcilePermitHook = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        should_reconcile: *const fn (ptr: *anyopaque) bool,
    };

    pub fn shouldReconcile(self: LocalReplicaRootReconcilePermitHook) bool {
        return self.vtable.should_reconcile(self.ptr);
    }
};

pub const MetadataServiceConfig = struct {
    raft: raft_service.ManagedServiceConfig = .{
        .max_inbound_messages = 128,
        .max_tick_groups = 8,
        .max_ready_steps = 8,
    },
    reconcile_lease: metadata_reconcile_lease.Config = .{},
    observe_local_replica_root: bool = true,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    secret_store: ?*common_secrets.FileStore = null,
    internal_service_secret: ?[]const u8 = null,
    internal_service_issuer: ?[]const u8 = null,
    destination_authorizer: ?stored_destination_authorization.Authorizer = null,
    reallocation_protocol_peers: []const ReallocationProtocolPeer = &.{},
};

pub const ReallocationProtocolPeer = struct {
    node_id: u64,
    orchestration_url: ?[]const u8 = null,
};

/// Maps a non-leader node's Raft observation to a table-mutation route. Every
/// blocked outcome is `error.NotLeader`: a provably pre-admission authority
/// failure that a load-balanced public caller may safely retry elsewhere.
fn tableMutationRouteFromObservation(
    observation: ServiceGroupRaftObservation,
    peers: []const ReallocationProtocolPeer,
) !MetadataHttpService.TableMutationRoute {
    const leader_id = observation.leader_id orelse return error.NotLeader;
    // The caller already failed the authoritative local-leader check; a stale
    // observation naming this node must not forward to itself.
    if (leader_id == observation.local_node_id) return error.NotLeader;
    const peer = findReallocationProtocolPeer(peers, leader_id) orelse {
        std.log.warn("table mutation routing blocked: metadata leader {d} is not in the configured peer set", .{leader_id});
        return error.NotLeader;
    };
    const orchestration_url = peer.orchestration_url orelse {
        std.log.warn("table mutation routing blocked: metadata leader {d} has no orchestration URL", .{leader_id});
        return error.NotLeader;
    };
    if (orchestration_url.len == 0) {
        std.log.warn("table mutation routing blocked: metadata leader {d} has an empty orchestration URL", .{leader_id});
        return error.NotLeader;
    }
    return .{ .forward = peer };
}

test "metadata.table mutation routing forwards only to a routable remote leader" {
    const peers = [_]ReallocationProtocolPeer{
        .{ .node_id = 2, .orchestration_url = "http://metadata-1:8080" },
        .{ .node_id = 3, .orchestration_url = null },
        .{ .node_id = 4, .orchestration_url = "" },
    };

    const forwarded = try tableMutationRouteFromObservation(
        .{ .local_node_id = 1, .leader_id = 2 },
        &peers,
    );
    try std.testing.expectEqual(@as(u64, 2), forwarded.forward.node_id);
    try std.testing.expectEqualStrings("http://metadata-1:8080", forwarded.forward.orchestration_url.?);

    // No known leader yet.
    try std.testing.expectError(error.NotLeader, tableMutationRouteFromObservation(
        .{ .local_node_id = 1, .leader_id = null },
        &peers,
    ));
    // Stale observation naming the local node must not self-forward.
    try std.testing.expectError(error.NotLeader, tableMutationRouteFromObservation(
        .{ .local_node_id = 1, .leader_id = 1 },
        &peers,
    ));
    // Leader outside the configured peer set.
    try std.testing.expectError(error.NotLeader, tableMutationRouteFromObservation(
        .{ .local_node_id = 1, .leader_id = 9 },
        &peers,
    ));
    // Leader without a usable orchestration URL.
    try std.testing.expectError(error.NotLeader, tableMutationRouteFromObservation(
        .{ .local_node_id = 1, .leader_id = 3 },
        &peers,
    ));
    try std.testing.expectError(error.NotLeader, tableMutationRouteFromObservation(
        .{ .local_node_id = 1, .leader_id = 4 },
        &peers,
    ));
}

fn findReallocationProtocolPeer(peers: []const ReallocationProtocolPeer, node_id: u64) ?ReallocationProtocolPeer {
    for (peers) |peer| {
        if (peer.node_id == node_id) return peer;
    }
    return null;
}

fn collectReallocationBarrierNodeIds(
    alloc: std.mem.Allocator,
    conf_state: raft_engine.core.ConfState,
    configured_peers: []const ReallocationProtocolPeer,
) ![]u64 {
    var required = std.AutoHashMapUnmanaged(u64, void).empty;
    defer required.deinit(alloc);

    const membership_sets = [_][]const u64{
        conf_state.voters,
        conf_state.voters_outgoing,
        conf_state.learners,
        conf_state.learners_next,
    };
    for (membership_sets) |node_ids| {
        for (node_ids) |node_id| try required.put(alloc, node_id, {});
    }
    // The configured peer set is the target membership for this process.
    // Probing it as well as the currently applied ConfState prevents a
    // compatible request from racing a configured learner admission and later
    // being exposed to an older leader after promotion.
    for (configured_peers) |peer| try required.put(alloc, peer.node_id, {});

    const node_ids = try alloc.alloc(u64, required.count());
    var index: usize = 0;
    var it = required.keyIterator();
    while (it.next()) |node_id| : (index += 1) node_ids[index] = node_id.*;
    std.mem.sort(u64, node_ids, {}, std.sort.asc(u64));
    return node_ids;
}

const ReallocationBarrierContract = struct {
    metadata_incarnation: metadata_mod.MetadataClusterIncarnation,
    protected_metadata_member_count: u32,
    protected_metadata_membership_fingerprint: metadata_reallocation_request.MembershipFingerprint,
};

fn reallocationBarrierContract(
    incarnation: metadata_mod.MetadataClusterIncarnation,
    sorted_node_ids: []const u64,
) !ReallocationBarrierContract {
    if (!metadata_mod.incarnation.isValid(incarnation) or sorted_node_ids.len == 0) {
        return error.ReallocationProtocolUpgradeRequired;
    }
    return .{
        .metadata_incarnation = incarnation,
        .protected_metadata_member_count = std.math.cast(u32, sorted_node_ids.len) orelse
            return error.ReallocationProtocolUpgradeRequired,
        .protected_metadata_membership_fingerprint = metadata_reallocation_request.membershipFingerprint(sorted_node_ids),
    };
}

fn reallocationBarrierStatusCompatible(
    peer_status: MetadataStatus,
    metadata_group_id: u64,
    node_id: u64,
    incarnation: metadata_mod.MetadataClusterIncarnation,
) bool {
    const peer_incarnation = peer_status.metadata_incarnation orelse return false;
    return peer_status.metadata_group_id == metadata_group_id and
        peer_status.metadata_raft_local_node_id == node_id and
        std.mem.eql(u8, &peer_incarnation, &incarnation) and
        peer_status.reallocation_barrier_protocol_version >= metadata_reallocation_request.barrier_protocol_version;
}

fn runtimeStatusProtocolCompatible(
    peer_status: MetadataStatus,
    metadata_group_id: u64,
    node_id: u64,
    incarnation: metadata_mod.MetadataClusterIncarnation,
) bool {
    const peer_incarnation = peer_status.metadata_incarnation orelse return false;
    return peer_status.metadata_group_id == metadata_group_id and
        peer_status.metadata_raft_local_node_id == node_id and
        std.mem.eql(u8, &peer_incarnation, &incarnation) and
        peer_status.runtime_status_record_version >= metadata_runtime_status_protocol.current_record_version;
}

const RuntimeStatusMembershipFingerprint = [std.crypto.hash.sha2.Sha256.digest_length]u8;

fn runtimeStatusProtocolMembershipFingerprint(
    conf_state: raft_engine.core.ConfState,
    configured_peers: []const ReallocationProtocolPeer,
    incarnation: metadata_mod.MetadataClusterIncarnation,
) RuntimeStatusMembershipFingerprint {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(&incarnation);
    const membership_sets = [_][]const u64{
        conf_state.voters,
        conf_state.voters_outgoing,
        conf_state.learners,
        conf_state.learners_next,
    };
    for (membership_sets, 0..) |node_ids, set_index| {
        const tag: u8 = @intCast(set_index);
        const node_count: u64 = @intCast(node_ids.len);
        hasher.update(&.{tag});
        hasher.update(std.mem.asBytes(&node_count));
        for (node_ids) |node_id| hasher.update(std.mem.asBytes(&node_id));
    }
    hasher.update(&.{0xff});
    const configured_peer_count: u64 = @intCast(configured_peers.len);
    hasher.update(std.mem.asBytes(&configured_peer_count));
    for (configured_peers) |peer| hasher.update(std.mem.asBytes(&peer.node_id));
    var fingerprint: RuntimeStatusMembershipFingerprint = undefined;
    hasher.final(&fingerprint);
    return fingerprint;
}

fn runtimeStatusProtocolMembershipIsLocalOnly(
    conf_state: raft_engine.core.ConfState,
    configured_peers: []const ReallocationProtocolPeer,
    local_node_id: u64,
) bool {
    var found_local = false;
    const membership_sets = [_][]const u64{
        conf_state.voters,
        conf_state.voters_outgoing,
        conf_state.learners,
        conf_state.learners_next,
    };
    for (membership_sets) |node_ids| {
        for (node_ids) |node_id| {
            if (node_id != local_node_id) return false;
            found_local = true;
        }
    }
    for (configured_peers) |peer| {
        if (peer.node_id != local_node_id) return false;
        found_local = true;
    }
    return found_local;
}

fn runtimeStatusProtocolProbeBackoffNs(failures: u32) u64 {
    if (failures == 0) return 0;
    const shift: u6 = @intCast(@min(failures - 1, 4));
    return @min(
        runtime_status_protocol_probe_min_backoff_ns << shift,
        runtime_status_protocol_probe_max_backoff_ns,
    );
}

fn storeHasRuntimeRepairStatus(record: metadata_table_manager.StoreRecord) bool {
    for (record.runtime_statuses) |runtime_status| {
        for (runtime_status.indexes) |index_status| {
            if (index_status.repair_status != null) return true;
        }
    }
    return false;
}

fn reportsHaveRuntimeRepairStatus(reports: []const metadata_table_manager.StoreStatusReport) bool {
    for (reports) |report| {
        for (report.runtime_statuses) |runtime_status| {
            for (runtime_status.indexes) |index_status| {
                if (index_status.repair_status != null) return true;
            }
        }
    }
    return false;
}

fn storesHaveRuntimeRepairStatus(stores: []const metadata_table_manager.StoreRecord) bool {
    for (stores) |store| {
        if (storeHasRuntimeRepairStatus(store)) return true;
    }
    return false;
}

fn storesHaveRuntimeReporterFence(stores: []const metadata_table_manager.StoreRecord) bool {
    for (stores) |store| {
        if (store.reporter_incarnation != 0) return true;
    }
    return false;
}

fn reportsHaveRuntimeReporterFence(reports: []const metadata_table_manager.StoreStatusReport) bool {
    for (reports) |report| {
        if (report.reporter_incarnation != 0) return true;
    }
    return false;
}

fn stripRuntimeRepairStatus(record: *metadata_table_manager.StoreRecord) void {
    for (record.runtime_statuses) |*runtime_status| {
        for (runtime_status.indexes) |*index_status| {
            index_status.repair_status = null;
            index_status.repair_active_generation_serviceable = false;
        }
    }
}

fn stripRuntimeReporterFence(record: *metadata_table_manager.StoreRecord) void {
    record.reporter_incarnation = 0;
    record.status_generation = 0;
}

fn runtimeStatusProtocolSafeCommand(
    service: anytype,
    command: metadata_storage.TransitionCommand,
    owned_legacy_store: *?metadata_table_manager.StoreRecord,
) !metadata_storage.TransitionCommand {
    switch (command) {
        .upsert_store => |record| {
            if (!metadata_table_manager.reporterFenceValid(
                record.reporter_incarnation,
                record.status_generation,
            )) return error.InvalidStoreReporterFence;
            const needs_current_protocol = storeHasRuntimeRepairStatus(record) or
                record.reporter_incarnation != 0 or record.status_generation != 0;
            if (!needs_current_protocol or service.runtimeStatusRepairProtocolReady()) return command;
            // Unlike registration, an upsert carrying an incarnation may be a
            // clone of already-committed v13 state. Downgrading it would erase
            // causal authority. Retry after readiness can be proven instead.
            if (record.reporter_incarnation != 0) return error.RuntimeStatusProtocolUnavailable;
            var legacy_record = try metadata_table_manager.cloneStore(service.alloc, record);
            stripRuntimeRepairStatus(&legacy_record);
            stripRuntimeReporterFence(&legacy_record);
            owned_legacy_store.* = legacy_record;
            return .{ .upsert_store = legacy_record };
        },
        .register_store => |record| {
            if (!metadata_table_manager.reporterFenceValid(
                record.reporter_incarnation,
                record.status_generation,
            )) return error.InvalidStoreReporterFence;
            const needs_current_protocol = storeHasRuntimeRepairStatus(record) or
                record.reporter_incarnation != 0 or record.status_generation != 0;
            if (!needs_current_protocol or service.runtimeStatusRepairProtocolReady()) return command;
            var legacy_record = try metadata_table_manager.cloneStore(service.alloc, record);
            stripRuntimeRepairStatus(&legacy_record);
            stripRuntimeReporterFence(&legacy_record);
            owned_legacy_store.* = legacy_record;
            return .{ .register_store = legacy_record };
        },
        else => return command,
    }
}

pub const MetadataServiceDeps = struct {
    host: raft_managed_host.ManagedHostDeps = .{},
    raft: raft_service.ManagedServiceDeps = .{},
};

pub const MetadataHttpServiceDeps = struct {
    http: raft_managed_host.ManagedHttpHostDeps = .{},
    raft: raft_service.ManagedServiceDeps = .{},
};

pub const MetadataStatus = metadata_api.MetadataStatus;

fn applyTransitionMetrics(
    target: *raft_service.ManagedServiceMetrics,
    source: raft_transition_service.TransitionServiceMetrics,
) void {
    target.queued_split_transitions = source.queued_split_transitions;
    target.queued_merge_transitions = source.queued_merge_transitions;
    target.stepped_split_transitions = source.stepped_split_transitions;
    target.stepped_merge_transitions = source.stepped_merge_transitions;
    target.completed_split_transitions = source.completed_split_transitions;
    target.completed_merge_transitions = source.completed_merge_transitions;
    target.awaiting_split_source_start = source.awaiting_split_source_start;
    target.bootstrapping_split_destination = source.bootstrapping_split_destination;
    target.split_replay_blocked = source.split_replay_blocked;
    target.split_ready_to_finalize = source.split_ready_to_finalize;
    target.awaiting_merge_receiver_acceptance = source.awaiting_merge_receiver_acceptance;
    target.bootstrapping_merge_receiver = source.bootstrapping_merge_receiver;
    target.merge_replay_blocked = source.merge_replay_blocked;
    target.merge_ready_to_finalize = source.merge_ready_to_finalize;
}

const LinearizableMetadataReadTracker = struct {
    alloc: std.mem.Allocator,
    metadata_group_id: raft_engine.core.types.GroupId,
    mutex: std.Io.Mutex = .init,
    next_request_id: std.atomic.Value(u64) = .init(1),
    requests: std.AutoHashMapUnmanaged(u64, bool) = .empty,
    downstream: ?raft_state_machine.ReadStateObserver = null,

    fn deinit(self: *@This()) void {
        self.requests.deinit(self.alloc);
        self.* = undefined;
    }

    fn observer(self: *@This()) raft_state_machine.ReadStateObserver {
        return .{
            .ptr = self,
            .vtable = &.{
                .on_read_states = onReadStates,
            },
        };
    }

    fn registerRequest(self: *@This()) !u64 {
        const request_id = self.next_request_id.fetchAdd(1, .monotonic);
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        try self.requests.put(self.alloc, request_id, false);
        return request_id;
    }

    fn isComplete(self: *@This(), request_id: u64) bool {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        return if (self.requests.get(request_id)) |complete| complete else false;
    }

    fn markComplete(self: *@This(), request_id: u64) void {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        if (self.requests.getPtr(request_id)) |complete| complete.* = true;
    }

    fn finishRequest(self: *@This(), request_id: u64) void {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        _ = self.requests.remove(request_id);
    }

    fn onReadStates(
        ptr: *anyopaque,
        group_id: raft_engine.core.types.GroupId,
        read_states: []const raft_engine.core.ReadState,
    ) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (group_id == self.metadata_group_id) {
            for (read_states) |read_state| {
                if (!std.mem.startsWith(u8, read_state.request_ctx, linearizable_metadata_read_prefix)) continue;
                const suffix = read_state.request_ctx[linearizable_metadata_read_prefix.len..];
                const request_id = std.fmt.parseUnsigned(u64, suffix, 10) catch continue;
                self.markComplete(request_id);
            }
        }
        if (self.downstream) |downstream| try downstream.onReadStates(group_id, read_states);
    }
};

fn projectionSignalChangesCatalog(kind: metadata_storage.raft_apply_store.ProjectionSignalKind) bool {
    return switch (kind) {
        .table, .range => true,
        else => false,
    };
}

fn projectionSignalChangesProjectedCore(kind: metadata_storage.raft_apply_store.ProjectionSignalKind) bool {
    return switch (kind) {
        .store, .shuffle_join_lease, .schema_progress, .restore_progress, .replication_source_status => true,
        else => false,
    };
}

fn projectionSignalChangesTransitionReadiness(kind: metadata_storage.raft_apply_store.ProjectionSignalKind) bool {
    return switch (kind) {
        .store, .placement_intent => true,
        else => false,
    };
}

test "metadata service catalog validation epoch ignores non-catalog projection traffic" {
    try std.testing.expect(projectionSignalChangesCatalog(.table));
    try std.testing.expect(projectionSignalChangesCatalog(.range));
    try std.testing.expect(!projectionSignalChangesCatalog(.store));
    try std.testing.expect(!projectionSignalChangesCatalog(.reconcile_lease));
    try std.testing.expect(!projectionSignalChangesCatalog(.shuffle_join_lease));
    try std.testing.expect(!projectionSignalChangesCatalog(.schema_progress));
    try std.testing.expect(!projectionSignalChangesCatalog(.restore_progress));
    try std.testing.expect(!projectionSignalChangesCatalog(.restore_job));
    try std.testing.expect(!projectionSignalChangesCatalog(.replication_source_status));
    try std.testing.expect(!projectionSignalChangesCatalog(.placement_intent));
    try std.testing.expect(!projectionSignalChangesCatalog(.split_transition));
    try std.testing.expect(!projectionSignalChangesCatalog(.merge_transition));

    try std.testing.expect(!projectionSignalChangesProjectedCore(.table));
    try std.testing.expect(!projectionSignalChangesProjectedCore(.range));
    try std.testing.expect(projectionSignalChangesProjectedCore(.store));
    try std.testing.expect(projectionSignalChangesProjectedCore(.shuffle_join_lease));
    try std.testing.expect(projectionSignalChangesProjectedCore(.schema_progress));
    try std.testing.expect(projectionSignalChangesProjectedCore(.restore_progress));
    try std.testing.expect(projectionSignalChangesProjectedCore(.replication_source_status));
    try std.testing.expect(!projectionSignalChangesProjectedCore(.placement_intent));
    try std.testing.expect(!projectionSignalChangesProjectedCore(.reconcile_lease));
    try std.testing.expect(!projectionSignalChangesProjectedCore(.split_transition));
    try std.testing.expect(!projectionSignalChangesProjectedCore(.merge_transition));
    try std.testing.expect(!projectionSignalChangesProjectedCore(.restore_job));
}

// Backfill marker discovery does not need sub-second polling when the system is
// otherwise idle. Keep active-marker refreshes fast, but back off empty-root
// probes so they do not add filesystem churn on the read hot path.
const store_status_backfill_probe_interval_ticks: usize = 200;
const store_status_backfill_rescan_interval_ms: u64 = std.time.ms_per_s;
const store_status_backfill_empty_rescan_interval_ms: u64 = 30 * std.time.ms_per_s;
const local_placement_refresh_interval_ms: u64 = 5 * std.time.ms_per_s;
const local_transition_refresh_interval_ms: u64 = 5 * std.time.ms_per_s;
// These scan projected tables/ranges and local replica files. Epoch/group-id
// changes bypass the interval, so steady-state polling can be less aggressive
// without delaying structural metadata changes.
const local_schema_progress_refresh_interval_ms: u64 = 30 * std.time.ms_per_s;
const local_table_provisioning_refresh_interval_ms: u64 = 30 * std.time.ms_per_s;
const reconcile_lease_probe_interval_ms: u64 = 250;
const metadata_status_cache_refresh_interval_ms: u64 = 5 * std.time.ms_per_s;

const ReconcileLeaseProjectionCache = struct {
    epoch: ?u64 = null,
    record: ?metadata_reconcile_lease.ReconcileLeaseRecord = null,
    next_refresh_at_ms: u64 = 0,
};

pub const LocalGroupStatusProvider = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        collect: *const fn (
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            replica_root_dir: []const u8,
            tables: []const metadata_table_manager.TableRecord,
            ranges: []const metadata_table_manager.RangeRecord,
            stores: []const metadata_table_manager.StoreRecord,
            merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
            split_transitions: []const transition_state.SplitTransitionRecord,
            merge_transitions: []const transition_state.MergeTransitionRecord,
            split_observations: []const transition_state.SplitObservationRecord,
            merge_observations: []const transition_state.MergeObservationRecord,
        ) anyerror![]metadata_table_manager.GroupStatusReport,
    };

    pub fn collect(
        self: LocalGroupStatusProvider,
        alloc: std.mem.Allocator,
        replica_root_dir: []const u8,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        stores: []const metadata_table_manager.StoreRecord,
        merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
        split_transitions: []const transition_state.SplitTransitionRecord,
        merge_transitions: []const transition_state.MergeTransitionRecord,
        split_observations: []const transition_state.SplitObservationRecord,
        merge_observations: []const transition_state.MergeObservationRecord,
    ) ![]metadata_table_manager.GroupStatusReport {
        return try self.vtable.collect(
            self.ptr,
            alloc,
            replica_root_dir,
            tables,
            ranges,
            stores,
            merged_group_statuses,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        );
    }
};

fn nowMs() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

fn groupIdsFingerprint(group_ids: []const u64) u64 {
    return std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(group_ids));
}

const LocalProjectionInputs = struct {
    group_ids: []u64,
    group_ids_fingerprint: u64,
    tables: []metadata_table_manager.TableRecord,
    ranges: []metadata_table_manager.RangeRecord,
    stores: []metadata_table_manager.StoreRecord,
    schema_progresses: []metadata_table_manager.SchemaProgressRecord,
    restore_progresses: []metadata_table_manager.RestoreProgressRecord,
};

const ProjectedCoreSnapshot = struct {
    stores: []metadata_table_manager.StoreRecord = &.{},
    placement_intents: []raft_reconciler.PlacementIntent = &.{},
    placement_version_fences: []metadata_reconciler.PlacementVersionFence = &.{},
    shuffle_join_leases: []metadata_table_manager.ShuffleJoinLeaseRecord = &.{},
    schema_progresses: []metadata_table_manager.SchemaProgressRecord = &.{},
    restore_progresses: []metadata_table_manager.RestoreProgressRecord = &.{},
    replication_source_statuses: []metadata_table_manager.ReplicationSourceStatusRecord = &.{},
    split_transitions: []transition_state.SplitTransitionRecord = &.{},
    merge_transitions: []transition_state.MergeTransitionRecord = &.{},

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.stores) |record| metadata_table_manager.freeStore(alloc, record);
        if (self.stores.len > 0) alloc.free(self.stores);
        for (self.placement_intents) |intent| alloc.free(intent.peer_node_ids);
        if (self.placement_intents.len > 0) alloc.free(self.placement_intents);
        if (self.placement_version_fences.len > 0) alloc.free(self.placement_version_fences);
        if (self.shuffle_join_leases.len > 0) alloc.free(self.shuffle_join_leases);
        if (self.schema_progresses.len > 0) alloc.free(self.schema_progresses);
        for (self.restore_progresses) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
        if (self.restore_progresses.len > 0) alloc.free(self.restore_progresses);
        for (self.replication_source_statuses) |record| metadata_table_manager.freeReplicationSourceStatus(alloc, record);
        if (self.replication_source_statuses.len > 0) alloc.free(self.replication_source_statuses);
        for (self.split_transitions) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
        if (self.split_transitions.len > 0) alloc.free(self.split_transitions);
        for (self.merge_transitions) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
        if (self.merge_transitions.len > 0) alloc.free(self.merge_transitions);
        self.* = undefined;
    }

    fn diagnostics(self: *const @This()) ProjectedCoreSnapshotDiagnostics {
        var out = ProjectedCoreSnapshotDiagnostics{
            .cached = true,
            .stores = self.stores.len,
            .store_group_statuses = 0,
            .store_runtime_statuses = 0,
            .placement_intents = self.placement_intents.len,
            .shuffle_join_leases = self.shuffle_join_leases.len,
            .schema_progresses = self.schema_progresses.len,
            .restore_progresses = self.restore_progresses.len,
            .replication_source_statuses = self.replication_source_statuses.len,
            .split_transitions = self.split_transitions.len,
            .merge_transitions = self.merge_transitions.len,
            .estimated_bytes = @sizeOf(metadata_table_manager.StoreRecord) * self.stores.len +
                @sizeOf(raft_reconciler.PlacementIntent) * self.placement_intents.len +
                @sizeOf(metadata_reconciler.PlacementVersionFence) * self.placement_version_fences.len +
                @sizeOf(metadata_table_manager.ShuffleJoinLeaseRecord) * self.shuffle_join_leases.len +
                @sizeOf(metadata_table_manager.SchemaProgressRecord) * self.schema_progresses.len +
                @sizeOf(metadata_table_manager.RestoreProgressRecord) * self.restore_progresses.len +
                @sizeOf(metadata_table_manager.ReplicationSourceStatusRecord) * self.replication_source_statuses.len +
                @sizeOf(transition_state.SplitTransitionRecord) * self.split_transitions.len +
                @sizeOf(transition_state.MergeTransitionRecord) * self.merge_transitions.len,
        };
        for (self.stores) |record| {
            out.store_group_statuses += record.group_statuses.len;
            out.store_runtime_statuses += record.runtime_statuses.len;
            out.estimated_bytes += record.api_url.len + record.raft_url.len + record.role.len +
                record.health_class.len + record.failure_domain.len +
                @sizeOf(metadata_table_manager.GroupStatusReport) * record.group_statuses.len +
                @sizeOf(metadata_table_manager.RuntimeGroupStatusReport) * record.runtime_statuses.len;
            for (record.runtime_statuses) |status| {
                out.estimated_bytes += status.table_name.len + status.source.len + status.freshness.len +
                    status.enrichment.projection_checkpoint_status.len +
                    @sizeOf(metadata_table_manager.RuntimeIndexStatusReport) * status.indexes.len;
                for (status.indexes) |index| out.estimated_bytes += index.name.len + index.kind.len;
            }
        }
        for (self.placement_intents) |intent| {
            out.estimated_bytes += @sizeOf(u64) * intent.peer_node_ids.len;
            if (intent.record.snapshot_bootstrap) |record| out.estimated_bytes += record.snapshot_id.len + record.uri.len;
            if (intent.record.backup_restore_bootstrap) |record| {
                out.estimated_bytes += record.backup_id.len + record.artifact_backup_id.len +
                    record.location.len + record.snapshot_path.len + record.connection.len +
                    record.artifact_sha256.len;
            }
        }
        for (self.restore_progresses) |record| {
            out.estimated_bytes += record.backup_id.len + record.artifact_backup_id.len +
                record.location.len + record.snapshot_path.len + record.artifact_sha256.len +
                record.phase.len + record.last_error.len;
        }
        for (self.replication_source_statuses) |record| {
            out.estimated_bytes += record.source_kind.len + record.external_table.len + record.cutover_mode.len +
                record.slot_name.len + record.publication_name.len + record.phase.len + record.checkpoint.len +
                record.prepared_checkpoint.len + record.stream_checkpoint.len + record.last_error.len + record.failure_class.len;
        }
        for (self.split_transitions) |record| {
            out.estimated_bytes += optionalLen(record.split_key) + optionalLen(record.source_range_end) + optionalLen(record.rollback_reason);
        }
        for (self.merge_transitions) |record| out.estimated_bytes += optionalLen(record.rollback_reason);
        return out;
    }
};

const CatalogValidationSnapshot = struct {
    tables: []metadata_table_manager.TableRecord = &.{},
    ranges: []metadata_table_manager.RangeRecord = &.{},
    index: metadata_api.CatalogProjectionIndex = .{},

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.index.deinit(alloc);
        for (self.tables) |record| metadata_table_manager.freeTable(alloc, record);
        if (self.tables.len > 0) alloc.free(self.tables);
        for (self.ranges) |record| metadata_table_manager.freeRange(alloc, record);
        if (self.ranges.len > 0) alloc.free(self.ranges);
        self.* = .{};
    }

    fn addDiagnostics(self: *const @This(), out: *ProjectedCoreSnapshotDiagnostics) void {
        out.cached = true;
        out.tables = self.tables.len;
        out.ranges = self.ranges.len;
        out.estimated_bytes += @sizeOf(metadata_table_manager.TableRecord) * self.tables.len +
            @sizeOf(metadata_table_manager.RangeRecord) * self.ranges.len;
        for (self.tables) |record| {
            out.estimated_bytes += record.name.len + record.description.len + record.schema_json.len +
                record.read_schema_json.len + record.indexes_json.len + record.replication_sources_json.len +
                record.placement_role.len + record.restore_backup_id.len + record.restore_location.len;
        }
        for (self.ranges) |record| {
            out.estimated_bytes += record.start_key.len + optionalLen(record.end_key) +
                record.restore_backup_id.len + record.restore_artifact_backup_id.len +
                record.restore_location.len + record.restore_snapshot_path.len +
                record.restore_connection.len + record.restore_artifact_sha256.len;
        }
    }
};

const CatalogValidationSnapshotCache = struct {
    catalog_epoch: u64 = 0,
    snapshot: ?CatalogValidationSnapshot = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.snapshot) |*snapshot| snapshot.deinit(alloc);
        self.* = .{};
    }
};

const ProjectedCoreSnapshotCache = struct {
    core_epoch: u64 = 0,
    placement_epoch: u64 = 0,
    transition_epoch: u64 = 0,
    snapshot: ?ProjectedCoreSnapshot = null,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        if (self.snapshot) |*snapshot| snapshot.deinit(alloc);
        self.* = .{};
    }
};

const TransitionReadinessCache = struct {
    initialized: bool = false,
    epoch: u64 = 0,
    ready_by_group: std.AutoHashMapUnmanaged(u64, transition_state.StablePlacementReadiness) = .empty,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        self.ready_by_group.deinit(alloc);
        self.* = .{};
    }
};

const TransitionPlacementKey = struct {
    group_id: u64,
    store_id: u64,
};

const TransitionReadinessInputs = struct {
    stores: []metadata_table_manager.StoreRecord,
    placement_intents: []raft_reconciler.PlacementIntent,

    fn deinit(self: *@This(), alloc: std.mem.Allocator) void {
        for (self.stores) |store| metadata_table_manager.freeStore(alloc, store);
        alloc.free(self.stores);
        for (self.placement_intents) |intent| raft_reconciler.freeIntentOwned(alloc, intent);
        alloc.free(self.placement_intents);
        self.* = undefined;
    }
};

const ProjectedCoreSnapshotDiagnostics = struct {
    cached: bool = false,
    tables: usize = 0,
    ranges: usize = 0,
    stores: usize = 0,
    store_group_statuses: usize = 0,
    store_runtime_statuses: usize = 0,
    placement_intents: usize = 0,
    shuffle_join_leases: usize = 0,
    schema_progresses: usize = 0,
    restore_progresses: usize = 0,
    replication_source_statuses: usize = 0,
    split_transitions: usize = 0,
    merge_transitions: usize = 0,
    estimated_bytes: usize = 0,
};

pub const JsonResponseDiagnostics = struct {
    calls: u64 = 0,
    bytes_total: u64 = 0,
    peak_bytes: u64 = 0,
};

pub const LsmRetentionDiagnostics = struct {
    mutable_bytes: u64 = 0,
    immutable_bytes: u64 = 0,
    total_run_bytes: u64 = 0,
    wal_retained_bytes: u64 = 0,
    wal_retained_segments: u64 = 0,
    active_readers: u64 = 0,
    obsolete_paths: u64 = 0,
    obsolete_paths_pinned_by_readers: u64 = 0,
    obsolete_paths_pinned_by_versions: u64 = 0,
    bulk_ingest_current_scan_clone_active_bytes: u64 = 0,
};

pub const MetadataMemoryDiagnostics = struct {
    process: process_memory_mod.Stats = .{},
    json_response: JsonResponseDiagnostics = .{},
    projected_core_snapshot: ProjectedCoreSnapshotDiagnostics = .{},
    projected_store_lsm: LsmRetentionDiagnostics = .{},
    hosted_write_cache: api_table_writes.HostedManagedDbCacheDiagnostics = .{},
};

fn optionalLen(value: ?[]const u8) usize {
    return if (value) |bytes| bytes.len else 0;
}

fn lsmRetentionDiagnostics(stats: anytype) LsmRetentionDiagnostics {
    return .{
        .mutable_bytes = stats.mutable_bytes,
        .immutable_bytes = stats.immutable_bytes,
        .total_run_bytes = stats.total_run_bytes,
        .wal_retained_bytes = stats.wal_retained_bytes,
        .wal_retained_segments = stats.wal_retained_segments,
        .active_readers = stats.active_readers,
        .obsolete_paths = stats.obsolete_paths,
        .obsolete_paths_pinned_by_readers = stats.obsolete_paths_pinned_by_readers,
        .obsolete_paths_pinned_by_versions = stats.obsolete_paths_pinned_by_versions,
        .bulk_ingest_current_scan_clone_active_bytes = stats.bulk_ingest_current_scan_clone_active_bytes,
    };
}

fn recordJsonResponseAllocationCounters(
    calls: *std.atomic.Value(u64),
    bytes_total: *std.atomic.Value(u64),
    peak_bytes: *std.atomic.Value(u64),
    bytes: usize,
) void {
    const amount: u64 = @intCast(bytes);
    _ = calls.fetchAdd(1, .monotonic);
    _ = bytes_total.fetchAdd(amount, .monotonic);
    var observed = peak_bytes.load(.monotonic);
    while (amount > observed) {
        observed = peak_bytes.cmpxchgWeak(observed, amount, .monotonic, .monotonic) orelse break;
    }
}

fn jsonResponseDiagnosticsFromCounters(
    calls: *const std.atomic.Value(u64),
    bytes_total: *const std.atomic.Value(u64),
    peak_bytes: *const std.atomic.Value(u64),
) JsonResponseDiagnostics {
    return .{
        .calls = calls.load(.monotonic),
        .bytes_total = bytes_total.load(.monotonic),
        .peak_bytes = peak_bytes.load(.monotonic),
    };
}

const LocalPlacementInputs = struct {
    placement_intents: []raft_reconciler.PlacementIntent,
};

const LocalTransitionInputs = struct {
    split_transitions: []transition_state.SplitTransitionRecord,
    merge_transitions: []transition_state.MergeTransitionRecord,
};

fn captureLocalProjectionInputs(self: *MetadataHttpService) !LocalProjectionInputs {
    const group_ids = try self.raft.host.http_host.host.listGroupIds(self.alloc);
    errdefer self.alloc.free(group_ids);
    self.lockRuntime();
    defer self.unlockRuntime();
    self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
    defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
    const catalog = try self.catalogValidationSnapshotLocked();
    const core = try self.projectedCoreSnapshotLocked();
    const tables = try cloneProjectedTablesOwned(self.alloc, catalog.tables);
    errdefer self.freeProjectedTables(self.alloc, tables);
    const ranges = try cloneProjectedRangesOwned(self.alloc, catalog.ranges);
    errdefer self.freeProjectedRanges(self.alloc, ranges);
    const stores = try cloneProjectedStoresOwned(self.alloc, core.stores);
    errdefer self.freeProjectedStores(self.alloc, stores);
    const schema_progresses = try cloneProjectedSchemaProgressOwned(self.alloc, core.schema_progresses);
    errdefer self.freeProjectedSchemaProgress(self.alloc, schema_progresses);
    const restore_progresses = try cloneProjectedRestoreProgressesOwned(self.alloc, core.restore_progresses);
    errdefer self.freeProjectedRestoreProgress(self.alloc, restore_progresses);
    return .{
        .group_ids = group_ids,
        .group_ids_fingerprint = groupIdsFingerprint(group_ids),
        .tables = tables,
        .ranges = ranges,
        .stores = stores,
        .schema_progresses = schema_progresses,
        .restore_progresses = restore_progresses,
    };
}

fn freeLocalProjectionInputs(self: *MetadataHttpService, inputs: *LocalProjectionInputs) void {
    self.alloc.free(inputs.group_ids);
    self.freeProjectedTables(self.alloc, inputs.tables);
    self.freeProjectedRanges(self.alloc, inputs.ranges);
    self.freeProjectedStores(self.alloc, inputs.stores);
    self.freeProjectedSchemaProgress(self.alloc, inputs.schema_progresses);
    self.freeProjectedRestoreProgress(self.alloc, inputs.restore_progresses);
    inputs.* = undefined;
}

fn cloneProjectedTablesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.TableRecord,
) ![]metadata_table_manager.TableRecord {
    const out = try alloc.alloc(metadata_table_manager.TableRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeTable(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneTable(alloc, record);
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedRangesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.RangeRecord,
) ![]metadata_table_manager.RangeRecord {
    const out = try alloc.alloc(metadata_table_manager.RangeRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeRange(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneRange(alloc, record);
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedStoresOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.StoreRecord,
) ![]metadata_table_manager.StoreRecord {
    const out = try alloc.alloc(metadata_table_manager.StoreRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeStore(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneStore(alloc, record);
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedShuffleJoinLeasesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.ShuffleJoinLeaseRecord,
) ![]metadata_table_manager.ShuffleJoinLeaseRecord {
    const out = try alloc.alloc(metadata_table_manager.ShuffleJoinLeaseRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try metadata_table_manager.cloneShuffleJoinLease(alloc, record);
    return out;
}

fn cloneProjectedSchemaProgressOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.SchemaProgressRecord,
) ![]metadata_table_manager.SchemaProgressRecord {
    const out = try alloc.alloc(metadata_table_manager.SchemaProgressRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = record;
    return out;
}

fn cloneProjectedPlacementIntentsOwned(
    alloc: std.mem.Allocator,
    intents: []const raft_reconciler.PlacementIntent,
) ![]raft_reconciler.PlacementIntent {
    const out = try alloc.alloc(raft_reconciler.PlacementIntent, intents.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |intent| {
            raft_reconciler.freeIntentOwned(alloc, intent);
        }
        alloc.free(out);
    }
    for (intents, 0..) |intent, i| {
        out[i] = try raft_reconciler.cloneIntentOwned(alloc, intent);
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedPlacementVersionFencesOwned(
    alloc: std.mem.Allocator,
    fences: []const metadata_reconciler.PlacementVersionFence,
) ![]metadata_reconciler.PlacementVersionFence {
    return try alloc.dupe(metadata_reconciler.PlacementVersionFence, fences);
}

fn cloneProjectedSplitTransitionsOwned(
    alloc: std.mem.Allocator,
    records: []const transition_state.SplitTransitionRecord,
) ![]transition_state.SplitTransitionRecord {
    const out = try alloc.alloc(transition_state.SplitTransitionRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneSplitTransitionRecord(
            alloc,
            record,
        );
        cloned = i + 1;
    }
    return out;
}

fn cloneProjectedRestoreProgressesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.RestoreProgressRecord,
) ![]metadata_table_manager.RestoreProgressRecord {
    const out = try alloc.alloc(metadata_table_manager.RestoreProgressRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try metadata_table_manager.cloneRestoreProgress(alloc, record);
    return out;
}

fn cloneProjectedReplicationSourceStatusesOwned(
    alloc: std.mem.Allocator,
    records: []const metadata_table_manager.ReplicationSourceStatusRecord,
) ![]metadata_table_manager.ReplicationSourceStatusRecord {
    const out = try alloc.alloc(metadata_table_manager.ReplicationSourceStatusRecord, records.len);
    errdefer alloc.free(out);
    for (records, 0..) |record, i| out[i] = try metadata_table_manager.cloneReplicationSourceStatus(alloc, record);
    return out;
}

fn replicationCutoverIntentApplied(
    record: metadata_table_manager.ReplicationSourceStatusRecord,
    expected: metadata_table_manager.ReplicationSourceStatusRecord,
) bool {
    return record.table_id == expected.table_id and
        record.source_ordinal == expected.source_ordinal and
        record.cutover_intent_id == expected.cutover_intent_id and
        record.cutover_authority_id == expected.cutover_authority_id and
        std.mem.eql(
            u8,
            &record.cutover_config_fingerprint,
            &expected.cutover_config_fingerprint,
        ) and
        std.mem.eql(
            u8,
            &record.cutover_provider_identity,
            &expected.cutover_provider_identity,
        ) and
        std.mem.eql(u8, record.source_kind, expected.source_kind) and
        std.mem.eql(u8, record.external_table, expected.external_table) and
        std.mem.eql(u8, record.cutover_mode, expected.cutover_mode) and
        std.mem.eql(u8, record.slot_name, expected.slot_name) and
        std.mem.eql(u8, record.publication_name, expected.publication_name) and
        std.mem.eql(u8, record.phase, expected.phase) and
        record.retired_cutover_authority_id ==
            expected.retired_cutover_authority_id and
        std.mem.eql(
            u8,
            record.retired_slot_name,
            expected.retired_slot_name,
        ) and
        std.mem.eql(
            u8,
            record.retired_publication_name,
            expected.retired_publication_name,
        );
}

fn replicationCutoverAuthorityMatches(
    record: metadata_table_manager.ReplicationSourceStatusRecord,
    expected: metadata_table_manager.ReplicationSourceStatusRecord,
) bool {
    return record.table_id == expected.table_id and
        record.source_ordinal == expected.source_ordinal and
        record.cutover_intent_id == expected.cutover_intent_id and
        record.cutover_authority_id == expected.cutover_authority_id and
        std.mem.eql(
            u8,
            &record.cutover_config_fingerprint,
            &expected.cutover_config_fingerprint,
        ) and
        std.mem.eql(
            u8,
            &record.cutover_provider_identity,
            &expected.cutover_provider_identity,
        ) and
        std.mem.eql(u8, record.source_kind, expected.source_kind) and
        std.mem.eql(u8, record.external_table, expected.external_table) and
        std.mem.eql(u8, record.slot_name, expected.slot_name) and
        std.mem.eql(u8, record.publication_name, expected.publication_name);
}

fn replicationCutoverRetirementMatches(
    record: metadata_table_manager.ReplicationSourceStatusRecord,
    expected: metadata_table_manager.ReplicationSourceStatusRecord,
) bool {
    return record.retired_cutover_authority_id ==
        expected.retired_cutover_authority_id and
        std.mem.eql(
            u8,
            record.retired_slot_name,
            expected.retired_slot_name,
        ) and
        std.mem.eql(
            u8,
            record.retired_publication_name,
            expected.retired_publication_name,
        );
}

fn isExpectedCdcRoundError(err: anyerror) bool {
    return switch (err) {
        error.UnknownReplicationSource,
        error.UnsupportedReplicationSource,
        error.UnsupportedReplicationStreaming,
        error.UnsupportedReplicationTransform,
        error.UnsupportedReplicationRoute,
        error.ReplicationExactCutoverRequired,
        error.InvalidReplicationSourceConfig,
        error.InvalidReplicationSourceRow,
        error.LibpqUnavailable,
        error.ForeignAuthFailed,
        error.ForeignConnectionFailed,
        error.ForeignConnectionPoolLimitExceeded,
        error.ForeignColumnCacheLimitExceeded,
        error.ForeignQueryFailed,
        error.ForeignProviderIdentityMismatch,
        error.ExactCutoverProviderIdentityMismatch,
        error.ReplicationCutoverAuthorityLost,
        error.ReplicationSourceConfigChanged,
        error.InvalidReplicationCutoverIntent,
        error.ForeignReplicationSlotMissing,
        error.ForeignTableNotFound,
        error.ExactCutoverCleanupPending,
        error.CdcWorkLeaseLost,
        error.CdcWorkShuttingDown,
        error.CdcWorkLeaseRenewalTimeout,
        error.MetadataMutationApplyTimeout,
        error.Timeout,
        error.FileNotFound,
        error.InvalidQueryRequest,
        error.WriterLocked,
        error.LmdbUnexpected,
        error.Corrupted,
        error.UnknownColumn,
        => true,
        else => false,
    };
}

test "metadata durable cutover acknowledgement is attempt scoped" {
    const fingerprint = [_]u8{0x5a} ** std.crypto.hash.sha2.Sha256.digest_length;
    const expected = metadata_table_manager.ReplicationSourceStatusRecord{
        .table_id = 41,
        .source_ordinal = 2,
        .source_kind = "postgres",
        .external_table = "public.docs",
        .cutover_mode = "exported_snapshot_pending",
        .slot_name = "antfly_docs",
        .publication_name = "antfly_docs_pub",
        .phase = "cutover_preparing",
        .cutover_intent_id = 99,
        .cutover_authority_id = 1001,
        .cutover_config_fingerprint = fingerprint,
        .cutover_provider_identity = [_]u8{0x6b} ** std.crypto.hash.sha2.Sha256.digest_length,
    };
    try std.testing.expect(replicationCutoverIntentApplied(expected, expected));

    var stale_attempt = expected;
    stale_attempt.cutover_intent_id = 98;
    try std.testing.expect(!replicationCutoverIntentApplied(stale_attempt, expected));

    var stale_authority = expected;
    stale_authority.cutover_authority_id = 1000;
    try std.testing.expect(!replicationCutoverIntentApplied(stale_authority, expected));

    var stale_provider = expected;
    stale_provider.cutover_provider_identity[0] ^= 0xff;
    try std.testing.expect(!replicationCutoverIntentApplied(stale_provider, expected));

    var stale_config = expected;
    stale_config.cutover_config_fingerprint[0] ^= 0xff;
    try std.testing.expect(!replicationCutoverIntentApplied(stale_config, expected));

    var overwritten_phase = expected;
    overwritten_phase.phase = "failed";
    try std.testing.expect(!replicationCutoverIntentApplied(overwritten_phase, expected));

    var stale_retirement = expected;
    stale_retirement.retired_cutover_authority_id = 1000;
    stale_retirement.retired_slot_name = "antfly_docs_old";
    stale_retirement.retired_publication_name = "antfly_docs_pub_old";
    try std.testing.expect(!replicationCutoverIntentApplied(stale_retirement, expected));
}

test "metadata CDC round error policy isolates expected recovery failures" {
    const expected_errors = [_]anyerror{
        error.ExactCutoverCleanupPending,
        error.MetadataMutationApplyTimeout,
        error.Timeout,
        error.ForeignConnectionPoolLimitExceeded,
        error.ForeignColumnCacheLimitExceeded,
        error.ForeignConnectionFailed,
        error.ForeignQueryFailed,
        error.ForeignProviderIdentityMismatch,
        error.ExactCutoverProviderIdentityMismatch,
    };
    for (expected_errors) |err| try std.testing.expect(isExpectedCdcRoundError(err));

    // Process-health and authority failures remain visible to the lifecycle
    // owner instead of being mistaken for a source-local retry.
    try std.testing.expect(!isExpectedCdcRoundError(error.OutOfMemory));
    try std.testing.expect(!isExpectedCdcRoundError(error.NotLeader));
}

fn cloneProjectedMergeTransitionsOwned(
    alloc: std.mem.Allocator,
    records: []const transition_state.MergeTransitionRecord,
) ![]transition_state.MergeTransitionRecord {
    const out = try alloc.alloc(transition_state.MergeTransitionRecord, records.len);
    var cloned: usize = 0;
    errdefer {
        for (out[0..cloned]) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
        alloc.free(out);
    }
    for (records, 0..) |record, i| {
        out[i] = try metadata_table_manager.cloneMergeTransitionRecord(
            alloc,
            record,
        );
        cloned = i + 1;
    }
    return out;
}

fn captureLocalPlacementInputs(self: *MetadataHttpService) !LocalPlacementInputs {
    self.lockRuntime();
    defer self.unlockRuntime();
    const snapshot = try self.projectedCoreSnapshotLocked();
    return .{
        .placement_intents = try cloneProjectedPlacementIntentsOwned(self.alloc, snapshot.placement_intents),
    };
}

fn freeLocalPlacementInputs(self: *MetadataHttpService, inputs: *LocalPlacementInputs) void {
    self.freeProjectedPlacementIntents(self.alloc, inputs.placement_intents);
    inputs.* = undefined;
}

fn captureLocalTransitionInputs(self: *MetadataHttpService) !LocalTransitionInputs {
    self.lockRuntime();
    defer self.unlockRuntime();
    const snapshot = try self.projectedCoreSnapshotLocked();
    return .{
        .split_transitions = try cloneProjectedSplitTransitionsOwned(self.alloc, snapshot.split_transitions),
        .merge_transitions = try cloneProjectedMergeTransitionsOwned(self.alloc, snapshot.merge_transitions),
    };
}

fn freeLocalTransitionInputs(self: *MetadataHttpService, inputs: *LocalTransitionInputs) void {
    self.freeProjectedSplitTransitions(self.alloc, inputs.split_transitions);
    self.freeProjectedMergeTransitions(self.alloc, inputs.merge_transitions);
    inputs.* = undefined;
}

fn shouldRefreshLocalProjection(
    last_epoch: ?u64,
    last_group_ids_fingerprint: ?u64,
    last_refresh_at_ms: u64,
    current_epoch: u64,
    current_group_ids_fingerprint: u64,
    refresh_interval_ms: u64,
) bool {
    if (last_epoch != current_epoch) return true;
    if (last_group_ids_fingerprint != current_group_ids_fingerprint) return true;
    if (last_refresh_at_ms == 0) return true;
    return nowMs() -| last_refresh_at_ms >= refresh_interval_ms;
}

fn shouldRefreshLocalEpoch(
    last_epoch: ?u64,
    last_refresh_at_ms: u64,
    current_epoch: u64,
    refresh_interval_ms: u64,
) bool {
    if (last_epoch != current_epoch) return true;
    if (last_refresh_at_ms == 0) return true;
    return nowMs() -| last_refresh_at_ms >= refresh_interval_ms;
}

fn reconcileLeaseCacheNextRefreshAtMs(
    state: *const metadata_reconcile_lease.State,
    is_local_leader: bool,
    record: ?metadata_reconcile_lease.ReconcileLeaseRecord,
    now_ms: u64,
) u64 {
    const fallback_refresh_at_ms = now_ms + reconcile_lease_probe_interval_ms;
    const current = record orelse return fallback_refresh_at_ms;
    if (is_local_leader and current.owner_node_id == state.local_node_id) {
        const renew_margin_ms = if (state.config.lease_ttl_ms > 1_000)
            state.config.lease_ttl_ms / 2
        else
            state.config.lease_ttl_ms;
        const renew_at_ms = current.expires_at_ms -| renew_margin_ms;
        return if (renew_at_ms > now_ms) renew_at_ms else now_ms;
    }
    return if (current.expires_at_ms > now_ms)
        @min(current.expires_at_ms, fallback_refresh_at_ms)
    else
        now_ms;
}

pub const MetadataService = struct {
    alloc: std.mem.Allocator,
    metadata_group_id: u64,
    replica_root_dir: ?[]const u8,
    observe_local_replica_root: bool,
    store_status_ticks: usize,
    projection_epoch: std.atomic.Value(u64) = .init(1),
    catalog_epoch: std.atomic.Value(u64) = .init(1),
    placement_epoch: std.atomic.Value(u64) = .init(1),
    reconcile_lease_epoch: std.atomic.Value(u64) = .init(1),
    transition_epoch: std.atomic.Value(u64) = .init(1),
    metadata_incarnation_candidate: ?metadata_mod.MetadataClusterIncarnation = null,
    metadata_incarnation_proposal_pending: bool = false,
    local_placement_epoch: ?u64,
    last_local_placement_refresh_at_ms: u64,
    local_transition_epoch: ?u64,
    last_local_transition_refresh_at_ms: u64,
    local_table_provisioning_fingerprint: ?u64,
    local_table_provisioning_epoch: ?u64,
    local_table_provisioning_group_ids_fingerprint: ?u64,
    last_local_table_provisioning_refresh_at_ms: u64,
    local_schema_progress_epoch: ?u64,
    local_schema_progress_group_ids_fingerprint: ?u64,
    last_local_schema_progress_refresh_at_ms: u64,
    cdc_runtime_mutex: std.Io.Mutex = .init,
    reconcile_lease_mutex: std.Io.Mutex = .init,
    cdc_job_in_flight: std.atomic.Value(bool) = .init(false),
    cdc_shutdown: std.atomic.Value(bool) = .init(false),
    cdc_permit_check_after_ns: std.atomic.Value(u64) = .init(0),
    cdc_job_owner_id: u64 = 0,
    reconcile_lease: metadata_reconcile_lease.State,
    lifecycle_signal: LifecycleSignal,
    lifecycle_reconcile_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    lifecycle_reconcile_hook: ?LifecycleReconcileHook = null,
    local_replica_root_reconcile_hook: ?LocalReplicaRootReconcileHook = null,
    local_replica_root_reconcile_permit_hook: ?LocalReplicaRootReconcilePermitHook = null,
    lifecycle_listener_mutex: std.Io.Mutex = .init,
    lifecycle_listener_registered: bool = false,
    catalog_validation_mutex: std.Io.Mutex = .init,
    catalog_validation_cache: CatalogValidationSnapshotCache = .{},
    local_group_status_provider: ?LocalGroupStatusProvider = null,
    local_shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    routed_shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    reconcile_lease_projection_cache: ReconcileLeaseProjectionCache = .{},
    store_status_backfill_probe_ticks: usize = 0,
    store_status_backfill_marker_cache: StoreStatusBackfillMarkerCache = .{},
    cdc_backfill_registry: foreign_mod.Registry = .{},
    cdc_next_round_at_ms: u64 = 0,
    secret_store: ?*common_secrets.FileStore = null,
    internal_service_secret: ?[]const u8 = null,
    internal_service_issuer: ?[]const u8 = null,
    destination_authorizer: ?stored_destination_authorization.Authorizer = null,
    json_response_calls: std.atomic.Value(u64) = .init(0),
    json_response_bytes_total: std.atomic.Value(u64) = .init(0),
    json_response_peak_bytes: std.atomic.Value(u64) = .init(0),
    control_round_mutex: std.Io.Mutex = .init,
    placement_reconcile_mutex: std.Io.Mutex = .init,
    runtime_mutex: std.Io.Mutex = .init,
    transition_mutex: std.Io.Mutex = .init,
    backend_runtime_mutex: std.Io.Mutex = .init,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    owned_backend_runtime: ?backend_runtime_mod.BackendRuntimeHandle = null,
    linearizable_read_tracker: *LinearizableMetadataReadTracker,
    raft: raft_service.ManagedHostService,

    pub fn init(
        alloc: std.mem.Allocator,
        host_cfg: raft_managed_host.ManagedHostConfig,
        deps: MetadataServiceDeps,
        cfg: MetadataServiceConfig,
    ) !MetadataService {
        const metadata_group_id = host_cfg.host.metadata_group_id orelse return error.MissingMetadataGroupId;
        var host_deps = deps.host;
        const read_tracker = try alloc.create(LinearizableMetadataReadTracker);
        var read_tracker_owned = true;
        read_tracker.* = .{
            .alloc = alloc,
            .metadata_group_id = metadata_group_id,
            .downstream = host_deps.read_state_observer,
        };
        errdefer if (read_tracker_owned) {
            read_tracker.deinit();
            alloc.destroy(read_tracker);
        };
        host_deps.read_state_observer = read_tracker.observer();
        var service = MetadataService{
            .alloc = alloc,
            .metadata_group_id = metadata_group_id,
            .replica_root_dir = host_cfg.host.replica_root_dir,
            .observe_local_replica_root = cfg.observe_local_replica_root,
            .store_status_ticks = 0,
            .local_placement_epoch = null,
            .last_local_placement_refresh_at_ms = 0,
            .local_transition_epoch = null,
            .last_local_transition_refresh_at_ms = 0,
            .local_table_provisioning_fingerprint = null,
            .local_table_provisioning_epoch = null,
            .local_table_provisioning_group_ids_fingerprint = null,
            .last_local_table_provisioning_refresh_at_ms = 0,
            .local_schema_progress_epoch = null,
            .local_schema_progress_group_ids_fingerprint = null,
            .last_local_schema_progress_refresh_at_ms = 0,
            .reconcile_lease = metadata_reconcile_lease.State.init(host_cfg.host.local_node_id, cfg.reconcile_lease),
            .lifecycle_signal = LifecycleSignal.init(alloc),
            .backend_runtime = cfg.backend_runtime,
            .secret_store = cfg.secret_store,
            .internal_service_secret = cfg.internal_service_secret,
            .internal_service_issuer = cfg.internal_service_issuer,
            .destination_authorizer = cfg.destination_authorizer,
            .linearizable_read_tracker = read_tracker,
            .raft = try raft_service.ManagedHostService.init(alloc, host_cfg, host_deps, cfg.raft, deps.raft),
        };
        read_tracker_owned = false;
        errdefer service.deinit();
        try foreign_mod.registerDefaultPostgresExecutor(alloc, &service.cdc_backfill_registry);
        return service;
    }

    pub fn deinit(self: *MetadataService) void {
        shutdownCdcRuntimeJobs(self);
        // Projection listeners retain `self`; stop and drain their Raft apply
        // producer before releasing any callback-owned service state.
        self.raft.deinit();
        self.catalog_validation_cache.deinit(self.alloc);
        self.store_status_backfill_marker_cache.deinit(self.alloc);
        self.cdc_backfill_registry.deinit(self.alloc);
        self.lifecycle_signal.deinit();
        self.linearizable_read_tracker.deinit();
        self.alloc.destroy(self.linearizable_read_tracker);
        if (self.replica_root_dir) |replica_root_dir| {
            api_table_writes.closeHostedManagedDbCacheForRoot(replica_root_dir);
        }
        if (self.owned_backend_runtime) |*runtime| runtime.deinit();
        self.owned_backend_runtime = null;
        self.backend_runtime = null;
        self.* = undefined;
    }

    pub fn ensureBackendRuntime(self: *MetadataService) !*backend_runtime_mod.BackendRuntime {
        self.backend_runtime_mutex.lockUncancelable(std.Options.debug_io);
        defer self.backend_runtime_mutex.unlock(std.Options.debug_io);
        if (self.backend_runtime == null) {
            self.owned_backend_runtime = try backend_runtime_mod.BackendRuntimeHandle.init(self.alloc, .{});
            self.backend_runtime = self.owned_backend_runtime.?.ptr();
        }
        if (self.backend_runtime) |runtime| return runtime;
        unreachable;
    }

    fn lockRuntime(self: *MetadataService) void {
        self.runtime_mutex.lockUncancelable(std.Options.debug_io);
    }

    fn unlockRuntime(self: *MetadataService) void {
        self.runtime_mutex.unlock(std.Options.debug_io);
    }

    fn lockTransitions(self: *MetadataService) void {
        self.transition_mutex.lockUncancelable(std.Options.debug_io);
    }

    fn unlockTransitions(self: *MetadataService) void {
        self.transition_mutex.unlock(std.Options.debug_io);
    }

    fn stepTransitions(self: *MetadataService) !void {
        self.lockTransitions();
        defer self.unlockTransitions();
        _ = try self.raft.stepTransitions();
    }

    fn isLocalMetadataLeader(self: *MetadataService) bool {
        self.lockRuntime();
        defer self.unlockRuntime();
        return self.raft.host.host.isLocalLeader(self.metadata_group_id);
    }

    fn listLocalGroupIds(self: *MetadataService, alloc: std.mem.Allocator) ![]u64 {
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.host.host.listGroupIds(alloc);
    }

    pub fn ensureLinearizableRead(self: *MetadataService) !void {
        return self.ensureLinearizableReadWithContext(.{});
    }

    pub fn ensureLinearizableReadWithContext(
        self: *MetadataService,
        request: api_operation.RequestContext,
    ) !void {
        try request.ensureActive();
        const request_id = try self.linearizable_read_tracker.registerRequest();
        defer self.linearizable_read_tracker.finishRequest(request_id);
        var request_ctx_buf: [64]u8 = undefined;
        const request_ctx = try std.fmt.bufPrint(
            &request_ctx_buf,
            "{s}{d}",
            .{ linearizable_metadata_read_prefix, request_id },
        );

        const local_deadline_ns = platform_time.monotonicNs() +| linearizable_metadata_read_timeout_ns;
        const deadline_ns = if (request.deadline_ns) |caller_deadline_ns|
            @min(local_deadline_ns, caller_deadline_ns)
        else
            local_deadline_ns;
        var next_request_ns: u64 = 0;
        while (platform_time.monotonicNs() < deadline_ns) {
            try request.ensureActive();
            if (self.linearizable_read_tracker.isComplete(request_id)) return;
            const now_ns = platform_time.monotonicNs();
            if (now_ns >= next_request_ns) {
                self.lockRuntime();
                {
                    defer self.unlockRuntime();
                    self.raft.requestReadableLease(self.metadata_group_id, request_ctx) catch |err| switch (err) {
                        error.NotLeader => {},
                        else => return err,
                    };
                }
                next_request_ns = now_ns + linearizable_metadata_read_retry_ns;
            }
            self.lockRuntime();
            {
                defer self.unlockRuntime();
                try self.raft.runRaftRoundOnly();
            }
            if (self.linearizable_read_tracker.isComplete(request_id)) return;
            try request.ensureActive();
            platform_clock.Clock.real().sleepMs(1);
        }
        try request.ensureActive();
        return error.MetadataLinearizableReadTimeout;
    }

    pub fn lifecycleSignalCurrent(self: *const MetadataService) u32 {
        return self.lifecycle_signal.current();
    }

    pub fn captureLifecycleSignal(self: *MetadataService, table_name: ?[]const u8) LifecycleSignal.Snapshot {
        return self.lifecycle_signal.snapshot(table_name);
    }

    pub fn waitForLifecycleSignal(self: *MetadataService, observed: LifecycleSignal.Snapshot, timeout_ns: u64) void {
        self.lifecycle_signal.wait(observed, timeout_ns);
    }

    pub fn setLifecycleReconcileHook(self: *MetadataService, hook: ?LifecycleReconcileHook) void {
        self.lifecycle_reconcile_hook = hook;
        self.lifecycle_reconcile_requested.store(true, .release);
    }

    pub fn setLocalGroupStatusProvider(self: *MetadataService, provider: ?LocalGroupStatusProvider) void {
        self.local_group_status_provider = provider;
    }

    pub fn setLocalShardDbAdapter(self: *MetadataService, adapter: ?metadata_mod.ShardDbAdapter) void {
        self.local_shard_db_adapter = adapter;
    }

    pub fn setRoutedShardDbAdapter(self: *MetadataService, adapter: ?metadata_mod.ShardDbAdapter) void {
        self.routed_shard_db_adapter = adapter;
    }

    pub fn setLocalReplicaRootReconcileHook(self: *MetadataService, hook: ?LocalReplicaRootReconcileHook) void {
        self.local_replica_root_reconcile_hook = hook;
    }

    pub fn setLocalReplicaRootReconcilePermitHook(self: *MetadataService, hook: ?LocalReplicaRootReconcilePermitHook) void {
        self.local_replica_root_reconcile_permit_hook = hook;
    }

    fn ensureLifecycleListenerRegistered(self: *MetadataService) !void {
        self.lifecycle_listener_mutex.lockUncancelable(std.Options.debug_io);
        defer self.lifecycle_listener_mutex.unlock(std.Options.debug_io);
        if (self.lifecycle_listener_registered) return;
        const store = self.projectedStore() orelse return;
        try store.addLifecycleListeners(
            .{
                .ptr = self,
                .vtable = &.{
                    .on_projection_signal = metadataServiceProjectionSignal,
                },
            },
            .{
                .ptr = self,
                .vtable = &.{
                    .matches_key = metadataServiceLifecycleKeyMatches,
                    .on_committed_key = metadataServiceCommittedKeySignal,
                },
            },
        );
        self.lifecycle_listener_registered = true;
    }

    fn metadataServiceProjectionSignal(ptr: *anyopaque, signal: metadata_storage.raft_apply_store.ProjectionSignal) void {
        const self: *MetadataService = @ptrCast(@alignCast(ptr));
        if (projectionSignalChangesCatalog(signal.kind)) {
            _ = self.catalog_epoch.fetchAdd(1, .release);
        }
        switch (signal.kind) {
            .table, .range, .shuffle_join_lease, .restore_job => _ = self.projection_epoch.fetchAdd(1, .monotonic),
            .placement_intent => _ = self.placement_epoch.fetchAdd(1, .monotonic),
            .reconcile_lease => _ = self.reconcile_lease_epoch.fetchAdd(1, .monotonic),
            .split_transition, .merge_transition => _ = self.transition_epoch.fetchAdd(1, .monotonic),
            else => {},
        }
        self.lifecycle_signal.notify(signal.table_name);
    }

    fn metadataServiceLifecycleKeyMatches(ptr: *anyopaque, signal: metadata_storage.raft_apply_store.CommittedKeySignal) bool {
        const self: *MetadataService = @ptrCast(@alignCast(ptr));
        return lifecycleKeyMatchesMetadataNamespace(self.metadata_group_id, signal);
    }

    fn metadataServiceCommittedKeySignal(ptr: *anyopaque, _: metadata_storage.raft_apply_store.CommittedKeySignal) void {
        const self: *MetadataService = @ptrCast(@alignCast(ptr));
        self.lifecycle_reconcile_requested.store(true, .release);
        self.lifecycle_signal.notify(null);
    }

    pub fn medianKeyLookup(self: *MetadataService) ?metadata_reconciler.MedianKeyLookup {
        if (self.routed_shard_db_adapter == null and self.local_shard_db_adapter == null and self.replica_root_dir == null) return null;
        return .{
            .ptr = self,
            .vtable = &.{
                .fetch_median_key = fetchMedianKey,
            },
        };
    }

    fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        const self: *MetadataService = @ptrCast(@alignCast(ptr));
        if (self.routed_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
        if (self.local_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
        const replica_root_dir = self.replica_root_dir orelse return error.UnsupportedOperation;
        var fallback = metadata_mod.FallbackLocalShardDbAdapter{
            .replica_root_dir = replica_root_dir,
            .backend_runtime = try self.ensureBackendRuntime(),
        };
        return try fallback.adapter().fetchMedianKey(alloc, group_id);
    }

    pub fn ensureMetadataReplica(self: *MetadataService, record: raft_catalog.ReplicaRecord) !raft_engine.runtime.EnsureReplicaResult {
        if (record.group_id != self.metadata_group_id) return error.InvalidMetadataGroupId;
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.host.host.ensureReplica(record);
    }

    pub fn campaignMetadataGroup(self: *MetadataService) !void {
        self.lockRuntime();
        defer self.unlockRuntime();
        try self.raft.host.host.campaignGroup(self.metadata_group_id);
    }

    pub fn ensureLocalTableMutationAuthority(self: *MetadataService) !void {
        self.lockRuntime();
        defer self.unlockRuntime();
        if (!self.raft.host.host.isLocalLeader(self.metadata_group_id)) return error.NotLeader;
    }

    pub fn proposeTransitionCommand(self: *MetadataService, command: metadata_storage.TransitionCommand) !void {
        var owned_legacy_store: ?metadata_table_manager.StoreRecord = null;
        defer if (owned_legacy_store) |record| metadata_table_manager.freeStore(self.alloc, record);
        const safe_command = try runtimeStatusProtocolSafeCommand(self, command, &owned_legacy_store);
        self.lockRuntime();
        defer self.unlockRuntime();
        try metadata_storage.validateTransitionCommandDataGroupIds(safe_command);
        const encoded = try metadata_storage.encodeTransitionCommand(self.alloc, safe_command);
        defer self.alloc.free(encoded);
        try self.raft.host.host.propose(self.metadata_group_id, encoded);
        self.lifecycle_signal.notify(null);
    }

    pub fn upsertNode(self: *MetadataService, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_node = record });
    }

    pub fn registerNode(self: *MetadataService, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .register_node = record });
    }

    pub fn requestNodeShutdown(self: *MetadataService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .request_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn cancelNodeShutdown(self: *MetadataService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .cancel_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn finalizeNodeShutdown(self: *MetadataService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .finalize_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn removeNode(self: *MetadataService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_node = .{ .node_id = node_id } });
    }

    pub fn upsertStore(self: *MetadataService, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_store = record });
    }

    fn runtimeStatusRepairProtocolReady(self: *MetadataService) bool {
        const raft_status = self.raft.host.host.raftStatus(self.metadata_group_id) orelse return false;
        const local_node_id = self.raft.host.host.cfg.local_node_id;
        const membership_sets = [_][]const u64{
            raft_status.conf_state.voters,
            raft_status.conf_state.voters_outgoing,
            raft_status.conf_state.learners,
            raft_status.conf_state.learners_next,
        };
        var local_member = false;
        for (membership_sets) |node_ids| {
            for (node_ids) |node_id| {
                if (node_id != local_node_id) return false;
                local_member = true;
            }
        }
        return local_member;
    }

    pub fn registerStore(self: *MetadataService, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .register_store = record });
    }

    pub fn reportStoreStatus(self: *MetadataService, report: metadata_table_manager.StoreStatusReport) !void {
        _ = try self.reportStoreStatuses(&.{report});
    }

    pub fn reportStoreStatuses(self: *MetadataService, reports: []const metadata_table_manager.StoreStatusReport) !usize {
        const projected = try self.listProjectedStores(self.alloc);
        defer self.freeProjectedStores(self.alloc, projected);
        return try reportStoreStatusesWithProjected(self, projected, reports);
    }

    pub fn removeStore(self: *MetadataService, store_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_store = .{ .store_id = store_id } });
    }

    pub fn upsertSplitTransition(self: *MetadataService, record: transition_state.SplitTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_split_transition = record });
    }

    pub fn admitSplitTransition(self: *MetadataService, admission: metadata_reconciler.SplitAdmission) !void {
        try self.proposeTransitionCommand(.{ .admit_split_transition = .{
            .expected_source_epoch = admission.expected_source_epoch,
            .record = admission.record,
        } });
    }

    pub fn upsertReplicaIntent(
        self: *MetadataService,
        intent: raft_reconciler.PlacementIntent,
        expected_metadata_version: ?u64,
        expected_version_fence: u64,
        expected_target_drain_requested: bool,
    ) !void {
        try self.proposeTransitionCommand(.{ .upsert_replica_intent = .{
            .expected_metadata_version = expected_metadata_version,
            .expected_version_fence = expected_version_fence,
            .expected_target_drain_requested = expected_target_drain_requested,
            .replacement = intent,
        } });
    }

    pub fn removeReplicaIntent(self: *MetadataService, group_id: u64, local_node_id: u64, expected_metadata_version: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_replica_intent = .{
            .group_id = group_id,
            .local_node_id = local_node_id,
            .expected_metadata_version = expected_metadata_version,
        } });
    }

    pub fn upsertTable(self: *MetadataService, record: metadata_table_manager.TableRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_table = record });
    }

    pub fn replaceTableDefinition(
        self: *MetadataService,
        expected: metadata_table_manager.TableRecord,
        replacement: metadata_table_manager.TableRecord,
    ) !void {
        if (expected.table_id == 0 or
            replacement.table_id != expected.table_id or
            !std.mem.eql(u8, replacement.name, expected.name))
            return error.InvalidTableDefinitionReplacement;
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        const baseline_fence = try store.getTableTransitionFence(
            self.metadata_group_id,
            expected.table_id,
        );
        if (baseline_fence.active()) return error.TableTransitionActive;
        try self.proposeTransitionCommand(.{ .compare_and_replace_table = .{
            .expected = expected,
            .replacement = replacement,
        } });

        const deadline_ns = platform_time.monotonicNs() +| linearizable_metadata_read_timeout_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            const current = (try store.getTable(
                self.alloc,
                self.metadata_group_id,
                expected.table_id,
            )) orelse return error.TableNotFound;
            defer metadata_table_manager.freeTable(self.alloc, current);
            if (metadata_table_manager.tableDefinitionsEqual(current, replacement)) return;
            if (!metadata_table_manager.tableDefinitionsEqual(current, expected))
                return error.TableGenerationChanged;

            const fence = try store.getTableTransitionFence(
                self.metadata_group_id,
                expected.table_id,
            );
            if (fence.active() or fence.generation != baseline_fence.generation)
                return error.TableTransitionActive;

            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (!self.raft.host.host.isLocalLeader(self.metadata_group_id))
                    return error.NotLeader;
                try self.raft.runRaftRoundOnly();
            }
            platform_clock.Clock.real().sleepMs(1);
        }
        return error.MetadataMutationApplyTimeout;
    }

    pub fn removeTable(self: *MetadataService, table_id: u64) !void {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        const baseline_fence = try store.getTableTransitionFence(
            self.metadata_group_id,
            table_id,
        );
        if (baseline_fence.active()) return error.TableTransitionActive;
        if (try store.getTable(self.alloc, self.metadata_group_id, table_id)) |current| {
            metadata_table_manager.freeTable(self.alloc, current);
        } else {
            return;
        }
        try self.proposeTransitionCommand(.{ .remove_table = .{
            .table_id = table_id,
            .expected_transition_generation = baseline_fence.generation,
        } });

        const deadline_ns = platform_time.monotonicNs() +|
            linearizable_metadata_read_timeout_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            if (try store.getTable(
                self.alloc,
                self.metadata_group_id,
                table_id,
            )) |current| {
                metadata_table_manager.freeTable(self.alloc, current);
            } else {
                return;
            }
            const fence = try store.getTableTransitionFence(
                self.metadata_group_id,
                table_id,
            );
            if (fence.active() or
                fence.generation != baseline_fence.generation)
                return error.TableTransitionActive;

            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (!self.raft.host.host.isLocalLeader(self.metadata_group_id))
                    return error.NotLeader;
                try self.raft.runRaftRoundOnly();
            }
            platform_clock.Clock.real().sleepMs(1);
        }
        return error.MetadataMutationApplyTimeout;
    }

    pub fn upsertSchemaProgress(self: *MetadataService, record: metadata_table_manager.SchemaProgressRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_schema_progress = record });
    }

    pub fn removeSchemaProgress(self: *MetadataService, table_id: u64, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_schema_progress = .{
            .table_id = table_id,
            .node_id = node_id,
        } });
    }

    pub fn upsertRestoreProgress(self: *MetadataService, record: metadata_table_manager.RestoreProgressRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_restore_progress = record });
    }

    pub fn removeRestoreProgress(self: *MetadataService, table_id: u64, node_id: u64, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_restore_progress = .{
            .table_id = table_id,
            .node_id = node_id,
            .group_id = group_id,
        } });
    }

    pub fn upsertReplicationSourceStatus(self: *MetadataService, record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_replication_source_status = record });
    }

    /// Atomically claims exact-cutover authority against both the current
    /// source catalog and prior authority, then waits for the applied proof.
    pub fn claimReplicationSourceCutoverDurable(
        self: *MetadataService,
        expected_replication_sources_json: []const u8,
        expected_authority_id: u64,
        record: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        if (record.cutover_intent_id == 0 or
            record.cutover_authority_id == 0 or
            std.mem.allEqual(u8, &record.cutover_provider_identity, 0))
            return error.InvalidReplicationCutoverIntent;
        try self.proposeTransitionCommand(.{ .claim_replication_source_cutover = .{
            .expected_replication_sources_json = expected_replication_sources_json,
            .expected_authority_id = expected_authority_id,
            .record = record,
        } });

        const deadline_ns = platform_time.monotonicNs() +| linearizable_metadata_read_timeout_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            const store = self.projectedStore() orelse return error.MissingMetadataStore;
            if (try store.getReplicationSourceStatus(
                self.alloc,
                self.metadata_group_id,
                record.table_id,
                record.source_ordinal,
            )) |applied_record| {
                defer metadata_table_manager.freeReplicationSourceStatus(self.alloc, applied_record);
                if (replicationCutoverIntentApplied(applied_record, record)) return;
                if (applied_record.cutover_authority_id != expected_authority_id)
                    return error.ReplicationCutoverAuthorityLost;
            }
            const current_table = (try store.getTable(
                self.alloc,
                self.metadata_group_id,
                record.table_id,
            )) orelse return error.TableNotFound;
            defer metadata_table_manager.freeTable(self.alloc, current_table);
            if (!std.mem.eql(
                u8,
                current_table.replication_sources_json,
                expected_replication_sources_json,
            )) return error.ReplicationSourceConfigChanged;

            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (!self.raft.host.host.isLocalLeader(self.metadata_group_id))
                    return error.NotLeader;
                try self.raft.runRaftRoundOnly();
            }
            platform_clock.Clock.real().sleepMs(1);
        }
        return error.MetadataMutationApplyTimeout;
    }

    pub fn replicationSourceAuthorityCurrent(
        self: *MetadataService,
        expected_replication_sources_json: []const u8,
        expected: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        const table = (try store.getTable(
            self.alloc,
            self.metadata_group_id,
            expected.table_id,
        )) orelse return error.ReplicationSourceConfigChanged;
        defer metadata_table_manager.freeTable(self.alloc, table);
        if (!std.mem.eql(
            u8,
            table.replication_sources_json,
            expected_replication_sources_json,
        )) return error.ReplicationSourceConfigChanged;
        const current = (try store.getReplicationSourceStatus(
            self.alloc,
            self.metadata_group_id,
            expected.table_id,
            expected.source_ordinal,
        )) orelse return error.ReplicationCutoverAuthorityLost;
        defer metadata_table_manager.freeReplicationSourceStatus(self.alloc, current);
        if (!replicationCutoverAuthorityMatches(current, expected))
            return error.ReplicationCutoverAuthorityLost;
    }

    pub fn completeReplicationSourceCutoverRetirementDurable(
        self: *MetadataService,
        expected: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        if (expected.cutover_authority_id == 0 or
            expected.retired_cutover_authority_id == 0 or
            expected.retired_slot_name.len == 0 or
            expected.retired_publication_name.len == 0)
            return error.InvalidReplicationCutoverIntent;
        try self.proposeTransitionCommand(.{
            .complete_replication_source_retirement = expected,
        });
        const deadline_ns = platform_time.monotonicNs() +|
            linearizable_metadata_read_timeout_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            const store = self.projectedStore() orelse
                return error.MissingMetadataStore;
            const current = (try store.getReplicationSourceStatus(
                self.alloc,
                self.metadata_group_id,
                expected.table_id,
                expected.source_ordinal,
            )) orelse return error.ReplicationCutoverAuthorityLost;
            defer metadata_table_manager.freeReplicationSourceStatus(
                self.alloc,
                current,
            );
            if (!replicationCutoverAuthorityMatches(current, expected))
                return error.ReplicationCutoverAuthorityLost;
            if (current.retired_cutover_authority_id == 0 and
                current.retired_slot_name.len == 0 and
                current.retired_publication_name.len == 0)
                return;
            if (!replicationCutoverRetirementMatches(current, expected))
                return error.ReplicationCutoverAuthorityLost;

            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (!self.raft.host.host.isLocalLeader(self.metadata_group_id))
                    return error.NotLeader;
                try self.raft.runRaftRoundOnly();
            }
            platform_clock.Clock.real().sleepMs(1);
        }
        return error.MetadataMutationApplyTimeout;
    }

    pub fn upsertRange(self: *MetadataService, record: metadata_table_manager.RangeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_range = record });
    }

    pub fn completeRestoreRange(self: *MetadataService, identity: metadata_table_manager.RestoreIntentIdentity) !void {
        try self.proposeTransitionCommand(.{ .complete_restore_range = identity });
    }

    pub fn removeRange(self: *MetadataService, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_range = .{ .group_id = group_id } });
    }

    pub fn removeSplitTransition(self: *MetadataService, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_split_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertMergeTransition(self: *MetadataService, record: transition_state.MergeTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_merge_transition = record });
    }

    pub fn removeMergeTransition(self: *MetadataService, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_merge_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertReconcileLease(self: *MetadataService, record: metadata_reconcile_lease.ReconcileLeaseRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_reconcile_lease = record });
    }

    pub fn removeReconcileLease(self: *MetadataService) !void {
        try self.proposeTransitionCommand(.{ .remove_reconcile_lease = .{} });
    }

    pub fn upsertShuffleJoinLease(self: *MetadataService, record: metadata_table_manager.ShuffleJoinLeaseRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_shuffle_join_lease = record });
    }

    pub fn removeShuffleJoinLease(self: *MetadataService, job_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_shuffle_join_lease = .{ .job_id = job_id } });
    }

    pub fn requestReallocation(self: *MetadataService, requested_at_ms: u64) !void {
        const raft_status = self.raft.host.host.raftStatus(self.metadata_group_id) orelse
            return error.ReallocationProtocolUpgradeRequired;
        const local_node_id = self.raft.host.host.cfg.local_node_id;
        const required_node_ids = try collectReallocationBarrierNodeIds(
            self.alloc,
            raft_status.conf_state,
            &.{},
        );
        defer if (required_node_ids.len > 0) self.alloc.free(required_node_ids);
        // The non-HTTP service has no authenticated orchestration channel for
        // remote capability probes. Keep it safe for its single-node use and
        // require the HTTP service for multi-node request admission.
        if (required_node_ids.len != 1 or required_node_ids[0] != local_node_id) {
            return error.ReallocationProtocolUpgradeRequired;
        }
        const incarnation = (try self.metadataIncarnation()) orelse
            return error.ReallocationProtocolUpgradeRequired;
        const barrier = try reallocationBarrierContract(incarnation, required_node_ids);
        const runtime = try self.ensureBackendRuntime();
        const request_id = try metadata_reallocation_request.generateRequestId(runtime.io() orelse std.Options.debug_io);
        try self.proposeTransitionCommand(.{ .upsert_reallocation_request = .{
            .request_id = request_id,
            .requested_at_ms = requested_at_ms,
            .barrier_protocol_version = metadata_reallocation_request.barrier_protocol_version,
            .metadata_incarnation = barrier.metadata_incarnation,
            .protected_metadata_member_count = barrier.protected_metadata_member_count,
            .protected_metadata_membership_fingerprint = barrier.protected_metadata_membership_fingerprint,
        } });
    }

    pub fn clearReallocationRequest(self: *MetadataService, expected_request_id: u128) !void {
        try self.proposeTransitionCommand(.{ .remove_reallocation_request = .{
            .expected_request_id = expected_request_id,
        } });
    }

    pub fn upsertExtensionPackage(self: *MetadataService, record: extension_domain.PackageManifest) !void {
        try self.proposeTransitionCommand(.{ .upsert_extension_package = record });
    }

    pub fn syncExtensionPackageStore(self: *MetadataService, io: std.Io, root_path: []const u8) !usize {
        const entries = try extension_domain.scanPackageStoreAlloc(self.alloc, io, root_path);
        defer extension_domain.freePackageStoreEntries(self.alloc, entries);
        for (entries) |entry| try self.upsertExtensionPackage(entry.manifest);
        return entries.len;
    }

    pub fn runRound(self: *MetadataService) !void {
        self.control_round_mutex.lockUncancelable(std.Options.debug_io);
        defer self.control_round_mutex.unlock(std.Options.debug_io);
        try self.ensureLifecycleListenerRegistered();
        defer self.lifecycle_signal.notify(null);
        self.lockRuntime();
        {
            defer self.unlockRuntime();
            try self.raft.runRaftRoundOnly();
        }
        if (!try self.ensureMetadataIncarnation()) return;
        if (!self.observe_local_replica_root) return;
        const backfill_markers = try self.refreshStoreStatusBackfillMarkersForRound();
        if ((self.store_status_ticks >= 40 or backfill_markers.len > 0) and shouldRefreshLocalStoreStatus(self, backfill_markers)) {
            self.store_status_ticks = 0;
            self.refreshLocalStoreStatusWithBackfillMarkers(backfill_markers, true) catch |err| switch (err) {
                error.UnknownGroup, error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
                else => return err,
            };
        }
        self.refreshLocalSchemaProgress() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
        try self.refreshLocalTransitions();
        try self.stepTransitions();
        if (!try runReplicationBackfillIfLeaseHeld(self)) return;
        try self.refreshLocalPlacementIntents();
        _ = self.refreshLocalTableProvisioning() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => .{},
            else => return err,
        };
        try self.completeRestoreIntentsIfReady();
        try self.runLifecycleReconcileHookIfRequested();
    }

    pub fn runLifecycleRound(self: *MetadataService) !void {
        self.control_round_mutex.lockUncancelable(std.Options.debug_io);
        defer self.control_round_mutex.unlock(std.Options.debug_io);
        try self.ensureLifecycleListenerRegistered();
        defer self.lifecycle_signal.notify(null);
        self.lockRuntime();
        {
            defer self.unlockRuntime();
            try self.raft.runRaftRoundOnly();
        }
        if (!try self.ensureMetadataIncarnation()) return;
        if (!self.observe_local_replica_root) return;

        const backfill_markers = try self.refreshStoreStatusBackfillMarkersForLifecycleRound();
        if (shouldRefreshLocalStoreStatusForLifecycleRound(self, backfill_markers)) {
            // Lifecycle rounds run while schema provisioning can own the
            // shard DB for minutes. Use the registered data-runtime provider
            // here too: bypassing it cold-opened the complete DB once per
            // lifecycle tick, including every full-text segment, while the
            // authoritative writer was already rebuilding the next schema.
            self.refreshLocalStoreStatusWithBackfillMarkers(backfill_markers, true) catch |err| switch (err) {
                error.UnknownGroup, error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
                else => return err,
            };
        }
        self.refreshLocalSchemaProgress() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
        try self.refreshLocalTransitions();
        try self.stepTransitions();
        if (!try runReplicationBackfillIfLeaseHeld(self)) return;
        try self.refreshLocalPlacementIntents();
        _ = self.refreshLocalTableProvisioning() catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => .{},
            else => return err,
        };
        try self.completeRestoreIntentsIfReady();
        try self.runLifecycleReconcileHookIfRequested();
    }

    pub fn waitForTableLifecycle(self: *MetadataService, table_name: []const u8, expected: TableLifecycleExpectation) !void {
        try self.ensureLifecycleListenerRegistered();
        return try waitForTableLifecycleConvergence(self, table_name, expected);
    }

    pub fn waitForTableProjection(self: *MetadataService, table_name: []const u8, expected: TableProjectionExpectation) !void {
        try self.ensureLifecycleListenerRegistered();
        return try waitForTableProjectionConvergence(self, table_name, expected);
    }

    pub fn reconcileOnceIfLeaseHeld(self: *MetadataService, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return null;
        return try loop.reconcileOnce(self);
    }

    pub fn reconcilePreparedIfLeaseHeld(self: *MetadataService, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return null;
        return try loop.reconcilePrepared(self);
    }

    pub fn reconcileOnceEnsuringLease(self: *MetadataService, loop: *metadata_control_loop.MetadataControlLoop) !metadata_control_loop.ReconcileSummary {
        var rounds: usize = 0;
        while (rounds < 32) : (rounds += 1) {
            if (try self.reconcileOnceIfLeaseHeld(loop)) |summary| return summary;
            try self.runRound();
        }
        return error.ReconcileLeaseNotHeld;
    }

    /// Acquire or renew the reconcile lease without refreshing the control
    /// loop's prepared desired state. This is required after a transition
    /// observation has already been folded into that state: retrying through
    /// reconcileOnce would overwrite the prepared outcome from projection.
    pub fn reconcilePreparedEnsuringLease(self: *MetadataService, loop: *metadata_control_loop.MetadataControlLoop) !metadata_control_loop.ReconcileSummary {
        var rounds: usize = 0;
        while (rounds < 32) : (rounds += 1) {
            if (try self.reconcilePreparedIfLeaseHeld(loop)) |summary| return summary;
            try self.runRound();
        }
        return error.ReconcileLeaseNotHeld;
    }

    pub fn applyReconciliationPlan(self: *MetadataService, plan: *const metadata_reconciler.ReconciliationPlan) !void {
        std.debug.assert(plan.placement_upserts.len == plan.placement_upsert_preconditions.len);
        for (plan.placement_upserts, plan.placement_upsert_preconditions) |intent, precondition| {
            try self.upsertReplicaIntent(
                intent,
                precondition.expected_metadata_version,
                precondition.expected_version_fence,
                precondition.expected_target_drain_requested,
            );
        }
        for (plan.table_upserts) |record| try self.upsertTable(record);
        for (plan.split_admissions) |admission| try self.admitSplitTransition(admission);
        for (plan.range_upserts) |record| try self.upsertRange(record);
        for (plan.split_upserts) |record| try self.upsertSplitTransition(record);
        for (plan.merge_upserts) |record| try self.upsertMergeTransition(record);
        for (plan.placement_removals) |record| try self.removeReplicaIntent(record.group_id, record.local_node_id, record.expected_metadata_version);
        for (plan.table_removals) |table_id| try self.removeTable(table_id);
        for (plan.range_removals) |group_id| try self.removeRange(group_id);
        for (plan.split_removals) |transition_id| try self.removeSplitTransition(transition_id);
        for (plan.merge_removals) |transition_id| try self.removeMergeTransition(transition_id);
        if (plan.clear_reallocation_request) |expected| try self.clearReallocationRequest(expected);
    }

    pub fn observeSplitTransition(self: *MetadataService, transition_id: u64) !?transition_state.SplitObservation {
        self.lockTransitions();
        defer self.unlockTransitions();
        return try self.raft.observeSplitTransition(transition_id);
    }

    pub fn observeMergeTransition(self: *MetadataService, transition_id: u64) !?transition_state.MergeObservation {
        self.lockTransitions();
        defer self.unlockTransitions();
        return try self.raft.observeMergeTransition(transition_id);
    }

    pub fn syncPending(self: *MetadataService) !raft_managed_host.ManagedSyncResult {
        self.control_round_mutex.lockUncancelable(std.Options.debug_io);
        defer self.control_round_mutex.unlock(std.Options.debug_io);
        self.lockTransitions();
        defer self.unlockTransitions();
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.syncPending();
    }

    pub fn metrics(self: *MetadataService) raft_service.ManagedServiceMetrics {
        self.lockTransitions();
        defer self.unlockTransitions();
        self.lockRuntime();
        defer self.unlockRuntime();
        return self.raft.metrics;
    }

    pub fn recordJsonResponseAllocation(self: *MetadataService, bytes: usize) void {
        recordJsonResponseAllocationCounters(
            &self.json_response_calls,
            &self.json_response_bytes_total,
            &self.json_response_peak_bytes,
            bytes,
        );
    }

    pub fn jsonResponseDiagnostics(self: *MetadataService) JsonResponseDiagnostics {
        return jsonResponseDiagnosticsFromCounters(
            &self.json_response_calls,
            &self.json_response_bytes_total,
            &self.json_response_peak_bytes,
        );
    }

    pub fn head(self: *MetadataService) metadata_api.MetadataHead {
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadataIncarnation() catch null,
            .metadata_epoch = projectedProvisioningFingerprint(self.alloc, self) catch self.lifecycle_signal.currentEpoch(),
        };
    }

    pub fn runtimeTopology(self: *MetadataService) !metadata_api.MetadataRuntimeTopology {
        return metadataRuntimeTopology(
            self.metadata_group_id,
            try self.metadataIncarnation(),
            serviceGroupRaftTopologyObservation(self, self.metadata_group_id),
        );
    }

    pub fn status(self: *MetadataService) !MetadataStatus {
        var current_status = try snapshotStatus(self.alloc, self.metadata_group_id, self, self.metrics());
        current_status.metadata_epoch = self.lifecycle_signal.currentEpoch();
        return current_status;
    }

    pub fn metadataStatus(self: *MetadataService) !MetadataStatus {
        var current_status = try snapshotStatusWithOptions(self.alloc, self.metadata_group_id, self, self.metrics(), .{
            .include_reconciliation_planning = true,
        });
        current_status.metadata_epoch = self.lifecycle_signal.currentEpoch();
        return current_status;
    }

    pub fn adminSnapshot(self: *MetadataService) !metadata_api.AdminSnapshot {
        return try metadata_api.captureSnapshot(self.alloc, self);
    }

    pub fn adminSnapshotFence(self: *MetadataService) !AdminSnapshotFence {
        const raft = serviceGroupRaftObservation(self, self.metadata_group_id);
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = try self.metadataIncarnation(),
            .metadata_raft_term = raft.term,
            .metadata_raft_commit_index = raft.commit_index,
            .metadata_raft_applied_index = raft.applied_index,
            .projection_epoch = self.projection_epoch.load(.acquire),
            .catalog_epoch = self.catalog_epoch.load(.acquire),
            .placement_epoch = self.placement_epoch.load(.acquire),
            .reconcile_lease_epoch = self.reconcile_lease_epoch.load(.acquire),
            .transition_epoch = self.transition_epoch.load(.acquire),
        };
    }

    pub fn validatePublication(self: *MetadataService, contract: metadata_api.CatalogPublicationContract) !bool {
        try self.ensureLinearizableRead();
        self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
        defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
        const incarnation = try self.metadataIncarnation();
        const snapshot = try self.catalogValidationSnapshotLocked();
        return snapshot.index.matchesPublication(contract, self.metadata_group_id, incarnation, snapshot.tables, snapshot.ranges);
    }

    pub fn validateTablePublication(self: *MetadataService, contract: metadata_api.CatalogTablePublicationContract) !bool {
        try self.ensureLinearizableRead();
        self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
        defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
        const incarnation = try self.metadataIncarnation();
        const snapshot = try self.catalogValidationSnapshotLocked();
        return snapshot.index.matchesTablePublication(contract, self.metadata_group_id, incarnation, snapshot.tables);
    }

    fn captureCatalogValidationSnapshot(self: *MetadataService) !CatalogValidationSnapshot {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        var snapshot: CatalogValidationSnapshot = .{};
        errdefer snapshot.deinit(self.alloc);
        snapshot.tables = try store.listTables(self.alloc, self.metadata_group_id);
        snapshot.ranges = try store.listRanges(self.alloc, self.metadata_group_id);
        snapshot.index = try metadata_api.CatalogProjectionIndex.init(self.alloc, snapshot.tables, snapshot.ranges);
        return snapshot;
    }

    fn catalogValidationSnapshotLocked(self: *MetadataService) !*const CatalogValidationSnapshot {
        try self.ensureLifecycleListenerRegistered();
        const current_epoch = self.catalog_epoch.load(.acquire);
        if (self.catalog_validation_cache.snapshot != null and
            self.catalog_validation_cache.catalog_epoch == current_epoch)
        {
            return &(self.catalog_validation_cache.snapshot orelse unreachable);
        }

        // MetadataService does not own the HTTP runtime lock. Stabilize the
        // two projected lists against the listener epoch instead, so a table
        // and its ranges can never come from different applied revisions.
        var attempts: usize = 0;
        while (attempts < 4) : (attempts += 1) {
            const before = self.catalog_epoch.load(.acquire);
            var fresh = try self.captureCatalogValidationSnapshot();
            errdefer fresh.deinit(self.alloc);
            const after = self.catalog_epoch.load(.acquire);
            if (before != after) {
                fresh.deinit(self.alloc);
                continue;
            }
            if (self.catalog_validation_cache.snapshot) |*snapshot| snapshot.deinit(self.alloc);
            self.catalog_validation_cache = .{ .catalog_epoch = after, .snapshot = fresh };
            return &(self.catalog_validation_cache.snapshot orelse unreachable);
        }
        return error.CatalogProjectionUnstable;
    }

    pub fn freeAdminSnapshot(self: *MetadataService, snapshot: *metadata_api.AdminSnapshot) void {
        metadata_api.freeSnapshot(self.alloc, self, snapshot);
    }

    pub fn projectedStore(self: *MetadataService) ?*metadata_storage.RaftApplyStore {
        return self.raft.host.owned_metadata_store;
    }

    pub fn metadataIncarnation(self: *MetadataService) !?metadata_mod.MetadataClusterIncarnation {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getMetadataIncarnation(self.metadata_group_id);
    }

    fn ensureMetadataIncarnation(self: *MetadataService) !bool {
        if (try self.metadataIncarnation() != null) {
            self.metadata_incarnation_proposal_pending = false;
            return true;
        }
        if (!self.isLocalMetadataLeader()) {
            self.metadata_incarnation_proposal_pending = false;
            return false;
        }
        if (self.metadata_incarnation_proposal_pending) return false;
        if (self.metadata_incarnation_candidate == null) {
            self.metadata_incarnation_candidate = try metadata_mod.incarnation.generate(std.Options.debug_io);
        }
        self.proposeTransitionCommand(.{
            .initialize_metadata_incarnation = self.metadata_incarnation_candidate.?,
        }) catch |err| switch (err) {
            error.NotLeader => return false,
            else => return err,
        };
        self.metadata_incarnation_proposal_pending = true;
        return false;
    }

    pub fn getProjectedReconcileLease(self: *MetadataService) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getReconcileLease(self.metadata_group_id);
    }

    pub fn getProjectedShuffleJoinLease(self: *MetadataService, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getShuffleJoinLease(self.metadata_group_id, job_id);
    }

    pub fn getProjectedReallocationRequest(self: *MetadataService) !?metadata_mod.ReallocationRequestRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getReallocationRequest(self.metadata_group_id);
    }

    pub fn reconcileLeaseStats(self: *MetadataService) metadata_reconcile_lease.Stats {
        self.reconcile_lease_mutex.lockUncancelable(std.Options.debug_io);
        defer self.reconcile_lease_mutex.unlock(std.Options.debug_io);
        return self.reconcile_lease.stats();
    }

    pub fn listProjectedTables(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listTables(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedTables(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeTables(alloc, records);
    }

    pub fn listProjectedSchemaProgress(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.SchemaProgressRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listSchemaProgress(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedSchemaProgress(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.SchemaProgressRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeSchemaProgress(alloc, records);
    }

    pub fn listProjectedRestoreProgress(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.RestoreProgressRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listRestoreProgress(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedRestoreProgress(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.RestoreProgressRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeRestoreProgress(alloc, records);
    }

    pub fn listProjectedReplicationSourceStatuses(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listReplicationSourceStatuses(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedReplicationSourceStatuses(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeReplicationSourceStatuses(alloc, records);
    }

    pub fn listProjectedExtensionPackages(self: *MetadataService, alloc: std.mem.Allocator) ![]extension_domain.PackageManifest {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listExtensionPackages(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedExtensionPackages(self: *MetadataService, alloc: std.mem.Allocator, records: []extension_domain.PackageManifest) void {
        const store = self.projectedStore() orelse return;
        store.freeExtensionPackages(alloc, records);
    }

    pub fn listProjectedInstalledExtensions(self: *MetadataService, alloc: std.mem.Allocator) ![]extension_domain.InstalledExtension {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listInstalledExtensions(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedInstalledExtensions(self: *MetadataService, alloc: std.mem.Allocator, records: []extension_domain.InstalledExtension) void {
        const store = self.projectedStore() orelse return;
        store.freeInstalledExtensions(alloc, records);
    }

    pub fn listProjectedExtensionMembers(self: *MetadataService, alloc: std.mem.Allocator) ![]extension_domain.ExtensionMember {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listExtensionMembers(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedExtensionMembers(self: *MetadataService, alloc: std.mem.Allocator, records: []extension_domain.ExtensionMember) void {
        const store = self.projectedStore() orelse return;
        store.freeExtensionMembers(alloc, records);
    }

    pub fn listProjectedExtensionDependencies(self: *MetadataService, alloc: std.mem.Allocator) ![]extension_domain.ExtensionDependency {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listExtensionDependencies(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedExtensionDependencies(self: *MetadataService, alloc: std.mem.Allocator, records: []extension_domain.ExtensionDependency) void {
        const store = self.projectedStore() orelse return;
        store.freeExtensionDependencies(alloc, records);
    }

    pub fn listProjectedShuffleJoinLeases(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.ShuffleJoinLeaseRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listShuffleJoinLeases(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedShuffleJoinLeases(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.ShuffleJoinLeaseRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeShuffleJoinLeases(alloc, records);
    }

    pub fn listLocalBootstrapStatuses(self: *MetadataService, alloc: std.mem.Allocator) ![]raft_host.BootstrapStatus {
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.host.host.listBootstrapStatuses(alloc);
    }

    pub fn freeLocalBootstrapStatuses(self: *MetadataService, alloc: std.mem.Allocator, statuses: []raft_host.BootstrapStatus) void {
        self.raft.host.host.freeBootstrapStatuses(alloc, statuses);
    }

    pub fn listProjectedRanges(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listRanges(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedRanges(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeRanges(alloc, records);
    }

    pub fn listProjectedPlacementIntents(self: *MetadataService, alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listPlacementIntents(alloc, self.metadata_group_id);
    }

    pub fn listProjectedPlacementVersionFences(
        self: *MetadataService,
        alloc: std.mem.Allocator,
    ) ![]metadata_reconciler.PlacementVersionFence {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listPlacementVersionFences(alloc, self.metadata_group_id);
    }

    pub fn listProjectedNodes(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.NodeRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listNodes(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedNodes(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.NodeRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeNodes(alloc, records);
    }

    pub fn listProjectedStores(self: *MetadataService, alloc: std.mem.Allocator) ![]metadata_table_manager.StoreRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listStores(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedStores(self: *MetadataService, alloc: std.mem.Allocator, records: []metadata_table_manager.StoreRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeStores(alloc, records);
    }

    pub fn freeProjectedPlacementIntents(self: *MetadataService, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        const store = self.projectedStore() orelse return;
        store.freePlacementIntents(alloc, intents);
    }

    pub fn listProjectedSplitTransitions(self: *MetadataService, alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listSplitTransitions(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedSplitTransitions(self: *MetadataService, alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeSplitTransitions(alloc, records);
    }

    pub fn listProjectedMergeTransitions(self: *MetadataService, alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listMergeTransitions(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedMergeTransitions(self: *MetadataService, alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeMergeTransitions(alloc, records);
    }

    fn refreshLocalPlacementIntents(self: *MetadataService) !void {
        self.placement_reconcile_mutex.lockUncancelable(std.Options.debug_io);
        defer self.placement_reconcile_mutex.unlock(std.Options.debug_io);

        const current_epoch = self.placement_epoch.load(.monotonic);
        if (!shouldRefreshLocalEpoch(
            self.local_placement_epoch,
            self.last_local_placement_refresh_at_ms,
            current_epoch,
            local_placement_refresh_interval_ms,
        )) return;

        const projected = try self.listProjectedPlacementIntents(self.alloc);
        defer self.freeProjectedPlacementIntents(self.alloc, projected);

        var local = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        defer {
            for (local.items) |intent| raft_reconciler.freeIntentOwned(self.alloc, intent);
            local.deinit(self.alloc);
        }

        const local_node_id = self.raft.host.host.cfg.local_node_id;
        for (projected) |intent| {
            if (intent.record.local_node_id != local_node_id) continue;
            try local.append(self.alloc, try raft_reconciler.cloneIntentOwned(self.alloc, intent));
        }

        var reconcile = reconcile: {
            self.lockRuntime();
            defer self.unlockRuntime();
            if (!containsLocalIntent(local.items, self.metadata_group_id)) {
                if (self.raft.host.host.raftStatus(self.metadata_group_id)) |raft_status| {
                    try local.append(self.alloc, .{
                        .record = .{
                            .group_id = self.metadata_group_id,
                            .replica_id = local_node_id,
                            .local_node_id = local_node_id,
                            .bootstrap_mode = .persisted,
                        },
                        .peer_node_ids = try allocPeerNodeIdsExcludingSelf(
                            self.alloc,
                            raft_status.conf_state.voters,
                            local_node_id,
                        ),
                    });
                }
            }
            try self.raft.host.replacePlacementIntents(local.items);
            var prepared = try self.raft.host.prepareReconcile();
            prepared.beginPreparation();
            break :reconcile prepared;
        };
        defer reconcile.deinit();

        reconcile.prepareDurable() catch |err| {
            self.lockRuntime();
            defer self.unlockRuntime();
            reconcile.notePreparationFailure(err);
            return err;
        };

        self.lockRuntime();
        if (self.placement_epoch.load(.monotonic) != current_epoch) {
            self.unlockRuntime();
            try reconcile.abortDurable();
            self.local_placement_epoch = null;
            return;
        }
        defer self.unlockRuntime();
        _ = try reconcile.commit();
        self.local_placement_epoch = current_epoch;
        self.last_local_placement_refresh_at_ms = nowMs();
    }

    fn refreshLocalTransitions(self: *MetadataService) !void {
        const current_epoch = self.transition_epoch.load(.monotonic);
        if (!shouldRefreshLocalEpoch(
            self.local_transition_epoch,
            self.last_local_transition_refresh_at_ms,
            current_epoch,
            local_transition_refresh_interval_ms,
        )) return;

        const split_records = try self.listProjectedSplitTransitions(self.alloc);
        defer self.freeProjectedSplitTransitions(self.alloc, split_records);
        const merge_records = try self.listProjectedMergeTransitions(self.alloc);
        defer self.freeProjectedMergeTransitions(self.alloc, merge_records);

        self.lockTransitions();
        defer self.unlockTransitions();
        const transition_svc = if (self.raft.transition_svc) |*svc| svc else return;
        var split_index: usize = 0;
        while (split_index < transition_svc.pending_split.items.len) {
            const transition_id = transition_svc.pending_split.items[split_index].transition_id;
            if (findProjectedSplit(split_records, transition_id) == null) {
                _ = transition_svc.removeSplit(transition_id);
                continue;
            }
            split_index += 1;
        }
        for (split_records) |record| {
            if (findQueuedSplit(transition_svc.pending_split.items, record.transition_id) == null and
                !transition_svc.hasCompletedSplit(record.transition_id))
            {
                try transition_svc.submitSplit(record);
            }
        }

        var merge_index: usize = 0;
        while (merge_index < transition_svc.pending_merge.items.len) {
            const transition_id = transition_svc.pending_merge.items[merge_index].transition_id;
            if (findProjectedMerge(merge_records, transition_id) == null) {
                _ = transition_svc.removeMerge(transition_id);
                continue;
            }
            merge_index += 1;
        }
        for (merge_records) |record| {
            if (findQueuedMerge(transition_svc.pending_merge.items, record.transition_id) == null and
                !transition_svc.hasCompletedMerge(record.transition_id))
            {
                try transition_svc.submitMerge(record);
            }
        }

        self.raft.metrics.queued_split_transitions = transition_svc.metrics.queued_split_transitions;
        self.raft.metrics.queued_merge_transitions = transition_svc.metrics.queued_merge_transitions;
        self.local_transition_epoch = current_epoch;
        self.last_local_transition_refresh_at_ms = nowMs();
    }

    fn refreshLocalTableProvisioning(self: *MetadataService) !metadata_table_provisioner.ProvisionSummary {
        const replica_root_dir = self.replica_root_dir orelse return .{};
        const current_epoch = self.projection_epoch.load(.monotonic);
        const group_ids = try self.listLocalGroupIds(self.alloc);
        defer self.alloc.free(group_ids);
        const group_ids_fingerprint = groupIdsFingerprint(group_ids);
        if (!shouldRefreshLocalProjection(
            self.local_table_provisioning_epoch,
            self.local_table_provisioning_group_ids_fingerprint,
            self.last_local_table_provisioning_refresh_at_ms,
            current_epoch,
            group_ids_fingerprint,
            local_table_provisioning_refresh_interval_ms,
        )) return .{};

        const tables = try self.listProjectedTables(self.alloc);
        defer self.freeProjectedTables(self.alloc, tables);
        const ranges = try self.listProjectedRanges(self.alloc);
        defer self.freeProjectedRanges(self.alloc, ranges);
        const fingerprint = metadata_table_provisioner.provisioningFingerprint(
            self.metadata_group_id,
            group_ids,
            tables,
            ranges,
        );
        self.local_table_provisioning_epoch = current_epoch;
        self.local_table_provisioning_group_ids_fingerprint = group_ids_fingerprint;
        self.last_local_table_provisioning_refresh_at_ms = nowMs();
        if (self.local_table_provisioning_fingerprint == fingerprint) {
            try self.refreshLocalRestoreProgress(group_ids, tables, ranges);
            return .{};
        }
        if (self.local_replica_root_reconcile_permit_hook) |hook| {
            if (!hook.shouldReconcile()) return .{};
        }
        const summary: metadata_table_provisioner.ProvisionSummary = if (self.local_replica_root_reconcile_hook) |hook| owner: {
            // A configured hook is the local writer owner. Delegating avoids
            // opening a second DB writer and then notifying the owner after
            // the mutation has already happened.
            break :owner try hook.run(.{
                .metadata_group_id = self.metadata_group_id,
                .group_ids = group_ids,
                .tables = tables,
                .ranges = ranges,
            });
        } else try metadata_table_provisioner.reconcileReplicaRootWithOptions(
            self.alloc,
            replica_root_dir,
            self.metadata_group_id,
            group_ids,
            tables,
            ranges,
            .{
                .backend_runtime = try self.ensureBackendRuntime(),
                .destination_authorizer = self.destination_authorizer,
            },
        );
        try self.refreshLocalRestoreProgress(group_ids, tables, ranges);
        if (summary.indexes_pending != 0) return summary;
        self.local_table_provisioning_fingerprint = fingerprint;
        self.local_schema_progress_epoch = null;
        self.local_schema_progress_group_ids_fingerprint = null;
        self.last_local_schema_progress_refresh_at_ms = 0;
        try self.refreshLocalSchemaProgress();
        return summary;
    }

    fn refreshLocalRestoreProgress(
        self: *MetadataService,
        group_ids: []const u64,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
    ) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.host.cfg.local_node_id;
        const local_progress = try metadata_table_provisioner.collectLocalRestoreProgressUsingIo(
            self.alloc,
            (try self.ensureBackendRuntime()).io(),
            replica_root_dir,
            self.metadata_group_id,
            local_node_id,
            group_ids,
            tables,
            ranges,
        );
        defer {
            for (local_progress) |record| metadata_table_manager.freeRestoreProgress(self.alloc, record);
            self.alloc.free(local_progress);
        }
        const projected_progress = try self.listProjectedRestoreProgress(self.alloc);
        defer self.freeProjectedRestoreProgress(self.alloc, projected_progress);
        try syncLocalRestoreProgress(self, local_node_id, local_progress, projected_progress);
    }

    fn refreshLocalSchemaProgress(self: *MetadataService) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.host.cfg.local_node_id;
        const current_epoch = self.projection_epoch.load(.monotonic);
        const group_ids = try self.listLocalGroupIds(self.alloc);
        defer self.alloc.free(group_ids);
        const group_ids_fingerprint = groupIdsFingerprint(group_ids);
        if (!shouldRefreshLocalProjection(
            self.local_schema_progress_epoch,
            self.local_schema_progress_group_ids_fingerprint,
            self.last_local_schema_progress_refresh_at_ms,
            current_epoch,
            group_ids_fingerprint,
            local_schema_progress_refresh_interval_ms,
        )) return;

        const tables = try self.listProjectedTables(self.alloc);
        defer self.freeProjectedTables(self.alloc, tables);
        const ranges = try self.listProjectedRanges(self.alloc);
        defer self.freeProjectedRanges(self.alloc, ranges);
        const stores = try self.listProjectedStores(self.alloc);
        defer self.freeProjectedStores(self.alloc, stores);
        var local_progress = try metadata_table_provisioner.collectLocalSchemaProgressFromRuntime(
            self.alloc,
            local_node_id,
            group_ids,
            tables,
            ranges,
            stores,
        );
        defer self.alloc.free(local_progress);
        if (local_progress.len == 0 and !metadata_table_provisioner.localSchemaRuntimeCoverageComplete(
            local_node_id,
            group_ids,
            tables,
            ranges,
            stores,
        )) {
            self.alloc.free(local_progress);
            const backend_runtime = try self.ensureBackendRuntime();
            var fallback_shard_db = metadata_mod.FallbackLocalShardDbAdapter{
                .replica_root_dir = replica_root_dir,
                .backend_runtime = backend_runtime,
            };
            const shard_db = self.local_shard_db_adapter orelse fallback_shard_db.adapter();
            local_progress = try metadata_table_provisioner.collectLocalSchemaProgressWithOptions(
                self.alloc,
                replica_root_dir,
                self.metadata_group_id,
                local_node_id,
                group_ids,
                tables,
                ranges,
                .{
                    .backend_runtime = backend_runtime,
                    .shard_db_adapter = shard_db,
                },
            );
        }
        const projected_progress = try self.listProjectedSchemaProgress(self.alloc);
        defer self.freeProjectedSchemaProgress(self.alloc, projected_progress);
        try syncLocalSchemaProgress(self, local_node_id, local_progress, projected_progress);
        self.local_schema_progress_epoch = current_epoch;
        self.local_schema_progress_group_ids_fingerprint = group_ids_fingerprint;
        self.last_local_schema_progress_refresh_at_ms = nowMs();
    }

    fn refreshLocalStoreStatus(self: *MetadataService) !void {
        try self.refreshLocalStoreStatusWithBackfillMarkers(null, true);
    }

    fn refreshLocalStoreStatusWithBackfillMarkers(
        self: *MetadataService,
        backfill_markers: ?[]const StoreStatusBackfillMarker,
        use_provider: bool,
    ) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.host.cfg.local_node_id;
        try syncLocalStoreStatus(self, local_node_id, replica_root_dir, backfill_markers, use_provider);
    }

    fn refreshStoreStatusBackfillMarkersForRound(self: *MetadataService) ![]const StoreStatusBackfillMarker {
        self.store_status_ticks += 1;
        self.store_status_backfill_probe_ticks += 1;
        const replica_root_dir = self.replica_root_dir orelse return &.{};
        try maybeRefreshStoreStatusBackfillMarkerCache(
            self.alloc,
            replica_root_dir,
            self.store_status_ticks,
            &self.store_status_backfill_probe_ticks,
            &self.store_status_backfill_marker_cache,
        );
        return self.store_status_backfill_marker_cache.markers;
    }

    fn refreshStoreStatusBackfillMarkersForLifecycleRound(self: *MetadataService) ![]const StoreStatusBackfillMarker {
        const replica_root_dir = self.replica_root_dir orelse return &.{};
        if (self.store_status_backfill_marker_cache.markers.len == 0 and self.store_status_backfill_marker_cache.scanned_at_ms == 0) {
            try refreshStoreStatusBackfillMarkerCacheNow(
                self.alloc,
                replica_root_dir,
                &self.store_status_backfill_probe_ticks,
                &self.store_status_backfill_marker_cache,
            );
            return self.store_status_backfill_marker_cache.markers;
        }

        self.store_status_backfill_probe_ticks += 1;
        try maybeRefreshStoreStatusBackfillMarkerCache(
            self.alloc,
            replica_root_dir,
            0,
            &self.store_status_backfill_probe_ticks,
            &self.store_status_backfill_marker_cache,
        );
        return self.store_status_backfill_marker_cache.markers;
    }

    fn completeRestoreIntentsIfReady(self: *MetadataService) !void {
        try completeRestoreIntentsForService(self, null, null, null, null);
    }

    fn ensureReconcileLease(self: *MetadataService) !bool {
        self.reconcile_lease_mutex.lockUncancelable(std.Options.debug_io);
        defer self.reconcile_lease_mutex.unlock(std.Options.debug_io);
        return try self.ensureReconcileLeaseLocked();
    }

    fn ensureReconcileLeaseLocked(self: *MetadataService) !bool {
        const now_ms = self.reconcile_lease.nowMs();
        const is_local_leader = self.isLocalMetadataLeader();
        const projected = self.getCachedProjectedReconcileLease(now_ms, is_local_leader) catch |err| switch (err) {
            error.MissingMetadataStore => null,
            else => return err,
        };
        const has_lease = self.reconcile_lease.observe(is_local_leader, projected, now_ms);
        if (self.reconcile_lease.shouldRenew(is_local_leader, projected, now_ms)) {
            self.upsertReconcileLease(self.reconcile_lease.desiredRecord(now_ms)) catch |err| {
                self.reconcile_lease.noteAcquireFailure();
                return err;
            };
        }
        return has_lease;
    }

    fn getCachedProjectedReconcileLease(
        self: *MetadataService,
        now_ms: u64,
        is_local_leader: bool,
    ) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        const current_epoch = self.reconcile_lease_epoch.load(.monotonic);
        if (self.reconcile_lease_projection_cache.epoch == current_epoch and
            now_ms < self.reconcile_lease_projection_cache.next_refresh_at_ms)
        {
            return self.reconcile_lease_projection_cache.record;
        }
        const projected = try self.getProjectedReconcileLease();
        self.reconcile_lease_projection_cache = .{
            .epoch = current_epoch,
            .record = projected,
            .next_refresh_at_ms = reconcileLeaseCacheNextRefreshAtMs(&self.reconcile_lease, is_local_leader, projected, now_ms),
        };
        return projected;
    }

    fn statusProjectedReconcileLease(self: *MetadataService, now_ms: u64) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        return try self.getCachedProjectedReconcileLease(now_ms, self.isLocalMetadataLeader());
    }

    fn runLifecycleReconcileHookIfRequested(self: *MetadataService) !void {
        const hook = self.lifecycle_reconcile_hook orelse return;
        if (!self.lifecycle_reconcile_requested.swap(false, .acq_rel)) return;
        try hook.run();
    }

    fn runReplicationBackfillRound(self: *MetadataService) !void {
        try scheduleReplicationBackfillJob(self);
    }

    fn runReplicationBackfillWorker(self: *MetadataService) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        try ensureCdcWorkPermit(self);

        const runtime = try self.ensureBackendRuntime();
        var write_source = api_table_writes.ProvisionedTableWriteSource.init(
            replica_root_dir,
            api_table_catalog.CatalogSource.fromMetadataService(self),
        );
        write_source.backend_runtime = runtime;
        _ = write_source.withSecretStore(self.secret_store);
        _ = write_source.withDestinationAuthorization(self.destination_authorizer);
        var coordinator = metadata_replication_backfill.SnapshotBackfillCoordinator{
            .alloc = self.alloc,
            .runner = .{
                .alloc = self.alloc,
                .io = runtime.io() orelse std.Options.debug_io,
                .registry = &self.cdc_backfill_registry,
                .write_source = write_source.source(),
                .secret_store = self.secret_store,
                .work_permit = cdcWorkPermit(self),
                .destination_authorizer = self.destination_authorizer,
            },
        };
        const summary = coordinator.runRound(self) catch |err| {
            if (!isExpectedCdcRoundError(err)) return err;
            std.log.warn("metadata cdc snapshot round skipped: {s}", .{@errorName(err)});
            return;
        };
        if (summary.sources_considered > 0) {
            std.log.info(
                "metadata cdc snapshot round tables={d} sources={d} started={d} resumed={d} completed={d} yielded={d}",
                .{
                    summary.tables_considered,
                    summary.sources_considered,
                    summary.sources_started,
                    summary.sources_resumed,
                    summary.sources_completed,
                    summary.sources_yielded,
                },
            );
        }
        var streaming = metadata_replication_backfill.StreamingReplicationCoordinator{
            .alloc = self.alloc,
            .runner = .{
                .alloc = self.alloc,
                .registry = &self.cdc_backfill_registry,
                .write_source = write_source.source(),
                .secret_store = self.secret_store,
                .work_permit = cdcWorkPermit(self),
                .destination_authorizer = self.destination_authorizer,
            },
        };
        const stream_summary = streaming.runRound(self) catch |err| {
            if (!isExpectedCdcRoundError(err)) return err;
            std.log.warn("metadata cdc streaming round skipped: {s}", .{@errorName(err)});
            return;
        };
        if (stream_summary.sources_considered > 0) {
            std.log.info(
                "metadata cdc streaming round tables={d} sources={d} started={d} resumed={d} skipped={d} polled={d} changes={d}",
                .{
                    stream_summary.tables_considered,
                    stream_summary.sources_considered,
                    stream_summary.sources_started,
                    stream_summary.sources_resumed,
                    stream_summary.sources_skipped_pending_snapshot,
                    stream_summary.sources_polled,
                    stream_summary.changes_applied,
                },
            );
        }
    }
};

pub const MetadataHttpService = struct {
    alloc: std.mem.Allocator,
    metadata_group_id: u64,
    replica_root_dir: ?[]const u8,
    observe_local_replica_root: bool,
    reallocation_protocol_peers: []const ReallocationProtocolPeer,
    store_status_ticks: usize,
    projection_epoch: std.atomic.Value(u64) = .init(1),
    catalog_epoch: std.atomic.Value(u64) = .init(1),
    projected_core_epoch: std.atomic.Value(u64) = .init(1),
    transition_readiness_epoch: std.atomic.Value(u64) = .init(1),
    placement_epoch: std.atomic.Value(u64) = .init(1),
    reconcile_lease_epoch: std.atomic.Value(u64) = .init(1),
    transition_epoch: std.atomic.Value(u64) = .init(1),
    metadata_incarnation_candidate: ?metadata_mod.MetadataClusterIncarnation = null,
    metadata_incarnation_proposal_pending: bool = false,
    local_placement_epoch: ?u64,
    last_local_placement_refresh_at_ms: u64,
    local_transition_epoch: ?u64,
    last_local_transition_refresh_at_ms: u64,
    local_table_provisioning_fingerprint: ?u64,
    local_table_provisioning_epoch: ?u64,
    local_table_provisioning_group_ids_fingerprint: ?u64,
    last_local_table_provisioning_refresh_at_ms: u64,
    local_schema_progress_epoch: ?u64,
    local_schema_progress_group_ids_fingerprint: ?u64,
    last_local_schema_progress_refresh_at_ms: u64,
    cdc_runtime_mutex: std.Io.Mutex = .init,
    reconcile_lease_mutex: std.Io.Mutex = .init,
    cdc_job_in_flight: std.atomic.Value(bool) = .init(false),
    cdc_shutdown: std.atomic.Value(bool) = .init(false),
    cdc_permit_check_after_ns: std.atomic.Value(u64) = .init(0),
    cdc_job_owner_id: u64 = 0,
    reconcile_lease: metadata_reconcile_lease.State,
    runtime_mutex: std.Io.Mutex = .init,
    placement_reconcile_mutex: std.Io.Mutex = .init,
    transition_mutex: std.Io.Mutex = .init,
    transition_metrics_mutex: std.Io.Mutex = .init,
    transition_metrics_snapshot: raft_transition_service.TransitionServiceMetrics = .{},
    // One request drives low-latency Ready processing for every outstanding
    // metadata proposal. Other requests sleep on the driver's handoff signal,
    // avoiding a per-waiter Raft round and false lifecycle epoch changes.
    proposal_progress_driver: MetadataProposalProgressDriver = .{},
    lifecycle_signal: LifecycleSignal,
    lifecycle_reconcile_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    lifecycle_reconcile_hook: ?LifecycleReconcileHook = null,
    local_replica_root_reconcile_hook: ?LocalReplicaRootReconcileHook = null,
    local_replica_root_reconcile_permit_hook: ?LocalReplicaRootReconcilePermitHook = null,
    lifecycle_listener_mutex: std.Io.Mutex = .init,
    lifecycle_listener_registered: bool = false,
    catalog_validation_mutex: std.Io.Mutex = .init,
    catalog_validation_cache: CatalogValidationSnapshotCache = .{},
    cdc_write_source_override: ?api_table_writes.TableWriteSource = null,
    local_group_status_provider: ?LocalGroupStatusProvider = null,
    local_shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    routed_shard_db_adapter: ?metadata_mod.ShardDbAdapter = null,
    reconcile_lease_projection_cache: ReconcileLeaseProjectionCache = .{},
    projected_core_snapshot_cache: ProjectedCoreSnapshotCache = .{},
    transition_readiness_mutex: std.Io.Mutex = .init,
    transition_readiness_cache: TransitionReadinessCache = .{},
    metadata_status_cache_mutex: std.Io.Mutex = .init,
    metadata_status_cache_valid: bool = false,
    metadata_status_cache: MetadataStatus = .{ .metadata_group_id = 0, .metrics = .{} },
    metadata_status_cache_next_refresh_at_ms: u64 = 0,
    metadata_status_cache_projection_epoch: u64 = 0,
    metadata_status_cache_placement_epoch: u64 = 0,
    metadata_status_cache_transition_epoch: u64 = 0,
    probe_ready: std.atomic.Value(bool) = .init(false),
    runtime_status_protocol_cache_mutex: std.Io.Mutex = .init,
    runtime_status_protocol_ready_fingerprint: RuntimeStatusMembershipFingerprint =
        [_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length,
    runtime_status_protocol_probe_mutex: std.Io.Mutex = .init,
    runtime_status_protocol_activated_version: std.atomic.Value(u16) = .init(0),
    runtime_status_protocol_probe_in_flight: std.atomic.Value(bool) = .init(false),
    runtime_status_protocol_probe_shutdown: std.atomic.Value(bool) = .init(false),
    runtime_status_protocol_probe_failures: std.atomic.Value(u32) = .init(0),
    runtime_status_protocol_next_probe_ns: std.atomic.Value(u64) = .init(0),
    runtime_status_protocol_probe_owner_id: u64 = 0,
    store_status_backfill_probe_ticks: usize = 0,
    store_status_backfill_marker_cache: StoreStatusBackfillMarkerCache = .{},
    cdc_backfill_registry: foreign_mod.Registry = .{},
    cdc_next_round_at_ms: u64 = 0,
    secret_store: ?*common_secrets.FileStore = null,
    internal_service_secret: ?[]const u8 = null,
    internal_service_issuer: ?[]const u8 = null,
    destination_authorizer: ?stored_destination_authorization.Authorizer = null,
    backend_runtime_mutex: std.Io.Mutex = .init,
    backend_runtime: ?*backend_runtime_mod.BackendRuntime = null,
    owned_backend_runtime: ?backend_runtime_mod.BackendRuntimeHandle = null,
    linearizable_read_tracker: *LinearizableMetadataReadTracker,
    json_response_calls: std.atomic.Value(u64) = .init(0),
    json_response_bytes_total: std.atomic.Value(u64) = .init(0),
    json_response_peak_bytes: std.atomic.Value(u64) = .init(0),
    raft: raft_service.ManagedHttpHostService,

    pub fn init(
        alloc: std.mem.Allocator,
        host_cfg: raft_managed_host.ManagedHttpHostConfig,
        deps: MetadataHttpServiceDeps,
        cfg: MetadataServiceConfig,
    ) !MetadataHttpService {
        const metadata_group_id = host_cfg.http.host.metadata_group_id orelse return error.MissingMetadataGroupId;
        var owned_backend_runtime: ?backend_runtime_mod.BackendRuntimeHandle = null;
        errdefer if (owned_backend_runtime) |*runtime| runtime.deinit();
        const backend_runtime = cfg.backend_runtime orelse blk: {
            owned_backend_runtime = try backend_runtime_mod.BackendRuntimeHandle.init(alloc, .{});
            break :blk owned_backend_runtime.?.ptr();
        };
        var http_deps = deps.http;
        http_deps.http.backend_runtime = backend_runtime;
        const read_tracker = try alloc.create(LinearizableMetadataReadTracker);
        var read_tracker_owned = true;
        read_tracker.* = .{
            .alloc = alloc,
            .metadata_group_id = metadata_group_id,
            .downstream = http_deps.read_state_observer,
        };
        errdefer if (read_tracker_owned) {
            read_tracker.deinit();
            alloc.destroy(read_tracker);
        };
        http_deps.read_state_observer = read_tracker.observer();
        var service = MetadataHttpService{
            .alloc = alloc,
            .metadata_group_id = metadata_group_id,
            .replica_root_dir = host_cfg.http.host.replica_root_dir,
            .observe_local_replica_root = cfg.observe_local_replica_root,
            .reallocation_protocol_peers = cfg.reallocation_protocol_peers,
            .store_status_ticks = 0,
            .local_placement_epoch = null,
            .last_local_placement_refresh_at_ms = 0,
            .local_transition_epoch = null,
            .last_local_transition_refresh_at_ms = 0,
            .local_table_provisioning_fingerprint = null,
            .local_table_provisioning_epoch = null,
            .local_table_provisioning_group_ids_fingerprint = null,
            .last_local_table_provisioning_refresh_at_ms = 0,
            .local_schema_progress_epoch = null,
            .local_schema_progress_group_ids_fingerprint = null,
            .last_local_schema_progress_refresh_at_ms = 0,
            .reconcile_lease = metadata_reconcile_lease.State.init(host_cfg.http.host.local_node_id, cfg.reconcile_lease),
            .lifecycle_signal = LifecycleSignal.init(alloc),
            .backend_runtime = backend_runtime,
            .owned_backend_runtime = owned_backend_runtime,
            .secret_store = cfg.secret_store,
            .internal_service_secret = cfg.internal_service_secret,
            .internal_service_issuer = cfg.internal_service_issuer,
            .destination_authorizer = cfg.destination_authorizer,
            .linearizable_read_tracker = read_tracker,
            .raft = try raft_service.ManagedHttpHostService.init(alloc, host_cfg, http_deps, cfg.raft, deps.raft),
        };
        read_tracker_owned = false;
        owned_backend_runtime = null;
        errdefer service.deinit();
        service.publishTransitionMetricsLocked();
        try foreign_mod.registerDefaultPostgresExecutor(alloc, &service.cdc_backfill_registry);
        return service;
    }

    pub fn deinit(self: *MetadataHttpService) void {
        shutdownRuntimeStatusProtocolProbe(self);
        shutdownCdcRuntimeJobs(self);
        // Projection listeners retain `self`; stop and drain their Raft apply
        // producer before releasing any callback-owned service state.
        self.raft.deinit();
        self.catalog_validation_cache.deinit(self.alloc);
        self.projected_core_snapshot_cache.deinit(self.alloc);
        self.transition_readiness_cache.deinit(self.alloc);
        self.store_status_backfill_marker_cache.deinit(self.alloc);
        self.cdc_backfill_registry.deinit(self.alloc);
        self.lifecycle_signal.deinit();
        self.linearizable_read_tracker.deinit();
        self.alloc.destroy(self.linearizable_read_tracker);
        if (self.replica_root_dir) |replica_root_dir| {
            api_table_writes.closeHostedManagedDbCacheForRoot(replica_root_dir);
        }
        if (self.owned_backend_runtime) |*runtime| runtime.deinit();
        self.owned_backend_runtime = null;
        self.backend_runtime = null;
        self.* = undefined;
    }

    pub fn ensureBackendRuntime(self: *MetadataHttpService) !*backend_runtime_mod.BackendRuntime {
        self.backend_runtime_mutex.lockUncancelable(std.Options.debug_io);
        defer self.backend_runtime_mutex.unlock(std.Options.debug_io);
        if (self.backend_runtime == null) {
            self.owned_backend_runtime = try backend_runtime_mod.BackendRuntimeHandle.init(self.alloc, .{});
            self.backend_runtime = self.owned_backend_runtime.?.ptr();
        }
        if (self.backend_runtime) |runtime| return runtime;
        unreachable;
    }

    pub fn apiIoImpl(self: *MetadataHttpService) ?*backend_runtime_mod.IoImpl {
        const runtime = self.backend_runtime orelse return null;
        return runtime.apiIoImpl();
    }

    pub fn lifecycleSignalCurrent(self: *const MetadataHttpService) u32 {
        return self.lifecycle_signal.current();
    }

    pub fn captureLifecycleSignal(self: *MetadataHttpService, table_name: ?[]const u8) LifecycleSignal.Snapshot {
        return self.lifecycle_signal.snapshot(table_name);
    }

    pub fn waitForLifecycleSignal(self: *MetadataHttpService, observed: LifecycleSignal.Snapshot, timeout_ns: u64) void {
        self.lifecycle_signal.wait(observed, timeout_ns);
    }

    pub fn setLifecycleReconcileHook(self: *MetadataHttpService, hook: ?LifecycleReconcileHook) void {
        self.lifecycle_reconcile_hook = hook;
        self.lifecycle_reconcile_requested.store(true, .release);
    }

    pub fn setCdcWriteSource(self: *MetadataHttpService, source: ?api_table_writes.TableWriteSource) void {
        self.cdc_write_source_override = source;
    }

    pub fn setLocalGroupStatusProvider(self: *MetadataHttpService, provider: ?LocalGroupStatusProvider) void {
        self.local_group_status_provider = provider;
    }

    pub fn setLocalShardDbAdapter(self: *MetadataHttpService, adapter: ?metadata_mod.ShardDbAdapter) void {
        self.local_shard_db_adapter = adapter;
    }

    pub fn setRoutedShardDbAdapter(self: *MetadataHttpService, adapter: ?metadata_mod.ShardDbAdapter) void {
        self.routed_shard_db_adapter = adapter;
    }

    pub fn setLocalReplicaRootReconcileHook(self: *MetadataHttpService, hook: ?LocalReplicaRootReconcileHook) void {
        self.local_replica_root_reconcile_hook = hook;
    }

    pub fn setLocalReplicaRootReconcilePermitHook(self: *MetadataHttpService, hook: ?LocalReplicaRootReconcilePermitHook) void {
        self.local_replica_root_reconcile_permit_hook = hook;
    }

    fn ensureLifecycleListenerRegistered(self: *MetadataHttpService) !void {
        self.lifecycle_listener_mutex.lockUncancelable(std.Options.debug_io);
        defer self.lifecycle_listener_mutex.unlock(std.Options.debug_io);
        if (self.lifecycle_listener_registered) return;
        const store = self.projectedStore() orelse return;
        try store.addLifecycleListeners(
            .{
                .ptr = self,
                .vtable = &.{
                    .on_projection_signal = metadataHttpServiceProjectionSignal,
                },
            },
            .{
                .ptr = self,
                .vtable = &.{
                    .matches_key = metadataHttpServiceLifecycleKeyMatches,
                    .on_committed_key = metadataHttpServiceCommittedKeySignal,
                },
            },
        );
        self.lifecycle_listener_registered = true;
    }

    fn metadataHttpServiceProjectionSignal(ptr: *anyopaque, signal: metadata_storage.raft_apply_store.ProjectionSignal) void {
        const self: *MetadataHttpService = @ptrCast(@alignCast(ptr));
        if (projectionSignalChangesCatalog(signal.kind)) {
            _ = self.catalog_epoch.fetchAdd(1, .release);
        }
        if (projectionSignalChangesProjectedCore(signal.kind)) {
            _ = self.projected_core_epoch.fetchAdd(1, .release);
        }
        if (projectionSignalChangesTransitionReadiness(signal.kind)) {
            _ = self.transition_readiness_epoch.fetchAdd(1, .release);
        }
        switch (signal.kind) {
            .table, .range, .store, .shuffle_join_lease, .restore_job => _ = self.projection_epoch.fetchAdd(1, .monotonic),
            .schema_progress => _ = self.projection_epoch.fetchAdd(1, .monotonic),
            .restore_progress, .replication_source_status => _ = self.projection_epoch.fetchAdd(1, .monotonic),
            .placement_intent => _ = self.placement_epoch.fetchAdd(1, .monotonic),
            .reconcile_lease => _ = self.reconcile_lease_epoch.fetchAdd(1, .monotonic),
            .split_transition, .merge_transition => _ = self.transition_epoch.fetchAdd(1, .monotonic),
        }
        self.proposal_progress_driver.notifyWaiters();
        self.lifecycle_signal.notify(signal.table_name);
    }

    fn metadataHttpServiceLifecycleKeyMatches(ptr: *anyopaque, signal: metadata_storage.raft_apply_store.CommittedKeySignal) bool {
        const self: *MetadataHttpService = @ptrCast(@alignCast(ptr));
        return lifecycleKeyMatchesMetadataNamespace(self.metadata_group_id, signal);
    }

    fn metadataHttpServiceCommittedKeySignal(ptr: *anyopaque, _: metadata_storage.raft_apply_store.CommittedKeySignal) void {
        const self: *MetadataHttpService = @ptrCast(@alignCast(ptr));
        self.lifecycle_reconcile_requested.store(true, .release);
        self.proposal_progress_driver.notifyWaiters();
        self.lifecycle_signal.notify(null);
    }

    pub fn medianKeyLookup(self: *MetadataHttpService) ?metadata_reconciler.MedianKeyLookup {
        if (self.routed_shard_db_adapter == null and self.local_shard_db_adapter == null and self.replica_root_dir == null) return null;
        return .{
            .ptr = self,
            .vtable = &.{
                .fetch_median_key = fetchMedianKey,
            },
        };
    }

    fn fetchMedianKey(ptr: *anyopaque, alloc: std.mem.Allocator, group_id: u64) !?[]u8 {
        const self: *MetadataHttpService = @ptrCast(@alignCast(ptr));
        if (self.routed_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
        if (self.local_shard_db_adapter) |adapter| return try adapter.fetchMedianKey(alloc, group_id);
        const replica_root_dir = self.replica_root_dir orelse return error.UnsupportedOperation;
        var fallback = metadata_mod.FallbackLocalShardDbAdapter{
            .replica_root_dir = replica_root_dir,
            .backend_runtime = try self.ensureBackendRuntime(),
        };
        return try fallback.adapter().fetchMedianKey(alloc, group_id);
    }

    pub fn start(self: *MetadataHttpService) !void {
        try self.raft.start();
    }

    pub fn stop(self: *MetadataHttpService) void {
        self.raft.stop();
    }

    pub fn baseUri(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]u8 {
        return try self.raft.baseUri(alloc);
    }

    pub fn ensureMetadataReplica(self: *MetadataHttpService, record: raft_catalog.ReplicaRecord) !raft_engine.runtime.EnsureReplicaResult {
        if (record.group_id != self.metadata_group_id) return error.InvalidMetadataGroupId;
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.host.http_host.ensureReplica(record);
    }

    pub fn campaignMetadataGroup(self: *MetadataHttpService) !void {
        self.lockRuntime();
        defer self.unlockRuntime();
        try self.raft.host.http_host.campaignGroup(self.metadata_group_id);
    }

    pub fn ensureLocalTableMutationAuthority(self: *MetadataHttpService) !void {
        switch (try self.resolveTableMutationRoute()) {
            .local => {},
            .forward => return error.NotLeader,
        }
    }

    pub fn proposeTransitionCommand(self: *MetadataHttpService, command: metadata_storage.TransitionCommand) !void {
        var owned_legacy_store: ?metadata_table_manager.StoreRecord = null;
        defer if (owned_legacy_store) |record| metadata_table_manager.freeStore(self.alloc, record);
        const safe_command = try runtimeStatusProtocolSafeCommand(self, command, &owned_legacy_store);
        self.lockRuntime();
        defer self.unlockRuntime();
        try metadata_storage.validateTransitionCommandDataGroupIds(safe_command);
        const encoded = try metadata_storage.encodeTransitionCommand(self.alloc, safe_command);
        defer self.alloc.free(encoded);
        try self.raft.host.http_host.propose(self.metadata_group_id, encoded);
        self.lifecycle_signal.notify(null);
    }

    /// Proposes a metadata transition on the local leader and returns the
    /// exact Raft log position that accepted it. Unlike a ReadIndex barrier,
    /// this receipt can be used to prove that this specific mutation applied.
    pub fn proposeTransitionCommandWithReceipt(
        self: *MetadataHttpService,
        command: metadata_storage.TransitionCommand,
    ) !MetadataProposalReceipt {
        var owned_legacy_store: ?metadata_table_manager.StoreRecord = null;
        defer if (owned_legacy_store) |record| metadata_table_manager.freeStore(self.alloc, record);
        const safe_command = try runtimeStatusProtocolSafeCommand(self, command, &owned_legacy_store);
        self.lockRuntime();
        defer self.unlockRuntime();
        try metadata_storage.validateTransitionCommandDataGroupIds(safe_command);
        const raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id) orelse
            return error.NotLeader;
        if (raft_status.soft.role != .leader or raft_status.soft.leader_id == null or raft_status.soft.leader_id.? != raft_status.id) {
            return error.NotLeader;
        }
        const encoded = try metadata_storage.encodeTransitionCommand(self.alloc, safe_command);
        defer self.alloc.free(encoded);
        // Reserve proof storage before Raft accepts the entry. This keeps the
        // accepted-but-untrackable state impossible under allocator pressure.
        try self.raft.host.http_host.host.prepareProposalReceiptTracking(self.metadata_group_id);
        var accepted_index: ?u64 = null;
        var dispatch_error: ?anyerror = null;
        self.raft.host.http_host.proposeWithReceipt(self.metadata_group_id, encoded, &accepted_index) catch |err| {
            dispatch_error = err;
        };
        const index = try acceptedMetadataProposalIndex(accepted_index, dispatch_error);
        try self.raft.host.http_host.host.trackProposalReceipt(
            self.metadata_group_id,
            raft_status.hard.current_term,
            index,
        );
        if (dispatch_error) |err| {
            // Once the local leader assigned an index the proposal was
            // accepted. Replication-message construction can still fail after
            // that point; retain the receipt and let normal Raft progress retry
            // delivery rather than reporting a false rejection to the caller.
            std.log.warn(
                "metadata proposal accepted before dispatch failure group_id={} index={} err={s}",
                .{ self.metadata_group_id, index, @errorName(err) },
            );
        }
        self.lifecycle_signal.notify(null);
        return .{ .term = raft_status.hard.current_term, .index = index };
    }

    pub fn waitForTransitionApplied(
        self: *MetadataHttpService,
        receipt: MetadataProposalReceipt,
    ) !void {
        return self.waitForTransitionAppliedWithContext(receipt, .{});
    }

    pub fn waitForTransitionAppliedWithContext(
        self: *MetadataHttpService,
        receipt: MetadataProposalReceipt,
        request: api_operation.RequestContext,
    ) !void {
        self.lockRuntime();
        const tracked_receipt = self.raft.host.http_host.host.acquireProposalReceipt(
            self.metadata_group_id,
            receipt.term,
            receipt.index,
        );
        self.unlockRuntime();
        defer {
            if (tracked_receipt) {
                self.lockRuntime();
                self.raft.host.http_host.host.releaseProposalReceipt(self.metadata_group_id, receipt.term, receipt.index);
                self.unlockRuntime();
            }
        }
        var progress_driver_lease: ?MetadataProposalProgressDriver.Lease = null;
        defer if (progress_driver_lease) |*lease| lease.deinit();
        const local_deadline_ns = platform_time.monotonicNs() +| linearizable_metadata_read_timeout_ns;
        const deadline_ns = if (request.deadline_ns) |caller_deadline_ns|
            @min(local_deadline_ns, caller_deadline_ns)
        else
            local_deadline_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            try request.ensureActive();
            // Capture before observing Raft so an apply notification between
            // the observation and wait cannot be lost.
            const lifecycle_observation = self.lifecycle_signal.snapshot(null);
            const progress_driver_epoch = self.proposal_progress_driver.currentEpoch();
            self.lockRuntime();
            const maybe_raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id);
            const applied_entry_term = if (maybe_raft_status) |raft_status|
                if (raft_status.applied_index >= receipt.index)
                    self.raft.host.http_host.host.raftTermAtTrackedProposalReceipt(
                        self.metadata_group_id,
                        receipt.term,
                        receipt.index,
                    ) catch null
                else
                    null
            else
                null;
            const observation = observeMetadataProposalApply(maybe_raft_status, applied_entry_term, receipt);
            self.unlockRuntime();
            switch (observation) {
                .applied => return,
                .superseded => return error.NotLeader,
                .pending => {},
            }
            if (progress_driver_lease == null) {
                progress_driver_lease = self.proposal_progress_driver.tryAcquire();
            }
            const now_ns = platform_time.monotonicNs();
            if (now_ns >= deadline_ns) break;
            const remaining_ns = deadline_ns - now_ns;
            if (progress_driver_lease != null) {
                try self.runRaftProgressOnly();
                self.lifecycle_signal.wait(
                    lifecycle_observation,
                    @min(remaining_ns, metadata_proposal_driver_wait_ns),
                );
            } else {
                // Bound the passive sleep so request cancellation remains
                // responsive even if the driver stalls without producing a
                // lifecycle event. Apply and driver handoff wake passive
                // waiters immediately.
                self.proposal_progress_driver.waitForHandoff(
                    progress_driver_epoch,
                    @min(remaining_ns, metadata_proposal_passive_wait_ns),
                );
            }
        }
        try request.ensureActive();
        return error.MetadataProposalApplyTimeout;
    }

    pub fn proposeTransitionCommandAndWaitApplied(
        self: *MetadataHttpService,
        command: metadata_storage.TransitionCommand,
    ) !MetadataProposalReceipt {
        const receipt = try self.proposeTransitionCommandWithReceipt(command);
        try self.waitForTransitionApplied(receipt);
        return receipt;
    }

    pub fn upsertNode(self: *MetadataHttpService, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_node = record });
    }

    pub fn registerNode(self: *MetadataHttpService, record: metadata_table_manager.NodeRecord) !void {
        try self.proposeTransitionCommand(.{ .register_node = record });
    }

    pub fn requestNodeShutdown(self: *MetadataHttpService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .request_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn cancelNodeShutdown(self: *MetadataHttpService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .cancel_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn finalizeNodeShutdown(self: *MetadataHttpService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .finalize_node_shutdown = .{ .node_id = node_id } });
    }

    pub fn removeNode(self: *MetadataHttpService, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_node = .{ .node_id = node_id } });
    }

    pub fn upsertStore(self: *MetadataHttpService, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_store = record });
    }

    fn runtimeStatusRepairProtocolReady(self: *MetadataHttpService) bool {
        if (self.runtimeStatusProtocolActivationVersion() >=
            metadata_runtime_status_protocol.current_record_version) return true;
        const raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id) orelse return false;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        if (std.mem.indexOfScalar(u64, raft_status.conf_state.voters, local_node_id) == null and
            std.mem.indexOfScalar(u64, raft_status.conf_state.voters_outgoing, local_node_id) == null)
        {
            return false;
        }
        const incarnation = (self.metadataIncarnation() catch return false) orelse return false;
        const membership_fingerprint = runtimeStatusProtocolMembershipFingerprint(
            raft_status.conf_state,
            self.reallocation_protocol_peers,
            incarnation,
        );
        if (self.runtimeStatusProtocolFingerprintReady(membership_fingerprint)) return true;
        if (runtimeStatusProtocolMembershipIsLocalOnly(
            raft_status.conf_state,
            self.reallocation_protocol_peers,
            local_node_id,
        )) {
            self.storeRuntimeStatusProtocolFingerprint(membership_fingerprint);
            return true;
        }

        self.scheduleRuntimeStatusProtocolProbe(membership_fingerprint, incarnation);
        return false;
    }

    fn runtimeStatusProtocolActivationVersion(self: *MetadataHttpService) u16 {
        const cached = self.runtime_status_protocol_activated_version.load(.acquire);
        if (cached >= metadata_runtime_status_protocol.repair_status_record_version) return cached;
        const store = self.projectedStore() orelse return 0;
        const version = store.getRuntimeStatusProtocolActivationVersion(self.metadata_group_id) catch return 0;
        if (version >= metadata_runtime_status_protocol.repair_status_record_version) {
            self.runtime_status_protocol_activated_version.store(version, .release);
        }
        return version;
    }

    fn scheduleRuntimeStatusProtocolProbe(
        self: *MetadataHttpService,
        membership_fingerprint: RuntimeStatusMembershipFingerprint,
        incarnation: metadata_mod.MetadataClusterIncarnation,
    ) void {
        if (self.runtime_status_protocol_probe_shutdown.load(.acquire)) return;
        const now_ns = platform_time.monotonicNs();
        if (now_ns < self.runtime_status_protocol_next_probe_ns.load(.acquire)) return;
        if (self.runtime_status_protocol_probe_in_flight.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return;

        // Serialize runtime/owner creation and submission with teardown. Once
        // shutdown observes this owner, closeOwner drains every accepted job
        // before the service can release callback-owned state.
        self.runtime_status_protocol_probe_mutex.lockUncancelable(std.Options.debug_io);
        defer self.runtime_status_protocol_probe_mutex.unlock(std.Options.debug_io);
        if (self.runtime_status_protocol_probe_shutdown.load(.acquire)) {
            self.finishRuntimeStatusProtocolProbe(false);
            return;
        }
        const runtime = self.ensureBackendRuntime() catch {
            self.finishRuntimeStatusProtocolProbe(false);
            return;
        };
        // Manual/freestanding runtimes execute durable jobs on the submitter's
        // stack. Never turn status ingestion into a network wait in those
        // environments; their multi-node capability remains safely disabled.
        if (runtime.durable_jobs.executesInline()) {
            self.finishRuntimeStatusProtocolProbe(false);
            return;
        }

        const JobContext = struct {
            alloc: std.mem.Allocator,
            service: *MetadataHttpService,
            membership_fingerprint: RuntimeStatusMembershipFingerprint,
            incarnation: metadata_mod.MetadataClusterIncarnation,
            completed: bool = false,

            fn run(ptr: *anyopaque) !void {
                const ctx: *@This() = @ptrCast(@alignCast(ptr));
                if (ctx.service.runtime_status_protocol_probe_shutdown.load(.acquire)) return;
                const ready = ctx.service.probeRuntimeStatusRepairProtocol(
                    ctx.membership_fingerprint,
                    ctx.incarnation,
                );
                if (ready) ctx.service.storeRuntimeStatusProtocolFingerprint(ctx.membership_fingerprint);
                ctx.completed = true;
                ctx.service.finishRuntimeStatusProtocolProbe(ready);
                if (ready) ctx.service.lifecycle_signal.notify(null);
            }

            fn deinit(ptr: *anyopaque) void {
                const ctx: *@This() = @ptrCast(@alignCast(ptr));
                // A job rejected or cancelled before run must release the
                // single-flight gate as well.
                if (!ctx.completed) {
                    ctx.service.finishRuntimeStatusProtocolProbe(false);
                }
                const alloc = ctx.alloc;
                alloc.destroy(ctx);
            }
        };

        if (self.runtime_status_protocol_probe_owner_id == 0) {
            self.runtime_status_protocol_probe_owner_id = runtime.allocOwnerId() catch {
                self.finishRuntimeStatusProtocolProbe(false);
                return;
            };
        }
        const ctx = self.alloc.create(JobContext) catch {
            self.finishRuntimeStatusProtocolProbe(false);
            return;
        };
        ctx.* = .{
            .alloc = self.alloc,
            .service = self,
            .membership_fingerprint = membership_fingerprint,
            .incarnation = incarnation,
        };
        runtime.durable_jobs.submit(.{
            .owner_id = self.runtime_status_protocol_probe_owner_id,
            .class = .maintenance,
            .ptr = ctx,
            .run = JobContext.run,
            .deinit = JobContext.deinit,
        }) catch {
            JobContext.deinit(ctx);
        };
    }

    fn finishRuntimeStatusProtocolProbe(self: *MetadataHttpService, ready: bool) void {
        if (!ready and self.runtime_status_protocol_probe_shutdown.load(.acquire)) {
            self.runtime_status_protocol_probe_in_flight.store(false, .release);
            return;
        }
        if (ready) {
            self.runtime_status_protocol_probe_failures.store(0, .release);
            self.runtime_status_protocol_next_probe_ns.store(0, .release);
        } else {
            const failures = self.runtime_status_protocol_probe_failures.fetchAdd(1, .acq_rel) +| 1;
            const backoff_ns = runtimeStatusProtocolProbeBackoffNs(failures);
            self.runtime_status_protocol_next_probe_ns.store(
                platform_time.monotonicNs() +| backoff_ns,
                .release,
            );
            std.log.warn(
                "runtime-status protocol activation probe deferred after failure {d}; retrying in {d}ms",
                .{ failures, @divTrunc(backoff_ns, std.time.ns_per_ms) },
            );
        }
        self.runtime_status_protocol_probe_in_flight.store(false, .release);
    }

    fn runtimeStatusProtocolFingerprintReady(
        self: *MetadataHttpService,
        membership_fingerprint: RuntimeStatusMembershipFingerprint,
    ) bool {
        self.runtime_status_protocol_cache_mutex.lockUncancelable(std.Options.debug_io);
        defer self.runtime_status_protocol_cache_mutex.unlock(std.Options.debug_io);
        return std.mem.eql(
            u8,
            &self.runtime_status_protocol_ready_fingerprint,
            &membership_fingerprint,
        );
    }

    fn storeRuntimeStatusProtocolFingerprint(
        self: *MetadataHttpService,
        membership_fingerprint: RuntimeStatusMembershipFingerprint,
    ) void {
        self.runtime_status_protocol_cache_mutex.lockUncancelable(std.Options.debug_io);
        defer self.runtime_status_protocol_cache_mutex.unlock(std.Options.debug_io);
        self.runtime_status_protocol_ready_fingerprint = membership_fingerprint;
    }

    fn probeRuntimeStatusRepairProtocol(
        self: *MetadataHttpService,
        expected_membership_fingerprint: RuntimeStatusMembershipFingerprint,
        incarnation: metadata_mod.MetadataClusterIncarnation,
    ) bool {
        const raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id) orelse return false;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        if (std.mem.indexOfScalar(u64, raft_status.conf_state.voters, local_node_id) == null and
            std.mem.indexOfScalar(u64, raft_status.conf_state.voters_outgoing, local_node_id) == null)
        {
            return false;
        }
        const required_node_ids = collectReallocationBarrierNodeIds(
            self.alloc,
            raft_status.conf_state,
            self.reallocation_protocol_peers,
        ) catch return false;
        defer if (required_node_ids.len > 0) self.alloc.free(required_node_ids);
        if (required_node_ids.len == 0) return false;

        var client = metadata_http_client.MetadataHttpClient.init(
            self.alloc,
            self.raft.host.http_host.request_executor,
        );
        const probe_budget = metadata_http_client.RequestBudget{
            .deadline_ns = platform_time.monotonicNs() +| reallocation_protocol_probe_timeout_ns,
        };
        for (required_node_ids) |node_id| {
            if (node_id == local_node_id) continue;
            const peer = findReallocationProtocolPeer(self.reallocation_protocol_peers, node_id) orelse {
                std.log.warn("runtime-status protocol activation blocked: metadata member {d} is not configured", .{node_id});
                return false;
            };
            const orchestration_url = peer.orchestration_url orelse {
                std.log.warn("runtime-status protocol activation blocked: metadata member {d} has no orchestration URL", .{node_id});
                return false;
            };
            if (orchestration_url.len == 0) {
                std.log.warn("runtime-status protocol activation blocked: metadata member {d} has an empty orchestration URL", .{node_id});
                return false;
            }
            const peer_status = client.fetchStatusWithBudget(orchestration_url, probe_budget) catch |err| {
                std.log.warn(
                    "runtime-status protocol activation probe failed for metadata member {d}: {s}",
                    .{ node_id, @errorName(err) },
                );
                return false;
            };
            if (!runtimeStatusProtocolCompatible(peer_status, self.metadata_group_id, node_id, incarnation)) {
                std.log.warn(
                    "runtime-status protocol activation blocked: metadata member {d} reports node={d} group={d} version={d}",
                    .{ node_id, peer_status.metadata_raft_local_node_id, peer_status.metadata_group_id, peer_status.runtime_status_record_version },
                );
                return false;
            }
        }
        const final_raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id) orelse return false;
        const final_incarnation = (self.metadataIncarnation() catch return false) orelse return false;
        const final_membership_fingerprint = runtimeStatusProtocolMembershipFingerprint(
            final_raft_status.conf_state,
            self.reallocation_protocol_peers,
            final_incarnation,
        );
        return std.mem.eql(u8, &final_incarnation, &incarnation) and
            std.mem.eql(
                u8,
                &final_membership_fingerprint,
                &expected_membership_fingerprint,
            );
    }

    pub fn registerStore(self: *MetadataHttpService, record: metadata_table_manager.StoreRecord) !void {
        try self.proposeTransitionCommand(.{ .register_store = record });
    }

    pub fn reportStoreStatus(self: *MetadataHttpService, report: metadata_table_manager.StoreStatusReport) !void {
        _ = try self.reportStoreStatuses(&.{report});
    }

    pub fn reportStoreStatuses(self: *MetadataHttpService, reports: []const metadata_table_manager.StoreStatusReport) !usize {
        self.lockRuntime();
        var runtime_locked = true;
        errdefer if (runtime_locked) self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        const projected = try cloneProjectedStoresOwned(self.alloc, snapshot.stores);
        self.unlockRuntime();
        runtime_locked = false;
        defer self.freeProjectedStores(self.alloc, projected);

        return try reportStoreStatusesWithProjected(self, projected, reports);
    }

    pub fn removeStore(self: *MetadataHttpService, store_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_store = .{ .store_id = store_id } });
    }

    pub fn upsertSplitTransition(self: *MetadataHttpService, record: transition_state.SplitTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_split_transition = record });
    }

    pub fn admitSplitTransition(self: *MetadataHttpService, admission: metadata_reconciler.SplitAdmission) !void {
        try self.proposeTransitionCommand(.{ .admit_split_transition = .{
            .expected_source_epoch = admission.expected_source_epoch,
            .record = admission.record,
        } });
    }

    pub fn upsertReplicaIntent(
        self: *MetadataHttpService,
        intent: raft_reconciler.PlacementIntent,
        expected_metadata_version: ?u64,
        expected_version_fence: u64,
        expected_target_drain_requested: bool,
    ) !void {
        try self.proposeTransitionCommand(.{ .upsert_replica_intent = .{
            .expected_metadata_version = expected_metadata_version,
            .expected_version_fence = expected_version_fence,
            .expected_target_drain_requested = expected_target_drain_requested,
            .replacement = intent,
        } });
    }

    pub fn removeReplicaIntent(self: *MetadataHttpService, group_id: u64, local_node_id: u64, expected_metadata_version: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_replica_intent = .{
            .group_id = group_id,
            .local_node_id = local_node_id,
            .expected_metadata_version = expected_metadata_version,
        } });
    }

    pub fn upsertTable(self: *MetadataHttpService, record: metadata_table_manager.TableRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_table = record });
    }

    pub fn replaceTableDefinition(
        self: *MetadataHttpService,
        expected: metadata_table_manager.TableRecord,
        replacement: metadata_table_manager.TableRecord,
    ) !void {
        if (expected.table_id == 0 or
            replacement.table_id != expected.table_id or
            !std.mem.eql(u8, replacement.name, expected.name))
            return error.InvalidTableDefinitionReplacement;
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        const baseline_fence = try store.getTableTransitionFence(
            self.metadata_group_id,
            expected.table_id,
        );
        if (baseline_fence.active()) return error.TableTransitionActive;
        try self.proposeTransitionCommand(.{ .compare_and_replace_table = .{
            .expected = expected,
            .replacement = replacement,
        } });

        const deadline_ns = platform_time.monotonicNs() +| linearizable_metadata_read_timeout_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            const current = (try store.getTable(
                self.alloc,
                self.metadata_group_id,
                expected.table_id,
            )) orelse return error.TableNotFound;
            defer metadata_table_manager.freeTable(self.alloc, current);
            if (metadata_table_manager.tableDefinitionsEqual(current, replacement)) return;
            if (!metadata_table_manager.tableDefinitionsEqual(current, expected))
                return error.TableGenerationChanged;

            const fence = try store.getTableTransitionFence(
                self.metadata_group_id,
                expected.table_id,
            );
            if (fence.active() or fence.generation != baseline_fence.generation)
                return error.TableTransitionActive;

            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (!self.raft.host.http_host.host.isLocalLeader(self.metadata_group_id))
                    return error.NotLeader;
                if (self.raft.pending_updates.items.len > 0) {
                    _ = try self.raft.syncPendingRaftOnly();
                } else {
                    try self.raft.runRaftRoundOnly();
                }
            }
            platform_clock.Clock.real().sleepMs(1);
        }
        return error.MetadataMutationApplyTimeout;
    }

    pub fn removeTable(self: *MetadataHttpService, table_id: u64) !void {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        const baseline_fence = try store.getTableTransitionFence(
            self.metadata_group_id,
            table_id,
        );
        if (baseline_fence.active()) return error.TableTransitionActive;
        if (try store.getTable(self.alloc, self.metadata_group_id, table_id)) |current| {
            metadata_table_manager.freeTable(self.alloc, current);
        } else {
            return;
        }
        try self.proposeTransitionCommand(.{ .remove_table = .{
            .table_id = table_id,
            .expected_transition_generation = baseline_fence.generation,
        } });

        const deadline_ns = platform_time.monotonicNs() +|
            linearizable_metadata_read_timeout_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            if (try store.getTable(
                self.alloc,
                self.metadata_group_id,
                table_id,
            )) |current| {
                metadata_table_manager.freeTable(self.alloc, current);
            } else {
                return;
            }
            const fence = try store.getTableTransitionFence(
                self.metadata_group_id,
                table_id,
            );
            if (fence.active() or
                fence.generation != baseline_fence.generation)
                return error.TableTransitionActive;

            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (!self.raft.host.http_host.host.isLocalLeader(
                    self.metadata_group_id,
                )) return error.NotLeader;
                if (self.raft.pending_updates.items.len > 0) {
                    _ = try self.raft.syncPendingRaftOnly();
                } else {
                    try self.raft.runRaftRoundOnly();
                }
            }
            platform_clock.Clock.real().sleepMs(1);
        }
        return error.MetadataMutationApplyTimeout;
    }

    pub fn upsertSchemaProgress(self: *MetadataHttpService, record: metadata_table_manager.SchemaProgressRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_schema_progress = record });
    }

    pub fn removeSchemaProgress(self: *MetadataHttpService, table_id: u64, node_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_schema_progress = .{
            .table_id = table_id,
            .node_id = node_id,
        } });
    }

    pub fn upsertRestoreProgress(self: *MetadataHttpService, record: metadata_table_manager.RestoreProgressRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_restore_progress = record });
    }

    pub fn removeRestoreProgress(self: *MetadataHttpService, table_id: u64, node_id: u64, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_restore_progress = .{
            .table_id = table_id,
            .node_id = node_id,
            .group_id = group_id,
        } });
    }

    pub fn upsertReplicationSourceStatus(self: *MetadataHttpService, record: metadata_table_manager.ReplicationSourceStatusRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_replication_source_status = record });
    }

    /// Atomically claims exact-cutover authority against both the current
    /// source catalog and prior authority, then waits for the applied proof.
    pub fn claimReplicationSourceCutoverDurable(
        self: *MetadataHttpService,
        expected_replication_sources_json: []const u8,
        expected_authority_id: u64,
        record: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        if (record.cutover_intent_id == 0 or
            record.cutover_authority_id == 0 or
            std.mem.allEqual(u8, &record.cutover_provider_identity, 0))
            return error.InvalidReplicationCutoverIntent;
        try self.proposeTransitionCommand(.{ .claim_replication_source_cutover = .{
            .expected_replication_sources_json = expected_replication_sources_json,
            .expected_authority_id = expected_authority_id,
            .record = record,
        } });

        const deadline_ns = platform_time.monotonicNs() +| linearizable_metadata_read_timeout_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            const store = self.projectedStore() orelse return error.MissingMetadataStore;
            if (try store.getReplicationSourceStatus(
                self.alloc,
                self.metadata_group_id,
                record.table_id,
                record.source_ordinal,
            )) |applied_record| {
                defer metadata_table_manager.freeReplicationSourceStatus(self.alloc, applied_record);
                if (replicationCutoverIntentApplied(applied_record, record)) return;
                if (applied_record.cutover_authority_id != expected_authority_id)
                    return error.ReplicationCutoverAuthorityLost;
            }
            const current_table = (try store.getTable(
                self.alloc,
                self.metadata_group_id,
                record.table_id,
            )) orelse return error.TableNotFound;
            defer metadata_table_manager.freeTable(self.alloc, current_table);
            if (!std.mem.eql(
                u8,
                current_table.replication_sources_json,
                expected_replication_sources_json,
            )) return error.ReplicationSourceConfigChanged;

            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (!self.raft.host.http_host.host.isLocalLeader(self.metadata_group_id))
                    return error.NotLeader;
                if (self.raft.pending_updates.items.len > 0) {
                    _ = try self.raft.syncPendingRaftOnly();
                } else {
                    try self.raft.runRaftRoundOnly();
                }
            }
            platform_clock.Clock.real().sleepMs(1);
        }
        return error.MetadataMutationApplyTimeout;
    }

    pub fn replicationSourceAuthorityCurrent(
        self: *MetadataHttpService,
        expected_replication_sources_json: []const u8,
        expected: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        const table = (try store.getTable(
            self.alloc,
            self.metadata_group_id,
            expected.table_id,
        )) orelse return error.ReplicationSourceConfigChanged;
        defer metadata_table_manager.freeTable(self.alloc, table);
        if (!std.mem.eql(
            u8,
            table.replication_sources_json,
            expected_replication_sources_json,
        )) return error.ReplicationSourceConfigChanged;
        const current = (try store.getReplicationSourceStatus(
            self.alloc,
            self.metadata_group_id,
            expected.table_id,
            expected.source_ordinal,
        )) orelse return error.ReplicationCutoverAuthorityLost;
        defer metadata_table_manager.freeReplicationSourceStatus(self.alloc, current);
        if (!replicationCutoverAuthorityMatches(current, expected))
            return error.ReplicationCutoverAuthorityLost;
    }

    pub fn completeReplicationSourceCutoverRetirementDurable(
        self: *MetadataHttpService,
        expected: metadata_table_manager.ReplicationSourceStatusRecord,
    ) !void {
        if (expected.cutover_authority_id == 0 or
            expected.retired_cutover_authority_id == 0 or
            expected.retired_slot_name.len == 0 or
            expected.retired_publication_name.len == 0)
            return error.InvalidReplicationCutoverIntent;
        try self.proposeTransitionCommand(.{
            .complete_replication_source_retirement = expected,
        });
        const deadline_ns = platform_time.monotonicNs() +|
            linearizable_metadata_read_timeout_ns;
        while (platform_time.monotonicNs() < deadline_ns) {
            const store = self.projectedStore() orelse
                return error.MissingMetadataStore;
            const current = (try store.getReplicationSourceStatus(
                self.alloc,
                self.metadata_group_id,
                expected.table_id,
                expected.source_ordinal,
            )) orelse return error.ReplicationCutoverAuthorityLost;
            defer metadata_table_manager.freeReplicationSourceStatus(
                self.alloc,
                current,
            );
            if (!replicationCutoverAuthorityMatches(current, expected))
                return error.ReplicationCutoverAuthorityLost;
            if (current.retired_cutover_authority_id == 0 and
                current.retired_slot_name.len == 0 and
                current.retired_publication_name.len == 0)
                return;
            if (!replicationCutoverRetirementMatches(current, expected))
                return error.ReplicationCutoverAuthorityLost;

            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (!self.raft.host.http_host.host.isLocalLeader(
                    self.metadata_group_id,
                )) return error.NotLeader;
                if (self.raft.pending_updates.items.len > 0) {
                    _ = try self.raft.syncPendingRaftOnly();
                } else {
                    try self.raft.runRaftRoundOnly();
                }
            }
            platform_clock.Clock.real().sleepMs(1);
        }
        return error.MetadataMutationApplyTimeout;
    }

    pub fn upsertRange(self: *MetadataHttpService, record: metadata_table_manager.RangeRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_range = record });
    }

    pub fn completeRestoreRange(self: *MetadataHttpService, identity: metadata_table_manager.RestoreIntentIdentity) !void {
        try self.proposeTransitionCommand(.{ .complete_restore_range = identity });
    }

    pub fn removeRange(self: *MetadataHttpService, group_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_range = .{ .group_id = group_id } });
    }

    pub fn removeSplitTransition(self: *MetadataHttpService, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_split_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertMergeTransition(self: *MetadataHttpService, record: transition_state.MergeTransitionRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_merge_transition = record });
    }

    pub fn removeMergeTransition(self: *MetadataHttpService, transition_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_merge_transition = .{ .transition_id = transition_id } });
    }

    pub fn upsertReconcileLease(self: *MetadataHttpService, record: metadata_reconcile_lease.ReconcileLeaseRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_reconcile_lease = record });
    }

    pub fn removeReconcileLease(self: *MetadataHttpService) !void {
        try self.proposeTransitionCommand(.{ .remove_reconcile_lease = .{} });
    }

    pub fn upsertShuffleJoinLease(self: *MetadataHttpService, record: metadata_table_manager.ShuffleJoinLeaseRecord) !void {
        try self.proposeTransitionCommand(.{ .upsert_shuffle_join_lease = record });
    }

    pub fn removeShuffleJoinLease(self: *MetadataHttpService, job_id: u64) !void {
        try self.proposeTransitionCommand(.{ .remove_shuffle_join_lease = .{ .job_id = job_id } });
    }

    pub fn requestReallocation(self: *MetadataHttpService, requested_at_ms: u64) !void {
        const barrier = try self.ensureReallocationBarrierProtocolReady();
        const runtime = try self.ensureBackendRuntime();
        const request_id = try metadata_reallocation_request.generateRequestId(runtime.io() orelse std.Options.debug_io);
        try self.proposeTransitionCommand(.{ .upsert_reallocation_request = .{
            .request_id = request_id,
            .requested_at_ms = requested_at_ms,
            .barrier_protocol_version = metadata_reallocation_request.barrier_protocol_version,
            .metadata_incarnation = barrier.metadata_incarnation,
            .protected_metadata_member_count = barrier.protected_metadata_member_count,
            .protected_metadata_membership_fingerprint = barrier.protected_metadata_membership_fingerprint,
        } });
    }

    fn ensureReallocationBarrierProtocolReady(self: *MetadataHttpService) !ReallocationBarrierContract {
        const raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id) orelse
            return error.ReallocationProtocolUpgradeRequired;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        if (std.mem.indexOfScalar(u64, raft_status.conf_state.voters, local_node_id) == null and
            std.mem.indexOfScalar(u64, raft_status.conf_state.voters_outgoing, local_node_id) == null)
        {
            return error.ReallocationProtocolUpgradeRequired;
        }
        const incarnation = (try self.metadataIncarnation()) orelse
            return error.ReallocationProtocolUpgradeRequired;
        var client = metadata_http_client.MetadataHttpClient.init(
            self.alloc,
            self.raft.host.http_host.request_executor,
        );
        const probe_budget = metadata_http_client.RequestBudget{
            .deadline_ns = platform_time.monotonicNs() +| reallocation_protocol_probe_timeout_ns,
        };

        const required_node_ids = try collectReallocationBarrierNodeIds(
            self.alloc,
            raft_status.conf_state,
            self.reallocation_protocol_peers,
        );
        defer if (required_node_ids.len > 0) self.alloc.free(required_node_ids);
        for (required_node_ids) |node_id| {
            try self.requireReallocationBarrierPeer(&client, probe_budget, local_node_id, node_id, incarnation);
        }
        return try reallocationBarrierContract(incarnation, required_node_ids);
    }

    fn requireReallocationBarrierPeer(
        self: *MetadataHttpService,
        client: *metadata_http_client.MetadataHttpClient,
        probe_budget: metadata_http_client.RequestBudget,
        local_node_id: u64,
        node_id: u64,
        incarnation: metadata_mod.MetadataClusterIncarnation,
    ) !void {
        if (node_id == local_node_id) return;
        const peer = findReallocationProtocolPeer(self.reallocation_protocol_peers, node_id) orelse {
            std.log.warn("reallocation barrier blocked: metadata member {d} is not in the configured peer set", .{node_id});
            return error.ReallocationProtocolUpgradeRequired;
        };
        const orchestration_url = peer.orchestration_url orelse {
            std.log.warn("reallocation barrier blocked: metadata member {d} has no orchestration URL", .{node_id});
            return error.ReallocationProtocolUpgradeRequired;
        };
        if (orchestration_url.len == 0) {
            std.log.warn("reallocation barrier blocked: metadata member {d} has an empty orchestration URL", .{node_id});
            return error.ReallocationProtocolUpgradeRequired;
        }
        const peer_status = client.fetchStatusWithBudget(orchestration_url, probe_budget) catch |err| {
            std.log.warn("reallocation barrier blocked: metadata member {d} capability probe failed: {s}", .{ node_id, @errorName(err) });
            return error.ReallocationProtocolUpgradeRequired;
        };
        if (!reallocationBarrierStatusCompatible(peer_status, self.metadata_group_id, node_id, incarnation)) {
            std.log.warn(
                "reallocation barrier blocked: metadata voter {d} reports node={d} group={d} protocol={d}",
                .{ node_id, peer_status.metadata_raft_local_node_id, peer_status.metadata_group_id, peer_status.reallocation_barrier_protocol_version },
            );
            return error.ReallocationProtocolUpgradeRequired;
        }
    }

    pub const TableMutationRoute = union(enum) {
        local,
        forward: ReallocationProtocolPeer,
    };

    /// Routes public table create/drop mutations. Only the current metadata
    /// Raft leader may run the table workflow, so a follower must forward the
    /// mutation to the leader's configured orchestration URL instead of
    /// failing locally. `error.NotLeader` here is provably pre-admission: no
    /// mutation has been sent anywhere yet.
    pub fn resolveTableMutationRoute(self: *MetadataHttpService) !TableMutationRoute {
        self.lockRuntime();
        defer self.unlockRuntime();
        const raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id) orelse
            return error.NotLeader;
        if (raft_status.soft.role == .leader and
            raft_status.soft.leader_id != null and
            raft_status.soft.leader_id.? == raft_status.id)
        {
            return .local;
        }
        const observation = raftObservationFromStatus(
            raft_status,
            self.raft.host.http_host.host.cfg.local_node_id,
        );
        return tableMutationRouteFromObservation(observation, self.reallocation_protocol_peers);
    }

    pub fn tableMutationForwardClient(self: *MetadataHttpService) metadata_http_client.MetadataHttpClient {
        var client = metadata_http_client.MetadataHttpClient.init(
            self.alloc,
            self.raft.host.http_host.request_executor,
        );
        _ = client.withInternalServiceAuth(
            self.internal_service_secret,
            self.internal_service_issuer,
        );
        return client;
    }

    pub fn clearReallocationRequest(self: *MetadataHttpService, expected_request_id: u128) !void {
        try self.proposeTransitionCommand(.{ .remove_reallocation_request = .{
            .expected_request_id = expected_request_id,
        } });
    }

    pub fn upsertExtensionPackage(self: *MetadataHttpService, record: extension_domain.PackageManifest) !void {
        try self.proposeTransitionCommand(.{ .upsert_extension_package = record });
    }

    pub fn syncExtensionPackageStore(self: *MetadataHttpService, io: std.Io, root_path: []const u8) !usize {
        const entries = try extension_domain.scanPackageStoreAlloc(self.alloc, io, root_path);
        defer extension_domain.freePackageStoreEntries(self.alloc, entries);
        for (entries) |entry| try self.upsertExtensionPackage(entry.manifest);
        return entries.len;
    }

    pub fn runRound(self: *MetadataHttpService) !void {
        try self.runRoundInternal(true);
    }

    /// Advances only the latency-sensitive Raft runtime. Production runtimes
    /// drive this from a dedicated ticker so control-plane I/O cannot starve
    /// elections, heartbeats, or Ready processing.
    pub fn runRaftRoundOnly(self: *MetadataHttpService) !void {
        try self.ensureLifecycleListenerRegistered();
        var raft_diagnostics_snapshot: MetadataRaftDiagnosticsSnapshot = .{};
        self.lockRuntime();
        {
            defer self.unlockRuntime();
            if (self.raft.pending_updates.items.len > 0) {
                _ = try self.raft.syncPendingRaftOnly();
            } else {
                try self.raft.runRaftRoundOnly();
            }
            raft_diagnostics_snapshot = self.raftDiagnosticsSnapshotLocked();
        }
        if (raft_diagnostics_snapshot.last_runtime_round) |round| logMetadataRaftRoundDiagnostics(round);
    }

    /// Drains accepted proposals and inbound consensus work without becoming a
    /// second owner of election and heartbeat time.
    pub fn runRaftProgressOnly(self: *MetadataHttpService) !void {
        try self.ensureLifecycleListenerRegistered();
        self.lockRuntime();
        defer self.unlockRuntime();
        if (self.raft.pending_updates.items.len > 0) {
            _ = try self.raft.syncPendingRaftProgressOnly();
        } else {
            try self.raft.runRaftProgressOnly();
        }
    }

    /// Runs metadata projection and reconciliation without advancing Raft.
    /// Callers must concurrently drive `runRaftRoundOnly` at the configured
    /// tick cadence.
    pub fn runControlRoundOnly(self: *MetadataHttpService) !void {
        try self.runRoundInternal(false);
    }

    fn runRoundInternal(self: *MetadataHttpService, advance_raft: bool) !void {
        var run_round_trace = MetadataRunRoundTrace.init();
        defer run_round_trace.logIfSlow();
        var phase_start_ns = platform_time.monotonicNs();
        try self.ensureLifecycleListenerRegistered();
        run_round_trace.recordSince("ensure_lifecycle_listener", phase_start_ns);
        defer {
            const status_cache_phase_start_ns = platform_time.monotonicNs();
            self.refreshMetadataStatusCacheIfDue();
            run_round_trace.recordSince("refresh_metadata_status_cache", status_cache_phase_start_ns);
        }
        defer {
            const lifecycle_signal_phase_start_ns = platform_time.monotonicNs();
            self.lifecycle_signal.notify(null);
            run_round_trace.recordSince("lifecycle_signal_notify", lifecycle_signal_phase_start_ns);
        }
        var raft_diagnostics_snapshot: MetadataRaftDiagnosticsSnapshot = .{};
        if (advance_raft) {
            phase_start_ns = platform_time.monotonicNs();
            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (self.raft.pending_updates.items.len > 0) {
                    _ = try self.raft.syncPendingRaftOnly();
                } else {
                    try self.raft.runRaftRoundOnly();
                }
                raft_diagnostics_snapshot = self.raftDiagnosticsSnapshotLocked();
            }
            run_round_trace.recordSince("raft_round", phase_start_ns);
        }
        if (!try self.ensureMetadataIncarnation()) {
            self.probe_ready.store(false, .release);
            return;
        }
        self.refreshProbeReady();
        if (raft_diagnostics_snapshot.last_runtime_round) |round| logMetadataRaftRoundDiagnostics(round);
        if (!self.observe_local_replica_root) return;

        phase_start_ns = platform_time.monotonicNs();
        var local_transition_inputs = try captureLocalTransitionInputs(self);
        defer {
            const cleanup_phase_start_ns = platform_time.monotonicNs();
            freeLocalTransitionInputs(self, &local_transition_inputs);
            run_round_trace.recordSince("free_transition_inputs", cleanup_phase_start_ns);
        }
        run_round_trace.recordSince("capture_transition_inputs", phase_start_ns);
        phase_start_ns = platform_time.monotonicNs();
        try self.refreshLocalTransitions(&local_transition_inputs);
        run_round_trace.recordSince("refresh_local_transitions", phase_start_ns);

        phase_start_ns = platform_time.monotonicNs();
        const has_reconcile_lease = try self.ensureReconcileLease();
        run_round_trace.recordSince("ensure_reconcile_lease", phase_start_ns);
        if (!has_reconcile_lease) return;

        phase_start_ns = platform_time.monotonicNs();
        var local_projection_inputs = try captureLocalProjectionInputs(self);
        defer {
            const cleanup_phase_start_ns = platform_time.monotonicNs();
            freeLocalProjectionInputs(self, &local_projection_inputs);
            run_round_trace.recordSince("free_projection_inputs", cleanup_phase_start_ns);
        }
        run_round_trace.recordSince("capture_projection_inputs", phase_start_ns);

        phase_start_ns = platform_time.monotonicNs();
        const backfill_markers = try self.refreshStoreStatusBackfillMarkersForRound();
        run_round_trace.recordSince("refresh_store_status_backfill_markers", phase_start_ns);
        if ((self.store_status_ticks >= 40 or backfill_markers.len > 0) and shouldRefreshLocalStoreStatus(self, backfill_markers)) {
            self.store_status_ticks = 0;
            phase_start_ns = platform_time.monotonicNs();
            self.refreshLocalStoreStatusWithBackfillMarkers(backfill_markers, true) catch |err| switch (err) {
                error.UnknownGroup, error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
                else => return err,
            };
            run_round_trace.recordSince("refresh_local_store_status", phase_start_ns);
        }
        phase_start_ns = platform_time.monotonicNs();
        self.refreshLocalSchemaProgress(&local_projection_inputs) catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
        run_round_trace.recordSince("refresh_local_schema_progress", phase_start_ns);
        phase_start_ns = platform_time.monotonicNs();
        var local_placement_inputs = try captureLocalPlacementInputs(self);
        defer {
            const cleanup_phase_start_ns = platform_time.monotonicNs();
            freeLocalPlacementInputs(self, &local_placement_inputs);
            run_round_trace.recordSince("free_placement_inputs", cleanup_phase_start_ns);
        }
        run_round_trace.recordSince("capture_placement_inputs", phase_start_ns);
        phase_start_ns = platform_time.monotonicNs();
        try self.refreshLocalPlacementIntents(&local_placement_inputs);
        run_round_trace.recordSince("refresh_local_placement_intents", phase_start_ns);
        phase_start_ns = platform_time.monotonicNs();
        _ = self.refreshLocalTableProvisioning(&local_projection_inputs) catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => .{},
            else => return err,
        };
        run_round_trace.recordSince("refresh_local_table_provisioning", phase_start_ns);
        phase_start_ns = platform_time.monotonicNs();
        try self.completeRestoreIntentsIfReady(&local_projection_inputs, &local_placement_inputs);
        run_round_trace.recordSince("complete_restore_intents", phase_start_ns);
        phase_start_ns = platform_time.monotonicNs();
        try self.runReplicationBackfillRound();
        run_round_trace.recordSince("run_replication_backfill", phase_start_ns);
        phase_start_ns = platform_time.monotonicNs();
        try self.runLifecycleReconcileHookIfRequested();
        run_round_trace.recordSince("run_lifecycle_reconcile_hook", phase_start_ns);
        phase_start_ns = platform_time.monotonicNs();
        _ = try self.stepTransitions();
        run_round_trace.recordSince("step_post_reconcile_transitions", phase_start_ns);
    }

    pub fn probeReady(self: *const MetadataHttpService) bool {
        return self.probe_ready.load(.acquire);
    }

    fn refreshProbeReady(self: *MetadataHttpService) void {
        const ready = switch (self.raft.host.status(self.metadata_group_id)) {
            .active, .quiesced => true,
            .absent, .starting, .snapshotting, .failed => false,
        };
        self.probe_ready.store(ready, .release);
    }

    pub fn runLifecycleRound(self: *MetadataHttpService) !void {
        try self.ensureLifecycleListenerRegistered();
        defer self.refreshMetadataStatusCacheIfDue();
        defer self.lifecycle_signal.notify(null);
        self.lockRuntime();
        {
            defer self.unlockRuntime();
            if (self.raft.pending_updates.items.len > 0) {
                _ = try self.raft.syncPendingRaftOnly();
            } else {
                try self.raft.runRaftRoundOnly();
            }
        }
        if (!try self.ensureMetadataIncarnation()) {
            self.probe_ready.store(false, .release);
            return;
        }
        self.refreshProbeReady();
        if (!self.observe_local_replica_root) return;

        var local_transition_inputs = try captureLocalTransitionInputs(self);
        defer freeLocalTransitionInputs(self, &local_transition_inputs);
        try self.refreshLocalTransitions(&local_transition_inputs);

        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return;

        var local_projection_inputs = try captureLocalProjectionInputs(self);
        defer freeLocalProjectionInputs(self, &local_projection_inputs);

        const backfill_markers = try self.refreshStoreStatusBackfillMarkersForLifecycleRound();
        if (shouldRefreshLocalStoreStatusForLifecycleRound(self, backfill_markers)) {
            // Keep lifecycle status observational. The provider returns its
            // published/runtime cache immediately and refreshes cold state in
            // the background; it must not be bypassed during long migrations.
            self.refreshLocalStoreStatusWithBackfillMarkers(backfill_markers, true) catch |err| switch (err) {
                error.UnknownGroup, error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
                else => return err,
            };
        }
        self.refreshLocalSchemaProgress(&local_projection_inputs) catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => {},
            else => return err,
        };
        var local_placement_inputs = try captureLocalPlacementInputs(self);
        defer freeLocalPlacementInputs(self, &local_placement_inputs);
        try self.refreshLocalPlacementIntents(&local_placement_inputs);
        _ = self.refreshLocalTableProvisioning(&local_projection_inputs) catch |err| switch (err) {
            error.FileNotFound, error.WriterLocked, error.LmdbUnexpected, error.Corrupted => .{},
            else => return err,
        };
        try self.completeRestoreIntentsIfReady(&local_projection_inputs, &local_placement_inputs);
        try self.runReplicationBackfillRound();
        try self.runLifecycleReconcileHookIfRequested();
        _ = try self.stepTransitions();
    }

    pub fn runCdcRound(self: *MetadataHttpService) !void {
        if (!self.observe_local_replica_root) return;
        _ = try runReplicationBackfillIfLeaseHeld(self);
    }

    pub fn waitForTableLifecycle(self: *MetadataHttpService, table_name: []const u8, expected: TableLifecycleExpectation) !void {
        try self.ensureLifecycleListenerRegistered();
        return try waitForTableLifecycleConvergence(self, table_name, expected);
    }

    pub fn waitForTableProjection(self: *MetadataHttpService, table_name: []const u8, expected: TableProjectionExpectation) !void {
        try self.ensureLifecycleListenerRegistered();
        return try waitForTableProjectionConvergence(self, table_name, expected);
    }

    pub fn reconcileOnceIfLeaseHeld(self: *MetadataHttpService, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return null;
        return try loop.reconcileOnce(self);
    }

    pub fn reconcilePreparedIfLeaseHeld(self: *MetadataHttpService, loop: *metadata_control_loop.MetadataControlLoop) !?metadata_control_loop.ReconcileSummary {
        const has_reconcile_lease = try self.ensureReconcileLease();
        if (!has_reconcile_lease) return null;
        return try loop.reconcilePrepared(self);
    }

    pub fn reconcileOnceEnsuringLease(self: *MetadataHttpService, loop: *metadata_control_loop.MetadataControlLoop) !metadata_control_loop.ReconcileSummary {
        var rounds: usize = 0;
        while (rounds < 32) : (rounds += 1) {
            if (try self.reconcileOnceIfLeaseHeld(loop)) |summary| return summary;
            try self.runRound();
        }
        return error.ReconcileLeaseNotHeld;
    }

    /// HTTP-backed counterpart of MetadataService's prepared-state lease
    /// acquisition. The prepared state remains stable while Raft publishes a
    /// lease renewal.
    pub fn reconcilePreparedEnsuringLease(self: *MetadataHttpService, loop: *metadata_control_loop.MetadataControlLoop) !metadata_control_loop.ReconcileSummary {
        var rounds: usize = 0;
        while (rounds < 32) : (rounds += 1) {
            if (try self.reconcilePreparedIfLeaseHeld(loop)) |summary| return summary;
            try self.runRound();
        }
        return error.ReconcileLeaseNotHeld;
    }

    pub fn applyReconciliationPlan(self: *MetadataHttpService, plan: *const metadata_reconciler.ReconciliationPlan) !void {
        std.debug.assert(plan.placement_upserts.len == plan.placement_upsert_preconditions.len);
        for (plan.placement_upserts, plan.placement_upsert_preconditions) |intent, precondition| {
            try self.upsertReplicaIntent(
                intent,
                precondition.expected_metadata_version,
                precondition.expected_version_fence,
                precondition.expected_target_drain_requested,
            );
        }
        for (plan.table_upserts) |record| try self.upsertTable(record);
        for (plan.split_admissions) |admission| try self.admitSplitTransition(admission);
        for (plan.range_upserts) |record| try self.upsertRange(record);
        for (plan.split_upserts) |record| try self.upsertSplitTransition(record);
        for (plan.merge_upserts) |record| try self.upsertMergeTransition(record);
        for (plan.placement_removals) |record| try self.removeReplicaIntent(record.group_id, record.local_node_id, record.expected_metadata_version);
        for (plan.table_removals) |table_id| try self.removeTable(table_id);
        for (plan.range_removals) |group_id| try self.removeRange(group_id);
        for (plan.split_removals) |transition_id| try self.removeSplitTransition(transition_id);
        for (plan.merge_removals) |transition_id| try self.removeMergeTransition(transition_id);
        if (plan.clear_reallocation_request) |expected| try self.clearReallocationRequest(expected);
    }

    pub fn observeSplitTransition(self: *MetadataHttpService, transition_id: u64) !?transition_state.SplitObservation {
        self.transition_mutex.lockUncancelable(std.Options.debug_io);
        defer self.transition_mutex.unlock(std.Options.debug_io);
        return try self.raft.observeSplitTransition(transition_id);
    }

    pub fn observeMergeTransition(self: *MetadataHttpService, transition_id: u64) !?transition_state.MergeObservation {
        self.transition_mutex.lockUncancelable(std.Options.debug_io);
        defer self.transition_mutex.unlock(std.Options.debug_io);
        return try self.raft.observeMergeTransition(transition_id);
    }

    fn stepTransitions(self: *MetadataHttpService) !raft_transition_service.TransitionStepResult {
        self.transition_mutex.lockUncancelable(std.Options.debug_io);
        defer self.transition_mutex.unlock(std.Options.debug_io);
        defer self.publishTransitionMetricsLocked();
        return try self.raft.stepTransitions();
    }

    pub fn syncPending(self: *MetadataHttpService) !raft_managed_host.ManagedSyncResult {
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.syncPendingRaftOnly();
    }

    pub fn metrics(self: *MetadataHttpService) raft_service.ManagedServiceMetrics {
        // Runtime counters and transition counters have independent writers.
        // Copy each domain under its short publication lock so a slow
        // transition RPC never blocks health or metrics endpoints.
        var snapshot = raft_service.ManagedServiceMetrics{};
        self.lockRuntime();
        snapshot.queued_updates = self.raft.metrics.queued_updates;
        snapshot.applied_updates = self.raft.metrics.applied_updates;
        snapshot.sync_rounds = self.raft.metrics.sync_rounds;
        snapshot.read_lease_requests = self.raft.metrics.read_lease_requests;
        self.unlockRuntime();

        self.transition_metrics_mutex.lockUncancelable(std.Options.debug_io);
        const transition_metrics = self.transition_metrics_snapshot;
        self.transition_metrics_mutex.unlock(std.Options.debug_io);
        applyTransitionMetrics(&snapshot, transition_metrics);
        return snapshot;
    }

    pub fn head(self: *MetadataHttpService) metadata_api.MetadataHead {
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadataIncarnation() catch null,
            .metadata_epoch = projectedProvisioningFingerprint(self.alloc, self) catch self.lifecycle_signal.currentEpoch(),
        };
    }

    pub fn runtimeTopology(self: *MetadataHttpService) !metadata_api.MetadataRuntimeTopology {
        return metadataRuntimeTopology(
            self.metadata_group_id,
            try self.metadataIncarnation(),
            serviceGroupRaftTopologyObservation(self, self.metadata_group_id),
        );
    }

    fn fallbackStatus(self: *MetadataHttpService) MetadataStatus {
        var fallback = MetadataStatus{
            .metadata_group_id = self.metadata_group_id,
            .reallocation_barrier_protocol_version = metadata_reallocation_request.barrier_protocol_version,
            .runtime_status_record_version = metadata_runtime_status_protocol.current_record_version,
            .metadata_epoch = self.lifecycle_signal.currentEpoch(),
            .metrics = self.metrics(),
        };
        self.applyRuntimeStatusProtocolDiagnostics(&fallback);
        return fallback;
    }

    fn applyRuntimeStatusProtocolDiagnostics(
        self: *MetadataHttpService,
        snapshot: *MetadataStatus,
    ) void {
        snapshot.runtime_status_protocol_activated_version = self.runtimeStatusProtocolActivationVersion();
        snapshot.runtime_status_protocol_ready_version =
            if (self.runtimeStatusRepairProtocolReady())
                metadata_runtime_status_protocol.current_record_version
            else
                snapshot.runtime_status_protocol_activated_version;
        if (snapshot.runtime_status_protocol_activated_version >=
            metadata_runtime_status_protocol.current_record_version)
        {
            snapshot.runtime_status_protocol_probe_in_flight = false;
            snapshot.runtime_status_protocol_probe_failures = 0;
            return;
        }
        snapshot.runtime_status_protocol_probe_in_flight =
            self.runtime_status_protocol_probe_in_flight.load(.acquire);
        snapshot.runtime_status_protocol_probe_failures =
            self.runtime_status_protocol_probe_failures.load(.acquire);
    }

    fn loadMetadataStatusCache(self: *MetadataHttpService) ?MetadataStatus {
        self.lockMetadataStatusCache();
        defer self.unlockMetadataStatusCache();
        if (!self.metadata_status_cache_valid) return null;
        if (self.metadata_status_cache_projection_epoch != self.projection_epoch.load(.monotonic)) return null;
        if (self.metadata_status_cache_placement_epoch != self.placement_epoch.load(.monotonic)) return null;
        if (self.metadata_status_cache_transition_epoch != self.transition_epoch.load(.monotonic)) return null;
        return self.metadata_status_cache;
    }

    fn storeMetadataStatusCache(self: *MetadataHttpService, next_status: MetadataStatus, next_refresh_at_ms: u64) void {
        const projection_epoch = self.projection_epoch.load(.monotonic);
        const placement_epoch = self.placement_epoch.load(.monotonic);
        const transition_epoch = self.transition_epoch.load(.monotonic);
        self.lockMetadataStatusCache();
        defer self.unlockMetadataStatusCache();
        self.metadata_status_cache = next_status;
        self.metadata_status_cache_valid = true;
        self.metadata_status_cache_next_refresh_at_ms = next_refresh_at_ms;
        self.metadata_status_cache_projection_epoch = projection_epoch;
        self.metadata_status_cache_placement_epoch = placement_epoch;
        self.metadata_status_cache_transition_epoch = transition_epoch;
    }

    fn lockMetadataStatusCache(self: *MetadataHttpService) void {
        self.metadata_status_cache_mutex.lockUncancelable(std.Options.debug_io);
    }

    fn unlockMetadataStatusCache(self: *MetadataHttpService) void {
        self.metadata_status_cache_mutex.unlock(std.Options.debug_io);
    }

    fn refreshMetadataStatusCacheIfDue(self: *MetadataHttpService) void {
        const now_ms = nowMs();
        self.lockMetadataStatusCache();
        const due = now_ms >= self.metadata_status_cache_next_refresh_at_ms;
        if (due) self.metadata_status_cache_next_refresh_at_ms = now_ms + metadata_status_cache_refresh_interval_ms;
        self.unlockMetadataStatusCache();
        if (!due) return;

        var current_status = snapshotStatus(self.alloc, self.metadata_group_id, self, self.metrics()) catch |err| {
            std.log.warn("metadata status cache refresh failed err={s}", .{@errorName(err)});
            return;
        };
        current_status.metadata_epoch = self.lifecycle_signal.currentEpoch();
        self.applyRuntimeStatusProtocolDiagnostics(&current_status);
        self.storeMetadataStatusCache(current_status, now_ms + metadata_status_cache_refresh_interval_ms);
    }

    pub fn status(self: *MetadataHttpService) !MetadataStatus {
        const now_ms = nowMs();
        var current_status = snapshotStatus(self.alloc, self.metadata_group_id, self, self.metrics()) catch |err| blk: {
            std.log.warn("metadata status refresh failed err={s}", .{@errorName(err)});
            var fallback = self.loadMetadataStatusCache() orelse self.fallbackStatus();
            // Projection/catalog status is intentionally cacheable, but Raft
            // leadership and membership are live safety signals consumed by the
            // operator. Refresh only those cheap in-memory fields so a snapshot
            // failure can never present stale topology as current.
            applyMetadataRaftTopology(
                &fallback,
                serviceGroupRaftTopologyObservation(self, self.metadata_group_id),
            );
            break :blk fallback;
        };
        current_status.metadata_epoch = self.lifecycle_signal.currentEpoch();
        current_status.metrics = self.metrics();
        self.applyRuntimeStatusProtocolDiagnostics(&current_status);
        self.storeMetadataStatusCache(current_status, now_ms + metadata_status_cache_refresh_interval_ms);
        return current_status;
    }

    pub fn recordJsonResponseAllocation(self: *MetadataHttpService, bytes: usize) void {
        recordJsonResponseAllocationCounters(
            &self.json_response_calls,
            &self.json_response_bytes_total,
            &self.json_response_peak_bytes,
            bytes,
        );
    }

    pub fn jsonResponseDiagnostics(self: *MetadataHttpService) JsonResponseDiagnostics {
        return jsonResponseDiagnosticsFromCounters(
            &self.json_response_calls,
            &self.json_response_bytes_total,
            &self.json_response_peak_bytes,
        );
    }

    pub fn metadataStatus(self: *MetadataHttpService) !MetadataStatus {
        var current_status = try snapshotStatusWithOptions(self.alloc, self.metadata_group_id, self, self.metrics(), .{
            .include_reconciliation_planning = true,
        });
        current_status.metadata_epoch = self.lifecycle_signal.currentEpoch();
        current_status.metrics = self.metrics();
        return current_status;
    }

    pub fn ensureLinearizableRead(self: *MetadataHttpService) !void {
        return self.ensureLinearizableReadWithContext(.{});
    }

    pub fn ensureLinearizableReadWithContext(
        self: *MetadataHttpService,
        request: api_operation.RequestContext,
    ) !void {
        try request.ensureActive();
        const request_id = try self.linearizable_read_tracker.registerRequest();
        defer self.linearizable_read_tracker.finishRequest(request_id);
        var request_ctx_buf: [64]u8 = undefined;
        const request_ctx = try std.fmt.bufPrint(
            &request_ctx_buf,
            "{s}{d}",
            .{ linearizable_metadata_read_prefix, request_id },
        );

        const local_deadline_ns = platform_time.monotonicNs() +| linearizable_metadata_read_timeout_ns;
        const deadline_ns = if (request.deadline_ns) |caller_deadline_ns|
            @min(local_deadline_ns, caller_deadline_ns)
        else
            local_deadline_ns;
        var next_request_ns: u64 = 0;
        var request_attempts: usize = 0;
        var not_leader_count: usize = 0;
        var raft_rounds: usize = 0;
        var slowest_round: raft_engine.runtime.multi_raft.HostRound = .{};
        var latest_raft_diagnostics_snapshot: MetadataRaftDiagnosticsSnapshot = .{};
        while (platform_time.monotonicNs() < deadline_ns) {
            try request.ensureActive();
            if (self.linearizable_read_tracker.isComplete(request_id)) return;
            const now_ns = platform_time.monotonicNs();
            if (now_ns >= next_request_ns) {
                self.lockRuntime();
                {
                    defer self.unlockRuntime();
                    request_attempts += 1;
                    self.raft.requestReadableLease(self.metadata_group_id, request_ctx) catch |err| switch (err) {
                        // A follower may not know the leader yet during elections,
                        // restarts, or after endpoint-level load balancing. Keep
                        // driving raft below and retry the same read context until
                        // the barrier completes or the caller's timeout expires.
                        error.NotLeader => {
                            not_leader_count += 1;
                        },
                        else => return err,
                    };
                }
                next_request_ns = now_ns + linearizable_metadata_read_retry_ns;
            }
            var raft_diagnostics_snapshot: MetadataRaftDiagnosticsSnapshot = .{};
            self.lockRuntime();
            {
                defer self.unlockRuntime();
                if (self.raft.pending_updates.items.len > 0) {
                    _ = try self.raft.syncPendingRaftOnly();
                } else {
                    try self.raft.runRaftRoundOnly();
                }
                raft_diagnostics_snapshot = self.raftDiagnosticsSnapshotLocked();
                latest_raft_diagnostics_snapshot = raft_diagnostics_snapshot;
            }
            raft_rounds += 1;
            if (raft_diagnostics_snapshot.last_runtime_round) |round| {
                if (round.elapsed_ns > slowest_round.elapsed_ns) slowest_round = round;
                logMetadataRaftRoundDiagnostics(round);
            }
            if (self.linearizable_read_tracker.isComplete(request_id)) return;
            try request.ensureActive();
            platform_clock.Clock.real().sleepMs(1);
        }
        if (self.linearizable_read_tracker.isComplete(request_id)) return;
        try request.ensureActive();
        self.logLinearizableReadTimeout(request_id, request_attempts, not_leader_count, raft_rounds, slowest_round, latest_raft_diagnostics_snapshot);
        return error.MetadataLinearizableReadTimeout;
    }

    fn logLinearizableReadTimeout(
        self: *MetadataHttpService,
        request_id: u64,
        request_attempts: usize,
        not_leader_count: usize,
        raft_rounds: usize,
        slowest_round: raft_engine.runtime.multi_raft.HostRound,
        snapshot: MetadataRaftDiagnosticsSnapshot,
    ) void {
        const ready = slowest_round.slowest_ready_group;
        std.log.warn(
            "metadata linearizable read timeout request_id={d} group_id={d} attempts={d} not_leader={d} raft_rounds={d} pending_updates={d} node_id={d} role={s} has_leader={} leader_id={d} term={d} commit_index={d} applied_index={d} last_index={d} election_elapsed={d} slowest_round_ms={d} slowest_inbound_ms={d} slowest_tick_ms={d} slowest_drain_ready_ms={d} slowest_drain_scan_ms={d} slowest_apply_flush_ms={d} slowest_transport_flush_ms={d} slowest_transport_advance_ms={d} slowest_ticked_groups={d} slowest_processed_groups={d} slowest_processed_ready_steps={d} slowest_ready_group_id={d} slowest_ready_group_ms={d}",
            .{
                request_id,
                self.metadata_group_id,
                request_attempts,
                not_leader_count,
                raft_rounds,
                snapshot.pending_updates,
                snapshot.node_id,
                snapshot.role,
                snapshot.has_leader,
                snapshot.leader_id,
                snapshot.term,
                snapshot.commit_index,
                snapshot.applied_index,
                snapshot.last_index,
                snapshot.election_elapsed,
                @divTrunc(slowest_round.elapsed_ns, std.time.ns_per_ms),
                @divTrunc(slowest_round.inbound_drain_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(slowest_round.tick_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(slowest_round.drain_ready_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(slowest_round.drain_ready_scan_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(slowest_round.apply_flush_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(slowest_round.transport_flush_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(slowest_round.transport_advance_elapsed_ns, std.time.ns_per_ms),
                slowest_round.ticked_groups,
                slowest_round.processed_groups,
                slowest_round.processed_ready_steps,
                ready.group_id,
                @divTrunc(ready.elapsed_ns, std.time.ns_per_ms),
            },
        );
        std.log.warn(
            "metadata linearizable read timeout ready timings request_id={d} group_id={d} ready_build_ms={d} ready_backpressure_ms={d} ready_capacity_ms={d} ready_snapshot_throttle_ms={d} ready_persist_ms={d} ready_async_ms={d} ready_clone_messages_ms={d} ready_enqueue_apply_ms={d} ready_async_loop_ms={d} ready_outbox_append_ms={d} ready_advance_ms={d} ready_inline_apply_flush_ms={d} ready_inline_outbox_drain_ms={d} ready_inline_transport_flush_ms={d}",
            .{
                request_id,
                ready.group_id,
                @divTrunc(ready.ready_build_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.backpressure_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.capacity_check_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.snapshot_throttle_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.persist_ready_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.async_ready_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.clone_messages_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.enqueue_apply_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.async_message_loop_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.outbox_append_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.raft_advance_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.inline_apply_flush_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.inline_outbox_drain_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(ready.inline_transport_flush_elapsed_ns, std.time.ns_per_ms),
            },
        );
        const persist = ready.persist_ready_detail;
        std.log.warn(
            "metadata linearizable read timeout persist detail request_id={d} group_id={d} skipped_no_durable_state={} used_batch={} used_group_storage={} storage_apply_ms={d} encode_ms={d} wal_append_ms={d} wal_wait_ms={d} wal_coalesce_ms={d} wal_txn_open_ms={d} wal_put_ms={d} wal_commit_ms={d} wal_physical_commits={d} wal_inner_segment_syncs={d} wal_inner_index_syncs={d} wal_post_commit_segment_syncs={d} wal_post_commit_index_syncs={d} encoded_bytes={d} replay_debt_records={d} replay_debt_bytes={d}",
            .{
                request_id,
                ready.group_id,
                persist.skipped_no_durable_state,
                persist.used_batch,
                persist.used_group_storage,
                @divTrunc(persist.storage_apply_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(persist.encode_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(persist.wal_append_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(persist.wal_wait_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(persist.wal_coalesce_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(persist.wal_txn_open_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(persist.wal_put_elapsed_ns, std.time.ns_per_ms),
                @divTrunc(persist.wal_commit_elapsed_ns, std.time.ns_per_ms),
                persist.wal_physical_commits,
                persist.wal_inner_segment_syncs,
                persist.wal_inner_index_syncs,
                persist.wal_post_commit_segment_syncs,
                persist.wal_post_commit_index_syncs,
                persist.encoded_bytes,
                persist.delta_records_since_checkpoint,
                persist.delta_bytes_since_checkpoint,
            },
        );
        std.log.warn(
            "metadata linearizable read timeout ready pressure request_id={d} group_id={d} slowest_ready_messages={d} slowest_ready_committed_entries={d} slowest_ready_unstable_entries={d} slowest_ready_read_states={d} slowest_ready_has_snapshot={} slowest_ready_async_storage={} slowest_ready_processed={} slowest_ready_denied_backpressure={} slowest_ready_denied_transport_capacity={} slowest_ready_denied_apply_capacity={} slowest_ready_denied_snapshot_throttle={} slowest_ready_has_more={}",
            .{
                request_id,
                ready.group_id,
                ready.message_count,
                ready.committed_entries,
                ready.unstable_entries,
                ready.read_states,
                ready.has_snapshot,
                ready.async_storage_writes,
                ready.processed,
                ready.denied_by_backpressure,
                ready.denied_by_transport_capacity,
                ready.denied_by_apply_capacity,
                ready.denied_by_snapshot_throttle,
                ready.has_more_ready,
            },
        );
    }

    pub fn adminSnapshot(self: *MetadataHttpService) !metadata_api.AdminSnapshot {
        return try self.buildAdminSnapshot(true);
    }

    pub fn adminSnapshotFence(self: *MetadataHttpService) !AdminSnapshotFence {
        const raft = serviceGroupRaftObservation(self, self.metadata_group_id);
        return .{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = try self.metadataIncarnation(),
            .metadata_raft_term = raft.term,
            .metadata_raft_commit_index = raft.commit_index,
            .metadata_raft_applied_index = raft.applied_index,
            .projection_epoch = self.projection_epoch.load(.acquire),
            .catalog_epoch = self.catalog_epoch.load(.acquire),
            .placement_epoch = self.placement_epoch.load(.acquire),
            .reconcile_lease_epoch = self.reconcile_lease_epoch.load(.acquire),
            .transition_epoch = self.transition_epoch.load(.acquire),
            .projected_core_epoch = self.projected_core_epoch.load(.acquire),
            .transition_readiness_epoch = self.transition_readiness_epoch.load(.acquire),
        };
    }

    fn buildAdminSnapshot(self: *MetadataHttpService, include_detailed_status: bool) !metadata_api.AdminSnapshot {
        var snapshot: metadata_api.AdminSnapshot = .{
            .status = if (include_detailed_status) try self.metadataStatus() else self.fallbackStatus(),
            .tables = &.{},
            .ranges = &.{},
            .stores = &.{},
            .placement_intents = &.{},
            .shuffle_join_leases = &.{},
            .local_bootstrap_statuses = &.{},
            .restore_progresses = &.{},
            .replication_source_statuses = &.{},
            .replication_source_action_hints = &.{},
            .split_transitions = &.{},
            .merge_transitions = &.{},
            .split_observations = &.{},
            .merge_observations = &.{},
            .merged_group_statuses = &.{},
            .extension_packages = &.{},
            .installed_extensions = &.{},
            .extension_members = &.{},
            .extension_dependencies = &.{},
        };
        errdefer self.freeAdminSnapshot(&snapshot);

        {
            self.lockRuntime();
            defer self.unlockRuntime();
            self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
            defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
            const catalog = try self.catalogValidationSnapshotLocked();
            const core = try self.projectedCoreSnapshotLocked();
            const store = self.projectedStore() orelse return error.MissingMetadataStore;
            snapshot.reallocation_request = try store.getReallocationRequest(self.metadata_group_id);
            snapshot.tables = try cloneProjectedTablesOwned(self.alloc, catalog.tables);
            snapshot.ranges = try cloneProjectedRangesOwned(self.alloc, catalog.ranges);
            snapshot.nodes = try store.listNodes(self.alloc, self.metadata_group_id);
            snapshot.stores = try cloneProjectedStoresOwned(self.alloc, core.stores);
            snapshot.placement_intents = try cloneProjectedPlacementIntentsOwned(self.alloc, core.placement_intents);
            snapshot.shuffle_join_leases = try cloneProjectedShuffleJoinLeasesOwned(self.alloc, core.shuffle_join_leases);
            snapshot.restore_progresses = try cloneProjectedRestoreProgressesOwned(self.alloc, core.restore_progresses);
            snapshot.replication_source_statuses = try cloneProjectedReplicationSourceStatusesOwned(self.alloc, core.replication_source_statuses);
            snapshot.split_transitions = try cloneProjectedSplitTransitionsOwned(self.alloc, core.split_transitions);
            snapshot.merge_transitions = try cloneProjectedMergeTransitionsOwned(self.alloc, core.merge_transitions);
            snapshot.extension_packages = try store.listExtensionPackages(self.alloc, self.metadata_group_id);
            snapshot.installed_extensions = try store.listInstalledExtensions(self.alloc, self.metadata_group_id);
            snapshot.extension_members = try store.listExtensionMembers(self.alloc, self.metadata_group_id);
            snapshot.extension_dependencies = try store.listExtensionDependencies(self.alloc, self.metadata_group_id);
        }

        snapshot.local_bootstrap_statuses = try self.listLocalBootstrapStatuses(self.alloc);
        snapshot.replication_source_action_hints = try metadata_api.deriveReplicationSourceActionHints(
            self.alloc,
            snapshot.tables,
            snapshot.replication_source_statuses,
        );
        snapshot.merged_group_statuses = try metadata_state.mergeHealthyGroupStatuses(
            self.alloc,
            snapshot.tables,
            snapshot.ranges,
            snapshot.placement_intents,
            snapshot.restore_progresses,
            snapshot.stores,
            snapshot.split_transitions,
            snapshot.merge_transitions,
            &.{},
            &.{},
        );
        return snapshot;
    }

    pub fn groupTransitionReadiness(
        self: *MetadataHttpService,
        group_id: u64,
    ) !transition_state.StablePlacementReadiness {
        self.transition_readiness_mutex.lockUncancelable(std.Options.debug_io);
        defer self.transition_readiness_mutex.unlock(std.Options.debug_io);

        const cache = &self.transition_readiness_cache;
        for (0..4) |_| {
            const epoch = self.transition_readiness_epoch.load(.acquire);
            if (cache.initialized and cache.epoch == epoch) {
                return cache.ready_by_group.get(group_id) orelse .status_unavailable;
            }

            var inputs = try self.captureTransitionReadinessInputs();
            defer inputs.deinit(self.alloc);
            var next = try buildTransitionReadinessMap(
                self.alloc,
                inputs.stores,
                inputs.placement_intents,
            );
            if (self.transition_readiness_epoch.load(.acquire) != epoch) {
                next.deinit(self.alloc);
                continue;
            }

            cache.ready_by_group.deinit(self.alloc);
            cache.ready_by_group = next;
            cache.epoch = epoch;
            cache.initialized = true;
            return cache.ready_by_group.get(group_id) orelse .status_unavailable;
        }
        return error.MetadataProjectionAdvanced;
    }

    fn captureTransitionReadinessInputs(self: *MetadataHttpService) !TransitionReadinessInputs {
        self.lockRuntime();
        defer self.unlockRuntime();
        const core = try self.projectedCoreSnapshotLocked();
        const stores = try cloneProjectedStoresOwned(self.alloc, core.stores);
        errdefer {
            for (stores) |store| metadata_table_manager.freeStore(self.alloc, store);
            self.alloc.free(stores);
        }
        return .{
            .stores = stores,
            .placement_intents = try cloneProjectedPlacementIntentsOwned(self.alloc, core.placement_intents),
        };
    }

    pub fn validatePublication(self: *MetadataHttpService, contract: metadata_api.CatalogPublicationContract) !bool {
        try self.ensureLinearizableRead();
        self.lockRuntime();
        defer self.unlockRuntime();
        self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
        defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        const incarnation = try store.getMetadataIncarnation(self.metadata_group_id);
        const snapshot = try self.catalogValidationSnapshotLocked();
        return snapshot.index.matchesPublication(contract, self.metadata_group_id, incarnation, snapshot.tables, snapshot.ranges);
    }

    pub fn validateTablePublication(self: *MetadataHttpService, contract: metadata_api.CatalogTablePublicationContract) !bool {
        try self.ensureLinearizableRead();
        self.lockRuntime();
        defer self.unlockRuntime();
        self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
        defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        const incarnation = try store.getMetadataIncarnation(self.metadata_group_id);
        const snapshot = try self.catalogValidationSnapshotLocked();
        return snapshot.index.matchesTablePublication(contract, self.metadata_group_id, incarnation, snapshot.tables);
    }

    /// Called with both the catalog-validation mutex and the Raft runtime lock
    /// held. Only catalog records are cloned, and non-catalog projection
    /// traffic cannot invalidate this cache.
    fn catalogValidationSnapshotLocked(self: *MetadataHttpService) !*const CatalogValidationSnapshot {
        try self.ensureLifecycleListenerRegistered();
        const current_epoch = self.catalog_epoch.load(.acquire);
        if (self.catalog_validation_cache.snapshot == null or
            self.catalog_validation_cache.catalog_epoch != current_epoch)
        {
            const store = self.projectedStore() orelse return error.MissingMetadataStore;
            var fresh: CatalogValidationSnapshot = .{};
            errdefer fresh.deinit(self.alloc);
            fresh.tables = try store.listTables(self.alloc, self.metadata_group_id);
            fresh.ranges = try store.listRanges(self.alloc, self.metadata_group_id);
            fresh.index = try metadata_api.CatalogProjectionIndex.init(self.alloc, fresh.tables, fresh.ranges);
            if (self.catalog_validation_cache.snapshot) |*snapshot| snapshot.deinit(self.alloc);
            self.catalog_validation_cache = .{
                .catalog_epoch = current_epoch,
                .snapshot = fresh,
            };
        }
        return &(self.catalog_validation_cache.snapshot orelse unreachable);
    }

    pub fn freeAdminSnapshot(self: *MetadataHttpService, snapshot: *metadata_api.AdminSnapshot) void {
        metadata_api.freeSnapshot(self.alloc, self, snapshot);
    }

    pub fn projectedStore(self: *MetadataHttpService) ?*metadata_storage.RaftApplyStore {
        return self.raft.host.owned_metadata_store;
    }

    pub fn metadataIncarnation(self: *MetadataHttpService) !?metadata_mod.MetadataClusterIncarnation {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getMetadataIncarnation(self.metadata_group_id);
    }

    fn ensureMetadataIncarnation(self: *MetadataHttpService) !bool {
        if (try self.metadataIncarnation() != null) {
            self.metadata_incarnation_proposal_pending = false;
            return true;
        }
        self.lockRuntime();
        const is_local_leader = self.raft.host.http_host.host.isLocalLeader(self.metadata_group_id);
        self.unlockRuntime();
        if (!is_local_leader) {
            self.metadata_incarnation_proposal_pending = false;
            return false;
        }
        if (self.metadata_incarnation_proposal_pending) return false;
        if (self.metadata_incarnation_candidate == null) {
            self.metadata_incarnation_candidate = try metadata_mod.incarnation.generate(std.Options.debug_io);
        }
        self.proposeTransitionCommand(.{
            .initialize_metadata_incarnation = self.metadata_incarnation_candidate.?,
        }) catch |err| switch (err) {
            error.NotLeader => return false,
            else => return err,
        };
        self.metadata_incarnation_proposal_pending = true;
        return false;
    }

    /// Returns the current Raft term only while this node is the metadata
    /// leader. A term change is the fencing boundary for control-plane jobs.
    pub fn localMetadataLeadershipTerm(self: *MetadataHttpService) ?u64 {
        self.lockRuntime();
        defer self.unlockRuntime();
        const raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id) orelse return null;
        if (raft_status.soft.role != .leader or raft_status.soft.leader_id == null or raft_status.soft.leader_id.? != raft_status.id) return null;
        return raft_status.hard.current_term;
    }

    pub fn getProjectedReconcileLease(self: *MetadataHttpService) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getReconcileLease(self.metadata_group_id);
    }

    pub fn getProjectedShuffleJoinLease(self: *MetadataHttpService, job_id: u64) !?metadata_table_manager.ShuffleJoinLeaseRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getShuffleJoinLease(self.metadata_group_id, job_id);
    }

    pub fn getProjectedReallocationRequest(self: *MetadataHttpService) !?metadata_mod.ReallocationRequestRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.getReallocationRequest(self.metadata_group_id);
    }

    pub fn reconcileLeaseStats(self: *MetadataHttpService) metadata_reconcile_lease.Stats {
        self.reconcile_lease_mutex.lockUncancelable(std.Options.debug_io);
        defer self.reconcile_lease_mutex.unlock(std.Options.debug_io);
        return self.reconcile_lease.stats();
    }

    fn captureProjectedCoreSnapshotLocked(self: *MetadataHttpService) !ProjectedCoreSnapshot {
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        var snapshot: ProjectedCoreSnapshot = .{};
        errdefer snapshot.deinit(self.alloc);
        snapshot.stores = try store.listStores(self.alloc, self.metadata_group_id);
        snapshot.placement_intents = try store.listPlacementIntents(self.alloc, self.metadata_group_id);
        snapshot.placement_version_fences = try store.listPlacementVersionFences(self.alloc, self.metadata_group_id);
        snapshot.shuffle_join_leases = try store.listShuffleJoinLeases(self.alloc, self.metadata_group_id);
        snapshot.schema_progresses = try store.listSchemaProgress(self.alloc, self.metadata_group_id);
        snapshot.restore_progresses = try store.listRestoreProgress(self.alloc, self.metadata_group_id);
        snapshot.replication_source_statuses = try store.listReplicationSourceStatuses(self.alloc, self.metadata_group_id);
        snapshot.split_transitions = try store.listSplitTransitions(self.alloc, self.metadata_group_id);
        snapshot.merge_transitions = try store.listMergeTransitions(self.alloc, self.metadata_group_id);
        return snapshot;
    }

    fn projectedCoreSnapshotLocked(self: *MetadataHttpService) !*const ProjectedCoreSnapshot {
        try self.ensureLifecycleListenerRegistered();
        const core_epoch = self.projected_core_epoch.load(.acquire);
        const placement_epoch = self.placement_epoch.load(.monotonic);
        const transition_epoch = self.transition_epoch.load(.monotonic);
        if (self.projected_core_snapshot_cache.snapshot == null or
            self.projected_core_snapshot_cache.core_epoch != core_epoch or
            self.projected_core_snapshot_cache.placement_epoch != placement_epoch or
            self.projected_core_snapshot_cache.transition_epoch != transition_epoch)
        {
            var fresh = try self.captureProjectedCoreSnapshotLocked();
            errdefer fresh.deinit(self.alloc);
            if (self.projected_core_snapshot_cache.snapshot) |*snapshot| snapshot.deinit(self.alloc);
            self.projected_core_snapshot_cache = .{
                .core_epoch = core_epoch,
                .placement_epoch = placement_epoch,
                .transition_epoch = transition_epoch,
                .snapshot = fresh,
            };
        }
        return &(self.projected_core_snapshot_cache.snapshot orelse unreachable);
    }

    pub fn memoryDiagnostics(self: *MetadataHttpService) MetadataMemoryDiagnostics {
        var out = MetadataMemoryDiagnostics{
            .process = process_memory_mod.snapshot(),
            .json_response = self.jsonResponseDiagnostics(),
            .hosted_write_cache = if (self.replica_root_dir) |root|
                api_table_writes.hostedManagedDbCacheDiagnosticsForRoot(root)
            else
                .{},
        };
        self.lockRuntime();
        defer self.unlockRuntime();
        self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
        defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
        if (self.projected_core_snapshot_cache.snapshot) |*snapshot| {
            out.projected_core_snapshot = snapshot.diagnostics();
        }
        if (self.catalog_validation_cache.snapshot) |*snapshot| {
            snapshot.addDiagnostics(&out.projected_core_snapshot);
        }
        if (self.projectedStore()) |store| {
            out.projected_store_lsm = lsmRetentionDiagnostics(store.snapshotMaintenanceStats());
        }
        return out;
    }

    pub fn listProjectedTables(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
        defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
        const snapshot = try self.catalogValidationSnapshotLocked();
        return try cloneProjectedTablesOwned(alloc, snapshot.tables);
    }

    pub fn freeProjectedTables(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeTables(alloc, records);
    }

    pub fn listProjectedSchemaProgress(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.SchemaProgressRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedSchemaProgressOwned(alloc, snapshot.schema_progresses);
    }

    pub fn freeProjectedSchemaProgress(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.SchemaProgressRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeSchemaProgress(alloc, records);
    }

    pub fn listProjectedRestoreProgress(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.RestoreProgressRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedRestoreProgressesOwned(alloc, snapshot.restore_progresses);
    }

    pub fn freeProjectedRestoreProgress(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.RestoreProgressRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeRestoreProgress(alloc, records);
    }

    pub fn listProjectedReplicationSourceStatuses(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedReplicationSourceStatusesOwned(alloc, snapshot.replication_source_statuses);
    }

    pub fn freeProjectedReplicationSourceStatuses(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeReplicationSourceStatuses(alloc, records);
    }

    pub fn listProjectedShuffleJoinLeases(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.ShuffleJoinLeaseRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedShuffleJoinLeasesOwned(alloc, snapshot.shuffle_join_leases);
    }

    pub fn freeProjectedShuffleJoinLeases(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.ShuffleJoinLeaseRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeShuffleJoinLeases(alloc, records);
    }

    pub fn listProjectedExtensionPackages(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]extension_domain.PackageManifest {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listExtensionPackages(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedExtensionPackages(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []extension_domain.PackageManifest) void {
        const store = self.projectedStore() orelse return;
        store.freeExtensionPackages(alloc, records);
    }

    pub fn listProjectedInstalledExtensions(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]extension_domain.InstalledExtension {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listInstalledExtensions(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedInstalledExtensions(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []extension_domain.InstalledExtension) void {
        const store = self.projectedStore() orelse return;
        store.freeInstalledExtensions(alloc, records);
    }

    pub fn listProjectedExtensionMembers(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]extension_domain.ExtensionMember {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listExtensionMembers(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedExtensionMembers(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []extension_domain.ExtensionMember) void {
        const store = self.projectedStore() orelse return;
        store.freeExtensionMembers(alloc, records);
    }

    pub fn listProjectedExtensionDependencies(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]extension_domain.ExtensionDependency {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listExtensionDependencies(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedExtensionDependencies(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []extension_domain.ExtensionDependency) void {
        const store = self.projectedStore() orelse return;
        store.freeExtensionDependencies(alloc, records);
    }

    pub fn listLocalBootstrapStatuses(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]raft_host.BootstrapStatus {
        self.lockRuntime();
        defer self.unlockRuntime();
        return try self.raft.host.http_host.host.listBootstrapStatuses(alloc);
    }

    pub fn freeLocalBootstrapStatuses(self: *MetadataHttpService, alloc: std.mem.Allocator, statuses: []raft_host.BootstrapStatus) void {
        self.raft.host.http_host.host.freeBootstrapStatuses(alloc, statuses);
    }

    pub fn listProjectedRanges(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        self.catalog_validation_mutex.lockUncancelable(std.Options.debug_io);
        defer self.catalog_validation_mutex.unlock(std.Options.debug_io);
        const snapshot = try self.catalogValidationSnapshotLocked();
        return try cloneProjectedRangesOwned(alloc, snapshot.ranges);
    }

    pub fn freeProjectedRanges(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeRanges(alloc, records);
    }

    pub fn listProjectedPlacementIntents(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedPlacementIntentsOwned(alloc, snapshot.placement_intents);
    }

    pub fn listProjectedPlacementVersionFences(
        self: *MetadataHttpService,
        alloc: std.mem.Allocator,
    ) ![]metadata_reconciler.PlacementVersionFence {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedPlacementVersionFencesOwned(alloc, snapshot.placement_version_fences);
    }

    pub fn listProjectedNodes(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.NodeRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const store = self.projectedStore() orelse return error.MissingMetadataStore;
        return try store.listNodes(alloc, self.metadata_group_id);
    }

    pub fn freeProjectedNodes(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.NodeRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeNodes(alloc, records);
    }

    pub fn listProjectedStores(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]metadata_table_manager.StoreRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedStoresOwned(alloc, snapshot.stores);
    }

    pub fn freeProjectedStores(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []metadata_table_manager.StoreRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeStores(alloc, records);
    }

    pub fn freeProjectedPlacementIntents(self: *MetadataHttpService, alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
        const store = self.projectedStore() orelse return;
        store.freePlacementIntents(alloc, intents);
    }

    pub fn listProjectedSplitTransitions(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]transition_state.SplitTransitionRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedSplitTransitionsOwned(alloc, snapshot.split_transitions);
    }

    pub fn freeProjectedSplitTransitions(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []transition_state.SplitTransitionRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeSplitTransitions(alloc, records);
    }

    pub fn listProjectedMergeTransitions(self: *MetadataHttpService, alloc: std.mem.Allocator) ![]transition_state.MergeTransitionRecord {
        self.lockRuntime();
        defer self.unlockRuntime();
        const snapshot = try self.projectedCoreSnapshotLocked();
        return try cloneProjectedMergeTransitionsOwned(alloc, snapshot.merge_transitions);
    }

    pub fn freeProjectedMergeTransitions(self: *MetadataHttpService, alloc: std.mem.Allocator, records: []transition_state.MergeTransitionRecord) void {
        const store = self.projectedStore() orelse return;
        store.freeMergeTransitions(alloc, records);
    }

    fn lockRuntime(self: *MetadataHttpService) void {
        self.runtime_mutex.lockUncancelable(std.Options.debug_io);
    }

    fn unlockRuntime(self: *MetadataHttpService) void {
        self.runtime_mutex.unlock(std.Options.debug_io);
    }

    fn raftDiagnosticsSnapshotLocked(self: *MetadataHttpService) MetadataRaftDiagnosticsSnapshot {
        var snapshot = MetadataRaftDiagnosticsSnapshot{
            .pending_updates = self.raft.pending_updates.items.len,
            .last_runtime_round = self.raft.lastRuntimeRound(),
        };
        if (self.raft.raftStatus(self.metadata_group_id)) |raft_status| {
            snapshot.node_id = raft_status.id;
            snapshot.role = @tagName(raft_status.soft.role);
            snapshot.has_leader = raft_status.soft.leader_id != null;
            snapshot.leader_id = if (raft_status.soft.leader_id) |leader| leader else 0;
            snapshot.term = raft_status.hard.current_term;
            snapshot.commit_index = raft_status.hard.commit_index;
            snapshot.applied_index = raft_status.applied_index;
            snapshot.last_index = raft_status.last_index;
            snapshot.election_elapsed = raft_status.election_elapsed;
        }
        return snapshot;
    }

    fn refreshLocalPlacementIntents(self: *MetadataHttpService, round_inputs: ?*const LocalPlacementInputs) !void {
        self.placement_reconcile_mutex.lockUncancelable(std.Options.debug_io);
        defer self.placement_reconcile_mutex.unlock(std.Options.debug_io);

        const current_epoch = self.placement_epoch.load(.monotonic);
        if (!shouldRefreshLocalEpoch(
            self.local_placement_epoch,
            self.last_local_placement_refresh_at_ms,
            current_epoch,
            local_placement_refresh_interval_ms,
        )) return;

        var owned_inputs: ?LocalPlacementInputs = null;
        defer if (owned_inputs) |*inputs| freeLocalPlacementInputs(self, inputs);
        const inputs = blk: {
            if (round_inputs) |snapshot| break :blk snapshot;
            owned_inputs = try captureLocalPlacementInputs(self);
            break :blk &owned_inputs.?;
        };

        var local = std.ArrayListUnmanaged(raft_reconciler.PlacementIntent).empty;
        defer {
            for (local.items) |intent| raft_reconciler.freeIntentOwned(self.alloc, intent);
            local.deinit(self.alloc);
        }

        for (inputs.placement_intents) |intent| {
            if (intent.record.local_node_id != self.raft.host.http_host.host.cfg.local_node_id) continue;
            try local.append(self.alloc, try raft_reconciler.cloneIntentOwned(self.alloc, intent));
        }

        if (!containsLocalIntent(local.items, self.metadata_group_id)) {
            self.lockRuntime();
            const raft_status = self.raft.host.http_host.host.raftStatus(self.metadata_group_id);
            self.unlockRuntime();
            if (raft_status) |value| {
                try local.append(self.alloc, .{
                    .record = .{
                        .group_id = self.metadata_group_id,
                        .replica_id = self.raft.host.http_host.host.cfg.local_node_id,
                        .local_node_id = self.raft.host.http_host.host.cfg.local_node_id,
                        .bootstrap_mode = .persisted,
                    },
                    .peer_node_ids = try allocPeerNodeIdsExcludingSelf(
                        self.alloc,
                        value.conf_state.voters,
                        self.raft.host.http_host.host.cfg.local_node_id,
                    ),
                });
            }
        }

        // Publish the desired state and construct an immutable plan while the
        // runtime is locked. Durable restore/catalog I/O runs outside the lock;
        // only failure accounting and final installation re-enter it.
        var reconcile = reconcile: {
            self.lockRuntime();
            defer self.unlockRuntime();
            try self.raft.host.replacePlacementIntents(local.items);
            var prepared = try self.raft.host.prepareReconcile();
            prepared.beginPreparation();
            break :reconcile prepared;
        };
        defer reconcile.deinit();

        reconcile.prepareDurable() catch |err| {
            self.lockRuntime();
            defer self.unlockRuntime();
            reconcile.notePreparationFailure(err);
            return err;
        };

        self.lockRuntime();
        if (self.placement_epoch.load(.monotonic) != current_epoch) {
            self.unlockRuntime();
            try reconcile.abortDurable();
            self.local_placement_epoch = null;
            return;
        }
        defer self.unlockRuntime();
        _ = try reconcile.commit();
        self.local_placement_epoch = current_epoch;
        self.last_local_placement_refresh_at_ms = nowMs();
    }

    fn refreshLocalTransitions(self: *MetadataHttpService, round_inputs: ?*const LocalTransitionInputs) !void {
        self.transition_mutex.lockUncancelable(std.Options.debug_io);
        defer self.transition_mutex.unlock(std.Options.debug_io);
        defer self.publishTransitionMetricsLocked();
        const transition_svc = if (self.raft.transition_svc) |*svc| svc else return;
        const current_epoch = self.transition_epoch.load(.monotonic);
        if (!shouldRefreshLocalEpoch(
            self.local_transition_epoch,
            self.last_local_transition_refresh_at_ms,
            current_epoch,
            local_transition_refresh_interval_ms,
        )) return;

        var owned_inputs: ?LocalTransitionInputs = null;
        defer if (owned_inputs) |*inputs| freeLocalTransitionInputs(self, inputs);
        const inputs = blk: {
            if (round_inputs) |snapshot| break :blk snapshot;
            owned_inputs = try captureLocalTransitionInputs(self);
            break :blk &owned_inputs.?;
        };
        const split_records = inputs.split_transitions;
        const merge_records = inputs.merge_transitions;

        var split_index: usize = 0;
        while (split_index < transition_svc.pending_split.items.len) {
            const transition_id = transition_svc.pending_split.items[split_index].transition_id;
            if (findProjectedSplit(split_records, transition_id) == null) {
                _ = transition_svc.removeSplit(transition_id);
                continue;
            }
            split_index += 1;
        }
        for (split_records) |record| {
            if (findQueuedSplit(transition_svc.pending_split.items, record.transition_id) == null and
                !transition_svc.hasCompletedSplit(record.transition_id))
            {
                try transition_svc.submitSplit(record);
            }
        }

        var merge_index: usize = 0;
        while (merge_index < transition_svc.pending_merge.items.len) {
            const transition_id = transition_svc.pending_merge.items[merge_index].transition_id;
            if (findProjectedMerge(merge_records, transition_id) == null) {
                _ = transition_svc.removeMerge(transition_id);
                continue;
            }
            merge_index += 1;
        }
        for (merge_records) |record| {
            if (findQueuedMerge(transition_svc.pending_merge.items, record.transition_id) == null and
                !transition_svc.hasCompletedMerge(record.transition_id))
            {
                try transition_svc.submitMerge(record);
            }
        }

        self.raft.metrics.queued_split_transitions = transition_svc.metrics.queued_split_transitions;
        self.raft.metrics.queued_merge_transitions = transition_svc.metrics.queued_merge_transitions;
        self.local_transition_epoch = current_epoch;
        self.last_local_transition_refresh_at_ms = nowMs();
    }

    /// Publishes a fixed-size transition metric snapshot. The caller owns the
    /// transition mutex (or is still in single-threaded initialization).
    fn publishTransitionMetricsLocked(self: *MetadataHttpService) void {
        const snapshot: raft_transition_service.TransitionServiceMetrics = if (self.raft.transition_svc) |*svc| svc.metrics else .{};
        self.transition_metrics_mutex.lockUncancelable(std.Options.debug_io);
        self.transition_metrics_snapshot = snapshot;
        self.transition_metrics_mutex.unlock(std.Options.debug_io);
    }

    fn refreshLocalTableProvisioning(self: *MetadataHttpService, round_inputs: ?*const LocalProjectionInputs) !metadata_table_provisioner.ProvisionSummary {
        const replica_root_dir = self.replica_root_dir orelse return .{};
        const current_epoch = self.projection_epoch.load(.monotonic);
        var owned_inputs: ?LocalProjectionInputs = null;
        defer if (owned_inputs) |*inputs| freeLocalProjectionInputs(self, inputs);
        const inputs = blk: {
            if (round_inputs) |snapshot| break :blk snapshot;
            owned_inputs = try captureLocalProjectionInputs(self);
            break :blk &owned_inputs.?;
        };
        const group_ids = inputs.group_ids;
        const group_ids_fingerprint = inputs.group_ids_fingerprint;
        if (!shouldRefreshLocalProjection(
            self.local_table_provisioning_epoch,
            self.local_table_provisioning_group_ids_fingerprint,
            self.last_local_table_provisioning_refresh_at_ms,
            current_epoch,
            group_ids_fingerprint,
            local_table_provisioning_refresh_interval_ms,
        )) return .{};

        const fingerprint = metadata_table_provisioner.provisioningFingerprint(
            self.metadata_group_id,
            group_ids,
            inputs.tables,
            inputs.ranges,
        );
        self.local_table_provisioning_epoch = current_epoch;
        self.local_table_provisioning_group_ids_fingerprint = group_ids_fingerprint;
        self.last_local_table_provisioning_refresh_at_ms = nowMs();
        if (self.local_table_provisioning_fingerprint == fingerprint) {
            try self.refreshLocalRestoreProgress(group_ids, inputs.tables, inputs.ranges, inputs.restore_progresses);
            return .{};
        }
        if (self.local_replica_root_reconcile_permit_hook) |hook| {
            if (!hook.shouldReconcile()) return .{};
        }
        const summary: metadata_table_provisioner.ProvisionSummary = if (self.local_replica_root_reconcile_hook) |hook| owner: {
            break :owner try hook.run(.{
                .metadata_group_id = self.metadata_group_id,
                .group_ids = group_ids,
                .tables = inputs.tables,
                .ranges = inputs.ranges,
            });
        } else try metadata_table_provisioner.reconcileReplicaRootWithOptions(
            self.alloc,
            replica_root_dir,
            self.metadata_group_id,
            group_ids,
            inputs.tables,
            inputs.ranges,
            .{
                .backend_runtime = try self.ensureBackendRuntime(),
                .destination_authorizer = self.destination_authorizer,
            },
        );
        try self.refreshLocalRestoreProgress(group_ids, inputs.tables, inputs.ranges, inputs.restore_progresses);
        if (summary.indexes_pending != 0) return summary;
        self.local_table_provisioning_fingerprint = fingerprint;
        self.local_schema_progress_epoch = null;
        self.local_schema_progress_group_ids_fingerprint = null;
        self.last_local_schema_progress_refresh_at_ms = 0;
        try self.refreshLocalSchemaProgress(inputs);
        return summary;
    }

    fn refreshLocalRestoreProgress(
        self: *MetadataHttpService,
        group_ids: []const u64,
        tables: []const metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        projected_progress: []const metadata_table_manager.RestoreProgressRecord,
    ) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        const local_progress = try metadata_table_provisioner.collectLocalRestoreProgressUsingIo(
            self.alloc,
            (try self.ensureBackendRuntime()).io(),
            replica_root_dir,
            self.metadata_group_id,
            local_node_id,
            group_ids,
            tables,
            ranges,
        );
        defer {
            for (local_progress) |record| metadata_table_manager.freeRestoreProgress(self.alloc, record);
            self.alloc.free(local_progress);
        }
        try syncLocalRestoreProgress(self, local_node_id, local_progress, projected_progress);
    }

    fn refreshLocalSchemaProgress(self: *MetadataHttpService, round_inputs: ?*const LocalProjectionInputs) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        const current_epoch = self.projection_epoch.load(.monotonic);
        var owned_inputs: ?LocalProjectionInputs = null;
        defer if (owned_inputs) |*inputs| freeLocalProjectionInputs(self, inputs);
        const inputs = blk: {
            if (round_inputs) |snapshot| break :blk snapshot;
            owned_inputs = try captureLocalProjectionInputs(self);
            break :blk &owned_inputs.?;
        };
        const group_ids = inputs.group_ids;
        const group_ids_fingerprint = inputs.group_ids_fingerprint;
        if (!shouldRefreshLocalProjection(
            self.local_schema_progress_epoch,
            self.local_schema_progress_group_ids_fingerprint,
            self.last_local_schema_progress_refresh_at_ms,
            current_epoch,
            group_ids_fingerprint,
            local_schema_progress_refresh_interval_ms,
        )) return;

        var local_progress = try metadata_table_provisioner.collectLocalSchemaProgressFromRuntime(
            self.alloc,
            local_node_id,
            group_ids,
            inputs.tables,
            inputs.ranges,
            inputs.stores,
        );
        defer self.alloc.free(local_progress);
        if (local_progress.len == 0 and !metadata_table_provisioner.localSchemaRuntimeCoverageComplete(
            local_node_id,
            group_ids,
            inputs.tables,
            inputs.ranges,
            inputs.stores,
        )) {
            self.alloc.free(local_progress);
            const backend_runtime = try self.ensureBackendRuntime();
            var fallback_shard_db = metadata_mod.FallbackLocalShardDbAdapter{
                .replica_root_dir = replica_root_dir,
                .backend_runtime = backend_runtime,
            };
            const shard_db = self.local_shard_db_adapter orelse fallback_shard_db.adapter();
            local_progress = metadata_table_provisioner.collectLocalSchemaProgressWithOptions(
                self.alloc,
                replica_root_dir,
                self.metadata_group_id,
                local_node_id,
                group_ids,
                inputs.tables,
                inputs.ranges,
                .{
                    .backend_runtime = backend_runtime,
                    .shard_db_adapter = shard_db,
                },
            ) catch |err| switch (err) {
                error.WriterLocked => try self.alloc.alloc(metadata_table_manager.SchemaProgressRecord, 0),
                else => return err,
            };
        }
        try syncLocalSchemaProgress(self, local_node_id, local_progress, inputs.schema_progresses);
        self.local_schema_progress_epoch = current_epoch;
        self.local_schema_progress_group_ids_fingerprint = group_ids_fingerprint;
        self.last_local_schema_progress_refresh_at_ms = nowMs();
    }

    fn refreshLocalStoreStatus(self: *MetadataHttpService) !void {
        try self.refreshLocalStoreStatusWithBackfillMarkers(null, true);
    }

    fn refreshLocalStoreStatusWithBackfillMarkers(
        self: *MetadataHttpService,
        backfill_markers: ?[]const StoreStatusBackfillMarker,
        use_provider: bool,
    ) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        const local_node_id = self.raft.host.http_host.host.cfg.local_node_id;
        try syncLocalStoreStatus(self, local_node_id, replica_root_dir, backfill_markers, use_provider);
    }

    fn refreshStoreStatusBackfillMarkersForRound(self: *MetadataHttpService) ![]const StoreStatusBackfillMarker {
        self.store_status_ticks += 1;
        self.store_status_backfill_probe_ticks += 1;
        const replica_root_dir = self.replica_root_dir orelse return &.{};
        try maybeRefreshStoreStatusBackfillMarkerCache(
            self.alloc,
            replica_root_dir,
            self.store_status_ticks,
            &self.store_status_backfill_probe_ticks,
            &self.store_status_backfill_marker_cache,
        );
        return self.store_status_backfill_marker_cache.markers;
    }

    fn refreshStoreStatusBackfillMarkersForLifecycleRound(self: *MetadataHttpService) ![]const StoreStatusBackfillMarker {
        const replica_root_dir = self.replica_root_dir orelse return &.{};
        if (self.store_status_backfill_marker_cache.markers.len == 0 and self.store_status_backfill_marker_cache.scanned_at_ms == 0) {
            try refreshStoreStatusBackfillMarkerCacheNow(
                self.alloc,
                replica_root_dir,
                &self.store_status_backfill_probe_ticks,
                &self.store_status_backfill_marker_cache,
            );
            return self.store_status_backfill_marker_cache.markers;
        }

        self.store_status_backfill_probe_ticks += 1;
        try maybeRefreshStoreStatusBackfillMarkerCache(
            self.alloc,
            replica_root_dir,
            0,
            &self.store_status_backfill_probe_ticks,
            &self.store_status_backfill_marker_cache,
        );
        return self.store_status_backfill_marker_cache.markers;
    }

    fn completeRestoreIntentsIfReady(
        self: *MetadataHttpService,
        round_projection_inputs: ?*const LocalProjectionInputs,
        round_placement_inputs: ?*const LocalPlacementInputs,
    ) !void {
        try completeRestoreIntentsForService(
            self,
            if (round_projection_inputs) |inputs| inputs.tables else null,
            if (round_projection_inputs) |inputs| inputs.ranges else null,
            if (round_placement_inputs) |inputs| inputs.placement_intents else null,
            if (round_projection_inputs) |inputs| inputs.restore_progresses else null,
        );
    }

    fn ensureReconcileLease(self: *MetadataHttpService) !bool {
        self.reconcile_lease_mutex.lockUncancelable(std.Options.debug_io);
        defer self.reconcile_lease_mutex.unlock(std.Options.debug_io);
        return try self.ensureReconcileLeaseLocked();
    }

    fn ensureReconcileLeaseLocked(self: *MetadataHttpService) !bool {
        const now_ms = self.reconcile_lease.nowMs();
        const is_local_leader = self.raft.host.http_host.host.isLocalLeader(self.metadata_group_id);
        const projected = self.getCachedProjectedReconcileLease(now_ms, is_local_leader) catch |err| switch (err) {
            error.MissingMetadataStore => null,
            else => return err,
        };
        const has_lease = self.reconcile_lease.observe(is_local_leader, projected, now_ms);
        if (self.reconcile_lease.shouldRenew(is_local_leader, projected, now_ms)) {
            self.upsertReconcileLease(self.reconcile_lease.desiredRecord(now_ms)) catch |err| {
                self.reconcile_lease.noteAcquireFailure();
                return err;
            };
        }
        return has_lease;
    }

    fn getCachedProjectedReconcileLease(
        self: *MetadataHttpService,
        now_ms: u64,
        is_local_leader: bool,
    ) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        const current_epoch = self.reconcile_lease_epoch.load(.monotonic);
        if (self.reconcile_lease_projection_cache.epoch == current_epoch and
            now_ms < self.reconcile_lease_projection_cache.next_refresh_at_ms)
        {
            return self.reconcile_lease_projection_cache.record;
        }
        const projected = try self.getProjectedReconcileLease();
        self.reconcile_lease_projection_cache = .{
            .epoch = current_epoch,
            .record = projected,
            .next_refresh_at_ms = reconcileLeaseCacheNextRefreshAtMs(&self.reconcile_lease, is_local_leader, projected, now_ms),
        };
        return projected;
    }

    fn statusProjectedReconcileLease(self: *MetadataHttpService, now_ms: u64) !?metadata_reconcile_lease.ReconcileLeaseRecord {
        return try self.getCachedProjectedReconcileLease(now_ms, self.raft.host.http_host.host.isLocalLeader(self.metadata_group_id));
    }

    fn runLifecycleReconcileHookIfRequested(self: *MetadataHttpService) !void {
        const hook = self.lifecycle_reconcile_hook orelse return;
        if (!self.lifecycle_reconcile_requested.swap(false, .acq_rel)) return;
        try hook.run();
    }

    fn runReplicationBackfillRound(self: *MetadataHttpService) !void {
        try scheduleReplicationBackfillJob(self);
    }

    fn runReplicationBackfillWorker(self: *MetadataHttpService) !void {
        const replica_root_dir = self.replica_root_dir orelse return;
        try ensureCdcWorkPermit(self);

        const runtime = try self.ensureBackendRuntime();
        const catalog = api_table_catalog.CatalogSource.fromMetadataHttpService(self);
        var cdc_group_router = api_table_router.CatalogBackedGroupRouter.init(
            catalog,
            // CDC is metadata-owned but data-applied; force the routed API path even
            // when metadata and data live in the same standalone process.
            0,
        );
        var hosted_write_source = api_table_writes.HostedProvisionedTableWriteSource.init(
            replica_root_dir,
            catalog,
            cdc_group_router.router(),
            self.raft.host.http_host.request_executor,
        );
        _ = hosted_write_source.withBackendRuntime(runtime);
        _ = hosted_write_source.withSecretStore(self.secret_store);
        _ = hosted_write_source.withInternalServiceAuth(
            self.internal_service_secret,
            self.internal_service_issuer,
        );
        _ = hosted_write_source.withDestinationAuthorization(self.destination_authorizer);
        const write_source = self.cdc_write_source_override orelse hosted_write_source.source();
        var coordinator = metadata_replication_backfill.SnapshotBackfillCoordinator{
            .alloc = self.alloc,
            .runner = .{
                .alloc = self.alloc,
                .io = runtime.io() orelse std.Options.debug_io,
                .registry = &self.cdc_backfill_registry,
                .write_source = write_source,
                .secret_store = self.secret_store,
                .work_permit = cdcWorkPermit(self),
                .destination_authorizer = self.destination_authorizer,
            },
        };
        const summary = coordinator.runRound(self) catch |err| {
            if (!isExpectedCdcRoundError(err)) return err;
            if (comptime builtin.is_test) {
                std.debug.print("metadata http cdc snapshot round skipped: {s}\n", .{@errorName(err)});
            }
            std.log.warn("metadata http cdc snapshot round skipped: {s}", .{@errorName(err)});
            return;
        };
        if (summary.sources_considered > 0) {
            std.log.info(
                "metadata http cdc snapshot round tables={d} sources={d} started={d} resumed={d} completed={d} yielded={d}",
                .{
                    summary.tables_considered,
                    summary.sources_considered,
                    summary.sources_started,
                    summary.sources_resumed,
                    summary.sources_completed,
                    summary.sources_yielded,
                },
            );
        }
        var streaming = metadata_replication_backfill.StreamingReplicationCoordinator{
            .alloc = self.alloc,
            .runner = .{
                .alloc = self.alloc,
                .registry = &self.cdc_backfill_registry,
                .write_source = write_source,
                .secret_store = self.secret_store,
                .work_permit = cdcWorkPermit(self),
                .destination_authorizer = self.destination_authorizer,
            },
        };
        const stream_summary = streaming.runRound(self) catch |err| {
            if (!isExpectedCdcRoundError(err)) return err;
            std.log.warn("metadata http cdc streaming round skipped: {s}", .{@errorName(err)});
            return;
        };
        if (stream_summary.sources_considered > 0) {
            std.log.info(
                "metadata http cdc streaming round tables={d} sources={d} started={d} resumed={d} skipped={d} polled={d} changes={d}",
                .{
                    stream_summary.tables_considered,
                    stream_summary.sources_considered,
                    stream_summary.sources_started,
                    stream_summary.sources_resumed,
                    stream_summary.sources_skipped_pending_snapshot,
                    stream_summary.sources_polled,
                    stream_summary.changes_applied,
                },
            );
        }
    }
};

fn cdcLocalMetadataLeader(service: anytype) bool {
    const Service = @TypeOf(service.*);
    if (Service == MetadataService) return service.isLocalMetadataLeader();
    service.lockRuntime();
    defer service.unlockRuntime();
    return service.raft.host.http_host.host.isLocalLeader(service.metadata_group_id);
}

test "metadata service reallocation barrier requires a current protocol from the exact metadata voter" {
    const incarnation: metadata_mod.MetadataClusterIncarnation = "0123456789abcdef0123456789abcdef".*;
    const legacy_status = MetadataStatus{
        .metadata_group_id = 42,
        .metadata_incarnation = incarnation,
        .metadata_raft_local_node_id = 7,
        .metrics = .{},
    };
    try std.testing.expect(!reallocationBarrierStatusCompatible(legacy_status, 42, 7, incarnation));

    var current_status = legacy_status;
    current_status.reallocation_barrier_protocol_version = metadata_reallocation_request.barrier_protocol_version;
    try std.testing.expect(reallocationBarrierStatusCompatible(current_status, 42, 7, incarnation));
    try std.testing.expect(!reallocationBarrierStatusCompatible(current_status, 43, 7, incarnation));
    try std.testing.expect(!reallocationBarrierStatusCompatible(current_status, 42, 8, incarnation));
    try std.testing.expect(!reallocationBarrierStatusCompatible(
        current_status,
        42,
        7,
        "fedcba9876543210fedcba9876543210".*,
    ));
    current_status.metadata_incarnation = null;
    try std.testing.expect(!reallocationBarrierStatusCompatible(current_status, 42, 7, incarnation));
}

test "metadata runtime status protocol requires the exact current metadata replica" {
    const incarnation: metadata_mod.MetadataClusterIncarnation = "0123456789abcdef0123456789abcdef".*;
    var status = MetadataStatus{
        .metadata_group_id = 42,
        .metadata_incarnation = incarnation,
        .metadata_raft_local_node_id = 7,
        .metrics = .{},
    };
    try std.testing.expect(!runtimeStatusProtocolCompatible(status, 42, 7, incarnation));

    status.runtime_status_record_version = metadata_runtime_status_protocol.current_record_version;
    try std.testing.expect(runtimeStatusProtocolCompatible(status, 42, 7, incarnation));
    try std.testing.expect(!runtimeStatusProtocolCompatible(status, 43, 7, incarnation));
    try std.testing.expect(!runtimeStatusProtocolCompatible(status, 42, 8, incarnation));
    status.metadata_incarnation = null;
    try std.testing.expect(!runtimeStatusProtocolCompatible(status, 42, 7, incarnation));
}

test "metadata runtime status capability cache is scoped to incarnation and membership" {
    const incarnation: metadata_mod.MetadataClusterIncarnation = "0123456789abcdef0123456789abcdef".*;
    const conf_state = raft_engine.core.ConfState{
        .voters = @constCast((&[_]u64{ 1, 2 })[0..]),
        .learners = @constCast((&[_]u64{3})[0..]),
    };
    const peers = [_]ReallocationProtocolPeer{
        .{ .node_id = 1 },
        .{ .node_id = 2 },
        .{ .node_id = 3 },
    };
    const fingerprint = runtimeStatusProtocolMembershipFingerprint(conf_state, &peers, incarnation);
    try std.testing.expect(!std.mem.eql(
        u8,
        &fingerprint,
        &([_]u8{0} ** std.crypto.hash.sha2.Sha256.digest_length),
    ));

    var changed_conf_state = conf_state;
    changed_conf_state.learners = @constCast((&[_]u64{ 3, 4 })[0..]);
    const changed_membership_fingerprint = runtimeStatusProtocolMembershipFingerprint(
        changed_conf_state,
        &peers,
        incarnation,
    );
    try std.testing.expect(!std.mem.eql(u8, &fingerprint, &changed_membership_fingerprint));
    const changed_incarnation_fingerprint = runtimeStatusProtocolMembershipFingerprint(
        conf_state,
        &peers,
        "fedcba9876543210fedcba9876543210".*,
    );
    try std.testing.expect(!std.mem.eql(u8, &fingerprint, &changed_incarnation_fingerprint));
}

test "metadata runtime status capability probing is local-only or exponentially backed off" {
    const local_conf = raft_engine.core.ConfState{
        .voters = @constCast((&[_]u64{1})[0..]),
    };
    try std.testing.expect(runtimeStatusProtocolMembershipIsLocalOnly(
        local_conf,
        &.{.{ .node_id = 1 }},
        1,
    ));
    try std.testing.expect(!runtimeStatusProtocolMembershipIsLocalOnly(
        local_conf,
        &.{ .{ .node_id = 1 }, .{ .node_id = 2 } },
        1,
    ));
    var remote_conf = local_conf;
    remote_conf.learners = @constCast((&[_]u64{2})[0..]);
    try std.testing.expect(!runtimeStatusProtocolMembershipIsLocalOnly(remote_conf, &.{}, 1));

    try std.testing.expectEqual(@as(u64, 0), runtimeStatusProtocolProbeBackoffNs(0));
    try std.testing.expectEqual(5 * std.time.ns_per_s, runtimeStatusProtocolProbeBackoffNs(1));
    try std.testing.expectEqual(10 * std.time.ns_per_s, runtimeStatusProtocolProbeBackoffNs(2));
    try std.testing.expectEqual(40 * std.time.ns_per_s, runtimeStatusProtocolProbeBackoffNs(4));
    try std.testing.expectEqual(60 * std.time.ns_per_s, runtimeStatusProtocolProbeBackoffNs(5));
    try std.testing.expectEqual(60 * std.time.ns_per_s, runtimeStatusProtocolProbeBackoffNs(100));
}

test "metadata runtime repair status downgrade clears state and serviceability proof together" {
    var indexes = [_]metadata_table_manager.RuntimeIndexStatusReport{.{
        .name = "visual_idx",
        .kind = "dense_vector",
        .repair_status = .rebuilding,
        .repair_active_generation_serviceable = true,
    }};
    var runtime_statuses = [_]metadata_table_manager.RuntimeGroupStatusReport{.{
        .indexes = indexes[0..],
    }};
    var store = metadata_table_manager.StoreRecord{
        .store_id = 1,
        .node_id = 2,
        .runtime_statuses = runtime_statuses[0..],
    };
    try std.testing.expect(storeHasRuntimeRepairStatus(store));
    stripRuntimeRepairStatus(&store);
    try std.testing.expect(!storeHasRuntimeRepairStatus(store));
    try std.testing.expect(!indexes[0].repair_active_generation_serviceable);
}

test "metadata service transition commands downgrade runtime repair status until protocol activation" {
    const FakeService = struct {
        alloc: std.mem.Allocator,
        ready: bool,

        fn runtimeStatusRepairProtocolReady(self: *@This()) bool {
            return self.ready;
        }
    };
    var indexes = [_]metadata_table_manager.RuntimeIndexStatusReport{.{
        .name = "visual_idx",
        .kind = "dense_vector",
        .repair_status = .rebuilding,
        .repair_active_generation_serviceable = true,
    }};
    var runtime_statuses = [_]metadata_table_manager.RuntimeGroupStatusReport{.{
        .indexes = indexes[0..],
    }};
    const store = metadata_table_manager.StoreRecord{
        .store_id = 1,
        .node_id = 2,
        .reporter_incarnation = 77,
        .runtime_statuses = runtime_statuses[0..],
    };

    var legacy_service = FakeService{ .alloc = std.testing.allocator, .ready = false };
    var owned_legacy_store: ?metadata_table_manager.StoreRecord = null;
    defer if (owned_legacy_store) |record| metadata_table_manager.freeStore(std.testing.allocator, record);
    const safe_command = try runtimeStatusProtocolSafeCommand(
        &legacy_service,
        .{ .register_store = store },
        &owned_legacy_store,
    );
    try std.testing.expect(!storeHasRuntimeRepairStatus(safe_command.register_store));
    try std.testing.expectEqual(@as(u64, 0), safe_command.register_store.reporter_incarnation);
    try std.testing.expect(storeHasRuntimeRepairStatus(store));

    var refused_downgrade: ?metadata_table_manager.StoreRecord = null;
    try std.testing.expectError(
        error.RuntimeStatusProtocolUnavailable,
        runtimeStatusProtocolSafeCommand(
            &legacy_service,
            .{ .upsert_store = store },
            &refused_downgrade,
        ),
    );
    try std.testing.expect(refused_downgrade == null);

    var current_service = FakeService{ .alloc = std.testing.allocator, .ready = true };
    var unexpectedly_owned: ?metadata_table_manager.StoreRecord = null;
    const current_command = try runtimeStatusProtocolSafeCommand(
        &current_service,
        .{ .upsert_store = store },
        &unexpectedly_owned,
    );
    try std.testing.expect(unexpectedly_owned == null);
    try std.testing.expect(storeHasRuntimeRepairStatus(current_command.upsert_store));
    try std.testing.expectEqual(@as(u64, 77), current_command.upsert_store.reporter_incarnation);
}

test "metadata service defers reporter fence transitions while activation is unknown" {
    const FakeService = struct {
        alloc: std.mem.Allocator,
        upserts: usize = 0,
        last_reporter_incarnation: u64 = 0,

        fn runtimeStatusRepairProtocolReady(_: *@This()) bool {
            return false;
        }

        fn upsertStore(self: *@This(), record: metadata_table_manager.StoreRecord) !void {
            self.upserts += 1;
            self.last_reporter_incarnation = record.reporter_incarnation;
        }
    };

    var projected = [_]metadata_table_manager.StoreRecord{
        try metadata_table_manager.cloneStore(std.testing.allocator, .{
            .store_id = 7,
            .node_id = 9,
            .reporter_incarnation = 0x1234,
            .status_generation = 5,
            .capacity_bytes = 100,
        }),
        try metadata_table_manager.cloneStore(std.testing.allocator, .{
            .store_id = 8,
            .node_id = 10,
            .capacity_bytes = 100,
        }),
    };
    defer for (&projected) |*store| metadata_table_manager.freeStore(std.testing.allocator, store.*);
    const reports = [_]metadata_table_manager.StoreStatusReport{
        .{
            .store_id = 7,
            .reporter_incarnation = 0x1234,
            .status_generation = 6,
            .capacity_bytes = 200,
        },
        .{
            .store_id = 8,
            .reporter_incarnation = 0x5678,
            .status_generation = 1,
            .capacity_bytes = 200,
        },
    };
    var service = FakeService{ .alloc = std.testing.allocator };

    try std.testing.expectEqual(
        @as(usize, 2),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    // The committed fenced store must wait. The legacy store can still publish
    // ordinary telemetry because reporter authority is established only by
    // registration, so applying its report does not copy the incoming fence.
    try std.testing.expectEqual(@as(usize, 1), service.upserts);
    try std.testing.expectEqual(@as(u64, 0), service.last_reporter_incarnation);
    try std.testing.expectEqual(@as(u64, 0x1234), projected[0].reporter_incarnation);
    try std.testing.expectEqual(@as(u64, 0), projected[1].reporter_incarnation);
}

test "metadata service status reporting never proposes deletion of committed repair facts while activation is unknown" {
    const FakeService = struct {
        alloc: std.mem.Allocator,
        upserts: usize = 0,
        proposed_repair_status: bool = false,

        fn runtimeStatusRepairProtocolReady(_: *@This()) bool {
            return false;
        }

        fn upsertStore(self: *@This(), record: metadata_table_manager.StoreRecord) !void {
            self.upserts += 1;
            self.proposed_repair_status = storeHasRuntimeRepairStatus(record);
        }
    };

    var existing_indexes = [_]metadata_table_manager.RuntimeIndexStatusReport{.{
        .name = "visual_idx",
        .kind = "dense_vector",
        .doc_count = 10,
        .coverage_generation = 7,
        .coverage_config_hash = 8,
        .coverage_identity_ready = true,
        .repair_status = .rebuilding,
        .repair_active_generation_serviceable = true,
    }};
    var existing_runtime = [_]metadata_table_manager.RuntimeGroupStatusReport{.{
        .table_id = 1,
        .table_name = "products",
        .group_id = 2,
        .store_id = 3,
        .node_id = 4,
        .updated_at_ns = 200 * std.time.ns_per_ms,
        .status_generation = 10,
        .indexes = existing_indexes[0..],
    }};
    var projected = [_]metadata_table_manager.StoreRecord{try metadata_table_manager.cloneStore(std.testing.allocator, .{
        .store_id = 3,
        .node_id = 4,
        .runtime_statuses = existing_runtime[0..],
    })};
    defer metadata_table_manager.freeStore(std.testing.allocator, projected[0]);

    var observed_indexes = existing_indexes;
    observed_indexes[0].doc_count = 11;
    var observed_runtime = existing_runtime;
    observed_runtime[0].indexes = observed_indexes[0..];
    var reports = [_]metadata_table_manager.StoreStatusReport{.{
        .store_id = 3,
        .runtime_statuses = observed_runtime[0..],
    }};
    var service = FakeService{ .alloc = std.testing.allocator };
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 0), service.upserts);
    try std.testing.expect(storeHasRuntimeRepairStatus(projected[0]));

    // Absence is also a repair-state transition. A legacy/null heartbeat must
    // not clear a committed repair fact while activation is unavailable.
    observed_indexes[0].doc_count = 12;
    observed_indexes[0].repair_status = null;
    observed_indexes[0].repair_active_generation_serviceable = false;
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 0), service.upserts);
    try std.testing.expect(storeHasRuntimeRepairStatus(projected[0]));
    try std.testing.expectEqual(
        metadata_table_manager.IndexRepairStatus.rebuilding,
        projected[0].runtime_statuses[0].indexes[0].repair_status.?,
    );

    // Omitting the index is not an authoritative deletion while activation is
    // unknown. Preserve the whole committed snapshot rather than allowing the
    // repair admission fact to disappear with an incomplete heartbeat.
    observed_runtime[0].indexes = &.{};
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 0), service.upserts);
    try std.testing.expect(storeHasRuntimeRepairStatus(projected[0]));
    try std.testing.expectEqual(@as(usize, 1), projected[0].runtime_statuses[0].indexes.len);

    // A transient heartbeat can also omit the whole runtime group. That absence
    // carries no more deletion authority than an omitted member index.
    reports[0].runtime_statuses = &.{};
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 0), service.upserts);
    try std.testing.expect(storeHasRuntimeRepairStatus(projected[0]));
    try std.testing.expectEqual(@as(usize, 1), projected[0].runtime_statuses.len);

    // Missing or changed kind is not an authoritative catalog replacement
    // without a complete identity. In particular, legacy JSON defaults kind to
    // empty and must not erase a committed admission-safety fact.
    reports[0].runtime_statuses = observed_runtime[0..];
    observed_runtime[0].indexes = observed_indexes[0..];
    observed_indexes[0].doc_count = 13;
    observed_indexes[0].kind = "";
    observed_indexes[0].coverage_identity_ready = false;
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 0), service.upserts);
    try std.testing.expect(storeHasRuntimeRepairStatus(projected[0]));
    try std.testing.expectEqualStrings("dense_vector", projected[0].runtime_statuses[0].indexes[0].kind);

    observed_indexes[0].kind = "sparse_vector";
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 0), service.upserts);
    try std.testing.expect(storeHasRuntimeRepairStatus(projected[0]));
    try std.testing.expectEqualStrings("dense_vector", projected[0].runtime_statuses[0].indexes[0].kind);

    // A same-name observation with an unknown identity is not proof of a new
    // materialization. Startup/degraded defaults must not erase repair state.
    observed_indexes[0].kind = "dense_vector";
    observed_indexes[0].doc_count = 14;
    observed_indexes[0].coverage_generation = 9;
    observed_indexes[0].coverage_config_hash = 0;
    observed_indexes[0].coverage_identity_ready = false;
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 0), service.upserts);
    try std.testing.expect(storeHasRuntimeRepairStatus(projected[0]));
    try std.testing.expectEqual(@as(u64, 7), projected[0].runtime_statuses[0].indexes[0].coverage_generation);

    // A complete but older cached identity still cannot delete newer repair
    // state, even if its process-local generation is numerically larger.
    observed_indexes[0].doc_count = 15;
    observed_indexes[0].coverage_identity_ready = true;
    observed_runtime[0].updated_at_ns = 100 * std.time.ns_per_ms;
    observed_runtime[0].status_generation = 99;
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 0), service.upserts);
    try std.testing.expect(storeHasRuntimeRepairStatus(projected[0]));
    try std.testing.expectEqual(@as(u64, 7), projected[0].runtime_statuses[0].indexes[0].coverage_generation);

    // A complete, newer identity is authoritative. A lower generation is valid
    // because the producer may have restarted between the observations.
    observed_runtime[0].updated_at_ns = 300 * std.time.ns_per_ms;
    observed_runtime[0].status_generation = 1;
    try std.testing.expectEqual(
        @as(usize, 1),
        try reportStoreStatusesWithProjected(&service, &projected, &reports),
    );
    try std.testing.expectEqual(@as(usize, 1), service.upserts);
    try std.testing.expect(!service.proposed_repair_status);
}

test "metadata service reallocation barrier covers configured and transitional members" {
    const configured_peers = [_]ReallocationProtocolPeer{
        .{ .node_id = 2, .orchestration_url = "http://127.0.0.1:12378" },
        .{ .node_id = 6 },
    };
    const conf_state = raft_engine.core.ConfState{
        .voters = @constCast((&[_]u64{ 1, 2 })[0..]),
        .voters_outgoing = @constCast((&[_]u64{ 2, 3 })[0..]),
        .learners = @constCast((&[_]u64{4})[0..]),
        .learners_next = @constCast((&[_]u64{5})[0..]),
    };
    const required = try collectReallocationBarrierNodeIds(
        std.testing.allocator,
        conf_state,
        &configured_peers,
    );
    defer std.testing.allocator.free(required);
    try std.testing.expectEqualSlices(u64, &.{ 1, 2, 3, 4, 5, 6 }, required);
}

fn shutdownRuntimeStatusProtocolProbe(service: *MetadataHttpService) void {
    service.runtime_status_protocol_probe_shutdown.store(true, .release);
    service.runtime_status_protocol_probe_mutex.lockUncancelable(std.Options.debug_io);
    const runtime = service.backend_runtime;
    const owner_id = service.runtime_status_protocol_probe_owner_id;
    service.runtime_status_protocol_probe_mutex.unlock(std.Options.debug_io);
    if (runtime) |value| {
        if (owner_id != 0) value.durable_jobs.closeOwner(owner_id);
    }
}

fn shutdownCdcRuntimeJobs(service: anytype) void {
    service.cdc_shutdown.store(true, .release);
    service.cdc_runtime_mutex.lockUncancelable(std.Options.debug_io);
    const runtime = service.backend_runtime;
    const owner_id = service.cdc_job_owner_id;
    service.cdc_runtime_mutex.unlock(std.Options.debug_io);
    if (runtime) |value| {
        if (owner_id != 0) value.durable_jobs.closeOwner(owner_id);
    }
}

fn advanceCdcLeaseRaft(service: anytype) !void {
    const Service = @TypeOf(service.*);
    service.lockRuntime();
    defer service.unlockRuntime();
    if (Service == MetadataService) {
        if (!service.raft.host.host.isLocalLeader(service.metadata_group_id))
            return error.CdcWorkLeaseLost;
        try service.raft.runRaftRoundOnly();
    } else {
        if (!service.raft.host.http_host.host.isLocalLeader(service.metadata_group_id))
            return error.CdcWorkLeaseLost;
        if (service.raft.pending_updates.items.len > 0) {
            _ = try service.raft.syncPendingRaftOnly();
        } else {
            try service.raft.runRaftRoundOnly();
        }
    }
}

fn ensureCdcWorkPermit(service: anytype) !void {
    if (service.cdc_shutdown.load(.acquire)) return error.CdcWorkShuttingDown;
    const now_ns = platform_time.monotonicNs();
    if (now_ns < service.cdc_permit_check_after_ns.load(.acquire)) return;

    service.reconcile_lease_mutex.lockUncancelable(std.Options.debug_io);
    defer service.reconcile_lease_mutex.unlock(std.Options.debug_io);

    const locked_now_ns = platform_time.monotonicNs();
    if (locked_now_ns < service.cdc_permit_check_after_ns.load(.acquire)) return;
    if (!cdcLocalMetadataLeader(service)) return error.CdcWorkLeaseLost;
    const permit_cache_ns = @min(
        @as(u64, 250 * std.time.ns_per_ms),
        @max(
            @as(u64, std.time.ns_per_ms),
            (service.reconcile_lease.config.lease_ttl_ms *|
                std.time.ns_per_ms) / 10,
        ),
    );
    if (!service.reconcile_lease.config.enabled) {
        service.cdc_permit_check_after_ns.store(
            locked_now_ns +| permit_cache_ns,
            .release,
        );
        return;
    }

    const renew_timeout_ns = @min(
        linearizable_metadata_read_timeout_ns,
        service.reconcile_lease.config.lease_ttl_ms *| std.time.ns_per_ms,
    );
    const deadline_ns = platform_time.monotonicNs() +| renew_timeout_ns;
    var proposed_expires_at_ms: u64 = 0;
    while (platform_time.monotonicNs() < deadline_ns) {
        if (service.cdc_shutdown.load(.acquire)) return error.CdcWorkShuttingDown;
        if (!cdcLocalMetadataLeader(service)) return error.CdcWorkLeaseLost;

        const now_ms = service.reconcile_lease.nowMs();
        const projected = try service.getProjectedReconcileLease();
        _ = service.reconcile_lease.observe(true, projected, now_ms);
        const minimum_remaining_ms = @max(
            @as(u64, 1),
            service.reconcile_lease.config.lease_ttl_ms / 2,
        );
        if (projected) |current| {
            if (current.owner_node_id == service.reconcile_lease.local_node_id and
                current.expires_at_ms > now_ms +| minimum_remaining_ms)
            {
                service.cdc_permit_check_after_ns.store(
                    platform_time.monotonicNs() +| permit_cache_ns,
                    .release,
                );
                return;
            }
            if (current.owner_node_id != service.reconcile_lease.local_node_id and
                current.expires_at_ms > now_ms)
                return error.CdcWorkLeaseLost;
        }

        const desired = service.reconcile_lease.desiredRecord(now_ms);
        if (desired.expires_at_ms > proposed_expires_at_ms) {
            service.upsertReconcileLease(desired) catch |err| {
                service.reconcile_lease.noteAcquireFailure();
                return err;
            };
            proposed_expires_at_ms = desired.expires_at_ms;
        }
        try advanceCdcLeaseRaft(service);
        platform_clock.Clock.real().sleepMs(1);
    }
    return error.CdcWorkLeaseRenewalTimeout;
}

fn cdcWorkDeadlineNs(service: anytype) !u64 {
    try ensureCdcWorkPermit(service);
    if (!service.reconcile_lease.config.enabled)
        return platform_time.monotonicNs() +| 5 * std.time.ns_per_s;

    service.reconcile_lease_mutex.lockUncancelable(std.Options.debug_io);
    defer service.reconcile_lease_mutex.unlock(std.Options.debug_io);
    const now_ms = service.reconcile_lease.nowMs();
    if (service.reconcile_lease.owner_node_id !=
        service.reconcile_lease.local_node_id or
        service.reconcile_lease.expires_at_ms <= now_ms)
        return error.CdcWorkLeaseLost;
    const remaining_ms = service.reconcile_lease.expires_at_ms - now_ms;
    const quantum_ns = try cdcProviderQuantumNs(
        service.reconcile_lease.config.lease_ttl_ms,
        remaining_ms,
    );
    return platform_time.monotonicNs() +| quantum_ns;
}

fn cdcProviderQuantumNs(ttl_ms: u64, remaining_ms: u64) !u64 {
    if (ttl_ms == 0 or remaining_ms <= 1) return error.CdcWorkLeaseLost;
    const safety_ms = @max(
        @as(u64, 1),
        @min(ttl_ms / 10, remaining_ms / 2),
    );
    if (remaining_ms <= safety_ms) return error.CdcWorkLeaseLost;
    const lease_budget_ns =
        (remaining_ms - safety_ms) *| std.time.ns_per_ms;
    const configured_quantum_ns = @max(
        @as(u64, std.time.ns_per_ms),
        (ttl_ms *| std.time.ns_per_ms) / 3,
    );
    return @min(
        @as(u64, 5 * std.time.ns_per_s),
        @min(configured_quantum_ns, lease_budget_ns),
    );
}

test "metadata cdc provider quantum is bounded by observed lease remainder" {
    try std.testing.expectError(
        error.CdcWorkLeaseLost,
        cdcProviderQuantumNs(100, 1),
    );
    try std.testing.expectEqual(
        @as(u64, 100 * std.time.ns_per_ms / 3),
        try cdcProviderQuantumNs(100, 60),
    );
    try std.testing.expectEqual(
        @as(u64, 50 * std.time.ns_per_ms),
        try cdcProviderQuantumNs(5_000, 100),
    );
    try std.testing.expectEqual(
        @as(u64, 5_000 * std.time.ns_per_ms / 3),
        try cdcProviderQuantumNs(5_000, 3_000),
    );
}

fn cdcWorkPermit(service: anytype) metadata_replication_backfill.WorkPermit {
    const Service = @TypeOf(service.*);
    const Callbacks = struct {
        fn checkpoint(ptr: *anyopaque) !void {
            const typed: *Service = @ptrCast(@alignCast(ptr));
            try ensureCdcWorkPermit(typed);
        }

        fn deadline(ptr: *anyopaque) !u64 {
            const typed: *Service = @ptrCast(@alignCast(ptr));
            return try cdcWorkDeadlineNs(typed);
        }
    };
    return .{
        .ptr = service,
        .checkpoint_fn = Callbacks.checkpoint,
        .deadline_fn = Callbacks.deadline,
    };
}

fn scheduleReplicationBackfillJob(service: anytype) !void {
    const Service = @TypeOf(service.*);
    if (service.replica_root_dir == null or !cdcLocalMetadataLeader(service)) return;

    service.cdc_runtime_mutex.lockUncancelable(std.Options.debug_io);
    defer service.cdc_runtime_mutex.unlock(std.Options.debug_io);
    if (service.cdc_shutdown.load(.acquire)) return;
    if (service.cdc_job_in_flight.load(.acquire)) return;

    const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
    if (now_ms < service.cdc_next_round_at_ms) return;

    const runtime = try service.ensureBackendRuntime();
    if (service.cdc_job_owner_id == 0)
        service.cdc_job_owner_id = try runtime.allocOwnerId();

    const JobContext = struct {
        alloc: std.mem.Allocator,
        service: *Service,

        fn run(ptr: *anyopaque) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            ctx.service.runReplicationBackfillWorker() catch |err| {
                if (isExpectedCdcRoundError(err)) {
                    std.log.warn("metadata CDC maintenance job deferred: {s}", .{@errorName(err)});
                } else {
                    std.log.err("metadata CDC maintenance job failed: {s}", .{@errorName(err)});
                }
            };
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            ctx.service.cdc_job_in_flight.store(false, .release);
            const alloc = ctx.alloc;
            alloc.destroy(ctx);
        }
    };

    const ctx = try service.alloc.create(JobContext);
    ctx.* = .{ .alloc = service.alloc, .service = service };
    service.cdc_job_in_flight.store(true, .release);
    runtime.durable_jobs.submit(.{
        .owner_id = service.cdc_job_owner_id,
        .class = .maintenance,
        .ptr = ctx,
        .run = JobContext.run,
        .deinit = JobContext.deinit,
    }) catch |err| {
        JobContext.deinit(ctx);
        return err;
    };
    service.cdc_next_round_at_ms = now_ms + cdc_replication_round_interval_ms;
}

fn syncLocalSchemaProgress(
    service: anytype,
    local_node_id: u64,
    local_progress: []const metadata_table_manager.SchemaProgressRecord,
    projected_progress: []const metadata_table_manager.SchemaProgressRecord,
) !void {
    for (local_progress) |record| {
        const existing = findSchemaProgress(projected_progress, record.table_id, record.node_id);
        if (existing != null and existing.?.schema_version == record.schema_version) continue;
        try service.upsertSchemaProgress(record);
    }

    for (projected_progress) |record| {
        if (record.node_id != local_node_id) continue;
        if (findSchemaProgress(local_progress, record.table_id, record.node_id) != null) continue;
        try service.removeSchemaProgress(record.table_id, record.node_id);
    }
}

fn runReplicationBackfillIfLeaseHeld(service: anytype) !bool {
    const has_reconcile_lease = try service.ensureReconcileLease();
    if (!has_reconcile_lease) return false;
    try service.runReplicationBackfillRound();
    return true;
}

fn syncLocalRestoreProgress(
    service: anytype,
    local_node_id: u64,
    local_progress: []const metadata_table_manager.RestoreProgressRecord,
    projected_progress: []const metadata_table_manager.RestoreProgressRecord,
) !void {
    for (local_progress) |record| {
        const existing = findRestoreProgress(projected_progress, record.table_id, record.node_id, record.group_id);
        if (existing != null and restoreProgressEquivalent(existing.?, record)) continue;
        try service.upsertRestoreProgress(record);
    }

    for (projected_progress) |record| {
        if (record.node_id != local_node_id) continue;
        const local = findRestoreProgress(local_progress, record.table_id, record.node_id, record.group_id);
        if (local != null and restoreProgressEquivalent(local.?, record)) continue;
        try service.removeRestoreProgress(record.table_id, record.node_id, record.group_id);
    }
}

fn restoreProgressEquivalent(a: metadata_table_manager.RestoreProgressRecord, b: metadata_table_manager.RestoreProgressRecord) bool {
    return std.mem.eql(u8, a.backup_id, b.backup_id) and
        std.mem.eql(u8, a.artifact_backup_id, b.artifact_backup_id) and
        std.mem.eql(u8, a.location, b.location) and
        std.mem.eql(u8, a.snapshot_path, b.snapshot_path) and
        std.mem.eql(u8, a.artifact_sha256, b.artifact_sha256) and
        a.primary_restored == b.primary_restored and
        a.runtime_repair_complete == b.runtime_repair_complete and
        std.mem.eql(u8, a.phase, b.phase) and
        std.mem.eql(u8, a.last_error, b.last_error);
}

fn cloneSplitRuntimeObservationsForMerge(
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

fn cloneMergeRuntimeObservationsForMerge(
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

fn completeRestoreIntentsForService(
    service: anytype,
    provided_tables: ?[]const metadata_table_manager.TableRecord,
    provided_ranges: ?[]const metadata_table_manager.RangeRecord,
    provided_placements: ?[]const raft_reconciler.PlacementIntent,
    provided_progress: ?[]const metadata_table_manager.RestoreProgressRecord,
) !void {
    const tables = if (provided_tables) |tables| tables else try service.listProjectedTables(service.alloc);
    defer if (provided_tables == null) service.freeProjectedTables(service.alloc, @constCast(tables));
    const ranges = if (provided_ranges) |ranges| ranges else try service.listProjectedRanges(service.alloc);
    defer if (provided_ranges == null) service.freeProjectedRanges(service.alloc, @constCast(ranges));
    const placements = if (provided_placements) |placements| placements else try service.listProjectedPlacementIntents(service.alloc);
    defer if (provided_placements == null) service.freeProjectedPlacementIntents(service.alloc, @constCast(placements));
    const progress = if (provided_progress) |progress| progress else try service.listProjectedRestoreProgress(service.alloc);
    defer if (provided_progress == null) service.freeProjectedRestoreProgress(service.alloc, @constCast(progress));

    for (ranges) |range| {
        const table = findProjectedTableById(tables, range.table_id) orelse continue;
        if (range.restore_backup_id.len == 0 or range.restore_location.len == 0) continue;
        if (!rangeRestoreIntentComplete(
            table.table_id,
            range,
            range.restore_backup_id,
            range.restore_location,
            placements,
            progress,
        )) continue;

        try service.completeRestoreRange(metadata_table_manager.restoreIntentIdentity(range));
    }
}

fn rangeRestoreIntentComplete(
    table_id: u64,
    range: metadata_table_manager.RangeRecord,
    restore_backup_id: []const u8,
    restore_location: []const u8,
    placements: []const raft_reconciler.PlacementIntent,
    progress: []const metadata_table_manager.RestoreProgressRecord,
) bool {
    var found_any_placement = false;
    for (placements) |intent| {
        if (intent.record.group_id != range.group_id) continue;
        found_any_placement = true;
        const restored = findRestoreProgress(progress, table_id, intent.record.local_node_id, range.group_id) orelse return false;
        if (!std.mem.eql(u8, restored.backup_id, restore_backup_id)) return false;
        if (!std.mem.eql(u8, restored.artifact_backup_id, range.restore_artifact_backup_id)) return false;
        if (!std.mem.eql(u8, restored.location, restore_location)) return false;
        if (!std.mem.eql(u8, restored.snapshot_path, range.restore_snapshot_path)) return false;
        if (!std.mem.eql(u8, restored.artifact_sha256, range.restore_artifact_sha256)) return false;
        if (!restored.primary_restored or !restored.runtime_repair_complete) return false;
    }
    return found_any_placement;
}

fn collectProjectedSplitObservations(
    alloc: std.mem.Allocator,
    service: anytype,
    split_transitions: []const transition_state.SplitTransitionRecord,
) ![]transition_state.SplitObservationRecord {
    var out = std.ArrayListUnmanaged(transition_state.SplitObservationRecord).empty;
    errdefer out.deinit(alloc);
    for (split_transitions) |record| {
        const observation = (try service.observeSplitTransition(record.transition_id)) orelse continue;
        try out.append(alloc, .{
            .transition_id = record.transition_id,
            .observation = observation,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn collectProjectedMergeObservations(
    alloc: std.mem.Allocator,
    service: anytype,
    merge_transitions: []const transition_state.MergeTransitionRecord,
) ![]transition_state.MergeObservationRecord {
    var out = std.ArrayListUnmanaged(transition_state.MergeObservationRecord).empty;
    errdefer out.deinit(alloc);
    for (merge_transitions) |record| {
        const observation = (try service.observeMergeTransition(record.transition_id)) orelse continue;
        try out.append(alloc, .{
            .transition_id = record.transition_id,
            .observation = observation,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn findSchemaProgress(
    records: []const metadata_table_manager.SchemaProgressRecord,
    table_id: u64,
    node_id: u64,
) ?metadata_table_manager.SchemaProgressRecord {
    for (records) |record| {
        if (record.table_id == table_id and record.node_id == node_id) return record;
    }
    return null;
}

fn findRestoreProgress(
    records: []const metadata_table_manager.RestoreProgressRecord,
    table_id: u64,
    node_id: u64,
    group_id: u64,
) ?metadata_table_manager.RestoreProgressRecord {
    for (records) |record| {
        if (record.table_id == table_id and record.node_id == node_id and record.group_id == group_id) return record;
    }
    return null;
}

fn syncLocalStoreStatus(
    service: anytype,
    local_node_id: u64,
    replica_root_dir: []const u8,
    scanned_backfill_markers: ?[]const StoreStatusBackfillMarker,
    use_provider: bool,
) !void {
    var admin_snapshot = try service.adminSnapshot();
    defer service.freeAdminSnapshot(&admin_snapshot);
    var owned_backfill_markers: ?[]const StoreStatusBackfillMarker = null;
    const backfill_markers = scanned_backfill_markers orelse blk: {
        owned_backfill_markers = try collectStoreStatusBackfillMarkers(service.alloc, replica_root_dir);
        break :blk owned_backfill_markers.?;
    };
    defer if (owned_backfill_markers) |markers| freeStoreStatusBackfillMarkers(service.alloc, markers);
    const stores = admin_snapshot.stores;
    const placements = admin_snapshot.placement_intents;
    const tables = admin_snapshot.tables;
    const ranges = admin_snapshot.ranges;
    const split_transitions = admin_snapshot.split_transitions;
    const merge_transitions = admin_snapshot.merge_transitions;
    const split_observations = admin_snapshot.split_observations;
    const merge_observations = admin_snapshot.merge_observations;
    const backend_runtime = try service.ensureBackendRuntime();
    const status_io = backend_runtime.io() orelse if (builtin.is_test)
        std.testing.io
    else
        return error.BackendIoUnavailable;

    var local_stores = std.ArrayListUnmanaged(metadata_table_manager.StoreRecord).empty;
    defer local_stores.deinit(service.alloc);
    for (stores) |store| {
        if (store.node_id != local_node_id) continue;
        try local_stores.append(service.alloc, store);
    }

    if (local_stores.items.len == 0) return;
    const group_statuses = try collectLocalGroupStatusReportsWithProvider(
        service,
        service.alloc,
        backend_runtime,
        status_io,
        local_node_id,
        replica_root_dir,
        tables,
        ranges,
        stores,
        admin_snapshot.merged_group_statuses,
        split_transitions,
        merge_transitions,
        split_observations,
        merge_observations,
        use_provider,
    );
    defer metadata_table_manager.freeGroupStatuses(service.alloc, group_statuses);
    if (local_stores.items.len == 1) {
        const report = try collectLocalStoreStatusReport(
            service,
            service.alloc,
            replica_root_dir,
            local_stores.items[0],
            ranges,
            group_statuses,
            backfill_markers,
        );
        defer freeOwnedStoreStatusReport(service.alloc, report);
        try service.reportStoreStatus(report);
        try maybeRequestStoreStatusBackfillMarkerRescan(service, replica_root_dir, scanned_backfill_markers, backfill_markers);
        return;
    }

    const reports = try collectExplicitLocalStoreStatusReports(
        service,
        service.alloc,
        status_io,
        replica_root_dir,
        local_stores.items,
        ranges,
        group_statuses,
        backfill_markers,
    );
    defer freeOwnedStoreStatusReports(service.alloc, reports);
    if (reports.len > 0) {
        _ = try reportStoreStatusesWithProjected(service, stores, reports);
        try maybeRequestStoreStatusBackfillMarkerRescan(service, replica_root_dir, scanned_backfill_markers, backfill_markers);
        return;
    }

    const shared_reports = try collectSharedRootLocalStoreStatusReports(
        service,
        service.alloc,
        status_io,
        replica_root_dir,
        local_node_id,
        local_stores.items,
        placements,
        tables,
        ranges,
        group_statuses,
        backfill_markers,
    );
    defer freeOwnedStoreStatusReports(service.alloc, shared_reports);
    if (shared_reports.len == 0) {
        try maybeRequestStoreStatusBackfillMarkerRescan(service, replica_root_dir, scanned_backfill_markers, backfill_markers);
        return;
    }
    _ = try reportStoreStatusesWithProjected(service, stores, shared_reports);
    try maybeRequestStoreStatusBackfillMarkerRescan(service, replica_root_dir, scanned_backfill_markers, backfill_markers);
}

fn reportStoreStatusesWithProjected(
    service: anytype,
    projected: []metadata_table_manager.StoreRecord,
    reports: []const metadata_table_manager.StoreStatusReport,
) !usize {
    // Reporter fences and both publishing and clearing repair facts are v13
    // transitions. Avoid the activation lookup for the overwhelmingly common
    // all-v12 case, but fail closed as soon as either side carries v13 state.
    // Otherwise a status report could introduce an unreadable record or erase
    // committed causal authority during startup or a transient marker failure.
    const repair_status_transition_possible = reportsHaveRuntimeRepairStatus(reports) or
        storesHaveRuntimeRepairStatus(projected);
    const current_protocol_transition_possible = repair_status_transition_possible or
        reportsHaveRuntimeReporterFence(reports) or
        storesHaveRuntimeReporterFence(projected);
    const current_protocol_ready = !current_protocol_transition_possible or
        service.runtimeStatusRepairProtocolReady();
    const include_repair_status = !repair_status_transition_possible or current_protocol_ready;
    var changed_indices = std.ArrayListUnmanaged(usize).empty;
    defer changed_indices.deinit(service.alloc);
    for (reports) |report| {
        if (!metadata_table_manager.reporterFenceValid(
            report.reporter_incarnation,
            report.status_generation,
        )) return error.InvalidStoreReporterFence;
        const index = metadata_store_observer.findStoreIndex(projected, report.store_id) orelse return error.UnknownStore;
        if (!metadata_store_observer.observationChangesRecordWithRepairStatus(
            projected[index],
            report,
            include_repair_status,
        )) continue;
        try changed_indices.append(service.alloc, index);
    }

    const applied = try metadata_store_observer.applyObservationsOwnedWithRepairStatus(
        service.alloc,
        projected,
        reports,
        include_repair_status,
    );
    for (changed_indices.items) |index| {
        // If committed v13 facts were preserved while activation is unknown,
        // do not feed them through the proposal-boundary downgrade (which has
        // no prior-record context and would erase them). The heartbeat is
        // retried after the background probe or durable marker becomes visible.
        if (!current_protocol_ready and
            (storeHasRuntimeRepairStatus(projected[index]) or
                projected[index].reporter_incarnation != 0)) continue;
        try service.upsertStore(projected[index]);
    }
    return applied;
}

fn collectExplicitLocalStoreStatusReports(
    service: anytype,
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    stores: []const metadata_table_manager.StoreRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    group_statuses: []const metadata_table_manager.GroupStatusReport,
    backfill_markers: []const StoreStatusBackfillMarker,
) ![]metadata_table_manager.StoreStatusReport {
    var reports = std.ArrayListUnmanaged(metadata_table_manager.StoreStatusReport).empty;
    errdefer reports.deinit(alloc);

    for (stores) |store| {
        const store_root = try std.fmt.allocPrint(alloc, "{s}/store-{d}", .{ replica_root_dir, store.store_id });
        defer alloc.free(store_root);

        var dir = openDirPath(io, store_root, false) catch |err| switch (err) {
            error.FileNotFound, error.NotDir => {
                reports.deinit(alloc);
                return try alloc.alloc(metadata_table_manager.StoreStatusReport, 0);
            },
            else => return err,
        };
        dir.close(io);

        try reports.append(alloc, try collectLocalStoreStatusReport(
            service,
            alloc,
            store_root,
            store,
            ranges,
            group_statuses,
            backfill_markers,
        ));
    }

    return try reports.toOwnedSlice(alloc);
}

fn collectLocalStoreStatusReport(
    service: anytype,
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    store: metadata_table_manager.StoreRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    group_statuses: []const metadata_table_manager.GroupStatusReport,
    backfill_markers: []const StoreStatusBackfillMarker,
) !metadata_table_manager.StoreStatusReport {
    _ = service;
    _ = replica_root_dir;
    var report: metadata_table_manager.StoreStatusReport = .{
        .store_id = store.store_id,
        .live = store.live,
        .health_class = store.health_class,
        .capacity_bytes = store.capacity_bytes,
        .available_bytes = store.available_bytes,
        .lease_pressure = store.lease_pressure,
        .read_load = store.read_load,
        .write_load = store.write_load,
        .active_backfills = 0,
        .backfill_progress_millis = 1000,
        .group_statuses = &.{},
        .runtime_statuses = &.{},
    };

    var progress_sum: f64 = 0.0;
    for (backfill_markers) |marker| {
        if (!storeStatusBackfillMarkerMatchesStore(marker, store.store_id, true)) continue;
        if (!backfillMarkerMatchesRuntimeGeneration(marker, store.runtime_statuses)) continue;
        try accumulateStoreStatusBackfillProgress(
            alloc,
            ranges,
            marker,
            &report.active_backfills,
            &progress_sum,
        );
    }

    if (report.active_backfills > 0) {
        const avg_progress = progress_sum / @as(f64, @floatFromInt(report.active_backfills));
        const millis = std.math.clamp(avg_progress * 1000.0, 0.0, 1000.0);
        report.backfill_progress_millis = @intFromFloat(millis);
    }
    report.runtime_statuses = try collectStoreRuntimeStatusesWithQuarantinedBackfills(
        alloc,
        store.runtime_statuses,
        ranges,
        backfill_markers,
        store.store_id,
        store.node_id,
        true,
    );
    errdefer metadata_table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
    report.group_statuses = try metadata_table_manager.cloneGroupStatuses(alloc, group_statuses);
    return report;
}

fn collectSharedRootLocalStoreStatusReports(
    service: anytype,
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    local_node_id: u64,
    stores: []const metadata_table_manager.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
    tables: []const metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    group_statuses: []const metadata_table_manager.GroupStatusReport,
    backfill_markers: []const StoreStatusBackfillMarker,
) ![]metadata_table_manager.StoreStatusReport {
    _ = service;
    var reports = try alloc.alloc(metadata_table_manager.StoreStatusReport, stores.len);
    errdefer {
        for (reports) |report| {
            metadata_table_manager.freeGroupStatuses(alloc, report.group_statuses);
            metadata_table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
        }
        alloc.free(reports);
    }
    for (stores, 0..) |store, i| {
        reports[i] = .{
            .store_id = store.store_id,
            .live = store.live,
            .health_class = store.health_class,
            .capacity_bytes = store.capacity_bytes,
            .available_bytes = store.available_bytes,
            .lease_pressure = store.lease_pressure,
            .read_load = store.read_load,
            .write_load = store.write_load,
            .active_backfills = 0,
            .backfill_progress_millis = 1000,
            .group_statuses = &.{},
            .runtime_statuses = &.{},
        };
    }

    var progress_sum = try alloc.alloc(f64, stores.len);
    defer alloc.free(progress_sum);
    @memset(progress_sum, 0.0);
    var quarantined = try alloc.alloc(std.ArrayListUnmanaged(StoreStatusBackfillMarker), stores.len);
    defer alloc.free(quarantined);
    for (quarantined) |*list| list.* = .empty;
    defer for (quarantined) |*list| list.deinit(alloc);

    for (backfill_markers) |marker| {
        if (marker.store_id != null) continue;
        const range = findRangeByGroupId(ranges, marker.group_id) orelse continue;
        const store_id = try resolveSharedRootStoreAffinity(alloc, io, replica_root_dir, local_node_id, marker.group_id, stores, placements, tables, range);
        const report_index = findStoreStatusReportIndex(reports, store_id) orelse continue;
        if (!backfillMarkerMatchesRuntimeGeneration(marker, stores[report_index].runtime_statuses)) continue;
        try accumulateStoreStatusBackfillProgress(
            alloc,
            ranges,
            marker,
            &reports[report_index].active_backfills,
            &progress_sum[report_index],
        );
        if (marker.state == .corrupt) try quarantined[report_index].append(alloc, marker);
    }

    for (reports, 0..) |*report, i| {
        if (report.active_backfills == 0) continue;
        const avg_progress = progress_sum[i] / @as(f64, @floatFromInt(report.active_backfills));
        const millis = std.math.clamp(avg_progress * 1000.0, 0.0, 1000.0);
        report.backfill_progress_millis = @intFromFloat(millis);
    }
    for (reports) |*report| {
        report.group_statuses = try metadata_table_manager.cloneGroupStatuses(alloc, group_statuses);
    }
    for (reports, 0..) |*report, i| {
        report.runtime_statuses = try collectStoreRuntimeStatusesWithQuarantinedBackfills(
            alloc,
            stores[i].runtime_statuses,
            ranges,
            quarantined[i].items,
            report.store_id,
            stores[i].node_id,
            true,
        );
    }
    return reports;
}

fn shouldRefreshLocalStoreStatus(service: anytype, backfill_markers: []const StoreStatusBackfillMarker) bool {
    if (@field(service, "local_group_status_provider") != null and backfill_markers.len == 0) return false;
    return true;
}

fn shouldRefreshLocalStoreStatusForLifecycleRound(service: anytype, backfill_markers: []const StoreStatusBackfillMarker) bool {
    if (backfill_markers.len > 0) return true;
    if (@field(service, "local_group_status_provider") != null) return true;
    return shouldRefreshLocalStoreStatus(service, backfill_markers);
}

fn collectLocalGroupStatusReportsWithProvider(
    service: anytype,
    alloc: std.mem.Allocator,
    backend_runtime: *backend_runtime_mod.BackendRuntime,
    io: std.Io,
    local_node_id: u64,
    replica_root_dir: []const u8,
    tables: []const metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    stores: []const metadata_table_manager.StoreRecord,
    merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
    split_transitions: []const transition_state.SplitTransitionRecord,
    merge_transitions: []const transition_state.MergeTransitionRecord,
    split_observations: []const transition_state.SplitObservationRecord,
    merge_observations: []const transition_state.MergeObservationRecord,
    use_provider: bool,
) ![]metadata_table_manager.GroupStatusReport {
    if (use_provider) {
        if (@field(service, "local_group_status_provider")) |provider| {
            return try provider.collect(
                alloc,
                replica_root_dir,
                tables,
                ranges,
                stores,
                merged_group_statuses,
                split_transitions,
                merge_transitions,
                split_observations,
                merge_observations,
            );
        }
    }
    return try collectLocalGroupStatusReports(
        service,
        alloc,
        backend_runtime,
        io,
        local_node_id,
        replica_root_dir,
        tables,
        ranges,
        stores,
        merged_group_statuses,
        split_transitions,
        merge_transitions,
        split_observations,
        merge_observations,
    );
}

fn collectLocalGroupStatusReports(
    service: anytype,
    alloc: std.mem.Allocator,
    backend_runtime: *backend_runtime_mod.BackendRuntime,
    io: std.Io,
    local_node_id: u64,
    replica_root_dir: []const u8,
    tables: []const metadata_table_manager.TableRecord,
    ranges: []const metadata_table_manager.RangeRecord,
    stores: []const metadata_table_manager.StoreRecord,
    merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
    split_transitions: []const transition_state.SplitTransitionRecord,
    merge_transitions: []const transition_state.MergeTransitionRecord,
    split_observations: []const transition_state.SplitObservationRecord,
    merge_observations: []const transition_state.MergeObservationRecord,
) ![]metadata_table_manager.GroupStatusReport {
    var reports = std.ArrayListUnmanaged(metadata_table_manager.GroupStatusReport).empty;
    errdefer {
        for (reports.items) |record| metadata_table_manager.freeGroupStatus(alloc, record);
        reports.deinit(alloc);
    }

    for (ranges) |range| {
        _ = findTableById(tables, range.table_id) orelse continue;
        const db_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/table-db", .{ replica_root_dir, range.group_id });
        defer alloc.free(db_path);

        const path_present = present: {
            _ = statFilePath(io, db_path) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => break :present false,
                else => return err,
            };
            break :present true;
        };

        const group_status = if (path_present)
            try collectLocalGroupStatusReport(
                service,
                alloc,
                backend_runtime,
                io,
                db_path,
                replica_root_dir,
                range.group_id,
                stores,
                merged_group_statuses,
                split_transitions,
                merge_transitions,
                split_observations,
                merge_observations,
            )
        else
            null;
        if (group_status) |status| {
            errdefer metadata_table_manager.freeGroupStatus(alloc, status);
            try reports.append(alloc, status);
        } else if (latestLocalGroupStatus(stores, local_node_id, range.group_id)) |previous| {
            var preserved = previous;
            const readiness = transition_state.readinessForGroupWithObservations(
                range.group_id,
                split_transitions,
                merge_transitions,
                split_observations,
                merge_observations,
            );
            preserved.transition_pending = readiness.transition_pending;
            preserved.replay_required = readiness.replay_required;
            preserved.replay_caught_up = readiness.replay_caught_up;
            preserved.cutover_ready = readiness.cutover_ready;
            preserved.reads_ready_after_cutover = readiness.reads_ready_after_cutover;
            try reports.append(alloc, preserved);
        }
    }

    return try reports.toOwnedSlice(alloc);
}

fn collectLocalGroupStatusReport(
    service: anytype,
    alloc: std.mem.Allocator,
    backend_runtime: *backend_runtime_mod.BackendRuntime,
    io: std.Io,
    db_path: []const u8,
    replica_root_dir: ?[]const u8,
    group_id: u64,
    stores: []const metadata_table_manager.StoreRecord,
    merged_group_statuses: []const metadata_reconciler.MergedGroupStatus,
    split_transitions: []const transition_state.SplitTransitionRecord,
    merge_transitions: []const transition_state.MergeTransitionRecord,
    split_observations: []const transition_state.SplitObservationRecord,
    merge_observations: []const transition_state.MergeObservationRecord,
) !?metadata_table_manager.GroupStatusReport {
    _ = stores;
    _ = merged_group_statuses;
    var db = db_mod.DB.open(alloc, db_path, .{
        // This path is only a fallback when no local data-runtime provider is
        // installed. Group status needs primary identity count and filesystem
        // size, never query execution. Catalog-only mode avoids mmap/open of
        // every derived segment and cannot race a live index generation.
        .open_mode = .status_only,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .transaction_recovery = .{ .enabled = false },
        .text_merge = .{ .enabled = false },
        .backend_runtime = backend_runtime,
    }) catch |err| switch (err) {
        error.GenerationTransitionActive, error.FileNotFound, error.NotDir => return null,
        else => return err,
    };
    defer db.close();

    const stats = try db.stats(alloc);
    defer db_mod.types.freeDBStats(alloc, stats);

    const now_realtime_ms = platform_clock.Clock.real().nowRealtimeMs();
    const created_at_millis = (try db.getGroupCreatedAtMillis(alloc, group_id)) orelse now_realtime_ms;
    const readiness = if (replica_root_dir) |root_dir|
        try transition_state.readinessForLocalGroup(
            alloc,
            io,
            root_dir,
            group_id,
            split_transitions,
            merge_transitions,
            split_observations,
            merge_observations,
        )
    else
        transition_state.readinessForGroup(group_id, split_transitions, merge_transitions);
    const membership = serviceGroupMembership(service, group_id);

    return .{
        .group_id = group_id,
        .doc_count = stats.doc_count,
        .disk_bytes = try directoryUsageBytes(alloc, io, db_path),
        .empty = stats.doc_count == 0,
        .created_at_millis = created_at_millis,
        .updated_at_millis = now_realtime_ms,
        .local_leader = membership.local_leader,
        .local_voter = membership.local_voter,
        .voter_count = membership.voter_count,
        .voter_set_known = membership.voter_set_known,
        .voter_set_fingerprint = membership.voter_set_fingerprint,
        .joint_consensus = membership.joint_consensus,
        .raft_term = membership.raft_term,
        .raft_membership_index = membership.raft_membership_index,
        .transition_pending = readiness.transition_pending,
        .replay_required = readiness.replay_required,
        .replay_caught_up = readiness.replay_caught_up,
        .cutover_ready = readiness.cutover_ready,
        .reads_ready_after_cutover = readiness.reads_ready_after_cutover,
    };
}

fn latestLocalGroupStatus(
    stores: []const metadata_table_manager.StoreRecord,
    local_node_id: u64,
    group_id: u64,
) ?metadata_table_manager.GroupStatusReport {
    var latest: ?metadata_table_manager.GroupStatusReport = null;
    for (stores) |store| {
        if (store.node_id != local_node_id) continue;
        for (store.group_statuses) |status| {
            if (status.group_id != group_id) continue;
            if (latest == null or
                status.raft_term > latest.?.raft_term or
                (status.raft_term == latest.?.raft_term and
                    status.raft_applied_index > latest.?.raft_applied_index) or
                (status.raft_term == latest.?.raft_term and
                    status.raft_applied_index == latest.?.raft_applied_index and
                    status.updated_at_millis > latest.?.updated_at_millis))
            {
                latest = status;
            }
        }
    }
    return latest;
}

test "metadata service status preserves the last observation during a generation transition" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/{s}/status-generation-transition",
        .{tmp.sub_path},
    );
    defer alloc.free(replica_root);
    const db_path = try std.fmt.allocPrint(alloc, "{s}/group-77/table-db", .{replica_root});
    defer alloc.free(db_path);
    try fs_paths.createDirPathPortable(std.testing.io, db_path);

    var runtime = try backend_runtime_mod.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var transition = try db_mod.generation_lifecycle.beginProcessExclusive(db_path);
    defer transition.deinit();

    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 7, .name = "docs" },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 77, .range_id = 77, .table_id = 7, .start_key = "" },
    };
    var previous_statuses = [_]metadata_table_manager.GroupStatusReport{.{
        .group_id = 77,
        .doc_count = 42,
        .disk_bytes = 4096,
        .disk_bytes_known = true,
        .empty = false,
        .updated_at_millis = 1234,
        .local_leader = true,
        .local_voter = true,
        .voter_count = 1,
    }};
    var stores = [_]metadata_table_manager.StoreRecord{.{
        .store_id = 1,
        .node_id = 1,
        .group_statuses = &previous_statuses,
    }};
    const splits = [_]transition_state.SplitTransitionRecord{.{
        .transition_id = 7001,
        .attempt_epoch = 1,
        .source_group_id = 77,
        .destination_group_id = 78,
        .phase = .prepare,
    }};
    const split_observations = [_]transition_state.SplitObservationRecord{.{
        .transition_id = 7001,
        .observation = .{
            .status = .{
                .phase = .cutover_ready,
                .source_split_phase = .splitting,
                .bootstrapped = true,
                .replay_required = true,
                .replay_caught_up = true,
                .cutover_ready = true,
                .destination_ready_for_reads = true,
                .source_delta_sequence = 5,
                .dest_delta_sequence = 5,
            },
        },
    }};
    var fake_service: struct {} = .{};

    const reports = try collectLocalGroupStatusReports(
        &fake_service,
        alloc,
        runtime.ptr(),
        runtime.ptr().io().?,
        1,
        replica_root,
        &tables,
        &ranges,
        &stores,
        &.{},
        &splits,
        &.{},
        &split_observations,
        &.{},
    );
    defer metadata_table_manager.freeGroupStatuses(alloc, reports);

    try std.testing.expectEqual(@as(usize, 1), reports.len);
    try std.testing.expectEqual(@as(u64, 42), reports[0].doc_count);
    try std.testing.expectEqual(@as(u64, 1234), reports[0].updated_at_millis);
    try std.testing.expect(reports[0].transition_pending);
    try std.testing.expect(reports[0].replay_required);
    try std.testing.expect(reports[0].replay_caught_up);
    try std.testing.expect(reports[0].cutover_ready);
    try std.testing.expect(reports[0].reads_ready_after_cutover);
}

const ServiceGroupRaftObservation = struct {
    local_node_id: u64 = 0,
    role: []const u8 = "absent",
    leader_id: ?u64 = null,
    term: u64 = 0,
    commit_index: u64 = 0,
    applied_index: u64 = 0,
    local_voter: bool = false,
    voter_count: usize = 0,
    voter_set_fingerprint: ?metadata_api.MetadataRaftVoterSetFingerprint = null,
    joint_consensus: bool = false,
    learner_count: usize = 0,
    election_elapsed: u32 = 0,
    randomized_election_timeout: u32 = 0,
    votes_granted: usize = 0,
    votes_rejected: usize = 0,
    votes_unknown: usize = 0,
    inbound_message_enqueues: usize = 0,
    inbound_message_drains: usize = 0,
    pending_inbound_messages: usize = 0,
    transport_sent_frames: usize = 0,
    transport_send_failures: usize = 0,
    transport_retries_scheduled: usize = 0,
    transport_retries_exhausted: usize = 0,
    transport_retried_successes: usize = 0,
    transport_peer_refreshes: usize = 0,
    transport_peer_routes: usize = 0,
    transport_served_groups: usize = 0,
    transport_pending_retries: usize = 0,
};

fn serviceGroupRaftTopologyObservation(service: anytype, group_id: u64) ServiceGroupRaftObservation {
    const Service = @TypeOf(service);
    if (Service == *MetadataService) {
        const raft_status = service.raft.host.host.raftStatus(group_id) orelse return .{};
        return raftObservationFromStatus(raft_status, service.raft.host.host.cfg.local_node_id);
    }
    if (Service == *MetadataHttpService) {
        const raft_status = service.raft.host.http_host.host.raftStatus(group_id) orelse return .{};
        return raftObservationFromStatus(raft_status, service.raft.host.http_host.host.cfg.local_node_id);
    }
    return .{};
}

fn serviceGroupRaftObservation(service: anytype, group_id: u64) ServiceGroupRaftObservation {
    const Service = @TypeOf(service);
    var observation = serviceGroupRaftTopologyObservation(service, group_id);
    if (Service == *MetadataService) {
        const host_metrics = service.raft.host.host.metricsSnapshot();
        observation.inbound_message_enqueues = host_metrics.inbound_message_enqueues;
        observation.inbound_message_drains = host_metrics.inbound_message_drains;
        observation.pending_inbound_messages = host_metrics.pending_inbound_messages;
        return observation;
    }
    if (Service == *MetadataHttpService) {
        const host_metrics = service.raft.host.http_host.host.metricsSnapshot();
        observation.inbound_message_enqueues = host_metrics.inbound_message_enqueues;
        observation.inbound_message_drains = host_metrics.inbound_message_drains;
        observation.pending_inbound_messages = host_metrics.pending_inbound_messages;
        const transport_metrics = service.raft.host.http_host.transport_stack.transport_host.metricsSnapshot();
        observation.transport_sent_frames = transport_metrics.sent_frames;
        observation.transport_send_failures = transport_metrics.send_failures;
        observation.transport_retries_scheduled = transport_metrics.retries_scheduled;
        observation.transport_retries_exhausted = transport_metrics.retries_exhausted;
        observation.transport_retried_successes = transport_metrics.retried_successes;
        observation.transport_peer_refreshes = transport_metrics.peer_refreshes;
        observation.transport_peer_routes = service.raft.host.http_host.transport_stack.transport_host.peer_routes.count();
        observation.transport_served_groups = service.raft.host.http_host.transport_stack.transport_host.served_groups.count();
        observation.transport_pending_retries = service.raft.host.http_host.transport_stack.transport_host.pendingRetryCount();
        return observation;
    }
    return .{};
}

fn applyMetadataRaftTopology(status: *MetadataStatus, observation: ServiceGroupRaftObservation) void {
    status.metadata_raft_local_node_id = observation.local_node_id;
    status.metadata_raft_role = observation.role;
    status.metadata_raft_leader_id = observation.leader_id;
    status.metadata_raft_term = observation.term;
    status.metadata_raft_commit_index = observation.commit_index;
    status.metadata_raft_local_voter = observation.local_voter;
    status.metadata_raft_voter_count = observation.voter_count;
    status.metadata_raft_voter_set_fingerprint = observation.voter_set_fingerprint;
    status.metadata_raft_joint_consensus = observation.joint_consensus;
    status.metadata_raft_learner_count = observation.learner_count;
    status.metadata_raft_election_elapsed = observation.election_elapsed;
    status.metadata_raft_randomized_election_timeout = observation.randomized_election_timeout;
    status.metadata_raft_votes_granted = observation.votes_granted;
    status.metadata_raft_votes_rejected = observation.votes_rejected;
    status.metadata_raft_votes_unknown = observation.votes_unknown;
}

fn metadataRuntimeTopology(
    metadata_group_id: u64,
    metadata_incarnation: ?metadata_api.MetadataClusterIncarnation,
    observation: ServiceGroupRaftObservation,
) metadata_api.MetadataRuntimeTopology {
    return .{
        .metadata_group_id = metadata_group_id,
        .metadata_incarnation = metadata_incarnation,
        .metadata_raft_local_node_id = observation.local_node_id,
        .metadata_raft_role = observation.role,
        .metadata_raft_leader_id = observation.leader_id,
        .metadata_raft_term = observation.term,
        .metadata_raft_local_voter = observation.local_voter,
        .metadata_raft_voter_count = observation.voter_count,
        .metadata_raft_voter_set_fingerprint = observation.voter_set_fingerprint,
        .metadata_raft_joint_consensus = observation.joint_consensus,
        .metadata_raft_learner_count = observation.learner_count,
    };
}

fn raftObservationFromStatus(
    raft_status: raft_engine.core.Status,
    local_node_id: u64,
) ServiceGroupRaftObservation {
    var local_voter = false;
    for (raft_status.conf_state.voters) |node_id| {
        if (node_id == local_node_id) {
            local_voter = true;
            break;
        }
    }
    const voter_set_fingerprint = metadata_table_manager.voterSetFingerprint(raft_status.conf_state.voters, null);
    return .{
        .local_node_id = local_node_id,
        .role = raftRoleName(raft_status.soft.role),
        .leader_id = raft_status.soft.leader_id,
        .term = raft_status.hard.current_term,
        .commit_index = raft_status.hard.commit_index,
        .applied_index = raft_status.applied_index,
        .local_voter = local_voter,
        .voter_count = raft_status.conf_state.voters.len,
        .voter_set_fingerprint = std.fmt.bytesToHex(voter_set_fingerprint, .lower),
        .joint_consensus = raft_status.conf_state.voters_outgoing.len > 0,
        .learner_count = raft_status.conf_state.learners.len + raft_status.conf_state.learners_next.len,
        .election_elapsed = raft_status.election_elapsed,
        .randomized_election_timeout = raft_status.randomized_election_timeout,
        .votes_granted = raft_status.votes_granted,
        .votes_rejected = raft_status.votes_rejected,
        .votes_unknown = raft_status.votes_unknown,
    };
}

test "metadata service Raft observation exposes canonical membership configuration" {
    var voters = [_]u64{ 3, 1, 2 };
    var outgoing = [_]u64{ 1, 2 };
    var learners = [_]u64{4};
    var learners_next = [_]u64{5};
    const observation = raftObservationFromStatus(.{
        .id = 2,
        .group_id = 1,
        .soft = .{ .leader_id = 1 },
        .hard = .{},
        .conf_state = .{
            .voters = &voters,
            .voters_outgoing = &outgoing,
            .learners = &learners,
            .learners_next = &learners_next,
        },
    }, 2);
    const expected = std.fmt.bytesToHex(metadata_table_manager.voterSetFingerprint(&voters, null), .lower);
    try std.testing.expectEqualStrings(&expected, &(observation.voter_set_fingerprint orelse return error.TestUnexpectedResult));
    try std.testing.expect(observation.joint_consensus);
    try std.testing.expectEqual(@as(usize, 2), observation.learner_count);
}

fn raftRoleName(role: raft_engine.core.types.StateRole) []const u8 {
    return switch (role) {
        .follower => "follower",
        .pre_candidate => "pre_candidate",
        .candidate => "candidate",
        .leader => "leader",
    };
}

const ServiceGroupMembership = struct {
    local_leader: bool = false,
    local_voter: bool = false,
    voter_count: u16 = 0,
    voter_set_known: bool = false,
    voter_set_fingerprint: metadata_table_manager.VoterSetFingerprint = [_]u8{0} ** metadata_table_manager.voter_set_fingerprint_len,
    joint_consensus: bool = false,
    raft_term: u64 = 0,
    raft_membership_index: u64 = 0,
};

fn serviceGroupMembership(service: anytype, group_id: u64) ServiceGroupMembership {
    const Service = @TypeOf(service);
    if (Service == *MetadataService) {
        const raft_status = service.raft.host.host.raftStatus(group_id) orelse return .{};
        var local_voter = false;
        for (raft_status.conf_state.voters) |node_id| {
            if (node_id == service.raft.host.host.cfg.local_node_id) {
                local_voter = true;
                break;
            }
        }
        return .{
            .local_leader = raft_status.soft.role == .leader and
                raft_status.soft.leader_id == raft_status.id,
            .local_voter = local_voter,
            .voter_count = @intCast(raft_status.conf_state.voters.len),
            .voter_set_known = true,
            .voter_set_fingerprint = metadata_table_manager.voterSetFingerprint(raft_status.conf_state.voters, null),
            .joint_consensus = raft_status.conf_state.voters_outgoing.len > 0,
            .raft_term = raft_status.hard.current_term,
            .raft_membership_index = raft_status.applied_index,
        };
    }
    if (Service == *MetadataHttpService) {
        const raft_status = service.raft.host.http_host.host.raftStatus(group_id) orelse return .{};
        var local_voter = false;
        for (raft_status.conf_state.voters) |node_id| {
            if (node_id == service.raft.host.http_host.host.cfg.local_node_id) {
                local_voter = true;
                break;
            }
        }
        return .{
            .local_leader = raft_status.soft.role == .leader and
                raft_status.soft.leader_id == raft_status.id,
            .local_voter = local_voter,
            .voter_count = @intCast(raft_status.conf_state.voters.len),
            .voter_set_known = true,
            .voter_set_fingerprint = metadata_table_manager.voterSetFingerprint(raft_status.conf_state.voters, null),
            .joint_consensus = raft_status.conf_state.voters_outgoing.len > 0,
            .raft_term = raft_status.hard.current_term,
            .raft_membership_index = raft_status.applied_index,
        };
    }
    return .{};
}

fn directoryUsageBytes(alloc: std.mem.Allocator, io: std.Io, path: []const u8) !u64 {
    var dir = openDirPath(io, path, true) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer dir.close(io);

    var total: u64 = 0;
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        const stat = try dir.statFile(io, entry.path, .{});
        total += stat.size;
    }
    return total;
}

fn statFilePath(io: anytype, path: []const u8) !std.Io.Dir.Stat {
    if (std.fs.path.isAbsolute(path)) {
        var file = try std.Io.Dir.openFileAbsolute(io, path, .{});
        defer file.close(io);
        return try file.stat(io);
    }
    return try std.Io.Dir.cwd().statFile(io, path, .{});
}

fn openDirPath(io: anytype, path: []const u8, iterate: bool) !std.Io.Dir {
    const opts: std.Io.Dir.OpenOptions = .{ .iterate = iterate };
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, opts)
    else
        try std.Io.Dir.cwd().openDir(io, path, opts);
}

const StoreStatusBackfillMarker = struct {
    store_id: ?u64,
    group_id: u64,
    path: []const u8,
    owner_generation: ?u64 = null,
    state: State = .absent,

    const State = union(enum) {
        absent,
        legacy,
        corrupt,
        valid: []const u8,

        fn deinit(self: State, alloc: std.mem.Allocator) void {
            switch (self) {
                .valid => |resume_key| alloc.free(resume_key),
                else => {},
            }
        }
    };
};

const StoreStatusBackfillMarkerCache = struct {
    markers: []StoreStatusBackfillMarker = &.{},
    scanned_at_ms: u64 = 0,
    rescan_requested: bool = false,

    fn deinit(self: *StoreStatusBackfillMarkerCache, alloc: std.mem.Allocator) void {
        freeStoreStatusBackfillMarkers(alloc, self.markers);
        self.* = .{};
    }

    fn replace(self: *StoreStatusBackfillMarkerCache, alloc: std.mem.Allocator, markers: []StoreStatusBackfillMarker, scanned_at_ms: u64) void {
        self.deinit(alloc);
        self.markers = markers;
        self.scanned_at_ms = scanned_at_ms;
        self.rescan_requested = false;
    }
};

fn maybeRefreshStoreStatusBackfillMarkerCache(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    store_status_ticks: usize,
    probe_ticks: *usize,
    cache: *StoreStatusBackfillMarkerCache,
) !void {
    const now_ms = monotonicMs();
    const should_rescan = if (cache.markers.len > 0)
        cache.rescan_requested or now_ms -| cache.scanned_at_ms >= store_status_backfill_rescan_interval_ms
    else
        cache.scanned_at_ms == 0 or
            cache.rescan_requested or
            ((store_status_ticks >= 40 or probe_ticks.* >= store_status_backfill_probe_interval_ticks) and
                now_ms -| cache.scanned_at_ms >= store_status_backfill_empty_rescan_interval_ms);
    if (!should_rescan) return;

    const markers = try collectStoreStatusBackfillMarkers(alloc, replica_root_dir);
    const markers_missing_state = storeStatusBackfillMarkersHaveMissingState(markers);
    cache.replace(alloc, markers, now_ms);
    cache.rescan_requested = markers_missing_state;
    probe_ticks.* = 0;
}

fn refreshStoreStatusBackfillMarkerCacheNow(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    probe_ticks: *usize,
    cache: *StoreStatusBackfillMarkerCache,
) !void {
    const markers = try collectStoreStatusBackfillMarkers(alloc, replica_root_dir);
    const markers_missing_state = storeStatusBackfillMarkersHaveMissingState(markers);
    cache.replace(alloc, markers, monotonicMs());
    cache.rescan_requested = markers_missing_state;
    probe_ticks.* = 0;
}

fn monotonicMs() u64 {
    return @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
}

fn scanStoreStatusBackfillMarkersWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
) ![]StoreStatusBackfillMarker {
    var root_dir = std.Io.Dir.cwd().openDir(io, replica_root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return &.{},
        else => return err,
    };
    defer root_dir.close(io);

    var markers = std.ArrayListUnmanaged(StoreStatusBackfillMarker).empty;
    errdefer {
        for (markers.items) |marker| {
            alloc.free(marker.path);
            marker.state.deinit(alloc);
        }
        markers.deinit(alloc);
    }

    var walker = try root_dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (backfill_state_mod.ownerGenerationFromPath(entry.path) == null and
            !std.mem.endsWith(u8, entry.path, "/rebuild.state"))
        {
            continue;
        }
        const parsed = parseStoreStatusBackfillMarkerPath(entry.path) orelse continue;
        try markers.append(alloc, .{
            .store_id = parsed.store_id,
            .group_id = parsed.group_id,
            .path = try alloc.dupe(u8, entry.path),
            .owner_generation = backfill_state_mod.ownerGenerationFromPath(entry.path),
        });
    }
    if (markers.items.len == 0) {
        markers.deinit(alloc);
        return &.{};
    }
    return try markers.toOwnedSlice(alloc);
}

fn loadStoreStatusBackfillMarkerResumeKeys(
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    markers: []StoreStatusBackfillMarker,
) !void {
    _ = try refreshStoreStatusBackfillMarkerResumeKeys(alloc, io, replica_root_dir, markers);
}

fn refreshStoreStatusBackfillMarkerResumeKeys(
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    markers: []StoreStatusBackfillMarker,
) !bool {
    var any_missing = false;

    for (markers) |*marker| {
        marker.state.deinit(alloc);
        marker.state = .absent;
        const state_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ replica_root_dir, marker.path });
        defer alloc.free(state_path);
        var loaded = try rebuildStateForPath(state_path).loadWithIo(alloc, io);
        marker.state = switch (loaded) {
            .absent => .absent,
            .legacy => .legacy,
            .corrupt => blk: {
                std.log.warn("derived index rebuild state quarantined path={s}", .{state_path});
                break :blk .corrupt;
            },
            .valid => |resume_key| blk: {
                loaded = undefined;
                break :blk .{ .valid = resume_key };
            },
        };
        if (marker.state == .absent) any_missing = true;
    }
    return any_missing;
}

fn rebuildStateForPath(path: []const u8) backfill_state_mod.RebuildState {
    const root_path = std.fs.path.dirname(path) orelse ".";
    if (backfill_state_mod.ownerGenerationFromPath(path)) |generation| {
        return backfill_state_mod.RebuildState.initOwned(root_path, null, generation);
    }
    return backfill_state_mod.RebuildState.init(root_path);
}

fn collectStoreStatusBackfillMarkers(alloc: std.mem.Allocator, replica_root_dir: []const u8) ![]StoreStatusBackfillMarker {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    const markers = try scanStoreStatusBackfillMarkersWithIo(alloc, io, replica_root_dir);
    errdefer freeStoreStatusBackfillMarkers(alloc, markers);
    try loadStoreStatusBackfillMarkerResumeKeys(alloc, io, replica_root_dir, markers);
    return markers;
}

fn storeStatusBackfillMarkersHaveMissingState(markers: []const StoreStatusBackfillMarker) bool {
    for (markers) |marker| {
        if (marker.state == .absent) return true;
    }
    return false;
}

fn backfillMarkerStateFileExistsWithIo(
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    marker: StoreStatusBackfillMarker,
) !bool {
    const state_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ replica_root_dir, marker.path });
    defer alloc.free(state_path);
    std.Io.Dir.cwd().access(io, state_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    return true;
}

fn backfillMarkerStateFileExists(
    alloc: std.mem.Allocator,
    replica_root_dir: []const u8,
    marker: StoreStatusBackfillMarker,
) !bool {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    return try backfillMarkerStateFileExistsWithIo(alloc, io_impl.io(), replica_root_dir, marker);
}

fn maybeRequestStoreStatusBackfillMarkerRescan(
    service: anytype,
    replica_root_dir: []const u8,
    scanned_backfill_markers: ?[]const StoreStatusBackfillMarker,
    active_backfill_markers: []const StoreStatusBackfillMarker,
) !void {
    if (scanned_backfill_markers == null) return;
    if (active_backfill_markers.len == 0) return;
    if (service.store_status_backfill_marker_cache.rescan_requested) return;

    var io_impl = std.Io.Threaded.init(service.alloc, .{});
    defer io_impl.deinit();
    for (active_backfill_markers) |marker| {
        if (marker.state == .absent) {
            service.store_status_backfill_marker_cache.rescan_requested = true;
            return;
        }
        if (!try backfillMarkerStateFileExistsWithIo(service.alloc, io_impl.io(), replica_root_dir, marker)) {
            service.store_status_backfill_marker_cache.rescan_requested = true;
            return;
        }
    }
}

fn freeStoreStatusBackfillMarkers(alloc: std.mem.Allocator, markers: []const StoreStatusBackfillMarker) void {
    for (markers) |marker| {
        alloc.free(marker.path);
        marker.state.deinit(alloc);
    }
    if (markers.len > 0) alloc.free(markers);
}

fn parseStoreStatusBackfillMarkerPath(path: []const u8) ?struct { store_id: ?u64, group_id: u64 } {
    const first_slash = std.mem.indexOfScalar(u8, path, '/') orelse return null;
    const first_segment = path[0..first_slash];
    if (parsePrefixedStoreStatusId(first_segment, "group-")) |group_id| {
        return .{ .store_id = null, .group_id = group_id };
    }
    const store_id = parsePrefixedStoreStatusId(first_segment, "store-") orelse return null;
    const rest = path[first_slash + 1 ..];
    const second_slash = std.mem.indexOfScalar(u8, rest, '/') orelse return null;
    const second_segment = rest[0..second_slash];
    const group_id = parsePrefixedStoreStatusId(second_segment, "group-") orelse return null;
    return .{ .store_id = store_id, .group_id = group_id };
}

fn parsePrefixedStoreStatusId(segment: []const u8, prefix: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, segment, prefix)) return null;
    return std.fmt.parseInt(u64, segment[prefix.len..], 10) catch null;
}

fn storeStatusBackfillMarkerMatchesStore(marker: StoreStatusBackfillMarker, store_id: u64, include_unassigned: bool) bool {
    if (marker.store_id) |assigned_store_id| return assigned_store_id == store_id;
    return include_unassigned;
}

fn backfillMarkerMatchesRuntimeGeneration(
    marker: StoreStatusBackfillMarker,
    runtime_statuses: []const metadata_table_manager.RuntimeGroupStatusReport,
) bool {
    const marker_generation = marker.owner_generation orelse return true;
    const index_dir = std.fs.path.dirname(marker.path) orelse return true;
    const index_name = std.fs.path.basename(index_dir);
    for (runtime_statuses) |status| {
        if (status.group_id != marker.group_id) continue;
        for (status.indexes) |index| {
            if (!std.mem.eql(u8, index.name, index_name)) continue;
            // A current runtime identity is authoritative for same-name
            // replacement. Ignore a stale worker's generation cursor instead
            // of reporting duplicate progress or quarantining the replacement.
            if (index.coverage_generation != 0) {
                return index.coverage_generation == marker_generation;
            }
        }
    }
    // Missing runtime status is not evidence of staleness. Preserve marker
    // visibility until the data runtime publishes an authoritative generation.
    return true;
}

test "metadata service ignores stale rebuild generation markers after replacement" {
    const marker = StoreStatusBackfillMarker{
        .store_id = 7,
        .group_id = 9501,
        .path = "store-7/group-9501/table-db/indexes/search_idx/rebuild.state.g-000000000000005f",
        .owner_generation = 95,
    };
    var indexes = [_]metadata_table_manager.RuntimeIndexStatusReport{.{
        .name = "search_idx",
        .kind = "full_text",
        .coverage_generation = 96,
    }};
    const statuses = [_]metadata_table_manager.RuntimeGroupStatusReport{.{
        .group_id = 9501,
        .indexes = &indexes,
    }};

    try std.testing.expect(!backfillMarkerMatchesRuntimeGeneration(marker, &statuses));
    indexes[0].coverage_generation = 95;
    try std.testing.expect(backfillMarkerMatchesRuntimeGeneration(marker, &statuses));

    var legacy = marker;
    legacy.owner_generation = null;
    try std.testing.expect(backfillMarkerMatchesRuntimeGeneration(legacy, &statuses));
}

const rebuild_state_quarantine_source = "rebuild_state_quarantine";
const rebuild_state_quarantine_error = "InvalidRebuildState";

fn collectStoreRuntimeStatusesWithQuarantinedBackfills(
    alloc: std.mem.Allocator,
    existing: []const metadata_table_manager.RuntimeGroupStatusReport,
    ranges: []const metadata_table_manager.RangeRecord,
    markers: []const StoreStatusBackfillMarker,
    store_id: u64,
    node_id: u64,
    include_unassigned: bool,
) ![]metadata_table_manager.RuntimeGroupStatusReport {
    // Keep the data runtime's authoritative observations intact. Quarantine
    // reports are reversible overlays: retaining the underlying report lets a
    // repaired marker disappear immediately on the next scan without waiting
    // for another data-server heartbeat.
    var base = std.ArrayListUnmanaged(metadata_table_manager.RuntimeGroupStatusReport).empty;
    errdefer freeOwnedRuntimeStatusList(alloc, &base);
    for (existing) |status| {
        if (std.mem.eql(u8, status.source, rebuild_state_quarantine_source)) continue;
        const cloned = try metadata_table_manager.cloneRuntimeGroupStatusReport(alloc, status);
        errdefer metadata_table_manager.freeRuntimeGroupStatusReport(alloc, cloned);
        try base.append(alloc, cloned);
    }

    var overlays = std.ArrayListUnmanaged(metadata_table_manager.RuntimeGroupStatusReport).empty;
    errdefer freeOwnedRuntimeStatusList(alloc, &overlays);
    for (markers) |marker| {
        if (marker.state != .corrupt) continue;
        if (!storeStatusBackfillMarkerMatchesStore(marker, store_id, include_unassigned)) continue;
        if (!backfillMarkerMatchesRuntimeGeneration(marker, base.items)) continue;

        const overlay_index = findRuntimeStatusIndexByGroup(overlays.items, marker.group_id) orelse blk: {
            var overlay = if (findRuntimeStatusIndexByGroup(base.items, marker.group_id)) |base_index|
                try metadata_table_manager.cloneRuntimeGroupStatusReport(alloc, base.items[base_index])
            else
                try quarantinedBackfillRuntimeStatus(
                    alloc,
                    marker,
                    findRangeByGroupId(ranges, marker.group_id),
                    store_id,
                    node_id,
                );
            errdefer metadata_table_manager.freeRuntimeGroupStatusReport(alloc, overlay);
            try replaceOwnedRuntimeStatusSource(alloc, &overlay, rebuild_state_quarantine_source);
            try overlays.append(alloc, overlay);
            break :blk overlays.items.len - 1;
        };
        try markRuntimeIndexQuarantined(alloc, &overlays.items[overlay_index], marker);
    }

    var statuses = std.ArrayListUnmanaged(metadata_table_manager.RuntimeGroupStatusReport).empty;
    errdefer freeOwnedRuntimeStatusList(alloc, &statuses);
    try statuses.ensureTotalCapacity(alloc, overlays.items.len + base.items.len);
    statuses.appendSliceAssumeCapacity(overlays.items);
    overlays.items.len = 0;
    statuses.appendSliceAssumeCapacity(base.items);
    base.items.len = 0;
    overlays.deinit(alloc);
    overlays = .empty;
    base.deinit(alloc);
    base = .empty;
    if (statuses.items.len == 0) {
        statuses.deinit(alloc);
        return &.{};
    }
    return try statuses.toOwnedSlice(alloc);
}

fn freeOwnedRuntimeStatusList(
    alloc: std.mem.Allocator,
    statuses: *std.ArrayListUnmanaged(metadata_table_manager.RuntimeGroupStatusReport),
) void {
    for (statuses.items) |status| metadata_table_manager.freeRuntimeGroupStatusReport(alloc, status);
    statuses.deinit(alloc);
}

fn findRuntimeStatusIndexByGroup(
    statuses: []const metadata_table_manager.RuntimeGroupStatusReport,
    group_id: u64,
) ?usize {
    for (statuses, 0..) |status, i| {
        if (status.group_id == group_id) return i;
    }
    return null;
}

fn replaceOwnedRuntimeStatusSource(
    alloc: std.mem.Allocator,
    status: *metadata_table_manager.RuntimeGroupStatusReport,
    source: []const u8,
) !void {
    if (std.mem.eql(u8, status.source, source)) return;
    const next = try alloc.dupe(u8, source);
    alloc.free(status.source);
    status.source = next;
}

fn markRuntimeIndexQuarantined(
    alloc: std.mem.Allocator,
    status: *metadata_table_manager.RuntimeGroupStatusReport,
    marker: StoreStatusBackfillMarker,
) !void {
    const index_dir = std.fs.path.dirname(marker.path) orelse return error.InvalidBackfillMarkerPath;
    const index_name = std.fs.path.basename(index_dir);
    for (status.indexes) |*index| {
        if (!std.mem.eql(u8, index.name, index_name)) continue;
        const load_error = try alloc.dupe(u8, rebuild_state_quarantine_error);
        if (index.load_error) |previous| alloc.free(previous);
        index.load_error = load_error;
        index.coverage_summary_ready = false;
        index.backfill_active = false;
        index.backfill_progress_millis = 0;
        index.replay_catch_up_required = false;
        return;
    }

    const name = try alloc.dupe(u8, index_name);
    errdefer alloc.free(name);
    const kind = try alloc.dupe(u8, "unknown");
    errdefer alloc.free(kind);
    const load_error = try alloc.dupe(u8, rebuild_state_quarantine_error);
    errdefer alloc.free(load_error);
    const next = try alloc.alloc(metadata_table_manager.RuntimeIndexStatusReport, status.indexes.len + 1);
    @memcpy(next[0..status.indexes.len], status.indexes);
    next[status.indexes.len] = .{
        .name = name,
        .kind = kind,
        .load_error = load_error,
        .coverage_summary_ready = false,
    };
    if (status.indexes.len > 0) alloc.free(status.indexes);
    status.indexes = next;
    status.index_count = @intCast(next.len);
}

fn quarantinedBackfillRuntimeStatus(
    alloc: std.mem.Allocator,
    marker: StoreStatusBackfillMarker,
    range: ?metadata_table_manager.RangeRecord,
    store_id: u64,
    node_id: u64,
) !metadata_table_manager.RuntimeGroupStatusReport {
    const index_dir = std.fs.path.dirname(marker.path) orelse return error.InvalidBackfillMarkerPath;
    const index_name = std.fs.path.basename(index_dir);
    const table_name = try alloc.dupe(u8, "");
    errdefer alloc.free(table_name);
    const source = try alloc.dupe(u8, rebuild_state_quarantine_source);
    errdefer alloc.free(source);
    const freshness = try alloc.dupe(u8, "failed");
    errdefer alloc.free(freshness);
    const projection_checkpoint_status = try alloc.dupe(u8, "repair_required");
    errdefer alloc.free(projection_checkpoint_status);
    const owned_index_name = try alloc.dupe(u8, index_name);
    errdefer alloc.free(owned_index_name);
    const index_kind = try alloc.dupe(u8, "unknown");
    errdefer alloc.free(index_kind);
    const load_error = try alloc.dupe(u8, rebuild_state_quarantine_error);
    errdefer alloc.free(load_error);
    const indexes = try alloc.alloc(metadata_table_manager.RuntimeIndexStatusReport, 1);
    errdefer alloc.free(indexes);
    indexes[0] = .{
        .name = owned_index_name,
        .kind = index_kind,
        .load_error = load_error,
        .coverage_summary_ready = false,
        .backfill_progress_millis = 0,
    };

    var status: metadata_table_manager.RuntimeGroupStatusReport = .{
        .table_id = if (range) |record| record.table_id else 0,
        .table_name = table_name,
        .group_id = marker.group_id,
        .store_id = store_id,
        .node_id = node_id,
        .updated_at_ns = platform_clock.Clock.real().nowRealtimeMs() * std.time.ns_per_ms,
        .source = source,
        .freshness = freshness,
        .index_count = 1,
        .indexes = indexes,
    };
    status.enrichment.projection_checkpoint_status = projection_checkpoint_status;
    return status;
}

fn accumulateStoreStatusBackfillProgress(
    alloc: std.mem.Allocator,
    ranges: []const metadata_table_manager.RangeRecord,
    marker: StoreStatusBackfillMarker,
    active_backfills: *u32,
    progress_sum: *f64,
) !void {
    const range = findRangeByGroupId(ranges, marker.group_id) orelse return;
    active_backfills.* += 1;
    const resume_key = switch (marker.state) {
        .valid => |key| key,
        .absent, .legacy, .corrupt => return,
    };
    const range_start = try internal_keys.documentRangeLowerAlloc(alloc, range.start_key);
    defer alloc.free(range_start);
    const range_end = if (range.end_key) |key| try internal_keys.documentRangeUpperAlloc(alloc, key) else null;
    defer if (range_end) |key| alloc.free(key);
    progress_sum.* += backfill_state_mod.estimateProgressForKey(
        range_start,
        if (range_end) |key| key else "",
        resume_key,
    );
}

fn freeOwnedStoreStatusReport(alloc: std.mem.Allocator, report: metadata_table_manager.StoreStatusReport) void {
    metadata_table_manager.freeGroupStatuses(alloc, report.group_statuses);
    metadata_table_manager.freeRuntimeGroupStatusReports(alloc, report.runtime_statuses);
}

fn freeOwnedStoreStatusReports(alloc: std.mem.Allocator, reports: []const metadata_table_manager.StoreStatusReport) void {
    for (reports) |report| freeOwnedStoreStatusReport(alloc, report);
    if (reports.len > 0) alloc.free(reports);
}

fn resolveSharedRootStoreAffinity(
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    local_node_id: u64,
    group_id: u64,
    stores: []const metadata_table_manager.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
    tables: []const metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
) !u64 {
    if (findPlacementIntentStoreId(placements, group_id, local_node_id, stores)) |store_id| {
        try writeStoreAffinityFile(alloc, io, replica_root_dir, group_id, store_id);
        return store_id;
    }
    const existing = try readStoreAffinityFile(alloc, io, replica_root_dir, group_id);
    if (existing) |store_id| {
        if (findProjectedStore(stores, store_id) != null) return store_id;
    }

    const assigned = try assignSharedRootStoreAffinity(alloc, stores, tables, range);
    try writeStoreAffinityFile(alloc, io, replica_root_dir, group_id, assigned);
    return assigned;
}

fn readStoreAffinityFile(
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    group_id: u64,
) !?u64 {
    const path = try std.fmt.allocPrint(alloc, "{s}/group-{d}/store-affinity", .{ replica_root_dir, group_id });
    defer alloc.free(path);
    const contents = std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(128)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(contents);
    const trimmed = std.mem.trim(u8, contents, " \t\r\n");
    if (trimmed.len == 0) return null;
    return std.fmt.parseInt(u64, trimmed, 10) catch null;
}

fn writeStoreAffinityFile(
    alloc: std.mem.Allocator,
    io: std.Io,
    replica_root_dir: []const u8,
    group_id: u64,
    store_id: u64,
) !void {
    const dir_path = try std.fmt.allocPrint(alloc, "{s}/group-{d}", .{ replica_root_dir, group_id });
    defer alloc.free(dir_path);
    try fs_paths.createDirPathPortable(io, dir_path);
    const path = try std.fmt.allocPrint(alloc, "{s}/store-affinity", .{dir_path});
    defer alloc.free(path);
    var file = try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);
    var buf: [64]u8 = undefined;
    var writer = file.writer(io, &buf);
    try writer.interface.print("{d}\n", .{store_id});
    try writer.end();
}

fn assignSharedRootStoreAffinity(
    alloc: std.mem.Allocator,
    stores: []const metadata_table_manager.StoreRecord,
    tables: []const metadata_table_manager.TableRecord,
    range: metadata_table_manager.RangeRecord,
) !u64 {
    var candidates = std.ArrayListUnmanaged(metadata_table_manager.StoreRecord).empty;
    defer candidates.deinit(alloc);

    const table = findTableById(tables, range.table_id);
    if (table) |owned_table| {
        for (stores) |store| {
            if (std.mem.eql(u8, store.role, owned_table.placement_role)) {
                try candidates.append(alloc, store);
            }
        }
    }
    if (candidates.items.len == 0) {
        for (stores) |store| try candidates.append(alloc, store);
    }

    std.mem.sort(metadata_table_manager.StoreRecord, candidates.items, {}, struct {
        fn lessThan(_: void, a: metadata_table_manager.StoreRecord, b: metadata_table_manager.StoreRecord) bool {
            return a.store_id < b.store_id;
        }
    }.lessThan);

    const index = @as(usize, @intCast(range.group_id % candidates.items.len));
    return candidates.items[index].store_id;
}

fn findStoreStatusReportIndex(reports: []const metadata_table_manager.StoreStatusReport, store_id: u64) ?usize {
    for (reports, 0..) |report, i| {
        if (report.store_id == store_id) return i;
    }
    return null;
}

fn findPlacementIntentStoreId(
    placements: []const raft_reconciler.PlacementIntent,
    group_id: u64,
    local_node_id: u64,
    stores: []const metadata_table_manager.StoreRecord,
) ?u64 {
    for (placements) |intent| {
        if (intent.record.group_id != group_id) continue;
        if (intent.record.local_node_id != local_node_id) continue;
        if (intent.store_id == 0) return null;
        if (findProjectedStore(stores, intent.store_id) != null) return intent.store_id;
        return null;
    }
    return null;
}

fn findTableById(
    tables: []const metadata_table_manager.TableRecord,
    table_id: u64,
) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn parseStoreStatusGroupId(path: []const u8) ?u64 {
    const slash = std.mem.indexOfScalar(u8, path, '/') orelse return null;
    const group_dir = path[0..slash];
    if (!std.mem.startsWith(u8, group_dir, "group-")) return null;
    return std.fmt.parseInt(u64, group_dir["group-".len..], 10) catch null;
}

fn findRangeByGroupId(
    ranges: []const metadata_table_manager.RangeRecord,
    group_id: u64,
) ?metadata_table_manager.RangeRecord {
    for (ranges) |range| {
        if (range.group_id == group_id) return range;
    }
    return null;
}

fn containsLocalIntent(intents: []const raft_reconciler.PlacementIntent, group_id: u64) bool {
    for (intents) |intent| {
        if (intent.record.group_id == group_id) return true;
    }
    return false;
}

fn allocPeerNodeIdsExcludingSelf(alloc: std.mem.Allocator, voters: []const u64, local_node_id: u64) ![]u64 {
    var count: usize = 0;
    for (voters) |node_id| {
        if (node_id == local_node_id) continue;
        count += 1;
    }
    if (count == 0) return &.{};

    const peer_node_ids = try alloc.alloc(u64, count);
    var index: usize = 0;
    errdefer alloc.free(peer_node_ids);
    for (voters) |node_id| {
        if (node_id == local_node_id) continue;
        peer_node_ids[index] = node_id;
        index += 1;
    }
    return peer_node_ids;
}

pub const TableLifecycleExpectation = enum {
    present,
    absent,
};

pub const TableProjectionExpectation = struct {
    schema_json: ?[]const u8 = null,
    indexes_json: ?[]const u8 = null,
};

fn lifecycleKeyMatchesMetadataNamespace(metadata_group_id: u64, signal: metadata_storage.raft_apply_store.CommittedKeySignal) bool {
    if (signal.metadata_group_id != metadata_group_id) return false;

    var node_prefix_buf: [96]u8 = undefined;
    const node_prefix = metadata_storage.raft_apply_store.nodePrefixForGroup(&node_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, node_prefix)) return true;

    var table_prefix_buf: [96]u8 = undefined;
    const table_prefix = metadata_storage.raft_apply_store.tablePrefixForGroup(&table_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, table_prefix)) return true;

    var range_prefix_buf: [96]u8 = undefined;
    const range_prefix = metadata_storage.raft_apply_store.rangePrefixForGroup(&range_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, range_prefix)) return true;

    var store_prefix_buf: [96]u8 = undefined;
    const store_prefix = metadata_storage.raft_apply_store.storePrefixForGroup(&store_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, store_prefix)) return true;

    var placement_prefix_buf: [96]u8 = undefined;
    const placement_prefix = metadata_storage.raft_apply_store.placementPrefixForGroup(&placement_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, placement_prefix)) return true;

    var schema_progress_prefix_buf: [128]u8 = undefined;
    const schema_progress_prefix = metadata_storage.raft_apply_store.schemaProgressPrefixForGroup(&schema_progress_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, schema_progress_prefix)) return true;

    var restore_progress_prefix_buf: [128]u8 = undefined;
    const restore_progress_prefix = metadata_storage.raft_apply_store.restoreProgressPrefixForGroup(&restore_progress_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, restore_progress_prefix)) return true;

    var split_transition_prefix_buf: [128]u8 = undefined;
    const split_transition_prefix = metadata_storage.raft_apply_store.splitTransitionPrefixForGroup(&split_transition_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, split_transition_prefix)) return true;

    var merge_transition_prefix_buf: [128]u8 = undefined;
    const merge_transition_prefix = metadata_storage.raft_apply_store.mergeTransitionPrefixForGroup(&merge_transition_prefix_buf, metadata_group_id) catch return false;
    if (std.mem.startsWith(u8, signal.key, merge_transition_prefix)) return true;

    var reconcile_lease_key_buf: [96]u8 = undefined;
    const reconcile_lease_key = metadata_storage.raft_apply_store.reconcileLeaseKeyForGroup(&reconcile_lease_key_buf, metadata_group_id) catch return false;
    if (std.mem.eql(u8, signal.key, reconcile_lease_key)) return true;

    var reallocation_request_key_buf: [96]u8 = undefined;
    const reallocation_request_key = metadata_storage.raft_apply_store.reallocationRequestKeyForGroup(&reallocation_request_key_buf, metadata_group_id) catch return false;
    return std.mem.eql(u8, signal.key, reallocation_request_key);
}

fn waitForTableLifecycleConvergence(
    service: anytype,
    table_name: []const u8,
    expected: TableLifecycleExpectation,
) !void {
    const timeout_ns = 30 * std.time.ns_per_s;
    const poll_interval_ns = 10 * std.time.ns_per_ms;
    const start_ns = platform_time.monotonicNs();

    while (true) {
        var snapshot = try service.adminSnapshot();
        defer service.freeAdminSnapshot(&snapshot);

        if (tableLifecycleMatches(&snapshot, table_name, expected)) return;
        if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
        const remaining_ns = timeout_ns -| (platform_time.monotonicNs() -| start_ns);
        try awaitLifecycleProgress(service, table_name, @min(remaining_ns, poll_interval_ns));
    }
}

fn waitForTableProjectionConvergence(
    service: anytype,
    table_name: []const u8,
    expected: TableProjectionExpectation,
) !void {
    const timeout_ns = 30 * std.time.ns_per_s;
    const poll_interval_ns = 10 * std.time.ns_per_ms;
    const start_ns = platform_time.monotonicNs();

    while (true) {
        var snapshot = try service.adminSnapshot();
        defer service.freeAdminSnapshot(&snapshot);

        if (tableProjectionMatches(&snapshot, table_name, expected)) return;
        if (platform_time.monotonicNs() -| start_ns >= timeout_ns) return error.TableVisibilityTimeout;
        const remaining_ns = timeout_ns -| (platform_time.monotonicNs() -| start_ns);
        try awaitLifecycleProgress(service, table_name, @min(remaining_ns, poll_interval_ns));
    }
}

fn awaitLifecycleProgress(service: anytype, table_name: ?[]const u8, wait_ns: u64) !void {
    const ServiceType = @TypeOf(service);
    const ServiceDeclType = switch (@typeInfo(ServiceType)) {
        .pointer => |pointer| pointer.child,
        else => ServiceType,
    };

    if (@hasDecl(ServiceDeclType, "captureLifecycleSignal") and @hasDecl(ServiceDeclType, "waitForLifecycleSignal")) {
        const observed = service.captureLifecycleSignal(table_name);
        service.waitForLifecycleSignal(observed, wait_ns);
        const current = service.captureLifecycleSignal(table_name);
        if (current.global_epoch != observed.global_epoch or current.table_epoch != observed.table_epoch) return;
    }

    try service.runLifecycleRound();
    if (!(@hasDecl(ServiceDeclType, "captureLifecycleSignal") and @hasDecl(ServiceDeclType, "waitForLifecycleSignal"))) {
        platform_clock.Clock.real().sleepMs(@max(@as(u64, 1), @divFloor(wait_ns + std.time.ns_per_ms - 1, std.time.ns_per_ms)));
    }
}

fn tableLifecycleMatches(
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    expected: TableLifecycleExpectation,
) bool {
    const table = findProjectedTableByName(snapshot.tables, table_name);
    return switch (expected) {
        .present => {
            const record = table orelse return false;
            return tableRangesReady(snapshot, record.table_id);
        },
        .absent => table == null,
    };
}

fn tableProjectionMatches(
    snapshot: *const metadata_api.AdminSnapshot,
    table_name: []const u8,
    expected: TableProjectionExpectation,
) bool {
    const record = findProjectedTableByName(snapshot.tables, table_name) orelse return false;
    if (expected.schema_json) |schema_json| {
        const matches = jsonDocumentsEqual(std.heap.page_allocator, record.schema_json, schema_json) catch return false;
        if (!matches) return false;
    }
    if (expected.indexes_json) |indexes_json| {
        const matches = jsonDocumentsEqual(std.heap.page_allocator, record.indexes_json, indexes_json) catch return false;
        if (!matches) return false;
    }
    return true;
}

fn jsonDocumentsEqual(
    alloc: std.mem.Allocator,
    lhs_json: []const u8,
    rhs_json: []const u8,
) !bool {
    if (std.mem.eql(u8, lhs_json, rhs_json)) return true;

    var lhs = try std.json.parseFromSlice(std.json.Value, alloc, lhs_json, .{});
    defer lhs.deinit();
    var rhs = try std.json.parseFromSlice(std.json.Value, alloc, rhs_json, .{});
    defer rhs.deinit();
    return jsonValuesEqual(lhs.value, rhs.value);
}

fn jsonValuesEqual(lhs: std.json.Value, rhs: std.json.Value) bool {
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return false;

    return switch (lhs) {
        .null => true,
        .bool => |lhs_bool| lhs_bool == rhs.bool,
        .integer => |lhs_int| lhs_int == rhs.integer,
        .float => |lhs_float| lhs_float == rhs.float,
        .number_string => |lhs_number| std.mem.eql(u8, lhs_number, rhs.number_string),
        .string => |lhs_string| std.mem.eql(u8, lhs_string, rhs.string),
        .array => |lhs_array| blk: {
            if (lhs_array.items.len != rhs.array.items.len) break :blk false;
            for (lhs_array.items, rhs.array.items) |lhs_item, rhs_item| {
                if (!jsonValuesEqual(lhs_item, rhs_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |lhs_object| blk: {
            if (lhs_object.count() != rhs.object.count()) break :blk false;
            var it = lhs_object.iterator();
            while (it.next()) |entry| {
                const rhs_value = rhs.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValuesEqual(entry.value_ptr.*, rhs_value)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn findProjectedTableByName(
    tables: []const metadata_table_manager.TableRecord,
    table_name: []const u8,
) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (std.mem.eql(u8, table.name, table_name)) return table;
    }
    return null;
}

fn findProjectedTableById(
    tables: []const metadata_table_manager.TableRecord,
    table_id: u64,
) ?metadata_table_manager.TableRecord {
    for (tables) |table| {
        if (table.table_id == table_id) return table;
    }
    return null;
}

fn tableRangesReady(snapshot: *const metadata_api.AdminSnapshot, table_id: u64) bool {
    var range_count: usize = 0;
    for (snapshot.ranges) |range| {
        if (range.table_id != table_id) continue;
        range_count += 1;
        if (!groupReadyForTableLifecycle(snapshot, range.group_id)) return false;
    }
    return range_count > 0;
}

fn groupReadyForTableLifecycle(snapshot: *const metadata_api.AdminSnapshot, group_id: u64) bool {
    const status = findMergedGroupStatus(snapshot.merged_group_statuses, group_id) orelse return false;
    if (status.updated_at_millis == 0) return false;
    if (status.joint_consensus) return false;
    if (status.transition_pending) return false;
    if (status.replay_required and !status.replay_caught_up) return false;
    if (status.restore_pending) return false;
    return groupHasExpectedHealthyPlacement(snapshot, group_id, status);
}

fn findMergedGroupStatus(
    statuses: []const metadata_reconciler.MergedGroupStatus,
    group_id: u64,
) ?metadata_reconciler.MergedGroupStatus {
    for (statuses) |status| {
        if (status.group_id == group_id) return status;
    }
    return null;
}

fn transitionStatusStablePlacementReadiness(
    status: metadata_reconciler.MergedGroupStatus,
    expected_voters: u32,
    leader_placed: bool,
) transition_state.StablePlacementReadiness {
    if (expected_voters == 0 or expected_voters > std.math.maxInt(u16))
        return .invalid_expected_voter_count;
    const expected_voter_count: u16 = @intCast(expected_voters);
    if (!status.leader_known) return .leader_unknown;
    if (status.leader_store_id == 0) return .leader_store_unknown;
    if (!leader_placed) return .leader_not_placed;
    if (!status.voter_count_known) return .voter_count_unknown;
    if (status.voter_count != expected_voter_count) return .voter_count_mismatch;
    if (status.healthy_voter_reports < status.voter_count) return .insufficient_healthy_voters;
    if (status.joint_consensus) return .joint_consensus;
    return .ready;
}

fn buildTransitionReadinessMap(
    alloc: std.mem.Allocator,
    stores: []const metadata_table_manager.StoreRecord,
    placements: []const raft_reconciler.PlacementIntent,
) !std.AutoHashMapUnmanaged(u64, transition_state.StablePlacementReadiness) {
    const merged = try metadata_state.mergeHealthyGroupStatuses(
        alloc,
        &.{},
        &.{},
        placements,
        &.{},
        stores,
        &.{},
        &.{},
        &.{},
        &.{},
    );
    defer alloc.free(merged);

    var placement_counts = std.AutoHashMapUnmanaged(u64, u32).empty;
    defer placement_counts.deinit(alloc);
    var placement_stores = std.AutoHashMapUnmanaged(TransitionPlacementKey, void).empty;
    defer placement_stores.deinit(alloc);
    try placement_counts.ensureTotalCapacity(alloc, @intCast(placements.len));
    try placement_stores.ensureTotalCapacity(alloc, @intCast(placements.len));
    for (placements) |intent| {
        const count = placement_counts.getOrPutAssumeCapacity(intent.record.group_id);
        if (!count.found_existing) count.value_ptr.* = 0;
        count.value_ptr.* +|= 1;
        placement_stores.putAssumeCapacity(.{
            .group_id = intent.record.group_id,
            .store_id = intent.store_id,
        }, {});
    }

    var ready_by_group = std.AutoHashMapUnmanaged(u64, transition_state.StablePlacementReadiness).empty;
    errdefer ready_by_group.deinit(alloc);
    try ready_by_group.ensureTotalCapacity(alloc, @intCast(merged.len));
    for (merged) |status| {
        ready_by_group.putAssumeCapacity(
            status.group_id,
            transitionStatusStablePlacementReadiness(
                status,
                placement_counts.get(status.group_id) orelse 0,
                placement_stores.contains(.{
                    .group_id = status.group_id,
                    .store_id = status.leader_store_id,
                }),
            ),
        );
    }
    return ready_by_group;
}

test "metadata service transition readiness requires stable healthy placement" {
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .store_id = 1, .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 1 } },
        .{ .store_id = 2, .record = .{ .group_id = 77, .replica_id = 2, .local_node_id = 2 } },
        .{ .store_id = 3, .record = .{ .group_id = 77, .replica_id = 3, .local_node_id = 3 } },
    };
    var status = metadata_reconciler.MergedGroupStatus{
        .group_id = 77,
        .leader_known = true,
        .leader_store_id = 1,
        .voter_count_known = true,
        .voter_count = 3,
        .healthy_voter_reports = 3,
    };
    try std.testing.expectEqual(
        transition_state.StablePlacementReadiness.ready,
        transitionStatusStablePlacementReadiness(status, placements.len, true),
    );
    status.healthy_voter_reports = 2;
    try std.testing.expectEqual(
        transition_state.StablePlacementReadiness.insufficient_healthy_voters,
        transitionStatusStablePlacementReadiness(status, placements.len, true),
    );
    status.healthy_voter_reports = 3;
    status.joint_consensus = true;
    try std.testing.expectEqual(
        transition_state.StablePlacementReadiness.joint_consensus,
        transitionStatusStablePlacementReadiness(status, placements.len, true),
    );
    status.joint_consensus = false;
    status.leader_store_id = 99;
    try std.testing.expectEqual(
        transition_state.StablePlacementReadiness.leader_not_placed,
        transitionStatusStablePlacementReadiness(status, placements.len, false),
    );
}

test "metadata service transition readiness fences each member relocation generation independently" {
    const voter_set_fingerprint = metadata_table_manager.voterSetFingerprint(&.{ 1, 2, 3 }, null);
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .store_id = 1, .record = .{ .group_id = 77, .replica_id = 1, .local_node_id = 1 }, .relocation_generation = 4 },
        .{ .store_id = 2, .record = .{ .group_id = 77, .replica_id = 2, .local_node_id = 2 }, .relocation_generation = 4 },
        .{ .store_id = 3, .record = .{ .group_id = 77, .replica_id = 3, .local_node_id = 3 }, .relocation_generation = 4 },
    };
    var leader_statuses = [_]metadata_table_manager.GroupStatusReport{
        .{
            .group_id = 77,
            .local_leader = true,
            .local_voter = true,
            .voter_set_known = true,
            .voter_set_fingerprint = voter_set_fingerprint,
            .voter_count = 3,
            .relocation_generation = 4,
        },
    };
    var follower_statuses = [_]metadata_table_manager.GroupStatusReport{
        .{
            .group_id = 77,
            .local_voter = true,
            .voter_set_known = true,
            .voter_set_fingerprint = voter_set_fingerprint,
            .voter_count = 3,
            .relocation_generation = 4,
        },
    };
    var stores = [_]metadata_table_manager.StoreRecord{
        .{ .store_id = 1, .node_id = 1, .live = true, .health_class = "healthy", .group_statuses = &leader_statuses },
        .{ .store_id = 2, .node_id = 2, .live = true, .health_class = "healthy", .group_statuses = &follower_statuses },
        .{ .store_id = 3, .node_id = 3, .live = true, .health_class = "healthy", .group_statuses = &follower_statuses },
    };

    var ready = try buildTransitionReadinessMap(std.testing.allocator, &stores, &placements);
    defer ready.deinit(std.testing.allocator);
    try std.testing.expectEqual(
        transition_state.StablePlacementReadiness.ready,
        ready.get(77) orelse .status_unavailable,
    );

    var relocating_placements = placements;
    relocating_placements[2].relocation_generation = 5;
    relocating_placements[2].serving_state = .draining;
    var relocating_follower_statuses = follower_statuses;
    relocating_follower_statuses[0].relocation_generation = 5;
    var relocating_stores = stores;
    relocating_stores[2].group_statuses = &relocating_follower_statuses;
    var relocating = try buildTransitionReadinessMap(
        std.testing.allocator,
        &relocating_stores,
        &relocating_placements,
    );
    defer relocating.deinit(std.testing.allocator);
    try std.testing.expectEqual(
        transition_state.StablePlacementReadiness.ready,
        relocating.get(77) orelse .status_unavailable,
    );

    var stale_leader_statuses = leader_statuses;
    stale_leader_statuses[0].relocation_generation = 3;
    var stale_follower_statuses = follower_statuses;
    stale_follower_statuses[0].relocation_generation = 3;
    var stale_stores = stores;
    stale_stores[0].group_statuses = &stale_leader_statuses;
    stale_stores[1].group_statuses = &stale_follower_statuses;
    stale_stores[2].group_statuses = &stale_follower_statuses;
    var stale = try buildTransitionReadinessMap(std.testing.allocator, &stale_stores, &placements);
    defer stale.deinit(std.testing.allocator);
    try std.testing.expectEqual(
        transition_state.StablePlacementReadiness.status_unavailable,
        stale.get(77) orelse .status_unavailable,
    );
}

fn groupHasExpectedHealthyPlacement(
    snapshot: *const metadata_api.AdminSnapshot,
    group_id: u64,
    status: metadata_reconciler.MergedGroupStatus,
) bool {
    const expected = countPlacementIntentsForGroup(snapshot.placement_intents, group_id);
    if (status.voter_count_known) {
        if (expected > 0 and status.voter_count != expected) return false;
        return status.healthy_voter_reports >= status.voter_count;
    }
    if (expected == 0) return true;
    return countHealthyStoresReportingGroup(snapshot.stores, group_id) >= expected;
}

fn countPlacementIntentsForGroup(
    intents: []const raft_reconciler.PlacementIntent,
    group_id: u64,
) u16 {
    var count: u16 = 0;
    for (intents) |intent| {
        if (intent.record.group_id == group_id) count +|= 1;
    }
    return count;
}

fn countHealthyStoresReportingGroup(
    stores: []const metadata_table_manager.StoreRecord,
    group_id: u64,
) usize {
    var count: usize = 0;
    for (stores) |store| {
        if (!store.live) continue;
        if (!std.mem.eql(u8, store.health_class, "healthy")) continue;
        for (store.group_statuses) |group_status| {
            if (group_status.group_id != group_id) continue;
            count += 1;
            break;
        }
    }
    return count;
}

fn runServiceRounds(svc: *MetadataService, count: usize) !void {
    try runServiceRoundsUntilMetadataReady(svc);
    var rounds: usize = 0;
    while (rounds < count) : (rounds += 1) try svc.runRound();
}

fn runServiceRoundsUntilMetadataReady(svc: *MetadataService) !void {
    var rounds: usize = 0;
    while (rounds < 32) : (rounds += 1) {
        if (try svc.metadataIncarnation() != null) return;
        try svc.runRound();
    }
    return error.MetadataIncarnationUnavailable;
}

fn logServiceWaitTimeout(svc: *MetadataService, label: []const u8, group_id: u64, desired: raft_host.HostedReplicaStatus, rounds: usize) !void {
    const metadata_status = try svc.status();
    std.log.warn(
        "metadata test wait timed out label={s} group_id={} desired={} actual={} rounds={} local_leader={} lease_held={} lease_owner={}",
        .{
            label,
            group_id,
            desired,
            svc.raft.host.host.status(group_id),
            rounds,
            svc.raft.host.host.isLocalLeader(svc.metadata_group_id),
            metadata_status.reconcile_lease_held_by_local,
            metadata_status.reconcile_lease_owner_node_id,
        },
    );
}

fn runServiceRoundsUntilHostedStatus(
    svc: *MetadataService,
    group_id: u64,
    desired: raft_host.HostedReplicaStatus,
    max_rounds: usize,
    label: []const u8,
) !void {
    var rounds: usize = 0;
    while (rounds < max_rounds) : (rounds += 1) {
        if (svc.raft.host.host.status(group_id) == desired) return;
        try svc.runRound();
    }
    try logServiceWaitTimeout(svc, label, group_id, desired, rounds);
    return error.TestExpectedEqual;
}

fn findProjectedSplit(records: []const transition_state.SplitTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findProjectedMerge(records: []const transition_state.MergeTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findProjectedStore(records: []const metadata_table_manager.StoreRecord, store_id: u64) ?metadata_table_manager.StoreRecord {
    for (records) |record| {
        if (record.store_id == store_id) return record;
    }
    return null;
}

fn findQueuedSplit(records: []const transition_state.SplitTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn findQueuedMerge(records: []const transition_state.MergeTransitionRecord, transition_id: u64) ?usize {
    for (records, 0..) |record, i| {
        if (record.transition_id == transition_id) return i;
    }
    return null;
}

fn projectedProvisioningFingerprint(alloc: std.mem.Allocator, service: anytype) !u64 {
    var hasher = std.hash.Wyhash.init(0x8f6b4f5a2c13d9e1);

    const tables = try service.listProjectedTables(alloc);
    defer service.freeProjectedTables(alloc, tables);
    for (tables) |table| {
        hasher.update(std.mem.asBytes(&table.table_id));
        hashProjectedProvisioningBytes(&hasher, table.name);
        hashProjectedProvisioningBytes(&hasher, table.schema_json);
        hashProjectedProvisioningBytes(&hasher, table.read_schema_json);
        hashProjectedProvisioningBytes(&hasher, table.indexes_json);
        hashProjectedProvisioningBytes(&hasher, table.replication_sources_json);
        hashProjectedProvisioningBytes(&hasher, table.placement_role);
        hashProjectedProvisioningBytes(&hasher, table.restore_backup_id);
        hashProjectedProvisioningBytes(&hasher, table.restore_location);
        hasher.update(std.mem.asBytes(&table.desired_replica_count));
        hasher.update(std.mem.asBytes(&table.min_ranges));
    }

    const ranges = try service.listProjectedRanges(alloc);
    defer service.freeProjectedRanges(alloc, ranges);
    for (ranges) |range| {
        hasher.update(std.mem.asBytes(&range.group_id));
        hasher.update(std.mem.asBytes(&range.range_id));
        hasher.update(std.mem.asBytes(&range.table_id));
        hasher.update(std.mem.asBytes(&range.doc_identity_shard_id));
        hasher.update(std.mem.asBytes(&range.doc_identity_range_id));
        hasher.update(std.mem.asBytes(&range.split_attempt_epoch));
        hashProjectedProvisioningBytes(&hasher, range.start_key);
        if (range.end_key) |end_key| {
            hasher.update(&.{1});
            hashProjectedProvisioningBytes(&hasher, end_key);
        } else {
            hasher.update(&.{0});
        }
        hashProjectedProvisioningBytes(&hasher, range.restore_backup_id);
        hashProjectedProvisioningBytes(&hasher, range.restore_artifact_backup_id);
        hashProjectedProvisioningBytes(&hasher, range.restore_location);
        hashProjectedProvisioningBytes(&hasher, range.restore_snapshot_path);
        hashProjectedProvisioningBytes(&hasher, range.restore_connection);
        hasher.update(std.mem.asBytes(&range.restore_artifact_size_bytes));
        hashProjectedProvisioningBytes(&hasher, range.restore_artifact_sha256);
        hasher.update(&range.completed_restore_fingerprint);
    }

    const placements = try service.listProjectedPlacementIntents(alloc);
    defer service.freeProjectedPlacementIntents(alloc, placements);
    for (placements) |intent| {
        hasher.update(std.mem.asBytes(&intent.record.group_id));
        hasher.update(std.mem.asBytes(&intent.record.replica_id));
        hasher.update(std.mem.asBytes(&intent.record.local_node_id));
        hasher.update(std.mem.asBytes(&@intFromEnum(intent.record.bootstrap_mode)));
        hasher.update(std.mem.asBytes(&intent.record.metadata_version));
        hasher.update(std.mem.asBytes(&intent.store_id));
        hasher.update(std.mem.asBytes(&@as(u64, intent.peer_node_ids.len)));
        for (intent.peer_node_ids) |peer_node_id| hasher.update(std.mem.asBytes(&peer_node_id));
        if (intent.record.snapshot_bootstrap) |snapshot| {
            hasher.update(&.{1});
            hasher.update(std.mem.asBytes(&snapshot.from_node_id));
            hasher.update(std.mem.asBytes(&snapshot.term));
            hashProjectedProvisioningBytes(&hasher, snapshot.snapshot_id);
            hashProjectedProvisioningBytes(&hasher, snapshot.uri);
        } else {
            hasher.update(&.{0});
        }
        if (intent.record.backup_restore_bootstrap) |restore| {
            hasher.update(&.{1});
            hashProjectedProvisioningBytes(&hasher, restore.backup_id);
            hashProjectedProvisioningBytes(&hasher, restore.location);
            hashProjectedProvisioningBytes(&hasher, restore.snapshot_path);
            hashProjectedProvisioningBytes(&hasher, restore.connection);
            hasher.update(std.mem.asBytes(&restore.artifact_size_bytes));
            hashProjectedProvisioningBytes(&hasher, restore.artifact_sha256);
        } else {
            hasher.update(&.{0});
        }
    }

    return hasher.final();
}

fn hashProjectedProvisioningBytes(hasher: *std.hash.Wyhash, bytes: []const u8) void {
    const len: u64 = @intCast(bytes.len);
    hasher.update(std.mem.asBytes(&len));
    hasher.update(bytes);
}

pub fn snapshotStatus(
    alloc: std.mem.Allocator,
    metadata_group_id: u64,
    service: anytype,
    metrics: raft_service.ManagedServiceMetrics,
) !MetadataStatus {
    return try snapshotStatusWithOptions(alloc, metadata_group_id, service, metrics, .{});
}

const SnapshotStatusOptions = struct {
    include_reconciliation_planning: bool = false,
};

pub fn snapshotStatusWithOptions(
    alloc: std.mem.Allocator,
    metadata_group_id: u64,
    service: anytype,
    metrics: raft_service.ManagedServiceMetrics,
    options: SnapshotStatusOptions,
) !MetadataStatus {
    const SourceType = @TypeOf(service);
    const SourceDeclType = switch (@typeInfo(SourceType)) {
        .pointer => |pointer| pointer.child,
        else => SourceType,
    };
    const PlanningSummary = struct {
        placement_upserts: usize = 0,
        placement_removals: usize = 0,
        repair_placement_groups: usize = 0,
        rebalance_placement_groups: usize = 0,
    };
    const now_ms = realtimeNowMillis();
    const lease_stats = if (@hasDecl(SourceDeclType, "reconcileLeaseStats"))
        service.reconcileLeaseStats()
    else
        metadata_reconcile_lease.Stats{};
    const projected_reconcile_lease = if (@hasDecl(SourceDeclType, "statusProjectedReconcileLease"))
        service.statusProjectedReconcileLease(now_ms) catch null
    else if (@hasDecl(SourceDeclType, "getProjectedReconcileLease"))
        service.getProjectedReconcileLease() catch null
    else
        null;
    const metadata_incarnation = if (@hasDecl(SourceDeclType, "metadataIncarnation"))
        try service.metadataIncarnation()
    else
        null;
    const projected_tables = try service.listProjectedTables(alloc);
    defer service.freeProjectedTables(alloc, projected_tables);
    const projected_ranges = try service.listProjectedRanges(alloc);
    defer service.freeProjectedRanges(alloc, projected_ranges);
    const projected_stores = try service.listProjectedStores(alloc);
    defer service.freeProjectedStores(alloc, projected_stores);
    const projected_placement_intents = try service.listProjectedPlacementIntents(alloc);
    defer service.freeProjectedPlacementIntents(alloc, projected_placement_intents);
    const projected_shuffle_join_leases = if (@hasDecl(SourceDeclType, "listProjectedShuffleJoinLeases"))
        try service.listProjectedShuffleJoinLeases(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedShuffleJoinLeases") and projected_shuffle_join_leases.len > 0) {
        service.freeProjectedShuffleJoinLeases(alloc, projected_shuffle_join_leases);
    };
    const projected_restore_progress = if (@hasDecl(SourceDeclType, "listProjectedRestoreProgress"))
        try service.listProjectedRestoreProgress(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedRestoreProgress") and projected_restore_progress.len > 0) {
        service.freeProjectedRestoreProgress(alloc, projected_restore_progress);
    };
    const projected_replication_source_statuses = if (@hasDecl(SourceDeclType, "listProjectedReplicationSourceStatuses"))
        try service.listProjectedReplicationSourceStatuses(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedReplicationSourceStatuses") and projected_replication_source_statuses.len > 0) {
        service.freeProjectedReplicationSourceStatuses(alloc, projected_replication_source_statuses);
    };
    const projected_extension_packages = if (@hasDecl(SourceDeclType, "listProjectedExtensionPackages"))
        try service.listProjectedExtensionPackages(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedExtensionPackages") and projected_extension_packages.len > 0) {
        service.freeProjectedExtensionPackages(alloc, projected_extension_packages);
    };
    const projected_installed_extensions = if (@hasDecl(SourceDeclType, "listProjectedInstalledExtensions"))
        try service.listProjectedInstalledExtensions(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedInstalledExtensions") and projected_installed_extensions.len > 0) {
        service.freeProjectedInstalledExtensions(alloc, projected_installed_extensions);
    };
    const projected_extension_members = if (@hasDecl(SourceDeclType, "listProjectedExtensionMembers"))
        try service.listProjectedExtensionMembers(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedExtensionMembers") and projected_extension_members.len > 0) {
        service.freeProjectedExtensionMembers(alloc, projected_extension_members);
    };
    const projected_extension_dependencies = if (@hasDecl(SourceDeclType, "listProjectedExtensionDependencies"))
        try service.listProjectedExtensionDependencies(alloc)
    else
        &.{};
    defer if (@hasDecl(SourceDeclType, "freeProjectedExtensionDependencies") and projected_extension_dependencies.len > 0) {
        service.freeProjectedExtensionDependencies(alloc, projected_extension_dependencies);
    };
    const projected_split_transitions = try service.listProjectedSplitTransitions(alloc);
    defer service.freeProjectedSplitTransitions(alloc, projected_split_transitions);
    const projected_merge_transitions = try service.listProjectedMergeTransitions(alloc);
    defer service.freeProjectedMergeTransitions(alloc, projected_merge_transitions);
    const metadata_raft = serviceGroupRaftObservation(service, metadata_group_id);

    var preferred_stores: usize = 0;
    var constrained_stores: usize = 0;
    var overloaded_stores: usize = 0;
    var excluded_stores: usize = 0;
    var backfill_stores: usize = 0;
    var active_backfills: usize = 0;
    var projected_tables_with_replication_sources: usize = 0;
    var projected_replication_sources: usize = 0;
    var projected_replication_source_statuses_exact_cutover: usize = 0;
    var projected_replication_source_statuses_non_exact_cutover: usize = 0;
    var projected_replication_source_statuses_reseed_recommended: usize = 0;
    var projected_replication_source_statuses_exported_snapshot: usize = 0;
    var projected_replication_source_statuses_slot_first: usize = 0;
    var projected_replication_source_statuses_slot_resumed: usize = 0;
    var projected_replication_source_statuses_snapshot: usize = 0;
    var projected_replication_source_statuses_cutover_prepared: usize = 0;
    var projected_replication_source_statuses_streaming: usize = 0;
    var projected_replication_source_statuses_failed: usize = 0;
    var projected_replication_source_statuses_with_last_error: usize = 0;
    var projected_replication_source_statuses_slot_missing_failed: usize = 0;
    var projected_replication_source_statuses_retryable_failed: usize = 0;
    var projected_replication_source_statuses_terminal_failed: usize = 0;
    var projected_replication_source_statuses_with_consecutive_failures: usize = 0;
    var projected_replication_source_statuses_with_success_timestamp: usize = 0;
    var projected_replication_source_statuses_with_change_timestamp: usize = 0;
    var projected_replication_source_consecutive_failures_total: u64 = 0;
    var projected_replication_source_consecutive_failures_max: u64 = 0;
    var projected_replication_source_lag_records_total: u64 = 0;
    var projected_replication_source_lag_records_max: u64 = 0;
    var projected_replication_source_lag_millis_total: u64 = 0;
    var projected_replication_source_lag_millis_max: u64 = 0;
    var projected_replication_source_observed_lag_millis_total: u64 = 0;
    var projected_replication_source_observed_lag_millis_max: u64 = 0;
    var projected_replication_source_statuses_with_source_commit_timestamp: usize = 0;
    var projected_replication_source_last_success_at_ms_max: u64 = 0;
    var projected_replication_source_last_source_commit_at_ms_max: u64 = 0;
    var projected_replication_source_last_change_applied_at_ms_max: u64 = 0;
    var projected_snapshot_bootstrap_intents: usize = 0;
    var projected_backup_restore_bootstrap_intents: usize = 0;
    for (projected_tables) |table| {
        const source_count = countReplicationSourcesJson(alloc, table.replication_sources_json) catch 0;
        if (source_count > 0) {
            projected_tables_with_replication_sources += 1;
            projected_replication_sources += source_count;
        }
    }
    for (projected_stores) |record| {
        if (record.active_backfills > 0) backfill_stores += 1;
        active_backfills += record.active_backfills;
        switch (metadata_store_observer.classifyStore(record).tag) {
            .preferred => preferred_stores += 1,
            .constrained => constrained_stores += 1,
            .overloaded => overloaded_stores += 1,
            .excluded => excluded_stores += 1,
        }
    }
    for (projected_placement_intents) |intent| {
        if (intent.record.snapshot_bootstrap != null) projected_snapshot_bootstrap_intents += 1;
        if (intent.record.backup_restore_bootstrap != null) projected_backup_restore_bootstrap_intents += 1;
    }
    for (projected_replication_source_statuses) |status| {
        if (std.mem.eql(u8, status.cutover_mode, "exported_snapshot")) {
            projected_replication_source_statuses_exact_cutover += 1;
            projected_replication_source_statuses_exported_snapshot += 1;
        } else if (std.mem.eql(u8, status.cutover_mode, "slot_first")) {
            projected_replication_source_statuses_non_exact_cutover += 1;
            projected_replication_source_statuses_slot_first += 1;
        } else if (std.mem.eql(u8, status.cutover_mode, "slot_resumed")) {
            projected_replication_source_statuses_non_exact_cutover += 1;
            projected_replication_source_statuses_slot_resumed += 1;
        }
        if (std.mem.eql(u8, status.source_kind, "postgres") and (std.mem.eql(u8, status.cutover_mode, "slot_resumed") or std.mem.eql(u8, status.last_error, "ReplicationExactCutoverRequired"))) {
            projected_replication_source_statuses_reseed_recommended += 1;
        }
        if (std.mem.eql(u8, status.phase, "snapshot")) {
            projected_replication_source_statuses_snapshot += 1;
        } else if (std.mem.eql(u8, status.phase, "cutover_prepared")) {
            projected_replication_source_statuses_cutover_prepared += 1;
        } else if (std.mem.eql(u8, status.phase, "streaming")) {
            projected_replication_source_statuses_streaming += 1;
        } else if (std.mem.eql(u8, status.phase, "failed") or std.mem.endsWith(u8, status.phase, "_failed")) {
            projected_replication_source_statuses_failed += 1;
            if (std.mem.eql(u8, status.failure_class, "terminal")) {
                projected_replication_source_statuses_terminal_failed += 1;
            } else {
                projected_replication_source_statuses_retryable_failed += 1;
            }
        }
        if (status.last_error.len > 0) {
            projected_replication_source_statuses_with_last_error += 1;
            if (std.mem.eql(u8, status.last_error, "ForeignReplicationSlotMissing")) {
                projected_replication_source_statuses_slot_missing_failed += 1;
            }
        }
        if (status.consecutive_failures > 0) projected_replication_source_statuses_with_consecutive_failures += 1;
        if (status.last_success_at_ms > 0) projected_replication_source_statuses_with_success_timestamp += 1;
        if (status.last_change_applied_at_ms > 0) projected_replication_source_statuses_with_change_timestamp += 1;
        projected_replication_source_consecutive_failures_total +%= status.consecutive_failures;
        projected_replication_source_consecutive_failures_max = @max(
            projected_replication_source_consecutive_failures_max,
            status.consecutive_failures,
        );
        projected_replication_source_lag_records_total +%= status.lag_records;
        projected_replication_source_lag_records_max = @max(projected_replication_source_lag_records_max, status.lag_records);
        projected_replication_source_lag_millis_total +%= status.lag_millis;
        projected_replication_source_lag_millis_max = @max(projected_replication_source_lag_millis_max, status.lag_millis);
        const observed_lag_millis: u64 = if (status.last_source_commit_at_ms > 0 and now_ms > status.last_source_commit_at_ms)
            @max(status.lag_millis, now_ms - status.last_source_commit_at_ms)
        else
            status.lag_millis;
        projected_replication_source_observed_lag_millis_total +%= observed_lag_millis;
        projected_replication_source_observed_lag_millis_max = @max(projected_replication_source_observed_lag_millis_max, observed_lag_millis);
        if (status.last_source_commit_at_ms > 0) projected_replication_source_statuses_with_source_commit_timestamp += 1;
        projected_replication_source_last_success_at_ms_max = @max(
            projected_replication_source_last_success_at_ms_max,
            status.last_success_at_ms,
        );
        projected_replication_source_last_source_commit_at_ms_max = @max(
            projected_replication_source_last_source_commit_at_ms_max,
            status.last_source_commit_at_ms,
        );
        projected_replication_source_last_change_applied_at_ms_max = @max(
            projected_replication_source_last_change_applied_at_ms_max,
            status.last_change_applied_at_ms,
        );
    }

    var doc_identity_lifecycle_unknown: usize = 0;
    var doc_identity_lifecycle_preserving: usize = 0;
    var doc_identity_lifecycle_reassigning: usize = 0;
    var doc_identity_lifecycle_rebuild_required: usize = 0;
    var doc_identity_lifecycle_ready: usize = 0;
    var planning: PlanningSummary = .{};
    if (options.include_reconciliation_planning) {
        var status_state = metadata_state.MetadataState.init(alloc);
        defer status_state.deinit();
        try status_state.syncProjected(service);
        try status_state.seedDesiredFromProjected();
        var current = try status_state.captureCurrent(service);
        defer current.deinit(alloc);
        var reconciler = metadata_reconciler.Reconciler.init(alloc);
        defer reconciler.deinit();
        var plan = try reconciler.computePlan(
            status_state.tableManager(),
            status_state.placementCandidates(),
            status_state.placementCandidateInfo(),
            current.current,
        );
        defer plan.deinit(alloc);
        planning = .{
            .placement_upserts = plan.placement_upserts.len,
            .placement_removals = plan.placement_removals.len,
            .repair_placement_groups = plan.repair_placement_groups,
            .rebalance_placement_groups = plan.rebalance_placement_groups,
        };
        for (current.current.merged_group_statuses) |merged_status| {
            const lifecycle = metadata_reconciler.deriveDocIdentityLifecycle(merged_status);
            if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_unknown)) {
                doc_identity_lifecycle_unknown += 1;
            } else if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_preserving)) {
                doc_identity_lifecycle_preserving += 1;
            } else if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_reassigning)) {
                doc_identity_lifecycle_reassigning += 1;
            } else if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_rebuild_required)) {
                doc_identity_lifecycle_rebuild_required += 1;
            } else if (std.mem.eql(u8, lifecycle, metadata_reconciler.doc_identity_lifecycle_ready)) {
                doc_identity_lifecycle_ready += 1;
            }
        }
    }

    return .{
        .metadata_group_id = metadata_group_id,
        .reallocation_barrier_protocol_version = metadata_reallocation_request.barrier_protocol_version,
        .runtime_status_record_version = metadata_runtime_status_protocol.current_record_version,
        .metadata_incarnation = metadata_incarnation,
        .metadata_raft_local_node_id = metadata_raft.local_node_id,
        .metadata_raft_role = metadata_raft.role,
        .metadata_raft_leader_id = metadata_raft.leader_id,
        .metadata_raft_term = metadata_raft.term,
        .metadata_raft_commit_index = metadata_raft.commit_index,
        .metadata_raft_local_voter = metadata_raft.local_voter,
        .metadata_raft_voter_count = metadata_raft.voter_count,
        .metadata_raft_voter_set_fingerprint = metadata_raft.voter_set_fingerprint,
        .metadata_raft_joint_consensus = metadata_raft.joint_consensus,
        .metadata_raft_learner_count = metadata_raft.learner_count,
        .metadata_raft_election_elapsed = metadata_raft.election_elapsed,
        .metadata_raft_randomized_election_timeout = metadata_raft.randomized_election_timeout,
        .metadata_raft_votes_granted = metadata_raft.votes_granted,
        .metadata_raft_votes_rejected = metadata_raft.votes_rejected,
        .metadata_raft_votes_unknown = metadata_raft.votes_unknown,
        .metadata_raft_inbound_message_enqueues = metadata_raft.inbound_message_enqueues,
        .metadata_raft_inbound_message_drains = metadata_raft.inbound_message_drains,
        .metadata_raft_pending_inbound_messages = metadata_raft.pending_inbound_messages,
        .metadata_raft_transport_sent_frames = metadata_raft.transport_sent_frames,
        .metadata_raft_transport_send_failures = metadata_raft.transport_send_failures,
        .metadata_raft_transport_retries_scheduled = metadata_raft.transport_retries_scheduled,
        .metadata_raft_transport_retries_exhausted = metadata_raft.transport_retries_exhausted,
        .metadata_raft_transport_retried_successes = metadata_raft.transport_retried_successes,
        .metadata_raft_transport_peer_refreshes = metadata_raft.transport_peer_refreshes,
        .metadata_raft_transport_peer_routes = metadata_raft.transport_peer_routes,
        .metadata_raft_transport_served_groups = metadata_raft.transport_served_groups,
        .metadata_raft_transport_pending_retries = metadata_raft.transport_pending_retries,
        .metrics = metrics,
        .reconcile_lease_enabled = lease_stats.enabled,
        .reconcile_lease_owner_node_id = if (projected_reconcile_lease) |record| record.owner_node_id else lease_stats.owner_node_id,
        .reconcile_lease_expires_at_ms = if (projected_reconcile_lease) |record| record.expires_at_ms else lease_stats.expires_at_ms,
        .reconcile_lease_held_by_local = lease_stats.held_by_local,
        .reconcile_lease_acquisition_count = lease_stats.acquisition_count,
        .reconcile_lease_acquire_failures = lease_stats.acquire_failures,
        .reconcile_lease_lost_leases = lease_stats.lost_leases,
        .reconcile_lease_last_acquired_ms = lease_stats.last_acquired_ms,
        .projected_tables = projected_tables.len,
        .projected_tables_with_replication_sources = projected_tables_with_replication_sources,
        .projected_replication_sources = projected_replication_sources,
        .projected_replication_source_statuses = projected_replication_source_statuses.len,
        .projected_replication_source_statuses_exact_cutover = projected_replication_source_statuses_exact_cutover,
        .projected_replication_source_statuses_non_exact_cutover = projected_replication_source_statuses_non_exact_cutover,
        .projected_replication_source_statuses_reseed_recommended = projected_replication_source_statuses_reseed_recommended,
        .projected_replication_source_statuses_exported_snapshot = projected_replication_source_statuses_exported_snapshot,
        .projected_replication_source_statuses_slot_first = projected_replication_source_statuses_slot_first,
        .projected_replication_source_statuses_slot_resumed = projected_replication_source_statuses_slot_resumed,
        .projected_replication_source_statuses_snapshot = projected_replication_source_statuses_snapshot,
        .projected_replication_source_statuses_cutover_prepared = projected_replication_source_statuses_cutover_prepared,
        .projected_replication_source_statuses_streaming = projected_replication_source_statuses_streaming,
        .projected_replication_source_statuses_failed = projected_replication_source_statuses_failed,
        .projected_replication_source_statuses_with_last_error = projected_replication_source_statuses_with_last_error,
        .projected_replication_source_statuses_slot_missing_failed = projected_replication_source_statuses_slot_missing_failed,
        .projected_replication_source_statuses_retryable_failed = projected_replication_source_statuses_retryable_failed,
        .projected_replication_source_statuses_terminal_failed = projected_replication_source_statuses_terminal_failed,
        .projected_replication_source_statuses_with_consecutive_failures = projected_replication_source_statuses_with_consecutive_failures,
        .projected_replication_source_statuses_with_success_timestamp = projected_replication_source_statuses_with_success_timestamp,
        .projected_replication_source_statuses_with_change_timestamp = projected_replication_source_statuses_with_change_timestamp,
        .projected_replication_source_consecutive_failures_total = projected_replication_source_consecutive_failures_total,
        .projected_replication_source_consecutive_failures_max = projected_replication_source_consecutive_failures_max,
        .projected_replication_source_lag_records_total = projected_replication_source_lag_records_total,
        .projected_replication_source_lag_records_max = projected_replication_source_lag_records_max,
        .projected_replication_source_lag_millis_total = projected_replication_source_lag_millis_total,
        .projected_replication_source_lag_millis_max = projected_replication_source_lag_millis_max,
        .projected_replication_source_observed_lag_millis_total = projected_replication_source_observed_lag_millis_total,
        .projected_replication_source_observed_lag_millis_max = projected_replication_source_observed_lag_millis_max,
        .projected_replication_source_statuses_with_source_commit_timestamp = projected_replication_source_statuses_with_source_commit_timestamp,
        .projected_replication_source_last_success_at_ms_max = projected_replication_source_last_success_at_ms_max,
        .projected_replication_source_last_source_commit_at_ms_max = projected_replication_source_last_source_commit_at_ms_max,
        .projected_replication_source_last_change_applied_at_ms_max = projected_replication_source_last_change_applied_at_ms_max,
        .projected_extension_packages = projected_extension_packages.len,
        .projected_installed_extensions = projected_installed_extensions.len,
        .projected_extension_members = projected_extension_members.len,
        .projected_extension_dependencies = projected_extension_dependencies.len,
        .projected_ranges = projected_ranges.len,
        .projected_stores = projected_stores.len,
        .projected_placement_intents = projected_placement_intents.len,
        .projected_snapshot_bootstrap_intents = projected_snapshot_bootstrap_intents,
        .projected_backup_restore_bootstrap_intents = projected_backup_restore_bootstrap_intents,
        .projected_shuffle_join_leases = projected_shuffle_join_leases.len,
        .projected_restore_progress = projected_restore_progress.len,
        .projected_split_transitions = projected_split_transitions.len,
        .projected_merge_transitions = projected_merge_transitions.len,
        .projected_doc_identity_lifecycle_unknown = doc_identity_lifecycle_unknown,
        .projected_doc_identity_lifecycle_preserving = doc_identity_lifecycle_preserving,
        .projected_doc_identity_lifecycle_reassigning = doc_identity_lifecycle_reassigning,
        .projected_doc_identity_lifecycle_rebuild_required = doc_identity_lifecycle_rebuild_required,
        .projected_doc_identity_lifecycle_ready = doc_identity_lifecycle_ready,
        .preferred_stores = preferred_stores,
        .constrained_stores = constrained_stores,
        .overloaded_stores = overloaded_stores,
        .excluded_stores = excluded_stores,
        .backfill_stores = backfill_stores,
        .active_backfills = active_backfills,
        .placement_upserts = planning.placement_upserts,
        .placement_removals = planning.placement_removals,
        .repair_placement_groups = planning.repair_placement_groups,
        .rebalance_placement_groups = planning.rebalance_placement_groups,
    };
}

fn realtimeNowMillis() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {},
        else => return 0,
    }
    const sec: u64 = @intCast(@max(ts.sec, 0));
    const nsec: u64 = @intCast(@max(ts.nsec, 0));
    return sec * std.time.ms_per_s + @divTrunc(nsec, std.time.ns_per_ms);
}

fn countReplicationSourcesJson(alloc: std.mem.Allocator, replication_sources_json: []const u8) !usize {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, replication_sources_json, .{});
    defer parsed.deinit();
    return switch (parsed.value) {
        .array => |array| array.items.len,
        else => 0,
    };
}

test "metadata service proposes split transitions into the metadata group" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-service-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-service-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1900,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1900,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertTable(.{ .table_id = 20, .name = "docs" });
    try svc.upsertRange(.{
        .group_id = 2001,
        .range_id = 2001,
        .table_id = 20,
        .start_key = "",
        .end_key = "doc:z",
        .doc_identity_shard_id = 2001,
        .doc_identity_range_id = 2001,
        .split_attempt_epoch = 0,
    });
    try svc.admitSplitTransition(.{
        .expected_source_epoch = 0,
        .record = .{
            .transition_id = 4001,
            .attempt_epoch = 1,
            .source_group_id = 2001,
            .destination_group_id = 2002,
            .phase = .prepare,
            .split_key = "doc:m",
            .source_range_end = "doc:z",
            .table_contract = .{
                .table_id = 20,
                .table_name = "docs",
                .indexes_json = "{}",
                .source_identity = .{ .shard_id = 2001, .range_id = 2001 },
                .target_identity = .{ .shard_id = 2001, .range_id = 2001 },
            },
        },
    });

    try runServiceRounds(&svc, 8);

    const projected = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u64, 4001), projected[0].transition_id);
    try std.testing.expectEqualStrings("doc:m", projected[0].split_key.?);
}

test "metadata service requires a configured metadata group id" {
    try std.testing.expectError(error.MissingMetadataGroupId, MetadataService.init(
        std.testing.allocator,
        .{ .host = .{ .local_node_id = 1 } },
        .{},
        .{},
    ));
}

test "metadata service status reflects reconcile lease ownership" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-lease-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-lease-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1915,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{
        .reconcile_lease = .{ .lease_ttl_ms = 2_000 },
    });
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1915,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });

    try svc.runRound();
    const before_leadership = try svc.status();
    try std.testing.expect(before_leadership.reconcile_lease_enabled);
    try std.testing.expect(!before_leadership.reconcile_lease_held_by_local);
    try std.testing.expectEqual(@as(u64, 0), before_leadership.reconcile_lease_owner_node_id);

    try svc.campaignMetadataGroup();
    try runServiceRoundsUntilMetadataReady(&svc);
    try runServiceRounds(&svc, 2);

    const held_status = try svc.status();
    try std.testing.expect(held_status.reconcile_lease_enabled);
    try std.testing.expect(held_status.reconcile_lease_held_by_local);
    try std.testing.expectEqual(@as(u64, 1), held_status.reconcile_lease_owner_node_id);
    try std.testing.expect(held_status.reconcile_lease_expires_at_ms > 0);
    try std.testing.expectEqual(@as(u64, 1), held_status.reconcile_lease_acquisition_count);

    const projected_lease_opt = try svc.getProjectedReconcileLease();
    try std.testing.expect(projected_lease_opt != null);
    const projected_lease = projected_lease_opt.?;
    try std.testing.expectEqual(@as(u64, 1), projected_lease.owner_node_id);
    try std.testing.expect(projected_lease.expires_at_ms >= held_status.reconcile_lease_expires_at_ms);
}

test "metadata snapshot status prefers cached reconcile lease accessor when available" {
    const FakeService = struct {
        cached_calls: usize = 0,
        direct_calls: usize = 0,

        pub fn reconcileLeaseStats(_: *@This()) metadata_reconcile_lease.Stats {
            return .{ .enabled = true };
        }

        pub fn statusProjectedReconcileLease(self: *@This(), _: u64) !?metadata_reconcile_lease.ReconcileLeaseRecord {
            self.cached_calls += 1;
            return .{
                .owner_node_id = 9,
                .expires_at_ms = 1234,
            };
        }

        pub fn getProjectedReconcileLease(self: *@This()) !?metadata_reconcile_lease.ReconcileLeaseRecord {
            self.direct_calls += 1;
            return .{
                .owner_node_id = 77,
                .expires_at_ms = 9999,
            };
        }

        pub fn listProjectedTables(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            return try alloc.alloc(metadata_table_manager.TableRecord, 0);
        }

        pub fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedRanges(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            return try alloc.alloc(metadata_table_manager.RangeRecord, 0);
        }

        pub fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedStores(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.StoreRecord {
            return try alloc.alloc(metadata_table_manager.StoreRecord, 0);
        }

        pub fn freeProjectedStores(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.StoreRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            return try alloc.alloc(raft_reconciler.PlacementIntent, 0);
        }

        pub fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            alloc.free(intents);
        }

        pub fn listProjectedShuffleJoinLeases(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.ShuffleJoinLeaseRecord {
            return try alloc.alloc(metadata_table_manager.ShuffleJoinLeaseRecord, 0);
        }

        pub fn freeProjectedShuffleJoinLeases(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.ShuffleJoinLeaseRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedRestoreProgress(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RestoreProgressRecord {
            return try alloc.alloc(metadata_table_manager.RestoreProgressRecord, 0);
        }

        pub fn freeProjectedRestoreProgress(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RestoreProgressRecord) void {
            alloc.free(records);
        }

        pub fn listProjectedReplicationSourceStatuses(_: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.ReplicationSourceStatusRecord {
            return try alloc.alloc(metadata_table_manager.ReplicationSourceStatusRecord, 0);
        }

        pub fn freeProjectedReplicationSourceStatuses(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.ReplicationSourceStatusRecord) void {
            alloc.free(records);
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

        pub fn observeSplitTransition(_: *@This(), _: u64) !?transition_state.SplitObservation {
            return null;
        }

        pub fn observeMergeTransition(_: *@This(), _: u64) !?transition_state.MergeObservation {
            return null;
        }

        pub fn applyReconciliationPlan(_: *@This(), _: *const metadata_reconciler.ReconciliationPlan) !void {}
    };

    var service = FakeService{};
    const status = try snapshotStatus(std.testing.allocator, 77, &service, .{});
    try std.testing.expectEqual(@as(usize, 1), service.cached_calls);
    try std.testing.expectEqual(@as(usize, 0), service.direct_calls);
    try std.testing.expectEqual(@as(u64, 9), status.reconcile_lease_owner_node_id);
    try std.testing.expectEqual(@as(u64, 1234), status.reconcile_lease_expires_at_ms);
}

test "metadata service can apply reconciliation plan proposals" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-plan-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-plan-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1910,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1910,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try runServiceRoundsUntilMetadataReady(&svc);

    var manager = metadata_table_manager.TableManager.init(std.testing.allocator);
    defer manager.deinit();
    try manager.upsertTable(.{ .table_id = 10, .name = "docs" });
    try manager.upsertRange(.{
        .group_id = 2101,
        .range_id = 2101,
        .table_id = 10,
        .start_key = "doc:a",
        .end_key = "doc:m",
        .doc_identity_shard_id = 2101,
        .doc_identity_range_id = 2101,
    });
    try manager.upsertRange(.{
        .group_id = 2102,
        .range_id = 2102,
        .table_id = 10,
        .start_key = "doc:m",
        .end_key = "doc:z",
        .doc_identity_shard_id = 2102,
        .doc_identity_range_id = 2102,
    });
    try manager.requestSplit(.{
        .transition_id = 9101,
        .table_id = 10,
        .source_group_id = 2101,
        .destination_group_id = 2103,
        .split_key = "doc:h",
    });

    var reconciler = metadata_reconciler.Reconciler.init(std.testing.allocator);
    var plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{});
    defer plan.deinit(std.testing.allocator);

    try svc.applyReconciliationPlan(&plan);
    try runServiceRounds(&svc, 8);

    const projected_tables = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, projected_tables);
    const projected_ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, projected_ranges);

    var admission_plan = try reconciler.computePlan(&manager, &.{}, &.{}, .{
        .tables = projected_tables,
        .ranges = projected_ranges,
    });
    defer admission_plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 1), admission_plan.split_admissions.len);
    try svc.applyReconciliationPlan(&admission_plan);
    try runServiceRounds(&svc, 8);

    const split_records = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, split_records);
    try std.testing.expectEqual(@as(usize, 1), split_records.len);
    try std.testing.expectEqual(@as(u64, 9101), split_records[0].transition_id);
}

test "metadata control loop preserves prepared state while renewing its lease" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-loop-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-loop-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };
    var reconcile_clock = platform_clock.ManualClock{};

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1920,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{
        .reconcile_lease = .{
            .lease_ttl_ms = 100,
            .clock = reconcile_clock.clock(),
        },
    });
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1920,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertTable(.{ .table_id = 20, .name = "docs" });
    try svc.upsertRange(.{ .group_id = 2201, .table_id = 20, .start_key = "doc:a", .end_key = "doc:m" });
    try svc.upsertRange(.{ .group_id = 2202, .table_id = 20, .start_key = "doc:m", .end_key = "doc:z" });
    try runServiceRounds(&svc, 8);

    var loop = metadata_control_loop.MetadataControlLoop.init(std.testing.allocator);
    defer loop.deinit();
    try loop.stateRef().syncProjected(&svc);
    try loop.stateRef().seedDesiredFromProjected();
    try loop.stateRef().tableManager().requestSplit(.{
        .transition_id = 9201,
        .table_id = 20,
        .source_group_id = 2201,
        .destination_group_id = 2203,
        .split_key = "doc:h",
    });

    // Force the lease to expire after the caller has staged desired state.
    // Renewal must not refresh that state from projection and lose the split
    // request before reconciliation applies it.
    reconcile_clock.advanceMs(101);
    const summary = try svc.reconcilePreparedEnsuringLease(&loop);
    try std.testing.expectEqual(@as(usize, 1), summary.split_admissions);
    try std.testing.expectEqual(@as(usize, 0), summary.split_upserts);

    try runServiceRounds(&svc, 8);

    const split_records = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, split_records);
    try std.testing.expectEqual(@as(usize, 1), split_records.len);
    try std.testing.expectEqual(@as(u64, 9201), split_records[0].transition_id);
}

test "metadata service projects committed table and range topology" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-topology-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1930,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1930,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertTable(.{
        .table_id = 77,
        .name = "docs",
        .description = "docs table",
        .schema_json = "{\"kind\":\"demo\"}",
        .indexes_json = "{\"default\":{}}",
        .replication_sources_json = "[\"seed\"]",
        .desired_replica_count = 5,
        .min_ranges = 2,
    });
    try svc.upsertRange(.{ .group_id = 7701, .table_id = 77, .start_key = "doc:a", .end_key = "doc:z" });

    try runServiceRounds(&svc, 8);

    const tables = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, tables);
    const ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(u64, 77), tables[0].table_id);
    try std.testing.expectEqualStrings("docs", tables[0].name);
    try std.testing.expectEqualStrings("docs table", tables[0].description);
    try std.testing.expectEqualStrings("{\"kind\":\"demo\"}", tables[0].schema_json);
    try std.testing.expectEqualStrings("{\"default\":{}}", tables[0].indexes_json);
    try std.testing.expectEqualStrings("[\"seed\"]", tables[0].replication_sources_json);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 7701), ranges[0].group_id);
}

test "table workflow can drive real metadata service topology and split setup" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{
                .ptr = self,
                .vtable = &.{
                    .build_descriptor = buildDescriptor,
                    .free_descriptor = freeDescriptor,
                },
            };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-workflow-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-workflow-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1940,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1940,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();

    const create_summary = try workflow.createTable(&svc, .{
        .table_id = 88,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 8801,
        .table_id = 88,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try std.testing.expectEqual(@as(usize, 1), create_summary.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), create_summary.range_upserts);

    var rounds: usize = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const tables = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, tables);
    const ranges = try svc.listProjectedRanges(std.testing.allocator);
    defer svc.freeProjectedRanges(std.testing.allocator, ranges);
    try std.testing.expectEqual(@as(usize, 1), tables.len);
    try std.testing.expectEqual(@as(usize, 1), ranges.len);

    try svc.upsertStore(.{
        .store_id = 1,
        .node_id = 1,
        .role = "data",
        .health_class = "healthy",
        .live = true,
    });
    try svc.runRound();

    var group_statuses = [_]metadata_table_manager.GroupStatusReport{
        .{
            .group_id = 8801,
            .local_leader = true,
            .local_voter = true,
            .voter_count = 1,
        },
    };
    var runtime_statuses = [_]metadata_table_manager.RuntimeGroupStatusReport{
        .{
            .table_id = 88,
            .table_name = "docs",
            .group_id = 8801,
            .store_id = 1,
            .node_id = 1,
            .source = "test",
            .freshness = "fresh",
            .doc_identity = .{
                .namespace_table_id = 88,
                .namespace_shard_id = 8801,
                .namespace_range_id = 8801,
                .next_ordinal = 1,
                .complete = true,
            },
        },
    };
    try svc.reportStoreStatus(.{
        .store_id = 1,
        .group_statuses = &group_statuses,
        .runtime_statuses = &runtime_statuses,
    });
    try svc.runRound();

    try workflow.bootstrapDesiredFromCommitted(&svc);
    const split_summary = try workflow.requestSplit(&svc, .{
        .transition_id = 9401,
        .table_id = 88,
        .source_group_id = 8801,
        .destination_group_id = 8802,
        .split_key = "doc:m",
    });
    try std.testing.expectEqual(@as(usize, 1), split_summary.split_admissions);
    try std.testing.expectEqual(@as(usize, 0), split_summary.split_upserts);

    rounds = 0;
    while (rounds < 8) : (rounds += 1) try svc.runRound();

    const splits = try svc.listProjectedSplitTransitions(std.testing.allocator);
    defer svc.freeProjectedSplitTransitions(std.testing.allocator, splits);
    try std.testing.expectEqual(@as(usize, 1), splits.len);
    try std.testing.expectEqual(@as(u64, 9401), splits[0].transition_id);
}

test "metadata service projects committed placement intents into local hosted replicas" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-placement-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1950,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1950,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertNode(.{ .node_id = 1 });
    try svc.runRound();
    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 1951,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .empty,
        },
        .peer_node_ids = &.{ 1, 2, 3 },
    }, null, 0, false);

    try runServiceRoundsUntilHostedStatus(&svc, 1951, .active, 8, "placement-intent activation");

    const projected = try svc.listProjectedPlacementIntents(std.testing.allocator);
    defer svc.freeProjectedPlacementIntents(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u64, 1951), projected[0].record.group_id);

    try svc.removeReplicaIntent(1951, 1, projected[0].record.metadata_version);
    try runServiceRoundsUntilHostedStatus(&svc, 1951, .absent, 8, "placement-intent removal");
}

test "table workflow can drive placement intents through the real metadata control loop" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-control-loop-placement-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-control-loop-placement-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1960,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1960,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertNode(.{ .node_id = 1 });
    try svc.upsertNode(.{ .node_id = 2 });
    try svc.upsertNode(.{ .node_id = 3 });
    try svc.runRound();
    var workflow = metadata_table_workflow.TableWorkflow.init(std.testing.allocator);
    defer workflow.deinit();
    try workflow.setPlacementCandidates(&.{ 1, 2, 3 });

    const create_summary = try workflow.createTable(&svc, .{
        .table_id = 99,
        .name = "docs",
        .desired_replica_count = 3,
        .min_ranges = 1,
    }, .{
        .group_id = 9901,
        .table_id = 99,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try std.testing.expectEqual(@as(usize, 1), create_summary.table_upserts);
    try std.testing.expectEqual(@as(usize, 1), create_summary.range_upserts);
    try std.testing.expectEqual(@as(usize, 3), create_summary.placement_upserts);

    try runServiceRoundsUntilHostedStatus(&svc, 9901, .active, 12, "table-workflow placement activation");

    const intents = try svc.listProjectedPlacementIntents(std.testing.allocator);
    defer svc.freeProjectedPlacementIntents(std.testing.allocator, intents);
    try std.testing.expectEqual(@as(usize, 3), intents.len);
}

test "metadata service reports store status without losing placement attributes" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-status-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-status-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1970,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1970,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{
        .store_id = 11,
        .node_id = 1,
        .role = "data",
        .health_class = "healthy",
        .failure_domain = "rack-a",
        .live = true,
        .drain_requested = true,
        .capacity_bytes = 1024,
        .available_bytes = 800,
    });
    try svc.runRound();

    var group_statuses = [_]metadata_table_manager.GroupStatusReport{.{
        .group_id = 4201,
        .disk_bytes = 4096,
        .disk_bytes_known = true,
        .updated_at_millis = 1_700_000_000_000,
    }};
    try svc.reportStoreStatus(.{
        .store_id = 11,
        .live = false,
        .health_class = "degraded",
        .capacity_bytes = 2048,
        .available_bytes = 0,
        .lease_pressure = 92,
        .read_load = 210,
        .write_load = 130,
        .active_backfills = 2,
        .backfill_progress_millis = 375,
        .group_statuses = group_statuses[0..],
    });
    try svc.runRound();

    // The acknowledgement itself must be enough to publish another store
    // record even when all coalesced status and capacity fields are unchanged.
    group_statuses[0].observed_reallocation_request_id = 0x123456789abcdef00123456789abcdef;
    try svc.reportStoreStatus(.{
        .store_id = 11,
        .live = false,
        .health_class = "degraded",
        .capacity_bytes = 2048,
        .available_bytes = 0,
        .lease_pressure = 92,
        .read_load = 210,
        .write_load = 130,
        .active_backfills = 2,
        .backfill_progress_millis = 375,
        .group_statuses = group_statuses[0..],
    });
    try svc.runRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u64, 11), projected[0].store_id);
    try std.testing.expectEqual(@as(u64, 1), projected[0].node_id);
    try std.testing.expect(std.mem.eql(u8, projected[0].role, "data"));
    try std.testing.expect(std.mem.eql(u8, projected[0].failure_domain, "rack-a"));
    try std.testing.expect(std.mem.eql(u8, projected[0].health_class, "degraded"));
    try std.testing.expectEqual(false, projected[0].live);
    try std.testing.expect(projected[0].drain_requested);
    try std.testing.expectEqual(@as(u64, 2048), projected[0].capacity_bytes);
    try std.testing.expectEqual(@as(u64, 0), projected[0].available_bytes);
    try std.testing.expectEqual(@as(u32, 92), projected[0].lease_pressure);
    try std.testing.expectEqual(@as(u32, 210), projected[0].read_load);
    try std.testing.expectEqual(@as(u32, 130), projected[0].write_load);
    try std.testing.expectEqual(@as(u32, 2), projected[0].active_backfills);
    try std.testing.expectEqual(@as(u16, 375), projected[0].backfill_progress_millis);
    try std.testing.expectEqual(@as(usize, 1), projected[0].group_statuses.len);
    try std.testing.expectEqual(
        @as(u128, 0x123456789abcdef00123456789abcdef),
        projected[0].group_statuses[0].observed_reallocation_request_id,
    );

    const status = try svc.status();
    try std.testing.expectEqual(@as(usize, 1), status.backfill_stores);
    try std.testing.expectEqual(@as(usize, 2), status.active_backfills);
}

test "metadata service batches store status reports" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-status-batch-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-store-status-batch-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1971,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1971,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 21, .node_id = 1, .role = "data", .failure_domain = "rack-a", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 22, .node_id = 2, .role = "data", .failure_domain = "rack-b", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try svc.runRound();

    try std.testing.expectEqual(@as(usize, 2), try svc.reportStoreStatuses(&.{
        .{ .store_id = 21, .live = false, .health_class = "degraded", .capacity_bytes = 1024, .available_bytes = 0, .lease_pressure = 95, .read_load = 140, .write_load = 110, .active_backfills = 1, .backfill_progress_millis = 250 },
        .{ .store_id = 22, .live = true, .health_class = "healthy", .capacity_bytes = 2048, .available_bytes = 1200, .lease_pressure = 8, .read_load = 18, .write_load = 12, .active_backfills = 0, .backfill_progress_millis = 1000 },
    }));
    try svc.runRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 2), projected.len);
    const first = metadata_store_observer.findStoreIndex(projected, 21).?;
    const second = metadata_store_observer.findStoreIndex(projected, 22).?;
    try std.testing.expect(std.mem.eql(u8, projected[first].failure_domain, "rack-a"));
    try std.testing.expectEqual(false, projected[first].live);
    try std.testing.expect(std.mem.eql(u8, projected[first].health_class, "degraded"));
    try std.testing.expectEqual(@as(u64, 0), projected[first].available_bytes);
    try std.testing.expectEqual(@as(u32, 95), projected[first].lease_pressure);
    try std.testing.expectEqual(@as(u32, 140), projected[first].read_load);
    try std.testing.expectEqual(@as(u32, 110), projected[first].write_load);
    try std.testing.expectEqual(@as(u32, 1), projected[first].active_backfills);
    try std.testing.expectEqual(@as(u16, 250), projected[first].backfill_progress_millis);
    try std.testing.expect(std.mem.eql(u8, projected[second].failure_domain, "rack-b"));
    try std.testing.expectEqual(true, projected[second].live);
    try std.testing.expect(std.mem.eql(u8, projected[second].health_class, "healthy"));
    try std.testing.expectEqual(@as(u64, 2048), projected[second].capacity_bytes);
    try std.testing.expectEqual(@as(u64, 1200), projected[second].available_bytes);
    try std.testing.expectEqual(@as(u32, 8), projected[second].lease_pressure);
    try std.testing.expectEqual(@as(u32, 18), projected[second].read_load);
    try std.testing.expectEqual(@as(u32, 12), projected[second].write_load);
    try std.testing.expectEqual(@as(u32, 0), projected[second].active_backfills);
    try std.testing.expectEqual(@as(u16, 1000), projected[second].backfill_progress_millis);

    const status = try svc.status();
    try std.testing.expectEqual(@as(usize, 1), status.backfill_stores);
    try std.testing.expectEqual(@as(usize, 1), status.active_backfills);
}

test "metadata service persists coalesces promotes and clears reallocation requests" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reallocation-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reallocation-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1972,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1972,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try runServiceRoundsUntilMetadataReady(&svc);

    try svc.requestReallocation(77_000);
    try svc.runRound();
    const requested = try svc.getProjectedReallocationRequest();
    try std.testing.expect(requested != null);
    try std.testing.expect(requested.?.request_id != 0);
    try std.testing.expectEqual(@as(u64, 77_000), requested.?.requested_at_ms);
    try std.testing.expectEqual(metadata_reallocation_request.barrier_protocol_version, requested.?.barrier_protocol_version);
    try std.testing.expectEqual(try svc.metadataIncarnation(), requested.?.metadata_incarnation);
    try std.testing.expectEqual(@as(u32, 1), requested.?.protected_metadata_member_count);
    try std.testing.expectEqualSlices(
        u8,
        &metadata_reallocation_request.membershipFingerprint(&.{1}),
        &requested.?.protected_metadata_membership_fingerprint,
    );
    var requested_snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&requested_snapshot);
    try std.testing.expectEqual(requested, requested_snapshot.reallocation_request);

    // A reconcile plan may decide to clear this generation before a lifecycle
    // mutation queues another reallocation. Apply two such triggers before the
    // stale clear: they must coalesce into one private pending generation, and
    // the clear must atomically promote the latest trigger instead of losing it.
    try svc.requestReallocation(88_000);
    try svc.requestReallocation(99_000);
    var clear_active_plan = metadata_reconciler.ReconciliationPlan.empty();
    clear_active_plan.clear_reallocation_request = requested.?.request_id;
    try svc.applyReconciliationPlan(&clear_active_plan);
    try svc.runRound();
    const promoted = try svc.getProjectedReallocationRequest();
    try std.testing.expect(promoted != null);
    try std.testing.expect(promoted.?.request_id != requested.?.request_id);
    try std.testing.expectEqual(@as(u64, 99_000), promoted.?.requested_at_ms);

    // A repeated clear from the old plan cannot consume its promoted successor.
    var stale_plan = metadata_reconciler.ReconciliationPlan.empty();
    stale_plan.clear_reallocation_request = requested.?.request_id;
    try svc.applyReconciliationPlan(&stale_plan);
    try svc.runRound();
    try std.testing.expectEqual(promoted, try svc.getProjectedReallocationRequest());

    var clear_promoted_plan = metadata_reconciler.ReconciliationPlan.empty();
    clear_promoted_plan.clear_reallocation_request = promoted.?.request_id;
    try svc.applyReconciliationPlan(&clear_promoted_plan);
    try svc.runRound();
    try std.testing.expect((try svc.getProjectedReallocationRequest()) == null);
}

test "metadata service auto-reports local store backfill status during runRound" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1974,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1974,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 51, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertTable(.{ .table_id = 88, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 8801, .table_id = 88, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-8801/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    try rebuildStateForPath(state_path).update("doc:m");

    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();
    try svc.runLifecycleRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u32, 1), projected[0].active_backfills);
    try std.testing.expect(projected[0].backfill_progress_millis > 0);
}

test "metadata service reports automatic store status across shared multi-store roots" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-multi-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-multi-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1975,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1975,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 61, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 62, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880 });
    try svc.upsertTable(.{ .table_id = 89, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 8901, .table_id = 89, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-8901/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    try rebuildStateForPath(state_path).update("doc:m");

    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();
    try svc.runLifecycleRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 2), projected.len);
    const first = metadata_store_observer.findStoreIndex(projected, 61).?;
    const second = metadata_store_observer.findStoreIndex(projected, 62).?;
    const total = projected[first].active_backfills + projected[second].active_backfills;
    try std.testing.expectEqual(@as(u32, 1), total);
    try std.testing.expect(projected[first].backfill_progress_millis > 0 or projected[second].backfill_progress_millis > 0);

    const affinity_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-8901/store-affinity", .{replica_root});
    defer std.testing.allocator.free(affinity_path);
    const affinity_contents = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), affinity_path, std.testing.allocator, .limited(128));
    defer std.testing.allocator.free(affinity_contents);
    const affinity_store_id = try std.fmt.parseInt(u64, std.mem.trim(u8, affinity_contents, " \t\r\n"), 10);
    try std.testing.expect(affinity_store_id == 61 or affinity_store_id == 62);
}

test "metadata service reports automatic store status across explicit multi-store roots" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-explicit-roots", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-explicit-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1976,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1976,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 71, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 72, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880 });
    try svc.upsertTable(.{ .table_id = 90, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 9001, .table_id = 90, .start_key = "doc:a", .end_key = "doc:m" });
    try svc.upsertRange(.{ .group_id = 9002, .table_id = 90, .start_key = "doc:m", .end_key = "doc:z" });
    try svc.runRound();

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const left_db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/store-71/group-9001/table-db", .{replica_root});
    defer std.testing.allocator.free(left_db_path);
    const right_db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/store-72/group-9002/table-db", .{replica_root});
    defer std.testing.allocator.free(right_db_path);
    try fs_paths.createDirPathPortable(io_impl.io(), left_db_path);
    try fs_paths.createDirPathPortable(io_impl.io(), right_db_path);

    {
        var left_db = try db_mod.DB.open(std.testing.allocator, left_db_path, .{});
        defer left_db.close();
        try left_db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }
    {
        var right_db = try db_mod.DB.open(std.testing.allocator, right_db_path, .{});
        defer right_db.close();
        try right_db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const left_state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{left_db_path});
    defer std.testing.allocator.free(left_state_path);
    try rebuildStateForPath(left_state_path).update("doc:g");

    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();
    try svc.runLifecycleRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 2), projected.len);
    const first = metadata_store_observer.findStoreIndex(projected, 71).?;
    const second = metadata_store_observer.findStoreIndex(projected, 72).?;
    try std.testing.expectEqual(@as(u32, 1), projected[first].active_backfills);
    try std.testing.expect(projected[first].backfill_progress_millis > 0);
    try std.testing.expectEqual(@as(u32, 0), projected[second].active_backfills);
}

test "metadata service prefers placement-role-compatible store affinity in shared roots" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-role-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-role-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1977,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1977,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 81, .node_id = 1, .role = "hot", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 82, .node_id = 1, .role = "cold", .live = true, .capacity_bytes = 1024, .available_bytes = 880 });
    try svc.upsertTable(.{ .table_id = 91, .name = "docs", .placement_role = "cold", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 9101, .table_id = 91, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9101/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    try rebuildStateForPath(state_path).update("doc:m");

    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();
    try svc.runLifecycleRound();

    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    const hot_index = metadata_store_observer.findStoreIndex(projected, 81).?;
    const cold_index = metadata_store_observer.findStoreIndex(projected, 82).?;
    try std.testing.expectEqual(@as(u32, 0), projected[hot_index].active_backfills);
    try std.testing.expectEqual(@as(u32, 1), projected[cold_index].active_backfills);
}

test "metadata service shared-root reports survive transient rebuild marker removal" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-transient-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9301/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    try rebuildStateForPath(state_path).update("doc:m");

    const markers = try collectStoreStatusBackfillMarkers(std.testing.allocator, replica_root);
    defer freeStoreStatusBackfillMarkers(std.testing.allocator, markers);
    try std.testing.expectEqual(@as(usize, 1), markers.len);

    try std.Io.Dir.cwd().deleteFile(io_impl.io(), state_path);

    const stores = [_]metadata_table_manager.StoreRecord{
        .{ .store_id = 93, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880 },
        .{ .store_id = 94, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 980 },
    };
    const placements = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{
                .group_id = 9301,
                .replica_id = 1,
                .local_node_id = 1,
                .bootstrap_mode = .persisted,
            },
            .store_id = 94,
            .peer_node_ids = &.{1},
        },
    };
    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 93, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 9301, .table_id = 93, .start_key = "doc:a", .end_key = "doc:z" },
    };

    const projected = try collectSharedRootLocalStoreStatusReports(
        .{},
        std.testing.allocator,
        std.testing.io,
        replica_root,
        1,
        stores[0..],
        placements[0..],
        tables[0..],
        ranges[0..],
        &.{},
        markers,
    );
    defer freeOwnedStoreStatusReports(std.testing.allocator, projected);
    const first_index = findStoreStatusReportIndex(projected, 93).?;
    const second_index = findStoreStatusReportIndex(projected, 94).?;
    try std.testing.expectEqual(@as(u32, 0), projected[first_index].active_backfills);
    try std.testing.expectEqual(@as(u32, 1), projected[second_index].active_backfills);
}

test "metadata service lifecycle round uses cached backfill markers" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const ProviderCapture = struct {
        calls: usize = 0,

        fn collect(
            ptr: *anyopaque,
            alloc: std.mem.Allocator,
            _: []const u8,
            _: []const metadata_table_manager.TableRecord,
            _: []const metadata_table_manager.RangeRecord,
            _: []const metadata_table_manager.StoreRecord,
            _: []const metadata_reconciler.MergedGroupStatus,
            _: []const transition_state.SplitTransitionRecord,
            _: []const transition_state.MergeTransitionRecord,
            _: []const transition_state.SplitObservationRecord,
            _: []const transition_state.MergeObservationRecord,
        ) ![]metadata_table_manager.GroupStatusReport {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return try alloc.alloc(metadata_table_manager.GroupStatusReport, 0);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-store-status-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-store-status-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1978,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();
    var provider_capture = ProviderCapture{};
    svc.setLocalGroupStatusProvider(.{
        .ptr = &provider_capture,
        .vtable = &.{ .collect = ProviderCapture.collect },
    });

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1978,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 101, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertTable(.{ .table_id = 101, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 10101, .table_id = 101, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-10101/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    try rebuildStateForPath(state_path).update("doc:m");

    svc.store_status_backfill_marker_cache.replace(
        std.testing.allocator,
        try collectStoreStatusBackfillMarkers(std.testing.allocator, replica_root),
        monotonicMs(),
    );
    try std.testing.expectEqual(@as(usize, 1), svc.store_status_backfill_marker_cache.markers.len);
    svc.store_status_ticks = 39;
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), state_path);

    try svc.runLifecycleRound();
    try std.testing.expectEqual(@as(usize, 1), provider_capture.calls);
    try std.testing.expectEqual(@as(usize, 1), svc.store_status_backfill_marker_cache.markers.len);
    try std.testing.expect(svc.store_status_backfill_marker_cache.rescan_requested);
    try std.testing.expectEqual(@as(usize, 39), svc.store_status_ticks);
}

test "metadata service lifecycle round discovers backfill markers immediately" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-discovery-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-discovery-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1979,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1979,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertStore(.{ .store_id = 102, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertTable(.{ .table_id = 102, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 10201, .table_id = 102, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.runRound();

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-10201/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    try rebuildStateForPath(state_path).update("doc:m");

    try std.testing.expectEqual(@as(usize, 0), svc.store_status_backfill_marker_cache.markers.len);
    svc.store_status_backfill_marker_cache.rescan_requested = true;
    try svc.runLifecycleRound();

    try std.testing.expectEqual(@as(usize, 1), svc.store_status_backfill_marker_cache.markers.len);
    try svc.runLifecycleRound();
    const projected = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, projected);
    try std.testing.expectEqual(@as(usize, 1), projected.len);
    try std.testing.expectEqual(@as(u32, 1), projected[0].active_backfills);
}

test "metadata service lifecycle round backs off empty backfill probes after initial scan" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-empty-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-lifecycle-empty-catalog.txt", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1980,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1980,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try runServiceRoundsUntilMetadataReady(&svc);

    try std.testing.expectEqual(@as(usize, 0), svc.store_status_backfill_marker_cache.markers.len);
    const first_scanned_at_ms = svc.store_status_backfill_marker_cache.scanned_at_ms;
    try std.testing.expect(first_scanned_at_ms > 0);

    try svc.runLifecycleRound();
    try std.testing.expectEqual(first_scanned_at_ms, svc.store_status_backfill_marker_cache.scanned_at_ms);
    try std.testing.expectEqual(@as(usize, 0), svc.store_status_backfill_marker_cache.markers.len);
    try std.testing.expectEqual(@as(usize, 1), svc.store_status_backfill_probe_ticks);

    try svc.runLifecycleRound();
    try std.testing.expectEqual(first_scanned_at_ms, svc.store_status_backfill_marker_cache.scanned_at_ms);
    try std.testing.expectEqual(@as(usize, 0), svc.store_status_backfill_marker_cache.markers.len);
    try std.testing.expectEqual(@as(usize, 2), svc.store_status_backfill_probe_ticks);
}

test "metadata service cached backfill markers rescan immediately after disappearance" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-store-status-rescan-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9401/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    try rebuildStateForPath(state_path).update("doc:m");

    var cache = StoreStatusBackfillMarkerCache{};
    defer cache.deinit(std.testing.allocator);
    cache.replace(
        std.testing.allocator,
        try collectStoreStatusBackfillMarkers(std.testing.allocator, replica_root),
        monotonicMs(),
    );
    try std.testing.expectEqual(@as(usize, 1), cache.markers.len);
    try std.testing.expect(!cache.rescan_requested);

    try std.Io.Dir.cwd().deleteFile(io_impl.io(), state_path);

    const FakeService = struct {
        alloc: std.mem.Allocator,
        store_status_backfill_marker_cache: StoreStatusBackfillMarkerCache,
    };
    var service = FakeService{
        .alloc = std.testing.allocator,
        .store_status_backfill_marker_cache = cache,
    };
    cache = .{};
    defer service.store_status_backfill_marker_cache.deinit(std.testing.allocator);

    for (service.store_status_backfill_marker_cache.markers) |marker| {
        try std.testing.expect(!try backfillMarkerStateFileExists(std.testing.allocator, replica_root, marker));
    }
    try maybeRequestStoreStatusBackfillMarkerRescan(
        &service,
        replica_root,
        service.store_status_backfill_marker_cache.markers,
        service.store_status_backfill_marker_cache.markers,
    );
    try std.testing.expect(service.store_status_backfill_marker_cache.rescan_requested);
    var probe_ticks: usize = 0;
    try maybeRefreshStoreStatusBackfillMarkerCache(
        std.testing.allocator,
        replica_root,
        0,
        &probe_ticks,
        &service.store_status_backfill_marker_cache,
    );
    try std.testing.expectEqual(@as(usize, 0), service.store_status_backfill_marker_cache.markers.len);
}

test "metadata service caches corrupt rebuild state and publishes quarantine status" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-corrupt-backfill-marker-cache", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);
    const index_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9501/table-db/indexes/search_idx", .{replica_root});
    defer std.testing.allocator.free(index_root);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), index_root);
    const rebuild_state = backfill_state_mod.RebuildState.initOwned(index_root, null, 95);
    try rebuild_state.updateWithIo(io_impl.io(), "doc:m");
    const state_path = try rebuild_state.pathAlloc(std.testing.allocator);
    defer std.testing.allocator.free(state_path);
    const encoded = try std.Io.Dir.cwd().readFileAlloc(
        io_impl.io(),
        state_path,
        std.testing.allocator,
        .limited(64 * 1024 + 64),
    );
    defer std.testing.allocator.free(encoded);
    encoded[encoded.len - 1] ^= 0xff;
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{ .sub_path = state_path, .data = encoded });

    var cache = StoreStatusBackfillMarkerCache{};
    defer cache.deinit(std.testing.allocator);
    var probe_ticks: usize = 0;
    try maybeRefreshStoreStatusBackfillMarkerCache(
        std.testing.allocator,
        replica_root,
        0,
        &probe_ticks,
        &cache,
    );
    try std.testing.expectEqual(@as(usize, 1), cache.markers.len);
    try std.testing.expect(cache.markers[0].state == .corrupt);
    try std.testing.expect(!cache.rescan_requested);

    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 9501, .table_id = 95, .start_key = "doc:a", .end_key = "doc:z" },
        .{ .group_id = 9502, .table_id = 96, .start_key = "doc:a", .end_key = "doc:z" },
    };
    var group_indexes = [_]metadata_table_manager.RuntimeIndexStatusReport{
        .{ .name = "search_idx", .kind = "full_text", .doc_count = 7 },
        .{ .name = "healthy_peer_idx", .kind = "graph", .edge_count = 11 },
    };
    var unrelated_indexes = [_]metadata_table_manager.RuntimeIndexStatusReport{
        .{ .name = "unrelated_idx", .kind = "sparse_vector", .doc_count = 13 },
    };
    var existing_runtime_statuses = [_]metadata_table_manager.RuntimeGroupStatusReport{
        .{
            .table_id = 95,
            .table_name = "docs",
            .group_id = 9501,
            .store_id = 95,
            .node_id = 5,
            .source = "live_writer_publish",
            .freshness = "fresh",
            .index_count = 2,
            .indexes = &group_indexes,
        },
        .{
            .table_id = 96,
            .table_name = "other",
            .group_id = 9502,
            .store_id = 95,
            .node_id = 5,
            .source = "live_writer_publish",
            .freshness = "fresh",
            .index_count = 1,
            .indexes = &unrelated_indexes,
        },
    };
    const report = try collectLocalStoreStatusReport(
        .{},
        std.testing.allocator,
        replica_root,
        .{ .store_id = 95, .node_id = 5, .runtime_statuses = &existing_runtime_statuses },
        &ranges,
        &.{},
        cache.markers,
    );
    defer freeOwnedStoreStatusReport(std.testing.allocator, report);
    try std.testing.expectEqual(@as(u32, 1), report.active_backfills);
    try std.testing.expectEqual(@as(u16, 0), report.backfill_progress_millis);
    try std.testing.expectEqual(@as(usize, 3), report.runtime_statuses.len);
    try std.testing.expectEqualStrings("rebuild_state_quarantine", report.runtime_statuses[0].source);
    try std.testing.expectEqualStrings("fresh", report.runtime_statuses[0].freshness);
    try std.testing.expectEqual(@as(usize, 2), report.runtime_statuses[0].indexes.len);
    try std.testing.expectEqualStrings("search_idx", report.runtime_statuses[0].indexes[0].name);
    try std.testing.expectEqualStrings(rebuild_state_quarantine_error, report.runtime_statuses[0].indexes[0].load_error.?);
    try std.testing.expect(report.runtime_statuses[0].indexes[1].load_error == null);
    try std.testing.expectEqualStrings("live_writer_publish", report.runtime_statuses[1].source);
    try std.testing.expect(report.runtime_statuses[1].indexes[0].load_error == null);
    try std.testing.expectEqual(@as(u64, 9502), report.runtime_statuses[2].group_id);
    try std.testing.expectEqualStrings("unrelated_idx", report.runtime_statuses[2].indexes[0].name);

    const repaired = try collectStoreRuntimeStatusesWithQuarantinedBackfills(
        std.testing.allocator,
        report.runtime_statuses,
        &ranges,
        &.{},
        95,
        5,
        true,
    );
    defer metadata_table_manager.freeRuntimeGroupStatusReports(std.testing.allocator, repaired);
    try std.testing.expectEqual(@as(usize, 2), repaired.len);
    try std.testing.expectEqualStrings("live_writer_publish", repaired[0].source);
    try std.testing.expect(repaired[0].indexes[0].load_error == null);
    try std.testing.expectEqual(@as(u64, 9502), repaired[1].group_id);

    cache.scanned_at_ms = monotonicMs();
    const scanned_at_ms = cache.scanned_at_ms;
    try rebuild_state.updateWithIo(io_impl.io(), "doc:z");
    try maybeRefreshStoreStatusBackfillMarkerCache(
        std.testing.allocator,
        replica_root,
        0,
        &probe_ticks,
        &cache,
    );
    try std.testing.expectEqual(scanned_at_ms, cache.scanned_at_ms);
    try std.testing.expect(cache.markers[0].state == .corrupt);
}

test "metadata service does not rescan empty backfill markers before idle interval" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-empty-backfill-marker-cache", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), replica_root);

    var cache = StoreStatusBackfillMarkerCache{};
    defer cache.deinit(std.testing.allocator);
    cache.scanned_at_ms = monotonicMs();
    const scanned_at_ms = cache.scanned_at_ms;

    var probe_ticks: usize = store_status_backfill_probe_interval_ticks;
    try maybeRefreshStoreStatusBackfillMarkerCache(
        std.testing.allocator,
        replica_root,
        40,
        &probe_ticks,
        &cache,
    );

    try std.testing.expectEqual(@as(usize, 0), cache.markers.len);
    try std.testing.expectEqual(scanned_at_ms, cache.scanned_at_ms);
    try std.testing.expectEqual(store_status_backfill_probe_interval_ticks, probe_ticks);
}

test "metadata service prefers planned store affinity in shared roots" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const cwd = try std.process.currentPathAlloc(std.testing.io, std.testing.allocator);
    defer std.testing.allocator.free(cwd);
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, "{s}/.zig-cache/tmp/{s}/metadata-auto-store-status-planned-root", .{ cwd, tmp.sub_path });
    defer std.testing.allocator.free(replica_root);

    const db_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9201/table-db", .{replica_root});
    defer std.testing.allocator.free(db_path);
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), db_path);

    {
        var db = try db_mod.DB.open(std.testing.allocator, db_path, .{});
        defer db.close();
        try db.addIndex(.{
            .name = "search_idx",
            .kind = .full_text,
            .config_json = "{}",
        });
    }

    const state_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/indexes/search_idx/rebuild.state", .{db_path});
    defer std.testing.allocator.free(state_path);
    try rebuildStateForPath(state_path).update("doc:m");

    const stores = [_]metadata_table_manager.StoreRecord{
        .{ .store_id = 91, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 880 },
        .{ .store_id = 92, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 980 },
    };
    const placements = [_]raft_reconciler.PlacementIntent{
        .{
            .record = .{
                .group_id = 9201,
                .replica_id = 1,
                .local_node_id = 1,
                .bootstrap_mode = .persisted,
            },
            .store_id = 92,
            .peer_node_ids = &.{1},
        },
    };
    const tables = [_]metadata_table_manager.TableRecord{
        .{ .table_id = 92, .name = "docs", .desired_replica_count = 1, .min_ranges = 1 },
    };
    const ranges = [_]metadata_table_manager.RangeRecord{
        .{ .group_id = 9201, .table_id = 92, .start_key = "doc:a", .end_key = "doc:z" },
    };
    const markers = try collectStoreStatusBackfillMarkers(std.testing.allocator, replica_root);
    defer freeStoreStatusBackfillMarkers(std.testing.allocator, markers);

    const projected = try collectSharedRootLocalStoreStatusReports(
        .{},
        std.testing.allocator,
        std.testing.io,
        replica_root,
        1,
        stores[0..],
        placements[0..],
        tables[0..],
        ranges[0..],
        &.{},
        markers,
    );
    defer freeOwnedStoreStatusReports(std.testing.allocator, projected);
    const first_index = findStoreStatusReportIndex(projected, 91).?;
    const second_index = findStoreStatusReportIndex(projected, 92).?;
    try std.testing.expectEqual(@as(u32, 0), projected[first_index].active_backfills);
    try std.testing.expectEqual(@as(u32, 1), projected[second_index].active_backfills);

    const affinity_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/group-9201/store-affinity", .{replica_root});
    defer std.testing.allocator.free(affinity_path);
    const affinity_contents = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), affinity_path, std.testing.allocator, .limited(128));
    defer std.testing.allocator.free(affinity_contents);
    const affinity_store_id = try std.fmt.parseInt(u64, std.mem.trim(u8, affinity_contents, " \t\r\n"), 10);
    try std.testing.expectEqual(@as(u64, 92), affinity_store_id);
}

test "metadata service status reports repair and rebalance counts" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-status-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-status-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1972,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1972,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();

    try svc.upsertNode(.{ .node_id = 1 });
    try svc.upsertNode(.{ .node_id = 2 });
    try svc.upsertStore(.{ .store_id = 31, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 32, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try svc.runRound();

    try svc.upsertTable(.{
        .table_id = 88,
        .name = "docs",
        .replication_sources_json = "[{\"type\":\"postgres\",\"dsn\":\"postgres://db\",\"postgres_table\":\"users\"}]",
        .desired_replica_count = 3,
        .min_ranges = 1,
    });
    try svc.upsertRange(.{
        .group_id = 8801,
        .table_id = 88,
        .start_key = "doc:a",
        .end_key = "doc:z",
    });
    try svc.upsertReplicationSourceStatus(.{
        .table_id = 88,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .phase = "snapshot",
        .checkpoint = "lsn:0/16B6A50",
        .cutover_mode = "slot_first",
        .lag_records = 12,
        .updated_at_ms = 500,
    });
    try svc.runRound();

    const repair_status = try svc.metadataStatus();
    try std.testing.expectEqual(@as(u64, 1972), repair_status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_tables);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_tables_with_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_source_statuses);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_exact_cutover);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_source_statuses_non_exact_cutover);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_reseed_recommended);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_exported_snapshot);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_source_statuses_slot_first);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_slot_resumed);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_replication_source_statuses_snapshot);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_cutover_prepared);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_streaming);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_failed);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_last_error);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_slot_missing_failed);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_retryable_failed);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_terminal_failed);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_consecutive_failures);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_success_timestamp);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_change_timestamp);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_consecutive_failures_total);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_consecutive_failures_max);
    try std.testing.expectEqual(@as(u64, 12), repair_status.projected_replication_source_lag_records_total);
    try std.testing.expectEqual(@as(u64, 12), repair_status.projected_replication_source_lag_records_max);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_lag_millis_max);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_observed_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_observed_lag_millis_max);
    try std.testing.expectEqual(@as(usize, 0), repair_status.projected_replication_source_statuses_with_source_commit_timestamp);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_last_success_at_ms_max);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_last_source_commit_at_ms_max);
    try std.testing.expectEqual(@as(u64, 0), repair_status.projected_replication_source_last_change_applied_at_ms_max);
    try std.testing.expectEqual(@as(usize, 1), repair_status.projected_ranges);
    try std.testing.expectEqual(@as(usize, 2), repair_status.projected_stores);
    try std.testing.expectEqual(@as(usize, 0), repair_status.rebalance_placement_groups);
    try std.testing.expectEqual(@as(usize, 1), repair_status.repair_placement_groups);

    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 8801,
            .replica_id = 1,
            .local_node_id = 1,
            .bootstrap_mode = .fetch_snapshot,
            .snapshot_bootstrap = .{
                .from_node_id = 41,
                .term = 7,
                .snapshot_id = "snap-8801",
                .uri = "http://node-41/snapshots/snap-8801",
            },
        },
        .peer_node_ids = &.{2},
    }, null, 0, false);
    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 8801,
            .replica_id = 2,
            .local_node_id = 2,
            .bootstrap_mode = .fetch_snapshot,
            .backup_restore_bootstrap = .{
                .backup_id = "backup-8801",
                .artifact_backup_id = "backup-8801",
                .location = "file:///tmp/backups",
                .snapshot_path = "backup-8801/groups/8801",
                .connection = "backup-store",
                .artifact_size_bytes = 4096,
                .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
        },
        .peer_node_ids = &.{1},
    }, null, 0, false);
    try svc.runRound();

    try svc.reportStoreStatus(.{
        .store_id = 31,
        .live = true,
        .health_class = "healthy",
        .capacity_bytes = 1024,
        .available_bytes = 900,
        .lease_pressure = 98,
        .read_load = 220,
        .write_load = 160,
    });
    try svc.upsertStore(.{ .store_id = 33, .node_id = 3, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 950 });
    try svc.upsertReplicationSourceStatus(.{
        .table_id = 88,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .phase = "streaming_failed",
        .checkpoint = "lsn:0/16B6A90",
        .cutover_mode = "slot_first",
        .last_error = "network timeout",
        .failure_class = "retryable",
        .lag_records = 7,
        .lag_millis = 45,
        .consecutive_failures = 3,
        .last_source_commit_at_ms = 555,
        .last_success_at_ms = 600,
        .last_change_applied_at_ms = 650,
        .updated_at_ms = 700,
    });
    try svc.runRound();

    const rebalance_status = try svc.metadataStatus();
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.overloaded_stores);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.repair_placement_groups);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.rebalance_placement_groups);
    try std.testing.expectEqual(@as(usize, 3), rebalance_status.placement_upserts);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.placement_removals);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_snapshot_bootstrap_intents);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_backup_restore_bootstrap_intents);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_tables_with_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_sources);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_exact_cutover);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_non_exact_cutover);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_reseed_recommended);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_exported_snapshot);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_slot_first);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_slot_resumed);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_snapshot);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_cutover_prepared);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_streaming);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_failed);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_last_error);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_slot_missing_failed);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_retryable_failed);
    try std.testing.expectEqual(@as(usize, 0), rebalance_status.projected_replication_source_statuses_terminal_failed);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_consecutive_failures);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_success_timestamp);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_change_timestamp);
    try std.testing.expectEqual(@as(u64, 3), rebalance_status.projected_replication_source_consecutive_failures_total);
    try std.testing.expectEqual(@as(u64, 3), rebalance_status.projected_replication_source_consecutive_failures_max);
    try std.testing.expectEqual(@as(u64, 7), rebalance_status.projected_replication_source_lag_records_total);
    try std.testing.expectEqual(@as(u64, 7), rebalance_status.projected_replication_source_lag_records_max);
    try std.testing.expectEqual(@as(u64, 45), rebalance_status.projected_replication_source_lag_millis_total);
    try std.testing.expectEqual(@as(u64, 45), rebalance_status.projected_replication_source_lag_millis_max);
    try std.testing.expect(rebalance_status.projected_replication_source_observed_lag_millis_total >= rebalance_status.projected_replication_source_lag_millis_total);
    try std.testing.expect(rebalance_status.projected_replication_source_observed_lag_millis_max >= rebalance_status.projected_replication_source_lag_millis_max);
    try std.testing.expectEqual(@as(usize, 1), rebalance_status.projected_replication_source_statuses_with_source_commit_timestamp);
    try std.testing.expectEqual(@as(u64, 600), rebalance_status.projected_replication_source_last_success_at_ms_max);
    try std.testing.expectEqual(@as(u64, 555), rebalance_status.projected_replication_source_last_source_commit_at_ms_max);
    try std.testing.expectEqual(@as(u64, 650), rebalance_status.projected_replication_source_last_change_applied_at_ms_max);
}

test "metadata service admin snapshot captures projected topology and status" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-admin-snapshot-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-admin-snapshot-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 1973,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 1973,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();
    try svc.runRound();
    try svc.ensureLinearizableRead();

    try svc.upsertStore(.{ .store_id = 41, .node_id = 1, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 900 });
    try svc.upsertStore(.{ .store_id = 42, .node_id = 2, .role = "data", .live = true, .capacity_bytes = 1024, .available_bytes = 850 });
    try svc.upsertTable(.{ .table_id = 89, .name = "docs", .desired_replica_count = 3, .min_ranges = 1 });
    try svc.upsertRange(.{ .group_id = 8901, .table_id = 89, .start_key = "doc:a", .end_key = "doc:z" });
    try svc.upsertReplicationSourceStatus(.{
        .table_id = 89,
        .source_ordinal = 0,
        .source_kind = "postgres",
        .external_table = "users",
        .cutover_mode = "exported_snapshot",
        .slot_name = "antfly_postgres_users_docs",
        .publication_name = "antfly_pub_postgres_users_docs",
        .phase = "streaming",
        .checkpoint = "lsn:0/16B6B10",
        .snapshot_offset = 2,
        .prepared_checkpoint = "lsn:0/16B6A50",
        .stream_checkpoint = "lsn:0/16B6B10",
        .lag_records = 3,
        .updated_at_ms = 777,
    });
    try svc.runRound();

    var snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&snapshot);

    try std.testing.expectEqual(@as(u64, 1973), snapshot.status.metadata_group_id);
    try std.testing.expectEqual(@as(usize, 1), snapshot.tables.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.ranges.len);
    try std.testing.expectEqual(@as(usize, 2), snapshot.stores.len);
    try std.testing.expectEqual(@as(usize, 0), snapshot.placement_intents.len);
    try std.testing.expectEqual(@as(usize, 1), snapshot.replication_source_statuses.len);
    try std.testing.expectEqualStrings("postgres", snapshot.replication_source_statuses[0].source_kind);
    try std.testing.expectEqualStrings("users", snapshot.replication_source_statuses[0].external_table);
    try std.testing.expectEqualStrings("exported_snapshot", snapshot.replication_source_statuses[0].cutover_mode);
    try std.testing.expectEqualStrings("antfly_postgres_users_docs", snapshot.replication_source_statuses[0].slot_name);
    try std.testing.expectEqualStrings("antfly_pub_postgres_users_docs", snapshot.replication_source_statuses[0].publication_name);
    try std.testing.expectEqual(@as(u64, 2), snapshot.replication_source_statuses[0].snapshot_offset);
    try std.testing.expectEqualStrings("lsn:0/16B6A50", snapshot.replication_source_statuses[0].prepared_checkpoint);
    try std.testing.expectEqualStrings("lsn:0/16B6B10", snapshot.replication_source_statuses[0].stream_checkpoint);
    try std.testing.expectEqual(@as(usize, 1), snapshot.status.repair_placement_groups);
}

test "metadata service committed metadata changes request lifecycle reconcile hook" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const HookCapture = struct {
        calls: usize = 0,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-lifecycle-hook-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-lifecycle-hook-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 2048,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2048,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();

    var capture = HookCapture{};
    svc.setLifecycleReconcileHook(.{
        .ptr = &capture,
        .vtable = &.{
            .run = HookCapture.run,
        },
    });

    try runServiceRounds(&svc, 8);
    try std.testing.expect(capture.calls >= 1);

    capture.calls = 0;
    try svc.upsertTable(.{ .table_id = 99, .name = "docs" });
    try svc.upsertRange(.{ .group_id = 9901, .table_id = 99, .start_key = "", .end_key = null });
    try runServiceRounds(&svc, 8);
    try std.testing.expect(capture.calls >= 1);

    capture.calls = 0;
    try svc.upsertNode(.{ .node_id = 1 });
    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 9901,
            .local_node_id = 1,
            .replica_id = 1,
        },
        .store_id = 0,
        .peer_node_ids = &.{},
    }, null, 0, false);
    try svc.requestReallocation(1);
    try runServiceRounds(&svc, 8);
    try std.testing.expect(capture.calls >= 1);
}

test "linearizable metadata read tracker completes only matching request" {
    var tracker = LinearizableMetadataReadTracker{
        .alloc = std.testing.allocator,
        .metadata_group_id = 42,
    };
    defer tracker.deinit();

    const first_id = try tracker.registerRequest();
    const second_id = try tracker.registerRequest();

    const second_ctx = try std.fmt.allocPrint(std.testing.allocator, "{s}{d}", .{ linearizable_metadata_read_prefix, second_id });
    defer std.testing.allocator.free(second_ctx);

    try tracker.observer().onReadStates(43, &.{.{
        .index = 1,
        .request_ctx = second_ctx,
    }});
    try std.testing.expect(!tracker.isComplete(first_id));
    try std.testing.expect(!tracker.isComplete(second_id));

    try tracker.observer().onReadStates(42, &.{.{
        .index = 1,
        .request_ctx = second_ctx,
    }});

    try std.testing.expect(!tracker.isComplete(first_id));
    try std.testing.expect(tracker.isComplete(second_id));

    tracker.finishRequest(first_id);
    tracker.finishRequest(second_id);
}

test "metadata service clears restore intent once all placement replicas report restore progress" {
    const FakeService = struct {
        alloc: std.mem.Allocator,
        table: metadata_table_manager.TableRecord,
        ranges: []const metadata_table_manager.RangeRecord,
        placements: []const raft_reconciler.PlacementIntent,
        progress: []const metadata_table_manager.RestoreProgressRecord,
        upserted_table: ?metadata_table_manager.TableRecord = null,
        completed_restore: ?metadata_table_manager.RestoreIntentIdentity = null,

        fn deinit(self: *@This()) void {
            if (self.upserted_table) |record| metadata_table_manager.freeTable(self.alloc, record);
            if (self.completed_restore) |identity| metadata_table_manager.freeRestoreIntentIdentity(self.alloc, identity);
        }

        fn listProjectedTables(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.TableRecord {
            const out = try alloc.alloc(metadata_table_manager.TableRecord, 1);
            out[0] = try metadata_table_manager.cloneTable(alloc, self.table);
            return out;
        }

        fn freeProjectedTables(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.TableRecord) void {
            for (records) |record| metadata_table_manager.freeTable(alloc, record);
            alloc.free(records);
        }

        fn listProjectedRanges(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RangeRecord {
            const out = try alloc.alloc(metadata_table_manager.RangeRecord, self.ranges.len);
            for (self.ranges, 0..) |record, i| out[i] = try metadata_table_manager.cloneRange(alloc, record);
            return out;
        }

        fn freeProjectedRanges(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RangeRecord) void {
            for (records) |record| metadata_table_manager.freeRange(alloc, record);
            alloc.free(records);
        }

        fn listProjectedPlacementIntents(self: *@This(), alloc: std.mem.Allocator) ![]raft_reconciler.PlacementIntent {
            const out = try alloc.alloc(raft_reconciler.PlacementIntent, self.placements.len);
            for (self.placements, 0..) |intent, i| {
                out[i] = .{
                    .record = intent.record,
                    .store_id = intent.store_id,
                    .peer_node_ids = if (intent.peer_node_ids.len == 0) &.{} else try alloc.dupe(u64, intent.peer_node_ids),
                };
            }
            return out;
        }

        fn freeProjectedPlacementIntents(_: *@This(), alloc: std.mem.Allocator, records: []raft_reconciler.PlacementIntent) void {
            for (records) |intent| if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            alloc.free(records);
        }

        fn listProjectedRestoreProgress(self: *@This(), alloc: std.mem.Allocator) ![]metadata_table_manager.RestoreProgressRecord {
            const out = try alloc.alloc(metadata_table_manager.RestoreProgressRecord, self.progress.len);
            for (self.progress, 0..) |record, i| out[i] = try metadata_table_manager.cloneRestoreProgress(alloc, record);
            return out;
        }

        fn freeProjectedRestoreProgress(_: *@This(), alloc: std.mem.Allocator, records: []metadata_table_manager.RestoreProgressRecord) void {
            for (records) |record| metadata_table_manager.freeRestoreProgress(alloc, record);
            alloc.free(records);
        }

        fn upsertTable(self: *@This(), record: metadata_table_manager.TableRecord) !void {
            if (self.upserted_table) |existing| metadata_table_manager.freeTable(self.alloc, existing);
            self.upserted_table = try metadata_table_manager.cloneTable(self.alloc, record);
        }

        fn completeRestoreRange(self: *@This(), identity: metadata_table_manager.RestoreIntentIdentity) !void {
            if (self.completed_restore) |existing| metadata_table_manager.freeRestoreIntentIdentity(self.alloc, existing);
            self.completed_restore = try metadata_table_manager.cloneRestoreIntentIdentity(self.alloc, identity);
        }
    };

    var service = FakeService{
        .alloc = std.testing.allocator,
        .table = .{
            .table_id = 7,
            .name = "docs",
        },
        .ranges = &.{
            .{
                .group_id = 7001,
                .table_id = 7,
                .start_key = "",
                .end_key = null,
                .restore_backup_id = "snap1",
                .restore_artifact_backup_id = "snap1-artifacts",
                .restore_location = "file:///tmp/backups",
                .restore_snapshot_path = "snap/groups/7001",
                .restore_connection = "backups",
                .restore_artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            },
        },
        .placements = &.{
            .{ .record = .{ .group_id = 7001, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 0, .peer_node_ids = &.{ 1, 2 } },
            .{ .record = .{ .group_id = 7001, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 0, .peer_node_ids = &.{ 1, 2 } },
        },
        .progress = &.{
            .{ .table_id = 7, .node_id = 1, .group_id = 7001, .backup_id = "snap1", .artifact_backup_id = "snap1-artifacts", .location = "file:///tmp/backups", .snapshot_path = "snap/groups/7001", .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
            .{ .table_id = 7, .node_id = 2, .group_id = 7001, .backup_id = "snap1", .artifact_backup_id = "snap1-artifacts", .location = "file:///tmp/backups", .snapshot_path = "snap/groups/7001", .artifact_sha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
        },
    };
    defer service.deinit();

    try completeRestoreIntentsForService(&service, null, null, null, null);
    try std.testing.expect(service.completed_restore != null);
    try std.testing.expectEqualStrings("snap1", service.completed_restore.?.backup_id);
    try std.testing.expectEqualStrings("file:///tmp/backups", service.completed_restore.?.location);
    try std.testing.expectEqualStrings("snap/groups/7001", service.completed_restore.?.snapshot_path);
    try std.testing.expectEqualStrings("backups", service.completed_restore.?.connection);
    try std.testing.expect(service.upserted_table == null);
}

test "metadata service keeps restore intent until runtime repair completes" {
    const placements = [_]raft_reconciler.PlacementIntent{
        .{ .record = .{ .group_id = 7001, .replica_id = 1, .local_node_id = 1, .bootstrap_mode = .persisted }, .store_id = 0, .peer_node_ids = &.{ 1, 2 } },
        .{ .record = .{ .group_id = 7001, .replica_id = 2, .local_node_id = 2, .bootstrap_mode = .persisted }, .store_id = 0, .peer_node_ids = &.{ 1, 2 } },
    };
    const progress = [_]metadata_table_manager.RestoreProgressRecord{
        .{ .table_id = 7, .node_id = 1, .group_id = 7001, .backup_id = "snap1", .artifact_backup_id = "snap1-artifacts", .location = "file:///tmp/backups", .snapshot_path = "snap/groups/7001", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
        .{ .table_id = 7, .node_id = 2, .group_id = 7001, .backup_id = "snap1", .artifact_backup_id = "snap1-artifacts", .location = "file:///tmp/backups", .snapshot_path = "snap/groups/7001", .primary_restored = true, .runtime_repair_complete = false, .phase = "runtime_repair" },
    };

    try std.testing.expect(!rangeRestoreIntentComplete(7, .{
        .group_id = 7001,
        .table_id = 7,
        .start_key = "",
        .restore_artifact_backup_id = "snap1-artifacts",
        .restore_snapshot_path = "snap/groups/7001",
    }, "snap1", "file:///tmp/backups", &placements, &progress));

    const stale_progress = [_]metadata_table_manager.RestoreProgressRecord{
        .{ .table_id = 7, .node_id = 1, .group_id = 7001, .backup_id = "snap1", .artifact_backup_id = "snap1-artifacts", .location = "file:///tmp/backups", .snapshot_path = "snap/groups/7001", .artifact_sha256 = "old-artifact", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
        .{ .table_id = 7, .node_id = 2, .group_id = 7001, .backup_id = "snap1", .artifact_backup_id = "snap1-artifacts", .location = "file:///tmp/backups", .snapshot_path = "snap/groups/7001", .artifact_sha256 = "old-artifact", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
    };
    try std.testing.expect(!rangeRestoreIntentComplete(7, .{
        .group_id = 7001,
        .table_id = 7,
        .start_key = "",
        .restore_artifact_backup_id = "snap1-artifacts",
        .restore_snapshot_path = "snap/groups/7001",
        .restore_artifact_sha256 = "new-artifact",
    }, "snap1", "file:///tmp/backups", &placements, &stale_progress));

    const stale_namespace_progress = [_]metadata_table_manager.RestoreProgressRecord{
        .{ .table_id = 7, .node_id = 1, .group_id = 7001, .backup_id = "snap1", .artifact_backup_id = "old-artifacts", .location = "file:///tmp/backups", .snapshot_path = "snap/groups/7001", .artifact_sha256 = "new-artifact", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
        .{ .table_id = 7, .node_id = 2, .group_id = 7001, .backup_id = "snap1", .artifact_backup_id = "old-artifacts", .location = "file:///tmp/backups", .snapshot_path = "snap/groups/7001", .artifact_sha256 = "new-artifact", .primary_restored = true, .runtime_repair_complete = true, .phase = "complete" },
    };
    try std.testing.expect(!rangeRestoreIntentComplete(7, .{
        .group_id = 7001,
        .table_id = 7,
        .start_key = "",
        .restore_artifact_backup_id = "new-artifacts",
        .restore_snapshot_path = "snap/groups/7001",
        .restore_artifact_sha256 = "new-artifact",
    }, "snap1", "file:///tmp/backups", &placements, &stale_namespace_progress));
}

test "metadata http service catalog cache is independent from volatile projection traffic" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataHttpService.init(std.testing.allocator, .{
        .http = .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 2900,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
            .transport = .{
                .snapshot = .{ .root_dir = snapshot_root },
            },
        },
    }, .{
        .http = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory.iface(),
                },
            },
        },
    }, .{
        .observe_local_replica_root = false,
    });
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2900,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();

    try std.testing.expectEqual(false, svc.lifecycle_listener_registered);

    const before = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, before);
    try std.testing.expectEqual(@as(usize, 0), before.len);
    try std.testing.expectEqual(true, svc.lifecycle_listener_registered);
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{
        .upsert_node = .{ .node_id = 77 },
    });
    try std.testing.expect(receipt.term > 0);
    try std.testing.expect(receipt.index > 0);
    try svc.waitForTransitionApplied(receipt);
    const receipt_status = svc.raft.host.http_host.host.raftStatus(2900) orelse return error.MissingRaftStatus;
    try std.testing.expect(receipt_status.applied_index >= receipt.index);
    const receipt_nodes = try svc.listProjectedNodes(std.testing.allocator);
    defer svc.freeProjectedNodes(std.testing.allocator, receipt_nodes);
    var found_receipt_node = false;
    for (receipt_nodes) |node| {
        if (node.node_id == 77) found_receipt_node = true;
    }
    try std.testing.expect(found_receipt_node);
    const catalog_epoch_before = svc.catalog_epoch.load(.acquire);
    try std.testing.expectEqual(catalog_epoch_before, svc.catalog_validation_cache.catalog_epoch);
    try std.testing.expect(svc.projected_core_snapshot_cache.snapshot == null);

    const stores_before = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, stores_before);
    const volatile_epoch_before = svc.projection_epoch.load(.monotonic);
    const core_epoch_before = svc.projected_core_epoch.load(.acquire);
    try std.testing.expectEqual(core_epoch_before, svc.projected_core_snapshot_cache.core_epoch);

    MetadataHttpService.metadataHttpServiceProjectionSignal(&svc, .{
        .kind = .store,
        .metadata_group_id = 2900,
        .store_id = 41,
    });
    const volatile_epoch_after = svc.projection_epoch.load(.monotonic);
    const core_epoch_after = svc.projected_core_epoch.load(.acquire);
    try std.testing.expect(volatile_epoch_after > volatile_epoch_before);
    try std.testing.expect(core_epoch_after > core_epoch_before);
    try std.testing.expectEqual(catalog_epoch_before, svc.catalog_epoch.load(.acquire));
    try std.testing.expectEqual(catalog_epoch_before, svc.catalog_validation_cache.catalog_epoch);

    const after_store_signal = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, after_store_signal);
    try std.testing.expectEqual(@as(usize, 0), after_store_signal.len);
    try std.testing.expectEqual(catalog_epoch_before, svc.catalog_validation_cache.catalog_epoch);
    try std.testing.expect(svc.projected_core_snapshot_cache.core_epoch < core_epoch_after);

    const stores_after = try svc.listProjectedStores(std.testing.allocator);
    defer svc.freeProjectedStores(std.testing.allocator, stores_after);
    try std.testing.expectEqual(core_epoch_after, svc.projected_core_snapshot_cache.core_epoch);

    MetadataHttpService.metadataHttpServiceProjectionSignal(&svc, .{
        .kind = .table,
        .metadata_group_id = 2900,
        .table_name = "docs",
        .table_id = 77,
    });
    const catalog_epoch_after = svc.catalog_epoch.load(.acquire);
    try std.testing.expect(catalog_epoch_after > catalog_epoch_before);
    try std.testing.expect(svc.catalog_validation_cache.catalog_epoch < catalog_epoch_after);
    try std.testing.expectEqual(core_epoch_after, svc.projected_core_epoch.load(.acquire));
    try std.testing.expectEqual(core_epoch_after, svc.projected_core_snapshot_cache.core_epoch);

    const after = try svc.listProjectedTables(std.testing.allocator);
    defer svc.freeProjectedTables(std.testing.allocator, after);
    try std.testing.expectEqual(@as(usize, 0), after.len);
    try std.testing.expectEqual(catalog_epoch_after, svc.catalog_validation_cache.catalog_epoch);

    var incarnation_rounds: usize = 0;
    while (try svc.metadataIncarnation() == null and incarnation_rounds < 32) : (incarnation_rounds += 1) {
        try svc.runRound();
    }
    try std.testing.expect(try svc.metadataIncarnation() != null);
    try svc.requestReallocation(77_000);
    try svc.runRound();
    var admin_snapshot = try svc.adminSnapshot();
    defer svc.freeAdminSnapshot(&admin_snapshot);
    try std.testing.expect(admin_snapshot.reallocation_request != null);
    try std.testing.expectEqual(@as(u64, 77_000), admin_snapshot.reallocation_request.?.requested_at_ms);
    try std.testing.expectEqual(
        metadata_reallocation_request.barrier_protocol_version,
        admin_snapshot.reallocation_request.?.barrier_protocol_version,
    );
    try std.testing.expectEqual(try svc.metadataIncarnation(), admin_snapshot.reallocation_request.?.metadata_incarnation);

    // A full status refresh may fail while cached projection data is still
    // useful. Poison the cached Raft fields, force the projected store lookup
    // to fail, and prove the fallback overlays the current in-memory topology.
    const live_raft = serviceGroupRaftTopologyObservation(&svc, 2900);
    try std.testing.expect(live_raft.voter_set_fingerprint != null);
    var stale_status = try svc.status();
    stale_status.metadata_raft_local_node_id = 99;
    stale_status.metadata_raft_role = "leader";
    stale_status.metadata_raft_leader_id = 99;
    stale_status.metadata_raft_term += 100;
    stale_status.metadata_raft_commit_index += 100;
    stale_status.metadata_raft_local_voter = false;
    stale_status.metadata_raft_voter_count = 99;
    stale_status.metadata_raft_voter_set_fingerprint = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff".*;
    stale_status.metadata_raft_joint_consensus = true;
    stale_status.metadata_raft_learner_count = 99;
    stale_status.metadata_raft_election_elapsed += 100;
    stale_status.metadata_raft_randomized_election_timeout += 100;
    stale_status.metadata_raft_votes_granted += 100;
    stale_status.metadata_raft_votes_rejected += 100;
    stale_status.metadata_raft_votes_unknown += 100;
    svc.storeMetadataStatusCache(stale_status, nowMs() + metadata_status_cache_refresh_interval_ms);

    const fallback_status = blk: {
        const projected_store = svc.raft.host.owned_metadata_store orelse return error.MissingMetadataStore;
        svc.raft.host.owned_metadata_store = null;
        defer svc.raft.host.owned_metadata_store = projected_store;
        break :blk try svc.status();
    };
    try std.testing.expectEqual(live_raft.local_node_id, fallback_status.metadata_raft_local_node_id);
    try std.testing.expectEqualStrings(live_raft.role, fallback_status.metadata_raft_role);
    try std.testing.expectEqual(live_raft.leader_id, fallback_status.metadata_raft_leader_id);
    try std.testing.expectEqual(live_raft.term, fallback_status.metadata_raft_term);
    try std.testing.expectEqual(live_raft.commit_index, fallback_status.metadata_raft_commit_index);
    try std.testing.expectEqual(live_raft.local_voter, fallback_status.metadata_raft_local_voter);
    try std.testing.expectEqual(live_raft.voter_count, fallback_status.metadata_raft_voter_count);
    try std.testing.expectEqual(live_raft.voter_set_fingerprint, fallback_status.metadata_raft_voter_set_fingerprint);
    try std.testing.expectEqual(live_raft.joint_consensus, fallback_status.metadata_raft_joint_consensus);
    try std.testing.expectEqual(live_raft.learner_count, fallback_status.metadata_raft_learner_count);
    try std.testing.expectEqual(live_raft.election_elapsed, fallback_status.metadata_raft_election_elapsed);
    try std.testing.expectEqual(live_raft.randomized_election_timeout, fallback_status.metadata_raft_randomized_election_timeout);
    try std.testing.expectEqual(live_raft.votes_granted, fallback_status.metadata_raft_votes_granted);
    try std.testing.expectEqual(live_raft.votes_rejected, fallback_status.metadata_raft_votes_rejected);
    try std.testing.expectEqual(live_raft.votes_unknown, fallback_status.metadata_raft_votes_unknown);
}

test "metadata http service linearizable read waits for leader discovery" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-read-barrier-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-read-barrier-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);
    const snapshot_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-http-service-read-barrier-snapshots", .{tmp.sub_path});
    defer std.testing.allocator.free(snapshot_root);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataHttpService.init(std.testing.allocator, .{
        .http = .{
            .host = .{
                .local_node_id = 1,
                .metadata_group_id = 2910,
                .replica_root_dir = replica_root,
                .replica_catalog_path = replica_catalog_path,
            },
            .transport = .{
                .snapshot = .{ .root_dir = snapshot_root },
            },
        },
    }, .{
        .http = .{
            .http = .{
                .host = .{
                    .descriptor_factory = factory.iface(),
                },
            },
        },
    }, .{
        .observe_local_replica_root = false,
    });
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 2910,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });

    try std.testing.expect(!svc.raft.host.http_host.host.isLocalLeader(2910));
    try svc.ensureLinearizableRead();
    try std.testing.expect(svc.raft.host.http_host.host.isLocalLeader(2910));
    try std.testing.expect(svc.metrics().read_lease_requests > 0);
}

test "metadata http projected clone helpers clean up on allocation failure" {
    const Runner = struct {
        fn freePlacementIntents(alloc: std.mem.Allocator, intents: []raft_reconciler.PlacementIntent) void {
            for (intents) |intent| {
                if (intent.peer_node_ids.len > 0) alloc.free(intent.peer_node_ids);
            }
            alloc.free(intents);
        }

        fn run(alloc: std.mem.Allocator) !void {
            const tables = [_]metadata_table_manager.TableRecord{
                .{
                    .table_id = 1,
                    .name = "docs",
                    .description = "docs table",
                    .schema_json = "{\"kind\":\"demo\"}",
                    .indexes_json = "{\"default\":{}}",
                    .replication_sources_json = "[\"seed\"]",
                },
            };
            const ranges = [_]metadata_table_manager.RangeRecord{
                .{
                    .group_id = 11,
                    .table_id = 1,
                    .start_key = "doc:a",
                    .end_key = "doc:z",
                },
            };
            const stores = [_]metadata_table_manager.StoreRecord{
                .{
                    .store_id = 4,
                    .node_id = 1,
                    .role = "data",
                    .health_class = "healthy",
                    .failure_domain = "rack-a",
                    .live = true,
                    .capacity_bytes = 1024,
                    .available_bytes = 512,
                },
            };
            const placements = [_]raft_reconciler.PlacementIntent{
                .{
                    .record = .{
                        .group_id = 11,
                        .replica_id = 1,
                        .local_node_id = 1,
                        .bootstrap_mode = .persisted,
                    },
                    .store_id = 4,
                    .peer_node_ids = &.{ 2, 3 },
                },
            };
            const split_transitions = [_]transition_state.SplitTransitionRecord{
                .{
                    .transition_id = 91,
                    .attempt_epoch = 1,
                    .source_group_id = 11,
                    .destination_group_id = 12,
                    .phase = .prepare,
                    .split_key = "doc:m",
                    .source_range_end = "doc:z",
                    .rollback_reason = "none",
                },
            };
            const merge_transitions = [_]transition_state.MergeTransitionRecord{
                .{
                    .transition_id = 92,
                    .donor_group_id = 12,
                    .receiver_group_id = 11,
                    .phase = .prepare,
                    .rollback_reason = "none",
                },
            };

            const cloned_tables = try cloneProjectedTablesOwned(alloc, &tables);
            defer {
                for (cloned_tables) |record| metadata_table_manager.freeTable(alloc, record);
                alloc.free(cloned_tables);
            }

            const cloned_ranges = try cloneProjectedRangesOwned(alloc, &ranges);
            defer {
                for (cloned_ranges) |record| metadata_table_manager.freeRange(alloc, record);
                alloc.free(cloned_ranges);
            }

            const cloned_stores = try cloneProjectedStoresOwned(alloc, &stores);
            defer {
                for (cloned_stores) |record| metadata_table_manager.freeStore(alloc, record);
                alloc.free(cloned_stores);
            }

            const cloned_placements = try cloneProjectedPlacementIntentsOwned(alloc, &placements);
            defer freePlacementIntents(alloc, cloned_placements);

            const cloned_splits = try cloneProjectedSplitTransitionsOwned(alloc, &split_transitions);
            defer {
                for (cloned_splits) |record| metadata_table_manager.freeSplitTransitionRecord(alloc, record);
                alloc.free(cloned_splits);
            }

            const cloned_merges = try cloneProjectedMergeTransitionsOwned(alloc, &merge_transitions);
            defer {
                for (cloned_merges) |record| metadata_table_manager.freeMergeTransitionRecord(alloc, record);
                alloc.free(cloned_merges);
            }
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "metadata service local replica root reconcile permit hook defers reconcile work" {
    const Factory = struct {
        alloc: std.mem.Allocator,
        store: *raft_engine.core.MemoryStorage,

        fn iface(self: *@This()) raft_host.ReplicaDescriptorFactory {
            return .{ .ptr = self, .vtable = &.{ .build_descriptor = buildDescriptor, .free_descriptor = freeDescriptor } };
        }

        fn buildDescriptor(ptr: *anyopaque, record: raft_host.catalog.ReplicaRecord) !raft_engine.runtime.ReplicaDescriptor {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const peers = try self.alloc.dupe(raft_engine.core.types.NodeId, &[_]raft_engine.core.types.NodeId{record.local_node_id});
            return .{
                .group = .{
                    .group_id = record.group_id,
                    .local_node_id = record.local_node_id,
                    .raft_config = .{
                        .id = record.local_node_id,
                        .group_id = record.group_id,
                        .peers = peers,
                        .election_tick = 5,
                        .heartbeat_tick = 1,
                        .pre_vote = false,
                        .check_quorum = true,
                    },
                    .storage = self.store.storage(),
                },
                .bootstrap = switch (record.bootstrap_mode) {
                    .empty => .empty,
                    .persisted => .persisted,
                    .fetch_snapshot => .persisted,
                },
            };
        }

        fn freeDescriptor(ptr: *anyopaque, alloc: std.mem.Allocator, desc: *raft_engine.runtime.ReplicaDescriptor) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = alloc;
            self.alloc.free(desc.group.raft_config.peers);
        }
    };

    const HookCapture = struct {
        calls: usize = 0,
        indexes_pending: usize = 0,
        last_group_count: usize = 0,

        fn run(
            ptr: *anyopaque,
            request: LocalReplicaRootReconcileHook.Request,
        ) !metadata_table_provisioner.ProvisionSummary {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            self.last_group_count = request.group_ids.len;
            return .{ .indexes_pending = self.indexes_pending };
        }
    };

    const PermitCapture = struct {
        allow: bool = false,

        fn shouldReconcile(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.allow;
        }
    };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const replica_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reconcile-permit-root", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_root);
    const replica_catalog_path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/metadata-reconcile-permit-catalog.txt", .{tmp.sub_path});
    defer std.testing.allocator.free(replica_catalog_path);

    var store = raft_engine.core.MemoryStorage.init(std.testing.allocator);
    defer store.deinit();
    var factory = Factory{ .alloc = std.testing.allocator, .store = &store };

    var svc = try MetadataService.init(std.testing.allocator, .{
        .host = .{
            .local_node_id = 1,
            .metadata_group_id = 4096,
            .replica_root_dir = replica_root,
            .replica_catalog_path = replica_catalog_path,
        },
    }, .{
        .host = .{
            .host = .{
                .descriptor_factory = factory.iface(),
            },
        },
    }, .{});
    defer svc.deinit();

    _ = try svc.ensureMetadataReplica(.{
        .group_id = 4096,
        .replica_id = 1,
        .local_node_id = 1,
        .bootstrap_mode = .empty,
    });
    try svc.campaignMetadataGroup();

    try svc.upsertTable(.{ .table_id = 99, .name = "docs" });
    try svc.upsertRange(.{ .group_id = 9901, .table_id = 99, .start_key = "", .end_key = null });
    try svc.upsertNode(.{ .node_id = 1 });
    try svc.upsertReplicaIntent(.{
        .record = .{
            .group_id = 9901,
            .local_node_id = 1,
            .replica_id = 1,
        },
        .store_id = 0,
        .peer_node_ids = &.{},
    }, null, 0, false);

    var capture = HookCapture{};
    var permit = PermitCapture{ .allow = false };
    svc.setLocalReplicaRootReconcileHook(.{
        .ptr = &capture,
        .vtable = &.{ .run = HookCapture.run },
    });
    svc.setLocalReplicaRootReconcilePermitHook(.{
        .ptr = &permit,
        .vtable = &.{ .should_reconcile = PermitCapture.shouldReconcile },
    });

    try runServiceRounds(&svc, 8);
    try std.testing.expectEqual(@as(usize, 0), capture.calls);

    permit.allow = true;
    capture.indexes_pending = 1;
    svc.local_table_provisioning_epoch = null;
    svc.local_table_provisioning_group_ids_fingerprint = null;
    svc.last_local_table_provisioning_refresh_at_ms = 0;
    try runServiceRounds(&svc, 8);
    try std.testing.expect(capture.calls >= 1);
    try std.testing.expectEqual(@as(usize, 2), capture.last_group_count);
    try std.testing.expectEqual(null, svc.local_table_provisioning_fingerprint);

    capture.indexes_pending = 0;
    svc.local_table_provisioning_epoch = null;
    svc.local_table_provisioning_group_ids_fingerprint = null;
    svc.last_local_table_provisioning_refresh_at_ms = 0;
    try runServiceRounds(&svc, 8);
    try std.testing.expect(svc.local_table_provisioning_fingerprint != null);
}
