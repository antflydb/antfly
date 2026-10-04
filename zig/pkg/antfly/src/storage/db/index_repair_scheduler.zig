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
const Allocator = std.mem.Allocator;
const types = @import("types.zig");
const index_repair_state = @import("derived/index_repair_state.zig");

pub const Summary = struct { runnable: usize = 0, paused: usize = 0, terminal: usize = 0, earliest_retry_at_ms: u64 = 0, wake: types.IndexRepairWake = .empty };

pub fn retryableTerminalPhase(
    last_error: ?[]const u8,
    trigger: index_repair_state.Trigger,
) ?index_repair_state.Phase {
    const reason = last_error orelse return null;
    if (!std.mem.eql(u8, reason, @errorName(error.RepairSourceCoverageIncomplete))) return null;
    // A shadow can lag changing source artifacts during replacement or
    // initial catalog admission. Resume its durable owner and discard only
    // the inactive candidate; coverage lag is not structural corruption.
    // Externally supplied and structurally invalid generations stay closed.
    if (trigger != .operator_generation_rebuild and
        trigger != .artifact_baseline_adoption and
        trigger != .storage_format_migration and
        trigger != .artifact_coverage_mismatch and
        trigger != .replay_artifact_unavailable and
        trigger != .catalog_admission)
    {
        return null;
    }
    return .detected;
}

const IndexRepairScheduleClass = enum {
    runnable,
    paused,
    terminal,
};

const IndexRepairScheduleRecord = struct {
    repair_id: u128,
    revision: u64,
    index_name: []u8,
    work_class: index_repair_state.WorkClass,
    config_hash: u64,
    root_generation: u64,
    class: IndexRepairScheduleClass,
    phase_terminal: bool,
    /// Durable retry deadline from the checkpoint. Progress waits are a
    /// process-local acceleration layer and may temporarily replace the
    /// effective deadline without changing durable repair authority.
    durable_next_retry_at_ms: u64,
    next_retry_at_ms: u64,
    progress_wait_until_sequence: ?u64 = null,
    // Process-local grace period for an already-queryable initial build.
    // Once its deadline is selected, the repair owner receives one explicit
    // audit authority instead of blindly rearming the same grace period.
    audit_wait_armed: bool = false,
    heap_position: ?usize = null,
};

/// Process-resident projection of the durable repair checkpoint. Durable
/// intents remain the authority; this directory is rebuilt once per open and
/// then revision-reconciled after every committed mutation. It makes the
/// maintenance owner's steady-state inspection cost depend on its quantum,
/// not on the total number of paused or future-dated intents.
pub const Directory = struct {
    pub const ControlMutationDisposition = enum { stale, next, gap };

    initialized: bool = false,
    identity: ?index_repair_state.ReplicaIdentity = null,
    /// Exact durable checkpoint revision materialized by this projection.
    /// Events must arrive consecutively; a gap invalidates the acceleration
    /// layer and the next owner reconstructs it from the durable authority.
    control_revision: u64 = 0,
    records: std.ArrayListUnmanaged(IndexRepairScheduleRecord) = .empty,
    by_id: std.AutoHashMapUnmanaged(u128, usize) = .empty,
    by_name: std.StringHashMapUnmanaged(usize) = .empty,
    /// Exact min-heap of runnable repair IDs. Record-owned positions make
    /// update/removal O(log N) without tombstones, lazy pruning, or an
    /// occasionally unbounded compaction pass under the owner mutex.
    runnable_heap: std.ArrayListUnmanaged(u128) = .empty,
    cursor: usize = 0,
    runnable: usize = 0,
    paused: usize = 0,
    terminal: usize = 0,
    progress_waiters: usize = 0,

    pub fn classifyControlMutation(
        self: *const @This(),
        identity: index_repair_state.ReplicaIdentity,
        control_revision: u64,
    ) ControlMutationDisposition {
        const current_identity = self.identity orelse return .gap;
        if (!current_identity.eql(identity)) return .gap;
        if (control_revision <= self.control_revision) return .stale;
        if (control_revision != self.control_revision +| 1) return .gap;
        return .next;
    }

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        for (self.records.items) |record| alloc.free(record.index_name);
        self.records.deinit(alloc);
        self.by_id.deinit(alloc);
        self.by_name.deinit(alloc);
        self.runnable_heap.deinit(alloc);
        self.* = .{};
    }

    pub fn heapLess(self: *const @This(), a: u128, b: u128) bool {
        const a_record = self.records.items[self.by_id.get(a).?];
        const b_record = self.records.items[self.by_id.get(b).?];
        if (a_record.next_retry_at_ms != b_record.next_retry_at_ms) {
            return a_record.next_retry_at_ms < b_record.next_retry_at_ms;
        }
        return a < b;
    }

    pub fn swapHeap(self: *@This(), a: usize, b: usize) void {
        if (a == b) return;
        std.mem.swap(u128, &self.runnable_heap.items[a], &self.runnable_heap.items[b]);
        self.records.items[self.by_id.get(self.runnable_heap.items[a]).?].heap_position = a;
        self.records.items[self.by_id.get(self.runnable_heap.items[b]).?].heap_position = b;
    }

    pub fn siftHeapUp(self: *@This(), start: usize) usize {
        var child = start;
        while (child != 0) {
            const parent = (child - 1) / 2;
            if (!self.heapLess(self.runnable_heap.items[child], self.runnable_heap.items[parent])) break;
            self.swapHeap(child, parent);
            child = parent;
        }
        return child;
    }

    pub fn siftHeapDown(self: *@This(), start: usize) void {
        var parent = start;
        while (true) {
            const left = parent * 2 + 1;
            if (left >= self.runnable_heap.items.len) break;
            const right = left + 1;
            const child = if (right < self.runnable_heap.items.len and
                self.heapLess(self.runnable_heap.items[right], self.runnable_heap.items[left]))
                right
            else
                left;
            if (!self.heapLess(self.runnable_heap.items[child], self.runnable_heap.items[parent])) break;
            self.swapHeap(parent, child);
            parent = child;
        }
    }

    pub fn addRunnableAssumeCapacity(self: *@This(), repair_id: u128) void {
        const position = self.runnable_heap.items.len;
        self.runnable_heap.appendAssumeCapacity(repair_id);
        self.records.items[self.by_id.get(repair_id).?].heap_position = position;
        _ = self.siftHeapUp(position);
    }

    pub fn removeRunnable(self: *@This(), repair_id: u128) void {
        const record_index = self.by_id.get(repair_id) orelse return;
        const position = self.records.items[record_index].heap_position orelse return;
        const last = self.runnable_heap.pop().?;
        self.records.items[record_index].heap_position = null;
        if (position == self.runnable_heap.items.len) return;
        self.runnable_heap.items[position] = last;
        self.records.items[self.by_id.get(last).?].heap_position = position;
        const new_position = self.siftHeapUp(position);
        self.siftHeapDown(new_position);
    }

    pub fn rescheduleRunnable(self: *@This(), repair_id: u128) void {
        const record = self.records.items[self.by_id.get(repair_id).?];
        const position = record.heap_position orelse unreachable;
        const new_position = self.siftHeapUp(position);
        self.siftHeapDown(new_position);
    }

    /// Arm an event-driven wait for an exact durable repair revision. The
    /// fallback deadline recovers missed callbacks and non-replay maintenance
    /// completion without turning a queryable partial generation into a hot
    /// owner loop.
    pub fn deferForProgress(
        self: *@This(),
        repair_id: u128,
        expected_revision: u64,
        wake_at_sequence: u64,
        fallback_at_ms: u64,
    ) bool {
        const record_index = self.by_id.get(repair_id) orelse return false;
        const record = &self.records.items[record_index];
        if (record.revision != expected_revision or record.class != .runnable) return false;
        if (record.progress_wait_until_sequence == null) self.progress_waiters += 1;
        record.progress_wait_until_sequence = wake_at_sequence;
        record.audit_wait_armed = false;
        record.next_retry_at_ms = @max(fallback_at_ms, record.durable_next_retry_at_ms);
        self.rescheduleRunnable(repair_id);
        return true;
    }

    /// Defer a runnable lifecycle intent to its bounded audit deadline without
    /// arming an applied-watermark waiter. Once replay has already reached its
    /// durable target, later watermark increments are ordinary producer
    /// progress and cannot prove corpus-wide coverage complete; waking the
    /// lifecycle owner for every increment would turn a long initial build
    /// into a maintenance hot loop.
    pub fn deferForAudit(
        self: *@This(),
        repair_id: u128,
        expected_revision: u64,
        fallback_at_ms: u64,
    ) bool {
        const record_index = self.by_id.get(repair_id) orelse return false;
        const record = &self.records.items[record_index];
        if (record.revision != expected_revision or record.class != .runnable) return false;
        if (record.progress_wait_until_sequence != null) {
            record.progress_wait_until_sequence = null;
            self.progress_waiters -= 1;
        }
        record.audit_wait_armed = true;
        record.next_retry_at_ms = @max(fallback_at_ms, record.durable_next_retry_at_ms);
        self.rescheduleRunnable(repair_id);
        return true;
    }

    pub const ProgressWake = struct {
        repair_id: u128,
        revision: u64,
        work_class: index_repair_state.WorkClass,
        config_hash: u64,
        root_generation: u64,
    };

    /// Wake only the waiting repair for the exact index whose durable applied
    /// watermark advanced. Return the exact lifecycle identity so the outer
    /// owner cannot mistake normal initial-build progress for corruption debt;
    /// repeated notifications at the same sequence are coalesced.
    pub fn wakeForIndexProgress(self: *@This(), index_name: []const u8, applied_sequence: u64) ?ProgressWake {
        const record_index = self.by_name.get(index_name) orelse return null;
        const record = &self.records.items[record_index];
        const wake_at = record.progress_wait_until_sequence orelse return null;
        if (applied_sequence < wake_at) return null;
        record.progress_wait_until_sequence = null;
        record.audit_wait_armed = false;
        self.progress_waiters -= 1;
        record.next_retry_at_ms = record.durable_next_retry_at_ms;
        self.rescheduleRunnable(record.repair_id);
        return .{
            .repair_id = record.repair_id,
            .revision = record.revision,
            .work_class = record.work_class,
            .config_hash = record.config_hash,
            .root_generation = record.root_generation,
        };
    }

    pub fn earliestRetryDeadline(self: *const @This()) u64 {
        const repair_id = if (self.runnable_heap.items.len == 0)
            return 0
        else
            self.runnable_heap.items[0];
        return self.records.items[self.by_id.get(repair_id).?].next_retry_at_ms;
    }

    pub fn wake(self: *const @This()) types.IndexRepairWake {
        if (self.runnable != 0) {
            const deadline = self.earliestRetryDeadline();
            return if (deadline == 0) .immediate else .{ .at_realtime_ms = deadline };
        }
        if (self.paused != 0 or self.terminal != 0) return .parked;
        return .empty;
    }

    pub fn incrementClass(self: *@This(), class: IndexRepairScheduleClass) void {
        switch (class) {
            .runnable => self.runnable += 1,
            .paused => self.paused += 1,
            .terminal => self.terminal += 1,
        }
    }

    pub fn decrementClass(self: *@This(), class: IndexRepairScheduleClass) void {
        switch (class) {
            .runnable => self.runnable -= 1,
            .paused => self.paused -= 1,
            .terminal => self.terminal -= 1,
        }
    }

    pub fn classForIntent(intent: index_repair_state.IndexRepairIntent) IndexRepairScheduleClass {
        if (intent.phase == .terminal and retryableTerminalPhase(
            intent.last_error,
            intent.trigger,
        ) == null) return .terminal;
        if (intent.automation == .paused) return .paused;
        return .runnable;
    }

    pub fn upsert(
        self: *@This(),
        alloc: Allocator,
        intent: index_repair_state.IndexRepairIntent,
        persisted_revision: u64,
    ) !void {
        const class = classForIntent(intent);
        if (self.by_id.get(intent.repair_id)) |record_index| {
            const record = &self.records.items[record_index];
            if (!std.mem.eql(u8, record.index_name, intent.index_name)) return error.InvalidIndexRepairState;
            if (persisted_revision < record.revision) return;
            if (persisted_revision == record.revision) {
                if (record.class != class or
                    record.work_class != intent.work_class or
                    record.config_hash != intent.config_hash or
                    record.root_generation != intent.root_generation or
                    record.phase_terminal != (intent.phase == .terminal) or
                    record.durable_next_retry_at_ms != intent.next_retry_at_ms)
                {
                    return error.InvalidIndexRepairState;
                }
                return;
            }
            const schedule_changed = record.class != class or
                record.next_retry_at_ms != intent.next_retry_at_ms or
                record.progress_wait_until_sequence != null;
            if (schedule_changed and record.class != .runnable and class == .runnable)
                try self.runnable_heap.ensureUnusedCapacity(alloc, 1);
            if (schedule_changed and record.class == .runnable and class != .runnable)
                self.removeRunnable(intent.repair_id);
            if (record.class != class) {
                self.decrementClass(record.class);
                self.incrementClass(class);
                record.class = class;
            }
            record.next_retry_at_ms = intent.next_retry_at_ms;
            record.durable_next_retry_at_ms = intent.next_retry_at_ms;
            record.work_class = intent.work_class;
            record.config_hash = intent.config_hash;
            record.root_generation = intent.root_generation;
            if (record.progress_wait_until_sequence != null) self.progress_waiters -= 1;
            record.progress_wait_until_sequence = null;
            record.audit_wait_armed = false;
            record.phase_terminal = intent.phase == .terminal;
            record.revision = persisted_revision;
            if (schedule_changed and class == .runnable) {
                if (record.heap_position == null)
                    self.addRunnableAssumeCapacity(intent.repair_id)
                else
                    self.rescheduleRunnable(intent.repair_id);
            }
            return;
        }
        if (self.by_name.contains(intent.index_name)) return error.InvalidIndexRepairState;
        const owned_name = try alloc.dupe(u8, intent.index_name);
        errdefer alloc.free(owned_name);
        try self.records.ensureUnusedCapacity(alloc, 1);
        try self.by_id.ensureUnusedCapacity(alloc, 1);
        try self.by_name.ensureUnusedCapacity(alloc, 1);
        if (class == .runnable) try self.runnable_heap.ensureUnusedCapacity(alloc, 1);
        const record_index = self.records.items.len;
        self.records.appendAssumeCapacity(.{
            .repair_id = intent.repair_id,
            .revision = persisted_revision,
            .index_name = owned_name,
            .work_class = intent.work_class,
            .config_hash = intent.config_hash,
            .root_generation = intent.root_generation,
            .class = class,
            .phase_terminal = intent.phase == .terminal,
            .durable_next_retry_at_ms = intent.next_retry_at_ms,
            .next_retry_at_ms = intent.next_retry_at_ms,
        });
        self.by_id.putAssumeCapacity(intent.repair_id, record_index);
        self.by_name.putAssumeCapacity(owned_name, record_index);
        self.incrementClass(class);
        if (class == .runnable) self.addRunnableAssumeCapacity(intent.repair_id);
    }

    pub fn remove(self: *@This(), alloc: Allocator, repair_id: u128) void {
        const record_index = self.by_id.get(repair_id) orelse return;
        const removed = self.records.items[record_index];
        if (removed.progress_wait_until_sequence != null) self.progress_waiters -= 1;
        if (removed.class == .runnable) self.removeRunnable(removed.repair_id);
        self.decrementClass(removed.class);
        _ = self.by_id.remove(removed.repair_id);
        _ = self.by_name.remove(removed.index_name);
        alloc.free(removed.index_name);
        _ = self.records.swapRemove(record_index);
        if (record_index < self.records.items.len) {
            const moved = self.records.items[record_index];
            self.by_id.getPtr(moved.repair_id).?.* = record_index;
            self.by_name.getPtr(moved.index_name).?.* = record_index;
        }
        if (self.records.items.len == 0) {
            self.cursor = 0;
        } else if (self.cursor >= self.records.items.len) {
            self.cursor %= self.records.items.len;
        }
    }

    pub fn buildFromState(alloc: Allocator, state: index_repair_state.State) !@This() {
        var directory: @This() = .{
            .initialized = true,
            .identity = state.identity,
            .control_revision = state.control_revision,
        };
        errdefer directory.deinit(alloc);
        try directory.records.ensureTotalCapacity(alloc, state.entries.items.len);
        const map_capacity: u32 = @intCast(state.entries.items.len);
        try directory.by_id.ensureTotalCapacity(alloc, map_capacity);
        try directory.by_name.ensureTotalCapacity(alloc, map_capacity);
        try directory.runnable_heap.ensureTotalCapacity(alloc, state.entries.items.len);
        for (state.entries.items) |entry| try directory.upsert(alloc, entry.intent, entry.intent.revision);
        return directory;
    }

    pub fn summary(self: *@This()) Summary {
        return .{
            .runnable = self.runnable,
            .paused = self.paused,
            .terminal = self.terminal,
            .earliest_retry_at_ms = self.earliestRetryDeadline(),
            .wake = self.wake(),
        };
    }

    pub fn summaryForIndex(self: *@This(), target_index_name: ?[]const u8) Summary {
        const name = target_index_name orelse return self.summary();
        const index = self.by_name.get(name) orelse return .{};
        const record = self.records.items[index];
        return .{
            .runnable = @intFromBool(record.class == .runnable),
            .paused = @intFromBool(record.class == .paused),
            .terminal = @intFromBool(record.class == .terminal),
            .earliest_retry_at_ms = if (record.class == .runnable) record.next_retry_at_ms else 0,
            .wake = if (record.class != .runnable) .empty else if (record.next_retry_at_ms == 0) .immediate else .{ .at_realtime_ms = record.next_retry_at_ms },
        };
    }
};

test "resident index repair scheduler maintains exact aggregate wake precedence" {
    const alloc = std.testing.allocator;
    var directory = Directory{
        .initialized = true,
        .identity = .{ .db_identity = 1, .replica_id = 1, .root_generation = 1 },
    };
    defer directory.deinit(alloc);

    const Intent = struct {
        fn make(
            allocator: Allocator,
            id: u128,
            deadline: u64,
            automation: index_repair_state.Automation,
        ) !index_repair_state.IndexRepairIntent {
            return .{
                .repair_id = id,
                .revision = 1,
                .db_identity = 1,
                .group_id = 1,
                .replica_id = 1,
                .root_generation = 1,
                .index_name = try std.fmt.allocPrint(allocator, "repair-{d}", .{id}),
                .kind = .full_text,
                .config_hash = 1,
                .detected_sequence = 1,
                .target_sequence = 1,
                .started_at_ms = 1,
                .updated_at_ms = 1,
                .owner_epoch = 0,
                .next_retry_at_ms = deadline,
                .automation = automation,
            };
        }
    };

    var future = try Intent.make(alloc, 1, 900, .enabled);
    defer future.deinit(alloc);
    try directory.upsert(alloc, future, future.revision);
    var immediate = try Intent.make(alloc, 2, 0, .enabled);
    defer immediate.deinit(alloc);
    try directory.upsert(alloc, immediate, immediate.revision);
    var earlier = try Intent.make(alloc, 3, 400, .enabled);
    defer earlier.deinit(alloc);
    try directory.upsert(alloc, earlier, earlier.revision);
    try std.testing.expectEqual(@as(usize, 3), directory.runnable_heap.items.len);
    try std.testing.expectEqual(types.IndexRepairWake.immediate, directory.wake());

    // Deadline churn updates one indexed heap position; it never accumulates
    // lazy tombstones that later require an unbounded owner-side compaction.
    for (0..4096) |i| {
        immediate.revision += 1;
        immediate.next_retry_at_ms = if (i % 2 == 0) 0 else 800;
        try directory.upsert(alloc, immediate, immediate.revision);
        try std.testing.expectEqual(@as(usize, 3), directory.runnable_heap.items.len);
    }
    immediate.next_retry_at_ms = 0;
    immediate.revision += 1;
    try directory.upsert(alloc, immediate, immediate.revision);

    // Model the first selected intent deferring itself. A different immediate
    // intent must continue to dominate the aggregate wake until it too moves.
    future.next_retry_at_ms = 1_200;
    future.revision += 1;
    try directory.upsert(alloc, future, future.revision);
    try std.testing.expectEqual(types.IndexRepairWake.immediate, directory.wake());
    immediate.next_retry_at_ms = 700;
    immediate.revision += 1;
    try directory.upsert(alloc, immediate, immediate.revision);
    try std.testing.expectEqual(@as(u64, 400), directory.wake().at_realtime_ms);

    earlier.automation = .paused;
    earlier.revision += 1;
    try directory.upsert(alloc, earlier, earlier.revision);
    try std.testing.expectEqual(@as(u64, 700), directory.wake().at_realtime_ms);
    try std.testing.expectEqual(directory.runnable, directory.runnable_heap.items.len);
    directory.remove(alloc, 2);
    directory.remove(alloc, 1);
    try std.testing.expectEqual(types.IndexRepairWake.parked, directory.wake());
    try std.testing.expectEqual(@as(usize, 0), directory.runnable_heap.items.len);

    // A durable clear advances the materialized-view revision even though it
    // removes the record. A delayed pre-clear upsert is then stale and cannot
    // resurrect debt which is absent from the checkpoint. A missed event is a
    // gap, forcing reconstruction rather than accepting a partial view.
    directory.control_revision = 41;
    const identity = directory.identity.?;
    try std.testing.expectEqual(
        Directory.ControlMutationDisposition.next,
        directory.classifyControlMutation(identity, 42),
    );
    directory.control_revision = 42;
    try std.testing.expectEqual(
        Directory.ControlMutationDisposition.stale,
        directory.classifyControlMutation(identity, 41),
    );
    try std.testing.expectEqual(
        Directory.ControlMutationDisposition.gap,
        directory.classifyControlMutation(identity, 44),
    );
}

test "resident index repair progress waits are revision scoped and event driven" {
    const alloc = std.testing.allocator;
    var directory = Directory{
        .initialized = true,
        .identity = .{ .db_identity = 1, .replica_id = 1, .root_generation = 1 },
    };
    defer directory.deinit(alloc);

    var intent = index_repair_state.IndexRepairIntent{
        .repair_id = 17,
        .revision = 1,
        .db_identity = 1,
        .group_id = 1,
        .replica_id = 1,
        .root_generation = 1,
        .index_name = try alloc.dupe(u8, "semantic_idx"),
        .kind = .dense_vector,
        .config_hash = 9,
        .detected_sequence = 1,
        .target_sequence = 20,
        .started_at_ms = 1,
        .updated_at_ms = 1,
        .owner_epoch = 0,
    };
    defer intent.deinit(alloc);
    try directory.upsert(alloc, intent, intent.revision);

    try std.testing.expect(directory.deferForProgress(17, 1, 20, 500));
    try std.testing.expectEqual(@as(usize, 1), directory.progress_waiters);
    try std.testing.expectEqual(@as(u64, 500), directory.wake().at_realtime_ms);
    // An idempotent durable projection must not erase a resident wait.
    try directory.upsert(alloc, intent, intent.revision);
    try std.testing.expectEqual(@as(u64, 500), directory.wake().at_realtime_ms);
    try std.testing.expect(directory.wakeForIndexProgress("semantic_idx", 10) == null);
    try std.testing.expect(directory.wakeForIndexProgress("semantic_idx", 19) == null);
    const progress_wake = directory.wakeForIndexProgress("semantic_idx", 20) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u128, 17), progress_wake.repair_id);
    try std.testing.expectEqual(@as(u64, 1), progress_wake.revision);
    try std.testing.expectEqual(index_repair_state.WorkClass.repair, progress_wake.work_class);
    try std.testing.expectEqual(@as(u64, 9), progress_wake.config_hash);
    try std.testing.expectEqual(@as(u64, 1), progress_wake.root_generation);
    try std.testing.expectEqual(@as(usize, 0), directory.progress_waiters);
    try std.testing.expectEqual(types.IndexRepairWake.immediate, directory.wake());
    try std.testing.expect(directory.wakeForIndexProgress("unrelated", 99) == null);

    try std.testing.expect(directory.deferForProgress(17, 1, 30, 600));
    intent.revision += 1;
    intent.next_retry_at_ms = 700;
    try directory.upsert(alloc, intent, intent.revision);
    try std.testing.expectEqual(@as(usize, 0), directory.progress_waiters);
    try std.testing.expectEqual(@as(u64, 700), directory.wake().at_realtime_ms);
    // A delayed event from the prior revision cannot wake the new schedule.
    try std.testing.expect(directory.wakeForIndexProgress("semantic_idx", 12) == null);

    // Once replay is at its durable target, corpus coverage—not another
    // watermark increment—is the remaining completion proof. The bounded
    // audit must therefore ignore arbitrarily high progress notifications.
    try std.testing.expect(directory.deferForAudit(17, 2, 800));
    try std.testing.expectEqual(@as(usize, 0), directory.progress_waiters);
    try std.testing.expectEqual(@as(u64, 800), directory.wake().at_realtime_ms);
    try std.testing.expect(directory.wakeForIndexProgress("semantic_idx", std.math.maxInt(u64)) == null);
    try std.testing.expectEqual(@as(u64, 800), directory.wake().at_realtime_ms);
}
