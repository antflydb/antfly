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
const platform = @import("antfly_platform");
const runtime_backend = @import("runtime_backend.zig");
const storage_io = @import("lsm_backend/storage_io.zig");
const threaded_io_limits = @import("../common/threaded_io_limits.zig");

const Allocator = std.mem.Allocator;
const Io = std.Io;
const AtomicU64 = platform.atomic.Value(u64);

pub const LsmOwnerKind = enum { primary, full_text, dense_vector };

pub const LsmMutableSnapshotReason = enum(u8) {
    bound_read_txn,
    namespace_read_txn,
    current_scan,
    other,
    bulk_current_scan,
};

pub const lsm_mutable_snapshot_reason_count = @typeInfo(LsmMutableSnapshotReason).@"enum".fields.len;

pub const LsmMutableSnapshotCloneReasonStats = struct {
    calls: u64 = 0,
    bytes_total: u64 = 0,
    peak_bytes: u64 = 0,

    fn accumulate(self: *@This(), other: @This()) void {
        self.calls +|= other.calls;
        self.bytes_total +|= other.bytes_total;
        self.peak_bytes = @max(self.peak_bytes, other.peak_bytes);
    }
};

pub const LsmOwnerCloneStats = struct {
    calls: u64 = 0,
    bytes_total: u64 = 0,
    peak_bytes: u64 = 0,
    bulk_current_scan_peak_active_bytes: u64 = 0,
    /// Number of distinct retired owner labels folded into this bounded
    /// attribution record. This is a counter, not owner residency.
    labels_collapsed_total: u64 = 0,
    by_reason: [lsm_mutable_snapshot_reason_count]LsmMutableSnapshotCloneReasonStats =
        [_]LsmMutableSnapshotCloneReasonStats{.{}} ** lsm_mutable_snapshot_reason_count,

    pub fn accumulate(self: *@This(), other: @This()) void {
        self.calls +|= other.calls;
        self.bytes_total +|= other.bytes_total;
        self.peak_bytes = @max(self.peak_bytes, other.peak_bytes);
        self.bulk_current_scan_peak_active_bytes = @max(
            self.bulk_current_scan_peak_active_bytes,
            other.bulk_current_scan_peak_active_bytes,
        );
        self.labels_collapsed_total +|= other.labels_collapsed_total;
        for (&self.by_reason, other.by_reason) |*dst, src| dst.accumulate(src);
    }
};

pub const LsmOwnerCloneMetricSnapshot = struct {
    table_name: []u8,
    group_id: u64,
    owner_kind: LsmOwnerKind,
    owner_name: []u8,
    owner_overflow: bool = false,
    stats: LsmOwnerCloneStats,

    pub fn deinit(self: *@This(), alloc: Allocator) void {
        alloc.free(self.table_name);
        alloc.free(self.owner_name);
        self.* = undefined;
    }
};

const LsmOwnerCloneRegistry = struct {
    const max_entries: usize = 4096;
    const max_sources: usize = 8192;

    const Entry = struct {
        table_name: []u8,
        group_id: u64,
        owner_kind: LsmOwnerKind,
        owner_name: []u8,
        owner_overflow: bool,
        stats: LsmOwnerCloneStats,

        fn deinit(self: *Entry, alloc: Allocator) void {
            alloc.free(self.table_name);
            alloc.free(self.owner_name);
            self.* = undefined;
        }
    };

    const EntryKey = struct {
        table_name: []const u8,
        group_id: u64,
        owner_kind: LsmOwnerKind,
        owner_name: []const u8,
        owner_overflow: bool,
    };

    const EntryKeyContext = struct {
        pub fn hash(_: @This(), key: EntryKey) u64 {
            var hasher = std.hash.Wyhash.init(0);
            hasher.update(std.mem.asBytes(&key.group_id));
            hasher.update(std.mem.asBytes(&key.owner_kind));
            hasher.update(std.mem.asBytes(&key.owner_overflow));
            hasher.update(key.table_name);
            hasher.update("\x00");
            hasher.update(key.owner_name);
            return hasher.final();
        }

        pub fn eql(_: @This(), lhs: EntryKey, rhs: EntryKey) bool {
            return lhs.group_id == rhs.group_id and lhs.owner_kind == rhs.owner_kind and
                lhs.owner_overflow == rhs.owner_overflow and
                std.mem.eql(u8, lhs.table_name, rhs.table_name) and
                std.mem.eql(u8, lhs.owner_name, rhs.owner_name);
        }
    };

    const EntryMap = std.HashMapUnmanaged(EntryKey, usize, EntryKeyContext, 80);

    const SourceKey = struct { id: usize, entry_index: usize };

    const Source = struct {
        key: SourceKey,
        observed: LsmOwnerCloneStats,
        previous_for_id: ?usize = null,
        next_for_id: ?usize = null,
    };

    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    entries: std.ArrayListUnmanaged(Entry) = .empty,
    entry_by_key: EntryMap = .empty,
    sources: std.ArrayListUnmanaged(Source) = .empty,
    source_by_key: std.AutoHashMapUnmanaged(SourceKey, usize) = .empty,
    source_head_by_id: std.AutoHashMapUnmanaged(usize, usize) = .empty,
    dropped_observations: u64 = 0,
    collapsed_labels: u64 = 0,

    fn init(alloc: Allocator) LsmOwnerCloneRegistry {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *LsmOwnerCloneRegistry) void {
        for (self.entries.items) |*entry| entry.deinit(self.alloc);
        self.entries.deinit(self.alloc);
        self.entry_by_key.deinit(self.alloc);
        self.sources.deinit(self.alloc);
        self.source_by_key.deinit(self.alloc);
        self.source_head_by_id.deinit(self.alloc);
        self.* = undefined;
    }

    fn findOrCreateEntryLocked(
        self: *LsmOwnerCloneRegistry,
        table_name: []const u8,
        group_id: u64,
        owner_kind: LsmOwnerKind,
        owner_name: []const u8,
        owner_overflow: bool,
    ) !?usize {
        const lookup_key: EntryKey = .{
            .table_name = table_name,
            .group_id = group_id,
            .owner_kind = owner_kind,
            .owner_name = owner_name,
            .owner_overflow = owner_overflow,
        };
        if (self.entry_by_key.get(lookup_key)) |index| return index;
        if (self.entries.items.len >= max_entries) {
            self.dropped_observations +|= 1;
            return null;
        }
        const owned_table_name = try self.alloc.dupe(u8, table_name);
        errdefer self.alloc.free(owned_table_name);
        const owned_owner_name = try self.alloc.dupe(u8, owner_name);
        errdefer self.alloc.free(owned_owner_name);
        try self.entries.ensureUnusedCapacity(self.alloc, 1);
        try self.entry_by_key.ensureUnusedCapacity(self.alloc, 1);
        self.entries.appendAssumeCapacity(.{
            .table_name = owned_table_name,
            .group_id = group_id,
            .owner_kind = owner_kind,
            .owner_name = owned_owner_name,
            .owner_overflow = owner_overflow,
            .stats = .{},
        });
        const index = self.entries.items.len - 1;
        const entry = &self.entries.items[index];
        self.entry_by_key.putAssumeCapacity(.{
            .table_name = entry.table_name,
            .group_id = entry.group_id,
            .owner_kind = entry.owner_kind,
            .owner_name = entry.owner_name,
            .owner_overflow = entry.owner_overflow,
        }, index);
        return index;
    }

    fn accumulateObserved(
        total: *LsmOwnerCloneStats,
        previous: LsmOwnerCloneStats,
        current: LsmOwnerCloneStats,
    ) void {
        total.calls +|= current.calls -| previous.calls;
        total.bytes_total +|= current.bytes_total -| previous.bytes_total;
        total.peak_bytes = @max(total.peak_bytes, current.peak_bytes);
        total.bulk_current_scan_peak_active_bytes = @max(
            total.bulk_current_scan_peak_active_bytes,
            current.bulk_current_scan_peak_active_bytes,
        );
        total.labels_collapsed_total +|= current.labels_collapsed_total -| previous.labels_collapsed_total;
        for (&total.by_reason, previous.by_reason, current.by_reason) |*dst, prior, now| {
            dst.calls +|= now.calls -| prior.calls;
            dst.bytes_total +|= now.bytes_total -| prior.bytes_total;
            dst.peak_bytes = @max(dst.peak_bytes, now.peak_bytes);
        }
    }

    /// Observe absolute counters from one live DB generation. The registry
    /// converts them to deltas under the same mutex that serves snapshots, so
    /// a scrape can never fall back from live totals to a smaller archived
    /// value while a cache entry is being retired.
    fn observe(
        self: *LsmOwnerCloneRegistry,
        source_id: usize,
        table_name: []const u8,
        group_id: u64,
        owner_kind: LsmOwnerKind,
        owner_name: []const u8,
        owner_overflow: bool,
        stats: LsmOwnerCloneStats,
    ) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const lookup_key: EntryKey = .{
            .table_name = table_name,
            .group_id = group_id,
            .owner_kind = owner_kind,
            .owner_name = owner_name,
            .owner_overflow = owner_overflow,
        };
        if (self.entry_by_key.get(lookup_key)) |entry_index| {
            const source_key: SourceKey = .{ .id = source_id, .entry_index = entry_index };
            if (self.source_by_key.get(source_key)) |source_index| {
                const source = &self.sources.items[source_index];
                const collapsed_delta = stats.labels_collapsed_total -| source.observed.labels_collapsed_total;
                accumulateObserved(&self.entries.items[entry_index].stats, source.observed, stats);
                self.collapsed_labels +|= collapsed_delta;
                source.observed = stats;
                return;
            }
        } else if (self.entries.items.len >= max_entries) {
            // Reject a new label before reserving source storage. At the label
            // ceiling this observation is intentionally dropped, so allocator
            // pressure must not turn a healthy /metrics response into OOM.
            self.dropped_observations +|= 1;
            return;
        }
        // Never commit a permanent label entry unless its initial absolute
        // source baseline can be retained. Otherwise source saturation could
        // fill the entry registry with zero-valued tombstones that survive
        // after live sources retire.
        if (self.sources.items.len >= max_sources) {
            self.dropped_observations +|= 1;
            return;
        }
        // Reserve both source containers before publishing a new label entry.
        // This makes admission transactional with respect to allocator failure:
        // findOrCreateEntryLocked cannot leave an unreachable zero-stat entry
        // if the initial source baseline cannot be stored.
        try self.sources.ensureUnusedCapacity(self.alloc, 1);
        try self.source_by_key.ensureUnusedCapacity(self.alloc, 1);
        const previous_head = self.source_head_by_id.get(source_id);
        if (previous_head == null) try self.source_head_by_id.ensureUnusedCapacity(self.alloc, 1);
        const entry_index = (try self.findOrCreateEntryLocked(
            table_name,
            group_id,
            owner_kind,
            owner_name,
            owner_overflow,
        )) orelse return;
        const source_key: SourceKey = .{ .id = source_id, .entry_index = entry_index };
        self.sources.appendAssumeCapacity(.{
            .key = source_key,
            .observed = stats,
            .next_for_id = previous_head,
        });
        const source_index = self.sources.items.len - 1;
        self.source_by_key.putAssumeCapacity(source_key, source_index);
        if (previous_head) |head_index| {
            self.sources.items[head_index].previous_for_id = source_index;
            self.source_head_by_id.getPtr(source_id).?.* = source_index;
        } else {
            self.source_head_by_id.putAssumeCapacity(source_id, source_index);
        }
        self.entries.items[entry_index].stats.accumulate(stats);
        self.collapsed_labels +|= stats.labels_collapsed_total;
    }

    fn removeSourceAtLocked(self: *LsmOwnerCloneRegistry, source_index: usize) void {
        const removed = self.sources.items[source_index];
        _ = self.source_by_key.remove(removed.key);

        if (removed.previous_for_id) |previous_index| {
            self.sources.items[previous_index].next_for_id = removed.next_for_id;
        } else if (removed.next_for_id) |next_index| {
            self.source_head_by_id.getPtr(removed.key.id).?.* = next_index;
        } else {
            _ = self.source_head_by_id.remove(removed.key.id);
        }
        if (removed.next_for_id) |next_index| {
            self.sources.items[next_index].previous_for_id = removed.previous_for_id;
        }

        const last_index = self.sources.items.len - 1;
        if (source_index == last_index) {
            _ = self.sources.pop();
            return;
        }

        const moved = self.sources.items[last_index];
        self.sources.items[source_index] = moved;
        _ = self.sources.pop();
        self.source_by_key.getPtr(moved.key).?.* = source_index;
        if (moved.previous_for_id) |previous_index| {
            self.sources.items[previous_index].next_for_id = source_index;
        } else {
            self.source_head_by_id.getPtr(moved.key.id).?.* = source_index;
        }
        if (moved.next_for_id) |next_index| {
            self.sources.items[next_index].previous_for_id = source_index;
        }
    }

    fn retireSource(self: *LsmOwnerCloneRegistry, source_id: usize) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        while (self.source_head_by_id.get(source_id)) |source_index| {
            self.removeSourceAtLocked(source_index);
        }
    }

    fn accumulate(
        self: *LsmOwnerCloneRegistry,
        table_name: []const u8,
        group_id: u64,
        owner_kind: LsmOwnerKind,
        owner_name: []const u8,
        owner_overflow: bool,
        stats: LsmOwnerCloneStats,
    ) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const entry_index = (try self.findOrCreateEntryLocked(
            table_name,
            group_id,
            owner_kind,
            owner_name,
            owner_overflow,
        )) orelse return;
        self.entries.items[entry_index].stats.accumulate(stats);
        self.collapsed_labels +|= stats.labels_collapsed_total;
    }

    fn snapshotAlloc(self: *LsmOwnerCloneRegistry, alloc: Allocator) ![]LsmOwnerCloneMetricSnapshot {
        // Registry labels are immutable and entries are never removed. Capture
        // a prefix boundary under the mutex, then allocate outside it. Labels
        // admitted after that boundary belong to the next scrape; retrying for
        // them could turn sustained label admission into quadratic allocation
        // churn or prevent a scrape from completing.
        lockAtomic(&self.mutex);
        const entry_count = self.entries.items.len;
        self.mutex.unlock();
        return try self.snapshotPrefixAlloc(alloc, entry_count);
    }

    fn snapshotPrefixAlloc(
        self: *LsmOwnerCloneRegistry,
        alloc: Allocator,
        entry_count: usize,
    ) ![]LsmOwnerCloneMetricSnapshot {
        const result = try alloc.alloc(LsmOwnerCloneMetricSnapshot, entry_count);
        lockAtomic(&self.mutex);
        // Entries are append-only for the registry lifetime, so the prefix
        // selected by snapshotAlloc remains present and its labels remain
        // stable even if observations append entries during allocation.
        std.debug.assert(self.entries.items.len >= result.len);
        for (self.entries.items[0..result.len], result) |entry, *snapshot| {
            snapshot.* = .{
                // Temporarily borrowed. The loop below replaces both slices
                // with owned copies after releasing the registry mutex.
                .table_name = entry.table_name,
                .group_id = entry.group_id,
                .owner_kind = entry.owner_kind,
                .owner_name = entry.owner_name,
                .owner_overflow = entry.owner_overflow,
                .stats = entry.stats,
            };
        }
        self.mutex.unlock();

        var initialized: usize = 0;
        errdefer {
            for (result[0..initialized]) |*entry| entry.deinit(alloc);
            alloc.free(result);
        }
        for (result) |*snapshot| {
            const table_name = try alloc.dupe(u8, snapshot.table_name);
            const owner_name = alloc.dupe(u8, snapshot.owner_name) catch |err| {
                alloc.free(table_name);
                return err;
            };
            snapshot.table_name = table_name;
            snapshot.owner_name = owner_name;
            initialized += 1;
        }
        return result;
    }

    fn droppedObservationsTotal(self: *LsmOwnerCloneRegistry) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.dropped_observations;
    }

    fn collapsedLabelsTotal(self: *LsmOwnerCloneRegistry) u64 {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.collapsed_labels;
    }
};

pub const Backend = runtime_backend.Backend;
pub const IoImpl = if (builtin.os.tag == .freestanding) void else Io.Threaded;
pub const default_io_concurrent_limit: u32 = threaded_io_limits.service;

pub const Config = struct {
    backend: Backend = runtime_backend.defaultExecutorBackend(),
    /// Optional caller-owned I/O interfaces. These make the production
    /// runtime usable with deterministic `std.Io` implementations without
    /// teaching it about VOPR or any concrete backend. The caller must keep
    /// every supplied interface alive until all lane leases are released and
    /// `BackendRuntime.deinit` returns.
    borrowed_io: ?BorrowedIo = null,
    /// Optional caller-owned synchronous filesystem authority. Manual
    /// runtimes use this for lifecycle locks and durable metadata without
    /// acquiring a worker executor. It must outlive the runtime.
    filesystem_io: ?Io = null,
};

pub const BorrowedIo = struct {
    general: Io,
    raft_inbound: ?Io = null,
    raft_outbound: ?Io = null,
    api: ?Io = null,
    inference: ?Io = null,
    control: ?Io = null,
};

/// Atomic admission gate for a lane whose backing executor is destroyed only
/// after every committed borrower has released it. The high bit permanently
/// closes admission; the remaining bits are the active lease count. Keeping
/// both in one word eliminates the check/increment teardown race.
const LaneLeaseGate = struct {
    const closed_bit: usize = @as(usize, 1) << (@bitSizeOf(usize) - 1);
    const count_mask: usize = closed_bit - 1;

    state: std.atomic.Value(usize) = .init(0),
    drain_mutex: Io.Mutex = .init,
    drained: Io.Condition = .init,

    fn tryAcquire(self: *LaneLeaseGate) ?usize {
        var observed = self.state.load(.acquire);
        while (true) {
            if (observed & closed_bit != 0) return null;
            const count = observed & count_mask;
            std.debug.assert(count < count_mask);
            if (self.state.cmpxchgWeak(observed, observed + 1, .acq_rel, .acquire)) |actual| {
                observed = actual;
                continue;
            }
            return count + 1;
        }
    }

    fn release(self: *LaneLeaseGate, coordinator_io: ?Io) void {
        const previous = self.state.fetchSub(1, .acq_rel);
        std.debug.assert(previous & count_mask > 0);
        if (previous & closed_bit != 0 and previous & count_mask == 1) {
            if (coordinator_io) |io| {
                // Synchronize with waitDrained's final state check so a last
                // release cannot race between that check and parking.
                self.drain_mutex.lockUncancelable(io);
                self.drained.broadcast(io);
                self.drain_mutex.unlock(io);
            }
        }
    }

    fn close(self: *LaneLeaseGate) void {
        _ = self.state.fetchOr(closed_bit, .acq_rel);
    }

    fn active(self: *const LaneLeaseGate) usize {
        return self.state.load(.acquire) & count_mask;
    }

    fn isClosed(self: *const LaneLeaseGate) bool {
        return self.state.load(.acquire) & closed_bit != 0;
    }

    fn waitDrained(self: *LaneLeaseGate, coordinator_io: ?Io) void {
        if (coordinator_io) |io| {
            self.drain_mutex.lockUncancelable(io);
            defer self.drain_mutex.unlock(io);
            while (self.active() != 0) self.drained.waitUncancelable(io, &self.drain_mutex);
            return;
        }

        if (comptime builtin.os.tag == .freestanding or builtin.single_threaded) {
            if (self.active() != 0) @panic("cannot drain a lane lease without an I/O coordinator");
            return;
        }
        // Manual runtimes have no executor to park on. They ordinarily have
        // no successful lane leases; retain an executor-independent fallback
        // for a close racing an unavailable acquisition.
        while (self.active() != 0) std.Thread.yield() catch {};
    }
};

/// Process-local hook used by composed runtimes to replace a filesystem DB
/// open with another storage implementation. The options pointer is opaque here
/// to keep the executor layer independent of the DB module; DB.open is the sole
/// caller and passes a `*db.OpenOptions`.
pub const DbOpenConfigurator = struct {
    ptr: *anyopaque,
    configure_fn: *const fn (ptr: *anyopaque, path: []const u8, options: *anyopaque) anyerror!void,

    pub fn configure(self: @This(), path: []const u8, options: anytype) !void {
        try self.configure_fn(self.ptr, path, @ptrCast(options));
    }
};

pub const DurableJobLane = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    fn lifecycleUnsupported(_: *anyopaque, _: u64) anyerror!void {
        return error.BackgroundOwnerLifecycleUnsupported;
    }

    pub const VTable = struct {
        submit: *const fn (ptr: *anyopaque, job: Job) anyerror!void,
        drain_owner: *const fn (ptr: *anyopaque, owner_id: u64) void,
        close_owner: *const fn (ptr: *anyopaque, owner_id: u64) void,
        pause_owner: *const fn (ptr: *anyopaque, owner_id: u64) anyerror!void = lifecycleUnsupported,
        resume_owner: *const fn (ptr: *anyopaque, owner_id: u64) anyerror!void = lifecycleUnsupported,
        reopen_owner: *const fn (ptr: *anyopaque, owner_id: u64) anyerror!void = lifecycleUnsupported,
        poll: *const fn (ptr: *anyopaque, max_jobs: usize) anyerror!usize,
        is_accepting: ?*const fn (ptr: *anyopaque) bool = null,
        executes_inline: bool = false,
    };

    /// On success, the lane owns `job` and will call `job.deinit`.
    /// On error, ownership remains with the caller.
    pub fn submit(self: DurableJobLane, job: Job) !void {
        return try self.vtable.submit(self.ptr, job);
    }

    pub fn drainOwner(self: DurableJobLane, owner_id: u64) void {
        self.vtable.drain_owner(self.ptr, owner_id);
    }

    pub fn closeOwner(self: DurableJobLane, owner_id: u64) void {
        self.vtable.close_owner(self.ptr, owner_id);
    }

    /// Pausing closes admission for new jobs without canceling work that has
    /// already committed to the lane. Drain may be used after pause to reach a
    /// stable quiescent owner state.
    pub fn pauseOwner(self: DurableJobLane, owner_id: u64) !void {
        try self.vtable.pause_owner(self.ptr, owner_id);
    }

    pub fn resumeOwner(self: DurableJobLane, owner_id: u64) !void {
        try self.vtable.resume_owner(self.ptr, owner_id);
    }

    /// Re-registers the same logical owner only after close has drained and
    /// retired its prior generation.
    pub fn reopenOwner(self: DurableJobLane, owner_id: u64) !void {
        try self.vtable.reopen_owner(self.ptr, owner_id);
    }

    pub fn poll(self: DurableJobLane, max_jobs: usize) !usize {
        return try self.vtable.poll(self.ptr, max_jobs);
    }

    /// Whether the lane still admits successor work. Durable jobs use this to
    /// make delayed retries responsive to runtime shutdown while leaving their
    /// on-disk intent available for reconciliation on the next open.
    pub fn isAccepting(self: DurableJobLane) bool {
        const callback = self.vtable.is_accepting orelse return true;
        return callback(self.ptr);
    }

    /// Manual runtimes execute submissions on the caller's stack. Workers
    /// that page durable work must leave the marker pending for a later
    /// explicit poll/reopen instead of recursively submitting their successor.
    pub fn executesInline(self: DurableJobLane) bool {
        return self.vtable.executes_inline;
    }
};

pub const Job = struct {
    owner_id: u64,
    class: Class,
    ptr: *anyopaque,
    run: *const fn (ptr: *anyopaque) anyerror!void,
    deinit: *const fn (ptr: *anyopaque) void,

    pub const Class = enum {
        commit_durable,
        maintenance,
        cleanup,
    };
};

fn initIoLane(alloc: Allocator, concurrent_limit: u32) !*IoImpl {
    if (comptime builtin.os.tag == .freestanding) {
        return error.UnsupportedPlatform;
    } else {
        const io_impl = try alloc.create(IoImpl);
        errdefer alloc.destroy(io_impl);
        // Backend runtimes are process-long and own several independent I/O
        // lanes. Threaded retains concurrent workers until deinit, so a finite
        // ceiling prevents any lane from converting a transient fan-out spike
        // into an unbounded kernel-thread/stack reservation ratchet.
        io_impl.* = Io.Threaded.init(alloc, .{
            .concurrent_limit = .limited(concurrent_limit),
        });
        return io_impl;
    }
}

fn deinitIoLane(alloc: Allocator, io_impl: *IoImpl) void {
    if (comptime builtin.os.tag != .freestanding) {
        io_impl.deinit();
    }
    alloc.destroy(io_impl);
}

const OwnerRegistry = struct {
    const State = struct {
        closing: bool = false,
        paused: bool = false,
        in_flight: usize = 0,
    };

    alloc: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    states: std.AutoHashMapUnmanaged(u64, State) = .empty,

    fn init(alloc: Allocator) OwnerRegistry {
        return .{ .alloc = alloc };
    }

    fn deinit(self: *OwnerRegistry) void {
        var iterator = self.states.valueIterator();
        while (iterator.next()) |state| std.debug.assert(state.in_flight == 0);
        self.states.deinit(self.alloc);
        self.* = undefined;
    }

    fn register(self: *OwnerRegistry, owner_id: u64) !void {
        if (owner_id == 0) return error.InvalidBackgroundOwner;
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.states.contains(owner_id)) return error.BackgroundOwnerIdExhausted;
        try self.states.putNoClobber(self.alloc, owner_id, .{});
    }

    fn beginJob(self: *OwnerRegistry, owner_id: u64) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return error.BackgroundOwnerClosed;
        if (state.closing) return error.BackgroundOwnerClosing;
        if (state.paused) return error.BackgroundOwnerPaused;
        if (state.in_flight == std.math.maxInt(usize)) return error.BackgroundOwnerCapacityExceeded;
        state.in_flight += 1;
    }

    fn setPaused(self: *OwnerRegistry, owner_id: u64, paused: bool) !void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return error.BackgroundOwnerClosed;
        if (state.closing) return error.BackgroundOwnerClosing;
        state.paused = paused;
    }

    fn finishJob(self: *OwnerRegistry, owner_id: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse {
            std.debug.panic("background owner {} retired with a job in flight", .{owner_id});
        };
        std.debug.assert(state.in_flight > 0);
        state.in_flight -= 1;
    }

    fn beginClose(self: *OwnerRegistry, owner_id: u64) bool {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return false;
        state.closing = true;
        return true;
    }

    fn waitIdle(self: *OwnerRegistry, owner_id: u64) void {
        while (true) {
            lockAtomic(&self.mutex);
            const idle = if (self.states.getPtr(owner_id)) |state|
                state.in_flight == 0
            else
                true;
            self.mutex.unlock();
            if (idle) return;
            if (builtin.os.tag == .freestanding or builtin.single_threaded) {
                std.atomic.spinLoopHint();
            } else {
                std.Thread.yield() catch {};
            }
        }
    }

    fn retireClosed(self: *OwnerRegistry, owner_id: u64) void {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        const state = self.states.getPtr(owner_id) orelse return;
        std.debug.assert(state.closing);
        std.debug.assert(state.in_flight == 0);
        _ = self.states.remove(owner_id);
    }

    fn close(self: *OwnerRegistry, owner_id: u64) void {
        if (!self.beginClose(owner_id)) return;
        self.waitIdle(owner_id);
        self.retireClosed(owner_id);
    }
};

pub const BackendRuntime = struct {
    alloc: Allocator,
    backend: Backend,
    next_owner_id: AtomicU64,
    retired_generation_cleanup_owner_id: u64,
    owner_registry: *OwnerRegistry,
    native_storage_pool: *storage_io.NativeStoragePool,
    borrowed_storage: ?storage_io.IoStorage = null,
    lsm_owner_clone_registry: LsmOwnerCloneRegistry,
    borrowed_filesystem_io: ?Io = null,
    io_impl: ?*IoImpl = null,
    raft_inbound_io_impl: ?*IoImpl = null,
    raft_outbound_io_impl: ?*IoImpl = null,
    api_io_impl: ?*IoImpl = null,
    inference_io_impl: ?*IoImpl = null,
    control_io_impl: ?*IoImpl = null,
    borrowed_io: ?BorrowedIo = null,
    api_lane_gate: LaneLeaseGate = .{},
    api_lane_peak_leases: std.atomic.Value(usize) = .init(0),
    api_lane_acquisitions_total: std.atomic.Value(u64) = .init(0),
    api_lane_rejections_total: std.atomic.Value(u64) = .init(0),
    inference_lane_gate: LaneLeaseGate = .{},
    inference_lane_peak_leases: std.atomic.Value(usize) = .init(0),
    inference_lane_acquisitions_total: std.atomic.Value(u64) = .init(0),
    inference_lane_rejections_total: std.atomic.Value(u64) = .init(0),
    control_lane_gate: LaneLeaseGate = .{},
    control_lane_peak_leases: std.atomic.Value(usize) = .init(0),
    control_lane_acquisitions_total: std.atomic.Value(u64) = .init(0),
    control_lane_rejections_total: std.atomic.Value(u64) = .init(0),
    threaded_jobs: ?*ThreadedDurableJobLane = null,
    durable_jobs: DurableJobLane,
    db_open_configurator: ?DbOpenConfigurator = null,

    pub fn init(alloc: Allocator, config: Config) !BackendRuntime {
        try runtime_backend.ensureExecutorBackendAvailable(config.backend);
        if (config.borrowed_io != null and config.backend != .manual)
            return error.BorrowedIoRequiresManualBackend;

        const owner_registry = try alloc.create(OwnerRegistry);
        errdefer alloc.destroy(owner_registry);
        owner_registry.* = OwnerRegistry.init(alloc);
        errdefer owner_registry.deinit();
        const retired_generation_cleanup_owner_id: u64 = 1;
        try owner_registry.register(retired_generation_cleanup_owner_id);

        const native_storage_pool = try alloc.create(storage_io.NativeStoragePool);
        errdefer alloc.destroy(native_storage_pool);
        native_storage_pool.* = storage_io.NativeStoragePool.init(alloc);
        errdefer native_storage_pool.deinit();

        var runtime = BackendRuntime{
            .alloc = alloc,
            .backend = config.backend,
            .next_owner_id = .init(retired_generation_cleanup_owner_id + 1),
            .retired_generation_cleanup_owner_id = retired_generation_cleanup_owner_id,
            .owner_registry = owner_registry,
            .native_storage_pool = native_storage_pool,
            .lsm_owner_clone_registry = LsmOwnerCloneRegistry.init(alloc),
            .borrowed_filesystem_io = config.filesystem_io,
            .durable_jobs = undefined,
            .borrowed_io = config.borrowed_io,
            .borrowed_storage = if (config.borrowed_io) |borrowed| storage_io.IoStorage.init(borrowed.general) else null,
        };
        runtime.durable_jobs = InlineDurableJobLane.lane(owner_registry);

        if (config.backend != .manual) {
            if (comptime builtin.os.tag == .freestanding) {
                return error.UnsupportedPlatform;
            } else {
                const io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, io_impl);
                const raft_inbound_io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, raft_inbound_io_impl);
                const raft_outbound_io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, raft_outbound_io_impl);
                const api_io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, api_io_impl);
                const inference_io_impl = try initIoLane(alloc, threaded_io_limits.inference);
                errdefer deinitIoLane(alloc, inference_io_impl);
                const control_io_impl = try initIoLane(alloc, threaded_io_limits.service);
                errdefer deinitIoLane(alloc, control_io_impl);

                const threaded_jobs = try alloc.create(ThreadedDurableJobLane);
                errdefer alloc.destroy(threaded_jobs);
                threaded_jobs.* = ThreadedDurableJobLane.init(alloc, io_impl, owner_registry);
                try threaded_jobs.start();
                errdefer threaded_jobs.deinit();

                runtime.io_impl = io_impl;
                runtime.raft_inbound_io_impl = raft_inbound_io_impl;
                runtime.raft_outbound_io_impl = raft_outbound_io_impl;
                runtime.api_io_impl = api_io_impl;
                runtime.inference_io_impl = inference_io_impl;
                runtime.control_io_impl = control_io_impl;
                runtime.threaded_jobs = threaded_jobs;
                runtime.durable_jobs = threaded_jobs.lane();
            }
        }

        return runtime;
    }

    pub fn deinit(self: *BackendRuntime) void {
        // Close every lane before waiting for any one of them. Otherwise a
        // borrower could continue entering a later lane while teardown drains
        // an earlier one. These waits are production lifetime enforcement,
        // not debug-only diagnostics: no executor is destroyed while a lease
        // can still expose its std.Io interface.
        const coordinator_io = self.io();
        self.api_lane_gate.close();
        self.inference_lane_gate.close();
        self.control_lane_gate.close();
        self.api_lane_gate.waitDrained(coordinator_io);
        self.inference_lane_gate.waitDrained(coordinator_io);
        self.control_lane_gate.waitDrained(coordinator_io);
        if (self.threaded_jobs) |jobs| {
            jobs.deinit();
            self.alloc.destroy(jobs);
            self.threaded_jobs = null;
        }
        if (self.api_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.api_io_impl = null;
        }
        if (self.inference_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.inference_io_impl = null;
        }
        if (self.control_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.control_io_impl = null;
        }
        if (self.raft_outbound_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.raft_outbound_io_impl = null;
        }
        if (self.raft_inbound_io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.raft_inbound_io_impl = null;
        }
        if (self.io_impl) |io_impl| {
            deinitIoLane(self.alloc, io_impl);
            self.io_impl = null;
        }
        self.native_storage_pool.deinit();
        self.alloc.destroy(self.native_storage_pool);
        self.lsm_owner_clone_registry.deinit();
        self.owner_registry.deinit();
        self.alloc.destroy(self.owner_registry);
        self.* = undefined;
    }

    pub fn io(self: *BackendRuntime) ?Io {
        if (self.borrowed_io) |borrowed| return borrowed.general;
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.io_impl) |io_impl| io_impl.io() else null;
    }

    pub fn usesBorrowedIo(self: *const BackendRuntime) bool {
        return self.borrowed_io != null;
    }

    /// I/O authority for synchronous storage work. Unlike `io`, this may be
    /// caller-owned and does not imply that background scheduling is enabled.
    pub fn filesystemIo(self: *BackendRuntime) ?Io {
        return self.io() orelse self.borrowed_filesystem_io;
    }

    pub fn nativeStoragePool(self: *BackendRuntime) *storage_io.NativeStoragePool {
        return self.native_storage_pool;
    }

    pub fn storage(self: *BackendRuntime) ?storage_io.Storage {
        if (self.borrowed_storage) |*borrowed| return borrowed.storage();
        return null;
    }

    pub fn snapshotNativeStorageStats(self: *const BackendRuntime) storage_io.NativeStorageStats {
        return self.native_storage_pool.snapshotStats();
    }

    /// Installs a process-local DB-open policy for composed simulations and
    /// embedded runtimes. The configurator is borrowed and must outlive this
    /// runtime and every DB opened through it.
    pub fn setDbOpenConfigurator(self: *BackendRuntime, configurator: ?DbOpenConfigurator) void {
        self.db_open_configurator = configurator;
    }

    pub fn hasDbOpenConfigurator(self: *const BackendRuntime) bool {
        return self.db_open_configurator != null;
    }

    pub fn accumulateRetiredLsmOwnerCloneStats(
        self: *BackendRuntime,
        table_name: []const u8,
        group_id: u64,
        owner_kind: LsmOwnerKind,
        owner_name: []const u8,
        owner_overflow: bool,
        stats: LsmOwnerCloneStats,
    ) !void {
        try self.lsm_owner_clone_registry.accumulate(
            table_name,
            group_id,
            owner_kind,
            owner_name,
            owner_overflow,
            stats,
        );
    }

    pub fn observeLsmOwnerCloneStats(
        self: *BackendRuntime,
        source_id: usize,
        table_name: []const u8,
        group_id: u64,
        owner_kind: LsmOwnerKind,
        owner_name: []const u8,
        owner_overflow: bool,
        stats: LsmOwnerCloneStats,
    ) !void {
        try self.lsm_owner_clone_registry.observe(
            source_id,
            table_name,
            group_id,
            owner_kind,
            owner_name,
            owner_overflow,
            stats,
        );
    }

    pub fn retireLsmOwnerCloneSource(self: *BackendRuntime, source_id: usize) void {
        self.lsm_owner_clone_registry.retireSource(source_id);
    }

    pub fn snapshotRetiredLsmOwnerCloneStatsAlloc(
        self: *BackendRuntime,
        alloc: Allocator,
    ) ![]LsmOwnerCloneMetricSnapshot {
        return try self.lsm_owner_clone_registry.snapshotAlloc(alloc);
    }

    pub fn retiredLsmOwnerCloneStatsDroppedTotal(self: *BackendRuntime) u64 {
        return self.lsm_owner_clone_registry.droppedObservationsTotal();
    }

    pub fn retiredLsmOwnerCloneLabelsCollapsedTotal(self: *BackendRuntime) u64 {
        return self.lsm_owner_clone_registry.collapsedLabelsTotal();
    }

    pub fn raftInboundIo(self: *BackendRuntime) ?Io {
        if (self.borrowed_io) |borrowed| return borrowed.raft_inbound orelse borrowed.general;
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.raft_inbound_io_impl) |io_impl| io_impl.io() else self.io();
    }

    pub fn raftInboundIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (self.borrowed_io != null) return null;
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.raft_inbound_io_impl orelse self.io_impl;
    }

    pub fn raftOutboundIo(self: *BackendRuntime) ?Io {
        if (self.borrowed_io) |borrowed| return borrowed.raft_outbound orelse borrowed.general;
        if (comptime builtin.os.tag == .freestanding) return null;
        return if (self.raft_outbound_io_impl) |io_impl| io_impl.io() else self.io();
    }

    pub fn raftOutboundIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (self.borrowed_io != null) return null;
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.raft_outbound_io_impl orelse self.io_impl;
    }

    pub fn apiIoImpl(self: *BackendRuntime) ?*IoImpl {
        if (self.borrowed_io != null) return null;
        if (comptime builtin.os.tag == .freestanding) return null;
        return self.api_io_impl orelse self.io_impl;
    }

    /// Returns the API executor interface without exposing its implementation.
    /// Components own and await the tasks they submit; BackendRuntime only owns
    /// the executor lane and must outlive every borrower.
    pub fn apiIo(self: *BackendRuntime) ?Io {
        if (self.borrowed_io) |borrowed| return borrowed.api orelse borrowed.general;
        const io_impl = self.apiIoImpl() orelse return null;
        return io_impl.io();
    }

    pub const ApiLaneLease = struct {
        runtime: *BackendRuntime,
        borrowed_io: Io,
        concurrent_capacity: u32,
        released: bool = false,

        pub fn io(self: *const ApiLaneLease) Io {
            std.debug.assert(!self.released);
            return self.borrowed_io;
        }

        pub fn concurrentCapacity(self: *const ApiLaneLease) u32 {
            std.debug.assert(!self.released);
            return self.concurrent_capacity;
        }

        pub fn release(self: *ApiLaneLease) void {
            if (self.released) return;
            self.released = true;
            self.runtime.api_lane_gate.release(self.runtime.io());
        }
    };

    /// Acquires an explicit lifetime lease for the API executor lane. The
    /// caller must stop and await every submitted task before releasing it.
    pub fn acquireApiLane(self: *BackendRuntime) !ApiLaneLease {
        const leases = self.api_lane_gate.tryAcquire() orelse {
            _ = self.api_lane_rejections_total.fetchAdd(1, .monotonic);
            return error.BackendRuntimeShuttingDown;
        };
        errdefer self.api_lane_gate.release(self.io());
        const borrowed_io = self.apiIo() orelse return error.BackendRuntimeUnavailable;
        updateAtomicMax(&self.api_lane_peak_leases, leases);
        _ = self.api_lane_acquisitions_total.fetchAdd(1, .monotonic);
        return .{
            .runtime = self,
            .borrowed_io = borrowed_io,
            .concurrent_capacity = threaded_io_limits.service,
        };
    }

    pub fn outstandingApiLeases(self: *const BackendRuntime) usize {
        return self.api_lane_gate.active();
    }

    /// Executor isolated for inference graph I/O, model loading, and nested
    /// fan-out. A lifetime lease is required because the linked inference
    /// archive retains a copy of the interface until its node is destroyed.
    pub fn inferenceIo(self: *BackendRuntime) ?Io {
        if (self.borrowed_io) |borrowed| return borrowed.inference orelse borrowed.general;
        if (comptime builtin.os.tag == .freestanding) return null;
        const io_impl = self.inference_io_impl orelse self.io_impl orelse return null;
        return io_impl.io();
    }

    pub const InferenceLaneLease = struct {
        runtime: *BackendRuntime,
        borrowed_io: Io,
        released: bool = false,

        pub fn io(self: *const InferenceLaneLease) Io {
            std.debug.assert(!self.released);
            return self.borrowed_io;
        }

        pub fn release(self: *InferenceLaneLease) void {
            if (self.released) return;
            self.released = true;
            self.runtime.inference_lane_gate.release(self.runtime.io());
        }
    };

    pub fn acquireInferenceLane(self: *BackendRuntime) !InferenceLaneLease {
        const leases = self.inference_lane_gate.tryAcquire() orelse {
            _ = self.inference_lane_rejections_total.fetchAdd(1, .monotonic);
            return error.BackendRuntimeShuttingDown;
        };
        errdefer self.inference_lane_gate.release(self.io());
        const borrowed_io = self.inferenceIo() orelse return error.BackendRuntimeUnavailable;
        updateAtomicMax(&self.inference_lane_peak_leases, leases);
        _ = self.inference_lane_acquisitions_total.fetchAdd(1, .monotonic);
        return .{ .runtime = self, .borrowed_io = borrowed_io };
    }

    pub fn outstandingInferenceLeases(self: *const BackendRuntime) usize {
        return self.inference_lane_gate.active();
    }

    /// Reserved control-plane executor for health, metrics, and shutdown
    /// coordination. It is intentionally isolated from public API work so
    /// overload cannot consume the runtime's last observable control path.
    pub fn controlIo(self: *BackendRuntime) ?Io {
        if (self.borrowed_io) |borrowed| return borrowed.control orelse borrowed.general;
        if (comptime builtin.os.tag == .freestanding) return null;
        const io_impl = self.control_io_impl orelse self.io_impl orelse return null;
        return io_impl.io();
    }

    pub const ControlLaneLease = struct {
        runtime: *BackendRuntime,
        borrowed_io: Io,
        released: bool = false,

        pub fn io(self: *const ControlLaneLease) Io {
            std.debug.assert(!self.released);
            return self.borrowed_io;
        }

        pub fn release(self: *ControlLaneLease) void {
            if (self.released) return;
            self.released = true;
            self.runtime.control_lane_gate.release(self.runtime.io());
        }
    };

    pub fn acquireControlLane(self: *BackendRuntime) !ControlLaneLease {
        const leases = self.control_lane_gate.tryAcquire() orelse {
            _ = self.control_lane_rejections_total.fetchAdd(1, .monotonic);
            return error.BackendRuntimeShuttingDown;
        };
        errdefer self.control_lane_gate.release(self.io());
        const borrowed_io = self.controlIo() orelse return error.BackendRuntimeUnavailable;
        updateAtomicMax(&self.control_lane_peak_leases, leases);
        _ = self.control_lane_acquisitions_total.fetchAdd(1, .monotonic);
        return .{ .runtime = self, .borrowed_io = borrowed_io };
    }

    pub fn outstandingControlLeases(self: *const BackendRuntime) usize {
        return self.control_lane_gate.active();
    }

    pub const LaneStats = struct {
        api_active_leases: usize,
        api_peak_leases: usize,
        api_acquisitions_total: u64,
        api_rejections_total: u64,
        inference_active_leases: usize,
        inference_peak_leases: usize,
        inference_acquisitions_total: u64,
        inference_rejections_total: u64,
        control_active_leases: usize,
        control_peak_leases: usize,
        control_acquisitions_total: u64,
        control_rejections_total: u64,
    };

    pub fn laneStats(self: *const BackendRuntime) LaneStats {
        return .{
            .api_active_leases = self.api_lane_gate.active(),
            .api_peak_leases = self.api_lane_peak_leases.load(.acquire),
            .api_acquisitions_total = self.api_lane_acquisitions_total.load(.acquire),
            .api_rejections_total = self.api_lane_rejections_total.load(.acquire),
            .inference_active_leases = self.inference_lane_gate.active(),
            .inference_peak_leases = self.inference_lane_peak_leases.load(.acquire),
            .inference_acquisitions_total = self.inference_lane_acquisitions_total.load(.acquire),
            .inference_rejections_total = self.inference_lane_rejections_total.load(.acquire),
            .control_active_leases = self.control_lane_gate.active(),
            .control_peak_leases = self.control_lane_peak_leases.load(.acquire),
            .control_acquisitions_total = self.control_lane_acquisitions_total.load(.acquire),
            .control_rejections_total = self.control_lane_rejections_total.load(.acquire),
        };
    }

    fn updateAtomicMax(counter: *std.atomic.Value(usize), value: usize) void {
        var observed = counter.load(.acquire);
        while (observed < value) {
            if (counter.cmpxchgWeak(observed, value, .acq_rel, .acquire) == null) return;
            observed = counter.load(.acquire);
        }
    }

    pub fn allocOwnerId(self: *BackendRuntime) !u64 {
        while (true) {
            const owner_id = self.next_owner_id.fetchAdd(1, .monotonic);
            if (owner_id == 0) continue;
            try self.owner_registry.register(owner_id);
            return owner_id;
        }
    }
};

pub const BackendRuntimeHandle = struct {
    alloc: Allocator,
    runtime: *BackendRuntime,
    /// Filesystem executor owned by this handle and lent to a manual runtime.
    /// Keeping the authority in the same move-only value as the runtime makes
    /// runtime handoff atomic and prevents callers from preserving one while
    /// accidentally destroying the other.
    owned_filesystem_io: ?*IoImpl = null,

    pub fn init(alloc: Allocator, config: Config) !BackendRuntimeHandle {
        const runtime = try alloc.create(BackendRuntime);
        errdefer alloc.destroy(runtime);
        runtime.* = try BackendRuntime.init(alloc, config);
        return .{
            .alloc = alloc,
            .runtime = runtime,
        };
    }

    pub fn initManualWithOwnedFilesystemIo(alloc: Allocator) !BackendRuntimeHandle {
        if (comptime builtin.os.tag == .freestanding) return error.UnsupportedPlatform;
        const filesystem_io = try initIoLane(alloc, threaded_io_limits.service);
        errdefer deinitIoLane(alloc, filesystem_io);
        var handle = try init(alloc, .{
            .backend = .manual,
            .filesystem_io = filesystem_io.io(),
        });
        handle.owned_filesystem_io = filesystem_io;
        return handle;
    }

    pub fn deinit(self: *BackendRuntimeHandle) void {
        self.runtime.deinit();
        self.alloc.destroy(self.runtime);
        if (self.owned_filesystem_io) |io_impl| deinitIoLane(self.alloc, io_impl);
        self.* = undefined;
    }

    pub fn ownsFilesystemIo(self: *const BackendRuntimeHandle) bool {
        return self.owned_filesystem_io != null;
    }

    pub fn ptr(self: *BackendRuntimeHandle) *BackendRuntime {
        return self.runtime;
    }
};

const InlineDurableJobLane = struct {
    fn lane(owners: *OwnerRegistry) DurableJobLane {
        return .{
            .ptr = owners,
            .vtable = &inline_vtable,
        };
    }

    fn submit(ptr: *anyopaque, job: Job) !void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        try owners.beginJob(job.owner_id);
        defer owners.finishJob(job.owner_id);
        try job.run(job.ptr);
        job.deinit(job.ptr);
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        owners.waitIdle(owner_id);
    }

    fn closeOwner(ptr: *anyopaque, owner_id: u64) void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        owners.close(owner_id);
    }

    fn pauseOwner(ptr: *anyopaque, owner_id: u64) !void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        try owners.setPaused(owner_id, true);
    }

    fn resumeOwner(ptr: *anyopaque, owner_id: u64) !void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        try owners.setPaused(owner_id, false);
    }

    fn reopenOwner(ptr: *anyopaque, owner_id: u64) !void {
        const owners: *OwnerRegistry = @ptrCast(@alignCast(ptr));
        try owners.register(owner_id);
    }

    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }
};

const inline_vtable = DurableJobLane.VTable{
    .submit = InlineDurableJobLane.submit,
    .drain_owner = InlineDurableJobLane.drainOwner,
    .close_owner = InlineDurableJobLane.closeOwner,
    .pause_owner = InlineDurableJobLane.pauseOwner,
    .resume_owner = InlineDurableJobLane.resumeOwner,
    .reopen_owner = InlineDurableJobLane.reopenOwner,
    .poll = InlineDurableJobLane.poll,
    .executes_inline = true,
};

const ThreadedDurableJobLane = if (builtin.os.tag == .freestanding) struct {
    fn init(_: Allocator, _: *IoImpl, _: *OwnerRegistry) ThreadedDurableJobLane {
        return .{};
    }

    fn start(_: *ThreadedDurableJobLane) !void {}

    fn lane(self: *ThreadedDurableJobLane) DurableJobLane {
        return .{
            .ptr = self,
            .vtable = &threaded_vtable,
        };
    }

    fn deinit(_: *ThreadedDurableJobLane) void {}

    fn submit(_: *anyopaque, _: Job) !void {
        return error.UnsupportedPlatform;
    }

    fn drainOwner(_: *anyopaque, _: u64) void {}

    fn closeOwner(_: *anyopaque, _: u64) void {}

    fn pauseOwner(_: *anyopaque, _: u64) !void {
        return error.UnsupportedPlatform;
    }

    fn resumeOwner(_: *anyopaque, _: u64) !void {
        return error.UnsupportedPlatform;
    }

    fn reopenOwner(_: *anyopaque, _: u64) !void {
        return error.UnsupportedPlatform;
    }

    fn poll(_: *anyopaque, _: usize) !usize {
        return 0;
    }

    fn isAccepting(_: *anyopaque) bool {
        return false;
    }
} else struct {
    const Entry = struct {
        lane: *ThreadedDurableJobLane,
        job: Job,
        future: Io.Future(void),
        completed: std.atomic.Value(bool) = .init(false),
        job_deinited: std.atomic.Value(bool) = .init(false),

        fn deinitJobOnce(self: *Entry) void {
            if (self.job_deinited.swap(true, .acq_rel)) return;
            self.job.deinit(self.job.ptr);
        }
    };

    const reap_batch_limit: usize = 4096;
    const idle_reap_interval_ms: u64 = 10;

    alloc: Allocator,
    io_impl: *IoImpl,
    owners: *OwnerRegistry,
    mutex: std.atomic.Mutex = .unlocked,
    reap_mutex: std.atomic.Mutex = .unlocked,
    shutdown_reaper: std.atomic.Value(bool) = .init(false),
    completed_count: std.atomic.Value(usize) = .init(0),
    accepting: std.atomic.Value(bool) = .init(true),
    reaper_future: ?Io.Future(void) = null,
    entries: std.ArrayListUnmanaged(*Entry) = .empty,

    fn init(alloc: Allocator, io_impl: *IoImpl, owners: *OwnerRegistry) ThreadedDurableJobLane {
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .owners = owners,
        };
    }

    fn start(self: *ThreadedDurableJobLane) !void {
        self.reaper_future = try self.io_impl.io().concurrent(reaperLoop, .{self});
    }

    fn lane(self: *ThreadedDurableJobLane) DurableJobLane {
        return .{
            .ptr = self,
            .vtable = &threaded_vtable,
        };
    }

    fn deinit(self: *ThreadedDurableJobLane) void {
        self.accepting.store(false, .release);
        self.shutdown_reaper.store(true, .release);
        if (self.reaper_future) |*future| {
            _ = future.await(self.io_impl.io());
            self.reaper_future = null;
        }
        self.drainAll();
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    fn submit(ptr: *anyopaque, job: Job) !void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        if (!self.accepting.load(.acquire)) return error.BackendRuntimeShuttingDown;
        try self.owners.beginJob(job.owner_id);
        errdefer self.owners.finishJob(job.owner_id);
        if (!self.accepting.load(.acquire)) return error.BackendRuntimeShuttingDown;
        const entry = try self.alloc.create(Entry);
        entry.* = .{
            .lane = self,
            .job = job,
            .future = undefined,
        };
        errdefer self.alloc.destroy(entry);

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        try self.entries.ensureUnusedCapacity(self.alloc, 1);
        entry.future = try self.io_impl.io().concurrent(runEntry, .{entry});
        self.entries.appendAssumeCapacity(entry);
    }

    fn drainOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        self.drainMatching(owner_id);
    }

    fn closeOwner(ptr: *anyopaque, owner_id: u64) void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        if (!self.owners.beginClose(owner_id)) return;
        self.drainMatching(owner_id);
        self.owners.waitIdle(owner_id);
        self.owners.retireClosed(owner_id);
    }

    fn pauseOwner(ptr: *anyopaque, owner_id: u64) !void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        try self.owners.setPaused(owner_id, true);
    }

    fn resumeOwner(ptr: *anyopaque, owner_id: u64) !void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        try self.owners.setPaused(owner_id, false);
    }

    fn reopenOwner(ptr: *anyopaque, owner_id: u64) !void {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        try self.owners.register(owner_id);
    }

    fn poll(ptr: *anyopaque, max_jobs: usize) !usize {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        return self.reapCompleted(max_jobs);
    }

    fn isAccepting(ptr: *anyopaque) bool {
        const self: *ThreadedDurableJobLane = @ptrCast(@alignCast(ptr));
        return self.accepting.load(.acquire);
    }

    fn runEntry(entry: *Entry) void {
        entry.job.run(entry.job.ptr) catch |err| {
            std.log.warn("background durable job failed owner={} class={s} err={s}", .{
                entry.job.owner_id,
                @tagName(entry.job.class),
                @errorName(err),
            });
        };
        // The payload often owns the transaction state and buffers that make
        // a durable job large. Release it on the worker at the actual lifetime
        // boundary instead of retaining it until the bookkeeping reaper joins
        // the already-completed future. `deinitJobOnce` also makes concurrent
        // owner drains safe.
        entry.deinitJobOnce();
        // Publish the count first. It is only a wake/drain hint; the release
        // store below remains authoritative. Publishing in this order also
        // prevents a reaper from freeing `entry` before this worker's last
        // access to it.
        entry.lane.owners.finishJob(entry.job.owner_id);
        _ = entry.lane.completed_count.fetchAdd(1, .monotonic);
        entry.completed.store(true, .release);
    }

    fn reaperLoop(self: *ThreadedDurableJobLane) void {
        while (!self.shutdown_reaper.load(.acquire)) {
            const reaped = self.reapCompleted(reap_batch_limit);
            // Drain a backlog without an artificial rate cap. At idle, a
            // short sleep avoids scanning the active set continuously.
            if (reaped == reap_batch_limit or self.completed_count.load(.monotonic) > 0) continue;
            self.io_impl.io().sleep(Io.Duration.fromMilliseconds(idle_reap_interval_ms), .awake) catch {};
        }
        while (self.reapCompleted(reap_batch_limit) > 0) {}
    }

    fn drainAll(self: *ThreadedDurableJobLane) void {
        while (true) {
            // Detaching under reap_mutex gives this caller exclusive ownership
            // of the entry. Do not retain the global reap lock while awaiting
            // the job: a durable job may close a nested DB/background owner,
            // which must be allowed to enter this lane and drain its own jobs.
            lockAtomic(&self.reap_mutex);
            const entry = self.popAny();
            self.reap_mutex.unlock();
            const detached = entry orelse return;
            self.awaitAndDestroy(detached);
        }
    }

    fn drainMatching(self: *ThreadedDurableJobLane, owner_id: u64) void {
        while (true) {
            // See drainAll: awaiting outside reap_mutex is required for nested
            // owner teardown and is safe once the entry has been detached.
            lockAtomic(&self.reap_mutex);
            const entry = self.popOwner(owner_id);
            self.reap_mutex.unlock();
            const detached = entry orelse return;
            self.awaitAndDestroy(detached);
        }
    }

    fn reapCompleted(self: *ThreadedDurableJobLane, max_jobs: usize) usize {
        if (max_jobs == 0 or self.completed_count.load(.monotonic) == 0) return 0;
        lockAtomic(&self.reap_mutex);
        defer self.reap_mutex.unlock();

        // Detach completed entries in one pass. The previous implementation
        // repeatedly called orderedRemove, shifting the entire tail for every
        // completed job. A large ingest could therefore retain millions of
        // finished payloads while spending most of a core in memmove.
        var detached: [reap_batch_limit]*Entry = undefined;
        const target = @min(max_jobs, detached.len);
        var detached_count: usize = 0;
        lockAtomic(&self.mutex);
        var idx: usize = 0;
        while (idx < self.entries.items.len and detached_count < target) {
            const entry = self.entries.items[idx];
            if (!entry.completed.load(.acquire)) {
                idx += 1;
                continue;
            }
            detached[detached_count] = entry;
            detached_count += 1;
            _ = self.entries.swapRemove(idx);
        }
        self.mutex.unlock();

        for (detached[0..detached_count]) |entry| self.awaitAndDestroy(entry);
        return detached_count;
    }

    fn popAny(self: *ThreadedDurableJobLane) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        if (self.entries.items.len == 0) return null;
        return self.entries.swapRemove(0);
    }

    fn popOwner(self: *ThreadedDurableJobLane, owner_id: u64) ?*Entry {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        for (self.entries.items, 0..) |entry, idx| {
            if (entry.job.owner_id == owner_id) return self.entries.swapRemove(idx);
        }
        return null;
    }

    fn awaitAndDestroy(self: *ThreadedDurableJobLane, entry: *Entry) void {
        _ = entry.future.await(self.io_impl.io());
        if (entry.completed.swap(false, .acq_rel)) {
            _ = self.completed_count.fetchSub(1, .monotonic);
        }
        entry.deinitJobOnce();
        self.alloc.destroy(entry);
    }
};

const threaded_vtable = DurableJobLane.VTable{
    .submit = ThreadedDurableJobLane.submit,
    .drain_owner = ThreadedDurableJobLane.drainOwner,
    .close_owner = ThreadedDurableJobLane.closeOwner,
    .pause_owner = ThreadedDurableJobLane.pauseOwner,
    .resume_owner = ThreadedDurableJobLane.resumeOwner,
    .reopen_owner = ThreadedDurableJobLane.reopenOwner,
    .poll = ThreadedDurableJobLane.poll,
    .is_accepting = ThreadedDurableJobLane.isAccepting,
};

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (builtin.os.tag == .freestanding or builtin.single_threaded) {
            std.atomic.spinLoopHint();
            continue;
        }
        std.Thread.yield() catch {};
    }
}

test "lane lease gate closes admission and drains a committed borrower" {
    if (builtin.os.tag == .freestanding) return;

    var gate = LaneLeaseGate{};
    try std.testing.expectEqual(@as(?usize, 1), gate.tryAcquire());

    var drained = std.atomic.Value(bool).init(false);
    const closer = try std.Thread.spawn(.{}, struct {
        fn run(g: *LaneLeaseGate, done: *std.atomic.Value(bool)) void {
            g.close();
            g.waitDrained(null);
            done.store(true, .release);
        }
    }.run, .{ &gate, &drained });

    while (!gate.isClosed()) std.Thread.yield() catch {};
    try std.testing.expectEqual(@as(?usize, null), gate.tryAcquire());
    try std.testing.expect(!drained.load(.acquire));
    gate.release(null);
    closer.join();
    try std.testing.expect(drained.load(.acquire));
}

test "backend runtime handle owns a stable runtime pointer" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .manual,
        .filesystem_io = std.testing.io,
    });
    defer handle.deinit();

    const first = handle.ptr();
    const second = handle.ptr();
    try std.testing.expect(first == second);
    try std.testing.expect(first.io_impl == null);
    try std.testing.expect(first.io() == null);
    try std.testing.expect(first.filesystemIo() != null);
}

test "backend runtime durable lane runs inline jobs" {
    const Ctx = struct {
        ran: bool = false,
        deinit_called: bool = false,
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran = true;
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called = true;
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    try std.testing.expect(handle.ptr().durable_jobs.executesInline());
    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    try std.testing.expect(ctx.ran);
    try std.testing.expect(ctx.deinit_called);
}

test "backend runtime durable lane leaves inline failed jobs owned by caller" {
    const Ctx = struct {
        ran: bool = false,
        deinit_called: bool = false,
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran = true;
            return error.ExpectedFailure;
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called = true;
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try std.testing.expectError(error.ExpectedFailure, handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));

    try std.testing.expect(ctx.ran);
    try std.testing.expect(!ctx.deinit_called);
    Fns.deinit(&ctx);
    try std.testing.expect(ctx.deinit_called);
}

test "backend runtime threaded durable lane sees initialized jobs" {
    if (builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Ctx = struct {
        ran: std.atomic.Value(bool) = .init(false),
        deinit_called: std.atomic.Value(bool) = .init(false),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran.store(true, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called.store(true, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctxs: [64]Ctx = [_]Ctx{.{}} ** 64;
    for (&ctxs) |*ctx| {
        try handle.ptr().durable_jobs.submit(.{
            .owner_id = owner_id,
            .class = .commit_durable,
            .ptr = ctx,
            .run = Fns.run,
            .deinit = Fns.deinit,
        });
    }
    handle.ptr().durable_jobs.drainOwner(owner_id);

    for (&ctxs) |*ctx| {
        try std.testing.expect(ctx.ran.load(.acquire));
        try std.testing.expect(ctx.deinit_called.load(.acquire));
    }
}

test "backend runtime threaded durable job can close a nested owner while its owner drains" {
    if (builtin.os.tag == .freestanding) return error.SkipZigTest;

    const Context = struct {
        lane: DurableJobLane,
        nested_owner_id: u64,
        outer_ran: std.atomic.Value(bool) = .init(false),
        nested_ran: std.atomic.Value(bool) = .init(false),
        outer_deinited: std.atomic.Value(bool) = .init(false),
        nested_deinited: std.atomic.Value(bool) = .init(false),

        fn runOuter(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lane.closeOwner(self.nested_owner_id);
            self.outer_ran.store(true, .release);
        }

        fn runNested(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.nested_ran.store(true, .release);
        }

        fn deinitOuter(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.outer_deinited.store(true, .release);
        }

        fn deinitNested(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.nested_deinited.store(true, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const outer_owner_id = try handle.ptr().allocOwnerId();
    const nested_owner_id = try handle.ptr().allocOwnerId();
    const lane = handle.ptr().durable_jobs;
    var context = Context{ .lane = lane, .nested_owner_id = nested_owner_id };
    try lane.submit(.{
        .owner_id = nested_owner_id,
        .class = .cleanup,
        .ptr = &context,
        .run = Context.runNested,
        .deinit = Context.deinitNested,
    });
    try lane.submit(.{
        .owner_id = outer_owner_id,
        .class = .maintenance,
        .ptr = &context,
        .run = Context.runOuter,
        .deinit = Context.deinitOuter,
    });

    lane.drainOwner(outer_owner_id);
    try std.testing.expect(context.outer_ran.load(.acquire));
    try std.testing.expect(context.nested_ran.load(.acquire));
    try std.testing.expect(context.outer_deinited.load(.acquire));
    try std.testing.expect(context.nested_deinited.load(.acquire));
}

test "backend runtime allocates stable nonzero owner ids" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const first = try handle.ptr().allocOwnerId();
    const second = try handle.ptr().allocOwnerId();

    try std.testing.expect(first != 0);
    try std.testing.expectEqual(first + 1, second);
}

test "backend runtime durable owner lifecycle pauses drains closes and reopens after handle relocation" {
    const Context = struct {
        runs: usize = 0,
        deinits: usize = 0,

        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.runs += 1;
        }

        fn deinitJob(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.deinits += 1;
        }
    };

    var original = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    // The runtime itself is heap-stable; moving its owning wrapper must not
    // invalidate lifecycle callbacks or retain the temporary wrapper address.
    var relocated = original;
    original = undefined;
    defer relocated.deinit();

    const owner_id = try relocated.ptr().allocOwnerId();
    const lane = relocated.ptr().durable_jobs;
    var context = Context{};
    try lane.pauseOwner(owner_id);
    try std.testing.expectError(error.BackgroundOwnerPaused, lane.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    }));
    try std.testing.expectEqual(@as(usize, 0), context.deinits);

    try lane.resumeOwner(owner_id);
    try lane.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    });
    lane.drainOwner(owner_id);
    lane.closeOwner(owner_id);
    try std.testing.expectError(error.BackgroundOwnerClosed, lane.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    }));

    try lane.reopenOwner(owner_id);
    try lane.submit(.{
        .owner_id = owner_id,
        .class = .cleanup,
        .ptr = &context,
        .run = Context.run,
        .deinit = Context.deinitJob,
    });
    lane.closeOwner(owner_id);
    try std.testing.expectEqual(@as(usize, 2), context.runs);
    try std.testing.expectEqual(@as(usize, 2), context.deinits);
}

test "backend runtime retains LSM owner clone counters across generations" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    var first = LsmOwnerCloneStats{
        .calls = 2,
        .bytes_total = 1024,
        .peak_bytes = 768,
        .bulk_current_scan_peak_active_bytes = 512,
    };
    first.by_reason[@intFromEnum(LsmMutableSnapshotReason.bulk_current_scan)] = .{
        .calls = 2,
        .bytes_total = 1024,
        .peak_bytes = 768,
    };
    try handle.ptr().accumulateRetiredLsmOwnerCloneStats("docs", 17, .dense_vector, "embedding", false, first);
    try handle.ptr().accumulateRetiredLsmOwnerCloneStats("docs", 17, .dense_vector, "embedding", false, .{
        .calls = 1,
        .bytes_total = 256,
        .peak_bytes = 256,
    });

    const snapshot = try handle.ptr().snapshotRetiredLsmOwnerCloneStatsAlloc(std.testing.allocator);
    defer {
        for (snapshot) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(snapshot);
    }
    try std.testing.expectEqual(@as(usize, 1), snapshot.len);
    try std.testing.expectEqualStrings("docs", snapshot[0].table_name);
    try std.testing.expectEqualStrings("embedding", snapshot[0].owner_name);
    try std.testing.expectEqual(@as(u64, 3), snapshot[0].stats.calls);
    try std.testing.expectEqual(@as(u64, 1280), snapshot[0].stats.bytes_total);
    try std.testing.expectEqual(@as(u64, 768), snapshot[0].stats.peak_bytes);
    try std.testing.expectEqual(@as(u64, 512), snapshot[0].stats.bulk_current_scan_peak_active_bytes);
}

test "backend runtime observes live clone counters monotonically across retirement" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();
    const runtime = handle.ptr();

    try runtime.observeLsmOwnerCloneStats(101, "docs", 17, .dense_vector, "embedding", false, .{
        .calls = 2,
        .bytes_total = 1024,
        .peak_bytes = 768,
        .labels_collapsed_total = 1,
    });
    try runtime.observeLsmOwnerCloneStats(101, "docs", 17, .dense_vector, "embedding", false, .{
        .calls = 5,
        .bytes_total = 4096,
        .peak_bytes = 2048,
        .labels_collapsed_total = 3,
    });
    runtime.retireLsmOwnerCloneSource(101);

    // A replacement generation starts its counters at zero. Its absolute
    // values add to, rather than replace, the retired generation.
    try runtime.observeLsmOwnerCloneStats(102, "docs", 17, .dense_vector, "embedding", false, .{
        .calls = 1,
        .bytes_total = 256,
        .peak_bytes = 256,
    });

    const snapshot = try runtime.snapshotRetiredLsmOwnerCloneStatsAlloc(std.testing.allocator);
    defer {
        for (snapshot) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(snapshot);
    }
    try std.testing.expectEqual(@as(usize, 1), snapshot.len);
    try std.testing.expectEqual(@as(u64, 6), snapshot[0].stats.calls);
    try std.testing.expectEqual(@as(u64, 4352), snapshot[0].stats.bytes_total);
    try std.testing.expectEqual(@as(u64, 2048), snapshot[0].stats.peak_bytes);
    try std.testing.expectEqual(@as(u64, 0), runtime.retiredLsmOwnerCloneStatsDroppedTotal());
    try std.testing.expectEqual(@as(u64, 3), runtime.retiredLsmOwnerCloneLabelsCollapsedTotal());
}

test "backend runtime keeps synthetic overflow owners distinct from user names" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();
    const runtime = handle.ptr();

    try runtime.observeLsmOwnerCloneStats(201, "docs", 17, .dense_vector, "__retired_owner_overflow__", false, .{
        .calls = 2,
        .bytes_total = 512,
    });
    try runtime.observeLsmOwnerCloneStats(201, "docs", 17, .dense_vector, "__retired_owner_overflow__", true, .{
        .calls = 3,
        .bytes_total = 1024,
        .labels_collapsed_total = 7,
    });

    const snapshot = try runtime.snapshotRetiredLsmOwnerCloneStatsAlloc(std.testing.allocator);
    defer {
        for (snapshot) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(snapshot);
    }
    try std.testing.expectEqual(@as(usize, 2), snapshot.len);
    var concrete_calls: ?u64 = null;
    var overflow_calls: ?u64 = null;
    for (snapshot) |entry| {
        if (entry.owner_overflow) {
            overflow_calls = entry.stats.calls;
        } else {
            concrete_calls = entry.stats.calls;
        }
    }
    try std.testing.expectEqual(@as(?u64, 2), concrete_calls);
    try std.testing.expectEqual(@as(?u64, 3), overflow_calls);
    try std.testing.expectEqual(@as(u64, 7), runtime.retiredLsmOwnerCloneLabelsCollapsedTotal());
}

test "LSM owner registry reports capacity loss in observation units" {
    var registry = LsmOwnerCloneRegistry.init(std.testing.allocator);
    defer registry.deinit();

    for (0..LsmOwnerCloneRegistry.max_entries) |i| {
        try registry.observe(i + 1, "docs", @intCast(i), .primary, "primary", false, .{ .calls = 1 });
    }
    try registry.observe(999_999, "docs", LsmOwnerCloneRegistry.max_entries, .primary, "primary", false, .{ .calls = 1 });
    try registry.observe(999_999, "docs", LsmOwnerCloneRegistry.max_entries, .primary, "primary", false, .{ .calls = 1 });

    try std.testing.expectEqual(@as(u64, 2), registry.droppedObservationsTotal());
    try std.testing.expectEqual(@as(u64, 0), registry.collapsedLabelsTotal());
}

test "LSM owner entry saturation rejects before reserving source storage" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var registry = LsmOwnerCloneRegistry.init(failing.allocator());
    defer registry.deinit();

    for (0..LsmOwnerCloneRegistry.max_entries) |i| {
        try registry.observe(i + 1, "docs", @intCast(i), .primary, "primary", false, .{ .calls = 1 });
    }
    // Fill the source list to its current allocation boundary so the old
    // reserve-before-label-cap ordering would necessarily allocate.
    var source_id = LsmOwnerCloneRegistry.max_entries + 1;
    while (registry.sources.items.len < registry.sources.capacity) : (source_id += 1) {
        try registry.observe(source_id, "docs", 0, .primary, "primary", false, .{ .calls = 1 });
    }
    try std.testing.expect(registry.sources.items.len < LsmOwnerCloneRegistry.max_sources);

    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;
    const dropped_before = registry.droppedObservationsTotal();
    try registry.observe(999_999, "other", 99_999, .primary, "primary", false, .{ .calls = 1 });
    try std.testing.expectEqual(dropped_before + 1, registry.droppedObservationsTotal());
    try std.testing.expectEqual(LsmOwnerCloneRegistry.max_entries, registry.entries.items.len);
}

test "LSM owner source saturation does not consume empty label entries" {
    var registry = LsmOwnerCloneRegistry.init(std.testing.allocator);
    defer registry.deinit();

    for (0..LsmOwnerCloneRegistry.max_sources) |i| {
        try registry.observe(i + 1, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 1 });
    }
    try std.testing.expectEqual(@as(usize, 1), registry.entries.items.len);
    try registry.observe(999_999, "other", 23, .primary, "primary", false, .{ .calls = 1 });
    try std.testing.expectEqual(@as(usize, 1), registry.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), registry.droppedObservationsTotal());

    registry.retireSource(1);
    try registry.observe(999_999, "other", 23, .primary, "primary", false, .{ .calls = 1 });
    try std.testing.expectEqual(@as(usize, 2), registry.entries.items.len);
    try std.testing.expectEqual(@as(u64, 1), registry.entries.items[1].stats.calls);
}

test "LSM owner source allocation failure does not publish an empty label entry" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var registry = LsmOwnerCloneRegistry.init(failing.allocator());
    defer registry.deinit();

    try std.testing.expectError(
        error.OutOfMemory,
        registry.observe(1, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 1 }),
    );
    try std.testing.expectEqual(@as(usize, 0), registry.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), registry.sources.items.len);

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    try registry.observe(1, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 1 });
    try std.testing.expectEqual(@as(usize, 1), registry.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), registry.sources.items.len);
}

test "LSM owner source admission is transactional across every allocation failure" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var registry = LsmOwnerCloneRegistry.init(alloc);
            defer registry.deinit();
            try registry.observe(1, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 1 });
            try std.testing.expectEqual(@as(usize, 1), registry.entries.items.len);
            try std.testing.expectEqual(@as(usize, 1), registry.sources.items.len);
            try std.testing.expectEqual(@as(usize, 1), registry.source_head_by_id.count());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

test "LSM owner indexed retirement repairs interleaved swap removals" {
    const expectIndexIntegrity = struct {
        fn run(source: *LsmOwnerCloneRegistry) !void {
            for (source.sources.items, 0..) |item, index| {
                try std.testing.expectEqual(index, source.source_by_key.get(item.key).?);
                if (item.previous_for_id) |previous_index| {
                    try std.testing.expectEqual(index, source.sources.items[previous_index].next_for_id.?);
                } else {
                    try std.testing.expectEqual(index, source.source_head_by_id.get(item.key.id).?);
                }
                if (item.next_for_id) |next_index| {
                    try std.testing.expectEqual(index, source.sources.items[next_index].previous_for_id.?);
                }
            }
        }
    }.run;

    var registry = LsmOwnerCloneRegistry.init(std.testing.allocator);
    defer registry.deinit();

    try registry.observe(1, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 1 });
    try registry.observe(2, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 2 });
    try registry.observe(1, "docs", 17, .primary, "primary", false, .{ .calls = 3 });
    try registry.observe(3, "docs", 17, .full_text, "body", false, .{ .calls = 4 });

    registry.retireSource(1);
    try std.testing.expectEqual(@as(usize, 2), registry.sources.items.len);
    try std.testing.expect(registry.source_head_by_id.get(1) == null);
    try std.testing.expect(registry.source_head_by_id.get(2) != null);
    try std.testing.expect(registry.source_head_by_id.get(3) != null);
    try expectIndexIntegrity(&registry);

    // Both surviving exact-key indexes must still target the entries moved by
    // swap removal, and a replacement source must join a new per-ID chain.
    try registry.observe(2, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 5 });
    try registry.observe(3, "docs", 17, .full_text, "body", false, .{ .calls = 6 });
    try registry.observe(4, "docs", 17, .primary, "primary", false, .{ .calls = 7 });
    try expectIndexIntegrity(&registry);

    const snapshot = try registry.snapshotAlloc(std.testing.allocator);
    defer {
        for (snapshot) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(snapshot);
    }
    var dense_calls: ?u64 = null;
    var primary_calls: ?u64 = null;
    var full_text_calls: ?u64 = null;
    for (snapshot) |entry| switch (entry.owner_kind) {
        .dense_vector => dense_calls = entry.stats.calls,
        .primary => primary_calls = entry.stats.calls,
        .full_text => full_text_calls = entry.stats.calls,
    };
    try std.testing.expectEqual(@as(?u64, 6), dense_calls);
    try std.testing.expectEqual(@as(?u64, 10), primary_calls);
    try std.testing.expectEqual(@as(?u64, 6), full_text_calls);
}

test "LSM owner snapshot is allocation-failure safe after capture" {
    var registry = LsmOwnerCloneRegistry.init(std.testing.allocator);
    defer registry.deinit();
    try registry.observe(1, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 1 });
    try registry.observe(1, "docs", 17, .primary, "primary", false, .{ .calls = 2 });

    const Runner = struct {
        fn run(alloc: Allocator, source: *LsmOwnerCloneRegistry) !void {
            const snapshot = try source.snapshotAlloc(alloc);
            defer {
                for (snapshot) |*entry| entry.deinit(alloc);
                alloc.free(snapshot);
            }
            try std.testing.expectEqual(@as(usize, 2), snapshot.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{&registry});
}

test "LSM owner snapshot keeps its prefix boundary during concurrent label growth" {
    var registry = LsmOwnerCloneRegistry.init(std.testing.allocator);
    defer registry.deinit();
    try registry.observe(1, "docs", 17, .dense_vector, "embedding", false, .{ .calls = 1 });

    const GrowingAllocator = struct {
        backing: Allocator,
        registry: *LsmOwnerCloneRegistry,
        growth_injected: bool = false,
        injection_error: ?anyerror = null,
        snapshot_array_allocations: usize = 0,

        fn allocator(self: *@This()) Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = allocate,
                .resize = resize,
                .remap = remap,
                .free = free,
            } };
        }

        fn allocate(
            context: *anyopaque,
            len: usize,
            alignment: std.mem.Alignment,
            return_address: usize,
        ) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(context));
            if (len == @sizeOf(LsmOwnerCloneMetricSnapshot) or
                len == 2 * @sizeOf(LsmOwnerCloneMetricSnapshot))
            {
                self.snapshot_array_allocations += 1;
            }
            if (!self.growth_injected) {
                self.growth_injected = true;
                self.registry.observe(2, "docs", 17, .primary, "primary", false, .{ .calls = 2 }) catch |err| {
                    self.injection_error = err;
                    return null;
                };
            }
            return self.backing.rawAlloc(len, alignment, return_address);
        }

        fn resize(
            context: *anyopaque,
            memory: []u8,
            alignment: std.mem.Alignment,
            new_len: usize,
            return_address: usize,
        ) bool {
            const self: *@This() = @ptrCast(@alignCast(context));
            return self.backing.rawResize(memory, alignment, new_len, return_address);
        }

        fn remap(
            context: *anyopaque,
            memory: []u8,
            alignment: std.mem.Alignment,
            new_len: usize,
            return_address: usize,
        ) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(context));
            return self.backing.rawRemap(memory, alignment, new_len, return_address);
        }

        fn free(
            context: *anyopaque,
            memory: []u8,
            alignment: std.mem.Alignment,
            return_address: usize,
        ) void {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.backing.rawFree(memory, alignment, return_address);
        }
    };

    // The allocator injects a new label after snapshotAlloc releases the
    // sizing mutex and before it reacquires the mutex to capture counters.
    var growing = GrowingAllocator{ .backing = std.testing.allocator, .registry = &registry };
    const alloc = growing.allocator();
    const prefix = try registry.snapshotAlloc(alloc);
    defer {
        for (prefix) |*entry| entry.deinit(alloc);
        alloc.free(prefix);
    }
    try std.testing.expect(growing.growth_injected);
    try std.testing.expectEqual(@as(?anyerror, null), growing.injection_error);
    try std.testing.expectEqual(@as(usize, 1), growing.snapshot_array_allocations);
    try std.testing.expectEqual(@as(usize, 1), prefix.len);
    try std.testing.expectEqual(LsmOwnerKind.dense_vector, prefix[0].owner_kind);
    try std.testing.expectEqual(@as(u64, 1), prefix[0].stats.calls);

    const next_snapshot = try registry.snapshotAlloc(std.testing.allocator);
    defer {
        for (next_snapshot) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(next_snapshot);
    }
    try std.testing.expectEqual(@as(usize, 2), next_snapshot.len);
}

test "backend runtime API lane leases expose and release the interface" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingApiLeases());
    var first = try handle.ptr().acquireApiLane();
    var second = try handle.ptr().acquireApiLane();
    try std.testing.expectEqual(@as(usize, 2), handle.ptr().outstandingApiLeases());
    const active_stats = handle.ptr().laneStats();
    try std.testing.expectEqual(@as(usize, 2), active_stats.api_active_leases);
    try std.testing.expectEqual(@as(usize, 2), active_stats.api_peak_leases);
    try std.testing.expectEqual(@as(u64, 2), active_stats.api_acquisitions_total);
    _ = first.io();
    _ = second.io();

    first.release();
    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingApiLeases());
    // Release is idempotent so cleanup paths may call it defensively.
    first.release();
    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingApiLeases());
    second.release();
    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingApiLeases());
}

test "backend runtime borrows backend-agnostic std.Io lanes" {
    var general_token: u8 = 0;
    var api_token: u8 = 0;
    var control_token: u8 = 0;
    const general = Io{ .userdata = &general_token, .vtable = std.Io.failing.vtable };
    const api = Io{ .userdata = &api_token, .vtable = std.Io.failing.vtable };
    const control = Io{ .userdata = &control_token, .vtable = std.Io.failing.vtable };

    try std.testing.expectError(
        error.BorrowedIoRequiresManualBackend,
        BackendRuntimeHandle.init(std.testing.allocator, .{
            .backend = .io_threaded,
            .borrowed_io = .{ .general = general },
        }),
    );

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{
        .backend = .manual,
        .borrowed_io = .{
            .general = general,
            .api = api,
            .control = control,
        },
    });
    defer handle.deinit();

    try std.testing.expect(handle.ptr().apiIoImpl() == null);
    try std.testing.expect(handle.ptr().raftInboundIoImpl() == null);
    try std.testing.expectEqual(@intFromPtr(&general_token), @intFromPtr(handle.ptr().io().?.userdata.?));
    try std.testing.expectEqual(@intFromPtr(&general_token), @intFromPtr(handle.ptr().raftInboundIo().?.userdata.?));
    try std.testing.expectEqual(@intFromPtr(&general_token), @intFromPtr(handle.ptr().raftOutboundIo().?.userdata.?));
    try std.testing.expectEqual(@intFromPtr(&api_token), @intFromPtr(handle.ptr().apiIo().?.userdata.?));
    try std.testing.expectEqual(@intFromPtr(&general_token), @intFromPtr(handle.ptr().inferenceIo().?.userdata.?));
    try std.testing.expectEqual(@intFromPtr(&control_token), @intFromPtr(handle.ptr().controlIo().?.userdata.?));

    var lease = try handle.ptr().acquireApiLane();
    try std.testing.expectEqual(@intFromPtr(&api_token), @intFromPtr(lease.io().userdata.?));
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingApiLeases());
}

test "backend runtime deinit closes admission and waits for active lane leases" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    const runtime = handle.ptr();
    var lease = try runtime.acquireApiLane();
    var deinitialized = std.atomic.Value(bool).init(false);
    const deinit_thread = try std.Thread.spawn(.{}, struct {
        fn run(h: *BackendRuntimeHandle, done: *std.atomic.Value(bool)) void {
            h.deinit();
            done.store(true, .release);
        }
    }.run, .{ &handle, &deinitialized });

    while (!runtime.api_lane_gate.isClosed()) std.Thread.yield() catch {};
    try std.testing.expectError(error.BackendRuntimeShuttingDown, runtime.acquireApiLane());
    try std.testing.expect(!deinitialized.load(.acquire));
    lease.release();
    deinit_thread.join();
    try std.testing.expect(deinitialized.load(.acquire));
}

test "backend runtime rejects API lane leases after shutdown begins" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();
    handle.ptr().api_lane_gate.close();

    try std.testing.expectError(error.BackendRuntimeShuttingDown, handle.ptr().acquireApiLane());
    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingApiLeases());
    try std.testing.expectEqual(@as(u64, 1), handle.ptr().laneStats().api_rejections_total);
}

test "backend runtime control lane leases are isolated from API leases" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    var api = try handle.ptr().acquireApiLane();
    defer api.release();
    var control = try handle.ptr().acquireControlLane();
    defer control.release();

    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingApiLeases());
    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingControlLeases());
    const stats = handle.ptr().laneStats();
    try std.testing.expectEqual(@as(usize, 1), stats.control_peak_leases);
    try std.testing.expectEqual(@as(u64, 1), stats.control_acquisitions_total);
    _ = api.io();
    _ = control.io();
    try std.testing.expect(handle.ptr().api_io_impl.? != handle.ptr().control_io_impl.?);
}

test "backend runtime inference lane has an isolated bounded executor" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    var inference = try handle.ptr().acquireInferenceLane();
    defer inference.release();
    _ = inference.io();
    try std.testing.expectEqual(@as(usize, 1), handle.ptr().outstandingInferenceLeases());
    const stats = handle.ptr().laneStats();
    try std.testing.expectEqual(@as(usize, 1), stats.inference_peak_leases);
    try std.testing.expectEqual(@as(u64, 1), stats.inference_acquisitions_total);
    try std.testing.expect(handle.ptr().inference_io_impl.? != handle.ptr().api_io_impl.?);
    try std.testing.expectEqual(
        std.Io.Limit.limited(threaded_io_limits.inference),
        handle.ptr().inference_io_impl.?.concurrent_limit,
    );
}

test "backend runtime rejects control lane leases after shutdown begins" {
    if (builtin.os.tag == .freestanding) return;

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();
    handle.ptr().control_lane_gate.close();

    try std.testing.expectError(error.BackendRuntimeShuttingDown, handle.ptr().acquireControlLane());
    try std.testing.expectEqual(@as(usize, 0), handle.ptr().outstandingControlLeases());
    try std.testing.expectEqual(@as(u64, 1), handle.ptr().laneStats().control_rejections_total);
}

test "backend runtime retires closed owner registry state" {
    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .manual });
    defer handle.deinit();

    const shared_owner_count = handle.ptr().owner_registry.states.count();
    for (0..1024) |_| {
        const owner_id = try handle.ptr().allocOwnerId();
        handle.ptr().durable_jobs.closeOwner(owner_id);
    }
    try std.testing.expectEqual(shared_owner_count, handle.ptr().owner_registry.states.count());
}

test "backend runtime durable lane drains threaded jobs by owner" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        value: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        deinits: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.value.fetchAdd(1, .monotonic);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .monotonic);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const first_owner_id = try handle.ptr().allocOwnerId();
    const second_owner_id = try handle.ptr().allocOwnerId();
    var first = Ctx{};
    var second = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = first_owner_id,
        .class = .cleanup,
        .ptr = &first,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = second_owner_id,
        .class = .cleanup,
        .ptr = &second,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    handle.ptr().durable_jobs.drainOwner(first_owner_id);
    try std.testing.expectEqual(@as(u32, 1), first.value.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), first.deinits.load(.monotonic));

    handle.ptr().durable_jobs.drainOwner(second_owner_id);
    try std.testing.expectEqual(@as(u32, 1), second.value.load(.monotonic));
    try std.testing.expectEqual(@as(u32, 1), second.deinits.load(.monotonic));
}

test "backend runtime threaded durable lane rejects jobs after owner close" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        ran: std.atomic.Value(u32) = .init(0),
        deinits: std.atomic.Value(u32) = .init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.ran.fetchAdd(1, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    handle.ptr().durable_jobs.closeOwner(owner_id);

    try std.testing.expectEqual(@as(u32, 1), ctx.ran.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
    try std.testing.expect(!handle.ptr().owner_registry.states.contains(owner_id));
    try std.testing.expectError(error.BackgroundOwnerClosed, handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    }));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
}

test "backend runtime owner close rejects recursive submit from draining job" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        lane: DurableJobLane,
        owner_id: u64,
        started: std.atomic.Value(bool) = .init(false),
        allow_submit: std.atomic.Value(bool) = .init(false),
        submit_rejected: std.atomic.Value(bool) = .init(false),
        run_count: std.atomic.Value(u32) = .init(0),
        deinits: std.atomic.Value(u32) = .init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.run_count.fetchAdd(1, .release);
            ctx.started.store(true, .release);
            while (!ctx.allow_submit.load(.acquire)) {
                std.atomic.spinLoopHint();
            }
            ctx.lane.submit(.{
                .owner_id = ctx.owner_id,
                .class = .maintenance,
                .ptr = ctx,
                .run = run,
                .deinit = deinit,
            }) catch |err| switch (err) {
                error.BackgroundOwnerClosing => {
                    ctx.submit_rejected.store(true, .release);
                    return;
                },
                else => return err,
            };
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{ .lane = handle.ptr().durable_jobs, .owner_id = owner_id };
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });
    while (!ctx.started.load(.acquire)) {
        std.atomic.spinLoopHint();
    }
    try std.testing.expect(handle.ptr().owner_registry.beginClose(owner_id));
    ctx.allow_submit.store(true, .release);
    handle.ptr().durable_jobs.drainOwner(owner_id);
    handle.ptr().owner_registry.waitIdle(owner_id);
    handle.ptr().owner_registry.retireClosed(owner_id);

    try std.testing.expect(ctx.submit_rejected.load(.acquire));
    const run_count = ctx.run_count.load(.acquire);
    try std.testing.expect(run_count >= 1);
    try std.testing.expectEqual(run_count, ctx.deinits.load(.acquire));
}

test "backend runtime durable lane deinits threaded job payload after completion" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        ran: std.atomic.Value(u32) = .init(0),
        deinits: std.atomic.Value(u32) = .init(0),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.ran.fetchAdd(1, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            _ = ctx.deinits.fetchAdd(1, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .maintenance,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    var attempts: usize = 0;
    while (ctx.deinits.load(.acquire) == 0 and attempts < 200) : (attempts += 1) {
        _ = try handle.ptr().durable_jobs.poll(8);
        if (handle.ptr().io()) |io| io.sleep(Io.Duration.fromMilliseconds(2), .awake) catch {};
    }
    handle.ptr().durable_jobs.drainOwner(owner_id);

    try std.testing.expectEqual(@as(u32, 1), ctx.ran.load(.acquire));
    try std.testing.expectEqual(@as(u32, 1), ctx.deinits.load(.acquire));
}

test "backend runtime threaded worker releases payload before reaper joins" {
    if (builtin.os.tag == .freestanding) return;

    const Ctx = struct {
        ran: std.atomic.Value(bool) = .init(false),
        deinit_called: std.atomic.Value(bool) = .init(false),
    };
    const Fns = struct {
        fn run(ptr: *anyopaque) !void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.ran.store(true, .release);
        }

        fn deinit(ptr: *anyopaque) void {
            const ctx: *Ctx = @ptrCast(@alignCast(ptr));
            ctx.deinit_called.store(true, .release);
        }
    };

    var handle = try BackendRuntimeHandle.init(std.testing.allocator, .{ .backend = .io_threaded });
    defer handle.deinit();

    // Prevent both the background reaper and explicit poll from joining the
    // future. The worker must still release the owned payload promptly.
    const jobs = handle.ptr().threaded_jobs.?;
    lockAtomic(&jobs.reap_mutex);
    defer jobs.reap_mutex.unlock();

    const owner_id = try handle.ptr().allocOwnerId();
    var ctx = Ctx{};
    try handle.ptr().durable_jobs.submit(.{
        .owner_id = owner_id,
        .class = .commit_durable,
        .ptr = &ctx,
        .run = Fns.run,
        .deinit = Fns.deinit,
    });

    var attempts: usize = 0;
    while (!ctx.deinit_called.load(.acquire) and attempts < 200) : (attempts += 1) {
        if (handle.ptr().io()) |io| io.sleep(Io.Duration.fromMilliseconds(2), .awake) catch {};
    }
    try std.testing.expect(ctx.ran.load(.acquire));
    try std.testing.expect(ctx.deinit_called.load(.acquire));
}
