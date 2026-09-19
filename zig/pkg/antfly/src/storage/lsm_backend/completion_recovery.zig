// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Native completion startup only. The durable sidecar owns the obligation
//! independently of WAL replay; every failure leaves it on disk. No foreground
//! request or maintenance worker may observe the backend until finish succeeds.
const std = @import("std");
const completion = @import("completion_runtime.zig");
const wal = @import("wal.zig");
const codec = @import("completion_slot.zig");

pub fn restoreBeforeReplay(comptime Backend: type, backend: *Backend) !bool {
    var first_info: ?completion.guard.Info = null;
    for (completion.guard_filenames, 0..) |filename, index| {
        const path = try std.fs.path.join(backend.allocator, &.{ backend.root_dir.?, filename });
        defer backend.allocator.free(path);
        const size = backend.storage.?.fileSize(path) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        if (size == 0 or size > completion.limits.max_encoded_bytes + completion.guard.header_bytes) return error.InvalidCompletionSlot;
        const encoded = try backend.allocator.alloc(u8, @intCast(size));
        defer backend.allocator.free(encoded);
        try backend.storage.?.readFileRangeInto(backend.allocator, path, 0, encoded);
        const decoded = try completion.guard.decode(encoded, backend.next_run_id, backend.runs.count());
        if (decoded.info.index != index or (index != 0 and first_info == null)) return error.InvalidCompletionSlot;
        if (first_info) |first| {
            if (first.legacy or decoded.info.legacy or first.base_run_id != decoded.info.base_run_id or first.initial_runs != decoded.info.initial_runs or !std.mem.eql(u8, &first.cohort_id, &decoded.info.cohort_id)) return error.InvalidCompletionSlot;
        } else first_info = decoded.info;
        var descriptor = try codec.decode(backend.allocator, decoded.descriptor, .{});
        defer descriptor.deinit();
        for (backend.durable_completion_members) |maybe| if (maybe) |member| {
            if (std.mem.eql(u8, &member.descriptor.descriptor.txn_id, &descriptor.descriptor.txn_id)) return error.InvalidCompletionSlot;
        };
        const slot = completion.Slot(Backend).create(backend, decoded.descriptor, true, decoded.info) catch |err| switch (err) {
            error.ResourceBudgetExceeded, error.DescriptorAdmissionExhausted, error.OutOfMemory => return error.CompletionRecoveryCapacityRequired,
            else => return err,
        };
        backend.durable_completion_members[index] = slot;
        if (index == 0) backend.durable_completion = slot;
    }
    return first_info != null;
}

pub fn replay(comptime Backend: type, backend: *Backend) !wal.ReplayStats {
    const slot = backend.durable_completion orelse return error.InvalidCompletionSlot;
    const alloc = slot.scratch.allocator();
    var lock = try backend.acquireWalOperationLock(.exclusive);
    defer lock.release();
    // New slots start from segment one and admit at most 128 ordinary
    // records plus one completion. Check before any segment enumeration.
    if (try wal.currentSegment(slot.io.storage(), alloc, backend.root_dir.?) > completion.recovery_wal_records + 2)
        return error.CompletionRecoveryCapacityRequired;
    const retention = try wal.snapshotRetention(slot.io.storage(), alloc, backend.root_dir.?);
    if (retention.bytes > completion.recovery_wal_bytes + completion.limits.max_encoded_bytes + 4096 or retention.segments > completion.recovery_wal_records + 2)
        return error.CompletionRecoveryCapacityRequired;
    // The slab contains all parser and retained-entry allocations. There is no
    // ordinary mutable admission, implicit flush, or separate FD acquisition.
    // WAL allocates a pending parser buffer per segment. Recycle that
    // temporary space rather than spending a fresh bump allocation for every
    // tiny segment; retained nodes use the separately owned scratch domain.
    const parser_buffer = try alloc.alloc(u8, 4 * 1024 * 1024);
    defer alloc.free(parser_buffer);
    var parser = std.heap.FixedBufferAllocator.init(parser_buffer);
    const Hooks = struct {
        slot: *completion.Slot(Backend),
        entries: u64 = 0,
        records: u64 = 0,
        fn entryAllocator(raw: *anyopaque, _: std.mem.Allocator) !std.mem.Allocator {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.slot.scratch.allocator();
        }
        fn entry(raw: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.entries += 1;
            if (self.entries > completion.foreground_entries + completion.limits.max_operations + 2)
                return error.CompletionRecoveryCapacityRequired;
        }
        fn record(raw: *anyopaque, _: u64, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.records += 1;
            if (self.records > completion.recovery_wal_records + 1)
                return error.CompletionRecoveryCapacityRequired;
        }
    };
    var hooks = Hooks{ .slot = slot };
    const stats = try wal.replayIntoMutableWithHooks(slot.io.storage(), parser.allocator(), backend.root_dir.?, &backend.mutable, .{
        .ctx = &hooks,
        .entry_allocator = Hooks.entryAllocator,
        .on_applied_entry = Hooks.entry,
        .on_applied_record = Hooks.record,
    });
    if (stats.entries > completion.foreground_entries + completion.limits.max_operations + 2 or stats.records > completion.recovery_wal_records + 1)
        return error.CompletionRecoveryCapacityRequired;
    slot.replayed_wal_bytes = retention.bytes;
    slot.replayed_wal_entries = stats.entries;
    slot.replayed_wal_records = stats.records;
    backend.wal_retention.primary = retention;
    backend.wal_retention.primary_ns = backend.writeStatsNowNs();
    slot.wal_pin.manager.observeUsage(.lsm_wal_retention, &backend.tracked_wal_retention_bytes, retention.bytes);
    backend.write_stats.wal_replay_records += stats.records;
    backend.write_stats.wal_replay_entries += stats.entries;
    backend.write_stats.wal_replay_bytes += stats.bytes;
    backend.write_stats.wal_replay_truncated_tail_bytes += stats.truncated_tail_bytes;
    backend.mutable_wal_range = if (retention.segments == 0 or backend.mutable.entryCount() == 0) .{} else .{
        .first = retention.oldest_retained_segment,
        .last = retention.current_segment,
    };
    backend.syncTrackedInMemoryStateUsageCurrentLocked();
    return stats;
}

pub fn finish(comptime Backend: type, backend: *Backend, stats: wal.ReplayStats) !void {
    const first = backend.durable_completion orelse return error.InvalidCompletionSlot;
    if (stats.truncated_tail_bytes != 0) try repairTail(Backend, backend, stats);
    var needs_drain: ?*completion.Slot(Backend) = null;
    var all_manifested_completed = true;
    for (backend.durable_completion_members) |maybe| if (maybe) |slot| {
        const descriptor = slot.currentMetadata(backend, slot.storageKey(), if (slot.baseline_slot_present) slot.encoded else null);
        const marker = slot.currentMetadata(backend, slot.appliedKey(), if (slot.baseline_applied) |*value| value else null);
        const applied = if (marker) |value| std.mem.eql(u8, value, &slot.descriptor.descriptor.txn_id) else false;
        if (descriptor) |encoded| {
            all_manifested_completed = false;
            if (!std.mem.eql(u8, encoded, slot.encoded) or applied) return error.InvalidCompletionSlot;
            slot.durable = true;
        } else {
            slot.retired = true;
            if (applied) {
                const manifested = !slot.baseline_slot_present and if (slot.baseline_applied) |value| std.mem.eql(u8, &value, &slot.descriptor.descriptor.txn_id) else false;
                if (!manifested) {
                    all_manifested_completed = false;
                    if (needs_drain != null) return error.InvalidCompletionSlot;
                    needs_drain = slot;
                }
            } else all_manifested_completed = false;
        }
    };
    if (needs_drain) |slot| {
        // One uncertain completion fences the entire backend. Its atomic WAL
        // result and all sibling prepares are drained together, without rebinding.
        try slot.finishReplayed(backend);
    } else if (all_manifested_completed) {
        // Every retained guard has a matching manifest outcome. The last
        // completion covers the full WAL and fenced further foreground work.
        try first.finishManifested(backend);
    } else if (try wal.hasProtectedResetCut(first.io.storage(), first.scratch.allocator(), backend.root_dir.?)) {
        // An exclusion cut itself proves a full durable manifest checkpoint.
        // Finish it before admitting new WAL records, preserving sibling guards.
        try first.finishCheckpointCut(backend);
    }
    // A manifested sibling alone does not prove later WAL belongs to that
    // manifest. Preserve replayed ordinary/sibling writes; never blindly drop it.
    try backend.retireDurableCompletionCohort();
}

fn repairTail(comptime Backend: type, backend: *Backend, stats: wal.ReplayStats) !void {
    const slot = backend.durable_completion orelse return error.InvalidCompletionSlot;
    const alloc = slot.scratch.allocator();
    var wal_lock = try backend.acquireWalOperationLock(.exclusive);
    defer wal_lock.release();
    const segment = try wal.currentSegment(slot.io.storage(), alloc, backend.root_dir.?);
    if (stats.multiple_truncated_segments or stats.truncated_tail_segment == null or stats.truncated_tail_segment.? != segment)
        return error.CorruptLsmWal;
    const path = try std.fmt.allocPrint(alloc, "{s}/wal/{d:0>20}.log", .{ backend.root_dir.?, segment });
    defer alloc.free(path);
    const bytes = try slot.io.storage().fileSize(path);
    if (stats.truncated_tail_bytes > bytes) return error.CorruptLsmWal;
    try slot.io.truncateWalTail(segment, bytes - stats.truncated_tail_bytes);
    const retention = try wal.snapshotRetention(slot.io.storage(), alloc, backend.root_dir.?);
    backend.wal_retention.primary = retention;
    backend.wal_retention.primary_ns = backend.writeStatsNowNs();
    slot.replayed_wal_bytes = retention.bytes;
    slot.wal_pin.manager.observeUsage(.lsm_wal_retention, &backend.tracked_wal_retention_bytes, retention.bytes);
}

pub fn rejectUnanchored(comptime Backend: type, backend: *Backend) !void {
    for ([_]?[]const u8{ null, "docs" }) |name| for (completion.storage_keys) |key| {
        if (backend.getMergedWithMutable(&backend.mutable, .{ .name = name }, key)) |_| {
            return error.InvalidCompletionSlot;
        } else |err| if (err != error.NotFound) return err;
    };
}

const GuardFixture = enum { clean, corrupt, torn_prepare };

fn exerciseGuardRecovery(fault: GuardFixture) !void {
    const Backend = @import("../lsm_backend.zig").Backend;
    const repository = @import("repository.zig");
    const storage_io = @import("storage_io.zig");
    const resources = @import("../resource_manager.zig");
    const alloc = std.testing.allocator;
    var pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
    defer pool.deinit();
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    var path_buffer: [256]u8 = undefined;
    const path = repository.tmpPath(&path_buffer, "completion-guard-recovery");
    defer repository.cleanupTmp(path);
    const options: @import("../lsm_backend.zig").Options = .{ .resource_manager = &manager, .native_storage_pool = &pool, .flush_threshold = 10000 };
    const encoded = try codec.encode(alloc, .{
        .txn_id = @splat(91),
        .intent_revision = 1,
        .limits = completion.limits,
        .profile_fence = "native-guard-test",
        .commit = &.{.{ .kind = .put, .key = "unexecuted", .value = "commit" }},
        .abort = &.{.{ .kind = .put, .key = "unexecuted", .value = "abort" }},
    }, .{});
    defer alloc.free(encoded);
    {
        var backend = try Backend.open(alloc, std.mem.span(path), options);
        defer backend.abandonAfterCrash();
        {
            var seed = try backend.beginWrite();
            errdefer seed.abort();
            try seed.put(.{}, "document", "old");
            try seed.commit();
        }
        try backend.reserveDurableCompletion(encoded);
        // An unrelated acknowledged write is part of the valid prefix even
        // though prepare has not yet become an atomic WAL record.
        {
            var ordinary = try backend.beginWrite();
            errdefer ordinary.abort();
            try ordinary.put(.{}, "independent", "survives");
            try ordinary.commit();
        }
    }
    var native = try storage_io.NativeStorage.init(alloc, .threaded);
    defer native.deinit();
    const guard_path = try std.fs.path.join(alloc, &.{ std.mem.span(path), completion.guard_filename });
    defer alloc.free(guard_path);
    if (fault == .corrupt) {
        const damaged = try alloc.dupe(u8, encoded);
        defer alloc.free(damaged);
        damaged[damaged.len - 1] ^= 1;
        try native.storage().writeFileAbsolute(guard_path, damaged);
        try native.storage().syncFileContentsAbsolute(guard_path);
        var rejected: Backend = undefined;
        try std.testing.expectError(error.CompletionSlotChecksumMismatch, rejected.openInto(alloc, std.mem.span(path), options));
        const retained = try native.storage().readFileAlloc(alloc, guard_path, damaged.len + 1);
        defer alloc.free(retained);
        try std.testing.expectEqualSlices(u8, damaged, retained);
        // Repairing the deliberate fixture corruption is explicit test work,
        // not an automatic product recovery operation.
        try native.storage().writeFileAbsolute(guard_path, encoded);
        try native.storage().syncFileContentsAbsolute(guard_path);
    } else if (fault == .torn_prepare) {
        var prepare: @import("state.zig").ActiveMemTable = .{};
        defer prepare.deinit(alloc);
        try prepare.upsert(alloc, .{}, completion.storage_key, encoded, false);
        var append = try wal.PreparedAppend.init(alloc, std.mem.span(path), &prepare, true, .{});
        defer append.deinit();
        const segment = try wal.currentSegment(native.storage(), alloc, std.mem.span(path));
        const wal_path = try std.fmt.allocPrint(alloc, "{s}/wal/{d:0>20}.log", .{ std.mem.span(path), segment });
        defer alloc.free(wal_path);
        try native.storage().appendFileAbsolute(alloc, wal_path, append.record[0 .. append.record.len / 2], true);
    }
    {
        var reopened: Backend = undefined;
        try reopened.openInto(alloc, std.mem.span(path), options);
        defer reopened.close();
        try std.testing.expect(reopened.durable_completion == null);
        try std.testing.expectError(error.NotFound, reopened.getMergedWithMutable(&reopened.mutable, .{}, completion.storage_key));
        try std.testing.expectError(error.NotFound, reopened.getMergedWithMutable(&reopened.mutable, .{}, "unexecuted"));
        try std.testing.expectEqualStrings("old", try reopened.getMergedWithMutable(&reopened.mutable, .{}, "document"));
        try std.testing.expectEqualStrings("survives", try reopened.getMergedWithMutable(&reopened.mutable, .{}, "independent"));
        try std.testing.expectError(error.FileNotFound, native.storage().fileSize(guard_path));
    }
    // A second ordinary open sees the same data and no hidden obligation.
    var final = try Backend.open(alloc, std.mem.span(path), options);
    defer final.close();
    try std.testing.expectEqualStrings("old", try final.getMergedWithMutable(&final.mutable, .{}, "document"));
    try std.testing.expectEqualStrings("survives", try final.getMergedWithMutable(&final.mutable, .{}, "independent"));
    try std.testing.expectError(error.NotFound, final.getMergedWithMutable(&final.mutable, .{}, "unexecuted"));
}

test "workload admission lsm durable slot clean unprepared guard retires without execution" {
    try exerciseGuardRecovery(.clean);
}
test "workload admission lsm durable slot corrupt guard preserves durable evidence" {
    try exerciseGuardRecovery(.corrupt);
}
test "workload admission lsm durable slot torn unprepared record preserves acknowledged prefix" {
    try exerciseGuardRecovery(.torn_prepare);
}

test "workload admission lsm durable slot fences direct mutation and restores the 65 run manifested edge idempotently" {
    const native = @import("../lsm_backend.zig");
    const repository = @import("repository.zig");
    const storage_io = @import("storage_io.zig");
    const resources = @import("../resource_manager.zig");
    const State = @import("state.zig").State;
    const Active = @import("state.zig").ActiveMemTable;
    const alloc = std.testing.allocator;
    var pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
    defer pool.deinit();
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 256 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    var path_buffer: [256]u8 = undefined;
    const path = repository.tmpPath(&path_buffer, "completion-mutation-fence");
    defer repository.cleanupTmp(path);
    const options: native.Options = .{ .resource_manager = &manager, .native_storage_pool = &pool, .direct_bulk_ingest = true, .flush_threshold = 10000, .compact_threshold_runs = 1024, .l0_overlap_compact_threshold_runs = 1024 };
    var backend = try native.Backend.open(alloc, std.mem.span(path), options);
    var backend_live = true;
    defer if (backend_live) backend.close();
    for (0..64) |i| {
        var key_buffer: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buffer, "seed-{d:0>4}", .{i});
        {
            var seed = try backend.beginWrite();
            errdefer seed.abort();
            try seed.put(.{}, key, "seed");
            try seed.commit();
        }
        try backend.checkpointWalAfterDurableBoundary();
    }
    try std.testing.expectEqual(@as(usize, 64), backend.runs.count());
    const id: [16]u8 = @splat(93);
    const record_key = "\x00\x00__txn_records__:".* ++ id;
    var record: [53]u8 = @splat(0);
    std.mem.writeInt(u64, record[33..41], 1, .little);
    record[49] = 1;
    var terminal = record;
    terminal[0] = 1;
    terminal[52] = 1;
    std.mem.writeInt(u64, terminal[9..17], 200, .little);
    const encoded = try codec.encode(alloc, .{
        .txn_id = id,
        .intent_revision = 1,
        .limits = completion.limits,
        .profile_fence = "native-mutation-test",
        .commit = &.{ .{ .kind = .put, .key = "document", .value = "committed" }, .{ .kind = .put, .key = &record_key, .value = &terminal } },
        .abort = &.{},
    }, .{});
    defer alloc.free(encoded);
    try backend.reserveDurableCompletion(encoded);
    {
        var prepare = try backend.beginWrite();
        errdefer prepare.abort();
        try prepare.put(.{}, &record_key, &record);
        try prepare.put(.{}, completion.storage_key, encoded);
        try prepare.commit();
    }
    try backend.confirmDurableCompletion(id);
    const mutable_root = backend.mutable.ordered.root;
    const run_count = backend.runs.count();
    const next_run_id = backend.next_run_id;
    var input: State = .{};
    defer input.deinit(alloc);
    try input.upsert(alloc, .{}, "bypass", "never-published", false);
    var active: Active = .{};
    defer active.deinit(alloc);
    try active.upsert(alloc, .{}, "bypass", "never-published", false);
    try std.testing.expectError(error.PreparedCompletionActive, backend.beginBulkIngestSession());
    try std.testing.expectError(error.PreparedCompletionActive, backend.beginBatchWithOptions(.{ .mode = .bulk_ingest }));
    try std.testing.expectError(error.PreparedCompletionActive, backend.checkpointWalAfterDurableBoundary());
    try std.testing.expectError(error.PreparedCompletionActive, backend.requestValueReclamation());
    {
        const runtime = @import("runtime.zig");
        const locked = runtime.lockBackend(native.Backend, &backend);
        defer runtime.unlockBackend(native.Backend, &backend, locked);
        try std.testing.expectError(error.PreparedCompletionActive, backend.beginBatchMode(.{ .mode = .bulk_ingest }));
        try std.testing.expectError(error.PreparedCompletionActive, backend.ingestSortedState(&input));
        try std.testing.expectError(error.PreparedCompletionActive, backend.ingestOwnedSortedState(&input));
        try std.testing.expectError(error.PreparedCompletionActive, backend.enqueueOwnedSortedStateForFlush(&input));
        try std.testing.expectError(error.PreparedCompletionActive, backend.directIngestCombinedMutable(&active));
        try std.testing.expectError(error.PreparedCompletionActive, backend.drainMutableBeforeBulkAppendDirectIngest());
    }
    {
        const runtime = @import("runtime.zig");
        const locked = runtime.lockBackend(native.Backend, &backend);
        defer runtime.unlockBackend(native.Backend, &backend, locked);
        const wal_bytes = backend.write_stats.wal_append_bytes;
        const retention_before = try wal.snapshotRetention(backend.storage.?, alloc, std.mem.span(path));
        var reserved_state: State = .{};
        defer reserved_state.deinit(alloc);
        try reserved_state.upsert(alloc, .{}, completion.applied_key, &id, false);
        try std.testing.expectError(error.PreparedCompletionActive, backend.appendWalForState(&reserved_state));
        var reserved_active: Active = .{};
        defer reserved_active.deinit(alloc);
        try reserved_active.upsert(alloc, .{}, completion.storage_key, "overwrite", false);
        try std.testing.expectError(error.PreparedCompletionActive, backend.appendWalForMutable(&reserved_active));
        try std.testing.expectError(error.PreparedCompletionActive, backend.prepareAndAppendWalForMutable(&reserved_active));
        const oversized = try alloc.alloc(u8, completion.foreground_bytes);
        defer alloc.free(oversized);
        @memset(oversized, 0x61);
        var oversized_state: State = .{};
        defer oversized_state.deinit(alloc);
        try oversized_state.upsert(alloc, .{}, "oversized-raw", oversized, false);
        try std.testing.expectError(error.CompletionForegroundCapacityExceeded, backend.appendWalForState(&oversized_state));
        try std.testing.expectEqual(wal_bytes, backend.write_stats.wal_append_bytes);
        const retention_after = try wal.snapshotRetention(backend.storage.?, alloc, std.mem.span(path));
        try std.testing.expectEqualDeep(retention_before, retention_after);
    }
    try std.testing.expectEqual(mutable_root, backend.mutable.ordered.root);
    try std.testing.expectEqual(run_count, backend.runs.count());
    try std.testing.expectEqual(next_run_id, backend.next_run_id);
    try std.testing.expectEqual(@as(usize, 0), backend.active_bulk_ingest_batches);
    try std.testing.expectEqual(@as(usize, 1), input.entryCount());
    try std.testing.expectEqual(@as(usize, 1), active.entryCount());
    {
        var ordinary = try backend.beginWrite();
        errdefer ordinary.abort();
        try ordinary.put(.{}, "independent", "survives");
        try ordinary.commit();
    }
    const Hook = struct {
        fn afterManifest() bool {
            return true;
        }
        fn afterCut(boundary: wal.ProtectedResetBoundary) bool {
            return boundary == .excluded_old_segments;
        }
    };
    completion.test_after_manifest = Hook.afterManifest;
    defer completion.test_after_manifest = null;
    try std.testing.expectError(error.RecoveryRequired, backend.completeDurableCompletion(id, true, .{ .commit_timestamp = 200, .replay_sequence = 1, .shared_ledger_count = 0, .shared_ledger_bytes = 0 }));
    completion.test_after_manifest = null;
    backend.abandonAfterCrash();
    backend_live = false;
    // Repeated crashes after the same manifest must not add an SST or exceed
    // the original 64-run admission plus one protected output.
    wal.test_protected_reset_hook = Hook.afterCut;
    defer wal.test_protected_reset_hook = null;
    for (0..3) |_| {
        try std.testing.expectError(error.InjectedProtectedResetCrash, backend.openInto(alloc, std.mem.span(path), options));
    }
    wal.test_protected_reset_hook = null;
    try backend.openInto(alloc, std.mem.span(path), options);
    backend_live = true;
    try std.testing.expectEqual(@as(usize, 65), backend.runs.count());
    try std.testing.expect(backend.durable_completion == null);
    try std.testing.expectEqualStrings("committed", try backend.getMergedWithMutable(&backend.mutable, .{}, "document"));
    try std.testing.expectEqualStrings("survives", try backend.getMergedWithMutable(&backend.mutable, .{}, "independent"));
    try std.testing.expectError(error.NotFound, backend.getMergedWithMutable(&backend.mutable, .{}, "bypass"));
}

test "workload admission lsm durable slot needs ordinary FD headroom beyond its retained pair" {
    const native = @import("../lsm_backend.zig");
    const repository = @import("repository.zig");
    const storage_io = @import("storage_io.zig");
    const resources = @import("../resource_manager.zig");
    const alloc = std.testing.allocator;
    for ([_]usize{ 2, 3, 4 }) |usable_capacity| {
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
        defer pool.deinit();
        var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 * 1024 * 1024 } });
        defer manager.deinit(alloc);
        var path_buffer: [256]u8 = undefined;
        const path = repository.tmpPath(&path_buffer, "completion-fd-headroom");
        defer repository.cleanupTmp(path);
        var backend = try native.Backend.open(alloc, std.mem.span(path), .{ .resource_manager = &manager, .native_storage_pool = &pool });
        defer backend.close();
        // Root locks must open before reducing the shared pool to the tested
        // transient envelope. Preserve the native persistent/open reserves.
        const persistent = pool.snapshotStats().fd_persistent_descriptors;
        const held_headroom = @max(pool.fd_cache.persistent_reserve -| persistent, pool.fd_cache.persistent_open_headroom);
        pool.fd_cache.capacity = persistent + held_headroom + usable_capacity;
        try std.testing.expectEqual(usable_capacity, backend.storage_owner.?.usableTransientDescriptorCapacity());
        const id: [16]u8 = @splat(96);
        const record_key = "\x00\x00__txn_records__:".* ++ id;
        var record: [53]u8 = @splat(0);
        std.mem.writeInt(u64, record[33..41], 1, .little);
        record[49] = 1;
        const encoded = try codec.encode(alloc, .{
            .txn_id = id,
            .intent_revision = 1,
            .limits = completion.limits,
            .profile_fence = "native-fd-test",
            .commit = &.{.{ .kind = .put, .key = "document", .value = "done" }},
            .abort = &.{},
        }, .{});
        defer alloc.free(encoded);
        if (usable_capacity < 4) {
            const before = backend.write_stats;
            try std.testing.expectError(error.DescriptorAdmissionExhausted, backend.reserveDurableCompletion(encoded));
            try std.testing.expect(backend.durable_completion == null);
            try std.testing.expectEqualDeep(before, backend.write_stats);
            const guard = try std.fs.path.join(alloc, &.{ std.mem.span(path), completion.guard_filename });
            defer alloc.free(guard);
            try std.testing.expectError(error.FileNotFound, backend.storage.?.fileSize(guard));
            continue;
        }
        try backend.reserveDurableCompletion(encoded);
        {
            var prepare = try backend.beginWrite();
            errdefer prepare.abort();
            try prepare.put(.{}, &record_key, &record);
            try prepare.put(.{}, completion.storage_key, encoded);
            try prepare.commit();
        }
        try backend.confirmDurableCompletion(id);
        try backend.completeDurableCompletion(id, true, .{ .commit_timestamp = 200, .replay_sequence = 1, .shared_ledger_count = 0, .shared_ledger_bytes = 0 });
        try std.testing.expectEqualStrings("done", try backend.getMergedWithMutable(&backend.mutable, .{}, "document"));
    }
}

test "workload admission lsm durable cohort preserves siblings across reverse completion and restart" {
    for (0..3) |boundary| try exerciseCohortRecovery(boundary);
}

fn exerciseCohortRecovery(boundary: usize) !void {
    const native = @import("../lsm_backend.zig");
    const repository = @import("repository.zig");
    const storage_io = @import("storage_io.zig");
    const resources = @import("../resource_manager.zig");
    const alloc = std.testing.allocator;
    var pool = storage_io.NativeStoragePool.initWithCapacityForTest(alloc, 32);
    defer pool.deinit();
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 512 * 1024 * 1024 } });
    defer manager.deinit(alloc);
    var path_buffer: [256]u8 = undefined;
    const path = repository.tmpPath(&path_buffer, "completion-cohort");
    defer repository.cleanupTmp(path);
    const options: native.Options = .{ .resource_manager = &manager, .native_storage_pool = &pool, .flush_threshold = 10000, .compact_threshold_runs = 1024, .l0_overlap_compact_threshold_runs = 1024 };
    var ids: [4][16]u8 = undefined;
    var wires: [4]?[]u8 = @splat(null);
    defer for (wires) |wire| {
        if (wire) |owned| alloc.free(owned);
    };
    var backend: native.Backend = undefined;
    try native.Backend.openInto(&backend, alloc, std.mem.span(path), options);
    var live = true;
    defer if (live) backend.close();
    for (0..64) |i| {
        var key_buffer: [32]u8 = undefined;
        const seed_key = try std.fmt.bufPrint(&key_buffer, "seed-{d:0>4}", .{i});
        {
            var seed = try backend.beginWrite();
            errdefer seed.abort();
            try seed.put(.{}, seed_key, "seed");
            try seed.commit();
        }
        try backend.checkpointWalAfterDurableBoundary();
    }
    const summary_key = "\x00\x00__metadata__:txn_completion_v1";
    const summary_bindings = [_]codec.Binding{
        .{ .kind = .shared_ledger_count, .target = .value, .byte_order = .little, .offset = 0 },
        .{ .kind = .shared_ledger_bytes, .target = .value, .byte_order = .little, .offset = 8 },
    };
    const zero_summary: [16]u8 = @splat(0);
    for (0..4) |i| {
        ids[i] = @splat(@as(u8, @intCast(110 + i)));
        const record_key = "\x00\x00__txn_records__:".* ++ ids[i];
        const credit_key = "\x00\x00__txn_completion_v1__:".* ++ ids[i];
        var credit: [16]u8 = @splat(0);
        std.mem.writeInt(u64, credit[0..8], 16, .little);
        std.mem.writeInt(u64, credit[8..16], 32, .little);
        var summary: [16]u8 = @splat(0);
        std.mem.writeInt(u64, summary[0..8], i + 1, .little);
        std.mem.writeInt(u64, summary[8..16], (i + 1) * 48, .little);
        var record: [53]u8 = @splat(0);
        std.mem.writeInt(u64, record[33..41], 1, .little);
        record[49] = 1;
        var committed = record;
        committed[0] = 1;
        committed[52] = 1;
        std.mem.writeInt(u64, committed[9..17], 200, .little);
        var aborted = record;
        aborted[0] = 2;
        aborted[52] = 1;
        std.mem.writeInt(u64, aborted[25..33], 200, .little);
        const key = [_]u8{ 'd', @as(u8, @intCast('0' + i)) };
        wires[i] = try codec.encode(alloc, .{
            .txn_id = ids[i],
            .intent_revision = 1,
            .limits = completion.limits,
            .profile_fence = "cohort-native",
            .commit = &.{ .{ .kind = .put, .key = &key, .value = "committed" }, .{ .kind = .put, .key = &record_key, .value = &committed }, .{ .kind = .delete, .key = &credit_key }, .{ .kind = .put, .key = summary_key, .value = &zero_summary, .bindings = &summary_bindings } },
            .abort = &.{ .{ .kind = .put, .key = &record_key, .value = &aborted }, .{ .kind = .delete, .key = &credit_key }, .{ .kind = .put, .key = summary_key, .value = &zero_summary, .bindings = &summary_bindings } },
        }, .{});
        try backend.reserveDurableCompletion(wires[i].?);
        const slot = backend.findDurableCompletion(ids[i]).?;
        {
            var prepare = try backend.beginWrite();
            errdefer prepare.abort();
            try prepare.put(.{}, &record_key, &record);
            try prepare.put(.{}, &credit_key, &credit);
            try prepare.put(.{}, summary_key, &summary);
            try prepare.put(.{}, slot.storageKey(), wires[i].?);
            try prepare.commit();
        }
        try backend.confirmDurableCompletion(ids[i]);
        if (i == 0) {
            const conflict = try codec.encode(alloc, .{ .txn_id = @splat(200), .intent_revision = 1, .limits = completion.limits, .profile_fence = "conflicting-static-key", .commit = &.{.{ .kind = .put, .key = &key, .value = "conflict" }}, .abort = &.{} }, .{});
            defer alloc.free(conflict);
            try std.testing.expectError(error.PreparedCompletionActive, backend.reserveDurableCompletion(conflict));
        }
    }
    try std.testing.expectEqual(@as(usize, 4), blk: {
        var count: usize = 0;
        for (backend.durableCompletionSlots()) |slot| if (slot != null) {
            count += 1;
        };
        break :blk count;
    });
    {
        const extra = try codec.encode(alloc, .{ .txn_id = @splat(201), .intent_revision = 1, .limits = completion.limits, .profile_fence = "fifth", .commit = &.{.{ .kind = .put, .key = "fifth", .value = "never" }}, .abort = &.{} }, .{});
        defer alloc.free(extra);
        try std.testing.expectError(error.CompletionReservationBusy, backend.reserveDurableCompletion(extra));
        var conflicting: @import("state.zig").State = .{};
        defer conflicting.deinit(alloc);
        try conflicting.upsert(alloc, .{}, "d0", "corrupt", false);
        const runtime = @import("runtime.zig");
        const locked = runtime.lockBackend(native.Backend, &backend);
        defer runtime.unlockBackend(native.Backend, &backend, locked);
        const records_before = backend.write_stats.wal_append_records;
        try std.testing.expectError(error.PreparedCompletionActive, backend.appendWalForState(&conflicting));
        try std.testing.expectEqual(records_before, backend.write_stats.wal_append_records);
    }
    const values: native.Backend.DurableCompletionValues = .{ .commit_timestamp = 200, .replay_sequence = 1, .shared_ledger_count = 0, .shared_ledger_bytes = 0 };
    {
        var old = try backend.beginRead();
        defer old.abort();
        try backend.completeDurableCompletion(ids[2], true, values);
        {
            var ordinary = try backend.beginWrite();
            errdefer ordinary.abort();
            try ordinary.put(.{}, "independent", "preserved");
            try ordinary.commit();
        }
        const Hook = struct {
            fn stop() bool {
                return true;
            }
        };
        if (boundary == 1) completion.test_after_wal = Hook.stop;
        if (boundary == 2) completion.test_after_manifest = Hook.stop;
        defer {
            completion.test_after_wal = null;
            completion.test_after_manifest = null;
        }
        if (boundary == 0) try backend.completeDurableCompletion(ids[0], false, values) else try std.testing.expectError(error.RecoveryRequired, backend.completeDurableCompletion(ids[0], false, values));
        completion.test_after_wal = null;
        completion.test_after_manifest = null;
        try std.testing.expectError(error.NotFound, old.get(.{}, "d2"));
        const old_summary = try old.get(.{}, summary_key);
        try std.testing.expectEqual(@as(u64, 4), std.mem.readInt(u64, old_summary[0..8], .little));
    }
    backend.abandonAfterCrash();
    live = false;
    try native.Backend.openInto(&backend, alloc, std.mem.span(path), options);
    live = true;
    try std.testing.expect(backend.findDurableCompletion(ids[0]) == null);
    try std.testing.expect(backend.findDurableCompletion(ids[2]) == null);
    try std.testing.expect(backend.findDurableCompletion(ids[1]) != null);
    try std.testing.expect(backend.findDurableCompletion(ids[3]) != null);
    const current_summary = try backend.getMergedWithMutable(&backend.mutable, .{}, summary_key);
    try std.testing.expectEqual(@as(u64, 2), std.mem.readInt(u64, current_summary[0..8], .little));
    try std.testing.expect((try backend.durableCompletionDecision(ids[1])).status == .pending);
    try std.testing.expectEqualStrings("committed", try backend.getMergedWithMutable(&backend.mutable, .{}, "d2"));
    try std.testing.expectEqualStrings("preserved", try backend.getMergedWithMutable(&backend.mutable, .{}, "independent"));
    manager.memory.budget.hard_limit_bytes = 1;
    pool.fd_cache.capacity = 1;
    try backend.completeDurableCompletion(ids[3], false, values);
    try backend.completeDurableCompletion(ids[1], true, values);
    manager.memory.budget.hard_limit_bytes = 512 * 1024 * 1024;
    pool.fd_cache.capacity = 32;
    try std.testing.expectEqual(@as(usize, 68), backend.runs.count());
    const final_summary = try backend.getMergedWithMutable(&backend.mutable, .{}, summary_key);
    try std.testing.expectEqual(@as(u64, 0), std.mem.readInt(u64, final_summary[0..8], .little));
    try std.testing.expectEqual(@as(u64, 0), std.mem.readInt(u64, final_summary[8..16], .little));
    try std.testing.expect(!backend.hasDurableCompletions());
    try std.testing.expect(backend.durable_completion == null);
    try std.testing.expectEqualStrings("committed", try backend.getMergedWithMutable(&backend.mutable, .{}, "d1"));
    try std.testing.expectError(error.NotFound, backend.getMergedWithMutable(&backend.mutable, .{}, "d0"));
    try std.testing.expectError(error.NotFound, backend.getMergedWithMutable(&backend.mutable, .{}, "d3"));
}
