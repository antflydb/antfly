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
const builtin = @import("builtin");
const bloom = @import("bloom");
const Allocator = std.mem.Allocator;
const backend_adapter = @import("../backend_adapter.zig");
const backend_erased = @import("../backend_erased.zig");
const backend_types = @import("../backend_types.zig");
const lsm_table_file = @import("../lsm/table_file.zig");
const cache_mod = @import("cache.zig");
const repository_mod = @import("repository.zig");
const state_mod = @import("state.zig");
const platform_time = @import("../../platform/time.zig");

const Run = repository_mod.Run;
const State = state_mod.State;
const ActiveMemTable = state_mod.ActiveMemTable;
const namespaceOf = state_mod.namespaceOf;
const compareNamespace = state_mod.compareNamespace;
const compareEntryTo = state_mod.compareEntryTo;

fn releaseHeldBlocks(held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle), allocator: Allocator) void {
    for (held_blocks.items) |*handle| handle.release();
    held_blocks.deinit(allocator);
}

fn releaseHeldValues(held_values: *std.ArrayListUnmanaged([]u8), allocator: Allocator) void {
    for (held_values.items) |value| allocator.free(value);
    held_values.deinit(allocator);
}

fn compareTableEntryTo(entry: lsm_table_file.Entry, namespace: backend_types.Namespace, key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(.{ .name = entry.namespace_name }, namespace);
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, entry.key, key);
}

fn blockBeforeScanLower(block: lsm_table_file.TableIndex.BlockMeta, namespace: backend_types.Namespace, lower: []const u8) bool {
    return switch (compareNamespace(.{ .name = block.largest_namespace_name }, namespace)) {
        .lt => true,
        .gt => false,
        .eq => std.mem.order(u8, block.largest_key, lower) == .lt,
    };
}

fn invalidateSnapshot(snapshot: *?State, allocator: Allocator) void {
    if (snapshot.*) |*state| {
        state.deinit(allocator);
        snapshot.* = null;
    }
}

fn runtimeScratchAllocator(fallback: Allocator) Allocator {
    if (comptime builtin.os.tag == .freestanding) return fallback;
    if (comptime builtin.link_libc) return std.heap.c_allocator;
    if (comptime builtin.single_threaded) return fallback;
    return std.heap.smp_allocator;
}

fn localBlockCacheEnabled(backend: anytype) bool {
    if (@hasDecl(@TypeOf(backend.*), "localBlockCacheEnabled")) {
        return backend.localBlockCacheEnabled();
    }
    return true;
}

fn elapsedNs(start_ns: u64) u64 {
    const end_ns = platform_time.monotonicNs();
    return if (end_ns >= start_ns) end_ns - start_ns else 0;
}

pub fn lockBackend(comptime BackendType: type, backend: *BackendType) bool {
    if (builtin.os.tag == .freestanding) return false;
    if (@hasField(BackendType, "mu")) {
        if (backend.mu.tryLock()) return true;
        const started_ns = if (@hasDecl(BackendType, "recordBackendLockWait"))
            platform_time.monotonicNs()
        else
            0;
        while (!backend.mu.tryLock()) std.atomic.spinLoopHint();
        if (@hasDecl(BackendType, "recordBackendLockWait")) {
            backend.recordBackendLockWait(platform_time.monotonicNs() -| started_ns);
        }
        return true;
    }
    return false;
}

pub fn unlockBackend(comptime BackendType: type, backend: *BackendType, locked: bool) void {
    if (locked) {
        backend.mu.unlock();
    }
}

const RunGroup = struct {
    smallest_namespace_name: ?[]const u8,
    smallest_key: []const u8,
    largest_namespace_name: ?[]const u8,
    largest_key: []const u8,
    run_indices: []usize,

    fn deinit(self: *RunGroup, allocator: Allocator) void {
        allocator.free(self.run_indices);
        self.* = undefined;
    }
};

const RunLevel = struct {
    level: u32,
    start_index: usize,
    len: usize,
};

const BorrowedReadHint = struct {
    run_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
    entry_index: usize,
};

pub fn BoundStore(comptime BackendType: type) type {
    const LocalReadTxn = BoundReadTxn(BackendType);
    const LocalProbeTxn = BoundProbeTxn(BackendType);
    const LocalCurrentScanTxn = BoundCurrentScanTxn(BackendType);
    const LocalWriteTxn = BoundWriteTxn(BackendType);
    return struct {
        backend: *BackendType,
        namespace: backend_types.Namespace,

        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{
                .ordered_ranges = true,
                .reverse_ranges = true,
                .cursors = true,
                .ordered_append_puts = true,
                .native_namespaces = false,
                .write_batches = .atomic,
                .single_writer = true,
                .read_snapshots = .snapshot,
            };
        }

        pub fn beginRead(self: *@This()) !LocalReadTxn {
            return try LocalReadTxn.open(self.backend, self.namespace);
        }

        pub fn beginProbe(self: *@This()) !LocalProbeTxn {
            return try LocalProbeTxn.open(self.backend, self.namespace);
        }

        pub fn beginCurrentScan(self: *@This()) !LocalCurrentScanTxn {
            return try LocalCurrentScanTxn.open(self.backend, self.namespace);
        }

        pub fn beginWrite(self: *@This()) !LocalWriteTxn {
            return try LocalWriteTxn.open(self.backend, self.namespace);
        }

        pub fn beginBatch(self: *@This()) !LocalWriteTxn {
            return try LocalWriteTxn.open(self.backend, self.namespace);
        }

        pub fn beginBatchWithOptions(self: *@This(), options: backend_types.BatchOptions) !LocalWriteTxn {
            return try LocalWriteTxn.openWithOptions(self.backend, self.namespace, options);
        }

        pub fn sync(self: *@This(), force: bool) !void {
            if (@hasDecl(BackendType, "sync")) {
                try self.backend.sync(force);
            }
        }

        pub fn syncReplayState(self: *@This()) !void {
            if (@hasDecl(BackendType, "syncReplayState")) {
                try self.backend.syncReplayState();
            } else {
                try self.sync(false);
            }
        }

        pub fn beginBulkIngestSession(self: *@This()) !void {
            if (@hasDecl(BackendType, "beginBulkIngestSession")) {
                try self.backend.beginBulkIngestSession();
            }
        }

        pub fn finishBulkIngestSessionWithOptions(self: *@This(), options: backend_types.BulkIngestFinishOptions) !void {
            if (@hasDecl(BackendType, "finishBulkIngestSessionWithOptions")) {
                try self.backend.finishBulkIngestSessionWithOptions(options);
            } else if (@hasDecl(BackendType, "finishBulkIngestSession")) {
                try self.backend.finishBulkIngestSession();
            }
        }

        pub fn abortBulkIngestSession(self: *@This()) void {
            if (@hasDecl(BackendType, "abortBulkIngestSession")) {
                self.backend.abortBulkIngestSession();
            }
        }
    };
}

pub fn BoundCursor(comptime StateType: type) type {
    return struct {
        state: *const StateType,
        namespace: backend_types.Namespace,
        current: ?usize = null,
        upper_bound: ?[]const u8 = null,

        pub fn close(_: *@This()) void {}

        pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
            self.upper_bound = upper;
        }

        pub fn first(self: *@This()) !?backend_adapter.Entry {
            const idx = self.firstIndex() orelse return null;
            self.current = idx;
            return self.entryIfBeforeUpper(idx);
        }

        pub fn last(self: *@This()) !?backend_adapter.Entry {
            const idx = self.lastIndex() orelse return null;
            self.current = idx;
            return self.state.entries.items[idx].entry();
        }

        pub fn next(self: *@This()) !?backend_adapter.Entry {
            const current = self.current orelse return null;
            var idx = current + 1;
            while (idx < self.state.entries.items.len) : (idx += 1) {
                if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) == .eq) {
                    self.current = idx;
                    return self.entryIfBeforeUpper(idx);
                }
            }
            return null;
        }

        pub fn prev(self: *@This()) !?backend_adapter.Entry {
            const current = self.current orelse return null;
            if (current == 0) return null;
            var idx = current - 1;
            while (true) {
                if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) == .eq) {
                    self.current = idx;
                    return self.state.entries.items[idx].entry();
                }
                if (idx == 0) break;
                idx -= 1;
            }
            return null;
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            const idx = self.state.lowerBound(self.namespace, key);
            if (idx >= self.state.entries.items.len) return null;
            if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) != .eq) return null;
            self.current = idx;
            return self.entryIfBeforeUpper(idx);
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            const idx = self.state.lowerBound(self.namespace, key);
            if (idx < self.state.entries.items.len and compareEntryTo(self.state.entries.items[idx], self.namespace, key) == .eq) {
                self.current = idx;
                return self.state.entries.items[idx].entry();
            }
            if (idx == 0) return null;
            var probe = idx - 1;
            while (true) {
                if (compareNamespace(namespaceOf(self.state.entries.items[probe]), self.namespace) == .eq) {
                    self.current = probe;
                    return self.state.entries.items[probe].entry();
                }
                if (probe == 0) break;
                probe -= 1;
            }
            return null;
        }

        fn firstIndex(self: *const @This()) ?usize {
            const idx = self.state.lowerBound(self.namespace, "");
            if (idx >= self.state.entries.items.len) return null;
            if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) != .eq) return null;
            return idx;
        }

        fn lastIndex(self: *const @This()) ?usize {
            if (self.state.entries.items.len == 0) return null;
            var idx = self.state.entries.items.len;
            while (idx > 0) {
                idx -= 1;
                if (compareNamespace(namespaceOf(self.state.entries.items[idx]), self.namespace) == .eq) return idx;
            }
            return null;
        }

        fn entryIfBeforeUpper(self: *const @This(), idx: usize) ?backend_adapter.Entry {
            const entry = self.state.entries.items[idx].entry();
            if (!self.keyBeforeUpper(entry.key)) return null;
            return entry;
        }

        fn keyBeforeUpper(self: *const @This(), key: []const u8) bool {
            const upper = self.upper_bound orelse return true;
            return std.mem.order(u8, key, upper) == .lt;
        }
    };
}

pub fn MergeCursor(comptime BackendType: type, comptime MutableType: type) type {
    return struct {
        allocator: Allocator,
        backend: *BackendType,
        mutable: *const MutableType,
        immutable_memtables: []const *const State = &.{},
        runs: []Run,
        l0_groups: []const RunGroup,
        levels: []const RunLevel,
        namespace: backend_types.Namespace,
        positions: []?usize,
        source_entries: []?SourceEntry,
        source_block_bytes: []?[]const u8,
        source_block_handles: []?cache_mod.Handle,
        source_block_indices: []?usize,
        advance_sources: []usize,
        source_heap: []usize,
        source_heap_positions: []?usize,
        source_heap_len: usize = 0,
        visible_entry_bytes: ?[]u8 = null,
        mutable_source_entry_bytes: ?[]u8 = null,
        current_key: ?[]const u8 = null,
        upper_bound: ?[]const u8 = null,
        backend_locked: bool = false,

        pub fn close(self: *@This()) void {
            for (0..self.source_block_bytes.len) |source_index| self.clearSourceBlock(source_index);
            self.clearVisibleEntryBytes();
            self.clearMutableSourceEntryBytes();
            self.allocator.free(self.source_block_indices);
            self.allocator.free(self.source_block_handles);
            self.allocator.free(self.source_block_bytes);
            self.allocator.free(self.source_entries);
            self.allocator.free(self.positions);
            self.allocator.free(self.advance_sources);
            self.allocator.free(self.source_heap);
            self.allocator.free(self.source_heap_positions);
        }

        pub fn first(self: *@This()) !?backend_adapter.Entry {
            try self.initForwardPositions("", true);
            const entry = try self.selectVisibleForward();
            self.current_key = if (entry) |e| e.key else null;
            return entry;
        }

        pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
            self.upper_bound = upper;
        }

        pub fn last(self: *@This()) !?backend_adapter.Entry {
            const entry = try self.findLast();
            if (entry) |e| {
                try self.initForwardPositions(e.key, true);
                self.current_key = e.key;
            } else {
                self.current_key = null;
            }
            return entry;
        }

        pub fn next(self: *@This()) !?backend_adapter.Entry {
            if (self.current_key == null) return null;
            try self.advanceForwardSources();
            const entry = try self.selectVisibleForward();
            self.current_key = if (entry) |e| e.key else null;
            return entry;
        }

        pub fn prev(self: *@This()) !?backend_adapter.Entry {
            const key = self.current_key orelse return null;
            const entry = try self.findAtOrBefore(key, false);
            if (entry) |e| {
                try self.initForwardPositions(e.key, true);
                self.current_key = e.key;
            } else {
                self.current_key = null;
            }
            return entry;
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            try self.initForwardPositions(key, true);
            const entry = try self.selectVisibleForward();
            self.current_key = if (entry) |e| e.key else null;
            return entry;
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            const entry = try self.findAtOrBefore(key, true);
            if (entry) |e| {
                try self.initForwardPositions(e.key, true);
                self.current_key = e.key;
            } else {
                self.current_key = null;
            }
            return entry;
        }

        fn initForwardPositions(self: *@This(), target: []const u8, inclusive: bool) !void {
            for (0..self.positions.len) |source_index| {
                try self.setSourceAtOrAfter(source_index, target, inclusive);
            }
            self.rebuildForwardHeap();
        }

        fn selectVisibleForward(self: *@This()) !?backend_adapter.Entry {
            while (true) {
                const winner_source = self.bestVisibleForwardSource() orelse return null;
                const candidate = self.source_entries[winner_source].?.key;
                const entry = self.source_entries[winner_source].?;
                if (!self.keyBeforeUpper(entry.key)) return null;
                if (!entry.tombstone) return .{ .key = entry.key, .value = entry.value };
                try self.advanceForwardSourcesAtKey(candidate);
            }
        }

        fn bestVisibleForwardSource(self: *@This()) ?usize {
            if (self.source_heap_len == 0) return null;
            return self.source_heap[0];
        }

        fn rebuildForwardHeap(self: *@This()) void {
            @memset(self.source_heap_positions, null);
            self.source_heap_len = 0;
            for (0..self.positions.len) |source_index| {
                if (!self.sourceIsForwardActive(source_index)) continue;
                self.heapInsert(source_index);
            }
        }

        fn updateForwardHeapSource(self: *@This(), source_index: usize) void {
            if (!self.sourceIsForwardActive(source_index)) {
                self.heapRemove(source_index);
                return;
            }
            if (self.source_heap_positions[source_index]) |heap_index| {
                self.heapFix(heap_index);
            } else {
                self.heapInsert(source_index);
            }
        }

        fn sourceIsForwardActive(self: *const @This(), source_index: usize) bool {
            return self.positions[source_index] != null and self.source_entries[source_index] != null;
        }

        fn heapInsert(self: *@This(), source_index: usize) void {
            const heap_index = self.source_heap_len;
            self.source_heap_len += 1;
            self.source_heap[heap_index] = source_index;
            self.source_heap_positions[source_index] = heap_index;
            self.heapSiftUp(heap_index);
        }

        fn heapRemove(self: *@This(), source_index: usize) void {
            const heap_index = self.source_heap_positions[source_index] orelse return;
            self.source_heap_positions[source_index] = null;
            self.source_heap_len -= 1;
            if (heap_index == self.source_heap_len) return;
            const moved_source = self.source_heap[self.source_heap_len];
            self.source_heap[heap_index] = moved_source;
            self.source_heap_positions[moved_source] = heap_index;
            self.heapFix(heap_index);
        }

        fn heapFix(self: *@This(), heap_index: usize) void {
            if (heap_index > 0) {
                const parent = (heap_index - 1) / 2;
                if (self.sourceBeats(self.source_heap[heap_index], self.source_heap[parent])) {
                    self.heapSiftUp(heap_index);
                    return;
                }
            }
            self.heapSiftDown(heap_index);
        }

        fn heapSiftUp(self: *@This(), start_index: usize) void {
            var child = start_index;
            while (child > 0) {
                const parent = (child - 1) / 2;
                if (!self.sourceBeats(self.source_heap[child], self.source_heap[parent])) break;
                self.heapSwap(child, parent);
                child = parent;
            }
        }

        fn heapSiftDown(self: *@This(), start_index: usize) void {
            var parent = start_index;
            while (true) {
                const left = parent * 2 + 1;
                if (left >= self.source_heap_len) break;
                const right = left + 1;
                var best = left;
                if (right < self.source_heap_len and self.sourceBeats(self.source_heap[right], self.source_heap[left])) {
                    best = right;
                }
                if (!self.sourceBeats(self.source_heap[best], self.source_heap[parent])) break;
                self.heapSwap(parent, best);
                parent = best;
            }
        }

        fn heapSwap(self: *@This(), lhs: usize, rhs: usize) void {
            const lhs_source = self.source_heap[lhs];
            const rhs_source = self.source_heap[rhs];
            self.source_heap[lhs] = rhs_source;
            self.source_heap[rhs] = lhs_source;
            self.source_heap_positions[lhs_source] = rhs;
            self.source_heap_positions[rhs_source] = lhs;
        }

        fn sourceBeats(self: *const @This(), lhs_source: usize, rhs_source: usize) bool {
            const lhs = self.source_entries[lhs_source].?;
            const rhs = self.source_entries[rhs_source].?;
            return switch (std.mem.order(u8, lhs.key, rhs.key)) {
                .lt => true,
                .eq => lhs_source < rhs_source,
                .gt => false,
            };
        }

        fn advanceForwardSources(self: *@This()) !void {
            const key = self.current_key orelse return;
            try self.advanceForwardSourcesAtKey(key);
        }

        fn advanceForwardSourcesAtKey(self: *@This(), key: []const u8) !void {
            const match_count = self.collectForwardHeapSourcesAtKey(key);
            for (self.advance_sources[0..match_count]) |source_index| {
                const idx = self.positions[source_index] orelse continue;
                try self.advanceSource(source_index, idx);
                self.updateForwardHeapSource(source_index);
            }
        }

        fn collectForwardHeapSourcesAtKey(self: *@This(), key: []const u8) usize {
            if (self.source_heap_len == 0) return 0;
            var match_count: usize = 0;
            var stack_start = self.advance_sources.len;
            stack_start -= 1;
            self.advance_sources[stack_start] = 0;

            while (stack_start < self.advance_sources.len) {
                const heap_index = self.advance_sources[stack_start];
                stack_start += 1;
                if (heap_index >= self.source_heap_len) continue;

                const source_index = self.source_heap[heap_index];
                const entry = self.source_entries[source_index] orelse continue;
                const order = std.mem.order(u8, entry.key, key);
                if (order == .eq) {
                    self.advance_sources[match_count] = source_index;
                    match_count += 1;
                } else if (order == .gt) {
                    continue;
                }

                const left = heap_index * 2 + 1;
                const right = left + 1;
                if (right < self.source_heap_len) {
                    stack_start -= 1;
                    std.debug.assert(stack_start >= match_count);
                    self.advance_sources[stack_start] = right;
                }
                if (left < self.source_heap_len) {
                    stack_start -= 1;
                    std.debug.assert(stack_start >= match_count);
                    self.advance_sources[stack_start] = left;
                }
            }
            return match_count;
        }

        const SourceEntry = struct {
            namespace_name: ?[]const u8,
            key: []const u8,
            value: []const u8,
            tombstone: bool,
        };

        fn runSourceOffset(self: *const @This()) usize {
            return 1 + self.immutable_memtables.len;
        }

        fn immutableForSource(self: *const @This(), source_index: usize) ?*const State {
            if (source_index == 0 or source_index >= self.runSourceOffset()) return null;
            return self.immutable_memtables[source_index - 1];
        }

        fn runForSource(self: *@This(), source_index: usize) !*Run {
            const offset = self.runSourceOffset();
            if (source_index < offset) return error.RunStateUnavailable;
            const run_index = source_index - offset;
            if (run_index >= self.runs.len) return error.RunStateUnavailable;
            return &self.runs[run_index];
        }

        fn sourceEntryAt(self: *@This(), source_index: usize, idx: usize) !SourceEntry {
            if (source_index == 0) {
                if (comptime MutableType == ActiveMemTable) return try self.copyMutableSourceEntryAt(idx);
                const entry = self.mutable.entries.items[idx];
                return .{ .namespace_name = namespaceOf(entry).name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
            }
            if (self.immutableForSource(source_index)) |state| {
                const entry = state.entries.items[idx];
                return .{ .namespace_name = namespaceOf(entry).name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
            }

            const run = try self.runForSource(source_index);
            if (run.state) |*state| {
                const entry = state.entries.items[idx];
                return .{ .namespace_name = namespaceOf(entry).name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
            }

            if (run.path != null) {
                const index = try indexForRunNoCacheMaybeLocked(self.backend, run, self.backend_locked);
                return try self.sourceEntryAtFromLocalIndex(source_index, run, index, idx);
            }

            const table = try tableForRunMaybeLocked(self.backend, run, self.backend_locked);
            const entry = try table.entryAt(idx);
            return .{ .namespace_name = entry.namespace_name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
        }

        fn sourceLowerBound(self: *@This(), source_index: usize, target: []const u8, inclusive: bool) !?usize {
            if (!self.keyBeforeUpper(target)) return null;
            if (source_index == 0) return nextStateIndex(self.mutable, self.namespace, target, inclusive);
            if (self.immutableForSource(source_index)) |state| return nextStateIndex(state, self.namespace, target, inclusive);

            const run = try self.runForSource(source_index);
            if (!runMayContainAtOrAfter(run.*, self.namespace, target)) return null;
            if (run.state) |*state| return nextStateIndex(state, self.namespace, target, inclusive);

            if (run.path != null) {
                const index = try indexForRunNoCacheMaybeLocked(self.backend, run, self.backend_locked);
                return try self.sourceLowerBoundFromLocalIndex(source_index, run, index, target, inclusive);
            }

            const table = try tableForRunMaybeLocked(self.backend, run, self.backend_locked);
            var idx = try table.lowerBound(self.namespace.name, target);
            while (idx < table.entryCount()) : (idx += 1) {
                const entry = try table.entryAt(idx);
                if (compareNamespace(.{ .name = entry.namespace_name }, self.namespace) != .eq) return null;
                if (!inclusive and std.mem.eql(u8, entry.key, target)) continue;
                return idx;
            }
            return null;
        }

        fn setSourceAtOrAfter(self: *@This(), source_index: usize, target: []const u8, inclusive: bool) !void {
            if (source_index == 0 and comptime MutableType == ActiveMemTable) {
                try self.setMutableSourceAtOrAfter(target, inclusive);
                return;
            }
            if (source_index == 0 or self.immutableForSource(source_index) != null) {
                self.positions[source_index] = try self.sourceLowerBound(source_index, target, inclusive);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                return;
            }

            const run = try self.runForSource(source_index);
            if (!runMayContainAtOrAfter(run.*, self.namespace, target)) {
                self.clearSourceBlock(source_index);
                self.positions[source_index] = null;
                self.source_entries[source_index] = null;
                return;
            }
            if (run.state != null) {
                self.positions[source_index] = try self.sourceLowerBound(source_index, target, inclusive);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                return;
            }

            if (run.path != null) {
                self.positions[source_index] = try self.sourceLowerBound(source_index, target, inclusive);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                if (self.positions[source_index] == null) self.clearSourceBlock(source_index);
                return;
            }

            const table = try tableForRunMaybeLocked(self.backend, run, self.backend_locked);
            if (try table.lowerBoundPosition(self.namespace.name, target, inclusive)) |positioned| {
                self.positions[source_index] = positioned.index;
                self.source_entries[source_index] = .{
                    .namespace_name = positioned.entry.namespace_name,
                    .key = positioned.entry.key,
                    .value = positioned.entry.value,
                    .tombstone = positioned.entry.tombstone,
                };
            } else {
                self.positions[source_index] = null;
                self.source_entries[source_index] = null;
            }
        }

        fn advanceSource(self: *@This(), source_index: usize, current: usize) !void {
            if (source_index == 0 and comptime MutableType == ActiveMemTable) {
                try self.advanceMutableSource();
                return;
            }
            if (source_index == 0 or self.immutableForSource(source_index) != null) {
                self.positions[source_index] = try self.nextSourceIndex(source_index, current);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                return;
            }

            const run = try self.runForSource(source_index);
            if (run.state != null) {
                self.positions[source_index] = try self.nextSourceIndex(source_index, current);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                return;
            }

            if (run.path != null) {
                self.positions[source_index] = try self.nextSourceIndex(source_index, current);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                if (self.positions[source_index] == null) self.clearSourceBlock(source_index);
                return;
            }

            const table = try tableForRunMaybeLocked(self.backend, run, self.backend_locked);
            if (try table.nextPositionInNamespace(self.namespace.name, current)) |positioned| {
                self.positions[source_index] = positioned.index;
                self.source_entries[source_index] = .{
                    .namespace_name = positioned.entry.namespace_name,
                    .key = positioned.entry.key,
                    .value = positioned.entry.value,
                    .tombstone = positioned.entry.tombstone,
                };
            } else {
                self.positions[source_index] = null;
                self.source_entries[source_index] = null;
            }
        }

        fn nextSourceIndex(self: *@This(), source_index: usize, current: usize) !?usize {
            if (source_index == 0) return nextIndexFrom(self.mutable, self.namespace, current);
            if (self.immutableForSource(source_index)) |state| return nextIndexFrom(state, self.namespace, current);

            const run = try self.runForSource(source_index);
            if (run.state) |*state| return nextIndexFrom(state, self.namespace, current);

            if (run.path != null) {
                return try self.nextSourceIndexFromLocalIndex(source_index, run, current);
            }

            const table = try tableForRunMaybeLocked(self.backend, run, self.backend_locked);
            var idx = current + 1;
            while (idx < table.entryCount()) : (idx += 1) {
                const entry = try table.entryAt(idx);
                const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                if (order == .eq) return idx;
                if (order == .gt) return null;
            }
            return null;
        }

        fn findAtOrBefore(self: *@This(), target: []const u8, inclusive: bool) !?backend_adapter.Entry {
            var probe = target;
            var owned_probe: ?[]u8 = null;
            defer if (owned_probe) |bytes| self.allocator.free(bytes);
            var include_probe = inclusive;
            while (true) {
                const maybe_candidate = blk: {
                    const stable_probe = try self.allocator.dupe(u8, probe);
                    defer self.allocator.free(stable_probe);
                    break :blk try self.prevCandidateKey(stable_probe, include_probe);
                };
                const candidate = maybe_candidate orelse return null;
                const stable_candidate = try self.allocator.dupe(u8, candidate);
                if (try self.visibleEntryAtKey(stable_candidate)) |entry| {
                    self.allocator.free(stable_candidate);
                    return entry;
                }
                if (owned_probe) |bytes| self.allocator.free(bytes);
                owned_probe = stable_candidate;
                probe = stable_candidate;
                include_probe = false;
            }
        }

        fn findLast(self: *@This()) !?backend_adapter.Entry {
            var best: ?[]const u8 = try self.mutableLastKeyStable();
            for (self.immutable_memtables) |state| {
                const concrete = mutableLastKey(state, self.namespace) orelse continue;
                if (best == null or std.mem.order(u8, concrete, best.?) == .gt) best = concrete;
            }
            for (self.runs, 0..) |*run, run_i| {
                const source_index = self.runSourceOffset() + run_i;
                const candidate = if (run.state) |*state|
                    mutableLastKey(state, self.namespace)
                else if (run.path != null) blk: {
                    break :blk try self.sourceLastKeyFromLocalIndex(source_index, run);
                } else null;
                const concrete = candidate orelse continue;
                if (best == null or std.mem.order(u8, concrete, best.?) == .gt) best = concrete;
            }
            const key = best orelse return null;
            const stable_key = try self.allocator.dupe(u8, key);
            defer self.allocator.free(stable_key);
            return try self.findAtOrBefore(stable_key, true);
        }

        fn prevCandidateKey(self: *@This(), target: []const u8, inclusive: bool) !?[]const u8 {
            var best: ?[]const u8 = try self.mutablePrevStateKeyStable(target, inclusive);
            for (self.immutable_memtables) |state| {
                const concrete = prevStateKey(state, self.namespace, target, inclusive) orelse continue;
                if (best == null or std.mem.order(u8, concrete, best.?) == .gt) best = concrete;
            }
            for (self.runs, 0..) |*run, run_i| {
                const source_index = self.runSourceOffset() + run_i;
                const candidate = if (run.state) |*state|
                    prevStateKey(state, self.namespace, target, inclusive)
                else if (run.path != null) blk: {
                    break :blk try self.sourcePrevKeyFromLocalIndex(source_index, run, target, inclusive);
                } else null;
                const concrete = candidate orelse continue;
                if (best == null or std.mem.order(u8, concrete, best.?) == .gt) best = concrete;
            }
            return best;
        }

        fn visibleEntryAtKey(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            self.clearVisibleEntryBytes();
            if (comptime MutableType == ActiveMemTable) {
                if (try self.visibleMutableEntryAtKey(key)) |entry| return entry;
            } else if (self.mutable.findIndex(self.namespace, key)) |idx| {
                const entry = self.mutable.entries.items[idx];
                if (entry.tombstone) return null;
                return entry.entry();
            }
            for (self.immutable_memtables) |state| {
                if (state.findIndex(self.namespace, key)) |idx| {
                    const entry = state.entries.items[idx];
                    if (entry.tombstone) return null;
                    return entry.entry();
                }
            }
            if (findRunGroup(self.l0_groups, self.namespace, key)) |candidate_group| {
                if (try visibleEntryFromRunIndices(self.backend, self.runs, candidate_group.run_indices, self.namespace, key, &self.visible_entry_bytes, self.backend_locked)) |entry| {
                    return entry;
                }
            }
            for (self.levels) |level| {
                const run_index = findRunIndexInLevel(self.runs, level, self.namespace, key) orelse continue;
                const one = [_]usize{run_index};
                if (try visibleEntryFromRunIndices(self.backend, self.runs, &one, self.namespace, key, &self.visible_entry_bytes, self.backend_locked)) |entry| {
                    return entry;
                }
            }
            return null;
        }

        fn clearVisibleEntryBytes(self: *@This()) void {
            if (self.visible_entry_bytes) |bytes| self.backend.allocator.free(bytes);
            self.visible_entry_bytes = null;
        }

        fn clearMutableSourceEntryBytes(self: *@This()) void {
            if (self.mutable_source_entry_bytes) |bytes| self.allocator.free(bytes);
            self.mutable_source_entry_bytes = null;
        }

        fn mutableSourceLock(self: *@This()) bool {
            if (comptime MutableType == ActiveMemTable) {
                if (!self.backend_locked) return lockBackend(BackendType, self.backend);
            }
            return false;
        }

        fn mutableSourceUnlock(self: *@This(), locked: bool) void {
            if (comptime MutableType == ActiveMemTable) {
                unlockBackend(BackendType, self.backend, locked);
            }
        }

        fn copyMutableSourceEntryAt(self: *@This(), idx: usize) !SourceEntry {
            const entry = self.mutable.entries.items[idx];
            const namespace_name = namespaceOf(entry).name;
            const namespace_len = if (namespace_name) |name| name.len else 0;
            const bytes = try self.allocator.alloc(u8, namespace_len + entry.key.len + entry.value.len);
            errdefer self.allocator.free(bytes);
            var offset: usize = 0;
            const copied_namespace = if (namespace_name) |name| blk: {
                @memcpy(bytes[offset..][0..name.len], name);
                const copied = bytes[offset..][0..name.len];
                offset += name.len;
                break :blk copied;
            } else null;
            @memcpy(bytes[offset..][0..entry.key.len], entry.key);
            const copied_key = bytes[offset..][0..entry.key.len];
            offset += entry.key.len;
            @memcpy(bytes[offset..][0..entry.value.len], entry.value);
            const copied_value = bytes[offset..][0..entry.value.len];

            self.clearMutableSourceEntryBytes();
            self.mutable_source_entry_bytes = bytes;
            return .{
                .namespace_name = copied_namespace,
                .key = copied_key,
                .value = copied_value,
                .tombstone = entry.tombstone,
            };
        }

        fn setMutableSourceAtOrAfter(self: *@This(), target: []const u8, inclusive: bool) !void {
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const idx = nextStateIndex(self.mutable, self.namespace, target, inclusive) orelse {
                self.positions[0] = null;
                self.source_entries[0] = null;
                self.clearMutableSourceEntryBytes();
                return;
            };
            self.positions[0] = idx;
            self.source_entries[0] = try self.copyMutableSourceEntryAt(idx);
        }

        fn advanceMutableSource(self: *@This()) !void {
            const current = self.source_entries[0] orelse {
                self.positions[0] = null;
                self.clearMutableSourceEntryBytes();
                return;
            };
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const idx = nextStateIndex(self.mutable, self.namespace, current.key, false) orelse {
                self.positions[0] = null;
                self.source_entries[0] = null;
                self.clearMutableSourceEntryBytes();
                return;
            };
            self.positions[0] = idx;
            self.source_entries[0] = try self.copyMutableSourceEntryAt(idx);
        }

        fn copyKeyToVisibleBytes(self: *@This(), key: []const u8) ![]const u8 {
            const bytes = try self.backend.allocator.dupe(u8, key);
            errdefer self.backend.allocator.free(bytes);
            self.clearVisibleEntryBytes();
            self.visible_entry_bytes = bytes;
            return bytes;
        }

        fn mutableLastKeyStable(self: *@This()) !?[]const u8 {
            if (comptime MutableType != ActiveMemTable) return mutableLastKey(self.mutable, self.namespace);
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const key = mutableLastKey(self.mutable, self.namespace) orelse return null;
            return try self.copyKeyToVisibleBytes(key);
        }

        fn mutablePrevStateKeyStable(self: *@This(), target: []const u8, inclusive: bool) !?[]const u8 {
            if (comptime MutableType != ActiveMemTable) return prevStateKey(self.mutable, self.namespace, target, inclusive);
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const key = prevStateKey(self.mutable, self.namespace, target, inclusive) orelse return null;
            return try self.copyKeyToVisibleBytes(key);
        }

        fn visibleMutableEntryAtKey(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const idx = self.mutable.findIndex(self.namespace, key) orelse return null;
            const entry = self.mutable.entries.items[idx];
            if (entry.tombstone) return null;
            const bytes = try self.backend.allocator.alloc(u8, entry.key.len + entry.value.len);
            errdefer self.backend.allocator.free(bytes);
            @memcpy(bytes[0..entry.key.len], entry.key);
            @memcpy(bytes[entry.key.len..][0..entry.value.len], entry.value);
            self.visible_entry_bytes = bytes;
            return .{
                .key = bytes[0..entry.key.len],
                .value = bytes[entry.key.len..][0..entry.value.len],
            };
        }

        fn clearSourceBlock(self: *@This(), source_index: usize) void {
            if (self.source_block_handles[source_index]) |*handle| {
                handle.release();
                self.source_block_handles[source_index] = null;
            } else if (self.source_block_bytes[source_index]) |bytes| {
                self.backend.allocator.free(@constCast(bytes));
            }
            self.source_block_bytes[source_index] = null;
            self.source_block_indices[source_index] = null;
        }

        fn sourceEntryAtFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
            index: *const lsm_table_file.TableIndex,
            entry_index: usize,
        ) !SourceEntry {
            const window = if (index.findBlockIndexForEntry(entry_index)) |block_index|
                index.blockWindow(block_index)
            else
                index.entryDataWindow(entry_index, cache_mod.DefaultTableBlockSize);
            const bytes = try self.ensureSourceBlockLoaded(source_index, run, index, window);
            const relative_offset: usize = @intCast(index.entryStart(entry_index) - window.relative_offset);
            const entry = try parseEntryAtWithStats(self.backend, bytes, relative_offset);
            return .{
                .namespace_name = entry.namespace_name,
                .key = entry.key,
                .value = entry.value,
                .tombstone = entry.tombstone,
            };
        }

        fn ensureSourceBlockLoaded(
            self: *@This(),
            source_index: usize,
            run: *Run,
            index: *const lsm_table_file.TableIndex,
            window: lsm_table_file.EntryDataWindow,
        ) ![]const u8 {
            if (self.source_block_indices[source_index]) |loaded_index| {
                if (loaded_index == window.relative_offset) {
                    self.backend.recordCursorBlockReuse();
                    return self.source_block_bytes[source_index].?;
                }
            }
            self.clearSourceBlock(source_index);
            self.backend.recordCursorBlockLoad();
            const bytes = if (self.backend.options.cache != null) blk: {
                var handle = try loadRunTableBlockHandle(self.backend, run, index, window);
                errdefer handle.release();
                const block = handle.runTableBlock();
                self.source_block_handles[source_index] = handle;
                break :blk block;
            } else try loadOwnedBlockForWindowAllocMaybeLocked(
                self.backend,
                self.backend.allocator,
                run,
                index,
                window,
                self.backend_locked,
            );
            self.source_block_bytes[source_index] = bytes;
            self.source_block_indices[source_index] = window.relative_offset;
            return bytes;
        }

        fn sourceLastKeyFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
        ) !?[]const u8 {
            const index = try indexForRunNoCacheMaybeLocked(self.backend, run, self.backend_locked);
            var idx = index.entryCount();
            while (idx > 0) {
                idx -= 1;
                const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, idx);
                const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                if (order == .eq) return entry.key;
                if (order == .lt) return null;
            }
            return null;
        }

        fn sourcePrevKeyFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
            target: []const u8,
            inclusive: bool,
        ) !?[]const u8 {
            const index = try indexForRunNoCacheMaybeLocked(self.backend, run, self.backend_locked);
            var lo: usize = 0;
            var hi: usize = index.entryCount();
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, mid);
                const ord = compareTableEntryTo(.{
                    .namespace_name = entry.namespace_name,
                    .key = entry.key,
                    .value = entry.value,
                    .tombstone = entry.tombstone,
                }, self.namespace, target);
                if (ord == .lt or (inclusive and ord == .eq)) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }

            var idx = lo;
            while (idx > 0) {
                idx -= 1;
                const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, idx);
                const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                if (order == .eq) return entry.key;
                if (order == .lt) return null;
            }
            return null;
        }

        fn sourceLowerBoundFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
            index: *const lsm_table_file.TableIndex,
            target: []const u8,
            inclusive: bool,
        ) !?usize {
            if (index.blockCount() > 0) {
                var block_index = index.findBlockIndex(self.namespace.name, target) orelse return null;
                while (block_index < index.blockCount()) : (block_index += 1) {
                    const block = index.blocks[block_index];
                    if (blockBeforeScanLower(block, self.namespace, target)) continue;
                    if (self.blockStartsAtOrPastUpper(block)) return null;
                    const window = index.blockWindow(block_index);
                    const bytes = try self.ensureSourceBlockLoaded(source_index, run, index, window);
                    if (try lsm_table_file.lowerBoundPositionInBlock(
                        index,
                        bytes,
                        block_index,
                        self.namespace.name,
                        target,
                        inclusive,
                    )) |positioned| {
                        return positioned.index;
                    }

                    if (compareNamespace(.{ .name = block.largest_namespace_name }, self.namespace) == .gt) {
                        return null;
                    }
                }
                return null;
            }

            var lo: usize = 0;
            var hi: usize = index.entryCount();
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, mid);
                const ord = compareTableEntryTo(.{
                    .namespace_name = entry.namespace_name,
                    .key = entry.key,
                    .value = entry.value,
                    .tombstone = entry.tombstone,
                }, self.namespace, target);
                if (ord == .lt) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }

            var idx = lo;
            while (idx < index.entryCount()) : (idx += 1) {
                const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, idx);
                const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                if (order != .eq) return null;
                if (!self.keyBeforeUpper(entry.key)) return null;
                if (!inclusive and std.mem.eql(u8, entry.key, target)) continue;
                return idx;
            }
            return null;
        }

        fn nextSourceIndexFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
            current: usize,
        ) !?usize {
            const index = try indexForRunNoCacheMaybeLocked(self.backend, run, self.backend_locked);
            if (current + 1 >= index.entryCount()) return null;

            if (index.blockCount() == 0) {
                var idx = current + 1;
                while (idx < index.entryCount()) : (idx += 1) {
                    const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, idx);
                    const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                    if (order == .eq) return idx;
                    if (order == .gt) return null;
                }
                return null;
            }

            var block_index = index.findBlockIndexForEntry(current) orelse return error.RunStateUnavailable;
            var idx = current + 1;
            while (block_index < index.blockCount()) : (block_index += 1) {
                const block = index.blocks[block_index];
                if (idx < block.first_entry_index) idx = block.first_entry_index;
                if (idx > block.lastEntryIndex()) continue;
                if (blockBeforeScanLower(block, self.namespace, "")) continue;
                if (self.blockStartsAtOrPastUpper(block)) return null;

                const window = index.blockWindow(block_index);
                const bytes = try self.ensureSourceBlockLoaded(source_index, run, index, window);
                var probe = idx;
                while (probe <= block.lastEntryIndex()) : (probe += 1) {
                    const relative_offset: usize = @intCast(index.entryStart(probe) - window.relative_offset);
                    const entry = try parseEntryAtWithStats(self.backend, bytes, relative_offset);
                    const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                    if (order == .eq) {
                        if (!self.keyBeforeUpper(entry.key)) return null;
                        return probe;
                    }
                    if (order == .gt) return null;
                }
                idx = block.lastEntryIndex() + 1;
            }
            return null;
        }

        fn keyBeforeUpper(self: *const @This(), key: []const u8) bool {
            const upper = self.upper_bound orelse return true;
            return std.mem.order(u8, key, upper) == .lt;
        }

        fn blockStartsAtOrPastUpper(self: *const @This(), block: lsm_table_file.TableIndex.BlockMeta) bool {
            const smallest_key = block.smallest_key orelse return false;
            return switch (compareNamespace(.{ .name = block.smallest_namespace_name }, self.namespace)) {
                .lt => false,
                .gt => true,
                .eq => blk: {
                    const upper = self.upper_bound orelse break :blk false;
                    break :blk std.mem.order(u8, smallest_key, upper) != .lt;
                },
            };
        }
    };
}

const BatchCursorReadResult = struct {
    hits: usize = 0,
    misses: usize = 0,

    fn add(self: *@This(), other: @This()) void {
        self.hits += other.hits;
        self.misses += other.misses;
    }
};

const max_current_batch_read_keys_per_backend_lock: usize = 128;

const MultiGetPlan = enum {
    point,
    sorted_by_run,
    cursor,
};

const MultiGetContext = enum {
    snapshot,
    stable_probe,
    current_live,
};

fn chooseMultiGetPlan(keys: []const []const u8, context: MultiGetContext) MultiGetPlan {
    const min_sorted_by_run_keys: usize = 32;
    if (keys.len < 2) return .point;
    if (!keysAreStrictlySorted(keys)) return .point;

    // Probe batches are exact key lookups, not range scans. Sorted key order is
    // not enough evidence that the table layout is scan-friendly, especially
    // for public source hydration and HBC artifact payload reads.
    if (context != .snapshot) return .point;

    if (isCursorFriendlyExactBatch(keys)) return .cursor;
    if (keys.len >= min_sorted_by_run_keys and context == .snapshot) return .sorted_by_run;
    return .point;
}

fn recordMultiGetPlan(backend: anytype, plan: MultiGetPlan) void {
    if (@hasDecl(@TypeOf(backend.*), "recordGetManySortedPlan")) {
        backend.recordGetManySortedPlan(switch (plan) {
            .point => .point,
            .sorted_by_run => .sorted_by_run,
            .cursor => .cursor,
        });
    }
}

fn isCursorFriendlyExactBatch(keys: []const []const u8) bool {
    const max_cursor_exact_batch_keys: usize = 64;
    if (keys.len < 2 or keys.len > max_cursor_exact_batch_keys) return false;

    for (keys[1..], 1..) |key, i| {
        const prev = keys[i - 1];
        if (std.mem.order(u8, prev, key) != .lt) return false;
        if (prev.len != key.len) return false;
        var common_prefix: usize = 0;
        while (common_prefix < prev.len and prev[common_prefix] == key[common_prefix]) : (common_prefix += 1) {}
        if (common_prefix + 2 < prev.len) return false;
    }
    return true;
}

fn advanceSortedBatchCursorToKey(cursor: anytype, current: ?backend_adapter.Entry, target: []const u8) !?backend_adapter.Entry {
    const max_linear_skips: usize = 64;
    var entry = current orelse return null;
    var skipped: usize = 0;
    while (std.mem.order(u8, entry.key, target) == .lt) {
        if (skipped >= max_linear_skips) return try cursor.seekAtOrAfter(target);
        entry = (try cursor.next()) orelse return null;
        skipped += 1;
    }
    return entry;
}

fn readManySortedFromCursor(
    backend: anytype,
    allocator: Allocator,
    held_values: *std.ArrayListUnmanaged([]u8),
    cursor: anytype,
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    @memset(values, null);
    if (keys.len == 0) return .{};

    var result: BatchCursorReadResult = .{};
    backend.recordPointGets(keys.len);
    var current = try cursor.seekAtOrAfter(keys[0]);
    for (keys, 0..) |key, i| {
        current = try advanceSortedBatchCursorToKey(cursor, current, key);
        const entry = current orelse {
            result.misses += keys.len - i;
            break;
        };
        if (!std.mem.eql(u8, entry.key, key)) {
            result.misses += 1;
            continue;
        }
        const owned = try allocator.dupe(u8, entry.value);
        errdefer allocator.free(owned);
        try held_values.append(allocator, owned);
        values[i] = owned;
        result.hits += 1;
    }
    return result;
}

fn readManySortedPointFromSnapshot(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(cache_mod.Handle),
    held_values: *std.ArrayListUnmanaged([]u8),
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
) !BatchCursorReadResult {
    @memset(values, null);
    var local_held_blocks = std.ArrayListUnmanaged(cache_mod.Handle).empty;
    defer if (held_blocks == null) releaseHeldBlocks(&local_held_blocks, backend.allocator);
    const block_handles = held_blocks orelse &local_held_blocks;

    var result: BatchCursorReadResult = .{};
    var last_l0_group_index: ?usize = null;
    var read_hint: ?BorrowedReadHint = null;
    backend.recordPointGets(keys.len);
    for (keys, 0..) |key, i| {
        const value = getFromSnapshotRuns(
            backend,
            mutable,
            immutable_memtables,
            runs,
            l0_groups,
            levels,
            &last_l0_group_index,
            &read_hint,
            block_handles,
            held_values,
            allocator,
            namespace,
            key,
            backend_locked,
            null,
        ) catch |err| switch (err) {
            error.NotFound => {
                result.misses += 1;
                continue;
            },
            else => return err,
        };
        if (held_blocks == null) {
            const owned = try allocator.dupe(u8, value);
            errdefer allocator.free(owned);
            try held_values.append(allocator, owned);
            values[i] = owned;
        } else {
            values[i] = value;
        }
        result.hits += 1;
    }
    return result;
}

const RunBatchIndexState = struct {
    run_index: usize,
    handle: cache_mod.Handle,
    block_index: ?usize = null,
    block_handle: ?cache_mod.Handle = null,
    block_has_values: bool = false,

    fn deinit(self: *@This()) void {
        self.handle.release();
        if (self.block_handle) |*handle| handle.release();
        self.* = undefined;
    }

    fn transferBlock(self: *@This(), allocator: Allocator, held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle)) !void {
        if (self.block_handle) |handle| {
            self.block_handle = null;
            self.block_index = null;
            if (self.block_has_values) {
                var transfer = handle;
                errdefer transfer.release();
                try held_blocks.append(allocator, transfer);
            } else {
                var discard = handle;
                discard.release();
            }
        }
        self.block_has_values = false;
    }
};

const RunBatchIndexHandles = struct {
    allocator: Allocator,
    items: std.ArrayListUnmanaged(RunBatchIndexState) = .empty,

    fn deinit(self: *@This()) void {
        for (self.items.items) |*item| item.deinit();
        self.items.deinit(self.allocator);
        self.* = undefined;
    }

    fn tableIndex(self: *@This(), backend: anytype, run: *Run, run_index: usize) !*const lsm_table_file.TableIndex {
        for (self.items.items) |*item| {
            if (item.run_index == run_index) return item.handle.runTableIndex();
        }
        var handle = try loadRunTableIndexHandle(backend, run);
        errdefer handle.release();
        try self.items.append(self.allocator, .{
            .run_index = run_index,
            .handle = handle,
        });
        return self.items.items[self.items.items.len - 1].handle.runTableIndex();
    }

    fn state(self: *@This(), backend: anytype, run: *Run, run_index: usize) !*RunBatchIndexState {
        _ = try self.tableIndex(backend, run, run_index);
        for (self.items.items) |*item| {
            if (item.run_index == run_index) return item;
        }
        unreachable;
    }

    fn transferBlocks(self: *@This(), allocator: Allocator, held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle)) !void {
        for (self.items.items) |*item| try item.transferBlock(allocator, held_blocks);
    }
};

fn readManySortedByRunFromSnapshot(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(cache_mod.Handle),
    held_values: *std.ArrayListUnmanaged([]u8),
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
) !BatchCursorReadResult {
    @memset(values, null);
    var local_held_blocks = std.ArrayListUnmanaged(cache_mod.Handle).empty;
    defer if (held_blocks == null) releaseHeldBlocks(&local_held_blocks, backend.allocator);
    const block_handles = held_blocks orelse &local_held_blocks;

    var batch_indexes = RunBatchIndexHandles{ .allocator = runtimeScratchAllocator(allocator) };
    defer batch_indexes.deinit();

    var result: BatchCursorReadResult = .{};
    var last_l0_group_index: ?usize = null;
    var read_hint: ?BorrowedReadHint = null;
    backend.recordPointGets(keys.len);
    for (keys, 0..) |key, i| {
        const value = getFromSnapshotRuns(
            backend,
            mutable,
            immutable_memtables,
            runs,
            l0_groups,
            levels,
            &last_l0_group_index,
            &read_hint,
            block_handles,
            held_values,
            allocator,
            namespace,
            key,
            backend_locked,
            &batch_indexes,
        ) catch |err| switch (err) {
            error.NotFound => {
                result.misses += 1;
                continue;
            },
            else => return err,
        };
        if (held_blocks == null) {
            const owned = try allocator.dupe(u8, value);
            errdefer allocator.free(owned);
            try held_values.append(allocator, owned);
            values[i] = owned;
        } else {
            values[i] = value;
        }
        result.hits += 1;
    }
    try batch_indexes.transferBlocks(backend.allocator, block_handles);
    return result;
}

fn readManyCurrentPointLocked(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_values: *std.ArrayListUnmanaged([]u8),
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    @memset(values, null);
    var result: BatchCursorReadResult = .{};
    backend.recordPointGets(keys.len);
    for (keys, 0..) |key, i| {
        const value = getCurrentPointRetainedLocked(BackendType, backend, namespace, allocator, held_values, key) catch |err| switch (err) {
            error.NotFound => {
                result.misses += 1;
                continue;
            },
            else => return err,
        } orelse {
            result.misses += 1;
            continue;
        };
        values[i] = value;
        result.hits += 1;
    }
    return result;
}

fn getCurrentPointRetainedLocked(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_values: *std.ArrayListUnmanaged([]u8),
    key: []const u8,
) !?[]const u8 {
    if (backend.mutable.findIndex(namespace, key)) |idx| {
        const entry = backend.mutable.entries.items[idx];
        if (entry.tombstone) return error.NotFound;
        const owned = try allocator.dupe(u8, entry.value);
        errdefer allocator.free(owned);
        try held_values.append(allocator, owned);
        backend.recordMutableHit();
        return owned;
    }

    var immutable_index = backend.immutable_memtables.items.len;
    while (immutable_index > backend.immutable_head) {
        immutable_index -= 1;
        const immutable = backend.immutable_memtables.items[immutable_index];
        if (immutable.findIndex(namespace, key)) |idx| {
            const entry = immutable.entries.items[idx];
            if (entry.tombstone) return error.NotFound;
            const owned = try allocator.dupe(u8, entry.value);
            errdefer allocator.free(owned);
            try held_values.append(allocator, owned);
            backend.recordMutableHit();
            return owned;
        }
    }

    var run_index: usize = 0;
    while (run_index < backend.runs.items.len and backend.runs.items[run_index].level == 0) : (run_index += 1) {
        if (try getFromRunPointRetainedLocked(backend, &backend.runs.items[run_index], held_values, allocator, namespace, key)) |value| return value;
    }

    while (run_index < backend.runs.items.len) {
        const level = backend.runs.items[run_index].level;
        const level_start = run_index;
        while (run_index < backend.runs.items.len and backend.runs.items[run_index].level == level) : (run_index += 1) {}
        const candidate = findRunIndexInSortedLevel(backend.runs.items[level_start..run_index], namespace, key) orelse continue;
        if (try getFromRunPointRetainedLocked(backend, &backend.runs.items[level_start + candidate], held_values, allocator, namespace, key)) |value| return value;
    }

    return null;
}

fn getFromRunPointRetainedLocked(
    backend: anytype,
    run: *Run,
    held_values: *std.ArrayListUnmanaged([]u8),
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?[]const u8 {
    if (!try runMayContainWithFilterMaybeLocked(backend, run, namespace, key, true)) return null;
    backend.recordRunProbe();

    if (run.path != null) {
        const value = try getFromRunWithLocalIndex(backend, run, held_values, value_allocator, namespace, key, true) orelse return null;
        if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
        return value;
    }

    const state = if (run.state) |*present_state| present_state else return null;
    if (state.findIndex(namespace, key)) |idx| {
        const entry = state.entries.items[idx];
        if (entry.tombstone) return error.NotFound;
        const owned = try value_allocator.dupe(u8, entry.value);
        errdefer value_allocator.free(owned);
        try held_values.append(value_allocator, owned);
        if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
        return owned;
    }
    return null;
}

fn readManyCurrentSortedPointByRunLocked(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_values: *std.ArrayListUnmanaged([]u8),
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    @memset(values, null);
    const metadata_allocator = runtimeScratchAllocator(allocator);
    const resolved = try metadata_allocator.alloc(bool, keys.len);
    defer metadata_allocator.free(resolved);
    @memset(resolved, false);

    var result: BatchCursorReadResult = .{};
    backend.recordPointGets(keys.len);
    for (keys, 0..) |key, i| {
        if (backend.mutable.findIndex(namespace, key)) |idx| {
            const entry = backend.mutable.entries.items[idx];
            resolved[i] = true;
            if (entry.tombstone) {
                result.misses += 1;
            } else {
                const owned = try allocator.dupe(u8, entry.value);
                errdefer allocator.free(owned);
                try held_values.append(allocator, owned);
                values[i] = owned;
                result.hits += 1;
                backend.recordMutableHit();
            }
        }
    }

    var immutable_index = backend.immutable_memtables.items.len;
    while (immutable_index > backend.immutable_head) {
        immutable_index -= 1;
        const immutable = backend.immutable_memtables.items[immutable_index];
        for (keys, 0..) |key, i| {
            if (resolved[i]) continue;
            if (immutable.findIndex(namespace, key)) |idx| {
                const entry = immutable.entries.items[idx];
                resolved[i] = true;
                if (entry.tombstone) {
                    result.misses += 1;
                } else {
                    const owned = try allocator.dupe(u8, entry.value);
                    errdefer allocator.free(owned);
                    try held_values.append(allocator, owned);
                    values[i] = owned;
                    result.hits += 1;
                    backend.recordMutableHit();
                }
            }
        }
    }

    var held_blocks = std.ArrayListUnmanaged(cache_mod.Handle).empty;
    defer releaseHeldBlocks(&held_blocks, backend.allocator);
    var read_hint: ?BorrowedReadHint = null;

    for (backend.runs.items, 0..) |*run, run_index| {
        var key_index = lowerBoundRunStart(keys, namespace, run.*);
        var state: ?*const State = null;
        while (key_index < keys.len) : (key_index += 1) {
            if (compareRunBound(namespace.name, keys[key_index], run.largest_namespace_name, run.largest_key) == .gt) break;
            if (resolved[key_index]) continue;
            backend.recordRunProbe();

            if (run.path != null) {
                const value = if (backend.options.cache != null) blk: {
                    const located = try getFromRunWithBlockCache(backend, run, run_index, &read_hint, &held_blocks, namespace, keys[key_index], false) orelse break :blk null;
                    read_hint = .{
                        .run_index = run_index,
                        .namespace_name = namespace.name,
                        .key = located.entry.key,
                        .entry_index = located.entry_index,
                    };
                    resolved[key_index] = true;
                    if (located.entry.tombstone) {
                        result.misses += 1;
                        continue;
                    }
                    break :blk located.entry.value;
                } else getFromRunWithLocalIndex(backend, run, held_values, allocator, namespace, keys[key_index], true) catch |err| switch (err) {
                    error.NotFound => {
                        resolved[key_index] = true;
                        result.misses += 1;
                        continue;
                    },
                    else => return err,
                };

                const concrete = value orelse continue;
                resolved[key_index] = true;
                if (backend.options.cache != null) {
                    const owned = try allocator.dupe(u8, concrete);
                    errdefer allocator.free(owned);
                    try held_values.append(allocator, owned);
                    values[key_index] = owned;
                } else {
                    values[key_index] = concrete;
                }
                result.hits += 1;
                if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
                continue;
            }

            const maybe_value = if (run.state) |*present_state| blk: {
                if (!runMayContain(run.*, namespace, keys[key_index])) break :blk null;
                if (!lsm_table_file.maybeContains(try run.ensureBloomFilter(backend.allocator), namespace.name, keys[key_index])) {
                    backend.recordBloomNegative();
                    break :blk null;
                }
                state = present_state;
                break :blk state.?;
            } else null;

            if (maybe_value) |present_state| {
                if (present_state.findIndex(namespace, keys[key_index])) |idx| {
                    const entry = present_state.entries.items[idx];
                    resolved[key_index] = true;
                    if (entry.tombstone) {
                        result.misses += 1;
                    } else {
                        const owned = try allocator.dupe(u8, entry.value);
                        errdefer allocator.free(owned);
                        try held_values.append(allocator, owned);
                        values[key_index] = owned;
                        result.hits += 1;
                        if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
                    }
                }
                continue;
            }
        }
    }

    for (resolved) |was_resolved| {
        if (!was_resolved) result.misses += 1;
    }
    return result;
}

fn keysAreSorted(keys: []const []const u8) bool {
    if (keys.len < 2) return true;
    for (keys[1..], 1..) |key, i| {
        if (std.mem.order(u8, keys[i - 1], key) == .gt) return false;
    }
    return true;
}

fn keysAreStrictlySorted(keys: []const []const u8) bool {
    if (keys.len < 2) return true;
    for (keys[1..], 1..) |key, i| {
        if (std.mem.order(u8, keys[i - 1], key) != .lt) return false;
    }
    return true;
}

fn lowerBoundRunStart(keys: []const []const u8, namespace: backend_types.Namespace, run: Run) usize {
    var lo: usize = 0;
    var hi: usize = keys.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(namespace.name, keys[mid], run.smallest_namespace_name, run.smallest_key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

fn readManySortedCurrentLocked(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_values: *std.ArrayListUnmanaged([]u8),
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    const LocalCursor = MergeCursor(BackendType, ActiveMemTable);
    const metadata_allocator = runtimeScratchAllocator(allocator);
    const runs = try borrowRunSnapshotList(metadata_allocator, backend.runs.items);
    defer freeRunSnapshotList(metadata_allocator, runs);
    const l0_groups = try buildL0RunGroupsWithStats(backend, metadata_allocator, runs);
    defer deinitRunGroups(metadata_allocator, l0_groups);
    const levels = try buildLowerLevels(metadata_allocator, runs);
    defer metadata_allocator.free(levels);
    const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
        try backend.snapshotImmutableMemtables()
    else
        &.{};
    defer if (immutable_memtables.len > 0) backend.allocator.free(immutable_memtables);

    switch (chooseMultiGetPlan(keys, .stable_probe)) {
        .cursor => {},
        .sorted_by_run => return try readManySortedByRunFromSnapshot(
            backend,
            &backend.mutable,
            immutable_memtables,
            runs,
            l0_groups,
            levels,
            allocator,
            null,
            held_values,
            namespace,
            keys,
            values,
            true,
        ),
        .point => return try readManySortedPointFromSnapshot(
            backend,
            &backend.mutable,
            immutable_memtables,
            runs,
            l0_groups,
            levels,
            allocator,
            null,
            held_values,
            namespace,
            keys,
            values,
            true,
        ),
    }

    const source_count = 1 + immutable_memtables.len + runs.len;
    const positions = try metadata_allocator.alloc(?usize, source_count);
    errdefer metadata_allocator.free(positions);
    @memset(positions, null);
    const source_entries = try metadata_allocator.alloc(?LocalCursor.SourceEntry, source_count);
    errdefer metadata_allocator.free(source_entries);
    @memset(source_entries, null);
    const source_block_bytes = try metadata_allocator.alloc(?[]const u8, source_count);
    errdefer metadata_allocator.free(source_block_bytes);
    @memset(source_block_bytes, null);
    const source_block_handles = try metadata_allocator.alloc(?cache_mod.Handle, source_count);
    errdefer metadata_allocator.free(source_block_handles);
    @memset(source_block_handles, null);
    const source_block_indices = try metadata_allocator.alloc(?usize, source_count);
    errdefer metadata_allocator.free(source_block_indices);
    @memset(source_block_indices, null);
    const advance_sources = try metadata_allocator.alloc(usize, source_count);
    errdefer metadata_allocator.free(advance_sources);
    const source_heap = try metadata_allocator.alloc(usize, source_count);
    errdefer metadata_allocator.free(source_heap);
    const source_heap_positions = try metadata_allocator.alloc(?usize, source_count);
    errdefer metadata_allocator.free(source_heap_positions);
    @memset(source_heap_positions, null);

    var cursor = LocalCursor{
        .allocator = metadata_allocator,
        .backend = backend,
        .mutable = &backend.mutable,
        .immutable_memtables = immutable_memtables,
        .runs = runs,
        .l0_groups = l0_groups,
        .levels = levels,
        .namespace = namespace,
        .positions = positions,
        .source_entries = source_entries,
        .source_block_bytes = source_block_bytes,
        .source_block_handles = source_block_handles,
        .source_block_indices = source_block_indices,
        .advance_sources = advance_sources,
        .source_heap = source_heap,
        .source_heap_positions = source_heap_positions,
        .backend_locked = true,
    };
    defer cursor.close();

    return try readManySortedFromCursor(backend, allocator, held_values, &cursor, keys, values);
}

pub fn BoundReadTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, State);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        mutable_snapshot: *const State,
        owns_mutable_snapshot: bool = false,
        immutable_memtables: []const *const State = &.{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},
        last_l0_group_index: ?usize = null,
        read_hint: ?BorrowedReadHint = null,
        held_blocks: std.ArrayListUnmanaged(cache_mod.Handle) = .empty,
        held_values: std.ArrayListUnmanaged([]u8) = .empty,

        pub fn open(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            const runs = try borrowRunSnapshotList(metadata_allocator, backend.runs.items);
            errdefer freeRunSnapshotList(metadata_allocator, runs);
            const l0_groups = try buildL0RunGroupsWithStats(backend, metadata_allocator, runs);
            errdefer deinitRunGroups(metadata_allocator, l0_groups);
            const levels = try buildLowerLevels(metadata_allocator, runs);
            errdefer metadata_allocator.free(levels);
            backend.retainReader();
            errdefer backend.releaseReader();
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try backend.snapshotImmutableMemtables()
            else
                &.{};
            errdefer if (immutable_memtables.len > 0) backend.allocator.free(immutable_memtables);
            const mutable_snapshot = try snapshotReadMutable(BackendType, backend);
            errdefer if (mutable_snapshot.owned) {
                var owned = @constCast(mutable_snapshot.state);
                owned.deinit(backend.allocator);
                backend.allocator.destroy(owned);
            };
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .namespace = namespace,
                .mutable_snapshot = mutable_snapshot.state,
                .owns_mutable_snapshot = mutable_snapshot.owned,
                .immutable_memtables = immutable_memtables,
                .runs = runs,
                .l0_groups = l0_groups,
                .levels = levels,
            };
        }

        pub fn abort(self: *@This()) void {
            const backend = self.backend;
            if (self.owns_mutable_snapshot) {
                var owned = @constCast(self.mutable_snapshot);
                owned.deinit(self.allocator);
                self.allocator.destroy(owned);
            }
            deinitRunGroups(self.metadata_allocator, self.l0_groups);
            self.metadata_allocator.free(self.levels);
            freeRunSnapshotList(self.metadata_allocator, self.runs);
            if (self.immutable_memtables.len > 0) self.allocator.free(self.immutable_memtables);
            releaseHeldBlocks(&self.held_blocks, backend.allocator);
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.releaseReader();
            self.* = undefined;
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.backend.recordPointGet();
            return try getFromSnapshotRuns(self.backend, self.mutable_snapshot, self.immutable_memtables, self.runs, self.l0_groups, self.levels, &self.last_l0_group_index, &self.read_hint, &self.held_blocks, &self.held_values, self.allocator, self.namespace, key, false, null);
        }

        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);
            const plan = chooseMultiGetPlan(keys, .snapshot);
            recordMultiGetPlan(self.backend, plan);
            const result = switch (plan) {
                .cursor => blk: {
                    var cursor = try self.openCursor();
                    defer cursor.close();
                    break :blk try readManySortedFromCursor(self.backend, self.allocator, &self.held_values, &cursor, keys, values);
                },
                .sorted_by_run => try readManySortedByRunFromSnapshot(self.backend, self.mutable_snapshot, self.immutable_memtables, self.runs, self.l0_groups, self.levels, self.allocator, &self.held_blocks, &self.held_values, self.namespace, keys, values, false),
                .point => try readManySortedPointFromSnapshot(self.backend, self.mutable_snapshot, self.immutable_memtables, self.runs, self.l0_groups, self.levels, self.allocator, &self.held_blocks, &self.held_values, self.namespace, keys, values, false),
            };
            self.backend.recordGetManySortedResults(result.hits, result.misses);
        }

        pub fn openCursor(self: *@This()) !LocalCursor {
            const source_count = 1 + self.immutable_memtables.len + self.runs.len;
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            const positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(positions);
            @memset(positions, null);
            const source_entries = try cursor_alloc.alloc(?LocalCursor.SourceEntry, source_count);
            errdefer cursor_alloc.free(source_entries);
            @memset(source_entries, null);
            const source_block_bytes = try cursor_alloc.alloc(?[]const u8, source_count);
            errdefer cursor_alloc.free(source_block_bytes);
            @memset(source_block_bytes, null);
            const source_block_handles = try cursor_alloc.alloc(?cache_mod.Handle, source_count);
            errdefer cursor_alloc.free(source_block_handles);
            @memset(source_block_handles, null);
            const source_block_indices = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_block_indices);
            @memset(source_block_indices, null);
            const advance_sources = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(advance_sources);
            const source_heap = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(source_heap);
            const source_heap_positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_heap_positions);
            @memset(source_heap_positions, null);
            return .{
                .allocator = cursor_alloc,
                .backend = self.backend,
                .mutable = self.mutable_snapshot,
                .immutable_memtables = self.immutable_memtables,
                .runs = self.runs,
                .l0_groups = self.l0_groups,
                .levels = self.levels,
                .namespace = self.namespace,
                .positions = positions,
                .source_entries = source_entries,
                .source_block_bytes = source_block_bytes,
                .source_block_handles = source_block_handles,
                .source_block_indices = source_block_indices,
                .advance_sources = advance_sources,
                .source_heap = source_heap,
                .source_heap_positions = source_heap_positions,
            };
        }
    };
}

const MutableReadSnapshot = struct {
    state: *const State,
    owned: bool,
};

fn snapshotReadMutable(comptime BackendType: type, backend: *BackendType) !MutableReadSnapshot {
    if (@hasDecl(BackendType, "snapshotMutableState")) {
        return .{ .state = try backend.snapshotMutableState(), .owned = false };
    }
    const snapshot = try backend.allocator.create(State);
    errdefer backend.allocator.destroy(snapshot);
    snapshot.* = try backend.mutable.clone(backend.allocator);
    return .{ .state = snapshot, .owned = true };
}

pub fn BoundProbeTxn(comptime BackendType: type) type {
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        stable_point_view: bool = false,
        stable_point_view_loaded: bool = false,
        empty_state: State = .{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},
        last_l0_group_index: ?usize = null,
        read_hint: ?BorrowedReadHint = null,
        held_blocks: std.ArrayListUnmanaged(cache_mod.Handle) = .empty,
        held_values: std.ArrayListUnmanaged([]u8) = .empty,

        pub fn open(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.retainReader();
            errdefer backend.releaseReader();
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            const stable_point_view = backend.mutable.entries.items.len == 0 and backend.immutable_memtables.items.len == backend.immutable_head;
            return .{
                .allocator = runtimeScratchAllocator(backend.allocator),
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .namespace = namespace,
                .stable_point_view = stable_point_view,
            };
        }

        pub fn abort(self: *@This()) void {
            const backend = self.backend;
            if (self.stable_point_view_loaded) {
                deinitRunGroups(self.metadata_allocator, self.l0_groups);
                self.metadata_allocator.free(self.levels);
                freeRunSnapshotList(self.metadata_allocator, self.runs);
            }
            releaseHeldBlocks(&self.held_blocks, backend.allocator);
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.releaseReader();
            self.* = undefined;
        }

        fn ensureStablePointViewLoaded(self: *@This()) !void {
            if (!self.stable_point_view or self.stable_point_view_loaded) return;
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            const runs = try borrowRunSnapshotList(self.metadata_allocator, self.backend.runs.items);
            errdefer freeRunSnapshotList(self.metadata_allocator, runs);
            const l0_groups = try buildL0RunGroupsWithStats(self.backend, self.metadata_allocator, runs);
            errdefer deinitRunGroups(self.metadata_allocator, l0_groups);
            const levels = try buildLowerLevels(self.metadata_allocator, runs);
            errdefer self.metadata_allocator.free(levels);
            self.runs = runs;
            self.l0_groups = l0_groups;
            self.levels = levels;
            self.stable_point_view_loaded = true;
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (self.stable_point_view) {
                try self.ensureStablePointViewLoaded();
                self.backend.recordPointGet();
                switch (try getFromStableCachedPointView(self.backend, self.metadata_allocator, self.runs, self.l0_groups, self.levels, &self.last_l0_group_index, self.namespace, key)) {
                    .hit => |value| return value,
                    .miss => return error.NotFound,
                    .unavailable => return try getFromSnapshotRuns(
                        self.backend,
                        &self.empty_state,
                        &.{},
                        self.runs,
                        self.l0_groups,
                        self.levels,
                        &self.last_l0_group_index,
                        &self.read_hint,
                        &self.held_blocks,
                        &self.held_values,
                        self.allocator,
                        self.namespace,
                        key,
                        false,
                        null,
                    ),
                }
            }
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            const keys = [_][]const u8{key};
            var values = [_]?[]const u8{null};
            const result = try readManyCurrentPointLocked(BackendType, self.backend, self.namespace, self.allocator, &self.held_values, &keys, &values);
            if (result.hits == 0) return error.NotFound;
            return values[0].?;
        }

        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);

            var result: BatchCursorReadResult = .{};
            var offset: usize = 0;
            while (offset < keys.len) {
                const end = @min(offset + max_current_batch_read_keys_per_backend_lock, keys.len);
                const plan = chooseMultiGetPlan(keys[offset..end], if (self.stable_point_view) .stable_probe else .current_live);
                recordMultiGetPlan(self.backend, plan);
                const chunk_result = if (self.stable_point_view) blk: {
                    try self.ensureStablePointViewLoaded();
                    break :blk switch (plan) {
                        .sorted_by_run => try readManySortedByRunFromSnapshot(
                            self.backend,
                            &self.empty_state,
                            &.{},
                            self.runs,
                            self.l0_groups,
                            self.levels,
                            self.allocator,
                            &self.held_blocks,
                            &self.held_values,
                            self.namespace,
                            keys[offset..end],
                            values[offset..end],
                            false,
                        ),
                        .cursor, .point => try readManySortedPointFromSnapshot(
                            self.backend,
                            &self.empty_state,
                            &.{},
                            self.runs,
                            self.l0_groups,
                            self.levels,
                            self.allocator,
                            &self.held_blocks,
                            &self.held_values,
                            self.namespace,
                            keys[offset..end],
                            values[offset..end],
                            false,
                        ),
                    };
                } else blk: {
                    const locked = lockBackend(BackendType, self.backend);
                    defer unlockBackend(BackendType, self.backend, locked);
                    break :blk switch (plan) {
                        .cursor => try readManySortedCurrentLocked(BackendType, self.backend, self.namespace, self.allocator, &self.held_values, keys[offset..end], values[offset..end]),
                        .sorted_by_run => try readManyCurrentSortedPointByRunLocked(BackendType, self.backend, self.namespace, self.allocator, &self.held_values, keys[offset..end], values[offset..end]),
                        .point => try readManySortedCurrentLocked(BackendType, self.backend, self.namespace, self.allocator, &self.held_values, keys[offset..end], values[offset..end]),
                    };
                };
                result.add(chunk_result);
                offset = end;
            }
            self.backend.recordGetManySortedResults(result.hits, result.misses);
        }
    };
}

pub fn BoundCurrentScanTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, ActiveMemTable);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        immutable_memtables: []const *const State = &.{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},

        pub fn open(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            const runs = try borrowRunSnapshotList(metadata_allocator, backend.runs.items);
            errdefer freeRunSnapshotList(metadata_allocator, runs);
            const l0_groups = try buildL0RunGroupsWithStats(backend, metadata_allocator, runs);
            errdefer deinitRunGroups(metadata_allocator, l0_groups);
            const levels = try buildLowerLevels(metadata_allocator, runs);
            errdefer metadata_allocator.free(levels);
            backend.retainReader();
            errdefer backend.releaseReader();
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try backend.snapshotImmutableMemtables()
            else
                &.{};
            errdefer if (immutable_memtables.len > 0) backend.allocator.free(immutable_memtables);
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .namespace = namespace,
                .immutable_memtables = immutable_memtables,
                .runs = runs,
                .l0_groups = l0_groups,
                .levels = levels,
            };
        }

        pub fn abort(self: *@This()) void {
            const backend = self.backend;
            deinitRunGroups(self.metadata_allocator, self.l0_groups);
            self.metadata_allocator.free(self.levels);
            freeRunSnapshotList(self.metadata_allocator, self.runs);
            if (self.immutable_memtables.len > 0) self.allocator.free(self.immutable_memtables);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.releaseReader();
            self.* = undefined;
        }

        pub fn openCursor(self: *@This()) !LocalCursor {
            const source_count = 1 + self.immutable_memtables.len + self.runs.len;
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            const positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(positions);
            @memset(positions, null);
            const source_entries = try cursor_alloc.alloc(?LocalCursor.SourceEntry, source_count);
            errdefer cursor_alloc.free(source_entries);
            @memset(source_entries, null);
            const source_block_bytes = try cursor_alloc.alloc(?[]const u8, source_count);
            errdefer cursor_alloc.free(source_block_bytes);
            @memset(source_block_bytes, null);
            const source_block_handles = try cursor_alloc.alloc(?cache_mod.Handle, source_count);
            errdefer cursor_alloc.free(source_block_handles);
            @memset(source_block_handles, null);
            const source_block_indices = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_block_indices);
            @memset(source_block_indices, null);
            const advance_sources = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(advance_sources);
            const source_heap = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(source_heap);
            const source_heap_positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_heap_positions);
            @memset(source_heap_positions, null);
            return .{
                .allocator = cursor_alloc,
                .backend = self.backend,
                .mutable = &self.backend.mutable,
                .immutable_memtables = self.immutable_memtables,
                .runs = self.runs,
                .l0_groups = self.l0_groups,
                .levels = self.levels,
                .namespace = self.namespace,
                .positions = positions,
                .source_entries = source_entries,
                .source_block_bytes = source_block_bytes,
                .source_block_handles = source_block_handles,
                .source_block_indices = source_block_indices,
                .advance_sources = advance_sources,
                .source_heap = source_heap,
                .source_heap_positions = source_heap_positions,
            };
        }
    };
}

pub fn BoundProbeCursor(comptime BackendType: type) type {
    return struct {
        allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        current_key: ?[]u8 = null,
        visible_entry_bytes: ?[]u8 = null,
        upper_bound: ?[]const u8 = null,

        pub fn close(self: *@This()) void {
            self.clearCurrentKey();
            self.clearVisibleEntryBytes();
            self.* = undefined;
        }

        pub fn first(self: *@This()) !?backend_adapter.Entry {
            return try self.seekAtOrAfter("");
        }

        pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
            self.upper_bound = upper;
        }

        pub fn last(_: *@This()) !?backend_adapter.Entry {
            return error.Unsupported;
        }

        pub fn next(self: *@This()) !?backend_adapter.Entry {
            const key = self.current_key orelse return null;
            return try self.findAtOrAfter(key, false);
        }

        pub fn prev(_: *@This()) !?backend_adapter.Entry {
            return error.Unsupported;
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            return try self.findAtOrAfter(key, true);
        }

        pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?backend_adapter.Entry {
            return error.Unsupported;
        }

        fn findAtOrAfter(self: *@This(), key: []const u8, inclusive: bool) !?backend_adapter.Entry {
            const stable_key = try self.allocator.dupe(u8, key);
            defer self.allocator.free(stable_key);

            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);

            const metadata_allocator = runtimeScratchAllocator(self.allocator);
            const runs = try borrowRunSnapshotList(metadata_allocator, self.backend.runs.items);
            defer freeRunSnapshotList(metadata_allocator, runs);
            const l0_groups = try buildL0RunGroupsWithStats(self.backend, metadata_allocator, runs);
            defer deinitRunGroups(metadata_allocator, l0_groups);
            const levels = try buildLowerLevels(metadata_allocator, runs);
            defer metadata_allocator.free(levels);
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try self.backend.snapshotImmutableMemtables()
            else
                &.{};
            defer if (immutable_memtables.len > 0) self.allocator.free(immutable_memtables);

            const positions = try metadata_allocator.alloc(?usize, 1 + immutable_memtables.len + runs.len);
            defer metadata_allocator.free(positions);
            @memset(positions, null);
            const source_entries = try metadata_allocator.alloc(?MergeCursor(BackendType, ActiveMemTable).SourceEntry, positions.len);
            defer metadata_allocator.free(source_entries);
            @memset(source_entries, null);
            const source_block_bytes = try metadata_allocator.alloc(?[]const u8, positions.len);
            defer {
                metadata_allocator.free(source_block_bytes);
            }
            @memset(source_block_bytes, null);
            const source_block_handles = try metadata_allocator.alloc(?cache_mod.Handle, positions.len);
            defer metadata_allocator.free(source_block_handles);
            @memset(source_block_handles, null);
            const source_block_indices = try metadata_allocator.alloc(?usize, positions.len);
            defer metadata_allocator.free(source_block_indices);
            @memset(source_block_indices, null);
            const advance_sources = try metadata_allocator.alloc(usize, positions.len);
            defer metadata_allocator.free(advance_sources);
            const source_heap = try metadata_allocator.alloc(usize, positions.len);
            defer metadata_allocator.free(source_heap);
            const source_heap_positions = try metadata_allocator.alloc(?usize, positions.len);
            defer metadata_allocator.free(source_heap_positions);
            @memset(source_heap_positions, null);

            var cursor = MergeCursor(BackendType, ActiveMemTable){
                .allocator = metadata_allocator,
                .backend = self.backend,
                .mutable = &self.backend.mutable,
                .immutable_memtables = immutable_memtables,
                .runs = runs,
                .l0_groups = l0_groups,
                .levels = levels,
                .namespace = self.namespace,
                .positions = positions,
                .source_entries = source_entries,
                .source_block_bytes = source_block_bytes,
                .source_block_handles = source_block_handles,
                .source_block_indices = source_block_indices,
                .advance_sources = advance_sources,
                .source_heap = source_heap,
                .source_heap_positions = source_heap_positions,
                .upper_bound = self.upper_bound,
                .backend_locked = true,
            };
            defer {
                for (0..source_block_bytes.len) |source_index| cursor.clearSourceBlock(source_index);
            }
            const entry = if (inclusive)
                try cursor.seekAtOrAfter(stable_key)
            else blk: {
                try cursor.initForwardPositions(stable_key, false);
                break :blk try cursor.selectVisibleForward();
            };
            const visible = entry orelse {
                self.clearCurrentKey();
                self.clearVisibleEntryBytes();
                return null;
            };
            return try self.replaceVisibleEntry(visible);
        }

        fn replaceVisibleEntry(self: *@This(), entry: backend_adapter.Entry) !backend_adapter.Entry {
            const key_len = entry.key.len;
            const value_len = entry.value.len;
            const bytes = try self.allocator.alloc(u8, key_len + value_len);
            errdefer self.allocator.free(bytes);
            @memcpy(bytes[0..key_len], entry.key);
            @memcpy(bytes[key_len..][0..value_len], entry.value);
            const key = bytes[0..key_len];
            const value = bytes[key_len..][0..value_len];

            self.clearCurrentKey();
            self.clearVisibleEntryBytes();
            self.current_key = try self.allocator.dupe(u8, key);
            self.visible_entry_bytes = bytes;
            return .{ .key = key, .value = value };
        }

        fn clearCurrentKey(self: *@This()) void {
            if (self.current_key) |key| self.allocator.free(key);
            self.current_key = null;
        }

        fn clearVisibleEntryBytes(self: *@This()) void {
            if (self.visible_entry_bytes) |bytes| self.allocator.free(bytes);
            self.visible_entry_bytes = null;
        }
    };
}

pub fn BoundWriteTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, State);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        mutable: ActiveMemTable,
        bulk_appends: State = .{},
        cursor_overlay: ?State = null,
        cursor_base_mutable: ?State = null,
        cursor_immutable_memtables: []const *const State = &.{},
        cursor_runs: []Run = &.{},
        cursor_l0_groups: []RunGroup = &.{},
        cursor_levels: []RunLevel = &.{},
        held_values: std.ArrayListUnmanaged([]u8) = .empty,
        batch_options: backend_types.BatchOptions = .{},
        closed: bool = false,

        pub fn open(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            return try openWithOptions(backend, namespace, .{});
        }

        pub fn openWithOptions(backend: *BackendType, namespace: backend_types.Namespace, options: backend_types.BatchOptions) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.retainReader();
            errdefer backend.releaseReader();
            backend.beginBatchMode(options);
            errdefer backend.finishBatchMode(options);
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = runtimeScratchAllocator(backend.allocator),
                .backend = backend,
                .namespace = namespace,
                .mutable = .{},
                .batch_options = options,
            };
        }

        pub fn abort(self: *@This()) void {
            if (self.closed) return;
            const backend = self.backend;
            self.mutable.deinit(self.allocator);
            self.bulk_appends.deinit(self.allocator);
            self.invalidateCursorSnapshot();
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.finishBatchMode(self.batch_options);
            backend.releaseReader();
            self.* = undefined;
        }

        pub fn commit(self: *@This()) !void {
            if (self.closed) return error.TransactionClosed;
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            var release_on_error = true;
            errdefer if (release_on_error) {
                self.mutable.deinit(self.allocator);
                self.mutable = .{};
                self.bulk_appends.deinit(self.allocator);
                self.bulk_appends = .{};
                self.invalidateCursorSnapshot();
                releaseHeldValues(&self.held_values, self.allocator);
                self.backend.finishBatchMode(self.batch_options);
                self.backend.releaseReader();
                self.closed = true;
            };
            const direct_ingested_bulk_appends = try self.tryCommitDirectBulkAppends();
            if (!try self.tryCommitDirectBulkIngest()) {
                const mutated = self.mutable.entries.items.len > 0;
                if (@hasDecl(BackendType, "appendWalForMutable")) {
                    try self.backend.appendWalForMutable(&self.mutable);
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyMutableMoveToMutable(&self.backend.mutable, self.allocator, &self.mutable);
                } else if (@hasDecl(BackendType, "appendWalForState")) {
                    var sorted = try self.mutable.toStateMove(self.allocator);
                    defer sorted.deinit(self.allocator);
                    try self.backend.appendWalForState(&sorted);
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyStateMoveToMutable(&self.backend.mutable, self.allocator, &sorted);
                } else {
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyMutableMoveToMutable(&self.backend.mutable, self.allocator, &self.mutable);
                }
                if ((mutated or direct_ingested_bulk_appends) and @hasDecl(BackendType, "notePotentialMaintenanceDebt")) self.backend.notePotentialMaintenanceDebt();
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
                try self.backend.maybeFlushMutable();
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
            } else {
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
            }
            self.backend.finishBatchMode(self.batch_options);
            try self.backend.finalizeExitedBatchMode(self.batch_options);
            release_on_error = false;
            self.closed = true;
            self.invalidateCursorSnapshot();
            releaseHeldValues(&self.held_values, self.allocator);
            var finalize_err: ?anyerror = null;
            self.backend.finalizeWriteReaderRelease() catch |err| {
                finalize_err = err;
            };
            if (finalize_err) |err| return err;
        }

        fn tryCommitDirectBulkAppends(self: *@This()) !bool {
            var entries = self.bulk_appends.entries.items.len;
            if (entries == 0) return false;
            if (self.batch_options.mode != .bulk_ingest) {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackNonBulk")) self.backend.recordBulkAppendFallbackNonBulk(entries);
                try state_mod.applyStateMoveToMutable(&self.mutable, self.allocator, &self.bulk_appends);
                return false;
            }
            if (!@hasDecl(BackendType, "ingestSortedState") or !@hasDecl(BackendType, "shouldDirectIngestBulkState")) {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackUnsupported")) self.backend.recordBulkAppendFallbackUnsupported(entries);
                try state_mod.applyStateMoveToMutable(&self.mutable, self.allocator, &self.bulk_appends);
                return false;
            }
            if (self.backend.mutable.entries.items.len != 0 and @hasDecl(BackendType, "drainMutableBeforeBulkAppendDirectIngest")) {
                if (!try self.backend.drainMutableBeforeBulkAppendDirectIngest()) {
                    if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                    if (@hasDecl(BackendType, "recordBulkAppendFallbackBackendPending")) self.backend.recordBulkAppendFallbackBackendPending(entries);
                    try state_mod.applyStateMoveToMutable(&self.mutable, self.allocator, &self.bulk_appends);
                    return false;
                }
            }
            if (self.backend.mutable.entries.items.len != 0 or self.backend.activeImmutableMemtableCount() != 0) {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackBackendPending")) self.backend.recordBulkAppendFallbackBackendPending(entries);
                try state_mod.applyStateMoveToMutable(&self.mutable, self.allocator, &self.bulk_appends);
                return false;
            }
            if (self.mutable.entries.items.len > 0) {
                try state_mod.applyMutableMoveToMutable(&self.bulk_appends, self.allocator, &self.mutable);
                entries = self.bulk_appends.entries.items.len;
            }
            if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);

            const sort_start_ns = platform_time.monotonicNs();
            state_mod.sortStateEntries(&self.bulk_appends);
            const sort_ns = elapsedNs(sort_start_ns);
            if (!bulkStateEntriesAreUnique(&self.bulk_appends)) {
                if (@hasDecl(BackendType, "recordBulkAppendFallbackDuplicateKeys")) self.backend.recordBulkAppendFallbackDuplicateKeys(entries, sort_ns);
                try state_mod.applyStateMoveToMutable(&self.mutable, self.allocator, &self.bulk_appends);
                return false;
            }
            if (!self.backend.shouldDirectIngestBulkState(&self.bulk_appends)) {
                if (@hasDecl(BackendType, "recordBulkAppendFallbackBelowThreshold")) self.backend.recordBulkAppendFallbackBelowThreshold(entries, sort_ns);
                try state_mod.applyStateMoveToMutable(&self.mutable, self.allocator, &self.bulk_appends);
                return false;
            }

            if (@hasDecl(BackendType, "appendWalForState")) try self.backend.appendWalForState(&self.bulk_appends);
            if (@hasDecl(BackendType, "ingestOwnedSortedState")) {
                try self.backend.ingestOwnedSortedState(&self.bulk_appends);
            } else {
                try self.backend.ingestSortedState(&self.bulk_appends);
            }
            if (@hasDecl(BackendType, "recordBulkAppendSuccess")) self.backend.recordBulkAppendSuccess(entries, sort_ns);
            self.bulk_appends.deinit(self.allocator);
            self.bulk_appends = .{};
            return true;
        }

        fn bulkStateEntriesAreUnique(state: *const State) bool {
            if (state.entries.items.len <= 1) return true;
            var previous = state.entries.items[0];
            for (state.entries.items[1..]) |entry| {
                if (compareEntryTo(previous, state_mod.namespaceOf(entry), entry.key) == .eq) return false;
                previous = entry;
            }
            return true;
        }

        fn tryCommitDirectBulkIngest(self: *@This()) !bool {
            if (self.batch_options.mode != .bulk_ingest) return false;
            const entries = self.mutable.entries.items.len;
            if (entries == 0) return false;
            if (@hasDecl(BackendType, "recordDirectBulkIngestAttempt")) self.backend.recordDirectBulkIngestAttempt(entries);
            if (!@hasDecl(BackendType, "ingestSortedState") or !@hasDecl(BackendType, "shouldDirectIngestBulkState")) {
                if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackUnsupported")) self.backend.recordDirectBulkIngestFallbackUnsupported();
                return false;
            }
            if (self.backend.mutable.entries.items.len != 0) {
                if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBackendMutable")) self.backend.recordDirectBulkIngestFallbackBackendMutable();
                return false;
            }
            if (@hasDecl(BackendType, "shouldDirectIngestBulkMutable")) {
                if (!self.backend.shouldDirectIngestBulkMutable(&self.mutable)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBelowThreshold")) self.backend.recordDirectBulkIngestFallbackBelowThreshold();
                    return false;
                }
                if (@hasDecl(BackendType, "appendWalForMutable")) try self.backend.appendWalForMutable(&self.mutable);
                const sort_start_ns = platform_time.monotonicNs();
                var sorted = try self.mutable.toStateMove(self.allocator);
                errdefer sorted.deinit(self.allocator);
                const sort_ns = elapsedNs(sort_start_ns);
                if (@hasDecl(BackendType, "ingestOwnedSortedState")) {
                    try self.backend.ingestOwnedSortedState(&sorted);
                } else {
                    try self.backend.ingestSortedState(&sorted);
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, sort_ns);
                sorted.deinit(self.allocator);
            } else {
                const sort_start_ns = platform_time.monotonicNs();
                var sorted = try self.mutable.clone(self.allocator);
                errdefer sorted.deinit(self.allocator);
                const sort_ns = elapsedNs(sort_start_ns);
                if (!self.backend.shouldDirectIngestBulkState(&sorted)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBelowThreshold")) self.backend.recordDirectBulkIngestFallbackBelowThreshold();
                    sorted.deinit(self.allocator);
                    return false;
                }
                if (@hasDecl(BackendType, "appendWalForState")) try self.backend.appendWalForState(&sorted);
                if (@hasDecl(BackendType, "ingestOwnedSortedState")) {
                    try self.backend.ingestOwnedSortedState(&sorted);
                } else {
                    try self.backend.ingestSortedState(&sorted);
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, sort_ns);
                sorted.deinit(self.allocator);
            }
            self.mutable.deinit(self.allocator);
            self.mutable = .{};
            if (@hasDecl(BackendType, "notePotentialMaintenanceDebt")) self.backend.notePotentialMaintenanceDebt();
            return true;
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            var bulk_idx = self.bulk_appends.entries.items.len;
            while (bulk_idx > 0) {
                bulk_idx -= 1;
                const entry = self.bulk_appends.entries.items[bulk_idx];
                if (compareEntryTo(entry, self.namespace, key) == .eq) {
                    if (entry.tombstone) return error.NotFound;
                    return entry.value;
                }
            }
            if (self.mutable.findIndex(self.namespace, key)) |idx| {
                const entry = self.mutable.entries.items[idx];
                if (entry.tombstone) return error.NotFound;
                return entry.value;
            }
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            if (comptime @hasField(BackendType, "runs") and @hasField(BackendType, "immutable_memtables")) {
                self.backend.recordPointGets(1);
                return try getCurrentPointRetainedLocked(BackendType, self.backend, self.namespace, self.allocator, &self.held_values, key) orelse error.NotFound;
            }
            return self.backend.getMergedWithOverlay(&self.backend.mutable, &self.mutable, self.namespace, key);
        }

        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);
            @memset(values, null);

            const miss_keys = try self.metadata_allocator.alloc([]const u8, keys.len);
            defer self.metadata_allocator.free(miss_keys);
            const miss_indexes = try self.metadata_allocator.alloc(usize, keys.len);
            defer self.metadata_allocator.free(miss_indexes);

            var hits: usize = 0;
            var misses: usize = 0;
            var miss_count: usize = 0;
            var overlay_point_gets: usize = 0;
            for (keys, 0..) |key, i| {
                var bulk_idx = self.bulk_appends.entries.items.len;
                while (bulk_idx > 0) {
                    bulk_idx -= 1;
                    const entry = self.bulk_appends.entries.items[bulk_idx];
                    if (compareEntryTo(entry, self.namespace, key) == .eq) {
                        overlay_point_gets += 1;
                        if (entry.tombstone) {
                            misses += 1;
                        } else {
                            values[i] = entry.value;
                            hits += 1;
                        }
                        break;
                    }
                } else {
                    if (self.mutable.findIndex(self.namespace, key)) |idx| {
                        overlay_point_gets += 1;
                        const entry = self.mutable.entries.items[idx];
                        if (entry.tombstone) {
                            misses += 1;
                        } else {
                            values[i] = entry.value;
                            hits += 1;
                        }
                        continue;
                    }
                    miss_keys[miss_count] = key;
                    miss_indexes[miss_count] = i;
                    miss_count += 1;
                    continue;
                }
                continue;
            }
            self.backend.recordPointGets(overlay_point_gets);

            if (miss_count > 0) {
                const miss_values = try self.metadata_allocator.alloc(?[]const u8, miss_count);
                defer self.metadata_allocator.free(miss_values);
                var offset: usize = 0;
                while (offset < miss_count) {
                    const end = @min(offset + max_current_batch_read_keys_per_backend_lock, miss_count);
                    const plan = chooseMultiGetPlan(miss_keys[offset..end], .current_live);
                    recordMultiGetPlan(self.backend, plan);
                    const result = blk: {
                        const locked = lockBackend(BackendType, self.backend);
                        defer unlockBackend(BackendType, self.backend, locked);
                        break :blk switch (plan) {
                            .cursor => try readManySortedCurrentLocked(BackendType, self.backend, self.namespace, self.allocator, &self.held_values, miss_keys[offset..end], miss_values[offset..end]),
                            .sorted_by_run => try readManyCurrentSortedPointByRunLocked(BackendType, self.backend, self.namespace, self.allocator, &self.held_values, miss_keys[offset..end], miss_values[offset..end]),
                            .point => try readManyCurrentPointLocked(BackendType, self.backend, self.namespace, self.allocator, &self.held_values, miss_keys[offset..end], miss_values[offset..end]),
                        };
                    };
                    hits += result.hits;
                    misses += result.misses;
                    offset = end;
                }
                for (miss_values, 0..) |maybe_value, miss_index| {
                    values[miss_indexes[miss_index]] = maybe_value;
                }
            }

            self.backend.recordGetManySortedResults(hits, misses);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            try self.mutable.upsert(self.allocator, self.namespace, key, value, false);
            self.invalidateCursorSnapshot();
        }

        pub fn appendPut(self: *@This(), key: []const u8, value: []const u8) !void {
            if (self.batch_options.mode == .bulk_ingest) {
                const entry_allocator = try self.bulk_appends.ensureArenaAllocator(self.allocator);
                try self.bulk_appends.entries.append(self.allocator, try state_mod.initArenaEntry(entry_allocator, self.namespace, key, value, false));
                self.invalidateCursorSnapshot();
                return;
            }
            try self.mutable.appendUpsert(self.allocator, self.namespace, key, value, false);
            self.invalidateCursorSnapshot();
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            try self.mutable.upsert(self.allocator, self.namespace, key, "", true);
            self.invalidateCursorSnapshot();
        }

        pub fn openCursor(self: *@This()) !LocalCursor {
            try self.ensureCursorSnapshot();
            const source_count = 1 + self.cursor_immutable_memtables.len + self.cursor_runs.len;
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            const positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(positions);
            @memset(positions, null);
            const source_entries = try cursor_alloc.alloc(?LocalCursor.SourceEntry, source_count);
            errdefer cursor_alloc.free(source_entries);
            @memset(source_entries, null);
            const source_block_bytes = try cursor_alloc.alloc(?[]const u8, source_count);
            errdefer cursor_alloc.free(source_block_bytes);
            @memset(source_block_bytes, null);
            const source_block_handles = try cursor_alloc.alloc(?cache_mod.Handle, source_count);
            errdefer cursor_alloc.free(source_block_handles);
            @memset(source_block_handles, null);
            const source_block_indices = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_block_indices);
            @memset(source_block_indices, null);
            const advance_sources = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(advance_sources);
            const source_heap = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(source_heap);
            const source_heap_positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_heap_positions);
            @memset(source_heap_positions, null);
            return .{
                .allocator = cursor_alloc,
                .backend = self.backend,
                .mutable = &self.cursor_overlay.?,
                .immutable_memtables = self.cursor_immutable_memtables,
                .runs = self.cursor_runs,
                .l0_groups = self.cursor_l0_groups,
                .levels = self.cursor_levels,
                .namespace = self.namespace,
                .positions = positions,
                .source_entries = source_entries,
                .source_block_bytes = source_block_bytes,
                .source_block_handles = source_block_handles,
                .source_block_indices = source_block_indices,
                .advance_sources = advance_sources,
                .source_heap = source_heap,
                .source_heap_positions = source_heap_positions,
            };
        }

        fn ensureCursorSnapshot(self: *@This()) !void {
            if (self.cursor_overlay != null) return;
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);

            var overlay: State = .{};
            errdefer overlay.deinit(self.allocator);
            try state_mod.applyState(&overlay, self.allocator, &self.mutable);
            try state_mod.applyState(&overlay, self.allocator, &self.bulk_appends);

            var base_mutable: State = .{};
            errdefer base_mutable.deinit(self.allocator);
            try state_mod.applyState(&base_mutable, self.allocator, &self.backend.mutable);

            const backend_immutable = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try self.backend.snapshotImmutableMemtables()
            else
                &.{};
            defer if (backend_immutable.len > 0) self.allocator.free(backend_immutable);

            const immutable = try self.allocator.alloc(*const State, 1 + backend_immutable.len);
            errdefer self.allocator.free(immutable);
            for (backend_immutable, 0..) |state, i| immutable[i + 1] = state;

            const runs = try borrowRunSnapshotList(self.metadata_allocator, self.backend.runs.items);
            errdefer freeRunSnapshotList(self.metadata_allocator, runs);
            const l0_groups = try buildL0RunGroupsWithStats(self.backend, self.metadata_allocator, runs);
            errdefer deinitRunGroups(self.metadata_allocator, l0_groups);
            const levels = try buildLowerLevels(self.metadata_allocator, runs);
            errdefer self.metadata_allocator.free(levels);

            self.cursor_overlay = overlay;
            self.cursor_base_mutable = base_mutable;
            immutable[0] = &self.cursor_base_mutable.?;
            self.cursor_immutable_memtables = immutable;
            self.cursor_runs = runs;
            self.cursor_l0_groups = l0_groups;
            self.cursor_levels = levels;
        }

        fn invalidateCursorSnapshot(self: *@This()) void {
            if (self.cursor_overlay) |*state| {
                state.deinit(self.allocator);
                self.cursor_overlay = null;
            }
            if (self.cursor_base_mutable) |*state| {
                state.deinit(self.allocator);
                self.cursor_base_mutable = null;
            }
            if (self.cursor_immutable_memtables.len > 0) {
                self.allocator.free(self.cursor_immutable_memtables);
                self.cursor_immutable_memtables = &.{};
            }
            if (self.cursor_l0_groups.len > 0) {
                deinitRunGroups(self.metadata_allocator, self.cursor_l0_groups);
                self.cursor_l0_groups = &.{};
            }
            if (self.cursor_levels.len > 0) {
                self.metadata_allocator.free(self.cursor_levels);
                self.cursor_levels = &.{};
            }
            if (self.cursor_runs.len > 0) {
                freeRunSnapshotList(self.metadata_allocator, self.cursor_runs);
                self.cursor_runs = &.{};
            }
        }
    };
}

pub fn NamespaceReadTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, State);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        mutable_snapshot: *const State,
        owns_mutable_snapshot: bool = false,
        snapshot: ?State = null,
        immutable_memtables: []const *const State = &.{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},
        last_l0_group_index: ?usize = null,
        read_hint: ?BorrowedReadHint = null,
        held_blocks: std.ArrayListUnmanaged(cache_mod.Handle) = .empty,
        held_values: std.ArrayListUnmanaged([]u8) = .empty,

        pub fn open(backend: *BackendType) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            const runs = try borrowRunSnapshotList(metadata_allocator, backend.runs.items);
            errdefer freeRunSnapshotList(metadata_allocator, runs);
            const l0_groups = try buildL0RunGroupsWithStats(backend, metadata_allocator, runs);
            errdefer deinitRunGroups(metadata_allocator, l0_groups);
            const levels = try buildLowerLevels(metadata_allocator, runs);
            errdefer metadata_allocator.free(levels);
            backend.retainReader();
            errdefer backend.releaseReader();
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try backend.snapshotImmutableMemtables()
            else
                &.{};
            errdefer if (immutable_memtables.len > 0) backend.allocator.free(immutable_memtables);
            const mutable_snapshot = try snapshotReadMutable(BackendType, backend);
            errdefer if (mutable_snapshot.owned) {
                var owned = @constCast(mutable_snapshot.state);
                owned.deinit(backend.allocator);
                backend.allocator.destroy(owned);
            };
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .mutable_snapshot = mutable_snapshot.state,
                .owns_mutable_snapshot = mutable_snapshot.owned,
                .immutable_memtables = immutable_memtables,
                .runs = runs,
                .l0_groups = l0_groups,
                .levels = levels,
            };
        }

        pub fn abort(self: *@This()) void {
            const backend = self.backend;
            if (self.owns_mutable_snapshot) {
                var owned = @constCast(self.mutable_snapshot);
                owned.deinit(self.allocator);
                self.allocator.destroy(owned);
            }
            deinitRunGroups(self.metadata_allocator, self.l0_groups);
            self.metadata_allocator.free(self.levels);
            freeRunSnapshotList(self.metadata_allocator, self.runs);
            if (self.immutable_memtables.len > 0) self.allocator.free(self.immutable_memtables);
            if (self.snapshot) |*snapshot| snapshot.deinit(self.allocator);
            releaseHeldBlocks(&self.held_blocks, self.allocator);
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.releaseReader();
            self.* = undefined;
        }

        pub fn get(self: *@This(), namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            self.backend.recordPointGet();
            return try getFromSnapshotRuns(self.backend, self.mutable_snapshot, self.immutable_memtables, self.runs, self.l0_groups, self.levels, &self.last_l0_group_index, &self.read_hint, &self.held_blocks, &self.held_values, self.allocator, namespace, key, false, null);
        }

        pub fn getManySorted(self: *@This(), namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);
            const plan = chooseMultiGetPlan(keys, .snapshot);
            recordMultiGetPlan(self.backend, plan);
            const result = switch (plan) {
                .cursor => blk: {
                    var cursor = try self.openCursor(namespace);
                    defer cursor.close();
                    break :blk try readManySortedFromCursor(self.backend, self.allocator, &self.held_values, &cursor, keys, values);
                },
                .sorted_by_run => try readManySortedByRunFromSnapshot(self.backend, self.mutable_snapshot, self.immutable_memtables, self.runs, self.l0_groups, self.levels, self.allocator, &self.held_blocks, &self.held_values, namespace, keys, values, false),
                .point => try readManySortedPointFromSnapshot(self.backend, self.mutable_snapshot, self.immutable_memtables, self.runs, self.l0_groups, self.levels, self.allocator, &self.held_blocks, &self.held_values, namespace, keys, values, false),
            };
            self.backend.recordGetManySortedResults(result.hits, result.misses);
        }

        pub fn openCursor(self: *@This(), namespace: backend_types.Namespace) !LocalCursor {
            const source_count = 1 + self.immutable_memtables.len + self.runs.len;
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            const positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(positions);
            @memset(positions, null);
            const source_entries = try cursor_alloc.alloc(?LocalCursor.SourceEntry, source_count);
            errdefer cursor_alloc.free(source_entries);
            @memset(source_entries, null);
            const source_block_bytes = try cursor_alloc.alloc(?[]const u8, source_count);
            errdefer cursor_alloc.free(source_block_bytes);
            @memset(source_block_bytes, null);
            const source_block_handles = try cursor_alloc.alloc(?cache_mod.Handle, source_count);
            errdefer cursor_alloc.free(source_block_handles);
            @memset(source_block_handles, null);
            const source_block_indices = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_block_indices);
            @memset(source_block_indices, null);
            const advance_sources = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(advance_sources);
            const source_heap = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(source_heap);
            const source_heap_positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_heap_positions);
            @memset(source_heap_positions, null);
            return .{
                .allocator = cursor_alloc,
                .backend = self.backend,
                .mutable = self.mutable_snapshot,
                .immutable_memtables = self.immutable_memtables,
                .runs = self.runs,
                .l0_groups = self.l0_groups,
                .levels = self.levels,
                .namespace = namespace,
                .positions = positions,
                .source_entries = source_entries,
                .source_block_bytes = source_block_bytes,
                .source_block_handles = source_block_handles,
                .source_block_indices = source_block_indices,
                .advance_sources = advance_sources,
                .source_heap = source_heap,
                .source_heap_positions = source_heap_positions,
            };
        }
    };
}

fn borrowRunSnapshotList(allocator: Allocator, source: []const Run) ![]Run {
    const runs = try allocator.alloc(Run, source.len);
    var initialized: usize = 0;
    errdefer {
        for (runs[0..initialized]) |*run| {
            if (run.owns_bloom_filter) {
                if (run.bloom_filter) |*filter| filter.deinit(allocator);
            }
            if (run.table_index) |*index| index.deinit(allocator);
            if (run.state) |*state| state.deinit(allocator);
        }
        allocator.free(runs);
    }

    for (source, 0..) |run, i| {
        runs[i] = run;
        runs[i].owns_metadata = false;
        runs[i].owns_bloom_filter = false;
        runs[i].state = null;
        runs[i].bloom_filter = null;
        runs[i].table_index = null;

        if (run.path == null) {
            const state = run.state orelse return error.RunStateUnavailable;
            runs[i].state = try state.clone(allocator);
        }

        if (run.bloom_filter) |filter| {
            runs[i].bloom_filter = filter;
        }
        initialized = i + 1;
    }
    return runs;
}

fn freeRunSnapshotList(allocator: Allocator, runs: []Run) void {
    for (runs) |*run| {
        if (run.owns_bloom_filter) {
            if (run.bloom_filter) |*filter| filter.deinit(allocator);
        }
        if (run.table_index) |*index| index.deinit(allocator);
        if (run.state) |*state| state.deinit(allocator);
    }
    allocator.free(runs);
}

fn deinitRunGroups(allocator: Allocator, groups: []RunGroup) void {
    for (groups) |*group| group.deinit(allocator);
    allocator.free(groups);
}

fn getFromSnapshotRuns(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    last_l0_group_index: *?usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    held_values: *std.ArrayListUnmanaged([]u8),
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    batch_run_indexes: ?*RunBatchIndexHandles,
) ![]const u8 {
    if (mutable.findIndex(namespace, key)) |idx| {
        const entry = mutable.entries.items[idx];
        if (entry.tombstone) return error.NotFound;
        read_hint.* = null;
        backend.recordMutableHit();
        return entry.value;
    }
    for (immutable_memtables) |state| {
        if (state.findIndex(namespace, key)) |idx| {
            const entry = state.entries.items[idx];
            if (entry.tombstone) return error.NotFound;
            read_hint.* = null;
            backend.recordMutableHit();
            return entry.value;
        }
    }
    var candidate_group_index = if (last_l0_group_index.*) |hinted_index|
        if (hinted_index < l0_groups.len and groupMayContain(l0_groups[hinted_index], namespace, key)) hinted_index else null
    else
        null;
    if (candidate_group_index == null) {
        candidate_group_index = findRunGroupIndex(l0_groups, namespace, key);
    }
    last_l0_group_index.* = candidate_group_index;
    if (candidate_group_index) |group_index| {
        if (try getFromRunIndices(backend, runs, l0_groups[group_index].run_indices, read_hint, held_blocks, held_values, value_allocator, namespace, key, backend_locked, batch_run_indexes)) |value| {
            backend.recordL0Hit();
            return value;
        }
    }
    for (levels) |level| {
        const run_index = findRunIndexInLevel(runs, level, namespace, key) orelse continue;
        const one = [_]usize{run_index};
        if (try getFromRunIndices(backend, runs, &one, read_hint, held_blocks, held_values, value_allocator, namespace, key, backend_locked, batch_run_indexes)) |value| {
            backend.recordLevelHit();
            return value;
        }
    }
    read_hint.* = null;
    return error.NotFound;
}

fn buildL0RunGroups(allocator: Allocator, runs: []const Run) ![]RunGroup {
    var l0_count: usize = 0;
    while (l0_count < runs.len and runs[l0_count].level == 0) : (l0_count += 1) {}
    return try buildRunGroups(allocator, runs[0..l0_count]);
}

fn buildL0RunGroupsWithStats(backend: anytype, allocator: Allocator, runs: []const Run) ![]RunGroup {
    const BackendType = @TypeOf(backend.*);
    const start_ns = if (@hasDecl(BackendType, "readStatsNowNs")) backend.readStatsNowNs() else 0;
    var l0_count: usize = 0;
    while (l0_count < runs.len and runs[l0_count].level == 0) : (l0_count += 1) {}
    const groups = try buildRunGroups(allocator, runs[0..l0_count]);
    if (@hasDecl(BackendType, "recordRunGroupBuild")) {
        const elapsed_ns = if (@hasDecl(BackendType, "readStatsElapsedNs")) backend.readStatsElapsedNs(start_ns) else 0;
        backend.recordRunGroupBuild(runs.len, l0_count, elapsed_ns);
    }
    return groups;
}

fn buildLowerLevels(allocator: Allocator, runs: []const Run) ![]RunLevel {
    var start: usize = 0;
    while (start < runs.len and runs[start].level == 0) : (start += 1) {}
    if (start >= runs.len) return try allocator.alloc(RunLevel, 0);

    var levels = std.ArrayListUnmanaged(RunLevel).empty;
    errdefer levels.deinit(allocator);

    var i = start;
    while (i < runs.len) {
        const level = runs[i].level;
        const level_start = i;
        while (i < runs.len and runs[i].level == level) : (i += 1) {}
        try levels.append(allocator, .{
            .level = level,
            .start_index = level_start,
            .len = i - level_start,
        });
    }
    return try levels.toOwnedSlice(allocator);
}

fn buildRunGroups(allocator: Allocator, runs: []const Run) ![]RunGroup {
    const IndexedRun = struct {
        run_index: usize,
        smallest_namespace_name: ?[]const u8,
        smallest_key: []const u8,
        largest_namespace_name: ?[]const u8,
        largest_key: []const u8,
    };

    if (runs.len == 0) return try allocator.alloc(RunGroup, 0);

    var indexed = try allocator.alloc(IndexedRun, runs.len);
    defer allocator.free(indexed);
    for (runs, 0..) |run, i| {
        indexed[i] = .{
            .run_index = i,
            .smallest_namespace_name = run.smallest_namespace_name,
            .smallest_key = run.smallest_key,
            .largest_namespace_name = run.largest_namespace_name,
            .largest_key = run.largest_key,
        };
    }
    std.mem.sort(IndexedRun, indexed, {}, struct {
        fn lessThan(_: void, lhs: IndexedRun, rhs: IndexedRun) bool {
            return compareRunBound(lhs.smallest_namespace_name, lhs.smallest_key, rhs.smallest_namespace_name, rhs.smallest_key) == .lt;
        }
    }.lessThan);

    var groups = std.ArrayListUnmanaged(RunGroup).empty;
    errdefer {
        for (groups.items) |*group| group.deinit(allocator);
        groups.deinit(allocator);
    }

    var current = std.ArrayListUnmanaged(usize).empty;
    defer current.deinit(allocator);

    var group_smallest_namespace_name = indexed[0].smallest_namespace_name;
    var group_smallest_key = indexed[0].smallest_key;
    var group_largest_namespace_name = indexed[0].largest_namespace_name;
    var group_largest_key = indexed[0].largest_key;
    try current.append(allocator, indexed[0].run_index);

    for (indexed[1..]) |run| {
        if (!rangesOverlap(
            run.smallest_namespace_name,
            run.smallest_key,
            run.largest_namespace_name,
            run.largest_key,
            group_smallest_namespace_name,
            group_smallest_key,
            group_largest_namespace_name,
            group_largest_key,
        )) {
            std.mem.sort(usize, current.items, {}, std.sort.asc(usize));
            try groups.append(allocator, .{
                .smallest_namespace_name = group_smallest_namespace_name,
                .smallest_key = group_smallest_key,
                .largest_namespace_name = group_largest_namespace_name,
                .largest_key = group_largest_key,
                .run_indices = try current.toOwnedSlice(allocator),
            });
            current = .empty;
            group_smallest_namespace_name = run.smallest_namespace_name;
            group_smallest_key = run.smallest_key;
            group_largest_namespace_name = run.largest_namespace_name;
            group_largest_key = run.largest_key;
        } else {
            if (compareRunBound(run.smallest_namespace_name, run.smallest_key, group_smallest_namespace_name, group_smallest_key) == .lt) {
                group_smallest_namespace_name = run.smallest_namespace_name;
                group_smallest_key = run.smallest_key;
            }
            if (compareRunBound(run.largest_namespace_name, run.largest_key, group_largest_namespace_name, group_largest_key) == .gt) {
                group_largest_namespace_name = run.largest_namespace_name;
                group_largest_key = run.largest_key;
            }
        }
        try current.append(allocator, run.run_index);
    }

    std.mem.sort(usize, current.items, {}, std.sort.asc(usize));
    try groups.append(allocator, .{
        .smallest_namespace_name = group_smallest_namespace_name,
        .smallest_key = group_smallest_key,
        .largest_namespace_name = group_largest_namespace_name,
        .largest_key = group_largest_key,
        .run_indices = try current.toOwnedSlice(allocator),
    });

    return try groups.toOwnedSlice(allocator);
}

fn findRunGroupIndex(groups: []const RunGroup, namespace: backend_types.Namespace, key: []const u8) ?usize {
    var lo: usize = 0;
    var hi: usize = groups.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(groups[mid].largest_namespace_name, groups[mid].largest_key, namespace.name, key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= groups.len) return null;
    if (!groupMayContain(groups[lo], namespace, key)) return null;
    return lo;
}

fn findRunGroup(groups: []const RunGroup, namespace: backend_types.Namespace, key: []const u8) ?RunGroup {
    const idx = findRunGroupIndex(groups, namespace, key) orelse return null;
    return groups[idx];
}

fn groupMayContain(group: RunGroup, namespace: backend_types.Namespace, key: []const u8) bool {
    return compareRunBound(namespace.name, key, group.smallest_namespace_name, group.smallest_key) != .lt and
        compareRunBound(namespace.name, key, group.largest_namespace_name, group.largest_key) != .gt;
}

fn findRunIndexInSortedLevel(runs: []const Run, namespace: backend_types.Namespace, key: []const u8) ?usize {
    var lo: usize = 0;
    var hi: usize = runs.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(runs[mid].largest_namespace_name, runs[mid].largest_key, namespace.name, key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= runs.len) return null;
    if (!runMayContain(runs[lo], namespace, key)) return null;
    return lo;
}

fn findRunIndexInLevel(runs: []const Run, level: RunLevel, namespace: backend_types.Namespace, key: []const u8) ?usize {
    if (level.len == 0) return null;
    const slice = runs[level.start_index .. level.start_index + level.len];
    var lo: usize = 0;
    var hi: usize = slice.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(slice[mid].largest_namespace_name, slice[mid].largest_key, namespace.name, key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= slice.len) return null;
    const run_index = level.start_index + lo;
    if (!runMayContain(runs[run_index], namespace, key)) return null;
    return run_index;
}

fn getFromRunIndices(
    backend: anytype,
    runs: []Run,
    run_indices: []const usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    held_values: *std.ArrayListUnmanaged([]u8),
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    batch_run_indexes: ?*RunBatchIndexHandles,
) !?[]const u8 {
    for (run_indices) |run_index| {
        backend.recordRunProbe();
        const run = &runs[run_index];
        if (run.state) |*state| {
            if (state.findIndex(namespace, key)) |idx| {
                const entry = state.entries.items[idx];
                if (entry.tombstone) return error.NotFound;
                read_hint.* = null;
                return entry.value;
            }
            continue;
        }

        if (run.path != null) {
            if (run.cached_state_index) |index| {
                if (backend.cachedRunStateIndexMatches(index, run.path.?, run.id)) {
                    const state = backend.getCachedRunStateByIndex(index);
                    if (state.findIndex(namespace, key)) |idx| {
                        const entry = state.entries.items[idx];
                        if (entry.tombstone) return error.NotFound;
                        read_hint.* = null;
                        return entry.value;
                    }
                    continue;
                }
            }
            const run_filter_checked = run.bloom_filter != null or run.encoded_bloom_filter != null;
            if (run_filter_checked and !try runMayContainWithFilterMaybeLocked(backend, run, namespace, key, backend_locked)) continue;
            if (backend.options.cache != null) {
                const located = if (batch_run_indexes) |indexes|
                    try getFromRunWithBlockCacheBatch(backend, run, run_index, read_hint, held_blocks, namespace, key, run_filter_checked, indexes) orelse continue
                else
                    try getFromRunWithBlockCache(backend, run, run_index, read_hint, held_blocks, namespace, key, run_filter_checked) orelse continue;
                if (located.entry.tombstone) return error.NotFound;
                read_hint.* = .{
                    .run_index = run_index,
                    .namespace_name = namespace.name,
                    .key = located.entry.key,
                    .entry_index = located.entry_index,
                };
                return located.entry.value;
            }
            if (try getFromRunWithLocalIndex(backend, run, held_values, value_allocator, namespace, key, backend_locked)) |value| {
                read_hint.* = null;
                return value;
            }
            continue;
        }

        if (!try runMayContainWithFilterMaybeLocked(backend, run, namespace, key, backend_locked)) continue;

        const table = try tableForRunMaybeLocked(backend, run, backend_locked);
        var read_hint_attempted = false;
        const positioned = if (read_hint.*) |hint|
            if (hint.run_index == run_index and
                compareNamespace(namespace, .{ .name = hint.namespace_name }) == .eq and
                std.mem.order(u8, key, hint.key) != .lt)
            blk: {
                read_hint_attempted = true;
                backend.recordReadHintAttempt();
                break :blk try table.seekAtOrAfterFromIndex(namespace.name, key, hint.entry_index);
            } else null
        else
            null;
        const located = if (positioned) |cached|
            if (compareNamespace(.{ .name = cached.entry.namespace_name }, namespace) == .eq and std.mem.eql(u8, cached.entry.key, key)) blk2: {
                backend.recordReadHintHit();
                break :blk2 .{ cached.index, cached.entry };
            } else blk2: {
                if (read_hint_attempted) backend.recordReadHintMiss();
                const idx = try table.findIndex(namespace.name, key) orelse break :blk2 null;
                break :blk2 .{ idx, try table.entryAt(idx) };
            }
        else blk2: {
            if (read_hint_attempted) backend.recordReadHintMiss();
            const idx = try table.findIndex(namespace.name, key) orelse break :blk2 null;
            break :blk2 .{ idx, try table.entryAt(idx) };
        };
        const entry_index, const entry = located orelse continue;
        if (entry.tombstone) return error.NotFound;
        read_hint.* = .{
            .run_index = run_index,
            .namespace_name = namespace.name,
            .key = entry.key,
            .entry_index = entry_index,
        };
        return entry.value;
    }
    return null;
}

const BlockLocatedEntry = struct {
    entry_index: usize,
    entry: lsm_table_file.Entry,
    handle: cache_mod.Handle,
};

const LocatedTableEntry = struct {
    entry_index: usize,
    entry: lsm_table_file.Entry,
};

const CachedPointLookupResult = union(enum) {
    hit: []const u8,
    miss,
    unavailable,
};

fn getFromStableCachedPointView(
    backend: anytype,
    allocator: Allocator,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    last_l0_group_index: *?usize,
    namespace: backend_types.Namespace,
    key: []const u8,
) !CachedPointLookupResult {
    var candidate_group_index = if (last_l0_group_index.*) |hinted_index|
        if (hinted_index < l0_groups.len and groupMayContain(l0_groups[hinted_index], namespace, key)) hinted_index else null
    else
        null;
    if (candidate_group_index == null) {
        candidate_group_index = findRunGroupIndex(l0_groups, namespace, key);
    }
    last_l0_group_index.* = candidate_group_index;
    if (candidate_group_index) |group_index| {
        switch (try getFromCachedRunStates(backend, allocator, runs, l0_groups[group_index].run_indices, namespace, key)) {
            .hit => |value| return .{ .hit = value },
            .unavailable => return .unavailable,
            .miss => {},
        }
    }

    for (levels) |level| {
        const run_index = findRunIndexInLevel(runs, level, namespace, key) orelse continue;
        const one = [_]usize{run_index};
        switch (try getFromCachedRunStates(backend, allocator, runs, &one, namespace, key)) {
            .hit => |value| return .{ .hit = value },
            .unavailable => return .unavailable,
            .miss => {},
        }
    }
    return .miss;
}

fn getFromCachedRunStates(
    backend: anytype,
    allocator: Allocator,
    runs: []Run,
    run_indices: []const usize,
    namespace: backend_types.Namespace,
    key: []const u8,
) !CachedPointLookupResult {
    for (run_indices) |run_index| {
        const run = &runs[run_index];
        if (!runMayContain(run.*, namespace, key)) continue;
        if (!lsm_table_file.maybeContains(try run.ensureBloomFilter(allocator), namespace.name, key)) {
            continue;
        }
        const state = if (run.state) |*present|
            present
        else blk: {
            const path = run.path orelse return .unavailable;
            const index = run.cached_state_index orelse return .unavailable;
            if (!backend.cachedRunStateIndexMatches(index, path, run.id)) return .unavailable;
            break :blk backend.getCachedRunStateByIndex(index);
        };
        if (state.findIndex(namespace, key)) |idx| {
            const entry = state.entries.items[idx];
            if (entry.tombstone) return error.NotFound;
            return .{ .hit = entry.value };
        }
    }
    return .miss;
}

fn parseEntryAtWithStats(backend: anytype, bytes: []const u8, relative_offset: usize) !lsm_table_file.Entry {
    const start_ns = backend.readStatsNowNs();
    const parsed = lsm_table_file.parseEntryAt(bytes, relative_offset);
    backend.recordTableEntryParse(backend.readStatsElapsedNs(start_ns));
    return try parsed;
}

fn decodeRunTableIndexWithStats(backend: anytype, allocator: Allocator, bytes: []const u8) !lsm_table_file.TableIndex {
    const start_ns = backend.readStatsNowNs();
    const decoded = lsm_table_file.decodeIndexAlloc(allocator, bytes);
    backend.recordTableIndexDecode(backend.readStatsElapsedNs(start_ns));
    return try decoded;
}

fn loadRunTableIndexWithStats(backend: anytype, allocator: Allocator, path: []const u8) !lsm_table_file.TableIndex {
    const start_ns = backend.readStatsNowNs();
    const loaded = repository_mod.loadRunTableIndexAllocWithStorage(backend.storage.?, allocator, path);
    backend.recordTableIndexLoad(backend.readStatsElapsedNs(start_ns));
    return try loaded;
}

fn loadRunTableBlockWithStats(backend: anytype, allocator: Allocator, path: []const u8, absolute_offset: u64, len: usize) ![]u8 {
    const start_ns = backend.readStatsNowNs();
    const loaded = backend.storage.?.readFileRangeAlloc(allocator, path, absolute_offset, len);
    const elapsed_ns = backend.readStatsElapsedNs(start_ns);
    if (loaded) |bytes| backend.recordTableBlockLoad(bytes.len, elapsed_ns) else |_| backend.recordTableBlockLoad(len, elapsed_ns);
    return try loaded;
}

fn loadRunTableDecodedBlockWithStats(
    backend: anytype,
    allocator: Allocator,
    path: []const u8,
    absolute_offset: u64,
    physical_len: usize,
    compression: lsm_table_file.BlockCompression,
    logical_len: usize,
) ![]u8 {
    const payload = try loadRunTableBlockWithStats(backend, allocator, path, absolute_offset, physical_len);
    defer allocator.free(payload);
    return try lsm_table_file.decodeBlockPayloadAlloc(allocator, compression, payload, logical_len);
}

fn getFromRunWithBlockCache(
    backend: anytype,
    run: *Run,
    run_index: usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    namespace: backend_types.Namespace,
    key: []const u8,
    run_filter_checked: bool,
) !?LocatedTableEntry {
    if (!runMayContain(run.*, namespace, key)) return null;

    var index_handle = try loadRunTableIndexHandle(backend, run);
    defer index_handle.release();
    return try getFromRunWithBlockCacheIndex(backend, run, run_index, index_handle.runTableIndex(), read_hint, held_blocks, namespace, key, run_filter_checked);
}

fn getFromRunWithBlockCacheBatch(
    backend: anytype,
    run: *Run,
    run_index: usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    namespace: backend_types.Namespace,
    key: []const u8,
    run_filter_checked: bool,
    batch_run_indexes: *RunBatchIndexHandles,
) !?LocatedTableEntry {
    if (!runMayContain(run.*, namespace, key)) return null;

    const state = try batch_run_indexes.state(backend, run, run_index);
    return try getFromRunWithBlockCacheBatchState(backend, run, run_index, state, read_hint, held_blocks, namespace, key, run_filter_checked);
}

fn getFromRunWithBlockCacheIndex(
    backend: anytype,
    run: *Run,
    run_index: usize,
    index: *const lsm_table_file.TableIndex,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    namespace: backend_types.Namespace,
    key: []const u8,
    run_filter_checked: bool,
) !?LocatedTableEntry {
    if (!run_filter_checked) {
        const present = lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
        if (!present) {
            backend.recordBloomNegative();
            return null;
        }
    }

    var located: ?BlockLocatedEntry = null;
    if (index.blockCount() == 0 and read_hint.* != null) {
        const hint = (read_hint.*).?;
        if (hint.run_index == run_index and
            compareNamespace(namespace, .{ .name = hint.namespace_name }) == .eq and
            std.mem.order(u8, key, hint.key) != .lt)
        {
            backend.recordReadHintAttempt();
            located = try scanForExactEntryInCachedBlocks(backend, run, index, hint.entry_index, namespace, key);
            if (located != null) {
                backend.recordReadHintHit();
            } else {
                backend.recordReadHintMiss();
            }
        }
    }
    if (located == null) {
        located = try findExactEntryInCachedBlocks(backend, run, index, namespace, key);
    }
    var pinned = located orelse return null;
    errdefer pinned.handle.release();
    try held_blocks.append(backend.allocator, pinned.handle);
    return .{
        .entry_index = pinned.entry_index,
        .entry = pinned.entry,
    };
}

fn getFromRunWithBlockCacheBatchState(
    backend: anytype,
    run: *Run,
    run_index: usize,
    state: *RunBatchIndexState,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    namespace: backend_types.Namespace,
    key: []const u8,
    run_filter_checked: bool,
) !?LocatedTableEntry {
    const index = state.handle.runTableIndex();
    if (!run_filter_checked) {
        const present = lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
        if (!present) {
            backend.recordBloomNegative();
            return null;
        }
    }

    var located: ?LocatedTableEntry = null;
    if (index.blockCount() == 0 and read_hint.* != null) {
        const hint = (read_hint.*).?;
        if (hint.run_index == run_index and
            compareNamespace(namespace, .{ .name = hint.namespace_name }) == .eq and
            std.mem.order(u8, key, hint.key) != .lt)
        {
            backend.recordReadHintAttempt();
            located = try scanForExactEntryInBatchBlocks(backend, run, index, state, held_blocks, hint.entry_index, namespace, key);
            if (located != null) {
                backend.recordReadHintHit();
            } else {
                backend.recordReadHintMiss();
            }
        }
    }
    if (located == null) {
        located = try findExactEntryInBatchBlocks(backend, run, index, state, held_blocks, namespace, key);
    }
    if (located) |entry| {
        if (!entry.entry.tombstone) state.block_has_values = true;
    }
    return located;
}

fn loadRunTableIndexHandle(backend: anytype, run: *Run) !cache_mod.Handle {
    const cache = backend.options.cache orelse return error.RunStateUnavailable;
    const path = run.path orelse return error.RunStateUnavailable;
    const generation = backend.root_generation;
    while (true) {
        if (cache.retainRunTableIndex(path, run.id, generation)) |retained| return retained;
        try cache.beginLoad(path, run.id, generation, .run_table_index);
        defer cache.finishLoad(path, run.id, generation, .run_table_index);
        if (cache.retainRunTableIndex(path, run.id, generation)) |retained| return retained;
        if (cache.retainRunTableRaw(path, run.id, generation)) |retained_raw| {
            var raw_handle = retained_raw;
            defer raw_handle.release();
            const index = try decodeRunTableIndexWithStats(backend, cache.valueAllocator(), raw_handle.runTableRaw());
            return try cache.putRunTableIndex(path, run.id, generation, index);
        }
        const index = try loadRunTableIndexWithStats(backend, cache.valueAllocator(), path);
        return try cache.putRunTableIndex(path, run.id, generation, index);
    }
}

fn loadRunTableBlockHandle(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
) !cache_mod.Handle {
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    return try loadRunTableBlockHandleAtOffset(backend, run, absolute_offset, window.physicalLen(), window.compression, window.len);
}

fn loadRunTableBlockHandleAtOffset(
    backend: anytype,
    run: *Run,
    absolute_offset: u64,
    physical_len: u32,
    compression: lsm_table_file.BlockCompression,
    logical_len: u32,
) !cache_mod.Handle {
    const cache = backend.options.cache orelse return error.RunStateUnavailable;
    const path = run.path orelse return error.RunStateUnavailable;
    const generation = backend.root_generation;
    while (true) {
        if (cache.retainRunTableBlock(path, run.id, generation, absolute_offset, physical_len)) |retained| {
            backend.recordSharedBlockCacheHit();
            return retained;
        }
        try cache.beginLoadWithBlock(path, run.id, generation, .run_table_block, absolute_offset, physical_len);
        defer cache.finishLoadWithBlock(path, run.id, generation, .run_table_block, absolute_offset, physical_len);
        if (cache.retainRunTableBlock(path, run.id, generation, absolute_offset, physical_len)) |retained| {
            backend.recordSharedBlockCacheHit();
            return retained;
        }
        backend.recordSharedBlockCacheMiss();
        const block = try loadRunTableDecodedBlockWithStats(backend, cache.valueAllocator(), path, absolute_offset, physical_len, compression, logical_len);
        return try cache.putRunTableBlock(path, run.id, generation, absolute_offset, physical_len, block);
    }
}

fn loadEntryFromCachedBlock(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    entry_index: usize,
) !BlockLocatedEntry {
    const window = index.entryDataWindow(entry_index, cache_mod.DefaultTableBlockSize);
    var handle = try loadRunTableBlockHandle(backend, run, index, window);
    errdefer handle.release();
    const relative_offset: usize = @intCast(index.entryStart(entry_index) - window.relative_offset);
    const entry = try parseEntryAtWithStats(backend, handle.runTableBlock(), relative_offset);
    return .{
        .entry_index = entry_index,
        .entry = entry,
        .handle = handle,
    };
}

fn findExactEntryInCachedBlocks(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?BlockLocatedEntry {
    if (index.findBlockIndex(namespace.name, key)) |block_index| {
        const block = index.blocks[block_index];
        if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
        if (!block.maybeContains(namespace.name, key)) {
            backend.recordBloomNegative();
            return null;
        }
        const window = index.blockWindow(block_index);
        var handle = try loadRunTableBlockHandle(backend, run, index, window);
        errdefer handle.release();
        const positioned = try lsm_table_file.findExactEntryInBlock(
            index,
            handle.runTableBlock(),
            block_index,
            namespace.name,
            key,
        ) orelse return null;
        return .{
            .entry_index = positioned.index,
            .entry = positioned.entry,
            .handle = handle,
        };
    }

    var lo: usize = 0;
    var hi: usize = index.entryCount();
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        var loaded = try loadEntryFromCachedBlock(backend, run, index, mid);
        defer loaded.handle.release();
        const ord = compareTableEntryTo(loaded.entry, namespace, key);
        if (ord == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= index.entryCount()) return null;
    var loaded = try loadEntryFromCachedBlock(backend, run, index, lo);
    const ord = compareTableEntryTo(loaded.entry, namespace, key);
    if (ord != .eq) {
        loaded.handle.release();
        return null;
    }
    return loaded;
}

fn loadBatchBlock(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    state: *RunBatchIndexState,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    block_index: usize,
) ![]const u8 {
    if (state.block_index == null or state.block_index.? != block_index) {
        try state.transferBlock(backend.allocator, held_blocks);
        const window = index.blockWindow(block_index);
        state.block_handle = try loadRunTableBlockHandle(backend, run, index, window);
        state.block_index = block_index;
        state.block_has_values = false;
    }
    return state.block_handle.?.runTableBlock();
}

fn loadEntryFromBatchBlock(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    state: *RunBatchIndexState,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    entry_index: usize,
) !LocatedTableEntry {
    const block_index = index.findBlockIndexForEntry(entry_index) orelse {
        var loaded = try loadEntryFromCachedBlock(backend, run, index, entry_index);
        errdefer loaded.handle.release();
        try held_blocks.append(backend.allocator, loaded.handle);
        return .{
            .entry_index = loaded.entry_index,
            .entry = loaded.entry,
        };
    };
    const block_bytes = try loadBatchBlock(backend, run, index, state, held_blocks, block_index);
    const relative_offset: usize = @intCast(index.entryStart(entry_index) - index.blockWindow(block_index).relative_offset);
    return .{
        .entry_index = entry_index,
        .entry = try parseEntryAtWithStats(backend, block_bytes, relative_offset),
    };
}

fn findExactEntryInBatchBlocks(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    state: *RunBatchIndexState,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    namespace: backend_types.Namespace,
    key: []const u8,
) !?LocatedTableEntry {
    if (index.findBlockIndex(namespace.name, key)) |block_index| {
        const block = index.blocks[block_index];
        if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
        if (!block.maybeContains(namespace.name, key)) {
            backend.recordBloomNegative();
            return null;
        }
        const block_bytes = try loadBatchBlock(backend, run, index, state, held_blocks, block_index);
        const positioned = try lsm_table_file.findExactEntryInBlock(
            index,
            block_bytes,
            block_index,
            namespace.name,
            key,
        ) orelse return null;
        return .{
            .entry_index = positioned.index,
            .entry = positioned.entry,
        };
    }

    var lo: usize = 0;
    var hi: usize = index.entryCount();
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const loaded = try loadEntryFromBatchBlock(backend, run, index, state, held_blocks, mid);
        const ord = compareTableEntryTo(loaded.entry, namespace, key);
        if (ord == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= index.entryCount()) return null;
    const loaded = try loadEntryFromBatchBlock(backend, run, index, state, held_blocks, lo);
    const ord = compareTableEntryTo(loaded.entry, namespace, key);
    if (ord != .eq) return null;
    return loaded;
}

fn scanForExactEntryInCachedBlocks(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    start_index: usize,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?BlockLocatedEntry {
    var idx = start_index;
    while (idx < index.entryCount()) : (idx += 1) {
        var loaded = try loadEntryFromCachedBlock(backend, run, index, idx);
        const order = compareTableEntryTo(loaded.entry, namespace, key);
        if (order == .lt) {
            loaded.handle.release();
            continue;
        }
        if (compareNamespace(.{ .name = loaded.entry.namespace_name }, namespace) != .eq or order != .eq) {
            loaded.handle.release();
            return null;
        }
        return loaded;
    }
    return null;
}

fn scanForExactEntryInBatchBlocks(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    state: *RunBatchIndexState,
    held_blocks: *std.ArrayListUnmanaged(cache_mod.Handle),
    start_index: usize,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?LocatedTableEntry {
    var idx = start_index;
    while (idx < index.entryCount()) : (idx += 1) {
        const loaded = try loadEntryFromBatchBlock(backend, run, index, state, held_blocks, idx);
        const order = compareTableEntryTo(loaded.entry, namespace, key);
        if (order == .lt) continue;
        if (compareNamespace(.{ .name = loaded.entry.namespace_name }, namespace) != .eq or order != .eq) return null;
        return loaded;
    }
    return null;
}

fn visibleEntryFromRunIndices(
    backend: anytype,
    runs: []Run,
    run_indices: []const usize,
    namespace: backend_types.Namespace,
    key: []const u8,
    visible_entry_bytes: *?[]u8,
    backend_locked: bool,
) !?backend_adapter.Entry {
    for (run_indices) |run_index| {
        const run = &runs[run_index];
        if (!try runMayContainWithFilterMaybeLocked(backend, run, namespace, key, backend_locked)) continue;
        if (run.state) |*state| {
            if (state.findIndex(namespace, key)) |idx| {
                const entry = state.entries.items[idx];
                if (entry.tombstone) return null;
                return entry.entry();
            }
            continue;
        }

        if (run.path != null) {
            const loaded = try loadVisibleEntryFromPathRunMaybeLocked(backend, run, namespace, key, backend_locked) orelse continue;
            if (loaded.entry.tombstone) {
                backend.allocator.free(loaded.bytes);
                return null;
            }
            visible_entry_bytes.* = loaded.bytes;
            return .{
                .key = loaded.entry.key,
                .value = loaded.entry.value,
            };
        }
    }
    return null;
}

fn rangesOverlap(
    lhs_smallest_namespace_name: ?[]const u8,
    lhs_smallest_key: []const u8,
    lhs_largest_namespace_name: ?[]const u8,
    lhs_largest_key: []const u8,
    rhs_smallest_namespace_name: ?[]const u8,
    rhs_smallest_key: []const u8,
    rhs_largest_namespace_name: ?[]const u8,
    rhs_largest_key: []const u8,
) bool {
    return compareRunBound(lhs_smallest_namespace_name, lhs_smallest_key, rhs_largest_namespace_name, rhs_largest_key) != .gt and
        compareRunBound(lhs_largest_namespace_name, lhs_largest_key, rhs_smallest_namespace_name, rhs_smallest_key) != .lt;
}

fn stateForRun(backend: anytype, run: *Run) !*const State {
    if (run.state) |*state| return state;
    const locked = lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    const path = run.path orelse return error.RunStateUnavailable;
    if (run.cached_state_index) |index| {
        if (!(index < backend.run_state_cache.items.len and
            backend.run_state_cache.items[index].run_id == run.id and
            std.mem.eql(u8, backend.run_state_cache.items[index].path, path)))
        {
            run.cached_state_index = null;
        }
    }
    if (run.cached_state_index == null) {
        run.cached_state_index = try backend.getCachedRunStateIndex(path, run.id);
    }
    return backend.getCachedRunStateByIndex(run.cached_state_index.?);
}

fn tableForRunLocked(backend: anytype, run: *Run) !*const lsm_table_file.BorrowedDecoded {
    const path = run.path orelse return error.RunStateUnavailable;
    if (run.cached_table_index) |index| {
        if (!(index < backend.run_table_cache.items.len and
            backend.run_table_cache.items[index].run_id == run.id and
            std.mem.eql(u8, backend.run_table_cache.items[index].path, path)))
        {
            run.cached_table_index = null;
        }
    }
    if (run.cached_table_index == null) {
        run.cached_table_index = try backend.getCachedRunTableIndex(path, run.id);
    }
    return backend.getCachedRunTableByIndex(run.cached_table_index.?);
}

fn tableForRun(backend: anytype, run: *Run) !*const lsm_table_file.BorrowedDecoded {
    const locked = lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    return try tableForRunLocked(backend, run);
}

fn tableForRunMaybeLocked(backend: anytype, run: *Run, backend_locked: bool) !*const lsm_table_file.BorrowedDecoded {
    if (backend_locked) return try tableForRunLocked(backend, run);
    return try tableForRun(backend, run);
}

fn indexForRunNoCacheLocked(backend: anytype, run: *Run) !*const lsm_table_file.TableIndex {
    const path = run.path orelse return error.RunStateUnavailable;
    if (run.cached_index_index) |index| {
        if (!(index < backend.run_index_cache.items.len and
            backend.run_index_cache.items[index].run_id == run.id and
            std.mem.eql(u8, backend.run_index_cache.items[index].path, path)))
        {
            run.cached_index_index = null;
        }
    }
    if (run.cached_index_index == null) {
        run.cached_index_index = try backend.getCachedRunIndexIndex(path, run.id);
    }
    return backend.getCachedRunIndexByIndex(run.cached_index_index.?);
}

fn indexForRunNoCache(backend: anytype, run: *Run) !*const lsm_table_file.TableIndex {
    const locked = lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    return try indexForRunNoCacheLocked(backend, run);
}

fn indexForRunNoCacheMaybeLocked(backend: anytype, run: *Run, backend_locked: bool) !*const lsm_table_file.TableIndex {
    if (backend_locked) return try indexForRunNoCacheLocked(backend, run);
    return try indexForRunNoCache(backend, run);
}

const OwnedTableEntry = struct {
    entry: lsm_table_file.Entry,
    bytes: []u8,
};

fn findExactEntryWithLocalIndex(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?OwnedTableEntry {
    const index = try indexForRunNoCache(backend, run);
    if (index.blockCount() > 0) {
        return try findExactEntryWithLocalIndexBlockMeta(backend, run, index, namespace, key);
    }

    var lo: usize = 0;
    var hi: usize = index.entryCount();
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const loaded = try loadOwnedEntryFromStorageWindow(backend, run, index, mid);
        const ord = compareTableEntryTo(loaded.entry, namespace, key);
        backend.allocator.free(loaded.bytes);
        if (ord == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if (lo >= index.entryCount()) return null;
    const loaded = try loadOwnedEntryFromStorageWindow(backend, run, index, lo);
    errdefer backend.allocator.free(loaded.bytes);
    if (compareTableEntryTo(loaded.entry, namespace, key) != .eq) {
        backend.allocator.free(loaded.bytes);
        return null;
    }
    return loaded;
}

fn loadOwnedEntryFromStorageWindow(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    entry_index: usize,
) !OwnedTableEntry {
    const window = index.entryDataWindow(entry_index, cache_mod.DefaultTableBlockSize);
    const relative_offset: usize = @intCast(index.entryStart(entry_index) - window.relative_offset);
    const bytes = try loadOwnedBlockForWindow(
        backend,
        run,
        index,
        window,
    );
    errdefer backend.allocator.free(bytes);
    const entry = try parseEntryAtWithStats(backend, bytes, relative_offset);

    return .{
        .entry = entry,
        .bytes = bytes,
    };
}

fn loadOwnedEntryFromStorageWindowMaybeLocked(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    entry_index: usize,
    backend_locked: bool,
) !OwnedTableEntry {
    if (!backend_locked) return try loadOwnedEntryFromStorageWindow(backend, run, index, entry_index);

    const window = index.entryDataWindow(entry_index, cache_mod.DefaultTableBlockSize);
    const relative_offset: usize = @intCast(index.entryStart(entry_index) - window.relative_offset);
    const bytes = try loadOwnedBlockForWindowAllocMaybeLocked(
        backend,
        backend.allocator,
        run,
        index,
        window,
        true,
    );
    errdefer backend.allocator.free(bytes);
    const entry = try parseEntryAtWithStats(backend, bytes, relative_offset);

    return .{
        .entry = entry,
        .bytes = bytes,
    };
}

fn loadOwnedBlockForWindowAlloc(
    backend: anytype,
    allocator: Allocator,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
) ![]u8 {
    const path = run.path orelse return error.RunStateUnavailable;
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const physical_len = window.physicalLen();
    if (backend.options.cache != null) {
        var block_handle = try loadRunTableBlockHandleAtOffset(backend, run, absolute_offset, physical_len, window.compression, window.len);
        defer block_handle.release();
        return try allocator.dupe(u8, block_handle.runTableBlock());
    }

    {
        const locked = lockBackend(@TypeOf(backend.*), backend);
        defer unlockBackend(@TypeOf(backend.*), backend, locked);
        if (@hasField(@TypeOf(backend.*), "run_block_cache") and localBlockCacheEnabled(backend)) {
            if (backend.getCachedRunBlock(path, run.id, absolute_offset, physical_len)) |cached_bytes| {
                backend.recordLocalBlockCacheHit();
                return try allocator.dupe(u8, cached_bytes);
            }
        }
    }
    if (localBlockCacheEnabled(backend)) {
        backend.recordLocalBlockCacheMiss();
    }

    const bytes = try loadRunTableDecodedBlockWithStats(backend, allocator, path, absolute_offset, physical_len, window.compression, window.len);
    errdefer allocator.free(bytes);
    {
        const locked = lockBackend(@TypeOf(backend.*), backend);
        defer unlockBackend(@TypeOf(backend.*), backend, locked);
        if (@hasField(@TypeOf(backend.*), "run_block_cache") and localBlockCacheEnabled(backend)) {
            _ = try backend.putCachedRunBlock(path, run.id, absolute_offset, physical_len, try backend.allocator.dupe(u8, bytes));
        }
    }
    return bytes;
}

fn loadOwnedBlockForWindowAllocMaybeLocked(
    backend: anytype,
    allocator: Allocator,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
    backend_locked: bool,
) ![]u8 {
    if (!backend_locked) return try loadOwnedBlockForWindowAlloc(backend, allocator, run, index, window);

    const path = run.path orelse return error.RunStateUnavailable;
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const physical_len = window.physicalLen();
    if (backend.options.cache != null) {
        var block_handle = try loadRunTableBlockHandleAtOffset(backend, run, absolute_offset, physical_len, window.compression, window.len);
        defer block_handle.release();
        return try allocator.dupe(u8, block_handle.runTableBlock());
    }

    if (@hasField(@TypeOf(backend.*), "run_block_cache") and localBlockCacheEnabled(backend)) {
        if (backend.getCachedRunBlock(path, run.id, absolute_offset, physical_len)) |cached_bytes| {
            backend.recordLocalBlockCacheHit();
            return try allocator.dupe(u8, cached_bytes);
        }
    }
    if (localBlockCacheEnabled(backend)) {
        backend.recordLocalBlockCacheMiss();
    }

    const bytes = try loadRunTableDecodedBlockWithStats(backend, allocator, path, absolute_offset, physical_len, window.compression, window.len);
    errdefer allocator.free(bytes);
    if (@hasField(@TypeOf(backend.*), "run_block_cache") and localBlockCacheEnabled(backend)) {
        _ = try backend.putCachedRunBlock(path, run.id, absolute_offset, physical_len, try backend.allocator.dupe(u8, bytes));
    }
    return bytes;
}

fn loadOwnedBlockForWindow(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
) ![]u8 {
    return try loadOwnedBlockForWindowAlloc(backend, backend.allocator, run, index, window);
}

fn loadOwnedBlockForWindowMaybeLocked(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
    backend_locked: bool,
) ![]u8 {
    return try loadOwnedBlockForWindowAllocMaybeLocked(backend, backend.allocator, run, index, window, backend_locked);
}

fn findExactEntryWithLocalIndexBlockMeta(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?OwnedTableEntry {
    const block_index = index.findBlockIndex(namespace.name, key) orelse return null;
    const block = index.blocks[block_index];
    if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
    if (!block.maybeContains(namespace.name, key)) {
        backend.recordBloomNegative();
        return null;
    }
    const window = index.blockWindow(block_index);
    const bytes = try loadOwnedBlockForWindow(
        backend,
        run,
        index,
        window,
    );
    errdefer backend.allocator.free(bytes);
    const positioned = try lsm_table_file.findExactEntryInBlock(
        index,
        bytes,
        block_index,
        namespace.name,
        key,
    ) orelse {
        backend.allocator.free(bytes);
        return null;
    };
    return .{
        .entry = positioned.entry,
        .bytes = bytes,
    };
}

fn findExactEntryWithLocalIndexBlockMetaMaybeLocked(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !?OwnedTableEntry {
    if (!backend_locked) return try findExactEntryWithLocalIndexBlockMeta(backend, run, index, namespace, key);

    const block_index = index.findBlockIndex(namespace.name, key) orelse return null;
    const block = index.blocks[block_index];
    if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
    if (!block.maybeContains(namespace.name, key)) {
        backend.recordBloomNegative();
        return null;
    }
    const window = index.blockWindow(block_index);
    const bytes = try loadOwnedBlockForWindowMaybeLocked(
        backend,
        run,
        index,
        window,
        true,
    );
    errdefer backend.allocator.free(bytes);
    const positioned = try lsm_table_file.findExactEntryInBlock(
        index,
        bytes,
        block_index,
        namespace.name,
        key,
    ) orelse {
        backend.allocator.free(bytes);
        return null;
    };
    return .{
        .entry = positioned.entry,
        .bytes = bytes,
    };
}

fn loadVisibleEntryFromPathRun(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?OwnedTableEntry {
    return try findExactEntryWithLocalIndex(backend, run, namespace, key);
}

fn findExactEntryWithLocalIndexMaybeLocked(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !?OwnedTableEntry {
    if (!backend_locked) return try findExactEntryWithLocalIndex(backend, run, namespace, key);

    const index = try indexForRunNoCacheMaybeLocked(backend, run, true);
    if (index.blockCount() > 0) {
        return try findExactEntryWithLocalIndexBlockMetaMaybeLocked(backend, run, index, namespace, key, true);
    }

    var lo: usize = 0;
    var hi: usize = index.entryCount();
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const loaded = try loadOwnedEntryFromStorageWindowMaybeLocked(backend, run, index, mid, true);
        const ord = compareTableEntryTo(loaded.entry, namespace, key);
        backend.allocator.free(loaded.bytes);
        if (ord == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }

    if (lo >= index.entryCount()) return null;
    const loaded = try loadOwnedEntryFromStorageWindowMaybeLocked(backend, run, index, lo, true);
    errdefer backend.allocator.free(loaded.bytes);
    if (compareTableEntryTo(loaded.entry, namespace, key) != .eq) {
        backend.allocator.free(loaded.bytes);
        return null;
    }
    return loaded;
}

fn loadVisibleEntryFromPathRunMaybeLocked(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !?OwnedTableEntry {
    return try findExactEntryWithLocalIndexMaybeLocked(backend, run, namespace, key, backend_locked);
}

fn getFromRunWithLocalIndex(
    backend: anytype,
    run: *Run,
    held_values: *std.ArrayListUnmanaged([]u8),
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !?[]const u8 {
    const loaded = try findExactEntryWithLocalIndexMaybeLocked(backend, run, namespace, key, backend_locked) orelse return null;
    defer backend.allocator.free(loaded.bytes);
    if (loaded.entry.tombstone) return error.NotFound;

    const owned_value = try value_allocator.dupe(u8, loaded.entry.value);
    errdefer value_allocator.free(owned_value);
    try held_values.append(value_allocator, owned_value);
    return owned_value;
}

fn runMayContainWithFilter(backend: anytype, run: *Run, namespace: backend_types.Namespace, key: []const u8) !bool {
    if (!runMayContain(run.*, namespace, key)) return false;
    const filter = try ensureRunBloomFilterForRead(backend, run);
    if (filter) |present_filter| {
        const present = lsm_table_file.maybeContains(present_filter, namespace.name, key);
        if (!present) backend.recordBloomNegative();
        return present;
    }
    if (run.path != null) {
        const present = if (backend.options.cache != null) blk: {
            var handle = try loadRunTableIndexHandle(backend, run);
            defer handle.release();
            break :blk lsm_table_file.maybeContains(handle.runTableIndex().borrowFilter(), namespace.name, key);
        } else blk: {
            const index = try indexForRunNoCache(backend, run);
            break :blk lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
        };
        if (!present) backend.recordBloomNegative();
        return present;
    }
    return true;
}

fn runMayContainWithFilterMaybeLocked(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !bool {
    if (!backend_locked) return try runMayContainWithFilter(backend, run, namespace, key);

    if (!runMayContain(run.*, namespace, key)) return false;
    const filter = try ensureRunBloomFilterForReadMaybeLocked(backend, run, true);
    if (filter) |present_filter| {
        const present = lsm_table_file.maybeContains(present_filter, namespace.name, key);
        if (!present) backend.recordBloomNegative();
        return present;
    }
    if (run.path != null) {
        const present = if (backend.options.cache != null) blk: {
            var handle = try loadRunTableIndexHandle(backend, run);
            defer handle.release();
            break :blk lsm_table_file.maybeContains(handle.runTableIndex().borrowFilter(), namespace.name, key);
        } else blk: {
            const index = try indexForRunNoCacheMaybeLocked(backend, run, true);
            break :blk lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
        };
        if (!present) backend.recordBloomNegative();
        return present;
    }
    return true;
}

fn ensureRunBloomFilterForRead(backend: anytype, run: *Run) !?bloom.OwnedFilter {
    if (run.bloom_filter) |filter| return filter;
    if (run.encoded_bloom_filter == null) return null;

    const locked = lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);

    if (@hasField(@TypeOf(backend.*), "runs")) {
        for (backend.runs.items) |*source_run| {
            if (source_run.id != run.id) continue;
            if (!sameRunPath(source_run.path, run.path)) continue;

            const filter = try source_run.ensureBloomFilter(backend.allocator);
            if (source_run != run) {
                run.bloom_filter = filter;
                run.owns_bloom_filter = false;
            }
            return filter;
        }
    }

    return try run.ensureBloomFilter(backend.allocator);
}

fn ensureRunBloomFilterForReadMaybeLocked(backend: anytype, run: *Run, backend_locked: bool) !?bloom.OwnedFilter {
    if (!backend_locked) return try ensureRunBloomFilterForRead(backend, run);

    if (run.bloom_filter) |filter| return filter;
    if (run.encoded_bloom_filter == null) return null;

    if (@hasField(@TypeOf(backend.*), "runs")) {
        for (backend.runs.items) |*source_run| {
            if (source_run.id != run.id) continue;
            if (!sameRunPath(source_run.path, run.path)) continue;

            const filter = try source_run.ensureBloomFilter(backend.allocator);
            if (source_run != run) {
                run.bloom_filter = filter;
                run.owns_bloom_filter = false;
            }
            return filter;
        }
    }

    return try run.ensureBloomFilter(backend.allocator);
}

fn sameRunPath(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null and rhs == null) return true;
    if (lhs == null or rhs == null) return false;
    return std.mem.eql(u8, lhs.?, rhs.?);
}

fn runMayContain(run: Run, namespace: backend_types.Namespace, key: []const u8) bool {
    return compareRunBound(namespace.name, key, run.smallest_namespace_name, run.smallest_key) != .lt and
        compareRunBound(namespace.name, key, run.largest_namespace_name, run.largest_key) != .gt;
}

fn runMayContainAtOrAfter(run: Run, namespace: backend_types.Namespace, key: []const u8) bool {
    if (compareNamespace(namespace, .{ .name = run.smallest_namespace_name }) == .lt) {
        return compareNamespace(namespace, .{ .name = run.largest_namespace_name }) != .gt;
    }
    return compareRunBound(namespace.name, key, run.largest_namespace_name, run.largest_key) != .gt;
}

fn compareRunBound(lhs_namespace_name: ?[]const u8, lhs_key: []const u8, rhs_namespace_name: ?[]const u8, rhs_key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(.{ .name = lhs_namespace_name }, .{ .name = rhs_namespace_name });
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, lhs_key, rhs_key);
}

fn nextStateKey(state: *const State, namespace: backend_types.Namespace, target: []const u8, inclusive: bool) ?[]const u8 {
    var idx = state.lowerBound(namespace, target);
    while (idx < state.entries.items.len) : (idx += 1) {
        const entry = state.entries.items[idx];
        if (compareNamespace(namespaceOf(entry), namespace) != .eq) return null;
        if (!inclusive and std.mem.eql(u8, entry.key, target)) continue;
        return entry.key;
    }
    return null;
}

fn nextStateIndex(state: anytype, namespace: backend_types.Namespace, target: []const u8, inclusive: bool) ?usize {
    const StateType = @TypeOf(state.*);
    if (StateType == ActiveMemTable) {
        var best: ?usize = null;
        for (state.entries.items, 0..) |entry, idx| {
            if (compareNamespace(namespaceOf(entry), namespace) != .eq) continue;
            switch (std.mem.order(u8, entry.key, target)) {
                .lt => continue,
                .eq => if (!inclusive) continue,
                .gt => {},
            }
            if (best == null or std.mem.order(u8, entry.key, state.entries.items[best.?].key) == .lt) {
                best = idx;
            }
        }
        return best;
    }

    var idx = state.lowerBound(namespace, target);
    while (idx < state.entries.items.len) : (idx += 1) {
        const entry = state.entries.items[idx];
        if (compareNamespace(namespaceOf(entry), namespace) != .eq) return null;
        if (!inclusive and std.mem.eql(u8, entry.key, target)) continue;
        return idx;
    }
    return null;
}

fn nextIndexFrom(state: anytype, namespace: backend_types.Namespace, current: usize) ?usize {
    const StateType = @TypeOf(state.*);
    if (StateType == ActiveMemTable) {
        if (current >= state.entries.items.len) return null;
        const current_entry = state.entries.items[current];
        var best: ?usize = null;
        for (state.entries.items, 0..) |entry, idx| {
            if (idx == current) continue;
            if (compareNamespace(namespaceOf(entry), namespace) != .eq) continue;
            if (std.mem.order(u8, entry.key, current_entry.key) != .gt) continue;
            if (best == null or std.mem.order(u8, entry.key, state.entries.items[best.?].key) == .lt) {
                best = idx;
            }
        }
        return best;
    }

    var idx = current + 1;
    while (idx < state.entries.items.len) : (idx += 1) {
        if (compareNamespace(namespaceOf(state.entries.items[idx]), namespace) == .eq) return idx;
        if (compareNamespace(namespaceOf(state.entries.items[idx]), namespace) == .gt) return null;
    }
    return null;
}

fn prevStateKey(state: anytype, namespace: backend_types.Namespace, target: []const u8, inclusive: bool) ?[]const u8 {
    const StateType = @TypeOf(state.*);
    if (StateType == ActiveMemTable) {
        var best: ?[]const u8 = null;
        for (state.entries.items) |entry| {
            if (compareNamespace(namespaceOf(entry), namespace) != .eq) continue;
            switch (std.mem.order(u8, entry.key, target)) {
                .gt => continue,
                .eq => if (!inclusive) continue,
                .lt => {},
            }
            if (best == null or std.mem.order(u8, entry.key, best.?) == .gt) {
                best = entry.key;
            }
        }
        return best;
    }

    const idx = state.lowerBound(namespace, target);
    var probe: usize = if (idx < state.entries.items.len and inclusive and compareEntryTo(state.entries.items[idx], namespace, target) == .eq)
        idx
    else if (idx > 0)
        idx - 1
    else
        return null;

    while (true) {
        const entry = state.entries.items[probe];
        if (compareNamespace(namespaceOf(entry), namespace) == .eq) {
            if (inclusive or !std.mem.eql(u8, entry.key, target)) return entry.key;
        } else if (compareNamespace(namespaceOf(entry), namespace) == .lt) {
            return null;
        }
        if (probe == 0) break;
        probe -= 1;
    }
    return null;
}

fn mutableLastKey(state: anytype, namespace: backend_types.Namespace) ?[]const u8 {
    if (state.entries.items.len == 0) return null;
    var idx = state.entries.items.len;
    while (idx > 0) {
        idx -= 1;
        if (compareNamespace(namespaceOf(state.entries.items[idx]), namespace) == .eq) {
            return state.entries.items[idx].key;
        }
    }
    return null;
}

pub fn NamespaceWriteTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, State);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        mutable: ActiveMemTable,
        cursor_overlay: ?State = null,
        cursor_base_mutable: ?State = null,
        cursor_immutable_memtables: []const *const State = &.{},
        cursor_runs: []Run = &.{},
        cursor_l0_groups: []RunGroup = &.{},
        cursor_levels: []RunLevel = &.{},
        held_values: std.ArrayListUnmanaged([]u8) = .empty,
        batch_options: backend_types.BatchOptions = .{},
        closed: bool = false,

        pub fn open(backend: *BackendType) !@This() {
            return try openWithOptions(backend, .{});
        }

        pub fn openWithOptions(backend: *BackendType, options: backend_types.BatchOptions) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.retainReader();
            errdefer backend.releaseReader();
            backend.beginBatchMode(options);
            errdefer backend.finishBatchMode(options);
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = runtimeScratchAllocator(backend.allocator),
                .backend = backend,
                .mutable = .{},
                .batch_options = options,
            };
        }

        pub fn abort(self: *@This()) void {
            if (self.closed) return;
            const backend = self.backend;
            self.mutable.deinit(self.allocator);
            self.invalidateCursorSnapshot();
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.finishBatchMode(self.batch_options);
            backend.releaseReader();
            self.* = undefined;
        }

        pub fn commit(self: *@This()) !void {
            if (self.closed) return error.TransactionClosed;
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            var release_on_error = true;
            errdefer if (release_on_error) {
                self.mutable.deinit(self.allocator);
                self.mutable = .{};
                self.invalidateCursorSnapshot();
                releaseHeldValues(&self.held_values, self.allocator);
                self.backend.finishBatchMode(self.batch_options);
                self.backend.releaseReader();
                self.closed = true;
            };
            if (!try self.tryCommitDirectBulkIngest()) {
                const mutated = self.mutable.entries.items.len > 0;
                if (@hasDecl(BackendType, "appendWalForMutable")) {
                    try self.backend.appendWalForMutable(&self.mutable);
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyMutableMoveToMutable(&self.backend.mutable, self.allocator, &self.mutable);
                } else if (@hasDecl(BackendType, "appendWalForState")) {
                    var sorted = try self.mutable.toStateMove(self.allocator);
                    defer sorted.deinit(self.allocator);
                    try self.backend.appendWalForState(&sorted);
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyStateMoveToMutable(&self.backend.mutable, self.allocator, &sorted);
                } else {
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyMutableMoveToMutable(&self.backend.mutable, self.allocator, &self.mutable);
                }
                if (mutated and @hasDecl(BackendType, "notePotentialMaintenanceDebt")) self.backend.notePotentialMaintenanceDebt();
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
                try self.backend.maybeFlushMutable();
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
            } else {
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
            }
            self.backend.finishBatchMode(self.batch_options);
            try self.backend.finalizeExitedBatchMode(self.batch_options);
            release_on_error = false;
            self.closed = true;
            self.invalidateCursorSnapshot();
            releaseHeldValues(&self.held_values, self.allocator);
            var finalize_err: ?anyerror = null;
            self.backend.finalizeWriteReaderRelease() catch |err| {
                finalize_err = err;
            };
            if (finalize_err) |err| return err;
        }

        fn tryCommitDirectBulkIngest(self: *@This()) !bool {
            if (self.batch_options.mode != .bulk_ingest) return false;
            const entries = self.mutable.entries.items.len;
            if (entries == 0) return false;
            if (@hasDecl(BackendType, "recordDirectBulkIngestAttempt")) self.backend.recordDirectBulkIngestAttempt(entries);
            if (!@hasDecl(BackendType, "ingestSortedState") or !@hasDecl(BackendType, "shouldDirectIngestBulkState")) {
                if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackUnsupported")) self.backend.recordDirectBulkIngestFallbackUnsupported();
                return false;
            }
            if (self.backend.mutable.entries.items.len != 0) {
                if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBackendMutable")) self.backend.recordDirectBulkIngestFallbackBackendMutable();
                return false;
            }
            if (@hasDecl(BackendType, "shouldDirectIngestBulkMutable")) {
                if (!self.backend.shouldDirectIngestBulkMutable(&self.mutable)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBelowThreshold")) self.backend.recordDirectBulkIngestFallbackBelowThreshold();
                    return false;
                }
                if (@hasDecl(BackendType, "appendWalForMutable")) try self.backend.appendWalForMutable(&self.mutable);
                const sort_start_ns = platform_time.monotonicNs();
                var sorted = try self.mutable.toStateMove(self.allocator);
                errdefer sorted.deinit(self.allocator);
                const sort_ns = elapsedNs(sort_start_ns);
                if (@hasDecl(BackendType, "ingestOwnedSortedState")) {
                    try self.backend.ingestOwnedSortedState(&sorted);
                } else {
                    try self.backend.ingestSortedState(&sorted);
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, sort_ns);
                sorted.deinit(self.allocator);
            } else {
                const sort_start_ns = platform_time.monotonicNs();
                var sorted = try self.mutable.clone(self.allocator);
                errdefer sorted.deinit(self.allocator);
                const sort_ns = elapsedNs(sort_start_ns);
                if (!self.backend.shouldDirectIngestBulkState(&sorted)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBelowThreshold")) self.backend.recordDirectBulkIngestFallbackBelowThreshold();
                    sorted.deinit(self.allocator);
                    return false;
                }
                if (@hasDecl(BackendType, "appendWalForState")) try self.backend.appendWalForState(&sorted);
                if (@hasDecl(BackendType, "ingestOwnedSortedState")) {
                    try self.backend.ingestOwnedSortedState(&sorted);
                } else {
                    try self.backend.ingestSortedState(&sorted);
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, sort_ns);
                sorted.deinit(self.allocator);
            }
            self.mutable.deinit(self.allocator);
            self.mutable = .{};
            if (@hasDecl(BackendType, "notePotentialMaintenanceDebt")) self.backend.notePotentialMaintenanceDebt();
            return true;
        }

        pub fn get(self: *@This(), namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            if (self.mutable.findIndex(namespace, key)) |idx| {
                const entry = self.mutable.entries.items[idx];
                if (entry.tombstone) return error.NotFound;
                return entry.value;
            }
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            if (comptime @hasField(BackendType, "runs") and @hasField(BackendType, "immutable_memtables")) {
                self.backend.recordPointGets(1);
                return try getCurrentPointRetainedLocked(BackendType, self.backend, namespace, self.allocator, &self.held_values, key) orelse error.NotFound;
            }
            return self.backend.getMergedWithOverlay(&self.backend.mutable, &self.mutable, namespace, key);
        }

        pub fn put(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            try self.mutable.upsert(self.allocator, namespace, key, value, false);
            self.invalidateCursorSnapshot();
        }

        pub fn appendPut(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            try self.mutable.appendUpsert(self.allocator, namespace, key, value, false);
            self.invalidateCursorSnapshot();
        }

        pub fn delete(self: *@This(), namespace: backend_types.Namespace, key: []const u8) !void {
            try self.mutable.upsert(self.allocator, namespace, key, "", true);
            self.invalidateCursorSnapshot();
        }

        pub fn openCursor(self: *@This(), namespace: backend_types.Namespace) !LocalCursor {
            try self.ensureCursorSnapshot();
            const source_count = 1 + self.cursor_immutable_memtables.len + self.cursor_runs.len;
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            const positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(positions);
            @memset(positions, null);
            const source_entries = try cursor_alloc.alloc(?LocalCursor.SourceEntry, source_count);
            errdefer cursor_alloc.free(source_entries);
            @memset(source_entries, null);
            const source_block_bytes = try cursor_alloc.alloc(?[]const u8, source_count);
            errdefer cursor_alloc.free(source_block_bytes);
            @memset(source_block_bytes, null);
            const source_block_handles = try cursor_alloc.alloc(?cache_mod.Handle, source_count);
            errdefer cursor_alloc.free(source_block_handles);
            @memset(source_block_handles, null);
            const source_block_indices = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_block_indices);
            @memset(source_block_indices, null);
            const advance_sources = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(advance_sources);
            const source_heap = try cursor_alloc.alloc(usize, source_count);
            errdefer cursor_alloc.free(source_heap);
            const source_heap_positions = try cursor_alloc.alloc(?usize, source_count);
            errdefer cursor_alloc.free(source_heap_positions);
            @memset(source_heap_positions, null);
            return .{
                .allocator = cursor_alloc,
                .backend = self.backend,
                .mutable = &self.cursor_overlay.?,
                .immutable_memtables = self.cursor_immutable_memtables,
                .runs = self.cursor_runs,
                .l0_groups = self.cursor_l0_groups,
                .levels = self.cursor_levels,
                .namespace = namespace,
                .positions = positions,
                .source_entries = source_entries,
                .source_block_bytes = source_block_bytes,
                .source_block_handles = source_block_handles,
                .source_block_indices = source_block_indices,
                .advance_sources = advance_sources,
                .source_heap = source_heap,
                .source_heap_positions = source_heap_positions,
            };
        }

        fn ensureCursorSnapshot(self: *@This()) !void {
            if (self.cursor_overlay != null) return;
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);

            var overlay = try self.mutable.clone(self.allocator);
            errdefer overlay.deinit(self.allocator);

            var base_mutable: State = .{};
            errdefer base_mutable.deinit(self.allocator);
            try state_mod.applyState(&base_mutable, self.allocator, &self.backend.mutable);

            const backend_immutable = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try self.backend.snapshotImmutableMemtables()
            else
                &.{};
            defer if (backend_immutable.len > 0) self.allocator.free(backend_immutable);

            const immutable = try self.allocator.alloc(*const State, 1 + backend_immutable.len);
            errdefer self.allocator.free(immutable);
            for (backend_immutable, 0..) |state, i| immutable[i + 1] = state;

            const runs = try borrowRunSnapshotList(self.metadata_allocator, self.backend.runs.items);
            errdefer freeRunSnapshotList(self.metadata_allocator, runs);
            const l0_groups = try buildL0RunGroupsWithStats(self.backend, self.metadata_allocator, runs);
            errdefer deinitRunGroups(self.metadata_allocator, l0_groups);
            const levels = try buildLowerLevels(self.metadata_allocator, runs);
            errdefer self.metadata_allocator.free(levels);

            self.cursor_overlay = overlay;
            self.cursor_base_mutable = base_mutable;
            immutable[0] = &self.cursor_base_mutable.?;
            self.cursor_immutable_memtables = immutable;
            self.cursor_runs = runs;
            self.cursor_l0_groups = l0_groups;
            self.cursor_levels = levels;
        }

        fn invalidateCursorSnapshot(self: *@This()) void {
            if (self.cursor_overlay) |*state| {
                state.deinit(self.allocator);
                self.cursor_overlay = null;
            }
            if (self.cursor_base_mutable) |*state| {
                state.deinit(self.allocator);
                self.cursor_base_mutable = null;
            }
            if (self.cursor_immutable_memtables.len > 0) {
                self.allocator.free(self.cursor_immutable_memtables);
                self.cursor_immutable_memtables = &.{};
            }
            if (self.cursor_l0_groups.len > 0) {
                deinitRunGroups(self.metadata_allocator, self.cursor_l0_groups);
                self.cursor_l0_groups = &.{};
            }
            if (self.cursor_levels.len > 0) {
                self.metadata_allocator.free(self.cursor_levels);
                self.cursor_levels = &.{};
            }
            if (self.cursor_runs.len > 0) {
                freeRunSnapshotList(self.metadata_allocator, self.cursor_runs);
                self.cursor_runs = &.{};
            }
        }
    };
}

test "lsm namespace write txn keeps merged mutable state when flush fails after ownership transfer" {
    const TestBackend = struct {
        allocator: Allocator,
        mu: std.atomic.Mutex = .unlocked,
        mutable: State = .{},
        retained_readers: usize = 0,
        active_batches: usize = 0,

        fn retainReader(self: *@This()) void {
            self.retained_readers += 1;
        }

        fn releaseReader(self: *@This()) void {
            std.debug.assert(self.retained_readers > 0);
            self.retained_readers -= 1;
        }

        fn beginBatchMode(self: *@This(), _: backend_types.BatchOptions) void {
            self.active_batches += 1;
        }

        fn finishBatchMode(self: *@This(), _: backend_types.BatchOptions) void {
            std.debug.assert(self.active_batches > 0);
            self.active_batches -= 1;
        }

        fn maybeFlushMutable(_: *@This()) !void {
            return error.InjectedFlushFailure;
        }

        fn finalizeExitedBatchMode(_: *@This(), _: backend_types.BatchOptions) !void {}

        fn finalizeWriteReaderRelease(_: *@This()) !void {}

        fn getMergedWithOverlay(
            _: *@This(),
            backend_mutable: *const State,
            overlay: *const State,
            namespace: backend_types.Namespace,
            key: []const u8,
        ) ![]const u8 {
            if (overlay.get(namespace, key)) |value| return value else |_| {}
            return backend_mutable.get(namespace, key);
        }
    };

    var backend = TestBackend{
        .allocator = std.testing.allocator,
    };
    defer {
        backend.mutable.deinit(std.testing.allocator);
        std.testing.expectEqual(@as(usize, 0), backend.retained_readers) catch unreachable;
        std.testing.expectEqual(@as(usize, 0), backend.active_batches) catch unreachable;
    }

    var txn = try NamespaceWriteTxn(TestBackend).open(&backend);
    errdefer txn.abort();
    try txn.put(.{ .name = "docs" }, "doc:a", "A");
    try std.testing.expectError(error.InjectedFlushFailure, txn.commit());

    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entries.items.len);
    try std.testing.expectEqualStrings("docs", backend.mutable.entries.items[0].namespace_name.?);
    try std.testing.expectEqualStrings("doc:a", backend.mutable.entries.items[0].key);
    try std.testing.expectEqualStrings("A", backend.mutable.entries.items[0].value);
    try std.testing.expect(txn.closed);
}

test "lsm namespace write txn releases local mutable state when wal append fails" {
    const TestBackend = struct {
        allocator: Allocator,
        mu: std.atomic.Mutex = .unlocked,
        mutable: State = .{},
        retained_readers: usize = 0,
        active_batches: usize = 0,

        fn retainReader(self: *@This()) void {
            self.retained_readers += 1;
        }

        fn releaseReader(self: *@This()) void {
            std.debug.assert(self.retained_readers > 0);
            self.retained_readers -= 1;
        }

        fn beginBatchMode(self: *@This(), _: backend_types.BatchOptions) void {
            self.active_batches += 1;
        }

        fn finishBatchMode(self: *@This(), _: backend_types.BatchOptions) void {
            std.debug.assert(self.active_batches > 0);
            self.active_batches -= 1;
        }

        fn appendWalForState(_: *@This(), _: *const State) !void {
            return error.InjectedWalFailure;
        }

        fn maybeFlushMutable(_: *@This()) !void {}

        fn finalizeExitedBatchMode(_: *@This(), _: backend_types.BatchOptions) !void {}

        fn finalizeWriteReaderRelease(_: *@This()) !void {}
    };

    var backend = TestBackend{
        .allocator = std.testing.allocator,
    };
    defer {
        backend.mutable.deinit(std.testing.allocator);
        std.testing.expectEqual(@as(usize, 0), backend.retained_readers) catch unreachable;
        std.testing.expectEqual(@as(usize, 0), backend.active_batches) catch unreachable;
    }

    var txn = try NamespaceWriteTxn(TestBackend).open(&backend);
    try txn.put(.{ .name = "docs" }, "doc:a", "A");
    try std.testing.expectError(error.InjectedWalFailure, txn.commit());

    try std.testing.expect(txn.closed);
    try std.testing.expectEqual(@as(usize, 0), txn.mutable.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entries.items.len);
}

test "lsm merge cursor frees loaded blocks with backend allocator" {
    const TestBackend = struct {
        allocator: Allocator,
    };

    const Cursor = MergeCursor(TestBackend, State);
    const cursor_alloc = std.heap.c_allocator;
    const backend_alloc = std.heap.page_allocator;

    var backend = TestBackend{ .allocator = backend_alloc };
    const positions = try cursor_alloc.alloc(?usize, 1);
    defer cursor_alloc.free(positions);
    positions[0] = null;

    const source_entries = try cursor_alloc.alloc(?Cursor.SourceEntry, 1);
    defer cursor_alloc.free(source_entries);
    source_entries[0] = null;

    const source_block_bytes = try cursor_alloc.alloc(?[]const u8, 1);
    defer cursor_alloc.free(source_block_bytes);
    source_block_bytes[0] = try backend_alloc.alloc(u8, 4096);
    const source_block_handles = try cursor_alloc.alloc(?cache_mod.Handle, 1);
    defer cursor_alloc.free(source_block_handles);
    source_block_handles[0] = null;

    const source_block_indices = try cursor_alloc.alloc(?usize, 1);
    defer cursor_alloc.free(source_block_indices);
    source_block_indices[0] = 0;
    const advance_sources = try cursor_alloc.alloc(usize, 1);
    defer cursor_alloc.free(advance_sources);
    const source_heap = try cursor_alloc.alloc(usize, 1);
    defer cursor_alloc.free(source_heap);
    const source_heap_positions = try cursor_alloc.alloc(?usize, 1);
    defer cursor_alloc.free(source_heap_positions);
    source_heap_positions[0] = null;

    var cursor = Cursor{
        .allocator = cursor_alloc,
        .backend = &backend,
        .mutable = undefined,
        .immutable_memtables = &.{},
        .runs = &.{},
        .l0_groups = &.{},
        .levels = &.{},
        .namespace = .{ .name = "docs" },
        .positions = positions,
        .source_entries = source_entries,
        .source_block_bytes = source_block_bytes,
        .source_block_handles = source_block_handles,
        .source_block_indices = source_block_indices,
        .advance_sources = advance_sources,
        .source_heap = source_heap,
        .source_heap_positions = source_heap_positions,
    };

    cursor.clearSourceBlock(0);
    try std.testing.expectEqual(@as(?[]const u8, null), cursor.source_block_bytes[0]);
    try std.testing.expectEqual(@as(?cache_mod.Handle, null), cursor.source_block_handles[0]);
    try std.testing.expectEqual(@as(?usize, null), cursor.source_block_indices[0]);
}
