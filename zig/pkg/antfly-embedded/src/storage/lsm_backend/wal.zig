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

const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);
const backend_types = @import("../backend_types.zig");
const change_journal_mod = @import("../db/derived/change_journal.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const state_mod = @import("state.zig");
const storage_io = @import("storage_io.zig");

const State = state_mod.State;

const legacy_wal_file_name = "wal.log";
const wal_dir_name = "wal";
const wal_index_file_name = "index";
const wal_checkpoint_file_name = "checkpoint.index";
const replay_index_file_name = "replay.index";
const main_replay_segments_file_name = "replay-main.segments";
const replay_segments_file_name = "replay.segments";
const replay_dir_name = "replay";
const default_wal_segment_bytes: u64 = 64 * 1024 * 1024;
const replay_chunk_bytes: usize = 1024 * 1024;
const record_magic: u32 = 0x314c5741; // "AWL1", little-endian.
const record_version: u16 = 1;
const record_header_len: usize = 16;
const entry_header_len: usize = 16;
const replay_record_magic: u32 = 0x31435741; // "AWC1", little-endian.
const replay_record_version: u16 = 1;
const replay_record_header_len: usize = 24;
const control_magic = "AFWALC01";
const control_version: u16 = 1;
const control_header_len: usize = 16;
const control_checksum_len: usize = @sizeOf(u32);
const control_overhead: usize = control_header_len + control_checksum_len;
const control_pair_len: usize = control_overhead + 16;
const control_triple_len: usize = control_overhead + 24;
const committed_segment_entry_len: usize = control_pair_len;

const ControlKind = enum(u8) {
    current_segment = 1,
    checkpoint = 2,
    replay_index = 3,
    main_replay_segment = 4,
    replay_segment = 5,
    reclamation = 6,
};
pub const default_replay_scratch_retained_cap_bytes: usize = replay_chunk_bytes + @max(record_header_len, replay_record_header_len);

fn scratchAllocator(allocator: Allocator) Allocator {
    // Preserve an explicitly supplied reclaimable root instead of escaping
    // its process ceiling through libc. Unwrap result arenas so scratch frees
    // remain independent, and never introduce reclaim callbacks under locks.
    if (@import("../resource_manager.zig").BudgetedAllocator.fromAllocator(allocator)) |root| {
        return root.threadSafeNoReclaimAllocator();
    }
    if (@import("runtime_memory_abi").Allocator.isStdAdapter(allocator)) return allocator;
    return if (builtin.link_libc) std.heap.c_allocator else allocator;
}

pub const ReplayEntry = backend_types.ReplayEntry;

pub const AppendOptions = struct {
    segment_bytes: u64 = default_wal_segment_bytes,
};

pub const AppendResult = struct {
    bytes: usize = 0,
    segment: u64 = 0,
    segment_became_nonempty: bool = false,
    segment_syncs: u64 = 0,
    index_syncs: u64 = 0,
};

pub const SyncResult = struct {
    segment_syncs: u64 = 0,
    index_syncs: u64 = 0,
};

pub const ReplayStats = struct {
    records: u64 = 0,
    entries: u64 = 0,
    bytes: u64 = 0,
    segments: u64 = 0,
    truncated_tail_bytes: u64 = 0,
};

pub const ReplayHooks = struct {
    ctx: *anyopaque,
    entry_allocator: ?*const fn (ctx: *anyopaque, default_allocator: Allocator) anyerror!Allocator = null,
    on_applied_entry: ?*const fn (ctx: *anyopaque, segment: u64, entry_bytes: u64) anyerror!void = null,
    on_applied_record: *const fn (ctx: *anyopaque, segment: u64, entries: u64) anyerror!void,
};

pub const RetentionStats = struct {
    segments: u64 = 0,
    bytes: u64 = 0,
    oldest_retained_segment: u64 = 0,
    current_segment: u64 = 0,
    current_segment_bytes: u64 = 0,
    checkpoint_covered_through_segment: u64 = 0,
};

pub const ReplayStreamStats = struct {
    records: u64 = 0,
    bytes: u64 = 0,
    segments: u64 = 0,
    truncated_tail_bytes: u64 = 0,
};

const ReplayFileOptions = struct {
    allow_corrupt_tail: bool = false,
};

pub const ReplayWorkingSetOptions = struct {
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
    tracked_working_set_bytes: ?*u64 = null,
    retained_cap_bytes: usize = default_replay_scratch_retained_cap_bytes,
};

const ReplayWorkingSetTracker = struct {
    manager: ?*resource_manager_mod.ResourceManager,
    tracked: ?*u64,
    local_tracked: u64 = 0,

    fn init(options: ReplayWorkingSetOptions) @This() {
        return .{
            .manager = options.resource_manager,
            .tracked = options.tracked_working_set_bytes,
        };
    }

    fn observe(self: *@This(), next: u64) void {
        const manager = self.manager orelse return;
        const tracked = self.tracked orelse &self.local_tracked;
        manager.observeUsage(.lsm_recovery_working_set, tracked, next);
    }

    fn observePending(self: *@This(), pending: *const std.ArrayListUnmanaged(u8)) void {
        self.observe(@intCast(pending.capacity));
    }

    fn release(self: *@This()) void {
        self.observe(0);
    }
};

const ReplaySegmentEntry = struct {
    segment: u64,
    first_sequence: u64,
};

const ReplayScratch = struct {
    path_buf: std.ArrayListUnmanaged(u8) = .empty,
    pending: std.ArrayListUnmanaged(u8) = .empty,
    cached_root_dir: std.ArrayListUnmanaged(u8) = .empty,
    cached_dedicated_layout_only_valid: bool = false,
    cached_dedicated_layout_only: bool = false,
    cached_replay_index_valid: bool = false,
    cached_replay_index_epoch: u64 = 0,
    cached_replay_index: ReadReplayIndex = .{},

    pub fn deinit(self: *@This(), allocator: Allocator) void {
        const scratch_allocator = scratchAllocator(allocator);
        self.path_buf.deinit(scratch_allocator);
        self.pending.deinit(scratch_allocator);
        self.cached_root_dir.deinit(scratch_allocator);
        self.* = .{};
    }
};

const ThreadReplayScratch = struct {
    in_use: bool = false,
    scratch: ReplayScratch = .{},
};

threadlocal var thread_replay_scratch = ThreadReplayScratch{};
var replay_index_epoch: AtomicU64 = .init(1);

pub fn pathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return legacyPathAlloc(allocator, root_dir);
}

pub fn appendState(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    state: anytype,
    sync: bool,
) !usize {
    return try appendStateWithOptions(storage, allocator, root_dir, state, sync, .{});
}

pub fn appendStateWithOptions(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    state: anytype,
    sync: bool,
    options: AppendOptions,
) !usize {
    return (try appendStateWithOptionsResult(storage, allocator, root_dir, state, sync, options)).bytes;
}

pub fn appendStateWithOptionsResult(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    state: anytype,
    sync: bool,
    options: AppendOptions,
) !AppendResult {
    if (state.entryCount() == 0) return .{};

    const payload_len = encodedPayloadLen(state);
    var record = try allocator.alloc(u8, record_header_len + payload_len);
    defer allocator.free(record);
    const payload = record[record_header_len..];
    try encodePayloadIntoSlice(payload, state);

    std.mem.writeInt(u32, record[0..4], record_magic, .little);
    std.mem.writeInt(u16, record[4..6], record_version, .little);
    std.mem.writeInt(u16, record[6..8], record_header_len, .little);
    std.mem.writeInt(u32, record[8..12], @intCast(payload.len), .little);
    std.mem.writeInt(u32, record[12..16], Crc32.hash(payload), .little);

    const wal_dir = try walDirPathAlloc(allocator, root_dir);
    defer allocator.free(wal_dir);
    try storage.createDirPath(wal_dir);

    const current = try readCurrentSegment(storage, allocator, root_dir);
    var segment = current.segment;
    var current_size = current.size;
    var index_syncs: u64 = 0;
    if (!current.index_exists) {
        // Persist the WAL directory's own namespace entry once during
        // initialization. Segment rotation only needs the already-durable WAL
        // directory and does not add a steady-state parent sync.
        if (sync) try storage.syncParentAbsolute(wal_dir);
        try writeCurrentSegment(storage, allocator, root_dir, segment, current_size);
        index_syncs += 1;
        try writeCheckpointIndex(storage, allocator, root_dir, .{
            .oldest_retained_segment = 1,
            .covered_through_segment = 0,
        });
        index_syncs += 1;
    }
    var segment_path = try segmentPathAlloc(allocator, root_dir, segment);
    var segment_path_owned = true;
    errdefer if (segment_path_owned) allocator.free(segment_path);
    if (current_size > 0 and current_size + record.len > options.segment_bytes) {
        allocator.free(segment_path);
        segment_path_owned = false;
        segment += 1;
        current_size = 0;
        try writeCurrentSegment(storage, allocator, root_dir, segment, current_size);
        index_syncs += 1;
        segment_path = try segmentPathAlloc(allocator, root_dir, segment);
        segment_path_owned = true;
    }
    const segment_became_nonempty = current_size == 0;
    storage.appendFileAbsolute(allocator, segment_path, record, sync) catch |err| switch (err) {
        error.FileNotFound => {
            try storage.createDirPath(wal_dir);
            try storage.appendFileAbsolute(allocator, segment_path, record, sync);
        },
        else => return err,
    };
    // fsync(file) does not make a newly-created directory entry durable on
    // POSIX filesystems. Pay the parent-directory boundary only for the first
    // record in a segment; steady-state appends remain one file sync.
    if (sync and segment_became_nonempty) try storage.syncParentAbsolute(segment_path);
    allocator.free(segment_path);
    segment_path_owned = false;
    return .{
        .bytes = record.len,
        .segment = segment,
        .segment_became_nonempty = segment_became_nonempty,
        .segment_syncs = @intFromBool(sync),
        .index_syncs = index_syncs,
    };
}

pub fn encodedStateRecordLen(state: anytype) usize {
    return record_header_len + encodedPayloadLen(state);
}

pub fn currentSegment(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !u64 {
    return (try readCurrentSegment(storage, allocator, root_dir)).segment;
}

pub fn syncCurrentState(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !SyncResult {
    const current = readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => return .{},
        else => return err,
    };
    const temp_allocator = allocator;
    const segment_path = try segmentPathAlloc(temp_allocator, root_dir, current.segment);
    defer temp_allocator.free(segment_path);
    try storage.appendFileAbsolute(allocator, segment_path, "", true);

    const index_path = try indexPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(index_path);
    try storage.appendFileAbsolute(allocator, index_path, "", true);
    return .{ .segment_syncs = 1, .index_syncs = 1 };
}

/// Owns one WAL durability cut after its caller has acquired publication and
/// reusable completion resources. Publication must provide release(). The
/// backend/publication owner outlives this value, and callers serialize methods.
/// This performs only CURRENT metadata and ordered barriers, not backend flush.
pub fn CurrentStateSync(comptime Publication: type) type {
    return struct {
        const Self = @This();
        const Stage = enum { current, segment, index, complete, failed };
        const Pending = union(enum) { none, read: storage_io.ControlFileReadOperation, sync: storage_io.SyncOperation };
        index_path: [std.fs.max_path_bytes]u8 = undefined,
        index_len: usize = 0,
        segment_path: [std.fs.max_path_bytes]u8 = undefined,
        segment_prefix_len: usize = 0,
        segment_len: usize = 0,
        resources: ?storage_io.DurabilityResources = null,
        publication: ?Publication = null,
        pending: Pending = .none,
        stage: Stage = .current,
        result: SyncResult = .{},
        failure: ?anyerror = null,

        /// Prepare bounded paths before acquiring any publication lock. This
        /// value is movable: it stores lengths, never slices into itself.
        pub fn prepare(root_dir: []const u8) !Self {
            var self: Self = .{};
            if (root_dir.len >= std.fs.max_path_bytes) return error.NameTooLong;
            const separator = if (root_dir.len == 0 or std.mem.endsWith(u8, root_dir, "/")) "" else "/";
            const prefix = std.fmt.bufPrint(&self.segment_path, "{s}{s}{s}/", .{ root_dir, separator, wal_dir_name }) catch return error.NameTooLong;
            if (prefix.len + 24 >= self.segment_path.len) return error.NameTooLong;
            self.segment_prefix_len = prefix.len;
            const path = std.fmt.bufPrint(&self.index_path, "{s}{s}", .{ prefix, wal_index_file_name }) catch return error.NameTooLong;
            self.index_len = path.len;
            return self;
        }

        /// Transfer both grants only after the initial read is accepted. A
        /// pre-dispatch failure leaves both arguments owned by the caller.
        pub fn start(self: *Self, resources: storage_io.DurabilityResources, publication: Publication, original: @import("../../common/read_admission.zig").Port) !void {
            std.debug.assert(self.resources == null and self.publication == null);
            var prepared = resources;
            const read = try prepared.beginControlRead(self.index_path[0..self.index_len], control_pair_len, original);
            self.resources = prepared;
            self.publication = publication;
            self.pending = .{ .read = read };
        }

        pub fn waitWithAdmission(self: *Self, original: @import("../../common/read_admission.zig").Port) !SyncResult {
            const admission = @import("../../common/read_admission.zig");
            _ = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
            _ = (try original.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
            while (true) {
                try original.checkpoint();
                switch (self.stage) {
                    .complete => return self.result,
                    .failed => return self.failure.?,
                    .current => {
                        std.debug.assert(self.pending == .read);
                        var trace: admission.RejectionTrace = .{};
                        const control = trace.bind(original).?;
                        const current = self.pending.read.waitWithAdmission(control) catch |cause| {
                            // An original FileNotFound is a rejected waiter,
                            // never evidence that CURRENT is absent.
                            if (trace.failure) |rejected| return rejected;
                            if (cause == error.FileNotFound) {
                                self.retirePending();
                                self.stage = .complete;
                                self.releasePublication();
                                continue;
                            }
                            return self.fail(cause);
                        };
                        // Only successful reentry authorizes format/checksum
                        // CPU. Rejection preserves the exact read and bytes.
                        try original.checkpoint();
                        if (current.file_size != control_pair_len or current.bytes.len != control_pair_len)
                            return self.fail(error.CorruptLsmWalIndex);
                        const payload = decodeControl(current.bytes, .current_segment, 16) catch |cause| return self.fail(cause);
                        const segment = std.mem.readInt(u64, payload[0..8], .little);
                        if (segment == 0) return self.fail(error.CorruptLsmWalIndex);
                        const name = std.fmt.bufPrint(self.segment_path[self.segment_prefix_len..], "{d:0>20}.log", .{segment}) catch unreachable;
                        self.segment_len = self.segment_prefix_len + name.len;
                        self.retirePending();
                        self.stage = .segment;
                    },
                    .segment, .index => {
                        if (self.pending == .none) {
                            const path = if (self.stage == .segment) self.segment_path[0..self.segment_len] else self.index_path[0..self.index_len];
                            // These phases use the already admitted worker,
                            // descriptor and fixed buffers, even under pressure.
                            self.pending = .{ .sync = try self.resources.?.beginSyncContents(path, original) };
                        }
                        var trace: admission.RejectionTrace = .{};
                        self.pending.sync.waitWithAdmission(trace.bind(original).?) catch |cause| {
                            if (trace.failure) |rejected| return rejected;
                            return self.fail(cause);
                        };
                        self.retirePending();
                        if (self.stage == .segment) {
                            self.result.segment_syncs = 1;
                            self.stage = .index;
                        } else {
                            self.result.index_syncs = 1;
                            self.stage = .complete;
                            self.releasePublication();
                        }
                    },
                }
            }
        }

        fn fail(self: *Self, cause: anyerror) anyerror {
            // Called only after admitted observation of physical completion or
            // admitted validation. A terminal error cannot replay a phase.
            self.retirePending();
            self.failure = cause;
            self.stage = .failed;
            self.releasePublication();
            return cause;
        }

        fn retirePending(self: *Self) void {
            switch (self.pending) {
                .read => |*read| read.deinit(),
                .sync => |*sync| sync.deinit(),
                .none => {},
            }
            self.pending = .none;
        }

        fn releasePublication(self: *Self) void {
            if (self.publication) |*publication| publication.release();
            self.publication = null;
        }

        /// Terminal worker retirement borrows the original cleanup execution.
        /// Rejected join/reentry retains the grant and its charge for retry.
        pub fn retireWithAdmission(self: *Self, original: @import("../../common/read_admission.zig").Port) !void {
            try original.checkpoint();
            if (self.stage != .complete and self.stage != .failed) return error.ResourceBudgetExceeded;
            std.debug.assert(self.pending == .none and self.publication == null);
            if (self.resources) |*resources| {
                try resources.retireWithAdmission(original);
                self.resources = null;
            }
        }

        /// Mandatory quiesced retirement, never automatic cleanup after a
        /// rejected wait. Accepted physical work finishes before publication
        /// is released; the idle worker and descriptor are retired last.
        pub fn deinit(self: *Self) void {
            self.retirePending();
            self.releasePublication();
            if (self.resources) |*resources| resources.deinit();
            self.* = undefined;
        }
    };
}

pub fn replayIntoMutable(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    mutable: anytype,
) !ReplayStats {
    return try replayIntoMutableWithHooks(storage, allocator, root_dir, mutable, null);
}

pub fn replayIntoMutableWithHooks(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    mutable: anytype,
    hooks: ?ReplayHooks,
) !ReplayStats {
    return try replayIntoMutableWithHooksAndOptions(storage, allocator, root_dir, mutable, hooks, .{});
}

pub fn replayIntoMutableWithHooksAndOptions(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    mutable: anytype,
    hooks: ?ReplayHooks,
    options: ReplayWorkingSetOptions,
) !ReplayStats {
    var stats = ReplayStats{};
    var working_set = ReplayWorkingSetTracker.init(options);
    defer working_set.release();
    const retained_cap_bytes = normalizedReplayPendingRetainedCap(options.retained_cap_bytes);
    const legacy_path = try legacyPathAlloc(allocator, root_dir);
    defer allocator.free(legacy_path);
    try replayFileStreaming(storage, allocator, legacy_path, 0, mutable, &stats, hooks, .{}, &working_set, retained_cap_bytes);

    const current_segment = (readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => return stats,
        else => return err,
    }).segment;
    const checkpoint = try readCheckpointIndex(storage, allocator, root_dir);
    var segment: u64 = checkpoint.oldest_retained_segment;
    while (segment <= current_segment) : (segment += 1) {
        const segment_path = try segmentPathAlloc(allocator, root_dir, segment);
        errdefer allocator.free(segment_path);
        try replayFileStreaming(storage, allocator, segment_path, segment, mutable, &stats, hooks, .{
            .allow_corrupt_tail = segment == current_segment,
        }, &working_set, retained_cap_bytes);
        allocator.free(segment_path);
    }

    return stats;
}

fn normalizedReplayPendingRetainedCap(retained_cap_bytes: usize) usize {
    const min_retained = replay_chunk_bytes + @max(record_header_len, replay_record_header_len);
    return @max(retained_cap_bytes, min_retained);
}

pub fn reset(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !void {
    const wal_dir = try walDirPathAlloc(allocator, root_dir);
    defer allocator.free(wal_dir);
    try storage.createDirPath(wal_dir);
    try storage.syncParentAbsolute(wal_dir);

    const current_segment = (readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => CurrentSegment{ .segment = 1, .size = 0, .index_exists = false },
        else => return err,
    }).segment;
    try writeCurrentSegment(storage, allocator, root_dir, 1, 0);
    try writeCheckpointIndex(storage, allocator, root_dir, .{
        .oldest_retained_segment = 1,
        .covered_through_segment = 0,
    });
    const first_segment = try segmentPathAlloc(allocator, root_dir, 1);
    defer allocator.free(first_segment);
    try storage.writeFileAbsolute(first_segment, "");
    try writeReplayIndex(storage, allocator, root_dir, .{
        .current_segment = 1,
        .next_sequence = 1,
        .truncated_through = 0,
    });
    const replay_segments_path = try replaySegmentsPathAlloc(allocator, root_dir);
    defer allocator.free(replay_segments_path);
    try replaceFileAtomically(storage, allocator, replay_segments_path, "");

    var segment: u64 = 2;
    while (segment <= current_segment) : (segment += 1) {
        const segment_path = try segmentPathAlloc(allocator, root_dir, segment);
        errdefer allocator.free(segment_path);
        storage.deleteFileAbsolute(segment_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        allocator.free(segment_path);
    }

    const legacy_path = try legacyPathAlloc(allocator, root_dir);
    defer allocator.free(legacy_path);
    storage.deleteFileAbsolute(legacy_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}

pub fn snapshotRetention(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
) !RetentionStats {
    var stats = RetentionStats{};

    const legacy_path = try legacyPathAlloc(allocator, root_dir);
    defer allocator.free(legacy_path);
    const legacy_size = storage.fileSize(legacy_path) catch |err| switch (err) {
        error.FileNotFound => 0,
        else => return err,
    };
    if (legacy_size > 0) {
        stats.segments += 1;
        stats.bytes += legacy_size;
    }

    const current_segment = (readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => return stats,
        else => return err,
    }).segment;
    const checkpoint = try readCheckpointIndex(storage, allocator, root_dir);
    stats.oldest_retained_segment = checkpoint.oldest_retained_segment;
    stats.checkpoint_covered_through_segment = checkpoint.covered_through_segment;
    stats.current_segment = current_segment;

    var segment: u64 = checkpoint.oldest_retained_segment;
    while (segment <= current_segment) : (segment += 1) {
        const segment_path = try segmentPathAlloc(allocator, root_dir, segment);
        const size = storage.fileSize(segment_path) catch |err| switch (err) {
            error.FileNotFound => {
                allocator.free(segment_path);
                continue;
            },
            else => return err,
        };
        allocator.free(segment_path);
        if (size == 0) continue;
        if (segment == current_segment) stats.current_segment_bytes = size;
        stats.segments += 1;
        stats.bytes += size;
    }

    return stats;
}

pub fn snapshotReplayRetention(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
) !RetentionStats {
    var stats = RetentionStats{};
    const index = try readReplayIndex(storage, allocator, root_dir);
    if (!index.exists) return stats;

    stats.current_segment = index.current_segment;
    var segment: u64 = 1;
    while (segment <= index.current_segment) : (segment += 1) {
        const segment_path = try replaySegmentPathAlloc(allocator, root_dir, segment);
        const size = storage.fileSize(segment_path) catch |err| switch (err) {
            error.FileNotFound => {
                allocator.free(segment_path);
                continue;
            },
            else => return err,
        };
        allocator.free(segment_path);
        if (size == 0) continue;
        if (segment == index.current_segment) stats.current_segment_bytes = size;
        if (stats.oldest_retained_segment == 0) stats.oldest_retained_segment = segment;
        stats.segments += 1;
        stats.bytes += size;
    }

    return stats;
}

pub fn retireCoveredSegments(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    covered_through_segment: u64,
) !void {
    if (covered_through_segment == 0) return;
    const current_segment = (readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    }).segment;
    if (current_segment <= 1) return;
    const checkpoint = try readCheckpointIndex(storage, allocator, root_dir);
    const max_covered = @min(covered_through_segment, current_segment - 1);
    if (max_covered < checkpoint.oldest_retained_segment) return;

    var segment = checkpoint.oldest_retained_segment;
    while (segment <= max_covered) : (segment += 1) {
        const segment_path = try segmentPathAlloc(allocator, root_dir, segment);
        errdefer allocator.free(segment_path);
        storage.deleteFileAbsolute(segment_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        allocator.free(segment_path);
    }

    try writeCheckpointIndex(storage, allocator, root_dir, .{
        .oldest_retained_segment = max_covered + 1,
        .covered_through_segment = max_covered,
    });
}

const ReplayIndex = struct {
    current_segment: u64,
    next_sequence: u64,
    truncated_through: u64,
};

pub fn appendReplay(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    sequence: u64,
    payload: []const u8,
    sync: bool,
    options: AppendOptions,
) !usize {
    if (payload.len == 0) return 0;

    var record = try allocator.alloc(u8, replay_record_header_len + payload.len);
    defer allocator.free(record);

    std.mem.writeInt(u32, record[0..4], replay_record_magic, .little);
    std.mem.writeInt(u16, record[4..6], replay_record_version, .little);
    std.mem.writeInt(u16, record[6..8], replay_record_header_len, .little);
    std.mem.writeInt(u64, record[8..16], sequence, .little);
    std.mem.writeInt(u32, record[16..20], @intCast(payload.len), .little);
    std.mem.writeInt(u32, record[20..24], Crc32.hash(payload), .little);
    @memcpy(record[replay_record_header_len..], payload);

    const wal_dir = try walDirPathAlloc(allocator, root_dir);
    defer allocator.free(wal_dir);
    try storage.createDirPath(wal_dir);
    const replay_dir = try replayDirPathAlloc(allocator, root_dir);
    defer allocator.free(replay_dir);
    try storage.createDirPath(replay_dir);

    const index = try readReplayIndex(storage, allocator, root_dir);
    if (!index.exists) {
        // The replay segment is nested two directory levels below the root.
        // Make both ancestors durable before publishing an index that can
        // expose replay data after a crash.
        if (sync) {
            try storage.syncParentAbsolute(wal_dir);
            try storage.syncParentAbsolute(replay_dir);
        }
        try writeReplayIndex(storage, allocator, root_dir, .{
            .current_segment = 1,
            .next_sequence = index.next_sequence,
            .truncated_through = index.truncated_through,
        });
    }
    if (index.next_sequence == 1) {
        try writeDedicatedReplayMarker(storage, allocator, root_dir);
    }

    var segment = if (index.exists) index.current_segment else 1;
    var segment_path = try replaySegmentPathAlloc(allocator, root_dir, segment);
    var segment_path_owned = true;
    defer if (segment_path_owned) allocator.free(segment_path);
    const current_size = storage.fileSize(segment_path) catch |err| switch (err) {
        error.FileNotFound => 0,
        else => return err,
    };
    var segment_became_nonempty = current_size == 0;
    if (current_size > 0 and current_size + record.len > options.segment_bytes) {
        segment += 1;
        allocator.free(segment_path);
        segment_path_owned = false;
        segment_path = try replaySegmentPathAlloc(allocator, root_dir, segment);
        segment_path_owned = true;
        segment_became_nonempty = true;
    }

    try appendReplaySegmentEntryIfNeeded(storage, allocator, root_dir, segment, sequence, true);
    try storage.appendFileAbsolute(allocator, segment_path, record, sync);
    if (sync and segment_became_nonempty) try storage.syncParentAbsolute(segment_path);
    try writeReplayIndex(storage, allocator, root_dir, .{
        .current_segment = segment,
        .next_sequence = sequence + 1,
        .truncated_through = index.truncated_through,
    });
    return record.len;
}

pub fn nextReplaySequence(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    fallback_next: u64,
) u64 {
    const index = readReplayIndex(storage, allocator, root_dir) catch return fallback_next;
    return if (index.exists) index.next_sequence else fallback_next;
}

pub fn lastReplaySequence(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    fallback_last: u64,
) u64 {
    const next = nextReplaySequence(storage, allocator, root_dir, fallback_last + 1);
    return if (next <= 1) 0 else next - 1;
}

pub fn truncateReplayUpTo(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    up_to_sequence: u64,
) !void {
    if (up_to_sequence == 0) return;
    const index = try readReplayIndex(storage, allocator, root_dir);
    if (!index.exists) return;
    if (up_to_sequence <= index.truncated_through) return;
    try writeReplayIndex(storage, allocator, root_dir, .{
        .current_segment = index.current_segment,
        .next_sequence = index.next_sequence,
        .truncated_through = up_to_sequence,
    });
}

pub fn iterateReplayFrom(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    from_sequence: u64,
) ![]ReplayEntry {
    var results = std.ArrayListUnmanaged(ReplayEntry).empty;
    errdefer {
        for (results.items) |*entry| entry.deinit(allocator);
        results.deinit(allocator);
    }
    const Collector = struct {
        allocator: Allocator,
        out: *std.ArrayListUnmanaged(ReplayEntry),

        fn append(self: *@This(), sequence: u64, payload: []const u8) !void {
            try self.out.append(self.allocator, .{
                .sequence = sequence,
                .payload = try self.allocator.dupe(u8, payload),
            });
        }
    };
    var collector = Collector{
        .allocator = allocator,
        .out = &results,
    };
    try forEachReplayFrom(storage, allocator, root_dir, from_sequence, &collector, Collector.append);
    return try results.toOwnedSlice(allocator);
}

pub fn forEachReplayFrom(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    from_sequence: u64,
    ctx: anytype,
    comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
) !void {
    try forEachReplayFromMatchingHintMask(storage, allocator, root_dir, from_sequence, 0, ctx, callback);
}

pub fn forEachReplayFromMatchingHintMask(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    from_sequence: u64,
    required_hint_mask: u8,
    ctx: anytype,
    comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
) !void {
    var stats = ReplayStreamStats{};
    if (builtin.link_libc and !thread_replay_scratch.in_use) {
        thread_replay_scratch.in_use = true;
        defer thread_replay_scratch.in_use = false;
        try iterateReplayStreaming(
            storage,
            allocator,
            root_dir,
            from_sequence,
            required_hint_mask,
            &thread_replay_scratch.scratch,
            ctx,
            callback,
            &stats,
        );
        return;
    }

    var scratch = ReplayScratch{};
    defer scratch.deinit(allocator);
    try iterateReplayStreaming(storage, allocator, root_dir, from_sequence, required_hint_mask, &scratch, ctx, callback, &stats);
}

fn iterateReplayStreaming(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    from_sequence: u64,
    required_hint_mask: u8,
    scratch: *ReplayScratch,
    ctx: anytype,
    comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    stats: *ReplayStreamStats,
) !void {
    const index = try readReplayIndexCached(storage, allocator, root_dir, scratch);
    if (!index.exists) return;

    const effective_from = @max(from_sequence, index.truncated_through + 1);
    const scratch_allocator = scratchAllocator(allocator);
    scratch.path_buf.clearRetainingCapacity();
    scratch.pending.clearRetainingCapacity();
    const max_path_len =
        root_dir.len +
        1 +
        wal_dir_name.len +
        1 +
        replay_dir_name.len +
        1 +
        "00000000000000000000.log".len;
    try scratch.path_buf.ensureTotalCapacityPrecise(scratch_allocator, max_path_len);
    try scratch.pending.ensureTotalCapacityPrecise(scratch_allocator, default_replay_scratch_retained_cap_bytes);

    if (!(try replayScratchDedicatedLayoutOnly(scratch, storage, allocator, root_dir))) {
        const main_wal_start_segment = try replayMainWalStartSegment(storage, allocator, root_dir, effective_from);
        const current_segment = (readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
            error.FileNotFound => CurrentSegment{ .segment = 0, .size = 0, .index_exists = false },
            else => return err,
        }).segment;
        if (main_wal_start_segment <= current_segment) {
            const main_prefix_len = try appendWalSegmentPathPrefix(&scratch.path_buf, scratch_allocator, root_dir);
            var segment = main_wal_start_segment;
            while (segment <= current_segment) : (segment += 1) {
                const segment_path = try appendSegmentPathSuffix(&scratch.path_buf, scratch_allocator, main_prefix_len, segment);
                try replayRecordsFromMainWal(storage, allocator, &scratch.pending, segment_path, effective_from, required_hint_mask, ctx, callback, stats);
            }
        }
    }

    const replay_prefix_len = try appendReplaySegmentPathPrefix(&scratch.path_buf, scratch_allocator, root_dir);
    var segment = try replayStartSegment(storage, allocator, root_dir, effective_from);
    while (segment <= index.current_segment) : (segment += 1) {
        const segment_path = try appendSegmentPathSuffix(&scratch.path_buf, scratch_allocator, replay_prefix_len, segment);
        try replayFile(storage, allocator, &scratch.pending, segment_path, effective_from, required_hint_mask, ctx, callback, stats);
    }
}

fn readReplayIndexCached(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    scratch: *ReplayScratch,
) !ReadReplayIndex {
    const epoch = replay_index_epoch.load(.monotonic);
    if (scratch.cached_replay_index_valid and
        scratch.cached_replay_index_epoch == epoch and
        std.mem.eql(u8, scratch.cached_root_dir.items, root_dir))
    {
        return scratch.cached_replay_index;
    }

    const index = try readReplayIndex(storage, allocator, root_dir);
    const scratch_allocator = scratchAllocator(allocator);
    if (!std.mem.eql(u8, scratch.cached_root_dir.items, root_dir)) {
        scratch.cached_root_dir.clearRetainingCapacity();
        try scratch.cached_root_dir.appendSlice(scratch_allocator, root_dir);
    }
    scratch.cached_replay_index = index;
    scratch.cached_replay_index_epoch = epoch;
    scratch.cached_replay_index_valid = true;
    return index;
}

fn replayScratchDedicatedLayoutOnly(
    scratch: *ReplayScratch,
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
) !bool {
    if (scratch.cached_dedicated_layout_only_valid and std.mem.eql(u8, scratch.cached_root_dir.items, root_dir)) {
        return scratch.cached_dedicated_layout_only;
    }

    const scratch_allocator = scratchAllocator(allocator);
    scratch.cached_root_dir.clearRetainingCapacity();
    try scratch.cached_root_dir.appendSlice(scratch_allocator, root_dir);

    const segments_path = try mainReplaySegmentsPathAlloc(scratch_allocator, root_dir);
    defer scratch_allocator.free(segments_path);
    const size = storage.fileSize(segments_path) catch |err| switch (err) {
        error.FileNotFound => {
            scratch.cached_dedicated_layout_only = false;
            scratch.cached_dedicated_layout_only_valid = true;
            return false;
        },
        else => return err,
    };

    scratch.cached_dedicated_layout_only = size == 0;
    scratch.cached_dedicated_layout_only_valid = true;
    return scratch.cached_dedicated_layout_only;
}

fn replayRecordsFromMainWal(
    storage: storage_io.Storage,
    allocator: Allocator,
    pending: *std.ArrayListUnmanaged(u8),
    wal_path: []const u8,
    from_sequence: u64,
    required_hint_mask: u8,
    ctx: anytype,
    comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    stats: *ReplayStreamStats,
) !void {
    const chunk_allocator = scratchAllocator(allocator);
    const file_size = storage.fileSize(wal_path) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    if (file_size == 0) return;
    stats.segments += 1;
    stats.bytes += file_size;
    pending.clearRetainingCapacity();

    var offset: u64 = 0;
    while (offset < file_size) {
        const len: usize = @intCast(@min(@as(u64, replay_chunk_bytes), file_size - offset));
        const start = pending.items.len;
        try pending.ensureUnusedCapacity(chunk_allocator, len);
        pending.items.len += len;
        errdefer pending.items.len = start;
        try storage.readFileRangeInto(chunk_allocator, wal_path, offset, pending.items[start..][0..len]);
        offset += len;
        consumeReplayRecordsFromMixedWal(allocator, pending, from_sequence, required_hint_mask, ctx, callback, stats) catch |err| switch (err) {
            error.CorruptLsmWal => {
                std.log.warn("lsm replay corrupt path={s} source=main_wal offset={} pending_len={}", .{
                    wal_path,
                    offset,
                    pending.items.len,
                });
                return err;
            },
            else => return err,
        };
        releaseOversizedReplayPendingBuffer(chunk_allocator, pending, default_replay_scratch_retained_cap_bytes);
    }
    if (pending.items.len > 0) stats.truncated_tail_bytes += @intCast(pending.items.len);
}

fn replayFile(
    storage: storage_io.Storage,
    allocator: Allocator,
    pending: *std.ArrayListUnmanaged(u8),
    wal_path: []const u8,
    from_sequence: u64,
    required_hint_mask: u8,
    ctx: anytype,
    comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    stats: *ReplayStreamStats,
) !void {
    const chunk_allocator = scratchAllocator(allocator);
    pending.clearRetainingCapacity();

    var offset: u64 = 0;
    var saw_bytes = false;
    while (true) {
        const start = pending.items.len;
        try pending.ensureUnusedCapacity(chunk_allocator, replay_chunk_bytes);
        pending.items.len += replay_chunk_bytes;
        errdefer pending.items.len = start;
        const read_len = storage.readFileRangeAtMostInto(
            chunk_allocator,
            wal_path,
            offset,
            pending.items[start..][0..replay_chunk_bytes],
        ) catch |err| switch (err) {
            error.FileNotFound => {
                pending.items.len = start;
                return;
            },
            else => return err,
        };
        pending.items.len = start + read_len;
        if (read_len == 0) break;
        saw_bytes = true;
        stats.bytes += read_len;
        offset += read_len;
        consumeCompleteReplayRecords(allocator, pending, from_sequence, required_hint_mask, ctx, callback, stats) catch |err| switch (err) {
            error.TruncatedLsmWalSparseHole => {
                const retained_bytes: u64 = offset - pending.items.len;
                repairTruncatedReplayFile(storage, allocator, wal_path, retained_bytes) catch |repair_err| {
                    std.log.warn("lsm replay zero-hole repair failed path={s} retained_bytes={} err={s}", .{
                        wal_path,
                        retained_bytes,
                        @errorName(repair_err),
                    });
                };
                std.log.warn("lsm replay truncated zero hole path={s} source=dedicated_replay offset={} pending_len={}", .{
                    wal_path,
                    offset,
                    pending.items.len,
                });
                stats.truncated_tail_bytes += @intCast(pending.items.len);
                pending.clearRetainingCapacity();
                releaseOversizedReplayPendingBuffer(chunk_allocator, pending, default_replay_scratch_retained_cap_bytes);
                break;
            },
            error.CorruptLsmWal => {
                std.log.warn("lsm replay corrupt path={s} source=dedicated_replay offset={} pending_len={}", .{
                    wal_path,
                    offset,
                    pending.items.len,
                });
                return err;
            },
            else => return err,
        };
        releaseOversizedReplayPendingBuffer(chunk_allocator, pending, default_replay_scratch_retained_cap_bytes);
        if (read_len < replay_chunk_bytes) break;
    }
    if (saw_bytes) stats.segments += 1;
    if (pending.items.len > 0) stats.truncated_tail_bytes += @intCast(pending.items.len);
}

fn releaseOversizedReplayPendingBuffer(allocator: Allocator, pending: *std.ArrayListUnmanaged(u8), retained_cap_bytes: usize) void {
    const retained_cap = normalizedReplayPendingRetainedCap(retained_cap_bytes);
    if (pending.capacity <= retained_cap) return;
    if (pending.items.len > retained_cap) return;
    pending.shrinkAndFree(allocator, pending.items.len);
}

fn repairTruncatedReplayFile(
    storage: storage_io.Storage,
    allocator: Allocator,
    wal_path: []const u8,
    retained_bytes: u64,
) !void {
    const temp_allocator = allocator;
    const retained = try storage.readFileRangeAlloc(temp_allocator, wal_path, 0, @intCast(retained_bytes));
    defer temp_allocator.free(retained);
    try storage.writeFileAbsolute(wal_path, retained);
}

fn consumeCompleteReplayRecords(
    allocator: Allocator,
    pending: *std.ArrayListUnmanaged(u8),
    from_sequence: u64,
    required_hint_mask: u8,
    ctx: anytype,
    comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    stats: *ReplayStreamStats,
) !void {
    var pos: usize = 0;
    while (pending.items.len - pos >= replay_record_header_len) {
        const header = pending.items[pos..][0..replay_record_header_len];
        const magic = std.mem.readInt(u32, header[0..4], .little);
        if (magic != replay_record_magic) {
            if (magic == 0) {
                if (pos == 0) {
                    logCorruptWalDetail("replay_zero_hole", 0, pos, pending.items.len, "zero-filled replay file has no valid prefix");
                    return error.CorruptLsmWal;
                }
                if (pos > 0) {
                    const remaining_len = pending.items.len - pos;
                    std.mem.copyForwards(u8, pending.items[0..remaining_len], pending.items[pos..]);
                    pending.items.len = remaining_len;
                }
                std.log.warn("lsm wal truncated zero hole site=replay_zero_hole pos={} pending_len={}", .{
                    pos,
                    pending.items.len,
                });
                return error.TruncatedLsmWalSparseHole;
            }
            logCorruptWalDetail("replay_magic", 0, pos, pending.items.len, "unknown replay record magic");
            return error.CorruptLsmWal;
        }
        const version = std.mem.readInt(u16, header[4..6], .little);
        if (version != replay_record_version) return error.UnsupportedLsmWalVersion;
        const header_len = std.mem.readInt(u16, header[6..8], .little);
        if (header_len != replay_record_header_len) return error.UnsupportedLsmWalHeader;
        const sequence = std.mem.readInt(u64, header[8..16], .little);
        const payload_len: usize = @intCast(std.mem.readInt(u32, header[16..20], .little));
        const expected_crc = std.mem.readInt(u32, header[20..24], .little);
        const total_len = replay_record_header_len + payload_len;
        if (pending.items.len - pos < total_len) break;

        if (sequence >= from_sequence) {
            const payload = pending.items[pos + replay_record_header_len .. pos + total_len];
            if (Crc32.hash(payload) != expected_crc) {
                logCorruptWalDetail("replay_crc", 0, pos, pending.items.len, "replay CRC mismatch");
                return error.CorruptLsmWal;
            }
            if (required_hint_mask != 0 and !(try change_journal_mod.encodedRecordMatchesHintMask(payload, required_hint_mask))) {
                pos += total_len;
                continue;
            }
            _ = allocator;
            try callback(ctx, sequence, payload);
            stats.records += 1;
        }
        pos += total_len;
    }

    if (pos == 0) return;
    const remaining_len = pending.items.len - pos;
    if (remaining_len > 0) {
        std.mem.copyForwards(u8, pending.items[0..remaining_len], pending.items[pos..]);
    }
    pending.items.len = remaining_len;
}

fn consumeReplayRecordsFromMixedWal(
    allocator: Allocator,
    pending: *std.ArrayListUnmanaged(u8),
    from_sequence: u64,
    required_hint_mask: u8,
    ctx: anytype,
    comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    stats: *ReplayStreamStats,
) !void {
    var pos: usize = 0;
    while (pending.items.len - pos >= 4) {
        const magic = std.mem.readInt(u32, pending.items[pos..][0..4], .little);
        if (magic == record_magic) {
            if (pending.items.len - pos < record_header_len) break;
            const header = pending.items[pos..][0..record_header_len];
            const version = std.mem.readInt(u16, header[4..6], .little);
            if (version != record_version) return error.UnsupportedLsmWalVersion;
            const header_len = std.mem.readInt(u16, header[6..8], .little);
            if (header_len != record_header_len) return error.UnsupportedLsmWalHeader;
            const payload_len: usize = @intCast(std.mem.readInt(u32, header[8..12], .little));
            const total_len = record_header_len + payload_len;
            if (pending.items.len - pos < total_len) break;
            pos += total_len;
            continue;
        }
        if (magic == replay_record_magic) {
            if (pending.items.len - pos < replay_record_header_len) break;
            const header = pending.items[pos..][0..replay_record_header_len];
            const version = std.mem.readInt(u16, header[4..6], .little);
            if (version != replay_record_version) return error.UnsupportedLsmWalVersion;
            const header_len = std.mem.readInt(u16, header[6..8], .little);
            if (header_len != replay_record_header_len) return error.UnsupportedLsmWalHeader;
            const sequence = std.mem.readInt(u64, header[8..16], .little);
            const payload_len: usize = @intCast(std.mem.readInt(u32, header[16..20], .little));
            const expected_crc = std.mem.readInt(u32, header[20..24], .little);
            const total_len = replay_record_header_len + payload_len;
            if (pending.items.len - pos < total_len) break;
            if (sequence >= from_sequence) {
                const payload = pending.items[pos + replay_record_header_len .. pos + total_len];
                if (Crc32.hash(payload) != expected_crc) {
                    logCorruptWalDetail("mixed_replay_crc", 0, pos, pending.items.len, "mixed WAL replay CRC mismatch");
                    return error.CorruptLsmWal;
                }
                if (required_hint_mask != 0 and !(try change_journal_mod.encodedRecordMatchesHintMask(payload, required_hint_mask))) {
                    pos += total_len;
                    continue;
                }
                _ = allocator;
                try callback(ctx, sequence, payload);
                stats.records += 1;
            }
            pos += total_len;
            continue;
        }
        logCorruptWalDetail("mixed_record_magic", 0, pos, pending.items.len, "unknown mixed WAL record magic");
        return error.CorruptLsmWal;
    }

    if (pos == 0) return;
    const remaining_len = pending.items.len - pos;
    if (remaining_len > 0) {
        std.mem.copyForwards(u8, pending.items[0..remaining_len], pending.items[pos..]);
    }
    pending.items.len = remaining_len;
}

fn replayFileStreaming(
    storage: storage_io.Storage,
    allocator: Allocator,
    wal_path: []const u8,
    segment: u64,
    mutable: anytype,
    stats: *ReplayStats,
    hooks: ?ReplayHooks,
    options: ReplayFileOptions,
    working_set: *ReplayWorkingSetTracker,
    retained_cap_bytes: usize,
) !void {
    const file_size = storage.fileSize(wal_path) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    if (file_size == 0) return;
    stats.segments += 1;
    stats.bytes += file_size;

    var pending = std.ArrayListUnmanaged(u8).empty;
    defer pending.deinit(allocator);
    try pending.ensureTotalCapacityPrecise(allocator, retained_cap_bytes);
    working_set.observePending(&pending);
    defer working_set.observe(0);

    var offset: u64 = 0;
    while (offset < file_size) {
        const len: usize = @intCast(@min(@as(u64, replay_chunk_bytes), file_size - offset));
        const start = pending.items.len;
        try pending.ensureUnusedCapacity(allocator, len);
        pending.items.len += len;
        working_set.observePending(&pending);
        errdefer pending.items.len = start;
        try storage.readFileRangeInto(allocator, wal_path, offset, pending.items[start..][0..len]);
        const final_chunk = offset + len >= file_size;
        offset += len;
        consumeCompleteRecords(allocator, &pending, segment, mutable, stats, hooks) catch |err| switch (err) {
            error.TruncatedLsmWalTailJunk => {
                if (!options.allow_corrupt_tail or !final_chunk) {
                    std.log.warn("lsm wal replay corrupt path={s} segment={} offset={} final_chunk={} pending_len={}", .{
                        wal_path,
                        segment,
                        offset,
                        final_chunk,
                        pending.items.len,
                    });
                    return err;
                }
                std.log.warn("lsm wal replay truncated corrupt tail path={s} segment={} bytes={}", .{
                    wal_path,
                    segment,
                    pending.items.len,
                });
                stats.truncated_tail_bytes += @intCast(pending.items.len);
                pending.clearRetainingCapacity();
                releaseOversizedReplayPendingBuffer(allocator, &pending, retained_cap_bytes);
                working_set.observePending(&pending);
                break;
            },
            else => return err,
        };
        releaseOversizedReplayPendingBuffer(allocator, &pending, retained_cap_bytes);
        working_set.observePending(&pending);
    }
    if (pending.items.len > 0) {
        stats.truncated_tail_bytes += @intCast(pending.items.len);
    }
}

fn logCorruptWalDetail(site: []const u8, segment: u64, pos: usize, pending_len: usize, detail: []const u8) void {
    std.log.warn("lsm wal corrupt site={s} segment={} pos={} pending_len={} detail={s}", .{
        site,
        segment,
        pos,
        pending_len,
        detail,
    });
}

fn consumeCompleteRecords(
    allocator: Allocator,
    pending: *std.ArrayListUnmanaged(u8),
    segment: u64,
    mutable: anytype,
    stats: *ReplayStats,
    hooks: ?ReplayHooks,
) !void {
    var pos: usize = 0;
    while (pending.items.len - pos >= 4) {
        const magic = std.mem.readInt(u32, pending.items[pos..][0..4], .little);
        if (magic == record_magic) {
            if (pending.items.len - pos < record_header_len) break;
            const header = pending.items[pos..][0..record_header_len];
            const version = std.mem.readInt(u16, header[4..6], .little);
            if (version != record_version) return error.UnsupportedLsmWalVersion;
            const header_len = std.mem.readInt(u16, header[6..8], .little);
            if (header_len != record_header_len) return error.UnsupportedLsmWalHeader;
            const payload_len: usize = @intCast(std.mem.readInt(u32, header[8..12], .little));
            const expected_crc = std.mem.readInt(u32, header[12..16], .little);
            const total_len = record_header_len + payload_len;
            if (pending.items.len - pos < total_len) break;

            const payload = pending.items[pos + record_header_len .. pos + total_len];
            if (Crc32.hash(payload) != expected_crc) {
                logCorruptWalDetail("record_crc", segment, pos, pending.items.len, "state record CRC mismatch");
                return error.CorruptLsmWal;
            }
            const apply_allocator = if (hooks) |active_hooks|
                if (active_hooks.entry_allocator) |entry_allocator_hook|
                    try entry_allocator_hook(active_hooks.ctx, allocator)
                else
                    allocator
            else
                allocator;
            const applied = decodePayloadIntoMutableWithHooks(apply_allocator, payload, mutable, hooks, segment) catch |err| switch (err) {
                error.CorruptLsmWal => {
                    logCorruptWalDetail("record_decode", segment, pos, pending.items.len, "state record payload decode failed");
                    return err;
                },
                else => return err,
            };
            if (hooks) |active_hooks| {
                try active_hooks.on_applied_record(active_hooks.ctx, segment, applied);
            }
            stats.records += 1;
            stats.entries += applied;
            pos += total_len;
            continue;
        }
        if (magic == replay_record_magic) {
            if (pending.items.len - pos < replay_record_header_len) break;
            const header = pending.items[pos..][0..replay_record_header_len];
            const version = std.mem.readInt(u16, header[4..6], .little);
            if (version != replay_record_version) return error.UnsupportedLsmWalVersion;
            const header_len = std.mem.readInt(u16, header[6..8], .little);
            if (header_len != replay_record_header_len) return error.UnsupportedLsmWalHeader;
            const payload_len: usize = @intCast(std.mem.readInt(u32, header[16..20], .little));
            const expected_crc = std.mem.readInt(u32, header[20..24], .little);
            const total_len = replay_record_header_len + payload_len;
            if (pending.items.len - pos < total_len) break;
            const payload = pending.items[pos + replay_record_header_len .. pos + total_len];
            if (Crc32.hash(payload) != expected_crc) {
                logCorruptWalDetail("replay_record_crc", segment, pos, pending.items.len, "replay record CRC mismatch");
                return error.CorruptLsmWal;
            }
            pos += total_len;
            continue;
        }
        logCorruptWalDetail("record_magic", segment, pos, pending.items.len, "unknown WAL record magic");
        if (pos == 0) return error.CorruptLsmWal;
        const remaining_len = pending.items.len - pos;
        if (remaining_len < @min(record_header_len, replay_record_header_len)) {
            return error.TruncatedLsmWalTailJunk;
        }
        return error.CorruptLsmWal;
    }

    if (pos == 0) return;
    const remaining_len = pending.items.len - pos;
    if (remaining_len > 0) {
        std.mem.copyForwards(u8, pending.items[0..remaining_len], pending.items[pos..]);
    }
    pending.items.len = remaining_len;
}

fn legacyPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, legacy_wal_file_name });
}

fn walDirPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, wal_dir_name });
}

fn replayDirPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, wal_dir_name, replay_dir_name });
}

fn indexPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, wal_dir_name, wal_index_file_name });
}

fn checkpointPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, wal_dir_name, wal_checkpoint_file_name });
}

fn replayIndexPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, wal_dir_name, replay_index_file_name });
}

fn mainReplaySegmentsPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, wal_dir_name, main_replay_segments_file_name });
}

fn replaySegmentsPathAlloc(allocator: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(allocator, &.{ root_dir, wal_dir_name, replay_segments_file_name });
}

fn segmentPathAlloc(allocator: Allocator, root_dir: []const u8, segment: u64) ![]u8 {
    return try std.fmt.allocPrint(allocator, "{s}/{s}/{d:0>20}.log", .{ root_dir, wal_dir_name, segment });
}

fn replaySegmentPathAlloc(allocator: Allocator, root_dir: []const u8, segment: u64) ![]u8 {
    return try std.fmt.allocPrint(allocator, "{s}/{s}/{s}/{d:0>20}.log", .{ root_dir, wal_dir_name, replay_dir_name, segment });
}

fn appendWalSegmentPathPrefix(
    out: *std.ArrayListUnmanaged(u8),
    allocator: Allocator,
    root_dir: []const u8,
) !usize {
    out.clearRetainingCapacity();
    try out.appendSlice(allocator, root_dir);
    try out.append(allocator, '/');
    try out.appendSlice(allocator, wal_dir_name);
    try out.append(allocator, '/');
    return out.items.len;
}

fn appendReplaySegmentPathPrefix(
    out: *std.ArrayListUnmanaged(u8),
    allocator: Allocator,
    root_dir: []const u8,
) !usize {
    out.clearRetainingCapacity();
    try out.appendSlice(allocator, root_dir);
    try out.append(allocator, '/');
    try out.appendSlice(allocator, wal_dir_name);
    try out.append(allocator, '/');
    try out.appendSlice(allocator, replay_dir_name);
    try out.append(allocator, '/');
    return out.items.len;
}

fn appendSegmentPathSuffix(
    out: *std.ArrayListUnmanaged(u8),
    allocator: Allocator,
    prefix_len: usize,
    segment: u64,
) ![]const u8 {
    var segment_name_buf: [32]u8 = undefined;
    const segment_name = try std.fmt.bufPrint(&segment_name_buf, "{d:0>20}.log", .{segment});
    out.items.len = prefix_len;
    try out.appendSlice(allocator, segment_name);
    return out.items;
}

fn encodeControl(out: []u8, kind: ControlKind, payload: []const u8) void {
    std.debug.assert(out.len == control_overhead + payload.len);
    @memcpy(out[0..control_magic.len], control_magic);
    std.mem.writeInt(u16, out[8..10], control_version, .little);
    out[10] = @backingInt(kind);
    out[11] = 0;
    std.mem.writeInt(u32, out[12..16], @intCast(payload.len), .little);
    @memcpy(out[control_header_len .. control_header_len + payload.len], payload);
    std.mem.writeInt(
        u32,
        out[out.len - control_checksum_len ..][0..control_checksum_len],
        Crc32.hash(out[0 .. out.len - control_checksum_len]),
        .little,
    );
}

fn decodeControl(raw: []const u8, kind: ControlKind, payload_len: usize) ![]const u8 {
    if (raw.len != control_overhead + payload_len) return error.CorruptLsmWalIndex;
    if (!std.mem.eql(u8, raw[0..control_magic.len], control_magic)) return error.CorruptLsmWalIndex;
    if (std.mem.readInt(u16, raw[8..10], .little) != control_version) return error.CorruptLsmWalIndex;
    if (raw[10] != @backingInt(kind) or raw[11] != 0) return error.CorruptLsmWalIndex;
    if (std.mem.readInt(u32, raw[12..16], .little) != payload_len) return error.CorruptLsmWalIndex;
    const expected_checksum = std.mem.readInt(
        u32,
        raw[raw.len - control_checksum_len ..][0..control_checksum_len],
        .little,
    );
    if (Crc32.hash(raw[0 .. raw.len - control_checksum_len]) != expected_checksum)
        return error.CorruptLsmWalIndex;
    return raw[control_header_len .. control_header_len + payload_len];
}

const CommittedSegmentEntry = struct {
    segment: u64,
    first_sequence: u64,
};

fn encodeCommittedSegmentEntry(kind: ControlKind, segment: u64, sequence: u64) [committed_segment_entry_len]u8 {
    var payload: [16]u8 = undefined;
    std.mem.writeInt(u64, payload[0..8], segment, .little);
    std.mem.writeInt(u64, payload[8..16], sequence, .little);
    var raw: [committed_segment_entry_len]u8 = undefined;
    encodeControl(&raw, kind, &payload);
    return raw;
}

fn decodeCommittedSegmentEntry(raw: []const u8, kind: ControlKind) !CommittedSegmentEntry {
    const payload = try decodeControl(raw, kind, 16);
    const entry = CommittedSegmentEntry{
        .segment = std.mem.readInt(u64, payload[0..8], .little),
        .first_sequence = std.mem.readInt(u64, payload[8..16], .little),
    };
    if (entry.segment == 0 or entry.first_sequence == 0) return error.CorruptLsmWalIndex;
    return entry;
}

fn appendMainReplaySegmentEntryIfNeeded(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    segment: u64,
    sequence: u64,
    allow_create: bool,
) !void {
    const temp_allocator = allocator;
    const segments_path = try mainReplaySegmentsPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(segments_path);

    const size = storage.fileSize(segments_path) catch |err| switch (err) {
        error.FileNotFound => {
            if (!allow_create) return;
            const raw = encodeCommittedSegmentEntry(.main_replay_segment, segment, sequence);
            try replaceFileAtomically(storage, allocator, segments_path, &raw);
            return;
        },
        else => return err,
    };
    if (size == 0) {
        if (!allow_create) return;
        const raw = encodeCommittedSegmentEntry(.main_replay_segment, segment, sequence);
        try replaceFileAtomically(storage, allocator, segments_path, &raw);
        return;
    }
    if (size % committed_segment_entry_len != 0) return error.CorruptLsmWalIndex;

    const tail = try storage.readFileRangeAlloc(
        temp_allocator,
        segments_path,
        size - committed_segment_entry_len,
        committed_segment_entry_len,
    );
    defer temp_allocator.free(tail);
    if (tail.len != committed_segment_entry_len) return error.CorruptLsmWalIndex;
    const last = try decodeCommittedSegmentEntry(tail, .main_replay_segment);
    if (last.segment == segment) return;

    const raw = encodeCommittedSegmentEntry(.main_replay_segment, segment, sequence);
    try storage.appendFileAbsolute(allocator, segments_path, &raw, true);
}

fn appendReplaySegmentEntryIfNeeded(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    segment: u64,
    sequence: u64,
    allow_create: bool,
) !void {
    const temp_allocator = allocator;
    const segments_path = try replaySegmentsPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(segments_path);

    const size = storage.fileSize(segments_path) catch |err| switch (err) {
        error.FileNotFound => {
            if (!allow_create) return;
            const raw = encodeCommittedSegmentEntry(.replay_segment, segment, sequence);
            try replaceFileAtomically(storage, allocator, segments_path, &raw);
            return;
        },
        else => return err,
    };
    if (size == 0) {
        if (!allow_create) return;
        const raw = encodeCommittedSegmentEntry(.replay_segment, segment, sequence);
        try replaceFileAtomically(storage, allocator, segments_path, &raw);
        return;
    }
    if (size % committed_segment_entry_len != 0) return error.CorruptLsmWalIndex;

    const tail = try storage.readFileRangeAlloc(
        temp_allocator,
        segments_path,
        size - committed_segment_entry_len,
        committed_segment_entry_len,
    );
    defer temp_allocator.free(tail);
    if (tail.len != committed_segment_entry_len) return error.CorruptLsmWalIndex;
    const last = try decodeCommittedSegmentEntry(tail, .replay_segment);
    if (last.segment == segment) return;

    const raw = encodeCommittedSegmentEntry(.replay_segment, segment, sequence);
    try storage.appendFileAbsolute(allocator, segments_path, &raw, true);
}

fn writeDedicatedReplayMarker(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
) !void {
    const temp_allocator = allocator;
    const segments_path = try mainReplaySegmentsPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(segments_path);
    try replaceFileAtomically(storage, allocator, segments_path, "");
}

fn replayMainWalStartSegment(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    from_sequence: u64,
) !u64 {
    const temp_allocator = allocator;
    const segments_path = try mainReplaySegmentsPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(segments_path);

    const size = storage.fileSize(segments_path) catch |err| switch (err) {
        error.FileNotFound => return 1,
        else => return err,
    };
    if (size == 0) return try replayStartSegment(storage, allocator, root_dir, from_sequence);
    if (size % committed_segment_entry_len != 0) return error.CorruptLsmWalIndex;

    const raw = try storage.readFileRangeAlloc(temp_allocator, segments_path, 0, @intCast(size));
    defer temp_allocator.free(raw);
    if (raw.len != size) return error.CorruptLsmWalIndex;

    var start_segment: u64 = 1;
    var pos: usize = 0;
    while (pos < raw.len) : (pos += committed_segment_entry_len) {
        const entry = try decodeCommittedSegmentEntry(
            raw[pos..][0..committed_segment_entry_len],
            .main_replay_segment,
        );
        if (entry.first_sequence > from_sequence) break;
        start_segment = entry.segment;
    }
    return start_segment;
}

fn replayStartSegment(
    storage: storage_io.Storage,
    allocator: Allocator,
    root_dir: []const u8,
    from_sequence: u64,
) !u64 {
    const temp_allocator = allocator;
    const segments_path = try replaySegmentsPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(segments_path);

    const size = storage.fileSize(segments_path) catch |err| switch (err) {
        error.FileNotFound => return 1,
        else => return err,
    };
    if (size == 0) return 1;
    if (size % committed_segment_entry_len != 0) return error.CorruptLsmWalIndex;

    const raw = try storage.readFileRangeAlloc(temp_allocator, segments_path, 0, @intCast(size));
    defer temp_allocator.free(raw);
    if (raw.len != size) return error.CorruptLsmWalIndex;

    var start_segment: u64 = 1;
    var pos: usize = 0;
    while (pos < raw.len) : (pos += committed_segment_entry_len) {
        const entry = try decodeCommittedSegmentEntry(
            raw[pos..][0..committed_segment_entry_len],
            .replay_segment,
        );
        if (entry.first_sequence > from_sequence) break;
        start_segment = entry.segment;
    }
    return start_segment;
}

const CurrentSegment = struct {
    segment: u64,
    size: u64,
    index_exists: bool,
};

const ReadCheckpoint = struct {
    oldest_retained_segment: u64 = 1,
    covered_through_segment: u64 = 0,
    exists: bool = false,
};

const CheckpointIndex = struct {
    oldest_retained_segment: u64,
    covered_through_segment: u64,
};

fn readCurrentSegment(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !CurrentSegment {
    var segment = readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => return .{ .segment = 1, .size = 0, .index_exists = false },
        else => return err,
    };
    // The index's size field is advisory for backwards compatibility. Derive
    // the active size from the segment itself so a normal append needs only the
    // segment fsync; the atomic index is published only when the segment changes.
    const segment_path = try segmentPathAlloc(allocator, root_dir, segment.segment);
    defer allocator.free(segment_path);
    segment.size = storage.fileSize(segment_path) catch |err| switch (err) {
        error.FileNotFound => 0,
        else => return err,
    };
    return segment;
}

fn readCurrentSegmentIfPresent(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !CurrentSegment {
    const temp_allocator = allocator;
    const index_path = try indexPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(index_path);
    const index_size = try storage.fileSize(index_path);
    if (index_size != control_pair_len) return error.CorruptLsmWalIndex;
    var raw: [control_pair_len]u8 = undefined;
    try storage.readFileRangeInto(temp_allocator, index_path, 0, &raw);
    const payload = try decodeControl(&raw, .current_segment, 16);
    const segment = std.mem.readInt(u64, payload[0..8], .little);
    if (segment == 0) return error.CorruptLsmWalIndex;
    return .{
        .segment = segment,
        .size = std.mem.readInt(u64, payload[8..16], .little),
        .index_exists = true,
    };
}

fn writeCurrentSegment(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8, segment: u64, size: u64) !void {
    var payload: [16]u8 = undefined;
    std.mem.writeInt(u64, payload[0..8], segment, .little);
    std.mem.writeInt(u64, payload[8..16], size, .little);
    var raw: [control_pair_len]u8 = undefined;
    encodeControl(&raw, .current_segment, &payload);
    const temp_allocator = allocator;
    const index_path = try indexPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(index_path);
    try replaceFileAtomically(storage, allocator, index_path, &raw);
}

fn readCheckpointIndex(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !ReadCheckpoint {
    const temp_allocator = allocator;
    const checkpoint_path = try checkpointPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(checkpoint_path);
    const index_size = storage.fileSize(checkpoint_path) catch |err| switch (err) {
        error.FileNotFound => return .{},
        else => return err,
    };
    if (index_size != control_pair_len) return error.CorruptLsmWalIndex;
    var raw: [control_pair_len]u8 = undefined;
    try storage.readFileRangeInto(temp_allocator, checkpoint_path, 0, &raw);
    const payload = try decodeControl(&raw, .checkpoint, 16);
    const oldest_retained_segment = std.mem.readInt(u64, payload[0..8], .little);
    const covered_through_segment = std.mem.readInt(u64, payload[8..16], .little);
    if (oldest_retained_segment == 0) return error.CorruptLsmWalIndex;
    if (covered_through_segment + 1 < oldest_retained_segment) return error.CorruptLsmWalIndex;
    return .{
        .oldest_retained_segment = oldest_retained_segment,
        .covered_through_segment = covered_through_segment,
        .exists = true,
    };
}

fn writeCheckpointIndex(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8, index: CheckpointIndex) !void {
    if (index.oldest_retained_segment == 0) return error.CorruptLsmWalIndex;
    if (index.covered_through_segment + 1 < index.oldest_retained_segment) return error.CorruptLsmWalIndex;
    var payload: [16]u8 = undefined;
    std.mem.writeInt(u64, payload[0..8], index.oldest_retained_segment, .little);
    std.mem.writeInt(u64, payload[8..16], index.covered_through_segment, .little);
    var raw: [control_pair_len]u8 = undefined;
    encodeControl(&raw, .checkpoint, &payload);
    const temp_allocator = allocator;
    const checkpoint_path = try checkpointPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(checkpoint_path);
    try replaceFileAtomically(storage, allocator, checkpoint_path, &raw);
}

const ReadReplayIndex = struct {
    current_segment: u64 = 1,
    next_sequence: u64 = 1,
    truncated_through: u64 = 0,
    exists: bool = false,
};

fn readReplayIndex(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !ReadReplayIndex {
    const temp_allocator = allocator;
    const index_path = try replayIndexPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(index_path);
    const size = storage.fileSize(index_path) catch |err| switch (err) {
        error.FileNotFound => return .{},
        else => return err,
    };
    if (size != control_triple_len) return error.CorruptLsmWalIndex;
    var raw: [control_triple_len]u8 = undefined;
    storage.readFileRangeInto(temp_allocator, index_path, 0, &raw) catch |err| switch (err) {
        error.EndOfStream => return error.CorruptLsmWalIndex,
        else => return err,
    };
    const payload = try decodeControl(&raw, .replay_index, 24);
    const current_segment = std.mem.readInt(u64, payload[0..8], .little);
    const next_sequence = std.mem.readInt(u64, payload[8..16], .little);
    const truncated_through = std.mem.readInt(u64, payload[16..24], .little);
    if (current_segment == 0 or next_sequence == 0) return error.CorruptLsmWalIndex;
    return .{
        .current_segment = current_segment,
        .next_sequence = next_sequence,
        .truncated_through = truncated_through,
        .exists = true,
    };
}

fn writeReplayIndex(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8, index: ReplayIndex) !void {
    var payload: [24]u8 = undefined;
    std.mem.writeInt(u64, payload[0..8], index.current_segment, .little);
    std.mem.writeInt(u64, payload[8..16], index.next_sequence, .little);
    std.mem.writeInt(u64, payload[16..24], index.truncated_through, .little);
    var raw: [control_triple_len]u8 = undefined;
    encodeControl(&raw, .replay_index, &payload);
    const wal_dir = try walDirPathAlloc(allocator, root_dir);
    defer allocator.free(wal_dir);
    try storage.createDirPath(wal_dir);
    const temp_allocator = allocator;
    const index_path = try replayIndexPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(index_path);
    try replaceFileAtomically(storage, allocator, index_path, &raw);
    _ = replay_index_epoch.fetchAdd(1, .monotonic);
}

fn replaceFileAtomically(storage: storage_io.Storage, allocator: Allocator, path: []const u8, contents: []const u8) !void {
    var writer = try storage.beginAtomicWrite(allocator, path);
    var active = true;
    errdefer if (active) writer.abort();
    try writer.appendSlice(contents);
    active = false;
    try writer.finish();
}

fn encodePayload(allocator: Allocator, out: *std.ArrayListUnmanaged(u8), state: anytype) !void {
    try appendInt(out, allocator, u32, @intCast(state.entryCount()));
    for (0..state.entryCount()) |entry_index| {
        const entry = state.entryAt(entry_index);
        const ns = entry.namespace_name orelse "";
        const flags: u8 = @as(u8, @intFromBool(entry.tombstone)) |
            (@as(u8, @intFromBool(entry.namespace_name != null)) << 1);
        try out.append(allocator, flags);
        try out.appendNTimes(allocator, 0, 3);
        try appendInt(out, allocator, u32, @intCast(ns.len));
        try appendInt(out, allocator, u32, @intCast(entry.key.len));
        try appendInt(out, allocator, u32, @intCast(entry.value.len));
        try out.appendSlice(allocator, ns);
        try out.appendSlice(allocator, entry.key);
        try out.appendSlice(allocator, entry.value);
    }
}

fn encodePayloadIntoSlice(out: []u8, state: anytype) !void {
    var pos: usize = 0;
    writeIntToPayload(out, &pos, u32, @intCast(state.entryCount()));
    for (0..state.entryCount()) |entry_index| {
        const entry = state.entryAt(entry_index);
        const ns = entry.namespace_name orelse "";
        const flags: u8 = @as(u8, @intFromBool(entry.tombstone)) |
            (@as(u8, @intFromBool(entry.namespace_name != null)) << 1);
        if (out.len - pos < entry_header_len + ns.len + entry.key.len + entry.value.len) return error.BufferTooSmall;
        out[pos] = flags;
        out[pos + 1] = 0;
        out[pos + 2] = 0;
        out[pos + 3] = 0;
        pos += 4;
        writeIntToPayload(out, &pos, u32, @intCast(ns.len));
        writeIntToPayload(out, &pos, u32, @intCast(entry.key.len));
        writeIntToPayload(out, &pos, u32, @intCast(entry.value.len));
        @memcpy(out[pos..][0..ns.len], ns);
        pos += ns.len;
        @memcpy(out[pos..][0..entry.key.len], entry.key);
        pos += entry.key.len;
        @memcpy(out[pos..][0..entry.value.len], entry.value);
        pos += entry.value.len;
    }
    if (pos != out.len) return error.BufferTooSmall;
}

fn writeIntToPayload(out: []u8, pos: *usize, comptime T: type, value: T) void {
    const len = @divExact(@typeInfo(T).int.bits, 8);
    std.mem.writeInt(T, out[pos.*..][0..len], value, .little);
    pos.* += len;
}

fn encodedPayloadLen(state: anytype) usize {
    var total: usize = @sizeOf(u32);
    for (0..state.entryCount()) |entry_index| {
        const entry = state.entryAt(entry_index);
        const ns = entry.namespace_name orelse "";
        total += entry_header_len + ns.len + entry.key.len + entry.value.len;
    }
    return total;
}

fn decodePayloadIntoMutable(allocator: Allocator, payload: []const u8, mutable: anytype) !u64 {
    return try decodePayloadIntoMutableWithHooks(allocator, payload, mutable, null, 0);
}

fn decodePayloadIntoMutableWithHooks(
    allocator: Allocator,
    payload: []const u8,
    mutable: anytype,
    hooks: ?ReplayHooks,
    segment: u64,
) !u64 {
    var pos: usize = 0;
    const entry_count = try readInt(payload, &pos, u32);
    var applied: u64 = 0;
    var i: u32 = 0;
    while (i < entry_count) : (i += 1) {
        if (payload.len - pos < entry_header_len) return error.CorruptLsmWal;
        const flags = payload[pos];
        pos += 4;
        const ns_len: usize = @intCast(try readInt(payload, &pos, u32));
        const key_len: usize = @intCast(try readInt(payload, &pos, u32));
        const value_len: usize = @intCast(try readInt(payload, &pos, u32));
        if (ns_len > payload.len - pos) return error.CorruptLsmWal;
        const ns = payload[pos .. pos + ns_len];
        pos += ns_len;
        if (key_len > payload.len - pos) return error.CorruptLsmWal;
        const key = payload[pos .. pos + key_len];
        pos += key_len;
        if (value_len > payload.len - pos) return error.CorruptLsmWal;
        const value = payload[pos .. pos + value_len];
        pos += value_len;

        const namespace = backend_types.Namespace{ .name = if ((flags & 0x02) != 0) ns else null };
        try mutable.upsert(allocator, namespace, key, value, (flags & 0x01) != 0);
        applied += 1;
        if (hooks) |active_hooks| {
            if (active_hooks.on_applied_entry) |on_applied_entry| {
                try on_applied_entry(active_hooks.ctx, segment, @intCast(ns.len + key.len + value.len));
            }
        }
    }
    if (pos != payload.len) return error.CorruptLsmWal;
    return applied;
}

fn appendInt(out: *std.ArrayListUnmanaged(u8), allocator: Allocator, comptime T: type, value: T) !void {
    var bytes: [@divExact(@typeInfo(T).int.bits, 8)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try out.appendSlice(allocator, &bytes);
}

fn readInt(bytes: []const u8, pos: *usize, comptime T: type) !T {
    const len = @divExact(@typeInfo(T).int.bits, 8);
    if (bytes.len - pos.* < len) return error.CorruptLsmWal;
    const value = std.mem.readInt(T, bytes[pos.*..][0..len], .little);
    pos.* += len;
    return value;
}

test "lsm wal replay pending scratch releases oversized retained capacity" {
    var pending = std.ArrayListUnmanaged(u8).empty;
    defer pending.deinit(std.testing.allocator);

    try pending.ensureTotalCapacityPrecise(std.testing.allocator, default_replay_scratch_retained_cap_bytes * 2);
    try std.testing.expect(pending.capacity > default_replay_scratch_retained_cap_bytes);

    pending.items.len = default_replay_scratch_retained_cap_bytes + 1;
    releaseOversizedReplayPendingBuffer(std.testing.allocator, &pending, default_replay_scratch_retained_cap_bytes);
    try std.testing.expect(pending.capacity > default_replay_scratch_retained_cap_bytes);

    pending.items.len = 8;
    releaseOversizedReplayPendingBuffer(std.testing.allocator, &pending, default_replay_scratch_retained_cap_bytes);
    try std.testing.expectEqual(@as(usize, 8), pending.items.len);
    try std.testing.expect(pending.capacity <= default_replay_scratch_retained_cap_bytes);
}

test "lsm wal encodes and replays state" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-test";
    try storage.storage().createDirPath(root_dir);

    var state: State = .{};
    defer state.deinit(std.testing.allocator);
    try state.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);
    try state.upsert(std.testing.allocator, .{}, "b", "", true);

    _ = try appendState(storage.storage(), std.testing.allocator, root_dir, &state, false);

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    const stats = try replayIntoMutable(storage.storage(), std.testing.allocator, root_dir, &replayed);
    try std.testing.expectEqual(@as(u64, 1), stats.records);
    try std.testing.expectEqual(@as(u64, 2), stats.entries);
    try std.testing.expectEqualStrings("A", try replayed.get(.{ .name = "docs" }, "a"));
    try std.testing.expectError(error.NotFound, replayed.get(.{}, "b"));
}

test "lsm wal replay reads chunks into bounded pending buffer" {
    const CountingStorage = struct {
        backing: *storage_io.MemoryStorage,
        range_alloc_reads: usize = 0,
        range_alloc_log_reads: usize = 0,
        range_into_reads: usize = 0,
        range_into_log_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.range_alloc_reads += 1;
            if (std.mem.endsWith(u8, path, ".log")) self.range_alloc_log_reads += 1;
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn readFileRangeInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.range_into_reads += 1;
            if (std.mem.endsWith(u8, path, ".log")) self.range_into_log_reads += 1;
            return self.backing.storage().readFileRangeInto(std.testing.allocator, path, offset, out);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().appendFileAbsolute(std.testing.allocator, path, contents, sync);
        }

        fn syncFileContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncFileContentsAbsolute(path);
        }

        fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().syncParentAbsolute(path);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    const vtable: storage_io.Storage.VTable = .{
        .create_dir_path = CountingStorage.createDirPath,
        .read_file_alloc = CountingStorage.readFileAlloc,
        .read_file_range_alloc = CountingStorage.readFileRangeAlloc,
        .read_file_range_into = CountingStorage.readFileRangeInto,
        .file_size = CountingStorage.fileSize,
        .read_file_trailer_alloc = CountingStorage.readFileTrailerAlloc,
        .write_file_absolute = CountingStorage.writeFileAbsolute,
        .append_file_absolute = CountingStorage.appendFileAbsolute,
        .sync_contents_absolute = CountingStorage.syncFileContentsAbsolute,
        .sync_parent_absolute = CountingStorage.syncParentAbsolute,
        .rename_is_atomic = true,
        .rename_absolute = CountingStorage.renameAbsolute,
        .delete_file_absolute = CountingStorage.deleteFileAbsolute,
        .delete_tree = CountingStorage.deleteTree,
        .now_ns = CountingStorage.nowNs,
    };

    var backing = storage_io.MemoryStorage.init(std.testing.allocator);
    defer backing.deinit();
    var counting = CountingStorage{ .backing = &backing };
    const storage = storage_io.HostStorage.init(&counting, &vtable).storage();
    const root_dir = "/wal-replay-bounded-buffer-test";
    try storage.createDirPath(root_dir);

    var state: State = .{};
    defer state.deinit(std.testing.allocator);
    for (0..64) |i| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "doc:{d:0>3}", .{i});
        try state.upsert(std.testing.allocator, .{ .name = "docs" }, key, "value", false);
    }
    _ = try appendStateWithOptions(storage, std.testing.allocator, root_dir, &state, false, .{ .segment_bytes = 256 });

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    const stats = try replayIntoMutable(storage, std.testing.allocator, root_dir, &replayed);
    try std.testing.expectEqual(@as(u64, 1), stats.records);
    try std.testing.expectEqual(@as(u64, 64), stats.entries);
    try std.testing.expect(counting.range_into_reads > 0);
    try std.testing.expect(counting.range_into_log_reads > 0);
    try std.testing.expectEqual(@as(usize, 0), counting.range_alloc_log_reads);
}

test "lsm wal rotates small segments and replays all records" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-rotation-test";
    try storage.storage().createDirPath(root_dir);

    var first: State = .{};
    defer first.deinit(std.testing.allocator);
    try first.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);

    var second: State = .{};
    defer second.deinit(std.testing.allocator);
    try second.upsert(std.testing.allocator, .{ .name = "docs" }, "b", "B", false);

    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &first, false, .{ .segment_bytes = 32 });
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &second, false, .{ .segment_bytes = 32 });

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    const stats = try replayIntoMutable(storage.storage(), std.testing.allocator, root_dir, &replayed);
    try std.testing.expect(stats.segments > 1);
    try std.testing.expectEqual(@as(u64, 2), stats.records);
    try std.testing.expectEqual(@as(u64, 2), stats.entries);
    try std.testing.expectEqualStrings("A", try replayed.get(.{ .name = "docs" }, "a"));
    try std.testing.expectEqualStrings("B", try replayed.get(.{ .name = "docs" }, "b"));
}

test "lsm wal derives current segment size without republishing the index" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-current-segment-size-test";
    try storage.storage().createDirPath(root_dir);

    var state: State = .{};
    defer state.deinit(std.testing.allocator);
    try state.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);

    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &state, false, .{});

    const index_path = try indexPathAlloc(std.testing.allocator, root_dir);
    defer std.testing.allocator.free(index_path);
    try std.testing.expectEqual(@as(u64, control_pair_len), try storage.storage().fileSize(index_path));

    const segment_path = try segmentPathAlloc(std.testing.allocator, root_dir, 1);
    defer std.testing.allocator.free(segment_path);
    const current = try readCurrentSegment(storage.storage(), std.testing.allocator, root_dir);
    try std.testing.expectEqual(@as(u64, 1), current.segment);
    try std.testing.expectEqual(try storage.storage().fileSize(segment_path), current.size);

    const raw_index = try storage.storage().readFileAlloc(std.testing.allocator, index_path, control_pair_len);
    defer std.testing.allocator.free(raw_index);
    const payload = try decodeControl(raw_index, .current_segment, 16);
    try std.testing.expectEqual(@as(u64, 0), std.mem.readInt(u64, payload[8..16], .little));
}

test "lsm wal control indexes reject plausible checksummed corruption" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-control-checksum-test";
    try storage.storage().createDirPath(root_dir);

    const Cases = struct {
        fn corruptFile(
            store: storage_io.Storage,
            path: []const u8,
            len: usize,
        ) !void {
            const raw = try store.readFileAlloc(std.testing.allocator, path, len);
            defer std.testing.allocator.free(raw);
            raw[control_header_len] ^= 0x40;
            try store.writeFileAbsolute(path, raw);
        }
    };

    try writeCurrentSegment(storage.storage(), std.testing.allocator, root_dir, 7, 4096);
    const current_path = try indexPathAlloc(std.testing.allocator, root_dir);
    defer std.testing.allocator.free(current_path);
    try Cases.corruptFile(storage.storage(), current_path, control_pair_len);
    try std.testing.expectError(
        error.CorruptLsmWalIndex,
        readCurrentSegmentIfPresent(storage.storage(), std.testing.allocator, root_dir),
    );

    try writeCheckpointIndex(storage.storage(), std.testing.allocator, root_dir, .{
        .oldest_retained_segment = 3,
        .covered_through_segment = 6,
    });
    const checkpoint_path = try checkpointPathAlloc(std.testing.allocator, root_dir);
    defer std.testing.allocator.free(checkpoint_path);
    try Cases.corruptFile(storage.storage(), checkpoint_path, control_pair_len);
    try std.testing.expectError(
        error.CorruptLsmWalIndex,
        readCheckpointIndex(storage.storage(), std.testing.allocator, root_dir),
    );

    try writeReplayIndex(storage.storage(), std.testing.allocator, root_dir, .{
        .current_segment = 4,
        .next_sequence = 17,
        .truncated_through = 9,
    });
    const replay_path = try replayIndexPathAlloc(std.testing.allocator, root_dir);
    defer std.testing.allocator.free(replay_path);
    try Cases.corruptFile(storage.storage(), replay_path, control_triple_len);
    try std.testing.expectError(
        error.CorruptLsmWalIndex,
        readReplayIndex(storage.storage(), std.testing.allocator, root_dir),
    );

    const segments_path = try replaySegmentsPathAlloc(std.testing.allocator, root_dir);
    defer std.testing.allocator.free(segments_path);
    var segment_entry = encodeCommittedSegmentEntry(.replay_segment, 4, 17);
    segment_entry[control_header_len] ^= 0x20;
    try storage.storage().writeFileAbsolute(segments_path, &segment_entry);
    try std.testing.expectError(
        error.CorruptLsmWalIndex,
        replayStartSegment(storage.storage(), std.testing.allocator, root_dir, 17),
    );
}

test "lsm wal retention snapshot counts replayed segment debt and reset clears it" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-retention-snapshot-test";
    try storage.storage().createDirPath(root_dir);

    var first: State = .{};
    defer first.deinit(std.testing.allocator);
    try first.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);

    var second: State = .{};
    defer second.deinit(std.testing.allocator);
    try second.upsert(std.testing.allocator, .{ .name = "docs" }, "b", "B", false);

    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &first, false, .{ .segment_bytes = 32 });
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &second, false, .{ .segment_bytes = 32 });

    const retained = try snapshotRetention(storage.storage(), std.testing.allocator, root_dir);
    try std.testing.expectEqual(@as(u64, 1), retained.oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 2), retained.current_segment);
    try std.testing.expectEqual(@as(u64, 2), retained.segments);
    try std.testing.expect(retained.bytes > 0);
    try std.testing.expect(retained.current_segment_bytes > 0);
    try std.testing.expect(retained.current_segment_bytes <= retained.bytes);

    try reset(storage.storage(), std.testing.allocator, root_dir);

    const after_reset = try snapshotRetention(storage.storage(), std.testing.allocator, root_dir);
    try std.testing.expectEqual(@as(u64, 1), after_reset.oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 1), after_reset.current_segment);
    try std.testing.expectEqual(@as(u64, 0), after_reset.segments);
    try std.testing.expectEqual(@as(u64, 0), after_reset.bytes);
    try std.testing.expectEqual(@as(u64, 0), after_reset.current_segment_bytes);
}

test "lsm wal checkpoint retires covered segments and replay starts at retained floor" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-checkpoint-retire-test";
    try storage.storage().createDirPath(root_dir);

    var first: State = .{};
    defer first.deinit(std.testing.allocator);
    try first.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);

    var second: State = .{};
    defer second.deinit(std.testing.allocator);
    try second.upsert(std.testing.allocator, .{ .name = "docs" }, "b", "B", false);

    var third: State = .{};
    defer third.deinit(std.testing.allocator);
    try third.upsert(std.testing.allocator, .{ .name = "docs" }, "c", "C", false);

    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &first, false, .{ .segment_bytes = 32 });
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &second, false, .{ .segment_bytes = 32 });
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &third, false, .{ .segment_bytes = 32 });

    try retireCoveredSegments(storage.storage(), std.testing.allocator, root_dir, 2);

    const retained = try snapshotRetention(storage.storage(), std.testing.allocator, root_dir);
    try std.testing.expectEqual(@as(u64, 3), retained.oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 2), retained.checkpoint_covered_through_segment);
    try std.testing.expectEqual(@as(u64, 3), retained.current_segment);
    try std.testing.expectEqual(@as(u64, 1), retained.segments);
    try std.testing.expect(retained.bytes > 0);

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    const stats = try replayIntoMutable(storage.storage(), std.testing.allocator, root_dir, &replayed);
    try std.testing.expectEqual(@as(u64, 1), stats.records);
    try std.testing.expectEqual(@as(u64, 1), stats.entries);
    try std.testing.expectEqualStrings("C", try replayed.get(.{ .name = "docs" }, "c"));
    try std.testing.expectError(error.NotFound, replayed.get(.{ .name = "docs" }, "a"));
    try std.testing.expectError(error.NotFound, replayed.get(.{ .name = "docs" }, "b"));
}

test "lsm wal replay tolerates corrupt tail on current segment" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-corrupt-tail-current-segment-test";
    try storage.storage().createDirPath(root_dir);

    var first: State = .{};
    defer first.deinit(std.testing.allocator);
    try first.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);

    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &first, false, .{});

    const current_segment = try currentSegment(storage.storage(), std.testing.allocator, root_dir);
    const segment_path = try segmentPathAlloc(std.testing.allocator, root_dir, current_segment);
    defer std.testing.allocator.free(segment_path);
    try storage.storage().appendFileAbsolute(std.testing.allocator, segment_path, "JUNK", false);

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    const stats = try replayIntoMutable(storage.storage(), std.testing.allocator, root_dir, &replayed);
    try std.testing.expectEqual(@as(u64, 1), stats.records);
    try std.testing.expectEqual(@as(u64, 1), stats.entries);
    try std.testing.expect(stats.truncated_tail_bytes >= 4);
    try std.testing.expectEqualStrings("A", try replayed.get(.{ .name = "docs" }, "a"));
}

test "lsm wal replay rejects complete corrupt final record" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-corrupt-final-record-test";
    try storage.storage().createDirPath(root_dir);

    var first: State = .{};
    defer first.deinit(std.testing.allocator);
    try first.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &first, false, .{});

    const current_segment = try currentSegment(storage.storage(), std.testing.allocator, root_dir);
    const segment_path = try segmentPathAlloc(std.testing.allocator, root_dir, current_segment);
    defer std.testing.allocator.free(segment_path);
    const original = try storage.storage().readFileAlloc(std.testing.allocator, segment_path, 1024 * 1024);
    defer std.testing.allocator.free(original);

    const corrupted = try std.testing.allocator.dupe(u8, original);
    defer std.testing.allocator.free(corrupted);
    corrupted[corrupted.len - 1] ^= 0xFF;
    try storage.storage().writeFileAbsolute(segment_path, corrupted);

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    try std.testing.expectError(error.CorruptLsmWal, replayIntoMutable(storage.storage(), std.testing.allocator, root_dir, &replayed));
}

test "lsm wal replay rejects unreadable current segment without valid prefix" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-unreadable-current-segment-test";
    try storage.storage().createDirPath(root_dir);

    var first: State = .{};
    defer first.deinit(std.testing.allocator);
    try first.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &first, false, .{});

    const current_segment = try currentSegment(storage.storage(), std.testing.allocator, root_dir);
    const segment_path = try segmentPathAlloc(std.testing.allocator, root_dir, current_segment);
    defer std.testing.allocator.free(segment_path);
    try storage.storage().writeFileAbsolute(segment_path, "JUNK");

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    try std.testing.expectError(error.CorruptLsmWal, replayIntoMutable(storage.storage(), std.testing.allocator, root_dir, &replayed));
}

test "lsm wal replay rejects final record with corrupted header magic" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-corrupt-final-header-record-test";
    try storage.storage().createDirPath(root_dir);

    var first: State = .{};
    defer first.deinit(std.testing.allocator);
    try first.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &first, false, .{});

    const current_segment = try currentSegment(storage.storage(), std.testing.allocator, root_dir);
    const segment_path = try segmentPathAlloc(std.testing.allocator, root_dir, current_segment);
    defer std.testing.allocator.free(segment_path);
    const first_size = try storage.storage().fileSize(segment_path);

    var second: State = .{};
    defer second.deinit(std.testing.allocator);
    try second.upsert(std.testing.allocator, .{ .name = "docs" }, "a", "A", false);
    try second.upsert(std.testing.allocator, .{ .name = "docs" }, "b", "B", false);
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &second, false, .{});

    const original = try storage.storage().readFileAlloc(std.testing.allocator, segment_path, 1024 * 1024);
    defer std.testing.allocator.free(original);
    const corrupted = try std.testing.allocator.dupe(u8, original);
    defer std.testing.allocator.free(corrupted);
    corrupted[@intCast(first_size)] ^= 0xFF;
    try storage.storage().writeFileAbsolute(segment_path, corrupted);

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    try std.testing.expectError(error.CorruptLsmWal, replayIntoMutable(storage.storage(), std.testing.allocator, root_dir, &replayed));
}

test "lsm wal appends iterates and truncates replay rows" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-committed-changes-test";
    try storage.storage().createDirPath(root_dir);

    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        1,
        "first",
        false,
        .{ .segment_bytes = 32 },
    );
    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        2,
        "second",
        false,
        .{ .segment_bytes = 32 },
    );

    const replay_retention = try snapshotReplayRetention(storage.storage(), std.testing.allocator, root_dir);
    try std.testing.expectEqual(@as(u64, 2), replay_retention.segments);
    try std.testing.expect(replay_retention.bytes > 0);

    try std.testing.expectEqual(
        @as(u64, 2),
        lastReplaySequence(storage.storage(), std.testing.allocator, root_dir, 0),
    );
    try std.testing.expectEqual(
        @as(u64, 3),
        nextReplaySequence(storage.storage(), std.testing.allocator, root_dir, 1),
    );

    const after_first = try iterateReplayFrom(storage.storage(), std.testing.allocator, root_dir, 2);
    defer {
        for (after_first) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(after_first);
    }
    try std.testing.expectEqual(@as(usize, 1), after_first.len);
    try std.testing.expectEqual(@as(u64, 2), after_first[0].sequence);
    try std.testing.expectEqualStrings("second", after_first[0].payload);

    try truncateReplayUpTo(storage.storage(), std.testing.allocator, root_dir, 1);

    const from_zero = try iterateReplayFrom(storage.storage(), std.testing.allocator, root_dir, 0);
    defer {
        for (from_zero) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(from_zero);
    }
    try std.testing.expectEqual(@as(usize, 1), from_zero.len);
    try std.testing.expectEqual(@as(u64, 2), from_zero[0].sequence);
    try std.testing.expectEqualStrings("second", from_zero[0].payload);

    const replay_index_path = try replayIndexPathAlloc(std.testing.allocator, root_dir);
    defer std.testing.allocator.free(replay_index_path);
    try std.testing.expectEqual(@as(u64, control_triple_len), try storage.storage().fileSize(replay_index_path));

    const replay_segment_path = try replaySegmentPathAlloc(std.testing.allocator, root_dir, 1);
    defer std.testing.allocator.free(replay_segment_path);
    try std.testing.expect((try storage.storage().fileSize(replay_segment_path)) > 0);

    const main_replay_segments_path = try mainReplaySegmentsPathAlloc(std.testing.allocator, root_dir);
    defer std.testing.allocator.free(main_replay_segments_path);
    try std.testing.expectEqual(@as(u64, 0), try storage.storage().fileSize(main_replay_segments_path));

    const replay_segments_path = try replaySegmentsPathAlloc(std.testing.allocator, root_dir);
    defer std.testing.allocator.free(replay_segments_path);
    try std.testing.expect((try storage.storage().fileSize(replay_segments_path)) > 0);
}

test "lsm wal replay rows tolerate zero-filled gap and ignore stale tail" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-committed-zero-gap-test";
    try storage.storage().createDirPath(root_dir);

    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        1,
        "first",
        false,
        .{ .segment_bytes = 1024 * 1024 },
    );

    const replay_segment_path = try replaySegmentPathAlloc(std.testing.allocator, root_dir, 1);
    defer std.testing.allocator.free(replay_segment_path);
    const original = try storage.storage().readFileAlloc(std.testing.allocator, replay_segment_path, 1024 * 1024);
    defer std.testing.allocator.free(original);

    var corrupted = std.ArrayListUnmanaged(u8).empty;
    defer corrupted.deinit(std.testing.allocator);
    try corrupted.appendSlice(std.testing.allocator, original);
    try corrupted.appendNTimes(std.testing.allocator, 0, 8192);
    try corrupted.appendSlice(std.testing.allocator, original);
    try storage.storage().writeFileAbsolute(replay_segment_path, corrupted.items);

    const entries = try iterateReplayFrom(storage.storage(), std.testing.allocator, root_dir, 1);
    defer {
        for (entries) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 1), entries[0].sequence);
    try std.testing.expectEqualStrings("first", entries[0].payload);
    try std.testing.expectEqual(@as(u64, original.len), try storage.storage().fileSize(replay_segment_path));
}

test "lsm wal replay rows reject leading zero-filled file" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-committed-leading-zero-gap-test";
    try storage.storage().createDirPath(root_dir);

    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        1,
        "first",
        false,
        .{ .segment_bytes = 1024 * 1024 },
    );

    const replay_segment_path = try replaySegmentPathAlloc(std.testing.allocator, root_dir, 1);
    defer std.testing.allocator.free(replay_segment_path);
    const original_len = try storage.storage().fileSize(replay_segment_path);
    const zeros = try std.testing.allocator.alloc(u8, @intCast(original_len));
    defer std.testing.allocator.free(zeros);
    @memset(zeros, 0);
    try storage.storage().writeFileAbsolute(replay_segment_path, zeros);

    try std.testing.expectError(
        error.CorruptLsmWal,
        iterateReplayFrom(storage.storage(), std.testing.allocator, root_dir, 1),
    );
    try std.testing.expectEqual(original_len, try storage.storage().fileSize(replay_segment_path));
}

test "lsm wal records main replay segment sequence floors" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-committed-segment-floor-test";
    try storage.storage().createDirPath(root_dir);

    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        1,
        "one",
        false,
        .{ .segment_bytes = 28 },
    );
    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        2,
        "two",
        false,
        .{ .segment_bytes = 28 },
    );
    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        3,
        "three",
        false,
        .{ .segment_bytes = 28 },
    );

    try std.testing.expectEqual(@as(u64, 1), try replayMainWalStartSegment(storage.storage(), std.testing.allocator, root_dir, 1));
    try std.testing.expectEqual(@as(u64, 2), try replayMainWalStartSegment(storage.storage(), std.testing.allocator, root_dir, 2));
    try std.testing.expectEqual(@as(u64, 3), try replayMainWalStartSegment(storage.storage(), std.testing.allocator, root_dir, 3));
}

test "lsm wal records dedicated replay segment sequence floors" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-dedicated-committed-segment-floor-test";
    try storage.storage().createDirPath(root_dir);

    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        1,
        "one",
        false,
        .{ .segment_bytes = 28 },
    );
    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        2,
        "two",
        false,
        .{ .segment_bytes = 28 },
    );
    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        3,
        "three",
        false,
        .{ .segment_bytes = 28 },
    );

    try std.testing.expectEqual(@as(u64, 1), try replayStartSegment(storage.storage(), std.testing.allocator, root_dir, 1));
    try std.testing.expectEqual(@as(u64, 2), try replayStartSegment(storage.storage(), std.testing.allocator, root_dir, 2));
    try std.testing.expectEqual(@as(u64, 3), try replayStartSegment(storage.storage(), std.testing.allocator, root_dir, 3));
}

test "lsm wal replays state and replay rows from shared wal segments" {
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    const root_dir = "/wal-mixed-test";
    try storage.storage().createDirPath(root_dir);

    var state: State = .{};
    defer state.deinit(std.testing.allocator);
    try state.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:a", "A", false);

    _ = try appendReplay(
        storage.storage(),
        std.testing.allocator,
        root_dir,
        1,
        "first",
        false,
        .{ .segment_bytes = 64 },
    );
    _ = try appendStateWithOptions(storage.storage(), std.testing.allocator, root_dir, &state, false, .{ .segment_bytes = 64 });

    var replayed: State = .{};
    defer replayed.deinit(std.testing.allocator);
    const replay_stats = try replayIntoMutable(storage.storage(), std.testing.allocator, root_dir, &replayed);
    try std.testing.expectEqual(@as(u64, 1), replay_stats.records);
    try std.testing.expectEqual(@as(u64, 1), replay_stats.entries);
    try std.testing.expectEqualStrings("A", try replayed.get(.{ .name = "docs" }, "doc:a"));

    const entries = try iterateReplayFrom(storage.storage(), std.testing.allocator, root_dir, 1);
    defer {
        for (entries) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 1), entries[0].sequence);
    try std.testing.expectEqualStrings("first", entries[0].payload);
}

test "WAL allocator owner replay scratch preserves the physical root across ABI callbacks" {
    var manager = resource_manager_mod.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var budget = resource_manager_mod.BudgetedAllocator.initReclaiming(&manager, .lsm_in_memory_state, std.testing.allocator, 1);
    defer budget.deinit();
    budget.credit_quantum = 1;
    var root = budget.threadSafeNoReclaimAllocator();
    const descriptor = @import("runtime_memory_abi").Allocator.fromStd(&root);
    for ([_]Allocator{ scratchAllocator(root), scratchAllocator(descriptor.asStd()) }) |scratch| {
        const bytes = try scratch.alloc(u8, 97);
        try std.testing.expectEqual(@as(u64, 97), budget.liveBytesThreadSafe());
        scratch.free(bytes);
        try std.testing.expectError(error.OutOfMemory, scratch.alloc(u8, 2 * 1024 * 1024));
        try std.testing.expectEqual(@as(u64, 0), budget.liveBytesThreadSafe());
    }
}

const ControlledSyncPublication = struct {
    held: *bool,
    releases: *usize,
    pub fn release(self: *@This()) void {
        std.debug.assert(self.held.*);
        self.held.* = false;
        self.releases.* += 1;
    }
};

test "controlled physical sync WAL coordinator retains each accepted phase across original failures" {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const cpu = @import("../../common/runnable_scheduler.zig");
    const admission = @import("../../common/read_admission.zig");
    const now = platform.time.monotonicNs;
    const Operation = CurrentStateSync(ControlledSyncPublication);
    const Mode = enum { begin_failure, end_failure, lifetime_failure };
    const Target = @FieldType(Operation, "stage");
    for ([_]Target{ .current, .segment, .index }) |target| for (std.enums.values(Mode)) |mode| {
        const a = std.testing.allocator;
        var backing = std.testing.FailingAllocator.init(a, .{});
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 16);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(backing.allocator(), .threaded, &pool);
        var native_live = true;
        defer if (native_live) native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer a.free(root);
        const directory = try walDirPathAlloc(a, root);
        defer a.free(directory);
        try native.storage().createDirPath(directory);
        try writeCurrentSegment(native.storage(), a, root, 7, 5);
        const segment = try segmentPathAlloc(a, root, 7);
        defer a.free(segment);
        const index_path = try indexPathAlloc(a, root);
        defer a.free(index_path);
        try native.storage().writeFileAbsolute(segment, "state");
        var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer slots.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.start(&slots, .{ .io = std.testing.io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 10 * std.time.ns_per_s }, .query);
        var health: cpu.Execution = .{};
        defer health.finish();
        try health.start(&slots, .{ .io = std.testing.io, .native_now_ns = now }, .control);
        var operation = try Operation.prepare(root);
        var held = true;
        var releases: usize = 0;
        const Control = struct {
            operation: *Operation,
            execution: *cpu.Execution,
            slots: *cpu.Scheduler,
            target: Target,
            mode: ?Mode,
            previous: ?Target = null,
            triggered: bool = false,
            fn lifetime(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.execution.checkLifetime();
                if (self.triggered and self.mode == .lifetime_failure) return error.DeadlineExceeded;
            }
            fn checkpoint(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try lifetime(raw);
                if (self.slots.state(&self.execution.entry.?) != .running) return error.InvalidRunnableTransition;
                if (self.previous != self.operation.stage) {
                    self.previous = self.operation.stage;
                    storage_io.NativeSyncTestControl.release.store(false, .release);
                }
            }
            fn deadline(raw: *anyopaque) !?u64 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return self.execution.entry.?.options.deadline_ns;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.operation.stage == self.target and self.mode != null) {
                    self.triggered = true;
                    if (self.mode == .begin_failure) return error.FileNotFound;
                }
                try self.execution.park(.resource_blocked);
                if (!(self.triggered and self.mode == .lifetime_failure))
                    storage_io.NativeSyncTestControl.release.store(true, .release);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                storage_io.NativeSyncTestControl.release.store(true, .release);
                if (self.triggered and self.mode == .end_failure) return error.Canceled;
                try self.execution.enter(.query);
            }
        };
        var control: Control = .{ .operation = &operation, .execution = &execution, .slots = &slots, .target = target, .mode = mode };
        const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.checkpoint, .check_lifetime = Control.lifetime, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const original = admission.Port.init(&payload);
        storage_io.NativeSyncTestControl.pause.store(true, .release);
        storage_io.NativeSyncTestControl.release.store(false, .release);
        storage_io.NativeSyncTestControl.reads.store(0, .release);
        storage_io.NativeSyncTestControl.calls.store(0, .release);
        defer {
            storage_io.NativeSyncTestControl.release.store(true, .release);
            storage_io.NativeSyncTestControl.pause.store(false, .release);
        }
        var resources = (try native.storage().tryPrepareDurabilityWithAdmission(original)) orelse return error.TestUnexpectedResult;
        operation.start(resources, .{ .held = &held, .releases = &releases }, original) catch |cause| {
            resources.deinit();
            return cause;
        };
        defer {
            storage_io.NativeSyncTestControl.release.store(true, .release);
            execution.finish();
            operation.deinit();
        }
        native.state.threaded.concurrent_limit = .nothing;
        backing.fail_index = backing.alloc_index;
        backing.resize_fail_index = backing.resize_index;
        const alloc_index = backing.alloc_index;
        const resize_index = backing.resize_index;
        var reserved: usize = 0;
        while (pool.tryReserveDescriptorsForTest(std.testing.io, 1)) reserved += 1;
        defer pool.releaseDescriptorsForTest(std.testing.io, reserved);
        native.deinit();
        native_live = false;
        try std.testing.expectError(switch (mode) {
            .begin_failure => error.FileNotFound,
            .end_failure => error.Canceled,
            .lifetime_failure => error.DeadlineExceeded,
        }, operation.waitWithAdmission(original));
        try std.testing.expect(control.triggered);
        try std.testing.expectEqual(target, operation.stage);
        try std.testing.expect(held);
        try std.testing.expectEqual(@as(usize, 0), releases);
        if (mode == .end_failure) try execution.enter(.query);
        control.mode = null;
        storage_io.NativeSyncTestControl.release.store(true, .release);
        const result = try operation.waitWithAdmission(original);
        try std.testing.expectEqual(@as(u64, 1), result.segment_syncs);
        try std.testing.expectEqual(@as(u64, 1), result.index_syncs);
        try std.testing.expect(!held);
        try std.testing.expectEqual(@as(usize, 1), releases);
        _ = try operation.waitWithAdmission(original);
        try std.testing.expectEqual(@as(usize, 1), storage_io.NativeSyncTestControl.reads.load(.acquire));
        try std.testing.expectEqual(@as(usize, 2), storage_io.NativeSyncTestControl.calls.load(.acquire));
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, segment), storage_io.NativeSyncTestControl.sync_path_hashes[0].load(.acquire));
        try std.testing.expectEqual(std.hash.Wyhash.hash(0, index_path), storage_io.NativeSyncTestControl.sync_path_hashes[1].load(.acquire));
        try std.testing.expectEqual(alloc_index, backing.alloc_index);
        try std.testing.expectEqual(resize_index, backing.resize_index);
        try std.testing.expectEqual(cpu.State.running, slots.state(&health.entry.?));
    };
}

test "controlled physical sync WAL coordinator rejects corrupt metadata and never creates a missing segment" {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const admission = @import("../../common/read_admission.zig");
    const Operation = CurrentStateSync(ControlledSyncPublication);
    const Case = enum { absent, oversized, checksum, zero_segment, missing_segment, valid };
    const Control = struct {
        fn check(_: *anyopaque) !void {}
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn begin(_: *anyopaque) !void {}
        fn end(_: *anyopaque) !void {}
    };
    var marker: u8 = 0;
    const payload: admission.Payload = .{ .ptr = &marker, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &marker, .begin = Control.begin, .end = Control.end } };
    const original = admission.Port.init(&payload);
    for (std.enums.values(Case)) |case| {
        const a = std.testing.allocator;
        var native = try storage_io.NativeStorage.init(a, .threaded);
        defer native.deinit();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer a.free(root);
        const directory = try walDirPathAlloc(a, root);
        defer a.free(directory);
        try native.storage().createDirPath(directory);
        const index = try indexPathAlloc(a, root);
        defer a.free(index);
        const segment = try segmentPathAlloc(a, root, 7);
        defer a.free(segment);
        switch (case) {
            .absent => {},
            .oversized => {
                const bytes: [8192]u8 = @splat(0);
                try native.storage().writeFileAbsolute(index, &bytes);
            },
            .checksum => {
                var bytes: [control_pair_len]u8 = undefined;
                const pair: [16]u8 = @splat(0);
                encodeControl(&bytes, .current_segment, &pair);
                bytes[bytes.len - 1] ^= 1;
                try native.storage().writeFileAbsolute(index, &bytes);
            },
            .zero_segment => try writeCurrentSegment(native.storage(), a, root, 0, 0),
            .missing_segment => try writeCurrentSegment(native.storage(), a, root, 7, 5),
            .valid => {
                try writeCurrentSegment(native.storage(), a, root, 7, 5);
                try native.storage().writeFileAbsolute(segment, "state");
            },
        }
        var held = true;
        var releases: usize = 0;
        var operation = try Operation.prepare(root);
        var resources = (try native.storage().tryPrepareDurabilityWithAdmission(original)) orelse return error.TestUnexpectedResult;
        operation.start(resources, .{ .held = &held, .releases = &releases }, original) catch |cause| {
            resources.deinit();
            return cause;
        };
        defer operation.deinit();
        storage_io.NativeSyncTestControl.reads.store(0, .release);
        storage_io.NativeSyncTestControl.calls.store(0, .release);
        // Only sync counters are asserted: the initial read may already run
        // by the time the caller resets its observation counters.
        const result = operation.waitWithAdmission(original);
        if (case == .absent or case == .valid) {
            const stats = try result;
            try std.testing.expectEqual(@as(u64, @intFromBool(case == .valid)), stats.segment_syncs);
            try std.testing.expectEqual(stats.segment_syncs, stats.index_syncs);
            _ = try operation.waitWithAdmission(original);
        } else {
            const expected: anyerror = if (case == .missing_segment) error.FileNotFound else error.CorruptLsmWalIndex;
            try std.testing.expectError(expected, result);
            try std.testing.expectError(expected, operation.waitWithAdmission(original));
            try std.testing.expectError(error.FileNotFound, native.storage().fileSize(segment));
        }
        try std.testing.expect(!held);
        try std.testing.expectEqual(@as(usize, 1), releases);
        try std.testing.expectEqual(@as(usize, if (case == .valid) 2 else if (case == .missing_segment) 1 else 0), storage_io.NativeSyncTestControl.calls.load(.acquire));
    }
}

/// Checked allocation planning shared by capture and canonical encoding. A
/// rejected addition leaves the entire plan unchanged, including row count.
pub const StateRecordSize = struct {
    count: u32 = 0,
    payload: u32 = 4,

    pub fn add(self: *@This(), namespace_len: usize, key_len: usize, value_len: usize) !void {
        const count = std.math.add(u32, self.count, 1) catch return error.ResourceBudgetExceeded;
        var size = std.math.add(u32, self.payload, entry_header_len) catch return error.ResourceBudgetExceeded;
        for ([_]usize{ namespace_len, key_len, value_len }) |len| {
            if (len > std.math.maxInt(u32)) return error.ResourceBudgetExceeded;
            size = std.math.add(u32, size, @intCast(len)) catch return error.ResourceBudgetExceeded;
        }
        _ = std.math.add(usize, size, record_header_len) catch return error.ResourceBudgetExceeded;
        self.count = count;
        self.payload = size;
    }

    pub fn total(self: @This()) usize {
        return if (self.count == 0) 0 else @as(usize, self.payload) + record_header_len;
    }
};

/// Canonical state-record preparation on admitted CPU, before publication and
/// physical dispatch. The stable source outlives this synchronous call only.
pub fn stateRecordLenWithAdmission(state: anytype, original: @import("../../common/read_admission.zig").Port) !usize {
    var plan: StateRecordSize = .{};
    for (0..state.entryCount()) |index| {
        try original.checkpoint();
        const entry = state.entryAt(index);
        const namespace: []const u8 = entry.namespace_name orelse "";
        try plan.add(namespace.len, entry.key.len, entry.value.len);
    }
    return plan.total();
}

pub fn prepareStateRecordWithAdmission(allocator: Allocator, state: anytype, original: @import("../../common/read_admission.zig").Port) ![]u8 {
    try original.checkpoint();
    const count = state.entryCount();
    if (count == 0) return allocator.alloc(u8, 0);
    if (count > std.math.maxInt(u32)) return error.ResourceBudgetExceeded;
    var plan: StateRecordSize = .{};
    for (0..count) |index| {
        try original.checkpoint();
        const entry = state.entryAt(index);
        const ns = entry.namespace_name orelse "";
        try plan.add(ns.len, entry.key.len, entry.value.len);
    }
    const size = plan.payload;
    const total = plan.total();
    const raw = allocator.rawAlloc(total, .of(u8), @returnAddress()) orelse return error.OutOfMemory;
    const record = raw[0..total];
    errdefer allocator.rawFree(record, .of(u8), @returnAddress());
    const payload = record[record_header_len..];
    var pos: usize = 0;
    writeIntToPayload(payload, &pos, u32, @intCast(count));
    for (0..count) |index| {
        try original.checkpoint();
        const entry = state.entryAt(index);
        const ns = entry.namespace_name orelse "";
        payload[pos] = @as(u8, @intFromBool(entry.tombstone)) | (@as(u8, @intFromBool(entry.namespace_name != null)) << 1);
        @memset(payload[pos + 1 .. pos + 4], 0);
        pos += 4;
        writeIntToPayload(payload, &pos, u32, @intCast(ns.len));
        writeIntToPayload(payload, &pos, u32, @intCast(entry.key.len));
        writeIntToPayload(payload, &pos, u32, @intCast(entry.value.len));
        for ([_][]const u8{ ns, entry.key, entry.value }) |bytes| {
            var offset: usize = 0;
            while (offset < bytes.len) {
                try original.checkpoint();
                const len = @min(bytes.len - offset, 16 * 1024);
                @memcpy(payload[pos..][0..len], bytes[offset..][0..len]);
                pos += len;
                offset += len;
            }
        }
    }
    std.debug.assert(pos == size);
    var crc = Crc32.init();
    pos = 0;
    while (pos < payload.len) {
        try original.checkpoint();
        const end = pos + @min(payload.len - pos, 16 * 1024);
        crc.update(payload[pos..end]);
        pos = end;
    }
    try original.checkpoint();
    std.mem.writeInt(u32, record[0..4], record_magic, .little);
    std.mem.writeInt(u16, record[4..6], record_version, .little);
    std.mem.writeInt(u16, record[6..8], record_header_len, .little);
    std.mem.writeInt(u32, record[8..12], @intCast(payload.len), .little);
    std.mem.writeInt(u32, record[12..16], crc.final(), .little);
    return record;
}

/// Owns canonical WAL bootstrap/rotation, one prepared append and its ordered
/// barriers. Publication supplies bounded publish(AppendResult), requireRecovery
/// and release callbacks. It owns a prepared mutable successor and metadata
/// credits; no writer/maintenance may mutate that backend until retirement.
/// Successful I/O retains publication until publishWithAdmission atomically
/// adopts that successor on caller CPU. No caller admission borrow is stored.
pub fn CurrentStateAppend(comptime Publication: type) type {
    return struct {
        const Self = @This();
        pub var test_hook: ?*const fn (*Self, @import("../../common/read_admission.zig").Port) anyerror!void = null;
        const Stage = enum { current, checkpoint_probe, segment_stat, next_segment_probe, create_directory, directory_parent, current_replace, checkpoint_replace, append, segment_sync, segment_parent, ready, published, failed };
        const Pending = union(enum) { none, read: storage_io.ControlFileReadOperation, physical: storage_io.SyncOperation };
        index: [std.fs.max_path_bytes]u8 = undefined,
        index_len: usize = 0,
        checkpoint: [std.fs.max_path_bytes]u8 = undefined,
        checkpoint_len: usize = 0,
        segment: [std.fs.max_path_bytes]u8 = undefined,
        prefix_len: usize = 0,
        segment_len: usize = 0,
        options: AppendOptions,
        record_len: usize,
        bootstrap: bool = false,
        sync_on_commit: bool = true,
        create_segment: bool = false,
        mutation_started: bool = false,
        resources: ?storage_io.DurabilityResources = null,
        publication: ?Publication = null,
        pending: Pending = .none,
        stage: Stage = .current,
        result: AppendResult = .{},
        failure: ?anyerror = null,

        pub fn prepare(root: []const u8, record_len: usize, options: AppendOptions) !Self {
            if (record_len <= record_header_len or options.segment_bytes == 0) return error.ResourceBudgetExceeded;
            var self: Self = .{ .options = options, .record_len = record_len };
            const separator = if (root.len == 0 or std.mem.endsWith(u8, root, "/")) "" else "/";
            const prefix = std.fmt.bufPrint(&self.segment, "{s}{s}{s}/", .{ root, separator, wal_dir_name }) catch return error.NameTooLong;
            // Every future name, including the reserved control temporary,
            // must fit before the caller acquires filesystem publication.
            if (prefix.len + 40 >= self.segment.len) return error.NameTooLong;
            self.prefix_len = prefix.len;
            self.index_len = (std.fmt.bufPrint(&self.index, "{s}{s}", .{ prefix, wal_index_file_name }) catch unreachable).len;
            self.checkpoint_len = (std.fmt.bufPrint(&self.checkpoint, "{s}{s}", .{ prefix, wal_checkpoint_file_name }) catch unreachable).len;
            return self;
        }

        /// Ownership transfers only after the first physical read is accepted.
        /// The grant was prepared with exactly this canonical record's bytes.
        pub fn start(self: *Self, resources: storage_io.DurabilityResources, publication: Publication, original: @import("../../common/read_admission.zig").Port) !void {
            std.debug.assert(self.resources == null and self.publication == null);
            var grant = resources;
            const read = try grant.beginControlRead(self.index[0..self.index_len], control_pair_len, original);
            self.resources = grant;
            self.publication = publication;
            self.pending = .{ .read = read };
        }

        fn segmentPath(self: *Self, number: u64) void {
            self.result.segment = number;
            const name = std.fmt.bufPrint(self.segment[self.prefix_len..], "{d:0>20}.log", .{number}) catch unreachable;
            self.segment_len = self.prefix_len + name.len;
        }

        pub fn waitWithAdmission(self: *Self, original: @import("../../common/read_admission.zig").Port) !AppendResult {
            const admission = @import("../../common/read_admission.zig");
            _ = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
            _ = (try original.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
            while (true) {
                try original.checkpoint();
                if (comptime @import("builtin").is_test) if (test_hook) |hook| try hook(self, original);
                switch (self.stage) {
                    .ready, .published => return self.result,
                    .failed => return self.failure.?,
                    .current, .checkpoint_probe, .segment_stat, .next_segment_probe => {
                        if (self.pending == .none) {
                            const path = if (self.stage == .checkpoint_probe) self.checkpoint[0..self.checkpoint_len] else self.segment[0..self.segment_len];
                            self.pending = .{ .read = try self.resources.?.beginControlRead(path, 1, original) };
                        }
                        var trace: admission.RejectionTrace = .{};
                        var absent = false;
                        const read = self.pending.read.waitWithAdmission(trace.bind(original).?) catch |cause| blk: {
                            if (trace.failure) |rejected| return rejected;
                            if (cause != error.FileNotFound) return self.fail(cause);
                            absent = true;
                            break :blk storage_io.ControlFileResult{ .file_size = 0, .bytes = &.{} };
                        };
                        try original.checkpoint();
                        switch (self.stage) {
                            .current => {
                                if (absent) {
                                    self.bootstrap = true;
                                    self.create_segment = true;
                                    self.segmentPath(1);
                                    self.stage = .checkpoint_probe;
                                } else {
                                    if (read.file_size != control_pair_len or read.bytes.len != control_pair_len) return self.fail(error.CorruptLsmWalIndex);
                                    const data = decodeControl(read.bytes, .current_segment, 16) catch |cause| return self.fail(cause);
                                    const number = std.mem.readInt(u64, data[0..8], .little);
                                    if (number == 0) return self.fail(error.CorruptLsmWalIndex);
                                    self.segmentPath(number);
                                    self.stage = .segment_stat;
                                }
                            },
                            .checkpoint_probe => {
                                // An orphan checkpoint is recovery evidence,
                                // never permission to erase retention history.
                                if (!absent) return self.fail(error.RecoveryRequired);
                                self.stage = .segment_stat;
                            },
                            .segment_stat => {
                                if (self.bootstrap) {
                                    if (read.file_size != 0) return self.fail(error.RecoveryRequired);
                                    self.stage = .create_directory;
                                } else {
                                    if (absent) return self.fail(error.FileNotFound);
                                    const size = std.math.add(u64, read.file_size, self.record_len) catch return self.fail(error.ResourceBudgetExceeded);
                                    self.create_segment = read.file_size == 0;
                                    if (read.file_size > 0 and size > self.options.segment_bytes) {
                                        const next = std.math.add(u64, self.result.segment, 1) catch return self.fail(error.CorruptLsmWalIndex);
                                        self.segmentPath(next);
                                        self.create_segment = true;
                                        self.stage = .next_segment_probe;
                                    } else self.stage = .append;
                                }
                            },
                            .next_segment_probe => {
                                if (read.file_size != 0) return self.fail(error.RecoveryRequired);
                                self.stage = .current_replace;
                            },
                            else => unreachable,
                        }
                        self.retirePending();
                    },
                    else => {
                        if (self.pending == .none) {
                            const operation = switch (self.stage) {
                                .create_directory => try self.resources.?.beginCreateDirectory(self.segment[0 .. self.prefix_len - 1], original),
                                .directory_parent => try self.resources.?.beginSyncParent(self.segment[0 .. self.prefix_len - 1], original),
                                .current_replace, .checkpoint_replace => blk: {
                                    var data: [16]u8 = undefined;
                                    std.mem.writeInt(u64, data[0..8], if (self.stage == .current_replace) self.result.segment else 1, .little);
                                    std.mem.writeInt(u64, data[8..16], 0, .little);
                                    var raw: [control_pair_len]u8 = undefined;
                                    encodeControl(&raw, if (self.stage == .current_replace) .current_segment else .checkpoint, &data);
                                    const path = if (self.stage == .current_replace) self.index[0..self.index_len] else self.checkpoint[0..self.checkpoint_len];
                                    break :blk try self.resources.?.beginControlReplace(path, &raw, original);
                                },
                                .append => try self.resources.?.beginPreparedAppend(self.segment[0..self.segment_len], self.create_segment, original),
                                .segment_sync => try self.resources.?.beginSyncContents(self.segment[0..self.segment_len], original),
                                .segment_parent => try self.resources.?.beginSyncParent(self.segment[0..self.segment_len], original),
                                else => unreachable,
                            };
                            self.pending = .{ .physical = operation };
                            if (self.stage == .create_directory or self.stage == .current_replace or self.stage == .checkpoint_replace or self.stage == .append) self.mutation_started = true;
                        }
                        var trace: admission.RejectionTrace = .{};
                        self.pending.physical.waitWithAdmission(trace.bind(original).?) catch |cause| {
                            if (trace.failure) |rejected| return rejected;
                            return self.fail(cause);
                        };
                        self.retirePending();
                        self.stage = switch (self.stage) {
                            .create_directory => .directory_parent,
                            .directory_parent => .current_replace,
                            .current_replace => blk: {
                                self.result.index_syncs += 1;
                                break :blk if (self.bootstrap) .checkpoint_replace else .append;
                            },
                            .checkpoint_replace => blk: {
                                self.result.index_syncs += 1;
                                break :blk .append;
                            },
                            .append => blk: {
                                self.result.bytes = self.record_len;
                                self.result.segment_became_nonempty = self.create_segment;
                                break :blk if (self.sync_on_commit) .segment_sync else .ready;
                            },
                            .segment_sync => blk: {
                                self.result.segment_syncs = 1;
                                break :blk if (self.create_segment) .segment_parent else .ready;
                            },
                            .segment_parent => .ready,
                            else => unreachable,
                        };
                    },
                }
            }
        }

        /// The callback is an already-prepared, bounded, infallible publication
        /// on caller CPU. No reentry check may split adoption from its receipt.
        pub fn publishWithAdmission(self: *Self, original: @import("../../common/read_admission.zig").Port) !AppendResult {
            const result = try self.waitWithAdmission(original);
            if (self.stage == .published) return result;
            try original.checkpoint();
            self.publication.?.publish(result);
            self.stage = .published;
            self.releasePublication(false);
            return result;
        }

        fn retirePending(self: *Self) void {
            switch (self.pending) {
                .read => |*read| read.deinit(),
                .physical => |*operation| operation.deinit(),
                .none => {},
            }
            self.pending = .none;
        }

        fn releasePublication(self: *Self, recovery: bool) void {
            if (self.publication) |*publication| {
                if (recovery and self.mutation_started) publication.requireRecovery();
                publication.release();
            }
            self.publication = null;
        }

        fn fail(self: *Self, cause: anyerror) anyerror {
            self.retirePending();
            self.failure = cause;
            self.stage = .failed;
            self.releasePublication(true);
            return cause;
        }

        /// Join an abandoned attempt under independent protected execution.
        /// Never publish or complete missing barriers after original rejection.
        pub fn abortWithAdmission(self: *Self, original: @import("../../common/read_admission.zig").Port) !void {
            try original.checkpoint();
            var trace: @import("../../common/read_admission.zig").RejectionTrace = .{};
            switch (self.pending) {
                .read => |*read| {
                    _ = read.waitWithAdmission(trace.bind(original).?) catch {
                        if (trace.failure) |rejected| return rejected;
                        self.joinAndRelease();
                        return;
                    };
                },
                .physical => |*op| op.waitWithAdmission(trace.bind(original).?) catch {
                    if (trace.failure) |rejected| return rejected;
                },
                .none => {},
            }
            self.joinAndRelease();
        }
        pub fn joinAndRelease(self: *Self) void {
            self.retirePending();
            self.releasePublication(self.stage != .published);
            if (self.stage != .published) {
                self.failure = error.Canceled;
                self.stage = .failed;
            }
        }

        /// Retire a terminal append's worker on the original wait boundary.
        /// A failed reentry leaves the grant, frozen bytes and their charge
        /// reachable for retry, including when physical join already finished.
        pub fn retireWithAdmission(self: *Self, original: @import("../../common/read_admission.zig").Port) !void {
            try original.checkpoint();
            if (self.stage != .published and self.stage != .failed) return error.ResourceBudgetExceeded;
            std.debug.assert(self.pending == .none and self.publication == null);
            if (self.resources) |*resources| {
                try resources.retireWithAdmission(original);
                self.resources = null;
            }
        }

        /// Mandatory quiesced destruction joins only accepted physical work.
        /// Unpublished possible writes require recovery before releasing the
        /// guard. It never starts missing barriers or publishes caller state.
        pub fn deinit(self: *Self) void {
            self.retirePending();
            self.releasePublication(self.stage != .published);
            if (self.resources) |*resources| resources.deinit();
            self.* = undefined;
        }
    };
}

pub fn validateStateRecordWithAdmission(record: []const u8, original: @import("../../common/read_admission.zig").Port) !u32 {
    try original.checkpoint();
    if (record.len < record_header_len + 4) return error.CorruptLsmWal;
    if (std.mem.readInt(u32, record[0..4], .little) != record_magic) return error.CorruptLsmWal;
    if (std.mem.readInt(u16, record[4..6], .little) != record_version or std.mem.readInt(u16, record[6..8], .little) != record_header_len) return error.UnsupportedLsmWalHeader;
    const payload = record[record_header_len..];
    if (std.mem.readInt(u32, record[8..12], .little) != payload.len) return error.CorruptLsmWal;
    var crc = Crc32.init();
    var pos: usize = 0;
    while (pos < payload.len) {
        try original.checkpoint();
        const end = pos + @min(payload.len - pos, 16 * 1024);
        crc.update(payload[pos..end]);
        pos = end;
    }
    if (crc.final() != std.mem.readInt(u32, record[12..16], .little)) return error.CorruptLsmWal;
    pos = 0;
    const count = try readInt(payload, &pos, u32);
    for (0..count) |_| {
        try original.checkpoint();
        if (payload.len - pos < entry_header_len) return error.CorruptLsmWal;
        pos += 4;
        const ns = try readInt(payload, &pos, u32);
        const key = try readInt(payload, &pos, u32);
        const value = try readInt(payload, &pos, u32);
        for ([_]u32{ ns, key, value }) |len| {
            if (len > payload.len - pos) return error.CorruptLsmWal;
            pos += len;
        }
    }
    if (pos != payload.len) return error.CorruptLsmWal;
    return count;
}

const AppendTestAuthority = struct {
    checks: usize = 0,
    fail_at: usize = std.math.maxInt(usize),
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.checks += 1;
        if (self.checks >= self.fail_at) return error.NotFound;
    }
    fn deadline(_: *anyopaque) !?u64 {
        return std.math.maxInt(u64);
    }
    fn begin(_: *anyopaque) !void {}
    fn end(_: *anyopaque) !void {}
    fn payload(self: *@This()) @import("../../common/read_admission.zig").Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = check, .deadline_ns = deadline }, .execution_wait = .{ .ptr = self, .begin = begin, .end = end } };
    }
};

const AppendTestPublication = struct {
    held: *bool,
    publishes: *usize,
    recover: *bool,
    releases: *usize,
    pub fn publish(self: *@This(), _: AppendResult) void {
        std.debug.assert(self.held.* and self.publishes.* == 0);
        self.publishes.* += 1;
    }
    pub fn requireRecovery(self: *@This()) void {
        std.debug.assert(self.held.*);
        self.recover.* = true;
    }
    pub fn release(self: *@This()) void {
        std.debug.assert(self.held.*);
        self.held.* = false;
        self.releases.* += 1;
    }
};

test "controlled physical sync WAL record planning rejects overflow without advancing its cursor" {
    var plan: StateRecordSize = .{};
    try std.testing.expectEqual(@as(usize, 0), plan.total());
    try std.testing.expectError(error.ResourceBudgetExceeded, plan.add(std.math.maxInt(usize), 0, 0));
    try std.testing.expectEqual(@as(u32, 0), plan.count);
    try std.testing.expectEqual(@as(u32, 4), plan.payload);
    try plan.add(0, 3, 72);
    try std.testing.expectEqual(@as(usize, record_header_len + 4 + entry_header_len + 3 + 72), plan.total());
    const before = plan;
    try std.testing.expectError(error.ResourceBudgetExceeded, plan.add(0, std.math.maxInt(usize), 1));
    try std.testing.expectEqual(before, plan);
    plan.count = std.math.maxInt(u32);
    const full = plan;
    try std.testing.expectError(error.ResourceBudgetExceeded, plan.add(0, 1, 1));
    try std.testing.expectEqual(full, plan);
    try std.testing.expectError(error.ResourceBudgetExceeded, @import("state.zig").sharedEntryAllocationBytes(null, std.math.maxInt(usize), 1));
}

test "controlled physical sync WAL record preparation preserves canonical bytes and bounds copy checksum work" {
    const a = std.testing.allocator;
    var state: State = .{};
    defer state.deinit(a);
    const value = try a.alloc(u8, 70 * 1024);
    defer a.free(value);
    @memset(value, 'v');
    try state.upsert(a, .{ .name = "docs" }, "key", value, false);
    var authority: AppendTestAuthority = .{ .fail_at = 8 };
    var payload = authority.payload();
    const original = @import("../../common/read_admission.zig").Port.init(&payload);
    var backing = std.testing.FailingAllocator.init(a, .{});
    try std.testing.expectError(error.NotFound, prepareStateRecordWithAdmission(backing.allocator(), &state, original));
    try std.testing.expectEqual(backing.allocated_bytes, backing.freed_bytes);
    authority.fail_at = std.math.maxInt(usize);
    try state.upsert(a, .{}, "deleted", "", true);
    try state.upsert(a, .{ .name = "" }, "binary\x00key", "\xff\x00", false);
    const record = try prepareStateRecordWithAdmission(a, &state, original);
    defer a.free(record);
    try std.testing.expectEqual(@as(u32, 3), try validateStateRecordWithAdmission(record, original));
    var memory = storage_io.MemoryStorage.init(a);
    defer memory.deinit();
    _ = try appendState(memory.storage(), a, "/canonical", &state, false);
    const raw = try memory.storage().readFileAlloc(a, "/canonical/wal/00000000000000000001.log", record.len);
    defer a.free(raw);
    try std.testing.expectEqualSlices(u8, raw, record);
    record[record.len - 1] ^= 1;
    try std.testing.expectError(error.CorruptLsmWal, validateStateRecordWithAdmission(record, original));
    authority.fail_at = authority.checks + 2;
    try std.testing.expectError(error.NotFound, validateStateRecordWithAdmission(raw, original));
}

test "controlled physical sync WAL append retains every bootstrap phase and publishes once under exhausted resources" {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const cpu = @import("../../common/runnable_scheduler.zig");
    const admission = @import("../../common/read_admission.zig");
    const now = platform.time.monotonicNs;
    const Operation = CurrentStateAppend(AppendTestPublication);
    const Target = @FieldType(Operation, "stage");
    const Mode = enum { begin_failure, end_failure, lifetime_failure };
    for ([_]Target{ .current, .checkpoint_probe, .segment_stat, .create_directory, .directory_parent, .current_replace, .checkpoint_replace, .append, .segment_sync, .segment_parent }) |target| for (std.enums.values(Mode)) |mode| {
        const a = std.testing.allocator;
        var backing = std.testing.FailingAllocator.init(a, .{});
        var pool = storage_io.NativeStoragePool.initWithCapacityForTest(a, 8);
        defer pool.deinit();
        var native = try storage_io.NativeStorage.initWithPool(backing.allocator(), .threaded, &pool);
        var native_live = true;
        defer if (native_live) native.deinit();
        const native_state = native.state;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer a.free(root);
        var state: State = .{};
        defer state.deinit(a);
        try state.upsert(a, .{ .name = "docs" }, "key", "value", false);
        var authority: AppendTestAuthority = .{};
        var preparation_payload = authority.payload();
        const record = try prepareStateRecordWithAdmission(a, &state, .init(&preparation_payload));
        defer a.free(record);
        var operation = try Operation.prepare(root, record.len, .{});
        var slots = try cpu.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 });
        defer slots.close();
        var execution: cpu.Execution = .{};
        defer execution.finish();
        try execution.start(&slots, .{ .io = std.testing.io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 15 * std.time.ns_per_s }, .query);
        var health: cpu.Execution = .{};
        defer health.finish();
        try health.start(&slots, .{ .io = std.testing.io, .native_now_ns = now }, .control);
        const Control = struct {
            operation: *Operation,
            execution: *cpu.Execution,
            slots: *cpu.Scheduler,
            target: Target,
            mode: ?Mode,
            previous: ?Target = null,
            triggered: bool = false,
            reject_publish: bool = false,
            fn lifetime(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.execution.checkLifetime();
                if (self.reject_publish) return error.NotFound;
                if (self.triggered and self.mode == .lifetime_failure) return error.DeadlineExceeded;
            }
            fn check(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try lifetime(raw);
                if (self.slots.state(&self.execution.entry.?) != .running) return error.InvalidRunnableTransition;
                if (self.previous != self.operation.stage) {
                    self.previous = self.operation.stage;
                    storage_io.NativeSyncTestControl.release.store(false, .release);
                }
            }
            fn deadline(raw: *anyopaque) !?u64 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return self.execution.entry.?.options.deadline_ns;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (self.operation.stage == self.target and self.mode != null) {
                    self.triggered = true;
                    if (self.mode == .begin_failure) return error.FileNotFound;
                }
                try self.execution.park(.resource_blocked);
                if (!(self.triggered and self.mode == .lifetime_failure)) storage_io.NativeSyncTestControl.release.store(true, .release);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                storage_io.NativeSyncTestControl.release.store(true, .release);
                if (self.triggered and self.mode == .end_failure) return error.Canceled;
                try self.execution.enter(.query);
            }
        };
        var control: Control = .{ .operation = &operation, .execution = &execution, .slots = &slots, .target = target, .mode = mode };
        const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.lifetime, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const original = admission.Port.init(&payload);
        var held = true;
        var publishes: usize = 0;
        var releases: usize = 0;
        var recover = false;
        storage_io.NativeSyncTestControl.pause.store(true, .release);
        storage_io.NativeSyncTestControl.reads.store(0, .release);
        storage_io.NativeSyncTestControl.calls.store(0, .release);
        storage_io.NativeSyncTestControl.appends.store(0, .release);
        storage_io.NativeSyncTestControl.metadata.store(0, .release);
        var resources = (try native.storage().tryPrepareAppendWithAdmission(record, original)).?;
        operation.start(resources, .{ .held = &held, .publishes = &publishes, .releases = &releases, .recover = &recover }, original) catch |cause| {
            resources.deinit();
            return cause;
        };
        defer {
            storage_io.NativeSyncTestControl.release.store(true, .release);
            storage_io.NativeSyncTestControl.pause.store(false, .release);
            execution.finish();
            operation.deinit();
        }
        native_state.threaded.concurrent_limit = .nothing;
        backing.fail_index = backing.alloc_index;
        backing.resize_fail_index = backing.resize_index;
        const allocations = backing.alloc_index;
        const resizes = backing.resize_index;
        var reserved: usize = 0;
        while (pool.tryReserveDescriptorsForTest(std.testing.io, 1)) reserved += 1;
        defer pool.releaseDescriptorsForTest(std.testing.io, reserved);
        native.deinit();
        native_live = false;
        try std.testing.expectError(switch (mode) {
            .begin_failure => error.FileNotFound,
            .end_failure => error.Canceled,
            .lifetime_failure => error.DeadlineExceeded,
        }, operation.waitWithAdmission(original));
        try std.testing.expectEqual(target, operation.stage);
        try std.testing.expect(held and !recover and publishes == 0 and releases == 0);
        if (mode == .end_failure) try execution.enter(.query);
        control.mode = null;
        storage_io.NativeSyncTestControl.release.store(true, .release);
        const result = try operation.waitWithAdmission(original);
        try std.testing.expect(held and publishes == 0);
        control.reject_publish = true;
        try std.testing.expectError(error.NotFound, operation.publishWithAdmission(original));
        try std.testing.expect(held and publishes == 0);
        control.reject_publish = false;
        _ = try operation.publishWithAdmission(original);
        _ = try operation.publishWithAdmission(original);
        try std.testing.expect(!held and !recover and publishes == 1 and releases == 1);
        try std.testing.expectEqual(record.len, result.bytes);
        try std.testing.expectEqual(@as(u64, 1), result.segment);
        try std.testing.expectEqual(@as(u64, 1), result.segment_syncs);
        try std.testing.expectEqual(@as(u64, 2), result.index_syncs);
        try std.testing.expectEqual(@as(usize, 3), storage_io.NativeSyncTestControl.reads.load(.acquire));
        try std.testing.expectEqual(@as(usize, 3), storage_io.NativeSyncTestControl.metadata.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), storage_io.NativeSyncTestControl.appends.load(.acquire));
        try std.testing.expectEqual(@as(usize, 3), storage_io.NativeSyncTestControl.calls.load(.acquire));
        try std.testing.expectEqual(allocations, backing.alloc_index);
        try std.testing.expectEqual(resizes, backing.resize_index);
        try std.testing.expectEqual(cpu.State.running, slots.state(&health.entry.?));
        var io_storage = storage_io.IoStorage.init(std.testing.io);
        var replayed: State = .{};
        defer replayed.deinit(a);
        const replay = try replayIntoMutable(io_storage.storage(), a, root, &replayed);
        try std.testing.expectEqual(@as(u64, 1), replay.records);
        try std.testing.expectEqualStrings("value", try replayed.get(.{ .name = "docs" }, "key"));
    };
}

test "controlled physical sync WAL append preserves rotation recovery and physical failure semantics" {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const Case = enum { steady, rotation, empty_segment, missing_segment, orphan_checkpoint, orphan_segment, occupied_next, corrupt_current, bad_checksum, zero_segment, partial_append, metadata_failure, abandoned_publication };
    for (std.enums.values(Case)) |case| {
        const a = std.testing.allocator;
        var native = try storage_io.NativeStorage.init(a, .threaded);
        defer native.deinit();
        const store = native.storage();
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
        defer a.free(root);
        var state: State = .{};
        defer state.deinit(a);
        try state.upsert(a, .{ .name = "docs" }, "new", "value", false);
        var authority: AppendTestAuthority = .{};
        var payload = authority.payload();
        const original = @import("../../common/read_admission.zig").Port.init(&payload);
        const record = try prepareStateRecordWithAdmission(a, &state, original);
        defer a.free(record);
        const directory = try walDirPathAlloc(a, root);
        defer a.free(directory);
        try store.createDirPath(directory);
        const current_path = try indexPathAlloc(a, root);
        defer a.free(current_path);
        const first_path = try segmentPathAlloc(a, root, 1);
        defer a.free(first_path);
        const next_path = try segmentPathAlloc(a, root, 2);
        defer a.free(next_path);
        switch (case) {
            .steady, .rotation, .occupied_next, .partial_append => {
                var seed: State = .{};
                defer seed.deinit(a);
                try seed.upsert(a, .{ .name = "docs" }, "seed", "old", false);
                _ = try appendState(store, a, root, &seed, false);
                if (case == .occupied_next) try store.writeFileAbsolute(next_path, "unclaimed-record");
            },
            .empty_segment => {
                try writeCurrentSegment(store, a, root, 1, 999999);
                try store.writeFileAbsolute(first_path, "");
            },
            .missing_segment => try writeCurrentSegment(store, a, root, 1, 0),
            .orphan_checkpoint => try writeCheckpointIndex(store, a, root, .{ .oldest_retained_segment = 1, .covered_through_segment = 0 }),
            .orphan_segment => try store.writeFileAbsolute(first_path, "unclaimed-record"),
            .corrupt_current => try store.writeFileAbsolute(current_path, "bad"),
            .bad_checksum, .zero_segment => {
                try writeCurrentSegment(store, a, root, if (case == .zero_segment) 0 else 1, 0);
                if (case == .bad_checksum) {
                    const bytes = try store.readFileRangeAlloc(a, current_path, 0, control_pair_len);
                    defer a.free(bytes);
                    bytes[bytes.len - 1] ^= 1;
                    try store.writeFileAbsolute(current_path, bytes);
                }
            },
            .metadata_failure, .abandoned_publication => {},
        }
        if (case == .rotation) {
            var warm: [control_pair_len]u8 = undefined;
            try store.readFileRangeInto(a, current_path, 0, &warm);
        }
        var held = true;
        var publishes: usize = 0;
        var releases: usize = 0;
        var recover = false;
        const options: AppendOptions = .{ .segment_bytes = if (case == .rotation or case == .occupied_next or case == .empty_segment) 1 else default_wal_segment_bytes };
        var operation = try CurrentStateAppend(AppendTestPublication).prepare(root, record.len, options);
        var resources = (try store.tryPrepareAppendWithAdmission(record, original)).?;
        operation.start(resources, .{ .held = &held, .publishes = &publishes, .releases = &releases, .recover = &recover }, original) catch |cause| {
            resources.deinit();
            return cause;
        };
        var live = true;
        defer if (live) operation.deinit();
        storage_io.NativeSyncTestControl.appends.store(0, .release);
        if (case == .partial_append) storage_io.NativeSyncTestControl.append_failure_after.store(3, .release);
        if (case == .metadata_failure) storage_io.NativeSyncTestControl.control_failure_after_rename.store(true, .release);
        defer {
            storage_io.NativeSyncTestControl.append_failure_after.store(std.math.maxInt(usize), .release);
            storage_io.NativeSyncTestControl.control_failure_after_rename.store(false, .release);
        }
        const failure: ?anyerror = switch (case) {
            .missing_segment => error.FileNotFound,
            .orphan_checkpoint, .orphan_segment, .occupied_next => error.RecoveryRequired,
            .corrupt_current, .bad_checksum, .zero_segment => error.CorruptLsmWalIndex,
            .partial_append => error.NoSpaceLeft,
            .metadata_failure => error.InputOutput,
            else => null,
        };
        if (failure) |expected| {
            for (0..2) |_| try std.testing.expectError(expected, operation.publishWithAdmission(original));
            try std.testing.expect(!held and releases == 1 and publishes == 0);
            try std.testing.expectEqual(case == .partial_append or case == .metadata_failure, recover);
            try std.testing.expectEqual(@as(usize, if (case == .partial_append) 1 else 0), storage_io.NativeSyncTestControl.appends.load(.acquire));
            if (case == .metadata_failure) {
                // The rename did happen even though namespace durability was
                // unobserved. Never retry or publish this ambiguous outcome.
                const current = try readCurrentSegmentIfPresent(store, a, root);
                try std.testing.expectEqual(@as(u64, 1), current.segment);
            }
        } else {
            const result = try operation.waitWithAdmission(original);
            try std.testing.expect(held and publishes == 0);
            if (case == .abandoned_publication) {
                operation.deinit();
                live = false;
                try std.testing.expect(!held and recover and publishes == 0 and releases == 1);
            } else {
                _ = try operation.publishWithAdmission(original);
                try std.testing.expect(!held and !recover and publishes == 1);
                try std.testing.expectEqual(@as(u64, if (case == .rotation) 2 else 1), result.segment);
                try std.testing.expectEqual(@as(u64, if (case == .rotation) 1 else 0), result.index_syncs);
                try std.testing.expectEqual(case != .steady, result.segment_became_nonempty);
                const current = try readCurrentSegmentIfPresent(store, a, root);
                try std.testing.expectEqual(result.segment, current.segment);
                var replayed: State = .{};
                defer replayed.deinit(a);
                const replay = try replayIntoMutable(store, a, root, &replayed);
                try std.testing.expectEqual(@as(u64, if (case == .empty_segment) 1 else 2), replay.records);
                try std.testing.expectEqualStrings("value", try replayed.get(.{ .name = "docs" }, "new"));
            }
        }
    }
}

test "controlled physical sync WAL append retirement joins accepted writes before poisoning and releasing publication" {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var native = try storage_io.NativeStorage.init(a, .threaded);
    defer native.deinit();
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(root);
    var state: State = .{};
    defer state.deinit(a);
    try state.upsert(a, .{}, "seed", "value", false);
    _ = try appendState(native.storage(), a, root, &state, false);
    const Operation = CurrentStateAppend(AppendTestPublication);
    const Control = struct {
        operation: *Operation,
        fn check(_: *anyopaque) !void {}
        fn deadline(_: *anyopaque) !?u64 {
            return std.math.maxInt(u64);
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.operation.stage == .append) return error.NotFound;
        }
        fn end(_: *anyopaque) !void {}
    };
    var authority: AppendTestAuthority = .{};
    var preparation = authority.payload();
    const record = try prepareStateRecordWithAdmission(a, &state, .init(&preparation));
    defer a.free(record);
    var operation = try Operation.prepare(root, record.len, .{});
    var control: Control = .{ .operation = &operation };
    const admission = @import("../../common/read_admission.zig");
    const payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.check, .check_lifetime = Control.check, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
    const original = admission.Port.init(&payload);
    var held = true;
    var publishes: usize = 0;
    var releases: usize = 0;
    var recover = false;
    var grant = (try native.storage().tryPrepareAppendWithAdmission(record, original)).?;
    operation.start(grant, .{ .held = &held, .publishes = &publishes, .releases = &releases, .recover = &recover }, original) catch |cause| {
        grant.deinit();
        return cause;
    };
    var transferred = false;
    defer if (!transferred) {
        storage_io.NativeSyncTestControl.release.store(true, .release);
        storage_io.NativeSyncTestControl.pause.store(false, .release);
        operation.deinit();
    };
    // The checkpoint gate pauses only append, after metadata reads succeed.
    storage_io.NativeSyncTestControl.calls.store(0, .release);
    storage_io.NativeSyncTestControl.appends.store(0, .release);
    storage_io.NativeSyncTestControl.pause.store(true, .release);
    storage_io.NativeSyncTestControl.release.store(true, .release);
    const Gate = struct {
        operation: *Operation,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.operation.stage == .append) storage_io.NativeSyncTestControl.release.store(false, .release);
        }
    };
    var gate: Gate = .{ .operation = &operation };
    var gated_payload = payload;
    gated_payload.vtable = &.{ .checkpoint = Gate.check, .check_lifetime = Gate.check, .deadline_ns = Control.deadline };
    gated_payload.ptr = &gate;
    // Execution-wait callbacks still reference the original Control.
    try std.testing.expectError(error.NotFound, operation.waitWithAdmission(.init(&gated_payload)));
    const Join = struct {
        operation: *Operation,
        entered: std.Io.Event = .unset,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.entered.set(std.testing.io);
            self.operation.deinit();
            self.done.store(true, .release);
        }
    };
    var joining: Join = .{ .operation = &operation };
    var thread = try std.testing.io.concurrent(Join.run, .{&joining});
    transferred = true;
    defer {
        storage_io.NativeSyncTestControl.release.store(true, .release);
        storage_io.NativeSyncTestControl.pause.store(false, .release);
        thread.await(std.testing.io);
    }
    try joining.entered.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expect(held and !recover and !joining.done.load(.acquire));
    storage_io.NativeSyncTestControl.release.store(true, .release);
    thread.await(std.testing.io);
    try std.testing.expect(!held and recover and publishes == 0 and releases == 1);
    try std.testing.expectEqual(@as(usize, 1), storage_io.NativeSyncTestControl.appends.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), storage_io.NativeSyncTestControl.calls.load(.acquire));
}

/// Physical primary-WAL retirement on one prepaid durability grant. Publication
/// supplies noteMutation(), publishWithAdmission(result, original), release().
/// Its real owner proves durable coverage and excludes appends until retirement.
/// Dedicated replay data is untouched; legacy embedded replay refuses deletion.
pub fn CurrentStateRetirement(comptime Publication: type) type {
    return struct {
        const Self = @This();
        const admission = @import("../../common/read_admission.zig");
        const Stage = enum { current, current_stat, checkpoint, cleanup, catalog, proof_header, proof_payload, rotation_probe, rotation_file, current_replace, floor, remove, directory, marker, ready, published, failed };
        const Pending = union(enum) { none, read: storage_io.ControlFileReadOperation, physical: storage_io.SyncOperation };
        pub const Result = struct { current_segment: u64 = 0, oldest_retained_segment: u64 = 1, covered_through_segment: u64 = 0, reclaimed_through_segment: u64 = 0, deletion_completions: u64 = 0 };
        index: [std.fs.max_path_bytes]u8 = undefined,
        checkpoint: [std.fs.max_path_bytes]u8 = undefined,
        cleanup: [std.fs.max_path_bytes]u8 = undefined,
        catalog: [std.fs.max_path_bytes]u8 = undefined,
        segment: [std.fs.max_path_bytes]u8 = undefined,
        lengths: [4]usize = @splat(0),
        prefix_len: usize,
        segment_len: usize = 0,
        covered: ?u64,
        resources: ?storage_io.DurabilityResources = null,
        publication: ?Publication = null,
        pending: Pending = .none,
        stage: Stage = .current,
        result: Result = .{},
        failure: ?anyerror = null,
        previous_current: u64 = 0,
        previous_covered: u64 = 0,
        max_covered: u64 = 0,
        delete_next: u64 = 1,
        proof_segment: u64 = 1,
        record_offset: u64 = 0,
        record_end: u64 = 0,
        payload_offset: u64 = 0,
        expected_crc: u32 = 0,
        crc: Crc32 = .init(),
        mutation_started: bool = false,

        pub fn memoryBound(storage: storage_io.Storage) !u64 {
            return std.math.add(u64, @sizeOf(Self), try storage.appendResourceBytes(0)) catch error.ResourceBudgetExceeded;
        }
        /// null retires all covered primary state by rotating a fresh current
        /// segment; a finite bound always preserves the existing current file.
        pub fn prepare(root: []const u8, covered_through: ?u64) !Self {
            var self: Self = .{ .covered = covered_through, .prefix_len = 0 };
            const prefix = std.fmt.bufPrint(&self.segment, "{s}{s}wal/", .{ root, if (std.mem.endsWith(u8, root, "/")) "" else "/" }) catch return error.NameTooLong;
            if (prefix.len + 64 >= self.segment.len) return error.NameTooLong;
            self.prefix_len = prefix.len;
            self.lengths[0] = (std.fmt.bufPrint(&self.index, "{s}index", .{prefix}) catch unreachable).len;
            self.lengths[1] = (std.fmt.bufPrint(&self.checkpoint, "{s}checkpoint.index", .{prefix}) catch unreachable).len;
            self.lengths[2] = (std.fmt.bufPrint(&self.cleanup, "{s}reclaimed.index", .{prefix}) catch unreachable).len;
            self.lengths[3] = (std.fmt.bufPrint(&self.catalog, "{s}replay-main.segments", .{prefix}) catch unreachable).len;
            return self;
        }
        pub fn start(self: *Self, resources: storage_io.DurabilityResources, publication: Publication, original: admission.Port) !void {
            if (resources.vtable.begin_delete == null or resources.vtable.begin_read_range == null or resources.vtable.begin_control_replace == null or resources.vtable.retire_with_admission == null) return error.ExecutionWaitUnsupported;
            var offered = resources;
            const initial_read = try offered.beginControlRead(self.index[0..self.lengths[0]], control_pair_len, original);
            self.resources = offered;
            self.publication = publication;
            self.pending = .{ .read = initial_read };
        }
        fn segmentPath(self: *Self, id: u64) []const u8 {
            const tail = std.fmt.bufPrint(self.segment[self.prefix_len..], "{d:0>20}.log", .{id}) catch unreachable;
            self.segment_len = self.prefix_len + tail.len;
            return self.segment[0..self.segment_len];
        }
        const Read = struct { absent: bool = false, file_size: u64 = 0, bytes: []const u8 = &.{} };
        fn read(self: *Self, path: []const u8, offset: ?u64, len: usize, original: admission.Port) !Read {
            if (self.pending == .none) self.pending = .{ .read = if (offset) |at| try self.resources.?.beginReadRange(path, at, len, original) else try self.resources.?.beginControlRead(path, len, original) };
            var trace: admission.RejectionTrace = .{};
            const value = self.pending.read.waitWithAdmission(trace.bind(original).?) catch |cause| {
                if (trace.failure) |failure| return failure;
                self.retirePending();
                if (cause == error.FileNotFound) return .{ .absent = true };
                return self.fail(cause);
            };
            try original.checkpoint();
            return .{ .file_size = value.file_size, .bytes = value.bytes };
        }
        fn pair(self: *Self, read_result: Read, kind: ControlKind) ![]const u8 {
            if (read_result.file_size != control_pair_len or read_result.bytes.len != control_pair_len) return self.fail(error.CorruptLsmWalIndex);
            return decodeControl(read_result.bytes, kind, 16) catch |cause| return self.fail(cause);
        }
        fn mutate(self: *Self) !void {
            if (!self.mutation_started) {
                try self.publication.?.noteMutation();
                self.mutation_started = true;
            }
        }
        fn waitPhysical(self: *Self, original: admission.Port) !void {
            var trace: admission.RejectionTrace = .{};
            self.pending.physical.waitWithAdmission(trace.bind(original).?) catch |cause| {
                if (trace.failure) |failure| return failure;
                return self.fail(cause);
            };
            self.retirePending();
        }
        fn replace(self: *Self, path: []const u8, kind: ?ControlKind, first: u64, second: u64, original: admission.Port) !void {
            if (self.pending == .none) {
                try self.mutate();
                var payload: [16]u8 = undefined;
                var raw: [control_pair_len]u8 = undefined;
                std.mem.writeInt(u64, payload[0..8], first, .little);
                std.mem.writeInt(u64, payload[8..16], second, .little);
                if (kind) |tag| encodeControl(&raw, tag, &payload);
                self.pending = .{ .physical = try self.resources.?.beginControlReplace(path, if (kind != null) &raw else "", original) };
            }
            try self.waitPhysical(original);
        }
        fn afterProof(self: *Self) void {
            self.stage = if (self.result.current_segment != self.previous_current) .rotation_probe else .floor;
        }
        pub fn waitWithAdmission(self: *Self, original: admission.Port) !Result {
            _ = (try original.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
            _ = (try original.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
            while (true) {
                try original.checkpoint();
                switch (self.stage) {
                    .ready, .published => return self.result,
                    .failed => return self.failure.?,
                    .current => {
                        const value = try self.read(self.index[0..self.lengths[0]], null, control_pair_len, original);
                        if (value.absent) {
                            self.stage = .ready;
                            continue;
                        }
                        const data = try self.pair(value, .current_segment);
                        self.previous_current = std.mem.readInt(u64, data[0..8], .little);
                        if (self.previous_current == 0) return self.fail(error.CorruptLsmWalIndex);
                        self.result.current_segment = self.previous_current;
                        self.max_covered = if (self.covered) |cap| @min(cap, self.previous_current - 1) else self.previous_current;
                        self.retirePending();
                        self.stage = .current_stat;
                    },
                    .current_stat => {
                        const value = try self.read(self.segmentPath(self.previous_current), null, 1, original);
                        if (value.absent) return self.fail(error.FileNotFound);
                        if (self.covered == null) {
                            if (value.file_size == 0) self.max_covered = self.previous_current - 1 else self.result.current_segment = std.math.add(u64, self.previous_current, 1) catch return self.fail(error.ResourceBudgetExceeded);
                        }
                        self.retirePending();
                        self.stage = .checkpoint;
                    },
                    .checkpoint => {
                        const value = try self.read(self.checkpoint[0..self.lengths[1]], null, control_pair_len, original);
                        if (!value.absent) {
                            const data = try self.pair(value, .checkpoint);
                            self.result.oldest_retained_segment = std.mem.readInt(u64, data[0..8], .little);
                            self.previous_covered = std.mem.readInt(u64, data[8..16], .little);
                            if (self.result.oldest_retained_segment == 0 or self.previous_covered == std.math.maxInt(u64) or self.previous_covered + 1 < self.result.oldest_retained_segment or self.result.oldest_retained_segment > self.previous_current) return self.fail(error.CorruptLsmWalIndex);
                        }
                        self.result.covered_through_segment = @max(self.previous_covered, self.max_covered);
                        self.retirePending();
                        self.stage = .cleanup;
                    },
                    .cleanup => {
                        const value = try self.read(self.cleanup[0..self.lengths[2]], null, control_pair_len, original);
                        if (!value.absent) {
                            const data = try self.pair(value, .reclamation);
                            const cleaned = std.mem.readInt(u64, data[0..8], .little);
                            const covered = std.mem.readInt(u64, data[8..16], .little);
                            if (cleaned > covered) return self.fail(error.CorruptLsmWalIndex);
                            // Legacy reset can reuse IDs; its checkpoint reset
                            // invalidates a watermark from the former generation.
                            if (covered <= self.previous_covered) self.result.reclaimed_through_segment = cleaned;
                        }
                        self.delete_next = self.result.reclaimed_through_segment + 1;
                        self.proof_segment = self.delete_next;
                        self.retirePending();
                        if (self.delete_next > self.max_covered) {
                            self.result.current_segment = self.previous_current;
                            self.stage = .ready;
                        } else self.stage = .catalog;
                    },
                    .catalog => {
                        const value = try self.read(self.catalog[0..self.lengths[3]], null, 1, original);
                        if (!value.absent and value.file_size != 0) return self.fail(error.ExecutionWaitUnsupported);
                        self.retirePending();
                        if (value.absent) self.stage = .proof_header else self.afterProof();
                    },
                    .proof_header => {
                        if (self.proof_segment > self.max_covered) {
                            self.afterProof();
                            continue;
                        }
                        const value = try self.read(self.segmentPath(self.proof_segment), self.record_offset, record_header_len, original);
                        if (value.absent or self.record_offset == value.file_size) {
                            self.retirePending();
                            self.proof_segment += 1;
                            self.record_offset = 0;
                            continue;
                        }
                        if (value.bytes.len != record_header_len) return self.fail(error.CorruptLsmWal);
                        if (std.mem.readInt(u32, value.bytes[0..4], .little) == replay_record_magic) return self.fail(error.ExecutionWaitUnsupported);
                        if (std.mem.readInt(u32, value.bytes[0..4], .little) != record_magic or std.mem.readInt(u16, value.bytes[4..6], .little) != record_version or std.mem.readInt(u16, value.bytes[6..8], .little) != record_header_len) return self.fail(error.CorruptLsmWal);
                        const payload_len = std.mem.readInt(u32, value.bytes[8..12], .little);
                        self.payload_offset = self.record_offset + record_header_len;
                        self.record_end = std.math.add(u64, self.payload_offset, payload_len) catch return self.fail(error.CorruptLsmWal);
                        if (payload_len < 4 or self.record_end > value.file_size) return self.fail(error.CorruptLsmWal);
                        self.expected_crc = std.mem.readInt(u32, value.bytes[12..16], .little);
                        self.crc = .init();
                        self.retirePending();
                        self.stage = .proof_payload;
                    },
                    .proof_payload => {
                        const len: usize = @intCast(@min(16 * 1024, self.record_end - self.payload_offset));
                        const value = try self.read(self.segmentPath(self.proof_segment), self.payload_offset, len, original);
                        if (value.absent or value.bytes.len != len) return self.fail(error.CorruptLsmWal);
                        self.crc.update(value.bytes);
                        self.payload_offset += len;
                        self.retirePending();
                        if (self.payload_offset == self.record_end) {
                            if (self.crc.final() != self.expected_crc) return self.fail(error.CorruptLsmWal);
                            self.record_offset = self.record_end;
                            self.stage = .proof_header;
                        }
                    },
                    .rotation_probe => {
                        const value = try self.read(self.segmentPath(self.result.current_segment), null, 1, original);
                        if (!value.absent and value.file_size != 0) return self.fail(error.RecoveryRequired);
                        self.retirePending();
                        self.stage = .rotation_file;
                    },
                    .rotation_file => {
                        try self.replace(self.segmentPath(self.result.current_segment), null, 0, 0, original);
                        self.stage = .current_replace;
                    },
                    .current_replace => {
                        try self.replace(self.index[0..self.lengths[0]], .current_segment, self.result.current_segment, 0, original);
                        self.stage = .floor;
                    },
                    .floor => {
                        self.result.oldest_retained_segment = @max(self.result.oldest_retained_segment, self.max_covered + 1);
                        try self.replace(self.checkpoint[0..self.lengths[1]], .checkpoint, self.result.oldest_retained_segment, self.result.covered_through_segment, original);
                        self.stage = .remove;
                    },
                    .remove => {
                        if (self.delete_next > self.max_covered) {
                            self.stage = .directory;
                            continue;
                        }
                        if (self.pending == .none) self.pending = .{ .physical = try self.resources.?.beginDeleteFile(self.segmentPath(self.delete_next), original) };
                        try self.waitPhysical(original);
                        self.delete_next += 1;
                        self.result.deletion_completions += 1;
                    },
                    .directory => {
                        if (self.pending == .none) self.pending = .{ .physical = try self.resources.?.beginSyncParent(self.checkpoint[0..self.lengths[1]], original) };
                        try self.waitPhysical(original);
                        self.stage = .marker;
                    },
                    .marker => {
                        try self.replace(self.cleanup[0..self.lengths[2]], .reclamation, self.max_covered, self.result.covered_through_segment, original);
                        self.result.reclaimed_through_segment = self.max_covered;
                        self.stage = .ready;
                    },
                }
            }
        }
        pub fn publishWithAdmission(self: *Self, original: admission.Port) !Result {
            const result = try self.waitWithAdmission(original);
            if (self.stage == .published) return result;
            try self.publication.?.publishWithAdmission(result, original);
            self.releasePublication();
            self.stage = .published;
            return result;
        }
        fn fail(self: *Self, cause: anyerror) anyerror {
            self.retirePending();
            self.failure = cause;
            self.stage = .failed;
            self.releasePublication();
            return cause;
        }
        fn retirePending(self: *Self) void {
            switch (self.pending) {
                .read => |*value| value.deinit(),
                .physical => |*value| value.deinit(),
                .none => {},
            }
            self.pending = .none;
        }
        fn releasePublication(self: *Self) void {
            if (self.publication) |*publication| publication.release();
            self.publication = null;
        }
        /// Abandon further logical work while joining the exact accepted read
        /// or write under independent execution. Publication remains owned until
        /// that join; the retained grant retires separately.
        pub fn abortWithAdmission(self: *Self, original: admission.Port) !void {
            try original.checkpoint();
            var trace: admission.RejectionTrace = .{};
            switch (self.pending) {
                .read => |*value| {
                    _ = value.waitWithAdmission(trace.bind(original).?) catch {
                        if (trace.failure) |rejected| return rejected;
                        self.joinAndRelease();
                        return;
                    };
                },
                .physical => |*value| value.waitWithAdmission(trace.bind(original).?) catch {
                    if (trace.failure) |rejected| return rejected;
                },
                .none => {},
            }
            self.joinAndRelease();
        }
        /// Exclusive mandatory fallback when controlled join is unsupported.
        /// Keep the grant owned for later retirement instead of destroying it.
        pub fn joinAndRelease(self: *Self) void {
            self.retirePending();
            self.releasePublication();
            self.failure = error.Canceled;
            self.stage = .failed;
        }
        pub fn retireWithAdmission(self: *Self, original: admission.Port) !void {
            try original.checkpoint();
            if (self.stage != .published and self.stage != .failed) return error.ResourceBudgetExceeded;
            if (self.resources) |*resources| {
                try resources.retireWithAdmission(original);
                self.resources = null;
            }
        }
        /// Quiesced fallback joins accepted work before releasing authority.
        pub fn deinit(self: *Self) void {
            self.retirePending();
            self.releasePublication();
            if (self.resources) |*resources| resources.deinit();
            self.* = undefined;
        }
    };
}
