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
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);
const backend_types = @import("../backend_types.zig");
const change_journal_mod = @import("../db/derived/change_journal.zig");
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
const committed_segment_entry_len: usize = 16;
const max_retained_replay_pending_bytes: usize = replay_chunk_bytes + @max(record_header_len, replay_record_header_len);

fn scratchAllocator(allocator: Allocator) Allocator {
    return if (builtin.link_libc) std.heap.c_allocator else allocator;
}

pub const ReplayEntry = backend_types.ReplayEntry;

pub const AppendOptions = struct {
    segment_bytes: u64 = default_wal_segment_bytes,
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
    on_applied_entry: ?*const fn (ctx: *anyopaque, segment: u64) anyerror!void = null,
    on_applied_record: *const fn (ctx: *anyopaque, segment: u64, entries: u64) anyerror!void,
};

pub const RetentionStats = struct {
    segments: u64 = 0,
    bytes: u64 = 0,
    oldest_retained_segment: u64 = 0,
    current_segment: u64 = 0,
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

    fn deinit(self: *@This(), allocator: Allocator) void {
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
    if (state.entries.items.len == 0) return 0;

    const payload_len = encodedPayloadLen(state);
    var record = try allocator.alloc(u8, record_header_len + payload_len);
    defer allocator.free(record);
    const payload = record[record_header_len..];
    try encodePayloadIntoSlice(payload, state);

    std.mem.writeInt(u32, record[0..4], record_magic, .little);
    std.mem.writeInt(u16, record[4..6], record_version, .little);
    std.mem.writeInt(u16, record[6..8], record_header_len, .little);
    std.mem.writeInt(u32, record[8..12], @intCast(payload.len), .little);
    std.mem.writeInt(u32, record[12..16], std.hash.Crc32.hash(payload), .little);

    const wal_dir = try walDirPathAlloc(allocator, root_dir);
    defer allocator.free(wal_dir);
    try storage.createDirPath(wal_dir);

    const current = try readCurrentSegment(storage, allocator, root_dir);
    var segment = current.segment;
    var current_size = current.size;
    if (!current.index_exists) {
        try writeCurrentSegment(storage, allocator, root_dir, segment, current_size);
        try writeCheckpointIndex(storage, allocator, root_dir, .{
            .oldest_retained_segment = 1,
            .covered_through_segment = 0,
        });
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
        segment_path = try segmentPathAlloc(allocator, root_dir, segment);
        segment_path_owned = true;
    }
    storage.appendFileAbsolute(allocator, segment_path, record, sync) catch |err| switch (err) {
        error.FileNotFound => {
            try storage.createDirPath(wal_dir);
            try storage.appendFileAbsolute(allocator, segment_path, record, sync);
        },
        else => return err,
    };
    current_size += record.len;
    if (!current.index_exists or current.segment != segment or current.size != current_size) {
        try writeCurrentSegment(storage, allocator, root_dir, segment, current_size);
    }
    allocator.free(segment_path);
    segment_path_owned = false;
    return record.len;
}

pub fn encodedStateRecordLen(state: anytype) usize {
    return record_header_len + encodedPayloadLen(state);
}

pub fn currentSegment(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !u64 {
    return (try readCurrentSegment(storage, allocator, root_dir)).segment;
}

pub fn syncCurrentState(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !void {
    const current = readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    const temp_allocator = allocator;
    const segment_path = try segmentPathAlloc(temp_allocator, root_dir, current.segment);
    defer temp_allocator.free(segment_path);
    try storage.appendFileAbsolute(allocator, segment_path, "", true);

    const index_path = try indexPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(index_path);
    try storage.appendFileAbsolute(allocator, index_path, "", true);
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
    var stats = ReplayStats{};
    const legacy_path = try legacyPathAlloc(allocator, root_dir);
    defer allocator.free(legacy_path);
    try replayFileStreaming(storage, allocator, legacy_path, 0, mutable, &stats, hooks, .{});

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
        });
        allocator.free(segment_path);
    }

    return stats;
}

pub fn reset(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !void {
    const wal_dir = try walDirPathAlloc(allocator, root_dir);
    defer allocator.free(wal_dir);
    try storage.createDirPath(wal_dir);

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
    std.mem.writeInt(u32, record[20..24], std.hash.Crc32.hash(payload), .little);
    @memcpy(record[replay_record_header_len..], payload);

    const wal_dir = try walDirPathAlloc(allocator, root_dir);
    defer allocator.free(wal_dir);
    try storage.createDirPath(wal_dir);
    const replay_dir = try replayDirPathAlloc(allocator, root_dir);
    defer allocator.free(replay_dir);
    try storage.createDirPath(replay_dir);

    const index = try readReplayIndex(storage, allocator, root_dir);
    if (!index.exists) {
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
    if (current_size > 0 and current_size + record.len > options.segment_bytes) {
        segment += 1;
        allocator.free(segment_path);
        segment_path_owned = false;
        segment_path = try replaySegmentPathAlloc(allocator, root_dir, segment);
        segment_path_owned = true;
    }

    try appendReplaySegmentEntryIfNeeded(storage, allocator, root_dir, segment, sequence, true);
    try storage.appendFileAbsolute(allocator, segment_path, record, sync);
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
    try scratch.pending.ensureTotalCapacityPrecise(scratch_allocator, max_retained_replay_pending_bytes);

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
        releaseOversizedReplayPendingBuffer(chunk_allocator, pending);
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
                releaseOversizedReplayPendingBuffer(chunk_allocator, pending);
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
        releaseOversizedReplayPendingBuffer(chunk_allocator, pending);
        if (read_len < replay_chunk_bytes) break;
    }
    if (saw_bytes) stats.segments += 1;
    if (pending.items.len > 0) stats.truncated_tail_bytes += @intCast(pending.items.len);
}

fn releaseOversizedReplayPendingBuffer(allocator: Allocator, pending: *std.ArrayListUnmanaged(u8)) void {
    if (pending.capacity <= max_retained_replay_pending_bytes) return;
    if (pending.items.len > max_retained_replay_pending_bytes) return;
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
            if (std.hash.Crc32.hash(payload) != expected_crc) {
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
                if (std.hash.Crc32.hash(payload) != expected_crc) {
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
    try pending.ensureTotalCapacityPrecise(allocator, max_retained_replay_pending_bytes);

    var offset: u64 = 0;
    while (offset < file_size) {
        const len: usize = @intCast(@min(@as(u64, replay_chunk_bytes), file_size - offset));
        const start = pending.items.len;
        try pending.ensureUnusedCapacity(allocator, len);
        pending.items.len += len;
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
                releaseOversizedReplayPendingBuffer(allocator, &pending);
                break;
            },
            else => return err,
        };
        releaseOversizedReplayPendingBuffer(allocator, &pending);
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
            if (std.hash.Crc32.hash(payload) != expected_crc) {
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
            if (std.hash.Crc32.hash(payload) != expected_crc) {
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
            var raw: [committed_segment_entry_len]u8 = undefined;
            std.mem.writeInt(u64, raw[0..8], segment, .little);
            std.mem.writeInt(u64, raw[8..16], sequence, .little);
            try storage.writeFileAbsolute(segments_path, &raw);
            return;
        },
        else => return err,
    };
    if (size == 0) {
        if (!allow_create) return;
        var raw: [committed_segment_entry_len]u8 = undefined;
        std.mem.writeInt(u64, raw[0..8], segment, .little);
        std.mem.writeInt(u64, raw[8..16], sequence, .little);
        try storage.writeFileAbsolute(segments_path, &raw);
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
    const last_segment = std.mem.readInt(u64, tail[0..8], .little);
    if (last_segment == segment) return;

    var raw: [committed_segment_entry_len]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], segment, .little);
    std.mem.writeInt(u64, raw[8..16], sequence, .little);
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
            var raw: [committed_segment_entry_len]u8 = undefined;
            std.mem.writeInt(u64, raw[0..8], segment, .little);
            std.mem.writeInt(u64, raw[8..16], sequence, .little);
            try storage.writeFileAbsolute(segments_path, &raw);
            return;
        },
        else => return err,
    };
    if (size == 0) {
        if (!allow_create) return;
        var raw: [committed_segment_entry_len]u8 = undefined;
        std.mem.writeInt(u64, raw[0..8], segment, .little);
        std.mem.writeInt(u64, raw[8..16], sequence, .little);
        try storage.writeFileAbsolute(segments_path, &raw);
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
    const last_segment = std.mem.readInt(u64, tail[0..8], .little);
    if (last_segment == segment) return;

    var raw: [committed_segment_entry_len]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], segment, .little);
    std.mem.writeInt(u64, raw[8..16], sequence, .little);
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
        const segment = std.mem.readInt(u64, raw[pos..][0..8], .little);
        const first_sequence = std.mem.readInt(u64, raw[pos + 8 ..][0..8], .little);
        if (segment == 0 or first_sequence == 0) return error.CorruptLsmWalIndex;
        if (first_sequence > from_sequence) break;
        start_segment = segment;
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
        const segment = std.mem.readInt(u64, raw[pos..][0..8], .little);
        const first_sequence = std.mem.readInt(u64, raw[pos + 8 ..][0..8], .little);
        if (segment == 0 or first_sequence == 0) return error.CorruptLsmWalIndex;
        if (first_sequence > from_sequence) break;
        start_segment = segment;
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
    const segment = readCurrentSegmentIfPresent(storage, allocator, root_dir) catch |err| switch (err) {
        error.FileNotFound => return .{ .segment = 1, .size = 0, .index_exists = false },
        else => return err,
    };
    return .{
        .segment = segment.segment,
        .size = segment.size,
        .index_exists = true,
    };
}

fn readCurrentSegmentIfPresent(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8) !CurrentSegment {
    const temp_allocator = allocator;
    const index_path = try indexPathAlloc(temp_allocator, root_dir);
    defer temp_allocator.free(index_path);
    const index_size = try storage.fileSize(index_path);
    if (index_size != 16) return error.CorruptLsmWalIndex;
    const raw = try storage.readFileRangeAlloc(temp_allocator, index_path, 0, 16);
    defer temp_allocator.free(raw);
    if (raw.len != 16) return error.CorruptLsmWalIndex;
    const segment = std.mem.readInt(u64, raw[0..8], .little);
    if (segment == 0) return error.CorruptLsmWalIndex;
    return .{
        .segment = segment,
        .size = std.mem.readInt(u64, raw[8..16], .little),
        .index_exists = true,
    };
}

fn writeCurrentSegment(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8, segment: u64, size: u64) !void {
    var raw: [16]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], segment, .little);
    std.mem.writeInt(u64, raw[8..16], size, .little);
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
    if (index_size != 16) return error.CorruptLsmWalIndex;
    const raw = try storage.readFileRangeAlloc(temp_allocator, checkpoint_path, 0, 16);
    defer temp_allocator.free(raw);
    if (raw.len != 16) return error.CorruptLsmWalIndex;
    const oldest_retained_segment = std.mem.readInt(u64, raw[0..8], .little);
    const covered_through_segment = std.mem.readInt(u64, raw[8..16], .little);
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
    var raw: [16]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], index.oldest_retained_segment, .little);
    std.mem.writeInt(u64, raw[8..16], index.covered_through_segment, .little);
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
    var raw: [24]u8 = undefined;
    storage.readFileRangeInto(temp_allocator, index_path, 0, &raw) catch |err| switch (err) {
        error.FileNotFound => return .{},
        error.EndOfStream => return error.CorruptLsmWalIndex,
        else => return err,
    };
    const current_segment = std.mem.readInt(u64, raw[0..8], .little);
    const next_sequence = std.mem.readInt(u64, raw[8..16], .little);
    const truncated_through = std.mem.readInt(u64, raw[16..24], .little);
    if (current_segment == 0 or next_sequence == 0) return error.CorruptLsmWalIndex;
    return .{
        .current_segment = current_segment,
        .next_sequence = next_sequence,
        .truncated_through = truncated_through,
        .exists = true,
    };
}

fn writeReplayIndex(storage: storage_io.Storage, allocator: Allocator, root_dir: []const u8, index: ReplayIndex) !void {
    var raw: [24]u8 = undefined;
    std.mem.writeInt(u64, raw[0..8], index.current_segment, .little);
    std.mem.writeInt(u64, raw[8..16], index.next_sequence, .little);
    std.mem.writeInt(u64, raw[16..24], index.truncated_through, .little);
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
    try appendInt(out, allocator, u32, @intCast(state.entries.items.len));
    for (state.entries.items) |entry| {
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
    writeIntToPayload(out, &pos, u32, @intCast(state.entries.items.len));
    for (state.entries.items) |entry| {
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
    for (state.entries.items) |entry| {
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
                try on_applied_entry(active_hooks.ctx, segment);
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

    try pending.ensureTotalCapacityPrecise(std.testing.allocator, max_retained_replay_pending_bytes * 2);
    try std.testing.expect(pending.capacity > max_retained_replay_pending_bytes);

    pending.items.len = max_retained_replay_pending_bytes + 1;
    releaseOversizedReplayPendingBuffer(std.testing.allocator, &pending);
    try std.testing.expect(pending.capacity > max_retained_replay_pending_bytes);

    pending.items.len = 8;
    releaseOversizedReplayPendingBuffer(std.testing.allocator, &pending);
    try std.testing.expectEqual(@as(usize, 8), pending.items.len);
    try std.testing.expect(pending.capacity <= max_retained_replay_pending_bytes);
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

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
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

test "lsm wal current segment index stores cached segment size" {
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
    try std.testing.expectEqual(@as(u64, 16), try storage.storage().fileSize(index_path));

    const segment_path = try segmentPathAlloc(std.testing.allocator, root_dir, 1);
    defer std.testing.allocator.free(segment_path);
    const current = try readCurrentSegment(storage.storage(), std.testing.allocator, root_dir);
    try std.testing.expectEqual(@as(u64, 1), current.segment);
    try std.testing.expectEqual(try storage.storage().fileSize(segment_path), current.size);
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

    try reset(storage.storage(), std.testing.allocator, root_dir);

    const after_reset = try snapshotRetention(storage.storage(), std.testing.allocator, root_dir);
    try std.testing.expectEqual(@as(u64, 1), after_reset.oldest_retained_segment);
    try std.testing.expectEqual(@as(u64, 1), after_reset.current_segment);
    try std.testing.expectEqual(@as(u64, 0), after_reset.segments);
    try std.testing.expectEqual(@as(u64, 0), after_reset.bytes);
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
    try std.testing.expectEqual(@as(u64, 24), try storage.storage().fileSize(replay_index_path));

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
