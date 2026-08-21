// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

//! Durable publication for immutable vector posting segments plus a bounded
//! committed WAL tail.
//!
//! Publication order is segment -> empty next-generation WAL -> CURRENT. A
//! crash before CURRENT leaves only unreferenced artifacts; a crash after it
//! leaves a complete recoverable generation. Old artifacts are removed only
//! after CURRENT is durably replaced.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const lsm_backend = @import("lsm_backend/mod.zig");
const vectorindex = @import("antfly_vectorindex");
const posting_segment = vectorindex.posting_segment;
const posting_wal = vectorindex.posting_wal;

const current_name = "CURRENT";
const max_control_bytes = posting_wal.Checkpoint.encoded_len;
const max_wal_bytes: usize = 512 * 1024 * 1024;
const max_segment_bytes: usize = if (@sizeOf(usize) >= 8) 4 * 1024 * 1024 * 1024 else std.math.maxInt(usize);

pub const RetainedSegment = union(enum) {
    heap: []u8,
    mapped: []align(std.heap.page_size_min) u8,

    pub fn bytes(self: RetainedSegment) []const u8 {
        return switch (self) {
            .heap => |data| data,
            .mapped => |data| data,
        };
    }

    pub fn isMapped(self: RetainedSegment) bool {
        return self == .mapped;
    }

    pub fn deinit(self: *RetainedSegment, alloc: Allocator) void {
        switch (self.*) {
            .heap => |data| alloc.free(data),
            .mapped => |data| if (builtin.os.tag != .freestanding and builtin.os.tag != .windows and builtin.os.tag != .wasi)
                std.posix.munmap(data)
            else
                unreachable,
        }
        self.* = undefined;
    }
};

pub const BatchRecord = struct {
    kind: posting_wal.RecordKind,
    posting_id: posting_wal.PostingId,
    source_sequence: u64,
    payload: []const u8,
};

pub const AppendOptions = struct {
    sync: bool = true,
};

pub const CheckpointPolicy = struct {
    min_wal_bytes: u64 = 1 * 1024 * 1024,
    max_wal_bytes: u64 = 64 * 1024 * 1024,
    wal_to_segment_percent: u8 = 50,
};

pub const RecoveredWal = struct {
    alloc: Allocator,
    bytes: []u8,
    replay: posting_wal.Replay,

    pub fn deinit(self: *RecoveredWal) void {
        self.replay.deinit();
        self.alloc.free(self.bytes);
        self.* = undefined;
    }
};

pub const Store = struct {
    alloc: Allocator,
    storage: lsm_backend.Storage,
    root_dir: []u8,
    checkpoint: ?posting_wal.Checkpoint,
    wal_generation: u64,
    wal_committed_bytes: u64,
    last_committed_batch: ?u64,
    covered_source_sequence: u64,
    segment_bytes: u64,
    poisoned: bool,

    pub fn open(alloc: Allocator, storage: lsm_backend.Storage, root_dir: []const u8) !Store {
        return try openInternal(alloc, storage, root_dir, null);
    }

    /// Opens and validates the store while retaining the already-read segment
    /// bytes for a read-serving caller. The caller owns both returned fields.
    /// This avoids reading and checksumming a large immutable segment twice on
    /// process startup.
    pub fn openWithSegmentAlloc(alloc: Allocator, storage: lsm_backend.Storage, root_dir: []const u8) !OpenedWithSegment {
        var retained_segment: ?RetainedSegment = null;
        var store = try openInternal(alloc, storage, root_dir, &retained_segment);
        errdefer store.deinit();
        return .{
            .store = store,
            .segment = retained_segment orelse return error.MissingPostingCheckpoint,
        };
    }

    fn openInternal(
        alloc: Allocator,
        storage: lsm_backend.Storage,
        root_dir: []const u8,
        retained_segment: ?*?RetainedSegment,
    ) !Store {
        const owned_root = try alloc.dupe(u8, root_dir);
        storage.createDirPath(owned_root) catch |err| {
            alloc.free(owned_root);
            return err;
        };

        var store: Store = .{
            .alloc = alloc,
            .storage = storage,
            .root_dir = owned_root,
            .checkpoint = null,
            .wal_generation = 1,
            .wal_committed_bytes = 0,
            .last_committed_batch = null,
            .covered_source_sequence = 0,
            .segment_bytes = 0,
            .poisoned = false,
        };
        errdefer store.deinit();

        const current_path = try store.currentPathAlloc();
        defer alloc.free(current_path);
        // std.Io's limited read treats reaching the limit as StreamTooLong;
        // reserve one byte beyond the accepted payload so an exactly sized
        // file can reach EOF and still be admitted by the codec below.
        const current = storage.readFileAlloc(alloc, current_path, max_control_bytes + 1) catch |err| switch (err) {
            error.FileNotFound => {
                try store.replaceWal(&.{});
                return store;
            },
            else => return err,
        };
        defer alloc.free(current);
        const checkpoint = try posting_wal.Checkpoint.decode(current);
        store.checkpoint = checkpoint;
        store.wal_generation = checkpoint.wal_generation;
        store.covered_source_sequence = checkpoint.covered_source_sequence;

        var retained = try store.readSegmentRetainedFor(checkpoint);
        var segment_owned = true;
        defer if (segment_owned) retained.deinit(alloc);
        const segment_bytes = retained.bytes();
        store.segment_bytes = @intCast(segment_bytes.len);
        var recovered = try store.recoverWal();
        defer recovered.deinit();
        if (recovered.replay.committed_bytes < checkpoint.wal_committed_bytes) {
            return error.PostingWalShorterThanCheckpoint;
        }
        for (recovered.replay.records.items) |record| {
            if (record.source_sequence < checkpoint.covered_source_sequence) {
                return error.PostingWalOverlapsCheckpoint;
            }
        }

        // Never append after an incomplete or uncommitted tail: later frames
        // would remain permanently hidden behind the first ignored bytes.
        if (recovered.bytes.len != recovered.replay.committed_bytes) {
            try store.replaceWal(recovered.bytes[0..recovered.replay.committed_bytes]);
        }
        store.wal_committed_bytes = recovered.replay.committed_bytes;
        store.last_committed_batch = recovered.replay.last_committed_batch;
        store.covered_source_sequence = @max(
            checkpoint.covered_source_sequence,
            recovered.replay.covered_source_sequence,
        );
        if (retained_segment) |out| {
            out.* = retained;
            segment_owned = false;
        }
        return store;
    }

    pub fn deinit(self: *Store) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn appendBatch(self: *Store, batch_id: u64, records: []const BatchRecord, covered_source_sequence: u64) !void {
        return try self.appendBatchWithOptions(batch_id, records, covered_source_sequence, .{});
    }

    /// Returns the next monotonically ordered derived batch id in the current
    /// WAL generation. Batch order is deliberately independent from source
    /// sequence order: maintenance may commit multiple posting mutations at
    /// the same authoritative source sequence.
    pub fn nextBatchId(self: *const Store) !u64 {
        return std.math.add(u64, self.last_committed_batch orelse 0, 1) catch
            error.PostingWalBatchOverflow;
    }

    pub fn appendBatchWithOptions(
        self: *Store,
        batch_id: u64,
        records: []const BatchRecord,
        covered_source_sequence: u64,
        options: AppendOptions,
    ) !void {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        if (self.checkpoint == null) return error.MissingPostingCheckpoint;
        if (records.len == 0) return error.EmptyPostingWalBatch;
        for (records) |record| {
            if (record.source_sequence < self.covered_source_sequence) return error.PostingWalOverlapsCheckpoint;
            if (record.source_sequence > covered_source_sequence) return error.InvalidPostingWalCommit;
        }
        var writer = posting_wal.Writer.initAfterCommitted(
            self.alloc,
            self.last_committed_batch,
            self.covered_source_sequence,
        );
        defer writer.deinit();
        for (records) |record| {
            try writer.append(record.kind, batch_id, record.posting_id, record.source_sequence, record.payload);
        }
        try writer.commit(batch_id, covered_source_sequence);
        const next_committed_bytes = std.math.add(u64, self.wal_committed_bytes, writer.bytes().len) catch
            return error.PostingWalTooLarge;
        if (next_committed_bytes > max_wal_bytes) return error.PostingWalTooLarge;

        const wal_path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(wal_path);
        self.storage.appendFileAbsolute(self.alloc, wal_path, writer.bytes(), options.sync) catch |err| {
            // The storage error may be ambiguous (for example fsync failed
            // after the append reached the page cache). Refuse retries on this
            // handle; reopen reparses the durable prefix without risking a
            // duplicate committed batch.
            self.poisoned = true;
            return err;
        };
        self.wal_committed_bytes = next_committed_bytes;
        self.last_committed_batch = batch_id;
        self.covered_source_sequence = covered_source_sequence;
    }

    pub fn appendCoverage(self: *Store, batch_id: u64, covered_source_sequence: u64, options: AppendOptions) !void {
        return try self.appendBatchWithOptions(batch_id, &.{.{
            .kind = .coverage,
            .posting_id = 0,
            .source_sequence = covered_source_sequence,
            .payload = &.{},
        }}, covered_source_sequence, options);
    }

    pub fn shouldCheckpoint(self: *const Store, policy: CheckpointPolicy) bool {
        if (self.checkpoint == null or self.wal_committed_bytes == 0) return false;
        const proportional = std.math.mul(u64, self.segment_bytes, policy.wal_to_segment_percent) catch
            std.math.maxInt(u64);
        const ratio_threshold = proportional / 100;
        const threshold = @min(policy.max_wal_bytes, @max(policy.min_wal_bytes, ratio_threshold));
        return self.wal_committed_bytes >= threshold;
    }

    pub fn publishCheckpoint(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        segment_bytes: []const u8,
    ) !void {
        return try self.publishCheckpointInternal(
            segment_generation,
            covered_source_sequence,
            segment_bytes,
            null,
            false,
        );
    }

    /// Durably writes an unreferenced immutable segment. Until a later CURRENT
    /// publication names it, a crash leaves only a harmless orphan. This lets
    /// checkpoint construction and its large file write run off the foreground
    /// replay path.
    pub fn stageCheckpointSegment(
        self: *Store,
        segment_generation: u64,
        segment_bytes: []const u8,
    ) !void {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        if (self.checkpoint) |current| {
            if (segment_generation <= current.segment_generation) return error.OutOfOrderPostingSegmentGeneration;
        }
        _ = try posting_segment.Reader.init(segment_bytes);
        const segment_path = try self.segmentPathAlloc(segment_generation);
        defer self.alloc.free(segment_path);
        try atomicReplace(self.alloc, self.storage, segment_path, segment_bytes);
    }

    /// Publishes a segment materialized from the committed WAL prefix ending
    /// at `flattened_wal_bytes`, and carries every subsequently committed
    /// batch into the next WAL generation. The byte boundary is intentional:
    /// maintenance may commit more than one ordered batch at the same source
    /// sequence, so filtering the tail by sequence could silently lose work.
    pub fn publishCheckpointPreservingWalTail(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        segment_bytes: []const u8,
        flattened_wal_bytes: u64,
    ) !void {
        return try self.publishCheckpointInternal(
            segment_generation,
            covered_source_sequence,
            segment_bytes,
            flattened_wal_bytes,
            false,
        );
    }

    /// Publishes a segment already made durable by `stageCheckpointSegment`
    /// while carrying forward the committed WAL suffix.
    pub fn publishStagedCheckpointPreservingWalTail(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        segment_bytes: []const u8,
        flattened_wal_bytes: u64,
    ) !void {
        return try self.publishCheckpointInternal(
            segment_generation,
            covered_source_sequence,
            segment_bytes,
            flattened_wal_bytes,
            true,
        );
    }

    fn publishCheckpointInternal(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        segment_bytes: []const u8,
        flattened_wal_bytes: ?u64,
        segment_already_staged: bool,
    ) !void {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        if (self.checkpoint) |current| {
            if (segment_generation <= current.segment_generation) return error.OutOfOrderPostingSegmentGeneration;
        }
        _ = try posting_segment.Reader.init(segment_bytes);

        var retained_wal: ?RecoveredWal = null;
        defer if (retained_wal) |*wal| wal.deinit();
        var tail: []const u8 = &.{};
        var tail_last_committed_batch: ?u64 = null;
        var tail_covered_source_sequence = covered_source_sequence;
        if (flattened_wal_bytes) |prefix_bytes_u64| {
            const prefix_bytes = std.math.cast(usize, prefix_bytes_u64) orelse return error.InvalidPostingWalBoundary;
            retained_wal = try self.recoverWal();
            const wal = &retained_wal.?;
            if (@as(u64, @intCast(wal.replay.committed_bytes)) != self.wal_committed_bytes or
                prefix_bytes > wal.replay.committed_bytes)
            {
                return error.InvalidPostingWalBoundary;
            }
            var prefix_replay = try posting_wal.Replay.parse(self.alloc, wal.bytes[0..prefix_bytes]);
            defer prefix_replay.deinit();
            if (prefix_replay.committed_bytes != prefix_bytes or
                prefix_replay.covered_source_sequence != covered_source_sequence)
            {
                return error.InvalidPostingWalBoundary;
            }
            tail = wal.bytes[prefix_bytes..wal.replay.committed_bytes];
            if (tail.len > 0) {
                var tail_replay = try posting_wal.Replay.parse(self.alloc, tail);
                defer tail_replay.deinit();
                if (tail_replay.committed_bytes != tail.len) return error.InvalidPostingWalBoundary;
                for (tail_replay.records.items) |record| {
                    if (record.source_sequence < covered_source_sequence) return error.PostingWalOverlapsCheckpoint;
                }
                tail_last_committed_batch = tail_replay.last_committed_batch;
                tail_covered_source_sequence = @max(covered_source_sequence, tail_replay.covered_source_sequence);
            }
            if (tail.len == 0) {
                if (covered_source_sequence != self.covered_source_sequence) return error.InvalidPostingWalBoundary;
            } else if (tail_covered_source_sequence != self.covered_source_sequence or
                tail_last_committed_batch != self.last_committed_batch)
            {
                return error.InvalidPostingWalBoundary;
            }
        } else if (covered_source_sequence < self.covered_source_sequence) {
            return error.OutOfOrderPostingCheckpointSequence;
        }

        const next_wal_generation = std.math.add(u64, self.wal_generation, 1) catch
            return error.PostingWalGenerationOverflow;
        const segment_path = try self.segmentPathAlloc(segment_generation);
        defer self.alloc.free(segment_path);
        if (segment_already_staged) {
            if (try self.storage.fileSize(segment_path) != segment_bytes.len) return error.MissingPostingSegment;
        } else {
            try atomicReplace(self.alloc, self.storage, segment_path, segment_bytes);
        }

        const next_wal_path = try self.walPathAlloc(next_wal_generation);
        defer self.alloc.free(next_wal_path);
        try atomicReplace(self.alloc, self.storage, next_wal_path, tail);

        const next_checkpoint: posting_wal.Checkpoint = .{
            .segment_generation = segment_generation,
            .segment_checksum = std.hash.Crc32.hash(segment_bytes),
            .segment_admission_checksum = try posting_segment.admissionChecksum(segment_bytes),
            .wal_generation = next_wal_generation,
            .wal_committed_bytes = @intCast(tail.len),
            .covered_source_sequence = covered_source_sequence,
        };
        const encoded = next_checkpoint.encode();
        const current_path = try self.currentPathAlloc();
        defer self.alloc.free(current_path);
        try atomicReplace(self.alloc, self.storage, current_path, &encoded);

        const previous = self.checkpoint;
        const previous_wal_generation = self.wal_generation;
        self.checkpoint = next_checkpoint;
        self.wal_generation = next_wal_generation;
        self.wal_committed_bytes = @intCast(tail.len);
        self.last_committed_batch = tail_last_committed_batch;
        self.covered_source_sequence = tail_covered_source_sequence;
        self.segment_bytes = @intCast(segment_bytes.len);

        if (previous) |old| {
            const old_segment_path = try self.segmentPathAlloc(old.segment_generation);
            defer self.alloc.free(old_segment_path);
            deleteFileBestEffort(self.storage, old_segment_path);
        }
        const old_wal_path = try self.walPathAlloc(previous_wal_generation);
        defer self.alloc.free(old_wal_path);
        deleteFileBestEffort(self.storage, old_wal_path);
    }

    pub fn recoverWal(self: *Store) !RecoveredWal {
        const wal_path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(wal_path);
        const bytes = self.storage.readFileAlloc(self.alloc, wal_path, max_wal_bytes + 1) catch |err| switch (err) {
            error.FileNotFound => if (self.wal_committed_bytes == 0)
                try self.alloc.alloc(u8, 0)
            else
                return error.MissingPostingWal,
            else => return err,
        };
        errdefer self.alloc.free(bytes);
        return .{
            .alloc = self.alloc,
            .bytes = bytes,
            .replay = try posting_wal.Replay.parse(self.alloc, bytes),
        };
    }

    pub fn readSegmentAlloc(self: *Store) ![]u8 {
        const checkpoint = self.checkpoint orelse return error.MissingPostingCheckpoint;
        const path = try self.segmentPathAlloc(checkpoint.segment_generation);
        defer self.alloc.free(path);
        const bytes = try self.storage.readFileAlloc(self.alloc, path, boundedReadLimit(max_segment_bytes));
        errdefer self.alloc.free(bytes);
        if (std.hash.Crc32.hash(bytes) != checkpoint.segment_checksum) return error.PostingSegmentChecksumMismatch;
        _ = try posting_segment.Reader.init(bytes);
        return bytes;
    }

    fn readSegmentAllocFor(self: *Store, checkpoint: posting_wal.Checkpoint) ![]u8 {
        const path = try self.segmentPathAlloc(checkpoint.segment_generation);
        defer self.alloc.free(path);
        const bytes = self.storage.readFileAlloc(self.alloc, path, boundedReadLimit(max_segment_bytes)) catch |err| switch (err) {
            error.FileNotFound => return error.MissingPostingSegment,
            else => return err,
        };
        errdefer self.alloc.free(bytes);
        if (std.hash.Crc32.hash(bytes) != checkpoint.segment_checksum) return error.PostingSegmentChecksumMismatch;
        _ = try posting_segment.Reader.init(bytes);
        return bytes;
    }

    fn readSegmentRetainedFor(self: *Store, checkpoint: posting_wal.Checkpoint) !RetainedSegment {
        const path = try self.segmentPathAlloc(checkpoint.segment_generation);
        defer self.alloc.free(path);
        if (mapSegmentFile(path)) |mapped| {
            const published_checksum_matches = mapped.len <= max_segment_bytes and (if (checkpoint.segment_admission_checksum != 0)
                (posting_segment.admissionChecksum(mapped) catch 0) == checkpoint.segment_admission_checksum
            else
                std.hash.Crc32.hash(mapped) == checkpoint.segment_checksum);
            if (published_checksum_matches) {
                if (posting_segment.Reader.init(mapped)) |_| {
                    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.RANDOM) catch {};
                    return .{ .mapped = mapped };
                } else |_| {}
            }
            std.posix.munmap(mapped);
        } else |_| {}
        return .{ .heap = try self.readSegmentAllocFor(checkpoint) };
    }

    fn replaceWal(self: *Store, contents: []const u8) !void {
        const path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(path);
        try atomicReplace(self.alloc, self.storage, path, contents);
        self.wal_committed_bytes = @intCast(contents.len);
    }

    /// Removes the publication pointer before an uncovered authoritative
    /// mutation can commit. Segment and WAL generations are intentionally left
    /// behind as harmless orphans; a later checkpoint replaces them.
    pub fn invalidate(self: *Store) !void {
        const current_path = try self.currentPathAlloc();
        defer self.alloc.free(current_path);
        self.storage.deleteFileAbsolute(current_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        try self.storage.syncParentAbsolute(current_path);
        self.checkpoint = null;
        self.covered_source_sequence = 0;
        self.segment_bytes = 0;
    }

    fn currentPathAlloc(self: *const Store) ![]u8 {
        return try std.fs.path.join(self.alloc, &.{ self.root_dir, current_name });
    }

    fn segmentPathAlloc(self: *const Store, generation: u64) ![]u8 {
        const name = try std.fmt.allocPrint(self.alloc, "segment-{d}.afps", .{generation});
        defer self.alloc.free(name);
        return try std.fs.path.join(self.alloc, &.{ self.root_dir, name });
    }

    fn walPathAlloc(self: *const Store, generation: u64) ![]u8 {
        const name = try std.fmt.allocPrint(self.alloc, "wal-{d}.afpw", .{generation});
        defer self.alloc.free(name);
        return try std.fs.path.join(self.alloc, &.{ self.root_dir, name });
    }
};

pub const OpenedWithSegment = struct {
    store: Store,
    segment: RetainedSegment,

    pub fn deinit(self: *OpenedWithSegment) void {
        const alloc = self.store.alloc;
        self.store.deinit();
        self.segment.deinit(alloc);
        self.* = undefined;
    }
};

fn mapSegmentFile(path: []const u8) ![]align(std.heap.page_size_min) u8 {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) {
        return error.UnsupportedPlatform;
    }
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDONLY,
        .CLOEXEC = true,
    }, 0);
    defer _ = std.posix.system.close(fd);
    const size_raw = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
    if (size_raw <= 0) return error.EmptyPostingSegment;
    const size = std.math.cast(usize, size_raw) orelse return error.PostingSegmentTooLarge;
    return try std.posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
}

fn boundedReadLimit(max_bytes: usize) usize {
    return std.math.add(usize, max_bytes, 1) catch std.math.maxInt(usize);
}

fn atomicReplace(alloc: Allocator, storage: lsm_backend.Storage, path: []const u8, contents: []const u8) !void {
    var sink = try storage.beginAtomicWrite(alloc, path);
    var active = true;
    defer if (active) sink.abort();
    try sink.appendSlice(contents);
    active = false;
    try sink.finish();
}

fn deleteFileBestEffort(storage: lsm_backend.Storage, path: []const u8) void {
    storage.deleteFileAbsolute(path) catch {};
}

test "storage.posting segment store publishes checkpoint and committed WAL generations" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-store");
    defer store.deinit();
    try std.testing.expectError(error.MissingPostingCheckpoint, store.appendBatch(1, &.{.{
        .kind = .base,
        .posting_id = 7,
        .source_sequence = 10,
        .payload = "base-v1",
    }}, 10));

    var segment_writer = posting_segment.Writer.init(alloc);
    defer segment_writer.deinit();
    try segment_writer.appendBaseAt(7, 10, "base-v1");
    try segment_writer.appendQuantizedCheckpointAt(7, 10, "quant-v1");
    const segment = try segment_writer.build();
    defer alloc.free(segment);
    try store.publishCheckpoint(1, 10, segment);
    try std.testing.expectError(error.FileNotFound, memory.storage().fileSize("/posting-store/wal-1.afpw"));

    try store.appendBatch(1, &.{.{
        .kind = .quantized_checkpoint,
        .posting_id = 7,
        .source_sequence = 11,
        .payload = "quant-v2",
    }}, 11);
    store.deinit();

    store = try Store.open(alloc, memory.storage(), "/posting-store");
    const loaded_segment = try store.readSegmentAlloc();
    defer alloc.free(loaded_segment);
    const reader = try posting_segment.Reader.init(loaded_segment);
    try std.testing.expectEqualStrings("base-v1", (try reader.getBase(7)).?);
    var replay = try store.recoverWal();
    defer replay.deinit();
    try std.testing.expectEqualStrings("quant-v2", replay.replay.latest(7, .quantized_checkpoint).?.payload);
}

test "storage.posting segment store truncates incomplete WAL tail before append" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-tail");
    var segment_writer = posting_segment.Writer.init(alloc);
    defer segment_writer.deinit();
    try segment_writer.appendBaseAt(3, 0, "zero");
    const segment = try segment_writer.build();
    defer alloc.free(segment);
    try store.publishCheckpoint(1, 0, segment);
    try store.appendBatch(1, &.{.{
        .kind = .base,
        .posting_id = 3,
        .source_sequence = 1,
        .payload = "one",
    }}, 1);
    const committed_bytes = store.wal_committed_bytes;
    try memory.storage().appendFileAbsolute(alloc, "/posting-tail/wal-2.afpw", "partial", false);
    store.deinit();

    store = try Store.open(alloc, memory.storage(), "/posting-tail");
    defer store.deinit();
    try std.testing.expectEqual(committed_bytes, try memory.storage().fileSize("/posting-tail/wal-2.afpw"));
    try store.appendBatch(2, &.{.{
        .kind = .base,
        .posting_id = 3,
        .source_sequence = 2,
        .payload = "two",
    }}, 2);
    var replay = try store.recoverWal();
    defer replay.deinit();
    try std.testing.expectEqualStrings("two", replay.replay.latest(3, .base).?.payload);
}

test "storage.posting segment store orders maintenance batches independently from source coverage" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-maintenance");
    defer store.deinit();
    var segment_writer = posting_segment.Writer.init(alloc);
    defer segment_writer.deinit();
    try segment_writer.appendBaseAt(3, 10, "initial");
    const segment = try segment_writer.build();
    defer alloc.free(segment);
    try store.publishCheckpoint(1, 10, segment);

    try store.appendBatch(try store.nextBatchId(), &.{.{
        .kind = .base,
        .posting_id = 3,
        .source_sequence = 10,
        .payload = "maintenance-one",
    }}, 10);
    try store.appendBatch(try store.nextBatchId(), &.{.{
        .kind = .base,
        .posting_id = 3,
        .source_sequence = 10,
        .payload = "maintenance-two",
    }}, 10);
    try std.testing.expectEqual(@as(u64, 10), store.covered_source_sequence);
    try std.testing.expectEqual(@as(?u64, 2), store.last_committed_batch);
    store.deinit();

    store = try Store.open(alloc, memory.storage(), "/posting-maintenance");
    try std.testing.expectEqual(@as(u64, 10), store.covered_source_sequence);
    try std.testing.expectEqual(@as(?u64, 2), store.last_committed_batch);
    var replay = try store.recoverWal();
    defer replay.deinit();
    try std.testing.expectEqualStrings("maintenance-two", replay.replay.latest(3, .base).?.payload);

    try store.appendCoverage(try store.nextBatchId(), 11, .{ .sync = false });
    try std.testing.expectEqual(@as(u64, 11), store.covered_source_sequence);
    try std.testing.expectEqual(@as(?u64, 3), store.last_committed_batch);
}

test "storage.posting segment publication preserves the exact committed WAL tail" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-preserved-tail");
    defer store.deinit();
    var initial_writer = posting_segment.Writer.init(alloc);
    defer initial_writer.deinit();
    try initial_writer.appendBaseAt(3, 10, "initial");
    const initial_segment = try initial_writer.build();
    defer alloc.free(initial_segment);
    try store.publishCheckpoint(1, 10, initial_segment);

    try store.appendBatch(try store.nextBatchId(), &.{.{
        .kind = .base,
        .posting_id = 3,
        .source_sequence = 11,
        .payload = "flattened",
    }}, 11);
    const flattened_wal_bytes = store.wal_committed_bytes;

    // This maintenance batch deliberately has the same source sequence as
    // the segment boundary. Sequence-based filtering would lose it.
    try store.appendBatch(try store.nextBatchId(), &.{.{
        .kind = .base,
        .posting_id = 3,
        .source_sequence = 11,
        .payload = "same-sequence-tail",
    }}, 11);
    try store.appendCoverage(try store.nextBatchId(), 12, .{ .sync = false });

    var next_writer = posting_segment.Writer.init(alloc);
    defer next_writer.deinit();
    try next_writer.appendBaseAt(3, 11, "flattened");
    const next_segment = try next_writer.build();
    defer alloc.free(next_segment);
    try store.publishCheckpointPreservingWalTail(2, 11, next_segment, flattened_wal_bytes);
    try std.testing.expect(store.wal_committed_bytes > 0);
    try std.testing.expectEqual(@as(u64, 12), store.covered_source_sequence);
    try std.testing.expectEqual(@as(?u64, 3), store.last_committed_batch);

    store.deinit();
    store = try Store.open(alloc, memory.storage(), "/posting-preserved-tail");
    try std.testing.expectEqual(@as(u64, 11), store.checkpoint.?.covered_source_sequence);
    try std.testing.expectEqual(@as(u64, 12), store.covered_source_sequence);
    var replay = try store.recoverWal();
    defer replay.deinit();
    try std.testing.expectEqualStrings("same-sequence-tail", replay.replay.latest(3, .base).?.payload);
}

test "storage.posting segment publication rejects a non-commit WAL boundary" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-bad-boundary");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBaseAt(1, 1, "one");
    const segment = try writer.build();
    defer alloc.free(segment);
    try store.publishCheckpoint(1, 1, segment);
    try store.appendCoverage(1, 2, .{ .sync = false });
    try std.testing.expectError(
        error.InvalidPostingWalBoundary,
        store.publishCheckpointPreservingWalTail(2, 2, segment, store.wal_committed_bytes - 1),
    );
}

test "storage.posting segment store bounds WAL tails relative to the segment" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-policy");
    defer store.deinit();
    var segment_writer = posting_segment.Writer.init(alloc);
    defer segment_writer.deinit();
    const base: [256]u8 = @splat('x');
    try segment_writer.appendBaseAt(1, 1, &base);
    const segment = try segment_writer.build();
    defer alloc.free(segment);
    try store.publishCheckpoint(1, 1, segment);
    try std.testing.expect(!store.shouldCheckpoint(.{ .min_wal_bytes = 1, .max_wal_bytes = 1024, .wal_to_segment_percent = 50 }));

    try store.appendCoverage(2, 2, .{ .sync = false });
    try std.testing.expect(store.shouldCheckpoint(.{ .min_wal_bytes = 1, .max_wal_bytes = 1024, .wal_to_segment_percent = 1 }));
}

test "storage.posting segment invalidation removes only the publication pointer" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-invalidate");
    var segment_writer = posting_segment.Writer.init(alloc);
    defer segment_writer.deinit();
    try segment_writer.appendBaseAt(1, 1, "base");
    const segment = try segment_writer.build();
    defer alloc.free(segment);
    try store.publishCheckpoint(1, 1, segment);
    try store.invalidate();
    store.deinit();

    try std.testing.expectError(error.FileNotFound, memory.storage().fileSize("/posting-invalidate/CURRENT"));
    try std.testing.expect((try memory.storage().fileSize("/posting-invalidate/segment-1.afps")) > 0);
    store = try Store.open(alloc, memory.storage(), "/posting-invalidate");
    defer store.deinit();
    try std.testing.expect(store.checkpoint == null);
}
