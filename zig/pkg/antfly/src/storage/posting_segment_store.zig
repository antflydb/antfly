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
const generation_publication = @import("generation_publication.zig");
const vectorindex = @import("antfly_vectorindex");
const posting_segment = vectorindex.posting_segment;
const posting_wal = vectorindex.posting_wal;

const current_name = "CURRENT";
pub const authority_name = "AUTHORITY";
pub const authority_value = "antfly-hbc-native-v1\n";
const max_control_bytes = posting_wal.Checkpoint.encoded_len;
const max_wal_bytes: usize = 512 * 1024 * 1024;
const max_segment_bytes: usize = if (@sizeOf(usize) >= 8) 4 * 1024 * 1024 * 1024 else std.math.maxInt(usize);

pub fn checkpointCurrentPathAlloc(alloc: Allocator, root_dir: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, current_name });
}

pub fn checkpointSegmentPathAlloc(alloc: Allocator, root_dir: []const u8, generation: u64) ![]u8 {
    const name = try std.fmt.allocPrint(alloc, "segment-{d}.afps", .{generation});
    defer alloc.free(name);
    return try std.fs.path.join(alloc, &.{ root_dir, name });
}

pub fn checkpointWalPathAlloc(alloc: Allocator, root_dir: []const u8, generation: u64) ![]u8 {
    const name = try std.fmt.allocPrint(alloc, "wal-{d}.afpw", .{generation});
    defer alloc.free(name);
    return try std.fs.path.join(alloc, &.{ root_dir, name });
}

pub const RetainedSegment = union(enum) {
    heap: []u8,
    mapped: []align(std.heap.page_size_min) u8,
    shared: *Shared,

    const Shared = struct {
        alloc: Allocator,
        refs: std.atomic.Value(usize) = .init(1),
        namespace: []u8,
        descriptor: posting_wal.Checkpoint.Segment,
        payload: RetainedSegment,
        retirement_storage: ?lsm_backend.Storage.Lease = null,
        retired: std.atomic.Value(bool) = .init(false),
    };

    /// Ownership of payload transfers only on success. Namespace and complete
    /// immutable descriptor identity are required before a mapping is reused.
    fn share(alloc: Allocator, payload: RetainedSegment, namespace: []const u8, descriptor: posting_wal.Checkpoint.Segment) !RetainedSegment {
        const shared = try alloc.create(Shared);
        errdefer alloc.destroy(shared);
        shared.* = .{ .alloc = alloc, .payload = payload, .namespace = try alloc.dupe(u8, namespace), .descriptor = descriptor };
        return .{ .shared = shared };
    }

    pub fn matchesIdentity(self: RetainedSegment, namespace: []const u8, descriptor: posting_wal.Checkpoint.Segment) bool {
        return self == .shared and std.mem.eql(u8, self.shared.namespace, namespace) and
            std.meta.eql(self.shared.descriptor, descriptor);
    }

    fn retainMatching(self: RetainedSegment, namespace: []const u8, descriptor: posting_wal.Checkpoint.Segment) ?RetainedSegment {
        if (!self.matchesIdentity(namespace, descriptor)) return null;
        const previous = self.shared.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0 and previous < std.math.maxInt(usize));
        return self;
    }

    /// Called by the serialized publication owner only after CURRENT is
    /// durable and no longer references this immutable identity. The caller
    /// holds a reference throughout; release of the last query lease performs
    /// deletion after unmapping, including on platforms that forbid unlinking
    /// a mapped file. Allocation/I/O failure leaves recoverable startup debt.
    fn retireMatching(self: RetainedSegment, storage: lsm_backend.Storage, namespace: []const u8, descriptor: posting_wal.Checkpoint.Segment) bool {
        if (self != .shared or !std.mem.eql(u8, self.shared.namespace, namespace) or
            !std.meta.eql(self.shared.descriptor, descriptor)) return false;
        if (!self.shared.retired.load(.acquire)) {
            // Failure leaves an orphan for owner-only startup reclamation;
            // it must not fall through to immediate deletion of a leased file.
            self.shared.retirement_storage = storage.acquireLease() catch null;
            self.shared.retired.store(true, .release);
        }
        return true;
    }

    pub fn bytes(self: RetainedSegment) []const u8 {
        return switch (self) {
            .heap => |data| data,
            .mapped => |data| data,
            .shared => |value| value.payload.bytes(),
        };
    }

    pub fn isMapped(self: RetainedSegment) bool {
        return if (self == .shared) self.shared.payload.isMapped() else self == .mapped;
    }

    pub fn mappedBytes(self: RetainedSegment) ?[]align(std.heap.page_size_min) u8 {
        return switch (self) {
            .mapped => |data| data,
            .heap => null,
            .shared => |value| value.payload.mappedBytes(),
        };
    }

    pub fn deinit(self: *RetainedSegment, alloc: Allocator) void {
        switch (self.*) {
            .shared => |value| {
                if (value.refs.fetchSub(1, .acq_rel) == 1) {
                    const owner = value.alloc;
                    value.payload.deinit(owner);
                    if (value.retired.load(.acquire)) if (value.retirement_storage) |*lease| {
                        defer lease.deinit();
                        if (checkpointSegmentPathAlloc(owner, value.namespace, value.descriptor.generation)) |path| {
                            defer owner.free(path);
                            lease.view.deleteFileAbsolute(path) catch {};
                        } else |_| {}
                    };
                    owner.free(value.namespace);
                    owner.destroy(value);
                }
            },
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

fn testSharedSegmentOwnership(alloc: Allocator) !void {
    var payload = RetainedSegment{ .heap = try alloc.dupe(u8, "immutable segment") };
    var payload_owned = true;
    defer if (payload_owned) payload.deinit(alloc);
    const descriptor = posting_wal.Checkpoint.Segment{ .generation = 3, .checksum = 17, .admission_checksum = 23 };
    var original = try RetainedSegment.share(alloc, payload, "/index-a", descriptor);
    payload_owned = false;
    var original_owned = true;
    defer if (original_owned) original.deinit(alloc);
    try std.testing.expect(original.retainMatching("/index-b", descriptor) == null);
    var changed = descriptor;
    changed.admission_checksum += 1;
    try std.testing.expect(original.retainMatching("/index-a", changed) == null);
    var retained = original.retainMatching("/index-a", descriptor) orelse return error.TestUnexpectedResult;
    defer retained.deinit(alloc);
    try std.testing.expectEqual(original.bytes().ptr, retained.bytes().ptr);
    original.deinit(alloc);
    original_owned = false;
    try std.testing.expectEqualStrings("immutable segment", retained.bytes());
    try std.testing.expect(!retained.isMapped());
    try std.testing.expect(retained.mappedBytes() == null);
}

test "storage.posting shared immutable segments retain namespace-bound payload ownership" {
    try testSharedSegmentOwnership(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testSharedSegmentOwnership, .{});
}

test "storage.posting retired segment owns deletion storage past provider shutdown" {
    if (builtin.os.tag != .macos and builtin.os.tag != .linux) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer alloc.free(root);
    const path = try checkpointSegmentPathAlloc(alloc, root, 1);
    defer alloc.free(path);
    var native = try lsm_backend.NativeStorage.init(alloc, .threaded);
    var native_owned = true;
    defer if (native_owned) native.deinit();
    try native.storage().writeFileAbsolute(path, "leased payload");
    var payload = RetainedSegment{ .heap = try alloc.dupe(u8, "leased payload") };
    var payload_owned = true;
    defer if (payload_owned) payload.deinit(alloc);
    const descriptor = posting_wal.Checkpoint.Segment{ .generation = 1 };
    var segment = try RetainedSegment.share(alloc, payload, root, descriptor);
    payload_owned = false;
    var segment_owned = true;
    defer if (segment_owned) segment.deinit(alloc);
    try std.testing.expect(segment.retireMatching(native.storage(), root, descriptor));
    _ = try native.storage().fileSize(path);
    native.deinit();
    native_owned = false;
    try std.testing.expectEqualStrings("leased payload", segment.bytes());
    segment.deinit(alloc);
    segment_owned = false;
    var verifier = try lsm_backend.NativeStorage.init(alloc, .threaded);
    defer verifier.deinit();
    try std.testing.expectError(error.FileNotFound, verifier.storage().fileSize(path));
}

pub const AppendOptions = struct {
    sync: bool = true,
};

pub const ReclaimStats = struct {
    observed_debt: usize = 0,
    removed: usize = 0,
    remaining_debt: usize = 0,
};

pub const CheckpointPolicy = struct {
    min_wal_bytes: u64 = 1 * 1024 * 1024,
    max_wal_bytes: u64 = 64 * 1024 * 1024,
    wal_to_segment_percent: u8 = 50,
};

/// Proof produced only after an immutable checkpoint segment has been parsed,
/// checksummed, and durably staged. Publication may trust these content
/// fingerprints because the generation file is immutable and private to the
/// store; it still verifies generation and file size before replacing CURRENT.
pub const StagedCheckpointSegment = struct {
    generation: u64,
    bytes: u64,
    checksum: u32,
    admission_checksum: u32,
};

/// Crash-safe file-backed checkpoint construction. The temporary sibling is
/// invisible until `finish`, and the finished generation remains an orphan
/// until CURRENT is published. `deinit` aborts any unfinished write.
pub const StagedCheckpointWriter = struct {
    alloc: Allocator,
    storage: lsm_backend.Storage,
    generation: u64,
    path: []u8,
    output_sink: ?lsm_backend.storage_io.AtomicWriteSink,

    pub fn output(self: *StagedCheckpointWriter) *lsm_backend.storage_io.AtomicWriteSink {
        return &self.output_sink.?;
    }

    pub fn finish(self: *StagedCheckpointWriter, admission_checksum: u32) !StagedCheckpointSegment {
        var sink = self.output_sink orelse return error.CheckpointWriterFinished;
        const bytes: u64 = @intCast(sink.len());
        const checksum = if (admission_checksum == 0)
            try sink.crc32Prefix(sink.len())
        else
            0;
        sink.finish() catch |err| {
            self.output_sink = null;
            return err;
        };
        self.output_sink = null;
        if (try self.storage.fileSize(self.path) != bytes) return error.MissingPostingSegment;
        return .{
            .generation = self.generation,
            .bytes = bytes,
            .checksum = checksum,
            .admission_checksum = admission_checksum,
        };
    }

    pub fn deinit(self: *StagedCheckpointWriter) void {
        if (self.output_sink) |*sink| sink.abort();
        self.alloc.free(self.path);
        self.* = undefined;
    }
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

fn replayHasStateRecords(replay: *const posting_wal.Replay) bool {
    for (replay.records.items) |record| {
        if (record.kind != .coverage) return true;
    }
    return false;
}

pub const Store = struct {
    alloc: Allocator,
    storage: lsm_backend.Storage,
    root_dir: []u8,
    checkpoint: ?posting_wal.Checkpoint,
    wal_generation: u64,
    wal_committed_bytes: u64,
    wal_has_state_records: bool,
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
        var retained_segments: ?[]RetainedSegment = null;
        var store = try openInternal(alloc, storage, root_dir, &retained_segments);
        errdefer store.deinit();
        return .{
            .store = store,
            .segments = retained_segments orelse return error.MissingPostingCheckpoint,
        };
    }

    fn openInternal(
        alloc: Allocator,
        storage: lsm_backend.Storage,
        root_dir: []const u8,
        retained_segments: ?*?[]RetainedSegment,
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
            .wal_has_state_records = false,
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

        const retained = try alloc.alloc(RetainedSegment, checkpoint.segmentCount());
        var retained_count: usize = 0;
        errdefer {
            for (retained[0..retained_count]) |*segment| segment.deinit(alloc);
            alloc.free(retained);
        }
        for (0..checkpoint.segmentCount()) |index| {
            retained[index] = try store.readSegmentRetainedFor(checkpoint.segment(index));
            retained_count += 1;
            store.segment_bytes = std.math.add(u64, store.segment_bytes, retained[index].bytes().len) catch
                return error.PostingSegmentTooLarge;
        }
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
        store.wal_has_state_records = replayHasStateRecords(&recovered.replay);
        store.last_committed_batch = recovered.replay.last_committed_batch;
        store.covered_source_sequence = @max(
            checkpoint.covered_source_sequence,
            recovered.replay.covered_source_sequence,
        );
        if (retained_segments) |out| {
            out.* = retained;
            retained_count = 0;
        } else {
            for (retained) |*segment| segment.deinit(alloc);
            alloc.free(retained);
            retained_count = 0;
        }
        return store;
    }

    pub fn deinit(self: *Store) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn latestSegmentGeneration(self: *const Store) ?u64 {
        return if (self.checkpoint) |checkpoint| checkpoint.latestSegmentGeneration() else null;
    }

    /// Opens the complete base through a maintenance-private descriptor. It
    /// deliberately does not reuse the foreground mmap/FD cache: long-running
    /// flattening can then stream cold payloads without making query residency
    /// proportional to the corpus. The returned reader pins its own storage
    /// descriptor and remains valid across CURRENT publication.
    pub fn beginBaseColdSequentialRead(self: *const Store, allocator: Allocator) !lsm_backend.storage_io.ColdSequentialReader {
        const checkpoint = self.checkpoint orelse return error.MissingPostingCheckpoint;
        const path = try self.segmentPathAlloc(checkpoint.segment_generation);
        defer self.alloc.free(path);
        return try self.storage.beginColdSequentialRead(allocator, path);
    }

    pub fn deltaSegmentCount(self: *const Store) usize {
        return if (self.checkpoint) |checkpoint| checkpoint.delta_segment_count else 0;
    }

    /// Rotate only at a committed batch boundary. CURRENT first makes the
    /// immutable extent recoverable together with the new append target;
    /// checkpoint preparation can subsequently drop covered extents by name.
    /// The caller owns the writer lane. An ambiguous CURRENT write poisons it.
    pub fn sealWalForCheckpoint(self: *Store) !bool {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        var checkpoint = self.checkpoint orelse return error.MissingPostingCheckpoint;
        const sealed_bytes = checkpoint.sealedWalBytes();
        if (self.wal_committed_bytes == sealed_bytes) return false;
        if (checkpoint.sealed_wal_count == posting_wal.Checkpoint.max_sealed_wals) return false;
        const next_generation = std.math.add(u64, self.wal_generation, 1) catch return error.PostingWalGenerationOverflow;
        const old_path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(old_path);
        const next_path = try self.walPathAlloc(next_generation);
        defer self.alloc.free(next_path);
        const current_path = try self.currentPathAlloc();
        defer self.alloc.free(current_path);
        try self.storage.syncFileContentsAbsolute(old_path);
        try atomicReplace(self.alloc, self.storage, next_path, &.{});
        checkpoint.sealed_wals[checkpoint.sealed_wal_count] = .{
            .generation = self.wal_generation,
            .committed_bytes = self.wal_committed_bytes - sealed_bytes,
            .covered_source_sequence = self.covered_source_sequence,
            .last_batch = self.last_committed_batch orelse return error.InvalidPostingWalBoundary,
        };
        checkpoint.sealed_wal_count += 1;
        checkpoint.wal_generation = next_generation;
        checkpoint.wal_committed_bytes = self.wal_committed_bytes;
        const encoded = checkpoint.encode();
        generation_publication.publishControlFile(self.alloc, self.storage, current_path, &encoded) catch |err| {
            self.poisoned = true;
            return err;
        };
        self.checkpoint = checkpoint;
        self.wal_generation = next_generation;
        return true;
    }

    pub fn markAuthoritative(self: *Store) !void {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, authority_name });
        defer self.alloc.free(path);
        generation_publication.publishControlFile(self.alloc, self.storage, path, authority_value) catch |err| {
            self.poisoned = true;
            return err;
        };
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
        const trace = @import("dense_perf_experiments.zig").enabled("ANTFLY_EXPERIMENT_CAPTURE_STAGES");
        const started = if (trace) @import("antfly_platform").time.monotonicNs() else 0;
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
        const encoded_at = if (trace) @import("antfly_platform").time.monotonicNs() else 0;
        self.storage.appendFileAbsolute(self.alloc, wal_path, writer.bytes(), options.sync) catch |err| {
            // The storage error may be ambiguous (for example fsync failed
            // after the append reached the page cache). Refuse retries on this
            // handle; reopen reparses the durable prefix without risking a
            // duplicate committed batch.
            self.poisoned = true;
            return err;
        };
        self.wal_committed_bytes = next_committed_bytes;
        if (!self.wal_has_state_records) {
            for (records) |record| {
                if (record.kind != .coverage) {
                    self.wal_has_state_records = true;
                    break;
                }
            }
        }
        self.last_committed_batch = batch_id;
        self.covered_source_sequence = covered_source_sequence;
        if (trace) std.log.info("dense WAL stages batch={} sequence={} records={} bytes={} encode_ns={} append_sync_ns={} sync={}", .{
            batch_id,              covered_source_sequence,                                     records.len,  writer.bytes().len,
            encoded_at -| started, @import("antfly_platform").time.monotonicNs() -| encoded_at, options.sync,
        });
    }

    pub fn appendCoverage(self: *Store, batch_id: u64, covered_source_sequence: u64, options: AppendOptions) !void {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        if (self.checkpoint == null) return error.MissingPostingCheckpoint;
        if (covered_source_sequence < self.covered_source_sequence) return error.PostingWalOverlapsCheckpoint;
        // Coverage is a watermark, not a transaction identity. Re-emitting the
        // current watermark after a long-running checkpoint only creates a
        // non-empty WAL tail and can provoke an otherwise identical full
        // rewrite at the next readiness fence.
        if (covered_source_sequence == self.covered_source_sequence) {
            // An equal watermark may have come from an earlier unsynced append.
            // Preserve the caller-visible durability meaning of `sync=true`
            // without manufacturing another logical WAL transaction.
            if (options.sync and self.wal_committed_bytes != 0) {
                const wal_path = try self.walPathAlloc(self.wal_generation);
                defer self.alloc.free(wal_path);
                self.storage.syncFileContentsAbsolute(wal_path) catch |err| {
                    // As with append+fsync, the durability outcome can be
                    // ambiguous. Reopen before permitting another write.
                    self.poisoned = true;
                    return err;
                };
            }
            return;
        }
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
            null,
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
    ) !StagedCheckpointSegment {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        if (self.checkpoint) |current| {
            if (segment_generation <= current.latestSegmentGeneration()) return error.OutOfOrderPostingSegmentGeneration;
        }
        const reader = try posting_segment.Reader.init(segment_bytes);
        const admission_checksum = reader.admissionChecksum();
        const receipt: StagedCheckpointSegment = .{
            .generation = segment_generation,
            .bytes = @intCast(segment_bytes.len),
            // New generations use the eagerly verified index/footer checksum
            // plus each entry's lazy payload checksum. A second full-file CRC
            // would fault every payload page and duplicate that integrity
            // coverage on every checkpoint.
            // Zero means "not present" in older checkpoint formats. Preserve
            // an unambiguous fallback if the index CRC happens to be zero.
            .checksum = if (admission_checksum == 0) std.hash.Crc32.hash(segment_bytes) else 0,
            .admission_checksum = admission_checksum,
        };
        const segment_path = try self.segmentPathAlloc(segment_generation);
        defer self.alloc.free(segment_path);
        try generation_publication.replaceColdImmutable(self.alloc, self.storage, segment_path, segment_bytes);
        return receipt;
    }

    /// Begins a bounded-memory immutable segment build. Production native
    /// storage writes directly to a temporary file; compatibility backends may
    /// use their buffered atomic sink without changing publication semantics.
    pub fn beginStagedCheckpointSegment(
        self: *Store,
        segment_generation: u64,
    ) !StagedCheckpointWriter {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        if (self.checkpoint) |current| {
            if (segment_generation <= current.latestSegmentGeneration()) return error.OutOfOrderPostingSegmentGeneration;
        }
        const path = try self.segmentPathAlloc(segment_generation);
        errdefer self.alloc.free(path);
        var output_sink = try self.storage.beginAtomicWrite(self.alloc, path);
        output_sink.setCacheIntent(.cold_sequential);
        return .{
            .alloc = self.alloc,
            .storage = self.storage,
            .generation = segment_generation,
            .path = path,
            .output_sink = output_sink,
        };
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
            null,
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
        staged: StagedCheckpointSegment,
    ) !void {
        return try self.publishCheckpointInternal(
            segment_generation,
            covered_source_sequence,
            segment_bytes,
            flattened_wal_bytes,
            staged,
        );
    }

    /// Publishes an already validated streaming checkpoint without retaining
    /// its complete contents in heap merely to repeat the staged receipt.
    pub fn publishStagedCheckpointReceiptPreservingWalTail(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        flattened_wal_bytes: u64,
        staged: StagedCheckpointSegment,
    ) !void {
        return try self.publishCheckpointInternal(
            segment_generation,
            covered_source_sequence,
            null,
            flattened_wal_bytes,
            staged,
        );
    }

    /// Publishes a small immutable replacement delta over the current base
    /// generation. CURRENT names the complete ordered chain atomically; the
    /// old base and deltas remain live and mmap-safe until a later full
    /// checkpoint compacts the chain.
    pub fn publishStagedDeltaPreservingWalTail(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        segment_bytes: []const u8,
        flattened_wal_bytes: u64,
        staged: StagedCheckpointSegment,
    ) !void {
        return try self.publishCheckpointInternalMode(
            segment_generation,
            covered_source_sequence,
            segment_bytes,
            flattened_wal_bytes,
            staged,
            .delta,
        );
    }

    /// Streaming deltas carry the same durable receipt as full checkpoints;
    /// publication must not require a second, corpus-sized heap copy.
    pub fn publishStagedDeltaReceiptPreservingWalTail(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        flattened_wal_bytes: u64,
        staged: StagedCheckpointSegment,
    ) !void {
        return try self.publishCheckpointInternalMode(
            segment_generation,
            covered_source_sequence,
            null,
            flattened_wal_bytes,
            staged,
            .delta,
        );
    }

    fn publishCheckpointInternal(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        segment_bytes: ?[]const u8,
        flattened_wal_bytes: ?u64,
        staged: ?StagedCheckpointSegment,
    ) !void {
        return try self.publishCheckpointInternalMode(
            segment_generation,
            covered_source_sequence,
            segment_bytes,
            flattened_wal_bytes,
            staged,
            .full,
        );
    }

    pub const PublicationMode = enum { full, delta, compact_deltas };

    /// Open only the immutable part of a staged checkpoint. This must not
    /// prepare/rotate a WAL or write CURRENT: the real writer may already be
    /// appending a newer tail. The publication owner later validates every
    /// descriptor against its independently prepared durable transaction.
    pub fn openStagedReadersReusing(
        self: *const Store,
        alloc: Allocator,
        staged: StagedCheckpointSegment,
        sequence: u64,
        mode: PublicationMode,
        previous: []const RetainedSegment,
    ) !OpenedWithSegment {
        const current = self.checkpoint orelse return error.MissingPostingCheckpoint;
        if (staged.generation <= current.latestSegmentGeneration()) return error.OutOfOrderPostingSegmentGeneration;
        if (sequence < current.covered_source_sequence) return error.OutOfOrderPostingCheckpointSequence;
        const descriptor: posting_wal.Checkpoint.Segment = .{
            .generation = staged.generation,
            .checksum = staged.checksum,
            .admission_checksum = staged.admission_checksum,
        };
        var checkpoint = current;
        checkpoint.covered_source_sequence = sequence;
        switch (mode) {
            .full => {
                checkpoint.segment_generation = descriptor.generation;
                checkpoint.segment_checksum = descriptor.checksum;
                checkpoint.segment_admission_checksum = descriptor.admission_checksum;
                checkpoint.delta_segment_count = 0;
            },
            .delta, .compact_deltas => {
                if (mode == .compact_deltas) {
                    if (current.delta_segment_count == 0) return error.MissingPostingDeltaSegments;
                    checkpoint.delta_segment_count = 0;
                }
                if (checkpoint.delta_segment_count >= posting_wal.Checkpoint.max_delta_segments)
                    return error.TooManyPostingDeltaSegments;
                checkpoint.delta_segments[checkpoint.delta_segment_count] = descriptor;
                checkpoint.delta_segment_count += 1;
            },
        }
        var next = self.*;
        next.alloc = alloc;
        next.root_dir = try alloc.dupe(u8, self.root_dir);
        errdefer next.deinit();
        next.checkpoint = checkpoint;
        next.covered_source_sequence = sequence;
        const segments = try alloc.alloc(RetainedSegment, checkpoint.segmentCount());
        var count: usize = 0;
        errdefer {
            for (segments[0..count]) |*segment| segment.deinit(alloc);
            alloc.free(segments);
        }
        for (segments, 0..) |*segment, index| {
            const wanted = checkpoint.segment(index);
            segment.* = reuse: {
                for (previous) |retained| if (retained.retainMatching(next.root_dir, wanted)) |shared|
                    break :reuse shared;
                break :reuse try next.readSegmentRetainedFor(wanted);
            };
            count += 1;
        }
        return .{ .store = next, .segments = segments };
    }

    /// A durable candidate which is not authoritative until commitPrepared.
    /// Reader construction/admission must happen before CURRENT changes. On
    /// failure the old WAL and generation remain writable and recoverable.
    pub const PreparedPublication = struct {
        next: Store,
        encoded: [posting_wal.Checkpoint.encoded_len]u8,
        current_path: []u8,
        previous_checkpoint: ?posting_wal.Checkpoint,
        previous_wal_generation: u64,
        previous_wal_bytes: u64,
        previous_sequence: u64,
        previous_batch: ?u64,
        mode: PublicationMode,
        committed: bool = false,
        published_checkpoint: posting_wal.Checkpoint,

        pub fn deinit(self: *PreparedPublication) void {
            self.next.alloc.free(self.current_path);
            self.next.deinit();
        }

        pub fn openReaders(self: *const PreparedPublication) !OpenedWithSegment {
            return self.openReadersReusing(&.{});
        }

        pub fn openReadersReusing(self: *const PreparedPublication, previous: []const RetainedSegment) !OpenedWithSegment {
            if (self.committed) return error.PostingPublicationAlreadyCommitted;
            var next = self.next;
            next.root_dir = try next.alloc.dupe(u8, next.root_dir);
            errdefer next.deinit();
            const checkpoint = next.checkpoint.?;
            const segments = try next.alloc.alloc(RetainedSegment, checkpoint.segmentCount());
            var count: usize = 0;
            errdefer {
                for (segments[0..count]) |*segment| segment.deinit(next.alloc);
                next.alloc.free(segments);
            }
            for (segments, 0..) |*segment, index| {
                const descriptor = checkpoint.segment(index);
                segment.* = reuse: {
                    for (previous) |retained| if (retained.retainMatching(next.root_dir, descriptor)) |shared|
                        break :reuse shared;
                    break :reuse try next.readSegmentRetainedFor(descriptor);
                };
                count += 1;
            }
            return .{ .store = next, .segments = segments };
        }

        /// Reclaim only names made obsolete by this transaction, never a
        /// directory sweep which could race the next unpublished build.
        pub fn reclaimObsolete(self: *const PreparedPublication) void {
            self.reclaimObsoleteWithLeases(&.{});
        }

        pub fn reclaimObsoleteWithLeases(self: *const PreparedPublication, previous: []const RetainedSegment) void {
            if (!self.committed) return;
            const old = &self.next; // commit swaps the old store into next.
            if (old.checkpoint) |checkpoint| {
                for (0..checkpoint.segmentCount()) |index| {
                    const descriptor = checkpoint.segment(index);
                    var retained = false;
                    for (0..self.published_checkpoint.segmentCount()) |next_index| {
                        if (std.meta.eql(descriptor, self.published_checkpoint.segment(next_index))) {
                            retained = true;
                            break;
                        }
                    }
                    if (retained) continue;
                    for (previous) |segment| {
                        if (segment.retireMatching(old.storage, old.root_dir, descriptor)) {
                            retained = true;
                            break;
                        }
                    }
                    if (retained) continue;
                    const path = old.segmentPathAlloc(descriptor.generation) catch continue;
                    defer old.alloc.free(path);
                    old.storage.deleteFileAbsolute(path) catch {};
                }
            }
            if (old.checkpoint) |checkpoint| {
                for (checkpoint.sealed_wals[0..checkpoint.sealed_wal_count]) |extent| {
                    if (self.published_checkpoint.retainsWal(extent.generation)) continue;
                    const path = old.walPathAlloc(extent.generation) catch continue;
                    defer old.alloc.free(path);
                    old.storage.deleteFileAbsolute(path) catch {};
                }
            }
            if (!self.published_checkpoint.retainsWal(old.wal_generation)) {
                const path = old.walPathAlloc(old.wal_generation) catch return;
                defer old.alloc.free(path);
                old.storage.deleteFileAbsolute(path) catch {};
            }
        }
    };

    pub fn prepareStagedReceiptPreservingWalTail(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        flattened_wal_bytes: u64,
        staged: StagedCheckpointSegment,
        mode: PublicationMode,
    ) !PreparedPublication {
        return self.prepareCheckpointInternalMode(segment_generation, covered_source_sequence, null, flattened_wal_bytes, staged, mode);
    }

    pub fn prepareCheckpoint(self: *Store, segment_generation: u64, covered_source_sequence: u64, segment_bytes: []const u8) !PreparedPublication {
        return self.prepareCheckpointInternalMode(segment_generation, covered_source_sequence, segment_bytes, null, null, .full);
    }

    pub fn commitPrepared(self: *Store, prepared: *PreparedPublication) !void {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        if (prepared.committed) return error.PostingPublicationAlreadyCommitted;
        if (!std.meta.eql(self.checkpoint, prepared.previous_checkpoint) or
            self.wal_generation != prepared.previous_wal_generation or
            self.wal_committed_bytes != prepared.previous_wal_bytes or
            self.covered_source_sequence != prepared.previous_sequence or
            self.last_committed_batch != prepared.previous_batch)
            return error.InvalidPostingWalBoundary;
        generation_publication.publishControlFile(self.alloc, self.storage, prepared.current_path, &prepared.encoded) catch |err| {
            self.poisoned = true;
            return err;
        };
        std.mem.swap(Store, self, &prepared.next);
        prepared.committed = true;
    }

    fn publishCheckpointInternalMode(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        segment_bytes: ?[]const u8,
        flattened_wal_bytes: ?u64,
        staged: ?StagedCheckpointSegment,
        mode: PublicationMode,
    ) !void {
        var prepared = try self.prepareCheckpointInternalMode(segment_generation, covered_source_sequence, segment_bytes, flattened_wal_bytes, staged, mode);
        defer prepared.deinit();
        try self.commitPrepared(&prepared);
        prepared.reclaimObsolete();
    }

    fn prepareCheckpointInternalMode(
        self: *Store,
        segment_generation: u64,
        covered_source_sequence: u64,
        segment_bytes: ?[]const u8,
        flattened_wal_bytes: ?u64,
        staged: ?StagedCheckpointSegment,
        mode: PublicationMode,
    ) !PreparedPublication {
        if (self.poisoned) return error.PostingStoreRequiresReopen;
        if (self.checkpoint) |current| {
            if (segment_generation <= current.latestSegmentGeneration()) return error.OutOfOrderPostingSegmentGeneration;
        }
        if (mode == .delta) {
            const current = self.checkpoint orelse return error.MissingPostingCheckpoint;
            if (current.delta_segment_count >= posting_wal.Checkpoint.max_delta_segments) {
                return error.TooManyPostingDeltaSegments;
            }
        }
        if (mode == .compact_deltas) {
            const current = self.checkpoint orelse return error.MissingPostingCheckpoint;
            if (current.delta_segment_count == 0) return error.MissingPostingDeltaSegments;
        }
        if (staged == null) _ = try posting_segment.Reader.init(segment_bytes orelse return error.MissingPostingCheckpoint);

        var retained_wal: ?RecoveredWal = null;
        defer if (retained_wal) |*wal| wal.deinit();
        var tail: []const u8 = &.{};
        var tail_last_committed_batch: ?u64 = null;
        var tail_covered_source_sequence = covered_source_sequence;
        var tail_has_state_records = false;
        // A sealed-prefix receipt is an exact byte/sequence boundary, even
        // when later maintenance batches have the same source sequence.
        var reused_wals: ?posting_wal.Checkpoint = null;
        if (flattened_wal_bytes) |prefix_bytes| if (self.checkpoint) |current| {
            var bytes: u64 = 0;
            for (current.sealed_wals[0..current.sealed_wal_count], 0..) |extent, i| {
                bytes += extent.committed_bytes;
                if (bytes != prefix_bytes or extent.covered_source_sequence != covered_source_sequence) continue;
                var remaining = current;
                remaining.sealed_wals = [_]posting_wal.Checkpoint.SealedWal{.{}} ** posting_wal.Checkpoint.max_sealed_wals;
                remaining.sealed_wal_count = @intCast(current.sealed_wal_count - i - 1);
                @memcpy(remaining.sealed_wals[0..remaining.sealed_wal_count], current.sealed_wals[i + 1 .. current.sealed_wal_count]);
                reused_wals = remaining;
                tail_last_committed_batch = if (self.wal_committed_bytes > prefix_bytes) self.last_committed_batch else null;
                tail_covered_source_sequence = self.covered_source_sequence;
                // Conservative debt accounting avoids reading payloads here.
                tail_has_state_records = self.wal_has_state_records and self.wal_committed_bytes > prefix_bytes;
                break;
            }
        };
        if (flattened_wal_bytes != null and reused_wals == null) {
            const prefix_bytes_u64 = flattened_wal_bytes.?;
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
            const prefix_covered_source_sequence = if (prefix_bytes == 0)
                (self.checkpoint orelse return error.MissingPostingCheckpoint).covered_source_sequence
            else
                prefix_replay.covered_source_sequence;
            if (prefix_replay.committed_bytes != prefix_bytes or
                prefix_covered_source_sequence != covered_source_sequence)
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
                tail_has_state_records = replayHasStateRecords(&tail_replay);
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
        } else if (flattened_wal_bytes == null and covered_source_sequence < self.covered_source_sequence) {
            return error.OutOfOrderPostingCheckpointSequence;
        }

        const next_wal_generation = if (reused_wals != null) self.wal_generation else std.math.add(u64, self.wal_generation, 1) catch return error.PostingWalGenerationOverflow;
        const next_wal_bytes = if (reused_wals != null) self.wal_committed_bytes - flattened_wal_bytes.? else tail.len;
        const segment_path = try self.segmentPathAlloc(segment_generation);
        defer self.alloc.free(segment_path);
        const segment_checksum: u32 = if (staged) |receipt| blk: {
            if (receipt.generation != segment_generation or
                (segment_bytes != null and receipt.bytes != segment_bytes.?.len))
            {
                return error.InvalidStagedPostingSegment;
            }
            if (try self.storage.fileSize(segment_path) != receipt.bytes) return error.MissingPostingSegment;
            break :blk receipt.checksum;
        } else 0;
        const segment_admission_checksum = if (staged) |receipt|
            receipt.admission_checksum
        else
            try posting_segment.admissionChecksum(segment_bytes.?);
        const effective_segment_checksum = if (staged != null or segment_admission_checksum != 0)
            segment_checksum
        else blk: {
            // A zero admission CRC is valid but indistinguishable from the
            // legacy "field absent" encoding. Only that one-in-2^32 case pays
            // for the old full-file checksum.
            break :blk std.hash.Crc32.hash(segment_bytes.?);
        };
        if (staged == null) {
            try generation_publication.replaceColdImmutable(self.alloc, self.storage, segment_path, segment_bytes.?);
        }

        if (reused_wals == null) {
            const next_wal_path = try self.walPathAlloc(next_wal_generation);
            defer self.alloc.free(next_wal_path);
            try atomicReplace(self.alloc, self.storage, next_wal_path, tail);
        }

        var next_checkpoint: posting_wal.Checkpoint = .{
            .segment_generation = segment_generation,
            .segment_checksum = effective_segment_checksum,
            .segment_admission_checksum = segment_admission_checksum,
            .wal_generation = next_wal_generation,
            .wal_committed_bytes = next_wal_bytes,
            .covered_source_sequence = covered_source_sequence,
        };
        if (reused_wals) |remaining| {
            next_checkpoint.sealed_wal_count = remaining.sealed_wal_count;
            next_checkpoint.sealed_wals = remaining.sealed_wals;
        }
        if (mode == .delta or mode == .compact_deltas) {
            const current = self.checkpoint.?;
            next_checkpoint.segment_generation = current.segment_generation;
            next_checkpoint.segment_checksum = current.segment_checksum;
            next_checkpoint.segment_admission_checksum = current.segment_admission_checksum;
            const retained_deltas: u8 = if (mode == .delta) current.delta_segment_count else 0;
            next_checkpoint.delta_segment_count = retained_deltas + 1;
            @memcpy(next_checkpoint.delta_segments[0..retained_deltas], current.delta_segments[0..retained_deltas]);
            next_checkpoint.delta_segments[retained_deltas] = .{
                .generation = segment_generation,
                .checksum = effective_segment_checksum,
                .admission_checksum = segment_admission_checksum,
            };
        }
        const published_segment_bytes: u64 = if (staged) |receipt| receipt.bytes else @intCast(segment_bytes.?.len);
        const retained_segment_bytes: u64 = switch (mode) {
            .full => 0,
            .delta => self.segment_bytes,
            .compact_deltas => blk: {
                const base_path = try self.segmentPathAlloc(self.checkpoint.?.segment_generation);
                defer self.alloc.free(base_path);
                break :blk try self.storage.fileSize(base_path);
            },
        };
        const next_segment_bytes: u64 =
            std.math.add(u64, retained_segment_bytes, published_segment_bytes) catch
                return error.PostingSegmentTooLarge;
        const encoded = next_checkpoint.encode();
        const current_path = try self.currentPathAlloc();
        errdefer self.alloc.free(current_path);
        var next = self.*;
        next.root_dir = try self.alloc.dupe(u8, self.root_dir);
        next.checkpoint = next_checkpoint;
        next.wal_generation = next_wal_generation;
        next.wal_committed_bytes = next_wal_bytes;
        next.wal_has_state_records = tail_has_state_records;
        next.last_committed_batch = tail_last_committed_batch;
        next.covered_source_sequence = tail_covered_source_sequence;
        next.segment_bytes = next_segment_bytes;
        return .{
            .next = next,
            .encoded = encoded,
            .current_path = current_path,
            .previous_checkpoint = self.checkpoint,
            .previous_wal_generation = self.wal_generation,
            .previous_wal_bytes = self.wal_committed_bytes,
            .previous_sequence = self.covered_source_sequence,
            .previous_batch = self.last_committed_batch,
            .mode = mode,
            .published_checkpoint = next_checkpoint,
        };
    }

    pub fn recoverWal(self: *Store) !RecoveredWal {
        const wal_path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(wal_path);
        const active = self.storage.readFileAlloc(self.alloc, wal_path, max_wal_bytes + 1) catch |err| switch (err) {
            error.FileNotFound => if (self.wal_committed_bytes == 0)
                try self.alloc.alloc(u8, 0)
            else
                return error.MissingPostingWal,
            else => return err,
        };
        if (self.checkpoint == null or self.checkpoint.?.sealed_wal_count == 0) {
            errdefer self.alloc.free(active);
            if (active.len > max_wal_bytes) return error.PostingWalTooLarge;
            return .{ .alloc = self.alloc, .bytes = active, .replay = try posting_wal.Replay.parse(self.alloc, active) };
        }
        defer self.alloc.free(active);
        var combined = std.ArrayListUnmanaged(u8).empty;
        defer combined.deinit(self.alloc);
        if (self.checkpoint) |checkpoint| {
            const sealed_bytes = checkpoint.sealedWalBytes();
            if (sealed_bytes > max_wal_bytes or active.len > max_wal_bytes - sealed_bytes) return error.PostingWalTooLarge;
            try combined.ensureTotalCapacity(self.alloc, @intCast(sealed_bytes + active.len));
            for (checkpoint.sealed_wals[0..checkpoint.sealed_wal_count]) |extent| {
                const path = try self.walPathAlloc(extent.generation);
                defer self.alloc.free(path);
                const bytes = self.storage.readFileAlloc(self.alloc, path, @intCast(extent.committed_bytes + 1)) catch |err| switch (err) {
                    error.FileNotFound => return error.MissingPostingWal,
                    else => return err,
                };
                defer self.alloc.free(bytes);
                if (bytes.len != extent.committed_bytes) return error.InvalidPostingWalBoundary;
                var replay = try posting_wal.Replay.parse(self.alloc, bytes);
                defer replay.deinit();
                if (replay.committed_bytes != bytes.len or replay.covered_source_sequence != extent.covered_source_sequence or
                    replay.last_committed_batch != extent.last_batch) return error.InvalidPostingWalBoundary;
                combined.appendSliceAssumeCapacity(bytes);
            }
        }
        try combined.appendSlice(self.alloc, active);
        const bytes = try combined.toOwnedSlice(self.alloc);
        errdefer self.alloc.free(bytes);
        return .{
            .alloc = self.alloc,
            .bytes = bytes,
            .replay = try posting_wal.Replay.parse(self.alloc, bytes),
        };
    }

    /// Reconciles the flat generation directory against CURRENT. A crash at
    /// any point around publication can leave an unreferenced staged segment,
    /// while Windows may defer unlink until old mmap leases close. Retrying on
    /// startup and publication reconciliation make both cases bounded without
    /// ever
    /// deriving liveness from mutable in-memory state.
    /// The caller must own startup/publication exclusion from unpublished
    /// checkpoint builders; observational Store.open calls never invoke this.
    pub fn reclaimUnreferencedFiles(self: *const Store) !ReclaimStats {
        const names = try self.storage.listFileNamesAlloc(self.alloc, self.root_dir);
        defer lsm_backend.Storage.freeFileNames(self.alloc, names);
        var stats: ReclaimStats = .{};
        for (names) |name| {
            if (!isManagedArtifactName(name) or self.artifactNameIsLive(name)) continue;
            stats.observed_debt += 1;
            const path = try std.fs.path.join(self.alloc, &.{ self.root_dir, name });
            defer self.alloc.free(path);
            self.storage.deleteFileAbsolute(path) catch |err| switch (err) {
                error.FileNotFound => {},
                else => {
                    stats.remaining_debt += 1;
                    continue;
                },
            };
            stats.removed += 1;
        }
        return stats;
    }

    fn reclaimUnreferencedFilesBestEffort(self: *const Store) void {
        const stats = self.reclaimUnreferencedFiles() catch |err| {
            std.log.warn("posting generation cleanup deferred root={s} err={s}", .{ self.root_dir, @errorName(err) });
            return;
        };
        if (stats.remaining_debt != 0)
            std.log.warn("posting generation cleanup retained root={s} observed={} removed={} remaining={}", .{
                self.root_dir,
                stats.observed_debt,
                stats.removed,
                stats.remaining_debt,
            });
    }

    fn artifactNameIsLive(self: *const Store, name: []const u8) bool {
        if (parseManagedGeneration(name, "wal-", ".afpw")) |generation|
            return if (self.checkpoint) |checkpoint| checkpoint.retainsWal(generation) else generation == self.wal_generation;
        if (parseManagedGeneration(name, "segment-", ".afps")) |generation| {
            const checkpoint = self.checkpoint orelse return false;
            for (0..checkpoint.segmentCount()) |index| {
                if (checkpoint.segment(index).generation == generation) return true;
            }
        }
        return false;
    }

    pub fn readSegmentAlloc(self: *Store) ![]u8 {
        const checkpoint = self.checkpoint orelse return error.MissingPostingCheckpoint;
        return try self.readSegmentAllocFor(checkpoint.segment(0));
    }

    fn readSegmentAllocFor(self: *Store, descriptor: posting_wal.Checkpoint.Segment) ![]u8 {
        const path = try self.segmentPathAlloc(descriptor.generation);
        defer self.alloc.free(path);
        const bytes = self.storage.readFileAlloc(self.alloc, path, boundedReadLimit(max_segment_bytes)) catch |err| switch (err) {
            error.FileNotFound => return error.MissingPostingSegment,
            else => return err,
        };
        errdefer self.alloc.free(bytes);
        if (descriptor.admission_checksum != 0) {
            if ((posting_segment.admissionChecksum(bytes) catch 0) != descriptor.admission_checksum) {
                return error.PostingSegmentChecksumMismatch;
            }
        } else if (std.hash.Crc32.hash(bytes) != descriptor.checksum) {
            return error.PostingSegmentChecksumMismatch;
        }
        _ = try posting_segment.Reader.init(bytes);
        return bytes;
    }

    fn readSegmentRetainedFor(self: *Store, descriptor: posting_wal.Checkpoint.Segment) !RetainedSegment {
        const path = try self.segmentPathAlloc(descriptor.generation);
        defer self.alloc.free(path);
        if (mapSegmentFile(path)) |mapped| {
            const published_checksum_matches = mapped.len <= max_segment_bytes and (if (descriptor.admission_checksum != 0)
                (posting_segment.admissionChecksum(mapped) catch 0) == descriptor.admission_checksum
            else
                std.hash.Crc32.hash(mapped) == descriptor.checksum);
            if (published_checksum_matches) {
                if (posting_segment.Reader.init(mapped)) |_| {
                    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.RANDOM) catch {};
                    var payload = RetainedSegment{ .mapped = mapped };
                    errdefer payload.deinit(self.alloc);
                    return try RetainedSegment.share(self.alloc, payload, self.root_dir, descriptor);
                } else |_| {}
            }
            std.posix.munmap(mapped);
        } else |_| {}
        var payload = RetainedSegment{ .heap = try self.readSegmentAllocFor(descriptor) };
        errdefer payload.deinit(self.alloc);
        return try RetainedSegment.share(self.alloc, payload, self.root_dir, descriptor);
    }

    fn replaceWal(self: *Store, contents: []const u8) !void {
        const path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(path);
        const sealed_bytes: usize = @intCast(if (self.checkpoint) |checkpoint| checkpoint.sealedWalBytes() else 0);
        if (contents.len < sealed_bytes) return error.InvalidPostingWalBoundary;
        try atomicReplace(self.alloc, self.storage, path, contents[sealed_bytes..]);
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
        return try checkpointCurrentPathAlloc(self.alloc, self.root_dir);
    }

    fn segmentPathAlloc(self: *const Store, generation: u64) ![]u8 {
        return try checkpointSegmentPathAlloc(self.alloc, self.root_dir, generation);
    }

    fn walPathAlloc(self: *const Store, generation: u64) ![]u8 {
        return try checkpointWalPathAlloc(self.alloc, self.root_dir, generation);
    }
};

pub const OpenedWithSegment = struct {
    store: Store,
    /// Ordered oldest-to-newest: one complete base followed by replacement
    /// deltas. The slice and every retained mmap/heap buffer are owned here.
    segments: []RetainedSegment,

    pub fn deinit(self: *OpenedWithSegment) void {
        const alloc = self.store.alloc;
        self.store.deinit();
        for (self.segments) |*segment| segment.deinit(alloc);
        alloc.free(self.segments);
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
    try generation_publication.replaceImmutable(alloc, storage, path, contents);
}

fn isManagedArtifactName(name: []const u8) bool {
    return parseManagedGeneration(name, "wal-", ".afpw") != null or
        parseManagedGeneration(name, "segment-", ".afps") != null;
}

fn parseManagedGeneration(name: []const u8, prefix: []const u8, suffix: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, suffix)) return null;
    const digits = name[prefix.len .. name.len - suffix.len];
    if (digits.len == 0) return null;
    return std.fmt.parseInt(u64, digits, 10) catch null;
}

test "storage.posting segment store reclaims crash orphans from CURRENT" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const storage = memory.storage();
    try storage.writeFileAbsolute("/posting-gc/segment-99.afps", "orphan");
    try storage.writeFileAbsolute("/posting-gc/wal-99.afpw", "orphan");
    try storage.writeFileAbsolute("/posting-gc/unmanaged", "keep");

    var store = try Store.open(alloc, storage, "/posting-gc");
    defer store.deinit();
    // Ordinary opens may race an off-lock checkpoint build and are therefore
    // observational. Only the startup/publication owner reconciles orphans.
    try std.testing.expectEqual(@as(u64, "orphan".len), try storage.fileSize("/posting-gc/segment-99.afps"));
    _ = try store.reclaimUnreferencedFiles();
    try std.testing.expectError(error.FileNotFound, storage.fileSize("/posting-gc/segment-99.afps"));
    try std.testing.expectError(error.FileNotFound, storage.fileSize("/posting-gc/wal-99.afpw"));
    try std.testing.expectEqual(@as(u64, 0), try storage.fileSize("/posting-gc/wal-1.afpw"));
    try std.testing.expectEqual(@as(u64, "keep".len), try storage.fileSize("/posting-gc/unmanaged"));
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

test "storage.posting segment store poisons ambiguous CURRENT publication" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-ambiguous-current");
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBaseAt(1, 7, "base");
    const segment = try writer.build();
    defer alloc.free(segment);

    generation_publication.injectPostPublishFailuresForTest(2);
    try std.testing.expectError(
        error.GenerationPublicationDurabilityUncertain,
        store.publishCheckpoint(1, 7, segment),
    );
    try std.testing.expect(store.poisoned);
    try std.testing.expectError(error.PostingStoreRequiresReopen, store.appendCoverage(1, 8, .{}));
    try std.testing.expectError(error.PostingStoreRequiresReopen, store.markAuthoritative());
    store.deinit();

    store = try Store.open(alloc, memory.storage(), "/posting-ambiguous-current");
    defer store.deinit();
    try std.testing.expectEqual(@as(u64, 1), store.checkpoint.?.segment_generation);
    try std.testing.expectEqual(@as(u64, 7), store.covered_source_sequence);
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

test "storage.posting segment store keeps coverage-only WAL tails out of state debt" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-coverage-only-tail");
    var segment_writer = posting_segment.Writer.init(alloc);
    defer segment_writer.deinit();
    try segment_writer.appendBaseAt(3, 10, "initial");
    const segment = try segment_writer.build();
    defer alloc.free(segment);
    try store.publishCheckpoint(1, 10, segment);

    try store.appendCoverage(try store.nextBatchId(), 11, .{ .sync = false });
    try std.testing.expect(store.wal_committed_bytes > 0);
    try std.testing.expect(!store.wal_has_state_records);
    const coverage_wal_bytes = store.wal_committed_bytes;
    const coverage_batch = store.last_committed_batch;

    // Repeating the same watermark is a durable no-op, including when a
    // checkpoint finished concurrently with its caller.
    try store.appendCoverage(try store.nextBatchId(), 11, .{ .sync = false });
    try std.testing.expectEqual(coverage_wal_bytes, store.wal_committed_bytes);
    try std.testing.expectEqual(coverage_batch, store.last_committed_batch);

    const syncs_before = memory.sync_contents_calls;
    try store.appendCoverage(try store.nextBatchId(), 11, .{ .sync = true });
    try std.testing.expectEqual(syncs_before + 1, memory.sync_contents_calls);
    try std.testing.expectEqual(coverage_wal_bytes, store.wal_committed_bytes);
    try std.testing.expectEqual(coverage_batch, store.last_committed_batch);

    // A checkpoint may retain a coverage-only tail that arrived after its
    // zero-WAL source boundary. It advances recovery coverage without turning
    // into query-state debt.
    try store.publishCheckpointPreservingWalTail(2, 10, segment, 0);
    try std.testing.expectEqual(coverage_wal_bytes, store.wal_committed_bytes);
    try std.testing.expectEqual(@as(u64, 11), store.covered_source_sequence);
    try std.testing.expect(!store.wal_has_state_records);
    store.deinit();

    store = try Store.open(alloc, memory.storage(), "/posting-coverage-only-tail");
    defer store.deinit();
    try std.testing.expect(!store.wal_has_state_records);
    try store.appendBatch(try store.nextBatchId(), &.{.{
        .kind = .base,
        .posting_id = 3,
        .source_sequence = 11,
        .payload = "maintenance",
    }}, 11);
    try std.testing.expect(store.wal_has_state_records);
}

test "storage.posting segment coverage requires a published checkpoint" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-coverage-missing-checkpoint");
    defer store.deinit();
    try std.testing.expectError(
        error.MissingPostingCheckpoint,
        store.appendCoverage(1, 0, .{}),
    );
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
    const staged = try store.stageCheckpointSegment(2, next_segment);
    var wrong_generation = staged;
    wrong_generation.generation += 1;
    try std.testing.expectError(
        error.InvalidStagedPostingSegment,
        store.publishStagedCheckpointPreservingWalTail(2, 11, next_segment, flattened_wal_bytes, wrong_generation),
    );
    try store.publishStagedCheckpointPreservingWalTail(2, 11, next_segment, flattened_wal_bytes, staged);
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

test "storage.posting sealed WAL handoff reuses the active file and recovers same-sequence tails" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/posting-sealed");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBaseAt(3, 10, "base");
    const initial = try writer.build();
    defer alloc.free(initial);
    try store.publishCheckpoint(1, 10, initial);
    try store.appendBatch(try store.nextBatchId(), &.{.{ .kind = .base, .posting_id = 3, .source_sequence = 11, .payload = "sealed" }}, 11);
    const prefix_bytes = store.wal_committed_bytes;
    const sealed_generation = store.wal_generation;
    try std.testing.expect(try store.sealWalForCheckpoint());
    const active_generation = store.wal_generation;
    try std.testing.expectEqual(@as(u8, 1), store.checkpoint.?.sealed_wal_count);
    // Crash/reopen before checkpoint publication must retain the old base and
    // its entire sealed prefix, even though the append target is now empty.
    var before = try Store.open(alloc, memory.storage(), "/posting-sealed");
    defer before.deinit();
    try std.testing.expectEqual(@as(u64, 11), before.covered_source_sequence);
    try std.testing.expectEqual(prefix_bytes, before.wal_committed_bytes);
    try store.appendBatch(try store.nextBatchId(), &.{.{ .kind = .base, .posting_id = 3, .source_sequence = 11, .payload = "same-sequence-tail" }}, 11);
    const active_bytes = store.wal_committed_bytes - prefix_bytes;
    var next_writer = posting_segment.Writer.init(alloc);
    defer next_writer.deinit();
    try next_writer.appendBaseAt(3, 11, "sealed");
    const next = try next_writer.build();
    defer alloc.free(next);
    const staged = try store.stageCheckpointSegment(2, next);
    var prepared = try store.prepareStagedReceiptPreservingWalTail(2, 11, prefix_bytes, staged, .full);
    defer prepared.deinit();
    try std.testing.expectEqual(active_generation, prepared.next.wal_generation);
    try std.testing.expectEqual(active_bytes, prepared.next.wal_committed_bytes);
    try store.commitPrepared(&prepared);
    prepared.reclaimObsolete();
    try std.testing.expectEqual(active_generation, store.wal_generation);
    const sealed_path = try store.walPathAlloc(sealed_generation);
    defer alloc.free(sealed_path);
    try std.testing.expectError(error.FileNotFound, memory.storage().fileSize(sealed_path));
    var reopened = try Store.open(alloc, memory.storage(), "/posting-sealed");
    defer reopened.deinit();
    var replay = try reopened.recoverWal();
    defer replay.deinit();
    try std.testing.expectEqualStrings("same-sequence-tail", replay.replay.latest(3, .base).?.payload);
    try std.testing.expectEqual(@as(?u64, 2), reopened.last_committed_batch);
    try reopened.appendCoverage(try reopened.nextBatchId(), 12, .{ .sync = true });
}

test "storage.posting ambiguous WAL seal requires reopen and preserves the committed extent" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/posting-seal-ambiguous");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBaseAt(1, 0, "base");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    try store.publishCheckpoint(1, 0, bytes);
    try store.appendBatch(1, &.{.{ .kind = .base, .posting_id = 1, .source_sequence = 1, .payload = "committed" }}, 1);
    generation_publication.injectPostPublishFailuresForTest(2);
    defer generation_publication.injectPostPublishFailuresForTest(0);
    try std.testing.expectError(error.GenerationPublicationDurabilityUncertain, store.sealWalForCheckpoint());
    try std.testing.expectError(error.PostingStoreRequiresReopen, store.appendCoverage(2, 2, .{}));
    try std.testing.expectError(error.PostingStoreRequiresReopen, store.sealWalForCheckpoint());
    var recovered = try Store.open(alloc, memory.storage(), "/posting-seal-ambiguous");
    defer recovered.deinit();
    try std.testing.expectEqual(@as(u8, 1), recovered.checkpoint.?.sealed_wal_count);
    try std.testing.expectEqual(@as(u64, 1), recovered.covered_source_sequence);
    try recovered.appendCoverage(2, 2, .{});
    var replay = try recovered.recoverWal();
    defer replay.deinit();
    try std.testing.expectEqualStrings("committed", replay.replay.latest(1, .base).?.payload);
    try std.testing.expectEqual(@as(u64, 2), replay.replay.covered_source_sequence);
}

test "storage.posting sealed extents survive active-tail truncation and reject missing extents" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/posting-sealed-tail");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    const initial = try writer.build();
    defer alloc.free(initial);
    try store.publishCheckpoint(1, 0, initial);
    try store.appendCoverage(try store.nextBatchId(), 1, .{ .sync = true });
    try std.testing.expect(try store.sealWalForCheckpoint());
    const sealed_generation = store.checkpoint.?.sealed_wals[0].generation;
    try store.appendCoverage(try store.nextBatchId(), 2, .{ .sync = true });
    const active_path = try store.walPathAlloc(store.wal_generation);
    defer alloc.free(active_path);
    const active_bytes = try memory.storage().fileSize(active_path);
    try memory.storage().appendFileAbsolute(alloc, active_path, "partial", false);
    var reopened = try Store.open(alloc, memory.storage(), "/posting-sealed-tail");
    defer reopened.deinit();
    try std.testing.expectEqual(@as(u64, 2), reopened.covered_source_sequence);
    try std.testing.expectEqual(active_bytes, try memory.storage().fileSize(active_path));
    const sealed_path = try store.walPathAlloc(sealed_generation);
    defer alloc.free(sealed_path);
    try memory.storage().deleteFileAbsolute(sealed_path);
    try std.testing.expectError(error.MissingPostingWal, Store.open(alloc, memory.storage(), "/posting-sealed-tail"));
}

test "storage.posting checkpoint retains newer sealed extents across prefix reclamation" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/posting-multiple-extents");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    const segment = try writer.build();
    defer alloc.free(segment);
    try store.publishCheckpoint(1, 0, segment);
    var prefix: u64 = 0;
    for (1..4) |sequence| {
        try store.appendCoverage(try store.nextBatchId(), sequence, .{ .sync = true });
        if (sequence == 1) prefix = store.wal_committed_bytes;
        try std.testing.expect(try store.sealWalForCheckpoint());
    }
    const before = store.checkpoint.?;
    const staged = try store.stageCheckpointSegment(2, segment);
    var prepared = try store.prepareStagedReceiptPreservingWalTail(2, 1, prefix, staged, .full);
    defer prepared.deinit();
    try store.commitPrepared(&prepared);
    prepared.reclaimObsolete();
    try std.testing.expectEqual(@as(u8, 2), store.checkpoint.?.sealed_wal_count);
    try std.testing.expectEqual(before.wal_generation, store.wal_generation);
    _ = try store.reclaimUnreferencedFiles();
    for (before.sealed_wals[1..3]) |extent| {
        const path = try store.walPathAlloc(extent.generation);
        defer alloc.free(path);
        try std.testing.expectEqual(extent.committed_bytes, try memory.storage().fileSize(path));
    }
    var reopened = try Store.open(alloc, memory.storage(), "/posting-multiple-extents");
    defer reopened.deinit();
    try std.testing.expectEqual(@as(u64, 3), reopened.covered_source_sequence);
    try std.testing.expectEqual(@as(?u64, 3), reopened.last_committed_batch);
    try std.testing.expectEqual(store.wal_committed_bytes, reopened.wal_committed_bytes);
    try reopened.appendCoverage(try reopened.nextBatchId(), 4, .{ .sync = true });
}

test "storage.posting prepared checkpoint leaves CURRENT writable on reader failure and rejects stale tails" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/posting-prepared");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBaseAt(7, 10, "base");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    try store.publishCheckpoint(1, 10, bytes);
    const staged = try store.stageCheckpointSegment(2, bytes);
    var prepared = try store.prepareStagedReceiptPreservingWalTail(2, 10, 0, staged, .full);
    defer prepared.deinit();

    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    prepared.next.alloc = failing.allocator();
    try std.testing.expectError(error.OutOfMemory, prepared.openReaders());
    prepared.next.alloc = alloc;
    var observed = try Store.open(alloc, memory.storage(), "/posting-prepared");
    defer observed.deinit();
    try std.testing.expectEqual(@as(?u64, 1), observed.latestSegmentGeneration());
    try std.testing.expect(!store.poisoned);

    // Same-sequence maintenance must invalidate the prepared byte boundary.
    try store.appendBatch(try store.nextBatchId(), &.{.{ .kind = .base, .posting_id = 7, .source_sequence = 10, .payload = "tail" }}, 10);
    try std.testing.expectError(error.InvalidPostingWalBoundary, store.commitPrepared(&prepared));
    var refreshed = try store.prepareStagedReceiptPreservingWalTail(2, 10, 0, staged, .full);
    defer refreshed.deinit();
    var readers = try refreshed.openReaders();
    defer {
        for (readers.segments) |*segment| segment.deinit(alloc);
        alloc.free(readers.segments);
        readers.store.deinit();
    }
    var replay = try readers.store.recoverWal();
    defer replay.deinit();
    try std.testing.expectEqualStrings("tail", replay.replay.latest(7, .base).?.payload);
    try store.commitPrepared(&refreshed);
    try std.testing.expectEqual(@as(?u64, 2), store.latestSegmentGeneration());
    try std.testing.expectError(error.PostingPublicationAlreadyCommitted, store.commitPrepared(&refreshed));
    refreshed.reclaimObsolete();
    var reopened = try Store.open(alloc, memory.storage(), "/posting-prepared");
    defer reopened.deinit();
    try std.testing.expectEqual(store.wal_committed_bytes, reopened.wal_committed_bytes);
    try std.testing.expectEqual(store.checkpoint, reopened.checkpoint);
}

test "storage.posting delta publication survives restart and full compaction" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();

    var store = try Store.open(alloc, memory.storage(), "/posting-delta-chain");
    defer store.deinit();
    var base_writer = posting_segment.Writer.init(alloc);
    defer base_writer.deinit();
    try base_writer.appendBaseAt(7, 10, "base-v1");
    try base_writer.appendValueAt(9, .vector_leaf, 10, "leaf-v1");
    const base = try base_writer.build();
    defer alloc.free(base);
    try store.publishCheckpoint(1, 10, base);

    try store.appendBatch(try store.nextBatchId(), &.{
        .{ .kind = .base, .posting_id = 7, .source_sequence = 11, .payload = "base-v2" },
        .{ .kind = .vector_leaf_tombstone, .posting_id = 9, .source_sequence = 11, .payload = &.{} },
    }, 11);
    const flattened_wal_bytes = store.wal_committed_bytes;
    var delta_writer = posting_segment.Writer.init(alloc);
    defer delta_writer.deinit();
    try delta_writer.appendBaseAt(7, 11, "base-v2");
    try delta_writer.appendValueAt(9, .vector_leaf_tombstone, 11, &.{});
    const delta = try delta_writer.build();
    defer alloc.free(delta);
    const staged = try store.stageCheckpointSegment(2, delta);
    try store.publishStagedDeltaPreservingWalTail(2, 11, delta, flattened_wal_bytes, staged);
    try std.testing.expectEqual(@as(u8, 1), store.checkpoint.?.delta_segment_count);
    try std.testing.expectEqual(@as(u64, 2), store.latestSegmentGeneration().?);
    _ = try memory.storage().fileSize("/posting-delta-chain/segment-1.afps");
    _ = try memory.storage().fileSize("/posting-delta-chain/segment-2.afps");

    store.deinit();
    var opened = try Store.openWithSegmentAlloc(alloc, memory.storage(), "/posting-delta-chain");
    try std.testing.expectEqual(@as(usize, 2), opened.segments.len);
    var base_reader = try posting_segment.VerifiedReader.init(alloc, opened.segments[0].bytes());
    defer base_reader.deinit();
    var delta_reader = try posting_segment.VerifiedReader.init(alloc, opened.segments[1].bytes());
    defer delta_reader.deinit();
    try std.testing.expectEqualStrings("base-v1", (try base_reader.getValue(7, .base)).?);
    try std.testing.expectEqualStrings("base-v2", (try delta_reader.getValue(7, .base)).?);
    try std.testing.expect((try delta_reader.getValue(9, .vector_leaf_tombstone)) != null);
    store = opened.store;
    for (opened.segments) |*segment| segment.deinit(alloc);
    alloc.free(opened.segments);

    var compacted_writer = posting_segment.Writer.init(alloc);
    defer compacted_writer.deinit();
    try compacted_writer.appendBaseAt(7, 11, "base-v2");
    const compacted = try compacted_writer.build();
    defer alloc.free(compacted);
    try store.publishCheckpoint(3, 11, compacted);
    try std.testing.expectEqual(@as(u8, 0), store.checkpoint.?.delta_segment_count);
    try std.testing.expectError(error.FileNotFound, memory.storage().fileSize("/posting-delta-chain/segment-1.afps"));
    try std.testing.expectError(error.FileNotFound, memory.storage().fileSize("/posting-delta-chain/segment-2.afps"));
    _ = try memory.storage().fileSize("/posting-delta-chain/segment-3.afps");
}

test "storage.posting suffix compaction retains base and delays retired files until leases release" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/posting-fold");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBaseAt(7, 1, "base");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    try store.publishCheckpoint(1, 1, bytes);
    for (2..10) |generation| {
        const staged = try store.stageCheckpointSegment(generation, bytes);
        try store.publishStagedDeltaPreservingWalTail(generation, 1, bytes, 0, staged);
    }
    var old = try Store.openWithSegmentAlloc(alloc, memory.storage(), "/posting-fold");
    defer old.store.deinit();
    var old_owned = true;
    defer if (old_owned) {
        for (old.segments) |*segment| segment.deinit(alloc);
        alloc.free(old.segments);
    };
    const staged = try store.stageCheckpointSegment(10, bytes);
    var prepared = try store.prepareStagedReceiptPreservingWalTail(10, 1, 0, staged, .compact_deltas);
    defer prepared.deinit();
    var next = try prepared.openReadersReusing(old.segments);
    defer {
        for (next.segments) |*segment| segment.deinit(alloc);
        alloc.free(next.segments);
        next.store.deinit();
    }
    try std.testing.expectEqual(old.segments[0].bytes().ptr, next.segments[0].bytes().ptr);
    // Before CURRENT commits, neither live nor staged identities are removed.
    prepared.reclaimObsoleteWithLeases(old.segments);
    _ = try memory.storage().fileSize("/posting-fold/segment-2.afps");
    try store.commitPrepared(&prepared);
    prepared.reclaimObsoleteWithLeases(old.segments);
    try std.testing.expectEqual(@as(u8, 1), store.checkpoint.?.delta_segment_count);
    try std.testing.expectEqual(@as(u64, bytes.len * 2), store.segment_bytes);
    _ = try memory.storage().fileSize("/posting-fold/segment-2.afps");
    for (old.segments) |*segment| segment.deinit(alloc);
    alloc.free(old.segments);
    old_owned = false;
    // MemoryStorage has no owned lifetime lease. Unsupported retirement is
    // deliberately left as startup debt, never a borrowed callback.
    _ = try memory.storage().fileSize("/posting-fold/segment-2.afps");
    _ = try store.reclaimUnreferencedFiles();
    try std.testing.expectError(error.FileNotFound, memory.storage().fileSize("/posting-fold/segment-2.afps"));
    _ = try memory.storage().fileSize("/posting-fold/segment-1.afps");
    var reopened = try Store.open(alloc, memory.storage(), "/posting-fold");
    defer reopened.deinit();
    try std.testing.expectEqual(store.checkpoint, reopened.checkpoint);
    try std.testing.expectEqual(@as(u64, bytes.len * 2), reopened.segment_bytes);
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

test "storage.posting reader staging neither changes CURRENT nor rewrites the live WAL" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/posting-read-stage");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBaseAt(7, 1, "base");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    try store.publishCheckpoint(1, 1, bytes);
    const delta = try store.stageCheckpointSegment(2, bytes);
    try store.publishStagedDeltaPreservingWalTail(2, 1, bytes, 0, delta);
    var old = try Store.openWithSegmentAlloc(alloc, memory.storage(), "/posting-read-stage");
    defer old.deinit();
    const staged = try store.stageCheckpointSegment(3, bytes);
    try store.appendCoverage(1, 2, .{ .sync = true });
    const checkpoint_before = store.checkpoint;
    const wal_generation_before = store.wal_generation;
    var before = try store.recoverWal();
    defer before.deinit();
    for ([_]Store.PublicationMode{ .full, .delta, .compact_deltas }) |mode| {
        var readers = try store.openStagedReadersReusing(alloc, staged, 1, mode, old.segments);
        defer readers.deinit();
        const checkpoint = readers.store.checkpoint.?;
        try std.testing.expectEqual(@as(u64, 1), readers.store.covered_source_sequence);
        try std.testing.expectEqual(@as(u64, 3), checkpoint.latestSegmentGeneration());
        try std.testing.expectEqual(@as(usize, switch (mode) {
            .full => 1,
            .delta => 3,
            .compact_deltas => 2,
        }), readers.segments.len);
        if (mode != .full) try std.testing.expectEqual(old.segments[0].bytes().ptr, readers.segments[0].bytes().ptr);
        for (readers.segments, 0..) |segment, i|
            try std.testing.expect(segment.matchesIdentity(store.root_dir, checkpoint.segment(i)));
        try std.testing.expectEqual(checkpoint_before, store.checkpoint);
        try std.testing.expectEqual(wal_generation_before, store.wal_generation);
        var after = try store.recoverWal();
        defer after.deinit();
        try std.testing.expectEqualSlices(u8, before.bytes, after.bytes);
        var reopened = try Store.open(alloc, memory.storage(), "/posting-read-stage");
        defer reopened.deinit();
        try std.testing.expectEqual(checkpoint_before, reopened.checkpoint);
        try std.testing.expectEqual(@as(u64, 2), reopened.covered_source_sequence);
    }
}

fn testStagedReaderAllocationFailure(reader_alloc: Allocator) !void {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/posting-reader-allocation");
    defer store.deinit();
    var writer = posting_segment.Writer.init(alloc);
    defer writer.deinit();
    try writer.appendBaseAt(7, 1, "base");
    const bytes = try writer.build();
    defer alloc.free(bytes);
    try store.publishCheckpoint(1, 1, bytes);
    var old = try Store.openWithSegmentAlloc(alloc, memory.storage(), store.root_dir);
    defer old.deinit();
    const staged = try store.stageCheckpointSegment(2, bytes);
    // Fail only the new reader allocations, including a failure after an old
    // mapping has been retained. Every partial lease/array must unwind.
    var readers = try store.openStagedReadersReusing(reader_alloc, staged, 1, .delta, old.segments);
    defer readers.deinit();
    try std.testing.expectEqual(old.segments[0].bytes().ptr, readers.segments[0].bytes().ptr);
}

test "storage.posting staged readers unwind every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testStagedReaderAllocationFailure, .{});
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
