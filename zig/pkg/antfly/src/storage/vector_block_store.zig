// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license

//! Durable table-level store for mmap exact-vector blocks.
//!
//! Publication order is immutable blocks, a fresh WAL generation, then an
//! atomic CURRENT manifest. Source mutations append complete committed WAL
//! batches. A handle is poisoned after an ambiguous append failure and must be
//! reopened, preventing duplicate commits after a failed fsync.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const lsm_backend = @import("lsm_backend/mod.zig");
const generation_publication = @import("generation_publication.zig");
const vectorindex = @import("antfly_vectorindex");
const vector_block = vectorindex.vector_block;
const vector_wal = vectorindex.vector_block_wal;
const vector_manifest = vectorindex.vector_block_manifest;
const internal_keys = @import("internal_keys.zig");
const artifact_codec = @import("db/enrichment/artifact_codec.zig");

pub const Encoding = vector_block.Encoding;

const EmptyBaseLayout = struct {
    shard_count: u32,
    encoding: Encoding,
};

fn scorePrecisionForEncoding(encoding: Encoding) vector_manifest.ScorePrecision {
    return switch (encoding) {
        .float32 => .authoritative_float32,
        .float16 => .authoritative_float32_with_bounded_float16,
    };
}

const current_name = "CURRENT";
// A maximally admitted 1024-shard, 64-delta manifest is roughly 2.7 MiB
// before scoped coverage certificates. Keep the local recovery read bounded
// while permitting every layout accepted by vector_block_manifest.validate.
const max_manifest_bytes: usize = 4 * 1024 * 1024;
const max_wal_bytes: usize = 512 * 1024 * 1024;
const wal_checkpoint_bytes: usize = 64 * 1024 * 1024;
const max_block_bytes: usize = if (@sizeOf(usize) >= 8) 8 * 1024 * 1024 * 1024 else std.math.maxInt(usize);
var positional_read_test_nonce: std.atomic.Value(u64) = .init(0);

pub fn checkpointBlockPathAlloc(alloc: Allocator, root_dir: []const u8, generation: u64, shard_id: u32) ![]u8 {
    const name = try std.fmt.allocPrint(alloc, "block-{d}-{d}.afvb", .{ generation, shard_id });
    defer alloc.free(name);
    return try std.fs.path.join(alloc, &.{ root_dir, name });
}

pub fn checkpointWalPathAlloc(alloc: Allocator, root_dir: []const u8, generation: u64) ![]u8 {
    const name = try std.fmt.allocPrint(alloc, "wal-{d}.afvw", .{generation});
    defer alloc.free(name);
    return try std.fs.path.join(alloc, &.{ root_dir, name });
}

pub const RetainedBlock = struct {
    shared: *Shared,

    const MappedPayload = struct {
        bytes: []align(std.heap.page_size_min) u8,
        fd: std.posix.fd_t,
    };

    const Payload = union(enum) {
        mapped: MappedPayload,
        heap: []u8,
    };

    const Shared = struct {
        alloc: Allocator,
        refs: std.atomic.Value(u64) = .init(1),
        payload: Payload,
    };

    fn init(alloc: Allocator, payload: Payload) !RetainedBlock {
        const shared = try alloc.create(Shared);
        shared.* = .{ .alloc = alloc, .payload = payload };
        return .{ .shared = shared };
    }

    pub fn retain(self: RetainedBlock) RetainedBlock {
        _ = self.shared.refs.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn bytes(self: RetainedBlock) []const u8 {
        return switch (self.shared.payload) {
            .mapped => |value| value.bytes,
            .heap => |bytes_value| bytes_value,
        };
    }

    fn readAllAt(self: RetainedBlock, out: []u8, offset: usize) !void {
        switch (self.shared.payload) {
            .heap => |bytes_value| {
                if (offset > bytes_value.len or out.len > bytes_value.len - offset) return error.EndOfStream;
                @memcpy(out, bytes_value[offset..][0..out.len]);
            },
            .mapped => |value| {
                var read_len: usize = 0;
                while (read_len < out.len) {
                    const rc = std.posix.system.pread(value.fd, out.ptr + read_len, out.len - read_len, @intCast(offset + read_len));
                    switch (std.posix.errno(rc)) {
                        .SUCCESS => {
                            const n: usize = @intCast(rc);
                            if (n == 0) return error.EndOfStream;
                            read_len += n;
                        },
                        .INTR => continue,
                        else => |err| return std.posix.unexpectedErrno(err),
                    }
                }
            },
        }
    }

    /// Release clean pages after a sequential maintenance scan. The immutable
    /// mapping and every generation lease remain valid; a concurrent or later
    /// query can fault the page back without observing different bytes. Heap
    /// test/fallback blocks are allocator demand and must not be discarded.
    fn discardResidentPages(self: RetainedBlock) void {
        switch (self.shared.payload) {
            .mapped => |mapped| std.posix.madvise(mapped.bytes.ptr, mapped.bytes.len, std.posix.MADV.DONTNEED) catch {},
            .heap => {},
        }
    }

    pub fn deinit(self: *RetainedBlock, _: Allocator) void {
        const shared = self.shared;
        self.* = undefined;
        if (shared.refs.fetchSub(1, .acq_rel) != 1) return;
        switch (shared.payload) {
            .mapped => |mapped| {
                std.posix.munmap(mapped.bytes);
                _ = std.posix.system.close(mapped.fd);
            },
            .heap => |heap| shared.alloc.free(heap),
        }
        shared.alloc.destroy(shared);
    }
};

pub const BatchRecord = struct {
    kind: enum { upsert, tombstone },
    key: []const u8,
    source_sequence: u64,
    revision: u64,
    vector: []const f32 = &.{},
};

pub const AppendOptions = struct {
    sync: bool = true,
};

pub const ReclaimStats = struct {
    observed_debt: usize = 0,
    removed: usize = 0,
    remaining_debt: usize = 0,
};

pub const ShardBlock = struct {
    shard_id: u32,
    bytes: []const u8,
};

pub const StagedBlock = struct {
    generation: u64,
    covered_source_sequence: u64,
    shard_id: u32,
    shard_count: u32,
    bytes: u64,
    admission_checksum: u32,
    encoding: Encoding,
    score_precision: vector_manifest.ScorePrecision,
};

pub const BaseBuildOptions = struct {
    // Keep both construction phases comfortably below the governed builder
    // slice on memory-constrained nodes. Small per-shard buffers prevent the
    // partition fan-out from retaining hundreds of MiB; 128 shards bound a
    // 1M x 768 float32 spool unit near 24 MiB without doubling cold mmap/open
    // fan-out for every generation.
    shard_count: u32 = 128,
    spool_buffer_bytes: usize = 64 * 1024,
    // Float16 is the compact candidate plane. Fresh writers co-publish a
    // lossless residual for native authoritative float32 completion; callers
    // still inspect score precision because legacy float16 bases remain
    // readable as bounded-only generations until rebuilt.
    encoding: vector_block.Encoding = .float16,
    /// Sorted, unique hashes of configured embedding artifact names. The
    /// shared base stores each payload once while publishing an independent
    /// exact cardinality/membership certificate for every logical scope.
    artifact_scope_hashes: []const u64 = &.{},
};

pub const BaseBuildStats = struct {
    vectors: u64 = 0,
    vector_bytes: u64 = 0,
    artifact_bytes_scanned: u64 = 0,
    block_bytes: u64 = 0,
};

/// Durable but unpublished result of one snapshot-bound base build. Immutable
/// blocks may be staged without excluding writers; the caller later rotates
/// the WAL and publishes CURRENT under the short table-level mutation lock.
pub const StagedBaseBuild = struct {
    alloc: Allocator,
    storage: lsm_backend.Storage,
    root_dir: []u8,
    generation: u64,
    covered_source_sequence: u64,
    logical_shard_count: u32 = 0,
    encoding: Encoding = .float32,
    staged: []StagedBlock,
    coverages: []vector_manifest.Coverage,
    stats: BaseBuildStats,
    cleanup_staged: bool = true,

    /// Transfers ownership of the durable blocks to CURRENT. This is also
    /// used after an ambiguous CURRENT result: recovery, not the caller, must
    /// decide whether those files became authoritative.
    pub fn disarmCleanup(self: *StagedBaseBuild) void {
        self.cleanup_staged = false;
    }

    pub fn deinit(self: *StagedBaseBuild) void {
        if (self.cleanup_staged) discardStagedBlocksAt(
            self.storage,
            self.root_dir,
            self.staged,
        );
        self.alloc.free(self.root_dir);
        self.alloc.free(self.staged);
        self.alloc.free(self.coverages);
        self.* = undefined;
    }
};

fn discardStagedBlocksAt(
    storage: lsm_backend.Storage,
    root_dir: []const u8,
    staged: []const StagedBlock,
) void {
    for (staged) |receipt| {
        var path_buffer: [std.fs.max_path_bytes]u8 = undefined;
        const separator = if (std.mem.endsWith(u8, root_dir, std.fs.path.sep_str)) "" else std.fs.path.sep_str;
        const path = std.fmt.bufPrint(&path_buffer, "{s}{s}block-{d}-{d}.afvb", .{
            root_dir,
            separator,
            receipt.generation,
            receipt.shard_id,
        }) catch continue;
        storage.deleteFileAbsolute(path) catch {};
    }
}

pub const TopologyEpoch = struct {
    base_generation: u64,
    wal_mutation_sequence: u64,
};

pub const BaseWalDisposition = union(enum) {
    /// The staged base establishes a generation with no retained WAL prefix.
    no_tail,
    /// The staged snapshot contains this exact committed prefix; preserve any
    /// later complete batches as the new WAL tail.
    flatten_prefix: WalPrefixBoundary,
    /// A restore installed a complete authoritative source snapshot whose
    /// replay sequence belongs to a new epoch. Drop the imported old-epoch WAL
    /// instead of comparing incomparable sequence numbers.
    reset_source_epoch,
};

pub const WalPrefixBoundary = struct {
    generation: u64,
    committed_bytes: u64,
    covered_source_sequence: u64,
};

const RecoveredWal = struct {
    alloc: Allocator,
    bytes: []u8,
    replay: vector_wal.Replay,

    fn deinit(self: *RecoveredWal) void {
        self.replay.deinit();
        self.alloc.free(self.bytes);
        self.* = undefined;
    }
};

pub const Store = struct {
    alloc: Allocator,
    storage: lsm_backend.Storage,
    root_dir: []u8,
    manifest: ?vector_manifest.Manifest = null,
    manifest_segments: []vector_manifest.Segment = &.{},
    manifest_coverages: []vector_manifest.Coverage = &.{},
    wal_generation: u64 = 1,
    wal_committed_bytes: u64 = 0,
    wal_has_mutations: bool = false,
    wal_latest_mutation_sequence: u64 = 0,
    last_committed_batch: ?u64 = null,
    covered_source_sequence: u64 = 0,
    segment_covered_source_sequence: u64 = 0,
    poisoned: bool = false,

    pub fn open(alloc: Allocator, storage: lsm_backend.Storage, root_dir: []const u8) !Store {
        var opened = try openInternal(alloc, storage, root_dir, false, null);
        defer for (opened.blocks) |*block| block.deinit(alloc);
        alloc.free(opened.blocks);
        opened.blocks = &.{};
        alloc.free(opened.readers);
        opened.readers = &.{};
        alloc.free(opened.reader_order);
        opened.reader_order = &.{};
        alloc.free(opened.shard_offsets);
        opened.shard_offsets = &.{};
        opened.wal.deinit();
        alloc.free(opened.wal_bytes);
        opened.wal_bytes = &.{};
        opened.wal_order.deinit(alloc);
        return opened.store;
    }

    pub fn openWithBlocks(alloc: Allocator, storage: lsm_backend.Storage, root_dir: []const u8) !Opened {
        return try openInternal(alloc, storage, root_dir, true, null);
    }

    /// Opens the latest CURRENT/WAL while sharing unchanged immutable mmap
    /// leases with an already-open generation. Descriptors are content-bound,
    /// so copying the admitted Reader is safe and avoids O(corpus) key-index
    /// revalidation after each small WAL commit.
    pub fn openWithBlocksReusing(
        alloc: Allocator,
        storage: lsm_backend.Storage,
        root_dir: []const u8,
        previous: *const Opened,
    ) !Opened {
        return try openInternal(alloc, storage, root_dir, true, previous);
    }

    pub fn deinit(self: *Store) void {
        if (self.manifest_segments.len != 0) self.alloc.free(self.manifest_segments);
        if (self.manifest_coverages.len != 0) self.alloc.free(self.manifest_coverages);
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn nextBatchId(self: *const Store) !u64 {
        return std.math.add(u64, self.last_committed_batch orelse 0, 1) catch error.VectorWalBatchOverflow;
    }

    pub fn walPrefixBoundary(self: *const Store) WalPrefixBoundary {
        return .{
            .generation = self.wal_generation,
            .committed_bytes = self.wal_committed_bytes,
            .covered_source_sequence = self.covered_source_sequence,
        };
    }

    /// Changes only when exact-vector content changes. Coverage-only WAL
    /// commits deliberately leave this epoch stable so derived topology does
    /// not rebuild for unrelated table/source progress.
    pub fn topologyEpoch(self: *const Store) ?TopologyEpoch {
        const manifest = self.manifest orelse return null;
        return .{
            .base_generation = manifest.base_generation,
            .wal_mutation_sequence = self.wal_latest_mutation_sequence,
        };
    }

    pub fn shouldCheckpointWal(self: *const Store) bool {
        return self.wal_has_mutations and self.wal_committed_bytes >= wal_checkpoint_bytes;
    }

    pub fn checkpointedThrough(self: *const Store) u64 {
        return self.segment_covered_source_sequence;
    }

    pub fn appendBatch(
        self: *Store,
        batch_id: u64,
        records: []const BatchRecord,
        covered_source_sequence: u64,
        options: AppendOptions,
    ) !void {
        if (self.poisoned) return error.VectorBlockStoreRequiresReopen;
        if (self.manifest == null) return error.MissingVectorBlockManifest;
        if (records.len == 0) return error.EmptyVectorWalBatch;
        var writer = vector_wal.Writer.initAfterCommitted(self.alloc, self.last_committed_batch, self.covered_source_sequence);
        defer writer.deinit();
        for (records) |record| {
            if (record.source_sequence < self.segment_covered_source_sequence) return error.VectorWalOverlapsCheckpoint;
            if (record.source_sequence > covered_source_sequence) return error.InvalidVectorWalCommit;
            switch (record.kind) {
                .upsert => try writer.appendUpsert(batch_id, record.source_sequence, record.revision, record.key, record.vector),
                .tombstone => {
                    if (record.vector.len != 0) return error.InvalidVectorWalRecord;
                    try writer.appendTombstone(batch_id, record.source_sequence, record.revision, record.key);
                },
            }
        }
        try writer.commit(batch_id, covered_source_sequence);
        try self.appendWriter(batch_id, covered_source_sequence, writer.bytes(), options);
        self.wal_has_mutations = true;
        for (records) |record| {
            self.wal_latest_mutation_sequence = @max(self.wal_latest_mutation_sequence, record.source_sequence);
        }
    }

    pub fn appendCoverage(self: *Store, batch_id: u64, covered_source_sequence: u64, options: AppendOptions) !void {
        if (self.poisoned) return error.VectorBlockStoreRequiresReopen;
        if (self.manifest == null) return error.MissingVectorBlockManifest;
        var writer = vector_wal.Writer.initAfterCommitted(self.alloc, self.last_committed_batch, self.covered_source_sequence);
        defer writer.deinit();
        try writer.appendCoverage(batch_id, covered_source_sequence);
        try writer.commit(batch_id, covered_source_sequence);
        try self.appendWriter(batch_id, covered_source_sequence, writer.bytes(), options);
    }

    fn appendWriter(self: *Store, batch_id: u64, covered_source_sequence: u64, bytes: []const u8, options: AppendOptions) !void {
        const next_bytes = std.math.add(u64, self.wal_committed_bytes, bytes.len) catch return error.VectorWalTooLarge;
        if (next_bytes > max_wal_bytes) return error.VectorWalTooLarge;
        const path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(path);
        self.storage.appendFileAbsolute(self.alloc, path, bytes, options.sync) catch |err| {
            self.poisoned = true;
            return err;
        };
        self.wal_committed_bytes = next_bytes;
        self.last_committed_batch = batch_id;
        self.covered_source_sequence = covered_source_sequence;
    }

    /// Publishes either a complete replacement base or a sparse delta. Every
    /// block must already contain the requested generation, shard layout, and
    /// source watermark. The current WAL must be fully represented by the new
    /// generation; concurrent appenders are serialized by the caller.
    pub fn publishGeneration(
        self: *Store,
        generation: u64,
        covered_source_sequence: u64,
        blocks: []const ShardBlock,
        replace_base: bool,
    ) !void {
        if (self.poisoned) return error.VectorBlockStoreRequiresReopen;
        if (blocks.len == 0) return error.EmptyVectorBlockGeneration;
        if (covered_source_sequence != self.covered_source_sequence) return error.InvalidVectorBlockPublicationBoundary;
        const receipts = try self.alloc.alloc(StagedBlock, blocks.len);
        defer self.alloc.free(receipts);
        var previous_shard: ?u32 = null;
        for (blocks, 0..) |block, i| {
            if (previous_shard) |previous| if (block.shard_id <= previous) return error.OutOfOrderVectorBlockShard;
            receipts[i] = try self.stageBlock(generation, covered_source_sequence, block.shard_id, block.bytes);
            previous_shard = block.shard_id;
        }
        try self.publishStagedGeneration(generation, covered_source_sequence, receipts, replace_base);
    }

    /// Makes one immutable block durable without publishing it. Callers can
    /// build, stage, and release one shard at a time, keeping a 3+ GiB corpus
    /// out of process heap during base construction.
    pub fn stageBlock(
        self: *Store,
        generation: u64,
        covered_source_sequence: u64,
        shard_id: u32,
        bytes: []const u8,
    ) !StagedBlock {
        if (self.poisoned) return error.VectorBlockStoreRequiresReopen;
        const reader = try vector_block.Reader.init(bytes);
        if (reader.generation != generation or reader.shard_id != shard_id or
            reader.covered_source_sequence != covered_source_sequence)
        {
            return error.InvalidVectorBlockGeneration;
        }
        const path = try self.blockPathAlloc(generation, shard_id);
        defer self.alloc.free(path);
        try generation_publication.replaceColdImmutable(self.alloc, self.storage, path, bytes);
        return .{
            .generation = generation,
            .covered_source_sequence = covered_source_sequence,
            .shard_id = shard_id,
            .shard_count = reader.shard_count,
            .bytes = bytes.len,
            .admission_checksum = reader.admissionChecksum(),
            .encoding = reader.encoding,
            .score_precision = switch (reader.encoding) {
                .float32 => .authoritative_float32,
                .float16 => if (reader.hasExactResiduals())
                    .authoritative_float32_with_bounded_float16
                else
                    .bounded_float16,
            },
        };
    }

    /// Removes an unpublished build reservation. Staged block names are never
    /// referenced by CURRENT until publication, so cleanup is safe even while
    /// readers retain the preceding generation.
    pub fn discardStagedBlocks(self: *const Store, staged: []const StagedBlock) void {
        discardStagedBlocksAt(self.storage, self.root_dir, staged);
    }

    pub fn publishStagedGeneration(
        self: *Store,
        generation: u64,
        covered_source_sequence: u64,
        staged: []const StagedBlock,
        replace_base: bool,
    ) !void {
        return try self.publishStagedGenerationMode(
            generation,
            covered_source_sequence,
            staged,
            if (replace_base) .replace_base else .append_delta,
            null,
            null,
            false,
            null,
        );
    }

    fn publishStagedBaseWithCoverage(
        self: *Store,
        generation: u64,
        covered_source_sequence: u64,
        staged: []const StagedBlock,
        coverages: []const vector_manifest.Coverage,
    ) !void {
        return try self.publishStagedGenerationMode(
            generation,
            covered_source_sequence,
            staged,
            .replace_base,
            coverages,
            null,
            false,
            null,
        );
    }

    /// Replaces the immutable base built at `covered_source_sequence` while
    /// carrying every batch committed after `flattened` into the new WAL.
    /// The boundary is a byte offset after a complete commit frame, so batches
    /// sharing a source sequence are never filtered or lost.
    pub fn publishStagedBasePreservingWalTail(
        self: *Store,
        generation: u64,
        covered_source_sequence: u64,
        staged: []const StagedBlock,
        coverages: []const vector_manifest.Coverage,
        flattened: WalPrefixBoundary,
    ) !void {
        return try self.publishStagedGenerationMode(
            generation,
            covered_source_sequence,
            staged,
            .replace_base,
            coverages,
            flattened,
            false,
            null,
        );
    }

    /// Publishes an owned staged snapshot and transfers its durable blocks to
    /// CURRENT. Before the CURRENT commit point, errors leave cleanup armed;
    /// after an ambiguous commit result the poisoned Store disarms cleanup so
    /// recovery can safely determine which generation won.
    pub fn publishStagedBaseBuild(
        self: *Store,
        build: *StagedBaseBuild,
        wal_disposition: BaseWalDisposition,
    ) !void {
        const poisoned_before = self.poisoned;
        const flattened_wal: ?WalPrefixBoundary = switch (wal_disposition) {
            .no_tail, .reset_source_epoch => null,
            .flatten_prefix => |boundary| boundary,
        };
        self.publishStagedGenerationMode(
            build.generation,
            build.covered_source_sequence,
            build.staged,
            .replace_base,
            build.coverages,
            flattened_wal,
            wal_disposition == .reset_source_epoch,
            if (build.staged.len == 0) .{
                .shard_count = build.logical_shard_count,
                .encoding = build.encoding,
            } else null,
        ) catch |err| {
            if (!poisoned_before and self.poisoned) build.disarmCleanup();
            return err;
        };
        build.disarmCleanup();
    }

    const PublicationMode = enum { replace_base, append_delta, replace_deltas };

    /// Replaces every existing sparse delta with one merged delta while
    /// retaining the immutable base. Old mmap leases remain valid after their
    /// unlinked files leave CURRENT, so active queries keep generation
    /// isolation without forcing a corpus-sized base rewrite.
    fn publishCompactedDeltaGeneration(
        self: *Store,
        generation: u64,
        covered_source_sequence: u64,
        staged: []const StagedBlock,
    ) !void {
        return try self.publishStagedGenerationMode(
            generation,
            covered_source_sequence,
            staged,
            .replace_deltas,
            null,
            null,
            false,
            null,
        );
    }

    fn publishStagedGenerationMode(
        self: *Store,
        generation: u64,
        covered_source_sequence: u64,
        staged: []const StagedBlock,
        mode: PublicationMode,
        replacement_coverages: ?[]const vector_manifest.Coverage,
        flattened_wal: ?WalPrefixBoundary,
        reset_source_epoch: bool,
        empty_base_layout: ?EmptyBaseLayout,
    ) !void {
        if (self.poisoned) return error.VectorBlockStoreRequiresReopen;
        if (staged.len == 0 and empty_base_layout == null) return error.EmptyVectorBlockGeneration;
        // A complete authoritative snapshot can replace an older base at a
        // newer source watermark when there is no WAL tail to merge. Once the
        // WAL contains mutations, publication must stay at its exact covered
        // boundary or it could silently drop a committed update.
        const replace_base = mode == .replace_base;
        if (empty_base_layout != null and !replace_base) return error.InvalidVectorBlockGeneration;
        if (reset_source_epoch and (!replace_base or flattened_wal != null))
            return error.InvalidVectorBlockPublicationBoundary;
        if (flattened_wal != null and !replace_base) return error.InvalidVectorBlockPublicationBoundary;
        const authoritative_replacement = replace_base and
            (!self.wal_has_mutations or flattened_wal != null or reset_source_epoch);
        if (!authoritative_replacement and covered_source_sequence != self.covered_source_sequence) return error.InvalidVectorBlockPublicationBoundary;
        if (flattened_wal) |boundary| {
            if (boundary.generation != self.wal_generation or
                boundary.covered_source_sequence > covered_source_sequence or
                boundary.committed_bytes > self.wal_committed_bytes or
                (covered_source_sequence > self.covered_source_sequence and
                    boundary.committed_bytes != self.wal_committed_bytes))
            {
                return error.InvalidVectorBlockPublicationBoundary;
            }
        }
        const shard_count = if (staged.len != 0) staged[0].shard_count else empty_base_layout.?.shard_count;
        if (shard_count == 0 or shard_count > vector_manifest.max_shards or !std.math.isPowerOfTwo(shard_count))
            return error.InvalidVectorBlockGeneration;
        const staged_precision: vector_manifest.ScorePrecision = if (staged.len != 0)
            staged[0].score_precision
        else
            scorePrecisionForEncoding(empty_base_layout.?.encoding);
        if (replace_base and staged.len != 0 and staged.len != shard_count) return error.IncompleteVectorBlockBase;
        if (!replace_base and self.manifest == null) return error.MissingVectorBlockManifest;
        if (self.manifest) |manifest| {
            if (generation <= manifest.latest_generation) return error.OutOfOrderVectorBlockGeneration;
            // A complete snapshot with no committed WAL tail represents every
            // key at one pinned source boundary, so it may also replace the
            // physical shard layout atomically. Deltas and WAL-backed
            // generations must preserve shard identity or lookups could route
            // around an older committed record.
            if (manifest.shard_count != shard_count and !authoritative_replacement) return error.InvalidVectorBlockGeneration;
        }
        var previous_shard: ?u32 = null;
        for (staged, 0..) |receipt, i| {
            if (receipt.generation != generation or receipt.covered_source_sequence != covered_source_sequence or
                receipt.shard_count != shard_count or receipt.shard_id >= shard_count or
                receipt.encoding != staged[0].encoding or receipt.score_precision != staged_precision)
            {
                return error.InvalidVectorBlockGeneration;
            }
            if (previous_shard) |previous| if (receipt.shard_id <= previous) return error.OutOfOrderVectorBlockShard;
            if (replace_base and receipt.shard_id != i) return error.IncompleteVectorBlockBase;
            const path = try self.blockPathAlloc(generation, receipt.shard_id);
            defer self.alloc.free(path);
            if (try self.storage.fileSize(path) != receipt.bytes) return error.MissingVectorBlock;
            previous_shard = receipt.shard_id;
        }

        const base_segment_count: usize = if (self.manifest) |manifest| baseSegmentCount(manifest) else 0;
        const retained_count = switch (mode) {
            .replace_base => 0,
            .append_delta => self.manifest_segments.len,
            .replace_deltas => base_segment_count,
        };
        const next_count = retained_count + staged.len;
        const next_segments = try self.alloc.alloc(vector_manifest.Segment, next_count);
        errdefer self.alloc.free(next_segments);
        if (retained_count != 0) @memcpy(next_segments[0..retained_count], self.manifest_segments[0..retained_count]);
        for (staged, next_segments[retained_count..]) |receipt, *descriptor| descriptor.* = stagedDescriptor(receipt);
        const coverage_source = if (replace_base)
            replacement_coverages orelse &.{}
        else
            self.manifest_coverages;
        const next_coverages = try self.alloc.dupe(vector_manifest.Coverage, coverage_source);
        errdefer self.alloc.free(next_coverages);
        var recovered_wal: ?RecoveredWal = null;
        defer if (recovered_wal) |*wal| wal.deinit();
        var next_wal_bytes: []const u8 = &.{};
        var next_wal_last_batch: ?u64 = null;
        var next_wal_has_mutations = false;
        var next_wal_latest_mutation_sequence: u64 = 0;
        var next_covered_source_sequence = covered_source_sequence;
        if (flattened_wal) |boundary| {
            recovered_wal = try self.recoverWal();
            const recovered = &recovered_wal.?;
            if (@as(u64, @intCast(recovered.replay.committed_bytes)) != self.wal_committed_bytes) {
                return error.InvalidVectorBlockPublicationBoundary;
            }
            const prefix_len = std.math.cast(usize, boundary.committed_bytes) orelse return error.InvalidVectorBlockPublicationBoundary;
            var prefix = try vector_wal.Replay.parse(self.alloc, recovered.bytes[0..prefix_len]);
            defer prefix.deinit();
            if (prefix.committed_bytes != prefix_len or
                (prefix_len != 0 and prefix.covered_source_sequence != boundary.covered_source_sequence))
            {
                return error.InvalidVectorBlockPublicationBoundary;
            }
            next_wal_bytes = recovered.bytes[prefix_len..recovered.replay.committed_bytes];
            if (next_wal_bytes.len != 0) {
                var tail = try vector_wal.Replay.parse(self.alloc, next_wal_bytes);
                defer tail.deinit();
                if (tail.committed_bytes != next_wal_bytes.len) return error.InvalidVectorBlockPublicationBoundary;
                for (tail.records.items) |record| {
                    if (record.kind != .coverage and record.source_sequence <= covered_source_sequence)
                        return error.VectorWalOverlapsCheckpoint;
                    if (record.kind == .upsert or record.kind == .tombstone) {
                        next_wal_has_mutations = true;
                        next_wal_latest_mutation_sequence = @max(next_wal_latest_mutation_sequence, record.source_sequence);
                    }
                }
                next_wal_last_batch = tail.last_committed_batch;
                next_covered_source_sequence = @max(covered_source_sequence, tail.covered_source_sequence);
            }
        }
        const next_wal_generation = std.math.add(u64, self.wal_generation, 1) catch return error.VectorWalGenerationOverflow;
        const next_manifest: vector_manifest.Manifest = .{
            .base_generation = if (replace_base) generation else self.manifest.?.base_generation,
            .latest_generation = generation,
            .wal_generation = next_wal_generation,
            .wal_committed_bytes = @intCast(next_wal_bytes.len),
            .covered_source_sequence = next_covered_source_sequence,
            .shard_count = shard_count,
            .segments = next_segments,
            .coverages = next_coverages,
            .score_precision = if (replace_base)
                staged_precision
            else if (self.manifest.?.score_precision == staged_precision)
                staged_precision
            else
                .bounded_float16,
        };
        try next_manifest.validate();
        const encoded = try next_manifest.encodeAlloc(self.alloc);
        defer self.alloc.free(encoded);
        const next_wal_path = try self.walPathAlloc(next_wal_generation);
        defer self.alloc.free(next_wal_path);
        // Allocate post-publication cleanup state before the CURRENT commit
        // point so no allocator failure can make a committed generation look
        // retryable to the caller.
        const previous_wal_path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(previous_wal_path);
        try atomicReplace(self.alloc, self.storage, next_wal_path, next_wal_bytes);
        const current_path = try self.currentPathAlloc();
        defer self.alloc.free(current_path);
        generation_publication.publishControlFile(self.alloc, self.storage, current_path, encoded) catch |err| {
            self.poisoned = true;
            return err;
        };

        const obsolete_start: usize = switch (mode) {
            .replace_base => 0,
            .append_delta => self.manifest_segments.len,
            .replace_deltas => base_segment_count,
        };
        for (self.manifest_segments[obsolete_start..]) |descriptor| {
            const obsolete_path = self.blockPathAlloc(descriptor.generation, descriptor.shard_id) catch continue;
            defer self.alloc.free(obsolete_path);
            self.storage.deleteFileAbsolute(obsolete_path) catch {};
        }
        self.storage.deleteFileAbsolute(previous_wal_path) catch {};
        if (self.manifest_segments.len != 0) self.alloc.free(self.manifest_segments);
        if (self.manifest_coverages.len != 0) self.alloc.free(self.manifest_coverages);
        self.manifest_segments = next_segments;
        self.manifest_coverages = next_coverages;
        self.manifest = next_manifest;
        self.manifest.?.segments = self.manifest_segments;
        self.manifest.?.coverages = self.manifest_coverages;
        self.wal_generation = next_wal_generation;
        self.wal_committed_bytes = @intCast(next_wal_bytes.len);
        self.wal_has_mutations = next_wal_has_mutations;
        self.wal_latest_mutation_sequence = next_wal_latest_mutation_sequence;
        self.last_committed_batch = next_wal_last_batch;
        self.covered_source_sequence = next_covered_source_sequence;
        self.segment_covered_source_sequence = covered_source_sequence;
        self.reclaimUnreferencedFilesBestEffort();
    }

    /// Reconciles immutable blocks and WAL generations against CURRENT. This
    /// recovers both pre-publication staged orphans and post-publication unlink
    /// failures. Active native mmap leases remain valid on POSIX; providers
    /// which cannot unlink an open file leave it for the next retry/open.
    /// The caller must own startup/publication exclusion from unpublished
    /// block builders; observational Store.open calls never invoke this.
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
            std.log.warn("vector-block generation cleanup deferred root={s} err={s}", .{ self.root_dir, @errorName(err) });
            return;
        };
        if (stats.remaining_debt != 0)
            std.log.warn("vector-block generation cleanup retained root={s} observed={} removed={} remaining={}", .{
                self.root_dir,
                stats.observed_debt,
                stats.removed,
                stats.remaining_debt,
            });
    }

    fn artifactNameIsLive(self: *const Store, name: []const u8) bool {
        if (parseWalGeneration(name)) |generation| return generation == self.wal_generation;
        const identity = parseBlockIdentity(name) orelse return false;
        for (self.manifest_segments) |descriptor| {
            if (descriptor.generation == identity.generation and descriptor.shard_id == identity.shard_id)
                return true;
        }
        return false;
    }

    /// Builds a complete shared base from authoritative embedding artifacts
    /// with bounded memory. The source scan spools by hash shard; each shard is
    /// then sorted, encoded, staged, and released before the next is opened.
    pub fn buildBaseFromArtifacts(
        self: *Store,
        doc_store: anytype,
        generation: u64,
        covered_source_sequence: u64,
        options: BaseBuildOptions,
    ) !BaseBuildStats {
        var txn = try doc_store.beginReadTxn();
        defer txn.abort();
        return try self.buildBaseFromArtifactsTxn(
            doc_store,
            &txn,
            generation,
            covered_source_sequence,
            options,
        );
    }

    /// Snapshot-bound variant used by production publication. The manifest's
    /// source watermark must describe the exact primary snapshot scanned into
    /// the base; accepting a separately-opened scan could publish vectors from
    /// a newer source generation under an older HBC lease.
    pub fn buildBaseFromArtifactsTxn(
        self: *Store,
        doc_store: anytype,
        txn: anytype,
        generation: u64,
        covered_source_sequence: u64,
        options: BaseBuildOptions,
    ) !BaseBuildStats {
        var build = try self.stageBaseFromArtifactsTxn(
            doc_store,
            txn,
            generation,
            covered_source_sequence,
            options,
        );
        defer build.deinit();
        try self.publishStagedBaseBuild(&build, .no_tail);
        return build.stats;
    }

    pub fn stageBaseFromArtifactsTxn(
        self: *Store,
        doc_store: anytype,
        txn: anytype,
        generation: u64,
        covered_source_sequence: u64,
        options: BaseBuildOptions,
    ) !StagedBaseBuild {
        if (builtin.target.cpu.arch.endian() != .little) return error.UnsupportedVectorBlockBuildEndian;
        if (options.shard_count == 0 or options.shard_count > vector_manifest.max_shards or
            !std.math.isPowerOfTwo(options.shard_count) or options.spool_buffer_bytes == 0)
        {
            return error.InvalidVectorBlockBuildOptions;
        }
        var previous_scope: ?u64 = null;
        for (options.artifact_scope_hashes) |scope_hash| {
            if (previous_scope) |previous| if (scope_hash <= previous) return error.InvalidVectorBlockBuildOptions;
            previous_scope = scope_hash;
        }
        const coverages = try self.alloc.alloc(vector_manifest.Coverage, options.artifact_scope_hashes.len);
        errdefer self.alloc.free(coverages);
        for (coverages, options.artifact_scope_hashes) |*coverage, scope_hash| coverage.* = .{
            .scope_hash = scope_hash,
            .vector_count = 0,
            .key_hash_xor = 0,
            .key_hash_sum = 0,
        };
        const buffers = try self.alloc.alloc(std.ArrayListUnmanaged(u8), options.shard_count);
        defer self.alloc.free(buffers);
        for (buffers) |*buffer| buffer.* = .empty;
        defer for (buffers) |*buffer| buffer.deinit(self.alloc);

        // Create a spool only when its shard receives a record. This removes
        // the 128-file create+sync tax from empty tables and avoids physical
        // files for untouched shards during sparse builds.
        const spool_initialized = try self.alloc.alloc(bool, options.shard_count);
        defer self.alloc.free(spool_initialized);
        @memset(spool_initialized, false);

        const spool_paths = try self.alloc.alloc([]u8, options.shard_count);
        var path_count: usize = 0;
        defer {
            for (spool_paths[0..path_count]) |path| {
                self.storage.deleteFileAbsolute(path) catch {};
                self.alloc.free(path);
            }
            self.alloc.free(spool_paths);
        }
        for (0..options.shard_count) |shard| {
            const name = try std.fmt.allocPrint(self.alloc, "spool-{d}.tmp", .{shard});
            defer self.alloc.free(name);
            spool_paths[shard] = try std.fs.path.join(self.alloc, &.{ self.root_dir, name });
            path_count += 1;
        }

        var stats: BaseBuildStats = .{};
        const ScanContext = struct {
            alloc: Allocator,
            storage: lsm_backend.Storage,
            buffers: []std.ArrayListUnmanaged(u8),
            paths: []const []u8,
            initialized: []bool,
            shard_count: u32,
            flush_bytes: usize,
            encoding: vector_block.Encoding,
            stats: *BaseBuildStats,
            coverages: []vector_manifest.Coverage,

            fn coverageIndex(ctx: *const @This(), scope_hash: u64) ?usize {
                var lo: usize = 0;
                var hi = ctx.coverages.len;
                while (lo < hi) {
                    const mid = lo + (hi - lo) / 2;
                    if (ctx.coverages[mid].scope_hash < scope_hash) lo = mid + 1 else hi = mid;
                }
                if (lo >= ctx.coverages.len or ctx.coverages[lo].scope_hash != scope_hash) return null;
                return lo;
            }

            fn flush(ctx: *@This(), shard: usize) !void {
                const buffer = &ctx.buffers[shard];
                if (buffer.items.len == 0) return;
                if (!ctx.initialized[shard]) {
                    // Replace, rather than append to, a stale crash artifact.
                    try atomicReplace(ctx.alloc, ctx.storage, ctx.paths[shard], &.{});
                    ctx.initialized[shard] = true;
                }
                try ctx.storage.appendFileAbsolute(ctx.alloc, ctx.paths[shard], buffer.items, false);
                buffer.clearRetainingCapacity();
            }

            fn appendVectorRecord(
                ctx: *@This(),
                key: []const u8,
                artifact: []const u8,
                vector: []const f32,
                coverage_index: usize,
            ) !void {
                const dims = std.math.cast(u32, vector.len) orelse return error.VectorBlockTooLarge;
                const source_vector_bytes = std.math.mul(usize, vector.len, @sizeOf(f32)) catch return error.VectorBlockTooLarge;
                const encoded_bytes = try vector_block.encodedVectorBytesLen(ctx.encoding, vector.len);
                const residual_max = if (ctx.encoding == .float16)
                    try vector_block.exactResidualMaxBytes(vector.len)
                else
                    0;
                const record_max = std.math.add(usize, 40 + key.len + encoded_bytes, residual_max) catch return error.VectorBlockTooLarge;
                const hash = vector_block.keyHash(key);
                const shard: usize = @intCast(hash & (@as(u64, ctx.shard_count) - 1));
                var record = try ctx.alloc.alloc(u8, record_max);
                defer ctx.alloc.free(record);
                std.mem.writeInt(u64, record[0..8], hash, .big);
                std.mem.writeInt(u32, record[8..12], @intCast(key.len), .big);
                std.mem.writeInt(u32, record[12..16], dims, .big);
                std.mem.writeInt(u64, record[16..24], std.hash.XxHash64.hash(0, artifact), .big);
                @memcpy(record[40..][0..key.len], key);
                const vector_out = record[40 + key.len ..][0..encoded_bytes];
                const encoded = try vector_block.encodeVectorIntoWithStats(ctx.encoding, vector, vector_out);
                std.mem.writeInt(u32, record[24..28], @bitCast(encoded.scale), .little);
                std.mem.writeInt(u32, record[28..32], @bitCast(encoded.quantization.error_norm), .little);
                std.mem.writeInt(u32, record[32..36], @bitCast(encoded.quantization.decoded_norm_lower_bound), .little);
                const residual_len = if (ctx.encoding == .float16)
                    try vector_block.encodeExactResidualInto(
                        vector,
                        vector_out,
                        encoded.scale,
                        record[40 + key.len + encoded_bytes ..],
                    )
                else
                    0;
                std.mem.writeInt(u32, record[36..40], @intCast(residual_len), .little);
                try ctx.buffers[shard].appendSlice(ctx.alloc, record[0 .. 40 + key.len + encoded_bytes + residual_len]);
                ctx.stats.vectors += 1;
                // Preserve source-byte accounting: the primary artifact and
                // WAL remain float32 even when the transient spool and final
                // query projection use a narrower encoding.
                ctx.stats.vector_bytes += source_vector_bytes;
                ctx.stats.artifact_bytes_scanned += artifact.len;
                const coverage = &ctx.coverages[coverage_index];
                coverage.vector_count +|= 1;
                coverage.key_hash_xor ^= hash;
                coverage.key_hash_sum +%= hash;
                if (ctx.buffers[shard].items.len >= ctx.flush_bytes) try ctx.flush(shard);
            }

            fn scan(raw_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!@import("docstore.zig").DocStore.ScanAction {
                const ctx: *@This() = @ptrCast(@alignCast(raw_ctx orelse return error.InvalidArgument));
                if (!internal_keys.isEmbeddingArtifactKey(key) and !internal_keys.isDerivedEmbeddingArtifactKey(key)) return .@"continue";
                // The vector projection is shared by active dense indexes, not
                // an archive of every historical embedding artifact. Reject
                // inactive scopes before decoding or spooling their payloads;
                // multiple indexes sharing one scope still store it once.
                const scope_hash = internal_keys.embeddingArtifactScopeHash(key) orelse return .@"continue";
                const coverage_index = ctx.coverageIndex(scope_hash) orelse return .@"continue";
                const header = try artifact_codec.decodeHeader(value);
                if (header.kind != .dense_embedding) return .@"continue";
                const dims = try artifact_codec.decodeDenseEmbeddingDims(value);
                if (dims == 0) return error.InvalidVectorDimensions;
                if (try artifact_codec.denseEmbeddingVectorView(value)) |vector| {
                    try ctx.appendVectorRecord(key, value, vector, coverage_index);
                } else {
                    const scratch = try ctx.alloc.alloc(f32, dims);
                    defer ctx.alloc.free(scratch);
                    const vector = try artifact_codec.decodeDenseEmbeddingInto(value, scratch);
                    try ctx.appendVectorRecord(key, value, vector, coverage_index);
                }
                return .@"continue";
            }
        };
        var scan_context: ScanContext = .{
            .alloc = self.alloc,
            .storage = self.storage,
            .buffers = buffers,
            .paths = spool_paths,
            .initialized = spool_initialized,
            .shard_count = options.shard_count,
            .flush_bytes = options.spool_buffer_bytes,
            .encoding = options.encoding,
            .stats = &stats,
            .coverages = coverages,
        };
        try doc_store.scanReadTxnWithContext(txn, "", "", .{}, &scan_context, ScanContext.scan);
        for (0..options.shard_count) |shard| {
            try scan_context.flush(shard);
            if (spool_initialized[shard]) try self.storage.syncFileContentsAbsolute(spool_paths[shard]);
            // ArrayList.clearRetainingCapacity deliberately made the scan
            // fast, but retaining every shard buffer through the build would
            // add shard_count * spool_buffer_bytes to peak anonymous memory.
            buffers[shard].deinit(self.alloc);
            buffers[shard] = .empty;
        }

        // Empty authority is a real V4 manifest with the final logical shard
        // topology and no physical data files. The first WAL checkpoint can
        // therefore publish sparse blocks directly in their serving shards;
        // it never creates a corpus-sized one-shard bootstrap generation.
        const physical_shard_count: u32 = if (stats.vectors == 0) 0 else options.shard_count;
        const staged = try self.alloc.alloc(StagedBlock, physical_shard_count);
        errdefer self.alloc.free(staged);
        var staged_count: usize = 0;
        errdefer self.discardStagedBlocks(staged[0..staged_count]);
        for (0..physical_shard_count) |shard| {
            const spool = if (spool_initialized[shard])
                try self.storage.readFileAlloc(self.alloc, spool_paths[shard], boundedReadLimit(max_block_bytes))
            else
                try self.alloc.alloc(u8, 0);
            defer self.alloc.free(spool);
            var entries = try parseSpoolEntries(self.alloc, spool, options.encoding);
            defer entries.deinit(self.alloc);
            std.mem.sortUnstable(SpoolEntry, entries.items, {}, SpoolEntry.lessThan);
            var writer = try vector_block.Writer.initWithEncoding(
                self.alloc,
                generation,
                @intCast(shard),
                physical_shard_count,
                covered_source_sequence,
                options.encoding,
            );
            defer writer.deinit();
            for (entries.items) |entry| {
                if (entry.exact_residual) |residual| {
                    try writer.appendEncodedVectorWithStatsAndResidual(
                        entry.key,
                        covered_source_sequence,
                        entry.revision,
                        entry.dims,
                        entry.vector_bytes,
                        entry.scale,
                        entry.quantization,
                        residual,
                    );
                } else {
                    try writer.appendEncodedVectorWithStats(
                        entry.key,
                        covered_source_sequence,
                        entry.revision,
                        entry.dims,
                        entry.vector_bytes,
                        entry.scale,
                        entry.quantization,
                    );
                }
            }
            const block = try writer.build();
            defer self.alloc.free(block);
            stats.block_bytes += block.len;
            staged[shard] = try self.stageBlock(generation, covered_source_sequence, @intCast(shard), block);
            staged_count += 1;
        }
        const build_root = try self.alloc.dupe(u8, self.root_dir);
        errdefer self.alloc.free(build_root);
        return .{
            .alloc = self.alloc,
            .storage = self.storage,
            .root_dir = build_root,
            .generation = generation,
            .covered_source_sequence = covered_source_sequence,
            .logical_shard_count = options.shard_count,
            .encoding = options.encoding,
            .staged = staged,
            .coverages = coverages,
            .stats = stats,
        };
    }

    fn currentPathAlloc(self: *const Store) ![]u8 {
        return try std.fs.path.join(self.alloc, &.{ self.root_dir, current_name });
    }

    fn walPathAlloc(self: *const Store, generation: u64) ![]u8 {
        return try checkpointWalPathAlloc(self.alloc, self.root_dir, generation);
    }

    fn blockPathAlloc(self: *const Store, generation: u64, shard_id: u32) ![]u8 {
        return try checkpointBlockPathAlloc(self.alloc, self.root_dir, generation, shard_id);
    }

    fn recoverWal(self: *const Store) !RecoveredWal {
        const path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(path);
        const bytes = try self.storage.readFileAlloc(self.alloc, path, max_wal_bytes + 1);
        errdefer self.alloc.free(bytes);
        var replay = try vector_wal.Replay.parse(self.alloc, bytes);
        errdefer replay.deinit();
        if (replay.committed_bytes != bytes.len) return error.InvalidVectorBlockPublicationBoundary;
        return .{ .alloc = self.alloc, .bytes = bytes, .replay = replay };
    }
};

/// Generation-local handle for a vector whose key/revision lookup has already
/// been validated. Immutable block handles retain physical offsets; WAL
/// handles borrow the exact float32 record owned by the same Opened lease.
pub const LocatedValue = union(enum) {
    wal: vector_block.Value,
    block: struct {
        reader_index: usize,
        reader_generation: u64,
        reader_shard_id: u32,
        location: vector_block.ValueLocation,
    },

    pub fn projectionBytes(self: LocatedValue) usize {
        return switch (self) {
            .wal => 0,
            .block => |value| value.location.vector_len,
        };
    }

    pub fn residualBytes(self: LocatedValue) usize {
        return switch (self) {
            .wal => 0,
            .block => |value| value.location.residual_len,
        };
    }

    pub fn exactScratchBytes(self: LocatedValue) !usize {
        return switch (self) {
            .wal => 0,
            .block => |value| value.location.scratchBytes(),
        };
    }
};

pub const LocatedLookup = union(enum) {
    missing,
    tombstone: vector_block.Tombstone,
    vector: LocatedValue,
};

/// A compact vector plane whose payload checksum has already been validated,
/// tied to the immutable location and generation lease that owns its bytes.
/// Keeping this view for one query avoids both a second lookup and a second
/// projection read when the RaBitQ interval proof requests exact completion.
pub const LoadedProjection = struct {
    located: LocatedValue,
    value: vector_block.Value,
};

/// One independently owned destination in a bounded positional-read batch.
/// Callers retain the Opened generation and every scratch slice until the
/// batch returns. Individual I/O or validation failures are reported on the
/// request so a query can fall back to primary authority without discarding
/// successful siblings.
pub const ProjectionReadRequest = struct {
    located: LocatedValue,
    scratch: []u8,
    value: ?vector_block.Value = null,
    err: ?anyerror = null,
};

pub const ExactReadRequest = struct {
    located: LocatedValue,
    scratch: []u8,
    value: ?vector_block.Value = null,
    err: ?anyerror = null,
};

pub const ResidualReadRequest = struct {
    projection: LoadedProjection,
    scratch: []u8,
    value: ?vector_block.Value = null,
    err: ?anyerror = null,
};

pub const ReadBatchStats = struct {
    physical_reads: u64 = 0,
    physical_bytes: u64 = 0,
};

pub const Opened = struct {
    /// A wave is deliberately smaller than an ANN rerank batch. The shared
    /// std.Io runtime supplies global backpressure while this local ceiling
    /// prevents one query from monopolizing its workers or multiplying the
    /// query's transient payload residency.
    const positional_read_wave: usize = 8;

    store: Store,
    blocks: []RetainedBlock,
    readers: []vector_block.Reader,
    /// Reader indexes grouped by shard while preserving manifest generation
    /// order inside each group. Queries walk only one shard, newest first.
    reader_order: []usize,
    shard_offsets: []usize,
    wal_bytes: []u8,
    wal: vector_wal.Replay,
    wal_order: std.ArrayListUnmanaged(usize),

    pub fn deinit(self: *Opened) void {
        const alloc = self.store.alloc;
        self.store.deinit();
        for (self.blocks) |*block| block.deinit(alloc);
        alloc.free(self.blocks);
        alloc.free(self.readers);
        alloc.free(self.reader_order);
        alloc.free(self.shard_offsets);
        self.wal.deinit();
        alloc.free(self.wal_bytes);
        self.wal_order.deinit(alloc);
        self.* = undefined;
    }

    pub fn baseEncoding(self: *const Opened) ?vector_block.Encoding {
        const manifest = self.store.manifest orelse return null;
        if (!manifest.hasPhysicalBase()) return switch (manifest.score_precision) {
            .authoritative_float32 => .float32,
            .bounded_float16, .authoritative_float32_with_bounded_float16 => .float16,
            .unspecified => null,
        };
        const base_shards: usize = @intCast(manifest.shard_count);
        if (self.readers.len < base_shards or base_shards == 0) return null;
        const encoding = self.readers[0].encoding;
        for (self.readers[1..base_shards]) |reader| {
            if (reader.generation != manifest.base_generation or reader.encoding != encoding) return null;
        }
        return encoding;
    }

    pub fn usesBaseEncoding(self: *const Opened, encoding: vector_block.Encoding) bool {
        return self.baseEncoding() == encoding;
    }

    pub fn scorePrecision(self: *const Opened) vector_manifest.ScorePrecision {
        const manifest = self.store.manifest orelse return .unspecified;
        if (manifest.score_precision != .unspecified) return manifest.score_precision;
        // V1/V2 manifests predate the declaration. Their immutable block
        // encoding is nevertheless explicit and fully admission-validated.
        return switch (self.baseEncoding() orelse return .unspecified) {
            .float32 => .authoritative_float32,
            .float16 => .bounded_float16,
        };
    }

    /// Returns an exact O(shards) cardinality certificate when CURRENT is a
    /// complete immutable base plus coverage-only WAL. Sparse delta blocks or
    /// vector-bearing WAL require key reconciliation, so callers must treat
    /// those layouts as having no cheap certificate rather than guessing from
    /// physical entry counts.
    pub fn baseOnlyVectorCount(self: *const Opened) ?u64 {
        const manifest = self.store.manifest orelse return null;
        if (self.store.wal_has_mutations or manifest.segments.len != baseSegmentCount(manifest)) return null;
        return self.baseVectorCount();
    }

    /// Returns the complete-base certificate for one configured artifact
    /// family. Sparse overlays remain transactionally authoritative but do not
    /// have an additive physical-count interpretation, matching
    /// baseOnlyVectorCount's readiness contract.
    pub fn baseOnlyCoverage(self: *const Opened, scope_hash: u64) ?vector_manifest.Coverage {
        const manifest = self.store.manifest orelse return null;
        if (self.store.wal_has_mutations or manifest.segments.len != baseSegmentCount(manifest)) return null;
        var lo: usize = 0;
        var hi = manifest.coverages.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (manifest.coverages[mid].scope_hash < scope_hash) lo = mid + 1 else hi = mid;
        }
        if (lo >= manifest.coverages.len or manifest.coverages[lo].scope_hash != scope_hash) return null;
        return manifest.coverages[lo];
    }

    /// Counts the immutable base without reconciling its WAL/delta overlays.
    /// This distinguishes an empty bootstrap base from an established base
    /// that is serving ordinary online mutations.
    pub fn baseVectorCount(self: *const Opened) ?u64 {
        const manifest = self.store.manifest orelse return null;
        if (!manifest.hasPhysicalBase()) return 0;
        const base_shards: usize = @intCast(manifest.shard_count);
        if (self.readers.len < base_shards) return null;
        var count: u64 = 0;
        for (self.readers[0..base_shards]) |reader| {
            if (reader.generation != manifest.base_generation) return null;
            count = std.math.add(u64, count, reader.count) catch return null;
        }
        return count;
    }

    /// Rewrites the immutable base, sparse deltas, and one committed WAL
    /// prefix as a complete base generation. Publication preserves a later
    /// complete WAL tail atomically. Work is shard-local: at most one output
    /// shard and its compact ordering index are resident at a time,
    /// independent of total corpus size.
    pub fn compactDeltasToBase(self: *Opened) !bool {
        const manifest = self.store.manifest orelse return error.MissingVectorBlockManifest;
        return self.compactDeltasToBaseWithShardCount(manifest.shard_count, 64 * 1024);
    }

    /// Complete-base compaction with an optional topology change. The normal
    /// path preserves shard-local ordering. Bootstrap and format-migration
    /// callers may instead fan the latest live records into bounded temporary
    /// spools, then build one destination shard at a time. This avoids both a
    /// primary-LSM rescan and a corpus-sized in-memory repartition.
    pub fn compactDeltasToBaseWithShardCount(
        self: *Opened,
        target_shard_count: u32,
        spool_buffer_bytes: usize,
    ) !bool {
        const manifest = self.store.manifest orelse return error.MissingVectorBlockManifest;
        if (target_shard_count == 0 or target_shard_count > vector_manifest.max_shards or
            !std.math.isPowerOfTwo(target_shard_count) or spool_buffer_bytes == 0)
        {
            return error.InvalidVectorBlockBuildOptions;
        }
        if (target_shard_count != manifest.shard_count) {
            return self.compactDeltasToReshardedBase(target_shard_count, spool_buffer_bytes);
        }
        const shard_count: usize = @intCast(manifest.shard_count);
        if (!self.store.wal_has_mutations and manifest.segments.len == baseSegmentCount(manifest)) return false;
        const flattened_wal = if (self.store.wal_has_mutations)
            self.store.walPrefixBoundary()
        else
            null;

        const generation = std.math.add(u64, manifest.latest_generation, 1) catch
            return error.VectorBlockGenerationOverflow;
        const encoding = self.baseEncoding() orelse return error.InconsistentVectorBlockEncoding;
        const staged = try self.store.alloc.alloc(StagedBlock, shard_count);
        defer self.store.alloc.free(staged);
        const coverages = try self.store.alloc.alloc(vector_manifest.Coverage, manifest.coverages.len);
        defer self.store.alloc.free(coverages);
        for (coverages, manifest.coverages) |*coverage, existing| coverage.* = .{
            .scope_hash = existing.scope_hash,
            .vector_count = 0,
            .key_hash_xor = 0,
            .key_hash_sum = 0,
        };
        var staged_count: usize = 0;
        var records = std.ArrayListUnmanaged(CompactionRecord).empty;
        defer records.deinit(self.store.alloc);
        var scratch = std.ArrayListUnmanaged(f32).empty;
        defer scratch.deinit(self.store.alloc);
        const wal_by_shard = try self.store.alloc.alloc(std.ArrayListUnmanaged(usize), shard_count);
        defer self.store.alloc.free(wal_by_shard);
        for (wal_by_shard) |*items| items.* = .empty;
        defer for (wal_by_shard) |*items| items.deinit(self.store.alloc);
        if (flattened_wal != null) try self.collectLatestWalByShard(wal_by_shard);

        for (0..shard_count) |shard| {
            records.clearRetainingCapacity();
            const start = self.shard_offsets[shard];
            const end = self.shard_offsets[shard + 1];
            for (self.reader_order[start..end]) |reader_index| {
                const reader = self.readers[reader_index];
                for (0..reader.count) |entry_index| {
                    const entry = try reader.entryAt(entry_index);
                    try records.append(self.store.alloc, CompactionRecord.fromBlock(entry, reader.generation));
                }
            }
            for (wal_by_shard[shard].items) |record_index| {
                try records.append(self.store.alloc, CompactionRecord.fromWal(self.wal.records.items[record_index], generation));
            }
            std.mem.sortUnstable(CompactionRecord, records.items, {}, CompactionRecord.lessThan);

            var writer = try vector_block.Writer.initWithEncoding(
                self.store.alloc,
                generation,
                @intCast(shard),
                manifest.shard_count,
                self.store.covered_source_sequence,
                encoding,
            );
            defer writer.deinit();
            var pos: usize = 0;
            while (pos < records.items.len) {
                var record_end = pos + 1;
                while (record_end < records.items.len and records.items[pos].sameKey(records.items[record_end])) : (record_end += 1) {}
                const latest = records.items[record_end - 1];
                if (latest.isLive()) noteCoverage(coverages, latest.key);
                switch (latest.payload) {
                    .wal_vector => try latest.appendTo(&writer, self.store.alloc, &scratch),
                    .block_vector, .tombstone => try latest.appendLiveBlockVectorTo(&writer, encoding),
                }
                pos = record_end;
            }
            const bytes = try writer.build();
            defer self.store.alloc.free(bytes);
            staged[staged_count] = try self.store.stageBlock(
                generation,
                self.store.covered_source_sequence,
                @intCast(shard),
                bytes,
            );
            staged_count += 1;
            // The next shard cannot reference these immutable input pages.
            // Drop only residency, not the mmap lease, so compaction does not
            // accumulate a corpus-sized RSS high-water.
            self.discardShardResidentPages(shard, true);
        }
        if (flattened_wal) |boundary| {
            try self.store.publishStagedBasePreservingWalTail(
                generation,
                self.store.covered_source_sequence,
                staged[0..staged_count],
                coverages,
                boundary,
            );
        } else {
            try self.store.publishStagedBaseWithCoverage(
                generation,
                self.store.covered_source_sequence,
                staged[0..staged_count],
                coverages,
            );
        }
        return true;
    }

    fn compactDeltasToReshardedBase(
        self: *Opened,
        target_shard_count: u32,
        spool_buffer_bytes: usize,
    ) !bool {
        const manifest = self.store.manifest orelse return error.MissingVectorBlockManifest;
        const flattened_wal = if (self.store.wal_has_mutations)
            self.store.walPrefixBoundary()
        else
            null;
        const generation = std.math.add(u64, manifest.latest_generation, 1) catch
            return error.VectorBlockGenerationOverflow;
        const encoding = self.baseEncoding() orelse return error.InconsistentVectorBlockEncoding;
        const destination_count: usize = @intCast(target_shard_count);

        const buffers = try self.store.alloc.alloc(std.ArrayListUnmanaged(u8), destination_count);
        defer self.store.alloc.free(buffers);
        for (buffers) |*buffer| buffer.* = .empty;
        defer for (buffers) |*buffer| buffer.deinit(self.store.alloc);
        const spool_initialized = try self.store.alloc.alloc(bool, destination_count);
        defer self.store.alloc.free(spool_initialized);
        @memset(spool_initialized, false);
        const spool_paths = try self.store.alloc.alloc([]u8, destination_count);
        var path_count: usize = 0;
        defer {
            for (spool_paths[0..path_count]) |path| {
                self.store.storage.deleteFileAbsolute(path) catch {};
                self.store.alloc.free(path);
            }
            self.store.alloc.free(spool_paths);
        }
        for (0..destination_count) |shard| {
            const name = try std.fmt.allocPrint(self.store.alloc, "compact-{d}-{d}.tmp", .{ generation, shard });
            defer self.store.alloc.free(name);
            spool_paths[shard] = try std.fs.path.join(self.store.alloc, &.{ self.store.root_dir, name });
            path_count += 1;
        }

        const coverages = try self.store.alloc.alloc(vector_manifest.Coverage, manifest.coverages.len);
        defer self.store.alloc.free(coverages);
        for (coverages, manifest.coverages) |*coverage, existing| coverage.* = .{
            .scope_hash = existing.scope_hash,
            .vector_count = 0,
            .key_hash_xor = 0,
            .key_hash_sum = 0,
        };
        var records = std.ArrayListUnmanaged(CompactionRecord).empty;
        defer records.deinit(self.store.alloc);
        var decode_scratch = std.ArrayListUnmanaged(f32).empty;
        defer decode_scratch.deinit(self.store.alloc);

        const SpoolContext = struct {
            alloc: Allocator,
            storage: lsm_backend.Storage,
            buffers: []std.ArrayListUnmanaged(u8),
            paths: []const []u8,
            initialized: []bool,
            shard_count: u32,
            flush_bytes: usize,
            encoding: vector_block.Encoding,
            decode_scratch: *std.ArrayListUnmanaged(f32),

            fn flush(ctx: *@This(), shard: usize) !void {
                const buffer = &ctx.buffers[shard];
                if (buffer.items.len == 0) return;
                if (!ctx.initialized[shard]) {
                    try atomicReplace(ctx.alloc, ctx.storage, ctx.paths[shard], &.{});
                    ctx.initialized[shard] = true;
                }
                try ctx.storage.appendFileAbsolute(ctx.alloc, ctx.paths[shard], buffer.items, false);
                buffer.clearRetainingCapacity();
            }

            fn append(ctx: *@This(), record: CompactionRecord) !void {
                if (!record.isLive()) return;
                const dims: u32 = switch (record.payload) {
                    .tombstone => unreachable,
                    .block_vector => |value| value.dims,
                    .wal_vector => |value| value.dims,
                };
                const encoded_len = try vector_block.encodedVectorBytesLen(ctx.encoding, dims);
                const residual_max = if (ctx.encoding == .float16)
                    try vector_block.exactResidualMaxBytes(dims)
                else
                    0;
                const record_max = std.math.add(usize, 40 + record.key.len + encoded_len, residual_max) catch
                    return error.VectorBlockTooLarge;
                const bytes = try ctx.alloc.alloc(u8, record_max);
                defer ctx.alloc.free(bytes);
                std.mem.writeInt(u64, bytes[0..8], record.hash, .big);
                std.mem.writeInt(u32, bytes[8..12], @intCast(record.key.len), .big);
                std.mem.writeInt(u32, bytes[12..16], dims, .big);
                std.mem.writeInt(u64, bytes[16..24], record.revision, .big);
                @memcpy(bytes[40..][0..record.key.len], record.key);
                const vector_out = bytes[40 + record.key.len ..][0..encoded_len];
                var scale: f32 = 1;
                var quantization: vector_block.QuantizationStats = undefined;
                var residual_len: usize = 0;
                switch (record.payload) {
                    .tombstone => unreachable,
                    .block_vector => |value| {
                        if (value.encoding != ctx.encoding) return error.InconsistentVectorBlockEncoding;
                        scale = value.scale;
                        if (value.quantization_error_norm) |error_norm| {
                            quantization = .{
                                .error_norm = error_norm,
                                .decoded_norm_lower_bound = value.decoded_norm_lower_bound orelse return error.CorruptedVectorBlock,
                            };
                            @memcpy(vector_out, value.bytes);
                            if (value.exact_residual) |residual| {
                                @memcpy(bytes[40 + record.key.len + encoded_len ..][0..residual.len], residual);
                                residual_len = residual.len;
                            }
                        } else {
                            try ctx.decode_scratch.resize(ctx.alloc, value.dims);
                            const decoded = try value.decodeInto(ctx.decode_scratch.items);
                            const encoded = try vector_block.encodeVectorIntoWithStats(ctx.encoding, decoded, vector_out);
                            scale = encoded.scale;
                            quantization = encoded.quantization;
                        }
                    },
                    .wal_vector => |value| {
                        try ctx.decode_scratch.resize(ctx.alloc, value.dims);
                        const decoded = try value.decodeVectorInto(ctx.decode_scratch.items);
                        const encoded = try vector_block.encodeVectorIntoWithStats(ctx.encoding, decoded, vector_out);
                        scale = encoded.scale;
                        quantization = encoded.quantization;
                        if (ctx.encoding == .float16) {
                            residual_len = try vector_block.encodeExactResidualInto(
                                decoded,
                                vector_out,
                                scale,
                                bytes[40 + record.key.len + encoded_len ..][0..residual_max],
                            );
                        }
                    },
                }
                std.mem.writeInt(u32, bytes[24..28], @bitCast(scale), .little);
                std.mem.writeInt(u32, bytes[28..32], @bitCast(quantization.error_norm), .little);
                std.mem.writeInt(u32, bytes[32..36], @bitCast(quantization.decoded_norm_lower_bound), .little);
                std.mem.writeInt(u32, bytes[36..40], @intCast(residual_len), .little);
                const shard: usize = @intCast(record.hash & (@as(u64, ctx.shard_count) - 1));
                try ctx.buffers[shard].appendSlice(ctx.alloc, bytes[0 .. 40 + record.key.len + encoded_len + residual_len]);
                if (ctx.buffers[shard].items.len >= ctx.flush_bytes) try ctx.flush(shard);
            }
        };
        var spool: SpoolContext = .{
            .alloc = self.store.alloc,
            .storage = self.store.storage,
            .buffers = buffers,
            .paths = spool_paths,
            .initialized = spool_initialized,
            .shard_count = target_shard_count,
            .flush_bytes = spool_buffer_bytes,
            .encoding = encoding,
            .decode_scratch = &decode_scratch,
        };

        const source_shard_count: usize = @intCast(manifest.shard_count);
        const wal_by_shard = try self.store.alloc.alloc(std.ArrayListUnmanaged(usize), source_shard_count);
        defer self.store.alloc.free(wal_by_shard);
        for (wal_by_shard) |*items| items.* = .empty;
        defer for (wal_by_shard) |*items| items.deinit(self.store.alloc);
        if (flattened_wal != null) try self.collectLatestWalByShard(wal_by_shard);
        for (0..source_shard_count) |source_shard| {
            records.clearRetainingCapacity();
            const start = self.shard_offsets[source_shard];
            const end = self.shard_offsets[source_shard + 1];
            for (self.reader_order[start..end]) |reader_index| {
                const reader = self.readers[reader_index];
                for (0..reader.count) |entry_index| {
                    const entry = try reader.entryAt(entry_index);
                    try records.append(self.store.alloc, CompactionRecord.fromBlock(entry, reader.generation));
                }
            }
            for (wal_by_shard[source_shard].items) |record_index| {
                try records.append(self.store.alloc, CompactionRecord.fromWal(self.wal.records.items[record_index], generation));
            }
            std.mem.sortUnstable(CompactionRecord, records.items, {}, CompactionRecord.lessThan);
            var pos: usize = 0;
            while (pos < records.items.len) {
                var record_end = pos + 1;
                while (record_end < records.items.len and records.items[pos].sameKey(records.items[record_end])) : (record_end += 1) {}
                const latest = records.items[record_end - 1];
                if (latest.isLive()) {
                    noteCoverage(coverages, latest.key);
                    try spool.append(latest);
                }
                pos = record_end;
            }
            self.discardShardResidentPages(source_shard, true);
        }
        for (0..destination_count) |shard| {
            try spool.flush(shard);
            if (spool_initialized[shard]) try self.store.storage.syncFileContentsAbsolute(spool_paths[shard]);
            buffers[shard].deinit(self.store.alloc);
            buffers[shard] = .empty;
        }

        const staged = try self.store.alloc.alloc(StagedBlock, destination_count);
        defer self.store.alloc.free(staged);
        var staged_count: usize = 0;
        errdefer self.store.discardStagedBlocks(staged[0..staged_count]);
        for (0..destination_count) |shard| {
            const spool_bytes = if (spool_initialized[shard])
                try self.store.storage.readFileAlloc(self.store.alloc, spool_paths[shard], boundedReadLimit(max_block_bytes))
            else
                try self.store.alloc.alloc(u8, 0);
            defer self.store.alloc.free(spool_bytes);
            var entries = try parseSpoolEntries(self.store.alloc, spool_bytes, encoding);
            defer entries.deinit(self.store.alloc);
            std.mem.sortUnstable(SpoolEntry, entries.items, {}, SpoolEntry.lessThan);
            var writer = try vector_block.Writer.initWithEncoding(
                self.store.alloc,
                generation,
                @intCast(shard),
                target_shard_count,
                self.store.covered_source_sequence,
                encoding,
            );
            defer writer.deinit();
            for (entries.items) |entry| {
                if (entry.exact_residual) |residual| {
                    try writer.appendEncodedVectorWithStatsAndResidual(
                        entry.key,
                        self.store.covered_source_sequence,
                        entry.revision,
                        entry.dims,
                        entry.vector_bytes,
                        entry.scale,
                        entry.quantization,
                        residual,
                    );
                } else {
                    try writer.appendEncodedVectorWithStats(
                        entry.key,
                        self.store.covered_source_sequence,
                        entry.revision,
                        entry.dims,
                        entry.vector_bytes,
                        entry.scale,
                        entry.quantization,
                    );
                }
            }
            const block = try writer.build();
            defer self.store.alloc.free(block);
            staged[staged_count] = try self.store.stageBlock(
                generation,
                self.store.covered_source_sequence,
                @intCast(shard),
                block,
            );
            staged_count += 1;
        }
        if (flattened_wal) |boundary| {
            try self.store.publishStagedBasePreservingWalTail(
                generation,
                self.store.covered_source_sequence,
                staged[0..staged_count],
                coverages,
                boundary,
            );
        } else {
            try self.store.publishStagedBaseWithCoverage(
                generation,
                self.store.covered_source_sequence,
                staged[0..staged_count],
                coverages,
            );
        }
        return true;
    }

    /// Checkpoints the committed mutation WAL into mmap-friendly sparse
    /// blocks. Ordinary checkpoints contain only WAL-touched shards. At the
    /// manifest's delta-chain limit, prior deltas and the WAL are coalesced
    /// into one sparse generation, preserving tombstones against the base and
    /// keeping both write amplification and query fan-out bounded.
    pub fn checkpointWalToDelta(self: *Opened, force: bool) !bool {
        if (!self.store.wal_has_mutations) return false;
        if (!force and !self.store.shouldCheckpointWal()) return false;
        const manifest = self.store.manifest orelse return error.MissingVectorBlockManifest;
        // Initial ingestion is an append-heavy, mostly disjoint workload over
        // the intentionally empty bootstrap base. Preserve those immutable
        // runs and merge them once at stable tip instead of repeatedly
        // rewriting every vector after each eight WAL checkpoints. Established
        // bases keep the short online chain and its point-lookup bound.
        const base_is_empty = (self.baseVectorCount() orelse 1) == 0;
        const delta_limit = if (base_is_empty)
            vector_manifest.max_bootstrap_delta_generations
        else
            vector_manifest.max_online_delta_generations;
        const compact_existing = deltaGenerationCount(manifest) >= delta_limit;
        const generation = std.math.add(u64, manifest.latest_generation, 1) catch return error.VectorBlockGenerationOverflow;
        const encoding = self.baseEncoding() orelse return error.InconsistentVectorBlockEncoding;
        const shard_count: usize = @intCast(manifest.shard_count);

        const wal_by_shard = try self.store.alloc.alloc(std.ArrayListUnmanaged(usize), shard_count);
        defer self.store.alloc.free(wal_by_shard);
        for (wal_by_shard) |*items| items.* = .empty;
        defer for (wal_by_shard) |*items| items.deinit(self.store.alloc);
        try self.collectLatestWalByShard(wal_by_shard);

        var staged = std.ArrayListUnmanaged(StagedBlock).empty;
        defer staged.deinit(self.store.alloc);
        var records = std.ArrayListUnmanaged(CompactionRecord).empty;
        defer records.deinit(self.store.alloc);
        var scratch = std.ArrayListUnmanaged(f32).empty;
        defer scratch.deinit(self.store.alloc);

        for (0..shard_count) |shard| {
            records.clearRetainingCapacity();
            if (compact_existing) {
                const start = self.shard_offsets[shard];
                const end = self.shard_offsets[shard + 1];
                for (self.reader_order[start..end]) |reader_index| {
                    const reader = self.readers[reader_index];
                    if (reader.generation == manifest.base_generation) continue;
                    for (0..reader.count) |entry_index| {
                        const entry = try reader.entryAt(entry_index);
                        try records.append(self.store.alloc, CompactionRecord.fromBlock(entry, reader.generation));
                    }
                }
            }
            for (wal_by_shard[shard].items) |record_index| {
                try records.append(self.store.alloc, CompactionRecord.fromWal(self.wal.records.items[record_index], generation));
            }
            if (records.items.len == 0) continue;
            std.mem.sortUnstable(CompactionRecord, records.items, {}, CompactionRecord.lessThan);

            var writer = try vector_block.Writer.initWithEncoding(
                self.store.alloc,
                generation,
                @intCast(shard),
                manifest.shard_count,
                self.store.covered_source_sequence,
                encoding,
            );
            defer writer.deinit();
            var pos: usize = 0;
            while (pos < records.items.len) {
                var end = pos + 1;
                while (end < records.items.len and records.items[pos].sameKey(records.items[end])) : (end += 1) {}
                try records.items[end - 1].appendTo(&writer, self.store.alloc, &scratch);
                pos = end;
            }
            const bytes = try writer.build();
            defer self.store.alloc.free(bytes);
            try staged.append(self.store.alloc, try self.store.stageBlock(
                generation,
                self.store.covered_source_sequence,
                @intCast(shard),
                bytes,
            ));
            if (compact_existing) self.discardShardResidentPages(shard, false);
        }
        if (staged.items.len == 0) return error.EmptyVectorBlockGeneration;
        if (compact_existing) {
            try self.store.publishCompactedDeltaGeneration(generation, self.store.covered_source_sequence, staged.items);
        } else {
            try self.store.publishStagedGeneration(generation, self.store.covered_source_sequence, staged.items, false);
        }
        return true;
    }

    fn discardShardResidentPages(self: *const Opened, shard: usize, include_base: bool) void {
        const manifest = self.store.manifest orelse return;
        const start = self.shard_offsets[shard];
        const end = self.shard_offsets[shard + 1];
        for (self.reader_order[start..end]) |reader_index| {
            if (!include_base and self.readers[reader_index].generation == manifest.base_generation) continue;
            self.blocks[reader_index].discardResidentPages();
        }
    }

    fn collectLatestWalByShard(self: *const Opened, by_shard: []std.ArrayListUnmanaged(usize)) !void {
        var pos: usize = 0;
        while (pos < self.wal_order.items.len) {
            const first = self.wal.records.items[self.wal_order.items[pos]];
            var end = pos + 1;
            while (end < self.wal_order.items.len) : (end += 1) {
                const candidate = self.wal.records.items[self.wal_order.items[end]];
                if (candidate.key_hash != first.key_hash or !std.mem.eql(u8, candidate.key, first.key)) break;
            }
            const latest_index = self.wal_order.items[end - 1];
            const latest = self.wal.records.items[latest_index];
            const shard: usize = @intCast(latest.key_hash & (@as(u64, by_shard.len) - 1));
            try by_shard[shard].append(self.store.alloc, latest_index);
            pos = end;
        }
    }

    pub fn get(self: *const Opened, key: []const u8, max_source_sequence: u64, expected_revision: ?u64) !vector_block.Lookup {
        return self.getHashed(key, vector_block.keyHash(key), max_source_sequence, expected_revision);
    }

    pub fn getHashed(self: *const Opened, key: []const u8, hash: u64, max_source_sequence: u64, expected_revision: ?u64) !vector_block.Lookup {
        var saw_revision_mismatch = false;
        if (try self.getWalHashed(key, hash, max_source_sequence)) |record| {
            if (expected_revision == null or expected_revision.? == record.revision) {
                return switch (record.kind) {
                    .upsert => .{ .vector = .{
                        .source_sequence = record.source_sequence,
                        .revision = record.revision,
                        .dims = record.dims,
                        .bytes = record.vector_bytes,
                    } },
                    .tombstone => .{ .tombstone = .{ .source_sequence = record.source_sequence, .revision = record.revision } },
                    else => unreachable,
                };
            }
            saw_revision_mismatch = true;
        }
        const manifest = self.store.manifest orelse return .missing;
        const target_shard: u32 = @intCast(hash & (@as(u64, manifest.shard_count) - 1));
        const shard_start = self.shard_offsets[target_shard];
        var order_pos = self.shard_offsets[target_shard + 1];
        while (order_pos > shard_start) {
            order_pos -= 1;
            const reader_index = self.reader_order[order_pos];
            const reader = self.readers[reader_index];
            const found = try reader.getHashed(key, hash, max_source_sequence, null);
            switch (found) {
                .missing => {},
                .vector => |value| {
                    if (expected_revision == null or expected_revision.? == value.revision) return found;
                    saw_revision_mismatch = true;
                },
                .tombstone => |value| {
                    if (expected_revision == null or expected_revision.? == value.revision) return found;
                    saw_revision_mismatch = true;
                },
            }
        }
        if (saw_revision_mismatch) return error.VectorBlockRevisionMismatch;
        return .missing;
    }

    /// Point lookup with mmap metadata and bounded positional payload reads.
    /// Returned vector bytes borrow scratch until the caller's next use.
    pub fn getHashedInto(
        self: *const Opened,
        key: []const u8,
        hash: u64,
        max_source_sequence: u64,
        expected_revision: ?u64,
        scratch: []u8,
    ) !vector_block.Lookup {
        var saw_revision_mismatch = false;
        if (try self.getWalHashed(key, hash, max_source_sequence)) |record| {
            if (expected_revision == null or expected_revision.? == record.revision) {
                return switch (record.kind) {
                    .upsert => .{ .vector = .{
                        .source_sequence = record.source_sequence,
                        .revision = record.revision,
                        .dims = record.dims,
                        .bytes = record.vector_bytes,
                    } },
                    .tombstone => .{ .tombstone = .{ .source_sequence = record.source_sequence, .revision = record.revision } },
                    else => unreachable,
                };
            }
            saw_revision_mismatch = true;
        }
        const manifest = self.store.manifest orelse return .missing;
        const target_shard: u32 = @intCast(hash & (@as(u64, manifest.shard_count) - 1));
        const shard_start = self.shard_offsets[target_shard];
        var order_pos = self.shard_offsets[target_shard + 1];
        while (order_pos > shard_start) {
            order_pos -= 1;
            const reader_index = self.reader_order[order_pos];
            const found = try self.readers[reader_index].locateHashed(key, hash, max_source_sequence, null);
            switch (found) {
                .missing => {},
                .tombstone => |value| {
                    if (expected_revision == null or expected_revision.? == value.revision)
                        return .{ .tombstone = value };
                    saw_revision_mismatch = true;
                },
                .vector => |location| {
                    if (expected_revision != null and expected_revision.? != location.revision) {
                        saw_revision_mismatch = true;
                        continue;
                    }
                    const required = try location.scratchBytes();
                    if (scratch.len < required) return error.BufferTooSmall;
                    const vector_bytes = scratch[0..location.vector_len];
                    const residual_bytes = scratch[location.vector_len..required];
                    try self.blocks[reader_index].readAllAt(vector_bytes, location.vector_offset);
                    if (residual_bytes.len != 0)
                        try self.blocks[reader_index].readAllAt(residual_bytes, location.residual_offset);
                    return .{ .vector = try location.valueFromPayload(vector_bytes, residual_bytes) };
                },
            }
        }
        if (saw_revision_mismatch) return error.VectorBlockRevisionMismatch;
        return .missing;
    }

    /// Resolves a key once without touching either immutable payload plane.
    /// The returned location is valid only while this Opened generation is
    /// retained; read methods reject accidental use with a different reader.
    pub fn locateHashed(
        self: *const Opened,
        key: []const u8,
        hash: u64,
        max_source_sequence: u64,
        expected_revision: ?u64,
    ) !LocatedLookup {
        var saw_revision_mismatch = false;
        if (try self.getWalHashed(key, hash, max_source_sequence)) |record| {
            if (expected_revision == null or expected_revision.? == record.revision) {
                return switch (record.kind) {
                    .upsert => .{ .vector = .{ .wal = .{
                        .source_sequence = record.source_sequence,
                        .revision = record.revision,
                        .dims = record.dims,
                        .bytes = record.vector_bytes,
                    } } },
                    .tombstone => .{ .tombstone = .{
                        .source_sequence = record.source_sequence,
                        .revision = record.revision,
                    } },
                    else => unreachable,
                };
            }
            saw_revision_mismatch = true;
        }
        const manifest = self.store.manifest orelse return .missing;
        const target_shard: u32 = @intCast(hash & (@as(u64, manifest.shard_count) - 1));
        const shard_start = self.shard_offsets[target_shard];
        var order_pos = self.shard_offsets[target_shard + 1];
        while (order_pos > shard_start) {
            order_pos -= 1;
            const reader_index = self.reader_order[order_pos];
            const reader = self.readers[reader_index];
            const found = try reader.locateHashed(key, hash, max_source_sequence, null);
            switch (found) {
                .missing => {},
                .tombstone => |value| {
                    if (expected_revision == null or expected_revision.? == value.revision)
                        return .{ .tombstone = value };
                    saw_revision_mismatch = true;
                },
                .vector => |location| {
                    if (expected_revision != null and expected_revision.? != location.revision) {
                        saw_revision_mismatch = true;
                        continue;
                    }
                    return .{ .vector = .{ .block = .{
                        .reader_index = reader_index,
                        .reader_generation = reader.generation,
                        .reader_shard_id = reader.shard_id,
                        .location = location,
                    } } };
                },
            }
        }
        if (saw_revision_mismatch) return error.VectorBlockRevisionMismatch;
        return .missing;
    }

    /// Reads and verifies only the compact candidate plane.
    pub fn readProjectionInto(
        self: *const Opened,
        located: LocatedValue,
        scratch: []u8,
    ) !vector_block.Value {
        return switch (located) {
            .wal => |value| value,
            .block => |block| blk: {
                const reader = try self.readerForLocated(block.reader_index, block.reader_generation, block.reader_shard_id);
                _ = reader;
                if (scratch.len < block.location.vector_len) return error.BufferTooSmall;
                const vector_bytes = scratch[0..block.location.vector_len];
                try self.blocks[block.reader_index].readAllAt(vector_bytes, block.location.vector_offset);
                break :blk try block.location.projectionValueFromPayload(vector_bytes);
            },
        };
    }

    fn runProjectionRead(self: *const Opened, request: *ProjectionReadRequest) std.Io.Cancelable!void {
        request.value = self.readProjectionInto(request.located, request.scratch) catch |err| {
            request.err = err;
            return;
        };
    }

    /// Starts unrelated immutable payload reads together on Antfly's shared
    /// I/O runtime. Runtimes without concurrent support transparently retain
    /// scalar behavior; cancellation still joins every issued operation before
    /// the caller may release generation-owned descriptors or scratch memory.
    pub fn readProjectionsIntoBatch(
        self: *const Opened,
        io: ?std.Io,
        requests: []ProjectionReadRequest,
    ) !ReadBatchStats {
        try runPositionalReadBatch(ProjectionReadRequest, self, io, requests, runProjectionRead);
        var stats: ReadBatchStats = .{};
        for (requests) |request| switch (request.located) {
            .wal => {},
            .block => |block| {
                stats.physical_reads += 1;
                stats.physical_bytes +|= block.location.vector_len;
            },
        };
        return stats;
    }

    /// Releases clean immutable pages accumulated by a one-pass projection
    /// build while retaining every mmap and generation lease. This is not a
    /// query-cache eviction policy: callers use it only after copying the
    /// requested projection bytes into a different immutable generation.
    pub fn discardResidentPagesAfterMaintenanceScan(self: *const Opened) void {
        for (self.blocks) |block| block.discardResidentPages();
    }

    /// Returns a zero-copy generation-leased compact view for latency-critical
    /// query scoring. Mapped blocks were admitted with MADV_RANDOM, so this
    /// faults only the vector pages selected by the ANN candidate shell and
    /// leaves ordinary host page pressure free to reclaim them.
    pub fn viewProjection(self: *const Opened, located: LocatedValue) !LoadedProjection {
        return switch (located) {
            .wal => |value| .{ .located = located, .value = value },
            .block => |block| blk: {
                _ = try self.readerForLocated(block.reader_index, block.reader_generation, block.reader_shard_id);
                const payload = self.blocks[block.reader_index].bytes();
                if (block.location.vector_offset > payload.len or
                    block.location.vector_len > payload.len - block.location.vector_offset)
                {
                    return error.CorruptedVectorBlock;
                }
                const vector_bytes = payload[block.location.vector_offset..][0..block.location.vector_len];
                break :blk .{
                    .located = located,
                    .value = try block.location.projectionValueFromPayload(vector_bytes),
                };
            },
        };
    }

    /// Binds a projection borrowed from another immutable structure to this
    /// exact-vector generation. Reuse is permitted only when identity lookup,
    /// payload checksum, dimensions, encoding, and quantization metadata all
    /// match the located authoritative record. Callers fall back to reading
    /// the complete exact payload on any mismatch.
    pub fn bindBorrowedProjection(
        self: *const Opened,
        located: LocatedValue,
        bytes: []const u8,
        scale: f32,
        error_norm: f32,
        decoded_norm_lower_bound: f32,
        checksum: u32,
    ) !LoadedProjection {
        return switch (located) {
            .wal => error.UnsupportedBorrowedVectorProjection,
            .block => |block| blk: {
                _ = try self.readerForLocated(block.reader_index, block.reader_generation, block.reader_shard_id);
                const location = block.location;
                if (location.encoding != .float16 or
                    @as(u32, @bitCast(location.scale)) != @as(u32, @bitCast(scale)) or
                    location.quantization_error_norm == null or
                    @as(u32, @bitCast(location.quantization_error_norm.?)) != @as(u32, @bitCast(error_norm)) or
                    location.decoded_norm_lower_bound == null or
                    @as(u32, @bitCast(location.decoded_norm_lower_bound.?)) != @as(u32, @bitCast(decoded_norm_lower_bound)))
                {
                    return error.VectorBlockProjectionLocationMismatch;
                }
                break :blk .{
                    .located = located,
                    .value = try location.projectionValueFromVerifiedPayload(bytes, checksum),
                };
            },
        };
    }

    /// Reconstructs a generation-local residual handle from an authenticated
    /// posting V5 hint. The posting owns the already-verified float16 bytes;
    /// this method binds them only when the exact-vector lease still contains
    /// the named immutable reader and all projection metadata matches. Any
    /// mismatch is non-authoritative and callers fall back to key lookup.
    pub fn bindPersistedResidualLocation(
        self: *const Opened,
        projection_bytes: []const u8,
        scale: f32,
        error_norm: f32,
        decoded_norm_lower_bound: f32,
        projection_checksum: u32,
        source_sequence: u64,
        hint: vectorindex.types.NativeResidualLocation,
    ) !LoadedProjection {
        if (hint.residual_len == 0) return error.VectorBlockProjectionLocationMismatch;
        const index = self.readerIndexForGenerationShard(
            hint.reader_generation,
            hint.reader_shard_id,
        ) orelse return error.VectorBlockLocationGenerationMismatch;
        if (projection_bytes.len % @sizeOf(f16) != 0) return error.VectorBlockProjectionLocationMismatch;
        const projection_dims = projection_bytes.len / @sizeOf(f16);
        const location: vector_block.ValueLocation = .{
            .source_sequence = source_sequence,
            .revision = hint.revision,
            .dims = std.math.cast(u32, projection_dims) orelse return error.VectorBlockProjectionLocationMismatch,
            .encoding = .float16,
            .scale = scale,
            .quantization_error_norm = error_norm,
            .decoded_norm_lower_bound = decoded_norm_lower_bound,
            // The projection payload is owned by the posting generation, so
            // exact completion never reads this offset. Length/checksum still
            // bind it to the shared vector record.
            .vector_offset = 0,
            .vector_len = projection_bytes.len,
            .vector_checksum = projection_checksum,
            .residual_offset = std.math.cast(usize, hint.residual_offset) orelse return error.VectorBlockProjectionLocationMismatch,
            .residual_len = @intCast(hint.residual_len),
            .residual_checksum = hint.residual_checksum,
        };
        return try self.bindBorrowedProjection(
            .{ .block = .{
                .reader_index = index,
                .reader_generation = hint.reader_generation,
                .reader_shard_id = hint.reader_shard_id,
                .location = location,
            } },
            projection_bytes,
            scale,
            error_norm,
            decoded_norm_lower_bound,
            projection_checksum,
        );
    }

    /// Zero-copy authoritative view for callers that did not perform a prior
    /// bounded pass (for example, an ambiguity candidate admitted outside the
    /// original quantized shell).
    pub fn viewExact(self: *const Opened, located: LocatedValue) !vector_block.Value {
        return try self.viewExactFromProjection(try self.viewProjection(located));
    }

    /// Completes a previously validated compact view by touching only its
    /// lossless residual plane. The caller must hold the same Opened lease.
    pub fn viewExactFromProjection(self: *const Opened, projection: LoadedProjection) !vector_block.Value {
        return switch (projection.located) {
            .wal => projection.value,
            .block => |block| blk: {
                _ = try self.readerForLocated(block.reader_index, block.reader_generation, block.reader_shard_id);
                const payload = self.blocks[block.reader_index].bytes();
                if (block.location.residual_offset > payload.len or
                    block.location.residual_len > payload.len - block.location.residual_offset)
                {
                    return error.CorruptedVectorBlock;
                }
                const residual_bytes = if (block.location.residual_len == 0)
                    &.{}
                else
                    payload[block.location.residual_offset..][0..block.location.residual_len];
                break :blk try block.location.completeProjection(projection.value, residual_bytes);
            },
        };
    }

    /// Reads and verifies the authoritative payload for a previously resolved
    /// location. Float16 entries touch the projection and residual planes;
    /// float32/WAL values have no residual plane.
    pub fn readExactInto(
        self: *const Opened,
        located: LocatedValue,
        scratch: []u8,
    ) !vector_block.Value {
        return switch (located) {
            .wal => |value| value,
            .block => |block| blk: {
                const reader = try self.readerForLocated(block.reader_index, block.reader_generation, block.reader_shard_id);
                _ = reader;
                const required = try block.location.scratchBytes();
                if (scratch.len < required) return error.BufferTooSmall;
                const vector_bytes = scratch[0..block.location.vector_len];
                const residual_bytes = scratch[block.location.vector_len..required];
                try self.blocks[block.reader_index].readAllAt(vector_bytes, block.location.vector_offset);
                if (residual_bytes.len != 0)
                    try self.blocks[block.reader_index].readAllAt(residual_bytes, block.location.residual_offset);
                break :blk try block.location.valueFromPayload(vector_bytes, residual_bytes);
            },
        };
    }

    fn runExactRead(self: *const Opened, request: *ExactReadRequest) std.Io.Cancelable!void {
        request.value = self.readExactInto(request.located, request.scratch) catch |err| {
            request.err = err;
            return;
        };
    }

    pub fn readExactIntoBatch(
        self: *const Opened,
        io: ?std.Io,
        requests: []ExactReadRequest,
    ) !ReadBatchStats {
        try runPositionalReadBatch(ExactReadRequest, self, io, requests, runExactRead);
        var stats: ReadBatchStats = .{};
        for (requests) |request| switch (request.located) {
            .wal => {},
            .block => |block| {
                stats.physical_reads += @as(u64, 1) + @intFromBool(block.location.residual_len != 0);
                stats.physical_bytes +|= block.location.vector_len +| block.location.residual_len;
            },
        };
        return stats;
    }

    /// Reads only the exact residual for a compact projection already fetched
    /// and validated by the same query. This preserves positional-I/O RSS
    /// behavior without paying a second projection read at exact completion.
    pub fn readExactResidualInto(
        self: *const Opened,
        projection: LoadedProjection,
        residual_scratch: []u8,
    ) !vector_block.Value {
        return switch (projection.located) {
            .wal => projection.value,
            .block => |block| blk: {
                _ = try self.readerForLocated(block.reader_index, block.reader_generation, block.reader_shard_id);
                if (residual_scratch.len < block.location.residual_len) return error.BufferTooSmall;
                const residual_bytes = residual_scratch[0..block.location.residual_len];
                if (residual_bytes.len != 0)
                    try self.blocks[block.reader_index].readAllAt(residual_bytes, block.location.residual_offset);
                break :blk try block.location.completeProjection(projection.value, residual_bytes);
            },
        };
    }

    fn runResidualRead(self: *const Opened, request: *ResidualReadRequest) std.Io.Cancelable!void {
        request.value = self.readExactResidualInto(request.projection, request.scratch) catch |err| {
            request.err = err;
            return;
        };
    }

    pub fn readExactResidualsIntoBatch(
        self: *const Opened,
        io: ?std.Io,
        requests: []ResidualReadRequest,
    ) !ReadBatchStats {
        try runPositionalReadBatch(ResidualReadRequest, self, io, requests, runResidualRead);
        var stats: ReadBatchStats = .{};
        for (requests) |request| switch (request.projection.located) {
            .wal => {},
            .block => |block| if (block.location.residual_len != 0) {
                stats.physical_reads += 1;
                stats.physical_bytes +|= block.location.residual_len;
            },
        };
        return stats;
    }

    fn readerForLocated(
        self: *const Opened,
        reader_index: usize,
        reader_generation: u64,
        reader_shard_id: u32,
    ) !vector_block.Reader {
        if (reader_index >= self.readers.len or reader_index >= self.blocks.len)
            return error.VectorBlockLocationGenerationMismatch;
        const reader = self.readers[reader_index];
        if (reader.generation != reader_generation or reader.shard_id != reader_shard_id)
            return error.VectorBlockLocationGenerationMismatch;
        return reader;
    }

    /// Resolves an immutable physical location without scanning unrelated
    /// shards. Generations may temporarily retain more than one reader per
    /// shard, so search that shard newest-first just like point lookup.
    fn readerIndexForGenerationShard(
        self: *const Opened,
        reader_generation: u64,
        reader_shard_id: u32,
    ) ?usize {
        const manifest = self.store.manifest orelse return null;
        if (reader_shard_id >= manifest.shard_count) return null;
        const shard: usize = @intCast(reader_shard_id);
        const shard_start = self.shard_offsets[shard];
        var order_pos = self.shard_offsets[shard + 1];
        while (order_pos > shard_start) {
            order_pos -= 1;
            const reader_index = self.reader_order[order_pos];
            const reader = self.readers[reader_index];
            if (reader.generation == reader_generation) return reader_index;
        }
        return null;
    }

    fn getWalHashed(self: *const Opened, key: []const u8, hash: u64, max_source_sequence: u64) !?vector_wal.Record {
        var lo: usize = 0;
        var hi = self.wal_order.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const record = self.wal.records.items[self.wal_order.items[mid]];
            if (compareWalKey(record, hash, key) == .lt) lo = mid + 1 else hi = mid;
        }
        var selected: ?vector_wal.Record = null;
        var pos = lo;
        while (pos < self.wal_order.items.len) : (pos += 1) {
            const record = self.wal.records.items[self.wal_order.items[pos]];
            if (compareWalKey(record, hash, key) != .eq) break;
            if (record.source_sequence > max_source_sequence) break;
            selected = record;
        }
        return selected;
    }
};

fn runPositionalReadBatch(
    comptime Request: type,
    opened: *const Opened,
    maybe_io: ?std.Io,
    requests: []Request,
    comptime run: fn (*const Opened, *Request) std.Io.Cancelable!void,
) !void {
    const io = maybe_io orelse {
        for (requests) |*request| try run(opened, request);
        return;
    };
    if (requests.len < 2) {
        for (requests) |*request| try run(opened, request);
        return;
    }

    var start: usize = 0;
    var concurrency_available = true;
    while (start < requests.len) {
        if (!concurrency_available) {
            for (requests[start..]) |*request| try run(opened, request);
            return;
        }

        const end = @min(requests.len, start + Opened.positional_read_wave);
        var group = std.Io.Group.init;
        var scheduled_end = start;
        while (scheduled_end < end) : (scheduled_end += 1) {
            group.concurrent(io, run, .{ opened, &requests[scheduled_end] }) catch {
                concurrency_available = false;
                break;
            };
        }
        // Await is a cancellation propagation boundary and guarantees all
        // issued reads have stopped touching request memory before returning.
        try group.await(io);
        for (requests[scheduled_end..end]) |*request| try run(opened, request);
        start = end;
    }
}

const CompactionRecord = struct {
    hash: u64,
    key: []const u8,
    source_sequence: u64,
    revision: u64,
    generation: u64,
    payload: union(enum) {
        tombstone,
        block_vector: vector_block.Value,
        wal_vector: vector_wal.Record,
    },

    fn fromBlock(entry: vector_block.EntryView, generation: u64) CompactionRecord {
        return switch (entry.value) {
            .missing => unreachable,
            .tombstone => |value| .{
                .hash = vector_block.keyHash(entry.key),
                .key = entry.key,
                .source_sequence = value.source_sequence,
                .revision = value.revision,
                .generation = generation,
                .payload = .tombstone,
            },
            .vector => |value| .{
                .hash = vector_block.keyHash(entry.key),
                .key = entry.key,
                .source_sequence = value.source_sequence,
                .revision = value.revision,
                .generation = generation,
                .payload = .{ .block_vector = value },
            },
        };
    }

    fn fromWal(record: vector_wal.Record, generation: u64) CompactionRecord {
        return .{
            .hash = record.key_hash,
            .key = record.key,
            .source_sequence = record.source_sequence,
            .revision = record.revision,
            .generation = generation,
            .payload = switch (record.kind) {
                .upsert => .{ .wal_vector = record },
                .tombstone => .tombstone,
                else => unreachable,
            },
        };
    }

    fn sameKey(self: CompactionRecord, other: CompactionRecord) bool {
        return self.hash == other.hash and std.mem.eql(u8, self.key, other.key);
    }

    fn isLive(self: CompactionRecord) bool {
        return switch (self.payload) {
            .tombstone => false,
            .block_vector, .wal_vector => true,
        };
    }

    fn lessThan(_: void, lhs: CompactionRecord, rhs: CompactionRecord) bool {
        if (lhs.hash != rhs.hash) return lhs.hash < rhs.hash;
        const key_order = std.mem.order(u8, lhs.key, rhs.key);
        if (key_order != .eq) return key_order == .lt;
        if (lhs.source_sequence != rhs.source_sequence) return lhs.source_sequence < rhs.source_sequence;
        if (lhs.revision != rhs.revision) return lhs.revision < rhs.revision;
        return lhs.generation < rhs.generation;
    }

    fn appendTo(
        self: CompactionRecord,
        writer: *vector_block.Writer,
        alloc: Allocator,
        scratch: *std.ArrayListUnmanaged(f32),
    ) !void {
        switch (self.payload) {
            .tombstone => try writer.appendTombstone(self.key, self.source_sequence, self.revision),
            .block_vector => |value| {
                try scratch.resize(alloc, value.dims);
                const vector = try value.decodeExactInto(scratch.items);
                try writer.appendVector(self.key, self.source_sequence, self.revision, vector);
            },
            .wal_vector => |record| {
                try scratch.resize(alloc, record.dims);
                const vector = try record.decodeVectorInto(scratch.items);
                try writer.appendVector(self.key, self.source_sequence, self.revision, vector);
            },
        }
    }

    /// Complete-base compaction runs only after the WAL has become immutable
    /// delta blocks. Preserve their target encoding byte-for-byte and omit a
    /// latest tombstone: a replacement base is a complete current snapshot,
    /// so there is no older generation left for that tombstone to mask.
    fn appendLiveBlockVectorTo(
        self: CompactionRecord,
        writer: *vector_block.Writer,
        encoding: vector_block.Encoding,
    ) !void {
        switch (self.payload) {
            .tombstone => {},
            .block_vector => |value| {
                if (value.encoding != encoding) return error.InconsistentVectorBlockEncoding;
                if (value.quantization_error_norm) |error_norm| {
                    const quantization: vector_block.QuantizationStats = .{
                        .error_norm = error_norm,
                        .decoded_norm_lower_bound = value.decoded_norm_lower_bound orelse return error.CorruptedVectorBlock,
                    };
                    if (value.exact_residual) |residual| {
                        try writer.appendEncodedVectorWithStatsAndResidual(
                            self.key,
                            self.source_sequence,
                            self.revision,
                            value.dims,
                            value.bytes,
                            value.scale,
                            quantization,
                            residual,
                        );
                    } else {
                        try writer.appendEncodedVectorWithStats(
                            self.key,
                            self.source_sequence,
                            self.revision,
                            value.dims,
                            value.bytes,
                            value.scale,
                            quantization,
                        );
                    }
                } else {
                    // A v1-v3 block has no persisted quantization metadata.
                    // Compute conservative metadata once while upgrading it;
                    // subsequent queries and compactions remain single-pass.
                    try writer.appendEncodedVector(
                        self.key,
                        self.source_sequence,
                        self.revision,
                        value.dims,
                        value.bytes,
                        value.scale,
                    );
                }
            },
            .wal_vector => return error.VectorWalCheckpointRequired,
        }
    }
};

fn deltaGenerationCount(manifest: vector_manifest.Manifest) usize {
    var count: usize = 0;
    var previous = manifest.base_generation;
    for (manifest.segments[baseSegmentCount(manifest)..]) |segment| {
        if (segment.generation != previous) {
            count += 1;
            previous = segment.generation;
        }
    }
    return count;
}

fn baseSegmentCount(manifest: vector_manifest.Manifest) usize {
    return if (manifest.hasPhysicalBase()) @intCast(manifest.shard_count) else 0;
}

fn openInternal(
    alloc: Allocator,
    storage: lsm_backend.Storage,
    root_dir: []const u8,
    retain_blocks: bool,
    previous: ?*const Opened,
) !Opened {
    const owned_root = try alloc.dupe(u8, root_dir);
    errdefer alloc.free(owned_root);
    try storage.createDirPath(owned_root);
    var store: Store = .{ .alloc = alloc, .storage = storage, .root_dir = owned_root };
    errdefer store.deinit();

    const current_path = try store.currentPathAlloc();
    defer alloc.free(current_path);
    const current = storage.readFileAlloc(alloc, current_path, max_manifest_bytes) catch |err| switch (err) {
        error.FileNotFound => blk: {
            const wal_path = try store.walPathAlloc(1);
            defer alloc.free(wal_path);
            try atomicReplace(alloc, storage, wal_path, &.{});
            break :blk try alloc.alloc(u8, 0);
        },
        else => return err,
    };
    defer alloc.free(current);
    if (current.len != 0) {
        var decoded = try vector_manifest.decodeAlloc(alloc, current);
        store.manifest_segments = decoded.owned_segments;
        decoded.owned_segments = &.{};
        store.manifest_coverages = decoded.owned_coverages;
        decoded.owned_coverages = &.{};
        store.manifest = decoded.manifest;
        store.manifest.?.segments = store.manifest_segments;
        store.manifest.?.coverages = store.manifest_coverages;
        store.wal_generation = store.manifest.?.wal_generation;
        store.wal_committed_bytes = store.manifest.?.wal_committed_bytes;
        store.covered_source_sequence = store.manifest.?.covered_source_sequence;
        store.segment_covered_source_sequence = if (store.manifest_segments.len != 0)
            store.manifest_segments[store.manifest_segments.len - 1].covered_source_sequence
        else
            store.manifest.?.covered_source_sequence;
    }

    const blocks = if (retain_blocks and store.manifest != null)
        try alloc.alloc(RetainedBlock, store.manifest_segments.len)
    else
        try alloc.alloc(RetainedBlock, 0);
    const readers = try alloc.alloc(vector_block.Reader, blocks.len);
    errdefer alloc.free(readers);
    var block_count: usize = 0;
    errdefer {
        for (blocks[0..block_count]) |*block| block.deinit(alloc);
        alloc.free(blocks);
    }
    if (retain_blocks) for (store.manifest_segments) |descriptor| {
        if (previous) |old| {
            if (reusableBlockIndex(old, descriptor)) |old_index| {
                blocks[block_count] = old.blocks[old_index].retain();
                readers[block_count] = old.readers[old_index];
                block_count += 1;
                continue;
            }
        }
        blocks[block_count] = try readBlockRetained(&store, descriptor);
        block_count += 1;
        readers[block_count - 1] = try vector_block.Reader.init(blocks[block_count - 1].bytes());
    };

    const shard_count: usize = if (store.manifest) |manifest| @intCast(manifest.shard_count) else 0;
    const reader_order = try alloc.alloc(usize, readers.len);
    errdefer alloc.free(reader_order);
    const shard_offsets = try alloc.alloc(usize, shard_count + 1);
    errdefer alloc.free(shard_offsets);
    @memset(shard_offsets, 0);
    for (readers) |reader| shard_offsets[@as(usize, reader.shard_id) + 1] += 1;
    for (0..shard_count) |shard| shard_offsets[shard + 1] += shard_offsets[shard];
    const shard_cursors = try alloc.dupe(usize, shard_offsets[0..shard_count]);
    defer alloc.free(shard_cursors);
    for (readers, 0..) |reader, reader_index| {
        const shard: usize = @intCast(reader.shard_id);
        reader_order[shard_cursors[shard]] = reader_index;
        shard_cursors[shard] += 1;
    }

    const wal_path = try store.walPathAlloc(store.wal_generation);
    defer alloc.free(wal_path);
    var wal_bytes = storage.readFileAlloc(alloc, wal_path, max_wal_bytes + 1) catch |err| switch (err) {
        error.FileNotFound => if (store.wal_committed_bytes == 0) try alloc.alloc(u8, 0) else return error.MissingVectorWal,
        else => return err,
    };
    errdefer alloc.free(wal_bytes);
    var wal = try vector_wal.Replay.parse(alloc, wal_bytes);
    errdefer wal.deinit();
    if (wal.committed_bytes < store.wal_committed_bytes) return error.VectorWalShorterThanManifest;
    for (wal.records.items) |record| {
        if (record.kind != .coverage and record.source_sequence < store.segment_covered_source_sequence) return error.VectorWalOverlapsCheckpoint;
        if (record.kind == .upsert or record.kind == .tombstone) {
            store.wal_has_mutations = true;
            store.wal_latest_mutation_sequence = @max(store.wal_latest_mutation_sequence, record.source_sequence);
        }
    }
    if (wal_bytes.len != wal.committed_bytes) {
        const committed = try alloc.dupe(u8, wal_bytes[0..wal.committed_bytes]);
        alloc.free(wal_bytes);
        wal_bytes = committed;
        try atomicReplace(alloc, storage, wal_path, wal_bytes);
        wal.deinit();
        wal = try vector_wal.Replay.parse(alloc, wal_bytes);
    }
    store.wal_committed_bytes = wal.committed_bytes;
    store.last_committed_batch = wal.last_committed_batch;
    store.covered_source_sequence = @max(store.covered_source_sequence, wal.covered_source_sequence);
    var wal_order = std.ArrayListUnmanaged(usize).empty;
    errdefer wal_order.deinit(alloc);
    for (wal.records.items, 0..) |record, index| if (record.kind == .upsert or record.kind == .tombstone) try wal_order.append(alloc, index);
    std.mem.sortUnstable(usize, wal_order.items, &wal, walRecordLessThan);
    return .{
        .store = store,
        .blocks = blocks,
        .readers = readers,
        .reader_order = reader_order,
        .shard_offsets = shard_offsets,
        .wal_bytes = wal_bytes,
        .wal = wal,
        .wal_order = wal_order,
    };
}

const BlockIdentity = struct {
    generation: u64,
    shard_id: u32,
};

fn isManagedArtifactName(name: []const u8) bool {
    return parseWalGeneration(name) != null or parseBlockIdentity(name) != null;
}

fn parseWalGeneration(name: []const u8) ?u64 {
    return parseSingleNumberName(name, "wal-", ".afvw");
}

fn parseBlockIdentity(name: []const u8) ?BlockIdentity {
    const prefix = "block-";
    const suffix = ".afvb";
    if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, suffix)) return null;
    const body = name[prefix.len .. name.len - suffix.len];
    const separator = std.mem.indexOfScalar(u8, body, '-') orelse return null;
    if (separator == 0 or separator + 1 == body.len or std.mem.indexOfScalar(u8, body[separator + 1 ..], '-') != null)
        return null;
    return .{
        .generation = std.fmt.parseInt(u64, body[0..separator], 10) catch return null,
        .shard_id = std.fmt.parseInt(u32, body[separator + 1 ..], 10) catch return null,
    };
}

fn parseSingleNumberName(name: []const u8, prefix: []const u8, suffix: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, name, prefix) or !std.mem.endsWith(u8, name, suffix)) return null;
    const digits = name[prefix.len .. name.len - suffix.len];
    if (digits.len == 0) return null;
    return std.fmt.parseInt(u64, digits, 10) catch null;
}

test "vector block store reclaims crash orphans from CURRENT" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const storage = memory.storage();
    try storage.writeFileAbsolute("/vector-gc/block-99-0.afvb", "orphan");
    try storage.writeFileAbsolute("/vector-gc/wal-99.afvw", "orphan");
    try storage.writeFileAbsolute("/vector-gc/unmanaged", "keep");

    var store = try Store.open(alloc, storage, "/vector-gc");
    defer store.deinit();
    try std.testing.expectEqual(@as(u64, "orphan".len), try storage.fileSize("/vector-gc/block-99-0.afvb"));
    _ = try store.reclaimUnreferencedFiles();
    try std.testing.expectError(error.FileNotFound, storage.fileSize("/vector-gc/block-99-0.afvb"));
    try std.testing.expectError(error.FileNotFound, storage.fileSize("/vector-gc/wal-99.afvw"));
    try std.testing.expectEqual(@as(u64, 0), try storage.fileSize("/vector-gc/wal-1.afvw"));
    try std.testing.expectEqual(@as(u64, "keep".len), try storage.fileSize("/vector-gc/unmanaged"));
}

fn reusableBlockIndex(previous: *const Opened, descriptor: vector_manifest.Segment) ?usize {
    for (previous.readers, 0..) |reader, index| {
        if (reader.generation == descriptor.generation and
            reader.covered_source_sequence == descriptor.covered_source_sequence and
            reader.shard_id == descriptor.shard_id and
            previous.blocks[index].bytes().len == descriptor.bytes and
            reader.admissionChecksum() == descriptor.admission_checksum)
        {
            return index;
        }
    }
    return null;
}

fn walRecordLessThan(replay: *vector_wal.Replay, lhs_index: usize, rhs_index: usize) bool {
    const lhs = replay.records.items[lhs_index];
    const rhs = replay.records.items[rhs_index];
    if (lhs.key_hash != rhs.key_hash) return lhs.key_hash < rhs.key_hash;
    const key_order = std.mem.order(u8, lhs.key, rhs.key);
    if (key_order != .eq) return key_order == .lt;
    if (lhs.source_sequence != rhs.source_sequence) return lhs.source_sequence < rhs.source_sequence;
    if (lhs.revision != rhs.revision) return lhs.revision < rhs.revision;
    return lhs_index < rhs_index;
}

fn compareWalKey(record: vector_wal.Record, hash: u64, key: []const u8) std.math.Order {
    if (record.key_hash != hash) return std.math.order(record.key_hash, hash);
    return std.mem.order(u8, record.key, key);
}

fn stagedDescriptor(staged: StagedBlock) vector_manifest.Segment {
    return .{
        .generation = staged.generation,
        .covered_source_sequence = staged.covered_source_sequence,
        .shard_id = staged.shard_id,
        .bytes = staged.bytes,
        .admission_checksum = staged.admission_checksum,
    };
}

fn noteCoverage(coverages: []vector_manifest.Coverage, key: []const u8) void {
    const scope_hash = internal_keys.embeddingArtifactScopeHash(key) orelse return;
    const key_hash = vector_block.keyHash(key);
    for (coverages) |*coverage| {
        if (coverage.scope_hash < scope_hash) continue;
        if (coverage.scope_hash > scope_hash) return;
        coverage.vector_count +|= 1;
        coverage.key_hash_xor ^= key_hash;
        coverage.key_hash_sum +%= key_hash;
        return;
    }
}

const SpoolEntry = struct {
    hash: u64,
    key: []const u8,
    revision: u64,
    dims: u32,
    vector_bytes: []const u8,
    scale: f32,
    quantization: vector_block.QuantizationStats,
    exact_residual: ?[]const u8,

    fn lessThan(_: void, lhs: SpoolEntry, rhs: SpoolEntry) bool {
        if (lhs.hash != rhs.hash) return lhs.hash < rhs.hash;
        return std.mem.order(u8, lhs.key, rhs.key) == .lt;
    }
};

fn parseSpoolEntries(
    alloc: Allocator,
    bytes: []const u8,
    encoding: vector_block.Encoding,
) !std.ArrayListUnmanaged(SpoolEntry) {
    var entries = std.ArrayListUnmanaged(SpoolEntry).empty;
    errdefer entries.deinit(alloc);
    var pos: usize = 0;
    while (pos < bytes.len) {
        if (bytes.len - pos < 40) return error.CorruptedVectorBlockSpool;
        const hash = std.mem.readInt(u64, bytes[pos..][0..8], .big);
        const key_len: usize = @intCast(std.mem.readInt(u32, bytes[pos + 8 ..][0..4], .big));
        const dims = std.mem.readInt(u32, bytes[pos + 12 ..][0..4], .big);
        const revision = std.mem.readInt(u64, bytes[pos + 16 ..][0..8], .big);
        const scale: f32 = @bitCast(std.mem.readInt(u32, bytes[pos + 24 ..][0..4], .little));
        const quantization: vector_block.QuantizationStats = .{
            .error_norm = @bitCast(std.mem.readInt(u32, bytes[pos + 28 ..][0..4], .little)),
            .decoded_norm_lower_bound = @bitCast(std.mem.readInt(u32, bytes[pos + 32 ..][0..4], .little)),
        };
        const residual_len: usize = @intCast(std.mem.readInt(u32, bytes[pos + 36 ..][0..4], .little));
        if (key_len == 0 or dims == 0) return error.CorruptedVectorBlockSpool;
        const vector_bytes_len = vector_block.encodedVectorBytesLen(encoding, dims) catch return error.CorruptedVectorBlockSpool;
        const record_len = std.math.add(usize, 40 + key_len + vector_bytes_len, residual_len) catch return error.CorruptedVectorBlockSpool;
        if (record_len > bytes.len - pos) return error.CorruptedVectorBlockSpool;
        const key = bytes[pos + 40 ..][0..key_len];
        if (vector_block.keyHash(key) != hash or !std.math.isFinite(scale) or scale <= 0 or
            !std.math.isFinite(quantization.error_norm) or quantization.error_norm < 0 or
            !std.math.isFinite(quantization.decoded_norm_lower_bound) or quantization.decoded_norm_lower_bound < 0)
            return error.CorruptedVectorBlockSpool;
        const vector_bytes = bytes[pos + 40 + key_len ..][0..vector_bytes_len];
        const exact_residual = if (residual_len == 0)
            null
        else
            bytes[pos + 40 + key_len + vector_bytes_len ..][0..residual_len];
        try entries.append(alloc, .{
            .hash = hash,
            .key = key,
            .revision = revision,
            .dims = dims,
            .vector_bytes = vector_bytes,
            .scale = scale,
            .quantization = quantization,
            .exact_residual = exact_residual,
        });
        pos += record_len;
    }
    return entries;
}

fn readBlockRetained(store: *const Store, descriptor: vector_manifest.Segment) !RetainedBlock {
    const path = try store.blockPathAlloc(descriptor.generation, descriptor.shard_id);
    defer store.alloc.free(path);
    if (mapBlockFile(path)) |mapped| {
        if (mapped.bytes.len == descriptor.bytes) {
            if (vector_block.Reader.init(mapped.bytes)) |reader| {
                if (reader.generation == descriptor.generation and reader.shard_id == descriptor.shard_id and
                    reader.covered_source_sequence == descriptor.covered_source_sequence and reader.admissionChecksum() == descriptor.admission_checksum)
                {
                    return RetainedBlock.init(store.alloc, .{ .mapped = mapped }) catch |err| {
                        std.posix.munmap(mapped.bytes);
                        _ = std.posix.system.close(mapped.fd);
                        return err;
                    };
                }
            } else |_| {}
        }
        std.posix.munmap(mapped.bytes);
        _ = std.posix.system.close(mapped.fd);
    } else |_| {}
    const bytes = store.storage.readFileAlloc(store.alloc, path, boundedReadLimit(max_block_bytes)) catch |err| switch (err) {
        error.FileNotFound => return error.MissingVectorBlock,
        else => return err,
    };
    errdefer store.alloc.free(bytes);
    if (bytes.len != descriptor.bytes) return error.VectorBlockDescriptorMismatch;
    const reader = try vector_block.Reader.init(bytes);
    if (reader.generation != descriptor.generation or reader.shard_id != descriptor.shard_id or
        reader.covered_source_sequence != descriptor.covered_source_sequence or reader.admissionChecksum() != descriptor.admission_checksum)
    {
        return error.VectorBlockDescriptorMismatch;
    }
    return try RetainedBlock.init(store.alloc, .{ .heap = bytes });
}

fn mapBlockFile(path: []const u8) !RetainedBlock.MappedPayload {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.UnsupportedPlatform;
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    errdefer _ = std.posix.system.close(fd);
    const size_raw = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
    if (size_raw <= 0) return error.EmptyVectorBlock;
    const size = std.math.cast(usize, size_raw) orelse return error.VectorBlockTooLarge;
    const mapped = try std.posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    // Vector point reads are physically sorted within a request but sparse
    // across the corpus. Disable broad kernel read-ahead so a recall-parity
    // workload does not pull the complete multi-GiB projection into RSS.
    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.RANDOM) catch {};
    return .{ .bytes = mapped, .fd = fd };
}

fn boundedReadLimit(max_bytes: usize) usize {
    return std.math.add(usize, max_bytes, 1) catch std.math.maxInt(usize);
}

fn atomicReplace(alloc: Allocator, storage: lsm_backend.Storage, path: []const u8, contents: []const u8) !void {
    try generation_publication.replaceImmutable(alloc, storage, path, contents);
}

test "vector block store publishes base and replays committed WAL" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/vector-block-store");
    defer store.deinit();

    var writer = try vector_block.Writer.init(alloc, 1, 0, 1, 0);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 0, 1, &.{ 1.0, 2.0 });
    const base = try writer.build();
    defer alloc.free(base);
    store.covered_source_sequence = 0;
    try store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = base }}, true);
    try std.testing.expectEqual(TopologyEpoch{ .base_generation = 1, .wal_mutation_sequence = 0 }, store.topologyEpoch().?);
    {
        var base_opened = try Store.openWithBlocks(alloc, memory.storage(), "/vector-block-store");
        defer base_opened.deinit();
        try std.testing.expectEqual(@as(?u64, 1), base_opened.baseOnlyVectorCount());
    }
    const batch_id = try store.nextBatchId();
    try store.appendBatch(batch_id, &.{.{
        .kind = .upsert,
        .key = "artifact-a",
        .source_sequence = 2,
        .revision = 2,
        .vector = &.{ 3.0, 4.0 },
    }}, 2, .{});
    try std.testing.expectEqual(TopologyEpoch{ .base_generation = 1, .wal_mutation_sequence = 2 }, store.topologyEpoch().?);
    try store.appendCoverage(try store.nextBatchId(), 3, .{});
    try std.testing.expectEqual(TopologyEpoch{ .base_generation = 1, .wal_mutation_sequence = 2 }, store.topologyEpoch().?);
    try std.testing.expect(store.wal_has_mutations);

    // A full replacement cannot change sharding while a committed vector
    // mutation is present in the WAL, even though all replacement shards are
    // otherwise complete.
    var replacement_keys: [2][32]u8 = undefined;
    var replacement_key_lens = [_]usize{ 0, 0 };
    var candidate: usize = 0;
    while (replacement_key_lens[0] == 0 or replacement_key_lens[1] == 0) : (candidate += 1) {
        var candidate_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&candidate_buf, "replacement-{d}", .{candidate});
        const shard = try vector_block.shardForKey(key, 2);
        if (replacement_key_lens[shard] != 0) continue;
        @memcpy(replacement_keys[shard][0..key.len], key);
        replacement_key_lens[shard] = key.len;
    }
    var replacement_blocks: [2][]u8 = undefined;
    var replacement_block_count: usize = 0;
    defer for (replacement_blocks[0..replacement_block_count]) |block| alloc.free(block);
    for (0..2) |shard| {
        var replacement = try vector_block.Writer.init(alloc, 2, @intCast(shard), 2, 3);
        defer replacement.deinit();
        try replacement.appendVector(replacement_keys[shard][0..replacement_key_lens[shard]], 3, 1, &.{ 1.0, 2.0 });
        replacement_blocks[shard] = try replacement.build();
        replacement_block_count += 1;
    }
    try std.testing.expectError(error.InvalidVectorBlockGeneration, store.publishGeneration(2, 3, &.{
        .{ .shard_id = 0, .bytes = replacement_blocks[0] },
        .{ .shard_id = 1, .bytes = replacement_blocks[1] },
    }, true));

    var opened = try Store.openWithBlocks(alloc, memory.storage(), "/vector-block-store");
    defer opened.deinit();
    try std.testing.expectEqual(@as(u64, 3), opened.store.covered_source_sequence);
    try std.testing.expect(opened.store.wal_has_mutations);
    try std.testing.expectEqual(@as(?u64, null), opened.baseOnlyVectorCount());
    try std.testing.expectEqual(TopologyEpoch{ .base_generation = 1, .wal_mutation_sequence = 2 }, opened.store.topologyEpoch().?);
    try std.testing.expectEqualSlices(f32, &.{ 1.0, 2.0 }, (try opened.get("artifact-a", 1, 1)).vector.vectorView().?);
    var lookup_scratch: [64]u8 = undefined;
    try std.testing.expectEqualSlices(
        f32,
        &.{ 1.0, 2.0 },
        (try opened.getHashedInto("artifact-a", vector_block.keyHash("artifact-a"), 1, 1, &lookup_scratch)).vector.vectorView().?,
    );
    const latest = (try opened.get("artifact-a", 2, 2)).vector;
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0 }, latest.vectorView().?);
    try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0 }, (try opened.get("artifact-a", 3, 2)).vector.vectorView().?);
    {
        var reused = try Store.openWithBlocksReusing(alloc, memory.storage(), "/vector-block-store", &opened);
        defer reused.deinit();
        try std.testing.expect(reused.blocks[0].shared == opened.blocks[0].shared);
        try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0 }, (try reused.get("artifact-a", 3, 2)).vector.vectorView().?);
    }
}

test "vector block positional lookup reads mmap payload through retained descriptor" {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi)
        return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var native = try lsm_backend.NativeStorage.init(alloc, .threaded);
    defer native.deinit();
    var root_buf: [256]u8 = undefined;
    const root = try std.fmt.bufPrint(&root_buf, "/tmp/antfly-vector-positional-{d}-{d}", .{
        std.posix.system.getpid(),
        positional_read_test_nonce.fetchAdd(1, .monotonic),
    });
    defer native.storage().deleteTree(root) catch {};

    var store = try Store.open(alloc, native.storage(), root);
    defer store.deinit();
    var writer = try vector_block.Writer.initWithEncoding(alloc, 1, 0, 1, 1, .float16);
    defer writer.deinit();
    const TestEntry = struct {
        key: []const u8,
        revision: u64,
        vector: [3]f32,

        fn lessThan(_: void, lhs: @This(), rhs: @This()) bool {
            const lhs_hash = vector_block.keyHash(lhs.key);
            const rhs_hash = vector_block.keyHash(rhs.key);
            if (lhs_hash != rhs_hash) return lhs_hash < rhs_hash;
            return std.mem.order(u8, lhs.key, rhs.key) == .lt;
        }
    };
    var entries = [_]TestEntry{
        .{ .key = "artifact-a", .revision = 7, .vector = .{ 1.25, -2.5, 3.75 } },
        .{ .key = "artifact-b", .revision = 8, .vector = .{ 4.5, 5.25, -6.75 } },
        .{ .key = "artifact-c", .revision = 9, .vector = .{ -7.125, 8.5, 9.875 } },
    };
    std.mem.sort(TestEntry, &entries, {}, TestEntry.lessThan);
    for (entries) |entry| try writer.appendVector(entry.key, 1, entry.revision, &entry.vector);
    const base = try writer.build();
    defer alloc.free(base);
    store.covered_source_sequence = 1;
    try store.publishGeneration(1, 1, &.{.{ .shard_id = 0, .bytes = base }}, true);

    var opened = try Store.openWithBlocks(alloc, native.storage(), root);
    defer opened.deinit();
    switch (opened.blocks[0].shared.payload) {
        .mapped => {},
        .heap => return error.ExpectedMappedVectorBlock,
    }
    var payload_scratch: [256]u8 = undefined;
    const located = (try opened.locateHashed(
        "artifact-a",
        vector_block.keyHash("artifact-a"),
        1,
        7,
    )).vector;
    try std.testing.expect(located.projectionBytes() > 0);
    try std.testing.expect(located.residualBytes() > 0);
    const projection = try opened.readProjectionInto(located, &payload_scratch);
    try std.testing.expect(projection.exact_residual == null);
    var projection_decoded: [3]f32 = undefined;
    _ = try projection.decodeInto(&projection_decoded);
    try std.testing.expectError(error.ExactVectorResidualMissing, projection.decodeExactInto(&projection_decoded));

    const exact = try opened.readExactInto(located, &payload_scratch);
    var exact_decoded: [3]f32 = undefined;
    try std.testing.expectEqualSlices(f32, &.{ 1.25, -2.5, 3.75 }, try exact.decodeExactInto(&exact_decoded));

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const keys = [_][]const u8{ "artifact-a", "artifact-b", "artifact-c" };
    const revisions = [_]u64{ 7, 8, 9 };
    const expected_vectors = [_][3]f32{
        .{ 1.25, -2.5, 3.75 },
        .{ 4.5, 5.25, -6.75 },
        .{ -7.125, 8.5, 9.875 },
    };
    var batch_scratch: [keys.len][256]u8 = undefined;
    var projection_requests: [keys.len]ProjectionReadRequest = undefined;
    for (keys, revisions, 0..) |key, revision, i| {
        const batch_location = (try opened.locateHashed(
            key,
            vector_block.keyHash(key),
            1,
            revision,
        )).vector;
        projection_requests[i] = .{ .located = batch_location, .scratch = &batch_scratch[i] };
    }
    const projection_read_stats = try opened.readProjectionsIntoBatch(io_impl.io(), &projection_requests);
    try std.testing.expectEqual(@as(u64, keys.len), projection_read_stats.physical_reads);
    var expected_projection_bytes: u64 = 0;
    for (projection_requests) |request| expected_projection_bytes += @intCast(request.located.block.location.vector_len);
    try std.testing.expectEqual(expected_projection_bytes, projection_read_stats.physical_bytes);
    var exact_scratch: [keys.len][256]u8 = undefined;
    var exact_requests: [keys.len]ExactReadRequest = undefined;
    for (projection_requests, 0..) |request, i| {
        exact_requests[i] = .{ .located = request.located, .scratch = &exact_scratch[i] };
    }
    const exact_read_stats = try opened.readExactIntoBatch(io_impl.io(), &exact_requests);
    try std.testing.expectEqual(@as(u64, keys.len * 2), exact_read_stats.physical_reads);
    var expected_exact_bytes: u64 = 0;
    for (exact_requests) |request| {
        expected_exact_bytes += @intCast(
            request.located.block.location.vector_len + request.located.block.location.residual_len,
        );
    }
    try std.testing.expectEqual(expected_exact_bytes, exact_read_stats.physical_bytes);
    for (&exact_requests, expected_vectors) |*request, expected_vector| {
        try std.testing.expect(request.err == null);
        const exact_value = request.value orelse return error.TestExpectedExactVector;
        try std.testing.expectEqualSlices(f32, &expected_vector, try exact_value.decodeExactInto(&exact_decoded));
    }
    var residual_scratch: [keys.len][256]u8 = undefined;
    var residual_requests: [keys.len]ResidualReadRequest = undefined;
    for (&projection_requests, 0..) |*request, i| {
        try std.testing.expect(request.err == null);
        const projection_value = request.value orelse return error.TestExpectedProjection;
        const rebound = try opened.bindBorrowedProjection(
            request.located,
            projection_value.bytes,
            projection_value.scale,
            projection_value.quantization_error_norm.?,
            projection_value.decoded_norm_lower_bound.?,
            request.located.block.location.vector_checksum,
        );
        try std.testing.expectEqual(request.located.block.location.revision, rebound.value.revision);
        try std.testing.expectError(
            error.VectorBlockProjectionLocationMismatch,
            opened.bindBorrowedProjection(
                request.located,
                projection_value.bytes,
                projection_value.scale + 1,
                projection_value.quantization_error_norm.?,
                projection_value.decoded_norm_lower_bound.?,
                request.located.block.location.vector_checksum,
            ),
        );
        try std.testing.expectError(
            error.VectorBlockPayloadChecksumMismatch,
            opened.bindBorrowedProjection(
                request.located,
                projection_value.bytes,
                projection_value.scale,
                projection_value.quantization_error_norm.?,
                projection_value.decoded_norm_lower_bound.?,
                request.located.block.location.vector_checksum ^ 1,
            ),
        );
        const block_location = request.located.block;
        const persisted = try opened.bindPersistedResidualLocation(
            projection_value.bytes,
            projection_value.scale,
            projection_value.quantization_error_norm.?,
            projection_value.decoded_norm_lower_bound.?,
            block_location.location.vector_checksum,
            block_location.location.source_sequence,
            .{
                .reader_generation = block_location.reader_generation,
                .reader_shard_id = block_location.reader_shard_id,
                .revision = block_location.location.revision,
                .residual_offset = @intCast(block_location.location.residual_offset),
                .residual_len = @intCast(block_location.location.residual_len),
                .residual_checksum = block_location.location.residual_checksum,
            },
        );
        try std.testing.expectError(
            error.VectorBlockLocationGenerationMismatch,
            opened.bindPersistedResidualLocation(
                projection_value.bytes,
                projection_value.scale,
                projection_value.quantization_error_norm.?,
                projection_value.decoded_norm_lower_bound.?,
                block_location.location.vector_checksum,
                block_location.location.source_sequence,
                .{
                    .reader_generation = block_location.reader_generation + 1,
                    .reader_shard_id = block_location.reader_shard_id,
                    .revision = block_location.location.revision,
                    .residual_offset = @intCast(block_location.location.residual_offset),
                    .residual_len = @intCast(block_location.location.residual_len),
                    .residual_checksum = block_location.location.residual_checksum,
                },
            ),
        );
        residual_requests[i] = .{
            .projection = persisted,
            .scratch = &residual_scratch[i],
        };
    }
    const residual_read_stats = try opened.readExactResidualsIntoBatch(io_impl.io(), &residual_requests);
    try std.testing.expectEqual(@as(u64, keys.len), residual_read_stats.physical_reads);
    var expected_residual_bytes: u64 = 0;
    for (residual_requests) |request| expected_residual_bytes += @intCast(request.projection.located.block.location.residual_len);
    try std.testing.expectEqual(expected_residual_bytes, residual_read_stats.physical_bytes);
    for (&residual_requests, expected_vectors) |*request, expected_vector| {
        try std.testing.expect(request.err == null);
        const exact_value = request.value orelse return error.TestExpectedExactVector;
        try std.testing.expectEqualSlices(f32, &expected_vector, try exact_value.decodeExactInto(&exact_decoded));
    }

    const mapped_projection = try opened.viewProjection(located);
    const block_location = mapped_projection.located.block.location;
    try std.testing.expectEqual(
        @intFromPtr(opened.blocks[0].bytes().ptr) + block_location.vector_offset,
        @intFromPtr(mapped_projection.value.bytes.ptr),
    );
    try std.testing.expect(mapped_projection.value.exact_residual == null);
    const mapped_exact = try opened.viewExactFromProjection(mapped_projection);
    try std.testing.expectEqual(
        @intFromPtr(opened.blocks[0].bytes().ptr) + block_location.residual_offset,
        @intFromPtr(mapped_exact.exact_residual.?.ptr),
    );
    try std.testing.expectEqualSlices(f32, &.{ 1.25, -2.5, 3.75 }, try mapped_exact.decodeExactInto(&exact_decoded));
    try std.testing.expectError(
        error.BufferTooSmall,
        opened.readProjectionInto(located, payload_scratch[0..1]),
    );
    try std.testing.expectError(
        error.BufferTooSmall,
        opened.readExactInto(located, payload_scratch[0..1]),
    );

    const found = try opened.getHashedInto(
        "artifact-a",
        vector_block.keyHash("artifact-a"),
        1,
        7,
        &payload_scratch,
    );
    var decoded: [3]f32 = undefined;
    try std.testing.expectEqualSlices(f32, &.{ 1.25, -2.5, 3.75 }, try found.vector.decodeExactInto(&decoded));
    try std.testing.expectError(
        error.BufferTooSmall,
        opened.getHashedInto("artifact-a", vector_block.keyHash("artifact-a"), 1, 7, payload_scratch[0..1]),
    );
}

test "vector block store publishes snapshot base with committed WAL suffix" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-snapshot-wal-suffix";
    var store = try Store.open(alloc, memory.storage(), root);
    defer store.deinit();

    var initial_writer = try vector_block.Writer.init(alloc, 1, 0, 1, 0);
    defer initial_writer.deinit();
    try initial_writer.appendVector("artifact-a", 0, 1, &.{1.0});
    const initial = try initial_writer.build();
    defer alloc.free(initial);
    try store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = initial }}, true);

    // This lease represents a query that began before the replacement. It
    // must remain on the old immutable generation after CURRENT advances.
    var old_query = try Store.openWithBlocks(alloc, memory.storage(), root);
    defer old_query.deinit();
    const boundary = store.walPrefixBoundary();

    var replacement_writer = try vector_block.Writer.init(alloc, 2, 0, 1, 0);
    defer replacement_writer.deinit();
    try replacement_writer.appendVector("artifact-a", 0, 2, &.{2.0});
    const replacement = try replacement_writer.build();
    defer alloc.free(replacement);
    const staged = [_]StagedBlock{try store.stageBlock(2, 0, 0, replacement)};

    // This complete batch raced the snapshot encoder. Publication retains its
    // exact byte suffix in the next WAL generation instead of replaying or
    // filtering records by source sequence.
    try store.appendBatch(try store.nextBatchId(), &.{.{
        .kind = .upsert,
        .key = "artifact-a",
        .source_sequence = 1,
        .revision = 3,
        .vector = &.{3.0},
    }}, 1, .{});
    try store.publishStagedBasePreservingWalTail(2, 0, &staged, &.{}, boundary);

    try std.testing.expectEqualSlices(f32, &.{1.0}, (try old_query.get("artifact-a", 0, null)).vector.vectorView().?);
    var current = try Store.openWithBlocks(alloc, memory.storage(), root);
    defer current.deinit();
    try std.testing.expectEqual(@as(u64, 2), current.store.manifest.?.base_generation);
    try std.testing.expectEqual(@as(u64, 1), current.store.covered_source_sequence);
    try std.testing.expect(current.store.wal_has_mutations);
    try std.testing.expectEqualSlices(f32, &.{2.0}, (try current.get("artifact-a", 0, 2)).vector.vectorView().?);
    try std.testing.expectEqualSlices(f32, &.{3.0}, (try current.get("artifact-a", 1, 3)).vector.vectorView().?);
}

test "vector block store authoritative snapshot flattens older WAL prefix" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-snapshot-flatten-prefix";
    var store = try Store.open(alloc, memory.storage(), root);
    defer store.deinit();

    var initial_writer = try vector_block.Writer.init(alloc, 1, 0, 1, 0);
    defer initial_writer.deinit();
    try initial_writer.appendVector("artifact-a", 0, 1, &.{1.0});
    const initial = try initial_writer.build();
    defer alloc.free(initial);
    try store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = initial }}, true);
    try store.appendBatch(try store.nextBatchId(), &.{.{
        .kind = .upsert,
        .key = "artifact-a",
        .source_sequence = 1,
        .revision = 2,
        .vector = &.{2.0},
    }}, 1, .{});

    // A separately pinned authoritative snapshot at sequence two contains the
    // complete older WAL prefix. Publishing at that newer watermark may drop
    // the prefix only when its boundary is the current committed WAL end.
    const flattened = store.walPrefixBoundary();
    var replacement_writer = try vector_block.Writer.init(alloc, 2, 0, 1, 2);
    defer replacement_writer.deinit();
    try replacement_writer.appendVector("artifact-a", 2, 3, &.{3.0});
    const replacement = try replacement_writer.build();
    defer alloc.free(replacement);
    const staged = [_]StagedBlock{try store.stageBlock(2, 2, 0, replacement)};
    try store.publishStagedBasePreservingWalTail(2, 2, &staged, &.{}, flattened);

    var current = try Store.openWithBlocks(alloc, memory.storage(), root);
    defer current.deinit();
    try std.testing.expectEqual(@as(u64, 2), current.store.covered_source_sequence);
    try std.testing.expect(!current.store.wal_has_mutations);
    try std.testing.expectEqual(@as(u64, 0), current.store.wal_committed_bytes);
    try std.testing.expectEqualSlices(f32, &.{3.0}, (try current.get("artifact-a", 2, 3)).vector.vectorView().?);
}

test "owned staged base can reset a restored source epoch" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-reset-source-epoch";
    var store = try Store.open(alloc, memory.storage(), root);
    defer store.deinit();

    var initial_writer = try vector_block.Writer.init(alloc, 1, 0, 1, 0);
    defer initial_writer.deinit();
    try initial_writer.appendVector("artifact-a", 0, 1, &.{0.0});
    const initial = try initial_writer.build();
    defer alloc.free(initial);
    try store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = initial }}, true);
    try store.appendBatch(try store.nextBatchId(), &.{.{
        .kind = .upsert,
        .key = "artifact-a",
        .source_sequence = 6,
        .revision = 6,
        .vector = &.{6.0},
    }}, 6, .{});

    // A normal replacement cannot move behind a committed WAL tail. Restore
    // is different: the complete imported source snapshot establishes a new
    // sequence epoch and must retire every mutation from the old source.
    var replacement_writer = try vector_block.Writer.init(alloc, 2, 0, 1, 1);
    defer replacement_writer.deinit();
    try replacement_writer.appendVector("artifact-a", 1, 1, &.{1.0});
    const replacement = try replacement_writer.build();
    defer alloc.free(replacement);
    const receipt = try store.stageBlock(2, 1, 0, replacement);
    var build: StagedBaseBuild = .{
        .alloc = alloc,
        .storage = memory.storage(),
        .root_dir = try alloc.dupe(u8, root),
        .generation = 2,
        .covered_source_sequence = 1,
        .staged = try alloc.dupe(StagedBlock, &.{receipt}),
        .coverages = try alloc.alloc(vector_manifest.Coverage, 0),
        .stats = .{},
    };
    defer build.deinit();
    try store.publishStagedBaseBuild(&build, .reset_source_epoch);

    var current = try Store.openWithBlocks(alloc, memory.storage(), root);
    defer current.deinit();
    try std.testing.expectEqual(@as(u64, 1), current.store.covered_source_sequence);
    try std.testing.expect(!current.store.wal_has_mutations);
    try std.testing.expectEqual(@as(u64, 0), current.store.wal_committed_bytes);
    try std.testing.expectEqualSlices(f32, &.{1.0}, (try current.get("artifact-a", 1, 1)).vector.vectorView().?);
}

test "vector block store poisons ambiguous CURRENT publication" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/vector-block-ambiguous-current");

    var writer = try vector_block.Writer.init(alloc, 1, 0, 1, 0);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 0, 1, &.{ 1.0, 2.0 });
    const base = try writer.build();
    defer alloc.free(base);

    generation_publication.injectPostPublishFailuresForTest(2);
    try std.testing.expectError(
        error.GenerationPublicationDurabilityUncertain,
        store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = base }}, true),
    );
    try std.testing.expect(store.poisoned);
    try std.testing.expectError(error.VectorBlockStoreRequiresReopen, store.appendCoverage(1, 1, .{}));
    store.deinit();

    store = try Store.open(alloc, memory.storage(), "/vector-block-ambiguous-current");
    defer store.deinit();
    try std.testing.expectEqual(@as(u64, 1), store.manifest.?.base_generation);
    try std.testing.expectEqual(@as(u64, 0), store.covered_source_sequence);
}

test "owned staged base removes blocks after pre-CURRENT rejection" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-staged-rejected";
    var store = try Store.open(alloc, memory.storage(), root);
    defer store.deinit();

    var writer = try vector_block.Writer.init(alloc, 1, 0, 1, 0);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 0, 1, &.{ 1.0, 2.0 });
    const base = try writer.build();
    defer alloc.free(base);
    const receipt = try store.stageBlock(1, 0, 0, base);
    const block_path = try store.blockPathAlloc(1, 0);
    defer alloc.free(block_path);

    var build: StagedBaseBuild = .{
        .alloc = alloc,
        .storage = memory.storage(),
        .root_dir = try alloc.dupe(u8, root),
        .generation = 1,
        .covered_source_sequence = 0,
        .staged = try alloc.dupe(StagedBlock, &.{receipt}),
        .coverages = try alloc.alloc(vector_manifest.Coverage, 0),
        .stats = .{},
    };
    // Receipt validation fails before CURRENT. The owner must remove the
    // durable reservation instead of accumulating an unreachable generation.
    build.staged[0].bytes += 1;
    try std.testing.expectError(error.MissingVectorBlock, store.publishStagedBaseBuild(&build, .no_tail));
    build.deinit();
    try std.testing.expectError(error.FileNotFound, memory.storage().fileSize(block_path));
}

test "owned staged base survives ambiguous CURRENT publication" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-staged-ambiguous";
    var store = try Store.open(alloc, memory.storage(), root);

    var writer = try vector_block.Writer.init(alloc, 1, 0, 1, 0);
    defer writer.deinit();
    try writer.appendVector("artifact-a", 0, 1, &.{ 1.0, 2.0 });
    const base = try writer.build();
    defer alloc.free(base);
    const receipt = try store.stageBlock(1, 0, 0, base);
    const block_path = try store.blockPathAlloc(1, 0);
    defer alloc.free(block_path);

    var build: StagedBaseBuild = .{
        .alloc = alloc,
        .storage = memory.storage(),
        .root_dir = try alloc.dupe(u8, root),
        .generation = 1,
        .covered_source_sequence = 0,
        .staged = try alloc.dupe(StagedBlock, &.{receipt}),
        .coverages = try alloc.alloc(vector_manifest.Coverage, 0),
        .stats = .{},
    };
    generation_publication.injectPostPublishFailuresForTest(2);
    try std.testing.expectError(
        error.GenerationPublicationDurabilityUncertain,
        store.publishStagedBaseBuild(&build, .no_tail),
    );
    try std.testing.expect(store.poisoned);
    build.deinit();
    try std.testing.expect((try memory.storage().fileSize(block_path)) > 0);
    store.deinit();

    store = try Store.open(alloc, memory.storage(), root);
    defer store.deinit();
    try std.testing.expectEqual(@as(u64, 1), store.manifest.?.base_generation);
}

test "vector block store checkpoints and consolidates sparse delta generations" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-delta-checkpoint";
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        var writer = try vector_block.Writer.initWithEncoding(alloc, 1, 0, 1, 0, .float16);
        defer writer.deinit();
        try writer.appendVector("artifact-a", 0, 0, &.{1.0});
        const base = try writer.build();
        defer alloc.free(base);
        try store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = base }}, true);
    }

    // Fill the complete permitted delta chain. Every checkpoint is sparse and
    // leaves the immutable base untouched.
    for (1..vector_manifest.max_online_delta_generations + 1) |step| {
        const sequence: u64 = @intCast(step);
        {
            var store = try Store.open(alloc, memory.storage(), root);
            defer store.deinit();
            const batch_id = try store.nextBatchId();
            try store.appendBatch(batch_id, &.{.{
                .kind = .upsert,
                .key = "artifact-a",
                .source_sequence = sequence,
                .revision = sequence,
                .vector = &.{@floatFromInt(step)},
            }}, sequence, .{});
        }
        {
            var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
            defer opened.deinit();
            try std.testing.expect(try opened.checkpointWalToDelta(true));
            try std.testing.expect(!opened.store.wal_has_mutations);
            try std.testing.expectEqual(@as(usize, step + 1), opened.store.manifest.?.segments.len);
        }
    }

    // The next checkpoint merges all eight old deltas and the WAL into one
    // new delta. It does not rewrite the base and preserves the latest value.
    const final_sequence: u64 = vector_manifest.max_online_delta_generations + 1;
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        const batch_id = try store.nextBatchId();
        try store.appendBatch(batch_id, &.{.{
            .kind = .upsert,
            .key = "artifact-a",
            .source_sequence = final_sequence,
            .revision = final_sequence,
            .vector = &.{@floatFromInt(final_sequence)},
        }}, final_sequence, .{});
    }
    {
        var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer opened.deinit();
        try std.testing.expect(try opened.checkpointWalToDelta(true));
        try std.testing.expectEqual(@as(usize, 2), opened.store.manifest.?.segments.len);
        try std.testing.expectEqual(@as(u64, 1), opened.store.manifest.?.base_generation);
        try std.testing.expect(!opened.store.wal_has_mutations);
    }
    {
        var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer opened.deinit();
        try std.testing.expect(try opened.compactDeltasToBase());
        try std.testing.expectEqual(@as(usize, 1), opened.store.manifest.?.segments.len);
    }
    {
        var recovered = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer recovered.deinit();
        const latest = (try recovered.get("artifact-a", final_sequence, null)).vector;
        try std.testing.expect(latest.quantization_error_norm != null);
        try std.testing.expect(latest.decoded_norm_lower_bound != null);
        try std.testing.expect(latest.exact_residual != null);
        var decoded: [1]f32 = undefined;
        try std.testing.expectEqualSlices(f32, &.{@floatFromInt(final_sequence)}, try latest.decodeExactInto(&decoded));
    }
}

test "vector block store bootstrap checkpoints remain append only past online limit" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-bootstrap-linear-checkpoint";
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        var writer = try vector_block.Writer.initWithEncoding(alloc, 1, 0, 1, 0, .float16);
        defer writer.deinit();
        const base = try writer.build();
        defer alloc.free(base);
        try store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = base }}, true);
    }

    for (1..vector_manifest.max_online_delta_generations + 2) |step| {
        const sequence: u64 = @intCast(step);
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "artifact-{d}", .{step});
        {
            var store = try Store.open(alloc, memory.storage(), root);
            defer store.deinit();
            try store.appendBatch(try store.nextBatchId(), &.{.{
                .kind = .upsert,
                .key = key,
                .source_sequence = sequence,
                .revision = sequence,
                .vector = &.{@floatFromInt(step)},
            }}, sequence, .{});
        }
        {
            var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
            defer opened.deinit();
            try std.testing.expect(try opened.checkpointWalToDelta(true));
        }
    }

    var recovered = try Store.openWithBlocks(alloc, memory.storage(), root);
    defer recovered.deinit();
    try std.testing.expectEqual(
        @as(usize, vector_manifest.max_online_delta_generations + 2),
        recovered.store.manifest.?.segments.len,
    );
    try std.testing.expectEqual(
        @as(usize, vector_manifest.max_online_delta_generations + 1),
        deltaGenerationCount(recovered.store.manifest.?),
    );
    try std.testing.expect(recovered.baseOnlyVectorCount() == null);
    var decoded: [1]f32 = undefined;
    const latest_sequence: u64 = vector_manifest.max_online_delta_generations + 1;
    const latest = (try recovered.get("artifact-9", latest_sequence, latest_sequence)).vector;
    try std.testing.expectEqualSlices(f32, &.{9.0}, try latest.decodeInto(&decoded));

    // Stable-tip consolidation changes the bootstrap topology without
    // consulting the source store. Every destination shard is a complete base
    // member, exact residuals survive the spool, and CURRENT is immediately
    // restart-readable at the new routing fan-out.
    try std.testing.expect(try recovered.compactDeltasToBaseWithShardCount(4, 32));
    try std.testing.expectEqual(@as(u32, 4), recovered.store.manifest.?.shard_count);
    try std.testing.expectEqual(@as(usize, 4), recovered.store.manifest.?.segments.len);
    recovered.deinit();
    recovered = try Store.openWithBlocks(alloc, memory.storage(), root);
    try std.testing.expectEqual(@as(?u64, 9), recovered.baseOnlyVectorCount());
    for (1..10) |step| {
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "artifact-{d}", .{step});
        const value = (try recovered.get(key, latest_sequence, @intCast(step))).vector;
        try std.testing.expect(value.exact_residual != null);
        try std.testing.expectEqualSlices(f32, &.{@as(f32, @floatFromInt(step))}, try value.decodeExactInto(&decoded));
    }
}

test "vector block store complete base compaction omits latest tombstones" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-complete-base-tombstone";
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        var writer = try vector_block.Writer.initWithEncoding(alloc, 1, 0, 1, 0, .float16);
        defer writer.deinit();
        try writer.appendVector("deleted", 0, 1, &.{ 1.0, 2.0 });
        try writer.appendVector("retained", 0, 1, &.{ 3.0, 4.0 });
        const base = try writer.build();
        defer alloc.free(base);
        try store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = base }}, true);
    }
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        const batch_id = try store.nextBatchId();
        try store.appendBatch(batch_id, &.{.{
            .kind = .tombstone,
            .key = "deleted",
            .source_sequence = 2,
            .revision = 2,
        }}, 2, .{});
    }
    {
        var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer opened.deinit();
        try std.testing.expect(try opened.checkpointWalToDelta(true));
    }
    {
        var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer opened.deinit();
        try std.testing.expect(try opened.compactDeltasToBase());
    }
    {
        var recovered = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer recovered.deinit();
        try std.testing.expectEqual(@as(?u64, 1), recovered.baseOnlyVectorCount());
        try std.testing.expect((try recovered.get("deleted", 2, null)) == .missing);
        const retained = (try recovered.get("retained", 2, null)).vector;
        try std.testing.expect(retained.quantization_error_norm != null);
        try std.testing.expect(retained.decoded_norm_lower_bound != null);
        var decoded: [2]f32 = undefined;
        try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0 }, try retained.decodeInto(&decoded));
    }
}

test "vector block store reshard compacts committed WAL directly into exact base" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-direct-wal-reshard";
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        var writer = try vector_block.Writer.initWithEncoding(alloc, 1, 0, 1, 0, .float16);
        defer writer.deinit();
        try writer.appendVector("updated", 0, 1, &.{ 1.25, -2.5 });
        const base = try writer.build();
        defer alloc.free(base);
        try store.publishGeneration(1, 0, &.{.{ .shard_id = 0, .bytes = base }}, true);
    }
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        try store.appendBatch(try store.nextBatchId(), &.{
            .{
                .kind = .upsert,
                .key = "updated",
                .source_sequence = 2,
                .revision = 2,
                .vector = &.{ 1.234567, -9.876543 },
            },
            .{
                .kind = .tombstone,
                .key = "deleted",
                .source_sequence = 2,
                .revision = 2,
            },
            .{
                .kind = .upsert,
                .key = "inserted",
                .source_sequence = 2,
                .revision = 2,
                .vector = &.{ 3.1415927, 2.7182817 },
            },
        }, 2, .{});
    }
    {
        var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer opened.deinit();
        try std.testing.expect(opened.store.wal_has_mutations);
        try std.testing.expect(try opened.compactDeltasToBaseWithShardCount(4, 32));
        try std.testing.expectEqual(@as(u64, 2), opened.store.manifest.?.base_generation);
        try std.testing.expectEqual(@as(u32, 4), opened.store.manifest.?.shard_count);
        try std.testing.expectEqual(@as(usize, 4), opened.store.manifest.?.segments.len);
        try std.testing.expect(!opened.store.wal_has_mutations);
    }
    {
        var recovered = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer recovered.deinit();
        try std.testing.expectEqual(@as(?u64, 2), recovered.baseOnlyVectorCount());
        try std.testing.expect((try recovered.get("deleted", 2, null)) == .missing);
        var decoded: [2]f32 = undefined;
        const updated = (try recovered.get("updated", 2, 2)).vector;
        try std.testing.expect(updated.exact_residual != null);
        try std.testing.expectEqualSlices(f32, &.{ 1.234567, -9.876543 }, try updated.decodeExactInto(&decoded));
        const inserted = (try recovered.get("inserted", 2, 2)).vector;
        try std.testing.expect(inserted.exact_residual != null);
        try std.testing.expectEqualSlices(f32, &.{ 3.1415927, 2.7182817 }, try inserted.decodeExactInto(&decoded));
    }
}

test "vector block store empty authority checkpoints directly in logical shards" {
    const alloc = std.testing.allocator;
    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    const root = "/vector-block-empty-logical-shards";
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        var build: StagedBaseBuild = .{
            .alloc = alloc,
            .storage = memory.storage(),
            .root_dir = try alloc.dupe(u8, root),
            .generation = 1,
            .covered_source_sequence = 0,
            .logical_shard_count = 4,
            .encoding = .float16,
            .staged = try alloc.alloc(StagedBlock, 0),
            .coverages = try alloc.alloc(vector_manifest.Coverage, 0),
            .stats = .{},
        };
        defer build.deinit();
        try store.publishStagedBaseBuild(&build, .no_tail);
    }
    {
        var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer opened.deinit();
        try std.testing.expectEqual(@as(u32, 4), opened.store.manifest.?.shard_count);
        try std.testing.expectEqual(@as(usize, 0), opened.store.manifest.?.segments.len);
        try std.testing.expectEqual(Encoding.float16, opened.baseEncoding().?);
        try std.testing.expectEqual(@as(?u64, 0), opened.baseOnlyVectorCount());
    }
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        try store.appendBatch(try store.nextBatchId(), &.{
            .{
                .kind = .upsert,
                .key = "logical-a",
                .source_sequence = 1,
                .revision = 1,
                .vector = &.{ 1.234567, -9.876543 },
            },
            .{
                .kind = .upsert,
                .key = "logical-b",
                .source_sequence = 1,
                .revision = 1,
                .vector = &.{ 3.1415927, 2.7182817 },
            },
        }, 1, .{});
    }
    {
        var opened = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer opened.deinit();
        try std.testing.expect(try opened.checkpointWalToDelta(true));
        try std.testing.expect(!opened.store.manifest.?.hasPhysicalBase());
        try std.testing.expect(opened.store.manifest.?.segments.len > 0);
        for (opened.store.manifest.?.segments) |segment| {
            try std.testing.expectEqual(@as(u64, 2), segment.generation);
            try std.testing.expect(segment.shard_id < 4);
        }
    }
    {
        var store = try Store.open(alloc, memory.storage(), root);
        defer store.deinit();
        try store.appendBatch(try store.nextBatchId(), &.{.{
            .kind = .upsert,
            .key = "logical-a",
            .source_sequence = 2,
            .revision = 2,
            .vector = &.{ -6.0221406, 9.1093837 },
        }}, 2, .{});
    }
    {
        var recovered = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer recovered.deinit();
        var decoded: [2]f32 = undefined;
        const value = (try recovered.get("logical-a", 2, 2)).vector;
        // The live WAL is already authoritative float32; lossless residuals
        // are created only when it enters the compact float16 base.
        try std.testing.expectEqual(Encoding.float32, value.encoding);
        try std.testing.expectEqualSlices(f32, &.{ -6.0221406, 9.1093837 }, try value.decodeExactInto(&decoded));
        try std.testing.expect(try recovered.compactDeltasToBaseWithShardCount(4, 32));
        try std.testing.expect(recovered.store.manifest.?.hasPhysicalBase());
        try std.testing.expectEqual(@as(usize, 4), recovered.store.manifest.?.segments.len);
        try std.testing.expect(!recovered.store.wal_has_mutations);
    }
    {
        var recovered = try Store.openWithBlocks(alloc, memory.storage(), root);
        defer recovered.deinit();
        try std.testing.expectEqual(@as(?u64, 2), recovered.baseOnlyVectorCount());
        var decoded: [2]f32 = undefined;
        const value = (try recovered.get("logical-b", 2, 1)).vector;
        try std.testing.expectEqualSlices(f32, &.{ 3.1415927, 2.7182817 }, try value.decodeExactInto(&decoded));
    }
}

test "vector block store bulk builder stages bounded shared shards" {
    const alloc = std.testing.allocator;
    const mem_backend = @import("mem_backend.zig");
    const docstore = @import("docstore.zig");
    var primary_backend = mem_backend.Backend.init(alloc, .{});
    defer primary_backend.close();
    const runtime_store = try primary_backend.runtimeStore(alloc, .{});
    var primary = try docstore.DocStore.openRuntime(alloc, runtime_store);
    defer primary.close();

    const key_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc-a", "embedding-v1");
    defer alloc.free(key_a);
    const key_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc-b", "embedding-v1");
    defer alloc.free(key_b);
    const key_c = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc-c", "embedding-v2");
    defer alloc.free(key_c);
    const payload_a = try artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &.{ 1.0, 2.0, 3.0 });
    defer alloc.free(payload_a);
    const payload_b = try artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &.{ 100_000.0, -250_000.0, 6.0 });
    defer alloc.free(payload_b);
    const payload_c = try artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &.{ 4.0, 5.0, 6.0 });
    defer alloc.free(payload_c);
    try primary.put(key_a, payload_a);
    try primary.put(key_b, payload_b);
    try primary.put(key_c, payload_c);
    try primary.put("ordinary-document", "{\"body\":\"not an artifact\"}");

    var memory = lsm_backend.MemoryStorage.init(alloc);
    defer memory.deinit();
    var store = try Store.open(alloc, memory.storage(), "/vector-block-builder");
    defer store.deinit();
    const embedding_v1_scope = internal_keys.embeddingArtifactScopeHashForName("embedding-v1");
    const stats = try store.buildBaseFromArtifacts(&primary, 1, 9, .{
        .shard_count = 4,
        .spool_buffer_bytes = 32,
        .encoding = .float16,
        .artifact_scope_hashes = &.{embedding_v1_scope},
    });
    try std.testing.expectEqual(@as(u64, 2), stats.vectors);
    try std.testing.expectEqual(@as(u64, 24), stats.vector_bytes);

    // An authoritative snapshot may advance an older base directly when its
    // WAL is empty. This is the safe bootstrap/migration replacement path.
    const payload_a2 = try artifact_codec.encodeDenseEmbeddingAlloc(alloc, null, &.{ 7.0, 8.0, 9.0 });
    defer alloc.free(payload_a2);
    try primary.put(key_a, payload_a2);
    try store.appendCoverage(try store.nextBatchId(), 10, .{});
    try std.testing.expect(!store.wal_has_mutations);
    _ = try store.buildBaseFromArtifacts(&primary, 2, 10, .{
        .shard_count = 8,
        .spool_buffer_bytes = 32,
        .encoding = .float16,
        .artifact_scope_hashes = &.{embedding_v1_scope},
    });
    for (0..8) |shard| {
        if (shard < 4) {
            const old_path = try store.blockPathAlloc(1, @intCast(shard));
            defer alloc.free(old_path);
            try std.testing.expectError(error.FileNotFound, memory.storage().fileSize(old_path));
        }
        const current_path = try store.blockPathAlloc(2, @intCast(shard));
        defer alloc.free(current_path);
        try std.testing.expect((try memory.storage().fileSize(current_path)) > 0);
    }

    var opened = try Store.openWithBlocks(alloc, memory.storage(), "/vector-block-builder");
    defer opened.deinit();
    try std.testing.expectEqual(Encoding.float16, opened.baseEncoding().?);
    try std.testing.expectEqual(
        vector_manifest.ScorePrecision.authoritative_float32_with_bounded_float16,
        opened.scorePrecision(),
    );
    try std.testing.expectEqual(@as(?u64, 2), opened.baseOnlyVectorCount());
    try std.testing.expectEqual(@as(u64, 2), opened.baseOnlyCoverage(embedding_v1_scope).?.vector_count);
    try std.testing.expect(opened.baseOnlyCoverage(internal_keys.embeddingArtifactScopeHashForName("embedding-v2")) == null);
    try std.testing.expect((try opened.get(key_c, 10, null)) == .missing);
    const revision_a = std.hash.XxHash64.hash(0, payload_a2);
    const projected = (try opened.get(key_a, 10, revision_a)).vector;
    try std.testing.expect(projected.quantization_error_norm != null);
    try std.testing.expect(projected.decoded_norm_lower_bound != null);
    try std.testing.expect(projected.exact_residual != null);
    var decoded: [3]f32 = undefined;
    try std.testing.expectEqualSlices(f32, &.{ 7.0, 8.0, 9.0 }, try projected.decodeExactInto(&decoded));
    const revision_b = std.hash.XxHash64.hash(0, payload_b);
    const scaled = (try opened.get(key_b, 10, revision_b)).vector;
    try std.testing.expect(scaled.quantization_error_norm != null);
    try std.testing.expect(scaled.decoded_norm_lower_bound != null);
    try std.testing.expect(scaled.exact_residual != null);
    _ = try scaled.decodeExactInto(&decoded);
    try std.testing.expectEqual(@as(u32, @bitCast(@as(f32, 100_000.0))), @as(u32, @bitCast(decoded[0])));
    try std.testing.expectEqual(@as(u32, @bitCast(@as(f32, -250_000.0))), @as(u32, @bitCast(decoded[1])));
    try std.testing.expect((try opened.get("ordinary-document", 9, null)) == .missing);
}
