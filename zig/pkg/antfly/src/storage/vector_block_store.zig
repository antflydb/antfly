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

const current_name = "CURRENT";
// A maximally admitted 1024-shard, 64-delta manifest is roughly 2.7 MiB
// before scoped coverage certificates. Keep the local recovery read bounded
// while permitting every layout accepted by vector_block_manifest.validate.
const max_manifest_bytes: usize = 4 * 1024 * 1024;
const max_wal_bytes: usize = 512 * 1024 * 1024;
const wal_checkpoint_bytes: usize = 64 * 1024 * 1024;
const max_block_bytes: usize = if (@sizeOf(usize) >= 8) 8 * 1024 * 1024 * 1024 else std.math.maxInt(usize);

pub const RetainedBlock = struct {
    shared: *Shared,

    const Payload = union(enum) {
        mapped: []align(std.heap.page_size_min) u8,
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
            .mapped => |bytes_value| bytes_value,
            .heap => |bytes_value| bytes_value,
        };
    }

    /// Release clean pages after a sequential maintenance scan. The immutable
    /// mapping and every generation lease remain valid; a concurrent or later
    /// query can fault the page back without observing different bytes. Heap
    /// test/fallback blocks are allocator demand and must not be discarded.
    fn discardResidentPages(self: RetainedBlock) void {
        switch (self.shared.payload) {
            .mapped => |mapped| std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.DONTNEED) catch {},
            .heap => {},
        }
    }

    pub fn deinit(self: *RetainedBlock, _: Allocator) void {
        const shared = self.shared;
        self.* = undefined;
        if (shared.refs.fetchSub(1, .acq_rel) != 1) return;
        switch (shared.payload) {
            .mapped => |mapped| std.posix.munmap(mapped),
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
};

pub const BaseBuildOptions = struct {
    // Keep both construction phases comfortably below the governed builder
    // slice on memory-constrained nodes. Small per-shard buffers prevent the
    // partition fan-out from retaining hundreds of MiB; 128 shards bound a
    // 1M x 768 float32 spool unit near 24 MiB without doubling cold mmap/open
    // fan-out for every generation.
    shard_count: u32 = 128,
    spool_buffer_bytes: usize = 64 * 1024,
    // The authoritative source artifact and mutation WAL remain float32. The
    // immutable query projection uses IEEE float16 by default to halve ranged
    // read and file-cache demand; its encoding is explicit in every block and
    // old float32 generations remain readable.
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

pub const TopologyEpoch = struct {
    base_generation: u64,
    wal_mutation_sequence: u64,
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
        try atomicReplace(self.alloc, self.storage, path, bytes);
        return .{
            .generation = generation,
            .covered_source_sequence = covered_source_sequence,
            .shard_id = shard_id,
            .shard_count = reader.shard_count,
            .bytes = bytes.len,
            .admission_checksum = reader.admissionChecksum(),
        };
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
        );
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
        );
    }

    fn publishStagedGenerationMode(
        self: *Store,
        generation: u64,
        covered_source_sequence: u64,
        staged: []const StagedBlock,
        mode: PublicationMode,
        replacement_coverages: ?[]const vector_manifest.Coverage,
    ) !void {
        if (self.poisoned) return error.VectorBlockStoreRequiresReopen;
        if (staged.len == 0) return error.EmptyVectorBlockGeneration;
        // A complete authoritative snapshot can replace an older base at a
        // newer source watermark when there is no WAL tail to merge. Once the
        // WAL contains mutations, publication must stay at its exact covered
        // boundary or it could silently drop a committed update.
        const replace_base = mode == .replace_base;
        const authoritative_replacement = replace_base and !self.wal_has_mutations;
        if (!authoritative_replacement and covered_source_sequence != self.covered_source_sequence) return error.InvalidVectorBlockPublicationBoundary;
        const shard_count = staged[0].shard_count;
        if (replace_base and staged.len != shard_count) return error.IncompleteVectorBlockBase;
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
                receipt.shard_count != shard_count or receipt.shard_id >= shard_count)
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

        const base_segment_count: usize = if (self.manifest) |manifest| @intCast(manifest.shard_count) else 0;
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
        const next_wal_generation = std.math.add(u64, self.wal_generation, 1) catch return error.VectorWalGenerationOverflow;
        const next_manifest: vector_manifest.Manifest = .{
            .base_generation = if (replace_base) generation else self.manifest.?.base_generation,
            .latest_generation = generation,
            .wal_generation = next_wal_generation,
            .wal_committed_bytes = 0,
            .covered_source_sequence = covered_source_sequence,
            .shard_count = shard_count,
            .segments = next_segments,
            .coverages = next_coverages,
        };
        try next_manifest.validate();
        const encoded = try next_manifest.encodeAlloc(self.alloc);
        defer self.alloc.free(encoded);
        const next_wal_path = try self.walPathAlloc(next_wal_generation);
        defer self.alloc.free(next_wal_path);
        // Allocate every piece of post-publication cleanup state before the
        // CURRENT commit point. Once CURRENT is replaced, returning an
        // allocator error would report failure for a generation that is
        // already the crash-recovery authority and invite an unsafe retry.
        const previous_wal_path = try self.walPathAlloc(self.wal_generation);
        defer self.alloc.free(previous_wal_path);
        try atomicReplace(self.alloc, self.storage, next_wal_path, &.{});
        const current_path = try self.currentPathAlloc();
        defer self.alloc.free(current_path);
        generation_publication.publishControlFile(self.alloc, self.storage, current_path, encoded) catch |err| {
            self.poisoned = true;
            return err;
        };

        // CURRENT is now the crash-recovery authority. Replacement bases do
        // not reference any prior block, so unlink those generations before
        // releasing their descriptors. Existing POSIX mmap leases remain
        // valid; platforms that cannot unlink a mapped file leave harmless
        // startup-cleanup debt instead of compromising publication.
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
        if (self.manifest_segments.len != 0) self.alloc.free(self.manifest_segments);
        if (self.manifest_coverages.len != 0) self.alloc.free(self.manifest_coverages);
        self.manifest_segments = next_segments;
        self.manifest_coverages = next_coverages;
        self.manifest = next_manifest;
        self.manifest.?.segments = self.manifest_segments;
        self.manifest.?.coverages = self.manifest_coverages;
        self.wal_generation = next_wal_generation;
        self.wal_committed_bytes = 0;
        self.wal_has_mutations = false;
        self.wal_latest_mutation_sequence = 0;
        self.last_committed_batch = null;
        self.covered_source_sequence = covered_source_sequence;
        self.segment_covered_source_sequence = covered_source_sequence;
        self.storage.deleteFileAbsolute(previous_wal_path) catch {};
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
        defer self.alloc.free(coverages);
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
            try atomicReplace(self.alloc, self.storage, spool_paths[shard], &.{});
        }

        var stats: BaseBuildStats = .{};
        const ScanContext = struct {
            alloc: Allocator,
            storage: lsm_backend.Storage,
            buffers: []std.ArrayListUnmanaged(u8),
            paths: []const []u8,
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
                const record_len = std.math.add(usize, 28 + key.len, encoded_bytes) catch return error.VectorBlockTooLarge;
                const hash = vector_block.keyHash(key);
                const shard: usize = @intCast(hash & (@as(u64, ctx.shard_count) - 1));
                var record = try ctx.alloc.alloc(u8, record_len);
                defer ctx.alloc.free(record);
                std.mem.writeInt(u64, record[0..8], hash, .big);
                std.mem.writeInt(u32, record[8..12], @intCast(key.len), .big);
                std.mem.writeInt(u32, record[12..16], dims, .big);
                std.mem.writeInt(u64, record[16..24], std.hash.XxHash64.hash(0, artifact), .big);
                @memcpy(record[28..][0..key.len], key);
                const vector_out = record[28 + key.len ..];
                const scale = try vector_block.encodeVectorInto(ctx.encoding, vector, vector_out);
                std.mem.writeInt(u32, record[24..28], @bitCast(scale), .little);
                try ctx.buffers[shard].appendSlice(ctx.alloc, record);
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
            .shard_count = options.shard_count,
            .flush_bytes = options.spool_buffer_bytes,
            .encoding = options.encoding,
            .stats = &stats,
            .coverages = coverages,
        };
        try doc_store.scanReadTxnWithContext(txn, "", "", .{}, &scan_context, ScanContext.scan);
        for (0..options.shard_count) |shard| {
            try scan_context.flush(shard);
            try self.storage.syncFileContentsAbsolute(spool_paths[shard]);
            // ArrayList.clearRetainingCapacity deliberately made the scan
            // fast, but retaining every shard buffer through the build would
            // add shard_count * spool_buffer_bytes to peak anonymous memory.
            buffers[shard].deinit(self.alloc);
            buffers[shard] = .empty;
        }

        const staged = try self.alloc.alloc(StagedBlock, options.shard_count);
        defer self.alloc.free(staged);
        for (0..options.shard_count) |shard| {
            const spool = try self.storage.readFileAlloc(self.alloc, spool_paths[shard], boundedReadLimit(max_block_bytes));
            defer self.alloc.free(spool);
            var entries = try parseSpoolEntries(self.alloc, spool, options.encoding);
            defer entries.deinit(self.alloc);
            std.mem.sortUnstable(SpoolEntry, entries.items, {}, SpoolEntry.lessThan);
            var writer = try vector_block.Writer.initWithEncoding(
                self.alloc,
                generation,
                @intCast(shard),
                options.shard_count,
                covered_source_sequence,
                options.encoding,
            );
            defer writer.deinit();
            for (entries.items) |entry| {
                try writer.appendEncodedVector(
                    entry.key,
                    covered_source_sequence,
                    entry.revision,
                    entry.dims,
                    entry.vector_bytes,
                    entry.scale,
                );
            }
            const block = try writer.build();
            defer self.alloc.free(block);
            stats.block_bytes += block.len;
            staged[shard] = try self.stageBlock(generation, covered_source_sequence, @intCast(shard), block);
        }
        try self.publishStagedBaseWithCoverage(generation, covered_source_sequence, staged, coverages);
        return stats;
    }

    fn currentPathAlloc(self: *const Store) ![]u8 {
        return try std.fs.path.join(self.alloc, &.{ self.root_dir, current_name });
    }

    fn walPathAlloc(self: *const Store, generation: u64) ![]u8 {
        const name = try std.fmt.allocPrint(self.alloc, "wal-{d}.afvw", .{generation});
        defer self.alloc.free(name);
        return try std.fs.path.join(self.alloc, &.{ self.root_dir, name });
    }

    fn blockPathAlloc(self: *const Store, generation: u64, shard_id: u32) ![]u8 {
        const name = try std.fmt.allocPrint(self.alloc, "block-{d}-{d}.afvb", .{ generation, shard_id });
        defer self.alloc.free(name);
        return try std.fs.path.join(self.alloc, &.{ self.root_dir, name });
    }
};

pub const Opened = struct {
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

    /// Returns an exact O(shards) cardinality certificate when CURRENT is a
    /// complete immutable base plus coverage-only WAL. Sparse delta blocks or
    /// vector-bearing WAL require key reconciliation, so callers must treat
    /// those layouts as having no cheap certificate rather than guessing from
    /// physical entry counts.
    pub fn baseOnlyVectorCount(self: *const Opened) ?u64 {
        const manifest = self.store.manifest orelse return null;
        if (self.store.wal_has_mutations or manifest.segments.len != @as(usize, @intCast(manifest.shard_count))) return null;
        return self.baseVectorCount();
    }

    /// Returns the complete-base certificate for one configured artifact
    /// family. Sparse overlays remain transactionally authoritative but do not
    /// have an additive physical-count interpretation, matching
    /// baseOnlyVectorCount's readiness contract.
    pub fn baseOnlyCoverage(self: *const Opened, scope_hash: u64) ?vector_manifest.Coverage {
        const manifest = self.store.manifest orelse return null;
        if (self.store.wal_has_mutations or manifest.segments.len != @as(usize, @intCast(manifest.shard_count))) return null;
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
        const base_shards: usize = @intCast(manifest.shard_count);
        if (self.readers.len < base_shards) return null;
        var count: u64 = 0;
        for (self.readers[0..base_shards]) |reader| {
            if (reader.generation != manifest.base_generation) return null;
            count = std.math.add(u64, count, reader.count) catch return null;
        }
        return count;
    }

    /// Rewrites the immutable base plus every sparse delta as one complete
    /// base generation. The mutation WAL must first be checkpointed so the
    /// merge reads only immutable, checksummed blocks. Work is shard-local:
    /// at most one output shard and its compact ordering index are resident at
    /// a time, independent of total corpus size.
    pub fn compactDeltasToBase(self: *Opened) !bool {
        if (self.store.wal_has_mutations) return error.VectorWalCheckpointRequired;
        const manifest = self.store.manifest orelse return error.MissingVectorBlockManifest;
        const shard_count: usize = @intCast(manifest.shard_count);
        if (manifest.segments.len == shard_count) return false;

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
                try latest.appendLiveBlockVectorTo(&writer, encoding);
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
        try self.store.publishStagedBaseWithCoverage(
            generation,
            self.store.covered_source_sequence,
            staged[0..staged_count],
            coverages,
        );
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
            const reader = self.readers[self.reader_order[order_pos]];
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
                const vector = try value.decodeInto(scratch.items);
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
                try writer.appendEncodedVector(
                    self.key,
                    self.source_sequence,
                    self.revision,
                    value.dims,
                    value.bytes,
                    value.scale,
                );
            },
            .wal_vector => return error.VectorWalCheckpointRequired,
        }
    }
};

fn deltaGenerationCount(manifest: vector_manifest.Manifest) usize {
    var count: usize = 0;
    var previous = manifest.base_generation;
    for (manifest.segments[@as(usize, @intCast(manifest.shard_count))..]) |segment| {
        if (segment.generation != previous) {
            count += 1;
            previous = segment.generation;
        }
    }
    return count;
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
        const last_segment = store.manifest_segments[store.manifest_segments.len - 1];
        store.segment_covered_source_sequence = last_segment.covered_source_sequence;
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
        if (bytes.len - pos < 28) return error.CorruptedVectorBlockSpool;
        const hash = std.mem.readInt(u64, bytes[pos..][0..8], .big);
        const key_len: usize = @intCast(std.mem.readInt(u32, bytes[pos + 8 ..][0..4], .big));
        const dims = std.mem.readInt(u32, bytes[pos + 12 ..][0..4], .big);
        const revision = std.mem.readInt(u64, bytes[pos + 16 ..][0..8], .big);
        const scale: f32 = @bitCast(std.mem.readInt(u32, bytes[pos + 24 ..][0..4], .little));
        if (key_len == 0 or dims == 0) return error.CorruptedVectorBlockSpool;
        const vector_bytes_len = vector_block.encodedVectorBytesLen(encoding, dims) catch return error.CorruptedVectorBlockSpool;
        const record_len = std.math.add(usize, 28 + key_len, vector_bytes_len) catch return error.CorruptedVectorBlockSpool;
        if (record_len > bytes.len - pos) return error.CorruptedVectorBlockSpool;
        const key = bytes[pos + 28 ..][0..key_len];
        if (vector_block.keyHash(key) != hash or !std.math.isFinite(scale) or scale <= 0) return error.CorruptedVectorBlockSpool;
        const vector_bytes = bytes[pos + 28 + key_len ..][0..vector_bytes_len];
        try entries.append(alloc, .{
            .hash = hash,
            .key = key,
            .revision = revision,
            .dims = dims,
            .vector_bytes = vector_bytes,
            .scale = scale,
        });
        pos += record_len;
    }
    return entries;
}

fn readBlockRetained(store: *const Store, descriptor: vector_manifest.Segment) !RetainedBlock {
    const path = try store.blockPathAlloc(descriptor.generation, descriptor.shard_id);
    defer store.alloc.free(path);
    if (mapBlockFile(path)) |mapped| {
        if (mapped.len == descriptor.bytes) {
            if (vector_block.Reader.init(mapped)) |reader| {
                if (reader.generation == descriptor.generation and reader.shard_id == descriptor.shard_id and
                    reader.covered_source_sequence == descriptor.covered_source_sequence and reader.admissionChecksum() == descriptor.admission_checksum)
                {
                    return RetainedBlock.init(store.alloc, .{ .mapped = mapped }) catch |err| {
                        std.posix.munmap(mapped);
                        return err;
                    };
                }
            } else |_| {}
        }
        std.posix.munmap(mapped);
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

fn mapBlockFile(path: []const u8) ![]align(std.heap.page_size_min) u8 {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) return error.UnsupportedPlatform;
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    defer _ = std.posix.system.close(fd);
    const size_raw = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
    if (size_raw <= 0) return error.EmptyVectorBlock;
    const size = std.math.cast(usize, size_raw) orelse return error.VectorBlockTooLarge;
    return try std.posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
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
        var decoded: [1]f32 = undefined;
        try std.testing.expectEqualSlices(f32, &.{@floatFromInt(final_sequence)}, try latest.decodeInto(&decoded));
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
        var decoded: [2]f32 = undefined;
        try std.testing.expectEqualSlices(f32, &.{ 3.0, 4.0 }, try retained.decodeInto(&decoded));
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
    try std.testing.expectEqual(@as(?u64, 2), opened.baseOnlyVectorCount());
    try std.testing.expectEqual(@as(u64, 2), opened.baseOnlyCoverage(embedding_v1_scope).?.vector_count);
    try std.testing.expect(opened.baseOnlyCoverage(internal_keys.embeddingArtifactScopeHashForName("embedding-v2")) == null);
    try std.testing.expect((try opened.get(key_c, 10, null)) == .missing);
    const revision_a = std.hash.XxHash64.hash(0, payload_a2);
    const projected = (try opened.get(key_a, 10, revision_a)).vector;
    var decoded: [3]f32 = undefined;
    try std.testing.expectEqualSlices(f32, &.{ 7.0, 8.0, 9.0 }, try projected.decodeInto(&decoded));
    const revision_b = std.hash.XxHash64.hash(0, payload_b);
    const scaled = (try opened.get(key_b, 10, revision_b)).vector;
    _ = try scaled.decodeInto(&decoded);
    try std.testing.expectApproxEqRel(@as(f32, 100_000.0), decoded[0], 0.001);
    try std.testing.expectApproxEqRel(@as(f32, -250_000.0), decoded[1], 0.001);
    try std.testing.expect((try opened.get("ordinary-document", 9, null)) == .missing);
}
