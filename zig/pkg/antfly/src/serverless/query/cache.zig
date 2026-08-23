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
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const fs_paths = @import("../../common/fs_paths.zig");
const artifacts_mod = @import("../artifacts/mod.zig");

// v2 invalidates range/block entries written before provider identities were
// pinned across verification and fetch. Full entries remain cheap to rebuild
// and sharing one record format keeps startup reconciliation fail-closed.
const cache_record_magic = "AFQCR002";
const cache_record_digest_len = std.crypto.hash.sha2.Sha256.digest_length;
const cache_record_header_len = cache_record_magic.len + @sizeOf(u64) + cache_record_digest_len;
const abandoned_cache_write_age_ns: i96 = 24 * std.time.ns_per_hour;
const abandoned_cache_write_sweep_interval_ns: u64 = 5 * std.time.ns_per_min;
const cache_write_owner_dir_name = ".query-cache-owners";
const cache_write_owner_lease_suffix = ".lease";
const cache_publication_lease_suffix = ".query-cache-publish.lease";
const cache_coordination_file_name = ".query-cache-coordination.lock";
const cache_coordination_magic = "AFQCC001";
const cache_coordination_state_len = cache_coordination_magic.len + 2 * @sizeOf(u64);
const reserved_cache_write_marker = ".tmp-query-cache-v2-";
const abandoned_cache_write_dir_name = ".query-cache-abandoned";
const abandoned_cache_write_suffix = ".abandoned";

pub const QueryCacheConfig = struct {
    max_bytes: u64 = 0,
    max_payload_bytes: u64 = 0,
};

pub const QueryCacheStats = struct {
    hits: u64 = 0,
    misses: u64 = 0,
    writes: u64 = 0,
    full_hits: u64 = 0,
    full_misses: u64 = 0,
    full_writes: u64 = 0,
    range_hits: u64 = 0,
    range_misses: u64 = 0,
    range_writes: u64 = 0,
    block_hits: u64 = 0,
    block_misses: u64 = 0,
    block_writes: u64 = 0,
    routing_block_hits: u64 = 0,
    routing_block_misses: u64 = 0,
    routing_block_writes: u64 = 0,
    payload_block_hits: u64 = 0,
    payload_block_misses: u64 = 0,
    payload_block_writes: u64 = 0,
    approx_payload_block_hits: u64 = 0,
    approx_payload_block_misses: u64 = 0,
    approx_payload_block_writes: u64 = 0,
    exact_payload_block_hits: u64 = 0,
    exact_payload_block_misses: u64 = 0,
    exact_payload_block_writes: u64 = 0,
    evictions: u64 = 0,
    bypasses: u64 = 0,
    integrity_failures: u64 = 0,
    current_bytes: u64 = 0,
    pinned_bytes: u64 = 0,
    payload_bytes: u64 = 0,
    pinned_block_count: u64 = 0,
    payload_block_count: u64 = 0,
    max_bytes: u64 = 0,
    max_payload_bytes: u64 = 0,
};

const CacheUsage = struct {
    total_bytes: u64 = 0,
    pinned_bytes: u64 = 0,
    payload_bytes: u64 = 0,
    pinned_block_count: u64 = 0,
    payload_block_count: u64 = 0,
};

const CacheReconciliation = struct {
    usage: CacheUsage = .{},
    integrity_failures: u64 = 0,
};

const PayloadBlockClass = enum {
    none,
    approximate,
    exact,
};

const BlockClass = enum {
    routing,
    payload,
};

const CacheWriteLane = enum {
    full,
    range,
    routing_block,
    payload_block,
};

pub const graph_metric_status_header_block_id = "graph-metric-status-header-v1";
pub const graph_metric_control_block_id = "graph-metric-control-v1";
pub const graph_metric_routing_block_id = "graph-metric-routing-v1";
pub const graph_metric_score_block_id = "graph-metric-score-exact";

fn classifyBlockId(block_id: []const u8) BlockClass {
    if (std.mem.eql(u8, block_id, "vector-header")) return .routing;
    if (std.mem.eql(u8, block_id, "vector-table")) return .routing;
    if (std.mem.eql(u8, block_id, "sparse-header")) return .routing;
    if (std.mem.eql(u8, block_id, "sparse-docs")) return .routing;
    if (std.mem.eql(u8, block_id, "sparse-table")) return .routing;
    if (std.mem.eql(u8, block_id, "sparse-terms")) return .routing;
    if (std.mem.eql(u8, block_id, graph_metric_status_header_block_id)) return .routing;
    if (std.mem.eql(u8, block_id, graph_metric_control_block_id)) return .routing;
    if (std.mem.eql(u8, block_id, graph_metric_routing_block_id)) return .routing;
    return .payload;
}

fn classifyPayloadBlockId(block_id: []const u8) PayloadBlockClass {
    if (classifyBlockId(block_id) == .routing) return .none;
    if (std.mem.endsWith(u8, block_id, "-exact")) return .exact;
    if (std.mem.endsWith(u8, block_id, "-quantized")) return .approximate;
    if (std.mem.startsWith(u8, block_id, "sparse-term-") and std.mem.endsWith(u8, block_id, "-postings")) return .approximate;
    return .approximate;
}

fn cachedPayloadBlockClass(file_name: []const u8) PayloadBlockClass {
    if (std.mem.startsWith(u8, file_name, "exact-")) return .exact;
    if (std.mem.startsWith(u8, file_name, "approx-")) return .approximate;
    return .none;
}

const EvictableFile = struct {
    path: []u8,
    size: u64,
    last_access_ns: i128,
    payload_block_class: PayloadBlockClass,
    counts_as_payload_block: bool,
};

const CacheWriteOwnerLease = struct {
    path: []u8,
    file: std.Io.File,
    transferred: bool = false,

    fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.transferred) return;
        var io_impl = threadedIo();
        defer io_impl.deinit();
        self.file.unlock(io_impl.io());
        self.file.close(io_impl.io());
        deleteFilePath(io_impl.io(), self.path) catch {};
        alloc.free(self.path);
        self.* = undefined;
    }
};

const CachePublicationLease = struct {
    path: []u8,
    file: std.Io.File,

    fn release(self: *@This(), alloc: Allocator) void {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        self.file.unlock(io_impl.io());
        self.file.close(io_impl.io());
        alloc.free(self.path);
        self.* = undefined;
    }

    fn releaseAndDeleteUnderCoordinationLock(self: *@This(), alloc: Allocator, io: std.Io) void {
        self.file.unlock(io);
        self.file.close(io);
        deleteFilePath(io, self.path) catch {};
        alloc.free(self.path);
        self.* = undefined;
    }
};

pub const QueryCache = struct {
    alloc: Allocator,
    root_dir: []u8,
    instance_id: u128,
    owner_lease_path: []u8,
    owner_lease_file: std.Io.File,
    coordination_file: std.Io.File,
    observed_coordination_generation: u64,
    next_abandoned_write_sweep_ns: u64,
    cfg: QueryCacheConfig,
    stats_mu: std.atomic.Mutex = .unlocked,
    maintenance_mu: std.atomic.Mutex = .unlocked,
    usage: CacheUsage = .{},
    stats: QueryCacheStats = .{},

    pub fn init(alloc: Allocator, root_dir: []const u8) !QueryCache {
        return try initWithConfig(alloc, root_dir, .{});
    }

    pub fn initWithConfig(alloc: Allocator, root_dir: []const u8, cfg: QueryCacheConfig) !QueryCache {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        const coordination_path = try std.fs.path.join(alloc, &.{ root_dir, cache_coordination_file_name });
        defer alloc.free(coordination_path);
        var coordination_file = try fs_paths.createFilePortable(io_impl.io(), coordination_path, .{
            .read = true,
            .truncate = false,
        });
        var coordination_file_transferred = false;
        errdefer if (!coordination_file_transferred) coordination_file.close(io_impl.io());
        try lockFileExclusiveWithCancellation(coordination_file, io_impl.io(), .none);
        defer coordination_file.unlock(io_impl.io());
        const coordination_generation = try advanceCacheCoordinationGeneration(coordination_file, io_impl.io());
        var instance_id: u128 = undefined;
        io_impl.io().random(std.mem.asBytes(&instance_id));
        var owner_lease = try acquireCacheWriteOwnerLease(alloc, root_dir, instance_id);
        errdefer owner_lease.deinit(alloc);
        const startup_reconciliation = try reconcileCacheOnDisk(alloc, root_dir, instance_id, .none);
        var cache = QueryCache{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
            .instance_id = instance_id,
            .owner_lease_path = owner_lease.path,
            .owner_lease_file = owner_lease.file,
            .coordination_file = coordination_file,
            .observed_coordination_generation = coordination_generation,
            .next_abandoned_write_sweep_ns = platform_time.monotonicNs() +| abandoned_cache_write_sweep_interval_ns,
            .cfg = cfg,
            .stats = .{
                .max_bytes = cfg.max_bytes,
                .max_payload_bytes = cfg.max_payload_bytes,
            },
        };
        owner_lease.transferred = true;
        coordination_file_transferred = true;
        errdefer {
            cache.owner_lease_file.unlock(io_impl.io());
            cache.owner_lease_file.close(io_impl.io());
            deleteFilePath(io_impl.io(), cache.owner_lease_path) catch {};
            cache.alloc.free(cache.owner_lease_path);
            cache.alloc.free(cache.root_dir);
            coordination_file_transferred = false;
        }
        cache.usage = startup_reconciliation.usage;
        cache.stats.integrity_failures = startup_reconciliation.integrity_failures;
        applyUsageToStats(&cache.stats, cache.usage);
        try enforceStartupCapacity(&cache);
        return cache;
    }

    pub fn deinit(self: *QueryCache) void {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        const coordination_locked = blk: {
            lockFileExclusiveWithCancellation(self.coordination_file, io_impl.io(), .none) catch break :blk false;
            break :blk true;
        };
        if (coordination_locked) {
            self.observed_coordination_generation = advanceCacheCoordinationGeneration(self.coordination_file, io_impl.io()) catch self.observed_coordination_generation;
        }
        self.owner_lease_file.unlock(io_impl.io());
        self.owner_lease_file.close(io_impl.io());
        // If the coordination lock itself is unavailable, leave the unlocked
        // lease behind for the next reconciler. Deleting it without the lock
        // would reopen the create-before-lock race this protocol prevents.
        if (coordination_locked) deleteFilePath(io_impl.io(), self.owner_lease_path) catch {};
        if (coordination_locked) self.coordination_file.unlock(io_impl.io());
        self.coordination_file.close(io_impl.io());
        self.alloc.free(self.owner_lease_path);
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn statsSnapshot(self: *QueryCache) QueryCacheStats {
        lockAtomic(&self.stats_mu);
        defer self.stats_mu.unlock();
        return self.stats;
    }

    pub fn getOrFetchAlloc(self: *QueryCache, artifacts: *artifacts_mod.ArtifactStore, artifact_id: []const u8) ![]u8 {
        return try self.getOrFetchAllocWithCancellation(artifacts, artifact_id, .none);
    }

    pub fn getOrFetchAllocWithCancellation(
        self: *QueryCache,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getOrFetchAllocWithCancellationUsingAllocator(self.alloc, artifacts, artifact_id, cancellation);
    }

    pub fn getOrFetchAllocWithCancellationUsingAllocator(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        const checksum = try artifacts_mod.sha256ChecksumFromArtifactId(artifact_id);
        const path = try cachePathAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);

        const cached = readFileAllocWithLimitAndCancellation(result_alloc, path, cacheReadLimit(self), cancellation) catch |err| switch (err) {
            error.FileNotFound => null,
            error.CacheEntryTooLarge => blk: {
                try removeCorruptCacheEntry(self, path, cancellation);
                break :blk null;
            },
            else => return err,
        };
        if (cached) |value| {
            artifacts_mod.validatePayloadSha256WithCancellation(value, checksum, cancellation) catch |err| switch (err) {
                error.Canceled => {
                    result_alloc.free(value);
                    return err;
                },
                else => {
                    result_alloc.free(value);
                    try removeCorruptCacheEntry(self, path, cancellation);
                    const byte_len = try fileSizeForArtifact(result_alloc, artifacts, artifact_id, cancellation);
                    return try self.fetchAndPublishVerifiedArtifact(result_alloc, artifacts, artifact_id, byte_len, checksum, path, cancellation);
                },
            };
            touchFileNow(path) catch {};
            recordFullHit(self);
            return value;
        }

        const byte_len = try fileSizeForArtifact(result_alloc, artifacts, artifact_id, cancellation);
        return try self.fetchAndPublishVerifiedArtifact(result_alloc, artifacts, artifact_id, byte_len, checksum, path, cancellation);
    }

    pub fn getOrFetchVerifiedAllocWithCancellation(
        self: *QueryCache,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getOrFetchVerifiedAllocWithCancellationUsingAllocator(
            self.alloc,
            artifacts,
            artifact_id,
            expected_byte_len,
            expected_checksum,
            cancellation,
        );
    }

    pub fn getOrFetchVerifiedAllocWithCancellationUsingAllocator(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        artifacts_mod.validateSha256ArtifactIdentity(artifact_id, expected_checksum) catch
            return error.ArtifactIntegrityMismatch;
        const expected_len = std.math.cast(usize, expected_byte_len) orelse return error.ArtifactTooLarge;
        const path = try cachePathAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);

        const cached = readFileExactAllocWithCancellation(result_alloc, path, expected_len, cancellation) catch |err| switch (err) {
            error.FileNotFound => null,
            error.CacheEntryCorrupt => blk: {
                try removeCorruptCacheEntry(self, path, cancellation);
                break :blk null;
            },
            else => return err,
        };
        if (cached) |value| {
            artifacts_mod.validatePayloadSha256WithCancellation(value, expected_checksum, cancellation) catch |err| switch (err) {
                error.Canceled => {
                    result_alloc.free(value);
                    return err;
                },
                else => {
                    result_alloc.free(value);
                    try removeCorruptCacheEntry(self, path, cancellation);
                    return try self.fetchAndPublishVerifiedArtifact(
                        result_alloc,
                        artifacts,
                        artifact_id,
                        expected_byte_len,
                        expected_checksum,
                        path,
                        cancellation,
                    );
                },
            };
            touchFileNow(path) catch {};
            recordFullHit(self);
            return value;
        }
        return try self.fetchAndPublishVerifiedArtifact(
            result_alloc,
            artifacts,
            artifact_id,
            expected_byte_len,
            expected_checksum,
            path,
            cancellation,
        );
    }

    fn fetchAndPublishVerifiedArtifact(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        path: []const u8,
        cancellation: CancellationToken,
    ) ![]u8 {
        const contents = try artifacts.getVerifiedAllocWithCancellationUsingAllocator(
            result_alloc,
            artifact_id,
            expected_byte_len,
            expected_checksum,
            cancellation,
        );
        errdefer result_alloc.free(contents);
        const published = try publishCacheEntry(self, path, contents, .full, cancellation);
        recordFullMiss(self, published);
        return contents;
    }

    pub fn getRangeOrFetchAlloc(
        self: *QueryCache,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
    ) ![]u8 {
        return try self.getRangeOrFetchAllocWithCancellation(artifacts, artifact_id, offset, len, .none);
    }

    pub fn getRangeOrFetchAllocWithCancellation(
        self: *QueryCache,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getRangeOrFetchAllocWithCancellationUsingAllocator(self.alloc, artifacts, artifact_id, offset, len, cancellation);
    }

    pub fn getRangeOrFetchAllocWithCancellationUsingAllocator(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getRangeOrFetchImplAlloc(
            result_alloc,
            artifacts,
            artifact_id,
            null,
            offset,
            len,
            cancellation,
        );
    }

    pub fn getVerifiedRangeOrFetchAllocWithCancellationUsingAllocator(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getRangeOrFetchImplAlloc(
            result_alloc,
            artifacts,
            artifact_id,
            .{ .byte_len = expected_byte_len, .checksum = expected_checksum },
            offset,
            len,
            cancellation,
        );
    }

    const ExpectedArtifact = struct {
        byte_len: u64,
        checksum: []const u8,
    };

    fn getRangeOrFetchImplAlloc(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        expected: ?ExpectedArtifact,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        if (expected) |value| try validateExpectedRange(artifact_id, value, offset, len);
        const range_path = try rangeCachePathAlloc(self.alloc, self.root_dir, artifact_id, offset, len);
        defer self.alloc.free(range_path);
        const cached = readVerifiedCacheRecordAllocWithCancellation(result_alloc, range_path, len, cancellation) catch |err| switch (err) {
            error.FileNotFound => null,
            error.CacheEntryCorrupt => blk: {
                try removeCorruptCacheEntry(self, range_path, cancellation);
                break :blk null;
            },
            else => return err,
        };
        if (cached) |value| {
            errdefer result_alloc.free(value);
            touchFileNow(range_path) catch {};
            recordRangeHit(self);
            return value;
        }

        const contents = if (expected) |value|
            try artifacts.getVerifiedRangeAllocWithCancellationUsingAllocator(
                result_alloc,
                artifact_id,
                value.byte_len,
                value.checksum,
                offset,
                len,
                cancellation,
            )
        else
            try artifacts.getRangeAllocWithCancellationUsingAllocator(result_alloc, artifact_id, offset, len, cancellation);
        errdefer result_alloc.free(contents);
        if (contents.len != len) return error.ArtifactIntegrityMismatch;
        const published = try publishCacheEntry(self, range_path, contents, .range, cancellation);
        recordRangeMiss(self, published);
        return contents;
    }

    pub fn getBlockOrFetchRangeAlloc(
        self: *QueryCache,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        block_id: []const u8,
        offset: u64,
        len: usize,
    ) ![]u8 {
        return try self.getBlockOrFetchRangeAllocWithCancellation(artifacts, artifact_id, block_id, offset, len, .none);
    }

    pub fn getBlockOrFetchRangeAllocWithCancellation(
        self: *QueryCache,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        block_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getBlockOrFetchRangeAllocWithCancellationUsingAllocator(self.alloc, artifacts, artifact_id, block_id, offset, len, cancellation);
    }

    pub fn getBlockOrFetchRangeAllocWithCancellationUsingAllocator(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        block_id: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getBlockOrFetchRangeImplAlloc(
            result_alloc,
            artifacts,
            artifact_id,
            block_id,
            null,
            offset,
            len,
            cancellation,
        );
    }

    pub fn getVerifiedBlockOrFetchRangeAllocWithCancellationUsingAllocator(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        block_id: []const u8,
        expected_byte_len: u64,
        expected_checksum: []const u8,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        return try self.getBlockOrFetchRangeImplAlloc(
            result_alloc,
            artifacts,
            artifact_id,
            block_id,
            .{ .byte_len = expected_byte_len, .checksum = expected_checksum },
            offset,
            len,
            cancellation,
        );
    }

    fn getBlockOrFetchRangeImplAlloc(
        self: *QueryCache,
        result_alloc: Allocator,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        block_id: []const u8,
        expected: ?ExpectedArtifact,
        offset: u64,
        len: usize,
        cancellation: CancellationToken,
    ) ![]u8 {
        try cancellation.check();
        if (expected) |value| try validateExpectedRange(artifact_id, value, offset, len);
        const block_class = classifyBlockId(block_id);
        const payload_block_class = classifyPayloadBlockId(block_id);
        const block_path = try blockCachePathAlloc(self.alloc, self.root_dir, artifact_id, block_id, offset, len, block_class);
        defer self.alloc.free(block_path);
        const cached = readVerifiedCacheRecordAllocWithCancellation(result_alloc, block_path, len, cancellation) catch |err| switch (err) {
            error.FileNotFound => null,
            error.CacheEntryCorrupt => blk: {
                try removeCorruptCacheEntry(self, block_path, cancellation);
                break :blk null;
            },
            else => return err,
        };
        if (cached) |value| {
            errdefer result_alloc.free(value);
            touchFileNow(block_path) catch {};
            recordBlockHit(self, block_class, payload_block_class);
            return value;
        }

        const contents = if (expected) |value|
            try artifacts.getVerifiedRangeAllocWithCancellationUsingAllocator(
                result_alloc,
                artifact_id,
                value.byte_len,
                value.checksum,
                offset,
                len,
                cancellation,
            )
        else
            try artifacts.getRangeAllocWithCancellationUsingAllocator(result_alloc, artifact_id, offset, len, cancellation);
        errdefer result_alloc.free(contents);
        if (contents.len != len) return error.ArtifactIntegrityMismatch;
        const published = try publishCacheEntry(self, block_path, contents, switch (block_class) {
            .routing => .routing_block,
            .payload => .payload_block,
        }, cancellation);
        recordBlockMiss(self, block_class, payload_block_class, published);
        return contents;
    }
};

fn validateExpectedRange(artifact_id: []const u8, expected: QueryCache.ExpectedArtifact, offset: u64, len: usize) !void {
    artifacts_mod.validateSha256ArtifactIdentity(artifact_id, expected.checksum) catch return error.ArtifactIntegrityMismatch;
    const end = std.math.add(u64, offset, std.math.cast(u64, len) orelse return error.InvalidRange) catch return error.InvalidRange;
    if (end > expected.byte_len) return error.InvalidRange;
}

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn lockFileExclusiveWithCancellation(
    file: std.Io.File,
    io: std.Io,
    cancellation: CancellationToken,
) !void {
    var attempts: usize = 0;
    while (!try file.tryLock(io, .exclusive)) : (attempts += 1) {
        try cancellation.check();
        if (attempts < 64) {
            std.atomic.spinLoopHint();
        } else if (attempts < 256) {
            std.Thread.yield() catch {};
        } else {
            platform_time.sleepNs(std.time.ns_per_ms);
        }
    }
    errdefer file.unlock(io);
    try cancellation.check();
}

fn readCacheCoordinationGeneration(file: std.Io.File, io: std.Io) !?u64 {
    const stat = try file.stat(io);
    if (stat.size != cache_coordination_state_len) return null;
    var encoded: [cache_coordination_state_len]u8 = undefined;
    if (try file.readPositionalAll(io, &encoded, 0) != encoded.len) return null;
    if (!std.mem.eql(u8, encoded[0..cache_coordination_magic.len], cache_coordination_magic)) return null;
    const generation = std.mem.readInt(u64, encoded[cache_coordination_magic.len..][0..8], .little);
    const complement = std.mem.readInt(u64, encoded[cache_coordination_magic.len + 8 ..][0..8], .little);
    if (complement != ~generation) return null;
    return generation;
}

/// Advances the shared mutation token before changing cache files. A torn or
/// missing token is intentionally treated as unknown by the next writer, which
/// forces a complete reconciliation rather than trusting stale local usage.
fn advanceCacheCoordinationGeneration(file: std.Io.File, io: std.Io) !u64 {
    const previous = (try readCacheCoordinationGeneration(file, io)) orelse 0;
    const generation = previous +% 1;
    var encoded: [cache_coordination_state_len]u8 = undefined;
    @memcpy(encoded[0..cache_coordination_magic.len], cache_coordination_magic);
    std.mem.writeInt(u64, encoded[cache_coordination_magic.len..][0..8], generation, .little);
    std.mem.writeInt(u64, encoded[cache_coordination_magic.len + 8 ..][0..8], ~generation, .little);
    try file.setLength(io, encoded.len);
    try file.writePositionalAll(io, &encoded, 0);
    return generation;
}

fn synchronizeCacheUsageUnderCoordinationLock(
    self: *QueryCache,
    io: std.Io,
    cancellation: CancellationToken,
    force: bool,
) !void {
    const shared_generation = try readCacheCoordinationGeneration(self.coordination_file, io);
    if (!force and shared_generation != null and shared_generation.? == self.observed_coordination_generation) return;
    const reconciliation_generation = try advanceCacheCoordinationGeneration(self.coordination_file, io);
    const reconciliation = try reconcileCacheOnDisk(self.alloc, self.root_dir, self.instance_id, cancellation);
    self.usage = reconciliation.usage;
    self.observed_coordination_generation = reconciliation_generation;
    recordReconciliation(self, reconciliation.integrity_failures);
}

fn cacheWriteOwnerLeasePathAlloc(alloc: Allocator, root_dir: []const u8, instance_id: u128) ![]u8 {
    const file_name = try std.fmt.allocPrint(alloc, "{x}{s}", .{ instance_id, cache_write_owner_lease_suffix });
    defer alloc.free(file_name);
    return try std.fs.path.join(alloc, &.{ root_dir, cache_write_owner_dir_name, file_name });
}

fn acquireCacheWriteOwnerLease(alloc: Allocator, root_dir: []const u8, instance_id: u128) !CacheWriteOwnerLease {
    const owner_dir = try std.fs.path.join(alloc, &.{ root_dir, cache_write_owner_dir_name });
    defer alloc.free(owner_dir);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    try fs_paths.createDirPathPortable(io, owner_dir);
    const path = try cacheWriteOwnerLeasePathAlloc(alloc, root_dir, instance_id);
    errdefer alloc.free(path);
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true, .exclusive = true });
    errdefer deleteFilePath(io, path) catch {};
    errdefer file.close(io);
    try file.lock(io, .exclusive);
    return .{ .path = path, .file = file };
}

fn cachePathAlloc(alloc: Allocator, root_dir: []const u8, artifact_id: []const u8) ![]u8 {
    const checksum = try artifacts_mod.sha256ChecksumFromArtifactId(artifact_id);
    return try std.fs.path.join(alloc, &.{ root_dir, checksum });
}

fn rangeCachePathAlloc(alloc: Allocator, root_dir: []const u8, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
    const checksum = try artifacts_mod.sha256ChecksumFromArtifactId(artifact_id);
    const suffix = try std.fmt.allocPrint(alloc, "{d}-{d}.range", .{ offset, len });
    defer alloc.free(suffix);
    return try std.fs.path.join(alloc, &.{ root_dir, ".ranges", checksum, suffix });
}

fn blockCachePathAlloc(
    alloc: Allocator,
    root_dir: []const u8,
    artifact_id: []const u8,
    block_id: []const u8,
    offset: u64,
    len: usize,
    block_class: BlockClass,
) ![]u8 {
    const checksum = try artifacts_mod.sha256ChecksumFromArtifactId(artifact_id);
    const lane = switch (block_class) {
        .routing => ".blocks-pinned",
        .payload => ".blocks",
    };
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update(block_id);
    var coordinates: [@sizeOf(u64) * 2]u8 = undefined;
    std.mem.writeInt(u64, coordinates[0..8], offset, .little);
    std.mem.writeInt(u64, coordinates[8..16], std.math.cast(u64, len) orelse return error.CacheEntryTooLarge, .little);
    hasher.update(&coordinates);
    hasher.final(&digest);
    const digest_hex = std.fmt.bytesToHex(digest, .lower);
    const class_prefix = switch (classifyPayloadBlockId(block_id)) {
        .none => "routing",
        .approximate => "approx",
        .exact => "exact",
    };
    const safe_block_id = try std.fmt.allocPrint(alloc, "{s}-{s}", .{ class_prefix, digest_hex });
    defer alloc.free(safe_block_id);
    return try std.fs.path.join(alloc, &.{ root_dir, lane, checksum, safe_block_id });
}

fn readFileAllocWithLimitAndCancellation(alloc: Allocator, path: []const u8, limit: usize, cancellation: CancellationToken) ![]u8 {
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const file = try openFilePath(io_impl.io(), path);
    defer file.close(io_impl.io());
    const stat = try file.stat(io_impl.io());
    if (stat.size > limit) return error.CacheEntryTooLarge;
    const expected_len = std.math.cast(usize, stat.size) orelse return error.CacheEntryTooLarge;
    return (try readFileExactAllocWithCancellation(alloc, path, expected_len, cancellation)) orelse
        return error.InvalidRange;
}

fn readFileExactAllocWithCancellation(
    alloc: Allocator,
    path: []const u8,
    expected_len: usize,
    cancellation: CancellationToken,
) !?[]u8 {
    const payload = (try readFileRangeAllocWithCancellation(alloc, path, 0, expected_len, cancellation)) orelse
        return error.CacheEntryCorrupt;
    errdefer alloc.free(payload);
    if (payload.len != expected_len) return error.CacheEntryCorrupt;

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const file = try openFilePath(io_impl.io(), path);
    defer file.close(io_impl.io());
    const stat = try file.stat(io_impl.io());
    if (stat.size != expected_len) return error.CacheEntryCorrupt;
    return payload;
}

fn readVerifiedCacheRecordAllocWithCancellation(
    alloc: Allocator,
    path: []const u8,
    expected_payload_len: usize,
    cancellation: CancellationToken,
) ![]u8 {
    try cancellation.check();
    const expected_file_len = storedCacheEntryBytes(expected_payload_len, .range) catch return error.CacheEntryCorrupt;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const file = try openFilePath(io, path);
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.size != expected_file_len) return error.CacheEntryCorrupt;

    var header: [cache_record_header_len]u8 = undefined;
    if (try file.readPositionalAll(io, &header, 0) != header.len) return error.CacheEntryCorrupt;
    if (!std.mem.eql(u8, header[0..cache_record_magic.len], cache_record_magic)) return error.CacheEntryCorrupt;
    const declared_len = std.mem.readInt(u64, header[cache_record_magic.len..][0..8], .little);
    const expected_payload_len_u64 = std.math.cast(u64, expected_payload_len) orelse return error.CacheEntryCorrupt;
    if (declared_len != expected_payload_len_u64) return error.CacheEntryCorrupt;
    const expected_digest = header[cache_record_magic.len + @sizeOf(u64) ..][0..cache_record_digest_len];

    const payload = try alloc.alloc(u8, expected_payload_len);
    errdefer alloc.free(payload);
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    const cancellation_chunk_bytes = 1024 * 1024;
    var copied: usize = 0;
    while (copied < payload.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, payload.len - copied);
        const chunk = payload[copied..][0..chunk_len];
        if (try file.readPositionalAll(io, chunk, @intCast(cache_record_header_len + copied)) != chunk.len) {
            return error.CacheEntryCorrupt;
        }
        hasher.update(chunk);
        copied += chunk.len;
    }
    var actual_digest: [cache_record_digest_len]u8 = undefined;
    hasher.final(&actual_digest);
    if (!std.mem.eql(u8, &actual_digest, expected_digest)) return error.CacheEntryCorrupt;
    try cancellation.check();
    return payload;
}

/// Returns null when the file exists but the requested offset is beyond its
/// current end. Range callers can then fall through to their authoritative
/// source without allocating the entire cached artifact.
fn readFileRangeAllocWithCancellation(
    alloc: Allocator,
    path: []const u8,
    offset: u64,
    len: usize,
    cancellation: CancellationToken,
) !?[]u8 {
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    const file = try openFilePath(io, path);
    defer file.close(io);
    const stat = try file.stat(io);
    if (offset > stat.size) return null;
    const available = stat.size - offset;
    const output_len = std.math.cast(usize, @min(available, len)) orelse return error.CacheEntryTooLarge;
    const out = try alloc.alloc(u8, output_len);
    errdefer alloc.free(out);
    const cancellation_chunk_bytes = 1024 * 1024;
    var copied: usize = 0;
    while (copied < out.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, out.len - copied);
        const chunk = out[copied..][0..chunk_len];
        if (try file.readPositionalAll(io, chunk, offset + copied) != chunk.len) return error.ShortCacheRead;
        copied += chunk.len;
    }
    try cancellation.check();
    return out;
}

fn cacheReadLimit(self: *const QueryCache) usize {
    if (self.cfg.max_bytes == 0) return std.math.maxInt(usize);
    return std.math.cast(usize, self.cfg.max_bytes) orelse std.math.maxInt(usize);
}

fn fileSizeForArtifact(
    result_alloc: Allocator,
    artifacts: *artifacts_mod.ArtifactStore,
    artifact_id: []const u8,
    cancellation: CancellationToken,
) !u64 {
    var metadata = try artifacts.statWithCancellationUsingAllocator(result_alloc, artifact_id, cancellation);
    defer metadata.deinit(result_alloc);
    const checksum = try artifacts_mod.sha256ChecksumFromArtifactId(artifact_id);
    if (!std.mem.eql(u8, metadata.artifact_id, artifact_id) or
        !std.mem.eql(u8, metadata.checksum, checksum))
    {
        return error.ArtifactIntegrityMismatch;
    }
    return metadata.byte_len;
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), parent);
}

var nonce: std.atomic.Value(u64) = .init(0);

fn reserveTempFile(
    alloc: Allocator,
    path: []const u8,
    instance_id: u128,
    reserved_bytes: u64,
) ![]u8 {
    try ensureParentDir(path);
    const tmp_path = try std.fmt.allocPrint(
        alloc,
        "{s}{s}{x}-{d}-{d}",
        .{ path, reserved_cache_write_marker, instance_id, nonce.fetchAdd(1, .monotonic), reserved_bytes },
    );
    errdefer alloc.free(tmp_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    errdefer deleteFilePath(io, tmp_path) catch {};
    var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true, .exclusive = true });
    file.close(io);
    return tmp_path;
}

fn writeReservedTempFileWithCancellation(
    path: []const u8,
    contents: []const u8,
    lane: CacheWriteLane,
    cancellation: CancellationToken,
) !void {
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    {
        var file = if (std.fs.path.isAbsolute(path))
            try std.Io.Dir.openFileAbsolute(io, path, .{ .mode = .read_write })
        else
            try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
        defer file.close(io);
        try file.lock(io, .exclusive);
        defer file.unlock(io);
        try file.setLength(io, 0);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        if (usesVerifiedCacheRecord(lane)) {
            var header: [cache_record_header_len]u8 = undefined;
            @memcpy(header[0..cache_record_magic.len], cache_record_magic);
            std.mem.writeInt(u64, header[cache_record_magic.len..][0..8], @intCast(contents.len), .little);
            var digest: [cache_record_digest_len]u8 = undefined;
            try sha256DigestWithCancellation(contents, &digest, cancellation);
            @memcpy(header[cache_record_magic.len + @sizeOf(u64) ..], &digest);
            try writer.interface.writeAll(&header);
        }
        const cancellation_chunk_bytes = 1024 * 1024;
        var written: usize = 0;
        while (written < contents.len) {
            try cancellation.check();
            const chunk_len = @min(cancellation_chunk_bytes, contents.len - written);
            try writer.interface.writeAll(contents[written..][0..chunk_len]);
            written += chunk_len;
        }
        try writer.end();
    }
    try cancellation.check();
}

fn usesVerifiedCacheRecord(lane: CacheWriteLane) bool {
    return lane != .full;
}

fn storedCacheEntryBytes(payload_len: usize, lane: CacheWriteLane) !u64 {
    const payload_bytes = std.math.cast(u64, payload_len) orelse return error.CacheEntryTooLarge;
    if (!usesVerifiedCacheRecord(lane)) return payload_bytes;
    return std.math.add(u64, payload_bytes, cache_record_header_len) catch return error.CacheEntryTooLarge;
}

fn sha256DigestWithCancellation(
    contents: []const u8,
    digest: *[cache_record_digest_len]u8,
    cancellation: CancellationToken,
) !void {
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    const cancellation_chunk_bytes = 1024 * 1024;
    var offset: usize = 0;
    while (offset < contents.len) {
        try cancellation.check();
        const chunk_len = @min(cancellation_chunk_bytes, contents.len - offset);
        hasher.update(contents[offset..][0..chunk_len]);
        offset += chunk_len;
    }
    hasher.final(digest);
    try cancellation.check();
}

fn publishCacheEntry(
    self: *QueryCache,
    path: []const u8,
    contents: []const u8,
    lane: CacheWriteLane,
    cancellation: CancellationToken,
) !bool {
    const incoming_bytes = try storedCacheEntryBytes(contents.len, lane);
    if (!entryFitsEmptyCache(self.cfg, incoming_bytes, lane)) {
        recordBypass(self);
        return false;
    }
    var publication_lease: ?CachePublicationLease = null;
    defer if (publication_lease) |*lease| lease.release(self.alloc);
    var tmp_path: []u8 = undefined;
    {
        try lockAtomicWithCancellation(&self.maintenance_mu, cancellation);
        defer self.maintenance_mu.unlock();
        var coordination_io = threadedIo();
        defer coordination_io.deinit();
        const io = coordination_io.io();
        try lockFileExclusiveWithCancellation(self.coordination_file, io, cancellation);
        defer self.coordination_file.unlock(io);

        const now_ns = platform_time.monotonicNs();
        const periodic_reconcile_due = now_ns >= self.next_abandoned_write_sweep_ns;
        try synchronizeCacheUsageUnderCoordinationLock(self, io, cancellation, periodic_reconcile_due);
        if (periodic_reconcile_due) {
            self.next_abandoned_write_sweep_ns = now_ns +| abandoned_cache_write_sweep_interval_ns;
        }
        if (fileExists(path)) return false;
        try reapAbandonedCacheWritesUnderCoordinationLock(self, io, cancellation);
        // The caller already owns the fetched bytes, so waiting for another
        // publisher would only add tail latency. Treat its durable reservation
        // as a per-key publication lease and let this caller return its bytes
        // without consuming budget or evicting unrelated entries.
        publication_lease = (try tryAcquireCachePublicationLease(self.alloc, io, path)) orelse return false;

        // Publish the mutation token before eviction or reservation creation.
        // A process crash at any later instruction forces every overlapping
        // writer to reconcile the filesystem before trusting local usage.
        self.observed_coordination_generation = try advanceCacheCoordinationGeneration(self.coordination_file, io);
        if (!try ensureCapacityForWrite(self, incoming_bytes, lane, cancellation)) {
            recordBypass(self);
            publication_lease.?.releaseAndDeleteUnderCoordinationLock(self.alloc, io);
            publication_lease = null;
            return false;
        }
        tmp_path = try reserveTempFile(self.alloc, path, self.instance_id, incoming_bytes);
        addUsage(&self.usage, incoming_bytes, lane);
        recordUsage(self);
    }
    defer self.alloc.free(tmp_path);
    var reservation_active = true;
    defer if (reservation_active) discardCacheReservationBestEffort(self, tmp_path, incoming_bytes, lane);

    try writeReservedTempFileWithCancellation(tmp_path, contents, lane, cancellation);
    try lockAtomicWithCancellation(&self.maintenance_mu, cancellation);
    defer self.maintenance_mu.unlock();
    var coordination_io = threadedIo();
    defer coordination_io.deinit();
    const io = coordination_io.io();
    try lockFileExclusiveWithCancellation(self.coordination_file, io, cancellation);
    defer self.coordination_file.unlock(io);
    try synchronizeCacheUsageUnderCoordinationLock(self, io, cancellation, false);
    self.observed_coordination_generation = try advanceCacheCoordinationGeneration(self.coordination_file, io);
    if (fileExists(path)) {
        deleteFilePath(io, tmp_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
        subtractUsage(&self.usage, incoming_bytes, lane);
        recordUsage(self);
        reservation_active = false;
        publication_lease.?.releaseAndDeleteUnderCoordinationLock(self.alloc, io);
        publication_lease = null;
        return false;
    }
    try cancellation.check();
    try renameFilePath(io, tmp_path, path);
    recordUsage(self);
    reservation_active = false;
    publication_lease.?.releaseAndDeleteUnderCoordinationLock(self.alloc, io);
    publication_lease = null;
    return true;
}

fn tryAcquireCachePublicationLease(alloc: Allocator, io: std.Io, path: []const u8) !?CachePublicationLease {
    try ensureParentDir(path);
    const lease_path = try std.fmt.allocPrint(alloc, "{s}{s}", .{ path, cache_publication_lease_suffix });
    errdefer alloc.free(lease_path);
    var file = try fs_paths.createFilePortable(io, lease_path, .{
        .read = true,
        .truncate = false,
    });
    errdefer file.close(io);
    const locked = try file.tryLock(io, .exclusive);
    if (!locked) {
        file.close(io);
        alloc.free(lease_path);
        return null;
    }
    return .{ .path = lease_path, .file = file };
}

fn discardCacheReservationBestEffort(
    self: *QueryCache,
    tmp_path: []const u8,
    reserved_bytes: u64,
    lane: CacheWriteLane,
) void {
    var abandoned_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const abandoned_path = abandonedCacheWritePath(
        &abandoned_path_buf,
        self.root_dir,
        tmp_path,
    ) catch return;
    ensureParentDir(abandoned_path) catch return;
    var handoff_io = threadedIo();
    defer handoff_io.deinit();
    renameFilePath(handoff_io.io(), tmp_path, abandoned_path) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return,
    };

    // Request teardown must never wait behind cache maintenance or another
    // process. The atomic rename above is the durable handoff; this fast path
    // reclaims immediately only when both locks are already available.
    if (!self.maintenance_mu.tryLock()) return;
    defer self.maintenance_mu.unlock();
    var coordination_io = threadedIo();
    defer coordination_io.deinit();
    const io = coordination_io.io();
    const coordination_locked = self.coordination_file.tryLock(io, .exclusive) catch return;
    if (!coordination_locked) return;
    defer self.coordination_file.unlock(io);
    // Keep cancellation teardown O(1): publish a dirty generation, remove this
    // exact handoff, and adjust the conservative local snapshot. Intentionally
    // leave observed_coordination_generation unchanged so the next ordinary
    // operation reconciles any concurrent process mutations.
    _ = advanceCacheCoordinationGeneration(self.coordination_file, io) catch return;
    deleteFilePath(io, abandoned_path) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return,
    };
    subtractUsage(&self.usage, reserved_bytes, lane);
    recordUsage(self);
}

fn abandonedCacheWritePath(buf: []u8, root_dir: []const u8, tmp_path: []const u8) ![]u8 {
    const tmp_name = std.fs.path.basename(tmp_path);
    const marker_index = std.mem.lastIndexOf(u8, tmp_name, reserved_cache_write_marker) orelse
        return error.InvalidCacheReservation;
    const separator = if (std.mem.endsWith(u8, root_dir, std.fs.path.sep_str)) "" else std.fs.path.sep_str;
    return try std.fmt.bufPrint(
        buf,
        "{s}{s}{s}{s}{s}{s}",
        .{
            root_dir,
            separator,
            abandoned_cache_write_dir_name,
            std.fs.path.sep_str,
            tmp_name[marker_index..],
            abandoned_cache_write_suffix,
        },
    );
}

fn isAbandonedCacheWrite(path: []const u8) bool {
    return std.mem.endsWith(u8, path, abandoned_cache_write_suffix) and
        std.mem.indexOf(u8, path, reserved_cache_write_marker) != null;
}

fn abandonedCacheWritesExist(alloc: Allocator, io: std.Io, root_dir: []const u8) !bool {
    const path = try std.fs.path.join(alloc, &.{ root_dir, abandoned_cache_write_dir_name });
    defer alloc.free(path);
    var dir = (if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true })
    else
        std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true })) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer dir.close(io);
    var iter = dir.iterateAssumeFirstIteration();
    while (try iter.next(io)) |entry| {
        if (entry.kind == .file and isAbandonedCacheWrite(entry.name)) return true;
    }
    return false;
}

fn reapAbandonedCacheWritesUnderCoordinationLock(
    self: *QueryCache,
    io: std.Io,
    cancellation: CancellationToken,
) !void {
    if (!try abandonedCacheWritesExist(self.alloc, io, self.root_dir)) return;
    // Publish the mutation before deleting durable handoffs. Only publish the
    // observed generation after reconciliation succeeds, so an I/O failure
    // forces the next operation to rescan instead of trusting stale counters.
    const mutation_generation = try advanceCacheCoordinationGeneration(self.coordination_file, io);
    const reconciliation = try reconcileCacheOnDisk(self.alloc, self.root_dir, self.instance_id, cancellation);
    self.usage = reconciliation.usage;
    self.observed_coordination_generation = mutation_generation;
    recordReconciliation(self, reconciliation.integrity_failures);
}

fn touchFileNow(path: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var file = try openFilePath(io_impl.io(), path);
    defer file.close(io_impl.io());
    try file.setTimestampsNow(io_impl.io());
}

fn setFileModifyTimestamp(path: []const u8, ts_ns: i96) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var file = try openFilePath(io_impl.io(), path);
    defer file.close(io_impl.io());
    const ts = std.Io.Timestamp.fromNanoseconds(ts_ns);
    try file.setTimestamps(io_impl.io(), .{
        .access_timestamp = .{ .new = ts },
        .modify_timestamp = .{ .new = ts },
    });
}

fn ensureCapacityForWrite(
    self: *QueryCache,
    incoming_bytes: u64,
    lane: CacheWriteLane,
    cancellation: CancellationToken,
) !bool {
    if (!entryFitsEmptyCache(self.cfg, incoming_bytes, lane)) return false;
    if (!needsEviction(self.cfg, self.usage, incoming_bytes, lane)) return true;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var evicted_any = false;
    const payload_files = try collectEvictablePayloadFilesAlloc(self.alloc, self.root_dir, cancellation);
    defer freeEvictableFiles(self.alloc, payload_files);
    std.mem.sort(EvictableFile, payload_files, {}, lessEvictableFile);

    for (payload_files) |file| {
        try cancellation.check();
        if (!needsEviction(self.cfg, self.usage, incoming_bytes, lane)) break;
        deleteFilePath(io_impl.io(), file.path) catch continue;
        self.usage.total_bytes -= @min(self.usage.total_bytes, file.size);
        self.usage.payload_bytes -= @min(self.usage.payload_bytes, file.size);
        if (file.counts_as_payload_block) {
            self.usage.payload_block_count -= @min(self.usage.payload_block_count, 1);
        }
        evicted_any = true;
    }

    if (needsEviction(self.cfg, self.usage, incoming_bytes, lane)) {
        const root_files = try collectEvictableRootFilesAlloc(self.alloc, self.root_dir, cancellation);
        defer freeEvictableFiles(self.alloc, root_files);
        std.mem.sort(EvictableFile, root_files, {}, lessEvictableFile);
        for (root_files) |file| {
            try cancellation.check();
            if (!needsEviction(self.cfg, self.usage, incoming_bytes, lane)) break;
            deleteFilePath(io_impl.io(), file.path) catch continue;
            self.usage.total_bytes -= @min(self.usage.total_bytes, file.size);
            self.usage.payload_bytes -= @min(self.usage.payload_bytes, file.size);
            evicted_any = true;
        }
    }

    if (evicted_any) recordEviction(self);
    recordUsage(self);
    return !needsEviction(self.cfg, self.usage, incoming_bytes, lane);
}

fn enforceStartupCapacity(self: *QueryCache) !void {
    if (!needsEviction(self.cfg, self.usage, 0, .full)) return;
    _ = try ensureCapacityForWrite(self, 0, .full, .none);
    if (self.cfg.max_bytes > 0 and self.usage.total_bytes > self.cfg.max_bytes) {
        try evictPinnedFilesToBudget(self);
    }
    if (needsEviction(self.cfg, self.usage, 0, .full)) {
        return error.QueryCacheBudgetCannotBeEnforced;
    }
    recordUsage(self);
}

fn evictPinnedFilesToBudget(self: *QueryCache) !void {
    const files = try collectEvictablePinnedFilesAlloc(self.alloc, self.root_dir, .none);
    defer freeEvictableFiles(self.alloc, files);
    std.mem.sort(EvictableFile, files, {}, lessEvictableFile);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var evicted_any = false;
    for (files) |file| {
        if (self.cfg.max_bytes == 0 or self.usage.total_bytes <= self.cfg.max_bytes) break;
        deleteFilePath(io_impl.io(), file.path) catch continue;
        self.usage.total_bytes -= @min(self.usage.total_bytes, file.size);
        self.usage.pinned_bytes -= @min(self.usage.pinned_bytes, file.size);
        self.usage.pinned_block_count -= @min(self.usage.pinned_block_count, 1);
        evicted_any = true;
    }
    if (evicted_any) recordEviction(self);
}

fn needsEviction(cfg: QueryCacheConfig, usage: CacheUsage, incoming_bytes: u64, lane: CacheWriteLane) bool {
    if (cfg.max_bytes > 0 and
        (usage.total_bytes > cfg.max_bytes or incoming_bytes > cfg.max_bytes - usage.total_bytes)) return true;
    if (cfg.max_payload_bytes > 0 and
        consumesPayloadBudget(lane) and
        (usage.payload_bytes > cfg.max_payload_bytes or incoming_bytes > cfg.max_payload_bytes - usage.payload_bytes)) return true;
    return false;
}

fn entryFitsEmptyCache(cfg: QueryCacheConfig, incoming_bytes: u64, lane: CacheWriteLane) bool {
    if (cfg.max_bytes > 0 and incoming_bytes > cfg.max_bytes) return false;
    if (cfg.max_payload_bytes > 0 and consumesPayloadBudget(lane) and incoming_bytes > cfg.max_payload_bytes) return false;
    return true;
}

fn consumesPayloadBudget(lane: CacheWriteLane) bool {
    return switch (lane) {
        .routing_block => false,
        .full, .range, .payload_block => true,
    };
}

fn currentBytesNoLock(alloc: Allocator, root_dir: []const u8) !u64 {
    return (try cacheUsageNoLock(alloc, root_dir, .none)).total_bytes;
}

fn cacheUsageNoLock(
    alloc: Allocator,
    root_dir: []const u8,
    cancellation: CancellationToken,
) !CacheUsage {
    var usage = CacheUsage{};
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return usage,
        else => return err,
    };
    defer dir.close(io_impl.io());

    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        try cancellation.check();
        if (entry.kind != .file) continue;
        if (isQueryCacheControlPath(entry.path)) continue;
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        if (isTemporaryCachePath(entry.path)) {
            addScannedFileUsage(&usage, entry.path, temporaryCacheAccountedBytes(entry.path, stat.size));
            continue;
        }
        addScannedFileUsage(&usage, entry.path, stat.size);
    }
    return usage;
}

const TemporaryCacheWriteDisposition = enum {
    removed,
    current_owner,
    retained,
};

const CacheWriteOwnerState = enum {
    live,
    dead,
    unknown,
};

/// Reconciles physical disk usage and recoverable cache state. Owner leases
/// allow a crashed writer's staging files to be reclaimed immediately without
/// racing an overlapping rolling-restart writer. Legacy staging files without
/// a lease retain the age-and-file-lock fallback and count against the physical
/// cache budget until they are safe to remove.
fn reconcileCacheOnDisk(
    alloc: Allocator,
    root_dir: []const u8,
    current_instance_id: u128,
    cancellation: CancellationToken,
) !CacheReconciliation {
    var reconciliation = CacheReconciliation{};
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var dir = std.Io.Dir.cwd().openDir(io, root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return reconciliation,
        else => return err,
    };
    defer dir.close(io);
    const cutoff_ns = std.Io.Timestamp.now(io, .real).toNanoseconds() - abandoned_cache_write_age_ns;
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io)) |entry| {
        try cancellation.check();
        if (entry.kind != .file) continue;
        if (isCachePublicationLeasePath(entry.path)) {
            _ = try removeUnlockedCacheWrite(io, dir, entry.path, null);
            continue;
        }
        if (isQueryCacheControlPath(entry.path)) continue;
        const stat = dir.statFile(io, entry.path, .{}) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        if (isTemporaryCachePath(entry.path)) {
            const disposition = try reconcileTemporaryCacheWrite(
                alloc,
                io,
                dir,
                root_dir,
                entry.path,
                current_instance_id,
                stat.mtime.toNanoseconds(),
                cutoff_ns,
            );
            if (disposition != .removed) {
                addScannedFileUsage(&reconciliation.usage, entry.path, temporaryCacheAccountedBytes(entry.path, stat.size));
            }
            continue;
        }
        if (usesVerifiedCacheRecordPath(entry.path) and
            !try cacheRecordFramingValid(io, dir, entry.path, stat.size))
        {
            const removed = blk: {
                dir.deleteFile(io, entry.path) catch |err| switch (err) {
                    error.FileNotFound => break :blk false,
                    else => return err,
                };
                break :blk true;
            };
            if (removed) {
                reconciliation.integrity_failures +|= 1;
            }
            continue;
        }
        addScannedFileUsage(&reconciliation.usage, entry.path, stat.size);
    }
    try pruneDeadCacheWriteOwnerLeases(alloc, root_dir, current_instance_id, cancellation);
    return reconciliation;
}

fn maybeReconcileCacheOnDisk(self: *QueryCache, cancellation: CancellationToken) !void {
    const now_ns = platform_time.monotonicNs();
    if (now_ns < self.next_abandoned_write_sweep_ns) return;
    try lockAtomicWithCancellation(&self.maintenance_mu, cancellation);
    defer self.maintenance_mu.unlock();
    var coordination_io = threadedIo();
    defer coordination_io.deinit();
    const io = coordination_io.io();
    try lockFileExclusiveWithCancellation(self.coordination_file, io, cancellation);
    defer self.coordination_file.unlock(io);
    try synchronizeCacheUsageUnderCoordinationLock(self, io, cancellation, true);
    self.next_abandoned_write_sweep_ns = now_ns +| abandoned_cache_write_sweep_interval_ns;
}

fn reconcileTemporaryCacheWrite(
    alloc: Allocator,
    io: std.Io,
    dir: std.Io.Dir,
    root_dir: []const u8,
    path: []const u8,
    current_instance_id: u128,
    observed_mtime_ns: i128,
    cutoff_ns: i128,
) !TemporaryCacheWriteDisposition {
    if (isAbandonedCacheWrite(path)) return try removeUnlockedCacheWrite(io, dir, path, null);
    if (temporaryCacheWriteOwnerId(path)) |owner_id| {
        if (owner_id == current_instance_id) return .current_owner;
        switch (try cacheWriteOwnerState(alloc, io, root_dir, owner_id)) {
            .live => return .retained,
            .dead => return try removeUnlockedCacheWrite(io, dir, path, null),
            .unknown => {},
        }
    }
    if (observed_mtime_ns > cutoff_ns) return .retained;
    return try removeUnlockedCacheWrite(io, dir, path, cutoff_ns);
}

fn removeUnlockedCacheWrite(
    io: std.Io,
    dir: std.Io.Dir,
    path: []const u8,
    cutoff_ns: ?i128,
) !TemporaryCacheWriteDisposition {
    var file = dir.openFile(io, path, .{ .mode = .read_write }) catch |err| switch (err) {
        error.FileNotFound => return .removed,
        else => return err,
    };
    const locked = file.tryLock(io, .exclusive) catch |err| {
        file.close(io);
        return err;
    };
    if (!locked) {
        file.close(io);
        return .retained;
    }
    defer {
        file.unlock(io);
        file.close(io);
    }
    const locked_stat = try file.stat(io);
    if (cutoff_ns) |cutoff| {
        if (locked_stat.mtime.toNanoseconds() > cutoff) return .retained;
    }
    dir.deleteFile(io, path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    return .removed;
}

fn temporaryCacheWriteOwnerId(path: []const u8) ?u128 {
    const marker = if (std.mem.lastIndexOf(u8, path, reserved_cache_write_marker) != null)
        reserved_cache_write_marker
    else
        ".tmp-query-cache-";
    const marker_index = std.mem.lastIndexOf(u8, path, marker) orelse return null;
    const encoded = path[marker_index + marker.len ..];
    const separator = std.mem.indexOfScalar(u8, encoded, '-') orelse return null;
    if (separator == 0) return null;
    return std.fmt.parseInt(u128, encoded[0..separator], 16) catch null;
}

fn temporaryCacheReservedBytes(path: []const u8) ?u64 {
    const marker_index = std.mem.lastIndexOf(u8, path, reserved_cache_write_marker) orelse return null;
    const raw_encoded = path[marker_index + reserved_cache_write_marker.len ..];
    const encoded = if (std.mem.endsWith(u8, raw_encoded, abandoned_cache_write_suffix))
        raw_encoded[0 .. raw_encoded.len - abandoned_cache_write_suffix.len]
    else
        raw_encoded;
    const separator = std.mem.lastIndexOfScalar(u8, encoded, '-') orelse return null;
    if (separator + 1 >= encoded.len) return null;
    return std.fmt.parseInt(u64, encoded[separator + 1 ..], 10) catch null;
}

fn temporaryCacheAccountedBytes(path: []const u8, physical_bytes: u64) u64 {
    return @max(physical_bytes, temporaryCacheReservedBytes(path) orelse 0);
}

fn cacheWriteOwnerState(
    alloc: Allocator,
    io: std.Io,
    root_dir: []const u8,
    owner_id: u128,
) !CacheWriteOwnerState {
    const lease_path = try cacheWriteOwnerLeasePathAlloc(alloc, root_dir, owner_id);
    defer alloc.free(lease_path);
    var file = (if (std.fs.path.isAbsolute(lease_path))
        std.Io.Dir.openFileAbsolute(io, lease_path, .{ .mode = .read_write })
    else
        std.Io.Dir.cwd().openFile(io, lease_path, .{ .mode = .read_write })) catch |err| switch (err) {
        error.FileNotFound => return .unknown,
        else => return err,
    };
    const locked = file.tryLock(io, .exclusive) catch |err| {
        file.close(io);
        return err;
    };
    if (!locked) {
        file.close(io);
        return .live;
    }
    file.unlock(io);
    file.close(io);
    return .dead;
}

fn pruneDeadCacheWriteOwnerLeases(
    alloc: Allocator,
    root_dir: []const u8,
    current_instance_id: u128,
    cancellation: CancellationToken,
) !void {
    const owner_dir_path = try std.fs.path.join(alloc, &.{ root_dir, cache_write_owner_dir_name });
    defer alloc.free(owner_dir_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    var owner_dir = std.Io.Dir.cwd().openDir(io, owner_dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer owner_dir.close(io);
    var iter = owner_dir.iterateAssumeFirstIteration();
    while (try iter.next(io)) |entry| {
        try cancellation.check();
        if (entry.kind != .file) continue;
        const owner_id = cacheWriteOwnerIdFromLeaseName(entry.name) orelse continue;
        if (owner_id == current_instance_id) continue;
        var file = owner_dir.openFile(io, entry.name, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => continue,
            else => return err,
        };
        const locked = file.tryLock(io, .exclusive) catch |err| {
            file.close(io);
            return err;
        };
        if (!locked) {
            file.close(io);
            continue;
        }
        file.unlock(io);
        file.close(io);
        owner_dir.deleteFile(io, entry.name) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
    }
}

fn cacheWriteOwnerIdFromLeaseName(name: []const u8) ?u128 {
    if (!std.mem.endsWith(u8, name, cache_write_owner_lease_suffix)) return null;
    const encoded = name[0 .. name.len - cache_write_owner_lease_suffix.len];
    if (encoded.len == 0) return null;
    return std.fmt.parseInt(u128, encoded, 16) catch null;
}

fn isCacheWriteOwnerLeasePath(path: []const u8) bool {
    const parent = std.fs.path.dirname(path) orelse return false;
    return std.mem.eql(u8, parent, cache_write_owner_dir_name) and
        std.mem.endsWith(u8, std.fs.path.basename(path), cache_write_owner_lease_suffix);
}

fn isCachePublicationLeasePath(path: []const u8) bool {
    return std.mem.endsWith(u8, path, cache_publication_lease_suffix);
}

fn isQueryCacheControlPath(path: []const u8) bool {
    return std.mem.eql(u8, path, cache_coordination_file_name) or
        isCacheWriteOwnerLeasePath(path) or
        isCachePublicationLeasePath(path);
}

fn usesVerifiedCacheRecordPath(path: []const u8) bool {
    return std.mem.startsWith(u8, path, ".ranges/") or
        std.mem.startsWith(u8, path, ".blocks/") or
        std.mem.startsWith(u8, path, ".blocks-pinned/");
}

fn cacheRecordFramingValid(io: std.Io, dir: std.Io.Dir, path: []const u8, file_size: u64) !bool {
    if (file_size < cache_record_header_len) return false;
    var file = dir.openFile(io, path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false,
        else => return err,
    };
    defer file.close(io);
    var header: [cache_record_header_len]u8 = undefined;
    if (try file.readPositionalAll(io, &header, 0) != header.len) return false;
    if (!std.mem.eql(u8, header[0..cache_record_magic.len], cache_record_magic)) return false;
    const declared_len = std.mem.readInt(u64, header[cache_record_magic.len..][0..8], .little);
    const framed_size = std.math.add(u64, cache_record_header_len, declared_len) catch return false;
    return framed_size == file_size;
}

fn addScannedFileUsage(usage: *CacheUsage, path: []const u8, size: u64) void {
    usage.total_bytes += size;
    if (std.mem.startsWith(u8, path, ".blocks-pinned/")) {
        usage.pinned_bytes += size;
        usage.pinned_block_count += 1;
    } else {
        usage.payload_bytes += size;
        if (std.mem.startsWith(u8, path, ".blocks/")) {
            usage.payload_block_count += 1;
        }
    }
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    platform_sync.lockYielding(mutex);
}

fn lockAtomicWithCancellation(mutex: *std.atomic.Mutex, cancellation: CancellationToken) !void {
    var attempts: usize = 0;
    while (!mutex.tryLock()) : (attempts += 1) {
        try cancellation.check();
        if (attempts < 64) {
            std.atomic.spinLoopHint();
        } else {
            std.Thread.yield() catch {};
        }
    }
    errdefer mutex.unlock();
    try cancellation.check();
}

fn applyUsageToStats(stats: *QueryCacheStats, usage: CacheUsage) void {
    stats.current_bytes = usage.total_bytes;
    stats.pinned_bytes = usage.pinned_bytes;
    stats.payload_bytes = usage.payload_bytes;
    stats.pinned_block_count = usage.pinned_block_count;
    stats.payload_block_count = usage.payload_block_count;
}

fn addUsage(usage: *CacheUsage, bytes: u64, lane: CacheWriteLane) void {
    usage.total_bytes += bytes;
    switch (lane) {
        .routing_block => {
            usage.pinned_bytes += bytes;
            usage.pinned_block_count += 1;
        },
        .payload_block => {
            usage.payload_bytes += bytes;
            usage.payload_block_count += 1;
        },
        .full, .range => usage.payload_bytes += bytes,
    }
}

fn subtractUsage(usage: *CacheUsage, bytes: u64, lane: CacheWriteLane) void {
    usage.total_bytes -|= bytes;
    switch (lane) {
        .routing_block => {
            usage.pinned_bytes -|= bytes;
            usage.pinned_block_count -|= 1;
        },
        .payload_block => {
            usage.payload_bytes -|= bytes;
            usage.payload_block_count -|= 1;
        },
        .full, .range => usage.payload_bytes -|= bytes,
    }
}

fn recordUsage(self: *QueryCache) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    applyUsageToStats(&self.stats, self.usage);
}

fn recordReconciliation(self: *QueryCache, integrity_failures: u64) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.integrity_failures +|= integrity_failures;
    applyUsageToStats(&self.stats, self.usage);
}

fn recordFullHit(self: *QueryCache) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.hits += 1;
    self.stats.full_hits += 1;
}

fn recordFullMiss(self: *QueryCache, published: bool) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.misses += 1;
    self.stats.full_misses += 1;
    if (published) {
        self.stats.writes += 1;
        self.stats.full_writes += 1;
    }
}

fn recordRangeHit(self: *QueryCache) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.hits += 1;
    self.stats.range_hits += 1;
}

fn recordRangeMiss(self: *QueryCache, published: bool) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.misses += 1;
    self.stats.range_misses += 1;
    if (published) {
        self.stats.writes += 1;
        self.stats.range_writes += 1;
    }
}

fn recordBlockHit(self: *QueryCache, block_class: BlockClass, payload_block_class: PayloadBlockClass) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.hits += 1;
    self.stats.block_hits += 1;
    incrementBlockClassHit(&self.stats, block_class);
    incrementPayloadBlockClassHit(&self.stats, payload_block_class);
}

fn recordBlockMiss(self: *QueryCache, block_class: BlockClass, payload_block_class: PayloadBlockClass, published: bool) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.misses += 1;
    self.stats.block_misses += 1;
    incrementBlockClassMiss(&self.stats, block_class);
    incrementPayloadBlockClassMiss(&self.stats, payload_block_class);
    if (published) {
        self.stats.writes += 1;
        self.stats.block_writes += 1;
        incrementBlockClassWrite(&self.stats, block_class);
        incrementPayloadBlockClassWrite(&self.stats, payload_block_class);
    }
}

fn recordEviction(self: *QueryCache) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.evictions += 1;
}

fn recordBypass(self: *QueryCache) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.bypasses += 1;
}

fn removeCorruptCacheEntry(self: *QueryCache, path: []const u8, cancellation: CancellationToken) !void {
    try lockAtomicWithCancellation(&self.maintenance_mu, cancellation);
    defer self.maintenance_mu.unlock();
    var coordination_io = threadedIo();
    defer coordination_io.deinit();
    const io = coordination_io.io();
    try lockFileExclusiveWithCancellation(self.coordination_file, io, cancellation);
    defer self.coordination_file.unlock(io);
    try synchronizeCacheUsageUnderCoordinationLock(self, io, cancellation, false);
    self.observed_coordination_generation = try advanceCacheCoordinationGeneration(self.coordination_file, io);
    try cancellation.check();
    const removed = blk: {
        deleteFilePath(io, path) catch |err| switch (err) {
            error.FileNotFound => break :blk false,
            else => return err,
        };
        break :blk true;
    };
    self.usage = try cacheUsageNoLock(self.alloc, self.root_dir, cancellation);
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    if (removed) self.stats.integrity_failures +|= 1;
    applyUsageToStats(&self.stats, self.usage);
}

fn openFilePath(io: std.Io, path: []const u8) !std.Io.File {
    return if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
}

fn deleteFilePath(io: std.Io, path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io, path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io, path);
    }
}

fn renameFilePath(io: std.Io, source: []const u8, destination: []const u8) !void {
    if (std.fs.path.isAbsolute(source) and std.fs.path.isAbsolute(destination)) {
        try std.Io.Dir.renameAbsolute(source, destination, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), source, std.Io.Dir.cwd(), destination, io);
    }
}

fn fileExists(path: []const u8) bool {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var file = openFilePath(io_impl.io(), path) catch return false;
    file.close(io_impl.io());
    return true;
}

fn isTemporaryCachePath(path: []const u8) bool {
    return std.mem.indexOf(u8, path, ".tmp-query-cache-") != null;
}

fn incrementBlockClassHit(stats: *QueryCacheStats, block_class: BlockClass) void {
    switch (block_class) {
        .routing => stats.routing_block_hits += 1,
        .payload => stats.payload_block_hits += 1,
    }
}

fn incrementBlockClassMiss(stats: *QueryCacheStats, block_class: BlockClass) void {
    switch (block_class) {
        .routing => stats.routing_block_misses += 1,
        .payload => stats.payload_block_misses += 1,
    }
}

fn incrementBlockClassWrite(stats: *QueryCacheStats, block_class: BlockClass) void {
    switch (block_class) {
        .routing => stats.routing_block_writes += 1,
        .payload => stats.payload_block_writes += 1,
    }
}

fn incrementPayloadBlockClassHit(stats: *QueryCacheStats, payload_block_class: PayloadBlockClass) void {
    switch (payload_block_class) {
        .none => {},
        .approximate => stats.approx_payload_block_hits += 1,
        .exact => stats.exact_payload_block_hits += 1,
    }
}

fn incrementPayloadBlockClassMiss(stats: *QueryCacheStats, payload_block_class: PayloadBlockClass) void {
    switch (payload_block_class) {
        .none => {},
        .approximate => stats.approx_payload_block_misses += 1,
        .exact => stats.exact_payload_block_misses += 1,
    }
}

fn incrementPayloadBlockClassWrite(stats: *QueryCacheStats, payload_block_class: PayloadBlockClass) void {
    switch (payload_block_class) {
        .none => {},
        .approximate => stats.approx_payload_block_writes += 1,
        .exact => stats.exact_payload_block_writes += 1,
    }
}

fn collectEvictablePayloadFilesAlloc(alloc: Allocator, root_dir: []const u8, cancellation: CancellationToken) ![]EvictableFile {
    var out = std.ArrayListUnmanaged(EvictableFile).empty;
    errdefer freeEvictableFiles(alloc, out.items);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return try out.toOwnedSlice(alloc),
        else => return err,
    };
    defer dir.close(io_impl.io());
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        try cancellation.check();
        if (entry.kind != .file) continue;
        if (isTemporaryCachePath(entry.path) or isQueryCacheControlPath(entry.path)) continue;
        const is_payload = std.mem.startsWith(u8, entry.path, ".blocks/") or std.mem.startsWith(u8, entry.path, ".ranges/");
        if (!is_payload) continue;
        const path = try std.fs.path.join(alloc, &.{ root_dir, entry.path });
        errdefer alloc.free(path);
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        try out.append(alloc, .{
            .path = path,
            .size = stat.size,
            .last_access_ns = stat.mtime.toNanoseconds(),
            .payload_block_class = if (std.mem.startsWith(u8, entry.path, ".blocks/"))
                cachedPayloadBlockClass(std.fs.path.basename(entry.path))
            else
                .approximate,
            .counts_as_payload_block = std.mem.startsWith(u8, entry.path, ".blocks/"),
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn collectEvictableRootFilesAlloc(alloc: Allocator, root_dir: []const u8, cancellation: CancellationToken) ![]EvictableFile {
    var out = std.ArrayListUnmanaged(EvictableFile).empty;
    errdefer freeEvictableFiles(alloc, out.items);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return try out.toOwnedSlice(alloc),
        else => return err,
    };
    defer dir.close(io_impl.io());
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        try cancellation.check();
        if (entry.kind != .file) continue;
        if (isTemporaryCachePath(entry.path) or isQueryCacheControlPath(entry.path)) continue;
        if (std.mem.indexOfScalar(u8, entry.path, std.fs.path.sep)) |_| continue;
        const path = try std.fs.path.join(alloc, &.{ root_dir, entry.path });
        errdefer alloc.free(path);
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        try out.append(alloc, .{
            .path = path,
            .size = stat.size,
            .last_access_ns = stat.mtime.toNanoseconds(),
            .payload_block_class = .approximate,
            .counts_as_payload_block = false,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn collectEvictablePinnedFilesAlloc(alloc: Allocator, root_dir: []const u8, cancellation: CancellationToken) ![]EvictableFile {
    var out = std.ArrayListUnmanaged(EvictableFile).empty;
    errdefer freeEvictableFiles(alloc, out.items);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return try out.toOwnedSlice(alloc),
        else => return err,
    };
    defer dir.close(io_impl.io());
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        try cancellation.check();
        if (entry.kind != .file or isTemporaryCachePath(entry.path) or isQueryCacheControlPath(entry.path)) continue;
        if (!std.mem.startsWith(u8, entry.path, ".blocks-pinned/")) continue;
        const path = try std.fs.path.join(alloc, &.{ root_dir, entry.path });
        errdefer alloc.free(path);
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        try out.append(alloc, .{
            .path = path,
            .size = stat.size,
            .last_access_ns = stat.mtime.toNanoseconds(),
            .payload_block_class = .none,
            .counts_as_payload_block = false,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn freeEvictableFiles(alloc: Allocator, files: []EvictableFile) void {
    for (files) |file| alloc.free(file.path);
    alloc.free(files);
}

fn lessEvictableFile(_: void, lhs: EvictableFile, rhs: EvictableFile) bool {
    if (lhs.last_access_ns != rhs.last_access_ns) return lhs.last_access_ns < rhs.last_access_ns;
    const lhs_priority = payloadEvictionPriority(lhs.payload_block_class);
    const rhs_priority = payloadEvictionPriority(rhs.payload_block_class);
    if (lhs_priority != rhs_priority) return lhs_priority > rhs_priority;
    if (lhs.size != rhs.size) return lhs.size > rhs.size;
    return std.mem.order(u8, lhs.path, rhs.path) == .lt;
}

fn payloadEvictionPriority(payload_block_class: PayloadBlockClass) u8 {
    return switch (payload_block_class) {
        .none => 0,
        .approximate => 1,
        .exact => 3,
    };
}

test "serverless query cache reuses cached artifact contents" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts");
    const cache_root = tmpPath(&cache_root_buf, "cache");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var meta = try artifact_store.put("hello-cache");
    defer meta.deinit(alloc);

    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    const first = try cache.getOrFetchAlloc(&artifact_store, meta.artifact_id);
    defer alloc.free(first);
    try std.testing.expectEqualStrings("hello-cache", first);

    try artifact_store.delete(meta.artifact_id);
    const second = try cache.getOrFetchAlloc(&artifact_store, meta.artifact_id);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("hello-cache", second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.misses);
    try std.testing.expectEqual(@as(u64, 1), stats.hits);
    try std.testing.expectEqual(@as(u64, 1), stats.writes);
    try std.testing.expectEqual(@as(u64, 1), stats.full_misses);
    try std.testing.expectEqual(@as(u64, 1), stats.full_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.full_writes);
}

test "serverless query cache cancels bounded positional reads without recording a hit" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-cancel");
    const cache_root = tmpPath(&cache_root_buf, "cache-cancel");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    const contents = try alloc.alloc(u8, 2 * 1024 * 1024);
    defer alloc.free(contents);
    @memset(contents, 'x');
    var meta = try artifact_store.put(contents);
    defer meta.deinit(alloc);

    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();
    const warmed = try cache.getOrFetchAlloc(&artifact_store, meta.artifact_id);
    alloc.free(warmed);

    const State = struct {
        checks: usize = 0,

        fn isCancelled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 4;
        }
    };
    var state = State{};
    try std.testing.expectError(
        error.Canceled,
        cache.getRangeOrFetchAllocWithCancellation(
            &artifact_store,
            meta.artifact_id,
            0,
            contents.len,
            CancellationToken{ .ptr = &state, .is_cancelled_fn = State.isCancelled },
        ),
    );
    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.range_hits);
}

test "serverless query cache rejects unsafe artifact ids before filesystem access" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-safe-keys");
    const cache_root = tmpPath(&cache_root_buf, "cache-safe-keys");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    try std.testing.expectError(
        error.InvalidArtifactId,
        cache.getOrFetchAlloc(&artifact_store, "../../outside-cache"),
    );
}

test "serverless query cache supports relative cache directories" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-relative-cache");
    defer cleanupTmp(artifact_root);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const relative_cache_root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/relative-cache", .{tmp.sub_path});
    defer alloc.free(relative_cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var meta = try artifact_store.put("relative");
    defer meta.deinit(alloc);
    var cache = try QueryCache.init(alloc, relative_cache_root);
    defer cache.deinit();

    const payload = try cache.getOrFetchVerifiedAllocWithCancellation(
        &artifact_store,
        meta.artifact_id,
        meta.byte_len,
        meta.checksum,
        .none,
    );
    defer alloc.free(payload);
    try std.testing.expectEqualStrings("relative", payload);
    try std.testing.expectEqual(@as(u64, payload.len), cache.statsSnapshot().current_bytes);
}

test "serverless query cache bypasses oversized entries without evicting useful data" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-oversize-bypass");
    const cache_root = tmpPath(&cache_root_buf, "cache-oversize-bypass");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var useful = try artifact_store.put("keep");
    defer useful.deinit(alloc);
    var oversized = try artifact_store.put("too-large");
    defer oversized.deinit(alloc);
    var cache = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 4,
        .max_payload_bytes = 4,
    });
    defer cache.deinit();

    const first = try cache.getOrFetchAlloc(&artifact_store, useful.artifact_id);
    defer alloc.free(first);
    const second = try cache.getOrFetchAlloc(&artifact_store, oversized.artifact_id);
    defer alloc.free(second);
    try artifact_store.delete(useful.artifact_id);
    const retained = try cache.getOrFetchAlloc(&artifact_store, useful.artifact_id);
    defer alloc.free(retained);
    try std.testing.expectEqualStrings("keep", retained);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 4), stats.current_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.bypasses);
    try std.testing.expectEqual(@as(u64, 0), stats.evictions);
}

test "serverless query cache rejects corrupted verified full entries" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-corrupt-full");
    const cache_root = tmpPath(&cache_root_buf, "cache-corrupt-full");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var meta = try artifact_store.put("trusted");
    defer meta.deinit(alloc);
    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();
    const warmed = try cache.getOrFetchVerifiedAllocWithCancellation(
        &artifact_store,
        meta.artifact_id,
        meta.byte_len,
        meta.checksum,
        .none,
    );
    alloc.free(warmed);

    const path = try cachePathAlloc(alloc, std.mem.span(cache_root), meta.artifact_id);
    defer alloc.free(path);
    try overwriteFile(path, "altered");
    try artifact_store.delete(meta.artifact_id);
    try std.testing.expectError(
        error.FileNotFound,
        cache.getOrFetchVerifiedAllocWithCancellation(
            &artifact_store,
            meta.artifact_id,
            meta.byte_len,
            meta.checksum,
            .none,
        ),
    );
    try std.testing.expectEqual(@as(u64, 1), cache.statsSnapshot().integrity_failures);
    try std.testing.expect(!fileExists(path));
}

test "serverless query cache uses the requested allocator on misses and hits" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-result-allocator");
    const cache_root = tmpPath(&cache_root_buf, "cache-result-allocator");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var meta = try artifact_store.put("allocator-owned");
    defer meta.deinit(alloc);
    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    var result_storage: [4096]u8 = undefined;
    var result_fba = std.heap.FixedBufferAllocator.init(&result_storage);
    const result_alloc = result_fba.allocator();

    const first = try cache.getOrFetchVerifiedAllocWithCancellationUsingAllocator(
        result_alloc,
        &artifact_store,
        meta.artifact_id,
        meta.byte_len,
        meta.checksum,
        .none,
    );
    try expectSliceInBuffer(first, &result_storage);
    result_alloc.free(first);
    try std.testing.expectEqual(@as(usize, 0), result_fba.end_index);

    try artifact_store.delete(meta.artifact_id);
    const second = try cache.getOrFetchVerifiedAllocWithCancellationUsingAllocator(
        result_alloc,
        &artifact_store,
        meta.artifact_id,
        meta.byte_len,
        meta.checksum,
        .none,
    );
    try expectSliceInBuffer(second, &result_storage);
    try std.testing.expectEqualStrings("allocator-owned", second);
    result_alloc.free(second);
    try std.testing.expectEqual(@as(usize, 0), result_fba.end_index);
}

test "serverless query cache rejects same-size corrupted range and block records" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-corrupt-records");
    const cache_root = tmpPath(&cache_root_buf, "cache-corrupt-records");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var range_meta = try artifact_store.put("range-data");
    defer range_meta.deinit(alloc);
    var block_meta = try artifact_store.put("block-data");
    defer block_meta.deinit(alloc);
    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    const range = try cache.getRangeOrFetchAlloc(&artifact_store, range_meta.artifact_id, 1, 3);
    alloc.free(range);
    const block = try cache.getBlockOrFetchRangeAlloc(&artifact_store, block_meta.artifact_id, "vector-cluster-0-exact", 1, 3);
    alloc.free(block);

    const range_path = try rangeCachePathAlloc(alloc, std.mem.span(cache_root), range_meta.artifact_id, 1, 3);
    defer alloc.free(range_path);
    const block_path = try blockCachePathAlloc(
        alloc,
        std.mem.span(cache_root),
        block_meta.artifact_id,
        "vector-cluster-0-exact",
        1,
        3,
        .payload,
    );
    defer alloc.free(block_path);
    const corrupt_record = [_]u8{'x'} ** (cache_record_header_len + 3);
    try overwriteFile(range_path, &corrupt_record);
    try overwriteFile(block_path, &corrupt_record);
    // Force the first corrupt read through shared reconciliation. Both records
    // can be scrubbed in that pass, and telemetry must count each physical
    // corruption exactly once rather than only the directly requested path.
    {
        var coordination_io = threadedIo();
        defer coordination_io.deinit();
        const io = coordination_io.io();
        try lockFileExclusiveWithCancellation(cache.coordination_file, io, .none);
        defer cache.coordination_file.unlock(io);
        _ = try advanceCacheCoordinationGeneration(cache.coordination_file, io);
    }
    try artifact_store.delete(range_meta.artifact_id);
    try artifact_store.delete(block_meta.artifact_id);

    try std.testing.expectError(
        error.FileNotFound,
        cache.getRangeOrFetchAlloc(&artifact_store, range_meta.artifact_id, 1, 3),
    );
    try std.testing.expectError(
        error.FileNotFound,
        cache.getBlockOrFetchRangeAlloc(&artifact_store, block_meta.artifact_id, "vector-cluster-0-exact", 1, 3),
    );
    try std.testing.expectEqual(@as(u64, 2), cache.statsSnapshot().integrity_failures);
    try std.testing.expect(!fileExists(range_path));
    try std.testing.expect(!fileExists(block_path));
}

test "serverless query cache leases live writes and reclaims crashed writers during startup" {
    const alloc = std.testing.allocator;
    var cache_root_buf: [256]u8 = undefined;
    const cache_root = tmpPath(&cache_root_buf, "cache-startup-temp");
    defer cleanupTmp(cache_root);

    var initial = try QueryCache.init(alloc, std.mem.span(cache_root));
    const live_path = try std.fmt.allocPrint(
        alloc,
        "{s}/live.tmp-query-cache-{x}-0",
        .{ std.mem.span(cache_root), initial.instance_id },
    );
    defer alloc.free(live_path);
    try overwriteFile(live_path, "live-write");

    var fresh_overlap = try QueryCache.init(alloc, std.mem.span(cache_root));
    try std.testing.expectEqual(@as(u64, "live-write".len), fresh_overlap.statsSnapshot().current_bytes);
    fresh_overlap.deinit();
    try std.testing.expect(fileExists(live_path));

    initial.deinit();
    try setFileModifyTimestamp(live_path, 1);
    var recovered_live_owner = try QueryCache.init(alloc, std.mem.span(cache_root));
    recovered_live_owner.deinit();
    try std.testing.expect(!fileExists(live_path));

    const crashed_owner_id: u128 = 0x42;
    const crashed_lease_path = try cacheWriteOwnerLeasePathAlloc(alloc, std.mem.span(cache_root), crashed_owner_id);
    defer alloc.free(crashed_lease_path);
    try ensureParentDir(crashed_lease_path);
    try overwriteFile(crashed_lease_path, "");
    const crashed_path = try std.fmt.allocPrint(
        alloc,
        "{s}/crashed.tmp-query-cache-{x}-0",
        .{ std.mem.span(cache_root), crashed_owner_id },
    );
    defer alloc.free(crashed_path);
    try overwriteFile(crashed_path, "recent-crash");

    var recovered_crash = try QueryCache.init(alloc, std.mem.span(cache_root));
    recovered_crash.deinit();
    try std.testing.expect(!fileExists(crashed_path));
    try std.testing.expect(!fileExists(crashed_lease_path));

    const legacy_path = try std.fs.path.join(alloc, &.{ std.mem.span(cache_root), "legacy.tmp-query-cache-42" });
    defer alloc.free(legacy_path);
    try overwriteFile(legacy_path, "incomplete");

    var legacy_overlap = try QueryCache.init(alloc, std.mem.span(cache_root));
    try std.testing.expect(fileExists(legacy_path));
    try std.testing.expectEqual(@as(u64, "incomplete".len), legacy_overlap.statsSnapshot().current_bytes);
    legacy_overlap.deinit();

    try setFileModifyTimestamp(legacy_path, 1);
    {
        var lock_io = threadedIo();
        defer lock_io.deinit();
        var locked_file = try std.Io.Dir.openFileAbsolute(lock_io.io(), legacy_path, .{ .mode = .read_write });
        defer locked_file.close(lock_io.io());
        try locked_file.lock(lock_io.io(), .exclusive);
        defer locked_file.unlock(lock_io.io());

        var locked_overlap = try QueryCache.init(alloc, std.mem.span(cache_root));
        defer locked_overlap.deinit();
        try std.testing.expect(fileExists(legacy_path));
        try std.testing.expectEqual(@as(u64, "incomplete".len), locked_overlap.statsSnapshot().current_bytes);
    }

    var recovered = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer recovered.deinit();
    try std.testing.expect(!fileExists(legacy_path));
    try std.testing.expectEqual(@as(u64, 0), recovered.statsSnapshot().current_bytes);
}

test "serverless query cache maintenance reclaims newly orphaned owner writes" {
    const alloc = std.testing.allocator;
    var cache_root_buf: [256]u8 = undefined;
    const cache_root = tmpPath(&cache_root_buf, "cache-maintenance-orphan");
    defer cleanupTmp(cache_root);

    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    const crashed_owner_id: u128 = 0x99;
    const crashed_lease_path = try cacheWriteOwnerLeasePathAlloc(alloc, std.mem.span(cache_root), crashed_owner_id);
    defer alloc.free(crashed_lease_path);
    try ensureParentDir(crashed_lease_path);
    try overwriteFile(crashed_lease_path, "");
    const crashed_path = try std.fmt.allocPrint(
        alloc,
        "{s}/crashed.tmp-query-cache-{x}-0",
        .{ std.mem.span(cache_root), crashed_owner_id },
    );
    defer alloc.free(crashed_path);
    try overwriteFile(crashed_path, "orphaned-after-startup");

    cache.next_abandoned_write_sweep_ns = 0;
    try maybeReconcileCacheOnDisk(&cache, .none);
    try std.testing.expect(!fileExists(crashed_path));
    try std.testing.expect(!fileExists(crashed_lease_path));
    try std.testing.expectEqual(@as(u64, 0), cache.statsSnapshot().current_bytes);
}

test "serverless query cache reserves shared disk budget before staging bytes" {
    const alloc = std.testing.allocator;
    var cache_root_buf: [256]u8 = undefined;
    const cache_root = tmpPath(&cache_root_buf, "cache-shared-reservation-budget");
    defer cleanupTmp(cache_root);

    const cfg = QueryCacheConfig{
        .max_bytes = 10,
        .max_payload_bytes = 10,
    };
    var first = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), cfg);
    defer first.deinit();
    // Open before the reservation to prove the shared mutation token repairs a
    // stale per-process usage snapshot at the next admission boundary.
    var overlap = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), cfg);
    defer overlap.deinit();

    const first_path = try std.fs.path.join(alloc, &.{ std.mem.span(cache_root), "first" });
    defer alloc.free(first_path);
    var reservation_path: []u8 = undefined;
    {
        try lockAtomicWithCancellation(&first.maintenance_mu, .none);
        defer first.maintenance_mu.unlock();
        var coordination_io = threadedIo();
        defer coordination_io.deinit();
        const io = coordination_io.io();
        try lockFileExclusiveWithCancellation(first.coordination_file, io, .none);
        defer first.coordination_file.unlock(io);
        try synchronizeCacheUsageUnderCoordinationLock(&first, io, .none, false);
        first.observed_coordination_generation = try advanceCacheCoordinationGeneration(first.coordination_file, io);
        try std.testing.expect(try ensureCapacityForWrite(&first, 8, .full, .none));
        reservation_path = try reserveTempFile(alloc, first_path, first.instance_id, 8);
        addUsage(&first.usage, 8, .full);
        recordUsage(&first);
    }
    defer alloc.free(reservation_path);
    var reservation_active = true;
    defer if (reservation_active) discardCacheReservationBestEffort(&first, reservation_path, 8, .full);

    const second_path = try std.fs.path.join(alloc, &.{ std.mem.span(cache_root), "second" });
    defer alloc.free(second_path);
    try std.testing.expect(!try publishCacheEntry(&overlap, second_path, "12345678", .full, .none));
    try std.testing.expect(fileExists(reservation_path));
    try std.testing.expectEqual(@as(u64, 8), overlap.statsSnapshot().current_bytes);
    try std.testing.expect(overlap.statsSnapshot().current_bytes <= cfg.max_bytes);

    discardCacheReservationBestEffort(&first, reservation_path, 8, .full);
    reservation_active = false;
}

test "serverless query cache coalesces duplicate publication reservations before eviction" {
    const alloc = std.testing.allocator;
    var cache_root_buf: [256]u8 = undefined;
    const cache_root = tmpPath(&cache_root_buf, "cache-shared-publication-lease");
    defer cleanupTmp(cache_root);

    const cfg = QueryCacheConfig{
        .max_bytes = 16,
        .max_payload_bytes = 16,
    };
    var first = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), cfg);
    defer first.deinit();
    var overlap = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), cfg);
    defer overlap.deinit();

    const retained_path = try std.fs.path.join(alloc, &.{ std.mem.span(cache_root), "retained" });
    defer alloc.free(retained_path);
    try std.testing.expect(try publishCacheEntry(&first, retained_path, "12345678", .full, .none));

    const shared_path = try std.fs.path.join(alloc, &.{ std.mem.span(cache_root), "shared" });
    defer alloc.free(shared_path);
    var reservation_path: []u8 = undefined;
    var publication_lease: CachePublicationLease = undefined;
    {
        try lockAtomicWithCancellation(&first.maintenance_mu, .none);
        defer first.maintenance_mu.unlock();
        var coordination_io = threadedIo();
        defer coordination_io.deinit();
        const io = coordination_io.io();
        try lockFileExclusiveWithCancellation(first.coordination_file, io, .none);
        defer first.coordination_file.unlock(io);
        try synchronizeCacheUsageUnderCoordinationLock(&first, io, .none, false);
        publication_lease = (try tryAcquireCachePublicationLease(alloc, io, shared_path)).?;
        first.observed_coordination_generation = try advanceCacheCoordinationGeneration(first.coordination_file, io);
        try std.testing.expect(try ensureCapacityForWrite(&first, 8, .full, .none));
        reservation_path = try reserveTempFile(alloc, shared_path, first.instance_id, 8);
        addUsage(&first.usage, 8, .full);
        recordUsage(&first);
    }
    defer publication_lease.release(alloc);
    defer alloc.free(reservation_path);
    var reservation_active = true;
    defer if (reservation_active) discardCacheReservationBestEffort(&first, reservation_path, 8, .full);

    try std.testing.expect(!try publishCacheEntry(&overlap, shared_path, "abcdefgh", .full, .none));
    try std.testing.expect(fileExists(retained_path));
    try std.testing.expect(!fileExists(shared_path));
    try std.testing.expectEqual(@as(u64, 0), overlap.statsSnapshot().evictions);
    try std.testing.expectEqual(@as(u64, 0), overlap.statsSnapshot().bypasses);
    try std.testing.expectEqual(@as(u64, 16), overlap.statsSnapshot().current_bytes);

    discardCacheReservationBestEffort(&first, reservation_path, 8, .full);
    reservation_active = false;
}

test "serverless query cache hands cleanup off without waiting for maintenance" {
    const alloc = std.testing.allocator;
    var cache_root_buf: [256]u8 = undefined;
    const cache_root = tmpPath(&cache_root_buf, "cache-cancel-cleanup-handoff");
    defer cleanupTmp(cache_root);

    var cache = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 8,
        .max_payload_bytes = 8,
    });
    defer cache.deinit();
    const path = try std.fs.path.join(alloc, &.{ std.mem.span(cache_root), "canceled" });
    defer alloc.free(path);
    var reservation_path: []u8 = undefined;
    var publication_lease: CachePublicationLease = undefined;
    {
        var coordination_io = threadedIo();
        defer coordination_io.deinit();
        const io = coordination_io.io();
        try lockFileExclusiveWithCancellation(cache.coordination_file, io, .none);
        defer cache.coordination_file.unlock(io);
        publication_lease = (try tryAcquireCachePublicationLease(alloc, io, path)).?;
        cache.observed_coordination_generation = try advanceCacheCoordinationGeneration(cache.coordination_file, io);
        reservation_path = try reserveTempFile(alloc, path, cache.instance_id, 8);
    }
    defer alloc.free(reservation_path);
    addUsage(&cache.usage, 8, .full);
    recordUsage(&cache);

    lockAtomic(&cache.maintenance_mu);
    discardCacheReservationBestEffort(&cache, reservation_path, 8, .full);
    cache.maintenance_mu.unlock();
    // Cancellation releases the key lease without waiting for the global
    // coordination lock. Its unlocked file is intentionally reusable.
    publication_lease.release(alloc);

    try std.testing.expect(!fileExists(reservation_path));
    try std.testing.expectEqual(@as(u64, 8), try currentBytesNoLock(alloc, std.mem.span(cache_root)));
    try std.testing.expect(try publishCacheEntry(&cache, path, "abcdefgh", .full, .none));
    try std.testing.expectEqual(@as(u64, 8), try currentBytesNoLock(alloc, std.mem.span(cache_root)));
    try std.testing.expectEqual(@as(u64, 8), cache.statsSnapshot().current_bytes);
}

test "serverless query cache prunes legacy pinned block records during startup" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-legacy-pinned");
    const cache_root = tmpPath(&cache_root_buf, "cache-legacy-pinned");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var meta = try artifact_store.put("hdr");
    defer meta.deinit(alloc);
    var initial = try QueryCache.init(alloc, std.mem.span(cache_root));
    const current = try initial.getBlockOrFetchRangeAlloc(&artifact_store, meta.artifact_id, "vector-header", 0, 3);
    alloc.free(current);
    initial.deinit();

    const checksum = try artifacts_mod.sha256ChecksumFromArtifactId(meta.artifact_id);
    var legacy_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash("vector-header", &legacy_digest, .{});
    const legacy_digest_hex = std.fmt.bytesToHex(legacy_digest, .lower);
    const legacy_name = try std.fmt.allocPrint(alloc, "routing-{s}", .{legacy_digest_hex});
    defer alloc.free(legacy_name);
    const legacy_path = try std.fs.path.join(alloc, &.{ std.mem.span(cache_root), ".blocks-pinned", checksum, legacy_name });
    defer alloc.free(legacy_path);
    try ensureParentDir(legacy_path);
    try overwriteFile(legacy_path, "legacy-raw-block");
    try std.testing.expect(fileExists(legacy_path));

    var migrated = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer migrated.deinit();
    try std.testing.expect(!fileExists(legacy_path));
    const stats = migrated.statsSnapshot();
    try std.testing.expectEqual(@as(u64, cache_record_header_len + 3), stats.pinned_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.pinned_block_count);

    try artifact_store.delete(meta.artifact_id);
    const preserved = try migrated.getBlockOrFetchRangeAlloc(&artifact_store, meta.artifact_id, "vector-header", 0, 3);
    defer alloc.free(preserved);
    try std.testing.expectEqualStrings("hdr", preserved);
}

test "serverless query cache enforces tighter budgets during startup" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-startup-budget");
    const cache_root = tmpPath(&cache_root_buf, "cache-startup-budget");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var full_meta = try artifact_store.put("full");
    defer full_meta.deinit(alloc);
    var routing_meta = try artifact_store.put("hdr");
    defer routing_meta.deinit(alloc);

    var initial = try QueryCache.init(alloc, std.mem.span(cache_root));
    const full = try initial.getOrFetchAlloc(&artifact_store, full_meta.artifact_id);
    alloc.free(full);
    const routing = try initial.getBlockOrFetchRangeAlloc(&artifact_store, routing_meta.artifact_id, "vector-header", 0, 3);
    alloc.free(routing);
    initial.deinit();

    var constrained = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 4,
        .max_payload_bytes = 4,
    });
    defer constrained.deinit();
    const stats = constrained.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 0), stats.current_bytes);
    try std.testing.expect(stats.payload_bytes <= 4);
    try std.testing.expectEqual(@as(u64, 0), stats.pinned_bytes);
    try std.testing.expectEqual(@as(u64, 0), stats.pinned_block_count);
    try std.testing.expect(stats.evictions >= 1);
}

test "serverless query cache maintenance waits remain cancellation responsive" {
    const alloc = std.testing.allocator;
    var cache_root_buf: [256]u8 = undefined;
    const cache_root = tmpPath(&cache_root_buf, "cache-maintenance-cancel");
    defer cleanupTmp(cache_root);
    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();
    const artifact_id = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const path = try cachePathAlloc(alloc, std.mem.span(cache_root), artifact_id);
    defer alloc.free(path);

    const State = struct {
        checks: usize = 0,
        fn isCancelled(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            return self.checks >= 4;
        }
    };
    var state = State{};
    lockAtomic(&cache.maintenance_mu);
    defer cache.maintenance_mu.unlock();
    try std.testing.expectError(
        error.Canceled,
        publishCacheEntry(
            &cache,
            path,
            "x",
            .full,
            .{ .ptr = &state, .is_cancelled_fn = State.isCancelled },
        ),
    );
    try std.testing.expect(!fileExists(path));
}

test "serverless query cache clears cache when max bytes would be exceeded" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-evict");
    const cache_root = tmpPath(&cache_root_buf, "cache-evict");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var a = try artifact_store.put("aaaa");
    defer a.deinit(alloc);
    var b = try artifact_store.put("bbbb");
    defer b.deinit(alloc);

    var cache = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 4,
        .max_payload_bytes = 4,
    });
    defer cache.deinit();

    const first = try cache.getOrFetchAlloc(&artifact_store, a.artifact_id);
    defer alloc.free(first);
    const second = try cache.getOrFetchAlloc(&artifact_store, b.artifact_id);
    defer alloc.free(second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 2), stats.misses);
    try std.testing.expectEqual(@as(u64, 2), stats.full_misses);
    try std.testing.expectEqual(@as(u64, 1), stats.evictions);
    try std.testing.expect(stats.current_bytes <= 4);
}

test "serverless query cache keeps integrity-checked ranges independent of full artifacts" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-range");
    const cache_root = tmpPath(&cache_root_buf, "cache-range");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var meta = try artifact_store.put("abcdefgh");
    defer meta.deinit(alloc);

    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    const ranged = try cache.getRangeOrFetchAlloc(&artifact_store, meta.artifact_id, 2, 3);
    defer alloc.free(ranged);
    try std.testing.expectEqualStrings("cde", ranged);

    const full = try cache.getOrFetchAlloc(&artifact_store, meta.artifact_id);
    defer alloc.free(full);
    try std.testing.expectEqualStrings("abcdefgh", full);

    try artifact_store.delete(meta.artifact_id);
    const ranged_again = try cache.getRangeOrFetchAlloc(&artifact_store, meta.artifact_id, 2, 3);
    defer alloc.free(ranged_again);
    try std.testing.expectEqualStrings("cde", ranged_again);

    const stats = cache.statsSnapshot();
    try std.testing.expect(stats.hits >= 1);
    try std.testing.expect(stats.misses >= 1);
    try std.testing.expect(stats.range_hits >= 1);
    try std.testing.expect(stats.range_misses >= 1);
    try std.testing.expect(stats.full_misses >= 1);
}

test "serverless query cache binds named block keys to exact coordinates" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-block-coordinates");
    const cache_root = tmpPath(&cache_root_buf, "cache-block-coordinates");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var meta = try artifact_store.put("abcdef");
    defer meta.deinit(alloc);
    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    const first = try cache.getBlockOrFetchRangeAlloc(&artifact_store, meta.artifact_id, "vector-cluster-0-exact", 0, 3);
    defer alloc.free(first);
    const second = try cache.getBlockOrFetchRangeAlloc(&artifact_store, meta.artifact_id, "vector-cluster-0-exact", 3, 3);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("abc", first);
    try std.testing.expectEqualStrings("def", second);

    try artifact_store.delete(meta.artifact_id);
    const first_cached = try cache.getBlockOrFetchRangeAlloc(&artifact_store, meta.artifact_id, "vector-cluster-0-exact", 0, 3);
    defer alloc.free(first_cached);
    const second_cached = try cache.getBlockOrFetchRangeAlloc(&artifact_store, meta.artifact_id, "vector-cluster-0-exact", 3, 3);
    defer alloc.free(second_cached);
    try std.testing.expectEqualStrings("abc", first_cached);
    try std.testing.expectEqualStrings("def", second_cached);
}

test "serverless query cache caches named blocks by artifact and block id" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-block");
    const cache_root = tmpPath(&cache_root_buf, "cache-block");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var meta = try artifact_store.put("abcdefgh");
    defer meta.deinit(alloc);

    var cache = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer cache.deinit();

    const first = try cache.getBlockOrFetchRangeAlloc(&artifact_store, meta.artifact_id, "vector-header", 0, 3);
    defer alloc.free(first);
    try std.testing.expectEqualStrings("abc", first);

    try artifact_store.delete(meta.artifact_id);
    const second = try cache.getBlockOrFetchRangeAlloc(&artifact_store, meta.artifact_id, "vector-header", 0, 3);
    defer alloc.free(second);
    try std.testing.expectEqualStrings("abc", second);

    const stats = cache.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.block_misses);
    try std.testing.expectEqual(@as(u64, 1), stats.block_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.block_writes);
    try std.testing.expectEqual(@as(u64, 1), stats.routing_block_misses);
    try std.testing.expectEqual(@as(u64, 1), stats.routing_block_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.routing_block_writes);
}

test "serverless query cache preserves pinned routing blocks when evicting payload blocks" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-block-pinned");
    const cache_root = tmpPath(&cache_root_buf, "cache-block-pinned");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var routing_meta = try artifact_store.put("abc");
    defer routing_meta.deinit(alloc);
    var payload_meta_a = try artifact_store.put("xy");
    defer payload_meta_a.deinit(alloc);
    var payload_meta_b = try artifact_store.put("zz");
    defer payload_meta_b.deinit(alloc);

    var cache = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 101,
        .max_payload_bytes = 50,
    });
    defer cache.deinit();

    const header = try cache.getBlockOrFetchRangeAlloc(&artifact_store, routing_meta.artifact_id, "vector-header", 0, 3);
    defer alloc.free(header);
    try std.testing.expectEqualStrings("abc", header);

    const payload_a = try cache.getBlockOrFetchRangeAlloc(&artifact_store, payload_meta_a.artifact_id, "vector-cluster-0-exact", 0, 2);
    defer alloc.free(payload_a);
    try std.testing.expectEqualStrings("xy", payload_a);

    const payload_b = try cache.getBlockOrFetchRangeAlloc(&artifact_store, payload_meta_b.artifact_id, "vector-cluster-1-exact", 0, 2);
    defer alloc.free(payload_b);
    try std.testing.expectEqualStrings("zz", payload_b);

    try artifact_store.delete(routing_meta.artifact_id);
    const header_again = try cache.getBlockOrFetchRangeAlloc(&artifact_store, routing_meta.artifact_id, "vector-header", 0, 3);
    defer alloc.free(header_again);
    try std.testing.expectEqualStrings("abc", header_again);

    const stats = cache.statsSnapshot();
    try std.testing.expect(stats.evictions >= 1);
    try std.testing.expectEqual(@as(u64, 1), stats.routing_block_hits);
    try std.testing.expectEqual(@as(u64, 2), stats.payload_block_misses);
    try std.testing.expectEqual(@as(u64, 2), stats.payload_block_writes);
    try std.testing.expectEqual(@as(u64, 1), stats.pinned_block_count);
    try std.testing.expect(stats.payload_block_count <= 1);
}

test "serverless query cache evicts colder payload blocks before hotter ones" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-block-temp");
    const cache_root = tmpPath(&cache_root_buf, "cache-block-temp");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var cold_meta = try artifact_store.put("cold");
    defer cold_meta.deinit(alloc);
    var hot_meta = try artifact_store.put("heat");
    defer hot_meta.deinit(alloc);
    var incoming_meta = try artifact_store.put("zzzz");
    defer incoming_meta.deinit(alloc);

    var cache = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 104,
        .max_payload_bytes = 104,
    });
    defer cache.deinit();

    const cold_first = try cache.getBlockOrFetchRangeAlloc(&artifact_store, cold_meta.artifact_id, "vector-cluster-0-exact", 0, 4);
    defer alloc.free(cold_first);
    const hot_first = try cache.getBlockOrFetchRangeAlloc(&artifact_store, hot_meta.artifact_id, "vector-cluster-1-exact", 0, 4);
    defer alloc.free(hot_first);

    const cold_path = try blockCachePathAlloc(alloc, std.mem.span(cache_root), cold_meta.artifact_id, "vector-cluster-0-exact", 0, 4, .payload);
    defer alloc.free(cold_path);
    const hot_path = try blockCachePathAlloc(alloc, std.mem.span(cache_root), hot_meta.artifact_id, "vector-cluster-1-exact", 0, 4, .payload);
    defer alloc.free(hot_path);

    try setFileModifyTimestamp(cold_path, 1);
    try setFileModifyTimestamp(hot_path, 2);

    const hot_again = try cache.getBlockOrFetchRangeAlloc(&artifact_store, hot_meta.artifact_id, "vector-cluster-1-exact", 0, 4);
    defer alloc.free(hot_again);

    try setFileModifyTimestamp(cold_path, 1);

    const incoming = try cache.getBlockOrFetchRangeAlloc(&artifact_store, incoming_meta.artifact_id, "vector-cluster-2-exact", 0, 4);
    defer alloc.free(incoming);

    try artifact_store.delete(hot_meta.artifact_id);
    const hot_cached = try cache.getBlockOrFetchRangeAlloc(&artifact_store, hot_meta.artifact_id, "vector-cluster-1-exact", 0, 4);
    defer alloc.free(hot_cached);
    try std.testing.expectEqualStrings("heat", hot_cached);

    try artifact_store.delete(cold_meta.artifact_id);
    try std.testing.expectError(
        error.FileNotFound,
        cache.getBlockOrFetchRangeAlloc(&artifact_store, cold_meta.artifact_id, "vector-cluster-0-exact", 0, 4),
    );

    const stats = cache.statsSnapshot();
    try std.testing.expect(stats.evictions >= 1);
    try std.testing.expectEqual(@as(u64, 3), stats.payload_block_misses);
    try std.testing.expect(stats.payload_block_hits >= 1);
}

test "serverless query cache does not count range entries as payload blocks during eviction" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-range-accounting");
    const cache_root = tmpPath(&cache_root_buf, "cache-range-accounting");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();
    var range_meta = try artifact_store.put("rang");
    defer range_meta.deinit(alloc);
    var block_meta = try artifact_store.put("blok");
    defer block_meta.deinit(alloc);
    var incoming_meta = try artifact_store.put("next");
    defer incoming_meta.deinit(alloc);
    var cache = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 104,
        .max_payload_bytes = 104,
    });
    defer cache.deinit();

    const range = try cache.getRangeOrFetchAlloc(&artifact_store, range_meta.artifact_id, 0, 4);
    defer alloc.free(range);
    const block = try cache.getBlockOrFetchRangeAlloc(&artifact_store, block_meta.artifact_id, "vector-cluster-0-exact", 0, 4);
    defer alloc.free(block);
    const range_path = try rangeCachePathAlloc(alloc, std.mem.span(cache_root), range_meta.artifact_id, 0, 4);
    defer alloc.free(range_path);
    const block_path = try blockCachePathAlloc(
        alloc,
        std.mem.span(cache_root),
        block_meta.artifact_id,
        "vector-cluster-0-exact",
        0,
        4,
        .payload,
    );
    defer alloc.free(block_path);
    try setFileModifyTimestamp(range_path, 1);
    try setFileModifyTimestamp(block_path, 2);

    const incoming = try cache.getBlockOrFetchRangeAlloc(&artifact_store, incoming_meta.artifact_id, "vector-cluster-1-exact", 0, 4);
    defer alloc.free(incoming);
    try std.testing.expect(!fileExists(range_path));
    try std.testing.expect(fileExists(block_path));
    try std.testing.expectEqual(@as(u64, 2), cache.statsSnapshot().payload_block_count);
}

test "serverless query cache prefers evicting exact payload blocks before approximate ones" {
    const alloc = std.testing.allocator;
    var artifact_root_buf: [256]u8 = undefined;
    var cache_root_buf: [256]u8 = undefined;
    const artifact_root = tmpPath(&artifact_root_buf, "artifacts-block-class");
    const cache_root = tmpPath(&cache_root_buf, "cache-block-class");
    defer cleanupTmp(artifact_root);
    defer cleanupTmp(cache_root);

    var fs_artifacts = try artifacts_mod.FsStore.init(alloc, std.mem.span(artifact_root));
    var artifact_store = fs_artifacts.artifactStore();
    defer artifact_store.deinit();

    var approx_meta = try artifact_store.put("apx1");
    defer approx_meta.deinit(alloc);
    var exact_meta = try artifact_store.put("ext1");
    defer exact_meta.deinit(alloc);
    var incoming_meta = try artifact_store.put("new1");
    defer incoming_meta.deinit(alloc);

    var cache = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 104,
        .max_payload_bytes = 104,
    });
    defer cache.deinit();

    const approx = try cache.getBlockOrFetchRangeAlloc(&artifact_store, approx_meta.artifact_id, "vector-cluster-0-quantized", 0, 4);
    defer alloc.free(approx);
    const exact = try cache.getBlockOrFetchRangeAlloc(&artifact_store, exact_meta.artifact_id, "vector-cluster-0-exact", 0, 4);
    defer alloc.free(exact);

    const approx_path = try blockCachePathAlloc(alloc, std.mem.span(cache_root), approx_meta.artifact_id, "vector-cluster-0-quantized", 0, 4, .payload);
    defer alloc.free(approx_path);
    const exact_path = try blockCachePathAlloc(alloc, std.mem.span(cache_root), exact_meta.artifact_id, "vector-cluster-0-exact", 0, 4, .payload);
    defer alloc.free(exact_path);
    try setFileModifyTimestamp(approx_path, 1);
    try setFileModifyTimestamp(exact_path, 1);

    const incoming = try cache.getBlockOrFetchRangeAlloc(&artifact_store, incoming_meta.artifact_id, "vector-cluster-1-exact", 0, 4);
    defer alloc.free(incoming);

    try artifact_store.delete(approx_meta.artifact_id);
    const approx_cached = try cache.getBlockOrFetchRangeAlloc(&artifact_store, approx_meta.artifact_id, "vector-cluster-0-quantized", 0, 4);
    defer alloc.free(approx_cached);
    try std.testing.expectEqualStrings("apx1", approx_cached);

    try artifact_store.delete(exact_meta.artifact_id);
    try std.testing.expectError(
        error.FileNotFound,
        cache.getBlockOrFetchRangeAlloc(&artifact_store, exact_meta.artifact_id, "vector-cluster-0-exact", 0, 4),
    );

    const stats = cache.statsSnapshot();
    try std.testing.expect(stats.evictions >= 1);
    try std.testing.expectEqual(@as(u64, 1), stats.approx_payload_block_hits);
    try std.testing.expectEqual(@as(u64, 2), stats.exact_payload_block_misses);
}

fn nowNs() u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn overwriteFile(path: []const u8, contents: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var file = try fs_paths.createFilePortable(io_impl.io(), path, .{ .truncate = true });
    defer file.close(io_impl.io());
    var buffer: [128]u8 = undefined;
    var writer = file.writer(io_impl.io(), &buffer);
    try writer.interface.writeAll(contents);
    try writer.end();
}

fn expectSliceInBuffer(slice: []const u8, buffer: []const u8) !void {
    const buffer_start = @intFromPtr(buffer.ptr);
    const buffer_end = buffer_start + buffer.len;
    const slice_start = @intFromPtr(slice.ptr);
    try std.testing.expect(slice_start >= buffer_start);
    try std.testing.expect(slice_start + slice.len <= buffer_end);
}

fn tmpPath(buf: []u8, label: []const u8) [*:0]const u8 {
    const n = nonce.fetchAdd(1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "/tmp/antfly-serverless-query-cache-{s}-{d}-{d}\x00", .{ label, nowNs(), n }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
