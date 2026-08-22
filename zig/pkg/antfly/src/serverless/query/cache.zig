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
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../../common/cancellation.zig").CancellationToken;
const fs_paths = @import("../../common/fs_paths.zig");
const artifacts_mod = @import("../artifacts/mod.zig");

const cache_record_magic = "AFQCR001";
const cache_record_digest_len = std.crypto.hash.sha2.Sha256.digest_length;
const cache_record_header_len = cache_record_magic.len + @sizeOf(u64) + cache_record_digest_len;

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

fn classifyBlockId(block_id: []const u8) BlockClass {
    if (std.mem.eql(u8, block_id, "vector-header")) return .routing;
    if (std.mem.eql(u8, block_id, "vector-table")) return .routing;
    if (std.mem.eql(u8, block_id, "sparse-header")) return .routing;
    if (std.mem.eql(u8, block_id, "sparse-docs")) return .routing;
    if (std.mem.eql(u8, block_id, "sparse-table")) return .routing;
    if (std.mem.eql(u8, block_id, "sparse-terms")) return .routing;
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

pub const QueryCache = struct {
    alloc: Allocator,
    root_dir: []u8,
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
        try cleanupInterruptedCacheWrites(alloc, root_dir);
        var cache = QueryCache{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
            .cfg = cfg,
            .stats = .{
                .max_bytes = cfg.max_bytes,
                .max_payload_bytes = cfg.max_payload_bytes,
            },
        };
        errdefer cache.deinit();
        cache.usage = try cacheUsageNoLock(alloc, root_dir, .none);
        applyUsageToStats(&cache.stats, cache.usage);
        try enforceStartupCapacity(&cache);
        return cache;
    }

    pub fn deinit(self: *QueryCache) void {
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
        try cancellation.check();
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

        const contents = try artifacts.getRangeAllocWithCancellationUsingAllocator(result_alloc, artifact_id, offset, len, cancellation);
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
        try cancellation.check();
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

        const contents = try artifacts.getRangeAllocWithCancellationUsingAllocator(result_alloc, artifact_id, offset, len, cancellation);
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

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
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

fn writeTempFileWithCancellation(
    alloc: Allocator,
    path: []const u8,
    contents: []const u8,
    lane: CacheWriteLane,
    cancellation: CancellationToken,
) ![]u8 {
    try cancellation.check();
    try ensureParentDir(path);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-query-cache-{d}", .{ path, nonce.fetchAdd(1, .monotonic) });
    errdefer alloc.free(tmp_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    errdefer deleteFilePath(io, tmp_path) catch {};
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
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
    return tmp_path;
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
    const tmp_path = try writeTempFileWithCancellation(self.alloc, path, contents, lane, cancellation);
    defer self.alloc.free(tmp_path);
    defer {
        var cleanup_io = threadedIo();
        defer cleanup_io.deinit();
        deleteFilePath(cleanup_io.io(), tmp_path) catch {};
    }

    try lockAtomicWithCancellation(&self.maintenance_mu, cancellation);
    defer self.maintenance_mu.unlock();
    if (fileExists(path)) return false;
    if (!try ensureCapacityForWrite(self, incoming_bytes, lane, cancellation)) {
        recordBypass(self);
        return false;
    }
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try renameFilePath(io_impl.io(), tmp_path, path);
    addUsage(&self.usage, incoming_bytes, lane);
    recordUsage(self);
    return true;
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

fn cacheUsageNoLock(alloc: Allocator, root_dir: []const u8, cancellation: CancellationToken) !CacheUsage {
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
        if (isTemporaryCachePath(entry.path)) continue;
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        usage.total_bytes += stat.size;
        if (std.mem.startsWith(u8, entry.path, ".blocks-pinned/")) {
            usage.pinned_bytes += stat.size;
            usage.pinned_block_count += 1;
        } else {
            usage.payload_bytes += stat.size;
            if (std.mem.startsWith(u8, entry.path, ".blocks/")) {
                usage.payload_block_count += 1;
            }
        }
    }
    return usage;
}

fn cleanupInterruptedCacheWrites(alloc: Allocator, root_dir: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer dir.close(io_impl.io());
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file or !isTemporaryCachePath(entry.path)) continue;
        const path = try std.fs.path.join(alloc, &.{ root_dir, entry.path });
        defer alloc.free(path);
        deleteFilePath(io_impl.io(), path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };
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

fn recordUsage(self: *QueryCache) void {
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
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
    try cancellation.check();
    var io_impl = threadedIo();
    defer io_impl.deinit();
    deleteFilePath(io_impl.io(), path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    self.usage = try cacheUsageNoLock(self.alloc, self.root_dir, cancellation);
    lockAtomic(&self.stats_mu);
    defer self.stats_mu.unlock();
    self.stats.integrity_failures += 1;
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
        if (isTemporaryCachePath(entry.path)) continue;
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
        if (isTemporaryCachePath(entry.path)) continue;
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
        if (entry.kind != .file or isTemporaryCachePath(entry.path)) continue;
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

test "serverless query cache removes interrupted writes during startup" {
    const alloc = std.testing.allocator;
    var cache_root_buf: [256]u8 = undefined;
    const cache_root = tmpPath(&cache_root_buf, "cache-startup-temp");
    defer cleanupTmp(cache_root);

    var initial = try QueryCache.init(alloc, std.mem.span(cache_root));
    initial.deinit();
    const orphan_path = try std.fs.path.join(alloc, &.{ std.mem.span(cache_root), "orphan.tmp-query-cache-42" });
    defer alloc.free(orphan_path);
    try overwriteFile(orphan_path, "incomplete");
    try std.testing.expect(fileExists(orphan_path));

    var recovered = try QueryCache.init(alloc, std.mem.span(cache_root));
    defer recovered.deinit();
    try std.testing.expect(!fileExists(orphan_path));
    try std.testing.expectEqual(@as(u64, 0), recovered.statsSnapshot().current_bytes);
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
