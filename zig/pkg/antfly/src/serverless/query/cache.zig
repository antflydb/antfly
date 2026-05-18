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
const Allocator = std.mem.Allocator;
const fs_paths = @import("../../common/fs_paths.zig");
const artifacts_mod = @import("../artifacts/mod.zig");

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

const EvictableFile = struct {
    path: []u8,
    size: u64,
    last_access_ns: i128,
    payload_block_class: PayloadBlockClass,
};

pub const QueryCache = struct {
    alloc: Allocator,
    root_dir: []u8,
    cfg: QueryCacheConfig,
    mu: std.atomic.Mutex = .unlocked,
    stats: QueryCacheStats = .{},

    pub fn init(alloc: Allocator, root_dir: []const u8) !QueryCache {
        return try initWithConfig(alloc, root_dir, .{});
    }

    pub fn initWithConfig(alloc: Allocator, root_dir: []const u8, cfg: QueryCacheConfig) !QueryCache {
        var io_impl = threadedIo();
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), root_dir);
        return .{
            .alloc = alloc,
            .root_dir = try alloc.dupe(u8, root_dir),
            .cfg = cfg,
            .stats = .{
                .max_bytes = cfg.max_bytes,
                .max_payload_bytes = cfg.max_payload_bytes,
            },
        };
    }

    pub fn deinit(self: *QueryCache) void {
        self.alloc.free(self.root_dir);
        self.* = undefined;
    }

    pub fn statsSnapshot(self: *QueryCache) QueryCacheStats {
        lockAtomic(&self.mu);
        defer self.mu.unlock();
        return self.stats;
    }

    pub fn getOrFetchAlloc(self: *QueryCache, artifacts: *artifacts_mod.ArtifactStore, artifact_id: []const u8) ![]u8 {
        const path = try cachePathAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(path);

        const cached = readFileAlloc(self.alloc, path) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (cached) |value| {
            touchFileNow(path) catch {};
            lockAtomic(&self.mu);
            defer self.mu.unlock();
            self.stats.hits += 1;
            self.stats.full_hits += 1;
            try refreshUsageStatsNoLock(self);
            return value;
        }

        const contents = try artifacts.getAlloc(artifact_id);
        errdefer self.alloc.free(contents);
        try ensureParentDir(path);

        lockAtomic(&self.mu);
        defer self.mu.unlock();

        self.stats.misses += 1;
        self.stats.full_misses += 1;
        try ensureCapacityNoLock(self, @intCast(contents.len), .full);
        try ensureParentDir(path);
        try writeFileAtomically(path, contents);
        self.stats.writes += 1;
        self.stats.full_writes += 1;
        try refreshUsageStatsNoLock(self);
        return contents;
    }

    pub fn getRangeOrFetchAlloc(
        self: *QueryCache,
        artifacts: *artifacts_mod.ArtifactStore,
        artifact_id: []const u8,
        offset: u64,
        len: usize,
    ) ![]u8 {
        const full_path = try cachePathAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(full_path);

        const full_cached = readFileAlloc(self.alloc, full_path) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (full_cached) |value| {
            defer self.alloc.free(value);
            if (offset <= value.len) {
                const start: usize = @intCast(offset);
                const end = @min(value.len, start + len);
                touchFileNow(full_path) catch {};
                lockAtomic(&self.mu);
                defer self.mu.unlock();
                self.stats.hits += 1;
                self.stats.range_hits += 1;
                try refreshUsageStatsNoLock(self);
                return try self.alloc.dupe(u8, value[start..end]);
            }
        }

        const range_path = try rangeCachePathAlloc(self.alloc, self.root_dir, artifact_id, offset, len);
        defer self.alloc.free(range_path);
        const cached = readFileAlloc(self.alloc, range_path) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (cached) |value| {
            touchFileNow(range_path) catch {};
            lockAtomic(&self.mu);
            defer self.mu.unlock();
            self.stats.hits += 1;
            self.stats.range_hits += 1;
            try refreshUsageStatsNoLock(self);
            return value;
        }

        const contents = try artifacts.getRangeAlloc(artifact_id, offset, len);
        errdefer self.alloc.free(contents);
        try ensureParentDir(range_path);

        lockAtomic(&self.mu);
        defer self.mu.unlock();

        self.stats.misses += 1;
        self.stats.range_misses += 1;
        try ensureCapacityNoLock(self, @intCast(contents.len), .range);
        try ensureParentDir(range_path);
        try writeFileAtomically(range_path, contents);
        self.stats.writes += 1;
        self.stats.range_writes += 1;
        try refreshUsageStatsNoLock(self);
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
        const block_class = classifyBlockId(block_id);
        const payload_block_class = classifyPayloadBlockId(block_id);
        const full_path = try cachePathAlloc(self.alloc, self.root_dir, artifact_id);
        defer self.alloc.free(full_path);

        const full_cached = readFileAlloc(self.alloc, full_path) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (full_cached) |value| {
            defer self.alloc.free(value);
            if (offset <= value.len) {
                const start: usize = @intCast(offset);
                const end = @min(value.len, start + len);
                touchFileNow(full_path) catch {};
                lockAtomic(&self.mu);
                defer self.mu.unlock();
                self.stats.hits += 1;
                self.stats.block_hits += 1;
                incrementBlockClassHit(&self.stats, block_class);
                incrementPayloadBlockClassHit(&self.stats, payload_block_class);
                try refreshUsageStatsNoLock(self);
                return try self.alloc.dupe(u8, value[start..end]);
            }
        }

        const block_path = try blockCachePathAlloc(self.alloc, self.root_dir, artifact_id, block_id, block_class);
        defer self.alloc.free(block_path);
        const cached = readFileAlloc(self.alloc, block_path) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        if (cached) |value| {
            touchFileNow(block_path) catch {};
            lockAtomic(&self.mu);
            defer self.mu.unlock();
            self.stats.hits += 1;
            self.stats.block_hits += 1;
            incrementBlockClassHit(&self.stats, block_class);
            incrementPayloadBlockClassHit(&self.stats, payload_block_class);
            try refreshUsageStatsNoLock(self);
            return value;
        }

        const contents = try artifacts.getRangeAlloc(artifact_id, offset, len);
        errdefer self.alloc.free(contents);
        try ensureParentDir(block_path);

        lockAtomic(&self.mu);
        defer self.mu.unlock();

        self.stats.misses += 1;
        self.stats.block_misses += 1;
        incrementBlockClassMiss(&self.stats, block_class);
        incrementPayloadBlockClassMiss(&self.stats, payload_block_class);
        try ensureCapacityNoLock(self, @intCast(contents.len), switch (block_class) {
            .routing => .routing_block,
            .payload => .payload_block,
        });
        try ensureParentDir(block_path);
        try writeFileAtomically(block_path, contents);
        self.stats.writes += 1;
        self.stats.block_writes += 1;
        incrementBlockClassWrite(&self.stats, block_class);
        incrementPayloadBlockClassWrite(&self.stats, payload_block_class);
        try refreshUsageStatsNoLock(self);
        return contents;
    }
};

fn threadedIo() std.Io.Threaded {
    return std.Io.Threaded.init(std.heap.page_allocator, .{});
}

fn cachePathAlloc(alloc: Allocator, root_dir: []const u8, artifact_id: []const u8) ![]u8 {
    return try std.fs.path.join(alloc, &.{ root_dir, artifact_id });
}

fn rangeCachePathAlloc(alloc: Allocator, root_dir: []const u8, artifact_id: []const u8, offset: u64, len: usize) ![]u8 {
    const suffix = try std.fmt.allocPrint(alloc, "{d}-{d}.range", .{ offset, len });
    defer alloc.free(suffix);
    return try std.fs.path.join(alloc, &.{ root_dir, ".ranges", artifact_id, suffix });
}

fn blockCachePathAlloc(
    alloc: Allocator,
    root_dir: []const u8,
    artifact_id: []const u8,
    block_id: []const u8,
    block_class: BlockClass,
) ![]u8 {
    const lane = switch (block_class) {
        .routing => ".blocks-pinned",
        .payload => ".blocks",
    };
    return try std.fs.path.join(alloc, &.{ root_dir, lane, artifact_id, block_id });
}

fn readFileAlloc(alloc: Allocator, path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    return try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(std.math.maxInt(usize)));
}

fn ensureParentDir(path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), parent);
}

var nonce: std.atomic.Value(u64) = .init(0);

fn writeFileAtomically(path: []const u8, contents: []const u8) !void {
    const tmp_path = try std.fmt.allocPrint(std.heap.page_allocator, "{s}.tmp-{d}", .{ path, nonce.fetchAdd(1, .monotonic) });
    defer std.heap.page_allocator.free(tmp_path);

    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    {
        var file = try std.Io.Dir.createFileAbsolute(io, tmp_path, .{ .truncate = true });
        defer file.close(io);
        var buf: [4096]u8 = undefined;
        var writer = file.writer(io, &buf);
        try writer.interface.writeAll(contents);
        try writer.end();
    }
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.renameAbsolute(tmp_path, path, io);
    } else {
        try std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io);
    }
}

fn touchFileNow(path: []const u8) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var file = try std.Io.Dir.cwd().openFile(io_impl.io(), path, .{});
    defer file.close(io_impl.io());
    try file.setTimestampsNow(io_impl.io());
}

fn setFileModifyTimestamp(path: []const u8, ts_ns: i96) !void {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var file = try std.Io.Dir.cwd().openFile(io_impl.io(), path, .{});
    defer file.close(io_impl.io());
    const ts = std.Io.Timestamp.fromNanoseconds(ts_ns);
    try file.setTimestamps(io_impl.io(), .{
        .access_timestamp = .{ .new = ts },
        .modify_timestamp = .{ .new = ts },
    });
}

fn ensureCapacityNoLock(self: *QueryCache, incoming_bytes: u64, lane: CacheWriteLane) !void {
    if (self.cfg.max_bytes == 0 and self.cfg.max_payload_bytes == 0) return;
    var usage = try cacheUsageNoLock(self.alloc, self.root_dir);
    if (!needsEviction(self.cfg, usage, incoming_bytes, lane)) return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var evicted_any = false;
    const payload_files = try collectEvictablePayloadFilesAlloc(self.alloc, self.root_dir);
    defer freeEvictableFiles(self.alloc, payload_files);
    std.mem.sort(EvictableFile, payload_files, {}, lessEvictableFile);

    for (payload_files) |file| {
        if (!needsEviction(self.cfg, usage, incoming_bytes, lane)) break;
        std.Io.Dir.cwd().deleteFile(io_impl.io(), file.path) catch continue;
        usage.total_bytes -= @min(usage.total_bytes, file.size);
        usage.payload_bytes -= @min(usage.payload_bytes, file.size);
        if (file.payload_block_class != .none) {
            usage.payload_block_count -= @min(usage.payload_block_count, 1);
        }
        evicted_any = true;
    }

    if (needsEviction(self.cfg, usage, incoming_bytes, lane)) {
        const root_files = try collectEvictableRootFilesAlloc(self.alloc, self.root_dir);
        defer freeEvictableFiles(self.alloc, root_files);
        std.mem.sort(EvictableFile, root_files, {}, lessEvictableFile);
        for (root_files) |file| {
            if (!needsEviction(self.cfg, usage, incoming_bytes, lane)) break;
            std.Io.Dir.cwd().deleteFile(io_impl.io(), file.path) catch continue;
            usage.total_bytes -= @min(usage.total_bytes, file.size);
            usage.payload_bytes -= @min(usage.payload_bytes, file.size);
            evicted_any = true;
        }
    }

    if (evicted_any) self.stats.evictions += 1;
    self.stats.current_bytes = usage.total_bytes;
    self.stats.pinned_bytes = usage.pinned_bytes;
    self.stats.payload_bytes = usage.payload_bytes;
    self.stats.pinned_block_count = usage.pinned_block_count;
    self.stats.payload_block_count = usage.payload_block_count;
}

fn needsEviction(cfg: QueryCacheConfig, usage: CacheUsage, incoming_bytes: u64, lane: CacheWriteLane) bool {
    if (cfg.max_bytes > 0 and usage.total_bytes + incoming_bytes > cfg.max_bytes) return true;
    if (cfg.max_payload_bytes > 0 and consumesPayloadBudget(lane) and usage.payload_bytes + incoming_bytes > cfg.max_payload_bytes) return true;
    return false;
}

fn consumesPayloadBudget(lane: CacheWriteLane) bool {
    return switch (lane) {
        .routing_block => false,
        .full, .range, .payload_block => true,
    };
}

fn currentBytesNoLock(alloc: Allocator, root_dir: []const u8) !u64 {
    return (try cacheUsageNoLock(alloc, root_dir)).total_bytes;
}

fn cacheUsageNoLock(alloc: Allocator, root_dir: []const u8) !CacheUsage {
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
        if (entry.kind != .file) continue;
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

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
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

fn refreshUsageStatsNoLock(self: *QueryCache) !void {
    const usage = try cacheUsageNoLock(self.alloc, self.root_dir);
    self.stats.current_bytes = usage.total_bytes;
    self.stats.pinned_bytes = usage.pinned_bytes;
    self.stats.payload_bytes = usage.payload_bytes;
    self.stats.pinned_block_count = usage.pinned_block_count;
    self.stats.payload_block_count = usage.payload_block_count;
}

fn collectEvictablePayloadFilesAlloc(alloc: Allocator, root_dir: []const u8) ![]EvictableFile {
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
        if (entry.kind != .file) continue;
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
                classifyPayloadBlockId(std.fs.path.basename(entry.path))
            else
                .approximate,
        });
    }
    return try out.toOwnedSlice(alloc);
}

fn collectEvictableRootFilesAlloc(alloc: Allocator, root_dir: []const u8) ![]EvictableFile {
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
        if (entry.kind != .file) continue;
        if (std.mem.indexOfScalar(u8, entry.path, std.fs.path.sep)) |_| continue;
        const path = try std.fs.path.join(alloc, &.{ root_dir, entry.path });
        errdefer alloc.free(path);
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        try out.append(alloc, .{
            .path = path,
            .size = stat.size,
            .last_access_ns = stat.mtime.toNanoseconds(),
            .payload_block_class = .approximate,
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

test "query cache reuses cached artifact contents" {
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

test "query cache clears cache when max bytes would be exceeded" {
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

test "query cache caches artifact ranges and can reuse full artifact cache" {
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

test "query cache caches named blocks by artifact and block id" {
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

test "query cache preserves pinned routing blocks when evicting payload blocks" {
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
    var payload_meta_b = try artifact_store.put("payload");
    defer payload_meta_b.deinit(alloc);

    var cache = try QueryCache.initWithConfig(alloc, std.mem.span(cache_root), .{
        .max_bytes = 5,
        .max_payload_bytes = 5,
    });
    defer cache.deinit();

    const header = try cache.getBlockOrFetchRangeAlloc(&artifact_store, routing_meta.artifact_id, "vector-header", 0, 3);
    defer alloc.free(header);
    try std.testing.expectEqualStrings("abc", header);

    const payload_a = try cache.getBlockOrFetchRangeAlloc(&artifact_store, payload_meta_a.artifact_id, "vector-cluster-0-exact", 0, 2);
    defer alloc.free(payload_a);
    try std.testing.expectEqualStrings("xy", payload_a);

    const payload_b = try cache.getBlockOrFetchRangeAlloc(&artifact_store, payload_meta_b.artifact_id, "vector-cluster-1-exact", 0, 7);
    defer alloc.free(payload_b);
    try std.testing.expectEqualStrings("payload", payload_b);

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

test "query cache evicts colder payload blocks before hotter ones" {
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
        .max_bytes = 8,
        .max_payload_bytes = 8,
    });
    defer cache.deinit();

    const cold_first = try cache.getBlockOrFetchRangeAlloc(&artifact_store, cold_meta.artifact_id, "vector-cluster-0-exact", 0, 4);
    defer alloc.free(cold_first);
    const hot_first = try cache.getBlockOrFetchRangeAlloc(&artifact_store, hot_meta.artifact_id, "vector-cluster-1-exact", 0, 4);
    defer alloc.free(hot_first);

    const cold_path = try blockCachePathAlloc(alloc, std.mem.span(cache_root), cold_meta.artifact_id, "vector-cluster-0-exact", .payload);
    defer alloc.free(cold_path);
    const hot_path = try blockCachePathAlloc(alloc, std.mem.span(cache_root), hot_meta.artifact_id, "vector-cluster-1-exact", .payload);
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

test "query cache prefers evicting exact payload blocks before approximate ones" {
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
        .max_bytes = 8,
        .max_payload_bytes = 8,
    });
    defer cache.deinit();

    const approx = try cache.getBlockOrFetchRangeAlloc(&artifact_store, approx_meta.artifact_id, "vector-cluster-0-quantized", 0, 4);
    defer alloc.free(approx);
    const exact = try cache.getBlockOrFetchRangeAlloc(&artifact_store, exact_meta.artifact_id, "vector-cluster-0-exact", 0, 4);
    defer alloc.free(exact);

    const approx_path = try blockCachePathAlloc(alloc, std.mem.span(cache_root), approx_meta.artifact_id, "vector-cluster-0-quantized", .payload);
    defer alloc.free(approx_path);
    const exact_path = try blockCachePathAlloc(alloc, std.mem.span(cache_root), exact_meta.artifact_id, "vector-cluster-0-exact", .payload);
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
