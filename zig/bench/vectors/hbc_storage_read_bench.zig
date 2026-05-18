// Copyright 2026 Antfly, Inc.
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
const antfly = @import("antfly-zig");

const Allocator = std.mem.Allocator;
const Namespace = antfly.storage_backend.Namespace;
const platform_time = antfly.platform_time;

const bench_namespace: Namespace = .{ .name = "docs" };

const Config = struct {
    docs: usize = 75_000,
    dims: usize = 512,
    queries: usize = 1000,
    candidates: usize = 800,
    batch_size: usize = 1024,
    flush_threshold: usize = 512,
    cache_bytes: usize = antfly.lsm_backend.DefaultCacheSizeBytes,
    storage: StorageMode = .memory,
};

const StorageMode = enum { memory, disk };

const KeySet = struct {
    metadata: [][]u8,
    artifacts: [][]u8,
    docs: [][]u8,

    fn deinit(self: *KeySet, alloc: Allocator) void {
        for (self.metadata) |key| alloc.free(key);
        for (self.artifacts) |key| alloc.free(key);
        for (self.docs) |key| alloc.free(key);
        alloc.free(self.metadata);
        alloc.free(self.artifacts);
        alloc.free(self.docs);
        self.* = undefined;
    }
};

const QuerySet = struct {
    metadata_flat: [][]const u8,
    artifact_flat: [][]const u8,
    queries: usize,
    candidates: usize,

    fn metadata(self: QuerySet, query: usize) []const []const u8 {
        const start = query * self.candidates;
        return self.metadata_flat[start..][0..self.candidates];
    }

    fn artifacts(self: QuerySet, query: usize) []const []const u8 {
        const start = query * self.candidates;
        return self.artifact_flat[start..][0..self.candidates];
    }

    fn deinit(self: *QuerySet, alloc: Allocator) void {
        alloc.free(self.metadata_flat);
        alloc.free(self.artifact_flat);
        self.* = undefined;
    }
};

const TimedResult = struct {
    total_ns: u64,
    hits: usize,
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    const cfg = try parseArgs(alloc, init.minimal.args);

    const root_dir = try std.fmt.allocPrint(alloc, "/tmp/hbc-storage-read-bench-{d}", .{nanotime()});
    defer alloc.free(root_dir);
    cleanupBenchDir(init.io, root_dir);
    defer cleanupBenchDir(init.io, root_dir);

    var memory_storage: antfly.lsm_backend.MemoryStorage = undefined;
    var native_storage: antfly.lsm_backend.storage_io.NativeStorage = undefined;
    var memory_storage_initialized = false;
    var native_storage_initialized = false;
    defer if (memory_storage_initialized) memory_storage.deinit();
    defer if (native_storage_initialized) native_storage.deinit();

    const selected_storage = switch (cfg.storage) {
        .memory => blk: {
            memory_storage = antfly.lsm_backend.MemoryStorage.init(alloc);
            memory_storage_initialized = true;
            break :blk memory_storage.storage();
        },
        .disk => blk: {
            native_storage = try antfly.lsm_backend.storage_io.NativeStorage.init(alloc, .threaded);
            native_storage_initialized = true;
            break :blk native_storage.storage();
        },
    };

    var cache = antfly.lsm_backend.Cache.init(alloc, cfg.cache_bytes);
    defer cache.deinit();

    var backend = try antfly.lsm_backend.Backend.open(alloc, root_dir, .{
        .backend = .{ .create_if_missing = true },
        .storage = selected_storage,
        .cache = &cache,
        .flush_threshold = cfg.flush_threshold,
        .bloom = .{},
        .io_runtime = .threaded,
    });
    defer backend.close();

    var store = try backend.runtimeStore(alloc, bench_namespace);
    defer store.deinit();

    var keys = try makeKeys(alloc, cfg.docs);
    defer keys.deinit(alloc);

    try populate(alloc, &store, cfg, keys);
    try backend.sync(true);

    var queries = try makeQueries(alloc, keys, cfg);
    defer queries.deinit(alloc);

    try warm(alloc, &store, queries);

    const metadata_batch = try timeGetMany(alloc, &store, queries, .metadata);
    const artifact_batch = try timeGetMany(alloc, &store, queries, .artifact);
    const metadata_point = try timePointGets(&store, queries, .metadata);
    const artifact_point = try timePointGets(&store, queries, .artifact);

    const read_stats = backend.snapshotReadStats();
    const cache_stats = backend.snapshotCacheStats() orelse antfly.lsm_backend.cache.Stats{};

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    try out.print(
        "hbc_storage_read_bench engine=zig_lsm storage={s} docs={d} dims={d} queries={d} candidates={d} artifact_bytes={d}\n",
        .{ @tagName(cfg.storage), cfg.docs, cfg.dims, cfg.queries, cfg.candidates, artifactValueSize(cfg.dims) },
    );
    try printTimed(out, "metadata_get_many", metadata_batch, cfg);
    try printTimed(out, "artifact_get_many", artifact_batch, cfg);
    try printTimed(out, "metadata_point_get", metadata_point, cfg);
    try printTimed(out, "artifact_point_get", artifact_point, cfg);
    try out.print(
        "hbc_storage_read_lsm_stats get_many_calls={d} get_many_keys={d} point_gets={d} get_many_hits={d} get_many_misses={d} run_probes={d} bloom_negatives={d} mutable_hits={d} l0_hits={d} level_hits={d} cache_used_mb={d:.2} block_hits={d} block_misses={d} index_hits={d} index_misses={d} run_state_hits={d} run_state_misses={d}\n",
        .{
            read_stats.get_many_sorted_calls,
            read_stats.get_many_sorted_keys,
            read_stats.point_gets,
            read_stats.get_many_sorted_hits,
            read_stats.get_many_sorted_misses,
            read_stats.run_probes,
            read_stats.bloom_negatives,
            read_stats.mutable_hits,
            read_stats.l0_hits,
            read_stats.level_hits,
            bytesToMiB(cache_stats.used_bytes),
            cache_stats.run_table_block.hits,
            cache_stats.run_table_block.misses,
            cache_stats.run_table_index.hits,
            cache_stats.run_table_index.misses,
            cache_stats.run_state.hits,
            cache_stats.run_state.misses,
        },
    );
    try out.flush();
}

const ReadKind = enum { metadata, artifact };

fn printTimed(out: *std.Io.Writer, name: []const u8, result: TimedResult, cfg: Config) !void {
    const reads = cfg.queries * cfg.candidates;
    try out.print(
        "hbc_storage_read_result op={s} total_ms={d:.3} per_query_us={d:.3} per_key_ns={d:.2} hits={d}\n",
        .{
            name,
            nsToMs(result.total_ns),
            nsToUs(result.total_ns / @max(@as(u64, 1), @as(u64, @intCast(cfg.queries)))),
            @as(f64, @floatFromInt(result.total_ns)) / @as(f64, @floatFromInt(@max(@as(usize, 1), reads))),
            result.hits,
        },
    );
}

fn populate(alloc: Allocator, store: *antfly.storage_backend_erased.Store, cfg: Config, keys: KeySet) !void {
    const artifact_value = try alloc.alloc(u8, artifactValueSize(cfg.dims));
    defer alloc.free(artifact_value);
    @memset(artifact_value, 0);

    var offset: usize = 0;
    while (offset < cfg.docs) {
        var txn = try store.beginWrite();
        errdefer txn.abort();
        const end = @min(offset + cfg.batch_size, cfg.docs);
        for (offset..end) |i| {
            writeU64(artifact_value[0..8], @intCast(i));
            try txn.put(keys.metadata[i], keys.docs[i]);
            try txn.put(keys.artifacts[i], artifact_value);
        }
        try txn.commit();
        offset = end;
    }
}

fn warm(alloc: Allocator, store: *antfly.storage_backend_erased.Store, queries: QuerySet) !void {
    _ = try timeGetMany(alloc, store, queries, .metadata);
    _ = try timeGetMany(alloc, store, queries, .artifact);
}

fn timeGetMany(alloc: Allocator, store: *antfly.storage_backend_erased.Store, queries: QuerySet, kind: ReadKind) !TimedResult {
    const values = try alloc.alloc(?[]const u8, queries.candidates);
    defer alloc.free(values);

    var hits: usize = 0;
    const started = nanotime();
    for (0..queries.queries) |query_index| {
        @memset(values, null);
        var txn = try store.beginProbe();
        defer txn.abort();
        const batch = switch (kind) {
            .metadata => queries.metadata(query_index),
            .artifact => queries.artifacts(query_index),
        };
        try txn.getManySorted(batch, values);
        for (values) |value| {
            if (value != null) hits += 1;
        }
    }
    return .{ .total_ns = elapsedSince(started), .hits = hits };
}

fn timePointGets(store: *antfly.storage_backend_erased.Store, queries: QuerySet, kind: ReadKind) !TimedResult {
    var hits: usize = 0;
    const started = nanotime();
    for (0..queries.queries) |query_index| {
        var txn = try store.beginProbe();
        defer txn.abort();
        const batch = switch (kind) {
            .metadata => queries.metadata(query_index),
            .artifact => queries.artifacts(query_index),
        };
        for (batch) |key| {
            _ = txn.get(key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
            hits += 1;
        }
    }
    return .{ .total_ns = elapsedSince(started), .hits = hits };
}

fn makeKeys(alloc: Allocator, docs: usize) !KeySet {
    var metadata = try alloc.alloc([]u8, docs);
    errdefer alloc.free(metadata);
    var artifacts = try alloc.alloc([]u8, docs);
    errdefer alloc.free(artifacts);
    var doc_keys = try alloc.alloc([]u8, docs);
    errdefer alloc.free(doc_keys);

    var initialized: usize = 0;
    errdefer {
        for (metadata[0..initialized]) |key| alloc.free(key);
        for (artifacts[0..initialized]) |key| alloc.free(key);
        for (doc_keys[0..initialized]) |key| alloc.free(key);
    }

    for (0..docs) |i| {
        metadata[i] = try std.fmt.allocPrint(alloc, "__hbc_meta__:dense_idx:{d:0>16}", .{i});
        artifacts[i] = try std.fmt.allocPrint(alloc, "__embedding__:doc:{d:0>16}:dense_idx", .{i});
        doc_keys[i] = try std.fmt.allocPrint(alloc, "doc:{d:0>16}", .{i});
        initialized += 1;
    }
    return .{ .metadata = metadata, .artifacts = artifacts, .docs = doc_keys };
}

fn makeQueries(alloc: Allocator, keys: KeySet, cfg: Config) !QuerySet {
    const total = try std.math.mul(usize, cfg.queries, cfg.candidates);
    var metadata_flat = try alloc.alloc([]const u8, total);
    errdefer alloc.free(metadata_flat);
    var artifact_flat = try alloc.alloc([]const u8, total);
    errdefer alloc.free(artifact_flat);
    const ids = try alloc.alloc(usize, cfg.candidates);
    defer alloc.free(ids);

    for (0..cfg.queries) |query_index| {
        for (ids, 0..) |*id, candidate_index| {
            id.* = pickDocId(query_index, candidate_index, cfg.docs);
        }
        std.mem.sort(usize, ids, {}, comptime std.sort.asc(usize));
        const start = query_index * cfg.candidates;
        for (ids, 0..) |id, j| {
            metadata_flat[start + j] = keys.metadata[id];
            artifact_flat[start + j] = keys.artifacts[id];
        }
    }
    return .{
        .metadata_flat = metadata_flat,
        .artifact_flat = artifact_flat,
        .queries = cfg.queries,
        .candidates = cfg.candidates,
    };
}

fn pickDocId(query_index: usize, candidate_index: usize, docs: usize) usize {
    if (docs == 0) return 0;
    var x: u64 = @as(u64, @intCast(query_index + 1)) *% 0x9e3779b185ebca87;
    x ^= @as(u64, @intCast(candidate_index + 17)) *% 0xc2b2ae3d27d4eb4f;
    x ^= x >> 33;
    x *%= 0xff51afd7ed558ccd;
    x ^= x >> 33;
    return @intCast(x % @as(u64, @intCast(docs)));
}

fn artifactValueSize(dims: usize) usize {
    return 16 + dims * @sizeOf(f32);
}

fn writeU64(dst: []u8, value: u64) void {
    std.mem.writeInt(u64, dst[0..8], value, .little);
}

fn parseArgs(alloc: Allocator, proc_args: std.process.Args) !Config {
    var cfg = Config{};
    var args = try std.process.Args.Iterator.initAllocator(proc_args, alloc);
    defer args.deinit();
    _ = args.next();
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--docs")) {
            cfg.docs = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--queries")) {
            cfg.queries = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--candidates")) {
            cfg.candidates = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--batch-size")) {
            cfg.batch_size = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--flush-threshold")) {
            cfg.flush_threshold = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--cache-bytes")) {
            cfg.cache_bytes = try parseNextUsize(&args, arg);
        } else if (std.mem.eql(u8, arg, "--storage")) {
            const value = args.next() orelse {
                std.debug.print("missing value for {s}\n", .{arg});
                return error.InvalidArgument;
            };
            cfg.storage = parseStorageMode(value) orelse return error.InvalidArgument;
        } else {
            std.debug.print("unknown argument: {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }
    if (cfg.docs == 0 or cfg.candidates == 0 or cfg.queries == 0) return error.InvalidArgument;
    return cfg;
}

fn parseNextUsize(args: anytype, flag: []const u8) !usize {
    const value = args.next() orelse {
        std.debug.print("missing value for {s}\n", .{flag});
        return error.InvalidArgument;
    };
    return std.fmt.parseUnsigned(usize, value, 10);
}

fn parseStorageMode(value: []const u8) ?StorageMode {
    if (std.mem.eql(u8, value, "memory")) return .memory;
    if (std.mem.eql(u8, value, "disk")) return .disk;
    return null;
}

fn cleanupBenchDir(io: std.Io, path: []const u8) void {
    std.Io.Dir.cwd().deleteTree(io, path) catch {};
}

fn nanotime() u64 {
    return platform_time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return nanotime() - start_ns;
}

fn nsToUs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000.0;
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1_000_000.0;
}

fn bytesToMiB(bytes: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}
