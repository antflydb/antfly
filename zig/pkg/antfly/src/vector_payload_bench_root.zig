// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Micro-benchmark for the table-owned source vector payload store.
//!
//! Exercises the same public surface the DB uses (Session.put / prepareCommit /
//! get, Store.checkpoint) against native on-disk storage so the write, read,
//! disk, and memory costs of the store itself can be attributed without the
//! surrounding primary LSM, ANN indexes, or HTTP layers.
//!
//!   zig build vector-payload-bench -Doptimize=ReleaseFast
//!   ./zig-out/bin/vector_payload_bench --vectors 50000 --dims 768 --batch 64 --root /tmp/vps
const std = @import("std");
const Allocator = std.mem.Allocator;
const payload_store = @import("storage/vector_payload_store.zig");
const payload = @import("storage/artifact_payload.zig");
const codec = @import("storage/db/enrichment/artifact_codec.zig");
const keys = @import("storage/internal_keys.zig");
const lsm = @import("storage/lsm_backend/mod.zig");
const time = @import("antfly_platform").time;

const Config = struct {
    vectors: usize = 20_000,
    dims: usize = 768,
    batch: usize = 64,
    reads: usize = 20_000,
    update_fraction_percent: usize = 10,
    root: []const u8 = "/tmp/vector_payload_bench",
    seed: u64 = 42,
    managed: bool = false,
    keep: bool = false,
    skip_checkpoint: bool = false,
    drop_caches: bool = false,
    rerank_batch: usize = 100,
    rerank_batches: usize = 200,
    /// Open an existing root written by a prior --keep run, skipping ingest.
    reopen_only: bool = false,
};

/// Bytes the benchmark itself keeps resident (keys, artifacts, references) so
/// process RSS can be reported net of the workload dataset.
var dataset_bytes: u64 = 0;

const vector_block = @import("antfly_vectorindex").vector_block;

fn dropCaches() bool {
    const fd = std.c.open("/proc/sys/vm/drop_caches", .{ .ACCMODE = .WRONLY }, @as(c_uint, 0));
    if (fd < 0) return false;
    defer _ = std.c.close(fd);
    return std.c.write(fd, "3", 1) == 1;
}

/// Counts live and peak bytes handed out to the store so resident metadata
/// (WAL view chunks, AVL nodes, reader arrays) can be attributed separately
/// from process RSS, which also includes mmap'd immutable blocks.
const CountingAllocator = struct {
    backing: Allocator,
    live: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),
    allocs: std.atomic.Value(u64) = .init(0),
    frees: std.atomic.Value(u64) = .init(0),
    mutex: std.atomic.Mutex = .unlocked,

    fn allocator(self: *CountingAllocator) Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    const vtable: Allocator.VTable = .{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };

    fn account(self: *CountingAllocator, delta: isize) void {
        const prev = self.live.load(.monotonic);
        const next: usize = if (delta >= 0) prev + @as(usize, @intCast(delta)) else prev - @as(usize, @intCast(-delta));
        self.live.store(next, .monotonic);
        if (next > self.peak.load(.monotonic)) self.peak.store(next, .monotonic);
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        while (!self.mutex.tryLock()) std.Thread.yield() catch {};
        defer self.mutex.unlock();
        const result = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.account(@intCast(len));
        _ = self.allocs.fetchAdd(1, .monotonic);
        return result;
    }
    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        while (!self.mutex.tryLock()) std.Thread.yield() catch {};
        defer self.mutex.unlock();
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.account(@as(isize, @intCast(new_len)) - @as(isize, @intCast(memory.len)));
        return true;
    }
    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        while (!self.mutex.tryLock()) std.Thread.yield() catch {};
        defer self.mutex.unlock();
        const result = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.account(@as(isize, @intCast(new_len)) - @as(isize, @intCast(memory.len)));
        return result;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        while (!self.mutex.tryLock()) std.Thread.yield() catch {};
        defer self.mutex.unlock();
        self.backing.rawFree(memory, alignment, ret_addr);
        self.account(-@as(isize, @intCast(memory.len)));
        _ = self.frees.fetchAdd(1, .monotonic);
    }
};

const Rss = struct {
    rss_kb: u64 = 0,
    hwm_kb: u64 = 0,

    fn read() Rss {
        var result: Rss = .{};
        const fd = std.c.open("/proc/self/status", .{ .ACCMODE = .RDONLY }, @as(c_uint, 0));
        if (fd < 0) return result;
        defer _ = std.c.close(fd);
        var buf: [16384]u8 = undefined;
        var len: usize = 0;
        while (len < buf.len) {
            const rc = std.c.read(fd, buf[len..].ptr, buf.len - len);
            if (rc <= 0) break;
            len += @intCast(rc);
        }
        var lines = std.mem.splitScalar(u8, buf[0..len], '\n');
        while (lines.next()) |line| {
            if (std.mem.startsWith(u8, line, "VmRSS:")) result.rss_kb = parseKb(line);
            if (std.mem.startsWith(u8, line, "VmHWM:")) result.hwm_kb = parseKb(line);
        }
        return result;
    }

    fn parseKb(line: []const u8) u64 {
        var it = std.mem.tokenizeAny(u8, line, " \t");
        _ = it.next();
        const value = it.next() orelse return 0;
        return std.fmt.parseInt(u64, value, 10) catch 0;
    }
};

const Timing = struct {
    samples: std.ArrayListUnmanaged(u64) = .empty,

    fn add(self: *Timing, alloc: Allocator, ns: u64) !void {
        try self.samples.append(alloc, ns);
    }
    fn percentile(self: *Timing, p: f64) u64 {
        if (self.samples.items.len == 0) return 0;
        std.mem.sortUnstable(u64, self.samples.items, {}, std.sort.asc(u64));
        const idx: usize = @intFromFloat(@as(f64, @floatFromInt(self.samples.items.len - 1)) * p);
        return self.samples.items[idx];
    }
    fn total(self: *Timing) u64 {
        var sum: u64 = 0;
        for (self.samples.items) |s| sum += s;
        return sum;
    }
};

fn ms(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e6;
}
fn us(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e3;
}
fn mib(bytes: u64) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

fn diskBytes(storage: lsm.Storage, alloc: Allocator, root: []const u8) !u64 {
    const names = try storage.listFileNamesAlloc(alloc, root);
    defer {
        for (names) |name| alloc.free(name);
        alloc.free(names);
    }
    var total: u64 = 0;
    for (names) |name| {
        const path = try std.fs.path.join(alloc, &.{ root, name });
        defer alloc.free(path);
        total += storage.fileSize(path) catch 0;
    }
    return total;
}

fn fileCount(storage: lsm.Storage, alloc: Allocator, root: []const u8) !usize {
    const names = try storage.listFileNamesAlloc(alloc, root);
    defer {
        for (names) |name| alloc.free(name);
        alloc.free(names);
    }
    return names.len;
}

fn printStats(out: *std.Io.Writer, label: []const u8, s: payload.Stats) !void {
    try out.print(
        "[{s}] prepare_batches={d} prepared_payloads={d} preparation_ms={d:.1} durable_append_ms={d:.1} lock_wait_ms={d:.1} wal_written_mib={d:.2} active_wal_mib={d:.2} checkpoint_ms={d:.1} ckpt_read_mib={d:.2} ckpt_written_mib={d:.2} immutable_mib={d:.2} segments={d} shards={d} resolved={d} loc_cache_hits={d} loc_cache_misses={d} dir_entries={d} dir_written_mib={d:.2}\n",
        .{
            label,
            s.prepare_batches,
            s.prepared_payloads,
            ms(s.preparation_ns),
            ms(s.durable_append_ns),
            ms(s.prepare_lock_wait_ns),
            mib(s.wal_bytes_written),
            mib(s.active_wal_bytes),
            ms(s.checkpoint_ns),
            mib(s.checkpoint_bytes_read),
            mib(s.checkpoint_bytes_written),
            mib(s.immutable_block_bytes),
            s.source_segments,
            s.source_shards,
            s.resolved_payloads,
            s.location_cache_hits,
            s.location_cache_misses,
            s.directory_entries,
            mib(s.directory_bytes_written),
        },
    );
}

fn printMem(out: *std.Io.Writer, label: []const u8, counting: *CountingAllocator) !void {
    const rss = Rss.read();
    try out.print(
        "[{s}] store_heap_live_mib={d:.2} store_heap_peak_mib={d:.2} allocs={d} frees={d} rss_mib={d:.1} hwm_mib={d:.1} rss_ex_dataset_mib={d:.1}\n",
        .{
            label,
            mib(counting.live.load(.monotonic)),
            mib(counting.peak.load(.monotonic)),
            counting.allocs.load(.monotonic),
            counting.frees.load(.monotonic),
            @as(f64, @floatFromInt(rss.rss_kb)) / 1024.0,
            @as(f64, @floatFromInt(rss.hwm_kb)) / 1024.0,
            @as(f64, @floatFromInt(rss.rss_kb)) / 1024.0 - mib(dataset_bytes),
        },
    );
}

fn fillVector(random: std.Random, out: []f32) void {
    for (out) |*v| v.* = random.float(f32) * 2.0 - 1.0;
}

pub fn main(init: std.process.Init) !void {
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, init.gpa);
    defer args.deinit();
    _ = args.skip();
    var cfg: Config = .{};
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--vectors")) {
            cfg.vectors = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--dims")) {
            cfg.dims = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--batch")) {
            cfg.batch = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--reads")) {
            cfg.reads = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--update-percent")) {
            cfg.update_fraction_percent = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            cfg.seed = try std.fmt.parseInt(u64, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--root")) {
            cfg.root = args.next() orelse return error.InvalidArgument;
        } else if (std.mem.eql(u8, arg, "--managed")) {
            cfg.managed = true;
        } else if (std.mem.eql(u8, arg, "--keep")) {
            cfg.keep = true;
        } else if (std.mem.eql(u8, arg, "--skip-checkpoint")) {
            cfg.skip_checkpoint = true;
        } else if (std.mem.eql(u8, arg, "--drop-caches")) {
            cfg.drop_caches = true;
        } else if (std.mem.eql(u8, arg, "--reopen-only")) {
            cfg.reopen_only = true;
            cfg.keep = true;
        } else if (std.mem.eql(u8, arg, "--rerank-batch")) {
            cfg.rerank_batch = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else if (std.mem.eql(u8, arg, "--rerank-batches")) {
            cfg.rerank_batches = try std.fmt.parseInt(usize, args.next() orelse return error.InvalidArgument, 10);
        } else {
            std.debug.print("unknown argument {s}\n", .{arg});
            return error.InvalidArgument;
        }
    }

    const alloc = std.heap.c_allocator;
    var counting: CountingAllocator = .{ .backing = alloc };
    const store_alloc = counting.allocator();

    var stdout_buffer: [8192]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const out = &stdout_writer.interface;
    defer stdout_writer.flush() catch {};

    var native = try lsm.NativeStorage.init(alloc, .threaded);
    defer native.deinit();
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const storage = native.storage();
    if (!cfg.reopen_only) {
        storage.deleteTree(cfg.root) catch {};
        try storage.createDirPath(cfg.root);
    }
    defer if (!cfg.keep) storage.deleteTree(cfg.root) catch {};

    try out.print("vector_payload_bench vectors={d} dims={d} batch={d} reads={d} update_percent={d} managed={} root={s}\n", .{ cfg.vectors, cfg.dims, cfg.batch, cfg.reads, cfg.update_fraction_percent, cfg.managed, cfg.root });
    try stdout_writer.flush();

    // Dataset: keys as the DB would generate them, random float32 artifacts.
    var rng = std.Random.DefaultPrng.init(cfg.seed);
    const random = rng.random();
    const doc_keys = try alloc.alloc([]u8, cfg.vectors);
    defer {
        for (doc_keys) |k| alloc.free(k);
        alloc.free(doc_keys);
    }
    const artifacts = try alloc.alloc([]u8, cfg.vectors);
    defer {
        for (artifacts) |a| alloc.free(a);
        alloc.free(artifacts);
    }
    const references = try alloc.alloc([payload.reference_len]u8, cfg.vectors);
    defer alloc.free(references);
    {
        const scratch = try alloc.alloc(f32, cfg.dims);
        defer alloc.free(scratch);
        for (0..cfg.vectors) |i| {
            var doc_buf: [64]u8 = undefined;
            const doc = try std.fmt.bufPrint(&doc_buf, "doc-{d:0>8}", .{i});
            doc_keys[i] = try keys.embeddingArtifactKeyForDocumentAlloc(alloc, doc, "embedding");
            fillVector(random, scratch);
            artifacts[i] = try codec.encodeDenseEmbeddingAlloc(alloc, @intCast(i + 1), scratch);
            dataset_bytes += doc_keys[i].len + artifacts[i].len + payload.reference_len;
        }
    }
    try out.print("dataset_mib={d:.2}\n", .{mib(dataset_bytes)});

    var manager: ?*@import("storage/resource_manager.zig").ResourceManager = null;
    var manager_storage: @import("storage/resource_manager.zig").ResourceManager = undefined;
    if (cfg.managed) {
        manager_storage = @import("storage/resource_manager.zig").ResourceManager.init(.{});
        manager = &manager_storage;
    }
    defer if (manager) |m| m.deinit(alloc);

    if (cfg.reopen_only) {
        // The same seed regenerates the same artifacts, so references can be
        // recomputed without the original ingest run's output.
        for (0..cfg.vectors) |i| references[i] = (try payload.Reference.forArtifact(doc_keys[i], artifacts[i])).encode();
        if (cfg.drop_caches) {
            if (!dropCaches()) try out.print("[reopen] drop_caches unavailable\n", .{});
        }
        const rss_before = Rss.read();
        const reopen_started = time.monotonicNs();
        var reopened = if (cfg.managed)
            try payload_store.Store.openManaged(store_alloc, manager, storage, cfg.root, false)
        else
            try payload_store.Store.open(store_alloc, storage, cfg.root, false);
        defer reopened.deinit();
        const reopen_ns = time.monotonicNs() -| reopen_started;
        const rss_after = Rss.read();
        const reopen_stats = reopened.statsSnapshot();
        try out.print("[reopen] drop_caches={} wall_ms={d:.1} inventory_ms={d:.1} inventory_rows={d} retained_payloads={d} segments={d} rss_delta_mib={d:.1}\n", .{
            cfg.drop_caches,
            ms(reopen_ns),
            ms(reopen_stats.inventory_update_ns),
            reopen_stats.inventory_rows_scanned,
            reopen_stats.retained_payloads,
            reopen_stats.source_segments,
            (@as(f64, @floatFromInt(rss_after.rss_kb)) - @as(f64, @floatFromInt(rss_before.rss_kb))) / 1024.0,
        });
        try printMem(out, "reopen", &counting);
        try stdout_writer.flush();
        try benchReads(out, alloc, &reopened, doc_keys, references, cfg, "read_after_reopen", random);
        try benchRerank(out, alloc, &reopened, references, cfg, "rerank_after_reopen", random, io);
        try stdout_writer.flush();
        return;
    }

    var source = if (cfg.managed)
        try payload_store.Store.openManaged(store_alloc, manager, storage, cfg.root, false)
    else
        try payload_store.Store.open(store_alloc, storage, cfg.root, false);
    var source_open = true;
    defer if (source_open) source.deinit();
    try out.print("encoding={s} wal_admission_mib={d:.1}\n", .{ @tagName(source.opened.payloadEncoding() orelse .float32), mib(source.wal_admission_bytes) });

    // ---------------------------------------------------------------- ingest
    var digest_ns: u64 = 0;
    var batch_timing: Timing = .{};
    defer batch_timing.samples.deinit(alloc);
    const ingest_started = time.monotonicNs();
    {
        var pos: usize = 0;
        while (pos < cfg.vectors) {
            const end = @min(cfg.vectors, pos + cfg.batch);
            const session = try payload.Session.create(alloc, source.interface());
            const put_started = time.monotonicNs();
            for (pos..end) |i| {
                const ref = try session.put(doc_keys[i], artifacts[i]);
                references[i] = ref[0..payload.reference_len].*;
            }
            digest_ns += time.monotonicNs() -| put_started;
            const prepare_started = time.monotonicNs();
            try session.prepareCommit();
            try batch_timing.add(alloc, time.monotonicNs() -| prepare_started);
            session.committed = true;
            session.release();
            pos = end;
        }
    }
    const ingest_ns = time.monotonicNs() -| ingest_started;
    const ingest_stats = source.statsSnapshot();
    const payload_bytes: u64 = @as(u64, cfg.vectors) * @as(u64, cfg.dims) * 4;
    try out.print(
        "[ingest] wall_ms={d:.1} vectors_per_s={d:.0} payload_mib={d:.2} put_digest_ms={d:.1} prepare_p50_us={d:.1} prepare_p99_us={d:.1} prepare_max_us={d:.1} prepare_sum_ms={d:.1}\n",
        .{
            ms(ingest_ns),
            @as(f64, @floatFromInt(cfg.vectors)) / (@as(f64, @floatFromInt(ingest_ns)) / 1e9),
            mib(payload_bytes),
            ms(digest_ns),
            us(batch_timing.percentile(0.5)),
            us(batch_timing.percentile(0.99)),
            us(batch_timing.percentile(1.0)),
            ms(batch_timing.total()),
        },
    );
    try printStats(out, "ingest", ingest_stats);
    try printMem(out, "ingest", &counting);
    try out.print("[ingest] disk_mib={d:.2} files={d} disk_amplification={d:.2}\n", .{ mib(try diskBytes(storage, alloc, cfg.root)), try fileCount(storage, alloc, cfg.root), @as(f64, @floatFromInt(try diskBytes(storage, alloc, cfg.root))) / @as(f64, @floatFromInt(payload_bytes)) });
    try stdout_writer.flush();

    // --------------------------------------------------- reads before checkpoint
    try benchReads(out, alloc, &source, doc_keys, references, cfg, "read_hot", random);
    try stdout_writer.flush();

    // ------------------------------------------------------------ checkpoint
    if (!cfg.skip_checkpoint) {
        const before = source.statsSnapshot();
        const rss_before = Rss.read();
        const ckpt_started = time.monotonicNs();
        try source.checkpoint();
        const ckpt_ns = time.monotonicNs() -| ckpt_started;
        const after = source.statsSnapshot();
        const rss_after = Rss.read();
        try out.print("[checkpoint] wall_ms={d:.1} stat_checkpoint_ms={d:.1} read_mib={d:.2} written_mib={d:.2} segments={d} active_wal_mib={d:.2} rss_delta_mib={d:.1}\n", .{
            ms(ckpt_ns),
            ms(after.checkpoint_ns -| before.checkpoint_ns),
            mib(after.checkpoint_bytes_read -| before.checkpoint_bytes_read),
            mib(after.checkpoint_bytes_written -| before.checkpoint_bytes_written),
            after.source_segments,
            mib(after.active_wal_bytes),
            (@as(f64, @floatFromInt(rss_after.rss_kb)) - @as(f64, @floatFromInt(rss_before.rss_kb))) / 1024.0,
        });
        try printMem(out, "checkpoint", &counting);
        try out.print("[checkpoint] disk_mib={d:.2} files={d}\n", .{ mib(try diskBytes(storage, alloc, cfg.root)), try fileCount(storage, alloc, cfg.root) });
        try stdout_writer.flush();

        try benchReads(out, alloc, &source, doc_keys, references, cfg, "read_cold_blocks", random);
        try benchReads(out, alloc, &source, doc_keys, references, cfg, "read_warm_blocks", random);
        try stdout_writer.flush();
    }

    // ------------------------------------------------------------- updates
    if (cfg.update_fraction_percent > 0) {
        const updates = cfg.vectors * cfg.update_fraction_percent / 100;
        const scratch = try alloc.alloc(f32, cfg.dims);
        defer alloc.free(scratch);
        var update_timing: Timing = .{};
        defer update_timing.samples.deinit(alloc);
        const before = source.statsSnapshot();
        const started = time.monotonicNs();
        var done: usize = 0;
        while (done < updates) {
            const end = @min(updates, done + cfg.batch);
            const session = try payload.Session.create(alloc, source.interface());
            for (done..end) |_| {
                const i = random.uintLessThan(usize, cfg.vectors);
                fillVector(random, scratch);
                const artifact = try codec.encodeDenseEmbeddingAlloc(alloc, @intCast(i + 1000), scratch);
                defer alloc.free(artifact);
                const ref = try session.put(doc_keys[i], artifact);
                references[i] = ref[0..payload.reference_len].*;
                alloc.free(artifacts[i]);
                artifacts[i] = try alloc.dupe(u8, artifact);
            }
            const prepare_started = time.monotonicNs();
            try session.prepareCommit();
            try update_timing.add(alloc, time.monotonicNs() -| prepare_started);
            session.committed = true;
            session.release();
            done = end;
        }
        const elapsed = time.monotonicNs() -| started;
        const after = source.statsSnapshot();
        try out.print("[update] count={d} wall_ms={d:.1} vectors_per_s={d:.0} prepare_p50_us={d:.1} prepare_p99_us={d:.1} wal_written_mib={d:.2} checkpoint_ms={d:.1} ckpt_written_mib={d:.2} segments={d}\n", .{
            updates,
            ms(elapsed),
            @as(f64, @floatFromInt(updates)) / (@as(f64, @floatFromInt(elapsed)) / 1e9),
            us(update_timing.percentile(0.5)),
            us(update_timing.percentile(0.99)),
            mib(after.wal_bytes_written -| before.wal_bytes_written),
            ms(after.checkpoint_ns -| before.checkpoint_ns),
            mib(after.checkpoint_bytes_written -| before.checkpoint_bytes_written),
            after.source_segments,
        });
        try printMem(out, "update", &counting);
        try out.print("[update] disk_mib={d:.2} files={d}\n", .{ mib(try diskBytes(storage, alloc, cfg.root)), try fileCount(storage, alloc, cfg.root) });
        try stdout_writer.flush();
        try benchReads(out, alloc, &source, doc_keys, references, cfg, "read_after_update", random);
    }

    try benchComponents(out, alloc, &source, doc_keys, artifacts, references, cfg, random);
    try benchRerank(out, alloc, &source, references, cfg, "rerank_warm", random, io);
    try stdout_writer.flush();

    const final = source.statsSnapshot();
    try printStats(out, "final", final);
    try printMem(out, "final", &counting);
    try stdout_writer.flush();

    // ------------------------------------------------------------- reopen
    source.deinit();
    source_open = false;
    if (cfg.drop_caches) {
        if (!dropCaches()) try out.print("[reopen] drop_caches unavailable\n", .{});
    }
    counting.peak.store(counting.live.load(.monotonic), .monotonic);
    const rss_before = Rss.read();
    const reopen_started = time.monotonicNs();
    var reopened = if (cfg.managed)
        try payload_store.Store.openManaged(store_alloc, manager, storage, cfg.root, false)
    else
        try payload_store.Store.open(store_alloc, storage, cfg.root, false);
    const reopen_ns = time.monotonicNs() -| reopen_started;
    const rss_after = Rss.read();
    const reopen_stats = reopened.statsSnapshot();
    try out.print("[reopen] drop_caches={} wall_ms={d:.1} inventory_ms={d:.1} inventory_rows={d} retained_payloads={d} segments={d} rss_delta_mib={d:.1}\n", .{
        cfg.drop_caches,
        ms(reopen_ns),
        ms(reopen_stats.inventory_update_ns),
        reopen_stats.inventory_rows_scanned,
        reopen_stats.retained_payloads,
        reopen_stats.source_segments,
        (@as(f64, @floatFromInt(rss_after.rss_kb)) - @as(f64, @floatFromInt(rss_before.rss_kb))) / 1024.0,
    });
    try printMem(out, "reopen", &counting);
    try stdout_writer.flush();
    try benchReads(out, alloc, &reopened, doc_keys, references, cfg, "read_after_reopen", random);
    try benchRerank(out, alloc, &reopened, references, cfg, "rerank_after_reopen", random, io);
    try stdout_writer.flush();
    reopened.deinit();
}

/// Attributes the exact-read path: digest recomputation, raw lookup and
/// payload validation, versus the full resolve that the DB performs.
fn benchComponents(
    out: *std.Io.Writer,
    alloc: Allocator,
    source: *payload_store.Store,
    doc_keys: []const []u8,
    artifacts: []const []u8,
    references: []const [payload.reference_len]u8,
    cfg: Config,
    random: std.Random,
) !void {
    const n = @min(cfg.reads, 5000);
    if (n == 0) return;
    var sink: u64 = 0;
    // SHA-256 digest of (key, artifact) as Session.put and resolve compute it.
    var started = time.monotonicNs();
    for (0..n) |_| {
        const i = random.uintLessThan(usize, cfg.vectors);
        const ref = try payload.Reference.forArtifact(doc_keys[i], artifacts[i]);
        sink +%= ref.digest[0];
    }
    const digest_ns = (time.monotonicNs() -| started) / n;
    // Metadata-only location (index binary search, no payload bytes).
    started = time.monotonicNs();
    for (0..n) |_| {
        const i = random.uintLessThan(usize, cfg.vectors);
        const ref = try payload.Reference.decode(&references[i]);
        const found = try source.opened.locateHashed(&ref.digest, vector_block.keyHash(&ref.digest), std.math.maxInt(u64), 1);
        sink +%= @intFromEnum(found);
    }
    const locate_ns = (time.monotonicNs() -| started) / n;
    // Lookup plus payload checksum validation, borrowing mmap bytes.
    started = time.monotonicNs();
    for (0..n) |_| {
        const i = random.uintLessThan(usize, cfg.vectors);
        const ref = try payload.Reference.decode(&references[i]);
        const found = try source.opened.get(&ref.digest, std.math.maxInt(u64), 1);
        sink +%= found.vector.bytes[0];
    }
    const get_ns = (time.monotonicNs() -| started) / n;
    // Full resolve as the DB read path performs it.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const session = try payload.Session.create(alloc, source.interface());
    defer session.release();
    started = time.monotonicNs();
    for (0..n) |k| {
        const i = random.uintLessThan(usize, cfg.vectors);
        const value = try session.getAlloc(arena.allocator(), doc_keys[i], &references[i]);
        sink +%= value[value.len - 1];
        if ((k & 255) == 255) _ = arena.reset(.retain_capacity);
    }
    const resolve_ns = (time.monotonicNs() -| started) / n;
    try out.print("[components] samples={d} digest_us={d:.2} locate_us={d:.2} get_crc_us={d:.2} resolve_us={d:.2} sink={d}\n", .{ n, us(digest_ns), us(locate_ns), us(get_ns), us(resolve_ns), sink & 1 });
}

/// Models the ANN rerank read pattern: a batch of digests located against a
/// retained snapshot, then exact payloads fetched by bounded positional I/O.
fn benchRerank(
    out: *std.Io.Writer,
    alloc: Allocator,
    source: *payload_store.Store,
    references: []const [payload.reference_len]u8,
    cfg: Config,
    label: []const u8,
    random: std.Random,
    io: std.Io,
) !void {
    if (cfg.rerank_batches == 0 or cfg.rerank_batch == 0) return;
    var snapshot = try source.snapshot(alloc);
    defer snapshot.deinit();
    const k = cfg.rerank_batch;
    const requests = try alloc.alloc(@import("storage/vector_block_store.zig").ExactReadRequest, k);
    defer alloc.free(requests);
    const projections = try alloc.alloc(@import("storage/vector_block_store.zig").ProjectionReadRequest, k);
    defer alloc.free(projections);
    const scratch = try alloc.alloc(u8, k * (cfg.dims * 8 + 64));
    defer alloc.free(scratch);
    var locate_ns: u64 = 0;
    var serial_ns: u64 = 0;
    var parallel_ns: u64 = 0;
    var projection_ns: u64 = 0;
    var physical: u64 = 0;
    var wal_hits: u64 = 0;
    var errors: u64 = 0;
    for (0..cfg.rerank_batches) |b| {
        var t0 = time.monotonicNs();
        var offset: usize = 0;
        for (0..k) |j| {
            const i = random.uintLessThan(usize, cfg.vectors);
            const ref = try payload.Reference.decode(&references[i]);
            const found = try snapshot.locateHashed(&ref.digest, vector_block.keyHash(&ref.digest), std.math.maxInt(u64), 1);
            if (found != .vector) return error.MissingCommittedVectorPayload;
            const need = switch (found.vector) {
                .wal => 0,
                .block => |blk| try blk.location.scratchBytes(),
            };
            if (found.vector == .wal) wal_hits += 1;
            requests[j] = .{ .located = found.vector, .scratch = scratch[offset..][0..need] };
            projections[j] = .{ .located = found.vector, .scratch = scratch[offset..][0..found.vector.projectionBytes()] };
            offset += need;
        }
        locate_ns += time.monotonicNs() -| t0;
        // Alternate serial, concurrent, and projection-only reads per batch so
        // page-cache state is shared fairly across the three modes.
        t0 = time.monotonicNs();
        switch (b % 3) {
            0 => {
                const stats = try snapshot.readExactIntoBatch(null, requests);
                serial_ns += time.monotonicNs() -| t0;
                physical += stats.physical_bytes;
            },
            1 => {
                _ = try snapshot.readExactIntoBatch(io, requests);
                parallel_ns += time.monotonicNs() -| t0;
            },
            else => {
                _ = try snapshot.readProjectionsIntoBatch(null, projections);
                projection_ns += time.monotonicNs() -| t0;
            },
        }
        for (requests) |request| if (request.err != null) {
            errors += 1;
        };
    }
    const per_mode = (cfg.rerank_batches + 2) / 3;
    try out.print("[{s}] batch={d} batches={d} locate_us_per_vec={d:.2} exact_serial_us_per_vec={d:.2} exact_io_us_per_vec={d:.2} projection_serial_us_per_vec={d:.2} wal_hits={d} errors={d} physical_mib={d:.2}\n", .{
        label,
        k,
        cfg.rerank_batches,
        us(locate_ns / (cfg.rerank_batches * k)),
        us(serial_ns / (per_mode * k)),
        us(parallel_ns / (per_mode * k)),
        us(projection_ns / (per_mode * k)),
        wal_hits,
        errors,
        mib(physical),
    });
}

fn benchReads(
    out: *std.Io.Writer,
    alloc: Allocator,
    source: *payload_store.Store,
    doc_keys: []const []u8,
    references: []const [payload.reference_len]u8,
    cfg: Config,
    label: []const u8,
    random: std.Random,
) !void {
    if (cfg.reads == 0) return;
    var timing: Timing = .{};
    defer timing.samples.deinit(alloc);
    const session = try payload.Session.create(alloc, source.interface());
    defer session.release();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const started = time.monotonicNs();
    var checked: u64 = 0;
    for (0..cfg.reads) |n| {
        const i = random.uintLessThan(usize, cfg.vectors);
        const t0 = time.monotonicNs();
        const value = try session.getAlloc(arena.allocator(), doc_keys[i], &references[i]);
        try timing.add(alloc, time.monotonicNs() -| t0);
        checked += value.len;
        if ((n & 255) == 255) _ = arena.reset(.retain_capacity);
    }
    const elapsed = time.monotonicNs() -| started;
    try out.print("[{s}] reads={d} wall_ms={d:.1} reads_per_s={d:.0} p50_us={d:.1} p99_us={d:.1} max_us={d:.1} bytes={d}\n", .{
        label,
        cfg.reads,
        ms(elapsed),
        @as(f64, @floatFromInt(cfg.reads)) / (@as(f64, @floatFromInt(elapsed)) / 1e9),
        us(timing.percentile(0.5)),
        us(timing.percentile(0.99)),
        us(timing.percentile(1.0)),
        checked,
    });
}
