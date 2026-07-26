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
const builtin = @import("builtin");
const tokenizer_mod = @import("inference_tokenizer");

const ValidationMode = enum {
    exact,
    complete_hash,
};

const Config = struct {
    tokenizer_path: []const u8,
    corpus_path: []const u8,
    warmup_iterations: usize = 2,
    iterations: usize = 5,
    threads: usize = 1,
    internal_threads: usize = 1,
    repeat: usize = 1,
    profile_bpe: bool = false,
    diagnostics: bool = false,
    diagnostic_iterations: usize = 1,
    cache_max_bytes: ?usize = null,
    cache_bulk_slots_per_shard: usize = 0,
    chunks_per_task: usize = 0,
    max_chunks: usize = 256,
    worker_cache_count: usize = 0,
    worker_cache_slots: usize = 0,
    workspace_retain_max_bytes: usize = 64 * 1024 * 1024,
    validation_mode: ValidationMode = .exact,
    mmap_corpus: bool = false,
    resident_corpus: bool = false,
    prefault_corpus: bool = false,
    stable_input: bool = false,
    segmented_output: bool = false,
    stable_boundary_index: bool = false,
    recycle_segmented_output_pages: bool = false,
    packed_u16_output: bool = false,
};

fn usage(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\usage: zig build bench-tokenizer -- <tokenizer.json> <corpus.txt> [options]
        \\
        \\options:
        \\  --warmup N                warm-cache iterations (default: 2)
        \\  --iterations N            timed iterations (default: 5)
        \\  --threads N               concurrent complete encodes (default: 1)
        \\  --internal-threads N      std.Io consumers per encode (default: 1)
        \\  --repeat N                concatenate the corpus N times
        \\  --cache-max-mb N          cache hard limit; zero disables it
        \\  --cache-bulk-slots N      second-tier slots per shard; zero disables it
        \\  --chunks-per-task N       zero keeps the adaptive scheduler default
        \\  --max-chunks N            cap chunks per encode at 1..256
        \\  --worker-cache-count N    persistent private cache tables (0..64)
        \\  --worker-cache-slots N    power-of-two 32-byte entries per table
        \\  --workspace-retain-max-mb N retain reusable chunk buffers up to N MiB
        \\  --profile-bpe             collect detailed BPE/cache counters
        \\  --diagnostics             measure scanner-only, serial no-cache, and serial warm stages
        \\  --diagnostic-iterations N stage repetitions (default: 1)
        \\  --validation exact|hash   exact replay or memory-bounded complete BLAKE3 validation
        \\  --mmap-corpus             map the corpus read-only instead of copying it
        \\  --resident-corpus         copy a temporary mapping into owned resident memory
        \\  --prefault-corpus         touch mapped pages before timing
        \\  --stable-input            reuse immutable-source segmentation metadata
        \\  --segmented-output        retain ordered worker chunks without flattening
        \\  --stable-boundary-index   retain one-bit-per-byte pretoken boundaries
        \\  --recycle-segment-pages   release prior segmented-output pages to Darwin VM
        \\  --packed-u16-output       retain lossless u16 token segments when IDs fit
        \\
        \\Measures the native Zig HuggingFace tokenizer's steady-state encodeInto
        \\throughput. The tokenizer and reusable output buffer persist across all
        \\iterations. Concurrent tasks use the process std.Io runtime and share the
        \\tokenizer and its concurrent cache.
        \\
    );
}

fn parseArgs(io: std.Io, args_in: std.process.Args) !Config {
    var args = std.process.Args.Iterator.init(args_in);
    _ = args.skip();

    const tokenizer_path = args.next() orelse {
        var stderr_buf: [1024]u8 = undefined;
        var stderr = std.Io.File.stderr().writerStreaming(io, &stderr_buf);
        try usage(&stderr.interface);
        try stderr.interface.flush();
        return error.MissingTokenizerPath;
    };
    if (std.mem.eql(u8, tokenizer_path, "--help") or std.mem.eql(u8, tokenizer_path, "-h")) {
        var stdout_buf: [1024]u8 = undefined;
        var stdout = std.Io.File.stdout().writerStreaming(io, &stdout_buf);
        try usage(&stdout.interface);
        try stdout.interface.flush();
        std.process.exit(0);
    }

    var cfg = Config{
        .tokenizer_path = tokenizer_path,
        .corpus_path = args.next() orelse return error.MissingCorpusPath,
    };
    while (args.next()) |arg| {
        if (std.mem.eql(u8, arg, "--warmup")) {
            cfg.warmup_iterations = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--iterations")) {
            cfg.iterations = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--threads")) {
            cfg.threads = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--internal-threads")) {
            cfg.internal_threads = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--repeat")) {
            cfg.repeat = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--profile-bpe")) {
            cfg.profile_bpe = true;
        } else if (std.mem.eql(u8, arg, "--diagnostics")) {
            cfg.diagnostics = true;
        } else if (std.mem.eql(u8, arg, "--diagnostic-iterations")) {
            cfg.diagnostic_iterations = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--cache-max-mb")) {
            const max_mb = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
            cfg.cache_max_bytes = std.math.mul(
                usize,
                max_mb,
                1024 * 1024,
            ) catch return error.InvalidConfiguration;
        } else if (std.mem.eql(u8, arg, "--chunks-per-task")) {
            cfg.chunks_per_task = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--cache-bulk-slots")) {
            cfg.cache_bulk_slots_per_shard = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--max-chunks")) {
            cfg.max_chunks = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--worker-cache-count")) {
            cfg.worker_cache_count = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--worker-cache-slots")) {
            cfg.worker_cache_slots = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
        } else if (std.mem.eql(u8, arg, "--workspace-retain-max-mb")) {
            const max_mb = try std.fmt.parseInt(
                usize,
                args.next() orelse return error.MissingArgument,
                10,
            );
            cfg.workspace_retain_max_bytes = std.math.mul(
                usize,
                max_mb,
                1024 * 1024,
            ) catch return error.InvalidConfiguration;
        } else if (std.mem.eql(u8, arg, "--validation")) {
            const mode = args.next() orelse return error.MissingArgument;
            if (std.mem.eql(u8, mode, "exact")) {
                cfg.validation_mode = .exact;
            } else if (std.mem.eql(u8, mode, "hash")) {
                cfg.validation_mode = .complete_hash;
            } else {
                return error.InvalidValidationMode;
            }
        } else if (std.mem.eql(u8, arg, "--mmap-corpus")) {
            cfg.mmap_corpus = true;
        } else if (std.mem.eql(u8, arg, "--resident-corpus")) {
            cfg.resident_corpus = true;
        } else if (std.mem.eql(u8, arg, "--prefault-corpus")) {
            cfg.prefault_corpus = true;
        } else if (std.mem.eql(u8, arg, "--stable-input")) {
            cfg.stable_input = true;
        } else if (std.mem.eql(u8, arg, "--segmented-output")) {
            cfg.segmented_output = true;
        } else if (std.mem.eql(u8, arg, "--stable-boundary-index")) {
            cfg.stable_boundary_index = true;
        } else if (std.mem.eql(u8, arg, "--recycle-segment-pages")) {
            cfg.recycle_segmented_output_pages = true;
        } else if (std.mem.eql(u8, arg, "--packed-u16-output")) {
            cfg.packed_u16_output = true;
        } else {
            return error.UnknownArgument;
        }
    }
    if (cfg.iterations == 0 or
        cfg.threads == 0 or
        cfg.threads > 256 or
        cfg.internal_threads == 0 or
        cfg.internal_threads > 64 or
        cfg.repeat == 0 or
        cfg.repeat > 4096 or
        cfg.diagnostic_iterations == 0 or
        cfg.chunks_per_task > 256 or
        cfg.cache_bulk_slots_per_shard > 131072 or
        (cfg.segmented_output and !cfg.stable_input) or
        (cfg.stable_boundary_index and !cfg.stable_input) or
        (cfg.recycle_segmented_output_pages and !cfg.segmented_output) or
        (cfg.packed_u16_output and !cfg.segmented_output) or
        (cfg.prefault_corpus and
            !cfg.mmap_corpus and
            !cfg.resident_corpus) or
        cfg.max_chunks == 0 or
        cfg.max_chunks > 256)
    {
        return error.InvalidConfiguration;
    }
    return cfg;
}

const Worker = struct {
    tokenizer: tokenizer_mod.Tokenizer,
    hf: *tokenizer_mod.hf.HfTokenizer,
    io: std.Io,
    corpus: []const u8,
    internal_threads: usize,
    stable_input_id: ?u64,
    segmented_output: bool,
    packed_u16_output: bool,
    ids: std.ArrayListUnmanaged(i32) = .empty,
    segments: ?tokenizer_mod.hf.HfTokenizer.ParallelTokenSegments = null,
    segments_u16: ?tokenizer_mod.hf.HfTokenizer.ParallelTokenSegmentsU16 = null,
    failure: ?anyerror = null,

    fn deinit(self: *Worker) void {
        if (self.segments) |*segments| segments.deinit();
        self.segments = null;
        if (self.segments_u16) |*segments| segments.deinit();
        self.segments_u16 = null;
        self.ids.deinit(std.heap.c_allocator);
        self.ids = .empty;
    }

    fn tokenCount(self: *const Worker) usize {
        if (self.segments_u16) |*segments| return segments.tokenCount();
        if (self.segments) |*segments| return segments.tokenCount();
        return self.ids.items.len;
    }

    fn runOnce(self: *Worker) std.Io.Cancelable!void {
        const allocator = std.heap.c_allocator;

        if (self.segments) |*segments| segments.deinit();
        self.segments = null;
        if (self.segments_u16) |*segments| segments.deinit();
        self.segments_u16 = null;
        self.ids.clearRetainingCapacity();
        self.failure = null;
        if (self.segmented_output) {
            if (self.packed_u16_output) {
                self.segments_u16 =
                    self.hf.encodeParallelSegmentsU16Stable(
                        self.io,
                        self.corpus,
                        self.internal_threads,
                        self.stable_input_id.?,
                    ) catch |err| {
                        self.failure = err;
                        if (err == error.Canceled)
                            return error.Canceled;
                        return;
                    };
                if (self.segments_u16.?.segmentCount() != 0) {
                    std.mem.doNotOptimizeAway(
                        self.segments_u16.?.segment(0).ptr,
                    );
                }
            } else {
                self.segments =
                    self.hf.encodeParallelSegmentsStable(
                        self.io,
                        self.corpus,
                        self.internal_threads,
                        self.stable_input_id.?,
                    ) catch |err| {
                        self.failure = err;
                        if (err == error.Canceled)
                            return error.Canceled;
                        return;
                    };
                if (self.segments.?.segmentCount() != 0) {
                    std.mem.doNotOptimizeAway(
                        self.segments.?.segment(0).ptr,
                    );
                }
            }
            return;
        }
        const result = if (self.stable_input_id) |stable_input_id|
            self.tokenizer.encodeIntoParallelStable(
                self.io,
                allocator,
                self.corpus,
                &self.ids,
                self.internal_threads,
                stable_input_id,
            )
        else
            self.tokenizer.encodeIntoParallel(
                self.io,
                allocator,
                self.corpus,
                &self.ids,
                self.internal_threads,
            );
        result catch |err| {
            self.failure = err;
            if (err == error.Canceled) return error.Canceled;
            return;
        };
        std.mem.doNotOptimizeAway(self.ids.items.ptr);
    }
};

fn runWorkerBatch(io: std.Io, workers: []Worker) !void {
    var group: std.Io.Group = .init;
    errdefer group.cancel(io);
    for (workers[0 .. workers.len - 1]) |*worker| {
        group.async(io, Worker.runOnce, .{worker});
    }
    try workers[workers.len - 1].runOnce();
    try group.await(io);
}

const SequenceMismatch = struct {
    index: usize,
    expected_token: ?i32,
    actual_token: ?i32,
};

fn findSequenceMismatch(
    expected_ids: []const i32,
    actual_ids: []const i32,
) ?SequenceMismatch {
    if (std.mem.eql(i32, expected_ids, actual_ids)) return null;

    const shared_len = @min(expected_ids.len, actual_ids.len);
    var mismatch_index: usize = 0;
    while (mismatch_index < shared_len and
        expected_ids[mismatch_index] == actual_ids[mismatch_index])
    {
        mismatch_index += 1;
    }
    return .{
        .index = mismatch_index,
        .expected_token = if (mismatch_index < expected_ids.len)
            expected_ids[mismatch_index]
        else
            null,
        .actual_token = if (mismatch_index < actual_ids.len)
            actual_ids[mismatch_index]
        else
            null,
    };
}

fn reportSequenceMismatch(
    io: std.Io,
    phase: []const u8,
    worker_index: usize,
    expected_len: usize,
    actual_len: usize,
    mismatch: SequenceMismatch,
) !void {
    var stderr_buf: [512]u8 = undefined;
    var stderr = std.Io.File.stderr().writerStreaming(io, &stderr_buf);
    try stderr.interface.print(
        "token validation failed phase={s} worker={d} expected_tokens={d} actual_tokens={d} " ++
            "first_mismatch={d} expected_token={any} actual_token={any}\n",
        .{
            phase,
            worker_index,
            expected_len,
            actual_len,
            mismatch.index,
            mismatch.expected_token,
            mismatch.actual_token,
        },
    );
    try stderr.interface.flush();
}

const ValidationWorker = struct {
    tokenizer: tokenizer_mod.Tokenizer,
    io: std.Io,
    corpus: []const u8,
    expected_ids: []const i32,
    internal_threads: usize,
    actual_len: usize = 0,
    mismatch_index: ?usize = null,
    expected_token: ?i32 = null,
    actual_token: ?i32 = null,
    failure: ?anyerror = null,

    fn run(self: *ValidationWorker) std.Io.Cancelable!void {
        const allocator = std.heap.c_allocator;
        var ids: std.ArrayListUnmanaged(i32) = .empty;
        defer ids.deinit(allocator);

        self.tokenizer.encodeIntoParallel(
            self.io,
            allocator,
            self.corpus,
            &ids,
            self.internal_threads,
        ) catch |err| {
            self.failure = err;
            if (err == error.Canceled) return error.Canceled;
            return;
        };
        self.actual_len = ids.items.len;
        if (findSequenceMismatch(self.expected_ids, ids.items)) |mismatch| {
            self.mismatch_index = mismatch.index;
            self.expected_token = mismatch.expected_token;
            self.actual_token = mismatch.actual_token;
        }
    }
};

fn hashTokenIds(ids: []const i32) u64 {
    var hash: u64 = 0xcbf29ce484222325;
    for (ids) |id| {
        hash = (hash ^ @as(u32, @bitCast(id))) *% 0x100000001b3;
    }
    return hash;
}

fn hashTokenIdsBlake3(ids: []const i32) [32]u8 {
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update(std.mem.sliceAsBytes(ids));
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn hashTokenSegmentsBlake3(
    segments: *const tokenizer_mod.hf.HfTokenizer.ParallelTokenSegments,
) [32]u8 {
    var hasher = std.crypto.hash.Blake3.init(.{});
    for (0..segments.segmentCount()) |idx| {
        hasher.update(std.mem.sliceAsBytes(segments.segment(idx)));
    }
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

fn hashTokenSegmentsU16Blake3(
    segments: *const tokenizer_mod.hf.HfTokenizer.ParallelTokenSegmentsU16,
) [32]u8 {
    var hasher = std.crypto.hash.Blake3.init(.{});
    var widened: [4096]i32 = undefined;
    for (0..segments.segmentCount()) |idx| {
        const segment = segments.segment(idx);
        var start: usize = 0;
        while (start < segment.len) {
            const count = @min(widened.len, segment.len - start);
            for (
                segment[start..][0..count],
                widened[0..count],
            ) |id, *output| {
                output.* = id;
            }
            hasher.update(
                std.mem.sliceAsBytes(widened[0..count]),
            );
            start += count;
        }
    }
    var digest: [std.crypto.hash.Blake3.digest_length]u8 = undefined;
    hasher.final(&digest);
    return digest;
}

const MappedCorpus = struct {
    bytes: []align(std.heap.page_size_min) u8,
    fd: std.posix.fd_t,

    fn init(path: []const u8) !MappedCorpus {
        const fd = try std.posix.openat(
            std.posix.AT.FDCWD,
            path,
            .{ .ACCMODE = .RDONLY, .CLOEXEC = true },
            0,
        );
        errdefer _ = std.posix.system.close(fd);
        const size_raw = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
        if (size_raw < 0) return error.Unexpected;
        const size = std.math.cast(usize, size_raw) orelse return error.CorpusSizeOverflow;
        if (size == 0) return error.EmptyCorpus;
        const bytes = try std.posix.mmap(
            null,
            size,
            .{ .READ = true },
            .{ .TYPE = .SHARED },
            fd,
            0,
        );
        std.posix.madvise(bytes.ptr, bytes.len, std.posix.MADV.SEQUENTIAL) catch {};
        return .{ .bytes = bytes, .fd = fd };
    }

    fn deinit(self: *MappedCorpus) void {
        std.posix.munmap(self.bytes);
        _ = std.posix.system.close(self.fd);
        self.* = undefined;
    }
};

fn processPeakRssBytes() usize {
    const resource_usage = std.posix.getrusage(std.posix.rusage.SELF);
    if (resource_usage.maxrss <= 0) return 0;
    const maxrss: usize = @intCast(resource_usage.maxrss);
    return switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => maxrss,
        .linux => std.math.mul(usize, maxrss, 1024) catch std.math.maxInt(usize),
        else => maxrss,
    };
}

fn timevalNs(value: anytype) u64 {
    if (value.sec < 0 or value.usec < 0) return 0;
    const seconds: u64 = @intCast(value.sec);
    const microseconds: u64 = @intCast(value.usec);
    return seconds *| std.time.ns_per_s +| microseconds *| std.time.ns_per_us;
}

fn processCpuNs() u64 {
    const resource_usage = std.posix.getrusage(std.posix.rusage.SELF);
    return timevalNs(resource_usage.utime) +| timevalNs(resource_usage.stime);
}

const StageDiagnostics = struct {
    scanner_pretokens: usize,
    scanner_seconds: f64,
    scanner_mb_per_second: f64,
    serial_no_cache_tokens: usize,
    serial_no_cache_hash: u64,
    serial_no_cache_seconds: f64,
    serial_no_cache_mb_per_second: f64,
    serial_warm_tokens: usize,
    serial_warm_hash: u64,
    serial_warm_seconds: f64,
    serial_warm_mb_per_second: f64,
};

fn elapsedSeconds(started_at: std.Io.Timestamp, finished_at: std.Io.Timestamp) f64 {
    const elapsed_ns = std.Io.Timestamp.durationTo(started_at, finished_at).nanoseconds;
    return @as(f64, @floatFromInt(elapsed_ns)) /
        @as(f64, @floatFromInt(std.time.ns_per_s));
}

fn prefaultCorpus(corpus: []const u8) void {
    if (corpus.len == 0) return;
    var page_checksum: u8 = 0;
    var offset: usize = 0;
    while (offset < corpus.len) : (offset += std.heap.page_size_min) {
        page_checksum +%= corpus[offset];
    }
    page_checksum +%= corpus[corpus.len - 1];
    std.mem.doNotOptimizeAway(page_checksum);
}

fn runStageDiagnostics(
    io: std.Io,
    allocator: std.mem.Allocator,
    tokenizer_json: []const u8,
    hf: *tokenizer_mod.hf.HfTokenizer,
    corpus: []const u8,
    iterations: usize,
) !StageDiagnostics {
    const total_bytes = std.math.mul(
        usize,
        corpus.len,
        iterations,
    ) catch return error.InvalidConfiguration;
    var scanner_pretokens: usize = 0;
    const scanner_started = std.Io.Timestamp.now(io, .awake);
    for (0..iterations) |_| {
        scanner_pretokens = std.math.add(
            usize,
            scanner_pretokens,
            try hf.countByteLevelPretokens(corpus),
        ) catch return error.InvalidConfiguration;
    }
    const scanner_finished = std.Io.Timestamp.now(io, .awake);
    const scanner_seconds = elapsedSeconds(scanner_started, scanner_finished);

    var no_cache_ids: std.ArrayListUnmanaged(i32) = .empty;
    errdefer no_cache_ids.deinit(allocator);
    const no_cache_hf = try tokenizer_mod.hf.HfTokenizer.loadFromBytes(
        allocator,
        tokenizer_json,
    );
    defer no_cache_hf.deinitSelf();
    try no_cache_hf.configureBpeCache(.{ .max_bytes = 0 });
    const no_cache_started = std.Io.Timestamp.now(io, .awake);
    var serial_no_cache_tokens: usize = 0;
    for (0..iterations) |_| {
        no_cache_ids.clearRetainingCapacity();
        try no_cache_hf.tokenizer().encodeInto(allocator, corpus, &no_cache_ids);
        serial_no_cache_tokens = std.math.add(
            usize,
            serial_no_cache_tokens,
            no_cache_ids.items.len,
        ) catch return error.InvalidConfiguration;
    }
    const no_cache_finished = std.Io.Timestamp.now(io, .awake);
    const no_cache_seconds = elapsedSeconds(no_cache_started, no_cache_finished);
    const no_cache_hash = hashTokenIds(no_cache_ids.items);
    const no_cache_blake3 = hashTokenIdsBlake3(no_cache_ids.items);
    no_cache_ids.deinit(allocator);
    no_cache_ids = .empty;

    var warm_ids: std.ArrayListUnmanaged(i32) = .empty;
    defer warm_ids.deinit(allocator);
    const warm_started = std.Io.Timestamp.now(io, .awake);
    var serial_warm_tokens: usize = 0;
    for (0..iterations) |_| {
        warm_ids.clearRetainingCapacity();
        try hf.tokenizer().encodeInto(allocator, corpus, &warm_ids);
        serial_warm_tokens = std.math.add(
            usize,
            serial_warm_tokens,
            warm_ids.items.len,
        ) catch return error.InvalidConfiguration;
    }
    const warm_finished = std.Io.Timestamp.now(io, .awake);
    const warm_seconds = elapsedSeconds(warm_started, warm_finished);
    const warm_hash = hashTokenIds(warm_ids.items);
    const warm_blake3 = hashTokenIdsBlake3(warm_ids.items);
    if (serial_no_cache_tokens / iterations != serial_warm_tokens / iterations or
        no_cache_hash != warm_hash or
        !std.mem.eql(u8, &no_cache_blake3, &warm_blake3))
    {
        return error.StageDiagnosticSequenceMismatch;
    }
    return .{
        .scanner_pretokens = scanner_pretokens / iterations,
        .scanner_seconds = scanner_seconds,
        .scanner_mb_per_second = @as(f64, @floatFromInt(total_bytes)) / scanner_seconds / 1_000_000.0,
        .serial_no_cache_tokens = serial_no_cache_tokens / iterations,
        .serial_no_cache_hash = no_cache_hash,
        .serial_no_cache_seconds = no_cache_seconds,
        .serial_no_cache_mb_per_second = @as(f64, @floatFromInt(total_bytes)) / no_cache_seconds / 1_000_000.0,
        .serial_warm_tokens = serial_warm_tokens / iterations,
        .serial_warm_hash = warm_hash,
        .serial_warm_seconds = warm_seconds,
        .serial_warm_mb_per_second = @as(f64, @floatFromInt(total_bytes)) / warm_seconds / 1_000_000.0,
    };
}

pub fn main(init: std.process.Init) !void {
    const allocator = std.heap.c_allocator;
    const cfg = try parseArgs(init.io, init.minimal.args);

    const tokenizer_json = try std.Io.Dir.cwd().readFileAlloc(
        init.io,
        cfg.tokenizer_path,
        allocator,
        .limited(256 * 1024 * 1024),
    );
    defer allocator.free(tokenizer_json);
    var mapped_corpus: ?MappedCorpus = if (cfg.mmap_corpus or
        cfg.resident_corpus)
        try .init(cfg.corpus_path)
    else
        null;
    defer if (mapped_corpus) |*mapped| mapped.deinit();
    const corpus_file_owned: ?[]u8 = if (cfg.resident_corpus) blk: {
        const owned = try allocator.dupe(u8, mapped_corpus.?.bytes);
        mapped_corpus.?.deinit();
        mapped_corpus = null;
        break :blk owned;
    } else if (!cfg.mmap_corpus)
        try std.Io.Dir.cwd().readFileAlloc(
            init.io,
            cfg.corpus_path,
            allocator,
            .limited(16 * 1024 * 1024 * 1024),
        )
    else
        null;
    defer if (corpus_file_owned) |owned| allocator.free(owned);
    const corpus_file: []const u8 =
        corpus_file_owned orelse mapped_corpus.?.bytes;
    const repeated_corpus: ?[]u8 = if (cfg.repeat == 1)
        null
    else blk: {
        if (corpus_file.len != 0 and cfg.repeat > std.math.maxInt(usize) / corpus_file.len)
            return error.CorpusSizeOverflow;
        const repeated_len = corpus_file.len * cfg.repeat;
        const repeated = try allocator.alloc(u8, repeated_len);
        for (0..cfg.repeat) |idx| {
            @memcpy(repeated[idx * corpus_file.len ..][0..corpus_file.len], corpus_file);
        }
        break :blk repeated;
    };
    defer if (repeated_corpus) |repeated| allocator.free(repeated);
    const corpus: []const u8 = repeated_corpus orelse corpus_file;
    if (cfg.prefault_corpus) {
        prefaultCorpus(corpus);
    }

    const hf = try tokenizer_mod.hf.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    defer hf.deinitSelf();
    if (cfg.cache_max_bytes) |max_bytes| {
        try hf.configureBpeCache(.{
            .max_bytes = max_bytes,
            .bulk_slots_per_shard = cfg.cache_bulk_slots_per_shard,
        });
    } else if (cfg.cache_bulk_slots_per_shard != 0) {
        try hf.configureBpeCache(.{
            .bulk_slots_per_shard = cfg.cache_bulk_slots_per_shard,
        });
    }
    try hf.configureParallelBpe(.{
        .chunks_per_task = cfg.chunks_per_task,
        .max_chunks = cfg.max_chunks,
        .worker_cache_count = cfg.worker_cache_count,
        .worker_cache_slots = cfg.worker_cache_slots,
        .max_retained_workspace_bytes = cfg.workspace_retain_max_bytes,
        .retain_stable_pretoken_boundaries = cfg.stable_boundary_index,
        .recycle_segmented_output_pages = cfg.recycle_segmented_output_pages,
    });
    const tokenizer = hf.tokenizer();
    const rss_after_load = processPeakRssBytes();

    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(allocator);
    for (0..cfg.warmup_iterations) |_| {
        ids.clearRetainingCapacity();
        if (cfg.segmented_output) {
            if (cfg.packed_u16_output) {
                var segments =
                    try hf.encodeParallelSegmentsU16Stable(
                        init.io,
                        corpus,
                        cfg.internal_threads,
                        1,
                    );
                segments.deinit();
            } else {
                var segments = try hf.encodeParallelSegmentsStable(
                    init.io,
                    corpus,
                    cfg.internal_threads,
                    1,
                );
                segments.deinit();
            }
        } else if (cfg.stable_input) {
            try tokenizer.encodeIntoParallelStable(
                init.io,
                allocator,
                corpus,
                &ids,
                cfg.internal_threads,
                1,
            );
        } else {
            try tokenizer.encodeIntoParallel(
                init.io,
                allocator,
                corpus,
                &ids,
                cfg.internal_threads,
            );
        }
    }
    const rss_after_warmup = processPeakRssBytes();
    // Warmup exists to populate tokenizer-owned state. Its output capacity is
    // not reused by timed workers, so retaining it would only inflate the
    // full-corpus peak while those workers build their own outputs.
    ids.deinit(allocator);
    ids = .empty;
    const stage_diagnostics = if (cfg.diagnostics)
        try runStageDiagnostics(
            init.io,
            allocator,
            tokenizer_json,
            hf,
            corpus,
            cfg.diagnostic_iterations,
        )
    else
        null;
    if (cfg.profile_bpe) hf.setBpeProfiling(true);

    const workers = try allocator.alloc(Worker, cfg.threads);
    defer {
        for (workers) |*worker| worker.deinit();
        allocator.free(workers);
    }
    for (workers) |*worker| {
        worker.* = .{
            .tokenizer = tokenizer,
            .hf = hf,
            .io = init.io,
            .corpus = corpus,
            .internal_threads = cfg.internal_threads,
            .stable_input_id = if (cfg.stable_input) 1 else null,
            .segmented_output = cfg.segmented_output,
            .packed_u16_output = cfg.packed_u16_output,
        };
    }

    // Warmup output is no longer live. Re-establish the benchmark's
    // in-memory-input contract immediately before timing; on Darwin the
    // optional segmented-page recycling above prevents stale anonymous
    // token pages from displacing this file-backed corpus.
    if (cfg.prefault_corpus) prefaultCorpus(corpus);
    const timed_sample_count = std.math.mul(
        usize,
        cfg.iterations,
        cfg.threads,
    ) catch return error.InvalidConfiguration;
    const total_bytes = std.math.mul(
        usize,
        corpus.len,
        timed_sample_count,
    ) catch return error.InvalidConfiguration;
    const timed_digests = try allocator.alloc([32]u8, timed_sample_count);
    defer allocator.free(timed_digests);
    const timed_token_counts = try allocator.alloc(usize, timed_sample_count);
    defer allocator.free(timed_token_counts);
    const rss_before_timed = processPeakRssBytes();
    var elapsed_ns: u64 = 0;
    var cpu_elapsed_ns: u64 = 0;
    var token_total: usize = 0;
    for (0..cfg.iterations) |iteration| {
        // Hashing the preceding complete outputs happens outside the measured
        // interval. Restore the in-memory input contract before the next
        // encode batch so validation reads cannot bias corpus residency.
        if (iteration != 0 and cfg.prefault_corpus) prefaultCorpus(corpus);
        const cpu_started_ns = processCpuNs();
        const started_at = std.Io.Timestamp.now(init.io, .awake);
        try runWorkerBatch(init.io, workers);
        const finished_at = std.Io.Timestamp.now(init.io, .awake);
        elapsed_ns +|= @intCast(
            std.Io.Timestamp.durationTo(
                started_at,
                finished_at,
            ).nanoseconds,
        );
        cpu_elapsed_ns +|= processCpuNs() -| cpu_started_ns;

        for (workers, 0..) |*worker, worker_index| {
            if (worker.failure) |err| return err;
            const sample_index = iteration * cfg.threads + worker_index;
            const token_count = worker.tokenCount();
            timed_token_counts[sample_index] = token_count;
            token_total = std.math.add(
                usize,
                token_total,
                token_count,
            ) catch return error.InvalidConfiguration;
            timed_digests[sample_index] =
                if (worker.segments_u16) |*segments|
                    hashTokenSegmentsU16Blake3(segments)
                else if (worker.segments) |*segments|
                    hashTokenSegmentsBlake3(segments)
                else
                    hashTokenIdsBlake3(worker.ids.items);
        }
    }
    const rss_after_timed = processPeakRssBytes();

    // Snapshot attribution before correctness validation exercises the cache
    // again. Validation is intentionally outside the measured interval.
    const cache_stats = hf.bpeCacheStats();
    const profile = if (cfg.profile_bpe) hf.bpeProfileSnapshot() else null;

    // The memory-bounded mode digests and releases timed outputs before
    // constructing the independent serial reference. This keeps full-corpus
    // validation from retaining several multi-gigabyte token arrays at once.
    if (cfg.validation_mode == .complete_hash) {
        for (workers) |*worker| worker.deinit();
    }

    // Derive the reference from a fresh tokenizer with its optional cache
    // disabled. This keeps the expected sequence independent of any shared
    // cache state produced by the timed concurrent run.
    ids.clearRetainingCapacity();
    {
        const reference_hf = try tokenizer_mod.hf.HfTokenizer.loadFromBytes(
            allocator,
            tokenizer_json,
        );
        defer reference_hf.deinitSelf();
        try reference_hf.configureBpeCache(.{ .max_bytes = 0 });
        try reference_hf.tokenizer().encodeInto(allocator, corpus, &ids);
    }
    const token_hash = hashTokenIds(ids.items);
    const token_blake3 = hashTokenIdsBlake3(ids.items);
    const token_blake3_hex = std.fmt.bytesToHex(token_blake3, .lower);
    const expected_token_count = ids.items.len;

    // Validate every measured encode, not only the final retained output.
    // Digests are produced after each separately timed concurrent batch, so
    // complete validation does not contribute to wall or CPU throughput.
    for (
        timed_digests,
        timed_token_counts,
        0..,
    ) |digest, actual_token_count, sample_index| {
        if (actual_token_count != expected_token_count or
            !std.mem.eql(u8, &digest, &token_blake3))
        {
            var stderr_buf: [512]u8 = undefined;
            var stderr = std.Io.File.stderr().writerStreaming(
                init.io,
                &stderr_buf,
            );
            const actual_hex = std.fmt.bytesToHex(digest, .lower);
            try stderr.interface.print(
                "token validation failed phase=timed_hash iteration={d} " ++
                    "worker={d} expected_tokens={d} actual_tokens={d} " ++
                    "expected_blake3={s} actual_blake3={s}\n",
                .{
                    sample_index / cfg.threads,
                    sample_index % cfg.threads,
                    expected_token_count,
                    actual_token_count,
                    &token_blake3_hex,
                    &actual_hex,
                },
            );
            try stderr.interface.flush();
            return error.TokenSequenceHashMismatch;
        }
    }

    if (cfg.validation_mode == .exact) {
        for (workers) |*worker| {
            if (worker.segments_u16) |*segments| {
                try worker.ids.ensureTotalCapacityPrecise(
                    allocator,
                    segments.tokenCount(),
                );
                for (0..segments.segmentCount()) |idx| {
                    for (segments.segment(idx)) |id| {
                        worker.ids.appendAssumeCapacity(id);
                    }
                }
                segments.deinit();
                worker.segments_u16 = null;
            } else if (worker.segments) |*segments| {
                try worker.ids.ensureTotalCapacityPrecise(
                    allocator,
                    segments.tokenCount(),
                );
                for (0..segments.segmentCount()) |idx| {
                    worker.ids.appendSliceAssumeCapacity(
                        segments.segment(idx),
                    );
                }
                segments.deinit();
                worker.segments = null;
            }
        }
        // The final output retained by every timed worker was produced while
        // all external callers and their internal consumers shared the
        // tokenizer. Validate those measured outputs before replay.
        for (workers, 0..) |*worker, worker_index| {
            if (findSequenceMismatch(ids.items, worker.ids.items)) |mismatch| {
                try reportSequenceMismatch(
                    init.io,
                    "timed",
                    worker_index,
                    ids.items.len,
                    worker.ids.items.len,
                    mismatch,
                );
                return error.TokenSequenceMismatch;
            }
            worker.deinit();
        }

        // Exercise the same external and internal concurrency requested for
        // the benchmark, then compare every complete output byte-for-byte.
        const validation_workers = try allocator.alloc(ValidationWorker, cfg.threads);
        defer allocator.free(validation_workers);
        for (validation_workers) |*worker| {
            worker.* = .{
                .tokenizer = tokenizer,
                .io = init.io,
                .corpus = corpus,
                .expected_ids = ids.items,
                .internal_threads = cfg.internal_threads,
            };
        }
        var validation_group: std.Io.Group = .init;
        errdefer validation_group.cancel(init.io);
        for (validation_workers[0 .. validation_workers.len - 1]) |*worker| {
            validation_group.async(init.io, ValidationWorker.run, .{worker});
        }
        try validation_workers[validation_workers.len - 1].run();
        try validation_group.await(init.io);

        for (validation_workers, 0..) |worker, worker_index| {
            if (worker.failure) |err| return err;
            if (worker.mismatch_index) |mismatch_index| {
                try reportSequenceMismatch(
                    init.io,
                    "replay",
                    worker_index,
                    ids.items.len,
                    worker.actual_len,
                    .{
                        .index = mismatch_index,
                        .expected_token = worker.expected_token,
                        .actual_token = worker.actual_token,
                    },
                );
                return error.TokenSequenceMismatch;
            }
        }
    } else {
        ids.deinit(allocator);
        ids = .empty;
    }
    const rss_after_validation = processPeakRssBytes();

    const seconds = @as(f64, @floatFromInt(elapsed_ns)) /
        @as(f64, @floatFromInt(std.time.ns_per_s));
    const mb_per_second = @as(f64, @floatFromInt(total_bytes)) / seconds / 1_000_000.0;
    const mtok_per_second = @as(f64, @floatFromInt(token_total)) / seconds / 1_000_000.0;
    const cpu_seconds = @as(f64, @floatFromInt(cpu_elapsed_ns)) /
        @as(f64, @floatFromInt(std.time.ns_per_s));
    const average_cores = cpu_seconds / seconds;
    const cpu_ns_per_byte =
        @as(f64, @floatFromInt(cpu_elapsed_ns)) /
        @as(f64, @floatFromInt(total_bytes));
    const validation_name = switch (cfg.validation_mode) {
        .exact => "exact_timed_and_replay",
        .complete_hash => "complete_blake3_timed",
    };

    var stdout_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buf);
    try stdout.interface.print(
        "runtime=std_io corpus_storage={s} stable_input={} segmented_output={} packed_u16_output={} stable_boundary_index={} recycle_segment_pages={} tokenizer_bytes={d} corpus_bytes={d} repeat={d} warmup_iterations={d} iterations={d} threads={d} internal_threads={d} " ++
            "validation={s} tokens_per_iteration={d} token_hash={x} token_blake3={s} elapsed_seconds={d:.6} " ++
            "cpu_seconds={d:.6} average_cores={d:.3} cpu_ns_per_byte={d:.3} mb_per_second={d:.3} " ++
            "mtokens_per_second={d:.3} chunks_per_task={d} max_chunks={d} " ++
            "worker_cache_count={d} worker_cache_slots_per_table={d} " ++
            "workspace_retain_limit_bytes={d} ",
        .{
            if (cfg.resident_corpus)
                "resident_copy"
            else if (cfg.mmap_corpus)
                "mmap"
            else
                "allocated",
            cfg.stable_input,
            cfg.segmented_output,
            cfg.packed_u16_output,
            cfg.stable_boundary_index,
            cfg.recycle_segmented_output_pages,
            tokenizer_json.len,
            corpus.len,
            cfg.repeat,
            cfg.warmup_iterations,
            cfg.iterations,
            cfg.threads,
            cfg.internal_threads,
            validation_name,
            token_total / cfg.iterations / cfg.threads,
            token_hash,
            &token_blake3_hex,
            seconds,
            cpu_seconds,
            average_cores,
            cpu_ns_per_byte,
            mb_per_second,
            mtok_per_second,
            cfg.chunks_per_task,
            cfg.max_chunks,
            cfg.worker_cache_count,
            cfg.worker_cache_slots,
            cfg.workspace_retain_max_bytes,
        },
    );
    try stdout.interface.print(
        "cache_entries={d} cache_front_entries={d} cache_bulk_entries={d} cache_bulk_slots={d} " ++
            "cache_bytes={d} cache_limit_bytes={d} cache_rejected_reservations={d} " ++
            "cache_rejected_admissions={d} cache_evictions={d} " ++
            "worker_cache_tables={d} worker_cache_entries={d} worker_cache_slots={d} " ++
            "worker_cache_bytes={d} worker_token_arena_ids={d} " ++
            "worker_cache_superpage_tables={d} " ++
            "workspace_cached_count={d} " ++
            "workspace_cached_bytes={d} affinity_learns={d} " ++
            "affinity_replays={d} affinity_stolen_chunks={d} " ++
            "stable_offset_learns={d} stable_offset_replays={d} " ++
            "stable_offset_count={d} stable_offset_bytes={d} " ++
            "stable_boundary_words={d} stable_boundary_bytes={d} " ++
            "parallel_max_chunk_bytes={d} parallel_max_chunk_ns={d} ",
        .{
            cache_stats.entries,
            cache_stats.front_entries,
            cache_stats.bulk_entries,
            cache_stats.bulk_slots,
            cache_stats.used_bytes,
            cache_stats.max_bytes,
            cache_stats.rejected_reservations,
            cache_stats.rejected_admissions,
            cache_stats.evictions,
            cache_stats.worker_tables,
            cache_stats.worker_entries,
            cache_stats.worker_slots,
            cache_stats.worker_bytes,
            cache_stats.worker_token_arena_ids,
            cache_stats.worker_superpage_tables,
            cache_stats.workspace_cached_count,
            cache_stats.workspace_cached_bytes,
            cache_stats.affinity_learns,
            cache_stats.affinity_replays,
            cache_stats.affinity_stolen_chunks,
            cache_stats.stable_offset_learns,
            cache_stats.stable_offset_replays,
            cache_stats.stable_offset_count,
            cache_stats.stable_offset_bytes,
            cache_stats.stable_boundary_words,
            cache_stats.stable_boundary_bytes,
            cache_stats.parallel_max_chunk_bytes,
            cache_stats.parallel_max_chunk_ns,
        },
    );
    try stdout.interface.print(
        "worker_cache_min_entries={d} worker_cache_max_entries={d} " ++
            "workspace_total_count={d} workspace_total_bytes={d} " ++
            "workspace_active_count={d} workspace_active_bytes={d} " ++
            "workspace_accounted_bytes={d} " ++
            "segmented_output_bytes={d} " ++
            "segmented_output_capacity_bytes={d} " ++
            "parallel_slowest_chunk_bytes={d} " ++
            "parallel_slowest_chunk_owner={d} " ++
            "parallel_max_owner_chunk_ns={d} " ++
            "parallel_min_owner_chunk_ns={d} ",
        .{
            cache_stats.worker_min_entries,
            cache_stats.worker_max_entries,
            cache_stats.workspace_total_count,
            cache_stats.workspace_total_bytes,
            cache_stats.workspace_active_count,
            cache_stats.workspace_active_bytes,
            cache_stats.workspace_accounted_bytes,
            cache_stats.workspace_active_output_bytes,
            cache_stats.workspace_active_output_capacity_bytes,
            cache_stats.parallel_slowest_chunk_bytes,
            cache_stats.parallel_slowest_chunk_owner,
            cache_stats.parallel_max_owner_chunk_ns,
            cache_stats.parallel_min_owner_chunk_ns,
        },
    );
    try stdout.interface.print(
        "rss_after_load_bytes={d} rss_after_warmup_bytes={d} " ++
            "rss_before_timed_bytes={d} " ++
            "rss_after_timed_bytes={d} rss_after_validation_bytes={d}\n",
        .{
            rss_after_load,
            rss_after_warmup,
            rss_before_timed,
            rss_after_timed,
            rss_after_validation,
        },
    );
    if (stage_diagnostics) |diagnostics| {
        try stdout.interface.print(
            "stage_diagnostics iterations={d} scanner_pretokens={d} scanner_seconds={d:.6} " ++
                "scanner_mb_per_second={d:.3} serial_no_cache_tokens={d} " ++
                "serial_no_cache_hash={x} serial_no_cache_seconds={d:.6} " ++
                "serial_no_cache_mb_per_second={d:.3} serial_warm_tokens={d} " ++
                "serial_warm_hash={x} serial_warm_seconds={d:.6} " ++
                "serial_warm_mb_per_second={d:.3}\n",
            .{
                cfg.diagnostic_iterations,
                diagnostics.scanner_pretokens,
                diagnostics.scanner_seconds,
                diagnostics.scanner_mb_per_second,
                diagnostics.serial_no_cache_tokens,
                diagnostics.serial_no_cache_hash,
                diagnostics.serial_no_cache_seconds,
                diagnostics.serial_no_cache_mb_per_second,
                diagnostics.serial_warm_tokens,
                diagnostics.serial_warm_hash,
                diagnostics.serial_warm_seconds,
                diagnostics.serial_warm_mb_per_second,
            },
        );
    }
    if (cfg.profile_bpe) {
        const bpe_profile = profile.?;
        const cache_lookups = bpe_profile.hits + bpe_profile.misses;
        const cache_hit_rate = if (cache_lookups == 0)
            0.0
        else
            @as(f64, @floatFromInt(bpe_profile.hits)) /
                @as(f64, @floatFromInt(cache_lookups));
        try stdout.interface.print(
            "bpe_profile pretokens={d} direct_hits={d} hits={d} misses={d} " ++
                "hit_rate={d:.6} probes={d} key_bytes={d} token_ids={d} " ++
                "key_len_histogram={any} id_count_histogram={any} probe_histogram={any}\n",
            .{
                bpe_profile.pretokens,
                bpe_profile.direct_hits,
                bpe_profile.hits,
                bpe_profile.misses,
                cache_hit_rate,
                bpe_profile.probes,
                bpe_profile.key_bytes,
                bpe_profile.token_ids,
                bpe_profile.key_len_histogram,
                bpe_profile.id_count_histogram,
                bpe_profile.probe_histogram,
            },
        );
    }
    try stdout.interface.flush();
}
