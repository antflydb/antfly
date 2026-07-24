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
const tokenizer_mod = @import("inference_tokenizer");

const Config = struct {
    tokenizer_path: []const u8,
    corpus_path: []const u8,
    warmup_iterations: usize = 2,
    iterations: usize = 5,
    threads: usize = 1,
    internal_threads: usize = 1,
    repeat: usize = 1,
    profile_bpe: bool = false,
};

fn usage(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\usage: zig build bench-tokenizer -- <tokenizer.json> <corpus.txt> [--warmup N] [--iterations N] [--threads N] [--internal-threads N] [--repeat N] [--profile-bpe]
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
        cfg.repeat > 4096)
    {
        return error.InvalidConfiguration;
    }
    return cfg;
}

const Worker = struct {
    tokenizer: tokenizer_mod.Tokenizer,
    io: std.Io,
    corpus: []const u8,
    iterations: usize,
    internal_threads: usize,
    token_total: usize = 0,
    failure: ?anyerror = null,

    fn run(self: *Worker) std.Io.Cancelable!void {
        const allocator = std.heap.c_allocator;
        var ids: std.ArrayListUnmanaged(i32) = .empty;
        defer ids.deinit(allocator);

        for (0..self.iterations) |_| {
            ids.clearRetainingCapacity();
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
            self.token_total +%= ids.items.len;
            std.mem.doNotOptimizeAway(ids.items.ptr);
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
    const corpus_file = try std.Io.Dir.cwd().readFileAlloc(
        init.io,
        cfg.corpus_path,
        allocator,
        .limited(16 * 1024 * 1024 * 1024),
    );
    defer allocator.free(corpus_file);
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

    const hf = try tokenizer_mod.hf.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    defer hf.deinitSelf();
    const tokenizer = hf.tokenizer();

    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(allocator);
    for (0..cfg.warmup_iterations) |_| {
        ids.clearRetainingCapacity();
        try tokenizer.encodeIntoParallel(init.io, allocator, corpus, &ids, cfg.internal_threads);
    }
    if (cfg.profile_bpe) hf.setBpeProfiling(true);

    const workers = try allocator.alloc(Worker, cfg.threads);
    defer allocator.free(workers);
    for (workers) |*worker| {
        worker.* = .{
            .tokenizer = tokenizer,
            .io = init.io,
            .corpus = corpus,
            .iterations = cfg.iterations,
            .internal_threads = cfg.internal_threads,
        };
    }

    const started_at = std.Io.Timestamp.now(init.io, .awake);
    var group: std.Io.Group = .init;
    errdefer group.cancel(init.io);
    for (workers[0 .. workers.len - 1]) |*worker| {
        group.async(init.io, Worker.run, .{worker});
    }
    try workers[workers.len - 1].run();
    try group.await(init.io);
    const finished_at = std.Io.Timestamp.now(init.io, .awake);
    const elapsed_ns = std.Io.Timestamp.durationTo(started_at, finished_at).nanoseconds;

    ids.clearRetainingCapacity();
    try tokenizer.encodeIntoParallel(init.io, allocator, corpus, &ids, cfg.internal_threads);
    const token_hash = hashTokenIds(ids.items);

    var token_total: usize = 0;
    for (workers) |worker| {
        if (worker.failure) |err| return err;
        token_total +%= worker.token_total;
    }
    const seconds = @as(f64, @floatFromInt(elapsed_ns)) /
        @as(f64, @floatFromInt(std.time.ns_per_s));
    const total_bytes = corpus.len * cfg.iterations * cfg.threads;
    const mb_per_second = @as(f64, @floatFromInt(total_bytes)) / seconds / 1_000_000.0;
    const mtok_per_second = @as(f64, @floatFromInt(token_total)) / seconds / 1_000_000.0;
    const cache_stats = hf.bpeCacheStats();

    var stdout_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buf);
    try stdout.interface.print(
        "runtime=std_io tokenizer_bytes={d} corpus_bytes={d} repeat={d} warmup_iterations={d} iterations={d} threads={d} internal_threads={d} " ++
            "tokens_per_iteration={d} token_hash={x} elapsed_seconds={d:.6} mb_per_second={d:.3} " ++
            "mtokens_per_second={d:.3} cache_entries={d} cache_bytes={d} cache_limit_bytes={d} cache_rejected_reservations={d}\n",
        .{
            tokenizer_json.len,
            corpus.len,
            cfg.repeat,
            cfg.warmup_iterations,
            cfg.iterations,
            cfg.threads,
            cfg.internal_threads,
            token_total / cfg.iterations / cfg.threads,
            token_hash,
            seconds,
            mb_per_second,
            mtok_per_second,
            cache_stats.entries,
            cache_stats.used_bytes,
            cache_stats.max_bytes,
            cache_stats.rejected_reservations,
        },
    );
    if (cfg.profile_bpe) {
        const profile = hf.bpeProfileSnapshot();
        try stdout.interface.print(
            "bpe_profile hits={d} misses={d} probes={d} key_bytes={d} token_ids={d} key_len_histogram={any} id_count_histogram={any}\n",
            .{
                profile.hits,
                profile.misses,
                profile.probes,
                profile.key_bytes,
                profile.token_ids,
                profile.key_len_histogram,
                profile.id_count_histogram,
            },
        );
    }
    try stdout.interface.flush();
}
