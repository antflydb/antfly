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
};

fn usage(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\usage: zig build bench-tokenizer -- <tokenizer.json> <corpus.txt> [--warmup N] [--iterations N] [--threads N]
        \\
        \\Measures the native Zig HuggingFace tokenizer's steady-state encodeInto
        \\throughput. The tokenizer and reusable output buffer persist across all
        \\iterations. Multiple threads share the tokenizer and its concurrent cache.
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
        } else {
            return error.UnknownArgument;
        }
    }
    if (cfg.iterations == 0 or cfg.threads == 0 or cfg.threads > 256) return error.InvalidConfiguration;
    return cfg;
}

fn nowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => {},
        else => unreachable,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s +
        @as(u64, @intCast(ts.nsec));
}

const Worker = struct {
    tokenizer: tokenizer_mod.Tokenizer,
    corpus: []const u8,
    iterations: usize,
    ready: *std.atomic.Value(u32),
    start: *std.atomic.Value(bool),
    token_total: usize = 0,
    failure: ?anyerror = null,

    fn run(self: *Worker) void {
        const allocator = std.heap.c_allocator;
        var ids: std.ArrayListUnmanaged(i32) = .empty;
        defer ids.deinit(allocator);

        _ = self.ready.fetchAdd(1, .acq_rel);
        while (!self.start.load(.acquire)) std.atomic.spinLoopHint();

        for (0..self.iterations) |_| {
            ids.clearRetainingCapacity();
            self.tokenizer.encodeInto(allocator, self.corpus, &ids) catch |err| {
                self.failure = err;
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
    const corpus = try std.Io.Dir.cwd().readFileAlloc(
        init.io,
        cfg.corpus_path,
        allocator,
        .limited(16 * 1024 * 1024 * 1024),
    );
    defer allocator.free(corpus);

    const hf = try tokenizer_mod.hf.HfTokenizer.loadFromBytes(allocator, tokenizer_json);
    defer hf.deinitSelf();
    const tokenizer = hf.tokenizer();

    var ids: std.ArrayListUnmanaged(i32) = .empty;
    defer ids.deinit(allocator);
    for (0..cfg.warmup_iterations) |_| {
        ids.clearRetainingCapacity();
        try tokenizer.encodeInto(allocator, corpus, &ids);
    }

    const workers = try allocator.alloc(Worker, cfg.threads);
    defer allocator.free(workers);
    const threads = try allocator.alloc(std.Thread, cfg.threads);
    defer allocator.free(threads);
    var ready = std.atomic.Value(u32).init(0);
    var start = std.atomic.Value(bool).init(false);
    var spawned: usize = 0;
    errdefer {
        start.store(true, .release);
        for (threads[0..spawned]) |thread| thread.join();
    }
    for (workers, 0..) |*worker, idx| {
        worker.* = .{
            .tokenizer = tokenizer,
            .corpus = corpus,
            .iterations = cfg.iterations,
            .ready = &ready,
            .start = &start,
        };
        threads[idx] = try std.Thread.spawn(.{}, Worker.run, .{worker});
        spawned += 1;
    }
    while (ready.load(.acquire) != cfg.threads) std.Thread.yield() catch {};

    const start_ns = nowNs();
    start.store(true, .release);
    for (threads) |thread| thread.join();
    const elapsed_ns = nowNs() - start_ns;

    ids.clearRetainingCapacity();
    try tokenizer.encodeInto(allocator, corpus, &ids);
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

    var stdout_buf: [4096]u8 = undefined;
    var stdout = std.Io.File.stdout().writerStreaming(init.io, &stdout_buf);
    try stdout.interface.print(
        "tokenizer_bytes={d} corpus_bytes={d} warmup_iterations={d} iterations={d} threads={d} " ++
            "tokens_per_iteration={d} token_hash={x} elapsed_seconds={d:.6} mb_per_second={d:.3} " ++
            "mtokens_per_second={d:.3}\n",
        .{
            tokenizer_json.len,
            corpus.len,
            cfg.warmup_iterations,
            cfg.iterations,
            cfg.threads,
            token_total / cfg.iterations / cfg.threads,
            token_hash,
            seconds,
            mb_per_second,
            mtok_per_second,
        },
    );
    try stdout.interface.flush();
}
