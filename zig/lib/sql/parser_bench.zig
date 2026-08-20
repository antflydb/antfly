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

const generated = @import("grammar/generated/root.zig");
const generated_parser = @import("generated_parser.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

const default_iterations = 1_000;
const max_iterations = 100_000;

const corpus = [_][]const u8{
    "SELECT id, status FROM docs WHERE id = $1 ORDER BY status LIMIT 10",
    "SELECT public.select FROM public.docs WHERE score >= 1.25E-3",
    "INSERT INTO docs (id, status) VALUES ($1, 'ready') RETURNING id",
    "UPDATE docs SET status = 'done' WHERE id = $1",
    "DELETE FROM docs WHERE id = $1",
    "CREATE TABLE public.docs (id text PRIMARY KEY, score text)",
    "CREATE INDEX docs_status_idx ON docs (status)",
    "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY",
    "SELECT * FROM docs FOR system_time AS OF '2026-01-01'",
};

const Mode = enum { parse, lex_parse, result, all };

const Config = struct {
    iterations: usize = default_iterations,
    mode: Mode = .parse,
};

const TokenizedCase = struct {
    sql: []const u8,
    tokens: []token_mod.Token,
};

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    allocated_total: usize = 0,
    live_bytes: usize = 0,
    peak_live_bytes: usize = 0,

    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn reset(self: *@This()) void {
        std.debug.assert(self.live_bytes == 0);
        self.allocated_total = 0;
        self.peak_live_bytes = 0;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const memory = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.allocated_total += len;
        self.live_bytes += len;
        self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        return memory;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        self.recordResize(memory.len, new_len);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        const remapped = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        self.recordResize(memory.len, new_len);
        return remapped;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        std.debug.assert(memory.len <= self.live_bytes);
        self.live_bytes -= memory.len;
        self.backing.rawFree(memory, alignment, ret_addr);
    }

    fn recordResize(self: *@This(), old_len: usize, new_len: usize) void {
        if (new_len > old_len) {
            const increase = new_len - old_len;
            self.allocated_total += increase;
            self.live_bytes += increase;
            self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
        } else {
            self.live_bytes -= old_len - new_len;
        }
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

const Summary = struct {
    mode: []const u8,
    operations: usize,
    tokens: usize,
    elapsed_ns: u64,
    allocated_bytes: usize,
    peak_live_bytes: usize,
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const config = parseArgs(allocator, init.minimal.args) catch |err| {
        std.debug.print("sql-parser-bench: {s}\n\n{s}", .{ @errorName(err), usage });
        std.process.exit(2);
    };

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;

    switch (config.mode) {
        .parse => try runParse(allocator, stdout, config.iterations),
        .lex_parse => try runLexParse(allocator, stdout, config.iterations),
        .result => try runResult(allocator, stdout, config.iterations),
        .all => {
            try runParse(allocator, stdout, config.iterations);
            try runLexParse(allocator, stdout, config.iterations);
            try runResult(allocator, stdout, config.iterations);
        },
    }
    try stdout.flush();
}

fn runResult(allocator: std.mem.Allocator, stdout: anytype, iterations: usize) !void {
    const cases = try tokenizeCorpus(allocator);
    defer freeTokenizedCorpus(allocator, cases);
    var tokens_per_iteration: usize = 0;
    for (cases) |case| tokens_per_iteration += case.tokens.len;

    const operation_count = iterations * corpus.len;
    const durations = try allocator.alloc(u64, operation_count);
    defer allocator.free(durations);
    var counting = CountingAllocator{ .backing = allocator };
    const operation_allocator = counting.allocator();
    var total_allocated: usize = 0;
    var peak_live: usize = 0;
    var sample: usize = 0;

    const started = monotonicNanos();
    for (0..iterations) |_| {
        for (corpus) |sql| {
            counting.reset();
            const operation_started = monotonicNanos();
            var result = try generated_parser.parseSqlResultAlloc(operation_allocator, sql);
            switch (result) {
                .success => {},
                .diagnostic => {
                    result.deinit(operation_allocator);
                    return error.UnexpectedCorpusDiagnostic;
                },
            }
            result.deinit(operation_allocator);
            durations[sample] = monotonicNanos() - operation_started;
            sample += 1;
            total_allocated += counting.allocated_total;
            peak_live = @max(peak_live, counting.peak_live_bytes);
            std.debug.assert(counting.live_bytes == 0);
        }
    }
    try printSummary(stdout, .{
        .mode = "result",
        .operations = operation_count,
        .tokens = tokens_per_iteration * iterations,
        .elapsed_ns = monotonicNanos() - started,
        .allocated_bytes = total_allocated,
        .peak_live_bytes = peak_live,
    }, durations);
}

fn runParse(allocator: std.mem.Allocator, stdout: anytype, iterations: usize) !void {
    const cases = try tokenizeCorpus(allocator);
    defer freeTokenizedCorpus(allocator, cases);
    for (cases) |case| try generated_parser.parseTokensAlloc(allocator, case.tokens);

    const operation_count = iterations * cases.len;
    const durations = try allocator.alloc(u64, operation_count);
    defer allocator.free(durations);
    var counting = CountingAllocator{ .backing = allocator };
    const parse_allocator = counting.allocator();
    var total_tokens: usize = 0;
    var total_allocated: usize = 0;
    var peak_live: usize = 0;
    var sample: usize = 0;

    const started = monotonicNanos();
    for (0..iterations) |_| {
        for (cases) |case| {
            counting.reset();
            const operation_started = monotonicNanos();
            try generated_parser.parseTokensAlloc(parse_allocator, case.tokens);
            durations[sample] = monotonicNanos() - operation_started;
            sample += 1;
            total_tokens += case.tokens.len;
            total_allocated += counting.allocated_total;
            peak_live = @max(peak_live, counting.peak_live_bytes);
            std.debug.assert(counting.live_bytes == 0);
        }
    }
    try printSummary(stdout, .{
        .mode = "parse",
        .operations = operation_count,
        .tokens = total_tokens,
        .elapsed_ns = monotonicNanos() - started,
        .allocated_bytes = total_allocated,
        .peak_live_bytes = peak_live,
    }, durations);
}

fn runLexParse(allocator: std.mem.Allocator, stdout: anytype, iterations: usize) !void {
    for (corpus) |sql| try generated_parser.parseSqlAlloc(allocator, sql);

    const operation_count = iterations * corpus.len;
    const durations = try allocator.alloc(u64, operation_count);
    defer allocator.free(durations);
    var counting = CountingAllocator{ .backing = allocator };
    const operation_allocator = counting.allocator();
    var total_tokens: usize = 0;
    var total_allocated: usize = 0;
    var peak_live: usize = 0;
    var sample: usize = 0;

    const started = monotonicNanos();
    for (0..iterations) |_| {
        for (corpus) |sql| {
            counting.reset();
            const operation_started = monotonicNanos();
            var tokens = try lexer.tokenizeAlloc(operation_allocator, sql);
            generated_parser.parseTokensAlloc(operation_allocator, tokens.items) catch |err| {
                lexer.freeTokens(operation_allocator, &tokens);
                return err;
            };
            durations[sample] = monotonicNanos() - operation_started;
            sample += 1;
            total_tokens += tokens.items.len;
            total_allocated += counting.allocated_total;
            peak_live = @max(peak_live, counting.peak_live_bytes);
            lexer.freeTokens(operation_allocator, &tokens);
            std.debug.assert(counting.live_bytes == 0);
        }
    }
    try printSummary(stdout, .{
        .mode = "lex_parse",
        .operations = operation_count,
        .tokens = total_tokens,
        .elapsed_ns = monotonicNanos() - started,
        .allocated_bytes = total_allocated,
        .peak_live_bytes = peak_live,
    }, durations);
}

fn parseArgs(allocator: std.mem.Allocator, args: std.process.Args) !Config {
    var config: Config = .{};
    var iterator = try std.process.Args.Iterator.initAllocator(args, allocator);
    defer iterator.deinit();
    _ = iterator.skip();
    while (iterator.next()) |arg| {
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            std.debug.print("{s}", .{usage});
            std.process.exit(0);
        } else if (std.mem.eql(u8, arg, "--iterations")) {
            config.iterations = try std.fmt.parseUnsigned(usize, iterator.next() orelse return error.MissingIterations, 10);
        } else if (std.mem.eql(u8, arg, "--mode")) {
            const mode = iterator.next() orelse return error.MissingMode;
            if (std.mem.eql(u8, mode, "parse")) config.mode = .parse else if (std.mem.eql(u8, mode, "lex-parse")) config.mode = .lex_parse else if (std.mem.eql(u8, mode, "result")) config.mode = .result else if (std.mem.eql(u8, mode, "all")) config.mode = .all else return error.InvalidMode;
        } else {
            config.iterations = try std.fmt.parseUnsigned(usize, arg, 10);
        }
    }
    if (config.iterations == 0 or config.iterations > max_iterations) return error.InvalidIterations;
    return config;
}

const usage =
    \\usage: zig build sql-parser-bench -- [iterations] [--mode parse|lex-parse|result|all]
    \\
    \\  iterations must be between 1 and 100000 (default: 1000)
    \\  parse measures the pre-tokenized hot path; lex-parse includes tokenization
    \\  result measures the single-pass user-facing parse and diagnostic API
    \\
;

fn tokenizeCorpus(allocator: std.mem.Allocator) ![]TokenizedCase {
    const cases = try allocator.alloc(TokenizedCase, corpus.len);
    var initialized: usize = 0;
    errdefer {
        for (cases[0..initialized]) |case| freeTokenSlice(allocator, case.tokens);
        allocator.free(cases);
    }
    for (corpus, 0..) |sql, index| {
        var tokens = try lexer.tokenizeAlloc(allocator, sql);
        cases[index] = .{ .sql = sql, .tokens = try tokens.toOwnedSlice(allocator) };
        initialized += 1;
    }
    return cases;
}

fn freeTokenizedCorpus(allocator: std.mem.Allocator, cases: []TokenizedCase) void {
    for (cases) |case| freeTokenSlice(allocator, case.tokens);
    allocator.free(cases);
}

fn freeTokenSlice(allocator: std.mem.Allocator, tokens: []token_mod.Token) void {
    var list = std.ArrayListUnmanaged(token_mod.Token){ .items = tokens, .capacity = tokens.len };
    lexer.freeTokens(allocator, &list);
}

fn printSummary(stdout: anytype, summary: Summary, durations: []u64) !void {
    std.mem.sort(u64, durations, {}, std.sort.asc(u64));
    const elapsed_seconds = @as(f64, @floatFromInt(summary.elapsed_ns)) / std.time.ns_per_s;
    const operations = @as(f64, @floatFromInt(summary.operations));
    try stdout.print(
        "{{\"benchmark\":\"sql_parser\",\"mode\":\"{s}\",\"corpus_statements\":{},\"operations\":{},\"tokens\":{},\"elapsed_ns\":{},\"latency_ns\":{{\"avg\":{d:.2},\"p50\":{},\"p95\":{},\"p99\":{},\"max\":{}}},\"throughput\":{{\"statements_per_second\":{d:.2},\"tokens_per_second\":{d:.2}}},\"allocation\":{{\"bytes_per_statement\":{d:.2},\"peak_live_bytes\":{}}},\"generated_table\":{{\"states\":{},\"actions\":{},\"gotos\":{},\"rules\":{},\"productions\":{},\"estimated_bytes\":{},\"conflicts\":{}}}}}\n",
        .{
            summary.mode,
            corpus.len,
            summary.operations,
            summary.tokens,
            summary.elapsed_ns,
            @as(f64, @floatFromInt(summary.elapsed_ns)) / operations,
            percentile(durations, 50),
            percentile(durations, 95),
            percentile(durations, 99),
            durations[durations.len - 1],
            operations / elapsed_seconds,
            @as(f64, @floatFromInt(summary.tokens)) / elapsed_seconds,
            @as(f64, @floatFromInt(summary.allocated_bytes)) / operations,
            summary.peak_live_bytes,
            generated.state_count,
            generated.action_count,
            generated.goto_count,
            generated.rule_count,
            generated.production_count,
            generated.parse_table_estimated_bytes,
            generated.conflict_count,
        },
    );
}

fn percentile(sorted: []const u64, percent: usize) u64 {
    return sorted[((sorted.len - 1) * percent) / 100];
}

fn monotonicNanos() u64 {
    var timestamp: std.posix.timespec = undefined;
    return switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &timestamp))) {
        .SUCCESS => @intCast(@as(i128, timestamp.sec) * std.time.ns_per_s + timestamp.nsec),
        else => @panic("monotonic clock unavailable"),
    };
}
