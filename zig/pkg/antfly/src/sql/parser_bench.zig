// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const generated = @import("grammar/generated/root.zig");
const generated_parser = @import("generated_parser.zig");
const lexer = @import("lexer.zig");
const token_mod = @import("token.zig");

const default_iterations = 100;

const corpus = generated_parser.first_family_corpus ++
    generated_parser.simple_ddl_corpus ++
    generated_parser.simple_dml_corpus ++
    generated_parser.simple_read_corpus ++
    generated_parser.antfly_extension_read_corpus ++
    generated_parser.simple_graph_corpus ++
    generated_parser.unsupported_corpus;

const TokenizedCase = struct {
    sql: []const u8,
    tokens: []token_mod.Token,
};

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    allocated_total: usize = 0,
    live_bytes: usize = 0,
    peak_live_bytes: usize = 0,

    pub fn init(backing: std.mem.Allocator) CountingAllocator {
        return .{ .backing = backing };
    }

    pub fn reset(self: *CountingAllocator) void {
        self.allocated_total = 0;
        self.live_bytes = 0;
        self.peak_live_bytes = 0;
    }

    pub fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn recordAlloc(self: *CountingAllocator, len: usize) void {
        self.allocated_total += len;
        self.live_bytes += len;
        self.peak_live_bytes = @max(self.peak_live_bytes, self.live_bytes);
    }

    fn recordFree(self: *CountingAllocator, len: usize) void {
        self.live_bytes -|= len;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        self.recordAlloc(len);
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) return false;
        if (new_len > memory.len) {
            self.recordAlloc(new_len - memory.len);
        } else {
            self.recordFree(memory.len - new_len);
        }
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (new_len > memory.len) {
            self.recordAlloc(new_len - memory.len);
        } else {
            self.recordFree(memory.len - new_len);
        }
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.recordFree(memory.len);
        self.backing.rawFree(memory, alignment, ret_addr);
    }

    const vtable = std.mem.Allocator.VTable{
        .alloc = alloc,
        .resize = resize,
        .remap = remap,
        .free = free,
    };
};

pub fn main(init: std.process.Init) !void {
    var debug_allocator = std.heap.DebugAllocator(.{}).init;
    defer _ = debug_allocator.deinit();
    const base_alloc = debug_allocator.allocator();

    const iterations = try parseIterations(base_alloc, init.minimal.args);
    const cases = try tokenizeCorpus(base_alloc);
    defer freeTokenizedCorpus(base_alloc, cases);

    const total_parses = iterations * cases.len;
    var durations = try base_alloc.alloc(u64, total_parses);
    defer base_alloc.free(durations);

    var counting = CountingAllocator.init(base_alloc);
    const parse_alloc = counting.allocator();

    var duration_index: usize = 0;
    var total_tokens: usize = 0;
    var total_allocated_bytes: usize = 0;
    var peak_live_bytes: usize = 0;

    const started = monotonicNanos();
    for (0..iterations) |_| {
        for (cases) |case| {
            counting.reset();
            const parse_started = monotonicNanos();
            var parsed = try generated_parser.parseTokensAlloc(parse_alloc, case.tokens);
            const parse_elapsed = monotonicNanos() - parse_started;

            durations[duration_index] = @intCast(parse_elapsed);
            duration_index += 1;
            total_tokens += case.tokens.len;
            total_allocated_bytes += counting.allocated_total;
            peak_live_bytes = @max(peak_live_bytes, counting.peak_live_bytes);
            parsed.deinit(parse_alloc);
        }
    }
    const elapsed_ns = monotonicNanos() - started;

    std.mem.sort(u64, durations, {}, std.sort.asc(u64));
    const total_parses_f: f64 = @floatFromInt(total_parses);
    const elapsed_seconds = @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;
    const avg_ns_per_statement = @as(f64, @floatFromInt(elapsed_ns)) / total_parses_f;
    const tokens_per_second = @as(f64, @floatFromInt(total_tokens)) / elapsed_seconds;
    const allocated_bytes_per_statement = @as(f64, @floatFromInt(total_allocated_bytes)) / total_parses_f;
    const action_range_avg = if (generated.state_count == 0) 0 else @as(f64, @floatFromInt(generated.action_count)) / @as(f64, @floatFromInt(generated.state_count));
    const goto_range_avg = if (generated.state_count == 0) 0 else @as(f64, @floatFromInt(generated.goto_count)) / @as(f64, @floatFromInt(generated.state_count));

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print(
        \\sql_parser_bench statements={} iterations={} parses={} tokens={} elapsed_ns={}
        \\latency_ns avg={d:.2} p50={} p95={} p99={} max={}
        \\throughput tokens_per_second={d:.2} statements_per_second={d:.2}
        \\allocation bytes_per_statement={d:.2} peak_live_bytes={}
        \\generated_table states={} actions={} gotos={} rules={} productions={} rhs={} state_items={} static_bytes={} symbol_name_bytes={} estimated_bytes={}
        \\generated_table_entry_bytes action={} goto={} range={}
        \\generated_table_rows action_max={} action_avg={d:.2} goto_max={} goto_avg={d:.2}
        \\
    , .{
        cases.len,
        iterations,
        total_parses,
        total_tokens,
        elapsed_ns,
        avg_ns_per_statement,
        percentile(durations, 50),
        percentile(durations, 95),
        percentile(durations, 99),
        durations[durations.len - 1],
        tokens_per_second,
        total_parses_f / elapsed_seconds,
        allocated_bytes_per_statement,
        peak_live_bytes,
        generated.state_count,
        generated.action_count,
        generated.goto_count,
        generated.rule_count,
        generated.production_count,
        generated.production_rhs_count,
        generated.state_item_count,
        generated.parse_table_static_bytes,
        generated.symbol_name_bytes,
        generated.parse_table_estimated_bytes,
        generated.action_entry_bytes,
        generated.goto_entry_bytes,
        generated.table_range_entry_bytes,
        generated.action_range_max,
        action_range_avg,
        generated.goto_range_max,
        goto_range_avg,
    });
    try stdout.flush();
}

fn parseIterations(alloc: std.mem.Allocator, args: std.process.Args) !usize {
    var iterator = try std.process.Args.Iterator.initAllocator(args, alloc);
    defer iterator.deinit();
    _ = iterator.skip();
    const first = iterator.next() orelse return default_iterations;
    if (std.mem.eql(u8, first, "--iterations")) {
        const value = iterator.next() orelse return error.MissingIterations;
        return try std.fmt.parseUnsigned(usize, value, 10);
    }
    return try std.fmt.parseUnsigned(usize, first, 10);
}

fn monotonicNanos() u64 {
    var ts: std.c.timespec = undefined;
    const rc = std.c.clock_gettime(.MONOTONIC, &ts);
    std.debug.assert(rc == 0);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn tokenizeCorpus(alloc: std.mem.Allocator) ![]TokenizedCase {
    var cases = try alloc.alloc(TokenizedCase, corpus.len);
    var initialized: usize = 0;
    errdefer {
        for (cases[0..initialized]) |case| {
            var tokens = std.ArrayListUnmanaged(token_mod.Token){ .items = case.tokens, .capacity = case.tokens.len };
            lexer.freeTokens(alloc, &tokens);
        }
        alloc.free(cases);
    }

    for (corpus, 0..) |case, index| {
        var tokens = try lexer.tokenizeAlloc(alloc, case.sql);
        cases[index] = .{
            .sql = case.sql,
            .tokens = try tokens.toOwnedSlice(alloc),
        };
        initialized += 1;
    }
    return cases;
}

fn freeTokenizedCorpus(alloc: std.mem.Allocator, cases: []TokenizedCase) void {
    for (cases) |case| {
        var tokens = std.ArrayListUnmanaged(token_mod.Token){ .items = case.tokens, .capacity = case.tokens.len };
        lexer.freeTokens(alloc, &tokens);
    }
    alloc.free(cases);
}

fn percentile(sorted: []const u64, pct: usize) u64 {
    if (sorted.len == 0) return 0;
    const idx = ((sorted.len - 1) * pct) / 100;
    return sorted[idx];
}
