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
const document_plan = @import("document_plan.zig");
const lexer = @import("lexer.zig");
const lower_select = @import("lower_select.zig");
const schema_api = @import("../schema/mod.zig");
const storage_schema = @import("../storage/schema.zig");
const token_mod = @import("token.zig");
const tokenized = @import("tokenized.zig");

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

const BenchMode = enum {
    lex,
    parse,
    lex_parse,
    lower,
    all,
};

const Config = struct {
    iterations: usize = default_iterations,
    mode: BenchMode = .parse,
};

const LowerKind = enum {
    relational,
    document,
};

const LowerCase = struct {
    sql: []const u8,
    kind: LowerKind,
    parsed: tokenized.ParsedSql,
};

const LowerSchemas = struct {
    relational: storage_schema.TableSchema,
    document: storage_schema.TableSchema,

    fn deinit(self: *LowerSchemas, alloc: std.mem.Allocator) void {
        storage_schema.freeSchema(alloc, self.relational);
        storage_schema.freeSchema(alloc, self.document);
        self.* = undefined;
    }
};

const TimingSummary = struct {
    label: []const u8,
    statements: usize,
    iterations: usize,
    operations: usize,
    tokens: usize,
    elapsed_ns: u64,
    avg_ns_per_statement: f64,
    operations_per_second: f64,
    tokens_per_second: f64,
    allocated_bytes_per_statement: f64,
    peak_live_bytes: usize,
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

    const cfg = try parseArgs(base_alloc, init.minimal.args);

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;

    switch (cfg.mode) {
        .lex => try runLexBenchmark(base_alloc, stdout, cfg.iterations),
        .parse => try runParseBenchmark(base_alloc, stdout, cfg.iterations),
        .lex_parse => try runLexParseBenchmark(base_alloc, stdout, cfg.iterations),
        .lower => try runLowerBenchmark(base_alloc, stdout, cfg.iterations),
        .all => {
            try runLexBenchmark(base_alloc, stdout, cfg.iterations);
            try runParseBenchmark(base_alloc, stdout, cfg.iterations);
            try runLexParseBenchmark(base_alloc, stdout, cfg.iterations);
            try runLowerBenchmark(base_alloc, stdout, cfg.iterations);
        },
    }
    try stdout.flush();
}

fn runLexBenchmark(base_alloc: std.mem.Allocator, stdout: anytype, iterations: usize) !void {
    const total_ops = iterations * corpus.len;
    var durations = try base_alloc.alloc(u64, total_ops);
    defer base_alloc.free(durations);

    var counting = CountingAllocator.init(base_alloc);
    const op_alloc = counting.allocator();

    var duration_index: usize = 0;
    var total_tokens: usize = 0;
    var total_allocated_bytes: usize = 0;
    var peak_live_bytes: usize = 0;

    const started = monotonicNanos();
    for (0..iterations) |_| {
        for (corpus) |entry| {
            counting.reset();
            const op_started = monotonicNanos();
            var tokens = try lexer.tokenizeAlloc(op_alloc, entry.sql);
            const op_elapsed = monotonicNanos() - op_started;

            durations[duration_index] = @intCast(op_elapsed);
            duration_index += 1;
            total_tokens += tokens.items.len;
            total_allocated_bytes += counting.allocated_total;
            peak_live_bytes = @max(peak_live_bytes, counting.peak_live_bytes);
            lexer.freeTokens(op_alloc, &tokens);
        }
    }
    const elapsed_ns = monotonicNanos() - started;

    try printBenchmarkSummary(stdout, .{
        .label = "lex",
        .statements = corpus.len,
        .iterations = iterations,
        .operations = total_ops,
        .tokens = total_tokens,
        .elapsed_ns = elapsed_ns,
        .avg_ns_per_statement = avgNs(elapsed_ns, total_ops),
        .operations_per_second = opsPerSecond(total_ops, elapsed_ns),
        .tokens_per_second = tokensPerSecond(total_tokens, elapsed_ns),
        .allocated_bytes_per_statement = avgBytes(total_allocated_bytes, total_ops),
        .peak_live_bytes = peak_live_bytes,
    }, durations);
}

fn runParseBenchmark(base_alloc: std.mem.Allocator, stdout: anytype, iterations: usize) !void {
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
            var parsed = generated_parser.parseTokensAlloc(parse_alloc, case.tokens) catch |err| {
                std.debug.print("sql_parser_bench parse error={s} sql=\"{s}\"\n", .{ @errorName(err), case.sql });
                return err;
            };
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

    try printBenchmarkSummary(stdout, .{
        .label = "parse",
        .statements = cases.len,
        .iterations = iterations,
        .operations = total_parses,
        .tokens = total_tokens,
        .elapsed_ns = elapsed_ns,
        .avg_ns_per_statement = avgNs(elapsed_ns, total_parses),
        .operations_per_second = opsPerSecond(total_parses, elapsed_ns),
        .tokens_per_second = tokensPerSecond(total_tokens, elapsed_ns),
        .allocated_bytes_per_statement = avgBytes(total_allocated_bytes, total_parses),
        .peak_live_bytes = peak_live_bytes,
    }, durations);
}

fn runLexParseBenchmark(base_alloc: std.mem.Allocator, stdout: anytype, iterations: usize) !void {
    const total_ops = iterations * corpus.len;
    var durations = try base_alloc.alloc(u64, total_ops);
    defer base_alloc.free(durations);

    var counting = CountingAllocator.init(base_alloc);
    const op_alloc = counting.allocator();

    var duration_index: usize = 0;
    var total_tokens: usize = 0;
    var total_allocated_bytes: usize = 0;
    var peak_live_bytes: usize = 0;

    const started = monotonicNanos();
    for (0..iterations) |_| {
        for (corpus) |entry| {
            counting.reset();
            const op_started = monotonicNanos();
            var tokens = try lexer.tokenizeAlloc(op_alloc, entry.sql);
            var parsed = generated_parser.parseTokensAlloc(op_alloc, tokens.items) catch |err| {
                lexer.freeTokens(op_alloc, &tokens);
                std.debug.print("sql_parser_bench lex_parse error={s} sql=\"{s}\"\n", .{ @errorName(err), entry.sql });
                return err;
            };
            const op_elapsed = monotonicNanos() - op_started;

            durations[duration_index] = @intCast(op_elapsed);
            duration_index += 1;
            total_tokens += tokens.items.len;
            total_allocated_bytes += counting.allocated_total;
            peak_live_bytes = @max(peak_live_bytes, counting.peak_live_bytes);
            parsed.deinit(op_alloc);
            lexer.freeTokens(op_alloc, &tokens);
        }
    }
    const elapsed_ns = monotonicNanos() - started;

    try printBenchmarkSummary(stdout, .{
        .label = "lex_parse",
        .statements = corpus.len,
        .iterations = iterations,
        .operations = total_ops,
        .tokens = total_tokens,
        .elapsed_ns = elapsed_ns,
        .avg_ns_per_statement = avgNs(elapsed_ns, total_ops),
        .operations_per_second = opsPerSecond(total_ops, elapsed_ns),
        .tokens_per_second = tokensPerSecond(total_tokens, elapsed_ns),
        .allocated_bytes_per_statement = avgBytes(total_allocated_bytes, total_ops),
        .peak_live_bytes = peak_live_bytes,
    }, durations);
}

fn runLowerBenchmark(base_alloc: std.mem.Allocator, stdout: anytype, iterations: usize) !void {
    var schemas = try initLowerSchemas(base_alloc);
    defer schemas.deinit(base_alloc);

    const cases = try initLowerCases(base_alloc);
    defer freeLowerCases(base_alloc, cases);

    const total_ops = iterations * cases.len;
    var durations = try base_alloc.alloc(u64, total_ops);
    defer base_alloc.free(durations);

    var counting = CountingAllocator.init(base_alloc);
    const lower_alloc = counting.allocator();

    var duration_index: usize = 0;
    var total_tokens: usize = 0;
    var total_allocated_bytes: usize = 0;
    var peak_live_bytes: usize = 0;

    const started = monotonicNanos();
    for (0..iterations) |_| {
        for (cases) |*entry| {
            counting.reset();
            const op_started = monotonicNanos();
            switch (entry.kind) {
                .relational => {
                    var lowered = lower_select.lowerReadPlanWithOptionalSourceSchemaParsedSqlAlloc(
                        lower_alloc,
                        &entry.parsed,
                        schemas.relational,
                        null,
                        &.{},
                        .{},
                    ) catch |err| {
                        std.debug.print("sql_parser_bench lower error={s} sql=\"{s}\"\n", .{ @errorName(err), entry.sql });
                        return err;
                    };
                    lowered.deinit(lower_alloc);
                },
                .document => {
                    var lowered = document_plan.lowerDocumentReadPlanWithBoundedScanPolicyParsedSqlAlloc(
                        lower_alloc,
                        &entry.parsed,
                        schemas.document,
                        .{ .max_rows = 10_000 },
                    ) catch |err| {
                        std.debug.print("sql_parser_bench lower error={s} sql=\"{s}\"\n", .{ @errorName(err), entry.sql });
                        return err;
                    };
                    lowered.deinit(lower_alloc);
                },
            }
            const op_elapsed = monotonicNanos() - op_started;

            durations[duration_index] = @intCast(op_elapsed);
            duration_index += 1;
            total_tokens += entry.parsed.items().len;
            total_allocated_bytes += counting.allocated_total;
            peak_live_bytes = @max(peak_live_bytes, counting.peak_live_bytes);
        }
    }
    const elapsed_ns = monotonicNanos() - started;

    try printBenchmarkSummary(stdout, .{
        .label = "lower",
        .statements = cases.len,
        .iterations = iterations,
        .operations = total_ops,
        .tokens = total_tokens,
        .elapsed_ns = elapsed_ns,
        .avg_ns_per_statement = avgNs(elapsed_ns, total_ops),
        .operations_per_second = opsPerSecond(total_ops, elapsed_ns),
        .tokens_per_second = tokensPerSecond(total_tokens, elapsed_ns),
        .allocated_bytes_per_statement = avgBytes(total_allocated_bytes, total_ops),
        .peak_live_bytes = peak_live_bytes,
    }, durations);
}

fn parseArgs(alloc: std.mem.Allocator, args: std.process.Args) !Config {
    var cfg = Config{};
    var iterator = try std.process.Args.Iterator.initAllocator(args, alloc);
    defer iterator.deinit();
    _ = iterator.skip();
    while (iterator.next()) |arg| {
        if (std.mem.eql(u8, arg, "--iterations")) {
            const value = iterator.next() orelse return error.MissingIterations;
            cfg.iterations = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--mode")) {
            const value = iterator.next() orelse return error.MissingMode;
            cfg.mode = parseMode(value) orelse return error.InvalidMode;
        } else {
            cfg.iterations = try std.fmt.parseUnsigned(usize, arg, 10);
        }
    }
    if (cfg.iterations == 0) return error.InvalidIterations;
    return cfg;
}

fn parseMode(raw: []const u8) ?BenchMode {
    if (std.mem.eql(u8, raw, "lex")) return .lex;
    if (std.mem.eql(u8, raw, "parse")) return .parse;
    if (std.mem.eql(u8, raw, "lex-parse") or std.mem.eql(u8, raw, "lex_parse")) return .lex_parse;
    if (std.mem.eql(u8, raw, "lower")) return .lower;
    if (std.mem.eql(u8, raw, "all")) return .all;
    return null;
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

fn initLowerSchemas(alloc: std.mem.Allocator) !LowerSchemas {
    var relational_parsed = schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"relational","default_type":"items","enforce_types":true,"document_schemas":{"items":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"tenant_id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"created_at":{"type":"numeric"},"body":{"type":"text"},"tags":{"type":"array","items":{"type":"keyword"}},"attrs":{"type":"json"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ) catch |err| {
        std.debug.print("sql_parser_bench lower schema=relational error={s}\n", .{@errorName(err)});
        return err;
    };
    defer relational_parsed.deinit(alloc);
    const relational = try schema_api.deriveRuntimeTableSchema(alloc, relational_parsed);
    errdefer storage_schema.freeSchema(alloc, relational);

    var document_parsed = schema_api.parseValidatedTableSchema(alloc,
        \\{"version":1,"storage_mode":"document","default_type":"doc","enforce_types":false,"document_schemas":{"doc":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"title":{"type":"text"},"tags":{"type":"array","items":{"type":"keyword"}},"metadata":{"type":"object","properties":{"tier":{"type":"keyword"}}}},"additionalProperties":true}}}}
    ) catch |err| {
        std.debug.print("sql_parser_bench lower schema=document error={s}\n", .{@errorName(err)});
        return err;
    };
    defer document_parsed.deinit(alloc);
    const document = try schema_api.deriveRuntimeTableSchema(alloc, document_parsed);

    return .{
        .relational = relational,
        .document = document,
    };
}

fn initLowerCases(alloc: std.mem.Allocator) ![]LowerCase {
    const definitions = [_]struct {
        sql: []const u8,
        kind: LowerKind,
    }{
        .{ .kind = .relational, .sql = "SELECT id, status FROM items WHERE status = 'open' LIMIT 10" },
        .{ .kind = .relational, .sql = "SELECT id, amount FROM items WHERE amount > 100 ORDER BY amount DESC LIMIT 20" },
        .{ .kind = .relational, .sql = "SELECT status, count(*) FROM items GROUP BY status" },
        .{ .kind = .document, .sql = "SELECT id, status FROM doc WHERE status = 'open' LIMIT 10" },
        .{ .kind = .document, .sql = "SELECT title, amount FROM doc WHERE amount >= 5 ORDER BY amount DESC LIMIT 20" },
        .{ .kind = .document, .sql = "SELECT status FROM doc WHERE status != 'closed' LIMIT 10" },
    };

    var cases = try alloc.alloc(LowerCase, definitions.len);
    var initialized: usize = 0;
    errdefer {
        for (cases[0..initialized]) |*entry| entry.parsed.deinit(alloc);
        alloc.free(cases);
    }

    for (definitions, 0..) |definition, index| {
        cases[index] = .{
            .sql = definition.sql,
            .kind = definition.kind,
            .parsed = tokenized.ParsedSql.initAlloc(alloc, definition.sql) catch |err| {
                std.debug.print("sql_parser_bench lower setup error={s} sql=\"{s}\"\n", .{ @errorName(err), definition.sql });
                return err;
            },
        };
        initialized += 1;
    }
    return cases;
}

fn freeLowerCases(alloc: std.mem.Allocator, cases: []LowerCase) void {
    for (cases) |*entry| entry.parsed.deinit(alloc);
    alloc.free(cases);
}

fn printBenchmarkSummary(stdout: anytype, summary: TimingSummary, durations: []u64) !void {
    std.mem.sort(u64, durations, {}, std.sort.asc(u64));
    const action_range_avg = if (generated.state_count == 0) 0 else @as(f64, @floatFromInt(generated.action_count)) / @as(f64, @floatFromInt(generated.state_count));
    const goto_range_avg = if (generated.state_count == 0) 0 else @as(f64, @floatFromInt(generated.goto_count)) / @as(f64, @floatFromInt(generated.state_count));

    try stdout.print(
        \\sql_parser_bench mode={s} statements={} iterations={} operations={} tokens={} elapsed_ns={}
        \\latency_ns avg={d:.2} p50={} p95={} p99={} max={}
        \\throughput tokens_per_second={d:.2} statements_per_second={d:.2}
        \\allocation bytes_per_statement={d:.2} peak_live_bytes={}
        \\generated_table states={} actions={} gotos={} rules={} productions={} rhs={} state_items={} static_bytes={} symbol_name_bytes={} estimated_bytes={}
        \\generated_table_entry_bytes action={} goto={} range={}
        \\generated_table_rows action_max={} action_avg={d:.2} goto_max={} goto_avg={d:.2}
        \\
    , .{
        summary.label,
        summary.statements,
        summary.iterations,
        summary.operations,
        summary.tokens,
        summary.elapsed_ns,
        summary.avg_ns_per_statement,
        percentile(durations, 50),
        percentile(durations, 95),
        percentile(durations, 99),
        durations[durations.len - 1],
        summary.tokens_per_second,
        summary.operations_per_second,
        summary.allocated_bytes_per_statement,
        summary.peak_live_bytes,
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
}

fn avgNs(elapsed_ns: u64, operations: usize) f64 {
    return @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, @floatFromInt(operations));
}

fn avgBytes(bytes: usize, operations: usize) f64 {
    return @as(f64, @floatFromInt(bytes)) / @as(f64, @floatFromInt(operations));
}

fn opsPerSecond(operations: usize, elapsed_ns: u64) f64 {
    return @as(f64, @floatFromInt(operations)) / elapsedSeconds(elapsed_ns);
}

fn tokensPerSecond(tokens: usize, elapsed_ns: u64) f64 {
    return @as(f64, @floatFromInt(tokens)) / elapsedSeconds(elapsed_ns);
}

fn elapsedSeconds(elapsed_ns: u64) f64 {
    return @as(f64, @floatFromInt(elapsed_ns)) / std.time.ns_per_s;
}

fn percentile(sorted: []const u64, pct: usize) u64 {
    if (sorted.len == 0) return 0;
    const idx = ((sorted.len - 1) * pct) / 100;
    return sorted[idx];
}
