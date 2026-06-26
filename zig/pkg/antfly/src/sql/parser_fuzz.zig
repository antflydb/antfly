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

const generated_parser = @import("generated_parser.zig");
const lexer = @import("lexer.zig");

const default_cases = 16_384;
const default_seed: u64 = 0x5151_4c46_555a_5a21;

const corpus = generated_parser.first_family_corpus ++
    generated_parser.simple_ddl_corpus ++
    generated_parser.simple_dml_corpus ++
    generated_parser.simple_read_corpus ++
    generated_parser.antfly_extension_read_corpus ++
    generated_parser.simple_graph_corpus ++
    generated_parser.unsupported_corpus;

const Options = struct {
    cases: usize = default_cases,
    seed: u64 = default_seed,
};

const FuzzStats = struct {
    attempted: usize = 0,
    parsed: usize = 0,
    diagnosed: usize = 0,
    unsupported_token_shape: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    var debug_allocator = std.heap.DebugAllocator(.{}).init;
    defer _ = debug_allocator.deinit();
    const alloc = debug_allocator.allocator();

    const options = try parseOptions(alloc, init.minimal.args);
    var prng = std.Random.DefaultPrng.init(options.seed);
    const random = prng.random();

    var stats: FuzzStats = .{};
    for (corpus) |case| try exerciseGeneratedParserFuzzSql(alloc, case.sql, &stats);
    for (0..options.cases) |case_index| {
        var buffer: [512]u8 = undefined;
        const sql = if (case_index % 3 == 0)
            generatedParserRandomFuzzSql(random, &buffer)
        else
            generatedParserMutatedFuzzSql(random, corpus[random.intRangeLessThan(usize, 0, corpus.len)].sql, &buffer);
        try exerciseGeneratedParserFuzzSql(alloc, sql, &stats);
    }

    var stdout_buffer: [1024]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(init.io, &stdout_buffer);
    const stdout = &stdout_writer.interface;
    try stdout.print(
        \\sql_parser_fuzz seed={} corpus={} generated_cases={} attempted={} parsed={} diagnosed={} unsupported_token_shape={}
        \\
    , .{
        options.seed,
        corpus.len,
        options.cases,
        stats.attempted,
        stats.parsed,
        stats.diagnosed,
        stats.unsupported_token_shape,
    });
    try stdout.flush();
}

fn parseOptions(alloc: std.mem.Allocator, args: std.process.Args) !Options {
    var options: Options = .{};
    var iterator = try std.process.Args.Iterator.initAllocator(args, alloc);
    defer iterator.deinit();
    _ = iterator.skip();
    while (iterator.next()) |arg| {
        if (std.mem.eql(u8, arg, "--cases")) {
            const value = iterator.next() orelse return error.MissingCases;
            options.cases = try std.fmt.parseUnsigned(usize, value, 10);
        } else if (std.mem.eql(u8, arg, "--seed")) {
            const value = iterator.next() orelse return error.MissingSeed;
            options.seed = try std.fmt.parseUnsigned(u64, value, 0);
        } else {
            return error.UnknownArgument;
        }
    }
    return options;
}

fn appendFuzzSqlPart(buffer: []u8, len: *usize, part: []const u8) void {
    if (len.* + part.len > buffer.len) return;
    @memcpy(buffer[len.* .. len.* + part.len], part);
    len.* += part.len;
}

fn appendFuzzSqlByte(buffer: []u8, len: *usize, byte: u8) void {
    if (len.* == buffer.len) return;
    buffer[len.*] = byte;
    len.* += 1;
}

fn generatedParserRandomFuzzSql(random: std.Random, buffer: []u8) []const u8 {
    const parts = [_][]const u8{
        "SELECT",      "WITH",             "RECURSIVE",   "INSERT",
        "UPDATE",      "DELETE",           "CREATE",      "DROP",
        "ALTER",       "EXPLAIN",          "FROM",        "WHERE",
        "GROUP",       "ORDER",            "BY",          "LIMIT",
        "OFFSET",      "FETCH",            "FIRST",       "ROWS",
        "ONLY",        "AS",               "JOIN",        "LEFT",
        "RIGHT",       "FULL",             "ON",          "UNION",
        "INTERSECT",   "EXCEPT",           "ALL",         "VALUES",
        "SET",         "INTO",             "TABLE",       "INDEX",
        "GRAPH",       "METRIC",           "antfly",      "graph_metric",
        "graph_match", "full_text_search", "id",          "status",
        "tenant",      "usage_records",    "source_rows", "1",
        "42",          "'open'",           "$1",          "(",
        ")",           ",",                ".",           "*",
        "=",           "<>",               "::",          "+",
        "-",
    };
    var len: usize = 0;
    const part_count = random.intRangeLessThan(usize, 1, 64);
    for (0..part_count) |idx| {
        if (idx != 0 and random.boolean()) appendFuzzSqlByte(buffer, &len, ' ');
        appendFuzzSqlPart(buffer, &len, parts[random.intRangeLessThan(usize, 0, parts.len)]);
    }
    return buffer[0..len];
}

fn generatedParserMutatedFuzzSql(random: std.Random, seed: []const u8, buffer: []u8) []const u8 {
    const replacement = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_(),.*=<>+-' ";
    var len: usize = 0;
    for (seed) |byte| {
        const action = random.intRangeLessThan(u8, 0, 18);
        switch (action) {
            0 => {},
            1 => appendFuzzSqlByte(buffer, &len, replacement[random.intRangeLessThan(usize, 0, replacement.len)]),
            2 => {
                appendFuzzSqlByte(buffer, &len, byte);
                appendFuzzSqlByte(buffer, &len, byte);
            },
            3 => {
                appendFuzzSqlByte(buffer, &len, byte);
                appendFuzzSqlByte(buffer, &len, replacement[random.intRangeLessThan(usize, 0, replacement.len)]);
            },
            4 => {
                appendFuzzSqlByte(buffer, &len, byte);
                appendFuzzSqlByte(buffer, &len, ' ');
            },
            else => appendFuzzSqlByte(buffer, &len, byte),
        }
    }
    return buffer[0..len];
}

fn exerciseGeneratedParserFuzzSql(alloc: std.mem.Allocator, sql: []const u8, stats: *FuzzStats) !void {
    stats.attempted += 1;
    var tokens = lexer.tokenizeAlloc(alloc, sql) catch |err| switch (err) {
        error.UnsupportedSqlShape => {
            stats.unsupported_token_shape += 1;
            return;
        },
        else => return err,
    };
    defer lexer.freeTokens(alloc, &tokens);

    var parsed = generated_parser.parseTokensAlloc(alloc, tokens.items) catch |err| switch (err) {
        error.UnsupportedSqlShape, error.UnexpectedToken => {
            const diagnostic = generated_parser.diagnosticAlloc(alloc, tokens.items) catch |diagnostic_err| switch (diagnostic_err) {
                error.UnsupportedSqlShape => {
                    stats.unsupported_token_shape += 1;
                    return;
                },
                else => return diagnostic_err,
            } orelse return error.ExpectedDiagnostic;
            defer alloc.free(diagnostic.expected);
            if (diagnostic.token_index > tokens.items.len) return error.InvalidDiagnosticTokenIndex;
            if (diagnostic.source_end < diagnostic.source_start or diagnostic.source_end > sql.len) return error.InvalidDiagnosticSpan;
            if (diagnostic.expected.len == 0) return error.EmptyDiagnosticExpectedSet;
            stats.diagnosed += 1;
            return;
        },
        else => return err,
    };
    defer parsed.deinit(alloc);
    stats.parsed += 1;
}
