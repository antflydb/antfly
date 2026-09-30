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

//! Standalone compiler/admission microbenchmark, not a storage throughput test.
const std = @import("std");
const compiler = @import("compiler.zig");
const lexer = @import("sql_parser").lexer;
const describe = @import("describe.zig");
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");

const ShapeBackend = struct {
    fn resolve(_: *anyopaque, _: std.mem.Allocator, _: ast.Name, _: catalog.Action) !catalog.Table {
        return error.UnexpectedCatalogRead;
    }
    fn scan(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: catalog.Scan) !catalog.Page {
        return error.UnexpectedDataRead;
    }
    fn mutate(_: *anyopaque, _: std.mem.Allocator, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
        return error.UnexpectedMutation;
    }
    fn checkpoint(_: *anyopaque) !void {}
};

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;
    const queries = [_][]const u8{
        "SELECT _id,name,age FROM public.users WHERE age >= $1 AND enabled=true LIMIT 100",
        "INSERT INTO users (_id,name,age) VALUES ($1,$2,$3),($4,$5,$6)",
        "UPDATE users SET enabled=false WHERE age < $1",
    };
    const iterations = 10_000;
    var retained: usize = 0;
    var start = std.Io.Clock.now(.awake, io).nanoseconds;
    for (0..iterations) |i| {
        var prepared = try compiler.compile(allocator, queries[i % queries.len], .{});
        retained += prepared.arena.queryCapacity();
        std.mem.doNotOptimizeAway(prepared.statement);
        prepared.deinit();
    }
    const elapsed = std.Io.Clock.now(.awake, io).nanoseconds - start;
    std.debug.print("compiler cases={d} operations={d} ns_per_compile={d:.1} retained_bytes_per_plan={d}\n", .{
        queries.len, iterations, @as(f64, @floatFromInt(elapsed)) / iterations, retained / iterations,
    });

    var source = std.ArrayList(u8).empty;
    defer source.deinit(allocator);
    try source.appendSlice(allocator, "SELECT name FROM users /*");
    try source.appendNTimes(allocator, 'x', 100_000);
    try source.appendSlice(allocator, "*/ WHERE _id=$1");
    var prepared = try compiler.compile(allocator, source.items, .{});
    defer prepared.deinit();
    std.debug.print("compiler source_bytes={d} retained_bytes={d} (comments and tokens released)\n", .{ source.items.len, prepared.arena.queryCapacity() });

    source.clearRetainingCapacity();
    for (0..128_000) |_| try source.appendSlice(allocator, "x,");
    const admission_iterations = 20;
    start = std.Io.Clock.now(.awake, io).nanoseconds;
    for (0..admission_iterations) |_| {
        _ = lexer.tokenizeBoundedDiagnosticAlloc(allocator, source.items, 128) catch |err| {
            if (err != error.SqlTokenLimitExceeded) return err;
            continue;
        };
        return error.ExpectedTokenAdmissionFailure;
    }
    const bounded_elapsed = std.Io.Clock.now(.awake, io).nanoseconds - start;
    start = std.Io.Clock.now(.awake, io).nanoseconds;
    for (0..admission_iterations) |_| {
        var tokens = try lexer.tokenizeAlloc(allocator, source.items);
        std.mem.doNotOptimizeAway(tokens.items);
        lexer.freeTokens(allocator, &tokens);
    }
    const unbounded_elapsed = std.Io.Clock.now(.awake, io).nanoseconds - start;
    std.debug.print("token_admission source_bytes={d} token_limit=128 bounded_ns={d:.1} full_tokenization_ns={d:.1}\n", .{
        source.items.len,
        @as(f64, @floatFromInt(bounded_elapsed)) / admission_iterations,
        @as(f64, @floatFromInt(unbounded_elapsed)) / admission_iterations,
    });

    var shape_query = try compiler.compile(allocator, "WITH seed(x) AS (SELECT $1), q AS (SELECT x FROM seed) SELECT x FROM q UNION SELECT 1", .{});
    defer shape_query.deinit();
    var fixture: ShapeBackend = .{};
    const backend: catalog.Backend = .{ .ptr = &fixture, .vtable = &.{ .resolve = ShapeBackend.resolve, .scan = ShapeBackend.scan, .mutate = ShapeBackend.mutate, .checkpoint = ShapeBackend.checkpoint } };
    for ([_]bool{ false, true }) |typed| {
        const binding_iterations = 1000;
        retained = 0;
        start = std.Io.Clock.now(.awake, io).nanoseconds;
        for (0..binding_iterations) |_| {
            var binding = try describe.describe(allocator, backend, &shape_query, if (typed) &.{.integer} else &.{});
            retained += binding.arena.queryCapacity();
            if (binding.binding.parameter_types[0] != .integer) return error.InvalidShapeInference;
            binding.deinit();
        }
        std.debug.print("shape_binding explicit_types={} operations={d} ns_per_bind={d:.1} retained_bytes={d} data_reads=0\n", .{ typed, binding_iterations, @as(f64, @floatFromInt(std.Io.Clock.now(.awake, io).nanoseconds - start)) / binding_iterations, retained / binding_iterations });
    }
}
