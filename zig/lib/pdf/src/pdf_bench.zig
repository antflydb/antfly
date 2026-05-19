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
const pdf = @import("antfly_pdf");

const BenchError = error{
    InvalidArguments,
};

const max_pdf_input_bytes = 512 * 1024 * 1024;

const PdfBenchResult = struct {
    elapsed_ns: u64,
    total_output_bytes: usize,
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "pdf_bench";
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return BenchError.InvalidArguments;
    };

    if (std.mem.eql(u8, subcommand, "suite")) {
        const path = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const iterations = try parseIterations(args.next(), 25);
        try benchSuite(alloc, path, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "extract-text")) {
        const path = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const iterations = try parseIterations(args.next(), 100);
        try benchExtractText(alloc, path, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "render-first-page")) {
        const path = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const iterations = try parseIterations(args.next(), 10);
        try benchRenderFirstPage(alloc, path, iterations);
        return;
    }

    printUsage(argv0);
    return BenchError.InvalidArguments;
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage:
        \\  {s} suite <pdf-path> [iterations]
        \\  {s} extract-text <pdf-path> [iterations]
        \\  {s} render-first-page <pdf-path> [iterations]
        \\
    , .{ argv0, argv0, argv0 });
}

fn parseIterations(maybe_value: ?[]const u8, default_value: usize) !usize {
    return if (maybe_value) |value|
        try std.fmt.parseInt(usize, value, 10)
    else
        default_value;
}

fn benchSuite(alloc: std.mem.Allocator, path: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_pdf_input_bytes));
    defer alloc.free(bytes);

    const backend = pdf.Backend.native();
    const extract = try timeExtractText(alloc, backend, bytes, iterations);
    printBenchLine("pdf-extract-text", path, iterations, extract.elapsed_ns, bytes.len, extract.total_output_bytes);
    const render = try timeRenderFirstPage(alloc, backend, bytes, iterations);
    printBenchLine("pdf-render-first-page", path, iterations, render.elapsed_ns, bytes.len, render.total_output_bytes);
}

fn benchExtractText(alloc: std.mem.Allocator, path: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_pdf_input_bytes));
    defer alloc.free(bytes);

    const backend = pdf.Backend.native();
    const result = try timeExtractText(alloc, backend, bytes, iterations);
    printBenchLine("pdf-extract-text", path, iterations, result.elapsed_ns, bytes.len, result.total_output_bytes);
}

fn benchRenderFirstPage(alloc: std.mem.Allocator, path: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), path, alloc, .limited(max_pdf_input_bytes));
    defer alloc.free(bytes);

    const backend = pdf.Backend.native();
    const result = try timeRenderFirstPage(alloc, backend, bytes, iterations);
    printBenchLine("pdf-render-first-page", path, iterations, result.elapsed_ns, bytes.len, result.total_output_bytes);
}

fn timeExtractText(
    alloc: std.mem.Allocator,
    backend: pdf.Backend,
    bytes: []const u8,
    iterations: usize,
) !PdfBenchResult {
    const warmup = try backend.extractText(alloc, bytes);
    alloc.free(warmup);

    const start_ns = monotonicNowNs();
    var total_output_bytes: usize = 0;
    for (0..iterations) |_| {
        const text = try backend.extractText(alloc, bytes);
        total_output_bytes += text.len;
        alloc.free(text);
    }
    return .{
        .elapsed_ns = monotonicNowNs() - start_ns,
        .total_output_bytes = total_output_bytes,
    };
}

fn timeRenderFirstPage(
    alloc: std.mem.Allocator,
    backend: pdf.Backend,
    bytes: []const u8,
    iterations: usize,
) !PdfBenchResult {
    const warmup = try backend.renderFirstPagePng(alloc, bytes);
    alloc.free(warmup);

    const start_ns = monotonicNowNs();
    var total_output_bytes: usize = 0;
    for (0..iterations) |_| {
        const png = try backend.renderFirstPagePng(alloc, bytes);
        total_output_bytes += png.len;
        alloc.free(png);
    }
    return .{
        .elapsed_ns = monotonicNowNs() - start_ns,
        .total_output_bytes = total_output_bytes,
    };
}

fn printBenchLine(
    label: []const u8,
    path: []const u8,
    iterations: usize,
    elapsed_ns: u64,
    bytes_per_iter: usize,
    total_output_bytes: usize,
) void {
    std.debug.print(
        "{s} fixture={s} iterations={d} total_ns={d} ns_per_iter={d} input_bytes_per_sec={d} output_bytes_per_sec={d}\n",
        .{
            label,
            path,
            iterations,
            elapsed_ns,
            nsPerIter(elapsed_ns, iterations),
            ratePerSecond(bytes_per_iter * iterations, elapsed_ns),
            ratePerSecond(total_output_bytes, elapsed_ns),
        },
    );
}

fn nsPerIter(elapsed_ns: u64, iterations: usize) u64 {
    return if (iterations == 0) 0 else elapsed_ns / iterations;
}

fn ratePerSecond(units: usize, elapsed_ns: u64) u64 {
    if (elapsed_ns == 0) return 0;
    return @intCast((@as(u128, units) * std.time.ns_per_s) / elapsed_ns);
}

fn monotonicNowNs() u64 {
    const clock_id: std.posix.clockid_t = switch (builtin.os.tag) {
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => std.posix.CLOCK.UPTIME_RAW,
        else => std.posix.CLOCK.MONOTONIC,
    };
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(clock_id, &ts))) {
        .SUCCESS => return @intCast(@as(u128, @intCast(ts.sec)) * std.time.ns_per_s + @as(u128, @intCast(ts.nsec))),
        else => return 0,
    }
}
