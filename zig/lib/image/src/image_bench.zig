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
const build_options = @import("build_options");
const jpeg = @import("jpeg.zig");
const jpeg2000_decode = @import("jpeg2000/decode.zig");
const png = @import("png.zig");
const processing = @import("processing.zig");
const test_support = @import("test_support.zig");
// Zig 0.17 removed @cImport; the build provides `spng_c` as a translate-c
// module of spng.h when libspng is available, else an empty struct.
const c = @import("spng_c");

const BenchError = error{
    InvalidArguments,
    MissingImageFixture,
    OpenJpegNotFound,
    OpenJpegDecodeFailed,
    SpngNotFound,
    SpngDecodeFailed,
};

const max_bench_input_bytes = 512 * 1024 * 1024;

const opj_decompress_candidates = [_][]const u8{
    "/opt/homebrew/bin/opj_decompress",
    "/usr/local/bin/opj_decompress",
    "/usr/bin/opj_decompress",
};

const Jpeg2000BenchResult = struct {
    elapsed_ns: u64,
    total_samples: usize,
};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "image_bench";
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return BenchError.InvalidArguments;
    };

    if (std.mem.eql(u8, subcommand, "jpeg-decode")) {
        const fixture = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const iterations = try parseIterations(args.next(), 1000);
        try benchJpegDecode(alloc, fixture, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "jpeg-decode-all")) {
        const iterations = try parseIterations(args.next(), 250);
        try benchAllJpegDecode(alloc, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "png-decode")) {
        const fixture = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const iterations = try parseIterations(args.next(), 1000);
        try benchPngDecode(alloc, fixture, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "png-decode-all")) {
        const iterations = try parseIterations(args.next(), 250);
        try benchAllPngDecode(alloc, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "png-spng-compare")) {
        const fixture = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const iterations = try parseIterations(args.next(), 1000);
        try benchPngSpngCompare(alloc, fixture, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "image-decode-suite")) {
        const iterations = try parseIterations(args.next(), 100);
        try benchImageDecodeSuite(alloc, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "jpeg2000-decode")) {
        const path = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const iterations = try parseIterations(args.next(), 25);
        try benchJpeg2000Decode(alloc, path, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "jpeg2000-openjpeg-compare")) {
        const path = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const iterations = try parseIterations(args.next(), 10);
        try benchJpeg2000OpenJpegCompare(alloc, path, iterations);
        return;
    }

    if (std.mem.eql(u8, subcommand, "preprocess")) {
        const fixture = args.next() orelse {
            printUsage(argv0);
            return BenchError.InvalidArguments;
        };
        const target_size = try parseU32(args.next(), 512);
        const iterations = try parseIterations(args.next(), 500);
        try benchPreprocess(alloc, fixture, target_size, iterations);
        return;
    }

    printUsage(argv0);
    return BenchError.InvalidArguments;
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage:
        \\  {s} jpeg-decode <fixture> [iterations]
        \\  {s} jpeg-decode-all [iterations]
        \\  {s} png-decode <fixture> [iterations]
        \\  {s} png-decode-all [iterations]
        \\  {s} png-spng-compare <fixture> [iterations]
        \\  {s} image-decode-suite [iterations]
        \\  {s} jpeg2000-decode <path> [iterations]
        \\  {s} jpeg2000-openjpeg-compare <path> [iterations]
        \\  {s} preprocess <fixture> [target_size] [iterations]
        \\
        \\fixture paths are relative to testdata/image/, for example:
        \\  jpeg/baseline/pattern-4x4-422.jpg
        \\JPEG2000 paths may be absolute paths or paths relative to cwd.
        \\
    , .{ argv0, argv0, argv0, argv0, argv0, argv0, argv0, argv0, argv0 });
}

fn parseIterations(maybe_value: ?[]const u8, default_value: usize) !usize {
    return if (maybe_value) |value|
        try std.fmt.parseInt(usize, value, 10)
    else
        default_value;
}

fn parseU32(maybe_value: ?[]const u8, default_value: u32) !u32 {
    return if (maybe_value) |value|
        try std.fmt.parseInt(u32, value, 10)
    else
        default_value;
}

fn benchJpegDecode(alloc: std.mem.Allocator, fixture_path: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture_path);
    defer alloc.free(bytes);

    const warmup = try jpeg.decodeRgba(alloc, bytes);
    alloc.free(warmup.rgba);

    const start_ns = monotonicNowNs();
    var total_pixels: usize = 0;
    for (0..iterations) |_| {
        const decoded = try jpeg.decodeRgba(alloc, bytes);
        total_pixels += @as(usize, decoded.width) * @as(usize, decoded.height);
        alloc.free(decoded.rgba);
    }
    const elapsed_ns = monotonicNowNs() - start_ns;

    try printBenchLine("jpeg-decode", fixture_path, iterations, elapsed_ns, bytes.len, total_pixels);
}

fn benchAllJpegDecode(alloc: std.mem.Allocator, iterations: usize) !void {
    try benchAllManifestDecodeFormat(alloc, "jpeg", iterations);
}

fn benchPngDecode(alloc: std.mem.Allocator, fixture_path: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture_path);
    defer alloc.free(bytes);

    const result = try timePngDecode(alloc, bytes, iterations);
    try printBenchLine("png-decode", fixture_path, iterations, result.elapsed_ns, bytes.len, result.total_pixels);
}

fn benchAllPngDecode(alloc: std.mem.Allocator, iterations: usize) !void {
    try benchAllManifestDecodeFormat(alloc, "png", iterations);
}

fn benchPngSpngCompare(alloc: std.mem.Allocator, fixture_path: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture_path);
    defer alloc.free(bytes);

    const zig_result = try timePngDecode(alloc, bytes, iterations);
    try printBenchLine("png-zig", fixture_path, iterations, zig_result.elapsed_ns, bytes.len, zig_result.total_pixels);

    const spng_result = try timeSpngDecode(alloc, bytes, iterations);
    try printBenchLine("png-spng", fixture_path, iterations, spng_result.elapsed_ns, bytes.len, spng_result.total_pixels);

    const zig_ns = nsPerIter(zig_result.elapsed_ns, iterations);
    const spng_ns = nsPerIter(spng_result.elapsed_ns, iterations);
    const zig_vs_spng: f64 = if (spng_ns == 0)
        0.0
    else
        @as(f64, @floatFromInt(zig_ns)) / @as(f64, @floatFromInt(spng_ns));
    const spng_vs_zig: f64 = if (zig_ns == 0)
        0.0
    else
        @as(f64, @floatFromInt(spng_ns)) / @as(f64, @floatFromInt(zig_ns));
    std.debug.print(
        "png-spng-compare fixture={s} iterations={d} zig_ns_per_iter={d} spng_ns_per_iter={d} zig_vs_spng={d:.3} spng_vs_zig={d:.3}\n",
        .{ fixture_path, iterations, zig_ns, spng_ns, zig_vs_spng, spng_vs_zig },
    );
}

const ImageDecodeResult = struct {
    elapsed_ns: u64,
    total_pixels: usize,
};

fn timeJpegDecode(alloc: std.mem.Allocator, bytes: []const u8, iterations: usize) !ImageDecodeResult {
    const warmup = try jpeg.decodeRgba(alloc, bytes);
    alloc.free(warmup.rgba);

    const start_ns = monotonicNowNs();
    var total_pixels: usize = 0;
    for (0..iterations) |_| {
        const decoded = try jpeg.decodeRgba(alloc, bytes);
        total_pixels += @as(usize, decoded.width) * @as(usize, decoded.height);
        alloc.free(decoded.rgba);
    }
    return .{
        .elapsed_ns = monotonicNowNs() - start_ns,
        .total_pixels = total_pixels,
    };
}

fn timePngDecode(alloc: std.mem.Allocator, bytes: []const u8, iterations: usize) !ImageDecodeResult {
    const warmup = try png.decodeRgba(alloc, bytes);
    alloc.free(warmup.rgba);

    const start_ns = monotonicNowNs();
    var total_pixels: usize = 0;
    for (0..iterations) |_| {
        const decoded = try png.decodeRgba(alloc, bytes);
        total_pixels += @as(usize, decoded.width) * @as(usize, decoded.height);
        alloc.free(decoded.rgba);
    }
    return .{
        .elapsed_ns = monotonicNowNs() - start_ns,
        .total_pixels = total_pixels,
    };
}

fn timeSpngDecode(alloc: std.mem.Allocator, bytes: []const u8, iterations: usize) !ImageDecodeResult {
    _ = try decodeSpngOnce(alloc, bytes);

    const start_ns = monotonicNowNs();
    var total_pixels: usize = 0;
    for (0..iterations) |_| {
        total_pixels += try decodeSpngOnce(alloc, bytes);
    }
    return .{
        .elapsed_ns = monotonicNowNs() - start_ns,
        .total_pixels = total_pixels,
    };
}

fn decodeSpngOnce(alloc: std.mem.Allocator, bytes: []const u8) !usize {
    if (!build_options.enable_spng) return BenchError.SpngNotFound;

    const ctx = c.spng_ctx_new(0) orelse return BenchError.SpngNotFound;
    defer c.spng_ctx_free(ctx);

    if (c.spng_set_png_buffer(ctx, bytes.ptr, bytes.len) != c.SPNG_OK) return BenchError.SpngDecodeFailed;

    var ihdr: c.struct_spng_ihdr = undefined;
    if (c.spng_get_ihdr(ctx, &ihdr) != c.SPNG_OK) return BenchError.SpngDecodeFailed;

    var decoded_len: usize = 0;
    if (c.spng_decoded_image_size(ctx, c.SPNG_FMT_RGBA8, &decoded_len) != c.SPNG_OK) return BenchError.SpngDecodeFailed;

    const decoded = try alloc.alloc(u8, decoded_len);
    defer alloc.free(decoded);

    if (c.spng_decode_image(ctx, decoded.ptr, decoded.len, c.SPNG_FMT_RGBA8, 0) != c.SPNG_OK) return BenchError.SpngDecodeFailed;

    return @as(usize, ihdr.width) * @as(usize, ihdr.height);
}

fn benchAllManifestDecodeFormat(alloc: std.mem.Allocator, format: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const manifest = try test_support.loadManifest(alloc, io_impl.io());
    defer test_support.freeManifest(alloc, manifest);

    var total_elapsed_ns: u64 = 0;
    var total_bytes: usize = 0;
    var total_pixels: usize = 0;
    var checked: usize = 0;

    for (manifest.fixtures) |fixture| {
        if (!std.mem.eql(u8, fixture.format, format)) continue;
        if (!std.mem.eql(u8, fixture.result, manifest.results.success)) continue;

        const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture.path);
        defer alloc.free(bytes);

        const result = if (std.mem.eql(u8, format, "jpeg"))
            try timeJpegDecode(alloc, bytes, iterations)
        else if (std.mem.eql(u8, format, "png"))
            try timePngDecode(alloc, bytes, iterations)
        else
            return BenchError.InvalidArguments;

        total_elapsed_ns += result.elapsed_ns;
        total_bytes += bytes.len * iterations;
        total_pixels += result.total_pixels;
        checked += 1;

        const label = if (std.mem.eql(u8, format, "jpeg")) "jpeg-decode" else "png-decode";
        try printBenchLine(label, fixture.path, iterations, result.elapsed_ns, bytes.len, result.total_pixels);
    }

    std.debug.print(
        "{s}-decode-all fixtures={d} iterations={d} total_ns={d} avg_ns_per_fixture={d} bytes_per_sec={d} pixels_per_sec={d}\n",
        .{
            format,
            checked,
            iterations,
            total_elapsed_ns,
            if (checked == 0) @as(u64, 0) else total_elapsed_ns / checked,
            ratePerSecond(total_bytes, total_elapsed_ns),
            ratePerSecond(total_pixels, total_elapsed_ns),
        },
    );
}

fn benchImageDecodeSuite(alloc: std.mem.Allocator, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const manifest = try test_support.loadManifest(alloc, io_impl.io());
    defer test_support.freeManifest(alloc, manifest);

    var jpeg_elapsed_ns: u64 = 0;
    var jpeg_bytes: usize = 0;
    var jpeg_pixels: usize = 0;
    var jpeg_count: usize = 0;

    var png_elapsed_ns: u64 = 0;
    var png_bytes: usize = 0;
    var png_pixels: usize = 0;
    var png_count: usize = 0;

    for (manifest.fixtures) |fixture| {
        if (!std.mem.eql(u8, fixture.result, manifest.results.success)) continue;
        if (!std.mem.eql(u8, fixture.format, "jpeg") and !std.mem.eql(u8, fixture.format, "png")) continue;

        const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture.path);
        defer alloc.free(bytes);

        if (std.mem.eql(u8, fixture.format, "jpeg")) {
            const result = try timeJpegDecode(alloc, bytes, iterations);
            jpeg_elapsed_ns += result.elapsed_ns;
            jpeg_bytes += bytes.len * iterations;
            jpeg_pixels += result.total_pixels;
            jpeg_count += 1;
            try printBenchLine("jpeg-decode", fixture.path, iterations, result.elapsed_ns, bytes.len, result.total_pixels);
        } else {
            const result = try timePngDecode(alloc, bytes, iterations);
            png_elapsed_ns += result.elapsed_ns;
            png_bytes += bytes.len * iterations;
            png_pixels += result.total_pixels;
            png_count += 1;
            try printBenchLine("png-decode", fixture.path, iterations, result.elapsed_ns, bytes.len, result.total_pixels);
        }
    }

    const total_elapsed_ns = jpeg_elapsed_ns + png_elapsed_ns;
    std.debug.print(
        "image-decode-suite iterations={d} jpeg_fixtures={d} png_fixtures={d} total_ns={d} jpeg_pixels_per_sec={d} png_pixels_per_sec={d} total_pixels_per_sec={d} total_bytes_per_sec={d}\n",
        .{
            iterations,
            jpeg_count,
            png_count,
            total_elapsed_ns,
            ratePerSecond(jpeg_pixels, jpeg_elapsed_ns),
            ratePerSecond(png_pixels, png_elapsed_ns),
            ratePerSecond(jpeg_pixels + png_pixels, total_elapsed_ns),
            ratePerSecond(jpeg_bytes + png_bytes, total_elapsed_ns),
        },
    );
}

fn benchJpeg2000Decode(alloc: std.mem.Allocator, path: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try readBenchInputAlloc(alloc, io_impl.io(), path);
    defer alloc.free(bytes);

    const header = try jpeg2000_decode.decodeHeaderBytes(alloc, bytes);
    const result = try timeJpeg2000ZigDecode(alloc, bytes, header, iterations);
    printJpeg2000BenchLine("jpeg2000-zig", path, header, iterations, result.elapsed_ns, bytes.len, result.total_samples);
}

fn benchJpeg2000OpenJpegCompare(alloc: std.mem.Allocator, path: []const u8, iterations: usize) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try readBenchInputAlloc(alloc, io_impl.io(), path);
    defer alloc.free(bytes);

    const header = try jpeg2000_decode.decodeHeaderBytes(alloc, bytes);
    const zig_result = try timeJpeg2000ZigDecode(alloc, bytes, header, iterations);
    printJpeg2000BenchLine("jpeg2000-zig", path, header, iterations, zig_result.elapsed_ns, bytes.len, zig_result.total_samples);

    const opj_tool = resolveOpenJpegDecompress(io_impl.io()) orelse return BenchError.OpenJpegNotFound;
    const opj_result = try timeOpenJpegDecodeCli(alloc, io_impl.io(), opj_tool, bytes, header, iterations);
    printJpeg2000BenchLine("jpeg2000-openjpeg-cli", path, header, iterations, opj_result.elapsed_ns, bytes.len, opj_result.total_samples);

    const zig_ns = nsPerIter(zig_result.elapsed_ns, iterations);
    const opj_ns = nsPerIter(opj_result.elapsed_ns, iterations);
    const zig_vs_opj: f64 = if (opj_ns == 0)
        0.0
    else
        @as(f64, @floatFromInt(zig_ns)) / @as(f64, @floatFromInt(opj_ns));
    const opj_vs_zig: f64 = if (zig_ns == 0)
        0.0
    else
        @as(f64, @floatFromInt(opj_ns)) / @as(f64, @floatFromInt(zig_ns));
    std.debug.print(
        "jpeg2000-openjpeg-compare fixture={s} iterations={d} zig_ns_per_iter={d} openjpeg_cli_ns_per_iter={d} zig_vs_openjpeg={d:.3} openjpeg_vs_zig={d:.3}\n",
        .{ path, iterations, zig_ns, opj_ns, zig_vs_opj, opj_vs_zig },
    );
}

fn readBenchInputAlloc(alloc: std.mem.Allocator, io: std.Io, path: []const u8) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(io, path, alloc, .limited(max_bench_input_bytes)) catch |cwd_err| {
        if (std.mem.startsWith(u8, path, "/")) return cwd_err;
        return test_support.readFixtureAlloc(alloc, io, path) catch cwd_err;
    };
}

fn timeJpeg2000ZigDecode(
    alloc: std.mem.Allocator,
    bytes: []const u8,
    header: jpeg2000_decode.Header,
    iterations: usize,
) !Jpeg2000BenchResult {
    _ = try decodeJpeg2000Once(alloc, bytes, header);

    const start_ns = monotonicNowNs();
    var total_samples: usize = 0;
    for (0..iterations) |_| {
        total_samples += try decodeJpeg2000Once(alloc, bytes, header);
    }
    return .{
        .elapsed_ns = monotonicNowNs() - start_ns,
        .total_samples = total_samples,
    };
}

fn decodeJpeg2000Once(
    alloc: std.mem.Allocator,
    bytes: []const u8,
    header: jpeg2000_decode.Header,
) !usize {
    if (header.bits_per_component <= 8) {
        var decoded = try jpeg2000_decode.decodeComponentPlanesU8Bytes(alloc, bytes);
        defer decoded.deinit();
        return countPlaneSamplesU32(decoded.widths, decoded.heights);
    }

    var decoded = try jpeg2000_decode.decodeComponentPlanesU16Bytes(alloc, bytes);
    defer decoded.deinit();
    return countPlaneSamplesU32(decoded.widths, decoded.heights);
}

fn timeOpenJpegDecodeCli(
    alloc: std.mem.Allocator,
    io: std.Io,
    tool: []const u8,
    bytes: []const u8,
    header: jpeg2000_decode.Header,
    iterations: usize,
) !Jpeg2000BenchResult {
    const in_path = try tempPath(alloc, "antfly_j2k_bench_in", "j2k");
    defer alloc.free(in_path);
    defer deleteFile(io, in_path);

    const out_path = try tempPath(alloc, "antfly_j2k_bench_out", "pgx");
    defer alloc.free(out_path);
    defer deleteOpenJpegPgxOutputs(alloc, io, out_path, header.components);

    try std.Io.Dir.cwd().writeFile(io, .{
        .sub_path = in_path,
        .data = bytes,
    });

    try runOpenJpegDecode(io, tool, in_path, out_path);
    deleteOpenJpegPgxOutputs(alloc, io, out_path, header.components);

    const start_ns = monotonicNowNs();
    for (0..iterations) |_| {
        try runOpenJpegDecode(io, tool, in_path, out_path);
    }

    return .{
        .elapsed_ns = monotonicNowNs() - start_ns,
        .total_samples = countHeaderSamples(header) * iterations,
    };
}

fn resolveOpenJpegDecompress(io: std.Io) ?[]const u8 {
    for (opj_decompress_candidates) |candidate| {
        std.Io.Dir.accessAbsolute(io, candidate, .{}) catch continue;
        return candidate;
    }
    return null;
}

fn runOpenJpegDecode(io: std.Io, tool: []const u8, in_path: []const u8, out_path: []const u8) !void {
    const argv = &[_][]const u8{ tool, "-i", in_path, "-o", out_path };
    var child = try std.process.spawn(io, .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    });
    const term = try child.wait(io);
    switch (term) {
        .exited => |code| if (code == 0) return,
        else => {},
    }
    return BenchError.OpenJpegDecodeFailed;
}

fn tempPath(allocator: std.mem.Allocator, prefix: []const u8, ext: []const u8) ![]u8 {
    const salt: usize = @intFromPtr(&tempPath);
    const now = monotonicNowNs();
    return std.fmt.allocPrint(
        allocator,
        "/tmp/{s}_{x}_{x}.{s}",
        .{ prefix, salt, now, ext },
    );
}

fn deleteOpenJpegPgxOutputs(
    allocator: std.mem.Allocator,
    io: std.Io,
    out_path: []const u8,
    components: u16,
) void {
    deleteFile(io, out_path);

    const dot = std.mem.lastIndexOfScalar(u8, out_path, '.') orelse out_path.len;
    const stem = out_path[0..dot];
    const limit: usize = @max(@as(usize, components), 8);
    var i: usize = 0;
    while (i < limit) : (i += 1) {
        const numbered = std.fmt.allocPrint(allocator, "{s}_{d}.pgx", .{ stem, i }) catch return;
        defer allocator.free(numbered);
        deleteFile(io, numbered);
    }
}

fn deleteFile(io: std.Io, path: []const u8) void {
    std.Io.Dir.cwd().deleteFile(io, path) catch {};
}

fn countPlaneSamplesU32(widths: []const u32, heights: []const u32) usize {
    var total: usize = 0;
    for (widths, heights) |width, height| {
        total += @as(usize, width) * @as(usize, height);
    }
    return total;
}

fn countHeaderSamples(header: jpeg2000_decode.Header) usize {
    return @as(usize, header.width) * @as(usize, header.height) * @as(usize, header.components);
}

fn benchPreprocess(
    alloc: std.mem.Allocator,
    fixture_path: []const u8,
    target_size: u32,
    iterations: usize,
) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture_path);
    defer alloc.free(bytes);

    const decoded = try jpeg.decodeRgba(alloc, bytes);
    defer alloc.free(decoded.rgba);

    const img = processing.ImageU8{
        .data = decoded.rgba,
        .width = decoded.width,
        .height = decoded.height,
        .format = .rgba8,
    };

    const warmup = try processing.preprocessDecoded(
        alloc,
        img,
        target_size,
        .{ 0.485, 0.456, 0.406 },
        .{ 0.229, 0.224, 0.225 },
    );
    alloc.free(warmup);

    const start_ns = monotonicNowNs();
    for (0..iterations) |_| {
        const out = try processing.preprocessDecoded(
            alloc,
            img,
            target_size,
            .{ 0.485, 0.456, 0.406 },
            .{ 0.229, 0.224, 0.225 },
        );
        alloc.free(out);
    }
    const elapsed_ns = monotonicNowNs() - start_ns;
    const output_floats = @as(usize, 3) * @as(usize, target_size) * @as(usize, target_size) * iterations;

    std.debug.print(
        "preprocess fixture={s} target={d} iterations={d} total_ns={d} ns_per_iter={d} output_floats_per_sec={d}\n",
        .{
            fixture_path,
            target_size,
            iterations,
            elapsed_ns,
            if (iterations == 0) @as(u64, 0) else elapsed_ns / iterations,
            ratePerSecond(output_floats, elapsed_ns),
        },
    );
}

fn printBenchLine(
    label: []const u8,
    fixture_path: []const u8,
    iterations: usize,
    elapsed_ns: u64,
    bytes_per_iter: usize,
    total_pixels: usize,
) !void {
    std.debug.print(
        "{s} fixture={s} iterations={d} total_ns={d} ns_per_iter={d} bytes_per_sec={d} pixels_per_sec={d}\n",
        .{
            label,
            fixture_path,
            iterations,
            elapsed_ns,
            if (iterations == 0) @as(u64, 0) else elapsed_ns / iterations,
            ratePerSecond(bytes_per_iter * iterations, elapsed_ns),
            ratePerSecond(total_pixels, elapsed_ns),
        },
    );
}

fn printJpeg2000BenchLine(
    label: []const u8,
    path: []const u8,
    header: jpeg2000_decode.Header,
    iterations: usize,
    elapsed_ns: u64,
    bytes_per_iter: usize,
    total_samples: usize,
) void {
    std.debug.print(
        "{s} fixture={s} size={d}x{d} components={d} bpc={d} signed={} iterations={d} total_ns={d} ns_per_iter={d} bytes_per_sec={d} samples_per_sec={d}\n",
        .{
            label,
            path,
            header.width,
            header.height,
            header.components,
            header.bits_per_component,
            header.is_signed,
            iterations,
            elapsed_ns,
            nsPerIter(elapsed_ns, iterations),
            ratePerSecond(bytes_per_iter * iterations, elapsed_ns),
            ratePerSecond(total_samples, elapsed_ns),
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
