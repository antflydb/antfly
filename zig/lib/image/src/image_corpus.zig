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
const build_options = @import("build_options");
const gif = @import("gif.zig");
const jpeg = @import("jpeg.zig");
const png = @import("png.zig");
const test_support = @import("test_support.zig");
const c = if (build_options.enable_spng) @cImport({
    @cInclude("spng.h");
}) else struct {};

pub fn main(init: std.process.Init) !void {
    const alloc = init.gpa;
    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "image_corpus";
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return error.InvalidArguments;
    };

    if (std.mem.eql(u8, subcommand, "list")) {
        try cmdList(alloc);
        return;
    }
    if (std.mem.eql(u8, subcommand, "describe")) {
        const fixture_path = args.next() orelse {
            printUsage(argv0);
            return error.InvalidArguments;
        };
        try cmdDescribe(alloc, fixture_path);
        return;
    }
    if (std.mem.eql(u8, subcommand, "verify-jpeg")) {
        try cmdVerifyJpeg(alloc);
        return;
    }
    if (std.mem.eql(u8, subcommand, "verify-png")) {
        try cmdVerifyPng(alloc);
        return;
    }
    if (std.mem.eql(u8, subcommand, "verify-png-spng")) {
        try cmdVerifyPngSpng(alloc);
        return;
    }
    if (std.mem.eql(u8, subcommand, "verify-gif")) {
        try cmdVerifyGif(alloc);
        return;
    }

    printUsage(argv0);
    return error.InvalidArguments;
}

fn cmdList(alloc: std.mem.Allocator) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const manifest = try test_support.loadManifest(alloc, io_impl.io());
    defer test_support.freeManifest(alloc, manifest);

    std.debug.print("image corpus manifest v{d}\n", .{manifest.version});
    std.debug.print("fixtures: {d}\n", .{manifest.fixtures.len});
    for (manifest.fixtures) |fixture| {
        if (fixture.frames) |frames| {
            std.debug.print("{s}\t{s}\t{s}\tframes={d}\n", .{
                fixture.format,
                fixture.result,
                fixture.path,
                frames,
            });
        } else {
            std.debug.print("{s}\t{s}\t{s}\tframes=n/a\n", .{
                fixture.format,
                fixture.result,
                fixture.path,
            });
        }
    }
}

fn cmdDescribe(alloc: std.mem.Allocator, fixture_path: []const u8) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const manifest = try test_support.loadManifest(alloc, io_impl.io());
    defer test_support.freeManifest(alloc, manifest);

    const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture_path);
    defer alloc.free(bytes);

    const fixture = test_support.findFixture(manifest, fixture_path);
    const format = if (fixture) |found| found.format else inferFormat(fixture_path) orelse return error.InvalidArguments;

    if (std.mem.eql(u8, format, "jpeg")) {
        const structure = jpeg.parseStructure(bytes) catch |err| {
            std.debug.print("jpeg parse error: {s}\n", .{@errorName(err)});
            return;
        };
        std.debug.print(
            "jpeg {s}: {d}x{d} kind={s} components={d} scans={d}\n",
            .{
                fixture_path,
                structure.info.width,
                structure.info.height,
                @tagName(structure.info.frame_kind),
                structure.info.component_count,
                structure.scan_count,
            },
        );
        const decoded = jpeg.decodeRgba(alloc, bytes) catch |err| {
            std.debug.print("jpeg decode error: {s}\n", .{@errorName(err)});
            return;
        };
        defer alloc.free(decoded.rgba);
        const hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(hash);
        std.debug.print("rgba sha256: {s}\n", .{hash});
        return;
    }

    if (std.mem.eql(u8, format, "png")) {
        const decoded = png.decodeRgba(alloc, bytes) catch |err| {
            std.debug.print("png decode error: {s}\n", .{@errorName(err)});
            return;
        };
        defer alloc.free(decoded.rgba);
        const hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
        defer alloc.free(hash);
        std.debug.print("png {s}: {d}x{d}\n", .{ fixture_path, decoded.width, decoded.height });
        std.debug.print("rgba sha256: {s}\n", .{hash});
        return;
    }

    if (std.mem.eql(u8, format, "gif")) {
        const frames = gif.decodeFramesAlloc(alloc, bytes) catch |err| {
            std.debug.print("gif decode error: {s}\n", .{@errorName(err)});
            return;
        };
        defer {
            for (frames) |frame| alloc.free(frame.rgba);
            alloc.free(frames);
        }

        if (frames.len == 0) return error.GifDecodeFailed;
        std.debug.print("gif {s}: {d}x{d} frames={d}\n", .{ fixture_path, frames[0].width, frames[0].height, frames.len });
        for (frames, 0..) |frame, i| {
            const hash = try test_support.sha256HexAlloc(alloc, frame.rgba);
            defer alloc.free(hash);
            std.debug.print("frame {d}: delay_ms={d} rgba_sha256={s}\n", .{ i, frame.delay_ms, hash });
        }
        return;
    }

    return error.InvalidArguments;
}

fn inferFormat(fixture_path: []const u8) ?[]const u8 {
    if (std.mem.endsWith(u8, fixture_path, ".jpg") or std.mem.endsWith(u8, fixture_path, ".jpeg")) return "jpeg";
    if (std.mem.endsWith(u8, fixture_path, ".png")) return "png";
    if (std.mem.endsWith(u8, fixture_path, ".gif")) return "gif";
    return null;
}

fn cmdVerifyJpeg(alloc: std.mem.Allocator) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const manifest = try test_support.loadManifest(alloc, io_impl.io());
    defer test_support.freeManifest(alloc, manifest);

    var checked: usize = 0;
    var skipped: usize = 0;

    for (manifest.fixtures) |fixture| {
        if (!std.mem.eql(u8, fixture.format, "jpeg")) {
            skipped += 1;
            continue;
        }

        const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture.path);
        defer alloc.free(bytes);

        if (std.mem.eql(u8, fixture.result, manifest.results.success)) {
            const structure = try jpeg.parseStructure(bytes);
            const info = structure.info;
            const decoded = try jpeg.decodeRgba(alloc, bytes);
            defer alloc.free(decoded.rgba);

            const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
            defer alloc.free(actual_hash);

            if (info.width != fixture.width.? or info.height != fixture.height.?) {
                std.debug.print(
                    "FAIL jpeg probe dimension mismatch: {s} expected {d}x{d} got {d}x{d}\n",
                    .{ fixture.path, fixture.width.?, fixture.height.?, info.width, info.height },
                );
                return error.ImageCorpusVerificationFailed;
            }
            if (decoded.width != fixture.width.? or decoded.height != fixture.height.?) {
                std.debug.print(
                    "FAIL jpeg success fixture dimension mismatch: {s} expected {d}x{d} got {d}x{d}\n",
                    .{ fixture.path, fixture.width.?, fixture.height.?, decoded.width, decoded.height },
                );
                return error.ImageCorpusVerificationFailed;
            }
            if (!std.mem.eql(u8, actual_hash, fixture.pixel_hashes[0])) {
                std.debug.print(
                    "FAIL jpeg success fixture hash mismatch: {s}\nexpected {s}\nactual   {s}\n",
                    .{ fixture.path, fixture.pixel_hashes[0], actual_hash },
                );
                return error.ImageCorpusVerificationFailed;
            }
            std.debug.print("OK   jpeg success fixture: {s} ({s})\n", .{ fixture.path, @tagName(info.frame_kind) });
            checked += 1;
            continue;
        }

        if (std.mem.eql(u8, fixture.result, manifest.results.invalid)) {
            const parse_result = jpeg.parseStructure(bytes);
            if (parse_result) |_| {
                const decoded = jpeg.decodeRgba(alloc, bytes) catch |decode_err| {
                    if (decode_err == error.JpegDecodeFailed or decode_err == error.UnsupportedJpegFormat) {
                        std.debug.print("OK   jpeg invalid fixture: {s} (decode)\n", .{fixture.path});
                        checked += 1;
                        continue;
                    }
                    std.debug.print(
                        "FAIL jpeg invalid fixture wrong decode error after successful parse: {s} decode={s}\n",
                        .{ fixture.path, @errorName(decode_err) },
                    );
                    return error.ImageCorpusVerificationFailed;
                };
                alloc.free(decoded.rgba);
                std.debug.print("FAIL jpeg invalid fixture decoded successfully: {s}\n", .{fixture.path});
                return error.ImageCorpusVerificationFailed;
            } else |parse_err| {
                const decoded = jpeg.decodeRgba(alloc, bytes) catch |decode_err| {
                    if ((decode_err == error.JpegDecodeFailed or decode_err == error.UnsupportedJpegFormat) and
                        (parse_err == error.JpegDecodeFailed or parse_err == error.UnsupportedJpegFormat))
                    {
                        std.debug.print("OK   jpeg invalid fixture: {s} (parse)\n", .{fixture.path});
                        checked += 1;
                        continue;
                    }
                    std.debug.print(
                        "FAIL jpeg invalid fixture wrong error: {s} parse={s} decode={s}\n",
                        .{ fixture.path, @errorName(parse_err), @errorName(decode_err) },
                    );
                    return error.ImageCorpusVerificationFailed;
                };
                alloc.free(decoded.rgba);
                std.debug.print("FAIL jpeg invalid fixture decoded successfully: {s}\n", .{fixture.path});
                return error.ImageCorpusVerificationFailed;
            }
        }

        if (std.mem.eql(u8, fixture.result, manifest.results.known_unsupported)) {
            const structure = try jpeg.parseStructure(bytes);
            const info = structure.info;
            if (fixture.width) |expected_width| {
                if (info.width != expected_width) {
                    std.debug.print(
                        "FAIL jpeg known_unsupported width mismatch: {s} expected {d} got {d}\n",
                        .{ fixture.path, expected_width, info.width },
                    );
                    return error.ImageCorpusVerificationFailed;
                }
            }
            if (fixture.height) |expected_height| {
                if (info.height != expected_height) {
                    std.debug.print(
                        "FAIL jpeg known_unsupported height mismatch: {s} expected {d} got {d}\n",
                        .{ fixture.path, expected_height, info.height },
                    );
                    return error.ImageCorpusVerificationFailed;
                }
            }
            if (fixtureHasTag(fixture, "progressive") and !isProgressiveFrameKind(info.frame_kind)) {
                std.debug.print(
                    "FAIL jpeg known_unsupported expected progressive frame kind: {s} got {s}\n",
                    .{ fixture.path, @tagName(info.frame_kind) },
                );
                return error.ImageCorpusVerificationFailed;
            }
            if (fixtureHasTag(fixture, "progressive") and !jpeg.supportsPlannedProgressiveDecode(structure)) {
                std.debug.print(
                    "FAIL jpeg known_unsupported progressive fixture is outside the planned progressive decode subset: {s}\n",
                    .{fixture.path},
                );
                return error.ImageCorpusVerificationFailed;
            }
            if (fixtureHasTag(fixture, "arithmetic") and !isArithmeticFrameKind(info.frame_kind)) {
                std.debug.print(
                    "FAIL jpeg known_unsupported expected arithmetic frame kind: {s} got {s}\n",
                    .{ fixture.path, @tagName(info.frame_kind) },
                );
                return error.ImageCorpusVerificationFailed;
            }
            const decoded = jpeg.decodeRgba(alloc, bytes) catch |decode_err| {
                if (decode_err == error.JpegDecodeFailed) {
                    std.debug.print("OK   jpeg known_unsupported fixture: {s} ({s})\n", .{ fixture.path, @tagName(info.frame_kind) });
                    checked += 1;
                    continue;
                }
                std.debug.print(
                    "FAIL jpeg known_unsupported wrong decode error: {s} decode={s}\n",
                    .{ fixture.path, @errorName(decode_err) },
                );
                return error.ImageCorpusVerificationFailed;
            };
            alloc.free(decoded.rgba);
            std.debug.print("FAIL jpeg known_unsupported decoded successfully: {s}\n", .{fixture.path});
            return error.ImageCorpusVerificationFailed;
        }

        std.debug.print("SKIP jpeg fixture with unsupported manifest result: {s} ({s})\n", .{ fixture.path, fixture.result });
        skipped += 1;
    }

    std.debug.print("verified jpeg fixtures: checked={d} skipped={d}\n", .{ checked, skipped });
}

fn fixtureHasTag(fixture: test_support.Manifest.Fixture, tag: []const u8) bool {
    for (fixture.tags) |fixture_tag| {
        if (std.mem.eql(u8, fixture_tag, tag)) return true;
    }
    return false;
}

fn isProgressiveFrameKind(kind: jpeg.FrameKind) bool {
    return switch (kind) {
        .progressive_dct, .differential_progressive_dct, .arithmetic_progressive_dct => true,
        else => false,
    };
}

fn isArithmeticFrameKind(kind: jpeg.FrameKind) bool {
    return switch (kind) {
        .arithmetic_sequential_dct, .arithmetic_progressive_dct, .arithmetic_lossless => true,
        else => false,
    };
}

fn cmdVerifyPng(alloc: std.mem.Allocator) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const manifest = try test_support.loadManifest(alloc, io_impl.io());
    defer test_support.freeManifest(alloc, manifest);

    var checked: usize = 0;
    var skipped: usize = 0;

    for (manifest.fixtures) |fixture| {
        if (!std.mem.eql(u8, fixture.format, "png")) {
            skipped += 1;
            continue;
        }
        const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture.path);
        defer alloc.free(bytes);

        if (std.mem.eql(u8, fixture.result, manifest.results.success)) {
            const decoded = try png.decodeRgba(alloc, bytes);
            defer alloc.free(decoded.rgba);

            const actual_hash = try test_support.sha256HexAlloc(alloc, decoded.rgba);
            defer alloc.free(actual_hash);

            if (decoded.width != fixture.width.? or decoded.height != fixture.height.?) {
                std.debug.print(
                    "FAIL png success fixture dimension mismatch: {s} expected {d}x{d} got {d}x{d}\n",
                    .{ fixture.path, fixture.width.?, fixture.height.?, decoded.width, decoded.height },
                );
                return error.ImageCorpusVerificationFailed;
            }
            if (!std.mem.eql(u8, actual_hash, fixture.pixel_hashes[0])) {
                std.debug.print(
                    "FAIL png success fixture hash mismatch: {s}\nexpected {s}\nactual   {s}\n",
                    .{ fixture.path, fixture.pixel_hashes[0], actual_hash },
                );
                return error.ImageCorpusVerificationFailed;
            }

            std.debug.print("OK   png success fixture: {s}\n", .{fixture.path});
            checked += 1;
            continue;
        }

        if (std.mem.eql(u8, fixture.result, manifest.results.known_unsupported)) {
            _ = png.decodeRgba(alloc, bytes) catch |decode_err| {
                if (decode_err == error.UnsupportedPngFormat) {
                    std.debug.print("OK   png known_unsupported fixture: {s}\n", .{fixture.path});
                    checked += 1;
                    continue;
                }
                std.debug.print(
                    "FAIL png known_unsupported wrong decode error: {s} decode={s}\n",
                    .{ fixture.path, @errorName(decode_err) },
                );
                return error.ImageCorpusVerificationFailed;
            };
            std.debug.print("FAIL png known_unsupported decoded successfully: {s}\n", .{fixture.path});
            return error.ImageCorpusVerificationFailed;
        }

        if (std.mem.eql(u8, fixture.result, manifest.results.invalid)) {
            _ = png.decodeRgba(alloc, bytes) catch |decode_err| {
                if (decode_err == error.PngDecodeFailed) {
                    std.debug.print("OK   png invalid fixture: {s}\n", .{fixture.path});
                    checked += 1;
                    continue;
                }
                std.debug.print(
                    "FAIL png invalid wrong decode error: {s} decode={s}\n",
                    .{ fixture.path, @errorName(decode_err) },
                );
                return error.ImageCorpusVerificationFailed;
            };
            std.debug.print("FAIL png invalid decoded successfully: {s}\n", .{fixture.path});
            return error.ImageCorpusVerificationFailed;
        }

        std.debug.print("SKIP png fixture with unsupported manifest result: {s} ({s})\n", .{ fixture.path, fixture.result });
        skipped += 1;
    }

    std.debug.print("verified png fixtures: checked={d} skipped={d}\n", .{ checked, skipped });
}

const SpngDecodedImage = struct {
    rgba: []u8,
    width: u32,
    height: u32,
};

fn decodeSpngRgba(alloc: std.mem.Allocator, bytes: []const u8) !SpngDecodedImage {
    const ctx = c.spng_ctx_new(0) orelse return error.PngDecodeFailed;
    defer c.spng_ctx_free(ctx);

    if (c.spng_set_png_buffer(ctx, bytes.ptr, bytes.len) != c.SPNG_OK) return error.PngDecodeFailed;

    var ihdr: c.struct_spng_ihdr = undefined;
    if (c.spng_get_ihdr(ctx, &ihdr) != c.SPNG_OK) return error.PngDecodeFailed;

    const rgba = if (ihdr.bit_depth == 16) blk: {
        var decoded_len: usize = 0;
        if (c.spng_decoded_image_size(ctx, c.SPNG_FMT_RGBA16, &decoded_len) != c.SPNG_OK) return error.PngDecodeFailed;

        const raw_rgba16 = try alloc.alloc(u8, decoded_len);
        defer alloc.free(raw_rgba16);

        if (c.spng_decode_image(ctx, raw_rgba16.ptr, raw_rgba16.len, c.SPNG_FMT_RGBA16, c.SPNG_DECODE_TRNS) != c.SPNG_OK) return error.PngDecodeFailed;

        const samples = std.mem.bytesAsSlice(u16, raw_rgba16);
        const rgba8 = try alloc.alloc(u8, samples.len);
        errdefer alloc.free(rgba8);
        for (samples, 0..) |sample, i| rgba8[i] = sample16To8(sample);
        break :blk rgba8;
    } else blk: {
        var decoded_len: usize = 0;
        if (c.spng_decoded_image_size(ctx, c.SPNG_FMT_RGBA8, &decoded_len) != c.SPNG_OK) return error.PngDecodeFailed;

        const rgba8 = try alloc.alloc(u8, decoded_len);
        errdefer alloc.free(rgba8);

        if (c.spng_decode_image(ctx, rgba8.ptr, rgba8.len, c.SPNG_FMT_RGBA8, c.SPNG_DECODE_TRNS) != c.SPNG_OK) return error.PngDecodeFailed;
        break :blk rgba8;
    };

    return .{
        .rgba = rgba,
        .width = @intCast(ihdr.width),
        .height = @intCast(ihdr.height),
    };
}

fn sample16To8(sample: u16) u8 {
    return @intCast(@min((@as(u32, sample) + 128) >> 8, 255));
}

fn cmdVerifyPngSpng(alloc: std.mem.Allocator) !void {
    if (!build_options.enable_spng) {
        std.debug.print("SKIP png spng conformance: libspng unavailable\n", .{});
        return;
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const manifest = try test_support.loadManifest(alloc, io_impl.io());
    defer test_support.freeManifest(alloc, manifest);

    var checked: usize = 0;
    var skipped: usize = 0;

    for (manifest.fixtures) |fixture| {
        if (!std.mem.eql(u8, fixture.format, "png")) {
            skipped += 1;
            continue;
        }
        if (!std.mem.eql(u8, fixture.result, manifest.results.success)) {
            skipped += 1;
            continue;
        }

        const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture.path);
        defer alloc.free(bytes);

        const zig_decoded = try png.decodeRgba(alloc, bytes);
        defer alloc.free(zig_decoded.rgba);

        const spng_decoded = try decodeSpngRgba(alloc, bytes);
        defer alloc.free(spng_decoded.rgba);

        if (spng_decoded.width != fixture.width.? or spng_decoded.height != fixture.height.?) {
            std.debug.print(
                "FAIL png spng dimension mismatch: {s} expected {d}x{d} got {d}x{d}\n",
                .{ fixture.path, fixture.width.?, fixture.height.?, spng_decoded.width, spng_decoded.height },
            );
            return error.ImageCorpusVerificationFailed;
        }
        if (zig_decoded.width != spng_decoded.width or zig_decoded.height != spng_decoded.height) {
            std.debug.print(
                "FAIL png zig/spng shape mismatch: {s} zig={d}x{d} spng={d}x{d}\n",
                .{ fixture.path, zig_decoded.width, zig_decoded.height, spng_decoded.width, spng_decoded.height },
            );
            return error.ImageCorpusVerificationFailed;
        }
        if (!std.mem.eql(u8, zig_decoded.rgba, spng_decoded.rgba)) {
            const zig_hash = try test_support.sha256HexAlloc(alloc, zig_decoded.rgba);
            defer alloc.free(zig_hash);
            const spng_hash = try test_support.sha256HexAlloc(alloc, spng_decoded.rgba);
            defer alloc.free(spng_hash);
            std.debug.print(
                "FAIL png zig/spng hash mismatch: {s}\nzig  {s}\nspng {s}\n",
                .{ fixture.path, zig_hash, spng_hash },
            );
            return error.ImageCorpusVerificationFailed;
        }

        std.debug.print("OK   png spng success fixture: {s}\n", .{fixture.path});
        checked += 1;
    }

    std.debug.print("verified png spng fixtures: checked={d} skipped={d}\n", .{ checked, skipped });
}

fn cmdVerifyGif(alloc: std.mem.Allocator) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const manifest = try test_support.loadManifest(alloc, io_impl.io());
    defer test_support.freeManifest(alloc, manifest);

    var checked: usize = 0;
    var skipped: usize = 0;

    for (manifest.fixtures) |fixture| {
        if (!std.mem.eql(u8, fixture.format, "gif")) {
            skipped += 1;
            continue;
        }
        const bytes = try test_support.readFixtureAlloc(alloc, io_impl.io(), fixture.path);
        defer alloc.free(bytes);

        if (std.mem.eql(u8, fixture.result, manifest.results.success)) {
            const frames = try gif.decodeFramesAlloc(alloc, bytes);
            defer {
                for (frames) |frame| alloc.free(frame.rgba);
                alloc.free(frames);
            }

            if (frames.len != fixture.frames.?) {
                std.debug.print(
                    "FAIL gif frame count mismatch: {s} expected {d} got {d}\n",
                    .{ fixture.path, fixture.frames.?, frames.len },
                );
                return error.ImageCorpusVerificationFailed;
            }

            for (frames, 0..) |frame, i| {
                const actual_hash = try test_support.sha256HexAlloc(alloc, frame.rgba);
                defer alloc.free(actual_hash);

                if (frame.width != fixture.width.? or frame.height != fixture.height.?) {
                    std.debug.print(
                        "FAIL gif frame dimension mismatch: {s} frame {d} expected {d}x{d} got {d}x{d}\n",
                        .{ fixture.path, i, fixture.width.?, fixture.height.?, frame.width, frame.height },
                    );
                    return error.ImageCorpusVerificationFailed;
                }
                if (frame.delay_ms != fixture.frame_delays_ms[i]) {
                    std.debug.print(
                        "FAIL gif frame delay mismatch: {s} frame {d} expected {d} got {d}\n",
                        .{ fixture.path, i, fixture.frame_delays_ms[i], frame.delay_ms },
                    );
                    return error.ImageCorpusVerificationFailed;
                }
                if (!std.mem.eql(u8, actual_hash, fixture.pixel_hashes[i])) {
                    std.debug.print(
                        "FAIL gif frame hash mismatch: {s} frame {d}\nexpected {s}\nactual   {s}\n",
                        .{ fixture.path, i, fixture.pixel_hashes[i], actual_hash },
                    );
                    return error.ImageCorpusVerificationFailed;
                }
            }

            std.debug.print("OK   gif success fixture: {s}\n", .{fixture.path});
            checked += 1;
            continue;
        }

        if (std.mem.eql(u8, fixture.result, manifest.results.invalid)) {
            _ = gif.decodeFramesAlloc(alloc, bytes) catch |decode_err| {
                if (decode_err == error.GifDecodeFailed) {
                    std.debug.print("OK   gif invalid fixture: {s}\n", .{fixture.path});
                    checked += 1;
                    continue;
                }
                std.debug.print(
                    "FAIL gif invalid wrong decode error: {s} decode={s}\n",
                    .{ fixture.path, @errorName(decode_err) },
                );
                return error.ImageCorpusVerificationFailed;
            };
            std.debug.print("FAIL gif invalid decoded successfully: {s}\n", .{fixture.path});
            return error.ImageCorpusVerificationFailed;
        }

        std.debug.print("SKIP gif fixture with unsupported manifest result: {s} ({s})\n", .{ fixture.path, fixture.result });
        skipped += 1;
    }

    std.debug.print("verified gif fixtures: checked={d} skipped={d}\n", .{ checked, skipped });
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage:
        \\  {s} list
        \\  {s} describe <fixture-path>
        \\  {s} verify-jpeg
        \\  {s} verify-png
        \\  {s} verify-png-spng
        \\  {s} verify-gif
        \\
        \\commands:
        \\  list         print the current image corpus entries
        \\  describe     print decoded metadata and hashes for one fixture
        \\  verify-jpeg  validate JPEG success and invalid fixtures against the current decoder
        \\  verify-png   validate PNG success, unsupported, and invalid fixtures against the current decoder
        \\  verify-png-spng validate PNG success fixtures against the current decoder and libspng
        \\  verify-gif   validate GIF success and invalid fixtures against the current decoder
        \\
    , .{ argv0, argv0, argv0, argv0, argv0, argv0 });
}
