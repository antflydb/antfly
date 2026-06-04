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
const jpeg = @import("jpeg.zig");

const Allocator = std.mem.Allocator;

const default_seed_corpora_dir = "/tmp/libjpeg-turbo-seed-corpora";
const default_seed_corpora_url = "https://github.com/libjpeg-turbo/seed-corpora";
const max_fixture_bytes = 16 * 1024 * 1024;

const Outcome = enum {
    success,
    parse_failed,
    decode_failed,
    crashed,
};

const ProbeExitCode = enum(u8) {
    success = 0,
    parse_failed = 10,
    decode_failed = 11,
};

const Config = struct {
    self_exe: []const u8,
    root_dir: []const u8 = default_seed_corpora_dir,
    refresh: bool = false,
    print_successes: bool = false,
    fail_on_decode: bool = false,
    quiet_failures: bool = false,
    allow_fetch: bool = true,
};

const Summary = struct {
    total_files: usize = 0,
    jpeg_files: usize = 0,
    success: usize = 0,
    parse_failed: usize = 0,
    decode_failed: usize = 0,
    crashed: usize = 0,
};

const DjpegTriageSummary = struct {
    decode_failed: usize = 0,
    djpeg_valid: usize = 0,
    djpeg_invalid: usize = 0,
    djpeg_error: usize = 0,
};

const DjpegParitySummary = struct {
    compared: usize = 0,
    matched: usize = 0,
    mismatched: usize = 0,
    skipped_decode_failed: usize = 0,
    skipped_parse_failed: usize = 0,
    skipped_crashed: usize = 0,
    skipped_djpeg_invalid: usize = 0,
    skipped_djpeg_error: usize = 0,
};

const StatusSummary = struct {
    root_dir: []const u8,
    corpus_present: bool,
    jpeg_files: usize = 0,
    djpeg_path: ?[]u8 = null,
    git_head: ?[]u8 = null,
};

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "jpeg_seed_corpora";
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return error.InvalidArguments;
    };

    if (std.mem.eql(u8, subcommand, "fetch")) {
        const root_dir = args.next() orelse default_seed_corpora_dir;
        try ensureSeedCorporaAvailable(alloc, root_dir, false, true);
        std.debug.print("seed-corpora ready: {s}\n", .{root_dir});
        return;
    }

    if (std.mem.eql(u8, subcommand, "status")) {
        const root_dir = args.next() orelse default_seed_corpora_dir;
        const summary = try collectStatusSummary(alloc, root_dir);
        defer if (summary.djpeg_path) |path| alloc.free(path);
        defer if (summary.git_head) |head| alloc.free(head);
        printStatusSummary(summary);
        return;
    }

    if (std.mem.eql(u8, subcommand, "run")) {
        var config = Config{ .self_exe = argv0 };
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--refresh")) {
                config.refresh = true;
            } else if (std.mem.eql(u8, arg, "--no-fetch")) {
                config.allow_fetch = false;
            } else if (std.mem.eql(u8, arg, "--print-successes")) {
                config.print_successes = true;
            } else if (std.mem.eql(u8, arg, "--fail-on-decode")) {
                config.fail_on_decode = true;
            } else if (std.mem.eql(u8, arg, "--quiet-failures")) {
                config.quiet_failures = true;
            } else if (std.mem.startsWith(u8, arg, "--")) {
                printUsage(argv0);
                return error.InvalidArguments;
            } else {
                config.root_dir = arg;
            }
        }

        try ensureSeedCorporaAvailable(alloc, config.root_dir, config.refresh, config.allow_fetch);
        const summary = try runSeedCorporaSweep(alloc, config);
        printSummary(summary);

        if (config.fail_on_decode and (summary.parse_failed != 0 or summary.decode_failed != 0 or summary.crashed != 0)) {
            return error.ImageSeedCorporaVerificationFailed;
        }
        return;
    }

    if (std.mem.eql(u8, subcommand, "probe-one")) {
        const root_dir = args.next() orelse {
            printUsage(argv0);
            return error.InvalidArguments;
        };
        const relative_path = args.next() orelse {
            printUsage(argv0);
            return error.InvalidArguments;
        };
        var print_success = false;
        var quiet_failure = false;
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--print-success")) {
                print_success = true;
            } else if (std.mem.eql(u8, arg, "--quiet-failure")) {
                quiet_failure = true;
            } else {
                printUsage(argv0);
                return error.InvalidArguments;
            }
        }

        const outcome = try probeOneFile(alloc, root_dir, relative_path, print_success, quiet_failure);
        std.process.exit(@intFromEnum(toExitCode(outcome)));
    }

    if (std.mem.eql(u8, subcommand, "triage-djpeg")) {
        var config = Config{ .self_exe = argv0, .quiet_failures = true };
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--refresh")) {
                config.refresh = true;
            } else if (std.mem.eql(u8, arg, "--no-fetch")) {
                config.allow_fetch = false;
            } else if (std.mem.startsWith(u8, arg, "--")) {
                printUsage(argv0);
                return error.InvalidArguments;
            } else {
                config.root_dir = arg;
            }
        }

        try ensureSeedCorporaAvailable(alloc, config.root_dir, config.refresh, config.allow_fetch);
        const summary = try triageDecodeFailuresWithDjpeg(alloc, config);
        printDjpegTriageSummary(summary);
        return;
    }

    if (std.mem.eql(u8, subcommand, "compare-one")) {
        const root_dir = args.next() orelse default_seed_corpora_dir;
        const relative_path = args.next() orelse {
            printUsage(argv0);
            return error.InvalidArguments;
        };
        try ensureSeedCorporaAvailable(alloc, root_dir, false, true);
        try compareOneWithDjpeg(alloc, root_dir, relative_path);
        return;
    }

    if (std.mem.eql(u8, subcommand, "triage-djpeg-parity")) {
        var config = Config{ .self_exe = argv0, .quiet_failures = true };
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--refresh")) {
                config.refresh = true;
            } else if (std.mem.eql(u8, arg, "--no-fetch")) {
                config.allow_fetch = false;
            } else if (std.mem.startsWith(u8, arg, "--")) {
                printUsage(argv0);
                return error.InvalidArguments;
            } else {
                config.root_dir = arg;
            }
        }

        try ensureSeedCorporaAvailable(alloc, config.root_dir, config.refresh, config.allow_fetch);
        const summary = try triageSuccessfulDecodesWithDjpegParity(alloc, config);
        printDjpegParitySummary(summary);
        return;
    }

    printUsage(argv0);
    return error.InvalidArguments;
}

fn runSeedCorporaSweep(alloc: Allocator, config: Config) !Summary {
    var summary = Summary{};
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), config.root_dir, .{ .iterate = true });
    defer dir.close(io_impl.io());

    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        summary.total_files += 1;
        if (!isJpegPath(entry.path)) continue;
        summary.jpeg_files += 1;
        switch (try runProbeOneChild(alloc, config, entry.path)) {
            .success => summary.success += 1,
            .parse_failed => summary.parse_failed += 1,
            .decode_failed => summary.decode_failed += 1,
            .crashed => summary.crashed += 1,
        }
    }

    return summary;
}

fn collectStatusSummary(alloc: Allocator, root_dir: []const u8) !StatusSummary {
    var summary = StatusSummary{
        .root_dir = root_dir,
        .djpeg_path = try resolveDjpegPath(alloc),
        .corpus_present = false,
    };

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return summary,
        else => return err,
    };
    defer dir.close(io_impl.io());
    summary.corpus_present = true;
    summary.git_head = try readGitHeadAlloc(alloc, io_impl.io(), root_dir);

    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!isJpegPath(entry.path)) continue;
        summary.jpeg_files += 1;
    }

    return summary;
}

fn triageDecodeFailuresWithDjpeg(alloc: Allocator, config: Config) !DjpegTriageSummary {
    var summary = DjpegTriageSummary{};
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), config.root_dir, .{ .iterate = true });
    defer dir.close(io_impl.io());

    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!isJpegPath(entry.path)) continue;

        const probe_outcome = try runProbeOneChild(alloc, config, entry.path);
        if (probe_outcome != .decode_failed) continue;

        summary.decode_failed += 1;
        const djpeg_outcome = try runDjpegChild(alloc, config.root_dir, entry.path);
        if (djpeg_outcome == .valid) {
            summary.djpeg_valid += 1;
            std.debug.print("DJPEG_VALID\t{s}\n", .{entry.path});
        } else if (djpeg_outcome == .invalid) {
            summary.djpeg_invalid += 1;
        } else {
            summary.djpeg_error += 1;
            std.debug.print("DJPEG_ERROR\t{s}\n", .{entry.path});
        }
    }

    return summary;
}

fn triageSuccessfulDecodesWithDjpegParity(alloc: Allocator, config: Config) !DjpegParitySummary {
    var summary = DjpegParitySummary{};
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), config.root_dir, .{ .iterate = true });
    defer dir.close(io_impl.io());

    var walker = try dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        if (!isJpegPath(entry.path)) continue;

        switch (try runProbeOneChild(alloc, config, entry.path)) {
            .parse_failed => {
                summary.skipped_parse_failed += 1;
                continue;
            },
            .decode_failed => {
                summary.skipped_decode_failed += 1;
                continue;
            },
            .crashed => {
                summary.skipped_crashed += 1;
                continue;
            },
            .success => {},
        }

        const decoded_hash = try decodeOneFileHashAlloc(alloc, config.root_dir, entry.path);
        defer alloc.free(decoded_hash);

        const djpeg_rgba = readDjpegRgbaAlloc(alloc, config.root_dir, entry.path) catch |err| {
            switch (err) {
                error.DjpegInvalidImage => {
                    summary.skipped_djpeg_invalid += 1;
                    continue;
                },
                error.DjpegUnavailable => {
                    summary.skipped_djpeg_error += 1;
                    continue;
                },
                else => {
                    summary.skipped_djpeg_error += 1;
                    std.debug.print("DJPEG_ERROR\t{s}\terror={s}\n", .{ entry.path, @errorName(err) });
                    continue;
                },
            }
        };
        defer alloc.free(djpeg_rgba);

        const djpeg_hash = try sha256HexAlloc(alloc, djpeg_rgba);
        defer alloc.free(djpeg_hash);

        summary.compared += 1;
        if (std.mem.eql(u8, decoded_hash, djpeg_hash)) {
            summary.matched += 1;
        } else {
            summary.mismatched += 1;
            std.debug.print(
                "MISMATCH\t{s}\tdecoded={s}\tdjpeg={s}\n",
                .{ entry.path, decoded_hash, djpeg_hash },
            );
        }
    }

    return summary;
}

fn compareOneWithDjpeg(alloc: Allocator, root_dir: []const u8, relative_path: []const u8) !void {
    const metadata = try loadImageMetadataAlloc(alloc, root_dir, relative_path);
    const decoded_rgba = try decodeOneFileRgbaAlloc(alloc, root_dir, relative_path);
    defer alloc.free(decoded_rgba);
    const decoded_hash = try sha256HexAlloc(alloc, decoded_rgba);
    defer alloc.free(decoded_hash);

    const djpeg_rgba = try readDjpegRgbaAlloc(alloc, root_dir, relative_path);
    defer alloc.free(djpeg_rgba);
    const djpeg_hash = try sha256HexAlloc(alloc, djpeg_rgba);
    defer alloc.free(djpeg_hash);

    const compare_len = @min(decoded_rgba.len, djpeg_rgba.len);
    var diff_count: usize = 0;
    var first_diff: ?usize = null;
    var channel_diff_counts = @as([4]usize, @splat(0));
    var bbox: ?DiffBoundingBox = null;
    const block_cols = divCeil(metadata.width, 8);
    const block_rows = divCeil(metadata.height, 8);
    const total_blocks = block_cols * block_rows;
    var diff_blocks = try alloc.alloc(bool, total_blocks);
    defer alloc.free(diff_blocks);
    @memset(diff_blocks, false);
    var diff_block_count: usize = 0;
    var diff_block_bbox: ?DiffBoundingBox = null;
    for (0..compare_len) |i| {
        if (decoded_rgba[i] != djpeg_rgba[i]) {
            diff_count += 1;
            if (first_diff == null) first_diff = i;
            channel_diff_counts[i % 4] += 1;

            const pixel_index = i / 4;
            const x = pixelIndexToX(pixel_index, metadata.width);
            const y = pixelIndexToY(pixel_index, metadata.width);
            if (bbox) |*existing| {
                existing.min_x = @min(existing.min_x, x);
                existing.min_y = @min(existing.min_y, y);
                existing.max_x = @max(existing.max_x, x);
                existing.max_y = @max(existing.max_y, y);
            } else {
                bbox = .{
                    .min_x = x,
                    .min_y = y,
                    .max_x = x,
                    .max_y = y,
                };
            }

            const block_x = @divFloor(x, 8);
            const block_y = @divFloor(y, 8);
            const block_index = block_y * block_cols + block_x;
            if (!diff_blocks[block_index]) {
                diff_blocks[block_index] = true;
                diff_block_count += 1;
                if (diff_block_bbox) |*existing| {
                    existing.min_x = @min(existing.min_x, block_x);
                    existing.min_y = @min(existing.min_y, block_y);
                    existing.max_x = @max(existing.max_x, block_x);
                    existing.max_y = @max(existing.max_y, block_y);
                } else {
                    diff_block_bbox = .{
                        .min_x = block_x,
                        .min_y = block_y,
                        .max_x = block_x,
                        .max_y = block_y,
                    };
                }
            }
        }
    }
    diff_count += @max(decoded_rgba.len, djpeg_rgba.len) - compare_len;

    std.debug.print("compare-one:\n", .{});
    std.debug.print("  path={s}\n", .{relative_path});
    std.debug.print("  kind={s}\n", .{metadata.kind_name});
    std.debug.print("  dimensions={d}x{d}\n", .{ metadata.width, metadata.height });
    std.debug.print("  bits_per_sample={d}\n", .{metadata.bits_per_sample});
    std.debug.print("  components={d}\n", .{metadata.component_count});
    std.debug.print("  scans={d}\n", .{metadata.scan_count});
    std.debug.print("  restart_interval={d}\n", .{metadata.restart_interval});
    std.debug.print("  sampling=", .{});
    printSamplingSummary(metadata);
    std.debug.print("\n", .{});
    std.debug.print("  decoded_len={d}\n", .{decoded_rgba.len});
    std.debug.print("  djpeg_len={d}\n", .{djpeg_rgba.len});
    std.debug.print("  decoded_hash={s}\n", .{decoded_hash});
    std.debug.print("  djpeg_hash={s}\n", .{djpeg_hash});
    std.debug.print("  equal={s}\n", .{if (std.mem.eql(u8, decoded_hash, djpeg_hash)) "yes" else "no"});
    std.debug.print("  differing_bytes={d}\n", .{diff_count});
    std.debug.print(
        "  differing_channels=r:{d} g:{d} b:{d} a:{d}\n",
        .{ channel_diff_counts[0], channel_diff_counts[1], channel_diff_counts[2], channel_diff_counts[3] },
    );
    if (bbox) |value| {
        std.debug.print(
            "  diff_bbox_xy={d},{d}..{d},{d}\n",
            .{ value.min_x, value.min_y, value.max_x, value.max_y },
        );
    }
    std.debug.print("  differing_blocks={d}\n", .{diff_block_count});
    if (diff_block_bbox) |value| {
        std.debug.print(
            "  diff_block_bbox={d},{d}..{d},{d}\n",
            .{ value.min_x, value.min_y, value.max_x, value.max_y },
        );
    }
    if (first_diff) |index| {
        const pixel_index = index / 4;
        const channel_index = index % 4;
        const x = pixelIndexToX(pixel_index, metadata.width);
        const y = pixelIndexToY(pixel_index, metadata.width);
        const decoded_pixel = rgbaPixelAt(decoded_rgba, pixel_index);
        const djpeg_pixel = rgbaPixelAt(djpeg_rgba, pixel_index);
        std.debug.print("  first_diff_index={d}\n", .{index});
        std.debug.print("  first_diff_pixel={d}\n", .{pixel_index});
        std.debug.print("  first_diff_xy={d},{d}\n", .{ x, y });
        std.debug.print("  first_diff_block_xy={d},{d}\n", .{ @divFloor(x, 8), @divFloor(y, 8) });
        std.debug.print("  first_diff_channel={s}\n", .{rgbaChannelName(channel_index)});
        std.debug.print("  decoded_first_pixel_rgba={any}\n", .{decoded_pixel});
        std.debug.print("  djpeg_first_pixel_rgba={any}\n", .{djpeg_pixel});
        std.debug.print("  decoded_window={any}\n", .{decoded_rgba[index..@min(index + 16, decoded_rgba.len)]});
        std.debug.print("  djpeg_window={any}\n", .{djpeg_rgba[index..@min(index + 16, djpeg_rgba.len)]});
        const pixel_start = pixel_index * 4;
        const pixel_end = @min(pixel_start + 16, compare_len);
        std.debug.print("  decoded_pixel_window={any}\n", .{decoded_rgba[pixel_start..pixel_end]});
        std.debug.print("  djpeg_pixel_window={any}\n", .{djpeg_rgba[pixel_start..pixel_end]});
    }
}

const DiffBoundingBox = struct {
    min_x: usize,
    min_y: usize,
    max_x: usize,
    max_y: usize,
};

fn divCeil(value: usize, divisor: usize) usize {
    return @divFloor(value + divisor - 1, divisor);
}

const SamplingFactor = struct {
    horizontal: u8,
    vertical: u8,
};

const ImageMetadata = struct {
    width: usize,
    height: usize,
    kind_name: []const u8,
    bits_per_sample: u8,
    component_count: usize,
    scan_count: usize,
    restart_interval: usize,
    sampling: [4]SamplingFactor,
};

fn loadImageMetadataAlloc(alloc: Allocator, root_dir: []const u8, relative_path: []const u8) !ImageMetadata {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{});
    defer dir.close(io_impl.io());

    const bytes = try dir.readFileAlloc(io_impl.io(), relative_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(bytes);

    const structure = try jpeg.parseStructure(bytes);
    var sampling = std.mem.zeroes([4]SamplingFactor);
    for (0..structure.info.component_count) |i| {
        sampling[i] = .{
            .horizontal = structure.info.components[i].horizontal_sampling,
            .vertical = structure.info.components[i].vertical_sampling,
        };
    }
    return .{
        .width = structure.info.width,
        .height = structure.info.height,
        .kind_name = @tagName(structure.info.frame_kind),
        .bits_per_sample = structure.info.bits_per_sample,
        .component_count = structure.info.component_count,
        .scan_count = structure.scan_count,
        .restart_interval = structure.restart_interval orelse 0,
        .sampling = sampling,
    };
}

fn printSamplingSummary(metadata: ImageMetadata) void {
    for (0..metadata.component_count) |i| {
        if (i != 0) std.debug.print(",", .{});
        std.debug.print("{d}x{d}", .{
            metadata.sampling[i].horizontal,
            metadata.sampling[i].vertical,
        });
    }
}

fn pixelIndexToX(pixel_index: usize, width: usize) usize {
    return pixel_index % width;
}

fn pixelIndexToY(pixel_index: usize, width: usize) usize {
    return pixel_index / width;
}

fn rgbaPixelAt(rgba: []const u8, pixel_index: usize) [4]u8 {
    const start = pixel_index * 4;
    return .{
        rgba[start + 0],
        rgba[start + 1],
        rgba[start + 2],
        rgba[start + 3],
    };
}

fn rgbaChannelName(index: usize) []const u8 {
    return switch (index) {
        0 => "r",
        1 => "g",
        2 => "b",
        3 => "a",
        else => "unknown",
    };
}

fn probeOneFile(
    alloc: Allocator,
    root_dir: []const u8,
    relative_path: []const u8,
    print_success: bool,
    quiet_failure: bool,
) !Outcome {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{});
    defer dir.close(io_impl.io());

    const bytes = try dir.readFileAlloc(io_impl.io(), relative_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(bytes);

    const parse_result = jpeg.parseStructure(bytes);
    if (parse_result) |structure| {
        const decoded = jpeg.decodeRgba(alloc, bytes) catch |decode_err| {
            if (!quiet_failure) {
                printParsedOutcome("DECODE_FAIL", relative_path, structure);
                std.debug.print("\terror={s}\n", .{@errorName(decode_err)});
            }
            return .decode_failed;
        };
        defer alloc.free(decoded.rgba);

        if (print_success) {
            printParsedOutcome("OK", relative_path, structure);
            std.debug.print("\n", .{});
        }
        return .success;
    } else |parse_err| {
        if (!quiet_failure) {
            std.debug.print("PARSE_FAIL\t{s}\terror={s}\n", .{ relative_path, @errorName(parse_err) });
        }
        return .parse_failed;
    }
}

fn printParsedOutcome(prefix: []const u8, relative_path: []const u8, structure: jpeg.Structure) void {
    std.debug.print(
        "{s}\t{s}\tkind={s}\t{d}x{d}\tcomponents={d}\tscans={d}\tsampling=",
        .{
            prefix,
            relative_path,
            @tagName(structure.info.frame_kind),
            structure.info.width,
            structure.info.height,
            structure.info.component_count,
            structure.scan_count,
        },
    );
    for (0..structure.info.component_count) |i| {
        if (i != 0) std.debug.print(",", .{});
        const component = structure.info.components[i];
        std.debug.print("{d}x{d}", .{ component.horizontal_sampling, component.vertical_sampling });
    }
}

fn runProbeOneChild(alloc: Allocator, config: Config, relative_path: []const u8) !Outcome {
    var argv = std.array_list.Managed([]const u8).init(alloc);
    defer argv.deinit();

    try argv.append(config.self_exe);
    try argv.append("probe-one");
    try argv.append(config.root_dir);
    try argv.append(relative_path);
    if (config.print_successes) {
        try argv.append("--print-success");
    }
    if (config.quiet_failures) {
        try argv.append("--quiet-failure");
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var child = try std.process.spawn(io_impl.io(), .{
        .argv = argv.items,
        .stdin = .ignore,
        .stdout = if (config.quiet_failures) .ignore else .inherit,
        .stderr = if (config.quiet_failures) .ignore else .inherit,
    });
    const term = try child.wait(io_impl.io());
    return switch (term) {
        .exited => |code| switch (code) {
            @intFromEnum(ProbeExitCode.success) => .success,
            @intFromEnum(ProbeExitCode.parse_failed) => .parse_failed,
            @intFromEnum(ProbeExitCode.decode_failed) => .decode_failed,
            else => blk: {
                std.debug.print("CRASH\t{s}\texit_code={d}\n", .{ relative_path, code });
                break :blk .crashed;
            },
        },
        else => blk: {
            std.debug.print("CRASH\t{s}\tabnormal_termination\n", .{relative_path});
            break :blk .crashed;
        },
    };
}

const DjpegOutcome = enum {
    valid,
    invalid,
    @"error",
};

fn runDjpegChild(alloc: Allocator, root_dir: []const u8, relative_path: []const u8) !DjpegOutcome {
    const full_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, relative_path });
    defer alloc.free(full_path);
    const djpeg_path = try resolveDjpegPath(alloc) orelse return .@"error";
    defer alloc.free(djpeg_path);
    var environ_map = std.process.Environ.Map.init(alloc);
    defer environ_map.deinit();
    try environ_map.put("JSIMD_FORCENONE", "1");

    var argv = std.array_list.Managed([]const u8).init(alloc);
    defer argv.deinit();
    try argv.append(djpeg_path);
    try argv.append("-dct");
    try argv.append("int");
    try argv.append(full_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var child = std.process.spawn(io_impl.io(), .{
        .argv = argv.items,
        .expand_arg0 = .no_expand,
        .environ_map = &environ_map,
        .stdin = .ignore,
        .stdout = .ignore,
        .stderr = .ignore,
    }) catch |err| switch (err) {
        error.FileNotFound => return .@"error",
        else => return err,
    };
    const term = try child.wait(io_impl.io());
    return switch (term) {
        .exited => |code| if (code == 0) .valid else .invalid,
        else => .invalid,
    };
}

fn decodeOneFileRgbaAlloc(alloc: Allocator, root_dir: []const u8, relative_path: []const u8) ![]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{});
    defer dir.close(io_impl.io());

    const bytes = try dir.readFileAlloc(io_impl.io(), relative_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(bytes);

    const decoded = try jpeg.decodeRgba(alloc, bytes);
    return decoded.rgba;
}

fn decodeOneFileHashAlloc(alloc: Allocator, root_dir: []const u8, relative_path: []const u8) ![]u8 {
    const rgba = try decodeOneFileRgbaAlloc(alloc, root_dir, relative_path);
    defer alloc.free(rgba);
    return sha256HexAlloc(alloc, rgba);
}

fn sha256HexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    return std.fmt.allocPrint(alloc, "{s}", .{std.fmt.bytesToHex(digest, .lower)});
}

fn readDjpegRgbaAlloc(alloc: Allocator, root_dir: []const u8, relative_path: []const u8) ![]u8 {
    const full_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, relative_path });
    defer alloc.free(full_path);
    const djpeg_path = try resolveDjpegPath(alloc) orelse return error.DjpegUnavailable;
    defer alloc.free(djpeg_path);
    var environ_map = std.process.Environ.Map.init(alloc);
    defer environ_map.deinit();
    try environ_map.put("JSIMD_FORCENONE", "1");

    var argv = std.array_list.Managed([]const u8).init(alloc);
    defer argv.deinit();
    try argv.append(djpeg_path);
    try argv.append("-dct");
    try argv.append("int");
    try argv.append("-rgb");
    try argv.append(full_path);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    var child = std.process.spawn(io_impl.io(), .{
        .argv = argv.items,
        .expand_arg0 = .no_expand,
        .environ_map = &environ_map,
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .ignore,
    }) catch |err| switch (err) {
        error.FileNotFound => return error.DjpegUnavailable,
        else => return err,
    };
    const stdout_pipe = child.stdout.?;
    var reader = stdout_pipe.readerStreaming(io_impl.io(), &.{});
    const ppm = try reader.interface.allocRemaining(alloc, .unlimited);
    defer alloc.free(ppm);

    const term = try child.wait(io_impl.io());
    switch (term) {
        .exited => |code| if (code != 0) return error.DjpegInvalidImage,
        else => return error.DjpegInvalidImage,
    }

    return ppmToRgbaAlloc(alloc, ppm);
}

fn ppmToRgbaAlloc(alloc: Allocator, ppm: []const u8) ![]u8 {
    var index: usize = 0;
    const magic = try readPpmToken(ppm, &index);
    if (!std.mem.eql(u8, magic, "P6")) return error.InvalidPpm;

    const width_token = try readPpmToken(ppm, &index);
    const height_token = try readPpmToken(ppm, &index);
    const maxval_token = try readPpmToken(ppm, &index);

    const width = try std.fmt.parseInt(usize, width_token, 10);
    const height = try std.fmt.parseInt(usize, height_token, 10);
    const maxval = try std.fmt.parseInt(u32, maxval_token, 10);
    if (maxval == 0 or maxval > 65535) return error.InvalidPpm;

    if (index >= ppm.len or !std.ascii.isWhitespace(ppm[index])) return error.InvalidPpm;
    index += 1;
    const bytes_per_sample: usize = if (maxval < 256) 1 else 2;
    const rgb_len = width * height * 3 * bytes_per_sample;
    if (ppm.len - index != rgb_len) return error.InvalidPpm;

    const rgba = try alloc.alloc(u8, width * height * 4);
    var src: usize = index;
    var dst: usize = 0;
    while (src < ppm.len) : (dst += 4) {
        rgba[dst + 0] = if (bytes_per_sample == 1)
            scalePpmSampleToU8(ppm[src + 0], maxval)
        else
            scalePpmSampleToU8(std.mem.readInt(u16, ppm[src..][0..2], .big), maxval);
        src += bytes_per_sample;
        rgba[dst + 1] = if (bytes_per_sample == 1)
            scalePpmSampleToU8(ppm[src + 0], maxval)
        else
            scalePpmSampleToU8(std.mem.readInt(u16, ppm[src..][0..2], .big), maxval);
        src += bytes_per_sample;
        rgba[dst + 2] = if (bytes_per_sample == 1)
            scalePpmSampleToU8(ppm[src + 0], maxval)
        else
            scalePpmSampleToU8(std.mem.readInt(u16, ppm[src..][0..2], .big), maxval);
        src += bytes_per_sample;
        rgba[dst + 3] = 0xff;
    }
    return rgba;
}

fn scalePpmSampleToU8(sample: anytype, maxval: u32) u8 {
    const sample_u32: u32 = @intCast(sample);
    if (maxval == 255) return @intCast(sample_u32);
    return @intCast((sample_u32 * 255 + (maxval / 2)) / maxval);
}

fn readPpmToken(ppm: []const u8, index: *usize) ![]const u8 {
    while (index.* < ppm.len) {
        const byte = ppm[index.*];
        if (std.ascii.isWhitespace(byte)) {
            index.* += 1;
            continue;
        }
        if (byte == '#') {
            while (index.* < ppm.len and ppm[index.*] != '\n') : (index.* += 1) {}
            continue;
        }
        break;
    }

    if (index.* >= ppm.len) return error.InvalidPpm;
    const start = index.*;
    while (index.* < ppm.len and !std.ascii.isWhitespace(ppm[index.*])) : (index.* += 1) {}
    return ppm[start..index.*];
}

fn readGitHeadAlloc(alloc: Allocator, io: std.Io, root_dir: []const u8) !?[]u8 {
    const head_path = try std.fmt.allocPrint(alloc, "{s}/.git/HEAD", .{root_dir});
    defer alloc.free(head_path);
    const head_raw = std.Io.Dir.cwd().readFileAlloc(io, head_path, alloc, .limited(256)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer alloc.free(head_raw);

    const head = std.mem.trim(u8, head_raw, " \r\n\t");
    if (std.mem.startsWith(u8, head, "ref: ")) {
        const ref_name = head["ref: ".len..];
        const ref_path = try std.fmt.allocPrint(alloc, "{s}/.git/{s}", .{ root_dir, ref_name });
        defer alloc.free(ref_path);
        const ref_raw = std.Io.Dir.cwd().readFileAlloc(io, ref_path, alloc, .limited(256)) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer alloc.free(ref_raw);
        return try alloc.dupe(u8, std.mem.trim(u8, ref_raw, " \r\n\t"));
    }

    return try alloc.dupe(u8, head);
}

fn resolveDjpegPath(alloc: Allocator) !?[]u8 {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    const candidates = [_][]const u8{
        "/opt/homebrew/bin/djpeg",
        "/usr/local/bin/djpeg",
        "/usr/bin/djpeg",
    };
    for (candidates) |candidate| {
        std.Io.Dir.cwd().access(io_impl.io(), candidate, .{}) catch continue;
        return try alloc.dupe(u8, candidate);
    }
    return null;
}

fn ensureSeedCorporaAvailable(alloc: Allocator, root_dir: []const u8, refresh: bool, allow_fetch: bool) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const dir_exists = blk: {
        var dir = std.Io.Dir.cwd().openDir(io_impl.io(), root_dir, .{}) catch break :blk false;
        dir.close(io_impl.io());
        break :blk true;
    };

    if (!dir_exists) {
        if (!allow_fetch) {
            std.debug.print(
                "seed corpora unavailable at {s}; run `fetch` first or use a populated checkout\n",
                .{root_dir},
            );
            return error.SeedCorporaUnavailable;
        }
        try runChild(alloc, &.{ "git", "clone", "--depth=1", default_seed_corpora_url, root_dir });
        return;
    }

    if (refresh) {
        try runChild(alloc, &.{ "git", "-C", root_dir, "pull", "--ff-only" });
    }
}

fn runChild(alloc: Allocator, argv: []const []const u8) !void {
    _ = alloc;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();

    var child = try std.process.spawn(io_impl.io(), .{
        .argv = argv,
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    });
    const term = try child.wait(io_impl.io());
    switch (term) {
        .exited => |code| if (code != 0) return error.ChildProcessFailed,
        else => return error.ChildProcessFailed,
    }
}

fn isJpegPath(path: []const u8) bool {
    return std.ascii.endsWithIgnoreCase(path, ".jpg") or
        std.ascii.endsWithIgnoreCase(path, ".jpeg");
}

fn printSummary(summary: Summary) void {
    std.debug.print(
        \\summary:
        \\  files_scanned={d}
        \\  jpeg_files={d}
        \\  success={d}
        \\  parse_failed={d}
        \\  decode_failed={d}
        \\  crashed={d}
        \\
    , .{
        summary.total_files,
        summary.jpeg_files,
        summary.success,
        summary.parse_failed,
        summary.decode_failed,
        summary.crashed,
    });
}

fn printDjpegTriageSummary(summary: DjpegTriageSummary) void {
    std.debug.print(
        \\djpeg triage summary:
        \\  decode_failed={d}
        \\  djpeg_valid={d}
        \\  djpeg_invalid={d}
        \\  djpeg_error={d}
        \\
    , .{
        summary.decode_failed,
        summary.djpeg_valid,
        summary.djpeg_invalid,
        summary.djpeg_error,
    });
}

fn printDjpegParitySummary(summary: DjpegParitySummary) void {
    std.debug.print(
        \\djpeg parity summary:
        \\  compared={d}
        \\  matched={d}
        \\  mismatched={d}
        \\  skipped_parse_failed={d}
        \\  skipped_decode_failed={d}
        \\  skipped_crashed={d}
        \\  skipped_djpeg_invalid={d}
        \\  skipped_djpeg_error={d}
        \\
    , .{
        summary.compared,
        summary.matched,
        summary.mismatched,
        summary.skipped_parse_failed,
        summary.skipped_decode_failed,
        summary.skipped_crashed,
        summary.skipped_djpeg_invalid,
        summary.skipped_djpeg_error,
    });
}

fn printStatusSummary(summary: StatusSummary) void {
    std.debug.print(
        \\status:
        \\  root_dir={s}
        \\  corpus_present={s}
        \\  jpeg_files={d}
        \\  git_head={s}
        \\  djpeg={s}
        \\
    , .{
        summary.root_dir,
        if (summary.corpus_present) "yes" else "no",
        summary.jpeg_files,
        if (summary.git_head) |head| head else "unknown",
        if (summary.djpeg_path) |path| path else "missing",
    });
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage:
        \\  {s} fetch [seed_corpora_dir]
        \\  {s} status [seed_corpora_dir]
        \\  {s} run [seed_corpora_dir] [--refresh] [--no-fetch] [--print-successes] [--fail-on-decode] [--quiet-failures]
        \\  {s} probe-one <seed_corpora_dir> <relative_jpeg_path> [--print-success]
        \\  {s} triage-djpeg [seed_corpora_dir] [--refresh] [--no-fetch]
        \\  {s} compare-one <seed_corpora_dir> <relative_jpeg_path>
        \\  {s} triage-djpeg-parity [seed_corpora_dir] [--refresh] [--no-fetch]
        \\
        \\defaults:
        \\  seed_corpora_dir = {s}
        \\  upstream_url      = {s}
        \\
    , .{ argv0, argv0, argv0, argv0, argv0, argv0, argv0, default_seed_corpora_dir, default_seed_corpora_url });
}

fn toExitCode(outcome: Outcome) ProbeExitCode {
    return switch (outcome) {
        .success => .success,
        .parse_failed => .parse_failed,
        .decode_failed => .decode_failed,
        .crashed => unreachable,
    };
}
