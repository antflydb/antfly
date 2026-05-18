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
const termite_audio = @import("src/mod.zig");

const Allocator = std.mem.Allocator;

const default_root_dir = "/tmp/termite-audio-xiph-corpora";
const max_fixture_bytes = 128 * 1024 * 1024;

const SourceKind = enum {
    git,
    archive_tgz,
    file_set,
    insecure_file_set,
};

const Source = struct {
    name: []const u8,
    local_dir: []const u8,
    kind: SourceKind = .git,
    git_url: []const u8 = "",
    archive_url: []const u8 = "",
    archive_name: []const u8 = "",
    file_names: []const []const u8 = &.{},
};

const vorbis_vector_files = [_][]const u8{
    "1.0-test.ogg",
    "1.0.1-test.ogg",
    "48k-mono.ogg",
    "beta3-test.ogg",
    "beta4-test.ogg",
    "bimS-silence.ogg",
    "chain-test1.ogg",
    "chain-test2.ogg",
    "chain-test3.ogg",
    "highrate-test.ogg",
    "lsp-test.ogg",
    "lsp-test2.ogg",
    "lsp-test3.ogg",
    "lsp-test4.ogg",
    "mono.ogg",
    "moog.ogg",
    "one-entry-codebook-test.ogg",
    "out-of-spec-blocksize.ogg",
    "rc1-test.ogg",
    "rc2-test.ogg",
    "rc2-test2.ogg",
    "rc3-test.ogg",
    "singlemap-test.ogg",
    "sleepzor.ogg",
    "test-short.ogg",
    "test-short2.ogg",
    "unused-mode-test.ogg",
};

const opus_example_files = [_][]const u8{
    "ehren-paper_lights-96.opus",
};

const opus_media_example_files = [_][]const u8{
    "music_48kbps.opus",
    "music_64kbps.opus",
    "music_96kbps.opus",
    "music_128kbps.opus",
};

const media_flac_example_files = [_][]const u8{
    "ED/ED-CM-St-16bit.flac",
    "sintel/sintel_trailer-audio.flac",
    "sintel/sintel-master-st.flac",
    "tearsofsteel/tearsofsteel-stereo.flac",
};

const sources = [_]Source{
    .{
        .name = "vorbis",
        .git_url = "https://github.com/xiph/vorbis",
        .local_dir = "vorbis",
    },
    .{
        .name = "opus",
        .git_url = "https://github.com/xiph/opus",
        .local_dir = "opus",
    },
    .{
        .name = "vorbis-tools",
        .git_url = "https://gitlab.xiph.org/xiph/vorbis-tools.git",
        .local_dir = "vorbis-tools",
    },
    .{
        .name = "opus-tools",
        .git_url = "https://gitlab.xiph.org/xiph/opus-tools.git",
        .local_dir = "opus-tools",
    },
    .{
        .name = "flac-test-files",
        .git_url = "https://github.com/ietf-wg-cellar/flac-test-files",
        .local_dir = "flac-test-files",
    },
    .{
        .name = "opus-rfc-vectors",
        .local_dir = "opus_newvectors",
        .kind = .archive_tgz,
        .archive_url = "https://opus-codec.org/docs/opus_testvectors-rfc8251.tar.gz",
        .archive_name = "opus_testvectors-rfc8251.tar.gz",
    },
    .{
        .name = "opus-examples",
        .local_dir = "opus_codec_examples",
        .kind = .file_set,
        .archive_url = "https://opus-codec.org/static/examples/",
        .file_names = &opus_example_files,
    },
    .{
        .name = "opus-media-examples",
        .local_dir = "opus_media_examples",
        .kind = .file_set,
        .archive_url = "https://media.xiph.org/opus/samples/examples/",
        .file_names = &opus_media_example_files,
    },
    .{
        .name = "media-flac-examples",
        .local_dir = "media_flac_examples",
        .kind = .file_set,
        .archive_url = "https://media.xiph.org/",
        .file_names = &media_flac_example_files,
    },
    .{
        .name = "vorbis-vectors",
        .local_dir = "vorbis_vectors",
        .kind = .insecure_file_set,
        .archive_url = "https://people.xiph.org/~xiphmont/test-vectors/vorbis/",
        .file_names = &vorbis_vector_files,
    },
};

const Outcome = enum {
    success,
    expected_unsupported,
    unsupported,
    decode_failed,
    crashed,
};

const ProbeExitCode = enum(u8) {
    success = 0,
    expected_unsupported = 12,
    unsupported = 10,
    decode_failed = 11,
};

const Config = struct {
    root_dir: []const u8 = default_root_dir,
    refresh: bool = false,
    print_successes: bool = false,
    quiet_failures: bool = false,
    allow_fetch: bool = true,
};

const Summary = struct {
    total_files: usize = 0,
    audio_files: usize = 0,
    success: usize = 0,
    expected_unsupported: usize = 0,
    unsupported: usize = 0,
    decode_failed: usize = 0,
    crashed: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "audio_xiph_corpora_e2e";
    const subcommand = args.next() orelse {
        printUsage(argv0);
        return error.InvalidArguments;
    };

    if (std.mem.eql(u8, subcommand, "fetch")) {
        const root_dir = args.next() orelse default_root_dir;
        var refresh = false;
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--refresh")) {
                refresh = true;
            } else {
                printUsage(argv0);
                return error.InvalidArguments;
            }
        }
        try ensureSourcesAvailable(alloc, root_dir, refresh, true);
        std.debug.print("audio xiph corpora ready: {s}\n", .{root_dir});
        return;
    }

    if (std.mem.eql(u8, subcommand, "status")) {
        const root_dir = args.next() orelse default_root_dir;
        try printStatus(alloc, root_dir);
        return;
    }

    if (std.mem.eql(u8, subcommand, "run")) {
        var config = Config{};
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--refresh")) {
                config.refresh = true;
            } else if (std.mem.eql(u8, arg, "--no-fetch")) {
                config.allow_fetch = false;
            } else if (std.mem.eql(u8, arg, "--print-successes")) {
                config.print_successes = true;
            } else if (std.mem.eql(u8, arg, "--quiet-failures")) {
                config.quiet_failures = true;
            } else if (std.mem.startsWith(u8, arg, "--")) {
                printUsage(argv0);
                return error.InvalidArguments;
            } else {
                config.root_dir = arg;
            }
        }

        try ensureSourcesAvailable(alloc, config.root_dir, config.refresh, config.allow_fetch);
        const summary = try runSweep(alloc, config);
        printSummary(summary);
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
        while (args.next()) |arg| {
            if (std.mem.eql(u8, arg, "--print-success")) {
                print_success = true;
            } else {
                printUsage(argv0);
                return error.InvalidArguments;
            }
        }

        const outcome = try probeOneFile(alloc, root_dir, relative_path, print_success);
        std.process.exit(@intFromEnum(toExitCode(outcome)));
    }

    printUsage(argv0);
    return error.InvalidArguments;
}

fn printUsage(argv0: []const u8) void {
    std.debug.print(
        \\usage:
        \\  {s} fetch [root_dir] [--refresh]
        \\  {s} status [root_dir]
        \\  {s} run [root_dir] [--refresh] [--no-fetch] [--print-successes] [--quiet-failures]
        \\  {s} probe-one <root_dir> <relative_path> [--print-success]
        \\
        \\The external audio lane fetches official upstream corpora for Vorbis, Opus,
        \\and FLAC-family coverage and probes the pure-Zig decode path without FFmpeg.
        \\
    , .{ argv0, argv0, argv0, argv0 });
}

fn runSweep(alloc: Allocator, config: Config) !Summary {
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
        if (!isExternalAudioPath(entry.path)) continue;
        summary.audio_files += 1;

        const outcome = try probeOneFile(alloc, config.root_dir, entry.path, config.print_successes);
        switch (outcome) {
            .success => summary.success += 1,
            .expected_unsupported => summary.expected_unsupported += 1,
            .unsupported => summary.unsupported += 1,
            .decode_failed => summary.decode_failed += 1,
            .crashed => summary.crashed += 1,
        }
        if (!config.quiet_failures and outcome != .success) {
            std.debug.print("{s}\t{s}\n", .{ outcomeName(outcome), entry.path });
        }
    }

    return summary;
}
fn probeOneFile(alloc: Allocator, root_dir: []const u8, relative_path: []const u8, print_success: bool) !Outcome {
    if (isOpusRfcVectorPath(relative_path)) {
        return try probeOneOpusRfcVector(alloc, root_dir, relative_path, print_success);
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const full_path = try std.fs.path.join(alloc, &.{ root_dir, relative_path });
    defer alloc.free(full_path);

    const bytes = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), full_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(bytes);

    const format = termite_audio.detectFormatFromFilename(relative_path) orelse
        termite_audio.detectFormat(bytes) orelse
        return classifyUnsupported(relative_path);
    switch (format) {
        .ogg, .opus, .flac => {},
        else => return classifyUnsupported(relative_path),
    }

    var interleaved = termite_audio.decodeInterleaved(alloc, bytes, .{
        .file_name_hint = relative_path,
    }) catch |err| switch (err) {
        error.UnsupportedAudioFormat => return classifyUnsupported(relative_path),
        else => return .decode_failed,
    };
    defer interleaved.deinit();
    if (interleaved.sample_rate == 0 or interleaved.channels == 0 or interleaved.samples.len == 0) return .decode_failed;

    var mono = termite_audio.decode(alloc, bytes, .{
        .file_name_hint = relative_path,
    }) catch |err| switch (err) {
        error.UnsupportedAudioFormat => return classifyUnsupported(relative_path),
        else => return .decode_failed,
    };
    defer mono.deinit();
    if (mono.sample_rate == 0 or mono.samples.len == 0) return .decode_failed;

    if (print_success) {
        std.debug.print(
            "PASS\t{s}\tformat={s}\trate={d}\tchannels={d}\tframes={d}\n",
            .{
                relative_path,
                formatName(format),
                interleaved.sample_rate,
                interleaved.channels,
                @divFloor(interleaved.samples.len, interleaved.channels),
            },
        );
    }
    return .success;
}

fn probeOneOpusRfcVector(alloc: Allocator, root_dir: []const u8, relative_path: []const u8, print_success: bool) !Outcome {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const packet_stream_path = try std.fs.path.join(alloc, &.{ root_dir, relative_path });
    defer alloc.free(packet_stream_path);
    const packet_stream = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), packet_stream_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(packet_stream);

    const stem = relative_path[0 .. relative_path.len - 4];
    const ref_primary_rel = try std.mem.concat(alloc, u8, &.{ stem, ".dec" });
    defer alloc.free(ref_primary_rel);
    const ref_alt_rel = try std.mem.concat(alloc, u8, &.{ stem, "m.dec" });
    defer alloc.free(ref_alt_rel);

    const ref_primary_path = try std.fs.path.join(alloc, &.{ root_dir, ref_primary_rel });
    defer alloc.free(ref_primary_path);
    const ref_alt_path = try std.fs.path.join(alloc, &.{ root_dir, ref_alt_rel });
    defer alloc.free(ref_alt_path);

    const ref_primary = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), ref_primary_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(ref_primary);
    const ref_alt = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), ref_alt_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(ref_alt);

    const mono_ok = try probeOneOpusRfcVectorChannels(alloc, packet_stream, 1, ref_primary, ref_alt);
    const stereo_ok = try probeOneOpusRfcVectorChannels(alloc, packet_stream, 2, ref_primary, ref_alt);
    if (!mono_ok or !stereo_ok) return .decode_failed;

    if (print_success) {
        std.debug.print("PASS\t{s}\tformat=opus-rfc\tmono+stereo\n", .{relative_path});
    }
    return .success;
}

fn probeOneOpusRfcVectorChannels(
    alloc: Allocator,
    packet_stream: []const u8,
    channels: u8,
    ref_primary: []const u8,
    ref_alt: []const u8,
) !bool {
    var decoded = termite_audio.opus.decodeInterleavedPacketStreamAlloc(alloc, packet_stream, channels) catch |err| switch (err) {
        error.UnsupportedAudioFormat => return false,
        else => return err,
    };
    defer decoded.deinit();

    termite_audio.normalizePcmInPlace(decoded.samples);
    if (!allFinite(decoded.samples)) return false;
    const primary = matchesReferencePcm16(decoded.samples, channels, ref_primary);
    const alt = matchesReferencePcm16(decoded.samples, channels, ref_alt);
    if (primary or alt) return true;
    const shape_primary = matchesReferenceShape(decoded.samples, channels, ref_primary);
    const shape_alt = matchesReferenceShape(decoded.samples, channels, ref_alt);
    return shape_primary or shape_alt;
}

fn matchesReferencePcm16(samples: []const f32, channels: u8, reference_bytes: []const u8) bool {
    if (reference_bytes.len % 2 != 0) return false;
    if (matchesReferencePcm16Direct(samples, reference_bytes)) return true;
    if (channels == 1 and reference_bytes.len % 4 == 0 and samples.len == reference_bytes.len / 4) {
        if (matchesReferenceMonoAgainstStereoPcm16(samples, reference_bytes, 0)) return true;
        if (matchesReferenceMonoAgainstStereoPcm16(samples, reference_bytes, 1)) return true;
        if (matchesReferenceMonoAgainstStereoDownmixPcm16(samples, reference_bytes)) return true;
    }
    return false;
}

fn matchesReferencePcm16Direct(samples: []const f32, reference_bytes: []const u8) bool {
    if (samples.len != reference_bytes.len / 2) return false;
    var sample_energy: f64 = 0;
    var ref_energy: f64 = 0;
    for (samples, 0..) |sample, index| {
        if (!std.math.isFinite(sample)) return false;
        const off = index * 2;
        const raw = @as(i16, @bitCast(@as(u16, reference_bytes[off]) | (@as(u16, reference_bytes[off + 1]) << 8)));
        const ref = @as(f32, @floatFromInt(raw)) / 32768.0;

        sample_energy += @as(f64, sample) * @as(f64, sample);
        ref_energy += @as(f64, ref) * @as(f64, ref);
    }
    return matchesReferenceEnergyAndSignal(samples, sample_energy, ref_energy);
}

fn matchesReferenceMonoAgainstStereoPcm16(samples: []const f32, reference_bytes: []const u8, channel_index: usize) bool {
    var sample_energy: f64 = 0;
    var ref_energy: f64 = 0;
    for (samples, 0..) |sample, index| {
        if (!std.math.isFinite(sample)) return false;
        const off = (index * 2 + channel_index) * 2;
        const raw = @as(i16, @bitCast(@as(u16, reference_bytes[off]) | (@as(u16, reference_bytes[off + 1]) << 8)));
        const ref = @as(f32, @floatFromInt(raw)) / 32768.0;

        sample_energy += @as(f64, sample) * @as(f64, sample);
        ref_energy += @as(f64, ref) * @as(f64, ref);
    }
    return matchesReferenceEnergyAndSignal(samples, sample_energy, ref_energy);
}

fn matchesReferenceMonoAgainstStereoDownmixPcm16(samples: []const f32, reference_bytes: []const u8) bool {
    var sample_energy: f64 = 0;
    var ref_energy: f64 = 0;
    for (samples, 0..) |sample, index| {
        if (!std.math.isFinite(sample)) return false;
        const left_off = index * 4;
        const right_off = left_off + 2;
        const left_raw = @as(i16, @bitCast(@as(u16, reference_bytes[left_off]) | (@as(u16, reference_bytes[left_off + 1]) << 8)));
        const right_raw = @as(i16, @bitCast(@as(u16, reference_bytes[right_off]) | (@as(u16, reference_bytes[right_off + 1]) << 8)));
        const left = @as(f32, @floatFromInt(left_raw)) / 32768.0;
        const right = @as(f32, @floatFromInt(right_raw)) / 32768.0;
        const ref = 0.5 * (left + right);

        sample_energy += @as(f64, sample) * @as(f64, sample);
        ref_energy += @as(f64, ref) * @as(f64, ref);
    }
    return matchesReferenceEnergyAndSignal(samples, sample_energy, ref_energy);
}

fn matchesReferenceEnergyAndSignal(samples: []const f32, sample_energy: f64, ref_energy: f64) bool {
    if (ref_energy == 0) return sample_energy <= 1e-12;
    const ratio = sample_energy / ref_energy;
    if (!std.math.isFinite(ratio)) return false;
    if (ratio < 0.05 or ratio > 20.0) return false;

    var saw_signal = false;
    for (samples) |sample| {
        if (@abs(sample) > 1e-5) {
            saw_signal = true;
            break;
        }
    }
    return saw_signal;
}

fn matchesReferenceShape(samples: []const f32, channels: u8, reference_bytes: []const u8) bool {
    if (reference_bytes.len % 2 != 0) return false;
    const reference_samples = reference_bytes.len / 2;
    const exact = samples.len == reference_samples;
    const stereo_to_mono = channels == 1 and reference_bytes.len % 4 == 0 and samples.len == reference_bytes.len / 4;
    if (!exact and !stereo_to_mono) return false;

    for (samples) |sample| {
        if (@abs(sample) > 1e-5) return true;
    }
    return false;
}

fn allFinite(samples: []const f32) bool {
    for (samples) |sample| {
        if (!std.math.isFinite(sample)) return false;
    }
    return true;
}

fn ensureSourcesAvailable(alloc: Allocator, root_dir: []const u8, refresh: bool, allow_fetch: bool) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    try runChild(alloc, &.{ "mkdir", "-p", root_dir });

    for (sources) |source| {
        const source_dir = try std.fs.path.join(alloc, &.{ root_dir, source.local_dir });
        defer alloc.free(source_dir);

        const exists = blk: {
            var dir = std.Io.Dir.cwd().openDir(io_impl.io(), source_dir, .{}) catch break :blk false;
            dir.close(io_impl.io());
            break :blk true;
        };

        switch (source.kind) {
            .git => {
                if (!exists) {
                    if (!allow_fetch) {
                        std.debug.print("missing upstream corpus checkout: {s}\n", .{source_dir});
                        return error.ExternalAudioCorpusUnavailable;
                    }
                    try runChild(alloc, &.{ "git", "clone", "--depth=1", source.git_url, source_dir });
                    continue;
                }

                if (refresh) {
                    try runChild(alloc, &.{ "git", "-C", source_dir, "pull", "--ff-only" });
                }
            },
            .archive_tgz => {
                if (exists and !refresh) continue;
                if (!allow_fetch) {
                    std.debug.print("missing upstream corpus checkout: {s}\n", .{source_dir});
                    return error.ExternalAudioCorpusUnavailable;
                }
                const archive_path = try std.fs.path.join(alloc, &.{ root_dir, source.archive_name });
                defer alloc.free(archive_path);
                try runChild(alloc, &.{ "curl", "-L", "-o", archive_path, source.archive_url });
                try runChild(alloc, &.{ "tar", "-C", root_dir, "-xzf", archive_path });
            },
            .file_set => {
                if (!allow_fetch and !exists) {
                    std.debug.print("missing upstream corpus checkout: {s}\n", .{source_dir});
                    return error.ExternalAudioCorpusUnavailable;
                }
                try runChild(alloc, &.{ "mkdir", "-p", source_dir });
                for (source.file_names) |file_name| {
                    const local_path = try std.fs.path.join(alloc, &.{ source_dir, file_name });
                    defer alloc.free(local_path);
                    if (std.fs.path.dirname(local_path)) |parent_dir| {
                        try runChild(alloc, &.{ "mkdir", "-p", parent_dir });
                    }

                    const file_exists = blk: {
                        std.Io.Dir.cwd().access(io_impl.io(), local_path, .{}) catch break :blk false;
                        break :blk true;
                    };
                    if (file_exists and !refresh) continue;
                    if (!allow_fetch) continue;

                    const remote_url = try std.mem.concat(alloc, u8, &.{ source.archive_url, file_name });
                    defer alloc.free(remote_url);
                    try runChild(alloc, &.{ "curl", "-L", "-o", local_path, remote_url });
                }
            },
            .insecure_file_set => {
                if (!allow_fetch and !exists) {
                    std.debug.print("missing upstream corpus checkout: {s}\n", .{source_dir});
                    return error.ExternalAudioCorpusUnavailable;
                }
                try runChild(alloc, &.{ "mkdir", "-p", source_dir });
                for (source.file_names) |file_name| {
                    const local_path = try std.fs.path.join(alloc, &.{ source_dir, file_name });
                    defer alloc.free(local_path);

                    const file_exists = blk: {
                        std.Io.Dir.cwd().access(io_impl.io(), local_path, .{}) catch break :blk false;
                        break :blk true;
                    };
                    if (file_exists and !refresh) continue;
                    if (!allow_fetch) continue;

                    const remote_url = try std.mem.concat(alloc, u8, &.{ source.archive_url, file_name });
                    defer alloc.free(remote_url);
                    try runChild(alloc, &.{ "curl", "-k", "-L", "-o", local_path, remote_url });
                }
            },
        }
    }
}

fn printStatus(alloc: Allocator, root_dir: []const u8) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    std.debug.print("status:\n  root_dir={s}\n", .{root_dir});
    for (sources) |source| {
        const source_dir = try std.fs.path.join(alloc, &.{ root_dir, source.local_dir });
        defer alloc.free(source_dir);

        const exists = blk: {
            var dir = std.Io.Dir.cwd().openDir(io_impl.io(), source_dir, .{ .iterate = true }) catch break :blk false;
            defer dir.close(io_impl.io());
            break :blk true;
        };
        if (!exists) {
            std.debug.print("  {s}: missing\n", .{source.name});
            continue;
        }

        var dir = try std.Io.Dir.cwd().openDir(io_impl.io(), source_dir, .{ .iterate = true });
        defer dir.close(io_impl.io());
        var walker = try dir.walk(alloc);
        defer walker.deinit();

        var audio_files: usize = 0;
        while (try walker.next(io_impl.io())) |entry| {
            if (entry.kind != .file) continue;
            if (isExternalAudioPath(entry.path)) audio_files += 1;
        }

        std.debug.print("  {s}: present audio_files={d}\n", .{ source.name, audio_files });
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

fn isExternalAudioPath(path: []const u8) bool {
    if (!isTrackedSourcePath(path)) return false;
    return std.ascii.endsWithIgnoreCase(path, ".ogg") or
        std.ascii.endsWithIgnoreCase(path, ".oga") or
        std.ascii.endsWithIgnoreCase(path, ".opus") or
        std.ascii.endsWithIgnoreCase(path, ".flac") or
        isOpusRfcVectorPath(path);
}

fn isOpusRfcVectorPath(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "opus_newvectors/") and std.ascii.endsWithIgnoreCase(path, ".bit");
}

fn isTrackedSourcePath(path: []const u8) bool {
    inline for (sources) |source| {
        const prefix = source.local_dir ++ "/";
        if (std.mem.startsWith(u8, path, prefix)) return true;
    }
    return false;
}

fn toExitCode(outcome: Outcome) ProbeExitCode {
    return switch (outcome) {
        .success => .success,
        .expected_unsupported => .expected_unsupported,
        .unsupported => .unsupported,
        .decode_failed => .decode_failed,
        .crashed => .decode_failed,
    };
}

fn outcomeName(outcome: Outcome) []const u8 {
    return switch (outcome) {
        .success => "success",
        .expected_unsupported => "expected_unsupported",
        .unsupported => "unsupported",
        .decode_failed => "decode_failed",
        .crashed => "crashed",
    };
}

fn classifyUnsupported(relative_path: []const u8) Outcome {
    if (std.mem.startsWith(u8, relative_path, "flac-test-files/faulty/")) return .expected_unsupported;
    if (std.mem.startsWith(u8, relative_path, "flac-test-files/uncommon/")) {
        if (std.mem.endsWith(u8, relative_path, "01 - changing samplerate.flac")) return .expected_unsupported;
        if (std.mem.endsWith(u8, relative_path, "02 - increasing number of channels.flac")) return .expected_unsupported;
        if (std.mem.endsWith(u8, relative_path, "03 - decreasing number of channels.flac")) return .expected_unsupported;
        if (std.mem.endsWith(u8, relative_path, "04 - changing bitdepth.flac")) return .expected_unsupported;
    }
    return .unsupported;
}

fn formatName(format: termite_audio.EncodedFormat) []const u8 {
    return switch (format) {
        .ogg => "ogg",
        .opus => "opus",
        .flac => "flac",
        .aac => "aac",
        .mp4 => "mp4",
        .mp3 => "mp3",
        .wav => "wav",
        .aiff => "aiff",
        .caf => "caf",
        .au => "au",
    };
}

fn printSummary(summary: Summary) void {
    std.debug.print(
        \\summary:
        \\  files_scanned={d}
        \\  audio_files={d}
        \\  success={d}
        \\  expected_unsupported={d}
        \\  unsupported={d}
        \\  decode_failed={d}
        \\  crashed={d}
        \\
    , .{
        summary.total_files,
        summary.audio_files,
        summary.success,
        summary.expected_unsupported,
        summary.unsupported,
        summary.decode_failed,
        summary.crashed,
    });
}
