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
const inference_audio = @import("src/mod.zig");

const Allocator = std.mem.Allocator;

const default_root_dir = "/tmp/termite-audio-misc-corpora";
const max_fixture_bytes = 128 * 1024 * 1024;

const Source = struct {
    name: []const u8,
    local_dir: []const u8,
    archive_url: []const u8,
    file_names: []const []const u8,
};

const minimp3_vector_files = [_][]const u8{
    "l3-compl.bit",
    "l3-compl.pcm",
    "l3-he_free.bit",
    "l3-he_free.pcm",
    "l3-he_mode.bit",
    "l3-he_mode.pcm",
    "l3-si.bit",
    "l3-si.pcm",
    "l3-si_huff.bit",
    "l3-si_huff.pcm",
    "l3-sin1k0db.bit",
    "l3-sin1k0db.pcm",
};

const hotmart_audio_sample_files = [_][]const u8{
    "sample.m4a",
    "sample.mp4",
    "db539f71-b296-4a4a-922d-f207f54154e0.m4a",
    "e2a2cafc-bc54-498b-b5a8-3059f94dbdee.aac",
};

const projectivetech_audio_sample_files = [_][]const u8{
    "sample.aac",
    "sample.m4a",
};

const sources = [_]Source{
    .{
        .name = "minimp3-vectors",
        .local_dir = "minimp3_vectors",
        .archive_url = "https://raw.githubusercontent.com/lieff/minimp3/master/vectors/",
        .file_names = &minimp3_vector_files,
    },
    .{
        .name = "hotmart-audio-samples",
        .local_dir = "hotmart_audio_samples",
        .archive_url = "https://github.com/rafaelreis-hotmart/Audio-Sample-files/raw/master/",
        .file_names = &hotmart_audio_sample_files,
    },
    .{
        .name = "projectivetech-audio-samples",
        .local_dir = "projectivetech_audio_samples",
        .archive_url = "https://raw.githubusercontent.com/projectivetech/media-samples/master/",
        .file_names = &projectivetech_audio_sample_files,
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

const Minimp3VectorCase = struct {
    name: []const u8,
    sample_rate: u32,
    channels: u8,
};

const Mp3FrameShape = struct {
    samples_per_frame: usize,
    channels: u8,
};

const minimp3_vector_cases = [_]Minimp3VectorCase{
    .{ .name = "l3-compl.bit", .sample_rate = 48_000, .channels = 1 },
    .{ .name = "l3-he_free.bit", .sample_rate = 44_100, .channels = 2 },
    .{ .name = "l3-he_mode.bit", .sample_rate = 44_100, .channels = 2 },
    .{ .name = "l3-si.bit", .sample_rate = 44_100, .channels = 1 },
    .{ .name = "l3-si_huff.bit", .sample_rate = 44_100, .channels = 1 },
    .{ .name = "l3-sin1k0db.bit", .sample_rate = 44_100, .channels = 2 },
};

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const alloc = gpa.allocator();

    var args = try std.process.Args.Iterator.initAllocator(init.minimal.args, alloc);
    defer args.deinit();

    const argv0 = args.next() orelse "audio_misc_corpora_e2e";
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
        std.debug.print("audio misc corpora ready: {s}\n", .{root_dir});
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
        \\The external misc audio lane fetches public MP3 and AAC/MP4 samples and
        \\probes the pure-Zig decode path without vendoring the media into the repo.
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
    if (isMinimp3VectorPath(relative_path)) {
        return try probeOneMinimp3Vector(alloc, root_dir, relative_path, print_success);
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const full_path = try std.fs.path.join(alloc, &.{ root_dir, relative_path });
    defer alloc.free(full_path);

    const bytes = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), full_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(bytes);

    const format = inference_audio.detectFormatFromFilename(relative_path) orelse
        inference_audio.detectFormat(bytes) orelse
        return classifyUnsupported(relative_path);
    switch (format) {
        .mp3, .aac, .mp4 => {},
        else => return classifyUnsupported(relative_path),
    }

    var interleaved = inference_audio.decodeInterleaved(alloc, bytes, .{
        .file_name_hint = relative_path,
    }) catch |err| switch (err) {
        error.UnsupportedAudioFormat => {
            if (format == .mp4) {
                const demuxed = inference_audio.mp4.demux(alloc, bytes) catch |demux_err| {
                    std.debug.print("MP4_DEMUX_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(demux_err) });
                    std.debug.print("UNSUPPORTED\t{s}\terr={s}\n", .{ relative_path, @errorName(err) });
                    return classifyUnsupported(relative_path);
                };
                defer demuxed.deinit();
                std.debug.print(
                    "MP4_DEMUXED\t{s}\tcodec={s}\trate={d}\tchannels={d}\tconfig_bytes={d}\taccess_units={d}\n",
                    .{
                        relative_path,
                        switch (demuxed.codec) {
                            .aac => "aac",
                            .alac => "alac",
                        },
                        demuxed.sample_rate,
                        demuxed.channels,
                        demuxed.decoder_config.len,
                        demuxed.access_units.len,
                    },
                );
                if (demuxed.codec == .aac) {
                    if (inference_audio.aac.parseAudioSpecificConfig(demuxed.decoder_config)) |config| {
                        const config_channels = config.explicit_channel_count orelse config.channel_config;
                        std.debug.print(
                            "AAC_CONFIG\t{s}\tobject_type={d}\tsample_rate={d}\tchannel_config={d}\tframe_length_960={any}\tsbr={any}\tps={any}\n",
                            .{
                                relative_path,
                                config.object_type,
                                config.sample_rate,
                                config_channels,
                                config.frame_length_960,
                                config.sbr_present,
                                config.ps_present,
                            },
                        );
                        if (demuxed.access_units.len > 0) {
                            if (inference_audio.aac.summarizeAccessUnit(demuxed.access_units[0])) |summary| {
                                std.debug.print(
                                    "AAC_FIRST_AU\t{s}\tfirst_element={s}\ttag={d}\tcommon_window={any}\n",
                                    .{
                                        relative_path,
                                        @tagName(summary.first_element),
                                        summary.element_instance_tag,
                                        summary.common_window,
                                    },
                                );
                            } else |summary_err| {
                                std.debug.print("AAC_FIRST_AU_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(summary_err) });
                            }
                            _ = inference_audio.aac.dequantizeFirstChannelSpectralCoefficientsAlloc(
                                alloc,
                                demuxed.sample_rate,
                                demuxed.access_units[0],
                            ) catch |dequant_err| {
                                std.debug.print("AAC_FIRST_AU_DEQUANT_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(dequant_err) });
                            };
                            _ = inference_audio.aac.decodeFirstChannelPcmBlockAlloc(
                                alloc,
                                demuxed.sample_rate,
                                null,
                                demuxed.access_units[0],
                            ) catch |pcm_err| {
                                std.debug.print("AAC_FIRST_AU_PCM_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(pcm_err) });
                            };
                        }
                        const effective_channels: u16 = if (config_channels == 1 or config_channels == 2) config_channels else demuxed.channels;
                        if (effective_channels == 1) {
                            var tail: ?[]f32 = null;
                            defer if (tail) |owned_tail| alloc.free(owned_tail);
                            for (demuxed.access_units, 0..) |unit, index| {
                                const block = inference_audio.aac.decodeFirstChannelPcmBlockAlloc(
                                    alloc,
                                    demuxed.sample_rate,
                                    tail,
                                    unit,
                                ) catch |block_err| {
                                    std.debug.print("AAC_MONO_AU_FAILED\t{s}\tindex={d}\terr={s}\n", .{ relative_path, index, @errorName(block_err) });
                                    break;
                                };
                                if (tail) |owned_tail| alloc.free(owned_tail);
                                tail = block.tail;
                                alloc.free(block.pcm);
                            }
                            _ = inference_audio.aac.decodeFirstChannelPcmSequenceAlloc(
                                alloc,
                                demuxed.sample_rate,
                                demuxed.access_units,
                            ) catch |seq_err| {
                                std.debug.print("AAC_RAW_MONO_SEQUENCE_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(seq_err) });
                            };
                        }
                        _ = inference_audio.aac.decodeInterleavedMonoAccessUnitsAlloc(
                            alloc,
                            demuxed.sample_rate,
                            effective_channels,
                            demuxed.decoder_config,
                            demuxed.access_units,
                        ) catch |mono_err| {
                            std.debug.print("AAC_MONO_UNSUPPORTED\t{s}\terr={s}\n", .{ relative_path, @errorName(mono_err) });
                        };
                        _ = inference_audio.aac.decodeInterleavedStereoAccessUnitsAlloc(
                            alloc,
                            demuxed.sample_rate,
                            effective_channels,
                            demuxed.decoder_config,
                            demuxed.access_units,
                        ) catch |stereo_err| {
                            std.debug.print("AAC_STEREO_UNSUPPORTED\t{s}\terr={s}\n", .{ relative_path, @errorName(stereo_err) });
                        };
                    } else |config_err| {
                        std.debug.print("AAC_CONFIG_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(config_err) });
                    }
                }
            }
            std.debug.print("UNSUPPORTED\t{s}\terr={s}\n", .{ relative_path, @errorName(err) });
            return classifyUnsupported(relative_path);
        },
        else => {
            std.debug.print("DECODE_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(err) });
            return .decode_failed;
        },
    };
    defer interleaved.deinit();
    if (interleaved.sample_rate == 0 or interleaved.channels == 0 or interleaved.samples.len == 0) return .decode_failed;
    if (!allFinite(interleaved.samples)) return .decode_failed;

    var mono = inference_audio.decode(alloc, bytes, .{
        .file_name_hint = relative_path,
    }) catch |err| switch (err) {
        error.UnsupportedAudioFormat => {
            std.debug.print("UNSUPPORTED\t{s}\terr={s}\n", .{ relative_path, @errorName(err) });
            return classifyUnsupported(relative_path);
        },
        else => {
            std.debug.print("DECODE_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(err) });
            return .decode_failed;
        },
    };
    defer mono.deinit();
    if (mono.sample_rate == 0 or mono.samples.len == 0) return .decode_failed;
    if (!allFinite(mono.samples)) return .decode_failed;

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

fn probeOneMinimp3Vector(alloc: Allocator, root_dir: []const u8, relative_path: []const u8, print_success: bool) !Outcome {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    const bitstream_path = try std.fs.path.join(alloc, &.{ root_dir, relative_path });
    defer alloc.free(bitstream_path);
    const bitstream = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), bitstream_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(bitstream);

    const pcm_rel = try replaceSuffixAlloc(alloc, relative_path, ".bit", ".pcm");
    defer alloc.free(pcm_rel);
    const pcm_path = try std.fs.path.join(alloc, &.{ root_dir, pcm_rel });
    defer alloc.free(pcm_path);
    const pcm_bytes = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), pcm_path, alloc, .limited(max_fixture_bytes));
    defer alloc.free(pcm_bytes);

    const expected_case = minimp3ExpectedCase(relative_path) orelse return .decode_failed;

    const decoded = inference_audio.mp3.decodeInterleaved(alloc, bitstream) catch |err| switch (err) {
        error.Mp3PureZigUnimplemented => return .unsupported,
        else => {
            std.debug.print("DECODE_FAILED\t{s}\terr={s}\n", .{ relative_path, @errorName(err) });
            return .decode_failed;
        },
    };
    defer alloc.free(decoded.samples);

    if (decoded.sample_rate != expected_case.sample_rate) {
        std.debug.print(
            "DECODE_FAILED\t{s}\terr=SampleRateMismatch\texpected={d}\tactual={d}\n",
            .{ relative_path, expected_case.sample_rate, decoded.sample_rate },
        );
        return .decode_failed;
    }
    if (decoded.channels != expected_case.channels) {
        std.debug.print(
            "DECODE_FAILED\t{s}\terr=ChannelMismatch\texpected={d}\tactual={d}\n",
            .{ relative_path, expected_case.channels, decoded.channels },
        );
        return .decode_failed;
    }
    if (decoded.samples.len == 0) return .decode_failed;
    if (!allFinite(decoded.samples)) return .decode_failed;

    const raw_reference = try pcm16LeToFloatAlloc(alloc, pcm_bytes);
    defer alloc.free(raw_reference);
    if (raw_reference.len == 0) return .decode_failed;

    const frame_shapes = try collectMp3FrameShapesAlloc(alloc, bitstream);
    defer alloc.free(frame_shapes);
    const decoded_for_compare = try collapseMp3DecodedAlloc(alloc, decoded.samples, frame_shapes, decoded.channels);
    defer alloc.free(decoded_for_compare);

    const metrics = inference_audio.conformance.bestAlignmentMetrics(raw_reference, decoded_for_compare, 8192);
    if (metrics.compared < 8000) {
        std.debug.print(
            "DECODE_FAILED\t{s}\terr=ComparedTooShort\tref_samples={d}\tdecoded_samples={d}\tcompared={d}\n",
            .{ relative_path, raw_reference.len, decoded_for_compare.len, metrics.compared },
        );
        return .decode_failed;
    }
    if (metrics.correlation < 0.90) {
        const negated = try transformedSamplesAlloc(alloc, decoded_for_compare, .negate_all);
        defer alloc.free(negated);
        const alternating = try transformedSamplesAlloc(alloc, decoded_for_compare, .negate_every_other_sample);
        defer alloc.free(alternating);
        const block32 = try transformedSamplesAlloc(alloc, decoded_for_compare, .negate_every_32_samples);
        defer alloc.free(block32);
        const block64 = try transformedSamplesAlloc(alloc, decoded_for_compare, .negate_every_64_samples);
        defer alloc.free(block64);
        const swapped = if (expected_case.channels == 2)
            try swapStereoPairsAlloc(alloc, decoded_for_compare)
        else
            try alloc.dupe(f32, decoded_for_compare);
        defer alloc.free(swapped);
        const frame_sample_count = @as(usize, expected_case.channels) * 1152;
        const trimmed_front = if (decoded_for_compare.len > frame_sample_count)
            decoded_for_compare[frame_sample_count..]
        else
            decoded_for_compare[0..0];
        const trimmed_back = if (decoded_for_compare.len > frame_sample_count)
            decoded_for_compare[0 .. decoded_for_compare.len - frame_sample_count]
        else
            decoded_for_compare[0..0];

        const neg_metrics = inference_audio.conformance.bestAlignmentMetrics(raw_reference, negated, 8192);
        const alternating_metrics = inference_audio.conformance.bestAlignmentMetrics(raw_reference, alternating, 8192);
        const block32_metrics = inference_audio.conformance.bestAlignmentMetrics(raw_reference, block32, 8192);
        const block64_metrics = inference_audio.conformance.bestAlignmentMetrics(raw_reference, block64, 8192);
        const swapped_metrics = inference_audio.conformance.bestAlignmentMetrics(raw_reference, swapped, 8192);
        const trimmed_front_metrics = if (trimmed_front.len >= 8000)
            inference_audio.conformance.bestAlignmentMetrics(raw_reference, trimmed_front, 8192)
        else
            inference_audio.conformance.AlignmentMetrics{
                .offset = 0,
                .compared = 0,
                .correlation = -1.0,
                .mean_abs_error = std.math.inf(f32),
            };
        const trimmed_back_metrics = if (trimmed_back.len >= 8000)
            inference_audio.conformance.bestAlignmentMetrics(raw_reference, trimmed_back, 8192)
        else
            inference_audio.conformance.AlignmentMetrics{
                .offset = 0,
                .compared = 0,
                .correlation = -1.0,
                .mean_abs_error = std.math.inf(f32),
            };
        std.debug.print(
            "DECODE_FAILED\t{s}\terr=CorrelationTooLow\tref_samples={d}\tdecoded_samples={d}\toffset={d}\tcorr={d:.4}\tmae={d:.5}\n",
            .{ relative_path, raw_reference.len, decoded_for_compare.len, metrics.offset, metrics.correlation, metrics.mean_abs_error },
        );
        std.debug.print(
            "MP3_VARIANTS\t{s}\tneg_corr={d:.4}\talt_corr={d:.4}\tblock32_corr={d:.4}\tblock64_corr={d:.4}\tswap_corr={d:.4}\ttrim_front_corr={d:.4}\ttrim_back_corr={d:.4}\n",
            .{
                relative_path,
                neg_metrics.correlation,
                alternating_metrics.correlation,
                block32_metrics.correlation,
                block64_metrics.correlation,
                swapped_metrics.correlation,
                trimmed_front_metrics.correlation,
                trimmed_back_metrics.correlation,
            },
        );
        const preview_len = @min(@as(usize, 8), @min(raw_reference.len, decoded_for_compare.len));
        std.debug.print(
            "MP3_PREVIEW\t{s}\tref_energy={d:.6}\tdecoded_energy={d:.6}\tref=",
            .{ relative_path, sampleEnergy(raw_reference), sampleEnergy(decoded_for_compare) },
        );
        for (raw_reference[0..preview_len]) |sample| std.debug.print("{d:.5},", .{sample});
        std.debug.print("\tdecoded=", .{});
        for (decoded_for_compare[0..preview_len]) |sample| std.debug.print("{d:.5},", .{sample});
        std.debug.print("\n", .{});
        return .decode_failed;
    }
    if (metrics.mean_abs_error > 0.05) {
        std.debug.print(
            "DECODE_FAILED\t{s}\terr=MeanAbsErrorTooHigh\tref_samples={d}\tdecoded_samples={d}\toffset={d}\tcorr={d:.4}\tmae={d:.5}\n",
            .{ relative_path, raw_reference.len, decoded_for_compare.len, metrics.offset, metrics.correlation, metrics.mean_abs_error },
        );
        return .decode_failed;
    }

    if (print_success) {
        std.debug.print(
            "PASS\t{s}\tformat=mp3-vector\trate={d}\tchannels={d}\tframes={d}\toffset={d}\tcorr={d:.4}\tmae={d:.5}\n",
            .{
                relative_path,
                decoded.sample_rate,
                decoded.channels,
                @divFloor(decoded.samples.len, decoded.channels),
                metrics.offset,
                metrics.correlation,
                metrics.mean_abs_error,
            },
        );
    }
    return .success;
}

fn pcm16LeToFloatAlloc(alloc: Allocator, pcm_bytes: []const u8) ![]f32 {
    if (pcm_bytes.len % 2 != 0) return error.UnsupportedAudioFormat;
    const sample_count = pcm_bytes.len / 2;
    const samples = try alloc.alloc(f32, sample_count);
    errdefer alloc.free(samples);

    for (samples, 0..) |*sample, index| {
        const off = index * 2;
        const raw = @as(i16, @bitCast(@as(u16, pcm_bytes[off]) | (@as(u16, pcm_bytes[off + 1]) << 8)));
        sample.* = @as(f32, @floatFromInt(raw)) / 32768.0;
    }
    return samples;
}

fn collectMp3FrameShapesAlloc(alloc: Allocator, mp3_bytes: []const u8) ![]Mp3FrameShape {
    var it = inference_audio.mp3.bitstream.FrameIterator.init(mp3_bytes);
    var shapes = std.ArrayList(Mp3FrameShape).empty;
    errdefer shapes.deinit(alloc);

    while (true) {
        const frame = it.next() catch |err| switch (err) {
            error.Mp3TruncatedFrame => break,
            else => return err,
        } orelse break;
        try shapes.append(alloc, .{
            .samples_per_frame = frame.header.samplesPerFrame(),
            .channels = frame.header.channels(),
        });
    }

    return shapes.toOwnedSlice(alloc);
}

fn expandMp3ReferenceAlloc(
    alloc: Allocator,
    reference: []const f32,
    frame_shapes: []const Mp3FrameShape,
    output_channels: u8,
) ![]f32 {
    if (frame_shapes.len == 0) return alloc.dupe(f32, reference);

    var requires_expansion = false;
    var natural_sample_count: usize = 0;
    for (frame_shapes) |shape| {
        natural_sample_count += shape.samples_per_frame * shape.channels;
        if (shape.channels != output_channels) requires_expansion = true;
    }
    if (!requires_expansion) return alloc.dupe(f32, reference);
    if (natural_sample_count != reference.len) return error.UnsupportedAudioFormat;

    var expanded_count: usize = 0;
    for (frame_shapes) |shape| {
        expanded_count += shape.samples_per_frame * output_channels;
    }

    const expanded = try alloc.alloc(f32, expanded_count);
    errdefer alloc.free(expanded);

    var src_index: usize = 0;
    var dst_index: usize = 0;
    for (frame_shapes) |shape| {
        const frame_samples_per_channel = shape.samples_per_frame;
        if (shape.channels == output_channels) {
            const sample_count = frame_samples_per_channel * output_channels;
            @memcpy(expanded[dst_index .. dst_index + sample_count], reference[src_index .. src_index + sample_count]);
            src_index += sample_count;
            dst_index += sample_count;
            continue;
        }
        if (shape.channels != 1 or output_channels != 2) return error.UnsupportedAudioFormat;

        const mono = reference[src_index .. src_index + frame_samples_per_channel];
        for (mono) |sample| {
            expanded[dst_index] = sample;
            expanded[dst_index + 1] = sample;
            dst_index += 2;
        }
        src_index += frame_samples_per_channel;
    }

    return expanded;
}

fn collapseMp3DecodedAlloc(
    alloc: Allocator,
    decoded: []const f32,
    frame_shapes: []const Mp3FrameShape,
    decoded_channels: u8,
) ![]f32 {
    if (frame_shapes.len == 0 or decoded_channels == 0) return alloc.dupe(f32, decoded);

    var expanded_count: usize = 0;
    var natural_count: usize = 0;
    var requires_collapse = false;
    for (frame_shapes) |shape| {
        expanded_count += shape.samples_per_frame * decoded_channels;
        natural_count += shape.samples_per_frame * shape.channels;
        if (shape.channels != decoded_channels) requires_collapse = true;
    }
    if (!requires_collapse) return alloc.dupe(f32, decoded);
    if (expanded_count != decoded.len) return error.UnsupportedAudioFormat;

    const collapsed = try alloc.alloc(f32, natural_count);
    errdefer alloc.free(collapsed);

    var src_index: usize = 0;
    var dst_index: usize = 0;
    for (frame_shapes) |shape| {
        const frame_samples_per_channel = shape.samples_per_frame;
        if (shape.channels == decoded_channels) {
            const sample_count = frame_samples_per_channel * decoded_channels;
            @memcpy(collapsed[dst_index .. dst_index + sample_count], decoded[src_index .. src_index + sample_count]);
            src_index += sample_count;
            dst_index += sample_count;
            continue;
        }
        if (shape.channels != 1 or decoded_channels != 2) return error.UnsupportedAudioFormat;

        var frame_index: usize = 0;
        while (frame_index < frame_samples_per_channel) : (frame_index += 1) {
            collapsed[dst_index] = decoded[src_index];
            src_index += 2;
            dst_index += 1;
        }
    }

    return collapsed;
}

fn sampleEnergy(samples: []const f32) f64 {
    var energy: f64 = 0;
    for (samples) |sample| {
        energy += @as(f64, sample) * @as(f64, sample);
    }
    return energy;
}

const SampleTransform = enum {
    negate_all,
    negate_every_other_sample,
    negate_every_32_samples,
    negate_every_64_samples,
};

fn transformedSamplesAlloc(alloc: Allocator, samples: []const f32, transform: SampleTransform) ![]f32 {
    const out = try alloc.alloc(f32, samples.len);
    errdefer alloc.free(out);

    for (samples, 0..) |sample, index| {
        const negate = switch (transform) {
            .negate_all => true,
            .negate_every_other_sample => (index & 1) != 0,
            .negate_every_32_samples => ((index / 32) & 1) != 0,
            .negate_every_64_samples => ((index / 64) & 1) != 0,
        };
        out[index] = if (negate) -sample else sample;
    }
    return out;
}

fn swapStereoPairsAlloc(alloc: Allocator, samples: []const f32) ![]f32 {
    if ((samples.len & 1) != 0) return error.UnsupportedAudioFormat;
    const out = try alloc.alloc(f32, samples.len);
    errdefer alloc.free(out);

    var index: usize = 0;
    while (index < samples.len) : (index += 2) {
        out[index] = samples[index + 1];
        out[index + 1] = samples[index];
    }
    return out;
}

fn replaceSuffixAlloc(alloc: Allocator, path: []const u8, old_suffix: []const u8, new_suffix: []const u8) ![]u8 {
    if (!std.mem.endsWith(u8, path, old_suffix)) return error.UnsupportedAudioFormat;
    return std.mem.concat(alloc, u8, &.{ path[0 .. path.len - old_suffix.len], new_suffix });
}

fn minimp3ExpectedCase(relative_path: []const u8) ?Minimp3VectorCase {
    const file_name = std.fs.path.basename(relative_path);
    for (minimp3_vector_cases) |case| {
        if (std.mem.eql(u8, file_name, case.name)) return case;
    }
    return null;
}

fn ensureSourcesAvailable(alloc: Allocator, root_dir: []const u8, refresh: bool, allow_fetch: bool) !void {
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();

    try runChild(&.{ "mkdir", "-p", root_dir });

    for (sources) |source| {
        const source_dir = try std.fs.path.join(alloc, &.{ root_dir, source.local_dir });
        defer alloc.free(source_dir);

        if (!allow_fetch) {
            var dir = std.Io.Dir.cwd().openDir(io_impl.io(), source_dir, .{}) catch {
                std.debug.print("missing upstream corpus checkout: {s}\n", .{source_dir});
                return error.ExternalAudioCorpusUnavailable;
            };
            dir.close(io_impl.io());
        }

        try runChild(&.{ "mkdir", "-p", source_dir });
        for (source.file_names) |file_name| {
            const local_path = try std.fs.path.join(alloc, &.{ source_dir, file_name });
            defer alloc.free(local_path);
            if (std.fs.path.dirname(local_path)) |parent_dir| {
                try runChild(&.{ "mkdir", "-p", parent_dir });
            }

            const file_exists = blk: {
                std.Io.Dir.cwd().access(io_impl.io(), local_path, .{}) catch break :blk false;
                break :blk true;
            };
            if (file_exists and !refresh) continue;
            if (!allow_fetch) continue;

            const remote_url = try std.mem.concat(alloc, u8, &.{ source.archive_url, file_name });
            defer alloc.free(remote_url);
            try runChild(&.{ "curl", "-L", "-o", local_path, remote_url });
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
            const rooted_path = try std.fs.path.join(alloc, &.{ source.local_dir, entry.path });
            defer alloc.free(rooted_path);
            if (isExternalAudioPath(rooted_path)) audio_files += 1;
        }

        std.debug.print("  {s}: present audio_files={d}\n", .{ source.name, audio_files });
    }
}

fn runChild(argv: []const []const u8) !void {
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
    return std.ascii.endsWithIgnoreCase(path, ".mp3") or
        std.ascii.endsWithIgnoreCase(path, ".aac") or
        std.ascii.endsWithIgnoreCase(path, ".m4a") or
        std.ascii.endsWithIgnoreCase(path, ".mp4") or
        isMinimp3VectorPath(path);
}

fn isTrackedSourcePath(path: []const u8) bool {
    for (sources) |source| {
        for (source.file_names) |file_name| {
            if (path.len != source.local_dir.len + 1 + file_name.len) continue;
            if (!std.mem.startsWith(u8, path, source.local_dir)) continue;
            if (path[source.local_dir.len] != '/') continue;
            if (std.mem.eql(u8, path[source.local_dir.len + 1 ..], file_name)) return true;
        }
    }
    return false;
}

fn isMinimp3VectorPath(path: []const u8) bool {
    return std.mem.startsWith(u8, path, "minimp3_vectors/") and std.ascii.endsWithIgnoreCase(path, ".bit");
}

fn classifyUnsupported(relative_path: []const u8) Outcome {
    _ = relative_path;
    return .unsupported;
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

fn formatName(format: inference_audio.EncodedFormat) []const u8 {
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

fn allFinite(samples: []const f32) bool {
    for (samples) |sample| {
        if (!std.math.isFinite(sample)) return false;
    }
    return true;
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
