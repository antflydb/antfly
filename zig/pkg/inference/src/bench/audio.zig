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
const inference_audio = @import("inference_audio");
const mp3_synth = inference_audio.mp3.synthesis;
const aac = inference_audio.aac;

const mp3_fixture_path = "lib/audio/testdata/tone.mp3";
const vorbis_fixture_path = "lib/audio/testdata/codec-corpus/tone-stereo.ogg";
const opus_fixture_path = "lib/audio/testdata/codec-corpus/tone-stereo.opus";
const aac_adts_fixture_path = "lib/audio/testdata/codec-corpus/tone-stereo.aac";
const aac_fixture_path = "lib/audio/testdata/codec-corpus/tone-stereo.m4a";

const BenchKind = enum {
    all,
    mp3_decode,
    vorbis_decode,
    opus_decode,
    aac_adts_decode,
    aac_decode,
    mp3_synth,
};

const BenchConfig = struct {
    bench: BenchKind = .all,
    warmup_iters: usize = 2,
    measure_iters: usize = 10,
};

const BenchResult = struct {
    name: []const u8,
    iterations: usize,
    total_ns: u64,
    bytes_per_iter: usize,
    aac_perf: ?aac.PerfCounters = null,

    fn nsPerIter(self: BenchResult) f64 {
        return @as(f64, @floatFromInt(self.total_ns)) / @as(f64, @floatFromInt(self.iterations));
    }

    fn mibPerSec(self: BenchResult) f64 {
        const total_bytes = @as(f64, @floatFromInt(self.bytes_per_iter * self.iterations));
        const mib = total_bytes / (1024.0 * 1024.0);
        const seconds = @as(f64, @floatFromInt(self.total_ns)) / @as(f64, std.time.ns_per_s);
        return if (seconds == 0.0) 0.0 else mib / seconds;
    }
};

pub fn main(init: std.process.Init) !void {
    const cfg = try parseArgs(init);
    const allocator = std.heap.page_allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();

    var results = std.ArrayList(BenchResult).empty;
    defer results.deinit(allocator);

    if (cfg.bench == .all or cfg.bench == .mp3_decode) {
        const fixture = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), mp3_fixture_path, allocator, .limited(16 * 1024 * 1024));
        defer allocator.free(fixture);
        try results.append(allocator, try benchDecode(allocator, "mp3_decode", fixture, cfg));
    }
    if (cfg.bench == .all or cfg.bench == .vorbis_decode) {
        const fixture = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), vorbis_fixture_path, allocator, .limited(16 * 1024 * 1024));
        defer allocator.free(fixture);
        try results.append(allocator, try benchDecode(allocator, "vorbis_decode", fixture, cfg));
    }
    if (cfg.bench == .all or cfg.bench == .opus_decode) {
        const fixture = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), opus_fixture_path, allocator, .limited(16 * 1024 * 1024));
        defer allocator.free(fixture);
        try results.append(allocator, try benchDecode(allocator, "opus_decode", fixture, cfg));
    }
    if (cfg.bench == .all or cfg.bench == .aac_adts_decode) {
        const fixture = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), aac_adts_fixture_path, allocator, .limited(16 * 1024 * 1024));
        defer allocator.free(fixture);
        try results.append(allocator, try benchDecode(allocator, "aac_adts_decode", fixture, cfg));
    }
    if (cfg.bench == .all or cfg.bench == .aac_decode) {
        const fixture = try std.Io.Dir.cwd().readFileAlloc(io_impl.io(), aac_fixture_path, allocator, .limited(16 * 1024 * 1024));
        defer allocator.free(fixture);
        try results.append(allocator, try benchDecode(allocator, "aac_decode", fixture, cfg));
    }
    if (cfg.bench == .all or cfg.bench == .mp3_synth) {
        try results.append(allocator, try benchMp3Synthesis(cfg));
    }

    for (results.items) |result| {
        std.debug.print(
            "{s}\titerations={d}\ttotal_ms={d:.3}\tns_per_iter={d:.0}\tMiB_per_s={d:.2}\n",
            .{
                result.name,
                result.iterations,
                nsToMs(result.total_ns),
                result.nsPerIter(),
                result.mibPerSec(),
            },
        );
        if (result.aac_perf) |perf| {
            printAacPerf(result.iterations, perf);
        }
    }
}

fn parseArgs(init: std.process.Init) !BenchConfig {
    var cfg = BenchConfig{};
    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args_buf: [32][]const u8 = undefined;
    var args_len: usize = 0;
    while (args_iter.next()) |arg| {
        if (args_len < args_buf.len) {
            args_buf[args_len] = arg;
            args_len += 1;
        }
    }
    const args = args_buf[0..args_len];

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        const arg = args[i];
        if (std.mem.eql(u8, arg, "--bench")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.bench = try parseBenchKind(args[i]);
        } else if (std.mem.eql(u8, arg, "--warmup-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.warmup_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--measure-iters")) {
            i += 1;
            if (i >= args.len) return error.MissingValue;
            cfg.measure_iters = try std.fmt.parseInt(usize, args[i], 10);
        } else if (std.mem.eql(u8, arg, "--help")) {
            try printUsage();
            std.process.exit(0);
        } else {
            return error.UnknownArgument;
        }
    }

    if (cfg.measure_iters == 0) return error.InvalidArguments;
    return cfg;
}

fn parseBenchKind(arg: []const u8) !BenchKind {
    inline for (std.meta.fields(BenchKind)) |field| {
        if (std.mem.eql(u8, arg, field.name)) return @field(BenchKind, field.name);
    }
    return error.InvalidBenchKind;
}

fn printUsage() !void {
    std.debug.print(
        \\Usage: termite-audio-bench [options]
        \\  --bench all|mp3_decode|vorbis_decode|opus_decode|aac_adts_decode|aac_decode|mp3_synth
        \\  --warmup-iters N
        \\  --measure-iters N
        \\
    , .{});
}

fn benchDecode(
    allocator: std.mem.Allocator,
    name: []const u8,
    fixture: []const u8,
    cfg: BenchConfig,
) !BenchResult {
    const profile_aac = std.mem.eql(u8, name, "aac_decode") or std.mem.eql(u8, name, "aac_adts_decode");
    var warmup: usize = 0;
    while (warmup < cfg.warmup_iters) : (warmup += 1) {
        const decoded = try inference_audio.decodeInterleaved(allocator, fixture, .{});
        allocator.free(decoded.samples);
    }

    if (profile_aac) aac.resetPerfCounters();
    const started_ns = try monotonicNowNs();
    var iter: usize = 0;
    while (iter < cfg.measure_iters) : (iter += 1) {
        const decoded = try inference_audio.decodeInterleaved(allocator, fixture, .{});
        allocator.free(decoded.samples);
    }
    const maybe_perf = if (profile_aac) blk: {
        defer aac.disablePerfCounters();
        break :blk aac.snapshotPerfCounters();
    } else null;

    return .{
        .name = name,
        .iterations = cfg.measure_iters,
        .total_ns = try elapsedNsSince(started_ns),
        .bytes_per_iter = fixture.len,
        .aac_perf = maybe_perf,
    };
}

fn printAacPerf(iterations: usize, perf: aac.PerfCounters) void {
    const denom = @as(f64, @floatFromInt(@max(iterations, 1)));
    std.debug.print(
        "aac_perf\tconfig_ms={d:.3}\tcollect_ms={d:.3}\tenhance_ms={d:.3}\tstate_init_ms={d:.3}\tcoeff_offsets_ms={d:.3}\tcoeff_copy_ms={d:.3}\tspectral_parse_ms={d:.3}\tspectral_decode_ms={d:.3}\ttools_ms={d:.3}\ttrailing_ms={d:.3}\tpcm_block_ms={d:.3}\tsequence_unit_ms={d:.3}\tsequence_output_ms={d:.3}\tfilterbank_sequence_ms={d:.3}\tfilterbank_ms={d:.3}\timdct_ms={d:.3}\twindow_ms={d:.3}\toverlap_ms={d:.3}\tpost_ms={d:.3}\taccess_units={d}\tchannel_decodes={d}\n",
        .{
            @as(f64, @floatFromInt(perf.config_parse_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.access_unit_collect_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.enhancement_info_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.spectral_state_init_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.coeff_offsets_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.coeff_copy_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.spectral_parse_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.spectral_decode_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.tns_tools_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.trailing_validate_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.pcm_block_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.sequence_unit_decode_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.sequence_output_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.filterbank_sequence_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.filterbank_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.filterbank_imdct_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.filterbank_window_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.filterbank_overlap_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            @as(f64, @floatFromInt(perf.postprocess_ns)) / @as(f64, std.time.ns_per_ms) / denom,
            perf.access_unit_count / @max(iterations, 1),
            perf.channel_decode_count / @max(iterations, 1),
        },
    );
}

fn benchMp3Synthesis(cfg: BenchConfig) !BenchResult {
    var qmf_state = mp3_synth.QmfState{};
    var left_subband: [32 * 18]f32 = undefined;
    var right_subband: [32 * 18]f32 = undefined;
    var pcm_out: [32 * 18 * 2]f32 = undefined;

    fillSynthInput(left_subband[0..], 0.03125);
    fillSynthInput(right_subband[0..], 0.046875);

    var warmup: usize = 0;
    while (warmup < cfg.warmup_iters) : (warmup += 1) {
        try mp3_synth.synthesizeFrameStereo(&qmf_state, left_subband[0..], right_subband[0..], pcm_out[0..]);
    }

    qmf_state = .{};
    const started_ns = try monotonicNowNs();
    var iter: usize = 0;
    while (iter < cfg.measure_iters) : (iter += 1) {
        try mp3_synth.synthesizeFrameStereo(&qmf_state, left_subband[0..], right_subband[0..], pcm_out[0..]);
    }

    return .{
        .name = "mp3_synth",
        .iterations = cfg.measure_iters,
        .total_ns = try elapsedNsSince(started_ns),
        .bytes_per_iter = (@sizeOf(f32) * left_subband.len * 2) + (@sizeOf(f32) * pcm_out.len),
    };
}

fn fillSynthInput(samples: []f32, seed_scale: f32) void {
    for (samples, 0..) |*sample, index| {
        const phase = @as(f32, @floatFromInt((index % 37) + 1));
        sample.* = @sin(phase * seed_scale);
    }
}

fn nsToMs(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / @as(f64, std.time.ns_per_ms);
}

fn elapsedNsSince(started_ns: u64) !u64 {
    const now = try monotonicNowNs();
    return now - started_ns;
}

fn monotonicNowNs() !u64 {
    var timespec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(std.posix.CLOCK.MONOTONIC, &timespec))) {
        .SUCCESS => return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec),
        else => return error.ClockGetTimeFailed,
    }
}
