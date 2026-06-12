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
const open_corpus_cases = @import("open_corpus_cases.zig");
const inference_audio = @import("src/mod.zig");
const Io = std.Io;

const EncodedFormat = inference_audio.EncodedFormat;
const OpenCorpusCodecCase = open_corpus_cases.OpenCorpusCodecCase;

const Summary = struct {
    scanned_files: usize = 0,
    matched_files: usize = 0,
    skipped_files: usize = 0,
    decode_successes: usize = 0,
    failures: usize = 0,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.arena.allocator();

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = Io.File.stdout().writer(io, &stdout_buffer);
    const stdout = &stdout_writer.interface;

    var stderr_buffer: [4096]u8 = undefined;
    var stderr_writer = Io.File.stderr().writer(io, &stderr_buffer);
    const stderr = &stderr_writer.interface;

    const args = try init.minimal.args.toSlice(allocator);
    const root_arg = if (args.len >= 2) args[1] else {
        try printUsage(stderr);
        try stderr.flush();
        return error.InvalidArguments;
    };

    const cwd = Io.Dir.cwd();
    var root_dir = try cwd.openDir(io, root_arg, .{ .iterate = true });
    defer root_dir.close(io);

    var walker = try root_dir.walk(allocator);
    defer walker.deinit();

    var summary = Summary{};
    while (try walker.next(io)) |entry| {
        if (entry.kind != .file) continue;
        summary.scanned_files += 1;

        if (std.mem.eql(u8, entry.basename, "README.md")) {
            summary.skipped_files += 1;
            continue;
        }

        const rel_path = try std.fs.path.join(allocator, &.{ root_arg, entry.path });
        defer allocator.free(rel_path);

        const bytes = try cwd.readFileAlloc(io, rel_path, allocator, .limited(128 * 1024 * 1024));
        defer allocator.free(bytes);

        const case = findCheckedInCase(entry.basename) orelse {
            summary.skipped_files += 1;
            try stdout.print("SKIP  {s}  unknown-open-corpus-case\n", .{entry.path});
            continue;
        };

        summary.matched_files += 1;
        verifyCheckedInCase(allocator, case, bytes) catch |err| {
            summary.failures += 1;
            try stderr.print("FAIL  {s}  {s}\n", .{ entry.path, @errorName(err) });
            continue;
        };
        summary.decode_successes += 1;

        try stdout.print(
            "PASS  {s}  {s}\n",
            .{ entry.path, formatName(case.format) },
        );
    }

    try stdout.print(
        "SUMMARY scanned={d} matched={d} skipped={d} decoded={d} failures={d}\n",
        .{
            summary.scanned_files,
            summary.matched_files,
            summary.skipped_files,
            summary.decode_successes,
            summary.failures,
        },
    );
    try stdout.flush();
    try stderr.flush();

    if (summary.failures != 0) return error.OpenCorpusFailed;
}

fn printUsage(stderr: anytype) !void {
    try stderr.writeAll(
        "usage: zig run lib/audio/open_corpus_root.zig -- <dir>\n" ++
            "scans a directory recursively and validates files represented in the shared\n" ++
            "non-MP3 open-corpus table against the pure-Zig audio path and the owned checked-in reference case.\n",
    );
}

fn formatName(format: EncodedFormat) []const u8 {
    return switch (format) {
        .wav => "wav",
        .mp3 => "mp3",
        .aac => "aac",
        .mp4 => "mp4",
        .ogg => "ogg",
        .opus => "opus",
        .flac => "flac",
        .aiff => "aiff",
        .caf => "caf",
        .au => "au",
    };
}

fn findCheckedInCase(name: []const u8) ?OpenCorpusCodecCase {
    for (open_corpus_cases.checked_in_cases) |case| {
        if (std.mem.eql(u8, case.name, name)) return case;
    }
    return null;
}

fn verifyCheckedInCase(
    allocator: std.mem.Allocator,
    case: OpenCorpusCodecCase,
    bytes: []const u8,
) !void {
    const detected = inference_audio.detectFormat(bytes) orelse
        inference_audio.detectFormatFromFilename(case.name) orelse
        return error.UndetectedFormat;
    if (detected != case.format) return error.UnexpectedDetectedFormat;

    var interleaved = inference_audio.decodeInterleaved(allocator, bytes, .{
        .file_name_hint = case.name,
    }) catch |err| switch (err) {
        error.UnsupportedAudioFormat => return error.InterleavedDecodeUnsupported,
        else => return error.InterleavedDecodeFailed,
    };
    defer interleaved.deinit();

    if (interleaved.sample_rate != case.expected_sample_rate) return error.UnexpectedSampleRate;
    if (interleaved.channels != case.expected_channels) return error.UnexpectedChannelCount;
    if (interleaved.samples.len == 0) return error.EmptyDecode;

    var mono = inference_audio.decode(allocator, bytes, .{
        .file_name_hint = case.name,
    }) catch |err| switch (err) {
        error.UnsupportedAudioFormat => return error.MonoDecodeUnsupported,
        else => return error.MonoDecodeFailed,
    };
    defer mono.deinit();
    if (mono.samples.len == 0) return error.EmptyMonoDecode;

    var reference = inference_audio.decodeInterleaved(allocator, case.bytes, .{
        .file_name_hint = case.name,
    }) catch |err| switch (err) {
        error.UnsupportedAudioFormat => return error.ReferenceDecodeUnsupported,
        else => return error.ReferenceDecodeFailed,
    };
    defer reference.deinit();
    if (reference.channels != case.expected_channels) return error.ReferenceChannelCountMismatch;

    const aligned_reference = if (interleaved.sample_rate == reference.sample_rate)
        try inference_audio.copyOrResample(allocator, reference.samples, reference.sample_rate, reference.sample_rate)
    else
        try inference_audio.resample(allocator, reference.samples, reference.sample_rate, interleaved.sample_rate);
    defer allocator.free(aligned_reference);

    const metrics = inference_audio.conformance.bestAlignmentMetrics(aligned_reference, interleaved.samples, 8192);
    if (metrics.compared < case.min_compared) return error.ReferenceComparedTooShort;
    if (metrics.correlation < case.min_correlation) return error.ReferenceCorrelationTooLow;
    if (metrics.mean_abs_error > case.max_mean_abs_error) return error.ReferenceMaeTooHigh;
}
