// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! `zig build tile-whisper -- <hf-model-dir> <audio.wav|mel.f32> [options]`
//!
//! Transcribes English audio with Whisper running entirely on tile kernels
//! (CPU lowering). `mel.f32` is a raw [n_mels, 3000] f32 log-mel, e.g. from
//! the Hugging Face feature extractor; a WAV goes through Antfly's log-mel.
//!
//! Options:
//!   --reference <dir>  compare with dumps from a reference run
//!                      (encoder_out.f32, step0_logits.f32, reference.json)
//!   --threads <n>      CPU threads (default: all)
//!   --profile          time per kernel kind

const std = @import("std");
const audio = @import("inference_audio");
const whisper = @import("whisper_model.zig");
const parallel = @import("antfly_tile").parallel;

/// <|startoftranscript|><|en|><|transcribe|><|notimestamps|>
const prompt = [_]u32{ 50258, 50259, 50359, 50363 };
const first_special: u32 = 50257;

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = std.heap.smp_allocator;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    if (args.len < 3) {
        std.debug.print("usage: tile-whisper <hf-model-dir> <audio.wav|mel.f32> [--reference <dir>] [--threads <n>]\n", .{});
        return error.InvalidArguments;
    }
    const model_dir = args[1];
    const input = args[2];
    var reference_dir: ?[]const u8 = null;
    var i: usize = 3;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--reference") and i + 1 < args.len) {
            i += 1;
            reference_dir = args[i];
        } else if (std.mem.eql(u8, args[i], "--profile")) {
            whisper.profile_enabled = true;
        } else if (std.mem.eql(u8, args[i], "--threads") and i + 1 < args.len) {
            i += 1;
            parallel.setThreads(try std.fmt.parseInt(usize, args[i], 10));
        } else return error.InvalidArguments;
    }

    var t0 = now(io);
    var model = try whisper.Model.load(allocator, io, model_dir);
    defer model.deinit();
    var detok = try whisper.Detokenizer.load(allocator, io, model_dir);
    defer detok.deinit();
    const c = model.cfg;
    std.debug.print("model: d_model={d} layers={d}/{d} heads={d} vocab={d} ({d} ms to load), {d} threads\n", .{ c.d_model, c.encoder_layers, c.decoder_layers, c.encoder_heads, c.vocab_size, ms(io, t0), parallel.threadCount() });

    // Log-mel input.
    const mel_len = c.num_mel_bins * c.melFrames();
    const mel = try allocator.alloc(f32, mel_len);
    defer allocator.free(mel);
    if (std.mem.endsWith(u8, input, ".f32")) {
        try readF32(io, input, mel);
    } else {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(io, input, allocator, .limited(1 << 30));
        defer allocator.free(bytes);
        const decoded = try audio.wav.decodeMono(allocator, bytes);
        defer allocator.free(decoded.samples);
        if (decoded.format.sample_rate != 16_000) return error.ExpectedSixteenKilohertzAudio;
        const features = try audio.logMelSpectrogramWithConfig(allocator, decoded.samples, .{
            .sample_rate = 16_000,
            .n_fft = 400,
            .hop_length = 160,
            .n_mels = @intCast(c.num_mel_bins),
            .chunk_length_s = 30,
            .normalization = .whisper,
        });
        defer allocator.free(features);
        @memcpy(mel, features[0..mel_len]);
        if (reference_dir) |dir| {
            const ref_mel = try allocator.alloc(f32, mel_len);
            defer allocator.free(ref_mel);
            if (readF32(io, try join(init.arena.allocator(), dir, "mel.f32"), ref_mel)) |_| {
                report("log-mel vs reference", ref_mel, mel);
            } else |_| {}
        }
    }

    // Encoder.
    const encoded = try allocator.alloc(f32, c.audio_ctx * c.d_model);
    defer allocator.free(encoded);
    t0 = now(io);
    try model.encode(mel, encoded);
    const encode_ms = ms(io, t0);
    std.debug.print("encoder: {d} ms\n", .{encode_ms});
    whisper.printProfile("encoder");
    if (reference_dir) |dir| {
        const ref = try allocator.alloc(f32, encoded.len);
        defer allocator.free(ref);
        try readF32(io, try join(init.arena.allocator(), dir, "encoder_out.f32"), ref);
        report("encoder output vs reference", ref, encoded);
    }

    // Greedy decode, suppressing what generation_config.json suppresses.
    const gen_json = try std.Io.Dir.cwd().readFileAlloc(io, try join(init.arena.allocator(), model_dir, "generation_config.json"), allocator, .limited(1 << 20));
    defer allocator.free(gen_json);
    const gen = try std.json.parseFromSlice(std.json.Value, allocator, gen_json, .{});
    defer gen.deinit();
    const suppress = try intList(init.arena.allocator(), gen.value.object.get("suppress_tokens").?.array.items, &.{});
    const begin_suppress = try intList(init.arena.allocator(), gen.value.object.get("begin_suppress_tokens").?.array.items, suppress);
    var decoder = try whisper.Model.Decoder.init(&model, encoded);
    defer decoder.deinit();
    var tokens: std.ArrayList(u32) = .empty;
    defer tokens.deinit(allocator);
    try tokens.appendSlice(allocator, &prompt);
    t0 = now(io);
    var pending: []const u32 = &prompt;
    var single: [1]u32 = undefined;
    while (tokens.items.len < c.text_ctx) {
        try decoder.step(pending);
        if (tokens.items.len == prompt.len) if (reference_dir) |dir| {
            const ref = try allocator.alloc(f32, c.vocab_size);
            defer allocator.free(ref);
            if (readF32(io, try join(init.arena.allocator(), dir, "step0_logits.f32"), ref)) |_| {
                report("first-step logits vs reference", ref, decoder.logits);
            } else |_| {}
        };
        const next = decoder.choose(if (tokens.items.len == prompt.len) begin_suppress else suppress);
        try tokens.append(allocator, next);
        if (next == c.eot) break;
        single[0] = next;
        pending = &single;
    }
    const decode_ms = ms(io, t0);
    const generated = tokens.items.len - prompt.len;
    whisper.printProfile("decoder");
    std.debug.print("decoder: {d} tokens in {d} ms ({d} ms/token)\n", .{ generated, decode_ms, @divTrunc(decode_ms, @as(i64, @intCast(@max(generated, 1)))) });

    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(allocator);
    try detok.decode(tokens.items, first_special, &text);
    std.debug.print("tokens: {any}\n", .{tokens.items});
    std.debug.print("text: {s}\n", .{text.items});

    if (reference_dir) |dir| {
        const json = try std.Io.Dir.cwd().readFileAlloc(io, try join(init.arena.allocator(), dir, "reference.json"), allocator, .limited(1 << 20));
        defer allocator.free(json);
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json, .{});
        defer parsed.deinit();
        const want = parsed.value.object.get("tokens").?.array.items;
        var same = want.len == tokens.items.len;
        if (same) for (want, tokens.items) |w, t| {
            if (w.integer != t) same = false;
        };
        std.debug.print("tokens vs reference: {s}\n", .{if (same) "identical" else "DIFFERENT"});
        if (!same) return error.TokensDiffer;
    }
}

/// JSON ints, appended to `base`.
fn intList(a: std.mem.Allocator, items: []const std.json.Value, base: []const i32) ![]const i32 {
    const out = try a.alloc(i32, base.len + items.len);
    @memcpy(out[0..base.len], base);
    for (items, out[base.len..]) |v, *o| o.* = @intCast(v.integer);
    return out;
}

fn now(io: std.Io) i96 {
    return std.Io.Clock.awake.now(io).nanoseconds;
}

fn ms(io: std.Io, start: i96) i64 {
    return @intCast(@divTrunc(now(io) - start, std.time.ns_per_ms));
}

fn join(a: std.mem.Allocator, dir: []const u8, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{s}/{s}", .{ dir, name });
}

fn readF32(io: std.Io, path: []const u8, out: []f32) !void {
    var file = try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    var reader = file.reader(io, &.{});
    try reader.interface.readSliceAll(std.mem.sliceAsBytes(out));
}

fn report(label: []const u8, want: []const f32, got: []const f32) void {
    var max_abs: f32 = 0;
    var max_ref: f32 = 0;
    var sum_sq: f64 = 0;
    for (want, got) |w, g| {
        max_abs = @max(max_abs, @abs(w - g));
        max_ref = @max(max_ref, @abs(w));
        sum_sq += @as(f64, w - g) * (w - g);
    }
    const rms = @sqrt(sum_sq / @as(f64, @floatFromInt(want.len)));
    std.debug.print("{s}: max |diff| {e:.2} (values up to {d:.2}), rms diff {e:.2}\n", .{ label, max_abs, max_ref, rms });
}
