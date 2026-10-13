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

//! Real-model audio embedding qualification. File reads and model loading are
//! outside warm timing; vectors are synchronized and retained for parity.
const std = @import("std");
const inference = @import("inference_internal");
const embedding = inference.pipelines.embedding;
const grouped = inference.pipelines.embedding_gemma2;
const Sample = struct { seconds: f64, execution: []const u8, media: grouped.MediaTimings };

pub fn main(init: std.process.Init) !void {
    inference.execution_control.allowUninterruptibleInProcess();
    const a = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const args = try init.minimal.args.toSlice(out);
    var model: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var batch: usize = 1;
    var warmups: usize = 2;
    var iterations: usize = 5;
    var batching = true;
    var ahead = true;
    var allow_errors = false;
    var pcm_scope = false;
    var paths = std.ArrayList([]const u8).empty;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const k = args[i];
        const v = args[i + 1];
        if (std.mem.eql(u8, k, "--model")) model = v else if (std.mem.eql(u8, k, "--output")) output = v else if (std.mem.eql(u8, k, "--batch")) batch = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--warmups")) warmups = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--iterations")) iterations = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--audio")) try paths.append(out, v) else if (std.mem.eql(u8, k, "--scope")) {
            if (!std.mem.eql(u8, v, "encoded") and !std.mem.eql(u8, v, "pcm")) return error.InvalidMode;
            pcm_scope = std.mem.eql(u8, v, "pcm");
        } else if (std.mem.eql(u8, k, "--allow-item-errors")) {
            if (!std.mem.eql(u8, v, "on") and !std.mem.eql(u8, v, "off")) return error.InvalidMode;
            allow_errors = std.mem.eql(u8, v, "on");
        } else if (std.mem.eql(u8, k, "--batching") or std.mem.eql(u8, k, "--prepare-ahead")) {
            if (!std.mem.eql(u8, v, "on") and !std.mem.eql(u8, v, "off")) return error.InvalidMode;
            if (std.mem.eql(u8, k, "--batching")) batching = std.mem.eql(u8, v, "on") else ahead = std.mem.eql(u8, v, "on");
        } else return error.UnknownArgument;
    }
    if (batch == 0 or batch > 32 or paths.items.len == 0 or iterations == 0 or iterations > 1000 or warmups > 100) return error.InvalidBenchmark;
    var sessions = inference.backends.SessionManager.initWithIo(a, init.io);
    sessions.preferred_backends = &.{.metal};
    var manager = inference.server.model_manager.ModelManager.init(a, sessions);
    defer manager.deinit();
    const start = inference.platform.time.monotonicNs();
    const loaded = try manager.loadFromDir(model orelse return error.MissingModel);
    try loaded.ensureEmbeddingAssets(false, false, true);
    const load_ns = inference.platform.time.monotonicNs() - start;
    const files = try out.alloc([]const u8, paths.items.len);
    for (files, paths.items) |*bytes, path| bytes.* = try std.Io.Dir.cwd().readFileAlloc(init.io, path, out, .limited(64 * 1024 * 1024));
    const clips = try out.alloc(embedding.EncodedAudioClip, batch);
    for (clips, 0..) |*clip, row| clip.* = .{ .bytes = files[row % files.len] };
    const pcms = try out.alloc(inference.pipelines.audio.PcmAudio, if (pcm_scope) batch else 0);
    if (pcm_scope) for (pcms, 0..) |*pcm, row| {
        const decoded = try inference.pipelines.audio.decodeBounded(out, files[row % files.len], .{}, 64 * 1024 * 1024);
        pcm.* = .{ .samples = decoded.samples, .sample_rate = decoded.sample_rate };
    };
    const item_errors = try out.alloc(?[]const u8, batch);
    @memset(item_errors, null);
    const samples = try out.alloc(Sample, iterations);
    var vectors: [][]f32 = &.{};
    for (0..warmups + iterations) |iteration| {
        var pipeline = loaded.embeddingPipeline(a);
        var media: grouped.MediaTimings = .{};
        if (comptime @hasField(embedding.EmbeddingConfig, "audio_prepared_batches")) pipeline.config.audio_prepared_batches = batching;
        if (comptime @hasField(embedding.EmbeddingConfig, "audio_prepare_ahead")) pipeline.config.audio_prepare_ahead = ahead;
        if (comptime @hasField(embedding.EmbeddingConfig, "gemma4_audio_batching")) pipeline.config.gemma4_audio_batching = batching;
        if (comptime @hasField(embedding.EmbeddingPipeline, "audio_timings")) pipeline.audio_timings = &media;
        const errors = try a.alloc(?anyerror, batch);
        defer a.free(errors);
        const begun = inference.platform.time.monotonicNs();
        @memset(errors, null);
        const result = if (pcm_scope) try pipeline.embedAudioPcmReported(pcms) else try pipeline.embedEncodedAudioIndexed(clips, errors);
        const elapsed = inference.platform.time.monotonicNs() - begun;
        defer {
            for (result.vectors) |vector| a.free(vector);
            a.free(result.vectors);
        }
        for (errors, 0..) |err, row| if (err) |failure| {
            if (!allow_errors) return failure;
            item_errors[row] = @errorName(failure);
        };
        if (result.vectors.len != batch) return error.InvalidOutput;
        if (iteration >= warmups) samples[iteration - warmups] = .{ .seconds = @as(f64, @floatFromInt(elapsed)) / std.time.ns_per_s, .execution = @tagName(result.execution), .media = media };
        if (iteration + 1 == warmups + iterations) {
            vectors = try out.alloc([]f32, batch);
            for (vectors, result.vectors) |*dst, src| dst.* = try out.dupe(f32, src);
        }
        std.debug.print("audio batch={d} iteration={d}/{d}\n", .{ batch, iteration + 1, warmups + iterations });
    }
    const json = try std.json.Stringify.valueAlloc(out, .{ .model = model.?, .backend = @tagName(loaded.session.backend()), .load_ns = load_ns, .batch = batch, .batching = batching, .prepare_ahead = ahead, .scope = if (pcm_scope) "pcm" else "encoded", .item_errors = item_errors, .warmups = warmups, .samples = samples, .vectors = vectors }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output orelse return error.MissingOutput, .data = json });
}
