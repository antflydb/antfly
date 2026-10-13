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

//! Synchronized real-model image benchmark. Inputs are read outside timing.
const std = @import("std");
const internal = @import("inference_internal");
const factory = internal.architectures.session_factory;
const projector = internal.architectures.gemma4_projector;
const pipeline = internal.pipelines.embedding_gemma2;
const Bounded = internal.runtime.bounded_allocator.BoundedAllocator;
const Memory = if (@import("build_options").enable_metal) internal.metal_runtime.RawRuntimeMemoryStats else struct {};
const Sample = struct { seconds: f64, host_peak_bytes: usize, device_peak_bytes: u64, download_bytes: u64, device_readbacks: u64, gpu_frame_ns: u128, frame_submits: u64, frame_wait_ns: u128, media: pipeline.MediaTimings, memory: Memory };
const Report = struct { max_projector_batch: ?usize, image: []const u8, batch: usize, scope: []const u8, projected_tokens: usize, samples: []Sample, p50_seconds: f64, p95_seconds: f64, images_per_second: f64, output: []f32 };
fn now() !u64 {
    var ts: std.posix.timespec = undefined;
    if (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts)) != .SUCCESS) return error.ClockFailed;
    return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec);
}
pub fn main(init: std.process.Init) !void {
    // This executable is an offline child; its driver can terminate the entire
    // process. Production request boundary requirements are unchanged.
    internal.execution_control.allowUninterruptibleInProcess();
    const a = std.heap.c_allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const args = try init.minimal.args.toSlice(out);
    var model: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var backend: []const u8 = "metal";
    var scope: []const u8 = "projector";
    var batch: usize = 1;
    var batching = false;
    var prepare_ahead = true;
    var warmups: usize = 10;
    var iterations: usize = 100;
    var scratch_mib: usize = 4096;
    var paths = std.ArrayList([]const u8).empty;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const k = args[i];
        const v = args[i + 1];
        if (std.mem.eql(u8, k, "--model")) model = v else if (std.mem.eql(u8, k, "--output")) output = v else if (std.mem.eql(u8, k, "--backend")) backend = v else if (std.mem.eql(u8, k, "--scope")) scope = v else if (std.mem.eql(u8, k, "--batching")) {
            if (!std.mem.eql(u8, v, "on") and !std.mem.eql(u8, v, "off")) return error.InvalidBatching;
            batching = std.mem.eql(u8, v, "on");
        } else if (std.mem.eql(u8, k, "--prepare-ahead")) {
            if (!std.mem.eql(u8, v, "on") and !std.mem.eql(u8, v, "off")) return error.InvalidPreparationMode;
            prepare_ahead = std.mem.eql(u8, v, "on");
        } else if (std.mem.eql(u8, k, "--batch")) batch = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--warmups")) warmups = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--scratch-mib")) scratch_mib = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--iterations")) iterations = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--image")) try paths.append(out, v) else return error.UnknownArgument;
    }
    if (scratch_mib == 0 or scratch_mib > 8192 or paths.items.len == 0 or batch == 0 or batch > 8 or warmups > 100 or iterations == 0 or iterations > 10000) return error.InvalidBenchmark;
    if (!std.mem.eql(u8, scope, "projector") and !std.mem.eql(u8, scope, "image") and !std.mem.eql(u8, scope, "embedding") and !std.mem.eql(u8, scope, "encoded-embedding")) return error.InvalidScope;
    const embedding_scope = std.mem.eql(u8, scope, "embedding") or std.mem.eql(u8, scope, "encoded-embedding");
    const metal = std.mem.eql(u8, backend, "metal");
    if (!metal and !std.mem.eql(u8, backend, "native")) return error.InvalidBackend;
    const load_started = try now();
    const session = if (metal) try factory.createMetalSession(a, model orelse return error.MissingModel) else try factory.createNativeSession(a, model orelse return error.MissingModel);
    defer session.close();
    if (!metal) factory.attachIo(session, init.io);
    const load_seconds = @as(f64, @floatFromInt(try now() - load_started)) / std.time.ns_per_s;
    const cfg = factory.getEmbeddingGemma2Config(session) orelse return error.WrongArchitecture;
    if (!cfg.vision) return error.NoVisionSession;
    const tokenizer_path = try std.fs.path.join(out, &.{ model.?, "tokenizer.json" });
    const tokenizer_bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, tokenizer_path, out, .limited(64 * 1024 * 1024));
    const tokenizer_impl = try internal.hf_tokenizer.HfTokenizer.loadFromBytes(a, tokenizer_bytes);
    const tokenizer = tokenizer_impl.tokenizer();
    defer tokenizer.deinitTokenizer();
    var reports = std.ArrayList(Report).empty;
    for (paths.items) |path| {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, out, .limited(64 * 1024 * 1024));
        const prepared = try projector.prepareEmbeddingGemma2Image(a, bytes);
        defer prepared.deinit();
        const samples = try out.alloc(Sample, iterations);
        var saved: []f32 = &.{};
        var executed_batch: usize = 1;
        for (0..warmups + iterations) |iteration| {
            var bounded = Bounded{ .backing = pipeline.workspaceBackingAllocator(session.backend()), .limit = 512 * 1024 * 1024 };
            const workspace = bounded.allocator();
            {
                var permit: ?internal.RunPermit = if (embedding_scope) null else try session.admit(.{ .batch = batch, .sequence = 2520, .host_preprocess_bytes = 512 * 1024 * 1024, .workspace_bytes = scratch_mib * 1024 * 1024, .output_bytes = batch * 280 * 512 * 4 });
                defer if (permit) |*p| p.deinit();
                var optional_cb: ?internal.ops.ComputeBackend = if (embedding_scope) null else try factory.getComputeBackend(session, workspace);
                defer if (optional_cb) |*backend_cb| backend_cb.deinit();
                const cb = if (optional_cb) |*backend_cb| backend_cb else null;
                const before = if (cb) |backend_cb| backend_cb.debugTimingSnapshot().provider else internal.ops.NativeQuantTimingStats{};
                var values = std.ArrayList(f32).empty;
                defer values.deinit(workspace);
                var media: pipeline.MediaTimings = .{};
                const started = try now();
                if (batching and !embedding_scope) {
                    var prepared_batch: [8]projector.PreparedImage = undefined;
                    var owned: [8]projector.PreparedImage = undefined;
                    var initialized: usize = 0;
                    defer for (owned[0..initialized]) |item| item.deinit();
                    for (0..batch) |b| {
                        if (std.mem.eql(u8, scope, "projector")) prepared_batch[b] = prepared else {
                            owned[initialized] = try projector.prepareEmbeddingGemma2Image(workspace, bytes);
                            prepared_batch[b] = owned[initialized];
                            initialized += 1;
                        }
                    }
                    var result = try projector.encodePreparedEmbeddingGemma2Images(cb.?, workspace, prepared_batch[0..batch], .{ .max_batch = batch, .max_patch_tokens = 20160, .max_scratch_bytes = scratch_mib * 1024 * 1024 });
                    defer result.deinit();
                    executed_batch = result.max_batch_executed;
                    try values.appendSlice(workspace, result.embeddings);
                } else if (std.mem.eql(u8, scope, "encoded-embedding")) {
                    // Every row decodes independently through the production
                    // encoded-image path, including bounded lookahead work.
                    var images: [8][]const u8 = undefined;
                    @memset(images[0..batch], bytes);
                    if (batching) {
                        const result = try pipeline.embedImages(workspace, session, tokenizer, images[0..batch], .{ .prepare_ahead = prepare_ahead, .timings = &media }, null, null, null);
                        defer workspace.free(result.vectors);
                        defer for (result.vectors) |vector| workspace.free(vector);
                        for (result.vectors) |vector| try values.appendSlice(workspace, vector);
                    } else for (images[0..batch]) |encoded| {
                        const result = try pipeline.embed(workspace, session, tokenizer, .{ .content = &.{.{ .image = encoded }} }, .{}, null, null);
                        defer workspace.free(result.vector);
                        try values.appendSlice(workspace, result.vector);
                    }
                } else if (std.mem.eql(u8, scope, "embedding")) {
                    // Use the PDF/raster production route for both A/B modes.
                    // Decode and RGBA packing are inside the timed boundary.
                    const image = internal.pipelines.image;
                    const decoded = try image.decode(workspace, bytes);
                    defer decoded.deinit(workspace);
                    const rgba = try workspace.alloc(u8, @as(usize, decoded.width) * decoded.height * 4);
                    defer workspace.free(rgba);
                    for (0..@as(usize, decoded.width) * decoded.height) |pixel| {
                        @memcpy(rgba[pixel * 4 ..][0..3], decoded.data[pixel * 3 ..][0..3]);
                        rgba[pixel * 4 + 3] = 255;
                    }
                    const Raster = internal.antfly_image.BorrowedRasterAttachment;
                    var rasters: [8]Raster = undefined;
                    for (rasters[0..batch]) |*raster| raster.* = .{ .bytes = rgba, .width = decoded.width, .height = decoded.height, .stride_bytes = @as(usize, decoded.width) * 4 };
                    if (batching) {
                        const result = try pipeline.embedRasters(workspace, session, tokenizer, rasters[0..batch], .{ .prepare_ahead = prepare_ahead, .timings = &media }, null, null);
                        defer workspace.free(result.vectors);
                        defer for (result.vectors) |vector| workspace.free(vector);
                        for (result.vectors) |vector| try values.appendSlice(workspace, vector);
                    } else for (rasters[0..batch]) |raster| {
                        const result = try pipeline.embed(workspace, session, tokenizer, .{ .content = &.{.{ .raster = raster }} }, .{}, null, null);
                        defer workspace.free(result.vector);
                        try values.appendSlice(workspace, result.vector);
                    }
                } else {
                    for (0..batch) |_| {
                        const result = if (std.mem.eql(u8, scope, "projector")) try projector.encodePreparedEmbeddingGemma2Image(cb.?, workspace, prepared) else try projector.encodeEmbeddingGemma2Image(cb.?, workspace, bytes);
                        defer workspace.free(result.embeddings);
                        try values.appendSlice(workspace, result.embeddings);
                    }
                }
                const elapsed = try now() - started;
                const after = if (cb) |backend_cb| backend_cb.debugTimingSnapshot().provider else internal.ops.NativeQuantTimingStats{};
                var memory: Memory = .{};
                if (comptime @import("build_options").enable_metal) {
                    if (metal and cb != null) {
                        const compute: *internal.native_compute.metal.MetalCompute = @ptrCast(@alignCast(cb.?.ptr));
                        memory = internal.metal_runtime.runtimeMemorySnapshot(compute.provider_impl.raw_decode_runtime);
                    }
                }
                if (iteration >= warmups) samples[iteration - warmups] = .{ .seconds = @as(f64, @floatFromInt(elapsed)) / std.time.ns_per_s, .host_peak_bytes = bounded.peak, .device_peak_bytes = after.metal_tensor_device_owned_peak_live_bytes, .download_bytes = after.metal_tensor_host_mirror_download_bytes - before.metal_tensor_host_mirror_download_bytes, .device_readbacks = after.metal_tensor_to_host_device_calls - before.metal_tensor_to_host_device_calls, .gpu_frame_ns = if (cb == null) media.gpu_frame_ns else after.decoder_runtime_frame_gpu_nanos -| before.decoder_runtime_frame_gpu_nanos, .frame_submits = if (cb == null) media.frame_submits else after.decoder_runtime_frame_submits -| before.decoder_runtime_frame_submits, .frame_wait_ns = if (cb == null) media.frame_wait_ns else after.decoder_runtime_frame_wait_nanos -| before.decoder_runtime_frame_wait_nanos, .media = media, .memory = memory };
                if (iteration + 1 == warmups + iterations) saved = try out.dupe(f32, values.items);
            }
            if (bounded.live != 0) return error.WorkspaceLeak;
            std.debug.print("{s} {s} batch={d} iteration={d}/{d}\n", .{ scope, path, batch, iteration + 1, warmups + iterations });
        }
        const sorted = try out.alloc(f64, iterations);
        var sum: f64 = 0;
        for (samples, sorted) |sample, *v| {
            v.* = sample.seconds;
            sum += sample.seconds;
        }
        std.mem.sort(f64, sorted, {}, std.sort.asc(f64));
        try reports.append(out, .{ .max_projector_batch = if (embedding_scope) null else executed_batch, .image = path, .batch = batch, .scope = scope, .projected_tokens = prepared.geometry.tokenCount(), .samples = samples, .p50_seconds = sorted[(iterations - 1) / 2], .p95_seconds = sorted[(iterations * 95 + 99) / 100 - 1], .images_per_second = @as(f64, @floatFromInt(batch * iterations)) / sum, .output = saved });
    }
    const json = try std.json.Stringify.valueAlloc(out, .{ .version = 1, .model = model.?, .backend = backend, .load_seconds = load_seconds, .warmups = warmups, .execution = if (batching) "bounded same-shape batches" else "sequential", .transfer_and_workspace_counters_available = !embedding_scope, .synchronized_readback = true, .prepare_ahead = prepare_ahead, .cases = reports.items }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output orelse return error.MissingOutput, .data = json });
}
