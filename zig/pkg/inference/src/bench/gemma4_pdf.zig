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

//! Real PDFs, bounded cross-document page waves, synchronized vectors.
const std = @import("std");
const pdf = @import("antfly_pdf");
const internal = @import("inference_internal");
const gpu = @import("pdf_gpu.zig");
const factory = internal.architectures.session_factory;
const pipeline = internal.pipelines.embedding_gemma2;
const a = std.heap.c_allocator;
const Document = struct { path: []const u8, bytes: []u8, parsed: pdf.reader.Reader, template: pdf.reader.RenderForkTemplate, count: usize };
const Item = struct { document: usize, page: usize };
const Worker = struct {
    io: std.Io,
    template: *const pdf.reader.RenderForkTemplate,
    item: Item,
    dpi: u16,
    dimension: u32,
    use_gpu: bool,
    classified: std.Io.Event = .unset,
    scene: ?gpu.Scene = null,
    result: ?pdf.RenderedPageRaster = null,
    failure: ?anyerror = null,
    scratch_peak: usize = 0,
    prepare_ns: u64 = 0,
    cpu_render_ns: u64 = 0,
    fn run(self: *Worker) void {
        // Also publish failures before preparation could classify this page.
        defer self.classified.set(self.io);
        self.render() catch |err| {
            self.failure = err;
        };
    }
    fn render(self: *Worker) !void {
        var bounded = internal.runtime.bounded_allocator.BoundedAllocator{ .backing = a, .limit = 256 * 1024 * 1024 };
        defer std.debug.assert(bounded.live == 0);
        const scratch = bounded.allocator();
        defer self.scratch_peak = bounded.peak;
        const started = now();
        var parsed = try self.template.instantiate(scratch, .{});
        defer parsed.deinit();
        if (self.use_gpu) self.scene = gpu.prepare(a, &parsed, .{ .page_number = self.item.page, .requested_dpi = self.dpi, .max_dimension = self.dimension, .max_pixels = 8_000_000 }) catch |err| switch (err) {
            error.UnsupportedGpuPdfPage => null,
            else => return err,
        };
        self.prepare_ns = now() - started;
        // Scene data is owned separately from parser scratch. Publishing an
        // unsupported page lets the painter run while its CPU fallback continues.
        self.classified.set(self.io);
        if (self.scene != null) return;
        const cpu_started = now();
        var raster = try pdf.renderParsedPageRasterAdaptiveWithProfileAlloc(scratch, &parsed, self.item.page, self.dpi, 8_000_000, self.dimension, .exact);
        defer raster.deinit(scratch);
        const retained = try a.dupe(u8, raster.bytes);
        self.result = raster;
        self.result.?.bytes = retained;
        self.cpu_render_ns = now() - cpu_started;
    }
};
const PixelParity = struct { document: usize, page: usize, max_error: u8, mean_absolute_error: f64 };
const WaveSample = struct {
    iteration: usize,
    first_item: usize,
    pages: usize,
    seconds: f64,
    render_seconds: f64,
    gpu_paint_seconds: f64,
    model: pipeline.MediaTimings,
    gpu_pages: usize,
    cpu_pages: usize,
    retained_raster_bytes: usize,
    cpu_scratch_peak_sum: usize,
    prepare_seconds_sum: f64,
    cpu_render_seconds_sum: f64,
    gpu_wall_seconds: f64,
    gpu_cached_buffer_bytes: usize,
};
const SavedVector = struct { document: usize, page: usize, values: []f32 };
fn now() u64 {
    return internal.platform.time.monotonicNs();
}
fn seconds(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / std.time.ns_per_s;
}

pub fn main(init: std.process.Init) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const args = try init.minimal.args.toSlice(out);
    var paths = std.ArrayList([]const u8).empty;
    var model: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    var renderer: []const u8 = "cpu";
    var backend: []const u8 = "metal";
    var scope: []const u8 = "pipeline";
    var resident = true;
    var batch: usize = 4;
    var workers: usize = 4;
    var warmups: usize = 1;
    var iterations: usize = 3;
    var max_pages: usize = 32;
    var dpi: u16 = 150;
    var dimension: u32 = 1536;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const k = args[i];
        const v = args[i + 1];
        if (std.mem.eql(u8, k, "--pdf")) try paths.append(out, v) else if (std.mem.eql(u8, k, "--model")) model = v else if (std.mem.eql(u8, k, "--output")) output = v else if (std.mem.eql(u8, k, "--renderer")) renderer = v else if (std.mem.eql(u8, k, "--backend")) backend = v else if (std.mem.eql(u8, k, "--scope")) scope = v else if (std.mem.eql(u8, k, "--resident")) {
            if (!std.mem.eql(u8, v, "on") and !std.mem.eql(u8, v, "off")) return error.InvalidArguments;
            resident = std.mem.eql(u8, v, "on");
        } else if (std.mem.eql(u8, k, "--batch")) batch = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--workers")) workers = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--warmups")) warmups = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--iterations")) iterations = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--max-pages")) max_pages = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--dpi")) dpi = try std.fmt.parseInt(u16, v, 10) else if (std.mem.eql(u8, k, "--max-dimension")) dimension = try std.fmt.parseInt(u32, v, 10) else return error.UnknownArgument;
    }
    if (paths.items.len == 0 or paths.items.len > 64 or batch == 0 or batch > 4 or workers == 0 or workers > 4 or iterations == 0 or iterations > 1000 or warmups > 100 or max_pages == 0 or max_pages > 10000 or dpi < 72 or dpi > 600 or dimension == 0 or dimension > 4096) return error.InvalidArguments;
    const use_gpu = std.mem.eql(u8, renderer, "gpu");
    if (!use_gpu and !std.mem.eql(u8, renderer, "cpu")) return error.InvalidRenderer;
    const do_model = std.mem.eql(u8, scope, "pipeline");
    if (!do_model and !std.mem.eql(u8, scope, "render")) return error.InvalidScope;
    const metal = std.mem.eql(u8, backend, "metal");
    if (!metal and !std.mem.eql(u8, backend, "native")) return error.InvalidBackend;
    var gpu_renderer: ?gpu.Renderer = if (use_gpu) try gpu.Renderer.init() else null;
    defer if (gpu_renderer) |r| r.deinit();
    const load_started = now();
    const session: ?internal.backends.Session = if (do_model) (if (metal) try factory.createMetalSession(a, model orelse return error.MissingModel) else try factory.createNativeSession(a, model orelse return error.MissingModel)) else null;
    defer if (session) |s| s.close();
    if (session) |s| {
        if (!metal) factory.attachIo(s, init.io);
        if (factory.getEmbeddingGemma2Config(s) == null) return error.WrongArchitecture;
    }
    const model_load_seconds = seconds(now() - load_started);
    const tokenizer: ?internal.tokenizer.Tokenizer = if (do_model) blk: {
        const path = try std.fs.path.join(out, &.{ model.?, "tokenizer.json" });
        const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, out, .limited(64 * 1024 * 1024));
        const t = try internal.hf_tokenizer.HfTokenizer.loadFromBytes(a, bytes);
        break :blk t.tokenizer();
    } else null;
    defer if (tokenizer) |t| t.deinitTokenizer();
    const parse_started = now();
    const documents = try a.alloc(Document, paths.items.len);
    defer a.free(documents);
    var initialized: usize = 0;
    defer for (documents[0..initialized]) |*doc| {
        doc.template.deinit();
        doc.parsed.deinit();
        a.free(doc.bytes);
    };
    var input_bytes: usize = 0;
    for (paths.items, documents) |path, *doc| {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, path, a, .limited(64 * 1024 * 1024));
        errdefer a.free(bytes);
        input_bytes += bytes.len;
        if (input_bytes > 512 * 1024 * 1024) return error.ResourceLimitExceeded;
        var parsed = try pdf.reader.Reader.init(a, bytes);
        errdefer parsed.deinit();
        const count = try parsed.pageCount();
        const template = try parsed.prepareRenderForkTemplate(a, .{});
        doc.* = .{ .path = path, .bytes = bytes, .parsed = parsed, .template = template, .count = count };
        initialized += 1;
    }
    const parse_seconds = seconds(now() - parse_started);
    var items = std.ArrayList(Item).empty;
    var page: usize = 1;
    // Round-robin documents: preserve explicit identifiers while filling
    // compatible vision cohorts from more than one PDF.
    while (items.items.len < max_pages) : (page += 1) {
        var any = false;
        for (documents, 0..) |doc, d| {
            if (page > doc.count) continue;
            any = true;
            try items.append(out, .{ .document = d, .page = page });
            if (items.items.len == max_pages) break;
        }
        if (!any) break;
    }
    if (items.items.len == 0) return error.EmptyPdf;
    var samples = std.ArrayList(WaveSample).empty;
    var parity = std.ArrayList(PixelParity).empty;
    var saved = std.ArrayList(SavedVector).empty;
    const totals = try out.alloc(f64, iterations);
    for (0..warmups + iterations) |iteration| {
        const iteration_started = now();
        var offset: usize = 0;
        while (offset < items.items.len) {
            const count = @min(batch, items.items.len - offset);
            const wave_items = items.items[offset..][0..count];
            const wave_started = now();
            var rasters: [4]?pdf.RenderedPageRaster = @splat(null);
            defer for (rasters[0..count]) |*r| if (r.*) |*raster| raster.deinit(a);
            var gpu_indexes: [4]usize = undefined;
            var gpu_count: usize = 0;
            var work: [4]Worker = undefined;
            var cpu_count: usize = 0;
            for (wave_items, 0..) |item, r| work[r] = .{ .io = init.io, .template = &documents[item.document].template, .item = item, .dpi = dpi, .dimension = dimension, .use_gpu = use_gpu };
            var scratch_sum: usize = 0;
            var prepare_ns: u64 = 0;
            var cpu_render_ns: u64 = 0;
            var gpu_seconds: f64 = 0;
            var gpu_wall_ns: u64 = 0;
            var worker_offset: usize = 0;
            while (worker_offset < count) : (worker_offset += @min(workers, count - worker_offset)) {
                const n = @min(workers, count - worker_offset);
                var threads: [4]?std.Thread = @splat(null);
                // On every exit, drain owned workers and attach their output
                // before outer raster cleanup. Scene scratch never outlives a join.
                defer for (0..n) |w| {
                    if (threads[w]) |thread| thread.join();
                    const r = worker_offset + w;
                    rasters[r] = work[r].result orelse rasters[r];
                    scratch_sum += work[r].scratch_peak;
                    prepare_ns += work[r].prepare_ns;
                    cpu_render_ns += work[r].cpu_render_ns;
                    if (work[r].scene) |*scene| scene.deinit(a);
                };
                for (0..n) |w| {
                    threads[w] = std.Thread.spawn(.{}, Worker.run, .{&work[worker_offset + w]}) catch null;
                    if (threads[w] == null) work[worker_offset + w].run();
                }
                var scenes: [4]gpu.Scene = undefined;
                var indexes: [4]usize = undefined;
                var prepared_count: usize = 0;
                for (0..n) |w| {
                    const r = worker_offset + w;
                    work[r].classified.waitUncancelable(init.io);
                    if (work[r].scene) |scene| {
                        scenes[prepared_count] = scene;
                        indexes[prepared_count] = r;
                        prepared_count += 1;
                    } else cpu_count += 1;
                }
                if (prepared_count > 0) {
                    const gpu_started = now();
                    var command_seconds: f64 = 0;
                    const rendered = try gpu_renderer.?.renderBatch(a, scenes[0..prepared_count], &command_seconds);
                    defer a.free(rendered);
                    for (rendered, indexes[0..prepared_count]) |raster, r| {
                        rasters[r] = raster;
                        gpu_indexes[gpu_count] = r;
                        gpu_count += 1;
                    }
                    gpu_seconds += command_seconds;
                    gpu_wall_ns += now() - gpu_started;
                }
                // Finish before advancing indexes observed by the drain defer.
                for (threads[0..n]) |*t| if (t.*) |thread| {
                    thread.join();
                    t.* = null;
                };
                for (work[worker_offset..][0..n]) |worker| if (worker.failure) |err| return err;
            }
            const render_seconds = seconds(now() - wave_started);
            var views: [4]internal.antfly_image.BorrowedRasterAttachment = undefined;
            var retained_bytes: usize = 0;
            for (rasters[0..count], views[0..count]) |r, *view| {
                const raster = r orelse return error.MissingRaster;
                retained_bytes += raster.bytes.len;
                view.* = .{ .bytes = raster.bytes, .width = raster.width, .height = raster.height, .stride_bytes = raster.stride };
            }
            if (retained_bytes > 256 * 1024 * 1024) return error.ResourceLimitExceeded;
            var timings = pipeline.MediaTimings{};
            if (do_model) {
                const result = try pipeline.embedRasters(a, session.?, tokenizer.?, views[0..count], .{ .resident_images = resident, .timings = &timings }, null, .{ .native_call_policy = .in_process });
                defer a.free(result.vectors);
                defer for (result.vectors) |v| a.free(v);
                if (iteration + 1 == warmups + iterations) for (wave_items, result.vectors) |item, v| try saved.append(out, .{ .document = item.document, .page = item.page, .values = try out.dupe(f32, v) });
            }
            const elapsed = seconds(now() - wave_started);
            if (iteration >= warmups) try samples.append(out, .{ .iteration = iteration - warmups, .first_item = offset, .pages = count, .seconds = elapsed, .render_seconds = render_seconds, .gpu_paint_seconds = gpu_seconds, .model = timings, .gpu_pages = gpu_count, .cpu_pages = cpu_count, .retained_raster_bytes = retained_bytes, .cpu_scratch_peak_sum = scratch_sum, .prepare_seconds_sum = seconds(prepare_ns), .cpu_render_seconds_sum = seconds(cpu_render_ns), .gpu_wall_seconds = seconds(gpu_wall_ns), .gpu_cached_buffer_bytes = if (gpu_renderer) |r| r.cachedBytes() else 0 });
            // First-iteration pixel gate is outside wave timing. A separate
            // render-only run provides uncontaminated pipeline totals.
            if (use_gpu and iteration == 0) for (gpu_indexes[0..gpu_count]) |r| {
                const item = wave_items[r];
                var ref = try pdf.renderParsedPageRasterAdaptiveWithProfileAlloc(a, &documents[item.document].parsed, item.page, dpi, 8_000_000, dimension, .exact);
                defer ref.deinit(a);
                const actual = rasters[r].?;
                if (actual.width != ref.width or actual.height != ref.height or actual.bytes.len != ref.bytes.len) return error.RenderGeometryMismatch;
                var maximum: u8 = 0;
                var sum: u64 = 0;
                for (actual.bytes, ref.bytes) |x, y| {
                    const delta: u8 = @intCast(@abs(@as(i16, x) - @as(i16, y)));
                    maximum = @max(maximum, delta);
                    sum += delta;
                }
                const mae = @as(f64, @floatFromInt(sum)) / @as(f64, @floatFromInt(ref.bytes.len));
                try parity.append(out, .{ .document = item.document, .page = item.page, .max_error = maximum, .mean_absolute_error = mae });
                // Geometry rounding at f32 boundaries may move a sample. No
                // pixel gate is silently replaced with an embedding-only gate.
                if (maximum > 1) {
                    std.debug.print("GPU PDF parity document={d} page={d} max_error={d} mae={d:.6} dimensions={d}x{d}\n", .{ item.document, item.page, maximum, mae, actual.width, actual.height });
                    return error.GpuPdfPixelParityFailed;
                }
            };
            offset += count;
        }
        // Sum measured wave boundaries, excluding first-iteration pixel gates.
        var total: f64 = 0;
        if (iteration >= warmups) {
            for (samples.items) |sample| if (sample.iteration == iteration - warmups) {
                total += sample.seconds;
            };
            totals[iteration - warmups] = total;
        }
        std.debug.print("pdf renderer={s} resident={} iteration={d}/{d} elapsed={d:.3}s\n", .{ renderer, resident, iteration + 1, warmups + iterations, seconds(now() - iteration_started) });
    }
    const sorted = try out.dupe(f64, totals);
    std.mem.sort(f64, sorted, {}, std.sort.asc(f64));
    var total: f64 = 0;
    for (totals) |v| total += v;
    const json = try std.json.Stringify.valueAlloc(out, .{
        .version = 1,
        .model = model,
        .backend = backend,
        .scope = scope,
        .renderer = renderer,
        .resident_requested = resident,
        .documents = paths.items,
        .items = items.items,
        .pages = items.items.len,
        .batch = batch,
        .workers = workers,
        .warmups = warmups,
        .iterations = iterations,
        .dpi = dpi,
        .max_dimension = dimension,
        .profile = "exact",
        .model_load_seconds = model_load_seconds,
        .parse_seconds = parse_seconds,
        .input_bytes = input_bytes,
        .p50_seconds = sorted[(iterations - 1) / 2],
        .p95_seconds = sorted[(iterations * 95 + 99) / 100 - 1],
        .pages_per_second = @as(f64, @floatFromInt(items.items.len * iterations)) / total,
        .samples = samples.items,
        .pixel_parity = parity.items,
        .output = saved.items,
        .timing_note = "stage values are CPU enqueue durations; final vector readback synchronizes the model; gpu_paint_seconds is Metal command-buffer time",
        .memory_note = "worker scratch is bounded per worker; device_owned_peak excludes model weights and runtime pools; input bytes and retained rasters reported separately; this is not total RSS",
        .gpu_note = "prototype paints simple fills and opaque nearest-sampled images; unsupported pages use CPU; GPU raster output is copied to host for shared preprocessing",
    }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output orelse return error.MissingOutput, .data = json });
}
