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

//! Experimental Metal page painter. Parsing/image decoding stays on CPU.
//! Unsupported features reject the entire page before GPU submission.
const std = @import("std");
const pdf = @import("antfly_pdf");
const builtin = @import("builtin");
const enabled = builtin.os.tag == .macos and @import("build_options").enable_metal;
pub const Command = extern struct {
    kind: u32,
    offset: u32,
    count: u32,
    fill_rule: u32,
    color: u32,
    antialias: u32,
    image_width: u32,
    image_height: u32,
    /// Legacy affine slots; the Metal painter fills conservative paint bounds.
    inverse: [6]f32,
    clip: [4]f32,
};
const Page = extern struct {
    commands: [*]const Command,
    command_count: usize,
    points: [*]const [2]f32,
    point_count: usize,
    pixels: [*]const u8,
    pixel_count: usize,
    output: [*]u8,
    width: u32,
    height: u32,
};
extern fn antfly_pdf_gpu_create() ?*anyopaque;
extern fn antfly_pdf_gpu_destroy(*anyopaque) void;
extern fn antfly_pdf_gpu_cached_bytes(*anyopaque) usize;
extern fn antfly_pdf_gpu_render(*anyopaque, [*]const Page, usize, *f64) c_int;

pub const Scene = struct {
    commands: []Command,
    points: [][2]f32,
    pixels: []u8,
    width: u32,
    height: u32,
    dpi: u16,
    pub fn deinit(self: *Scene, a: std.mem.Allocator) void {
        a.free(self.commands);
        a.free(self.points);
        a.free(self.pixels);
    }
};

/// Same adaptive DPI and source resolution as the exact CPU renderer.
pub fn prepare(a: std.mem.Allocator, parsed: *pdf.reader.Reader, request: pdf.PageRenderRequest) !Scene {
    const rotation = (try parsed.extractPageRotation(request.page_number)) orelse 0;
    if (@mod(rotation, 360) != 0) return error.UnsupportedGpuPdfPage;
    const geometry = try pdf.planParsedPageRenderGeometry(parsed, request);
    var runs = try parsed.extractPageRenderRunsForRasterAlloc(request.page_number, geometry.width, geometry.height);
    defer runs.deinit(parsed.allocator());
    if (runs.text_runs.len != 0 or runs.shading_runs.len != 0 or runs.pattern_runs.len != 0) return error.UnsupportedGpuPdfPage;
    return try prepareRuns(a, runs, geometry);
}

fn prepareRuns(a: std.mem.Allocator, runs: pdf.reader.PageRenderRuns, geometry: pdf.PageRenderGeometry) !Scene {
    if (runs.shape_runs.len + runs.image_runs.len > 1024) return error.UnsupportedGpuPdfPage;
    var commands = std.ArrayList(Command).empty;
    defer commands.deinit(a);
    var points = std.ArrayList([2]f32).empty;
    defer points.deinit(a);
    var pixels = std.ArrayList(u8).empty;
    defer pixels.deinit(a);
    const scale = @as(f64, @floatFromInt(geometry.effective_dpi)) / 72.0;
    const min_x = runs.page_box.min_x * scale;
    // Native rendering snaps the scaled box extent to its integer raster plan.
    // Using the unsnapped PDF max_y shifts scan sampling at fractional origins.
    const max_y = runs.page_box.min_y * scale + @as(f64, @floatFromInt(geometry.height));
    const Entry = struct { image: bool, index: usize, order: usize, phase: usize };
    var entries = std.ArrayList(Entry).empty;
    defer entries.deinit(a);
    for (runs.shape_runs, 0..) |r, i| try entries.append(a, .{ .image = false, .index = i, .order = r.paint_order, .phase = r.paint_phase });
    for (runs.image_runs, 0..) |r, i| try entries.append(a, .{ .image = true, .index = i, .order = r.paint_order, .phase = r.paint_phase });
    std.mem.sort(Entry, entries.items, {}, struct {
        fn less(_: void, l: Entry, r: Entry) bool {
            if (l.order != r.order) return l.order < r.order;
            if (l.phase != r.phase) return l.phase < r.phase;
            // Native renderer places images before shapes for equal order.
            if (l.image != r.image) return l.image;
            return l.index < r.index;
        }
    }.less);
    for (entries.items) |entry| {
        var c: Command = .{ .kind = 0, .offset = 0, .count = 0, .fill_rule = 0, .color = 0, .antialias = 0, .image_width = 0, .image_height = 0, .inverse = @splat(0), .clip = .{ 0, 0, @floatFromInt(geometry.width), @floatFromInt(geometry.height) } };
        if (!entry.image) {
            const r = runs.shape_runs[entry.index];
            if (r.kind != .fill or !r.closed or r.points.len < 3 or r.group_id != null or r.group_parent_id != null or r.blend_mode != .normal or r.color[3] != 255 or r.clip_points != null or r.subpath_starts != null) return error.UnsupportedGpuPdfPage;
            if (points.items.len + r.points.len > 65536) return error.UnsupportedGpuPdfPage;
            c.offset = @intCast(points.items.len);
            c.count = @intCast(r.points.len);
            c.fill_rule = @intFromBool(r.fill_rule == .even_odd);
            c.antialias = @intFromBool(r.antialias);
            c.color = @as(u32, r.color[0]) | (@as(u32, r.color[1]) << 8) | (@as(u32, r.color[2]) << 16) | 0xff000000;
            for (r.points) |p| try points.append(a, .{ try exactCoordinate(p[0] * scale - min_x), try exactCoordinate(max_y - p[1] * scale) });
            if (r.clip_box) |clip| c.clip = try clipBox(clip, min_x, max_y, scale);
        } else {
            const r = runs.image_runs[entry.index];
            for ([_]f64{ r.a, r.b, r.c, r.d, r.e, r.f }) |v| if (!std.math.isFinite(v)) return error.UnsupportedGpuPdfPage;
            // CPU decodes the embedded codec. This prototype accelerates its
            // page transform/painting, never advertises GPU codec decoding.
            if (r.interpolate or r.ocr_coverage_minify or r.stencil_color != null or r.opacity_mask_rgba != null or r.group_id != null or r.group_parent_id != null or r.blend_mode != .normal or r.alpha != 255 or r.clip_points != null or r.width == 0 or r.height == 0) return error.UnsupportedGpuPdfPage;
            if (r.rgba.len != @as(usize, r.width) * r.height * 4 or pixels.items.len + r.rgba.len > 64 * 1024 * 1024) return error.UnsupportedGpuPdfPage;
            var alpha: usize = 3;
            while (alpha < r.rgba.len) : (alpha += 4) if (r.rgba[alpha] != 255) return error.UnsupportedGpuPdfPage;
            // Match CPU f64 nearest-sample selection exactly, using O(width +
            // height) lookup tables. Codec decode stays on CPU; pixel painting
            // stays on GPU. Rotated/sheared images use the CPU fallback.
            if (r.b != 0 or r.c != 0 or r.a <= 0 or r.d <= 0) return error.UnsupportedGpuPdfPage;
            const sa = r.a * scale;
            const sd = r.d * scale;
            const det = sa * sd;
            for ([_]f64{ sa, sd, det, r.e * scale, r.f * scale }) |v| if (!std.math.isFinite(v)) return error.UnsupportedGpuPdfPage;
            if (@abs(det) < 0.000001) return error.UnsupportedGpuPdfPage;
            if (points.items.len + geometry.width + geometry.height > 65536) return error.UnsupportedGpuPdfPage;
            c.count = @intCast(points.items.len);
            for (0..geometry.width) |x| {
                const u = (sd / det) * (min_x + (@as(f64, @floatFromInt(x)) + 0.5) - r.e * scale);
                const sx: f32 = if (u < 0 or u > 1) -1 else @floatFromInt(@min(r.width - 1, @as(u32, @intFromFloat(@floor(u * @as(f64, @floatFromInt(r.width)))))));
                try points.append(a, .{ sx, 0 });
            }
            c.fill_rule = @intCast(points.items.len);
            for (0..geometry.height) |y| {
                const v = (sa / det) * (max_y - (@as(f64, @floatFromInt(y)) + 0.5) - r.f * scale);
                const sy: f32 = if (v < 0 or v > 1) -1 else @floatFromInt(@min(r.height - 1, @as(u32, @intFromFloat(@floor((1.0 - v) * @as(f64, @floatFromInt(r.height)))))));
                try points.append(a, .{ sy, 0 });
            }
            c.kind = 1;
            c.offset = @intCast(pixels.items.len);
            c.image_width = r.width;
            c.image_height = r.height;
            if (r.clip_box) |clip| c.clip = try clipBox(clip, min_x, max_y, scale);
            try pixels.appendSlice(a, r.rgba);
        }
        try commands.append(a, c);
    }
    const owned_commands = try commands.toOwnedSlice(a);
    errdefer a.free(owned_commands);
    const owned_points = try points.toOwnedSlice(a);
    errdefer a.free(owned_points);
    return .{ .commands = owned_commands, .points = owned_points, .pixels = try pixels.toOwnedSlice(a), .width = geometry.width, .height = geometry.height, .dpi = geometry.effective_dpi };
}
fn finiteFloat(v: f64) !f32 {
    const out: f32 = @floatCast(v);
    if (!std.math.isFinite(out)) return error.UnsupportedGpuPdfPage;
    return out;
}
fn exactCoordinate(v: f64) !f32 {
    const f = try finiteFloat(v);
    // Metal lacks f64. Keep the experiment's eligibility conservative rather
    // than allowing a one-pixel edge displacement to pass an MAE-only gate.
    if (@abs(@as(f64, f) - v) > 1e-9) return error.UnsupportedGpuPdfPage;
    return f;
}
fn clipBox(b: pdf.reader.PageBox, min_x: f64, max_y: f64, scale: f64) ![4]f32 {
    return .{ try exactCoordinate(b.min_x * scale - min_x), try exactCoordinate(max_y - b.max_y * scale), try exactCoordinate(b.max_x * scale - min_x), try exactCoordinate(max_y - b.min_y * scale) };
}

pub const Renderer = struct {
    handle: *anyopaque,
    pub fn init() !Renderer {
        if (comptime !enabled) return error.GpuPdfUnavailable;
        return .{ .handle = antfly_pdf_gpu_create() orelse return error.GpuPdfUnavailable };
    }
    pub fn deinit(self: Renderer) void {
        if (comptime enabled) antfly_pdf_gpu_destroy(self.handle);
    }
    pub fn cachedBytes(self: Renderer) usize {
        if (comptime !enabled) return 0;
        return antfly_pdf_gpu_cached_bytes(self.handle);
    }
    /// One command buffer for up to four pages. Output readback is deliberate
    /// for the CPU-renderer A/B prototype, and included in its timed boundary.
    pub fn renderBatch(self: Renderer, a: std.mem.Allocator, scenes: []const Scene, gpu_seconds: *f64) ![]pdf.RenderedPageRaster {
        if (comptime !enabled) return error.GpuPdfUnavailable;
        if (scenes.len == 0 or scenes.len > 4) return error.InvalidGpuPdfBatch;
        var pages: [4]Page = undefined;
        const outputs = try a.alloc(pdf.RenderedPageRaster, scenes.len);
        var initialized: usize = 0;
        errdefer {
            for (outputs[0..initialized]) |*r| r.deinit(a);
            a.free(outputs);
        }
        var retained: usize = 0;
        for (scenes, outputs, 0..) |scene, *output, i| {
            const bytes = std.math.mul(usize, @as(usize, scene.width) * scene.height, 4) catch return error.RenderedPageTooLarge;
            retained = std.math.add(usize, retained, bytes) catch return error.RenderedPageTooLarge;
            if (retained > 256 * 1024 * 1024) return error.RenderedPageTooLarge;
            output.* = .{ .bytes = try a.alloc(u8, bytes), .pixel_format = .rgba8, .width = scene.width, .height = scene.height, .stride = @as(usize, scene.width) * 4, .requested_dpi = scene.dpi, .effective_dpi = scene.dpi };
            initialized += 1;
            pages[i] = .{ .commands = scene.commands.ptr, .command_count = scene.commands.len, .points = scene.points.ptr, .point_count = scene.points.len, .pixels = scene.pixels.ptr, .pixel_count = scene.pixels.len, .output = output.bytes.ptr, .width = scene.width, .height = scene.height };
        }
        if (antfly_pdf_gpu_render(self.handle, &pages, scenes.len, gpu_seconds) != 0) return error.GpuPdfRenderingFailed;
        return outputs;
    }
};

test "GPU PDF preparation rejects unsupported content before submission" {
    const a = std.testing.allocator;
    const geometry = pdf.PageRenderGeometry{ .effective_dpi = 72, .width = 16, .height = 16, .pixels = 256 };
    var points = [_][2]f64{ .{ 1, 1 }, .{ 15, 1 }, .{ 8, 15 } };
    var shapes = [_]pdf.reader.ShapeRun{.{ .kind = .fill, .color = .{ 255, 0, 0, 255 }, .stroke_width = 1, .closed = true, .points = &points }};
    var scene = try prepareRuns(a, .{ .page_box = .{ .min_x = 0, .min_y = 0, .max_x = 16, .max_y = 16 }, .shape_runs = &shapes }, geometry);
    defer scene.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), scene.commands.len);
    shapes[0].kind = .stroke;
    try std.testing.expectError(error.UnsupportedGpuPdfPage, prepareRuns(a, .{ .shape_runs = &shapes }, geometry));
    shapes[0].kind = .fill;
    shapes[0].group_id = 1;
    try std.testing.expectError(error.UnsupportedGpuPdfPage, prepareRuns(a, .{ .shape_runs = &shapes }, geometry));
    shapes[0].group_id = null;
    points[0][0] = std.math.nan(f64);
    try std.testing.expectError(error.UnsupportedGpuPdfPage, prepareRuns(a, .{ .shape_runs = &shapes }, geometry));
}

test "GPU PDF painter batches distinct pages and matches native fill pixels" {
    const a = std.testing.allocator;
    const renderer = Renderer.init() catch return error.SkipZigTest;
    defer renderer.deinit();
    const box = pdf.reader.PageBox{ .min_x = 0, .min_y = 0, .max_x = 16, .max_y = 16 };
    const geometry = pdf.PageRenderGeometry{ .effective_dpi = 72, .width = 16, .height = 16, .pixels = 256 };
    var points = [_][2]f64{ .{ 1, 1 }, .{ 15, 1 }, .{ 8, 15 } };
    var shapes = [_]pdf.reader.ShapeRun{.{ .kind = .fill, .color = .{ 255, 0, 0, 255 }, .stroke_width = 1, .closed = true, .points = &points }};
    var scenes: [2]Scene = undefined;
    scenes[0] = try prepareRuns(a, .{ .page_box = box, .shape_runs = &shapes }, geometry);
    defer scenes[0].deinit(a);
    shapes[0].color = .{ 0, 0, 255, 255 };
    shapes[0].antialias = true;
    scenes[1] = try prepareRuns(a, .{ .page_box = box, .shape_runs = &shapes }, geometry);
    defer scenes[1].deinit(a);
    var gpu_seconds: f64 = 0;
    const outputs = try renderer.renderBatch(a, &scenes, &gpu_seconds);
    defer {
        for (outputs) |*r| r.deinit(a);
        a.free(outputs);
    }
    for (0..2) |i| {
        shapes[0].color = if (i == 0) .{ 255, 0, 0, 255 } else .{ 0, 0, 255, 255 };
        shapes[0].antialias = i != 0;
        const native = try pdf.render.renderPageContentRgbaInBoxRotatedCancelable(a, box, &.{}, &.{}, &.{}, &.{}, &shapes, .none, .{});
        defer a.free(native.rgba);
        try std.testing.expectEqualSlices(u8, native.rgba, outputs[i].bytes);
    }
}

test "GPU PDF image samples match snapped fractional-origin pixels and unsupported transforms reject" {
    const a = std.testing.allocator;
    const box = pdf.reader.PageBox{ .min_x = -0.25, .min_y = -0.25, .max_x = 15.5, .max_y = 15.5 };
    const geometry = pdf.PageRenderGeometry{ .effective_dpi = 72, .width = 16, .height = 16, .pixels = 256 };
    var rgba = [_]u8{ 255, 0, 0, 255, 0, 255, 0, 255, 0, 0, 255, 255, 255, 255, 0, 255 };
    var images = [_]pdf.reader.ImageRun{.{ .rgba = &rgba, .width = 2, .height = 2, .a = 16, .b = 0, .c = 0, .d = 16, .e = 0, .f = 0, .x = 0, .y = 0, .draw_width = 16, .draw_height = 16 }};
    var scene = try prepareRuns(a, .{ .page_box = box, .image_runs = &images }, geometry);
    defer scene.deinit(a);
    images[0].b = 1;
    try std.testing.expectError(error.UnsupportedGpuPdfPage, prepareRuns(a, .{ .page_box = box, .image_runs = &images }, geometry));
    images[0].b = 0;
    images[0].a = std.math.nan(f64);
    try std.testing.expectError(error.UnsupportedGpuPdfPage, prepareRuns(a, .{ .page_box = box, .image_runs = &images }, geometry));
    images[0].a = 16;
    const renderer = Renderer.init() catch return error.SkipZigTest;
    defer renderer.deinit();
    var gpu_seconds: f64 = 0;
    const outputs = try renderer.renderBatch(a, &.{scene}, &gpu_seconds);
    defer {
        for (outputs) |*r| r.deinit(a);
        a.free(outputs);
    }
    const snapped = pdf.reader.PageBox{ .min_x = -0.25, .min_y = -0.25, .max_x = 15.75, .max_y = 15.75 };
    const native = try pdf.render.renderPageContentRgbaInBoxRotatedCancelable(a, snapped, &.{}, &images, &.{}, &.{}, &.{}, .none, .{});
    defer a.free(native.rgba);
    try std.testing.expectEqualSlices(u8, native.rgba, outputs[0].bytes);
}

test "GPU PDF tiles preserve overlapping paint order clipping and reusable buffers" {
    const a = std.testing.allocator;
    const renderer = Renderer.init() catch return error.SkipZigTest;
    defer renderer.deinit();
    const box = pdf.reader.PageBox{ .min_x = 0, .min_y = 0, .max_x = 96, .max_y = 80 };
    const geometry = pdf.PageRenderGeometry{ .effective_dpi = 72, .width = 96, .height = 80, .pixels = 7680 };
    var triangle = [_][2]f64{ .{ 24, 8 }, .{ 72, 8 }, .{ 48, 72 } };
    var rectangle = [_][2]f64{ .{ 30, 0 }, .{ 64, 0 }, .{ 64, 80 }, .{ 30, 80 } };
    var shapes = [_]pdf.reader.ShapeRun{
        .{ .kind = .fill, .color = .{ 255, 0, 0, 255 }, .stroke_width = 1, .closed = true, .points = &triangle, .antialias = true, .paint_order = 1, .clip_box = .{ .min_x = 32.25, .min_y = 20.25, .max_x = 80.75, .max_y = 64.75 } },
        .{ .kind = .fill, .color = .{ 0, 255, 0, 255 }, .stroke_width = 1, .closed = true, .points = &rectangle, .paint_order = 2 },
    };
    var rgba = [_]u8{ 0, 0, 255, 255, 255, 255, 0, 255, 0, 255, 255, 255, 255, 0, 255, 255 };
    var images = [_]pdf.reader.ImageRun{.{ .rgba = &rgba, .width = 2, .height = 2, .a = 96, .b = 0, .c = 0, .d = 80, .e = 0, .f = 0, .x = 0, .y = 0, .draw_width = 96, .draw_height = 80 }};
    var scene = try prepareRuns(a, .{ .page_box = box, .image_runs = &images, .shape_runs = &shapes }, geometry);
    defer scene.deinit(a);
    const native = try pdf.render.renderPageContentRgbaInBoxRotatedCancelable(a, box, &.{}, &images, &.{}, &.{}, &shapes, .none, .{});
    defer a.free(native.rgba);
    var gpu_seconds: f64 = 0;
    for (0..2) |_| {
        const output = try renderer.renderBatch(a, &.{scene}, &gpu_seconds);
        defer {
            for (output) |*r| r.deinit(a);
            a.free(output);
        }
        try std.testing.expectEqualSlices(u8, native.rgba, output[0].bytes);
    }
    const cached = renderer.cachedBytes();
    try std.testing.expect(cached > 0 and cached <= 512 * 1024 * 1024);
    var empty = try prepareRuns(a, .{ .page_box = box }, geometry);
    defer empty.deinit(a);
    const cleared = try renderer.renderBatch(a, &.{empty}, &gpu_seconds);
    defer {
        for (cleared) |*r| r.deinit(a);
        a.free(cleared);
    }
    try std.testing.expectEqual(cached, renderer.cachedBytes());
    for (cleared[0].bytes) |v| try std.testing.expectEqual(@as(u8, 255), v);
}
