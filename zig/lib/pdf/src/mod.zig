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
const builtin = @import("builtin");
const darwin_render = if (builtin.os.tag == .macos) @import("darwin_render.zig") else struct {};

pub const text_encoding = @import("text_encoding.zig");
pub const reader = @import("reader.zig");
pub const syntax = @import("syntax.zig");
pub const render = @import("render.zig");

const Allocator = std.mem.Allocator;
const minimum_direct_render_dpi: u16 = 72;
const minimum_requested_render_dpi: u16 = 72;

pub const RenderedPagePng = struct {
    png: []u8,
    requested_dpi: u16,
    effective_dpi: u16,
    width: u32,
    height: u32,

    pub fn deinit(self: *RenderedPagePng, alloc: Allocator) void {
        alloc.free(self.png);
        self.* = undefined;
    }
};

pub const Backend = struct {
    ptr: *const anyopaque,
    extract_text_fn: *const fn (ptr: *const anyopaque, alloc: Allocator, pdf_bytes: []const u8) anyerror![]u8,
    render_first_page_png_fn: *const fn (ptr: *const anyopaque, alloc: Allocator, pdf_bytes: []const u8) anyerror![]u8,

    pub fn extractText(self: Backend, alloc: Allocator, pdf_bytes: []const u8) ![]u8 {
        return try self.extract_text_fn(self.ptr, alloc, pdf_bytes);
    }

    pub fn renderFirstPagePng(self: Backend, alloc: Allocator, pdf_bytes: []const u8) ![]u8 {
        return try self.render_first_page_png_fn(self.ptr, alloc, pdf_bytes);
    }

    pub fn system() Backend {
        // Keep the existing call sites stable while the backend implementation
        // pivots to pure Zig.
        return native();
    }

    pub fn native() Backend {
        return .{
            .ptr = &native_backend,
            .extract_text_fn = extractTextNative,
            .render_first_page_png_fn = renderFirstPagePngNative,
        };
    }
};

const native_backend: u8 = 0;

fn extractTextNative(_: *const anyopaque, alloc: Allocator, pdf_bytes: []const u8) ![]u8 {
    var parsed = try reader.Reader.init(alloc, pdf_bytes);
    defer parsed.deinit();
    return try parsed.extractPlainTextAlloc();
}

fn renderFirstPagePngNative(_: *const anyopaque, alloc: Allocator, pdf_bytes: []const u8) ![]u8 {
    return try renderPagePngAlloc(alloc, pdf_bytes, 1, 72, 40_000_000);
}

/// Renders a one-based PDF page at the requested raster resolution. Geometry is
/// scaled before rasterization so embedded page images are sampled directly at
/// the target resolution rather than upscaling a 72-DPI preview.
pub fn renderPagePngAlloc(alloc: Allocator, pdf_bytes: []const u8, page_number: usize, dpi: u16, max_pixels: u64) ![]u8 {
    var parsed = try reader.Reader.init(alloc, pdf_bytes);
    defer parsed.deinit();
    return try renderParsedPagePngAlloc(alloc, &parsed, page_number, dpi, max_pixels);
}

pub fn renderParsedPagePngAlloc(alloc: Allocator, parsed: *reader.Reader, page_number: usize, dpi: u16, max_pixels: u64) ![]u8 {
    if (dpi < minimum_requested_render_dpi or dpi > 600) return error.InvalidRenderDpi;
    return try renderParsedPagePngEffectiveAlloc(alloc, parsed, page_number, dpi, max_pixels);
}

fn renderParsedPagePngEffectiveAlloc(alloc: Allocator, parsed: *reader.Reader, page_number: usize, dpi: u16, max_pixels: u64) ![]u8 {
    if (page_number == 0 or page_number > try parsed.pageCount()) return error.InvalidPageNumber;
    const rotation = try normalizedPageRotation(try parsed.extractPageRotation(page_number));
    return try renderParsedPagePngEffectiveWithRotationAlloc(alloc, parsed, page_number, dpi, max_pixels, rotation);
}

fn renderParsedPagePngEffectiveWithRotationAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
) ![]u8 {
    return renderParsedPagePngNativeAlloc(alloc, parsed, page_number, dpi, max_pixels, rotation) catch |err| switch (err) {
        error.UnsupportedStreamFilter,
        error.UnsupportedNativeDecode,
        error.UnsupportedPdfRendering,
        error.InvalidFlateStream,
        error.MissingEndStream,
        error.UnexpectedEof,
        => if (builtin.os.tag == .macos)
            try darwin_render.renderPagePngAlloc(alloc, parsed.sourceBytes(), page_number, dpi, max_pixels, rotation)
        else
            return err,
        else => return err,
    };
}

fn renderParsedPagePngNativeAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
) ![]u8 {
    const reader_alloc = parsed.allocator();
    if (dpi < minimum_direct_render_dpi or dpi > 600) return error.InvalidRenderDpi;
    // Reject oversized pages before decoding page images and font resources.
    const unscaled_box = try parsed.extractPageBox(page_number);
    const scale = @as(f64, @floatFromInt(dpi)) / 72.0;
    const preflight_width = rasterAxisExtent(unscaled_box.min_x, unscaled_box.max_x, scale);
    const preflight_height = rasterAxisExtent(unscaled_box.min_y, unscaled_box.max_y, scale);
    if (preflight_width * preflight_height > @as(f64, @floatFromInt(max_pixels))) return error.RenderedPageTooLarge;
    if (preflight_width > std.math.maxInt(u32) or preflight_height > std.math.maxInt(u32)) return error.RenderedPageTooLarge;
    var render_runs = try parsed.extractPageRenderRunsForRasterAlloc(page_number, @intFromFloat(preflight_width), @intFromFloat(preflight_height));
    defer render_runs.deinit(reader_alloc);
    scalePageRenderRuns(&render_runs, scale);
    alignPageBoxToPixelGrid(&render_runs.page_box);
    const page_box = render_runs.page_box;
    const page_width = @max(1.0, page_box.max_x - page_box.min_x);
    const page_height = @max(1.0, page_box.max_y - page_box.min_y);
    if (page_width * page_height > @as(f64, @floatFromInt(max_pixels))) return error.RenderedPageTooLarge;
    const runs = render_runs.text_runs;
    const image_runs = render_runs.image_runs;
    const shading_runs = render_runs.shading_runs;
    const pattern_runs = render_runs.pattern_runs;
    const shape_runs = render_runs.shape_runs;
    var plain_runs = std.ArrayList(reader.TextRun).empty;
    defer plain_runs.deinit(alloc);
    for (runs) |run| {
        const has_pattern = run.fill_pattern_name != null or run.stroke_pattern_name != null;
        if (has_pattern or run.vectorizable) continue;
        try plain_runs.append(alloc, run);
    }
    std.mem.sort(reader.TextRun, plain_runs.items, {}, struct {
        fn lessThan(_: void, a: reader.TextRun, b: reader.TextRun) bool {
            return a.paint_order < b.paint_order;
        }
    }.lessThan);
    std.mem.sort(reader.ShapeRun, shape_runs, {}, struct {
        fn lessThan(_: void, a: reader.ShapeRun, b: reader.ShapeRun) bool {
            return a.paint_order < b.paint_order;
        }
    }.lessThan);
    std.mem.sort(reader.PatternRun, pattern_runs, {}, struct {
        fn lessThan(_: void, a: reader.PatternRun, b: reader.PatternRun) bool {
            return a.paint_order < b.paint_order;
        }
    }.lessThan);
    return try render.renderPageContentPngInBoxRotated(alloc, page_box, plain_runs.items, image_runs, shading_runs, pattern_runs, shape_runs, rotation);
}

fn normalizedPageRotation(rotation: ?i32) !render.PageRotation {
    const normalized = @mod(rotation orelse 0, 360);
    return switch (normalized) {
        0 => .none,
        90 => .clockwise_90,
        180 => .clockwise_180,
        270 => .clockwise_270,
        else => error.InvalidPageRotation,
    };
}

/// Renders at the requested DPI when safe, reducing it only enough to satisfy
/// both the dimension and pixel guards without crossing the 72-DPI quality
/// floor. Pages that cannot fit safely at that floor fail explicitly rather
/// than silently producing an OCR input below the documented minimum.
pub fn renderParsedPagePngAdaptiveAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    max_dimension: u32,
) !RenderedPagePng {
    if (page_number == 0) return error.InvalidPageNumber;
    if (requested_dpi < 72 or requested_dpi > 600) return error.InvalidRenderDpi;
    if (max_pixels == 0 or max_dimension == 0) return error.RenderedPageTooLarge;
    const page_count = try parsed.pageCount();
    if (page_number > page_count) return error.InvalidPageNumber;
    const box = try parsed.extractPageBox(page_number);
    const rotation = try normalizedPageRotation(try parsed.extractPageRotation(page_number));
    const swaps_dimensions = rotation == .clockwise_90 or rotation == .clockwise_270;

    var effective_dpi = requested_dpi;
    var width: u32 = 0;
    var height: u32 = 0;
    while (true) {
        const scale = @as(f64, @floatFromInt(effective_dpi)) / 72.0;
        const unrotated_width = rasterAxisExtent(box.min_x, box.max_x, scale);
        const unrotated_height = rasterAxisExtent(box.min_y, box.max_y, scale);
        const width_f = if (swaps_dimensions) unrotated_height else unrotated_width;
        const height_f = if (swaps_dimensions) unrotated_width else unrotated_height;
        const fits_integer = width_f <= @as(f64, @floatFromInt(std.math.maxInt(u32))) and
            height_f <= @as(f64, @floatFromInt(std.math.maxInt(u32)));
        if (fits_integer) {
            width = @intFromFloat(width_f);
            height = @intFromFloat(height_f);
            const pixels = @as(u64, width) * @as(u64, height);
            if (width <= max_dimension and height <= max_dimension and pixels <= max_pixels) break;
        }
        if (effective_dpi == minimum_direct_render_dpi) return error.RenderedPageTooLarge;
        effective_dpi -= 1;
    }

    return .{
        .png = try renderParsedPagePngEffectiveWithRotationAlloc(alloc, parsed, page_number, effective_dpi, max_pixels, rotation),
        .requested_dpi = requested_dpi,
        .effective_dpi = effective_dpi,
        .width = width,
        .height = height,
    };
}

fn scaleBox(box: *reader.PageBox, scale: f64) void {
    box.min_x *= scale;
    box.min_y *= scale;
    box.max_x *= scale;
    box.max_y *= scale;
}

fn scalePoints(points: ?[]const [2]f64, scale: f64) void {
    if (points) |items| for (@constCast(items)) |*point| {
        point[0] *= scale;
        point[1] *= scale;
    };
}

fn scaleTextRuns(runs: []reader.TextRun, scale: f64) void {
    for (runs) |*run| {
        run.x *= scale;
        run.y *= scale;
        run.font_size *= scale;
        run.stroke_width *= scale;
        run.char_spacing *= scale;
        run.word_spacing *= scale;
        run.advance_width *= scale;
        run.ascent *= scale;
        run.descent *= scale;
        if (run.clip_box) |*box| scaleBox(box, scale);
        scalePoints(run.clip_points, scale);
    }
}

fn scaleImageRuns(runs: []reader.ImageRun, scale: f64) void {
    for (runs) |*run| {
        run.a *= scale;
        run.b *= scale;
        run.c *= scale;
        run.d *= scale;
        run.e *= scale;
        run.f *= scale;
        run.x *= scale;
        run.y *= scale;
        run.draw_width *= scale;
        run.draw_height *= scale;
        if (run.clip_box) |*box| scaleBox(box, scale);
        scalePoints(run.clip_points, scale);
    }
}

fn scaleShadingRuns(runs: []reader.ShadingRun, scale: f64) void {
    for (runs) |*run| {
        run.x0 *= scale;
        run.y0 *= scale;
        run.r0 *= scale;
        run.x1 *= scale;
        run.y1 *= scale;
        run.r1 *= scale;
        if (run.clip_box) |*box| scaleBox(box, scale);
        scalePoints(run.clip_points, scale);
    }
}

fn scaleShapeRuns(runs: []reader.ShapeRun, scale: f64) void {
    for (runs) |*run| {
        run.stroke_width *= scale;
        run.dash_phase *= scale;
        if (run.dash_array) |dash| {
            for (dash) |*value| value.* *= scale;
        }
        for (run.points) |*point| {
            point[0] *= scale;
            point[1] *= scale;
        }
        if (run.clip_box) |*box| scaleBox(box, scale);
        scalePoints(run.clip_points, scale);
    }
}

fn scalePatternRuns(runs: []reader.PatternRun, scale: f64) void {
    for (runs) |*run| {
        run.stroke_width *= scale;
        run.dash_phase *= scale;
        if (run.dash_array) |dash| {
            for (dash) |*value| value.* *= scale;
        }
        for (run.points) |*point| {
            point[0] *= scale;
            point[1] *= scale;
        }
        if (run.clip_box) |*box| scaleBox(box, scale);
        scalePoints(run.clip_points, scale);
        // Tiling geometry and tile-local runs remain in pattern space. The
        // pattern matrix is the single mapping into the scaled page space;
        // scaling both produced tiles that grew by scale^2 at higher DPI.
        run.pattern_matrix.a *= scale;
        run.pattern_matrix.b *= scale;
        run.pattern_matrix.c *= scale;
        run.pattern_matrix.d *= scale;
        run.pattern_matrix.e *= scale;
        run.pattern_matrix.f *= scale;
        if (run.shading) |*shading| scaleShadingRuns(@as(*[1]reader.ShadingRun, @ptrCast(shading))[0..], scale);
    }
}

fn scalePageRenderRuns(runs: *reader.PageRenderRuns, scale: f64) void {
    scaleBox(&runs.page_box, scale);
    scaleTextRuns(runs.text_runs, scale);
    scaleImageRuns(runs.image_runs, scale);
    scaleShadingRuns(runs.shading_runs, scale);
    scalePatternRuns(runs.pattern_runs, scale);
    scaleShapeRuns(runs.shape_runs, scale);
}

fn rasterAxisExtent(min_value: f64, max_value: f64, scale: f64) f64 {
    return @max(1.0, @ceil((max_value - min_value) * scale));
}

fn alignPageBoxToPixelGrid(box: *reader.PageBox) void {
    box.max_x = box.min_x + @max(1.0, @ceil(box.max_x - box.min_x));
    box.max_y = box.min_y + @max(1.0, @ceil(box.max_y - box.min_y));
}

test "raster extents include fractional crop-box edges" {
    const box: reader.PageBox = .{
        .min_x = 0.720001,
        .min_y = 0.479996,
        .max_x = 595.92,
        .max_y = 842.16,
    };
    const scale = 150.0 / 72.0;
    try std.testing.expectEqual(@as(f64, 1240), rasterAxisExtent(box.min_x, box.max_x, scale));
    try std.testing.expectEqual(@as(f64, 1754), rasterAxisExtent(box.min_y, box.max_y, scale));

    var scaled = box;
    scaleBox(&scaled, scale);
    alignPageBoxToPixelGrid(&scaled);
    try std.testing.expectApproxEqAbs(1.500002083, scaled.min_x, 0.000001);
    try std.testing.expectApproxEqAbs(0.999991667, scaled.min_y, 0.000001);
    try std.testing.expectApproxEqAbs(1241.500002083, scaled.max_x, 0.000001);
    try std.testing.expectApproxEqAbs(1754.999991667, scaled.max_y, 0.000001);
}

fn dupTextRunAlloc(alloc: Allocator, run: reader.TextRun) !reader.TextRun {
    var out = run;
    out.text = &.{};
    out.raw_text = null;
    out.fill_pattern_name = null;
    out.stroke_pattern_name = null;
    out.clip_points = null;
    errdefer out.deinit(alloc);

    out.text = try alloc.dupe(u8, run.text);
    if (run.raw_text) |raw| out.raw_text = try alloc.dupe(u8, raw);
    if (run.fill_pattern_name) |name| out.fill_pattern_name = try alloc.dupe(u8, name);
    if (run.stroke_pattern_name) |name| out.stroke_pattern_name = try alloc.dupe(u8, name);
    if (run.clip_points) |points| out.clip_points = try alloc.dupe([2]f64, points);
    return out;
}

fn dupImageRunAlloc(alloc: Allocator, run: reader.ImageRun) !reader.ImageRun {
    var out = run;
    out.rgba = &.{};
    out.clip_points = null;
    errdefer out.deinit(alloc);

    out.rgba = try alloc.dupe(u8, run.rgba);
    if (run.clip_points) |points| out.clip_points = try alloc.dupe([2]f64, points);
    return out;
}

fn dupShadingRunAlloc(alloc: Allocator, run: reader.ShadingRun) !reader.ShadingRun {
    var out = run;
    out.clip_points = null;
    errdefer out.deinit(alloc);

    if (run.clip_points) |points| out.clip_points = try alloc.dupe([2]f64, points);
    return out;
}

fn dupShapeRunAlloc(alloc: Allocator, run: reader.ShapeRun) !reader.ShapeRun {
    var out = run;
    out.dash_array = null;
    out.clip_points = null;
    out.points = &.{};
    out.subpath_starts = null;
    errdefer out.deinit(alloc);

    if (run.dash_array) |dash| out.dash_array = try alloc.dupe(f64, dash);
    if (run.clip_points) |points| out.clip_points = try alloc.dupe([2]f64, points);
    out.points = try alloc.dupe([2]f64, run.points);
    if (run.subpath_starts) |starts| out.subpath_starts = try alloc.dupe(usize, starts);
    return out;
}

fn dupPatternRunAlloc(alloc: Allocator, run: reader.PatternRun) !reader.PatternRun {
    var out = run;
    out.dash_array = null;
    out.clip_points = null;
    out.points = &.{};
    out.subpath_starts = null;
    out.shading = null;
    out.tile_text_runs = &.{};
    out.tile_image_runs = &.{};
    out.tile_shading_runs = &.{};
    out.tile_pattern_runs = &.{};
    out.tile_shape_runs = &.{};
    errdefer out.deinit(alloc);

    if (run.dash_array) |dash| out.dash_array = try alloc.dupe(f64, dash);
    if (run.clip_points) |points| out.clip_points = try alloc.dupe([2]f64, points);
    out.points = try alloc.dupe([2]f64, run.points);
    if (run.subpath_starts) |starts| out.subpath_starts = try alloc.dupe(usize, starts);
    if (run.shading) |shading| out.shading = try dupShadingRunAlloc(alloc, shading);

    if (run.tile_text_runs.len > 0) {
        var list = try std.ArrayList(reader.TextRun).initCapacity(alloc, run.tile_text_runs.len);
        defer list.deinit(alloc);
        errdefer for (list.items) |*item| item.deinit(alloc);
        for (run.tile_text_runs) |item| {
            var cloned = try dupTextRunAlloc(alloc, item);
            errdefer cloned.deinit(alloc);
            list.appendAssumeCapacity(cloned);
        }
        out.tile_text_runs = try list.toOwnedSlice(alloc);
    }
    if (run.tile_image_runs.len > 0) {
        var list = try std.ArrayList(reader.ImageRun).initCapacity(alloc, run.tile_image_runs.len);
        defer list.deinit(alloc);
        errdefer for (list.items) |*item| item.deinit(alloc);
        for (run.tile_image_runs) |item| {
            var cloned = try dupImageRunAlloc(alloc, item);
            errdefer cloned.deinit(alloc);
            list.appendAssumeCapacity(cloned);
        }
        out.tile_image_runs = try list.toOwnedSlice(alloc);
    }
    if (run.tile_shading_runs.len > 0) {
        var list = try std.ArrayList(reader.ShadingRun).initCapacity(alloc, run.tile_shading_runs.len);
        defer list.deinit(alloc);
        errdefer for (list.items) |*item| item.deinit(alloc);
        for (run.tile_shading_runs) |item| {
            var cloned = try dupShadingRunAlloc(alloc, item);
            errdefer cloned.deinit(alloc);
            list.appendAssumeCapacity(cloned);
        }
        out.tile_shading_runs = try list.toOwnedSlice(alloc);
    }
    if (run.tile_pattern_runs.len > 0) {
        var list = try std.ArrayList(reader.PatternRun).initCapacity(alloc, run.tile_pattern_runs.len);
        defer list.deinit(alloc);
        errdefer for (list.items) |*item| item.deinit(alloc);
        for (run.tile_pattern_runs) |item| {
            var cloned = try dupPatternRunAlloc(alloc, item);
            errdefer cloned.deinit(alloc);
            list.appendAssumeCapacity(cloned);
        }
        out.tile_pattern_runs = try list.toOwnedSlice(alloc);
    }
    if (run.tile_shape_runs.len > 0) {
        var list = try std.ArrayList(reader.ShapeRun).initCapacity(alloc, run.tile_shape_runs.len);
        defer list.deinit(alloc);
        errdefer for (list.items) |*item| item.deinit(alloc);
        for (run.tile_shape_runs) |item| {
            var cloned = try dupShapeRunAlloc(alloc, item);
            errdefer cloned.deinit(alloc);
            list.appendAssumeCapacity(cloned);
        }
        out.tile_shape_runs = try list.toOwnedSlice(alloc);
    }
    return out;
}

test "PDF render pattern cloning is allocation-failure safe" {
    const Runner = struct {
        fn run(alloc: Allocator) !void {
            var pattern_dash = [_]f64{ 1, 2 };
            var pattern_clip = [_][2]f64{ .{ 0, 0 }, .{ 8, 8 } };
            var pattern_points = [_][2]f64{ .{ 0, 0 }, .{ 8, 0 }, .{ 8, 8 }, .{ 0, 8 } };
            var text_clip = [_][2]f64{ .{ 1, 1 }, .{ 2, 2 } };
            var tile_text = [_]reader.TextRun{.{
                .text = "visible",
                .raw_text = "raw",
                .x = 1,
                .y = 2,
                .font_size = 12,
                .fill_pattern_name = "fill-pattern",
                .stroke_pattern_name = "stroke-pattern",
                .clip_points = &text_clip,
            }};
            var rgba = [_]u8{ 0, 64, 128, 255 };
            var image_clip = [_][2]f64{ .{ 2, 2 }, .{ 3, 3 } };
            var tile_images = [_]reader.ImageRun{.{
                .rgba = &rgba,
                .width = 1,
                .height = 1,
                .clip_points = &image_clip,
                .a = 1,
                .b = 0,
                .c = 0,
                .d = 1,
                .e = 0,
                .f = 0,
                .x = 0,
                .y = 0,
                .draw_width = 1,
                .draw_height = 1,
            }};
            var shading_clip = [_][2]f64{ .{ 3, 3 }, .{ 4, 4 } };
            var tile_shadings = [_]reader.ShadingRun{.{
                .kind = .axial,
                .clip_points = &shading_clip,
                .x0 = 0,
                .y0 = 0,
                .x1 = 8,
                .y1 = 8,
                .c0 = .{ 0, 0, 0, 255 },
                .c1 = .{ 255, 255, 255, 255 },
            }};
            var shape_dash = [_]f64{ 3, 4 };
            var shape_clip = [_][2]f64{ .{ 4, 4 }, .{ 5, 5 } };
            var shape_points = [_][2]f64{ .{ 0, 0 }, .{ 4, 4 } };
            var tile_shapes = [_]reader.ShapeRun{.{
                .kind = .stroke,
                .dash_array = &shape_dash,
                .color = .{ 0, 0, 0, 255 },
                .stroke_width = 1,
                .closed = false,
                .clip_points = &shape_clip,
                .points = &shape_points,
            }};
            var nested_points = [_][2]f64{ .{ 0, 0 }, .{ 2, 2 } };
            var nested_patterns = [_]reader.PatternRun{.{
                .kind = .fill,
                .points = &nested_points,
                .pattern_bbox = .{ .min_x = 0, .min_y = 0, .max_x = 2, .max_y = 2 },
                .pattern_x_step = 2,
                .pattern_y_step = 2,
            }};
            const source = reader.PatternRun{
                .kind = .fill,
                .dash_array = &pattern_dash,
                .clip_points = &pattern_clip,
                .points = &pattern_points,
                .pattern_bbox = .{ .min_x = 0, .min_y = 0, .max_x = 8, .max_y = 8 },
                .pattern_x_step = 8,
                .pattern_y_step = 8,
                .shading = tile_shadings[0],
                .tile_text_runs = &tile_text,
                .tile_image_runs = &tile_images,
                .tile_shading_runs = &tile_shadings,
                .tile_pattern_runs = &nested_patterns,
                .tile_shape_runs = &tile_shapes,
            };

            var cloned = try dupPatternRunAlloc(alloc, source);
            defer cloned.deinit(alloc);
            try std.testing.expectEqualStrings("fill-pattern", cloned.tile_text_runs[0].fill_pattern_name.?);
            try std.testing.expect(cloned.tile_text_runs[0].fill_pattern_name.?.ptr != source.tile_text_runs[0].fill_pattern_name.?.ptr);
        }
    };

    try std.testing.checkAllAllocationFailures(std.testing.allocator, Runner.run, .{});
}

fn buildRotatedTestPdfAlloc(alloc: Allocator, rotation: i32) ![]u8 {
    const content = "0 0 10 10 re f\n";
    const pages_object = try std.fmt.allocPrint(alloc, "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 /MediaBox [0 0 20 30] /Rotate {d} >>\nendobj\n", .{rotation});
    defer alloc.free(pages_object);
    const content_object = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(content_object);
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        pages_object,
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\nendobj\n",
        content_object,
    };

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |object, index| {
        offsets[index] = out.items.len;
        try out.appendSlice(alloc, object);
    }
    const xref_offset = out.items.len;
    try out.appendSlice(alloc, "xref\n0 5\n0000000000 65535 f \n");
    for (offsets) |offset| {
        const entry = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{offset});
        defer alloc.free(entry);
        try out.appendSlice(alloc, entry);
    }
    const trailer = try std.fmt.allocPrint(alloc, "trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n{d}\n%%EOF\n", .{xref_offset});
    defer alloc.free(trailer);
    try out.appendSlice(alloc, trailer);
    return try out.toOwnedSlice(alloc);
}

test "page rotation normalization accepts equivalent quarter turns" {
    try std.testing.expectEqual(render.PageRotation.none, try normalizedPageRotation(null));
    try std.testing.expectEqual(render.PageRotation.clockwise_90, try normalizedPageRotation(450));
    try std.testing.expectEqual(render.PageRotation.clockwise_270, try normalizedPageRotation(-90));
    try std.testing.expectError(error.InvalidPageRotation, normalizedPageRotation(45));
}

test "native and adaptive page rendering honor inherited rotation" {
    const alloc = std.testing.allocator;
    const fixture = try buildRotatedTestPdfAlloc(alloc, 90);
    defer alloc.free(fixture);

    const png = try renderPagePngAlloc(alloc, fixture, 1, 72, 40_000_000);
    defer alloc.free(png);
    const decoded = try @import("antfly_image").png.decodeRgba(alloc, png);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqual(@as(u32, 30), decoded.width);
    try std.testing.expectEqual(@as(u32, 20), decoded.height);

    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    var adaptive = try renderParsedPagePngAdaptiveAlloc(alloc, &parsed, 1, 150, 40_000_000, 40);
    defer adaptive.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 96), adaptive.effective_dpi);
    try std.testing.expectEqual(@as(u32, 40), adaptive.width);
    try std.testing.expectEqual(@as(u32, 27), adaptive.height);
    const adaptive_decoded = try @import("antfly_image").png.decodeRgba(alloc, adaptive.png);
    defer alloc.free(adaptive_decoded.rgba);
    try std.testing.expectEqual(adaptive.width, adaptive_decoded.width);
    try std.testing.expectEqual(adaptive.height, adaptive_decoded.height);
}

fn encryptType1EexecAlloc(alloc: Allocator, plain: []const u8) ![]u8 {
    const prefix = [_]u8{ 0, 0, 0, 0 };
    var out = try alloc.alloc(u8, prefix.len + plain.len);
    var r: u16 = 55665;
    for (prefix, 0..) |value, i| {
        const cipher = value ^ @as(u8, @truncate(r >> 8));
        out[i] = cipher;
        r = @truncate((@as(u32, cipher) + r) * 52845 + 22719);
    }
    for (plain, 0..) |value, i| {
        const cipher = value ^ @as(u8, @truncate(r >> 8));
        out[prefix.len + i] = cipher;
        r = @truncate((@as(u32, cipher) + r) * 52845 + 22719);
    }
    return out;
}

test "mock pdf backend interface compiles" {
    const Mock = struct {
        fn extract(_: *const anyopaque, alloc: Allocator, _: []const u8) ![]u8 {
            return try alloc.dupe(u8, "pdf text");
        }

        fn render(_: *const anyopaque, alloc: Allocator, _: []const u8) ![]u8 {
            return try alloc.dupe(u8, "png");
        }
    };

    const backend = Backend{
        .ptr = undefined,
        .extract_text_fn = Mock.extract,
        .render_first_page_png_fn = Mock.render,
    };

    const alloc = std.testing.allocator;
    const text = try backend.extractText(alloc, "pdf");
    defer alloc.free(text);
    const png = try backend.renderFirstPagePng(alloc, "pdf");
    defer alloc.free(png);

    try std.testing.expectEqualStrings("pdf text", text);
    try std.testing.expectEqualStrings("png", png);
}

test "native backend extracts simple pdf text" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello World) Tj\nET\n";

    const obj1 =
        "1 0 obj\n" ++
        "<< /Type /Catalog /Pages 2 0 R >>\n" ++
        "endobj\n";
    const obj2 =
        "2 0 obj\n" ++
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>\n" ++
        "endobj\n";
    const obj3 =
        "3 0 obj\n" ++
        "<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\n" ++
        "endobj\n";
    const obj4_prefix =
        "4 0 obj\n" ++
        "<< /Length ";
    const obj4_suffix =
        " >>\n" ++
        "stream\n";
    const obj4_end =
        "endstream\n" ++
        "endobj\n";

    const len_str = try std.fmt.allocPrint(alloc, "{d}", .{content.len});
    defer alloc.free(len_str);
    const obj4 = try std.mem.concat(alloc, u8, &.{ obj4_prefix, len_str, obj4_suffix, content, obj4_end });
    defer alloc.free(obj4);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);

    try out.appendSlice(alloc, "%PDF-1.4\n");

    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n" ++
        "<< /Root 1 0 R /Size 5 >>\n" ++
        "startxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const text = try backend.extractText(alloc, out.items);
    defer alloc.free(text);
    try std.testing.expectEqualStrings("Hello World\n", text);
}

test "native backend extracts text from embedded fixture pdf" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");
    const backend = Backend.native();
    const text = try backend.extractText(alloc, fixture);
    defer alloc.free(text);
    try std.testing.expectEqualStrings("Hello Fixture\n", text);
}

test "native backend renders simple pdf first page png" {
    const alloc = std.testing.allocator;
    const content = "BT\n(Hello World) Tj\nET\n";

    const obj1 =
        "1 0 obj\n" ++
        "<< /Type /Catalog /Pages 2 0 R >>\n" ++
        "endobj\n";
    const obj2 =
        "2 0 obj\n" ++
        "<< /Type /Pages /Kids [3 0 R] /Count 1 >>\n" ++
        "endobj\n";
    const obj3 =
        "3 0 obj\n" ++
        "<< /Type /Page /Parent 2 0 R /Contents 4 0 R >>\n" ++
        "endobj\n";
    const obj4 = try std.fmt.allocPrint(
        alloc,
        "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n",
        .{ content.len, content },
    );
    defer alloc.free(obj4);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");

    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
    const ocr_png = try renderPagePngAlloc(alloc, out.items, 1, 150, 40_000_000);
    defer alloc.free(ocr_png);
    const native_page = try @import("antfly_image").png.decodeRgba(alloc, png);
    defer alloc.free(native_page.rgba);
    const ocr_page = try @import("antfly_image").png.decodeRgba(alloc, ocr_png);
    defer alloc.free(ocr_page.rgba);
    try std.testing.expect(ocr_page.width > native_page.width);
    try std.testing.expect(ocr_page.height > native_page.height);
    try std.testing.expectError(error.RenderedPageTooLarge, renderPagePngAlloc(alloc, out.items, 1, 150, 10));
    try std.testing.expectError(error.InvalidPageNumber, renderPagePngAlloc(alloc, out.items, 2, 150, 40_000_000));
}

test "native backend renders embedded fixture pdf first page png" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");
    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, fixture);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "parsed rendering releases reader-owned runs with the reader allocator" {
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");

    var reader_gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(reader_gpa.deinit() == .ok);
    var output_gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(output_gpa.deinit() == .ok);

    var parsed = try reader.Reader.init(reader_gpa.allocator(), fixture);
    defer parsed.deinit();
    const png = try renderParsedPagePngAlloc(output_gpa.allocator(), &parsed, 1, 150, 40_000_000);
    defer output_gpa.allocator().free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders type1 cleartext fixture pdf first page png" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/type1_cleartext_fixture.pdf");
    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, fixture);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders type1 eexec fixture pdf first page png" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/type1_eexec_fixture.pdf");
    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, fixture);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders simple image xobject pdf first page png" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const content = "q\n10 0 0 10 20 30 cm\n/Im1 Do\nQ\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = try std.fmt.allocPrint(
        alloc,
        "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
        .{ image_data.len, image_data },
    );
    defer alloc.free(obj5);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");

    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 6\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 6 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
    const scanned_ocr_png = try renderPagePngAlloc(alloc, out.items, 1, 150, 40_000_000);
    defer alloc.free(scanned_ocr_png);
    const scanned_native_page = try @import("antfly_image").png.decodeRgba(alloc, png);
    defer alloc.free(scanned_native_page.rgba);
    const scanned_ocr_page = try @import("antfly_image").png.decodeRgba(alloc, scanned_ocr_png);
    defer alloc.free(scanned_ocr_page.rgba);
    try std.testing.expect(scanned_ocr_page.width > scanned_native_page.width);
    try std.testing.expect(scanned_ocr_page.height > scanned_native_page.height);
}

test "native backend renders Type3 text glyphs through shape path" {
    const alloc = std.testing.allocator;
    const content =
        "BT\n" ++
        "/F1 20 Tf\n" ++
        "1 0 0 rg\n" ++
        "10 20 Td\n" ++
        "(A) Tj\n" ++
        "ET\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 =
        "5 0 obj\n" ++
        "<< /Type /Font /Subtype /Type3 /PaintType 2 /FontMatrix [0.001 0 0 0.001 0 0] /Encoding << /Differences [65 /A] >> /FirstChar 65 /LastChar 65 /Widths [1000] /CharProcs << /A 6 0 R >> >>\n" ++
        "endobj\n";
    const glyph_content = "0 0 1000 1000 re\nf\n";
    const obj6 = try std.fmt.allocPrint(alloc, "6 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ glyph_content.len, glyph_content });
    defer alloc.free(obj6);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 7\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 7 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders rotated image xobject pdf first page png" {
    const alloc = std.testing.allocator;
    const image_data = &.{ 255, 0, 0 };
    const content = "q\n0 10 -10 0 20 30 cm\n/Im1 Do\nQ\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /Resources << /XObject << /Im1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = try std.fmt.allocPrint(
        alloc,
        "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 1 /Height 1 /ColorSpace /DeviceRGB /BitsPerComponent 8 /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
        .{ image_data.len, image_data },
    );
    defer alloc.free(obj5);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");

    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 6\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 6 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders simple filled rectangle pdf first page png" {
    const alloc = std.testing.allocator;
    const content = "1 0 0 rg\n10 20 30 40 re\nf\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 5\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 5 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

fn appendU16Be(alloc: Allocator, out: *std.ArrayList(u8), value: u16) !void {
    try out.appendSlice(alloc, &std.mem.toBytes(std.mem.nativeToBig(u16, value)));
}

fn appendI16Be(alloc: Allocator, out: *std.ArrayList(u8), value: i16) !void {
    try appendU16Be(alloc, out, @bitCast(value));
}

fn appendU32Be(alloc: Allocator, out: *std.ArrayList(u8), value: u32) !void {
    try out.appendSlice(alloc, &std.mem.toBytes(std.mem.nativeToBig(u32, value)));
}

fn pad4(alloc: Allocator, out: *std.ArrayList(u8)) !void {
    while ((out.items.len % 4) != 0) try out.append(alloc, 0);
}

fn buildSimpleTrueTypeFontAlloc(alloc: Allocator) ![]u8 {
    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16Be(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16Be(alloc, &head, 0);
    try appendU16Be(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32Be(alloc, &maxp, 0x00010000);
    try appendU16Be(alloc, &maxp, 3);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16Be(alloc, &hhea, 3);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16Be(alloc, &hmtx, 500);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1600);
    try appendI16Be(alloc, &hmtx, 0);

    var glyph = std.ArrayList(u8).empty;
    defer glyph.deinit(alloc);
    try appendI16Be(alloc, &glyph, 1);
    try appendI16Be(alloc, &glyph, 0);
    try appendI16Be(alloc, &glyph, 0);
    try appendI16Be(alloc, &glyph, 1000);
    try appendI16Be(alloc, &glyph, 1000);
    try appendU16Be(alloc, &glyph, 2);
    try appendU16Be(alloc, &glyph, 0);
    try glyph.appendSlice(alloc, &.{ 0x31, 0x21, 0x01 });
    try appendI16Be(alloc, &glyph, 1000);
    try appendI16Be(alloc, &glyph, -500);
    try appendI16Be(alloc, &glyph, 1000);
    if ((glyph.items.len % 2) != 0) try glyph.append(alloc, 0);

    var composite = std.ArrayList(u8).empty;
    defer composite.deinit(alloc);
    try appendI16Be(alloc, &composite, -1);
    try appendI16Be(alloc, &composite, 0);
    try appendI16Be(alloc, &composite, 0);
    try appendI16Be(alloc, &composite, 1600);
    try appendI16Be(alloc, &composite, 1000);
    try appendU16Be(alloc, &composite, 0x0023);
    try appendU16Be(alloc, &composite, 1);
    try appendI16Be(alloc, &composite, 0);
    try appendI16Be(alloc, &composite, 0);
    try appendU16Be(alloc, &composite, 0x0003);
    try appendU16Be(alloc, &composite, 1);
    try appendI16Be(alloc, &composite, 600);
    try appendI16Be(alloc, &composite, 0);
    if ((composite.items.len % 2) != 0) try composite.append(alloc, 0);

    var loca = std.ArrayList(u8).empty;
    defer loca.deinit(alloc);
    try appendU16Be(alloc, &loca, 0);
    try appendU16Be(alloc, &loca, 0);
    try appendU16Be(alloc, &loca, @intCast(glyph.items.len / 2));
    try appendU16Be(alloc, &loca, @intCast((glyph.items.len + composite.items.len) / 2));

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 3);
    try appendU16Be(alloc, &cmap, 1);
    try appendU32Be(alloc, &cmap, 12);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 32);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 66);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 0);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'g', 'l', 'y', 'f' }, .bytes = &.{} },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'l', 'o', 'c', 'a' }, .bytes = loca.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try appendU32Be(alloc, &out, 0x00010000);
    try appendU16Be(alloc, &out, tables.len);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    const dir_offset = out.items.len;
    try out.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &out);
        const table_offset: u32 = @intCast(out.items.len);
        if (std.mem.eql(u8, &table.tag, "glyf")) {
            try out.appendSlice(alloc, glyph.items);
            try out.appendSlice(alloc, composite.items);
        } else {
            try out.appendSlice(alloc, table.bytes);
        }
        try pad4(alloc, &out);
        const base = dir_offset + i * 16;
        out.items[base + 0] = table.tag[0];
        out.items[base + 1] = table.tag[1];
        out.items[base + 2] = table.tag[2];
        out.items[base + 3] = table.tag[3];
        out.items[base + 4] = 0;
        out.items[base + 5] = 0;
        out.items[base + 6] = 0;
        out.items[base + 7] = 0;
        out.items[base + 8] = @intCast((table_offset >> 24) & 0xff);
        out.items[base + 9] = @intCast((table_offset >> 16) & 0xff);
        out.items[base + 10] = @intCast((table_offset >> 8) & 0xff);
        out.items[base + 11] = @intCast(table_offset & 0xff);
        const table_len: u32 = if (std.mem.eql(u8, &table.tag, "glyf"))
            @intCast(glyph.items.len + composite.items.len)
        else
            @intCast(table.bytes.len);
        out.items[base + 12] = @intCast((table_len >> 24) & 0xff);
        out.items[base + 13] = @intCast((table_len >> 16) & 0xff);
        out.items[base + 14] = @intCast((table_len >> 8) & 0xff);
        out.items[base + 15] = @intCast(table_len & 0xff);
    }

    return try out.toOwnedSlice(alloc);
}

fn buildSimpleOpenTypeCffFontAlloc(alloc: Allocator) ![]u8 {
    const cff_bytes = &[_]u8{
        1,   0,   4,   1,
        0,   1,   1,   1,
        5,   'T', 'e', 's',
        't', 0,   1,   1,
        1,   5,   190, 15,
        165, 17,  0,   0,
        0,   0,   0,   2,
        1,   1,   2,   20,
        14,  139, 139, 21,
        247, 124, 139, 5,
        251, 124, 250, 124,
        5,   251, 124, 251,
        124, 5,   14,  0,
        0,   1,
    };

    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16Be(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16Be(alloc, &head, 0);
    try appendU16Be(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32Be(alloc, &maxp, 0x00010000);
    try appendU16Be(alloc, &maxp, 2);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16Be(alloc, &hhea, 2);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16Be(alloc, &hmtx, 500);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 3);
    try appendU16Be(alloc, &cmap, 1);
    try appendU32Be(alloc, &cmap, 12);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 32);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 0);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'C', 'F', 'F', ' ' }, .bytes = cff_bytes },
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "OTTO");
    try appendU16Be(alloc, &out, tables.len);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    const dir_offset = out.items.len;
    try out.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &out);
        const table_offset: u32 = @intCast(out.items.len);
        try out.appendSlice(alloc, table.bytes);
        try pad4(alloc, &out);
        const base = dir_offset + i * 16;
        out.items[base + 0] = table.tag[0];
        out.items[base + 1] = table.tag[1];
        out.items[base + 2] = table.tag[2];
        out.items[base + 3] = table.tag[3];
        out.items[base + 4] = 0;
        out.items[base + 5] = 0;
        out.items[base + 6] = 0;
        out.items[base + 7] = 0;
        out.items[base + 8] = @intCast((table_offset >> 24) & 0xff);
        out.items[base + 9] = @intCast((table_offset >> 16) & 0xff);
        out.items[base + 10] = @intCast((table_offset >> 8) & 0xff);
        out.items[base + 11] = @intCast(table_offset & 0xff);
        const table_len: u32 = @intCast(table.bytes.len);
        out.items[base + 12] = @intCast((table_len >> 24) & 0xff);
        out.items[base + 13] = @intCast((table_len >> 16) & 0xff);
        out.items[base + 14] = @intCast((table_len >> 8) & 0xff);
        out.items[base + 15] = @intCast(table_len & 0xff);
    }

    return try out.toOwnedSlice(alloc);
}

fn appendCffInt(alloc: Allocator, out: *std.ArrayList(u8), value: i32) !void {
    if (value >= -107 and value <= 107) {
        try out.append(alloc, @intCast(value + 139));
        return;
    }
    if (value >= -32768 and value <= 32767) {
        try out.append(alloc, 28);
        try appendU16Be(alloc, out, @bitCast(@as(i16, @intCast(value))));
        return;
    }
    return error.OutOfMemory;
}

fn appendCffIndex(alloc: Allocator, out: *std.ArrayList(u8), objects: []const []const u8) !void {
    try appendU16Be(alloc, out, @intCast(objects.len));
    if (objects.len == 0) return;
    try out.append(alloc, 1);
    var offset: usize = 1;
    try out.append(alloc, @intCast(offset));
    for (objects) |obj| {
        offset += obj.len;
        try out.append(alloc, @intCast(offset));
    }
    for (objects) |obj| try out.appendSlice(alloc, obj);
}

fn buildFdSelectOpenTypeCffFontAlloc(alloc: Allocator) ![]u8 {
    var name_index = std.ArrayList(u8).empty;
    defer name_index.deinit(alloc);
    try appendCffIndex(alloc, &name_index, &.{"Test"});

    const top_dict_len: usize = 18;
    const top_dict_index_len: usize = 2 + 1 + 2 + top_dict_len;
    const prefix_len = 4 + name_index.items.len + top_dict_index_len + 2 + 2;

    const charset_offset: i32 = @intCast(prefix_len);
    const fdselect_offset: i32 = charset_offset + 5;
    const fdarray_offset: i32 = fdselect_offset + 4;
    const charstrings_offset: i32 = fdarray_offset + 20;
    const private0_offset: i32 = charstrings_offset + 20;
    const local0_offset: i32 = private0_offset + 2;
    const private1_offset: i32 = local0_offset + 9;

    var top_dict = std.ArrayList(u8).empty;
    defer top_dict.deinit(alloc);
    try appendCffInt(alloc, &top_dict, charset_offset);
    try top_dict.append(alloc, 15);
    try appendCffInt(alloc, &top_dict, fdselect_offset);
    try top_dict.appendSlice(alloc, &.{ 12, 37 });
    try appendCffInt(alloc, &top_dict, fdarray_offset);
    try top_dict.appendSlice(alloc, &.{ 12, 36 });
    try appendCffInt(alloc, &top_dict, charstrings_offset);
    try top_dict.append(alloc, 17);

    var top_dict_index = std.ArrayList(u8).empty;
    defer top_dict_index.deinit(alloc);
    try appendCffIndex(alloc, &top_dict_index, &.{top_dict.items});

    var fd0_dict = std.ArrayList(u8).empty;
    defer fd0_dict.deinit(alloc);
    try appendCffInt(alloc, &fd0_dict, 2);
    try appendCffInt(alloc, &fd0_dict, private0_offset);
    try fd0_dict.append(alloc, 18);

    var fd1_dict = std.ArrayList(u8).empty;
    defer fd1_dict.deinit(alloc);
    try appendCffInt(alloc, &fd1_dict, 2);
    try appendCffInt(alloc, &fd1_dict, private1_offset);
    try fd1_dict.append(alloc, 18);

    var fdarray_index = std.ArrayList(u8).empty;
    defer fdarray_index.deinit(alloc);
    try appendCffIndex(alloc, &fdarray_index, &.{ fd0_dict.items, fd1_dict.items });

    const glyph0 = [_]u8{14};
    const glyph1 = [_]u8{ 139, 139, 21, 32, 10, 14 };
    const glyph2 = [_]u8{ 139, 139, 21, 32, 10, 14 };
    var charstrings_index = std.ArrayList(u8).empty;
    defer charstrings_index.deinit(alloc);
    try appendCffIndex(alloc, &charstrings_index, &.{ &glyph0, &glyph1, &glyph2 });

    const charset = [_]u8{
        0,
        0,
        1,
        0,
        2,
    };
    const fdselect = [_]u8{
        0,
        0,
        0,
        1,
    };
    const private_dict = [_]u8{ 141, 19 };
    var local0_index = std.ArrayList(u8).empty;
    defer local0_index.deinit(alloc);
    const local0_subr = [_]u8{ 189, 139, 5, 11 };
    try appendCffIndex(alloc, &local0_index, &.{&local0_subr});
    var local1_index = std.ArrayList(u8).empty;
    defer local1_index.deinit(alloc);
    const local1_subr = [_]u8{ 139, 189, 5, 11 };
    try appendCffIndex(alloc, &local1_index, &.{&local1_subr});

    var cff = std.ArrayList(u8).empty;
    defer cff.deinit(alloc);
    try cff.appendSlice(alloc, &.{ 1, 0, 4, 1 });
    try cff.appendSlice(alloc, name_index.items);
    try cff.appendSlice(alloc, top_dict_index.items);
    try cff.appendSlice(alloc, &.{ 0, 0 });
    try cff.appendSlice(alloc, &.{ 0, 0 });
    try cff.appendSlice(alloc, &charset);
    try cff.appendSlice(alloc, &fdselect);
    try cff.appendSlice(alloc, fdarray_index.items);
    try cff.appendSlice(alloc, charstrings_index.items);
    try cff.appendSlice(alloc, &private_dict);
    try cff.appendSlice(alloc, local0_index.items);
    try cff.appendSlice(alloc, &private_dict);
    try cff.appendSlice(alloc, local1_index.items);

    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16Be(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16Be(alloc, &head, 0);
    try appendU16Be(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32Be(alloc, &maxp, 0x00010000);
    try appendU16Be(alloc, &maxp, 3);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16Be(alloc, &hhea, 3);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16Be(alloc, &hmtx, 500);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 3);
    try appendU16Be(alloc, &cmap, 1);
    try appendU32Be(alloc, &cmap, 12);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 32);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 66);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 0);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'C', 'F', 'F', ' ' }, .bytes = cff.items },
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "OTTO");
    try appendU16Be(alloc, &out, tables.len);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    const dir_offset = out.items.len;
    try out.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &out);
        const table_offset: u32 = @intCast(out.items.len);
        try out.appendSlice(alloc, table.bytes);
        try pad4(alloc, &out);
        const base = dir_offset + i * 16;
        out.items[base + 0] = table.tag[0];
        out.items[base + 1] = table.tag[1];
        out.items[base + 2] = table.tag[2];
        out.items[base + 3] = table.tag[3];
        out.items[base + 4] = 0;
        out.items[base + 5] = 0;
        out.items[base + 6] = 0;
        out.items[base + 7] = 0;
        out.items[base + 8] = @intCast((table_offset >> 24) & 0xff);
        out.items[base + 9] = @intCast((table_offset >> 16) & 0xff);
        out.items[base + 10] = @intCast((table_offset >> 8) & 0xff);
        out.items[base + 11] = @intCast(table_offset & 0xff);
        const table_len: u32 = @intCast(table.bytes.len);
        out.items[base + 12] = @intCast((table_len >> 24) & 0xff);
        out.items[base + 13] = @intCast((table_len >> 16) & 0xff);
        out.items[base + 14] = @intCast((table_len >> 8) & 0xff);
        out.items[base + 15] = @intCast(table_len & 0xff);
    }

    return try out.toOwnedSlice(alloc);
}

fn buildFdSelectFormat3OpenTypeCffFontAlloc(alloc: Allocator) ![]u8 {
    var name_index = std.ArrayList(u8).empty;
    defer name_index.deinit(alloc);
    try appendCffIndex(alloc, &name_index, &.{"Test"});

    const top_dict_len: usize = 18;
    const top_dict_index_len: usize = 2 + 1 + 2 + top_dict_len;
    const prefix_len = 4 + name_index.items.len + top_dict_index_len + 2 + 2;

    const charset_offset: i32 = @intCast(prefix_len);
    const fdselect_offset: i32 = charset_offset + 5;
    const fdarray_offset: i32 = fdselect_offset + 12;
    const charstrings_offset: i32 = fdarray_offset + 20;
    const private0_offset: i32 = charstrings_offset + 20;
    const local0_offset: i32 = private0_offset + 2;
    const private1_offset: i32 = local0_offset + 9;

    var top_dict = std.ArrayList(u8).empty;
    defer top_dict.deinit(alloc);
    try appendCffInt(alloc, &top_dict, charset_offset);
    try top_dict.append(alloc, 15);
    try appendCffInt(alloc, &top_dict, fdselect_offset);
    try top_dict.appendSlice(alloc, &.{ 12, 37 });
    try appendCffInt(alloc, &top_dict, fdarray_offset);
    try top_dict.appendSlice(alloc, &.{ 12, 36 });
    try appendCffInt(alloc, &top_dict, charstrings_offset);
    try top_dict.append(alloc, 17);

    var top_dict_index = std.ArrayList(u8).empty;
    defer top_dict_index.deinit(alloc);
    try appendCffIndex(alloc, &top_dict_index, &.{top_dict.items});

    var fd0_dict = std.ArrayList(u8).empty;
    defer fd0_dict.deinit(alloc);
    try appendCffInt(alloc, &fd0_dict, 2);
    try appendCffInt(alloc, &fd0_dict, private0_offset);
    try fd0_dict.append(alloc, 18);

    var fd1_dict = std.ArrayList(u8).empty;
    defer fd1_dict.deinit(alloc);
    try appendCffInt(alloc, &fd1_dict, 2);
    try appendCffInt(alloc, &fd1_dict, private1_offset);
    try fd1_dict.append(alloc, 18);

    var fdarray_index = std.ArrayList(u8).empty;
    defer fdarray_index.deinit(alloc);
    try appendCffIndex(alloc, &fdarray_index, &.{ fd0_dict.items, fd1_dict.items });

    const glyph0 = [_]u8{14};
    const glyph1 = [_]u8{ 139, 139, 21, 32, 10, 14 };
    const glyph2 = [_]u8{ 139, 139, 21, 32, 10, 14 };
    var charstrings_index = std.ArrayList(u8).empty;
    defer charstrings_index.deinit(alloc);
    try appendCffIndex(alloc, &charstrings_index, &.{ &glyph0, &glyph1, &glyph2 });

    const charset = [_]u8{
        0,
        0,
        1,
        0,
        2,
    };
    const fdselect = [_]u8{
        3,
        0,
        3,
        0,
        0,
        0,
        0,
        1,
        1,
        0,
        2,
        0,
        0,
        3,
    };
    const private_dict = [_]u8{ 141, 19 };
    var local0_index = std.ArrayList(u8).empty;
    defer local0_index.deinit(alloc);
    const local0_subr = [_]u8{ 189, 139, 5, 11 };
    try appendCffIndex(alloc, &local0_index, &.{&local0_subr});
    var local1_index = std.ArrayList(u8).empty;
    defer local1_index.deinit(alloc);
    const local1_subr = [_]u8{ 139, 189, 5, 11 };
    try appendCffIndex(alloc, &local1_index, &.{&local1_subr});

    var cff = std.ArrayList(u8).empty;
    defer cff.deinit(alloc);
    try cff.appendSlice(alloc, &.{ 1, 0, 4, 1 });
    try cff.appendSlice(alloc, name_index.items);
    try cff.appendSlice(alloc, top_dict_index.items);
    try cff.appendSlice(alloc, &.{ 0, 0 });
    try cff.appendSlice(alloc, &.{ 0, 0 });
    try cff.appendSlice(alloc, &charset);
    try cff.appendSlice(alloc, &fdselect);
    try cff.appendSlice(alloc, fdarray_index.items);
    try cff.appendSlice(alloc, charstrings_index.items);
    try cff.appendSlice(alloc, &private_dict);
    try cff.appendSlice(alloc, local0_index.items);
    try cff.appendSlice(alloc, &private_dict);
    try cff.appendSlice(alloc, local1_index.items);

    var head = std.ArrayList(u8).empty;
    defer head.deinit(alloc);
    try head.appendNTimes(alloc, 0, 18);
    try appendU16Be(alloc, &head, 1000);
    try head.appendNTimes(alloc, 0, 30);
    try appendI16Be(alloc, &head, 0);
    try appendU16Be(alloc, &head, 0);

    var maxp = std.ArrayList(u8).empty;
    defer maxp.deinit(alloc);
    try appendU32Be(alloc, &maxp, 0x00010000);
    try appendU16Be(alloc, &maxp, 3);

    var hhea = std.ArrayList(u8).empty;
    defer hhea.deinit(alloc);
    try hhea.appendNTimes(alloc, 0, 34);
    try appendU16Be(alloc, &hhea, 3);

    var hmtx = std.ArrayList(u8).empty;
    defer hmtx.deinit(alloc);
    try appendU16Be(alloc, &hmtx, 500);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);
    try appendU16Be(alloc, &hmtx, 1000);
    try appendI16Be(alloc, &hmtx, 0);

    var cmap = std.ArrayList(u8).empty;
    defer cmap.deinit(alloc);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 3);
    try appendU16Be(alloc, &cmap, 1);
    try appendU32Be(alloc, &cmap, 12);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 32);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 4);
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 66);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 65);
    try appendU16Be(alloc, &cmap, 0xFFFF);
    try appendU16Be(alloc, &cmap, @bitCast(@as(i16, -64)));
    try appendU16Be(alloc, &cmap, 1);
    try appendU16Be(alloc, &cmap, 0);
    try appendU16Be(alloc, &cmap, 0);

    const tables = [_]struct { tag: [4]u8, bytes: []const u8 }{
        .{ .tag = .{ 'C', 'F', 'F', ' ' }, .bytes = cff.items },
        .{ .tag = .{ 'c', 'm', 'a', 'p' }, .bytes = cmap.items },
        .{ .tag = .{ 'h', 'e', 'a', 'd' }, .bytes = head.items },
        .{ .tag = .{ 'h', 'h', 'e', 'a' }, .bytes = hhea.items },
        .{ .tag = .{ 'h', 'm', 't', 'x' }, .bytes = hmtx.items },
        .{ .tag = .{ 'm', 'a', 'x', 'p' }, .bytes = maxp.items },
    };

    var out = std.ArrayList(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "OTTO");
    try appendU16Be(alloc, &out, tables.len);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    try appendU16Be(alloc, &out, 0);
    const dir_offset = out.items.len;
    try out.appendNTimes(alloc, 0, tables.len * 16);

    for (tables, 0..) |table, i| {
        try pad4(alloc, &out);
        const table_offset: u32 = @intCast(out.items.len);
        try out.appendSlice(alloc, table.bytes);
        try pad4(alloc, &out);
        const base = dir_offset + i * 16;
        out.items[base + 0] = table.tag[0];
        out.items[base + 1] = table.tag[1];
        out.items[base + 2] = table.tag[2];
        out.items[base + 3] = table.tag[3];
        out.items[base + 4] = 0;
        out.items[base + 5] = 0;
        out.items[base + 6] = 0;
        out.items[base + 7] = 0;
        out.items[base + 8] = @intCast((table_offset >> 24) & 0xff);
        out.items[base + 9] = @intCast((table_offset >> 16) & 0xff);
        out.items[base + 10] = @intCast((table_offset >> 8) & 0xff);
        out.items[base + 11] = @intCast(table_offset & 0xff);
        const table_len: u32 = @intCast(table.bytes.len);
        out.items[base + 12] = @intCast((table_len >> 24) & 0xff);
        out.items[base + 13] = @intCast((table_len >> 16) & 0xff);
        out.items[base + 14] = @intCast((table_len >> 8) & 0xff);
        out.items[base + 15] = @intCast(table_len & 0xff);
    }

    return try out.toOwnedSlice(alloc);
}

test "native backend renders embedded FontFile2 true type glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /TrueType /BaseFont /TestTT /FirstChar 65 /LastChar 66 /Widths [1000 1600] /Encoding /WinAnsiEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestTT /FontFile2 7 0 R >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile2 composite glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(B) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /TrueType /BaseFont /TestTT /FirstChar 65 /LastChar 66 /Widths [1000 1600] /Encoding /WinAnsiEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestTT /FontFile2 7 0 R >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_program =
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
        "/FontName /TestT1 def\n" ++
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 2 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "end readonly def\n";

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding << /Differences [65 /A] >> /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 glyph pdf with standard encoding" {
    const alloc = std.testing.allocator;
    const font_program =
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
        "/FontName /TestT1 def\n" ++
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 2 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "end readonly def\n";

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding /StandardEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 RD charstrings pdf first page png" {
    const alloc = std.testing.allocator;
    const subr = [_]u8{ 189, 139, 5, 11 };
    const glyph = [_]u8{ 139, 139, 21, 139, 10, 14 };

    var font_program = std.ArrayList(u8).empty;
    defer font_program.deinit(alloc);
    try font_program.appendSlice(
        alloc,
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
            "/FontName /TestT1 def\n" ++
            "/lenIV -1 def\n" ++
            "/Private 1 dict dup begin\n" ++
            "/Subrs 1 array\n" ++
            "dup 0 4 RD ",
    );
    try font_program.appendSlice(alloc, &subr);
    try font_program.appendSlice(alloc, " ND\nend readonly def\n/CharStrings 2 dict dup begin\ndup /.notdef 4 RD ");
    try font_program.appendSlice(alloc, &[_]u8{ 139, 139, 21, 14 });
    try font_program.appendSlice(alloc, " ND\ndup /A 6 RD ");
    try font_program.appendSlice(alloc, &glyph);
    try font_program.appendSlice(alloc, " ND\nend readonly def\n");

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding /StandardEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.items.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program.items);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 eexec glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const eexec_plain =
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 2 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "end readonly def\n" ++
        "cleartomark\n";
    const encrypted = try encryptType1EexecAlloc(alloc, eexec_plain);
    defer alloc.free(encrypted);

    var hex_payload = std.ArrayList(u8).empty;
    defer hex_payload.deinit(alloc);
    for (encrypted) |b| {
        const piece = try std.fmt.allocPrint(alloc, "{X:0>2}", .{b});
        defer alloc.free(piece);
        try hex_payload.appendSlice(alloc, piece);
    }

    var font_program = std.ArrayList(u8).empty;
    defer font_program.deinit(alloc);
    try font_program.appendSlice(
        alloc,
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
            "/FontName /TestT1 def\n" ++
            "/Encoding /StandardEncoding def\n" ++
            "currentfile eexec\n",
    );
    try font_program.appendSlice(alloc, hex_payload.items);
    try font_program.appendSlice(alloc, "\n");

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding /StandardEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.items.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program.items);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded FontFile type1 pfb eexec glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const eexec_plain =
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 2 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "end readonly def\n" ++
        "cleartomark\n";
    const encrypted = try encryptType1EexecAlloc(alloc, eexec_plain);
    defer alloc.free(encrypted);

    var font_program = std.ArrayList(u8).empty;
    defer font_program.deinit(alloc);
    const ascii_segment =
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
        "/FontName /TestT1 def\n" ++
        "/Encoding /StandardEncoding def\n" ++
        "currentfile eexec\n";
    try font_program.appendSlice(alloc, &.{ 0x80, 0x01 });
    try font_program.appendSlice(alloc, &.{
        @intCast(ascii_segment.len & 0xff),
        @intCast((ascii_segment.len >> 8) & 0xff),
        @intCast((ascii_segment.len >> 16) & 0xff),
        @intCast((ascii_segment.len >> 24) & 0xff),
    });
    try font_program.appendSlice(alloc, ascii_segment);
    try font_program.appendSlice(alloc, &.{ 0x80, 0x02 });
    try font_program.appendSlice(alloc, &.{
        @intCast(encrypted.len & 0xff),
        @intCast((encrypted.len >> 8) & 0xff),
        @intCast((encrypted.len >> 16) & 0xff),
        @intCast((encrypted.len >> 24) & 0xff),
    });
    try font_program.appendSlice(alloc, encrypted);
    try font_program.appendSlice(alloc, &.{ 0x80, 0x03 });

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding /StandardEncoding /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.items.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program.items);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "reader extracts vector text shapes for embedded FontFile type1 seac glyph" {
    const alloc = std.testing.allocator;
    const font_program =
        "%!PS-AdobeFont-1.0: TestT1 1.0\n" ++
        "/FontName /TestT1 def\n" ++
        "/Encoding /StandardEncoding def\n" ++
        "/lenIV -1 def\n" ++
        "/Private 1 dict dup begin\n" ++
        "/Subrs 0 array def\n" ++
        "end readonly def\n" ++
        "/CharStrings 4 dict dup begin\n" ++
        "/.notdef <8B8B150E> def\n" ++
        "/A <8B8B15F77C8B05FB7CFA7C05FB7CFB7C050E> def\n" ++
        "/acute <8B8B15938B058B93058D8B058B8D050E> def\n" ++
        "/Aacute <8BF75CF7C0CCF7560C060E> def\n" ++
        "end readonly def\n";

    const content = "BT\n/F1 20 Tf\n10 10 Td\n(A) Tj\nET\n";
    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /TestT1 /FirstChar 65 /LastChar 65 /Widths [1000] /Encoding << /Differences [65 /Aacute] >> /FontDescriptor 6 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /FontDescriptor /FontName /TestT1 /FontFile 7 0 R >>\nendobj\n";

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    const obj7_head = try std.fmt.allocPrint(alloc, "7 0 obj\n<< /Length {d} >>\nstream\n", .{font_program.len});
    defer alloc.free(obj7_head);
    try out.appendSlice(alloc, obj7_head);
    try out.appendSlice(alloc, font_program);
    try out.appendSlice(alloc, "endstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 8\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 8 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    var parsed = try reader.Reader.init(alloc, out.items);
    defer parsed.deinit();
    const extracted = try parsed.extractPageTextAlloc(1);
    defer alloc.free(extracted);
    try std.testing.expectEqualStrings("Á\n", extracted);
    const runs = try parsed.extractPageVectorTextShapeRunsAlloc(1);
    defer {
        for (runs) |*run| run.deinit(alloc);
        alloc.free(runs);
    }
    try std.testing.expect(runs.len > 0);
}

test "native backend renders embedded Type0 CIDFontType2 glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildSimpleTrueTypeFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n<0041> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "1 beginbfchar\n" ++
        "<0041> <0041>\n" ++
        "endbfchar\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type0 /BaseFont /TestCID /Encoding /Identity-H /DescendantFonts [6 0 R] /ToUnicode 8 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /Font /Subtype /CIDFontType2 /BaseFont /TestCID /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /DW 1000 /W [65 [1000]] /FontDescriptor 7 0 R >>\nendobj\n";
    const obj7 = "7 0 obj\n<< /Type /FontDescriptor /FontName /TestCID /FontFile2 9 0 R >>\nendobj\n";
    const obj8 = try std.fmt.allocPrint(alloc, "8 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap });
    defer alloc.free(obj8);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    try out.appendSlice(alloc, obj7);
    const obj8_offset = out.items.len;
    try out.appendSlice(alloc, obj8);
    const obj9_offset = out.items.len;
    const obj9_head = try std.fmt.allocPrint(alloc, "9 0 obj\n<< /Length {d} >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj9_head);
    try out.appendSlice(alloc, obj9_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 10\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset, obj8_offset, obj9_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 10 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded Type0 CIDFontType0 OpenType CFF glyph pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildSimpleOpenTypeCffFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n<0041> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "1 beginbfchar\n" ++
        "<0041> <0041>\n" ++
        "endbfchar\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type0 /BaseFont /TestCID /Encoding /Identity-H /DescendantFonts [6 0 R] /ToUnicode 8 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /Font /Subtype /CIDFontType0 /BaseFont /TestCID /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /FontDescriptor 7 0 R >>\nendobj\n";
    const obj7 = "7 0 obj\n<< /Type /FontDescriptor /FontName /TestCID /FontFile3 9 0 R >>\nendobj\n";
    const obj8 = try std.fmt.allocPrint(alloc, "8 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap });
    defer alloc.free(obj8);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    try out.appendSlice(alloc, obj7);
    const obj8_offset = out.items.len;
    try out.appendSlice(alloc, obj8);
    const obj9_offset = out.items.len;
    const obj9_head = try std.fmt.allocPrint(alloc, "9 0 obj\n<< /Subtype /OpenType /Length {d} >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj9_head);
    try out.appendSlice(alloc, obj9_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 10\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset, obj8_offset, obj9_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 10 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded Type0 CIDFontType0 OpenType CFF fdselect glyphs pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildFdSelectOpenTypeCffFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n<00410042> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "2 beginbfchar\n" ++
        "<0041> <0041>\n" ++
        "<0042> <0042>\n" ++
        "endbfchar\n" ++
        "endcmap\n" ++
        "end\n" ++
        "end\n";

    const obj1 = "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n";
    const obj2 = "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj\n";
    const obj3 = "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Resources << /Font << /F1 5 0 R >> >> /Contents 4 0 R >>\nendobj\n";
    const obj4 = try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content });
    defer alloc.free(obj4);
    const obj5 = "5 0 obj\n<< /Type /Font /Subtype /Type0 /BaseFont /TestCID /Encoding /Identity-H /DescendantFonts [6 0 R] /ToUnicode 8 0 R >>\nendobj\n";
    const obj6 = "6 0 obj\n<< /Type /Font /Subtype /CIDFontType0 /BaseFont /TestCID /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /FontDescriptor 7 0 R >>\nendobj\n";
    const obj7 = "7 0 obj\n<< /Type /FontDescriptor /FontName /TestCID /FontFile3 9 0 R >>\nendobj\n";
    const obj8 = try std.fmt.allocPrint(alloc, "8 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ cmap.len, cmap });
    defer alloc.free(obj8);

    var out = std.ArrayList(u8).empty;
    defer out.deinit(alloc);
    try out.appendSlice(alloc, "%PDF-1.4\n");
    const obj1_offset = out.items.len;
    try out.appendSlice(alloc, obj1);
    const obj2_offset = out.items.len;
    try out.appendSlice(alloc, obj2);
    const obj3_offset = out.items.len;
    try out.appendSlice(alloc, obj3);
    const obj4_offset = out.items.len;
    try out.appendSlice(alloc, obj4);
    const obj5_offset = out.items.len;
    try out.appendSlice(alloc, obj5);
    const obj6_offset = out.items.len;
    try out.appendSlice(alloc, obj6);
    const obj7_offset = out.items.len;
    try out.appendSlice(alloc, obj7);
    const obj8_offset = out.items.len;
    try out.appendSlice(alloc, obj8);
    const obj9_offset = out.items.len;
    const obj9_head = try std.fmt.allocPrint(alloc, "9 0 obj\n<< /Length {d} /Subtype /OpenType >>\nstream\n", .{font_bytes.len});
    defer alloc.free(obj9_head);
    try out.appendSlice(alloc, obj9_head);
    try out.appendSlice(alloc, font_bytes);
    try out.appendSlice(alloc, "\nendstream\nendobj\n");

    const xref_offset = out.items.len;
    const xref = try std.fmt.allocPrint(
        alloc,
        "xref\n0 10\n0000000000 65535 f \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n{d:0>10} 00000 n \n",
        .{ obj1_offset, obj2_offset, obj3_offset, obj4_offset, obj5_offset, obj6_offset, obj7_offset, obj8_offset, obj9_offset },
    );
    defer alloc.free(xref);
    try out.appendSlice(alloc, xref);
    try out.appendSlice(alloc, "trailer\n<< /Root 1 0 R /Size 10 >>\nstartxref\n");
    const startxref = try std.fmt.allocPrint(alloc, "{d}\n", .{xref_offset});
    defer alloc.free(startxref);
    try out.appendSlice(alloc, startxref);
    try out.appendSlice(alloc, "%%EOF\n");

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test {
    _ = text_encoding;
    _ = reader;
    _ = syntax;
    _ = render;
}

test "native page renderer honors OCR DPI and pixel guard" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");
    const png_72 = try renderPagePngAlloc(alloc, fixture, 1, 72, 40_000_000);
    defer alloc.free(png_72);
    const png_150 = try renderPagePngAlloc(alloc, fixture, 1, 150, 40_000_000);
    defer alloc.free(png_150);
    const decoded_72 = try @import("antfly_image").png.decodeRgba(alloc, png_72);
    defer alloc.free(decoded_72.rgba);
    const decoded_150 = try @import("antfly_image").png.decodeRgba(alloc, png_150);
    defer alloc.free(decoded_150.rgba);
    try std.testing.expect(decoded_150.width > decoded_72.width);
    try std.testing.expect(decoded_150.height > decoded_72.height);
    try std.testing.expectError(error.RenderedPageTooLarge, renderPagePngAlloc(alloc, fixture, 1, 150, 10));
    try std.testing.expectError(error.InvalidPageNumber, renderPagePngAlloc(alloc, fixture, 2, 150, 40_000_000));
}

test "adaptive OCR rendering records effective DPI and enforces safety caps" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();

    var adaptive = try renderParsedPagePngAdaptiveAlloc(alloc, &parsed, 1, 150, 40_000_000, 1000);
    defer adaptive.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 150), adaptive.requested_dpi);
    try std.testing.expect(adaptive.effective_dpi >= 72);
    try std.testing.expect(adaptive.effective_dpi < adaptive.requested_dpi);
    try std.testing.expect(adaptive.width <= 1000);
    try std.testing.expect(adaptive.height <= 1000);
    const decoded = try @import("antfly_image").png.decodeRgba(alloc, adaptive.png);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqual(adaptive.width, decoded.width);
    try std.testing.expectEqual(adaptive.height, decoded.height);

    try std.testing.expectError(error.RenderedPageTooLarge, renderParsedPagePngAdaptiveAlloc(alloc, &parsed, 1, 150, 40_000_000, 700));
    try std.testing.expectError(error.RenderedPageTooLarge, renderParsedPagePngAdaptiveAlloc(alloc, &parsed, 1, 150, 40_000_000, 400));
    try std.testing.expectError(error.RenderedPageTooLarge, renderParsedPagePngAdaptiveAlloc(alloc, &parsed, 1, 150, 10, 4096));
    try std.testing.expectError(error.InvalidRenderDpi, renderParsedPagePngAlloc(alloc, &parsed, 1, 48, 40_000_000));
}

test "OCR DPI scaling maps tiling patterns exactly once" {
    const alloc = std.testing.allocator;
    const tile_points = try alloc.dupe([2]f64, &.{ .{ 0, 0 }, .{ 5, 0 }, .{ 5, 5 }, .{ 0, 5 } });
    var tile_shapes = try alloc.alloc(reader.ShapeRun, 1);
    tile_shapes[0] = .{
        .kind = .fill,
        .color = .{ 0, 0, 0, 0xff },
        .stroke_width = 0,
        .closed = true,
        .points = tile_points,
    };
    const target_points = try alloc.dupe([2]f64, &.{ .{ 0, 0 }, .{ 20, 0 }, .{ 20, 20 }, .{ 0, 20 } });
    var runs = [_]reader.PatternRun{.{
        .kind = .fill,
        .points = target_points,
        .pattern_bbox = .{ .min_x = 0, .min_y = 0, .max_x = 5, .max_y = 5 },
        .pattern_x_step = 5,
        .pattern_y_step = 5,
        .tile_shape_runs = tile_shapes,
    }};
    defer runs[0].deinit(alloc);

    scalePatternRuns(&runs, 2);
    try std.testing.expectApproxEqAbs(@as(f64, 40), runs[0].points[1][0], 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 2), runs[0].pattern_matrix.a, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 5), runs[0].pattern_x_step, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 5), runs[0].pattern_bbox.max_x, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 5), runs[0].tile_shape_runs[0].points[1][0], 0.001);
}

test "native page renderer renders the requested one-based PDF page" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    const second_text = try parsed.extractPageTextAlloc(2);
    defer alloc.free(second_text);
    try std.testing.expect(std.mem.indexOf(u8, second_text, "SECOND PAGE") != null);

    const first_png = try renderPagePngAlloc(alloc, fixture, 1, 150, 40_000_000);
    defer alloc.free(first_png);
    const second_png = try renderPagePngAlloc(alloc, fixture, 2, 150, 40_000_000);
    defer alloc.free(second_png);
    const first = try @import("antfly_image").png.decodeRgba(alloc, first_png);
    defer alloc.free(first.rgba);
    const second = try @import("antfly_image").png.decodeRgba(alloc, second_png);
    defer alloc.free(second.rgba);
    try std.testing.expectEqual(first.width, second.width);
    try std.testing.expectEqual(first.height, second.height);
    try std.testing.expect(!std.mem.eql(u8, first.rgba, second.rgba));
}

test "reader ignores stale positive page-tree Count hints" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    const mutated = try alloc.dupe(u8, fixture);
    defer alloc.free(mutated);
    const marker = std.mem.indexOf(u8, mutated, "/Count 2") orelse return error.InvalidTestFixture;
    mutated[marker + "/Count ".len] = '1';

    var parsed = try reader.Reader.init(alloc, mutated);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), try parsed.pageCount());
    const second = try parsed.extractPageTextAlloc(2);
    defer alloc.free(second);
    try std.testing.expect(std.mem.indexOf(u8, second, "SECOND PAGE") != null);
}

test "native page renderer preserves a raster scanned-table fixture for OCR" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/scanned_table_fixture.pdf");
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    const embedded_text = try parsed.extractPageTextAlloc(1);
    defer alloc.free(embedded_text);
    try std.testing.expectEqual(@as(usize, 0), std.mem.trim(u8, embedded_text, &std.ascii.whitespace).len);

    const png = try renderPagePngAlloc(alloc, fixture, 1, 150, 40_000_000);
    defer alloc.free(png);
    const page = try @import("antfly_image").png.decodeRgba(alloc, png);
    defer alloc.free(page.rgba);
    try std.testing.expect(page.width >= 133);
    try std.testing.expect(page.height >= 100);

    var dark_pixels: usize = 0;
    var i: usize = 0;
    while (i + 3 < page.rgba.len) : (i += 4) {
        if (page.rgba[i] < 128 and page.rgba[i + 1] < 128 and page.rgba[i + 2] < 128) dark_pixels += 1;
    }
    try std.testing.expect(dark_pixels > 500);
}
