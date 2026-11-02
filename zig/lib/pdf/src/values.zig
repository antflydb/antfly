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

//! Shared PDF values and ownership math. No reader, codec, font parser or
//! renderer implementation imports belong in this module.
const std = @import("std");
const Allocator = std.mem.Allocator;
pub const default_max_decoded_stream_bytes: usize = 64 * 1024 * 1024;

pub const default_max_decode_working_set_bytes: usize = 128 * 1024 * 1024;

pub const DecodeLimits = struct {
    max_decoded_stream_bytes: usize = default_max_decoded_stream_bytes,
    max_working_set_bytes: usize = default_max_decode_working_set_bytes,

    pub fn validate(self: DecodeLimits) !void {
        if (self.max_decoded_stream_bytes == 0 or self.max_working_set_bytes == 0)
            return error.InvalidPdfDecodeLimits;
    }
};

pub const TextOutputSpan = struct {
    start: usize,
    end: usize,
};

pub const TextFallbackReason = enum {
    missing_font,
    unsupported_font,
    missing_text_data,
    missing_glyph,
    invalid_font_program,
    outline_too_complex,
    unsupported_resource,
    materialization_limit,
    vector_work_limit,
    font_work_limit,
    other,
};

pub const PageRenderDiagnostics = struct {
    text_groups: u32 = 0,
    vector_text_groups: u32 = 0,
    fallback_text_groups: u32 = 0,
    vector_text_runs: u32 = 0,
    fallback_text_runs: u32 = 0,
    first_fallback_reason: ?TextFallbackReason = null,
    fallback_reason_counts: [@typeInfo(TextFallbackReason).@"enum".field_names.len]u32 =
        @as([@typeInfo(TextFallbackReason).@"enum".field_names.len]u32, @splat(0)),
    text_materialization_bytes: u64 = 0,
    peak_operator_materialization_bytes: u64 = 0,
    remaining_edge_tests: u64 = 0,
    remaining_scanline_work: u64 = 0,
    remaining_points: u64 = 0,
    remaining_resource_bytes: u64 = 0,
    remaining_materialization_bytes: u64 = 0,
    remaining_font_operations: u64 = 0,
    glyph_outline_cache_hits: u32 = 0,
    glyph_outline_cache_misses: u32 = 0,
    glyph_outline_cache_bytes: u64 = 0,

    pub fn recordFallback(self: *PageRenderDiagnostics, reason: TextFallbackReason, run_count: usize) void {
        self.fallback_text_groups +|= 1;
        self.fallback_text_runs +|= @intCast(@min(run_count, std.math.maxInt(u32)));
        self.fallback_reason_counts[@backingInt(reason)] +|= 1;
        if (self.first_fallback_reason == null) self.first_fallback_reason = reason;
    }
};

pub const CancellationProbe = struct {
    context: ?*const anyopaque = null,
    is_cancelled_fn: ?*const fn (?*const anyopaque) bool = null,

    pub fn check(self: CancellationProbe) !void {
        if (self.is_cancelled_fn) |is_cancelled| if (is_cancelled(self.context)) return error.Canceled;
    }
};

pub const TextRun = struct {
    text: []const u8,
    raw_text: ?[]const u8 = null,
    font_index: ?u16 = null,
    /// Render-time font resource for text nested inside a Form XObject. Form
    /// font indices are local to their resource dictionary and cannot be
    /// interpreted against the page font slice. This borrow is consumed and
    /// cleared before PageRenderRuns escapes the Reader call.
    render_font: ?*const anyopaque = null,
    vectorizable: bool = false,
    fallback_reason: ?TextFallbackReason = null,
    x: f64,
    y: f64,
    font_size: f64,
    a: f64 = 1,
    b: f64 = 0,
    c: f64 = 0,
    d: f64 = 1,
    /// Graphics CTM at the point where this text run was painted. The text
    /// basis above is already transformed, but pattern space is defined by
    /// the graphics CTM and must not be reconstructed from the glyph basis.
    graphics_matrix: GraphicsMatrix = .{},
    alpha: u8 = 0xff,
    stroke_alpha: u8 = 0xff,
    render_mode: i64 = 0,
    fill_color: [4]u8 = .{ 0, 0, 0, 0xff },
    stroke_color: [4]u8 = .{ 0, 0, 0, 0xff },
    stroke_width: f64 = 1,
    horizontal_scale: f64 = 1.0,
    vertical: bool = false,
    char_spacing: f64 = 0,
    word_spacing: f64 = 0,
    advance_width: f64 = 0,
    ascent: f64 = 0,
    descent: f64 = 0,
    paint_order: usize = 0,
    /// Consistent TJ positioning measured on both sides of authored whitespace.
    /// Only matching local gaps between glyph fragments count as tracking.
    text_show_tracking_gap: ?f64 = null,
    paint_phase: usize = 0,
    output_span: ?TextOutputSpan = null,

    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    group_alpha: u8 = 0xff,
    group_blend_mode: BlendMode = .normal,
    fill_pattern_name: ?[]const u8 = null,
    stroke_pattern_name: ?[]const u8 = null,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,

    /// Page-space bounds of horizontal text, including its transformed basis
    /// and font ascent/descent. Vertical writing needs per-glyph vertical
    /// origins; leave that geometry unknown rather than inventing a rectangle.
    pub fn bounds(self: TextRun) ?PageBox {
        if (self.vertical) return null;
        inline for (.{ self.x, self.y, self.a, self.b, self.c, self.d, self.advance_width, self.ascent, self.descent }) |value| if (!std.math.isFinite(value)) return null;
        if (self.ascent < 0 or self.descent < 0) return null;
        const box = preciseTextRunBounds(self);
        inline for (.{ box.min_x, box.min_y, box.max_x, box.max_y }) |value| if (!std.math.isFinite(value)) return null;
        if (box.min_x >= box.max_x or box.min_y >= box.max_y) return null;
        return .{ .min_x = box.min_x, .min_y = box.min_y, .max_x = box.max_x, .max_y = box.max_y };
    }

    pub fn deinit(self: *TextRun, alloc: Allocator) void {
        if (self.clip_points) |points| alloc.free(points);
        if (self.fill_pattern_name) |name| alloc.free(name);
        if (self.stroke_pattern_name) |name| alloc.free(name);
        if (self.raw_text) |raw| alloc.free(raw);
        alloc.free(self.text);
        self.* = undefined;
    }
};

pub const PageTextAnalysis = struct {
    text: []u8,
    runs: []TextRun,
    outline_fallback: bool = false,
    has_nontext_visual: bool = false,
    has_unrendered_annotations: bool = false,

    pub fn deinit(self: *PageTextAnalysis, alloc: Allocator) void {
        alloc.free(self.text);
        for (self.runs) |*run| run.deinit(alloc);
        if (self.runs.len > 0) alloc.free(self.runs);
        self.* = undefined;
    }
};

pub const ImageRun = struct {
    rgba: []u8,
    /// Decoded XObject samples are immutable and shared by every `Do`
    /// occurrence. Ad-hoc callers may leave this null and retain the legacy
    /// owned-slice behavior.
    shared_rgba: ?*SharedImageRgba = null,
    width: u32,
    height: u32,
    interpolate: bool = false,
    /// True when the source is one-bit scan/line-art data. OCR rendering may
    /// use coverage-preserving minification without changing exact PDF mode.
    bilevel: bool = false,
    /// Whole-image premultiplied coverage summary. OCR minification uses this
    /// only when a full-source footprint cannot afford exact integration.
    bilevel_fallback: ?[4]u8 = null,
    ocr_coverage_minify: bool = false,
    /// PDF image masks are stencils: their decoded samples provide coverage,
    /// while the active nonstroking color at each `Do` provides paint. Keep
    /// that color on the occurrence instead of mutating shared decoded RGBA.
    stencil_color: ?[4]u8 = null,
    /// A graphics-state soft mask whose group is a single image covering the
    /// painted image. The mask is sampled in the same normalized image space.
    opacity_mask_rgba: ?[]u8 = null,
    opacity_mask_shared_rgba: ?*SharedImageRgba = null,
    opacity_mask_width: u32 = 0,
    opacity_mask_height: u32 = 0,
    opacity_mask_luminosity: bool = false,
    opacity_mask_interpolate: bool = false,
    alpha: u8 = 0xff,
    paint_order: usize = 0,
    paint_phase: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    group_alpha: u8 = 0xff,
    group_blend_mode: BlendMode = .normal,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,
    a: f64,
    b: f64,
    c: f64,
    d: f64,
    e: f64,
    f: f64,
    x: f64,
    y: f64,
    draw_width: f64,
    draw_height: f64,

    pub fn deinit(self: *ImageRun, alloc: Allocator) void {
        if (self.clip_points) |points| alloc.free(points);
        if (self.opacity_mask_shared_rgba) |shared|
            shared.release(alloc)
        else if (self.opacity_mask_rgba) |mask|
            alloc.free(mask);
        if (self.shared_rgba) |shared| shared.release(alloc) else alloc.free(self.rgba);
        self.* = undefined;
    }
};

pub const ShadingRun = struct {
    kind: ShadingKind,
    paint_order: usize = 0,
    paint_phase: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    group_alpha: u8 = 0xff,
    group_blend_mode: BlendMode = .normal,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,
    x0: f64,
    y0: f64,
    r0: f64 = 0,
    x1: f64,
    y1: f64,
    r1: f64 = 0,
    c0: [4]u8,
    c1: [4]u8,
    color_samples: [shading_color_sample_capacity][4]u8 = @as([shading_color_sample_capacity][4]u8, @splat(.{ 0, 0, 0, 0xff })),
    color_sample_positions: [shading_color_sample_capacity]f64 = @splat(0),
    color_sample_count: u8 = 0,
    exact_boundary_colors: [shading_discontinuity_capacity][4]u8 = @as([shading_discontinuity_capacity][4]u8, @splat(.{ 0, 0, 0, 0xff })),
    exact_boundary_positions: [shading_discontinuity_capacity]f64 = @splat(0),
    exact_boundary_count: u8 = 0,
    extend_start: bool = false,
    extend_end: bool = false,

    pub fn deinit(self: *ShadingRun, alloc: Allocator) void {
        if (self.clip_points) |points| alloc.free(points);
        self.* = undefined;
    }
};

pub const PatternRun = struct {
    kind: PatternKind,
    mode: PatternMode = .tiling,
    paint_order: usize = 0,
    /// Orders multiple paint phases emitted by one PDF operator, independent
    /// of backing paint type. Synthesized outlines use fill then stroke;
    /// Type3 glyphs retain their intrinsic CharProc operator sequence.
    paint_phase: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    group_alpha: u8 = 0xff,
    group_blend_mode: BlendMode = .normal,
    fill_rule: FillRule = .nonzero,
    line_cap: @FieldType(ShapeRun, "line_cap") = .butt,
    line_join: @FieldType(ShapeRun, "line_join") = .miter,
    miter_limit: f64 = 10,
    dash_array: ?[]f64 = null,
    dash_phase: f64 = 0,
    stroke_width: f64 = 1,
    closed: bool = true,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,
    points: [][2]f64,
    subpath_starts: ?[]usize = null,
    subpath_closed: ?[]bool = null,
    /// A Pattern-colored image mask uses the image only as target coverage.
    /// Keeping the immutable decoded mask here avoids premature rasterization
    /// and preserves the pattern cell, transform, clip, and blend semantics.
    stencil_mask: ?ImageRun = null,
    pattern_matrix: GraphicsMatrix = .{},
    pattern_bbox: PageBox,
    pattern_x_step: f64,
    pattern_y_step: f64,
    base_color: ?[4]u8 = null,
    shading: ?ShadingRun = null,
    tile_text_runs: []TextRun = &.{},
    tile_image_runs: []ImageRun = &.{},
    tile_shading_runs: []ShadingRun = &.{},
    tile_pattern_runs: []PatternRun = &.{},
    tile_shape_runs: []ShapeRun = &.{},

    pub fn deinit(self: *PatternRun, alloc: Allocator) void {
        if (self.dash_array) |dash| alloc.free(dash);
        if (self.clip_points) |clip| alloc.free(clip);
        if (self.subpath_starts) |starts| alloc.free(starts);
        if (self.subpath_closed) |closed| alloc.free(closed);
        alloc.free(self.points);
        if (self.stencil_mask) |*mask| mask.deinit(alloc);
        if (self.shading) |*shading| shading.deinit(alloc);
        for (self.tile_text_runs) |*run| run.deinit(alloc);
        if (self.tile_text_runs.len > 0) alloc.free(self.tile_text_runs);
        for (self.tile_image_runs) |*run| run.deinit(alloc);
        if (self.tile_image_runs.len > 0) alloc.free(self.tile_image_runs);
        for (self.tile_shading_runs) |*run| run.deinit(alloc);
        if (self.tile_shading_runs.len > 0) alloc.free(self.tile_shading_runs);
        for (self.tile_pattern_runs) |*run| run.deinit(alloc);
        if (self.tile_pattern_runs.len > 0) alloc.free(self.tile_pattern_runs);
        for (self.tile_shape_runs) |*run| run.deinit(alloc);
        if (self.tile_shape_runs.len > 0) alloc.free(self.tile_shape_runs);
        self.* = undefined;
    }
};

pub const PageBox = struct {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,
};

pub const BlendMode = enum {
    normal,
    multiply,
    screen,
    overlay,
    darken,
    lighten,
};

pub const ShapeKind = enum { fill, stroke };

pub const ShapeLineCap = enum { butt, round, square };

pub const ShapeLineJoin = enum { miter, round, bevel };

pub const ShapeRun = struct {
    kind: ShapeKind,
    paint_order: usize = 0,
    paint_phase: usize = 0,
    blend_mode: BlendMode = .normal,
    group_id: ?u32 = null,
    group_parent_id: ?u32 = null,
    group_isolated: bool = true,
    group_knockout: bool = false,
    group_alpha: u8 = 0xff,
    group_blend_mode: BlendMode = .normal,
    fill_rule: FillRule = .nonzero,
    line_cap: ShapeLineCap = .butt,
    line_join: ShapeLineJoin = .miter,
    miter_limit: f64 = 10,
    dash_array: ?[]f64 = null,
    dash_phase: f64 = 0,
    color: [4]u8,
    stroke_width: f64,
    closed: bool,
    clip_box: ?PageBox = null,
    clip_points: ?[]const [2]f64 = null,
    clip_fill_rule: FillRule = .nonzero,
    points: [][2]f64,
    antialias: bool = false,
    /// Start offsets for independently closed subpaths in `points`. Font
    /// glyphs need all contours painted as one path so counter contours
    /// (the holes in O, B, 8, and similar glyphs) participate in winding.
    subpath_starts: ?[]usize = null,
    /// Per-subpath closure for compound stroked paths. Filled paths close
    /// implicitly, but PDF permits one stroke operator to mix open and closed
    /// contours; one page-wide `closed` bit cannot represent that faithfully.
    subpath_closed: ?[]bool = null,

    pub fn deinit(self: *ShapeRun, alloc: Allocator) void {
        if (self.dash_array) |dash| alloc.free(dash);
        if (self.clip_points) |clip| alloc.free(clip);
        if (self.subpath_starts) |starts| alloc.free(starts);
        if (self.subpath_closed) |closed| alloc.free(closed);
        alloc.free(self.points);
        self.* = undefined;
    }
};

pub const PageRenderRuns = struct {
    page_box: PageBox = .{ .min_x = 0, .min_y = 0, .max_x = 612, .max_y = 792 },
    text_runs: []TextRun = &.{},
    image_runs: []ImageRun = &.{},
    shading_runs: []ShadingRun = &.{},
    pattern_runs: []PatternRun = &.{},
    shape_runs: []ShapeRun = &.{},
    diagnostics: PageRenderDiagnostics = .{},

    pub fn deinit(self: *PageRenderRuns, alloc: Allocator) void {
        for (self.text_runs) |*run| run.deinit(alloc);
        if (self.text_runs.len > 0) alloc.free(self.text_runs);
        for (self.image_runs) |*run| run.deinit(alloc);
        if (self.image_runs.len > 0) alloc.free(self.image_runs);
        for (self.shading_runs) |*run| run.deinit(alloc);
        if (self.shading_runs.len > 0) alloc.free(self.shading_runs);
        for (self.pattern_runs) |*run| run.deinit(alloc);
        if (self.pattern_runs.len > 0) alloc.free(self.pattern_runs);
        for (self.shape_runs) |*run| run.deinit(alloc);
        if (self.shape_runs.len > 0) alloc.free(self.shape_runs);
        self.* = undefined;
    }
};

pub const SharedImageRgba = struct {
    bytes: []u8,
    references: usize = 1,

    pub fn retain(self: *SharedImageRgba) void {
        self.references = std.math.add(usize, self.references, 1) catch std.math.maxInt(usize);
    }

    pub fn release(self: *SharedImageRgba, alloc: Allocator) void {
        std.debug.assert(self.references > 0);
        self.references -= 1;
        if (self.references == 0) {
            alloc.free(self.bytes);
            alloc.destroy(self);
        }
    }
};

pub const FillRule = enum { nonzero, even_odd };

pub const GraphicsMatrix = struct {
    a: f64 = 1,
    b: f64 = 0,
    c: f64 = 0,
    d: f64 = 1,
    e: f64 = 0,
    f: f64 = 0,
};

pub const TextRunBounds = struct {
    min_x: f64,
    min_y: f64,
    max_x: f64,
    max_y: f64,

    pub fn include(self: *TextRunBounds, other: TextRunBounds) void {
        self.min_x = @min(self.min_x, other.min_x);
        self.min_y = @min(self.min_y, other.min_y);
        self.max_x = @max(self.max_x, other.max_x);
        self.max_y = @max(self.max_y, other.max_y);
    }
};

pub const shading_uniform_color_sample_count: usize = 64;

pub const shading_color_sample_capacity: usize = 128;

pub const shading_color_sample_count = shading_uniform_color_sample_count;

pub const shading_discontinuity_capacity: usize = (shading_color_sample_capacity - shading_uniform_color_sample_count) / 2;

pub const ShadingKind = enum { axial, radial };
pub const PatternKind = enum { fill, stroke };
pub const PatternMode = enum { tiling, shading };

pub fn preciseTextRunBounds(run: anytype) TextRunBounds {
    const end_x = run.x + run.a * run.advance_width;
    const end_y = run.y + run.b * run.advance_width;
    const top_x = run.c * run.ascent;
    const top_y = run.d * run.ascent;
    const bottom_x = -run.c * run.descent;
    const bottom_y = -run.d * run.descent;
    return .{
        .min_x = @min(@min(run.x + top_x, run.x + bottom_x), @min(end_x + top_x, end_x + bottom_x)),
        .min_y = @min(@min(run.y + top_y, run.y + bottom_y), @min(end_y + top_y, end_y + bottom_y)),
        .max_x = @max(@max(run.x + top_x, run.x + bottom_x), @max(end_x + top_x, end_x + bottom_x)),
        .max_y = @max(@max(run.y + top_y, run.y + bottom_y), @max(end_y + top_y, end_y + bottom_y)),
    };
}

pub const PageRotation = enum(u16) {
    none = 0,
    clockwise_90 = 90,
    clockwise_180 = 180,
    clockwise_270 = 270,
};
pub const RenderedPagePng = struct {
    png: []u8,
    requested_dpi: u16,
    effective_dpi: u16,
    width: u32,
    height: u32,
    quality: RenderQuality = .native,
    diagnostics: ?PageRenderDiagnostics = null,

    pub fn deinit(self: *RenderedPagePng, alloc: Allocator) void {
        alloc.free(self.png);
        self.* = undefined;
    }
};

pub const PixelFormat = enum {
    rgba8,

    pub fn bytesPerPixel(self: @This()) usize {
        return switch (self) {
            .rgba8 => 4,
        };
    }
};

pub const RenderedPageRaster = struct {
    bytes: []u8,
    pixel_format: PixelFormat,
    width: u32,
    height: u32,
    stride: usize,
    requested_dpi: u16,
    effective_dpi: u16,
    quality: RenderQuality = .native,
    diagnostics: ?PageRenderDiagnostics = null,

    pub fn deinit(self: *RenderedPageRaster, alloc: Allocator) void {
        alloc.free(self.bytes);
        self.* = undefined;
    }
};

pub const RenderQuality = enum {
    /// Every supported page paint operation was rendered by the native Zig
    /// path within its deterministic limits.
    native,
    /// Native rendering completed, but one or more text groups used the
    /// bounded raster-font fallback. Callers may still OCR the result while
    /// surfacing the diagnostic counters.
    degraded,
    /// Native decoding rejected an unsupported construct and a compatibility
    /// backend produced the pixels. This is never selected on platforms
    /// without such a backend.
    compatibility_backend,
};

pub const RenderProfile = enum {
    /// Preserve PDF sampling semantics exactly, including nearest-neighbor
    /// minification when /Interpolate is absent.
    exact,
    /// Preserve bilevel ink coverage during minification for OCR inputs.
    ocr,
};

pub const RenderResolutionPolicy = enum { requested_dpi, model_input };

pub const PageRenderRequest = struct {
    page_number: usize,
    requested_dpi: u16 = 150,
    max_pixels: u64 = 40_000_000,
    max_dimension: u32 = 4096,
    /// Model input dimensions are not a quality guarantee. Direct rendering
    /// to those dimensions changes text antialiasing and requires opt-in.
    resolution_policy: RenderResolutionPolicy = .requested_dpi,
    /// Optional downstream fixed raster target. When both are set, geometry
    /// planning chooses the lowest DPI that supplies at least this many source
    /// samples on both axes. If the configured DPI/caps cannot reach it, the
    /// ordinary highest-quality admissible geometry is retained.
    preferred_width: ?u32 = null,
    preferred_height: ?u32 = null,
    /// Bound retained output for this page. PNG rendering resizes and
    /// re-encodes when necessary; raw raster rendering constrains geometry
    /// before painting because its byte size is known exactly.
    max_output_bytes: ?usize = null,
    min_output_dimension: u32 = 1,
    max_output_attempts: u8 = 8,
};

pub const PageRenderExecutor = struct {
    ptr: *anyopaque,
    concurrent_capacity: usize,
    run_batch_fn: *const fn (
        ptr: *anyopaque,
        contexts: []const *anyopaque,
        run: *const fn (context: *anyopaque, scratch: Allocator) void,
        max_scratch_bytes: usize,
    ) anyerror!BatchStats,

    pub const BatchStats = struct {
        peak_parallelism: usize,
    };

    pub fn runBatch(
        self: @This(),
        contexts: []const *anyopaque,
        run: *const fn (context: *anyopaque, scratch: Allocator) void,
        max_scratch_bytes: usize,
    ) !BatchStats {
        if (self.concurrent_capacity == 0) return error.InvalidPageRenderExecutor;
        return try self.run_batch_fn(self.ptr, contexts, run, max_scratch_bytes);
    }
};

pub const default_render_bytes_per_pixel_reserve: usize = 12;

pub const RenderScratchGrowth = struct {
    context: *anyopaque,
    max_bytes: usize,
    grow_fn: *const fn (*anyopaque) usize,
};

pub const PageRenderResult = struct {
    page_number: usize,
    rendered: ?RenderedPagePng = null,
    failure: ?anyerror = null,
    /// Wall-clock time spent inside the page worker's render operation. This
    /// excludes wave preparation, admission, and start-gate wait time.
    render_elapsed_ns: u64 = 0,

    pub fn deinit(self: *PageRenderResult, alloc: Allocator) void {
        if (self.rendered) |*page| page.deinit(alloc);
        self.* = undefined;
    }
};

pub const RenderedPageBatch = struct {
    results: []PageRenderResult,
    requested_parallelism: usize,
    /// Maximum number of worker tasks launched in a wave. This is distinct
    /// from peak_parallelism: launched work may still be queued by the runtime.
    peak_launched_workers: usize,
    /// Maximum number of workers executing their render operation at once.
    peak_parallelism: usize,
    peak_admitted_pixels: u64,
    /// Worst-case live worker memory admitted in one wave, including raster
    /// reservations, render-fork metadata, and per-page decode working sets.
    peak_admitted_bytes: usize,
    peak_worker_scratch_bytes: usize = 0,
    thread_spawn_fallbacks: usize,

    pub fn deinit(self: *RenderedPageBatch, alloc: Allocator) void {
        for (self.results) |*result| result.deinit(alloc);
        alloc.free(self.results);
        self.* = undefined;
    }
};

pub const PageRasterResult = struct {
    page_number: usize,
    rendered: ?RenderedPageRaster = null,
    failure: ?anyerror = null,
    render_elapsed_ns: u64 = 0,

    pub fn deinit(self: *PageRasterResult, alloc: Allocator) void {
        if (self.rendered) |*page| page.deinit(alloc);
        self.* = undefined;
    }
};

pub const RenderedPageRasterBatch = struct {
    results: []PageRasterResult,
    requested_parallelism: usize,
    peak_launched_workers: usize,
    peak_parallelism: usize,
    peak_admitted_pixels: u64,
    peak_admitted_bytes: usize,
    peak_worker_scratch_bytes: usize = 0,
    thread_spawn_fallbacks: usize,

    pub fn deinit(self: *RenderedPageRasterBatch, alloc: Allocator) void {
        for (self.results) |*result| result.deinit(alloc);
        alloc.free(self.results);
        self.* = undefined;
    }
};

pub const PageRenderGeometry = struct {
    effective_dpi: u16,
    width: u32,
    height: u32,
    pixels: u64,
};

pub const PreparedPageRenderPlan = struct {
    _request: PageRenderRequest,
    _geometry: AdaptiveRenderGeometry,
    _source_ptr: [*]const u8,
    _source_len: usize,
    /// Ordered page-local preparation failure. Such entries occupy an output
    /// slot but never launch a renderer or consume pixel admission.
    failure: ?anyerror = null,

    pub fn request(self: @This()) PageRenderRequest {
        return self._request;
    }

    pub fn geometry(self: @This()) PageRenderGeometry {
        if (self.failure != null) return .{ .effective_dpi = 0, .width = 0, .height = 0, .pixels = 0 };
        return .{
            .effective_dpi = self._geometry.effective_dpi,
            .width = self._geometry.width,
            .height = self._geometry.height,
            .pixels = self._geometry.pixels,
        };
    }

    /// Return the same source-bound geometry with a caller-owned encoded
    /// output ceiling. Output admission does not affect raster geometry, so a
    /// document planner may narrow this limit after it sizes the complete
    /// inference window without repeating page-tree and box discovery.
    pub fn withMaxOutputBytes(self: @This(), max_output_bytes: ?usize) @This() {
        var updated = self;
        updated._request.max_output_bytes = max_output_bytes;
        return updated;
    }

    pub fn matchesSourceBytes(self: @This(), source: []const u8) bool {
        return self._source_ptr == source.ptr and self._source_len == source.len;
    }
};

pub const RenderBatchOutputKind = enum { png, raster };

pub const AdaptiveRenderGeometry = struct {
    effective_dpi: u16,
    width: u32,
    height: u32,
    pixels: u64,
    rotation: PageRotation,
};
pub fn estimateRunWidth(run: TextRun) f64 {
    if (run.advance_width > 0) return run.advance_width;
    return estimateRunWidthFallback(run);
}

pub fn estimateRunWidthFallback(run: TextRun) f64 {
    var width_est: f64 = 0;
    var it = std.unicode.Utf8View.init(run.text) catch {
        for (run.text) |ch| width_est += baseRunCodepointAdvance(run, if (ch == ' ') ' ' else 0xfffd);
        return width_est;
    };
    var iter = it.iterator();
    while (iter.nextCodepoint()) |cp| width_est += baseRunCodepointAdvance(run, cp);
    return width_est;
}

pub fn baseRunCodepointAdvance(run: TextRun, cp: u21) f64 {
    if (run.vertical) return run.font_size + run.char_spacing;
    const glyph = run.font_size * 0.6;
    const spacing = run.char_spacing + if (cp == ' ') run.word_spacing else 0.0;
    return (glyph + spacing) * run.horizontal_scale;
}

pub fn renderTextRunBounds(run: TextRun) struct { min_x: f64, max_x: f64, min_y: f64, max_y: f64 } {
    const width_est = estimateRunWidth(run);
    const ascent = effectiveRunAscent(run);
    const descent = effectiveRunDescent(run);
    const local_min_x = if (run.vertical) -run.font_size / 2.0 else 0;
    const local_max_x = if (run.vertical) run.font_size / 2.0 else width_est;
    const local_min_y = if (run.vertical) -width_est - descent else -descent;
    const local_max_y = ascent;
    const corners = [_][2]f64{
        .{ run.x + run.a * local_min_x + run.c * local_min_y, run.y + run.b * local_min_x + run.d * local_min_y },
        .{ run.x + run.a * local_max_x + run.c * local_min_y, run.y + run.b * local_max_x + run.d * local_min_y },
        .{ run.x + run.a * local_min_x + run.c * local_max_y, run.y + run.b * local_min_x + run.d * local_max_y },
        .{ run.x + run.a * local_max_x + run.c * local_max_y, run.y + run.b * local_max_x + run.d * local_max_y },
    };

    var min_x = corners[0][0];
    var max_x = corners[0][0];
    var min_y = corners[0][1];
    var max_y = corners[0][1];
    for (corners[1..]) |corner| {
        min_x = @min(min_x, corner[0]);
        max_x = @max(max_x, corner[0]);
        min_y = @min(min_y, corner[1]);
        max_y = @max(max_y, corner[1]);
    }
    return .{ .min_x = min_x, .max_x = max_x, .min_y = min_y, .max_y = max_y };
}

pub fn effectiveRunAscent(run: TextRun) f64 {
    return if (run.ascent > 0 or run.descent > 0) run.ascent else run.font_size * 0.8;
}

pub fn effectiveRunDescent(run: TextRun) f64 {
    return if (run.ascent > 0 or run.descent > 0) run.descent else run.font_size * 0.2;
}

const Self = @This();
pub const reader = struct {
    pub const default_max_decoded_stream_bytes = Self.default_max_decoded_stream_bytes;
    pub const default_max_decode_working_set_bytes = Self.default_max_decode_working_set_bytes;
    pub const DecodeLimits = Self.DecodeLimits;
    pub const TextOutputSpan = Self.TextOutputSpan;
    pub const TextFallbackReason = Self.TextFallbackReason;
    pub const PageRenderDiagnostics = Self.PageRenderDiagnostics;
    pub const CancellationProbe = Self.CancellationProbe;
    pub const TextRun = Self.TextRun;
    pub const PageTextAnalysis = Self.PageTextAnalysis;
    pub const ImageRun = Self.ImageRun;
    pub const ShadingRun = Self.ShadingRun;
    pub const PatternRun = Self.PatternRun;
    pub const PageBox = Self.PageBox;
    pub const BlendMode = Self.BlendMode;
    pub const ShapeKind = Self.ShapeKind;
    pub const ShapeLineCap = Self.ShapeLineCap;
    pub const ShapeLineJoin = Self.ShapeLineJoin;
    pub const ShapeRun = Self.ShapeRun;
    pub const PageRenderRuns = Self.PageRenderRuns;
};
pub const render = struct {
    pub const textRunBounds = renderTextRunBounds;
};

pub fn normalizedPageRotation(rotation: ?i32) !PageRotation {
    const normalized = @mod(rotation orelse 0, 360);
    return switch (normalized) {
        0 => .none,
        90 => .clockwise_90,
        180 => .clockwise_180,
        270 => .clockwise_270,
        else => error.InvalidPageRotation,
    };
}

pub fn nativeRasterBboxToPagePoints(
    page_box: PageBox,
    effective_dpi: u16,
    raster_width: u32,
    raster_height: u32,
    page_rotation: ?i32,
    raster_bbox: [4]f64,
) ?[4]f64 {
    if (effective_dpi == 0 or raster_width == 0 or raster_height == 0) return null;
    if (!std.math.isFinite(page_box.min_x) or !std.math.isFinite(page_box.min_y) or
        !std.math.isFinite(page_box.max_x) or !std.math.isFinite(page_box.max_y) or
        !(page_box.min_x < page_box.max_x) or !(page_box.min_y < page_box.max_y))
        return null;
    for (raster_bbox) |coordinate| if (!std.math.isFinite(coordinate)) return null;
    const left = raster_bbox[0];
    const top = raster_bbox[1];
    const right = raster_bbox[2];
    const bottom = raster_bbox[3];
    const final_width: f64 = @floatFromInt(raster_width);
    const final_height: f64 = @floatFromInt(raster_height);
    if (!(left < right) or !(top < bottom) or left < 0 or top < 0 or
        right > final_width or bottom > final_height)
        return null;

    const rotation = normalizedPageRotation(page_rotation) catch return null;
    const scale = @as(f64, @floatFromInt(effective_dpi)) / 72.0;
    const expected_unrotated_width = @ceil((page_box.max_x - page_box.min_x) * scale);
    const expected_unrotated_height = @ceil((page_box.max_y - page_box.min_y) * scale);
    if (!std.math.isFinite(expected_unrotated_width) or !std.math.isFinite(expected_unrotated_height) or
        expected_unrotated_width < 1 or expected_unrotated_height < 1)
        return null;
    const expected_final_width = if (rotation == .clockwise_90 or rotation == .clockwise_270)
        expected_unrotated_height
    else
        expected_unrotated_width;
    const expected_final_height = if (rotation == .clockwise_90 or rotation == .clockwise_270)
        expected_unrotated_width
    else
        expected_unrotated_height;
    if (final_width != expected_final_width or final_height != expected_final_height) return null;
    var unrotated: [4]f64 = undefined;
    var unrotated_width = final_width;
    var unrotated_height = final_height;
    switch (rotation) {
        .none => unrotated = raster_bbox,
        .clockwise_90 => {
            unrotated_width = final_height;
            unrotated_height = final_width;
            unrotated = .{ top, unrotated_height - right, bottom, unrotated_height - left };
        },
        .clockwise_180 => {
            unrotated = .{
                unrotated_width - right,
                unrotated_height - bottom,
                unrotated_width - left,
                unrotated_height - top,
            };
        },
        .clockwise_270 => {
            unrotated_width = final_height;
            unrotated_height = final_width;
            unrotated = .{ unrotated_width - bottom, left, unrotated_width - top, right };
        },
    }
    if (unrotated[0] < 0 or unrotated[1] < 0 or
        unrotated[2] > unrotated_width or unrotated[3] > unrotated_height)
        return null;

    // `scale` and the exact pixel-grid dimensions were validated above.
    const mapped = [4]f64{
        page_box.min_x + unrotated[0] / scale,
        page_box.min_y + (unrotated_height - unrotated[3]) / scale,
        page_box.min_x + unrotated[2] / scale,
        page_box.min_y + (unrotated_height - unrotated[1]) / scale,
    };
    // The renderer rounds the trailing CropBox edges up to whole pixels. A
    // region touching those pixels may extend past the original page bounds.
    const clipped = [4]f64{
        std.math.clamp(mapped[0], page_box.min_x, page_box.max_x),
        std.math.clamp(mapped[1], page_box.min_y, page_box.max_y),
        std.math.clamp(mapped[2], page_box.min_x, page_box.max_x),
        std.math.clamp(mapped[3], page_box.min_y, page_box.max_y),
    };
    if (!(clipped[0] < clipped[2]) or !(clipped[1] < clipped[3])) return null;
    return clipped;
}
