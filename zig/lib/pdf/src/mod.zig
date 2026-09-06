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
const image = @import("antfly_image");
const darwin_render = if (builtin.os.tag == .macos) @import("darwin_render.zig") else struct {};
const CompatibilityRenderSession = if (builtin.os.tag == .macos) darwin_render.SharedSession else struct {};

pub const text_encoding = @import("text_encoding.zig");
pub const reader = @import("reader.zig");
pub const syntax = @import("syntax.zig");
pub const render = @import("render.zig");

const Allocator = std.mem.Allocator;

const minimum_direct_render_dpi: u16 = 1;
const minimum_requested_render_dpi: u16 = 72;

pub const RenderedPagePng = struct {
    png: []u8,
    requested_dpi: u16,
    effective_dpi: u16,
    width: u32,
    height: u32,
    quality: RenderQuality = .native,
    diagnostics: ?reader.PageRenderDiagnostics = null,

    pub fn deinit(self: *RenderedPagePng, alloc: Allocator) void {
        alloc.free(self.png);
        self.* = undefined;
    }
};

/// Physical layout of an unencoded page raster. Formats are deliberately
/// closed here rather than inferred from byte length so local inference ABIs
/// can validate the producer/consumer contract before borrowing the bytes.
pub const PixelFormat = enum {
    rgba8,

    pub fn bytesPerPixel(self: @This()) usize {
        return switch (self) {
            .rgba8 => 4,
        };
    }
};

/// Allocator-owned, tightly packed page pixels. Scratch allocations used to
/// parse and paint the page are never retained by this value; only `bytes`
/// survives the render call and must be released with `deinit`.
pub const RenderedPageRaster = struct {
    bytes: []u8,
    pixel_format: PixelFormat,
    width: u32,
    height: u32,
    stride: usize,
    requested_dpi: u16,
    effective_dpi: u16,
    quality: RenderQuality = .native,
    diagnostics: ?reader.PageRenderDiagnostics = null,

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

pub const PageRenderRequest = struct {
    page_number: usize,
    requested_dpi: u16 = 150,
    max_pixels: u64 = 40_000_000,
    max_dimension: u32 = 4096,
    /// Bound retained output for this page. PNG rendering resizes and
    /// re-encodes when necessary; raw raster rendering constrains geometry
    /// before painting because its byte size is known exactly.
    max_output_bytes: ?usize = null,
    min_output_dimension: u32 = 1,
    max_output_attempts: u8 = 8,
};

/// Erased fixed-worker executor supplied by a process-lifetime runtime. Each
/// callback runs completely on one physical worker and receives that worker's
/// private resettable scratch allocator. The executor call is synchronous: no
/// callback may retain either the allocator or scratch-backed memory.
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

/// Admission policy for one render microbatch. The caller deliberately owns
/// document-level windowing: requests.len must not exceed max_batch_pages, so
/// this API cannot accidentally retain a whole large document in memory.
pub const default_render_bytes_per_pixel_reserve: usize = 12;

pub const PageRenderBatchOptions = struct {
    max_batch_pages: usize = 8,
    max_parallel_pages: usize = 1,
    max_inflight_pixels: u64 = 50_000_000,
    max_inflight_bytes: usize = 512 * 1024 * 1024,
    max_retained_png_bytes: usize = 64 * 1024 * 1024,
    /// Hard bound for raw raster bytes retained after completed workers leave
    /// the batch scratch budget. This is intentionally separate from encoded
    /// PNG retention because their sizes have very different distributions.
    max_retained_raster_bytes: usize = 256 * 1024 * 1024,
    bytes_per_pixel_reserve: usize = default_render_bytes_per_pixel_reserve,
    profile: RenderProfile = .ocr,
    cancellation: reader.CancellationProbe = .{},
    /// Production callers borrow this from BackendRuntime. When present,
    /// render jobs use that runtime's bounded executor instead of creating a
    /// batch-local thread team.
    executor: ?PageRenderExecutor = null,
    /// Optional caller-owned immutable fork template. The batch refreshes it
    /// after serial preflight and before worker dispatch, but only walks the
    /// encrypted-stream registry when that registry grew. The owning Reader
    /// and this template must remain exclusively borrowed for the synchronous
    /// batch call.
    fork_template: ?*reader.RenderForkTemplate = null,
    /// Thread-safe allocator that workers may use for retained output bytes.
    /// This must be the result allocator passed to the batch and is accepted
    /// only with a fixed PageRenderExecutor. Successful output allocations are
    /// then detached directly into results instead of copied on the caller
    /// thread. Compatibility executors retain page_allocator-plus-copy.
    concurrent_output_allocator: ?Allocator = null,
    /// Compatibility executor for callers not yet migrated to a fixed PDF CPU
    /// lane. `std.Io` does not guarantee physical thread affinity, so it cannot
    /// provide reusable worker scratch.
    executor_io: ?std.Io = null,
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
    thread_spawn_fallbacks: usize,

    pub fn deinit(self: *RenderedPageRasterBatch, alloc: Allocator) void {
        for (self.results) |*result| result.deinit(alloc);
        alloc.free(self.results);
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
    return try renderParsedPagePngEffectiveAlloc(alloc, parsed, page_number, dpi, max_pixels, .exact);
}

fn renderParsedPagePngEffectiveAlloc(alloc: Allocator, parsed: *reader.Reader, page_number: usize, dpi: u16, max_pixels: u64, profile: RenderProfile) ![]u8 {
    if (page_number == 0 or page_number > try parsed.pageCount()) return error.InvalidPageNumber;
    const rotation = try normalizedPageRotation(try parsed.extractPageRotation(page_number));
    return try renderParsedPagePngEffectiveWithRotationAlloc(alloc, parsed, page_number, dpi, max_pixels, rotation, profile, null, null);
}

fn renderParsedPagePngEffectiveWithRotationAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
    profile: RenderProfile,
    used_compatibility_backend: ?*bool,
    compatibility_session: ?*CompatibilityRenderSession,
) ![]u8 {
    return try renderParsedPagePngEffectiveWithRotationAllocators(
        alloc,
        alloc,
        parsed,
        page_number,
        dpi,
        max_pixels,
        rotation,
        profile,
        used_compatibility_backend,
        compatibility_session,
    );
}

fn renderParsedPagePngEffectiveWithRotationAllocators(
    scratch_alloc: Allocator,
    output_alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
    profile: RenderProfile,
    used_compatibility_backend: ?*bool,
    compatibility_session: ?*CompatibilityRenderSession,
) ![]u8 {
    if (used_compatibility_backend) |value| value.* = false;
    return renderParsedPagePngNativeWithAllocators(scratch_alloc, output_alloc, parsed, page_number, dpi, max_pixels, rotation, profile) catch |err| switch (err) {
        error.UnsupportedStreamFilter,
        error.UnsupportedNativeDecode,
        error.UnsupportedPdfRendering,
        error.InvalidFlateStream,
        error.MissingEndStream,
        error.UnexpectedEof,
        => if (builtin.os.tag == .macos) blk: {
            if (used_compatibility_backend) |value| value.* = true;
            break :blk if (compatibility_session) |session|
                try session.renderPagePngAlloc(
                    output_alloc,
                    page_number,
                    dpi,
                    max_pixels,
                    rotation,
                    parsed.cancellationProbe(),
                )
            else
                try darwin_render.renderPagePngAlloc(output_alloc, parsed.sourceBytes(), page_number, dpi, max_pixels, rotation);
        } else return err,
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
    profile: RenderProfile,
) ![]u8 {
    return try renderParsedPagePngNativeWithAllocators(alloc, alloc, parsed, page_number, dpi, max_pixels, rotation, profile);
}

fn renderParsedPagePngNativeWithAllocators(
    scratch_alloc: Allocator,
    output_alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
    profile: RenderProfile,
) ![]u8 {
    const raw = try renderParsedPageRgbaNativeWithAllocators(scratch_alloc, scratch_alloc, parsed, page_number, dpi, max_pixels, rotation, profile);
    defer scratch_alloc.free(raw.rgba);
    try parsed.checkCancellation();
    return try image.png.encodeRgbaWithCancellation(
        output_alloc,
        @intCast(raw.width),
        @intCast(raw.height),
        raw.rgba,
        .{
            .context = parsed.cancellationProbe().context,
            .is_cancelled_fn = parsed.cancellationProbe().is_cancelled_fn,
        },
    );
}

fn renderParsedPageRgbaNativeAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
    profile: RenderProfile,
) !render.RgbaCanvas {
    return try renderParsedPageRgbaNativeWithAllocators(alloc, alloc, parsed, page_number, dpi, max_pixels, rotation, profile);
}

fn renderParsedPageRgbaNativeWithAllocators(
    scratch_alloc: Allocator,
    output_alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
    profile: RenderProfile,
) !render.RgbaCanvas {
    const reader_alloc = parsed.allocator();
    try parsed.checkCancellation();
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
    try parsed.checkCancellation();
    if (profile == .ocr) {
        try reader.prepareOcrRenderRunsAlloc(reader_alloc, render_runs.image_runs, render_runs.pattern_runs, parsed.cancellationProbe());
    }
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
    defer plain_runs.deinit(scratch_alloc);
    for (runs) |run| {
        const has_pattern = run.fill_pattern_name != null or run.stroke_pattern_name != null;
        if (has_pattern or run.vectorizable) continue;
        try plain_runs.append(scratch_alloc, run);
    }
    // RenderPlan merges every paint kind into group-local schedules and orders
    // them by (paint_order, paint_phase, kind, source index). Pre-sorting three
    // individual arrays here is both redundant and subtly less complete: it
    // ignores paint phase and cannot establish cross-kind ordering.
    try parsed.checkCancellation();
    const raw = try render.renderPageContentRgbaInBoxRotatedWithAllocatorsCancelable(scratch_alloc, output_alloc, page_box, plain_runs.items, image_runs, shading_runs, pattern_runs, shape_runs, rotation, parsed.cancellationProbe());
    errdefer output_alloc.free(raw.rgba);
    try parsed.checkCancellation();
    return raw;
}

fn renderParsedPageRgbaEffectiveWithRotationAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
    profile: RenderProfile,
    used_compatibility_backend: ?*bool,
    compatibility_session: ?*CompatibilityRenderSession,
) !render.RgbaCanvas {
    return try renderParsedPageRgbaEffectiveWithRotationAllocators(
        alloc,
        alloc,
        parsed,
        page_number,
        dpi,
        max_pixels,
        rotation,
        profile,
        used_compatibility_backend,
        compatibility_session,
    );
}

fn renderParsedPageRgbaEffectiveWithRotationAllocators(
    scratch_alloc: Allocator,
    output_alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
    profile: RenderProfile,
    used_compatibility_backend: ?*bool,
    compatibility_session: ?*CompatibilityRenderSession,
) !render.RgbaCanvas {
    if (used_compatibility_backend) |value| value.* = false;
    return renderParsedPageRgbaNativeWithAllocators(scratch_alloc, output_alloc, parsed, page_number, dpi, max_pixels, rotation, profile) catch |err| switch (err) {
        error.UnsupportedStreamFilter,
        error.UnsupportedNativeDecode,
        error.UnsupportedPdfRendering,
        error.InvalidFlateStream,
        error.MissingEndStream,
        error.UnexpectedEof,
        => if (builtin.os.tag == .macos) blk: {
            if (used_compatibility_backend) |value| value.* = true;
            try parsed.checkCancellation();
            const png = if (compatibility_session) |session|
                try session.renderPagePngAlloc(
                    scratch_alloc,
                    page_number,
                    dpi,
                    max_pixels,
                    rotation,
                    parsed.cancellationProbe(),
                )
            else
                try darwin_render.renderPagePngAlloc(scratch_alloc, parsed.sourceBytes(), page_number, dpi, max_pixels, rotation);
            defer scratch_alloc.free(png);
            const decoded = try image.png.decodeRgba(output_alloc, png);
            errdefer output_alloc.free(decoded.rgba);
            try parsed.checkCancellation();
            const pixels = std.math.mul(u64, decoded.width, decoded.height) catch return error.RenderedPageTooLarge;
            if (pixels > max_pixels) return error.RenderedPageTooLarge;
            break :blk .{ .rgba = decoded.rgba, .width = decoded.width, .height = decoded.height };
        } else return err,
        else => return err,
    };
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
/// both the dimension and pixel guards. The requested DPI remains at least 72,
/// but malformed or scan-oriented PDFs sometimes encode pixel dimensions as
/// page points; adaptive rendering may report a lower effective DPI while
/// still producing the largest output admitted by the explicit safety caps.
pub fn renderParsedPagePngAdaptiveAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    max_dimension: u32,
) !RenderedPagePng {
    return try renderParsedPagePngAdaptiveWithProfileAlloc(alloc, parsed, page_number, requested_dpi, max_pixels, max_dimension, .exact);
}

pub fn renderParsedPagePngAdaptiveWithProfileAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    max_dimension: u32,
    profile: RenderProfile,
) !RenderedPagePng {
    return try renderParsedPagePngAdaptiveWithProfileAndCompatibilityAlloc(
        alloc,
        parsed,
        page_number,
        requested_dpi,
        max_pixels,
        max_dimension,
        profile,
        null,
    );
}

fn renderParsedPagePngAdaptiveWithProfileAndCompatibilityAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    max_dimension: u32,
    profile: RenderProfile,
    compatibility_session: ?*CompatibilityRenderSession,
) !RenderedPagePng {
    const geometry = try adaptiveRenderGeometry(parsed, .{
        .page_number = page_number,
        .requested_dpi = requested_dpi,
        .max_pixels = max_pixels,
        .max_dimension = max_dimension,
    });

    return renderParsedPagePngWithGeometryAlloc(
        alloc,
        parsed,
        page_number,
        requested_dpi,
        max_pixels,
        geometry,
        profile,
        compatibility_session,
    );
}

fn renderParsedPagePngWithGeometryAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    geometry: AdaptiveRenderGeometry,
    profile: RenderProfile,
    compatibility_session: ?*CompatibilityRenderSession,
) !RenderedPagePng {
    return try renderParsedPagePngWithGeometryAllocators(
        alloc,
        alloc,
        parsed,
        page_number,
        requested_dpi,
        max_pixels,
        geometry,
        profile,
        compatibility_session,
    );
}

fn renderParsedPagePngWithGeometryAllocators(
    scratch_alloc: Allocator,
    output_alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    geometry: AdaptiveRenderGeometry,
    profile: RenderProfile,
    compatibility_session: ?*CompatibilityRenderSession,
) !RenderedPagePng {
    parsed.clearRenderDiagnostics();
    var used_compatibility_backend = false;
    const png = try renderParsedPagePngEffectiveWithRotationAllocators(scratch_alloc, output_alloc, parsed, page_number, geometry.effective_dpi, max_pixels, geometry.rotation, profile, &used_compatibility_backend, compatibility_session);
    const diagnostics = if (used_compatibility_backend) null else parsed.lastRenderDiagnostics();
    const degraded = if (diagnostics) |value| value.fallback_text_groups != 0 else false;
    return .{
        .png = png,
        .requested_dpi = requested_dpi,
        .effective_dpi = geometry.effective_dpi,
        .width = geometry.width,
        .height = geometry.height,
        .quality = if (used_compatibility_backend) .compatibility_backend else if (degraded) .degraded else .native,
        .diagnostics = diagnostics,
    };
}

/// Parse and render one page directly into owned RGBA8 pixels. This is the
/// unencoded counterpart to the adaptive PNG API and is intended for local
/// inference paths that can borrow a raster for the duration of an invocation.
pub fn renderPageRasterAdaptiveAlloc(
    alloc: Allocator,
    pdf_bytes: []const u8,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    max_dimension: u32,
) !RenderedPageRaster {
    var parsed = try reader.Reader.init(alloc, pdf_bytes);
    defer parsed.deinit();
    return try renderParsedPageRasterAdaptiveAlloc(alloc, &parsed, page_number, requested_dpi, max_pixels, max_dimension);
}

pub fn renderParsedPageRasterAdaptiveAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    max_dimension: u32,
) !RenderedPageRaster {
    return try renderParsedPageRasterAdaptiveWithProfileAlloc(alloc, parsed, page_number, requested_dpi, max_pixels, max_dimension, .exact);
}

pub fn renderParsedPageRasterAdaptiveWithProfileAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    max_dimension: u32,
    profile: RenderProfile,
) !RenderedPageRaster {
    return try renderParsedPageRasterAdaptiveWithProfileAndCompatibilityAlloc(
        alloc,
        parsed,
        page_number,
        requested_dpi,
        max_pixels,
        max_dimension,
        profile,
        null,
    );
}

fn renderParsedPageRasterAdaptiveWithProfileAndCompatibilityAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    max_dimension: u32,
    profile: RenderProfile,
    compatibility_session: ?*CompatibilityRenderSession,
) !RenderedPageRaster {
    const geometry = try adaptiveRenderGeometry(parsed, .{
        .page_number = page_number,
        .requested_dpi = requested_dpi,
        .max_pixels = max_pixels,
        .max_dimension = max_dimension,
    });
    return try renderParsedPageRasterWithGeometryAlloc(
        alloc,
        parsed,
        page_number,
        requested_dpi,
        max_pixels,
        geometry,
        profile,
        compatibility_session,
    );
}

fn renderParsedPageRasterWithGeometryAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    geometry: AdaptiveRenderGeometry,
    profile: RenderProfile,
    compatibility_session: ?*CompatibilityRenderSession,
) !RenderedPageRaster {
    return try renderParsedPageRasterWithGeometryAllocators(
        alloc,
        alloc,
        parsed,
        page_number,
        requested_dpi,
        max_pixels,
        geometry,
        profile,
        compatibility_session,
    );
}

fn renderParsedPageRasterWithGeometryAllocators(
    scratch_alloc: Allocator,
    output_alloc: Allocator,
    parsed: *reader.Reader,
    page_number: usize,
    requested_dpi: u16,
    max_pixels: u64,
    geometry: AdaptiveRenderGeometry,
    profile: RenderProfile,
    compatibility_session: ?*CompatibilityRenderSession,
) !RenderedPageRaster {
    parsed.clearRenderDiagnostics();
    var used_compatibility_backend = false;
    const raw = try renderParsedPageRgbaEffectiveWithRotationAllocators(
        scratch_alloc,
        output_alloc,
        parsed,
        page_number,
        geometry.effective_dpi,
        max_pixels,
        geometry.rotation,
        profile,
        &used_compatibility_backend,
        compatibility_session,
    );
    errdefer output_alloc.free(raw.rgba);
    const width = std.math.cast(u32, raw.width) orelse return error.RenderedPageTooLarge;
    const height = std.math.cast(u32, raw.height) orelse return error.RenderedPageTooLarge;
    const stride = try raw.stride();
    const expected_bytes = std.math.mul(usize, stride, raw.height) catch return error.RenderedPageTooLarge;
    if (raw.rgba.len != expected_bytes or width != geometry.width or height != geometry.height)
        return error.RenderedPageGeometryMismatch;
    const diagnostics = if (used_compatibility_backend) null else parsed.lastRenderDiagnostics();
    const degraded = if (diagnostics) |value| value.fallback_text_groups != 0 else false;
    return .{
        .bytes = raw.rgba,
        .pixel_format = .rgba8,
        .width = width,
        .height = height,
        .stride = stride,
        .requested_dpi = requested_dpi,
        .effective_dpi = geometry.effective_dpi,
        .quality = if (used_compatibility_backend) .compatibility_backend else if (degraded) .degraded else .native,
        .diagnostics = diagnostics,
    };
}

const AdaptiveRenderGeometry = struct {
    effective_dpi: u16,
    width: u32,
    height: u32,
    pixels: u64,
    rotation: render.PageRotation,
};

pub const PageRenderGeometry = struct {
    effective_dpi: u16,
    width: u32,
    height: u32,
    pixels: u64,
};

/// Immutable geometry resolved against one prepared Reader. Keeping this value
/// beside a planner's page item lets batch admission reuse the exact result
/// instead of resolving the page dictionary for a second time. The source
/// identity is checked again at execution, so plans cannot be mixed across
/// prepared documents accidentally.
pub const PreparedPageRenderPlan = struct {
    _request: PageRenderRequest,
    _geometry: AdaptiveRenderGeometry,
    _source_ptr: [*]const u8,
    _source_len: usize,

    pub fn request(self: @This()) PageRenderRequest {
        return self._request;
    }

    pub fn geometry(self: @This()) PageRenderGeometry {
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

    fn matchesSource(self: @This(), parsed: *const reader.Reader) bool {
        const source = parsed.sourceBytes();
        return self._source_ptr == source.ptr and self._source_len == source.len;
    }
};

pub fn prepareParsedPageRenderPlan(
    parsed: *reader.Reader,
    request: PageRenderRequest,
) !PreparedPageRenderPlan {
    const source = parsed.sourceBytes();
    return .{
        ._request = request,
        ._geometry = try adaptiveRenderGeometry(parsed, request),
        ._source_ptr = source.ptr,
        ._source_len = source.len,
    };
}

/// Return the maximum scratch reservation needed by one ordered wave of
/// prepared pages. Pixel admission may split a wave further at execution, so
/// this remains a safe upper bound without reserving the renderer's complete
/// configured ceiling for every window.
pub fn estimatePreparedPageRenderWaveScratchBytes(
    parsed: *const reader.Reader,
    plans: []const PreparedPageRenderPlan,
    max_parallel_pages: usize,
    bytes_per_pixel_reserve: usize,
) !usize {
    if (plans.len == 0 or max_parallel_pages == 0 or bytes_per_pixel_reserve < 4)
        return error.InvalidRenderBatchOptions;
    const worker_fixed = std.math.add(
        usize,
        try parsed.renderForkMetadataBytes(),
        parsed.decode_limits.max_working_set_bytes,
    ) catch return error.RenderBatchAdmissionExceeded;
    const wave_width = @min(max_parallel_pages, plans.len);
    var wave_bytes: usize = 0;
    var peak_bytes: usize = 0;
    for (plans, 0..) |plan, index| {
        if (!plan.matchesSource(parsed)) return error.PreparedPageRenderSourceMismatch;
        const raster_bytes = std.math.mul(
            usize,
            std.math.cast(usize, plan._geometry.pixels) orelse
                return error.RenderBatchAdmissionExceeded,
            bytes_per_pixel_reserve,
        ) catch return error.RenderBatchAdmissionExceeded;
        wave_bytes = std.math.add(usize, wave_bytes, worker_fixed) catch
            return error.RenderBatchAdmissionExceeded;
        wave_bytes = std.math.add(usize, wave_bytes, raster_bytes) catch
            return error.RenderBatchAdmissionExceeded;
        if (index >= wave_width) {
            const expired = plans[index - wave_width];
            const expired_raster_bytes = std.math.mul(
                usize,
                std.math.cast(usize, expired._geometry.pixels) orelse
                    return error.RenderBatchAdmissionExceeded,
                bytes_per_pixel_reserve,
            ) catch return error.RenderBatchAdmissionExceeded;
            wave_bytes -= worker_fixed;
            wave_bytes -= expired_raster_bytes;
        }
        peak_bytes = @max(peak_bytes, wave_bytes);
    }
    return peak_bytes;
}

/// Resolve the raster geometry for a page using the exact same adaptive rules
/// as rendering, without allocating the raster. Document planners use this to
/// bound the aggregate pixels retained by the following inference invocation.
pub fn planParsedPageRenderGeometry(parsed: *reader.Reader, request: PageRenderRequest) !PageRenderGeometry {
    const geometry = try adaptiveRenderGeometry(parsed, request);
    return .{
        .effective_dpi = geometry.effective_dpi,
        .width = geometry.width,
        .height = geometry.height,
        .pixels = geometry.pixels,
    };
}

fn adaptiveRenderGeometry(parsed: *reader.Reader, request: PageRenderRequest) !AdaptiveRenderGeometry {
    if (request.page_number == 0) return error.InvalidPageNumber;
    if (request.requested_dpi < 72 or request.requested_dpi > 600) return error.InvalidRenderDpi;
    if (request.max_pixels == 0 or request.max_dimension == 0) return error.RenderedPageTooLarge;
    const page_count = try parsed.pageCount();
    if (request.page_number > page_count) return error.InvalidPageNumber;
    const box = try parsed.extractPageBox(request.page_number);
    const rotation = try normalizedPageRotation(try parsed.extractPageRotation(request.page_number));
    const swaps_dimensions = rotation == .clockwise_90 or rotation == .clockwise_270;

    return try adaptiveRenderGeometryForBox(box, rotation, swaps_dimensions, request);
}

fn adaptiveRenderGeometryForBox(
    box: reader.PageBox,
    rotation: render.PageRotation,
    swaps_dimensions: bool,
    request: PageRenderRequest,
) !AdaptiveRenderGeometry {
    const point_width = box.max_x - box.min_x;
    const point_height = box.max_y - box.min_y;
    if (!(point_width > 0) or !(point_height > 0) or
        !std.math.isFinite(point_width) or !std.math.isFinite(point_height))
        return error.InvalidPageBox;

    // ceil(points * dpi / 72) <= max_dimension exactly when the unrounded
    // extent is <= max_dimension. Use that direct bound to avoid probing as
    // many as 600 DPIs. `ceil` deliberately leaves a one-DPI rounding margin;
    // the monotonic search below resolves both that margin and the pixel cap.
    const dimension_bound = @min(
        @as(f64, @floatFromInt(request.requested_dpi)),
        @min(
            @ceil(@as(f64, @floatFromInt(request.max_dimension)) * 72.0 / point_width),
            @ceil(@as(f64, @floatFromInt(request.max_dimension)) * 72.0 / point_height),
        ),
    );
    if (!(dimension_bound >= 1.0)) return error.RenderedPageTooLarge;
    var lower: u16 = minimum_direct_render_dpi;
    var upper: u16 = @intFromFloat(dimension_bound);
    var best: ?AdaptiveRenderGeometry = null;
    while (lower <= upper) {
        const dpi: u16 = lower + (upper - lower) / 2;
        const geometry = renderGeometryAtDpi(box, rotation, swaps_dimensions, dpi) catch {
            if (dpi == minimum_direct_render_dpi) break;
            upper = dpi - 1;
            continue;
        };
        if (geometry.width <= request.max_dimension and
            geometry.height <= request.max_dimension and
            geometry.pixels <= request.max_pixels)
        {
            best = geometry;
            if (dpi == upper) break;
            lower = dpi + 1;
        } else {
            if (dpi == minimum_direct_render_dpi) break;
            upper = dpi - 1;
        }
    }
    return best orelse error.RenderedPageTooLarge;
}

fn renderGeometryAtDpi(
    box: reader.PageBox,
    rotation: render.PageRotation,
    swaps_dimensions: bool,
    dpi: u16,
) !AdaptiveRenderGeometry {
    const scale = @as(f64, @floatFromInt(dpi)) / 72.0;
    const unrotated_width = rasterAxisExtent(box.min_x, box.max_x, scale);
    const unrotated_height = rasterAxisExtent(box.min_y, box.max_y, scale);
    const width_f = if (swaps_dimensions) unrotated_height else unrotated_width;
    const height_f = if (swaps_dimensions) unrotated_width else unrotated_height;
    if (width_f > @as(f64, @floatFromInt(std.math.maxInt(u32))) or
        height_f > @as(f64, @floatFromInt(std.math.maxInt(u32))))
        return error.RenderedPageTooLarge;
    const width: u32 = @intFromFloat(width_f);
    const height: u32 = @intFromFloat(height_f);
    return .{
        .effective_dpi = dpi,
        .width = width,
        .height = height,
        .pixels = @as(u64, width) * @as(u64, height),
        .rotation = rotation,
    };
}

const RenderBatchBudget = struct {
    live_bytes: std.atomic.Value(usize) = .init(0),
    max_live_bytes: usize,
    limit_exceeded: std.atomic.Value(bool) = .init(false),

    fn reserve(self: *@This(), bytes: usize) bool {
        var current = self.live_bytes.load(.acquire);
        while (bytes <= self.max_live_bytes -| current) {
            if (self.live_bytes.cmpxchgWeak(current, current + bytes, .acq_rel, .acquire)) |observed| {
                current = observed;
                continue;
            }
            return true;
        }
        self.limit_exceeded.store(true, .release);
        return false;
    }

    fn release(self: *@This(), bytes: usize) void {
        const previous = self.live_bytes.fetchSub(bytes, .acq_rel);
        std.debug.assert(previous >= bytes);
    }
};

const RenderWorkerBudgetAllocator = struct {
    backing: Allocator,
    shared: ?*RenderBatchBudget = null,
    live_bytes: usize = 0,
    max_live_bytes: usize,
    limit_exceeded: bool = false,
    /// Arena-backed lane scratch does not return physical memory on individual
    /// frees. Charge it cumulatively until the job finishes; the lane resets
    /// the arena before releasing its corresponding window charge.
    account_frees: bool = true,

    fn allocator(self: *RenderWorkerBudgetAllocator) Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *RenderWorkerBudgetAllocator = @ptrCast(@alignCast(ctx));
        if (len > self.max_live_bytes -| self.live_bytes) {
            self.limit_exceeded = true;
            return null;
        }
        if (self.shared) |shared| if (!shared.reserve(len)) {
            self.limit_exceeded = true;
            return null;
        };
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            if (self.shared) |shared| shared.release(len);
            return null;
        };
        self.live_bytes += len;
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *RenderWorkerBudgetAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > self.max_live_bytes -| self.live_bytes) {
            self.limit_exceeded = true;
            return false;
        }
        if (growth > 0) if (self.shared) |shared| if (!shared.reserve(growth)) {
            self.limit_exceeded = true;
            return false;
        };
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) {
            if (growth > 0) if (self.shared) |shared| shared.release(growth);
            return false;
        }
        if (self.account_frees and memory.len > new_len) {
            if (self.shared) |shared| shared.release(memory.len - new_len);
            self.live_bytes -= memory.len - new_len;
        } else {
            self.live_bytes += growth;
        }
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *RenderWorkerBudgetAllocator = @ptrCast(@alignCast(ctx));
        const growth = new_len -| memory.len;
        if (growth > self.max_live_bytes -| self.live_bytes) {
            self.limit_exceeded = true;
            return null;
        }
        if (growth > 0) if (self.shared) |shared| if (!shared.reserve(growth)) {
            self.limit_exceeded = true;
            return null;
        };
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse {
            if (growth > 0) if (self.shared) |shared| shared.release(growth);
            return null;
        };
        if (self.account_frees and memory.len > new_len) {
            if (self.shared) |shared| shared.release(memory.len - new_len);
            self.live_bytes -= memory.len - new_len;
        } else {
            self.live_bytes += growth;
        }
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *RenderWorkerBudgetAllocator = @ptrCast(@alignCast(ctx));
        self.backing.rawFree(memory, alignment, ret_addr);
        if (self.account_frees) {
            self.live_bytes -|= memory.len;
            if (self.shared) |shared| shared.release(memory.len);
        }
    }

    /// Relinquish accounting for an allocation without freeing it. This is
    /// valid only when ownership moves to a result that uses `backing`
    /// directly; the budget wrapper itself must never be used to free it
    /// afterward.
    fn detachOwned(self: *@This(), memory: []u8) void {
        std.debug.assert(self.account_frees);
        std.debug.assert(self.live_bytes >= memory.len);
        self.live_bytes -= memory.len;
        if (self.shared) |shared| shared.release(memory.len);
    }

    fn releaseAllCharges(self: *@This()) void {
        std.debug.assert(!self.account_frees);
        if (self.shared) |shared| shared.release(self.live_bytes);
        self.live_bytes = 0;
    }
};

const RenderBatchOutputKind = enum { png, raster };

const PreparedRenderPage = struct {
    request_index: usize,
    request: PageRenderRequest,
    pixels: u64,
    admitted_bytes: usize,
    worker_limit_bytes: usize,
    geometry: AdaptiveRenderGeometry,
};

const RenderWaveControl = struct {
    external: reader.CancellationProbe,
    stopped: std.atomic.Value(bool) = .init(false),
    active: std.atomic.Value(usize) = .init(0),
    peak_active: std.atomic.Value(usize) = .init(0),

    fn isCancelled(context: ?*const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(context orelse return true));
        if (self.stopped.load(.acquire)) return true;
        if (self.external.is_cancelled_fn) |check| return check(self.external.context);
        return false;
    }

    fn probe(self: *const @This()) reader.CancellationProbe {
        return .{ .context = self, .is_cancelled_fn = isCancelled };
    }

    fn stop(self: *@This()) void {
        self.stopped.store(true, .release);
    }

    fn enterRender(self: *@This()) void {
        const current = self.active.fetchAdd(1, .acq_rel) + 1;
        var peak = self.peak_active.load(.acquire);
        while (current > peak) {
            peak = self.peak_active.cmpxchgWeak(peak, current, .acq_rel, .acquire) orelse break;
        }
    }

    fn leaveRender(self: *@This()) void {
        _ = self.active.fetchSub(1, .acq_rel);
    }
};

/// Per-batch worker rendezvous. Threads are created once and reused across all
/// admitted waves in the window. Each lane owns a private render reader. Forks
/// borrow the source's immutable document index; page-allocator lanes retain
/// their bounded font cache until the batch ends, while executor-arena lanes
/// release all reader state at the end of each callback. The shared allocator
/// remains the hard aggregate cap for both retained caches and page work.
const RenderBatchThreadControl = struct {
    mutex: std.Io.Mutex = .init,
    wave_ready: std.Io.Condition = .init,
    wave_complete: std.Io.Condition = .init,
    sync_io: std.Io,
    generation: usize = 0,
    completed: usize = 0,
    active_len: usize = 0,
    stopping: bool = false,

    fn init() @This() {
        return .{ .sync_io = std.Io.Threaded.global_single_threaded.io() };
    }

    fn startWave(self: *@This(), active_len: usize) void {
        self.mutex.lockUncancelable(self.sync_io);
        self.active_len = active_len;
        self.completed = 0;
        self.generation +%= 1;
        self.wave_ready.broadcast(self.sync_io);
        self.mutex.unlock(self.sync_io);
    }

    fn awaitWave(self: *@This(), previous_generation: *usize, worker_index: usize) ?bool {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (!self.stopping and self.generation == previous_generation.*) {
            self.wave_ready.waitUncancelable(self.sync_io, &self.mutex);
        }
        if (self.stopping) return null;
        previous_generation.* = self.generation;
        return worker_index < self.active_len;
    }

    fn completeWave(self: *@This()) void {
        self.mutex.lockUncancelable(self.sync_io);
        self.completed += 1;
        self.wave_complete.signal(self.sync_io);
        self.mutex.unlock(self.sync_io);
    }

    fn waitForWorkers(self: *@This(), expected: usize) void {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (self.completed < expected) self.wave_complete.waitUncancelable(self.sync_io, &self.mutex);
    }

    fn stop(self: *@This()) void {
        self.mutex.lockUncancelable(self.sync_io);
        self.stopping = true;
        self.wave_ready.broadcast(self.sync_io);
        self.mutex.unlock(self.sync_io);
    }
};

const CombinedCancellation = struct {
    source: reader.CancellationProbe,
    operation: reader.CancellationProbe,

    fn isCancelled(context: ?*const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(context orelse return true));
        if (self.source.is_cancelled_fn) |check| {
            if (check(self.source.context)) return true;
        }
        if (self.operation.is_cancelled_fn) |check| {
            if (check(self.operation.context)) return true;
        }
        return false;
    }

    fn probe(self: *const @This()) reader.CancellationProbe {
        return .{ .context = self, .is_cancelled_fn = isCancelled };
    }
};

const PageRenderWorker = struct {
    request: PageRenderRequest,
    profile: RenderProfile,
    scratch_budget: RenderWorkerBudgetAllocator,
    output_budget: RenderWorkerBudgetAllocator,
    fork_template: *const reader.RenderForkTemplate,
    shared_budget: *RenderBatchBudget,
    parsed: ?reader.Reader = null,
    output_kind: RenderBatchOutputKind,
    rendered_png: ?RenderedPagePng = null,
    rendered_raster: ?RenderedPageRaster = null,
    failure: ?anyerror = null,
    render_elapsed_ns: u64 = 0,
    thread: ?std.Thread = null,
    wave_control: *RenderWaveControl = undefined,
    planned_geometry: AdaptiveRenderGeometry = undefined,
    thread_control: *RenderBatchThreadControl,
    worker_index: usize,
    task_configured: bool = false,
    scratch_initialized: bool = false,
    scratch_persistent: bool = false,
    compatibility_session: ?*CompatibilityRenderSession = null,

    fn initBase(self: *PageRenderWorker, thread_control: *RenderBatchThreadControl, worker_index: usize) void {
        self.* = .{
            .request = undefined,
            .profile = undefined,
            .scratch_budget = undefined,
            .output_budget = undefined,
            .fork_template = undefined,
            .shared_budget = undefined,
            .output_kind = undefined,
            .thread_control = thread_control,
            .worker_index = worker_index,
        };
    }

    fn configure(self: *PageRenderWorker, fork_template: *const reader.RenderForkTemplate, prepared: PreparedRenderPage, profile: RenderProfile, output_kind: RenderBatchOutputKind, wave_control: *RenderWaveControl, shared_budget: *RenderBatchBudget, output_allocator: ?Allocator, max_retained_output_bytes: usize, compatibility_session: ?*CompatibilityRenderSession) void {
        std.debug.assert(!self.task_configured);
        self.request = prepared.request;
        self.profile = profile;
        self.output_kind = output_kind;
        self.fork_template = fork_template;
        self.shared_budget = shared_budget;
        self.output_budget = .{
            .backing = output_allocator orelse std.heap.page_allocator,
            // Direct retained output is charged by the caller's concurrent
            // allocator. Compatibility output remains part of the renderer's
            // physical batch budget until it is copied on the caller thread.
            .shared = if (output_allocator == null) shared_budget else null,
            // Both choices retain a hard aggregate boundary: the shared render
            // budget for compatibility output, or the caller's thread-safe
            // window allocator for direct retained output.
            .max_live_bytes = if (output_allocator == null)
                shared_budget.max_live_bytes
            else
                max_retained_output_bytes,
        };
        self.wave_control = wave_control;
        self.compatibility_session = compatibility_session;
        self.planned_geometry = prepared.geometry;
        self.rendered_png = null;
        self.rendered_raster = null;
        self.failure = null;
        self.render_elapsed_ns = 0;
        self.task_configured = true;
    }

    fn deinitTask(self: *PageRenderWorker) void {
        if (!self.scratch_persistent) self.deinitScratch();
        if (self.task_configured) {
            if (self.rendered_png) |*rendered| rendered.deinit(self.output_budget.allocator());
            if (self.rendered_raster) |*rendered| rendered.deinit(self.output_budget.allocator());
        }
        self.rendered_png = null;
        self.rendered_raster = null;
        self.task_configured = false;
    }

    fn deinitLane(self: *PageRenderWorker) void {
        self.deinitTask();
        self.deinitScratch();
    }

    fn threadMain(self: *PageRenderWorker) void {
        var generation: usize = 0;
        while (self.thread_control.awaitWave(&generation, self.worker_index)) |active| {
            if (active) self.runPrepared();
            self.thread_control.completeWave();
        }
    }

    fn runAsync(self: *PageRenderWorker) std.Io.Cancelable!void {
        self.runPrepared();
    }

    fn runPrepared(self: *PageRenderWorker) void {
        self.runWithScratch(std.heap.page_allocator, false);
    }

    fn runOnExecutor(context: *anyopaque, scratch: Allocator) void {
        const self: *PageRenderWorker = @ptrCast(@alignCast(context));
        self.runWithScratch(scratch, true);
    }

    fn runWithScratch(self: *PageRenderWorker, scratch: Allocator, cumulative_accounting: bool) void {
        std.debug.assert(self.task_configured);
        if (!self.scratch_initialized) {
            self.scratch_budget = .{
                .backing = scratch,
                .shared = self.shared_budget,
                .max_live_bytes = self.shared_budget.max_live_bytes,
                .account_frees = !cumulative_accounting,
            };
            self.parsed = self.fork_template.instantiate(self.scratch_budget.allocator(), self.wave_control.probe()) catch |err| {
                self.recordRenderFailure(err);
                if (cumulative_accounting) self.scratch_budget.releaseAllCharges();
                return;
            };
            self.scratch_initialized = true;
            // Page-allocator lanes live for the entire render batch and can
            // safely retain immutable parsed-font work between their pages.
            // Executor scratch is arena-scoped to one callback and must not
            // escape it.
            self.scratch_persistent = !cumulative_accounting;
        } else {
            std.debug.assert(self.scratch_persistent and !cumulative_accounting);
            std.debug.assert(self.scratch_budget.backing.ptr == scratch.ptr and
                self.scratch_budget.backing.vtable == scratch.vtable);
            self.parsed.?.setCancellationProbe(self.wave_control.probe());
            self.scratch_budget.limit_exceeded = false;
        }
        defer if (cumulative_accounting) self.deinitScratch();
        self.wave_control.probe().check() catch {
            self.failure = error.Canceled;
            return;
        };
        const render_started_ns = monotonicNowNs();
        self.wave_control.enterRender();
        defer {
            self.wave_control.leaveRender();
            const finished_ns = monotonicNowNs();
            self.render_elapsed_ns = finished_ns -| render_started_ns;
        }
        switch (self.output_kind) {
            .png => self.runPreparedPng(),
            .raster => self.runPreparedRaster(),
        }
    }

    fn deinitScratch(self: *PageRenderWorker) void {
        if (!self.scratch_initialized) return;
        if (self.parsed) |*parsed| parsed.deinit();
        self.parsed = null;
        if (!self.scratch_budget.account_frees) self.scratch_budget.releaseAllCharges();
        self.scratch_initialized = false;
        self.scratch_persistent = false;
    }

    fn recordRenderFailure(self: *PageRenderWorker, err: anyerror) void {
        self.failure = if (err == error.OutOfMemory and
            (self.scratch_budget.limit_exceeded or self.output_budget.limit_exceeded))
            error.RenderWorkerMemoryLimitExceeded
        else
            err;
        if (self.failure.? == error.OutOfMemory or self.failure.? == error.Canceled)
            self.wave_control.stop();
    }

    fn runPreparedRaster(self: *PageRenderWorker) void {
        const request = self.request;
        const rendered = renderParsedPageRasterWithGeometryAllocators(
            self.scratch_budget.allocator(),
            self.output_budget.allocator(),
            &self.parsed.?,
            request.page_number,
            request.requested_dpi,
            request.max_pixels,
            self.planned_geometry,
            self.profile,
            self.compatibility_session,
        ) catch |err| {
            self.recordRenderFailure(err);
            return;
        };
        if (request.max_output_bytes) |limit| if (rendered.bytes.len > limit) {
            var oversized = rendered;
            oversized.deinit(self.output_budget.allocator());
            self.failure = error.RenderedPageOutputTooLarge;
            return;
        };
        self.rendered_raster = rendered;
    }

    fn runPreparedPng(self: *PageRenderWorker) void {
        const request = self.request;
        var rendered = renderParsedPagePngWithGeometryAllocators(
            self.scratch_budget.allocator(),
            self.output_budget.allocator(),
            &self.parsed.?,
            request.page_number,
            request.requested_dpi,
            request.max_pixels,
            self.planned_geometry,
            self.profile,
            self.compatibility_session,
        ) catch |err| {
            self.recordRenderFailure(err);
            return;
        };
        const output_limit = request.max_output_bytes orelse {
            self.rendered_png = rendered;
            return;
        };
        if (rendered.png.len <= output_limit) {
            self.rendered_png = rendered;
            return;
        }
        const minimum = @max(@as(u32, 1), request.min_output_dimension);
        const rendered_dimension = @max(rendered.width, rendered.height);
        if (request.max_output_attempts <= 1 or rendered_dimension <= minimum) {
            rendered.deinit(self.output_budget.allocator());
            self.failure = error.RenderedPageOutputTooLarge;
            return;
        }

        const previous_effective_dpi = rendered.effective_dpi;
        const previous_png = rendered.png;
        rendered.png = &.{};
        const resized = render.resizeOwnedPngToOutputLimitAlloc(
            self.output_budget.allocator(),
            previous_png,
            output_limit,
            minimum,
            request.max_output_attempts - 1,
            self.wave_control.probe(),
        ) catch |err| {
            self.recordRenderFailure(err);
            return;
        };
        rendered.png = resized.png;
        rendered.width = resized.width;
        rendered.height = resized.height;
        rendered.effective_dpi = @max(
            1,
            @as(u16, @intCast((@as(u64, previous_effective_dpi) * @max(resized.width, resized.height)) / rendered_dimension)),
        );
        self.rendered_png = rendered;
    }
};

fn monotonicNowNs() u64 {
    if (comptime builtin.os.tag == .freestanding or builtin.os.tag == .windows) return 0;
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => {},
        else => return 0,
    }
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

fn renderPageReservedBytes(parsed: *const reader.Reader, pixels: u64, options: PageRenderBatchOptions) !usize {
    const raster_bytes_u64 = std.math.mul(u64, pixels, std.math.cast(u64, options.bytes_per_pixel_reserve) orelse return error.RenderBatchAdmissionExceeded) catch return error.RenderBatchAdmissionExceeded;
    const raster_bytes = std.math.cast(usize, raster_bytes_u64) orelse return error.RenderBatchAdmissionExceeded;
    return std.math.add(usize, raster_bytes, try parsed.renderForkMetadataBytes()) catch return error.RenderBatchAdmissionExceeded;
}

fn prepareRenderPageForAdmission(
    parsed: *reader.Reader,
    request: PageRenderRequest,
    planned_geometry: ?AdaptiveRenderGeometry,
    options: PageRenderBatchOptions,
    output_kind: RenderBatchOutputKind,
    request_index: usize,
) !PreparedRenderPage {
    const fixed_bytes = std.math.add(
        usize,
        try parsed.renderForkMetadataBytes(),
        parsed.decode_limits.max_working_set_bytes,
    ) catch return error.RenderBatchAdmissionExceeded;
    if (fixed_bytes >= options.max_inflight_bytes) return error.RenderBatchAdmissionExceeded;
    const raster_budget = options.max_inflight_bytes - fixed_bytes;
    const memory_pixels = raster_budget / options.bytes_per_pixel_reserve;
    if (memory_pixels == 0) return error.RenderBatchAdmissionExceeded;

    var adjusted = request;
    if (output_kind == .raster) if (request.max_output_bytes) |output_limit| {
        const retained_pixels = output_limit / PixelFormat.rgba8.bytesPerPixel();
        if (retained_pixels == 0) return error.RenderedPageOutputTooLarge;
        adjusted.max_pixels = @min(
            adjusted.max_pixels,
            std.math.cast(u64, retained_pixels) orelse std.math.maxInt(u64),
        );
    };
    adjusted.max_pixels = @min(
        adjusted.max_pixels,
        @min(
            options.max_inflight_pixels,
            std.math.cast(u64, memory_pixels) orelse std.math.maxInt(u64),
        ),
    );
    const geometry = if (planned_geometry) |planned|
        if (planned.pixels <= adjusted.max_pixels and
            planned.width <= adjusted.max_dimension and
            planned.height <= adjusted.max_dimension)
            planned
        else
            try adaptiveRenderGeometry(parsed, adjusted)
    else
        try adaptiveRenderGeometry(parsed, adjusted);
    const admitted_bytes = try renderPageReservedBytes(parsed, geometry.pixels, options);
    const worker_limit_bytes = std.math.add(usize, admitted_bytes, parsed.decode_limits.max_working_set_bytes) catch
        return error.RenderBatchAdmissionExceeded;
    if (geometry.pixels > options.max_inflight_pixels or worker_limit_bytes > options.max_inflight_bytes)
        return error.RenderBatchAdmissionExceeded;
    if (output_kind == .raster and request.max_output_bytes != null and
        @max(geometry.width, geometry.height) < @max(@as(u32, 1), request.min_output_dimension))
        return error.RenderedPageOutputTooLarge;
    return .{
        .request_index = request_index,
        .request = adjusted,
        .pixels = geometry.pixels,
        .admitted_bytes = admitted_bytes,
        .worker_limit_bytes = worker_limit_bytes,
        .geometry = geometry,
    };
}

/// Render one bounded page window while preserving request order.
///
/// Page parse/render failures are returned in the corresponding result.
/// Allocator failure, invalid batch policy, and retained-output exhaustion are
/// systemic errors. The source Reader is used only while workers are prepared;
/// all concurrent work happens on independently allocated render forks.
pub fn renderParsedPagesBatchAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    requests: []const PageRenderRequest,
    options: PageRenderBatchOptions,
) !RenderedPageBatch {
    return renderParsedPageWorkBatchAlloc(.png, alloc, parsed, requests, null, options);
}

/// Execute page plans previously resolved against `parsed`. This is the
/// planner-facing batch entry point: admission still narrows geometry if its
/// aggregate byte/pixel ceilings require it, but the ordinary path does not
/// repeat page-box and rotation discovery.
pub fn renderPreparedPagesBatchAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    plans: []const PreparedPageRenderPlan,
    options: PageRenderBatchOptions,
) !RenderedPageBatch {
    for (plans) |plan| if (!plan.matchesSource(parsed))
        return error.PreparedPageRenderSourceMismatch;
    return renderParsedPageWorkBatchAlloc(.png, alloc, parsed, &.{}, plans, options);
}

/// Raw-raster variant of `renderParsedPagesBatchAlloc`. Results retain only
/// owned RGBA bytes and metadata; worker readers, renderer plans, and decode
/// scratch are released at the end of each admitted wave.
pub fn renderParsedPagesRasterBatchAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    requests: []const PageRenderRequest,
    options: PageRenderBatchOptions,
) !RenderedPageRasterBatch {
    return renderParsedPageWorkBatchAlloc(.raster, alloc, parsed, requests, null, options);
}

pub fn renderPreparedPagesRasterBatchAlloc(
    alloc: Allocator,
    parsed: *reader.Reader,
    plans: []const PreparedPageRenderPlan,
    options: PageRenderBatchOptions,
) !RenderedPageRasterBatch {
    for (plans) |plan| if (!plan.matchesSource(parsed))
        return error.PreparedPageRenderSourceMismatch;
    return renderParsedPageWorkBatchAlloc(.raster, alloc, parsed, &.{}, plans, options);
}

fn renderParsedPageWorkBatchAlloc(
    comptime output_kind: RenderBatchOutputKind,
    alloc: Allocator,
    parsed: *reader.Reader,
    requests: []const PageRenderRequest,
    prepared_plans: ?[]const PreparedPageRenderPlan,
    options: PageRenderBatchOptions,
) !(if (output_kind == .png) RenderedPageBatch else RenderedPageRasterBatch) {
    if (options.max_batch_pages == 0 or options.max_parallel_pages == 0 or
        options.max_inflight_pixels == 0 or options.max_inflight_bytes == 0 or
        (output_kind == .png and options.max_retained_png_bytes == 0) or
        (output_kind == .raster and options.max_retained_raster_bytes == 0) or
        options.bytes_per_pixel_reserve < 4 or
        (options.executor != null and options.executor.?.concurrent_capacity == 0) or
        (options.executor != null and options.executor_io != null) or
        (options.concurrent_output_allocator != null and
            (options.executor == null or
                !allocatorsEqual(alloc, options.concurrent_output_allocator.?))))
    {
        return error.InvalidRenderBatchOptions;
    }
    const request_count = if (prepared_plans) |plans| plans.len else requests.len;
    if (request_count > options.max_batch_pages) return error.RenderBatchTooLarge;
    const source_cancellation = parsed.cancellationProbe();
    const combined_cancellation = CombinedCancellation{
        .source = source_cancellation,
        .operation = options.cancellation,
    };
    const cancellation = combined_cancellation.probe();
    try cancellation.check();
    // Preflight reads the source Reader serially before private render forks
    // exist. Install the composed probe for that phase, then restore the
    // caller's session state before returning.
    parsed.setCancellationProbe(cancellation);
    defer parsed.setCancellationProbe(source_cancellation);

    const Result = if (output_kind == .png) PageRenderResult else PageRasterResult;
    const results = try alloc.alloc(Result, request_count);
    var initialized_results: usize = 0;
    errdefer {
        for (results[0..initialized_results]) |*result| result.deinit(alloc);
        alloc.free(results);
    }
    for (0..request_count) |i| {
        const request = if (prepared_plans) |plans| plans[i]._request else requests[i];
        results[i] = .{ .page_number = request.page_number };
        initialized_results += 1;
    }

    const prepared = try alloc.alloc(?PreparedRenderPage, request_count);
    defer alloc.free(prepared);
    @memset(prepared, null);
    for (0..request_count) |i| {
        const request = if (prepared_plans) |plans| plans[i]._request else requests[i];
        const geometry = if (prepared_plans) |plans| plans[i]._geometry else null;
        prepared[i] = prepareRenderPageForAdmission(parsed, request, geometry, options, output_kind, i) catch |err| {
            results[i].failure = err;
            continue;
        };
    }

    // Freeze the source Reader after every serial preflight lookup and before
    // any worker is dispatched. Worker-local Readers are instantiated only
    // from this immutable snapshot, so renderer threads never race lazy page
    // discovery or encrypted-stream bookkeeping on `parsed`. Document-scoped
    // callers may retain the snapshot across windows; ordinary callers keep a
    // compatibility-local snapshot for this invocation.
    var local_render_fork_template: ?reader.RenderForkTemplate = null;
    defer if (local_render_fork_template) |*template| template.deinit();
    const render_fork_template: *const reader.RenderForkTemplate = if (options.fork_template) |template| blk: {
        try template.refreshFrom(parsed, cancellation);
        break :blk template;
    } else blk: {
        local_render_fork_template = try parsed.prepareRenderForkTemplate(alloc, cancellation);
        break :blk &local_render_fork_template.?;
    };

    const requested_worker_capacity = @min(options.max_parallel_pages, request_count);
    const worker_capacity = if (options.executor) |executor|
        @min(requested_worker_capacity, executor.concurrent_capacity)
    else
        requested_worker_capacity;
    const workers = try alloc.alloc(PageRenderWorker, worker_capacity);
    defer alloc.free(workers);
    const wave = try alloc.alloc(PreparedRenderPage, worker_capacity);
    defer alloc.free(wave);
    const executor_contexts = try alloc.alloc(*anyopaque, worker_capacity);
    defer alloc.free(executor_contexts);
    var shared_budget = RenderBatchBudget{ .max_live_bytes = options.max_inflight_bytes };
    const max_retained_output_bytes = if (output_kind == .png)
        options.max_retained_png_bytes
    else
        options.max_retained_raster_bytes;
    var compatibility_session: CompatibilityRenderSession = if (builtin.os.tag == .macos)
        darwin_render.SharedSession.init(parsed.sourceBytes())
    else
        .{};
    defer if (builtin.os.tag == .macos) compatibility_session.deinit();

    var thread_control = RenderBatchThreadControl.init();
    for (workers, 0..) |*worker, i| worker.initBase(&thread_control, i);
    var thread_spawn_fallbacks: usize = 0;
    var spawned_workers: usize = 0;
    if (worker_capacity > 1 and options.executor == null and options.executor_io == null) {
        for (workers) |*worker| {
            worker.thread = std.Thread.spawn(.{}, PageRenderWorker.threadMain, .{worker}) catch blk: {
                thread_spawn_fallbacks += 1;
                break :blk null;
            };
            if (worker.thread != null) spawned_workers += 1;
        }
    }
    defer {
        thread_control.stop();
        for (workers) |*worker| {
            if (worker.thread) |thread| thread.join();
            worker.deinitLane();
        }
    }

    var next_request: usize = 0;
    var retained_output_bytes: usize = 0;
    var peak_launched_workers: usize = 0;
    var peak_parallelism: usize = 0;
    var peak_admitted_pixels: u64 = 0;
    var peak_admitted_bytes: usize = 0;
    while (next_request < request_count) {
        try cancellation.check();
        var wave_len: usize = 0;
        var wave_pixels: u64 = 0;
        var wave_bytes: usize = 0;
        while (next_request < request_count and wave_len < worker_capacity) : (next_request += 1) {
            const candidate = prepared[next_request] orelse continue;
            const next_pixels = std.math.add(u64, wave_pixels, candidate.pixels) catch break;
            // Account for the raster/fork reservation and the page's bounded
            // decode working set. The shared allocator enforces the same
            // aggregate ceiling at runtime, so an admitted wave cannot depend
            // on all workers avoiding their declared scratch limit at once.
            const next_bytes = std.math.add(usize, wave_bytes, candidate.worker_limit_bytes) catch break;
            if (wave_len > 0 and (next_pixels > options.max_inflight_pixels or next_bytes > options.max_inflight_bytes)) break;
            wave[wave_len] = candidate;
            wave_len += 1;
            wave_pixels = next_pixels;
            wave_bytes = next_bytes;
        }
        if (wave_len == 0) continue;
        peak_admitted_pixels = @max(peak_admitted_pixels, wave_pixels);
        peak_admitted_bytes = @max(peak_admitted_bytes, wave_bytes);

        var wave_control = RenderWaveControl{ .external = cancellation };
        var initialized_workers: usize = 0;
        defer {
            for (workers[0..initialized_workers]) |*worker| worker.deinitTask();
        }
        for (wave[0..wave_len], 0..) |candidate, i| {
            workers[i].configure(
                render_fork_template,
                candidate,
                options.profile,
                output_kind,
                &wave_control,
                &shared_budget,
                options.concurrent_output_allocator,
                max_retained_output_bytes,
                &compatibility_session,
            );
            initialized_workers += 1;
        }

        if (options.executor) |executor| {
            for (workers[0..wave_len], executor_contexts[0..wave_len]) |*worker, *context|
                context.* = @ptrCast(worker);
            _ = try executor.runBatch(
                executor_contexts[0..wave_len],
                PageRenderWorker.runOnExecutor,
                options.max_inflight_bytes,
            );
            peak_launched_workers = @max(peak_launched_workers, wave_len);
        } else if (options.executor_io) |io| {
            var group: std.Io.Group = .init;
            errdefer group.cancel(io);
            for (workers[0 .. wave_len - 1]) |*worker|
                group.async(io, PageRenderWorker.runAsync, .{worker});
            workers[wave_len - 1].runPrepared();
            try group.await(io);
            peak_launched_workers = @max(peak_launched_workers, wave_len);
        } else if (wave_len == 1 and spawned_workers == 0) {
            // The conservative default does not need a helper thread. Keeping
            // the serial case inline avoids thread setup cost while exercising
            // the exact same batch ownership and admission path.
            workers[0].runPrepared();
        } else {
            thread_control.startWave(wave_len);
            var active_spawned_workers: usize = 0;
            for (workers[0..wave_len]) |*worker| if (worker.thread == null) worker.runPrepared();
            for (workers[0..wave_len]) |worker| if (worker.thread != null) {
                active_spawned_workers += 1;
            };
            thread_control.waitForWorkers(spawned_workers);
            peak_launched_workers = @max(peak_launched_workers, active_spawned_workers);
        }

        peak_parallelism = @max(peak_parallelism, wave_control.peak_active.load(.acquire));

        for (wave[0..wave_len], workers[0..wave_len]) |candidate, *worker| {
            results[candidate.request_index].render_elapsed_ns = worker.render_elapsed_ns;
            if (worker.failure) |err| {
                if (err == error.OutOfMemory or err == error.Canceled) return err;
                results[candidate.request_index].failure = err;
                continue;
            }
            if (output_kind == .png) {
                const rendered = worker.rendered_png.?;
                const next_retained = std.math.add(usize, retained_output_bytes, rendered.png.len) catch return error.RenderBatchRetainedBytesExceeded;
                if (next_retained > options.max_retained_png_bytes) return error.RenderBatchRetainedBytesExceeded;
                const owned_png = if (allocatorsEqual(alloc, worker.output_budget.backing)) blk: {
                    worker.output_budget.detachOwned(rendered.png);
                    worker.rendered_png = null;
                    break :blk rendered.png;
                } else try alloc.dupe(u8, rendered.png);
                results[candidate.request_index].rendered = .{
                    .png = owned_png,
                    .requested_dpi = rendered.requested_dpi,
                    .effective_dpi = rendered.effective_dpi,
                    .width = rendered.width,
                    .height = rendered.height,
                    .quality = rendered.quality,
                    .diagnostics = rendered.diagnostics,
                };
                retained_output_bytes = next_retained;
            } else {
                const rendered = worker.rendered_raster.?;
                const next_retained = std.math.add(usize, retained_output_bytes, rendered.bytes.len) catch return error.RenderBatchRetainedBytesExceeded;
                if (next_retained > options.max_retained_raster_bytes) return error.RenderBatchRetainedBytesExceeded;
                const owned_bytes = if (allocatorsEqual(alloc, worker.output_budget.backing)) blk: {
                    worker.output_budget.detachOwned(rendered.bytes);
                    worker.rendered_raster = null;
                    break :blk rendered.bytes;
                } else try alloc.dupe(u8, rendered.bytes);
                results[candidate.request_index].rendered = .{
                    .bytes = owned_bytes,
                    .pixel_format = rendered.pixel_format,
                    .width = rendered.width,
                    .height = rendered.height,
                    .stride = rendered.stride,
                    .requested_dpi = rendered.requested_dpi,
                    .effective_dpi = rendered.effective_dpi,
                    .quality = rendered.quality,
                    .diagnostics = rendered.diagnostics,
                };
                retained_output_bytes = next_retained;
            }
        }
    }

    return .{
        .results = results,
        .requested_parallelism = options.max_parallel_pages,
        .peak_launched_workers = peak_launched_workers,
        .peak_parallelism = peak_parallelism,
        .peak_admitted_pixels = peak_admitted_pixels,
        .peak_admitted_bytes = peak_admitted_bytes,
        .thread_spawn_fallbacks = thread_spawn_fallbacks,
    };
}

fn allocatorsEqual(a: Allocator, b: Allocator) bool {
    return a.ptr == b.ptr and a.vtable == b.vtable;
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
        // A retained stencil is the page-space target of the Pattern paint,
        // unlike tile-local image runs. Scale it exactly once with the outer
        // pattern occurrence.
        if (run.stencil_mask) |*mask|
            scaleImageRuns(@as(*[1]reader.ImageRun, @ptrCast(mask))[0..], scale);
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

test "direct adaptive geometry matches exhaustive DPI selection" {
    const Case = struct {
        box: reader.PageBox,
        rotation: render.PageRotation,
        request: PageRenderRequest,
    };
    const cases = [_]Case{
        .{
            .box = .{ .min_x = 0.720001, .min_y = 0.479996, .max_x = 595.92, .max_y = 842.16 },
            .rotation = .none,
            .request = .{ .page_number = 1, .requested_dpi = 150, .max_pixels = 40_000_000, .max_dimension = 1000 },
        },
        .{
            .box = .{ .min_x = -12.25, .min_y = 3.75, .max_x = 599.9, .max_y = 795.125 },
            .rotation = .clockwise_90,
            .request = .{ .page_number = 1, .requested_dpi = 600, .max_pixels = 1_000_000, .max_dimension = 4096 },
        },
        .{
            // A very thin page exercises ceil-to-one-pixel behavior, where a
            // square-root area estimate would be unnecessarily conservative.
            .box = .{ .min_x = 0, .min_y = 0, .max_x = 720, .max_y = 0.01 },
            .rotation = .clockwise_270,
            .request = .{ .page_number = 1, .requested_dpi = 600, .max_pixels = 7000, .max_dimension = 7000 },
        },
        .{
            .box = .{ .min_x = 0, .min_y = 0, .max_x = 595, .max_y = 842 },
            .rotation = .none,
            .request = .{ .page_number = 1, .requested_dpi = 72, .max_pixels = 64, .max_dimension = 8 },
        },
    };

    for (cases) |case| {
        const swaps_dimensions = case.rotation == .clockwise_90 or case.rotation == .clockwise_270;
        var expected: ?AdaptiveRenderGeometry = null;
        var dpi = case.request.requested_dpi;
        while (true) {
            if (renderGeometryAtDpi(case.box, case.rotation, swaps_dimensions, dpi)) |geometry| {
                if (geometry.width <= case.request.max_dimension and
                    geometry.height <= case.request.max_dimension and
                    geometry.pixels <= case.request.max_pixels)
                {
                    expected = geometry;
                    break;
                }
            } else |_| {}
            if (dpi == minimum_direct_render_dpi) break;
            dpi -= 1;
        }

        if (expected) |want| {
            const got = try adaptiveRenderGeometryForBox(case.box, case.rotation, swaps_dimensions, case.request);
            try std.testing.expectEqual(want.effective_dpi, got.effective_dpi);
            try std.testing.expectEqual(want.width, got.width);
            try std.testing.expectEqual(want.height, got.height);
            try std.testing.expectEqual(want.pixels, got.pixels);
            try std.testing.expectEqual(want.rotation, got.rotation);
        } else {
            try std.testing.expectError(
                error.RenderedPageTooLarge,
                adaptiveRenderGeometryForBox(case.box, case.rotation, swaps_dimensions, case.request),
            );
        }
    }
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

    const content = "BT\n/F1 20 Tf\n10 10 Td\n<00010001> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        // Extraction consumes the whole string as one four-byte code while
        // Identity-H painting must still consume two fixed-width CIDs.
        "<00000000> <FFFFFFFF>\n" ++
        "endcodespacerange\n" ++
        "1 beginbfchar\n" ++
        // Extraction deliberately disagrees with both the raw CID and the
        // font's only cmap entry. Painting must still select both CFF CID 1s.
        "<00010001> <0042>\n" ++
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
    const obj6 = "6 0 obj\n<< /Type /Font /Subtype /CIDFontType0 /BaseFont /TestCID /CIDSystemInfo << /Registry (Adobe) /Ordering (Identity) /Supplement 0 >> /DW 700 /W [1 [250]] /FontDescriptor 7 0 R >>\nendobj\n";
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

    var parsed = try reader.Reader.init(alloc, out.items);
    defer parsed.deinit();
    var analysis = try parsed.extractPageTextAnalysisAlloc(1);
    defer analysis.deinit(alloc);
    try std.testing.expectEqualStrings("B\n", analysis.text);
    try std.testing.expectEqual(@as(usize, 1), analysis.runs.len);
    try std.testing.expectApproxEqAbs(@as(f64, 10), analysis.runs[0].advance_width, 0.001);
    try std.testing.expect(!analysis.outline_fallback);
    const native_shapes = try parsed.extractPageVectorTextShapeRunsAlloc(1);
    defer {
        for (native_shapes) |*shape| shape.deinit(alloc);
        if (native_shapes.len > 0) alloc.free(native_shapes);
    }
    try std.testing.expect(native_shapes.len > 0);

    const backend = Backend.native();
    const png = try backend.renderFirstPagePng(alloc, out.items);
    defer alloc.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "native backend renders embedded Type0 CIDFontType0 OpenType CFF fdselect glyphs pdf first page png" {
    const alloc = std.testing.allocator;
    const font_bytes = try buildFdSelectOpenTypeCffFontAlloc(alloc);
    defer alloc.free(font_bytes);

    const content = "BT\n/F1 20 Tf\n10 10 Td\n<00010002> Tj\nET\n";
    const cmap =
        "/CIDInit /ProcSet findresource begin\n" ++
        "12 dict begin\n" ++
        "begincmap\n" ++
        "1 begincodespacerange\n" ++
        "<0000> <FFFF>\n" ++
        "endcodespacerange\n" ++
        "2 beginbfchar\n" ++
        "<0001> <0041>\n" ++
        "<0002> <0042>\n" ++
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

/// Test allocator that records page-sized allocations while serializing an
/// otherwise task-local backing allocator. A zero-copy result must be the one
/// large allocation made by the render worker, not a caller-thread duplicate.
const LargeAllocationTrackingAllocator = struct {
    backing: Allocator,
    threshold: usize,
    mutex: std.atomic.Mutex = .unlocked,
    large_allocations: usize = 0,
    first_large_ptr: usize = 0,

    fn allocator(self: *@This()) Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    fn lock(self: *@This()) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mutex.unlock();
        const ptr = self.backing.rawAlloc(len, alignment, ret_addr) orelse return null;
        if (len >= self.threshold) {
            self.large_allocations += 1;
            if (self.first_large_ptr == 0) self.first_large_ptr = @intFromPtr(ptr);
        }
        return ptr;
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mutex.unlock();
        return self.backing.rawResize(memory, alignment, new_len, ret_addr);
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mutex.unlock();
        const ptr = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse return null;
        if (memory.len >= self.threshold and self.first_large_ptr == @intFromPtr(memory.ptr))
            self.first_large_ptr = @intFromPtr(ptr);
        return ptr;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mutex.unlock();
        self.backing.rawFree(memory, alignment, ret_addr);
    }
};

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
    const planned = try planParsedPageRenderGeometry(&parsed, .{
        .page_number = 1,
        .requested_dpi = 150,
        .max_pixels = 40_000_000,
        .max_dimension = 1000,
    });
    try std.testing.expectEqual(@as(u16, 150), adaptive.requested_dpi);
    try std.testing.expectEqual(adaptive.effective_dpi, planned.effective_dpi);
    try std.testing.expectEqual(adaptive.width, planned.width);
    try std.testing.expectEqual(adaptive.height, planned.height);
    try std.testing.expectEqual(@as(u64, adaptive.width) * adaptive.height, planned.pixels);
    try std.testing.expect(adaptive.effective_dpi >= 72);
    try std.testing.expect(adaptive.effective_dpi < adaptive.requested_dpi);
    try std.testing.expect(adaptive.width <= 1000);
    try std.testing.expect(adaptive.height <= 1000);
    const decoded = try @import("antfly_image").png.decodeRgba(alloc, adaptive.png);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqual(adaptive.width, decoded.width);
    try std.testing.expectEqual(adaptive.height, decoded.height);

    var compact = try renderParsedPagePngAdaptiveAlloc(alloc, &parsed, 1, 150, 40_000_000, 400);
    defer compact.deinit(alloc);
    try std.testing.expect(compact.effective_dpi < 72);
    try std.testing.expect(compact.width <= 400);
    try std.testing.expect(compact.height <= 400);
    try std.testing.expectError(error.RenderedPageTooLarge, renderParsedPagePngAdaptiveAlloc(alloc, &parsed, 1, 150, 10, 4096));
    try std.testing.expectError(error.InvalidRenderDpi, renderParsedPagePngAlloc(alloc, &parsed, 1, 48, 40_000_000));
}

test "adaptive raster rendering exposes the native RGBA layout without PNG loss" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");
    var raster_reader = try reader.Reader.init(alloc, fixture);
    defer raster_reader.deinit();
    var raster = try renderParsedPageRasterAdaptiveWithProfileAlloc(
        alloc,
        &raster_reader,
        1,
        150,
        40_000_000,
        1000,
        .ocr,
    );
    defer raster.deinit(alloc);

    try std.testing.expectEqual(PixelFormat.rgba8, raster.pixel_format);
    try std.testing.expectEqual(@as(usize, raster.width) * 4, raster.stride);
    try std.testing.expectEqual(raster.stride * raster.height, raster.bytes.len);

    var png_reader = try reader.Reader.init(alloc, fixture);
    defer png_reader.deinit();
    var png = try renderParsedPagePngAdaptiveWithProfileAlloc(
        alloc,
        &png_reader,
        1,
        150,
        40_000_000,
        1000,
        .ocr,
    );
    defer png.deinit(alloc);
    const decoded = try image.png.decodeRgba(alloc, png.png);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqual(png.width, raster.width);
    try std.testing.expectEqual(png.height, raster.height);
    try std.testing.expectEqual(png.effective_dpi, raster.effective_dpi);
    try std.testing.expectEqualSlices(u8, decoded.rgba, raster.bytes);
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
    const stencil_rgba = try alloc.dupe(u8, &.{ 0xff, 0xff, 0xff, 0xff });
    var runs = [_]reader.PatternRun{.{
        .kind = .fill,
        .points = target_points,
        .stencil_mask = .{
            .rgba = stencil_rgba,
            .width = 1,
            .height = 1,
            .a = 20,
            .b = 0,
            .c = 0,
            .d = 20,
            .e = 3,
            .f = 4,
            .x = 3,
            .y = 4,
            .draw_width = 20,
            .draw_height = 20,
        },
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
    try std.testing.expectApproxEqAbs(@as(f64, 40), runs[0].stencil_mask.?.a, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 40), runs[0].stencil_mask.?.d, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 6), runs[0].stencil_mask.?.e, 0.001);
    try std.testing.expectApproxEqAbs(@as(f64, 8), runs[0].stencil_mask.?.f, 0.001);
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

test "render forks isolate mutable state across concurrent pages" {
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var parsed = try reader.Reader.init(std.testing.allocator, fixture);
    defer parsed.deinit();

    // Source preparation is intentionally serial. Each worker then
    // instantiates from the immutable template with an allocator that no
    // other render thread touches.
    var fork_template = try parsed.prepareRenderForkTemplate(std.testing.allocator, .{});
    defer fork_template.deinit();
    var first_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer first_arena.deinit();
    var second_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer second_arena.deinit();
    var first_fork = try fork_template.instantiate(first_arena.allocator(), .{});
    defer first_fork.deinit();
    var second_fork = try fork_template.instantiate(second_arena.allocator(), .{});
    defer second_fork.deinit();
    try std.testing.expect(!first_fork.owns_document_metadata);
    try std.testing.expect(!second_fork.owns_document_metadata);
    try std.testing.expectEqual(@intFromPtr(parsed.xref_entries.ptr), @intFromPtr(first_fork.xref_entries.ptr));
    try std.testing.expectEqual(@intFromPtr(parsed.page_index.?.ptr), @intFromPtr(first_fork.page_index.?.ptr));
    try std.testing.expectEqual(@intFromPtr(parsed.page_index.?.ptr), @intFromPtr(second_fork.page_index.?.ptr));

    const Worker = struct {
        alloc: Allocator,
        parsed: *reader.Reader,
        page_number: usize,
        rendered: ?RenderedPagePng = null,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            self.rendered = renderParsedPagePngAdaptiveWithProfileAlloc(
                self.alloc,
                self.parsed,
                self.page_number,
                150,
                40_000_000,
                2048,
                .ocr,
            ) catch |err| {
                self.failure = err;
                return;
            };
        }
    };

    var first_worker: Worker = .{
        .alloc = first_arena.allocator(),
        .parsed = &first_fork,
        .page_number = 1,
    };
    var second_worker: Worker = .{
        .alloc = second_arena.allocator(),
        .parsed = &second_fork,
        .page_number = 2,
    };
    const first_thread = try std.Thread.spawn(.{}, Worker.run, .{&first_worker});
    const second_thread = try std.Thread.spawn(.{}, Worker.run, .{&second_worker});
    first_thread.join();
    second_thread.join();

    if (first_worker.failure) |err| return err;
    if (second_worker.failure) |err| return err;
    const first = first_worker.rendered.?;
    const second = second_worker.rendered.?;
    try std.testing.expectEqual(first.width, second.width);
    try std.testing.expectEqual(first.height, second.height);
    try std.testing.expect(!std.mem.eql(u8, first.png, second.png));

    // Concurrent fork work must not consume or mutate the source Reader's
    // task-local cancellation and diagnostics state.
    try std.testing.expect(parsed.lastRenderDiagnostics() == null);
    const source_text = try parsed.extractPageTextAlloc(1);
    defer std.testing.allocator.free(source_text);
    try std.testing.expect(std.mem.indexOf(u8, source_text, "FIRST PAGE") != null);
}

test "immutable render fork template supports concurrent encrypted readers" {
    if (comptime builtin.single_threaded) return;
    const fixture = @embedFile("../testdata/rc4_40_empty_password_fixture.pdf");
    var parsed = try reader.Reader.init(std.testing.allocator, fixture);
    defer parsed.deinit();

    // Force lazy encrypted-stream discovery on the source before freezing it.
    // Workers must copy that state from the immutable snapshot without ever
    // iterating or mutating the source Reader's map.
    const source_text = try parsed.extractPageTextAlloc(1);
    defer std.testing.allocator.free(source_text);
    try std.testing.expectEqualStrings("Hello RC4-40", std.mem.trim(u8, source_text, &std.ascii.whitespace));
    var fork_template = try parsed.prepareRenderForkTemplate(std.testing.allocator, .{});
    defer fork_template.deinit();

    const Worker = struct {
        template: *const reader.RenderForkTemplate,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            for (0..8) |_| {
                var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer arena.deinit();
                var fork = self.template.instantiate(arena.allocator(), .{}) catch |err| {
                    self.failure = err;
                    return;
                };
                defer fork.deinit();
                const text = fork.extractPageTextAlloc(1) catch |err| {
                    self.failure = err;
                    return;
                };
                defer arena.allocator().free(text);
                if (!std.mem.eql(u8, "Hello RC4-40", std.mem.trim(u8, text, &std.ascii.whitespace))) {
                    self.failure = error.UnexpectedEncryptedPageText;
                    return;
                }
            }
        }
    };

    const worker_count = 4;
    var workers = [_]Worker{.{ .template = &fork_template }} ** worker_count;
    var threads: [worker_count]std.Thread = undefined;
    for (&threads, &workers) |*thread, *worker|
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
    for (threads) |thread| thread.join();
    for (workers) |worker| if (worker.failure) |err| return err;

    // The original Reader remains usable after concurrent template work.
    const repeated_source_text = try parsed.extractPageTextAlloc(1);
    defer std.testing.allocator.free(repeated_source_text);
    try std.testing.expectEqualStrings("Hello RC4-40", std.mem.trim(u8, repeated_source_text, &std.ascii.whitespace));
}

test "bounded render batch preserves order and isolates page failures" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();

    var batch = try renderParsedPagesBatchAlloc(alloc, &parsed, &.{
        .{ .page_number = 2 },
        .{ .page_number = 99 },
        .{ .page_number = 1 },
    }, .{
        .max_batch_pages = 3,
        .max_parallel_pages = 2,
    });
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), batch.results.len);
    try std.testing.expectEqual(@as(usize, 2), batch.results[0].page_number);
    try std.testing.expect(batch.results[0].rendered != null);
    try std.testing.expect(batch.results[0].failure == null);
    try std.testing.expectEqual(@as(usize, 99), batch.results[1].page_number);
    try std.testing.expect(batch.results[1].rendered == null);
    try std.testing.expect(batch.results[1].failure.? == error.InvalidPageNumber);
    try std.testing.expectEqual(@as(usize, 1), batch.results[2].page_number);
    try std.testing.expect(batch.results[2].rendered != null);
    try std.testing.expect(batch.results[2].failure == null);
    try std.testing.expectEqual(@as(usize, 2), batch.peak_launched_workers);
    try std.testing.expect(batch.peak_parallelism >= 1);
    try std.testing.expect(batch.peak_parallelism <= 2);
    try std.testing.expect(batch.peak_admitted_pixels > 0);
    try std.testing.expect(batch.peak_admitted_bytes > 0);
    try std.testing.expect(!std.mem.eql(u8, batch.results[0].rendered.?.png, batch.results[2].rendered.?.png));
}

test "bounded raw raster batch preserves order, plans, and output byte limits" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    const requests = [_]PageRenderRequest{
        .{ .page_number = 2, .requested_dpi = 150, .max_dimension = 1200, .max_output_bytes = 100_000 },
        .{ .page_number = 99 },
        .{ .page_number = 1, .requested_dpi = 150, .max_dimension = 1200, .max_output_bytes = 100_000 },
    };
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    var batch = try renderParsedPagesRasterBatchAlloc(alloc, &parsed, &requests, .{
        .max_batch_pages = requests.len,
        .max_parallel_pages = 2,
        .max_retained_raster_bytes = 200_000,
    });
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 3), batch.results.len);
    try std.testing.expect(batch.results[1].rendered == null);
    try std.testing.expect(batch.results[1].failure.? == error.InvalidPageNumber);
    for ([_]usize{ 0, 2 }) |index| {
        const rendered = batch.results[index].rendered.?;
        try std.testing.expect(batch.results[index].failure == null);
        try std.testing.expectEqual(PixelFormat.rgba8, rendered.pixel_format);
        try std.testing.expectEqual(@as(usize, rendered.width) * 4, rendered.stride);
        try std.testing.expectEqual(rendered.stride * rendered.height, rendered.bytes.len);
        try std.testing.expect(rendered.bytes.len <= requests[index].max_output_bytes.?);
        try std.testing.expect(rendered.effective_dpi < rendered.requested_dpi);
    }
    try std.testing.expect(!std.mem.eql(u8, batch.results[0].rendered.?.bytes, batch.results[2].rendered.?.bytes));

    const plan_requests = [_]PageRenderRequest{
        .{ .page_number = 2, .requested_dpi = 72, .max_dimension = 96 },
        .{ .page_number = 1, .requested_dpi = 72, .max_dimension = 96 },
    };
    var plans: [plan_requests.len]PreparedPageRenderPlan = undefined;
    for (plan_requests, 0..) |request, i| plans[i] = try prepareParsedPageRenderPlan(&parsed, request);
    var prepared = try renderPreparedPagesRasterBatchAlloc(alloc, &parsed, &plans, .{
        .max_batch_pages = plans.len,
        .max_parallel_pages = 1,
        .max_retained_raster_bytes = 96 * 96 * 4 * plans.len,
    });
    defer prepared.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), prepared.results.len);
    try std.testing.expectEqual(@as(usize, 2), prepared.results[0].page_number);
    try std.testing.expectEqual(@as(usize, 1), prepared.results[1].page_number);
    for (prepared.results) |result| {
        try std.testing.expect(result.failure == null);
        try std.testing.expect(result.rendered != null);
    }
}

test "prepared page plans preserve batch output and reject another source" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    const requests = [_]PageRenderRequest{
        .{ .page_number = 2, .requested_dpi = 150, .max_dimension = 1200 },
        .{ .page_number = 1, .requested_dpi = 150, .max_dimension = 1200 },
    };

    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    var plans: [requests.len]PreparedPageRenderPlan = undefined;
    for (requests, 0..) |request, i| plans[i] = try prepareParsedPageRenderPlan(&parsed, request);
    for (plans, requests) |plan, request| {
        try std.testing.expectEqual(request.page_number, plan.request().page_number);
        try std.testing.expect(plan.geometry().pixels > 0);
    }
    const serial_scratch = try estimatePreparedPageRenderWaveScratchBytes(&parsed, &plans, 1, 12);
    const parallel_scratch = try estimatePreparedPageRenderWaveScratchBytes(&parsed, &plans, 2, 12);
    try std.testing.expect(serial_scratch > parsed.decode_limits.max_working_set_bytes);
    try std.testing.expect(parallel_scratch > serial_scratch);
    var uneven = [_]PreparedPageRenderPlan{plans[0]} ** 6;
    const uneven_pixels = [_]u64{ 1, 1, 100, 100, 1, 1 };
    for (&uneven, uneven_pixels) |*plan, pixels| plan._geometry.pixels = pixels;
    const worker_fixed = try parsed.renderForkMetadataBytes() + parsed.decode_limits.max_working_set_bytes;
    try std.testing.expectEqual(
        worker_fixed * 3 + 201 * 12,
        try estimatePreparedPageRenderWaveScratchBytes(&parsed, &uneven, 3, 12),
    );
    const capped_plan = plans[0].withMaxOutputBytes(1234);
    try std.testing.expectEqual(@as(?usize, 1234), capped_plan.request().max_output_bytes);
    try std.testing.expectEqual(plans[0].geometry(), capped_plan.geometry());
    try std.testing.expectEqual(requests[0].max_output_bytes, plans[0].request().max_output_bytes);

    var prepared_batch = try renderPreparedPagesBatchAlloc(alloc, &parsed, &plans, .{
        .max_batch_pages = plans.len,
        .max_parallel_pages = 2,
    });
    defer prepared_batch.deinit(alloc);

    var ordinary_reader = try reader.Reader.init(alloc, fixture);
    defer ordinary_reader.deinit();
    var ordinary_batch = try renderParsedPagesBatchAlloc(alloc, &ordinary_reader, &requests, .{
        .max_batch_pages = requests.len,
        .max_parallel_pages = 2,
    });
    defer ordinary_batch.deinit(alloc);
    for (prepared_batch.results, ordinary_batch.results) |prepared, ordinary| {
        try std.testing.expect(prepared.failure == null);
        try std.testing.expect(ordinary.failure == null);
        try std.testing.expectEqualSlices(u8, prepared.rendered.?.png, ordinary.rendered.?.png);
    }

    const copied_fixture = try alloc.dupe(u8, fixture);
    defer alloc.free(copied_fixture);
    var another_source = try reader.Reader.init(alloc, copied_fixture);
    defer another_source.deinit();
    try std.testing.expectError(
        error.PreparedPageRenderSourceMismatch,
        renderPreparedPagesBatchAlloc(alloc, &another_source, &plans, .{ .max_batch_pages = plans.len }),
    );
    try std.testing.expectError(
        error.PreparedPageRenderSourceMismatch,
        estimatePreparedPageRenderWaveScratchBytes(&another_source, &plans, 1, 12),
    );
}

test "bounded render batch is byte stable across concurrency levels" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    const requests = [_]PageRenderRequest{
        .{ .page_number = 1 },
        .{ .page_number = 2 },
    };

    var serial_reader = try reader.Reader.init(alloc, fixture);
    defer serial_reader.deinit();
    var serial = try renderParsedPagesBatchAlloc(alloc, &serial_reader, &requests, .{
        .max_parallel_pages = 1,
    });
    defer serial.deinit(alloc);

    var parallel_reader = try reader.Reader.init(alloc, fixture);
    defer parallel_reader.deinit();
    var parallel = try renderParsedPagesBatchAlloc(alloc, &parallel_reader, &requests, .{
        .max_parallel_pages = 2,
    });
    defer parallel.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), serial.peak_parallelism);
    try std.testing.expectEqual(@as(usize, 0), serial.peak_launched_workers);
    try std.testing.expectEqual(@as(usize, 2), parallel.peak_launched_workers);
    try std.testing.expect(parallel.peak_parallelism >= 1);
    try std.testing.expect(parallel.peak_parallelism <= 2);
    for (serial.results, parallel.results) |serial_result, parallel_result| {
        try std.testing.expect(serial_result.failure == null);
        try std.testing.expect(parallel_result.failure == null);
        try std.testing.expectEqualSlices(u8, serial_result.rendered.?.png, parallel_result.rendered.?.png);
    }
}

test "bounded render batch uses a caller-owned executor without local threads" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    const requests = [_]PageRenderRequest{
        .{ .page_number = 1 },
        .{ .page_number = 2 },
    };

    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    var batch = try renderParsedPagesBatchAlloc(alloc, &parsed, &requests, .{
        .max_parallel_pages = 2,
        .executor_io = std.testing.io,
    });
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 2), batch.peak_launched_workers);
    try std.testing.expectEqual(@as(usize, 0), batch.thread_spawn_fallbacks);
    for (batch.results) |result| {
        try std.testing.expect(result.failure == null);
        try std.testing.expect(result.rendered != null);
    }
}

test "bounded render batch keeps results alive across fixed-executor scratch resets" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    const requests = [_]PageRenderRequest{
        .{ .page_number = 1 },
        .{ .page_number = 2 },
    };
    const InlineExecutor = struct {
        invocations: usize = 0,
        resets: usize = 0,
        observed_scratch_limit: usize = 0,

        fn runBatch(
            context: *anyopaque,
            contexts: []const *anyopaque,
            run: *const fn (context: *anyopaque, scratch: Allocator) void,
            max_scratch_bytes: usize,
        ) anyerror!PageRenderExecutor.BatchStats {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.invocations += 1;
            self.observed_scratch_limit = max_scratch_bytes;
            var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer scratch.deinit();
            for (contexts) |item| {
                run(item, scratch.allocator());
                _ = scratch.reset(.free_all);
                self.resets += 1;
            }
            return .{ .peak_parallelism = @min(@as(usize, 1), contexts.len) };
        }
    };

    var executor = InlineExecutor{};
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    var batch = try renderParsedPagesBatchAlloc(alloc, &parsed, &requests, .{
        .max_parallel_pages = 2,
        .executor = .{
            .ptr = &executor,
            .concurrent_capacity = 2,
            .run_batch_fn = InlineExecutor.runBatch,
        },
    });
    defer batch.deinit(alloc);

    try std.testing.expectEqual(@as(usize, 1), executor.invocations);
    try std.testing.expectEqual(requests.len, executor.resets);
    try std.testing.expectEqual(@as(usize, 512 * 1024 * 1024), executor.observed_scratch_limit);
    try std.testing.expectEqual(requests.len, batch.peak_launched_workers);
    try std.testing.expectEqual(@as(usize, 0), batch.thread_spawn_fallbacks);
    for (batch.results) |result| {
        try std.testing.expect(result.failure == null);
        try std.testing.expect(result.rendered.?.png.len > 0);
    }

    try std.testing.expectError(error.InvalidRenderBatchOptions, renderParsedPagesBatchAlloc(
        alloc,
        &parsed,
        &requests,
        .{
            .max_parallel_pages = 2,
            .executor = .{
                .ptr = &executor,
                .concurrent_capacity = 2,
                .run_batch_fn = InlineExecutor.runBatch,
            },
            .executor_io = std.testing.io,
        },
    ));
}

test "fixed executor detaches retained raster output without a page-sized copy" {
    const fixture = @embedFile("../testdata/simple_text_fixture.pdf");
    const requests = [_]PageRenderRequest{.{
        .page_number = 1,
        .requested_dpi = 150,
        .max_dimension = 256,
    }};
    const InlineExecutor = struct {
        fn runBatch(
            _: *anyopaque,
            contexts: []const *anyopaque,
            run: *const fn (context: *anyopaque, scratch: Allocator) void,
            _: usize,
        ) anyerror!PageRenderExecutor.BatchStats {
            var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer scratch.deinit();
            for (contexts) |item| {
                run(item, scratch.allocator());
                _ = scratch.reset(.free_all);
            }
            return .{ .peak_parallelism = @min(@as(usize, 1), contexts.len) };
        }
    };

    var tracking = LargeAllocationTrackingAllocator{
        .backing = std.testing.allocator,
        .threshold = 64 * 1024,
    };
    const result_alloc = tracking.allocator();
    var parsed = try reader.Reader.init(std.testing.allocator, fixture);
    defer parsed.deinit();
    var executor_state: u8 = 0;
    const executor = PageRenderExecutor{
        .ptr = &executor_state,
        .concurrent_capacity = 1,
        .run_batch_fn = InlineExecutor.runBatch,
    };

    try std.testing.expectError(
        error.InvalidRenderBatchOptions,
        renderParsedPagesRasterBatchAlloc(result_alloc, &parsed, &requests, .{
            .concurrent_output_allocator = result_alloc,
        }),
    );

    var batch = try renderParsedPagesRasterBatchAlloc(result_alloc, &parsed, &requests, .{
        .max_batch_pages = 1,
        .max_parallel_pages = 1,
        .max_retained_raster_bytes = 1024 * 1024,
        .executor = executor,
        .concurrent_output_allocator = result_alloc,
    });
    defer batch.deinit(result_alloc);

    const rendered = batch.results[0].rendered.?;
    try std.testing.expect(batch.results[0].failure == null);
    try std.testing.expect(rendered.bytes.len >= tracking.threshold);
    try std.testing.expectEqual(@as(usize, 1), tracking.large_allocations);
    try std.testing.expectEqual(tracking.first_large_ptr, @intFromPtr(rendered.bytes.ptr));
}

test "bounded render batch downsizes pages before retaining outputs" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var baseline_reader = try reader.Reader.init(alloc, fixture);
    defer baseline_reader.deinit();
    var baseline = try renderParsedPagePngAdaptiveWithProfileAlloc(alloc, &baseline_reader, 1, 150, 40_000_000, 2048, .ocr);
    defer baseline.deinit(alloc);
    const output_limit = @max(@as(usize, 128), baseline.png.len / 2);

    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    var batch = try renderParsedPagesBatchAlloc(alloc, &parsed, &.{.{
        .page_number = 1,
        .max_dimension = 2048,
        .max_output_bytes = output_limit,
        .min_output_dimension = 64,
        .max_output_attempts = 8,
    }}, .{ .max_retained_png_bytes = output_limit });
    defer batch.deinit(alloc);

    try std.testing.expect(batch.results[0].failure == null);
    const rendered = batch.results[0].rendered.?;
    try std.testing.expect(rendered.png.len <= output_limit);
    try std.testing.expect(rendered.width < baseline.width or rendered.height < baseline.height);
}

test "bounded render batch derives viable geometry from a partial native grant" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    try parsed.setDecodeLimits(.{
        .max_working_set_bytes = 4 * 1024 * 1024,
        .max_decoded_stream_bytes = 4 * 1024 * 1024,
    });

    const raster_budget = 8 * 1024 * 1024;
    const fixed_bytes = fixture.len + parsed.decode_limits.max_working_set_bytes;
    const native_grant = fixed_bytes + raster_budget;
    var batch = try renderParsedPagesBatchAlloc(alloc, &parsed, &.{.{
        .page_number = 1,
        .requested_dpi = 300,
        .max_pixels = 40_000_000,
        .max_dimension = 4096,
    }}, .{
        .max_inflight_bytes = native_grant,
        .max_retained_png_bytes = 16 * 1024 * 1024,
    });
    defer batch.deinit(alloc);

    try std.testing.expect(batch.results[0].failure == null);
    const rendered = batch.results[0].rendered.?;
    try std.testing.expect(rendered.width < 4096 and rendered.height < 4096);
    try std.testing.expect(batch.peak_admitted_bytes <= native_grant);
    try std.testing.expect(batch.peak_admitted_pixels <= raster_budget / 12);
}

test "bounded render batch admits decode working sets per page" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();
    try parsed.setDecodeLimits(.{
        .max_working_set_bytes = 4 * 1024 * 1024,
        .max_decoded_stream_bytes = 4 * 1024 * 1024,
    });

    // At 72 DPI, both pages' raster reservations fit together in 6 MiB, but
    // their independent 4 MiB decode working sets do not. They must therefore
    // execute in separate one-page waves even when two lanes are available.
    const max_inflight_bytes = 6 * 1024 * 1024;
    var batch = try renderParsedPagesBatchAlloc(alloc, &parsed, &.{
        .{ .page_number = 1, .requested_dpi = 72 },
        .{ .page_number = 2, .requested_dpi = 72 },
    }, .{
        .max_batch_pages = 2,
        .max_parallel_pages = 2,
        .max_inflight_bytes = max_inflight_bytes,
    });
    defer batch.deinit(alloc);

    try std.testing.expect(batch.results[0].failure == null);
    try std.testing.expect(batch.results[1].failure == null);
    try std.testing.expectEqual(@as(usize, 1), batch.peak_parallelism);
    try std.testing.expect(batch.peak_admitted_bytes <= max_inflight_bytes);
    try std.testing.expect(batch.peak_admitted_bytes >= parsed.decode_limits.max_working_set_bytes);
}

test "render worker budget releases freed temporary memory" {
    var budget = RenderWorkerBudgetAllocator{
        .backing = std.testing.allocator,
        .max_live_bytes = 64,
    };
    const alloc = budget.allocator();
    const first = try alloc.alloc(u8, 64);
    alloc.free(first);
    const second = try alloc.alloc(u8, 64);
    alloc.free(second);
    try std.testing.expectEqual(@as(usize, 0), budget.live_bytes);
    try std.testing.expect(!budget.limit_exceeded);
}

test "bounded render batch rejects an already-canceled window" {
    const Cancelled = struct {
        fn check(_: ?*const anyopaque) bool {
            return true;
        }
    };
    const Active = struct {
        fn check(_: ?*const anyopaque) bool {
            return false;
        }
    };
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var parsed = try reader.Reader.init(std.testing.allocator, fixture);
    defer parsed.deinit();

    try std.testing.expectError(error.Canceled, renderParsedPagesBatchAlloc(
        std.testing.allocator,
        &parsed,
        &.{.{ .page_number = 1 }},
        .{ .cancellation = .{ .is_cancelled_fn = Cancelled.check } },
    ));

    parsed.setCancellationProbe(.{ .is_cancelled_fn = Cancelled.check });
    try std.testing.expectError(error.Canceled, renderParsedPagesBatchAlloc(
        std.testing.allocator,
        &parsed,
        &.{.{ .page_number = 1 }},
        .{ .cancellation = .{ .is_cancelled_fn = Active.check } },
    ));
}

test "render wave control composes cancellation and measures active workers" {
    var control = RenderWaveControl{ .external = .{} };
    try control.probe().check();
    control.enterRender();
    control.enterRender();
    try std.testing.expectEqual(@as(usize, 2), control.peak_active.load(.acquire));
    control.leaveRender();
    control.leaveRender();
    try std.testing.expectEqual(@as(usize, 0), control.active.load(.acquire));
    control.stop();
    try std.testing.expectError(error.Canceled, control.probe().check());
}

test "render batch thread control reuses workers across generations" {
    const Worker = struct {
        control: *RenderBatchThreadControl,
        waves: std.atomic.Value(usize) = .init(0),

        fn run(self: *@This()) void {
            var generation: usize = 0;
            while (self.control.awaitWave(&generation, 0)) |active| {
                if (active) _ = self.waves.fetchAdd(1, .acq_rel);
                self.control.completeWave();
            }
        }
    };
    var control = RenderBatchThreadControl.init();
    var worker = Worker{ .control = &control };
    const thread = try std.Thread.spawn(.{}, Worker.run, .{&worker});
    control.startWave(1);
    control.waitForWorkers(1);
    control.startWave(1);
    control.waitForWorkers(1);
    control.stop();
    thread.join();
    try std.testing.expectEqual(@as(usize, 2), worker.waves.load(.acquire));
}

test "bounded render batch enforces window admission and retained output limits" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();

    try std.testing.expectError(error.RenderBatchTooLarge, renderParsedPagesBatchAlloc(alloc, &parsed, &.{
        .{ .page_number = 1 },
        .{ .page_number = 2 },
    }, .{ .max_batch_pages = 1 }));

    var rejected = try renderParsedPagesBatchAlloc(alloc, &parsed, &.{.{ .page_number = 1 }}, .{
        .max_inflight_bytes = 1,
    });
    defer rejected.deinit(alloc);
    try std.testing.expect(rejected.results[0].rendered == null);
    try std.testing.expect(rejected.results[0].failure.? == error.RenderBatchAdmissionExceeded);
    try std.testing.expectEqual(@as(usize, 0), rejected.peak_parallelism);

    try std.testing.expectError(error.RenderBatchRetainedBytesExceeded, renderParsedPagesBatchAlloc(alloc, &parsed, &.{.{ .page_number = 1 }}, .{
        .max_retained_png_bytes = 1,
    }));
}

test "bounded render batch adapts geometry to the inflight pixel window" {
    const alloc = std.testing.allocator;
    const fixture = @embedFile("../testdata/two_page_text_fixture.pdf");
    var parsed = try reader.Reader.init(alloc, fixture);
    defer parsed.deinit();

    const pixel_window: u64 = 50_000;
    var batch = try renderParsedPagesBatchAlloc(alloc, &parsed, &.{.{
        .page_number = 1,
        .requested_dpi = 150,
        .max_pixels = 40_000_000,
    }}, .{
        .max_inflight_pixels = pixel_window,
    });
    defer batch.deinit(alloc);

    try std.testing.expect(batch.results[0].failure == null);
    const rendered = batch.results[0].rendered.?;
    try std.testing.expect(@as(u64, rendered.width) * @as(u64, rendered.height) <= pixel_window);
    try std.testing.expect(rendered.effective_dpi < rendered.requested_dpi);
    try std.testing.expect(batch.peak_admitted_pixels <= pixel_window);
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
