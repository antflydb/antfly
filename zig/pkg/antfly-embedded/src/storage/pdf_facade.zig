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

//! PDF consumer facade. Mutable readers, fork metadata, fonts, masks and
//! render caches live in the compiled media owner. Values retain caller budgets.
const std = @import("std");
const c = @import("pdf_native_contract");
const values = c.pdf;
const A = std.mem.Allocator;
pub const RenderedPagePng = values.RenderedPagePng;
pub const RenderedPageRaster = values.RenderedPageRaster;
pub const PixelFormat = values.PixelFormat;
pub const RenderQuality = values.RenderQuality;
pub const RenderProfile = values.RenderProfile;
pub const RenderResolutionPolicy = values.RenderResolutionPolicy;
pub const PageRenderRequest = values.PageRenderRequest;
pub const PageRenderExecutor = values.PageRenderExecutor;
pub const RenderScratchGrowth = values.RenderScratchGrowth;
pub const RenderedPageBatch = values.RenderedPageBatch;
pub const RenderedPageRasterBatch = values.RenderedPageRasterBatch;
pub const PageRenderResult = values.PageRenderResult;
pub const PageRasterResult = values.PageRasterResult;
pub const PageRenderGeometry = values.PageRenderGeometry;
pub const PreparedPageRenderPlan = values.PreparedPageRenderPlan;
pub const RenderBatchOutputKind = values.RenderBatchOutputKind;
pub const Backend = struct {
    ptr: *const anyopaque,
    extract_text_fn: *const fn (*const anyopaque, A, []const u8) anyerror![]u8,
    render_first_page_png_fn: *const fn (*const anyopaque, A, []const u8) anyerror![]u8,
    pub fn extractText(self: Backend, a: A, bytes: []const u8) ![]u8 {
        return self.extract_text_fn(self.ptr, a, bytes);
    }
    pub fn renderFirstPagePng(self: Backend, a: A, bytes: []const u8) ![]u8 {
        return self.render_first_page_png_fn(self.ptr, a, bytes);
    }
    pub fn system() Backend {
        return native();
    }
    pub fn native() Backend {
        return .{ .ptr = &sentinel, .extract_text_fn = extract, .render_first_page_png_fn = renderFirst };
    }
    const sentinel: u8 = 0;
    fn extract(_: *const anyopaque, a: A, bytes: []const u8) ![]u8 {
        var doc = try reader.Reader.init(a, bytes);
        defer doc.deinit();
        return doc.extractPlainTextAlloc();
    }
    fn renderFirst(_: *const anyopaque, a: A, bytes: []const u8) ![]u8 {
        return renderPagePngAlloc(a, bytes, 1, 72, 40_000_000);
    }
};
pub const default_render_bytes_per_pixel_reserve = values.default_render_bytes_per_pixel_reserve;
pub const render = values.render;
pub const native_fonts = struct {
    pub const sfnt = @import("antfly_font").sfnt;
    pub const regular = @import("pdf_standard_fonts").regular;
    pub const bold = @import("pdf_standard_fonts").bold;
};
pub const syntax = @import("pdf_syntax");
pub const text_encoding = @import("pdf_text_encoding");
pub const PageRenderDiagnostics = values.reader.PageRenderDiagnostics;
pub const reader = struct {
    pub const PageRenderRuns = values.reader.PageRenderRuns;
    pub const ShapeRun = values.reader.ShapeRun;
    pub const ImageRun = values.reader.ImageRun;
    pub const DecodeLimits = c.Limits;
    pub const CancellationProbe = c.Probe;
    pub const PageTextAnalysis = values.reader.PageTextAnalysis;
    pub const TextRun = values.reader.TextRun;
    pub const PageBox = c.Box;
    pub const Reader = struct {
        handle: *c.Document,
        alloc: A,
        source: []const u8,
        probe: CancellationProbe,
        pub fn init(a: A, bytes: []const u8) !Reader {
            return initWithDecodeLimits(a, bytes, .{});
        }
        pub fn initWithDecodeLimits(a: A, bytes: []const u8, limits: DecodeLimits) !Reader {
            return initWithDecodeLimitsAndCancellation(a, bytes, limits, .{});
        }
        pub fn initWithDecodeLimitsAndCancellation(a: A, bytes: []const u8, limits: DecodeLimits, probe: CancellationProbe) !Reader {
            return .{ .handle = try c.call("open", .{ a, bytes, limits, probe }), .alloc = a, .source = bytes, .probe = probe };
        }
        pub fn allocator(self: *const Reader) A {
            return self.alloc;
        }
        pub fn sourceBytes(self: *const Reader) []const u8 {
            return self.source;
        }
        pub fn cancellationProbe(self: *const Reader) CancellationProbe {
            return self.probe;
        }
        pub fn deinit(self: *Reader) void {
            c.call("close", .{self.handle}) catch @panic("PDF close contract mismatch");
            self.* = undefined;
        }
        pub fn setCancellationProbe(self: *Reader, probe: CancellationProbe) void {
            c.call("setProbe", .{ self.handle, probe }) catch @panic("PDF probe contract mismatch");
            self.probe = probe;
        }
        pub fn setDecodeLimits(self: *Reader, limits: DecodeLimits) !void {
            try c.call("setLimits", .{ self.handle, limits });
        }
        pub fn pageCount(self: *Reader) !usize {
            return c.call("count", .{self.handle});
        }
        pub fn extractPageTextAlloc(self: *Reader, page: usize) ![]u8 {
            return c.call("text", .{ self.handle, page });
        }
        pub fn extractPlainTextAlloc(self: *Reader) ![]u8 {
            return c.call("plainText", .{self.handle});
        }
        pub fn extractPageTextAnalysisAlloc(self: *Reader, page: usize) !PageTextAnalysis {
            return self.extractPageTextAnalysisWithRunLimitAlloc(page, std.math.maxInt(usize));
        }
        pub fn extractPageTextAnalysisWithRunLimitAlloc(self: *Reader, page: usize, limit: usize) !PageTextAnalysis {
            return c.call("analysis", .{ self.handle, page, limit });
        }
        pub fn extractPageTextRunsAlloc(self: *Reader, page: usize) ![]TextRun {
            return c.call("runs", .{ self.handle, page });
        }
        pub fn extractPageBox(self: *Reader, page: usize) !PageBox {
            return c.call("box", .{ self.handle, page });
        }
        pub fn extractPageRotation(self: *Reader, page: usize) !?i32 {
            return c.call("rotation", .{ self.handle, page });
        }
        pub fn extractPageRenderRunsForRasterAlloc(self: *Reader, page: usize, width: u32, height: u32) !PageRenderRuns {
            return c.call("renderRuns", .{ self.handle, page, width, height });
        }
        pub fn prepareRenderForkTemplate(self: *Reader, a: A, probe: CancellationProbe) !RenderForkTemplate {
            return .{ .handle = try c.call("forkOpen", .{ self.handle, a, probe }), .source = self.source };
        }
    };
    pub const RenderForkTemplate = struct {
        handle: *c.Fork,
        source: []const u8,
        pub fn deinit(self: *RenderForkTemplate) void {
            c.call("forkClose", .{self.handle}) catch @panic("PDF fork close contract mismatch");
            self.* = undefined;
        }
        pub fn refreshFrom(self: *RenderForkTemplate, source: *Reader, probe: CancellationProbe) !void {
            try c.call("forkRefresh", .{ self.handle, source.handle, probe });
        }
        pub fn instantiate(self: *const RenderForkTemplate, a: A, probe: CancellationProbe) !Reader {
            return .{ .handle = try c.call("forkInstantiate", .{ self.handle, a, probe }), .alloc = a, .source = self.source, .probe = probe };
        }
    };
};
pub const PageRenderBatchOptions = struct {
    max_batch_pages: usize = 8,
    max_parallel_pages: usize = 1,
    max_inflight_pixels: u64 = 50_000_000,
    max_inflight_bytes: usize = 512 * 1024 * 1024,
    scratch_growth: ?RenderScratchGrowth = null,
    max_retained_png_bytes: usize = 64 * 1024 * 1024,
    max_retained_raster_bytes: usize = 256 * 1024 * 1024,
    bytes_per_pixel_reserve: usize = default_render_bytes_per_pixel_reserve,
    profile: RenderProfile = .ocr,
    cancellation: reader.CancellationProbe = .{},
    executor: ?PageRenderExecutor = null,
    fork_template: ?*reader.RenderForkTemplate = null,
    concurrent_output_allocator: ?A = null,
    executor_io: ?std.Io = null,
};
const OptionLease = struct {
    original: PageRenderBatchOptions,
    last_error: ?anyerror = null,
    fn wire(self: *OptionLease) c.Options {
        var result: c.Options = .{};
        inline for (@typeInfo(c.Options).@"struct".field_names) |name| {
            if (comptime !std.mem.eql(u8, name, "executor") and !std.mem.eql(u8, name, "executor_io") and !std.mem.eql(u8, name, "fork_template"))
                @field(result, name) = @field(self.original, name);
        }
        result.fork_template = if (self.original.fork_template) |fork| fork.handle else null;
        result.executor = if (self.original.executor) |executor| .{ .context = self, .capacity = executor.concurrent_capacity, .run = run } else null;
        result.executor_io = if (self.original.executor_io) |*io| .init(io) else null;
        return result;
    }
    fn run(context: *anyopaque, contexts: []const *anyopaque, operation: *const fn (*anyopaque, A) void, limit: usize) c.Outcome(PageRenderExecutor.BatchStats) {
        const self: *OptionLease = @ptrCast(@alignCast(context));
        const result = self.original.executor.?.runBatch(contexts, operation, limit) catch |err| {
            self.last_error = err;
            return .{ .failure = .init(@errorName(err)) };
        };
        return .{ .value = result };
    }
    fn decode(self: *OptionLease, failure: c.Failure) ?anyerror {
        const ptr = failure.ptr orelse return null;
        const name = ptr[0..failure.len];
        if (self.last_error) |err| if (std.mem.eql(u8, name, @errorName(err))) return err;
        return @import("engine_errors").fromName(name);
    }
};
fn plansWire(a: A, values: []const PreparedPageRenderPlan) ![]c.Plan {
    const result = try a.alloc(c.Plan, values.len);
    for (result, values) |*out, value| out.* = .fromLocal(value);
    return result;
}
pub fn prepareParsedPageRenderPlan(doc: *reader.Reader, request: PageRenderRequest) !PreparedPageRenderPlan {
    return (try c.call("plan", .{ doc.handle, request })).local();
}
pub fn prepareAdmittedPageRenderPlan(doc: *reader.Reader, request: PageRenderRequest, options: PageRenderBatchOptions, kind: RenderBatchOutputKind) !PreparedPageRenderPlan {
    var lease: OptionLease = .{ .original = options };
    return (try c.call("admit", .{ doc.handle, request, lease.wire(), kind })).local();
}
pub fn planParsedPageRenderGeometry(doc: *reader.Reader, request: PageRenderRequest) !PageRenderGeometry {
    return (try prepareParsedPageRenderPlan(doc, request)).geometry();
}
pub fn estimatePreparedPageRenderWaveScratchBytes(doc: *const reader.Reader, values: []const PreparedPageRenderPlan, parallel: usize, reserve: usize) !usize {
    const plans = try plansWire(doc.alloc, values);
    defer doc.alloc.free(plans);
    return c.call("estimate", .{ doc.handle, plans, parallel, reserve });
}
pub fn renderPagePngAlloc(a: A, bytes: []const u8, page: usize, dpi: u16, pixels: u64) ![]u8 {
    var doc = try reader.Reader.init(a, bytes);
    defer doc.deinit();
    return renderParsedPagePngAlloc(a, &doc, page, dpi, pixels);
}
pub fn renderParsedPagePngAlloc(a: A, doc: *reader.Reader, page: usize, dpi: u16, pixels: u64) ![]u8 {
    return c.call("pngExact", .{ a, doc.handle, page, dpi, pixels });
}
pub fn renderParsedPagePngAdaptiveAlloc(a: A, doc: *reader.Reader, page: usize, dpi: u16, pixels: u64, dimension: u32) !RenderedPagePng {
    return renderParsedPagePngAdaptiveWithProfileAlloc(a, doc, page, dpi, pixels, dimension, .exact);
}
pub fn renderParsedPagePngAdaptiveWithProfileAlloc(a: A, doc: *reader.Reader, page: usize, dpi: u16, pixels: u64, dimension: u32, profile: RenderProfile) !RenderedPagePng {
    return c.call("png", .{ a, doc.handle, page, dpi, pixels, dimension, profile });
}
pub fn renderParsedPageRasterAdaptiveAlloc(a: A, doc: *reader.Reader, page: usize, dpi: u16, pixels: u64, dimension: u32) !RenderedPageRaster {
    return renderParsedPageRasterAdaptiveWithProfileAlloc(a, doc, page, dpi, pixels, dimension, .exact);
}
pub fn renderParsedPageRasterAdaptiveWithProfileAlloc(a: A, doc: *reader.Reader, page: usize, dpi: u16, pixels: u64, dimension: u32, profile: RenderProfile) !RenderedPageRaster {
    return c.call("raster", .{ a, doc.handle, page, dpi, pixels, dimension, profile });
}
pub fn renderPreparedPagesBatchAlloc(a: A, doc: *reader.Reader, plans: []const PreparedPageRenderPlan, options: PageRenderBatchOptions) !RenderedPageBatch {
    const values = try plansWire(a, plans);
    defer a.free(values);
    return batchPng(a, doc, values, &.{}, options, plans);
}
pub fn renderParsedPagesBatchAlloc(a: A, doc: *reader.Reader, requests: []const PageRenderRequest, options: PageRenderBatchOptions) !RenderedPageBatch {
    return batchPng(a, doc, null, requests, options, null);
}
fn batchPng(a: A, doc: *reader.Reader, plans: ?[]const c.Plan, requests: []const PageRenderRequest, options: PageRenderBatchOptions, local_plans: ?[]const PreparedPageRenderPlan) !RenderedPageBatch {
    var lease: OptionLease = .{ .original = options };
    var batch = try c.call("batchPng", .{ a, doc.handle, plans, requests, lease.wire() });
    defer batch.deinit(a);
    if (lease.decode(batch.failure)) |err| return err;
    const results = try a.alloc(PageRenderResult, batch.results.len);
    for (results, batch.results, 0..) |*out, *value, index| {
        out.* = .{ .page_number = value.page_number, .rendered = value.rendered, .failure = if (local_plans != null and local_plans.?[index].failure != null) local_plans.?[index].failure else lease.decode(value.failure), .render_elapsed_ns = value.render_elapsed_ns };
        value.rendered = null;
    }
    return .{ .results = results, .requested_parallelism = batch.requested_parallelism, .peak_launched_workers = batch.peak_launched_workers, .peak_parallelism = batch.peak_parallelism, .peak_admitted_pixels = batch.peak_admitted_pixels, .peak_admitted_bytes = batch.peak_admitted_bytes, .peak_worker_scratch_bytes = batch.peak_worker_scratch_bytes, .thread_spawn_fallbacks = batch.thread_spawn_fallbacks };
}
pub fn renderPreparedPagesRasterBatchAlloc(a: A, doc: *reader.Reader, plans: []const PreparedPageRenderPlan, options: PageRenderBatchOptions) !RenderedPageRasterBatch {
    const values = try plansWire(a, plans);
    defer a.free(values);
    return batchRaster(a, doc, values, &.{}, options, plans);
}
pub fn renderParsedPagesRasterBatchAlloc(a: A, doc: *reader.Reader, requests: []const PageRenderRequest, options: PageRenderBatchOptions) !RenderedPageRasterBatch {
    return batchRaster(a, doc, null, requests, options, null);
}
fn batchRaster(a: A, doc: *reader.Reader, plans: ?[]const c.Plan, requests: []const PageRenderRequest, options: PageRenderBatchOptions, local_plans: ?[]const PreparedPageRenderPlan) !RenderedPageRasterBatch {
    var lease: OptionLease = .{ .original = options };
    var batch = try c.call("batchRaster", .{ a, doc.handle, plans, requests, lease.wire() });
    defer batch.deinit(a);
    if (lease.decode(batch.failure)) |err| return err;
    const results = try a.alloc(PageRasterResult, batch.results.len);
    for (results, batch.results, 0..) |*out, *value, index| {
        out.* = .{ .page_number = value.page_number, .rendered = value.rendered, .failure = if (local_plans != null and local_plans.?[index].failure != null) local_plans.?[index].failure else lease.decode(value.failure), .render_elapsed_ns = value.render_elapsed_ns };
        value.rendered = null;
    }
    return .{ .results = results, .requested_parallelism = batch.requested_parallelism, .peak_launched_workers = batch.peak_launched_workers, .peak_parallelism = batch.peak_parallelism, .peak_admitted_pixels = batch.peak_admitted_pixels, .peak_admitted_bytes = batch.peak_admitted_bytes, .peak_worker_scratch_bytes = batch.peak_worker_scratch_bytes, .thread_spawn_fallbacks = batch.thread_spawn_fallbacks };
}
