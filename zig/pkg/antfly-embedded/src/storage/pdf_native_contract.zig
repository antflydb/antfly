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

//! Private same-compiler PDF operations. Reader/fork implementations never
//! cross this contract; errors carry names and opaque origin-scoped tokens.
const std = @import("std");
pub const pdf = @import("pdf_values");
pub const native = @import("antfly_runtime_abi").native_abi;
pub const work = @import("image_work_control");
pub const Failure = work.Failure;
pub const WorkBorrow = extern struct {
    context: ?*const anyopaque = null,
    check_fn: ?*const fn (?*const anyopaque) callconv(.c) Failure = null,
};
const WorkLease = struct {
    control: work.Control,
    fn check(context: ?*const anyopaque) callconv(.c) Failure {
        const self: *const WorkLease = @ptrCast(@alignCast(context.?));
        self.control.check() catch |err| return work.failure(err);
        return .{};
    }
    fn borrow(self: *const WorkLease) WorkBorrow {
        return if (self.control.check_fn == null) .{} else .{ .context = self, .check_fn = check };
    }
};
pub fn Outcome(comptime T: type) type {
    return struct { value: T = undefined, failure: Failure = .{} };
}
pub const Document = opaque {};
pub const Fork = opaque {};
pub const A = std.mem.Allocator;
pub const Probe = pdf.reader.CancellationProbe;
pub const Limits = pdf.reader.DecodeLimits;
pub const Box = pdf.reader.PageBox;
pub const Plan = struct {
    value: pdf.PreparedPageRenderPlan,
    failure: Failure = .{},
    pub fn fromLocal(value: pdf.PreparedPageRenderPlan) Plan {
        var safe = value;
        safe.failure = null;
        return .{ .value = safe, .failure = if (value.failure) |err| .init(@errorName(err)) else .{} };
    }
    /// Preparation failures are ordered slots, never renderer work. The
    /// provider preserves the original name beside the slot and restores it
    /// on return, without rooting a global error-name decoder in media.
    pub fn providerLocal(self: Plan) pdf.PreparedPageRenderPlan {
        var value = self.value;
        value.failure = if (self.failure.ptr != null) error.PdfNativePreparedFailure else null;
        return value;
    }
    pub fn local(self: Plan) pdf.PreparedPageRenderPlan {
        var value = self.value;
        value.failure = if (self.failure.ptr) |ptr| @import("engine_errors").fromName(ptr[0..self.failure.len]) else null;
        return value;
    }
};
pub const Executor = struct {
    context: *anyopaque,
    capacity: usize,
    run: *const fn (*anyopaque, []const *anyopaque, *const fn (*anyopaque, A) void, usize) Outcome(pdf.PageRenderExecutor.BatchStats),
};
pub const Options = struct {
    max_batch_pages: usize = 8,
    max_parallel_pages: usize = 1,
    max_inflight_pixels: u64 = 50_000_000,
    max_inflight_bytes: usize = 512 * 1024 * 1024,
    scratch_growth: ?pdf.RenderScratchGrowth = null,
    max_retained_png_bytes: usize = 64 * 1024 * 1024,
    max_retained_raster_bytes: usize = 256 * 1024 * 1024,
    bytes_per_pixel_reserve: usize = pdf.default_render_bytes_per_pixel_reserve,
    profile: pdf.RenderProfile = .ocr,
    cancellation: Probe = .{},
    executor: ?Executor = null,
    fork_template: ?*Fork = null,
    concurrent_output_allocator: ?A = null,
    executor_io: ?native.IoBorrow = null,
};
pub const PngResult = struct {
    page_number: usize,
    rendered: ?pdf.RenderedPagePng = null,
    failure: Failure = .{},
    render_elapsed_ns: u64 = 0,
};
pub const PngBatch = struct {
    failure: Failure = .{},
    results: []PngResult,
    requested_parallelism: usize = 0,
    peak_launched_workers: usize = 0,
    peak_parallelism: usize = 0,
    peak_admitted_pixels: u64 = 0,
    peak_admitted_bytes: usize = 0,
    peak_worker_scratch_bytes: usize = 0,
    thread_spawn_fallbacks: usize = 0,
    pub fn deinit(self: *@This(), a: A) void {
        for (self.results) |*item| if (item.rendered) |*value| value.deinit(a);
        a.free(self.results);
        self.* = undefined;
    }
};
pub const RasterResult = struct {
    page_number: usize,
    rendered: ?pdf.RenderedPageRaster = null,
    failure: Failure = .{},
    render_elapsed_ns: u64 = 0,
};
pub const RasterBatch = struct {
    failure: Failure = .{},
    results: []RasterResult,
    requested_parallelism: usize = 0,
    peak_launched_workers: usize = 0,
    peak_parallelism: usize = 0,
    peak_admitted_pixels: u64 = 0,
    peak_admitted_bytes: usize = 0,
    peak_worker_scratch_bytes: usize = 0,
    thread_spawn_fallbacks: usize = 0,
    pub fn deinit(self: *@This(), a: A) void {
        for (self.results) |*item| if (item.rendered) |*value| value.deinit(a);
        a.free(self.results);
        self.* = undefined;
    }
};
pub const Methods = struct {
    renderRuns: *const fn (doc: *Document, page: usize, width: u32, height: u32) anyerror!pdf.reader.PageRenderRuns,
    open: *const fn (a: A, bytes: []const u8, limits: Limits, probe: Probe) anyerror!*Document,
    close: *const fn (doc: *Document) anyerror!void,
    setLimits: *const fn (doc: *Document, limits: Limits) anyerror!void,
    setProbe: *const fn (doc: *Document, probe: Probe) anyerror!void,
    count: *const fn (doc: *Document) anyerror!usize,
    text: *const fn (doc: *Document, page: usize) anyerror![]u8,
    plainText: *const fn (doc: *Document) anyerror![]u8,
    analysis: *const fn (doc: *Document, page: usize, limit: usize) anyerror!pdf.reader.PageTextAnalysis,
    runs: *const fn (doc: *Document, page: usize) anyerror![]pdf.reader.TextRun,
    box: *const fn (doc: *Document, page: usize) anyerror!Box,
    rotation: *const fn (doc: *Document, page: usize) anyerror!?i32,
    forkOpen: *const fn (doc: *Document, a: A, probe: Probe) anyerror!*Fork,
    forkClose: *const fn (fork: *Fork) anyerror!void,
    forkRefresh: *const fn (fork: *Fork, doc: *Document, probe: Probe) anyerror!void,
    forkInstantiate: *const fn (fork: *Fork, a: A, probe: Probe) anyerror!*Document,
    plan: *const fn (doc: *Document, request: pdf.PageRenderRequest) anyerror!Plan,
    admit: *const fn (doc: *Document, request: pdf.PageRenderRequest, options: Options, kind: pdf.RenderBatchOutputKind) anyerror!Plan,
    estimate: *const fn (doc: *Document, plans: []const Plan, parallel: usize, reserve: usize) anyerror!usize,
    pngExact: *const fn (a: A, doc: *Document, page: usize, dpi: u16, pixels: u64) anyerror![]u8,
    png: *const fn (a: A, doc: *Document, page: usize, dpi: u16, pixels: u64, dimension: u32, profile: pdf.RenderProfile) anyerror!pdf.RenderedPagePng,
    raster: *const fn (a: A, doc: *Document, page: usize, dpi: u16, pixels: u64, dimension: u32, profile: pdf.RenderProfile) anyerror!pdf.RenderedPageRaster,
    batchPng: *const fn (a: A, doc: *Document, plans: ?[]const Plan, requests: []const pdf.PageRenderRequest, options: Options) anyerror!PngBatch,
    batchRaster: *const fn (a: A, doc: *Document, plans: ?[]const Plan, requests: []const pdf.PageRenderRequest, options: Options) anyerror!RasterBatch,
};
pub fn Function(comptime name: []const u8) type {
    return @FieldType(Methods, name);
}
pub fn Args(comptime name: []const u8) type {
    return std.meta.ArgsTuple(@typeInfo(Function(name)).pointer.child);
}
pub fn Value(comptime name: []const u8) type {
    return @typeInfo(@typeInfo(@typeInfo(Function(name)).pointer.child).@"fn".return_type.?).error_union.payload;
}
pub fn Wire(comptime name: []const u8) type {
    return Outcome(Value(name));
}
pub fn signature(comptime name: []const u8) native.CallContract {
    @setEvalBranchQuota(100_000);
    return .of("pdf_native_v2." ++ name, Function(name), Args(name), Wire(name));
}
const layout_types = .{ Failure, WorkBorrow, work.Control, pdf.reader.PageRenderRuns, pdf.reader.ImageRun, pdf.reader.ShadingRun, pdf.reader.PatternRun, pdf.GraphicsMatrix, pdf.SharedImageRgba, pdf.ShadingKind, pdf.PatternKind, pdf.PatternMode, pdf.FillRule, pdf.BlendMode, pdf.PageRotation, pdf.reader.ShapeRun, pdf.reader.ShapeKind, pdf.reader.ShapeLineCap, pdf.reader.ShapeLineJoin, Options, Executor, Plan, PngBatch, RasterBatch, PngResult, RasterResult, pdf.PreparedPageRenderPlan, @FieldType(pdf.PreparedPageRenderPlan, "_geometry"), pdf.PageRenderRequest, pdf.PageRenderGeometry, pdf.RenderedPagePng, pdf.RenderedPageRaster, pdf.reader.PageTextAnalysis, pdf.reader.TextRun, pdf.reader.PageRenderDiagnostics, Probe, Limits, Box, A.VTable, pdf.RenderProfile, pdf.RenderQuality, pdf.PixelFormat, pdf.RenderBatchOutputKind };
pub const layouts = blk: {
    @setEvalBranchQuota(100_000);
    var result: [layout_types.len]native.TypeContract = undefined;
    for (layout_types, 0..) |T, index| result[index] = .of(T);
    break :blk result;
};
pub const Endpoint = extern struct {
    version: u32 = 3,
    layout_count: u32 = layouts.len,
    contract: native.CallContract,
    layout: *const [layouts.len]native.TypeContract = &layouts,
    invoke: *const fn (*const anyopaque, *anyopaque, WorkBorrow) callconv(.c) void,
};
pub fn validate(comptime name: []const u8, endpoint: *const Endpoint) !void {
    if (endpoint.version != 3 or endpoint.layout_count != layouts.len or !endpoint.contract.matches(signature(name))) return error.InvalidArgument;
    for (endpoint.layout.*, layouts, 0..) |actual, expected, index| if (!actual.matches(expected)) {
        std.debug.print("PDF native layout mismatch op={s} index={d} actual={any} expected={any}\n", .{ name, index, actual, expected });
        return error.InvalidArgument;
    };
}
pub fn call(comptime name: []const u8, args: Args(name)) !Value(name) {
    const getter = @extern(*const fn () callconv(.c) *const Endpoint, .{ .name = "antfly_pdf_native_" ++ name ++ "_v3" });
    const endpoint = getter();
    try validate(name, endpoint);
    var out: Wire(name) = undefined;
    const lease: WorkLease = .{ .control = work.current() };
    endpoint.invoke(&args, &out, lease.borrow());
    if (out.failure.localError()) |err| return err;
    if (out.failure.ptr) |ptr| return @import("engine_errors").fromName(ptr[0..out.failure.len]);
    return out.value;
}
