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

//! Physical retained PDF reader, immutable fork and complete batch operations.
const std = @import("std");
const c = @import("pdf_native_contract");
const pdf = @import("antfly_pdf");
const A = std.mem.Allocator;
const Reader = struct { value: pdf.reader.Reader, allocator: A };
const Fork = struct { value: pdf.reader.RenderForkTemplate, allocator: A };
fn reader(doc: *c.Document) *Reader {
    return @ptrCast(@alignCast(doc));
}
fn forked(doc: *c.Fork) *Fork {
    return @ptrCast(@alignCast(doc));
}
fn wrap(a: A, value: pdf.reader.Reader) !*c.Document {
    var owned = value;
    errdefer owned.deinit();
    const state = try a.create(Reader);
    state.* = .{ .value = owned, .allocator = a };
    return @ptrCast(state);
}
const OptionLease = struct {
    options: c.Options,
    io: ?@TypeOf(@as(c.native.IoBorrow, undefined).receive() catch unreachable) = null,
    // The native I/O receiver lives at this stable address through all workers.
    host_failure: c.Failure = .{},
    fn init(options: c.Options) !OptionLease {
        return .{ .options = options, .io = if (options.executor_io) |io| try io.receive() else null };
    }
    fn get(self: *OptionLease) pdf.PageRenderBatchOptions {
        var result: pdf.PageRenderBatchOptions = .{};
        inline for (@typeInfo(c.Options).@"struct".field_names) |name| {
            if (comptime !std.mem.eql(u8, name, "executor") and !std.mem.eql(u8, name, "executor_io") and !std.mem.eql(u8, name, "fork_template"))
                @field(result, name) = @field(self.options, name);
        }
        result.fork_template = if (self.options.fork_template) |f| &forked(f).value else null;
        result.executor = if (self.options.executor) |e| .{ .ptr = self, .concurrent_capacity = e.capacity, .run_batch_fn = run } else null;
        result.executor_io = if (self.io) |*io| io.io() else null;
        return result;
    }
    fn run(ptr: *anyopaque, contexts: []const *anyopaque, operation: *const fn (*anyopaque, A) void, limit: usize) anyerror!pdf.PageRenderExecutor.BatchStats {
        const self: *OptionLease = @ptrCast(@alignCast(ptr));
        const executor = self.options.executor.?;
        const result = executor.run(executor.context, contexts, operation, limit);
        if (result.failure.ptr != null) {
            self.host_failure = result.failure;
            return error.PdfNativeExecutorFailure;
        }
        return result.value;
    }
    fn failure(self: *OptionLease, err: anyerror) c.Failure {
        return if (err == error.PdfNativeExecutorFailure and self.host_failure.ptr != null) self.host_failure else .init(@errorName(err));
    }
};
fn plansLocal(a: A, plans: []const c.Plan) ![]pdf.PreparedPageRenderPlan {
    const result = try a.alloc(pdf.PreparedPageRenderPlan, plans.len);
    for (result, plans) |*out, value| out.* = value.providerLocal();
    return result;
}
pub const Ops = struct {
    pub fn renderRuns(doc: *c.Document, page: usize, width: u32, height: u32) !pdf.reader.PageRenderRuns {
        return reader(doc).value.extractPageRenderRunsForRasterAlloc(page, width, height);
    }
    pub fn open(a: A, bytes: []const u8, limits: c.Limits, probe: c.Probe) !*c.Document {
        return wrap(a, try pdf.reader.Reader.initWithDecodeLimitsAndCancellation(a, bytes, limits, probe));
    }
    pub fn close(doc: *c.Document) void {
        const state = reader(doc);
        const a = state.allocator;
        state.value.deinit();
        a.destroy(state);
    }
    pub fn setLimits(doc: *c.Document, limits: c.Limits) !void {
        try reader(doc).value.setDecodeLimits(limits);
    }
    pub fn setProbe(doc: *c.Document, probe: c.Probe) void {
        reader(doc).value.setCancellationProbe(probe);
    }
    pub fn count(doc: *c.Document) !usize {
        return reader(doc).value.pageCount();
    }
    pub fn text(doc: *c.Document, page: usize) ![]u8 {
        return reader(doc).value.extractPageTextAlloc(page);
    }
    pub fn plainText(doc: *c.Document) ![]u8 {
        return reader(doc).value.extractPlainTextAlloc();
    }
    pub fn analysis(doc: *c.Document, page: usize, limit: usize) !pdf.reader.PageTextAnalysis {
        return reader(doc).value.extractPageTextAnalysisWithRunLimitAlloc(page, limit);
    }
    pub fn runs(doc: *c.Document, page: usize) ![]pdf.reader.TextRun {
        return reader(doc).value.extractPageTextRunsAlloc(page);
    }
    pub fn box(doc: *c.Document, page: usize) !c.Box {
        return reader(doc).value.extractPageBox(page);
    }
    pub fn rotation(doc: *c.Document, page: usize) !?i32 {
        return reader(doc).value.extractPageRotation(page);
    }
    pub fn forkOpen(doc: *c.Document, a: A, probe: c.Probe) !*c.Fork {
        var value = try reader(doc).value.prepareRenderForkTemplate(a, probe);
        errdefer value.deinit();
        const state = try a.create(Fork);
        state.* = .{ .value = value, .allocator = a };
        return @ptrCast(state);
    }
    pub fn forkClose(fork: *c.Fork) void {
        const state = forked(fork);
        const a = state.allocator;
        state.value.deinit();
        a.destroy(state);
    }
    pub fn forkRefresh(fork: *c.Fork, doc: *c.Document, probe: c.Probe) !void {
        try forked(fork).value.refreshFrom(&reader(doc).value, probe);
    }
    pub fn forkInstantiate(fork: *c.Fork, a: A, probe: c.Probe) !*c.Document {
        return wrap(a, try forked(fork).value.instantiate(a, probe));
    }
    pub fn plan(doc: *c.Document, request: pdf.PageRenderRequest) !c.Plan {
        return .fromLocal(try pdf.prepareParsedPageRenderPlan(&reader(doc).value, request));
    }
    pub fn admit(doc: *c.Document, request: pdf.PageRenderRequest, options: c.Options, kind: pdf.RenderBatchOutputKind) !c.Plan {
        var lease = try OptionLease.init(options);
        return .fromLocal(try pdf.prepareAdmittedPageRenderPlan(&reader(doc).value, request, lease.get(), kind));
    }
    pub fn estimate(doc: *c.Document, plans: []const c.Plan, parallel: usize, reserve: usize) !usize {
        const a = reader(doc).allocator;
        const local = try plansLocal(a, plans);
        defer a.free(local);
        return pdf.estimatePreparedPageRenderWaveScratchBytes(&reader(doc).value, local, parallel, reserve);
    }
    pub fn pngExact(a: A, doc: *c.Document, page: usize, dpi: u16, pixels: u64) ![]u8 {
        return pdf.renderParsedPagePngAlloc(a, &reader(doc).value, page, dpi, pixels);
    }
    pub fn png(a: A, doc: *c.Document, page: usize, dpi: u16, pixels: u64, dimension: u32, profile: pdf.RenderProfile) !pdf.RenderedPagePng {
        return pdf.renderParsedPagePngAdaptiveWithProfileAlloc(a, &reader(doc).value, page, dpi, pixels, dimension, profile);
    }
    pub fn raster(a: A, doc: *c.Document, page: usize, dpi: u16, pixels: u64, dimension: u32, profile: pdf.RenderProfile) !pdf.RenderedPageRaster {
        return pdf.renderParsedPageRasterAdaptiveWithProfileAlloc(a, &reader(doc).value, page, dpi, pixels, dimension, profile);
    }
    pub fn batchPng(a: A, doc: *c.Document, plans: ?[]const c.Plan, requests: []const pdf.PageRenderRequest, options: c.Options) !c.PngBatch {
        var lease = try OptionLease.init(options);
        const local = if (plans) |values| try plansLocal(a, values) else null;
        defer if (local) |values| a.free(values);
        var batch = (if (local) |values| pdf.renderPreparedPagesBatchAlloc(a, &reader(doc).value, values, lease.get()) else pdf.renderParsedPagesBatchAlloc(a, &reader(doc).value, requests, lease.get())) catch |err| return .{ .results = &.{}, .failure = lease.failure(err) };
        defer batch.deinit(a);
        const results = try a.alloc(c.PngResult, batch.results.len);
        for (results, batch.results, 0..) |*out, *value, index| {
            out.* = .{ .page_number = value.page_number, .rendered = value.rendered, .failure = if (value.failure) |err| lease.failure(err) else .{}, .render_elapsed_ns = value.render_elapsed_ns };
            if (plans) |original| if (original[index].failure.ptr != null) {
                out.failure = original[index].failure;
            };
            value.rendered = null;
        }
        return .{ .results = results, .requested_parallelism = batch.requested_parallelism, .peak_launched_workers = batch.peak_launched_workers, .peak_parallelism = batch.peak_parallelism, .peak_admitted_pixels = batch.peak_admitted_pixels, .peak_admitted_bytes = batch.peak_admitted_bytes, .peak_worker_scratch_bytes = batch.peak_worker_scratch_bytes, .thread_spawn_fallbacks = batch.thread_spawn_fallbacks };
    }
    pub fn batchRaster(a: A, doc: *c.Document, plans: ?[]const c.Plan, requests: []const pdf.PageRenderRequest, options: c.Options) !c.RasterBatch {
        var lease = try OptionLease.init(options);
        const local = if (plans) |values| try plansLocal(a, values) else null;
        defer if (local) |values| a.free(values);
        var batch = (if (local) |values| pdf.renderPreparedPagesRasterBatchAlloc(a, &reader(doc).value, values, lease.get()) else pdf.renderParsedPagesRasterBatchAlloc(a, &reader(doc).value, requests, lease.get())) catch |err| return .{ .results = &.{}, .failure = lease.failure(err) };
        defer batch.deinit(a);
        const results = try a.alloc(c.RasterResult, batch.results.len);
        for (results, batch.results, 0..) |*out, *value, index| {
            out.* = .{ .page_number = value.page_number, .rendered = value.rendered, .failure = if (value.failure) |err| lease.failure(err) else .{}, .render_elapsed_ns = value.render_elapsed_ns };
            if (plans) |original| if (original[index].failure.ptr != null) {
                out.failure = original[index].failure;
            };
            value.rendered = null;
        }
        return .{ .results = results, .requested_parallelism = batch.requested_parallelism, .peak_launched_workers = batch.peak_launched_workers, .peak_parallelism = batch.peak_parallelism, .peak_admitted_pixels = batch.peak_admitted_pixels, .peak_admitted_bytes = batch.peak_admitted_bytes, .peak_worker_scratch_bytes = batch.peak_worker_scratch_bytes, .thread_spawn_fallbacks = batch.thread_spawn_fallbacks };
    }
};
fn Bridge(comptime name: []const u8) type {
    return struct {
        fn invoke(raw: *const anyopaque, output: *anyopaque) callconv(.c) void {
            const args: *const c.Args(name) = @ptrCast(@alignCast(raw));
            const out: *c.Wire(name) = @ptrCast(@alignCast(output));
            const result = @call(.auto, @field(Ops, name), args.*);
            const value = if (@typeInfo(@TypeOf(result)) == .error_union) result catch |err| {
                out.* = .{ .failure = .init(@errorName(err)) };
                return;
            } else result;
            out.* = .{ .value = value };
        }
        const endpoint: c.Endpoint = .{ .contract = c.signature(name), .invoke = invoke };
        fn get() callconv(.c) *const c.Endpoint {
            return &endpoint;
        }
    };
}
pub fn exportOperations() void {
    inline for (@typeInfo(c.Methods).@"struct".field_names) |name|
        @export(&Bridge(name).get, .{ .name = "antfly_pdf_native_" ++ name ++ "_v1", .visibility = .hidden });
}
