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

const std = @import("std");
const pdf = @import("pdf");
const c = @import("pdf_native_contract");
fn fixture(a: std.mem.Allocator) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(std.testing.io, "lib/pdf/testdata/simple_text_fixture.pdf", a, .limited(1024 * 1024));
}
test "compiled PDF native retained analysis and fork cache" {
    const a = std.testing.allocator;
    const bytes = try fixture(a);
    defer a.free(bytes);
    var doc = try pdf.reader.Reader.init(a, bytes);
    defer doc.deinit();
    try std.testing.expectEqual(@as(usize, 1), try doc.pageCount());
    var text = try doc.extractPageTextAnalysisAlloc(1);
    defer text.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, text.text, "Hello Fixture") != null);
    var fork = try doc.prepareRenderForkTemplate(a, .{});
    defer fork.deinit();
    var child = try fork.instantiate(a, .{});
    defer child.deinit();
    const child_text = try child.extractPageTextAlloc(1);
    defer a.free(child_text);
    try std.testing.expectEqualStrings(text.text, child_text);
    try std.testing.expectError(error.PageOutOfRange, doc.extractPageTextAlloc(99));
}
test "compiled PDF native batch PNG raster and prepared plans" {
    const a = std.testing.allocator;
    const bytes = try fixture(a);
    defer a.free(bytes);
    var doc = try pdf.reader.Reader.init(a, bytes);
    defer doc.deinit();
    const plan = try pdf.prepareParsedPageRenderPlan(&doc, .{ .page_number = 1, .requested_dpi = 72, .max_dimension = 256 });
    var png = try pdf.renderPreparedPagesBatchAlloc(a, &doc, &.{plan}, .{});
    defer png.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), png.results.len);
    try std.testing.expect(png.results[0].failure == null);
    try std.testing.expect(png.results[0].rendered != null);
    var raster = try pdf.renderPreparedPagesRasterBatchAlloc(a, &doc, &.{plan}, .{});
    defer raster.deinit(a);
    const pixels = raster.results[0].rendered.?;
    try std.testing.expect(pixels.bytes.len > 0);
    try std.testing.expectEqual(pixels.stride * pixels.height, pixels.bytes.len);
    var bad = plan;
    bad.failure = error.OutOfMemory;
    var failed = try pdf.renderPreparedPagesBatchAlloc(a, &doc, &.{bad}, .{});
    defer failed.deinit(a);
    try std.testing.expectEqual(error.OutOfMemory, failed.results[0].failure.?);
}
test "compiled PDF native rejects incompatible operation layout" {
    // The endpoint guard rejects stale contracts before touching opaque state.
    var endpoint: c.Endpoint = .{ .contract = c.signature("count"), .invoke = undefined };
    try c.validate("count", &endpoint);
    // A stale provider/table shape must fail before the borrowed table is read.
    endpoint.layout = @ptrFromInt(@alignOf(@TypeOf(c.layouts)));
    endpoint.version = 2;
    try std.testing.expectError(error.InvalidArgument, c.validate("count", &endpoint));
    endpoint.version = 3;
    endpoint.layout_count = c.layouts.len - 1;
    try std.testing.expectError(error.InvalidArgument, c.validate("count", &endpoint));
    endpoint.layout_count = c.layouts.len + 1;
    try std.testing.expectError(error.InvalidArgument, c.validate("count", &endpoint));
}

test "compiled PDF native executor errors stay in the caller error domain" {
    const a = std.testing.allocator;
    const bytes = try fixture(a);
    defer a.free(bytes);
    var doc = try pdf.reader.Reader.init(a, bytes);
    defer doc.deinit();
    const Executor = struct {
        fail: bool = false,
        calls: usize = 0,
        fn run(ptr: *anyopaque, contexts: []const *anyopaque, operation: *const fn (*anyopaque, std.mem.Allocator) void, _: usize) anyerror!pdf.PageRenderExecutor.BatchStats {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.fail) return error.PdfExecutorCallerOnlySentinel;
            var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
            defer arena.deinit();
            for (contexts) |context| {
                operation(context, arena.allocator());
                _ = arena.reset(.free_all);
            }
            return .{ .peak_parallelism = 1 };
        }
    };
    var executor = Executor{};
    var fork = try doc.prepareRenderForkTemplate(a, .{});
    defer fork.deinit();
    const options: pdf.PageRenderBatchOptions = .{
        .max_parallel_pages = 2,
        .fork_template = &fork,
        .executor = .{ .ptr = &executor, .concurrent_capacity = 2, .run_batch_fn = Executor.run },
        .concurrent_output_allocator = a,
    };
    const request: pdf.PageRenderRequest = .{ .page_number = 1, .requested_dpi = 72, .max_dimension = 64 };
    var batch = try pdf.renderParsedPagesRasterBatchAlloc(a, &doc, &.{ request, request }, options);
    defer batch.deinit(a);
    try std.testing.expect(executor.calls > 0);
    try std.testing.expectEqual(@as(usize, 2), batch.results.len);
    for (batch.results) |result| {
        try std.testing.expect(result.failure == null);
        try std.testing.expect(result.rendered != null);
    }
    executor.fail = true;
    try std.testing.expectError(error.PdfExecutorCallerOnlySentinel, pdf.renderParsedPagesBatchAlloc(a, &doc, &.{request}, options));
    var plan = try pdf.prepareParsedPageRenderPlan(&doc, request);
    plan.failure = error.PdfPlanCallerOnlySentinel;
    var failed = try pdf.renderPreparedPagesBatchAlloc(a, &doc, &.{plan}, .{});
    defer failed.deinit(a);
    try std.testing.expectEqual(error.PdfPlanCallerOnlySentinel, failed.results[0].failure.?);
}

test "compiled PDF native cancellation limits and IO executor remain usable" {
    const a = std.testing.allocator;
    const bytes = try fixture(a);
    defer a.free(bytes);
    const Probe = struct {
        fn cancelled(ptr: ?*const anyopaque) bool {
            const flag: *const bool = @ptrCast(@alignCast(ptr.?));
            return flag.*;
        }
    };
    var cancelled = true;
    try std.testing.expectError(error.Canceled, pdf.reader.Reader.initWithDecodeLimitsAndCancellation(a, bytes, .{}, .{ .context = &cancelled, .is_cancelled_fn = Probe.cancelled }));
    cancelled = false;
    var doc = try pdf.reader.Reader.initWithDecodeLimitsAndCancellation(a, bytes, .{}, .{ .context = &cancelled, .is_cancelled_fn = Probe.cancelled });
    defer doc.deinit();
    try std.testing.expectError(error.InvalidPdfDecodeLimits, doc.setDecodeLimits(.{ .max_decoded_stream_bytes = 0 }));
    var batch = try pdf.renderParsedPagesBatchAlloc(a, &doc, &.{ .{ .page_number = 1, .requested_dpi = 72, .max_dimension = 64 }, .{ .page_number = 1, .requested_dpi = 72, .max_dimension = 64 } }, .{ .executor_io = std.testing.io, .max_parallel_pages = 2 });
    defer batch.deinit(a);
    try std.testing.expect(batch.results[0].failure == null);
    try std.testing.expect((try doc.pageCount()) > 0);
}

test "compiled PDF native allocation failures release retained and result state" {
    const bytes = try fixture(std.testing.allocator);
    defer std.testing.allocator.free(bytes);
    const Exercise = struct {
        fn run(a: std.mem.Allocator, input: []const u8) !void {
            var doc = try pdf.reader.Reader.init(a, input);
            defer doc.deinit();
            var analysis = try doc.extractPageTextAnalysisAlloc(1);
            defer analysis.deinit(a);
            var fork = try doc.prepareRenderForkTemplate(a, .{});
            defer fork.deinit();
            var child = try fork.instantiate(a, .{});
            defer child.deinit();
            const plan = try pdf.prepareParsedPageRenderPlan(&child, .{ .page_number = 1, .requested_dpi = 72, .max_dimension = 32 });
            var batch = try pdf.renderPreparedPagesRasterBatchAlloc(a, &child, &.{plan}, .{});
            defer batch.deinit(a);
            if (batch.results[0].failure) |err| return err;
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Exercise.run, .{bytes});
}

test "compiled PDF native backend factories and direct adaptive outputs" {
    const a = std.testing.allocator;
    const bytes = try fixture(a);
    defer a.free(bytes);
    const backend = pdf.Backend.system();
    const text = try backend.extractText(a, bytes);
    defer a.free(text);
    try std.testing.expectEqualStrings("Hello Fixture\n", text);
    const png = try backend.renderFirstPagePng(a, bytes);
    defer a.free(png);
    var doc = try pdf.reader.Reader.init(a, bytes);
    defer doc.deinit();
    const direct = try pdf.renderParsedPagePngAlloc(a, &doc, 1, 72, 40_000_000);
    defer a.free(direct);
    try std.testing.expectEqualSlices(u8, png, direct);
    var adaptive = try pdf.renderParsedPagePngAdaptiveWithProfileAlloc(a, &doc, 1, 150, 1024 * 1024, 128, .ocr);
    defer adaptive.deinit(a);
    try std.testing.expect(adaptive.width <= 128 and adaptive.height <= 128);
    var raster = try pdf.renderParsedPageRasterAdaptiveAlloc(a, &doc, 1, 72, 1024 * 1024, 64);
    defer raster.deinit(a);
    try std.testing.expect(raster.bytes.len > 0);
    var runs = try doc.extractPageRenderRunsForRasterAlloc(1, raster.width, raster.height);
    defer runs.deinit(a);
    try std.testing.expect(runs.page_box.max_x > runs.page_box.min_x);
}

test "compiled PDF native decoder cancellation retains caller error identity on all worker paths" {
    const a = std.testing.allocator;
    const bytes = try jpegFixture(a);
    defer a.free(bytes);
    var doc = try pdf.reader.Reader.init(a, bytes);
    defer doc.deinit();
    const Control = struct {
        calls: std.atomic.Value(usize) = .init(0),
        reverse: bool = false,
        fn check(ptr: ?*const anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr.?)));
            _ = self.calls.fetchAdd(1, .monotonic);
            return error.PdfCallerOnlyDecoderStopped;
        }
        fn run(raw: *anyopaque, contexts: []const *anyopaque, operation: *const fn (*anyopaque, std.mem.Allocator) void, _: usize) anyerror!pdf.PageRenderExecutor.BatchStats {
            const self: *@This() = @ptrCast(@alignCast(raw));
            for (0..contexts.len) |i| operation(contexts[if (self.reverse) contexts.len - 1 - i else i], std.heap.page_allocator);
            return .{ .peak_parallelism = contexts.len };
        }
    };
    var control: Control = .{};
    const request: pdf.PageRenderRequest = .{ .page_number = 1, .requested_dpi = 72, .max_dimension = 64 };
    {
        const scope = c.work.Scope.enter(.{ .context = &control, .check_fn = Control.check });
        defer scope.deinit();
        try std.testing.expectError(error.PdfCallerOnlyDecoderStopped, pdf.renderParsedPagePngAlloc(a, &doc, 1, 72, 1_000_000));
        for (0..4) |mode| {
            control.reverse = mode == 3;
            var options: pdf.PageRenderBatchOptions = .{ .max_parallel_pages = 2 };
            if (mode == 1) options.executor_io = std.testing.io;
            if (mode >= 2) options.executor = .{ .ptr = &control, .concurrent_capacity = 2, .run_batch_fn = Control.run };
            try std.testing.expectError(error.PdfCallerOnlyDecoderStopped, pdf.renderParsedPagesBatchAlloc(a, &doc, &.{ request, request }, options));
        }
    }
    try std.testing.expect(control.calls.load(.monotonic) >= 5);
    const recovered = try pdf.renderParsedPagePngAlloc(a, &doc, 1, 72, 1_000_000);
    defer a.free(recovered);
    try std.testing.expect(recovered.len > 0);
    // A token from a different module is never converted to a local error.
    const foreign: c.Failure = .{ .ptr = "Canceled".ptr, .len = "Canceled".len, .origin = &control, .token = std.math.maxInt(usize) };
    try std.testing.expect(foreign.localError() == null);
}

fn jpegFixture(alloc: std.mem.Allocator) ![]u8 {
    const jpeg_bytes = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, "testdata/image/jpeg/restart/gray-16x16-rst1.jpg", alloc, .limited(1024 * 1024));
    defer alloc.free(jpeg_bytes);

    const content = "q\n16 0 0 16 0 0 cm\n/Im0 Do\nQ\n";
    const objects = [_][]const u8{
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n",
        "2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n",
        "3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 32 32] /Resources << /XObject << /Im0 5 0 R >> >> /Contents 4 0 R >>\nendobj\n",
        try std.fmt.allocPrint(alloc, "4 0 obj\n<< /Length {d} >>\nstream\n{s}endstream\nendobj\n", .{ content.len, content }),
        try std.fmt.allocPrint(
            alloc,
            "5 0 obj\n<< /Type /XObject /Subtype /Image /Width 16 /Height 16 /ColorSpace /DeviceGray /BitsPerComponent 8 /Filter /DCTDecode /Length {d} >>\nstream\n{s}\nendstream\nendobj\n",
            .{ jpeg_bytes.len, jpeg_bytes },
        ),
    };
    defer alloc.free(objects[3]);
    defer alloc.free(objects[4]);

    var prefix = std.ArrayList(u8).empty;
    defer prefix.deinit(alloc);
    try prefix.appendSlice(alloc, "%PDF-1.7\n");
    var offsets: [objects.len]usize = undefined;
    for (objects, 0..) |obj_src, i| {
        offsets[i] = prefix.items.len;
        try prefix.appendSlice(alloc, obj_src);
    }
    const xref_offset = prefix.items.len;
    try prefix.appendSlice(alloc, "xref\n0 6\n0000000000 65535 f \n");
    for (offsets) |off| {
        const line = try std.fmt.allocPrint(alloc, "{d:0>10} 00000 n \n", .{off});
        defer alloc.free(line);
        try prefix.appendSlice(alloc, line);
    }
    try prefix.appendSlice(alloc, "trailer\n<< /Size 6 /Root 1 0 R >>\n");

    const sample = try std.fmt.allocPrint(alloc, "{s}startxref\n{d}\n%%EOF\n", .{ prefix.items, xref_offset });
    return sample;
}

test "compiled PDF native JPEG inner-loop control survives analysis and raster workers" {
    const a = std.testing.allocator;
    const bytes = try jpegFixture(a);
    defer a.free(bytes);
    const Control = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fail_after: usize = std.math.maxInt(usize),
        fn check(ptr: ?*const anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr.?)));
            if (self.calls.fetchAdd(1, .monotonic) >= self.fail_after) return error.PdfCallerOnlyJpegDeadline;
        }
    };
    var control: Control = .{};
    const scope = c.work.Scope.enter(.{ .context = &control, .check_fn = Control.check });
    defer scope.deinit();
    var doc = try pdf.reader.Reader.init(a, bytes);
    defer doc.deinit();
    var good = try doc.extractPageRenderRunsForRasterAlloc(1, 32, 32);
    defer good.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), good.image_runs.len);
    const baseline_checks = control.calls.load(.monotonic);
    try std.testing.expect(baseline_checks >= 2);
    control.calls.store(0, .monotonic);
    control.fail_after = 1;
    // A fresh owner avoids the successful decoded-image cache.
    var cancelled = try pdf.reader.Reader.init(a, bytes);
    defer cancelled.deinit();
    try std.testing.expectError(error.PdfCallerOnlyJpegDeadline, cancelled.extractPageRenderRunsForRasterAlloc(1, 32, 32));
    try std.testing.expect(control.calls.load(.monotonic) > 1);
    control.calls.store(0, .monotonic);
    control.fail_after = 0;
    var worker = try pdf.reader.Reader.init(a, bytes);
    defer worker.deinit();
    const request: pdf.PageRenderRequest = .{ .page_number = 1, .requested_dpi = 72, .max_dimension = 32 };
    try std.testing.expectError(error.PdfCallerOnlyJpegDeadline, pdf.renderParsedPagesRasterBatchAlloc(a, &worker, &.{request}, .{}));
}
