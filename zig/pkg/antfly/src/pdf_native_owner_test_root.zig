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
    endpoint.version = 0;
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
