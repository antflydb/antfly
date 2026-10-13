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

//! Production Apple Vision OCR corpus baseline for the Gemma4 OCR campaign.
const std = @import("std");
const internal = @import("inference_internal");
const Raster = internal.antfly_image.BorrowedRasterAttachment;
const Page = struct { path: []const u8, document: []const u8, page: usize };
const Corpus = struct { pages: []Page };
const VisionInput = extern struct {
    bytes: [*]const u8,
    length: usize,
    width: u32,
    height: u32,
    stride: usize,
    options: [*]const u8,
    options_length: usize,
    max_pixels: u64,
    context: ?*anyopaque,
    should_cancel: *const fn (?*anyopaque) callconv(.c) c_int,
    region: *const fn (?*anyopaque, [*]const u8, usize, f64, f64, f64, f64, f32) callconv(.c) c_int,
};
extern fn antfly_vision_read(*const VisionInput) c_int;
const Capture = struct {
    a: std.mem.Allocator,
    text: std.ArrayList(u8) = .empty,
    failure: ?anyerror = null,
    regions: usize = 0,
    fn canceled(_: ?*anyopaque) callconv(.c) c_int {
        return 0;
    }
    fn region(ptr: ?*anyopaque, bytes: [*]const u8, len: usize, _: f64, _: f64, _: f64, _: f64, _: f32) callconv(.c) c_int {
        const self: *Capture = @ptrCast(@alignCast(ptr.?));
        self.text.appendSlice(self.a, bytes[0..len]) catch |err| {
            self.failure = err;
            return 1;
        };
        self.text.append(self.a, '\n') catch |err| {
            self.failure = err;
            return 1;
        };
        self.regions += 1;
        return 0;
    }
};
const Wave = struct { iteration: usize, offset: usize, pages: usize, seconds: f64, ocr_seconds: f64, execution: []const u8 };
const Result = struct { document: []const u8, page: usize, text: []const u8 = "", regions: usize = 0 };
fn now() u64 {
    return internal.platform.time.monotonicNs();
}
fn sec(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / std.time.ns_per_s;
}
pub fn main(init: std.process.Init) !void {
    internal.execution_control.allowUninterruptibleInProcess();
    const a = std.heap.smp_allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const args = try init.minimal.args.toSlice(out);
    var corpus_path: ?[]const u8 = null;
    var output: ?[]const u8 = null;
    const mode: []const u8 = "apple";
    var recognition: []const u8 = "accurate";
    var batch: usize = 4;
    var warmups: usize = 2;
    var iterations: usize = 3;
    var i: usize = 1;
    while (i < args.len) : (i += 2) {
        if (i + 1 >= args.len) return error.MissingArgument;
        const k = args[i];
        const v = args[i + 1];
        if (std.mem.eql(u8, k, "--corpus")) corpus_path = v else if (std.mem.eql(u8, k, "--output")) output = v else if (std.mem.eql(u8, k, "--recognition")) recognition = v else if (std.mem.eql(u8, k, "--batch")) batch = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--warmups")) warmups = try std.fmt.parseInt(usize, v, 10) else if (std.mem.eql(u8, k, "--iterations")) iterations = try std.fmt.parseInt(usize, v, 10) else return error.UnknownArgument;
    }
    if (batch == 0 or batch > 32 or iterations == 0 or iterations > 100 or warmups > 100) return error.InvalidArguments;
    if (!std.mem.eql(u8, recognition, "accurate") and !std.mem.eql(u8, recognition, "fast")) return error.InvalidMode;
    const manifest = try std.Io.Dir.cwd().readFileAlloc(init.io, corpus_path orelse return error.MissingCorpus, out, .limited(8 * 1024 * 1024));
    const parsed = try std.json.parseFromSlice(Corpus, out, manifest, .{ .ignore_unknown_fields = true });
    const pages = parsed.value.pages;
    if (pages.len == 0 or pages.len > 128) return error.InvalidCorpus;
    const rasters = try out.alloc(Raster, pages.len);
    for (pages, rasters) |page, *raster| {
        const bytes = try std.Io.Dir.cwd().readFileAlloc(init.io, page.path, a, .limited(32 * 1024 * 1024));
        defer a.free(bytes);
        const img = try internal.pipelines.image.decode(a, bytes);
        defer img.deinit(a);
        const rgba = try out.alloc(u8, @as(usize, img.width) * img.height * 4);
        for (0..@as(usize, img.width) * img.height) |pixel| {
            @memcpy(rgba[pixel * 4 ..][0..3], img.data[pixel * 3 ..][0..3]);
            rgba[pixel * 4 + 3] = 255;
        }
        raster.* = .{ .bytes = rgba, .width = img.width, .height = img.height, .stride_bytes = @as(usize, img.width) * 4 };
    }
    const options = try std.json.Stringify.valueAlloc(out, .{ .recognition_level = recognition, .uses_language_correction = true, .recognition_languages = [_][]const u8{"en-US"} }, .{});
    var waves = std.ArrayList(Wave).empty;
    const totals = try out.alloc(f64, iterations);
    const results = try out.alloc(Result, pages.len);
    for (pages, results) |page, *r| r.* = .{ .document = page.document, .page = page.page };
    for (0..warmups + iterations) |iteration| {
        const started_iteration = now();
        var offset: usize = 0;
        while (offset < pages.len) {
            const count = @min(batch, pages.len - offset);
            const started_wave = now();
            const execution: []const u8 = "serialized_ocr";
            var ocr_ns: u64 = 0;
            for (rasters[offset..][0..count], results[offset..][0..count]) |raster, *r| {
                var capture = Capture{ .a = a };
                defer capture.text.deinit(a);
                const input = VisionInput{ .bytes = raster.bytes.ptr, .length = raster.bytes.len, .width = raster.width, .height = raster.height, .stride = raster.stride_bytes, .options = options.ptr, .options_length = options.len, .max_pixels = 8_000_000, .context = &capture, .should_cancel = Capture.canceled, .region = Capture.region };
                const began = now();
                const status = antfly_vision_read(&input);
                ocr_ns += now() - began;
                if (capture.failure) |err| return err;
                if (status != 0) {
                    std.debug.print("Vision status={d}\n", .{status});
                    return error.VisionFailed;
                }
                if (iteration + 1 == warmups + iterations) {
                    r.text = try out.dupe(u8, capture.text.items);
                    r.regions = capture.regions;
                }
            }
            if (iteration >= warmups) try waves.append(out, .{ .iteration = iteration - warmups, .offset = offset, .pages = count, .seconds = sec(now() - started_wave), .ocr_seconds = sec(ocr_ns), .execution = execution });
            offset += count;
        }
        if (iteration >= warmups) totals[iteration - warmups] = sec(now() - started_iteration);
        std.debug.print("{s}/{s} batch={d} iteration={d}/{d} seconds={d:.3}\n", .{ mode, recognition, batch, iteration + 1, warmups + iterations, sec(now() - started_iteration) });
    }
    const json = try std.json.Stringify.valueAlloc(out, .{ .mode = mode, .recognition = recognition, .batch = batch, .warmups = warmups, .pages = pages.len, .totals = totals, .waves = waves.items, .results = results, .scope = "Borrowed RGBA raster to OCR text; rendering and decode excluded", .apple_execution = "Production adapter: serialized per-page Vision requests; platform-selected compute" }, .{ .whitespace = .indent_2 });
    try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = output orelse return error.MissingOutput, .data = json });
}
