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
const image = @import("antfly_image");
const render = @import("render.zig");

const CFData = opaque {};
const CGDataProvider = opaque {};
const CGPDFDocument = opaque {};
const CGPDFPage = opaque {};
const CGColorSpace = opaque {};
const CGContext = opaque {};

const CGPoint = extern struct { x: f64, y: f64 };
const CGSize = extern struct { width: f64, height: f64 };
const CGRect = extern struct { origin: CGPoint, size: CGSize };
const CGAffineTransform = extern struct { a: f64, b: f64, c: f64, d: f64, tx: f64, ty: f64 };

extern var kCFAllocatorNull: ?*const anyopaque;
extern fn CFDataCreateWithBytesNoCopy(allocator: ?*const anyopaque, bytes: [*]const u8, length: isize, bytes_deallocator: ?*const anyopaque) ?*CFData;
extern fn CFRelease(value: *const anyopaque) void;
extern fn CGDataProviderCreateWithCFData(data: *CFData) ?*CGDataProvider;
extern fn CGDataProviderRelease(provider: *CGDataProvider) void;
extern fn CGPDFDocumentCreateWithProvider(provider: *CGDataProvider) ?*CGPDFDocument;
extern fn CGPDFDocumentRelease(document: *CGPDFDocument) void;
extern fn CGPDFDocumentGetNumberOfPages(document: *CGPDFDocument) usize;
extern fn CGPDFDocumentGetPage(document: *CGPDFDocument, page_number: usize) ?*CGPDFPage;
extern fn CGPDFPageGetBoxRect(page: *CGPDFPage, box: c_int) CGRect;
extern fn CGPDFPageGetRotationAngle(page: *CGPDFPage) c_int;
extern fn CGPDFPageGetDrawingTransform(page: *CGPDFPage, box: c_int, rect: CGRect, rotate: c_int, preserve_aspect_ratio: bool) CGAffineTransform;
extern fn CGColorSpaceCreateDeviceRGB() ?*CGColorSpace;
extern fn CGColorSpaceRelease(space: *CGColorSpace) void;
extern fn CGBitmapContextCreate(data: ?*anyopaque, width: usize, height: usize, bits_per_component: usize, bytes_per_row: usize, space: *CGColorSpace, bitmap_info: u32) ?*CGContext;
extern fn CGContextRelease(context: *CGContext) void;
extern fn CGContextSetRGBFillColor(context: *CGContext, red: f64, green: f64, blue: f64, alpha: f64) void;
extern fn CGContextFillRect(context: *CGContext, rect: CGRect) void;
extern fn CGContextConcatCTM(context: *CGContext, transform: CGAffineTransform) void;
extern fn CGContextDrawPDFPage(context: *CGContext, page: *CGPDFPage) void;

const crop_box: c_int = 1;
const bitmap_rgba_premultiplied: u32 = 1 | (4 << 12);

pub fn renderPagePngAlloc(
    scratch_alloc: std.mem.Allocator,
    output_alloc: std.mem.Allocator,
    pdf_bytes: []const u8,
    page_number: usize,
    canvas_size: render.CanvasSize,
    max_pixels: u64,
) ![]u8 {
    var session = try Session.init(pdf_bytes);
    defer session.deinit();
    return try session.renderPagePngAlloc(scratch_alloc, output_alloc, page_number, canvas_size, max_pixels);
}

pub fn renderPageRgbaAlloc(alloc: std.mem.Allocator, pdf_bytes: []const u8, page_number: usize, canvas_size: render.CanvasSize, max_pixels: u64) !render.RgbaCanvas {
    var session = try Session.init(pdf_bytes);
    defer session.deinit();
    return session.renderPageRgbaAlloc(alloc, page_number, canvas_size, max_pixels);
}

pub const SharedSession = struct {
    pdf_bytes: []const u8,
    mutex: std.Io.Mutex = .init,
    sync_io: std.Io,
    session: ?Session = null,

    pub fn init(pdf_bytes: []const u8) @This() {
        return .{
            .pdf_bytes = pdf_bytes,
            .sync_io = std.Io.Threaded.global_single_threaded.io(),
        };
    }

    pub fn deinit(self: *@This()) void {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        if (self.session) |*session| session.deinit();
        self.session = null;
    }

    pub fn renderPagePngAlloc(
        self: *@This(),
        scratch_alloc: std.mem.Allocator,
        output_alloc: std.mem.Allocator,
        page_number: usize,
        canvas_size: render.CanvasSize,
        max_pixels: u64,
        cancellation: anytype,
    ) ![]u8 {
        // CoreGraphics document concurrency and retained allocation behavior
        // are not part of our portable contract. One document-scoped session
        // therefore serializes only compatibility fallback pages; native Zig
        // pages continue rendering in parallel.
        try cancellation.check();
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        // Lock acquisition itself is deliberately blocking rather than a CPU
        // spin. Recheck after the wait so canceled queued work never enters
        // CoreGraphics once the current page releases the document session.
        try cancellation.check();
        if (self.session == null) self.session = try Session.init(self.pdf_bytes);
        return try self.session.?.renderPagePngAlloc(scratch_alloc, output_alloc, page_number, canvas_size, max_pixels);
    }

    /// Only the final canvas is charged to the retained-output allocator.
    /// Keep the same document lock and cancellation boundary as PNG rendering.
    pub fn renderPageRgbaAlloc(self: *@This(), alloc: std.mem.Allocator, page_number: usize, canvas_size: render.CanvasSize, max_pixels: u64, cancellation: anytype) !render.RgbaCanvas {
        try cancellation.check();
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        try cancellation.check();
        if (self.session == null) self.session = try Session.init(self.pdf_bytes);
        const raw = try self.session.?.renderPageRgbaAlloc(alloc, page_number, canvas_size, max_pixels);
        errdefer alloc.free(raw.rgba);
        try cancellation.check();
        return raw;
    }
};

test "shared session checks cancellation before opening the document" {
    const Cancelled = struct {
        fn check(_: @This()) !void {
            return error.Canceled;
        }
    };

    var session = SharedSession.init("not a pdf");
    defer session.deinit();
    try std.testing.expectError(error.Canceled, session.renderPagePngAlloc(
        std.testing.allocator,
        std.testing.allocator,
        1,
        .{ .width = 612, .height = 792 },
        1_000_000,
        Cancelled{},
    ));
}

test "compatibility raster needs only one retained canvas and matches PNG" {
    var session = SharedSession.init(@embedFile("../testdata/simple_text_fixture.pdf"));
    defer session.deinit();
    const Probe = struct {
        fn check(_: @This()) !void {}
    };
    const png = try session.renderPagePngAlloc(std.testing.allocator, std.testing.allocator, 1, .{ .width = 612, .height = 792 }, 1_000_000, Probe{});
    defer std.testing.allocator.free(png);
    const decoded = try image.png.decodeRgba(std.testing.allocator, png);
    defer std.testing.allocator.free(decoded.rgba);
    const storage = try std.testing.allocator.alloc(u8, decoded.rgba.len);
    defer std.testing.allocator.free(storage);
    var output = std.heap.FixedBufferAllocator.init(storage);
    const raw = try session.renderPageRgbaAlloc(output.allocator(), 1, .{ .width = 612, .height = 792 }, 1_000_000, Probe{});
    defer output.allocator().free(raw.rgba);
    try std.testing.expectEqualSlices(u8, decoded.rgba, raw.rgba);
    try std.testing.expectEqual(storage.len, output.end_index);
}

test "compatibility PNG retains only encoded bytes under the output budget" {
    const alloc = std.testing.allocator;
    var session = SharedSession.init(@embedFile("../testdata/simple_text_fixture.pdf"));
    defer session.deinit();
    const Probe = struct {
        fn check(_: @This()) !void {}
    };
    const reference_png = try session.renderPagePngAlloc(alloc, alloc, 1, .{ .width = 612, .height = 792 }, 1_000_000, Probe{});
    defer alloc.free(reference_png);
    const reference = try image.png.decodeRgba(alloc, reference_png);
    defer alloc.free(reference.rgba);
    const storage = try alloc.alloc(u8, reference_png.len);
    defer alloc.free(storage);
    var output = std.heap.FixedBufferAllocator.init(storage);
    const png = try session.renderPagePngAlloc(alloc, output.allocator(), 1, .{ .width = 612, .height = 792 }, 1_000_000, Probe{});
    defer output.allocator().free(png);
    const decoded = try image.png.decodeRgba(alloc, png);
    defer alloc.free(decoded.rgba);
    try std.testing.expectEqual(reference.width, decoded.width);
    try std.testing.expectEqual(reference.height, decoded.height);
    try std.testing.expectEqualSlices(u8, reference.rgba, decoded.rgba);
}

pub const Session = struct {
    data: *CFData,
    provider: *CGDataProvider,
    document: *CGPDFDocument,

    pub fn init(pdf_bytes: []const u8) !@This() {
        const data = CFDataCreateWithBytesNoCopy(
            null,
            pdf_bytes.ptr,
            std.math.cast(isize, pdf_bytes.len) orelse return error.PdfTooLarge,
            kCFAllocatorNull,
        ) orelse return error.SystemPdfRenderingFailed;
        errdefer CFRelease(data);
        const provider = CGDataProviderCreateWithCFData(data) orelse return error.SystemPdfRenderingFailed;
        errdefer CGDataProviderRelease(provider);
        const document = CGPDFDocumentCreateWithProvider(provider) orelse return error.SystemPdfRenderingFailed;
        return .{ .data = data, .provider = provider, .document = document };
    }

    pub fn deinit(self: *@This()) void {
        CGPDFDocumentRelease(self.document);
        CGDataProviderRelease(self.provider);
        CFRelease(self.data);
        self.* = undefined;
    }

    pub fn renderPagePngAlloc(
        self: *@This(),
        scratch_alloc: std.mem.Allocator,
        output_alloc: std.mem.Allocator,
        page_number: usize,
        canvas_size: render.CanvasSize,
        max_pixels: u64,
    ) ![]u8 {
        const raw = try self.renderPageRgbaAlloc(scratch_alloc, page_number, canvas_size, max_pixels);
        defer scratch_alloc.free(raw.rgba);
        return image.png.encodeRgbaWithCancellation(scratch_alloc, output_alloc, @intCast(raw.width), @intCast(raw.height), raw.rgba, .{});
    }

    pub fn renderPageRgbaAlloc(self: *@This(), alloc: std.mem.Allocator, page_number: usize, canvas_size: render.CanvasSize, max_pixels: u64) !render.RgbaCanvas {
        if (page_number == 0) return error.InvalidPageNumber;
        if (page_number > CGPDFDocumentGetNumberOfPages(self.document)) return error.InvalidPageNumber;
        const page = CGPDFDocumentGetPage(self.document, page_number) orelse return error.SystemPdfRenderingFailed;
        const box = CGPDFPageGetBoxRect(page, crop_box);
        if (!(box.size.width > 0) or !(box.size.height > 0) or !std.math.isFinite(box.size.width) or !std.math.isFinite(box.size.height)) return error.InvalidPageBox;
        // Allocation and metadata must use the parser's admitted integer
        // canvas, not a second rounding of CoreGraphics' floating CropBox.
        const width = std.math.cast(u32, canvas_size.width) orelse return error.RenderedPageTooLarge;
        const height = std.math.cast(u32, canvas_size.height) orelse return error.RenderedPageTooLarge;
        const pixel_count = @as(u64, width) * @as(u64, height);
        if (pixel_count == 0 or pixel_count > max_pixels) return error.RenderedPageTooLarge;
        const row_bytes = std.math.mul(usize, width, 4) catch return error.RenderedPageTooLarge;
        const rgba = try alloc.alloc(u8, std.math.mul(usize, row_bytes, height) catch return error.RenderedPageTooLarge);
        errdefer alloc.free(rgba);

        const color_space = CGColorSpaceCreateDeviceRGB() orelse return error.SystemPdfRenderingFailed;
        defer CGColorSpaceRelease(color_space);
        const context = CGBitmapContextCreate(rgba.ptr, width, height, 8, row_bytes, color_space, bitmap_rgba_premultiplied) orelse return error.SystemPdfRenderingFailed;
        defer CGContextRelease(context);
        const destination = CGRect{ .origin = .{ .x = 0, .y = 0 }, .size = .{ .width = @floatFromInt(width), .height = @floatFromInt(height) } };
        CGContextSetRGBFillColor(context, 1, 1, 1, 1);
        CGContextFillRect(context, destination);
        // GetDrawingTransform only scales down. Obtain the logical page
        // transform first, then scale its basis and translation explicitly.
        const quarter_turn = switch (@mod(CGPDFPageGetRotationAngle(page), 360)) {
            90, 270 => true,
            else => false,
        };
        const page_width = if (quarter_turn) box.size.height else box.size.width;
        const page_height = if (quarter_turn) box.size.width else box.size.height;
        const point_destination = CGRect{ .origin = .{ .x = 0, .y = 0 }, .size = .{ .width = page_width, .height = page_height } };
        var transform = CGPDFPageGetDrawingTransform(page, crop_box, point_destination, 0, true);
        const scale = @min(destination.size.width / page_width, destination.size.height / page_height);
        transform.a *= scale;
        transform.b *= scale;
        transform.c *= scale;
        transform.d *= scale;
        transform.tx = transform.tx * scale + (destination.size.width - page_width * scale) / 2;
        transform.ty = transform.ty * scale + (destination.size.height - page_height * scale) / 2;
        CGContextConcatCTM(context, transform);
        CGContextDrawPDFPage(context, page);
        return .{ .rgba = rgba, .width = width, .height = height };
    }
};
