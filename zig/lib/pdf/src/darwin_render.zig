// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

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
    alloc: std.mem.Allocator,
    pdf_bytes: []const u8,
    page_number: usize,
    dpi: u16,
    max_pixels: u64,
    rotation: render.PageRotation,
) ![]u8 {
    var session = try Session.init(pdf_bytes);
    defer session.deinit();
    return try session.renderPagePngAlloc(alloc, page_number, dpi, max_pixels, rotation);
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
        alloc: std.mem.Allocator,
        page_number: usize,
        dpi: u16,
        max_pixels: u64,
        rotation: render.PageRotation,
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
        return try self.session.?.renderPagePngAlloc(alloc, page_number, dpi, max_pixels, rotation);
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
        1,
        72,
        1_000_000,
        .none,
        Cancelled{},
    ));
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
        alloc: std.mem.Allocator,
        page_number: usize,
        dpi: u16,
        max_pixels: u64,
        rotation: render.PageRotation,
    ) ![]u8 {
        if (page_number == 0) return error.InvalidPageNumber;
        if (page_number > CGPDFDocumentGetNumberOfPages(self.document)) return error.InvalidPageNumber;
        const page = CGPDFDocumentGetPage(self.document, page_number) orelse return error.SystemPdfRenderingFailed;
        const box = CGPDFPageGetBoxRect(page, crop_box);
        if (!(box.size.width > 0) or !(box.size.height > 0)) return error.InvalidPageBox;
        const scale = @as(f64, @floatFromInt(dpi)) / 72.0;
        const swaps_dimensions = rotation == .clockwise_90 or rotation == .clockwise_270;
        const display_width = if (swaps_dimensions) box.size.height else box.size.width;
        const display_height = if (swaps_dimensions) box.size.width else box.size.height;
        const width_f = @ceil(display_width * scale);
        const height_f = @ceil(display_height * scale);
        if (width_f > @as(f64, @floatFromInt(std.math.maxInt(u32))) or height_f > @as(f64, @floatFromInt(std.math.maxInt(u32)))) return error.RenderedPageTooLarge;
        const width: u32 = @intFromFloat(width_f);
        const height: u32 = @intFromFloat(height_f);
        const pixel_count = @as(u64, width) * @as(u64, height);
        if (pixel_count == 0 or pixel_count > max_pixels) return error.RenderedPageTooLarge;
        const row_bytes = std.math.mul(usize, width, 4) catch return error.RenderedPageTooLarge;
        const rgba = try alloc.alloc(u8, std.math.mul(usize, row_bytes, height) catch return error.RenderedPageTooLarge);
        defer alloc.free(rgba);

        const color_space = CGColorSpaceCreateDeviceRGB() orelse return error.SystemPdfRenderingFailed;
        defer CGColorSpaceRelease(color_space);
        const context = CGBitmapContextCreate(rgba.ptr, width, height, 8, row_bytes, color_space, bitmap_rgba_premultiplied) orelse return error.SystemPdfRenderingFailed;
        defer CGContextRelease(context);
        const destination = CGRect{ .origin = .{ .x = 0, .y = 0 }, .size = .{ .width = @floatFromInt(width), .height = @floatFromInt(height) } };
        CGContextSetRGBFillColor(context, 1, 1, 1, 1);
        CGContextFillRect(context, destination);
        CGContextConcatCTM(context, CGPDFPageGetDrawingTransform(page, crop_box, destination, 0, true));
        CGContextDrawPDFPage(context, page);
        return try image.png.encodeRgba(alloc, width, height, rgba);
    }
};
