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

extern fn CFDataCreate(allocator: ?*const anyopaque, bytes: [*]const u8, length: isize) ?*CFData;
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
    if (page_number == 0) return error.InvalidPageNumber;
    const data = CFDataCreate(null, pdf_bytes.ptr, std.math.cast(isize, pdf_bytes.len) orelse return error.PdfTooLarge) orelse return error.SystemPdfRenderingFailed;
    defer CFRelease(data);
    const provider = CGDataProviderCreateWithCFData(data) orelse return error.SystemPdfRenderingFailed;
    defer CGDataProviderRelease(provider);
    const document = CGPDFDocumentCreateWithProvider(provider) orelse return error.SystemPdfRenderingFailed;
    defer CGPDFDocumentRelease(document);
    if (page_number > CGPDFDocumentGetNumberOfPages(document)) return error.InvalidPageNumber;
    const page = CGPDFDocumentGetPage(document, page_number) orelse return error.SystemPdfRenderingFailed;
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
