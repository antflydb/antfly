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
const builtin = @import("builtin");
const options = @import("apple_reader_options");
const work = @import("antfly_inference_work");
const platform = @import("antfly_platform");
const httpx = @import("httpx");
const scraping = @import("antfly_scraping");
const readers = @import("mod.zig");
const reader_config = @import("antfly_reader_config");
const Allocator = std.mem.Allocator;

pub const enabled = builtin.os.tag == .macos and options.enabled;
pub const max_pixels_per_image: u64 = 16 * 1024 * 1024;
pub const max_batch_pixels: u64 = 64 * 1024 * 1024;
pub const max_batch_bytes: usize = 64 * 1024 * 1024;
pub const max_raster_bytes: usize = 256 * 1024 * 1024;
pub const max_images: usize = 8;
pub const default_response_bytes: usize = 8 * 1024 * 1024;
// Admission reservation for native work not allocated through the Zig arena.
// This is a conservative scheduling allowance, not a limit on OS model memory.
pub const native_workspace_reservation_bytes: usize = 128 * 1024 * 1024 + max_pixels_per_image * 16;
pub const testing = struct {
    pub const fixture_png = @embedFile("testdata/apple-ocr.png");
};

pub fn checkAvailable() !void {
    if (!enabled) return error.AppleProviderUnavailable;
}

pub fn capabilities() !work.InferenceCapabilities {
    try checkAvailable();
    return .{
        .task = .read,
        .input_modalities = .{ .image = true },
        .accepted_mime_types = .{ .image_png = true, .image_jpeg = true },
        .input_granularity = .page,
        .batch = .{
            .mode = .serial_compatibility,
            .preferred_items = 1,
            .max_items = max_images,
            .max_encoded_media_bytes = max_batch_bytes,
            .max_decoded_pixels = max_batch_pixels,
            .max_media_parts_per_item = 1,
        },
        .output = .read_result,
        .prompt_policy = .model_default,
        .borrowed_attachments = true,
        .borrowed_rasters = true,
        .attachment_payload_max_bytes = max_raster_bytes,
    };
}

const Input = extern struct {
    bytes: [*]const u8,
    length: usize,
    width: u32 = 0,
    height: u32 = 0,
    stride: usize = 0,
    options: [*]const u8,
    options_length: usize,
    max_pixels: u64 = max_pixels_per_image,
    context: *anyopaque,
    should_cancel: *const fn (*anyopaque) callconv(.c) c_int,
    region: *const fn (*anyopaque, [*]const u8, usize, f64, f64, f64, f64, f32) callconv(.c) c_int,
};
extern fn antfly_vision_read(input: *const Input) c_int;

const Region = struct {
    text: []const u8,
    bbox: [4]f64,
    confidence: f32,
    coordinate_space: []const u8 = "image_pixels_top_left",
};

const Collector = struct {
    alloc: Allocator,
    regions: std.ArrayList(Region) = .empty,
    response_limit: usize,
    charged: usize = 0,
    cancellation: ?httpx.CancellationToken,
    deadline_ns: ?u64,
    failure: ?anyerror = null,

    fn check(self: *const Collector) !void {
        if (self.cancellation) |token| if (token.isCancelled()) return error.Cancelled;
        if (self.deadline_ns) |deadline| if (platform.time.monotonicNs() >= deadline) return error.Timeout;
    }

    fn shouldCancel(ptr: *anyopaque) callconv(.c) c_int {
        const self: *Collector = @ptrCast(@alignCast(ptr));
        self.check() catch return 1;
        return 0;
    }

    fn region(ptr: *anyopaque, bytes: [*]const u8, len: usize, x1: f64, y1: f64, x2: f64, y2: f64, confidence: f32) callconv(.c) c_int {
        const self: *Collector = @ptrCast(@alignCast(ptr));
        self.append(bytes[0..len], .{ x1, y1, x2, y2 }, confidence) catch |err| {
            self.failure = err;
            return 1;
        };
        return 0;
    }

    fn append(self: *Collector, text: []const u8, bbox: [4]f64, confidence: f32) !void {
        try self.check();
        if (self.regions.items.len >= 4096) return error.ResponseTooLarge;
        // One copy in plain text and up to six bytes per byte in JSON, plus
        // coordinates and punctuation. Admission is conservative, never truncated.
        const charge = std.math.add(usize, try std.math.mul(usize, text.len, 7), 256) catch return error.ResponseTooLarge;
        const total = std.math.add(usize, self.charged, charge) catch return error.ResponseTooLarge;
        if (total > self.response_limit) return error.ResponseTooLarge;
        const owned = try self.alloc.dupe(u8, text);
        errdefer self.alloc.free(owned);
        try self.regions.append(self.alloc, .{ .text = owned, .bbox = bbox, .confidence = confidence });
        self.charged = total;
    }

    fn deinit(self: *Collector) void {
        for (self.regions.items) |item| self.alloc.free(item.text);
        self.regions.deinit(self.alloc);
    }

    fn result(self: *Collector, identity: work.WorkIdentity) !readers.Result {
        try self.check();
        var texts: std.ArrayList([]const u8) = .empty;
        defer texts.deinit(self.alloc);
        for (self.regions.items) |item| try texts.append(self.alloc, item.text);
        const text = try std.mem.join(self.alloc, "\n", texts.items);
        errdefer self.alloc.free(text);
        const regions_json = try std.json.Stringify.valueAlloc(self.alloc, self.regions.items, .{});
        errdefer self.alloc.free(regions_json);
        if (text.len > self.response_limit or regions_json.len > self.response_limit - text.len)
            return error.ResponseTooLarge;
        const item_id = if (identity.item_id.len > 0) try self.alloc.dupe(u8, identity.item_id) else "";
        errdefer if (item_id.len > 0) self.alloc.free(item_id);
        return .{
            .text = text,
            .regions_json = regions_json,
            .item_id = item_id,
            .source_fingerprint = if (identity.source_fingerprint) |value| try self.alloc.dupe(u8, value) else null,
            .page_number = identity.page_number,
        };
    }
};

fn nativeRead(alloc: Allocator, input: Input, opts: readers.RemoteOptions, deadline_ns: ?u64, response_limit: usize, identity: work.WorkIdentity) !readers.Result {
    if (!enabled) return error.AppleProviderUnavailable;
    var collector = Collector{
        .alloc = alloc,
        .response_limit = response_limit,
        .cancellation = opts.cancellation,
        .deadline_ns = deadline_ns,
    };
    defer collector.deinit();
    try collector.check();
    var resolved = input;
    resolved.context = &collector;
    resolved.should_cancel = Collector.shouldCancel;
    resolved.region = Collector.region;
    const status = antfly_vision_read(&resolved);
    try collector.check();
    if (collector.failure) |err| return err;
    switch (status) {
        0 => {},
        1 => return error.InvalidInferenceMedia,
        3 => return error.UnsupportedAppleOcrLanguage,
        4 => return error.Cancelled,
        5 => return error.AppleOcrBusy,
        6 => return error.InferenceDecodedPixelsExceeded,
        7 => return error.AppleProviderUnavailable,
        else => return error.AppleOcrFailed,
    }
    return collector.result(identity);
}

fn optionsJson(alloc: Allocator, cfg: readers.Config, prompt: ?[]const u8, max_tokens: ?i64) ![]u8 {
    try cfg.validate();
    try reader_config.validateAppleRequest(prompt, max_tokens);
    try checkAvailable();
    return std.json.Stringify.valueAlloc(alloc, .{
        .recognition_languages = cfg.recognition_languages,
        .recognition_level = cfg.recognition_level,
        .uses_language_correction = cfg.uses_language_correction,
    }, .{});
}

fn invocationDeadline(opts: readers.RemoteOptions) ?u64 {
    const ms = opts.timeout_ms orelse return null;
    if (ms == 0) return null;
    return platform.time.monotonicNs() +| (ms *| std.time.ns_per_ms);
}

fn resultBytes(item: readers.Result) usize {
    return item.text.len + (if (item.regions_json) |json| json.len else @as(usize, 0));
}

fn deinitPartial(alloc: Allocator, items: []readers.Result, filled: usize) void {
    for (items[0..filled]) |*item| readers.deinitResult(alloc, item);
    alloc.free(items);
}

fn serial(items: []readers.Result) readers.BatchResult {
    return .{ .items = items, .execution = .{ .requested_items = items.len, .serial_items = items.len } };
}

pub fn readEncoded(alloc: Allocator, cfg: readers.Config, request: readers.EncodedRequest, opts: readers.RemoteOptions) !readers.BatchResult {
    try readers.validateEncodedRequest(request);
    const json = try optionsJson(alloc, cfg, request.prompt, request.max_tokens);
    defer alloc.free(json);
    const caps = try capabilities();
    var bytes: usize = 0;
    var pixels: u64 = 0;
    for (request.images) |image| {
        try caps.validateMimeType(image.mime_type);
        bytes = std.math.add(usize, bytes, image.bytes.len) catch return error.InferenceEncodedBytesExceeded;
        const image_pixels = try work.encodedImagePixels(image.mime_type, image.bytes);
        if (image_pixels > max_pixels_per_image) return error.InferenceDecodedPixelsExceeded;
        pixels = std.math.add(u64, pixels, image_pixels) catch return error.InferenceDecodedPixelsExceeded;
    }
    try caps.validateInvocation(.read, .{
        .item_count = request.images.len,
        .modalities = .{ .image = true },
        .encoded_media_bytes = bytes,
        .decoded_pixels = pixels,
        .max_media_parts_per_item = 1,
    });
    const end = invocationDeadline(opts);
    const items = try alloc.alloc(readers.Result, request.images.len);
    var filled: usize = 0;
    errdefer deinitPartial(alloc, items, filled);
    var remaining = request.max_response_bytes orelse default_response_bytes;
    for (request.images, items) |image, *item| {
        item.* = try nativeRead(alloc, .{
            .bytes = image.bytes.ptr,
            .length = image.bytes.len,
            .options = json.ptr,
            .options_length = json.len,
            .context = undefined,
            .should_cancel = undefined,
            .region = undefined,
        }, opts, end, remaining, .{
            .item_id = image.item_id,
            .source_fingerprint = image.source_fingerprint orelse request.source_fingerprint,
            .page_number = image.page_number,
        });
        filled += 1;
        remaining -= resultBytes(item.*);
    }
    return serial(items);
}

pub fn readRasters(alloc: Allocator, cfg: readers.Config, request: readers.RasterRequest, opts: readers.RemoteOptions) !readers.BatchResult {
    try readers.validateRasterRequest(request);
    const json = try optionsJson(alloc, cfg, request.prompt, request.max_tokens);
    defer alloc.free(json);
    var pixels: u64 = 0;
    var bytes: usize = 0;
    for (request.images) |image| {
        const count = try image.pixels();
        if (count > max_pixels_per_image) return error.InferenceDecodedPixelsExceeded;
        pixels = std.math.add(u64, pixels, count) catch return error.InferenceDecodedPixelsExceeded;
        bytes = std.math.add(usize, bytes, image.bytes.len) catch return error.InferenceEncodedBytesExceeded;
    }
    const caps = try capabilities();
    try caps.validateInvocation(.read, .{
        .item_count = request.images.len,
        .modalities = .{ .image = true },
        .raw_media_bytes = bytes,
        .decoded_pixels = pixels,
        .max_media_parts_per_item = 1,
    });
    const end = invocationDeadline(opts);
    const items = try alloc.alloc(readers.Result, request.images.len);
    var filled: usize = 0;
    errdefer deinitPartial(alloc, items, filled);
    var remaining = request.max_response_bytes orelse default_response_bytes;
    for (request.images, items) |image, *item| {
        item.* = try nativeRead(alloc, .{
            .bytes = image.bytes.ptr,
            .length = image.bytes.len,
            .width = image.width,
            .height = image.height,
            .stride = image.stride_bytes,
            .options = json.ptr,
            .options_length = json.len,
            .context = undefined,
            .should_cancel = undefined,
            .region = undefined,
        }, opts, end, remaining, .{
            .item_id = image.item_id,
            .source_fingerprint = image.source_fingerprint orelse request.source_fingerprint,
            .page_number = image.page_number,
        });
        filled += 1;
        remaining -= resultBytes(item.*);
    }
    return serial(items);
}

pub fn read(alloc: Allocator, http: *httpx.Client, cfg: readers.Config, request: readers.Request, opts: readers.RemoteOptions) !readers.BatchResult {
    // Preflight before decoding/downloading any input, including disabled builds.
    const json = try optionsJson(alloc, cfg, request.prompt, request.max_tokens);
    defer alloc.free(json);
    if (request.images.len == 0 or request.images.len > max_images) return error.ReadBatchTooLarge;
    const owned = try alloc.alloc(scraping.DownloadedContent, request.images.len);
    var filled: usize = 0;
    defer {
        for (owned[0..filled]) |*content| content.deinit(alloc);
        alloc.free(owned);
    }
    const encoded = try alloc.alloc(readers.EncodedImage, request.images.len);
    defer alloc.free(encoded);
    var total: usize = 0;
    const end = invocationDeadline(opts);
    for (request.images, 0..) |url, i| {
        const remaining_ms: ?u64 = if (end) |ns| blk: {
            const now = platform.time.monotonicNs();
            if (now >= ns) return error.Timeout;
            break :blk @max(1, (ns - now) / std.time.ns_per_ms);
        } else null;
        const security = scraping.ContentSecurityConfig{
            .max_download_size_bytes = max_batch_bytes - total,
            .block_private_ips = true,
        };
        if (total == max_batch_bytes) return error.InferenceEncodedBytesExceeded;
        owned[i] = try scraping.downloadContentAllocWithContext(alloc, .{
            .io = http.io,
            .timeout_ms = remaining_ms,
            .cancellation = if (opts.cancellation) |token| scraping.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn) else null,
        }, url, &security, null);
        filled += 1;
        total = std.math.add(usize, total, owned[i].data.len) catch return error.InferenceEncodedBytesExceeded;
        encoded[i] = .{ .bytes = owned[i].data, .mime_type = owned[i].content_type, .source_fingerprint = request.source_fingerprint };
    }
    var resolved_opts = opts;
    if (end) |ns| {
        const now = platform.time.monotonicNs();
        if (now >= ns) return error.Timeout;
        resolved_opts.timeout_ms = @max(1, (ns - now) / std.time.ns_per_ms);
    }
    return readEncoded(alloc, cfg, .{
        .images = encoded,
        .prompt = request.prompt,
        .max_tokens = request.max_tokens,
        .source_fingerprint = request.source_fingerprint,
        .max_response_bytes = request.max_response_bytes,
    }, resolved_opts);
}

test "apple OCR collector rejects oversized output without truncation" {
    var collector = Collector{ .alloc = std.testing.allocator, .response_limit = 260, .cancellation = null, .deadline_ns = null };
    defer collector.deinit();
    try std.testing.expectError(error.ResponseTooLarge, collector.append("invoice", .{ 0, 0, 1, 1 }, 1));
    try std.testing.expectEqual(@as(usize, 0), collector.regions.items.len);
}

test "apple OCR disabled builds fail without calling native symbols" {
    if (enabled) return error.SkipZigTest;
    try std.testing.expectError(error.AppleProviderUnavailable, capabilities());
    try std.testing.expectError(error.AppleProviderUnavailable, readRasters(std.testing.allocator, .{ .provider = .apple }, .{
        .images = &.{.{ .bytes = &.{ 0, 0, 0, 255 }, .width = 1, .height = 1, .stride_bytes = 4 }},
    }, .{}));
}

test "apple OCR recognizes encoded and borrowed raster inputs with page identity" {
    if (!enabled) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    const png = @embedFile("testdata/apple-ocr.png");
    var batch = try readers.readEncodedWithConfigReported(alloc, undefined, .{ .provider = .apple }, .{
        .images = &.{.{ .bytes = png, .mime_type = "image/png", .item_id = "page-a", .source_fingerprint = "doc-a", .page_number = 3 }},
    }, .{});
    defer batch.deinit(alloc);
    try expectFixture(batch.items[0]);
    try std.testing.expectEqualStrings("page-a", batch.items[0].item_id);
    try std.testing.expectEqualStrings("doc-a", batch.items[0].source_fingerprint.?);
    try std.testing.expectEqual(@as(?u32, 3), batch.items[0].page_number);
    try std.testing.expectEqual(@as(usize, 1), batch.execution.serial_items);
    try std.testing.expectEqual(@as(usize, 0), batch.execution.native_items);

    const decoded = try @import("antfly_image").png.decodeRgba(alloc, png);
    defer alloc.free(decoded.rgba);
    var raster_batch = try readers.readRasterWithConfigReported(alloc, .{ .provider = .apple }, .{
        .images = &.{.{ .bytes = decoded.rgba, .width = decoded.width, .height = decoded.height, .stride_bytes = @as(usize, decoded.width) * 4, .item_id = "page-b", .source_fingerprint = "doc-b", .page_number = 4 }},
    }, .{});
    defer raster_batch.deinit(alloc);
    try expectFixture(raster_batch.items[0]);
    try std.testing.expectEqualStrings("page-b", raster_batch.items[0].item_id);
    try std.testing.expectEqualStrings("doc-b", raster_batch.items[0].source_fingerprint.?);
    try std.testing.expectEqual(@as(?u32, 4), raster_batch.items[0].page_number);
}

test "apple OCR registry reads bounded data URI images" {
    if (!enabled) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var io_impl = platform.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    var client = httpx.Client.initWithConfig(alloc, io_impl.io(), .{});
    defer client.deinit();
    const encoded = try alloc.alloc(u8, std.base64.standard.Encoder.calcSize(testing.fixture_png.len));
    defer alloc.free(encoded);
    _ = std.base64.standard.Encoder.encode(encoded, testing.fixture_png);
    const uri = try std.fmt.allocPrint(alloc, "data:image/png;base64,{s}", .{encoded});
    defer alloc.free(uri);
    var registry = readers.Registry.init(alloc);
    defer registry.deinit();
    try registry.registerConfig("apple", .{ .provider = .apple });
    var batch = try readers.readWithConfigReported(alloc, &client, try registry.getConfig("apple"), .{ .images = &.{uri} }, .{});
    defer batch.deinit(alloc);
    try expectFixture(batch.items[0]);
}

fn expectFixture(item: readers.Result) !void {
    try std.testing.expectEqualStrings("Antfly local OCR\nInvoice 12345\nTotal USD 42.00", item.text);
    const parsed = try std.json.parseFromSlice([]Region, std.testing.allocator, item.regions_json.?, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 3), parsed.value.len);
    for (parsed.value) |region_value| {
        try std.testing.expectEqualStrings("image_pixels_top_left", region_value.coordinate_space);
        try std.testing.expect(region_value.confidence >= 0 and region_value.confidence <= 1);
        try std.testing.expect(region_value.bbox[0] < region_value.bbox[2]);
        try std.testing.expect(region_value.bbox[1] < region_value.bbox[3]);
        try std.testing.expect(region_value.bbox[0] >= 0 and region_value.bbox[2] <= 1200);
        try std.testing.expect(region_value.bbox[1] >= 0 and region_value.bbox[3] <= 360);
    }
    try std.testing.expect(parsed.value[0].bbox[1] < parsed.value[1].bbox[1]);
}

test "apple OCR cancels before native execution and rejects unsupported languages" {
    if (!enabled) return error.SkipZigTest;
    const request = readers.EncodedRequest{ .images = &.{.{ .bytes = @embedFile("testdata/apple-ocr.png"), .mime_type = "image/png" }} };
    const cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Cancelled, readEncoded(std.testing.allocator, .{ .provider = .apple }, request, .{
        .cancellation = httpx.CancellationToken.fromAtomic(&cancelled),
    }));
    try std.testing.expectError(error.UnsupportedAppleOcrLanguage, readEncoded(std.testing.allocator, .{
        .provider = .apple,
        .recognition_languages = &.{"xx-XX"},
    }, request, .{}));
    var bounded = request;
    bounded.max_response_bytes = 1;
    try std.testing.expectError(error.ResponseTooLarge, readEncoded(std.testing.allocator, .{ .provider = .apple }, bounded, .{}));
}
