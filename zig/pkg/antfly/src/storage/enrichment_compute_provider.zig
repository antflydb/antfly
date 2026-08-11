// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Physical document/media compute owner. This unit has no storage handle: it
//! receives one bounded source and returns batched units to storage-owned
//! orchestration.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const extraction = @import("db/enrichment/document_extraction.zig");

const Allocator = std.mem.Allocator;
const max_batch_units: usize = 64;
const max_batch_bytes: usize = 4 * 1024 * 1024;

const Downloaded = struct {
    data: []const u8,
    content_type: []const u8,
};

const Stream = struct {
    alloc: Allocator,
    request: *const abi.EnrichmentExtractRequest,
    units: std.ArrayListUnmanaged(extraction.Unit) = .empty,
    unit_bytes: usize = 0,

    fn sink(self: *@This()) extraction.UnitSink {
        return .{
            .ptr = self,
            .on_begin = onBegin,
            .on_unit = onUnit,
            .on_end = onEnd,
        };
    }

    fn deinit(self: *@This()) void {
        self.clear();
        self.units.deinit(self.alloc);
    }

    fn clear(self: *@This()) void {
        for (self.units.items) |*unit| unit.deinit(self.alloc);
        self.units.clearRetainingCapacity();
        self.unit_bytes = 0;
    }

    fn onBegin(ptr: *anyopaque, info: extraction.StreamInfo) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const callback = self.request.on_begin orelse return error.InvalidArgument;
        try error_identity.statusToError(callback(
            self.request.callback_ctx,
            .fromSlice(info.content_type),
            .fromSlice(info.route_type),
            .fromSlice(info.unsupported_reason),
        ));
    }

    fn onUnit(ptr: *anyopaque, unit: *extraction.Unit) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var cloned = try cloneUnit(self.alloc, unit.*);
        var owns_cloned = true;
        errdefer if (owns_cloned) cloned.deinit(self.alloc);
        try self.units.append(self.alloc, cloned);
        owns_cloned = false;
        self.unit_bytes +|= unitBytes(unit.*);
        if (self.units.items.len >= max_batch_units or self.unit_bytes >= max_batch_bytes) try self.flush();
    }

    fn onEnd(ptr: *anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        try self.flush();
    }

    fn flush(self: *@This()) !void {
        if (self.units.items.len == 0) return;
        const callback = self.request.on_units_json orelse return error.InvalidArgument;
        const encoded = try std.json.Stringify.valueAlloc(self.alloc, self.units.items, .{});
        defer self.alloc.free(encoded);
        try error_identity.statusToError(callback(self.request.callback_ctx, .fromSlice(encoded)));
        self.clear();
    }
};

fn dupeOptional(alloc: Allocator, value: ?[]u8) !?[]u8 {
    return if (value) |bytes| try alloc.dupe(u8, bytes) else null;
}

fn cloneUnit(alloc: Allocator, unit: extraction.Unit) !extraction.Unit {
    var out = extraction.Unit{
        .unit_id = try alloc.dupe(u8, unit.unit_id),
        .unit_type = undefined,
        .text = undefined,
        .method = undefined,
    };
    errdefer alloc.free(out.unit_id);
    out.unit_type = try alloc.dupe(u8, unit.unit_type);
    errdefer alloc.free(out.unit_type);
    out.text = try alloc.dupe(u8, unit.text);
    errdefer alloc.free(out.text);
    out.method = try alloc.dupe(u8, unit.method);
    errdefer alloc.free(out.method);
    out.source_path = try dupeOptional(alloc, unit.source_path);
    errdefer if (out.source_path) |value| alloc.free(value);
    out.extraction_status = try dupeOptional(alloc, unit.extraction_status);
    errdefer if (out.extraction_status) |value| alloc.free(value);
    out.source_sha256 = try dupeOptional(alloc, unit.source_sha256);
    errdefer if (out.source_sha256) |value| alloc.free(value);
    out.extraction_warning = try dupeOptional(alloc, unit.extraction_warning);
    errdefer if (out.extraction_warning) |value| alloc.free(value);
    out.page_label = try dupeOptional(alloc, unit.page_label);
    errdefer if (out.page_label) |value| alloc.free(value);
    out.text_regions = if (unit.text_regions.len == 0) &.{} else try alloc.dupe(extraction.TextRegion, unit.text_regions);
    out.byte_length = unit.byte_length;
    out.ocr_used = unit.ocr_used;
    out.ocr_confidence = unit.ocr_confidence;
    out.ocr_bbox = unit.ocr_bbox;
    out.transcript_used = unit.transcript_used;
    out.transcript_confidence = unit.transcript_confidence;
    out.page_number = unit.page_number;
    out.page_bbox = unit.page_bbox;
    out.page_rotation = unit.page_rotation;
    out.char_start = unit.char_start;
    out.char_end = unit.char_end;
    return out;
}

fn unitBytes(unit: extraction.Unit) usize {
    return unit.unit_id.len + unit.unit_type.len + unit.text.len + unit.method.len +
        (if (unit.source_path) |v| v.len else 0) +
        (if (unit.extraction_status) |v| v.len else 0) +
        (if (unit.source_sha256) |v| v.len else 0) +
        (if (unit.extraction_warning) |v| v.len else 0) +
        (if (unit.page_label) |v| v.len else 0) +
        unit.text_regions.len * @sizeOf(extraction.TextRegion);
}

pub fn extractStream(
    request: *const abi.EnrichmentExtractRequest,
    out_failure: *abi.FailureIdentity,
) callconv(.c) abi.Status {
    out_failure.* = .{};
    if (request.version != abi.abi_version)
        return fail(error.InvalidAbiVersion, .extract_stream, out_failure);
    if (request.on_begin == null or request.on_units_json == null)
        return fail(error.InvalidArgument, .extract_stream, out_failure);
    const alloc = std.heap.c_allocator;
    var config = extraction.parseConfig(alloc, request.config_json.slice()) catch |err| {
        return fail(err, .extract_stream, out_failure);
    };
    defer config.deinit(alloc);
    extraction.applySourceMetadataFromJson(alloc, &config, request.raw_document_json.slice()) catch |err| {
        return fail(err, .extract_stream, out_failure);
    };
    var stream = Stream{ .alloc = alloc, .request = request };
    defer stream.deinit();
    extraction.extractDownloadedStreaming(alloc, Downloaded{
        .data = request.downloaded.slice(),
        .content_type = request.downloaded_content_type.slice(),
    }, request.source_url.slice(), config, stream.sink()) catch |err| {
        return fail(err, .extract_stream, out_failure);
    };
    return .ok;
}

pub fn renderPdfPagePng(
    request: *const abi.EnrichmentRenderPdfRequest,
    out_png: *abi.OwnedBytes,
    out_failure: *abi.FailureIdentity,
) callconv(.c) abi.Status {
    out_png.* = .{};
    out_failure.* = .{};
    if (request.version != abi.abi_version)
        return fail(error.InvalidAbiVersion, .render_pdf_page, out_failure);
    const page_number = std.math.cast(usize, request.page_number) orelse
        return fail(error.InvalidArgument, .render_pdf_page, out_failure);
    const png = extraction.renderPdfPagePngAlloc(std.heap.c_allocator, request.pdf_bytes.slice(), page_number) catch |err| {
        return fail(err, .render_pdf_page, out_failure);
    };
    out_png.* = .{ .ptr = png.ptr, .len = @intCast(png.len) };
    return .ok;
}

fn fail(
    err: anyerror,
    operation: abi.EnrichmentOperation,
    out_failure: *abi.FailureIdentity,
) abi.Status {
    out_failure.* = error_identity.failureFromError(
        err,
        .enrichment_compute,
        abi.abi_version,
        @intFromEnum(operation),
    );
    return out_failure.status;
}

pub fn bufferDestroy(buffer: *abi.OwnedBytes) callconv(.c) void {
    if (buffer.ptr) |ptr| std.heap.c_allocator.free(ptr[0..@intCast(buffer.len)]);
    buffer.* = .{};
}
