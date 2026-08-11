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

//! Contract-only consumer for the separately compiled media compute unit.

const std = @import("std");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");
const extraction = @import("document_extraction.zig");

const Allocator = std.mem.Allocator;

const Context = struct {
    alloc: Allocator,
    sink: extraction.UnitSink,
    error_relay: error_identity.CallbackErrorRelay = .{},

    fn onBegin(
        ptr: ?*anyopaque,
        content_type: abi.BorrowedBytes,
        route_type: abi.BorrowedBytes,
        unsupported_reason: abi.BorrowedBytes,
    ) callconv(.c) abi.Status {
        const self: *@This() = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        self.sink.on_begin(self.sink.ptr, .{
            .content_type = content_type.slice(),
            .route_type = route_type.slice(),
            .unsupported_reason = unsupported_reason.slice(),
        }) catch |err| return self.error_relay.capture(err);
        return .ok;
    }

    fn onUnitsJson(ptr: ?*anyopaque, encoded: abi.BorrowedBytes) callconv(.c) abi.Status {
        const self: *@This() = @ptrCast(@alignCast(ptr orelse return .invalid_argument));
        var parsed = std.json.parseFromSlice([]extraction.Unit, self.alloc, encoded.slice(), .{
            .allocate = .alloc_always,
            .ignore_unknown_fields = false,
        }) catch |err| return self.error_relay.capture(err);
        defer parsed.deinit();
        for (parsed.value) |unit| {
            var owned = cloneUnit(self.alloc, unit) catch |err| return self.error_relay.capture(err);
            defer owned.deinit(self.alloc);
            self.sink.on_unit(self.sink.ptr, &owned) catch |err| return self.error_relay.capture(err);
        }
        return .ok;
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

const ResultCollector = struct {
    alloc: Allocator,
    content_type: ?[]u8 = null,
    route_type: ?[]u8 = null,
    unsupported_reason: ?[]u8 = null,
    units: std.ArrayListUnmanaged(extraction.Unit) = .empty,

    fn sink(self: *@This()) extraction.UnitSink {
        return .{
            .ptr = self,
            .on_begin = onBegin,
            .on_unit = onUnit,
            .on_end = onEnd,
        };
    }

    fn deinit(self: *@This()) void {
        if (self.content_type) |value| self.alloc.free(value);
        if (self.route_type) |value| self.alloc.free(value);
        if (self.unsupported_reason) |value| self.alloc.free(value);
        for (self.units.items) |*unit| unit.deinit(self.alloc);
        self.units.deinit(self.alloc);
    }

    fn onBegin(ptr: *anyopaque, info: extraction.StreamInfo) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        if (self.content_type != null or self.route_type != null) return error.InvalidDocumentExtractionState;
        self.content_type = try self.alloc.dupe(u8, info.content_type);
        errdefer {
            self.alloc.free(self.content_type.?);
            self.content_type = null;
        }
        self.route_type = try self.alloc.dupe(u8, info.route_type);
        errdefer {
            self.alloc.free(self.route_type.?);
            self.route_type = null;
        }
        if (info.unsupported_reason.len > 0) {
            self.unsupported_reason = try self.alloc.dupe(u8, info.unsupported_reason);
        }
    }

    fn onUnit(ptr: *anyopaque, unit: *extraction.Unit) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        const owned = try cloneUnit(self.alloc, unit.*);
        errdefer {
            var doomed = owned;
            doomed.deinit(self.alloc);
        }
        try self.units.append(self.alloc, owned);
    }

    fn onEnd(_: *anyopaque) anyerror!void {}

    fn finish(self: *@This()) !extraction.Result {
        const content_type = self.content_type orelse return error.InvalidDocumentExtractionState;
        const route_type = self.route_type orelse return error.InvalidDocumentExtractionState;
        const units = try self.units.toOwnedSlice(self.alloc);
        self.content_type = null;
        self.route_type = null;
        const unsupported_reason: []u8 = self.unsupported_reason orelse &.{};
        self.unsupported_reason = null;
        return .{
            .content_type = content_type,
            .route_type = route_type,
            .unsupported_reason = unsupported_reason,
            .units = units,
        };
    }
};

pub fn extractDownloadedStreaming(
    alloc: Allocator,
    downloaded: anytype,
    source_url: []const u8,
    config_json: []const u8,
    raw_document_json: []const u8,
    sink: extraction.UnitSink,
) !void {
    var failure: abi.FailureIdentity = .{};
    return extractDownloadedStreamingWithFailure(
        alloc,
        downloaded,
        source_url,
        config_json,
        raw_document_json,
        sink,
        &failure,
    );
}

pub fn extractDownloadedStreamingWithFailure(
    alloc: Allocator,
    downloaded: anytype,
    source_url: []const u8,
    config_json: []const u8,
    raw_document_json: []const u8,
    sink: extraction.UnitSink,
    out_failure: *abi.FailureIdentity,
) !void {
    out_failure.* = .{};
    var context = Context{ .alloc = alloc, .sink = sink };
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_enrichment_extract_stream(&.{
        .downloaded = .fromSlice(downloaded.data),
        .downloaded_content_type = .fromSlice(downloaded.content_type),
        .source_url = .fromSlice(source_url),
        .config_json = .fromSlice(config_json),
        .raw_document_json = .fromSlice(raw_document_json),
        .callback_ctx = &context,
        .on_begin = Context.onBegin,
        .on_units_json = Context.onUnitsJson,
    }, &failure);
    out_failure.* = failure;
    context.error_relay.finish(status) catch |err| {
        // Callback failures belong to the consumer and retain their exact Zig
        // identity through CallbackErrorRelay. Do not mislabel the provider's
        // unwind sentinel as the originating failure.
        if (context.error_relay.exact_error != null) out_failure.* = .{};
        if (status == .internal and failure.error_name_len != 0) {
            std.log.err("enrichment compute failed operation={d} provider_error={s} hash={x}", .{
                failure.operation,
                failure.errorName(),
                failure.error_name_hash,
            });
        }
        return err;
    };
    try sink.on_end(sink.ptr);
}

pub fn extractDownloadedAlloc(
    alloc: Allocator,
    downloaded: anytype,
    source_url: []const u8,
    config_json: []const u8,
    raw_document_json: []const u8,
) !extraction.Result {
    var failure: abi.FailureIdentity = .{};
    return extractDownloadedAllocWithFailure(
        alloc,
        downloaded,
        source_url,
        config_json,
        raw_document_json,
        &failure,
    );
}

pub fn extractDownloadedAllocWithFailure(
    alloc: Allocator,
    downloaded: anytype,
    source_url: []const u8,
    config_json: []const u8,
    raw_document_json: []const u8,
    out_failure: *abi.FailureIdentity,
) !extraction.Result {
    var collector = ResultCollector{ .alloc = alloc };
    defer collector.deinit();
    try extractDownloadedStreamingWithFailure(
        alloc,
        downloaded,
        source_url,
        config_json,
        raw_document_json,
        collector.sink(),
        out_failure,
    );
    return try collector.finish();
}

pub fn renderPdfPagePngAlloc(alloc: Allocator, pdf_bytes: []const u8, page_number: usize) ![]u8 {
    var failure: abi.FailureIdentity = .{};
    return renderPdfPagePngAllocWithFailure(alloc, pdf_bytes, page_number, &failure);
}

pub fn renderPdfPagePngAllocWithFailure(
    alloc: Allocator,
    pdf_bytes: []const u8,
    page_number: usize,
    out_failure: *abi.FailureIdentity,
) ![]u8 {
    out_failure.* = .{};
    var provider_png: abi.OwnedBytes = .{};
    defer abi.antfly_enrichment_buffer_destroy(&provider_png);
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_enrichment_render_pdf_page_png(&.{
        .pdf_bytes = .fromSlice(pdf_bytes),
        .page_number = @intCast(page_number),
    }, &provider_png, &failure);
    out_failure.* = failure;
    error_identity.statusToError(status) catch |err| {
        if (status == .internal and failure.error_name_len != 0) {
            std.log.err("enrichment PDF render failed provider_error={s} hash={x}", .{
                failure.errorName(),
                failure.error_name_hash,
            });
        }
        return err;
    };
    return try alloc.dupe(u8, provider_png.slice());
}
