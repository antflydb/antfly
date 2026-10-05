// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Media compute has no dependency on the DB owner protocol. Its version and
//! request layouts evolve independently; only semantic failure identity is shared.
const failure = @import("runtime_failure_abi");
pub const abi_version: u32 = 2;
pub const Allocator = @import("runtime_memory_abi").Allocator;
pub const Status = failure.Status;
pub const FailureIdentity = failure.FailureIdentity;
pub const FailureBoundary = failure.FailureBoundary;

pub const BorrowedBytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: u64 = 0,

    pub fn fromSlice(value: []const u8) BorrowedBytes {
        return .{
            .ptr = if (value.len == 0) null else value.ptr,
            .len = @intCast(value.len),
        };
    }

    pub fn slice(self: BorrowedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..@intCast(self.len)];
    }
};

pub const OwnedBytes = extern struct {
    ptr: ?[*]u8 = null,
    len: u64 = 0,

    pub fn slice(self: OwnedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..@intCast(self.len)];
    }
};

pub const EnrichmentStreamBeginFn = *const fn (
    ?*anyopaque,
    BorrowedBytes,
    BorrowedBytes,
    BorrowedBytes,
) callconv(.c) Status;

pub const EnrichmentExtractRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    /// Borrowed for this synchronous call; all temporary decode allocations use it.
    allocator: ?*const Allocator = null,
    downloaded: BorrowedBytes = .{},
    downloaded_content_type: BorrowedBytes = .{},
    source_url: BorrowedBytes = .{},
    config_json: BorrowedBytes = .{},
    raw_document_json: BorrowedBytes = .{},
    callback_ctx: ?*anyopaque = null,
    on_begin: ?EnrichmentStreamBeginFn = null,
    on_units_json: ?*const fn (?*anyopaque, BorrowedBytes) callconv(.c) Status = null,
    max_decoded_stream_bytes: u64 = 64 * 1024 * 1024,
    max_working_set_bytes: u64 = 96 * 1024 * 1024,
};

pub const EnrichmentRenderPdfRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    allocator: ?*const Allocator = null,
    render_timeout_ms: u64 = 0,
    pdf_bytes: BorrowedBytes = .{},
    page_number: u64 = 1,
    dpi: u16 = 150,
    _reserved1: u16 = 0,
    max_dimension: u32 = 4096,
    max_pixels: u64 = 40_000_000,
    max_decoded_stream_bytes: u64 = 64 * 1024 * 1024,
    max_working_set_bytes: u64 = 96 * 1024 * 1024,
};

pub const EnrichmentRenderedPdfPage = extern struct {
    requested_dpi: u16 = 0,
    effective_dpi: u16 = 0,
    width: u32 = 0,
    height: u32 = 0,
    _reserved0: u32 = 0,
};

pub const EnrichmentOperation = enum(u32) {
    extract_stream = 1,
    render_pdf_page = 2,
    validate_extract_response = 3,
    validate_render_response = 4,
};

pub extern fn antfly_enrichment_extract_stream(
    request: *const EnrichmentExtractRequest,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_enrichment_render_pdf_page_png(
    request: *const EnrichmentRenderPdfRequest,
    out_png: *OwnedBytes,
    out_page: *EnrichmentRenderedPdfPage,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_enrichment_buffer_destroy(buffer: *OwnedBytes) callconv(.c) void;
