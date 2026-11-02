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

//! Media compute has no dependency on the DB owner protocol. Its version and
//! request layouts evolve independently; only semantic failure identity is shared.
const failure = @import("runtime_failure_abi");
pub const abi_version: u32 = 3;
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

/// Process-local opaque PDF state. The input bytes and allocator callback
/// context are borrowed through close; neither may refer to an expired stack
/// frame. The provider copies the allocator table into stable session storage.
/// Calls on one session are exclusive; separate sessions can run concurrently.
pub const PdfSession = opaque {};

/// Borrowed for one synchronous operation, never retained between calls.
pub const PdfCancellation = extern struct {
    context: ?*const anyopaque = null,
    is_cancelled: ?*const fn (?*const anyopaque) callconv(.c) u8 = null,
};

pub const PdfSessionOpenRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    allocator: ?*const Allocator = null,
    pdf_bytes: BorrowedBytes = .{},
    timeout_ms: u64 = 0,
    cancellation: PdfCancellation = .{},
    max_decoded_stream_bytes: u64 = 64 * 1024 * 1024,
    max_working_set_bytes: u64 = 96 * 1024 * 1024,
};

pub const PdfSessionRenderRequest = extern struct {
    version: u32 = abi_version,
    _reserved0: u32 = 0,
    /// Borrowed only during this render call. When supplied, the PNG is
    /// allocated from this allocator and the caller must release it there.
    /// Null preserves the provider buffer/destroy convention for one-shot calls.
    output_allocator: ?*const Allocator = null,
    timeout_ms: u64 = 0,
    cancellation: PdfCancellation = .{},
    page_number: u64 = 1,
    dpi: u16 = 150,
    _reserved1: u16 = 0,
    max_dimension: u32 = 4096,
    max_pixels: u64 = 40_000_000,
};

pub extern fn antfly_enrichment_pdf_session_open(
    request: *const PdfSessionOpenRequest,
    out_session: *?*PdfSession,
    out_page_count: *u64,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

/// The request selects PNG output ownership. Session allocations remain
/// charged to the open allocator until close; render output is charged to the
/// supplied output allocator until the caller releases it.
pub extern fn antfly_enrichment_pdf_session_render_png(
    session: *PdfSession,
    request: *const PdfSessionRenderRequest,
    out_png: *OwnedBytes,
    out_page: *EnrichmentRenderedPdfPage,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

/// Null is an empty session; close clears the caller's handle before returning.
pub extern fn antfly_enrichment_pdf_session_close(session: *?*PdfSession) callconv(.c) void;

pub const EnrichmentOperation = enum(u32) {
    extract_stream = 1,
    render_pdf_page = 2,
    validate_extract_response = 3,
    validate_render_response = 4,
    pdf_session_open = 5,
    pdf_session_render = 6,
    validate_pdf_session_response = 7,
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
