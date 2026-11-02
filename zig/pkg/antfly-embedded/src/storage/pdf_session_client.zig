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

//! Source-free client for the single compiled PDF owner. Source bytes are
//! borrowed through deinit. Only the allocator context and opaque handle live
//! here; Reader state and its allocator/deadline adapters live in the provider.
const std = @import("std");
const abi = @import("enrichment_compute_abi");
const identity = @import("kernel_error_identity");

pub const Options = struct {
    timeout_ms: u64 = 0,
    cancellation: abi.PdfCancellation = .{},
    max_decoded_stream_bytes: u64 = 64 * 1024 * 1024,
    max_working_set_bytes: u64 = 96 * 1024 * 1024,
};

pub const Session = struct {
    const Lease = struct { allocator: std.mem.Allocator };
    lease: *Lease,
    handle: ?*abi.PdfSession,
    page_count: usize,

    pub fn init(allocator: std.mem.Allocator, bytes: []const u8, options: Options) !Session {
        var failure: abi.FailureIdentity = .{};
        return initWithFailure(allocator, bytes, options, &failure);
    }

    pub fn initWithFailure(allocator: std.mem.Allocator, bytes: []const u8, options: Options, out_failure: *abi.FailureIdentity) !Session {
        out_failure.* = .{};
        const lease = try allocator.create(Lease);
        errdefer allocator.destroy(lease);
        lease.* = .{ .allocator = allocator };
        const table = abi.Allocator.fromStd(&lease.allocator);
        var handle: ?*abi.PdfSession = null;
        errdefer abi.antfly_enrichment_pdf_session_close(&handle);
        var page_count: u64 = 0;
        const status = abi.antfly_enrichment_pdf_session_open(&.{
            .allocator = &table,
            .pdf_bytes = .fromSlice(bytes),
            .timeout_ms = options.timeout_ms,
            .cancellation = options.cancellation,
            .max_decoded_stream_bytes = options.max_decoded_stream_bytes,
            .max_working_set_bytes = options.max_working_set_bytes,
        }, &handle, &page_count, out_failure);
        try accept(status, out_failure.*);
        if (handle == null) return error.InvalidBoundaryFailureIdentity;
        return .{
            .lease = lease,
            .handle = handle,
            .page_count = std.math.cast(usize, page_count) orelse return error.InvalidBoundaryFailureIdentity,
        };
    }

    pub fn deinit(self: *Session) void {
        abi.antfly_enrichment_pdf_session_close(&self.handle);
        const allocator = self.lease.allocator;
        allocator.destroy(self.lease);
        self.* = undefined;
    }

    pub const Rendered = struct {
        png: []u8,
        requested_dpi: u16,
        effective_dpi: u16,
        width: u32,
        height: u32,
    };

    pub fn renderPng(self: *Session, allocator: std.mem.Allocator, request: abi.PdfSessionRenderRequest) !Rendered {
        var png: abi.OwnedBytes = .{};
        var delivered = false;
        defer if (!delivered) {
            if (png.ptr) |ptr| allocator.free(ptr[0..@intCast(png.len)]);
        };
        const table = abi.Allocator.fromStd(&allocator);
        var owned_request = request;
        owned_request.output_allocator = &table;
        var page: abi.EnrichmentRenderedPdfPage = .{};
        var failure: abi.FailureIdentity = .{};
        const status = abi.antfly_enrichment_pdf_session_render_png(self.handle.?, &owned_request, &png, &page, &failure);
        try accept(status, failure);
        if (png.len == 0 or png.ptr == null or page.width == 0 or page.height == 0)
            return error.InvalidBoundaryFailureIdentity;
        delivered = true;
        return .{
            .png = png.ptr.?[0..@intCast(png.len)],
            .requested_dpi = page.requested_dpi,
            .effective_dpi = page.effective_dpi,
            .width = page.width,
            .height = page.height,
        };
    }
};

fn accept(status: abi.Status, failure: abi.FailureIdentity) !void {
    try identity.validateFailureEnvelope(status, &failure, abi.abi_version);
    if (status != .ok and failure.boundary != .enrichment_compute)
        return error.InvalidBoundaryFailureIdentity;
    try identity.statusToError(status);
}
