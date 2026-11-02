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

//! This consumer imports no PDF, image, storage or inference implementation.
const std = @import("std");
const client = @import("pdf_session_client");
const abi = @import("enrichment_compute_abi");

fn fixture(allocator: std.mem.Allocator) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(std.testing.io, "lib/pdf/testdata/simple_text_fixture.pdf", allocator, .limited(1024 * 1024));
}

test "opaque PDF session retains source and allocator across open frame and repeated rendering" {
    const allocator = std.testing.allocator;
    const bytes = try fixture(allocator);
    defer allocator.free(bytes);
    var session = try client.Session.init(allocator, bytes, .{});
    defer session.deinit();
    try std.testing.expect(session.page_count > 0);
    const first = try session.renderPng(allocator, .{ .dpi = 72, .max_dimension = 512 });
    defer allocator.free(first.png);
    const second = try session.renderPng(allocator, .{ .dpi = 72, .max_dimension = 512 });
    defer allocator.free(second.png);
    try std.testing.expectEqualSlices(u8, first.png, second.png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, first.png[0..8]);
    try std.testing.expect(first.width <= 512 and first.height <= 512);
    try std.testing.expectError(error.InvalidAbiVersion, session.renderPng(allocator, .{ .version = abi.abi_version - 1 }));
    // Failed operations leave the retained reader usable.
    const third = try session.renderPng(allocator, .{ .dpi = 72, .max_dimension = 512 });
    defer allocator.free(third.png);
    try std.testing.expectEqualSlices(u8, first.png, third.png);
}

test "opaque PDF session releases partial parse and allocation failures" {
    var failure: abi.FailureIdentity = .{};
    try std.testing.expectError(error.StorageKernelFailure, client.Session.initWithFailure(std.testing.allocator, "not a pdf", .{}, &failure));
    try std.testing.expectEqualStrings("InvalidPdfHeader", failure.errorName());
    try std.testing.expectEqual(abi.FailureBoundary.enrichment_compute, failure.boundary);
    const bytes = try fixture(std.testing.allocator);
    defer std.testing.allocator.free(bytes);
    for (0..3) |index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, client.Session.init(failing.allocator(), bytes, .{}));
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}

test "opaque PDF session preflight clears output and close is null safe" {
    var handle: ?*abi.PdfSession = null;
    var count: u64 = 42;
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_enrichment_pdf_session_open(&.{ .version = abi.abi_version - 1 }, &handle, &count, &failure);
    try std.testing.expectEqual(abi.Status.invalid_abi, status);
    try std.testing.expect(handle == null);
    try std.testing.expectEqual(@as(u64, 0), count);
    try std.testing.expectEqual(abi.FailureBoundary.enrichment_compute, failure.boundary);
    try std.testing.expectEqual(@backingInt(abi.EnrichmentOperation.pdf_session_open), failure.operation);
    abi.antfly_enrichment_pdf_session_close(&handle);
    abi.antfly_enrichment_pdf_session_close(&handle);
}

fn canceled(_: ?*const anyopaque) callconv(.c) u8 {
    return 1;
}

test "opaque PDF session bounds cancellation and charges retained PNG to caller" {
    const allocator = std.testing.allocator;
    const bytes = try fixture(allocator);
    defer allocator.free(bytes);
    try std.testing.expectError(error.Canceled, client.Session.init(allocator, bytes, .{
        .cancellation = .{ .is_cancelled = canceled },
    }));
    var session = try client.Session.init(allocator, bytes, .{});
    defer session.deinit();
    try std.testing.expectError(error.Canceled, session.renderPng(allocator, .{
        .cancellation = .{ .is_cancelled = canceled },
    }));
    var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, session.renderPng(failing.allocator(), .{}));
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    // Borrowed cancellation/output-allocator frames did not escape. Recovery
    // allocates retained output in the caller's allocator and needs no copy.
    const rendered = try session.renderPng(allocator, .{ .dpi = 72, .max_dimension = 512 });
    defer allocator.free(rendered.png);
    try std.testing.expect(rendered.png.len > 8);
}
