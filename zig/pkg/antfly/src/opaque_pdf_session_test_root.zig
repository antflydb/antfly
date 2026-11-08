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

test "opaque PDF backend preserves exact plain text and first-page PNG" {
    const backend = @import("pdf_backend_client");
    const allocator = std.testing.allocator;
    const bytes = try fixture(allocator);
    defer allocator.free(bytes);
    const text = switch (try backend.compute(allocator, bytes, .extract_text)) {
        .bytes => |value| value,
        .failure => return error.UnexpectedPdfFailure,
    };
    defer allocator.free(text);
    try std.testing.expectEqualStrings("Hello Fixture\n", text);
    const png = switch (try backend.compute(allocator, bytes, .render_first_page_png)) {
        .bytes => |value| value,
        .failure => return error.UnexpectedPdfFailure,
    };
    defer allocator.free(png);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, png[0..8]);
}

test "opaque PDF backend keeps provider diagnostics and allocation admission" {
    const backend = @import("pdf_backend_client");
    const broken = try backend.compute(std.testing.allocator, "not a PDF", .extract_text);
    switch (broken) {
        .bytes => |bytes| {
            std.testing.allocator.free(bytes);
            return error.ExpectedPdfFailure;
        },
        .failure => |failure| {
            try std.testing.expectEqualStrings("InvalidPdfHeader", failure.errorName());
            try std.testing.expectEqual(abi.FailureBoundary.enrichment_compute, failure.boundary);
        },
    }
    const bytes = try fixture(std.testing.allocator);
    defer std.testing.allocator.free(bytes);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    switch (try backend.compute(failing.allocator(), bytes, .extract_text)) {
        .bytes => |result| {
            failing.allocator().free(result);
            return error.ExpectedAllocationFailure;
        },
        .failure => |failure| try std.testing.expectEqual(abi.Status.out_of_memory, failure.status),
    }
    try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
}

const ResizingTracker = struct {
    backing: std.mem.Allocator,
    live_bytes: usize = 0,
    resize_calls: usize = 0,
    remap_calls: usize = 0,
    successful_resizes: usize = 0,
    successful_remaps: usize = 0,

    fn allocator(self: *@This()) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &vtable };
    }
    const vtable: std.mem.Allocator.VTable = .{ .alloc = allocate, .resize = resize, .remap = remap, .free = release };
    fn allocate(context: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(context));
        const ptr = self.backing.rawAlloc(len, alignment, ra) orelse return null;
        self.live_bytes += len;
        return ptr;
    }
    fn resize(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.resize_calls += 1;
        if (!self.backing.rawResize(bytes, alignment, len, ra)) return false;
        self.successful_resizes += 1;
        self.live_bytes = self.live_bytes - bytes.len + len;
        return true;
    }
    fn remap(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.remap_calls += 1;
        const ptr = self.backing.rawRemap(bytes, alignment, len, ra) orelse return null;
        self.successful_remaps += 1;
        self.live_bytes = self.live_bytes - bytes.len + len;
        return ptr;
    }
    fn release(context: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.live_bytes -= bytes.len;
        self.backing.rawFree(bytes, alignment, ra);
    }
};

test "opaque PDF backend preserves resizing and retained output charging" {
    const backend = @import("pdf_backend_client");
    const bytes = try fixture(std.testing.allocator);
    defer std.testing.allocator.free(bytes);
    const storage = try std.testing.allocator.alloc(u8, 4 * 1024 * 1024);
    defer std.testing.allocator.free(storage);
    var arena = std.heap.FixedBufferAllocator.init(storage);
    var tracker = ResizingTracker{ .backing = arena.allocator() };
    const allocator = tracker.allocator();
    const text = switch (try backend.compute(allocator, bytes, .extract_text)) {
        .bytes => |value| value,
        .failure => return error.UnexpectedPdfFailure,
    };
    try std.testing.expectEqualStrings("Hello Fixture\n", text);
    try std.testing.expectEqual(text.len, tracker.live_bytes);
    try std.testing.expect(tracker.resize_calls + tracker.remap_calls > 0);
    try std.testing.expect(tracker.successful_resizes + tracker.successful_remaps > 0);
    allocator.free(text);
    try std.testing.expectEqual(@as(usize, 0), tracker.live_bytes);
}
