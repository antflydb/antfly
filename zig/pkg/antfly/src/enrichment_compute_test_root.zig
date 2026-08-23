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

//! Cross-archive contract tests for the separately compiled enrichment unit.

const std = @import("std");
const client = @import("storage/db/enrichment/document_extraction_client.zig");
const extraction = @import("storage/db/enrichment/document_extraction.zig");
const abi = @import("kernel_owner_abi");
const error_identity = @import("kernel_error_identity");

const Downloaded = struct {
    data: []const u8,
    content_type: []const u8,
};

test "enrichment compute boundary identity relay preserves origin and attributes protocol defects to consumer" {
    const failure = error_identity.failureFromError(
        error.InvalidPdfHeader,
        .enrichment_compute,
        abi.abi_version,
        @intFromEnum(abi.EnrichmentOperation.render_pdf_page),
    );
    var forwarded: abi.FailureIdentity = .{};
    try client.acceptProviderFailure(
        failure.status,
        failure,
        .validate_render_response,
        &forwarded,
    );
    try std.testing.expectEqualDeep(failure, forwarded);

    var malformed = failure;
    malformed.operation = 0;
    var replacement: abi.FailureIdentity = .{};
    try std.testing.expectError(
        error.InvalidBoundaryFailureIdentity,
        client.acceptProviderFailure(
            malformed.status,
            malformed,
            .validate_render_response,
            &replacement,
        ),
    );
    try std.testing.expectEqual(abi.Status.invalid_boundary_failure_identity, replacement.status);
    try std.testing.expectEqual(abi.FailureBoundary.storage_owner, replacement.boundary);
    try std.testing.expectEqual(abi.abi_version, replacement.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.EnrichmentOperation.validate_render_response),
        replacement.operation,
    );
    try std.testing.expectEqualStrings("InvalidBoundaryFailureIdentity", replacement.errorName());
}

test "enrichment compute boundary returns an owned extraction result" {
    const alloc = std.testing.allocator;
    var result = try client.extractDownloadedAlloc(
        alloc,
        Downloaded{ .data = "alpha beta", .content_type = "text/plain" },
        "https://example.test/readme.txt",
        "{}",
        "{}",
    );
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings("text/plain", result.content_type);
    try std.testing.expectEqualStrings("text", result.route_type);
    try std.testing.expectEqual(@as(usize, 1), result.units.len);
    try std.testing.expectEqualStrings("alpha beta", result.units[0].text);
}

test "enrichment compute boundary preserves PDF units regions and rendered bytes" {
    const alloc = std.testing.allocator;
    const pdf_fixture = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        "lib/pdf/testdata/simple_text_fixture.pdf",
        alloc,
        .limited(1024 * 1024),
    );
    defer alloc.free(pdf_fixture);
    var result = try client.extractDownloadedAlloc(
        alloc,
        Downloaded{ .data = pdf_fixture, .content_type = "application/pdf" },
        "https://example.test/simple.pdf",
        "{}",
        "{}",
    );
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings("pdf", result.route_type);
    try std.testing.expect(result.units.len > 0);
    try std.testing.expect(result.units[0].text.len > 0);
    try std.testing.expectEqual(@as(?u32, 1), result.units[0].page_number);
    try std.testing.expect(result.units[0].text_regions.len > 0);

    var rendered = try client.renderPdfPagePngAdaptiveAlloc(
        alloc,
        pdf_fixture,
        1,
        150,
        40_000_000,
        4096,
        64 * 1024 * 1024,
        96 * 1024 * 1024,
    );
    defer alloc.free(rendered.png);
    try std.testing.expect(rendered.png.len > 8);
    try std.testing.expectEqualSlices(u8, &.{ 0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n' }, rendered.png[0..8]);
}

test "enrichment compute boundary preserves semantic provider error identity" {
    var failure: abi.FailureIdentity = .{};
    try std.testing.expectError(error.InvalidDocumentExtractionConfig, client.extractDownloadedAllocWithFailure(
        std.testing.allocator,
        Downloaded{ .data = "alpha beta", .content_type = "text/plain" },
        "https://example.test/readme.txt",
        "[]",
        "{}",
        &failure,
    ));
    try std.testing.expectEqual(abi.Status.invalid_document_extraction_config, failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.enrichment_compute, failure.boundary);
    try std.testing.expectEqualStrings("InvalidDocumentExtractionConfig", failure.errorName());
    try std.testing.expectEqual(abi.abi_version, failure.boundary_version);
    try std.testing.expectEqual(@as(u32, 1), failure.operation);
}

test "enrichment compute boundary preflight failures carry a complete identity" {
    var request = abi.EnrichmentExtractRequest{};
    request.version = abi.abi_version - 1;
    var failure: abi.FailureIdentity = .{};
    const status = abi.antfly_enrichment_extract_stream(&request, &failure);
    try std.testing.expectEqual(abi.Status.invalid_abi, status);
    try std.testing.expectEqual(status, failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.enrichment_compute, failure.boundary);
    try std.testing.expectEqual(abi.abi_version, failure.boundary_version);
    try std.testing.expectEqual(
        @intFromEnum(abi.EnrichmentOperation.extract_stream),
        failure.operation,
    );
    try std.testing.expectEqualStrings("InvalidAbiVersion", failure.errorName());
    try std.testing.expect(failure.error_name_hash != 0);
}

test "enrichment compute boundary retains undeclared provider diagnostic identity" {
    var failure: abi.FailureIdentity = .{};
    try std.testing.expectError(error.StorageKernelFailure, client.extractDownloadedAllocWithFailure(
        std.testing.allocator,
        Downloaded{ .data = "not a PDF", .content_type = "application/pdf" },
        "https://example.test/broken.pdf",
        "{}",
        "{}",
        &failure,
    ));
    try std.testing.expectEqual(abi.Status.internal, failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.enrichment_compute, failure.boundary);
    try std.testing.expect(failure.error_name_len > 0);
    try std.testing.expect(failure.error_name_hash != 0);
    try std.testing.expectEqual(abi.abi_version, failure.boundary_version);
    try std.testing.expectEqual(@as(u32, 1), failure.operation);
}

test "enrichment compute boundary returns the exact consumer callback error" {
    const RejectingSink = struct {
        fn onBegin(_: *anyopaque, _: extraction.StreamInfo) anyerror!void {}

        fn onUnit(_: *anyopaque, _: *extraction.Unit) anyerror!void {
            return error.EnrichmentConsumerSentinel;
        }

        fn onEnd(_: *anyopaque) anyerror!void {}
    };

    const source = try std.testing.allocator.alloc(u8, 4 * 1024 * 1024 + 1);
    defer std.testing.allocator.free(source);
    @memset(source, 'x');
    var context: u8 = 0;
    var failure: abi.FailureIdentity = .{};
    try std.testing.expectError(error.EnrichmentConsumerSentinel, client.extractDownloadedStreamingWithFailure(
        std.testing.allocator,
        Downloaded{ .data = source, .content_type = "text/plain" },
        "https://example.test/readme.txt",
        "{}",
        "{}",
        .{
            .ptr = &context,
            .on_begin = RejectingSink.onBegin,
            .on_unit = RejectingSink.onUnit,
            .on_end = RejectingSink.onEnd,
        },
        &failure,
    ));
    try std.testing.expectEqual(abi.Status.ok, failure.status);
    try std.testing.expectEqual(abi.FailureBoundary.none, failure.boundary);
    try std.testing.expectEqual(@as(u8, 0), failure.error_name_len);
}
