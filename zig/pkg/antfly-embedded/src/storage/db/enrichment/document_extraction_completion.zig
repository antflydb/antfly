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

//! Publication convergence is independent of successful OCR coverage. A PDF
//! may publish usable embedded text while some pages still need inference.
const std = @import("std");

pub const Coverage = struct {
    failed: bool = false,
    retryable: bool = false,
};

// Decode only the completion header. Unknown child ranges, merge operations
// and retained unit bodies are skipped by the typed streaming JSON parser.
const ManifestHeader = struct {
    const Merge = struct { status: ?[]const u8 = null };
    generation: u64 = 0,
    route_type: ?[]const u8 = null,
    merge_plan: ?Merge = null,
    merge: ?Merge = null,
    merge_status: ?[]const u8 = null,
    last_error: ?std.json.Value = null,
    unsupported_reason: ?[]const u8 = null,
    incomplete_unit_count: u64 = 0,
    retryable_incomplete_unit_count: u64 = 0,
    ocr_failed_count: u64 = 0,
    ocr_retryable_failed_count: u64 = 0,
    ocr_failed_page_numbers: []const u64 = &.{},
    ocr_failure_details: []const struct { retryable: bool = false } = &.{},

    fn coverage(self: @This()) Coverage {
        var value: Coverage = .{
            .failed = (if (self.unsupported_reason) |reason| reason.len != 0 else false) or
                self.incomplete_unit_count != 0 or self.retryable_incomplete_unit_count != 0 or
                self.ocr_failed_count != 0 or self.ocr_retryable_failed_count != 0 or
                self.ocr_failed_page_numbers.len != 0 or self.ocr_failure_details.len != 0,
            .retryable = self.retryable_incomplete_unit_count != 0 or self.ocr_retryable_failed_count != 0,
        };
        for (self.ocr_failure_details) |detail| value.retryable = value.retryable or detail.retryable;
        return value;
    }

    fn state(self: @This()) PublicationState {
        const route = self.route_type orelse return .pending;
        if (route.len == 0) return .pending;
        if (std.mem.eql(u8, route, "error") or self.last_error != null) return .failed;
        var published = false;
        if (self.merge_plan orelse self.merge) |merge| {
            const status = merge.status orelse return .pending;
            if (std.mem.eql(u8, status, "failed")) return .failed;
            if (!std.mem.eql(u8, status, "converged")) return .pending;
            published = true;
        }
        if (self.merge_status) |status| {
            if (std.mem.eql(u8, status, "failed")) return .failed;
            if (!std.mem.eql(u8, status, "converged")) return .pending;
            published = true;
        }
        if (!published) return .pending;
        return if (self.coverage().failed) .failed else .complete;
    }
};

fn parseHeader(alloc: std.mem.Allocator, manifest: []const u8) !std.json.Parsed(ManifestHeader) {
    return std.json.parseFromSlice(ManifestHeader, alloc, manifest, .{ .ignore_unknown_fields = true }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidDocumentExtractionManifest,
    };
}

pub fn inspect(alloc: std.mem.Allocator, manifest: []const u8) !Coverage {
    var parsed = try parseHeader(alloc, manifest);
    defer parsed.deinit();
    return parsed.value.coverage();
}

/// Coverage and generation come from the same immutable manifest snapshot.
pub const Snapshot = struct {
    generation: u64 = 0,
    state: PublicationState = .pending,
    pub fn complete(self: Snapshot) bool {
        return self.state == .complete;
    }
};

pub fn snapshot(alloc: std.mem.Allocator, manifest: []const u8) !Snapshot {
    var parsed = try parseHeader(alloc, manifest);
    defer parsed.deinit();
    return .{ .generation = parsed.value.generation, .state = parsed.value.state() };
}

pub fn isComplete(alloc: std.mem.Allocator, manifest: []const u8) !bool {
    return (try snapshot(alloc, manifest)).complete();
}

test "document extraction completion retains retryable page debt" {
    const a = std.testing.allocator;
    const mixed = try inspect(a,
        \\{"merge_status":"converged","chunk_count":3,"ocr_failed_count":2,"ocr_failure_details":[{"retryable":false},{"retryable":true}]}
    );
    try std.testing.expect(mixed.failed and mixed.retryable);
    const permanent = try inspect(a,
        \\{"chunk_count":3,"ocr_failed_count":1,"ocr_failure_details":[{"retryable":false}]}
    );
    try std.testing.expect(permanent.failed and !permanent.retryable);
    try std.testing.expectEqual(Coverage{}, try inspect(a, "{\"chunk_count\":3,\"ocr_failed_count\":0}"));
    const unknown = try inspect(a, "{\"chunk_count\":3,\"ocr_failed_count\":1}");
    try std.testing.expect(unknown.failed and !unknown.retryable);
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, inspect(a, "{\"ocr_failed_count\":-1}"));
}

test "aggregate OCR retry debt survives truncated terminal diagnostics" {
    const a = std.testing.allocator;
    const coverage = try inspect(a,
        \\{"ocr_failed_count":33,"ocr_retryable_failed_count":1,"ocr_failed_pages_truncated":true,"ocr_failure_details":[{"retryable":false}]}
    );
    try std.testing.expect(coverage.failed and coverage.retryable);
    const terminal = try inspect(a,
        \\{"ocr_failed_count":33,"ocr_retryable_failed_count":0,"ocr_failed_pages_truncated":true,"ocr_failure_details":[{"retryable":false}]}
    );
    try std.testing.expect(terminal.failed and !terminal.retryable);
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, inspect(a, "{\"ocr_retryable_failed_count\":-1}"));
}

test "missing modality producers and transcription failures remain incomplete" {
    const a = std.testing.allocator;
    const absent = try inspect(a, "{\"incomplete_unit_count\":1,\"retryable_incomplete_unit_count\":0}");
    try std.testing.expect(absent.failed and !absent.retryable);
    const transient = try inspect(a, "{\"incomplete_unit_count\":1,\"retryable_incomplete_unit_count\":1,\"ocr_failed_count\":0,\"ocr_retryable_failed_count\":0}");
    try std.testing.expect(transient.failed and transient.retryable);
    const unsupported = try inspect(a, "{\"unsupported_reason\":\"video route not configured\"}");
    try std.testing.expect(unsupported.failed and !unsupported.retryable);
}

/// A converged merge can still contain incomplete OCR or another modality.
/// All repair paths use this predicate instead of equating readability with
/// complete extraction. Parse once; never inspect retained child bodies.
pub const PublicationState = enum { pending, failed, complete };
pub fn publicationState(alloc: std.mem.Allocator, manifest: []const u8) !PublicationState {
    return (try snapshot(alloc, manifest)).state;
}

test "publication health separates readable partial and pending extraction from success" {
    const a = std.testing.allocator;
    const prefix = "{\"route_type\":\"pdf\",\"merge_plan\":{\"status\":\"converged\"},\"unit_count\":25,";
    try std.testing.expectEqual(PublicationState.complete, try publicationState(a, prefix ++ "\"ocr_failed_count\":0}"));
    inline for (.{ "\"ocr_failed_count\":1", "\"ocr_failed_page_numbers\":[7]", "\"ocr_failure_details\":[{\"retryable\":false}]", "\"incomplete_unit_count\":1", "\"unsupported_reason\":\"missing_transcriber\"", "\"last_error\":{\"code\":\"ReadTransientFailure\"}" }) |failure|
        try std.testing.expectEqual(PublicationState.failed, try publicationState(a, prefix ++ failure ++ "}"));
    try std.testing.expectEqual(PublicationState.pending, try publicationState(a, "{\"route_type\":\"pdf\",\"merge_plan\":{\"status\":\"in_progress\"}}"));
}

test "incomplete selection preserves only published error-free coverage" {
    const a = std.testing.allocator;
    const published = "{\"route_type\":\"pdf\",\"generation\":7,\"merge_plan\":{\"status\":\"converged\"},\"ocr_failed_count\":0}";
    const view = try snapshot(a, published);
    try std.testing.expect(view.complete());
    try std.testing.expectEqual(@as(u64, 7), view.generation);
    try std.testing.expect(try isComplete(a, published));
    inline for (.{ "{}", "{\"route_type\":\"pdf\",\"merge_plan\":{\"status\":\"pending\"}}", "{\"route_type\":\"pdf\",\"merge_plan\":{\"status\":\"converged\"},\"ocr_failed_count\":1}", "{\"route_type\":\"pdf\",\"merge_plan\":{\"status\":\"converged\"},\"incomplete_unit_count\":1}" }) |manifest|
        try std.testing.expect(!try isComplete(a, manifest));
    try std.testing.expectError(error.InvalidDocumentExtractionManifest, snapshot(a, "{\"generation\":-1}"));
}

test "completion header skips large retained payloads" {
    const a = std.testing.allocator;
    const retained = try a.alloc(u8, 512 * 1024);
    defer a.free(retained);
    @memset(retained, 'x');
    const raw = try std.fmt.allocPrint(a, "{{\"route_type\":\"pdf\",\"generation\":7,\"merge_plan\":{{\"status\":\"converged\",\"operations\":[{{\"retained_payload\":\"{s}\"}}]}},\"ocr_failed_count\":0}}", .{retained});
    defer a.free(raw);
    var memory: [8192]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&memory);
    const header = try snapshot(bounded.allocator(), raw);
    try std.testing.expect(header.complete());
    try std.testing.expectEqual(@as(u64, 7), header.generation);
}
