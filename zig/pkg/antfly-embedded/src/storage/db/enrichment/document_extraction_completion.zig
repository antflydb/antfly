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

pub fn inspect(alloc: std.mem.Allocator, manifest: []const u8) !Coverage {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, manifest, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidDocumentExtractionManifest;
    var coverage: Coverage = .{};
    if (parsed.value.object.get("ocr_failed_count")) |count| {
        if (count != .integer or count.integer < 0) return error.InvalidDocumentExtractionManifest;
        coverage.failed = count.integer > 0;
    }
    if (parsed.value.object.get("ocr_failed_page_numbers")) |pages| {
        if (pages != .array) return error.InvalidDocumentExtractionManifest;
        coverage.failed = coverage.failed or pages.array.items.len > 0;
    }
    if (parsed.value.object.get("ocr_failure_details")) |details| {
        if (details != .array) return error.InvalidDocumentExtractionManifest;
        coverage.failed = coverage.failed or details.array.items.len > 0;
        for (details.array.items) |detail| {
            if (detail != .object) return error.InvalidDocumentExtractionManifest;
            if (detail.object.get("retryable")) |retryable| {
                if (retryable != .bool) return error.InvalidDocumentExtractionManifest;
                coverage.retryable = coverage.retryable or retryable.bool;
            }
        }
    }
    return coverage;
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
