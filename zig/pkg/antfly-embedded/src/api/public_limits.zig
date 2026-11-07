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

const std = @import("std");

pub const max_request_body_bytes: usize = 64 * 1024 * 1024;
pub const max_json_value_len: usize = max_request_body_bytes;
// Binary cursor header: two fingerprints, an offset, and an inclusive flag.
pub const privacy_cursor_header_bytes: usize = 137;
pub const max_privacy_cursor_payload_bytes: usize = max_request_body_bytes + privacy_cursor_header_bytes;
pub const max_privacy_cursor_encoded_bytes: usize = 3 + std.base64.url_safe_no_pad.Encoder.calcSize(max_privacy_cursor_payload_bytes);
/// One status envelope plus the JSON-escaped maximum raw document key.
pub const max_privacy_status_row_bytes: usize = 6 * max_request_body_bytes + 1024 * 1024;
pub const privacy_preview_sample_bytes: usize = 8 * 1024 * 1024;
pub const privacy_preview_response_bytes: usize = 8 * 1024 * 1024;
pub const privacy_findings_page_bytes: usize = 8 * 1024 * 1024;
/// A page may contain one oversized finding (escaped key and producer revision)
/// plus its continuation. Subsequent findings remain under the ordinary page cap.
pub const max_privacy_findings_response_bytes: usize = max_privacy_cursor_encoded_bytes + 7 * max_request_body_bytes + privacy_findings_page_bytes + 8192;
/// Internal derived keys can include an escaped primary ID and namespace
/// components. Keep that overhead distinct from the public document budget.
pub const max_internal_document_key_bytes: usize = 2 * max_request_body_bytes + 8192;
/// Base64 key bytes and bounded read controls travel outside the request line.
pub const max_document_read_body_bytes: usize = std.base64.url_safe_no_pad.Encoder.calcSize(max_internal_document_key_bytes) + max_request_body_bytes + 8192;

/// Carry a maximum-size identifier in the cursor and document/field filters
/// without imposing a smaller key limit than ordinary document/policy writes.
pub const max_privacy_findings_body_bytes: usize = max_privacy_cursor_encoded_bytes + 6 * max_request_body_bytes + 8192;
/// Only explicitly registered routes may exceed the ordinary body limit.
pub const max_route_body_bytes: usize = @max(max_document_read_body_bytes, max_privacy_findings_body_bytes);
/// Named document filters may be referenced repeatedly. Bound only the growth
/// introduced by normalization so named and inline forms retain the same
/// transport allowance while expansion amplification remains controlled.
pub const max_query_binding_expansion_growth_bytes: usize = 8 * 1024 * 1024;
/// Maximum number of non-null MATCH binding cells that may carry a hydrated
/// document in one named operation. This keeps document projection comparable
/// to a 10k-hit retrieval page even when a row projects many aliases.
pub const max_graph_hydrated_bindings: usize = 10_000;
/// Maximum number of primary items in one canonical graph result collection.
/// This matches the largest public traversal/MATCH limit and is also enforced
/// when decoding peer responses before allocating derived result state.
pub const max_graph_result_items: usize = 10_000;

test "public API request body limit matches Go linear merge contract" {
    try std.testing.expectEqual(@as(usize, 64 * 1024 * 1024), max_request_body_bytes);
    try std.testing.expectEqual(max_request_body_bytes, max_json_value_len);
    try std.testing.expect(max_query_binding_expansion_growth_bytes < max_request_body_bytes);
    try std.testing.expect(max_graph_hydrated_bindings <= 10_000);
    try std.testing.expectEqual(@as(usize, 10_000), max_graph_result_items);
}

/// Public per-request controls. Durations travel as remaining milliseconds,
/// never as a monotonic timestamp from a different process or clock domain.
pub const Execution = struct {
    pub const timeout_header = "x-antfly-timeout-ms";
    pub const scan_rows_header = "x-antfly-sql-scan-rows";
    pub const memory_header = "x-antfly-sql-memory-bytes";
    pub const spill_header = "x-antfly-sql-spill-bytes";
    pub const max_timeout_ms: u64 = 30 * 60 * 1000;

    pub fn positive(raw: []const u8, maximum: u64) !u64 {
        const value = std.fmt.parseUnsigned(u64, raw, 10) catch return error.InvalidExecutionBudget;
        if (value == 0 or value > maximum) return error.InvalidExecutionBudget;
        return value;
    }
};

test "public execution budgets reject unbounded malformed and overflowing controls" {
    try std.testing.expectEqual(@as(u64, 12), try Execution.positive("12", 20));
    for ([_][]const u8{ "0", "-1", "20ms", "21", "18446744073709551616" }) |raw|
        try std.testing.expectError(error.InvalidExecutionBudget, Execution.positive(raw, 20));
}
