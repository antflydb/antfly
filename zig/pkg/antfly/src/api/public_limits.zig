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

const std = @import("std");

pub const max_request_body_bytes: usize = 64 * 1024 * 1024;
pub const max_json_value_len: usize = max_request_body_bytes;
/// Named document filters may be referenced repeatedly. Bound only the growth
/// introduced by normalization so named and inline forms retain the same
/// transport allowance while expansion amplification remains controlled.
pub const max_query_binding_expansion_growth_bytes: usize = 8 * 1024 * 1024;
/// Maximum number of non-null MATCH binding cells that may carry a hydrated
/// document in one named operation. This keeps document projection comparable
/// to a 10k-hit retrieval page even when a row projects many aliases.
pub const max_graph_hydrated_bindings: usize = 10_000;

test "public API request body limit matches Go linear merge contract" {
    try std.testing.expectEqual(@as(usize, 64 * 1024 * 1024), max_request_body_bytes);
    try std.testing.expectEqual(max_request_body_bytes, max_json_value_len);
    try std.testing.expect(max_query_binding_expansion_growth_bytes < max_request_body_bytes);
    try std.testing.expect(max_graph_hydrated_bindings <= 10_000);
}
