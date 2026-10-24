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

//! Algebraic logical-work estimates, never predicted time or execution caps.
const std = @import("std");
pub const Estimate = struct {
    rows: ?u64 = null,
    rows_upper: ?u64 = null,
    source_bytes: ?u64 = null,
    local_work_units: ?u64 = null,
    basis: []const u8 = "unknown",
};
pub const model = .{
    .version = @as(u32, 1),
    .unit = "logical tuple visits/comparisons; not milliseconds or currency",
    .scope = "logical plan; execution may select a different physical join strategy",
    .calibrated_time_model = false,
    .source = "metadata-only provider; no cursor capture, row pulls or data sampling",
    .snapshot = "planning metadata may be stale; no execution snapshot is claimed",
    .formulas = "scan=N; hash input=N+M; nested-loop comparisons=N*M; order=N*ceil(log2(max(2,N))); unary input=N",
    .excludes = "provider/inference latency, remote I/O, compression, cache state, tuple width and unknown output selectivity",
};
pub fn source(rows: ?u64, source_bytes: ?u64, filtered: bool, point: bool) Estimate {
    return .{ .rows = if (filtered) null else rows, .rows_upper = if (point) 1 else null, .source_bytes = source_bytes, .local_work_units = rows, .basis = if (point) "identity point bound; source cardinality may be unknown" else if (rows != null) "captured source estimate; not an exact count" else "source statistics unavailable" };
}
pub fn unary(kind: []const u8, child: Estimate) Estimate {
    var result: Estimate = .{ .rows_upper = child.rows_upper, .local_work_units = child.rows, .basis = "input cardinality; output selectivity unknown" };
    if (std.mem.eql(u8, kind, "Order")) {
        result.rows = child.rows;
        if (child.rows) |n| result.local_work_units = std.math.mul(u64, n, std.math.log2_int_ceil(u64, @max(2, n))) catch null;
    }
    if (std.mem.eql(u8, kind, "Aggregate")) result.rows_upper = null;
    return result;
}
pub fn join(left: Estimate, right: Estimate, hash: bool) Estimate {
    return .{ .local_work_units = if (left.rows != null and right.rows != null) (if (hash) std.math.add(u64, left.rows.?, right.rows.?) catch null else std.math.mul(u64, left.rows.?, right.rows.?) catch null) else null, .basis = if (hash) "hash input visits only; output multiplicity and key skew unknown" else "Cartesian comparison bound; predicate selectivity unknown" };
}
test "cost model keeps unknowns unknown and exposes transparent algebra" {
    const known = source(100, 4000, false, false);
    try std.testing.expectEqual(@as(?u64, 700), unary("Order", known).local_work_units);
    try std.testing.expectEqual(@as(?u64, 200), join(known, known, true).local_work_units);
    try std.testing.expectEqual(@as(?u64, 10000), join(known, known, false).local_work_units);
    try std.testing.expect(join(known, .{}, true).local_work_units == null);
    try std.testing.expect(source(100, null, true, false).rows == null);
    try std.testing.expectEqual(@as(?u64, 1), source(null, null, false, true).rows_upper);
}
