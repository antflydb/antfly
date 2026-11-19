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

//! Fixed-memory naming-capability projection for final proposal admission.
const std = @import("std");

pub fn requiresOccupancy(bytes: []const u8, max_bytes: usize) bool {
    if (bytes.len > max_bytes) return true;
    // Unknown schema/parent/row fields are skipped rather than materialized.
    const Marker = struct { namespace_id: u64 };
    const Guard = struct { constraint_name_admission: ?Marker = null };
    const Shape = struct {
        version: u32 = 1,
        constraint_name_admission: ?Marker = null,
        constraint_name_guard: ?Guard = null,
        plan: ?struct { constraint_name_admission: ?Marker = null, constraint_name_guard: ?Guard = null } = null,
    };
    var buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    const shape = std.json.parseFromSliceLeaky(Shape, fixed.allocator(), bytes, .{ .ignore_unknown_fields = true }) catch return true;
    // Malformed, exhausted and future frames cannot use legacy admission.
    return shape.version != 1 or shape.constraint_name_admission != null or shape.constraint_name_guard != null or
        (if (shape.plan) |plan| plan.constraint_name_admission != null or plan.constraint_name_guard != null else false);
}

/// Fixed-memory projection for versioned catalog-identity commands, including
/// ACKs which carry no plan. Unknown or malformed markers fail closed.
pub fn requiresCatalogChecks(bytes: []const u8, max_bytes: usize) bool {
    if (bytes.len > max_bytes) return true;
    const Coverage = enum { legacy, checks_v1 };
    const Shape = struct {
        catalog_coverage: Coverage = .legacy,
        plan: ?struct { catalog_coverage: Coverage = .legacy } = null,
    };
    var buffer: [4096]u8 = undefined;
    var fixed = std.heap.FixedBufferAllocator.init(&buffer);
    const shape = std.json.parseFromSliceLeaky(Shape, fixed.allocator(), bytes, .{ .ignore_unknown_fields = true }) catch return true;
    return shape.catalog_coverage != .legacy or (if (shape.plan) |plan| plan.catalog_coverage != .legacy else false);
}

test "CHECK identity wire projection gates both plan admission and planless ACKs" {
    const limit = 3 * 1024 * 1024;
    try std.testing.expect(!requiresCatalogChecks("{}", limit));
    try std.testing.expect(!requiresCatalogChecks("{\"catalog_coverage\":\"legacy\"}", limit));
    try std.testing.expect(requiresCatalogChecks("{\"catalog_coverage\":\"checks_v1\",\"plan\":null}", limit));
    try std.testing.expect(requiresCatalogChecks("{\"plan\":{\"catalog_coverage\":\"checks_v1\"}}", limit));
    for ([_][]const u8{ "{", "{\"catalog_coverage\":\"future\"}", "{\"catalog_coverage\":null}" }) |invalid|
        try std.testing.expect(requiresCatalogChecks(invalid, limit));
}

test "naming wire projection skips large escaped schemas and detects nested and direct predicates" {
    const a = std.testing.allocator;
    const large = try a.alloc(u8, 64 * 1024);
    defer a.free(large);
    @memset(large, '"');
    const limit = 3 * 1024 * 1024;
    const legacy = try std.json.Stringify.valueAlloc(a, .{ .plan = .{ .child = .{ .schema_json = large }, .constraint_name_admission = @as(?u64, null) } }, .{});
    defer a.free(legacy);
    try std.testing.expect(!requiresOccupancy(legacy, limit));
    const named = try std.json.Stringify.valueAlloc(a, .{ .plan = .{ .child = .{ .schema_json = large }, .constraint_name_admission = .{ .namespace_id = 2, .names = &.{"ignored"} } } }, .{});
    defer a.free(named);
    try std.testing.expect(requiresOccupancy(named, limit));
    try std.testing.expect(requiresOccupancy("{\"plan\":{\"constraint_name_guard\":{\"constraint_name_admission\":{\"namespace_id\":2}}}}", limit));
    try std.testing.expect(requiresOccupancy("{\"plan\":{\"constraint_name_guard\":{}}}", limit));
    try std.testing.expect(requiresOccupancy("{\"constraint_name_guard\":{\"constraint_name_admission\":{\"namespace_id\":2}}}", limit));
    try std.testing.expect(!requiresOccupancy("{\"plan\":{\"constraint_name_guard\":null}}", limit));
    try std.testing.expect(requiresOccupancy("{\"constraint_name_admission\":{\"namespace_id\":2}}", limit));
    try std.testing.expect(!requiresOccupancy("{\"version\":1,\"constraint_name_admission\":null}", limit));
    for ([_][]const u8{ "{", "{\"version\":0}", "{\"version\":2}", "{\"plan\":{\"constraint_name_admission\":{}}}" }) |invalid|
        try std.testing.expect(requiresOccupancy(invalid, limit));
    try std.testing.expect(requiresOccupancy(legacy, 4096));
}
