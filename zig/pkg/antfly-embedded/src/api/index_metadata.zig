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

//! Control state stored beside index declarations is opaque to configuration
//! discovery, authorization and rewriting. Do not descend into evidence: its
//! user-defined keys can look exactly like live configuration.
const std = @import("std");
pub const aliases = @import("index_aliases.zig");

pub fn isOpaqueEntry(name: []const u8) bool {
    return std.mem.eql(u8, name, aliases.key) or std.mem.eql(u8, name, aliases.receipts_key) or std.mem.eql(u8, name, "_index_alias_epoch") or std.mem.eql(u8, name, "_privacy") or std.mem.eql(u8, name, "_resolver_record_store") or std.mem.eql(u8, name, "_resolver_record_owners") or
        std.mem.eql(u8, name, "_resolver_curation") or
        std.mem.eql(u8, name, "_resolver_namespaces") or
        std.mem.eql(u8, name, "_curation_proposals");
}

test "privacy progress cannot replace physical owners or query catalog cuts" {
    const V = std.json.Value;
    const privacy = @import("../privacy/catalog.zig");
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var resource = try privacy.next(a, null, .{ .mode = .monitor, .fields = &.{"body"} }, null);
    const before = try privacy.install(a, "{}", resource);
    resource.coverage.retry_after_millis = 200;
    _ = try privacy.startScan(a, &resource, "a", "z", 50);
    resource.checkpoint_encoding = 1;
    const progress = try privacy.replaceResource(a, before, resource);
    const physical = try aliases.physicalProjection(a, before);
    const query = try aliases.queryProjection(a, before);
    try std.testing.expectEqualStrings(physical, try aliases.physicalProjection(a, progress));
    try std.testing.expectEqualStrings(query, try aliases.queryProjection(a, progress));
    // The authoritative job state remains intact; projections never mutate it.
    const durable = try std.json.parseFromSliceLeaky(V, a, progress, .{});
    try std.testing.expect(durable.object.get("_privacy").?.object.contains("coverage"));
    try std.testing.expect(durable.object.get("_privacy").?.object.contains("scans"));
    const stripped = try std.json.parseFromSliceLeaky(V, a, physical, .{});
    const configuration = (try privacy.ownerExecution(a, physical)).?;
    try std.testing.expectEqualDeep(resource.policy_version, configuration.policy_version);
    try std.testing.expectEqualDeep(resource.policy.execution, configuration.execution);
    try std.testing.expectError(error.MissingField, privacy.current(a, stripped));
    try std.testing.expect(!stripped.object.get("_privacy").?.object.contains("coverage"));
    try std.testing.expect(!stripped.object.get("_privacy").?.object.contains("scans"));
    var invalid = try std.json.parseFromSliceLeaky(V, a, physical, .{});
    try invalid.object.getPtr("_privacy").?.object.put(a, "generation", .{ .integer = 0 });
    try std.testing.expectError(error.InvalidPrivacyCatalog, privacy.ownerExecution(a, try std.json.Stringify.valueAlloc(a, invalid, .{})));
    var future = try std.json.parseFromSliceLeaky(V, a, physical, .{});
    try future.object.getPtr("_privacy").?.object.put(a, "future_fence", .{ .integer = 1 });
    try std.testing.expectError(error.UnknownField, privacy.ownerExecution(a, try std.json.Stringify.valueAlloc(a, future, .{})));
    // Every semantic or future field is still an owner invalidation fence.
    inline for (.{ "resource_version", "generation", "policy_version", "artifact_name", "policy", "unprotected_indexes_json", "future_fence" }) |field| {
        var changed = try std.json.parseFromSliceLeaky(V, a, progress, .{});
        try changed.object.getPtr("_privacy").?.object.put(a, field, .{ .string = "changed" });
        const encoded = try std.json.Stringify.valueAlloc(a, changed, .{});
        try std.testing.expect(!std.mem.eql(u8, physical, try aliases.physicalProjection(a, encoded)));
        try std.testing.expect(!std.mem.eql(u8, query, try aliases.queryProjection(a, encoded)));
    }
}
