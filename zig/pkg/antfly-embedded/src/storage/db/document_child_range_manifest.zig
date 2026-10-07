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
const Allocator = std.mem.Allocator;
const types = @import("types.zig");

pub fn jsonObjectStringDup(alloc: Allocator, object: std.json.ObjectMap, field_name: []const u8) ![]u8 {
    const value = object.get(field_name) orelse return "";
    if (value != .string) return "";
    return try alloc.dupe(u8, value.string);
}

pub fn jsonObjectOptionalStringDup(alloc: Allocator, object: std.json.ObjectMap, field_name: []const u8) !?[]u8 {
    const value = object.get(field_name) orelse return null;
    if (value != .string) return null;
    return try alloc.dupe(u8, value.string);
}

pub fn jsonObjectOptionalNestedStringDup(alloc: Allocator, object: std.json.ObjectMap, object_field: []const u8, string_field: []const u8) !?[]u8 {
    const value = object.get(object_field) orelse return null;
    if (value != .object) return null;
    return try jsonObjectOptionalStringDup(alloc, value.object, string_field);
}

pub fn jsonObjectU64(object: std.json.ObjectMap, field_name: []const u8) !u64 {
    const value = object.get(field_name) orelse return 0;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(u64, value.integer) orelse return error.InvalidDocumentExtractionManifest;
}

pub fn jsonObjectUsize(object: std.json.ObjectMap, field_name: []const u8) !usize {
    return std.math.cast(usize, try jsonObjectU64(object, field_name)) orelse return error.InvalidDocumentExtractionManifest;
}

pub fn jsonObjectOptionalUsize(object: std.json.ObjectMap, field_name: []const u8) !?usize {
    const value = object.get(field_name) orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(usize, value.integer) orelse return error.InvalidDocumentExtractionManifest;
}

pub fn jsonObjectOptionalU64(object: std.json.ObjectMap, field_name: []const u8) !?u64 {
    const value = object.get(field_name) orelse return null;
    if (value != .integer or value.integer < 0) return error.InvalidDocumentExtractionManifest;
    return std.math.cast(u64, value.integer) orelse return error.InvalidDocumentExtractionManifest;
}

pub fn jsonObjectOptionalBool(object: std.json.ObjectMap, field_name: []const u8) !?bool {
    const value = object.get(field_name) orelse return null;
    if (value != .bool) return error.InvalidDocumentExtractionManifest;
    return value.bool;
}

pub fn documentArtifactChildRangesFromJsonAlloc(alloc: Allocator, object: std.json.ObjectMap) ![]types.DocumentArtifactChildRange {
    const value = object.get("child_ranges") orelse return try alloc.alloc(types.DocumentArtifactChildRange, 0);
    if (value != .array) return try alloc.alloc(types.DocumentArtifactChildRange, 0);

    const out = try alloc.alloc(types.DocumentArtifactChildRange, value.array.items.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*range| range.deinit(alloc);
        if (out.len > 0) alloc.free(out);
    }

    for (value.array.items, 0..) |item, i| {
        if (item != .object) return error.InvalidDocumentExtractionManifest;
        var range: types.DocumentArtifactChildRange = .{
            .range_id = @constCast(""),
            .range_kind = @constCast(""),
            .artifact_name = @constCast(""),
            .split_boundary = @constCast(""),
            .placement = @constCast(""),
            .start_key = @constCast(""),
            .end_key_exclusive = @constCast(""),
            .last_key = @constCast(""),
        };
        errdefer range.deinit(alloc);
        range.range_id = try jsonObjectStringDup(alloc, item.object, "range_id");
        range.range_kind = try jsonObjectStringDup(alloc, item.object, "range_kind");
        range.artifact_name = try jsonObjectStringDup(alloc, item.object, "artifact_name");
        range.split_boundary = try jsonObjectStringDup(alloc, item.object, "split_boundary");
        range.placement = try jsonObjectStringDup(alloc, item.object, "placement");
        range.owner_group_id = try jsonObjectOptionalU64(item.object, "owner_group_id");
        range.placement_generation = try jsonObjectOptionalU64(item.object, "placement_generation");
        range.route_status = try jsonObjectOptionalStringDup(alloc, item.object, "route_status");
        range.split_eligible = try jsonObjectOptionalBool(item.object, "split_eligible");
        range.start_key = try jsonObjectStringDup(alloc, item.object, "start_key");
        range.end_key_exclusive = try jsonObjectStringDup(alloc, item.object, "end_key_exclusive");
        range.last_key = try jsonObjectStringDup(alloc, item.object, "last_key");
        range.child_count = try jsonObjectUsize(item.object, "child_count");
        range.text_bytes = try jsonObjectOptionalUsize(item.object, "text_bytes");
        out[i] = range;
        initialized += 1;
    }

    return out;
}

pub fn documentArtifactChildRangesFromManifestJsonAlloc(alloc: Allocator, manifest_json: []const u8) ![]types.DocumentArtifactChildRange {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, manifest_json, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return try alloc.alloc(types.DocumentArtifactChildRange, 0);
    return try documentArtifactChildRangesFromJsonAlloc(alloc, parsed.value.object);
}

pub fn freeDocumentArtifactChildRanges(alloc: Allocator, child_ranges: []types.DocumentArtifactChildRange) void {
    for (child_ranges) |*child_range| child_range.deinit(alloc);
    if (child_ranges.len > 0) alloc.free(child_ranges);
}

test "child range manifest releases partially parsed ranges" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(alloc: Allocator) !void {
            const ranges = try documentArtifactChildRangesFromManifestJsonAlloc(alloc,
                \\{"child_ranges":[{"range_id":"range:1","range_kind":"unit","artifact_name":"units","split_boundary":"unit","placement":"remote","route_status":"remote_committed","start_key":"a","end_key_exclusive":"z","last_key":"y","child_count":1}]}
            );
            defer freeDocumentArtifactChildRanges(alloc, ranges);
            try std.testing.expectEqual(@as(usize, 1), ranges.len);
        }
    }.run, .{});
}
