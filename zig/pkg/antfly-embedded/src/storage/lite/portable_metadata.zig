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

//! Pure portable-catalog decoding, shared by inspection and restore staging.
const std = @import("std");
const Allocator = std.mem.Allocator;
const db_types = @import("../db/types.zig");
const internal_keys = @import("../internal_keys.zig");

pub fn tableIndexesJsonFromPortableMetadata(
    allocator: Allocator,
    raw_indexes_json: ?[]const u8,
    raw_enrichments_json: ?[]const u8,
) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    try out.append(allocator, '{');
    var first = true;

    if (raw_indexes_json) |indexes_json| {
        if (std.mem.startsWith(u8, indexes_json, "AIDX")) {
            try appendBinaryIndexCatalogFields(allocator, &out, &first, indexes_json);
        } else {
            var parsed = try std.json.parseFromSlice(std.json.Value, allocator, indexes_json, .{});
            defer parsed.deinit();
            switch (parsed.value) {
                .object => |object| {
                    var it = object.iterator();
                    while (it.next()) |entry| {
                        if (raw_enrichments_json != null and std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
                        try appendJsonObjectField(allocator, &out, &first, entry.key_ptr.*, entry.value_ptr.*);
                    }
                },
                .array => |array| {
                    for (array.items) |item| {
                        const object = switch (item) {
                            .object => |object| object,
                            else => return error.InvalidBackupRequest,
                        };
                        const name_value = object.get("name") orelse return error.InvalidBackupRequest;
                        if (name_value != .string or name_value.string.len == 0) return error.InvalidBackupRequest;
                        try appendJsonObjectField(allocator, &out, &first, name_value.string, item);
                    }
                },
                else => return error.InvalidBackupRequest,
            }
        }
    }

    if (raw_enrichments_json) |enrichments_json| {
        var parsed = try std.json.parseFromSlice(std.json.Value, allocator, enrichments_json, .{});
        defer parsed.deinit();
        if (parsed.value != .array) return error.InvalidBackupRequest;
        // Persisted enrichment catalogs use internal selector names and JSON
        // strings for nested policy. Table catalogs consume the public shape.
        // Preserve public-shaped inputs too; reject ambiguous mixed records.
        const arena = parsed.arena.allocator();
        for (parsed.value.array.items) |*item| {
            if (item.* != .object) return error.InvalidBackupRequest;
            inline for (.{ .{ "source_field", "field" }, .{ "source_template", "template" } }) |names| {
                if (item.object.fetchOrderedRemove(names[0])) |entry| {
                    if (item.object.contains(names[1])) return error.InvalidBackupRequest;
                    try item.object.put(arena, names[1], entry.value);
                }
            }
            inline for (.{ .{ "neighbor_context_json", "neighbor_context" }, .{ "execution_json", "execution" } }) |names| {
                if (item.object.fetchOrderedRemove(names[0])) |entry| {
                    if (entry.value != .string or item.object.contains(names[1])) return error.InvalidBackupRequest;
                    if (entry.value.string.len > 0) {
                        const nested = try std.json.parseFromSlice(std.json.Value, arena, entry.value.string, .{});
                        try item.object.put(arena, names[1], nested.value);
                    }
                }
            }
        }
        try appendJsonObjectField(allocator, &out, &first, "enrichments", parsed.value);
    }

    try out.append(allocator, '}');
    return try out.toOwnedSlice(allocator);
}

fn appendBinaryIndexCatalogFields(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    data: []const u8,
) !void {
    if (data.len < 12 or !std.mem.eql(u8, data[0..4], "AIDX")) return error.InvalidBackupRequest;

    var pos: usize = 4;
    const version = try readCatalogU32(data, &pos);
    if (version != 1 and version != 2) return error.InvalidBackupRequest;
    const count = try readCatalogU32(data, &pos);

    for (0..count) |_| {
        const name = try readCatalogString(data, &pos);
        if (pos >= data.len) return error.InvalidBackupRequest;
        const kind_value = data[pos];
        pos += 1;
        const kind = indexKindFromCatalogByte(kind_value) orelse return error.InvalidBackupRequest;
        const config_json = try readCatalogString(data, &pos);
        const coverage_generation = if (version >= 2)
            try readCatalogU64(data, &pos)
        else
            internal_keys.derivedCoverageGeneration(config_json);

        if (!first.*) try out.append(allocator, ',');
        first.* = false;
        try appendJsonString(allocator, out, name);
        try out.appendSlice(allocator, ":{\"name\":");
        try appendJsonString(allocator, out, name);
        try out.appendSlice(allocator, ",\"kind\":");
        try appendJsonString(allocator, out, @tagName(kind));
        try out.appendSlice(allocator, ",\"config_json\":");
        try appendJsonString(allocator, out, config_json);
        try out.appendSlice(allocator, ",\"coverage_generation\":");
        try appendJsonU64(allocator, out, coverage_generation);
        try out.append(allocator, '}');
    }
}

fn indexKindFromCatalogByte(value: u8) ?db_types.IndexKind {
    return switch (value) {
        @backingInt(db_types.IndexKind.full_text) => .full_text,
        @backingInt(db_types.IndexKind.dense_vector) => .dense_vector,
        @backingInt(db_types.IndexKind.sparse_vector) => .sparse_vector,
        @backingInt(db_types.IndexKind.graph) => .graph,
        @backingInt(db_types.IndexKind.algebraic) => .algebraic,
        else => null,
    };
}

fn readCatalogU32(data: []const u8, pos: *usize) !u32 {
    if (pos.* + 4 > data.len) return error.InvalidBackupRequest;
    const value = std.mem.readInt(u32, data[pos.*..][0..4], .little);
    pos.* += 4;
    return value;
}

fn readCatalogU64(data: []const u8, pos: *usize) !u64 {
    if (pos.* + 8 > data.len) return error.InvalidBackupRequest;
    const value = std.mem.readInt(u64, data[pos.*..][0..8], .little);
    pos.* += 8;
    return value;
}

fn readCatalogString(data: []const u8, pos: *usize) ![]const u8 {
    const len = try readCatalogU32(data, pos);
    if (pos.* + len > data.len) return error.InvalidBackupRequest;
    const value = data[pos.* .. pos.* + len];
    pos.* += len;
    return value;
}

fn appendJsonObjectField(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    first: *bool,
    name: []const u8,
    value: std.json.Value,
) !void {
    if (!first.*) try out.append(allocator, ',');
    first.* = false;
    try appendJsonString(allocator, out, name);
    try out.append(allocator, ':');
    const encoded = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(value, .{})});
    defer allocator.free(encoded);
    try out.appendSlice(allocator, encoded);
}

fn appendJsonString(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: []const u8,
) !void {
    const escaped = try std.fmt.allocPrint(allocator, "{f}", .{std.json.fmt(value, .{})});
    defer allocator.free(escaped);
    try out.appendSlice(allocator, escaped);
}

fn appendJsonU64(
    allocator: Allocator,
    out: *std.ArrayListUnmanaged(u8),
    value: u64,
) !void {
    const encoded = try std.fmt.allocPrint(allocator, "{d}", .{value});
    defer allocator.free(encoded);
    try out.appendSlice(allocator, encoded);
}

/// Translate persisted runtime catalogs at every Lite-to-public boundary.
pub const CatalogDestination = enum { table_restore, embedded_adoption };

pub fn publicTableIndexesJsonAlloc(alloc: Allocator, stored: []const u8, destination: CatalogDestination) ![]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidEmbeddedLiteIndex;
    const arena = parsed.arena.allocator();
    var it = parsed.value.object.iterator();
    while (it.next()) |entry| {
        if (std.mem.eql(u8, entry.key_ptr.*, "enrichments")) continue;
        const value = entry.value_ptr.*;
        if (value != .object) return error.InvalidEmbeddedLiteIndex;
        // Server archives already carry the public catalog shape.
        if (value.object.get("type")) |kind| {
            if (kind != .string) return error.InvalidEmbeddedLiteIndex;
            if (destination == .embedded_adoption and !std.mem.eql(u8, kind.string, "full_text")) return error.UnsupportedEmbeddedLiteIndex;
            continue;
        }
        const kind = value.object.get("kind") orelse return error.InvalidEmbeddedLiteIndex;
        if (kind != .string) return error.InvalidEmbeddedLiteIndex;
        const full_text = std.mem.eql(u8, kind.string, "full_text");
        if (destination == .embedded_adoption and !full_text) return error.UnsupportedEmbeddedLiteIndex;
        const raw = value.object.get("config_json") orelse return error.InvalidEmbeddedLiteIndex;
        if (raw != .string) return error.InvalidEmbeddedLiteIndex;
        var config = try std.json.parseFromSliceLeaky(std.json.Value, arena, raw.string, .{});
        if (config != .object) return error.InvalidEmbeddedLiteIndex;
        const dense = std.mem.eql(u8, kind.string, "dense_vector");
        const sparse = std.mem.eql(u8, kind.string, "sparse_vector");
        if (dense or sparse) {
            // Native managed generators are lowered executable programs; there
            // is no lossless public inverse. Artifact and external consumers
            // retain their authoritative vectors and can be translated exactly.
            if (config.object.contains("generator") or config.object.contains("embedder")) return error.UnsupportedEmbeddedLiteIndex;
            const external_value = config.object.get("external");
            const external = if (external_value) |v| v == .bool and v.bool else false;
            if (external) {
                // The primary vector backend materializes a native self-reference
                // for external vectors. It is not a public enrichment producer.
                if (config.object.get("embedding_name")) |name| {
                    if (name != .string or !std.mem.eql(u8, name.string, entry.key_ptr.*)) return error.UnsupportedEmbeddedLiteIndex;
                    _ = config.object.orderedRemove("embedding_name");
                }
                if (config.object.contains("sources")) return error.UnsupportedEmbeddedLiteIndex;
            }
            const artifact_backed = config.object.contains("sources") or config.object.contains("embedding_name");
            // An unmarked native sparse index can extract vectors from
            // documents. Only the public catalog identifies legacy external
            // definitions; native direct fields have no public inverse.
            if (!artifact_backed and !external) return error.UnsupportedEmbeddedLiteIndex;
            _ = config.object.orderedRemove("field");
            if (config.object.fetchOrderedRemove("dims")) |field| try config.object.put(arena, "dimension", field.value);
            if (config.object.fetchOrderedRemove("metric")) |field| try config.object.put(arena, "distance_metric", field.value);
            if (sparse) try config.object.put(arena, "sparse", .{ .bool = true });
            try config.object.put(arena, "type", .{ .string = "embeddings" });
        } else if (full_text or std.mem.eql(u8, kind.string, "graph") or std.mem.eql(u8, kind.string, "algebraic")) {
            try config.object.put(arena, "type", kind);
        } else return error.UnsupportedEmbeddedLiteIndex;
        if (value.object.get("coverage_generation")) |generation|
            try config.object.put(arena, "_index_incarnation", generation);
        entry.value_ptr.* = config;
    }
    return try std.json.Stringify.valueAlloc(alloc, parsed.value, .{});
}
