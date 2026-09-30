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
const types = @import("../types.zig");
const document_query = @import("../document_query.zig");
const search_exec = @import("search_exec.zig");
const hierarchy_navigation = @import("../../hierarchy_navigation.zig");

pub const SpecialFieldSelection = struct {
    all_artifacts: bool = false,
    all_chunks: bool = false,
    all_embeddings: bool = false,
};

pub const FieldSelectionPlan = struct {
    projection: types.LookupOptions,
    special: SpecialFieldSelection,
};

pub const SpecialFieldLoader = struct {
    ctx: ?*anyopaque,
    load_chunks: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_key: []const u8,
    ) anyerror!?std.json.Value,
    load_embeddings: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_key: []const u8,
    ) anyerror!?std.json.Value,
    load_artifacts: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        doc_key: []const u8,
    ) anyerror!?std.json.Value,
};

pub fn buildLookupFieldSelectionPlan(opts: types.LookupOptions) FieldSelectionPlan {
    return buildFieldSelectionPlan(opts.fields, opts.include_all_fields);
}

pub fn buildSearchFieldSelectionPlan(req: types.SearchRequest) FieldSelectionPlan {
    return buildFieldSelectionPlan(req.fields, req.include_all_fields);
}

pub fn shouldProjectSearchStored(req: types.SearchRequest) bool {
    return req.fields.len > 0 or !req.include_all_fields;
}

pub fn projectLookupStoredBytesWithPlan(
    alloc: Allocator,
    doc_key: []const u8,
    raw: []const u8,
    plan: FieldSelectionPlan,
    loader: SpecialFieldLoader,
) ![]u8 {
    const merged = try mergeStoredDocumentWithSpecialFields(alloc, doc_key, raw, plan.special, loader);
    errdefer alloc.free(merged);

    const projected = try document_query.lookupJson(alloc, merged, plan.projection);
    alloc.free(merged);
    return projected.json;
}

pub fn projectLookupStoredBytes(
    alloc: Allocator,
    doc_key: []const u8,
    raw: []const u8,
    opts: types.LookupOptions,
    loader: SpecialFieldLoader,
) ![]u8 {
    return try projectLookupStoredBytesWithPlan(alloc, doc_key, raw, buildLookupFieldSelectionPlan(opts), loader);
}

pub fn projectStoredBytesForSearch(
    alloc: Allocator,
    req: types.SearchRequest,
    doc_key: []const u8,
    raw: []const u8,
    loader: SpecialFieldLoader,
) ![]u8 {
    if (!shouldProjectSearchStored(req)) return try alloc.dupe(u8, raw);
    return try projectLookupStoredBytesWithPlan(alloc, doc_key, raw, buildSearchFieldSelectionPlan(req), loader);
}

pub fn projectOwnedStoredBytesForSearch(
    alloc: Allocator,
    req: types.SearchRequest,
    doc_key: []const u8,
    raw: []u8,
    loader: SpecialFieldLoader,
) ![]u8 {
    if (!shouldProjectSearchStored(req)) return raw;
    defer alloc.free(raw);
    return try projectLookupStoredBytes(alloc, doc_key, raw, search_exec.searchLookupOptions(req), loader);
}

const owned_json = @import("../../../common/owned_json.zig");
pub const freeJsonValue = owned_json.deinit;
pub const cloneJsonValue = owned_json.clone;
pub const putOwnedValue = owned_json.put;
const putClonedValue = owned_json.putClone;

pub fn normalizeChunkArtifactForQuery(alloc: Allocator, value: *std.json.Value) !void {
    if (value.* != .object) return;
    var obj = &value.object;

    // Synthetic `_chunks` and `_artifacts` values are public projections, not
    // raw storage records. Strip revision-validation metadata here so every
    // projection path is safe by construction, including nested values.
    hierarchy_navigation.stripPublicInternalFieldsValue(alloc, value);

    if (obj.get("_id") == null) {
        if (obj.get("_chunk_id")) |chunk_id| {
            try putClonedValue(alloc, obj, "_id", chunk_id);
        }
    }
    if (obj.get("_start_char") == null) {
        if (obj.get("_start_offset")) |start_offset| {
            try putClonedValue(alloc, obj, "_start_char", start_offset);
        }
    }
    if (obj.get("_end_char") == null) {
        if (obj.get("_end_offset")) |end_offset| {
            try putClonedValue(alloc, obj, "_end_char", end_offset);
        }
    }
    if (obj.get("_content") == null) {
        if (findChunkContentField(obj)) |content_value| {
            try putClonedValue(alloc, obj, "_content", content_value);
        }
    }
}

fn parseSpecialFieldSelection(fields: []const []const u8) SpecialFieldSelection {
    var special: SpecialFieldSelection = .{};
    for (fields) |field| {
        if (field.len == 0 or field[0] == '-') continue;
        if (std.mem.eql(u8, field, "_artifacts") or std.mem.eql(u8, field, "_artifacts.*")) {
            special.all_artifacts = true;
        } else if (std.mem.eql(u8, field, "_chunks") or std.mem.eql(u8, field, "_chunks.*")) {
            special.all_chunks = true;
        } else if (std.mem.eql(u8, field, "_embeddings") or std.mem.eql(u8, field, "_embeddings.*")) {
            special.all_embeddings = true;
        }
    }
    return special;
}

pub fn buildFieldSelectionPlan(fields: []const []const u8, include_all_fields: bool) FieldSelectionPlan {
    return .{
        .projection = .{
            .fields = fields,
            .include_all_fields = include_all_fields,
        },
        .special = parseSpecialFieldSelection(fields),
    };
}

fn findChunkContentField(obj: *const std.json.ObjectMap) ?std.json.Value {
    if (obj.get("_source_field")) |source_field| {
        if (source_field == .string) {
            if (obj.get(source_field.string)) |content| {
                if (content == .string) return content;
            }
        }
    }

    var it = obj.iterator();
    while (it.next()) |entry| {
        if (entry.key_ptr.*.len > 0 and entry.key_ptr.*[0] == '_') continue;
        if (entry.value_ptr.* == .string) return entry.value_ptr.*;
    }
    return null;
}

fn mergeStoredDocumentWithSpecialFields(
    alloc: Allocator,
    doc_key: []const u8,
    raw: []const u8,
    special: SpecialFieldSelection,
    loader: SpecialFieldLoader,
) ![]u8 {
    var artifact_value = if (special.all_artifacts) try loader.load_artifacts(loader.ctx, alloc, doc_key) else null;
    errdefer if (artifact_value) |value| {
        var mutable = value;
        freeJsonValue(alloc, &mutable);
    };
    var chunk_value = if (special.all_chunks) try loader.load_chunks(loader.ctx, alloc, doc_key) else null;
    errdefer if (chunk_value) |value| {
        var mutable = value;
        freeJsonValue(alloc, &mutable);
    };
    var embedding_value = if (special.all_embeddings) try loader.load_embeddings(loader.ctx, alloc, doc_key) else null;
    errdefer if (embedding_value) |value| {
        var mutable = value;
        freeJsonValue(alloc, &mutable);
    };
    if (artifact_value == null and chunk_value == null and embedding_value == null) return try alloc.dupe(u8, raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();

    var root = if (parsed.value == .object)
        try cloneJsonValue(alloc, parsed.value)
    else
        std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer freeJsonValue(alloc, &root);

    if (root != .object) unreachable;
    if (artifact_value) |value| {
        try putOwnedValue(alloc, &root.object, "_artifacts", value);
        artifact_value = null;
    }
    if (chunk_value) |value| {
        try putOwnedValue(alloc, &root.object, "_chunks", value);
        chunk_value = null;
    }
    if (embedding_value) |value| {
        try putOwnedValue(alloc, &root.object, "_embeddings", value);
        embedding_value = null;
    }

    const json = try std.json.Stringify.valueAlloc(alloc, root, .{});
    freeJsonValue(alloc, &root);
    return json;
}

test "ordered artifact inventory public projections transfer nested ownership across allocation faults" {
    const Harness = struct {
        fn load(_: ?*anyopaque, alloc: Allocator, _: []const u8) !?std.json.Value {
            const parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"group\":[{\"value\":\"text\",\"nested\":[1,2,3]}]}", .{});
            defer parsed.deinit();
            return try cloneJsonValue(alloc, parsed.value);
        }
        fn project(alloc: Allocator) !void {
            const json = try projectLookupStoredBytes(alloc, "doc", "{\"title\":\"retained\"}", .{ .fields = &.{ "_artifacts", "_chunks", "_embeddings", "title", "-secret.nested" }, .include_all_fields = false }, .{
                .ctx = null,
                .load_chunks = load,
                .load_embeddings = load,
                .load_artifacts = load,
            });
            defer alloc.free(json);
            const parsed = try std.json.parseFromSlice(std.json.Value, alloc, json, .{});
            defer parsed.deinit();
            for ([_][]const u8{ "_artifacts", "_chunks", "_embeddings" }) |name| {
                try std.testing.expectEqualStrings("text", parsed.value.object.get(name).?.object.get("group").?.array.items[0].object.get("value").?.string);
            }
            try std.testing.expectEqualStrings("retained", parsed.value.object.get("title").?.string);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Harness.project, .{});
}

test "normalizeChunkArtifactForQuery strips private unit revision metadata" {
    const alloc = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(
        std.json.Value,
        alloc,
        "{\"_chunk_id\":\"chunk:1\",\"_artifact_unit_fingerprint\":\"private\",\"text\":\"visible\"}",
        .{},
    );
    defer parsed.deinit();
    var owned = try cloneJsonValue(alloc, parsed.value);
    defer freeJsonValue(alloc, &owned);

    try normalizeChunkArtifactForQuery(alloc, &owned);

    try std.testing.expect(owned.object.get("_artifact_unit_fingerprint") == null);
    try std.testing.expectEqualStrings("chunk:1", owned.object.get("_id").?.string);
    try std.testing.expectEqualStrings("visible", owned.object.get("_content").?.string);
}
