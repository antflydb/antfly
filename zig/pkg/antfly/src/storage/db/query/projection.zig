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
const Allocator = std.mem.Allocator;
const types = @import("../types.zig");
const document_query = @import("../document_query.zig");
const search_exec = @import("search_exec.zig");

pub const SpecialFieldSelection = struct {
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

pub fn freeJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .string => |s| alloc.free(s),
        .array => |*arr| {
            for (arr.items) |*item| freeJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                freeJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        },
        .number_string => |s| alloc.free(s),
        else => {},
    }
    value.* = undefined;
}

pub fn cloneJsonValue(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .bool => |b| .{ .bool = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .float = f },
        .number_string => |s| .{ .number_string = try alloc.dupe(u8, s) },
        .string => |s| .{ .string = try alloc.dupe(u8, s) },
        .array => |arr| blk: {
            var cloned = std.json.Array.init(alloc);
            errdefer {
                for (cloned.items) |*item| freeJsonValue(alloc, item);
                cloned.deinit();
            }
            for (arr.items) |item| try cloned.append(try cloneJsonValue(alloc, item));
            break :blk .{ .array = cloned };
        },
        .object => |obj| blk: {
            var cloned = std.json.ObjectMap.empty;
            errdefer {
                var it = cloned.iterator();
                while (it.next()) |entry| {
                    alloc.free(entry.key_ptr.*);
                    freeJsonValue(alloc, entry.value_ptr);
                }
                cloned.deinit(alloc);
            }
            var it = obj.iterator();
            while (it.next()) |entry| {
                try cloned.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValue(alloc, entry.value_ptr.*));
            }
            break :blk .{ .object = cloned };
        },
    };
}

pub fn putOwnedValue(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    key: []const u8,
    value: std.json.Value,
) !void {
    if (obj.getPtr(key)) |existing| {
        freeJsonValue(alloc, existing);
        existing.* = value;
        return;
    }
    try obj.put(alloc, try alloc.dupe(u8, key), value);
}

pub fn normalizeChunkArtifactForQuery(alloc: Allocator, value: *std.json.Value) !void {
    if (value.* != .object) return;
    var obj = &value.object;

    if (obj.get("_id") == null) {
        if (obj.get("_chunk_id")) |chunk_id| {
            try putOwnedValue(alloc, obj, "_id", try cloneJsonValue(alloc, chunk_id));
        }
    }
    if (obj.get("_start_char") == null) {
        if (obj.get("_start_offset")) |start_offset| {
            try putOwnedValue(alloc, obj, "_start_char", try cloneJsonValue(alloc, start_offset));
        }
    }
    if (obj.get("_end_char") == null) {
        if (obj.get("_end_offset")) |end_offset| {
            try putOwnedValue(alloc, obj, "_end_char", try cloneJsonValue(alloc, end_offset));
        }
    }
    if (obj.get("_content") == null) {
        if (findChunkContentField(obj)) |content_value| {
            try putOwnedValue(alloc, obj, "_content", try cloneJsonValue(alloc, content_value));
        }
    }
}

fn parseSpecialFieldSelection(fields: []const []const u8) SpecialFieldSelection {
    var special: SpecialFieldSelection = .{};
    for (fields) |field| {
        if (field.len == 0 or field[0] == '-') continue;
        if (std.mem.eql(u8, field, "_chunks") or std.mem.eql(u8, field, "_chunks.*")) {
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
    const chunk_value = if (special.all_chunks) try loader.load_chunks(loader.ctx, alloc, doc_key) else null;
    errdefer if (chunk_value) |value| {
        var mutable = value;
        freeJsonValue(alloc, &mutable);
    };
    const embedding_value = if (special.all_embeddings) try loader.load_embeddings(loader.ctx, alloc, doc_key) else null;
    errdefer if (embedding_value) |value| {
        var mutable = value;
        freeJsonValue(alloc, &mutable);
    };
    if (chunk_value == null and embedding_value == null) return try alloc.dupe(u8, raw);

    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();

    var root = if (parsed.value == .object)
        try cloneJsonValue(alloc, parsed.value)
    else
        std.json.Value{ .object = std.json.ObjectMap.empty };
    errdefer freeJsonValue(alloc, &root);

    if (root != .object) unreachable;
    if (chunk_value) |value| {
        try putOwnedValue(alloc, &root.object, "_chunks", value);
    }
    if (embedding_value) |value| {
        try putOwnedValue(alloc, &root.object, "_embeddings", value);
    }

    const json = try std.json.Stringify.valueAlloc(alloc, root, .{});
    freeJsonValue(alloc, &root);
    return json;
}
