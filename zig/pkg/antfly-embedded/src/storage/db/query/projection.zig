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
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
    defer arena.deinit();
    return projectWithSelectionAndOutput(arena.allocator(), alloc, doc_key, raw, plan, loader, null);
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

pub fn projectLookupStoredBytesWithOutput(alloc: Allocator, output: Allocator, doc_key: []const u8, raw: []const u8, opts: types.LookupOptions, loader: SpecialFieldLoader) ![]u8 {
    return projectLookupStoredBytesWithPlanAndOutput(alloc, output, doc_key, raw, opts, loader, null);
}
pub fn projectLookupStoredBytesWithPlanAndOutput(alloc: Allocator, output: Allocator, doc_key: []const u8, raw: []const u8, opts: types.LookupOptions, loader: SpecialFieldLoader, plan: ?*const document_query.SpanProjectionPlan) ![]u8 {
    return projectWithSelectionAndOutput(alloc, output, doc_key, raw, buildLookupFieldSelectionPlan(opts), loader, plan);
}

pub fn projectStoredBytesForSearch(
    alloc: Allocator,
    req: types.SearchRequest,
    doc_key: []const u8,
    raw: []const u8,
    loader: SpecialFieldLoader,
) ![]u8 {
    if (!shouldProjectSearchStored(req)) return try alloc.dupe(u8, raw);
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
    defer arena.deinit();
    const opts = search_exec.searchLookupOptions(req);
    const prepared = if (req.document_projection_plan) |shared| (if (shared.matches(opts)) shared.plan else null) else null;
    return projectWithSelectionAndOutput(arena.allocator(), alloc, doc_key, raw, buildSearchFieldSelectionPlan(req), loader, prepared);
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
    return projectStoredBytesForSearch(alloc, req, doc_key, raw, loader);
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

fn projectWithSelectionAndOutput(alloc: Allocator, output: Allocator, doc_key: []const u8, raw: []const u8, selection: FieldSelectionPlan, loader: SpecialFieldLoader, prepared: ?*const document_query.SpanProjectionPlan) ![]u8 {
    var values: [3]?std.json.Value = @splat(null);
    defer for (&values) |*value| if (value.*) |*owned| freeJsonValue(alloc, owned);
    const names = [_][]const u8{ "_artifacts", "_chunks", "_embeddings" };
    if (selection.special.all_artifacts) values[0] = try loader.load_artifacts(loader.ctx, alloc, doc_key);
    if (selection.special.all_chunks) values[1] = try loader.load_chunks(loader.ctx, alloc, doc_key);
    if (selection.special.all_embeddings) values[2] = try loader.load_embeddings(loader.ctx, alloc, doc_key);
    var sources: [3]document_query.ProjectionSource = undefined;
    var count: usize = 0;
    defer for (sources[0..count]) |source| alloc.free(source.raw);
    for (values, names) |value, name| if (value) |logical| {
        const bytes = try std.json.Stringify.valueAlloc(alloc, logical, .{});
        sources[count] = .{ .name = name, .raw = bytes };
        count += 1;
    };
    if (prepared) |plan| return (try document_query.lookupJsonWithSources(alloc, output, raw, plan, sources[0..count])).json;
    if (try document_query.SpanProjectionPlan.init(alloc, selection.projection)) |owned| {
        var plan = owned;
        defer plan.deinit();
        return (try document_query.lookupJsonWithSources(alloc, output, raw, &plan, sources[0..count])).json;
    }
    if (count == 0) return (try document_query.lookupJsonWithOutput(alloc, output, raw, selection.projection)).json;
    // Unsupported wildcard/exclusion forms keep the DOM projection contract,
    // but their input overlay is streamed from raw spans, never parsed/cloned.
    const merged = try mergeProjectionSources(alloc, raw, sources[0..count]);
    defer alloc.free(merged);
    return (try document_query.lookupJsonWithOutput(alloc, output, merged, selection.projection)).json;
}
fn mergeProjectionSources(alloc: Allocator, raw: []const u8, sources: []const document_query.ProjectionSource) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const JsonView = @import("json_view.zig").View;
    var view = JsonView.init(a, raw);
    const selector = try @import("json_projection_plan.zig").compile(a, &.{});
    try view.prepareProjectionWithOptions(selector, .all, .object_only, .{ .retain_root_fields = true });
    var writer = std.Io.Writer.Allocating.init(alloc);
    defer writer.deinit();
    writer.writer.writeByte('{') catch return error.OutOfMemory;
    var emitted = false;
    if (view.root.children) |children| {
        const names = try a.alloc([]const u8, children.len);
        var it = view.root.object_index.iterator();
        while (it.next()) |entry| names[entry.value_ptr.*] = entry.key_ptr.*;
        for (names, children) |name, child| {
            const bytes = for (sources) |source| {
                if (std.mem.eql(u8, name, source.name)) break source.raw;
            } else child.node.raw;
            if (emitted) writer.writer.writeByte(',') catch return error.OutOfMemory;
            emitted = true;
            std.json.Stringify.value(name, .{}, &writer.writer) catch return error.OutOfMemory;
            writer.writer.writeByte(':') catch return error.OutOfMemory;
            writer.writer.writeAll(bytes) catch return error.OutOfMemory;
        }
    }
    for (sources) |source| {
        if (view.root.object_index.contains(source.name)) continue;
        if (emitted) writer.writer.writeByte(',') catch return error.OutOfMemory;
        emitted = true;
        std.json.Stringify.value(source.name, .{}, &writer.writer) catch return error.OutOfMemory;
        writer.writer.writeByte(':') catch return error.OutOfMemory;
        writer.writer.writeAll(source.raw) catch return error.OutOfMemory;
    }
    writer.writer.writeByte('}') catch return error.OutOfMemory;
    return writer.toOwnedSlice();
}

test "ordered artifact inventory public projections transfer nested ownership across allocation faults" {
    const Harness = struct {
        fn load(_: ?*anyopaque, alloc: Allocator, _: []const u8) !?std.json.Value {
            const parsed = try std.json.parseFromSlice(std.json.Value, alloc, "{\"group\":[{\"value\":\"text\",\"nested\":[1,2,3]}]}", .{});
            defer parsed.deinit();
            return try cloneJsonValue(alloc, parsed.value);
        }
        fn project(backing: Allocator) !void {
            // In-place growth depends on OS mapping state. Enumerate every
            // allocation failure through the deterministic allocate/copy path.
            var vtable = backing.vtable.*;
            vtable.resize = Allocator.noResize;
            vtable.remap = Allocator.noRemap;
            const alloc: Allocator = .{ .ptr = backing.ptr, .vtable = &vtable };
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

test "public graph metric borrowed wire special projection sources preserve overlays and failure ownership" {
    const Fixture = struct {
        fn embeddings(_: ?*anyopaque, a: Allocator, _: []const u8) !?std.json.Value {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"a\":[1,2],\"nested\":{\"z\":3}}", .{});
            defer parsed.deinit();
            return try cloneJsonValue(a, parsed.value);
        }
        fn chunks(_: ?*anyopaque, a: Allocator, _: []const u8) !?std.json.Value {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"g\":[{\"id\":7},3]}", .{});
            defer parsed.deinit();
            return try cloneJsonValue(a, parsed.value);
        }
        fn none(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return null;
        }
        fn run(a: Allocator) !void {
            const loader: SpecialFieldLoader = .{ .ctx = null, .load_embeddings = embeddings, .load_chunks = chunks, .load_artifacts = none };
            const raw = "{\"title\":{\"n\":1e2,\"secret\":2},\"_embeddings\":{\"old\":0},\"omit\":[1,2]}";
            const merged = "{\"title\":{\"n\":1e2,\"secret\":2},\"_embeddings\":{\"a\":[1,2],\"nested\":{\"z\":3}},\"omit\":[1,2],\"_chunks\":{\"g\":[{\"id\":7},3]}}";
            const selections = [_][]const []const u8{
                &.{ "_embeddings", "_chunks", "title.n" },
                &.{ "_embeddings.*", "_chunks.*", "title" },
                &.{ "*", "_embeddings", "_chunks" },
                &.{ "_embeddings", "_chunks", "title", "-title.secret" },
                &.{ "_embeddings", "_chunks", "_embeddings.nested.z" },
                &.{ "_embeddings", "-_embeddings", "_chunks" },
            };
            for (selections) |fields| {
                const opts: types.LookupOptions = .{ .fields = fields, .include_all_fields = false };
                var expected = try document_query.lookupJson(a, merged, opts);
                defer expected.deinit(a);
                const result = try projectLookupStoredBytesWithOutput(a, a, "doc", raw, opts, loader);
                defer a.free(result);
                try std.testing.expectEqualStrings(expected.json, result);
            }
            for ([_][]const u8{ "42", "[1,2]", "null" }) |nonobject| {
                const result = try projectLookupStoredBytesWithOutput(a, a, "doc", nonobject, .{ .fields = &.{ "_embeddings", "_chunks" }, .include_all_fields = false }, loader);
                defer a.free(result);
                try std.testing.expectEqualStrings("{\"_embeddings\":{\"a\":[1,2],\"nested\":{\"z\":3}},\"_chunks\":{\"g\":[{\"id\":7},3]}}", result);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "public graph metric borrowed wire local search shares plans and bounds synthetic projection scratch" {
    const a = std.testing.allocator;
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    const Loader = struct {
        fn none(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return null;
        }
        fn small(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return .{ .integer = 1 };
        }
    };
    const loader: SpecialFieldLoader = .{ .ctx = null, .load_chunks = Loader.none, .load_artifacts = Loader.none, .load_embeddings = Loader.small };
    var input = std.Io.Writer.Allocating.init(a);
    defer input.deinit();
    try input.writer.writeAll("{\"keep\":1,\"omit\":[");
    for (0..20000) |i| {
        if (i != 0) try input.writer.writeByte(',');
        try input.writer.writeAll("{\"n\":0,\"s\":\"x\"}");
    }
    try input.writer.writeAll("]}");
    for ([_][]const []const u8{ &.{"keep"}, &.{ "keep", "_embeddings" }, &.{ "keep", "_embeddings.*" } }) |fields| {
        var plan = (try document_query.SpanProjectionPlan.init(a, .{ .fields = fields, .include_all_fields = false })).?;
        defer plan.deinit();
        const before = plan.arena.queryCapacity();
        const req: types.SearchRequest = .{ .query = .{ .match_all = {} }, .fields = fields, .include_all_fields = false, .document_projection_plan = .{ .plan = &plan, .fields = fields, .include_all_fields = false } };
        var counter: Counter = .{ .backing = a, .limit = 64 * 1024 };
        for (0..8) |_| {
            const result = try projectStoredBytesForSearch(counter.allocator(), req, "doc", input.written(), loader);
            defer counter.allocator().free(result);
            try std.testing.expectEqualStrings(if (fields.len == 1 or std.mem.endsWith(u8, fields[1], ".*")) "{\"keep\":1}" else "{\"keep\":1,\"_embeddings\":1}", result);
        }
        try std.testing.expectEqual(before, plan.arena.queryCapacity());
        try std.testing.expectEqual(@as(usize, 0), counter.live);
        std.debug.print("LOCAL_SPECIAL_FIXED fields={d} scratch_peak={d}\n", .{ fields.len, counter.peak });
        try std.testing.expect(!req.document_projection_plan.?.matches(.{ .fields = &.{"different"}, .include_all_fields = false }));
        try std.testing.expect(!req.document_projection_plan.?.matches(.{ .fields = fields, .include_all_fields = true }));
    }
}
