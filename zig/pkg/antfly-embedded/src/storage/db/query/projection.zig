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

pub const EmbeddingFields = @import("embedding_fields.zig").EmbeddingFields;
pub const EmbeddingStream = @import("embedding_fields.zig").EmbeddingStream;
pub const SpecialFieldLoader = struct {
    open_embeddings: ?*const fn (?*anyopaque, Allocator, []const u8) anyerror!?EmbeddingStream = null,
    load_embeddings_compact: ?*const fn (?*anyopaque, Allocator, []const u8) anyerror!?EmbeddingFields = null,
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
    var plan = buildFieldSelectionPlan(opts.fields, opts.include_all_fields);
    plan.projection.output_exact = opts.output_exact;
    return plan;
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
        if (std.mem.eql(u8, field, "_artifacts") or std.mem.startsWith(u8, field, "_artifacts.")) {
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

/// The old JSON round trip normalized arbitrary-precision numeric lexemes.
/// Preserve that contract without serializing strings or ordinary scalars.
fn normalizeLoadedNumbers(alloc: Allocator, root: *std.json.Value) !void {
    const work = @import("antfly_platform").allocator.reclaimingScratch(alloc);
    var pending: std.ArrayListUnmanaged(*std.json.Value) = .empty;
    defer pending.deinit(work);
    try pending.append(work, root);
    while (pending.pop()) |value| switch (value.*) {
        .number_string => |bytes| {
            var parsed = try std.json.parseFromSlice(std.json.Value, work, bytes, .{});
            defer parsed.deinit();
            switch (parsed.value) {
                .integer, .float => {
                    alloc.free(bytes);
                    value.* = parsed.value;
                },
                .number_string => |normalized| if (!std.mem.eql(u8, normalized, bytes)) {
                    const owned = try alloc.dupe(u8, normalized);
                    alloc.free(bytes);
                    value.* = .{ .number_string = owned };
                },
                else => return error.SyntaxError,
            }
        },
        .object => |*object| for (object.values()) |*child| {
            if (child.* == .number_string or child.* == .object or child.* == .array) try pending.append(work, child);
        },
        .array => |*array| for (array.items) |*child| {
            if (child.* == .number_string or child.* == .object or child.* == .array) try pending.append(work, child);
        },
        else => {},
    };
}

fn projectWithSelectionAndOutput(alloc: Allocator, output: Allocator, doc_key: []const u8, raw: []const u8, selection: FieldSelectionPlan, loader: SpecialFieldLoader, prepared: ?*const document_query.SpanProjectionPlan) ![]u8 {
    var values: [3]?std.json.Value = @splat(null);
    defer for (&values) |*value| if (value.*) |*owned| freeJsonValue(alloc, owned);
    const names = [_][]const u8{ "_artifacts", "_chunks", "_embeddings" };
    if (selection.special.all_artifacts) values[0] = try loader.load_artifacts(loader.ctx, alloc, doc_key);
    if (selection.special.all_chunks) values[1] = try loader.load_chunks(loader.ctx, alloc, doc_key);
    var stream: ?EmbeddingStream = null;
    defer if (stream) |*fields| fields.deinit();
    var compact: ?EmbeddingFields = null;
    defer if (compact) |*fields| fields.deinit(alloc);
    // Unsupported legacy selectors keep the established DOM contract.
    if (selection.special.all_embeddings) {
        if (loader.open_embeddings != null and document_query.spanProjectionSupported(selection.projection.fields)) {
            stream = try loader.open_embeddings.?(loader.ctx, alloc, doc_key);
        } else if (loader.load_embeddings_compact != null and document_query.spanProjectionSupported(selection.projection.fields)) {
            compact = try loader.load_embeddings_compact.?(loader.ctx, alloc, doc_key);
        } else values[2] = try loader.load_embeddings(loader.ctx, alloc, doc_key);
    }
    for (&values) |*value| if (value.*) |*logical| try normalizeLoadedNumbers(alloc, logical);
    var sources: [3]document_query.ProjectionSource = undefined;
    var count: usize = 0;
    for (values, names) |value, name| if (value) |logical| {
        sources[count] = .{ .name = name, .value = logical };
        count += 1;
    };
    if (stream) |*fields| {
        sources[count] = .{ .name = "_embeddings", .embedding_stream = fields };
        count += 1;
    }
    if (compact) |*fields| {
        sources[count] = .{ .name = "_embeddings", .embeddings = fields };
        count += 1;
    }
    if (prepared) |plan| return (try document_query.lookupJsonWithSourcesPolicy(alloc, output, raw, plan, sources[0..count], selection.projection.output_exact)).json;
    if (try document_query.SpanProjectionPlan.init(alloc, selection.projection)) |owned| {
        var plan = owned;
        defer plan.deinit();
        return (try document_query.lookupJsonWithSourcesPolicy(alloc, output, raw, &plan, sources[0..count], selection.projection.output_exact)).json;
    }
    if (count == 0) return (try document_query.lookupJsonWithOutput(alloc, output, raw, selection.projection)).json;
    // Legacy selectors still decode the primary, but loaded special fields
    // remain logical values throughout projection. No intermediate JSON body.
    var arena = std.heap.ArenaAllocator.init(@import("antfly_platform").allocator.reclaimingScratch(alloc));
    defer arena.deinit();
    const scratch = arena.allocator();
    var root = try std.json.parseFromSliceLeaky(std.json.Value, scratch, raw, .{});
    if (root != .object) root = .{ .object = .empty };
    for (sources[0..count]) |source| try root.object.put(scratch, source.name, source.value);
    var projected = try document_query.projectLookupView(scratch, root, selection.projection);
    defer document_query.deinitProjectionView(scratch, &projected);
    return document_query.serializeWithOutput(scratch, output, projected);
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

test "public graph metric borrowed wire decoded special fields preserve numeric and ordered selector contracts" {
    const Fixture = struct {
        fn load(_: ?*anyopaque, a: Allocator, _: []const u8) !?std.json.Value {
            var parsed = try std.json.parseFromSlice(std.json.Value, a, "{\"n\":1.00,\"big\":123456789012345678901234567890,\"arr\":[{\"x\":1e2,\"y\":\"escaped\\ntext\"},{\"x\":2}],\"nested\":{\"a\":3}}", .{ .parse_numbers = false });
            defer parsed.deinit();
            return try cloneJsonValue(a, parsed.value);
        }
        fn none(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return null;
        }
        fn run(a: Allocator) !void {
            const loader: SpecialFieldLoader = .{ .ctx = null, .load_embeddings = load, .load_chunks = none, .load_artifacts = none };
            const raw = "{\"title\":\"retained\",\"_embeddings\":{\"old\":0}}";
            const merged = "{\"title\":\"retained\",\"_embeddings\":{\"n\":1.00,\"big\":123456789012345678901234567890,\"arr\":[{\"x\":1e2,\"y\":\"escaped\\ntext\"},{\"x\":2}],\"nested\":{\"a\":3}}}";
            const cases = [_][]const []const u8{
                &.{"_embeddings"},                                                  &.{"_embeddings.*"},
                &.{ "_embeddings", "_embeddings.arr.1.x", "_embeddings.nested.a" }, &.{ "_embeddings", "_embeddings.arr.x", "_embeddings.arr.0" },
                &.{ "_embeddings", "-_embeddings.nested", "title" },                &.{ "*", "_embeddings", "-_embeddings.arr.0.y" },
                &.{ "_embeddings", "-_embeddings" },
            };
            for (cases) |fields| {
                const opts: types.LookupOptions = .{ .fields = fields, .include_all_fields = false };
                var expected = try document_query.lookupJson(a, merged, opts);
                defer expected.deinit(a);
                const result = try projectLookupStoredBytesWithOutput(a, a, "doc", raw, opts, loader);
                defer a.free(result);
                try std.testing.expectEqualStrings(expected.json, result);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "public graph metric borrowed wire decoded embedding projection fits bounded row scratch" {
    const a = std.testing.allocator;
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    const Loader = struct {
        fn none(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return null;
        }
        fn load(ctx: ?*anyopaque, alloc: Allocator, _: []const u8) !?std.json.Value {
            const value: *const std.json.Value = @ptrCast(@alignCast(ctx.?));
            return try cloneJsonValue(alloc, value.*);
        }
    };
    var source_arena = std.heap.ArenaAllocator.init(a);
    defer source_arena.deinit();
    const s = source_arena.allocator();
    var vectors: std.json.ObjectMap = .empty;
    for (0..64) |i| {
        var array = std.json.Array.init(s);
        try array.ensureTotalCapacity(1536);
        for (0..1536) |j| try array.append(.{ .float = @as(f64, @as(f32, @floatFromInt(j)) / 1536) });
        try vectors.put(s, try std.fmt.allocPrint(s, "v{d}", .{i}), .{ .array = array });
    }
    var logical: std.json.Value = .{ .object = vectors };
    const loader: SpecialFieldLoader = .{ .ctx = &logical, .load_embeddings = Loader.load, .load_chunks = Loader.none, .load_artifacts = Loader.none };
    var budget: Counter = .{ .backing = a, .limit = 32 * 1024 * 1024 };
    const result = result: {
        var arena = std.heap.ArenaAllocator.init(budget.allocator());
        defer arena.deinit();
        break :result try projectLookupStoredBytesWithOutput(arena.allocator(), a, "doc", "{}", .{ .fields = &.{"_embeddings"}, .include_all_fields = false }, loader);
    };
    defer a.free(result);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, result, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 64), parsed.value.object.get("_embeddings").?.object.count());
    try std.testing.expectEqual(@as(usize, 1536), parsed.value.object.get("_embeddings").?.object.get("v63").?.array.items.len);
    std.debug.print("DECODED_EMBEDDINGS output_bytes={d} scratch_peak={d}\n", .{ result.len, budget.peak });
}

test "public graph metric borrowed wire compact embeddings preserve float spelling wildcard order and legacy fallback" {
    const Fixture = struct {
        fn none(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return null;
        }
        fn compact(_: ?*anyopaque, a: Allocator, _: []const u8) !?EmbeddingFields {
            var fields: EmbeddingFields = .{};
            errdefer fields.deinit(a);
            const vector = try a.dupe(f32, &.{ 0.1, -0.0, 1.25, 1e30 });
            errdefer a.free(vector);
            try fields.putOwned(a, "dense", vector);
            return fields;
        }
        fn logical(ctx: ?*anyopaque, a: Allocator, key: []const u8) !?std.json.Value {
            var fields = (try compact(ctx, a, key)).?;
            defer fields.deinit(a);
            return try fields.materialize(a);
        }
        fn run(a: Allocator) !void {
            const old: SpecialFieldLoader = .{ .ctx = null, .load_embeddings = logical, .load_chunks = none, .load_artifacts = none };
            var new = old;
            new.load_embeddings_compact = compact;
            for ([_][]const []const u8{
                &.{"_embeddings"},            &.{"_embeddings.*"},                 &.{ "_embeddings", "_embeddings.dense.2" },
                &.{ "_embeddings", "title" }, &.{ "_embeddings", "-_embeddings" }, &.{ "_embeddings", "-title.nested" },
            }) |fields| {
                const opts: types.LookupOptions = .{ .fields = fields, .include_all_fields = false };
                const expected = try projectLookupStoredBytesWithOutput(a, a, "doc", "{\"title\":{\"nested\":1},\"_embeddings\":{\"old\":[]}}", opts, old);
                defer a.free(expected);
                const actual = try projectLookupStoredBytesWithOutput(a, a, "doc", "{\"title\":{\"nested\":1},\"_embeddings\":{\"old\":[]}}", opts, new);
                defer a.free(actual);
                try std.testing.expectEqualStrings(expected, actual);
            }
        }
    };
    try Fixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "public graph metric borrowed wire compact embeddings bound numeric scratch" {
    const Loader = struct {
        fn none(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return null;
        }
        fn compact(_: ?*anyopaque, a: Allocator, _: []const u8) !?EmbeddingFields {
            var fields: EmbeddingFields = .{};
            errdefer fields.deinit(a);
            for (0..64) |i| {
                const vector = try a.alloc(f32, 1536);
                errdefer a.free(vector);
                for (vector, 0..) |*v, j| v.* = @as(f32, @floatFromInt(j)) / 1536;
                const name = try std.fmt.allocPrint(a, "v{d}", .{i});
                defer a.free(name);
                try fields.putOwned(a, name, vector);
            }
            return fields;
        }
    };
    const a = std.testing.allocator;
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var budget: Counter = .{ .backing = a, .limit = 1024 * 1024 };
    const result = try projectLookupStoredBytesWithOutput(budget.allocator(), a, "doc", "{}", .{ .fields = &.{"_embeddings"}, .include_all_fields = false }, .{
        .ctx = null,
        .load_embeddings = Loader.none,
        .load_embeddings_compact = Loader.compact,
        .load_chunks = Loader.none,
        .load_artifacts = Loader.none,
    });
    defer a.free(result);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("COMPACT_EMBEDDINGS output_bytes={d} scratch_peak={d}\n", .{ result.len, budget.peak });
}

test "public graph metric borrowed wire streamed embeddings bound slabs and preserve ordered fallback" {
    const Fixture = struct {
        owner: Allocator,
        index: usize = 0,
        field_count: usize = 64,
        vector: [1536]f32 = undefined,
        name: [32]u8 = undefined,
        fn none(_: ?*anyopaque, _: Allocator, _: []const u8) !?std.json.Value {
            return null;
        }
        fn open(_: ?*anyopaque, a: Allocator, _: []const u8) !?EmbeddingStream {
            const owner = @import("antfly_platform").allocator.reclaimingScratch(a);
            const state = try owner.create(@This());
            state.* = .{ .owner = owner };
            return .{ .ctx = state, .next_fn = next, .reset_fn = reset, .close_fn = close };
        }
        fn next(ctx: *anyopaque) anyerror!?EmbeddingStream.Field {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (self.index == self.field_count) return null;
            const name = try std.fmt.bufPrint(&self.name, "v{d}", .{self.index});
            for (&self.vector, 0..) |*v, i| v.* = @as(f32, @floatFromInt(i)) / 1536;
            self.index += 1;
            return .{ .name = name, .vector = &self.vector };
        }
        fn reset(ctx: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.index = 0;
        }
        fn close(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.owner.destroy(self);
        }
        fn compact(ctx: ?*anyopaque, a: Allocator, key: []const u8) !?EmbeddingFields {
            var stream = (try open(ctx, a, key)).?;
            defer stream.deinit();
            var result = (try stream.fields(a)).*;
            stream.cached = null;
            errdefer result.deinit(@import("antfly_platform").allocator.reclaimingScratch(a));
            return result;
        }
        fn run(a: Allocator) !void {
            const loader: SpecialFieldLoader = .{ .ctx = null, .load_embeddings = none, .load_chunks = none, .load_artifacts = none, .open_embeddings = open };
            var eager = loader;
            eager.open_embeddings = null;
            eager.load_embeddings_compact = compact;
            for ([_][]const []const u8{
                &.{"_embeddings"},                   &.{"_embeddings.*"},          &.{ "_embeddings", "_embeddings.v0.2" },
                &.{ "_embeddings", "-_embeddings" }, &.{ "_embeddings", "title" }, &.{ "_embeddings", "*" },
            }) |fields| {
                const opts: types.LookupOptions = .{ .fields = fields, .include_all_fields = false };
                const raw = "{\"title\":1,\"_embeddings\":{\"old\":[]}}";
                const expected = try projectLookupStoredBytesWithOutput(a, a, "doc", raw, opts, eager);
                defer a.free(expected);
                const actual = try projectLookupStoredBytesWithOutput(a, a, "doc", raw, opts, loader);
                defer a.free(actual);
                try std.testing.expectEqualStrings(expected, actual);
            }
        }
        fn failure(a: Allocator) !void {
            const body = try projectLookupStoredBytesWithOutput(a, a, "doc", "{}", .{ .fields = &.{ "_embeddings", "_embeddings.v0.2" }, .include_all_fields = false }, .{ .ctx = null, .load_embeddings = none, .load_chunks = none, .load_artifacts = none, .open_embeddings = open });
            defer a.free(body);
        }
    };
    const a = std.testing.allocator;
    try Fixture.run(a);
    // Keep fault enumeration small while still exercising post-navigation
    // cache ownership after the short-lived projection arena has been retired.
    const Small = struct {
        fn run(failing: Allocator) !void {
            var stream = (try Fixture.open(null, failing, "doc")).?;
            defer stream.deinit();
            const state: *Fixture = @ptrCast(@alignCast(stream.ctx));
            state.field_count = 2;
            var arena: std.heap.ArenaAllocator = .init(failing);
            _ = stream.fields(arena.allocator()) catch |err| {
                arena.deinit();
                return err;
            };
            arena.deinit();
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Small.run, .{});
    const Counter = @import("../../lite/test_allocator.zig").BudgetAllocator;
    var budget: Counter = .{ .backing = a, .limit = 64 * 1024 };
    const body = try projectLookupStoredBytesWithOutput(budget.allocator(), a, "doc", "{}", .{ .fields = &.{"_embeddings"}, .include_all_fields = false }, .{ .ctx = null, .load_embeddings = Fixture.none, .load_chunks = Fixture.none, .load_artifacts = Fixture.none, .open_embeddings = Fixture.open });
    defer a.free(body);
    try std.testing.expectEqual(@as(usize, 1609735), body.len);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    std.debug.print("STREAMED_EMBEDDINGS output_bytes={d} scratch_peak={d}\n", .{ body.len, budget.peak });
}
