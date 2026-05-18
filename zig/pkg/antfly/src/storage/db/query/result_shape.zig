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
const artifact_ids = @import("../artifact_ids.zig");
const internal_keys = @import("../../internal_keys.zig");
const graph_exec = @import("graph_exec.zig");

pub const VisibleHitEvaluator = struct {
    ctx: ?*anyopaque,
    func: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        hit: types.SearchHit,
    ) anyerror!bool,
    filter_many: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        hits: []const types.SearchHit,
    ) anyerror![]bool = null,
};

pub const ChunkParentResultShaper = struct {
    ctx: ?*anyopaque,
    resolve_parent_id: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        hit: types.SearchHit,
    ) anyerror![]u8,
    load_parent_stored: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        parent_id: []const u8,
    ) anyerror!?[]u8,
    load_parent_stored_many: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        parent_ids: []const []const u8,
    ) anyerror![]?[]u8 = null,
};

pub const ChunkParentResolver = struct {
    ctx: ?*anyopaque,
    load_stored: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
    ) anyerror!?[]u8,
};

pub const SearchHitVisibilityEvaluator = struct {
    ctx: ?*anyopaque,
    load_stored: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
    ) anyerror!?[]u8,
    is_expired_key: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
    ) anyerror!bool,
};

pub const StoredPatternFilterExecutor = struct {
    ctx: ?*anyopaque,
    load_stored: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
    ) anyerror!?[]u8,
    load_many_stored: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        keys: []const []const u8,
    ) anyerror![]?[]u8 = null,
};

pub const SearchResultPostprocessor = struct {
    ctx: ?*anyopaque,
    is_visible: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        hit: types.SearchHit,
    ) anyerror!bool,
    filter_visible_many: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        hits: []const types.SearchHit,
    ) anyerror![]bool = null,
    resolve_parent_id: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        hit: types.SearchHit,
    ) anyerror![]u8,
    load_parent_stored: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        parent_id: []const u8,
    ) anyerror!?[]u8,
    load_stored: *const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        key: []const u8,
    ) anyerror!?[]u8,
    load_many_stored: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        keys: []const []const u8,
    ) anyerror![]?[]u8 = null,
    load_many_parent_stored: ?*const fn (
        ctx: ?*anyopaque,
        alloc: Allocator,
        req: types.SearchRequest,
        parent_ids: []const []const u8,
    ) anyerror![]?[]u8 = null,
};

pub fn externalizeSearchResultArtifactIds(alloc: Allocator, result: *types.SearchResult) !void {
    for (result.hits) |*hit| {
        try externalizeSearchHitIdentity(alloc, hit);
    }
    for (result.graph_results) |*graph_result| {
        for (graph_result.hits) |*hit| {
            try externalizeSearchHitIdentity(alloc, hit);
        }
    }
}

pub fn dedupeSearchHitsById(alloc: Allocator, result: *types.SearchResult) !void {
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);

    var deduped = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (deduped.items) |*hit| hit.deinit(alloc);
        deduped.deinit(alloc);
    }

    for (result.hits) |hit| {
        const gop = try seen.getOrPut(alloc, hit.id);
        if (gop.found_existing) continue;
        try deduped.append(alloc, try hit.clone(alloc));
    }

    const owned_hits = try alloc.dupe(types.SearchHit, deduped.items);
    deduped.deinit(alloc);

    for (result.hits) |*hit| hit.deinit(alloc);
    if (result.hits.len > 0) alloc.free(result.hits);
    result.hits = owned_hits;
    result.total_hits = @intCast(result.hits.len);
}

pub fn filterVisibleSearchResult(
    alloc: Allocator,
    raw: types.SearchResult,
    evaluator: VisibleHitEvaluator,
) !types.SearchResult {
    var kept = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (kept.items) |*hit| hit.deinit(alloc);
        kept.deinit(alloc);
    }

    var owned = raw;
    errdefer owned.deinit();

    const keep_mask = if (evaluator.filter_many) |filter_many|
        try filter_many(evaluator.ctx, alloc, owned.hits)
    else
        null;
    defer if (keep_mask) |mask| alloc.free(mask);

    for (owned.hits, 0..) |*hit, i| {
        const keep = if (keep_mask) |mask|
            mask[i]
        else
            try evaluator.func(evaluator.ctx, alloc, hit.*);
        if (keep) {
            try kept.append(alloc, hit.*);
        } else {
            hit.deinit(alloc);
        }
    }

    alloc.free(owned.hits);
    owned.hits = try kept.toOwnedSlice(alloc);
    owned.total_hits = @intCast(owned.hits.len);
    return owned;
}

pub fn isVisibleSearchHit(
    alloc: Allocator,
    hit: types.SearchHit,
    evaluator: SearchHitVisibilityEvaluator,
) !bool {
    if (resolveChunkParentId(alloc, hit, .{
        .ctx = @constCast(&evaluator),
        .load_stored = loadStoredForVisibleHit,
    })) |parent_id| {
        defer alloc.free(parent_id);
        return !(try evaluator.is_expired_key(evaluator.ctx, alloc, parent_id));
    } else |err| switch (err) {
        error.InvalidChunkArtifact, error.StoredDocMissing => {},
        else => return err,
    }
    return !(try evaluator.is_expired_key(evaluator.ctx, alloc, hit.id));
}

pub fn reshapeChunkBackedResult(
    alloc: Allocator,
    req: types.SearchRequest,
    raw: types.SearchResult,
    shaper: ChunkParentResultShaper,
) !types.SearchResult {
    if (req.return_mode == .chunk) return raw;

    var grouped = std.StringHashMapUnmanaged(usize).empty;
    defer grouped.deinit(alloc);
    var parents = std.ArrayListUnmanaged(types.SearchHit).empty;
    errdefer {
        for (parents.items) |*hit| hit.deinit(alloc);
        parents.deinit(alloc);
    }

    for (raw.hits) |chunk_hit| {
        const parent_id = try shaper.resolve_parent_id(shaper.ctx, alloc, chunk_hit);
        defer alloc.free(parent_id);

        const gop = try grouped.getOrPut(alloc, parent_id);
        if (!gop.found_existing) {
            try parents.append(alloc, .{
                .id = try alloc.dupe(u8, parent_id),
                .score = chunk_hit.score,
                .stored_data = null,
                .chunk_hits = &.{},
            });
            gop.key_ptr.* = parents.items[parents.items.len - 1].id;
            gop.value_ptr.* = parents.items.len - 1;
        }

        const parent_hit = &parents.items[gop.value_ptr.*];
        if (parent_hit.score == null or (chunk_hit.score != null and chunk_hit.score.? > parent_hit.score.?)) {
            parent_hit.score = chunk_hit.score;
        }
        if (req.return_mode == .parent_with_chunks) {
            if (req.max_chunks_per_parent > 0 and parent_hit.chunk_hits.len >= req.max_chunks_per_parent) {
                continue;
            }
            var chunks = std.ArrayListUnmanaged(types.ChunkHit).fromOwnedSlice(parent_hit.chunk_hits);
            errdefer {
                for (chunks.items) |*chunk| chunk.deinit(alloc);
                chunks.deinit(alloc);
            }
            try chunks.append(alloc, .{
                .id = try alloc.dupe(u8, chunk_hit.id),
                .score = chunk_hit.score,
                .stored_data = if (chunk_hit.stored_data) |stored| try alloc.dupe(u8, stored) else null,
            });
            parent_hit.chunk_hits = try chunks.toOwnedSlice(alloc);
        }
    }

    if (req.include_stored and parents.items.len > 0) {
        try loadParentStoredForGroupedHits(alloc, req, &parents, shaper);
    }
    try normalizeGroupedParentHitOrder(alloc, &parents);

    var out = raw;
    defer out.deinit();
    const parent_count: u32 = @intCast(parents.items.len);
    const owned_hits = try paginateParentChunkHits(alloc, &parents, req.offset, req.limit);
    return .{
        .alloc = alloc,
        .hits = owned_hits,
        .total_hits = parent_count,
        .total_hits_relation = raw.total_hits_relation,
        .graph_results = &.{},
    };
}

fn normalizeGroupedParentHitOrder(
    alloc: Allocator,
    parents: *std.ArrayListUnmanaged(types.SearchHit),
) !void {
    if (parents.items.len < 2) return;

    const original_ordinals = try alloc.alloc(usize, parents.items.len);
    defer alloc.free(original_ordinals);
    for (original_ordinals, 0..) |*ordinal, i| ordinal.* = i;

    var i: usize = 1;
    while (i < parents.items.len) : (i += 1) {
        var j = i;
        while (j > 0 and groupedParentHitLess(
            parents.items[j],
            original_ordinals[j],
            parents.items[j - 1],
            original_ordinals[j - 1],
        )) : (j -= 1) {
            std.mem.swap(types.SearchHit, &parents.items[j], &parents.items[j - 1]);
            std.mem.swap(usize, &original_ordinals[j], &original_ordinals[j - 1]);
        }
    }
}

fn groupedParentHitLess(
    left: types.SearchHit,
    left_ordinal: usize,
    right: types.SearchHit,
    right_ordinal: usize,
) bool {
    if (searchHitScoresEqual(left.score, right.score)) {
        return std.mem.order(u8, left.id, right.id) == .lt;
    }
    return left_ordinal < right_ordinal;
}

fn searchHitScoresEqual(left: ?f32, right: ?f32) bool {
    if (left == null and right == null) return true;
    if (left == null or right == null) return false;
    return left.? == right.?;
}

fn loadParentStoredForGroupedHits(
    alloc: Allocator,
    req: types.SearchRequest,
    parents: *std.ArrayListUnmanaged(types.SearchHit),
    shaper: ChunkParentResultShaper,
) !void {
    const parent_ids = try alloc.alloc([]const u8, parents.items.len);
    defer alloc.free(parent_ids);
    for (parents.items, 0..) |hit, i| parent_ids[i] = hit.id;

    if (shaper.load_parent_stored_many) |load_many| {
        var loaded = try load_many(shaper.ctx, alloc, req, parent_ids);
        defer freeOptionalOwnedBytes(alloc, loaded);
        for (parents.items, 0..) |*hit, i| {
            if (loaded[i]) |stored| {
                hit.stored_data = stored;
                loaded[i] = null;
            }
        }
        return;
    }

    for (parents.items) |*hit| {
        hit.stored_data = try shaper.load_parent_stored(shaper.ctx, alloc, req, hit.id);
    }
}

pub fn applyStoredSearchPatternFilters(
    alloc: Allocator,
    req: types.SearchRequest,
    result: types.SearchResult,
    executor: StoredPatternFilterExecutor,
) !types.SearchResult {
    if (req.filter_query_json.len == 0 and req.exclusion_query_json.len == 0) return result;

    var filter_query = if (req.filter_query_json.len > 0)
        try std.json.parseFromSlice(std.json.Value, alloc, req.filter_query_json, .{})
    else
        null;
    defer if (filter_query) |*parsed| parsed.deinit();

    var exclusion_query = if (req.exclusion_query_json.len > 0)
        try std.json.parseFromSlice(std.json.Value, alloc, req.exclusion_query_json, .{})
    else
        null;
    defer if (exclusion_query) |*parsed| parsed.deinit();

    var matcher_arena = std.heap.ArenaAllocator.init(alloc);
    defer matcher_arena.deinit();
    const matcher_alloc = matcher_arena.allocator();

    const compiled_filter = if (filter_query) |parsed|
        try graph_exec.compilePatternFilter(matcher_alloc, parsed.value)
    else
        null;
    const compiled_exclusion = if (exclusion_query) |parsed|
        try graph_exec.compilePatternFilter(matcher_alloc, parsed.value)
    else
        null;

    const filter_needs_stored = if (compiled_filter) |compiled| compiled.needsStoredDoc() else false;
    const exclusion_needs_stored = if (compiled_exclusion) |compiled| compiled.needsStoredDoc() else false;
    const needs_stored = filter_needs_stored or exclusion_needs_stored;

    var owned = result;

    var missing_indices = std.ArrayListUnmanaged(usize).empty;
    defer missing_indices.deinit(alloc);
    for (owned.hits, 0..) |hit, i| {
        if (needs_stored and hit.stored_data == null) try missing_indices.append(alloc, i);
    }

    const loaded_many = if (needs_stored and executor.load_many_stored != null and missing_indices.items.len > 0) blk: {
        const keys = try alloc.alloc([]const u8, missing_indices.items.len);
        defer alloc.free(keys);
        for (missing_indices.items, 0..) |hit_index, i| keys[i] = owned.hits[hit_index].id;
        break :blk try executor.load_many_stored.?(executor.ctx, alloc, keys);
    } else null;
    defer if (loaded_many) |values| freeOptionalOwnedBytes(alloc, values);

    var arena_state = std.heap.ArenaAllocator.init(alloc);
    defer arena_state.deinit();

    var loaded_missing_index: usize = 0;
    var kept_len: usize = 0;
    for (owned.hits, 0..) |*hit, i| {
        _ = arena_state.reset(.retain_capacity);
        const hit_alloc = arena_state.allocator();
        const parsed_stored = if (needs_stored) blk: {
            const maybe_stored = if (hit.stored_data) |stored|
                stored
            else if (loaded_many) |values| blk2: {
                defer loaded_missing_index += 1;
                break :blk2 values[loaded_missing_index];
            } else try executor.load_stored(executor.ctx, alloc, hit.id);
            const stored = maybe_stored orelse {
                hit.deinit(alloc);
                continue;
            };
            defer if (hit.stored_data == null and loaded_many == null) alloc.free(stored);
            break :blk try std.json.parseFromSlice(std.json.Value, hit_alloc, stored, .{});
        } else null;

        var keep = true;
        if (compiled_filter) |compiled| {
            keep = if (filter_needs_stored)
                try compiled.matches(hit_alloc, hit.id, parsed_stored.?.value)
            else
                try compiled.matches(hit_alloc, hit.id, .null);
        }
        if (keep and compiled_exclusion != null) {
            keep = !(if (exclusion_needs_stored)
                try compiled_exclusion.?.matches(hit_alloc, hit.id, parsed_stored.?.value)
            else
                try compiled_exclusion.?.matches(hit_alloc, hit.id, .null));
        }

        if (keep) {
            if (kept_len != i) {
                owned.hits[kept_len] = hit.*;
                hit.* = undefined;
            }
            kept_len += 1;
        } else {
            hit.deinit(alloc);
        }
    }

    if (kept_len == 0) {
        if (owned.hits.len > 0) alloc.free(owned.hits);
        owned.hits = &.{};
    } else if (kept_len != owned.hits.len) {
        owned.hits = try alloc.realloc(owned.hits, kept_len);
    }
    owned.total_hits = @intCast(kept_len);
    return owned;
}

fn freeOptionalOwnedBytes(alloc: Allocator, values: []?[]u8) void {
    for (values) |value| {
        if (value) |bytes| alloc.free(bytes);
    }
    alloc.free(values);
}

fn stripCountOnlySearchHits(alloc: Allocator, result: types.SearchResult) types.SearchResult {
    var owned = result;
    for (owned.hits) |*hit| hit.deinit(alloc);
    if (owned.hits.len > 0) alloc.free(owned.hits);
    owned.hits = &.{};
    return owned;
}

pub fn postprocessTextSearchResult(
    alloc: Allocator,
    req: types.SearchRequest,
    raw: types.SearchResult,
    chunk_backed: bool,
    processor: SearchResultPostprocessor,
) !types.SearchResult {
    var filtered = try filterVisibleSearchResult(alloc, raw, .{
        .ctx = processor.ctx,
        .func = processor.is_visible,
        .filter_many = processor.filter_visible_many,
    });
    filtered = try applyStoredSearchPatternFilters(alloc, req, filtered, .{
        .ctx = processor.ctx,
        .load_stored = processor.load_stored,
    });
    try dedupeSearchHitsById(alloc, &filtered);
    if (chunk_backed) {
        const reshaped = try reshapeChunkBackedResult(alloc, req, filtered, .{
            .ctx = processor.ctx,
            .resolve_parent_id = processor.resolve_parent_id,
            .load_parent_stored = processor.load_parent_stored,
        });
        if (req.count_only) return stripCountOnlySearchHits(alloc, reshaped);
        return reshaped;
    }
    if (req.count_only) return stripCountOnlySearchHits(alloc, filtered);
    return filtered;
}

pub fn postprocessVectorSearchResult(
    alloc: Allocator,
    req: types.SearchRequest,
    raw: types.SearchResult,
    chunk_backed: bool,
    processor: SearchResultPostprocessor,
) !types.SearchResult {
    var filtered = try filterVisibleSearchResult(alloc, raw, .{
        .ctx = processor.ctx,
        .func = processor.is_visible,
        .filter_many = processor.filter_visible_many,
    });
    if (chunk_backed) {
        filtered = try reshapeChunkBackedResult(alloc, req, filtered, .{
            .ctx = processor.ctx,
            .resolve_parent_id = processor.resolve_parent_id,
            .load_parent_stored = processor.load_parent_stored,
        });
    }
    return try applyStoredSearchPatternFilters(alloc, req, filtered, .{
        .ctx = processor.ctx,
        .load_stored = processor.load_stored,
        .load_many_stored = processor.load_many_stored,
    });
}

pub fn resolveChunkParentId(
    alloc: Allocator,
    hit: types.SearchHit,
    resolver: ChunkParentResolver,
) ![]u8 {
    if (internal_keys.isChunkArtifactRecordKey(hit.id)) {
        return (try internal_keys.decodeDocumentComponentAlloc(alloc, hit.id)) orelse error.InvalidChunkArtifact;
    }

    const stored = if (hit.stored_data) |stored_data|
        stored_data
    else
        (try resolver.load_stored(resolver.ctx, alloc, hit.id)) orelse return error.StoredDocMissing;
    defer if (hit.stored_data == null) alloc.free(stored);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, stored, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidChunkArtifact;
    const parent = parsed.value.object.get("_parent_doc_key") orelse parsed.value.object.get("parent_doc_key") orelse return error.InvalidChunkArtifact;
    if (parent != .string) return error.InvalidChunkArtifact;
    return try alloc.dupe(u8, parent.string);
}

fn externalizeSearchHitIdentity(alloc: Allocator, hit: *types.SearchHit) !void {
    var resolved = try artifact_ids.resolvePublicHitIdentityAlloc(alloc, hit.id);
    defer resolved.deinit(alloc);

    alloc.free(hit.id);
    hit.id = try alloc.dupe(u8, resolved.id);
    if (hit.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
    hit.artifact_ref = if (resolved.artifact_ref) |artifact_ref| try artifact_ref.clone(alloc) else null;

    for (hit.chunk_hits) |*chunk_hit| {
        try externalizeChunkHitIdentity(alloc, chunk_hit);
    }
}

fn externalizeChunkHitIdentity(alloc: Allocator, hit: *types.ChunkHit) !void {
    var resolved = try artifact_ids.resolvePublicHitIdentityAlloc(alloc, hit.id);
    defer resolved.deinit(alloc);

    alloc.free(hit.id);
    hit.id = try alloc.dupe(u8, resolved.id);
    if (hit.artifact_ref) |*artifact_ref| artifact_ref.deinit(alloc);
    hit.artifact_ref = if (resolved.artifact_ref) |artifact_ref| try artifact_ref.clone(alloc) else null;
}

fn paginateParentChunkHits(
    alloc: Allocator,
    parents: *std.ArrayListUnmanaged(types.SearchHit),
    offset: u32,
    limit: u32,
) ![]types.SearchHit {
    const total: u32 = @intCast(parents.items.len);
    const start = @min(offset, total);
    const end = @min(start + limit, total);
    const start_usize: usize = @intCast(start);
    const end_usize: usize = @intCast(end);

    const selected = try alloc.alloc(types.SearchHit, end_usize - start_usize);
    errdefer alloc.free(selected);

    for (parents.items, 0..) |*hit, i| {
        if (i >= start_usize and i < end_usize) {
            selected[i - start_usize] = hit.*;
        } else {
            hit.deinit(alloc);
        }
    }

    parents.deinit(alloc);
    return selected;
}

fn loadStoredForVisibleHit(
    ctx: ?*anyopaque,
    alloc: Allocator,
    key: []const u8,
) anyerror!?[]u8 {
    const evaluator: *const SearchHitVisibilityEvaluator = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
    return try evaluator.load_stored(evaluator.ctx, alloc, key);
}

const TestStoredLoader = struct {
    single_calls: usize = 0,
    many_calls: usize = 0,

    fn loadStored(ctx: ?*anyopaque, alloc: Allocator, key: []const u8) !?[]u8 {
        const loader: *TestStoredLoader = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
        loader.single_calls += 1;
        if (std.mem.eql(u8, key, "doc:a")) return try alloc.dupe(u8, "{\"title\":\"alpha\"}");
        if (std.mem.eql(u8, key, "doc:b")) return try alloc.dupe(u8, "{\"title\":\"beta\"}");
        return null;
    }

    fn loadManyStored(ctx: ?*anyopaque, alloc: Allocator, keys: []const []const u8) ![]?[]u8 {
        const loader: *TestStoredLoader = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
        loader.many_calls += 1;
        const values = try alloc.alloc(?[]u8, keys.len);
        errdefer {
            for (values) |value| if (value) |bytes| alloc.free(bytes);
            alloc.free(values);
        }
        for (keys, 0..) |key, i| {
            values[i] = try loadStored(ctx, alloc, key);
            loader.single_calls -= 1;
        }
        return values;
    }
};

test "applyStoredSearchPatternFilters skips stored loads for doc_id-only filters" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(types.SearchHit, 2);
    hits[0] = .{ .id = try alloc.dupe(u8, "doc:a") };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b") };

    var loader = TestStoredLoader{};
    var result = try applyStoredSearchPatternFilters(alloc, .{
        .filter_query_json = "{\"doc_id\":{\"ids\":[\"doc:b\"]}}",
    }, .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
    }, .{
        .ctx = &loader,
        .load_stored = TestStoredLoader.loadStored,
        .load_many_stored = TestStoredLoader.loadManyStored,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), loader.single_calls);
    try std.testing.expectEqual(@as(usize, 0), loader.many_calls);
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
}

test "applyStoredSearchPatternFilters batch-loads only missing stored docs" {
    const alloc = std.testing.allocator;

    var hits = try alloc.alloc(types.SearchHit, 2);
    hits[0] = .{
        .id = try alloc.dupe(u8, "doc:a"),
        .stored_data = try alloc.dupe(u8, "{\"title\":\"alpha\"}"),
    };
    hits[1] = .{ .id = try alloc.dupe(u8, "doc:b") };

    var loader = TestStoredLoader{};
    var result = try applyStoredSearchPatternFilters(alloc, .{
        .filter_query_json = "{\"term\":{\"title\":\"beta\"}}",
    }, .{
        .alloc = alloc,
        .hits = hits,
        .total_hits = 2,
    }, .{
        .ctx = &loader,
        .load_stored = TestStoredLoader.loadStored,
        .load_many_stored = TestStoredLoader.loadManyStored,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 0), loader.single_calls);
    try std.testing.expectEqual(@as(usize, 1), loader.many_calls);
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
}

const TestChunkParentShaper = struct {
    fn resolveParentId(_: ?*anyopaque, alloc: Allocator, hit: types.SearchHit) ![]u8 {
        const sep = std.mem.indexOfScalar(u8, hit.id, '#') orelse return error.InvalidChunkArtifact;
        return try alloc.dupe(u8, hit.id[0..sep]);
    }

    fn loadParentStored(_: ?*anyopaque, _: Allocator, _: types.SearchRequest, _: []const u8) !?[]u8 {
        return null;
    }
};

test "reshapeChunkBackedResult orders equal-score parent hits by doc id" {
    const alloc = std.testing.allocator;

    var raw_hits = try alloc.alloc(types.SearchHit, 2);
    raw_hits[0] = .{
        .id = try alloc.dupe(u8, "doc:b#0"),
        .score = 0,
    };
    raw_hits[1] = .{
        .id = try alloc.dupe(u8, "doc:a#0"),
        .score = 0,
    };

    var result = try reshapeChunkBackedResult(alloc, .{
        .return_mode = .parent,
        .limit = 2,
        .include_stored = false,
    }, .{
        .alloc = alloc,
        .hits = raw_hits,
        .total_hits = 2,
    }, .{
        .ctx = null,
        .resolve_parent_id = TestChunkParentShaper.resolveParentId,
        .load_parent_stored = TestChunkParentShaper.loadParentStored,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqual(@as(usize, 2), result.hits.len);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    try std.testing.expectEqualStrings("doc:b", result.hits[1].id);
}
