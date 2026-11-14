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

//! Pure catalog transformations. The caller commits the returned index catalog
//! with the existing table-definition CAS; no process-local alias state exists.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const key = "_index_aliases";
pub const receipts_key = "_index_alias_receipts";
pub const receipt_lifetime_ms: u64 = 24 * 60 * 60 * 1000;
pub const max_receipts_per_alias = 128;
pub const max_aliases_per_table = 64;
pub const max_control_bytes = 512 * 1024;

/// Structural detection, including escaped JSON member names. Invalid input
/// fails closed at protocol admission; configuration validation reports it.
pub fn uses(bytes: []const u8) bool {
    if (std.mem.indexOfScalar(u8, bytes, '\\') == null and std.mem.indexOf(u8, bytes, "_index_alias") == null) return false;
    var parsed = std.json.parseFromSlice(V, std.heap.page_allocator, bytes, .{}) catch return true;
    defer parsed.deinit();
    return parsed.value == .object and (parsed.value.object.contains(key) or parsed.value.object.contains(receipts_key));
}

/// Transition envelopes contain TableRecord.indexes_json as an escaped string.
/// Inspect that structural field, never arbitrary prompt/document strings.
pub fn usesEnvelope(bytes: []const u8) bool {
    var parsed = std.json.parseFromSlice(V, std.heap.page_allocator, bytes, .{}) catch return true;
    defer parsed.deinit();
    return envelopeValue(parsed.value);
}
fn envelopeValue(value: V) bool {
    switch (value) {
        .object => |object| {
            if (object.contains(key) or object.contains(receipts_key)) return true;
            var entries = object.iterator();
            while (entries.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, "indexes_json") and entry.value_ptr.* == .string) {
                    if (uses(entry.value_ptr.string)) return true;
                } else if (envelopeValue(entry.value_ptr.*)) return true;
            }
        },
        .array => |array| for (array.items) |item| {
            if (envelopeValue(item)) return true;
        },
        else => {},
    }
    return false;
}

pub fn physicalProjection(out: A, bytes: []const u8) ![]u8 {
    if (!uses(bytes)) return out.dupe(u8, bytes);
    var arena = std.heap.ArenaAllocator.init(out);
    defer arena.deinit();
    var root = try std.json.parseFromSliceLeaky(V, arena.allocator(), bytes, .{});
    _ = root.object.orderedRemove(key);
    _ = root.object.orderedRemove(receipts_key);
    return std.json.Stringify.valueAlloc(out, root, .{});
}

/// Materialize at catalog publication, never on the query hot path.
pub fn queryProjection(out: A, bytes: []const u8) ![]u8 {
    if (!uses(bytes)) return out.dupe(u8, bytes);
    var arena = std.heap.ArenaAllocator.init(out);
    defer arena.deinit();
    const a = arena.allocator();
    var root = try std.json.parseFromSliceLeaky(V, a, bytes, .{});
    if (root != .object) return error.InvalidTableIndexMetadata;
    _ = root.object.orderedRemove(receipts_key);
    if (root.object.getPtr(key)) |declarations| {
        if (declarations.* != .object) return error.InvalidTableIndexMetadata;
        for (declarations.object.values()) |*resource| {
            if (resource.* != .object) return error.InvalidTableIndexMetadata;
            const target = resource.object.get("index") orelse return error.InvalidTableIndexMetadata;
            resource.* = .{ .object = .empty };
            try resource.object.put(a, "index", target);
        }
    }
    return std.json.Stringify.valueAlloc(out, root, .{});
}

pub const Requirement = enum { queryable, complete };
pub const Request = struct {
    index: []const u8,
    expected_index: ?[]const u8 = null,
    require: Requirement = .complete,
    idempotency_key: []const u8,
};
pub const Resource = struct {
    index: []const u8,
    previous_index: ?[]const u8,
    swapped_at_ms: u64,
    require: Requirement,
    idempotency_key: []const u8,
    readiness: V,
};

const Receipt = struct {
    request: Request,
    response: Resource,
};

/// Retries reconcile the original result even after a later swap or rollback.
/// Receipts have a documented 24-hour retention window and bounded admission.
pub fn replay(a: A, root: V, name: []const u8, request: Request) !?Resource {
    const ledger = root.object.get(receipts_key) orelse return null;
    if (ledger != .object) return error.InvalidTableIndexMetadata;
    const entries = ledger.object.get(name) orelse return null;
    if (entries != .array) return error.InvalidTableIndexMetadata;
    for (entries.array.items) |entry| {
        const receipt = try std.json.parseFromValueLeaky(Receipt, a, entry, .{});
        if (!std.mem.eql(u8, receipt.request.idempotency_key, request.idempotency_key)) continue;
        if (!std.mem.eql(u8, receipt.request.index, request.index) or receipt.request.require != request.require or
            !optionalEqual(receipt.request.expected_index, request.expected_index)) return error.IndexAliasIdempotencyConflict;
        return receipt.response;
    }
    return null;
}

fn validName(name: []const u8) bool {
    return name.len != 0 and name.len <= 256 and name[0] != '_' and
        std.mem.indexOfAny(u8, name, "/\x00") == null and !std.mem.eql(u8, name, "enrichments") and !std.mem.eql(u8, name, "resolvers");
}

fn parse(a: A, bytes: []const u8) !std.json.Parsed(V) {
    var result = try std.json.parseFromSlice(V, a, bytes, .{ .allocate = .alloc_always });
    errdefer result.deinit();
    if (result.value != .object) return error.InvalidTableIndexMetadata;
    if (result.value.object.get(key)) |aliases| if (aliases != .object) return error.InvalidTableIndexMetadata;
    return result;
}

pub fn resolve(root: V, name: []const u8) ![]const u8 {
    if (root != .object) return error.InvalidTableIndexMetadata;
    const aliases = root.object.get(key) orelse return name;
    if (aliases != .object) return error.InvalidTableIndexMetadata;
    const resource = aliases.object.get(name) orelse return name;
    if (resource != .object) return error.InvalidTableIndexMetadata;
    const target = resource.object.get("index") orelse return error.InvalidTableIndexMetadata;
    if (target != .string or !root.object.contains(target.string)) return error.InvalidTableIndexMetadata;
    return target.string;
}

pub fn ensureUnreferenced(a: A, bytes: []const u8, name: []const u8) !void {
    var parsed = try parse(a, bytes);
    defer parsed.deinit();
    const aliases = parsed.value.object.get(key) orelse return;
    for (aliases.object.values()) |resource| {
        if (resource != .object) return error.InvalidTableIndexMetadata;
        const target = resource.object.get("index") orelse return error.InvalidTableIndexMetadata;
        if (target != .string) return error.InvalidTableIndexMetadata;
        if (std.mem.eql(u8, target.string, name)) return error.IndexReferencedByAlias;
    }
}

pub fn validateReferences(root: V) !void {
    if (root != .object) return error.InvalidTableIndexMetadata;
    const aliases = root.object.get(key) orelse return;
    if (aliases != .object) return error.InvalidTableIndexMetadata;
    var it = aliases.object.iterator();
    while (it.next()) |entry| {
        if (!validName(entry.key_ptr.*) or root.object.contains(entry.key_ptr.*)) return error.IndexAliasNameConflict;
        const target = try resolve(root, entry.key_ptr.*);
        if (!validName(target)) return error.InvalidTableIndexMetadata;
        _ = try shape(root.object.get(target) orelse return error.IndexReferencedByAlias);
    }
}

/// Alias-only catalog updates do not mutate an object table's physical recipe.
pub fn configurationEqual(a: A, left: []const u8, right: []const u8) !bool {
    if (std.mem.eql(u8, left, right)) return true;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    var l = parse(s, left) catch |err| return if (err == error.OutOfMemory) err else false;
    var r = parse(s, right) catch |err| return if (err == error.OutOfMemory) err else false;
    _ = l.value.object.orderedRemove(key);
    _ = l.value.object.orderedRemove(receipts_key);
    _ = r.value.object.orderedRemove(key);
    _ = r.value.object.orderedRemove(receipts_key);
    return std.mem.eql(u8, try std.json.Stringify.valueAlloc(s, l.value, .{}), try std.json.Stringify.valueAlloc(s, r.value, .{}));
}

fn shape(config: V) !struct { kind: []const u8, sparse: bool } {
    if (config != .object) return error.InvalidTableIndexMetadata;
    const kind = config.object.get("type") orelse return error.InvalidTableIndexMetadata;
    if (kind != .string) return error.InvalidTableIndexMetadata;
    const sparse = config.object.get("sparse") orelse V{ .bool = false };
    if (sparse != .bool) return error.InvalidTableIndexMetadata;
    return .{ .kind = kind.string, .sparse = sparse.bool };
}

/// Readiness is the existing public index readiness object, not inferred from
/// progress percentages. A definition CAS binds this proof to the same catalog.
pub fn put(a: A, bytes: []const u8, name: []const u8, request: Request, readiness: V, now_ms: u64) ![]u8 {
    if (!validName(name) or !validName(request.index) or request.idempotency_key.len == 0 or request.idempotency_key.len > 256) return error.InvalidIndexAliasRequest;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var parsed = try parse(scratch, bytes);
    defer parsed.deinit();
    var root = &parsed.value;
    if (try replay(scratch, root.*, name, request) != null) return a.dupe(u8, bytes);
    if (root.object.contains(name)) return error.IndexAliasNameConflict;
    const target = root.object.get(request.index) orelse return error.IndexNotFound;
    const target_shape = try shape(target);
    var aliases = root.object.get(key) orelse V{ .object = std.json.ObjectMap{} };
    const current = aliases.object.get(name);
    if (current == null and aliases.object.count() >= max_aliases_per_table) return error.IndexAliasReceiptCapacity;
    var previous: ?[]const u8 = null;
    if (current) |value| {
        const resource = try std.json.parseFromValueLeaky(Resource, scratch, value, .{});
        // Reconcile a lost response before checking the caller's old expected
        // target or today's readiness. A retry never performs a second swap.
        if (std.mem.eql(u8, resource.idempotency_key, request.idempotency_key)) {
            if (!std.mem.eql(u8, resource.index, request.index) or resource.require != request.require or
                !optionalEqual(resource.previous_index, request.expected_index)) return error.IndexAliasIdempotencyConflict;
            return a.dupe(u8, bytes);
        }
        previous = resource.index;
        const old_shape = try shape(root.object.get(resource.index) orelse return error.InvalidTableIndexMetadata);
        if (!std.mem.eql(u8, old_shape.kind, target_shape.kind) or old_shape.sparse != target_shape.sparse) return error.IncompatibleIndexAliasTarget;
    }
    if (!optionalEqual(previous, request.expected_index)) return error.IndexAliasExpectedIndexMismatch;
    if (readiness != .object) return error.IndexAliasTargetNotReady;
    const ready = readiness.object.get(@tagName(request.require)) orelse return error.IndexAliasTargetNotReady;
    if (ready != .bool or !ready.bool) return error.IndexAliasTargetNotReady;
    const encoded = try std.json.Stringify.valueAlloc(scratch, Resource{
        .index = request.index,
        .previous_index = previous,
        .swapped_at_ms = now_ms,
        .require = request.require,
        .idempotency_key = request.idempotency_key,
        .readiness = readiness,
    }, .{});
    try aliases.object.put(scratch, name, try std.json.parseFromSliceLeaky(V, scratch, encoded, .{ .allocate = .alloc_always }));
    try root.object.put(scratch, key, aliases);
    var ledger = root.object.get(receipts_key) orelse V{ .object = .{} };
    if (ledger != .object) return error.InvalidTableIndexMetadata;
    // Reclaim expired receipts for every alias on every mutation. Fresh
    // receipts are never evicted to admit another operation.
    for (ledger.object.values()) |*old| {
        if (old.* != .array) return error.InvalidTableIndexMetadata;
        var retained = V{ .array = std.array_list.Managed(V).init(scratch) };
        for (old.array.items) |entry| {
            const receipt = try std.json.parseFromValueLeaky(Receipt, scratch, entry, .{});
            if (now_ms -| receipt.response.swapped_at_ms <= receipt_lifetime_ms) try retained.array.append(entry);
        }
        old.* = retained;
    }
    var entries = ledger.object.get(name) orelse V{ .array = std.array_list.Managed(V).init(scratch) };
    if (entries.array.items.len >= max_receipts_per_alias) return error.IndexAliasReceiptCapacity;
    const receipt = try std.json.Stringify.valueAlloc(scratch, Receipt{
        .request = request,
        .response = try std.json.parseFromSliceLeaky(Resource, scratch, encoded, .{}),
    }, .{});
    try entries.array.append(try std.json.parseFromSliceLeaky(V, scratch, receipt, .{}));
    try ledger.object.put(scratch, name, entries);
    try root.object.put(scratch, receipts_key, ledger);
    var control_root: V = .{ .object = .empty };
    try control_root.object.put(scratch, key, aliases);
    try control_root.object.put(scratch, receipts_key, ledger);
    const control = try std.json.Stringify.valueAlloc(scratch, control_root, .{});
    if (control.len > max_control_bytes) return error.IndexAliasReceiptCapacity;
    return std.json.Stringify.valueAlloc(a, root.*, .{});
}

fn optionalEqual(left: ?[]const u8, right: ?[]const u8) bool {
    if (left) |l| return if (right) |r| std.mem.eql(u8, l, r) else false;
    return right == null;
}

/// Rewrite only structural index names. Never descend into filters, documents,
/// prompts, or embedding values, which may contain arbitrary user keys.
pub fn bindQuery(a: A, root: V, query: *V) !void {
    if (query.* != .object) return error.InvalidQueryRequest;
    var fences = query.object.get("_index_alias_fences") orelse V{ .array = std.array_list.Managed(V).init(a) };
    if (fences != .array or fences.array.items.len > max_aliases_per_table) return error.InvalidQueryRequest;
    for (fences.array.items) |fence| {
        if (fence != .object) return error.InvalidQueryRequest;
        const name = fence.object.get("name") orelse return error.InvalidQueryRequest;
        const incarnation = fence.object.get("incarnation") orelse return error.InvalidQueryRequest;
        if (name != .string or incarnation != .integer or incarnation.integer < 0) return error.InvalidQueryRequest;
    }
    if (query.object.getPtr("full_text_index")) |name| {
        if (name.* != .null) {
            if (name.* != .string) return error.InvalidQueryRequest;
            name.* = .{ .string = try a.dupe(u8, try bindName(a, root, name.string, &fences)) };
        }
    }
    if (query.object.getPtr("indexes")) |names| if (names.* != .null) {
        if (names.* != .array) return error.InvalidQueryRequest;
        for (names.array.items) |*name| {
            if (name.* != .string) return error.InvalidQueryRequest;
            name.* = .{ .string = try a.dupe(u8, try bindName(a, root, name.string, &fences)) };
        }
    };
    for ([_][]const u8{ "embeddings", "sparse_embeddings" }) |field| {
        if (query.object.getPtr(field)) |values| {
            if (values.* == .null) continue;
            if (values.* != .object) return error.InvalidQueryRequest;
            var replacement = std.json.ObjectMap{};
            var it = values.object.iterator();
            while (it.next()) |entry| {
                const name = try bindName(a, root, entry.key_ptr.*, &fences);
                if (replacement.contains(name)) return error.InvalidQueryRequest;
                try replacement.put(a, try a.dupe(u8, name), entry.value_ptr.*);
            }
            values.* = .{ .object = replacement };
        }
    }
    if (query.object.getPtr("merge_config")) |merge| if (merge.* != .null) {
        if (merge.* != .object) return error.InvalidQueryRequest;
        if (merge.object.getPtr("weights")) |weights| {
            if (weights.* != .object) return error.InvalidQueryRequest;
            var replacement: std.json.ObjectMap = .{};
            var it = weights.object.iterator();
            while (it.next()) |entry| {
                const name = try bindName(a, root, entry.key_ptr.*, &fences);
                if (replacement.contains(name)) return error.InvalidQueryRequest;
                try replacement.put(a, try a.dupe(u8, name), entry.value_ptr.*);
            }
            weights.* = .{ .object = replacement };
        }
    };
    for ([_][]const u8{ "graph_queries", "graph_searches", "graph_metric_queries" }) |field| {
        if (query.object.getPtr(field)) |graphs| {
            if (graphs.* == .null) continue;
            if (graphs.* != .object) return error.InvalidQueryRequest;
            for (graphs.object.values()) |*graph| try bindIndexFields(a, root, graph, &fences);
        }
    }
    for ([_][]const u8{ "graph_metric_rerank", "graph_metric" }) |field| {
        if (query.object.getPtr(field)) |metric| try bindIndexFields(a, root, metric, &fences);
    }
    if (fences.array.items.len != 0) try query.object.put(a, "_index_alias_fences", fences);
}

fn bindName(a: A, root: V, name: []const u8, fences: *V) ![]const u8 {
    const target = try resolve(root, name);
    if (std.mem.eql(u8, name, target)) return target;
    for (fences.array.items) |fence| if (std.mem.eql(u8, fence.object.get("name").?.string, target)) return target;
    const generation = @import("index_incarnation.zig").get(root.object.get(target).?) orelse 0;
    const encoded = try std.json.Stringify.valueAlloc(a, .{ .name = target, .incarnation = generation }, .{});
    try fences.array.append(try std.json.parseFromSliceLeaky(V, a, encoded, .{}));
    return target;
}

fn bindIndexFields(a: A, root: V, value: *V, fences: *V) !void {
    if (value.* == .null) return;
    if (value.* != .object) return error.InvalidQueryRequest;
    for ([_][]const u8{ "index", "index_name" }) |field| {
        if (value.object.getPtr(field)) |name| {
            if (name.* != .string) return error.InvalidQueryRequest;
            name.* = .{ .string = try a.dupe(u8, try bindName(a, root, name.string, fences)) };
        }
    }
}

test "alias cutover binds names to one immutable catalog and fences deletion" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const s = arena.allocator();
    const base = "{\"a\":{\"type\":\"embeddings\"},\"b\":{\"type\":\"embeddings\"},\"s\":{\"type\":\"embeddings\",\"sparse\":true}}";
    const ready = try std.json.parseFromSliceLeaky(V, s, "{\"queryable\":true,\"complete\":true}", .{});
    const first = try put(s, base, "live", .{ .index = "a", .idempotency_key = "1" }, ready, 1);
    const second = try put(s, first, "live", .{ .index = "b", .expected_index = "a", .idempotency_key = "2" }, ready, 2);
    const retry = try put(s, second, "live", .{ .index = "b", .expected_index = "a", .idempotency_key = "2" }, .null, 3);
    try std.testing.expectEqualStrings(second, retry);
    try std.testing.expectError(error.IndexReferencedByAlias, ensureUnreferenced(s, second, "b"));
    try ensureUnreferenced(s, second, "a");
    try std.testing.expectError(error.IndexAliasExpectedIndexMismatch, put(s, second, "live", .{ .index = "a", .expected_index = "a", .idempotency_key = "3" }, ready, 3));
    try std.testing.expectError(error.IncompatibleIndexAliasTarget, put(s, second, "live", .{ .index = "s", .expected_index = "b", .idempotency_key = "3" }, ready, 3));
    try std.testing.expectError(error.IndexAliasTargetNotReady, put(s, second, "live", .{ .index = "a", .expected_index = "b", .idempotency_key = "3" }, .null, 3));
    const old = try std.json.parseFromSliceLeaky(V, s, first, .{});
    const new = try std.json.parseFromSliceLeaky(V, s, second, .{});
    try std.testing.expectEqualStrings("a", try resolve(old, "live"));
    try std.testing.expectEqualStrings("b", try resolve(new, "live"));
    var query = try std.json.parseFromSliceLeaky(V, s, "{\"indexes\":[\"live\"],\"embeddings\":{\"live\":[1,2]},\"filter_query\":{\"term\":{\"live\":\"live\"}}}", .{});
    try bindQuery(s, old, &query);
    try std.testing.expectEqualStrings("a", query.object.get("indexes").?.array.items[0].string);
    try std.testing.expect(query.object.get("embeddings").?.object.contains("a"));
    try std.testing.expect(query.object.get("filter_query").?.object.get("term").?.object.contains("live"));
    const rollback = try put(s, second, "live", .{ .index = "a", .expected_index = "b", .idempotency_key = "3" }, ready, 3);
    const replay_after_rollback = try put(s, rollback, "live", .{ .index = "b", .expected_index = "a", .idempotency_key = "2" }, .null, 4);
    try std.testing.expectEqualStrings(rollback, replay_after_rollback);
    const rolled = try std.json.parseFromSliceLeaky(V, s, rollback, .{});
    const original_response = (try replay(s, rolled, "live", .{ .index = "b", .expected_index = "a", .idempotency_key = "2" })).?;
    try std.testing.expectEqualStrings("b", original_response.index);
    try std.testing.expectEqual(@as(u64, 2), original_response.swapped_at_ms);
    try std.testing.expectError(error.IndexAliasIdempotencyConflict, put(s, rollback, "live", .{ .index = "a", .expected_index = "a", .idempotency_key = "2" }, ready, 4));
}

test "alias transformations release every partial allocation" {
    const Check = struct {
        fn run(a: A) !void {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const ready = try std.json.parseFromSliceLeaky(V, arena.allocator(), "{\"complete\":true}", .{});
            const result = try put(a, "{\"dense\":{\"type\":\"embeddings\"}}", "live", .{ .index = "dense", .idempotency_key = "key" }, ready, 1);
            defer a.free(result);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "canonical graph and merge weights bind only structural names" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a, "{\"g2\":{\"type\":\"graph\"},\"_index_aliases\":{\"live\":{\"index\":\"g2\"}}}", .{});
    var query = try std.json.parseFromSliceLeaky(V, a, "{\"graph_queries\":{\"walk\":{\"index\":\"live\",\"traverse\":{\"start_nodes\":[\"live\"]}}},\"merge_config\":{\"weights\":{\"live\":0.5}}}", .{});
    try bindQuery(a, root, &query);
    const graph = query.object.get("graph_queries").?.object.get("walk").?;
    try std.testing.expectEqualStrings("g2", graph.object.get("index").?.string);
    try std.testing.expectEqualStrings("live", graph.object.get("traverse").?.object.get("start_nodes").?.array.items[0].string);
    try std.testing.expect(query.object.get("merge_config").?.object.get("weights").?.object.contains("g2"));
}

test "alias receipt admission is bounded without evicting retriable outcomes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ready = try std.json.parseFromSliceLeaky(V, a, "{\"complete\":true}", .{});
    var bytes: []const u8 = "{\"a\":{\"type\":\"full_text\"},\"b\":{\"type\":\"full_text\"}}";
    var previous: ?[]const u8 = null;
    for (0..max_receipts_per_alias) |i| {
        const target: []const u8 = if (i % 2 == 0) "a" else "b";
        bytes = try put(a, bytes, "live", .{ .index = target, .expected_index = previous, .idempotency_key = try std.fmt.allocPrint(a, "key-{d}", .{i}) }, ready, i);
        previous = target;
    }
    const next: Request = .{ .index = "a", .expected_index = previous, .idempotency_key = "next" };
    try std.testing.expectError(error.IndexAliasReceiptCapacity, put(a, bytes, "live", next, ready, max_receipts_per_alias));
    const advanced = try put(a, bytes, "live", next, ready, receipt_lifetime_ms + 1000);
    const root = try std.json.parseFromSliceLeaky(V, a, advanced, .{});
    try std.testing.expectEqual(@as(usize, 1), root.object.get(receipts_key).?.object.get("live").?.array.items.len);
}

test "full text aliases and nullable optional query fields preserve public semantics" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(V, a, "{\"text2\":{\"type\":\"full_text\"},\"_index_aliases\":{\"live\":{\"index\":\"text2\"}}}", .{});
    var query = try std.json.parseFromSliceLeaky(V, a, "{\"full_text_index\":\"live\",\"indexes\":null,\"graph_queries\":null,\"embeddings\":null,\"merge_config\":null,\"graph_metric\":null}", .{});
    try bindQuery(a, root, &query);
    try std.testing.expectEqualStrings("text2", query.object.get("full_text_index").?.string);
    try std.testing.expect(query.object.get("indexes").? == .null);
}

test "index alias structural detection handles escaped keys and ignores user values" {
    try std.testing.expect(uses("{\"\\u005findex_aliases\":{}}"));
    try std.testing.expect(uses("{\"\\u005findex_alias_receipts\":{}}"));
    try std.testing.expect(!uses("{\"description\":\"_index_aliases\"}"));
    const a = std.testing.allocator;
    const compact = try queryProjection(a, "{\"a\":{\"type\":\"graph\"},\"_index_aliases\":{\"live\":{\"index\":\"a\",\"readiness\":{\"complete\":true}}},\"_index_alias_receipts\":{\"live\":[]}}");
    defer a.free(compact);
    try std.testing.expect(std.mem.indexOf(u8, compact, "readiness") == null);
    try std.testing.expect(std.mem.indexOf(u8, compact, receipts_key) == null);
}

test "index alias admission globally reclaims expired history and bounds retained bytes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const ready = try std.json.parseFromSliceLeaky(V, a, "{\"complete\":true}", .{});
    const first = try put(a, "{\"a\":{\"type\":\"full_text\"}}", "old", .{ .index = "a", .idempotency_key = "old" }, ready, 1);
    const second = try put(a, first, "new", .{ .index = "a", .idempotency_key = "new" }, ready, receipt_lifetime_ms + 2);
    const root = try std.json.parseFromSliceLeaky(V, a, second, .{});
    try std.testing.expectEqual(@as(usize, 0), root.object.get(receipts_key).?.object.get("old").?.array.items.len);
    var huge = ready;
    try huge.object.put(a, "evidence", .{ .string = try a.alloc(u8, max_control_bytes) });
    @memset(@constCast(huge.object.get("evidence").?.string), 'x');
    try std.testing.expectError(error.IndexAliasReceiptCapacity, put(a, "{\"a\":{\"type\":\"full_text\"}}", "live", .{ .index = "a", .idempotency_key = "1" }, huge, 1));
}
