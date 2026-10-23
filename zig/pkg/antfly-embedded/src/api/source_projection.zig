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

//! Conditional delivery of already authorized logical source values. Conditions
//! never authorize a read. This bounded reference path hashes selected values
//! directly; storage-owned authorities may replace that work without changing
//! the public identity contract. Raw storage directory hashes are not identities.
const std = @import("std");
const A = std.mem.Allocator;
const V = std.json.Value;
pub const Condition = struct { id: []const u8, identity: []const u8 };
pub const Field = struct { field: []const u8, if_match: []const Condition = &.{} };
pub const Request = struct { fields: []const Field };
pub const State = enum { value, unchanged, missing };
pub const Identity = struct { state: State, identity: ?[]const u8 = null };

/// Only final source hydration may use storage identities. Retrieval filters
/// still run normally; source-dependent coordinator operations retain bytes.
pub fn storageEligible(req: anytype) bool {
    if (req.source_projection_json.len == 0 or !req.include_stored or req.include_all_fields or req.fields.len == 0 or req.fields.len > 64 or
        req.document_authority_mode != .none or req.return_mode != .parent or req.hasHitEvaluation() or req.evaluation_json.len != 0 or
        req.reranker != null or req.pruner != null or req.highlight != null or req.aggregations_json.len != 0 or
        req.graph_queries.len != 0 or req.graph_metric_queries.len != 0 or req.graph_metric_rerank != null or
        req.hierarchy_children != null or req.hierarchy_grouped_matches or req.dense != null or req.sparse != null or
        req.dense_queries.len != 0 or req.sparse_queries.len != 0 or req.full_text_queries.len != 0) return false;
    switch (req.query) {
        .match_all, .doc_id => {},
        else => return false,
    }
    // Public ID/filter buckets normalize to a match-all text root. This is
    // membership scaffolding, with no source-dependent lexical scoring.
    if (req.full_text) |query| switch (query) {
        .match_all => {},
        else => return false,
    };
    for (req.fields) |field| {
        if (field.len == 0 or field[0] == '-' or std.mem.indexOfAny(u8, field, "*?[]") != null) return false;
        var parts = std.mem.splitScalar(u8, field, '.');
        while (parts.next()) |part| if (part.len == 0 or part[0] == '_') return false;
    }
    return true;
}

pub fn validate(a: A, request: Request, fields: []const []const u8, include_all: bool) !void {
    if (request.fields.len == 0 or request.fields.len > 64) return error.InvalidQueryRequest;
    var conditions: usize = 0;
    for (request.fields, 0..) |entry, i| {
        if (entry.field.len == 0 or entry.field.len > 1024 or std.mem.indexOfScalar(u8, entry.field, 0) != null or std.mem.indexOfAny(u8, entry.field, "*?[]") != null) return error.InvalidQueryRequest;
        var parts = std.mem.splitScalar(u8, entry.field, '.');
        while (parts.next()) |part| if (part.len == 0) return error.InvalidQueryRequest;
        // Overlapping requests would make omission depend on request order.
        for (request.fields[0..i]) |prior| if (covers(prior.field, entry.field) or covers(entry.field, prior.field)) return error.InvalidQueryRequest;
        if (!selectedPath(fields, include_all, entry.field)) return error.InvalidQueryRequest;
        conditions += entry.if_match.len;
        if (conditions > 4096) return error.InvalidQueryRequest;
        var seen: std.StringHashMapUnmanaged(void) = .empty;
        defer seen.deinit(a);
        for (entry.if_match) |condition| {
            if (condition.id.len == 0 or condition.id.len > 4096 or !validIdentity(condition.identity)) return error.InvalidQueryRequest;
            if ((try seen.getOrPut(a, condition.id)).found_existing) return error.InvalidQueryRequest;
        }
    }
}
/// Field masks use the document projector's segment wildcard and exclusion
/// semantics. Conditional paths themselves remain exact object paths.
pub fn selectedPath(fields: []const []const u8, include_all: bool, path: []const u8) bool {
    if (fields.len == 0) return include_all;
    var has_include = false;
    var selected = false;
    for (fields) |field| {
        if (std.mem.startsWith(u8, field, "-")) {
            if (patternCovers(field[1..], path)) return false;
        } else {
            has_include = true;
            selected = selected or patternCovers(field, path);
        }
    }
    return selected or !has_include;
}
fn patternCovers(pattern: []const u8, path: []const u8) bool {
    var p = std.mem.tokenizeScalar(u8, pattern, '.');
    var value = std.mem.splitScalar(u8, path, '.');
    var count: usize = 0;
    while (p.next()) |part| {
        const component = value.next() orelse return false;
        if (!std.mem.eql(u8, part, "*") and !std.mem.eql(u8, part, component)) return false;
        count += 1;
    }
    return count != 0;
}
fn covers(parent: []const u8, child: []const u8) bool {
    return std.mem.eql(u8, parent, child) or (child.len > parent.len and child[parent.len] == '.' and std.mem.startsWith(u8, child, parent));
}
pub fn validIdentity(bytes: []const u8) bool {
    if (bytes.len != 68 or !std.mem.startsWith(u8, bytes, "sv1-")) return false;
    for (bytes[4..]) |c| if (!std.ascii.isDigit(c) and (c < 'a' or c > 'f')) return false;
    return true;
}
pub const identity = @import("../storage/source_value_identity.zig").identity;
fn at(source: *V, path: []const u8) ?*V {
    var current = source;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (current.* != .object) return null;
        current = current.object.getPtr(part) orelse return null;
    }
    return current;
}
fn free(comptime borrowed: bool, a: A, value: *V) void {
    switch (value.*) {
        .string, .number_string => |bytes| if (!borrowed) a.free(bytes),
        .array => |*array| {
            for (array.items) |*item| free(borrowed, a, item);
            array.deinit();
        },
        .object => |*object| {
            var it = object.iterator();
            while (it.next()) |entry| {
                a.free(entry.key_ptr.*);
                free(borrowed, a, entry.value_ptr);
            }
            object.deinit(a);
        },
        else => {},
    }
}
fn remove(comptime borrowed: bool, a: A, source: *V, path: []const u8) void {
    const dot = std.mem.lastIndexOfScalar(u8, path, '.');
    const parent = if (dot) |i| at(source, path[0..i]) orelse return else source;
    if (parent.* != .object) return;
    const key = if (dot) |i| path[i + 1 ..] else path;
    if (parent.object.fetchSwapRemove(key)) |entry| {
        a.free(entry.key);
        var value = entry.value;
        free(borrowed, a, &value);
    }
}
/// Source owns its containers and keys; borrowed views keep immutable leaves.
/// Missing is
/// distinct from null. Only exact expected identities may omit source values.
pub const Prepared = struct {
    request: Request,
    conditions: []std.StringHashMapUnmanaged([]const u8),
    pub fn init(a: A, request: Request) !Prepared {
        const conditions = try a.alloc(std.StringHashMapUnmanaged([]const u8), request.fields.len);
        @memset(conditions, .empty);
        errdefer {
            for (conditions) |*map| map.deinit(a);
            a.free(conditions);
        }
        for (request.fields, conditions) |entry, *map| for (entry.if_match) |condition| try map.put(a, condition.id, condition.identity);
        return .{ .request = request, .conditions = conditions };
    }
    pub fn deinit(self: Prepared, a: A) void {
        for (self.conditions) |*map| map.deinit(a);
        a.free(self.conditions);
    }
};
pub fn apply(comptime borrowed: bool, a: A, prepared: Prepared, id: []const u8, source: *V) !std.json.ArrayHashMap(Identity) {
    var result: std.json.ArrayHashMap(Identity) = .{};
    errdefer {
        for (result.map.keys()) |key| a.free(key);
        for (result.map.values()) |entry| if (entry.identity) |bytes| a.free(bytes);
        result.map.deinit(a);
    }
    for (prepared.request.fields, prepared.conditions) |entry, conditions| {
        const value = at(source, entry.field);
        var state: State = .missing;
        var encoded: ?[]const u8 = null;
        if (value) |present| {
            const token = try identity(present.*);
            encoded = try a.dupe(u8, &token);
            state = .value;
            if (conditions.get(id)) |expected| if (std.mem.eql(u8, expected, &token)) {
                state = .unchanged;
            };
            if (state == .unchanged) remove(borrowed, a, source, entry.field);
        }
        errdefer if (encoded) |bytes| a.free(bytes);
        const name = try a.dupe(u8, entry.field);
        errdefer a.free(name);
        try result.map.put(a, name, .{ .state = state, .identity = encoded });
    }
    return result;
}

test "api query contract source identities bind typed values, distinguish missing and omit only matching IDs" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var source = try std.json.parseFromSliceLeaky(V, a, "{\"body\":{\"image\":\"payload\",\"null\":null}}", .{ .allocate = .alloc_always });
    const token = try identity(source.object.get("body").?.object.get("image").?);
    const request: Request = .{ .fields = &.{ .{ .field = "body.image", .if_match = &.{.{ .id = "a", .identity = &token }} }, .{ .field = "body.null" }, .{ .field = "body.absent" } } };
    try validate(a, request, &.{"body"}, false);
    var wrong = try std.json.parseFromSliceLeaky(V, a, "{\"body\":{\"image\":\"payload\"}}", .{ .allocate = .alloc_always });
    const prepared = try Prepared.init(a, request);
    defer prepared.deinit(a);
    const other = try apply(false, a, prepared, "b", &wrong);
    try std.testing.expectEqual(State.value, other.map.get("body.image").?.state);
    const result = try apply(false, a, prepared, "a", &source);
    try std.testing.expectEqual(State.unchanged, result.map.get("body.image").?.state);
    try std.testing.expect(source.object.get("body").?.object.get("image") == null);
    try std.testing.expectEqual(State.value, result.map.get("body.null").?.state);
    try std.testing.expectEqual(State.missing, result.map.get("body.absent").?.state);
    try std.testing.expectError(error.InvalidQueryRequest, validate(a, .{ .fields = &.{ .{ .field = "body" }, .{ .field = "body.image" } } }, &.{}, true));
}

test "api query contract conditional paths respect projection wildcards and exclusions" {
    const a = std.testing.allocator;
    const request: Request = .{ .fields = &.{.{ .field = "body.image" }} };
    for ([_][]const []const u8{ &.{"*"}, &.{"body.*"}, &.{"*.image"}, &.{"-revision"}, &.{ "*", "-body.private" } }) |fields| {
        try validate(a, request, fields, false);
    }
    for ([_][]const []const u8{ &.{}, &.{"revision"}, &.{ "*", "-body" }, &.{ "body.*", "-*.image" }, &.{"-*"} }) |fields| {
        try std.testing.expectError(error.InvalidQueryRequest, validate(a, request, fields, false));
    }
}
