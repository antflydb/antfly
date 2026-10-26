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

//! Content-addressed static alias profiles and indexed component adjacency.
//! Source enrollment/refcounts and both edge directions publish atomically.
const std = @import("std");
const records = @import("resolver_authority.zig");
const resolver = @import("antfly_resolver");
const A = std.mem.Allocator;
pub var test_edge_visits: std.atomic.Value(usize) = .init(0);
const routes = @import("resolver_namespace_routes.zig");
pub const Definition = struct {
    key_template: []const u8,
    aliases: []const resolver.KeyAlias = &.{},
    suppress_json: []const u8 = "",
    pub fn identity(self: Definition, a: A) ![]const u8 {
        var buffer: [1024]u8 = undefined;
        var writer: std.Io.Writer.Hashing(@import("antfly_hash").Sha256) = .init(&buffer);
        try std.json.Stringify.value(self, .{}, &writer.writer);
        try writer.writer.flush();
        var digest: [32]u8 = undefined;
        writer.hasher.final(&digest);
        return a.dupe(u8, &std.fmt.bytesToHex(digest, .lower));
    }
};
const Head = struct { refs: u64, key_template: []const u8 };
const Summary = struct { profiles: u64 = 0 };
const Edge = struct { profile: []const u8, key: []const u8, target: []const u8 };
fn headKey(a: A, root: []const u8, id: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{s}profile/{s}", .{ root, id });
}
fn edgeKey(a: A, root: []const u8, direction: []const u8, node: []const u8, profile: []const u8, key: []const u8) ![]const u8 {
    return std.fmt.allocPrint(a, "{s}{s}/{s}/{s}/{s}", .{ root, direction, try routes.digest(a, node), profile, try routes.digest(a, key) });
}
/// Deltas are per source/namespace. Identical profiles share one graph, even
/// when thousands of resolvers reference it. Execution-only edits do no work.
pub fn synchronize(a: A, txn: anytype, root: []const u8, before: []const Definition, after: []const Definition) !void {
    const Count = struct { delta: i64 = 0, definition: Definition };
    var counts: std.StringHashMapUnmanaged(Count) = .empty;
    inline for (.{ before, after }, .{ @as(i64, -1), @as(i64, 1) }) |definitions, delta| for (definitions) |definition| {
        const id = try definition.identity(a);
        const entry = try counts.getOrPut(a, id);
        if (!entry.found_existing) entry.value_ptr.* = .{ .definition = definition };
        entry.value_ptr.delta = try std.math.add(i64, entry.value_ptr.delta, delta);
    };
    var it = counts.iterator();
    while (it.next()) |entry| {
        const delta = entry.value_ptr.delta;
        if (delta == 0) continue;
        const definition = entry.value_ptr.definition;
        const id = entry.key_ptr.*;
        const key = try headKey(a, root, id);
        const old = txn.get(key) catch |err| if (err == error.NotFound) null else return err;
        var head: Head = if (old) |raw| try std.json.parseFromSliceLeaky(Head, a, raw, .{}) else .{ .refs = 0, .key_template = definition.key_template };
        const prior = head.refs;
        head.refs = if (delta > 0) try std.math.add(u64, head.refs, @intCast(delta)) else try std.math.sub(u64, head.refs, @intCast(-delta));
        if (head.refs == 0) try txn.delete(key) else try txn.put(key, try std.json.Stringify.valueAlloc(a, head, .{}));
        const size_key = try std.fmt.allocPrint(a, "{s}query-size/{s}", .{ root, id });
        if (head.refs == 0) try txn.delete(size_key) else {
            const estimate = (records.Store.InlineQueryProfile{ .aliases = definition.aliases }).estimate();
            try txn.put(size_key, try std.json.Stringify.valueAlloc(a, estimate, .{}));
        }
        if ((prior == 0) == (head.refs == 0)) continue;
        const summary_key = try std.mem.concat(a, u8, &.{ root, "summary" });
        const summary_raw = txn.get(summary_key) catch |err| if (err == error.NotFound) null else return err;
        var summary: Summary = if (summary_raw) |raw| try std.json.parseFromSliceLeaky(Summary, a, raw, .{}) else .{};
        summary.profiles = if (head.refs == 0) try std.math.sub(u64, summary.profiles, 1) else try std.math.add(u64, summary.profiles, 1);
        try txn.put(summary_key, try std.json.Stringify.valueAlloc(a, summary, .{}));
        const definition_key = try std.fmt.allocPrint(a, "{s}definition/{s}", .{ root, id });
        if (head.refs == 0) try txn.delete(definition_key) else try txn.put(definition_key, try std.json.Stringify.valueAlloc(a, definition, .{}));
        const policy = try std.json.parseFromSliceLeaky(struct { keys: []const []const u8 = &.{} }, a, if (definition.suppress_json.len == 0) "{}" else definition.suppress_json, .{ .ignore_unknown_fields = true });
        for (policy.keys) |entity| {
            const path = try std.fmt.allocPrint(a, "{s}negative/{s}/{s}", .{ root, try routes.digest(a, entity), id });
            if (head.refs == 0) try txn.delete(path) else try txn.put(path, try std.json.Stringify.valueAlloc(a, entity, .{}));
        }
        for (definition.aliases) |alias| {
            const out = try edgeKey(a, root, "out", alias.key, id, alias.key);
            const in = try edgeKey(a, root, "in", alias.target, id, alias.key);
            if (head.refs == 0) {
                try txn.delete(out);
                try txn.delete(in);
            } else {
                const raw = try std.json.Stringify.valueAlloc(a, Edge{ .profile = id, .key = alias.key, .target = alias.target }, .{});
                try txn.put(out, raw);
                try txn.put(in, raw);
            }
        }
    }
}
/// Only cold namespace adoption needs complete declarations. Warm reads use
/// adjacency and never decode this separately stored immutable definition.
pub fn readDefinition(a: A, txn: anytype, root: []const u8, id: []const u8) !Definition {
    const raw = try txn.get(try std.fmt.allocPrint(a, "{s}definition/{s}", .{ root, id }));
    return std.json.parseFromSliceLeaky(Definition, a, raw, .{ .allocate = .alloc_always });
}
pub fn definitionAliases(a: A, txn: anytype, root: []const u8, id: []const u8) ![]const resolver.KeyAlias {
    return (try readDefinition(a, txn, root, id)).aliases;
}
/// A query reference must still name an enrolled profile at its read cut.
/// Validate the scalar head on every page; load full content only on a miss.
pub fn queryReferenceActive(a: A, txn: anytype, root: []const u8, id: []const u8, template: []const u8) !bool {
    if (id.len != 64) return error.InvalidResolverCurationRequest;
    for (id) |byte| if (!std.ascii.isHex(byte)) return error.InvalidResolverCurationRequest;
    const raw = txn.get(try headKey(a, root, id)) catch |err| return if (err == error.NotFound) false else err;
    const head = try std.json.parseFromSliceLeaky(Head, a, raw, .{});
    return head.refs != 0 and std.mem.eql(u8, head.key_template, template);
}
pub fn queryAliases(a: A, txn: anytype, root: []const u8, id: []const u8, template: []const u8) ![]const resolver.KeyAlias {
    const definition = try readDefinition(a, txn, root, id);
    if (!std.mem.eql(u8, definition.key_template, template) or !std.mem.eql(u8, id, try definition.identity(a))) return error.InvalidCatalogRecord;
    return definition.aliases;
}

/// Optional derived admission size. Old writers may remove/re-enroll profiles
/// without maintaining this hint; it never qualifies identity or authorization.
pub fn queryEstimatedBytes(a: A, txn: anytype, root: []const u8, id: []const u8) !usize {
    const raw = txn.get(try std.fmt.allocPrint(a, "{s}query-size/{s}", .{ root, id })) catch |err| return if (err == error.NotFound) std.math.maxInt(usize) else err;
    return std.json.parseFromSliceLeaky(usize, a, raw, .{});
}
/// A single immutable profile's adjacency, without decoding its declaration or
/// mixing aliases from other consumers of this native namespace.
pub const QueryReader = struct {
    backend: records.Backend,
    root: []const u8,
    profile: []const u8,
    pub fn lookup(self: *QueryReader) records.AliasLookup {
        return .{ .ptr = self, .expand = expand };
    }
    fn expand(ptr: *anyopaque, a: A, id: []const u8, forward_only: bool, mask: records.EdgeMask) ![]const resolver.KeyAlias {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        var result: std.ArrayList(resolver.KeyAlias) = .empty;
        for ([_][]const u8{ "out", "in" }, 0..) |direction, i| {
            if (forward_only and i == 1) break;
            const prefix = try std.fmt.allocPrint(a, "{s}{s}/{s}/{s}/", .{ self.root, direction, try routes.digest(a, id), self.profile });
            var after: ?[]const u8 = null;
            while (true) {
                const page = try self.backend.scan(self.backend.ptr, a, prefix, after, 128);
                for (page.rows) |row| {
                    const edge = try std.json.parseFromSliceLeaky(Edge, a, row.value, .{ .allocate = .alloc_always });
                    if (!std.mem.eql(u8, edge.profile, self.profile) or !std.mem.eql(u8, if (i == 0) edge.key else edge.target, id)) return error.InvalidCatalogRecord;
                    if (try mask.enabled(mask.ptr, a, edge.key)) try result.append(a, .{ .key = edge.key, .target = edge.target });
                }
                after = page.next;
                if (after == null) break;
            }
        }
        return result.items;
    }
};

pub const Reader = struct {
    a: A,
    backend: records.Backend,
    root: []const u8,
    key_template: []const u8,
    profiles: std.StringArrayHashMapUnmanaged(Profile) = .empty,
    seen: std.StringHashMapUnmanaged(void) = .empty,
    visits: usize = 0,
    nodes: std.StringHashMapUnmanaged(void) = .empty,
    policies_loaded: bool = false,
    const Profile = struct { head: Head, aliases: std.ArrayList(resolver.KeyAlias) = .empty, keys: std.StringHashMapUnmanaged(void) = .empty };
    pub fn lookup(self: *Reader) records.AliasLookup {
        return .{ .ptr = self, .expand = expand };
    }
    pub fn aliases(self: *Reader, id: []const u8) []const resolver.KeyAlias {
        return if (self.profiles.get(id)) |profile| profile.aliases.items else &.{};
    }
    pub fn values(self: *Reader) ![]const records.AliasProfile {
        if (!self.policies_loaded) {
            var after: ?[]const u8 = null;
            var nodes = self.nodes.keyIterator();
            while (nodes.next()) |node| {
                const negative = try std.fmt.allocPrint(self.a, "{s}negative/{s}/", .{ self.root, try routes.digest(self.a, node.*) });
                after = null;
                while (true) {
                    const page = try self.backend.scan(self.backend.ptr, self.a, negative, after, 128);
                    for (page.rows) |row| {
                        const key = try std.json.parseFromSliceLeaky([]const u8, self.a, row.value, .{ .allocate = .alloc_always });
                        if (!std.mem.eql(u8, key, node.*)) return error.InvalidCatalogRecord;
                        const profile_id = row.key[negative.len..];
                        const profile = try self.profiles.getOrPut(self.a, profile_id);
                        if (!profile.found_existing) {
                            const raw = (try self.backend.get(self.backend.ptr, self.a, try headKey(self.a, self.root, profile_id))) orelse return error.InvalidCatalogRecord;
                            profile.value_ptr.* = .{ .head = try std.json.parseFromSliceLeaky(Head, self.a, raw, .{}) };
                        }
                        try profile.value_ptr.keys.put(self.a, key, {});
                    }
                    after = page.next;
                    if (after == null) break;
                }
            }
            self.policies_loaded = true;
        }
        const summary_raw = try self.backend.get(self.backend.ptr, self.a, try std.mem.concat(self.a, u8, &.{ self.root, "summary" }));
        const summary: Summary = if (summary_raw) |raw| try std.json.parseFromSliceLeaky(Summary, self.a, raw, .{}) else .{};
        if (summary.profiles < self.profiles.count()) return error.InvalidCatalogRecord;
        // A profile with no incident declarations still constrains the native
        // graph. Represent all such profiles with one empty overlay, rather
        // than enumerating their heads or duplicating them on the wire.
        const empty = summary.profiles > self.profiles.count();
        const result = try self.a.alloc(records.AliasProfile, self.profiles.count() + @intFromBool(empty));
        for (self.profiles.keys(), self.profiles.values(), result[0..self.profiles.count()]) |id, profile, *value| {
            const keys = try self.a.alloc([]const u8, profile.keys.count());
            var iterator = profile.keys.keyIterator();
            for (keys) |*key| key.* = iterator.next().?.*;
            value.* = .{ .id = id, .key_template = profile.head.key_template, .suppress_json = if (keys.len == 0) "" else try std.json.Stringify.valueAlloc(self.a, .{ .keys = keys }, .{}), .aliases = profile.aliases.items };
        }
        if (empty) {
            // The root is namespace-specific; a caller supplies the template
            // from the native head, so no profile directory is needed here.
            result[result.len - 1] = .{ .key_template = self.key_template, .aliases = &.{} };
        }
        return result;
    }
    fn expand(ptr: *anyopaque, a: A, id: []const u8, forward_only: bool, mask: records.EdgeMask) ![]const resolver.KeyAlias {
        const self: *Reader = @ptrCast(@alignCast(ptr));
        try self.nodes.put(a, id, {});
        var edges: std.ArrayList(resolver.KeyAlias) = .empty;
        for ([_][]const u8{ "out", "in" }, 0..) |direction, i| {
            if (forward_only and i == 1) break;
            const prefix = try std.fmt.allocPrint(a, "{s}{s}/{s}/", .{ self.root, direction, try routes.digest(a, id) });
            var after: ?[]const u8 = null;
            while (true) {
                const page = try self.backend.scan(self.backend.ptr, a, prefix, after, 128);
                for (page.rows) |row| {
                    const edge = try std.json.parseFromSliceLeaky(Edge, a, row.value, .{ .allocate = .alloc_always });
                    if (!std.mem.eql(u8, if (i == 0) edge.key else edge.target, id)) return error.InvalidCatalogRecord;
                    if (!try mask.enabled(mask.ptr, a, edge.key)) continue;
                    self.visits += 1;
                    if (comptime @import("builtin").is_test) _ = test_edge_visits.fetchAdd(1, .monotonic);
                    const alias: resolver.KeyAlias = .{ .key = edge.key, .target = edge.target };
                    try edges.append(a, alias);
                    if ((try self.seen.getOrPut(a, row.value)).found_existing) continue;
                    if (self.seen.count() > 16384) return error.ResolverCurationLimitExceeded;
                    const profile = try self.profiles.getOrPut(a, edge.profile);
                    if (!profile.found_existing) {
                        const raw = (try self.backend.get(self.backend.ptr, a, try headKey(a, self.root, edge.profile))) orelse return error.InvalidCatalogRecord;
                        profile.value_ptr.* = .{ .head = try std.json.parseFromSliceLeaky(Head, a, raw, .{}) };
                    }
                    try profile.value_ptr.aliases.append(a, alias);
                }
                after = page.next;
                if (after == null) break;
            }
        }
        return edges.items;
    }
};

/// Read-only adapter for the Lite physical transaction. Unlike logical DB
/// stores this can see cross-source adjacency in the shared file snapshot.
pub fn ReadTransaction(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        fn get(ptr: *anyopaque, a: A, key: []const u8) !?[]const u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const raw = self.txn.get(key) catch |err| return if (err == error.NotFound) null else err;
            return try a.dupe(u8, raw);
        }
        fn put(_: *anyopaque, _: A, _: []const u8, _: []const u8) !void {
            return error.ReadOnly;
        }
        fn remove(_: *anyopaque, _: A, _: []const u8) !void {
            return error.ReadOnly;
        }
        fn scan(ptr: *anyopaque, a: A, prefix: []const u8, after: ?[]const u8, limit: u32) !records.Page {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var cursor = try self.txn.openCursor();
            defer cursor.close();
            const Payload = @typeInfo(@TypeOf(cursor.seekAtOrAfter(prefix))).error_union.payload;
            const Entry = if (@typeInfo(Payload) == .optional) @typeInfo(Payload).optional.child else Payload;
            var row: ?Entry = cursor.seekAtOrAfter(after orelse prefix) catch |err| if (err == error.NotFound) null else return err;
            if (after) |key| if (row) |entry| if (std.mem.eql(u8, entry.key, key)) {
                row = cursor.next() catch |err| if (err == error.NotFound) null else return err;
            };
            var rows: std.ArrayList(records.Row) = .empty;
            while (row) |entry| {
                if (!std.mem.startsWith(u8, entry.key, prefix)) break;
                if (rows.items.len == limit) return .{ .rows = rows.items, .next = rows.items[rows.items.len - 1].key };
                try rows.append(a, .{ .key = try a.dupe(u8, entry.key), .value = try a.dupe(u8, entry.value) });
                row = cursor.next() catch |err| if (err == error.NotFound) null else return err;
            }
            return .{ .rows = rows.items };
        }
        pub fn backend(self: *@This()) records.Backend {
            return .{ .ptr = self, .get = get, .put = put, .remove = remove, .scan = scan };
        }
    };
}

test "resolver curation namespace propagation static profile identities stream large declarations" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const entries = try a.alloc(resolver.KeyAlias, 1000);
    for (entries, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "concept/from-{d}", .{i}), .target = try std.fmt.allocPrint(a, "concept/into-{d}", .{i}) };
    const definition: Definition = .{ .key_template = "concept/{{_key}}", .aliases = entries, .suppress_json = "{}" };
    var buffer: [64]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&buffer);
    const id = try definition.identity(bounded.allocator());
    const expected = try routes.digest(a, try std.json.Stringify.valueAlloc(a, definition, .{}));
    try std.testing.expectEqualStrings(expected, id);
}
