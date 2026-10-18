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

//! Indexed, execution-local view of durable resolver curation. Strings and maps
//! belong to the caller's arena; construction is linear in policy size, while
//! mention and candidate admission require constant-time lookups.
const std = @import("std");
const slug = @import("unicode_slug.zig");

pub const Suppression = struct {
    keys: []const []const u8 = &.{},
    texts: []const []const u8 = &.{},
    min_text_chars: u32 = 0,
};
pub const Pair = struct { from: []const u8, to: []const u8 };
pub const State = struct {
    version: u32 = 1,
    suppress: Suppression = .{},
    distinct: []const Pair = &.{},
    /// Mask a declarative alias after an explicit unmerge. Future graph
    /// replacements must not silently recreate the redirect just corrected.
    removed_aliases: []const []const u8 = &.{},
};
pub const Reason = enum { key, text, min_text_chars, curator };
pub const Redirect = struct { key: []const u8, target: []const u8 };
pub const RedirectIndex = struct {
    canonical_keys: std.StringHashMapUnmanaged([]const u8) = .empty,
    source: []const Redirect,

    pub fn init(a: std.mem.Allocator, aliases: []const Redirect) !RedirectIndex {
        var edges: std.StringHashMapUnmanaged([]const u8) = .empty;
        var out: RedirectIndex = .{ .source = aliases };
        for (aliases) |alias| {
            if (alias.key.len == 0 or alias.target.len == 0) return error.InvalidResolverAlias;
            const entry = try edges.getOrPut(a, alias.key);
            if (entry.found_existing) return error.InvalidResolverAlias;
            entry.value_ptr.* = alias.target;
        }
        var visiting: std.StringHashMapUnmanaged(void) = .empty;
        var path: std.ArrayListUnmanaged([]const u8) = .empty;
        for (aliases) |alias| {
            path.clearRetainingCapacity();
            var key = alias.key;
            const target = walk: while (true) {
                if (out.canonical_keys.get(key)) |known| break :walk known;
                const next = edges.get(key) orelse break :walk key;
                const entry = try visiting.getOrPut(a, key);
                if (entry.found_existing) return error.InvalidResolverAliasCycle;
                try path.append(a, key);
                key = next;
            };
            for (path.items) |visited| {
                try out.canonical_keys.put(a, visited, target);
                _ = visiting.remove(visited);
            }
        }
        return out;
    }
    pub fn canonical(self: RedirectIndex, key: []const u8) []const u8 {
        return self.canonical_keys.get(key) orelse key;
    }
};
/// Entity inspection is a projection of one redirect component, never an
/// alternate endpoint for downloading the source's complete negative policy.
/// Text rules cannot be attributed from an entity key; inspect those through
/// the authorized source resolver suppression resource instead.
pub const EntityState = struct { redirects: []const Redirect, canonical_key: []const u8, curation: State };
pub fn entityState(a: std.mem.Allocator, state: State, aliases: []const Redirect, key: []const u8) !EntityState {
    const index = try RedirectIndex.init(a, aliases);
    const canonical = index.canonical(key);
    var related: std.ArrayListUnmanaged(Redirect) = .empty;
    for (aliases) |alias| if (std.mem.eql(u8, index.canonical(alias.key), canonical)) try related.append(a, alias);
    var keys: std.ArrayListUnmanaged([]const u8) = .empty;
    for (state.suppress.keys) |suppressed| if (std.mem.eql(u8, index.canonical(suppressed), canonical)) try keys.append(a, suppressed);
    var removed: std.ArrayListUnmanaged([]const u8) = .empty;
    for (state.removed_aliases) |alias| if (std.mem.eql(u8, index.canonical(alias), canonical)) try removed.append(a, alias);
    var pairs: std.ArrayListUnmanaged(Pair) = .empty;
    for (state.distinct) |pair| if (std.mem.eql(u8, index.canonical(pair.from), canonical) or std.mem.eql(u8, index.canonical(pair.to), canonical)) try pairs.append(a, pair);
    return .{ .redirects = related.items, .canonical_key = canonical, .curation = .{
        .version = state.version,
        .suppress = .{ .keys = keys.items, .min_text_chars = state.suppress.min_text_chars },
        .distinct = pairs.items,
        .removed_aliases = removed.items,
    } };
}

pub const Action = enum { merge, unmerge, suppress, unsuppress, suppressions };
pub const Selection = struct { keys: []const []const u8 = &.{}, texts: []const []const u8 = &.{} };
pub const Wait = struct { timeout_ms: u32 = 1000 };
pub const Request = struct {
    action: Action,
    from: ?[]const u8 = null,
    to: ?[]const u8 = null,
    key: ?[]const u8 = null,
    target: ?[]const u8 = null,
    keys: []const []const u8 = &.{},
    texts: []const []const u8 = &.{},
    replace: bool = false,
    pin_distinct: bool = false,
    expected_revision: ?u64 = null,
    add: ?Selection = null,
    remove: ?Selection = null,
    wait: ?Wait = null,
};
pub const Plan = struct { state: State, merges: []const Redirect, changed: bool };

/// A bounded catalog edit, built in scratch memory before publication. The
/// catalog validates the complete redirect graph and compares revisions while
/// holding its apply lock. No mutation observes a partially edited policy.
pub fn plan(a: std.mem.Allocator, state_json: []const u8, merges: []const Redirect, effective: []const Redirect, request: Request) !Plan {
    if (request.wait) |wait| if (wait.timeout_ms == 0 or wait.timeout_ms > 300000) return error.InvalidResolverCurationRequest;
    if ((request.from != null and request.key != null) or (request.to != null and request.target != null)) return error.InvalidResolverCurationRequest;
    const selections = request.keys.len > 0 or request.texts.len > 0;
    switch (request.action) {
        .merge => if (selections or request.add != null or request.remove != null) return error.InvalidResolverCurationRequest,
        .unmerge => if (selections or request.add != null or request.remove != null or request.to != null or request.target != null or request.replace) return error.InvalidResolverCurationRequest,
        .suppress, .unsuppress => if (request.from != null or request.to != null or request.target != null or request.replace or request.pin_distinct or request.add != null or request.remove != null) return error.InvalidResolverCurationRequest,
        .suppressions => if (selections or request.key != null or request.from != null or request.to != null or request.target != null or request.replace or request.pin_distinct) return error.InvalidResolverCurationRequest,
    }
    var state = try parseState(a, state_json);
    var next: std.ArrayListUnmanaged(Redirect) = .empty;
    try next.appendSlice(a, merges);
    var changed = false;
    switch (request.action) {
        .suppressions => {
            if (request.add == null and request.remove == null) return error.InvalidResolverCurationRequest;
            if (request.add) |selection| {
                const result = try plan(a, try std.json.Stringify.valueAlloc(a, state, .{}), merges, effective, .{ .action = .suppress, .keys = selection.keys, .texts = selection.texts });
                state = result.state;
            }
            if (request.remove) |selection| {
                const result = try plan(a, try std.json.Stringify.valueAlloc(a, state, .{}), merges, effective, .{ .action = .unsuppress, .keys = selection.keys, .texts = selection.texts });
                state = result.state;
            }
            changed = !std.mem.eql(u8, try std.json.Stringify.valueAlloc(a, try parseState(a, state_json), .{}), try std.json.Stringify.valueAlloc(a, state, .{}));
        },
        .merge, .unmerge => {
            const from = request.from orelse request.key orelse return error.InvalidResolverCurationRequest;
            try validKey(from);
            var old: ?[]const u8 = null;
            for (effective) |alias| if (std.mem.eql(u8, alias.key, from)) {
                old = alias.target;
                break;
            };
            if (request.action == .merge) {
                const to = request.to orelse request.target orelse return error.InvalidResolverCurationRequest;
                try validKey(to);
                if (std.mem.eql(u8, from, to)) return error.InvalidResolverAliasCycle;
                if (old) |previous| {
                    if (std.mem.eql(u8, previous, to)) {
                        // A retry of an accepted re-point must remain a no-op,
                        // including when it pinned the former target distinct.
                        return .{ .state = state, .merges = merges, .changed = false };
                    }
                    if (!request.replace) return error.ResolverRedirectExists;
                } else if (request.pin_distinct) return error.InvalidResolverCurationRequest;
                var found = false;
                for (next.items) |*alias| if (std.mem.eql(u8, alias.key, from)) {
                    alias.target = to;
                    found = true;
                    break;
                };
                if (!found) try next.append(a, .{ .key = from, .target = to });
                changed = true;
            } else if (old != null) {
                var i: usize = 0;
                while (i < next.items.len) {
                    if (std.mem.eql(u8, next.items[i].key, from)) {
                        _ = next.orderedRemove(i);
                    } else i += 1;
                }
                state.removed_aliases = try editSet(a, state.removed_aliases, &.{from}, true, false);
                changed = true;
            }
            if (request.pin_distinct) if (old) |target| {
                const key = try pairKey(a, from, target);
                var present = false;
                for (state.distinct) |pair| if (std.mem.eql(u8, key, try pairKey(a, pair.from, pair.to))) {
                    present = true;
                    break;
                };
                if (!present) {
                    const pairs = try a.alloc(Pair, state.distinct.len + 1);
                    @memcpy(pairs[0..state.distinct.len], state.distinct);
                    pairs[state.distinct.len] = .{ .from = from, .to = target };
                    state.distinct = pairs;
                    changed = true;
                }
            };
        },
        .suppress, .unsuppress => {
            const add = request.action == .suppress;
            var keys: std.ArrayListUnmanaged([]const u8) = .empty;
            try keys.appendSlice(a, request.keys);
            if (request.key) |key| try keys.append(a, key);
            if (keys.items.len == 0 and request.texts.len == 0) return error.InvalidResolverCurationRequest;
            const before = try std.json.Stringify.valueAlloc(a, state.suppress, .{});
            state.suppress.keys = try editSet(a, state.suppress.keys, keys.items, add, false);
            state.suppress.texts = try editSet(a, state.suppress.texts, request.texts, add, true);
            changed = !std.mem.eql(u8, before, try std.json.Stringify.valueAlloc(a, state.suppress, .{}));
        },
    }
    return .{ .state = state, .merges = next.items, .changed = changed };
}
fn editSet(a: std.mem.Allocator, current: []const []const u8, changes: []const []const u8, add: bool, normalized: bool) ![]const []const u8 {
    var set: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (current) |key| try set.put(a, if (normalized) try normalize(a, key) else key, {});
    for (changes) |raw| {
        const key = if (normalized) try normalize(a, raw) else raw;
        try validKey(key);
        if (add) try set.put(a, key, {}) else _ = set.orderedRemove(key);
    }
    const result = try a.dupe([]const u8, set.keys());
    std.mem.sort([]const u8, result, {}, struct {
        fn less(_: void, x: []const u8, y: []const u8) bool {
            return std.mem.order(u8, x, y) == .lt;
        }
    }.less);
    return result;
}
pub const Index = struct {
    keys: std.StringHashMapUnmanaged(void) = .empty,
    texts: std.StringHashMapUnmanaged(void) = .empty,
    distinct: std.StringHashMapUnmanaged(void) = .empty,
    removed_aliases: std.StringHashMapUnmanaged(void) = .empty,
    min_text_chars: u32 = 0,
    redirects: ?RedirectIndex = null,

    pub fn init(a: std.mem.Allocator, config_json: []const u8, state_json: []const u8) !Index {
        const config = if (config_json.len == 0) Suppression{} else try std.json.parseFromSliceLeaky(Suppression, a, config_json, .{ .allocate = .alloc_always });
        const state = try parseState(a, state_json);
        if (config.keys.len + config.texts.len > 100000) return error.ResolverCurationLimitExceeded;
        var result: Index = .{};
        for ([_]Suppression{ config, state.suppress }) |policy| {
            result.min_text_chars = @max(result.min_text_chars, policy.min_text_chars);
            for (policy.keys) |key| {
                try validKey(key);
                try result.keys.put(a, key, {});
            }
            for (policy.texts) |text| {
                const normalized = try normalize(a, text);
                if (normalized.len == 0) return error.InvalidResolverSuppression;
                try result.texts.put(a, normalized, {});
            }
        }
        for (state.distinct) |pair| {
            try validKey(pair.from);
            try validKey(pair.to);
            if (std.mem.eql(u8, pair.from, pair.to)) return error.InvalidResolverDistinctPair;
            try result.distinct.put(a, try pairKey(a, pair.from, pair.to), {});
        }
        for (state.removed_aliases) |key| {
            try validKey(key);
            try result.removed_aliases.put(a, key, {});
        }
        return result;
    }

    pub fn bindRedirects(self: *Index, a: std.mem.Allocator, redirects: RedirectIndex) !void {
        self.redirects = redirects;
        const original = try a.alloc([]const u8, self.distinct.count());
        var it = self.distinct.keyIterator();
        var i: usize = 0;
        while (it.next()) |key| : (i += 1) original[i] = key.*;
        for (original) |key| {
            const split = std.mem.indexOfScalar(u8, key, 0).?;
            try self.distinct.put(a, try pairKey(a, redirects.canonical(key[0..split]), redirects.canonical(key[split + 1 ..])), {});
        }
    }
    pub fn textReason(self: Index, a: std.mem.Allocator, text: []const u8) !?Reason {
        const trimmed = std.mem.trim(u8, text, " \t\r\n");
        const chars = std.unicode.utf8CountCodepoints(trimmed) catch return error.InvalidResolverSuppression;
        if (chars < self.min_text_chars) return .min_text_chars;
        if (self.texts.count() > 0 and self.texts.contains(try normalize(a, text))) return .text;
        return null;
    }
    pub fn keySuppressed(self: Index, key: []const u8) bool {
        return self.keys.contains(key);
    }
    pub fn pinnedDistinct(self: Index, a: std.mem.Allocator, from: []const u8, to: []const u8) !bool {
        if (self.distinct.count() == 0) return false;
        const x = if (self.redirects) |r| r.canonical(from) else from;
        const y = if (self.redirects) |r| r.canonical(to) else to;
        return self.distinct.contains(try pairKey(a, x, y));
    }
};

pub fn parseState(a: std.mem.Allocator, json: []const u8) !State {
    if (json.len > 4 * 1024 * 1024) return error.ResolverCurationLimitExceeded;
    const value = if (json.len == 0) State{} else try std.json.parseFromSliceLeaky(State, a, json, .{ .allocate = .alloc_always });
    if (value.version != 1) return error.InvalidResolverCurationVersion;
    if (value.suppress.keys.len + value.suppress.texts.len + value.distinct.len + value.removed_aliases.len > 100000) return error.ResolverCurationLimitExceeded;
    return value;
}

pub fn validateConfig(a: std.mem.Allocator, json: []const u8) !void {
    if (json.len > 4 * 1024 * 1024) return error.ResolverCurationLimitExceeded;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    _ = try Index.init(arena.allocator(), json, "");
}
pub fn normalize(a: std.mem.Allocator, text: []const u8) ![]const u8 {
    var output: std.ArrayListUnmanaged(u8) = .empty;
    try slug.append(a, &output, text);
    return output.items;
}
pub fn pairKey(a: std.mem.Allocator, from: []const u8, to: []const u8) ![]const u8 {
    const first = std.mem.order(u8, from, to) == .lt;
    return std.fmt.allocPrint(a, "{s}\x00{s}", .{ if (first) from else to, if (first) to else from });
}
pub fn validKey(key: []const u8) !void {
    if (key.len == 0 or key.len > 16384 or std.mem.indexOfScalar(u8, key, 0) != null or !std.unicode.utf8ValidateSlice(key)) return error.InvalidResolverCurationKey;
}

test "curation indexes normalized text scalar lengths and symmetric distinct pairs" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const index = try Index.init(a,
        \\{"keys":["concept/note"],"texts":["Content"],"min_text_chars":3}
    ,
        \\{"distinct":[{"from":"a","to":"b"}],"suppress":{"texts":["Information"]}}
    );
    try std.testing.expect(index.keySuppressed("concept/note"));
    try std.testing.expectEqual(Reason.text, (try index.textReason(a, " CONTENT ")).?);
    try std.testing.expectEqual(Reason.text, (try index.textReason(a, "information")).?);
    try std.testing.expectEqual(Reason.min_text_chars, (try index.textReason(a, "漢字")).?);
    try std.testing.expect(try index.pinnedDistinct(a, "b", "a"));
    try std.testing.expect(!try index.pinnedDistinct(a, "a", "c"));
}

test "curation plans undo one redirect preserve chains and pin the old pair" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const aliases = [_]Redirect{ .{ .key = "a", .target = "b" }, .{ .key = "b", .target = "c" } };
    const undo = try plan(a, "", &aliases, &aliases, .{ .action = .unmerge, .from = "b", .pin_distinct = true });
    try std.testing.expect(undo.changed);
    try std.testing.expectEqual(@as(usize, 1), undo.merges.len);
    try std.testing.expectEqualStrings("b", undo.merges[0].target);
    try std.testing.expectEqualStrings("b", undo.state.distinct[0].from);
    const absent = try plan(a, "", &.{}, &.{}, .{ .action = .unmerge, .from = "missing" });
    try std.testing.expect(!absent.changed);
    try std.testing.expectError(error.ResolverRedirectExists, plan(a, "", &aliases, &aliases, .{ .action = .merge, .from = "a", .to = "d" }));
    const replace = try plan(a, "", &aliases, &aliases, .{ .action = .merge, .from = "a", .to = "d", .replace = true, .pin_distinct = true });
    try std.testing.expectEqualStrings("d", replace.merges[0].target);
    const suppress = try plan(a, "", &.{}, &.{}, .{ .action = .suppress, .texts = &.{"Note"}, .keys = &.{"concept/note"} });
    const raw = try std.json.Stringify.valueAlloc(a, suppress.state, .{});
    const duplicate = try plan(a, raw, &.{}, &.{}, .{ .action = .suppress, .texts = &.{" note "} });
    try std.testing.expect(!duplicate.changed);
    const remove = try plan(a, raw, &.{}, &.{}, .{ .action = .unsuppress, .texts = &.{"NOTE"} });
    try std.testing.expectEqual(@as(usize, 0), remove.state.suppress.texts.len);
}

test "curation redirect index compresses a long chain and rejects cycles" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const aliases = try a.alloc(Redirect, 10000);
    for (aliases, 0..) |*alias, i| alias.* = .{ .key = try std.fmt.allocPrint(a, "key/{d}", .{i}), .target = try std.fmt.allocPrint(a, "key/{d}", .{i + 1}) };
    const index = try RedirectIndex.init(a, aliases);
    try std.testing.expectEqualStrings("key/10000", index.canonical("key/0"));
    try std.testing.expectEqual(@as(usize, 10000), index.canonical_keys.count());
    aliases[9999].target = "key/0";
    try std.testing.expectError(error.InvalidResolverAliasCycle, RedirectIndex.init(a, aliases));
}

test "curation distinct pins follow both redirect components" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var index = try Index.init(a, "", "{\"version\":1,\"distinct\":[{\"from\":\"a\",\"to\":\"b\"}]}");
    try index.bindRedirects(a, try RedirectIndex.init(a, &.{ .{ .key = "a", .target = "d" }, .{ .key = "b", .target = "c" } }));
    try std.testing.expect(try index.pinnedDistinct(a, "a", "c"));
    try std.testing.expect(try index.pinnedDistinct(a, "d", "b"));
    try std.testing.expect(!(try index.pinnedDistinct(a, "a", "d")));
}

test "curation repoint with a distinct pin is idempotent on retry" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const original = [_]Redirect{.{ .key = "a", .target = "b" }};
    const intent: Request = .{ .action = .merge, .from = "a", .to = "c", .replace = true, .pin_distinct = true };
    const first = try plan(a, "", &original, &original, intent);
    try std.testing.expect(first.changed);
    const retry = try plan(a, try std.json.Stringify.valueAlloc(a, first.state, .{}), first.merges, first.merges, intent);
    try std.testing.expect(!retry.changed);
    try std.testing.expectEqual(@as(usize, 1), retry.state.distinct.len);
}

test "curation entity inspection scopes policy to the complete redirect component" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const aliases = [_]Redirect{ .{ .key = "a", .target = "b" }, .{ .key = "c", .target = "b" }, .{ .key = "private", .target = "elsewhere" } };
    const state: State = .{
        .suppress = .{ .keys = &.{ "a", "private" }, .texts = &.{"private text"} },
        .distinct = &.{ .{ .from = "c", .to = "d" }, .{ .from = "private", .to = "secret" } },
        .removed_aliases = &.{ "b", "secret" },
    };
    const scoped = try entityState(a, state, &aliases, "a");
    try std.testing.expectEqualStrings("b", scoped.canonical_key);
    try std.testing.expectEqual(@as(usize, 2), scoped.redirects.len);
    try std.testing.expectEqualSlices([]const u8, &.{"a"}, scoped.curation.suppress.keys);
    try std.testing.expectEqual(@as(usize, 0), scoped.curation.suppress.texts.len);
    try std.testing.expectEqual(@as(usize, 1), scoped.curation.distinct.len);
    try std.testing.expectEqualStrings("d", scoped.curation.distinct[0].to);
    try std.testing.expectEqualSlices([]const u8, &.{"b"}, scoped.curation.removed_aliases);
}
