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

//! Resolve bound context sources into a rendered, fingerprinted producer
//! input plus the lineage needed to publish it.
//!
//! Reads go through a host `Reader` so the same resolution runs in embedded,
//! standalone, and distributed runtimes: the resident DB serves its own table,
//! and hosts that own a table registry (Lite's table handles, the server's
//! routing-aware `TableReadSource`) serve foreign table identities under the
//! enrichment's execution identity. A host that cannot serve a table returns
//! `error.ContextSourceTableUnavailable`, which surfaces as explicit retry or
//! terminal debt; context is never silently dropped.
//!
//! Query sources are executed as a recall-safe *prefilter* (string conjuncts
//! as analyzed AND matches; numeric/boolean conjuncts omitted) and every
//! returned row is re-verified against the exact bound predicate. Prefilter
//! pages continue until the host reports exhaustion, so a verified member
//! set is complete or the resolution fails with `ContextBudgetExceeded`.

const std = @import("std");
const Allocator = std.mem.Allocator;
const ctx = @import("context_sources.zig");
const store = @import("context_dependency_store.zig");

pub const Row = struct {
    key: []u8,
    /// JSON document.
    value: []u8,
};

pub const Page = struct {
    rows: []Row,
    /// True when the host has no rows after this page.
    exhausted: bool,

    pub fn deinit(self: *Page, alloc: Allocator) void {
        for (self.rows) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        alloc.free(self.rows);
        self.* = undefined;
    }
};

pub const Reader = struct {
    ptr: *anyopaque,
    /// Immutable identity of the enrichment's own table ("" when the host has
    /// no table identities, e.g. a standalone single-table DB).
    own_table_id: []const u8 = "",
    lookup_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8,
    query_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8, filter_query_json: []const u8, offset: u32, limit: u32) anyerror!Page,
    /// Opaque position the subsequent reads are at least as fresh as.
    frontier_fn: ?*const fn (ptr: *anyopaque, alloc: Allocator, table_id: []const u8) anyerror![]u8 = null,

    pub fn isOwn(self: Reader, table_id: []const u8) bool {
        return table_id.len == 0 or std.mem.eql(u8, table_id, self.own_table_id);
    }

    /// Identity recorded in lineage: the canonical id even for own-table reads.
    pub fn identity(self: Reader, table_id: []const u8) []const u8 {
        return if (table_id.len == 0) self.own_table_id else table_id;
    }
};

pub const Limits = struct {
    page_rows: u32 = 256,
    /// Prefilter rows examined per source, as a multiple of max_documents
    /// plus a floor. Exceeding it fails explicitly instead of truncating.
    scan_factor: u32 = 8,
    scan_floor: u32 = 1024,
};

pub const Resolution = struct {
    arena: std.heap.ArenaAllocator,
    rendered: ctx.Rendered,
    inputs: []const store.ManifestInput,
    queries: []const store.ManifestQuery,
    absent: []const store.ManifestInput,
    /// `{"<table_id>":"<position>",...}` sorted by table id.
    frontier: []const u8,

    pub fn deinit(self: *Resolution, alloc: Allocator) void {
        self.rendered.deinit(alloc);
        self.arena.deinit();
        self.* = undefined;
    }
};

/// Recall-safe prefilter for a bound query. Returns `{"match_all":{}}` when
/// no conjunct can be pushed down (verification still enforces equality).
pub fn prefilterJsonAlloc(alloc: Allocator, bound: *const ctx.Bound) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    var pushed: usize = 0;
    try out.appendSlice(alloc, "{\"conjuncts\":[");
    for (bound.source.conjuncts, bound.values) |c, set| {
        var strings: usize = 0;
        for (set) |v| {
            if (v.len > 0 and v[0] == '"') strings += 1;
        }
        // Mixed or non-string sets cannot be pushed down safely.
        if (strings == 0 or strings != set.len) continue;
        if (pushed != 0) try out.append(alloc, ',');
        pushed += 1;
        if (set.len > 1) try out.appendSlice(alloc, "{\"disjuncts\":[");
        for (set, 0..) |v, i| {
            if (i != 0) try out.append(alloc, ',');
            try out.appendSlice(alloc, "{\"match\":");
            try out.appendSlice(alloc, v);
            try out.appendSlice(alloc, ",\"field\":");
            try ctx.appendJsonString(alloc, &out, c.field);
            try out.appendSlice(alloc, ",\"operator\":\"and\"}");
        }
        if (set.len > 1) try out.appendSlice(alloc, "]}");
    }
    if (pushed == 0) {
        out.clearRetainingCapacity();
        try out.appendSlice(alloc, "{\"match_all\":{}}");
        return try out.toOwnedSlice(alloc);
    }
    try out.appendSlice(alloc, "]}");
    return try out.toOwnedSlice(alloc);
}

/// Resolve every bound source and render the producer input. Errors:
/// `RequiredContextMissing`, `ContextBudgetExceeded` (explicit refusal, never
/// truncated output) and any host read error.
pub fn resolveAlloc(alloc: Allocator, reader: Reader, bindings: *const ctx.Bindings, limits: Limits) !Resolution {
    var arena = std.heap.ArenaAllocator.init(alloc);
    errdefer arena.deinit();
    const a = arena.allocator();
    var resolved = try a.alloc(ctx.ResolvedSource, bindings.items.len);
    var inputs = std.ArrayListUnmanaged(store.ManifestInput).empty;
    var queries = std.ArrayListUnmanaged(store.ManifestQuery).empty;
    var absent = std.ArrayListUnmanaged(store.ManifestInput).empty;
    var frontiers = std.StringArrayHashMapUnmanaged([]const u8).empty;

    for (bindings.items, 0..) |*bound, i| {
        const source = bound.source;
        const table_identity = reader.identity(source.table_id);
        if (!frontiers.contains(table_identity)) {
            const position: []const u8 = if (reader.frontier_fn) |f| try f(reader.ptr, a, source.table_id) else "";
            try frontiers.put(a, table_identity, position);
        }
        switch (source.kind) {
            .document => {
                resolved[i] = .{ .bound = bound, .members = &.{} };
                const key = bound.key orelse continue;
                const raw = (try reader.lookup_fn(reader.ptr, a, source.table_id, key)) orelse {
                    try absent.append(a, .{ .source = source.name, .table_id = table_identity, .key = key, .revision = @splat(0) });
                    continue;
                };
                const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, raw, .{});
                const member = ctx.ConsumedInput{ .key = key, .projection = try ctx.projectAlloc(a, parsed, source.fields) };
                const members = try a.alloc(ctx.ConsumedInput, 1);
                members[0] = member;
                resolved[i] = .{ .bound = bound, .members = members, .present = true };
                try inputs.append(a, .{ .source = source.name, .table_id = table_identity, .key = key, .revision = member.revision() });
            },
            .query => {
                var members = std.ArrayListUnmanaged(ctx.ConsumedInput).empty;
                if (!bound.unsatisfiable) {
                    const filter = try prefilterJsonAlloc(a, bound);
                    const budget = @max(limits.scan_floor, source.max_documents *| limits.scan_factor);
                    var offset: u32 = 0;
                    var seen = std.StringHashMapUnmanaged(void).empty;
                    while (true) {
                        if (offset >= budget) return error.ContextBudgetExceeded;
                        var page = try reader.query_fn(reader.ptr, alloc, source.table_id, filter, offset, limits.page_rows);
                        defer page.deinit(alloc);
                        for (page.rows) |row| {
                            if (seen.contains(row.key)) continue;
                            const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, row.value, .{});
                            if (!ctx.rowMatches(bound, parsed)) continue;
                            const key = try a.dupe(u8, row.key);
                            try seen.put(a, key, {});
                            try members.append(a, .{ .key = key, .projection = try ctx.projectAlloc(a, parsed, source.fields) });
                            if (members.items.len > source.max_documents) return error.ContextBudgetExceeded;
                        }
                        offset += @intCast(page.rows.len);
                        if (page.exhausted or page.rows.len == 0) break;
                    }
                }
                std.mem.sort(ctx.ConsumedInput, members.items, {}, struct {
                    fn lt(_: void, l: ctx.ConsumedInput, r: ctx.ConsumedInput) bool {
                        return std.mem.lessThan(u8, l.key, r.key);
                    }
                }.lt);
                for (members.items) |m| try inputs.append(a, .{ .source = source.name, .table_id = table_identity, .key = m.key, .revision = m.revision() });
                try queries.append(a, .{
                    .source = source.name,
                    .table_id = table_identity,
                    .query_identity = bound.query_identity,
                    .member_count = @intCast(members.items.len),
                    .membership = store.membershipDigest(members.items),
                    .complete = true,
                });
                resolved[i] = .{ .bound = bound, .members = members.items };
            },
        }
    }
    const rendered = try ctx.renderAlloc(alloc, resolved);
    errdefer {
        var r = rendered;
        r.deinit(alloc);
    }
    // Frontier vector: one opaque position per table identity, never a scalar
    // pretending to order independent tables.
    const SortCtx = struct {
        keys: [][]const u8,
        pub fn lessThan(self: @This(), l: usize, r: usize) bool {
            return std.mem.lessThan(u8, self.keys[l], self.keys[r]);
        }
    };
    frontiers.sort(SortCtx{ .keys = frontiers.keys() });
    var frontier = std.ArrayListUnmanaged(u8).empty;
    try frontier.append(a, '{');
    for (frontiers.keys(), frontiers.values(), 0..) |k, v, i| {
        if (i != 0) try frontier.append(a, ',');
        try ctx.appendJsonString(a, &frontier, k);
        try frontier.append(a, ':');
        try ctx.appendJsonString(a, &frontier, v);
    }
    try frontier.append(a, '}');
    return .{
        .arena = arena,
        .rendered = rendered,
        .inputs = inputs.items,
        .queries = queries.items,
        .absent = absent.items,
        .frontier = frontier.items,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

const testing = std.testing;

/// In-memory multi-table reader used by tests (and by embedded hosts' tests).
pub const MemoryTables = struct {
    alloc: Allocator,
    /// table_id -> sorted rows
    tables: std.StringArrayHashMapUnmanaged(std.ArrayListUnmanaged(Row)) = .empty,
    queries: usize = 0,

    pub fn deinit(self: *MemoryTables) void {
        for (self.tables.values()) |*rows| {
            for (rows.items) |row| {
                self.alloc.free(row.key);
                self.alloc.free(row.value);
            }
            rows.deinit(self.alloc);
        }
        for (self.tables.keys()) |k| self.alloc.free(k);
        self.tables.deinit(self.alloc);
    }

    pub fn put(self: *MemoryTables, table: []const u8, key: []const u8, value: ?[]const u8) !void {
        const slot = try self.tables.getOrPut(self.alloc, table);
        if (!slot.found_existing) {
            slot.key_ptr.* = try self.alloc.dupe(u8, table);
            slot.value_ptr.* = .empty;
        }
        const rows = slot.value_ptr;
        for (rows.items, 0..) |row, i| if (std.mem.eql(u8, row.key, key)) {
            self.alloc.free(row.value);
            if (value) |v| {
                rows.items[i].value = try self.alloc.dupe(u8, v);
            } else {
                self.alloc.free(row.key);
                _ = rows.orderedRemove(i);
            }
            return;
        };
        const v = value orelse return;
        var at: usize = 0;
        while (at < rows.items.len and std.mem.lessThan(u8, rows.items[at].key, key)) at += 1;
        try rows.insert(self.alloc, at, .{ .key = try self.alloc.dupe(u8, key), .value = try self.alloc.dupe(u8, v) });
    }

    pub fn reader(self: *MemoryTables, own: []const u8) Reader {
        return .{ .ptr = self, .own_table_id = own, .lookup_fn = lookup, .query_fn = query, .frontier_fn = frontier };
    }

    fn rowsFor(self: *MemoryTables, table_id: []const u8) ?*std.ArrayListUnmanaged(Row) {
        return self.tables.getPtr(table_id);
    }

    fn lookup(p: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8 {
        const self: *MemoryTables = @ptrCast(@alignCast(p));
        const rows = self.rowsFor(if (table_id.len == 0) "own" else table_id) orelse return null;
        for (rows.items) |row| if (std.mem.eql(u8, row.key, key)) return try alloc.dupe(u8, row.value);
        return null;
    }

    /// Unindexed prefilter: returns every row (worst-case recall-safe host).
    fn query(p: *anyopaque, alloc: Allocator, table_id: []const u8, _: []const u8, offset: u32, limit: u32) anyerror!Page {
        const self: *MemoryTables = @ptrCast(@alignCast(p));
        self.queries += 1;
        const rows = self.rowsFor(if (table_id.len == 0) "own" else table_id) orelse return .{ .rows = &.{}, .exhausted = true };
        const start = @min(offset, rows.items.len);
        const end = @min(rows.items.len, start + limit);
        const out = try alloc.alloc(Row, end - start);
        for (rows.items[start..end], out) |row, *o| o.* = .{ .key = try alloc.dupe(u8, row.key), .value = try alloc.dupe(u8, row.value) };
        return .{ .rows = out, .exhausted = end == rows.items.len };
    }

    fn frontier(_: *anyopaque, alloc: Allocator, table_id: []const u8) anyerror![]u8 {
        return std.fmt.allocPrint(alloc, "pos:{s}", .{table_id});
    }
};

const issue_sources =
    \\{"organization":{"document":{"table":"organizations","key_from":"/organization_id"},"fields":["voice","guidance"],"required":true},
    \\ "policies":{"query":{"table":"policies","filter_query":{"conjuncts":[
    \\   {"field":"tenant_id","term":{"$source":"/tenant_id"}},{"field":"organization_id","term":{"$source":"/organization_id"}},
    \\   {"field":"locale","term":{"$source":"/locale"}},{"field":"status","term":"approved"}]}},
    \\   "fields":["policy_id","revision","payload"],"max_documents":3}}
;

fn testConfig(alloc: Allocator) !ctx.Config {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), issue_sources, .{});
    const Resolve = struct {
        fn resolve(_: *anyopaque, a: Allocator, table: []const u8) anyerror!?[]u8 {
            return try std.fmt.allocPrint(a, "id-{s}", .{table});
        }
    };
    var dummy: u8 = 0;
    const stored = try ctx.canonicalFromPublicAlloc(alloc, value, null, .{ .ptr = &dummy, .resolve_fn = Resolve.resolve });
    defer alloc.free(stored);
    return ctx.parseStoredJson(alloc, stored);
}

fn bindJson(alloc: Allocator, config: *const ctx.Config, raw: []const u8) !ctx.Bindings {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const doc = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), raw, .{});
    return ctx.bindAlloc(alloc, config, doc);
}

test "resolution renders the issue example and records lineage and frontier vector" {
    const alloc = testing.allocator;
    var tables = MemoryTables{ .alloc = alloc };
    defer tables.deinit();
    try tables.put("id-organizations", "org-1", "{\"voice\":\"warm\",\"guidance\":\"be brief\",\"logo\":\"x\"}");
    try tables.put("id-policies", "p-2", "{\"policy_id\":\"p-2\",\"revision\":2,\"payload\":\"b\",\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    try tables.put("id-policies", "p-1", "{\"policy_id\":\"p-1\",\"revision\":1,\"payload\":\"a\",\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    try tables.put("id-policies", "p-3", "{\"policy_id\":\"p-3\",\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"draft\"}");
    try tables.put("id-policies", "p-4", "{\"policy_id\":\"p-4\",\"tenant_id\":\"t2\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    var config = try testConfig(alloc);
    defer config.deinit();
    var bindings = try bindJson(alloc, &config, "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\"}");
    defer bindings.deinit();
    var resolution = try resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{ .page_rows = 2 });
    defer resolution.deinit(alloc);
    try testing.expectEqualStrings(
        "{\"organization\":{\"key\":\"org-1\",\"value\":{\"guidance\":\"be brief\",\"voice\":\"warm\"}},\"policies\":{\"count\":2,\"members\":[{\"key\":\"p-1\",\"value\":{\"payload\":\"a\",\"policy_id\":\"p-1\",\"revision\":1}},{\"key\":\"p-2\",\"value\":{\"payload\":\"b\",\"policy_id\":\"p-2\",\"revision\":2}}]}}",
        resolution.rendered.json,
    );
    try testing.expectEqual(@as(usize, 3), resolution.inputs.len);
    try testing.expectEqual(@as(usize, 1), resolution.queries.len);
    try testing.expectEqual(@as(u32, 2), resolution.queries[0].member_count);
    try testing.expectEqualStrings("{\"id-organizations\":\"pos:id-organizations\",\"id-policies\":\"pos:id-policies\"}", resolution.frontier);

    // Equivalent inputs in a different storage order fingerprint identically.
    var shuffled = MemoryTables{ .alloc = alloc };
    defer shuffled.deinit();
    for ([_][]const u8{ "p-4", "p-3", "p-2", "p-1" }) |key| {
        for (tables.tables.getPtr("id-policies").?.items) |row| if (std.mem.eql(u8, row.key, key)) try shuffled.put("id-policies", key, row.value);
    }
    try shuffled.put("id-organizations", "org-1", "{\"guidance\":\"be brief\",\"voice\":\"warm\",\"logo\":\"y\"}");
    var again = try resolveAlloc(alloc, shuffled.reader("id-docs"), &bindings, .{});
    defer again.deinit(alloc);
    try testing.expectEqualSlices(u8, &resolution.rendered.fingerprint, &again.rendered.fingerprint);
}

test "resolution fails explicitly on overflow and missing required inputs, retains empty queries" {
    const alloc = testing.allocator;
    var tables = MemoryTables{ .alloc = alloc };
    defer tables.deinit();
    var config = try testConfig(alloc);
    defer config.deinit();
    var bindings = try bindJson(alloc, &config, "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\"}");
    defer bindings.deinit();
    try testing.expectError(error.RequiredContextMissing, resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{}));

    try tables.put("id-organizations", "org-1", "{\"voice\":\"v\"}");
    var empty = try resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{});
    defer empty.deinit(alloc);
    try testing.expect(std.mem.indexOf(u8, empty.rendered.json, "\"policies\":{\"count\":0,\"members\":[]}") != null);
    try testing.expectEqual(@as(usize, 1), empty.queries.len);

    var buf: [16]u8 = undefined;
    for (0..4) |i| {
        try tables.put("id-policies", try std.fmt.bufPrint(&buf, "p-{d}", .{i}), "{\"tenant_id\":\"t1\",\"organization_id\":\"org-1\",\"locale\":\"en-GB\",\"status\":\"approved\"}");
    }
    try testing.expectError(error.ContextBudgetExceeded, resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{}));
    // A prefilter scan budget smaller than the candidate set also refuses.
    try testing.expectError(error.ContextBudgetExceeded, resolveAlloc(alloc, tables.reader("id-docs"), &bindings, .{ .page_rows = 1, .scan_floor = 2, .scan_factor = 0 }));
}

test "prefilter pushes down only string conjuncts" {
    const alloc = testing.allocator;
    var config = try ctx.parsePublicJson(alloc,
        \\{"q":{"query":{"filter_query":{"conjuncts":[{"field":"org","term":{"$source":"/org"}},{"field":"n","term":3},{"field":"locale","terms":["en","fr"]}]}},"fields":["x"]}}
    , "");
    defer config.deinit();
    var bindings = try bindJson(alloc, &config, "{\"org\":\"o\"}");
    defer bindings.deinit();
    const filter = try prefilterJsonAlloc(alloc, &bindings.items[0]);
    defer alloc.free(filter);
    try testing.expectEqualStrings(
        "{\"conjuncts\":[{\"disjuncts\":[{\"match\":\"en\",\"field\":\"locale\",\"operator\":\"and\"},{\"match\":\"fr\",\"field\":\"locale\",\"operator\":\"and\"}]},{\"match\":\"o\",\"field\":\"org\",\"operator\":\"and\"}]}",
        filter,
    );
}
