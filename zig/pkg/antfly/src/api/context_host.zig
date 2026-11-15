// Copyright 2026 Antfly, Inc.
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

//! Context-source host for distributed tables (#1634).
//!
//! Serves the storage owner's context-dependency operations
//! (storage/db/context_host_json.zig) on top of server primitives: Raft
//! proposals to a group, group leadership, the groups of a table, routed
//! reads, and the metadata catalog. Table identities are `srv:<table_id>`
//! (the immutable metadata incarnation), so renaming or recreating a table
//! can never retarget a dependency.
//!
//! Admission helpers canonicalize a public `context_sources` declaration
//! against the catalog, reject cross-table generated-input cycles, and reject
//! source kinds that need shard-local indexes (graph traversal, generated
//! artifact values, and the search shorthands) on distributed tables.

const std = @import("std");
const Allocator = std.mem.Allocator;
const local = @import("antfly_local_sources");
const ctx = local.storage_db_enrichment_context_sources;
const notices = local.storage_db_enrichment_context_notices;
const command_mod = local.storage_db_enrichment_context_dependency_command;
const host_json = local.storage_db_context_host_wire;
const table_reads = local.api_table_read_source;
const db_types = local.storage_db_types;

pub const identity_prefix = "srv:";

pub fn identityAlloc(alloc: Allocator, table_id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, identity_prefix ++ "{d}", .{table_id});
}

pub fn parseIdentity(identity: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, identity, identity_prefix)) return null;
    return std.fmt.parseInt(u64, identity[identity_prefix.len..], 10) catch null;
}

/// The catalog fields context sources need.
pub const TableInfo = struct {
    table_id: u64,
    name: []const u8,
    indexes_json: []const u8,
};

fn findByName(tables: []const TableInfo, name: []const u8) ?TableInfo {
    for (tables) |t| if (std.mem.eql(u8, t.name, name)) return t;
    return null;
}

fn findByIdentity(tables: []const TableInfo, identity: []const u8) ?TableInfo {
    const id = parseIdentity(identity) orelse return null;
    for (tables) |t| if (t.table_id == id) return t;
    return null;
}

/// Every enrichment declaration of every table, with server identities.
pub fn declarationsAlloc(a: Allocator, tables: []const TableInfo, collect: CollectEnrichments) ![]ctx.DeclaredEnrichment {
    var out = std.ArrayListUnmanaged(ctx.DeclaredEnrichment).empty;
    for (tables) |t| {
        const configs = collect.collect_fn(a, t.indexes_json) catch continue;
        const identity = try identityAlloc(a, t.table_id);
        for (configs) |cfg| try out.append(a, .{
            .table_id = identity,
            .name = cfg.name,
            .source_artifact = cfg.source_artifact_name,
            .context_sources_json = cfg.context_sources_json,
        });
    }
    return out.items;
}

/// Parses a table's stored indexes JSON into its enrichment configs.
pub const CollectEnrichments = struct {
    collect_fn: *const fn (a: Allocator, indexes_json: []const u8) anyerror![]const db_types.EnrichmentConfig,
};

/// `{"table_id","inbound":[DeclaredConfig]}` for `table_name`: other tables'
/// enrichments whose live context sources read it.
pub fn topologyJsonAlloc(alloc: Allocator, tables: []const TableInfo, collect: CollectEnrichments, table_name: []const u8) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const own = findByName(tables, table_name) orelse return error.ContextSourceTableUnavailable;
    const own_identity = try identityAlloc(a, own.table_id);
    const declared = try declarationsAlloc(a, tables, collect);
    const inbound = try ctx.inboundDeclarationsAlloc(a, declared, own_identity);
    const wire = try a.alloc(notices.DeclaredConfig, inbound.len);
    for (inbound, wire) |d, *w| w.* = .{ .dependent_table_id = d.table_id, .artifact = d.name, .stored_json = d.context_sources_json };
    // Deterministic order: the replicated topology compares byte-for-byte.
    std.mem.sort(notices.DeclaredConfig, wire, {}, struct {
        fn lt(_: void, l: notices.DeclaredConfig, r: notices.DeclaredConfig) bool {
            return switch (std.mem.order(u8, l.dependent_table_id, r.dependent_table_id)) {
                .lt => true,
                .gt => false,
                .eq => std.mem.lessThan(u8, l.artifact, r.artifact),
            };
        }
    }.lt);
    return std.json.Stringify.valueAlloc(alloc, .{ .table_id = own_identity, .inbound = wire }, .{});
}

/// Table-name resolution for admission.
pub fn resolver(tables: *const []const TableInfo) ctx.TableResolver {
    return .{ .ptr = @constCast(tables), .resolve_fn = resolveName };
}

fn resolveName(ptr: *anyopaque, alloc: Allocator, name: []const u8) anyerror!?[]u8 {
    const tables: *const []const TableInfo = @ptrCast(@alignCast(ptr));
    const found = findByName(tables.*, name) orelse return null;
    return try identityAlloc(alloc, found.table_id);
}

/// Admission for a distributed table: canonicalize the public declaration
/// (table names to identities, row filters lowered by `lowering`), reject
/// kinds that need shard-local indexes, and reject generated-input cycles
/// across tables. Returns the canonical stored JSON.
pub fn admitAlloc(
    alloc: Allocator,
    tables: []const TableInfo,
    collect: CollectEnrichments,
    owner_table: []const u8,
    enrichment_name: []const u8,
    sources_value: std.json.Value,
    refresh_value: ?std.json.Value,
    lowering: ?ctx.FilterLowering,
) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const owner = findByName(tables, owner_table) orelse return error.ContextSourceTableUnavailable;
    var table_list = tables;
    const stored = try ctx.canonicalFromPublicAlloc(a, sources_value, refresh_value, resolver(&table_list), lowering);
    var config = try ctx.parseStoredJson(a, stored);
    defer config.deinit();
    for (config.sources) |source| {
        if (source.kind == .graph) return error.UnsupportedContextPredicate;
        if (source.artifact_names.len != 0) return error.UnsupportedContextPredicate;
        if (source.kind == .search and source.search.request.len == 0) return error.UnsupportedContextPredicate;
    }
    const owner_identity = try identityAlloc(a, owner.table_id);
    const declared = try declarationsAlloc(a, tables, collect);
    // The candidate replaces any previous declaration of the same name.
    var others = std.ArrayListUnmanaged(ctx.DeclaredEnrichment).empty;
    for (declared) |d| {
        if (std.mem.eql(u8, d.table_id, owner_identity) and std.mem.eql(u8, d.name, enrichment_name)) continue;
        try others.append(a, d);
    }
    try ctx.rejectCrossTableCycle(a, others.items, owner_identity, enrichment_name, stored);
    return alloc.dupe(u8, stored);
}

/// Server primitives the host is built on.
pub const Primitives = struct {
    ptr: *anyopaque,
    /// Propose `command` to `group_id` of `table_name`; returns once applied
    /// on this replica (the leader). `remote` allows forwarding to a leader
    /// on another node (cross-group delivery).
    propose_fn: *const fn (ptr: *anyopaque, alloc: Allocator, group_id: u64, table_name: []const u8, command: command_mod.Command, remote: bool) anyerror!void,
    is_leader_fn: *const fn (ptr: *anyopaque, group_id: u64) bool,
    /// Every group of a table.
    groups_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table_name: []const u8) anyerror![]const u64,
    /// The catalog (allocated in `a`).
    tables_fn: *const fn (ptr: *anyopaque, a: Allocator) anyerror![]const TableInfo,
    reads_fn: *const fn (ptr: *anyopaque) table_reads.TableReadSource,
    collect: CollectEnrichments,
    /// Parses a public QueryRequest body for routed execution.
    parse_query_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table_name: []const u8, body: []const u8) anyerror!db_types.SearchRequest,
};

pub const max_scan_rows: u32 = 100_000;

pub const Host = struct {
    primitives: Primitives,

    /// One context operation for the owner of `group_id`/`table_name`.
    pub fn call(self: *Host, alloc: Allocator, op_raw: u32, group_id: u64, table_name: []const u8, request: []const u8) ![]u8 {
        const op = std.enums.fromInt(host_json.Op, op_raw) orelse return error.InvalidArgument;
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const a = arena.allocator();
        const p = self.primitives;
        switch (op) {
            .propose => {
                const command = try std.json.parseFromSliceLeaky(command_mod.Command, a, request, .{ .allocate = .alloc_always });
                try p.propose_fn(p.ptr, alloc, group_id, table_name, command, false);
                return alloc.dupe(u8, "");
            },
            .is_leader => return alloc.dupe(u8, if (p.is_leader_fn(p.ptr, group_id)) "1" else "0"),
            .deliver => {
                const body = try std.json.parseFromSliceLeaky(struct { dependent: []const u8, notice: []const u8 }, a, request, .{ .allocate = .alloc_always });
                const dependent = if (body.dependent.len == 0) table_name else blk: {
                    const tables = try p.tables_fn(p.ptr, a);
                    break :blk (findByIdentity(tables, body.dependent) orelse return error.ContextDependentUnavailable).name;
                };
                // Every group holds part of the dependent table's dependents;
                // inbox appends are idempotent (keyed by notice digest).
                const groups = try p.groups_fn(p.ptr, a, dependent);
                for (groups) |g| try p.propose_fn(p.ptr, alloc, g, dependent, .{ .inbox = &.{body.notice} }, true);
                return alloc.dupe(u8, "");
            },
            .lookup => {
                const body = try std.json.parseFromSliceLeaky(struct { table: []const u8, key: []const u8 }, a, request, .{ .allocate = .alloc_always });
                const name = try self.tableName(a, body.table, table_name);
                var response = (try p.reads_fn(p.ptr).lookup(alloc, name, body.key, .{}, .read_index)) orelse
                    return std.json.Stringify.valueAlloc(alloc, .{ .value = @as(?[]const u8, null) }, .{});
                defer response.deinit(alloc);
                return std.json.Stringify.valueAlloc(alloc, .{ .value = @as(?[]const u8, response.json) }, .{});
            },
            .query => {
                const body = try std.json.parseFromSliceLeaky(struct { table: []const u8, envelope: []const u8, offset: u32, limit: u32 }, a, request, .{ .allocate = .alloc_always });
                const name = try self.tableName(a, body.table, table_name);
                const envelope = try std.json.parseFromSliceLeaky(std.json.Value, a, body.envelope, .{});
                const scan_filter: []const u8 = if (envelope == .object) if (envelope.object.get("scan")) |v| (if (v == .null) "" else try std.json.Stringify.valueAlloc(a, v, .{})) else "" else "";
                const want = @min(@as(u64, body.offset) + body.limit + 1, max_scan_rows);
                var response = (try p.reads_fn(p.ptr).scan(alloc, name, "", "", .{ .include_documents = false, .filter_query_json = scan_filter, .limit = @intCast(want) }, .read_index)) orelse
                    return host_json.encodePageAlloc(alloc, &.{}, true);
                defer response.deinit(alloc);
                var keys = std.ArrayListUnmanaged([]const u8).empty;
                var lines = std.mem.splitScalar(u8, response.ndjson, '\n');
                while (lines.next()) |line| {
                    if (line.len == 0) continue;
                    const wire = try std.json.parseFromSliceLeaky(struct { _id: []const u8 }, a, line, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
                    try keys.append(a, wire._id);
                }
                const start = @min(keys.items.len, body.offset);
                const end = @min(keys.items.len, @as(usize, body.offset) + body.limit);
                var rows = std.ArrayListUnmanaged(local.storage_db_enrichment_context_resolution.Row).empty;
                for (keys.items[start..end]) |key| {
                    var doc = (try p.reads_fn(p.ptr).lookup(alloc, name, key, .{}, .read_index)) orelse continue;
                    defer doc.deinit(alloc);
                    try rows.append(a, .{ .key = try a.dupe(u8, key), .value = try a.dupe(u8, doc.json) });
                }
                return host_json.encodePageAlloc(alloc, rows.items, keys.items.len <= end);
            },
            .request => {
                const body = try std.json.parseFromSliceLeaky(struct { table: []const u8, request: []const u8, k: u32 }, a, request, .{ .allocate = .alloc_always });
                const name = try self.tableName(a, body.table, table_name);
                var req = try p.parse_query_fn(p.ptr, a, name, body.request);
                req.include_stored = true;
                req.include_all_fields = true;
                if (req.limit == 0 or req.limit > body.k) req.limit = body.k;
                var response = (try p.reads_fn(p.ptr).query(alloc, name, req, .read_index)) orelse return error.ContextSearchUnavailable;
                defer alloc.free(response.json);
                return hitsPageAlloc(alloc, a, response.json);
            },
            .topology => {
                const tables = try p.tables_fn(p.ptr, a);
                return topologyJsonAlloc(alloc, tables, p.collect, table_name);
            },
        }
    }

    fn tableName(self: *Host, a: Allocator, identity: []const u8, own: []const u8) ![]const u8 {
        if (identity.len == 0) return own;
        const p = self.primitives;
        const tables = try p.tables_fn(p.ptr, a);
        return (findByIdentity(tables, identity) orelse return error.ContextSourceTableUnavailable).name;
    }
};

/// `{"responses":[{"hits":{"hits":[{"_id","_source"}]}}]}` to a page.
fn hitsPageAlloc(alloc: Allocator, a: Allocator, body: []const u8) ![]u8 {
    const parsed = try std.json.parseFromSliceLeaky(std.json.Value, a, body, .{});
    var rows = std.ArrayListUnmanaged(local.storage_db_enrichment_context_resolution.Row).empty;
    hits: {
        const responses = parsed.object.get("responses") orelse break :hits;
        if (responses != .array or responses.array.items.len == 0) break :hits;
        const first = responses.array.items[0];
        if (first != .object) break :hits;
        const outer = first.object.get("hits") orelse break :hits;
        if (outer != .object) break :hits;
        const list = outer.object.get("hits") orelse break :hits;
        if (list != .array) break :hits;
        for (list.array.items) |hit| {
            if (hit != .object) continue;
            const id = hit.object.get("_id") orelse continue;
            if (id != .string) continue;
            const source = hit.object.get("_source") orelse continue;
            try rows.append(a, .{ .key = try a.dupe(u8, id.string), .value = try std.json.Stringify.valueAlloc(a, source, .{}) });
        }
    }
    return host_json.encodePageAlloc(alloc, rows.items, true);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn testCollect(a: Allocator, indexes_json: []const u8) anyerror![]const db_types.EnrichmentConfig {
    // Test catalogs carry `{"enrichments":[{"name","context_sources_json"}]}`.
    const parsed = try std.json.parseFromSliceLeaky(struct { enrichments: []const struct { name: []const u8, context_sources_json: []const u8 = "", source_artifact_name: []const u8 = "" } = &.{} }, a, indexes_json, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
    const out = try a.alloc(db_types.EnrichmentConfig, parsed.enrichments.len);
    for (parsed.enrichments, out) |e, *c| c.* = .{ .name = e.name, .kind = .asset, .context_sources_json = e.context_sources_json, .source_artifact_name = e.source_artifact_name };
    return out;
}

test "context host admission resolves names, rejects shard-local kinds, and computes inbound topology" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const collect = CollectEnrichments{ .collect_fn = testCollect };
    const parse = struct {
        fn of(pa: Allocator, raw: []const u8) !std.json.Value {
            return std.json.parseFromSliceLeaky(std.json.Value, pa, raw, .{});
        }
    };
    var tables = [_]TableInfo{
        .{ .table_id = 7, .name = "orgs", .indexes_json = "{}" },
        .{ .table_id = 9, .name = "docs", .indexes_json = "{}" },
    };
    const stored = try admitAlloc(alloc, &tables, collect, "docs", "summary", try parse.of(a,
        \\{"org":{"document":{"table":"orgs","key_from":"/org"},"fields":["name"]}}
    ), null, null);
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"table_id\":\"srv:7\"") != null);
    try std.testing.expectError(error.UnknownContextSourceTable, admitAlloc(alloc, &tables, collect, "docs", "s", try parse.of(a,
        \\{"x":{"document":{"table":"missing","key_from":"/k"},"fields":["a"]}}
    ), null, null));
    try std.testing.expectError(error.UnsupportedContextPredicate, admitAlloc(alloc, &tables, collect, "docs", "s", try parse.of(a,
        \\{"g":{"graph":{"graph_index":"refs"},"fields":["a"]}}
    ), null, null));
    try std.testing.expectError(error.UnsupportedContextPredicate, admitAlloc(alloc, &tables, collect, "docs", "s", try parse.of(a,
        \\{"x":{"query":{"table":"orgs","filter_query":{"field":"k","term":"v"}},"fields":["_artifacts.tag"]}}
    ), null, null));

    // Inbound topology of `orgs` lists the `docs` declaration.
    tables[1].indexes_json = try std.fmt.allocPrint(a, "{{\"enrichments\":[{{\"name\":\"summary\",\"context_sources_json\":{f}}}]}}", .{std.json.fmt(stored, .{})});
    const topology = try topologyJsonAlloc(alloc, &tables, collect, "orgs");
    defer alloc.free(topology);
    try std.testing.expect(std.mem.indexOf(u8, topology, "\"table_id\":\"srv:7\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, topology, "\"dependent_table_id\":\"srv:9\"") != null);
    const own = try topologyJsonAlloc(alloc, &tables, collect, "docs");
    defer alloc.free(own);
    try std.testing.expect(std.mem.indexOf(u8, own, "\"inbound\":[]") != null);
}

test "context host delivers notices to every group of the dependent table" {
    const alloc = std.testing.allocator;
    const Fake = struct {
        proposals: std.ArrayListUnmanaged(struct { group: u64, inbox: usize, remote: bool }) = .empty,
        fn propose(ptr: *anyopaque, _: Allocator, group_id: u64, _: []const u8, command: command_mod.Command, remote: bool) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.proposals.append(std.testing.allocator, .{ .group = group_id, .inbox = command.inbox.len, .remote = remote });
        }
        fn isLeader(_: *anyopaque, group_id: u64) bool {
            return group_id == 1;
        }
        fn groups(_: *anyopaque, a: Allocator, name: []const u8) anyerror![]const u64 {
            const out = try a.alloc(u64, if (std.mem.eql(u8, name, "docs")) 3 else 1);
            for (out, 0..) |*g, i| g.* = 10 + i;
            return out;
        }
        fn tables(_: *anyopaque, a: Allocator) anyerror![]const TableInfo {
            const out = try a.alloc(TableInfo, 2);
            out[0] = .{ .table_id = 7, .name = "orgs", .indexes_json = "{}" };
            out[1] = .{ .table_id = 9, .name = "docs", .indexes_json = "{}" };
            return out;
        }
        fn reads(_: *anyopaque) table_reads.TableReadSource {
            unreachable;
        }
        fn parseQuery(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) anyerror!db_types.SearchRequest {
            return error.UnsupportedQueryRequest;
        }
    };
    var fake = Fake{};
    defer fake.proposals.deinit(alloc);
    var host = Host{ .primitives = .{ .ptr = &fake, .propose_fn = Fake.propose, .is_leader_fn = Fake.isLeader, .groups_fn = Fake.groups, .tables_fn = Fake.tables, .reads_fn = Fake.reads, .collect = .{ .collect_fn = testCollect }, .parse_query_fn = Fake.parseQuery } };
    const out = try host.call(alloc, @backingInt(host_json.Op.deliver), 10, "orgs", "{\"dependent\":\"srv:9\",\"notice\":\"n\"}");
    alloc.free(out);
    try std.testing.expectEqual(@as(usize, 3), fake.proposals.items.len);
    for (fake.proposals.items) |p| try std.testing.expect(p.remote and p.inbox == 1);
    // Own-table notices go to every group of the owner's own table.
    const own = try host.call(alloc, @backingInt(host_json.Op.deliver), 10, "orgs", "{\"dependent\":\"\",\"notice\":\"n\"}");
    alloc.free(own);
    try std.testing.expectEqual(@as(usize, 4), fake.proposals.items.len);
    const leader = try host.call(alloc, @backingInt(host_json.Op.is_leader), 1, "orgs", "");
    defer alloc.free(leader);
    try std.testing.expectEqualStrings("1", leader);
    try std.testing.expectError(error.ContextDependentUnavailable, host.call(alloc, @backingInt(host_json.Op.deliver), 10, "orgs", "{\"dependent\":\"srv:404\",\"notice\":\"n\"}"));
}
