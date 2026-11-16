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
//! against the catalog and reject cross-table generated-input cycles. Graph
//! adjacency and generated-artifact values are read from the group owning
//! the key; the search shorthands run as QueryRequests.

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

/// Maps a public (logical) table name to its storage name, authorizing the
/// caller to read it. Returns null when no such table exists. Server catalog
/// entries are keyed by storage name (`table:<hash>`), so admission must not
/// match public names against them directly.
pub const PublicNameResolver = struct {
    ptr: *anyopaque,
    resolve_fn: *const fn (ptr: *anyopaque, alloc: Allocator, public_name: []const u8) anyerror!?[]u8,
};

const NameLookup = struct {
    tables: []const TableInfo,
    names: ?PublicNameResolver,
};

/// Table-name resolution for admission.
fn resolver(lookup: *const NameLookup) ctx.TableResolver {
    return .{ .ptr = @ptrCast(@constCast(lookup)), .resolve_fn = resolveName };
}

fn resolveName(ptr: *anyopaque, alloc: Allocator, name: []const u8) anyerror!?[]u8 {
    const lookup: *const NameLookup = @ptrCast(@alignCast(ptr));
    const names = lookup.names orelse {
        const found = findByName(lookup.tables, name) orelse return null;
        return try identityAlloc(alloc, found.table_id);
    };
    const storage = (try names.resolve_fn(names.ptr, alloc, name)) orelse return null;
    defer alloc.free(storage);
    const found = findByName(lookup.tables, storage) orelse return null;
    return try identityAlloc(alloc, found.table_id);
}

/// Admission for a distributed table: canonicalize the public declaration
/// (table names to identities, row filters lowered by `lowering`) and reject
/// generated-input cycles across tables. Returns the canonical stored JSON.
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
    return admitResolvingAlloc(alloc, tables, collect, owner_table, enrichment_name, sources_value, refresh_value, lowering, null);
}

/// `admitAlloc` where `owner_table` is a storage name and declared source
/// tables are public names mapped through `names`.
pub fn admitResolvingAlloc(
    alloc: Allocator,
    tables: []const TableInfo,
    collect: CollectEnrichments,
    owner_table: []const u8,
    enrichment_name: []const u8,
    sources_value: std.json.Value,
    refresh_value: ?std.json.Value,
    lowering: ?ctx.FilterLowering,
    names: ?PublicNameResolver,
) ![]u8 {
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const owner = findByName(tables, owner_table) orelse return error.ContextSourceTableUnavailable;
    const lookup = NameLookup{ .tables = tables, .names = names };
    const stored = try ctx.canonicalFromPublicAlloc(a, sources_value, refresh_value, resolver(&lookup), lowering);
    var config = try ctx.parseStoredJson(a, stored);
    defer config.deinit();
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
    /// The group owning `key` in `table_name`.
    group_for_key_fn: *const fn (ptr: *anyopaque, alloc: Allocator, table_name: []const u8, key: []const u8) anyerror!u64,
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
                // Offset continuation without a cursor (rare: resolution pages
                // sequentially and the storage reader resumes by key).
                const body = try std.json.parseFromSliceLeaky(struct { table: []const u8, envelope: []const u8, offset: u32, limit: u32 }, a, request, .{ .allocate = .alloc_always });
                const name = try self.tableName(a, body.table, table_name);
                const want = @min(@as(u64, body.offset) + body.limit + 1, max_scan_rows);
                const keys = try scanKeys(p, alloc, a, name, try scanFilter(a, body.envelope), "", @intCast(want));
                const start = @min(keys.len, body.offset);
                const end = @min(keys.len, @as(usize, body.offset) + body.limit);
                const rows = try hydrateRows(p, alloc, a, name, keys[start..end]);
                return host_json.encodePageAlloc(alloc, rows, keys.len <= end);
            },
            .scan => {
                const body = try std.json.parseFromSliceLeaky(struct { table: []const u8, envelope: []const u8, after: []const u8, limit: u32 }, a, request, .{ .allocate = .alloc_always });
                const name = try self.tableName(a, body.table, table_name);
                const limit = @min(body.limit, max_scan_rows);
                const keys = try scanKeys(p, alloc, a, name, try scanFilter(a, body.envelope), body.after, limit + 1);
                const page = keys[0..@min(keys.len, limit)];
                const rows = try hydrateRows(p, alloc, a, name, page);
                return host_json.encodePageAlloc(alloc, rows, keys.len <= limit);
            },
            .edges => {
                const body = try std.json.parseFromSliceLeaky(struct { table: []const u8, graph_index: []const u8, node: []const u8, edge_types: []const []const u8 = &.{}, direction: []const u8 = "out", limit: u32 = 0, include_generated: bool = false }, a, request, .{ .allocate = .alloc_always });
                const name = try self.tableName(a, body.table, table_name);
                const read = try std.json.Stringify.valueAlloc(a, .{ .mode = "context_read", .context = .{ .op = "edges", .key = body.node, .graph_index = body.graph_index, .edge_types = body.edge_types, .direction = body.direction, .limit = body.limit, .include_generated = body.include_generated } }, .{});
                return self.groupRead(alloc, a, name, body.node, read);
            },
            .artifacts => {
                const body = try std.json.parseFromSliceLeaky(struct { table: []const u8, key: []const u8, names: []const []const u8 }, a, request, .{ .allocate = .alloc_always });
                const name = try self.tableName(a, body.table, table_name);
                const read = try std.json.Stringify.valueAlloc(a, .{ .mode = "context_read", .context = .{ .op = "artifacts", .key = body.key, .names = body.names } }, .{});
                return self.groupRead(alloc, a, name, body.key, read);
            },
            .request => {
                const body = try std.json.parseFromSliceLeaky(struct { table: []const u8, request: []const u8, k: u32 }, a, request, .{ .allocate = .alloc_always });
                const name = try self.tableName(a, body.table, table_name);
                var req = try p.parse_query_fn(p.ptr, a, name, body.request);
                req.include_stored = true;
                req.include_all_fields = true;
                if (req.limit == 0 or req.limit > body.k) req.limit = body.k;
                const response = (try p.reads_fn(p.ptr).query(alloc, name, req, .read_index)) orelse return error.ContextSearchUnavailable;
                defer alloc.free(response.json);
                return hitsPageAlloc(alloc, a, response.json);
            },
            .topology => {
                const tables = try p.tables_fn(p.ptr, a);
                return topologyJsonAlloc(alloc, tables, p.collect, table_name);
            },
        }
    }

    /// A group-local context read on the group owning `key`.
    fn groupRead(self: *Host, alloc: Allocator, a: Allocator, table: []const u8, key: []const u8, read_json: []const u8) ![]u8 {
        const p = self.primitives;
        const group = try p.group_for_key_fn(p.ptr, a, table, key);
        var response = (try p.reads_fn(p.ptr).lookupGroupLocal(alloc, group, table, "", .{ .relational_topology_json = read_json }, .read_index)) orelse return error.ContextSourceTableUnavailable;
        defer response.deinit(alloc);
        return alloc.dupe(u8, response.json);
    }

    fn tableName(self: *Host, a: Allocator, identity: []const u8, own: []const u8) ![]const u8 {
        if (identity.len == 0) return own;
        const p = self.primitives;
        const tables = try p.tables_fn(p.ptr, a);
        return (findByIdentity(tables, identity) orelse return error.ContextSourceTableUnavailable).name;
    }
};

fn scanFilter(a: Allocator, envelope_json: []const u8) ![]const u8 {
    const envelope = try std.json.parseFromSliceLeaky(std.json.Value, a, envelope_json, .{});
    if (envelope != .object) return "";
    const v = envelope.object.get("scan") orelse return "";
    if (v == .null) return "";
    return std.json.Stringify.valueAlloc(a, v, .{});
}

/// Keys of the rows matching the storage filter (evaluated by every group),
/// in key order, after `after` (exclusive; "" from the start).
fn scanKeys(p: Primitives, alloc: Allocator, a: Allocator, table: []const u8, filter: []const u8, after: []const u8, limit: u32) ![]const []const u8 {
    var response = (try p.reads_fn(p.ptr).scan(alloc, table, after, "", .{ .include_documents = false, .inclusive_from = after.len == 0, .filter_query_json = filter, .limit = limit }, .read_index)) orelse return &.{};
    defer response.deinit(alloc);
    var keys = std.ArrayListUnmanaged([]const u8).empty;
    var lines = std.mem.splitScalar(u8, response.ndjson, '\n');
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const wire = try std.json.parseFromSliceLeaky(struct { _id: []const u8 }, a, line, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        if (after.len != 0 and std.mem.eql(u8, wire._id, after)) continue;
        try keys.append(a, wire._id);
    }
    return keys.items;
}

/// Logical documents for `keys`, hydrated in bounded exact-ID batches
/// through the routed read fence (one round trip per 64 keys, not per key).
fn hydrateRows(p: Primitives, alloc: Allocator, a: Allocator, table: []const u8, keys: []const []const u8) ![]local.storage_db_enrichment_context_resolution.Row {
    var rows = std.ArrayListUnmanaged(local.storage_db_enrichment_context_resolution.Row).empty;
    var start: usize = 0;
    while (start < keys.len) {
        const batch = keys[start..@min(keys.len, start + 64)];
        start += batch.len;
        var hydrated = (try p.reads_fn(p.ptr).hydrateDocuments(alloc, table, .{
            .document_authority_mode = .lookup,
            .query = .match_all,
            .filter_doc_ids_positive = true,
            .filter_doc_ids = batch,
            .include_stored = true,
            .limit = @intCast(batch.len),
        }, .read_index)) orelse continue;
        defer hydrated.deinit();
        for (batch, hydrated.rows) |key, doc| {
            const value = doc orelse continue;
            try rows.append(a, .{ .key = try a.dupe(u8, key), .value = try a.dupe(u8, value) });
        }
    }
    return rows.items;
}

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

test "context host admission maps public source names to storage names" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const collect = CollectEnrichments{ .collect_fn = testCollect };
    // Server catalogs key tables by storage name, never by public name.
    const tables = [_]TableInfo{
        .{ .table_id = 7, .name = "table:aa", .indexes_json = "{}" },
        .{ .table_id = 9, .name = "table:bb", .indexes_json = "{}" },
    };
    const Names = struct {
        fn resolve(_: *anyopaque, ra: Allocator, public_name: []const u8) anyerror!?[]u8 {
            if (std.mem.eql(u8, public_name, "orgs")) return try ra.dupe(u8, "table:aa");
            if (std.mem.eql(u8, public_name, "secret")) return error.Forbidden;
            return null;
        }
    };
    var unused: u8 = 0;
    const names = PublicNameResolver{ .ptr = &unused, .resolve_fn = Names.resolve };
    const sources = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \{"org":{"document":{"table":"orgs","key_from":"/org"},"fields":["name"]}}
    , .{});
    const stored = try admitResolvingAlloc(alloc, &tables, collect, "table:bb", "summary", sources, null, null, names);
    defer alloc.free(stored);
    try std.testing.expect(std.mem.indexOf(u8, stored, "\"table_id\":\"srv:7\"") != null);
    // Without the mapping the public name matches no catalog entry.
    try std.testing.expectError(error.UnknownContextSourceTable, admitAlloc(alloc, &tables, collect, "table:bb", "summary", sources, null, null));
    // A storage name is not accepted in place of a public name.
    const by_storage = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \{"org":{"document":{"table":"table:aa","key_from":"/org"},"fields":["name"]}}
    , .{});
    try std.testing.expectError(error.UnknownContextSourceTable, admitResolvingAlloc(alloc, &tables, collect, "table:bb", "s", by_storage, null, null, names));
    // Authorization failures surface instead of being treated as unknown.
    const secret = try std.json.parseFromSliceLeaky(std.json.Value, a,
        \{"org":{"document":{"table":"secret","key_from":"/org"},"fields":["name"]}}
    , .{});
    try std.testing.expectError(error.Forbidden, admitResolvingAlloc(alloc, &tables, collect, "table:bb", "s", secret, null, null, names));
}

test "context host admission resolves names, rejects cycles, and computes inbound topology" {
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
    // Graph sources and generated-artifact fields are admitted (read from the
    // owning group); a cross-table artifact cycle is not.
    const graph = try admitAlloc(alloc, &tables, collect, "docs", "s", try parse.of(a,
        \\{"g":{"graph":{"graph_index":"refs"},"fields":["a"]}}
    ), null, null);
    alloc.free(graph);
    const artifacts = try admitAlloc(alloc, &tables, collect, "docs", "s", try parse.of(a,
        \\{"x":{"query":{"table":"orgs","filter_query":{"field":"k","term":"v"}},"fields":["_artifacts.tag"]}}
    ), null, null);
    alloc.free(artifacts);

    // `orgs.tag` reading `docs.summary` while `docs.summary` reads
    // `orgs._artifacts.tag` closes a cycle across tables.
    const reads_tag = try admitAlloc(alloc, &tables, collect, "docs", "summary", try parse.of(a,
        \\{"x":{"query":{"table":"orgs","filter_query":{"field":"k","term":"v"}},"fields":["_artifacts.tag"]}}
    ), null, null);
    defer alloc.free(reads_tag);
    var cyclic = tables;
    cyclic[0].indexes_json = try std.fmt.allocPrint(a, "{{\"enrichments\":[{{\"name\":\"tag\",\"context_sources_json\":{f}}}]}}", .{std.json.fmt(try admitAlloc(a, &tables, collect, "orgs", "tag", try parse.of(a,
        \\{"y":{"query":{"table":"docs","filter_query":{"field":"k","term":"v"}},"fields":["_artifacts.summary"]}}
    ), null, null), .{})});
    try std.testing.expectError(error.ContextSourceFeedbackCycle, admitAlloc(alloc, &cyclic, collect, "docs", "summary", try parse.of(a,
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
        fn groupForKey(_: *anyopaque, _: Allocator, _: []const u8, _: []const u8) anyerror!u64 {
            return 10;
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
    var host = Host{ .primitives = .{ .ptr = &fake, .propose_fn = Fake.propose, .is_leader_fn = Fake.isLeader, .groups_fn = Fake.groups, .group_for_key_fn = Fake.groupForKey, .tables_fn = Fake.tables, .reads_fn = Fake.reads, .collect = .{ .collect_fn = testCollect }, .parse_query_fn = Fake.parseQuery } };
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
