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

//! Embedded (Lite) host for cross-table context-source dependencies.
//!
//! Each logical table is its own `DB`. This module gives every table:
//!
//! * an immutable identity (`lite:<table id>`; table IDs never repeat, so a
//!   dropped-and-recreated table cannot silently inherit dependencies);
//! * a reader that serves context reads from any open table of the handle;
//! * a dispatcher that delivers change notices to the dependent table's DB;
//! * its inbound dependents: the foreign enrichments whose context sources
//!   read it, computed from every open table's catalog and persisted on the
//!   source table so a reopened source stages notices before its dependents'
//!   namespaces are opened again.

const std = @import("std");
const h = @import("handles.zig");
const tables = @import("tables.zig");
const db_mod = h.db_mod;
const ctx = @import("../storage/db/enrichment/context_sources.zig");
const notices = @import("../storage/db/enrichment/context_notices.zig");
const reader_mod = @import("../storage/db/context_source_db_reader.zig");
const resolution = @import("../storage/db/enrichment/context_resolution.zig");
const dependency_db = @import("../storage/db/context_dependency_db.zig");
const context_api = @import("../storage/db/context_api.zig");
const dependenciesJsonAlloc = context_api.dependenciesJsonAlloc;
const appendImpactRow = context_api.appendImpactRow;

pub const root_identity = "lite:default";

pub fn rootOf(handle: *h.Handle) *h.Handle {
    var current = handle;
    while (current.parent_handle) |parent| current = parent;
    return current;
}

pub fn identityAlloc(alloc: std.mem.Allocator, id: u64) ![]u8 {
    return std.fmt.allocPrint(alloc, "lite:{d}", .{id});
}

fn parseIdentity(identity: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, identity, "lite:")) return null;
    return std.fmt.parseInt(u64, identity[5..], 10) catch null;
}

/// Resolve a table name to its identity (for `context_sources[*].table`).
pub fn resolver(handle: *h.Handle) ctx.TableResolver {
    return .{ .ptr = rootOf(handle), .resolve_fn = resolveName };
}

fn resolveName(ptr: *anyopaque, alloc: std.mem.Allocator, name: []const u8) anyerror!?[]u8 {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    if (std.mem.eql(u8, name, "default")) return try alloc.dupe(u8, root_identity);
    try tables.loadCatalog(handle);
    const id = handle.embedded_table_records.get(name) orelse return null;
    return try identityAlloc(alloc, id);
}

fn dbForIdentity(handle: *h.Handle, identity: []const u8) ?*db_mod.DB {
    if (std.mem.eql(u8, identity, root_identity)) return &handle.db;
    const id = parseIdentity(identity) orelse return null;
    var it = handle.embedded_tables.valueIterator();
    while (it.next()) |table| if (table.*.id == id) return &table.*.db;
    return null;
}

fn identityForDb(alloc: std.mem.Allocator, handle: *h.Handle, db: *db_mod.DB) !?[]u8 {
    if (db == &handle.db) return try alloc.dupe(u8, root_identity);
    var it = handle.embedded_tables.valueIterator();
    while (it.next()) |table| if (&table.*.db == db) return try identityAlloc(alloc, table.*.id);
    return null;
}

const Host = struct {
    handle: *h.Handle,
    registry: reader_mod.Registry(db_mod.DB),
};

fn resolveDb(ptr: *anyopaque, identity: []const u8) ?*db_mod.DB {
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    return dbForIdentity(handle, identity);
}

fn deliver(ptr: *anyopaque, alloc: std.mem.Allocator, dependent: []const u8, notice: []const u8) anyerror!void {
    _ = alloc;
    const handle: *h.Handle = @ptrCast(@alignCast(ptr));
    // A dependent namespace that is not open keeps the notice in the source
    // outbox; it is delivered once the dependent opens.
    const db = dbForIdentity(handle, dependent) orelse return error.ContextDependentUnavailable;
    _ = try db.applyContextNotices(&.{notice});
}

/// Per-table registry readers must outlive the table's enrichment workers;
/// they are owned by the handle-lifetime arena of the root handle.
fn registryFor(root: *h.Handle, db: *db_mod.DB, identity: []const u8) !resolution.Reader {
    for (root.context_dependency_hosts.items) |ptr| {
        const existing: *Host = @ptrCast(@alignCast(ptr));
        if (existing.registry.own == db and std.mem.eql(u8, existing.registry.own_table_id, identity)) return existing.registry.reader();
    }
    const host = try root.alloc.create(Host);
    errdefer root.alloc.destroy(host);
    const own_table_id = try root.alloc.dupe(u8, identity);
    errdefer root.alloc.free(own_table_id);
    host.* = .{ .handle = root, .registry = .{ .ptr = root, .own = db, .own_table_id = own_table_id, .resolve_fn = resolveDb, .request_fn = executeRequest } };
    try root.context_dependency_hosts.append(root.alloc, host);
    return host.registry.reader();
}

fn tableNameForDb(root: *h.Handle, db: *db_mod.DB) ?[]const u8 {
    if (db == &root.db) return "default";
    var it = root.embedded_tables.iterator();
    while (it.next()) |entry| if (&entry.value_ptr.*.db == db) return entry.key_ptr.*;
    return null;
}

/// Snapshot sources with a full public `QueryRequest`: parse it like
/// `antfly_db_query` does and return the ranked hits with stored documents.
fn executeRequest(ptr: *anyopaque, alloc: std.mem.Allocator, db: *db_mod.DB, request_json: []const u8, k: u32) anyerror!resolution.Page {
    const root: *h.Handle = @ptrCast(@alignCast(ptr));
    const name = tableNameForDb(root, db) orelse return error.ContextSourceTableUnavailable;
    if (ctx.cypherRequest(alloc, request_json)) return executeCypher(alloc, root, db, name, request_json, k);
    var semantic = @import("db.zig").LiteSemanticResolver{ .handle = root };
    var owned = try h.query_api.parsePublicQueryRequest(alloc, if (root.lite_profile == .native) semantic.resolver() else null, name, request_json);
    defer owned.deinit(alloc);
    owned.req.include_stored = true;
    owned.req.include_all_fields = true;
    if (owned.req.limit == 0 or owned.req.limit > k) owned.req.limit = k;
    var result = try db.search(alloc, owned.req);
    defer result.deinit();
    var rows = std.ArrayListUnmanaged(resolution.Row).empty;
    errdefer {
        for (rows.items) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        rows.deinit(alloc);
    }
    // A graph-only request's members are the nodes its graph operations
    // return (in operation and result order, each once), not its hits.
    const graph_only = ctx.graphOnlyRequest(alloc, request_json);
    var seen = std.StringHashMapUnmanaged(void).empty;
    defer seen.deinit(alloc);
    const groups: []const []const db_mod.types.SearchHit = if (graph_only) blk: {
        const lists = try alloc.alloc([]const db_mod.types.SearchHit, result.graph_results.len);
        for (result.graph_results, lists) |graph, *list| list.* = graph.hits;
        break :blk lists;
    } else &.{result.hits};
    defer if (graph_only) alloc.free(groups);
    for (groups) |hits| for (hits) |hit| {
        if (rows.items.len >= k) break;
        if ((try seen.getOrPut(alloc, hit.id)).found_existing) continue;
        const value = if (hit.stored_data) |data| try alloc.dupe(u8, data) else (try db.get(alloc, hit.id)) orelse continue;
        errdefer alloc.free(value);
        try rows.append(alloc, .{ .key = try alloc.dupe(u8, hit.id), .value = value });
    };
    return .{ .rows = try rows.toOwnedSlice(alloc), .exhausted = true };
}

/// A Cypher source on Lite: the read-only statement runs against the
/// handle's graph catalog; its members are the source table's nodes it
/// returns, read back as stored documents.
fn executeCypher(alloc: std.mem.Allocator, root: *h.Handle, db: *db_mod.DB, name: []const u8, request_json: []const u8, k: u32) !resolution.Page {
    const cypher = @import("cypher");
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const request = try std.json.parseFromSliceLeaky(std.json.Value, a, request_json, .{});
    const spec = request.object.get("cypher").?;
    if (spec != .object) return error.InvalidContextSourcesConfig;
    const graph = spec.object.get("graph") orelse return error.InvalidContextSourcesConfig;
    const statement = spec.object.get("query") orelse return error.InvalidContextSourcesConfig;
    if (graph != .string or statement != .string) return error.InvalidContextSourcesConfig;
    {
        var diagnostic: cypher.compiler.Diagnostic = .{};
        var compiled = try cypher.compiler.compile(a, statement.string, .{}, &diagnostic);
        defer compiled.deinit();
        if (compiled.query.explain or cypher.runtime.queryHasWrites(compiled.query)) return error.UnsupportedContextPredicate;
    }
    const body = try std.json.Stringify.valueAlloc(a, .{ .query = statement.string, .parameters = spec.object.get("parameters") orelse std.json.Value{ .object = .empty } }, .{});
    var out: h.capi.Buffer = .{};
    var diagnostic: cypher.compiler.Diagnostic = .{};
    var scope: @import("cypher_write.zig").Scope = .{};
    try @import("cypher.zig").execute(root, graph.string, body, &out, &diagnostic, &scope);
    defer @import("db.zig").antfly_buffer_free(&out);
    const result: []const u8 = if (out.ptr) |ptr| ptr[0..out.len] else "";
    const keys = try ctx.cypherResultKeysAlloc(a, result, name, k);
    var rows = std.ArrayListUnmanaged(resolution.Row).empty;
    errdefer {
        for (rows.items) |row| {
            alloc.free(row.key);
            alloc.free(row.value);
        }
        rows.deinit(alloc);
    }
    for (keys) |key| {
        const value = (try db.get(alloc, key)) orelse continue;
        errdefer alloc.free(value);
        try rows.append(alloc, .{ .key = try alloc.dupe(u8, key), .value = value });
    }
    return .{ .rows = try rows.toOwnedSlice(alloc), .exhausted = true };
}

pub fn deinitHosts(handle: *h.Handle) void {
    for (handle.context_dependency_hosts.items) |ptr| {
        const host: *Host = @ptrCast(@alignCast(ptr));
        handle.alloc.free(host.registry.own_table_id);
        handle.alloc.destroy(host);
    }
    handle.context_dependency_hosts.deinit(handle.alloc);
    handle.context_dependency_hosts = .empty;
}

/// Install identity, reader, dispatcher and inbound dependents on every open
/// table of the handle. Call after a table opens and after any enrichment
/// change; never opens namespaces itself.
pub fn sync(handle: *h.Handle) !void {
    const root = rootOf(handle);
    const alloc = root.alloc;
    // Inbound dependents are recomputed from every table's catalog and
    // persisted on each source; a namespace left unopened here would drop
    // its declarations from every source's stored topology.
    try tables.ensureDiscovered(root);
    var dbs = std.ArrayListUnmanaged(*db_mod.DB).empty;
    defer dbs.deinit(alloc);
    try dbs.append(alloc, &root.db);
    var it = root.embedded_tables.valueIterator();
    while (it.next()) |table| try dbs.append(alloc, &table.*.db);

    // Gather every declared context configuration with its dependent identity.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var declared = std.ArrayListUnmanaged(notices.DeclaredConfig).empty;
    for (dbs.items) |db| {
        const identity = (try identityForDb(a, root, db)) orelse continue;
        for (db.core.index_manager.enrichments.items) |entry| {
            if (entry.kind != .asset or entry.context_sources_json.len == 0) continue;
            try declared.append(a, .{ .dependent_table_id = identity, .artifact = try a.dupe(u8, entry.name), .stored_json = try a.dupe(u8, entry.context_sources_json) });
        }
    }
    // Tables that failed to open this time: their declarations are unknown,
    // not removed, so each source keeps what it last persisted for them.
    var unavailable = std.StringHashMapUnmanaged(void).empty;
    var failed = root.embedded_table_errors.keyIterator();
    while (failed.next()) |name| {
        const id = root.embedded_table_records.get(name.*) orelse continue;
        try unavailable.put(a, try identityAlloc(a, id), {});
    }
    for (dbs.items) |db| {
        const identity = (try identityForDb(a, root, db)) orelse continue;
        var inbound = std.ArrayListUnmanaged(notices.DeclaredConfig).empty;
        for (declared.items) |d| {
            if (std.mem.eql(u8, d.dependent_table_id, identity)) continue; // own: from its catalog
            var config = ctx.parseStoredJson(a, d.stored_json) catch continue;
            defer config.deinit();
            for (config.sources) |source| if (std.mem.eql(u8, source.table_id, identity)) {
                try inbound.append(a, d);
                break;
            };
        }
        if (unavailable.count() != 0) if (try db.contextPersistedInboundAlloc(a)) |persisted| {
            for (persisted.value.inbound) |d| if (unavailable.contains(d.dependent_table_id)) try inbound.append(a, d);
        };
        const reader = try registryFor(root, db, identity);
        try db.setContextDependencyHost(identity, inbound.items, reader, .{ .ptr = root, .deliver = deliver });
    }
    // Deliver anything staged while a dependent was unavailable.
    for (dbs.items) |db| _ = db.drainContextNoticeOutbox(256) catch {};
}

/// Restore the persisted topology on a freshly opened table before any write
/// can reach it, then refresh the whole handle.
pub fn attach(handle: *h.Handle, db: *db_mod.DB, identity: []const u8) !void {
    const root = rootOf(handle);
    const reader = try registryFor(root, db, identity);
    _ = try db.restoreContextDependencyHost(reader, .{ .ptr = root, .deliver = deliver });
}

/// Canonicalize a Lite enrichment's context sources with table names
/// resolved to identities. Public `context_sources`/`refresh` objects take
/// precedence; a stored form is re-resolved (never trusted).
pub fn resolveEnrichment(handle: *h.Handle, alloc: std.mem.Allocator, raw_json: []const u8, cfg: *db_mod.types.EnrichmentConfig) !void {
    var parsed = std.json.parseFromSlice(std.json.Value, alloc, raw_json, .{}) catch return;
    defer parsed.deinit();
    if (parsed.value != .object) return;
    const resolved: ?[]u8 = if (parsed.value.object.get("context_sources")) |sources|
        (if (sources == .null) null else try ctx.canonicalFromPublicAlloc(alloc, sources, parsed.value.object.get("refresh"), resolver(handle), @import("../api/indexes.zig").contextFilterLowering()))
    else if (cfg.context_sources_json.len != 0)
        try ctx.reresolveStoredAlloc(alloc, cfg.context_sources_json, resolver(handle))
    else
        null;
    if (resolved) |value| {
        if (cfg.context_sources_json.len != 0) alloc.free(cfg.context_sources_json);
        cfg.context_sources_json = value;
    }
}

/// Reject a declaration that would close a generated-data cycle across
/// tables: following context reads of `_artifacts.X` (and upstream artifact
/// streams) from the candidate must never reach the candidate again.
pub fn rejectCrossTableCycle(handle: *h.Handle, owner: *db_mod.DB, candidate: db_mod.types.EnrichmentConfig) !void {
    const root = rootOf(handle);
    var arena = std.heap.ArenaAllocator.init(root.alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const owner_id = (try identityForDb(a, root, owner)) orelse return;
    const Node = struct { table: []const u8, name: []const u8 };
    var stack = std.ArrayListUnmanaged(Node).empty;
    var seen = std.StringHashMapUnmanaged(void).empty;
    // Seed with what the candidate reads.
    try pushReads(a, &stack, owner_id, candidate.context_sources_json, "");
    if (candidate.source_artifact_name.len != 0) try stack.append(a, .{ .table = owner_id, .name = candidate.source_artifact_name });
    var steps: usize = 0;
    while (stack.pop()) |node| {
        steps += 1;
        if (steps > 10_000) return error.ContextSourceFeedbackCycle;
        if (std.mem.eql(u8, node.table, owner_id) and std.mem.eql(u8, node.name, candidate.name)) return error.ContextSourceFeedbackCycle;
        const id = try std.fmt.allocPrint(a, "{s}\x00{s}", .{ node.table, node.name });
        if ((try seen.getOrPut(a, id)).found_existing) continue;
        const db = dbForIdentity(root, node.table) orelse continue;
        for (db.core.index_manager.enrichments.items) |entry| {
            if (!std.mem.eql(u8, entry.name, node.name)) continue;
            if (entry.source_artifact_name.len != 0) try stack.append(a, .{ .table = node.table, .name = entry.source_artifact_name });
            if (entry.context_sources_json.len != 0) try pushReads(a, &stack, node.table, entry.context_sources_json, "");
        }
    }
}

fn pushReads(a: std.mem.Allocator, stack: anytype, table: []const u8, stored: []const u8, _: []const u8) !void {
    var config = ctx.parseStoredJson(a, stored) catch return;
    defer config.deinit();
    for (config.sources) |source| {
        const target = if (source.table_id.len == 0) table else source.table_id;
        for (source.artifact_names) |name| try stack.append(a, .{ .table = try a.dupe(u8, target), .name = try a.dupe(u8, name) });
    }
}

/// Re-resolve already parsed configs (index-shorthand path).
pub fn resolveStored(handle: *h.Handle, alloc: std.mem.Allocator, cfg: *db_mod.types.EnrichmentConfig) !void {
    if (cfg.context_sources_json.len == 0) return;
    const value = try ctx.reresolveStoredAlloc(alloc, cfg.context_sources_json, resolver(handle));
    alloc.free(cfg.context_sources_json);
    cfg.context_sources_json = value;
}

pub fn drainAll(handle: *h.Handle) void {
    const root = rootOf(handle);
    _ = root.db.drainContextNoticeOutbox(256) catch {};
    var it = root.embedded_tables.valueIterator();
    while (it.next()) |table| _ = table.*.db.drainContextNoticeOutbox(256) catch {};
}

// ---------------------------------------------------------------------------
// C ABI: inspection, reverse lineage, refresh
// ---------------------------------------------------------------------------

const api = @import("db.zig");
const store = @import("../storage/db/enrichment/context_dependency_store.zig");

fn ownedBuffer(bytes: []const u8) !h.capi.Buffer {
    const copy = try std.heap.c_allocator.dupe(u8, bytes);
    return .{ .ptr = copy.ptr, .len = copy.len };
}

/// `{"freshness":{...}|null,"versions":[manifest...]}` (newest first, up to
/// `max_versions` retained manifests).
const ImpactRequest = struct {
    table: []const u8 = "default",
    key: []const u8,
    revision: ?[]const u8 = null,
    traversal: []const u8 = "direct",
    max_depth: u32 = 8,
    limit: u32 = 100,
};

/// Reverse lineage across every open table of the handle. Direct rows are
/// confirmed against retained manifests; transitive traversal follows
/// same-document artifact chains (`source_artifact_name`) and is bounded by
/// `max_depth` and `limit`. `complete` is false whenever a budget was hit.
pub fn impactJsonAlloc(alloc: std.mem.Allocator, handle: *h.Handle, request_json: []const u8) ![]u8 {
    const root = rootOf(handle);
    var parsed = try std.json.parseFromSlice(ImpactRequest, alloc, request_json, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const req = parsed.value;
    if (req.limit == 0 or req.limit > 10_000 or req.max_depth == 0 or req.max_depth > 64) return error.InvalidArgument;
    const transitive = std.mem.eql(u8, req.traversal, "transitive");
    if (!transitive and !std.mem.eql(u8, req.traversal, "direct")) return error.InvalidArgument;
    const input_identity = (try resolveName(root, alloc, req.table)) orelse return error.UndefinedTable;
    defer alloc.free(input_identity);
    const revision: ?store.Digest = if (req.revision) |text| store.parseRevision(text) orelse return error.InvalidArgument else null;

    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"input\":{\"table_id\":");
    try ctx.appendJsonString(alloc, &out, input_identity);
    try out.appendSlice(alloc, ",\"key\":");
    try ctx.appendJsonString(alloc, &out, req.key);
    try out.appendSlice(alloc, "},\"dependents\":[");
    var emitted: usize = 0;
    var complete = true;
    var dbs = std.ArrayListUnmanaged(*db_mod.DB).empty;
    defer dbs.deinit(alloc);
    try dbs.append(alloc, &root.db);
    var it = root.embedded_tables.valueIterator();
    while (it.next()) |table| try dbs.append(alloc, &table.*.db);
    // Breadth-first over (table, key, artifact): the input row, then
    // (transitively) every dependent output, followed through same-document
    // consumers and into tables whose declarations read it as
    // `_artifacts.<name>`.
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const Node = struct { identity: []const u8, key: []const u8, artifact: ?[]const u8, depth: u32 };
    var queue = std.ArrayListUnmanaged(Node).empty;
    try queue.append(a, .{ .identity = input_identity, .key = req.key, .artifact = null, .depth = 0 });
    var visited = std.StringHashMapUnmanaged(void).empty;
    var head: usize = 0;
    walk: while (head < queue.items.len) : (head += 1) {
        const node = queue.items[head];
        for (dbs.items) |db| {
            const identity = (try identityForDb(a, root, db)) orelse continue;
            // Own-table inputs are recorded with this table's identity.
            var page = try db.contextImpact(alloc, node.identity, node.key, if (node.depth == 0) revision else null, "", req.limit);
            defer page.deinit(alloc);
            if (page.resume_after != null) complete = false;
            for (page.rows.items) |row| {
                if (emitted >= req.limit) {
                    complete = false;
                    break :walk;
                }
                // Past the first hop, only consumers whose declaration reads
                // the regenerated artifact are affected by it.
                if (node.artifact) |upstream| {
                    const entry = db.core.index_manager.getEnrichment(.asset, row.artifact) orelse continue;
                    if (!ctx.declarationReadsArtifact(a, entry.context_sources_json, identity, node.identity, upstream)) continue;
                }
                const id = try std.fmt.allocPrint(a, "{s}\x00{s}\x00{s}", .{ identity, row.doc_key, row.artifact });
                if ((try visited.getOrPut(a, id)).found_existing) continue;
                try appendImpactRow(alloc, &out, &emitted, identity, row.doc_key, row.artifact, row.version, row.current_version, @tagName(row.freshness), &.{row.artifact});
                if (!transitive) continue;
                if (node.depth + 1 >= req.max_depth) {
                    complete = false;
                    continue;
                }
                const doc_key = try a.dupe(u8, row.doc_key);
                try queue.append(a, .{ .identity = identity, .key = doc_key, .artifact = try a.dupe(u8, row.artifact), .depth = node.depth + 1 });
                // Same-document downstream artifacts consume this output too.
                const budget = if (emitted >= req.limit) 0 else req.limit - emitted;
                const downstream = try context_api.downstreamImpactPaths(a, db.core.index_manager.enrichments.items, try a.dupe(u8, row.artifact), req.max_depth, budget);
                if (!downstream.complete) complete = false;
                for (downstream.paths) |path| {
                    const leaf = path[path.len - 1];
                    const leaf_id = try std.fmt.allocPrint(a, "{s}\x00{s}\x00{s}", .{ identity, doc_key, leaf });
                    if ((try visited.getOrPut(a, leaf_id)).found_existing) continue;
                    try appendImpactRow(alloc, &out, &emitted, identity, doc_key, leaf, 0, true, "candidate", path);
                    try queue.append(a, .{ .identity = identity, .key = doc_key, .artifact = leaf, .depth = node.depth + 1 });
                }
            }
        }
    }
    try out.print(alloc, "],\"complete\":{},\"traversal\":", .{complete});
    try ctx.appendJsonString(alloc, &out, req.traversal);
    try out.appendSlice(alloc, ",\"semantics\":\"possible impact: dependency edges establish that outputs consumed the input, not that regenerated bytes differ\"}");
    return out.toOwnedSlice(alloc);
}

pub fn antfly_db_artifact_dependencies_json(ptr: ?*anyopaque, doc_key: h.capi.Slice, artifact: h.capi.Slice, out: *h.capi.Buffer) callconv(.c) h.capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_artifact_dependencies_json), .{ .name = "antfly_db_artifact_dependencies_json" }), .{ ptr, doc_key, artifact, out });
    h.capi.clearLastError();
    out.* = .{};
    const guard = api.enterHandleWithIntent(ptr, .read, .read_only) orelse return h.capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return h.capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const bytes = dependenciesJsonAlloc(handle.alloc, handle.database(), doc_key.bytes(), artifact.bytes(), store.default_retain_versions) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(bytes);
    out.* = ownedBuffer(bytes) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

pub fn antfly_db_dependency_impact_json(ptr: ?*anyopaque, request_json: h.capi.Slice, out: *h.capi.Buffer) callconv(.c) h.capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_dependency_impact_json), .{ .name = "antfly_db_dependency_impact_json" }), .{ ptr, request_json, out });
    h.capi.clearLastError();
    out.* = .{};
    const guard = api.enterHandleWithIntent(ptr, .exclusive, .read_only) orelse return h.capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return h.capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const handle = guard.handle;
    const bytes = impactJsonAlloc(handle.alloc, handle, request_json.bytes()) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    defer handle.alloc.free(bytes);
    out.* = ownedBuffer(bytes) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    return .ok;
}

/// Deliver pending change notices, drain due debounced rebuilds, and advance
/// shadow backfill on every open table of the handle.
pub fn antfly_db_refresh_context_dependencies(ptr: ?*anyopaque) callconv(.c) h.capi.ErrorCode {
    if (comptime @import("../storage/engine_selection.zig").enabled)
        return @call(.auto, @extern(*const @TypeOf(antfly_db_refresh_context_dependencies), .{ .name = "antfly_db_refresh_context_dependencies" }), .{ptr});
    h.capi.clearLastError();
    const guard = api.enterHandleWithIntent(ptr, .maintain, .mutation) orelse return h.capi.mapErrorContext(error.InvalidArgument, @src().fn_name ++ ": invalid or closed database handle");
    if (guard.entry_error) |code| {
        guard.leave();
        return h.capi.ensureError(code, @src().fn_name);
    }
    defer guard.leave();
    const root = rootOf(guard.handle);
    sync(root) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    _ = root.db.advanceContextShadowBackfill(256) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    var it = root.embedded_tables.valueIterator();
    while (it.next()) |table| _ = table.*.db.advanceContextShadowBackfill(256) catch |err| return h.capi.mapErrorContext(err, @src().fn_name);
    drainAll(root);
    return .ok;
}

comptime {
    _ = dependency_db;
    @import("exports.zig").publish(@This(), &.{
        "antfly_db_artifact_dependencies_json",
        "antfly_db_dependency_impact_json",
        "antfly_db_refresh_context_dependencies",
    });
}

test "capi Lite cross-table context sources: transitive impact and an unavailable table keep their dependents" {
    const a = std.testing.allocator;
    const asset_producer = @import("../storage/db/enrichment/asset_producer.zig");
    const Producer = struct {
        fn produce(_: *anyopaque, alloc: std.mem.Allocator, request: asset_producer.Request) anyerror![]u8 {
            return std.fmt.allocPrint(alloc, "summary:{s}", .{request.source_text});
        }
        fn memory(_: *anyopaque, _: std.mem.Allocator, _: []const asset_producer.Request) anyerror!@import("antfly_inference_work").InvocationMemoryPlan {
            return .{ .attachment_transport = .borrowed_binary, .fixed_bytes = 8 << 20, .allocator_limit_bytes = 8 << 20, .max_result_bytes_per_item = 4 << 20, .max_result_bytes = 4 << 20 };
        }
        fn interface(self: *@This()) asset_producer.Producer {
            return .{ .ptr = self, .vtable = &.{ .produce = produce, .invocation_memory_for_requests = memory } };
        }
    };
    var producer = Producer{};
    var tmp = try api.TestDirectory.init("lite-context-transitive");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const id = try api.publishHandle(handle);
    defer api.antfly_db_close(id);
    for ([_][]const u8{ "orgs", "teams" }) |name| try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice(name), .fromSlice("{}")));
    const orgs = try tables.get(handle, "orgs");
    const teams = try tables.get(handle, "teams");
    try orgs.batch(.{ .writes = &.{.{ .key = "org:1", .value = "{\"voice\":\"warm\"}" }}, .sync_level = .write });

    // default.summary reads orgs; teams.digest reads default's summaries.
    const summary =
        \\{"name":"summary","kind":"asset","field":"body","content_type":"text/plain",
        \\ "producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
        \\ "context_sources":{"organization":{"document":{"table":"orgs","key_from":"/org"},"fields":["voice"],"required":true}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_enrichment_json(id, .fromSlice(summary)));
    var teams_handle: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("teams"), &teams_handle));
    defer api.antfly_db_close(teams_handle);
    const digest =
        \\{"name":"digest","kind":"asset","field":"body","content_type":"text/plain",
        \\ "producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
        \\ "context_sources":{"members":{"query":{"table":"default","filter_query":{"field":"org","term":{"$source":"/org"}}},"fields":["_artifacts.summary"]}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_enrichment_json(teams_handle, .fromSlice(digest)));
    try handle.db.reconfigureEnrichmentRuntime(.{ .asset_producer = producer.interface() });
    try teams.reconfigureEnrichmentRuntime(.{ .asset_producer = producer.interface() });
    try sync(handle);
    _ = try orgs.advanceContextShadowBackfill(64);
    _ = try handle.db.advanceContextShadowBackfill(64);

    try handle.db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"hello\",\"org\":\"org:1\"}" }}, .sync_level = .write });
    try handle.db.enrichment_runtime.?.catchUpUntilForDrain(handle.db.core.nextDerivedSequence());
    try handle.db.runDerivedUntil(handle.db.core.nextDerivedSequence());
    try teams.batch(.{ .writes = &.{.{ .key = "team:1", .value = "{\"body\":\"team\",\"org\":\"org:1\"}" }}, .sync_level = .write });
    try teams.enrichment_runtime.?.catchUpUntilForDrain(teams.core.nextDerivedSequence());
    try teams.runDerivedUntil(teams.core.nextDerivedSequence());
    {
        const json = try dependenciesJsonAlloc(a, teams, "team:1", "digest", 4);
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"state\":\"current\"") != null);
    }

    // Direct impact of the org row stops at default.summary; transitive
    // impact follows it into teams.digest, which read that summary.
    {
        const direct = try impactJsonAlloc(a, handle, "{\"table\":\"orgs\",\"key\":\"org:1\",\"limit\":10}");
        defer a.free(direct);
        try std.testing.expect(std.mem.indexOf(u8, direct, "\"key\":\"doc:a\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, direct, "\"key\":\"team:1\"") == null);
        const transitive = try impactJsonAlloc(a, handle, "{\"table\":\"orgs\",\"key\":\"org:1\",\"traversal\":\"transitive\",\"max_depth\":4,\"limit\":10}");
        defer a.free(transitive);
        try std.testing.expect(std.mem.indexOf(u8, transitive, "\"key\":\"team:1\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, transitive, "\"artifact\":\"digest\"") != null);
        try std.testing.expect(std.mem.indexOf(u8, transitive, "\"complete\":true") != null);
    }

    // teams fails to open on a later sync: the root table must keep its
    // persisted subscription for teams instead of dropping it.
    const teams_id = handle.embedded_table_records.get("teams").?;
    const teams_identity = try identityAlloc(a, teams_id);
    defer a.free(teams_identity);
    const removed = handle.embedded_tables.fetchRemove("teams").?;
    try handle.embedded_table_errors.put(handle.alloc, "teams", "InjectedOpenFailure");
    const discovered = handle.embedded_tables_discovered;
    handle.embedded_tables_discovered = true;
    const resync = sync(handle);
    handle.embedded_tables_discovered = discovered;
    _ = handle.embedded_table_errors.remove("teams");
    try handle.embedded_tables.put(handle.alloc, removed.key, removed.value);
    try resync;
    var persisted = (try handle.db.contextPersistedInboundAlloc(a)).?;
    defer persisted.deinit();
    var kept = false;
    for (persisted.value.inbound) |d| if (std.mem.eql(u8, d.dependent_table_id, teams_identity)) {
        kept = true;
    };
    try std.testing.expect(kept);
}

test "capi Lite cross-table context sources: cypher members are the statement's nodes" {
    const a = std.testing.allocator;
    const asset_producer = @import("../storage/db/enrichment/asset_producer.zig");
    const Producer = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fn produce(ptr: *anyopaque, alloc: std.mem.Allocator, request: asset_producer.Request) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .monotonic);
            return std.fmt.allocPrint(alloc, "summary:{s}", .{request.source_text});
        }
        fn memory(_: *anyopaque, _: std.mem.Allocator, _: []const asset_producer.Request) anyerror!@import("antfly_inference_work").InvocationMemoryPlan {
            return .{ .attachment_transport = .borrowed_binary, .fixed_bytes = 8 << 20, .allocator_limit_bytes = 8 << 20, .max_result_bytes_per_item = 4 << 20, .max_result_bytes = 4 << 20 };
        }
        fn interface(self: *@This()) asset_producer.Producer {
            return .{ .ptr = self, .vtable = &.{ .produce = produce, .invocation_memory_for_requests = memory } };
        }
    };
    var producer = Producer{};
    var tmp = try api.TestDirectory.init("lite-context-cypher");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const id = try api.publishHandle(handle);
    defer api.antfly_db_close(id);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice("orgs"), .fromSlice("{}")));
    const orgs = try tables.get(handle, "orgs");
    try orgs.batch(.{ .writes = &.{
        .{ .key = "org:1", .value = "{\"tier\":\"gold\",\"voice\":\"warm\"}" },
        .{ .key = "org:2", .value = "{\"tier\":\"silver\",\"voice\":\"cold\"}" },
    }, .sync_level = .write });
    var output: h.capi.Buffer = .{};
    const catalog_code = @import("cypher.zig").antfly_db_graph_catalog_json(id, .fromSlice("{\"action\":\"put\",\"name\":\"g\",\"definition\":{\"labels\":[{\"name\":\"Org\",\"table\":\"orgs\"}],\"relationships\":[]}}"), &output);
    if (catalog_code != .ok) std.debug.print("graph catalog failed: {s}\n", .{if (output.ptr) |p| p[0..output.len] else ""});
    api.antfly_buffer_free(&output);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, catalog_code);

    const declaration =
        \\{"name":"summary","kind":"asset","field":"body","content_type":"text/plain",
        \\ "producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
        \\ "context_sources":{"peers":{"query":{"table":"orgs","cypher":{"graph":"g","query":"MATCH (o:Org) WHERE o.tier = $tier RETURN o","parameters":{"tier":{"$source":"/tier"}}}},"fields":["voice"]}}}
    ;
    const added = api.antfly_db_add_enrichment_json(id, .fromSlice(declaration));
    if (added != .ok) std.debug.print("add enrichment failed: {s}\n", .{std.mem.span(h.capi.lastErrorDetail())});
    try std.testing.expectEqual(h.capi.ErrorCode.ok, added);
    try handle.db.reconfigureEnrichmentRuntime(.{ .asset_producer = producer.interface() });
    try sync(handle);

    try handle.db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"hello\",\"tier\":\"gold\"}" }}, .sync_level = .write });
    try handle.db.enrichment_runtime.?.catchUpUntilForDrain(handle.db.core.nextDerivedSequence());
    try handle.db.runDerivedUntil(handle.db.core.nextDerivedSequence());
    try std.testing.expect(producer.calls.load(.acquire) >= 1);
    const json = try dependenciesJsonAlloc(a, &handle.db, "doc:a", "summary", 4);
    defer a.free(json);
    if (std.mem.indexOf(u8, json, "org:1") == null) std.debug.print("dependencies: {s}\n", .{json});
    try std.testing.expect(std.mem.indexOf(u8, json, "org:1") != null);
    try std.testing.expect(std.mem.indexOf(u8, json, "org:2") == null);
}

test "capi Lite cross-table context sources invalidate dependents in another table" {
    const a = std.testing.allocator;
    const asset_producer = @import("../storage/db/enrichment/asset_producer.zig");
    const Producer = struct {
        calls: std.atomic.Value(usize) = .init(0),
        fn produce(ptr: *anyopaque, alloc: std.mem.Allocator, request: asset_producer.Request) anyerror![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .monotonic);
            return std.fmt.allocPrint(alloc, "summary:{s}", .{request.source_text});
        }
        fn memory(_: *anyopaque, _: std.mem.Allocator, _: []const asset_producer.Request) anyerror!@import("antfly_inference_work").InvocationMemoryPlan {
            return .{ .attachment_transport = .borrowed_binary, .fixed_bytes = 8 << 20, .allocator_limit_bytes = 8 << 20, .max_result_bytes_per_item = 4 << 20, .max_result_bytes = 4 << 20 };
        }
        fn interface(self: *@This()) asset_producer.Producer {
            return .{ .ptr = self, .vtable = &.{ .produce = produce, .invocation_memory_for_requests = memory } };
        }
    };
    var producer = Producer{};
    var tmp = try api.TestDirectory.init("lite-context-sources");
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, "{s}.aflite", .{tmp.path()});
    defer a.free(path);
    const handle = try api.openLiteHandleAllocWithRuntime(std.heap.c_allocator, path, .{ .profile = .native, .externally_locked = true }, true, null, null);
    const id = try api.publishHandle(handle);
    defer api.antfly_db_close(id);
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_create_table_json(id, .fromSlice("orgs"), .fromSlice("{}")));
    const orgs = try tables.get(handle, "orgs");
    try orgs.batch(.{ .writes = &.{.{ .key = "org:1", .value = "{\"voice\":\"warm\",\"logo\":\"a\"}" }}, .sync_level = .write });

    const declaration =
        \\{"name":"summary","kind":"asset","field":"body","content_type":"text/plain",
        \\ "producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
        \\ "context_sources":{"organization":{"document":{"table":"orgs","key_from":"/org"},"fields":["voice"],"required":true}}}
    ;
    const added = api.antfly_db_add_enrichment_json(id, .fromSlice(declaration));
    if (added != .ok) std.debug.print("add enrichment failed: {s}\n", .{std.mem.span(h.capi.lastErrorDetail())});
    try std.testing.expectEqual(h.capi.ErrorCode.ok, added);
    const stored = handle.db.core.index_manager.getEnrichment(.asset, "summary").?;
    try std.testing.expect(std.mem.indexOf(u8, stored.context_sources_json, "\"table_id\":\"lite:") != null);
    try handle.db.reconfigureEnrichmentRuntime(.{ .asset_producer = producer.interface() });
    try sync(handle);
    _ = try orgs.advanceContextShadowBackfill(64);

    try handle.db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"hello\",\"org\":\"org:1\"}" }}, .sync_level = .write });
    try handle.db.enrichment_runtime.?.catchUpUntilForDrain(handle.db.core.nextDerivedSequence());
    try handle.db.runDerivedUntil(handle.db.core.nextDerivedSequence());
    const first = producer.calls.load(.acquire);
    try std.testing.expect(first >= 1);
    {
        const json = try dependenciesJsonAlloc(a, &handle.db, "doc:a", "summary", 4);
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"state\":\"current\"") != null);
    }

    // Unconsumed field in the foreign table: no rebuild.
    try orgs.batch(.{ .writes = &.{.{ .key = "org:1", .value = "{\"voice\":\"warm\",\"logo\":\"b\"}" }}, .sync_level = .write });
    try handle.db.enrichment_runtime.?.catchUpUntilForDrain(handle.db.core.nextDerivedSequence());
    try std.testing.expectEqual(first, producer.calls.load(.acquire));

    // Consumed field in the foreign table: the dependent in the root table is
    // marked stale at the source commit, then rebuilt.
    try orgs.batch(.{ .writes = &.{.{ .key = "org:1", .value = "{\"voice\":\"cold\",\"logo\":\"b\"}" }}, .sync_level = .write });
    {
        const json = try dependenciesJsonAlloc(a, &handle.db, "doc:a", "summary", 4);
        defer a.free(json);
        try std.testing.expect(std.mem.indexOf(u8, json, "\"state\":\"stale\"") != null);
    }
    try handle.db.enrichment_runtime.?.catchUpUntilForDrain(handle.db.core.nextDerivedSequence());
    try handle.db.runDerivedUntil(handle.db.core.nextDerivedSequence());
    try std.testing.expect(producer.calls.load(.acquire) > first);

    const impact = try impactJsonAlloc(a, handle, "{\"table\":\"orgs\",\"key\":\"org:1\",\"traversal\":\"transitive\",\"limit\":10}");
    defer a.free(impact);
    try std.testing.expect(std.mem.indexOf(u8, impact, "\"key\":\"doc:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, impact, "\"complete\":true") != null);

    // Generated-data cycles across tables are rejected at admission.
    var orgs_handle: ?*anyopaque = null;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, tables.antfly_db_open_table(id, .fromSlice("orgs"), &orgs_handle));
    defer api.antfly_db_close(orgs_handle);
    const orgsum =
        \\{"name":"orgsum","kind":"asset","field":"voice","content_type":"text/plain",
        \\ "producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
        \\ "context_sources":{"root":{"query":{"table":"default","filter_query":{"field":"org","term":{"$source":"/org"}}},"fields":["_artifacts.rootsum"]}}}
    ;
    try std.testing.expectEqual(h.capi.ErrorCode.ok, api.antfly_db_add_enrichment_json(orgs_handle, .fromSlice(orgsum)));
    const rootsum =
        \\{"name":"rootsum","kind":"asset","field":"body","content_type":"text/plain",
        \\ "producer_json":"{\"type\":\"generator\",\"config\":{\"provider\":\"antfly\",\"model\":\"test-generator\"}}",
        \\ "context_sources":{"orgs":{"document":{"table":"orgs","key_from":"/org"},"fields":["_artifacts.orgsum"]}}}
    ;
    try std.testing.expect(api.antfly_db_add_enrichment_json(id, .fromSlice(rootsum)) != .ok);
    try std.testing.expect(handle.db.core.index_manager.getEnrichment(.asset, "rootsum") == null);
}
