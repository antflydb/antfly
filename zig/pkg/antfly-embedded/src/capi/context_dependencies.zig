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

pub const root_identity = "lite:default";

fn rootOf(handle: *h.Handle) *h.Handle {
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
    host.* = .{ .handle = root, .registry = .{ .ptr = root, .own = db, .own_table_id = try root.alloc.dupe(u8, identity), .resolve_fn = resolveDb } };
    try root.context_dependency_hosts.append(root.alloc, host);
    return host.registry.reader();
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
        (if (sources == .null) null else try ctx.canonicalFromPublicAlloc(alloc, sources, parsed.value.object.get("refresh"), resolver(handle)))
    else if (cfg.context_sources_json.len != 0)
        try ctx.reresolveStoredAlloc(alloc, cfg.context_sources_json, resolver(handle))
    else
        null;
    if (resolved) |value| {
        if (cfg.context_sources_json.len != 0) alloc.free(cfg.context_sources_json);
        cfg.context_sources_json = value;
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
pub fn dependenciesJsonAlloc(alloc: std.mem.Allocator, db: *db_mod.DB, doc_key: []const u8, artifact: []const u8, max_versions: usize) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    try out.appendSlice(alloc, "{\"freshness\":");
    var state = try db.contextDependencyState(alloc, doc_key, artifact);
    defer if (state) |*value| value.deinit(alloc);
    if (state) |value| {
        const fresh = try store.freshnessJsonAlloc(alloc, value);
        defer alloc.free(fresh);
        try out.appendSlice(alloc, fresh);
        try out.print(alloc, ",\"subscriptions\":{d}", .{value.tuples.len});
    } else try out.appendSlice(alloc, "null");
    try out.appendSlice(alloc, ",\"versions\":[");
    var first = true;
    if (state) |value| {
        var version = value.published_version;
        var emitted: usize = 0;
        while (version > 0 and emitted < max_versions) : (version -= 1) {
            const manifest = (try db.contextDependencyManifest(alloc, doc_key, artifact, version)) orelse break;
            defer alloc.free(manifest);
            if (!first) try out.append(alloc, ',');
            first = false;
            try out.appendSlice(alloc, manifest);
            emitted += 1;
        }
    }
    try out.appendSlice(alloc, "]}");
    return out.toOwnedSlice(alloc);
}

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
    for (dbs.items) |db| {
        const identity = (try identityForDb(alloc, root, db)) orelse continue;
        defer alloc.free(identity);
        // Own-table inputs are recorded with this table's identity.
        var page = try db.contextImpact(alloc, input_identity, req.key, revision, "", req.limit);
        defer page.deinit(alloc);
        if (page.resume_after != null) complete = false;
        for (page.rows.items) |row| {
            if (emitted >= req.limit) {
                complete = false;
                break;
            }
            try appendImpactRow(alloc, &out, &emitted, identity, row.doc_key, row.artifact, row.version, row.current_version, @tagName(row.freshness), &.{row.artifact});
            if (!transitive) continue;
            // Same-document downstream artifacts consume this output.
            var path = std.ArrayListUnmanaged([]const u8).empty;
            defer path.deinit(alloc);
            try path.append(alloc, row.artifact);
            var frontier: []const u8 = row.artifact;
            var depth: u32 = 1;
            while (depth < req.max_depth) : (depth += 1) {
                var next: ?[]const u8 = null;
                for (db.core.index_manager.enrichments.items) |entry| {
                    if (std.mem.eql(u8, entry.source_artifact_name, frontier)) {
                        next = entry.name;
                        break;
                    }
                }
                const downstream = next orelse break;
                try path.append(alloc, downstream);
                if (emitted >= req.limit) {
                    complete = false;
                    break;
                }
                try appendImpactRow(alloc, &out, &emitted, identity, row.doc_key, downstream, 0, true, "candidate", path.items);
                frontier = downstream;
            } else complete = false;
        }
    }
    try out.print(alloc, "],\"complete\":{},\"traversal\":", .{complete});
    try ctx.appendJsonString(alloc, &out, req.traversal);
    try out.appendSlice(alloc, ",\"semantics\":\"possible impact: dependency edges establish that outputs consumed the input, not that regenerated bytes differ\"}");
    return out.toOwnedSlice(alloc);
}

fn appendImpactRow(alloc: std.mem.Allocator, out: *std.ArrayListUnmanaged(u8), emitted: *usize, table_id: []const u8, doc_key: []const u8, artifact: []const u8, version: u64, current: bool, freshness: []const u8, path: []const []const u8) !void {
    if (emitted.* != 0) try out.append(alloc, ',');
    emitted.* += 1;
    try out.appendSlice(alloc, "{\"table_id\":");
    try ctx.appendJsonString(alloc, out, table_id);
    try out.appendSlice(alloc, ",\"key\":");
    try ctx.appendJsonString(alloc, out, doc_key);
    try out.appendSlice(alloc, ",\"artifact\":");
    try ctx.appendJsonString(alloc, out, artifact);
    if (version != 0) {
        try out.print(alloc, ",\"version\":{d},\"historical\":{}", .{ version, !current });
    } else try out.appendSlice(alloc, ",\"live_candidate\":true");
    try out.appendSlice(alloc, ",\"freshness\":");
    try ctx.appendJsonString(alloc, out, freshness);
    try out.appendSlice(alloc, ",\"path\":[");
    for (path, 0..) |p, i| {
        if (i != 0) try out.append(alloc, ',');
        try ctx.appendJsonString(alloc, out, p);
    }
    try out.appendSlice(alloc, "]}");
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
}
