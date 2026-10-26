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

//! In-process statement cuts retain storage transactions, never graph entities.
//! Capture happens under the common primary/replay cohort; page reads release
//! that cohort and fork immutable primary and adjacency transactions on demand.
const std = @import("std");
const cypher = @import("cypher");
const graph = @import("../../graph/graph.zig");
const types = @import("types.zig");
const docstore = @import("../docstore.zig");
const collectors = @import("result_collectors.zig");
const engine = @import("../engine_selection.zig");
const A = std.mem.Allocator;
pub const Snapshot = struct {
    a: A,
    primary: docstore.DocStore.Txn,
    indexes: ?@import("relational_index_catalog.zig").WriteSnapshot = null,
    schema_version: u32,
    population_upper_bound: ?u64 = null,
    now: u64,
    graphs: []Graph,
    pub const Graph = struct { name: []const u8, identity: cypher.catalog.IndexIdentity, context: graph.GraphIndex.EdgeReadContext };
    pub fn deinit(self: *Snapshot) void {
        if (comptime engine.enabled) return @import("../engine.zig").call("cypherPinnedSnapshotDeinit", .{self}) catch @panic("incompatible storage owner");
        for (self.graphs) |*entry| {
            entry.context.deinit();
            self.a.free(entry.name);
        }
        self.a.free(self.graphs);
        if (self.indexes) |*indexes| indexes.deinit();
        self.primary.abort();
        self.* = undefined;
    }
};
pub fn capture(db: anytype, a: A, fence: anytype, definition: cypher.catalog.Graph, table: []const u8) !Snapshot {
    if (fence.source != db) return error.CypherSnapshotRequired;
    db.core.lockApplyShared();
    var locked = true;
    defer if (locked) db.core.unlockApplyShared();
    var schema = db.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    var primary = try db.core.store.beginReadTxn();
    errdefer primary.abort();
    const population = if (try @import("doc_identity.zig").readVisibilitySummaryTxn(&primary)) |summary| summary.live_ordinals else null;
    var indexes = db.core.relational_indexes.acquire();
    errdefer if (indexes) |*pinned| pinned.deinit();
    db.core.unlockApplyShared();
    locked = false;
    var graphs: std.ArrayList(Snapshot.Graph) = .empty;
    errdefer {
        for (graphs.items) |*entry| {
            entry.context.deinit();
            a.free(entry.name);
        }
        graphs.deinit(a);
    }
    for (definition.relationships) |mapping| {
        if (!std.mem.eql(u8, definition.label(mapping.source_label).?.table, table)) continue;
        var duplicate = false;
        for (graphs.items) |entry| if (std.mem.eql(u8, entry.name, mapping.index)) {
            duplicate = true;
            break;
        };
        if (duplicate) continue;
        const identity = (try db.cypherGraphIndexIdentity(a, mapping.index)) orelse return error.IndexNotFound;
        db.core.lockApplyShared();
        defer db.core.unlockApplyShared();
        const entry = db.core.graphIndex(mapping.index) orelse return error.IndexNotFound;
        var context = try entry.index.beginEdgeReadContextWithProof(.both);
        errdefer context.deinit();
        const name = try a.dupe(u8, mapping.index);
        errdefer a.free(name);
        try graphs.append(a, .{ .name = name, .identity = identity, .context = context });
    }
    return .{ .a = a, .primary = primary, .indexes = indexes, .population_upper_bound = population, .schema_version = if (schema) |view| view.version() else 0, .now = @import("antfly_platform").time.realtimeNs(), .graphs = try graphs.toOwnedSlice(a) };
}
pub fn scan(db: anytype, a: A, cut: *Snapshot, from: []const u8, to: []const u8, input: types.ScanOptions) !types.ScanResult {
    var schema = db.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    if ((if (schema) |view| view.version() else 0) != cut.schema_version) return error.CatalogGenerationChanged;
    var scratch = std.heap.ArenaAllocator.init(cypher.backend.reclaimingAllocator(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var opts = try db.cypherDocumentScanOptions(temporary, input);
    if (opts.relational_query == null) opts.relational_query = .{ .fields = &.{} };
    opts.sql_document_preimage = true;
    var collector: collectors.Scan = .{ .alloc = a, .include_documents = true };
    defer collector.deinit();
    if (schema != null and schema.?.storageMode() == .relational) {
        var statement: @TypeOf(db.*).RelationalStatementSnapshot = retained: {
            var read = try cut.primary.forkRead();
            errdefer read.abort();
            const lease = try db.acquireCypherReadAuthority(temporary, "", "default");
            break :retained .{ .read = read, .schema_version = cut.schema_version, .row_policy_lease = lease };
        };
        defer statement.deinit();
        const session = try db.openRelationalReadSessionAtSnapshot(temporary, from, to, opts, &statement);
        defer session.deinit();
        session.reader.now_ns = cut.now;
        var delivered: usize = 0;
        while (delivered < opts.limit) {
            var page = try session.reader.nextPage(temporary, db.backend_runtime.io(), .{ .rows = opts.limit - delivered, .output_bytes = 16 << 20 });
            defer page.deinit();
            for (page.rows) |row| {
                try collectors.Scan.visit(&collector, .{ .id = row.key, .hash = row.version, .json_null_fields = row.json_null_fields, .document_json = row.json });
                delivered += 1;
            }
            if (!page.more) break;
        }
    } else {
        const session = try @TypeOf(db.*).DocumentReadSession.openSnapshot(temporary, db.core.store, try cut.primary.forkRead(), schema, db.core.byteRange(), from, to, opts, cut.now);
        defer session.deinit();
        var delivered: usize = 0;
        while (delivered < opts.limit) {
            var page = try session.next(temporary, opts.limit - @as(u32, @intCast(delivered)));
            defer page.deinit();
            for (page.rows) |row| {
                try collectors.Scan.visit(&collector, .{ .id = row.id, .hash = row.version, .document_json = try std.json.Stringify.valueAlloc(temporary, row.document orelse row.value, .{}) });
                delivered += 1;
            }
            if (page.after == null) break;
        }
    }
    return collector.finish();
}
pub fn edges(db: anytype, a: A, cut: *Snapshot, name: []const u8, key: []const u8, edge_types: []const []const u8, direction: graph.EdgeDirection, cursor: ?graph.CypherEdgeScanCursor, limits: graph.EdgePageLimits) !graph.CypherEdgePage {
    const pinned = for (cut.graphs) |*entry| {
        if (std.mem.eql(u8, entry.name, name)) break entry;
    } else return error.IndexNotFound;
    db.core.lockApplyShared();
    defer db.core.unlockApplyShared();
    const config = db.core.index_manager.get(name) orelse return error.CatalogGenerationChanged;
    const identity = db.core.index_manager.coverageIdentityForIndex(name) orelse return error.CatalogGenerationChanged;
    const fingerprint = identity.config_fingerprint orelse try @import("../internal_keys.zig").derivedCoverageConfigFingerprint(a, config.config_json);
    if (identity.generation != pinned.identity.incarnation or fingerprint != pinned.identity.config_hash) return error.CatalogGenerationChanged;
    const entry = db.core.graphIndex(name) orelse return error.CatalogGenerationChanged;
    if (&entry.index != pinned.context.index) return error.CatalogGenerationChanged;
    var page_read = try pinned.context.fork();
    defer page_read.deinit();
    return entry.index.getCypherEdgesByTypesPageInRead(a, key, edge_types, direction, cursor, limits, cut.now, &page_read);
}

/// Native adapter facade over a statement-owned immutable storage cut.
pub fn Database(comptime DB: type) type {
    return struct {
        source: *DB,
        core: @FieldType(DB, "core"),
        cut: DB.CypherPinnedSnapshot,
        /// Planning-only upper bound: TTL, graph membership and row policies
        /// may hide documents. Never licenses pruning or supplies an aggregate.
        pub fn cypherPopulationUpperBound(self: *@This()) !?u64 {
            return self.cut.population_upper_bound;
        }
        pub fn hasRetainedQueryCut(_: *@This()) bool {
            return true;
        }
        pub fn cypherDocumentKeyAllowed(key: []const u8) bool {
            return DB.cypherDocumentKeyAllowed(key);
        }
        pub fn cypherFieldDerivedEdgeType(self: *@This(), index: []const u8, name: []const u8) bool {
            return self.source.cypherFieldDerivedEdgeType(index, name);
        }
        pub fn cypherGraphIndexIdentity(self: *@This(), a: A, name: []const u8) !?cypher.catalog.IndexIdentity {
            return self.source.cypherGraphIndexIdentity(a, name);
        }
        pub fn scan(self: *@This(), a: A, from: []const u8, to: []const u8, opts: types.ScanOptions) !types.ScanResult {
            return self.source.scanCypherPinnedSnapshot(a, &self.cut, from, to, opts);
        }
        pub fn cypherDocumentScanOptions(self: *@This(), a: A, opts: types.ScanOptions) !types.ScanOptions {
            return self.source.cypherDocumentScanOptions(a, opts);
        }
        pub fn openRelationalReadSession(self: *@This(), a: A, from: []const u8, to: []const u8, opts: types.ScanOptions) !*DB.RelationalReadSession {
            var statement: DB.RelationalStatementSnapshot = cut: {
                var read = try self.cut.primary.forkRead();
                errdefer read.abort();
                const lease = try self.source.acquireCypherReadAuthority(a, opts.row_policy_principal_proof, opts.row_policy_database);
                break :cut .{ .index_catalog_captured = true, .indexes = if (self.cut.indexes) |indexes| .{ .head = indexes.head, .plan = indexes.plan.clone() } else null, .read = read, .schema_version = self.cut.schema_version, .row_policy_lease = lease };
            };
            defer statement.deinit();
            const session = try self.source.openRelationalReadSessionAtSnapshot(a, from, to, opts, &statement);
            session.reader.now_ns = self.cut.now;
            return session;
        }
        pub fn getDocument(self: *@This(), a: A, key: []const u8, _: types.LookupOptions) !?types.LookupResult {
            var page = try self.scan(a, key, key, .{ .inclusive_from = true, .include_documents = true, .limit = 1 });
            defer page.deinit(a);
            if (page.documents.len == 0) return null;
            return .{ .json = try a.dupe(u8, page.documents[0].json) };
        }
        pub fn graphEdgesForCypherPageAt(self: *@This(), a: A, index: []const u8, key: []const u8, edge_types: []const []const u8, direction: @import("cypher").ast.Direction, cursor: ?DB.CypherEdgeScanCursor, limits: anytype, _: u64) !DB.CypherEdgePage {
            return self.source.graphEdgesForCypherPinnedSnapshot(a, &self.cut, index, key, edge_types, switch (direction) {
                .out => .out,
                .in => .in,
                .both => .both,
            }, cursor, .{ .max_edges = limits.max_edges, .max_owned_bytes = limits.max_owned_bytes, .max_scanned_rows = limits.max_scanned_rows, .peer_key = limits.peer_key });
        }
    };
}
