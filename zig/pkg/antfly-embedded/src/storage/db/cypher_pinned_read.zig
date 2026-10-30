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
    byte_range: types.ByteRange,
    row_policy_phase: @import("table_catalog.zig").RowPolicyPhase,
    row_policy_generation: u64,
    row_policy_catalog_epoch: u64,
    now: u64,
    graphs: []Graph,
    pub const Graph = struct { name: []const u8, identity: cypher.catalog.IndexIdentity, context: graph.GraphIndex.EdgeReadContext, owner: *@import("catalog/index_manager.zig").IndexManager.GraphReadOwner };
    pub fn deinit(self: *Snapshot) void {
        if (comptime engine.enabled) return @import("../engine.zig").call("cypherPinnedSnapshotDeinit", .{self}) catch @panic("incompatible storage owner");
        for (self.graphs) |*entry| {
            entry.context.deinit();
            entry.owner.release();
            self.a.free(entry.name);
        }
        self.a.free(self.graphs);
        if (self.indexes) |*indexes| indexes.deinit();
        self.primary.abort();
        self.byte_range.deinit(self.a);
        self.* = undefined;
    }
};
pub fn capture(db: anytype, a: A, fence: anytype, definition: cypher.catalog.Graph, table: []const u8) !Snapshot {
    var names: std.ArrayList([]const u8) = .empty;
    defer names.deinit(a);
    for (definition.relationships) |mapping| {
        if (!std.mem.eql(u8, definition.label(mapping.source_label).?.table, table)) continue;
        if (!cypher.dependencies.contains(names.items, mapping.index)) try names.append(a, mapping.index);
    }
    return captureSelected(db, a, fence, names.items, @import("antfly_platform").time.realtimeNs());
}
pub fn captureSelected(db: anytype, a: A, fence: anytype, names: []const []const u8, now: u64) !Snapshot {
    if (fence.source != db or !fence.primary.active or !fence.replay.active) return error.CypherSnapshotRequired;
    db.core.lockApplyShared();
    var locked = true;
    defer if (locked) db.core.unlockApplyShared();
    const range = db.core.byteRange();
    var byte_range: types.ByteRange = .{ .start = try a.dupe(u8, range.start), .end = undefined };
    errdefer if (byte_range.start.len != 0) a.free(byte_range.start);
    byte_range.end = try a.dupe(u8, range.end);
    errdefer if (byte_range.end.len != 0) a.free(byte_range.end);
    const policy = &db.local_execution.row_policy_gate;
    const phase = policy.currentPhase();
    const policy_generation = policy.generation.load(.acquire);
    const policy_epoch = policy.catalog_epoch.load(.acquire);
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
            entry.owner.release();
            a.free(entry.name);
        }
        graphs.deinit(a);
    }
    for (names) |index_name| {
        var duplicate = false;
        for (graphs.items) |entry| if (std.mem.eql(u8, entry.name, index_name)) {
            duplicate = true;
            break;
        };
        if (duplicate) continue;
        const identity = (try db.cypherGraphIndexIdentity(a, index_name)) orelse return error.IndexNotFound;
        db.core.lockApplyShared();
        defer db.core.unlockApplyShared();
        const owner = try db.core.index_manager.retainGraphReadOwner(index_name);
        errdefer owner.release();
        const entry = db.core.graphIndex(index_name) orelse return error.IndexNotFound;
        var context = try entry.index.beginEdgeReadContextWithProof(.both);
        errdefer context.deinit();
        const name = try a.dupe(u8, index_name);
        errdefer a.free(name);
        try graphs.append(a, .{ .name = name, .identity = identity, .context = context, .owner = owner });
    }
    return .{ .byte_range = byte_range, .row_policy_phase = phase, .row_policy_generation = policy_generation, .row_policy_catalog_epoch = policy_epoch, .a = a, .primary = primary, .indexes = indexes, .population_upper_bound = population, .schema_version = if (schema) |view| view.version() else 0, .now = now, .graphs = try graphs.toOwnedSlice(a) };
}
pub fn scan(db: anytype, a: A, cut: *Snapshot, from: []const u8, to: []const u8, input: types.ScanOptions) !types.ScanResult {
    var schema = db.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    if ((if (schema) |view| view.version() else 0) != cut.schema_version) return error.CatalogGenerationChanged;
    var scratch = std.heap.ArenaAllocator.init(cypher.backend.reclaimingAllocator(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var opts = try db.cypherDocumentScanOptions(temporary, input);
    var collector: collectors.Scan = .{ .alloc = a, .include_documents = opts.include_documents };
    defer collector.deinit();
    if (schema != null and schema.?.storageMode() == .relational) {
        opts.sql_document_preimage = true;
        var statement: @TypeOf(db.*).RelationalStatementSnapshot = retained: {
            var read = try cut.primary.forkRead();
            errdefer read.abort();
            const lease = try db.acquireCypherReadAuthority(temporary, opts.row_policy_principal_proof, opts.row_policy_database);
            break :retained .{ .index_catalog_captured = true, .indexes = if (cut.indexes) |pinned| .{ .head = pinned.head, .plan = pinned.plan.clone() } else null, .read = read, .schema_version = cut.schema_version, .row_policy_lease = lease, .row_policy_phase = cut.row_policy_phase, .row_policy_generation = cut.row_policy_generation, .row_policy_catalog_epoch = cut.row_policy_catalog_epoch };
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
        try db.scanVisitAtCypherSnapshot(temporary, from, to, opts, .{ .context = &collector, .visit = collectors.Scan.visit }, cut);
    }
    return collector.finish();
}
/// Document point reads retain the primary bytes and use span projection, so
/// wide unrequested payloads never become a DOM or escape into reply storage.
pub fn get(db: anytype, a: A, cut: *Snapshot, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
    const results = try getMany(db, a, cut, &.{key}, .{ .include_documents = true, .fields = opts.fields, .include_all_fields = opts.include_all_fields, .execution_deadline_ns = opts.execution_deadline_ns, .cancellation = opts.cancellation, .row_policy_principal_proof = opts.row_policy_principal_proof, .row_policy_database = opts.row_policy_database });
    defer a.free(results);
    return results[0];
}

/// One native batch owns one schema/policy/read cut and one projection plan.
/// Results retain input order and missing/hidden identities remain null.
pub fn getMany(db: anytype, a: A, cut: *Snapshot, keys: []const []const u8, input: types.ScanOptions) ![]?types.LookupResult {
    if (keys.len > 128) return error.CypherOwnerBudgetExceeded;
    var scratch = std.heap.ArenaAllocator.init(cypher.backend.reclaimingAllocator(a));
    defer scratch.deinit();
    const temporary = scratch.allocator();
    var schema = db.core.acquireSchemaView();
    defer if (schema) |*view| view.release();
    if ((if (schema) |view| view.version() else 0) != cut.schema_version) return error.SchemaVersionChanged;
    const results = try a.alloc(?types.LookupResult, keys.len);
    @memset(results, null);
    errdefer {
        for (results) |*result| if (result.*) |*value| value.deinit(a);
        a.free(results);
    }
    const opts = if (input.relational_query != null) input else try db.cypherDocumentScanOptions(temporary, input);
    if (schema) |view| if (view.storageMode() == .relational) {
        var facade: Database(@TypeOf(db.*)) = .{ .source = db, .core = db.core, .cut = cut.* };
        // The native read scope must release its bounded source buffers on
        // each retry/page, independently of the caller's reply arena.
        const backing = cypher.backend.reclaimingAllocator(a);
        const session = try facade.openRelationalReadSession(backing, "", "", opts);
        defer session.deinit();
        var row_scratch = std.heap.ArenaAllocator.init(backing);
        defer row_scratch.deinit();
        var start: usize = 0;
        while (start < keys.len) {
            const end = @min(keys.len, start + 64);
            var selected: [64][]const u8 = undefined;
            var positions: [64]usize = undefined;
            var count: usize = 0;
            for (keys[start..end], start..) |key, i| {
                try session.checkpoint();
                if (!cut.byte_range.contains(key) or @import("group_source_identity.zig").isAnchor(key)) continue;
                selected[count] = key;
                positions[count] = i;
                count += 1;
            }
            var next: usize = 0;
            while (next < count) {
                _ = row_scratch.reset(.free_all);
                try session.checkpoint();
                const batch = try @import("relational_rows.zig").lookupCypherBatch(&session.reader, row_scratch.allocator(), selected[next..count], 16 << 20);
                if (batch.consumed == 0 or batch.consumed > count - next or batch.rows.len != batch.consumed) return error.InvalidResponse;
                for (batch.rows, positions[next..][0..batch.consumed]) |row, position| {
                    try session.checkpoint();
                    if (row) |value| results[position] = .{ .json = try std.json.Stringify.valueAlloc(a, value.typed orelse return error.InvalidResponse, .{}), .version = value.version };
                }
                next += batch.consumed;
            }
            start = end;
        }
        try session.checkpoint();
        return results;
    };
    var authority = try db.acquireCypherReadAuthority(temporary, opts.row_policy_principal_proof, opts.row_policy_database);
    defer authority.release();
    var read = try cut.primary.forkRead();
    defer read.abort();
    const projection = @import("document_query.zig");
    const lookup: types.LookupOptions = .{ .fields = opts.fields, .include_all_fields = opts.include_all_fields };
    var plan = try projection.SpanProjectionPlan.init(temporary, lookup);
    defer if (plan) |*value| value.deinit();
    const internal = @import("../internal_keys.zig");
    const duration = if (schema) |view| view.visibilityTtlDurationNs() else 0;
    for (keys, results) |key, *out| {
        try checkBatch(&authority, opts);
        if (!cut.byte_range.contains(key) or @import("group_source_identity.zig").isAnchor(key)) continue;
        const store_key = try internal.documentKeyAlloc(temporary, key);
        defer temporary.free(store_key);
        const raw = read.getLeased(store_key) catch |err| switch (err) {
            error.NotFound => continue,
            else => return err,
        };
        const timestamp_key = try internal.ttlKeyAlloc(temporary, key);
        defer temporary.free(timestamp_key);
        const timestamp = read.get(timestamp_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        const version: u64 = if (timestamp) |bytes| if (bytes.len == 8) std.mem.readInt(u64, bytes[0..8], .little) else return error.InvalidData else 0;
        if (duration != 0 and version != 0 and @import("../ttl.zig").isExpired(version, duration, cut.now)) continue;
        var result = if (plan) |*value| try projection.lookupJsonWithPlan(temporary, a, raw, value) else try projection.lookupJsonWithOutput(temporary, a, raw, lookup);
        result.version = version;
        out.* = result;
    }
    try checkBatch(&authority, opts);
    return results;
}
fn checkBatch(authority: anytype, opts: types.ScanOptions) !void {
    const time = @import("antfly_platform").time;
    try authority.checkAt(@intCast(time.realtimeNs() / std.time.ns_per_s));
    if (opts.cancellation) |token| try token.check();
    if (opts.execution_deadline_ns) |deadline| if (time.monotonicNs() >= deadline) return error.DeadlineExceeded;
}

pub fn adjacencyRoute(a: A, name: []const u8, context: *graph.GraphIndex.EdgeReadContext) !cypher.backend.AdjacencyRoute {
    const outgoing = if (context.reads.outgoing) |*read| try graph.GraphIndex.endpointRangeInRead(a, read) else null;
    const incoming = if (context.reads.incoming) |*read| try graph.GraphIndex.endpointRangeInRead(a, read) else null;
    return .{ .index = try a.dupe(u8, name), .outgoing = if (outgoing) |r| .{ .empty = r.empty, .min = r.min, .max = r.max } else null, .incoming = if (incoming) |r| .{ .empty = r.empty, .min = r.min, .max = r.max } else null };
}
pub fn adjacencyRoutes(a: A, cut: *Snapshot) ![]const cypher.backend.AdjacencyRoute {
    const routes = try a.alloc(cypher.backend.AdjacencyRoute, cut.graphs.len);
    for (cut.graphs, routes) |*entry, *route| route.* = try adjacencyRoute(a, entry.name, &entry.context);
    return routes;
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
        pub const DocumentReadSession = DB.DocumentReadSession;
        pub const RelationalReadSession = DB.RelationalReadSession;
        pub fn close(self: *@This()) void {
            self.cut.deinit();
        }
        pub fn acquireCypherSchemaView(self: *@This()) @TypeOf(self.core.acquireSchemaView()) {
            var view = self.core.acquireSchemaView() orelse return null;
            if (view.version() != self.cut.schema_version) {
                view.release();
                return null;
            }
            return view;
        }
        pub fn acquireCypherIndexCatalog(self: *@This()) @TypeOf(self.core.relational_indexes.acquire()) {
            return if (self.cut.indexes) |pinned| .{ .head = pinned.head, .plan = pinned.plan.clone() } else null;
        }
        pub fn openDocumentReadSession(self: *@This(), a: A, from: []const u8, to: []const u8, opts: types.ScanOptions) !*DB.DocumentReadSession {
            var schema = self.core.acquireSchemaView();
            defer if (schema) |*view| view.release();
            if ((if (schema) |view| view.version() else 0) != self.cut.schema_version) return error.SchemaVersionChanged;
            return DB.DocumentReadSession.openSnapshot(a, self.core.store, try self.cut.primary.forkRead(), schema, self.cut.byte_range, from, to, opts, self.cut.now);
        }
        // Physical managed-search generations retain the durable cut path.
        // A forged search operation cannot read live state through this facade.
        pub fn searchCypherCut(_: *@This(), _: A, _: types.SearchRequest, _: []const u8, _: []const u8) !types.SearchResult {
            return error.CypherSnapshotRequired;
        }
        pub fn listIndexes(self: *@This(), a: A) @TypeOf(self.source.listIndexes(a)) {
            return self.source.listIndexes(a);
        }
        pub fn permitsCypherDocument(self: *@This(), a: A, document: std.json.Value, fields: []const []const u8, proof: []const u8, database: []const u8) !bool {
            return self.source.permitsCypherDocument(a, document, fields, proof, database);
        }
        fn checkSchema(self: *@This()) !void {
            var schema = self.core.acquireSchemaView();
            defer if (schema) |*view| view.release();
            if ((if (schema) |view| view.version() else 0) != self.cut.schema_version) return error.SchemaVersionChanged;
        }
        // SQL staged rows need schema expressions and field-edge extraction.
        // These operations consume the supplied postimage and bound metadata,
        // never source rows or live adjacency. The host also guards both sides
        // with its captured graph identity and authorization leases.
        pub fn normalizeCypherNode(self: *@This(), a: A, definition: cypher.catalog.Graph, node: *const cypher.value.Node, defaults: bool) !*const cypher.value.Node {
            try self.checkSchema();
            const result = try self.source.normalizeCypherNode(a, definition, node, defaults);
            try self.checkSchema();
            return result;
        }
        pub fn cypherDocumentRelationships(self: *@This(), a: A, definition: cypher.catalog.Graph, node: *const cypher.value.Node) ![]const *const cypher.value.Relationship {
            try self.checkSchema();
            const result = try self.source.cypherDocumentRelationships(a, definition, node);
            try self.checkSchema();
            return result;
        }
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
                break :cut .{ .index_catalog_captured = true, .indexes = if (self.cut.indexes) |indexes| .{ .head = indexes.head, .plan = indexes.plan.clone() } else null, .read = read, .schema_version = self.cut.schema_version, .row_policy_lease = lease, .row_policy_phase = self.cut.row_policy_phase, .row_policy_generation = self.cut.row_policy_generation, .row_policy_catalog_epoch = self.cut.row_policy_catalog_epoch };
            };
            defer statement.deinit();
            const session = try self.source.openRelationalReadSessionAtSnapshot(a, from, to, opts, &statement);
            session.reader.now_ns = self.cut.now;
            return session;
        }
        pub fn getCypherDocuments(self: *@This(), a: A, keys: []const []const u8, opts: types.ScanOptions, _: u64) ![]?types.LookupResult {
            return self.source.getCypherPinnedDocuments(a, &self.cut, keys, opts);
        }
        pub fn getDocument(self: *@This(), a: A, key: []const u8, opts: types.LookupOptions) !?types.LookupResult {
            return self.source.getCypherPinnedDocument(a, &self.cut, key, opts);
        }
        /// Authenticate every adjacency read while retaining its captured roots.
        /// Producer/end-point projection remains the adapter's authoritative
        /// per-entity policy check; no live adjacency fallback is permitted.
        pub fn cypherAdjacencyRoutes(self: *@This(), a: A) ![]const cypher.backend.AdjacencyRoute {
            return self.source.cypherPinnedAdjacencyRoutes(a, &self.cut);
        }
        pub fn graphEdgesForCypherPageAuthorizedAt(self: *@This(), a: A, index: []const u8, key: []const u8, edge_types: []const []const u8, direction: cypher.ast.Direction, cursor: ?DB.CypherEdgeScanCursor, limits: anytype, now: u64, proof: []const u8, database: []const u8) !DB.CypherEdgePage {
            var authority = try self.source.acquireCypherReadAuthority(a, proof, database);
            defer authority.release();
            const time = @import("antfly_platform").time;
            try authority.checkAt(@intCast(time.realtimeNs() / std.time.ns_per_s));
            var page = try self.graphEdgesForCypherPageAt(a, index, key, edge_types, direction, cursor, limits, now);
            errdefer page.deinit(a);
            try authority.checkAt(@intCast(time.realtimeNs() / std.time.ns_per_s));
            return page;
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
