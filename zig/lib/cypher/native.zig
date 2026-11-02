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

//! Native document/adjacency adapter. The caller owns the retained DB cuts and
//! their catalog and authorization leases for the complete statement lifetime.
const std = @import("std");
const root = @import("root.zig");
const v = root.value;
const b = root.backend;
const A = std.mem.Allocator;

/// Intersect one exact scalar membership domain before building disjoint
/// ordered partitions. Repeated SQL admission hints are idempotent; independent
/// properties still need a separately costed Cartesian access proof.
fn orderedMembershipDomain(a: A, work: *@import("value_work.zig").Work, filters: []const b.PropertyFilter, chosen: usize, kind: std.meta.Tag(v.Value)) !?[]const v.Value {
    if (kind != .integer and kind != .string and kind != .boolean) return null;
    const first = filters[chosen];
    for (filters) |filter| if (filter.op == .in) {
        if (!std.mem.eql(u8, first.property, filter.property) or filter.value != .list) return null;
    };
    var unique: std.ArrayList(v.Value) = .empty;
    var distinct: @import("value_work.zig").Index = .{};
    for (first.value.list) |value| {
        try work.charge(work.ptr, 1);
        if (value == .null) continue;
        if (std.meta.activeTag(value) != kind) return null;
        const hash = (try work.info(value)).hash;
        var duplicate = false;
        var candidate = distinct.head(hash);
        while (candidate) |position| {
            const link = distinct.links.items[position];
            if (try work.eql(unique.items[link.index], value)) {
                duplicate = true;
                break;
            }
            candidate = link.next;
        }
        if (!duplicate) {
            if (unique.items.len == 256) return null;
            try distinct.insert(a, hash, unique.items.len);
            try unique.append(a, value);
        }
    }
    for (filters, 0..) |filter, index| {
        if (index == chosen or filter.op != .in) continue;
        var membership: @import("value_work.zig").Index = .{};
        for (filter.value.list, 0..) |value, position| {
            try work.charge(work.ptr, 1);
            if (value == .null) continue;
            if (std.meta.activeTag(value) != kind) return null;
            try membership.insert(a, (try work.info(value)).hash, position);
        }
        var retained: usize = 0;
        for (unique.items) |value| {
            var candidate = membership.head((try work.info(value)).hash);
            while (candidate) |position| {
                const link = membership.links.items[position];
                if (try work.eql(value, filter.value.list[link.index])) {
                    unique.items[retained] = value;
                    retained += 1;
                    break;
                }
                candidate = link.next;
            }
        }
        unique.items.len = retained;
    }
    return try unique.toOwnedSlice(a);
}

pub fn Adapter(comptime DB: type, comptime GraphIndex: type, comptime EdgeScanCursor: type) type {
    return struct {
        const Self = @This();
        pub const Table = struct {
            name: []const u8,
            /// Immutable namespace captured with this logical table binding.
            storage_name: ?[]const u8 = null,
            db: *DB,
            /// Only for bounded materialization while the exact owner's
            /// primary/replay capture fences are held. Never retain this
            /// capability in a query backend after releasing the fence.
            capture_fence: if (@hasDecl(DB, "StatementReadFence")) ?*const DB.StatementReadFence else ?*const anyopaque = null,
        };
        pub const ReadAuthorization = struct { proof: []const u8 = "", database: []const u8 = "", deadline_ns: ?u64 = null, filter_query_json: []const u8 = "" };
        /// Mandatory hooks hold graph/table authority, refresh revocation at
        /// checkpoints, and apply field projection before any value is bound.
        pub const Authority = struct {
            ptr: *anyopaque,
            bind: *const fn (*anyopaque, root.catalog.Graph) anyerror!void,
            checkpoint: *const fn (*anyopaque) anyerror!void,
            project: *const fn (*anyopaque, A, *const v.Node) anyerror!?*const v.Node,
            /// Opt in only when the retained reader has enforced the complete
            /// read authorization above. Does not license point-read bypasses.
            /// Private count inputs may carry nullness and membership views;
            /// policies cannot be derived from their projected payloads.
            project_stored: ?*const fn (*anyopaque, A, *const v.Node) anyerror!?*const v.Node = null,
            prepare_metadata: ?*const fn (*anyopaque, A, []const u8) anyerror!root.read_fields.PreparedMetadata = null,
            project_relationship: *const fn (*anyopaque, A, *const v.Relationship) anyerror!?*const v.Relationship,
            search: ?*const fn (*anyopaque, A, []const u8, b.SearchInput) anyerror!b.SearchPage = null,
            read_authorization: ?*const fn (*anyopaque, A, []const u8) anyerror!ReadAuthorization = null,
            /// Physical owners can supply metadata captured with a retained cut
            /// even when their DB adapter has no population method.
            population_upper_bound: ?*const fn (*anyopaque, []const u8) anyerror!?u64 = null,
            /// The native reader enforces full-record policies, and project
            /// independently checks authority by identity rather than requiring
            /// unrequested node properties. Label predicates are still retained.
            storage_projection: bool = false,
            /// Projection may reject rows but preserves visible scalar values.
            ordered_projection: bool = false,
        };
        tables: []const Table,
        authority: Authority,
        ttl_now_ns: u64,
        graph: ?root.catalog.Graph = null,
        /// A routed owner serves only its catalog-certified table partition;
        /// the coordinator binds the complete graph before opening any owner.
        partitioned: bool = false,
        work: ?b.Work = null,
        membership_units: usize = 0,
        read_fields: ?[]const []const u8 = null,
        read_demand: ?root.read_fields.Demand = null,

        pub fn backend(self: *Self) b.Backend {
            return .{ .ptr = self, .vtable = &.{ .prefer_count_nodes = false, .adjacency_routes = adjacencyRoutes, .bind = bind, .checkpoint = checkpoint, .scan = scan, .scan_where = scanWhere, .probe_where = probeWhere, .count_nodes = countNodes, .expand_many = expandMany, .planned_domains = if (@hasDecl(DB, "RelationalReadSession")) @hasDecl(DB.RelationalReadSession, "seekPrefix") else false, .scan_ordered = scanOrdered, .estimate_where = estimateWhere, .population_upper_bound = populationUpperBound, .get = get, .get_many = getMany, .expand = expand, .expand_between = expandBetween, .search = search, .set_work = setWork, .set_read_fields = setReadFields, .set_read_demand = setReadDemand } };
        }
        fn setReadDemand(ptr: *anyopaque, demand: ?root.read_fields.Demand) void {
            cast(ptr).read_demand = demand;
        }
        fn observedGraph(self: *Self, a: A, table: []const u8) !root.catalog.Graph {
            var graph = self.graph.?;
            if (self.authority.project_stored == null) return graph;
            const demand = self.read_demand orelse return graph;
            const selected_labels = if (demand.table(table)) |item| item.labels else demand.labels;
            var labels: std.ArrayList(root.catalog.Label) = .empty;
            for (graph.labels) |label| {
                if (!std.mem.eql(u8, table, label.table)) continue;
                if (selected_labels) |names| {
                    for (names) |name| {
                        if (std.mem.eql(u8, label.name, name)) break;
                    } else continue;
                }
                try labels.append(a, label);
            }
            graph.labels = labels.items;
            return graph;
        }
        fn nodeDemand(self: *Self, table: []const u8) ?root.read_fields.Entity {
            if (self.authority.project_stored == null or !self.authority.storage_projection) return null;
            return if (self.read_demand) |demand| if (demand.table(table)) |item| item.nodes else demand.nodes else null;
        }
        fn setReadFields(ptr: *anyopaque, fields: ?[]const []const u8) void {
            cast(ptr).read_fields = fields;
        }
        fn acquireSchema(db: *DB) @TypeOf(db.core.acquireSchemaView()) {
            if (comptime @hasDecl(DB, "acquireCypherSchemaView")) return db.acquireCypherSchemaView();
            return db.core.acquireSchemaView();
        }
        fn acquireIndexes(db: *DB) @TypeOf(db.core.relational_indexes.acquire()) {
            if (comptime @hasDecl(DB, "acquireCypherIndexCatalog")) return db.acquireCypherIndexCatalog();
            return db.core.relational_indexes.acquire();
        }
        const ScanOptions = if (@hasDecl(DB, "cypherDocumentScanOptions")) @typeInfo(@typeInfo(@TypeOf(DB.cypherDocumentScanOptions)).@"fn".return_type.?).error_union.payload else void;
        fn scanOptions(self: *Self, a: A, db: *DB, table: []const u8, input: ScanOptions) !ScanOptions {
            return self.scanOptionsInGraph(a, db, table, input, try self.observedGraph(a, table));
        }
        fn scanOptionsInGraph(self: *Self, a: A, db: *DB, table: []const u8, input: ScanOptions, graph: root.catalog.Graph) !ScanOptions {
            var opts = try db.cypherDocumentScanOptions(a, input);
            const node_demand = self.nodeDemand(table);
            const requested = if (node_demand) |demand| demand.fields else self.read_fields;
            if (requested) |required| if (opts.relational_query) |*query| {
                var fields: std.ArrayList([]const u8) = .empty;
                for (query.fields) |field| for (required) |name| {
                    if (std.mem.eql(u8, field, name)) {
                        try fields.append(a, field);
                        break;
                    }
                };
                query.fields = if (node_demand != null and root.read_fields.rootKeys(node_demand.?.projection)) query.fields else fields.items;
                if (node_demand) |demand| {
                    query.cypher_presence_fields = demand.presence;
                    query.cypher_projection_json = if (demand.projection.len == 0) "" else try std.json.Stringify.valueAlloc(a, demand.projection, .{});
                }
            } else if (self.authority.storage_projection) {
                var fields: std.StringHashMapUnmanaged(void) = .empty;
                const key_shape = node_demand != null and root.read_fields.rootKeys(node_demand.?.projection);
                if (key_shape) {
                    // The key-shape projector enumerates literal field names
                    // and applies its explicit value overrides itself.
                    try fields.put(a, "*", {});
                } else {
                    for (required) |field| try fields.put(a, field, {});
                    for (graph.labels) |label| for (label.predicate) |entry| try fields.put(a, entry.name, {});
                }
                // Native document selectors interpret path syntax. Preserve
                // literal Cypher property names by falling back to full reads
                // whenever a name could be interpreted as a selector.
                var keys = fields.keyIterator();
                while (keys.next()) |field| {
                    if (key_shape and std.mem.eql(u8, field.*, "*")) continue;
                    if (field.len == 0) return opts;
                    for (field.*) |byte| if (!std.ascii.isAlphanumeric(byte) and byte != '_') return opts;
                }
                const names = try a.alloc([]const u8, fields.count());
                var iterator = fields.keyIterator();
                for (names) |*name| name.* = iterator.next().?.*;
                opts.fields = names;
                opts.include_all_fields = false;
                if (node_demand) |demand| {
                    opts.cypher_presence_fields = demand.presence;
                    opts.cypher_projection_json = if (demand.projection.len == 0) "" else try std.json.Stringify.valueAlloc(a, demand.projection, .{});
                }
            };
            return opts;
        }
        fn setWork(ptr: *anyopaque, work: ?b.Work) void {
            cast(ptr).work = work;
        }
        fn chargeMembership(ptr: *anyopaque, amount: usize) !void {
            const self = cast(ptr);
            if (self.work) |work| return work.charge(work.ptr, amount);
            try checkpoint(ptr);
            if (amount > 1_000_000 -| self.membership_units) return error.CypherStateBudgetExceeded;
            self.membership_units += amount;
        }
        pub fn valueWork(self: *Self, a: A) @import("value_work.zig").Work {
            return .{ .a = a, .ptr = self, .charge = chargeMembership };
        }
        fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
            const self = cast(ptr);
            try checkpoint(ptr);
            _ = try self.selected(table);
            const callback = self.authority.search orelse return error.CypherSearchUnavailable;
            return callback(self.authority.ptr, a, table, input);
        }
        fn selected(self: *Self, name: []const u8) !*DB {
            for (self.tables) |table| if (std.mem.eql(u8, table.name, name)) return table.db;
            return error.UndefinedGraphTable;
        }
        fn storageTableName(self: *Self, logical: []const u8) ?[]const u8 {
            for (self.tables) |table| if (std.mem.eql(u8, table.name, logical)) return table.storage_name;
            return null;
        }
        fn matchesStorageTable(self: *Self, logical: []const u8, actual: []const u8) bool {
            if (std.mem.eql(u8, logical, actual)) return true;
            for (self.tables) |table| if (std.mem.eql(u8, table.name, logical)) {
                return if (table.storage_name) |physical| std.mem.eql(u8, physical, actual) else false;
            };
            return false;
        }
        fn cast(ptr: *anyopaque) *Self {
            return @ptrCast(@alignCast(ptr));
        }
        fn checkpoint(ptr: *anyopaque) !void {
            const self = cast(ptr);
            try self.authority.checkpoint(self.authority.ptr);
        }
        fn bind(ptr: *anyopaque, a: A, graph: root.catalog.Graph) !void {
            const self = cast(ptr);
            if (self.ttl_now_ns == 0) return error.CypherSnapshotRequired;
            try self.authority.bind(self.authority.ptr, graph);
            for (graph.labels) |label| {
                const db = self.selected(label.table) catch |err| {
                    if (self.partitioned and err == error.UndefinedGraphTable) continue;
                    return err;
                };
                if (label.table_id != 0 and db.core.identity_namespace.table_id != label.table_id) return error.CatalogGenerationChanged;
                // A shared apply lease on a mutable DB is not a snapshot of
                // independently published graph and document stores.
                if (!db.hasRetainedQueryCut()) {
                    if (comptime @hasDecl(DB, "StatementReadFence")) {
                        const table = for (self.tables) |entry| {
                            if (entry.db == db and std.mem.eql(u8, entry.name, label.table)) break entry;
                        } else return error.CypherSnapshotRequired;
                        const fence = table.capture_fence orelse return error.CypherSnapshotRequired;
                        if (fence.source != db) return error.CypherSnapshotRequired;
                    } else return error.CypherSnapshotRequired;
                }
            }
            for (graph.relationships) |relationship| {
                const table = graph.label(relationship.source_label).?.table;
                if (self.partitioned) {
                    _ = self.selected(table) catch |err| {
                        if (err == error.UndefinedGraphTable) continue;
                        return err;
                    };
                }
                // Binding validates the full catalog, including intentionally
                // unopened projections in a demand-scoped immutable cut.
                const configured = (try (try self.selected(table)).cypherGraphIndexIdentity(a, relationship.index)) orelse return error.IndexNotFound;
                if (relationship.identity) |expected| {
                    const actual = configured;
                    if (expected.incarnation != actual.incarnation or expected.config_hash != actual.config_hash) return error.CatalogGenerationChanged;
                }
            }
            self.graph = graph;
        }
        pub fn node(self: *Self, a: A, table: []const u8, key: []const u8, json: []const u8) !?*const v.Node {
            const fields = (if (self.nodeDemand(table)) |demand| demand.fields else self.read_fields) orelse return self.fullNode(a, table, key, json);
            const key_shape = if (self.nodeDemand(table)) |demand| root.read_fields.rootKeys(demand.projection) else false;
            // Membership and policies inspect complete documents. Decode them
            // in reclaimable scratch, then retain only the authorized projection.
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const authorized = (try self.fullNode(temporary, table, key, json)) orelse return null;
            var projected = authorized.*;
            var properties: std.ArrayList(v.Entry) = .empty;
            for (authorized.properties) |entry| {
                for (fields) |field| {
                    if (std.mem.eql(u8, entry.name, field)) {
                        try properties.append(temporary, entry);
                        break;
                    }
                } else if (key_shape) try properties.append(temporary, .{ .name = entry.name, .value = if (entry.value == .null) .null else .{ .boolean = true } });
            }
            projected.properties = properties.items;
            return (try (v.Value{ .node = &projected }).clone(a)).node;
        }
        fn fullNode(self: *Self, a: A, table: []const u8, key: []const u8, json: []const u8) !?*const v.Node {
            if (comptime @hasDecl(DB, "cypherDocumentKeyAllowed")) if (!DB.cypherDocumentKeyAllowed(key)) return null;
            return self.fullNodeValue(a, table, key, try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }));
        }
        fn fullNodeValue(self: *Self, a: A, table: []const u8, key: []const u8, input: std.json.Value) !?*const v.Node {
            return self.projectNodeValue(a, table, key, input, false);
        }
        fn projectNodeValue(self: *Self, a: A, table: []const u8, key: []const u8, input: std.json.Value, stored: bool) !?*const v.Node {
            return self.projectNodeValueInGraph(a, table, key, input, stored, try self.observedGraph(a, table));
        }
        fn projectNodeValueInGraph(self: *Self, a: A, table: []const u8, key: []const u8, input: std.json.Value, stored: bool, graph: root.catalog.Graph) !?*const v.Node {
            const value = try v.Value.fromJson(a, input);
            if (value != .map) return error.CypherInvalidDocument;
            const result = try a.create(v.Node);
            result.* = .{ .identity = .{ .table = try a.dupe(u8, table), .key = try a.dupe(u8, key) }, .properties = try v.entityProperties(a, value.map) };
            var membership = self.valueWork(a);
            var labels: std.ArrayList([]const u8) = .empty;
            for (graph.labels) |label| if (try graph.admits(result, label.name, &membership)) try labels.append(a, label.name);
            result.labels = try labels.toOwnedSlice(a);
            if (stored) if (self.authority.project_stored) |project| return project(self.authority.ptr, a, result);
            return self.authority.project(self.authority.ptr, a, result);
        }
        fn scan(ptr: *anyopaque, a: A, table: []const u8, cursor: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            const self = cast(ptr);
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            try checkpoint(ptr);
            const db = try self.selected(table);
            const authorization = try self.readAuthorization(temporary, table);
            var found = if (comptime @hasDecl(DB, "cypherDocumentScanOptions"))
                try db.scan(temporary, cursor orelse "", "", try self.scanOptions(temporary, db, table, .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .inclusive_from = cursor == null, .include_documents = true, .limit = std.math.cast(u32, limit) orelse return error.InvalidArgument }))
            else
                try db.scan(temporary, cursor orelse "", "", .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .inclusive_from = cursor == null, .include_documents = true, .limit = std.math.cast(u32, limit) orelse return error.InvalidArgument });
            defer found.deinit(temporary);
            var nodes: std.ArrayList(*const v.Node) = .empty;
            for (found.documents) |document| if (try self.node(a, table, document.id, document.json)) |authorized| if (authorized.labels.len != 0) try nodes.append(a, authorized);
            const next = if (found.documents.len == limit) try a.dupe(u8, found.documents[found.documents.len - 1].id) else null;
            try checkpoint(ptr);
            return .{ .items = try nodes.toOwnedSlice(a), .next = next, .examined = found.documents.len, .unfiltered = true };
        }
        const PropertyCursor = struct { branch: usize = 0, index: ?[]const u8 = null, after: ?[]const u8 = null, key: ?[]const u8 = null, schema_version: ?u32 = null, primary: bool = false, planned: bool = false, domain_filter: ?usize = null, domain_length: usize = 0, prefix_filters: []const usize = &.{}, prefix_lengths: []const usize = &.{} };
        fn propertyFallback(ptr: *anyopaque, a: A, table: []const u8, cursor: PropertyCursor, limit: usize) !b.Page(*const v.Node) {
            const page = try scan(ptr, a, table, cursor.key, limit);
            return .{ .items = page.items, .examined = page.examined, .unfiltered = page.unfiltered, .next = if (page.next) |key| try std.json.Stringify.valueAlloc(a, PropertyCursor{ .key = key, .primary = true }, .{}) else null };
        }
        fn filterCompatible(column_type: anytype, value: v.Value) bool {
            return switch (column_type) {
                .string => value == .string,
                .integer => value == .integer,
                .number => value == .number or value == .integer,
                .boolean => value == .boolean,
                else => false,
            };
        }
        fn scanWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
            return (try scanWhereImpl(ptr, a, table, filters, encoded, limit, false)).?;
        }
        fn expandBatchOne(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identity: v.Identity, direction: root.ast.Direction, limit: usize, maximum: usize) !b.Page(*const v.Relationship) {
            return expandPair(ptr, a, mapping, identity, null, direction, null, limit, maximum);
        }
        fn expandMany(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identities: []const v.Identity, direction: root.ast.Direction, limit: usize, maximum: usize) !?b.ExpansionBatch {
            return @import("expansion_batch.zig").run(a, ptr, mapping, identities, direction, limit, maximum, cast(ptr).work, expandBatchOne);
        }
        fn adjacencyRoutes(ptr: *anyopaque, a: A, table: []const u8) ![]const b.AdjacencyRoute {
            const self = cast(ptr);
            try checkpoint(ptr);
            if (comptime @hasDecl(DB, "cypherAdjacencyRoutes")) {
                const db = try self.selected(table);
                return db.cypherAdjacencyRoutes(a);
            }
            return &.{};
        }
        fn countNodes(ptr: *anyopaque, a: A, table: []const u8, input: b.CountInput) !?b.CountResult {
            const self = cast(ptr);
            const work = self.work;
            const fields = self.read_fields;
            const demand = self.read_demand;
            defer {
                self.work = work;
                self.read_fields = fields;
                self.read_demand = demand;
            }
            if (try self.streamingCount(a, table, input)) |result| return result;
            return @import("owner_count.zig").run(a, self.backend(), self.graph orelse return null, table, input, work);
        }
        fn streamingCount(self: *Self, a: A, table: []const u8, input: b.CountInput) !?b.CountResult {
            if (comptime !@hasDecl(DB, "visitCypherDocuments")) return null;
            if (!self.authority.storage_projection or !self.authority.ordered_projection or input.max_examined >= std.math.maxInt(u32)) return null;
            const graph = self.graph orelse return null;
            var quota: @import("budget.zig") = .{ .backing = b.reclaimingAllocator(a), .limit = input.memory_bytes };
            return self.streamingCountBudget(a, table, input, graph, &quota) catch |err| {
                if (err == error.OutOfMemory and quota.exhausted) return error.CypherMemoryBudgetExceeded;
                return err;
            };
        }
        fn streamingCountBudget(self: *Self, a: A, table: []const u8, input: b.CountInput, graph: root.catalog.Graph, quota: *@import("budget.zig")) !?b.CountResult {
            if (comptime !@hasDecl(DB, "visitCypherDocuments")) return null;
            var arena = std.heap.ArenaAllocator.init(quota.allocator());
            defer arena.deinit();
            const temporary = arena.allocator();
            const membership_graph = if (self.authority.project_stored != null) try @import("owner_count.zig").membershipGraph(temporary, graph, input, table) else graph;
            const projection = (try @import("owner_count.zig").Projection.init(temporary, membership_graph, input)) orelse return null;
            if (!@import("owner_count.zig").permitsTable(graph, input, table)) return error.Forbidden;
            const Visitor = @typeInfo(@TypeOf(DB.visitCypherDocuments)).@"fn".param_types[5].?;
            const Entry = @typeInfo(@FieldType(Visitor, "visit")).pointer.child;
            const RowEntry = @typeInfo(Entry).@"fn".param_types[1].?;
            const Fold = struct {
                adapter: *Self,
                projection: @import("owner_count.zig").Projection,
                input: b.CountInput,
                graph: root.catalog.Graph,
                evaluator: ?*@import("runtime.zig").OwnerCountEvaluator = null,
                table: []const u8,
                quota: *@import("budget.zig"),
                parent: ?b.Work,
                meter: b.Meter,
                count: usize = 0,
                examined: usize = 0,
                progress_reported: bool = false,
                counts: []usize,
                stored: bool = false,
                fn charge(ptr: *anyopaque, amount: usize) !void {
                    const fold: *@This() = @ptrCast(@alignCast(ptr));
                    const local = fold.meter.work();
                    try local.charge(local.ptr, amount);
                    if (fold.parent) |parent| try parent.charge(parent.ptr, amount);
                }
                fn value(ptr: *anyopaque, item: v.Value) !void {
                    const fold: *@This() = @ptrCast(@alignCast(ptr));
                    var row = std.heap.ArenaAllocator.init(fold.quota.allocator());
                    defer row.deinit();
                    var evaluator: @import("value_work.zig").Work = .{ .a = row.allocator(), .ptr = fold, .charge = charge };
                    try evaluator.output(item);
                }
                fn account(fold: *@This(), examined: usize) !void {
                    try checkpoint(fold.adapter);
                    if (examined > fold.input.max_examined -| fold.examined) return error.CypherAnchorBudgetExceeded;
                    fold.examined += examined;
                    try charge(fold, examined);
                }
                fn progress(ptr: ?*anyopaque, examined: usize) !void {
                    const fold: *@This() = @ptrCast(@alignCast(ptr.?));
                    fold.progress_reported = true;
                    try fold.account(examined);
                }
                fn visit(ptr: ?*anyopaque, entry: RowEntry) !void {
                    const fold: *@This() = @ptrCast(@alignCast(ptr.?));
                    try checkpoint(fold.adapter);
                    // Typed readers report physical candidates before filters;
                    // plain document providers retain per-entry accounting.
                    if (!fold.progress_reported) try fold.account(1);
                    // Scans account for physical progress but share point-read
                    // admission: receipts and reserved rows are never nodes.
                    if (comptime @hasDecl(DB, "cypherDocumentKeyAllowed")) if (!DB.cypherDocumentKeyAllowed(entry.id)) return;
                    var row = std.heap.ArenaAllocator.init(fold.quota.allocator());
                    defer row.deinit();
                    const scratch = row.allocator();
                    const counted_node = candidate: {
                        if (entry.document_stream) |document| if (try document.countProjection(scratch, fold.projection.presence)) |projected|
                            break :candidate try fold.adapter.projectNodeValueInGraph(scratch, fold.table, entry.id, projected, fold.stored, fold.graph);
                        // Providers without the optional borrowed projection
                        // retain ordinary JSON validation and scalar semantics.
                        if (entry.document_json) |json| break :candidate try fold.adapter.fullNode(scratch, fold.table, entry.id, json);
                        if (entry.document_stream) |document| {
                            var output = std.Io.Writer.Allocating.init(scratch);
                            try output.writer.writeAll("{\"_id\":null");
                            try document.writeFields(&output.writer);
                            try output.writer.writeByte('}');
                            break :candidate try fold.adapter.fullNode(scratch, fold.table, entry.id, output.written());
                        }
                        return error.CypherInvalidDocument;
                    } orelse return;
                    var membership = fold.adapter.valueWork(scratch);
                    if (!try @import("owner_count.zig").admits(fold.graph, fold.input, counted_node, &membership)) return;
                    if (!try fold.evaluator.?.matches(scratch, counted_node)) return;
                    try charge(fold, fold.projection.arguments.len);
                    fold.count += 1;
                    for (fold.projection.arguments, fold.counts) |argument_value, *count| {
                        const present = switch (argument_value) {
                            .present => |present| present,
                            .field => |field| (try membership.property(.{ .node = counted_node }, field)) != .null,
                        };
                        if (present) count.* += 1;
                    }
                    try checkpoint(fold.adapter);
                }
            };
            const counts = try temporary.alloc(usize, input.arguments.len);
            @memset(counts, 0);
            var fold: Fold = .{ .adapter = self, .projection = projection, .input = input, .graph = membership_graph, .table = table, .quota = quota, .parent = self.work, .meter = .{ .a = quota.allocator(), .backend = self.backend(), .limit = input.max_work }, .counts = counts };
            const previous_work = self.work;
            const previous_fields = self.read_fields;
            const previous_demand = self.read_demand;
            self.read_demand = projection.demand;
            defer {
                self.read_demand = previous_demand;
                self.work = previous_work;
                self.read_fields = previous_fields;
            }
            self.work = .{ .ptr = &fold, .charge = Fold.charge, .value = Fold.value };
            self.read_fields = projection.fields;
            fold.evaluator = try @import("runtime.zig").OwnerCountEvaluator.create(temporary, quota.allocator(), self.backend(), membership_graph, input, self.work);
            defer fold.evaluator.?.deinit();
            const db = try self.selected(table);
            const authorization = try self.readAuthorization(temporary, table);
            // Only certified owner projections may consume visibility at the
            // original scan. Opaque providers retain their per-node callback.
            fold.stored = self.authority.project_stored != null;
            var opts = try self.scanOptionsInGraph(temporary, db, table, .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .filter_query_json = if (fold.stored) authorization.filter_query_json else "", .include_documents = true, .limit = @intCast(input.max_examined + 1) }, membership_graph);
            if (opts.relational_query) |*query| {
                const filters = try fold.evaluator.?.filters();
                // Share the complete sparse/composite candidate planner, but
                // consume its admitted domains without materializing pages.
                const sparse = for (filters) |filter| {
                    if (filter.op == .in) break true;
                } else false;
                if (sparse) {
                    if (comptime @hasDecl(DB, "RelationalReadSession") and @hasDecl(DB.RelationalReadSession, "visitCypherRows")) {
                        const streamed = try scanPlanned(self, temporary, table, filters, null, input.max_examined + 1, false, true, .{ .borrow_documents = true, .context = &fold, .visit = Fold.visit, .progress = Fold.progress });
                        if (streamed == null) return null;
                        const result: b.CountResult = .{ .count = fold.count, .counts = try a.dupe(usize, counts), .examined = fold.examined };
                        try result.validateFor(input.max_examined, input.arguments.len);
                        return result;
                    } else return null;
                }
                var schema = acquireSchema(db) orelse return error.SchemaVersionChanged;
                defer schema.release();
                const Query = @TypeOf(query.*);
                var conditions: std.ArrayList(Query.Condition) = .empty;
                try conditions.appendSlice(temporary, query.conditions);
                for (filters) |filter| {
                    if (conditions.items.len == 256) break;
                    const column = for (schema.tableSchema().relational_columns) |column| {
                        if (std.mem.eql(u8, column.name, filter.property)) break column;
                    } else continue;
                    // The same exact scalar admission used by ordinary native
                    // equality scans. Unsupported values remain authoritative
                    // graph membership checks after the storage reader.
                    if (filter.op != .is_null and !filterCompatible(column.column_type, filter.value)) continue;
                    const value: std.json.Value = if (column.column_type == .number and filter.value == .integer and (filter.value.integer < -9007199254740992 or filter.value.integer > 9007199254740992)) exact: {
                        // Share ordinary native equality's binary64 proof. An
                        // exactly representable large integer is a valid seek;
                        // ranges and rounded integers remain scalar residuals.
                        if (filter.op != .eq) continue;
                        const number: f64 = @floatFromInt(filter.value.integer);
                        if (@as(i128, @intFromFloat(number)) != @as(i128, filter.value.integer)) continue;
                        break :exact .{ .float = number };
                    } else try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, filter.value, .{}), .{ .parse_numbers = false });
                    try conditions.append(temporary, .{ .column = filter.property, .op = switch (filter.op) {
                        .eq => .eq,
                        .lt => .lt,
                        .lte => .lte,
                        .gt => .gt,
                        .gte => .gte,
                        .is_null => .is_not_distinct,
                        .in => unreachable,
                    }, .value = if (filter.op == .is_null) null else value });
                }
                query.conditions = conditions.items;
                query.auto_index = true;
            }
            try db.visitCypherDocuments(quota.allocator(), "", "", opts, .{ .borrow_documents = true, .context = &fold, .visit = Fold.visit, .progress = Fold.progress });
            try checkpoint(self);
            const result: b.CountResult = .{ .count = fold.count, .counts = try a.dupe(usize, counts), .examined = fold.examined };
            try result.validateFor(input.max_examined, input.arguments.len);
            return result;
        }
        fn probeWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) !?b.Probe {
            if (comptime !@hasDecl(DB, "openRelationalReadSession") or !@hasDecl(DB, "cypherDocumentScanOptions")) return null;
            if (comptime !@hasDecl(DB.RelationalReadSession, "seekPrefix")) return null;
            if (limit == 0 or limit > 128) return null;
            const self = cast(ptr);
            if (self.work) |work| try work.charge(work.ptr, limit + 1);
            var quota: @import("budget.zig") = .{ .backing = b.reclaimingAllocator(a), .limit = 1 << 20 };
            var arena = std.heap.ArenaAllocator.init(quota.allocator());
            defer arena.deinit();
            const page = (scanPlanned(ptr, arena.allocator(), table, filters, null, limit + 1, false, true, null) catch |err| {
                if (err == error.OutOfMemory and quota.exhausted) return .{ .page = .{ .items = &.{} }, .complete = false };
                if (err == error.RelationalRowResultTooLarge) return .{ .page = .{ .items = &.{} }, .complete = false };
                return err;
            }) orelse return null;
            const complete = page.next == null and (page.examined orelse page.items.len) <= limit;
            if (!complete) return .{ .page = .{ .items = &.{}, .examined = page.examined }, .complete = false };
            const nodes = try a.alloc(*const v.Node, page.items.len);
            for (page.items, nodes) |item, *out| out.* = (try (v.Value{ .node = item }).clone(a)).node;
            return .{ .page = .{ .items = nodes, .examined = page.examined }, .complete = true };
        }
        fn filterStrength(self: *Self, filters: []const b.PropertyFilter, name: []const u8, column_type: anytype) !u8 {
            var strength: u8 = 0;
            for (filters) |filter| {
                if (self.work) |work| try work.charge(work.ptr, 1);
                if (!std.mem.eql(u8, filter.property, name) or (filter.op != .is_null and !filterCompatible(column_type, filter.value))) continue;
                strength = @max(strength, if (filter.op == .eq or filter.op == .is_null) @as(u8, 4) else if (filter.op != .in) @as(u8, 2) else 0);
            }
            return strength;
        }
        const OrderedCursor = struct { reverse: bool = false, coarse: bool = false, index: []const u8, after: ?[]const u8 = null, schema_version: u32 };
        const OrderedPartition = struct {
            parent: *Self,
            filters: []const b.PropertyFilter,
            fn check(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try checkpoint(self.parent);
            }
            fn bindPartition(_: *anyopaque, _: A, _: root.catalog.Graph) !void {}
            fn close(_: *anyopaque) void {}
            fn scanPartition(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: usize) !b.Page(*const v.Node) {
                return error.CypherInvalidBackendPage;
            }
            fn getPartition(_: *anyopaque, _: A, _: v.Identity) !?*const v.Node {
                return error.CypherInvalidBackendPage;
            }
            fn expandPartition(_: *anyopaque, _: A, _: root.catalog.Relationship, _: v.Identity, _: root.ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
                return error.CypherInvalidBackendPage;
            }
            fn ordered(ptr: *anyopaque, a: A, table: []const u8, _: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, maximum: usize) !?b.Page(*const v.Node) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return scanOrderedScalar(self.parent, a, table, self.filters, order, after, limit, maximum, true);
            }
            fn backend(self: *@This()) b.Backend {
                return .{ .ptr = self, .vtable = &.{ .bind = bindPartition, .checkpoint = check, .scan = scanPartition, .get = getPartition, .expand = expandPartition, .scan_ordered = ordered } };
            }
        };
        fn scanOrdered(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, encoded: ?[]const u8, limit: usize, max_examined: usize) !?b.Page(*const v.Node) {
            if (comptime !@hasDecl(DB, "openRelationalReadSession") or !@hasDecl(DB, "cypherDocumentScanOptions")) return null;
            const self = cast(ptr);
            if (!self.authority.ordered_projection or (order.len == 0 and filters.len == 0) or order.len > 32) return null;
            try checkpoint(ptr);
            const chosen = for (filters, 0..) |filter, index| {
                if (filter.op == .in) break index;
            } else return scanOrderedScalar(ptr, a, table, filters, order, encoded, limit, max_examined, false);
            // A bounded disjoint union, never the hull of sparse values.
            const operand = filters[chosen].value;
            if (operand != .list) return null;
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const db = try self.selected(table);
            var schema = acquireSchema(db) orelse return null;
            defer schema.release();
            const column_type = for (schema.tableSchema().relational_columns) |column| {
                if (std.mem.eql(u8, column.name, filters[chosen].property)) break column.column_type;
            } else return null;
            // Exact scalar domains keep partitions disjoint after storage
            // conversion. Mixed and floating operands remain runtime residuals.
            const kind: std.meta.Tag(v.Value) = switch (column_type) {
                .integer => .integer,
                .string => .string,
                .boolean => .boolean,
                else => return null,
            };
            var work = self.valueWork(temporary);
            const unique = (try orderedMembershipDomain(temporary, &work, filters, chosen, kind)) orelse return null;
            // Cursor traces retain one baseline and one update per emitted
            // row. Domain admission follows the existing bounded membership
            // proof and statement work/memory/anchor budgets.
            if (unique.len == 0) return null;
            // Preserve the sparse-seek plan when the global ceiling cannot
            // cover even the merge's required owner heads and lookahead.
            if (encoded == null and limit != 0 and max_examined < limit +| (unique.len - 1)) return null;
            const partitions = try temporary.alloc(OrderedPartition, unique.len);
            const targets = try temporary.alloc(root.distributed.Target, unique.len);
            const snapshots = try temporary.alloc(root.distributed.Snapshot, unique.len);
            for (partitions, targets, snapshots, unique, 0..) |*partition, *target, *snapshot, value, index| {
                var conditions: std.ArrayList(b.PropertyFilter) = .empty;
                try conditions.append(temporary, .{ .property = filters[chosen].property, .op = .eq, .value = value });
                for (filters) |filter| if (filter.op != .in) try conditions.append(temporary, filter);
                partition.* = .{ .parent = self, .filters = conditions.items };
                target.* = .{ .table = table, .table_id = 1, .group_id = index + 1 };
                snapshot.* = .{ .backend = partition.backend(), .ptr = partition, .validate = OrderedPartition.check, .close = OrderedPartition.close };
            }
            // Reuse the authenticated coordinator's budgeted heap and exact
            // continuation protocol over these already-pinned local partitions.
            var merge: root.distributed.ReadSet = .{ .a = temporary, .work = self.work, .ordered_identity_ties = true, .ordered_item_cursors = true, .targets = targets, .snapshots = snapshots, .independent_validations = &.{}, .source = .{ .ptr = self, .checkpoint = checkpoint, .admit = undefined, .capture = undefined } };
            const merged_backend = merge.backend();
            var page = try merged_backend.vtable.scan_ordered.?(merged_backend.ptr, a, table, &.{}, order, encoded, limit, max_examined);
            // These partitions share one physical owner's primary encounter
            // domain; their synthetic merge positions are not owner provenance.
            if (page) |*value| value.encounter_owners = null;
            return page;
        }
        fn orderedIndexPrefix(self: *Self, schema: anytype, index: anytype, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, coarse: bool, reverse: bool) !?usize {
            if (index.predicate != null or index.tuple.keys.len == 0 or index.tuple.keys.len < order.len or (coarse and order.len == 0)) return null;
            const last = index.tuple.keys.len - order.len;
            var prefix: usize = if (coarse) 0 else last;
            candidates: while (prefix <= last) : (prefix += 1) {
                if (coarse and !reverse and prefix == last) break;
                for (index.tuple.keys[0..prefix]) |key| {
                    switch (key.column_type) {
                        .integer, .string, .boolean => {},
                        else => continue :candidates,
                    }
                    if (key.expression != null or key.fold_ascii) continue :candidates;
                    const name = schema.tableSchema().relational_columns[key.ordinal].name;
                    if (try self.filterStrength(filters, name, key.column_type) != 4) continue :candidates;
                }
                for (index.tuple.keys[prefix..][0..order.len], order) |key, requested| {
                    if (self.work) |work| try work.charge(work.ptr, 1);
                    if (key.expression != null or key.fold_ascii or key.descending != (requested.descending != reverse) or key.nulls_first != (requested.descending != reverse)) continue :candidates;
                    if (!std.mem.eql(u8, schema.tableSchema().relational_columns[key.ordinal].name, requested.property)) continue :candidates;
                    switch (key.column_type) {
                        .integer, .string, .boolean => {},
                        else => continue :candidates,
                    }
                }
                return prefix;
            }
            return null;
        }
        fn scanOrderedScalar(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, encoded: ?[]const u8, limit: usize, max_examined: usize, partition: bool) !?b.Page(*const v.Node) {
            if (comptime !@hasDecl(DB, "openRelationalReadSession") or !@hasDecl(DB, "cypherDocumentScanOptions")) return null;
            const self = cast(ptr);
            if (!self.authority.ordered_projection or (order.len == 0 and filters.len == 0) or order.len > 32) return null;
            // Sparse unions already have bounded equality seeks. A single
            // ordered scan must not replace them with their broad hull.
            for (filters) |filter| if (filter.op == .in) return null;
            if (limit != 0 and max_examined == 0) return error.CypherAnchorBudgetExceeded;
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            try checkpoint(ptr);
            const db = try self.selected(table);
            var schema = acquireSchema(db) orelse return null;
            defer schema.release();
            var indexes = acquireIndexes(db) orelse return null;
            defer indexes.deinit();
            if (indexes.plan.schemaView().epoch != schema.epoch) return error.CatalogGenerationChanged;
            const cursor = if (encoded) |bytes| try std.json.parseFromSliceLeaky(OrderedCursor, temporary, bytes, .{}) else null;
            const modes = [_]struct { coarse: bool, reverse: bool }{ .{ .coarse = false, .reverse = false }, .{ .coarse = true, .reverse = false }, .{ .coarse = true, .reverse = true } };
            for (modes) |mode| candidates: for (indexes.plan.boundIndexes()) |index| {
                const coarse = mode.coarse;
                const reverse = mode.reverse;
                if (cursor) |position| if (!std.mem.eql(u8, position.index, index.name) or position.coarse != coarse or position.reverse != reverse) continue;
                const prefix = (try self.orderedIndexPrefix(schema, index, filters, order, coarse, reverse)) orelse continue;
                if (cursor == null) {
                    const leading = index.tuple.keys[0];
                    const strength = try self.filterStrength(filters, schema.tableSchema().relational_columns[leading.ordinal].name, leading.column_type);
                    // Prefer a stronger equality access path to an unrelated sort
                    // index. The ordinary native planner can cost those candidates;
                    // Cypher retains a bounded top-k over their qualifying rows.
                    for (indexes.plan.boundIndexes()) |alternative| {
                        if (alternative.predicate != null or alternative.tuple.keys.len == 0) continue;
                        const key = alternative.tuple.keys[0];
                        if (key.expression != null or key.fold_ascii) continue;
                        if (try self.filterStrength(filters, schema.tableSchema().relational_columns[key.ordinal].name, key.column_type) > strength) continue :candidates;
                    }
                }
                if (partition and cursor == null) {
                    const leading = index.tuple.keys[0];
                    const strength = try self.filterStrength(filters, schema.tableSchema().relational_columns[leading.ordinal].name, leading.column_type);
                    for (indexes.plan.boundIndexes()) |alternative| {
                        if (alternative.tuple.keys.len == 0) continue;
                        const key = alternative.tuple.keys[0];
                        if (key.expression != null or key.fold_ascii) continue;
                        if (try self.filterStrength(filters, schema.tableSchema().relational_columns[key.ordinal].name, key.column_type) <= strength) continue;
                        if (try self.orderedIndexPrefix(schema, alternative, filters, order, coarse, reverse) != null) continue :candidates;
                    }
                }
                const authorization = try self.readAuthorization(temporary, table);
                var opts = try self.scanOptions(temporary, db, table, .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .include_documents = true, .limit = @intCast(@min(limit, 1024)) });
                if (opts.relational_query == null) return null;
                // The explicit index supplies order; compatible predicates
                // supply exact seek bounds through the native condition planner.
                const Query = @TypeOf(opts.relational_query.?);
                var conditions: std.ArrayList(Query.Condition) = .empty;
                for (filters) |filter| {
                    if (conditions.items.len == 256) break;
                    if (self.work) |work| try work.charge(work.ptr, 1);
                    if (filter.op == .in) continue; // Sparse unions need separate ordered streams.
                    const column_type = for (schema.tableSchema().relational_columns) |column| {
                        if (std.mem.eql(u8, column.name, filter.property)) break column.column_type;
                    } else continue;
                    if (filter.op != .is_null and !filterCompatible(column_type, filter.value)) continue;
                    if (column_type == .number and filter.value == .integer and filter.op != .eq and
                        (filter.value.integer < -9007199254740992 or filter.value.integer > 9007199254740992)) continue;
                    const value = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, filter.value, .{}), .{ .parse_numbers = false });
                    try conditions.append(temporary, .{
                        .column = filter.property,
                        .op = switch (filter.op) {
                            .eq => .eq,
                            .lt => .lt,
                            .lte => .lte,
                            .gt => .gt,
                            .gte => .gte,
                            // Null-safe equality supplies an exact prefix seek to
                            // storage's equality-domain planner, even if empty.
                            .is_null => .is_not_distinct,
                            .in => unreachable,
                        },
                        .value = if (filter.op == .is_null) null else value,
                    });
                }
                // Partition equality is a disjointness proof, not an optional
                // hint. It is first in the filter list so the hint cap cannot
                // discard it. Failed storage lowering must decline the union.
                if (partition and (conditions.items.len == 0 or conditions.items[0].op != .eq or !std.mem.eql(u8, conditions.items[0].column, filters[0].property))) continue :candidates;
                // Omitting a fixed prefix would invalidate the order proof,
                // even if an oversized hint list or storage coercion declined
                // a condition. Every skipped key needs an enforced equality.
                for (index.tuple.keys[0..prefix]) |key| {
                    const name = schema.tableSchema().relational_columns[key.ordinal].name;
                    const enforced = for (conditions.items) |condition| {
                        if (self.work) |work| try work.charge(work.ptr, 1);
                        if ((condition.op == .eq or condition.op == .is_not_distinct) and std.mem.eql(u8, condition.column, name)) break true;
                    } else false;
                    if (!enforced) continue :candidates;
                }
                opts.relational_query.?.conditions = conditions.items;
                opts.relational_query.?.index = index.name;
                opts.relational_query.?.auto_index = true;
                opts.relational_query.?.reverse = reverse;
                const has_after = for (order) |field| {
                    if (field.after != null) break true;
                } else false;
                const nonnull_only = order.len == 1 and order[0].exclude_null;
                if (has_after or nonnull_only) {
                    if (has_after) for (order) |field| if (field.after == null) return error.CypherInvalidBackendPage;
                    const values = try temporary.alloc(std.json.Value, prefix + order.len);
                    for (index.tuple.keys[0..prefix], values[0..prefix]) |key, *out| {
                        const name = schema.tableSchema().relational_columns[key.ordinal].name;
                        const fixed = for (filters) |filter| {
                            if ((filter.op == .eq or filter.op == .is_null) and std.mem.eql(u8, filter.property, name) and (filter.op == .is_null or filterCompatible(key.column_type, filter.value))) break filter.value;
                        } else return error.CypherInvalidBackendPage;
                        out.* = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, fixed, .{}), .{ .parse_numbers = false });
                    }
                    for (order, values[prefix..]) |field, *out| out.* = if (field.after) |key| try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, key, .{}), .{ .parse_numbers = false }) else .null;
                    if (nonnull_only) {
                        const nonnull = try temporary.dupe(std.json.Value, values);
                        nonnull[prefix] = .null;
                        if (index.tuple.keys[prefix].nulls_first) opts.relational_query.?.lower = .{ .values = nonnull, .inclusive = false } else opts.relational_query.?.upper = .{ .values = nonnull, .inclusive = false };
                    }
                    if (has_after) {
                        if (!reverse) opts.relational_query.?.lower = .{ .values = values, .inclusive = false } else opts.relational_query.?.upper = .{ .values = values, .inclusive = false };
                    }
                }
                opts.relational_query.?.after = if (cursor) |position| position.after else null;
                opts.relational_query.?.schema_version = if (cursor) |position| position.schema_version else schema.version();
                const session = db.openRelationalReadSession(temporary, "", "", opts) catch |err| switch (err) {
                    error.RelationalIndexNotReady, error.IndexNotFound => if (cursor == null) continue :candidates else return error.CatalogGenerationChanged,
                    error.InvalidBatchRequest, error.RelationalIndexColumnTypeMismatch, error.UnsupportedRelationalIndexColumn, error.RelationalIndexColumnNotFound => retry: {
                        if (prefix != 0 or partition) {
                            if (cursor != null) return error.CatalogGenerationChanged;
                            continue :candidates;
                        }
                        // A storage type can be narrower than Cypher. Keep the
                        // order certificate and evaluate unsupported operands
                        // in the authoritative runtime residual instead.
                        opts.relational_query.?.conditions = &.{};
                        break :retry try db.openRelationalReadSession(temporary, "", "", opts);
                    },
                    else => return err,
                };
                defer session.deinit();
                session.reader.now_ns = self.ttl_now_ns;
                try session.checkpoint();
                if (limit == 0) return .{ .items = &.{}, .cursors = &.{}, .examined = 0, .stable_ties = !coarse, .exact_keys = true, .next = try std.json.Stringify.valueAlloc(a, OrderedCursor{ .reverse = reverse, .coarse = coarse, .index = index.name, .schema_version = session.reader.active.version(), .after = if (cursor) |position| position.after else null }, .{}) };
                var page = try session.reader.nextPage(temporary, null, .{ .rows = @min(limit, 1024), .records = @min(@min(@max(limit, 128), max_examined), 1024), .output_bytes = 16 << 20 });
                defer page.deinit();
                var nodes: std.ArrayList(*const v.Node) = .empty;
                var cursors: std.ArrayList(?[]const u8) = .empty;
                for (page.rows) |row| if (try self.node(a, table, row.key, row.json)) |node_value| if (node_value.labels.len != 0) {
                    try nodes.append(a, node_value);
                    try cursors.append(a, try std.json.Stringify.valueAlloc(a, OrderedCursor{ .reverse = reverse, .coarse = coarse, .index = index.name, .schema_version = session.reader.active.version(), .after = row.cursor orelse return error.CypherInvalidBackendCursor }, .{}));
                };
                const next = if (page.more) try std.json.Stringify.valueAlloc(a, OrderedCursor{ .reverse = reverse, .coarse = coarse, .index = index.name, .schema_version = session.reader.active.version(), .after = page.next_cursor orelse return error.CypherInvalidBackendCursor }, .{}) else null;
                if (cursors.items.len != 0) cursors.items[cursors.items.len - 1] = next;
                try checkpoint(ptr);
                return .{ .items = try nodes.toOwnedSlice(a), .cursors = try cursors.toOwnedSlice(a), .examined = page.records_examined, .stable_ties = !coarse, .exact_keys = true, .next = next };
            };
            if (cursor != null) return error.CatalogGenerationChanged;
            return null;
        }
        fn populationUpperBound(ptr: *anyopaque, _: A, table: []const u8) !?u64 {
            const self = cast(ptr);
            if (comptime !@hasDecl(DB, "cypherPopulationUpperBound")) if (self.authority.population_upper_bound == null) return null;
            try checkpoint(ptr);
            if (self.work) |work| try work.charge(work.ptr, 1);
            const count = if (self.authority.population_upper_bound) |read| try read(self.authority.ptr, table) else if (comptime @hasDecl(DB, "cypherPopulationUpperBound")) try (try self.selected(table)).cypherPopulationUpperBound() else null;
            try checkpoint(ptr);
            return count;
        }
        fn estimateWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) !?usize {
            if (limit == 0) return null;
            if (filters.len == 0) return if (try populationUpperBound(ptr, a, table)) |count| @intCast(@min(count, limit)) else null;
            // Costing can ask about a much larger population than one native
            // owner page. A partial page is unknown, never an exact count.
            const maximum = @min(limit, 4096);
            if (cast(ptr).work) |work| try work.charge(work.ptr, maximum);
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            return scanWhereImpl(ptr, scratch.allocator(), table, filters, null, maximum, true);
        }
        fn WhereResult(comptime index_only: bool) type {
            return if (index_only) ?usize else ?b.Page(*const v.Node);
        }
        fn scanWhereImpl(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize, comptime index_only: bool) anyerror!WhereResult(index_only) {
            try checkpoint(ptr);
            if (filters.len == 0) return if (index_only) null else try scan(ptr, a, table, encoded, limit);
            for (filters) |filter| if (filter.op == .in and filter.value == .list and filter.value.list.len == 0) {
                if (index_only) return 0;
                return .{ .items = &.{} };
            };
            if (comptime @hasDecl(DB, "openRelationalReadSession") and @hasDecl(DB, "cypherDocumentScanOptions")) {
                if (comptime @hasDecl(DB.RelationalReadSession, "seekPrefix")) return scanPlanned(ptr, a, table, filters, encoded, limit, index_only, false, null);
            }
            return scanLegacy(ptr, a, table, filters, encoded, limit, index_only);
        }
        fn scanLegacy(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize, comptime index_only: bool) anyerror!WhereResult(index_only) {
            if (comptime !@hasDecl(DB, "openRelationalReadSession") or !@hasDecl(DB, "cypherDocumentScanOptions")) return if (index_only) null else try scan(ptr, a, table, encoded, limit);
            if (filters.len == 0) return if (index_only) null else try scan(ptr, a, table, encoded, limit);
            const self = cast(ptr);
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            try checkpoint(ptr);
            const db = try self.selected(table);
            const cursor = if (encoded) |json| try std.json.parseFromSliceLeaky(PropertyCursor, temporary, json, .{}) else PropertyCursor{};
            if (cursor.primary) return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
            const authorization = try self.readAuthorization(temporary, table);
            var opts = try self.scanOptions(temporary, db, table, .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .include_documents = true, .limit = @intCast(limit) });
            if (opts.relational_query == null) return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
            var schema = acquireSchema(db) orelse return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
            defer schema.release();
            const Query = @TypeOf(opts.relational_query.?);
            for (filters) |filter| if (filter.op == .in and filter.domain != null) {
                const column = for (schema.tableSchema().relational_columns) |column| {
                    if (std.mem.eql(u8, column.name, filter.property)) break column;
                } else return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
                const compatible = switch (filter.domain.?) {
                    .string => column.column_type == .string,
                    .boolean => column.column_type == .boolean,
                    .integer => column.column_type == .integer or column.column_type == .number,
                    .integer_inexact_double => column.column_type == .integer,
                };
                if (!compatible) return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
                break;
            };
            var conditions: std.ArrayList(Query.Condition) = .empty;
            var alternatives: []const v.Value = &.{};
            var alternative_condition: ?usize = null;
            for (filters) |filter| {
                if (conditions.items.len >= 256) break;
                var known = false;
                for (opts.relational_query.?.fields) |field| if (std.mem.eql(u8, field, filter.property)) {
                    known = true;
                    break;
                };
                if (!known) continue;
                const column_type = for (schema.tableSchema().relational_columns) |column| {
                    if (std.mem.eql(u8, column.name, filter.property)) break column.column_type;
                } else continue;
                var operand = filter.value;
                if (filter.op == .in) {
                    // One union of equality seeks; other IN predicates remain
                    // residuals. Never replace a sparse set by its broad hull.
                    if (alternatives.len != 0 or operand != .list or operand.list.len > 256) continue;
                    var unique: std.ArrayList(v.Value) = .empty;
                    var distinct: @import("value_work.zig").Index = .{};
                    var work = self.valueWork(temporary);
                    var compatible = true;
                    for (operand.list) |item| {
                        if (item == .null) continue;
                        if (!filterCompatible(column_type, item) or (column_type == .number and item == .integer and
                            (item.integer < -9007199254740992 or item.integer > 9007199254740992)))
                        {
                            compatible = false;
                            break;
                        }
                        const hash = (try work.info(item)).hash;
                        var duplicate = false;
                        var candidate = distinct.head(hash);
                        while (candidate) |position| {
                            const link = distinct.links.items[position];
                            if (try work.eql(unique.items[link.index], item)) {
                                duplicate = true;
                                break;
                            }
                            candidate = link.next;
                        }
                        if (!duplicate) {
                            try distinct.insert(temporary, hash, unique.items.len);
                            try unique.append(temporary, item);
                        }
                    }
                    if (!compatible) continue;
                    if (unique.items.len == 0) {
                        if (index_only) return 0;
                        return .{ .items = &.{} };
                    }
                    alternatives = unique.items;
                    alternative_condition = conditions.items.len;
                    if (cursor.branch >= alternatives.len) return error.CypherInvalidBackendCursor;
                    operand = alternatives[cursor.branch];
                }
                // NUMERIC can round in the Cypher projection; its exact index
                // bounds are unsafe. Unsupported/mixed operands stay residual.
                if (filter.op != .is_null and !filterCompatible(column_type, operand)) continue;
                // Open bounds cannot round a large integer to the neighboring
                // double: that would exclude a true Cypher comparison.
                if (column_type == .number and operand == .integer and filter.op != .eq and filter.op != .in and
                    (operand.integer < -9007199254740992 or operand.integer > 9007199254740992)) continue;
                const value = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, operand, .{}), .{ .parse_numbers = false });
                if (conditions.items.len < 256) try conditions.append(temporary, .{ .column = filter.property, .op = switch (filter.op) {
                    .eq, .in => .eq,
                    .lt => .lt,
                    .lte => .lte,
                    .gt => .gt,
                    .gte => .gte,
                    .is_null => .is_not_distinct,
                }, .value = if (filter.op == .is_null) null else value });
            }
            if (conditions.items.len == 0) return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
            opts.relational_query.?.conditions = conditions.items;
            if (index_only) {
                // Cardinality is advisory candidate evidence. Native policy
                // and visibility checks remain in the reader; graph membership
                // may further reduce the real input. No node binding or scalar
                // payload is needed to compute this upper bound.
                opts.fields = &.{};
                opts.include_all_fields = false;
                opts.relational_query.?.fields = &.{};
            }
            opts.relational_query.?.auto_index = true;
            opts.relational_query.?.index = cursor.index;
            opts.relational_query.?.schema_version = cursor.schema_version;
            opts.inclusive_from = cursor.key == null;
            var shared_session: ?*DB.RelationalReadSession = null;
            defer if (shared_session) |session| session.deinit();
            if (comptime @hasDecl(DB.RelationalReadSession, "seekPrefix")) if (alternative_condition) |condition| {
                var indexes = acquireIndexes(db);
                defer if (indexes) |*pinned| pinned.deinit();
                if (indexes) |pinned| for (pinned.plan.boundIndexes()) |index| {
                    if (cursor.index) |name| if (!std.mem.eql(u8, name, index.name)) continue;
                    if (index.predicate != null or index.tuple.keys.len == 0) continue;
                    const leading = index.tuple.keys[0];
                    if (leading.expression != null or leading.fold_ascii or !std.mem.eql(u8, schema.tableSchema().relational_columns[leading.ordinal].name, conditions.items[condition].column)) continue;
                    var shared_opts = opts;
                    const rest = try temporary.alloc(Query.Condition, conditions.items.len - 1);
                    @memcpy(rest[0..condition], conditions.items[0..condition]);
                    @memcpy(rest[condition..], conditions.items[condition + 1 ..]);
                    shared_opts.relational_query.?.conditions = rest;
                    shared_opts.relational_query.?.index = index.name;
                    shared_opts.relational_query.?.schema_version = schema.version();
                    shared_opts.relational_query.?.auto_index = false;
                    shared_session = db.openRelationalReadSession(temporary, "", "", shared_opts) catch |err| switch (err) {
                        error.RelationalIndexNotReady, error.IndexNotFound => continue,
                        else => return err,
                    };
                    shared_session.?.reader.now_ns = self.ttl_now_ns;
                    break;
                };
            };
            var branch = cursor.branch;
            var nodes: std.ArrayList(*const v.Node) = .empty;
            var examined: usize = 0;
            var count: usize = 0;
            while (true) {
                // Reclaim each physical seek's reader/decode state while the
                // prepared domain, schema and output page remain alive.
                var seek_scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
                defer seek_scratch.deinit();
                const seek_a = seek_scratch.allocator();
                try checkpoint(ptr);
                if (self.work) |work| try work.charge(work.ptr, 1);
                if (alternative_condition) |condition| conditions.items[condition].value = try std.json.parseFromSliceLeaky(std.json.Value, seek_a, try std.json.Stringify.valueAlloc(seek_a, alternatives[branch], .{}), .{ .parse_numbers = false });
                opts.relational_query.?.after = if (branch == cursor.branch) cursor.after else null;
                const session = shared_session orelse (db.openRelationalReadSession(seek_a, cursor.key orelse "", "", opts) catch |err| switch (err) {
                    error.InvalidBatchRequest, error.RelationalIndexColumnTypeMismatch, error.UnsupportedRelationalIndexColumn, error.RelationalIndexColumnNotFound => return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit),
                    else => return err,
                });
                defer if (shared_session == null) session.deinit();
                if (comptime @hasDecl(DB.RelationalReadSession, "seekPrefix")) if (shared_session != null) {
                    const value = conditions.items[alternative_condition.?].value orelse return error.CypherInvalidBackendPage;
                    try session.seekPrefix(&.{value}, if (branch == cursor.branch) cursor.after else null);
                };
                if (session.reader.index == null) return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
                session.reader.now_ns = self.ttl_now_ns;
                try session.checkpoint();
                var page = try session.reader.nextPage(seek_a, null, .{ .rows = limit - count, .records = limit - examined, .output_bytes = 16 << 20 });
                defer page.deinit();
                examined += page.records_examined;
                count += page.rows.len;
                if (!index_only) for (page.rows) |row| {
                    if (try self.node(a, table, row.key, row.json)) |node_value| if (node_value.labels.len != 0) try nodes.append(a, node_value);
                };
                const more_branches = alternatives.len != 0 and branch + 1 < alternatives.len;
                if (!page.more and !more_branches) {
                    try checkpoint(ptr);
                    if (index_only) return count;
                    return .{ .items = try nodes.toOwnedSlice(a), .examined = examined };
                }
                if (page.more or examined >= limit or count >= limit) {
                    if (index_only) return null;
                    const next = try std.json.Stringify.valueAlloc(a, PropertyCursor{ .branch = if (page.more) branch else branch + 1, .schema_version = session.reader.active.version(), .index = session.reader.index.?.name, .after = if (page.more) page.next_cursor else null }, .{});
                    try checkpoint(ptr);
                    return .{ .items = try nodes.toOwnedSlice(a), .examined = examined, .next = next };
                }
                branch += 1;
            }
        }

        const Domain = struct { filter: usize, values: []const v.Value, certified: bool, total: usize, max_string_bytes: ?usize = null };
        const PrefixDomain = struct { domain: usize, position: usize };
        const SeekPlan = struct {
            prefix_domains: []PrefixDomain = &.{},
            index: []const u8,
            domain: ?usize = null,
            prefix: []std.json.Value = &.{},
            usable: bool = true,
            complete: bool = false,
            cost: usize = std.math.maxInt(usize),
        };
        fn branchCount(plan: SeekPlan, domains: []const Domain, samples: bool) usize {
            const primary = plan.domain orelse return 1;
            var count = if (samples) domains[primary].values.len else domains[primary].total;
            for (plan.prefix_domains) |axis| count *= if (samples) domains[axis.domain].values.len else domains[axis.domain].total;
            return count;
        }
        fn branchPrefix(plan: *SeekPlan, domains: []const Domain, branch: usize, offset: usize, samples: bool) void {
            const primary = plan.domain.?;
            const size = if (samples) domains[primary].values.len else domains[primary].total;
            plan.prefix[plan.prefix.len - 1] = jsonOperand(domains[primary].values[branch % size - offset]);
            var rank = branch / size;
            var i = plan.prefix_domains.len;
            while (i != 0) {
                i -= 1;
                const axis = plan.prefix_domains[i];
                const length = if (samples) domains[axis.domain].values.len else domains[axis.domain].total;
                plan.prefix[axis.position] = jsonOperand(domains[axis.domain].values[rank % length]);
                rank /= length;
            }
        }
        fn jsonOperand(value: v.Value) std.json.Value {
            return switch (value) {
                .null => .null,
                .integer => |n| .{ .integer = n },
                .number => |n| .{ .float = n },
                .boolean => |n| .{ .bool = n },
                .string => |s| .{ .string = s },
                else => unreachable,
            };
        }
        fn unusableSeek(err: anyerror) bool {
            return switch (err) {
                error.IndexNotFound, error.RelationalIndexNotReady, error.PartialIndexPredicateNotImplied, error.InvalidBatchRequest, error.RelationalIndexColumnTypeMismatch, error.UnsupportedRelationalIndexColumn, error.RelationalIndexColumnNotFound, error.RelationalIndexKeyTooLarge, error.InvalidRelationalIndexBound => true,
                else => false,
            };
        }
        const ScanVisitor = if (@hasDecl(DB, "visitCypherDocuments")) @typeInfo(@TypeOf(DB.visitCypherDocuments)).@"fn".param_types[5].? else void;
        fn scanPlanned(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize, comptime index_only: bool, comptime probe: bool, visitor: ?ScanVisitor) anyerror!WhereResult(index_only) {
            if (comptime !@hasDecl(DB, "openRelationalReadSession") or !@hasDecl(DB, "cypherDocumentScanOptions")) return null;
            if (comptime !@hasDecl(DB.RelationalReadSession, "seekPrefix")) return null;
            const planning = for (filters) |filter| {
                if (filter.plan != null) break true;
            } else false;
            const self = cast(ptr);
            try checkpoint(ptr);
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const db = try self.selected(table);
            var cursor = if (encoded) |bytes| try std.json.parseFromSliceLeaky(PropertyCursor, temporary, bytes, .{}) else PropertyCursor{};
            if (cursor.primary) return if (index_only or probe) null else try propertyFallback(ptr, a, table, cursor, limit);
            for (filters) |filter| if (filter.op == .in and filter.value == .list and filter.value.list.len == 0) {
                if (index_only) return 0;
                return .{ .items = &.{} };
            };
            const authority = try self.readAuthorization(temporary, table);
            var opts = try self.scanOptions(temporary, db, table, .{ .execution_deadline_ns = authority.deadline_ns, .row_policy_principal_proof = authority.proof, .row_policy_database = authority.database, .include_documents = true, .limit = @intCast(if (visitor != null) @min(limit, 4096) else limit) });
            if (visitor != null and self.authority.project_stored != null) opts.filter_query_json = authority.filter_query_json;
            if (opts.relational_query == null) return if (index_only or probe) null else try propertyFallback(ptr, a, table, cursor, limit);
            var schema = acquireSchema(db) orelse return if (index_only or probe) null else try propertyFallback(ptr, a, table, cursor, limit);
            defer schema.release();
            const Query = @TypeOf(opts.relational_query.?);
            var fixed: std.ArrayList(Query.Condition) = .empty;
            var domains: std.ArrayList(Domain) = .empty;
            for (filters, 0..) |filter, ordinal| {
                const column = for (schema.tableSchema().relational_columns) |column| {
                    if (std.mem.eql(u8, column.name, filter.property)) break column;
                } else continue;
                if (filter.op == .in) {
                    if (filter.value != .list) continue;
                    const source = filter.value.list;
                    if (filter.plan) |proof| {
                        if (encoded != null or filter.domain == null or proof.length <= 3 or proof.length > 4096 or source.len != 3) return error.CypherInvalidBackendCursor;
                        for (source) |value| if (value == .string and value.string.len > proof.max_string_bytes) return error.CypherInvalidBackendCursor;
                    }
                    if (source.len > (if (filter.domain != null) @as(usize, 4096) else 256)) continue;
                    if (filter.domain == .integer_inexact_double and column.column_type == .number) continue;
                    var valid = true;
                    for (source) |value| {
                        if (value == .null) continue;
                        if (!filterCompatible(column.column_type, value) or (value == .number and !std.math.isFinite(value.number)) or (column.column_type == .number and value == .integer and (value.integer < -9007199254740992 or value.integer > 9007199254740992))) {
                            valid = false;
                            break;
                        }
                    }
                    if (!valid) continue; // An unusable hint does not discard other access paths.
                    var values = source;
                    if (filter.domain == null) {
                        var unique: std.ArrayList(v.Value) = .empty;
                        var seen: @import("value_work.zig").Index = .{};
                        var work = self.valueWork(temporary);
                        for (source) |value| {
                            if (value == .null) continue;
                            const hash = (try work.info(value)).hash;
                            var at = seen.head(hash);
                            while (at) |position| {
                                const link = seen.links.items[position];
                                if (try work.eql(unique.items[link.index], value)) break;
                                at = link.next;
                            }
                            if (at != null) continue;
                            try seen.insert(temporary, hash, unique.items.len);
                            try unique.append(temporary, value);
                        }
                        values = unique.items;
                    }
                    if (values.len == 0) {
                        if (index_only) return 0;
                        return .{ .items = &.{} };
                    }
                    try domains.append(temporary, .{ .filter = ordinal, .values = values, .certified = filter.domain != null, .total = if (filter.plan) |proof| proof.length else if (cursor.planned and cursor.domain_filter == ordinal) cursor.domain_length else values.len, .max_string_bytes = if (filter.plan) |proof| proof.max_string_bytes else null });
                    continue;
                }
                if (fixed.items.len == 256) continue;
                if (filter.op != .is_null and !filterCompatible(column.column_type, filter.value)) continue;
                if (filter.value == .number and !std.math.isFinite(filter.value.number)) continue;
                var operand = if (filter.op == .is_null) @as(std.json.Value, .null) else jsonOperand(filter.value);
                if (column.column_type == .number and filter.value == .integer and (filter.value.integer < -9007199254740992 or filter.value.integer > 9007199254740992)) {
                    if (filter.op != .eq) continue;
                    const number: f64 = @floatFromInt(filter.value.integer);
                    if (@as(i128, @intFromFloat(number)) != @as(i128, filter.value.integer)) continue;
                    // The equality is exact in binary64. Preserve those binary
                    // semantics rather than reinterpreting an integer token's
                    // shortest decimal spelling during SQL bound validation.
                    operand = .{ .float = number };
                }
                try fixed.append(temporary, .{ .column = filter.property, .op = switch (filter.op) {
                    .eq => .eq,
                    .lt => .lt,
                    .lte => .lte,
                    .gt => .gt,
                    .gte => .gte,
                    .is_null => .is_not_distinct,
                    .in => unreachable,
                }, .value = if (filter.op == .is_null) null else operand });
            }
            opts.relational_query.?.conditions = fixed.items;
            opts.relational_query.?.schema_version = cursor.schema_version orelse schema.version();
            if (index_only) {
                opts.fields = &.{};
                opts.include_all_fields = false;
                opts.relational_query.?.fields = &.{};
            }
            var plans: std.ArrayList(SeekPlan) = .empty;
            if (cursor.planned) {
                const domain = if (cursor.domain_filter) |ordinal| for (domains.items, 0..) |domain, i| {
                    if (domain.filter == ordinal) break @as(?usize, i);
                } else return error.CatalogGenerationChanged else @as(?usize, null);
                if (cursor.prefix_filters.len != cursor.prefix_lengths.len or cursor.prefix_filters.len > 3 or (cursor.prefix_filters.len != 0 and domain == null)) return error.CypherInvalidBackendCursor;
                const axes = try temporary.alloc(PrefixDomain, cursor.prefix_filters.len);
                var combinations: usize = if (domain) |d| domains.items[d].total else 1;
                for (axes, cursor.prefix_filters, cursor.prefix_lengths) |*axis, ordinal, length| {
                    const d = for (domains.items, 0..) |candidate, i| {
                        if (candidate.filter == ordinal) break i;
                    } else return error.CatalogGenerationChanged;
                    if (!domains.items[d].certified or domains.items[d].total != length or domains.items[d].values.len != length or length == 0 or combinations > 128 / length) return error.CypherInvalidBackendCursor;
                    combinations *= length;
                    axis.* = .{ .domain = d, .position = std.math.maxInt(usize) };
                }
                try plans.append(temporary, .{ .index = cursor.index orelse return error.CypherInvalidBackendCursor, .domain = domain, .prefix_domains = axes });
            } else if (domains.items.len == 0) {
                try plans.append(temporary, .{ .index = "" }); // Ordinary SQL auto-index selection.
            }
            // Domains are considered independently of their syntactic order.
            // A prefix may contain fixed equalities before its varying IN key.
            if (!cursor.planned and domains.items.len != 0) {
                var indexes = acquireIndexes(db);
                defer if (indexes) |*view| view.deinit();
                if (indexes) |view| for (domains.items, 0..) |domain, domain_id| {
                    var kept: usize = 0;
                    for (view.plan.boundIndexes()) |index| {
                        if (self.work) |work| try work.charge(work.ptr, 1);
                        var prefix: std.ArrayList(std.json.Value) = .empty;
                        var axes: std.ArrayList(PrefixDomain) = .empty;
                        var combinations = domain.total;
                        var found = false;
                        for (index.tuple.keys) |key| {
                            if (prefix.items.len == 32 or key.expression != null or key.fold_ascii) break;
                            const name = schema.tableSchema().relational_columns[key.ordinal].name;
                            const equality = for (fixed.items) |condition| {
                                if ((condition.op == .eq or condition.op == .is_not_distinct) and std.mem.eql(u8, condition.column, name)) break @as(?std.json.Value, condition.value orelse .null);
                            } else @as(?std.json.Value, null);
                            if (equality) |value| {
                                try prefix.append(temporary, value);
                                continue;
                            }
                            if (std.mem.eql(u8, name, filters[domain.filter].property)) {
                                try prefix.append(temporary, .null);
                                found = true;
                                break;
                            }
                            const other = for (domains.items, 0..) |candidate, i| {
                                if (candidate.certified and candidate.values.len == candidate.total and std.mem.eql(u8, name, filters[candidate.filter].property)) break @as(?usize, i);
                            } else @as(?usize, null);
                            if (other) |d| {
                                const length = domains.items[d].total;
                                if (!domain.certified or axes.items.len == 3 or length == 0 or combinations > 128 / length) break;
                                combinations *= length;
                                try axes.append(temporary, .{ .domain = d, .position = prefix.items.len });
                                try prefix.append(temporary, .null);
                                continue;
                            }
                            break;
                        }
                        if (!found) continue;
                        if (kept == 4 or plans.items.len == 32) break;
                        if (axes.items.len != 0) {
                            const ordinals = try temporary.alloc(usize, axes.items.len);
                            for (ordinals, axes.items) |*ordinal, axis| ordinal.* = domains.items[axis.domain].filter;
                            const pinned = try @import("sparse_seek.zig").retain(temporary, filters, domain.filter, ordinals);
                            var bytes = std.Io.Writer.Discarding.init(&.{});
                            try std.json.Stringify.value(.{ .filters = pinned, .table = table, .read_fields = self.read_fields }, .{}, &bytes.writer);
                            // Composite domains stay whole (at most 128 tuples).
                            // Leave room for escaped row cursors and envelopes.
                            if (bytes.fullCount() > 256 << 10) continue;
                        }
                        try plans.append(temporary, .{ .index = index.name, .domain = domain_id, .prefix = prefix.items, .prefix_domains = axes.items });
                        kept += 1;
                    }
                };
                if (fixed.items.len != 0) {
                    var base_opts = opts;
                    base_opts.fields = &.{};
                    base_opts.include_all_fields = false;
                    base_opts.relational_query.?.fields = &.{};
                    base_opts.relational_query.?.auto_index = true;
                    const base = db.openRelationalReadSession(temporary, "", "", base_opts) catch |err| blk: {
                        if (unusableSeek(err)) break :blk null;
                        return err;
                    };
                    if (base) |session| {
                        defer session.deinit();
                        if (session.reader.index) |index| try plans.append(temporary, .{ .index = try temporary.dupe(u8, index.name) });
                    }
                }
            }
            if (plans.items.len == 0) return if (index_only or probe) null else try propertyFallback(ptr, a, table, cursor, limit);
            // Recreate varying prefixes on continuation from the pinned index,
            // rather than retaining a catalog/reader in the continuation token.
            if (cursor.planned and plans.items[0].domain != null) {
                var indexes = acquireIndexes(db) orelse return error.CatalogGenerationChanged;
                defer indexes.deinit();
                const index = for (indexes.plan.boundIndexes()) |index| {
                    if (std.mem.eql(u8, index.name, plans.items[0].index)) break index;
                } else return error.CatalogGenerationChanged;
                var prefix: std.ArrayList(std.json.Value) = .empty;
                const domain = domains.items[plans.items[0].domain.?];
                for (index.tuple.keys) |key| {
                    if (key.expression != null or key.fold_ascii or prefix.items.len == 32) return error.CatalogGenerationChanged;
                    const name = schema.tableSchema().relational_columns[key.ordinal].name;
                    if (std.mem.eql(u8, name, filters[domain.filter].property)) {
                        try prefix.append(temporary, .null);
                        break;
                    }
                    var varying = false;
                    for (plans.items[0].prefix_domains) |*axis| if (std.mem.eql(u8, name, filters[domains.items[axis.domain].filter].property)) {
                        axis.position = prefix.items.len;
                        varying = true;
                        break;
                    };
                    if (varying) {
                        try prefix.append(temporary, .null);
                        continue;
                    }
                    const value = for (fixed.items) |condition| {
                        if ((condition.op == .eq or condition.op == .is_not_distinct) and std.mem.eql(u8, condition.column, name)) break condition.value orelse .null;
                    } else return error.CatalogGenerationChanged;
                    try prefix.append(temporary, value);
                }
                for (plans.items[0].prefix_domains) |axis| if (axis.position == std.math.maxInt(usize)) return error.CatalogGenerationChanged;
                plans.items[0].prefix = prefix.items;
            }
            if (!cursor.planned and plans.items.len > 1) candidates: for (plans.items) |*plan| {
                var sample_opts = opts;
                sample_opts.fields = &.{};
                sample_opts.include_all_fields = false;
                sample_opts.relational_query.?.fields = &.{};
                sample_opts.relational_query.?.index = plan.index;
                sample_opts.relational_query.?.auto_index = plan.domain == null;
                var sample_arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
                defer sample_arena.deinit();
                const sample = db.openRelationalReadSession(sample_arena.allocator(), "", "", sample_opts) catch |err| {
                    if (unusableSeek(err)) {
                        plan.usable = false;
                        continue :candidates;
                    }
                    return err;
                };
                defer sample.deinit();
                sample.reader.now_ns = self.ttl_now_ns;
                const branches = branchCount(plan.*, domains.items, false);
                const samples = @min(branches, 3);
                var visits: usize = 0;
                var complete = true;
                for (0..samples) |i| {
                    if (self.work) |work| try work.charge(work.ptr, 8);
                    if (plan.domain != null) {
                        const available = branchCount(plan.*, domains.items, true);
                        const ordinal = if (samples == 1) 0 else i * (available - 1) / (samples - 1);
                        branchPrefix(plan, domains.items, ordinal, 0, true);
                        sample.seekPrefix(plan.prefix, null) catch |err| {
                            if (unusableSeek(err)) {
                                plan.usable = false;
                                continue :candidates;
                            }
                            return err;
                        };
                    }
                    try sample.checkpoint();
                    var page = try sample.reader.nextPage(sample_arena.allocator(), null, .{ .rows = 8, .records = 8, .output_bytes = 1 << 20 });
                    defer page.deinit();
                    try sample.checkpoint();
                    visits += page.records_examined;
                    complete = complete and !page.more;
                }
                plan.complete = complete;
                plan.cost = branches + (visits * branches + samples - 1) / samples;
            };
            // Invalid/oversized domain bounds decline before the first emitted
            // row. Other ready access paths remain candidates, including base.
            selection: while (true) {
                var best: ?usize = null;
                for (plans.items, 0..) |plan, i| if (plan.usable) {
                    if (best == null or (plan.complete and !plans.items[best.?].complete) or (plan.complete == plans.items[best.?].complete and (plan.cost < plans.items[best.?].cost or (plan.cost == plans.items[best.?].cost and std.mem.lessThan(u8, plan.index, plans.items[best.?].index))))) best = i;
                };
                const chosen = best orelse return if (index_only or probe) null else try propertyFallback(ptr, a, table, cursor, limit);
                const plan = &plans.items[chosen];
                var execution_opts = opts;
                execution_opts.relational_query.?.index = if (plan.index.len != 0) plan.index else null;
                execution_opts.relational_query.?.auto_index = plan.domain == null;
                execution_opts.relational_query.?.after = if (plan.domain == null) cursor.after else null;
                const session = db.openRelationalReadSession(temporary, "", "", execution_opts) catch |err| {
                    if (unusableSeek(err) and !cursor.planned) {
                        plan.usable = false;
                        continue :selection;
                    }
                    return err;
                };
                defer session.deinit();
                if (session.reader.index == null) return if (index_only or probe) null else try propertyFallback(ptr, a, table, cursor, limit);
                session.reader.now_ns = self.ttl_now_ns;
                const domain: ?Domain = if (plan.domain) |d| domains.items[d] else null;
                if (!cursor.planned) if (domain) |d| {
                    for (0..branchCount(plan.*, domains.items, true)) |ordinal| {
                        if (self.work) |work| try work.charge(work.ptr, 1);
                        branchPrefix(plan, domains.items, ordinal, 0, true);
                        session.seekPrefix(plan.prefix, null) catch |err| {
                            if (unusableSeek(err)) {
                                plan.usable = false;
                                continue :selection;
                            }
                            return err;
                        };
                    }
                    if (d.max_string_bytes) |maximum| if (maximum != 0) {
                        // NUL bytes maximize tuple escaping for any string of
                        // this byte length, independently of index direction.
                        // Fixed prefix components use their actual encoding.
                        if (maximum > 1 << 20) {
                            plan.usable = false;
                            continue :selection;
                        }
                        const worst = try temporary.alloc(u8, maximum);
                        @memset(worst, 0);
                        plan.prefix[plan.prefix.len - 1] = .{ .string = worst };
                        session.seekPrefix(plan.prefix, null) catch |err| {
                            if (unusableSeek(err)) {
                                plan.usable = false;
                                continue :selection;
                            }
                            return err;
                        };
                    };
                    cursor.domain_length = d.total;
                };
                const prefix_filters = try temporary.alloc(usize, plan.prefix_domains.len);
                const prefix_lengths = try temporary.alloc(usize, plan.prefix_domains.len);
                for (prefix_filters, prefix_lengths, plan.prefix_domains) |*ordinal, *length, axis| {
                    ordinal.* = domains.items[axis.domain].filter;
                    length.* = domains.items[axis.domain].total;
                }
                if (planning) {
                    if (index_only or probe) return null;
                    try session.checkpoint();
                    try checkpoint(ptr);
                    const next = try std.json.Stringify.valueAlloc(a, PropertyCursor{ .planned = true, .index = session.reader.index.?.name, .schema_version = session.reader.active.version(), .domain_filter = if (domain) |d| d.filter else null, .domain_length = if (domain) |d| d.total else 0, .prefix_filters = prefix_filters, .prefix_lengths = prefix_lengths }, .{});
                    return .{ .items = &.{}, .examined = 0, .next = next, .seek_window = .{ .filter = if (domain) |d| d.filter else null, .prefix_filters = try a.dupe(usize, prefix_filters) } };
                }
                if (comptime !index_only and @hasDecl(DB, "RelationalReadSession") and @hasDecl(DB.RelationalReadSession, "visitCypherRows")) if (visitor) |sink| {
                    // Planning has certified disjoint branches before any row
                    // is observed. The fold charges physical progress and owns
                    // the global anchor ceiling across every branch.
                    if (encoded != null or planning) return null;
                    if (domain) |d| if (d.values.len != d.total) return null;
                    const total = branchCount(plan.*, domains.items, false);
                    for (0..total) |branch| {
                        if (self.work) |work| try work.charge(work.ptr, 1);
                        if (domain != null) {
                            branchPrefix(plan, domains.items, branch, 0, false);
                            try session.seekPrefix(plan.prefix, null);
                        }
                        try session.visitCypherRows(a, limit, sink);
                    }
                    try checkpoint(ptr);
                    return .{ .items = &.{} };
                };
                var nodes: std.ArrayList(*const v.Node) = .empty;
                var examined: usize = 0;
                var count: usize = 0;
                var branch = cursor.branch;
                const total = branchCount(plan.*, domains.items, false);
                if (domain != null and cursor.domain_length != domains.items[plan.domain.?].total and cursor.planned) return error.CypherInvalidBackendCursor;
                if (total == 0 or branch >= total) return error.CypherInvalidBackendCursor;
                const partitioned = plan.prefix_domains.len == 0 and domain != null and domain.?.certified and domain.?.values.len != domain.?.total;
                const offset = if (partitioned) branch else @as(usize, 0);
                if (partitioned and (domain.?.values.len == 0 or domain.?.values.len > @min(@import("sparse_seek.zig").width, total - offset))) return error.CypherInvalidBackendCursor;
                const end = if (partitioned) @min(total, offset + domain.?.values.len) else @min(total, branch + @import("sparse_seek.zig").width);
                while (true) {
                    try session.checkpoint();
                    if (self.work) |work| try work.charge(work.ptr, 1);
                    if (domain != null) {
                        if (partitioned and branch - offset >= domain.?.values.len) return error.CypherInvalidBackendCursor;
                        branchPrefix(plan, domains.items, branch, offset, false);
                        try session.seekPrefix(plan.prefix, if (branch == cursor.branch) cursor.after else null);
                    }
                    var page_arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
                    defer page_arena.deinit();
                    var page = try session.reader.nextPage(page_arena.allocator(), null, .{ .rows = limit - count, .records = limit - examined, .output_bytes = if (probe) 1 << 20 else 16 << 20 });
                    defer page.deinit();
                    examined += page.records_examined;
                    count += page.rows.len;
                    if (!index_only) for (page.rows) |row| {
                        if (try self.node(a, table, row.key, row.json)) |item| if (item.labels.len != 0) {
                            try nodes.append(a, item);
                        };
                    };
                    try session.checkpoint();
                    try checkpoint(ptr);
                    const more = branch + 1 < total;
                    if (!page.more and !more) {
                        if (index_only) return count;
                        return .{ .items = nodes.items, .examined = examined };
                    }
                    if (page.more or examined >= limit or count >= limit or branch + 1 == end) {
                        if (index_only) return null;
                        const next_branch = if (page.more) branch else branch + 1;
                        const next = try std.json.Stringify.valueAlloc(a, PropertyCursor{ .planned = true, .index = session.reader.index.?.name, .schema_version = session.reader.active.version(), .branch = next_branch, .after = if (page.more) page.next_cursor else null, .domain_filter = if (domain) |d| d.filter else null, .domain_length = if (domain) |d| d.total else 0, .prefix_filters = prefix_filters, .prefix_lengths = prefix_lengths }, .{});
                        return .{ .items = nodes.items, .examined = examined, .next = next, .seek_window = if (domain) |d| if (d.certified) b.SeekWindow{ .filter = d.filter, .offset = next_branch, .prefix_filters = try a.dupe(usize, prefix_filters) } else null else b.SeekWindow{ .filter = null } };
                    }
                    branch += 1;
                }
            }
        }
        fn getMany(ptr: *anyopaque, a: A, identities: []const v.Identity) ![]const ?*const v.Node {
            if (identities.len > 128) return error.CypherOwnerBudgetExceeded;
            const self = cast(ptr);
            const nodes = try a.alloc(?*const v.Node, identities.len);
            @memset(nodes, null);
            if (comptime !@hasDecl(DB, "getCypherDocuments")) {
                for (identities, nodes) |identity, *out| {
                    if (self.work) |work| try work.charge(work.ptr, 1);
                    out.* = try get(ptr, a, identity);
                }
                return nodes;
            }
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            var served: [128]bool = @splat(false);
            for (identities, 0..) |identity, i| {
                if (self.work) |work| try work.charge(work.ptr, 1);
                if (comptime @hasDecl(DB, "cypherDocumentKeyAllowed")) if (!DB.cypherDocumentKeyAllowed(identity.key)) {
                    served[i] = true;
                    continue;
                };
                _ = try self.selected(identity.table);
            }
            for (self.tables) |table| {
                var keys: [128][]const u8 = undefined;
                var positions: [128]usize = undefined;
                var count: usize = 0;
                for (identities, 0..) |identity, i| if (!served[i] and std.mem.eql(u8, identity.table, table.name)) {
                    keys[count] = identity.key;
                    positions[count] = i;
                    count += 1;
                    served[i] = true;
                };
                if (count == 0) continue;
                try checkpoint(ptr);
                const authorization = try self.readAuthorization(temporary, table.name);
                const opts = try self.scanOptions(temporary, table.db, table.name, .{ .include_documents = true, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .execution_deadline_ns = authorization.deadline_ns });
                const documents = try table.db.getCypherDocuments(temporary, keys[0..count], opts, self.ttl_now_ns);
                defer {
                    for (documents) |*document| if (document.*) |*value| value.deinit(temporary);
                    temporary.free(documents);
                }
                if (documents.len != count) return error.CypherInvalidBackendPage;
                for (documents, positions[0..count]) |document, position| {
                    if (document) |value| nodes[position] = try self.node(a, table.name, identities[position].key, value.json);
                }
                try checkpoint(ptr);
            }
            return nodes;
        }
        fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
            const self = cast(ptr);
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            if (comptime @hasDecl(DB, "cypherDocumentKeyAllowed")) {
                if (!DB.cypherDocumentKeyAllowed(identity.key)) return null;
            }
            try checkpoint(ptr);
            const db = try self.selected(identity.table);
            const authorization = try self.readAuthorization(temporary, identity.table);
            const result = if (comptime @hasDecl(DB, "cypherDocumentScanOptions")) projected: {
                if (self.read_fields != null or self.nodeDemand(identity.table) != null) {
                    const opts = try self.scanOptions(temporary, db, identity.table, .{ .inclusive_from = true, .include_documents = true, .limit = 1, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .execution_deadline_ns = authorization.deadline_ns });
                    if (opts.relational_query == null) {
                        if (opts.include_all_fields) break :projected try self.getFull(a, temporary, db, identity, authorization);
                        var document = (try db.getDocument(temporary, identity.key, .{ .fields = opts.fields, .cypher_presence_fields = opts.cypher_presence_fields, .cypher_projection_json = opts.cypher_projection_json, .include_all_fields = false, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .execution_deadline_ns = authorization.deadline_ns })) orelse break :projected null;
                        defer document.deinit(temporary);
                        break :projected try self.node(a, identity.table, identity.key, document.json);
                    }
                    var page = try db.scan(temporary, identity.key, identity.key, opts);
                    defer page.deinit(temporary);
                    break :projected if (page.documents.len == 0) null else try self.node(a, identity.table, identity.key, page.documents[0].json);
                }
                break :projected try self.getFull(a, temporary, db, identity, authorization);
            } else try self.getFull(a, temporary, db, identity, authorization);
            try checkpoint(ptr);
            return result;
        }
        fn getFull(self: *Self, a: A, temporary: A, db: *DB, identity: v.Identity, authorization: ReadAuthorization) !?*const v.Node {
            var document = (try db.getDocument(temporary, identity.key, .{ .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .execution_deadline_ns = authorization.deadline_ns })) orelse return null;
            defer document.deinit(temporary);
            return self.node(a, identity.table, identity.key, document.json);
        }
        const Resume = struct { incoming: bool = false, cursor: ?EdgeScanCursor = null };
        fn readAuthorization(self: *Self, a: A, table: []const u8) !ReadAuthorization {
            return if (self.authority.read_authorization) |resolve| try resolve(self.authority.ptr, a, table) else .{};
        }
        fn expand(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identity: v.Identity, direction: root.ast.Direction, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
            return expandPair(ptr, a, mapping, identity, null, direction, encoded, limit, 4096);
        }
        fn expandBetween(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identity: v.Identity, peer: v.Identity, direction: root.ast.Direction, _: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize, max_examined: usize) !b.Page(*const v.Relationship) {
            return expandPair(ptr, a, mapping, identity, peer, direction, encoded, limit, max_examined);
        }
        fn expandPair(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, identity: v.Identity, peer: ?v.Identity, direction: root.ast.Direction, encoded: ?[]const u8, limit: usize, max_examined: usize) !b.Page(*const v.Relationship) {
            const self = cast(ptr);
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            try checkpoint(ptr);
            const graph = self.graph.?;
            const source_table = graph.label(mapping.source_label).?.table;
            const target_table = graph.label(mapping.target_label).?.table;
            const relationship_demand: root.read_fields.Entity = if (self.read_demand) |demand| demand.relationship(source_table, mapping.index, mapping.stored_type) else .{};
            // Incoming adjacency physically belongs to the declared source
            // table, even when its target key belongs to another table.
            var position: Resume = if (encoded) |json| try std.json.parseFromSliceLeaky(Resume, a, json, .{}) else .{ .incoming = direction == .in or !std.mem.eql(u8, identity.table, source_table) };
            const incoming_possible = direction != .out and std.mem.eql(u8, identity.table, target_table);
            const outgoing_possible = direction != .in and std.mem.eql(u8, identity.table, source_table);
            if ((position.incoming and !incoming_possible) or (!position.incoming and !outgoing_possible)) return .{ .items = &.{} };
            const db = try self.selected(source_table);
            const authorization = try self.readAuthorization(temporary, source_table);
            const projects_metadata = if (@hasDecl(DB, "cypher_metadata_projection")) DB.cypher_metadata_projection else false;
            const projection_json = if (projects_metadata and self.authority.prepare_metadata != null and self.read_demand != null and relationship_demand.fields != null)
                try std.json.Stringify.valueAlloc(temporary, .{
                    .fields = relationship_demand.fields.?,
                    .presence = relationship_demand.presence,
                    .projection = relationship_demand.projection,
                    .source = source_table,
                    .target = target_table,
                    .source_storage = self.storageTableName(source_table),
                    .target_storage = self.storageTableName(target_table),
                    .skip_self_loops = position.incoming and outgoing_possible,
                }, .{})
            else
                "";
            const PageFunction = if (@hasDecl(DB, "graphEdgesForCypherPageAuthorizedAt")) @TypeOf(DB.graphEdgesForCypherPageAuthorizedAt) else @TypeOf(DB.graphEdgesForCypherPageAt);
            const PageLimits = @typeInfo(PageFunction).@"fn".param_types[7] orelse struct {
                max_edges: usize,
                max_owned_bytes: usize,
                max_scanned_rows: usize,
                peer_key: ?[]const u8,
                metadata_projection_json: []const u8 = "",
            };
            var page_limits: PageLimits = .{ .max_edges = limit, .max_owned_bytes = 4 << 20, .max_scanned_rows = @min(4096, @max(1, max_examined)), .peer_key = if (peer) |value| value.key else null };
            if (comptime @hasField(PageLimits, "metadata_projection_json")) page_limits.metadata_projection_json = projection_json;
            var page = captured: {
                if (comptime @hasDecl(DB, "graphEdgesForCypherCapturePageAuthorizedAt")) {
                    for (self.tables) |table| if (table.db == db and std.mem.eql(u8, table.name, source_table)) {
                        if (table.capture_fence) |fence| break :captured try db.graphEdgesForCypherCapturePageAuthorizedAt(fence, temporary, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, page_limits, self.ttl_now_ns, authorization.proof, authorization.database);
                    };
                }
                break :captured if (comptime @hasDecl(DB, "graphEdgesForCypherPageAuthorizedAt"))
                    try db.graphEdgesForCypherPageAuthorizedAt(temporary, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, page_limits, self.ttl_now_ns, authorization.proof, authorization.database)
                else blk: {
                    if (authorization.proof.len != 0) return error.CypherRowPolicyUnsupported;
                    break :blk try db.graphEdgesForCypherPageAt(temporary, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, page_limits, self.ttl_now_ns);
                };
            };
            defer page.deinit(temporary);
            var result: std.ArrayList(*const v.Relationship) = .empty;
            for (page.edges) |edge| {
                const projected_metadata = if (self.read_demand != null) if (relationship_demand.fields != null and self.authority.prepare_metadata != null)
                    try self.authority.prepare_metadata.?(self.authority.ptr, a, if (edge.metadata.len == 0) "{}" else edge.metadata)
                else
                    null else null;
                const metadata = if (projected_metadata) |projected| projected.headers else if (edge.metadata.len == 0) std.json.Value{ .object = .empty } else try std.json.parseFromSliceLeaky(std.json.Value, a, edge.metadata, .{ .parse_numbers = false });
                if (metadata != .object) return error.CypherInvalidRelationshipMetadata;
                const actual_source = metadata.object.get("source_table") orelse .null;
                const actual_target = metadata.object.get("target_table") orelse .null;
                if (actual_source != .null and (actual_source != .string or !self.matchesStorageTable(source_table, actual_source.string))) continue;
                if (actual_target != .null and (actual_target != .string or !self.matchesStorageTable(target_table, actual_target.string))) continue;
                const source: v.Identity = .{ .table = source_table, .key = edge.source };
                const target: v.Identity = .{ .table = target_table, .key = edge.target };
                if (!(if (position.incoming) target else source).eql(identity)) continue;
                if (peer) |value| if (!(if (position.incoming) source else target).eql(value)) continue;
                // Native in/out scans both contain a self-loop. Return it once
                // for an undirected pattern, preserving parallel IDs.
                if (position.incoming and outgoing_possible and source.eql(target)) continue;
                const relationship = try a.create(v.Relationship);
                var properties: std.ArrayList(v.Entry) = .empty;
                const encoded_properties = if (projected_metadata) |projected| try projected.project(projected.context, a, edge.weight, relationship_demand) else try root.wire.relationshipProperties(a, metadata);
                if (encoded_properties) |decoded| {
                    try properties.appendSlice(a, decoded);
                } else {
                    var entries = metadata.object.iterator();
                    while (entries.next()) |entry| {
                        const name = entry.key_ptr.*;
                        if (!std.mem.eql(u8, name, "weight") and !std.mem.eql(u8, name, "source_table") and !std.mem.eql(u8, name, "target_table"))
                            try properties.append(a, .{ .name = try a.dupe(u8, name), .value = try v.Value.fromJson(a, entry.value_ptr.*) });
                    }
                    try properties.append(a, .{ .name = "weight", .value = .{ .number = edge.weight } });
                }
                const field_derived = if (comptime @hasField(@TypeOf(edge), "field_derived")) edge.field_derived else edge.edge_id.len == 0 and if (comptime @hasDecl(DB, "cypherFieldDerivedEdgeType")) db.cypherFieldDerivedEdgeType(mapping.index, mapping.stored_type) else false;
                const artifact_derived = if (comptime @hasField(@TypeOf(edge), "artifact_derived")) edge.artifact_derived else false;
                const producer = if (edge.owner_document.len != 0) edge.owner_document else if (field_derived or artifact_derived) edge.source else "";
                const key = if (field_derived) try std.json.Stringify.valueAlloc(a, .{ edge.source, edge.target, edge.edge_type, edge.edge_id, producer, "field" }, .{}) else try std.json.Stringify.valueAlloc(a, .{ edge.source, edge.target, edge.edge_type, edge.edge_id, producer }, .{});
                relationship.* = .{ .native = .{ .weight = edge.weight, .created_at = if (comptime @hasField(@TypeOf(edge), "created_at")) edge.created_at else 0, .updated_at = if (comptime @hasField(@TypeOf(edge), "updated_at")) edge.updated_at else 0 }, .identity = .{ .table = try a.dupe(u8, source_table), .key = key }, .index = mapping.index, .type = mapping.name, .source = .{ .table = source_table, .key = try a.dupe(u8, source.key) }, .target = .{ .table = target_table, .key = try a.dupe(u8, target.key) }, .properties = try v.entityProperties(a, try properties.toOwnedSlice(a)), .ownership = if (artifact_derived) .artifact else if (field_derived) .field else if (producer.len == 0) .explicit else .document, .producer = if (producer.len == 0) null else .{ .table = source_table, .key = try a.dupe(u8, producer) } };
                if (try self.authority.project_relationship(self.authority.ptr, a, relationship)) |authorized| try result.append(a, authorized);
            }
            const next: ?[]const u8 = if (page.next_cursor) |cursor| try std.json.Stringify.valueAlloc(a, Resume{ .incoming = position.incoming, .cursor = cursor }, .{}) else if (!position.incoming and incoming_possible) blk: {
                position = .{ .incoming = true };
                break :blk try std.json.Stringify.valueAlloc(a, position, .{});
            } else null;
            try checkpoint(ptr);
            return .{ .items = try result.toOwnedSlice(a), .next = next, .examined = page.scanned_rows };
        }
        comptime {
            _ = GraphIndex;
        }
    };
}

test "ordered membership domains intersect repeated scalar admissions" {
    const Counter = struct {
        units: usize = 0,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.units += amount;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var counter: Counter = .{};
    var work: @import("value_work.zig").Work = .{ .a = a, .ptr = &counter, .charge = Counter.charge };
    const filters = [_]b.PropertyFilter{
        .{ .property = "score", .op = .in, .value = .{ .list = &.{ .{ .integer = 2 }, .{ .integer = 0 }, .{ .integer = 0 }, .null } } },
        .{ .property = "score", .op = .in, .value = .{ .list = &.{ .{ .integer = 0 }, .{ .integer = 1 }, .{ .integer = 2 } } } },
        .{ .property = "score", .op = .in, .value = .{ .list = &.{ .{ .integer = 0 }, .{ .integer = 2 }, .null } } },
    };
    const domain = (try orderedMembershipDomain(a, &work, &filters, 0, .integer)).?;
    try std.testing.expectEqual(@as(usize, 2), domain.len);
    try std.testing.expectEqual(@as(i64, 2), domain[0].integer);
    try std.testing.expectEqual(@as(i64, 0), domain[1].integer);
    var changed = filters;
    changed[2].value = .{ .list = &.{.{ .integer = 1 }} };
    try std.testing.expectEqual(@as(usize, 0), (try orderedMembershipDomain(a, &work, &changed, 0, .integer)).?.len);
    changed[2] = filters[2];
    changed[2].property = "other";
    try std.testing.expect(try orderedMembershipDomain(a, &work, &changed, 0, .integer) == null);
    changed[2].property = "score";
    changed[2].value = .{ .list = &.{.{ .number = 2 }} };
    try std.testing.expect(try orderedMembershipDomain(a, &work, &changed, 0, .integer) == null);
    const strings = [_]b.PropertyFilter{
        .{ .property = "id", .op = .in, .value = .{ .list = &.{ .{ .string = "β" }, .{ .string = "a" } } } },
        .{ .property = "id", .op = .in, .value = .{ .list = &.{ .{ .string = "a" }, .{ .string = "β" } } } },
    };
    const text_domain = (try orderedMembershipDomain(a, &work, &strings, 0, .string)).?;
    try std.testing.expectEqualStrings("β", text_domain[0].string);
    try std.testing.expectEqualStrings("a", text_domain[1].string);
    try std.testing.expect(counter.units < 256);
}
