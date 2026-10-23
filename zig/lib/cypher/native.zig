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
        pub const ReadAuthorization = struct { proof: []const u8 = "", database: []const u8 = "", deadline_ns: ?u64 = null };
        /// Mandatory hooks hold graph/table authority, refresh revocation at
        /// checkpoints, and apply field projection before any value is bound.
        pub const Authority = struct {
            ptr: *anyopaque,
            bind: *const fn (*anyopaque, root.catalog.Graph) anyerror!void,
            checkpoint: *const fn (*anyopaque) anyerror!void,
            project: *const fn (*anyopaque, A, *const v.Node) anyerror!?*const v.Node,
            project_relationship: *const fn (*anyopaque, A, *const v.Relationship) anyerror!?*const v.Relationship,
            search: ?*const fn (*anyopaque, A, []const u8, b.SearchInput) anyerror!b.SearchPage = null,
            read_authorization: ?*const fn (*anyopaque, A, []const u8) anyerror!ReadAuthorization = null,
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

        pub fn backend(self: *Self) b.Backend {
            return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .scan_where = scanWhere, .scan_ordered = scanOrdered, .estimate_where = estimateWhere, .get = get, .expand = expand, .expand_between = expandBetween, .search = search, .set_work = setWork, .set_read_fields = setReadFields } };
        }
        fn setReadFields(ptr: *anyopaque, fields: ?[]const []const u8) void {
            cast(ptr).read_fields = fields;
        }
        const ScanOptions = if (@hasDecl(DB, "cypherDocumentScanOptions")) @typeInfo(@typeInfo(@TypeOf(DB.cypherDocumentScanOptions)).@"fn".return_type.?).error_union.payload else void;
        fn scanOptions(self: *Self, a: A, db: *DB, input: ScanOptions) !ScanOptions {
            var opts = try db.cypherDocumentScanOptions(a, input);
            if (self.read_fields) |required| if (opts.relational_query) |*query| {
                var fields: std.ArrayList([]const u8) = .empty;
                for (query.fields) |field| for (required) |name| {
                    if (std.mem.eql(u8, field, name)) {
                        try fields.append(a, field);
                        break;
                    }
                };
                query.fields = fields.items;
            } else if (self.authority.storage_projection) {
                var fields: std.StringHashMapUnmanaged(void) = .empty;
                for (required) |field| try fields.put(a, field, {});
                for (self.graph.?.labels) |label| for (label.predicate) |entry| try fields.put(a, entry.name, {});
                // Native document selectors interpret path syntax. Preserve
                // literal Cypher property names by falling back to full reads
                // whenever a name could be interpreted as a selector.
                var keys = fields.keyIterator();
                while (keys.next()) |field| for (field.*) |byte| {
                    if (!std.ascii.isAlphanumeric(byte) and byte != '_') return opts;
                };
                const names = try a.alloc([]const u8, fields.count());
                var iterator = fields.keyIterator();
                for (names) |*name| name.* = iterator.next().?.*;
                opts.fields = names;
                opts.include_all_fields = false;
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
                if ((try self.selected(table)).core.graphIndex(relationship.index) == null) return error.IndexNotFound;
                if (relationship.identity) |expected| {
                    const actual = (try (try self.selected(table)).cypherGraphIndexIdentity(a, relationship.index)) orelse return error.IndexNotFound;
                    if (expected.incarnation != actual.incarnation or expected.config_hash != actual.config_hash) return error.CatalogGenerationChanged;
                }
            }
            self.graph = graph;
        }
        pub fn node(self: *Self, a: A, table: []const u8, key: []const u8, json: []const u8) !?*const v.Node {
            const fields = self.read_fields orelse return self.fullNode(a, table, key, json);
            // Membership and policies inspect complete documents. Decode them
            // in reclaimable scratch, then retain only the authorized projection.
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const authorized = (try self.fullNode(temporary, table, key, json)) orelse return null;
            var projected = authorized.*;
            var properties: std.ArrayList(v.Entry) = .empty;
            for (authorized.properties) |entry| for (fields) |field| {
                if (std.mem.eql(u8, entry.name, field)) {
                    try properties.append(temporary, entry);
                    break;
                }
            };
            projected.properties = properties.items;
            return (try (v.Value{ .node = &projected }).clone(a)).node;
        }
        fn fullNode(self: *Self, a: A, table: []const u8, key: []const u8, json: []const u8) !?*const v.Node {
            const value = try v.Value.fromJson(a, try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }));
            if (value != .map) return error.CypherInvalidDocument;
            const result = try a.create(v.Node);
            result.* = .{ .identity = .{ .table = try a.dupe(u8, table), .key = try a.dupe(u8, key) }, .properties = try v.entityProperties(a, value.map) };
            var membership = self.valueWork(a);
            var labels: std.ArrayList([]const u8) = .empty;
            for (self.graph.?.labels) |label| if (try self.graph.?.admits(result, label.name, &membership)) try labels.append(a, label.name);
            result.labels = try labels.toOwnedSlice(a);
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
                try db.scan(temporary, cursor orelse "", "", try self.scanOptions(temporary, db, .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .inclusive_from = cursor == null, .include_documents = true, .limit = std.math.cast(u32, limit) orelse return error.InvalidArgument }))
            else
                try db.scan(temporary, cursor orelse "", "", .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .inclusive_from = cursor == null, .include_documents = true, .limit = std.math.cast(u32, limit) orelse return error.InvalidArgument });
            defer found.deinit(temporary);
            var nodes: std.ArrayList(*const v.Node) = .empty;
            for (found.documents) |document| if (try self.node(a, table, document.id, document.json)) |authorized| if (authorized.labels.len != 0) try nodes.append(a, authorized);
            const next = if (found.documents.len == limit) try a.dupe(u8, found.documents[found.documents.len - 1].id) else null;
            try checkpoint(ptr);
            return .{ .items = try nodes.toOwnedSlice(a), .next = next, .examined = found.documents.len, .unfiltered = true };
        }
        const PropertyCursor = struct { branch: usize = 0, index: ?[]const u8 = null, after: ?[]const u8 = null, key: ?[]const u8 = null, schema_version: ?u32 = null, primary: bool = false };
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
        fn filterStrength(self: *Self, filters: []const b.PropertyFilter, name: []const u8, column_type: anytype) !u8 {
            var strength: u8 = 0;
            for (filters) |filter| {
                if (self.work) |work| try work.charge(work.ptr, 1);
                if (!std.mem.eql(u8, filter.property, name) or !filterCompatible(column_type, filter.value)) continue;
                strength = @max(strength, if (filter.op == .eq) @as(u8, 4) else if (filter.op != .in) @as(u8, 2) else 0);
            }
            return strength;
        }
        const OrderedCursor = struct { coarse: bool = false, index: []const u8, after: ?[]const u8 = null, schema_version: u32 };
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
            // Multiple independent IN domains retain the ordinary native
            // planner until their Cartesian product can be costed safely.
            for (filters[chosen + 1 ..]) |filter| if (filter.op == .in) return null;
            const operand = filters[chosen].value;
            if (operand != .list or operand.list.len > 256) return null;
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const db = try self.selected(table);
            var schema = db.core.acquireSchemaView() orelse return null;
            defer schema.release();
            const column_type = for (schema.tableSchema().relational_columns) |column| {
                if (std.mem.eql(u8, column.name, filters[chosen].property)) break column.column_type;
            } else return null;
            // Exact scalar domains keep partitions disjoint after storage
            // conversion. Mixed and floating operands remain runtime residuals.
            switch (column_type) {
                .integer, .string, .boolean => {},
                else => return null,
            }
            var unique: std.ArrayList(v.Value) = .empty;
            var distinct: @import("value_work.zig").Index = .{};
            var work = self.valueWork(temporary);
            for (operand.list) |value| {
                if (value == .null) continue;
                if (!filterCompatible(column_type, value)) return null;
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
                    try distinct.insert(temporary, hash, unique.items.len);
                    try unique.append(temporary, value);
                }
            }
            // Per-item continuations retain every partition position for
            // nested RPC paging. Admit only a small fanout: wider domains
            // keep sparse seeks/top-k instead of multiplying cursor work by
            // the output cardinality and exhausting the default state budget.
            if (unique.items.len == 0 or unique.items.len > 8) return null;
            // Preserve the sparse-seek plan when the global ceiling cannot
            // cover even the merge's required owner heads and lookahead.
            if (encoded == null and limit != 0 and max_examined < limit +| (unique.items.len - 1)) return null;
            const partitions = try temporary.alloc(OrderedPartition, unique.items.len);
            const targets = try temporary.alloc(root.distributed.Target, unique.items.len);
            const snapshots = try temporary.alloc(root.distributed.Snapshot, unique.items.len);
            for (partitions, targets, snapshots, unique.items, 0..) |*partition, *target, *snapshot, value, index| {
                const conditions = try temporary.dupe(b.PropertyFilter, filters);
                conditions[chosen].op = .eq;
                conditions[chosen].value = value;
                std.mem.swap(b.PropertyFilter, &conditions[0], &conditions[chosen]);
                partition.* = .{ .parent = self, .filters = conditions };
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
        fn orderedIndexPrefix(self: *Self, schema: anytype, index: anytype, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, coarse: bool) !?usize {
            if (index.predicate != null or index.tuple.keys.len == 0 or index.tuple.keys.len < order.len or (coarse and order.len == 0)) return null;
            const last = index.tuple.keys.len - order.len;
            var prefix: usize = if (coarse) 0 else last;
            candidates: while (prefix <= last) : (prefix += 1) {
                if (coarse and prefix == last) break;
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
                    if (key.expression != null or key.fold_ascii or key.descending != requested.descending or key.nulls_first != requested.descending) continue :candidates;
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
            var schema = db.core.acquireSchemaView() orelse return null;
            defer schema.release();
            var indexes = db.core.relational_indexes.acquire() orelse return null;
            defer indexes.deinit();
            if (indexes.plan.schemaView().epoch != schema.epoch) return error.CatalogGenerationChanged;
            const cursor = if (encoded) |bytes| try std.json.parseFromSliceLeaky(OrderedCursor, temporary, bytes, .{}) else null;
            for ([_]bool{ false, true }) |coarse| candidates: for (indexes.plan.boundIndexes()) |index| {
                if (cursor) |position| if (!std.mem.eql(u8, position.index, index.name) or position.coarse != coarse) continue;
                const prefix = (try self.orderedIndexPrefix(schema, index, filters, order, coarse)) orelse continue;
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
                const authorization = try self.readAuthorization(temporary, table);
                var opts = try self.scanOptions(temporary, db, .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .include_documents = true, .limit = @intCast(@min(limit, 1024)) });
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
                    if (!filterCompatible(column_type, filter.value)) continue;
                    if (column_type == .number and filter.value == .integer and filter.op != .eq and
                        (filter.value.integer < -9007199254740992 or filter.value.integer > 9007199254740992)) continue;
                    const value = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, filter.value, .{}), .{ .parse_numbers = false });
                    try conditions.append(temporary, .{ .column = filter.property, .op = switch (filter.op) {
                        .eq => .eq,
                        .lt => .lt,
                        .lte => .lte,
                        .gt => .gt,
                        .gte => .gte,
                        .in => unreachable,
                    }, .value = value });
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
                        if (condition.op == .eq and std.mem.eql(u8, condition.column, name)) break true;
                    } else false;
                    if (!enforced) continue :candidates;
                }
                opts.relational_query.?.conditions = conditions.items;
                opts.relational_query.?.index = index.name;
                opts.relational_query.?.auto_index = true;
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
                if (limit == 0) return .{ .items = &.{}, .cursors = &.{}, .examined = 0, .stable_ties = !coarse, .next = try std.json.Stringify.valueAlloc(a, OrderedCursor{ .coarse = coarse, .index = index.name, .schema_version = session.reader.active.version(), .after = if (cursor) |position| position.after else null }, .{}) };
                var page = try session.reader.nextPage(temporary, null, .{ .rows = @min(limit, 1024), .records = @min(@min(@max(limit, 128), max_examined), 1024), .output_bytes = 16 << 20 });
                defer page.deinit();
                var nodes: std.ArrayList(*const v.Node) = .empty;
                var cursors: std.ArrayList(?[]const u8) = .empty;
                for (page.rows) |row| if (try self.node(a, table, row.key, row.json)) |node_value| if (node_value.labels.len != 0) {
                    try nodes.append(a, node_value);
                    try cursors.append(a, try std.json.Stringify.valueAlloc(a, OrderedCursor{ .coarse = coarse, .index = index.name, .schema_version = session.reader.active.version(), .after = row.cursor orelse return error.CypherInvalidBackendCursor }, .{}));
                };
                const next = if (page.more) try std.json.Stringify.valueAlloc(a, OrderedCursor{ .coarse = coarse, .index = index.name, .schema_version = session.reader.active.version(), .after = page.next_cursor orelse return error.CypherInvalidBackendCursor }, .{}) else null;
                if (cursors.items.len != 0) cursors.items[cursors.items.len - 1] = next;
                try checkpoint(ptr);
                return .{ .items = try nodes.toOwnedSlice(a), .cursors = try cursors.toOwnedSlice(a), .examined = page.records_examined, .stable_ties = !coarse, .next = next };
            };
            if (cursor != null) return error.CatalogGenerationChanged;
            return null;
        }
        fn estimateWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) !?usize {
            if (limit == 0) return null;
            if (cast(ptr).work) |work| try work.charge(work.ptr, limit);
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const page = (try scanWhereImpl(ptr, scratch.allocator(), table, filters, null, limit, true)) orelse return null;
            return if (page.next == null) page.items.len else null;
        }
        fn scanWhereImpl(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize, index_only: bool) !?b.Page(*const v.Node) {
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
            var opts = try self.scanOptions(temporary, db, .{ .execution_deadline_ns = authorization.deadline_ns, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .include_documents = true, .limit = @intCast(limit) });
            if (opts.relational_query == null) return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
            var schema = db.core.acquireSchemaView() orelse return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
            defer schema.release();
            const Query = @TypeOf(opts.relational_query.?);
            var conditions: std.ArrayList(Query.Condition) = .empty;
            var alternatives: []const v.Value = &.{};
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
                    if (unique.items.len == 0) return .{ .items = &.{} };
                    alternatives = unique.items;
                    if (cursor.branch >= alternatives.len) return error.CypherInvalidBackendCursor;
                    operand = alternatives[cursor.branch];
                }
                // NUMERIC can round in the Cypher projection; its exact index
                // bounds are unsafe. Unsupported/mixed operands stay residual.
                if (!filterCompatible(column_type, operand)) continue;
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
                }, .value = value });
            }
            if (conditions.items.len == 0) return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
            opts.relational_query.?.conditions = conditions.items;
            opts.relational_query.?.auto_index = true;
            opts.relational_query.?.index = cursor.index;
            opts.relational_query.?.after = cursor.after;
            opts.relational_query.?.schema_version = cursor.schema_version;
            opts.inclusive_from = cursor.key == null;
            const session = db.openRelationalReadSession(temporary, cursor.key orelse "", "", opts) catch |err| switch (err) {
                // Native schema types can be narrower than Cypher values.
                // Unsupported operands keep the complete residual scan path.
                error.InvalidBatchRequest, error.RelationalIndexColumnTypeMismatch, error.UnsupportedRelationalIndexColumn, error.RelationalIndexColumnNotFound => return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit),
                else => return err,
            };
            defer session.deinit();
            // An unindexed predicate must remain a paged runtime residual,
            // rather than hiding an unbounded native scan inside one call.
            if (session.reader.index == null) return if (index_only) null else try propertyFallback(ptr, a, table, cursor, limit);
            session.reader.now_ns = self.ttl_now_ns;
            try session.checkpoint();
            var page = try session.reader.nextPage(temporary, null, .{ .rows = limit, .records = limit, .output_bytes = 16 << 20 });
            defer page.deinit();
            var nodes: std.ArrayList(*const v.Node) = .empty;
            for (page.rows) |row| if (try self.node(a, table, row.key, row.json)) |node_value| if (node_value.labels.len != 0) try nodes.append(a, node_value);
            const next = if (page.more) try std.json.Stringify.valueAlloc(a, PropertyCursor{ .branch = cursor.branch, .schema_version = session.reader.active.version(), .index = session.reader.index.?.name, .after = page.next_cursor orelse return error.CypherInvalidBackendCursor }, .{}) else if (cursor.branch + 1 < alternatives.len)
                try std.json.Stringify.valueAlloc(a, PropertyCursor{ .branch = cursor.branch + 1, .schema_version = session.reader.active.version(), .index = session.reader.index.?.name }, .{})
            else
                null;
            try checkpoint(ptr);
            return .{ .items = try nodes.toOwnedSlice(a), .next = next, .examined = page.records_examined };
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
                if (self.read_fields != null) {
                    const opts = try self.scanOptions(temporary, db, .{ .inclusive_from = true, .include_documents = true, .limit = 1, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .execution_deadline_ns = authorization.deadline_ns });
                    if (opts.relational_query == null) {
                        if (opts.include_all_fields) break :projected try self.getFull(a, temporary, db, identity, authorization);
                        var document = (try db.getDocument(temporary, identity.key, .{ .fields = opts.fields, .include_all_fields = false, .row_policy_principal_proof = authorization.proof, .row_policy_database = authorization.database, .execution_deadline_ns = authorization.deadline_ns })) orelse break :projected null;
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
            // Incoming adjacency physically belongs to the declared source
            // table, even when its target key belongs to another table.
            var position: Resume = if (encoded) |json| try std.json.parseFromSliceLeaky(Resume, a, json, .{}) else .{ .incoming = direction == .in or !std.mem.eql(u8, identity.table, source_table) };
            const incoming_possible = direction != .out and std.mem.eql(u8, identity.table, target_table);
            const outgoing_possible = direction != .in and std.mem.eql(u8, identity.table, source_table);
            if ((position.incoming and !incoming_possible) or (!position.incoming and !outgoing_possible)) return .{ .items = &.{} };
            const db = try self.selected(source_table);
            const authorization = try self.readAuthorization(temporary, source_table);
            var page = captured: {
                if (comptime @hasDecl(DB, "graphEdgesForCypherCapturePageAuthorizedAt")) {
                    for (self.tables) |table| if (table.db == db and std.mem.eql(u8, table.name, source_table)) {
                        if (table.capture_fence) |fence| break :captured try db.graphEdgesForCypherCapturePageAuthorizedAt(fence, temporary, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, .{ .max_edges = limit, .max_owned_bytes = 4 << 20, .max_scanned_rows = @min(4096, @max(1, max_examined)), .peer_key = if (peer) |value| value.key else null }, self.ttl_now_ns, authorization.proof, authorization.database);
                    };
                }
                break :captured if (comptime @hasDecl(DB, "graphEdgesForCypherPageAuthorizedAt"))
                    try db.graphEdgesForCypherPageAuthorizedAt(temporary, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, .{ .max_edges = limit, .max_owned_bytes = 4 << 20, .max_scanned_rows = @min(4096, @max(1, max_examined)), .peer_key = if (peer) |value| value.key else null }, self.ttl_now_ns, authorization.proof, authorization.database)
                else blk: {
                    if (authorization.proof.len != 0) return error.CypherRowPolicyUnsupported;
                    break :blk try db.graphEdgesForCypherPageAt(temporary, mapping.index, identity.key, &.{mapping.stored_type}, if (position.incoming) .in else .out, position.cursor, .{ .max_edges = limit, .max_owned_bytes = 4 << 20, .max_scanned_rows = @min(4096, @max(1, max_examined)), .peer_key = if (peer) |value| value.key else null }, self.ttl_now_ns);
                };
            };
            defer page.deinit(temporary);
            var result: std.ArrayList(*const v.Relationship) = .empty;
            for (page.edges) |edge| {
                const metadata = if (edge.metadata.len == 0) std.json.Value{ .object = .empty } else try std.json.parseFromSliceLeaky(std.json.Value, a, edge.metadata, .{ .parse_numbers = false });
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
                const encoded_properties = try root.wire.relationshipProperties(a, metadata);
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
