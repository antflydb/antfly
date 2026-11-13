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

//! Context-source reads served by a resident DB.
//!
//! `ownTable` serves the DB's own table identity and refuses every other one
//! (`ContextSourceTableUnavailable`): a single DB has no authority to read
//! foreign tables. Hosts with a table registry compose `Registry`, which maps
//! table identities to resident DBs (embedded Lite) or install their own
//! routing reader (the distributed server's `TableReadSource`).
//!
//! Reads use the DB's public read path, so TTL visibility and row policies
//! apply exactly as for any other reader of the table.

const std = @import("std");
const Allocator = std.mem.Allocator;
const context_resolution = @import("enrichment/context_resolution.zig");
const context_sources = @import("enrichment/context_sources.zig");
const graph_mod = @import("../../graph/graph.zig");

pub fn ownTable(comptime DB: type, db: *DB, own_table_id: []const u8) context_resolution.Reader {
    const Impl = Adapter(DB);
    return .{
        .ptr = db,
        .own_table_id = own_table_id,
        .lookup_fn = Impl.lookupOwn,
        .query_fn = Impl.queryOwn,
        .frontier_fn = Impl.frontierOwn,
        .edges_fn = Impl.edgesOwn,
        .artifacts_fn = Impl.artifactsOwn,
        .search_fn = Impl.searchOwn,
    };
}

pub fn Adapter(comptime DB: type) type {
    return struct {
        fn lookupOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8 {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return lookup(db, alloc, key);
        }

        fn queryOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, filter: []const u8, offset: u32, limit: u32) anyerror!context_resolution.Page {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return query(db, alloc, filter, offset, limit);
        }

        fn frontierOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8) anyerror![]u8 {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return frontier(db, alloc);
        }

        fn edgesOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, graph_index: []const u8, node: []const u8, types: []const []const u8, direction: context_sources.GraphSpec.Direction, options: context_resolution.EdgeOptions) anyerror![]context_resolution.Edge {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return edges(db, alloc, graph_index, node, types, direction, options);
        }

        fn artifactsOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8, names: []const []const u8) anyerror![]u8 {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return artifacts(db, alloc, key, names);
        }

        pub fn artifacts(db: *DB, alloc: Allocator, key: []const u8, names: []const []const u8) ![]u8 {
            return @import("context_dependency_db.zig").artifactValuesJsonAlloc(alloc, db.core.store, db.core.index_manager, key, names);
        }

        fn searchOwn(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, q: context_resolution.SearchQuery) anyerror!context_resolution.Page {
            const db: *DB = @ptrCast(@alignCast(ptr));
            if (!isOwn(db, table_id)) return error.ContextSourceTableUnavailable;
            return search(db, alloc, q);
        }

        /// Explicitly written edges, plus field-derived and generated edges
        /// when `include_generated` (their changes are observed through the
        /// graph edge artifacts staged by commits and generated records).
        pub fn edges(db: *DB, alloc: Allocator, graph_index: []const u8, node: []const u8, types: []const []const u8, direction: context_sources.GraphSpec.Direction, options: context_resolution.EdgeOptions) ![]context_resolution.Edge {
            const limit = options.limit;
            var out = std.ArrayListUnmanaged(context_resolution.Edge).empty;
            errdefer {
                for (out.items) |e| {
                    alloc.free(e.edge_type);
                    alloc.free(e.neighbor);
                }
                out.deinit(alloc);
            }
            const entry = db.core.index_manager.graphIndex(graph_index) orelse return error.ContextGraphUnavailable;
            const dir: graph_mod.EdgeDirection = switch (direction) {
                .out => .out,
                .in => .in,
                .both => .both,
            };
            var cursor: ?graph_mod.EdgeScanCursor = null;
            defer if (cursor) |*c| c.deinit(alloc);
            var scanned: usize = 0;
            while (out.items.len < limit) {
                var page = try entry.index.getEdgesByTypesPage(alloc, node, types, dir, cursor, .{ .max_edges = 256, .max_owned_bytes = 512 * 1024, .max_scanned_rows = 4096 });
                defer graph_mod.GraphIndex.freeEdges(alloc, page.edges);
                scanned += page.scanned_rows;
                for (page.edges) |edge| {
                    if (!options.include_generated and (edge.artifact_derived or edge.field_derived)) continue;
                    if (out.items.len >= limit) break;
                    const outgoing = std.mem.eql(u8, edge.source, node) and direction != .in;
                    const neighbor = if (outgoing) edge.target else edge.source;
                    const t = try alloc.dupe(u8, edge.edge_type);
                    errdefer alloc.free(t);
                    try out.append(alloc, .{ .edge_type = t, .neighbor = try alloc.dupe(u8, neighbor), .outgoing = outgoing });
                }
                if (cursor) |*c| c.deinit(alloc);
                cursor = page.next_cursor;
                page.next_cursor = null;
                if (cursor == null) break;
                if (scanned > 64 * 4096) return error.ContextBudgetExceeded;
            }
            return out.toOwnedSlice(alloc);
        }

        /// Ranked retrieval: full-text relevance (`match`) or dense k-NN.
        pub fn search(db: *DB, alloc: Allocator, q: context_resolution.SearchQuery) !context_resolution.Page {
            var result = if (q.vector.len != 0)
                try db.search(alloc, .{ .index_name = q.vector_index, .dense = .{ .vector = q.vector, .k = q.k }, .limit = q.k, .include_stored = true, .include_all_fields = true })
            else
                try db.search(alloc, .{ .full_text = .{ .match = .{ .field = q.text_field, .text = q.text } }, .limit = q.k, .include_stored = true, .include_all_fields = true });
            defer result.deinit();
            var rows = std.ArrayListUnmanaged(context_resolution.Row).empty;
            errdefer {
                for (rows.items) |row| {
                    alloc.free(row.key);
                    alloc.free(row.value);
                }
                rows.deinit(alloc);
            }
            for (result.hits) |hit| {
                const value = if (hit.stored_data) |data| try alloc.dupe(u8, data) else (try db.get(alloc, hit.id)) orelse continue;
                errdefer alloc.free(value);
                try rows.append(alloc, .{ .key = try alloc.dupe(u8, hit.id), .value = value });
            }
            return .{ .rows = try rows.toOwnedSlice(alloc), .exhausted = true };
        }

        fn isOwn(db: *DB, table_id: []const u8) bool {
            if (table_id.len == 0) return true;
            return std.mem.eql(u8, table_id, db.contextSourceTableId());
        }

        pub fn lookup(db: *DB, alloc: Allocator, key: []const u8) !?[]u8 {
            return db.get(alloc, key);
        }

        pub fn query(db: *DB, alloc: Allocator, envelope: []const u8, offset: u32, limit: u32) !context_resolution.Page {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, envelope, .{}) catch return error.InvalidContextBinding;
            defer parsed.deinit();
            const search_value = parsed.value.object.get("search") orelse return error.InvalidContextBinding;
            const scan_value = parsed.value.object.get("scan") orelse .null;
            const search = try std.json.Stringify.valueAlloc(alloc, search_value, .{});
            defer alloc.free(search);
            const scan_filter: []u8 = if (scan_value == .null) try alloc.dupe(u8, "") else try std.json.Stringify.valueAlloc(alloc, scan_value, .{});
            defer alloc.free(scan_filter);
            // Range-only predicates: the storage-evaluated scan filter is the
            // range-aware path (columnar block pruning on relational tables).
            const has_text_pushdown = std.mem.indexOf(u8, search, "\"match\"") != null;
            if (!has_text_pushdown and scan_filter.len != 0) return scanPage(db, alloc, scan_filter, offset, limit);
            return queryIndexed(db, alloc, search, offset, limit) catch |err| switch (err) {
                // No full-text index can serve the prefilter: fall back to a
                // filtered table scan. Resolution bounds it by the declared
                // candidate budget and verifies every row exactly.
                error.UnsupportedQueryRequest => scanPage(db, alloc, scan_filter, offset, limit),
                else => err,
            };
        }

        fn scanPage(db: *DB, alloc: Allocator, filter: []const u8, offset: u32, limit: u32) !context_resolution.Page {
            const Collect = struct {
                alloc: Allocator,
                rows: std.ArrayListUnmanaged(context_resolution.Row) = .empty,
                skip: u32,
                limit: u32,
                more: bool = false,
                fn visit(raw: ?*anyopaque, entry: @import("types.zig").ScanVisitEntry) anyerror!void {
                    const c: *@This() = @ptrCast(@alignCast(raw.?));
                    const doc = entry.document_json orelse return;
                    if (c.skip > 0) {
                        c.skip -= 1;
                        return;
                    }
                    if (c.rows.items.len >= c.limit) {
                        c.more = true;
                        return error.Canceled;
                    }
                    const key = try c.alloc.dupe(u8, entry.id);
                    errdefer c.alloc.free(key);
                    try c.rows.append(c.alloc, .{ .key = key, .value = try c.alloc.dupe(u8, doc) });
                }
            };
            var collect = Collect{ .alloc = alloc, .skip = offset, .limit = limit };
            errdefer {
                for (collect.rows.items) |row| {
                    alloc.free(row.key);
                    alloc.free(row.value);
                }
                collect.rows.deinit(alloc);
            }
            db.scanVisit(alloc, "", "", .{ .include_documents = true, .filter_query_json = filter }, .{ .context = &collect, .visit = Collect.visit }) catch |err| switch (err) {
                error.Canceled => {},
                else => return err,
            };
            return .{ .rows = try collect.rows.toOwnedSlice(alloc), .exhausted = !collect.more };
        }

        fn queryIndexed(db: *DB, alloc: Allocator, filter: []const u8, offset: u32, limit: u32) !context_resolution.Page {
            var result = try db.search(alloc, .{
                .filter_query_json = filter,
                .limit = limit,
                .offset = offset,
                .include_stored = true,
                .include_all_fields = true,
            });
            defer result.deinit();
            var rows = std.ArrayListUnmanaged(context_resolution.Row).empty;
            errdefer {
                for (rows.items) |row| {
                    alloc.free(row.key);
                    alloc.free(row.value);
                }
                rows.deinit(alloc);
            }
            for (result.hits) |hit| {
                const value = if (hit.stored_data) |data| try alloc.dupe(u8, data) else (try db.get(alloc, hit.id)) orelse continue;
                errdefer alloc.free(value);
                try rows.append(alloc, .{ .key = try alloc.dupe(u8, hit.id), .value = value });
            }
            const exhausted = result.hits.len < limit or
                (result.total_hits_relation == .exact and @as(u64, offset) + result.hits.len >= result.total_hits);
            return .{ .rows = try rows.toOwnedSlice(alloc), .exhausted = exhausted };
        }

        pub fn frontier(db: *DB, alloc: Allocator) ![]u8 {
            return std.fmt.allocPrint(alloc, "seq:{d}", .{db.core.nextDerivedSequence()});
        }
    };
}

/// Embedded multi-table reader: resolves table identities to resident DBs.
/// The registry callback must return a DB that stays open for the read; Lite
/// table handles outlive the enrichment workers of every table they host.
pub fn Registry(comptime DB: type) type {
    return struct {
        ptr: *anyopaque,
        own: *DB,
        own_table_id: []const u8,
        resolve_fn: *const fn (ptr: *anyopaque, table_id: []const u8) ?*DB,

        const Self = @This();
        const Impl = Adapter(DB);

        pub fn reader(self: *Self) context_resolution.Reader {
            return .{ .ptr = self, .own_table_id = self.own_table_id, .lookup_fn = lookupImpl, .query_fn = queryImpl, .frontier_fn = frontierImpl, .edges_fn = edgesImpl, .search_fn = searchImpl, .artifacts_fn = artifactsImpl };
        }

        fn target(self: *Self, table_id: []const u8) !*DB {
            if (table_id.len == 0 or std.mem.eql(u8, table_id, self.own_table_id)) return self.own;
            return self.resolve_fn(self.ptr, table_id) orelse error.ContextSourceTableUnavailable;
        }

        fn lookupImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8) anyerror!?[]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.lookup(try self.target(table_id), alloc, key);
        }

        fn queryImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, filter: []const u8, offset: u32, limit: u32) anyerror!context_resolution.Page {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.query(try self.target(table_id), alloc, filter, offset, limit);
        }

        fn edgesImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, graph_index: []const u8, node: []const u8, types: []const []const u8, direction: context_sources.GraphSpec.Direction, options: context_resolution.EdgeOptions) anyerror![]context_resolution.Edge {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.edges(try self.target(table_id), alloc, graph_index, node, types, direction, options);
        }

        fn artifactsImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, key: []const u8, names: []const []const u8) anyerror![]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.artifacts(try self.target(table_id), alloc, key, names);
        }

        fn searchImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8, q: context_resolution.SearchQuery) anyerror!context_resolution.Page {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.search(try self.target(table_id), alloc, q);
        }

        fn frontierImpl(ptr: *anyopaque, alloc: Allocator, table_id: []const u8) anyerror![]u8 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return Impl.frontier(try self.target(table_id), alloc);
        }
    };
}
