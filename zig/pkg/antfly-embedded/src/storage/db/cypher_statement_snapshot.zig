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

const std = @import("std");
const cypher = @import("cypher");
const port = @import("../cypher_read.zig");
const domain = @import("../../system_catalog/domain.zig");
pub fn Snapshot(comptime DB: type) type {
    return struct {
        const Self = @This();
        references: std.atomic.Value(usize) = .init(1),
        a: std.mem.Allocator,
        arena: std.heap.ArenaAllocator,
        source: *DB,
        db: DB,
        population_upper_bound: ?u64,
        input: port.Input,
        graph: cypher.catalog.Graph,
        graphs: []const cypher.catalog.Graph,
        catalog_guards: []cypher.catalog_guard.Guard,
        graph_index: usize = 0,
        tables: [1]DB.CypherBackend.Table,
        adapter: DB.CypherBackend,
        policy: @import("row_policy_gate.zig").Gate.Lease,
        pub fn create(a: std.mem.Allocator, db: *DB, fence: *const DB.StatementReadFence, input: port.Input, cancellation: @import("types.zig").CancellationToken) !port.Read {
            var stage: []const u8 = "catalog";
            errdefer std.log.warn("Cypher owner snapshot failed table_id={} stage={s}", .{ input.table_id, stage });
            const self = try a.create(Self);
            errdefer a.destroy(self);
            var arena = std.heap.ArenaAllocator.init(a);
            errdefer arena.deinit();
            const owned = arena.allocator();
            const copied = try std.json.parseFromSliceLeaky(port.Input, owned, try std.json.Stringify.valueAlloc(owned, input, .{}), .{ .allocate = .alloc_always });
            const record = try std.json.parseFromSliceLeaky(domain.Resource, owned, copied.catalog_json, .{ .allocate = .alloc_always });
            const graph = try domain.graphForExecution(owned, record);
            const records = if (copied.catalogs_json.len == 0) try owned.dupe(domain.Resource, &.{record}) else try std.json.parseFromSliceLeaky([]const domain.Resource, owned, copied.catalogs_json, .{ .allocate = .alloc_always });
            if (records.len == 0 or records.len > 64) return error.CypherOwnerBudgetExceeded;
            const graphs = try owned.alloc(cypher.catalog.Graph, records.len);
            var primary = false;
            for (records, graphs, 0..) |entry, *output, index| {
                for (records[0..index]) |prior| if (prior.id == entry.id) return error.CypherInvalidOwner;
                output.* = try domain.graphForExecution(owned, entry);
                if (entry.id == record.id) primary = true;
            }
            if (!primary) return error.CatalogGenerationChanged;

            if (copied.table_id != db.core.identity_namespace.table_id) return error.CatalogGenerationChanged;
            if (copied.ttl_now_ns == 0) return error.CypherSnapshotRequired;
            var schema = db.core.acquireSchemaView();
            defer if (schema) |*view| view.release();
            if (schema) |view| {
                if (view.version() != copied.schema_version) return error.SchemaVersionChanged;
            } else if (copied.schema_version != 0) return error.SchemaVersionChanged;
            stage = "authority";
            const authority = try db.acquireCypherReadAuthority(a, copied.principal_proof, copied.database);
            errdefer {
                var lease = authority;
                lease.release();
            }
            var cut: @import("native_query_cut.zig").Request = .{ .id = &copied.cut_id, .table_id = copied.table_id, .expires_ms = copied.expires_ms, .create = true, .indexes = copied.indexes };
            stage = "capture";
            // Install cleanup before capture so a failed publication/open/bind
            // cannot leave a statement generation consuming admission capacity.
            errdefer db.releaseStatementQueryCut(cut);
            try db.captureQueryCutAtStatementFence(cut, fence, cancellation);
            cut.create = false;
            stage = "open";
            var readonly = try db.openQueryCut(cut, cancellation);
            errdefer readonly.close();
            const population: ?u64 = population: {
                var primary_read = try readonly.core.store.beginReadTxn();
                defer primary_read.abort();
                const summary = (try @import("doc_identity.zig").readVisibilitySummaryTxn(&primary_read)) orelse break :population null;
                break :population summary.live_ordinals;
            };
            const guards = try owned.alloc(cypher.catalog_guard.Guard, graphs.len);
            for (graphs, guards) |entry, *guard| guard.* = try cypher.catalog_guard.Guard.init(owned, entry, copied.table);
            self.* = .{ .catalog_guards = guards, .a = a, .arena = arena, .source = db, .db = readonly, .population_upper_bound = population, .input = copied, .graph = graph, .graphs = graphs, .policy = authority, .tables = undefined, .adapter = undefined };
            self.tables = .{.{ .name = self.input.table, .db = &self.db }};
            self.adapter = .{ .tables = &self.tables, .ttl_now_ns = copied.ttl_now_ns, .partitioned = true, .authority = .{ .ptr = self, .bind = bind, .checkpoint = checkpoint, .project = project, .project_relationship = projectRelationship, .read_authorization = readAuthorization, .population_upper_bound = populationUpperBound, .search = search, .storage_projection = true, .ordered_projection = true } };
            const backend = self.adapter.backend();
            stage = "bind";
            // Every allowed immutable binding is checked once. Switching an
            // operation's graph later changes only the prevalidated catalog.
            for (graphs, 0..) |entry, index| {
                self.graph = entry;
                self.graph_index = index;
                try backend.vtable.bind(backend.ptr, owned, entry);
            }
            self.graph = graph;
            for (graphs, 0..) |entry, index| if (entry.incarnation == graph.incarnation and entry.revision == graph.revision) {
                self.graph_index = index;
                break;
            };
            self.adapter.graph = graph;
            self.arena = arena;
            return .{ .ptr = self, .vtable = &.{ .invoke = invoke, .invoke_typed = invokeTyped, .release = release, .open_sql = openSql } };
        }
        fn cast(ptr: *anyopaque) *Self {
            return @ptrCast(@alignCast(ptr));
        }
        fn bind(ptr: *anyopaque, graph: cypher.catalog.Graph) !void {
            const self = cast(ptr);
            const allowed = for (self.graphs) |entry| {
                if (graph.incarnation == entry.incarnation and graph.revision == entry.revision) break true;
            } else false;
            if (!allowed) return error.CatalogGenerationChanged;
            try checkpoint(ptr);
        }
        fn checkpoint(ptr: *anyopaque) !void {
            const self = cast(ptr);
            const time = @import("antfly_platform").time.realtimeNs();
            if (time / std.time.ns_per_ms >= self.input.expires_ms) return error.DeadlineExceeded;
            try self.policy.checkAt(@intCast(time / std.time.ns_per_s));
            var scratch = std.heap.ArenaAllocator.init(self.a);
            defer scratch.deinit();
            try self.catalog_guards[self.graph_index].validate(scratch.allocator(), CatalogSource{ .db = self.source });
        }
        const CatalogSource = struct {
            db: *DB,
            pub fn epoch(self: @This()) cypher.catalog_guard.Epoch {
                // Synchronize with catalog publication, including restore/load.
                self.db.core.lockApplyShared();
                defer self.db.core.unlockApplyShared();
                const manager = self.db.core.index_manager;
                manager.catalog_mutex.lockShared();
                defer manager.catalog_mutex.unlockShared();
                return .{ .owner = @intFromPtr(manager), .generation = manager.writePlanGeneration() };
            }
            pub fn identity(self: @This(), a: std.mem.Allocator, index: []const u8) !?cypher.catalog.IndexIdentity {
                return self.db.cypherGraphIndexIdentity(a, index);
            }
        };
        fn populationUpperBound(ptr: *anyopaque, table: []const u8) !?u64 {
            const self = cast(ptr);
            if (!std.mem.eql(u8, table, self.input.table)) return error.Forbidden;
            try checkpoint(ptr);
            return self.population_upper_bound;
        }
        fn readAuthorization(ptr: *anyopaque, _: std.mem.Allocator, table: []const u8) !DB.CypherBackend.ReadAuthorization {
            const self = cast(ptr);
            if (!std.mem.eql(u8, table, self.input.table)) return error.Forbidden;
            const remaining_ms = self.input.expires_ms -| @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
            return .{ .proof = self.input.principal_proof, .database = self.input.database, .deadline_ns = @import("antfly_platform").time.monotonicNs() + @as(u64, @min(remaining_ms, 5000)) * std.time.ns_per_ms };
        }
        fn project(ptr: *anyopaque, a: std.mem.Allocator, node: *const cypher.value.Node) !?*const cypher.value.Node {
            const self = cast(ptr);
            if (self.input.filter_query_json.len != 0) {
                var match = try self.db.scan(a, node.identity.key, node.identity.key, try self.db.cypherDocumentScanOptions(a, .{ .inclusive_from = true, .limit = 1, .execution_deadline_ns = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s, .filter_query_json = self.input.filter_query_json, .row_policy_principal_proof = self.input.principal_proof, .row_policy_database = self.input.database }));
                defer match.deinit(a);
                if (match.hashes.len == 0) return null;
            }
            return node;
        }
        const SearchMembership = struct {
            snapshot: *Self,
            table: []const u8,
            max_examined: usize,
            examined: usize = 0,
            mutex: std.atomic.Mutex = .unlocked,
            cache: [128]Cache = @splat(.{}),
            next: usize = 0,
            const Cache = struct { key: ?[]u8 = null, allowed: bool = false };
            fn deinit(self: *@This()) void {
                for (self.cache) |entry| if (entry.key) |key| std.heap.page_allocator.free(key);
            }
            fn allows(ptr: *anyopaque, member: []const u8) !bool {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                @import("antfly_platform").sync.lockYielding(&self.mutex);
                defer self.mutex.unlock();
                try checkpoint(self.snapshot);
                var scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator);
                defer scratch.deinit();
                const a = scratch.allocator();
                const ref = try @import("artifact_ids.zig").decodeArtifactRefAlloc(a, member);
                const key = if (ref) |artifact| artifact.document_id else member;
                for (self.cache) |entry| if (entry.key) |cached| if (std.mem.eql(u8, cached, key)) return entry.allowed;
                if (self.examined >= self.max_examined) return error.CypherAnchorBudgetExceeded;
                self.examined += 1;
                const backend = self.snapshot.adapter.backend();
                const node = try backend.vtable.get(backend.ptr, a, .{ .table = self.table, .key = key });
                const allowed = if (node) |value| value.labels.len != 0 else false;
                const entry = &self.cache[self.next];
                if (entry.key) |old| std.heap.page_allocator.free(old);
                entry.* = .{ .key = null, .allowed = allowed };
                entry.key = try std.heap.page_allocator.dupe(u8, key);
                self.next = (self.next + 1) % self.cache.len;
                return allowed;
            }
        };
        fn search(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, input: cypher.backend.SearchInput) !cypher.backend.SearchPage {
            if (input.usage) |usage| usage.examined = 0;
            const self = cast(ptr);
            if (!std.mem.eql(u8, table, self.input.table)) return error.Forbidden;
            if (input.prepared_json.len == 0) return error.CypherSearchPreparationRequired;
            try checkpoint(ptr);
            const authorization = try readAuthorization(ptr, a, table);
            const pinned_indexes = try self.db.listIndexes(a);
            defer {
                for (pinned_indexes) |*index| index.deinit(a);
                a.free(pinned_indexes);
            }
            const live_indexes = try self.source.listIndexes(a);
            defer {
                for (live_indexes) |*index| index.deinit(a);
                a.free(live_indexes);
            }
            for (pinned_indexes) |pinned| {
                const live = for (live_indexes) |index| {
                    if (std.mem.eql(u8, index.name, pinned.name)) break index;
                } else return error.CatalogGenerationChanged;
                if (pinned.kind != live.kind or pinned.coverage_generation != live.coverage_generation or pinned.coverage_config_fingerprint != live.coverage_config_fingerprint or !std.mem.eql(u8, pinned.config_json, live.config_json)) return error.CatalogGenerationChanged;
            }
            var parsed = try @import("../../api/query_contract.zig").parseQueryRequestWithDeadline(a, null, table, input.prepared_json, authorization.deadline_ns);
            defer parsed.deinit(a);
            var request = parsed.req;
            if (request.limit == 0 or request.limit > 1024 or request.offset != 0) return error.CypherInvalidSearchLimit;
            if (request.graph_queries.len != 0 or request.graph_metric_queries.len != 0 or request.graph_metric_rerank != null or request.reranker != null) return error.UnsupportedQueryRequest;
            // Native collectors gate candidates before selecting top-k. An
            // unrestricted table label needs no extra membership predicate.
            const unrestricted = for (self.graph.labels) |label| {
                if (std.mem.eql(u8, label.table, table) and label.predicate.len == 0) break true;
            } else false;
            var membership: SearchMembership = .{ .snapshot = self, .table = table, .max_examined = input.max_examined };
            defer membership.deinit();
            defer if (input.usage) |usage| {
                usage.examined = @max(usage.examined orelse 0, membership.examined);
            };
            if (!unrestricted or self.input.filter_query_json.len != 0 or self.db.core.table_catalog.row_policy_phase == .active)
                request.native_key_predicate = .{ .ptr = &membership, .allows = SearchMembership.allows };
            request.include_stored = false;
            var result = try self.db.searchCypherCut(a, request, authorization.proof, authorization.database);
            defer result.deinit();
            if (result.hits.len > input.max_examined) return error.CypherAnchorBudgetExceeded;
            if (input.usage) |usage| usage.examined = result.hits.len;
            var seeds: std.ArrayList(cypher.backend.Seed) = .empty;
            const backend = self.adapter.backend();
            for (result.hits) |hit| {
                try checkpoint(ptr);
                const node = (try backend.vtable.get(backend.ptr, a, .{ .table = table, .key = hit.id })) orelse continue;
                if (node.labels.len == 0) continue;
                const score: f64 = hit.score orelse 0;
                if (!std.math.isFinite(score)) return error.CypherInvalidSearchResult;
                try seeds.append(a, .{ .node = node, .score = score });
            }
            try checkpoint(ptr);
            const examined = @max(membership.examined, seeds.items.len);
            return .{ .seeds = try seeds.toOwnedSlice(a), .examined = examined, .approximate = request.dense != null or request.dense_queries.len != 0 or request.query == .dense_knn or result.total_hits_relation != .exact };
        }
        fn projectRelationship(ptr: *anyopaque, a: std.mem.Allocator, relationship: *const cypher.value.Relationship) !?*const cypher.value.Relationship {
            const self = cast(ptr);
            if (relationship.ownership != .explicit) {
                const producer_identity = (try relationship.producingDocument(a)) orelse return error.CypherInvalidRelationship;
                if (!std.mem.eql(u8, producer_identity.table, self.input.table)) return error.CypherInvalidRelationship;
                // An artifact may override its source key. The producing row
                // still owns its visibility, independently of both endpoints.
                var producer = (try self.db.getDocument(a, producer_identity.key, .{ .row_policy_principal_proof = self.input.principal_proof, .row_policy_database = self.input.database })) orelse return null;
                defer producer.deinit(a);
                if (self.input.filter_query_json.len != 0) {
                    var filter = try @import("../../search/pattern_filter.zig").PreparedPatternFilter.init(a, self.input.filter_query_json);
                    defer filter.deinit();
                    if (!try filter.matchesJson(a, producer_identity.key, try std.json.parseFromSliceLeaky(std.json.Value, a, producer.json, .{ .parse_numbers = false }))) return null;
                }
            }
            return relationship;
        }
        fn invoke(ptr: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
            if (body.len > 1 << 20) return error.CypherRequestTooLarge;
            var arena = std.heap.ArenaAllocator.init(cypher.backend.reclaimingAllocator(a));
            defer arena.deinit();
            const scratch = arena.allocator();
            const request = try std.json.parseFromSliceLeaky(cypher.rpc.Request, scratch, body, .{});
            const self = cast(ptr);
            if (request.operation == .observe or request.operation == .observe_ranges) {
                try checkpoint(ptr);
                _ = try self.selectGraph(scratch, request);
                const observations = @import("cypher_write_observation.zig");
                const observation = if (request.operation == .observe_ranges) try observations.captureRanges(Self, self, scratch) else try observations.capture(Self, self, scratch);
                return std.json.Stringify.valueAlloc(a, observation, .{});
            }
            const reply = try invokeTypedLocal(ptr, scratch, request);
            const encoded = try std.json.Stringify.valueAlloc(a, reply, .{});
            errdefer a.free(encoded);
            if (encoded.len > 32 << 20) return error.CypherResponseTooLarge;
            return encoded;
        }
        fn selectGraph(self: *Self, a: std.mem.Allocator, request: cypher.rpc.Request) !cypher.catalog.Graph {
            _ = a;
            const index = for (self.graphs, 0..) |graph, index| {
                if (graph.incarnation == request.graph_incarnation and graph.revision == request.graph_revision) break index;
            } else return error.CatalogGenerationChanged;
            const selected = self.graphs[index];
            self.graph_index = index;
            self.graph = selected;
            self.adapter.graph = selected;
            try checkpoint(self);
            return selected;
        }
        fn invokeTyped(ptr: *anyopaque, a: std.mem.Allocator, temporary: std.mem.Allocator, input: cypher.rpc.Request) !cypher.rpc.Reply {
            // Build the owner arena in this runtime. Foreign arena vtables
            // cannot be peeled by the native adapter's per-document scratch.
            var arena = std.heap.ArenaAllocator.init(temporary);
            defer arena.deinit();
            const reply = try invokeTypedLocal(ptr, arena.allocator(), input);
            return reply.clone(a);
        }
        fn invokeTypedLocal(ptr: *anyopaque, a: std.mem.Allocator, input: cypher.rpc.Request) !cypher.rpc.Reply {
            const self = cast(ptr);
            try checkpoint(ptr);
            const selected = try self.selectGraph(a, input);
            const backend = self.adapter.backend();
            if (input.operation == .project) {
                const identity = input.identity orelse return error.CypherInvalidDocument;
                if (!std.mem.eql(u8, identity.table, self.input.table) or !std.mem.eql(u8, input.table, self.input.table)) return error.Forbidden;
                const document = input.document orelse return error.CypherInvalidDocument;
                if (!try self.db.permitsCypherDocument(a, document, input.json_null_fields, self.input.principal_proof, self.input.database))
                    return .{};
                if (self.input.filter_query_json.len != 0) {
                    var filter = try @import("../../search/pattern_filter.zig").PreparedPatternFilter.init(a, self.input.filter_query_json);
                    defer filter.deinit();
                    if (!try filter.matchesJson(a, identity.key, document))
                        return .{};
                }
                const value = try cypher.value.Value.fromJson(a, document);
                if (value != .map) return error.CypherInvalidDocument;
                const node: cypher.value.Node = .{ .identity = identity, .properties = try cypher.value.entityProperties(a, value.map) };
                var meter: cypher.backend.Meter = .{ .a = a, .backend = backend, .limit = input.max_work };
                self.adapter.work = meter.work();
                defer self.adapter.work = null;
                var membership = self.adapter.valueWork(a);
                var labels: std.ArrayList([]const u8) = .empty;
                for (selected.labels) |label| if (try selected.admits(&node, label.name, &membership)) try labels.append(a, label.name);
                const projected = try a.create(cypher.value.Node);
                projected.* = node;
                projected.labels = labels.items;
                if (!input.include_relationships) {
                    try checkpoint(ptr);
                    const values = try a.alloc(cypher.wire.Value, 1);
                    values[0] = .{ .value = .{ .node = projected } };
                    return .{ .work = meter.units, .values = values };
                }
                const derived = try self.db.cypherDocumentRelationships(a, selected, projected);
                const relationships = try a.alloc(cypher.wire.Value, derived.len);
                for (derived, relationships) |edge, *out| out.* = .{ .value = .{ .relationship = edge }, .include_native = true };
                try checkpoint(ptr);
                const values = try a.alloc(cypher.wire.Value, 1);
                values[0] = .{ .value = .{ .node = projected } };
                return .{ .work = meter.units, .values = values, .relationships = relationships };
            }
            if (input.operation == .normalize) {
                const identity = input.identity orelse return error.CypherInvalidDocument;
                if (!std.mem.eql(u8, identity.table, self.input.table) or !std.mem.eql(u8, input.table, self.input.table)) return error.Forbidden;
                const document = input.document orelse return error.CypherInvalidDocument;
                if (document != .object) return error.CypherInvalidDocument;
                const value = try cypher.value.Value.fromJson(a, document);
                const node = try a.create(cypher.value.Node);
                node.* = .{ .identity = identity, .properties = value.map };
                const normalized = try self.db.normalizeCypherNode(a, selected, node, input.apply_defaults);
                try checkpoint(ptr);
                const values = try a.alloc(cypher.wire.Value, 1);
                values[0] = .{ .value = .{ .node = normalized } };
                return .{ .values = values };
            }
            const result = try (cypher.rpc.Owner{ .backend = backend, .graph = selected, .table = self.input.table }).executeTyped(a, input);
            try checkpoint(ptr);
            return result;
        }
        pub fn openWriteObservation(self: *Self, a: std.mem.Allocator) !@import("../relational_read_view.zig").View {
            return openSql(self, a, self.input.table, "", "", .{ .include_range_proofs = true, .include_content_hashes = true, .sql_document_preimage = true, .relational_query = .{ .fields = &.{} } });
        }
        fn openSql(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, input: @import("types.zig").ScanOptions) !@import("../relational_read_view.zig").View {
            const self = cast(ptr);
            if (!std.mem.eql(u8, table, self.input.table)) return error.Forbidden;
            try checkpoint(ptr);
            var scratch = std.heap.ArenaAllocator.init(a);
            defer scratch.deinit();
            var opts = input;
            opts.row_policy_principal_proof = self.input.principal_proof;
            opts.row_policy_database = self.input.database;
            if (self.input.filter_query_json.len != 0) {
                if (opts.filter_query_json.len == 0) opts.filter_query_json = self.input.filter_query_json else opts.filter_query_json = try std.fmt.allocPrint(scratch.allocator(), "{{\"conjuncts\":[{s},{s}]}}", .{ opts.filter_query_json, self.input.filter_query_json });
            }
            const view = try @import("cypher_sql_view.zig").open(Self, DB, a, self, &self.db, from, to, opts);
            _ = self.references.fetchAdd(1, .monotonic);
            return view;
        }
        pub fn sqlCheckpoint(self: *Self) !void {
            try checkpoint(self);
        }
        pub fn releaseSql(self: *Self) void {
            release(self);
        }
        fn release(ptr: *anyopaque) void {
            const self = cast(ptr);
            if (self.references.fetchSub(1, .acq_rel) != 1) return;
            self.db.close();
            self.source.releaseStatementQueryCut(.{ .id = &self.input.cut_id, .table_id = self.input.table_id, .expires_ms = self.input.expires_ms });
            self.policy.release();
            self.arena.deinit();
            self.a.destroy(self);
        }
    };
}
