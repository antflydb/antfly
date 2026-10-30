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

//! Retains prepared owner routes and leased private graph channels for one
//! statement. Routing plans are built before read-index/capture work begins.
const std = @import("std");
const cypher = @import("cypher");
const local = @import("antfly_local_sources");
const reads = local.api_table_read_source;
const port = local.storage_statement_read_fence;
const metadata = @import("../metadata/api.zig");
const time = @import("antfly_platform").time;
const http = local.common_http_http_common;
pub const Plan = struct {
    table: port.GraphTable,
    fence: metadata.CatalogRouteFence,
    ranges: []const cypher.distributed.KeyRange = &.{},
    route: union(enum) {
        local: reads.TableReadSource,
        remote: struct { executor: http.RequestExecutor, base_uri: []const u8 },
    },
};
pub const Statement = struct {
    a: std.mem.Allocator,
    shared: ?cypher.backend.FanoutAllocator = null,
    arena: std.heap.ArenaAllocator,
    input: port.GraphStatement,
    graph: cypher.catalog.Graph,
    graphs: []const cypher.catalog.Graph,
    versions: []const cypher.rpc.Version,
    plans: []const Plan,
    set: cypher.distributed.ReadSet,
    pub fn create(a: std.mem.Allocator, input: port.GraphStatement, plans: []const Plan, io: ?std.Io, width: usize) !port.GraphRead {
        const self = try a.create(Statement);
        errdefer a.destroy(self);
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const owned = arena.allocator();
        const record = try std.json.parseFromSliceLeaky(local.system_catalog_domain.Resource, owned, input.catalog_json, .{ .allocate = .alloc_always });
        const records = if (input.catalogs_json.len == 0) try owned.dupe(local.system_catalog_domain.Resource, &.{record}) else try std.json.parseFromSliceLeaky([]const local.system_catalog_domain.Resource, owned, input.catalogs_json, .{ .allocate = .alloc_always });
        if (records.len == 0 or records.len > 64) return error.CypherOwnerBudgetExceeded;
        const graphs = try owned.alloc(cypher.catalog.Graph, records.len);
        const versions = try owned.alloc(cypher.rpc.Version, records.len);
        for (records, graphs, versions, 0..) |entry, *graph, *version, index| {
            for (records[0..index]) |prior| if (prior.id == entry.id) return error.CypherInvalidOwner;
            graph.* = try local.system_catalog_domain.graphForExecution(owned, entry);
            version.* = .{ .incarnation = graph.incarnation, .revision = graph.revision };
        }
        const copied = try owned.alloc(Plan, plans.len);
        for (plans, copied) |plan, *out| {
            out.* = plan;
            out.table = try std.json.parseFromSliceLeaky(port.GraphTable, owned, try std.json.Stringify.valueAlloc(owned, plan.table, .{}), .{ .allocate = .alloc_always });
            const ranges = try owned.alloc(cypher.distributed.KeyRange, plan.ranges.len);
            for (plan.ranges, ranges) |range, *copy| copy.* = .{ .start = try owned.dupe(u8, range.start), .end = if (range.end) |end| try owned.dupe(u8, end) else null };
            out.ranges = ranges;
            out.fence.route = try std.json.parseFromSliceLeaky(@TypeOf(plan.fence.route), owned, try std.json.Stringify.valueAlloc(owned, plan.fence.route, .{}), .{ .allocate = .alloc_always });
            out.fence.admission_deadline_ns = input.deadline_ns;
            out.fence.admission_deadline_io = null;
            out.fence.admission_cancellation = input.cancellation;
            switch (out.route) {
                .local => |*source| source.route_fence = out.fence,
                .remote => |*remote| remote.base_uri = try owned.dupe(u8, remote.base_uri),
            }
        }
        var owned_input = input;
        owned_input.catalog_json = try owned.dupe(u8, input.catalog_json);
        owned_input.catalogs_json = try owned.dupe(u8, input.catalogs_json);
        // Table authorization bytes are retained in copied plans; the full
        // inventory is no longer needed after routes have been prepared.
        owned_input.tables = &.{};
        self.* = .{ .a = a, .arena = arena, .input = owned_input, .graph = try local.system_catalog_domain.graphForExecution(owned, record), .graphs = graphs, .versions = versions, .plans = copied, .set = undefined };
        // Capabilities retain this allocator beyond acquisition. Its lock
        // serializes caller-arena access without serializing owner RPCs.
        if (io) |runtime_io| {
            self.shared = .{ .backing = a, .io = runtime_io };
            self.a = self.shared.?.allocator();
        }
        const targets = try owned.alloc(cypher.distributed.Target, plans.len);
        for (copied, targets) |plan, *target| target.* = .{ .parallel = plan.route == .remote, .table = plan.table.name, .table_id = plan.table.table_id, .group_id = plan.fence.route.group_id, .ranges = plan.ranges };
        self.set = try cypher.distributed.ReadSet.acquire(self.a, .{ .fanout_io = io, .fanout_width = width, .ptr = self, .checkpoint = checkpoint, .admit = admit, .capture = capture }, targets);
        self.set.partial_graph = input.partial_graph;
        errdefer self.set.deinit();
        const backend = self.set.backend();
        try backend.vtable.bind(backend.ptr, owned, self.graph);
        self.arena = arena;
        return .{ .ptr = self, .vtable = &.{ .invoke = invoke, .invoke_typed = invokeTyped, .release = close, .open_sql = openSql } };
    }
    fn cast(ptr: *anyopaque) *Statement {
        return @ptrCast(@alignCast(ptr));
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self = cast(ptr);
        try self.input.cancellation.check();
        if (self.input.deadline_ns) |deadline| if (time.monotonicNs() >= deadline) return error.DeadlineExceeded;
        if (time.realtimeNs() / std.time.ns_per_ms >= self.input.expires_ms) return error.DeadlineExceeded;
    }
    fn planFor(self: *Statement, target: cypher.distributed.Target) !Plan {
        for (self.plans) |plan| if (plan.table.table_id == target.table_id and plan.fence.route.group_id == target.group_id and std.mem.eql(u8, plan.table.name, target.table)) return plan;
        return error.CatalogGenerationChanged;
    }
    fn admit(ptr: *anyopaque, target: cypher.distributed.Target) !void {
        const self = cast(ptr);
        const plan = try self.planFor(target);
        switch (plan.route) {
            .local => |source| {
                if (try source.lookupGroupLocal(self.a, target.group_id, target.table, "", .{ .cypher_prepare = true, .execution_deadline_ns = self.input.deadline_ns, .cancellation = self.input.cancellation }, .read_index)) |response| {
                    var owned = response;
                    owned.deinit(self.a);
                }
            },
            .remote => |remote| try @import("retained_read_client.zig").Client.admitGraph(self.a, remote.executor, remote.base_uri, target.table, plan.fence, plan.table.schema_version, self.input.deadline_ns),
        }
    }
    const Capture = struct {
        statement: *Statement,
        fence: reads.StatementReadFence,
        plan: Plan,
        read: ?port.GraphRead = null,
        fn validate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (!self.fence.graph_finalizes_validation) try self.fence.validate();
            const read = self.read orelse return error.CypherSnapshotNotReady;
            var arena = std.heap.ArenaAllocator.init(self.statement.a);
            defer arena.deinit();
            const a = arena.allocator();
            const request = try std.json.Stringify.valueAlloc(a, cypher.rpc.Request{ .operation = .validate, .graph_incarnation = self.statement.graph.incarnation, .graph_revision = self.statement.graph.revision }, .{});
            const response = try read.invoke(a, request);
            a.free(response);
        }
        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.fence.deinit();
            self.statement.a.destroy(self);
        }
        fn fork(ptr: *anyopaque, a: std.mem.Allocator) !cypher.distributed.Snapshot {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const statement = self.statement;
            // Each owner receives a distinct cut name even when two tables
            // happen to share a physical group or namespace implementation.
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            hash.update(&statement.input.cut_id);
            var identity: [16]u8 = undefined;
            std.mem.writeInt(u64, identity[0..8], self.plan.table.table_id, .little);
            std.mem.writeInt(u64, identity[8..16], self.plan.fence.route.group_id, .little);
            hash.update(&identity);
            var digest: [32]u8 = undefined;
            hash.final(&digest);
            const read = try self.fence.captureGraph(a, .{ .defer_validation = self.fence.graph_finalizes_validation, .indexes = self.plan.table.indexes, .catalog_json = statement.input.catalog_json, .catalogs_json = statement.input.catalogs_json, .table = self.plan.table.name, .table_id = self.plan.table.table_id, .schema_version = self.plan.table.schema_version, .cut_id = std.fmt.bytesToHex(digest, .lower), .expires_ms = statement.input.expires_ms, .ttl_now_ns = statement.input.ttl_now_ns, .principal_proof = self.plan.table.principal_proof, .database = self.plan.table.database, .filter_query_json = self.plan.table.filter_query_json });
            errdefer read.deinit();
            self.read = read;
            errdefer self.read = null;
            const snapshot = try a.create(Owner);
            snapshot.* = .{ .a = a, .statement = statement, .read = read, .client = undefined, .routing_arena = .init(a), .table = self.plan.table.name };
            errdefer a.destroy(snapshot);
            errdefer snapshot.routing_arena.deinit();
            snapshot.client = .{ .channel = .{ .ptr = snapshot, .call = Owner.call, .typed = Owner.callTyped }, .incarnation = statement.graph.incarnation, .revision = statement.graph.revision, .allowed_versions = statement.versions };
            const adjacency_needed = if (self.plan.table.indexes) |indexes| indexes.len != 0 else statement.graph.relationships.len != 0;
            return .{ .load_adjacency_routes = if (adjacency_needed) Owner.loadAdjacencyRoutes else null, .backend = snapshot.client.backend(), .ptr = snapshot, .validate = Owner.validate, .close = Owner.close, .validation_covered_by_source = true };
        }
    };
    const Owner = struct {
        a: std.mem.Allocator,
        statement: *Statement,
        read: port.GraphRead,
        client: cypher.rpc.Client,
        routing_arena: std.heap.ArenaAllocator,
        table: []const u8,
        fn loadAdjacencyRoutes(ptr: *anyopaque) ![]const cypher.backend.AdjacencyRoute {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const routing = try callTyped(ptr, self.routing_arena.allocator(), .{ .operation = .adjacency_routes, .table = self.table, .graph_incarnation = self.statement.graph.incarnation, .graph_revision = self.statement.graph.revision });
            try cypher.backend.validateAdjacencyRoutes(routing.adjacency_routes);
            return routing.adjacency_routes;
        }
        fn validate(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try checkpoint(self.statement);
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.read.deinit();
            self.routing_arena.deinit();
            self.a.destroy(self);
        }
        fn call(ptr: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try validate(ptr);
            return self.invoke(a, body);
        }
        fn callTyped(ptr: *anyopaque, a: std.mem.Allocator, request: cypher.rpc.Request) !cypher.rpc.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try validate(ptr);
            if (self.read.vtable.invoke_typed == null) {
                // Legacy local owners still need the reclaimable wire path;
                // forwarding a foreign reply arena would retain row scratch.
                const body = try std.json.Stringify.valueAlloc(a, request, .{});
                defer a.free(body);
                if (body.len > 1 << 20) return error.CypherRequestTooLarge;
                const response = try self.invoke(a, body);
                defer a.free(response);
                if (response.len > 32 << 20) return error.CypherResponseTooLarge;
                return std.json.parseFromSliceLeaky(cypher.rpc.Reply, a, response, .{ .allocate = .alloc_always });
            }
            return self.read.invokeTyped(a, request);
        }
        fn invoke(self: *@This(), a: std.mem.Allocator, body: []const u8) ![]u8 {
            // Arena vtables have different addresses across compiled libraries.
            // Peel caller arenas before the owner boundary, so owner scratch
            // can release quota as it decodes each document. Transfer only the
            // private JSON response back into the caller's requested allocator.
            const temporary = cypher.backend.reclaimingAllocator(a);
            if (temporary.ptr == a.ptr and temporary.vtable == a.vtable) return self.read.invoke(a, body);
            const response = try self.read.invoke(temporary, body);
            defer temporary.free(response);
            return a.dupe(u8, response);
        }
    };
    fn capture(ptr: *anyopaque, a: std.mem.Allocator, target: cypher.distributed.Target) !?cypher.distributed.Fence {
        const self = cast(ptr);
        const plan = try self.planFor(target);
        const fence = (try switch (plan.route) {
            .local => |source| source.tryStatementReadFenceGroupLocal(a, target.group_id, target.table, .{ .execution_deadline_ns = self.input.deadline_ns, .cancellation = self.input.cancellation }, .stale),
            .remote => |remote| @import("retained_read_client.zig").Client.capture(a, remote.executor, remote.base_uri, target.table, plan.fence, plan.table.schema_version, self.input.deadline_ns),
        }) orelse return null;
        errdefer fence.deinit();
        const owned = try a.create(Capture);
        owned.* = .{ .statement = self, .fence = fence, .plan = plan };
        return .{ .ptr = owned, .validate = Capture.validate, .fork = Capture.fork, .release = Capture.release };
    }
    fn invoke(ptr: *anyopaque, a: std.mem.Allocator, body: []const u8) ![]u8 {
        const self = cast(ptr);
        try checkpoint(ptr);
        var arena = std.heap.ArenaAllocator.init(cypher.backend.reclaimingAllocator(a));
        defer arena.deinit();
        const header = try std.json.parseFromSliceLeaky(struct { graph_incarnation: u64, graph_revision: u64, operation: []const u8 }, arena.allocator(), body, .{ .ignore_unknown_fields = true });
        const graph = for (self.graphs) |entry| {
            if (entry.incarnation == header.graph_incarnation and entry.revision == header.graph_revision) break entry;
        } else return error.CatalogGenerationChanged;
        if (std.mem.eql(u8, header.operation, "normalize") or std.mem.eql(u8, header.operation, "project")) {
            const request = try std.json.parseFromSliceLeaky(cypher.rpc.Request, arena.allocator(), body, .{});
            const permitted = for (graph.labels) |label| {
                if (std.mem.eql(u8, label.table, request.table)) break true;
            } else false;
            if (!permitted and !std.mem.eql(u8, header.operation, "project")) return error.Forbidden;
            for (self.set.targets, self.set.snapshots) |target, snapshot| {
                if (!std.mem.eql(u8, target.table, request.table)) continue;
                const owner: *Owner = @ptrCast(@alignCast(snapshot.ptr));
                return owner.invoke(a, body);
            }
            return error.CypherInvalidOwner;
        }
        if (std.mem.eql(u8, header.operation, "observe") or std.mem.eql(u8, header.operation, "observe_ranges")) {
            const observation = local.storage_db_cypher_write_observation;
            var owners: std.ArrayList(observation.Owner) = .empty;
            for (self.set.targets, self.set.snapshots) |target, snapshot| {
                const owner: *Owner = @ptrCast(@alignCast(snapshot.ptr));
                const bytes = try owner.invoke(arena.allocator(), body);
                const cut = try std.json.parseFromSliceLeaky(observation.Cut, arena.allocator(), bytes, .{});
                const plan = try self.planFor(target);
                try owners.append(arena.allocator(), .{ .table = target.table, .fence = plan.fence, .predicates = cut.predicates, .proofs = cut.proofs });
            }
            return std.json.Stringify.valueAlloc(a, owners.items, .{});
        }
        const backend = self.set.backend();
        try backend.vtable.bind(backend.ptr, arena.allocator(), graph);
        return (cypher.rpc.Owner{ .backend = backend, .graph = graph, .table = "" }).execute(a, body);
    }
    fn invokeTyped(ptr: *anyopaque, a: std.mem.Allocator, scratch: std.mem.Allocator, request: cypher.rpc.Request) !cypher.rpc.Reply {
        const local_scratch = cypher.backend.reclaimingAllocator(a);
        if (local_scratch.ptr == scratch.ptr and local_scratch.vtable == scratch.vtable) return invokeTypedLocal(ptr, a, request);
        // Hosted callers may also belong to an independent runtime. Preserve
        // their scratch capability without making local callers copy twice.
        var arena = std.heap.ArenaAllocator.init(scratch);
        defer arena.deinit();
        return (try invokeTypedLocal(ptr, arena.allocator(), request)).clone(a);
    }
    fn invokeTypedLocal(ptr: *anyopaque, a: std.mem.Allocator, request: cypher.rpc.Request) !cypher.rpc.Reply {
        const self = cast(ptr);
        try checkpoint(ptr);
        const graph = for (self.graphs) |entry| {
            if (entry.incarnation == request.graph_incarnation and entry.revision == request.graph_revision) break entry;
        } else return error.CatalogGenerationChanged;
        switch (request.operation) {
            .normalize, .project => {
                const permitted = for (graph.labels) |label| {
                    if (std.mem.eql(u8, label.table, request.table)) break true;
                } else false;
                if (!permitted and request.operation != .project) return error.Forbidden;
                for (self.set.targets, self.set.snapshots) |target, snapshot| {
                    if (!std.mem.eql(u8, target.table, request.table)) continue;
                    return Owner.callTyped(snapshot.ptr, a, request);
                }
                return error.CypherInvalidOwner;
            },
            .observe, .observe_ranges => return error.CypherObservationsUnavailable,
            else => {},
        }
        const backend = self.set.backend();
        try backend.vtable.bind(backend.ptr, a, graph);
        return (cypher.rpc.Owner{ .backend = backend, .graph = graph, .table = "" }).executeTyped(a, request);
    }
    fn openSql(ptr: *anyopaque, a: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, opts: local.storage_db_types.ScanOptions) !reads.RelationalReadView {
        const self = cast(ptr);
        try checkpoint(ptr);
        var views: std.ArrayList(reads.RelationalReadView) = .empty;
        defer views.deinit(a);
        errdefer for (views.items) |view| view.deinit();
        for (self.set.targets, self.set.snapshots) |target, snapshot| {
            if (!std.mem.eql(u8, target.table, table)) continue;
            if (views.items.len == 256) return error.SqlProgramLimitExceeded;
            const owner: *Owner = @ptrCast(@alignCast(snapshot.ptr));
            const view = try owner.read.openSql(a, table, from, to, opts);
            views.append(a, view) catch |err| {
                view.deinit();
                return err;
            };
        }
        if (views.items.len == 0) return error.UndefinedGraphTable;
        const result = try @import("../storage/relational_read_set.zig").Set.create(a, views.items, true);
        views.items.len = 0;
        return result;
    }
    fn close(ptr: *anyopaque) void {
        const self = cast(ptr);
        self.set.deinit();
        self.arena.deinit();
        const backing = if (self.shared) |shared| shared.backing else self.a;
        backing.destroy(self);
    }
};

test "graph admission batches preserve reclaimable allocators for legacy owner callbacks" {
    const Legacy = struct {
        fn invoke(_: *anyopaque, a: std.mem.Allocator, _: []const u8) ![]u8 {
            var probe = std.heap.ArenaAllocator.init(a);
            defer probe.deinit();
            try std.testing.expect(a.vtable != probe.allocator().vtable);
            return std.json.Stringify.valueAlloc(a, cypher.rpc.Reply{ .work = 3 }, .{});
        }
        fn release(_: *anyopaque) void {}
    };
    var statement: Statement = undefined;
    statement.input = .{ .catalog_json = "", .tables = &.{}, .cut_id = @splat('a'), .expires_ms = std.math.maxInt(u64), .ttl_now_ns = 1 };
    var marker: u8 = 0;
    var owner: Statement.Owner = .{ .a = std.testing.allocator, .statement = &statement, .read = .{ .ptr = &marker, .vtable = &.{ .invoke = Legacy.invoke, .release = Legacy.release } }, .client = undefined, .routing_arena = .init(std.testing.allocator), .table = "docs" };
    defer owner.routing_arena.deinit();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const reply = try Statement.Owner.callTyped(&owner, arena.allocator(), .{ .operation = .validate, .graph_incarnation = 1, .graph_revision = 1 });
    try std.testing.expectEqual(@as(usize, 3), reply.work);
}
