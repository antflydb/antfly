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

//! Owner data plane. Transport creates channels from authenticated, leased
//! catalog capabilities; public callers never supply these channels or cuts.
const std = @import("std");
const b = @import("backend.zig");
const v = @import("value.zig");
const wire = @import("wire.zig");
const ast = @import("ast.zig");
const catalog = @import("catalog.zig");
const A = std.mem.Allocator;
pub const Request = struct {
    operation: enum { validate, scan, scan_ordered, estimate, get, get_many, expand, search, observe, observe_ranges, normalize, project },
    graph_incarnation: u64,
    graph_revision: u64,
    table: []const u8 = "",
    identity: ?v.Identity = null,
    peer: ?v.Identity = null,
    identities: []const v.Identity = &.{},
    relationship: []const u8 = "",
    direction: ast.Direction = .out,
    filters: []const struct { property: []const u8, value: std.json.Value, op: b.FilterOp = .eq } = &.{},
    order: []const b.PropertyOrder = &.{},
    after: ?[]const u8 = null,
    limit: usize = 128,
    search: ?std.json.Value = null,
    prepared_search_json: []const u8 = "",
    max_examined: usize = 1_000_000,
    max_work: usize = 1_000_000,
    read_fields: ?[]const []const u8 = null,
    document: ?std.json.Value = null,
    apply_defaults: bool = false,
    json_null_fields: []const []const u8 = &.{},
    include_relationships: bool = false,
};
pub const Projection = struct { node: ?*const v.Node, relationships: []const *const v.Relationship = &.{} };
const Reply = struct { cursors: ?[]const ?[]const u8 = null, ordered: bool = false, estimate: ?usize = null, unfiltered: bool = false, work: usize = 0, relationships: []const std.json.Value = &.{}, values: []const std.json.Value = &.{}, next: ?[]const u8 = null, examined: ?usize = null, scores: []const f64 = &.{}, approximate: bool = false };
pub const Channel = struct { ptr: *anyopaque, call: *const fn (*anyopaque, A, []const u8) anyerror![]u8, checkpoint: ?*const fn (*anyopaque) anyerror!void = null };
pub const Version = struct { incarnation: u64, revision: u64 };
pub const Client = struct {
    channel: Channel,
    incarnation: u64,
    revision: u64,
    allowed_versions: []const Version = &.{},
    work: ?b.Work = null,
    read_fields: ?[]const []const u8 = null,
    pub fn backend(self: *Client) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .scan_where = scanWhere, .scan_ordered = scanOrdered, .estimate_where = estimateWhere, .get = get, .get_many = getMany, .expand = expand, .expand_between = expandBetween, .search = search, .normalize_node = normalizeNode, .project_postimage = projectPostimage, .document_relationships = documentRelationships, .set_work = setWork, .set_read_fields = setReadFields } };
    }
    fn setReadFields(ptr: *anyopaque, fields: ?[]const []const u8) void {
        cast(ptr).read_fields = fields;
    }
    fn setWork(ptr: *anyopaque, work: ?b.Work) void {
        cast(ptr).work = work;
    }
    fn cast(ptr: *anyopaque) *Client {
        return @ptrCast(@alignCast(ptr));
    }
    fn invoke(self: *Client, a: A, request: Request) !Reply {
        var bounded = request;
        bounded.read_fields = self.read_fields;
        if (self.work) |work| if (work.remaining) |remaining| {
            bounded.max_work = remaining(work.ptr);
        };
        const body = try std.json.Stringify.valueAlloc(a, bounded, .{});
        defer a.free(body);
        if (body.len > 1 << 20) return error.CypherRequestTooLarge;
        const response = try self.channel.call(self.channel.ptr, a, body);
        defer a.free(response);
        if (response.len > 32 << 20) return error.CypherResponseTooLarge;
        const reply = try std.json.parseFromSliceLeaky(Reply, a, response, .{ .allocate = .alloc_always });
        if (self.work) |work| try work.charge(work.ptr, reply.work);
        return reply;
    }
    fn bind(ptr: *anyopaque, _: A, graph: catalog.Graph) !void {
        const self = cast(ptr);
        if (graph.incarnation != self.incarnation or graph.revision != self.revision) {
            const allowed = for (self.allowed_versions) |version| {
                if (graph.incarnation == version.incarnation and graph.revision == version.revision) break true;
            } else false;
            if (!allowed) return error.CatalogGenerationChanged;
            self.incarnation = graph.incarnation;
            self.revision = graph.revision;
        }
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (self.channel.checkpoint) |callback| try callback(self.channel.ptr);
    }
    fn normalizeNode(ptr: *anyopaque, a: A, node: *const v.Node, apply_defaults: bool) !*const v.Node {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const document = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, v.Value{ .map = node.properties }, .{}), .{ .parse_numbers = false });
        const reply = try self.invoke(temporary, .{ .operation = .normalize, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = node.identity.table, .identity = node.identity, .document = document, .apply_defaults = apply_defaults });
        if (reply.values.len != 1) return error.CypherInvalidBackendPage;
        const value = try wire.transport(a, reply.values[0]);
        if (value != .node or !value.node.identity.eql(node.identity)) return error.CypherInvalidBackendPage;
        return value.node;
    }
    fn projectPostimage(ptr: *anyopaque, a: A, node: *const v.Node) !?*const v.Node {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const document = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, v.Value{ .map = node.properties }, .{}), .{ .parse_numbers = false });
        return (try self.projectDocument(a, node.identity, document, &.{}, false)).node;
    }
    pub fn projectDocument(self: *Client, a: A, identity: v.Identity, document: std.json.Value, json_null_fields: []const []const u8, include_relationships: bool) !Projection {
        // Retain canonical entities, not JSON request/response scaffolding for
        // every postimage in a large mutation statement.
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const reply = try self.invoke(scratch.allocator(), .{ .operation = .project, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = identity.table, .identity = identity, .document = document, .json_null_fields = json_null_fields, .include_relationships = include_relationships });
        if (reply.values.len == 0) return .{ .node = null };
        if (reply.values.len != 1) return error.CypherInvalidBackendPage;
        const value = try wire.transport(a, reply.values[0]);
        if (value != .node or !value.node.identity.eql(identity)) return error.CypherInvalidBackendPage;
        const relationships = try a.alloc(*const v.Relationship, reply.relationships.len);
        for (reply.relationships, relationships) |item, *out| {
            const decoded = try wire.transport(a, item);
            if (decoded != .relationship or !std.mem.eql(u8, decoded.relationship.identity.table, identity.table)) return error.CypherInvalidBackendPage;
            out.* = decoded.relationship;
        }
        return .{ .node = value.node, .relationships = relationships };
    }
    fn documentRelationships(ptr: *anyopaque, a: A, node: *const v.Node) ![]const *const v.Relationship {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const document = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, v.Value{ .map = node.properties }, .{}), .{ .parse_numbers = false });
        return (try self.projectDocument(a, node.identity, document, &.{}, true)).relationships;
    }
    fn scan(ptr: *anyopaque, a: A, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        return scanWhere(ptr, a, table, &.{}, after, limit);
    }
    fn estimateWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) !?usize {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const encoded = try std.json.parseFromSliceLeaky(@FieldType(Request, "filters"), temporary, try std.json.Stringify.valueAlloc(temporary, filters, .{}), .{ .parse_numbers = false });
        const reply = try self.invoke(temporary, .{ .filters = encoded, .operation = .estimate, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .limit = limit });
        if (reply.estimate) |upper| if (upper > limit) return error.CypherInvalidBackendPage;
        return reply.estimate;
    }
    fn scanOrdered(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, max_examined: usize) !?b.Page(*const v.Node) {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const encoded_filters = try std.json.parseFromSliceLeaky(@FieldType(Request, "filters"), temporary, try std.json.Stringify.valueAlloc(temporary, filters, .{}), .{ .parse_numbers = false });
        const reply = try self.invoke(temporary, .{ .filters = encoded_filters, .order = order, .operation = .scan_ordered, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .after = after, .limit = limit, .max_examined = max_examined });
        if (!reply.ordered) {
            if (reply.values.len != 0 or reply.cursors != null or reply.next != null or after != null) return error.CypherInvalidBackendPage;
            return null;
        }
        const nodes = try a.alloc(*const v.Node, reply.values.len);
        for (reply.values, nodes) |input, *out| {
            const value = try wire.transport(a, input);
            if (value != .node or !std.mem.eql(u8, value.node.identity.table, table)) return error.CypherInvalidBackendPage;
            out.* = value.node;
        }
        const cursors = if (reply.cursors) |positions| blk: {
            if (positions.len != nodes.len) return error.CypherInvalidBackendPage;
            const saved = try a.alloc(?[]const u8, positions.len);
            for (positions, saved) |position, *out| out.* = if (position) |bytes| try a.dupe(u8, bytes) else null;
            break :blk saved;
        } else null;
        return .{ .items = nodes, .cursors = cursors, .next = if (reply.next) |next| try a.dupe(u8, next) else null, .examined = reply.examined };
    }
    fn scanWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const encoded_filters = try std.json.parseFromSliceLeaky(@FieldType(Request, "filters"), temporary, try std.json.Stringify.valueAlloc(temporary, filters, .{}), .{ .parse_numbers = false });
        const reply = try self.invoke(temporary, .{ .filters = encoded_filters, .operation = .scan, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .after = after, .limit = limit });
        const nodes = try a.alloc(*const v.Node, reply.values.len);
        for (reply.values, nodes) |input, *out| {
            const value = try wire.transport(a, input);
            if (value != .node or !std.mem.eql(u8, value.node.identity.table, table)) return error.CypherInvalidBackendPage;
            out.* = value.node;
        }
        return .{ .items = nodes, .next = if (reply.next) |next| try a.dupe(u8, next) else null, .examined = reply.examined, .unfiltered = reply.unfiltered };
    }
    fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const reply = try self.invoke(temporary, .{ .operation = .get, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .identity = identity });
        if (reply.values.len == 0) return null;
        if (reply.values.len != 1) return error.CypherInvalidBackendPage;
        const value = try wire.transport(a, reply.values[0]);
        if (value != .node or !value.node.identity.eql(identity)) return error.CypherInvalidBackendPage;
        return value.node;
    }
    fn getMany(ptr: *anyopaque, a: A, identities: []const v.Identity) ![]const ?*const v.Node {
        if (identities.len > 128) return error.CypherOwnerBudgetExceeded;
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const reply = try self.invoke(temporary, .{ .operation = .get_many, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .identities = identities });
        if (reply.values.len != identities.len) return error.CypherInvalidBackendPage;
        const nodes = try a.alloc(?*const v.Node, identities.len);
        for (reply.values, identities, nodes) |input, identity, *out| {
            const value = try wire.transport(a, input);
            if (value == .null) {
                out.* = null;
                continue;
            }
            if (value != .node or !value.node.identity.eql(identity)) return error.CypherInvalidBackendPage;
            out.* = value.node;
        }
        return nodes;
    }
    fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const request = input.parameters;
        if (request != .map) return error.CypherTypeMismatch;
        const encoded = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, wire.Value{ .value = request }, .{}), .{});
        const reply = try self.invoke(temporary, .{ .operation = .search, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .search = encoded, .prepared_search_json = input.prepared_json, .max_examined = input.max_examined });
        if (reply.values.len != reply.scores.len or reply.values.len > 1024) return error.CypherInvalidBackendPage;
        const seeds = try a.alloc(b.Seed, reply.values.len);
        for (reply.values, reply.scores, seeds) |encoded_value, score, *out| {
            const value = try wire.transport(a, encoded_value);
            if (value != .node or !std.mem.eql(u8, value.node.identity.table, table) or !std.math.isFinite(score)) return error.CypherInvalidBackendPage;
            out.* = .{ .node = value.node, .score = score };
        }
        return .{ .seeds = seeds, .examined = reply.examined orelse return error.CypherInvalidSearchResult, .approximate = reply.approximate };
    }
    fn expand(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, direction: ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
        return expandPair(ptr, a, mapping, identity, null, direction, after, limit, 1_000_000);
    }
    fn expandBetween(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, peer: v.Identity, direction: ast.Direction, _: []const b.PropertyFilter, after: ?[]const u8, limit: usize, max_examined: usize) !b.Page(*const v.Relationship) {
        return expandPair(ptr, a, mapping, identity, peer, direction, after, limit, max_examined);
    }
    fn expandPair(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, peer: ?v.Identity, direction: ast.Direction, after: ?[]const u8, limit: usize, max_examined: usize) !b.Page(*const v.Relationship) {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const reply = try self.invoke(temporary, .{ .operation = .expand, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .identity = identity, .peer = peer, .max_examined = max_examined, .relationship = mapping.name, .direction = direction, .after = after, .limit = limit });
        const relationships = try a.alloc(*const v.Relationship, reply.values.len);
        for (reply.values, relationships) |input, *out| {
            const value = try wire.transport(a, input);
            if (value != .relationship or !std.mem.eql(u8, value.relationship.type, mapping.name) or !std.mem.eql(u8, value.relationship.index, mapping.index)) return error.CypherInvalidBackendPage;
            out.* = value.relationship;
        }
        return .{ .items = relationships, .next = if (reply.next) |next| try a.dupe(u8, next) else null, .examined = reply.examined };
    }
};
pub const Owner = struct {
    backend: b.Backend,
    graph: catalog.Graph,
    table: []const u8,
    fn permitsTable(self: Owner, table: []const u8) bool {
        if (self.table.len != 0) return std.mem.eql(u8, self.table, table);
        for (self.graph.labels) |label| if (std.mem.eql(u8, label.table, table)) return true;
        return false;
    }
    pub fn execute(self: Owner, a: A, body: []const u8) ![]u8 {
        if (body.len > 1 << 20) return error.CypherRequestTooLarge;
        var arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer arena.deinit();
        const scratch = arena.allocator();
        const request = try std.json.parseFromSliceLeaky(Request, scratch, body, .{});
        if (self.backend.vtable.set_read_fields) |configure| configure(self.backend.ptr, request.read_fields);
        defer if (self.backend.vtable.set_read_fields) |configure| configure(self.backend.ptr, null);
        if (request.graph_incarnation != self.graph.incarnation or request.graph_revision != self.graph.revision) return error.CatalogGenerationChanged;
        if ((request.limit == 0 and request.operation != .scan_ordered) or request.limit > 1024) return error.CypherOwnerBudgetExceeded;
        try self.backend.vtable.checkpoint(self.backend.ptr);
        var meter: b.Meter = .{ .a = scratch, .backend = self.backend, .limit = request.max_work };
        if (self.backend.vtable.set_work) |configure| configure(self.backend.ptr, meter.work());
        defer if (self.backend.vtable.set_work) |configure| configure(self.backend.ptr, null);
        var values: std.ArrayList(wire.Value) = .empty;
        var next: ?[]const u8 = null;
        var examined: ?usize = null;
        var unfiltered = false;
        var scores: std.ArrayList(f64) = .empty;
        var approximate = false;
        switch (request.operation) {
            .validate => {},
            .observe, .observe_ranges => return error.CypherObservationsUnavailable,
            .project => return error.CypherStagedProjectionUnavailable,
            .normalize => {
                const identity = request.identity orelse return error.CypherInvalidBackendPage;
                if (!self.permitsTable(identity.table)) return error.Forbidden;
                const document = try v.Value.fromJson(scratch, request.document orelse return error.CypherInvalidDocument);
                if (document != .map) return error.CypherInvalidDocument;
                const node: v.Node = .{ .identity = identity, .properties = document.map };
                const normalized = try (self.backend.vtable.normalize_node orelse return error.CypherInvalidDocument)(self.backend.ptr, scratch, &node, request.apply_defaults);
                try values.append(scratch, .{ .value = .{ .node = normalized } });
            },
            .estimate => {
                if (!self.permitsTable(request.table) or request.limit > 128) return error.Forbidden;
                const filters = try scratch.alloc(b.PropertyFilter, request.filters.len);
                for (request.filters, filters) |input, *filter| filter.* = .{ .property = input.property, .value = try v.Value.fromJson(scratch, input.value), .op = input.op };
                const upper = if (self.backend.vtable.estimate_where) |estimate| try estimate(self.backend.ptr, scratch, request.table, filters, request.limit) else null;
                try self.backend.vtable.checkpoint(self.backend.ptr);
                return std.json.Stringify.valueAlloc(a, .{ .work = meter.units, .estimate = upper }, .{});
            },
            .scan_ordered => {
                if (!self.permitsTable(request.table) or request.order.len > 32) return error.Forbidden;
                const filters = try scratch.alloc(b.PropertyFilter, request.filters.len);
                for (request.filters, filters) |input, *filter| filter.* = .{ .property = input.property, .value = try v.Value.fromJson(scratch, input.value), .op = input.op };
                const page = if (self.backend.vtable.scan_ordered) |read| try read(self.backend.ptr, scratch, request.table, filters, request.order, request.after, request.limit, request.max_examined) else null;
                if (page) |ordered| {
                    for (ordered.items) |node| try values.append(scratch, .{ .value = .{ .node = node } });
                    return std.json.Stringify.valueAlloc(a, .{ .ordered = true, .values = values.items, .cursors = ordered.cursors, .next = ordered.next, .examined = ordered.examined, .work = meter.units }, .{});
                }
                return std.json.Stringify.valueAlloc(a, .{ .ordered = false, .work = meter.units }, .{});
            },
            .scan => {
                if (!self.permitsTable(request.table)) return error.Forbidden;
                const filters = try scratch.alloc(b.PropertyFilter, request.filters.len);
                for (request.filters, filters) |input, *filter| filter.* = .{ .property = input.property, .value = try v.Value.fromJson(scratch, input.value), .op = input.op };
                const page = if (self.backend.vtable.scan_where) |filtered| try filtered(self.backend.ptr, scratch, request.table, filters, request.after, request.limit) else try self.backend.vtable.scan(self.backend.ptr, scratch, request.table, request.after, request.limit);
                for (page.items) |node| try values.append(scratch, .{ .value = .{ .node = node } });
                next = page.next;
                examined = page.examined;
                unfiltered = page.unfiltered;
            },
            .get => {
                const identity = request.identity orelse return error.InvalidCypherWireValue;
                if (!self.permitsTable(identity.table)) return error.Forbidden;
                if (try self.backend.vtable.get(self.backend.ptr, scratch, identity)) |node| try values.append(scratch, .{ .value = .{ .node = node } });
            },
            .get_many => {
                if (request.identities.len > 128) return error.CypherOwnerBudgetExceeded;
                for (request.identities) |identity| if (!self.permitsTable(identity.table)) return error.Forbidden;
                if (self.backend.vtable.get_many) |get_many| {
                    const nodes = try get_many(self.backend.ptr, scratch, request.identities);
                    if (nodes.len != request.identities.len) return error.CypherInvalidBackendPage;
                    for (nodes) |node| try values.append(scratch, .{ .value = if (node) |item| .{ .node = item } else .null });
                } else for (request.identities) |identity| {
                    try meter.work().charge(meter.work().ptr, 1);
                    const node = try self.backend.vtable.get(self.backend.ptr, scratch, identity);
                    try values.append(scratch, .{ .value = if (node) |item| .{ .node = item } else .null });
                }
            },
            .expand => {
                const mapping = self.graph.relationship(request.relationship) orelse return error.UndefinedGraphRelationship;
                if (!self.permitsTable(self.graph.label(mapping.source_label).?.table)) return error.Forbidden;
                const identity = request.identity orelse return error.InvalidCypherWireValue;
                const page = pair: {
                    if (request.peer) |peer| if (self.backend.vtable.expand_between) |between|
                        break :pair try between(self.backend.ptr, scratch, mapping, identity, peer, request.direction, &.{}, request.after, request.limit, request.max_examined);
                    break :pair try self.backend.vtable.expand(self.backend.ptr, scratch, mapping, identity, request.direction, request.after, request.limit);
                };
                for (page.items) |relationship| try values.append(scratch, .{ .value = .{ .relationship = relationship }, .include_native = true });
                next = page.next;
                examined = page.examined;
            },
            .search => {
                if (!self.permitsTable(request.table)) return error.Forbidden;
                const search_fn = self.backend.vtable.search orelse return error.CypherSearchUnavailable;
                const parameters = try wire.parameter(scratch, request.search orelse return error.InvalidCypherWireValue);
                if (parameters != .map) return error.CypherTypeMismatch;
                const page = try search_fn(self.backend.ptr, scratch, request.table, .{ .parameters = parameters, .prepared_json = request.prepared_search_json, .max_examined = request.max_examined });
                const seeds = page.seeds;
                examined = page.examined;
                approximate = page.approximate;
                if (seeds.len > 1024) return error.CypherOwnerBudgetExceeded;
                for (seeds) |seed| {
                    if (!std.math.isFinite(seed.score) or !std.mem.eql(u8, seed.node.identity.table, request.table)) return error.CypherInvalidSearchResult;
                    try values.append(scratch, .{ .value = .{ .node = seed.node } });
                    try scores.append(scratch, seed.score);
                }
            },
        }
        try self.backend.vtable.checkpoint(self.backend.ptr);
        const result = try std.json.Stringify.valueAlloc(a, .{ .work = meter.units, .unfiltered = unfiltered, .values = values.items, .next = next, .examined = examined, .scores = scores.items, .approximate = approximate }, .{});
        errdefer a.free(result);
        if (result.len > 32 << 20) return error.CypherResponseTooLarge;
        return result;
    }
};

test "bounded owner estimates retain authority and propagate planning work" {
    const Fixture = struct {
        work: ?b.Work = null,
        charged: usize = 0,
        invalid: bool = false,
        const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
        fn checkpoint(_: *anyopaque) !void {}
        fn setWork(ptr: *anyopaque, work: ?b.Work) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.work = work;
        }
        fn estimate(ptr: *anyopaque, _: A, _: []const u8, _: []const b.PropertyFilter, limit: usize) !?usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.work) |work| try work.charge(work.ptr, 7);
            return if (self.invalid) limit + 1 else if (limit >= 2) @as(usize, 2) else null;
        }
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.charged += amount;
        }
        fn value(_: *anyopaque, _: v.Value) !void {}
        fn call(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const owner: Owner = .{ .backend = .{ .ptr = ptr, .vtable = &.{ .bind = undefined, .checkpoint = checkpoint, .scan = undefined, .get = undefined, .expand = undefined, .estimate_where = estimate, .set_work = setWork } }, .graph = graph, .table = "docs" };
            return owner.execute(a, body);
        }
    };
    var fixture: Fixture = .{};
    var client: Client = .{ .channel = .{ .ptr = &fixture, .call = Fixture.call }, .incarnation = 1, .revision = 1, .work = .{ .ptr = &fixture, .charge = Fixture.charge, .value = Fixture.value } };
    const backend = client.backend();
    try std.testing.expectEqual(@as(?usize, 2), try backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", &.{}, 32));
    try std.testing.expectEqual(@as(usize, 7), fixture.charged);
    try std.testing.expectEqual(@as(?usize, null), try backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", &.{}, 1));
    try std.testing.expectError(error.Forbidden, backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "hidden", &.{}, 32));
    fixture.invalid = true;
    try std.testing.expectError(error.CypherInvalidBackendPage, backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", &.{}, 32));
}
