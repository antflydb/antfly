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
    operation: enum { validate, scan, get, expand, search, observe, observe_ranges, normalize, project },
    graph_incarnation: u64,
    graph_revision: u64,
    table: []const u8 = "",
    identity: ?v.Identity = null,
    relationship: []const u8 = "",
    direction: ast.Direction = .out,
    after: ?[]const u8 = null,
    limit: usize = 128,
    search: ?std.json.Value = null,
    prepared_search_json: []const u8 = "",
    max_examined: usize = 1_000_000,
    document: ?std.json.Value = null,
    apply_defaults: bool = false,
    json_null_fields: []const []const u8 = &.{},
    include_relationships: bool = false,
};
pub const Projection = struct { node: ?*const v.Node, relationships: []const *const v.Relationship = &.{} };
const Reply = struct { relationships: []const std.json.Value = &.{}, values: []const std.json.Value = &.{}, next: ?[]const u8 = null, examined: ?usize = null, scores: []const f64 = &.{}, approximate: bool = false };
pub const Channel = struct { ptr: *anyopaque, call: *const fn (*anyopaque, A, []const u8) anyerror![]u8, checkpoint: ?*const fn (*anyopaque) anyerror!void = null };
pub const Version = struct { incarnation: u64, revision: u64 };
pub const Client = struct {
    channel: Channel,
    incarnation: u64,
    revision: u64,
    allowed_versions: []const Version = &.{},
    pub fn backend(self: *Client) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .search = search, .normalize_node = normalizeNode, .project_postimage = projectPostimage, .document_relationships = documentRelationships } };
    }
    fn cast(ptr: *anyopaque) *Client {
        return @ptrCast(@alignCast(ptr));
    }
    fn invoke(self: *Client, a: A, request: Request) !Reply {
        const body = try std.json.Stringify.valueAlloc(a, request, .{});
        defer a.free(body);
        if (body.len > 1 << 20) return error.CypherRequestTooLarge;
        const response = try self.channel.call(self.channel.ptr, a, body);
        defer a.free(response);
        if (response.len > 32 << 20) return error.CypherResponseTooLarge;
        return std.json.parseFromSliceLeaky(Reply, a, response, .{ .allocate = .alloc_always });
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
        const document = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, v.Value{ .map = node.properties }, .{}), .{ .parse_numbers = false });
        const reply = try self.invoke(a, .{ .operation = .normalize, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = node.identity.table, .identity = node.identity, .document = document, .apply_defaults = apply_defaults });
        if (reply.values.len != 1) return error.CypherInvalidBackendPage;
        const value = try wire.transport(a, reply.values[0]);
        if (value != .node or !value.node.identity.eql(node.identity)) return error.CypherInvalidBackendPage;
        return value.node;
    }
    fn projectPostimage(ptr: *anyopaque, a: A, node: *const v.Node) !?*const v.Node {
        const self = cast(ptr);
        const document = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, v.Value{ .map = node.properties }, .{}), .{ .parse_numbers = false });
        return (try self.projectDocument(a, node.identity, document, &.{}, false)).node;
    }
    pub fn projectDocument(self: *Client, a: A, identity: v.Identity, document: std.json.Value, json_null_fields: []const []const u8, include_relationships: bool) !Projection {
        const reply = try self.invoke(a, .{ .operation = .project, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = identity.table, .identity = identity, .document = document, .json_null_fields = json_null_fields, .include_relationships = include_relationships });
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
        const document = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, v.Value{ .map = node.properties }, .{}), .{ .parse_numbers = false });
        return (try self.projectDocument(a, node.identity, document, &.{}, true)).relationships;
    }
    fn scan(ptr: *anyopaque, a: A, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self = cast(ptr);
        const reply = try self.invoke(a, .{ .operation = .scan, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .after = after, .limit = limit });
        const nodes = try a.alloc(*const v.Node, reply.values.len);
        for (reply.values, nodes) |input, *out| {
            const value = try wire.transport(a, input);
            if (value != .node or !std.mem.eql(u8, value.node.identity.table, table)) return error.CypherInvalidBackendPage;
            out.* = value.node;
        }
        return .{ .items = nodes, .next = reply.next, .examined = reply.examined };
    }
    fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
        const self = cast(ptr);
        const reply = try self.invoke(a, .{ .operation = .get, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .identity = identity });
        if (reply.values.len == 0) return null;
        if (reply.values.len != 1) return error.CypherInvalidBackendPage;
        const value = try wire.transport(a, reply.values[0]);
        if (value != .node or !value.node.identity.eql(identity)) return error.CypherInvalidBackendPage;
        return value.node;
    }
    fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
        const self = cast(ptr);
        const request = input.parameters;
        if (request != .map) return error.CypherTypeMismatch;
        const encoded = try std.json.parseFromSliceLeaky(std.json.Value, a, try std.json.Stringify.valueAlloc(a, wire.Value{ .value = request }, .{}), .{});
        const reply = try self.invoke(a, .{ .operation = .search, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .search = encoded, .prepared_search_json = input.prepared_json, .max_examined = input.max_examined });
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
        const self = cast(ptr);
        const reply = try self.invoke(a, .{ .operation = .expand, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .identity = identity, .relationship = mapping.name, .direction = direction, .after = after, .limit = limit });
        const relationships = try a.alloc(*const v.Relationship, reply.values.len);
        for (reply.values, relationships) |input, *out| {
            const value = try wire.transport(a, input);
            if (value != .relationship or !std.mem.eql(u8, value.relationship.type, mapping.name) or !std.mem.eql(u8, value.relationship.index, mapping.index)) return error.CypherInvalidBackendPage;
            out.* = value.relationship;
        }
        return .{ .items = relationships, .next = reply.next, .examined = reply.examined };
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
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const request = try std.json.parseFromSliceLeaky(Request, scratch, body, .{});
        if (request.graph_incarnation != self.graph.incarnation or request.graph_revision != self.graph.revision) return error.CatalogGenerationChanged;
        if (request.limit == 0 or request.limit > 1024) return error.CypherOwnerBudgetExceeded;
        try self.backend.vtable.checkpoint(self.backend.ptr);
        var values: std.ArrayList(wire.Value) = .empty;
        var next: ?[]const u8 = null;
        var examined: ?usize = null;
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
            .scan => {
                if (!self.permitsTable(request.table)) return error.Forbidden;
                const page = try self.backend.vtable.scan(self.backend.ptr, scratch, request.table, request.after, request.limit);
                for (page.items) |node| try values.append(scratch, .{ .value = .{ .node = node } });
                next = page.next;
                examined = page.examined;
            },
            .get => {
                const identity = request.identity orelse return error.InvalidCypherWireValue;
                if (!self.permitsTable(identity.table)) return error.Forbidden;
                if (try self.backend.vtable.get(self.backend.ptr, scratch, identity)) |node| try values.append(scratch, .{ .value = .{ .node = node } });
            },
            .expand => {
                const mapping = self.graph.relationship(request.relationship) orelse return error.UndefinedGraphRelationship;
                if (!self.permitsTable(self.graph.label(mapping.source_label).?.table)) return error.Forbidden;
                const page = try self.backend.vtable.expand(self.backend.ptr, scratch, mapping, request.identity orelse return error.InvalidCypherWireValue, request.direction, request.after, request.limit);
                for (page.items) |relationship| try values.append(scratch, .{ .value = .{ .relationship = relationship } });
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
        const result = try std.json.Stringify.valueAlloc(a, .{ .values = values.items, .next = next, .examined = examined, .scores = scores.items, .approximate = approximate }, .{});
        errdefer a.free(result);
        if (result.len > 32 << 20) return error.CypherResponseTooLarge;
        return result;
    }
};
