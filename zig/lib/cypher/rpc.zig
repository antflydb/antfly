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
// Named transport types keep the checked callback ABI stable across separately
// compiled runtimes. Anonymous enum/struct type names contain local IDs.
pub const Operation = enum { validate, scan, scan_ordered, estimate, population, get, get_many, expand, search, observe, observe_ranges, normalize, project, probe };
pub const Filter = struct { property: []const u8, value: std.json.Value, op: b.FilterOp = .eq, domain: ?b.ScalarDomain = null, plan: ?b.DomainPlan = null };
pub const OrderAfter = struct { value: std.json.Value };
pub const Order = struct { property: []const u8, descending: bool = false, exclude_null: bool = false, after: ?OrderAfter = null };
pub const BudgetFailure = enum { work, anchors };
pub const Request = struct {
    operation: Operation,
    graph_incarnation: u64,
    graph_revision: u64,
    table: []const u8 = "",
    identity: ?v.Identity = null,
    peer: ?v.Identity = null,
    identities: []const v.Identity = &.{},
    relationship: []const u8 = "",
    direction: ast.Direction = .out,
    filters: []const Filter = &.{},
    order: []const Order = &.{},
    after: ?[]const u8 = null,
    primary: bool = false,
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
pub const Reply = struct {
    read_failure: ?BudgetFailure = null,
    search_failure: ?BudgetFailure = null,
    exact_keys: bool = false,
    stable_ties: bool = true,
    encounter_owners: ?[]const usize = null,
    cursors: ?[]const ?[]const u8 = null,
    cursor_trace: ?b.CursorTrace = null,
    ordered: bool = false,
    estimate: ?usize = null,
    population: ?u64 = null,
    unfiltered: bool = false,
    seek_window: ?b.SeekWindow = null,
    probe_complete: bool = false,
    probe_supported: bool = false,
    work: usize = 0,
    relationships: []const wire.Value = &.{},
    values: []const wire.Value = &.{},
    next: ?[]const u8 = null,
    examined: ?usize = null,
    scores: []const f64 = &.{},
    approximate: bool = false,
    /// Copy only reply data across a runtime boundary, after owner-local
    /// scratch (including per-document decoding) has been reclaimed.
    pub fn clone(self: Reply, a: A) !Reply {
        var out = self;
        const values = try a.dupe(wire.Value, self.values);
        for (values) |*item| item.value = try item.value.clone(a);
        out.values = values;
        const relationships = try a.dupe(wire.Value, self.relationships);
        for (relationships) |*item| item.value = try item.value.clone(a);
        out.relationships = relationships;
        out.scores = try a.dupe(f64, self.scores);
        if (self.next) |next| out.next = try a.dupe(u8, next);
        if (self.encounter_owners) |owners| out.encounter_owners = try a.dupe(usize, owners);
        if (self.cursors) |cursors| {
            const copied = try a.dupe(?[]const u8, cursors);
            for (copied) |*cursor| if (cursor.*) |bytes| {
                cursor.* = try a.dupe(u8, bytes);
            };
            out.cursors = copied;
        }
        if (self.cursor_trace) |trace| out.cursor_trace = (try trace.clone(a)).*;
        return out;
    }
};
pub const Channel = struct {
    /// Only physical-owner channels partition sparse domains. A coordinator
    /// must retain the full domain until each shard's continuation is done.
    owner_local: bool = true,
    typed: ?*const fn (*anyopaque, A, Request) anyerror!Reply = null,
    ptr: *anyopaque,
    call: *const fn (*anyopaque, A, []const u8) anyerror![]u8,
    checkpoint: ?*const fn (*anyopaque) anyerror!void = null,
};
pub const Version = struct { incarnation: u64, revision: u64 };
pub const Client = struct {
    channel: Channel,
    incarnation: u64,
    revision: u64,
    allowed_versions: []const Version = &.{},
    work: ?b.Work = null,
    read_fields: ?[]const []const u8 = null,
    pub fn backend(self: *Client) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .scan_where = scanWhere, .probe_where = probeWhere, .scan_ordered = scanOrdered, .estimate_where = estimateWhere, .population_upper_bound = populationUpperBound, .get = get, .get_many = getMany, .expand = expand, .expand_between = expandBetween, .search = search, .normalize_node = normalizeNode, .project_postimage = projectPostimage, .document_relationships = documentRelationships, .set_work = setWork, .set_read_fields = setReadFields } };
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
        const reply = if (self.channel.typed) |typed| try typed(self.channel.ptr, a, bounded) else response: {
            const body = try std.json.Stringify.valueAlloc(a, bounded, .{});
            defer a.free(body);
            if (body.len > 1 << 20) return error.CypherRequestTooLarge;
            const response = try self.channel.call(self.channel.ptr, a, body);
            defer a.free(response);
            if (response.len > 32 << 20) return error.CypherResponseTooLarge;
            break :response try std.json.parseFromSliceLeaky(Reply, a, response, .{ .allocate = .alloc_always });
        };
        if (reply.work > bounded.max_work) return error.CypherInvalidBackendPage;
        if (self.work) |work| try work.charge(work.ptr, reply.work);
        if (reply.read_failure) |failure| {
            if ((request.operation != .scan_ordered and request.operation != .get_many and request.operation != .probe) or reply.values.len != 0 or reply.next != null or reply.ordered or reply.cursors != null or reply.cursor_trace != null) return error.CypherInvalidBackendPage;
            return switch (failure) {
                .work => error.CypherStateBudgetExceeded,
                .anchors => error.CypherAnchorBudgetExceeded,
            };
        }
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
        const value = try reply.values[0].value.clone(a);
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
        const value = try reply.values[0].value.clone(a);
        if (value != .node or !value.node.identity.eql(identity)) return error.CypherInvalidBackendPage;
        const relationships = try a.alloc(*const v.Relationship, reply.relationships.len);
        for (reply.relationships, relationships) |item, *out| {
            const decoded = try item.value.clone(a);
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
    fn populationUpperBound(ptr: *anyopaque, a: A, table: []const u8) !?u64 {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        return (try self.invoke(scratch.allocator(), .{ .operation = .population, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table })).population;
    }
    fn estimateWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) !?usize {
        if (limit == 0) return null;
        if (filters.len == 0) if (try populationUpperBound(ptr, a, table)) |count| return @intCast(@min(count, limit));
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const encoded = try std.json.parseFromSliceLeaky(@FieldType(Request, "filters"), temporary, try std.json.Stringify.valueAlloc(temporary, filters, .{}), .{ .parse_numbers = false });
        // The estimator's demand is independent of the owner's bounded sample
        // contract. A saturated sample cannot justify a larger full-pass plan.
        if (limit == 0) return null;
        const maximum = @min(limit, 128);
        const reply = try self.invoke(temporary, .{ .filters = encoded, .operation = .estimate, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .limit = maximum });
        if (reply.estimate) |upper| {
            if (upper > maximum) return error.CypherInvalidBackendPage;
            if (maximum < limit and upper == maximum) return null;
        }
        return reply.estimate;
    }
    fn scanOrdered(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, max_examined: usize) !?b.Page(*const v.Node) {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const encoded_filters = try std.json.parseFromSliceLeaky(@FieldType(Request, "filters"), temporary, try std.json.Stringify.valueAlloc(temporary, filters, .{}), .{ .parse_numbers = false });
        const encoded_order = try temporary.alloc(@typeInfo(@FieldType(Request, "order")).pointer.child, order.len);
        for (order, encoded_order) |input, *out| out.* = .{ .property = input.property, .descending = input.descending, .exclude_null = input.exclude_null, .after = if (input.after) |key| .{ .value = try std.json.parseFromSliceLeaky(std.json.Value, temporary, try std.json.Stringify.valueAlloc(temporary, key, .{}), .{ .parse_numbers = false }) } else null };
        const reply = try self.invoke(temporary, .{ .filters = encoded_filters, .order = encoded_order, .operation = .scan_ordered, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .after = after, .limit = limit, .max_examined = max_examined });
        if (!reply.ordered) {
            if (reply.values.len != 0 or reply.cursors != null or reply.cursor_trace != null or reply.next != null or after != null) return error.CypherInvalidBackendPage;
            return null;
        }
        if (reply.values.len > limit or (reply.cursor_trace != null and reply.cursors != null)) return error.CypherInvalidBackendPage;
        const nodes = try a.alloc(*const v.Node, reply.values.len);
        for (reply.values, nodes) |input, *out| {
            const value = try input.value.clone(a);
            if (value != .node or !std.mem.eql(u8, value.node.identity.table, table)) return error.CypherInvalidBackendPage;
            out.* = value.node;
        }
        if (reply.cursor_trace) |trace| try trace.validate(nodes.len);
        const cursors = if (reply.cursors) |positions| blk: {
            if (positions.len != nodes.len) return error.CypherInvalidBackendPage;
            const saved = try a.alloc(?[]const u8, positions.len);
            for (positions, saved) |position, *out| out.* = if (position) |bytes| try a.dupe(u8, bytes) else null;
            break :blk saved;
        } else null;
        if (reply.encounter_owners) |owners| if (owners.len != nodes.len) return error.CypherInvalidBackendPage;
        return .{ .cursor_trace = if (reply.cursor_trace) |trace| try trace.clone(a) else null, .stable_ties = reply.stable_ties, .exact_keys = reply.exact_keys, .encounter_owners = if (reply.encounter_owners) |owners| try a.dupe(usize, owners) else null, .items = nodes, .cursors = cursors, .next = if (reply.next) |next| try a.dupe(u8, next) else null, .examined = reply.examined };
    }
    fn scanWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const input = if (self.channel.owner_local) try @import("sparse_seek.zig").prepare(temporary, filters, after) else @import("sparse_seek.zig").Input{ .filters = filters, .after = after };
        var selected = input.filters;
        if (self.channel.owner_local and after == null) {
            // Count before allocating. Include variable projection/table bytes;
            // fixed request fields fit within the reserved header allowance.
            var count = std.Io.Writer.Discarding.init(&.{});
            try std.json.Stringify.value(.{ .filters = input.filters, .table = table, .read_fields = self.read_fields }, .{}, &count.writer);
            if (count.fullCount() > (1 << 20) - 2048)
                selected = try @import("sparse_seek.zig").planning(temporary, input.filters);
        }
        const filter_bytes = try std.json.Stringify.valueAlloc(temporary, selected, .{});
        const encoded_filters = try std.json.parseFromSliceLeaky(@FieldType(Request, "filters"), temporary, filter_bytes, .{ .parse_numbers = false });
        const reply = try self.invoke(temporary, .{ .filters = encoded_filters, .operation = .scan, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .table = table, .after = input.after, .primary = input.primary, .limit = limit });
        const nodes = try a.alloc(*const v.Node, reply.values.len);
        for (reply.values, nodes) |item, *out| {
            const value = try item.value.clone(a);
            if (value != .node or !std.mem.eql(u8, value.node.identity.table, table)) return error.CypherInvalidBackendPage;
            out.* = value.node;
        }
        const page: b.Page(*const v.Node) = .{ .items = nodes, .next = if (reply.next) |next| try a.dupe(u8, next) else null, .examined = reply.examined, .unfiltered = reply.unfiltered, .seek_window = if (self.channel.owner_local) reply.seek_window else null };
        return if (self.channel.owner_local) try @import("sparse_seek.zig").finish(a, page) else page;
    }
    fn probeWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) !?b.Probe {
        const self = cast(ptr);
        if (limit == 0 or limit > 128) return null;
        var arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer arena.deinit();
        const scratch = arena.allocator();
        const encoded = try std.json.parseFromSliceLeaky(@FieldType(Request, "filters"), scratch, try std.json.Stringify.valueAlloc(scratch, filters, .{}), .{ .parse_numbers = false });
        const reply = try self.invoke(scratch, .{ .operation = .probe, .filters = encoded, .table = table, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .limit = limit });
        if (!reply.probe_supported) return null;
        const used = reply.examined orelse reply.values.len;
        if (reply.next != null or reply.unfiltered or used > limit + 1 or (!reply.probe_complete and reply.values.len != 0) or (reply.probe_complete and (used > limit or used < reply.values.len))) return error.CypherInvalidBackendPage;
        const nodes = try a.alloc(*const v.Node, reply.values.len);
        for (reply.values, nodes) |item, *out| {
            const value = try item.value.clone(a);
            if (value != .node or !std.mem.eql(u8, value.node.identity.table, table)) return error.CypherInvalidBackendPage;
            out.* = value.node;
        }
        return .{ .page = .{ .items = nodes, .examined = used }, .complete = reply.probe_complete };
    }
    fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
        const self = cast(ptr);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        const reply = try self.invoke(temporary, .{ .operation = .get, .graph_incarnation = self.incarnation, .graph_revision = self.revision, .identity = identity });
        if (reply.values.len == 0) return null;
        if (reply.values.len != 1) return error.CypherInvalidBackendPage;
        const value = try reply.values[0].value.clone(a);
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
            const value = try input.value.clone(a);
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
        if (input.usage) |usage| usage.examined = reply.examined;
        if (reply.examined) |examined| if (examined > input.max_examined) return error.CypherInvalidSearchResult;
        if (reply.search_failure) |failure| {
            if (reply.values.len != 0 or reply.scores.len != 0 or reply.next != null) return error.CypherInvalidBackendPage;
            return switch (failure) {
                .work => error.CypherStateBudgetExceeded,
                .anchors => error.CypherAnchorBudgetExceeded,
            };
        }
        if (reply.values.len != reply.scores.len or reply.values.len > 1024) return error.CypherInvalidBackendPage;
        const seeds = try a.alloc(b.Seed, reply.values.len);
        for (reply.values, reply.scores, seeds) |encoded_value, score, *out| {
            const value = try encoded_value.value.clone(a);
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
            const value = try input.value.clone(a);
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
    fn readFailure(self: Owner, units: usize, err: anyerror) !Reply {
        const failure: @typeInfo(@FieldType(Reply, "read_failure")).optional.child = switch (err) {
            error.CypherStateBudgetExceeded => .work,
            error.CypherAnchorBudgetExceeded => .anchors,
            else => return err,
        };
        try self.backend.vtable.checkpoint(self.backend.ptr);
        return .{ .read_failure = failure, .work = units };
    }
    pub fn execute(self: Owner, a: A, body: []const u8) ![]u8 {
        if (body.len > 1 << 20) return error.CypherRequestTooLarge;
        var arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer arena.deinit();
        const scratch = arena.allocator();
        const request = try std.json.parseFromSliceLeaky(Request, scratch, body, .{});
        const result = try self.executeTyped(scratch, request);
        // Nested cursor steps require an explicit nullable `after` field. A
        // global omit-null option also removes those terminal markers.
        const encoded = try std.json.Stringify.valueAlloc(a, result, .{});
        errdefer a.free(encoded);
        if (encoded.len > 32 << 20) return error.CypherResponseTooLarge;
        return encoded;
    }

    /// The caller owns the request/reply arena through consumption of the reply.
    /// This is the same validated owner operation as the wire adapter above.
    pub fn executeTyped(self: Owner, scratch: A, request: Request) !Reply {
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
        var seek_window: ?b.SeekWindow = null;
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
                // A normalizer may return its input unchanged. The typed
                // reply outlives this stack frame, so the input must too.
                const node = try scratch.create(v.Node);
                node.* = .{ .identity = identity, .properties = document.map };
                const normalized = try (self.backend.vtable.normalize_node orelse return error.CypherInvalidDocument)(self.backend.ptr, scratch, node, request.apply_defaults);
                try values.append(scratch, .{ .value = .{ .node = normalized } });
            },
            .population => {
                if (!self.permitsTable(request.table)) return error.Forbidden;
                const count = if (self.backend.vtable.population_upper_bound) |read| try read(self.backend.ptr, scratch, request.table) else null;
                try self.backend.vtable.checkpoint(self.backend.ptr);
                return .{ .work = meter.units, .population = count };
            },
            .probe => {
                if (!self.permitsTable(request.table) or request.limit > 128) return error.Forbidden;
                const read = self.backend.vtable.probe_where orelse return .{ .work = meter.units };
                const filters = try scratch.alloc(b.PropertyFilter, request.filters.len);
                for (request.filters, filters) |input, *filter| filter.* = .{ .property = input.property, .value = try v.Value.fromJson(scratch, input.value), .op = input.op, .domain = input.domain, .plan = input.plan };
                const result = (read(self.backend.ptr, scratch, request.table, filters, request.limit) catch |err| return self.readFailure(meter.units, err)) orelse return .{ .work = meter.units };
                const used = result.page.examined orelse result.page.items.len;
                if (used > request.limit + 1 or (result.complete and (result.page.next != null or result.page.unfiltered or used > request.limit or used < result.page.items.len))) return error.CypherInvalidBackendPage;
                if (result.complete) for (result.page.items) |node| try values.append(scratch, .{ .value = .{ .node = node } });
                try self.backend.vtable.checkpoint(self.backend.ptr);
                return .{ .work = meter.units, .probe_supported = true, .probe_complete = result.complete, .examined = used, .values = values.items };
            },
            .estimate => {
                if (!self.permitsTable(request.table) or request.limit > 128) return error.Forbidden;
                const filters = try scratch.alloc(b.PropertyFilter, request.filters.len);
                for (request.filters, filters) |input, *filter| filter.* = .{ .property = input.property, .value = try v.Value.fromJson(scratch, input.value), .op = input.op, .domain = input.domain, .plan = input.plan };
                const upper = if (self.backend.vtable.estimate_where) |estimate| try estimate(self.backend.ptr, scratch, request.table, filters, request.limit) else null;
                try self.backend.vtable.checkpoint(self.backend.ptr);
                return .{ .work = meter.units, .estimate = upper };
            },
            .scan_ordered => {
                if (!self.permitsTable(request.table) or request.order.len > 32) return error.Forbidden;
                const filters = try scratch.alloc(b.PropertyFilter, request.filters.len);
                for (request.filters, filters) |input, *filter| filter.* = .{ .property = input.property, .value = try v.Value.fromJson(scratch, input.value), .op = input.op, .domain = input.domain, .plan = input.plan };
                const order = try scratch.alloc(b.PropertyOrder, request.order.len);
                for (request.order, order) |input, *out| out.* = .{ .property = input.property, .descending = input.descending, .exclude_null = input.exclude_null, .after = if (input.after) |key| try v.Value.fromJson(scratch, key.value) else null };
                const page = if (self.backend.vtable.scan_ordered) |read| read(self.backend.ptr, scratch, request.table, filters, order, request.after, request.limit, request.max_examined) catch |err| return self.readFailure(meter.units, err) else null;
                if (page) |ordered| {
                    for (ordered.items) |node| try values.append(scratch, .{ .value = .{ .node = node } });
                    return .{ .ordered = true, .stable_ties = ordered.stable_ties, .exact_keys = ordered.exact_keys, .encounter_owners = ordered.encounter_owners, .values = values.items, .cursors = ordered.cursors, .cursor_trace = if (ordered.cursor_trace) |trace| trace.* else null, .next = ordered.next, .examined = ordered.examined, .work = meter.units };
                }
                return .{ .ordered = false, .work = meter.units };
            },
            .scan => {
                if (!self.permitsTable(request.table)) return error.Forbidden;
                const filters = try scratch.alloc(b.PropertyFilter, request.filters.len);
                for (request.filters, filters) |input, *filter| filter.* = .{ .property = input.property, .value = try v.Value.fromJson(scratch, input.value), .op = input.op, .domain = input.domain, .plan = input.plan };
                const planning = for (filters) |filter| {
                    if (filter.plan != null) break true;
                } else false;
                // Hosts without the planning protocol enumerate their ordinary
                // source once; sampled hints can never become query bindings.
                const fallback = request.primary or (planning and !self.backend.vtable.planned_domains);
                const page = if (fallback) try self.backend.vtable.scan(self.backend.ptr, scratch, request.table, request.after, request.limit) else if (self.backend.vtable.scan_where) |filtered| try filtered(self.backend.ptr, scratch, request.table, filters, request.after, request.limit) else try self.backend.vtable.scan(self.backend.ptr, scratch, request.table, request.after, request.limit);
                for (page.items) |node| try values.append(scratch, .{ .value = .{ .node = node } });
                next = page.next;
                examined = page.examined;
                unfiltered = page.unfiltered;
                seek_window = if (fallback) b.SeekWindow{ .filter = null, .primary = true } else page.seek_window orelse if (planning and page.unfiltered) b.SeekWindow{ .filter = null } else null;
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
                    const nodes = get_many(self.backend.ptr, scratch, request.identities) catch |err| return self.readFailure(meter.units, err);
                    if (nodes.len != request.identities.len) return error.CypherInvalidBackendPage;
                    for (nodes) |node| try values.append(scratch, .{ .value = if (node) |item| .{ .node = item } else .null });
                } else for (request.identities) |identity| {
                    meter.work().charge(meter.work().ptr, 1) catch |err| return self.readFailure(meter.units, err);
                    const node = self.backend.vtable.get(self.backend.ptr, scratch, identity) catch |err| return self.readFailure(meter.units, err);
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
                var usage: b.SearchUsage = .{};
                const page = search_fn(self.backend.ptr, scratch, request.table, .{ .parameters = parameters, .prepared_json = request.prepared_search_json, .max_examined = request.max_examined, .usage = &usage }) catch |err| {
                    const failure: @typeInfo(@FieldType(Reply, "search_failure")).optional.child = switch (err) {
                        error.CypherStateBudgetExceeded => .work,
                        error.CypherAnchorBudgetExceeded => .anchors,
                        else => return err,
                    };
                    try self.backend.vtable.checkpoint(self.backend.ptr);
                    return .{ .search_failure = failure, .work = meter.units, .examined = usage.examined };
                };
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
        return .{ .work = meter.units, .seek_window = seek_window, .unfiltered = unfiltered, .values = values.items, .next = next, .examined = examined, .scores = scores.items, .approximate = approximate };
    }
};

test "bounded owner estimates retain authority and propagate planning work" {
    const Fixture = struct {
        work: ?b.Work = null,
        charged: usize = 0,
        invalid: bool = false,
        saturated: bool = false,
        fn population(ptr: *anyopaque, _: A, _: []const u8) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.work) |work| try work.charge(work.ptr, 2);
            return 50000;
        }
        const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
        fn checkpoint(_: *anyopaque) !void {}
        fn setWork(ptr: *anyopaque, work: ?b.Work) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.work = work;
        }
        fn estimate(ptr: *anyopaque, _: A, _: []const u8, _: []const b.PropertyFilter, limit: usize) !?usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.work) |work| try work.charge(work.ptr, 7);
            return if (self.invalid) limit + 1 else if (self.saturated) limit else if (limit >= 2) @as(usize, 2) else null;
        }
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.charged += amount;
        }
        fn value(_: *anyopaque, _: v.Value) !void {}
        fn call(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const owner: Owner = .{ .backend = .{ .ptr = ptr, .vtable = &.{ .bind = undefined, .checkpoint = checkpoint, .scan = undefined, .get = undefined, .expand = undefined, .estimate_where = estimate, .population_upper_bound = population, .set_work = setWork } }, .graph = graph, .table = "docs" };
            return owner.execute(a, body);
        }
    };
    var fixture: Fixture = .{};
    var client: Client = .{ .channel = .{ .ptr = &fixture, .call = Fixture.call }, .incarnation = 1, .revision = 1, .work = .{ .ptr = &fixture, .charge = Fixture.charge, .value = Fixture.value } };
    const backend = client.backend();
    const filters = &[_]b.PropertyFilter{.{ .property = "score", .value = .{ .integer = 1 } }};
    try std.testing.expectEqual(@as(?usize, 2), try backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", filters, 32));
    try std.testing.expectEqual(@as(usize, 7), fixture.charged);
    try std.testing.expectEqual(@as(?usize, 2), try backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", filters, 15000));
    try std.testing.expectEqual(@as(?usize, null), try backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", filters, 1));
    try std.testing.expectError(error.Forbidden, backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "hidden", filters, 32));
    try std.testing.expectEqual(@as(?usize, 50000), try backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", &.{}, 60000));
    try std.testing.expectEqual(@as(?u64, 50000), try backend.vtable.population_upper_bound.?(backend.ptr, std.testing.allocator, "docs"));
    try std.testing.expectError(error.Forbidden, backend.vtable.population_upper_bound.?(backend.ptr, std.testing.allocator, "hidden"));
    fixture.saturated = true;
    try std.testing.expectEqual(@as(?usize, null), try backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", filters, 15000));
    try std.testing.expectEqual(@as(?usize, 128), try backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", filters, 128));
    fixture.invalid = true;
    try std.testing.expectError(error.CypherInvalidBackendPage, backend.vtable.estimate_where.?(backend.ptr, std.testing.allocator, "docs", filters, 32));
}

test "search quota failures return charged work and candidate progress through RPC" {
    const Fixture = struct {
        work: ?b.Work = null,
        anchors: bool = false,
        revoked: bool = false,
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.revoked) return error.Forbidden;
        }
        fn configure(ptr: *anyopaque, work: ?b.Work) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.work = work;
        }
        fn search(ptr: *anyopaque, _: A, _: []const u8, input: b.SearchInput) !b.SearchPage {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (input.usage) |usage| usage.examined = 3;
            for (0..8) |_| if (self.work) |work| try work.charge(work.ptr, 1);
            return error.CypherAnchorBudgetExceeded;
        }
        const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
        fn call(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const owner: Owner = .{ .backend = .{ .ptr = ptr, .vtable = &.{ .bind = undefined, .checkpoint = check, .scan = undefined, .get = undefined, .expand = undefined, .search = search, .set_work = configure } }, .graph = graph, .table = "docs" };
            return owner.execute(a, body);
        }
    };
    var fixture: Fixture = .{};
    const leaf: b.Backend = .{ .ptr = &fixture, .vtable = &.{ .bind = undefined, .checkpoint = Fixture.check, .scan = undefined, .get = undefined, .expand = undefined } };
    var meter: b.Meter = .{ .a = std.testing.allocator, .backend = leaf, .limit = 5 };
    var client: Client = .{ .channel = .{ .ptr = &fixture, .call = Fixture.call }, .incarnation = 1, .revision = 1, .work = meter.work() };
    var usage: b.SearchUsage = .{};
    const input: b.SearchInput = .{ .parameters = .{ .map = &.{} }, .max_examined = 10, .usage = &usage };
    try std.testing.expectError(error.CypherStateBudgetExceeded, client.backend().vtable.search.?(&client, std.testing.allocator, "docs", input));
    try std.testing.expectEqual(@as(usize, 5), meter.units);
    try std.testing.expectEqual(@as(?usize, 3), usage.examined);
    meter.units = 0;
    meter.limit = 20;
    try std.testing.expectError(error.CypherAnchorBudgetExceeded, client.backend().vtable.search.?(&client, std.testing.allocator, "docs", input));
    try std.testing.expectEqual(@as(usize, 8), meter.units);
    try std.testing.expectEqual(@as(?usize, 3), usage.examined);
    fixture.revoked = true;
    try std.testing.expectError(error.Forbidden, client.backend().vtable.search.?(&client, std.testing.allocator, "docs", input));
}

test "ordered and batched read failures retain charged work over private RPC" {
    const Fixture = struct {
        work: ?b.Work = null,
        revoke_on_failure: bool = false,
        revoked: bool = false,
        fn check(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.revoked) return error.Forbidden;
        }
        fn configure(ptr: *anyopaque, work: ?b.Work) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.work = work;
        }
        fn consume(self: *@This()) !void {
            defer if (self.revoke_on_failure) {
                self.revoked = true;
            };
            for (0..8) |_| if (self.work) |work| try work.charge(work.ptr, 1);
            return error.CypherAnchorBudgetExceeded;
        }
        fn ordered(ptr: *anyopaque, _: A, _: []const u8, _: []const b.PropertyFilter, _: []const b.PropertyOrder, _: ?[]const u8, _: usize, _: usize) !?b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.consume();
            unreachable;
        }
        fn getMany(ptr: *anyopaque, _: A, _: []const v.Identity) ![]const ?*const v.Node {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try self.consume();
            unreachable;
        }
        fn call(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
            const owner: Owner = .{ .backend = .{ .ptr = ptr, .vtable = &.{ .bind = undefined, .checkpoint = check, .scan = undefined, .get = undefined, .expand = undefined, .scan_ordered = ordered, .get_many = getMany, .set_work = configure } }, .graph = graph, .table = "docs" };
            return owner.execute(a, body);
        }
    };
    for ([_]bool{ false, true }) |ordered| {
        var f: Fixture = .{};
        const leaf: b.Backend = .{ .ptr = &f, .vtable = &.{ .bind = undefined, .checkpoint = Fixture.check, .scan = undefined, .get = undefined, .expand = undefined } };
        var meter: b.Meter = .{ .a = std.testing.allocator, .backend = leaf, .limit = 5 };
        var client: Client = .{ .channel = .{ .ptr = &f, .call = Fixture.call }, .incarnation = 1, .revision = 1, .work = meter.work() };
        for ([_]usize{ 5, 20, 20 }) |limit| {
            meter.limit = limit;
            meter.units = 0;
            const expected = if (f.revoke_on_failure) error.Forbidden else if (limit == 5) error.CypherStateBudgetExceeded else error.CypherAnchorBudgetExceeded;
            const backend = client.backend();
            if (ordered) try std.testing.expectError(expected, backend.vtable.scan_ordered.?(backend.ptr, std.testing.allocator, "docs", &.{}, &.{.{ .property = "score" }}, null, 1, 10)) else try std.testing.expectError(expected, backend.vtable.get_many.?(backend.ptr, std.testing.allocator, &.{.{ .table = "docs", .key = "n0" }}));
            if (!f.revoke_on_failure) try std.testing.expectEqual(@as(usize, if (limit == 5) 5 else 8), meter.units);
            if (limit == 20) f.revoke_on_failure = true;
        }
    }
}

test "typed coordinator removes codec hop while retaining leaf validation and work" {
    const Fixture = struct {
        calls: usize = 0,
        checks: usize = 0,
        work: ?b.Work = null,
        node: v.Node = .{ .identity = .{ .table = "docs", .key = "a" }, .properties = &.{.{ .name = "x", .value = .{ .integer = std.math.maxInt(i64) } }} },
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.checks += 1;
        }
        fn configure(ptr: *anyopaque, work: ?b.Work) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.work = work;
        }
        fn get(ptr: *anyopaque, _: A, _: v.Identity) !?*const v.Node {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.work) |work| try work.charge(work.ptr, 1);
            return &self.node;
        }
        const Loop = struct {
            owner: Owner,
            calls: usize = 0,
            fn call(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                return self.owner.execute(a, body);
            }
            fn typed(ptr: *anyopaque, a: A, request: Request) !Reply {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return self.owner.executeTyped(a, request);
            }
        };
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    var leaf: Fixture.Loop = .{ .owner = .{ .backend = .{ .ptr = &fixture, .vtable = &.{ .bind = undefined, .checkpoint = Fixture.checkpoint, .scan = undefined, .get = Fixture.get, .expand = undefined, .set_work = Fixture.configure } }, .graph = graph, .table = "docs" } };
    var inner: Client = .{ .channel = .{ .ptr = &leaf, .call = Fixture.Loop.call }, .incarnation = 1, .revision = 1 };
    var coordinator: Fixture.Loop = .{ .owner = .{ .backend = inner.backend(), .graph = graph, .table = "" } };
    var client: Client = .{ .channel = .{ .ptr = &coordinator, .call = Fixture.Loop.call, .typed = Fixture.Loop.typed }, .incarnation = 1, .revision = 1 };
    const backend = client.backend();
    const node = (try backend.vtable.get(backend.ptr, a, fixture.node.identity)).?;
    try std.testing.expectEqual(std.math.maxInt(i64), node.properties[0].value.integer);
    try std.testing.expectEqual(@as(usize, 0), coordinator.calls);
    try std.testing.expectEqual(@as(usize, 1), leaf.calls);
    try std.testing.expect(fixture.checks >= 2);
    try std.testing.expect(fixture.work == null and inner.work == null);
    const request: Request = .{ .operation = .get, .graph_incarnation = 2, .graph_revision = 1, .identity = fixture.node.identity };
    try std.testing.expectError(error.CatalogGenerationChanged, coordinator.owner.executeTyped(a, request));
    try std.testing.expectError(error.Forbidden, coordinator.owner.executeTyped(a, .{ .operation = .get, .graph_incarnation = 1, .graph_revision = 1, .identity = .{ .table = "secret", .key = "a" } }));
    try std.testing.expectError(error.CypherStateBudgetExceeded, coordinator.owner.executeTyped(a, .{ .operation = .get, .graph_incarnation = 1, .graph_revision = 1, .identity = fixture.node.identity, .max_work = 0 }));
    try std.testing.expect(fixture.work == null and inner.work == null);
}

test "typed normalization retains forwarded input after the owner frame returns" {
    const Fixture = struct {
        fn checkpoint(_: *anyopaque) !void {}
        fn normalize(_: *anyopaque, _: A, node: *const v.Node, _: bool) !*const v.Node {
            return node;
        }
        noinline fn clobber() void {
            var bytes: [8192]u8 = @splat(0xa5);
            std.mem.doNotOptimizeAway(&bytes);
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var marker: u8 = 0;
    const owner: Owner = .{ .backend = .{ .ptr = &marker, .vtable = &.{ .bind = undefined, .checkpoint = Fixture.checkpoint, .scan = undefined, .get = undefined, .expand = undefined, .normalize_node = Fixture.normalize } }, .graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} }, .table = "docs" };
    const identity: v.Identity = .{ .table = "docs", .key = "a" };
    const document = try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"x\":42}", .{});
    const reply = try owner.executeTyped(a, .{ .operation = .normalize, .graph_incarnation = 1, .graph_revision = 1, .identity = identity, .document = document });
    Fixture.clobber();
    const value = try reply.values[0].value.clone(a);
    try std.testing.expect(value.node.identity.eql(identity));
    try std.testing.expectEqual(@as(i64, 42), value.node.properties[0].value.integer);
}

test "owner wire replies preserve terminal ordered cursor steps" {
    const Fixture = struct {
        node: v.Node = .{ .identity = .{ .table = "docs", .key = "last" } },
        fn check(_: *anyopaque) !void {}
        fn ordered(ptr: *anyopaque, a: A, _: []const u8, _: []const b.PropertyFilter, _: []const b.PropertyOrder, _: ?[]const u8, _: usize, _: usize) !?b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .items = try a.dupe(*const v.Node, &.{&self.node}), .cursor_trace = try b.CursorTrace.create(a, .{ .base = &.{.{ .owner = 0 }}, .steps = &.{.{ .slot = 0, .after = null }} }) };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    const owner: Owner = .{ .backend = .{ .ptr = &fixture, .vtable = &.{ .bind = undefined, .checkpoint = Fixture.check, .scan = undefined, .get = undefined, .expand = undefined, .scan_ordered = Fixture.ordered } }, .graph = graph, .table = "docs" };
    const body = try std.json.Stringify.valueAlloc(a, Request{ .operation = .scan_ordered, .graph_incarnation = 1, .graph_revision = 1, .table = "docs" }, .{});
    const response = try owner.execute(a, body);
    const reply = try std.json.parseFromSliceLeaky(Reply, a, response, .{});
    const trace = reply.cursor_trace.?;
    try trace.validate(reply.values.len);
    try std.testing.expectEqual(@as(?[]const u8, null), trace.steps[0].after);
    try std.testing.expectEqual(@as(?[]const u8, null), try trace.materialize(a, 1, @as(?b.Work, null)));
}

test "sparse RPC plans full predicate then bounds selected domain or omits unused membership" {
    const Fixture = struct {
        delivered: usize = 0,
        calls: usize = 0,
        fallback: bool = false,
        fn call(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const request = try std.json.parseFromSliceLeaky(Request, a, body, .{});
            self.calls += 1;
            if (self.calls == 1) {
                try std.testing.expectEqual(@as(usize, 400), request.filters[0].value.array.items.len);
                self.delivered = 128;
            } else if (self.fallback) {
                try std.testing.expectEqualStrings("primary-position", request.after.?);
                try std.testing.expectEqual(@as(usize, 0), request.filters.len);
            } else {
                const filter = request.filters[0];
                try std.testing.expect(filter.value.array.items.len <= 128);
                try std.testing.expectEqual(b.ScalarDomain.integer, filter.domain.?);
                self.delivered += filter.value.array.items.len;
            }
            const more = if (self.fallback) self.calls == 1 else self.delivered < 400;
            return std.json.Stringify.valueAlloc(a, Reply{
                .next = if (more) "primary-position" else null,
                .seek_window = if (more) .{ .filter = if (self.fallback) null else 0, .offset = self.delivered } else null,
            }, .{});
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const values = try a.alloc(v.Value, 400);
    for (values, 0..) |*value, i| value.* = .{ .integer = @intCast(i) };
    const filters = &[_]b.PropertyFilter{.{ .property = "id", .op = .in, .value = .{ .list = values }, .domain = .integer }};
    for ([_]bool{ false, true }) |fallback| {
        var fixture: Fixture = .{ .fallback = fallback };
        var client: Client = .{ .channel = .{ .ptr = &fixture, .call = Fixture.call }, .incarnation = 1, .revision = 1 };
        const backend = client.backend();
        var after: ?[]const u8 = null;
        while (true) {
            const page = try backend.vtable.scan_where.?(backend.ptr, a, "docs", filters, after, 128);
            after = page.next;
            if (after == null) break;
        }
        try std.testing.expectEqual(@as(usize, if (fallback) 2 else 4), fixture.calls);
        try std.testing.expectEqual(@as(usize, if (fallback) 128 else 400), fixture.delivered);
    }
}

test "probe RPC carries exact EOF admission and rejects malformed partial bindings" {
    const Fixture = struct {
        malformed: bool = false,
        fn call(ptr: *anyopaque, a: A, bytes: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const request = try std.json.parseFromSliceLeaky(Request, a, bytes, .{});
            try std.testing.expectEqual(Operation.probe, request.operation);
            return std.json.Stringify.valueAlloc(a, Reply{ .probe_supported = true, .probe_complete = true, .examined = if (self.malformed) request.limit + 1 else 1 }, .{});
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var fixture: Fixture = .{};
    var client: Client = .{ .channel = .{ .ptr = &fixture, .call = Fixture.call }, .incarnation = 1, .revision = 1 };
    const backend = client.backend();
    const page = (try backend.vtable.probe_where.?(backend.ptr, arena.allocator(), "docs", &.{}, 2)).?;
    try std.testing.expect(page.complete);
    try std.testing.expectEqual(@as(?usize, 1), page.page.examined);
    fixture.malformed = true;
    try std.testing.expectError(error.CypherInvalidBackendPage, backend.vtable.probe_where.?(backend.ptr, arena.allocator(), "docs", &.{}, 2));
}

test "wide sparse domains plan bounded samples before sending original value windows" {
    const Fixture = struct {
        calls: usize = 0,
        delivered: usize = 0,
        fn call(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(body.len <= 1 << 20);
            const request = try std.json.parseFromSliceLeaky(Request, a, body, .{});
            self.calls += 1;
            const filter = request.filters[0];
            if (self.calls == 1) {
                try std.testing.expectEqual(@as(usize, 400), filter.plan.?.length);
                try std.testing.expectEqual(@as(usize, 3), filter.value.array.items.len);
                try std.testing.expect(filter.plan.?.max_string_bytes >= 3000);
            } else {
                try std.testing.expect(filter.plan == null);
                try std.testing.expect(filter.value.array.items.len <= 128);
                self.delivered += filter.value.array.items.len;
            }
            return std.json.Stringify.valueAlloc(a, Reply{
                .next = if (self.delivered < 400) "native" else null,
                .seek_window = if (self.delivered < 400) .{ .filter = 0, .offset = self.delivered } else null,
            }, .{});
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const prefix = try a.alloc(u8, 3000);
    @memset(prefix, 'x');
    const values = try a.alloc(v.Value, 400);
    for (values, 0..) |*value, i| value.* = .{ .string = try std.fmt.allocPrint(a, "{s}{d}", .{ prefix, i }) };
    var fixture: Fixture = .{};
    var client: Client = .{ .channel = .{ .ptr = &fixture, .call = Fixture.call }, .incarnation = 1, .revision = 1 };
    const backend = client.backend();
    var after: ?[]const u8 = null;
    while (true) {
        var quota: @import("budget.zig") = .{ .backing = b.reclaimingAllocator(a), .limit = 256 << 10 };
        var packet_arena = std.heap.ArenaAllocator.init(quota.allocator());
        defer packet_arena.deinit();
        const page = try backend.vtable.scan_where.?(backend.ptr, if (after == null) packet_arena.allocator() else a, "docs", &.{.{ .property = "id", .op = .in, .value = .{ .list = values }, .domain = .string }}, after, 128);
        after = if (page.next) |next| try a.dupe(u8, next) else null;
        if (after == null) break;
    }
    try std.testing.expectEqual(@as(usize, 5), fixture.calls);
    try std.testing.expectEqual(@as(usize, 400), fixture.delivered);
}

test "sampled domains pin unsupported owner fallback to its primary cursor" {
    const Fixture = struct {
        scans: usize = 0,
        fn bind(_: *anyopaque, _: A, _: catalog.Graph) !void {}
        fn checkpoint(_: *anyopaque) !void {}
        fn scan(ptr: *anyopaque, _: A, _: []const u8, after: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.scans += 1;
            if (after) |cursor| try std.testing.expectEqualStrings("primary", cursor);
            return .{ .items = &.{}, .next = if (after == null) "primary" else null };
        }
        fn filtered(_: *anyopaque, _: A, _: []const u8, _: []const b.PropertyFilter, _: ?[]const u8, _: usize) !b.Page(*const v.Node) {
            return error.UnexpectedFilteredResume;
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    const owner: Owner = .{ .backend = .{ .ptr = &fixture, .vtable = &.{ .bind = Fixture.bind, .checkpoint = Fixture.checkpoint, .scan = Fixture.scan, .scan_where = Fixture.filtered, .get = undefined, .expand = undefined } }, .graph = graph, .table = "docs" };
    const first = try owner.executeTyped(a, .{ .operation = .scan, .graph_incarnation = 1, .graph_revision = 1, .table = "docs", .filters = &.{.{ .property = "id", .op = .in, .domain = .string, .value = try std.json.parseFromSliceLeaky(std.json.Value, a, "[\"a\",\"b\",\"c\"]", .{}), .plan = .{ .length = 400, .max_string_bytes = 1 } }} });
    try std.testing.expect(first.seek_window.?.primary);
    const encoded = try @import("sparse_seek.zig").finish(a, .{ .items = &.{}, .next = first.next, .seek_window = first.seek_window });
    const resumed = try @import("sparse_seek.zig").prepare(a, &.{}, encoded.next);
    try std.testing.expect(resumed.primary);
    const last = try owner.executeTyped(a, .{ .operation = .scan, .graph_incarnation = 1, .graph_revision = 1, .table = "docs", .after = resumed.after, .primary = resumed.primary });
    try std.testing.expect(last.next == null);
    try std.testing.expectEqual(@as(usize, 2), fixture.scans);
}
