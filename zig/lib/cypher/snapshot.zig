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

//! Portable immutable graph materialization. Capture all participating owners
//! under their cohort fences, then execute without holding any writer fence.
const std = @import("std");
const root = @import("root.zig");
const Budget = @import("budget.zig");
const A = std.mem.Allocator;
pub const Materialized = struct {
    backing: A,
    budget: *Budget,
    arena: std.heap.ArenaAllocator,
    graph: root.catalog.Graph,
    overlay: root.transaction.Overlay,
    postimages: ?Postimages = null,
    pub const Postimages = struct {
        ptr: *anyopaque,
        normalize: *const fn (*anyopaque, A, *const root.value.Node, bool) anyerror!*const root.value.Node,
        relationships: *const fn (*anyopaque, A, *const root.value.Node) anyerror![]const *const root.value.Relationship,
    };

    pub fn create(a: A, base: root.backend.Backend, graph: root.catalog.Graph, limits: root.runtime.Limits) !*Materialized {
        const self = try a.create(Materialized);
        errdefer a.destroy(self);
        const budget = try a.create(Budget);
        errdefer a.destroy(budget);
        budget.* = .{ .backing = a, .limit = limits.memory_bytes };
        self.* = .{
            .backing = a,
            .budget = budget,
            .arena = std.heap.ArenaAllocator.init(budget.allocator()),
            .graph = graph,
            .overlay = .{ .capture_allocator = budget.allocator(), .base = base, .host = .{ .ptr = self, .new_identity = unavailableIdentity, .commit = unavailableCommit }, .max_entities = limits.intermediate_states, .max_anchors = limits.scanned_anchors, .max_edges = limits.explored_edges },
        };
        errdefer self.arena.deinit();
        self.capture() catch |err| return if (budget.exhausted) error.CypherMemoryBudgetExceeded else err;
        return self;
    }
    fn capture(self: *Materialized) !void {
        const provider = self.overlay.backend();
        try provider.vtable.bind(provider.ptr, self.arena.allocator(), self.graph);
        try provider.vtable.begin.?(provider.ptr);
        // Capture clones each provider page before reclaiming its decode arena.
        try self.overlay.prepareNodeOrder();
        // Published snapshots may serve many transient page allocators. Their
        // capture counters are immutable; the consuming runtime budgets reads.
        self.overlay.capture_accounting = false;
    }
    pub fn deinit(self: *Materialized) void {
        const a = self.backing;
        self.arena.deinit();
        a.destroy(self.budget);
        a.destroy(self);
    }
    pub fn remainingLimits(self: *Materialized, requested: root.runtime.Limits) root.runtime.Limits {
        var result = requested;
        result.memory_bytes -|= self.budget.live;
        return result;
    }
    pub fn backend(self: *Materialized) root.backend.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .expand = expand, .expand_between = expandBetween, .startup_work = startupWork, .normalize_node = normalizeNode, .document_relationships = documentRelationships, .set_work = setWork } };
    }
    fn setWork(ptr: *anyopaque, work: ?root.backend.Work) void {
        cast(ptr).overlay.setExecutionWork(work);
    }
    fn cast(ptr: *anyopaque) *Materialized {
        return @ptrCast(@alignCast(ptr));
    }
    fn unavailableIdentity(_: *anyopaque, _: A, _: []const u8, _: bool) !root.value.Identity {
        return error.CypherMutationsUnavailable;
    }
    fn unavailableCommit(_: *anyopaque, _: A, _: []const root.backend.Mutation) !void {
        return error.CypherMutationsUnavailable;
    }
    fn bind(ptr: *anyopaque, _: A, graph: root.catalog.Graph) !void {
        const self = cast(ptr);
        if (graph.incarnation != self.graph.incarnation or graph.revision != self.graph.revision or !std.mem.eql(u8, graph.name, self.graph.name)) return error.CatalogGenerationChanged;
        try checkpoint(ptr);
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self = cast(ptr);
        try self.overlay.base.vtable.checkpoint(self.overlay.base.ptr);
    }
    fn normalizeNode(ptr: *anyopaque, a: A, node: *const root.value.Node, defaults: bool) !*const root.value.Node {
        const postimages = cast(ptr).postimages orelse return node;
        return postimages.normalize(postimages.ptr, a, node, defaults);
    }
    fn documentRelationships(ptr: *anyopaque, a: A, node: *const root.value.Node) ![]const *const root.value.Relationship {
        const postimages = cast(ptr).postimages orelse return error.CypherMutationsUnavailable;
        return postimages.relationships(postimages.ptr, a, node);
    }
    fn startupWork(ptr: *anyopaque) root.backend.StartupWork {
        return cast(ptr).overlay.captured;
    }
    fn scan(ptr: *anyopaque, a: A, table: []const u8, after: ?[]const u8, limit: usize) !root.backend.Page(*const root.value.Node) {
        const provider = cast(ptr).overlay.backend();
        return provider.vtable.scan(provider.ptr, a, table, after, limit);
    }
    fn get(ptr: *anyopaque, a: A, id: root.value.Identity) !?*const root.value.Node {
        const provider = cast(ptr).overlay.backend();
        return provider.vtable.get(provider.ptr, a, id);
    }
    fn expandBetween(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, id: root.value.Identity, peer: root.value.Identity, direction: root.ast.Direction, filters: []const root.backend.PropertyFilter, after: ?[]const u8, limit: usize, max_examined: usize) !root.backend.Page(*const root.value.Relationship) {
        const provider = cast(ptr).overlay.backend();
        return provider.vtable.expand_between.?(provider.ptr, a, mapping, id, peer, direction, filters, after, limit, max_examined);
    }
    fn expand(ptr: *anyopaque, a: A, mapping: root.catalog.Relationship, id: root.value.Identity, direction: root.ast.Direction, after: ?[]const u8, limit: usize) !root.backend.Page(*const root.value.Relationship) {
        const provider = cast(ptr).overlay.backend();
        return provider.vtable.expand(provider.ptr, a, mapping, id, direction, after, limit);
    }
};
