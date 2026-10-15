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

//! Statement-local graph overlay. Durable commit belongs to the host's native
//! transaction coordinator; no mutation reaches a live owner before commit.
const std = @import("std");
const b = @import("backend.zig");
const v = @import("value.zig");
const c = @import("catalog.zig");
const ast = @import("ast.zig");
const A = std.mem.Allocator;
pub const Host = struct {
    ptr: *anyopaque,
    new_identity: *const fn (*anyopaque, A, []const u8, bool) anyerror!v.Identity,
    commit: *const fn (*anyopaque, A, []const b.Mutation) anyerror!void,
};
pub const Overlay = struct {
    base: b.Backend,
    host: Host,
    graph: ?c.Graph = null,
    alloc: ?A = null,
    active: bool = false,
    staged_read_documents: bool = false,
    nodes: std.ArrayList(Node) = .empty,
    node_positions: std.HashMapUnmanaged(v.Identity, usize, v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty,
    relationships: std.ArrayList(Relationship) = .empty,
    relationship_positions: std.HashMapUnmanaged(v.RelationshipIdentity, usize, v.RelationshipIdentity.Context, std.hash_map.default_max_load_percentage) = .empty,
    mutations: std.ArrayList(b.Mutation) = .empty,
    max_entities: usize = 65536,
    max_anchors: usize = 1_000_000,
    max_edges: usize = 1_000_000,
    captured: b.StartupWork = .{},
    const Node = struct { value: *v.Node, deleted: bool = false, dirty: bool = false, projection_cached: bool = false, projected: ?*const v.Node = null };
    const Relationship = struct { value: *v.Relationship, deleted: bool = false };
    const plain_vtable: b.Backend.VTable = .{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .get = get, .get_relationship = getRelationship, .expand = expand, .search = search, .begin = begin, .startup_work = startupWork, .mutate = mutate, .commit = commit, .rollback = rollback, .new_identity = newIdentity };
    const projecting_vtable: b.Backend.VTable = projected: {
        var value = plain_vtable;
        value.project_node = projectNode;
        break :projected value;
    };
    pub fn backend(self: *Overlay) b.Backend {
        return .{ .ptr = self, .vtable = if (self.base.vtable.project_postimage != null or self.base.vtable.project_node != null) &projecting_vtable else &plain_vtable };
    }
    fn cast(ptr: *anyopaque) *Overlay {
        return @ptrCast(@alignCast(ptr));
    }
    fn bind(ptr: *anyopaque, a: A, graph: c.Graph) !void {
        const self = cast(ptr);
        try self.base.vtable.bind(self.base.ptr, a, graph);
        self.graph = graph;
        self.alloc = a;
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self = cast(ptr);
        try self.base.vtable.checkpoint(self.base.ptr);
    }
    fn capacity(self: *Overlay) !void {
        if (self.nodes.items.len + self.relationships.items.len >= self.max_entities) return error.CypherStateBudgetExceeded;
    }
    fn begin(ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (self.active) return error.CypherTransactionAlreadyActive;
        const a = self.alloc orelse return error.CypherSnapshotRequired;
        self.active = true;
        self.captured = if (self.base.vtable.startup_work) |work| work(self.base.ptr) else .{};
        errdefer rollback(ptr);
        for (self.graph.?.labels, 0..) |label, index| {
            var duplicate = false;
            for (self.graph.?.labels[0..index]) |other| if (std.mem.eql(u8, label.table, other.table)) {
                duplicate = true;
                break;
            };
            if (duplicate) continue;
            var after: ?[]const u8 = null;
            while (true) {
                try checkpoint(ptr);
                const page = try self.base.vtable.scan(self.base.ptr, a, label.table, after, 256);
                const examined = page.examined orelse page.items.len;
                if (examined > self.max_anchors -| self.captured.scanned_anchors) return error.CypherAnchorBudgetExceeded;
                self.captured.scanned_anchors += examined;
                for (page.items) |node| {
                    try self.capacity();
                    const copy = try a.create(v.Node);
                    copy.* = node.*;
                    try self.appendNode(a, .{ .value = copy });
                }
                after = page.next;
                if (after == null) break;
            }
        }
        for (self.graph.?.relationships) |mapping| {
            const source = self.graph.?.label(mapping.source_label).?.table;
            for (self.nodes.items) |node| {
                if (!std.mem.eql(u8, node.value.identity.table, source)) continue;
                var after: ?[]const u8 = null;
                while (true) {
                    try checkpoint(ptr);
                    const page = try self.base.vtable.expand(self.base.ptr, a, mapping, node.value.identity, .out, after, 256);
                    const examined = page.examined orelse page.items.len;
                    if (examined > self.max_edges -| self.captured.explored_edges) return error.CypherEdgeBudgetExceeded;
                    self.captured.explored_edges += examined;
                    for (page.items) |relationship| {
                        if (self.findRelationship(.{ .identity = relationship.identity, .index = relationship.index }) != null) continue;
                        try self.capacity();
                        const copy = try a.create(v.Relationship);
                        copy.* = relationship.*;
                        try self.appendRelationship(a, .{ .value = copy });
                    }
                    after = page.next;
                    if (after == null) break;
                }
            }
        }
        // Producer authority is part of the same retained cut, even when its
        // document fails every vertex predicate. Capture referenced producers
        // before releasing cohort fences; active lookups never return to live
        // storage. Deduplicate lookups independently of contribution fan-in.
        var producers: std.HashMapUnmanaged(v.Identity, bool, v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty;
        defer producers.deinit(a);
        for (self.nodes.items) |node| try producers.put(a, node.value.identity, true);
        for (self.relationships.items) |*relationship| {
            const identity = (try relationship.value.producingDocument(a)) orelse continue;
            const seen = try producers.getOrPut(a, identity);
            if (seen.found_existing) {
                if (!seen.value_ptr.*) relationship.deleted = true;
                continue;
            }
            seen.value_ptr.* = false;
            try checkpoint(ptr);
            if (self.captured.scanned_anchors >= self.max_anchors) return error.CypherAnchorBudgetExceeded;
            self.captured.scanned_anchors += 1;
            const producer = (try self.base.vtable.get(self.base.ptr, a, identity)) orelse {
                relationship.deleted = true;
                continue;
            };
            seen.value_ptr.* = true;
            try self.capacity();
            const copy = try a.create(v.Node);
            copy.* = producer.*;
            try self.appendNode(a, .{ .value = copy });
        }
    }
    /// All node insertion paths maintain the same identity index. Producers are
    /// retained entities too; resolving every edge's authority stays O(1).
    pub fn appendNode(self: *Overlay, a: A, node: Node) !void {
        try self.nodes.ensureUnusedCapacity(a, 1);
        try self.node_positions.put(a, node.value.identity, self.nodes.items.len);
        self.nodes.appendAssumeCapacity(node);
    }
    fn findNode(self: *Overlay, identity: v.Identity) ?*Node {
        const position = self.node_positions.get(identity) orelse return null;
        return &self.nodes.items[position];
    }
    fn startupWork(ptr: *anyopaque) b.StartupWork {
        return cast(ptr).captured;
    }
    pub fn appendRelationship(self: *Overlay, a: A, relationship: Relationship) !void {
        try self.relationships.ensureUnusedCapacity(a, 1);
        const value = relationship.value;
        try self.relationship_positions.put(a, .{ .identity = value.identity, .index = value.index }, self.relationships.items.len);
        self.relationships.appendAssumeCapacity(relationship);
    }
    /// A host can replace an older physical copy when merging staged effects.
    pub fn indexRelationship(self: *Overlay, a: A, position: usize) !void {
        const value = self.relationships.items[position].value;
        try self.relationship_positions.put(a, .{ .identity = value.identity, .index = value.index }, position);
    }
    fn findRelationship(self: *Overlay, identity: v.RelationshipIdentity) ?*Relationship {
        const position = self.relationship_positions.get(identity) orelse return null;
        return &self.relationships.items[position];
    }
    fn scan(ptr: *anyopaque, a: A, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (!self.active) return self.base.vtable.scan(self.base.ptr, a, table, after, limit);
        var found: std.ArrayList(*const v.Node) = .empty;
        for (self.nodes.items) |node| {
            if (node.deleted or !std.mem.eql(u8, node.value.identity.table, table)) continue;
            const vertex = for (self.graph.?.labels) |label| {
                if (self.graph.?.admits(node.value, label.name)) break true;
            } else false;
            if (!vertex) continue;
            if (after) |key| if (std.mem.order(u8, node.value.identity.key, key) != .gt) continue;
            try found.append(a, node.value);
        }
        std.mem.sort(*const v.Node, found.items, {}, struct {
            fn less(_: void, left: *const v.Node, right: *const v.Node) bool {
                return std.mem.order(u8, left.identity.key, right.identity.key) == .lt;
            }
        }.less);
        const length = @min(limit, found.items.len);
        return .{ .items = found.items[0..length], .next = if (length < found.items.len and length != 0) found.items[length - 1].identity.key else null, .examined = length };
    }
    fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (!self.active) return self.base.vtable.get(self.base.ptr, a, identity);
        const node = self.findNode(identity) orelse return null;
        return if (node.deleted) null else node.value;
    }
    fn getRelationship(ptr: *anyopaque, a: A, identity: v.RelationshipIdentity) !?*const v.Relationship {
        const self = cast(ptr);
        try checkpoint(ptr);
        // Replacement projections update the identity index, so retained aliases
        // resolve the current copy without scanning retired versions.
        const relationship = self.findRelationship(identity) orelse return null;
        return if (!relationship.deleted and try self.producerVisible(a, relationship.value)) relationship.value else null;
    }
    fn projectNode(ptr: *anyopaque, a: A, value: *const v.Node) !?*const v.Node {
        const self = cast(ptr);
        try checkpoint(ptr);
        const node = self.findNode(value.identity) orelse return value;
        if (!node.dirty) return node.value;
        if (!node.projection_cached) {
            const project = self.base.vtable.project_postimage orelse self.base.vtable.project_node orelse return node.value;
            node.projected = try project(self.base.ptr, a, node.value);
            node.projection_cached = true;
        }
        return node.projected;
    }
    fn expand(ptr: *anyopaque, a: A, mapping: c.Relationship, identity: v.Identity, direction: ast.Direction, after: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (!self.active) return self.base.vtable.expand(self.base.ptr, a, mapping, identity, direction, after, limit);
        const offset = if (after) |cursor| try std.fmt.parseInt(usize, cursor, 10) else 0;
        var found: std.ArrayList(*const v.Relationship) = .empty;
        var next: ?[]const u8 = null;
        for (self.relationships.items[offset..], offset..) |item, index| {
            if (item.deleted or !std.mem.eql(u8, item.value.type, mapping.name) or !std.mem.eql(u8, item.value.index, mapping.index)) continue;
            if (!((direction != .in and item.value.source.eql(identity)) or (direction != .out and item.value.target.eql(identity)))) continue;
            if (!try self.producerVisible(a, item.value)) continue;
            if (found.items.len == limit) {
                next = try std.fmt.allocPrint(a, "{d}", .{index});
                break;
            }
            try found.append(a, item.value);
        }
        return .{ .items = try found.toOwnedSlice(a), .next = next };
    }
    fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
        const self = cast(ptr);
        // Native rankers cannot see an uncommitted overlay. Failing here avoids
        // silently returning a mixture of pre-write scores and post-write rows.
        if (self.mutations.items.len != 0 or self.staged_read_documents) return error.CypherSearchAfterWrite;
        return (self.base.vtable.search orelse return error.CypherSearchUnavailable)(self.base.ptr, a, table, input);
    }
    fn properties(a: A, old: []const v.Entry, updates: []const v.Entry, replace: bool) ![]const v.Entry {
        var output: std.ArrayList(v.Entry) = .empty;
        if (!replace) for (old) |entry| {
            var changed = false;
            for (updates) |update| if (std.mem.eql(u8, entry.name, update.name)) {
                changed = true;
                break;
            };
            if (!changed) try output.append(a, entry);
        };
        for (updates) |entry| if (entry.value != .null) try output.append(a, entry);
        return output.toOwnedSlice(a);
    }
    /// Install an already-normalized SQL postimage in a read-only view.
    /// SQL has applied defaults/generated columns; re-running a Cypher CREATE
    /// normalizer here would turn an explicit SQL NULL back into a default.
    pub fn stageReadDocument(self: *Overlay, a: A, identity: v.Identity, postimage: ?*const v.Node) !void {
        if (!self.active) return error.CypherTransactionRequired;
        const copy = if (postimage) |node| blk: {
            if (!node.identity.eql(identity)) return error.CypherInvalidIdentity;
            break :blk (try (v.Value{ .node = node }).clone(a)).node;
        } else blk: {
            const tombstone = try a.create(v.Node);
            tombstone.* = .{ .identity = identity };
            break :blk tombstone;
        };
        var labels: std.ArrayList([]const u8) = .empty;
        if (postimage != null) for (self.graph.?.labels) |label| {
            if (self.graph.?.admits(copy, label.name)) try labels.append(a, label.name);
        };
        @constCast(copy).labels = labels.items;
        if (self.findNode(identity)) |node| {
            node.* = .{ .value = @constCast(copy), .deleted = postimage == null, .dirty = true };
        } else {
            try self.capacity();
            try self.appendNode(a, .{ .value = @constCast(copy), .deleted = postimage == null, .dirty = true });
        }
        self.staged_read_documents = true;
        if (postimage != null) {
            try self.refreshDocumentRelationships(a, copy);
        } else {
            for (self.relationships.items) |*relationship| {
                if (try ownedBy(a, relationship.value, identity)) relationship.deleted = true;
            }
        }
    }

    fn set(self: *Overlay, a: A, target: v.EntityIdentity, updates: []const v.Entry, replace: bool) !void {
        switch (target) {
            .node => |identity| {
                const node = self.findNode(identity) orelse return error.CypherDeletedEntity;
                if (node.deleted) return error.CypherDeletedEntity;
                node.value.properties = try properties(a, node.value.properties, updates, replace);
                node.dirty = true;
                node.projection_cached = false;
                if (self.base.vtable.normalize_node) |normalize| node.value.* = (try normalize(self.base.ptr, a, node.value, false)).*;
                try self.refreshDocumentRelationships(a, node.value);
            },
            .relationship => |identity| {
                const relationship = self.findRelationship(identity) orelse return error.CypherDeletedEntity;
                if (relationship.deleted) return error.CypherDeletedEntity;
                if (relationship.value.ownership != .explicit) return error.CypherDerivedRelationshipMutation;
                relationship.value.properties = try properties(a, relationship.value.properties, updates, replace);
            },
        }
    }
    pub fn ownedBy(a: A, relationship: *const v.Relationship, identity: v.Identity) !bool {
        const producer = (try relationship.producingDocument(a)) orelse return false;
        return producer.eql(identity);
    }
    fn producerVisible(self: *Overlay, a: A, relationship: *const v.Relationship) !bool {
        const identity = (try relationship.producingDocument(a)) orelse return true;
        // Initial producer admission belongs to the retained backend. Recheck
        // changed rows against postimage USING authority, independently of the
        // graph label predicates and the relationship's traversal endpoints.
        const node = self.findNode(identity) orelse return false;
        if (node.deleted) return false;
        return (try projectNode(self, a, node.value)) != null;
    }
    fn refreshDocumentRelationships(self: *Overlay, a: A, node: *const v.Node) !void {
        const project = self.base.vtable.document_relationships orelse return;
        const relationships = try project(self.base.ptr, a, node);
        for (self.relationships.items) |*relationship| {
            if (relationship.value.ownership == .field and try ownedBy(a, relationship.value, node.identity)) relationship.deleted = true;
        }
        for (relationships) |relationship| {
            try self.capacity();
            if (self.captured.explored_edges >= self.max_edges) return error.CypherEdgeBudgetExceeded;
            self.captured.explored_edges += 1;
            const copy = try a.create(v.Relationship);
            copy.* = relationship.*;
            try self.appendRelationship(a, .{ .value = copy });
        }
    }
    fn mutate(ptr: *anyopaque, a: A, mutation: b.Mutation) !void {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (!self.active) return error.CypherTransactionRequired;
        try mutation.validateProperties();
        switch (mutation) {
            .create_node => |node| {
                if (self.findNode(node.identity) != null) return error.CypherDuplicateIdentity;
                try self.capacity();
                const copy = try a.create(v.Node);
                copy.* = node.*;
                if (self.base.vtable.normalize_node) |normalize| copy.* = (try normalize(self.base.ptr, a, copy, true)).*;
                // Normalization may refresh labels. Validate the requested labels
                // against defaults and generated values before staging the node.
                for (node.labels) |name| if (!self.graph.?.admits(copy, name)) return error.CypherLabelPredicateViolation;
                try self.appendNode(a, .{ .value = copy, .dirty = true });
                try self.refreshDocumentRelationships(a, copy);
            },
            .create_relationship => |relationship| {
                for ([_]v.Identity{ relationship.source, relationship.target }) |identity| {
                    const endpoint = self.findNode(identity) orelse return error.CypherDeletedEntity;
                    if (endpoint.deleted) return error.CypherDeletedEntity;
                }
                if (self.findRelationship(.{ .identity = relationship.identity, .index = relationship.index }) != null) return error.CypherDuplicateIdentity;
                try self.capacity();
                const copy = try a.create(v.Relationship);
                copy.* = relationship.*;
                copy.properties = try v.entityProperties(a, relationship.properties);
                try self.appendRelationship(a, .{ .value = copy });
            },
            .set_properties => |update| try self.set(a, update.target, update.properties, update.replace),
            .remove_properties => |update| {
                const entries = try a.alloc(v.Entry, update.names.len);
                for (update.names, entries) |name, *entry| entry.* = .{ .name = name, .value = .null };
                try self.set(a, update.target, entries, false);
            },
            .delete_relationship => |identity| {
                const relationship = self.findRelationship(identity) orelse return error.CypherDeletedEntity;
                if (relationship.value.ownership != .explicit) return error.CypherDerivedRelationshipMutation;
                relationship.deleted = true;
            },
            .delete_node => |deletion| {
                const node = self.findNode(deletion.identity) orelse return error.CypherDeletedEntity;
                for (self.relationships.items) |*relationship| {
                    if (relationship.deleted) continue;
                    if (try ownedBy(a, relationship.value, deletion.identity) and !(relationship.value.source.eql(deletion.identity) or relationship.value.target.eql(deletion.identity))) {
                        relationship.deleted = true;
                        continue;
                    }
                    if (!(relationship.value.source.eql(deletion.identity) or relationship.value.target.eql(deletion.identity))) continue;
                    if (!deletion.detach) return error.CypherNodeHasRelationships;
                    relationship.deleted = true;
                }
                node.deleted = true;
            },
        }
        try self.mutations.append(a, mutation);
    }
    fn newIdentity(ptr: *anyopaque, a: A, table: []const u8, relationship: bool) !v.Identity {
        const self = cast(ptr);
        return self.host.new_identity(self.host.ptr, a, table, relationship);
    }
    fn commit(ptr: *anyopaque) !void {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (!self.active) return error.CypherTransactionRequired;
        if (self.staged_read_documents) return error.CypherMutationsUnavailable;
        try self.host.commit(self.host.ptr, self.alloc.?, self.mutations.items);
        self.active = false;
    }
    fn rollback(ptr: *anyopaque) void {
        const self = cast(ptr);
        self.active = false;
        self.staged_read_documents = false;
        self.nodes = .empty;
        self.node_positions = .empty;
        self.relationships = .empty;
        self.relationship_positions = .empty;
        self.mutations = .empty;
    }
};
