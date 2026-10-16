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
    /// Reclaim provider page/decode buffers after retaining independent values.
    /// This must support individual frees (the retained arena cannot).
    capture_allocator: ?A = null,
    active: bool = false,
    /// Base callbacks must share a retained statement cut. Only touched values
    /// are copied; untouched adjacency remains in the native indexes.
    lazy: bool = false,
    /// Writers retain the preimages already returned by their scan. Read-only
    /// SQL overlays keep untouched rows page-owned for fused streaming.
    retain_scan_preimages: bool = false,
    replaced_producers: v.IdentitySet = .empty,
    work: ?b.Work = null,
    capture_accounting: bool = true,
    node_order: ?[]const usize = null,
    staged_order: std.ArrayList(usize) = .empty,
    staged_order_valid: bool = false,
    references: std.HashMapUnmanaged(v.Identity, std.ArrayList(Reference), v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty,
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
    const Reference = struct { position: usize, version: usize };
    const Node = struct { value: *v.Node, deleted: bool = false, created: bool = false, dirty: bool = false, projection_cached: bool = false, projected: ?*const v.Node = null };
    const Relationship = struct { value: *v.Relationship, deleted: bool = false, created: bool = false, dirty: bool = false, version: usize = 0, from_base: bool = false };
    const plain_vtable: b.Backend.VTable = .{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .scan_where = scanWhere, .get = get, .get_relationship = getRelationship, .expand = expand, .search = search, .set_work = setWork, .set_read_fields = setReadFields, .begin = begin, .startup_work = startupWork, .mutate = mutate, .commit = commit, .rollback = rollback, .new_identity = newIdentity };
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
    fn setReadFields(ptr: *anyopaque, fields: ?[]const []const u8) void {
        const self = cast(ptr);
        if (self.base.vtable.set_read_fields) |configure| configure(self.base.ptr, if (self.active and (self.nodes.items.len != 0 or self.retain_scan_preimages)) null else fields);
    }
    fn setWork(ptr: *anyopaque, work: ?b.Work) void {
        cast(ptr).setExecutionWork(work);
    }
    pub fn setExecutionWork(self: *Overlay, work: ?b.Work) void {
        self.work = work;
        if (self.base.vtable.set_work) |configure| configure(self.base.ptr, work);
        if (work != null) self.capture_accounting = false;
    }
    pub fn valueWork(self: *Overlay, a: A) @import("value_work.zig").Work {
        return .{ .a = a, .ptr = self, .charge = chargeMembership };
    }
    fn chargeMembership(ptr: *anyopaque, amount: usize) !void {
        try cast(ptr).charge(amount);
    }
    fn remainingWork(ptr: *anyopaque) usize {
        const self = cast(ptr);
        if (self.work) |work| if (work.remaining) |remaining| return remaining(work.ptr);
        return self.max_entities -| self.captured.intermediate_states;
    }
    fn captureValue(ptr: *anyopaque, _: v.Value) !void {
        try cast(ptr).charge(1);
    }
    pub fn relabel(self: *Overlay, a: A, node: *v.Node) !void {
        var work = self.valueWork(a);
        var labels: std.ArrayList([]const u8) = .empty;
        for (self.graph.?.labels) |label| if (try self.graph.?.admits(node, label.name, &work)) try labels.append(a, label.name);
        node.labels = try labels.toOwnedSlice(a);
    }
    pub fn charge(self: *Overlay, amount: usize) !void {
        if (self.work) |work| return work.charge(work.ptr, amount);
        try self.base.vtable.checkpoint(self.base.ptr);
        if (!self.capture_accounting) return;
        // Snapshot construction precedes runtime.execute, but still consumes
        // the same statement ceiling and reports its work to that runtime.
        if (amount > self.max_entities -| self.captured.intermediate_states) return error.CypherStateBudgetExceeded;
        self.captured.intermediate_states += amount;
    }
    pub fn chargeValue(self: *Overlay, value: v.Value) !void {
        if (self.work) |work| return work.value(work.ptr, value);
        try self.charge(1);
    }
    fn capacity(self: *Overlay) !void {
        if (self.nodes.items.len + self.relationships.items.len >= self.max_entities) return error.CypherStateBudgetExceeded;
    }
    fn begin(ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (self.active) return error.CypherTransactionAlreadyActive;
        const a = self.alloc orelse return error.CypherSnapshotRequired;
        self.active = true;
        // Native decoding and routed owner predicates run before execution.
        // Charge that work to this capture, then release the borrowed meter.
        if (self.base.vtable.set_work) |configure| configure(self.base.ptr, self.work orelse .{ .ptr = self, .charge = chargeMembership, .value = captureValue, .remaining = remainingWork });
        defer if (self.base.vtable.set_work) |configure| configure(self.base.ptr, self.work);
        self.captured = if (self.base.vtable.startup_work) |work| work(self.base.ptr) else .{};
        errdefer rollback(ptr);
        if (self.lazy) return;
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
                var scratch = std.heap.ArenaAllocator.init(self.capture_allocator orelse a);
                defer scratch.deinit();
                const page_a = if (self.capture_allocator != null) scratch.allocator() else a;
                const page = try self.base.vtable.scan(self.base.ptr, page_a, label.table, after, 256);
                const examined = page.examined orelse page.items.len;
                if (examined > self.max_anchors -| self.captured.scanned_anchors) return error.CypherAnchorBudgetExceeded;
                self.captured.scanned_anchors += examined;
                for (page.items) |node| {
                    try self.capacity();
                    const copy = try self.retainNode(a, node);
                    try self.appendNode(a, .{ .value = copy });
                }
                after = if (page.next) |next| if (self.capture_allocator != null) try a.dupe(u8, next) else next else null;
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
                    var scratch = std.heap.ArenaAllocator.init(self.capture_allocator orelse a);
                    defer scratch.deinit();
                    const page_a = if (self.capture_allocator != null) scratch.allocator() else a;
                    const page = try self.base.vtable.expand(self.base.ptr, page_a, mapping, node.value.identity, .out, after, 256);
                    const examined = page.examined orelse page.items.len;
                    if (examined > self.max_edges -| self.captured.explored_edges) return error.CypherEdgeBudgetExceeded;
                    self.captured.explored_edges += examined;
                    for (page.items) |relationship| {
                        if (self.findRelationship(.{ .identity = relationship.identity, .index = relationship.index }) != null) continue;
                        try self.capacity();
                        const copy = if (self.capture_allocator != null) @constCast((try (v.Value{ .relationship = relationship }).clone(a)).relationship) else blk: {
                            const copy = try a.create(v.Relationship);
                            copy.* = relationship.*;
                            break :blk copy;
                        };
                        try self.appendRelationship(a, .{ .value = copy });
                    }
                    after = if (page.next) |next| if (self.capture_allocator != null) try a.dupe(u8, next) else next else null;
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
        for (self.relationships.items) |*relationship| {
            const identity = (try relationship.value.producingDocument(a)) orelse continue;
            // Admitted vertices already have a deduplicated identity index.
            // Only off-graph producer lookups need a second map; preloading
            // all vertices wastes retained memory even on edgeless graphs.
            if (self.node_positions.contains(identity)) continue;
            const seen = try producers.getOrPut(a, identity);
            if (seen.found_existing) {
                if (!seen.value_ptr.*) relationship.deleted = true;
                continue;
            }
            seen.value_ptr.* = false;
            try checkpoint(ptr);
            if (self.captured.scanned_anchors >= self.max_anchors) return error.CypherAnchorBudgetExceeded;
            self.captured.scanned_anchors += 1;
            var scratch = std.heap.ArenaAllocator.init(self.capture_allocator orelse a);
            defer scratch.deinit();
            const page_a = if (self.capture_allocator != null) scratch.allocator() else a;
            const producer = (try self.base.vtable.get(self.base.ptr, page_a, identity)) orelse {
                relationship.deleted = true;
                continue;
            };
            seen.value_ptr.* = true;
            try self.capacity();
            const copy = try self.retainNode(a, producer);
            try self.appendNode(a, .{ .value = copy });
        }
    }
    fn retainNode(self: *Overlay, a: A, node: *const v.Node) !*v.Node {
        if (self.capture_allocator != null) return @constCast((try (v.Value{ .node = node }).clone(a)).node);
        const copy = try a.create(v.Node);
        copy.* = node.*;
        return copy;
    }
    /// All node insertion paths maintain the same identity index. Producers are
    /// retained entities too; resolving every edge's authority stays O(1).
    pub fn appendNode(self: *Overlay, a: A, node: Node) !void {
        if (node.dirty) self.staged_order_valid = false;
        try self.nodes.ensureUnusedCapacity(a, 1);
        try self.node_positions.put(a, node.value.identity, self.nodes.items.len);
        self.nodes.appendAssumeCapacity(node);
        self.node_order = null;
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
        try self.indexReferences(a, self.relationships.items.len - 1);
    }
    fn indexReferences(self: *Overlay, a: A, position: usize) !void {
        const relationship = &self.relationships.items[position];
        relationship.version = try std.math.add(usize, relationship.version, 1);
        const value = relationship.value;
        const producer = try value.producingDocument(a);
        const identities = [_]?v.Identity{ value.source, value.target, producer };
        for (identities, 0..) |optional, index| {
            const identity = optional orelse continue;
            var duplicate = false;
            for (identities[0..index]) |previous| if (previous) |other| {
                if (identity.eql(other)) duplicate = true;
            };
            if (duplicate) continue;
            try self.charge(1);
            const slot = try self.references.getOrPut(a, identity);
            if (!slot.found_existing) slot.value_ptr.* = .empty;
            try slot.value_ptr.append(a, .{ .position = position, .version = relationship.version });
        }
    }
    fn currentReference(self: *Overlay, reference: Reference) ?*Relationship {
        const relationship = &self.relationships.items[reference.position];
        return if (relationship.version == reference.version) relationship else null;
    }
    /// A host can replace an older physical copy when merging staged effects.
    pub fn indexRelationship(self: *Overlay, a: A, position: usize) !void {
        const value = self.relationships.items[position].value;
        try self.relationship_positions.put(a, .{ .identity = value.identity, .index = value.index }, position);
        try self.indexReferences(a, position);
    }
    fn findRelationship(self: *Overlay, identity: v.RelationshipIdentity) ?*Relationship {
        const position = self.relationship_positions.get(identity) orelse return null;
        return &self.relationships.items[position];
    }
    /// Cached pages use the overlay's allocator, never a caller's transient
    /// page allocator. Materialized snapshots prepare this before publication.
    pub fn prepareNodeOrder(self: *Overlay) !void {
        if (self.node_order == null) {
            const order = try self.alloc.?.alloc(usize, self.nodes.items.len);
            // Sort once per structural change, not once per scan page. Sorting
            // is bounded before entering the standard library's comparator.
            const levels = if (order.len == 0) 0 else std.math.log2_int(usize, order.len) + 1;
            try self.charge(order.len *| levels);
            for (order, 0..) |*position, index| position.* = index;
            std.mem.sort(usize, order, self, struct {
                fn less(overlay: *Overlay, left: usize, right: usize) bool {
                    const x = overlay.nodes.items[left].value.identity;
                    const y = overlay.nodes.items[right].value.identity;
                    const tables = std.mem.order(u8, x.table, y.table);
                    return tables == .lt or (tables == .eq and std.mem.order(u8, x.key, y.key) == .lt);
                }
            }.less);
            self.node_order = order;
        }
    }
    fn ensureNode(self: *Overlay, a: A, identity: v.Identity) anyerror!?*Node {
        if (self.findNode(identity)) |node| return node;
        if (!self.lazy) return null;
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const retained = (try self.base.vtable.get(self.base.ptr, scratch.allocator(), identity)) orelse return null;
        try self.capacity();
        const retained_a = self.alloc.?;
        const copy = @constCast((try (v.Value{ .node = retained }).clone(retained_a)).node);
        try self.appendNode(retained_a, .{ .value = copy });
        return self.findNode(identity);
    }
    fn prepareStagedOrder(self: *Overlay) !void {
        if (self.staged_order_valid) return;
        const a = self.alloc.?;
        self.staged_order.clearRetainingCapacity();
        for (self.nodes.items, 0..) |node, index| if (node.dirty) try self.staged_order.append(a, index);
        std.mem.sort(usize, self.staged_order.items, self, struct {
            fn less(overlay: *Overlay, left: usize, right: usize) bool {
                const x = overlay.nodes.items[left].value.identity;
                const y = overlay.nodes.items[right].value.identity;
                const tables = std.mem.order(u8, x.table, y.table);
                return tables == .lt or (tables == .eq and std.mem.lessThan(u8, x.key, y.key));
            }
        }.less);
        self.staged_order_valid = true;
    }
    fn lazyScan(self: *Overlay, a: A, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        if (limit == 0) return error.CypherInvalidBackendPage;
        const page = try self.base.vtable.scan(self.base.ptr, a, table, after, limit);
        var found: std.ArrayList(*const v.Node) = .empty;
        var present: v.IdentitySet = .empty;
        for (page.items) |value| {
            try present.put(a, value.identity, {});
            if (self.findNode(value.identity)) |node| {
                if (!node.deleted and node.value.labels.len != 0) try found.append(a, node.value);
            } else {
                // Scanning is not a write/read-set admission. Untouched rows
                // remain page-owned; mutation/get imports a private copy only
                // when the statement actually needs to retain that identity.
                if (self.retain_scan_preimages) {
                    try self.capacity();
                    const retained = self.alloc.?;
                    // The runtime's write scan uses its statement allocator.
                    // Borrow immutable properties there; clone transient pages.
                    const copy = if (a.ptr == retained.ptr and a.vtable == retained.vtable)
                        try self.retainNode(retained, value)
                    else
                        @constCast((try (v.Value{ .node = value }).clone(retained)).node);
                    try self.appendNode(retained, .{ .value = copy });
                    try found.append(a, copy);
                } else try found.append(a, value);
            }
        }
        try self.prepareStagedOrder();
        const order = self.staged_order.items;
        var lo: usize = 0;
        var hi = order.len;
        while (lo < hi) {
            const middle = lo + (hi - lo) / 2;
            const identity = self.nodes.items[order[middle]].value.identity;
            const tables = std.mem.order(u8, identity.table, table);
            if (tables == .lt or (tables == .eq and if (after) |key| std.mem.order(u8, identity.key, key) != .gt else false)) lo = middle + 1 else hi = middle;
        }
        var staged: usize = 0;
        var examined = page.examined orelse page.items.len;
        for (order[lo..]) |index| {
            const node = self.nodes.items[index];
            if (!std.mem.eql(u8, node.value.identity.table, table)) break;
            if (page.next) |key| if (std.mem.order(u8, node.value.identity.key, key) == .gt) break;
            if (present.contains(node.value.identity)) continue;
            try self.charge(1);
            examined += 1;
            if (node.deleted or node.value.labels.len == 0) continue;
            try found.append(a, node.value);
            staged += 1;
            // Later staged keys cannot enter this page's retained prefix.
            if (staged > limit) break;
        }
        std.mem.sort(*const v.Node, found.items, {}, struct {
            fn less(_: void, left: *const v.Node, right: *const v.Node) bool {
                return std.mem.lessThan(u8, left.identity.key, right.identity.key);
            }
        }.less);
        const overflow = found.items.len > limit;
        const items = found.items[0..@min(limit, found.items.len)];
        return .{ .items = items, .next = if (overflow) items[items.len - 1].identity.key else page.next, .examined = examined };
    }
    const LazyCursor = struct { staged: bool = false, native: ?[]const u8 = null, offset: usize = 0 };
    fn lazyExpand(self: *Overlay, a: A, mapping: c.Relationship, identity: v.Identity, direction: ast.Direction, after: ?[]const u8, limit: usize) anyerror!b.Page(*const v.Relationship) {
        if (limit == 0) return error.CypherInvalidBackendPage;
        const cursor = if (after) |encoded| try std.json.parseFromSliceLeaky(LazyCursor, a, encoded, .{}) else LazyCursor{};
        var found: std.ArrayList(*const v.Relationship) = .empty;
        if (!cursor.staged) {
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const page = try self.base.vtable.expand(self.base.ptr, temporary, mapping, identity, direction, cursor.native, limit);
            for (page.items) |value| {
                if (value.ownership == .field) {
                    if (try value.producingDocument(temporary)) |producer| if (self.replaced_producers.contains(producer)) continue;
                }
                const item = if (self.findRelationship(.{ .identity = value.identity, .index = value.index })) |cached| cached else blk: {
                    try self.capacity();
                    const retained_a = self.alloc.?;
                    const copy = @constCast((try (v.Value{ .relationship = value }).clone(retained_a)).relationship);
                    try self.appendRelationship(retained_a, .{ .value = copy, .from_base = true });
                    break :blk self.findRelationship(.{ .identity = value.identity, .index = value.index }).?;
                };
                // Staged replacements are emitted only in the staged phase.
                // This also lets hosts install exact tombstones without first
                // enumerating the native adjacency that they mask.
                if (!item.dirty and !item.created and !item.deleted and try self.producerVisible(a, item.value)) try found.append(a, item.value);
            }
            return .{ .items = try found.toOwnedSlice(a), .next = try std.json.Stringify.valueAlloc(a, LazyCursor{ .staged = page.next == null, .native = page.next }, .{}), .examined = page.examined orelse page.items.len };
        }
        const references = if (self.references.get(identity)) |list| list.items else &.{};
        if (cursor.offset > references.len) return error.CypherInvalidBackendCursor;
        const end = cursor.offset + @min(limit, references.len - cursor.offset);
        for (references[cursor.offset..end]) |reference| {
            try self.charge(1);
            const item = self.currentReference(reference) orelse continue;
            if ((item.from_base and !item.dirty and !item.created) or item.deleted or !std.mem.eql(u8, item.value.type, mapping.name) or !std.mem.eql(u8, item.value.index, mapping.index)) continue;
            if (!((direction != .in and item.value.source.eql(identity)) or (direction != .out and item.value.target.eql(identity)))) continue;
            if (try self.producerVisible(a, item.value)) try found.append(a, item.value);
        }
        return .{ .items = try found.toOwnedSlice(a), .next = if (end < references.len) try std.json.Stringify.valueAlloc(a, LazyCursor{ .staged = true, .offset = end }, .{}) else null, .examined = end - cursor.offset };
    }
    fn loadIncidents(self: *Overlay, a: A, identity: v.Identity) !void {
        if (!self.lazy) return;
        for (self.graph.?.relationships) |mapping| {
            var after: ?[]const u8 = null;
            while (true) {
                const page = try self.lazyExpand(a, mapping, identity, .both, after, 256);
                const examined = page.examined orelse page.items.len;
                if (examined > self.max_edges -| self.captured.explored_edges) return error.CypherEdgeBudgetExceeded;
                self.captured.explored_edges += examined;
                after = page.next;
                if (after == null) break;
            }
        }
    }
    fn scanWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, after: ?[]const u8, limit: usize) anyerror!b.Page(*const v.Node) {
        const self = cast(ptr);
        try checkpoint(ptr);
        const filtered = self.base.vtable.scan_where orelse return scan(ptr, a, table, after, limit);
        if (!self.active) return filtered(self.base.ptr, a, table, filters, after, limit);
        if (!self.lazy or filters.len == 0) return scan(ptr, a, table, after, limit);
        if (limit == 0) return error.CypherInvalidBackendPage;
        const cursor = if (after) |encoded| try std.json.parseFromSliceLeaky(LazyCursor, a, encoded, .{}) else LazyCursor{};
        var found: std.ArrayList(*const v.Node) = .empty;
        if (!cursor.staged) {
            const page = try filtered(self.base.ptr, a, table, filters, cursor.native, limit);
            for (page.items) |value| {
                if (self.findNode(value.identity)) |node| {
                    // Emit every dirty postimage exactly once in the staged
                    // phase, even if its indexed value moved into/out of range.
                    if (!node.dirty and !node.deleted and node.value.labels.len != 0) try found.append(a, node.value);
                } else if (self.retain_scan_preimages) {
                    try self.capacity();
                    const retained = self.alloc.?;
                    const copy = if (a.ptr == retained.ptr and a.vtable == retained.vtable) try self.retainNode(retained, value) else @constCast((try (v.Value{ .node = value }).clone(retained)).node);
                    try self.appendNode(retained, .{ .value = copy });
                    try found.append(a, copy);
                } else try found.append(a, value);
            }
            return .{ .items = try found.toOwnedSlice(a), .examined = page.examined orelse page.items.len, .next = try std.json.Stringify.valueAlloc(a, LazyCursor{ .staged = page.next == null, .native = page.next }, .{}) };
        }
        try self.prepareStagedOrder();
        const order = self.staged_order.items;
        if (cursor.offset > order.len) return error.CypherInvalidBackendCursor;
        var position = cursor.offset;
        if (position == 0) {
            var hi = order.len;
            while (position < hi) {
                const middle = position + (hi - position) / 2;
                if (std.mem.order(u8, self.nodes.items[order[middle]].value.identity.table, table) == .lt) position = middle + 1 else hi = middle;
            }
        }
        var examined: usize = 0;
        while (position < order.len and examined < limit) : (position += 1) {
            const node = self.nodes.items[order[position]];
            if (!std.mem.eql(u8, node.value.identity.table, table)) break;
            try self.charge(1);
            examined += 1;
            if (!node.deleted and node.value.labels.len != 0) try found.append(a, node.value);
        }
        const more = position < order.len and std.mem.eql(u8, self.nodes.items[order[position]].value.identity.table, table);
        return .{ .items = try found.toOwnedSlice(a), .examined = examined, .next = if (more) try std.json.Stringify.valueAlloc(a, LazyCursor{ .staged = true, .offset = position }, .{}) else null };
    }
    fn scan(ptr: *anyopaque, a: A, table: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (!self.active) return self.base.vtable.scan(self.base.ptr, a, table, after, limit);
        if (self.lazy) return self.lazyScan(a, table, after, limit);
        if (limit == 0) return error.CypherInvalidBackendPage;
        try self.prepareNodeOrder();
        const order = self.node_order.?;
        var lo: usize = 0;
        var hi = order.len;
        while (lo < hi) {
            const middle = lo + (hi - lo) / 2;
            const identity = self.nodes.items[order[middle]].value.identity;
            const tables = std.mem.order(u8, identity.table, table);
            if (tables == .lt or (tables == .eq and if (after) |key| std.mem.order(u8, identity.key, key) != .gt else false)) lo = middle + 1 else hi = middle;
        }
        var found: std.ArrayList(*const v.Node) = .empty;
        var examined: usize = 0;
        var last: ?[]const u8 = null;
        var position = lo;
        while (position < order.len) : (position += 1) {
            const node = self.nodes.items[order[position]];
            if (!std.mem.eql(u8, node.value.identity.table, table)) break;
            if (examined == limit) break;
            try self.charge(1);
            examined += 1;
            last = node.value.identity.key;
            if (node.deleted) continue;
            var membership = self.valueWork(a);
            const vertex = for (self.graph.?.labels) |label| {
                if (try self.graph.?.admits(node.value, label.name, &membership)) break true;
            } else false;
            if (vertex) try found.append(a, node.value);
        }
        const more = position < order.len and std.mem.eql(u8, self.nodes.items[order[position]].value.identity.table, table);
        return .{ .items = try found.toOwnedSlice(a), .next = if (more) last else null, .examined = examined };
    }
    fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (!self.active) return self.base.vtable.get(self.base.ptr, a, identity);
        const node = (try self.ensureNode(a, identity)) orelse return null;
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
        if (limit == 0) return error.CypherInvalidBackendPage;
        if (self.lazy) return self.lazyExpand(a, mapping, identity, direction, after, limit);
        const references = if (self.references.get(identity)) |list| list.items else &.{};
        const offset = if (after) |cursor| try std.fmt.parseUnsigned(usize, cursor, 10) else 0;
        if (offset > references.len) return error.CypherInvalidBackendCursor;
        var found: std.ArrayList(*const v.Relationship) = .empty;
        const end = offset + @min(limit, references.len - offset);
        for (references[offset..end]) |reference| {
            try self.charge(1);
            const item = self.currentReference(reference) orelse continue;
            if (item.deleted or !std.mem.eql(u8, item.value.type, mapping.name) or !std.mem.eql(u8, item.value.index, mapping.index)) continue;
            if (!((direction != .in and item.value.source.eql(identity)) or (direction != .out and item.value.target.eql(identity)))) continue;
            if (!try self.producerVisible(a, item.value)) continue;
            try found.append(a, item.value);
        }
        return .{ .items = try found.toOwnedSlice(a), .next = if (end < references.len) try std.fmt.allocPrint(a, "{d}", .{end}) else null, .examined = end - offset };
    }
    fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
        const self = cast(ptr);
        // Native rankers cannot see an uncommitted overlay. Failing here avoids
        // silently returning a mixture of pre-write scores and post-write rows.
        if (self.mutations.items.len != 0 or self.staged_read_documents) return error.CypherSearchAfterWrite;
        return (self.base.vtable.search orelse return error.CypherSearchUnavailable)(self.base.ptr, a, table, input);
    }
    fn properties(self: *Overlay, a: A, old: []const v.Entry, updates: []const v.Entry, replace: bool) ![]const v.Entry {
        var output: std.ArrayList(v.Entry) = .empty;
        var names: std.StringHashMapUnmanaged(void) = .empty;
        defer names.deinit(a);
        if (updates.len > 8) for (updates) |entry| {
            try self.charge(1 + entry.name.len / 64);
            try names.put(a, entry.name, {});
        };
        if (!replace) for (old) |entry| {
            try self.charge(1 + entry.name.len / 64);
            const changed = if (updates.len > 8) names.contains(entry.name) else scan: {
                for (updates) |update| {
                    try self.charge(1 + update.name.len / 64);
                    if (std.mem.eql(u8, update.name, entry.name)) break :scan true;
                }
                break :scan false;
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
        self.staged_order_valid = false;
        if (!self.active) return error.CypherTransactionRequired;
        const copy = if (postimage) |node| blk: {
            if (!node.identity.eql(identity)) return error.CypherInvalidIdentity;
            break :blk (try (v.Value{ .node = node }).clone(a)).node;
        } else blk: {
            const tombstone = try a.create(v.Node);
            tombstone.* = .{ .identity = identity };
            break :blk tombstone;
        };
        var membership = self.valueWork(a);
        var labels: std.ArrayList([]const u8) = .empty;
        if (postimage != null) for (self.graph.?.labels) |label| {
            if (try self.graph.?.admits(copy, label.name, &membership)) try labels.append(a, label.name);
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
            try self.retireOwnedRelationships(a, identity, false);
        }
    }

    fn set(self: *Overlay, a: A, target: v.EntityIdentity, updates: []const v.Entry, replace: bool) !void {
        switch (target) {
            .node => |identity| {
                const node = (try self.ensureNode(a, identity)) orelse return error.CypherDeletedEntity;
                if (node.deleted) return error.CypherDeletedEntity;
                node.value.properties = try self.properties(a, node.value.properties, updates, replace);
                node.dirty = true;
                node.projection_cached = false;
                if (self.base.vtable.normalize_node) |normalize| node.value.* = (try normalize(self.base.ptr, a, node.value, false)).*;
                try self.relabel(a, node.value);
                try self.refreshDocumentRelationships(a, node.value);
            },
            .relationship => |identity| {
                const relationship = self.findRelationship(identity) orelse return error.CypherDeletedEntity;
                if (relationship.deleted) return error.CypherDeletedEntity;
                if (relationship.value.ownership != .explicit) return error.CypherDerivedRelationshipMutation;
                if (self.lazy) {
                    _ = try self.ensureNode(a, relationship.value.source);
                    _ = try self.ensureNode(a, relationship.value.target);
                }
                relationship.value.properties = try self.properties(a, relationship.value.properties, updates, replace);
                relationship.dirty = true;
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
        const node = (try self.ensureNode(a, identity)) orelse return false;
        if (node.deleted) return false;
        return (try projectNode(self, a, node.value)) != null;
    }
    pub fn retireOwnedRelationships(self: *Overlay, a: A, identity: v.Identity, fields_only: bool) !void {
        if (self.references.get(identity)) |references| for (references.items) |reference| {
            try self.charge(1);
            const relationship = self.currentReference(reference) orelse continue;
            if ((!fields_only or relationship.value.ownership == .field) and try ownedBy(a, relationship.value, identity)) relationship.deleted = true;
        };
    }
    fn refreshDocumentRelationships(self: *Overlay, a: A, node: *const v.Node) !void {
        const project = self.base.vtable.document_relationships orelse return;
        const relationships = try project(self.base.ptr, a, node);
        try self.retireOwnedRelationships(a, node.identity, true);
        if (self.lazy) try self.replaced_producers.put(a, node.identity, {});
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
        self.staged_order_valid = false;
        switch (mutation) {
            .create_node => |node| {
                if ((try self.ensureNode(a, node.identity)) != null) return error.CypherDuplicateIdentity;
                try self.capacity();
                const copy = try a.create(v.Node);
                copy.* = node.*;
                if (self.base.vtable.normalize_node) |normalize| copy.* = (try normalize(self.base.ptr, a, copy, true)).*;
                try self.relabel(a, copy);
                // Normalization may refresh labels. Validate the requested labels
                // against defaults and generated values before staging the node.
                var membership = self.valueWork(a);
                for (node.labels) |name| if (!try self.graph.?.admits(copy, name, &membership)) return error.CypherLabelPredicateViolation;
                try self.appendNode(a, .{ .value = copy, .dirty = true, .created = true });
                try self.refreshDocumentRelationships(a, copy);
            },
            .create_relationship => |relationship| {
                for ([_]v.Identity{ relationship.source, relationship.target }) |identity| {
                    const endpoint = (try self.ensureNode(a, identity)) orelse return error.CypherDeletedEntity;
                    if (endpoint.deleted) return error.CypherDeletedEntity;
                }
                if (self.findRelationship(.{ .identity = relationship.identity, .index = relationship.index }) != null) return error.CypherDuplicateIdentity;
                try self.capacity();
                const copy = try a.create(v.Relationship);
                copy.* = relationship.*;
                copy.properties = try v.entityProperties(a, relationship.properties);
                try self.appendRelationship(a, .{ .value = copy, .dirty = true, .created = true });
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
                relationship.dirty = true;
            },
            .delete_node => |deletion| {
                _ = (try self.ensureNode(a, deletion.identity)) orelse return error.CypherDeletedEntity;
                try self.loadIncidents(a, deletion.identity);
                if (self.references.get(deletion.identity)) |references| for (references.items) |reference| {
                    try self.charge(1);
                    const relationship = self.currentReference(reference) orelse continue;
                    if (relationship.deleted) continue;
                    if (try ownedBy(a, relationship.value, deletion.identity) and !(relationship.value.source.eql(deletion.identity) or relationship.value.target.eql(deletion.identity))) {
                        relationship.deleted = true;
                        relationship.dirty = true;
                        continue;
                    }
                    if (!(relationship.value.source.eql(deletion.identity) or relationship.value.target.eql(deletion.identity))) continue;
                    if (!deletion.detach) return error.CypherNodeHasRelationships;
                    relationship.deleted = true;
                    relationship.dirty = true;
                };
                const current = self.findNode(deletion.identity).?;
                current.deleted = true;
                current.dirty = true;
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
        self.replaced_producers = .empty;
        self.nodes = .empty;
        self.node_positions = .empty;
        self.node_order = null;
        self.staged_order = .empty;
        self.staged_order_valid = false;
        self.references = .empty;
        self.relationships = .empty;
        self.relationship_positions = .empty;
        self.mutations = .empty;
    }
};
