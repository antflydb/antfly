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
    staged_indexes: std.ArrayList(StagedIndex) = .empty,
    references: std.HashMapUnmanaged(v.Identity, std.ArrayList(Reference), v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty,
    staged_read_documents: bool = false,
    nodes: std.ArrayList(Node) = .empty,
    node_positions: std.HashMapUnmanaged(v.Identity, usize, v.Identity.Context, std.hash_map.default_max_load_percentage) = .empty,
    relationships: std.ArrayList(Relationship) = .empty,
    relationship_positions: std.HashMapUnmanaged(v.RelationshipIdentity, usize, v.RelationshipIdentity.Context, std.hash_map.default_max_load_percentage) = .empty,
    mutations: std.ArrayList(b.Mutation) = .empty,
    pairs: std.HashMapUnmanaged(Pair, PairBucket, Pair.Context, std.hash_map.default_max_load_percentage) = .empty,
    pair_keys: std.AutoHashMapUnmanaged(usize, Pair) = .empty,
    pair_indexed: bool = false,
    captured_adjacency: std.HashMapUnmanaged(Adjacency, void, Adjacency.Context, std.hash_map.default_max_load_percentage) = .empty,
    max_entities: usize = 65536,
    max_anchors: usize = 1_000_000,
    max_edges: usize = 1_000_000,
    captured: b.StartupWork = .{},
    const Pair = struct {
        source: v.Identity,
        target: v.Identity,
        index: []const u8,
        type: []const u8,
        const Context = struct {
            pub fn hash(_: @This(), pair: Pair) u64 {
                return std.hash.Wyhash.hash(std.hash.Wyhash.hash((v.Identity.Context{}).hash(pair.source), pair.index) ^ (v.Identity.Context{}).hash(pair.target), pair.type);
            }
            pub fn eql(_: @This(), left: Pair, right: Pair) bool {
                return left.source.eql(right.source) and left.target.eql(right.target) and std.mem.eql(u8, left.index, right.index) and std.mem.eql(u8, left.type, right.type);
            }
        };
    };
    const PairBucket = struct { positions: Positions = .empty, staged: Positions = .empty, properties: std.StringHashMapUnmanaged(StagedIndex) = .empty };
    const Adjacency = struct {
        peer: ?v.Identity = null,
        identity: v.Identity,
        index: []const u8,
        type: []const u8,
        direction: ast.Direction,
        const Context = struct {
            pub fn hash(_: @This(), key: Adjacency) u64 {
                return std.hash.Wyhash.hash(std.hash.Wyhash.hash((v.Identity.Context{}).hash(key.identity), key.index), key.type) ^ @backingInt(key.direction) ^ (if (key.peer) |peer| (v.Identity.Context{}).hash(peer) else 0);
            }
            pub fn eql(_: @This(), left: Adjacency, right: Adjacency) bool {
                return (if (left.peer) |peer| right.peer != null and peer.eql(right.peer.?) else right.peer == null) and left.identity.eql(right.identity) and left.direction == right.direction and std.mem.eql(u8, left.index, right.index) and std.mem.eql(u8, left.type, right.type);
            }
        };
    };
    fn pairProperty(self: *Overlay, index: *StagedIndex, position: usize, remove: bool) !void {
        const a = self.alloc.?;
        try self.charge(1);
        if (index.hashes.fetchRemove(position)) |old| _ = index.buckets.getPtr(old.value).?.swapRemove(position);
        if (remove) return;
        const hash = (try self.scalarHash((v.Value{ .relationship = self.relationships.items[position].value }).property(index.property))) orelse return;
        const bucket = try index.buckets.getOrPut(a, hash);
        if (!bucket.found_existing) bucket.value_ptr.* = .empty;
        try bucket.value_ptr.put(a, position, {});
        try index.hashes.put(a, position, hash);
    }
    fn indexPair(self: *Overlay, position: usize) !void {
        if (!self.pair_indexed) return;
        try self.charge(1);
        const a = self.alloc.?;
        if (self.pair_keys.fetchRemove(position)) |old| {
            const bucket = self.pairs.getPtr(old.value).?;
            _ = bucket.positions.swapRemove(position);
            _ = bucket.staged.swapRemove(position);
            var indexes = bucket.properties.valueIterator();
            while (indexes.next()) |index| try self.pairProperty(index, position, true);
        }
        const relationship = self.relationships.items[position];
        if (relationship.deleted) return;
        const value = relationship.value;
        const pair: Pair = .{ .source = value.source, .target = value.target, .index = value.index, .type = value.type };
        const entry = try self.pairs.getOrPut(a, pair);
        if (!entry.found_existing) entry.value_ptr.* = .{};
        try entry.value_ptr.positions.put(a, position, {});
        if (self.lazy and relationship.isStaged()) try entry.value_ptr.staged.put(a, position, {});
        try self.pair_keys.put(a, position, pair);
        var indexes = entry.value_ptr.properties.valueIterator();
        while (indexes.next()) |index| try self.pairProperty(index, position, false);
    }
    fn pairCandidates(self: *Overlay, _: A, pair: Pair, filters: []const b.PropertyFilter, staged_only: bool) ![]const usize {
        const bucket = self.pairs.getPtr(pair) orelse return &.{};
        var best: []const usize = if (staged_only) bucket.staged.keys() else bucket.positions.keys();
        for (filters) |filter| {
            if (filter.op != .eq) continue;
            const hash = (try self.scalarHash(filter.value)) orelse continue;
            const slot = try bucket.properties.getOrPut(self.alloc.?, filter.property);
            if (!slot.found_existing) {
                slot.key_ptr.* = try self.alloc.?.dupe(u8, filter.property);
                slot.value_ptr.* = .{ .table = "", .property = slot.key_ptr.* };
                for (bucket.positions.keys()) |position| try self.pairProperty(slot.value_ptr, position, false);
            }
            const candidates = if (slot.value_ptr.buckets.get(hash)) |positions| positions.keys() else &.{};
            if (candidates.len < best.len) best = candidates;
        }
        return best;
    }
    fn expandBetween(ptr: *anyopaque, a: A, mapping: c.Relationship, identity: v.Identity, peer: v.Identity, direction: ast.Direction, filters: []const b.PropertyFilter, after: ?[]const u8, limit: usize, max_examined: usize) !b.Page(*const v.Relationship) {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (limit == 0) return error.CypherInvalidBackendPage;
        if (!self.active) {
            if (self.base.vtable.expand_between) |between| return between(self.base.ptr, a, mapping, identity, peer, direction, filters, after, limit, max_examined);
            return self.base.vtable.expand(self.base.ptr, a, mapping, identity, direction, after, limit);
        }
        if (!self.pair_indexed) {
            self.pair_indexed = true;
            for (0..self.relationships.items.len) |position| try self.indexPair(position);
        }
        var examined: usize = 0;
        const adjacency: Adjacency = .{ .peer = if (self.base.vtable.expand_between != null) peer else null, .identity = identity, .index = mapping.index, .type = mapping.name, .direction = direction };
        // A retained pair scan is a pull cursor, not an adjacency barrier.
        // Completed captures can reuse the postings; partial captures retain
        // their opaque native continuation until their staged phase ends.
        if (self.lazy and self.base.vtable.expand_between != null) {
            if ((after != null and std.mem.startsWith(u8, after.?, "{")) or
                !self.captured_adjacency.contains(adjacency))
                return self.lazyPairExpand(a, mapping, identity, peer, direction, filters, after, limit, max_examined, adjacency);
        }
        if (self.lazy and !self.captured_adjacency.contains(adjacency)) {
            // Native stores without a pair seek are indexed once per retained
            // adjacency domain. Staged edges join the same postings incrementally.
            var cursor: ?[]const u8 = null;
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            var cursor_bytes: std.ArrayList(u8) = .empty;
            const cursor_allocator = b.reclaimingAllocator(a);
            defer cursor_bytes.deinit(cursor_allocator);
            while (true) {
                _ = scratch.reset(.retain_capacity);
                const page = try self.lazyExpand(scratch.allocator(), mapping, identity, direction, cursor, @min(128, @max(1, max_examined -| examined)));
                const count = page.examined orelse page.items.len;
                if (count > @min(self.max_edges, max_examined) -| examined) return error.CypherEdgeBudgetExceeded;
                examined += count;
                // lazyExpand has retained native identities and pair postings.
                const next = page.next orelse break;
                const continuation = try std.json.parseFromSliceLeaky(LazyCursor, scratch.allocator(), next, .{});
                if (continuation.staged) break;
                if (examined == max_examined) return error.CypherEdgeBudgetExceeded;
                cursor_bytes.clearRetainingCapacity();
                try cursor_bytes.appendSlice(cursor_allocator, next);
                cursor = cursor_bytes.items;
            }
            const retained = self.alloc.?;
            try self.captured_adjacency.put(retained, .{ .peer = if (adjacency.peer) |value| .{ .table = try retained.dupe(u8, value.table), .key = try retained.dupe(u8, value.key) } else null, .identity = .{ .table = try retained.dupe(u8, identity.table), .key = try retained.dupe(u8, identity.key) }, .index = try retained.dupe(u8, mapping.index), .type = try retained.dupe(u8, mapping.name), .direction = direction }, {});
        }
        const offset = if (after) |encoded| try std.fmt.parseUnsigned(usize, encoded, 10) else 0;
        return self.pairPage(a, mapping, identity, peer, direction, filters, offset, limit, max_examined, false, examined);
    }
    fn pairPage(self: *Overlay, a: A, mapping: c.Relationship, identity: v.Identity, peer: v.Identity, direction: ast.Direction, filters: []const b.PropertyFilter, offset: usize, limit: usize, max_examined: usize, staged_only: bool, captured_examined: usize) !b.Page(*const v.Relationship) {
        const outgoing = if (direction != .in) try self.pairCandidates(a, .{ .source = identity, .target = peer, .index = mapping.index, .type = mapping.name }, filters, staged_only) else &.{};
        const incoming = if (direction != .out and (!identity.eql(peer) or direction == .in)) try self.pairCandidates(a, .{ .source = peer, .target = identity, .index = mapping.index, .type = mapping.name }, filters, staged_only) else &.{};
        const count = outgoing.len + incoming.len;
        if (offset > count) return error.CypherInvalidBackendCursor;
        if (offset < count and max_examined <= captured_examined) return error.CypherEdgeBudgetExceeded;
        const end = offset + @min(@min(limit, max_examined -| captured_examined), count - offset);
        var output: std.ArrayList(*const v.Relationship) = .empty;
        for (offset..end) |candidate| {
            const position = if (candidate < outgoing.len) outgoing[candidate] else incoming[candidate - outgoing.len];
            try self.charge(1);
            const relationship = self.relationships.items[position];
            if (staged_only and !relationship.isStaged()) continue;
            if (!relationship.deleted and try self.producerVisible(a, relationship.value)) try output.append(a, relationship.value);
        }
        return .{ .items = try output.toOwnedSlice(a), .examined = captured_examined + end - offset, .next = if (end < count)
            if (staged_only) try std.json.Stringify.valueAlloc(a, LazyCursor{ .staged = true, .offset = end }, .{}) else try std.fmt.allocPrint(a, "{d}", .{end})
        else
            null };
    }
    fn lazyPairExpand(self: *Overlay, a: A, mapping: c.Relationship, identity: v.Identity, peer: v.Identity, direction: ast.Direction, filters: []const b.PropertyFilter, after: ?[]const u8, limit: usize, max_examined: usize, adjacency: Adjacency) !b.Page(*const v.Relationship) {
        const cursor = if (after) |encoded| try std.json.parseFromSliceLeaky(LazyCursor, a, encoded, .{}) else LazyCursor{};
        if (cursor.staged) return self.pairPage(a, mapping, identity, peer, direction, filters, cursor.offset, limit, max_examined, true, 0);
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        // Capture an unfiltered prefix so a completed capture is reusable for
        // every property residual, including subsequent staged replacements.
        const page = try self.base.vtable.expand_between.?(self.base.ptr, temporary, mapping, identity, peer, direction, &.{}, cursor.native, limit, max_examined);
        const examined = page.examined orelse page.items.len;
        if (examined > max_examined) return error.CypherEdgeBudgetExceeded;
        var output: std.ArrayList(*const v.Relationship) = .empty;
        for (page.items) |value| {
            if (value.ownership == .field) {
                if (try value.producingDocument(temporary)) |producer| if (self.replaced_producers.contains(producer)) continue;
            }
            const item = if (self.findRelationship(.{ .identity = value.identity, .index = value.index })) |cached| cached else blk: {
                try self.capacity();
                const retained = self.alloc.?;
                const copy = @constCast((try (v.Value{ .relationship = value }).clone(retained)).relationship);
                try self.appendRelationship(retained, .{ .value = copy, .from_base = true });
                break :blk self.findRelationship(.{ .identity = value.identity, .index = value.index }).?;
            };
            if (!item.isStaged() and !item.deleted and try self.producerVisible(a, item.value)) try output.append(a, item.value);
        }
        if (page.next == null) {
            const retained = self.alloc.?;
            try self.captured_adjacency.put(retained, .{ .peer = .{ .table = try retained.dupe(u8, peer.table), .key = try retained.dupe(u8, peer.key) }, .identity = .{ .table = try retained.dupe(u8, identity.table), .key = try retained.dupe(u8, identity.key) }, .index = try retained.dupe(u8, adjacency.index), .type = try retained.dupe(u8, adjacency.type), .direction = direction }, {});
        }
        return .{ .items = try output.toOwnedSlice(a), .examined = examined, .next = try std.json.Stringify.valueAlloc(a, LazyCursor{ .staged = page.next == null, .native = page.next }, .{}) };
    }
    const Positions = std.AutoArrayHashMapUnmanaged(usize, void);
    const StagedIndex = struct {
        table: []const u8,
        property: []const u8,
        buckets: std.AutoHashMapUnmanaged(u64, Positions) = .empty,
        hashes: std.AutoHashMapUnmanaged(usize, u64) = .empty,
    };
    fn scalarHash(self: *Overlay, value: v.Value) !?u64 {
        switch (value) {
            .boolean, .integer, .number, .string => {},
            else => return null,
        }
        var work = self.valueWork(self.alloc.?);
        return (try work.info(value)).hash;
    }
    fn indexStagedNode(self: *Overlay, index: *StagedIndex, position: usize) !void {
        const a = self.alloc.?;
        try self.charge(1);
        const node = self.nodes.items[position];
        if (index.hashes.fetchRemove(position)) |old| {
            _ = index.buckets.getPtr(old.value).?.swapRemove(position);
        }
        if (!node.dirty or node.deleted or node.value.labels.len == 0 or !std.mem.eql(u8, index.table, node.value.identity.table)) return;
        // Candidate values use the same authorized postimage as residuals;
        // masks and USING policies must not create false negatives.
        const projected = (try projectNode(self, a, node.value)) orelse return;
        const hash = (try self.scalarHash((v.Value{ .node = projected }).property(index.property))) orelse return;
        const bucket = try index.buckets.getOrPut(a, hash);
        if (!bucket.found_existing) bucket.value_ptr.* = .empty;
        try bucket.value_ptr.put(a, position, {});
        try index.hashes.put(a, position, hash);
    }
    fn refreshStagedIndexes(self: *Overlay, position: usize) !void {
        for (self.staged_indexes.items) |*index| try self.indexStagedNode(index, position);
    }
    fn stagedCandidates(self: *Overlay, a: A, table: []const u8, filters: []const b.PropertyFilter) !?[]const usize {
        var best_filter: ?b.PropertyFilter = null;
        var best_index: usize = 0;
        var smallest: usize = std.math.maxInt(usize);
        for (filters) |filter| {
            if (filter.op != .eq and filter.op != .in) continue;
            const values = if (filter.op == .in) if (filter.value == .list) filter.value.list else continue else &.{filter.value};
            var usable = true;
            for (values) |value| switch (value) {
                .null, .boolean, .integer, .number, .string => {},
                else => {
                    usable = false;
                    break;
                },
            };
            if (!usable) continue;
            const retained = self.alloc.?;
            var selected: ?usize = null;
            for (self.staged_indexes.items, 0..) |index, position| if (std.mem.eql(u8, table, index.table) and std.mem.eql(u8, filter.property, index.property)) {
                selected = position;
                break;
            };
            if (selected == null) {
                selected = self.staged_indexes.items.len;
                try self.staged_indexes.append(retained, .{ .table = try retained.dupe(u8, table), .property = try retained.dupe(u8, filter.property) });
                const index = &self.staged_indexes.items[selected.?];
                for (0..self.nodes.items.len) |position| try self.indexStagedNode(index, position);
            }
            const index = &self.staged_indexes.items[selected.?];
            var estimate: usize = 0;
            for (values) |value| {
                const hash = (try self.scalarHash(value)) orelse continue;
                if (index.buckets.get(hash)) |bucket| estimate +|= bucket.count();
            }
            if (best_filter == null or estimate < smallest) {
                best_filter = filter;
                best_index = selected.?;
                smallest = estimate;
            }
        }
        const filter = best_filter orelse return null;
        const values = if (filter.op == .in) filter.value.list else &.{filter.value};
        const index = &self.staged_indexes.items[best_index];
        var candidates: Positions = .empty;
        for (values) |value| {
            const hash = (try self.scalarHash(value)) orelse continue;
            if (index.buckets.get(hash)) |bucket| {
                for (bucket.keys()) |position| {
                    try self.charge(1);
                    try candidates.put(a, position, {});
                }
            }
        }
        const output = try a.alloc(usize, candidates.count());
        @memcpy(output, candidates.keys());
        try self.charge(output.len * (1 + std.math.log2_int(usize, @max(1, output.len))));
        std.mem.sort(usize, output, self, stagedLess);
        return output;
    }
    fn stagedLess(self: *Overlay, left: usize, right: usize) bool {
        const x = self.nodes.items[left].value.identity;
        const y = self.nodes.items[right].value.identity;
        const tables = std.mem.order(u8, x.table, y.table);
        return tables == .lt or (tables == .eq and std.mem.lessThan(u8, x.key, y.key));
    }
    const Reference = struct { position: usize, version: usize };
    const Node = struct { value: *v.Node, deleted: bool = false, created: bool = false, dirty: bool = false, projection_cached: bool = false, projected: ?*const v.Node = null };
    const Relationship = struct {
        value: *v.Relationship,
        deleted: bool = false,
        created: bool = false,
        dirty: bool = false,
        version: usize = 0,
        from_base: bool = false,
        fn isStaged(self: @This()) bool {
            // Document projections are staged effects too, even though they
            // are persisted by their producing row rather than an edge write.
            return !self.from_base or self.dirty or self.created;
        }
    };
    const plain_vtable: b.Backend.VTable = .{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .scan_where = scanWhere, .scan_ordered = scanOrdered, .estimate_where = estimateWhere, .probe_where = probeWhere, .population_upper_bound = populationUpperBound, .get = get, .get_many = getMany, .get_relationship = getRelationship, .expand = expand, .expand_between = expandBetween, .search = search, .set_work = setWork, .set_read_fields = setReadFields, .set_read_demand = setReadDemand, .begin = begin, .startup_work = startupWork, .mutate = mutate, .commit = commit, .rollback = rollback, .new_identity = newIdentity };
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
    fn setReadDemand(ptr: *anyopaque, demand: ?@import("read_fields.zig").Demand) void {
        const self = cast(ptr);
        if (self.base.vtable.set_read_demand) |configure| configure(self.base.ptr, if (self.active and (self.nodes.items.len != 0 or self.retain_scan_preimages)) null else demand);
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
        if (node.dirty) try self.refreshStagedIndexes(self.nodes.items.len - 1);
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
        try self.indexPair(self.relationships.items.len - 1);
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
        try self.indexPair(position);
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
        return self.lazyNodes(a, table, &.{}, after, limit);
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
                if (!item.isStaged() and !item.deleted and try self.producerVisible(a, item.value)) try found.append(a, item.value);
            }
            return .{ .items = try found.toOwnedSlice(a), .next = try std.json.Stringify.valueAlloc(a, LazyCursor{ .staged = page.next == null, .native = page.next }, .{}), .examined = page.examined orelse page.items.len };
        }
        const references = if (self.references.get(identity)) |list| list.items else &.{};
        if (cursor.offset > references.len) return error.CypherInvalidBackendCursor;
        const end = cursor.offset + @min(limit, references.len - cursor.offset);
        for (references[cursor.offset..end]) |reference| {
            try self.charge(1);
            const item = self.currentReference(reference) orelse continue;
            if (!item.isStaged() or item.deleted or !std.mem.eql(u8, item.value.type, mapping.name) or !std.mem.eql(u8, item.value.index, mapping.index)) continue;
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
    fn populationUpperBound(ptr: *anyopaque, a: A, table: []const u8) anyerror!?u64 {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (self.active) return null;
        return if (self.base.vtable.population_upper_bound) |read| try read(self.base.ptr, a, table) else null;
    }
    fn probeWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) anyerror!?b.Probe {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (self.active) return null;
        return if (self.base.vtable.probe_where) |read| try read(self.base.ptr, a, table, filters, limit) else null;
    }
    fn estimateWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) anyerror!?usize {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (self.active) return null;
        return if (self.base.vtable.estimate_where) |estimate| try estimate(self.base.ptr, a, table, filters, limit) else null;
    }
    fn scanOrdered(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, max_examined: usize) anyerror!?b.Page(*const v.Node) {
        const self = cast(ptr);
        try checkpoint(ptr);
        // Dirty postimages can move in sort order. Only an immutable base can
        // certify ordering; active overlays keep their ordinary merged scan.
        if (self.active) return null;
        const read = self.base.vtable.scan_ordered orelse return null;
        return read(self.base.ptr, a, table, filters, order, after, limit, max_examined);
    }
    fn scanWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, after: ?[]const u8, limit: usize) anyerror!b.Page(*const v.Node) {
        const self = cast(ptr);
        try checkpoint(ptr);
        if (!self.active) {
            const filtered = self.base.vtable.scan_where orelse return scan(ptr, a, table, after, limit);
            return filtered(self.base.ptr, a, table, filters, after, limit);
        }
        if (!self.lazy or filters.len == 0) return scan(ptr, a, table, after, limit);
        return self.lazyNodes(a, table, filters, after, limit);
    }
    // Native continuation tokens are opaque. Dirty postimages are emitted in
    // a separate phase, so they cannot manufacture a key-based native token.
    fn lazyNodes(self: *Overlay, a: A, table: []const u8, filters: []const b.PropertyFilter, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        if (limit == 0) return error.CypherInvalidBackendPage;
        const cursor = if (after) |encoded| try std.json.parseFromSliceLeaky(LazyCursor, a, encoded, .{}) else LazyCursor{};
        var found: std.ArrayList(*const v.Node) = .empty;
        if (!cursor.staged) {
            const page = if (filters.len != 0 and self.base.vtable.scan_where != null)
                try self.base.vtable.scan_where.?(self.base.ptr, a, table, filters, cursor.native, limit)
            else
                try self.base.vtable.scan(self.base.ptr, a, table, cursor.native, limit);
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
        const candidates = try self.stagedCandidates(a, table, filters);
        if (candidates == null) try self.prepareStagedOrder();
        const order = candidates orelse self.staged_order.items;
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
    fn getMany(ptr: *anyopaque, a: A, identities: []const v.Identity) ![]const ?*const v.Node {
        const self = cast(ptr);
        try checkpoint(ptr);
        const output = try a.alloc(?*const v.Node, identities.len);
        var pending: std.ArrayList(v.Identity) = .empty;
        var offsets: std.ArrayList(usize) = .empty;
        for (identities, output, 0..) |identity, *item, offset| {
            item.* = null;
            if (self.active) {
                if (self.findNode(identity)) |node| {
                    item.* = if (node.deleted) null else node.value;
                    continue;
                }
                if (!self.lazy) continue;
            }
            try pending.append(a, identity);
            try offsets.append(a, offset);
        }
        if (pending.items.len != 0) {
            if (self.base.vtable.get_many) |batch| {
                const nodes = try batch(self.base.ptr, a, pending.items);
                if (nodes.len != pending.items.len) return error.CypherInvalidBackendPage;
                for (nodes, pending.items, offsets.items) |node, identity, offset| {
                    if (node) |value| if (!value.identity.eql(identity)) return error.CypherInvalidBackendPage;
                    output[offset] = node;
                }
            } else for (pending.items, offsets.items) |identity, offset| output[offset] = try self.base.vtable.get(self.base.ptr, a, identity);
        }
        return output;
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
        try self.refreshStagedIndexes(self.node_positions.get(identity).?);
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
                try self.refreshStagedIndexes(self.node_positions.get(identity).?);
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
                try self.indexPair(self.relationship_positions.get(.{ .identity = relationship.value.identity, .index = relationship.value.index }).?);
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
            if ((!fields_only or relationship.value.ownership == .field) and try ownedBy(a, relationship.value, identity)) {
                relationship.deleted = true;
                try self.indexPair(reference.position);
            }
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
                try self.indexPair(self.relationship_positions.get(identity).?);
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
                        try self.indexPair(reference.position);
                        continue;
                    }
                    if (!(relationship.value.source.eql(deletion.identity) or relationship.value.target.eql(deletion.identity))) continue;
                    if (!deletion.detach) return error.CypherNodeHasRelationships;
                    relationship.deleted = true;
                    relationship.dirty = true;
                    try self.indexPair(reference.position);
                };
                const current = self.findNode(deletion.identity).?;
                current.deleted = true;
                current.dirty = true;
                try self.refreshStagedIndexes(self.node_positions.get(deletion.identity).?);
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
        self.staged_indexes = .empty;
        self.staged_order_valid = false;
        self.references = .empty;
        self.relationships = .empty;
        self.relationship_positions = .empty;
        self.mutations = .empty;
        self.pairs = .empty;
        self.pair_keys = .empty;
        self.pair_indexed = false;
        self.captured_adjacency = .empty;
    }
};
