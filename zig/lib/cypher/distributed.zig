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

//! Statement coordinator over authenticated owner capabilities. All owners
//! finish read-index admission before any mutation fence is held. Snapshots
//! are forked with the complete cohort fenced, then every fence is released.
const std = @import("std");
const b = @import("backend.zig");
const v = @import("value.zig");
const catalog = @import("catalog.zig");
const ast = @import("ast.zig");
const A = std.mem.Allocator;
pub const KeyRange = struct { start: []const u8 = "", end: ?[]const u8 = null };
pub const Target = struct {
    table: []const u8,
    table_id: u64,
    group_id: u64,
    /// Empty only for legacy/custom sources with no routing projection.
    ranges: []const KeyRange = &.{},
    fn owns(self: Target, identity: v.Identity) bool {
        if (!std.mem.eql(u8, self.table, identity.table)) return false;
        if (self.ranges.len == 0) return true;
        for (self.ranges) |range| {
            if (std.mem.order(u8, identity.key, range.start) == .lt) continue;
            if (range.end) |end| if (end.len != 0 and std.mem.order(u8, identity.key, end) != .lt) continue;
            return true;
        }
        return false;
    }
};
pub const Snapshot = struct {
    /// Set only when source.checkpoint already performs exactly this snapshot's
    /// validation. Independent authority checks remain mandatory by default.
    validation_covered_by_source: bool = false,
    backend: b.Backend,
    ptr: *anyopaque,
    validate: *const fn (*anyopaque) anyerror!void,
    close: *const fn (*anyopaque) void,
};
pub const Fence = struct { ptr: *anyopaque, validate: *const fn (*anyopaque) anyerror!void, fork: *const fn (*anyopaque, A) anyerror!Snapshot, release: *const fn (*anyopaque) void };
pub const Source = struct {
    ptr: *anyopaque,
    checkpoint: *const fn (*anyopaque) anyerror!void,
    admit: *const fn (*anyopaque, Target) anyerror!void,
    capture: *const fn (*anyopaque, A, Target) anyerror!?Fence,
};
pub const ReadSet = struct {
    /// Local disjoint index partitions share this merge. Their tie order is
    /// primary identity order, and nested coordinators need per-item cursors.
    ordered_identity_ties: bool = false,
    ordered_item_cursors: bool = false,
    work: ?b.Work = null,
    a: A,
    targets: []const Target,
    snapshots: []const Snapshot,
    independent_validations: []const Snapshot,
    source: Source,
    graph: ?catalog.Graph = null,
    pub fn acquire(a: A, source: Source, targets: []const Target) !ReadSet {
        if (targets.len == 0 or targets.len > 1024) return error.CypherOwnerBudgetExceeded;
        for (targets, 0..) |target, i| {
            if (target.table_id == 0 or target.group_id == 0 or target.table.len == 0) return error.CypherInvalidOwner;
            for (targets[0..i]) |other| {
                if (target.group_id == other.group_id and target.table_id == other.table_id) return error.CypherInvalidOwner;
                if (std.mem.eql(u8, target.table, other.table) and target.table_id != other.table_id) return error.CatalogGenerationChanged;
            }
        }
        for (targets) |target| {
            try source.checkpoint(source.ptr);
            try source.admit(source.ptr, target);
        }
        const fences = try a.alloc(Fence, targets.len);
        defer a.free(fences);
        const snapshots = try a.alloc(Snapshot, targets.len);
        errdefer a.free(snapshots);
        var captured: usize = 0;
        defer for (fences[0..captured]) |fence| fence.release(fence.ptr);
        var forked: usize = 0;
        errdefer for (snapshots[0..forked]) |snapshot| snapshot.close(snapshot.ptr);
        // A busy participant requires releasing the entire earlier cohort.
        // Never wait for transaction resolution while blocking another owner.
        for (0..4) |_| {
            for (targets) |target| {
                try source.checkpoint(source.ptr);
                const fence = (try source.capture(source.ptr, a, target)) orelse break;
                fences[captured] = fence;
                captured += 1;
            }
            if (captured == targets.len) break;
            for (fences[0..captured]) |fence| fence.release(fence.ptr);
            captured = 0;
        }
        if (captured != targets.len) return error.Backpressured;
        for (fences, snapshots) |fence, *snapshot| {
            try source.checkpoint(source.ptr);
            snapshot.* = try fence.fork(fence.ptr, a);
            forked += 1;
        }
        for (fences) |fence| try fence.validate(fence.ptr);
        const owned = try a.alloc(Target, targets.len);
        errdefer a.free(owned);
        var copied: usize = 0;
        errdefer for (owned[0..copied]) |target| freeTarget(a, target);
        for (targets, owned) |target, *out| {
            out.* = target;
            out.table = try a.dupe(u8, target.table);
            errdefer a.free(out.table);
            const ranges = try a.alloc(KeyRange, target.ranges.len);
            errdefer a.free(ranges);
            var initialized: usize = 0;
            errdefer for (ranges[0..initialized]) |range| {
                a.free(range.start);
                if (range.end) |end| a.free(end);
            };
            for (target.ranges, ranges) |range, *copy| {
                const start = try a.dupe(u8, range.start);
                errdefer a.free(start);
                copy.* = .{ .start = start, .end = if (range.end) |end| try a.dupe(u8, end) else null };
                initialized += 1;
            }
            out.ranges = ranges;
            copied += 1;
        }
        var validations: std.ArrayList(Snapshot) = .empty;
        errdefer validations.deinit(a);
        for (snapshots) |snapshot| if (!snapshot.validation_covered_by_source) try validations.append(a, snapshot);
        return .{ .a = a, .source = source, .targets = owned, .snapshots = snapshots, .independent_validations = try validations.toOwnedSlice(a) };
    }
    fn freeTarget(a: A, target: Target) void {
        a.free(target.table);
        for (target.ranges) |range| {
            a.free(range.start);
            if (range.end) |end| a.free(end);
        }
        a.free(target.ranges);
    }
    pub fn deinit(self: *ReadSet) void {
        for (self.snapshots) |snapshot| snapshot.close(snapshot.ptr);
        for (self.targets) |target| freeTarget(self.a, target);
        self.a.free(self.targets);
        self.a.free(self.snapshots);
        self.a.free(self.independent_validations);
        self.* = undefined;
    }
    pub fn backend(self: *ReadSet) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .scan_where = scanWhere, .scan_ordered = scanOrdered, .estimate_where = estimateWhere, .get = get, .get_many = getMany, .expand = expand, .expand_between = expandBetween, .search = search, .set_work = setWork, .set_read_fields = setReadFields } };
    }
    fn setReadFields(ptr: *anyopaque, fields: ?[]const []const u8) void {
        for (cast(ptr).snapshots) |snapshot| if (snapshot.backend.vtable.set_read_fields) |configure| configure(snapshot.backend.ptr, fields);
    }
    fn setWork(ptr: *anyopaque, work: ?b.Work) void {
        cast(ptr).work = work;
        for (cast(ptr).snapshots) |snapshot| if (snapshot.backend.vtable.set_work) |configure| configure(snapshot.backend.ptr, work);
    }
    fn cast(ptr: *anyopaque) *ReadSet {
        return @ptrCast(@alignCast(ptr));
    }
    fn checkpoint(ptr: *anyopaque) !void {
        const self = cast(ptr);
        try self.source.checkpoint(self.source.ptr);
        for (self.independent_validations) |snapshot| try snapshot.validate(snapshot.ptr);
    }
    fn bind(ptr: *anyopaque, a: A, graph: catalog.Graph) !void {
        const self = cast(ptr);
        try checkpoint(ptr);
        for (graph.labels) |label| {
            var found = false;
            for (self.targets) |target| if (std.mem.eql(u8, target.table, label.table)) {
                if (label.table_id != 0 and label.table_id != target.table_id) return error.CatalogGenerationChanged;
                found = true;
            };
            if (!found) return error.UndefinedGraphTable;
        }
        for (self.snapshots) |snapshot| try snapshot.backend.vtable.bind(snapshot.backend.ptr, a, graph);
        self.graph = graph;
    }
    const Cursor = struct { owner: usize, after: ?[]const u8 = null };
    fn cursor(self: *ReadSet, a: A, table: []const u8, encoded: ?[]const u8) !Cursor {
        if (encoded) |json| {
            const result = try std.json.parseFromSliceLeaky(Cursor, a, json, .{});
            if (result.owner >= self.targets.len or !std.mem.eql(u8, self.targets[result.owner].table, table)) return error.CypherInvalidBackendCursor;
            return result;
        }
        for (self.targets, 0..) |target, i| if (std.mem.eql(u8, target.table, table)) return .{ .owner = i };
        return error.UndefinedGraphTable;
    }
    fn next(self: *ReadSet, a: A, table: []const u8, position: Cursor, after: ?[]const u8) !?[]const u8 {
        if (after) |continuation| return try std.json.Stringify.valueAlloc(a, Cursor{ .owner = position.owner, .after = continuation }, .{});
        for (self.targets[position.owner + 1 ..], position.owner + 1..) |target, i| if (std.mem.eql(u8, target.table, table)) return try std.json.Stringify.valueAlloc(a, Cursor{ .owner = i }, .{});
        return null;
    }
    fn scan(ptr: *anyopaque, a: A, table: []const u8, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        return scanWhere(ptr, a, table, &.{}, encoded, limit);
    }
    fn scanWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
        const self = cast(ptr);
        try checkpoint(ptr);
        const position = try self.cursor(a, table, encoded);
        const owner = self.snapshots[position.owner].backend;
        const page = if (owner.vtable.scan_where) |filtered| try filtered(owner.ptr, a, table, filters, position.after, limit) else try owner.vtable.scan(owner.ptr, a, table, position.after, limit);
        try checkpoint(ptr);
        return .{ .items = page.items, .examined = page.examined, .unfiltered = page.unfiltered, .next = try self.next(a, table, position, page.next) };
    }
    const OrderedPosition = struct { exact_keys: bool = false, coarse: bool = false, owner: usize, batched: bool = false, after: ?[]const u8 = null, finished: bool = false };
    const OrderedHead = struct { node: *const v.Node, after: ?[]const u8 };
    const OrderedSlot = struct {
        position: OrderedPosition,
        head: ?OrderedHead = null,
        buffer: []const *const v.Node = &.{},
        cursors: ?[]const ?[]const u8 = null,
        offset: usize = 0,
        next: ?[]const u8 = null,
        primed: bool = false,
    };
    const OrderedMerge = struct {
        read_set: *ReadSet,
        a: A,
        table: []const u8,
        filters: []const b.PropertyFilter,
        order: []const b.PropertyOrder,
        max_examined: usize,
        examined: usize = 0,
        quantum: usize = 1,
        demand: usize = 1,
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.read_set.work) |work| try work.charge(work.ptr, amount);
        }
        fn fetch(self: *@This(), slot: *OrderedSlot) !void {
            const owner = self.read_set.snapshots[slot.position.owner].backend;
            const read = owner.vtable.scan_ordered orelse return error.CypherInvalidBackendPage;
            while (!slot.position.finished) {
                if (slot.offset < slot.buffer.len) {
                    const index = slot.offset;
                    slot.offset += 1;
                    slot.head = .{ .node = slot.buffer[index], .after = if (slot.cursors) |positions| positions[index] else slot.next };
                    return;
                }
                if (self.examined == self.max_examined) return error.CypherAnchorBudgetExceeded;
                // Prime one head per owner before speculating. Refills scale
                // with the remaining output demand, not the entire table.
                const count = if (slot.position.batched and slot.primed) @max(1, @min(self.quantum, self.demand)) else 1;
                const page = (try read(owner.ptr, self.a, self.table, self.filters, self.order, slot.position.after, count, self.max_examined - self.examined)) orelse return error.CypherInvalidBackendPage;
                slot.primed = true;
                if ((page.stable_ties == slot.position.coarse or page.exact_keys != slot.position.exact_keys)) return error.CypherInvalidBackendPage;
                const examined = page.examined orelse page.items.len;
                if (page.items.len > count or examined < page.items.len or examined > self.max_examined - self.examined) return error.CypherInvalidBackendPage;
                self.examined += examined;
                if (page.items.len != 0) {
                    for (page.items) |node| if (!std.mem.eql(u8, node.identity.table, self.table)) return error.CypherInvalidBackendPage;
                    if (page.cursors) |positions| {
                        if (positions.len != page.items.len or (positions[positions.len - 1] == null and page.next != null)) return error.CypherInvalidBackendPage;
                        for (positions[0 .. positions.len - 1]) |position| if (position == null) return error.CypherInvalidBackendPage;
                    } else if (page.items.len > 1) return error.CypherInvalidBackendPage;
                    slot.buffer = page.items;
                    slot.cursors = page.cursors;
                    slot.offset = 0;
                    slot.next = page.next;
                    continue;
                }
                if (page.next == null) slot.position.finished = true else {
                    if (slot.position.after) |before| if (std.mem.eql(u8, before, page.next.?)) return error.CypherInvalidBackendPage;
                    slot.position.after = page.next;
                }
            }
            slot.head = null;
        }
        fn less(self: *@This(), work: *@import("value_work.zig").Work, slots: []const OrderedSlot, left: usize, right: usize) !bool {
            try charge(self, 1);
            for (self.order) |order| {
                const comparison = try work.order((v.Value{ .node = slots[left].head.?.node }).property(order.property), (v.Value{ .node = slots[right].head.?.node }).property(order.property));
                if (comparison != .eq) return if (order.descending) comparison == .gt else comparison == .lt;
            }
            if (self.read_set.ordered_identity_ties) {
                const comparison = try work.order(.{ .string = slots[left].head.?.node.identity.key }, .{ .string = slots[right].head.?.node.identity.key });
                if (comparison != .eq) return comparison == .lt;
            }
            // Plain distributed scans concatenate owners in catalog order.
            return slots[left].position.owner < slots[right].position.owner;
        }
        fn sift(self: *@This(), work: *@import("value_work.zig").Work, slots: []const OrderedSlot, heap: []usize, root: usize) !void {
            var parent = root;
            while (parent * 2 + 1 < heap.len) {
                var child = parent * 2 + 1;
                if (child + 1 < heap.len and try self.less(work, slots, heap[child + 1], heap[child])) child += 1;
                if (!try self.less(work, slots, heap[child], heap[parent])) break;
                std.mem.swap(usize, &heap[parent], &heap[child]);
                parent = child;
            }
        }
    };
    /// Merge one certified head per owner. Continuations retain positions,
    /// never serialized documents; unused lookahead is re-read on the next page
    /// and is always included in the coordinator's examined-record budget.
    fn scanOrdered(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, encoded: ?[]const u8, limit: usize, max_examined: usize) !?b.Page(*const v.Node) {
        const self = cast(ptr);
        try checkpoint(ptr);
        var single: ?usize = null;
        var owner_count: usize = 0;
        for (self.targets, 0..) |target, index| if (std.mem.eql(u8, target.table, table)) {
            owner_count += 1;
            single = index;
        };
        if (owner_count == 1) {
            const index = single.?;
            const owner = self.snapshots[index].backend;
            const read = owner.vtable.scan_ordered orelse return null;
            var temporary_arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer temporary_arena.deinit();
            const position = if (encoded) |bytes| blk: {
                const saved = try std.json.parseFromSliceLeaky([]const OrderedPosition, temporary_arena.allocator(), bytes, .{});
                if (saved.len != 1 or saved[0].owner != index or saved[0].finished) return error.CypherInvalidBackendCursor;
                break :blk saved[0];
            } else OrderedPosition{ .owner = index };
            const page = (try read(owner.ptr, a, table, filters, order, position.after, limit, max_examined)) orelse {
                if (encoded != null) return error.CypherInvalidBackendPage;
                return null;
            };
            if (encoded != null and (position.coarse == page.stable_ties or position.exact_keys != page.exact_keys)) return error.CypherInvalidBackendPage;
            try checkpoint(ptr);
            const cursors = if (page.cursors) |positions| blk: {
                if (positions.len != page.items.len) return error.CypherInvalidBackendPage;
                const saved = try a.alloc(?[]const u8, positions.len);
                for (positions, saved) |position_after, *out| out.* = if (position_after) |after| try std.json.Stringify.valueAlloc(a, &.{OrderedPosition{ .exact_keys = page.exact_keys, .coarse = !page.stable_ties, .owner = index, .after = after }}, .{}) else null;
                break :blk saved;
            } else null;
            return .{ .exact_keys = page.exact_keys, .stable_ties = page.stable_ties, .encounter_owners = page.encounter_owners, .items = page.items, .cursors = cursors, .examined = page.examined, .next = if (page.next) |after| try std.json.Stringify.valueAlloc(a, &.{OrderedPosition{ .exact_keys = page.exact_keys, .coarse = !page.stable_ties, .owner = index, .after = after }}, .{}) else null };
        }
        var arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer arena.deinit();
        const temporary = arena.allocator();
        var positions: std.ArrayList(OrderedPosition) = .empty;
        if (encoded) |bytes| {
            const saved = try std.json.parseFromSliceLeaky([]const OrderedPosition, temporary, bytes, .{});
            try positions.appendSlice(temporary, saved);
        } else {
            for (self.targets, self.snapshots, 0..) |target, snapshot, owner| {
                if (!std.mem.eql(u8, target.table, table)) continue;
                const read = snapshot.backend.vtable.scan_ordered orelse return null;
                const proof = (try read(snapshot.backend.ptr, temporary, table, filters, order, null, 0, 0)) orelse return null;
                if (proof.items.len != 0 or (proof.examined orelse 0) != 0 or proof.next == null) return error.CypherInvalidBackendPage;
                try positions.append(temporary, .{ .exact_keys = proof.exact_keys, .coarse = !proof.stable_ties, .owner = owner, .after = proof.next, .batched = proof.cursors != null });
            }
        }
        var count: usize = 0;
        for (self.targets, 0..) |target, owner| if (std.mem.eql(u8, target.table, table)) {
            if (count >= positions.items.len or positions.items[count].owner != owner) return error.CypherInvalidBackendCursor;
            count += 1;
        };
        if (count != positions.items.len or count == 0) return error.CypherInvalidBackendCursor;
        const stable_ties = for (positions.items) |position| {
            if (position.coarse) break false;
        } else true;
        const exact_keys = for (positions.items) |position| {
            if (!position.exact_keys) break false;
        } else true;
        if (limit == 0) return .{ .exact_keys = exact_keys, .stable_ties = stable_ties, .items = &.{}, .cursors = if (self.ordered_item_cursors) &.{} else null, .examined = 0, .next = try std.json.Stringify.valueAlloc(a, positions.items, .{}) };
        const slots = try temporary.alloc(OrderedSlot, count);
        var heap: std.ArrayList(usize) = .empty;
        var merge: OrderedMerge = .{ .read_set = self, .a = a, .table = table, .filters = filters, .order = order, .max_examined = max_examined, .quantum = if (max_examined > limit *| 2 +| count) @max(1, @min(64, limit / count)) else 1, .demand = limit };
        var work: @import("value_work.zig").Work = .{ .a = temporary, .ptr = &merge, .charge = OrderedMerge.charge };
        for (slots, positions.items, 0..) |*slot, position, index| {
            slot.* = .{ .position = position };
            try merge.fetch(slot);
            if (slot.head != null) try heap.append(temporary, index);
        }
        var parent = heap.items.len / 2;
        while (parent != 0) {
            parent -= 1;
            try merge.sift(&work, slots, heap.items, parent);
        }
        var output: std.ArrayList(*const v.Node) = .empty;
        var encounter_owners: std.ArrayList(usize) = .empty;
        var cursors: std.ArrayList(?[]const u8) = .empty;
        while (output.items.len < limit and heap.items.len != 0) {
            const selected = heap.items[0];
            const slot = &slots[selected];
            try output.append(a, slot.head.?.node);
            if (!stable_ties) try encounter_owners.append(a, slot.position.owner);
            slot.position.after = slot.head.?.after;
            slot.position.finished = slot.position.after == null;
            slot.head = null;
            if (self.ordered_item_cursors) {
                var more = false;
                for (slots, positions.items) |current, *position| {
                    position.* = current.position;
                    more = more or !position.finished;
                }
                const saved = if (more) try std.json.Stringify.valueAlloc(a, positions.items, .{}) else null;
                try OrderedMerge.charge(&merge, count + if (saved) |bytes| bytes.len / 64 else @as(usize, 0));
                try cursors.append(a, saved);
            }
            if (output.items.len == limit) break;
            merge.demand = limit - output.items.len;
            try merge.fetch(slot);
            if (slot.head == null) {
                heap.items[0] = heap.items[heap.items.len - 1];
                heap.items.len -= 1;
            }
            if (heap.items.len != 0) try merge.sift(&work, slots, heap.items, 0);
        }
        var more = false;
        for (slots, positions.items) |slot, *position| {
            position.* = slot.position;
            more = more or !position.finished;
        }
        try checkpoint(ptr);
        const continuation = if (more) try std.json.Stringify.valueAlloc(a, positions.items, .{}) else null;
        if (cursors.items.len != 0) cursors.items[cursors.items.len - 1] = continuation;
        return .{ .exact_keys = exact_keys, .stable_ties = stable_ties, .encounter_owners = if (!stable_ties) encounter_owners.items else null, .items = try output.toOwnedSlice(a), .cursors = if (self.ordered_item_cursors) cursors.items else null, .examined = merge.examined, .next = continuation };
    }
    fn estimateWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) !?usize {
        const self = cast(ptr);
        var total: usize = 0;
        for (self.targets, self.snapshots) |target, snapshot| {
            if (!std.mem.eql(u8, target.table, table)) continue;
            try checkpoint(ptr);
            const estimate = snapshot.backend.vtable.estimate_where orelse return null;
            const count = (try estimate(snapshot.backend.ptr, a, table, filters, @max(1, limit -| total))) orelse return null;
            if (count > limit -| total) return null;
            total += count;
        }
        try checkpoint(ptr);
        return total;
    }
    fn get(ptr: *anyopaque, a: A, identity: v.Identity) !?*const v.Node {
        const self = cast(ptr);
        try checkpoint(ptr);
        var result: ?*const v.Node = null;
        for (self.targets, self.snapshots) |target, snapshot| {
            if (!target.owns(identity)) continue;
            if (try snapshot.backend.vtable.get(snapshot.backend.ptr, a, identity)) |node| {
                if (result != null) return error.CypherOverlappingOwners;
                result = node;
            }
        }
        try checkpoint(ptr);
        return result;
    }
    fn getMany(ptr: *anyopaque, a: A, identities: []const v.Identity) ![]const ?*const v.Node {
        const self = cast(ptr);
        if (identities.len > 128) return error.CypherOwnerBudgetExceeded;
        try checkpoint(ptr);
        const result = try a.alloc(?*const v.Node, identities.len);
        @memset(result, null);
        for (self.targets, self.snapshots) |target, snapshot| {
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            var selected: std.ArrayList(v.Identity) = .empty;
            var positions: std.ArrayList(usize) = .empty;
            for (identities, 0..) |identity, index| if (target.owns(identity)) {
                try selected.append(temporary, identity);
                try positions.append(temporary, index);
            };
            if (selected.items.len == 0) continue;
            const owner = snapshot.backend;
            const nodes = if (owner.vtable.get_many) |batch| try batch(owner.ptr, temporary, selected.items) else blk: {
                const fetched = try temporary.alloc(?*const v.Node, selected.items.len);
                for (selected.items, fetched) |identity, *node| node.* = try owner.vtable.get(owner.ptr, temporary, identity);
                break :blk fetched;
            };
            if (nodes.len != positions.items.len) return error.CypherInvalidBackendPage;
            for (nodes, positions.items) |node, index| if (node) |value| {
                if (result[index] != null) return error.CypherOverlappingOwners;
                result[index] = (try (v.Value{ .node = value }).clone(a)).node;
            };
        }
        try checkpoint(ptr);
        return result;
    }
    fn expand(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, direction: ast.Direction, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
        const self = cast(ptr);
        // Physical adjacency, including incoming adjacency, belongs to source
        // owners. Resolve the label from the statement-bound graph at bind.
        const table = self.mappingSource(mapping) orelse return error.UndefinedGraphTable;
        try checkpoint(ptr);
        const position = try self.cursor(a, table, encoded);
        const owner = self.snapshots[position.owner].backend;
        const page = try owner.vtable.expand(owner.ptr, a, mapping, identity, direction, position.after, limit);
        try checkpoint(ptr);
        return .{ .items = page.items, .examined = page.examined, .next = try self.next(a, table, position, page.next) };
    }
    fn expandBetween(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, peer: v.Identity, direction: ast.Direction, filters: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize, max_examined: usize) !b.Page(*const v.Relationship) {
        const self = cast(ptr);
        const table = self.mappingSource(mapping) orelse return error.UndefinedGraphTable;
        try checkpoint(ptr);
        const position = try self.cursor(a, table, encoded);
        const owner = self.snapshots[position.owner].backend;
        const page = if (owner.vtable.expand_between) |between|
            try between(owner.ptr, a, mapping, identity, peer, direction, filters, position.after, limit, max_examined)
        else
            try owner.vtable.expand(owner.ptr, a, mapping, identity, direction, position.after, limit);
        try checkpoint(ptr);
        return .{ .items = page.items, .examined = page.examined, .next = try self.next(a, table, position, page.next) };
    }
    fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
        const self = cast(ptr);
        try checkpoint(ptr);
        const parameters = input.parameters;
        if (parameters != .map) return error.CypherTypeMismatch;
        const requested = parameters.property("limit");
        const limit: usize = if (requested == .null) 10 else if (requested == .integer and requested.integer > 0 and requested.integer <= 1024) @intCast(requested.integer) else return error.CypherInvalidSearchLimit;
        var best: std.ArrayList(b.Seed) = .empty;
        var found = false;
        var examined: usize = 0;
        var approximate = false;
        var searched_owners: usize = 0;
        for (self.targets, self.snapshots) |target, snapshot| {
            if (!std.mem.eql(u8, target.table, table)) continue;
            found = true;
            searched_owners += 1;
            try checkpoint(ptr);
            const callback = snapshot.backend.vtable.search orelse return error.CypherSearchUnavailable;
            var demand = input;
            demand.max_examined = input.max_examined -| examined;
            const page = try callback(snapshot.backend.ptr, a, table, demand);
            if (page.examined > demand.max_examined) return error.CypherAnchorBudgetExceeded;
            examined += page.examined;
            approximate = approximate or page.approximate;
            const seeds = page.seeds;
            if (seeds.len > limit) return error.CypherInvalidSearchResult;
            for (seeds) |seed| {
                if (!std.math.isFinite(seed.score) or !std.mem.eql(u8, seed.node.identity.table, table)) return error.CypherInvalidSearchResult;
                for (best.items) |other| if (other.node.identity.eql(seed.node.identity)) return error.CypherOverlappingOwners;
                var index: usize = 0;
                while (index < best.items.len and less({}, best.items[index], seed)) : (index += 1) {}
                if (index < limit) {
                    try best.insert(a, index, seed);
                    if (best.items.len > limit) best.items.len = limit;
                }
            }
        }
        if (!found) return error.UndefinedGraphTable;
        try checkpoint(ptr);
        // Text and hybrid scores currently use owner-local corpus statistics.
        // Report this explicitly until a global-statistics scorer is available.
        return .{ .seeds = try best.toOwnedSlice(a), .examined = examined, .approximate = approximate or searched_owners > 1 };
    }
    fn less(_: void, left: b.Seed, right: b.Seed) bool {
        if (left.score != right.score) return left.score > right.score;
        return std.mem.order(u8, left.node.identity.key, right.node.identity.key) == .lt;
    }
    fn mappingSource(self: *ReadSet, mapping: catalog.Relationship) ?[]const u8 {
        return (self.graph orelse return null).label(mapping.source_label).?.table;
    }
};

test "distributed graph captures the whole cohort and pages immutable owners" {
    const Fixture = struct {
        held: usize = 0,
        admitted: usize = 0,
        forks: usize = 0,
        closed: usize = 0,
        busy_once: bool = true,
        revoked: bool = false,
        validations: usize = 0,
        covered: bool = false,
        owners: [2]Owner = undefined,
        const Owner = struct {
            fixture: *Self,
            index: usize,
            node: v.Node,
            gets: usize = 0,
            fn checkpoint(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.fixture.revoked) return error.Forbidden;
            }
            fn validate(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.fixture.validations += 1;
                try Owner.checkpoint(ptr);
            }
            fn release(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.fixture.held -= 1;
            }
            fn fork(ptr: *anyopaque, _: A) !Snapshot {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(@as(usize, 2), self.fixture.held);
                self.fixture.forks += 1;
                return .{ .ptr = self, .validate = validate, .close = close, .validation_covered_by_source = self.fixture.covered, .backend = .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = Owner.checkpoint, .scan = scan, .get = get, .expand = expand, .search = search } } };
            }
            fn close(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.fixture.closed += 1;
            }
            fn bind(ptr: *anyopaque, _: A, _: catalog.Graph) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try std.testing.expectEqual(@as(usize, 0), self.fixture.held);
            }
            fn scan(ptr: *anyopaque, a: A, _: []const u8, after: ?[]const u8, _: usize) !b.Page(*const v.Node) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                try Owner.checkpoint(ptr);
                if (after != null) return error.CypherInvalidBackendCursor;
                return .{ .items = try a.dupe(*const v.Node, &.{&self.node}), .examined = 1 };
            }
            fn get(ptr: *anyopaque, _: A, identity: v.Identity) !?*const v.Node {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.gets += 1;
                return if (self.node.identity.eql(identity)) &self.node else null;
            }
            fn search(ptr: *anyopaque, a: A, _: []const u8, _: b.SearchInput) !b.SearchPage {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                return .{ .seeds = try a.dupe(b.Seed, &.{.{ .node = &self.node, .score = @floatFromInt(self.index + 1) }}), .examined = 1 };
            }
            fn expand(_: *anyopaque, _: A, _: catalog.Relationship, _: v.Identity, _: ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
                return .{ .items = &.{} };
            }
        };
        const Self = @This();
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (self.revoked) return error.Forbidden;
        }
        fn admit(ptr: *anyopaque, _: Target) !void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 0), self.held);
            self.admitted += 1;
        }
        fn capture(ptr: *anyopaque, _: A, target: Target) !?Fence {
            const self: *Self = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqual(@as(usize, 2), self.admitted);
            if (target.group_id == 2 and self.busy_once) {
                self.busy_once = false;
                return null;
            }
            self.held += 1;
            return .{ .ptr = &self.owners[@intCast(target.group_id - 1)], .validate = Owner.validate, .fork = Owner.fork, .release = Owner.release };
        }
    };
    var fixture: Fixture = .{};
    fixture.owners = .{
        .{ .fixture = &fixture, .index = 0, .node = .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"Document"} } },
        .{ .fixture = &fixture, .index = 1, .node = .{ .identity = .{ .table = "docs", .key = "b" }, .labels = &.{"Document"} } },
    };
    var set = try ReadSet.acquire(std.testing.allocator, .{ .ptr = &fixture, .checkpoint = Fixture.checkpoint, .admit = Fixture.admit, .capture = Fixture.capture }, &.{ .{ .table = "docs", .table_id = 7, .group_id = 1, .ranges = &.{.{ .end = "b" }} }, .{ .table = "docs", .table_id = 7, .group_id = 2, .ranges = &.{.{ .start = "b" }} } });
    defer set.deinit();
    try std.testing.expectEqual(@as(usize, 0), fixture.held);
    try std.testing.expectEqual(@as(usize, 2), fixture.forks);
    var scratch = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer scratch.deinit();
    const provider = set.backend();
    _ = try provider.vtable.get(provider.ptr, scratch.allocator(), .{ .table = "docs", .key = "a" });
    const batch = try provider.vtable.get_many.?(provider.ptr, scratch.allocator(), &.{ .{ .table = "docs", .key = "a" }, .{ .table = "docs", .key = "b" } });
    try std.testing.expect(batch[0].?.identity.eql(fixture.owners[0].node.identity));
    try std.testing.expect(batch[1].?.identity.eql(fixture.owners[1].node.identity));
    try std.testing.expectEqual(@as(usize, 2), fixture.owners[0].gets);
    try std.testing.expectEqual(@as(usize, 1), fixture.owners[1].gets);
    const root = @import("root.zig");
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(std.testing.allocator, "MATCH (n:Document) RETURN antfly.documentKey(n) AS key ORDER BY key", .{}, &diagnostic);
    defer compiled.deinit();
    var result = try root.runtime.execute(std.testing.allocator, set.backend(), .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "Document", .table = "docs", .table_id = 7 }}, .relationships = &.{} }, &compiled, &.{}, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("a", result.rows[0][0].string);
    try std.testing.expectEqualStrings("b", result.rows[1][0].string);
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const backend = set.backend();
    const seeds = try backend.vtable.search.?(backend.ptr, arena.allocator(), "docs", .{ .parameters = .{ .map = &.{.{ .name = "limit", .value = .{ .integer = 1 } }} } });
    try std.testing.expectEqual(@as(usize, 1), seeds.seeds.len);
    try std.testing.expectEqualStrings("b", seeds.seeds[0].node.identity.key);
    try std.testing.expectEqual(@as(f64, 2), seeds.seeds[0].score);
    try std.testing.expect(seeds.approximate);
    try std.testing.expectError(error.CypherInvalidSearchLimit, backend.vtable.search.?(backend.ptr, arena.allocator(), "docs", .{ .parameters = .{ .map = &.{.{ .name = "limit", .value = .{ .integer = 1025 } }} } }));
    const independent_before = fixture.validations;
    try backend.vtable.checkpoint(backend.ptr);
    try std.testing.expectEqual(independent_before + 2, fixture.validations);
    // Coverage is fixed when acquiring the immutable cohort. No owner loop
    // remains on the runtime path when all checks belong to the statement.
    var covered_fixture: Fixture = .{ .covered = true };
    for (&covered_fixture.owners, 0..) |*owner, index| owner.* = .{ .fixture = &covered_fixture, .index = index, .node = fixture.owners[index].node };
    var covered_set = try ReadSet.acquire(std.testing.allocator, .{ .ptr = &covered_fixture, .checkpoint = Fixture.checkpoint, .admit = Fixture.admit, .capture = Fixture.capture }, set.targets);
    defer covered_set.deinit();
    try std.testing.expectEqual(@as(usize, 0), covered_set.independent_validations.len);
    const covered_before = covered_fixture.validations;
    for (0..1000) |_| try covered_set.backend().vtable.checkpoint(&covered_set);
    try std.testing.expectEqual(covered_before, covered_fixture.validations);
    covered_fixture.revoked = true;
    try std.testing.expectError(error.Forbidden, covered_set.backend().vtable.checkpoint(&covered_set));
    fixture.revoked = true;
    try std.testing.expectError(error.Forbidden, set.backend().vtable.checkpoint(&set));
}

test "ordered owner streams merge stable ties through RPC and bounded continuation state" {
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    const Host = struct {
        const Self = @This();
        revoked: bool = false,
        const nodes = [_]v.Node{
            .{ .identity = .{ .table = "docs", .key = "a" }, .labels = &.{"D"}, .properties = &.{.{ .name = "score", .value = .{ .integer = 1 } }} },
            .{ .identity = .{ .table = "docs", .key = "b" }, .labels = &.{"D"}, .properties = &.{.{ .name = "score", .value = .{ .integer = 5 } }} },
            .{ .identity = .{ .table = "docs", .key = "c" }, .labels = &.{"D"}, .properties = &.{.{ .name = "score", .value = .{ .integer = 3 } }} },
            .{ .identity = .{ .table = "docs", .key = "d" }, .labels = &.{"D"}, .properties = &.{.{ .name = "score", .value = .{ .integer = 5 } }} },
            .{ .identity = .{ .table = "docs", .key = "e" }, .labels = &.{"D"}, .properties = &.{.{ .name = "score", .value = .{ .integer = 2 } }} },
            .{ .identity = .{ .table = "docs", .key = "f" }, .labels = &.{"D"}, .properties = &.{.{ .name = "score", .value = .{ .integer = 4 } }} },
        };
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.revoked) return error.Forbidden;
        }
        const Owner = struct {
            index: usize,
            enabled: bool = true,
            deny_b: bool = false,
            examined: usize = 0,
            fn backend(self: *@This()) b.Backend {
                return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = check, .get = get, .expand = expand, .scan = scan, .scan_ordered = ordered } };
            }
            fn bind(_: *anyopaque, _: A, _: catalog.Graph) !void {}
            fn check(_: *anyopaque) !void {}
            fn close(_: *anyopaque) void {}
            fn get(_: *anyopaque, _: A, _: v.Identity) !?*const v.Node {
                return null;
            }
            fn expand(_: *anyopaque, _: A, _: catalog.Relationship, _: v.Identity, _: ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
                return .{ .items = &.{} };
            }
            fn scan(ptr: *anyopaque, a: A, _: []const u8, after: ?[]const u8, limit: usize) !b.Page(*const v.Node) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const start = if (after) |bytes| try std.fmt.parseInt(usize, bytes, 10) else 0;
                const end = @min(3, start + limit);
                var output: std.ArrayList(*const v.Node) = .empty;
                for (start..end) |position| {
                    const node = &Self.nodes[self.index * 3 + position];
                    if (self.deny_b and std.mem.eql(u8, node.identity.key, "b")) continue;
                    try output.append(a, node);
                }
                return .{ .items = output.items, .examined = end - start, .next = if (end < 3) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
            }
            fn ordered(ptr: *anyopaque, a: A, _: []const u8, _: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, max_examined: usize) !?b.Page(*const v.Node) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (!self.enabled or order.len != 1 or !std.mem.eql(u8, order[0].property, "score")) return null;
                const start = if (after) |bytes| try std.fmt.parseInt(usize, bytes, 10) else 0;
                if (limit == 0) return .{ .items = &.{}, .cursors = &.{}, .examined = 0, .next = try std.fmt.allocPrint(a, "{d}", .{start}) };
                const end = @min(3, start + @min(limit, max_examined));
                const sorted = if (self.index == 0) [_]usize{ 1, 2, 0 } else [_]usize{ 0, 2, 1 };
                var output: std.ArrayList(*const v.Node) = .empty;
                for (start..end) |position| {
                    self.examined += 1;
                    const node = &Self.nodes[self.index * 3 + sorted[if (order[0].descending) position else 2 - position]];
                    if (self.deny_b and std.mem.eql(u8, node.identity.key, "b")) continue;
                    try output.append(a, node);
                }
                const cursors = try a.alloc(?[]const u8, output.items.len);
                var emitted: usize = 0;
                for (start..end) |position| {
                    const node = &Self.nodes[self.index * 3 + sorted[if (order[0].descending) position else 2 - position]];
                    if (self.deny_b and std.mem.eql(u8, node.identity.key, "b")) continue;
                    cursors[emitted] = if (position + 1 == 3) null else try std.fmt.allocPrint(a, "{d}", .{position + 1});
                    emitted += 1;
                }
                return .{ .items = output.items, .cursors = cursors, .examined = end - start, .next = if (end < 3) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
            }
        };
        fn rpc(ptr: *anyopaque, a: A, request: []const u8) ![]u8 {
            const owner: *@import("rpc.zig").Owner = @ptrCast(@alignCast(ptr));
            return owner.execute(a, request);
        }
    };
    var host: Host = .{};
    var owners = [_]Host.Owner{ .{ .index = 0 }, .{ .index = 1 } };
    var endpoints = [_]@import("rpc.zig").Owner{
        .{ .backend = owners[0].backend(), .graph = graph, .table = "docs" },
        .{ .backend = owners[1].backend(), .graph = graph, .table = "docs" },
    };
    var clients = [_]@import("rpc.zig").Client{
        .{ .channel = .{ .ptr = &endpoints[0], .call = Host.rpc }, .incarnation = 1, .revision = 1 },
        .{ .channel = .{ .ptr = &endpoints[1], .call = Host.rpc }, .incarnation = 1, .revision = 1 },
    };
    var snapshots = [_]Snapshot{
        .{ .backend = clients[0].backend(), .ptr = &owners[0], .validate = Host.Owner.check, .close = Host.Owner.close },
        .{ .backend = clients[1].backend(), .ptr = &owners[1], .validate = Host.Owner.check, .close = Host.Owner.close },
    };
    const targets = [_]Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
    var set: ReadSet = .{ .a = std.testing.allocator, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &host, .checkpoint = Host.checkpoint, .admit = undefined, .capture = undefined } };
    const backend = set.backend();
    const order = &.{b.PropertyOrder{ .property = "score", .descending = true }};
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var cursor: ?[]const u8 = null;
    var text: std.ArrayList(u8) = .empty;
    while (true) {
        const page = (try backend.vtable.scan_ordered.?(backend.ptr, arena.allocator(), "docs", &.{}, order, cursor, 1, 64)).?;
        for (page.items) |node| try text.appendSlice(arena.allocator(), node.identity.key);
        cursor = page.next orelse break;
    }
    try std.testing.expectEqualStrings("bdfcea", text.items);
    try std.testing.expectError(error.CypherAnchorBudgetExceeded, backend.vtable.scan_ordered.?(backend.ptr, arena.allocator(), "docs", &.{}, order, null, 1, 1));
    owners[1].enabled = false;
    owners[0].examined = 0;
    owners[1].examined = 0;
    try std.testing.expectEqual(@as(?b.Page(*const v.Node), null), try backend.vtable.scan_ordered.?(backend.ptr, arena.allocator(), "docs", &.{}, order, null, 5, 64));
    try std.testing.expectEqual(@as(usize, 0), owners[0].examined + owners[1].examined);
    owners[1].enabled = true;
    for ([_]bool{ false, true }) |denied| {
        owners[0].deny_b = denied;
        var diagnostic: @import("compiler.zig").Diagnostic = .{};
        var compiled = try @import("compiler.zig").compile(std.testing.allocator, "MATCH (n:D) RETURN n ORDER BY n.score DESC SKIP 1 LIMIT 4", .{}, &diagnostic);
        defer compiled.deinit();
        var result = try @import("runtime.zig").execute(std.testing.allocator, backend, graph, &compiled, &.{}, .{ .scanned_anchors = 12 });
        defer result.deinit();
        const expected = if (denied) "fcea" else "dfce";
        for (result.rows, expected) |row, key| try std.testing.expectEqual(key, row[0].node.identity.key[0]);
        try std.testing.expect(result.stats.scanned_anchors <= 12);
    }
    host.revoked = true;
    try std.testing.expectError(error.Forbidden, backend.vtable.scan_ordered.?(backend.ptr, arena.allocator(), "docs", &.{}, order, null, 1, 64));
}

test "ordered RPC buffers keep calls bounded and resume every unconsumed item" {
    const rpc = @import("rpc.zig");
    const root = @import("root.zig");
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    const Fixture = struct {
        nodes: [1024]v.Node = undefined,
        properties: [1024][1]v.Entry = undefined,
        keys: [1024][16]u8 = undefined,
        hidden: bool = false,
        const Owner = struct {
            fixture: *ThisFixture,
            index: usize,
            calls: usize = 0,
            records: usize = 0,
            fn backend(self: *@This()) b.Backend {
                return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = check, .scan = scan, .scan_ordered = ordered, .get = get, .expand = expand } };
            }
            fn bind(_: *anyopaque, _: A, _: catalog.Graph) !void {}
            fn check(_: *anyopaque) !void {}
            fn close(_: *anyopaque) void {}
            fn get(_: *anyopaque, _: A, _: v.Identity) !?*const v.Node {
                return null;
            }
            fn expand(_: *anyopaque, _: A, _: catalog.Relationship, _: v.Identity, _: ast.Direction, _: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
                return .{ .items = &.{} };
            }
            fn scan(_: *anyopaque, _: A, _: []const u8, _: ?[]const u8, _: usize) !b.Page(*const v.Node) {
                return error.UnexpectedScan;
            }
            fn ordered(ptr: *anyopaque, a: A, _: []const u8, _: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8, limit: usize, maximum: usize) !?b.Page(*const v.Node) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (order.len != 1 or !order[0].descending or !std.mem.eql(u8, order[0].property, "score")) return null;
                if (limit == 0) return .{ .items = &.{}, .cursors = &.{}, .examined = 0, .next = "0" };
                const start = if (after) |bytes| try std.fmt.parseInt(usize, bytes, 10) else 0;
                const end = @min(512, start + @min(limit, maximum));
                self.records += end - start;
                var nodes: std.ArrayList(*const v.Node) = .empty;
                var cursors: std.ArrayList(?[]const u8) = .empty;
                for (start..end) |position| {
                    const node = &self.fixture.nodes[self.index * 512 + 511 - position];
                    if (self.fixture.hidden and @mod(node.properties[0].value.integer, 2) == 0) continue;
                    try nodes.append(a, node);
                    try cursors.append(a, if (position == 511) null else try std.fmt.allocPrint(a, "{d}", .{position + 1}));
                }
                return .{ .items = nodes.items, .cursors = cursors.items, .examined = end - start, .next = if (end < 512) try std.fmt.allocPrint(a, "{d}", .{end}) else null };
            }
        };
        const ThisFixture = @This();
        fn checkSource(_: *anyopaque) !void {}
        fn invoke(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const endpoint: *rpc.Owner = @ptrCast(@alignCast(ptr));
            const owner: *Owner = @ptrCast(@alignCast(endpoint.backend.ptr));
            owner.calls += 1;
            return endpoint.execute(a, body);
        }
    };
    var fixture: Fixture = .{};
    for (&fixture.nodes, &fixture.properties, &fixture.keys, 0..) |*node, *properties, *key, index| {
        properties.* = .{.{ .name = "score", .value = .{ .integer = @intCast(index % 512 * 2 + index / 512) } }};
        node.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.bufPrint(key, "k{d}", .{index}) }, .labels = &.{"D"}, .properties = properties };
    }
    var owners = [_]Fixture.Owner{ .{ .fixture = &fixture, .index = 0 }, .{ .fixture = &fixture, .index = 1 } };
    var endpoints = [_]rpc.Owner{ .{ .backend = owners[0].backend(), .graph = graph, .table = "docs" }, .{ .backend = owners[1].backend(), .graph = graph, .table = "docs" } };
    var clients = [_]rpc.Client{ .{ .channel = .{ .ptr = &endpoints[0], .call = Fixture.invoke }, .incarnation = 1, .revision = 1 }, .{ .channel = .{ .ptr = &endpoints[1], .call = Fixture.invoke }, .incarnation = 1, .revision = 1 } };
    var snapshots = [_]Snapshot{ .{ .backend = clients[0].backend(), .ptr = &owners[0], .validate = Fixture.Owner.check, .close = Fixture.Owner.close }, .{ .backend = clients[1].backend(), .ptr = &owners[1], .validate = Fixture.Owner.check, .close = Fixture.Owner.close } };
    const targets = [_]Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
    var set: ReadSet = .{ .a = std.testing.allocator, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &fixture, .checkpoint = Fixture.checkSource, .admit = undefined, .capture = undefined } };
    var diagnostic: root.compiler.Diagnostic = .{};
    var compiled = try root.compiler.compile(std.testing.allocator, "MATCH (n:D) RETURN n.score ORDER BY n.score DESC LIMIT 1000", .{}, &diagnostic);
    defer compiled.deinit();
    for ([_]bool{ false, true }) |hidden| {
        fixture.hidden = hidden;
        for (&owners) |*owner| {
            owner.calls = 0;
            owner.records = 0;
        }
        var result = try root.runtime.execute(std.testing.allocator, set.backend(), graph, &compiled, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, if (hidden) 512 else 1000), result.rows.len);
        for (result.rows, 0..) |row, index| try std.testing.expectEqual(@as(i64, 1023) - @as(i64, @intCast(index * (if (hidden) @as(usize, 2) else 1))), row[0].integer);
        try std.testing.expect(owners[0].calls + owners[1].calls < 80);
        try std.testing.expectEqual(result.stats.scanned_anchors, owners[0].records + owners[1].records);
    }
    fixture.hidden = false;
    set.ordered_item_cursors = true;
    set.ordered_identity_ties = true;
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var after: ?[]const u8 = null;
    var ordinal: usize = 0;
    const backend = set.backend();
    while (true) {
        const page = (try backend.vtable.scan_ordered.?(backend.ptr, arena.allocator(), "docs", &.{}, &.{.{ .property = "score", .descending = true }}, after, 17, 4096)).?;
        try std.testing.expectEqual(page.items.len, page.cursors.?.len);
        if (ordinal == 0) for (page.cursors.?, 0..) |position, index| {
            const resumed = (try backend.vtable.scan_ordered.?(backend.ptr, arena.allocator(), "docs", &.{}, &.{.{ .property = "score", .descending = true }}, position, 1, 4096)).?;
            try std.testing.expectEqual(@as(i64, 1022) - @as(i64, @intCast(index)), resumed.items[0].properties[0].value.integer);
        };
        for (page.items) |node| {
            try std.testing.expectEqual(@as(i64, 1023) - @as(i64, @intCast(ordinal)), node.properties[0].value.integer);
            ordinal += 1;
        }
        after = page.next orelse break;
    }
    try std.testing.expectEqual(@as(usize, 1024), ordinal);
}
