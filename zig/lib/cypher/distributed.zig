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
const fanout = @import("fanout.zig");
pub const KeyRange = struct { start: []const u8 = "", end: ?[]const u8 = null };
pub const Target = struct {
    /// Opt-in source admission is movable; retained local owner capabilities
    /// may still require their original controller thread.
    parallel: bool = true,
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
    /// Borrowed from this exact retained capability, never live catalog state.
    adjacency_routes: []const b.AdjacencyRoute = &.{},
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
    /// Opt in only when callbacks and their retained allocator support parallel
    /// calls for different targets. No owner is called twice in the same wave.
    fanout_io: ?std.Io = null,
    fanout_width: usize = 8,
    ptr: *anyopaque,
    checkpoint: *const fn (*anyopaque) anyerror!void,
    admit: *const fn (*anyopaque, Target) anyerror!void,
    capture: *const fn (*anyopaque, A, Target) anyerror!?Fence,
};
pub const ReadSet = struct {
    /// Local disjoint index partitions share this merge. Their tie order is
    /// primary identity order, and nested coordinators need per-item cursors.
    ordered_identity_ties: bool = false,
    /// Full catalog validated by the host; only proved data dependencies captured.
    partial_graph: bool = false,
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
        const slots = try a.alloc(AcquireSlot, targets.len);
        defer a.free(slots);
        for (slots, targets) |*slot, target| slot.* = .{ .target = target };
        var phase: AcquirePhase = .{ .a = a, .source = source, .slots = slots, .kind = .admit };
        defer phase.releaseFences();
        errdefer phase.closeSnapshots();
        try phase.run();
        // Capture never waits on a busy participant. Join the entire wave,
        // release every successful capture, and only then retry the cohort.
        for (0..4) |_| {
            phase.kind = .capture;
            try phase.run();
            const complete = for (slots) |slot| {
                if (slot.fence == null) break false;
            } else true;
            if (complete) break;
            phase.releaseFences();
        }
        for (slots) |slot| if (slot.fence == null) return error.Backpressured;
        phase.kind = .fork;
        try phase.run();
        phase.kind = .validate;
        try phase.run();
        const snapshots = try a.alloc(Snapshot, targets.len);
        errdefer a.free(snapshots);
        for (slots, snapshots) |slot, *snapshot| snapshot.* = slot.snapshot.?;
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
    const AcquireSlot = struct { target: Target, fence: ?Fence = null, snapshot: ?Snapshot = null, err: ?anyerror = null };
    const AcquirePhase = struct {
        a: A,
        source: Source,
        slots: []AcquireSlot,
        kind: enum { admit, capture, fork, validate },
        fn call(self: *@This(), index: usize) void {
            self.invoke(&self.slots[index]) catch |err| {
                self.slots[index].err = err;
            };
        }
        fn invoke(self: *@This(), slot: *AcquireSlot) !void {
            try self.source.checkpoint(self.source.ptr);
            switch (self.kind) {
                .admit => try self.source.admit(self.source.ptr, slot.target),
                .capture => slot.fence = try self.source.capture(self.source.ptr, self.a, slot.target),
                .fork => slot.snapshot = try slot.fence.?.fork(slot.fence.?.ptr, self.a),
                .validate => try slot.fence.?.validate(slot.fence.?.ptr),
            }
        }
        fn release(self: *@This(), index: usize) void {
            const slot = &self.slots[index];
            if (slot.fence) |fence| {
                fence.release(fence.ptr);
                slot.fence = null;
            }
        }
        fn close(self: *@This(), index: usize) void {
            const slot = &self.slots[index];
            if (slot.snapshot) |snapshot| {
                snapshot.close(snapshot.ptr);
                slot.snapshot = null;
            }
        }
        fn movable(self: *@This(), index: usize) bool {
            return self.kind == .admit or self.slots[index].target.parallel;
        }
        fn retainedMovable(self: *@This(), index: usize) bool {
            return self.slots[index].target.parallel;
        }
        fn releaseFences(self: *@This()) void {
            _ = fanout.selected(self.source.fanout_io, self.source.fanout_width, self.slots.len, self, release, retainedMovable, true);
        }
        fn closeSnapshots(self: *@This()) void {
            _ = fanout.selected(self.source.fanout_io, self.source.fanout_width, self.slots.len, self, close, retainedMovable, true);
        }
        fn run(self: *@This()) !void {
            for (self.slots) |*slot| slot.err = null;
            const canceled = fanout.selected(self.source.fanout_io, self.source.fanout_width, self.slots.len, self, call, movable, false);
            for (self.slots) |slot| if (slot.err) |err| return err;
            if (canceled) return error.Canceled;
            try self.source.checkpoint(self.source.ptr);
        }
    };
    fn freeTarget(a: A, target: Target) void {
        a.free(target.table);
        for (target.ranges) |range| {
            a.free(range.start);
            if (range.end) |end| a.free(end);
        }
        a.free(target.ranges);
    }
    fn closeSnapshot(self: *ReadSet, index: usize) void {
        const snapshot = self.snapshots[index];
        snapshot.close(snapshot.ptr);
    }
    fn movableSnapshot(self: *ReadSet, index: usize) bool {
        return self.targets[index].parallel;
    }
    pub fn deinit(self: *ReadSet) void {
        _ = fanout.selected(self.source.fanout_io, self.source.fanout_width, self.snapshots.len, self, closeSnapshot, movableSnapshot, true);
        for (self.targets) |target| freeTarget(self.a, target);
        self.a.free(self.targets);
        self.a.free(self.snapshots);
        self.a.free(self.independent_validations);
        self.* = undefined;
    }
    pub fn backend(self: *ReadSet) b.Backend {
        return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = checkpoint, .scan = scan, .scan_where = scanWhere, .probe_where = probeWhere, .count_nodes = countNodes, .expand_many = expandMany, .scan_ordered = scanOrdered, .estimate_where = estimateWhere, .population_upper_bound = populationUpperBound, .get = get, .get_many = getMany, .expand = expand, .expand_between = expandBetween, .search = search, .set_work = setWork, .set_read_fields = setReadFields } };
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
            if (!found and !self.partial_graph) return error.UndefinedGraphTable;
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
    const OrderedPosition = @import("cursor_trace.zig").Position;
    // Remote reads share scheduling, scratch ownership and quota admission.
    // A wave joins before its results are copied or any arena is reclaimed.
    const ReadJob = struct {
        owner: usize,
        operation: union(enum) {
            ordered: struct { table: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder, after: ?[]const u8 = null, limit: usize = 0 },
            get_many: []const v.Identity,
            count: struct { table: []const u8, input: b.CountInput },
            expand: struct { mapping: catalog.Relationship, identities: []const v.Identity, direction: ast.Direction, limit: usize, allowance: usize },
            probe: struct { table: []const u8, filters: []const b.PropertyFilter, limit: usize },
        },
        arena: std.heap.ArenaAllocator = undefined,
        meter: ?b.Meter = null,
        maximum: usize = 0,
        page: ?b.Page(*const v.Node) = null,
        nodes: []const ?*const v.Node = &.{},
        probe: ?b.Probe = null,
        count_result: ?b.CountResult = null,
        expansion: ?b.ExpansionBatch = null,
        err: ?anyerror = null,
        fn invoke(self: *@This(), read: *ReadSet) !void {
            const owner = read.snapshots[self.owner].backend;
            const a = self.arena.allocator();
            switch (self.operation) {
                .count => |input| {
                    var demand = input.input;
                    demand.max_examined = self.maximum;
                    if (self.meter) |meter| demand.max_work = @min(demand.max_work, meter.limit);
                    self.count_result = try owner.vtable.count_nodes.?(owner.ptr, a, input.table, demand);
                },
                .expand => |input| self.expansion = if (input.identities.len == 0) .{ .pages = &.{}, .examined = 0 } else try owner.vtable.expand_many.?(owner.ptr, a, input.mapping, input.identities, input.direction, input.limit, self.maximum),
                .probe => |input| self.probe = if (owner.vtable.probe_where) |callback| try callback(owner.ptr, a, input.table, input.filters, input.limit) else null,
                .ordered => |input| self.page = try (owner.vtable.scan_ordered orelse return error.CypherInvalidBackendPage)(owner.ptr, a, input.table, input.filters, input.order, input.after, input.limit, self.maximum),
                .get_many => |ids| self.nodes = if (owner.vtable.get_many) |batch| try batch(owner.ptr, a, ids) else blk: {
                    const nodes = try a.alloc(?*const v.Node, ids.len);
                    for (ids, nodes) |id, *node| node.* = try owner.vtable.get(owner.ptr, a, id);
                    break :blk nodes;
                },
            }
        }
    };
    const ReadWave = struct {
        read: *ReadSet,
        shared: fanout.Allocator,
        jobs: [16]ReadJob = undefined,
        count: usize = 0,
        initialized: usize = 0,
        examined: usize = 0,
        fn deinit(self: *@This()) void {
            for (self.jobs[0..self.initialized]) |*job| job.arena.deinit();
        }
        fn movable(self: *@This(), index: usize) bool {
            return self.read.targets[self.jobs[index].owner].parallel;
        }
        fn call(self: *@This(), index: usize) void {
            const job = &self.jobs[index];
            const owner = self.read.snapshots[job.owner].backend;
            if (job.meter) |*meter| owner.vtable.set_work.?(owner.ptr, meter.work());
            defer if (job.meter != null) owner.vtable.set_work.?(owner.ptr, self.read.work);
            job.invoke(self.read) catch |err| {
                job.err = err;
            };
        }
        fn usage(job: ReadJob) !usize {
            if (job.operation == .count) {
                if (job.count_result) |result| {
                    if (result.count > result.examined or result.examined > job.maximum) return error.CypherInvalidBackendPage;
                    return result.examined;
                }
                return if (job.err != null and (job.err.? == error.CypherAnchorBudgetExceeded or job.err.? == error.CypherStateBudgetExceeded)) job.maximum else 0;
            }
            if (job.operation == .expand) {
                if (job.expansion) |result| {
                    if (result.examined > job.maximum or result.pages.len != job.operation.expand.identities.len) return error.CypherInvalidBackendPage;
                    var admitted: usize = 0;
                    for (result.pages) |page| if (page) |value| {
                        const used = value.examined orelse value.items.len;
                        if (value.items.len > job.operation.expand.limit or used < value.items.len or used > result.examined - admitted) return error.CypherInvalidBackendPage;
                        admitted += used;
                    };
                    return result.examined;
                }
                return if (job.err != null and job.err.? == error.CypherStateBudgetExceeded) job.maximum else 0;
            }
            if (job.operation != .ordered) return 0;
            const input = job.operation.ordered;
            if (job.page) |page| {
                const used = page.examined orelse page.items.len;
                if (page.items.len > input.limit or used < page.items.len or used > job.maximum) return error.CypherInvalidBackendPage;
                return used;
            }
            // A failed private/custom read without candidate progress consumes
            // its reservation. Replaying it can never refund physical work.
            return if (job.err != null and input.limit != 0) job.maximum else 0;
        }
        fn run(self: *@This(), maximum: usize) !void {
            const remaining = if (self.read.work) |work| if (work.remaining) |left| left(work.ptr) else null else null;
            for (self.jobs[0..self.count]) |*job| {
                job.arena = .init(self.shared.allocator());
                self.initialized += 1;
                job.maximum = if (job.operation == .expand) @min(job.operation.expand.allowance, maximum / self.count) else maximum / self.count;
                if (remaining) |left| if (self.read.snapshots[job.owner].backend.vtable.set_work != null) {
                    job.meter = .{ .a = job.arena.allocator(), .backend = self.read.snapshots[job.owner].backend, .limit = if (self.count == 1) left else left / (self.count + 1) };
                };
            }
            const canceled = fanout.selected(self.read.source.fanout_io, self.read.source.fanout_width, self.count, self, call, movable, false);
            for (self.jobs[0..self.count]) |job| {
                if (job.meter) |meter| if (self.read.work) |work| try work.charge(work.ptr, meter.units);
                self.examined += try usage(job);
            }
            if (canceled) return error.Canceled;
            try checkpoint(self.read);
            for (self.jobs[0..self.count]) |job| if (job.err) |err| {
                if (err == error.OutOfMemory) {
                    if (self.shared.quota_exhausted.load(.acquire)) return error.CypherMemoryBudgetExceeded;
                    return err;
                }
                if (err != error.CypherStateBudgetExceeded and err != error.CypherAnchorBudgetExceeded) return err;
            };
            for (self.jobs[0..self.count]) |*job| if (job.err) |err| {
                if (self.count == 1) return err;
                if (self.read.work) |work| if (work.remaining) |left| if (left(work.ptr) == 0) return err;
                if (job.operation == .ordered and job.operation.ordered.limit != 0 and self.examined == maximum) return err;
                // Retry a skewed reservation once, after every started read has
                // joined and been charged. The unsuccessful scratch is dead.
                _ = job.arena.reset(.free_all);
                job.meter = null;
                job.err = null;
                job.maximum = if (job.operation == .expand) @min(job.operation.expand.allowance, maximum - self.examined) else maximum - self.examined;
                if (job.operation == .expand and job.maximum < job.operation.expand.identities.len) return err;
                job.invoke(self.read) catch |retry_error| {
                    job.err = retry_error;
                    self.examined += try usage(job.*);
                    return retry_error;
                };
                self.examined += try usage(job.*);
            };
        }
    };
    fn readWidth(self: *ReadSet, owners: []const usize, maximum: ?usize) usize {
        if (self.source.fanout_io == null or self.source.fanout_width <= 1) return 1;
        var width = @min(16, self.source.fanout_width, owners.len);
        if (self.work) |work| {
            const left = work.remaining orelse return 1;
            width = @min(width, left(work.ptr) / 256);
        }
        if (maximum) |limit| width = @min(width, limit / 64);
        // Local capabilities keep controller-thread affinity; custom backends
        // without isolated work counters also retain sequential admission.
        var eligible: usize = 0;
        for (owners[0..width]) |owner| {
            if (!self.targets[owner].parallel or (self.work != null and self.snapshots[owner].backend.vtable.set_work == null)) break;
            eligible += 1;
        }
        return @max(1, eligible);
    }
    fn readOwners(self: *ReadSet, a: A, table: ?[]const u8) ![]usize {
        var result: std.ArrayList(usize) = .empty;
        for ([_]bool{ true, false }) |movable| for (self.targets, 0..) |target, index| {
            if (table) |name| if (!std.mem.eql(u8, target.table, name)) continue;
            const eligible = target.parallel and (self.work == null or self.snapshots[index].backend.vtable.set_work != null);
            if (eligible == movable) try result.append(a, index);
        };
        return result.toOwnedSlice(a);
    }
    fn cloneOrderedPage(a: A, page: b.Page(*const v.Node), work: ?b.Work) !b.Page(*const v.Node) {
        const nodes = try a.alloc(*const v.Node, page.items.len);
        for (page.items, nodes) |node, *out| {
            if (work) |meter| try meter.value(meter.ptr, .{ .node = node });
            out.* = (try (v.Value{ .node = node }).clone(a)).node;
        }
        const cursors = if (page.cursors) |positions| blk: {
            const copy = try a.alloc(?[]const u8, positions.len);
            for (positions, copy) |position, *out| out.* = if (position) |bytes| try a.dupe(u8, bytes) else null;
            break :blk copy;
        } else null;
        return .{ .items = nodes, .cursors = cursors, .cursor_trace = if (page.cursor_trace) |trace| try trace.clone(a) else null, .next = if (page.next) |bytes| try a.dupe(u8, bytes) else null, .examined = page.examined, .stable_ties = page.stable_ties, .exact_keys = page.exact_keys, .encounter_owners = if (page.encounter_owners) |owners| try a.dupe(usize, owners) else null };
    }
    fn orderedProofs(self: *ReadSet, a: A, table: []const u8, filters: []const b.PropertyFilter, order: []const b.PropertyOrder) !?[]OrderedPosition {
        const owners = try self.readOwners(a, table);
        const positions = try a.alloc(OrderedPosition, owners.len);
        var lookup: [1024]usize = undefined;
        var index: usize = 0;
        for (self.targets, 0..) |target, owner| if (std.mem.eql(u8, target.table, table)) {
            lookup[owner] = index;
            index += 1;
        };
        var start: usize = 0;
        while (start < owners.len) {
            const width = self.readWidth(owners[start..], null);
            if (width == 1) {
                const owner = owners[start];
                const owner_backend = self.snapshots[owner].backend;
                const read = owner_backend.vtable.scan_ordered orelse return null;
                const proof = (try read(owner_backend.ptr, a, table, filters, order, null, 0, 0)) orelse return null;
                if (proof.items.len != 0 or (proof.examined orelse 0) != 0 or proof.next == null) return error.CypherInvalidBackendPage;
                positions[lookup[owner]] = .{ .exact_keys = proof.exact_keys, .coarse = !proof.stable_ties, .owner = owner, .after = proof.next, .batched = proof.cursors != null or proof.cursor_trace != null };
            } else {
                var wave: ReadWave = .{ .read = self, .shared = .{ .backing = b.reclaimingAllocator(a), .io = self.source.fanout_io.? }, .count = width };
                defer wave.deinit();
                for (wave.jobs[0..width], owners[start..][0..width]) |*job, owner| job.* = .{ .owner = owner, .operation = .{ .ordered = .{ .table = table, .filters = filters, .order = order } } };
                try wave.run(0);
                for (wave.jobs[0..width]) |job| {
                    const proof = job.page orelse return null;
                    if (proof.items.len != 0 or (proof.examined orelse 0) != 0 or proof.next == null) return error.CypherInvalidBackendPage;
                    positions[lookup[job.owner]] = .{ .exact_keys = proof.exact_keys, .coarse = !proof.stable_ties, .owner = job.owner, .after = try a.dupe(u8, proof.next.?), .batched = proof.cursors != null or proof.cursor_trace != null };
                }
            }
            start += width;
        }
        return positions;
    }
    const OrderedHead = struct { node: *const v.Node, after: ?[]const u8 };
    const OrderedSlot = struct {
        position: OrderedPosition,
        head: ?OrderedHead = null,
        buffer: []const *const v.Node = &.{},
        cursors: ?[]const ?[]const u8 = null,
        trace: ?*const b.CursorTrace = null,
        consumed: usize = 0,
        offset: usize = 0,
        next: ?[]const u8 = null,
        primed: bool = false,
        fn saved(self: OrderedSlot, a: A, work: ?b.Work) !OrderedPosition {
            var position = self.position;
            if (self.trace) |trace| if (self.consumed != 0) {
                position.after = if (self.consumed == self.buffer.len) self.next else try trace.materialize(a, self.consumed, work);
                position.finished = position.after == null;
            };
            return position;
        }
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
        fn prime(self: *@This(), temporary: A, slots: []OrderedSlot) !void {
            const read = self.read_set;
            const candidates = try read.readOwners(temporary, self.table);
            var lookup: [1024]usize = undefined;
            for (slots, 0..) |slot, index| lookup[slot.position.owner] = index;
            var active: usize = 0;
            for (candidates) |owner| if (!slots[lookup[owner]].position.finished) {
                candidates[active] = owner;
                active += 1;
            };
            const owners = candidates[0..active];
            var start: usize = 0;
            while (start < owners.len) {
                const width = read.readWidth(owners[start..], self.max_examined - self.examined);
                if (width == 1) {
                    try self.fetch(&slots[lookup[owners[start]]]);
                } else {
                    var wave: ReadWave = .{ .read = read, .shared = .{ .backing = b.reclaimingAllocator(self.a), .io = read.source.fanout_io.? }, .count = width };
                    defer wave.deinit();
                    for (wave.jobs[0..width], owners[start..][0..width]) |*job, owner| {
                        const slot = slots[lookup[owner]];
                        job.* = .{ .owner = owner, .operation = .{ .ordered = .{ .table = self.table, .filters = self.filters, .order = self.order, .after = slot.position.after, .limit = if (slot.position.finished) 0 else 1 } } };
                    }
                    try wave.run(self.max_examined - self.examined);
                    var returned: usize = 0;
                    for (wave.jobs[0..width]) |job| {
                        const page = job.page orelse return error.CypherInvalidBackendPage;
                        returned += page.examined orelse page.items.len;
                    }
                    // Failed reservations and retries remain physical reads.
                    self.examined += wave.examined - returned;
                    for (wave.jobs[0..width]) |job| {
                        const slot = &slots[lookup[job.owner]];
                        if (slot.position.finished) continue;
                        const page = try cloneOrderedPage(self.a, job.page.?, read.work);
                        slot.primed = true;
                        try self.receive(slot, page, 1);
                    }
                    for (wave.jobs[0..width]) |job| try self.fetch(&slots[lookup[job.owner]]);
                }
                start += width;
            }
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
                if (slot.trace != null) {
                    slot.position.after = slot.next;
                    slot.trace = null;
                    slot.consumed = 0;
                }
                if (self.examined == self.max_examined) return error.CypherAnchorBudgetExceeded;
                // Prime one head per owner before speculating. Refills scale
                // with the remaining output demand, not the entire table.
                const count = if (slot.position.batched and slot.primed) @max(1, @min(self.quantum, self.demand)) else 1;
                const page = (try read(owner.ptr, self.a, self.table, self.filters, self.order, slot.position.after, count, self.max_examined - self.examined)) orelse return error.CypherInvalidBackendPage;
                slot.primed = true;
                try self.receive(slot, page, count);
            }
            slot.head = null;
        }
        fn receive(self: *@This(), slot: *OrderedSlot, page: b.Page(*const v.Node), count: usize) !void {
            if ((page.stable_ties == slot.position.coarse or page.exact_keys != slot.position.exact_keys)) return error.CypherInvalidBackendPage;
            const examined = page.examined orelse page.items.len;
            if (page.items.len > count or examined < page.items.len or examined > self.max_examined - self.examined) return error.CypherInvalidBackendPage;
            self.examined += examined;
            if (page.items.len != 0) {
                for (page.items) |node| if (!std.mem.eql(u8, node.identity.table, self.table)) return error.CypherInvalidBackendPage;
                if (page.cursors) |positions| {
                    if (positions.len != page.items.len or (positions[positions.len - 1] == null and page.next != null)) return error.CypherInvalidBackendPage;
                    for (positions[0 .. positions.len - 1]) |position| if (position == null) return error.CypherInvalidBackendPage;
                } else if (page.cursor_trace == null and page.items.len > 1) return error.CypherInvalidBackendPage;
                if (page.cursor_trace) |trace| {
                    if (page.cursors != null) return error.CypherInvalidBackendPage;
                    try trace.validate(page.items.len);
                }
                slot.buffer = page.items;
                slot.cursors = page.cursors;
                slot.trace = page.cursor_trace;
                slot.consumed = 0;
                slot.offset = 0;
                slot.next = page.next;
                return;
            }
            if (page.next == null) slot.position.finished = true else {
                if (slot.position.after) |before| if (std.mem.eql(u8, before, page.next.?)) return error.CypherInvalidBackendPage;
                slot.position.after = page.next;
            }
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
            const cursors = if (page.cursor_trace == null) (if (page.cursors) |positions| blk: {
                if (positions.len != page.items.len) return error.CypherInvalidBackendPage;
                const saved = try a.alloc(?[]const u8, positions.len);
                for (positions, saved) |position_after, *out| out.* = if (position_after) |after| try std.json.Stringify.valueAlloc(a, &.{OrderedPosition{ .exact_keys = page.exact_keys, .coarse = !page.stable_ties, .owner = index, .after = after }}, .{}) else null;
                break :blk saved;
            } else null) else null;
            const trace = if (page.cursor_trace) |value| try value.wrap(a, .{ .owner = index, .exact_keys = page.exact_keys, .coarse = !page.stable_ties }) else null;
            return .{ .cursor_trace = trace, .exact_keys = page.exact_keys, .stable_ties = page.stable_ties, .encounter_owners = page.encounter_owners, .items = page.items, .cursors = cursors, .examined = page.examined, .next = if (page.next) |after| try std.json.Stringify.valueAlloc(a, &.{OrderedPosition{ .exact_keys = page.exact_keys, .coarse = !page.stable_ties, .owner = index, .after = after }}, .{}) else null };
        }
        var arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer arena.deinit();
        const temporary = arena.allocator();
        var positions: std.ArrayList(OrderedPosition) = .empty;
        if (encoded) |bytes| {
            const saved = try std.json.parseFromSliceLeaky([]const OrderedPosition, temporary, bytes, .{});
            try positions.appendSlice(temporary, saved);
        } else {
            try positions.appendSlice(temporary, (try self.orderedProofs(temporary, table, filters, order)) orelse return null);
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
        for (slots, positions.items) |*slot, position| slot.* = .{ .position = position };
        try merge.prime(temporary, slots);
        for (slots, 0..) |slot, index| {
            if (slot.head != null) try heap.append(temporary, index);
        }
        var parent = heap.items.len / 2;
        while (parent != 0) {
            parent -= 1;
            try merge.sift(&work, slots, heap.items, parent);
        }
        var output: std.ArrayList(*const v.Node) = .empty;
        var encounter_owners: std.ArrayList(usize) = .empty;
        const base: []OrderedPosition = if (self.ordered_item_cursors) try a.alloc(OrderedPosition, slots.len) else &.{};
        if (self.ordered_item_cursors) for (base, slots) |*position, slot| {
            position.* = try slot.saved(a, self.work);
            if (position.after) |bytes| position.after = try a.dupe(u8, bytes);
        };
        var steps: std.ArrayList(@import("cursor_trace.zig").Step) = .empty;
        while (output.items.len < limit and heap.items.len != 0) {
            const selected = heap.items[0];
            const slot = &slots[selected];
            try output.append(a, slot.head.?.node);
            if (!stable_ties) try encounter_owners.append(a, slot.position.owner);
            if (slot.trace != null) {
                slot.consumed = slot.offset;
                slot.position.finished = slot.offset == slot.buffer.len and slot.next == null;
            } else {
                slot.position.after = slot.head.?.after;
                slot.position.finished = slot.position.after == null;
            }
            slot.head = null;
            if (self.ordered_item_cursors) {
                const saved = try slot.saved(a, self.work);
                try OrderedMerge.charge(&merge, 1);
                try steps.append(a, .{ .slot = selected, .after = saved.after });
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
            position.* = try slot.saved(a, self.work);
            more = more or !position.finished;
        }
        try checkpoint(ptr);
        const continuation = if (more) try std.json.Stringify.valueAlloc(a, positions.items, .{}) else null;
        return .{ .exact_keys = exact_keys, .stable_ties = stable_ties, .encounter_owners = if (!stable_ties) encounter_owners.items else null, .items = try output.toOwnedSlice(a), .cursor_trace = if (self.ordered_item_cursors) try b.CursorTrace.create(a, .{ .base = base, .steps = steps.items }) else null, .examined = merge.examined, .next = continuation };
    }
    fn populationUpperBound(ptr: *anyopaque, a: A, table: []const u8) !?u64 {
        const self = cast(ptr);
        var total: u64 = 0;
        var found = false;
        for (self.targets, self.snapshots) |target, snapshot| {
            if (!std.mem.eql(u8, target.table, table)) continue;
            found = true;
            try checkpoint(ptr);
            const read = snapshot.backend.vtable.population_upper_bound orelse return null;
            const count = (try read(snapshot.backend.ptr, a, table)) orelse return null;
            total = std.math.add(u64, total, count) catch return null;
        }
        try checkpoint(ptr);
        return if (found) total else null;
    }
    fn probeUsage(result: b.Probe, limit: usize) !usize {
        const used = result.page.examined orelse result.page.items.len;
        if (used > limit + 1 or result.page.next != null or result.page.unfiltered or
            (result.complete and (used > limit or used < result.page.items.len)) or
            (!result.complete and result.page.items.len != 0)) return error.CypherInvalidBackendPage;
        return used;
    }
    fn countNodes(ptr: *anyopaque, a: A, table: []const u8, input: b.CountInput) !?b.CountResult {
        const self = cast(ptr);
        if (!@import("owner_count.zig").eligible(input)) return null;
        var inventory = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer inventory.deinit();
        var owners: std.ArrayList(usize) = .empty;
        for (self.targets, self.snapshots, 0..) |target, snapshot, owner| if (std.mem.eql(u8, target.table, table)) {
            if (snapshot.backend.vtable.count_nodes == null) return null;
            try owners.append(inventory.allocator(), owner);
        };
        var total: b.CountResult = .{ .count = 0, .examined = 0 };
        var start: usize = 0;
        while (start < owners.items.len) {
            try checkpoint(ptr);
            const remaining = input.max_examined - total.examined;
            const width = self.readWidth(owners.items[start..], remaining);
            var single: ReadJob = .{ .owner = owners.items[start], .operation = .{ .count = .{ .table = table, .input = input } }, .maximum = remaining };
            var wave: ReadWave = undefined;
            if (width > 1) wave = .{ .read = self, .shared = .{ .backing = b.reclaimingAllocator(a), .io = self.source.fanout_io.? }, .count = width };
            defer if (width > 1) wave.deinit() else single.arena.deinit();
            const jobs = if (width > 1) wave.jobs[0..width] else @as(*[1]ReadJob, @ptrCast(&single))[0..];
            if (width > 1) {
                for (jobs, owners.items[start..][0..width]) |*job, owner| job.* = .{ .owner = owner, .operation = .{ .count = .{ .table = table, .input = input } } };
                try wave.run(remaining);
                total.examined += wave.examined;
            } else {
                single.arena = .init(b.reclaimingAllocator(a));
                try single.invoke(self);
                total.examined += try ReadWave.usage(single);
            }
            for (jobs) |job| total.count += (job.count_result orelse return null).count;
            start += width;
        }
        try checkpoint(ptr);
        return if (owners.items.len != 0) total else null;
    }
    fn probeWhere(ptr: *anyopaque, a: A, table: []const u8, filters: []const b.PropertyFilter, limit: usize) !?b.Probe {
        const self = cast(ptr);
        if (limit == 0 or limit > 128) return null;
        var quota: @import("budget.zig") = .{ .backing = b.reclaimingAllocator(a), .limit = 2 << 20 };
        var collected = std.heap.ArenaAllocator.init(quota.allocator());
        defer collected.deinit();
        var inventory = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer inventory.deinit();
        var owners: std.ArrayList(usize) = .empty;
        // Preserve original owner encounter order, even when local owners split
        // remote waves. Every started probe joins and charges its isolated work.
        for (self.targets, 0..) |target, owner| if (std.mem.eql(u8, target.table, table)) try owners.append(inventory.allocator(), owner);
        var nodes: std.ArrayList(*const v.Node) = .empty;
        var examined: usize = 0;
        var found = false;
        var start: usize = 0;
        while (start < owners.items.len) {
            try checkpoint(ptr);
            const width = self.readWidth(owners.items[start..], null);
            const reservation = @max(1, (limit - examined) / width);
            var single: ReadJob = undefined;
            var wave: ReadWave = undefined;
            if (width > 1) wave = .{ .read = self, .shared = .{ .backing = b.reclaimingAllocator(a), .io = self.source.fanout_io.? }, .count = width };
            defer if (width > 1) wave.deinit() else single.arena.deinit();
            const jobs = if (width > 1) wave.jobs[0..width] else @as(*[1]ReadJob, @ptrCast(&single))[0..];
            for (jobs, owners.items[start..][0..width]) |*job, owner| job.* = .{ .owner = owner, .operation = .{ .probe = .{ .table = table, .filters = filters, .limit = reservation } } };
            if (width > 1) try wave.run(0) else {
                single.arena = .init(b.reclaimingAllocator(a));
                try single.invoke(self);
            }
            // Complete leaves reserve their exact populations before a skewed
            // leaf retries. Discarded advisory attempts charge work, not anchors.
            for (jobs) |job| {
                const result = job.probe orelse return if (found) b.Probe{ .page = .{ .items = &.{}, .examined = examined }, .complete = false } else null;
                found = true;
                const used = try probeUsage(result, reservation);
                if (result.complete) examined += used;
            }
            if (examined > limit) return .{ .page = .{ .items = &.{}, .examined = limit + 1 }, .complete = false };
            for (jobs) |*job| if (!job.probe.?.complete) {
                if (width == 1) return .{ .page = .{ .items = &.{}, .examined = @min(limit + 1, examined + try probeUsage(job.probe.?, reservation)) }, .complete = false };
                _ = job.arena.reset(.free_all);
                job.operation.probe.limit = @max(1, limit - examined);
                try job.invoke(self);
                const result = job.probe orelse return .{ .page = .{ .items = &.{}, .examined = examined }, .complete = false };
                const used = try probeUsage(result, job.operation.probe.limit);
                if (!result.complete or used > limit - examined) return .{ .page = .{ .items = &.{}, .examined = @min(limit + 1, examined + used) }, .complete = false };
                examined += used;
            };
            for (jobs) |job| for (job.probe.?.page.items) |node| {
                if (!std.mem.eql(u8, node.identity.table, table) or !self.targets[job.owner].owns(node.identity)) return error.CypherInvalidBackendPage;
                const owned = (v.Value{ .node = node }).clone(collected.allocator()) catch |err| {
                    if (err == error.OutOfMemory and quota.exhausted) return .{ .page = .{ .items = &.{}, .examined = examined }, .complete = false };
                    return err;
                };
                nodes.append(collected.allocator(), owned.node) catch |err| {
                    if (err == error.OutOfMemory and quota.exhausted) return .{ .page = .{ .items = &.{}, .examined = examined }, .complete = false };
                    return err;
                };
            };
            start += width;
        }
        if (!found) return null;
        try checkpoint(ptr);
        const output = try a.alloc(*const v.Node, nodes.items.len);
        for (nodes.items, output) |node, *out| out.* = (try (v.Value{ .node = node }).clone(a)).node;
        return .{ .page = .{ .items = output, .examined = examined }, .complete = true };
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
        var inventory = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer inventory.deinit();
        const candidates = try self.readOwners(inventory.allocator(), null);
        var found: usize = 0;
        for (candidates) |owner| {
            const matches = for (identities) |identity| {
                if (self.targets[owner].owns(identity)) break true;
            } else false;
            if (matches) {
                candidates[found] = owner;
                found += 1;
            }
        }
        const owners = candidates[0..found];
        var start: usize = 0;
        while (start < owners.len) {
            try checkpoint(ptr);
            var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
            defer scratch.deinit();
            const temporary = scratch.allocator();
            const width = self.readWidth(owners[start..], null);
            var selected: [16][]const v.Identity = undefined;
            var positions: [16][]const usize = undefined;
            for (owners[start..][0..width], 0..) |owner, offset| {
                var ids: std.ArrayList(v.Identity) = .empty;
                var indices: std.ArrayList(usize) = .empty;
                for (identities, 0..) |identity, index| if (self.targets[owner].owns(identity)) {
                    try ids.append(temporary, identity);
                    try indices.append(temporary, index);
                };
                selected[offset] = ids.items;
                positions[offset] = indices.items;
            }
            if (width == 1) {
                var job: ReadJob = .{ .owner = owners[start], .operation = .{ .get_many = selected[0] }, .arena = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a)) };
                defer job.arena.deinit();
                try job.invoke(self);
                try self.mergeNodes(a, result, selected[0], positions[0], job.nodes);
            } else {
                var wave: ReadWave = .{ .read = self, .shared = .{ .backing = b.reclaimingAllocator(a), .io = self.source.fanout_io.? }, .count = width };
                defer wave.deinit();
                for (wave.jobs[0..width], owners[start..][0..width], selected[0..width]) |*job, owner, ids| job.* = .{ .owner = owner, .operation = .{ .get_many = ids } };
                try wave.run(0);
                for (wave.jobs[0..width], selected[0..width], positions[0..width]) |job, ids, indices| try self.mergeNodes(a, result, ids, indices, job.nodes);
            }
            start += width;
        }
        try checkpoint(ptr);
        return result;
    }
    fn mergeNodes(self: *ReadSet, a: A, result: []?*const v.Node, ids: []const v.Identity, positions: []const usize, nodes: []const ?*const v.Node) !void {
        if (nodes.len != ids.len) return error.CypherInvalidBackendPage;
        for (nodes, ids, positions) |node, identity, index| if (node) |value| {
            if (!value.identity.eql(identity)) return error.CypherInvalidBackendPage;
            if (result[index] != null) return error.CypherOverlappingOwners;
            if (self.work) |work| try work.value(work.ptr, .{ .node = value });
            result[index] = (try (v.Value{ .node = value }).clone(a)).node;
        };
    }
    fn expandMany(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identities: []const v.Identity, direction: ast.Direction, limit: usize, maximum: usize) !?b.ExpansionBatch {
        const self = cast(ptr);
        if (identities.len == 0 or identities.len > 16 or limit == 0 or limit > 128) return null;
        const table = self.mappingSource(mapping) orelse return error.UndefinedGraphTable;
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const temporary = scratch.allocator();
        var owners: std.ArrayList(usize) = .empty;
        for (self.targets, self.snapshots, 0..) |target, snapshot, owner| if (std.mem.eql(u8, target.table, table)) {
            const needed = for (identities) |identity| {
                if (self.adjacencyOwner(owner, mapping, identity, null, direction)) break true;
            } else false;
            if (!needed) continue;
            if (snapshot.backend.vtable.expand_many == null) return null;
            try owners.append(temporary, owner);
        };
        if (owners.items.len != 0 and maximum / owners.items.len < identities.len) return null;
        const allowance = if (owners.items.len == 0) 0 else @min(4096, maximum / owners.items.len);
        const items = try temporary.alloc(std.ArrayList(*const v.Relationship), identities.len);
        const pages = try a.alloc(?b.Page(*const v.Relationship), identities.len);
        const finished = try temporary.alloc(bool, identities.len);
        for (items) |*list| list.* = .empty;
        for (pages) |*page| page.* = .{ .items = &.{} };
        @memset(finished, false);
        var examined: usize = 0;
        var bytes: usize = 0;
        // Group by immutable owner, retaining its original encounter position.
        // First-page batches replace V separate RPCs with one per owner. Each
        // node stops at its first unfinished owner; cursors retain native tokens.
        var start: usize = 0;
        while (start < owners.items.len) {
            const pending = for (finished) |done| {
                if (!done) break true;
            } else false;
            if (!pending) break;
            const width = self.readWidth(owners.items[start..], null);
            var selected: [16][16]v.Identity = undefined;
            var positions: [16][16]usize = undefined;
            var counts: [16]usize = @splat(0);
            var single: ReadJob = undefined;
            var wave: ReadWave = undefined;
            if (width > 1) wave = .{ .read = self, .shared = .{ .backing = b.reclaimingAllocator(a), .io = self.source.fanout_io.? }, .count = width };
            defer if (width > 1) wave.deinit() else single.arena.deinit();
            const jobs = if (width > 1) wave.jobs[0..width] else @as(*[1]ReadJob, @ptrCast(&single))[0..];
            for (jobs, owners.items[start..][0..width], 0..) |*job, owner, slot| {
                for (identities, 0..) |identity, index| if (!finished[index] and self.adjacencyOwner(owner, mapping, identity, null, direction)) {
                    selected[slot][counts[slot]] = identity;
                    positions[slot][counts[slot]] = index;
                    counts[slot] += 1;
                };
                job.* = .{ .owner = owner, .maximum = allowance, .operation = .{ .expand = .{ .mapping = mapping, .identities = selected[slot][0..counts[slot]], .direction = direction, .limit = limit, .allowance = allowance } } };
            }
            try checkpoint(ptr);
            if (width > 1) {
                try wave.run(maximum - examined);
                examined += wave.examined;
            } else {
                single.maximum = @min(allowance, maximum - examined);
                single.arena = .init(b.reclaimingAllocator(a));
                try single.invoke(self);
                examined += try ReadWave.usage(single);
            }
            for (jobs, 0..) |job, slot| {
                const owner = job.owner;
                const count = counts[slot];
                const batch = job.expansion orelse {
                    for (positions[slot][0..count]) |index| if (!finished[index]) {
                        pages[index] = null;
                        finished[index] = true;
                    };
                    continue;
                };
                if (batch.pages.len != count or batch.examined > allowance) return error.CypherInvalidBackendPage;
                for (batch.pages, positions[slot][0..count]) |page, index| {
                    if (finished[index]) continue;
                    const value = page orelse {
                        pages[index] = null;
                        finished[index] = true;
                        continue;
                    };
                    if (value.items.len > limit or (value.examined orelse value.items.len) < value.items.len) return error.CypherInvalidBackendPage;
                    const needed = try @import("expansion_batch.zig").pageBytes(value);
                    if (needed > @import("expansion_batch.zig").max_bytes - bytes) {
                        pages[index] = null;
                        finished[index] = true;
                        continue;
                    }
                    const copied = try @import("expansion_batch.zig").clone(a, value);
                    bytes += needed;
                    try items[index].appendSlice(a, copied.items);
                    const continuation = try self.adjacencyNext(a, mapping, identities[index], null, direction, .{ .owner = owner }, copied.next);
                    pages[index] = .{ .items = items[index].items, .next = continuation };
                    finished[index] = copied.items.len != 0 or copied.next != null or continuation == null;
                }
            }
            start += width;
        }
        try checkpoint(ptr);
        return .{ .pages = pages, .examined = examined };
    }
    fn adjacencyOwner(self: *ReadSet, owner: usize, mapping: catalog.Relationship, identity: v.Identity, peer: ?v.Identity, direction: ast.Direction) bool {
        const graph = self.graph orelse return false;
        const source = graph.label(mapping.source_label).?.table;
        const target = graph.label(mapping.target_label).?.table;
        const shard = self.targets[owner];
        if (!std.mem.eql(u8, shard.table, source)) return false;
        const outgoing = direction != .in and std.mem.eql(u8, identity.table, source) and (peer == null or std.mem.eql(u8, peer.?.table, target));
        const incoming = direction != .out and std.mem.eql(u8, identity.table, target) and (peer == null or std.mem.eql(u8, peer.?.table, source));
        if (!outgoing and !incoming) return false;
        for (self.snapshots[owner].adjacency_routes) |route| if (std.mem.eql(u8, route.index, mapping.index)) {
            const out_possible = outgoing and (if (route.outgoing) |range| range.contains(identity.key) else true);
            const in_possible = incoming and (if (route.incoming) |range| range.contains(identity.key) else true);
            if (!out_possible and !in_possible) return false;
            // Physical endpoint envelopes supersede document placement. A
            // producer can emit a source outside its primary document range.
            return true;
        };
        // Every source owner must certify placement before any can be pruned.
        // An artifact producer may be on the very owner pruning would omit.
        for (self.targets, self.snapshots) |candidate, snapshot| if (std.mem.eql(u8, candidate.table, source) and !snapshot.backend.vtable.source_owned_adjacency) return true;
        return (outgoing and shard.owns(identity)) or (incoming and (peer == null or shard.owns(peer.?)));
    }
    fn adjacencyPosition(self: *ReadSet, a: A, mapping: catalog.Relationship, identity: v.Identity, peer: ?v.Identity, direction: ast.Direction, encoded: ?[]const u8, start: usize) !?Cursor {
        if (encoded) |bytes| {
            const position = try self.cursor(a, self.mappingSource(mapping) orelse return error.UndefinedGraphTable, bytes);
            if (!self.adjacencyOwner(position.owner, mapping, identity, peer, direction)) return error.CypherInvalidBackendCursor;
            return position;
        }
        for (start..self.targets.len) |owner| if (self.adjacencyOwner(owner, mapping, identity, peer, direction)) return .{ .owner = owner };
        return null;
    }
    fn adjacencyNext(self: *ReadSet, a: A, mapping: catalog.Relationship, identity: v.Identity, peer: ?v.Identity, direction: ast.Direction, position: Cursor, after: ?[]const u8) !?[]const u8 {
        const next_position = if (after) |bytes| Cursor{ .owner = position.owner, .after = bytes } else (try self.adjacencyPosition(a, mapping, identity, peer, direction, null, position.owner + 1)) orelse return null;
        return try std.json.Stringify.valueAlloc(a, next_position, .{});
    }
    fn expand(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, direction: ast.Direction, encoded: ?[]const u8, limit: usize) !b.Page(*const v.Relationship) {
        const self = cast(ptr);
        // Physical adjacency, including incoming adjacency, belongs to source
        // owners. Resolve the label from the statement-bound graph at bind.
        try checkpoint(ptr);
        const position = (try self.adjacencyPosition(a, mapping, identity, null, direction, encoded, 0)) orelse return .{ .items = &.{} };
        const owner = self.snapshots[position.owner].backend;
        const page = try owner.vtable.expand(owner.ptr, a, mapping, identity, direction, position.after, limit);
        try checkpoint(ptr);
        return .{ .items = page.items, .examined = page.examined, .next = try self.adjacencyNext(a, mapping, identity, null, direction, position, page.next) };
    }
    fn expandBetween(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, peer: v.Identity, direction: ast.Direction, filters: []const b.PropertyFilter, encoded: ?[]const u8, limit: usize, max_examined: usize) !b.Page(*const v.Relationship) {
        const self = cast(ptr);
        try checkpoint(ptr);
        const position = (try self.adjacencyPosition(a, mapping, identity, peer, direction, encoded, 0)) orelse return .{ .items = &.{} };
        const owner = self.snapshots[position.owner].backend;
        const page = if (owner.vtable.expand_between) |between|
            try between(owner.ptr, a, mapping, identity, peer, direction, filters, position.after, limit, max_examined)
        else
            try owner.vtable.expand(owner.ptr, a, mapping, identity, direction, position.after, limit);
        try checkpoint(ptr);
        return .{ .items = page.items, .examined = page.examined, .next = try self.adjacencyNext(a, mapping, identity, peer, direction, position, page.next) };
    }
    const SearchMerge = struct {
        read: *ReadSet,
        a: A,
        limit: usize,
        steps: usize = 0,
        best: std.ArrayList(b.Seed) = .empty,
        // Backward-shift index deletion avoids tombstone accumulation during
        // continuous top-k eviction; cached hashes also avoid rehashing keys.
        identities: std.ArrayHashMapUnmanaged(v.Identity, ?*std.heap.ArenaAllocator, IdentityContext, true) = .empty,
        const IdentityContext = struct {
            pub fn hash(_: @This(), identity: v.Identity) u32 {
                return @truncate((v.Identity.Context{}).hash(identity));
            }
            pub fn eql(_: @This(), left: v.Identity, right: v.Identity, _: usize) bool {
                return left.eql(right);
            }
        };
        fn charge(self: *@This(), amount: usize) !void {
            if (self.read.work) |work| try work.charge(work.ptr, amount);
            const before = self.steps;
            self.steps +|= amount;
            if (before / 64 != self.steps / 64) try checkpoint(self.read);
        }
        fn better(self: *@This(), left: b.Seed, right: b.Seed) !bool {
            try self.charge(1);
            if (left.score == right.score) try self.charge((left.node.identity.key.len +| right.node.identity.key.len) / 64 +| 1);
            return less({}, left, right);
        }
        fn sift(self: *@This(), length: usize) !void {
            var parent: usize = 0;
            while (parent * 2 + 1 < length) {
                var child = parent * 2 + 1;
                if (child + 1 < length and try self.better(self.best.items[child], self.best.items[child + 1])) child += 1;
                if (!try self.better(self.best.items[parent], self.best.items[child])) break;
                std.mem.swap(b.Seed, &self.best.items[parent], &self.best.items[child]);
                parent = child;
            }
        }
        fn deinit(self: *@This()) void {
            for (self.identities.values()) |arena| if (arena) |owned| {
                owned.deinit();
                b.reclaimingAllocator(self.a).destroy(owned);
            };
            self.identities.deinit(self.a);
            self.best.deinit(self.a);
        }
        fn retain(self: *@This(), seed: b.Seed, own: bool) !b.Seed {
            if (!own) {
                try self.identities.put(self.a, seed.node.identity, null);
                return seed;
            }
            const allocator = b.reclaimingAllocator(self.a);
            const arena = try allocator.create(std.heap.ArenaAllocator);
            errdefer allocator.destroy(arena);
            arena.* = .init(allocator);
            errdefer arena.deinit();
            if (self.read.work) |work| try work.value(work.ptr, .{ .node = seed.node });
            const copy = try (v.Value{ .node = seed.node }).clone(arena.allocator());
            const entry = try self.identities.getOrPut(self.a, copy.node.identity);
            entry.key_ptr.* = copy.node.identity;
            entry.value_ptr.* = arena;
            return .{ .node = copy.node, .score = seed.score };
        }
        fn promote(self: *@This()) !void {
            // All worker pages remain alive until the wave joins. Copy only
            // the surviving top-k, once per wave, into reclaimable heap slots.
            for (self.best.items) |*seed| if (self.identities.get(seed.node.identity).? == null) {
                seed.* = try self.retain(seed.*, true);
            };
        }
        fn add(self: *@This(), seed: b.Seed) !void {
            const identity = seed.node.identity;
            // Hashing and key comparisons are physical coordinator work too.
            try self.charge(1 +| 3 *| ((identity.table.len +| identity.key.len) / 64 +| 1));
            if (self.identities.contains(identity)) return error.CypherOverlappingOwners;
            if (self.best.items.len < self.limit) {
                const retained = try self.retain(seed, false);
                try self.best.append(self.a, retained);
                var child = self.best.items.len - 1;
                while (child != 0) {
                    const parent = (child - 1) / 2;
                    if (!try self.better(self.best.items[parent], self.best.items[child])) break;
                    std.mem.swap(b.Seed, &self.best.items[parent], &self.best.items[child]);
                    child = parent;
                }
            } else if (try self.better(seed, self.best.items[0])) {
                const old = self.best.items[0].node.identity;
                try self.charge((old.table.len +| old.key.len) / 64 +| 1);
                const arena = self.identities.get(old).?;
                _ = self.identities.swapRemove(old);
                if (arena) |owned| {
                    owned.deinit();
                    b.reclaimingAllocator(self.a).destroy(owned);
                }
                // An allocation failure aborts the query; no dangling heap key
                // is consulted during cleanup.
                self.best.items[0] = try self.retain(seed, false);
                try self.sift(self.best.items.len);
            }
        }
        fn finish(self: *@This()) ![]const b.Seed {
            // Fallible heap sort preserves score/key order while metering every
            // comparison. The retained identity set never exceeds top-k size.
            var length = self.best.items.len;
            while (length > 1) {
                length -= 1;
                std.mem.swap(b.Seed, &self.best.items[0], &self.best.items[length]);
                try self.sift(length);
            }
            for (self.best.items) |*seed| if (self.identities.get(seed.node.identity).? != null) {
                if (self.read.work) |work| try work.value(work.ptr, .{ .node = seed.node });
                seed.node = (try (v.Value{ .node = seed.node }).clone(self.a)).node;
            };
            return self.best.toOwnedSlice(self.a);
        }
    };
    const SearchSlot = struct {
        owner: b.Backend,
        parent_work: ?b.Work,
        arena: std.heap.ArenaAllocator,
        meter: b.Meter = undefined,
        input: b.SearchInput,
        table: []const u8,
        usage: b.SearchUsage = .{},
        page: ?b.SearchPage = null,
        err: ?anyerror = null,
        fn run(slots: []SearchSlot, index: usize) void {
            const self = &slots[index];
            self.owner.vtable.set_work.?(self.owner.ptr, self.meter.work());
            defer self.owner.vtable.set_work.?(self.owner.ptr, self.parent_work);
            var input = self.input;
            input.usage = &self.usage;
            self.page = self.owner.vtable.search.?(self.owner.ptr, self.arena.allocator(), self.table, input) catch |err| {
                self.err = err;
                return;
            };
        }
    };
    fn absorb(merge: *SearchMerge, page: b.SearchPage, table: []const u8) !void {
        if (page.seeds.len > merge.limit) return error.CypherInvalidSearchResult;
        for (page.seeds) |seed| {
            if (!std.math.isFinite(seed.score) or !std.mem.eql(u8, seed.node.identity.table, table)) return error.CypherInvalidSearchResult;
            try merge.add(seed);
        }
    }
    fn searchOne(self: *ReadSet, a: A, owner: b.Backend, table: []const u8, input: b.SearchInput, merge: *SearchMerge) !b.SearchPage {
        _ = self;
        var scratch = std.heap.ArenaAllocator.init(b.reclaimingAllocator(a));
        defer scratch.deinit();
        const callback = owner.vtable.search orelse return error.CypherSearchUnavailable;
        const page = try callback(owner.ptr, scratch.allocator(), table, input);
        if (page.examined > input.max_examined) return error.CypherAnchorBudgetExceeded;
        try absorb(merge, page, table);
        try merge.promote();
        return .{ .seeds = &.{}, .examined = page.examined, .approximate = page.approximate };
    }
    fn searchConcurrent(self: *ReadSet, a: A, table: []const u8, input: b.SearchInput, merge: *SearchMerge) !b.SearchPage {
        const io = self.source.fanout_io.?;
        var matching: std.ArrayList(usize) = .empty;
        defer matching.deinit(a);
        // Preserve local capability affinity and collect remote owners into
        // contiguous waves even when routing interleaves local/remote ranges.
        for ([_]bool{ false, true }) |movable| for (self.targets, 0..) |target, index| {
            if (target.parallel == movable and std.mem.eql(u8, target.table, table)) try matching.append(a, index);
        };
        if (matching.items.len == 0) return error.UndefinedGraphTable;
        var examined: usize = 0;
        defer if (input.usage) |usage| {
            usage.examined = examined;
        };
        var approximate = false;
        var start: usize = 0;
        while (start < matching.items.len) {
            try checkpoint(self);
            const work_left = if (self.work) |work| work.remaining.?(work.ptr) else 1_000_000;
            // Small ceilings keep ordinary sequential admission. Speculative
            // waves reserve disjoint ceilings and leave room for merging.
            const width = @max(1, @min(self.source.fanout_width, 16, matching.items.len - start, work_left / @max(1, merge.limit *| 64), (input.max_examined -| examined) / merge.limit));
            var count: usize = 0;
            while (count < width) : (count += 1) {
                if (!self.targets[matching.items[start + count]].parallel) break;
                const owner = self.snapshots[matching.items[start + count]].backend;
                if (owner.vtable.search == null) return error.CypherSearchUnavailable;
                if (owner.vtable.set_work == null) break;
            }
            if (count <= 1) {
                const owner = self.snapshots[matching.items[start]].backend;
                var demand = input;
                demand.max_examined = input.max_examined -| examined;
                var usage: b.SearchUsage = .{};
                demand.usage = &usage;
                const page = self.searchOne(a, owner, table, demand, merge) catch |err| {
                    examined += @min(demand.max_examined, usage.examined orelse demand.max_examined);
                    return err;
                };
                if (page.examined > demand.max_examined) return error.CypherAnchorBudgetExceeded;
                examined += page.examined;
                approximate = approximate or page.approximate;
                start += 1;
                continue;
            }
            // Worker arenas and counters are independent. Only allocator
            // operations cross the shared memory-accounting lock.
            var shared: fanout.Allocator = .{ .backing = b.reclaimingAllocator(a), .io = io };
            var storage: [16]SearchSlot = undefined;
            const slots = storage[0..count];
            for (slots, matching.items[start..][0..count]) |*slot, index| {
                var demand = input;
                demand.max_examined = (input.max_examined - examined) / count;
                slot.* = .{ .owner = self.snapshots[index].backend, .parent_work = self.work, .arena = .init(shared.allocator()), .input = demand, .table = table };
                slot.meter = .{ .a = slot.arena.allocator(), .backend = slot.owner, .limit = work_left / (count + 1) };
            }
            defer for (slots) |*slot| slot.arena.deinit();
            const canceled = fanout.run(io, count, count, slots, SearchSlot.run);
            // Charge every completed attempt before a retry or result merge.
            // Budget failures carry actual work and native candidate progress;
            // unknown custom backends conservatively consume their reservation.
            for (slots) |slot| {
                if (self.work) |work| try work.charge(work.ptr, slot.meter.units);
                const used = if (slot.page) |page| page.examined else slot.usage.examined orelse slot.input.max_examined;
                if (used > slot.input.max_examined) return error.CypherAnchorBudgetExceeded;
                examined += used;
            }
            if (canceled) return error.Canceled;
            try checkpoint(self);
            for (slots) |slot| if (slot.err) |err| {
                if (err != error.CypherStateBudgetExceeded and err != error.CypherAnchorBudgetExceeded and err != error.OutOfMemory) return err;
            };
            for (slots) |slot| if (slot.err) |err| {
                if (err == error.OutOfMemory) {
                    if (shared.quota_exhausted.load(.acquire)) return error.CypherMemoryBudgetExceeded;
                    return error.OutOfMemory;
                }
            };
            for (slots) |*slot| {
                if (slot.page) |page| {
                    approximate = approximate or page.approximate;
                    try absorb(merge, page, table);
                    continue;
                }
                const err = slot.err.?;
                if (err != error.CypherStateBudgetExceeded and err != error.CypherAnchorBudgetExceeded) return err;
                if (self.work) |work| if (work.remaining.?(work.ptr) == 0) return err;
                if (examined == input.max_examined and err == error.CypherAnchorBudgetExceeded) return err;
                // Reuse unused reservations after skewed owner work. The
                // aborted attempt stays charged; there is at most one retry.
                var demand = input;
                demand.max_examined = input.max_examined - examined;
                var usage: b.SearchUsage = .{};
                demand.usage = &usage;
                const page = slot.owner.vtable.search.?(slot.owner.ptr, slot.arena.allocator(), table, demand) catch |retry_error| {
                    examined += @min(demand.max_examined, usage.examined orelse demand.max_examined);
                    return retry_error;
                };
                if (page.examined > demand.max_examined) return error.CypherAnchorBudgetExceeded;
                examined += page.examined;
                approximate = approximate or page.approximate;
                try absorb(merge, page, table);
            }
            try merge.promote();
            start += count;
        }
        try checkpoint(self);
        return .{ .seeds = try merge.finish(), .examined = examined, .approximate = approximate or matching.items.len > 1 };
    }
    fn search(ptr: *anyopaque, a: A, table: []const u8, input: b.SearchInput) !b.SearchPage {
        const self = cast(ptr);
        try checkpoint(ptr);
        const parameters = input.parameters;
        if (parameters != .map) return error.CypherTypeMismatch;
        const requested = parameters.property("limit");
        const limit: usize = if (requested == .null) 10 else if (requested == .integer and requested.integer > 0 and requested.integer <= 1024) @intCast(requested.integer) else return error.CypherInvalidSearchLimit;
        var merge: SearchMerge = .{ .read = self, .a = a, .limit = limit };
        defer merge.deinit();
        if (self.source.fanout_io != null and self.source.fanout_width > 1 and (self.work == null or self.work.?.remaining != null)) {
            var movable: usize = 0;
            for (self.targets) |target| if (target.parallel and std.mem.eql(u8, target.table, table)) {
                movable += 1;
            };
            if (movable > 1) return self.searchConcurrent(a, table, input, &merge);
        }
        var found = false;
        var examined: usize = 0;
        defer if (input.usage) |usage| {
            usage.examined = examined;
        };
        var approximate = false;
        var searched_owners: usize = 0;
        for (self.targets, self.snapshots) |target, snapshot| {
            if (!std.mem.eql(u8, target.table, table)) continue;
            found = true;
            searched_owners += 1;
            try checkpoint(ptr);
            var demand = input;
            demand.max_examined = input.max_examined -| examined;
            var usage: b.SearchUsage = .{};
            demand.usage = &usage;
            const page = self.searchOne(a, snapshot.backend, table, demand, &merge) catch |err| {
                examined += @min(demand.max_examined, usage.examined orelse demand.max_examined);
                return err;
            };
            if (page.examined > demand.max_examined) return error.CypherAnchorBudgetExceeded;
            examined += page.examined;
            approximate = approximate or page.approximate;
        }
        if (!found) return error.UndefinedGraphTable;
        try checkpoint(ptr);
        // Text and hybrid scores currently use owner-local corpus statistics.
        // Report this explicitly until a global-statistics scorer is available.
        return .{ .seeds = try merge.finish(), .examined = examined, .approximate = approximate or searched_owners > 1 };
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
        try std.testing.expectEqual(page.items.len, page.cursor_trace.?.steps.len);
        if (ordinal == 0) for (page.items, 0..) |_, index| {
            const position = try page.cursor_trace.?.materialize(arena.allocator(), index + 1, @as(?b.Work, null));
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

test "distributed search heap meters ranking hashes sorting and revocation" {
    const Fixture = struct {
        units: usize = 0,
        limit: usize = std.math.maxInt(usize),
        revoked: bool = false,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.revoked) return error.Forbidden;
        }
        fn charge(ptr: *anyopaque, amount: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.units += amount;
            if (self.units > self.limit) return error.CypherStateBudgetExceeded;
        }
        fn value(_: *anyopaque, _: v.Value) !void {}
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    var read: ReadSet = .{ .a = a, .targets = &.{}, .snapshots = &.{}, .independent_validations = &.{}, .source = .{ .ptr = &fixture, .checkpoint = Fixture.checkpoint, .admit = undefined, .capture = undefined }, .work = .{ .ptr = &fixture, .charge = Fixture.charge, .value = Fixture.value } };
    var merge: ReadSet.SearchMerge = .{ .read = &read, .a = a, .limit = 17 };
    defer merge.deinit();
    var reference: std.ArrayList(b.Seed) = .empty;
    for (0..1024) |ordinal| {
        const index = ordinal * 359 % 1024;
        const node = try a.create(v.Node);
        node.* = .{ .identity = .{ .table = "docs", .key = try std.fmt.allocPrint(a, "n{d:0>4}", .{index}) } };
        const seed: b.Seed = .{ .node = node, .score = @floatFromInt(index % 5) };
        try merge.add(seed);
        try reference.append(a, seed);
        try std.testing.expect(merge.identities.count() <= 17);
    }
    std.mem.sort(b.Seed, reference.items, {}, ReadSet.less);
    const result = try merge.finish();
    for (result, reference.items[0..17]) |actual, expected| try std.testing.expect(actual.node.identity.eql(expected.node.identity));
    try std.testing.expect(fixture.units > 1024);
    try std.testing.expect(fixture.units < 100000);
    var duplicate: ReadSet.SearchMerge = .{ .read = &read, .a = a, .limit = 1 };
    try duplicate.add(result[0]);
    try std.testing.expectError(error.CypherOverlappingOwners, duplicate.add(result[0]));
    fixture.limit = fixture.units;
    try std.testing.expectError(error.CypherStateBudgetExceeded, duplicate.add(result[1]));
    fixture.limit = std.math.maxInt(usize);
    fixture.revoked = true;
    try std.testing.expectError(error.Forbidden, duplicate.charge(64));
}

test "distributed population bounds sum only matching retained owners and reject overflow" {
    const Owner = struct {
        count: ?u64,
        fn checkpoint(_: *anyopaque) !void {}
        fn population(ptr: *anyopaque, _: A, _: []const u8) !?u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.count;
        }
    };
    var owners = [_]Owner{ .{ .count = 5000 }, .{ .count = 7000 }, .{ .count = 99 } };
    const targets = [_]Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 }, .{ .table = "other", .table_id = 2, .group_id = 3 } };
    var snapshots: [3]Snapshot = undefined;
    for (&owners, &snapshots) |*owner, *snapshot| snapshot.* = .{ .ptr = owner, .validate = Owner.checkpoint, .close = undefined, .backend = .{ .ptr = owner, .vtable = &.{ .bind = undefined, .checkpoint = Owner.checkpoint, .scan = undefined, .get = undefined, .expand = undefined, .population_upper_bound = Owner.population } } };
    var read: ReadSet = .{ .a = std.testing.allocator, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &owners, .checkpoint = Owner.checkpoint, .admit = undefined, .capture = undefined } };
    const backend = read.backend();
    const population = backend.vtable.population_upper_bound.?;
    try std.testing.expectEqual(@as(?u64, 12000), try population(backend.ptr, std.testing.allocator, "docs"));
    try std.testing.expectEqual(@as(?u64, 99), try population(backend.ptr, std.testing.allocator, "other"));
    try std.testing.expectEqual(@as(?u64, null), try population(backend.ptr, std.testing.allocator, "missing"));
    owners[1].count = std.math.maxInt(u64);
    try std.testing.expectEqual(@as(?u64, null), try population(backend.ptr, std.testing.allocator, "docs"));
    owners[1].count = null;
    try std.testing.expectEqual(@as(?u64, null), try population(backend.ptr, std.testing.allocator, "docs"));
}

test "sparse membership retains per-owner windows across indexed and primary shards" {
    const rpc = @import("rpc.zig");
    const Fixture = struct {
        primary: bool,
        fn check(_: *anyopaque) !void {}
        fn close(_: *anyopaque) void {}
        fn bind(_: *anyopaque, _: A, _: catalog.Graph) !void {}
        fn filtered(ptr: *anyopaque, a: A, _: []const u8, filters: []const b.PropertyFilter, after: ?[]const u8, maximum: usize) !b.Page(*const v.Node) {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const values = filters[0].value.list;
            const offset = if (after) |cursor| try std.fmt.parseUnsigned(usize, cursor, 10) else 0;
            try std.testing.expect(values.len == (if (offset == 0) @as(usize, 400) else @min(128, 400 - offset)));
            const count = if (self.primary) 1 else @min(maximum, @min(128, values.len));
            try std.testing.expect(count <= maximum);
            const nodes = try a.alloc(*const v.Node, count);
            for (nodes, 0..) |*out, i| {
                const node = try a.create(v.Node);
                node.* = .{ .identity = .{ .table = "docs", .key = if (self.primary) "primary" else try std.fmt.allocPrint(a, "indexed-{d}", .{values[i].integer}) }, .labels = &.{"D"}, .properties = try a.dupe(v.Entry, &.{.{ .name = "id", .value = if (self.primary) .{ .integer = 42 } else values[i] }}) };
                out.* = node;
            }
            return .{
                .items = nodes,
                .examined = count,
                .unfiltered = self.primary,
                .next = if (!self.primary and offset + count < 400) try std.fmt.allocPrint(a, "{d}", .{offset + count}) else null,
                .seek_window = if (!self.primary and offset + count < 400) .{ .filter = 0, .offset = offset + count } else null,
            };
        }
        fn backend(self: *@This()) b.Backend {
            return .{ .ptr = self, .vtable = &.{ .bind = bind, .checkpoint = check, .scan = undefined, .scan_where = filtered, .get = undefined, .expand = undefined } };
        }
        fn invoke(ptr: *anyopaque, a: A, body: []const u8) ![]u8 {
            const endpoint: *rpc.Owner = @ptrCast(@alignCast(ptr));
            return endpoint.execute(a, body);
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const graph: catalog.Graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{.{ .name = "D", .table = "docs" }}, .relationships = &.{} };
    var owners = [_]Fixture{ .{ .primary = false }, .{ .primary = true } };
    var endpoints = [_]rpc.Owner{ .{ .backend = owners[0].backend(), .graph = graph, .table = "docs" }, .{ .backend = owners[1].backend(), .graph = graph, .table = "docs" } };
    var clients = [_]rpc.Client{ .{ .channel = .{ .ptr = &endpoints[0], .call = Fixture.invoke }, .incarnation = 1, .revision = 1 }, .{ .channel = .{ .ptr = &endpoints[1], .call = Fixture.invoke }, .incarnation = 1, .revision = 1 } };
    var snapshots = [_]Snapshot{ .{ .backend = clients[0].backend(), .ptr = &owners[0], .validate = Fixture.check, .close = Fixture.close }, .{ .backend = clients[1].backend(), .ptr = &owners[1], .validate = Fixture.check, .close = Fixture.close } };
    const targets = [_]Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
    var set: ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &owners[0], .checkpoint = Fixture.check, .admit = undefined, .capture = undefined } };
    var coordinator: rpc.Owner = .{ .backend = set.backend(), .graph = graph, .table = "" };
    var client: rpc.Client = .{ .channel = .{ .ptr = &coordinator, .call = Fixture.invoke, .owner_local = false }, .incarnation = 1, .revision = 1 };
    const backend = client.backend();
    const values = try a.alloc(v.Value, 400);
    for (values, 0..) |*value, i| value.* = .{ .integer = @intCast(i) };
    const filters = &[_]b.PropertyFilter{.{ .property = "id", .op = .in, .value = .{ .list = values }, .domain = .integer }};
    var after: ?[]const u8 = null;
    var total: usize = 0;
    var primary: usize = 0;
    var seen: [400]bool = @splat(false);
    while (true) {
        const page = try backend.vtable.scan_where.?(backend.ptr, a, "docs", filters, after, 128);
        for (page.items) |node| {
            total += 1;
            if (std.mem.eql(u8, node.identity.key, "primary")) {
                primary += 1;
            } else {
                const id: usize = @intCast((v.Value{ .node = node }).property("id").integer);
                try std.testing.expect(!seen[id]);
                seen[id] = true;
            }
        }
        after = page.next;
        if (after == null) break;
    }
    try std.testing.expectEqual(@as(usize, 401), total);
    try std.testing.expectEqual(@as(usize, 1), primary);
    for (seen) |present| try std.testing.expect(present);
}

test "distributed probes prove EOF across trailing owners and decline fanout without bindings" {
    const Owner = struct {
        used: usize,
        revoked: bool = false,
        fn checkpoint(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.revoked) return error.Forbidden;
        }
        fn probeWhere(ptr: *anyopaque, _: A, _: []const u8, _: []const b.PropertyFilter, maximum: usize) !?b.Probe {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{ .page = .{ .items = &.{}, .examined = @min(self.used, maximum + 1) }, .complete = self.used <= maximum };
        }
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var owners = [_]Owner{ .{ .used = 2 }, .{ .used = 0 } };
    const targets = [_]Target{ .{ .table = "docs", .table_id = 1, .group_id = 1 }, .{ .table = "docs", .table_id = 1, .group_id = 2 } };
    var snapshots: [2]Snapshot = undefined;
    for (&owners, &snapshots) |*owner, *snapshot| snapshot.* = .{ .ptr = owner, .validate = Owner.checkpoint, .close = undefined, .backend = .{ .ptr = owner, .vtable = &.{ .bind = undefined, .checkpoint = Owner.checkpoint, .scan = undefined, .get = undefined, .expand = undefined, .probe_where = Owner.probeWhere } } };
    var read: ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &snapshots, .source = .{ .ptr = &owners, .checkpoint = Owner.checkpoint, .admit = undefined, .capture = undefined } };
    const backend = read.backend();
    const probe_where = backend.vtable.probe_where.?;
    const exact = (try probe_where(backend.ptr, a, "docs", &.{}, 2)).?;
    try std.testing.expect(exact.complete);
    try std.testing.expectEqual(@as(?usize, 2), exact.page.examined);
    owners[1].used = 1;
    const declined = (try probe_where(backend.ptr, a, "docs", &.{}, 2)).?;
    try std.testing.expect(!declined.complete);
    try std.testing.expectEqual(@as(usize, 0), declined.page.items.len);
    try std.testing.expectEqual(@as(?usize, 3), declined.page.examined);
    owners[1].revoked = true;
    try std.testing.expectError(error.Forbidden, probe_where(backend.ptr, a, "docs", &.{}, 2));
}

test "adjacency routing prunes only catalog certified source owners and preserves continuations" {
    const Fixture = struct {
        calls: [2]usize = .{ 0, 0 },
        pages: bool = false,
        const This = @This();
        const Owner = struct {
            fixture: *This,
            index: usize,
            fn check(_: *anyopaque) !void {}
            fn expand(ptr: *anyopaque, _: A, _: catalog.Relationship, _: v.Identity, _: ast.Direction, after: ?[]const u8, _: usize) !b.Page(*const v.Relationship) {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.fixture.calls[self.index] += 1;
                return .{ .items = &.{}, .next = if (self.fixture.pages and after == null) "more" else null };
            }
            fn between(ptr: *anyopaque, a: A, mapping: catalog.Relationship, identity: v.Identity, _: v.Identity, direction: ast.Direction, _: []const b.PropertyFilter, after: ?[]const u8, limit: usize, _: usize) !b.Page(*const v.Relationship) {
                return expand(ptr, a, mapping, identity, direction, after, limit);
            }
        };
    };
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var fixture: Fixture = .{};
    var owners = [_]Fixture.Owner{ .{ .fixture = &fixture, .index = 0 }, .{ .fixture = &fixture, .index = 1 } };
    var snapshots: [2]Snapshot = undefined;
    for (&owners, &snapshots) |*owner, *snapshot| snapshot.* = .{ .ptr = owner, .validate = Fixture.Owner.check, .close = undefined, .backend = .{ .ptr = owner, .vtable = &.{ .source_owned_adjacency = true, .bind = undefined, .checkpoint = Fixture.Owner.check, .scan = undefined, .get = undefined, .expand = Fixture.Owner.expand, .expand_between = Fixture.Owner.between } } };
    var targets = [_]Target{
        .{ .table = "docs", .table_id = 1, .group_id = 1, .ranges = &.{.{ .end = "m" }} },
        .{ .table = "docs", .table_id = 1, .group_id = 2, .ranges = &.{.{ .start = "m" }} },
    };
    const mapping: catalog.Relationship = .{ .name = "R", .stored_type = "R", .index = "links", .source_label = "D", .target_label = "D" };
    var set: ReadSet = .{ .a = a, .targets = &targets, .snapshots = &snapshots, .independent_validations = &.{}, .source = .{ .ptr = &fixture, .checkpoint = Fixture.Owner.check, .admit = undefined, .capture = undefined }, .graph = .{ .name = "g", .incarnation = 1, .revision = 1, .labels = &.{ .{ .name = "D", .table = "docs" }, .{ .name = "Other", .table = "other" } }, .relationships = &.{mapping} } };
    const backend = set.backend();
    for ([_]bool{ false, true }) |legacy| for ([_]bool{ false, true }) |pages| for ([_]bool{ false, true }) |pair| for ([_]ast.Direction{ .out, .in, .both }) |direction| {
        fixture.calls = .{ 0, 0 };
        fixture.pages = pages;
        if (legacy) {
            for (&targets) |*target| target.ranges = &.{};
        }
        var after: ?[]const u8 = null;
        while (true) {
            const page = if (pair) try backend.vtable.expand_between.?(backend.ptr, a, mapping, .{ .table = "docs", .key = "a" }, .{ .table = "docs", .key = "z" }, direction, &.{}, after, 1, 8) else try backend.vtable.expand(backend.ptr, a, mapping, .{ .table = "docs", .key = "a" }, direction, after, 1);
            after = page.next orelse break;
        }
        const visits: usize = if (pages) 2 else 1;
        try std.testing.expectEqual(if (legacy or direction != .in or !pair) visits else 0, fixture.calls[0]);
        try std.testing.expectEqual(if (legacy or direction != .out) visits else 0, fixture.calls[1]);
    };
    // Physical graph envelopes override primary placement. This assertion is
    // produced on owner z while its logical source is a on the other shard.
    targets[0].ranges = &.{.{ .end = "m" }};
    targets[1].ranges = &.{.{ .start = "m" }};
    snapshots[0].adjacency_routes = &.{.{ .index = "links", .outgoing = .{ .min = "k", .max = "l" }, .incoming = .{ .empty = true } }};
    snapshots[1].adjacency_routes = &.{.{ .index = "links", .outgoing = .{ .min = "a", .max = "a" }, .incoming = .{ .min = "b", .max = "b" } }};
    fixture.pages = false;
    fixture.calls = .{ 0, 0 };
    _ = try backend.vtable.expand(backend.ptr, a, mapping, .{ .table = "docs", .key = "a" }, .out, null, 1);
    try std.testing.expectEqual([2]usize{ 0, 1 }, fixture.calls);
    fixture.calls = .{ 0, 0 };
    _ = try backend.vtable.expand(backend.ptr, a, mapping, .{ .table = "docs", .key = "b" }, .in, null, 1);
    try std.testing.expectEqual([2]usize{ 0, 1 }, fixture.calls);
    fixture.calls = .{ 0, 0 };
    _ = try backend.vtable.expand(backend.ptr, a, mapping, .{ .table = "docs", .key = "outside" }, .both, null, 1);
    try std.testing.expectEqual([2]usize{ 0, 0 }, fixture.calls);
    // A missing directory is unknown, never proof of absence.
    snapshots[0].adjacency_routes = &.{};
    snapshots[1].adjacency_routes = &.{};
    // A known source in the other physical table cannot have outgoing rows in
    // this mapping. Incoming rows must still query every declared source owner.
    var cross = mapping;
    cross.target_label = "Other";
    fixture.pages = false;
    fixture.calls = .{ 0, 0 };
    _ = try backend.vtable.expand(backend.ptr, a, cross, .{ .table = "other", .key = "a" }, .out, null, 1);
    try std.testing.expectEqual([2]usize{ 0, 0 }, fixture.calls);
}
