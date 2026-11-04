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

//! Immutable, bounded vacuum inventories. Snapshot roots share manifest nodes;
//! saved jobs never repeat a manifest's complete file list for every ancestor.
//! Plans live beside the immutable maintenance journal, outside collected data.
const std = @import("std");
const objectstore = @import("objectstore");
const catalog = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog;
const A = std.mem.Allocator;
pub const max_node_bytes = 256 * 1024;
const fanout = 256;
const Digest = catalog.retirement_index.Digest;
const max_frontier_bytes = 1024;
pub const Retired = struct {
    snapshot: []const u8,
    // Version-one jobs retain their inline inventory and remain replayable.
    objects: []const []const u8 = &.{},
    object_plan: ?[]const u8 = null,
};
const Node = struct { objects: []const []const u8 = &.{}, children: []const []const u8 = &.{} };
pub const Inline = struct { hash: []const u8, node: Node, durable: bool = false };
const Cached = struct { node: Node, bytes: usize, durable: bool = false };
const CachedFrontier = struct { frame: Cursor.Frame, durable: bool = false };
pub const inline_budget = 2 * 1024 * 1024;

/// Allocations and all supplied object/child slices belong to the job arena.
pub const Store = struct {
    a: A,
    client: objectstore.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: catalog.types.Context,
    dry_run: bool,
    defer_writes: bool = false,
    uploads_remaining: usize = std.math.maxInt(usize),
    nodes: std.StringHashMapUnmanaged(Cached) = .empty,
    frontiers: std.AutoHashMapUnmanaged(Digest, CachedFrontier) = .empty,
    budget: usize = 256 * 1024 * 1024,

    fn key(self: *Store, hash: []const u8) ![]const u8 {
        if (hash.len != 64) return error.InvalidLakeVacuumPlan;
        for (hash) |byte| if (!std.ascii.isHex(byte)) return error.InvalidLakeVacuumPlan;
        return std.fmt.allocPrint(self.a, "{s}/maintenance/vacuum-plans/{s}.json", .{ self.prefix, hash });
    }
    fn save(self: *Store, node: Node) ![]const u8 {
        try self.context.ensureActive();
        const bytes = try std.json.Stringify.valueAlloc(self.a, node, .{});
        defer self.a.free(bytes);
        if (bytes.len > max_node_bytes or node.children.len > fanout) return error.LakeVacuumBudgetExceeded;
        const digest = catalog.types.digestHex(bytes);
        if (self.nodes.getEntry(&digest)) |entry| return entry.key_ptr.*;
        const hash = try self.a.dupe(u8, &digest);
        if (!self.dry_run and !self.defer_writes) try self.upload(hash, bytes);
        try self.nodes.put(self.a, hash, .{ .node = node, .bytes = bytes.len, .durable = !self.dry_run and !self.defer_writes });
        return hash;
    }
    fn upload(self: *Store, hash: []const u8, bytes: []const u8) !void {
        return self.putImmutable(try self.key(hash), bytes, max_node_bytes);
    }
    fn putImmutable(self: *Store, path: []const u8, bytes: []const u8, limit: usize) !void {
        var result: ?objectstore.PutResult = self.client.putObject(self.bucket, path, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.PreconditionFailed, error.ObjectAlreadyExists => existing: {
                var previous = try self.client.getObject(self.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = limit, .cancellation = catalog.types.contextCancellation(&self.context) });
                defer previous.deinit(self.client.allocator);
                if (!std.mem.eql(u8, previous.body, bytes)) return error.ArtifactIntegrityMismatch;
                break :existing null;
            },
            else => return err,
        };
        if (result) |*stored| stored.deinit(self.client.allocator);
    }
    fn frontierKey(self: *Store, hash: Digest) ![]const u8 {
        return std.fmt.allocPrint(self.a, "{s}/maintenance/vacuum-frontiers/{s}.json", .{ self.prefix, std.fmt.bytesToHex(hash, .lower) });
    }
    fn saveFrontier(self: *Store, frame: Cursor.Frame) !Digest {
        try self.context.ensureActive();
        const bytes = try std.json.Stringify.valueAlloc(self.a, frame, .{});
        defer self.a.free(bytes);
        if (bytes.len > max_frontier_bytes) return error.InvalidLakeVacuumPlan;
        var hash: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &hash, .{});
        if (self.frontiers.contains(hash)) return hash;
        // Intermediate parent positions need no durable object. Checkpoint
        // publication persists only the surviving parent chain.
        try self.frontiers.put(self.a, hash, .{ .frame = frame });
        return hash;
    }
    fn persistFrontier(self: *Store, root: ?Digest) !void {
        var pending: std.ArrayList(Digest) = .empty;
        var current = root;
        while (current) |hash| {
            // Uncached ancestors came from the prior durable checkpoint.
            const cached = self.frontiers.get(hash) orelse break;
            if (cached.durable) break;
            try self.context.ensureActive();
            try pending.append(self.a, hash);
            current = cached.frame.parent;
        }
        var remaining = pending.items.len;
        while (remaining != 0) {
            remaining -= 1;
            const hash = pending.items[remaining];
            try self.context.ensureActive();
            const bytes = try std.json.Stringify.valueAlloc(self.a, self.frontiers.get(hash).?.frame, .{});
            defer self.a.free(bytes);
            try self.putImmutable(try self.frontierKey(hash), bytes, max_frontier_bytes);
            self.frontiers.getPtr(hash).?.durable = true;
        }
    }
    fn loadFrontier(self: *Store, hash: Digest) !Cursor.Frame {
        try self.context.ensureActive();
        if (self.frontiers.get(hash)) |cached| return cached.frame;
        if (self.budget == 0) return error.LakeVacuumBudgetExceeded;
        var result = try self.client.getObject(self.bucket, try self.frontierKey(hash), .{ .skip_metadata_probe = true, .max_response_bytes = @min(max_frontier_bytes, self.budget), .cancellation = catalog.types.contextCancellation(&self.context) });
        defer result.deinit(self.client.allocator);
        self.budget -= result.body.len;
        var actual: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(result.body, &actual, .{});
        if (!std.mem.eql(u8, &actual, &hash)) return error.ArtifactIntegrityMismatch;
        const frame = try std.json.parseFromSliceLeaky(Cursor.Frame, self.a, result.body, .{ .allocate = .alloc_always });
        try self.frontiers.put(self.a, hash, .{ .frame = frame, .durable = true });
        return frame;
    }
    /// Select inline nodes before writing spills. An external node always owns
    /// a durable descendant closure, so future turns can skip closed subtrees.
    pub fn persist(self: *Store, roots: []const []const u8, inline_nodes: []const Inline) !void {
        if (self.dry_run) return;
        var embedded: std.StringHashMapUnmanaged(void) = .empty;
        for (inline_nodes) |entry| try embedded.put(self.a, entry.hash, {});
        var visited: std.StringHashMapUnmanaged(bool) = .empty;
        for (roots) |root| try self.persistNode(root, &embedded, &visited, false);
    }
    fn persistNode(self: *Store, root: []const u8, embedded: *const std.StringHashMapUnmanaged(void), visited: *std.StringHashMapUnmanaged(bool), closure: bool) anyerror!void {
        try self.context.ensureActive();
        // A missing cache entry was already durable in the previous checkpoint.
        const cached = self.nodes.get(root) orelse return;
        if (cached.durable) return;
        const external = closure or !embedded.contains(root);
        // A node seen only as inline may later be reached through an external
        // parent, whose complete closure must be persisted independently.
        if (visited.get(root)) |was_external| if (was_external or !external) return;
        try visited.put(self.a, root, external);
        for (cached.node.children) |child| try self.persistNode(child, embedded, visited, external);
        if (external) {
            const bytes = try std.json.Stringify.valueAlloc(self.a, cached.node, .{});
            defer self.a.free(bytes);
            if (self.uploads_remaining == 0) return error.LakeVacuumPlanYield;
            try self.upload(root, bytes);
            self.uploads_remaining -= 1;
            self.nodes.getPtr(root).?.durable = true;
        }
    }
    pub fn inlineFor(self: *Store, roots: []const []const u8) ![]const Inline {
        var reachable: std.StringHashMapUnmanaged(void) = .empty;
        var stack: std.ArrayList([]const u8) = .empty;
        try stack.appendSlice(self.a, roots);
        while (stack.pop()) |root| {
            if ((try reachable.getOrPut(self.a, root)).found_existing) continue;
            if (self.nodes.get(root)) |cached| try stack.appendSlice(self.a, cached.node.children);
        }
        return self.selectInline(&reachable, inline_budget);
    }
    pub fn objects(self: *Store, paths: []const []const u8) ![]const u8 {
        var parts: std.ArrayList([]const u8) = .empty;
        var start: usize = 0;
        var bytes: usize = 64; // JSON node framing; exact size checked in save().
        for (paths, 0..) |path, index| {
            const encoded = try std.json.Stringify.valueAlloc(self.a, path, .{});
            defer self.a.free(encoded);
            if (encoded.len + 1 > max_node_bytes - 64) return error.LakeVacuumBudgetExceeded;
            if (encoded.len + 1 > max_node_bytes - bytes) {
                try parts.append(self.a, try self.save(.{ .objects = paths[start..index] }));
                start = index;
                bytes = 64;
            }
            bytes += encoded.len + 1;
        }
        if (start < paths.len or parts.items.len == 0) try parts.append(self.a, try self.save(.{ .objects = paths[start..] }));
        return self.children(parts.items);
    }
    pub fn children(self: *Store, roots: []const []const u8) ![]const u8 {
        var level = roots;
        while (level.len > fanout) {
            var next: std.ArrayList([]const u8) = .empty;
            var begin: usize = 0;
            while (begin < level.len) {
                const end = @min(level.len, begin + fanout);
                try next.append(self.a, try self.save(.{ .children = level[begin..end] }));
                begin = end;
            }
            level = next.items;
        }
        if (level.len == 1) return level[0];
        return self.save(.{ .children = level });
    }
    /// Keep a bounded cache of small shared nodes in the root journal. Normal
    /// jobs reopen with one GET; larger inventories spill to authenticated nodes.
    pub fn inlineNodes(self: *Store) ![]const Inline {
        return self.selectInline(null, inline_budget);
    }
    fn selectInline(self: *Store, reachable: ?*const std.StringHashMapUnmanaged(void), limit: usize) ![]const Inline {
        const Ranked = struct { entry: Inline, cost: usize };
        var ranked: std.ArrayList(Ranked) = .empty;
        var iterator = self.nodes.iterator();
        while (iterator.next()) |entry| {
            if (reachable) |set| if (!set.contains(entry.key_ptr.*)) continue;
            try ranked.append(self.a, .{ .entry = .{ .hash = entry.key_ptr.*, .node = entry.value_ptr.node, .durable = entry.value_ptr.durable }, .cost = entry.value_ptr.bytes + 128 });
        }
        std.mem.sort(Ranked, ranked.items, {}, struct {
            fn less(_: void, left: Ranked, right: Ranked) bool {
                if (left.cost != right.cost) return left.cost < right.cost;
                return std.mem.lessThan(u8, left.entry.hash, right.entry.hash);
            }
        }.less);
        var out: std.ArrayList(Inline) = .empty;
        var bytes: usize = 2;
        for (ranked.items) |entry| {
            if (entry.cost > limit - bytes) break;
            bytes += entry.cost;
            try out.append(self.a, entry.entry);
        }
        return out.items;
    }
    // A planner temporarily retains unflushed nodes while bounded uploads
    // drain. The final immutable job keeps the smaller normal inline budget.
    pub const planning_budget = 24 * 1024 * 1024;
    pub fn pendingNodes(self: *Store) ![]const Inline {
        const selected = try self.selectInline(null, planning_budget);
        if (selected.len != self.nodes.count()) return error.LakeVacuumBudgetExceeded;
        return selected;
    }
    pub fn seed(self: *Store, nodes: []const Inline) !void {
        return self.seedLimit(nodes, inline_budget);
    }
    pub fn seedPlanning(self: *Store, nodes: []const Inline) !void {
        return self.seedLimit(nodes, planning_budget);
    }
    fn seedLimit(self: *Store, nodes: []const Inline, limit: usize) !void {
        var total: usize = 2;
        for (nodes) |entry| {
            try self.context.ensureActive();
            const bytes = try std.json.Stringify.valueAlloc(self.a, entry.node, .{});
            defer self.a.free(bytes);
            if (bytes.len > max_node_bytes or entry.node.children.len > fanout or bytes.len + 128 > limit - total) return error.InvalidLakeVacuumPlan;
            total += bytes.len + 128;
            if (!std.mem.eql(u8, &catalog.types.digestHex(bytes), entry.hash)) return error.ArtifactIntegrityMismatch;
            try self.nodes.put(self.a, entry.hash, .{ .node = entry.node, .bytes = bytes.len, .durable = entry.durable });
        }
    }
    fn load(self: *Store, hash: []const u8) !Node {
        try self.context.ensureActive();
        if (self.nodes.get(hash)) |cached| return cached.node;
        if (self.budget == 0) return error.LakeVacuumBudgetExceeded;
        const path = try self.key(hash);
        var result = try self.client.getObject(self.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = @min(max_node_bytes, self.budget), .cancellation = catalog.types.contextCancellation(&self.context) });
        defer result.deinit(self.client.allocator);
        if (result.body.len > self.budget) return error.LakeVacuumBudgetExceeded;
        self.budget -= result.body.len;
        if (!std.mem.eql(u8, &catalog.types.digestHex(result.body), hash)) return error.ArtifactIntegrityMismatch;
        const node = try std.json.parseFromSliceLeaky(Node, self.a, result.body, .{ .allocate = .alloc_always });
        if (node.children.len > fanout) return error.InvalidLakeVacuumPlan;
        try self.nodes.put(self.a, try self.a.dupe(u8, hash), .{ .node = node, .bytes = result.body.len, .durable = true });
        return node;
    }
};

/// A persisted DFS frontier. The visited-node set uses the existing immutable
/// radix index, so its checkpoint stays small even for very large inventories.
/// Paths in distinct manifest nodes may repeat; ownership deletion is idempotent
/// and selection deduplicates each bounded batch. Shared DAG nodes are read once.
pub const Cursor = struct {
    snapshot: usize = 0,
    legacy: usize = 0,
    frame: ?Frame = null,
    visited: ?Digest = null,
    // Parent frames are immutable pages, so checkpoint size and cold-resume
    // work do not grow with the depth of a partially planned inventory.
    pub const Frame = struct { hash: []const u8, object: usize = 0, child: usize = 0, parent: ?Digest = null };
};
pub const Walker = struct {
    store: *Store,
    retired: []const Retired,
    seen: *catalog.retirement_index.Index,
    cursor: Cursor,
    pending: std.StringHashMapUnmanaged(void) = .empty,

    pub fn init(store: *Store, retired: []const Retired, seen: *catalog.retirement_index.Index, cursor: Cursor) !Walker {
        if (cursor.snapshot > retired.len) return error.InvalidLakeVacuumPlan;
        return .{ .store = store, .retired = retired, .seen = seen, .cursor = cursor };
    }
    pub fn checkpoint(self: *Walker) !Cursor {
        var cursor = self.cursor;
        if (self.pending.count() != 0) {
            try self.store.context.ensureActive();
            var hashes: std.ArrayList([]const u8) = .empty;
            var keys = self.pending.keyIterator();
            while (keys.next()) |hash| try hashes.append(self.store.a, hash.*);
            cursor.visited = try self.seen.insertMany(cursor.visited, hashes.items);
        }
        // A returned cursor owns a durable closure, including every parent
        // page needed after a cold restart. Failed publication leaves the old
        // visited root and pending set available for a safe retry.
        try self.store.persistFrontier(if (cursor.frame) |frame| frame.parent else null);
        self.cursor = cursor;
        self.pending.clearRetainingCapacity();
        return cursor;
    }
    fn enter(self: *Walker, hash: []const u8, parent: ?Cursor.Frame) !bool {
        if (self.pending.contains(hash) or try self.seen.contains(self.cursor.visited, hash)) return false;
        const saved_parent = if (parent) |frame| try self.store.saveFrontier(frame) else null;
        try self.pending.put(self.store.a, hash, {});
        self.cursor.frame = .{ .hash = hash, .parent = saved_parent };
        return true;
    }
    /// The step budget includes graph edges, duplicate roots and frame pops,
    /// not only emitted paths. Yielded frontiers can always be checkpointed.
    pub fn next(self: *Walker, steps: *usize) !?[]const u8 {
        while (true) {
            try self.store.context.ensureActive();
            // Leave time to publish the constant-size mutable checkpoint.
            if (self.store.context.deadline_ns) |deadline| if (@import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s >= deadline) return error.LakeVacuumPlanYield;
            if (steps.* == 0) return error.LakeVacuumPlanYield;
            steps.* -= 1;
            if (self.cursor.frame) |current| {
                var frame = current;
                const node = try self.store.load(frame.hash);
                if (frame.object > node.objects.len or frame.child > node.children.len) return error.InvalidLakeVacuumPlan;
                if (frame.object < node.objects.len) {
                    const uri = node.objects[frame.object];
                    frame.object += 1;
                    self.cursor.frame = frame;
                    return uri;
                }
                if (frame.child < node.children.len) {
                    const hash = node.children[frame.child];
                    frame.child += 1;
                    if (!try self.enter(hash, frame)) self.cursor.frame = frame;
                } else self.cursor.frame = if (frame.parent) |hash| try self.store.loadFrontier(hash) else null;
                continue;
            }
            if (self.cursor.snapshot == self.retired.len) return null;
            const retired = self.retired[self.cursor.snapshot];
            if (retired.object_plan) |root| {
                _ = try self.enter(root, null);
            } else {
                if (self.cursor.legacy > retired.objects.len) return error.InvalidLakeVacuumPlan;
                if (self.cursor.legacy < retired.objects.len) {
                    const uri = retired.objects[self.cursor.legacy];
                    self.cursor.legacy += 1;
                    return uri;
                }
            }
            self.cursor.snapshot += 1;
            self.cursor.legacy = 0;
        }
    }
};

/// Visit the union of snapshot inventories once, including shared graph nodes.
/// The arena and store must outlive the iterator and every returned URI.
pub const Iterator = struct {
    store: *Store,
    retired: []const Retired,
    exclude: ?*const std.StringHashMapUnmanaged(void) = null,
    index: usize = 0,
    pending: []const []const u8 = &.{},
    position: usize = 0,
    stack: std.ArrayList([]const u8) = .empty,
    nodes: std.StringHashMapUnmanaged(void) = .empty,
    paths: std.StringHashMapUnmanaged(void) = .empty,

    pub fn next(self: *Iterator) !?[]const u8 {
        const a = self.store.a;
        while (true) {
            try self.store.context.ensureActive();
            if (self.position < self.pending.len) {
                const uri = self.pending[self.position];
                self.position += 1;
                if ((try self.paths.getOrPut(a, uri)).found_existing) continue;
                return uri;
            }
            if (self.stack.pop()) |hash| {
                if ((try self.nodes.getOrPut(a, hash)).found_existing) continue;
                const node = try self.store.load(hash);
                var index = node.children.len;
                while (index > 0) {
                    index -= 1;
                    try self.stack.append(a, node.children[index]);
                }
                self.pending = node.objects;
                self.position = 0;
                continue;
            }
            if (self.index == self.retired.len) return null;
            const retired = self.retired[self.index];
            self.index += 1;
            if (self.exclude) |excluded| if (excluded.contains(retired.snapshot)) continue;
            if (retired.object_plan) |root| {
                try self.stack.append(a, root);
            } else {
                self.pending = retired.objects;
                self.position = 0;
            }
        }
    }
};

test "external lake vacuum plan chunks a large inventory and shares it across restart and legacy jobs" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    const padding = try a.alloc(u8, 200);
    @memset(padding, 'x');
    const paths = try a.alloc([]const u8, 24000);
    for (paths, 0..) |*path, i| path.* = try std.fmt.allocPrint(a, "gs://archive/data/{s}-{d}.parquet", .{ padding, i });
    const inline_bytes = try std.json.Stringify.valueAlloc(a, paths, .{});
    try std.testing.expect(inline_bytes.len > catalog.types.max_commit_bytes);
    var store: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    const root = try store.objects(paths);
    try std.testing.expect(store.nodes.count() > 1);
    const retired = [_]Retired{
        .{ .snapshot = "1", .object_plan = root },
        .{ .snapshot = "2", .object_plan = root },
        .{ .snapshot = "legacy", .objects = &.{ paths[0], "gs://archive/legacy" } },
    };
    const journal = try std.json.Stringify.valueAlloc(a, retired, .{});
    try std.testing.expect(journal.len < 1024);
    var reopened: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    var iterator: Iterator = .{ .store = &reopened, .retired = &retired };
    var count: usize = 0;
    while (try iterator.next()) |path| {
        if (count < paths.len) try std.testing.expectEqualStrings(paths[count], path) else try std.testing.expectEqualStrings("gs://archive/legacy", path);
        count += 1;
    }
    try std.testing.expectEqual(paths.len + 1, count);
    try std.testing.expectEqual(store.nodes.count(), iterator.nodes.count());
    try std.testing.expectEqual(store.nodes.count(), reopened.nodes.count());
    // Each immutable node is read once even when snapshots share ancestors.
    try std.testing.expect(256 * 1024 * 1024 - reopened.budget < inline_bytes.len + 16 * 1024);
    const inline_nodes = try store.inlineNodes();
    const cached_bytes = try std.json.Stringify.valueAlloc(a, inline_nodes, .{});
    try std.testing.expect(cached_bytes.len <= inline_budget);
    var warm: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    try warm.seed(inline_nodes);
    var warmed: Iterator = .{ .store = &warm, .retired = retired[0..2] };
    var warm_count: usize = 0;
    while (try warmed.next()) |_| warm_count += 1;
    try std.testing.expectEqual(paths.len, warm_count);
    try std.testing.expect(warm.budget > reopened.budget);
    try std.testing.expectError(error.ArtifactIntegrityMismatch, warm.seed(&.{.{ .hash = root, .node = .{ .objects = &.{"changed"} } }}));
    // Authenticated roots cannot silently accept an overwritten plan chunk.
    var client = memory.client();
    var changed = try client.putObject("archive", try store.key(root), "{}", .{});
    changed.deinit(alloc);
    var corrupted: Store = .{ .a = a, .client = client, .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    var broken: Iterator = .{ .store = &corrupted, .retired = retired[0..1] };
    try std.testing.expectError(error.ArtifactIntegrityMismatch, broken.next());
}

test "external lake vacuum plan resumes lost uploads and marks shared objects of blocked snapshots" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var store: Store = .{ .a = a, .client = faults.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    faults.commitNextPutThenFail(error.InjectedAckLoss);
    try std.testing.expectError(error.InjectedAckLoss, store.objects(&.{"shared"}));
    const shared = try store.objects(&.{"shared"});
    const first = try store.children(&.{ shared, try store.objects(&.{"first"}) });
    const second = try store.children(&.{ shared, try store.objects(&.{"second"}) });
    const retired = [_]Retired{ .{ .snapshot = "1", .object_plan = first }, .{ .snapshot = "2", .object_plan = second } };
    var blocked: std.StringHashMapUnmanaged(void) = .empty;
    try blocked.put(a, "1", {});
    var marked: std.StringHashMapUnmanaged(void) = .empty;
    var marking: Iterator = .{ .store = &store, .retired = retired[0..1] };
    while (try marking.next()) |uri| try marked.put(a, uri, {});
    var sweeping: Iterator = .{ .store = &store, .retired = &retired, .exclude = &blocked };
    var deletable: usize = 0;
    while (try sweeping.next()) |uri| if (!marked.contains(uri)) {
        try std.testing.expectEqualStrings("second", uri);
        deletable += 1;
    };
    try std.testing.expectEqual(@as(usize, 1), deletable);
    // More than one fanout of shared children stays bounded and deduplicated.
    const children_ = try a.alloc([]const u8, fanout + 1);
    @memset(children_, shared);
    const wide = try store.children(children_);
    var wide_iterator: Iterator = .{ .store = &store, .retired = &.{.{ .snapshot = "wide", .object_plan = wide }} };
    try std.testing.expectEqualStrings("shared", (try wide_iterator.next()).?);
    try std.testing.expect(try wide_iterator.next() == null);
    const canceled: std.atomic.Value(bool) = .init(true);
    store.context.cancellation = objectstore.CancellationToken.fromAtomic(&canceled);
    try std.testing.expectError(error.Canceled, wide_iterator.next());
}

test "external lake vacuum plan dry runs keep inventories in memory" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("archive");
    var store: Store = .{ .a = a, .client = client, .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = true };
    const root = try store.objects(&.{"path"});
    var iterator: Iterator = .{ .store = &store, .retired = &.{.{ .snapshot = "1", .object_plan = root }} };
    try std.testing.expectEqualStrings("path", (try iterator.next()).?);
    try std.testing.expect(try iterator.next() == null);
    var listed = try client.listObjects("archive", .{ .prefix = "plans/" });
    defer listed.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), listed.entries.len);
}

test "external lake vacuum plan checkpoints bounded spills without uploading inline nodes" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var store: Store = .{ .a = a, .client = faults.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false, .defer_writes = true };
    const first = try store.objects(&.{"first"});
    const second = try store.objects(&.{"second"});
    const root = try store.children(&.{ first, second });
    const embedded = try store.inlineFor(&.{root});
    // This fault must remain armed while the entire graph is embedded.
    faults.commitNextPutThenFail(error.InjectedAckLoss);
    try store.persist(&.{root}, embedded);
    try std.testing.expectError(error.InjectedAckLoss, store.persist(&.{root}, &.{}));
    store.uploads_remaining = 1;
    try std.testing.expectError(error.LakeVacuumPlanYield, store.persist(&.{root}, &.{}));
    // Reopen only the saved checkpoint. Durable descendants are not uploaded
    // again; the external parent is published after its entire closure.
    const checkpoint = try store.pendingNodes();
    var reopened: Store = .{ .a = a, .client = faults.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false, .defer_writes = true, .uploads_remaining = 1 };
    try reopened.seedPlanning(checkpoint);
    try std.testing.expectError(error.LakeVacuumPlanYield, reopened.persist(&.{root}, &.{}));
    reopened.uploads_remaining = 1;
    try reopened.persist(&.{root}, &.{});
    var cold: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    var iterator: Iterator = .{ .store = &cold, .retired = &.{.{ .snapshot = "1", .object_plan = root }} };
    try std.testing.expectEqualStrings("first", (try iterator.next()).?);
    try std.testing.expectEqualStrings("second", (try iterator.next()).?);
    try std.testing.expect(try iterator.next() == null);
}

test "external lake vacuum traversal persists only final visited nodes and surviving frontier pages" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var writer: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    const children = try a.alloc([]const u8, 16);
    for (children, 0..) |*child, i| child.* = try writer.objects(try a.dupe([]const u8, &.{try std.fmt.allocPrint(a, "gs://archive/data/{d}", .{i})}));
    const root = try writer.children(children);
    var cold: Store = .{ .a = a, .client = faults.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    try cold.seed(try writer.inlineFor(&.{root}));
    const Counter = struct {
        puts: usize = 0,
        fn matches(raw: *anyopaque, _: []const u8, _: []const u8, _: []const u8) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.puts += 1;
            return false;
        }
    };
    var counter: Counter = .{};
    faults.put_filter = .{ .ptr = &counter, .matches = Counter.matches };
    var seen = catalog.retirement_index.Index.init(a, faults.client(), "archive", "seen", .{});
    defer seen.deinit();
    const retired = [_]Retired{ .{ .snapshot = "first", .object_plan = root }, .{ .snapshot = "shared", .object_plan = root } };
    var walker = try Walker.init(&cold, &retired, &seen, .{});
    var steps: usize = 64;
    var emitted: usize = 0;
    while (try walker.next(&steps)) |_| emitted += 1;
    try std.testing.expectEqual(@as(usize, 16), emitted);
    try std.testing.expectEqual(@as(usize, 0), counter.puts);
    const cursor = try walker.checkpoint();
    try std.testing.expect(counter.puts > 0 and counter.puts < 32);
    var reopened = catalog.retirement_index.Index.init(a, memory.client(), "archive", "seen", .{});
    defer reopened.deinit();
    try std.testing.expect(try reopened.contains(cursor.visited, root));
    for (children) |child| try std.testing.expect(try reopened.contains(cursor.visited, child));
}

test "external lake vacuum checkpoint resolves frontier ACK loss before returning a cold resumable cursor" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    var writer: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    const first = try writer.objects(&.{"first"});
    const second = try writer.objects(&.{"second"});
    const root = try writer.children(&.{ first, second });
    var cold: Store = .{ .a = a, .client = faults.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    try cold.seed(try writer.inlineFor(&.{root}));
    var seen = catalog.retirement_index.Index.init(a, faults.client(), "archive", "seen", .{});
    defer seen.deinit();
    const retired = [_]Retired{.{ .snapshot = "old", .object_plan = root }};
    var walker = try Walker.init(&cold, &retired, &seen, .{});
    var steps: usize = 64;
    try std.testing.expectEqualStrings("first", (try walker.next(&steps)).?);
    faults.put_filter = .{ .ptr = &faults, .matches = struct {
        fn matches(_: *anyopaque, _: []const u8, path: []const u8, _: []const u8) bool {
            return std.mem.indexOf(u8, path, "/vacuum-frontiers/") != null;
        }
    }.matches };
    faults.commitNextPutThenFail(error.InjectedAckLoss);
    try std.testing.expectError(error.InjectedAckLoss, walker.checkpoint());
    try std.testing.expect(walker.cursor.visited == null);
    const cursor = try walker.checkpoint();
    var resumed: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    var resumed_seen = catalog.retirement_index.Index.init(a, memory.client(), "archive", "seen", .{});
    defer resumed_seen.deinit();
    var replay = try Walker.init(&resumed, &retired, &resumed_seen, try std.json.parseFromSliceLeaky(Cursor, a, try std.json.Stringify.valueAlloc(a, cursor, .{}), .{}));
    steps = 64;
    try std.testing.expectEqualStrings("second", (try replay.next(&steps)).?);
    resumed_seen.context.deadline_ns = 0;
    const before = memory.operationCount();
    try std.testing.expectError(error.DeadlineExceeded, replay.checkpoint());
    try std.testing.expectEqual(before, memory.operationCount());
    try std.testing.expectEqual(cursor.visited.?, replay.cursor.visited.?);
    resumed_seen.context.deadline_ns = null;
    _ = try replay.checkpoint();
    try std.testing.expect(try replay.next(&steps) == null);
}

test "external lake vacuum frontier resumes cold chunks with bounded reads and shared roots" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var faults = objectstore.ScriptedFaultClient.init(alloc, memory.client());
    defer faults.deinit();
    const padding = try a.alloc(u8, 200);
    @memset(padding, 'x');
    const paths = try a.alloc([]const u8, 24000);
    for (paths, 0..) |*path, i| path.* = try std.fmt.allocPrint(a, "gs://archive/data/{s}-{d}.parquet", .{ padding, i });
    var store: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false };
    const root = try store.objects(paths);
    const embedded = try store.inlineFor(&.{root});
    const retired = [_]Retired{ .{ .snapshot = "old", .object_plan = root }, .{ .snapshot = "shared", .object_plan = root }, .{ .snapshot = "legacy", .objects = &.{"legacy"} } };
    var saved: []const u8 = "{}";
    var position: usize = 0;
    var turns: usize = 0;
    var complete = false;
    // The first immutable visited-index PUT commits but loses its response.
    faults.commitNextPutThenFail(error.InjectedAckLoss);
    while (!complete and turns < 512) : (turns += 1) {
        var turn = std.heap.ArenaAllocator.init(alloc);
        defer turn.deinit();
        const ta = turn.allocator();
        const cursor = try std.json.parseFromSliceLeaky(Cursor, ta, saved, .{ .allocate = .alloc_always });
        var cold: Store = .{ .a = ta, .client = faults.client(), .bucket = "archive", .prefix = "plans", .context = .{}, .dry_run = false, .budget = 4 * max_node_bytes };
        try cold.seed(embedded);
        var seen = catalog.retirement_index.Index.init(ta, faults.client(), "archive", "seen", .{});
        defer seen.deinit();
        var walker = try Walker.init(&cold, &retired, &seen, cursor);
        var steps: usize = 64;
        while (true) {
            const uri = (walker.next(&steps) catch |err| switch (err) {
                error.LakeVacuumPlanYield => break,
                else => return err,
            }) orelse {
                complete = true;
                break;
            };
            try std.testing.expectEqualStrings(if (position < paths.len) paths[position] else "legacy", uri);
            position += 1;
        }
        if (turns == 0) {
            // Simulate losing the mutable checkpoint after immutable seen-node
            // writes. Unpublished roots must not suppress the retried prefix.
            _ = try walker.checkpoint();
            position = 0;
            continue;
        }
        // Serialize/reopen every frontier, as a new process with a cold cache.
        saved = try std.json.Stringify.valueAlloc(a, try walker.checkpoint(), .{});
        try std.testing.expect(saved.len < 16 * 1024);
    }
    try std.testing.expect(complete and turns > 2);
    try std.testing.expectEqual(paths.len + 1, position);
    const terminal = try std.json.parseFromSliceLeaky(Cursor, a, saved, .{});
    var seen = catalog.retirement_index.Index.init(a, memory.client(), "archive", "seen", .{});
    defer seen.deinit();
    var replay = try Walker.init(&store, &retired, &seen, terminal);
    var steps: usize = 64;
    try std.testing.expect(try replay.next(&steps) == null);
    store.context.deadline_ns = @import("antfly_platform").time.monotonicNs() + std.time.ns_per_s;
    var pending = try Walker.init(&store, &retired, &seen, .{});
    const before = memory.operationCount();
    try std.testing.expectError(error.LakeVacuumPlanYield, pending.next(&steps));
    try std.testing.expectEqual(before, memory.operationCount());
    try std.testing.expectEqual(@as(usize, 0), (try pending.checkpoint()).snapshot);
    var bad = terminal;
    bad.snapshot = retired.len + 1;
    try std.testing.expectError(error.InvalidLakeVacuumPlan, Walker.init(&store, &retired, &seen, bad));
}

test "external lake vacuum frontier checkpoints stay bounded for deep inventory graphs" {
    const alloc = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    var memory = objectstore.MemoryClient.init(alloc);
    defer memory.deinit();
    var store: Store = .{ .a = a, .client = memory.client(), .bucket = "archive", .prefix = "deep", .context = .{}, .dry_run = false };
    const shared = try store.objects(&.{"shared"});
    var root = try store.objects(&.{"base"});
    for (0..512) |_| root = try store.children(try a.dupe([]const u8, &.{ root, shared }));
    const retired = [_]Retired{.{ .snapshot = "deep", .object_plan = root }};
    var saved: []const u8 = "{}";
    var emitted: usize = 0;
    var complete = false;
    var turns: usize = 0;
    while (!complete and turns < 256) : (turns += 1) {
        var turn = std.heap.ArenaAllocator.init(alloc);
        defer turn.deinit();
        const ta = turn.allocator();
        var cold: Store = .{ .a = ta, .client = memory.client(), .bucket = "archive", .prefix = "deep", .context = .{}, .dry_run = false, .budget = 4 * max_node_bytes };
        var seen = catalog.retirement_index.Index.init(ta, memory.client(), "archive", "deep-seen", .{});
        defer seen.deinit();
        var walker = try Walker.init(&cold, &retired, &seen, try std.json.parseFromSliceLeaky(Cursor, ta, saved, .{ .allocate = .alloc_always }));
        var steps: usize = 16;
        while (true) {
            const uri = (walker.next(&steps) catch |err| switch (err) {
                error.LakeVacuumPlanYield => break,
                else => return err,
            }) orelse {
                complete = true;
                break;
            };
            try std.testing.expectEqualStrings(if (emitted == 0) "base" else "shared", uri);
            emitted += 1;
        }
        saved = try std.json.Stringify.valueAlloc(a, try walker.checkpoint(), .{});
        try std.testing.expect(saved.len < 2048);
    }
    try std.testing.expect(complete and turns > 32);
    try std.testing.expectEqual(@as(usize, 2), emitted);
}
