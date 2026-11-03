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
pub const Retired = struct {
    snapshot: []const u8,
    // Version-one jobs retain their inline inventory and remain replayable.
    objects: []const []const u8 = &.{},
    object_plan: ?[]const u8 = null,
};
const Node = struct { objects: []const []const u8 = &.{}, children: []const []const u8 = &.{} };
pub const Inline = struct { hash: []const u8, node: Node };
const Cached = struct { node: Node, bytes: usize };
pub const inline_budget = 2 * 1024 * 1024;

/// Allocations and all supplied object/child slices belong to the job arena.
pub const Store = struct {
    a: A,
    client: objectstore.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: catalog.types.Context,
    dry_run: bool,
    nodes: std.StringHashMapUnmanaged(Cached) = .empty,
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
        if (!self.dry_run) {
            const path = try self.key(hash);
            var result: ?objectstore.PutResult = self.client.putObject(self.bucket, path, bytes, .{ .if_none_match = true, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
                error.PreconditionFailed, error.ObjectAlreadyExists => existing: {
                    var previous = try self.client.getObject(self.bucket, path, .{ .max_response_bytes = max_node_bytes, .cancellation = catalog.types.contextCancellation(&self.context) });
                    defer previous.deinit(self.client.allocator);
                    if (!std.mem.eql(u8, previous.body, bytes)) return error.ArtifactIntegrityMismatch;
                    break :existing null;
                },
                else => return err,
            };
            if (result) |*stored| stored.deinit(self.client.allocator);
        }
        try self.nodes.put(self.a, hash, .{ .node = node, .bytes = bytes.len });
        return hash;
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
        const Ranked = struct { entry: Inline, cost: usize };
        var ranked: std.ArrayList(Ranked) = .empty;
        var iterator = self.nodes.iterator();
        while (iterator.next()) |entry| {
            try ranked.append(self.a, .{ .entry = .{ .hash = entry.key_ptr.*, .node = entry.value_ptr.node }, .cost = entry.value_ptr.bytes + 128 });
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
            if (entry.cost > inline_budget - bytes) break;
            bytes += entry.cost;
            try out.append(self.a, entry.entry);
        }
        return out.items;
    }
    pub fn seed(self: *Store, nodes: []const Inline) !void {
        var total: usize = 2;
        for (nodes) |entry| {
            try self.context.ensureActive();
            const bytes = try std.json.Stringify.valueAlloc(self.a, entry.node, .{});
            defer self.a.free(bytes);
            if (bytes.len > max_node_bytes or entry.node.children.len > fanout or bytes.len + 128 > inline_budget - total) return error.InvalidLakeVacuumPlan;
            total += bytes.len + 128;
            if (!std.mem.eql(u8, &catalog.types.digestHex(bytes), entry.hash)) return error.ArtifactIntegrityMismatch;
            try self.nodes.put(self.a, entry.hash, .{ .node = entry.node, .bytes = bytes.len });
        }
    }
    fn load(self: *Store, hash: []const u8) !Node {
        try self.context.ensureActive();
        if (self.nodes.get(hash)) |cached| return cached.node;
        if (self.budget == 0) return error.LakeVacuumBudgetExceeded;
        const path = try self.key(hash);
        var result = try self.client.getObject(self.bucket, path, .{ .max_response_bytes = @min(max_node_bytes, self.budget), .cancellation = catalog.types.contextCancellation(&self.context) });
        defer result.deinit(self.client.allocator);
        if (result.body.len > self.budget) return error.LakeVacuumBudgetExceeded;
        self.budget -= result.body.len;
        if (!std.mem.eql(u8, &catalog.types.digestHex(result.body), hash)) return error.ArtifactIntegrityMismatch;
        const node = try std.json.parseFromSliceLeaky(Node, self.a, result.body, .{ .allocate = .alloc_always });
        if (node.children.len > fanout) return error.InvalidLakeVacuumPlan;
        try self.nodes.put(self.a, try self.a.dupe(u8, hash), .{ .node = node, .bytes = result.body.len });
        return node;
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
