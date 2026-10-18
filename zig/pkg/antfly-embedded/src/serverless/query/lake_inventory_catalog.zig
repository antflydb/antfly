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

//! Immutable snapshot file catalogs. Path-copying ordered trees reuse unchanged
//! manifest chunks; selected readers never flatten the archive or sort its IDs.
const std = @import("std");
const external = @import("../external_source/types.zig");
const decoded = @import("lake_decoded_cache.zig");
const Context = @import("lake_read_context.zig").Context;
const A = std.mem.Allocator;
pub const Chunk = struct { key: [32]u8, lease: decoded.Lease };
const Node = struct {
    refs: std.atomic.Value(usize) = .init(1),
    a: A,
    file: *const external.FileEntry,
    left: ?*Node,
    right: ?*Node,
    priority: u64,
    count: usize,
    rows: u64,
    bytes: u64,
    fn retain(node: ?*Node) ?*Node {
        if (node) |value| _ = value.refs.fetchAdd(1, .monotonic);
        return node;
    }
    fn release(node: ?*Node) void {
        const value = node orelse return;
        if (value.refs.fetchSub(1, .acq_rel) != 1) return;
        release(value.left);
        release(value.right);
        value.a.destroy(value);
    }
    fn create(a: A, file: *const external.FileEntry, left: ?*Node, right: ?*Node) !*Node {
        const node = try a.create(Node);
        var hash: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(file.file_id, &hash, .{});
        node.* = .{ .a = a, .file = file, .left = retain(left), .right = retain(right), .priority = std.mem.readInt(u64, hash[0..8], .little), .count = 1 + (if (left) |v| v.count else 0) + (if (right) |v| v.count else 0), .rows = file.row_count +| (if (left) |v| v.rows else 0) +| (if (right) |v| v.rows else 0), .bytes = file.byte_len +| (if (left) |v| v.bytes else 0) +| (if (right) |v| v.bytes else 0) };
        return node;
    }
    fn summarize(node: ?*Node, depth: usize) !void {
        const value = node orelse return;
        if (depth == 256) return error.InvalidExternalSourceInventory;
        try summarize(value.left, depth + 1);
        try summarize(value.right, depth + 1);
        value.count = 1 + (if (value.left) |n| n.count else 0) + (if (value.right) |n| n.count else 0);
        value.rows = value.file.row_count +| (if (value.left) |n| n.rows else 0) +| (if (value.right) |n| n.rows else 0);
        value.bytes = value.file.byte_len +| (if (value.left) |n| n.bytes else 0) +| (if (value.right) |n| n.bytes else 0);
    }
    fn precedes(priority: u64, id: []const u8, other: *const Node) bool {
        return priority < other.priority or (priority == other.priority and std.mem.lessThan(u8, id, other.file.file_id));
    }
    const Split = struct { left: ?*Node, right: ?*Node };
    fn split(a: A, root: ?*Node, id: []const u8, depth: usize) !Split {
        if (depth == 256) return error.InvalidExternalSourceInventory;
        const node = root orelse return .{ .left = null, .right = null };
        if (std.mem.lessThan(u8, node.file.file_id, id)) {
            const parts = try split(a, node.right, id, depth + 1);
            defer release(parts.left);
            errdefer release(parts.right);
            return .{ .left = try create(a, node.file, node.left, parts.left), .right = parts.right };
        }
        const parts = try split(a, node.left, id, depth + 1);
        defer release(parts.right);
        errdefer release(parts.left);
        return .{ .left = parts.left, .right = try create(a, node.file, parts.right, node.right) };
    }
    fn insert(a: A, root: ?*Node, file: *const external.FileEntry, depth: usize) !*Node {
        if (depth == 256) return error.InvalidExternalSourceInventory;
        const node = root orelse return create(a, file, null, null);
        const order = std.mem.order(u8, file.file_id, node.file.file_id);
        if (order == .eq) return error.InvalidExternalSourceInventory;
        var hash: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(file.file_id, &hash, .{});
        if (precedes(std.mem.readInt(u64, hash[0..8], .little), file.file_id, node)) {
            const parts = try split(a, root, file.file_id, depth + 1);
            defer release(parts.left);
            defer release(parts.right);
            return create(a, file, parts.left, parts.right);
        }
        const branch = try insert(a, if (order == .lt) node.left else node.right, file, depth + 1);
        defer release(branch);
        return create(a, node.file, if (order == .lt) branch else node.left, if (order == .gt) branch else node.right);
    }
    fn merge(a: A, left: ?*Node, right: ?*Node, depth: usize) !?*Node {
        if (depth == 256) return error.InvalidExternalSourceInventory;
        const l = left orelse return retain(right);
        const r = right orelse return retain(left);
        if (precedes(l.priority, l.file.file_id, r)) {
            const branch = try merge(a, l.right, right, depth + 1);
            defer release(branch);
            return try create(a, l.file, l.left, branch);
        }
        const branch = try merge(a, left, r.left, depth + 1);
        defer release(branch);
        return try create(a, r.file, branch, r.right);
    }
    fn remove(a: A, root: ?*Node, id: []const u8, depth: usize) !?*Node {
        if (depth == 256) return error.InvalidExternalSourceInventory;
        const node = root orelse return error.InvalidExternalSourceInventory;
        const order = std.mem.order(u8, id, node.file.file_id);
        if (order == .eq) return merge(a, node.left, node.right, depth + 1);
        const branch = try remove(a, if (order == .lt) node.left else node.right, id, depth + 1);
        defer release(branch);
        return try create(a, node.file, if (order == .lt) branch else node.left, if (order == .gt) branch else node.right);
    }
};
pub const Iterator = struct {
    stack: [256]*Node = undefined,
    len: usize = 0,
    pending: ?*Node,
    pub fn next(self: *Iterator) !?*const external.FileEntry {
        while (self.pending) |node| {
            if (self.len == self.stack.len) return error.InvalidExternalSourceInventory;
            self.stack[self.len] = node;
            self.len += 1;
            self.pending = node.left;
        }
        if (self.len == 0) return null;
        self.len -= 1;
        const node = self.stack[self.len];
        self.pending = node.right;
        return node.file;
    }
};
pub const Ordering = struct {
    a: A,
    identity: [32]u8,
    order: []usize,
    ranks: []usize,
    by_id: std.StringHashMapUnmanaged(usize) = .empty,
    fn destroy(self: *Ordering) void {
        const a = self.a;
        a.free(self.order);
        a.free(self.ranks);
        self.by_id.deinit(a);
        a.destroy(self);
    }
};
pub const Index = struct {
    census_mu: std.atomic.Mutex = .unlocked,
    census: ?*@import("lake_file_census.zig").Census = null,
    prior_census: ?*@import("lake_file_census.zig").Census = null,
    removed_ids: std.ArrayList([]const u8) = .empty,
    added_ids: std.ArrayList([]const u8) = .empty,
    a: A,
    cache_owner: ?*Cache = null,
    refs: std.atomic.Value(usize) = .init(1),
    root: ?*Node = null,
    chunks: []Chunk = &.{},
    by_key: std.AutoHashMapUnmanaged([32]u8, usize) = .empty,
    initialized_chunks: usize = 0,
    new_files: usize = 0,
    ordering: std.atomic.Value(?*Ordering) = .init(null),
    ordering2: std.atomic.Value(?*Ordering) = .init(null),
    ordering_mu: std.atomic.Mutex = .unlocked,
    pub fn retain(self: *Index) *Index {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *Index) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const a = self.a;
        if (self.ordering.load(.acquire)) |order| order.destroy();
        if (self.ordering2.load(.acquire)) |order| order.destroy();
        if (self.census) |value| value.release();
        if (self.prior_census) |value| value.release();
        for (self.removed_ids.items) |id| a.free(id);
        self.removed_ids.deinit(a);
        self.added_ids.deinit(a);
        Node.release(self.root);
        for (self.chunks[0..self.initialized_chunks]) |chunk| chunk.lease.release();
        a.free(self.chunks);
        self.by_key.deinit(a);
        a.destroy(self);
    }
    pub fn acquireCensus(self: *Index, context: Context) !?*@import("lake_file_census.zig").Census {
        while (!self.census_mu.tryLock()) {
            try context.ensureActive();
            if (context.io) |io| try io.sleep(.fromMilliseconds(1), .awake) else std.atomic.spinLoopHint();
        }
        defer self.census_mu.unlock();
        return if (self.census) |value| value.retain() else null;
    }
    /// Domain includes source interpretation, store and complete delete evidence.
    /// The catalog's credential-scoped immutable chunk lineage supplies deltas.
    pub fn prepareCensus(self: *Index, domain: [32]u8, all: []const external.FileEntry, fingerprint: @import("lake_file_census.zig").Fingerprint, context: Context) !*@import("lake_file_census.zig").Census {
        while (!self.census_mu.tryLock()) {
            try context.ensureActive();
            if (context.io) |io| try io.sleep(.fromMilliseconds(1), .awake) else std.atomic.spinLoopHint();
        }
        defer self.census_mu.unlock();
        if (self.census) |value| if (std.mem.eql(u8, &domain, &value.domain)) return value.retain();
        const value = try @import("lake_file_census.zig").Census.build(self.a, domain, self.prior_census, self.removed_ids.items, self.added_ids.items, all, fingerprint, context);
        if (value.summary()) |summary| if (summary.count != self.count()) {
            value.release();
            return error.InvalidExternalSourceInventory;
        };
        if (self.census) |old| old.release();
        self.census = value;
        return value.retain();
    }
    pub fn count(self: *const Index) usize {
        return if (self.root) |root| root.count else 0;
    }
    pub fn rows(self: *const Index) u64 {
        return if (self.root) |root| root.rows else 0;
    }
    pub fn bytes(self: *const Index) u64 {
        return if (self.root) |root| root.bytes else 0;
    }
    pub fn footprint(self: *const Index) usize {
        var total = self.count() * @sizeOf(Node) + self.chunks.len * @sizeOf(Chunk);
        for (self.chunks) |chunk| total +|= chunk.lease.item.budget.live;
        return total;
    }
    pub fn acquireChunk(self: *const Index, key: [32]u8) ?decoded.Lease {
        const position = self.by_key.get(key) orelse return null;
        return self.chunks[position].lease.retain();
    }
    pub fn find(self: *const Index, id: []const u8) ?*const external.FileEntry {
        var current = self.root;
        while (current) |node| switch (std.mem.order(u8, id, node.file.file_id)) {
            .eq => return node.file,
            .lt => current = node.left,
            .gt => current = node.right,
        };
        return null;
    }
    pub fn iterator(self: *const Index) Iterator {
        return .{ .pending = self.root };
    }
    /// Header/version strings remain snapshot-local; every other field borrows
    /// the immutable catalog pinned by the source's snapshot lease.
    pub fn qualify(a: A, file: external.FileEntry, snapshot: []const u8) !external.FileEntry {
        const marker = "iceberg:v1:snapshot=manifest-chunk";
        if (!std.mem.startsWith(u8, file.version_id, marker)) {
            var out = file;
            out.version_id = try a.dupe(u8, file.version_id);
            return out;
        }
        var out = file;
        out.version_id = try std.fmt.allocPrint(a, "iceberg:v1:snapshot={s}{s}", .{ snapshot, file.version_id[marker.len..] });
        return out;
    }
    pub fn materialize(self: *const Index, a: A, snapshot: []const u8, context: Context) ![]external.FileEntry {
        const files = try a.alloc(external.FileEntry, self.count());
        var initialized: usize = 0;
        errdefer {
            for (files[0..initialized]) |file| a.free(file.version_id);
            a.free(files);
        }
        var it = self.iterator();
        for (files) |*file| {
            try context.ensureActive();
            file.* = try qualify(a, (try it.next()).?.*, snapshot);
            initialized += 1;
        }
        return files;
    }
    pub fn orderingFor(self: *Index, version: u8) *std.atomic.Value(?*Ordering) {
        return if (version == 2) &self.ordering2 else &self.ordering;
    }
    pub fn ensureOrdering(self: *Index, source: []const u8, snapshot: []const u8, context: Context) !*const Ordering {
        return self.ensureOrderingVersion(source, snapshot, 1, context);
    }
    pub fn ensureOrderingVersion(self: *Index, source: []const u8, snapshot: []const u8, version: u8, context: Context) !*const Ordering {
        const identity = @import("../../storage/rowsource/identity.zig").fileDigest(source, snapshot, "");
        if (self.orderingFor(version).load(.acquire)) |order| {
            if (!std.mem.eql(u8, &identity, &order.identity)) return error.ExternalLakeSnapshotMismatch;
            return order;
        }
        while (!self.ordering_mu.tryLock()) {
            try context.ensureActive();
            if (context.io) |io| try io.sleep(.fromMilliseconds(1), .awake) else std.atomic.spinLoopHint();
        }
        defer self.ordering_mu.unlock();
        if (self.orderingFor(version).load(.acquire)) |order| {
            if (!std.mem.eql(u8, &identity, &order.identity)) return error.ExternalLakeSnapshotMismatch;
            return order;
        }
        const order = while (true) {
            break self.buildOrdering(source, snapshot, identity, version, context) catch |err| {
                if (err != error.OutOfMemory) return err;
                const cache = self.cache_owner orelse return err;
                if (!cache.evictOne()) return err;
                try context.ensureActive();
                continue;
            };
        };
        self.orderingFor(version).store(order, .release);
        return order;
    }
    fn buildOrdering(self: *Index, source: []const u8, snapshot: []const u8, identity: [32]u8, version: u8, context: Context) !*Ordering {
        const a = self.a;
        const order = try a.create(Ordering);
        errdefer a.destroy(order);
        order.* = .{ .a = a, .identity = identity, .order = try a.alloc(usize, self.count()), .ranks = &.{} };
        errdefer a.free(order.order);
        order.ranks = try a.alloc(usize, self.count());
        errdefer a.free(order.ranks);
        errdefer order.by_id.deinit(a);
        const hashes = try a.alloc([32]u8, self.count());
        defer a.free(hashes);
        try order.by_id.ensureTotalCapacity(a, @intCast(self.count()));
        var it = self.iterator();
        for (hashes, order.order, 0..) |*hash, *position, i| {
            try context.ensureActive();
            const file = (try it.next()).?;
            hash.* = @import("../../storage/rowsource/identity.zig").navigationDigest(version, source, snapshot, file.file_id);
            position.* = i;
            order.by_id.putAssumeCapacity(file.file_id, i);
        }
        std.mem.sort(usize, order.order, hashes, struct {
            fn less(keys: [][32]u8, l: usize, r: usize) bool {
                return std.mem.lessThan(u8, &keys[l], &keys[r]);
            }
        }.less);
        try context.ensureActive();
        for (order.order, 0..) |file, i| order.ranks[file] = i;
        return order;
    }
    pub fn build(a: A, prior: ?*const Index, chunks: []const Chunk, context: Context) !*Index {
        const index = try a.create(Index);
        index.* = .{ .a = a };
        errdefer index.release();
        const owned = try a.alloc(Chunk, chunks.len);
        index.chunks = owned;
        for (chunks, owned, 0..) |input, *out, i| {
            const slot = try index.by_key.getOrPut(a, input.key);
            if (slot.found_existing) return error.DuplicateIcebergManifest;
            slot.value_ptr.* = i;
            out.* = .{ .key = input.key, .lease = if (prior) |old| old.acquireChunk(input.key) orelse input.lease.retain() else input.lease.retain() };
            index.initialized_chunks = i + 1;
        }
        if (prior == null) {
            try index.buildInitial(context);
            return index;
        }
        index.root = Node.retain(if (prior) |old| old.root else null);
        if (prior) |old| index.prior_census = try @constCast(old).acquireCensus(context);
        if (prior) |old| for (old.chunks) |part| if (!index.by_key.contains(part.key)) {
            for (part.lease.item.payload.iceberg_chunk.files) |file| {
                try context.ensureActive();
                if (index.prior_census != null) {
                    const id = try a.dupe(u8, file.file_id);
                    index.removed_ids.append(a, id) catch |err| {
                        a.free(id);
                        return err;
                    };
                }
                const next = try Node.remove(a, index.root, file.file_id, 0);
                Node.release(index.root);
                index.root = next;
            }
        };
        for (index.chunks) |part| if (prior == null or !prior.?.by_key.contains(part.key)) {
            for (part.lease.item.payload.iceberg_chunk.files) |*file| {
                try context.ensureActive();
                if (index.find(file.file_id) != null) return error.InvalidExternalSourceInventory;
                const next = try Node.insert(a, index.root, file, 0);
                Node.release(index.root);
                index.root = next;
                index.new_files += 1;
                if (index.prior_census != null) try index.added_ids.append(a, file.file_id);
            }
        };
        return index;
    }
    /// Merge sorted chunks and build a Cartesian tree in one pass. Each file
    /// allocates one node and hashes its priority once; no path copies exist yet.
    fn buildInitial(self: *Index, context: Context) !void {
        const Head = struct { files: []const external.FileEntry, position: usize = 0 };
        const Order = struct {
            fn less(_: void, l: Head, r: Head) std.math.Order {
                return std.mem.order(u8, l.files[l.position].file_id, r.files[r.position].file_id);
            }
        };
        var queue: std.PriorityQueue(Head, void, Order.less) = .initContext({});
        defer queue.deinit(self.a);
        try queue.ensureUnusedCapacity(self.a, self.chunks.len);
        for (self.chunks) |part| {
            const files = part.lease.item.payload.iceberg_chunk.files;
            if (files.len != 0) try queue.push(self.a, .{ .files = files });
        }
        var stack: [256]*Node = undefined;
        var stack_len: usize = 0;
        var previous: ?[]const u8 = null;
        while (queue.pop()) |head_| {
            try context.ensureActive();
            var head = head_;
            const file = &head.files[head.position];
            if (previous) |id| if (!std.mem.lessThan(u8, id, file.file_id)) return error.InvalidExternalSourceInventory;
            previous = file.file_id;
            const node = try Node.create(self.a, file, null, null);
            var left: ?*Node = null;
            while (stack_len != 0 and Node.precedes(node.priority, file.file_id, stack[stack_len - 1])) {
                stack_len -= 1;
                left = stack[stack_len];
            }
            node.left = left; // transfer unpublished tree ownership
            if (stack_len == 0) self.root = node else stack[stack_len - 1].right = node;
            if (stack_len == stack.len) return error.InvalidExternalSourceInventory;
            stack[stack_len] = node;
            stack_len += 1;
            self.new_files += 1;
            head.position += 1;
            if (head.position != head.files.len) try queue.push(self.a, head);
        }
        try Node.summarize(self.root, 0);
    }
};
/// A bounded, data-only predecessor cache. The owner outlives every decoded
/// snapshot and every structurally shared catalog node.
pub const Cache = struct {
    budget: @import("../../sql/memory_budget.zig") = .{ .backing = std.heap.page_allocator, .limit = 128 * 1024 * 1024 },
    mutex: std.atomic.Mutex = .unlocked,
    entries: [4]?struct { key: [32]u8, index: *Index, used: u64 } = @splat(null),
    tick: u64 = 0,
    fn lock(self: *Cache) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    /// Drop only the cache's pin. Active readers and shared tree nodes remain
    /// valid; exhausting all reclaimable entries preserves the uncached fallback.
    fn evictOne(self: *Cache) bool {
        self.lock();
        var target: ?usize = null;
        for (self.entries, 0..) |entry, i| if (entry) |value| {
            if (target == null or value.used < self.entries[target.?].?.used) target = i;
        };
        const index = target orelse {
            self.mutex.unlock();
            return false;
        };
        const retired = self.entries[index].?.index;
        self.entries[index] = null;
        self.mutex.unlock();
        retired.release();
        return true;
    }
    pub fn build(self: *Cache, key: [32]u8, prior: ?*const Index, chunks: []const Chunk, context: Context) !*Index {
        const index = while (true) {
            try context.ensureActive();
            break Index.build(self.budget.allocator(), prior, chunks, context) catch |err| {
                if (err != error.OutOfMemory or !self.evictOne()) return err;
                continue;
            };
        };
        index.cache_owner = self;
        self.put(key, index);
        return index;
    }
    pub fn get(self: *Cache, key: [32]u8) ?*Index {
        self.lock();
        defer self.mutex.unlock();
        self.tick +|= 1;
        for (&self.entries) |*entry| if (entry.*) |*value| if (std.mem.eql(u8, &value.key, &key)) {
            value.used = self.tick;
            return value.index.retain();
        };
        return null;
    }
    pub fn put(self: *Cache, key: [32]u8, index: *Index) void {
        self.lock();
        self.tick +|= 1;
        var target: usize = 0;
        for (self.entries, 0..) |entry, i| {
            if (entry) |value| {
                if (std.mem.eql(u8, &value.key, &key)) {
                    target = i;
                    break;
                }
                if (self.entries[target] != null and value.used < self.entries[target].?.used) target = i;
            } else target = i;
        }
        const old = self.entries[target];
        self.entries[target] = .{ .key = key, .index = index.retain(), .used = self.tick };
        self.mutex.unlock();
        if (old) |entry| entry.index.release();
    }
    pub fn deinit(self: *Cache) void {
        for (self.entries) |entry| if (entry) |value| value.index.release();
        self.entries = @splat(null);
        std.debug.assert(self.budget.live == 0);
    }
};

test "external lake metadata navigation persistent catalogs reuse chunks and defer canonical ordering" {
    const a = std.testing.allocator;
    var memory: decoded.Cache = .{ .a = a };
    defer memory.deinit();
    const Fixture = struct {
        fn chunk(cache: *decoded.Cache, first: usize, count_: usize) !decoded.Lease {
            const lease = try cache.create(1024 * 1024);
            errdefer lease.release();
            const ca = lease.item.budget.allocator();
            const inventory: external.Inventory = .{ .format = .iceberg, .source_id = try ca.dupe(u8, "source"), .source_uri = try ca.dupe(u8, "uri"), .snapshot_id = try ca.dupe(u8, "manifest-chunk"), .schema_fingerprint = try ca.dupe(u8, "schema"), .files = try ca.alloc(external.FileEntry, count_) };
            for (inventory.files, first..) |*file, i| file.* = .{ .file_id = try std.fmt.allocPrint(ca, "file-{d:0>4}", .{i}), .object_uri = try ca.dupe(u8, "file://data"), .byte_len = 100, .row_count = 3, .row_groups = &.{}, .etag = &.{}, .version_id = try ca.dupe(u8, "iceberg:v1:snapshot=manifest-chunk:sequence=1") };
            lease.item.payload = .{ .iceberg_chunk = inventory };
            return lease;
        }
    };
    const one = try Fixture.chunk(&memory, 0, 100);
    defer one.release();
    const two = try Fixture.chunk(&memory, 100, 100);
    defer two.release();
    const append = try Fixture.chunk(&memory, 200, 1);
    defer append.release();
    const old = try Index.build(a, null, &.{ .{ .key = @splat(1), .lease = one }, .{ .key = @splat(2), .lease = two } }, .{});
    defer old.release();
    const next = try Index.build(a, old, &.{ .{ .key = @splat(1), .lease = one }, .{ .key = @splat(2), .lease = two }, .{ .key = @splat(3), .lease = append } }, .{});
    defer next.release();
    try std.testing.expectEqual(@as(usize, 1), next.new_files);
    try std.testing.expect(old.find("file-0001") == next.find("file-0001"));
    try std.testing.expectEqual(@as(usize, 201), next.count());
    try std.testing.expect(next.ordering.load(.acquire) == null);
    const unchanged = try Index.build(a, next, next.chunks, .{});
    defer unchanged.release();
    try std.testing.expectEqual(@as(usize, 0), unchanged.new_files);
    try std.testing.expect(unchanged.root == next.root);
    const removed = try Index.build(a, next, &.{.{ .key = @splat(1), .lease = one }}, .{});
    defer removed.release();
    try std.testing.expectEqual(@as(usize, 100), removed.count());
    try std.testing.expect(removed.find("file-0100") == null and old.find("file-0100") != null);
    const files = try next.materialize(a, "fresh", .{});
    defer {
        for (files) |file| a.free(file.version_id);
        a.free(files);
    }
    const inventory: external.Inventory = .{ .format = .iceberg, .source_id = @constCast("source"), .source_uri = @constCast("uri"), .snapshot_id = @constCast("fresh"), .schema_fingerprint = @constCast("schema"), .files = files };
    const reference = try @import("lake_serving.zig").ServingSource.canonicalFileOrder(a, inventory);
    defer a.free(reference);
    const ordered = try next.ensureOrdering("source", "fresh", .{});
    try std.testing.expectEqualSlices(usize, reference, ordered.order);
    try std.testing.expect(ordered == try next.ensureOrdering("source", "fresh", .{}));
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, next.ensureOrdering("source", "other", .{}));
    const Harness = struct {
        fn run(failing: A, prior: *Index, chunks: []const Chunk) !void {
            const candidate = try Index.build(failing, prior, chunks, .{});
            defer candidate.release();
            _ = try candidate.ensureOrdering("source", "fresh", .{});
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Harness.run, .{ old, next.chunks });
    var cache: Cache = .{ .budget = .{ .backing = a, .limit = 128 * 1024 } };
    defer cache.deinit();
    const resident = try cache.build(@splat(1), null, old.chunks, .{});
    resident.release();
    cache.budget.limit = cache.budget.live + cache.budget.live / 2;
    const replacement = try cache.build(@splat(2), null, old.chunks, .{});
    defer replacement.release();
    try std.testing.expectEqual(old.count(), replacement.count());
    try std.testing.expect(cache.get(@splat(1)) == null);
    // Reader pins survive eviction, even if they prevent reclaiming quota.
    try std.testing.expect(cache.evictOne());
    try std.testing.expectEqual(old.count(), replacement.count());
    const Cold = struct {
        fn run(failing: A, chunks: []const Chunk) !void {
            const index = try Index.build(failing, null, chunks, .{});
            defer index.release();
            try std.testing.expectEqual(@as(usize, 201), index.count());
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Cold.run, .{next.chunks});
}
