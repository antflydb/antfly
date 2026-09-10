// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Persistent AVL index over immutable, committed WAL transaction buffers.
//! Extending a view copies only changed search paths, never previous payloads.
//! Version keys preserve reads at an older source boundary in a leading view.
const std = @import("std");
const Allocator = std.mem.Allocator;
const wal = @import("antfly_vectorindex").vector_block_wal;
pub const Record = wal.Record;

const Chunk = struct {
    alloc: Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    bytes: []u8,

    fn copy(alloc: Allocator, bytes: []const u8) !*Chunk {
        const self = try alloc.create(Chunk);
        errdefer alloc.destroy(self);
        self.* = .{ .alloc = alloc, .bytes = try alloc.dupe(u8, bytes) };
        return self;
    }
    fn retain(self: *Chunk) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    fn release(self: *Chunk) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const alloc = self.alloc;
        alloc.free(self.bytes);
        alloc.destroy(self);
    }
};

pub const Node = struct {
    alloc: Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    chunk: *Chunk,
    record: Record,
    left: ?*Node,
    right: ?*Node,
    height: u8,
    min_batch: u64,
    max_batch: u64,

    pub fn retain(self: *Node) *Node {
        _ = self.refs.fetchAdd(1, .monotonic);
        return self;
    }
    pub fn release(self: *Node) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        if (self.left) |node| node.release();
        if (self.right) |node| node.release();
        self.chunk.release();
        self.alloc.destroy(self);
    }
    fn h(node: ?*Node) u8 {
        return if (node) |n| n.height else 0;
    }
    fn create(alloc: Allocator, chunk: *Chunk, record: Record, left: ?*Node, right: ?*Node) !*Node {
        const node = try alloc.create(Node);
        node.* = .{
            .alloc = alloc,
            .chunk = chunk,
            .record = record,
            .left = if (left) |n| n.retain() else null,
            .right = if (right) |n| n.retain() else null,
            .height = @max(h(left), h(right)) + 1,
            .min_batch = @min(record.batch_id, @min(if (left) |n| n.min_batch else record.batch_id, if (right) |n| n.min_batch else record.batch_id)),
            .max_batch = @max(record.batch_id, @max(if (left) |n| n.max_batch else record.batch_id, if (right) |n| n.max_batch else record.batch_id)),
        };
        chunk.retain();
        return node;
    }
    fn balanced(alloc: Allocator, chunk: *Chunk, record: Record, left: ?*Node, right: ?*Node) !*Node {
        if (@as(i16, h(left)) - h(right) > 1) {
            const l = left.?;
            if (h(l.left) >= h(l.right)) {
                const r = try create(alloc, chunk, record, l.right, right);
                defer r.release();
                return create(alloc, l.chunk, l.record, l.left, r);
            }
            const middle = l.right.?;
            const a = try create(alloc, l.chunk, l.record, l.left, middle.left);
            defer a.release();
            const b = try create(alloc, chunk, record, middle.right, right);
            defer b.release();
            return create(alloc, middle.chunk, middle.record, a, b);
        }
        if (@as(i16, h(right)) - h(left) > 1) {
            const r = right.?;
            if (h(r.right) >= h(r.left)) {
                const l = try create(alloc, chunk, record, left, r.left);
                defer l.release();
                return create(alloc, r.chunk, r.record, l, r.right);
            }
            const middle = r.left.?;
            const a = try create(alloc, chunk, record, left, middle.left);
            defer a.release();
            const b = try create(alloc, r.chunk, r.record, middle.right, r.right);
            defer b.release();
            return create(alloc, middle.chunk, middle.record, a, b);
        }
        return create(alloc, chunk, record, left, right);
    }
    fn put(alloc: Allocator, root: ?*Node, chunk: *Chunk, record: Record) Allocator.Error!*Node {
        const old = root orelse return create(alloc, chunk, record, null, null);
        switch (order(record, old.record)) {
            .eq => return create(alloc, chunk, record, old.left, old.right),
            .lt => {
                const child = try put(alloc, old.left, chunk, record);
                defer child.release();
                return balanced(alloc, old.chunk, old.record, child, old.right);
            },
            .gt => {
                const child = try put(alloc, old.right, chunk, record);
                defer child.release();
                return balanced(alloc, old.chunk, old.record, old.left, child);
            },
        }
    }

    pub fn lookup(root: ?*Node, key: []const u8, hash: u64, sequence: u64) ?Record {
        var cursor = root;
        var found: ?Record = null;
        while (cursor) |node| {
            const cmp = keyOrder(node.record, hash, key);
            if (cmp == .gt or (cmp == .eq and node.record.source_sequence > sequence)) {
                cursor = node.left;
            } else {
                if (cmp == .eq) found = node.record;
                cursor = node.right;
            }
        }
        return found;
    }

    /// Join arbitrary-height AVL subtrees; checkpoint filtering can remove
    /// many levels at once, unlike a single insertion/deletion rotation.
    fn join(alloc: Allocator, left: ?*Node, pivot: *Node, right: ?*Node) Allocator.Error!*Node {
        if (@as(i16, h(left)) > @as(i16, h(right)) + 1) {
            const l = left.?;
            const child = try join(alloc, l.right, pivot, right);
            defer child.release();
            return balanced(alloc, l.chunk, l.record, l.left, child);
        }
        if (@as(i16, h(right)) > @as(i16, h(left)) + 1) {
            const r = right.?;
            const child = try join(alloc, left, pivot, r.left);
            defer child.release();
            return balanced(alloc, r.chunk, r.record, child, r.right);
        }
        return create(alloc, pivot.chunk, pivot.record, left, right);
    }

    fn withoutMin(alloc: Allocator, root: *Node) Allocator.Error!?*Node {
        const left = root.left orelse return if (root.right) |r| r.retain() else null;
        const child = try withoutMin(alloc, left);
        defer if (child) |n| n.release();
        return try balanced(alloc, root.chunk, root.record, child, root.right);
    }

    /// Retain only committed transactions after an exact checkpoint receipt.
    /// Unchanged subtrees and every payload remain shared. Batch identity,
    /// rather than source sequence, preserves same-sequence tail mutations.
    pub fn afterBatch(alloc: Allocator, root: ?*Node, batch: u64) Allocator.Error!?*Node {
        const node = root orelse return null;
        if (node.max_batch <= batch) return null;
        if (node.min_batch > batch) return node.retain();
        const left = try afterBatch(alloc, node.left, batch);
        defer if (left) |n| n.release();
        const right = try afterBatch(alloc, node.right, batch);
        defer if (right) |n| n.release();
        if (node.record.batch_id > batch) return try join(alloc, left, node, right);
        if (left == null) return if (right) |r| r.retain() else null;
        var pivot = right orelse return left.?.retain();
        while (pivot.left) |l| pivot = l;
        const rest = try withoutMin(alloc, right.?);
        defer if (rest) |n| n.release();
        return try join(alloc, left, pivot, rest);
    }

    pub fn collectByShard(root: ?*Node, alloc: Allocator, shards: []std.ArrayListUnmanaged(Record)) Allocator.Error!void {
        const node = root orelse return;
        try collectByShard(node.left, alloc, shards);
        const shard: usize = @intCast(node.record.key_hash & (shards.len - 1));
        const rows = &shards[shard];
        if (rows.items.len != 0 and keyOrder(rows.items[rows.items.len - 1], node.record.key_hash, node.record.key) == .eq) {
            rows.items[rows.items.len - 1] = node.record;
        } else try rows.append(alloc, node.record);
        try collectByShard(node.right, alloc, shards);
    }
};

fn keyOrder(record: Record, hash: u64, key: []const u8) std.math.Order {
    const hashes = std.math.order(record.key_hash, hash);
    return if (hashes == .eq) std.mem.order(u8, record.key, key) else hashes;
}
fn order(a: Record, b: Record) std.math.Order {
    const keys = keyOrder(a, b.key_hash, b.key);
    if (keys != .eq) return keys;
    const sequences = std.math.order(a.source_sequence, b.source_sequence);
    if (sequences != .eq) return sequences;
    const revisions = std.math.order(a.revision, b.revision);
    return if (revisions != .eq) revisions else std.math.order(a.batch_id, b.batch_id);
}

pub fn extend(alloc: Allocator, base: ?*Node, committed: []const u8) !?*Node {
    if (committed.len == 0) return if (base) |node| node.retain() else null;
    const chunk = try Chunk.copy(alloc, committed);
    defer chunk.release();
    var parsed = try wal.Replay.parse(alloc, chunk.bytes);
    defer parsed.deinit();
    if (parsed.committed_bytes != committed.len) return error.UncommittedVectorWalView;
    var root = if (base) |node| node.retain() else null;
    errdefer if (root) |node| node.release();
    for (parsed.records.items) |record| {
        if (record.kind != .upsert and record.kind != .reference and record.kind != .tombstone) continue;
        const next = try Node.put(alloc, root, chunk, record);
        if (root) |node| node.release();
        root = next;
    }
    return root;
}

fn testVersionChain(alloc: Allocator, count: usize) !void {
    var root: ?*Node = null;
    defer if (root) |node| node.release();
    const hash = @import("antfly_vectorindex").vector_block.keyHash("ordered");
    // Same-key monotonic source versions exercise the pathological ordered
    // input of an ordinary BST and every persistent right-side rotation.
    for (0..count) |i| {
        var writer = wal.Writer.init(alloc);
        defer writer.deinit();
        try writer.appendUpsert(i + 1, i + 1, i + 1, "ordered", &.{@floatFromInt(i)});
        try writer.commit(i + 1, i + 1);
        const next = try extend(alloc, root, writer.bytes());
        if (root) |old| {
            try std.testing.expectEqual(@as(u64, i), Node.lookup(old, "ordered", hash, i + 1).?.source_sequence);
            old.release();
        }
        root = next;
    }
    try std.testing.expect(root.?.height < 2 * (std.math.log2_int(usize, count) + 1));
    for (0..count) |i| try std.testing.expectEqual(@as(u64, i + 1), Node.lookup(root, "ordered", hash, i + 1).?.source_sequence);
    var shards = [_]std.ArrayListUnmanaged(Record){.empty} ** 4;
    defer for (&shards) |*shard| shard.deinit(alloc);
    try Node.collectByShard(root, alloc, &shards);
    try std.testing.expectEqual(@as(usize, 1), shards[hash & 3].items.len);
    try std.testing.expectEqual(@as(u64, count), shards[hash & 3].items[0].source_sequence);
}

fn verifyBalance(node: ?*Node) !void {
    const n = node orelse return;
    try std.testing.expect(@abs(@as(i16, Node.h(n.left)) - Node.h(n.right)) <= 1);
    try std.testing.expectEqual(@max(Node.h(n.left), Node.h(n.right)) + 1, n.height);
    try verifyBalance(n.left);
    try verifyBalance(n.right);
}

fn testCheckpointFilter(alloc: Allocator, count: usize) !void {
    var root: ?*Node = null;
    defer if (root) |n| n.release();
    for (0..count) |i| {
        var writer = wal.Writer.init(alloc);
        defer writer.deinit();
        var key_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "key-{d}", .{(i * 17) % count});
        try writer.appendUpsert(i + 1, 7, 1, key, &.{@floatFromInt(i)});
        try writer.commit(i + 1, 7);
        const next = try extend(alloc, root, writer.bytes());
        if (root) |n| n.release();
        root = next;
    }
    for (0..count + 1) |floor| {
        const tail = try Node.afterBatch(alloc, root, floor);
        defer if (tail) |n| n.release();
        try verifyBalance(tail);
        for (0..count) |i| {
            var key_buf: [32]u8 = undefined;
            const key = try std.fmt.bufPrint(&key_buf, "key-{d}", .{(i * 17) % count});
            const hash = @import("antfly_vectorindex").vector_block.keyHash(key);
            const original = Node.lookup(root, key, hash, 7).?;
            const result = Node.lookup(tail, key, hash, 7);
            if (i < floor) {
                try std.testing.expect(result == null);
            } else {
                try std.testing.expectEqual(original.vector_bytes.ptr, result.?.vector_bytes.ptr);
                try std.testing.expectEqual(original.batch_id, result.?.batch_id);
            }
        }
    }
}

test "vector WAL checkpoint filtering shares payloads and preserves AVL balance" {
    try testCheckpointFilter(std.testing.allocator, 128);
}

test "vector WAL checkpoint filtering allocation failures preserve old leases" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testCheckpointFilter, .{@as(usize, 9)});
}

test "vector WAL ordered versions remain balanced" {
    try testVersionChain(std.testing.allocator, 256);
}

test "vector WAL persistent path allocation failures preserve ownership" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testVersionChain, .{@as(usize, 9)});
}

test "vector WAL views retain old revisions and payload ownership" {
    const alloc = std.testing.allocator;
    var first = wal.Writer.init(alloc);
    defer first.deinit();
    try first.appendUpsert(1, 1, 1, "key", &.{1});
    try first.commit(1, 1);
    const old = (try extend(alloc, null, first.bytes())).?;
    defer old.release();
    var second = wal.Writer.initAfterCommitted(alloc, 1, 1);
    defer second.deinit();
    try second.appendTombstone(2, 2, 2, "key");
    try second.commit(2, 2);
    const newer = (try extend(alloc, old, second.bytes())).?;
    defer newer.release();
    const hash = @import("antfly_vectorindex").vector_block.keyHash("key");
    try std.testing.expectEqual(wal.Kind.upsert, Node.lookup(old, "key", hash, 2).?.kind);
    try std.testing.expectEqual(wal.Kind.upsert, Node.lookup(newer, "key", hash, 1).?.kind);
    try std.testing.expectEqual(wal.Kind.tombstone, Node.lookup(newer, "key", hash, 2).?.kind);
    try std.testing.expectEqual(Node.lookup(old, "key", hash, 1).?.vector_bytes.ptr, Node.lookup(newer, "key", hash, 1).?.vector_bytes.ptr);
}
