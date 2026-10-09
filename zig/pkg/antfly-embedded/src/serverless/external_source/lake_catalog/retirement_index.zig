// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Immutable, content-addressed retirement set. Publishing its root through
//! catalog HEAD fences writers; individual object markers cannot provide that
//! atomicity. Unpublished nodes are harmless and never authorize deletion.
const std = @import("std");
const storage = @import("objectstore");
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
const Node = struct { version: u8 = 1, items: []const []const u8 = &.{}, children: ?[16]?Digest = null };
pub const Index = struct {
    arena: std.heap.ArenaAllocator,
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: types.Context,
    cache: std.AutoHashMapUnmanaged(Digest, Node) = .empty,
    cached_bytes: usize = 0,
    pub fn init(a: A, client: storage.Client, bucket: []const u8, prefix: []const u8, context: types.Context) Index {
        return .{ .arena = std.heap.ArenaAllocator.init(a), .client = client, .bucket = bucket, .prefix = prefix, .context = context };
    }
    pub fn deinit(self: *Index) void {
        self.arena.deinit();
        self.* = undefined;
    }
    fn digest(bytes: []const u8) Digest {
        var result: Digest = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
        return result;
    }
    fn nibble(uri: []const u8, depth: usize) usize {
        const hash = digest(uri);
        return if (depth % 2 == 0) hash[depth / 2] >> 4 else hash[depth / 2] & 15;
    }
    fn key(self: Index, a: A, hash: Digest) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/{s}.json", .{ self.prefix, std.fmt.bytesToHex(hash, .lower) });
    }
    fn validate(node: Node) !void {
        if (node.version != 1 or node.items.len > 8 or (node.children != null and node.items.len != 0)) return error.InvalidLakeRetirementIndex;
        for (node.items, 0..) |item, i| {
            if (item.len == 0 or item.len > 4096) return error.InvalidLakeRetirementIndex;
            if (i != 0 and std.mem.order(u8, node.items[i - 1], item) != .lt) return error.InvalidLakeRetirementIndex;
        }
    }
    fn read(self: *Index, hash: Digest) !Node {
        try self.context.ensureActive();
        if (self.cache.get(hash)) |node| return node;
        const a = self.arena.allocator();
        const path = try self.key(a, hash);
        var client = self.client;
        var object = try client.getObject(self.bucket, path, .{ .max_response_bytes = 64 * 1024, .cancellation = types.contextCancellation(&self.context) });
        defer object.deinit(client.allocator);
        if (!std.mem.eql(u8, &digest(object.body), &hash)) return error.InvalidLakeRetirementIndex;
        if (self.cached_bytes + object.body.len > 32 * 1024 * 1024) return error.LakeRetirementIndexBudgetExceeded;
        const node = try std.json.parseFromSliceLeaky(Node, a, object.body, .{ .allocate = .alloc_always });
        try validate(node);
        try self.cache.put(a, hash, node);
        self.cached_bytes += object.body.len;
        return node;
    }
    fn write(self: *Index, node: Node) !Digest {
        try self.context.ensureActive();
        try validate(node);
        const a = self.arena.allocator();
        const bytes = try std.json.Stringify.valueAlloc(a, node, .{});
        const hash = digest(bytes);
        if (self.cache.contains(hash)) return hash;
        const path = try self.key(a, hash);
        var client = self.client;
        var result = client.putObject(self.bucket, path, bytes, .{ .if_none_match = true, .cancellation = types.contextCancellation(&self.context) }) catch |err| {
            // Resolve both preexisting nodes and lost successful responses.
            _ = self.read(hash) catch return err;
            return hash;
        };
        result.deinit(client.allocator);
        if (self.cached_bytes + bytes.len > 32 * 1024 * 1024) return error.LakeRetirementIndexBudgetExceeded;
        // Nodes written by a caller may borrow a short-lived URI slice.
        const owned = try std.json.parseFromSliceLeaky(Node, a, bytes, .{ .allocate = .alloc_always });
        try self.cache.put(a, hash, owned);
        self.cached_bytes += bytes.len;
        return hash;
    }
    pub fn contains(self: *Index, root: ?Digest, uri: []const u8) !bool {
        if (uri.len == 0 or uri.len > 4096) return error.InvalidLakeRetirementIndex;
        var current = root;
        var depth: usize = 0;
        while (current) |hash| {
            const node = try self.read(hash);
            if (node.children) |children| {
                if (depth == 64) return error.InvalidLakeRetirementIndex;
                current = children[nibble(uri, depth)];
                depth += 1;
            } else {
                for (node.items) |item| if (std.mem.eql(u8, item, uri)) return true;
                return false;
            }
        }
        return false;
    }
    pub fn insert(self: *Index, root: ?Digest, uri: []const u8) !Digest {
        if (uri.len == 0 or uri.len > 4096) return error.InvalidLakeRetirementIndex;
        return self.insertAt(root, uri, 0);
    }
    fn insertAt(self: *Index, root: ?Digest, uri: []const u8, depth: usize) anyerror!Digest {
        const node = if (root) |hash| try self.read(hash) else Node{};
        if (node.children) |existing| {
            if (depth == 64) return error.InvalidLakeRetirementIndex;
            var children = existing;
            const slot = nibble(uri, depth);
            children[slot] = try self.insertAt(children[slot], uri, depth + 1);
            return self.write(.{ .children = children });
        }
        for (node.items) |item| if (std.mem.eql(u8, item, uri)) return root.?;
        const a = self.arena.allocator();
        const items = try a.alloc([]const u8, node.items.len + 1);
        @memcpy(items[0..node.items.len], node.items);
        items[node.items.len] = uri;
        if (items.len <= 8) {
            std.mem.sort([]const u8, items, {}, struct {
                fn less(_: void, left: []const u8, right: []const u8) bool {
                    return std.mem.order(u8, left, right) == .lt;
                }
            }.less);
            return self.write(.{ .items = items });
        }
        if (depth == 64) return error.LakeRetirementHashCollision;
        var children: [16]?Digest = @splat(null);
        for (items) |item| {
            const slot = nibble(item, depth);
            children[slot] = try self.insertAt(children[slot], item, depth + 1);
        }
        return self.write(.{ .children = children });
    }
};

test "lake retirement index retains roots across restart and cannot mutate earlier generations" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var index = Index.init(a, memory.client(), "archive", "retired", .{});
    var root: ?Digest = null;
    for (0..129) |i| {
        const uri = try std.fmt.allocPrint(a, "s3://archive/data/{d}.parquet", .{i});
        defer a.free(uri);
        root = try index.insert(root, uri);
    }
    const original = root;
    root = try index.insert(root, "s3://archive/extra.parquet");
    try std.testing.expect(!try index.contains(original, "s3://archive/extra.parquet"));
    index.deinit();
    var reopened = Index.init(a, memory.client(), "archive", "retired", .{});
    defer reopened.deinit();
    for (0..129) |i| {
        const uri = try std.fmt.allocPrint(a, "s3://archive/data/{d}.parquet", .{i});
        defer a.free(uri);
        try std.testing.expect(try reopened.contains(root, uri));
    }
    try std.testing.expect(try reopened.contains(root, "s3://archive/extra.parquet"));
    const same = try reopened.insert(root, "s3://archive/extra.parquet");
    try std.testing.expectEqual(root.?, same);
    try std.testing.expect(!try reopened.contains(root, "s3://archive/missing.parquet"));
}
