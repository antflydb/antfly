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

//! Immutable, content-addressed retirement set. Publishing its root through
//! catalog HEAD fences writers; individual object markers cannot provide that
//! atomicity. Unpublished nodes are harmless and never authorize deletion.
const std = @import("std");
const storage = @import("objectstore");
const types = @import("types.zig");
const A = std.mem.Allocator;
pub const Digest = [32]u8;
const Node = struct { version: u8 = 1, items: []const []const u8 = &.{}, children: ?[16]?Digest = null };
const Hint = struct { hash: Digest, node: Node };
const Hints = struct { version: u8 = 1, nodes: []const Hint };
const Cached = struct { node: Node, durable: bool };
pub const hint_bytes = 128 * 1024;
const decoded_hint_bytes = 1024 * 1024;
fn decodeHint(a: A, bytes: []const u8) ![]const u8 {
    // Legacy plain JSON remains readable. Older readers safely decline a
    // compressed optional hint and fall back to canonical hash-checked nodes.
    if (!std.mem.startsWith(u8, bytes, &.{ 0x1f, 0x8b })) return bytes;
    var input: std.Io.Reader = .fixed(bytes);
    var history: [std.compress.flate.max_window_len]u8 = undefined;
    var decompress: std.compress.flate.Decompress = .init(&input, .gzip, &history);
    const decoded = decompress.reader.allocRemaining(a, .limited(decoded_hint_bytes + 1)) catch |err| switch (err) {
        error.StreamTooLong => return error.LakeRetirementIndexBudgetExceeded,
        else => return err,
    };
    if (decoded.len > decoded_hint_bytes) return error.LakeRetirementIndexBudgetExceeded;
    return decoded;
}
fn encodeHint(a: A, bytes: []const u8) ![]const u8 {
    const buffer = try a.alloc(u8, hint_bytes);
    var output: std.Io.Writer = .fixed(buffer);
    var history: [std.compress.flate.max_window_len]u8 = undefined;
    var compressor = try std.compress.flate.Compress.init(&output, &history, .gzip, .default);
    compressor.writer.writeAll(bytes) catch return error.LakeRetirementIndexBudgetExceeded;
    compressor.finish() catch return error.LakeRetirementIndexBudgetExceeded;
    return output.buffered();
}
pub const Index = struct {
    arena: std.heap.ArenaAllocator,
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: types.Context,
    cache: std.AutoHashMapUnmanaged(Digest, Cached) = .empty,
    cached_bytes: usize = 0,
    hint_digest: ?Digest = null,
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
    fn hintContext(self: *Index) types.Context {
        var context = self.context;
        // Optional warming must leave time for the required read/checkpoint.
        const deadline = @import("antfly_platform").time.monotonicNs() +| std.time.ns_per_s;
        context.deadline_ns = @min(context.deadline_ns orelse deadline, deadline);
        return context;
    }
    /// One bounded, replaceable hint object survives worker restarts. Every
    /// node is independently authenticated; missing/stale hints are harmless.
    /// Callers fall back to read() on any hint failure. No catalog wire format
    /// or deletion authority depends on the cache.
    pub fn loadHints(self: *Index) !void {
        try self.context.ensureActive();
        const a = self.arena.allocator();
        const path = try std.fmt.allocPrint(a, "{s}/warm-v1.json", .{self.prefix});
        var context = self.hintContext();
        var object = try self.client.getObject(self.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = hint_bytes, .cancellation = types.contextCancellation(&context) });
        defer object.deinit(self.client.allocator);
        const hints = try std.json.parseFromSliceLeaky(Hints, a, try decodeHint(a, object.body), .{ .allocate = .alloc_always });
        if (hints.version != 1 or hints.nodes.len > 256) return error.InvalidLakeRetirementIndex;
        // Validate the entire hint before making any of it available.
        for (hints.nodes) |hint| {
            try self.context.ensureActive();
            try validate(hint.node);
            const bytes = try std.json.Stringify.valueAlloc(a, hint.node, .{});
            defer a.free(bytes);
            if (!std.mem.eql(u8, &digest(bytes), &hint.hash)) return error.InvalidLakeRetirementIndex;
        }
        for (hints.nodes) |hint| {
            if (self.cache.contains(hint.hash)) continue;
            const bytes = try std.json.Stringify.valueAlloc(a, hint.node, .{});
            defer a.free(bytes);
            if (self.cached_bytes + bytes.len > 32 * 1024 * 1024) return error.LakeRetirementIndexBudgetExceeded;
            try self.cache.put(a, hint.hash, .{ .node = hint.node, .durable = false });
            self.cached_bytes += bytes.len;
        }
        self.hint_digest = digest(object.body);
    }
    pub fn saveHints(self: *Index, roots: []const ?Digest) !void {
        try self.context.ensureActive();
        const a = self.arena.allocator();
        var queue: std.ArrayList(Digest) = .empty;
        for (0..roots.len) |i| if (roots[roots.len - 1 - i]) |hash| try queue.append(a, hash);
        var seen: std.AutoHashMapUnmanaged(Digest, void) = .empty;
        var hints: std.ArrayList(Hint) = .empty;
        var bytes_used: usize = 256;
        // Roots are prioritized, so repeated completed-manifest lookups do not
        // lose their leaves to a larger object-membership tree. Export cached
        // nodes only; warming never fetches additional source objects.
        while (queue.items.len != 0 and hints.items.len < 256) {
            try self.context.ensureActive();
            const hash = queue.pop().?;
            if ((try seen.getOrPut(a, hash)).found_existing) continue;
            const node = (self.cache.get(hash) orelse continue).node;
            const bytes = try std.json.Stringify.valueAlloc(a, node, .{});
            defer a.free(bytes);
            // Digest-array framing is bounded by 256 bytes per entry.
            if (bytes.len + 256 > decoded_hint_bytes - bytes_used) continue;
            bytes_used += bytes.len + 256;
            try hints.append(a, .{ .hash = hash, .node = node });
            if (node.children) |children| for (0..children.len) |i| if (children[children.len - 1 - i]) |next| try queue.append(a, next);
        }
        if (hints.items.len == 0) return;
        const plain = try std.json.Stringify.valueAlloc(a, Hints{ .nodes = hints.items }, .{});
        if (plain.len > decoded_hint_bytes) return error.LakeRetirementIndexBudgetExceeded;
        try self.context.ensureActive();
        const bytes = try encodeHint(a, plain);
        const hash = digest(bytes);
        if (self.hint_digest) |previous| if (std.mem.eql(u8, &hash, &previous)) return;
        var context = self.hintContext();
        const path = try std.fmt.allocPrint(a, "{s}/warm-v1.json", .{self.prefix});
        // Last-writer-wins is intentional: this bounded cache owns no progress,
        // roots or authority, and all restored nodes retain their hash checks.
        var stored = try self.client.putObject(self.bucket, path, bytes, .{ .cancellation = types.contextCancellation(&context) });
        stored.deinit(self.client.allocator);
        self.hint_digest = hash;
    }
    fn read(self: *Index, hash: Digest) !Node {
        try self.context.ensureActive();
        if (self.cache.get(hash)) |cached| return cached.node;
        return self.readCanonical(hash);
    }
    fn readCanonical(self: *Index, hash: Digest) !Node {
        try self.context.ensureActive();
        if (self.cache.get(hash)) |cached| if (cached.durable) return cached.node;
        const a = self.arena.allocator();
        const path = try self.key(a, hash);
        var client = self.client;
        // The content-addressed name and checked body hash are the authority;
        // a separate mutable-metadata probe adds no protection to this read.
        var object = try client.getObject(self.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = 64 * 1024, .cancellation = types.contextCancellation(&self.context) });
        defer object.deinit(client.allocator);
        if (!std.mem.eql(u8, &digest(object.body), &hash)) return error.InvalidLakeRetirementIndex;
        const added_bytes = if (self.cache.contains(hash)) 0 else object.body.len;
        if (self.cached_bytes + added_bytes > 32 * 1024 * 1024) return error.LakeRetirementIndexBudgetExceeded;
        const node = try std.json.parseFromSliceLeaky(Node, a, object.body, .{ .allocate = .alloc_always });
        try validate(node);
        try self.cache.put(a, hash, .{ .node = node, .durable = true });
        self.cached_bytes += added_bytes;
        return node;
    }
    fn write(self: *Index, node: Node) !Digest {
        try self.context.ensureActive();
        try validate(node);
        const a = self.arena.allocator();
        const bytes = try std.json.Stringify.valueAlloc(a, node, .{});
        const hash = digest(bytes);
        // A hint authenticates content, never a successful durable upload.
        if (self.cache.get(hash)) |cached| if (cached.durable) return hash;
        const path = try self.key(a, hash);
        var client = self.client;
        var result = client.putObject(self.bucket, path, bytes, .{ .if_none_match = true, .cancellation = types.contextCancellation(&self.context) }) catch |err| {
            // Resolve both preexisting nodes and lost successful responses.
            _ = self.readCanonical(hash) catch return err;
            return hash;
        };
        result.deinit(client.allocator);
        const added_bytes = if (self.cache.contains(hash)) 0 else bytes.len;
        if (self.cached_bytes + added_bytes > 32 * 1024 * 1024) return error.LakeRetirementIndexBudgetExceeded;
        // Nodes written by a caller may borrow a short-lived URI slice.
        const owned = try std.json.parseFromSliceLeaky(Node, a, bytes, .{ .allocate = .alloc_always });
        try self.cache.put(a, hash, .{ .node = owned, .durable = true });
        self.cached_bytes += added_bytes;
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
    /// Apply one bounded set update bottom-up. Only final changed nodes are
    /// published; intermediate roots from individual insertions are not durable
    /// inputs and need not amplify each checkpoint into hundreds of PUTs.
    /// The returned root has the same canonical wire format as insert(). A
    /// caller must publish its checkpoint only after this method succeeds.
    pub fn insertMany(self: *Index, root: ?Digest, uris: []const []const u8) !?Digest {
        try self.context.ensureActive();
        if (uris.len > 4096) return error.LakeRetirementIndexBudgetExceeded;
        for (uris) |uri| if (uri.len == 0 or uri.len > 4096) return error.InvalidLakeRetirementIndex;
        return self.insertManyAt(root, uris, 0);
    }
    fn insertManyAt(self: *Index, root: ?Digest, uris: []const []const u8, depth: usize) anyerror!?Digest {
        try self.context.ensureActive();
        if (uris.len == 0) return root;
        const a = self.arena.allocator();
        const node = if (root) |hash| try self.read(hash) else Node{};
        if (node.children) |existing| {
            if (depth == 64) return error.InvalidLakeRetirementIndex;
            var groups: [16]std.ArrayList([]const u8) = @splat(.empty);
            for (uris) |uri| try groups[nibble(uri, depth)].append(a, uri);
            var children = existing;
            for (&children, groups) |*child, group| child.* = try self.insertManyAt(child.*, group.items, depth + 1);
            // Returning an unchanged input root introduces no new durability
            // claim. A hint alone must still never acknowledge a new node.
            if (std.meta.eql(children, existing)) return root;
            return try self.write(.{ .children = children });
        }
        var items: std.ArrayList([]const u8) = .empty;
        try items.appendSlice(a, node.items);
        try items.appendSlice(a, uris);
        std.mem.sort([]const u8, items.items, {}, struct {
            fn less(_: void, left: []const u8, right: []const u8) bool {
                return std.mem.order(u8, left, right) == .lt;
            }
        }.less);
        var count: usize = 0;
        for (items.items) |uri| {
            if (count != 0 and std.mem.eql(u8, items.items[count - 1], uri)) continue;
            items.items[count] = uri;
            count += 1;
        }
        if (root != null and count == node.items.len) return root;
        if (count <= 8) return try self.write(.{ .items = items.items[0..count] });
        if (depth == 64) return error.LakeRetirementHashCollision;
        var groups: [16]std.ArrayList([]const u8) = @splat(.empty);
        for (items.items[0..count]) |uri| try groups[nibble(uri, depth)].append(a, uri);
        var children: [16]?Digest = @splat(null);
        for (&children, groups) |*child, group| child.* = try self.insertManyAt(null, group.items, depth + 1);
        return try self.write(.{ .children = children });
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

test "external lake retirement batch preserves canonical roots with bounded object writes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var sequential = Index.init(a, memory.client(), "archive", "sequential", .{});
    defer sequential.deinit();
    var batch = Index.init(a, memory.client(), "archive", "batch", .{});
    defer batch.deinit();
    var uris: [64][]const u8 = undefined;
    var expected: ?Digest = null;
    for (&uris, 0..) |*uri, i| {
        uri.* = try std.fmt.allocPrint(ca, "s3://archive/data/{d}.parquet", .{i});
        expected = try sequential.insert(expected, uri.*);
    }
    const root = try batch.insertMany(null, &uris);
    try std.testing.expectEqual(expected.?, root.?);
    var client = memory.client();
    var single_objects = try client.listObjects("archive", .{ .prefix = "sequential/", .max_keys = 1000 });
    defer single_objects.deinit(a);
    var batch_objects = try client.listObjects("archive", .{ .prefix = "batch/", .max_keys = 1000 });
    defer batch_objects.deinit(a);
    try std.testing.expect(batch_objects.entries.len * 2 < single_objects.entries.len);
    var reopened = Index.init(a, memory.client(), "archive", "batch", .{});
    defer reopened.deinit();
    for (uris) |uri| try std.testing.expect(try reopened.contains(root, uri));
    const extended = try reopened.insertMany(root, &.{ uris[0], "s3://archive/new.parquet", uris[0] });
    try std.testing.expect(!try batch.contains(root, "s3://archive/new.parquet"));
    try std.testing.expect(try reopened.contains(extended, "s3://archive/new.parquet"));
    try std.testing.expectEqual(extended.?, (try reopened.insertMany(extended, &uris)).?);
    var canceled = Index.init(a, memory.client(), "archive", "batch", .{ .deadline_ns = 0 });
    defer canceled.deinit();
    try std.testing.expectError(error.DeadlineExceeded, canceled.insertMany(root, &.{"s3://archive/canceled.parquet"}));
    try std.testing.expect(!try reopened.contains(root, "s3://archive/canceled.parquet"));
    // Unpinned immutable reads still reject a replaced object's body, even
    // when its replacement is a syntactically valid empty node.
    const root_key = try batch.key(ca, root.?);
    var forged = try client.putObject("archive", root_key, "{}", .{});
    forged.deinit(a);
    var cold = Index.init(a, memory.client(), "archive", "batch", .{});
    defer cold.deinit();
    try std.testing.expectError(error.InvalidLakeRetirementIndex, cold.contains(root, uris[0]));
}

test "external lake retirement hints survive cold turns without becoming authority" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var index = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer index.deinit();
    var uris: [64][]const u8 = undefined;
    for (&uris, 0..) |*uri, i| uri.* = try std.fmt.allocPrint(ca, "s3://archive/{d}.parquet", .{i});
    const root = try index.insertMany(null, &uris);
    try index.saveHints(&.{root});
    var cold = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer cold.deinit();
    memory.resetOperationCount();
    try cold.loadHints();
    const reads = memory.operationCount();
    try std.testing.expectEqual(@as(u64, 1), reads);
    for (uris) |uri| try std.testing.expect(try cold.contains(root, uri));
    try std.testing.expectEqual(reads, memory.operationCount());
    // An unchanged turn need not rewrite the mutable hint object.
    try cold.saveHints(&.{root});
    try std.testing.expectEqual(reads, memory.operationCount());

    const next = try index.insertMany(root, &.{"s3://archive/later.parquet"});
    try std.testing.expect(!try cold.contains(root, "s3://archive/later.parquet"));
    try std.testing.expect(try cold.contains(next, "s3://archive/later.parquet"));
    var client = memory.client();
    const forged = try std.json.Stringify.valueAlloc(ca, Hints{ .nodes = &.{.{ .hash = root.?, .node = .{} }} }, .{});
    var stored = try client.putObject("archive", "reachable/warm-v1.json", forged, .{});
    stored.deinit(a);
    var corrupt = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer corrupt.deinit();
    try std.testing.expectError(error.InvalidLakeRetirementIndex, corrupt.loadHints());
    try std.testing.expectEqual(@as(usize, 0), corrupt.cache.count());
    // A corrupt optional cache cannot turn a present member into a negative.
    try std.testing.expect(try corrupt.contains(root, uris[0]));
    try client.deleteObject("archive", "reachable/warm-v1.json", .{});
    var missing = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer missing.deinit();
    missing.loadHints() catch {};
    try std.testing.expect(try missing.contains(root, uris[0]));
    index.context.deadline_ns = 0;
    memory.resetOperationCount();
    try std.testing.expectError(error.DeadlineExceeded, index.loadHints());
    try std.testing.expectError(error.DeadlineExceeded, index.saveHints(&.{root}));
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
}

test "external lake retirement hints prioritize a repeated manifest cohort within stored bounds" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var index = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer index.deinit();
    var uris: [384][]const u8 = undefined;
    const prefix: [192]u8 = @splat('x');
    for (&uris, 0..) |*uri, i| uri.* = try std.fmt.allocPrint(ca, "gs://archive/{s}/manifest-{d}.avro", .{ prefix, i });
    const manifests = try index.insertMany(null, &uris);
    var members: ?Digest = null;
    for (0..4) |batch| {
        var files: [256][]const u8 = undefined;
        for (&files, 0..) |*file, i| file.* = try std.fmt.allocPrint(ca, "gs://archive/{s}/data-{d}.parquet", .{ prefix, batch * files.len + i });
        members = try index.insertMany(members, &files);
    }
    try index.saveHints(&.{ manifests, members });
    var cold = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer cold.deinit();
    try cold.loadHints();
    memory.resetOperationCount();
    for (uris) |uri| try std.testing.expect(try cold.contains(manifests, uri));
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
}

test "external lake retirement hints stay bounded and fall back for uncached nodes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var index = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer index.deinit();
    var uris: [2048][]const u8 = undefined;
    const padding: [512]u8 = @splat('x');
    for (&uris, 0..) |*uri, i| uri.* = try std.fmt.allocPrint(ca, "s3://archive/{s}/{d}.parquet", .{ padding, i });
    const root = try index.insertMany(null, &uris);
    try index.saveHints(&.{root});
    var client = memory.client();
    var object = try client.getObject("archive", "reachable/warm-v1.json", .{});
    defer object.deinit(a);
    try std.testing.expect(object.body.len <= hint_bytes);
    var cold = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer cold.deinit();
    try cold.loadHints();
    try std.testing.expect(cold.cache.count() < index.cache.count());
    memory.resetOperationCount();
    for (uris) |uri| try std.testing.expect(try cold.contains(root, uri));
    try std.testing.expect(memory.operationCount() > 0);
    try std.testing.expect(!try cold.contains(root, "s3://archive/missing.parquet"));
}

test "external lake retirement hints bound decoding and retain legacy fallback" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    var index = Index.init(a, client, "archive", "reachable", .{});
    defer index.deinit();
    const uri = "gs://archive/live.parquet";
    const root = (try index.insertMany(null, &.{uri})).?;
    const legacy = try std.json.Stringify.valueAlloc(ca, Hints{ .nodes = &.{.{ .hash = root, .node = index.cache.get(root).?.node }} }, .{});
    var stored = try client.putObject("archive", "reachable/warm-v1.json", legacy, .{});
    stored.deinit(a);
    var old = Index.init(a, client, "archive", "reachable", .{});
    defer old.deinit();
    try old.loadHints();
    memory.resetOperationCount();
    try std.testing.expect(try old.contains(root, uri));
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());

    const compressed = try encodeHint(ca, legacy);
    stored = try client.putObject("archive", "reachable/warm-v1.json", compressed[0 .. compressed.len / 2], .{});
    stored.deinit(a);
    var truncated = Index.init(a, client, "archive", "reachable", .{});
    defer truncated.deinit();
    const rejected = if (truncated.loadHints()) |_| false else |_| true;
    try std.testing.expect(rejected);
    try std.testing.expectEqual(@as(usize, 0), truncated.cache.count());
    try std.testing.expect(try truncated.contains(root, uri));

    const bomb = try ca.alloc(u8, decoded_hint_bytes + 32);
    @memset(bomb, 'x');
    stored = try client.putObject("archive", "reachable/warm-v1.json", try encodeHint(ca, bomb), .{});
    stored.deinit(a);
    var oversized = Index.init(a, client, "archive", "reachable", .{});
    defer oversized.deinit();
    try std.testing.expectError(error.LakeRetirementIndexBudgetExceeded, oversized.loadHints());
    try std.testing.expectEqual(@as(usize, 0), oversized.cache.count());

    const nodes = try ca.alloc(Hint, 257);
    @memset(nodes, .{ .hash = root, .node = index.cache.get(root).?.node });
    const excessive = try std.json.Stringify.valueAlloc(ca, Hints{ .nodes = nodes }, .{});
    stored = try client.putObject("archive", "reachable/warm-v1.json", try encodeHint(ca, excessive), .{});
    stored.deinit(a);
    var excessive_nodes = Index.init(a, client, "archive", "reachable", .{});
    defer excessive_nodes.deinit();
    try std.testing.expectError(error.InvalidLakeRetirementIndex, excessive_nodes.loadHints());
    try std.testing.expectEqual(@as(usize, 0), excessive_nodes.cache.count());
}

test "external lake retirement hints make repeated bulk updates read only" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var index = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer index.deinit();
    var uris: [128][]const u8 = undefined;
    for (&uris, 0..) |*uri, i| uri.* = try std.fmt.allocPrint(ca, "gs://archive/{d}.parquet", .{i});
    const root = try index.insertMany(null, &uris);
    try index.saveHints(&.{root});
    var cold = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer cold.deinit();
    try cold.loadHints();
    memory.resetOperationCount();
    try std.testing.expectEqual(root, try cold.insertMany(root, &uris));
    try std.testing.expectEqual(@as(u64, 0), memory.operationCount());
}

test "external lake retirement hints cannot acknowledge an unpersisted node" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    const node: Node = .{ .items = &.{"s3://archive/future.parquet"} };
    const body = try std.json.Stringify.valueAlloc(ca, node, .{});
    const hash = Index.digest(body);
    const hints = try std.json.Stringify.valueAlloc(ca, Hints{ .nodes = &.{.{ .hash = hash, .node = node }} }, .{});
    var hint = try client.putObject("archive", "reachable/warm-v1.json", hints, .{});
    hint.deinit(a);
    var vtable = client.vtable.*;
    vtable.put_object = struct {
        fn put(_: *anyopaque, _: A, _: []const u8, _: []const u8, _: []const u8, _: storage.PutOptions) anyerror!storage.PutResult {
            return error.InjectedHintUploadFailure;
        }
    }.put;
    var failing_client = client;
    failing_client.vtable = &vtable;
    var failed = Index.init(a, failing_client, "archive", "reachable", .{});
    defer failed.deinit();
    try failed.loadHints();
    try std.testing.expectError(error.InjectedHintUploadFailure, failed.insertMany(null, node.items));
    var writer = Index.init(a, client, "archive", "reachable", .{});
    defer writer.deinit();
    try writer.loadHints();
    try std.testing.expectEqual(hash, (try writer.insertMany(null, node.items)).?);
    var cold = Index.init(a, client, "archive", "reachable", .{});
    defer cold.deinit();
    try std.testing.expect(try cold.contains(hash, node.items[0]));
    // A failed response is acceptable only after a real canonical read proves
    // the object exists. The optional hint can never resolve the lost ACK.
    try std.testing.expectEqual(hash, (try failed.insertMany(null, node.items)).?);
}
