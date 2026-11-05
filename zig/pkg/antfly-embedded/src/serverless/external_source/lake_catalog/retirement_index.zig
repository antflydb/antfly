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
const hint_nodes = 2048;
const upload_width = 4;
const upload_buffer_bytes = 1024 * 1024;
// A process-wide cap on optional parallel slots. Saturated callers stay inline.
var upload_slots: std.atomic.Value(usize) = .init(0);
const Upload = struct { hash: Digest, path: []const u8, bytes: []const u8 };
const Uploads = struct {
    items: std.ArrayList(Upload) = .empty,
    seen: std.AutoHashMapUnmanaged(Digest, void) = .empty,
};
fn decodeHint(a: A, bytes: []const u8) ![]const u8 {
    if (!std.mem.startsWith(u8, bytes, &.{ 0x1f, 0x8b })) return error.InvalidLakeRetirementIndex;
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
    errdefer a.free(buffer);
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
    staging: ?*Uploads = null,
    upload_buffers: ?[]u8 = null,
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
    fn hintContext(self: *Index) !types.Context {
        var context = self.context;
        const now = @import("antfly_platform").time.monotonicNs();
        // Optional work must retain a required-read/checkpoint lane. A live
        // parent with less than that reserve declines warming without I/O.
        const reserve = std.time.ns_per_s;
        const limit = if (context.deadline_ns) |parent| parent -| reserve else std.math.maxInt(u64);
        if (now >= limit) return error.LakeRetirementHintBudgetExceeded;
        context.deadline_ns = @min(limit, now +| std.time.ns_per_s);
        return context;
    }
    /// One bounded, replaceable hint object survives worker restarts. Every
    /// node is independently authenticated; missing/stale hints are harmless.
    /// Callers fall back to read() on any hint failure. No catalog wire format
    /// or deletion authority depends on the cache.
    pub fn loadHints(self: *Index) !void {
        try self.context.ensureActive();
        var context = try self.hintContext();
        const a = self.arena.allocator();
        const path = try std.fmt.allocPrint(a, "{s}/warm-v1.json", .{self.prefix});
        var object = try self.client.getObject(self.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = hint_bytes, .cancellation = types.contextCancellation(&context) });
        defer object.deinit(self.client.allocator);
        const hints = try std.json.parseFromSliceLeaky(Hints, a, try decodeHint(a, object.body), .{ .allocate = .alloc_always });
        if (hints.version != 1 or hints.nodes.len > hint_nodes) return error.InvalidLakeRetirementIndex;
        // Validate the entire hint before making any of it available.
        for (hints.nodes) |hint| {
            try context.ensureActive();
            try validate(hint.node);
            const bytes = try std.json.Stringify.valueAlloc(a, hint.node, .{});
            defer a.free(bytes);
            if (!std.mem.eql(u8, &digest(bytes), &hint.hash)) return error.InvalidLakeRetirementIndex;
        }
        for (hints.nodes) |hint| {
            try context.ensureActive();
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
        var context = try self.hintContext();
        const a = self.arena.allocator();
        var queue: std.ArrayList(Digest) = .empty;
        for (0..roots.len) |i| if (roots[roots.len - 1 - i]) |hash| try queue.append(a, hash);
        var seen: std.AutoHashMapUnmanaged(Digest, void) = .empty;
        var hints: std.ArrayList(Hint) = .empty;
        var bytes_used: usize = 256;
        // Roots are prioritized, so repeated completed-manifest lookups do not
        // lose their leaves to a larger object-membership tree. Export cached
        // nodes only; warming never fetches additional source objects.
        while (queue.items.len != 0 and hints.items.len < hint_nodes) {
            try context.ensureActive();
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
        // Select by useful bytes rather than leaving most of the stored
        // budget unused at an arbitrary small node count. Incompressible
        // cohorts shrink deterministically; optional warming never expands
        // the stored or decoded limits and never drops the whole hint merely
        // because the first candidate did not compress enough.
        const bytes = encoded: {
            var count = hints.items.len;
            while (true) {
                try context.ensureActive();
                const plain = try std.json.Stringify.valueAlloc(a, Hints{ .nodes = hints.items[0..count] }, .{});
                defer a.free(plain);
                if (plain.len > decoded_hint_bytes) return error.LakeRetirementIndexBudgetExceeded;
                const compressed = encodeHint(a, plain) catch |err| switch (err) {
                    error.LakeRetirementIndexBudgetExceeded => {
                        if (count == 1) return err;
                        count = @max(1, count / 2);
                        continue;
                    },
                    else => return err,
                };
                break :encoded compressed;
            }
        };
        const hash = digest(bytes);
        if (self.hint_digest) |previous| if (std.mem.eql(u8, &hash, &previous)) return;
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
        if (self.staging) |pending| {
            if ((try pending.seen.getOrPut(a, hash)).found_existing) return hash;
            try pending.items.append(a, .{ .hash = hash, .path = path, .bytes = bytes });
            try self.cacheWritten(bytes, hash, false);
            return hash;
        }
        try self.persist(self.client, .{ .hash = hash, .path = path, .bytes = bytes });
        try self.cacheWritten(bytes, hash, true);
        return hash;
    }
    fn persist(self: *const Index, original: storage.Client, upload: Upload) !void {
        try self.context.ensureActive();
        var client = original;
        var result = client.putObject(self.bucket, upload.path, upload.bytes, .{ .if_none_match = true, .cancellation = types.contextCancellation(&self.context) }) catch |err| {
            // Resolve both preexisting nodes and lost successful responses.
            try self.context.ensureActive();
            var existing = client.getObject(self.bucket, upload.path, .{ .skip_metadata_probe = true, .max_response_bytes = 64 * 1024, .cancellation = types.contextCancellation(&self.context) }) catch |verification_error| {
                try self.context.ensureActive();
                if (verification_error == error.OutOfMemory) return verification_error;
                return err;
            };
            defer existing.deinit(client.allocator);
            try self.context.ensureActive();
            if (!std.mem.eql(u8, existing.body, upload.bytes)) return error.InvalidLakeRetirementIndex;
            return;
        };
        result.deinit(client.allocator);
        try self.context.ensureActive();
    }
    fn cacheWritten(self: *Index, bytes: []const u8, hash: Digest, durable: bool) !void {
        const a = self.arena.allocator();
        const added_bytes = if (self.cache.contains(hash)) 0 else bytes.len;
        if (self.cached_bytes + added_bytes > 32 * 1024 * 1024) return error.LakeRetirementIndexBudgetExceeded;
        // Nodes written by a caller may borrow a short-lived URI slice.
        const owned = try std.json.parseFromSliceLeaky(Node, a, bytes, .{ .allocate = .alloc_always });
        try self.cache.put(a, hash, .{ .node = owned, .durable = durable });
        self.cached_bytes += added_bytes;
    }
    fn flush(self: *Index, uploads: []const Upload) !void {
        if (uploads.len == 0) return;
        var width: usize = 1;
        var reservation: usize = 0;
        if (self.context.parallel_object_io and self.client.vtable.supports_concurrent_requests and self.context.io != null and uploads.len > 1) {
            var current = upload_slots.load(.monotonic);
            while (current < upload_width) {
                const count = @min(uploads.len, upload_width - current);
                if (upload_slots.cmpxchgWeak(current, current + count, .acquire, .monotonic)) |changed| {
                    current = changed;
                } else {
                    width = count;
                    reservation = count;
                    break;
                }
            }
        }
        defer {
            if (reservation != 0) _ = upload_slots.fetchSub(reservation, .release);
        }
        if (width > 1 and self.upload_buffers == null) self.upload_buffers = self.arena.allocator().alloc(u8, upload_width * upload_buffer_bytes) catch null;
        if (self.upload_buffers == null) width = 1;
        const Worker = struct {
            fn run(owner: *const Index, upload: Upload, buffer: []u8, failure: *?anyerror) void {
                var temporary: std.heap.FixedBufferAllocator = .init(buffer);
                var client = owner.client;
                client.allocator = temporary.allocator();
                owner.persist(client, upload) catch |err| {
                    failure.* = err;
                };
            }
        };
        var begin: usize = 0;
        while (begin < uploads.len) {
            try self.context.ensureActive();
            const end = @min(uploads.len, begin + width);
            var failures: [upload_width]?anyerror = @splat(null);
            if (width > 1) {
                const io = self.context.io.?;
                var group: std.Io.Group = .init;
                defer group.cancel(io);
                for (uploads[begin..end], 0..) |upload, i| {
                    const buffer = self.upload_buffers.?[i * upload_buffer_bytes ..][0..upload_buffer_bytes];
                    group.concurrent(io, Worker.run, .{ self, upload, buffer, &failures[i] }) catch Worker.run(self, upload, buffer, &failures[i]);
                }
                try group.await(io);
            } else for (uploads[begin..end], 0..) |upload, i| self.persist(self.client, upload) catch |err| {
                failures[i] = err;
            };
            for (uploads[begin..end], failures[0 .. end - begin]) |upload, failure| {
                if (failure) |err| {
                    // A bounded worker buffer must not turn a previously valid
                    // provider operation into an OOM failure. Retry on the
                    // caller lane, retaining conditional-write verification.
                    if (err != error.OutOfMemory or width == 1) return err;
                    try self.persist(self.client, upload);
                }
                self.cache.getPtr(upload.hash).?.durable = true;
            }
            begin = end;
        }
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
        std.debug.assert(self.staging == null);
        var pending: Uploads = .{};
        self.staging = &pending;
        const updated = self.insertManyAt(root, uris, 0) catch |err| {
            self.staging = null;
            return err;
        };
        self.staging = null;
        // Immutable children may upload in any order while unreachable. No
        // root escapes until every upload and verification has drained.
        try self.flush(pending.items.items);
        return updated;
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
    var stored = try client.putObject("archive", "reachable/warm-v1.json", try encodeHint(ca, forged), .{});
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
    for (0..1024) |i| {
        const uri = try std.fmt.allocPrint(ca, "gs://archive/{s}/data-{d}.parquet", .{ prefix, i });
        try std.testing.expect(try cold.contains(members, uri));
    }
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

test "external lake retirement hints bound decoding and reject obsolete encodings" {
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
    const plain = try std.json.Stringify.valueAlloc(ca, Hints{ .nodes = &.{.{ .hash = root, .node = index.cache.get(root).?.node }} }, .{});
    var stored = try client.putObject("archive", "reachable/warm-v1.json", plain, .{});
    stored.deinit(a);
    var obsolete = Index.init(a, client, "archive", "reachable", .{});
    defer obsolete.deinit();
    try std.testing.expectError(error.InvalidLakeRetirementIndex, obsolete.loadHints());
    memory.resetOperationCount();
    try std.testing.expect(try obsolete.contains(root, uri));
    try std.testing.expect(memory.operationCount() > 0);

    const compressed = try encodeHint(ca, plain);
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

    const nodes = try ca.alloc(Hint, hint_nodes + 1);
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
    var hint = try client.putObject("archive", "reachable/warm-v1.json", try encodeHint(ca, hints), .{});
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

const ParallelProbe = struct {
    memory: *storage.MemoryClient,
    allocator: A,
    io: std.Io,
    mutex: std.atomic.Mutex = .unlocked,
    active: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),
    released: std.atomic.Value(u32) = .init(0),
    cancelled: std.atomic.Value(bool) = .init(false),
    overlap: bool = false,
    lost_ack: bool = false,
    cancel_put: bool = false,
    exhaust_workers: bool = false,
    exhaust_verification: bool = false,
    fn lock(self: *@This()) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn canceled(raw: *const anyopaque) bool {
        const self: *const @This() = @ptrCast(@alignCast(raw));
        return self.cancelled.load(.acquire);
    }
    fn put(raw: *anyopaque, a: A, bucket: []const u8, path: []const u8, bytes: []const u8, options: storage.PutOptions) !storage.PutResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.exhaust_workers and a.vtable != self.allocator.vtable) return error.OutOfMemory;
        const active = self.active.fetchAdd(1, .acq_rel) + 1;
        defer _ = self.active.fetchSub(1, .release);
        var peak = self.peak.load(.monotonic);
        while (peak < active) peak = self.peak.cmpxchgWeak(peak, active, .monotonic, .monotonic) orelse active;
        if (self.overlap and self.released.load(.acquire) == 0) {
            if (active == upload_width) {
                self.released.store(1, .release);
                self.io.futexWake(u32, &self.released.raw, upload_width);
            } else {
                const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
                while (self.released.load(.acquire) == 0) {
                    if (@import("antfly_platform").time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
                    try self.io.futexWaitTimeout(u32, &self.released.raw, 0, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } });
                }
            }
        }
        self.lock();
        defer self.mutex.unlock();
        var client = self.memory.client();
        client.allocator = a;
        var result = try client.putObject(bucket, path, bytes, options);
        if (self.cancel_put) {
            self.cancelled.store(true, .release);
            result.deinit(a);
            return error.Canceled;
        }
        if (self.lost_ack) {
            result.deinit(a);
            return error.ConnectionResetByPeer;
        }
        return result;
    }
    fn get(raw: *anyopaque, a: A, bucket: []const u8, path: []const u8, options: storage.GetOptions) !storage.GetResult {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.exhaust_verification and a.vtable != self.allocator.vtable) return error.OutOfMemory;
        self.lock();
        defer self.mutex.unlock();
        var client = self.memory.client();
        client.allocator = a;
        return client.getObject(bucket, path, options);
    }
};

test "external lake retirement parallel bulk uploads confirm lost responses and bound overlap" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var executor = std.Io.Threaded.init(a, .{ .async_limit = .unlimited });
    defer executor.deinit();
    const io = executor.io();
    var probe: ParallelProbe = .{ .memory = &memory, .allocator = a, .io = io, .overlap = true, .lost_ack = true };
    var vtable = memory.client().vtable.*;
    vtable.supports_concurrent_requests = true;
    vtable.put_object = ParallelProbe.put;
    vtable.get_object = ParallelProbe.get;
    const client: storage.Client = .{ .allocator = a, .ptr = &probe, .vtable = &vtable };
    var index = Index.init(a, client, "archive", "parallel", .{ .io = io, .parallel_object_io = true });
    defer index.deinit();
    var uris: [64][]const u8 = undefined;
    for (&uris, 0..) |*uri, i| uri.* = try std.fmt.allocPrint(arena.allocator(), "gs://archive/{d}.parquet", .{i});
    const root = try index.insertMany(null, &uris);
    try std.testing.expectEqual(@as(usize, upload_width), probe.peak.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), probe.active.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), upload_slots.load(.acquire));
    var cold = Index.init(a, memory.client(), "archive", "parallel", .{});
    defer cold.deinit();
    for (uris) |uri| try std.testing.expect(try cold.contains(root, uri));
    // Upload succeeds, its response is lost, and the bounded verification
    // buffer is exhausted. The caller lane must still confirm durability.
    probe.exhaust_verification = true;
    const next = try index.insertMany(root, &.{"gs://archive/after-lost-response.parquet"});
    try std.testing.expect(try cold.contains(next, "gs://archive/after-lost-response.parquet"));
    try std.testing.expect(!try cold.contains(root, "gs://archive/after-lost-response.parquet"));
    try std.testing.expectEqual(@as(usize, 0), probe.active.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), upload_slots.load(.acquire));
    // Both opt-ins are required, even with an executor available.
    vtable.supports_concurrent_requests = false;
    probe.peak.store(0, .release);
    const serial = try index.insertMany(next, &.{"gs://archive/serial-provider.parquet"});
    try std.testing.expectEqual(@as(usize, 1), probe.peak.load(.acquire));
    try std.testing.expect(try cold.contains(serial, "gs://archive/serial-provider.parquet"));
    vtable.supports_concurrent_requests = true;
    index.context.parallel_object_io = false;
    probe.peak.store(0, .release);
    const scoped = try index.insertMany(serial, &.{"gs://archive/serial-scope.parquet"});
    try std.testing.expectEqual(@as(usize, 1), probe.peak.load(.acquire));
    try std.testing.expect(try cold.contains(scoped, "gs://archive/serial-scope.parquet"));
}

test "external lake retirement canceled bulk uploads drain before retrying staged nodes" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var executor = std.Io.Threaded.init(a, .{});
    defer executor.deinit();
    const io = executor.io();
    var probe: ParallelProbe = .{ .memory = &memory, .allocator = a, .io = io, .cancel_put = true };
    var vtable = memory.client().vtable.*;
    vtable.supports_concurrent_requests = true;
    vtable.put_object = ParallelProbe.put;
    vtable.get_object = ParallelProbe.get;
    const client: storage.Client = .{ .allocator = a, .ptr = &probe, .vtable = &vtable };
    var index = Index.init(a, client, "archive", "cancel", .{ .io = io, .parallel_object_io = true, .cancellation = .{ .ptr = &probe, .is_cancelled_fn = ParallelProbe.canceled } });
    defer index.deinit();
    var uris: [64][]const u8 = undefined;
    for (&uris, 0..) |*uri, i| uri.* = try std.fmt.allocPrint(arena.allocator(), "gs://archive/{d}.parquet", .{i});
    try std.testing.expectError(error.Canceled, index.insertMany(null, &uris));
    try std.testing.expectEqual(@as(usize, 0), probe.active.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), upload_slots.load(.acquire));
    probe.cancel_put = false;
    probe.cancelled.store(false, .release);
    // Workers use bounded, independent allocators. Exhaustion falls back to
    // the original caller allocator only after all workers have drained.
    probe.exhaust_workers = true;
    const root = try index.insertMany(null, &uris);
    var cold = Index.init(a, memory.client(), "archive", "cancel", .{});
    defer cold.deinit();
    for (uris) |uri| try std.testing.expect(try cold.contains(root, uri));
    try std.testing.expectEqual(@as(usize, 0), upload_slots.load(.acquire));
}

test "external lake retirement hint selection shrinks incompressible cohorts within stored bounds" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var index = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer index.deinit();
    var random = std.Random.DefaultPrng.init(20261104);
    const alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
    var uris: [256][]const u8 = undefined;
    for (&uris) |*uri| {
        const bytes = try ca.alloc(u8, 2048);
        for (bytes) |*byte| byte.* = alphabet[random.random().int(u6)];
        uri.* = try std.fmt.allocPrint(ca, "gs://archive/{s}.parquet", .{bytes});
    }
    const root = try index.insertMany(null, &uris);
    var all: std.ArrayList(Hint) = .empty;
    var entries = index.cache.iterator();
    while (entries.next()) |entry| try all.append(ca, .{ .hash = entry.key_ptr.*, .node = entry.value_ptr.node });
    const unbounded = try std.json.Stringify.valueAlloc(ca, Hints{ .nodes = all.items }, .{});
    try std.testing.expect(unbounded.len < decoded_hint_bytes);
    try std.testing.expectError(error.LakeRetirementIndexBudgetExceeded, encodeHint(ca, unbounded));
    try index.saveHints(&.{root});
    var client = memory.client();
    var object = try client.getObject("archive", "reachable/warm-v1.json", .{});
    defer object.deinit(a);
    try std.testing.expect(object.body.len <= hint_bytes);
    var cold = Index.init(a, memory.client(), "archive", "reachable", .{});
    defer cold.deinit();
    try cold.loadHints();
    try std.testing.expect(cold.cache.count() > 0);
    try std.testing.expect(cold.cache.count() < index.cache.count());
    for (uris) |uri| try std.testing.expect(try cold.contains(root, uri));
}

test "external lake retirement optional hints preserve required reads near a deadline" {
    const a = std.testing.allocator;
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var writer = Index.init(a, memory.client(), "archive", "hint-reserve", .{});
    defer writer.deinit();
    const root = try writer.insert(null, "required");
    const context: types.Context = .{ .deadline_ns = @import("antfly_platform").time.monotonicNs() +| std.time.ns_per_s / 2 };
    var reader = Index.init(a, memory.client(), "archive", "hint-reserve", context);
    defer reader.deinit();
    const before = memory.operationCount();
    try std.testing.expectError(error.LakeRetirementHintBudgetExceeded, reader.loadHints());
    try std.testing.expectEqual(before, memory.operationCount());
    try std.testing.expect(try reader.contains(root, "required"));
    const after = memory.operationCount();
    try std.testing.expectError(error.LakeRetirementHintBudgetExceeded, reader.saveHints(&.{root}));
    try std.testing.expectEqual(after, memory.operationCount());
    try context.ensureActive();
}
