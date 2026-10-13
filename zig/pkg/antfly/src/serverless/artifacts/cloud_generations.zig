// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Publication-owned persistent radix map. Every routing edge pins its provider
//! generation, including edges between map pages. Updates rewrite only touched
//! paths; query readers never install mutable bindings on shared store owners.
const std = @import("std");
const objectstore = @import("objectstore");
const api = @import("store.zig");
const local = @import("antfly_local_sources");
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const Ref = local.serverless_manifest_artifact_ref.ProviderGenerationRef;
pub const max_page_bytes = 1024 * 1024;
const leaf_records = 128;
pub const Record = struct {
    id: []const u8,
    bytes: u64,
    generation: []const u8,
    retention_until_ms: u64 = 0,
    fn validate(self: @This()) !void {
        _ = try api.sha256ChecksumFromArtifactId(self.id);
        if (self.bytes == 0 or self.generation.len == 0 or self.generation.len > 4096 or std.mem.eql(u8, self.generation, "null")) return error.InvalidCloudGenerationMap;
    }
    pub fn deinit(self: *@This(), a: A) void {
        a.free(self.id);
        a.free(self.generation);
    }
    fn clone(self: @This(), a: A) !Record {
        const id = try a.dupe(u8, self.id);
        errdefer a.free(id);
        return .{ .id = id, .bytes = self.bytes, .generation = try a.dupe(u8, self.generation), .retention_until_ms = self.retention_until_ms };
    }
};
const Child = struct { slot: u8, ref: Ref };
pub const Node = struct {
    version: u8 = 1,
    depth: u8,
    leaf: bool,
    records: []const Record = &.{},
    children: []const Child = &.{},
    fn validate(self: @This(), expected_depth: u8) !void {
        if (self.version != 1 or self.depth != expected_depth or self.depth > 64) return error.InvalidCloudGenerationMap;
        if (self.leaf) {
            if (self.children.len != 0 or self.records.len == 0 or self.records.len > leaf_records) return error.InvalidCloudGenerationMap;
            for (self.records, 0..) |record, i| {
                try record.validate();
                if (i != 0 and std.mem.order(u8, self.records[i - 1].id, record.id) != .lt) return error.InvalidCloudGenerationMap;
            }
        } else {
            if (self.depth == 64 or self.records.len != 0 or self.children.len == 0 or self.children.len > 16) return error.InvalidCloudGenerationMap;
            for (self.children, 0..) |child, i| {
                if (child.slot > 15 or (i != 0 and self.children[i - 1].slot >= child.slot)) return error.InvalidCloudGenerationMap;
                try child.ref.validate();
            }
        }
    }
};
fn hash(id: []const u8) [32]u8 {
    var value: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(id, &value, .{});
    return value;
}
fn slot(digest: [32]u8, depth: u8) u8 {
    return if (depth % 2 == 0) digest[depth / 2] >> 4 else digest[depth / 2] & 15;
}
fn remote(token: Token) ?objectstore.CancellationToken {
    return objectstore.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn);
}
fn key(a: A, prefix: []const u8, id: []const u8) ![]u8 {
    const suffix = try api.storageSuffixAlloc(a, id);
    defer a.free(suffix);
    return std.fmt.allocPrint(a, "{s}{s}{s}", .{ prefix, if (prefix.len == 0) "" else "/", suffix });
}
pub fn cloneRef(a: A, ref: Ref) !Ref {
    const id = try a.dupe(u8, ref.artifact_id);
    errdefer a.free(id);
    const checksum = try a.dupe(u8, ref.checksum);
    errdefer a.free(checksum);
    return .{ .artifact_id = id, .checksum = checksum, .byte_len = ref.byte_len, .generation = try a.dupe(u8, ref.generation), .retention_until_ms = ref.retention_until_ms };
}
pub fn freeRef(a: A, ref: Ref) void {
    a.free(ref.artifact_id);
    a.free(ref.checksum);
    a.free(ref.generation);
}
pub const Reader = struct {
    client: objectstore.Client,
    bucket: []const u8,
    prefix: []const u8,
    root: ?Ref = null,
    complete: bool = false,
    alternatives: []const *Reader = &.{},
    cache_limit: usize = 8 * 1024 * 1024,
    cache_allocator: A = std.heap.page_allocator,
    /// Operation-local, serialized read allowance. Independent read views own
    /// independent counters; no publication-sized cache is required.
    remaining_bytes: u64 = 256 * 1024 * 1024,
    mu: std.atomic.Mutex = .unlocked,
    cache: [16]?Cached = @splat(null),
    cache_bytes: usize = 0,
    clock: u64 = 0,
    decoded_hits: u64 = 0,
    const Cached = struct { ref: Ref, bytes: []u8, arena: *std.heap.ArenaAllocator, node: Node, cost: usize, used: u64 };
    fn freeCached(self: *Reader, value: Cached) void {
        value.arena.deinit();
        self.cache_allocator.destroy(value.arena);
        freeRef(self.cache_allocator, value.ref);
        self.cache_allocator.free(value.bytes);
    }
    fn cloneNode(a: A, node: Node) !Node {
        const records = try a.alloc(Record, node.records.len);
        for (records, node.records) |*out, record| out.* = try record.clone(a);
        const children = try a.alloc(Child, node.children.len);
        for (children, node.children) |*out, child| out.* = .{ .slot = child.slot, .ref = try cloneRef(a, child.ref) };
        return .{ .version = node.version, .depth = node.depth, .leaf = node.leaf, .records = records, .children = children };
    }
    pub fn resetBudget(self: *Reader) void {
        lock(&self.mu);
        defer self.mu.unlock();
        self.remaining_bytes = 256 * 1024 * 1024;
    }
    pub fn deinit(self: *Reader) void {
        for (self.cache) |entry| if (entry) |value| {
            self.freeCached(value);
        };
        self.cache = @splat(null);
        self.cache_bytes = 0;
    }
    fn validateDomain(self: *Reader, node: Node, ref: Ref) !void {
        const root_scope = (try api.uploadScopeFromArtifactId(if (self.root) |root| root.artifact_id else ref.artifact_id)) orelse return error.InvalidCloudGenerationMap;
        const scope = (try api.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidCloudGenerationMap;
        if (!std.mem.eql(u8, &root_scope.domain, &scope.domain)) return error.InvalidCloudGenerationMap;
        for (node.records) |record| {
            const payload = (try api.uploadScopeFromArtifactId(record.id)) orelse return error.InvalidCloudGenerationMap;
            if (!std.mem.eql(u8, &root_scope.domain, &payload.domain)) return error.InvalidCloudGenerationMap;
        }
        for (node.children) |child| {
            const nested = (try api.uploadScopeFromArtifactId(child.ref.artifact_id)) orelse return error.InvalidCloudGenerationMap;
            if (!std.mem.eql(u8, &root_scope.domain, &nested.domain)) return error.InvalidCloudGenerationMap;
        }
    }
    pub fn read(self: *Reader, a: A, ref: Ref, depth: u8, token: Token) !Node {
        try ref.validate();
        try token.check();
        if (ref.byte_len > max_page_bytes) return error.InvalidCloudGenerationMap;
        {
            lock(&self.mu);
            defer self.mu.unlock();
            self.clock +|= 1;
            for (&self.cache) |*entry| if (entry.*) |*cached| {
                if (std.mem.eql(u8, cached.ref.artifact_id, ref.artifact_id) and std.mem.eql(u8, cached.ref.generation, ref.generation)) {
                    if (cached.ref.byte_len != ref.byte_len or !std.mem.eql(u8, cached.ref.checksum, ref.checksum)) return error.ArtifactIntegrityMismatch;
                    cached.used = self.clock;
                    if (cached.node.depth != depth) return error.InvalidCloudGenerationMap;
                    self.decoded_hits += 1;
                    return cloneNode(a, cached.node);
                }
            };
            try api.chargeReadBudget(&self.remaining_bytes, ref.byte_len);
        }
        const path = try key(a, self.prefix, ref.artifact_id);
        defer a.free(path);
        var client = self.client;
        client.allocator = a;
        var result = client.getObject(self.bucket, path, .{ .version_id = ref.generation, .skip_metadata_probe = true, .max_response_bytes = @intCast(ref.byte_len), .cancellation = remote(token) }) catch |err| switch (err) {
            error.FileNotFound, error.NotFound => return error.CloudGenerationMapUnavailable,
            else => return err,
        };
        defer result.deinit(a);
        if (result.body.len != ref.byte_len) return error.ArtifactIntegrityMismatch;
        try api.validatePayloadSha256WithCancellation(result.body, ref.checksum, token);
        const node = try std.json.parseFromSliceLeaky(Node, a, result.body, .{ .allocate = .alloc_always });
        try node.validate(depth);
        try self.validateDomain(node, ref);
        if (result.body.len > self.cache_limit) return node;
        // Distinct cold leaves can use the shared I/O scheduler concurrently.
        // Cache bookkeeping stays serialized; no network request holds its lock.
        lock(&self.mu);
        defer self.mu.unlock();
        self.clock +|= 1;
        for (self.cache) |entry| if (entry) |cached| {
            if (std.mem.eql(u8, cached.ref.artifact_id, ref.artifact_id) and std.mem.eql(u8, cached.ref.generation, ref.generation)) return node;
        };
        const ca = self.cache_allocator;
        const owned = try ca.dupe(u8, result.body);
        const owned_ref = cloneRef(ca, ref) catch |err| {
            ca.free(owned);
            return err;
        };
        const parsed = ca.create(std.heap.ArenaAllocator) catch |err| {
            freeRef(ca, owned_ref);
            ca.free(owned);
            return err;
        };
        parsed.* = .init(ca);
        var retained = false;
        defer if (!retained) {
            parsed.deinit();
            ca.destroy(parsed);
            freeRef(ca, owned_ref);
            ca.free(owned);
        };
        const decoded = try std.json.parseFromSliceLeaky(Node, parsed.allocator(), owned, .{ .allocate = .alloc_if_needed });
        const cost = owned.len + parsed.queryCapacity() + @sizeOf(std.heap.ArenaAllocator) + @sizeOf(Cached) + owned_ref.artifact_id.len + owned_ref.checksum.len + owned_ref.generation.len;
        if (cost > self.cache_limit) return node;
        var index: usize = 0;
        while (true) {
            var free: ?usize = null;
            var oldest: ?usize = null;
            for (self.cache, 0..) |entry, i| {
                if (entry) |value| {
                    if (oldest == null or value.used < self.cache[oldest.?].?.used) oldest = i;
                } else free = i;
            }
            if (free != null and cost <= self.cache_limit - self.cache_bytes) {
                index = free.?;
                break;
            }
            const victim = oldest orelse return error.InvalidCloudGenerationMap;
            const old = self.cache[victim].?;
            self.cache_bytes -= old.cost;
            self.freeCached(old);
            self.cache[victim] = null;
        }
        self.cache[index] = .{ .ref = owned_ref, .bytes = owned, .arena = parsed, .node = decoded, .cost = cost, .used = self.clock };
        self.cache_bytes += cost;
        retained = true;
        return node;
    }
    const Step = union(enum) { next: Ref, found: ?Record };
    fn select(node: Node, a: A, id: []const u8, digest: [32]u8, depth: u8) !Step {
        if (node.depth != depth) return error.InvalidCloudGenerationMap;
        if (node.leaf) {
            var lo: usize = 0;
            var hi = node.records.len;
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                if (std.mem.order(u8, node.records[mid].id, id) == .lt) lo = mid + 1 else hi = mid;
            }
            return .{ .found = if (lo < node.records.len and std.mem.eql(u8, node.records[lo].id, id)) try node.records[lo].clone(a) else null };
        }
        for (node.children) |child| if (child.slot == slot(digest, depth)) return .{ .next = try cloneRef(std.heap.page_allocator, child.ref) };
        return .{ .found = null };
    }
    fn cachedStep(self: *Reader, a: A, ref: Ref, id: []const u8, digest: [32]u8, depth: u8) !?Step {
        lock(&self.mu);
        defer self.mu.unlock();
        self.clock +|= 1;
        for (&self.cache) |*entry| if (entry.*) |*value| {
            if (!std.mem.eql(u8, value.ref.artifact_id, ref.artifact_id) or !std.mem.eql(u8, value.ref.generation, ref.generation)) continue;
            if (value.ref.byte_len != ref.byte_len or !std.mem.eql(u8, value.ref.checksum, ref.checksum)) return error.ArtifactIntegrityMismatch;
            value.used = self.clock;
            self.decoded_hits += 1;
            return try select(value.node, a, id, digest, depth);
        };
        return null;
    }
    pub fn lookup(self: *Reader, a: A, id: []const u8, token: Token) !?Record {
        _ = try api.sha256ChecksumFromArtifactId(id);
        for (self.alternatives) |reader| if (try reader.lookup(a, id, token)) |record| return record;
        var ref = self.root orelse return null;
        const digest = hash(id);
        var memory: local.sql_memory_budget = .{ .backing = self.cache_allocator, .limit = 8 * 1024 * 1024 };
        var arena = std.heap.ArenaAllocator.init(memory.allocator());
        defer arena.deinit();
        var owned_ref: ?Ref = null;
        defer if (owned_ref) |value| freeRef(std.heap.page_allocator, value);
        var depth: u8 = 0;
        while (true) : (depth += 1) {
            try token.check();
            const step = (try self.cachedStep(a, ref, id, digest, depth)) orelse cold: {
                const node = try self.read(arena.allocator(), ref, depth, token);
                break :cold try select(node, a, id, digest, depth);
            };
            const retained = switch (step) {
                .found => |record| {
                    if (record == null and self.complete) return error.CloudGenerationMissing;
                    return record;
                },
                .next => |next| next,
            };
            if (owned_ref) |value| freeRef(std.heap.page_allocator, value);
            owned_ref = retained;
            ref = retained;
            _ = arena.reset(.free_all);
        }
    }
    /// Map pages are reachability roots themselves; records describe versions
    /// but do not retain orphan payloads merely because they share a map page.
    pub fn walk(self: *Reader, a: A, visitor: anytype, token: Token) !void {
        if (self.root) |root| try self.walkNode(a, root, 0, visitor, token);
    }
    fn walkNode(self: *Reader, a: A, ref: Ref, depth: u8, visitor: anytype, token: Token) anyerror!void {
        try visitor.page(ref);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const node = try self.read(arena.allocator(), ref, depth, token);
        for (node.records) |record| try visitor.record(record);
        for (node.children) |child| try self.walkNode(a, child.ref, depth + 1, visitor, token);
    }
};
pub const Writer = struct {
    a: A,
    reader: Reader,
    scope: api.UploadScope,
    qualified_retention: bool = false,
    cancellation: Token = .none,
    complete: bool = false,
    disabled: bool = false,
    pending: std.ArrayList(Record) = .empty,
    mu: std.atomic.Mutex = .unlocked,
    pub fn deinit(self: *Writer) void {
        self.reader.deinit();
        for (self.pending.items) |*record| record.deinit(self.a);
        self.pending.deinit(self.a);
        if (self.reader.root) |root| freeRef(self.a, root);
    }
    pub fn lookup(self: *Writer, a: A, id: []const u8, token: Token) !?Record {
        lock(&self.mu);
        defer self.mu.unlock();
        for (self.pending.items) |record| if (std.mem.eql(u8, record.id, id)) return try record.clone(a);
        return self.reader.lookup(a, id, token);
    }
    pub fn capture(self: *Writer, id: []const u8, bytes: u64, generation: ?[]const u8, incoming: Token) !void {
        try incoming.check();
        const token = if (incoming.ptr != null) incoming else self.cancellation;
        try token.check();
        lock(&self.mu);
        defer self.mu.unlock();
        if (self.disabled) return;
        const version = generation orelse {
            self.disabled = true;
            self.complete = false;
            return;
        };
        if (version.len == 0 or std.mem.eql(u8, version, "null")) {
            self.disabled = true;
            self.complete = false;
            return;
        }
        var record: Record = .{ .id = id, .bytes = bytes, .generation = version };
        try record.validate();
        const payload_scope = (try api.uploadScopeFromArtifactId(id)) orelse return error.InvalidCloudGenerationMap;
        if (!std.mem.eql(u8, &payload_scope.domain, &self.scope.domain)) return error.InvalidCloudGenerationMap;
        for (self.pending.items) |prior| if (std.mem.eql(u8, prior.id, id) and std.mem.eql(u8, prior.generation, version)) {
            if (prior.bytes != bytes) return error.ArtifactIntegrityMismatch;
            return;
        };
        if (try self.reader.lookup(self.a, id, token)) |previous| {
            var prior = previous;
            defer prior.deinit(self.a);
            if (prior.bytes != bytes) return error.ArtifactIntegrityMismatch;
            if (std.mem.eql(u8, prior.generation, version)) return;
        }
        if (self.qualified_retention) {
            const path = try key(self.a, self.reader.prefix, id);
            defer self.a.free(path);
            if (try self.reader.client.versionRetention(self.reader.bucket, path, version, remote(token))) |proof| record.retention_until_ms = proof.until_ms;
        }
        const owned = try record.clone(self.a);
        self.pending.append(self.a, owned) catch |err| {
            var value = owned;
            value.deinit(self.a);
            return err;
        };
        if (self.pending.items.len == leaf_records) try self.flushLocked(token);
    }
    pub fn rebase(self: *Writer) void {
        lock(&self.mu);
        defer self.mu.unlock();
        for (self.pending.items) |*record| record.deinit(self.a);
        self.pending.clearRetainingCapacity();
        if (self.reader.root) |root| freeRef(self.a, root);
        self.reader.root = null;
        self.reader.deinit();
        self.complete = true;
    }
    pub fn finish(self: *Writer, token: Token) !?Ref {
        lock(&self.mu);
        defer self.mu.unlock();
        try self.flushLocked(token);
        return if (self.reader.root) |root| try cloneRef(self.a, root) else null;
    }
    fn flushLocked(self: *Writer, token: Token) !void {
        if (self.pending.items.len == 0) return;
        defer {
            for (self.pending.items) |*record| record.deinit(self.a);
            self.pending.clearRetainingCapacity();
        }
        const next = self.update(self.reader.root, self.pending.items, 0, token) catch |err| switch (err) {
            error.ProviderGenerationUnavailable => {
                self.disabled = true;
                self.complete = false;
                return;
            },
            else => return err,
        };
        if (self.reader.root) |old| freeRef(self.a, old);
        self.reader.root = next;
    }
    fn update(self: *Writer, old: ?Ref, changes: []const Record, depth: u8, token: Token) anyerror!Ref {
        try token.check();
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        // Construction reads one bounded page, not a cumulative archive quota.
        self.reader.remaining_bytes = std.math.maxInt(u64);
        const node: Node = if (old) |ref| try self.reader.read(a, ref, depth, token) else .{ .depth = depth, .leaf = true };
        if (node.leaf) {
            var entries: std.StringHashMapUnmanaged(Record) = .empty;
            for (node.records) |record| try entries.put(a, record.id, record);
            for (changes) |record| {
                if (entries.get(record.id)) |prior| if (prior.bytes != record.bytes) return error.ArtifactIntegrityMismatch;
                try entries.put(a, record.id, record);
            }
            const records = try a.alloc(Record, entries.count());
            var it = entries.valueIterator();
            for (records) |*record| record.* = it.next().?.*;
            std.mem.sort(Record, records, {}, struct {
                fn less(_: void, l: Record, r: Record) bool {
                    return std.mem.order(u8, l.id, r.id) == .lt;
                }
            }.less);
            if (records.len <= leaf_records) return self.publish(.{ .depth = depth, .leaf = true, .records = records }, token);
            if (depth == 64) return error.InvalidCloudGenerationMap;
            return self.branch(&.{}, records, depth, token);
        }
        return self.branch(node.children, changes, depth, token);
    }
    fn branch(self: *Writer, previous: []const Child, changes: []const Record, depth: u8, token: Token) !Ref {
        var children: [16]Child = undefined;
        var owned: [16]?Ref = @splat(null);
        defer for (owned) |value| if (value) |ref| freeRef(self.a, ref);
        var count: usize = 0;
        for (0..16) |index| {
            var selected: [leaf_records * 2]Record = undefined;
            var length: usize = 0;
            for (changes) |record| if (slot(hash(record.id), depth) == index) {
                if (length == selected.len) return error.InvalidCloudGenerationMap;
                selected[length] = record;
                length += 1;
            };
            const prior = for (previous) |child| {
                if (child.slot == index) break child.ref;
            } else null;
            const ref = if (length != 0) changed: {
                owned[index] = try self.update(prior, selected[0..length], depth + 1, token);
                break :changed owned[index].?;
            } else prior orelse continue;
            children[count] = .{ .slot = @intCast(index), .ref = ref };
            count += 1;
        }
        return self.publish(.{ .depth = depth, .leaf = false, .children = children[0..count] }, token);
    }
    fn publish(self: *Writer, node: Node, token: Token) !Ref {
        try node.validate(node.depth);
        const bytes = try std.json.Stringify.valueAlloc(self.a, node, .{});
        defer self.a.free(bytes);
        if (bytes.len > max_page_bytes) return error.InvalidCloudGenerationMap;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        const checksum = std.fmt.bytesToHex(digest, .lower);
        const id = try self.scope.artifactId(&checksum);
        const path = try key(self.a, self.reader.prefix, &id);
        defer self.a.free(path);
        var client = self.reader.client;
        client.allocator = self.a;
        var result = client.putObject(self.reader.bucket, path, bytes, .{ .if_none_match = true, .cancellation = remote(token) }) catch |err| switch (err) {
            error.PreconditionFailed => existing: {
                var object = try client.getObject(self.reader.bucket, path, .{ .max_response_bytes = bytes.len, .cancellation = remote(token) });
                defer object.deinit(self.a);
                if (!std.mem.eql(u8, object.body, bytes)) return error.ArtifactIntegrityMismatch;
                break :existing objectstore.PutResult{ .version_id = if (object.metadata.version_id) |value| try self.a.dupe(u8, value) else null };
            },
            else => return err,
        };
        defer result.deinit(self.a);
        const generation = result.version_id orelse return error.ProviderGenerationUnavailable;
        if (generation.len == 0 or std.mem.eql(u8, generation, "null")) return error.ProviderGenerationUnavailable;
        var until: u64 = std.math.maxInt(u64);
        for (node.records) |record| until = @min(until, record.retention_until_ms);
        for (node.children) |child| until = @min(until, child.ref.retention_until_ms);
        const proof = if (self.qualified_retention) try client.versionRetention(self.reader.bucket, path, generation, remote(token)) else null;
        until = @min(until, if (proof) |value| value.until_ms else 0);
        return cloneRef(self.a, .{ .artifact_id = &id, .checksum = &checksum, .byte_len = bytes.len, .generation = generation, .retention_until_ms = until });
    }
};
fn lock(mu: *std.atomic.Mutex) void {
    while (!mu.tryLock()) std.atomic.spinLoopHint();
}

test "external lake global frontier generation maps pin restart reads and rewrite only changed radix paths" {
    const a = std.testing.allocator;
    const Provider = struct {
        memory: objectstore.MemoryClient,
        writes: usize = 0,
        serial: u64 = 0,
        fn client(self: *@This()) objectstore.Client {
            return .{ .allocator = self.memory.alloc, .ptr = self, .vtable = &vtable };
        }
        fn noop(_: A, _: *anyopaque) void {}
        fn exists(raw: *anyopaque, bucket: []const u8, _: objectstore.BucketOptions) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var c = self.memory.client();
            return c.bucketExists(bucket);
        }
        fn make(raw: *anyopaque, bucket: []const u8, _: objectstore.BucketOptions) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var c = self.memory.client();
            return c.makeBucket(bucket);
        }
        fn put(raw: *anyopaque, alloc: A, bucket: []const u8, name: []const u8, body: []const u8, options: objectstore.PutOptions) !objectstore.PutResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.writes += 1;
            var c = self.memory.client();
            var result = try c.putObject(bucket, name, body, options);
            result.deinit(c.allocator);
            self.serial += 1;
            const generation = try std.fmt.allocPrint(alloc, "{d}", .{self.serial});
            errdefer alloc.free(generation);
            const version_key = try std.fmt.allocPrint(alloc, "{s}@{s}", .{ name, generation });
            defer alloc.free(version_key);
            var versioned = try c.putObject(bucket, version_key, body, .{});
            versioned.deinit(c.allocator);
            return .{ .version_id = generation };
        }
        fn get(raw: *anyopaque, alloc: A, bucket: []const u8, name: []const u8, options: objectstore.GetOptions) !objectstore.GetResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            var c = self.memory.client();
            c.allocator = alloc;
            const selected = if (options.version_id) |version| try std.fmt.allocPrint(alloc, "{s}@{s}", .{ name, version }) else try alloc.dupe(u8, name);
            defer alloc.free(selected);
            var result = try c.getObject(bucket, selected, options);
            errdefer result.deinit(alloc);
            if (options.version_id) |version| result.metadata.version_id = try alloc.dupe(u8, version);
            return result;
        }
        fn stat(_: *anyopaque, _: A, _: []const u8, _: []const u8) !objectstore.ObjectMetadata {
            return error.UnexpectedMetadataProbe;
        }
        fn attributes(_: *anyopaque, _: A, _: []const u8, _: []const u8) !objectstore.ObjectAttributes {
            return error.UnexpectedMetadataProbe;
        }
        fn delete(_: *anyopaque, _: []const u8, _: []const u8, _: objectstore.DeleteOptions) !void {
            return error.UnexpectedDelete;
        }
        fn list(_: *anyopaque, _: A, _: []const u8, _: objectstore.ListOptions) !objectstore.ListResult {
            return error.UnexpectedList;
        }
        fn retention(_: *anyopaque, _: A, _: []const u8, _: []const u8, _: []const u8, _: ?objectstore.CancellationToken) !?objectstore.VersionRetention {
            return .{ .observed_ms = 100, .until_ms = 1_000_000 };
        }
        const vtable: objectstore.Client.VTable = .{ .deinit = noop, .bucket_exists = exists, .make_bucket = make, .put_object = put, .get_object = get, .stat_object = stat, .get_object_attributes = attributes, .delete_object = delete, .list_objects = list, .version_retention = retention };
    };
    var provider: Provider = .{ .memory = .init(a) };
    defer provider.memory.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    const scope: api.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    var writer: Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "maps" }, .scope = scope, .qualified_retention = true, .complete = true };
    defer writer.deinit();
    var ids: [513][175]u8 = undefined;
    for (&ids, 0..) |*id, i| {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, i, .big);
        id.* = try scope.artifactId(&std.fmt.bytesToHex(hash(&bytes), .lower));
        if (i < 512) try writer.capture(id, 8, "payload-v1", .none);
    }
    const first = (try writer.finish(.none)).?;
    defer freeRef(a, first);
    try std.testing.expectEqual(@as(u64, 1_000_000), first.retention_until_ms);
    const before = provider.writes;
    try writer.capture(&ids[512], 8, "payload-v2", .none);
    const second = (try writer.finish(.none)).?;
    defer freeRef(a, second);
    try std.testing.expect(provider.writes - before <= 3);
    var reopened: Reader = .{ .client = client, .bucket = "bucket", .prefix = "maps", .root = first, .complete = true };
    defer reopened.deinit();
    for (ids[0..512]) |id| {
        var found = (try reopened.lookup(a, &id, .none)).?;
        defer found.deinit(a);
        try std.testing.expectEqualStrings("payload-v1", found.generation);
    }
    try std.testing.expectError(error.CloudGenerationMissing, reopened.lookup(a, &ids[512], .none));
    var warmed = (try reopened.lookup(a, &ids[0], .none)).?;
    warmed.deinit(a);
    const charged = reopened.remaining_bytes;
    const hits = reopened.decoded_hits;
    for (0..100) |_| {
        var selected = (try reopened.lookup(a, &ids[0], .none)).?;
        selected.deinit(a);
    }
    try std.testing.expectEqual(charged, reopened.remaining_bytes);
    try std.testing.expect(reopened.decoded_hits >= hits + 100);
    try std.testing.expect(reopened.cache_bytes <= reopened.cache_limit);
    // Replacing the live map name cannot redirect an authenticated edge.
    const path = try key(a, "maps", first.artifact_id);
    defer a.free(path);
    var underlying = provider.memory.client();
    var replaced = try underlying.putObject("bucket", path, "corrupt live head", .{});
    replaced.deinit(a);
    var found = (try reopened.lookup(a, &ids[0], .none)).?;
    defer found.deinit(a);
    try std.testing.expectEqualStrings("payload-v1", found.generation);
}
