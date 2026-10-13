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
    fn validateDomain(_: *Reader, node: Node, ref: Ref) !void {
        const root_scope = (try api.uploadScopeFromArtifactId(ref.artifact_id)) orelse return error.InvalidCloudGenerationMap;
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
        return self.lookupAt(a, id, self.root, token);
    }
    fn lookupAt(self: *Reader, a: A, id: []const u8, root: ?Ref, token: Token) anyerror!?Record {
        _ = try api.sha256ChecksumFromArtifactId(id);
        for (self.alternatives) |reader| if (try reader.lookup(a, id, token)) |record| return record;
        var ref = root orelse return null;
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
pub const VacuumCut = struct {
    domain: [32]u8,
    floor: u64,
    cutoff: u64,
    marks: ?@import("../graph_segment/page_tree.zig").Ref,
    pub fn validate(self: @This()) !void {
        if (self.floor > self.cutoff or self.cutoff == 0 or std.mem.allEqual(u8, &self.domain, 0)) return error.InvalidCloudGenerationMap;
        if (self.marks) |root| try root.validate();
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
    io: ?std.Io = null,
    flush_mu: std.Io.Mutex = .init,
    active: std.atomic.Value(u32) = .init(0),
    vacuum_progress: local.metadata_lake_index_catalog.CloudVacuumProgress = .{},
    fn operationIo(self: *Writer) !std.Io {
        return self.io orelse (if (@import("builtin").is_test) std.testing.io else return error.UnsupportedOperation);
    }
    fn rootSnapshot(self: *Writer) !?Ref {
        lock(&self.mu);
        defer self.mu.unlock();
        return if (self.reader.root) |root| try cloneRef(std.heap.page_allocator, root) else null;
    }
    pub fn deinit(self: *Writer) void {
        self.reader.deinit();
        for (self.pending.items) |*record| record.deinit(self.a);
        self.pending.deinit(self.a);
        if (self.reader.root) |root| freeRef(self.a, root);
    }
    pub fn lookup(self: *Writer, a: A, id: []const u8, token: Token) !?Record {
        const root: ?Ref = held: {
            lock(&self.mu);
            defer self.mu.unlock();
            for (self.pending.items) |record| if (std.mem.eql(u8, record.id, id)) return try record.clone(a);
            break :held if (self.reader.root) |ref| try cloneRef(std.heap.page_allocator, ref) else null;
        };
        defer if (root) |ref| freeRef(std.heap.page_allocator, ref);
        return self.reader.lookupAt(a, id, root, token);
    }
    pub fn capture(self: *Writer, id: []const u8, bytes: u64, generation: ?[]const u8, incoming: Token) !void {
        _ = self.active.fetchAdd(1, .acq_rel);
        defer _ = self.active.fetchSub(1, .acq_rel);
        try incoming.check();
        const token = if (incoming.ptr != null) incoming else self.cancellation;
        try token.check();
        const version = generation orelse "";
        {
            lock(&self.mu);
            defer self.mu.unlock();
            if (self.disabled) return;
            if (version.len == 0 or std.mem.eql(u8, version, "null")) {
                self.disabled = true;
                self.complete = false;
                return;
            }
            for (self.pending.items) |prior| if (std.mem.eql(u8, prior.id, id) and std.mem.eql(u8, prior.generation, version)) {
                if (prior.bytes != bytes) return error.ArtifactIntegrityMismatch;
                return;
            };
        }
        var record: Record = .{ .id = id, .bytes = bytes, .generation = version };
        try record.validate();
        const payload_scope = (try api.uploadScopeFromArtifactId(id)) orelse return error.InvalidCloudGenerationMap;
        if (!std.mem.eql(u8, &payload_scope.domain, &self.scope.domain)) return error.InvalidCloudGenerationMap;
        // Provider lookups/proofs run concurrently. Only journal ownership and
        // publication of an immutable root require the short bookkeeping lock.
        const root = try self.rootSnapshot();
        defer if (root) |ref| freeRef(std.heap.page_allocator, ref);
        if (try self.reader.lookupAt(std.heap.page_allocator, id, root, token)) |previous| {
            var prior = previous;
            defer prior.deinit(std.heap.page_allocator);
            if (prior.bytes != bytes) return error.ArtifactIntegrityMismatch;
            if (std.mem.eql(u8, prior.generation, version)) return;
        }
        if (self.qualified_retention) {
            const path = try key(std.heap.page_allocator, self.reader.prefix, id);
            defer std.heap.page_allocator.free(path);
            if (try self.reader.client.versionRetention(self.reader.bucket, path, version, remote(token))) |proof| record.retention_until_ms = proof.until_ms;
        }
        const should_flush = appended: while (true) {
            lock(&self.mu);
            if (self.disabled) {
                self.mu.unlock();
                return;
            }
            if (self.pending.items.len >= leaf_records * 2) {
                self.mu.unlock();
                try self.flush(token);
                continue;
            }
            defer self.mu.unlock();
            if (self.disabled) return;
            for (self.pending.items) |prior| if (std.mem.eql(u8, prior.id, id)) {
                if (prior.bytes != bytes) return error.ArtifactIntegrityMismatch;
                if (std.mem.eql(u8, prior.generation, version)) return;
            };
            const owned = try record.clone(self.a);
            self.pending.append(self.a, owned) catch |err| {
                var value = owned;
                value.deinit(self.a);
                return err;
            };
            break :appended self.pending.items.len >= leaf_records;
        };
        if (should_flush) try self.flush(token);
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
        try token.check();
        const io = try self.operationIo();
        // Callers stop submitting uploads before this publication barrier.
        while (self.active.load(.acquire) != 0) {
            try token.check();
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try self.flush(token);
        lock(&self.mu);
        defer self.mu.unlock();
        return if (self.reader.root) |root| try cloneRef(self.a, root) else null;
    }
    fn flush(self: *Writer, token: Token) !void {
        const io = try self.operationIo();
        // Waiting flushers suspend; network I/O never holds the spinlock.
        try self.flush_mu.lock(io);
        defer self.flush_mu.unlock(io);
        var batch: std.ArrayList(Record) = .empty;
        const root: ?Ref = detached: {
            lock(&self.mu);
            defer self.mu.unlock();
            if (self.disabled or self.pending.items.len == 0) return;
            batch = self.pending;
            self.pending = .empty;
            break :detached self.reader.root;
        };
        defer {
            for (batch.items) |*record| record.deinit(self.a);
            batch.deinit(self.a);
        }
        const next = self.update(root, batch.items, 0, token) catch |err| {
            lock(&self.mu);
            defer self.mu.unlock();
            self.complete = false;
            self.disabled = true; // failed journals can never claim completeness
            if (err == error.ProviderGenerationUnavailable) return;
            return err;
        };
        lock(&self.mu);
        defer self.mu.unlock();
        if (self.reader.root) |old| freeRef(self.a, old);
        self.reader.root = next;
    }
    pub fn vacuum(self: *Writer, store: *api.ArtifactStore, cut: VacuumCut, progress: local.metadata_lake_index_catalog.CloudVacuumProgress, token: Token) !local.metadata_lake_index_catalog.CloudVacuumProgress {
        try token.check();
        try cut.validate();
        if (!std.mem.eql(u8, &cut.domain, &self.scope.domain)) return error.InvalidCloudGenerationMap;
        const io = try self.operationIo();
        try self.flush_mu.lock(io);
        defer self.flush_mu.unlock(io);
        const root = self.reader.root orelse return .{ .cutoff = cut.cutoff, .done = true };
        if (progress.cutoff == cut.cutoff and progress.done) return progress;
        const State = struct {
            writer: *Writer,
            cut: VacuumCut,
            after: ?[32]u8,
            last: ?[32]u8,
            leaves: usize = 4,
            reads: u64 = 16 * 1024 * 1024,
            done: bool = true,
            token: Token,
            marks: @import("../graph_segment/page_tree.zig").Store,
            fn prune(v: *@This(), ref: Ref, depth: u8, prefix: [32]u8) anyerror!?Ref {
                var upper = prefix;
                for (depth..64) |d| {
                    if (d % 2 == 0) upper[d / 2] |= 0xf0 else upper[d / 2] |= 0x0f;
                }
                if (v.after) |after| if (std.mem.order(u8, &upper, &after) != .gt) return try cloneRef(v.writer.a, ref);
                if (v.leaves == 0 or ref.byte_len > v.reads) {
                    v.done = false;
                    return try cloneRef(v.writer.a, ref);
                }
                v.reads -= ref.byte_len;
                var arena = std.heap.ArenaAllocator.init(v.writer.a);
                defer arena.deinit();
                const a = arena.allocator();
                const node = try v.writer.reader.read(a, ref, depth, v.token);
                if (node.leaf) {
                    v.leaves -= 1;
                    var kept: std.ArrayList(Record) = .empty;
                    for (node.records) |record| {
                        const digest = hash(record.id);
                        if (v.last == null or std.mem.order(u8, &digest, &v.last.?) == .gt) v.last = digest;
                        const scope = (try api.uploadScopeFromArtifactId(record.id)) orelse return error.InvalidCloudGenerationMap;
                        var retain = scope.fencingToken() < v.cut.floor or scope.fencingToken() >= v.cut.cutoff;
                        if (!retain) {
                            var cursor = try @import("../graph_segment/page_tree.zig").Cursor.init(a, v.marks, v.cut.marks, record.id, null);
                            defer cursor.deinit();
                            if (try cursor.next()) |entry| retain = std.mem.eql(u8, entry.key, record.id);
                        }
                        if (retain) try kept.append(a, record);
                    }
                    if (kept.items.len == node.records.len) return try cloneRef(v.writer.a, ref);
                    if (kept.items.len == 0) return null;
                    return try v.writer.publish(.{ .depth = depth, .leaf = true, .records = kept.items }, v.token);
                }
                var children: std.ArrayList(Child) = .empty;
                defer for (children.items) |child| freeRef(v.writer.a, child.ref);
                var changed = false;
                for (node.children) |child| {
                    var next_prefix = prefix;
                    if (depth % 2 == 0) next_prefix[depth / 2] |= child.slot << 4 else next_prefix[depth / 2] |= child.slot;
                    if (try v.prune(child.ref, depth + 1, next_prefix)) |next| {
                        changed = changed or !std.mem.eql(u8, next.artifact_id, child.ref.artifact_id);
                        children.append(a, .{ .slot = child.slot, .ref = next }) catch |err| {
                            freeRef(v.writer.a, next);
                            return err;
                        };
                    } else changed = true;
                }
                if (!changed) return try cloneRef(v.writer.a, ref);
                if (children.items.len == 0) return null;
                return try v.writer.publish(.{ .depth = depth, .leaf = false, .children = children.items }, v.token);
            }
        };
        var reads: u64 = 16 * 1024 * 1024;
        var writes: u64 = 0;
        var pages: @import("../graph_segment/page_store.zig").PageStore = .{ .domain = cut.domain, .artifacts = store, .cancellation = token, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
        var cache: @import("../graph_segment/page_tree.zig").Cache = .{ .alloc = self.a, .underlying = pages.store() };
        defer cache.deinit();
        const after = if (!progress.done) progress.after else null;
        var state: State = .{ .writer = self, .cut = cut, .after = after, .last = after, .token = token, .marks = cache.store() };
        const next = try state.prune(root, 0, @splat(0));
        // A failed pass leaves the old root and cursor intact. Readers retain
        // their own immutable map roots; only this new publication is vacuumed.
        lock(&self.mu);
        defer self.mu.unlock();
        freeRef(self.a, self.reader.root.?);
        self.reader.root = next;
        return .{ .cutoff = cut.cutoff, .after = state.last, .done = state.done };
    }
    fn update(self: *Writer, old: ?Ref, changes: []const Record, depth: u8, token: Token) anyerror!Ref {
        try token.check();
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        // Construction reads one bounded page, not a cumulative archive quota.
        lock(&self.reader.mu);
        self.reader.remaining_bytes = std.math.maxInt(u64);
        self.reader.mu.unlock();
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
            var selected: [leaf_records * 8]Record = undefined;
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

test "external lake global frontier cloud journal overlaps provider proofs and joins its barrier" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var provider = @import("cloud_generations_test_provider.zig").Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    const Gate = struct {
        io: std.Io,
        arrived: std.atomic.Value(u32) = .init(0),
        both: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn proof(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.arrived.fetchAdd(1, .acq_rel) == 1) self.both.set(self.io);
            try self.release.waitTimeout(self.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        }
        fn capture(writer: *Writer, id: []const u8) !void {
            try writer.capture(id, 8, "payload", .none);
        }
    };
    var gate: Gate = .{ .io = io };
    provider.retention_hook = .{ .ptr = &gate, .call = Gate.proof };
    var memory: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = a };
    const scope = try api.UploadScope.forPublication(@splat(1), 1, io);
    var writer: Writer = .{ .a = memory.allocator(), .io = io, .reader = .{ .client = client, .bucket = "bucket", .prefix = "maps" }, .scope = scope, .qualified_retention = true, .complete = true };
    defer writer.deinit();
    const first_id = try scope.artifactId(&std.fmt.bytesToHex(hash("first"), .lower));
    const second_id = try scope.artifactId(&std.fmt.bytesToHex(hash("second"), .lower));
    var first = try io.concurrent(Gate.capture, .{ &writer, &first_id });
    defer _ = first.cancel(io) catch {};
    var second = try io.concurrent(Gate.capture, .{ &writer, &second_id });
    defer _ = second.cancel(io) catch {};
    defer gate.release.set(io);
    try gate.both.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    gate.release.set(io);
    try first.await(io);
    try second.await(io);
    const root = (try writer.finish(.none)).?;
    defer freeRef(writer.a, root);
    try std.testing.expectEqual(@as(u32, 0), writer.active.load(.acquire));
    for ([_][]const u8{ &first_id, &second_id }) |id| {
        var found = (try writer.lookup(a, id, .none)).?;
        found.deinit(a);
    }
}

test "external lake global frontier cloud vacuum resumes from GC evidence and preserves old readers" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ca = arena.allocator();
    var provider = @import("cloud_generations_test_provider.zig").Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    const old_scope = try api.UploadScope.forPublication(@splat(1), 1, std.testing.io);
    const scope = try api.UploadScope.forPublication(@splat(1), 3, std.testing.io);
    var writer: Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "maps" }, .scope = scope, .complete = true };
    defer writer.deinit();
    var ids: [700][175]u8 = undefined;
    var mutations: std.ArrayList(@import("../graph_segment/page_tree.zig").Mutation) = .empty;
    for (&ids, 0..) |*id, i| {
        var number: [8]u8 = undefined;
        std.mem.writeInt(u64, &number, i, .big);
        id.* = try (if (i % 7 == 0) scope else old_scope).artifactId(&std.fmt.bytesToHex(hash(&number), .lower));
        try writer.capture(id, 8, "payload", .none);
        if (i % 2 == 0) try mutations.append(ca, .{ .key = id, .value = "live" });
    }
    const original = (try writer.finish(.none)).?;
    defer freeRef(a, original);
    var old_reader: Reader = .{ .client = client, .bucket = "bucket", .prefix = "maps", .root = original, .complete = true };
    defer old_reader.deinit();
    var directory = try local.common_test_directory.TestDirectory.init("cloud-vacuum-marks");
    defer directory.cleanup();
    var fs = try @import("fs_store.zig").FsStore.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = old_scope;
    var reads: u64 = 64 * 1024 * 1024;
    var writes: u64 = 64 * 1024 * 1024;
    var pages: @import("../graph_segment/page_store.zig").PageStore = .{ .domain = scope.domain, .attempt = old_scope.attempt, .artifacts = &store, .cancellation = .none, .remaining_read_bytes = &reads, .remaining_write_bytes = &writes };
    std.mem.sort(@import("../graph_segment/page_tree.zig").Mutation, mutations.items, {}, struct {
        fn less(_: void, l: @import("../graph_segment/page_tree.zig").Mutation, r: @import("../graph_segment/page_tree.zig").Mutation) bool {
            return std.mem.order(u8, l.key, r.key) == .lt;
        }
    }.less);
    const marks = try @import("../graph_segment/page_tree.zig").apply(a, pages.store(), null, mutations.items);
    const cut: VacuumCut = .{ .domain = scope.domain, .floor = 1, .cutoff = 2, .marks = marks };
    var progress: local.metadata_lake_index_catalog.CloudVacuumProgress = .{};
    var passes: usize = 0;
    const heads = provider.stats;
    while (!progress.done and passes < 32) : (passes += 1) {
        progress = try writer.vacuum(&store, cut, progress, .none);
    }
    try std.testing.expect(progress.done and passes > 1);
    try std.testing.expectEqual(heads, provider.stats);
    const root = (try writer.finish(.none)).?;
    defer freeRef(a, root);
    var reopened: Reader = .{ .client = client, .bucket = "bucket", .prefix = "maps", .root = root };
    defer reopened.deinit();
    for (&ids, 0..) |*id, i| {
        var old = (try old_reader.lookup(a, id, .none)).?;
        old.deinit(a);
        var found = try reopened.lookup(a, id, .none);
        defer if (found) |*record| record.deinit(a);
        try std.testing.expectEqual(i % 2 == 0 or i % 7 == 0, found != null);
    }
    const cancelled: Token = .{ .ptr = &passes, .is_cancelled_fn = struct {
        fn check(_: *const anyopaque) bool {
            return true;
        }
    }.check };
    try std.testing.expectError(error.Canceled, writer.vacuum(&store, cut, .{}, cancelled));
}
