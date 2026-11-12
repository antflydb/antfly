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
/// Data-only navigation shared across operation-local generation bindings.
/// Neither clients, roots, deadlines nor reader leases enter this pool.
const Navigation = struct {
    mu: std.atomic.Mutex = .unlocked,
    cache: [128]?Reader.Cached = @splat(null),
    cache_bytes: usize = 0,
    clock: u64 = 0,
    flights: [32]?[32]u8 = @splat(null),
    coalesced_waits: @import("antfly_platform").atomic.Value(u64) = .init(0),
    cache_allocator: A = std.heap.page_allocator,
    cache_limit: usize = 8 * 1024 * 1024,
    fn freeCached(self: *Navigation, value: Reader.Cached) void {
        value.arena.deinit();
        self.cache_allocator.destroy(value.arena);
        freeRef(self.cache_allocator, value.ref);
        self.cache_allocator.free(value.bytes);
    }
    fn deinit(self: *Navigation) void {
        for (self.cache) |entry| if (entry) |value| self.freeCached(value);
        self.cache = @splat(null);
        self.cache_bytes = 0;
    }
};
pub const NavigationOwner = struct {
    refs: std.atomic.Value(usize) = .init(1),
    budget: local.sql_memory_budget,
    navigation: Navigation = .{},
    pool: ?*NavigationPool = null,
    pub fn release(self: *NavigationOwner) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        self.navigation.deinit();
        std.debug.assert(self.budget.live == 0);
        self.budget.backing.destroy(self);
    }
};
pub const NavigationPool = struct {
    const Entry = struct { scope: [32]u8, owner: *NavigationOwner, used: u64 };
    entries: [8]?Entry = @splat(null),
    mu: std.atomic.Mutex = .unlocked,
    clock: u64 = 0,
    budget: local.sql_memory_budget = .{ .backing = @import("antfly_platform").allocator.processAllocator(if (@import("builtin").os.tag == .freestanding) std.heap.wasm_allocator else std.heap.smp_allocator), .limit = 64 * 1024 * 1024 },
    pub fn acquire(self: *NavigationPool, scope: [32]u8) !*NavigationOwner {
        lock(&self.mu);
        defer self.mu.unlock();
        self.clock +|= 1;
        var target: usize = 0;
        for (self.entries, 0..) |entry, i| {
            if (entry) |held| {
                if (std.mem.eql(u8, &scope, &held.scope)) {
                    self.entries[i].?.used = self.clock;
                    _ = held.owner.refs.fetchAdd(1, .monotonic);
                    return held.owner;
                }
                if (self.entries[target] != null and held.used < self.entries[target].?.used) target = i;
            } else target = i;
        }
        if (self.entries[target]) |retired| {
            self.entries[target] = null;
            retired.owner.release();
        }
        const owner = while (true) {
            break self.budget.allocator().create(NavigationOwner) catch |err| {
                if (err != error.OutOfMemory) return err;
                const idle = self.removeIdleLocked(null) orelse return err;
                idle.release();
                continue;
            };
        };
        owner.* = .{ .pool = self, .budget = .{ .backing = self.budget.allocator(), .limit = 64 * 1024 * 1024 } };
        owner.navigation.cache_limit = 64 * 1024 * 1024;
        owner.navigation.cache_allocator = owner.budget.allocator();
        self.entries[target] = .{ .scope = scope, .owner = owner, .used = self.clock };
        _ = owner.refs.fetchAdd(1, .monotonic);
        return owner;
    }
    fn removeIdleLocked(self: *NavigationPool, exclude: ?*NavigationOwner) ?*NavigationOwner {
        var oldest: ?usize = null;
        for (self.entries, 0..) |entry, i| if (entry) |held| {
            if (held.owner == exclude or held.owner.refs.load(.acquire) != 1) continue;
            if (oldest == null or held.used < self.entries[oldest.?].?.used) oldest = i;
        };
        const index = oldest orelse return null;
        const retired = self.entries[index].?.owner;
        self.entries[index] = null;
        return retired;
    }
    /// Reclaim cache pins only. Active readers retain their navigation owners.
    fn reclaimIdle(self: *NavigationPool, exclude: *NavigationOwner) bool {
        lock(&self.mu);
        const retired = self.removeIdleLocked(exclude);
        self.mu.unlock();
        if (retired) |owner| {
            owner.release();
            return true;
        }
        return false;
    }
    pub fn deinit(self: *NavigationPool) void {
        for (&self.entries) |*entry| if (entry.*) |held| {
            held.owner.release();
            entry.* = null;
        };
        std.debug.assert(self.budget.live == 0);
    }
};
var process_navigation: NavigationPool = .{};
pub fn sharedNavigation(scope: [32]u8) ?*NavigationOwner {
    if (std.mem.allEqual(u8, &scope, 0)) return null;
    return process_navigation.acquire(scope) catch null;
}
pub const ByteCache = struct {
    cache: *local.serverless_query_lake_serving_cache.Cache,
    scope: [32]u8,
    context: local.serverless_query_lake_read_context.Context,
};

pub const Reader = struct {
    opening_pages: []const local.metadata_lake_index_catalog.CloudNavigationPage = &.{},
    opening_root: ?Ref = null,
    byte_cache: ?ByteCache = null,
    io: ?std.Io = null,
    client: objectstore.Client,
    bucket: []const u8,
    prefix: []const u8,
    root: ?Ref = null,
    complete: bool = false,
    /// Borrowed from the admitted publication, for an optional opening seed
    /// only. Both the record and this exact map binding must outlive the reader.
    opening_hint: ?struct { root: Ref, record: Record } = null,
    bootstrap: []const local.metadata_lake_index_catalog.ProviderLocator = &.{},
    publication_locators: []const local.metadata_lake_index_catalog.OpeningBundle = &.{},
    locator_root: ?Ref = null,
    trace: ?*local.serverless_query_lake_read_context.IoTrace = null,
    alternatives: []const *Reader = &.{},
    cache_limit: usize = 8 * 1024 * 1024,
    cache_allocator: A = std.heap.page_allocator,
    /// Operation-local, serialized read allowance. Independent read views own
    /// independent counters; no publication-sized cache is required.
    remaining_bytes: u64 = 256 * 1024 * 1024,
    shared: ?*NavigationOwner = null,
    local_navigation: Navigation = .{},
    decoded_hits: u64 = 0,
    const Cached = struct { ref: Ref, bytes: []u8, arena: *std.heap.ArenaAllocator, node: Node, cost: usize, used: u64 };
    fn navigation(self: *Reader) *Navigation {
        if (self.shared) |owner| return &owner.navigation;
        return &self.local_navigation;
    }
    fn cacheLimit(self: *Reader) usize {
        return if (self.shared) |owner| owner.navigation.cache_limit else self.cache_limit;
    }
    fn cacheAllocator(self: *Reader) A {
        return if (self.shared) |owner| owner.navigation.cache_allocator else self.cache_allocator;
    }
    fn freeCached(self: *Reader, value: Cached) void {
        const a = self.cacheAllocator();
        value.arena.deinit();
        a.destroy(value.arena);
        freeRef(a, value.ref);
        a.free(value.bytes);
    }
    fn evictOne(self: *Reader) bool {
        const nav = self.navigation();
        var oldest: ?usize = null;
        for (nav.cache, 0..) |entry, i| if (entry) |value| {
            if (oldest == null or value.used < nav.cache[oldest.?].?.used) oldest = i;
        };
        const index = oldest orelse return false;
        const value = nav.cache[index].?;
        nav.cache_bytes -= value.cost;
        nav.cache[index] = null;
        self.freeCached(value);
        return true;
    }
    pub fn cachedBytes(self: *Reader) usize {
        const nav = self.navigation();
        lock(&nav.mu);
        defer nav.mu.unlock();
        return nav.cache_bytes;
    }
    fn cloneNode(a: A, node: Node) !Node {
        const records = try a.alloc(Record, node.records.len);
        for (records, node.records) |*out, record| out.* = try record.clone(a);
        const children = try a.alloc(Child, node.children.len);
        for (children, node.children) |*out, child| out.* = .{ .slot = child.slot, .ref = try cloneRef(a, child.ref) };
        return .{ .version = node.version, .depth = node.depth, .leaf = node.leaf, .records = records, .children = children };
    }
    pub fn resetBudget(self: *Reader) void {
        lock(&self.navigation().mu);
        defer self.navigation().mu.unlock();
        self.remaining_bytes = 256 * 1024 * 1024;
    }
    pub fn deinit(self: *Reader) void {
        if (self.shared) |owner| {
            owner.release();
            self.shared = null;
        }
        self.local_navigation.cache_allocator = self.cache_allocator;
        self.local_navigation.deinit();
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
    pub fn bindOpeningPages(self: *Reader, pages: []const local.metadata_lake_index_catalog.CloudNavigationPage, token: Token) !void {
        const root = self.root orelse return error.InvalidCloudGenerationMap;
        const domain = (try api.uploadScopeFromArtifactId(root.artifact_id)) orelse return error.InvalidCloudGenerationMap;
        if (pages.len > 17) return error.InvalidCloudGenerationMap;
        var remaining: usize = 32 * 1024;
        for (pages, 0..) |page, i| {
            try page.ref.validate();
            const scope_ = (try api.uploadScopeFromArtifactId(page.ref.artifact_id)) orelse return error.InvalidCloudGenerationMap;
            if (!std.mem.eql(u8, &scope_.domain, &domain.domain) or page.body.len != page.ref.byte_len or page.body.len > remaining) return error.InvalidCloudGenerationMap;
            for (pages[0..i]) |prior| if (std.mem.eql(u8, prior.ref.artifact_id, page.ref.artifact_id)) return error.InvalidCloudGenerationMap;
            try api.validatePayloadSha256WithCancellation(page.body, page.ref.checksum, token);
            remaining -= page.body.len;
        }
        self.opening_pages = pages;
        self.opening_root = root;
    }

    fn readBytes(self: *Reader, a: A, ref: Ref, token: Token) ![]u8 {
        if (self.opening_root) |bound| if (self.root) |root| if (sameRef(bound, root)) {
            for (self.opening_pages) |page| if (sameRef(page.ref, ref)) {
                try token.check();
                try api.validatePayloadSha256WithCancellation(page.body, ref.checksum, token);
                return a.dupe(u8, page.body);
            };
        };
        const Load = struct {
            reader: *Reader,
            ref: Ref,
            token: Token,
            fn load(raw: *anyopaque, alloc: A) ![]u8 {
                const args: *@This() = @ptrCast(@alignCast(raw));
                const path = try key(alloc, args.reader.prefix, args.ref.artifact_id);
                defer alloc.free(path);
                var client = args.reader.client;
                client.allocator = alloc;
                const started = @import("antfly_platform").time.monotonicNs();
                defer if (args.reader.trace) |trace| {
                    trace.add("map_provider_reads", 1);
                    trace.add("map_provider_nanoseconds", @import("antfly_platform").time.monotonicNs() -| started);
                };
                var result = client.getObject(args.reader.bucket, path, .{
                    .version_id = args.ref.generation,
                    .skip_metadata_probe = true,
                    .max_response_bytes = @intCast(args.ref.byte_len),
                    .cancellation = remote(args.token),
                }) catch |err| switch (err) {
                    error.FileNotFound, error.NotFound => return error.CloudGenerationMapUnavailable,
                    else => return err,
                };
                defer result.deinit(alloc);
                if (result.body.len != args.ref.byte_len) return error.ArtifactIntegrityMismatch;
                try api.validatePayloadSha256WithCancellation(result.body, args.ref.checksum, args.token);
                return alloc.dupe(u8, result.body);
            }
        };
        var loader: Load = .{ .reader = self, .ref = ref, .token = token };
        if (self.byte_cache) |cached| {
            // Version and credential scope are part of the key. Persisted
            // bytes still undergo checksum, namespace and node-depth checks.
            const identity = try std.fmt.allocPrint(a, "cloud-map:{s}:{s}", .{ ref.artifact_id, ref.generation });
            defer a.free(identity);
            var lease = try cached.cache.readImmutableBlockLease(a, cached.scope, identity, @intCast(ref.byte_len), try api.sha256DigestFromChecksum(ref.checksum), cached.context, .{ .ptr = &loader, .load = Load.load });
            defer lease.deinit();
            try token.check();
            return a.dupe(u8, lease.bytes());
        }
        return Load.load(&loader, a);
    }
    pub fn read(self: *Reader, a: A, ref: Ref, depth: u8, token: Token) !Node {
        try ref.validate();
        try token.check();
        if (ref.byte_len > max_page_bytes) return error.InvalidCloudGenerationMap;
        var flight: ?usize = null;
        var flight_hash = std.crypto.hash.Blake3.init(.{});
        flight_hash.update(ref.artifact_id);
        flight_hash.update(ref.generation);
        var identity: [32]u8 = undefined;
        flight_hash.final(&identity);
        defer if (flight) |index| {
            lock(&self.navigation().mu);
            self.navigation().flights[index] = null;
            self.navigation().mu.unlock();
        };
        while (true) {
            try token.check();
            lock(&self.navigation().mu);
            var locked = true;
            defer if (locked) self.navigation().mu.unlock();
            self.navigation().clock +|= 1;
            for (&self.navigation().cache) |*entry| if (entry.*) |*cached| {
                if (std.mem.eql(u8, cached.ref.artifact_id, ref.artifact_id) and std.mem.eql(u8, cached.ref.generation, ref.generation)) {
                    if (cached.ref.byte_len != ref.byte_len or !std.mem.eql(u8, cached.ref.checksum, ref.checksum)) return error.ArtifactIntegrityMismatch;
                    cached.used = self.navigation().clock;
                    if (cached.node.depth != depth) return error.InvalidCloudGenerationMap;
                    self.decoded_hits += 1;
                    return cloneNode(a, cached.node);
                }
            };
            if (self.io) |io| {
                var available: ?usize = null;
                var waiting = false;
                for (self.navigation().flights, 0..) |active, index| {
                    if (active) |held| {
                        if (std.mem.eql(u8, &held, &identity)) waiting = true;
                    } else available = index;
                }
                if (waiting) {
                    _ = self.navigation().coalesced_waits.fetchAdd(1, .monotonic);
                    self.navigation().mu.unlock();
                    locked = false;
                    try io.sleep(.fromMilliseconds(1), .awake);
                    continue;
                }
                if (available) |index| {
                    self.navigation().flights[index] = identity;
                    flight = index;
                }
            }
            try api.chargeReadBudget(&self.remaining_bytes, ref.byte_len);
            break;
        }
        const bytes = try self.readBytes(a, ref, token);
        defer a.free(bytes);
        const node = try std.json.parseFromSliceLeaky(Node, a, bytes, .{ .allocate = .alloc_always });
        try node.validate(depth);
        try self.validateDomain(node, ref);
        if (bytes.len > self.cacheLimit()) return node;
        // Distinct cold leaves can use the shared I/O scheduler concurrently.
        // Cache bookkeeping stays serialized; no network request holds its lock.
        lock(&self.navigation().mu);
        defer self.navigation().mu.unlock();
        self.navigation().clock +|= 1;
        for (self.navigation().cache) |entry| if (entry) |cached| {
            if (std.mem.eql(u8, cached.ref.artifact_id, ref.artifact_id) and std.mem.eql(u8, cached.ref.generation, ref.generation)) return node;
        };
        const ca = self.cacheAllocator();
        // Make room before optional decoding, including shared-parent pressure.
        // Otherwise a full byte budget would prevent reaching LRU eviction.
        const staging = @min(self.cacheLimit(), bytes.len * 4 + 16 * 1024);
        while ((local.sql_memory_budget.headroom(ca) orelse std.math.maxInt(usize)) < staging) {
            if (!self.evictOne()) {
                const owner = self.shared orelse break;
                const pool = owner.pool orelse break;
                if (!pool.reclaimIdle(owner)) break;
            }
        }
        const owned = ca.dupe(u8, bytes) catch return node;
        const owned_ref = cloneRef(ca, ref) catch {
            ca.free(owned);
            return node;
        };
        const parsed = ca.create(std.heap.ArenaAllocator) catch {
            freeRef(ca, owned_ref);
            ca.free(owned);
            return node;
        };
        parsed.* = .init(ca);
        var retained = false;
        defer if (!retained) {
            parsed.deinit();
            ca.destroy(parsed);
            freeRef(ca, owned_ref);
            ca.free(owned);
        };
        const decoded = std.json.parseFromSliceLeaky(Node, parsed.allocator(), owned, .{ .allocate = .alloc_if_needed }) catch return node;
        const cost = owned.len + parsed.queryCapacity() + @sizeOf(std.heap.ArenaAllocator) + @sizeOf(Cached) + owned_ref.artifact_id.len + owned_ref.checksum.len + owned_ref.generation.len;
        if (cost > self.cacheLimit()) return node;
        var index: usize = 0;
        while (true) {
            var free: ?usize = null;
            var oldest: ?usize = null;
            for (self.navigation().cache, 0..) |entry, i| {
                if (entry) |value| {
                    if (oldest == null or value.used < self.navigation().cache[oldest.?].?.used) oldest = i;
                } else free = i;
            }
            if (free != null and cost <= self.cacheLimit() - self.navigation().cache_bytes) {
                index = free.?;
                break;
            }
            const victim = oldest orelse return error.InvalidCloudGenerationMap;
            const old = self.navigation().cache[victim].?;
            self.navigation().cache_bytes -= old.cost;
            self.freeCached(old);
            self.navigation().cache[victim] = null;
        }
        self.navigation().cache[index] = .{ .ref = owned_ref, .bytes = owned, .arena = parsed, .node = decoded, .cost = cost, .used = self.navigation().clock };
        self.navigation().cache_bytes += cost;
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
        lock(&self.navigation().mu);
        defer self.navigation().mu.unlock();
        self.navigation().clock +|= 1;
        for (&self.navigation().cache) |*entry| if (entry.*) |*value| {
            if (!std.mem.eql(u8, value.ref.artifact_id, ref.artifact_id) or !std.mem.eql(u8, value.ref.generation, ref.generation)) continue;
            if (value.ref.byte_len != ref.byte_len or !std.mem.eql(u8, value.ref.checksum, ref.checksum)) return error.ArtifactIntegrityMismatch;
            value.used = self.navigation().clock;
            self.decoded_hits += 1;
            return try select(value.node, a, id, digest, depth);
        };
        return null;
    }
    pub fn bindOpeningHint(self: *Reader, record: Record) !void {
        try record.validate();
        const root = self.root orelse return error.InvalidCloudGenerationMap;
        try root.validate();
        const map_scope = (try api.uploadScopeFromArtifactId(root.artifact_id)) orelse return error.InvalidCloudGenerationMap;
        const payload_scope = (try api.uploadScopeFromArtifactId(record.id)) orelse return error.InvalidCloudGenerationMap;
        if (!std.mem.eql(u8, &map_scope.domain, &payload_scope.domain)) return error.InvalidCloudGenerationMap;
        self.opening_hint = .{ .root = root, .record = record };
    }
    /// The admitted publication authenticates these optional direct locators.
    /// Keep the exact root binding and namespace; hints grant no authority to
    /// unrelated IDs or a later incarnation/generation.
    pub fn bindPublicationLocators(self: *Reader, bundles: []const local.metadata_lake_index_catalog.OpeningBundle) !void {
        const root = self.root orelse return;
        const domain = (try api.uploadScopeFromArtifactId(root.artifact_id)) orelse return error.InvalidCloudGenerationMap;
        var total: usize = 0;
        var seen: [128][]const u8 = undefined;
        for (bundles) |bundle| for (bundle.provider_locators) |value| {
            total += 1;
            if (total > 128) return error.InvalidCloudGenerationMap;
            for (seen[0 .. total - 1]) |prior| if (std.mem.eql(u8, prior, value.id)) return error.InvalidCloudGenerationMap;
            seen[total - 1] = value.id;
            try (Record{ .id = value.id, .bytes = value.bytes, .generation = value.generation, .retention_until_ms = value.retention_until_ms }).validate();
            for (self.bootstrap) |bootstrap| if (std.mem.eql(u8, bootstrap.id, value.id)) {
                if (bootstrap.bytes != value.bytes or !std.mem.eql(u8, bootstrap.generation, value.generation) or bootstrap.retention_until_ms != value.retention_until_ms) return error.InvalidCloudGenerationMap;
            };
            const scoped = (try api.uploadScopeFromArtifactId(value.id)) orelse return error.InvalidCloudGenerationMap;
            if (!std.mem.eql(u8, &domain.domain, &scoped.domain)) return error.InvalidCloudGenerationMap;
        };
        self.publication_locators = bundles;
        self.locator_root = root;
    }
    pub fn lookup(self: *Reader, a: A, id: []const u8, token: Token) !?Record {
        try token.check();
        const started = @import("antfly_platform").time.monotonicNs();
        defer if (self.trace) |trace| trace.add("map_lookup_nanoseconds", @import("antfly_platform").time.monotonicNs() -| started);
        if (self.locator_root) |bound| if (self.root) |root| {
            if (bound.byte_len == root.byte_len and bound.retention_until_ms == root.retention_until_ms and std.mem.eql(u8, bound.artifact_id, root.artifact_id) and std.mem.eql(u8, bound.checksum, root.checksum) and std.mem.eql(u8, bound.generation, root.generation)) {
                for (self.bootstrap) |value| if (std.mem.eql(u8, value.id, id)) {
                    if (self.trace) |trace| trace.add("map_locator_hits", 1);
                    return try (Record{ .id = value.id, .bytes = value.bytes, .generation = value.generation, .retention_until_ms = value.retention_until_ms }).clone(a);
                };
                for (self.publication_locators) |bundle| for (bundle.provider_locators) |value| if (std.mem.eql(u8, value.id, id)) {
                    if (self.trace) |trace| trace.add("map_locator_hits", 1);
                    return try (Record{ .id = value.id, .bytes = value.bytes, .generation = value.generation, .retention_until_ms = value.retention_until_ms }).clone(a);
                };
            }
        };
        if (self.opening_hint) |hint| if (self.root) |root| {
            if (root.byte_len == hint.root.byte_len and root.retention_until_ms == hint.root.retention_until_ms and
                std.mem.eql(u8, root.artifact_id, hint.root.artifact_id) and std.mem.eql(u8, root.checksum, hint.root.checksum) and
                std.mem.eql(u8, root.generation, hint.root.generation) and std.mem.eql(u8, id, hint.record.id)) return try hint.record.clone(a);
        };
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
    pub fn fingerprint(self: @This(), a: A) ![32]u8 {
        const bytes = try std.json.Stringify.valueAlloc(a, .{ .format = "native-cloud-vacuum-proof-v1", .cut = self }, .{});
        defer a.free(bytes);
        return hash(bytes);
    }
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
        const proof = try cut.fingerprint(self.a);
        const same_proof = progress.cutoff == cut.cutoff and progress.proof != null and std.mem.eql(u8, &progress.proof.?, &proof);
        const root = self.reader.root orelse return .{ .cutoff = cut.cutoff, .proof = proof, .done = true };
        if (same_proof and progress.done) return progress;
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
        // Cursors prove work only under the complete GC evidence. A new
        // mark root can retire records without changing the generation cutoff.
        // Legacy cursors have no proof and restart safely.
        const after = if (same_proof and !progress.done) progress.after else null;
        var state: State = .{ .writer = self, .cut = cut, .after = after, .last = after, .token = token, .marks = cache.store() };
        const next = try state.prune(root, 0, @splat(0));
        // A failed pass leaves the old root and cursor intact. Readers retain
        // their own immutable map roots; only this new publication is vacuumed.
        lock(&self.mu);
        defer self.mu.unlock();
        freeRef(self.a, self.reader.root.?);
        self.reader.root = next;
        return .{ .cutoff = cut.cutoff, .proof = proof, .after = state.last, .done = state.done };
    }
    fn update(self: *Writer, old: ?Ref, changes: []const Record, depth: u8, token: Token) anyerror!Ref {
        try token.check();
        var arena = std.heap.ArenaAllocator.init(self.a);
        defer arena.deinit();
        const a = arena.allocator();
        // Construction reads one bounded page, not a cumulative archive quota.
        lock(&self.reader.navigation().mu);
        self.reader.remaining_bytes = std.math.maxInt(u64);
        self.reader.navigation().mu.unlock();
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
        const scheduler = local.sql_parallel_scheduler;
        const Worker = struct {
            fn run(writer: *Writer, prior: ?Ref, selected: []const Record, level: u8, cancellation: Token) anyerror!Ref {
                return writer.update(prior, selected, level, cancellation);
            }
        };
        var counts: [16]usize = @splat(0);
        for (changes) |record| counts[slot(hash(record.id), depth)] += 1;
        var offsets: [16]usize = undefined;
        var total: usize = 0;
        for (counts, 0..) |length, index| {
            offsets[index] = total;
            total += length;
        }
        const partitioned = try self.a.alloc(Record, changes.len);
        defer self.a.free(partitioned);
        var positions = offsets;
        for (changes) |record| {
            const index = slot(hash(record.id), depth);
            partitioned[positions[index]] = record;
            positions[index] += 1;
        }
        var children: [16]Child = undefined;
        var owned: [16]?Ref = @splat(null);
        defer for (owned) |value| if (value) |ref| freeRef(self.a, ref);
        var tasks: [16]?scheduler.Task(anyerror!Ref) = @splat(null);
        const io = try self.operationIo();
        // Join before partition buffers and parent page arenas are released,
        // including cancellation and a failed sibling. No partial root installs.
        defer for (&tasks) |*handle| if (handle.*) |task_| {
            var task = task_;
            handle.* = null;
            if (task.cancel(io)) |ref| freeRef(self.a, ref) else |_| {}
        };
        var submitted: usize = 0;
        for (counts, 0..) |length, index| {
            if (length == 0) continue;
            const prior = for (previous) |child| {
                if (child.slot == index) break child.ref;
            } else null;
            const selected = partitioned[offsets[index]..][0..length];
            // One level supplies ample independent I/O. Deeper recursion stays
            // inline, bounding both live pages and task descriptors per writer.
            if (depth == 0 and submitted < 8) {
                if (scheduler.global().submitIoTransient(io, 2 * max_page_bytes, Worker.run, .{ self, prior, selected, depth + 1, token })) |task| {
                    tasks[index] = task;
                    submitted += 1;
                    continue;
                }
            }
            owned[index] = try self.update(prior, selected, depth + 1, token);
        }
        for (&tasks, 0..) |*handle, index| if (handle.*) |task_| {
            var task = task_;
            handle.* = null;
            owned[index] = try task.await(io);
        };
        try token.check();
        var count: usize = 0;
        for (0..16) |index| {
            const prior = for (previous) |child| {
                if (child.slot == index) break child.ref;
            } else null;
            const ref = owned[index] orelse prior orelse continue;
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
        mutex: std.atomic.Mutex = .unlocked,
        writes: usize = 0,
        serial: u64 = 0,
        fn client(self: *@This()) objectstore.Client {
            return .{ .allocator = self.memory.alloc, .ptr = self, .vtable = &vtable };
        }
        fn noop(_: A, _: *anyopaque) void {}
        fn exists(raw: *anyopaque, bucket: []const u8, _: objectstore.BucketOptions) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            lock(&self.mutex);
            defer self.mutex.unlock();
            var c = self.memory.client();
            return c.bucketExists(bucket);
        }
        fn make(raw: *anyopaque, bucket: []const u8, _: objectstore.BucketOptions) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            lock(&self.mutex);
            defer self.mutex.unlock();
            var c = self.memory.client();
            return c.makeBucket(bucket);
        }
        fn put(raw: *anyopaque, alloc: A, bucket: []const u8, name: []const u8, body: []const u8, options: objectstore.PutOptions) !objectstore.PutResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            lock(&self.mutex);
            defer self.mutex.unlock();
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
            lock(&self.mutex);
            defer self.mutex.unlock();
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
    try std.testing.expect(reopened.cachedBytes() <= reopened.cache_limit);
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
    // Switch real GC cuts with a partial cursor, including unchanged cutoffs.
    for ([_]u64{ 2, 3 }) |next_cutoff| {
        var switching: Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "maps", .root = try cloneRef(a, original) }, .scope = scope, .complete = true };
        defer switching.deinit();
        var switched = try switching.vacuum(&store, cut, .{}, .none);
        try std.testing.expect(!switched.done);
        const newer: VacuumCut = .{ .domain = scope.domain, .floor = 1, .cutoff = next_cutoff, .marks = null };
        const next_proof = try newer.fingerprint(a);
        var switched_passes: usize = 0;
        while ((!switched.done or !std.meta.eql(switched.proof, @as(?[32]u8, next_proof))) and switched_passes < 32) : (switched_passes += 1) {
            switched = try switching.vacuum(&store, newer, switched, .none);
        }
        try std.testing.expect(switched.done);
        for (&ids, 0..) |*id, i| {
            if (i % 7 == 0) continue;
            var remaining_record = try switching.reader.lookup(a, id, .none);
            defer if (remaining_record) |*record| record.deinit(a);
            try std.testing.expect(remaining_record == null);
        }
    }
    // Completed and legacy progress must also restart for changed evidence.
    const empty_cut: VacuumCut = .{ .domain = scope.domain, .floor = 1, .cutoff = cut.cutoff, .marks = null };
    var complete_progress = progress;
    var complete_passes: usize = 0;
    const empty_proof = try empty_cut.fingerprint(a);
    while ((!complete_progress.done or !std.meta.eql(complete_progress.proof, @as(?[32]u8, empty_proof))) and complete_passes < 32) : (complete_passes += 1) {
        complete_progress = try writer.vacuum(&store, empty_cut, complete_progress, .none);
    }
    try std.testing.expect(complete_progress.done);
    try std.testing.expectEqualDeep(empty_proof, complete_progress.proof.?);
    const legacy = try writer.vacuum(&store, cut, .{ .cutoff = cut.cutoff, .done = true }, .none);
    try std.testing.expectEqualDeep(try cut.fingerprint(a), legacy.proof.?);
    try std.testing.expect(!legacy.done);
    const cancelled: Token = .{ .ptr = &passes, .is_cancelled_fn = struct {
        fn check(_: *const anyopaque) bool {
            return true;
        }
    }.check };
    try std.testing.expectError(error.Canceled, writer.vacuum(&store, cut, .{}, cancelled));
}

test "external lake global frontier cloud map siblings overlap before root publication" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var provider = @import("cloud_generations_test_provider.zig").Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    const Gate = struct {
        io: std.Io,
        arrived: std.atomic.Value(u32) = .init(0),
        active: std.atomic.Value(u32) = .init(0),
        fail: bool = false,
        both: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn put(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.active.fetchAdd(1, .acq_rel);
            defer _ = self.active.fetchSub(1, .acq_rel);
            if (self.arrived.fetchAdd(1, .acq_rel) == 1) self.both.set(self.io);
            if (self.fail) {
                try self.both.waitTimeout(self.io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
                return error.TestWriteFailed;
            }
            try self.release.waitTimeout(self.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        }
        fn build(writer: *Writer, records: []const Record) anyerror!Ref {
            return writer.update(null, records, 0, .none);
        }
    };
    var gate: Gate = .{ .io = io };
    provider.put_hook = .{ .ptr = &gate, .call = Gate.put };
    var memory: local.sql_parallel_scheduler.LockedAllocator = .{ .backing = a };
    const scope = try api.UploadScope.forPublication(@splat(1), 1, io);
    var writer: Writer = .{ .a = memory.allocator(), .io = io, .reader = .{ .client = client, .bucket = "bucket", .prefix = "maps" }, .scope = scope, .complete = true };
    defer writer.deinit();
    var ids: [256][175]u8 = undefined;
    var records: [256]Record = undefined;
    for (&ids, &records, 0..) |*id, *record, i| {
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, i, .big);
        id.* = try scope.artifactId(&std.fmt.bytesToHex(hash(&bytes), .lower));
        record.* = .{ .id = id, .bytes = 8, .generation = "payload" };
    }
    var build = try io.concurrent(Gate.build, .{ &writer, &records });
    var joined = false;
    defer if (!joined) {
        if (build.cancel(io)) |ref| freeRef(writer.a, ref) else |_| {}
    };
    defer gate.release.set(io);
    try gate.both.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    try std.testing.expect(writer.reader.root == null);
    gate.release.set(io);
    joined = true;
    const root = try build.await(io);
    defer freeRef(writer.a, root);
    for (records) |record| {
        var found = (try writer.reader.lookupAt(a, record.id, root, .none)).?;
        defer found.deinit(a);
        try std.testing.expectEqualStrings(record.generation, found.generation);
    }
    // A failed sibling preserves the installed root, disables completeness,
    // and joins all provider operations before the caller regains ownership.
    writer.reader.root = try cloneRef(writer.a, root);
    gate.both = .unset;
    gate.arrived.store(0, .release);
    gate.fail = true;
    for (records) |record| try writer.pending.append(writer.a, try record.clone(writer.a));
    try std.testing.expectError(error.TestWriteFailed, writer.flush(.none));
    try std.testing.expectEqual(@as(u32, 0), gate.active.load(.acquire));
    try std.testing.expect(!writer.complete and writer.disabled);
    try std.testing.expectEqualStrings(root.generation, writer.reader.root.?.generation);
}

test "external lake global frontier shared generation navigation survives request release and pinned eviction" {
    const a = std.testing.allocator;
    const Provider = @import("cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    const scope: api.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    var writer: Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "native" }, .scope = scope };
    defer writer.deinit();
    const id = try scope.artifactId(&(@as([64]u8, @splat('a'))));
    try writer.capture(&id, 10, "v1", .none);
    const root = (try writer.finish(.none)).?;
    defer freeRef(a, root);
    var pool: NavigationPool = .{};
    defer pool.deinit();
    var one: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root, .shared = try pool.acquire(@splat(7)) };
    const record = (try one.lookup(a, &id, .none)).?;
    defer {
        var owned = record;
        owned.deinit(a);
    }
    one.deinit();
    var two: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root, .shared = try pool.acquire(@splat(7)) };
    defer two.deinit();
    const before = provider.gets;
    const again = (try two.lookup(a, &id, .none)).?;
    defer {
        var owned = again;
        owned.deinit(a);
    }
    try std.testing.expectEqual(before, provider.gets);
    try std.testing.expectEqualStrings("v1", again.generation);
    for (0..9) |i| (try pool.acquire(@splat(@intCast(i + 20)))).release();
    const pinned = (try two.lookup(a, &id, .none)).?;
    defer {
        var owned = pinned;
        owned.deinit(a);
    }
    try std.testing.expectEqual(before, provider.gets);
    try std.testing.expect(two.cachedBytes() <= 8 * 1024 * 1024);
    // Decoded-page admission also reclaims another idle scope, even when
    // the owner's own cache is empty and cannot free any bytes.
    var reclaim: NavigationPool = .{ .budget = .{ .backing = a, .limit = 64 * 1024 * 1024 } };
    defer reclaim.deinit();
    var old_scope: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root, .shared = try reclaim.acquire(@splat(1)) };
    var initial = (try old_scope.lookup(a, &id, .none)).?;
    initial.deinit(a);
    try std.testing.expect(old_scope.cachedBytes() > 0);
    old_scope.deinit();
    var new_scope: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root, .shared = try reclaim.acquire(@splat(2)) };
    defer new_scope.deinit();
    reclaim.budget.limit = reclaim.budget.live + 1;
    var admitted = (try new_scope.lookup(a, &id, .none)).?;
    admitted.deinit(a);
    try std.testing.expect(new_scope.cachedBytes() > 0);
    for (reclaim.entries) |entry| if (entry) |held| try std.testing.expect(!std.mem.eql(u8, &held.scope, &(@as([32]u8, @splat(1)))));
    var pressure: NavigationPool = .{};
    defer pressure.deinit();
    pressure.budget.limit = @sizeOf(NavigationOwner) + 1;
    var private: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root, .shared = try pressure.acquire(@splat(7)) };
    defer private.deinit();
    const uncached = (try private.lookup(a, &id, .none)).?;
    defer {
        var owned = uncached;
        owned.deinit(a);
    }
    try std.testing.expectEqualStrings("v1", uncached.generation);
    try std.testing.expectEqual(@as(usize, 0), private.cachedBytes());
    var isolated: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root, .shared = try pool.acquire(@splat(99)) };
    defer isolated.deinit();
    const before_other = provider.gets;
    const other = (try isolated.lookup(a, &id, .none)).?;
    defer {
        var owned = other;
        owned.deinit(a);
    }
    try std.testing.expectEqual(before_other + 1, provider.gets);
}

test "external lake global frontier cold map reads coalesce and canceled leaders release waiters" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var provider = @import("cloud_generations_test_provider.zig").Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    const scope: api.UploadScope = .{ .domain = @splat(1), .attempt = @splat(2) };
    var writer: Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "native" }, .scope = scope };
    defer writer.deinit();
    const id = try scope.artifactId(&(@as([64]u8, @splat('a'))));
    try writer.capture(&id, 10, "v1", .none);
    const root = (try writer.finish(.none)).?;
    defer freeRef(a, root);
    const Gate = struct {
        io: std.Io,
        calls: std.atomic.Value(usize) = .init(0),
        started: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        fn get(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.calls.fetchAdd(1, .monotonic) != 0) return;
            self.started.set(self.io);
            try self.release.wait(self.io);
        }
        fn read(reader: *Reader, alloc: A, key_: []const u8) !?Record {
            return reader.lookup(alloc, key_, .none);
        }
    };
    for ([_]bool{ false, true }) |cancel| {
        var gate: Gate = .{ .io = io };
        provider.get_hook = .{ .ptr = &gate, .call = Gate.get };
        defer provider.get_hook = null;
        var pool: NavigationPool = .{};
        defer pool.deinit();
        var one: Reader = .{ .io = io, .client = client, .bucket = "bucket", .prefix = "native", .root = root, .shared = try pool.acquire(@splat(9)) };
        defer one.deinit();
        var two: Reader = .{ .io = io, .client = client, .bucket = "bucket", .prefix = "native", .root = root, .shared = try pool.acquire(@splat(9)) };
        defer two.deinit();
        var leader = try io.concurrent(Gate.read, .{ &one, a, @as([]const u8, &id) });
        var leader_done = false;
        defer if (!leader_done) {
            if (leader.cancel(io)) |record_| {
                if (record_) |record| {
                    var owned = record;
                    owned.deinit(a);
                }
            } else |_| {}
        };
        try gate.started.wait(io);
        var waiter = try io.concurrent(Gate.read, .{ &two, a, @as([]const u8, &id) });
        var waiter_done = false;
        defer if (!waiter_done) {
            if (waiter.cancel(io)) |record_| {
                if (record_) |record| {
                    var owned = record;
                    owned.deinit(a);
                }
            } else |_| {}
        };
        const deadline = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s;
        while (one.navigation().coalesced_waits.load(.monotonic) == 0) {
            if (@import("antfly_platform").time.monotonicNs() > deadline) return error.TestUnexpectedResult;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(@as(usize, 1), gate.calls.load(.monotonic));
        if (cancel) {
            leader_done = true;
            try std.testing.expectError(error.Canceled, leader.cancel(io));
        } else {
            gate.release.set(io);
            leader_done = true;
            var record = (try leader.await(io)).?;
            record.deinit(a);
        }
        waiter_done = true;
        var record = (try waiter.await(io)).?;
        defer record.deinit(a);
        try std.testing.expectEqualStrings("v1", record.generation);
        try std.testing.expectEqual(@as(usize, if (cancel) 2 else 1), gate.calls.load(.monotonic));
    }
}

test "external lake metadata navigation pool reclaims idle scopes before owner admission" {
    var pool: NavigationPool = .{ .budget = .{ .backing = std.testing.allocator, .limit = @sizeOf(NavigationOwner) + 1 } };
    defer pool.deinit();
    (try pool.acquire(@splat(1))).release();
    const next = try pool.acquire(@splat(2));
    try std.testing.expectEqual(@as(usize, 2), next.refs.load(.acquire));
    // A live reader cannot be reclaimed to satisfy another scope.
    try std.testing.expectError(error.OutOfMemory, pool.acquire(@splat(3)));
    try std.testing.expectEqual(@as(usize, 2), next.refs.load(.acquire));
    next.release();
    (try pool.acquire(@splat(3))).release();
}

test "external lake read ahead opening hints skip map I/O and fence map identity cancellation and unrelated records" {
    const a = std.testing.allocator;
    var memory = objectstore.MemoryClient.init(a);
    defer memory.deinit();
    const Probe = struct {
        calls: usize = 0,
        fn get(raw: *anyopaque, _: A, _: []const u8, _: []const u8, _: objectstore.GetOptions) !objectstore.GetResult {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.TestMapRead;
        }
    };
    var probe: Probe = .{};
    var vtable = memory.client().vtable.*;
    vtable.get_object = Probe.get;
    const client: objectstore.Client = .{ .allocator = a, .ptr = &probe, .vtable = &vtable };
    const checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const scope = try api.UploadScope.forPublication(@splat(46), 1, std.testing.io);
    const root_id = try scope.artifactId(checksum);
    const object_id = try scope.artifactId("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    const id = &object_id;
    const root: Ref = .{ .artifact_id = &root_id, .checksum = checksum, .byte_len = 1, .generation = "map-v1" };
    var reader: Reader = .{ .client = client, .bucket = "bucket", .prefix = "maps", .root = root };
    defer reader.deinit();
    try reader.bindOpeningHint(.{ .id = id, .bytes = 42, .generation = "object-v7" });
    var selected = (try reader.lookup(a, id, .none)).?;
    defer selected.deinit(a);
    try std.testing.expectEqualStrings("object-v7", selected.generation);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    const unrelated = try scope.artifactId("cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");
    try std.testing.expectError(error.TestMapRead, reader.lookup(a, &unrelated, .none));
    reader.root.?.generation = "map-v2";
    try std.testing.expectError(error.TestMapRead, reader.lookup(a, id, .none));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    reader.root = null;
    try std.testing.expect((try reader.lookup(a, id, .none)) == null);
    reader.root = root;
    const Stop = struct {
        fn canceled(_: *const anyopaque) bool {
            return true;
        }
    };
    var stop: u8 = 0;
    try std.testing.expectError(error.Canceled, reader.lookup(a, id, .{ .ptr = &stop, .is_cancelled_fn = Stop.canceled }));
    try std.testing.expectError(error.InvalidCloudGenerationMap, reader.bindOpeningHint(.{ .id = id, .bytes = 42, .generation = "" }));
    const foreign_scope = try api.UploadScope.forPublication(@splat(47), 1, std.testing.io);
    const foreign_id = try foreign_scope.artifactId(checksum);
    try std.testing.expectError(error.InvalidCloudGenerationMap, reader.bindOpeningHint(.{ .id = &foreign_id, .bytes = 42, .generation = "foreign-v1" }));
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    reader.opening_hint = null;
    const bundle: local.metadata_lake_index_catalog.OpeningBundle = .{
        .artifact = .{ .kind = .doc_values, .metadata_version = 1, .artifact_id = &root_id, .checksum = checksum, .byte_len = 1 },
        .decoded_bytes = 5,
        .provider_locators = &.{.{ .id = id, .bytes = 42, .generation = "published-v8" }},
    };
    try reader.bindPublicationLocators(&.{bundle});
    var trace: local.serverless_query_lake_read_context.IoTrace = .{};
    reader.trace = &trace;
    var direct = (try reader.lookup(a, id, .none)).?;
    defer direct.deinit(a);
    try std.testing.expectEqualStrings("published-v8", direct.generation);
    try std.testing.expectEqual(@as(u64, 1), trace.snapshot().map_locator_hits);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    try std.testing.expectError(error.Canceled, reader.lookup(a, id, .{ .ptr = &stop, .is_cancelled_fn = Stop.canceled }));
    try std.testing.expectError(error.TestMapRead, reader.lookup(a, &unrelated, .none));
    reader.root.?.generation = "changed-root";
    try std.testing.expectError(error.TestMapRead, reader.lookup(a, id, .none));
    reader.root = root;
    var foreign = bundle;
    foreign.provider_locators = &.{.{ .id = &foreign_id, .bytes = 42, .generation = "foreign" }};
    try std.testing.expectError(error.InvalidCloudGenerationMap, reader.bindPublicationLocators(&.{foreign}));
    try std.testing.expectError(error.InvalidCloudGenerationMap, reader.bindPublicationLocators(&.{ bundle, bundle }));
}

test "external lake metadata navigation verified bytes survive reader recreation and isolate credential scopes" {
    const a = std.testing.allocator;
    const Provider = @import("cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    const scope: api.UploadScope = .{ .domain = @splat(54), .attempt = @splat(2) };
    var writer: Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "native" }, .scope = scope };
    defer writer.deinit();
    const id = try scope.artifactId("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    try writer.capture(&id, 42, "v1", .none);
    const root = (try writer.finish(.none)).?;
    defer freeRef(a, root);
    var cache = local.serverless_query_lake_serving_cache.Cache.initWithMemoryLimit(a, 1024 * 1024);
    defer cache.deinit();
    var reads: usize = 0;
    for (0..3) |pass| {
        var reader: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root, .byte_cache = .{ .cache = &cache, .scope = @splat(if (pass == 2) 2 else 1), .context = .{ .io = std.testing.io } } };
        defer reader.deinit();
        var found = (try reader.lookup(a, &id, .none)).?;
        defer found.deinit(a);
        try std.testing.expectEqualStrings("v1", found.generation);
        if (pass == 0) reads = provider.gets;
        if (pass == 1) try std.testing.expectEqual(reads, provider.gets);
        if (pass == 2) try std.testing.expect(provider.gets > reads);
    }
}

fn sameRef(left: Ref, right: Ref) bool {
    return left.byte_len == right.byte_len and left.retention_until_ms == right.retention_until_ms and
        std.mem.eql(u8, left.artifact_id, right.artifact_id) and std.mem.eql(u8, left.checksum, right.checksum) and std.mem.eql(u8, left.generation, right.generation);
}
pub fn freeOpeningPages(a: A, pages: []const local.metadata_lake_index_catalog.CloudNavigationPage) void {
    for (pages) |page| {
        freeRef(a, page.ref);
        a.free(page.body);
    }
    if (pages.len != 0) a.free(pages);
}
/// A bounded copy of the authenticated map spine, carried in the publication.
/// No extra object is created, so the map never depends on its own seed pack.
pub fn collectOpeningPages(reader: *Reader, a: A, token: Token) ![]const local.metadata_lake_index_catalog.CloudNavigationPage {
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const sa = scratch.allocator();
    var output: std.ArrayList(local.metadata_lake_index_catalog.CloudNavigationPage) = .empty;
    defer output.deinit(a);
    errdefer for (output.items) |page| {
        freeRef(a, page.ref);
        a.free(page.body);
    };
    var queue: [17]struct { ref: Ref, depth: u8 } = undefined;
    queue[0] = .{ .ref = reader.root orelse return &.{}, .depth = 0 };
    var count: usize = 1;
    var position: usize = 0;
    var remaining: usize = 32 * 1024;
    while (position < count) : (position += 1) {
        const entry = queue[position];
        if (entry.ref.byte_len > remaining) continue;
        const bytes = try reader.readBytes(sa, entry.ref, token);
        const node = try std.json.parseFromSliceLeaky(Node, sa, bytes, .{ .allocate = .alloc_always });
        try node.validate(entry.depth);
        try reader.validateDomain(node, entry.ref);
        const owned_ref = try cloneRef(a, entry.ref);
        errdefer freeRef(a, owned_ref);
        const body = try a.dupe(u8, bytes);
        errdefer a.free(body);
        try output.append(a, .{ .ref = owned_ref, .body = body });
        remaining -= bytes.len;
        if (entry.depth == 0) for (node.children) |child| {
            if (count == queue.len) break;
            queue[count] = .{ .ref = child.ref, .depth = 1 };
            count += 1;
        };
    }
    return output.toOwnedSlice(a);
}

test "external lake inline map navigation removes cold dependency reads and fences the exact root" {
    const a = std.testing.allocator;
    const Provider = @import("cloud_generations_test_provider.zig").Provider;
    var provider = Provider.init(a);
    defer provider.deinit();
    var client = provider.client();
    try client.makeBucket("bucket");
    const scope: api.UploadScope = .{ .domain = @splat(61), .attempt = @splat(2) };
    var writer: Writer = .{ .a = a, .reader = .{ .client = client, .bucket = "bucket", .prefix = "native" }, .scope = scope };
    defer writer.deinit();
    const id = try scope.artifactId("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    try writer.capture(&id, 42, "payload-v1", .none);
    const root = (try writer.finish(.none)).?;
    defer freeRef(a, root);
    var seed_reader: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root };
    defer seed_reader.deinit();
    const pages = try collectOpeningPages(&seed_reader, a, .none);
    defer freeOpeningPages(a, pages);
    try std.testing.expectEqual(@as(usize, 1), pages.len);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var reader: Reader = .{ .client = client, .bucket = "bucket", .prefix = "native", .root = root };
    defer reader.deinit();
    try reader.bindOpeningPages(pages, .none);
    const reads = provider.gets;
    var selected = (try reader.lookup(arena.allocator(), &id, .none)).?;
    defer selected.deinit(arena.allocator());
    try std.testing.expectEqualStrings("payload-v1", selected.generation);
    try std.testing.expectEqual(reads, provider.gets);
    var damaged = [_]local.metadata_lake_index_catalog.CloudNavigationPage{pages[0]};
    var bytes = try a.dupe(u8, damaged[0].body);
    defer a.free(bytes);
    bytes[0] ^= 1;
    damaged[0].body = bytes;
    try std.testing.expectError(error.ArtifactIntegrityMismatch, reader.bindOpeningPages(&damaged, .none));
    const original = reader.root;
    reader.root.?.generation = "different-root";
    defer reader.root = original;
    try std.testing.expectError(error.CloudGenerationMapUnavailable, reader.lookup(arena.allocator(), &id, .none));
}
