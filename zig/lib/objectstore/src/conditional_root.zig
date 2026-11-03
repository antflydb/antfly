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

//! A directly conditional metadata authority. ROOT is permanent: collectors may
//! retire referenced immutable pages, never this authority or its fencing nonce.
//! Existing journal namespaces retain their original protocol.
const std = @import("std");
const Client = @import("client.zig").Client;
const T = @import("types.zig");
const journal = @import("immutable_log.zig");
const A = std.mem.Allocator;
const Hash = std.crypto.hash.sha2.Sha256;

pub const Cursor = struct { revision: ?T.ObjectRevision, nonce: [16]u8, retired: bool = false, auth: ?[32]u8 = null };

pub const Snapshot = struct {
    state: journal.State,
    revision: ?T.ObjectRevision,
    nonce: [16]u8,
    retired: bool = false,
    auth: ?[32]u8 = null,

    pub fn cursor(self: Snapshot) Cursor {
        return .{ .revision = self.revision, .nonce = self.nonce, .retired = self.retired, .auth = self.auth };
    }

    pub fn deinit(self: *Snapshot, a: A) void {
        a.free(self.state.root);
        if (self.revision) |*r| r.deinit(a);
        self.* = undefined;
    }
};
const Stored = struct {
    format: u8 = 1,
    namespace: [32]u8,
    nonce: [16]u8,
    retired: bool = false,
    state: journal.State,
};

/// Proofs contain only hashes. They are capabilities scoped to one store/binding
/// lifetime, not a cache of current authority. Every write still performs CAS.
pub const ProofCache = struct {
    const capacity = 16;
    mu: std.atomic.Mutex = .unlocked,
    entries: [capacity]?[32]u8 = @splat(null),
    next: usize = 0,
    pub fn clear(self: *ProofCache) void {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        self.entries = @splat(null);
        self.next = 0;
    }
    fn contains(self: *ProofCache, key: [32]u8) bool {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        for (self.entries) |entry| if (entry) |value| if (std.mem.eql(u8, &value, &key)) return true;
        return false;
    }
    fn remember(self: *ProofCache, key: [32]u8) void {
        @import("antfly_platform").sync.lockYielding(&self.mu);
        defer self.mu.unlock();
        for (self.entries) |entry| if (entry) |value| if (std.mem.eql(u8, &value, &key)) return;
        self.entries[self.next] = key;
        self.next = (self.next + 1) % capacity;
    }
};

pub const Context = struct {
    client: Client,
    bucket: []const u8,
    prefix: []const u8,
    namespace: []const u8,
    owner: [16]u8,
    io: std.Io,
    cancellation: ?T.CancellationToken = null,
    proofs: ?*ProofCache = null,
    /// An independently generated identity for the host's store/binding
    /// capability. Zero disables continuation reuse. Never take it from users.
    proof_scope: [16]u8 = @splat(0),
    /// Private host capability key. Never persisted or returned. It permits
    /// authenticated cursors to cross pooled kernels for this same binding.
    proof_key: ?[32]u8 = null,

    fn check(self: Context) !void {
        if (self.cancellation) |token| try token.check();
    }
    fn namespaceHash(self: Context) [32]u8 {
        var result: [32]u8 = undefined;
        Hash.hash(self.namespace, &result, .{});
        return result;
    }
    fn key(self: Context, a: A) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/ROOT", .{self.prefix});
    }
    fn validate(state: journal.State) !void {
        if (state.format != 1 or state.revision > 9_007_199_254_740_991 or state.next >= journal.Log.epoch_commits or state.root.len > journal.max_root_bytes) return error.InvalidConditionalRoot;
    }
    fn field(hash: *Hash, bytes: []const u8) void {
        var size: [8]u8 = undefined;
        std.mem.writeInt(u64, &size, bytes.len, .little);
        hash.update(&size);
        hash.update(bytes);
    }
    fn digest(self: Context, snapshot: Snapshot, physical: bool) ?[32]u8 {
        const revision = snapshot.revision orelse return null;
        var hash = Hash.init(.{});
        field(&hash, self.bucket);
        field(&hash, self.prefix);
        field(&hash, self.namespace);
        field(&hash, &self.proof_scope);
        if (physical) {
            var pointer: [8]u8 = undefined;
            std.mem.writeInt(u64, &pointer, @intFromPtr(self.client.ptr), .little);
            field(&hash, &pointer);
            std.mem.writeInt(u64, &pointer, @intFromPtr(self.client.vtable), .little);
            field(&hash, &pointer);
        }
        field(&hash, &snapshot.state.owner);
        field(&hash, &snapshot.state.epoch);
        field(&hash, &snapshot.nonce);
        field(&hash, snapshot.state.root);
        var counters: [17]u8 = undefined;
        std.mem.writeInt(u64, counters[0..8], snapshot.state.revision, .little);
        std.mem.writeInt(u64, counters[8..16], snapshot.state.next, .little);
        counters[16] = @intFromBool(snapshot.retired);
        field(&hash, &counters);
        switch (revision) {
            .etag => |value| {
                field(&hash, "etag");
                field(&hash, value);
            },
            .generation => |value| {
                field(&hash, "generation");
                field(&hash, value);
            },
        }
        return hash.finalResult();
    }
    fn remember(self: Context, snapshot: Snapshot) void {
        if (std.mem.allEqual(u8, &self.proof_scope, 0)) return;
        if (self.proofs) |proofs| if (self.digest(snapshot, true)) |value| proofs.remember(value);
    }
    fn known(self: Context, snapshot: Snapshot) bool {
        if (std.mem.allEqual(u8, &self.proof_scope, 0)) return false;
        if (self.proof_key) |capability_key| {
            const supplied = snapshot.auth orelse return false;
            const message = self.digest(snapshot, false) orelse return false;
            var expected: [32]u8 = undefined;
            std.crypto.auth.hmac.sha2.HmacSha256.create(&expected, &message, &capability_key);
            return std.crypto.timing_safe.eql([32]u8, supplied, expected);
        }
        if (std.mem.allEqual(u8, &self.proof_scope, 0)) return false;
        const proofs = self.proofs orelse return false;
        return if (self.digest(snapshot, true)) |value| proofs.contains(value) else false;
    }
    fn sign(self: Context, snapshot: *Snapshot) void {
        if (std.mem.allEqual(u8, &self.proof_scope, 0)) return;
        const capability_key = self.proof_key orelse return;
        const message = self.digest(snapshot.*, false) orelse return;
        var signature: [32]u8 = undefined;
        std.crypto.auth.hmac.sha2.HmacSha256.create(&signature, &message, &capability_key);
        snapshot.auth = signature;
    }
    pub fn observe(self: Context, a: A) !?Snapshot {
        try self.check();
        const name = try self.key(a);
        defer a.free(name);
        var client = self.client;
        client.allocator = a;
        var object = client.getObject(self.bucket, name, .{ .skip_metadata_probe = true, .max_response_bytes = 16384, .cancellation = self.cancellation }) catch |err| switch (err) {
            error.FileNotFound => return null,
            else => return err,
        };
        defer object.deinit(a);
        if (object.body.len > 16384) return error.InvalidConditionalRoot;
        var parsed = try std.json.parseFromSlice(Stored, a, object.body, .{ .allocate = .alloc_always });
        defer parsed.deinit();
        const stored = parsed.value;
        try validate(stored.state);
        if (stored.format != 1 or !std.mem.eql(u8, &stored.namespace, &self.namespaceHash())) return error.InvalidConditionalRoot;
        const revision = object.metadata.writeRevision() orelse return error.ConditionalReadUnsupported;
        var owned_revision = try revision.clone(a);
        errdefer owned_revision.deinit(a);
        var snapshot = Snapshot{ .state = .{ .format = stored.state.format, .epoch = stored.state.epoch, .owner = stored.state.owner, .next = stored.state.next, .revision = stored.state.revision, .root = try a.dupe(u8, stored.state.root) }, .revision = owned_revision, .nonce = stored.nonce, .retired = stored.retired };
        self.sign(&snapshot);
        self.remember(snapshot);
        return snapshot;
    }
    pub fn begin(self: Context, a: A) !Snapshot {
        var loaded = try self.observe(a);
        if (loaded == null) {
            var epoch: [16]u8 = undefined;
            var nonce: [16]u8 = undefined;
            self.io.random(&epoch);
            self.io.random(&nonce);
            const body = try std.json.Stringify.valueAlloc(a, Stored{ .namespace = self.namespaceHash(), .nonce = nonce, .state = .{ .owner = self.owner, .epoch = epoch } }, .{});
            defer a.free(body);
            const name = try self.key(a);
            defer a.free(name);
            var client = self.client;
            client.allocator = a;
            if (client.compareAndPut(self.bucket, name, body, null, .{ .content_type = "application/json", .cancellation = self.cancellation })) |result| {
                var receipt = result;
                receipt.deinit(a);
            } else |err| if (err != error.PreconditionFailed) return err;
            loaded = (try self.observe(a)) orelse return error.ConditionalRootMissing;
        }
        var snapshot = loaded.?;
        errdefer snapshot.deinit(a);
        if (snapshot.retired) return error.ConditionalRootRetired;
        if (std.mem.eql(u8, &snapshot.state.owner, &self.owner)) return snapshot;
        const claimed = try self.replace(a, snapshot, snapshot.state.root, false, true);
        snapshot.deinit(a);
        return claimed;
    }
    fn authenticate(self: Context, a: A, expected: Snapshot) !void {
        try validate(expected.state);
        if (expected.retired) return error.ConditionalRootRetired;
        if (!std.mem.eql(u8, &expected.state.owner, &self.owner)) return error.ImmutableLogFenced;
        if (expected.revision == null) return error.ConditionalRootRevisionRequired;
        if (self.known(expected)) return;
        var actual = (try self.observe(a)) orelse return error.ConditionalRootMissing;
        defer actual.deinit(a);
        const wanted = self.digest(expected, false) orelse return error.ConditionalRootRevisionRequired;
        const found = self.digest(actual, false) orelse return error.ConditionalRootRevisionRequired;
        if (!std.mem.eql(u8, &wanted, &found)) return error.PreconditionFailed;
    }
    /// Authenticate a source for a subsequent conditional operation. This does
    /// not assert that a cached source remains the latest root.
    pub fn verifySource(self: Context, a: A, expected: Snapshot) !void {
        try self.authenticate(a, expected);
    }

    pub fn append(self: Context, a: A, expected: Snapshot, root: []const u8) !Snapshot {
        try self.authenticate(a, expected);
        return self.replace(a, expected, root, false, false);
    }
    pub fn retire(self: Context, a: A, expected: Snapshot) !Snapshot {
        try self.authenticate(a, expected);
        return self.replace(a, expected, expected.state.root, true, false);
    }
    fn replace(self: Context, a: A, expected: Snapshot, root: []const u8, retired: bool, claim: bool) !Snapshot {
        try self.check();
        if (root.len > journal.max_root_bytes) return error.CoordinatorRootBudgetExceeded;
        const revision = expected.revision orelse return error.ConditionalRootRevisionRequired;
        const next_revision = try std.math.add(u64, expected.state.revision, 1);
        const owned_root = try a.dupe(u8, root);
        errdefer a.free(owned_root);
        var nonce: [16]u8 = undefined;
        self.io.random(&nonce);
        const state = journal.State{ .epoch = expected.state.epoch, .owner = if (claim) self.owner else expected.state.owner, .revision = next_revision, .next = next_revision % journal.Log.epoch_commits, .root = owned_root };
        try validate(state);
        const bytes = try std.json.Stringify.valueAlloc(a, Stored{ .namespace = self.namespaceHash(), .nonce = nonce, .retired = retired, .state = state }, .{});
        defer a.free(bytes);
        if (bytes.len > 16384) return error.CoordinatorRootBudgetExceeded;
        const name = try self.key(a);
        defer a.free(name);
        var client = self.client;
        client.allocator = a;
        var receipt = try client.compareAndPut(self.bucket, name, bytes, revision, .{ .content_type = "application/json", .cancellation = self.cancellation });
        defer receipt.deinit(a);
        // Transfer response-owned revision bytes without allocating after commit.
        // Missing response evidence leaves a valid ack but requires fresh recovery.
        const next_token: ?T.ObjectRevision = switch (revision) {
            .etag => if (receipt.etag) |value| blk: {
                receipt.etag = null;
                break :blk .{ .etag = value };
            } else null,
            .generation => if (receipt.version_id) |value| blk: {
                receipt.version_id = null;
                break :blk .{ .generation = value };
            } else null,
        };
        var result = Snapshot{ .state = state, .revision = next_token, .nonce = nonce, .retired = retired };
        self.sign(&result);
        self.remember(result);
        return result;
    }
};

test "conditional root preserves state and rejects stale owners and fabricated continuations" {
    const a = std.testing.allocator;
    var memory = @import("memory.zig").MemoryClient.init(a);
    defer memory.deinit();
    var client = memory.client();
    try client.makeBucket("db");
    var proofs = ProofCache{};
    const first = Context{ .client = client, .bucket = "db", .prefix = "metadata", .namespace = "one", .owner = @splat(1), .io = std.testing.io, .proofs = &proofs, .proof_scope = @splat(5) };
    var initial = try first.begin(a);
    defer initial.deinit(a);
    var committed = try first.append(a, initial, "accepted");
    defer committed.deinit(a);
    try std.testing.expectEqualStrings("accepted", committed.state.root);
    try std.testing.expectError(error.PreconditionFailed, first.append(a, initial, "fork"));
    var forged = committed;
    forged.state.root = "fabricated";
    try std.testing.expectError(error.PreconditionFailed, first.append(a, forged, "bad"));
    const second = Context{ .client = client, .bucket = "db", .prefix = "metadata", .namespace = "one", .owner = @splat(2), .io = std.testing.io, .proofs = &proofs, .proof_scope = @splat(6) };
    var claimed = try second.begin(a);
    defer claimed.deinit(a);
    try std.testing.expectEqualStrings("accepted", claimed.state.root);
    try std.testing.expectError(error.PreconditionFailed, first.append(a, committed, "stale owner"));
    var next = try second.append(a, claimed, "new owner");
    defer next.deinit(a);
    proofs.clear();
    var after_cache_loss = try second.append(a, next, "verified after cold cache");
    defer after_cache_loss.deinit(a);
    var retired = try second.retire(a, after_cache_loss);
    defer retired.deinit(a);
    try std.testing.expectError(error.ConditionalRootRetired, second.begin(a));
    try std.testing.expectError(error.ConditionalRootRetired, second.append(a, retired, "resurrection"));
    try std.testing.expectError(error.PreconditionFailed, first.append(a, initial, "old generation"));
    var other = first;
    other.namespace = "other";
    try std.testing.expectError(error.InvalidConditionalRoot, other.observe(a));
}

const TestBackend = struct {
    memory: @import("memory.zig").MemoryClient,
    gets: usize = 0,
    puts: usize = 0,
    generation: u64 = 9_007_199_254_740_993,
    generations: bool = false,
    lose_next_ack: bool = false,
    omit_next_revision: bool = false,
    fn get(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8, opts: T.GetOptions) !T.GetResult {
        const self: *TestBackend = @ptrCast(@alignCast(raw));
        self.gets += 1;
        var base = self.memory.client();
        base.allocator = a;
        var result = try base.getObject(bucket, name, opts);
        if (self.generations) {
            if (result.metadata.version_id) |old| a.free(old);
            result.metadata.version_id = try std.fmt.allocPrint(a, "{d}", .{self.generation});
            result.metadata.revision_kind = .generation;
        }
        return result;
    }
    fn put(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8, body: []const u8, opts: T.PutOptions) !T.PutResult {
        const self: *TestBackend = @ptrCast(@alignCast(raw));
        self.puts += 1;
        var base = self.memory.client();
        base.allocator = a;
        var result = try base.putObject(bucket, name, body, opts);
        self.generation += 1;
        if (self.generations) {
            if (result.version_id) |old| a.free(old);
            result.version_id = try std.fmt.allocPrint(a, "{d}", .{self.generation});
        }
        if (self.lose_next_ack) {
            self.lose_next_ack = false;
            result.deinit(a);
            return error.InjectedLostAck;
        }
        if (self.omit_next_revision) {
            self.omit_next_revision = false;
            result.deinit(a);
            return .{};
        }
        return result;
    }
    fn putRevision(raw: *anyopaque, a: A, bucket: []const u8, name: []const u8, body: []const u8, revision: T.ObjectRevision, opts: T.PutOptions) !T.PutResult {
        const self: *TestBackend = @ptrCast(@alignCast(raw));
        var text: [32]u8 = undefined;
        const actual = try std.fmt.bufPrint(&text, "{d}", .{self.generation});
        switch (revision) {
            .etag => return error.ObjectOperationUnsupported,
            .generation => |expected| if (!std.mem.eql(u8, actual, expected)) return error.PreconditionFailed,
        }
        var base = self.memory.client();
        base.allocator = a;
        // The test backend maps a generation guard to its in-memory atomic
        // revision primitive. This is internal storage work, not a network GET.
        var existing = try base.getObject(bucket, name, .{});
        defer existing.deinit(a);
        var conditional = opts;
        conditional.if_match_etag = existing.metadata.etag;
        return put(raw, a, bucket, name, body, conditional);
    }
    fn wrap(self: *TestBackend, table: *Client.VTable, generations: bool) Client {
        self.generations = generations;
        const base = self.memory.client();
        table.* = base.vtable.*;
        table.get_object = get;
        table.put_object = put;
        table.put_at_revision = if (generations) putRevision else null;
        return .{ .allocator = base.allocator, .ptr = self, .vtable = table };
    }
};

test "authenticated conditional continuations need one conditional write and no source reread" {
    const a = std.testing.allocator;
    var backend = TestBackend{ .memory = @import("memory.zig").MemoryClient.init(a) };
    defer backend.memory.deinit();
    var base = backend.memory.client();
    try base.makeBucket("db");
    var table: Client.VTable = undefined;
    var proofs = ProofCache{};
    const ctx = Context{ .client = backend.wrap(&table, false), .bucket = "db", .prefix = "fast", .namespace = "one", .owner = @splat(1), .io = std.testing.io, .proofs = &proofs, .proof_scope = @splat(7) };
    var current = try ctx.begin(a);
    defer current.deinit(a);
    const reads = backend.gets;
    for (0..100) |_| {
        var next = try ctx.append(a, current, "constant logical body, unique commit");
        current.deinit(a);
        current = next;
        next = undefined;
    }
    try std.testing.expectEqual(reads, backend.gets);
    try std.testing.expectEqual(@as(usize, 101), backend.puts);
    try std.testing.expectEqual(@as(u64, 100), current.state.revision);
    const prior = backend.gets;
    proofs.clear();
    var cold = try ctx.append(a, current, "verified on cache miss");
    defer cold.deinit(a);
    try std.testing.expectEqual(prior + 1, backend.gets);
    var no_scope = ctx;
    no_scope.proof_scope = @splat(0);
    const before = backend.gets;
    var uncached = try no_scope.append(a, cold, "safe default");
    defer uncached.deinit(a);
    try std.testing.expectEqual(before + 1, backend.gets);
}

test "conditional root transports preserve GCS-sized opaque generations without numeric coercion" {
    const a = std.testing.allocator;
    var backend = TestBackend{ .memory = @import("memory.zig").MemoryClient.init(a) };
    defer backend.memory.deinit();
    var base = backend.memory.client();
    try base.makeBucket("db");
    var table: Client.VTable = undefined;
    var proofs = ProofCache{};
    const ctx = Context{ .client = backend.wrap(&table, true), .bucket = "db", .prefix = "generation", .namespace = "one", .owner = @splat(1), .io = std.testing.io, .proofs = &proofs, .proof_scope = @splat(8) };
    var first = try ctx.begin(a);
    defer first.deinit(a);
    try std.testing.expectEqualStrings("9007199254740994", first.revision.?.generation);
    var next = try ctx.append(a, first, "opaque");
    defer next.deinit(a);
    try std.testing.expectEqualStrings("9007199254740995", next.revision.?.generation);
    try std.testing.expectError(error.PreconditionFailed, ctx.append(a, first, "stale generation"));
    table.put_at_revision = null;
    proofs.clear();
    try std.testing.expectError(error.ObjectOperationUnsupported, ctx.append(a, next, "unsupported is not unconditional"));
    var still = (try ctx.observe(a)).?;
    defer still.deinit(a);
    try std.testing.expectEqualStrings("opaque", still.state.root);
}

test "lost conditional acknowledgements preserve accepted state and require fresh observation" {
    const a = std.testing.allocator;
    var backend = TestBackend{ .memory = @import("memory.zig").MemoryClient.init(a) };
    defer backend.memory.deinit();
    var base = backend.memory.client();
    try base.makeBucket("db");
    var table: Client.VTable = undefined;
    var proofs = ProofCache{};
    const ctx = Context{ .client = backend.wrap(&table, false), .bucket = "db", .prefix = "lost", .namespace = "one", .owner = @splat(1), .io = std.testing.io, .proofs = &proofs, .proof_scope = @splat(9) };
    var first = try ctx.begin(a);
    defer first.deinit(a);
    backend.lose_next_ack = true;
    try std.testing.expectError(error.InjectedLostAck, ctx.append(a, first, "accepted once"));
    var recovered = try ctx.begin(a);
    defer recovered.deinit(a);
    try std.testing.expectEqualStrings("accepted once", recovered.state.root);
    try std.testing.expectEqual(@as(u64, 1), recovered.state.revision);
    try std.testing.expectError(error.PreconditionFailed, ctx.append(a, first, "never blindly replay"));
    backend.omit_next_revision = true;
    var acknowledged = try ctx.append(a, recovered, "ack without a reusable revision");
    defer acknowledged.deinit(a);
    try std.testing.expectEqual(@as(?T.ObjectRevision, null), acknowledged.revision);
    try std.testing.expectError(error.ConditionalRootRevisionRequired, ctx.append(a, acknowledged, "must recover"));
    var reopened = try ctx.begin(a);
    defer reopened.deinit(a);
    try std.testing.expectEqualStrings("ack without a reusable revision", reopened.state.root);
}

test "conditional root rejects oversized encodings before any write" {
    const a = std.testing.allocator;
    var backend = TestBackend{ .memory = @import("memory.zig").MemoryClient.init(a) };
    defer backend.memory.deinit();
    var base = backend.memory.client();
    try base.makeBucket("db");
    var table: Client.VTable = undefined;
    const ctx = Context{ .client = backend.wrap(&table, false), .bucket = "db", .prefix = "budget", .namespace = "one", .owner = @splat(1), .io = std.testing.io };
    var first = try ctx.begin(a);
    defer first.deinit(a);
    const puts = backend.puts;
    const oversized = try a.alloc(u8, journal.max_root_bytes + 1);
    defer a.free(oversized);
    @memset(oversized, 'x');
    try std.testing.expectError(error.CoordinatorRootBudgetExceeded, ctx.append(a, first, oversized));
    @memset(oversized, '"');
    try std.testing.expectError(error.CoordinatorRootBudgetExceeded, ctx.append(a, first, oversized[0..journal.max_root_bytes]));
    try std.testing.expectEqual(puts, backend.puts);
}

test "signed native continuations cross pooled kernels but never certify fabricated state" {
    const a = std.testing.allocator;
    var backend = TestBackend{ .memory = @import("memory.zig").MemoryClient.init(a) };
    defer backend.memory.deinit();
    var base = backend.memory.client();
    try base.makeBucket("db");
    var table: Client.VTable = undefined;
    var proofs = ProofCache{};
    const ctx = Context{ .client = backend.wrap(&table, false), .bucket = "db", .prefix = "signed", .namespace = "one", .owner = @splat(1), .io = std.testing.io, .proofs = &proofs, .proof_scope = @splat(10), .proof_key = @splat(42) };
    var first = try ctx.begin(a);
    defer first.deinit(a);
    try std.testing.expect(first.auth != null);
    const encoded = try std.json.Stringify.valueAlloc(a, first.cursor(), .{});
    defer a.free(encoded);
    var parsed = try std.json.parseFromSlice(Cursor, a, encoded, .{});
    defer parsed.deinit();
    try std.testing.expectEqualSlices(u8, &first.auth.?, &parsed.value.auth.?);
    const reads = backend.gets;
    proofs.clear();
    var cold_cache = ProofCache{};
    const other_table = table;
    var pooled = ctx;
    pooled.client.vtable = &other_table;
    pooled.proofs = &cold_cache;
    var next = try pooled.append(a, first, "portable");
    defer next.deinit(a);
    try std.testing.expectEqual(reads, backend.gets);
    var forged = next;
    forged.state.root = "not committed";
    const before_forgery = backend.puts;
    try std.testing.expectError(error.PreconditionFailed, pooled.append(a, forged, "must not publish"));
    try std.testing.expectEqual(before_forgery, backend.puts);
    try std.testing.expectEqual(reads + 1, backend.gets);
    var rotated = pooled;
    rotated.proof_key = @splat(43);
    const before_rotation = backend.gets;
    var reauthenticated = try rotated.append(a, next, "freshly verified after key rotation");
    defer reauthenticated.deinit(a);
    try std.testing.expectEqual(before_rotation + 1, backend.gets);
    var stored = try base.getObject("db", "signed/ROOT", .{});
    defer stored.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, stored.body, "\"proof_key\"") == null);
    try std.testing.expect(std.mem.indexOf(u8, stored.body, "\"auth\"") == null);
}
