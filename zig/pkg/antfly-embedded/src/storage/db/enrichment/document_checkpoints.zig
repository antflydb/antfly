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

//! Document executor persistence owned by an enrichment source and producer.
//! No public jobs, independent leases, or wall-clock retention are introduced.
const std = @import("std");
const readers = @import("antfly_readers");
const runs = readers.document_runs;
const rpc = readers.document_rpc;
const A = std.mem.Allocator;
pub const root_prefix = "\x00\x00__metadata__:enrichment_document:";
pub const blob_prefix = "\x00\x00__metadata__:enrichment_document_blob:";
pub const Write = struct { key: []const u8, value: []const u8 };
pub const Binding = struct {
    document: []const u8,
    producer: []const u8,
    source_key: []const u8,
    source_digest: [32]u8,
    producer_digest: [32]u8,
    catalog_digest: [32]u8 = @splat(0),
    namespace: [24]u8 = @splat(0),
    authority_digest: [32]u8 = @splat(0),
    authority_json: ?[]const u8 = null,
    catalog_epoch: u64 = 0,
    source_sequence: u64 = 0,
    generation: u64 = 0,
};
pub const Root = struct { binding: Binding, run: runs.Record };
pub const Backend = struct {
    ptr: *anyopaque,
    get: *const fn (*anyopaque, A, []const u8) anyerror!?[]u8,
    /// Must atomically validate the captured enrichment lease, catalog and all
    /// source guards along with the expected root before applying any writes.
    mutate: *const fn (*anyopaque, []const u8, ?[]const u8, []const Write, []const []const u8) anyerror!bool,
    keys: *const fn (*anyopaque, A, []const u8, usize) anyerror![][]u8,
};
pub fn hash(bytes: []const u8) [32]u8 {
    var value: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &value, .{});
    return value;
}
pub fn scope(a: A, document: []const u8, producer: []const u8) ![]u8 {
    const encoded = try std.json.Stringify.valueAlloc(a, .{ document, producer }, .{});
    defer a.free(encoded);
    return a.dupe(u8, &std.fmt.bytesToHex(hash(encoded), .lower));
}
pub fn rootKey(a: A, id: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ root_prefix, id });
}
pub fn blobScope(a: A, id: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ blob_prefix, id, ":" });
}

/// The invocation owner borrows the runtime, but every reverse RPC retains a
/// binding. `drain` is mandatory before that runtime/source proof may disappear.
pub const Owner = struct {
    allocator: A,
    backend: Backend,
    binding: Binding,
    io: std.Io,
    cancellation: @import("antfly_cancellation").CancellationToken = .none,
    references: std.atomic.Value(usize) = .init(0),
    pub fn factory(self: *Owner) rpc.Factory {
        return .{ .ptr = self, .bind_fn = bind };
    }
    pub fn drain(self: *Owner) void {
        while (self.references.load(.acquire) != 0) self.io.sleep(.fromMilliseconds(1), .awake) catch {};
    }
    const Call = struct {
        a: A,
        backing: A,
        memory: RetainedMemory,
        owner: *Owner,
        root_key: []const u8,
        blob_scope: []const u8,
        id: []const u8,
        mutex: std.Io.Mutex = .init,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const owner = self.owner;
            const a = self.a;
            a.free(self.root_key);
            a.free(self.blob_scope);
            a.free(self.id);
            self.backing.destroy(self);
            _ = owner.references.fetchSub(1, .release);
        }
        fn store(self: *@This()) runs.Store {
            return .{ .ptr = self, .load_fn = load, .cas_fn = cas, .nonce_fn = nonce, .put_fn = put, .get_fn = get };
        }
        fn root(self: *@This(), a: A) !?Root {
            const encoded = (try self.owner.backend.get(self.owner.backend.ptr, a, self.root_key)) orelse return null;
            return try std.json.parseFromSliceLeaky(Root, a, encoded, .{ .allocate = .alloc_always });
        }
        fn load(raw: *anyopaque, a: A, id: []const u8, owner: []const u8) !?runs.Record {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.owner.cancellation.check();
            if (!std.mem.eql(u8, id, self.id) or !std.mem.eql(u8, owner, self.root_key)) return error.DocumentRunNotFound;
            const value = (try self.root(a)) orelse return null;
            if (!std.mem.eql(u8, value.run.run_id, self.id)) return error.EnrichmentSourceChanged;
            return value.run;
        }
        fn nonce(raw: *anyopaque, out: []u8) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.owner.io.random(out);
        }
        fn cas(raw: *anyopaque, expected: ?runs.Version, next: runs.Record) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.owner.cancellation.check();
            if (!std.mem.eql(u8, next.run_id, self.id) or !std.mem.eql(u8, next.owner, self.root_key)) return error.DocumentRunNotFound;
            try self.mutex.lock(self.owner.io);
            defer self.mutex.unlock(self.owner.io);
            var arena = std.heap.ArenaAllocator.init(self.a);
            defer arena.deinit();
            const a = arena.allocator();
            const encoded = try self.owner.backend.get(self.owner.backend.ptr, a, self.root_key);
            if (encoded) |bytes| {
                const old = try std.json.parseFromSliceLeaky(Root, a, bytes, .{});
                if (expected == null or old.run.retired or !std.mem.eql(u8, old.run.run_id, self.id) or !std.meta.eql(old.run.version(), expected.?)) return false;
                if (next.revision != old.run.revision + 1 or next.retention_until_ms != 0 or next.operation != old.run.operation) return error.InvalidDocumentCheckpoint;
                try runs.validateIdentityTransition(old.run, next);
            } else if (expected != null) return false;
            var fenced = next;
            // This record is never scheduled by its protocol timestamp. The
            // captured enrichment lease/source proof is checked transactionally
            // by the backend on every write, including provider reservations.
            // Avoid a second timeout/heartbeat owner for long index invocations.
            if (fenced.state == .running) fenced.lease_until_ms = std.math.maxInt(u64);
            const bytes = try std.json.Stringify.valueAlloc(a, Root{ .binding = self.owner.binding, .run = fenced }, .{});
            if (bytes.len > rpc.max_metadata_bytes) return error.DocumentOutputTooLarge;
            return self.owner.backend.mutate(self.owner.backend.ptr, self.root_key, encoded, &.{.{ .key = self.root_key, .value = bytes }}, &.{}) catch |err| return protocolError(err);
        }
        fn authorize(raw: *anyopaque, reference: runs.Ref) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!std.mem.startsWith(u8, reference.id, self.blob_scope)) return error.DocumentRunNotFound;
        }
        fn put(raw: *anyopaque, a: A, upload: runs.Scope, bytes: []const u8) !runs.Ref {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.owner.cancellation.check();
            if (bytes.len > rpc.max_payload_bytes) return error.DocumentOutputTooLarge;
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const scratch = arena.allocator();
            const encoded = (try self.owner.backend.get(self.owner.backend.ptr, scratch, self.root_key)) orelse return error.DocumentRunNotFound;
            const value = try std.json.parseFromSliceLeaky(Root, scratch, encoded, .{});
            if (!std.mem.eql(u8, upload.run_id, self.id) or !std.mem.eql(u8, upload.owner, self.root_key) or !std.mem.eql(u8, value.run.run_id, self.id) or value.run.retired or value.run.state != .running or value.run.attempt != upload.attempt or !std.mem.eql(u8, &value.run.nonce, &upload.nonce)) return error.DocumentRunLeaseLost;
            const digest = hash(bytes);
            const id = try std.fmt.allocPrint(a, "{s}{s}", .{ self.blob_scope, std.fmt.bytesToHex(digest, .lower) });
            errdefer a.free(id);
            if (!(self.owner.backend.mutate(self.owner.backend.ptr, self.root_key, encoded, &.{.{ .key = id, .value = bytes }}, &.{}) catch |err| return protocolError(err))) return error.DocumentRunLeaseLost;
            return .{ .id = id, .bytes = bytes.len, .sha256 = digest };
        }
        fn get(raw: *anyopaque, a: A, reference: runs.Ref, max: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.owner.cancellation.check();
            try authorize(raw, reference);
            if (reference.bytes > max) return error.DocumentOutputTooLarge;
            return (try self.owner.backend.get(self.owner.backend.ptr, a, reference.id)) orelse error.DocumentRunNotFound;
        }
    };
    fn bind(raw: *anyopaque, a: A, bytes: []const u8, extract: bool) !*rpc.OwnedServer {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.cancellation.check();
        const id = try scope(a, self.binding.document, self.binding.producer);
        defer a.free(id);
        const request_hash = hash(bytes);
        const binding_json = try std.json.Stringify.valueAlloc(a, self.binding, .{});
        defer a.free(binding_json);
        var identity_hash = std.crypto.hash.sha2.Sha256.init(.{});
        identity_hash.update(binding_json);
        identity_hash.update(&request_hash);
        const call = try self.allocator.create(Call);
        errdefer self.allocator.destroy(call);
        call.* = .{ .a = undefined, .backing = self.allocator, .memory = .{ .budget = .init(self.allocator, 256 * 1024 * 1024) }, .owner = self, .root_key = &.{}, .blob_scope = &.{}, .id = &.{} };
        const retained = call.memory.allocator();
        call.a = retained;
        const run_id = try retained.dupe(u8, &std.fmt.bytesToHex(identity_hash.finalResult(), .lower));
        errdefer retained.free(run_id);
        const root_key = try rootKey(retained, id);
        errdefer retained.free(root_key);
        const blob_scope = try blobScope(retained, id);
        errdefer retained.free(blob_scope);
        call.id = run_id;
        call.root_key = root_key;
        call.blob_scope = blob_scope;
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const store = call.store();
        const initial: runs.Identity = .{ .request = request_hash, .sources = @splat(0), .definition = @splat(0), .models = @splat(0), .runtime = @splat(0) };
        if (try call.root(scratch)) |prior| {
            if (!std.mem.eql(u8, prior.run.run_id, run_id) or prior.run.retired) {
                if (!try collect(self.backend, scratch, root_key, blob_scope, 32)) return error.WriterLocked;
            }
        }
        if (try call.root(scratch) == null) try store.create(.{ .run_id = run_id, .owner = root_key, .operation = if (extract) .extract else .read, .identity = initial, .prepared = false, .limits = .{ .calls = 1024, .tokens = 1_000_000, .pixels = 400_000_000, .source_bytes = 128 * 1024 * 1024, .output_bytes = 256 * 1024 * 1024 }, .inputs = &.{} });
        const current = (try call.root(scratch)).?.run;
        if (current.state != .succeeded) {
            // The enrichment lease is the only scheduling authority. Relinquish
            // the protocol attempt immediately after a call; takeover may also
            // expire an interrupted attempt only under the caller's source fence.
            var next = current;
            next.revision += 1;
            next.lease_until_ms = 0;
            next.state = .queued;
            for (current.pending) |pending| inline for (@typeInfo(runs.Charge).@"struct".field_names) |field| {
                @field(next.uncertain, field) = std.math.add(u64, @field(next.uncertain, field), @field(pending.upper, field)) catch return error.DocumentBudgetExhausted;
            };
            next.pending = &.{};
            if (!try store.cas_fn(store.ptr, current.version(), next)) return error.DocumentRunLeaseLost;
            const lease = try store.claim(scratch, run_id, root_key, next.identity, now(), 1_800_000);
            if (next.inputs.len == 0) {
                const key = hash("enrichment-request-v1");
                if (!try store.reserve(scratch, lease, key, .{ .source_bytes = bytes.len, .output_bytes = bytes.len }, now())) return error.InvalidDocumentCheckpoint;
                const reference = try store.complete(scratch, lease, key, .{ .source_bytes = bytes.len, .output_bytes = bytes.len }, bytes, now());
                try store.attachRequest(scratch, lease, reference, now());
            }
        }
        _ = self.references.fetchAdd(1, .acq_rel);
        errdefer _ = self.references.fetchSub(1, .release);
        return rpc.OwnedServer.create(retained, .{ .store = store, .id = run_id, .owner = root_key, .authorization = .{ .ptr = call, .reference_fn = Call.authorize } }, .{ .ptr = call, .release_fn = Call.release });
    }
};
pub fn now() u64 {
    return @import("antfly_platform").time.realtimeNs() / std.time.ns_per_ms;
}
/// Atomically fence uploads first; bounded deletion leaves the root until the
/// last blob is gone. Native blob writes and this marker share one transaction.
pub fn collect(backend: Backend, a: A, root_key: []const u8, blobs: []const u8, limit: usize) !bool {
    const old = (try backend.get(backend.ptr, a, root_key)) orelse return true;
    var root = try std.json.parseFromSliceLeaky(Root, a, old, .{});
    root.run.retired = true;
    root.run.revision += 1;
    const marker = try std.json.Stringify.valueAlloc(a, root, .{});
    if (!try backend.mutate(backend.ptr, root_key, old, &.{.{ .key = root_key, .value = marker }}, &.{})) return false;
    const keys = try backend.keys(backend.ptr, a, blobs, limit);
    const deletes = try a.alloc([]const u8, keys.len + @as(usize, if (keys.len < limit) 1 else 0));
    for (keys, deletes[0..keys.len]) |key, *dest| dest.* = key;
    if (keys.len < limit) deletes[keys.len] = root_key;
    return try backend.mutate(backend.ptr, root_key, marker, &.{}, deletes) and keys.len < limit;
}

pub const TestBackend = struct {
    a: A,
    values: std.StringHashMap([]u8),
    epoch: u64 = 1,
    expected_epoch: u64 = 1,
    source_current: bool = true,
    pub fn init(a: A) TestBackend {
        return .{ .a = a, .values = .init(a) };
    }
    pub fn deinit(self: *TestBackend) void {
        var it = self.values.iterator();
        while (it.next()) |entry| {
            self.a.free(entry.key_ptr.*);
            self.a.free(entry.value_ptr.*);
        }
        self.values.deinit();
    }
    pub fn backend(self: *TestBackend) Backend {
        return .{ .ptr = self, .get = get, .mutate = mutate, .keys = keys };
    }
    fn get(raw: *anyopaque, a: A, key: []const u8) !?[]u8 {
        const self: *TestBackend = @ptrCast(@alignCast(raw));
        return if (self.values.get(key)) |bytes| try a.dupe(u8, bytes) else null;
    }
    fn mutate(raw: *anyopaque, key: []const u8, expected: ?[]const u8, writes: []const Write, deletes: []const []const u8) !bool {
        const self: *TestBackend = @ptrCast(@alignCast(raw));
        if (self.epoch != self.expected_epoch) return error.EnrichmentLeaseFenceLost;
        if (!self.source_current) return error.EnrichmentSourceChanged;
        const old = self.values.get(key);
        if ((old == null) != (expected == null)) return false;
        if (old) |bytes| if (!std.mem.eql(u8, bytes, expected.?)) return false;
        // Test fixture allocations are not fault-injected; production uses one
        // native storage transaction for this complete conditional mutation.
        for (writes) |write| {
            const bytes = try self.a.dupe(u8, write.value);
            if (self.values.getPtr(write.key)) |slot| {
                self.a.free(slot.*);
                slot.* = bytes;
            } else try self.values.put(try self.a.dupe(u8, write.key), bytes);
        }
        for (deletes) |deleted| if (self.values.fetchRemove(deleted)) |entry| {
            self.a.free(entry.key);
            self.a.free(entry.value);
        };
        return true;
    }
    fn keys(raw: *anyopaque, a: A, prefix: []const u8, limit: usize) ![][]u8 {
        const self: *TestBackend = @ptrCast(@alignCast(raw));
        var found: std.ArrayList([]u8) = .empty;
        var it = self.values.keyIterator();
        while (it.next()) |key| if (std.mem.startsWith(u8, key.*, prefix) and found.items.len < limit) {
            try found.append(a, try a.dupe(u8, key.*));
        };
        return found.toOwnedSlice(a);
    }
};

test "index document checkpoints replay accepted stages and retain unknown charges across owner handoff" {
    const a = std.testing.allocator;
    var backend = TestBackend.init(a);
    defer backend.deinit();
    var io = std.Io.Threaded.init(a, .{});
    defer io.deinit();
    const binding: Binding = .{ .document = "doc", .producer = "read", .source_key = "doc", .source_digest = @splat(1), .producer_digest = @splat(2) };
    var owner = Owner{ .allocator = a, .backend = backend.backend(), .binding = binding, .io = io.io() };
    defer owner.drain();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    {
        const endpoint = try owner.factory().bind(a, "request", false);
        defer endpoint.view().release();
        var client = try rpc.Client.initBound(a, endpoint.view(), io.io());
        defer client.deinit();
        const store = client.store();
        const record = try store.load(scratch, client.id, client.owner);
        const lease: runs.Lease = .{ .run_id = client.id, .owner = client.owner, .attempt = record.attempt, .nonce = record.nonce };
        try store.prepare(scratch, lease, record.identity, record.limits, record.inputs, now());
        try std.testing.expect(try store.reserve(scratch, lease, @splat(1), .{ .calls = 1, .tokens = 10, .output_bytes = 4 }, now()));
        _ = try store.complete(scratch, lease, @splat(1), .{ .calls = 1, .tokens = 3, .output_bytes = 4 }, "page", now());
        try std.testing.expect(try store.reserve(scratch, lease, @splat(2), .{ .calls = 1, .tokens = 7 }, now()));
        backend.epoch = 2;
        try std.testing.expectError(error.DocumentRunLeaseLost, store.complete(scratch, lease, @splat(2), .{}, "", now()));
    }
    backend.expected_epoch = 2;
    {
        const endpoint = try owner.factory().bind(a, "request", false);
        defer endpoint.view().release();
        var client = try rpc.Client.initBound(a, endpoint.view(), io.io());
        defer client.deinit();
        const store = client.store();
        const record = try store.load(scratch, client.id, client.owner);
        try std.testing.expectEqual(@as(u64, 7), record.uncertain.tokens);
        try std.testing.expectEqual(@as(u64, 10), record.used.tokens);
        var index = try runs.Index.init(a, store, record, 512, 32 * 1024 * 1024);
        defer index.deinit();
        const page = index.stages.get(@splat(1)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("page", try store.read(scratch, page, 4));
        const lease: runs.Lease = .{ .run_id = client.id, .owner = client.owner, .attempt = record.attempt, .nonce = record.nonce };
        backend.source_current = false;
        try std.testing.expectError(error.DocumentRunLeaseLost, store.reserve(scratch, lease, @splat(3), .{ .calls = 1 }, now()));
    }
}

test "index document checkpoint cleanup fences uploads and collects bounded blobs without job TTLs" {
    const a = std.testing.allocator;
    var backend = TestBackend.init(a);
    defer backend.deinit();
    var io = std.Io.Threaded.init(a, .{});
    defer io.deinit();
    var owner = Owner{ .allocator = a, .backend = backend.backend(), .binding = .{ .document = "doc", .producer = "read", .source_key = "doc", .source_digest = @splat(1), .producer_digest = @splat(2) }, .io = io.io() };
    defer owner.drain();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const endpoint = try owner.factory().bind(a, "request", false);
    defer endpoint.view().release();
    var client = try rpc.Client.initBound(a, endpoint.view(), io.io());
    defer client.deinit();
    const store = client.store();
    const record = try store.load(scratch, client.id, client.owner);
    try std.testing.expectEqual(@as(u64, 0), record.retention_until_ms);
    const upload: runs.Scope = .{ .run_id = client.id, .owner = client.owner, .attempt = record.attempt, .nonce = record.nonce };
    _ = try store.put_fn(store.ptr, scratch, upload, "orphan");
    const id = try scope(scratch, "doc", "read");
    const root_key = try rootKey(scratch, id);
    const prefix = try blobScope(scratch, id);
    try std.testing.expect(!try collect(backend.backend(), scratch, root_key, prefix, 1));
    try std.testing.expectError(error.DocumentRunLeaseLost, store.put_fn(store.ptr, scratch, upload, "late"));
    while (!try collect(backend.backend(), scratch, root_key, prefix, 1)) {}
    try std.testing.expectEqual(@as(usize, 0), backend.values.count());
}

fn protocolError(err: anyerror) anyerror {
    return switch (err) {
        error.EnrichmentLeaseFenceLost, error.EnrichmentSourceChanged, error.ArtifactCatalogDrift => error.DocumentRunLeaseLost,
        else => err,
    };
}

// The retained RPC binding must never borrow a producer's stack-scoped budget
// allocator. This bounded allocator lives with the last response and serializes
// accounting while independent native/remote checkpoint calls are in flight.
const RetainedMemory = struct {
    budget: @import("antfly_inference_work").BoundedInvocationAllocator,
    mutex: std.atomic.Mutex = .unlocked,
    fn allocator(self: *@This()) A {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn lock(self: *@This()) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, address: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        return self.budget.allocator().rawAlloc(len, alignment, address);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, address: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        return self.budget.allocator().rawResize(bytes, alignment, len, address);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, address: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        return self.budget.allocator().rawRemap(bytes, alignment, len, address);
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, address: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        self.budget.allocator().rawFree(bytes, alignment, address);
    }
};

test "index document checkpoint binding outlives the producer invocation allocator" {
    const a = std.testing.allocator;
    var backend = TestBackend.init(a);
    defer backend.deinit();
    var io = std.Io.Threaded.init(a, .{});
    defer io.deinit();
    var owner = Owner{ .allocator = a, .backend = backend.backend(), .binding = .{ .document = "doc", .producer = "read", .source_key = "doc", .source_digest = @splat(1), .producer_digest = @splat(2) }, .io = io.io() };
    defer owner.drain();
    const endpoint = binding: {
        var invocation = std.heap.ArenaAllocator.init(a);
        defer invocation.deinit();
        break :binding try owner.factory().bind(invocation.allocator(), "request", false);
    };
    defer endpoint.view().release();
    var client = try rpc.Client.initBound(a, endpoint.view(), io.io());
    defer client.deinit();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const record = try client.store().load(arena.allocator(), client.id, client.owner);
    try std.testing.expectEqual(hash("request"), record.identity.request);
}
