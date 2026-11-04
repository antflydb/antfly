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

//! Shared query/task ownership. The claim and accepted output occupy the same
//! CAS record: a delayed worker cannot publish through a superseded claim.
const std = @import("std");
const storage = @import("objectstore");
const catalog = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog;
const A = std.mem.Allocator;
pub const Digest = [64]u8;
pub fn validateDigest(value: Digest) !void {
    for (value) |byte| if (!std.ascii.isHex(byte)) return error.InvalidSqlExecutionState;
}
pub const Output = struct { key: []const u8, hash: Digest, bytes: u64, rows: u64 = 0, partitions: u16 = 0, index_hash: ?Digest = null };
pub const Status = enum { pending, running, succeeded, failed, cancelled };
pub const Record = struct {
    version: u8 = 1,
    plan: Digest,
    expires_ms: u64,
    status: Status = .pending,
    owner: []const u8 = "",
    epoch: u64 = 0,
    until_ms: u64 = 0,
    stage: u32 = 0,
    cursor: u32 = 0,
    output: ?Output = null,
    failure: ?[]const u8 = null,
    pub fn terminal(self: Record) bool {
        return self.status == .succeeded or self.status == .failed or self.status == .cancelled;
    }
};
pub const Claim = struct { owner: []const u8, epoch: u64 };
pub const Loaded = struct {
    object: storage.GetResult,
    value: Record,
    parsed: std.json.Parsed(Record),
    pub fn deinit(self: *Loaded, a: A) void {
        self.parsed.deinit();
        self.object.deinit(a);
    }
};
pub const Store = struct {
    client: storage.Client,
    bucket: []const u8,
    prefix: []const u8,
    context: catalog.types.Context = .{},
    now_ms: u64,
    pub fn key(self: Store, a: A, query: Digest, stage: ?u32, task: u32) ![]const u8 {
        try validateDigest(query);
        if (stage) |s| return std.fmt.allocPrint(a, "{s}/sql-execution/{s}/stage/{d}/task/{d}.json", .{ self.prefix, query, s, task });
        return std.fmt.allocPrint(a, "{s}/sql-execution/{s}/query.json", .{ self.prefix, query });
    }
    pub fn load(self: Store, a: A, path: []const u8) !?Loaded {
        try self.context.ensureActive();
        var client = self.client;
        client.allocator = a;
        var object = client.getObject(self.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = 16384, .cancellation = catalog.types.contextCancellation(&self.context) }) catch |err| switch (err) {
            error.NotFound, error.ObjectNotFound, error.FileNotFound => return null,
            else => return err,
        };
        errdefer object.deinit(a);
        const parsed = try std.json.parseFromSlice(Record, a, object.body, .{ .allocate = .alloc_always });
        errdefer parsed.deinit();
        const value = parsed.value;
        if (value.version != 1 or value.owner.len > 256 or (value.status == .succeeded and value.output == null)) return error.InvalidSqlExecutionState;
        return .{ .object = object, .value = value, .parsed = parsed };
    }
    fn write(self: Store, a: A, path: []const u8, before: ?Loaded, value: Record) !void {
        try self.context.ensureActive();
        const bytes = try std.json.Stringify.valueAlloc(a, value, .{});
        defer a.free(bytes);
        if (bytes.len > 16384) return error.InvalidSqlExecutionState;
        var client = self.client;
        client.allocator = a;
        var result = try client.compareAndPut(self.bucket, path, bytes, if (before) |old| old.object.metadata.writeRevision() orelse return error.MissingObjectRevision else null, .{ .cancellation = catalog.types.contextCancellation(&self.context) });
        result.deinit(a);
    }
    pub fn create(self: Store, a: A, path: []const u8, plan: Digest, expires_ms: u64) !void {
        if (expires_ms <= self.now_ms) return error.SqlQueryExpired;
        self.write(a, path, null, .{ .plan = plan, .expires_ms = expires_ms }) catch |err| {
            // Existing jobs and lost successful responses are both resolved by
            // identity. Never overwrite state advanced by another participant.
            var existing = (self.load(a, path) catch return err) orelse return err;
            defer existing.deinit(a);
            if (!std.mem.eql(u8, &existing.value.plan, &plan) or existing.value.expires_ms != expires_ms) return error.SqlExecutionIdReused;
        };
    }
    pub fn claim(self: Store, a: A, path: []const u8, owner: []const u8, ttl_ms: u64) !?Claim {
        if (owner.len == 0 or owner.len > 256 or ttl_ms == 0 or ttl_ms > 60_000) return error.InvalidSqlExecutionClaim;
        var old = (try self.load(a, path)) orelse return error.SqlExecutionNotFound;
        defer old.deinit(a);
        var next = old.value;
        if (next.expires_ms <= self.now_ms) return error.SqlQueryExpired;
        if (next.terminal() or (next.status == .running and next.until_ms +| 30_000 >= self.now_ms)) return null;
        next.status = .running;
        next.owner = owner;
        next.epoch = try std.math.add(u64, next.epoch, 1);
        next.until_ms = @min(next.expires_ms, self.now_ms +| ttl_ms);
        self.write(a, path, old, next) catch |err| switch (err) {
            error.PreconditionFailed => return null,
            else => return err,
        };
        return .{ .owner = owner, .epoch = next.epoch };
    }
    fn owned(self: Store, a: A, path: []const u8, claim_: Claim) !Loaded {
        var old = (try self.load(a, path)) orelse return error.SqlExecutionNotFound;
        errdefer old.deinit(a);
        if (old.value.expires_ms <= self.now_ms) return error.SqlQueryExpired;
        if (old.value.status != .running or old.value.epoch != claim_.epoch or !std.mem.eql(u8, old.value.owner, claim_.owner) or old.value.until_ms <= self.now_ms) return error.SqlExecutionFenced;
        return old;
    }
    pub fn renew(self: Store, a: A, path: []const u8, claim_: Claim, ttl_ms: u64) !void {
        if (ttl_ms == 0 or ttl_ms > 60_000) return error.InvalidSqlExecutionClaim;
        var old = try self.owned(a, path, claim_);
        defer old.deinit(a);
        var next = old.value;
        next.until_ms = @min(next.expires_ms, self.now_ms +| ttl_ms);
        try self.write(a, path, old, next);
    }
    pub fn checkpoint(self: Store, a: A, path: []const u8, claim_: Claim, stage: u32, cursor: u32) !void {
        var old = try self.owned(a, path, claim_);
        defer old.deinit(a);
        if (stage < old.value.stage or (stage == old.value.stage and cursor < old.value.cursor)) return error.InvalidSqlExecutionState;
        var next = old.value;
        next.stage = stage;
        next.cursor = cursor;
        try self.write(a, path, old, next);
    }
    pub fn complete(self: Store, a: A, path: []const u8, claim_: Claim, output: Output) !void {
        var old = (try self.load(a, path)) orelse return error.SqlExecutionNotFound;
        defer old.deinit(a);
        // Exact accepted retries survive an ACK loss, including after expiry.
        if (old.value.status == .succeeded) {
            if (old.value.epoch != claim_.epoch or !std.mem.eql(u8, old.value.owner, claim_.owner) or !std.meta.eql(old.value.output.?.hash, output.hash) or !std.mem.eql(u8, old.value.output.?.key, output.key) or old.value.output.?.bytes != output.bytes or old.value.output.?.rows != output.rows or old.value.output.?.partitions != output.partitions or !std.meta.eql(old.value.output.?.index_hash, output.index_hash)) return error.SqlExecutionFenced;
            return;
        }
        if (old.value.expires_ms <= self.now_ms) return error.SqlQueryExpired;
        if (old.value.status != .running or old.value.epoch != claim_.epoch or !std.mem.eql(u8, old.value.owner, claim_.owner) or old.value.until_ms <= self.now_ms) return error.SqlExecutionFenced;
        var next = old.value;
        next.status = .succeeded;
        next.output = output;
        try self.write(a, path, old, next);
    }
    pub fn terminate(self: Store, a: A, path: []const u8, claim_: Claim, status: Status, failure: []const u8) !void {
        if ((status != .failed and status != .cancelled) or failure.len > 256) return error.InvalidSqlExecutionState;
        var old = try self.owned(a, path, claim_);
        defer old.deinit(a);
        var next = old.value;
        next.status = status;
        next.failure = failure;
        try self.write(a, path, old, next);
    }
};

test "distributed SQL state fences delayed owners and resolves accepted completion ACK loss" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    var faults = storage.ScriptedFaultClient.init(a, memory.client());
    defer faults.deinit();
    var store: Store = .{ .client = faults.client(), .bucket = "queries", .prefix = "tenant", .now_ms = 1000 };
    const key_ = try store.key(scratch, @splat('a'), 0, 0);
    try store.create(scratch, key_, @splat('b'), 1000000);
    const first = (try store.claim(scratch, key_, "pod-a", 1000)).?;
    try std.testing.expect(try store.claim(scratch, key_, "pod-b", 1000) == null);
    store.now_ms = 32001;
    const second = (try store.claim(scratch, key_, "pod-b", 1000)).?;
    const output: Output = .{ .key = "result", .hash = @splat('c'), .bytes = 10 };
    try std.testing.expectError(error.SqlExecutionFenced, store.complete(scratch, key_, first, output));
    faults.next_put = .{ .commit_then_fail = error.ConnectionResetByPeer };
    try std.testing.expectError(error.ConnectionResetByPeer, store.complete(scratch, key_, second, output));
    try store.complete(scratch, key_, second, output);
    try std.testing.expect(try store.claim(scratch, key_, "pod-c", 1000) == null);
    try std.testing.expectError(error.SqlExecutionIdReused, store.create(scratch, key_, @splat('z'), 1000000));
}

test "distributed SQL state keeps cancellation terminal and rejects query path injection" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const sa = arena.allocator();
    var memory = storage.MemoryClient.init(a);
    defer memory.deinit();
    const store: Store = .{ .client = memory.client(), .bucket = "queries", .prefix = "tenant", .now_ms = 1000 };
    try std.testing.expectError(error.InvalidSqlExecutionState, store.key(sa, @splat('/'), 0, 0));
    const key_ = try store.key(sa, @splat('a'), null, 0);
    try store.create(sa, key_, @splat('b'), 100000);
    const owner = (try store.claim(sa, key_, "coordinator", 1000)).?;
    try store.terminate(sa, key_, owner, .cancelled, "Cancelled");
    try std.testing.expectError(error.SqlExecutionFenced, store.checkpoint(sa, key_, owner, 1, 0));
    try std.testing.expectError(error.SqlExecutionFenced, store.complete(sa, key_, owner, .{ .key = "ignored", .hash = @splat('c'), .bytes = 1 }));
    try std.testing.expect(try store.claim(sa, key_, "replacement", 1000) == null);
}
