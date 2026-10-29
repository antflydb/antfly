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

//! Durable closure evidence for providers that prevent deletion AND replacement
//! of current names until an authenticated expiry. S3 version locks alone do
//! not meet this contract. Unsupported/mutable backends retain complete audits.
const std = @import("std");
const api = @import("store.zig");
const Object = @import("object_store.zig").ObjectStore;
const Token = @import("antfly_cancellation").CancellationToken;
const objectstore = @import("objectstore");
const A = std.mem.Allocator;
const Provider = @import("../provider_operation.zig");
const Context = api.ReadContext;
const Certificate = struct { version: u8 = 1, root: []const u8, witnesses: []const api.RetentionWitness };
const Pending = struct { arena: std.heap.ArenaAllocator, root: []const u8, witnesses: []const api.RetentionWitness, fresh: api.UploadScope };
pending: std.ArrayList(Pending) = .empty,
mutex: std.Io.Mutex = .init,
const Self = @This();
pub fn deinit(self: *Self, a: A) void {
    for (self.pending.items) |*entry| entry.arena.deinit();
    self.pending.deinit(a);
}
fn supported(store: *Object) bool {
    return store.client.vtable.immutable_prefix_until != null and store.client.vtable.validate_immutable_until != null;
}
fn remoteToken(token: Token) ?objectstore.CancellationToken {
    return objectstore.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn);
}
fn stamp(store: *Object, scope: api.UploadScope, until: u64) [32]u8 {
    var result: [32]u8 = undefined;
    std.mem.writeInt(u64, result[0..8], until, .big);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-artifact-provider-retention-v1");
    // Length delimit backend identity components; certificates cannot move
    // across buckets, prefixes, domains or upload-attempt incarnations.
    var size: [8]u8 = undefined;
    for ([_][]const u8{ store.bucket, store.prefix }) |component| {
        std.mem.writeInt(u64, &size, component.len, .big);
        hash.update(&size);
        hash.update(component);
    }
    hash.update(&scope.domain);
    hash.update(&scope.attempt);
    hash.update(result[0..8]);
    var digest: [32]u8 = undefined;
    hash.final(&digest);
    @memcpy(result[8..], digest[0..24]);
    return result;
}
pub fn witness(store: *Object, scope: api.UploadScope, token: Token) !?api.RetentionWitness {
    return witnessFor(store, scope, token, .{});
}
pub fn retained(store: *Object, id: []const u8, token: Token) !bool {
    return retainedFor(store, id, token, .{});
}
pub fn remember(store: *Object, id: []const u8, witnesses: []const api.RetentionWitness, token: Token) !void {
    return rememberFor(store, id, witnesses, token, .{});
}
pub fn inherit(self: *Self, store: *Object, id: []const u8, previous: ?[]const u8, fresh: api.UploadScope, token: Token) !void {
    return inheritFor(self, store, id, previous, fresh, token, .{});
}
pub fn seal(self: *Self, store: *Object, token: Token) !void {
    return sealFor(self, store, token, .{});
}
pub fn witnessFor(store: *Object, scope: api.UploadScope, token: Token, context: Context) !?api.RetentionWitness {
    const op: Provider.Operation = .{ .context = context, .cancellation = token };
    try op.ready();
    try token.check();
    try scope.validate();
    if (!supported(store)) return null;
    const path = try std.fmt.allocPrint(store.alloc, "{s}{s}graph/{s}/{s}/", .{ store.prefix, if (store.prefix.len == 0) "" else "/", std.fmt.bytesToHex(&scope.domain, .lower), std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer store.alloc.free(path);
    const until = store.client.immutablePrefixUntil(store.bucket, path, op.token()) catch |err| {
        try op.check();
        return err;
    };
    try op.ready();
    const expiry = until orelse return null;
    return .{ .scope = scope, .stamp = stamp(store, scope, expiry) };
}
fn validFor(store: *Object, id: []const u8, witnesses: []const api.RetentionWitness, token: Token, context: Context) !bool {
    const op: Provider.Operation = .{ .context = context, .cancellation = token };
    try op.ready();
    if (!supported(store) or witnesses.len == 0 or witnesses.len > 4096) return false;
    const root_scope = (try api.uploadScopeFromArtifactId(id)) orelse return false;
    var includes_root = false;
    var until: u64 = std.math.maxInt(u64);
    for (witnesses) |entry| {
        try token.check();
        try entry.scope.validate();
        if (!std.mem.eql(u8, &entry.scope.domain, &root_scope.domain)) return false;
        if (std.meta.eql(entry.scope, root_scope)) includes_root = true;
        const expiry = std.mem.readInt(u64, entry.stamp[0..8], .big);
        if (!std.mem.eql(u8, &entry.stamp, &stamp(store, entry.scope, expiry))) return false;
        until = @min(until, expiry);
    }
    if (!includes_root) return false;
    const result = store.client.validateImmutableUntil(store.bucket, until, op.token()) catch |err| {
        try op.check();
        return err;
    };
    try op.ready();
    return result;
}
fn pathAlloc(store: *Object, id: []const u8) ![]u8 {
    _ = try api.sha256ChecksumFromArtifactId(id);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(id, &digest, .{});
    return std.fmt.allocPrint(store.alloc, "{s}{s}.retained-closures-v1/{s}", .{ store.prefix, if (store.prefix.len == 0) "" else "/", std.fmt.bytesToHex(&digest, .lower) });
}
fn loadFor(store: *Object, a: A, id: []const u8, token: Token, context: Context) !Certificate {
    const op: Provider.Operation = .{ .context = context, .cancellation = token };
    try op.ready();
    const path = try pathAlloc(store, id);
    defer store.alloc.free(path);
    var object = try op.get(&store.client, store.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = 1024 * 1024, .cancellation = op.token() });
    defer object.deinit(store.client.allocator);
    const bytes = object.body;
    if (bytes.len < 41 or !std.mem.eql(u8, bytes[0..9], "AFRCLOUD1")) return error.InvalidRetentionCertificate;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[41..], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[9..41])) return error.InvalidRetentionCertificate;
    const certificate = try std.json.parseFromSliceLeaky(Certificate, a, bytes[41..], .{ .allocate = .alloc_always });
    if (certificate.version != 1 or !std.mem.eql(u8, certificate.root, id) or certificate.witnesses.len > 4096) return error.InvalidRetentionCertificate;
    return certificate;
}
pub fn retainedFor(store: *Object, id: []const u8, token: Token, context: Context) !bool {
    const op: Provider.Operation = .{ .context = context, .cancellation = token };
    try op.ready();
    try token.check();
    if (!supported(store)) return false;
    var arena = std.heap.ArenaAllocator.init(store.alloc);
    defer arena.deinit();
    const certificate = loadFor(store, arena.allocator(), id, token, context) catch {
        try op.check();
        return false;
    };
    return validFor(store, id, certificate.witnesses, token, context);
}
pub fn rememberFor(store: *Object, id: []const u8, witnesses: []const api.RetentionWitness, token: Token, context: Context) !void {
    const op: Provider.Operation = .{ .context = context, .cancellation = token };
    try op.ready();
    if (store.read_only or !try validFor(store, id, witnesses, token, context)) return;
    const payload = try std.json.Stringify.valueAlloc(store.alloc, Certificate{ .root = id, .witnesses = witnesses }, .{});
    defer store.alloc.free(payload);
    if (payload.len > 1024 * 1024 - 41) return;
    const bytes = try store.alloc.alloc(u8, 41 + payload.len);
    defer store.alloc.free(bytes);
    @memcpy(bytes[0..9], "AFRCLOUD1");
    std.crypto.hash.sha2.Sha256.hash(payload, bytes[9..41], .{});
    @memcpy(bytes[41..], payload);
    const path = try pathAlloc(store, id);
    defer store.alloc.free(path);
    var result = op.put(&store.client, store.bucket, path, bytes, .{ .if_none_match = true, .cancellation = op.token() }) catch |err| {
        if (err == error.PreconditionFailed) return;
        return err;
    };
    result.deinit(store.client.allocator);
}
fn extendPending(self: *Self, id: []const u8, previous: ?[]const u8, fresh: api.UploadScope) !bool {
    if (previous) |previous_id| for (self.pending.items) |*entry| {
        if (std.mem.eql(u8, entry.root, previous_id) and std.meta.eql(entry.fresh, fresh)) {
            if (entry.root.len == id.len) @memcpy(@constCast(entry.root), id) else entry.root = try entry.arena.allocator().dupe(u8, id);
            return true;
        }
    };
    return self.pending.items.len == 64;
}
pub fn inheritFor(self: *Self, store: *Object, id: []const u8, previous: ?[]const u8, fresh: api.UploadScope, token: Token, context: Context) !void {
    var native: Provider.Native = undefined;
    try native.init(context, token);
    defer native.deinit();
    const op = &native.operation;
    const io = try op.receiver();
    if (store.read_only or !supported(store)) return;
    if (!std.meta.eql((try api.uploadScopeFromArtifactId(id)) orelse return error.InvalidRetentionCertificate, fresh)) return error.InvalidRetentionCertificate;
    {
        try op.lockIo(&self.mutex);
        defer self.mutex.unlock(io);
        if (try self.extendPending(id, previous, fresh)) return;
    }
    var arena = std.heap.ArenaAllocator.init(store.alloc);
    var keep = false;
    defer if (!keep) arena.deinit();
    const a = arena.allocator();
    var witnesses: []const api.RetentionWitness = &.{};
    if (previous) |previous_id| {
        const certificate = loadFor(store, a, previous_id, token, op.context) catch {
            try op.check();
            return;
        };
        if (!try validFor(store, previous_id, certificate.witnesses, token, op.context)) return;
        witnesses = certificate.witnesses;
        for (witnesses) |entry| if (!std.mem.eql(u8, &entry.scope.domain, &fresh.domain) or std.meta.eql(entry.scope, fresh)) return;
    }
    if (witnesses.len >= 4096) return;
    const root = try a.dupe(u8, id);
    try op.lockIo(&self.mutex);
    defer self.mutex.unlock(io);
    if (try self.extendPending(id, previous, fresh)) return;
    try self.pending.append(store.alloc, .{ .arena = arena, .root = root, .witnesses = witnesses, .fresh = fresh });
    keep = true;
}
pub fn sealFor(self: *Self, store: *Object, token: Token, context: Context) !void {
    var native: Provider.Native = undefined;
    try native.init(context, token);
    defer native.deinit();
    const op = &native.operation;
    const io = try op.receiver();
    try op.lockIo(&self.mutex);
    var pending_batch = self.pending;
    self.pending = .empty;
    self.mutex.unlock(io);
    defer {
        for (pending_batch.items) |*entry| entry.arena.deinit();
        pending_batch.deinit(store.alloc);
    }
    for (pending_batch.items) |*entry| {
        try op.ready();
        const fresh = (try witnessFor(store, entry.fresh, token, op.context)) orelse continue;
        const witnesses = try entry.arena.allocator().alloc(api.RetentionWitness, entry.witnesses.len + 1);
        @memcpy(witnesses[0..entry.witnesses.len], entry.witnesses);
        witnesses[entry.witnesses.len] = fresh;
        try rememberFor(store, entry.root, witnesses, token, op.context);
    }
    try op.ready();
}
