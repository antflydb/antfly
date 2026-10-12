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

//! Durable closure evidence for providers that prevent deletion AND replacement
//! of current names until an authenticated expiry. S3 version locks alone do
//! not meet this contract. Unsupported/mutable backends retain complete audits.
const std = @import("std");
const api = @import("store.zig");
const Object = @import("object_store.zig").ObjectStore;
const Token = @import("antfly_cancellation").CancellationToken;
const objectstore = @import("objectstore");
const A = std.mem.Allocator;
const Certificate = struct { version: u8 = 1, root: []const u8, witnesses: []const api.RetentionWitness };
const Pending = struct { arena: std.heap.ArenaAllocator, root: []const u8, witnesses: []const api.RetentionWitness, fresh: api.UploadScope };
pending: std.ArrayList(Pending) = .empty,
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
    try token.check();
    try scope.validate();
    if (!supported(store)) return null;
    const path = try std.fmt.allocPrint(store.alloc, "{s}{s}graph/{s}/{s}/", .{ store.prefix, if (store.prefix.len == 0) "" else "/", std.fmt.bytesToHex(&scope.domain, .lower), std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer store.alloc.free(path);
    const until = (try store.client.immutablePrefixUntil(store.bucket, path, remoteToken(token))) orelse return null;
    return .{ .scope = scope, .stamp = stamp(store, scope, until) };
}
fn valid(store: *Object, id: []const u8, witnesses: []const api.RetentionWitness, token: Token) !bool {
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
    return includes_root and try store.client.validateImmutableUntil(store.bucket, until, remoteToken(token));
}
fn pathAlloc(store: *Object, id: []const u8) ![]u8 {
    _ = try api.sha256ChecksumFromArtifactId(id);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(id, &digest, .{});
    return std.fmt.allocPrint(store.alloc, "{s}{s}.retained-closures-v1/{s}", .{ store.prefix, if (store.prefix.len == 0) "" else "/", std.fmt.bytesToHex(&digest, .lower) });
}
fn load(store: *Object, a: A, id: []const u8, token: Token) !Certificate {
    const path = try pathAlloc(store, id);
    defer store.alloc.free(path);
    var object = try store.client.getObject(store.bucket, path, .{ .skip_metadata_probe = true, .max_response_bytes = 1024 * 1024, .cancellation = remoteToken(token) });
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
pub fn retained(store: *Object, id: []const u8, token: Token) !bool {
    try token.check();
    if (!supported(store)) return false;
    var arena = std.heap.ArenaAllocator.init(store.alloc);
    defer arena.deinit();
    const certificate = load(store, arena.allocator(), id, token) catch {
        try token.check();
        return false;
    };
    return valid(store, id, certificate.witnesses, token);
}
pub fn remember(store: *Object, id: []const u8, witnesses: []const api.RetentionWitness, token: Token) !void {
    if (store.read_only or !try valid(store, id, witnesses, token)) return;
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
    var result = store.client.putObject(store.bucket, path, bytes, .{ .if_none_match = true, .cancellation = remoteToken(token) }) catch |err| {
        if (err == error.PreconditionFailed) return;
        return err;
    };
    result.deinit(store.client.allocator);
}
pub fn inherit(self: *Self, store: *Object, id: []const u8, previous: ?[]const u8, fresh: api.UploadScope, token: Token) !void {
    try token.check();
    if (store.read_only or !supported(store)) return;
    if (!std.meta.eql((try api.uploadScopeFromArtifactId(id)) orelse return error.InvalidRetentionCertificate, fresh)) return error.InvalidRetentionCertificate;
    if (previous) |old| for (self.pending.items) |*entry| {
        if (std.mem.eql(u8, entry.root, old) and std.meta.eql(entry.fresh, fresh)) {
            if (entry.root.len == id.len) @memcpy(@constCast(entry.root), id) else entry.root = try entry.arena.allocator().dupe(u8, id);
            return;
        }
    };
    if (self.pending.items.len == 64) return;
    var arena = std.heap.ArenaAllocator.init(store.alloc);
    var keep = false;
    defer if (!keep) arena.deinit();
    const a = arena.allocator();
    var witnesses: []const api.RetentionWitness = &.{};
    if (previous) |old| {
        const certificate = load(store, a, old, token) catch {
            try token.check();
            return;
        };
        if (!try valid(store, old, certificate.witnesses, token)) return;
        witnesses = certificate.witnesses;
        for (witnesses) |entry| if (!std.mem.eql(u8, &entry.scope.domain, &fresh.domain) or std.meta.eql(entry.scope, fresh)) return;
    }
    if (witnesses.len >= 4096) return;
    const root = try a.dupe(u8, id);
    try self.pending.append(store.alloc, .{ .arena = arena, .root = root, .witnesses = witnesses, .fresh = fresh });
    keep = true;
}
pub fn seal(self: *Self, store: *Object, token: Token) !void {
    defer {
        for (self.pending.items) |*entry| entry.arena.deinit();
        self.pending.clearRetainingCapacity();
    }
    for (self.pending.items) |*entry| {
        const fresh = (try witness(store, entry.fresh, token)) orelse continue;
        const witnesses = try entry.arena.allocator().alloc(api.RetentionWitness, entry.witnesses.len + 1);
        @memcpy(witnesses[0..entry.witnesses.len], entry.witnesses);
        witnesses[entry.witnesses.len] = fresh;
        try remember(store, entry.root, witnesses, token);
    }
}
