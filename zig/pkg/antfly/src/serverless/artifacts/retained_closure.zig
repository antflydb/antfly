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

//! Durable retention certificates for immutable filesystem upload namespaces.
//! Namespace inode/ctime/mtime are the incarnation: managed deletion, atomic
//! replacement and directory restoration invalidate all affected certificates.
//! Payload hashes remain verified by normal reads. This is never a proof for
//! a mutable backend or a substitute for a publication/reader lease.
const std = @import("std");
const api = @import("store.zig");
const Fs = @import("fs_store.zig").FsStore;
const Token = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
const Certificate = struct { version: u8 = 1, root: []const u8, witnesses: []const api.RetentionWitness };
const Pending = struct { arena: std.heap.ArenaAllocator, root: []const u8, witnesses: []const api.RetentionWitness, fresh: api.UploadScope, epoch: ?[16]u8 };
pending: std.ArrayList(Pending) = .empty,
const Self = @This();

pub fn deinit(self: *Self, a: A) void {
    for (self.pending.items) |*pending| pending.arena.deinit();
    self.pending.deinit(a);
}
pub fn witness(fs: *Fs, scope: api.UploadScope, token: Token) !?api.RetentionWitness {
    try token.check();
    try scope.validate();
    const path = try std.fs.path.join(fs.alloc, &.{ fs.root_dir, "graph", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
    defer fs.alloc.free(path);
    var threaded = std.Io.Threaded.init(fs.alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const dir = std.Io.Dir.cwd().openDir(io, path, .{ .follow_symlinks = false }) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return null,
        else => return err,
    };
    defer dir.close(io);
    const stat = try dir.stat(io);
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("native-artifact-filesystem-retention-v1");
    hash.update(fs.root_dir);
    hash.update(&scope.domain);
    hash.update(&scope.attempt);
    hash.update(std.mem.asBytes(&stat.inode));
    const times = [2]i128{ stat.mtime.toNanoseconds(), stat.ctime.toNanoseconds() };
    hash.update(std.mem.asBytes(&times));
    // Managed destructive operations advance a random durable epoch before
    // mutation, independent of filesystem timestamp granularity.
    const epoch_path = try epochPath(fs, fs.alloc, scope);
    defer fs.alloc.free(epoch_path);
    const epoch: ?[]u8 = std.Io.Dir.cwd().readFileAlloc(io, epoch_path, fs.alloc, .limited(17)) catch |err| switch (err) {
        error.FileNotFound => null,
        else => return err,
    };
    if (epoch) |bytes| {
        defer fs.alloc.free(bytes);
        if (bytes.len != 16) return error.InvalidRetentionCertificate;
        hash.update(bytes);
    }

    var stamp: [32]u8 = undefined;
    hash.final(&stamp);
    try token.check();
    return .{ .scope = scope, .stamp = stamp };
}
fn epochPath(fs: *Fs, a: A, scope: api.UploadScope) ![]u8 {
    return std.fs.path.join(a, &.{ fs.inventory_root, ".retention-epochs-v1", &std.fmt.bytesToHex(&scope.domain, .lower), &std.fmt.bytesToHex(&scope.attempt, .lower) });
}
fn readEpoch(fs: *Fs, scope: api.UploadScope, token: Token) !?[16]u8 {
    try token.check();
    const path = try epochPath(fs, fs.alloc, scope);
    defer fs.alloc.free(path);
    var threaded = std.Io.Threaded.init(fs.alloc, .{});
    defer threaded.deinit();
    const bytes = std.Io.Dir.cwd().readFileAlloc(threaded.io(), path, fs.alloc, .limited(17)) catch |err| switch (err) {
        error.FileNotFound => return null,
        else => return err,
    };
    defer fs.alloc.free(bytes);
    if (bytes.len != 16) return error.InvalidRetentionCertificate;
    return bytes[0..16].*;
}
/// Restoration tooling must call this before changing immutable payloads in
/// place. Ordinary uploads replace directory entries; their inode stamps also
/// invalidate proofs. GC/delete call this before each destructive mutation.
pub fn invalidate(fs: *Fs, scope: api.UploadScope, token: Token) !void {
    try scope.validate();
    try token.check();
    const path = try epochPath(fs, fs.alloc, scope);
    defer fs.alloc.free(path);
    var threaded = std.Io.Threaded.init(fs.alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();
    try fs.durable_dirs.ensure(fs.alloc, io, std.fs.path.dirname(path).?);
    var epoch: [16]u8 = undefined;
    io.random(&epoch);
    try @import("fs_store.zig").writeFileAtomicallyWithCancellation(path, &epoch, token);
}

fn pathAlloc(fs: *Fs, a: A, id: []const u8) ![]u8 {
    _ = try api.sha256ChecksumFromArtifactId(id);
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(id, &digest, .{});
    return std.fs.path.join(a, &.{ fs.inventory_root, ".retained-closures-v1", &std.fmt.bytesToHex(&digest, .lower) });
}
fn load(fs: *Fs, a: A, id: []const u8, token: Token) !Certificate {
    try token.check();
    const path = try pathAlloc(fs, a, id);
    var threaded = std.Io.Threaded.init(a, .{});
    defer threaded.deinit();
    const bytes = try std.Io.Dir.cwd().readFileAlloc(threaded.io(), path, a, .limited(1024 * 1024));
    if (bytes.len < 41 or !std.mem.eql(u8, bytes[0..9], "AFRETAIN1")) return error.InvalidRetentionCertificate;
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes[41..], &digest, .{});
    if (!std.mem.eql(u8, &digest, bytes[9..41])) return error.InvalidRetentionCertificate;
    const cert = try std.json.parseFromSliceLeaky(Certificate, a, bytes[41..], .{});
    if (cert.version != 1 or !std.mem.eql(u8, cert.root, id) or cert.witnesses.len == 0 or cert.witnesses.len > 4096) return error.InvalidRetentionCertificate;
    const root_scope = (try api.uploadScopeFromArtifactId(id)) orelse return error.InvalidRetentionCertificate;
    var includes_root = false;
    for (cert.witnesses) |entry| {
        if (std.meta.eql(entry.scope, root_scope)) includes_root = true;
        try entry.scope.validate();
        if (!std.mem.eql(u8, &root_scope.domain, &entry.scope.domain)) return error.InvalidRetentionCertificate;
    }
    if (!includes_root) return error.InvalidRetentionCertificate;
    return cert;
}
fn valid(fs: *Fs, witnesses: []const api.RetentionWitness, token: Token) !bool {
    for (witnesses) |entry| {
        const current = (try witness(fs, entry.scope, token)) orelse return false;
        if (!std.mem.eql(u8, &current.stamp, &entry.stamp)) return false;
    }
    return true;
}
pub fn forgetNamespace(fs: *Fs, scope: api.UploadScope) !void {
    const path = try epochPath(fs, fs.alloc, scope);
    defer fs.alloc.free(path);
    var threaded = std.Io.Threaded.init(fs.alloc, .{});
    defer threaded.deinit();
    std.Io.Dir.cwd().deleteFile(threaded.io(), path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}
pub fn forget(fs: *Fs, id: []const u8) !void {
    const path = try pathAlloc(fs, fs.alloc, id);
    defer fs.alloc.free(path);
    var threaded = std.Io.Threaded.init(fs.alloc, .{});
    defer threaded.deinit();
    std.Io.Dir.cwd().deleteFile(threaded.io(), path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
}
pub fn retained(fs: *Fs, id: []const u8, token: Token) !bool {
    var arena = std.heap.ArenaAllocator.init(fs.alloc);
    defer arena.deinit();
    const cert = load(fs, arena.allocator(), id, token) catch {
        try token.check();
        return false;
    };
    return valid(fs, cert.witnesses, token);
}
pub fn remember(fs: *Fs, id: []const u8, witnesses: []const api.RetentionWitness, token: Token) !void {
    if (witnesses.len == 0 or witnesses.len > 4096 or !try valid(fs, witnesses, token)) return;
    const path = try pathAlloc(fs, fs.alloc, id);
    defer fs.alloc.free(path);
    var threaded = std.Io.Threaded.init(fs.alloc, .{});
    defer threaded.deinit();
    try fs.durable_dirs.ensure(fs.alloc, threaded.io(), std.fs.path.dirname(path).?);
    const bytes = try std.json.Stringify.valueAlloc(fs.alloc, Certificate{ .root = id, .witnesses = witnesses }, .{});
    defer fs.alloc.free(bytes);
    if (bytes.len > 1024 * 1024 - 41) return;
    const framed = try fs.alloc.alloc(u8, 41 + bytes.len);
    defer fs.alloc.free(framed);
    @memcpy(framed[0..9], "AFRETAIN1");
    std.crypto.hash.sha2.Sha256.hash(bytes, framed[9..41], .{});
    @memcpy(framed[41..], bytes);
    try @import("fs_store.zig").writeFileAtomicallyWithCancellation(path, framed, token);
}
pub fn inherit(self: *Self, fs: *Fs, id: []const u8, previous: ?[]const u8, fresh: api.UploadScope, token: Token) !void {
    if (!std.meta.eql((try api.uploadScopeFromArtifactId(id)) orelse return error.InvalidRetentionCertificate, fresh)) return error.InvalidRetentionCertificate;
    // Same-attempt descendants inherit an unsealed proof by construction.
    // Replace the advisory ancestor entry so per-file checkpoints cannot fill
    // the bounded directory and prevent certifying the final serving root.
    if (previous) |old| for (self.pending.items) |*pending| {
        if (std.mem.eql(u8, pending.root, old) and std.meta.eql(pending.fresh, fresh)) {
            if (pending.root.len == id.len) @memcpy(@constCast(pending.root), id) else pending.root = try pending.arena.allocator().dupe(u8, id);
            return;
        }
    };
    if (self.pending.items.len == 64) return;
    var arena = std.heap.ArenaAllocator.init(fs.alloc);
    var keep = false;
    defer if (!keep) arena.deinit();
    const a = arena.allocator();
    var witnesses: []const api.RetentionWitness = &.{};
    if (previous) |old| {
        const cert = load(fs, a, old, token) catch {
            try token.check();
            return;
        };
        if (!try valid(fs, cert.witnesses, token)) return;
        witnesses = cert.witnesses;
        for (witnesses) |entry| {
            if (!std.mem.eql(u8, &entry.scope.domain, &fresh.domain)) return;
            // A namespace already covered by the base cannot be resealed.
            if (std.meta.eql(entry.scope, fresh)) return;
        }
    }
    if (witnesses.len == 4096) return;
    const owned = try a.dupe(u8, id);
    try self.pending.append(fs.alloc, .{ .arena = arena, .root = owned, .witnesses = witnesses, .fresh = fresh, .epoch = try readEpoch(fs, fresh, token) });
    keep = true;
}
pub fn seal(self: *Self, fs: *Fs, token: Token) !void {
    defer {
        for (self.pending.items) |*pending| pending.arena.deinit();
        self.pending.clearRetainingCapacity();
    }
    for (self.pending.items) |*pending| {
        if (!std.meta.eql(pending.epoch, try readEpoch(fs, pending.fresh, token))) continue;
        const fresh = (try witness(fs, pending.fresh, token)) orelse continue;
        if (!std.meta.eql(pending.epoch, try readEpoch(fs, pending.fresh, token))) continue;
        const a = pending.arena.allocator();
        const witnesses = try a.alloc(api.RetentionWitness, pending.witnesses.len + 1);
        @memcpy(witnesses[0..pending.witnesses.len], pending.witnesses);
        witnesses[pending.witnesses.len] = fresh;
        try remember(fs, pending.root, witnesses, token);
    }
}

test "external lake retained closure certificates survive reopen inherit deltas and invalidate lost artifacts" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("retained-closure");
    defer directory.cleanup();
    const first_scope: api.UploadScope = .{ .domain = @splat(7), .attempt = @splat(3) };
    const next_scope: api.UploadScope = .{ .domain = @splat(7), .attempt = @splat(4) };
    var fs = try Fs.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = first_scope;
    var root = try store.put("root");
    defer root.deinit(a);
    var leaf = try store.put("leaf");
    defer leaf.deinit(a);
    try store.inheritClosure(root.artifact_id, null, .none);
    try store.sealClosures(.none);
    try std.testing.expect(try store.retainedClosure(root.artifact_id, .none));
    // Proofs are durable backend state, independent of a query or builder cache.
    var reopened = try Fs.init(a, directory.path());
    defer reopened.deinit();
    var fresh = reopened.artifactStore();
    try std.testing.expect(try fresh.retainedClosure(root.artifact_id, .none));
    fresh.upload_scope = next_scope;
    var next = try fresh.put("next root");
    defer next.deinit(a);
    try fresh.inheritClosure(next.artifact_id, root.artifact_id, .none);
    // Other indexes may still upload in the new attempt before final sealing.
    var extra = try fresh.put("other index");
    defer extra.deinit(a);
    try fresh.sealClosures(.none);
    try std.testing.expect(try fresh.retainedClosure(next.artifact_id, .none));
    try fresh.delete(leaf.artifact_id);
    try std.testing.expect(!try fresh.retainedClosure(root.artifact_id, .none));
    try std.testing.expect(!try fresh.retainedClosure(next.artifact_id, .none));
    fresh.upload_scope = first_scope;
    var replacement = try fresh.putScoped(first_scope, "leaf", .none);
    defer replacement.deinit(a);
    try std.testing.expect(!try fresh.retainedClosure(root.artifact_id, .none));
    // A fresh audit may re-certify restored namespaces, but an old certificate
    // cannot gain authority merely because its content-addressed leaf returns.
    const witnesses = [_]api.RetentionWitness{ (try fresh.retentionWitness(first_scope, .none)).?, (try fresh.retentionWitness(next_scope, .none)).? };
    try fresh.rememberClosure(next.artifact_id, &witnesses, .none);
    try std.testing.expect(try fresh.retainedClosure(next.artifact_id, .none));
    const path = try pathAlloc(&reopened, a, next.artifact_id);
    defer a.free(path);
    try @import("fs_store.zig").writeFileAtomicallyWithCancellation(path, "corrupt certificate", .none);
    try std.testing.expect(!try fresh.retainedClosure(next.artifact_id, .none));
}

test "external lake retained closure pending descendants stay bounded and reject build time deletion" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("pending-closure-chain");
    defer directory.cleanup();
    var fs = try Fs.init(a, directory.path());
    defer fs.deinit();
    var store = fs.artifactStore();
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(2) };
    var root = try store.put("first");
    defer root.deinit(a);
    try store.inheritClosure(root.artifact_id, null, .none);
    for (0..100) |n| {
        var bytes: [32]u8 = undefined;
        var next = try store.put(try std.fmt.bufPrint(&bytes, "root-{d}", .{n}));
        store.inheritClosure(next.artifact_id, root.artifact_id, .none) catch |err| {
            next.deinit(a);
            return err;
        };
        root.deinit(a);
        root = next;
        try std.testing.expectEqual(@as(usize, 1), fs.retention.pending.items.len);
    }
    try store.sealClosures(.none);
    try std.testing.expect(try store.retainedClosure(root.artifact_id, .none));
    store.upload_scope = .{ .domain = @splat(7), .attempt = @splat(3) };
    var leaf = try store.put("referenced leaf");
    defer leaf.deinit(a);
    var next = try store.put("next root");
    defer next.deinit(a);
    try store.inheritClosure(next.artifact_id, root.artifact_id, .none);
    try store.delete(leaf.artifact_id);
    try store.sealClosures(.none);
    try std.testing.expect(!try store.retainedClosure(next.artifact_id, .none));
}
