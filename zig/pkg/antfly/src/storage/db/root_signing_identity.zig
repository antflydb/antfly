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

//! Private proof-of-possession identity for one physical replica store root.
//! A root UUID is an identity, not a secret. Future destructive retirement
//! ACKs must be signed by this root-local key and verified against the public
//! key durably registered for that exact root. This module alone grants no
//! permission to delete a replica or acknowledge retirement.

const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const root_identity = @import("root_identity.zig");
const Ed25519 = std.crypto.sign.Ed25519;
const Allocator = std.mem.Allocator;

const file_name = "root_signing_identity.checkpoint";
const lock_name = "root_signing_identity.checkpoint.lock";
const magic = "AFROOTS1";
const version: u32 = 1;
const encoded_len = magic.len + 4 + 16 + 32 + 32 + 4;

pub const State = struct {
    root_incarnation: u128,
    seed: [32]u8,
    public_key: [32]u8,

    pub fn sign(self: @This(), message: []const u8) ![64]u8 {
        const pair = try Ed25519.KeyPair.generateDeterministic(self.seed);
        const derived_public = pair.public_key.toBytes();
        if (!std.mem.eql(u8, &self.public_key, &derived_public)) return error.InvalidRootSigningIdentity;
        return (try pair.sign(message, null)).toBytes();
    }
};

pub fn verify(public_key: [32]u8, message: []const u8, signature: [64]u8) !void {
    const key = try Ed25519.PublicKey.fromBytes(public_key);
    const sig = Ed25519.Signature.fromBytes(signature);
    var verifier = try sig.verifier(key);
    verifier.update(message);
    try verifier.verify();
}

pub fn checkpointPathAlloc(alloc: Allocator, root_dir: []const u8) ![]u8 {
    return std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, file_name });
}

pub fn loadOrCreate(alloc: Allocator, io: std.Io, root_dir: []const u8) !State {
    const root = try root_identity.loadOrCreate(alloc, io, root_dir);
    const path = try checkpointPathAlloc(alloc, root_dir);
    defer alloc.free(path);
    return loadPath(alloc, io, path, root.incarnation) catch |err| switch (err) {
        error.FileNotFound => try createLocked(alloc, io, root_dir, path, root.incarnation),
        else => return err,
    };
}

pub fn load(alloc: Allocator, io: std.Io, root_dir: []const u8) !State {
    const root = try root_identity.load(alloc, io, root_dir);
    const path = try checkpointPathAlloc(alloc, root_dir);
    defer alloc.free(path);
    return loadPath(alloc, io, path, root.incarnation);
}

fn newState(io: std.Io, root_incarnation: u128) !State {
    var seed: [32]u8 = undefined;
    while (true) {
        try io.randomSecure(&seed);
        const pair = Ed25519.KeyPair.generateDeterministic(seed) catch continue;
        return .{ .root_incarnation = root_incarnation, .seed = seed, .public_key = pair.public_key.toBytes() };
    }
}

fn createLocked(alloc: Allocator, io: std.Io, root_dir: []const u8, path: []const u8, root_incarnation: u128) !State {
    const lock_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root_dir, lock_name });
    defer alloc.free(lock_path);
    if (fs_paths.createFilePortable(io, lock_path, .{ .truncate = false, .exclusive = true, .permissions = privateFilePermissions() })) |seed_file| {
        seed_file.close(io);
    } else |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    }
    const lock_file = try fs_paths.createFilePortable(io, lock_path, .{
        .read = true,
        .truncate = false,
        .lock = .exclusive,
        .lock_nonblocking = false,
    });
    defer lock_file.close(io);
    return loadPath(alloc, io, path, root_incarnation) catch |err| switch (err) {
        error.FileNotFound => {
            const created = try newState(io, root_incarnation);
            try writePath(alloc, io, path, created);
            return created;
        },
        else => return err,
    };
}

fn privateFilePermissions() std.Io.File.Permissions {
    if (comptime @import("builtin").os.tag == .windows) return .default_file;
    return @enumFromInt(0o600);
}

fn loadPath(alloc: Allocator, io: std.Io, path: []const u8, expected_root: u128) !State {
    // Open the exact inode we inspect. Never follow a symlink to an attacker-
    // controlled seed, and reject restored checkpoints with permissive mode.
    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .follow_symlinks = false });
    defer file.close(io);
    const stat = try file.stat(io);
    if (stat.kind != .file) return error.InsecureRootSigningIdentity;
    if (comptime @import("builtin").os.tag != .windows) {
        if ((stat.permissions.toMode() & 0o077) != 0) return error.InsecureRootSigningIdentity;
    }
    var buffer: [encoded_len + 1]u8 = undefined;
    var reader = file.reader(io, &buffer);
    const raw = try reader.interface.allocRemaining(alloc, .limited(encoded_len + 1));
    defer alloc.free(raw);
    const state = try decode(raw);
    if (state.root_incarnation != expected_root) return error.RootSigningIdentityRootChanged;
    return state;
}

fn writePath(alloc: Allocator, io: std.Io, path: []const u8, state: State) !void {
    const encoded = encode(state);
    const tmp_path = try std.fmt.allocPrint(alloc, "{s}.tmp-{x}", .{ path, state.root_incarnation });
    defer alloc.free(tmp_path);
    // Called only while holding the creation lock. A crash after writing the
    // temp but before rename must not permanently strand the key checkpoint.
    std.Io.Dir.cwd().deleteFile(io, tmp_path) catch |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    };
    {
        var file = try fs_paths.createFilePortable(io, tmp_path, .{
            .truncate = true,
            .exclusive = true,
            .permissions = privateFilePermissions(),
        });
        defer file.close(io);
        errdefer std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        var buffer: [encoded_len]u8 = undefined;
        var writer = file.writer(io, &buffer);
        try writer.interface.writeAll(&encoded);
        try writer.end();
        try file.sync(io);
    }
    std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), path, io) catch |err| {
        std.Io.Dir.cwd().deleteFile(io, tmp_path) catch {};
        return err;
    };
    try fs_paths.syncDirPortable(io, std.fs.path.dirname(path) orelse ".");
}

fn encode(state: State) [encoded_len]u8 {
    var bytes: [encoded_len]u8 = undefined;
    @memcpy(bytes[0..magic.len], magic);
    std.mem.writeInt(u32, bytes[magic.len..][0..4], version, .little);
    std.mem.writeInt(u128, bytes[magic.len + 4 ..][0..16], state.root_incarnation, .little);
    @memcpy(bytes[magic.len + 20 ..][0..32], &state.seed);
    @memcpy(bytes[magic.len + 52 ..][0..32], &state.public_key);
    std.mem.writeInt(u32, bytes[encoded_len - 4 ..][0..4], Crc32.hash(bytes[0 .. encoded_len - 4]), .little);
    return bytes;
}

fn decode(bytes: []const u8) !State {
    if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..magic.len], magic) or
        std.mem.readInt(u32, bytes[magic.len..][0..4], .little) != version or
        Crc32.hash(bytes[0 .. encoded_len - 4]) != std.mem.readInt(u32, bytes[encoded_len - 4 ..][0..4], .little))
        return error.InvalidRootSigningIdentity;
    const root_incarnation = std.mem.readInt(u128, bytes[magic.len + 4 ..][0..16], .little);
    if (root_incarnation == 0) return error.InvalidRootSigningIdentity;
    var seed: [32]u8 = undefined;
    var public_key: [32]u8 = undefined;
    @memcpy(&seed, bytes[magic.len + 20 ..][0..32]);
    @memcpy(&public_key, bytes[magic.len + 52 ..][0..32]);
    const pair = Ed25519.KeyPair.generateDeterministic(seed) catch return error.InvalidRootSigningIdentity;
    const derived_public = pair.public_key.toBytes();
    if (!std.mem.eql(u8, &public_key, &derived_public)) return error.InvalidRootSigningIdentity;
    return .{ .root_incarnation = root_incarnation, .seed = seed, .public_key = public_key };
}

test "root signing identity survives reopen, rotates with root, and rejects corruption" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const first_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/signing-a", .{tmp.sub_path});
    defer alloc.free(first_path);
    const second_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/signing-b", .{tmp.sub_path});
    defer alloc.free(second_path);
    const first = try loadOrCreate(alloc, std.testing.io, first_path);
    const reopened = try load(alloc, std.testing.io, first_path);
    try std.testing.expectEqualDeep(first, reopened);
    const second = try loadOrCreate(alloc, std.testing.io, second_path);
    try std.testing.expect(first.root_incarnation != second.root_incarnation);
    try std.testing.expect(!std.mem.eql(u8, &first.public_key, &second.public_key));
    const message = "initial-fk-retirement-ticket";
    const signature = try first.sign(message);
    try verify(first.public_key, message, signature);
    try std.testing.expectError(error.SignatureVerificationFailed, verify(second.public_key, message, signature));
    var corrupt = encode(first);
    corrupt[magic.len + 20] ^= 1;
    try std.testing.expectError(error.InvalidRootSigningIdentity, decode(&corrupt));
}

test "root signing identity recovers an interrupted temporary checkpoint" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/interrupted-signing", .{tmp.sub_path});
    defer alloc.free(path);
    const root = try root_identity.loadOrCreate(alloc, std.testing.io, path);
    const checkpoint = try checkpointPathAlloc(alloc, path);
    defer alloc.free(checkpoint);
    const stale = try std.fmt.allocPrint(alloc, "{s}.tmp-{x}", .{ checkpoint, root.incarnation });
    defer alloc.free(stale);
    {
        const file = try fs_paths.createFilePortable(std.testing.io, stale, .{ .exclusive = true, .permissions = privateFilePermissions() });
        file.close(std.testing.io);
    }
    const state = try loadOrCreate(alloc, std.testing.io, path);
    try std.testing.expectEqual(root.incarnation, state.root_incarnation);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openFile(std.testing.io, stale, .{}));
    try std.testing.expectEqualDeep(state, try load(alloc, std.testing.io, path));
}

test "root signing identity refuses a world-readable restored checkpoint" {
    if (comptime @import("builtin").os.tag == .windows) return;
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/permissive-signing", .{tmp.sub_path});
    defer alloc.free(path);
    _ = try loadOrCreate(alloc, std.testing.io, path);
    const checkpoint = try checkpointPathAlloc(alloc, path);
    defer alloc.free(checkpoint);
    try std.Io.Dir.cwd().setFilePermissions(std.testing.io, checkpoint, @enumFromInt(0o644), .{});
    try std.testing.expectError(error.InsecureRootSigningIdentity, load(alloc, std.testing.io, path));
}

test "root signing identity refuses a symlinked checkpoint" {
    if (comptime @import("builtin").os.tag == .windows) return;
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source-root", .{tmp.sub_path});
    defer alloc.free(source_path);
    const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/target-root", .{tmp.sub_path});
    defer alloc.free(target_path);
    _ = try loadOrCreate(alloc, std.testing.io, source_path);
    _ = try root_identity.loadOrCreate(alloc, std.testing.io, target_path);
    const target_checkpoint = try checkpointPathAlloc(alloc, target_path);
    defer alloc.free(target_checkpoint);
    const source_checkpoint = try checkpointPathAlloc(alloc, source_path);
    defer alloc.free(source_checkpoint);
    try std.Io.Dir.cwd().symLink(std.testing.io, source_checkpoint, target_checkpoint, .{});
    try std.testing.expectError(error.SymLinkLoop, load(alloc, std.testing.io, target_path));
}
