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

//! A signed receipt for a physically joined node, bound to one exact checkpoint.
//! The exclusive journal lease and fresh outgoing epoch prevent reuse after a
//! later owner starts. Publication history is never cleared by this receipt.
const std = @import("std");
const replies = @import("remote_attempt_reply.zig");
const durable = @import("remote_fence_store.zig");
const fs_paths = @import("antfly_runtime_fs").fs_paths;
pub const file_name = "remote-quiescence.v1";
const pending_name = "remote-quiescence.v1.pending";
pub const size = 208;
const unsigned_size = 144;
const magic = "AFQREC01";

fn digest(bytes: []const u8) [32]u8 {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/remote-node-quiescence/v1");
    hash.update(bytes);
    return hash.finalResult();
}

pub fn encode(signer: *const replies.Signer, initialization: durable.InitializationId, checkpoint: [32]u8) ![size]u8 {
    if (std.mem.allEqual(u8, &initialization, 0) or signer.local.reporter_incarnation == 0) return error.InvalidRemoteQuiescence;
    var bytes: [size]u8 = @splat(0);
    @memcpy(bytes[0..8], magic);
    @memcpy(bytes[8..40], &signer.local.metadata_incarnation);
    std.mem.writeInt(u64, bytes[40..48], signer.local.node_id, .little);
    std.mem.writeInt(u64, bytes[48..56], signer.local.store_id, .little);
    std.mem.writeInt(u128, bytes[56..72], signer.local.root_incarnation, .little);
    std.mem.writeInt(u64, bytes[72..80], signer.local.reporter_incarnation, .little);
    @memcpy(bytes[80..112], &initialization);
    @memcpy(bytes[112..144], &checkpoint);
    const signature = try signer.pair.sign(&digest(bytes[0..unsigned_size]), null);
    @memcpy(bytes[144..], &signature.toBytes());
    return bytes;
}

pub fn verify(bytes: *const [size]u8, authority: replies.Authority, initialization: durable.InitializationId, checkpoint: [32]u8) !void {
    const local = authority.local;
    if (!std.mem.eql(u8, bytes[0..8], magic) or
        !std.mem.eql(u8, bytes[8..40], &local.metadata_incarnation) or
        std.mem.readInt(u64, bytes[40..48], .little) != local.node_id or
        std.mem.readInt(u64, bytes[48..56], .little) != local.store_id or
        std.mem.readInt(u128, bytes[56..72], .little) != local.root_incarnation or
        std.mem.readInt(u64, bytes[72..80], .little) == 0 or
        std.mem.allEqual(u8, &initialization, 0) or
        !std.mem.eql(u8, bytes[80..112], &initialization) or
        !std.mem.eql(u8, bytes[112..144], &checkpoint)) return error.InvalidRemoteQuiescence;
    const key = std.crypto.sign.Ed25519.PublicKey.fromBytes(authority.public_key) catch return error.InvalidRemoteQuiescence;
    std.crypto.sign.Ed25519.Signature.fromBytes(bytes[144..208].*).verify(&digest(bytes[0..unsigned_size]), key) catch return error.InvalidRemoteQuiescence;
}

/// Call only after joining every inbound/outbound physical parent and transport.
/// The caller retains the exclusive journal lease through durable publication.
pub fn record(store: *durable.Store, signer: *const replies.Signer) !void {
    const journal = try store.initializationState();
    const bytes = try encode(signer, journal.id, try store.checkpointDigest());
    const file = try store.dir.createFile(store.io, pending_name, .{ .read = true, .truncate = true });
    var live = true;
    defer if (live) file.close(store.io);
    try file.writePositionalAll(store.io, &bytes, 0);
    try file.sync(store.io);
    file.close(store.io);
    live = false;
    try std.Io.Dir.rename(store.dir, pending_name, store.dir, file_name, store.io);
    try fs_paths.syncDirectoryHandlePortable(store.io, store.dir);
}

/// Absence, corruption and I/O refusal provide no recovery authority. Check
/// before startCoordinator advances the durable epoch and invalidates the receipt.
pub fn matches(store: *durable.Store, authority: replies.Authority) bool {
    const journal = store.initializationState() catch return false;
    const checkpoint = store.checkpointDigest() catch return false;
    const file = store.dir.openFile(store.io, file_name, .{}) catch return false;
    defer file.close(store.io);
    if ((file.stat(store.io) catch return false).size != size) return false;
    var bytes: [size]u8 = undefined;
    if ((file.readPositionalAll(store.io, &bytes, 0) catch return false) != bytes.len) return false;
    verify(&bytes, authority, journal.id, checkpoint) catch return false;
    return true;
}

test "remote owner recovery receipt authenticates exact root checkpoint initialization and prior reporter" {
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(7));
    const local: @import("remote_membership.zig").Local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 22, .reporter_incarnation = 17 };
    const signer = try replies.Signer.init(local, pair.public_key.toBytes(), @splat(7));
    var authority = replies.Authority{ .local = local, .public_key = pair.public_key.toBytes() };
    authority.local.reporter_incarnation = 18;
    const bytes = try encode(&signer, @splat(9), @splat(42));
    try verify(&bytes, authority, @splat(9), @splat(42));
    for ([_]usize{ 0, 8, 40, 48, 56, 72, 80, 112, 144, 207 }) |offset| {
        var changed = bytes;
        changed[offset] ^= 1;
        try std.testing.expectError(error.InvalidRemoteQuiescence, verify(&changed, authority, @splat(9), @splat(42)));
    }
    try std.testing.expectError(error.InvalidRemoteQuiescence, verify(&bytes, authority, @splat(10), @splat(42)));
    try std.testing.expectError(error.InvalidRemoteQuiescence, verify(&bytes, authority, @splat(9), @splat(43)));
    authority.public_key = (try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(8))).public_key.toBytes();
    try std.testing.expectError(error.InvalidRemoteQuiescence, verify(&bytes, authority, @splat(9), @splat(42)));
    try std.testing.expectError(error.InvalidRemoteQuiescence, encode(&signer, @splat(0), @splat(42)));
}
