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

//! Root-signed protocol discovery, separate from permanent retirement proof.
//! A fresh nonce is bound by AFAT; observations expire with the sender scope.
const std = @import("std");
const wire = @import("remote_attempt_wire.zig");
const replies = @import("remote_attempt_reply.zig");
const Ed25519 = std.crypto.sign.Ed25519;
pub const encoded_size = 352;

pub const Families = struct {
    group_query: bool = false,
    retained_read: bool = false,
    fn bits(self: Families) u8 {
        return @as(u8, @intFromBool(self.group_query)) | (@as(u8, @intFromBool(self.retained_read)) << 1);
    }
};
pub const Verified = struct {
    authority: replies.Authority,
    families: Families,
    deadline_ns: u64,
    pub fn forAuthority(self: Verified, authority: replies.Authority, now_ns: u64) !Families {
        if (!std.meta.eql(self.authority, authority)) return error.InvalidRemoteReply;
        if (now_ns >= self.deadline_ns) return error.DeadlineExceeded;
        return self.families;
    }
};
fn binding(request: wire.Context) ![32]u8 {
    try request.validate();
    if (request.kind != .capabilities) return error.InvalidRemoteAttempt;
    var normalized = request;
    normalized.remaining_ns = 1;
    const encoded = try normalized.encode();
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update("antfly/remote-capabilities-request/v1");
    hash.update(&encoded);
    return hash.finalResult();
}
pub const Reply = struct {
    local: @import("remote_membership.zig").Local,
    request_binding: [32]u8,
    families: Families,
    signature: [64]u8,
    fn unsigned(self: Reply) [112]u8 {
        var bytes: [112]u8 = @splat(0);
        @memcpy(bytes[0..4], "AFAC");
        std.mem.writeInt(u16, bytes[4..6], 2, .little);
        bytes[6] = self.families.bits();
        @memcpy(bytes[8..40], &self.local.metadata_incarnation);
        std.mem.writeInt(u64, bytes[40..48], self.local.node_id, .little);
        std.mem.writeInt(u64, bytes[48..56], self.local.store_id, .little);
        std.mem.writeInt(u128, bytes[56..72], self.local.root_incarnation, .little);
        std.mem.writeInt(u64, bytes[72..80], self.local.reporter_incarnation, .little);
        @memcpy(bytes[80..112], &self.request_binding);
        return bytes;
    }
    fn digest(self: Reply) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/remote-read-capabilities/v1");
        hash.update(&self.unsigned());
        return hash.finalResult();
    }
    pub fn sign(signer: *const replies.Signer, request: wire.Context, families: Families) !Reply {
        if (request.worker_incarnation != signer.local.root_incarnation) return error.InvalidRemoteReply;
        var reply: Reply = .{ .local = signer.local, .request_binding = try binding(request), .families = families, .signature = undefined };
        reply.signature = (try signer.pair.sign(&reply.digest(), null)).toBytes();
        return reply;
    }
    pub fn encode(self: Reply) [encoded_size]u8 {
        var bytes: [176]u8 = undefined;
        @memcpy(bytes[0..112], &self.unsigned());
        @memcpy(bytes[112..176], &self.signature);
        return std.fmt.bytesToHex(bytes, .lower);
    }
    pub fn decode(encoded: []const u8) !Reply {
        if (encoded.len != encoded_size) return error.InvalidRemoteReply;
        var bytes: [176]u8 = undefined;
        _ = std.fmt.hexToBytes(&bytes, encoded) catch return error.InvalidRemoteReply;
        if (!std.mem.eql(u8, bytes[0..4], "AFAC") or std.mem.readInt(u16, bytes[4..6], .little) != 2 or bytes[7] != 0 or bytes[6] & ~@as(u8, 3) != 0) return error.UnsupportedRemoteReplyProtocol;
        return .{ .local = .{ .metadata_incarnation = bytes[8..40].*, .node_id = std.mem.readInt(u64, bytes[40..48], .little), .store_id = std.mem.readInt(u64, bytes[48..56], .little), .root_incarnation = std.mem.readInt(u128, bytes[56..72], .little), .reporter_incarnation = std.mem.readInt(u64, bytes[72..80], .little) }, .request_binding = bytes[80..112].*, .families = .{ .group_query = bytes[6] & 1 != 0, .retained_read = bytes[6] & 2 != 0 }, .signature = bytes[112..176].* };
    }
    pub fn verify(self: Reply, authority: replies.Authority, request: wire.Context) !void {
        if (!std.meta.eql(self.local, authority.local) or request.worker_incarnation != authority.local.root_incarnation or !std.mem.eql(u8, &self.request_binding, &(try binding(request)))) return error.InvalidRemoteReply;
        const key = Ed25519.PublicKey.fromBytes(authority.public_key) catch return error.InvalidRemoteReplySignature;
        Ed25519.Signature.fromBytes(self.signature).verify(&self.digest(), key) catch return error.InvalidRemoteReplySignature;
    }
};

test "remote attempt wire capabilities bind nonce families root reporter and original observation scope" {
    const pair = try Ed25519.KeyPair.generateDeterministic(@splat(7));
    const local: @import("remote_membership.zig").Local = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 9, .store_id = 13, .root_incarnation = 11, .reporter_incarnation = 17 };
    const signer = try replies.Signer.init(local, pair.public_key.toBytes(), @splat(7));
    const authority: replies.Authority = .{ .local = local, .public_key = pair.public_key.toBytes() };
    const request: wire.Context = .{ .kind = .capabilities, .id = .{ .coordinator = 7, .generation = 2, .operation = 0, .sequence = 0 }, .worker_incarnation = 11, .remaining_ns = 100, .charged_bytes = 0, .request_digest = wire.requestDigest("POST", "/internal/v1/attempt/capabilities", &@as([32]u8, @splat(3))) };
    const reply = try Reply.sign(&signer, request, .{ .retained_read = true });
    const encoded = reply.encode();
    const decoded = try Reply.decode(&encoded);
    try decoded.verify(authority, request);
    try std.testing.expect(decoded.families.retained_read and !decoded.families.group_query);
    var forged = decoded;
    forged.families.group_query = true;
    try std.testing.expectError(error.InvalidRemoteReplySignature, forged.verify(authority, request));
    forged = decoded;
    forged.signature[0] ^= 1;
    try std.testing.expectError(error.InvalidRemoteReplySignature, forged.verify(authority, request));
    var changed = request;
    changed.request_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidRemoteReply, decoded.verify(authority, changed));
    changed = request;
    changed.id.generation += 1;
    try std.testing.expectError(error.InvalidRemoteReply, decoded.verify(authority, changed));
    changed = request;
    changed.kind = .fence;
    try std.testing.expectError(error.InvalidRemoteAttempt, decoded.verify(authority, changed));
    var changed_authority = authority;
    changed_authority.local.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidRemoteReply, decoded.verify(changed_authority, request));
    changed_authority = authority;
    changed_authority.local.root_incarnation += 1;
    try std.testing.expectError(error.InvalidRemoteReply, decoded.verify(changed_authority, request));
    changed_authority = authority;
    changed_authority.public_key = (try Ed25519.KeyPair.generateDeterministic(@splat(8))).public_key.toBytes();
    try std.testing.expectError(error.InvalidRemoteReplySignature, decoded.verify(changed_authority, request));
    // A capability observation cannot be decoded as permanent retirement proof.
    try std.testing.expectError(error.InvalidRemoteReply, replies.Reply.decode(&encoded));
    var malformed = encoded;
    malformed[9] = '1'; // AFAC v1 did not promise process-scoped endpoints.
    try std.testing.expectError(error.UnsupportedRemoteReplyProtocol, Reply.decode(&malformed));
    var bytes: [176]u8 = undefined;
    _ = try std.fmt.hexToBytes(&bytes, &encoded);
    bytes[6] |= 128;
    const unknown = std.fmt.bytesToHex(bytes, .lower);
    try std.testing.expectError(error.UnsupportedRemoteReplyProtocol, Reply.decode(&unknown));
    const observation: Verified = .{ .authority = authority, .families = decoded.families, .deadline_ns = 100 };
    _ = try observation.forAuthority(authority, 99);
    try std.testing.expectError(error.DeadlineExceeded, observation.forAuthority(authority, 100));
    try std.testing.expectError(error.InvalidRemoteReply, observation.forAuthority(changed_authority, 99));
    // Invalid data-like identities cannot masquerade as discovery requests.
    changed = request;
    changed.id.operation = 1;
    try std.testing.expectError(error.InvalidRemoteAttempt, changed.validate());
}
