// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Challenge-bound evidence of native backing and a DATA-owned membership
//! snapshot. This proof is not a free-slot promise or permission to publish a
//! metadata activation without checking its current generation and membership.
const std = @import("std");
const Hmac = std.crypto.auth.hmac.sha2.HmacSha256;
pub const max_members = 256;
pub const max_request_bytes = 2048;
pub const max_frame_bytes = 32768;
const domain = "antfly-native-completion-attestation-v1";

pub const Keys = struct {
    primary: []const u8,
    verification: ?[]const u8 = null,
    issuer: []const u8,
};
pub const Request = struct {
    version: u32 = 1,
    requester: u64,
    node_id: u64,
    group_id: u64,
    nonce: u128,
    incarnation: [16]u8,
    policy_digest: [32]u8,
    generation: u64,
};
pub const Membership = struct {
    voters: u32 = 0,
    outgoing: u32 = 0,
    learners: u32 = 0,
    learners_next: u32 = 0,
    auto_leave: bool = false,
    // Each category is sorted independently. The unused tail is always zero.
    nodes: [max_members]u64 = @splat(0),
};
pub const Proof = struct {
    version: u32 = 1,
    request: Request,
    capacity: u32,
    accepted: u32,
    prepared: u32,
    term: u64,
    commit_index: u64,
    applied_index: u64,
    last_index: u64,
    leader_id: u64,
    membership: Membership,
};

pub fn validateRequest(request: Request) !void {
    if (request.version != 1 or request.requester == 0 or request.node_id == 0 or request.group_id == 0 or
        request.nonce == 0 or request.generation == 0 or std.mem.allEqual(u8, &request.incarnation, 0) or
        std.mem.allEqual(u8, &request.policy_digest, 0)) return error.InvalidCompletionAttestation;
}

pub fn validateProof(proof: Proof) !void {
    try validateRequest(proof.request);
    if (proof.version != 1 or proof.capacity == 0 or proof.capacity > 256 or
        proof.accepted > proof.capacity or proof.prepared > proof.capacity or
        proof.applied_index != proof.commit_index or proof.commit_index != proof.last_index)
        return error.InvalidCompletionAttestation;
    var offset: usize = 0;
    var local_member = false;
    if (proof.membership.voters == 0) return error.InvalidCompletionAttestation;
    for ([_]u32{ proof.membership.voters, proof.membership.outgoing, proof.membership.learners, proof.membership.learners_next }) |count| {
        if (count > max_members - offset) return error.InvalidCompletionAttestation;
        var previous: u64 = 0;
        for (proof.membership.nodes[offset..][0..count]) |node| {
            if (node == 0 or node <= previous) return error.InvalidCompletionAttestation;
            previous = node;
            local_member = local_member or node == proof.request.node_id;
        }
        offset += count;
    }
    if (!local_member or !std.mem.allEqual(u64, proof.membership.nodes[offset..], 0)) return error.InvalidCompletionAttestation;
}

pub fn validateKeys(keys: Keys) !void {
    if (keys.primary.len < 32 or keys.issuer.len == 0 or keys.issuer.len > 256) return error.CompletionAttestationUnavailable;
    if (keys.verification) |key| if (key.len < 32) return error.CompletionAttestationUnavailable;
}
fn mac(keys: Keys, secret: []const u8, payload: []const u8) [32]u8 {
    var hash = Hmac.init(secret);
    for ([_][]const u8{ domain, keys.issuer, payload }) |part| {
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, @intCast(part.len), .little);
        hash.update(&length);
        hash.update(part);
    }
    var result: [32]u8 = undefined;
    hash.final(&result);
    return result;
}

pub fn sign(alloc: std.mem.Allocator, keys: Keys, proof: Proof) ![]u8 {
    try validateKeys(keys);
    try validateProof(proof);
    const json = try std.json.Stringify.valueAlloc(alloc, proof, .{});
    defer alloc.free(json);
    const encoder = std.base64.url_safe_no_pad.Encoder;
    const payload_len = encoder.calcSize(json.len);
    const length = payload_len + 1 + encoder.calcSize(32);
    if (length > max_frame_bytes) return error.InvalidCompletionAttestation;
    const frame = try alloc.alloc(u8, length);
    _ = encoder.encode(frame[0..payload_len], json);
    frame[payload_len] = '.';
    const signature = mac(keys, keys.primary, frame[0..payload_len]);
    _ = encoder.encode(frame[payload_len + 1 ..], &signature);
    return frame;
}

pub fn verify(alloc: std.mem.Allocator, keys: Keys, frame: []const u8, expected: Request) !Proof {
    try validateKeys(keys);
    try validateRequest(expected);
    if (frame.len > max_frame_bytes) return error.InvalidCompletionAttestation;
    const split = std.mem.indexOfScalar(u8, frame, '.') orelse return error.InvalidCompletionAttestation;
    const payload = frame[0..split];
    const decoder = std.base64.url_safe_no_pad.Decoder;
    if ((decoder.calcSizeForSlice(frame[split + 1 ..]) catch return error.InvalidCompletionAttestation) != 32) return error.InvalidCompletionAttestation;
    var supplied: [32]u8 = undefined;
    decoder.decode(&supplied, frame[split + 1 ..]) catch return error.InvalidCompletionAttestation;
    const primary = mac(keys, keys.primary, payload);
    const alternate = mac(keys, keys.verification orelse keys.primary, payload);
    const primary_ok = std.crypto.timing_safe.eql([32]u8, supplied, primary);
    const alternate_ok = std.crypto.timing_safe.eql([32]u8, supplied, alternate);
    if (!primary_ok and !alternate_ok) return error.InvalidCompletionAttestationSignature;
    const json = try alloc.alloc(u8, decoder.calcSizeForSlice(payload) catch return error.InvalidCompletionAttestation);
    defer alloc.free(json);
    decoder.decode(json, payload) catch return error.InvalidCompletionAttestation;
    const parsed = std.json.parseFromSlice(Proof, alloc, json, .{}) catch |err| return switch (err) {
        error.OutOfMemory => error.OutOfMemory,
        else => error.InvalidCompletionAttestation,
    };
    defer parsed.deinit();
    const proof = parsed.value;
    try validateProof(proof);
    if (!std.meta.eql(proof.request, expected)) return error.CompletionAttestationIdentityMismatch;
    return proof;
}

test "workload admission completion attestation binds backing identity membership challenge and principal" {
    const alloc = std.testing.allocator;
    const request: Request = .{ .requester = 3, .node_id = 5, .group_id = 7, .nonce = 11, .incarnation = @splat(13), .policy_digest = @splat(17), .generation = 19 };
    var proof: Proof = .{ .request = request, .capacity = 4, .accepted = 2, .prepared = 1, .term = 2, .commit_index = 3, .applied_index = 3, .last_index = 3, .leader_id = 5, .membership = .{ .voters = 2 } };
    proof.membership.nodes[0] = 5;
    proof.membership.nodes[1] = 6;
    const keys: Keys = .{ .primary = "a" ** 32, .issuer = "cluster" };
    const frame = try sign(alloc, keys, proof);
    defer alloc.free(frame);
    const verified = try verify(alloc, .{ .primary = "b" ** 32, .verification = keys.primary, .issuer = keys.issuer }, frame, request);
    try std.testing.expectEqual(@as(u32, 2), verified.membership.voters);
    inline for (.{ "requester", "node_id", "group_id", "nonce", "generation" }) |field| {
        var changed = request;
        @field(changed, field) += 1;
        try std.testing.expectError(error.CompletionAttestationIdentityMismatch, verify(alloc, keys, frame, changed));
    }
    var changed = request;
    changed.incarnation[0] += 1;
    try std.testing.expectError(error.CompletionAttestationIdentityMismatch, verify(alloc, keys, frame, changed));
    changed = request;
    changed.policy_digest[0] += 1;
    try std.testing.expectError(error.CompletionAttestationIdentityMismatch, verify(alloc, keys, frame, changed));
    try std.testing.expectError(error.InvalidCompletionAttestationSignature, verify(alloc, .{ .primary = keys.primary, .issuer = "other" }, frame, request));
    frame[1] = if (frame[1] == 'A') 'B' else 'A';
    try std.testing.expectError(error.InvalidCompletionAttestationSignature, verify(alloc, keys, frame, request));
    proof.last_index += 1;
    try std.testing.expectError(error.InvalidCompletionAttestation, sign(alloc, keys, proof));
    proof.last_index -= 1;
    proof.applied_index -= 1;
    try std.testing.expectError(error.InvalidCompletionAttestation, sign(alloc, keys, proof));
    proof.applied_index += 1;
    proof.membership.nodes[1] = 5;
    try std.testing.expectError(error.InvalidCompletionAttestation, sign(alloc, keys, proof));
    proof.membership.nodes[1] = 6;
    proof.membership.nodes[2] = 9;
    try std.testing.expectError(error.InvalidCompletionAttestation, sign(alloc, keys, proof));
}
