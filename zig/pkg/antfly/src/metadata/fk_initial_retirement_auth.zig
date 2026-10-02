// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Root-bound signature for an exact, durably unlinked initial-FK replica.
//! This envelope alone is not an ACK: metadata must authenticate the caller,
//! read the current StoreRecord and canceled work in one Raft transaction,
//! compare the registered root/key/reporter, then verify this signature.
const std = @import("std");
const contract = @import("fk_initial_retirement_contract.zig");
const root_signing_identity = @import("antfly_local_sources").storage_db_root_signing_identity;

const domain = "antfly/initial-fk-retirement-ack/v1\x00";
pub const message_len = domain.len + contract.receipt_encoded_len;

fn message(receipt: contract.Receipt) ![message_len]u8 {
    const encoded = try receipt.encode();
    var bytes: [message_len]u8 = undefined;
    @memcpy(bytes[0..domain.len], domain);
    @memcpy(bytes[domain.len..], &encoded);
    return bytes;
}

pub const SignedReceipt = struct {
    receipt: contract.Receipt,
    signature: [64]u8,

    pub fn sign(root: root_signing_identity.State, receipt: contract.Receipt) !@This() {
        try receipt.validate();
        if (root.root_incarnation != receipt.ticket.replica.store_root_incarnation)
            return error.InitialFkRetirementRootChanged;
        const bytes = try message(receipt);
        return .{ .receipt = receipt, .signature = try root.sign(&bytes) };
    }

    pub fn verify(self: @This(), registered_public_key: [32]u8) !void {
        try self.receipt.validate();
        const bytes = try message(self.receipt);
        root_signing_identity.verify(registered_public_key, &bytes, self.signature) catch
            return error.InvalidInitialFkRetirementSignature;
    }
};

test "initial FK retirement ACK signature binds ticket, reporter, and physical root" {
    const Ed25519 = std.crypto.sign.Ed25519;
    const seed: [32]u8 = @splat(7);
    const key_pair = try Ed25519.KeyPair.generateDeterministic(seed);
    const root: root_signing_identity.State = .{
        .root_incarnation = 19,
        .seed = seed,
        .public_key = key_pair.public_key.toBytes(),
    };
    const ticket: contract.Ticket = .{
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .cancel_revision = 29,
        .replica = .{
            .plan_id = @splat(1),
            .plan_digest = @splat(2),
            .child_table_id = 3,
            .group_id = 5,
            .range_id = 7,
            .node_id = 11,
            .store_id = 13,
            .store_incarnation = 17,
            .store_root_incarnation = 19,
            .replica_id = 23,
            .root_generation = 27,
            .canceled = true,
        },
    };
    const intent: contract.Intent = .{ .ticket = ticket, .phase = .unlinked };
    const receipt = try contract.Receipt.fromUnlinkedIntent(intent, 31);
    var signed = try SignedReceipt.sign(root, receipt);
    try signed.verify(root.public_key);
    var wrong_root = root;
    wrong_root.root_incarnation += 1;
    try std.testing.expectError(error.InitialFkRetirementRootChanged, SignedReceipt.sign(wrong_root, receipt));
    signed.receipt.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, signed.verify(root.public_key));
    signed.receipt = receipt;
    signed.signature[0] ^= 1;
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, signed.verify(root.public_key));
    const other_key = try Ed25519.KeyPair.generateDeterministic(@splat(9));
    const original = try SignedReceipt.sign(root, receipt);
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, original.verify(other_key.public_key.toBytes()));
}
