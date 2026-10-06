// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Internal-only, bounded transport contract for hosted initial-FK retirement.
//! These messages do not authorize deletion by themselves. The endpoint must
//! authenticate the store principal, and the Raft ACK must validate the exact
//! canceled work and current physical-root registration.
const std = @import("std");
const auth = @import("fk_initial_retirement_auth.zig");
const contract = @import("fk_initial_retirement_contract.zig");
const retirement = @import("fk_initial_retirement.zig");

test {
    _ = @import("fk_initial_retirement_ack.zig");
}

pub const max_page_items: u16 = 128;
pub const max_cursor_len: usize = 256;

pub const PageRequest = struct {
    store_id: u64,
    root_incarnation: u128 = 0,
    after_key: ?[]const u8 = null,
    limit: u16 = max_page_items,

    pub fn validate(self: @This(), metadata_group_id: u64) !void {
        if (metadata_group_id == 0 or self.store_id == 0 or self.limit == 0 or self.limit > max_page_items)
            return error.InvalidInitialFkRetirementPage;
        if (self.after_key) |key| try validateCursor(metadata_group_id, self.store_id, key);
    }
};

pub const SignedPageRequest = struct {
    identity: @import("store_root_enrollment.zig").Identity,
    reporter_incarnation: u64,
    page: PageRequest,
    signature: [64]u8,

    fn digest(self: @This()) ![32]u8 {
        if (self.reporter_incarnation == 0 or self.page.store_id != self.identity.store_id or self.page.root_incarnation != self.identity.root_incarnation or
            self.page.limit == 0 or self.page.limit > max_page_items or
            (self.page.after_key != null and self.page.after_key.?.len > max_cursor_len)) return error.InvalidInitialFkRetirementPage;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/initial-fk-retirement-page/v1");
        hash.update(&(try self.identity.digest()));
        var numbers: [10]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.reporter_incarnation, .little);
        std.mem.writeInt(u16, numbers[8..10], self.page.limit, .little);
        hash.update(&numbers);
        hash.update(&.{@intFromBool(self.page.after_key != null)});
        if (self.page.after_key) |cursor| hash.update(cursor);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn verify(self: @This()) !void {
        const message = try self.digest();
        const verifier = try std.crypto.sign.Ed25519.PublicKey.fromBytes(self.identity.public_key);
        std.crypto.sign.Ed25519.Signature.fromBytes(self.signature).verify(&message, verifier) catch return error.InvalidInitialFkRetirementSignature;
    }

    pub fn sign(identity: @import("store_root_enrollment.zig").Identity, reporter_incarnation: u64, page: PageRequest, seed: [32]u8) !@This() {
        var result: @This() = .{ .identity = identity, .reporter_incarnation = reporter_incarnation, .page = page, .signature = undefined };
        const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
        if (!std.mem.eql(u8, &pair.public_key.toBytes(), &identity.public_key)) return error.InvalidInitialFkRetirementSignature;
        result.signature = (try pair.sign(&(try result.digest()), null)).toBytes();
        return result;
    }
};

pub const Control = union(enum) {
    preflight_enrollment: @import("store_root_enrollment.zig").Request,
    enrollment_status: @import("store_root_enrollment.zig").Identity,
    authorized_page: SignedPageRequest,
    preflight_ack: AckRequest,
    ack_status: AckRequest,
};

test "hosted initial FK retirement page signature binds root reporter and cursor" {
    const seed: [32]u8 = @splat(11);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const identity: @import("store_root_enrollment.zig").Identity = .{
        .metadata_incarnation = @splat('7'),
        .node_id = 1,
        .store_id = 2,
        .root_incarnation = 3,
        .public_key = pair.public_key.toBytes(),
    };
    const signed = try SignedPageRequest.sign(identity, 4, .{ .store_id = 2, .root_incarnation = 3, .limit = 1 }, seed);
    try signed.verify();
    var changed = signed;
    changed.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, changed.verify());
    changed = signed;
    changed.page.after_key = "other cursor";
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, changed.verify());
    changed = signed;
    changed.page.limit = 2;
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, changed.verify());
    changed = signed;
    changed.identity.metadata_incarnation[0] ^= 1;
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, changed.verify());
    changed = signed;
    changed.page.root_incarnation += 1;
    try std.testing.expectError(error.InvalidInitialFkRetirementPage, changed.verify());
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, SignedPageRequest.sign(identity, 4, signed.page, @splat(12)));
}

/// Store-indexed work is ordered by the same immutable key as the metadata
/// cursor. `next_key` may advance across already-ACKed rows and is therefore
/// not required to equal the last returned ticket's key.
pub const PageResponse = struct {
    items: []const contract.Ticket,
    next_key: ?[]const u8 = null,

    pub fn validate(self: @This(), metadata_group_id: u64, request: PageRequest) !void {
        try request.validate(metadata_group_id);
        if (self.items.len > request.limit) return error.InvalidInitialFkRetirementPage;
        var key_buf: [max_cursor_len]u8 = undefined;
        var previous_key_buf: [max_cursor_len]u8 = undefined;
        var previous_key_len: usize = 0;
        for (self.items) |item| {
            try item.validate();
            if (item.replica.store_id != request.store_id or
                (request.root_incarnation != 0 and item.replica.store_root_incarnation != request.root_incarnation)) return error.InvalidInitialFkRetirementPage;
            const key = try retirement.storeKey(&key_buf, metadata_group_id, item.replica);
            if (previous_key_len != 0) {
                if (std.mem.order(u8, key, previous_key_buf[0..previous_key_len]) != .gt) return error.InvalidInitialFkRetirementPage;
            } else if (request.after_key) |prior| {
                if (std.mem.order(u8, key, prior) != .gt) return error.InvalidInitialFkRetirementPage;
            }
            @memcpy(previous_key_buf[0..key.len], key);
            previous_key_len = key.len;
        }
        if (self.next_key) |next| {
            try validateCursor(metadata_group_id, request.store_id, next);
            if (request.after_key) |prior| if (std.mem.order(u8, next, prior) != .gt) return error.InvalidInitialFkRetirementPage;
            if (previous_key_len != 0 and std.mem.order(u8, next, previous_key_buf[0..previous_key_len]) == .lt)
                return error.InvalidInitialFkRetirementPage;
        }
    }
};

pub const AckRequest = struct {
    signed: auth.SignedReceipt,

    pub fn validate(self: @This(), metadata_group_id: u64) !void {
        if (metadata_group_id == 0) return error.InvalidInitialFkRetirementAck;
        self.signed.receipt.validate() catch return error.InvalidInitialFkRetirementAck;
    }
};

pub const AckDisposition = enum { newly_acked, already_acked };

pub const AckResponse = struct {
    ticket_digest: contract.Digest,
    disposition: AckDisposition,

    pub fn validate(self: @This(), request: AckRequest) !void {
        const expected = try request.signed.receipt.ticket.digest();
        if (!std.mem.eql(u8, &self.ticket_digest, &expected)) return error.InvalidInitialFkRetirementAck;
    }
};

/// The cursor is an opaque exact store-index key, not arbitrary keyspace
/// access. Enforce the canonical fixed-width suffix before passing it to a
/// metadata cursor; the store prefix alone is insufficient for that check.
pub fn validateCursor(metadata_group_id: u64, store_id: u64, key: []const u8) !void {
    var prefix_buf: [160]u8 = undefined;
    const prefix = try retirement.storePrefix(&prefix_buf, metadata_group_id, store_id);
    const suffix_len = 16 + 1 + 16 + 1 + 16 + 1 + 16 + 1 + 32;
    if (key.len != prefix.len + suffix_len or key.len > max_cursor_len or
        !std.mem.startsWith(u8, key, prefix)) return error.InvalidInitialFkRetirementPage;
    const suffix = key[prefix.len..];
    for (suffix, 0..) |char, index| {
        if (index == 16 or index == 33 or index == 50 or index == 67) {
            if (char != ':') return error.InvalidInitialFkRetirementPage;
        } else if (!std.ascii.isDigit(char) and !(char >= 'a' and char <= 'f')) {
            return error.InvalidInitialFkRetirementPage;
        }
    }
}

test "hosted initial FK retirement wire bounds exact store cursor and ACK" {
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
    const request: PageRequest = .{ .store_id = 13, .limit = 1 };
    try request.validate(1);
    try (PageResponse{ .items = &.{ticket} }).validate(1, request);
    var second = ticket;
    second.replica.replica_id += 1;
    try (PageResponse{ .items = &.{ ticket, second } }).validate(1, .{ .store_id = 13, .limit = 2 });
    try std.testing.expectError(error.InvalidInitialFkRetirementPage, (PageResponse{ .items = &.{ second, ticket } }).validate(1, .{ .store_id = 13, .limit = 2 }));
    try std.testing.expectError(error.InvalidInitialFkRetirementPage, (PageResponse{ .items = &.{ ticket, ticket } }).validate(1, .{ .store_id = 13, .limit = 2 }));
    var key_buf: [max_cursor_len]u8 = undefined;
    const cursor = try retirement.storeKey(&key_buf, 1, ticket.replica);
    try validateCursor(1, 13, cursor);
    try std.testing.expectError(error.InvalidInitialFkRetirementPage, validateCursor(1, 14, cursor));
    try std.testing.expectError(error.InvalidInitialFkRetirementPage, (PageRequest{ .store_id = 13, .limit = 129 }).validate(1));
    try std.testing.expectError(error.InvalidInitialFkRetirementPage, (PageResponse{ .items = &.{ticket}, .next_key = cursor }).validate(1, .{ .store_id = 13, .after_key = cursor, .limit = 1 }));
    var bad_cursor: [max_cursor_len]u8 = undefined;
    @memcpy(bad_cursor[0..cursor.len], cursor);
    bad_cursor[cursor.len - 1] = 'G';
    try std.testing.expectError(error.InvalidInitialFkRetirementPage, validateCursor(1, 13, bad_cursor[0..cursor.len]));

    const identity = @import("antfly_local_sources").storage_db_root_signing_identity;
    const seed: [32]u8 = @splat(7);
    const key_pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: identity.State = .{
        .root_incarnation = ticket.replica.store_root_incarnation,
        .seed = seed,
        .public_key = key_pair.public_key.toBytes(),
    };
    const intent: contract.Intent = .{ .ticket = ticket, .phase = .unlinked };
    const ack: AckRequest = .{ .signed = try auth.SignedReceipt.sign(root, try contract.Receipt.fromUnlinkedIntent(intent, 31)) };
    try ack.validate(1);
    try (AckResponse{ .ticket_digest = try ticket.digest(), .disposition = .newly_acked }).validate(ack);
    try (AckResponse{ .ticket_digest = try ticket.digest(), .disposition = .already_acked }).validate(ack);
    try std.testing.expectError(error.InvalidInitialFkRetirementAck, (AckResponse{ .ticket_digest = @splat(0), .disposition = .newly_acked }).validate(ack));
}
