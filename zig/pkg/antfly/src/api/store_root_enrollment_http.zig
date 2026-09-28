// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Public, JSON-safe enrollment representation. The signed domain identity
//! uses u128 and fixed byte arrays; the API uses decimal/hex strings so JS
//! clients never silently round a root ID or reinterpret signature bytes.
const std = @import("std");
const enrollment = @import("../metadata/store_root_enrollment.zig");

pub const max_body_bytes: usize = 8192;

pub const Identity = struct {
    metadata_incarnation: [32]u8,
    node_id: u64,
    store_id: u64,
    root_incarnation: []const u8,
    public_key: []const u8,

    pub fn toDomain(self: @This()) !enrollment.Identity {
        if (self.node_id > std.math.maxInt(i64) or self.store_id > std.math.maxInt(i64) or
            self.public_key.len != 64 or !canonicalLowerHex(self.public_key) or
            self.root_incarnation.len == 0 or self.root_incarnation.len > 39 or
            self.root_incarnation[0] < '1' or self.root_incarnation[0] > '9')
            return error.InvalidStoreRootEnrollment;
        for (self.root_incarnation[1..]) |char| if (!std.ascii.isDigit(char))
            return error.InvalidStoreRootEnrollment;
        const root = std.fmt.parseInt(u128, self.root_incarnation, 10) catch
            return error.InvalidStoreRootEnrollment;
        var public_key: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&public_key, self.public_key) catch return error.InvalidStoreRootEnrollment;
        const identity: enrollment.Identity = .{
            .metadata_incarnation = self.metadata_incarnation,
            .node_id = self.node_id,
            .store_id = self.store_id,
            .root_incarnation = root,
            .public_key = public_key,
        };
        try identity.validate();
        return identity;
    }
};

pub const Request = struct {
    identity: Identity,
    signature: []const u8,

    pub fn toDomain(self: @This()) !enrollment.Request {
        if (self.signature.len != 128 or !canonicalLowerHex(self.signature))
            return error.InvalidStoreRootEnrollment;
        var signature: [64]u8 = undefined;
        _ = std.fmt.hexToBytes(&signature, self.signature) catch return error.InvalidStoreRootEnrollment;
        const request: enrollment.Request = .{
            .identity = try self.identity.toDomain(),
            .signature = signature,
        };
        try request.validate();
        return request;
    }
};

pub fn encodeAlloc(alloc: std.mem.Allocator, request: enrollment.Request) ![]u8 {
    try request.validate();
    const root = try std.fmt.allocPrint(alloc, "{d}", .{request.identity.root_incarnation});
    defer alloc.free(root);
    const key = std.fmt.bytesToHex(request.identity.public_key, .lower);
    const sig = std.fmt.bytesToHex(request.signature, .lower);
    return std.json.Stringify.valueAlloc(alloc, Request{
        .identity = .{
            .metadata_incarnation = request.identity.metadata_incarnation,
            .node_id = request.identity.node_id,
            .store_id = request.identity.store_id,
            .root_incarnation = root,
            .public_key = &key,
        },
        .signature = &sig,
    }, .{});
}

pub fn encodeIdentityAlloc(alloc: std.mem.Allocator, identity: enrollment.Identity) ![]u8 {
    try identity.validate();
    const root = try std.fmt.allocPrint(alloc, "{d}", .{identity.root_incarnation});
    defer alloc.free(root);
    const key = std.fmt.bytesToHex(identity.public_key, .lower);
    return std.json.Stringify.valueAlloc(alloc, Identity{
        .metadata_incarnation = identity.metadata_incarnation,
        .node_id = identity.node_id,
        .store_id = identity.store_id,
        .root_incarnation = root,
        .public_key = &key,
    }, .{});
}

fn canonicalLowerHex(raw: []const u8) bool {
    for (raw) |char| if (!std.ascii.isDigit(char) and !(char >= 'a' and char <= 'f')) return false;
    return true;
}

test "public enrollment encoding preserves a signed u128 root without JSON rounding" {
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const domain = try enrollment.Request.sign(.{
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .node_id = 3,
        .store_id = 5,
        .root_incarnation = std.math.maxInt(u128),
        .public_key = pair.public_key.toBytes(),
    }, seed);
    const encoded = try encodeAlloc(std.testing.allocator, domain);
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(std.mem.indexOf(u8, encoded, "\"metadata_incarnation\":\"0123456789abcdef0123456789abcdef\"") != null);
    const status_json = try encodeIdentityAlloc(std.testing.allocator, domain.identity);
    defer std.testing.allocator.free(status_json);
    var status = try std.json.parseFromSlice(Identity, std.testing.allocator, status_json, .{ .ignore_unknown_fields = false });
    defer status.deinit();
    try std.testing.expectEqual(domain.identity, try status.value.toDomain());
    var parsed = try std.json.parseFromSlice(Request, std.testing.allocator, encoded, .{ .ignore_unknown_fields = false });
    defer parsed.deinit();
    try std.testing.expectEqual(domain, try parsed.value.toDomain());
    var invalid = parsed.value;
    invalid.identity.root_incarnation = "0001";
    try std.testing.expectError(error.InvalidStoreRootEnrollment, invalid.toDomain());
    invalid.identity.root_incarnation = "+1";
    try std.testing.expectError(error.InvalidStoreRootEnrollment, invalid.toDomain());
    invalid.identity.root_incarnation = "1_0";
    try std.testing.expectError(error.InvalidStoreRootEnrollment, invalid.toDomain());
    invalid = parsed.value;
    invalid.identity.node_id = std.math.maxInt(u64);
    try std.testing.expectError(error.InvalidStoreRootEnrollment, invalid.toDomain());
}
