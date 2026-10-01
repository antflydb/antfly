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

//! Administrator-approved identity for a physical store root. Ordinary store
//! registration advertises a key; it cannot grant that key retirement authority.
const std = @import("std");
const incarnation = @import("incarnation.zig");
const Ed25519 = std.crypto.sign.Ed25519;

pub const Identity = struct {
    metadata_incarnation: incarnation.MetadataClusterIncarnation,
    node_id: u64,
    store_id: u64,
    root_incarnation: u128,
    public_key: [32]u8,

    pub fn validate(self: @This()) !void {
        if (!incarnation.isValid(self.metadata_incarnation) or self.node_id == 0 or
            self.store_id == 0 or self.root_incarnation == 0 or std.mem.allEqual(u8, &self.public_key, 0))
            return error.InvalidStoreRootEnrollment;
        _ = Ed25519.PublicKey.fromBytes(self.public_key) catch return error.InvalidStoreRootEnrollment;
    }

    pub fn digest(self: @This()) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/store-root-enrollment/v1");
        hash.update(&self.metadata_incarnation);
        var numbers: [32]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.node_id, .little);
        std.mem.writeInt(u64, numbers[8..16], self.store_id, .little);
        std.mem.writeInt(u128, numbers[16..32], self.root_incarnation, .little);
        hash.update(&numbers);
        hash.update(&self.public_key);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};

pub const Request = struct {
    identity: Identity,
    /// Proof of possession is in addition to the administrator's body-bound
    /// grant. Neither possession nor the service credential grants enrollment.
    signature: [64]u8,

    pub fn validate(self: @This()) !void {
        const digest = try self.identity.digest();
        const verifier = Ed25519.PublicKey.fromBytes(self.identity.public_key) catch return error.InvalidStoreRootEnrollment;
        Ed25519.Signature.fromBytes(self.signature).verify(&digest, verifier) catch return error.InvalidStoreRootEnrollment;
    }

    pub fn sign(identity: Identity, seed: [32]u8) !@This() {
        const pair = try Ed25519.KeyPair.generateDeterministic(seed);
        if (!std.mem.eql(u8, &identity.public_key, &pair.public_key.toBytes())) return error.InvalidStoreRootEnrollment;
        const digest = try identity.digest();
        return .{ .identity = identity, .signature = (try pair.sign(&digest, null)).toBytes() };
    }
};

pub fn prefix(buf: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "metadata:{x:0>16}:store-root-enrollment:", .{group_id});
}

pub fn key(buf: []u8, group_id: u64, store_id: u64, root: u128) ![]const u8 {
    return std.fmt.bufPrint(buf, "metadata:{x:0>16}:store-root-enrollment:{x:0>16}:{x:0>32}", .{ group_id, store_id, root });
}

test "store root enrollment proof binds cluster node store root and verifier" {
    const seed: [32]u8 = @splat(7);
    const pair = try Ed25519.KeyPair.generateDeterministic(seed);
    const identity: Identity = .{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 3, .store_id = 5, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    const request = try Request.sign(identity, seed);
    try request.validate();
    var forged = request;
    forged.identity.node_id += 1;
    try std.testing.expectError(error.InvalidStoreRootEnrollment, forged.validate());
    forged = request;
    forged.identity.store_id += 1;
    try std.testing.expectError(error.InvalidStoreRootEnrollment, forged.validate());
    forged = request;
    forged.identity.root_incarnation += 1;
    try std.testing.expectError(error.InvalidStoreRootEnrollment, forged.validate());
    forged = request;
    forged.identity.metadata_incarnation = "ffffffffffffffffffffffffffffffff".*;
    try std.testing.expectError(error.InvalidStoreRootEnrollment, forged.validate());
}
