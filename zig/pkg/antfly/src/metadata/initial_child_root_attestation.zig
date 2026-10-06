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

//! Physical-owner attestation is separate from the replicated logical receipt:
//! every replica applies the same logical command, but only its own enrolled
//! root can attest the local replica generation that produced a response.
const std = @import("std");
const enrollment = @import("store_root_enrollment.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;

pub const Attestation = struct {
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    replica_id: u64,
    root_generation: u64,
    signature: [64]u8,

    fn message(self: @This(), logical_digest: [32]u8) ![32]u8 {
        if (self.reporter_incarnation == 0 or self.replica_id == 0 or self.root_generation == 0)
            return error.InvalidGenerationPublication;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/initial-child-root-receipt/v1");
        hash.update(&(try self.identity.digest()));
        hash.update(&logical_digest);
        var bytes: [8]u8 = undefined;
        for ([_]u64{ self.reporter_incarnation, self.replica_id, self.root_generation }) |value| {
            std.mem.writeInt(u64, &bytes, value, .little);
            hash.update(&bytes);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn sign(self: @This(), root: signing.State, logical_digest: [32]u8) !@This() {
        if (root.root_incarnation != self.identity.root_incarnation or
            !std.mem.eql(u8, &root.public_key, &self.identity.public_key)) return error.InvalidGenerationPublication;
        var result = self;
        result.signature = try root.sign(&(try self.message(logical_digest)));
        return result;
    }

    pub fn verify(self: @This(), logical_digest: [32]u8) !void {
        signing.verify(self.identity.public_key, &(try self.message(logical_digest)), self.signature) catch
            return error.InvalidGenerationPublication;
    }
};

test "initial child physical attestation binds logical receipt and replica generation" {
    const seed: [32]u8 = @splat(9);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: signing.State = .{ .seed = seed, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    const proof = try (Attestation{
        .identity = .{ .metadata_incarnation = @splat('1'), .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key },
        .reporter_incarnation = 4,
        .replica_id = 5,
        .root_generation = 6,
        .signature = undefined,
    }).sign(root, @splat(8));
    try proof.verify(@splat(8));
    try std.testing.expectError(error.InvalidGenerationPublication, proof.verify(@splat(10)));
    var replaced = proof;
    replaced.root_generation += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, replaced.verify(@splat(8)));
}
