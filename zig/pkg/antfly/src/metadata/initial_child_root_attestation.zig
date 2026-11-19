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

//! Physical-owner attestation is separate from the replicated logical receipt:
//! every replica applies the same logical command, but only its own enrolled
//! root can attest the local replica generation that produced a response.
const std = @import("std");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;

pub const Attestation = @import("owner_root_attestation.zig").Proof("antfly/initial-child-root-receipt/v1");

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
    // Independent transcript of the shipped initial-child format: sharing the
    // signer must not change signatures already persisted by this feature.
    var original = std.crypto.hash.Blake3.init(.{});
    original.update("antfly/initial-child-root-receipt/v1");
    original.update(&try proof.identity.digest());
    original.update(&@as([32]u8, @splat(8)));
    var fields: [24]u8 = undefined;
    std.mem.writeInt(u64, fields[0..8], 4, .little);
    std.mem.writeInt(u64, fields[8..16], 5, .little);
    std.mem.writeInt(u64, fields[16..24], 6, .little);
    original.update(&fields);
    var original_digest: [32]u8 = undefined;
    original.final(&original_digest);
    try std.testing.expectEqualSlices(u8, &try root.sign(&original_digest), &proof.signature);
    try std.testing.expectError(error.InvalidGenerationPublication, proof.verify(@splat(10)));
    var replaced = proof;
    replaced.root_generation += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, replaced.verify(@splat(8)));
}
