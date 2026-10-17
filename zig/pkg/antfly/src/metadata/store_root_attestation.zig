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

//! Domain-separated root possession proofs shared by coordinated owners.
//! A signature alone grants no enrollment, ownership or publication permission.
const std = @import("std");
const enrollment = @import("store_root_enrollment.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;

pub const Authority = union(enum) {
    raft: u64,
    native: [32]u8,
};

pub fn ProofFor(comptime message_domain: []const u8, comptime accepted_domain: []const u8) type {
    comptime {
        if (message_domain.len == 0 or accepted_domain.len == 0 or std.mem.eql(u8, message_domain, accepted_domain))
            @compileError("root attestation requires distinct nonempty protocol domains");
    }
    return struct {
        const Proof = @This();
        identity: enrollment.Identity,
        reporter_incarnation: u64,
        authority: Authority,
        root_generation: u64,
        signature: [64]u8,

        fn message(self: Proof, logical_digest: [32]u8) ![32]u8 {
            if (self.reporter_incarnation == 0 or self.root_generation == 0)
                return error.InvalidGenerationPublication;
            var hash = std.crypto.hash.Blake3.init(.{});
            hash.update(message_domain);
            hash.update(&(try self.identity.digest()));
            hash.update(&logical_digest);
            var bytes: [8]u8 = undefined;
            for ([_]u64{ self.reporter_incarnation, self.root_generation }) |value| {
                std.mem.writeInt(u64, &bytes, value, .little);
                hash.update(&bytes);
            }
            switch (self.authority) {
                .raft => |replica_id| {
                    if (replica_id == 0) return error.InvalidGenerationPublication;
                    hash.update("raft");
                    std.mem.writeInt(u64, &bytes, replica_id, .little);
                    hash.update(&bytes);
                },
                .native => |binding_digest| {
                    if (std.mem.allEqual(u8, &binding_digest, 0)) return error.InvalidGenerationPublication;
                    hash.update("native");
                    hash.update(&binding_digest);
                },
            }
            var digest: [32]u8 = undefined;
            hash.final(&digest);
            return digest;
        }

        pub fn sign(self: Proof, root: signing.State, logical_digest: [32]u8) !Proof {
            if (root.root_incarnation != self.identity.root_incarnation or
                !std.mem.eql(u8, &root.public_key, &self.identity.public_key)) return error.InvalidGenerationPublication;
            var result = self;
            result.signature = try root.sign(&(try self.message(logical_digest)));
            return result;
        }

        pub fn verify(self: Proof, logical_digest: [32]u8) !void {
            signing.verify(self.identity.public_key, &(try self.message(logical_digest)), self.signature) catch
                return error.InvalidGenerationPublication;
        }

        /// Commit the physical signer/root as well as the logical receipt. This
        /// deliberately differs for re-attestation by another current reporter.
        pub fn commitment(self: Proof, logical_digest: [32]u8) ![32]u8 {
            var hash = std.crypto.hash.Blake3.init(.{});
            hash.update(accepted_domain);
            hash.update(&(try self.message(logical_digest)));
            hash.update(&self.signature);
            var result: [32]u8 = undefined;
            hash.final(&result);
            return result;
        }
    };
}

/// Immutable facts returned by a permission read, not enrollment or a lease.
/// Metadata must independently revalidate the registration at receipt apply.
pub const NativeView = struct {
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    binding_digest: [32]u8,

    pub fn validate(self: @This(), metadata_incarnation: @import("antfly_local_sources").metadata_incarnation.MetadataClusterIncarnation) !void {
        self.identity.validate() catch return error.InvalidCatalogRecord;
        if (self.reporter_incarnation == 0 or !std.mem.eql(u8, &metadata_incarnation, &self.identity.metadata_incarnation))
            return error.InvalidCatalogRecord;
        const binding: @import("standalone_native_owner.zig").Binding = .{
            .metadata_incarnation = self.identity.metadata_incarnation,
            .node_id = self.identity.node_id,
            .store_id = self.identity.store_id,
            .root_incarnation = self.identity.root_incarnation,
        };
        if (!std.mem.eql(u8, &try binding.digest(), &self.binding_digest)) return error.InvalidCatalogRecord;
    }
};

test "store root attestation preserves exact index protocol bytes for native and Raft roots" {
    const index = ProofFor("antfly/index-build-owner-receipt/v2", "antfly/index-owner-accepted-root/v1");
    const seed: [32]u8 = @splat(7);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: signing.State = .{ .root_incarnation = 9, .seed = seed, .public_key = pair.public_key.toBytes() };
    const identity: enrollment.Identity = .{
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .node_id = 2,
        .store_id = 3,
        .root_incarnation = root.root_incarnation,
        .public_key = root.public_key,
    };
    const logical: [32]u8 = @splat(11);
    for ([_]Authority{ .{ .native = @splat(13) }, .{ .raft = 17 } }) |authority| {
        const template: index = .{ .identity = identity, .reporter_incarnation = 5, .authority = authority, .root_generation = 6, .signature = undefined };
        const signed = try template.sign(root, logical);
        try signed.verify(logical);
        // Independent legacy preimage, including original field order,
        // integer endianness and authority tags. No re-encoded JSON is used.
        var legacy = std.crypto.hash.Blake3.init(.{});
        legacy.update("antfly/index-build-owner-receipt/v2");
        legacy.update(&try identity.digest());
        legacy.update(&logical);
        var numbers: [16]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], 5, .little);
        std.mem.writeInt(u64, numbers[8..16], 6, .little);
        legacy.update(&numbers);
        switch (authority) {
            .native => |binding| {
                legacy.update("native");
                legacy.update(&binding);
            },
            .raft => |replica| {
                legacy.update("raft");
                std.mem.writeInt(u64, numbers[0..8], replica, .little);
                legacy.update(numbers[0..8]);
            },
        }
        var message: [32]u8 = undefined;
        legacy.final(&message);
        try std.testing.expectEqualSlices(u8, &try root.sign(&message), &signed.signature);
        var accepted = std.crypto.hash.Blake3.init(.{});
        accepted.update("antfly/index-owner-accepted-root/v1");
        accepted.update(&message);
        accepted.update(&signed.signature);
        var commitment: [32]u8 = undefined;
        accepted.final(&commitment);
        try std.testing.expectEqualSlices(u8, &commitment, &try signed.commitment(logical));
    }
}
