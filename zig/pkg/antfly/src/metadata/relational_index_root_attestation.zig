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

//! An index receipt names its actual physical authority. Native ownership is
//! a durable catalog binding, never a fabricated Raft replica. Possession alone
//! grants neither enrollment nor ownership: apply verifies both independently.
const std = @import("std");
const enrollment = @import("store_root_enrollment.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;

pub const Authority = union(enum) {
    raft: u64,
    native: [32]u8,
};

pub const Proof = struct {
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    authority: Authority,
    root_generation: u64,
    signature: [64]u8,

    fn message(self: Proof, logical_digest: [32]u8) ![32]u8 {
        if (self.reporter_incarnation == 0 or self.root_generation == 0)
            return error.InvalidGenerationPublication;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/index-build-owner-receipt/v2");
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
        hash.update("antfly/index-owner-accepted-root/v1");
        hash.update(&(try self.message(logical_digest)));
        hash.update(&self.signature);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};
