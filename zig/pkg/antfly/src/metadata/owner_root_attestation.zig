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

//! Root possession is not enrollment or placement authority. Metadata callers
//! must independently verify the current enrolled root, reporter and replica.
const std = @import("std");
const enrollment = @import("store_root_enrollment.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;

/// Separate signature domains prevent one lifecycle operation from borrowing
/// another operation's physical-root receipt. The domain is never wire input.
pub fn Proof(comptime domain: []const u8) type {
    return struct {
        identity: enrollment.Identity,
        reporter_incarnation: u64,
        replica_id: u64,
        root_generation: u64,
        signature: [64]u8,

        fn message(self: @This(), logical_digest: [32]u8) ![32]u8 {
            if (self.reporter_incarnation == 0 or self.replica_id == 0 or self.root_generation == 0)
                return error.InvalidGenerationPublication;
            var hash = std.crypto.hash.Blake3.init(.{});
            hash.update(domain);
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
}
