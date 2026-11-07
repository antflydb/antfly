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

//! Immutable authority for externally decided two-phase transactions. This
//! binds an existing native coordinator to one metadata admission; it neither
//! commits a transaction nor permits a caller to invent a prepared GID.
const std = @import("std");

pub const External = struct {
    metadata_incarnation: [32]u8,
    operation_id: [16]u8,
    admission_digest: [32]u8,
    /// Bounds admission of a NEW native vote, never the prepared lifetime.
    expires_at_unix_ns: u64,

    pub const encoded_len = 88;
    pub const max_preparation_ns = 5 * std.time.ns_per_min;
    pub const clock_skew_allowance_ns = std.time.ns_per_min;

    pub fn validate(self: External) !void {
        if (self.expires_at_unix_ns == 0 or self.expires_at_unix_ns == std.math.maxInt(u64) or
            std.mem.allEqual(u8, &self.metadata_incarnation, '0') or
            std.mem.allEqual(u8, &self.operation_id, 0) or
            std.mem.allEqual(u8, &self.admission_digest, 0)) return error.InvalidExternalDecisionOwner;
        for (self.metadata_incarnation) |byte| if (!std.ascii.isDigit(byte) and (byte < 'a' or byte > 'f')) return error.InvalidExternalDecisionOwner;
    }

    pub fn requireFreshVoteAt(self: External, admitted_at_unix_ns: u64) !void {
        try self.validate();
        if (admitted_at_unix_ns == 0 or admitted_at_unix_ns >= self.expires_at_unix_ns) return error.ExternalTransactionAdmissionExpired;
        if (self.expires_at_unix_ns - admitted_at_unix_ns > max_preparation_ns + clock_skew_allowance_ns) return error.InvalidExternalDecisionOwner;
    }

    pub fn eql(self: External, other: External) bool {
        return self.expires_at_unix_ns == other.expires_at_unix_ns and
            std.mem.eql(u8, &self.metadata_incarnation, &other.metadata_incarnation) and
            std.mem.eql(u8, &self.operation_id, &other.operation_id) and
            std.mem.eql(u8, &self.admission_digest, &other.admission_digest);
    }

    pub fn encode(self: External) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = undefined;
        @memcpy(bytes[0..32], &self.metadata_incarnation);
        @memcpy(bytes[32..48], &self.operation_id);
        @memcpy(bytes[48..80], &self.admission_digest);
        std.mem.writeInt(u64, bytes[80..88], self.expires_at_unix_ns, .little);
        return bytes;
    }

    pub fn decode(bytes: []const u8) !External {
        if (bytes.len != encoded_len) return error.InvalidExternalDecisionOwner;
        const result: External = .{ .metadata_incarnation = bytes[0..32].*, .operation_id = bytes[32..48].*, .admission_digest = bytes[48..80].*, .expires_at_unix_ns = std.mem.readInt(u64, bytes[80..88], .little) };
        try result.validate();
        return result;
    }
};

test "external transaction decision authority binds incarnation admission and exact native bytes" {
    const owner: External = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    const encoded = try owner.encode();
    try std.testing.expect(owner.eql(try External.decode(&encoded)));
    var rebound = owner;
    rebound.operation_id[0] ^= 1;
    try std.testing.expect(!owner.eql(rebound));
    try std.testing.expectError(error.InvalidExternalDecisionOwner, External.decode(encoded[0..79]));
    rebound.metadata_incarnation[0] = 'G';
    try std.testing.expectError(error.InvalidExternalDecisionOwner, rebound.encode());
    rebound = owner;
    rebound.admission_digest = @splat(0);
    try std.testing.expectError(error.InvalidExternalDecisionOwner, rebound.encode());
}

test "external transaction vote deadline never expires an existing decision owner" {
    const owner: External = .{ .metadata_incarnation = @splat('1'), .operation_id = @splat(2), .admission_digest = @splat(3), .expires_at_unix_ns = 1000 };
    try owner.requireFreshVoteAt(999);
    try std.testing.expectError(error.ExternalTransactionAdmissionExpired, owner.requireFreshVoteAt(1000));
    // Resolution uses validate/eql/encode, not new-vote wall-clock admission.
    try owner.validate();
    try std.testing.expect(owner.eql(try External.decode(&try owner.encode())));
}
