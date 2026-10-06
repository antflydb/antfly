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

//! Retirement identity shared by coordination and standalone runtime owners.
//! Keep this contract free of runtime implementation imports.

const std = @import("std");

/// Immutable metadata identity carried by a crash-safe local retirement
/// intent. The owning metadata publication, not this sidecar, decides whether
/// cancellation committed; the cold physical AICH record must match it too.
pub const InitialFkRetirementProof = struct {
    child_table_id: u64,
    plan_id: [16]u8,
    plan_digest: [32]u8,

    pub const encoded_len = 8 + 16 + 32;

    pub fn validate(self: @This()) !void {
        if (self.child_table_id == 0 or std.mem.allEqual(u8, &self.plan_id, 0) or
            std.mem.allEqual(u8, &self.plan_digest, 0)) return error.InvalidReplicaRetirementIntent;
    }

    pub fn encode(self: @This()) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], self.child_table_id, .little);
        @memcpy(bytes[8..24], &self.plan_id);
        @memcpy(bytes[24..56], &self.plan_digest);
        return bytes;
    }

    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != encoded_len) return error.InvalidReplicaRetirementIntent;
        const result: @This() = .{
            .child_table_id = std.mem.readInt(u64, bytes[0..8], .little),
            .plan_id = bytes[8..24].*,
            .plan_digest = bytes[24..56].*,
        };
        try result.validate();
        return result;
    }
};
