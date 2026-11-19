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

//! Encoding-independent reference-set evidence shared by validation owners.
const std = @import("std");
const contract = @import("relational_integrity_contract.zig");

/// Count plus addition modulo 2^256 of domain-separated key hashes. Unlike
/// XOR, duplicate pairs do not disappear. Source and parent owners may combine
/// disjoint ranges in any order. Full cohort/root/epoch verification is separate.
pub const Witness = struct {
    count: u64 = 0,
    sum: [32]u8 = @splat(0),

    /// New witness bytes are explicitly binary-safe. Existing owner digests
    /// retain their established encoding and signed receipt identities.
    pub fn jsonStringify(self: @This(), writer: anytype) @TypeOf(writer.*).Error!void {
        try writer.beginObject();
        try writer.objectField("count");
        try writer.write(self.count);
        try writer.objectField("sum");
        try writer.beginArray();
        for (self.sum) |byte| try writer.write(byte);
        try writer.endArray();
        try writer.endObject();
    }

    pub fn identity(key: []const u8) ![32]u8 {
        if ((try contract.parseKey(key)).kind != .reference) return error.InvalidSchemaValidationJob;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/fk-validation-reference-set/v1");
        state.update(key);
        var result: [32]u8 = undefined;
        state.final(&result);
        return result;
    }
    pub fn validate(self: Witness) !void {
        if (self.count == 0 and !std.mem.allEqual(u8, &self.sum, 0)) return error.InvalidSchemaValidationJob;
    }
    pub fn combine(self: *Witness, other: Witness) !void {
        try self.validate();
        try other.validate();
        const count = std.math.add(u64, self.count, other.count) catch return error.InvalidSchemaValidationJob;
        const sum = std.mem.readInt(u256, &self.sum, .little) +% std.mem.readInt(u256, &other.sum, .little);
        std.mem.writeInt(u256, &self.sum, sum, .little);
        self.count = count;
    }
    pub fn addIdentity(self: *Witness, identity_hash: [32]u8) !void {
        try self.combine(.{ .count = 1, .sum = identity_hash });
    }
    pub fn addReference(self: *Witness, key: []const u8) !void {
        try self.addIdentity(try identity(key));
    }
    pub fn eql(self: Witness, other: Witness) bool {
        return self.count == other.count and std.mem.eql(u8, &self.sum, &other.sum);
    }
};
