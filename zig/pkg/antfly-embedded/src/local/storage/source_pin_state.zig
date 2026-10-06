// Copyright 2026 Antfly, Inc.
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

//! Durable, namespace-bound gap fence between source admission and local pin
//! publication. This is not a transfer fence: writes resume once the immutable
//! primary checkpoint is durable. A retry must never advance a blocked entry.
const std = @import("std");
pub const key = "\x00\x00__metadata__:source_pin_prepared";
const scope_size = @import("db/online_source_contract.zig").scope_encoded_size;
const payload_size = 76 + scope_size;
pub const encoded_size = payload_size + 32;
pub const Prepared = struct {
    namespace: [24]u8,
    pin: [32]u8,
    applied_index: u64,
    retained_start: u64,
    scope_bytes: [scope_size]u8 = @splat(0),

    pub fn encode(self: Prepared) [encoded_size]u8 {
        var result: [encoded_size]u8 = undefined;
        @memcpy(result[0..4], "ASP2");
        @memcpy(result[4..28], &self.namespace);
        @memcpy(result[28..60], &self.pin);
        std.mem.writeInt(u64, result[60..68], self.applied_index, .little);
        std.mem.writeInt(u64, result[68..76], self.retained_start, .little);
        @memcpy(result[76..payload_size], &self.scope_bytes);
        std.crypto.hash.sha2.Sha256.hash(result[0..payload_size], result[payload_size..encoded_size], .{});
        return result;
    }
    pub fn decode(bytes: []const u8) !Prepared {
        if (bytes.len != encoded_size or !std.mem.eql(u8, bytes[0..4], "ASP2")) return error.OnlineSourceCorrupt;
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes[0..payload_size], &digest, .{});
        if (!std.mem.eql(u8, bytes[payload_size..encoded_size], &digest)) return error.OnlineSourceCorrupt;
        const result: Prepared = .{ .namespace = bytes[4..28].*, .pin = bytes[28..60].*, .applied_index = std.mem.readInt(u64, bytes[60..68], .little), .retained_start = std.mem.readInt(u64, bytes[68..76], .little), .scope_bytes = bytes[76..payload_size].* };
        if (result.applied_index == 0 or std.mem.allEqual(u8, &result.namespace, 0) or std.mem.allEqual(u8, &result.pin, 0)) return error.OnlineSourceCorrupt;
        return result;
    }
};
pub fn load(txn: anytype) !?Prepared {
    const bytes = txn.get(key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try Prepared.decode(bytes);
}
pub fn requireNoPrepared(txn: anytype, namespace: [24]u8) !void {
    if (try load(txn)) |prepared| if (std.mem.eql(u8, &prepared.namespace, &namespace)) return error.OnlineSourcePinPending;
}

test "source pin prepared fence rejects corruption and preserves exact clocks" {
    const value: Prepared = .{ .namespace = @splat(1), .pin = @splat(2), .applied_index = 7, .retained_start = 11 };
    var encoded = value.encode();
    try std.testing.expectEqualDeep(value, try Prepared.decode(&encoded));
    encoded[68] ^= 1;
    try std.testing.expectError(error.OnlineSourceCorrupt, Prepared.decode(&encoded));
}
