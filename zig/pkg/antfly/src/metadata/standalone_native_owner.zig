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

//! One physical owner for a native standalone metadata catalog. This is not
//! a Raft placement and must never be inferred from a public table listing.
const std = @import("std");
const incarnation = @import("antfly_local_sources").metadata_incarnation;

pub const Binding = struct {
    metadata_incarnation: incarnation.MetadataClusterIncarnation,
    node_id: u64,
    store_id: u64,
    root_incarnation: u128,

    pub fn validate(self: Binding) !void {
        if (!incarnation.isValid(self.metadata_incarnation) or self.node_id == 0 or
            self.store_id == 0 or self.root_incarnation == 0) return error.InvalidStandaloneNativeOwner;
    }

    pub fn eql(self: Binding, other: Binding) bool {
        return std.meta.eql(self, other);
    }
};

const magic = "AFNOWN1\x00";
pub const encoded_len = magic.len + 32 + 8 + 8 + 16;

pub fn encode(value: Binding) ![encoded_len]u8 {
    try value.validate();
    var bytes: [encoded_len]u8 = undefined;
    @memcpy(bytes[0..magic.len], magic);
    @memcpy(bytes[magic.len..][0..32], &value.metadata_incarnation);
    std.mem.writeInt(u64, bytes[magic.len + 32 ..][0..8], value.node_id, .little);
    std.mem.writeInt(u64, bytes[magic.len + 40 ..][0..8], value.store_id, .little);
    std.mem.writeInt(u128, bytes[magic.len + 48 ..][0..16], value.root_incarnation, .little);
    return bytes;
}

pub fn decode(bytes: []const u8) !Binding {
    if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..magic.len], magic))
        return error.InvalidStandaloneNativeOwner;
    const value: Binding = .{
        .metadata_incarnation = bytes[magic.len..][0..32].*,
        .node_id = std.mem.readInt(u64, bytes[magic.len + 32 ..][0..8], .little),
        .store_id = std.mem.readInt(u64, bytes[magic.len + 40 ..][0..8], .little),
        .root_incarnation = std.mem.readInt(u128, bytes[magic.len + 48 ..][0..16], .little),
    };
    try value.validate();
    return value;
}

pub fn key(buf: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:standalone_native_owner:{d}", .{group_id});
}

test "native owner binding is canonical and rejects malformed roots" {
    const value: Binding = .{
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .node_id = 4,
        .store_id = 9,
        .root_incarnation = 77,
    };
    const bytes = try encode(value);
    try std.testing.expect(value.eql(try decode(&bytes)));
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, decode(bytes[0 .. bytes.len - 1]));
    var bad = bytes;
    bad[0] = 0;
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, decode(&bad));
}
