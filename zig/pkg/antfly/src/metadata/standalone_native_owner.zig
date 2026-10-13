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
const enrollment = @import("store_root_enrollment.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;

/// Process reporter admission, not root enrollment. Only startup under the
/// physical owner's exclusive lease may issue this request. A steady-state
/// status publisher must never reread a new preimage and steal registration.
pub const Registration = struct {
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    expected_reporter_incarnation: ?u64,
    signature: [64]u8,

    pub fn binding(self: Registration) Binding {
        return .{ .metadata_incarnation = self.identity.metadata_incarnation, .node_id = self.identity.node_id, .store_id = self.identity.store_id, .root_incarnation = self.identity.root_incarnation };
    }

    fn message(self: Registration) ![32]u8 {
        if (self.reporter_incarnation == 0) return error.InvalidStandaloneNativeOwner;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/standalone-native-reporter/v1");
        hash.update(&try self.identity.digest());
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, self.reporter_incarnation, .little);
        hash.update(&bytes);
        hash.update(&.{@intFromBool(self.expected_reporter_incarnation != null)});
        if (self.expected_reporter_incarnation) |expected| {
            std.mem.writeInt(u64, &bytes, expected, .little);
            hash.update(&bytes);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn sign(self: Registration, root: signing.State) !Registration {
        if (root.root_incarnation != self.identity.root_incarnation or
            !std.mem.eql(u8, &root.public_key, &self.identity.public_key)) return error.StoreRootEnrollmentChanged;
        var result = self;
        result.signature = try root.sign(&try self.message());
        return result;
    }

    pub fn validate(self: Registration) !void {
        signing.verify(self.identity.public_key, &try self.message(), self.signature) catch return error.InvalidStandaloneNativeOwner;
    }
};

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

    pub fn digest(self: Binding) ![32]u8 {
        const bytes = try encode(self);
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/standalone-native-owner/v1");
        hash.update(&bytes);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
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
    try std.testing.expectEqualSlices(u8, &try value.digest(), &try (try decode(&bytes)).digest());
    inline for (.{ "metadata_incarnation", "node_id", "store_id", "root_incarnation" }) |field| {
        var changed = value;
        if (comptime std.mem.eql(u8, field, "metadata_incarnation")) {
            changed.metadata_incarnation[0] = 'f';
        } else @field(changed, field) += 1;
        try std.testing.expect(!std.mem.eql(u8, &try value.digest(), &try changed.digest()));
    }
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, decode(bytes[0 .. bytes.len - 1]));
    var bad = bytes;
    bad[0] = 0;
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, decode(&bad));
}

test "relational index publication native reporter registration binds identity and CAS preimage" {
    const seed: [32]u8 = @splat(9);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: signing.State = .{ .seed = seed, .public_key = pair.public_key.toBytes(), .root_incarnation = 7 };
    const request = try (Registration{ .identity = .{ .metadata_incarnation = @splat('1'), .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key }, .reporter_incarnation = 4, .expected_reporter_incarnation = null, .signature = undefined }).sign(root);
    try request.validate();
    inline for (.{ "node_id", "store_id", "root_incarnation" }) |field| {
        var changed = request;
        @field(changed.identity, field) += 1;
        try std.testing.expectError(error.InvalidStandaloneNativeOwner, changed.validate());
    }
    var changed = request;
    changed.identity.metadata_incarnation[0] = '2';
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, changed.validate());
    changed = request;
    changed.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, changed.validate());
    changed = request;
    changed.expected_reporter_incarnation = 0;
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, changed.validate());
    const present_zero = try changed.sign(root);
    try present_zero.validate();
    changed = present_zero;
    changed.expected_reporter_incarnation = null;
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, changed.validate());
    changed = present_zero;
    changed.expected_reporter_incarnation = 5;
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, changed.validate());
    changed = request;
    changed.reporter_incarnation = 0;
    try std.testing.expectError(error.InvalidStandaloneNativeOwner, changed.sign(root));
}
