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
    schema_reader: @import("table_manager.zig").SchemaReaderCapabilities = .{},
    artifact_sources_protocol_version: u16 = 0,
    native_generation_restore_version: u16 = 0,
    dense_native_storage_protocol_version: u16 = 0,
    relational_topology_protocol_version: u16 = 0,

    pub fn binding(self: Registration) Binding {
        return .{ .metadata_incarnation = self.identity.metadata_incarnation, .node_id = self.identity.node_id, .store_id = self.identity.store_id, .root_incarnation = self.identity.root_incarnation };
    }

    pub fn message(self: Registration) ![32]u8 {
        if (self.reporter_incarnation == 0) return error.InvalidStandaloneNativeOwner;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/standalone-native-reporter/v2");
        hash.update(&try self.identity.digest());
        var profile: [16]u8 = undefined;
        std.mem.writeInt(u32, profile[0..4], self.schema_reader.semantic_ceiling, .little);
        std.mem.writeInt(u32, profile[4..8], self.schema_reader.encoding_ceiling, .little);
        std.mem.writeInt(u16, profile[8..10], self.artifact_sources_protocol_version, .little);
        std.mem.writeInt(u16, profile[10..12], self.native_generation_restore_version, .little);
        std.mem.writeInt(u16, profile[12..14], self.dense_native_storage_protocol_version, .little);
        std.mem.writeInt(u16, profile[14..16], self.relational_topology_protocol_version, .little);
        hash.update(&profile);
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
        if (!self.schema_reader.validForReporter(self.reporter_incarnation)) return error.InvalidStandaloneNativeOwner;
        signing.verify(self.identity.public_key, &try self.message(), self.signature) catch return error.InvalidStandaloneNativeOwner;
    }
};

/// Read-only restart evidence, not a registration grant or a GC instruction.
/// Every field belongs to one metadata snapshot. Admission must recheck the
/// signed preimage and its own decoder/physical-owner lease requirements.
pub const RestartObservation = struct {
    revision: u64,
    request_digest: [32]u8,
    header_digest: ?[32]u8,
    baseline_digest: [32]u8,
    reporter_matches: bool,
    root_enrolled: bool,
    replication_pending: bool,
    active: @import("store_report_baseline.zig").Generation,
    pending: ?@import("store_report_update.zig").Cursor,
    retired: ?@import("store_report_baseline.zig").Generation,
    gc_page: usize,
    gc_fragment: u32,
    gc_fragment_end: u32,
};

/// Startup commands are body-bound to the original signed reporter request.
/// The expected observation admits retirement; subsequent steps CAS the exact
/// durable cursor. Neither form enrolls the signing key or grants a schema ACK.
pub const Restart = struct {
    request: Registration,
    expected: ?RestartObservation = null,
    progress: ?[32]u8 = null,
};

pub const RestartProgress = struct {
    request: Registration,
    digest: [32]u8,
};

/// Share the cursor projection so snapshots and standby deltas retain the
/// startup cursor without inventing a second metadata transport protocol.
pub fn restartKey(buf: []u8, group_id: u64, store_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:store_report_cursor_v1:{d}:native_restart:{d}", .{ group_id, store_id });
}

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
