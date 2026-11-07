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

//! Physical resource identity shared by native lock admission and ordinary
//! mutation checks. SQL names, mutable primary-key values and schema versions
//! cannot identify a lock. The accepting data owner derives these digests.
const std = @import("std");
const ledger = @import("pessimistic_lock_ledger.zig");
const Identity = @import("db/doc_identity_namespace.zig").Namespace;

pub const binding_key = "\x00\x00__pessimistic_lock_binding_v1";
pub const Binding = struct {
    namespace: ledger.Namespace,
    identity: Identity,
    pub const encoded_len = 52;

    pub fn validate(self: Binding) !void {
        try self.namespace.validate();
        if (self.namespace.home != .data or self.identity.table_id == 0) return error.InvalidLockResource;
    }
    pub fn requireIdentity(self: Binding, receiving: Identity) !void {
        try self.validate();
        if (!self.identity.eql(receiving)) return error.IdentityNamespaceMismatch;
    }
    pub fn eql(self: Binding, other: Binding) bool {
        return std.meta.eql(self.namespace, other.namespace) and self.identity.eql(other.identity);
    }
    pub fn encode(self: Binding) ![encoded_len]u8 {
        try self.validate();
        var result: [encoded_len]u8 = undefined;
        @memcpy(result[0..4], "PLB1");
        @memcpy(result[4..20], &self.namespace.incarnation);
        std.mem.writeInt(u64, result[20..28], self.namespace.group, .little);
        std.mem.writeInt(u64, result[28..36], self.identity.table_id, .little);
        std.mem.writeInt(u64, result[36..44], self.identity.shard_id, .little);
        std.mem.writeInt(u64, result[44..52], self.identity.range_id, .little);
        return result;
    }
    pub fn decode(bytes: []const u8) !Binding {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "PLB1")) return error.InvalidLockRecord;
        const result: Binding = .{
            .namespace = .{ .home = .data, .incarnation = bytes[4..20].*, .group = std.mem.readInt(u64, bytes[20..28], .little) },
            .identity = .{
                .table_id = std.mem.readInt(u64, bytes[28..36], .little),
                .shard_id = std.mem.readInt(u64, bytes[36..44], .little),
                .range_id = std.mem.readInt(u64, bytes[44..52], .little),
            },
        };
        result.validate() catch return error.InvalidLockRecord;
        return result;
    }
    pub fn load(txn: anytype) !?Binding {
        const bytes = txn.get(binding_key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        return try decode(bytes);
    }
    /// The caller independently proves participant admission and native route
    /// ownership before installation. This helper grants no execution rights.
    /// A different home cannot silently abandon grants in an earlier journal.
    pub fn install(self: Binding, txn: anytype, receiving: Identity) !void {
        try self.requireIdentity(receiving);
        if (try load(txn)) |old| {
            if (old.eql(self)) return;
            // Topology and catalog replacement require every old participant
            // (including prepared grants) to retire before rebinding.
            if (try @import("pessimistic_lock_authority.zig").activeCount(txn, old.namespace) != 0) return error.LockOwnerChanged;
        }
        try txn.put(binding_key, &try self.encode());
    }
    pub fn table(self: Binding) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/pessimistic-table/v1");
        hash.update(&self.namespace.incarnation);
        var id: [8]u8 = undefined;
        std.mem.writeInt(u64, &id, self.identity.table_id, .little);
        hash.update(&id);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    pub fn row(self: Binding, physical_key: []const u8) ![32]u8 {
        if (physical_key.len == 0 or std.mem.startsWith(u8, physical_key, "\x00\x00")) return error.InvalidLockResource;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/pessimistic-row/v1");
        hash.update(&try self.table());
        var length: [8]u8 = undefined;
        std.mem.writeInt(u64, &length, @intCast(physical_key.len), .little);
        hash.update(&length);
        hash.update(physical_key);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};

test "native lock resources bind immutable table and physical row identities across routing changes" {
    const binding: Binding = .{ .namespace = .{ .home = .data, .group = 11, .incarnation = @splat(3) }, .identity = .{ .table_id = 7, .shard_id = 11, .range_id = 13 } };
    const bytes = try binding.encode();
    try std.testing.expect(binding.eql(try Binding.decode(&bytes)));
    try std.testing.expectError(error.InvalidLockRecord, Binding.decode(bytes[0..51]));
    var invalid = bytes;
    invalid[0] = 'X';
    try std.testing.expectError(error.InvalidLockRecord, Binding.decode(&invalid));
    invalid = bytes;
    @memset(invalid[4..20], 0);
    try std.testing.expectError(error.InvalidLockRecord, Binding.decode(&invalid));
    var changed = binding;
    changed.identity.table_id += 1;
    try std.testing.expectError(error.IdentityNamespaceMismatch, binding.requireIdentity(changed.identity));
    try std.testing.expect(!std.mem.eql(u8, &try binding.table(), &try changed.table()));
    try std.testing.expect(!std.mem.eql(u8, &try binding.row("a"), &try binding.row("a\x00")));
    try std.testing.expect(!std.mem.eql(u8, &try binding.row("a"), &try changed.row("a")));
    try std.testing.expect(!std.mem.eql(u8, &try binding.table(), &try binding.row("a")));
    changed = binding;
    changed.namespace.incarnation[0] += 1;
    try std.testing.expect(!std.mem.eql(u8, &try binding.row("a"), &try changed.row("a")));
    changed = binding;
    changed.namespace.group += 1;
    changed.identity.shard_id += 1;
    changed.identity.range_id += 1;
    // Routing is a journal namespace, not a new logical row identity. Moving
    // the participant requires its own handoff; it cannot rekey user locks.
    try std.testing.expectEqual(try binding.table(), try changed.table());
    try std.testing.expectEqual(try binding.row("a"), try changed.row("a"));
    try std.testing.expectError(error.InvalidLockResource, binding.row(""));
    try std.testing.expectError(error.InvalidLockResource, binding.row("\x00\x00metadata"));
}
