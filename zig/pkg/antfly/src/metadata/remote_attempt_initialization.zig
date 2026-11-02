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

//! Permanent first-use authority, separate from administrative root retirement.
//! Prepared permits initial journal creation; active permits only reopening the
//! same initialization ID. Neither phase proves external-work recovery.
const std = @import("std");
const enrollment = @import("store_root_enrollment.zig");
const Ed25519 = std.crypto.sign.Ed25519;
pub const Identity = enrollment.Identity;
pub const Id = [32]u8;
pub const Phase = enum(u8) { prepared = 1, active = 2 };
pub const Action = enum(u8) { prepare = 1, activate = 2, status = 3 };
pub const profile_version: u16 = 1;

pub const Request = struct {
    identity: Identity,
    reporter_incarnation: u64,
    profile: u16 = profile_version,
    action: Action,
    initialization_id: Id,
    signature: [64]u8,

    fn digest(self: Request) ![32]u8 {
        try self.identity.validate();
        if (self.reporter_incarnation == 0 or self.profile != profile_version or
            (self.action != .status and std.mem.allEqual(u8, &self.initialization_id, 0))) return error.InvalidStoreRootEnrollment;
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/remote-attempt-initialization/v1");
        hash.update(&(try self.identity.digest()));
        var numbers: [11]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], self.reporter_incarnation, .little);
        std.mem.writeInt(u16, numbers[8..10], self.profile, .little);
        numbers[10] = @backingInt(self.action);
        hash.update(&numbers);
        hash.update(&self.initialization_id);
        return hash.finalResult();
    }
    pub fn validate(self: Request) !void {
        const digest_ = try self.digest();
        const key_ = Ed25519.PublicKey.fromBytes(self.identity.public_key) catch return error.InvalidStoreRootEnrollment;
        Ed25519.Signature.fromBytes(self.signature).verify(&digest_, key_) catch return error.InvalidStoreRootEnrollment;
    }
    pub fn sign(identity: Identity, reporter: u64, action: Action, id: Id, seed: [32]u8) !Request {
        var self = Request{ .identity = identity, .reporter_incarnation = reporter, .action = action, .initialization_id = id, .signature = undefined };
        const pair = try Ed25519.KeyPair.generateDeterministic(seed);
        if (!std.mem.eql(u8, &pair.public_key.toBytes(), &identity.public_key)) return error.InvalidStoreRootEnrollment;
        self.signature = (try pair.sign(&(try self.digest()), null)).toBytes();
        return self;
    }
};

pub const Entry = struct {
    identity: Identity,
    profile: u16 = profile_version,
    initialization_id: Id,
    phase: Phase = .prepared,
    pub fn validate(self: Entry) !void {
        try self.identity.validate();
        if (self.profile != profile_version or std.mem.allEqual(u8, &self.initialization_id, 0)) return error.InvalidStoreRootEnrollment;
    }
};

/// Replayed preparation never replaces an existing ID or downgrades activation.
/// Caller validates the current metadata incarnation, registered key and process
/// reporter in the SAME transaction before applying this transition.
pub fn transition(previous: ?Entry, request: Request) !Entry {
    try request.validate();
    if (previous) |old| {
        try old.validate();
        if (!std.meta.eql(old.identity, request.identity) or old.profile != request.profile) return error.StoreRootEnrollmentChanged;
        if (request.action == .activate and !std.mem.eql(u8, &old.initialization_id, &request.initialization_id)) return error.StoreRootEnrollmentChanged;
        var next = old;
        if (request.action == .activate) next.phase = .active;
        return next;
    }
    if (request.action != .prepare) return error.CatalogNotFound;
    return .{ .identity = request.identity, .initialization_id = request.initialization_id };
}
pub fn prefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "metadata:{x:0>16}:remote-attempt-initialization:", .{group});
}
pub fn key(buf: []u8, group: u64, store: u64, root: u128) ![]const u8 {
    return std.fmt.bufPrint(buf, "metadata:{x:0>16}:remote-attempt-initialization:{x:0>16}:{x:0>32}", .{ group, store, root });
}

test "remote initialization identity signature and phase are immutable across replay" {
    const seed: [32]u8 = @splat(7);
    const pair = try Ed25519.KeyPair.generateDeterministic(seed);
    const identity = Identity{ .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .node_id = 3, .store_id = 5, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    const prepared = try Request.sign(identity, 9, .prepare, @splat(1), seed);
    const first = try transition(null, prepared);
    const retry = try Request.sign(identity, 10, .prepare, @splat(2), seed);
    try std.testing.expectEqualDeep(first, try transition(first, retry));
    const activated = try transition(first, try Request.sign(identity, 10, .activate, first.initialization_id, seed));
    try std.testing.expectEqual(Phase.active, activated.phase);
    try std.testing.expectEqualDeep(activated, try transition(activated, prepared));
    try std.testing.expectError(error.StoreRootEnrollmentChanged, transition(activated, try Request.sign(identity, 10, .activate, @splat(2), seed)));
    var forged = prepared;
    forged.action = .activate;
    try std.testing.expectError(error.InvalidStoreRootEnrollment, forged.validate());
    forged = prepared;
    forged.reporter_incarnation += 1;
    try std.testing.expectError(error.InvalidStoreRootEnrollment, forged.validate());
    try std.testing.expectError(error.CatalogNotFound, transition(null, try Request.sign(identity, 10, .activate, first.initialization_id, seed)));
    try std.testing.expectError(error.InvalidStoreRootEnrollment, Request.sign(identity, 10, .prepare, @splat(0), seed));
}
