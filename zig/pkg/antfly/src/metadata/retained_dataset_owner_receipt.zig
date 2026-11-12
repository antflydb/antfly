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

//! Physical native observations are signed separately from owner journal rows.
//! Signature verification is necessary but not sufficient: metadata admission
//! must check current root enrollment, reporter, placement and owner revision.
const std = @import("std");
const model = @import("retained_dataset_owners.zig");
const types = @import("antfly_local_sources").retained_dataset_types;
const enrollment = @import("store_root_enrollment.zig");
const Ed25519 = std.crypto.sign.Ed25519;
pub const max_bytes = 4096;
pub const Cut = struct { sequence: u64, complete_from_ns: i128, admitted_ns: i128 };
pub const Receipt = struct {
    resource: u64,
    generation: u64,
    plan: model.Id,
    dataset: model.Id,
    definition: model.Id,
    group: u64,
    partition: model.Id,
    incarnation: u64,
    owner_revision: u64,
    mode: model.Mode,
    cut: Cut,
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    replica_id: u64,
    /// Native data-Raft catalog admission version, distinct from source cut.
    metadata_version: u64,
    native_root: u128,
    signature: [64]u8 = @splat(0),

    pub fn validate(self: Receipt) !void {
        try self.identity.validate();
        if (self.resource == 0 or self.generation == 0 or self.generation > types.max_public_counter or
            types.zero(self.plan) or types.zero(self.dataset) or types.zero(self.definition) or
            self.group == 0 or types.zero(self.partition) or self.incarnation == 0 or
            self.owner_revision == 0 or self.owner_revision >= types.max_public_counter or
            self.native_root == 0 or self.mode == .registered or self.reporter_incarnation == 0 or self.replica_id == 0 or
            self.cut.sequence > types.max_public_counter) return error.InvalidRetainedOwnerPlan;
    }
    pub fn digest(self: Receipt) !model.Id {
        try self.validate();
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update("antfly/retained-native-owner-observation/v1");
        h.update(&(try self.identity.digest()));
        var bytes: [16]u8 = undefined;
        for ([_]u64{ self.resource, self.generation, self.group, self.incarnation, self.owner_revision, self.cut.sequence, self.reporter_incarnation, self.replica_id, self.metadata_version }) |n| {
            std.mem.writeInt(u64, bytes[0..8], n, .big);
            h.update(bytes[0..8]);
        }
        std.mem.writeInt(u128, &bytes, self.native_root, .big);
        h.update(&bytes);
        for ([_]i128{ self.cut.complete_from_ns, self.cut.admitted_ns }) |n| {
            std.mem.writeInt(i128, &bytes, n, .big);
            h.update(&bytes);
        }
        for ([_]model.Id{ self.plan, self.dataset, self.definition, self.partition }) |id| h.update(&id);
        h.update(&.{@backingInt(self.mode)});
        return h.finalResult();
    }
    pub fn sign(self: Receipt, seed: [32]u8) !Receipt {
        const pair = try Ed25519.KeyPair.generateDeterministic(seed);
        if (!std.mem.eql(u8, &pair.public_key.toBytes(), &self.identity.public_key)) return error.InvalidRetainedOwnerPlan;
        var signed = self;
        signed.signature = (try pair.sign(&(try self.digest()), null)).toBytes();
        return signed;
    }
    pub fn verify(self: Receipt) !void {
        const key = Ed25519.PublicKey.fromBytes(self.identity.public_key) catch return error.InvalidRetainedOwnerPlan;
        var verifier = Ed25519.Signature.fromBytes(self.signature).verifier(key) catch return error.InvalidRetainedOwnerPlan;
        verifier.update(&(try self.digest()));
        verifier.verify() catch return error.InvalidRetainedOwnerPlan;
    }
    /// Logical closure survives owner restarts and placement changes. It must
    /// never be derived from the physical serving root or its signature.
    pub fn closure(self: Receipt) !model.Id {
        try self.validate();
        if (self.mode != .closed) return error.InvalidRetainedOwnerPlan;
        var h = std.crypto.hash.sha2.Sha256.init(.{});
        h.update("antfly/retained-native-owner-closure/v1");
        for ([_]model.Id{ self.plan, self.dataset, self.definition, self.partition }) |id| h.update(&id);
        var bytes: [16]u8 = undefined;
        for ([_]u64{ self.generation, self.group, self.incarnation, self.cut.sequence }) |n| {
            std.mem.writeInt(u64, bytes[0..8], n, .big);
            h.update(bytes[0..8]);
        }
        for ([_]i128{ self.cut.complete_from_ns, self.cut.admitted_ns }) |n| {
            std.mem.writeInt(i128, &bytes, n, .big);
            h.update(&bytes);
        }
        return h.finalResult();
    }
    pub fn owner(self: Receipt, a: std.mem.Allocator, plan: model.Plan, previous: model.Owner) !model.Owner {
        try self.verify();
        try plan.validate();
        try previous.validate();
        if (plan.resource != self.resource or plan.generation != self.generation or
            !std.mem.eql(u8, &plan.id, &self.plan) or !std.mem.eql(u8, &plan.dataset, &self.dataset) or
            !std.mem.eql(u8, &plan.definition, &self.definition) or !std.mem.eql(u8, &previous.plan, &self.plan) or
            previous.group != self.group or previous.revision != self.owner_revision or
            previous.incarnation != self.incarnation or !std.mem.eql(u8, &previous.partition, &self.partition)) return error.RetainedOwnerPlanChanged;
        var next = previous;
        next.revision += 1;
        next.mode = self.mode;
        next.native_receipt = try self.digest();
        if (self.mode == .closed) next.cut = .{
            .closure = try self.closure(),
            .sequence = self.cut.sequence,
            .complete_from_ns = try std.fmt.allocPrint(a, "{d}", .{self.cut.complete_from_ns}),
            .admitted_ns = try std.fmt.allocPrint(a, "{d}", .{self.cut.admitted_ns}),
        };
        _ = try plan.acknowledge(previous, next);
        return next;
    }
};

test "retained native observations bind every signed scope and keep closure independent of serving root" {
    const seed: [32]u8 = @splat(9);
    const pair = try Ed25519.KeyPair.generateDeterministic(seed);
    const r = try (Receipt{ .resource = 7, .generation = 1, .plan = @splat(1), .dataset = @splat(2), .definition = @splat(3), .group = 11, .partition = @splat(4), .incarnation = 1, .owner_revision = 2, .mode = .closed, .cut = .{ .sequence = 42, .complete_from_ns = -100, .admitted_ns = 999 }, .identity = .{ .metadata_incarnation = "11111111111111111111111111111111".*, .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = pair.public_key.toBytes() }, .reporter_incarnation = 4, .replica_id = 5, .metadata_version = 6, .native_root = 17 }).sign(seed);
    try r.verify();
    inline for (.{ "resource", "generation", "group", "incarnation", "owner_revision", "reporter_incarnation", "replica_id", "metadata_version" }) |field| {
        var tampered = r;
        @field(tampered, field) += 1;
        try std.testing.expectError(error.InvalidRetainedOwnerPlan, tampered.verify());
    }
    inline for (.{ "plan", "dataset", "definition", "partition" }) |field| {
        var tampered = r;
        @field(tampered, field)[0] ^= 1;
        try std.testing.expectError(error.InvalidRetainedOwnerPlan, tampered.verify());
    }
    var changed = r;
    changed.native_root += 1;
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, changed.verify());
    changed = r;
    changed.cut.complete_from_ns -= 1;
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, changed.verify());
    changed = r;
    changed.cut.admitted_ns += 1;
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, changed.verify());
    changed = r;
    changed.mode = .ready;
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, changed.verify());
    var moved = r;
    moved.identity.root_incarnation += 1;
    moved.reporter_incarnation += 1;
    moved.native_root += 1;
    moved = try moved.sign(seed);
    try moved.verify();
    try std.testing.expectEqualDeep(try r.closure(), try moved.closure());
    try std.testing.expect(!std.mem.eql(u8, &try r.digest(), &try moved.digest()));
    moved.cut.sequence += 1;
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, moved.verify());
    try std.testing.expect(!std.mem.eql(u8, &try r.closure(), &try moved.closure()));
}
