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

//! Structural journal for per-owner retained producers. Register an immutable
//! owner before sending any native RPC, and retain it until cancellation is
//! acknowledged. Receipt verification belongs to fenced metadata admission;
//! these transitions alone never authorize source detachment or collection.
const std = @import("std");
const types = @import("antfly_local_sources").retained_dataset_types;
pub const Id = types.Id;
pub const max_owners = 4096;
pub const Activation = struct {
    resource: u64,
    generation: u64,
    plan: Id,
    revision: u64,
    pub fn validate(self: Activation) !void {
        try (Registration{ .resource = self.resource, .generation = self.generation, .plan = self.plan, .revision = self.revision, .limit = 1 }).validate();
    }
};
pub const Registration = struct {
    resource: u64,
    generation: u64,
    plan: Id,
    revision: u64,
    limit: u16 = 64,

    pub fn validate(self: Registration) !void {
        if (self.resource == 0 or self.generation == 0 or self.generation > types.max_public_counter or
            self.revision == 0 or self.revision > types.max_public_counter or types.zero(self.plan) or self.limit == 0 or self.limit > 64)
            return error.InvalidRetainedOwnerPlan;
    }
};
/// Private reads use distinct tags so older peers reject unknown authority.
pub const Cursor = struct { resource: u64 = 0, generation: u64 = 0, owner_group: u64 = 0 };
pub const WorkKey = struct { cursor: Cursor, plan: Id };
pub const Read = union(enum) {
    work: Cursor,
    work_key: Cursor,
    selection: WorkKey,
    plan: struct { resource: u64, generation: u64 },
    owner: struct { resource: u64, generation: u64, plan: Id, group: u64, revision: u64 },

    pub fn validate(self: Read) !void {
        switch (self) {
            .work, .work_key => |c| {
                if ((c.resource == 0 or c.generation == 0 or c.owner_group == 0) and
                    (c.resource != 0 or c.generation != 0 or c.owner_group != 0)) return error.InvalidRetainedOwnerPlan;
            },
            .selection => |q| {
                try (Read{ .work_key = q.cursor }).validate();
                if (q.cursor.resource == 0 or types.zero(q.plan)) return error.InvalidRetainedOwnerPlan;
            },
            .plan => |q| if (q.resource == 0 or q.generation == 0 or q.generation > types.max_public_counter) return error.InvalidRetainedOwnerPlan,
            .owner => |q| if (q.resource == 0 or q.generation == 0 or q.generation > types.max_public_counter or
                types.zero(q.plan) or q.group == 0 or q.revision == 0 or q.revision > types.max_public_counter) return error.InvalidRetainedOwnerPlan,
        }
    }
};
pub const ArchiveScope = @FieldType(Read, "owner");
/// Only a metadata leader which verified the sealed independent catalog may
/// propose this command. Clients send ArchiveScope, never an archive digest.
pub const ArchiveAcknowledgement = struct {
    scope: ArchiveScope,
    closure: Id,
    receipt: Id,

    pub fn validate(self: ArchiveAcknowledgement) !void {
        try (Read{ .owner = self.scope }).validate();
        if (types.zero(self.closure) or types.zero(self.receipt)) return error.InvalidRetainedOwnerPlan;
    }
};
pub const Phase = enum { activating, active, closing, closed, retiring, retired };
pub const Mode = enum { registered, installed, ready, closed, revoked };
pub const Cut = struct {
    /// Stable closure witness; never a serving session root incarnation.
    closure: Id,
    sequence: u64,
    complete_from_ns: []const u8,
    admitted_ns: []const u8,
    pub fn validate(self: Cut) !void {
        if (types.zero(self.closure) or self.sequence > types.max_public_counter or self.complete_from_ns.len > 40 or self.admitted_ns.len > 40) return error.InvalidRetainedOwnerPlan;
        _ = std.fmt.parseInt(i128, self.complete_from_ns, 10) catch return error.InvalidRetainedOwnerPlan;
        _ = std.fmt.parseInt(i128, self.admitted_ns, 10) catch return error.InvalidRetainedOwnerPlan;
    }
    pub fn jsonStringify(self: Cut, w: anytype) !void {
        try types.writeFields(self, .{}, w);
    }
};
pub const Owner = struct {
    plan: Id,
    group: u64,
    partition: Id,
    incarnation: u64,
    revision: u64 = 1,
    mode: Mode = .registered,
    native_receipt: ?Id = null,
    cut: ?Cut = null,
    independent_receipt: ?Id = null,

    pub fn validate(self: Owner) !void {
        if (types.zero(self.plan) or self.group == 0 or types.zero(self.partition) or self.incarnation == 0 or
            self.revision == 0 or self.revision > types.max_public_counter or
            (self.mode == .registered) != (self.native_receipt == null)) return error.InvalidRetainedOwnerPlan;
        if (self.native_receipt) |receipt| if (types.zero(receipt)) return error.InvalidRetainedOwnerPlan;
        if (self.mode == .closed and self.cut == null) return error.InvalidRetainedOwnerPlan;
        if (self.cut) |cut| {
            try cut.validate();
            if (self.mode != .closed and self.mode != .revoked) return error.InvalidRetainedOwnerPlan;
        }
        if (self.independent_receipt) |receipt| if (types.zero(receipt) or self.cut == null) return error.InvalidRetainedOwnerPlan;
    }
    pub fn jsonStringify(self: Owner, w: anytype) !void {
        try types.writeFields(self, .{}, w);
    }
    fn ready(self: Owner) u32 {
        return @intFromBool(self.mode == .ready or self.mode == .closed);
    }
    fn stopped(self: Owner) u32 {
        return @intFromBool(self.mode == .closed or self.mode == .revoked);
    }
};
pub const Plan = struct {
    format: u8 = 1,
    id: Id,
    resource: u64,
    dataset: Id,
    generation: u64,
    definition: Id,
    source: Id,
    source_table: u64,
    topology_generation: u64,
    membership: Id,
    revision: u64 = 1,
    phase: Phase = .activating,
    registration_after: u64 = 0,
    registration_complete: bool = false,
    registered: u32 = 0,
    registration_membership: @import("topology_protocol.zig").RangeMembershipAccumulator = .{},
    ready: u32 = 0,
    stopped: u32 = 0,
    revoked: u32 = 0,
    independent: u32 = 0,
    source_release_receipt: ?Id = null,
    collection_receipt: ?Id = null,

    /// Bind every immutable source/topology/definition field. Progress and
    /// revisions never change this identity, so old owner RPCs cannot be
    /// reinterpreted under a replacement plan with the same metadata key.
    pub fn identity(self: Plan) Id {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update("antfly/retained-owner-plan/v1");
        var bytes: [8]u8 = undefined;
        for ([_]u64{ self.resource, self.generation, self.source_table, self.topology_generation }) |number| {
            std.mem.writeInt(u64, &bytes, number, .big);
            hash.update(&bytes);
        }
        for ([_]Id{ self.dataset, self.definition, self.source, self.membership }) |digest| hash.update(&digest);
        return hash.finalResult();
    }
    pub fn validate(self: Plan) !void {
        if (!std.mem.eql(u8, &self.id, &self.identity())) return error.InvalidRetainedOwnerPlan;
        if (self.format != 1 or types.zero(self.id) or self.resource == 0 or types.zero(self.dataset) or self.generation == 0 or
            self.generation > types.max_public_counter or types.zero(self.definition) or types.zero(self.source) or self.source_table == 0 or
            types.zero(self.membership) or self.revision == 0 or self.revision > types.max_public_counter or self.registered > max_owners or
            self.ready > self.registered or self.stopped > self.registered or self.revoked > self.stopped or self.independent > self.registered or
            self.ready > self.registered - self.revoked or self.independent > self.ready + self.revoked or
            self.registration_membership.count != self.registered or
            (self.registered == 0) != (self.registration_after == 0)) return error.InvalidRetainedOwnerPlan;
        if (self.registration_complete and self.registered == 0) return error.InvalidRetainedOwnerPlan;
        if (self.registered == 0 and !types.zero(self.registration_membership.xor_digest)) return error.InvalidRetainedOwnerPlan;
        if (self.registration_complete and !std.mem.eql(u8, &self.registration_membership.finish(self.source_table).digest, &self.membership)) return error.InvalidRetainedOwnerPlan;
        if (self.phase == .active and (!self.registration_complete or self.ready != self.registered or self.stopped != 0)) return error.InvalidRetainedOwnerPlan;
        if (self.phase == .closing and !self.registration_complete) return error.InvalidRetainedOwnerPlan;
        if (self.source_release_receipt) |receipt| if (types.zero(receipt) or self.revoked != self.registered or
            (self.phase != .closed and self.phase != .retiring and self.phase != .retired)) return error.InvalidRetainedOwnerPlan;
        if (self.phase == .closed and (!self.registration_complete or self.independent != self.registered or self.source_release_receipt == null)) return error.InvalidRetainedOwnerPlan;
        if (self.phase == .retired and (self.source_release_receipt == null or self.collection_receipt == null)) return error.InvalidRetainedOwnerPlan;
        if (self.collection_receipt) |receipt| if (types.zero(receipt) or self.phase != .retired) return error.InvalidRetainedOwnerPlan;
    }
    pub fn jsonStringify(self: Plan, w: anytype) !void {
        try types.writeFields(self, .{}, w);
    }
    fn changed(self: Plan) !Plan {
        try self.validate();
        if (self.revision == types.max_public_counter) return error.InvalidRetainedOwnerPlan;
        var next = self;
        next.revision += 1;
        return next;
    }
    pub fn register(self: Plan, owner: Owner) !Plan {
        var next = try self.changed();
        try owner.validate();
        if (self.phase != .activating or self.registration_complete or owner.mode != .registered or owner.revision != 1 or
            !std.mem.eql(u8, &owner.plan, &self.id) or owner.group <= self.registration_after or self.registered == max_owners) return error.RetainedOwnerPlanChanged;
        next.registered += 1;
        try next.registration_membership.add(owner.group);
        next.registration_after = owner.group;
        try next.validate();
        return next;
    }
    pub fn completeRegistration(self: Plan) !Plan {
        var next = try self.changed();
        if (self.phase != .activating or self.registration_complete or self.registered == 0) return error.RetainedOwnerPlanChanged;
        next.registration_complete = true;
        try next.validate();
        return next;
    }
    /// Metadata must compare previous to the persisted owner row in the same
    /// transaction as this aggregate update. A response is never an owner row.
    pub fn acknowledge(self: Plan, previous: Owner, owner: Owner) !Plan {
        var next = try self.changed();
        try previous.validate();
        try owner.validate();
        if (!std.mem.eql(u8, &previous.plan, &self.id) or !std.mem.eql(u8, &owner.plan, &self.id) or previous.group != owner.group or
            previous.incarnation != owner.incarnation or !std.mem.eql(u8, &previous.partition, &owner.partition) or
            previous.revision == types.max_public_counter or owner.revision != previous.revision + 1 or self.registered == 0 or
            owner.group > self.registration_after or self.source_release_receipt != null) return error.RetainedOwnerPlanChanged;
        if (previous.independent_receipt != null and !std.meta.eql(previous.independent_receipt, owner.independent_receipt)) return error.RetainedOwnerPlanChanged;
        if (previous.cut) |cut| {
            const after = owner.cut orelse return error.RetainedOwnerPlanChanged;
            if (!std.mem.eql(u8, &cut.closure, &after.closure) or cut.sequence != after.sequence or
                !std.mem.eql(u8, cut.complete_from_ns, after.complete_from_ns) or !std.mem.eql(u8, cut.admitted_ns, after.admitted_ns)) return error.RetainedOwnerPlanChanged;
        }
        if (previous.independent_receipt == null and owner.independent_receipt != null and
            (self.phase != .closing or previous.mode != .closed or owner.mode != .closed)) return error.RetainedOwnerPlanChanged;
        if (previous.mode == owner.mode and std.meta.eql(previous.native_receipt, owner.native_receipt) and
            std.meta.eql(previous.independent_receipt, owner.independent_receipt)) return error.RetainedOwnerPlanChanged;
        const allowed = switch (owner.mode) {
            .registered => false,
            .installed => (self.phase == .activating or self.phase == .closing) and previous.mode == .registered,
            .ready => (self.phase == .activating or self.phase == .closing) and (previous.mode == .registered or previous.mode == .installed),
            .closed => self.phase == .closing and (previous.mode == .ready or previous.mode == .closed),
            .revoked => previous.mode != .revoked and (self.phase == .retiring or
                (self.phase == .closing and previous.mode == .closed and previous.independent_receipt != null)),
        };
        if (!allowed) return error.RetainedOwnerPlanChanged;
        next.ready = try adjust(self.ready, previous.ready(), owner.ready());
        next.stopped = try adjust(self.stopped, previous.stopped(), owner.stopped());
        next.revoked = try adjust(self.revoked, @intFromBool(previous.mode == .revoked), @intFromBool(owner.mode == .revoked));
        next.independent = try adjust(self.independent, @intFromBool(previous.independent_receipt != null), @intFromBool(owner.independent_receipt != null));
        try next.validate();
        return next;
    }
    pub fn activate(self: Plan) !Plan {
        var next = try self.changed();
        if (self.phase != .activating) return error.RetainedOwnerPlanChanged;
        next.phase = .active;
        try next.validate();
        return next;
    }
    pub fn close(self: Plan) !Plan {
        var next = try self.changed();
        if (self.phase != .active) return error.RetainedOwnerPlanChanged;
        next.phase = .closing;
        try next.validate();
        return next;
    }
    pub fn retire(self: Plan) !Plan {
        var next = try self.changed();
        if (self.phase == .retiring or self.phase == .retired) return error.RetainedOwnerPlanChanged;
        next.phase = .retiring;
        try next.validate();
        return next;
    }
    pub fn canReleaseSource(self: Plan) bool {
        if (self.source_release_receipt != null or self.revoked != self.registered) return false;
        return switch (self.phase) {
            .closing => self.registration_complete and self.independent == self.registered,
            .retiring => true,
            else => false,
        };
    }
    /// A digest is structural here. The caller must independently verify the
    /// exact native revocations and authoritative catalog publication first.
    pub fn releaseSource(self: Plan, verified_receipt: Id) !Plan {
        var next = try self.changed();
        if ((self.phase != .closing and self.phase != .retiring) or self.source_release_receipt != null or self.revoked != self.registered or types.zero(verified_receipt)) return error.RetainedOwnerPlanChanged;
        if (self.phase == .closing) {
            if (!self.registration_complete or self.independent != self.registered) return error.RetainedOwnerPlanChanged;
            next.phase = .closed;
        }
        next.source_release_receipt = verified_receipt;
        try next.validate();
        return next;
    }
    pub fn finishCollection(self: Plan, verified_receipt: Id) !Plan {
        var next = try self.changed();
        if (self.phase != .retiring or self.source_release_receipt == null or types.zero(verified_receipt)) return error.RetainedOwnerPlanChanged;
        next.phase = .retired;
        next.collection_receipt = verified_receipt;
        try next.validate();
        return next;
    }
};
fn adjust(current: u32, before: u32, after: u32) !u32 {
    const removed = std.math.sub(u32, current, before) catch return error.InvalidRetainedOwnerPlan;
    return std.math.add(u32, removed, after) catch return error.InvalidRetainedOwnerPlan;
}

fn fixture() Plan {
    var plan: Plan = .{ .id = @splat(0), .resource = 41, .dataset = @splat(2), .generation = 1, .definition = @splat(3), .source = @splat(4), .source_table = 7, .topology_generation = 2, .membership = @splat(5) };
    var membership: @import("topology_protocol.zig").RangeMembershipAccumulator = .{};
    membership.add(71) catch unreachable;
    plan.membership = membership.finish(plan.source_table).digest;
    plan.id = plan.identity();
    return plan;
}
test "retained owner plans cancel incomplete registration and retain source until every planned owner is revoked" {
    var plan = fixture();
    const owner: Owner = .{ .plan = plan.id, .group = 71, .partition = @splat(6), .incarnation = 1 };
    plan = try plan.register(owner);
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, plan.activate());
    plan = try plan.retire();
    try std.testing.expectError(error.RetainedOwnerPlanChanged, plan.register(.{ .plan = plan.id, .group = 72, .partition = @splat(7), .incarnation = 1 }));
    try std.testing.expectError(error.RetainedOwnerPlanChanged, plan.releaseSource(@splat(8)));
    var revoked = owner;
    revoked.revision += 1;
    revoked.mode = .revoked;
    revoked.native_receipt = @splat(9);
    plan = try plan.acknowledge(owner, revoked);
    try std.testing.expectError(error.RetainedOwnerPlanChanged, plan.acknowledge(revoked, revoked));
    plan = try plan.releaseSource(@splat(8));
    plan = try plan.finishCollection(@splat(10));
    try std.testing.expect(plan.phase == .retired and plan.revoked == 1 and !plan.registration_complete);
}
test "retained owner plans require closed coverage and independent publication before revoking a retained generation" {
    var plan = fixture();
    var owner: Owner = .{ .plan = plan.id, .group = 71, .partition = @splat(6), .incarnation = 1 };
    plan = try plan.register(owner);
    plan = try plan.completeRegistration();
    var ready = owner;
    ready.revision += 1;
    ready.mode = .ready;
    ready.native_receipt = @splat(7);
    plan = try plan.acknowledge(owner, ready);
    plan = try plan.activate();
    plan = try plan.close();
    owner = ready;
    owner.revision += 1;
    owner.mode = .closed;
    owner.native_receipt = @splat(8);
    owner.cut = .{ .closure = @splat(9), .sequence = 11, .complete_from_ns = "0", .admitted_ns = "100" };
    plan = try plan.acknowledge(ready, owner);
    var revoked = owner;
    revoked.revision += 1;
    revoked.mode = .revoked;
    revoked.native_receipt = @splat(10);
    try std.testing.expectError(error.RetainedOwnerPlanChanged, plan.acknowledge(owner, revoked));
    var published = owner;
    published.revision += 1;
    published.independent_receipt = @splat(11);
    plan = try plan.acknowledge(owner, published);
    revoked = published;
    revoked.revision += 1;
    revoked.mode = .revoked;
    revoked.native_receipt = @splat(10);
    plan = try plan.acknowledge(published, revoked);
    plan = try plan.releaseSource(@splat(12));
    try std.testing.expect(plan.phase == .closed and plan.independent == 1 and plan.revoked == 1);
    const a = std.testing.allocator;
    const bytes = try std.json.Stringify.valueAlloc(a, plan, .{});
    defer a.free(bytes);
    var decoded = try std.json.parseFromSlice(Plan, a, bytes, .{});
    defer decoded.deinit();
    try decoded.value.validate();
    try std.testing.expectEqualDeep(plan, decoded.value);
}

test "retained owner registration verifies exact frozen membership instead of count alone" {
    var plan = fixture();
    plan = try plan.register(.{ .plan = plan.id, .group = 72, .partition = @splat(6), .incarnation = 1 });
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, plan.completeRegistration());
    // The incomplete plan can still be cancelled and every attempted owner
    // revoked; an inconsistent index must never make activation possible.
    plan = try plan.retire();
    try std.testing.expect(plan.phase == .retiring and !plan.registration_complete);
}

test "retained owner plan identity rejects source or topology rebinding" {
    const original = fixture();
    var changed = original;
    changed.source_table += 1;
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, changed.validate());
    changed = original;
    changed.membership = @splat(8);
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, changed.validate());
    changed = original;
    changed.definition = @splat(9);
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, changed.validate());
}

test "retained owner reads reject ambiguous cursors and unbound exact requests" {
    try (Read{ .work = .{} }).validate();
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, (Read{ .work = .{ .resource = 1 } }).validate());
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, (Read{ .plan = .{ .resource = 1, .generation = 0 } }).validate());
    try std.testing.expectError(error.InvalidRetainedOwnerPlan, (Read{ .owner = .{ .resource = 1, .generation = 1, .plan = @splat(0), .group = 1, .revision = 1 } }).validate());
}
