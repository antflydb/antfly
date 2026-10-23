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

//! Historical metadata acceptance, not owner execution authority. A missing or
//! superseded witness is unknown, never proof that a proposal was not admitted.
const std = @import("std");
const read = @import("schema_validation_read.zig");
const barriers = @import("schema_validation_publication.zig");

pub const Effect = union(enum) {
    admission: void,
    acknowledgement: struct { ordinal: u16, stage: barriers.Stage, header_digest: [32]u8, proof_digest: [32]u8 },
    parent_authorization: struct { ordinal: u16, grant: [32]u8 },
    root_adoption: struct { ordinal: u16, epoch: u64, request_digest: [32]u8 },
    installation: Decision,
    publication: Decision,
    cancellation: struct { decision: Decision, failure: barriers.Failure },

    pub const Decision = struct { revision: u64, digest: [32]u8 };

    fn requireDecision(value: Decision) !void {
        if (value.revision == 0 or value.revision == std.math.maxInt(u64) or std.mem.allEqual(u8, &value.digest, 0)) return error.InvalidGenerationPublication;
    }
    pub fn validate(self: @This()) !void {
        switch (self) {
            .admission => {},
            .acknowledgement => |ack| if (ack.ordinal >= barriers.max_owners or std.mem.allEqual(u8, &ack.header_digest, 0) or std.mem.allEqual(u8, &ack.proof_digest, 0)) return error.InvalidGenerationPublication,
            .parent_authorization => |value| if (value.ordinal >= barriers.max_owners or std.mem.allEqual(u8, &value.grant, 0)) return error.InvalidGenerationPublication,
            .root_adoption => |value| if (value.ordinal >= barriers.max_owners or value.epoch == 0 or std.mem.allEqual(u8, &value.request_digest, 0)) return error.InvalidGenerationPublication,
            .installation, .publication => |decision| try requireDecision(decision),
            .cancellation => |cancel| {
                try requireDecision(cancel.decision);
                if (cancel.failure == .none or cancel.failure == .not_null_violation) return error.InvalidGenerationPublication;
            },
        }
    }
};

pub const Request = struct {
    lookup: read.Lookup,
    expected: Effect,

    pub fn validate(self: @This()) !void {
        try (read.OwnerReadRequest{ .lookup = self.lookup, .ordinal = 0 }).validate();
        try self.expected.validate();
    }
};

pub const Observation = struct {
    lookup: read.Lookup,
    head: ?barriers.Head = null,
    observed: ?Effect = null,

    pub fn isAcknowledged(self: @This(), request: Request) !bool {
        try request.validate();
        if (!std.meta.eql(self.lookup, request.lookup)) return error.GenerationPublicationChanged;
        const head = self.head orelse {
            if (self.observed != null) return error.InvalidGenerationPublication;
            return false;
        };
        try self.lookup.require(head);
        const effect = self.observed orelse return false;
        try effect.validate();
        if (std.meta.activeTag(effect) != std.meta.activeTag(request.expected)) return error.InvalidGenerationPublication;
        switch (effect) {
            .admission => {},
            .parent_authorization => |value| if (value.ordinal >= head.owner_count or value.ordinal != request.expected.parent_authorization.ordinal) return error.InvalidGenerationPublication,
            .root_adoption => |value| if (value.ordinal >= head.owner_count or value.ordinal != request.expected.root_adoption.ordinal or value.epoch != request.expected.root_adoption.epoch) return error.InvalidGenerationPublication,
            .acknowledgement => |ack| {
                const wanted = request.expected.acknowledgement;
                if (ack.ordinal >= head.owner_count or ack.ordinal != wanted.ordinal or ack.stage != wanted.stage) return error.InvalidGenerationPublication;
                const present = switch (ack.stage) {
                    .ready => head.ready != 0,
                    .installed => head.installed != 0,
                    .released => head.released != 0,
                    .canceled => head.canceled != 0,
                    .failed => head.failure == .not_null_violation,
                };
                if (!present) return error.InvalidGenerationPublication;
            },
            .installation => |decision| if (head.install_revision == 0 or decision.revision != head.install_revision - 1 or
                !std.mem.eql(u8, &decision.digest, &try head.installDecision())) return error.InvalidGenerationPublication,
            .publication => |decision| if (head.publication_revision == 0 or decision.revision != head.publication_revision - 1 or
                !std.mem.eql(u8, &decision.digest, &try head.publicationDecision())) return error.InvalidGenerationPublication,
            .cancellation => |cancel| if (head.cancellation_revision == 0 or cancel.decision.revision != head.cancellation_revision - 1 or
                cancel.failure != head.failure or !std.mem.eql(u8, &cancel.decision.digest, &try head.cancellationDecision())) return error.InvalidGenerationPublication,
        }
        return std.meta.eql(request.expected, effect);
    }

    /// A newer physical re-attestation may replace a proof without changing
    /// the committed logical owner phase. This is NOT proof that the older
    /// signature was accepted and never authorizes physical execution/adoption.
    pub fn isLogicalAcknowledgement(self: @This(), request: Request) !bool {
        _ = try self.isAcknowledged(request);
        if (request.expected != .acknowledgement or self.observed == null) return false;
        const expected = request.expected.acknowledgement;
        const actual = self.observed.?.acknowledgement;
        return std.mem.eql(u8, &expected.header_digest, &actual.header_digest);
    }
};

test "schema validation publication receipts distinguish exact logical and missing acceptance" {
    var head: barriers.Head = .{ .metadata_group_id = 21, .metadata_incarnation = @splat('1'), .id = @splat(1), .plan_digest = @splat(2), .table_id = 7, .owner_count = 2 };
    const lookup = read.Lookup.fromHead(head);
    var request: Request = .{ .lookup = lookup, .expected = .admission };
    var observed: Observation = .{ .lookup = lookup };
    try std.testing.expect(!try observed.isAcknowledged(request));
    observed.observed = .admission;
    try std.testing.expectError(error.InvalidGenerationPublication, observed.isAcknowledged(request));
    observed.head = head;
    try std.testing.expect(try observed.isAcknowledged(request));
    head.revision = 2;
    head.ready = 1;
    head.ready_chain = @splat(3);
    request.expected = .{ .acknowledgement = .{ .ordinal = 0, .stage = .ready, .header_digest = @splat(4), .proof_digest = @splat(5) } };
    observed = .{ .lookup = lookup, .head = head, .observed = request.expected };
    try std.testing.expect(try observed.isAcknowledged(request));
    observed.observed.?.acknowledgement.proof_digest[0] ^= 1;
    try std.testing.expect(!try observed.isAcknowledged(request));
    try std.testing.expect(try observed.isLogicalAcknowledgement(request));
    observed.observed.?.acknowledgement.header_digest[0] ^= 1;
    try std.testing.expect(!try observed.isLogicalAcknowledgement(request));
    observed.observed = request.expected;
    observed.observed.?.acknowledgement.ordinal = 1;
    try std.testing.expectError(error.InvalidGenerationPublication, observed.isAcknowledged(request));
    observed.observed = request.expected;
    observed.head.?.ready = 0;
    observed.head.?.ready_chain = @splat(0);
    try std.testing.expectError(error.InvalidGenerationPublication, observed.isAcknowledged(request));
    head.ready = 2;
    head.phase = .validated;
    head.revision = 3;
    const predecessor = head.revision;
    head = try head.decideInstall(predecessor);
    request.expected = .{ .installation = .{ .revision = predecessor, .digest = try head.installDecision() } };
    observed = .{ .lookup = lookup, .head = head, .observed = request.expected };
    try std.testing.expect(try observed.isAcknowledged(request));
    observed.head.?.revision += 1;
    try std.testing.expect(try observed.isAcknowledged(request));
    observed.observed.?.installation.digest[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, observed.isAcknowledged(request));
    head.installed = 2;
    head.installed_chain = @splat(6);
    head.phase = .installed;
    head.revision = 6;
    head = try head.publish(6);
    request.expected = .{ .publication = .{ .revision = 6, .digest = try head.publicationDecision() } };
    observed = .{ .lookup = lookup, .head = head, .observed = request.expected };
    try std.testing.expect(try observed.isAcknowledged(request));
    observed.head.?.released = 2;
    observed.head.?.terminal_chain = @splat(7);
    observed.head.?.phase = .succeeded;
    observed.head.?.revision = 9;
    try std.testing.expect(try observed.isAcknowledged(request));
    head = .{ .metadata_group_id = 21, .metadata_incarnation = lookup.metadata_incarnation, .id = lookup.id, .plan_digest = lookup.plan_digest, .table_id = 7, .owner_count = 2 };
    head = try head.cancel(1, .requested);
    request.expected = .{ .cancellation = .{ .decision = .{ .revision = 1, .digest = try head.cancellationDecision() }, .failure = .requested } };
    observed = .{ .lookup = lookup, .head = head, .observed = request.expected };
    try std.testing.expect(try observed.isAcknowledged(request));
    request.expected.cancellation.failure = .owner_failed;
    try std.testing.expect(!try observed.isAcknowledged(request));
    request.expected.cancellation.failure = .not_null_violation;
    try std.testing.expectError(error.InvalidGenerationPublication, request.validate());
}
