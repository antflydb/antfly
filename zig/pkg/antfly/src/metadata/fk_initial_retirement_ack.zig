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

//! The deterministic validation performed inside the metadata Raft ACK
//! transaction. Discovery tickets and the shared internal HTTP token are not
//! authority: the current registered physical root must sign the exact
//! unlinked receipt, and the canceled work must still match both indexes.
const std = @import("std");
const auth = @import("fk_initial_retirement_auth.zig");
const contract = @import("fk_initial_retirement_contract.zig");
const retirement = @import("fk_initial_retirement.zig");
const incarnation = @import("antfly_local_sources").metadata_incarnation;

pub const CanceledPublication = struct {
    child_table_id: u64,
    plan_id: [16]u8,
    plan_digest: [32]u8,
    revision: u64,
    hosted: bool,
    canceled: bool,
    published: bool = false,
};

pub const CanceledReservation = struct {
    child_table_id: u64,
    range_id: u64,
    plan_id: [16]u8,
    plan_digest: [32]u8,
    hosted: bool,
    canceled: bool,
    obsolete_placement: bool = false,
};

pub const RegisteredRoot = struct {
    reporter: contract.CurrentReporter,
    public_key: [32]u8,
};

pub const Disposition = enum { newly_acked, already_acked };

/// All inputs must be loaded from the *same* Raft write transaction. The
/// caller must compare the group and store index bytes before invoking this
/// function and write the returned replica to both keys atomically. No
/// caller-visible success is permitted after a failed comparison.
pub fn validate(
    signed: auth.SignedReceipt,
    metadata_cluster: incarnation.MetadataClusterIncarnation,
    publication: CanceledPublication,
    reservation: CanceledReservation,
    group_work: retirement.Replica,
    store_work: retirement.Replica,
    current: RegisteredRoot,
) !Disposition {
    const ticket = signed.receipt.ticket;
    try signed.receipt.validate();
    if (!std.meta.eql(group_work, store_work)) return error.InitialFkRetirementWorkChanged;
    try contract.requireExactWork(ticket, group_work, metadata_cluster, publication.revision);
    const phase_matches = switch (ticket.replica.retirement_authority) {
        .canceled_plan => publication.canceled and !publication.published,
        .published_obsolete => publication.published and !publication.canceled,
    };
    if (!phase_matches or !publication.hosted or
        publication.child_table_id != ticket.replica.child_table_id or
        !std.mem.eql(u8, &publication.plan_id, &ticket.replica.plan_id) or
        !std.mem.eql(u8, &publication.plan_digest, &ticket.replica.plan_digest))
        return error.InitialFkRetirementPublicationChanged;
    const reservation_matches = switch (ticket.replica.retirement_authority) {
        .canceled_plan => reservation.canceled and !reservation.obsolete_placement,
        .published_obsolete => reservation.obsolete_placement and !reservation.canceled,
    };
    if (!reservation_matches or !reservation.hosted or
        reservation.child_table_id != ticket.replica.child_table_id or
        reservation.range_id != ticket.replica.range_id or
        !std.mem.eql(u8, &reservation.plan_id, &ticket.replica.plan_id) or
        !std.mem.eql(u8, &reservation.plan_digest, &ticket.replica.plan_digest))
        return error.InitialFkRetirementReservationChanged;
    try contract.requireCurrentReporter(signed.receipt, current.reporter);
    if (std.mem.allEqual(u8, &current.public_key, 0)) return error.InitialFkRetirementSigningKeyUnavailable;
    try signed.verify(current.public_key);
    return if (group_work.acked) .already_acked else .newly_acked;
}

test "retirement ACK validates exact canceled work and current physical-root signature" {
    const root_identity = @import("antfly_local_sources").storage_db_root_signing_identity;
    const seed: [32]u8 = @splat(7);
    const key_pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: root_identity.State = .{
        .root_incarnation = 19,
        .seed = seed,
        .public_key = key_pair.public_key.toBytes(),
    };
    const work: retirement.Replica = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .child_table_id = 3,
        .group_id = 5,
        .range_id = 7,
        .node_id = 11,
        .store_id = 13,
        .store_incarnation = 17,
        .store_root_incarnation = 19,
        .replica_id = 23,
        .root_generation = 27,
        .canceled = true,
    };
    const ticket: contract.Ticket = .{
        .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
        .cancel_revision = 29,
        .replica = work,
    };
    const receipt = try contract.Receipt.fromUnlinkedIntent(.{ .ticket = ticket, .phase = .unlinked }, 31);
    const signed = try auth.SignedReceipt.sign(root, receipt);
    const publication: CanceledPublication = .{
        .child_table_id = 3,
        .plan_id = work.plan_id,
        .plan_digest = work.plan_digest,
        .revision = 29,
        .hosted = true,
        .canceled = true,
    };
    const reservation: CanceledReservation = .{
        .child_table_id = 3,
        .range_id = 7,
        .plan_id = work.plan_id,
        .plan_digest = work.plan_digest,
        .hosted = true,
        .canceled = true,
    };
    const current: RegisteredRoot = .{
        .reporter = .{ .node_id = 11, .store_id = 13, .reporter_incarnation = 31, .store_root_incarnation = 19, .live = true },
        .public_key = root.public_key,
    };
    try std.testing.expectEqual(Disposition.newly_acked, try validate(signed, ticket.metadata_incarnation, publication, reservation, work, work, current));
    var obsolete = work;
    obsolete.retirement_authority = .published_obsolete;
    try std.testing.expectEqualDeep(obsolete, try retirement.Replica.decode(&try obsolete.encode()));
    var obsolete_ticket = ticket;
    obsolete_ticket.replica = obsolete;
    const obsolete_signed = try auth.SignedReceipt.sign(root, try contract.Receipt.fromUnlinkedIntent(.{ .ticket = obsolete_ticket, .phase = .unlinked }, 31));
    var published = publication;
    published.canceled = false;
    published.published = true;
    var exclusion = reservation;
    exclusion.canceled = false;
    exclusion.obsolete_placement = true;
    try std.testing.expectEqual(Disposition.newly_acked, try validate(obsolete_signed, ticket.metadata_incarnation, published, exclusion, obsolete, obsolete, current));
    try std.testing.expectError(error.InitialFkRetirementPublicationChanged, validate(obsolete_signed, ticket.metadata_incarnation, publication, exclusion, obsolete, obsolete, current));
    try std.testing.expectError(error.InitialFkRetirementReservationChanged, validate(obsolete_signed, ticket.metadata_incarnation, published, reservation, obsolete, obsolete, current));
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, validate(signed, ticket.metadata_incarnation, published, exclusion, obsolete, obsolete, current));
    var acked = work;
    acked.acked = true;
    try std.testing.expectEqual(Disposition.already_acked, try validate(signed, ticket.metadata_incarnation, publication, reservation, acked, acked, current));
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, validate(signed, ticket.metadata_incarnation, publication, reservation, work, acked, current));
    var changed_publication = publication;
    changed_publication.revision += 1;
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, validate(signed, ticket.metadata_incarnation, changed_publication, reservation, work, work, current));
    changed_publication = publication;
    changed_publication.canceled = false;
    try std.testing.expectError(error.InitialFkRetirementPublicationChanged, validate(signed, ticket.metadata_incarnation, changed_publication, reservation, work, work, current));
    try std.testing.expectError(error.InitialFkRetirementWorkChanged, validate(signed, "ffffffffffffffffffffffffffffffff".*, publication, reservation, work, work, current));
    var changed_reservation = reservation;
    changed_reservation.range_id += 1;
    try std.testing.expectError(error.InitialFkRetirementReservationChanged, validate(signed, ticket.metadata_incarnation, publication, changed_reservation, work, work, current));
    var changed_root = current;
    changed_root.reporter.store_root_incarnation += 1;
    try std.testing.expectError(error.InitialFkRetirementReporterChanged, validate(signed, ticket.metadata_incarnation, publication, reservation, work, work, changed_root));
    changed_root = current;
    changed_root.reporter.reporter_incarnation += 1;
    try std.testing.expectError(error.InitialFkRetirementReporterChanged, validate(signed, ticket.metadata_incarnation, publication, reservation, work, work, changed_root));
    changed_root = current;
    changed_root.reporter.store_id += 1;
    try std.testing.expectError(error.InitialFkRetirementReporterChanged, validate(signed, ticket.metadata_incarnation, publication, reservation, work, work, changed_root));
    changed_root = current;
    changed_root.reporter.live = false;
    try std.testing.expectError(error.InitialFkRetirementReporterChanged, validate(signed, ticket.metadata_incarnation, publication, reservation, work, work, changed_root));
    changed_root = current;
    changed_root.public_key = @splat(0);
    try std.testing.expectError(error.InitialFkRetirementSigningKeyUnavailable, validate(signed, ticket.metadata_incarnation, publication, reservation, work, work, changed_root));
    changed_root.public_key = (try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(9))).public_key.toBytes();
    try std.testing.expectError(error.InvalidInitialFkRetirementSignature, validate(signed, ticket.metadata_incarnation, publication, reservation, work, work, changed_root));
}
