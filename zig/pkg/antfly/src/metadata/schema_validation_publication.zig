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

//! Compact schema-validation publication barriers. These transitions do not
//! admit a plan, verify a signature, publish a table or commit a transaction.
//! The metadata owner must authenticate its immutable cohort and physical
//! evidence in the same writer that stages both returned records. Native
//! install/release decisions are irreversible; cancellation is pre-install only.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.storage_db_relational_schema_owner_contract;
const evidence = @import("schema_validation_attestation.zig");
pub const Digest = [32]u8;
pub const max_owners = 4096;
pub const Phase = enum { validating, validated, installing, installed, releasing, succeeded, canceling, canceled };
pub const Failure = enum {
    none,
    requested,
    not_null_violation,
    foreign_key_violation,
    owner_failed,
    pub fn isConstraintViolation(self: @This()) bool {
        return self == .not_null_violation or self == .foreign_key_violation;
    }
};
pub const Stage = enum(u3) { ready, installed, released, canceled, failed };
const stage_count = @typeInfo(Stage).@"enum".field_names.len;

fn nonzero(value: anytype) bool {
    return !std.mem.allEqual(u8, value, 0);
}
fn eq(a: Digest, b: Digest) bool {
    return std.mem.eql(u8, &a, &b);
}
fn nextRevision(revision: u64) !u64 {
    return std.math.add(u64, revision, 1) catch error.InvalidGenerationPublication;
}

fn appendChain(comptime domain: []const u8, previous: Digest, owner: Owner, header: Digest) !Digest {
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update(domain);
    hasher.update(&previous);
    var ordinal: [2]u8 = undefined;
    std.mem.writeInt(u16, &ordinal, owner.ordinal, .little);
    hasher.update(&ordinal);
    hasher.update(&try evidence.bindingDigest(owner.binding));
    hasher.update(&header);
    var result: Digest = undefined;
    hasher.final(&result);
    return result;
}

pub const Head = struct {
    metadata_group_id: u64,
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    id: [16]u8,
    plan_digest: Digest,
    table_id: u64,
    owner_count: u16,
    input_count: u16 = 1,
    /// Derived once from the immutable before/after layouts. The accepting
    /// writer checks current owner capabilities without reparsing that plan.
    reader_requirements: ?local.common_schema_readers.ReaderRequirements = null,
    revision: u64 = 1,
    phase: Phase = .validating,
    failure: Failure = .none,
    ready: u16 = 0,
    installed: u16 = 0,
    released: u16 = 0,
    canceled: u16 = 0,
    install_revision: u64 = 0,
    publication_revision: u64 = 0,
    cancellation_revision: u64 = 0,
    ready_chain: Digest = @splat(0),
    installed_chain: Digest = @splat(0),
    terminal_chain: Digest = @splat(0),
    failure_digest: Digest = @splat(0),

    pub fn validate(self: Head) !void {
        if (self.reader_requirements) |required| if (!required.valid()) return error.InvalidGenerationPublication;
        if (self.metadata_group_id == 0 or self.table_id == 0 or self.revision == 0 or
            !local.metadata_incarnation.isValid(self.metadata_incarnation) or !nonzero(&self.id) or !nonzero(&self.plan_digest) or
            self.owner_count == 0 or self.owner_count > max_owners or self.input_count == 0 or self.input_count > self.owner_count or
            self.ready > self.owner_count or self.installed > self.ready or self.released > self.installed or self.canceled > self.owner_count or
            self.install_revision > self.revision or self.publication_revision > self.revision or self.cancellation_revision > self.revision or
            (self.ready != 0) != nonzero(&self.ready_chain) or (self.installed != 0) != nonzero(&self.installed_chain) or
            (self.released != 0 or self.canceled != 0) != nonzero(&self.terminal_chain) or
            self.failure.isConstraintViolation() != nonzero(&self.failure_digest))
            return error.InvalidGenerationPublication;
        const canceling = self.phase == .canceling or self.phase == .canceled;
        const installing = self.phase == .installing or self.phase == .installed or self.phase == .releasing or self.phase == .succeeded;
        const publishing = self.phase == .releasing or self.phase == .succeeded;
        if (canceling != (self.cancellation_revision != 0) or canceling != (self.failure != .none) or
            installing != (self.install_revision != 0) or publishing != (self.publication_revision != 0) or
            (self.canceled != 0 and !canceling) or (self.installed != 0 and !installing) or
            (self.released != 0 and !publishing) or (installing and self.ready != self.owner_count)) return error.InvalidGenerationPublication;
        switch (self.phase) {
            .validating => if (self.ready == self.owner_count) return error.InvalidGenerationPublication,
            .validated => if (self.ready != self.owner_count) return error.InvalidGenerationPublication,
            .installing => if (self.installed == self.owner_count) return error.InvalidGenerationPublication,
            .installed => if (self.installed != self.owner_count) return error.InvalidGenerationPublication,
            .releasing => if (self.installed != self.owner_count or self.released == self.owner_count) return error.InvalidGenerationPublication,
            .succeeded => if (self.released != self.owner_count) return error.InvalidGenerationPublication,
            .canceling => if (self.canceled == self.owner_count) return error.InvalidGenerationPublication,
            .canceled => if (self.canceled != self.owner_count) return error.InvalidGenerationPublication,
        }
        if (publishing and self.publication_revision <= self.install_revision) return error.InvalidGenerationPublication;
    }

    fn decision(self: Head, comptime domain: []const u8, barrier_revision: u64, installed_chain: Digest) !Digest {
        try self.validate();
        if (barrier_revision == 0) return error.SchemaValidationPublicationRequired;
        var hasher = std.crypto.hash.Blake3.init(.{});
        hasher.update(domain);
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hasher, &.{});
        try std.json.Stringify.value(.{
            .metadata_group_id = self.metadata_group_id,
            .metadata_incarnation = self.metadata_incarnation,
            .id = self.id,
            .plan_digest = self.plan_digest,
            .table_id = self.table_id,
            .owner_count = self.owner_count,
            .input_count = self.input_count,
            .reader_requirements = self.reader_requirements,
            .barrier_revision = barrier_revision,
            .ready_chain = self.ready_chain,
            .installed_chain = installed_chain,
            .failure = self.failure,
            .failure_digest = self.failure_digest,
        }, .{}, &writer.writer);
        var result: Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }
    pub fn installDecision(self: Head) !Digest {
        return self.decision("antfly/schema-validation-install-decision/v1", self.install_revision, @splat(0));
    }
    pub fn publicationDecision(self: Head) !Digest {
        return self.decision("antfly/schema-validation-publication-decision/v1", self.publication_revision, self.installed_chain);
    }
    pub fn cancellationDecision(self: Head) !Digest {
        return self.decision("antfly/schema-validation-cancel-decision/v1", self.cancellation_revision, @splat(0));
    }
    pub fn decideInstall(self: Head, expected_revision: u64) !Head {
        try self.validate();
        if (self.revision != expected_revision or self.phase != .validated) return error.GenerationPublicationChanged;
        var next = self;
        next.revision = try nextRevision(self.revision);
        next.install_revision = next.revision;
        next.phase = .installing;
        try next.validate();
        return next;
    }
    /// The caller stages the exact immutable table successor in THIS writer.
    /// Returning this value alone never publishes or releases any owner.
    pub fn publish(self: Head, expected_revision: u64) !Head {
        try self.validate();
        if (self.revision != expected_revision or self.phase != .installed) return error.GenerationPublicationChanged;
        var next = self;
        next.revision = try nextRevision(self.revision);
        next.publication_revision = next.revision;
        next.phase = .releasing;
        try next.validate();
        return next;
    }
    pub fn cancel(self: Head, expected_revision: u64, failure: Failure) !Head {
        try self.validate();
        if (self.revision != expected_revision or failure == .none or failure.isConstraintViolation()) return error.GenerationPublicationChanged;
        if (self.phase != .validating and self.phase != .validated) return error.SchemaValidationPublicationRequired;
        var next = self;
        next.revision = try nextRevision(self.revision);
        next.cancellation_revision = next.revision;
        next.failure = failure;
        next.phase = .canceling;
        try next.validate();
        return next;
    }
    pub fn encode(self: Head) !HeadRecord {
        try self.validate();
        return HeadRecord.init(self);
    }
    pub fn decode(bytes: []const u8) !Head {
        const result = try HeadRecord.decode(bytes);
        try result.validate();
        return result;
    }
};

pub const Owner = struct {
    ordinal: u16,
    /// Dense immutable ASVI blob ordinal, deduplicated by complete digest.
    input_ordinal: u16 = 0,
    binding: contract.Binding,
    /// Captured from independently authenticated owner preparation, not
    /// adopted from the first ACK. A different physical root requires a
    /// separate verified adoption; signatures alone cannot replace this cut.
    physical_root_incarnation: u128,
    /// Zero is the immutable preparation root. Adoption advances this epoch
    /// without rewriting the preparation, phase headers or prior signatures.
    root_epoch: u64 = 0,
    root_origin: u128 = 0,
    root_digest: Digest = @splat(0),
    proof_epochs: [stage_count]u64 = @splat(0),
    logical: [stage_count]Digest = @splat(@splat(0)),
    headers: [stage_count]Digest = @splat(@splat(0)),
    proofs: [stage_count]Digest = @splat(@splat(0)),
    accepted_revisions: [stage_count]u64 = @splat(0),

    pub fn validate(self: Owner, head: Head) !void {
        try head.validate();
        try self.binding.validate();
        if (self.ordinal >= head.owner_count or self.input_ordinal >= head.input_count or self.physical_root_incarnation == 0 or
            !std.mem.eql(u8, &self.binding.id, &head.id) or !eq(self.binding.plan_digest, head.plan_digest) or
            self.binding.fence.namespace.table_id != head.table_id) return error.InvalidGenerationPublication;
        if ((self.root_epoch != 0) != (self.root_origin != 0) or
            (self.root_epoch != 0) != nonzero(&self.root_digest)) return error.InvalidGenerationPublication;
        for (self.proof_epochs, 0..) |epoch, i| {
            if (epoch > self.root_epoch or (epoch != 0 and !nonzero(&self.headers[i]))) return error.InvalidGenerationPublication;
        }
        for (self.logical, self.headers, self.proofs, self.accepted_revisions) |logical, header, proof, revision|
            if (nonzero(&logical) != nonzero(&header) or nonzero(&logical) != nonzero(&proof) or
                nonzero(&logical) != (revision != 0) or revision > head.revision) return error.InvalidGenerationPublication;
        if ((self.has(.installed) and !self.has(.ready)) or (self.has(.released) and !self.has(.installed)) or
            (self.has(.canceled) and self.has(.installed)) or (self.has(.failed) and self.has(.ready))) return error.InvalidGenerationPublication;
        const ready_revision = self.accepted_revisions[@backingInt(Stage.ready)];
        const installed_revision = self.accepted_revisions[@backingInt(Stage.installed)];
        if ((self.has(.ready) and ((head.install_revision != 0 and ready_revision >= head.install_revision) or
            (head.cancellation_revision != 0 and ready_revision >= head.cancellation_revision))) or
            (self.has(.installed) and (head.install_revision == 0 or installed_revision <= head.install_revision or
                (head.publication_revision != 0 and installed_revision >= head.publication_revision))) or
            (self.has(.released) and (head.publication_revision == 0 or self.accepted_revisions[@backingInt(Stage.released)] <= head.publication_revision)) or
            (self.has(.canceled) and (head.cancellation_revision == 0 or self.accepted_revisions[@backingInt(Stage.canceled)] <= head.cancellation_revision)) or
            (self.has(.failed) and self.accepted_revisions[@backingInt(Stage.failed)] != head.cancellation_revision)) return error.InvalidGenerationPublication;
    }
    pub fn has(self: Owner, stage: Stage) bool {
        return nonzero(&self.headers[@backingInt(stage)]);
    }
    pub fn initialRoot(self: Owner) u128 {
        return if (self.root_epoch == 0) self.physical_root_incarnation else self.root_origin;
    }
    fn latest(self: Owner) ?Stage {
        for ([_]Stage{ .released, .canceled, .installed, .failed, .ready }) |stage| if (self.has(stage)) return stage;
        return null;
    }
    pub fn encode(self: Owner, head: Head) !OwnerRecord {
        try self.validate(head);
        return OwnerRecord.init(self);
    }
    pub fn decode(bytes: []const u8, head: Head) !Owner {
        const result = try OwnerRecord.decode(bytes);
        try result.validate(head);
        return result;
    }
};

/// Logical scheduling disposition only, not authenticated execution authority.
/// The receiving owner must obtain this from a current metadata read, verify
/// its own physical root and still apply the exact immutable binding.
pub const OwnerPermission = enum { execute, acknowledged };

/// Scheduling from durable facts, never execution permission. A worker must
/// independently rejoin current owner authority before executing an action.
pub const OwnerWork = union(enum) { idle, execute: contract.Action, ack: Stage };

pub fn nextOwnerWork(head: Head, owner: Owner, physical: ?contract.Header, directory: ?contract.TransactionDebtObservation) !OwnerWork {
    try owner.validate(head);
    if (head.phase == .succeeded or head.phase == .canceled) return .idle;
    var header = physical;
    if (header) |value| {
        try value.validate();
        if (!std.meta.eql(value.binding, owner.binding)) {
            // A finished older job may be replaced, but an accepted phase of
            // THIS job cannot be recreated from an unrelated terminal header.
            for (std.meta.tags(Stage)) |stage| if (owner.has(stage)) return error.SchemaValidationRuntimeRecoveryRequired;
            if ((value.phase != .released and value.phase != .canceled) or
                value.binding.fence.admission_epoch >= owner.binding.fence.admission_epoch) return error.SchemaValidationJobChanged;
            header = null;
        }
    }
    if (header) |value| {
        if ((owner.has(.released) and value.phase != .released) or
            (owner.has(.canceled) and value.phase != .canceled) or
            (owner.has(.installed) and value.phase != .installed and value.phase != .released) or
            (owner.has(.ready) and (value.phase == .fencing or value.phase == .scanning)) or
            (owner.has(.failed) and value.phase != .failed and value.phase != .canceled)) return error.SchemaValidationRuntimeRecoveryRequired;
    } else for (std.meta.tags(Stage)) |stage| if (owner.has(stage)) return error.SchemaValidationRuntimeRecoveryRequired;
    const action: contract.Action = if (head.phase == .canceling) cancel: {
        if (owner.has(.canceled)) return .idle;
        if (header) |value| {
            if (value.phase == .canceled) return .{ .ack = .canceled };
            if (value.phase == .installed or value.phase == .released) return error.SchemaValidationPublicationRequired;
            if (value.phase == .fencing) {
                const debt = directory orelse return error.SchemaValidationNotReady;
                try debt.validate();
                if (debt.retired or (debt.ready and debt.debt != 0)) return error.SchemaValidationJobChanged;
                if (!debt.ready) break :cancel .reconcile_transactions;
            }
        }
        break :cancel .cancel;
    } else if (header) |value| switch (value.phase) {
        .fencing => fencing: {
            const debt = directory orelse return error.SchemaValidationNotReady;
            try debt.validate();
            if (debt.retired or (debt.ready and debt.debt != 0)) return error.SchemaValidationJobChanged;
            break :fencing if (debt.ready) .freeze else .reconcile_transactions;
        },
        .scanning => .page,
        .ready => ready: {
            if (!owner.has(.ready)) return .{ .ack = .ready };
            if (head.install_revision == 0) return .idle;
            break :ready .install;
        },
        .installed => installed: {
            if (!owner.has(.installed)) return .{ .ack = .installed };
            if (head.publication_revision == 0) return .idle;
            break :installed .release;
        },
        .released => return if (owner.has(.released)) .idle else .{ .ack = .released },
        .failed => return if (owner.has(.failed)) .idle else .{ .ack = .failed },
        .canceling, .canceled => return error.SchemaValidationCancellationRequired,
    } else .begin;
    return if (try ownerPermission(head, owner, action) == .execute) .{ .execute = action } else .idle;
}

pub fn ownerPermission(head: Head, owner: Owner, action: contract.Action) !OwnerPermission {
    try owner.validate(head);
    switch (action) {
        .reconcile_transactions => {
            // Canceling an admitted but not-yet-drained owner still needs
            // independent bounded reconciliation before it can release its
            // fence. Metadata cancellation must not remove that recovery path.
            if (head.phase == .canceling and !owner.has(.ready) and !owner.has(.canceled) and !owner.has(.failed)) return .execute;
            return ownerPermission(head, owner, .page);
        },
        .begin, .freeze, .page => {
            if (head.phase == .canceling or head.phase == .canceled) return error.GenerationPublicationChanged;
            if (owner.has(.ready)) return .acknowledged;
            if (head.phase != .validating or owner.has(.failed) or owner.has(.canceled)) return error.GenerationPublicationChanged;
        },
        .install => {
            if (head.install_revision == 0 or head.phase == .canceling or head.phase == .canceled or !owner.has(.ready)) return error.SchemaValidationPublicationRequired;
            if (owner.has(.installed)) return .acknowledged;
            if (head.phase != .installing) return error.GenerationPublicationChanged;
        },
        .release => {
            if (head.publication_revision == 0 or !owner.has(.installed)) return error.SchemaValidationPublicationRequired;
            if (owner.has(.released)) return .acknowledged;
            if (head.phase != .releasing) return error.GenerationPublicationChanged;
        },
        .cancel => {
            if (head.phase != .canceling and head.phase != .canceled) return error.SchemaValidationPublicationRequired;
            if (owner.has(.canceled)) return .acknowledged;
            if (head.phase != .canceling or owner.has(.installed)) return error.GenerationPublicationChanged;
        },
    }
    return .execute;
}

/// Canonical, checksummed fixed-capacity records. ACK apply can stage bounded
/// stack-owned postimages without a heap allocation or re-encoding the plan.
pub const HeadRecord = Record(Head, "SVPH", 2048);
pub const OwnerRecord = Record(Owner, "SVPO", 16 * 1024);
pub fn Record(comptime T: type, comptime magic: *const [4]u8, comptime max_body: usize) type {
    return struct {
        pub const max_wire_bytes = max_body + 44;
        bytes: [max_wire_bytes]u8 = undefined,
        len: usize = 0,

        pub fn slice(self: *const @This()) []const u8 {
            return self.bytes[0..self.len];
        }
        pub fn init(value: T) !@This() {
            var result: @This() = .{};
            var writer = std.Io.Writer.fixed(result.bytes[12..][0..max_body]);
            std.json.Stringify.value(value, .{}, &writer) catch return error.InvalidGenerationPublication;
            const length = writer.buffered().len;
            result.len = length + 44;
            @memcpy(result.bytes[0..4], magic);
            @memcpy(result.bytes[4..8], "\x01\x00\x00\x00");
            std.mem.writeInt(u32, result.bytes[8..12], @intCast(length), .little);
            @memcpy(result.bytes[result.len - 32 ..][0..32], &contract.hash(result.bytes[0 .. result.len - 32]));
            return result;
        }
        pub fn decode(bytes: []const u8) !T {
            if (bytes.len < 44 or bytes.len > max_wire_bytes or !std.mem.eql(u8, bytes[0..4], magic) or
                !std.mem.eql(u8, bytes[4..8], "\x01\x00\x00\x00") or
                std.mem.readInt(u32, bytes[8..12], .little) != bytes.len - 44 or
                !eq(contract.hash(bytes[0 .. bytes.len - 32]), bytes[bytes.len - 32 ..][0..32].*)) return error.InvalidGenerationPublication;
            var scratch: [max_body * 8]u8 = undefined;
            var fixed = std.heap.FixedBufferAllocator.init(&scratch);
            const value = std.json.parseFromSliceLeaky(T, fixed.allocator(), bytes[12 .. bytes.len - 32], .{ .ignore_unknown_fields = false }) catch return error.InvalidGenerationPublication;
            const canonical = try init(value);
            if (!std.mem.eql(u8, canonical.slice(), bytes)) return error.InvalidGenerationPublication;
            return value;
        }
    };
}

pub const Update = struct { head: Head, owner: Owner, changed: bool };

/// Only metadata's domain-specific physical verifier produces the evidence
/// accepted by its writer. This deterministic reducer cannot authenticate an
/// arbitrary caller-constructed VerifiedObservation or grant plan admission.
pub fn acknowledge(head: Head, owner: Owner, expected_revision: u64, observed: evidence.VerifiedObservation) !Update {
    try owner.validate(head);
    try observed.header.validate();
    if (!std.meta.eql(owner.binding, observed.header.binding) or
        !eq(try evidence.logicalHeaderDigest(observed.header), observed.logical_digest) or !nonzero(&observed.root_commitment)) return error.SchemaValidationJobChanged;
    if (owner.physical_root_incarnation != observed.physical_root_incarnation) return error.SchemaValidationRuntimeRecoveryRequired;
    const stage: Stage = switch (observed.header.phase) {
        .ready => .ready,
        .installed => .installed,
        .released => .released,
        .canceled => .canceled,
        .failed => .failed,
        else => return error.SchemaValidationNotReady,
    };
    if (owner.binding.purpose == .foreign_key_source and (stage == .ready or stage == .installed or stage == .released or stage == .failed)) {
        const coverage = observed.header.transaction_coverage orelse return error.ForeignKeyValidationRequired;
        // Release is immutable publication history. Its original transaction
        // root must remain intact; current-root adoption/authority is checked
        // independently before this reducer receives terminal evidence.
        if (owner.binding.authority == .native and stage != .released and coverage.physical_root != observed.physical_root_incarnation) return error.SchemaValidationRuntimeRecoveryRequired;
    }
    const index = @backingInt(stage);
    const header_digest = try observed.header.digest();
    if (owner.has(stage)) {
        if (!eq(owner.logical[index], observed.logical_digest) or !eq(owner.headers[index], header_digest)) return error.SchemaValidationJobChanged;
        if (eq(owner.proofs[index], observed.root_commitment)) return .{ .head = head, .owner = owner, .changed = false };
        // Re-attesting an obsolete phase cannot replace newer physical truth.
        if (owner.latest().? != stage or head.revision != expected_revision) return error.GenerationPublicationChanged;
        var refreshed = owner;
        refreshed.proofs[index] = observed.root_commitment;
        refreshed.proof_epochs[index] = owner.root_epoch;
        var next = head;
        next.revision = try nextRevision(head.revision);
        try refreshed.validate(next);
        return .{ .head = next, .owner = refreshed, .changed = true };
    }
    if (head.revision != expected_revision) return error.GenerationPublicationChanged;
    var next = head;
    next.revision = try nextRevision(head.revision);
    switch (stage) {
        .ready => {
            if (head.phase != .validating or owner.has(.failed) or owner.has(.canceled)) return error.GenerationPublicationChanged;
            next.ready += 1;
            next.ready_chain = try appendChain("antfly/schema-validation-ready-chain/v1", head.ready_chain, owner, header_digest);
            if (next.ready == next.owner_count) next.phase = .validated;
        },
        .installed => {
            const installed = observed.header.installation.?;
            if (head.phase != .installing or !owner.has(.ready) or
                !eq(installed.ready_digest, owner.headers[@backingInt(Stage.ready)]) or
                !eq(installed.decision, try head.installDecision())) return error.SchemaValidationJobChanged;
            next.installed += 1;
            next.installed_chain = try appendChain("antfly/schema-validation-installed-chain/v1", head.installed_chain, owner, header_digest);
            if (next.installed == next.owner_count) next.phase = .installed;
        },
        .released => {
            const released = observed.header.release.?;
            if (head.phase != .releasing or !owner.has(.installed) or
                !eq(released.installed_digest, owner.headers[@backingInt(Stage.installed)]) or
                !eq(released.publication, try head.publicationDecision()) or
                !eq(observed.header.installation.?.decision, try head.installDecision())) return error.SchemaValidationJobChanged;
            next.released += 1;
            next.terminal_chain = try appendChain("antfly/schema-validation-released-chain/v1", head.terminal_chain, owner, header_digest);
            if (next.released == next.owner_count) next.phase = .succeeded;
        },
        .canceled => {
            if (head.phase != .canceling or owner.has(.installed)) return error.SchemaValidationPublicationRequired;
            next.canceled += 1;
            next.terminal_chain = try appendChain("antfly/schema-validation-canceled-chain/v1", head.terminal_chain, owner, header_digest);
            if (next.canceled == next.owner_count) next.phase = .canceled;
        },
        .failed => {
            if (head.phase != .validating or owner.has(.ready) or owner.has(.canceled)) return error.GenerationPublicationChanged;
            next.failure = if (owner.binding.purpose == .foreign_key_source) .foreign_key_violation else .not_null_violation;
            next.failure_digest = try appendChain("antfly/schema-validation-failed-owner/v1", @splat(0), owner, header_digest);
            next.cancellation_revision = next.revision;
            next.phase = .canceling;
        },
    }
    var accepted = owner;
    accepted.logical[index] = observed.logical_digest;
    accepted.headers[index] = header_digest;
    accepted.proofs[index] = observed.root_commitment;
    accepted.proof_epochs[index] = owner.root_epoch;
    accepted.accepted_revisions[index] = next.revision;
    try accepted.validate(next);
    return .{ .head = next, .owner = accepted, .changed = true };
}

/// Checkpoint/recovery audit, never a hot ACK scan. A single bounded ordinal
/// workspace reconstructs the actual accepted order and every chain. Missing,
/// duplicate and cross-phase records cannot manufacture a completed barrier.
pub fn validateCohort(head: Head, owners: []const Owner) !void {
    try head.validate();
    if (owners.len != head.owner_count) return error.InvalidGenerationPublication;
    for (owners, 0..) |owner, ordinal| {
        try owner.validate(head);
        if (owner.ordinal != ordinal) return error.InvalidGenerationPublication;
        inline for (.{ "before_version", "after_version", "columns", "before_schema", "after_schema", "before_public", "after_public", "projection_identity" }) |field|
            if (!std.meta.eql(@field(owner.binding, field), @field(owners[0].binding, field))) return error.InvalidGenerationPublication;
    }
    var ordinals: [max_owners]u16 = undefined;
    for ([_]Stage{ .ready, .installed, .released, .canceled, .failed }) |stage| {
        const index = @backingInt(stage);
        var count: usize = 0;
        for (owners) |owner| if (owner.has(stage)) {
            ordinals[count] = owner.ordinal;
            count += 1;
        };
        const Order = struct {
            owners: []const Owner,
            index: usize,
            fn less(self: @This(), a: u16, b: u16) bool {
                return self.owners[a].accepted_revisions[self.index] < self.owners[b].accepted_revisions[self.index];
            }
        };
        std.sort.pdq(u16, ordinals[0..count], Order{ .owners = owners, .index = index }, Order.less);
        var chain: Digest = @splat(0);
        var prior: u64 = 0;
        for (ordinals[0..count]) |ordinal| {
            const owner = owners[ordinal];
            const revision = owner.accepted_revisions[index];
            if (revision <= prior) return error.InvalidGenerationPublication;
            prior = revision;
            chain = switch (stage) {
                .ready => try appendChain("antfly/schema-validation-ready-chain/v1", chain, owner, owner.headers[index]),
                .installed => try appendChain("antfly/schema-validation-installed-chain/v1", chain, owner, owner.headers[index]),
                .released => try appendChain("antfly/schema-validation-released-chain/v1", chain, owner, owner.headers[index]),
                .canceled => try appendChain("antfly/schema-validation-canceled-chain/v1", chain, owner, owner.headers[index]),
                .failed => try appendChain("antfly/schema-validation-failed-owner/v1", chain, owner, owner.headers[index]),
            };
        }
        const expected_count: usize = switch (stage) {
            .ready => head.ready,
            .installed => head.installed,
            .released => head.released,
            .canceled => head.canceled,
            .failed => @intFromBool(head.failure.isConstraintViolation()),
        };
        if (count != expected_count) return error.InvalidGenerationPublication;
        const expected_chain: Digest = switch (stage) {
            .ready => head.ready_chain,
            .installed => head.installed_chain,
            .released => if (head.released == 0) @splat(0) else head.terminal_chain,
            .canceled => if (head.canceled == 0) @splat(0) else head.terminal_chain,
            .failed => head.failure_digest,
        };
        if (!eq(chain, expected_chain)) return error.InvalidGenerationPublication;
    }
    for (owners, 0..) |_, i| ordinals[i] = @intCast(i);
    const Groups = struct {
        fn less(items: []const Owner, a: u16, b: u16) bool {
            return items[a].binding.fence.owner_group_id < items[b].binding.fence.owner_group_id;
        }
    };
    std.sort.pdq(u16, ordinals[0..owners.len], owners, Groups.less);
    for (ordinals[0..owners.len], 0..) |ordinal, i| if (i != 0 and
        owners[ordinal].binding.fence.owner_group_id == owners[ordinals[i - 1]].binding.fence.owner_group_id) return error.InvalidGenerationPublication;
}

const Fixture = struct {
    fn head() Head {
        return .{ .metadata_group_id = 1, .metadata_incarnation = @splat('1'), .id = @splat(1), .plan_digest = @splat(2), .table_id = 7, .owner_count = 2 };
    }
    fn owner(ordinal: u16) Owner {
        const h = head();
        return .{ .ordinal = ordinal, .physical_root_incarnation = 17 + @as(u128, ordinal), .binding = .{
            .id = h.id,
            .plan_digest = h.plan_digest,
            .owner = @splat(3),
            .fence = .{ .transition_id = 4, .attempt = 1, .admission_epoch = 1, .owner_group_id = 72 + @as(u64, ordinal), .peer_group_id = 72 + @as(u64, ordinal), .namespace = .{ .table_id = h.table_id, .shard_id = 172 + @as(u64, ordinal), .range_id = 272 + @as(u64, ordinal) }, .catalog_digest = @splat(5), .role = .schema_validation },
            .authority = .native,
            .before_version = 1,
            .after_version = 2,
            .columns = 1,
            .before_schema = @splat(6),
            .after_schema = @splat(7),
            .before_public = @splat(8),
            .after_public = @splat(9),
            .after_catalog = @splat(10),
            .physical_catalog = @splat(11),
            .inputs = @splat(12),
            .projection_identity = @splat(13),
        } };
    }
    fn position(o: Owner, sequence: u64) local.storage_db_receipt_position.Position {
        return .{ .native = .{ .namespace = o.binding.fence.namespace, .sequence = sequence } };
    }
    fn ready(o: Owner) contract.Header {
        return .{ .binding = o.binding, .phase = .ready, .admitted = position(o, 1), .frozen = position(o, 2), .updated = position(o, 3), .revision = 3, .records = 2, .rows_scanned = 2, .cursor_digest = contract.hash(""), .chain = @splat(14), .last_effect = @splat(15) };
    }
    fn installed(o: Owner, h: Head) !contract.Header {
        var value = ready(o);
        const ready_digest = try value.digest();
        value.installation = .{ .decision = try h.installDecision(), .ready_digest = ready_digest, .ready_position = value.updated, .ready_last_effect = value.last_effect, .physical_catalog = @splat(16), .activation = @splat(17), .position = position(o, 4) };
        value.phase = .installed;
        value.revision += 1;
        value.updated = position(o, 4);
        value.last_effect = @splat(18);
        try value.validate();
        return value;
    }
    fn released(o: Owner, h: Head) !contract.Header {
        var value = try installed(o, h);
        const installed_digest = try value.digest();
        value.release = .{ .publication = try h.publicationDecision(), .installed_digest = installed_digest, .installed_last_effect = value.last_effect };
        value.phase = .released;
        value.revision += 1;
        value.updated = position(o, 5);
        value.last_effect = @splat(19);
        try value.validate();
        return value;
    }
    fn proof(o: Owner, header: contract.Header) !evidence.VerifiedObservation {
        return .{ .header = header, .logical_digest = try evidence.logicalHeaderDigest(header), .root_commitment = contract.hash(&try header.digest()), .physical_root_incarnation = o.physical_root_incarnation };
    }
    fn apply(h: *Head, o: *Owner, header: contract.Header) !void {
        const result = try acknowledge(h.*, o.*, h.revision, try proof(o.*, header));
        h.* = result.head;
        o.* = result.owner;
        try std.testing.expect(result.changed);
    }
};

test "schema validation publication owner work resumes whole cohorts across every durable phase" {
    var head = Fixture.head();
    var owners = [_]Owner{ Fixture.owner(0), Fixture.owner(1) };
    for (&owners) |*owner| {
        try std.testing.expectEqualDeep(OwnerWork{ .execute = .begin }, try nextOwnerWork(head, owner.*, null, null));
        var header = Fixture.ready(owner.*);
        header.phase = .fencing;
        header.frozen = null;
        header.records = 0;
        header.rows_scanned = 0;
        var directory: contract.TransactionDebtObservation = .{ .revision = 1, .debt = 0, .ready = false, .retired = false, .expected = @splat(71) };
        // An empty but unfinished directory still requires bounded work.
        try std.testing.expectEqualDeep(OwnerWork{ .execute = .reconcile_transactions }, try nextOwnerWork(head, owner.*, header, directory));
        directory.ready = true;
        try std.testing.expectEqualDeep(OwnerWork{ .execute = .freeze }, try nextOwnerWork(head, owner.*, header, directory));
        header.phase = .scanning;
        header.frozen = Fixture.position(owner.*, 2);
        try std.testing.expectEqualDeep(OwnerWork{ .execute = .page }, try nextOwnerWork(head, owner.*, header, directory));
        const ready = Fixture.ready(owner.*);
        try std.testing.expectEqualDeep(OwnerWork{ .ack = .ready }, try nextOwnerWork(head, owner.*, ready, directory));
        try Fixture.apply(&head, owner, ready);
        try std.testing.expectEqualDeep(OwnerWork.idle, try nextOwnerWork(head, owner.*, ready, directory));
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, nextOwnerWork(head, owner.*, header, directory));
        try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, nextOwnerWork(head, owner.*, null, null));
    }
    head = try head.decideInstall(head.revision);
    for (&owners) |*owner| {
        try std.testing.expectEqualDeep(OwnerWork{ .execute = .install }, try nextOwnerWork(head, owner.*, Fixture.ready(owner.*), null));
        const installed = try Fixture.installed(owner.*, head);
        try std.testing.expectEqualDeep(OwnerWork{ .ack = .installed }, try nextOwnerWork(head, owner.*, installed, null));
        try Fixture.apply(&head, owner, installed);
        try std.testing.expectEqualDeep(OwnerWork.idle, try nextOwnerWork(head, owner.*, installed, null));
    }
    head = try head.publish(head.revision);
    for (&owners) |*owner| {
        try std.testing.expectEqualDeep(OwnerWork{ .execute = .release }, try nextOwnerWork(head, owner.*, try Fixture.installed(owner.*, head), null));
        const released = try Fixture.released(owner.*, head);
        try std.testing.expectEqualDeep(OwnerWork{ .ack = .released }, try nextOwnerWork(head, owner.*, released, null));
        try Fixture.apply(&head, owner, released);
        try std.testing.expectEqualDeep(OwnerWork.idle, try nextOwnerWork(head, owner.*, released, null));
    }
    try std.testing.expectEqual(Phase.succeeded, head.phase);
    head = try Fixture.head().cancel(Fixture.head().revision, .requested);
    for (&owners, 0..) |*owner, ordinal| {
        owner.* = Fixture.owner(@intCast(ordinal));
        try std.testing.expectEqualDeep(OwnerWork{ .execute = .cancel }, try nextOwnerWork(head, owner.*, null, null));
        var canceled = Fixture.ready(owner.*);
        canceled.phase = .canceled;
        try std.testing.expectEqualDeep(OwnerWork{ .ack = .canceled }, try nextOwnerWork(head, owner.*, canceled, null));
        try Fixture.apply(&head, owner, canceled);
    }
    try std.testing.expectEqual(Phase.canceled, head.phase);
}

test "schema validation publication owner permissions enforce every irreversible phase barrier" {
    var head = Fixture.head();
    var owners = [_]Owner{ Fixture.owner(0), Fixture.owner(1) };
    for (owners) |owner| {
        inline for (.{ contract.Action.begin, .reconcile_transactions, .freeze, .page }) |action| try std.testing.expectEqual(OwnerPermission.execute, try ownerPermission(head, owner, action));
        inline for (.{ contract.Action.install, .release, .cancel }) |action| try std.testing.expectError(error.SchemaValidationPublicationRequired, ownerPermission(head, owner, action));
    }
    for (&owners, 0..) |*owner, i| {
        try Fixture.apply(&head, owner, Fixture.ready(owner.*));
        inline for (.{ contract.Action.begin, .reconcile_transactions, .freeze, .page }) |action| try std.testing.expectEqual(OwnerPermission.acknowledged, try ownerPermission(head, owner.*, action));
        try std.testing.expectError(error.SchemaValidationPublicationRequired, ownerPermission(head, owner.*, .install));
        if (i == 0) try std.testing.expectEqual(OwnerPermission.execute, try ownerPermission(head, owners[1], .page));
    }
    head = try head.decideInstall(head.revision);
    for (&owners) |*owner| {
        try std.testing.expectEqual(OwnerPermission.execute, try ownerPermission(head, owner.*, .install));
        try std.testing.expectError(error.SchemaValidationPublicationRequired, ownerPermission(head, owner.*, .release));
        try std.testing.expectError(error.SchemaValidationPublicationRequired, ownerPermission(head, owner.*, .cancel));
        try Fixture.apply(&head, owner, try Fixture.installed(owner.*, head));
        try std.testing.expectEqual(OwnerPermission.acknowledged, try ownerPermission(head, owner.*, .install));
        // Physical installation alone cannot release a write fence: the
        // complete all-owner catalog publication must commit first.
        try std.testing.expectError(error.SchemaValidationPublicationRequired, ownerPermission(head, owner.*, .release));
    }
    head = try head.publish(head.revision);
    for (&owners) |*owner| {
        try std.testing.expectEqual(OwnerPermission.execute, try ownerPermission(head, owner.*, .release));
        try Fixture.apply(&head, owner, try Fixture.released(owner.*, head));
        try std.testing.expectEqual(OwnerPermission.acknowledged, try ownerPermission(head, owner.*, .release));
    }
    for (owners) |owner| {
        inline for (.{ contract.Action.begin, .reconcile_transactions, .freeze, .page, .install, .release }) |action| try std.testing.expectEqual(OwnerPermission.acknowledged, try ownerPermission(head, owner, action));
        try std.testing.expectError(error.SchemaValidationPublicationRequired, ownerPermission(head, owner, .cancel));
    }
    var foreign = owners[0];
    foreign.binding.plan_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, ownerPermission(head, foreign, .release));
}

test "schema validation publication canceled owners cannot reopen work or obtain install permission" {
    var head = Fixture.head();
    var owners = [_]Owner{ Fixture.owner(0), Fixture.owner(1) };
    try Fixture.apply(&head, &owners[0], Fixture.ready(owners[0]));
    head = try head.cancel(head.revision, .requested);
    try std.testing.expectEqual(OwnerPermission.execute, try ownerPermission(head, owners[1], .reconcile_transactions));
    try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(head, owners[0], .reconcile_transactions));
    for (&owners) |*owner| {
        inline for (.{ contract.Action.begin, .freeze, .page }) |action| try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(head, owner.*, action));
        inline for (.{ contract.Action.install, .release }) |action| try std.testing.expectError(error.SchemaValidationPublicationRequired, ownerPermission(head, owner.*, action));
        try std.testing.expectEqual(OwnerPermission.execute, try ownerPermission(head, owner.*, .cancel));
        var canceled = Fixture.ready(owner.*);
        canceled.phase = .canceled;
        canceled.updated = Fixture.position(owner.*, 6);
        canceled.revision = 6;
        try Fixture.apply(&head, owner, canceled);
        try std.testing.expectEqual(OwnerPermission.acknowledged, try ownerPermission(head, owner.*, .cancel));
    }
    try std.testing.expectEqual(Phase.canceled, head.phase);
    for (owners) |owner| inline for (.{ contract.Action.begin, .reconcile_transactions, .freeze, .page }) |action| try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(head, owner, action));
}

test "schema validation publication requires complete owner barriers and preserves decisions across reattestation" {
    // Deterministic reducer fixture only. Signature/enrollment/physical scans
    // are separately verified by metadata's real-store and native campaigns.
    var head = Fixture.head();
    var owners = [_]Owner{ Fixture.owner(0), Fixture.owner(1) };
    try std.testing.expectError(error.GenerationPublicationChanged, head.decideInstall(head.revision));
    for (&owners) |*owner| try Fixture.apply(&head, owner, Fixture.ready(owner.*));
    try validateCohort(head, &owners);
    try std.testing.expectEqual(Phase.validated, head.phase);
    const before_duplicate = head;
    const duplicate = try acknowledge(head, owners[0], 1, try Fixture.proof(owners[0], Fixture.ready(owners[0])));
    try std.testing.expect(!duplicate.changed);
    try std.testing.expectEqualDeep(before_duplicate, duplicate.head);
    try std.testing.expectError(error.GenerationPublicationChanged, head.publish(head.revision));
    head = try head.decideInstall(head.revision);
    try std.testing.expectError(error.SchemaValidationPublicationRequired, head.cancel(head.revision, .requested));
    const install_decision = try head.installDecision();
    var refreshed = try Fixture.proof(owners[0], Fixture.ready(owners[0]));
    refreshed.root_commitment[0] ^= 1;
    const changed = try acknowledge(head, owners[0], head.revision, refreshed);
    head = changed.head;
    owners[0] = changed.owner;
    try std.testing.expectEqual(@as(u16, 2), head.ready);
    try std.testing.expectEqualSlices(u8, &install_decision, &try head.installDecision());
    var wrong_install = try Fixture.installed(owners[0], head);
    wrong_install.installation.?.decision[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, acknowledge(head, owners[0], head.revision, try Fixture.proof(owners[0], wrong_install)));
    for (&owners, 0..) |*owner, i| {
        try Fixture.apply(&head, owner, try Fixture.installed(owner.*, head));
        if (i == 0) try std.testing.expectError(error.GenerationPublicationChanged, head.publish(head.revision));
    }
    try std.testing.expectEqual(Phase.installed, head.phase);
    head = try head.publish(head.revision);
    const publication_decision = try head.publicationDecision();
    try std.testing.expect(!eq(publication_decision, install_decision));
    for (&owners) |*owner| try Fixture.apply(&head, owner, try Fixture.released(owner.*, head));
    try std.testing.expectEqual(Phase.succeeded, head.phase);
    try validateCohort(head, &owners);
    try std.testing.expectEqual(@as(u16, 2), head.released);
    try std.testing.expectEqualSlices(u8, &publication_decision, &try head.publicationDecision());
    const last = try Fixture.proof(owners[1], try Fixture.released(owners[1], head));
    const repeated = try acknowledge(head, owners[1], 1, last);
    try std.testing.expect(!repeated.changed);
    var foreign_root = last;
    foreign_root.physical_root_incarnation += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, acknowledge(head, owners[1], head.revision, foreign_root));
    refreshed = try Fixture.proof(owners[0], Fixture.ready(owners[0]));
    try std.testing.expectError(error.GenerationPublicationChanged, acknowledge(head, owners[0], head.revision, refreshed));
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, head, .{});
    defer std.testing.allocator.free(encoded);
    var decoded = try std.json.parseFromSlice(Head, std.testing.allocator, encoded, .{});
    defer decoded.deinit();
    try decoded.value.validate();
    try std.testing.expectEqualDeep(head, decoded.value);
    var missing = owners;
    missing[0].headers[@backingInt(Stage.ready)][0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, validateCohort(head, &missing));
    var corrupt_head = head;
    corrupt_head.ready_chain[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, validateCohort(corrupt_head, &owners));
    try std.testing.expectError(error.InvalidGenerationPublication, validateCohort(head, owners[0..1]));
}

test "schema validation publication failure cancels the cohort without admitting a partial install" {
    var head = Fixture.head();
    var owners = [_]Owner{ Fixture.owner(0), Fixture.owner(1) };
    try Fixture.apply(&head, &owners[0], Fixture.ready(owners[0]));
    var failed = Fixture.ready(owners[1]);
    failed.phase = .failed;
    failed.failure_column = 0;
    try Fixture.apply(&head, &owners[1], failed);
    try std.testing.expectEqual(Phase.canceling, head.phase);
    try std.testing.expectEqual(Failure.not_null_violation, head.failure);
    const decision = try head.cancellationDecision();
    try std.testing.expectError(error.SchemaValidationPublicationRequired, head.installDecision());
    for (&owners) |*owner| {
        try std.testing.expectEqual(OwnerPermission.execute, try ownerPermission(head, owner.*, .cancel));
        try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(head, owner.*, .page));
        try std.testing.expectError(error.SchemaValidationPublicationRequired, ownerPermission(head, owner.*, .install));
        var canceled = Fixture.ready(owner.*);
        canceled.phase = .canceled;
        canceled.updated = Fixture.position(owner.*, 6);
        canceled.revision = 6;
        try Fixture.apply(&head, owner, canceled);
        try std.testing.expectEqualSlices(u8, &decision, &try head.cancellationDecision());
    }
    try std.testing.expectEqual(Phase.canceled, head.phase);
    try validateCohort(head, &owners);
    try std.testing.expectEqual(@as(u16, 0), head.installed);
    var unsafe = head;
    unsafe.installed = 1;
    try std.testing.expectError(error.InvalidGenerationPublication, unsafe.validate());
    var overflow = Fixture.head();
    overflow.revision = std.math.maxInt(u64);
    const owner = Fixture.owner(0);
    try std.testing.expectError(error.InvalidGenerationPublication, acknowledge(overflow, owner, overflow.revision, try Fixture.proof(owner, Fixture.ready(owner))));
    var forged = try Fixture.proof(owner, Fixture.ready(owner));
    forged.logical_digest[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, acknowledge(Fixture.head(), owner, 1, forged));
}

test "schema validation publication compact records reject corruption alternate encodings and foreign cohorts" {
    var head = Fixture.head();
    var owner = Fixture.owner(0);
    try Fixture.apply(&head, &owner, Fixture.ready(owner));
    const head_record = try head.encode();
    const owner_record = try owner.encode(head);
    try std.testing.expectEqualDeep(head, try Head.decode(head_record.slice()));
    try std.testing.expectEqualDeep(owner, try Owner.decode(owner_record.slice(), head));
    try std.testing.expectError(error.InvalidGenerationPublication, Head.decode(owner_record.slice()));
    try std.testing.expectError(error.InvalidGenerationPublication, Owner.decode(head_record.slice(), head));
    var corrupt = head_record;
    corrupt.bytes[15] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, Head.decode(corrupt.slice()));
    corrupt = head_record;
    corrupt.bytes[7] = 1;
    @memcpy(corrupt.bytes[corrupt.len - 32 ..][0..32], &contract.hash(corrupt.bytes[0 .. corrupt.len - 32]));
    try std.testing.expectError(error.InvalidGenerationPublication, Head.decode(corrupt.slice()));
    // A valid checksum does not make whitespace/field-order alternatives a
    // second accepted CAS representation of the same metadata state.
    corrupt = head_record;
    const body_len = corrupt.len - 44;
    std.mem.copyBackwards(u8, corrupt.bytes[13..][0..body_len], corrupt.bytes[12..][0..body_len]);
    corrupt.bytes[12] = ' ';
    corrupt.len += 1;
    std.mem.writeInt(u32, corrupt.bytes[8..12], @intCast(body_len + 1), .little);
    @memcpy(corrupt.bytes[corrupt.len - 32 ..][0..32], &contract.hash(corrupt.bytes[0 .. corrupt.len - 32]));
    try std.testing.expectError(error.InvalidGenerationPublication, Head.decode(corrupt.slice()));
    var foreign = head;
    foreign.id[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, Owner.decode(owner_record.slice(), foreign));
    foreign = head;
    foreign.owner_count = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, foreign.encode());
    foreign = head;
    foreign.metadata_incarnation = local.metadata_incarnation.zero;
    try std.testing.expectError(error.InvalidGenerationPublication, foreign.encode());
}

test "schema validation publication 4096 owner barriers keep ACK state and recovery workspace bounded" {
    var head = Fixture.head();
    head.owner_count = max_owners;
    const owners = try std.testing.allocator.alloc(Owner, max_owners);
    defer std.testing.allocator.free(owners);
    for (owners, 0..) |*owner, i| owner.* = Fixture.owner(@intCast(i));
    for (owners) |*owner| try Fixture.apply(&head, owner, Fixture.ready(owner.*));
    try std.testing.expectEqual(Phase.validated, head.phase);
    try std.testing.expectEqual(@as(u64, max_owners + 1), head.revision);
    try validateCohort(head, owners);
    const encoded = try head.encode();
    try std.testing.expect(encoded.len < HeadRecord.max_wire_bytes);
    const owner_record = try owners[max_owners - 1].encode(head);
    try std.testing.expect(owner_record.len < OwnerRecord.max_wire_bytes);
    var foreign = head;
    foreign.metadata_group_id += 1;
    const original_decision = try (try head.decideInstall(head.revision)).installDecision();
    const foreign_decision = try (try foreign.decideInstall(foreign.revision)).installDecision();
    try std.testing.expect(!eq(original_decision, foreign_decision));
}

// Reducer-only evidence fixtures: native signatures and enrolled source cuts
// are qualified by the ledger and transaction campaigns.
test "schema validation publication foreign keys require transaction coverage and classify violations" {
    var head = Fixture.head();
    var owner = Fixture.owner(0);
    owner.binding.purpose = .foreign_key_source;
    var ready = Fixture.ready(owner);
    ready.reference_coverage = .{};
    try std.testing.expectError(error.ForeignKeyValidationRequired, acknowledge(head, owner, head.revision, try Fixture.proof(owner, ready)));
    ready.reference_coverage = null;
    ready.transaction_coverage = .{ .record = @splat(41), .physical_root = owner.physical_root_incarnation };
    var wrong_root = ready;
    wrong_root.transaction_coverage.?.physical_root += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, acknowledge(head, owner, head.revision, try Fixture.proof(owner, wrong_root)));
    const accepted = try acknowledge(head, owner, head.revision, try Fixture.proof(owner, ready));
    try std.testing.expectEqual(@as(u16, 1), accepted.head.ready);
    try std.testing.expectEqual(Phase.validating, accepted.head.phase);
    var failed = ready;
    failed.phase = .failed;
    failed.failure_column = 0;
    try Fixture.apply(&head, &owner, failed);
    try std.testing.expectEqual(Failure.foreign_key_violation, head.failure);
    try std.testing.expectEqual(Phase.canceling, head.phase);
    try std.testing.expectError(error.GenerationPublicationChanged, head.decideInstall(head.revision));
    try std.testing.expectError(error.GenerationPublicationChanged, Fixture.head().cancel(Fixture.head().revision, .foreign_key_violation));
}

// Reducer-only fixture. Root adoption and signatures are independently
// verified by the ledger; this must not weaken ready/installed root checks.
test "schema validation publication FK release preserves accepted native transaction roots after adoption" {
    var head = Fixture.head();
    head.owner_count = 1;
    var owner = Fixture.owner(0);
    owner.binding.purpose = .foreign_key_source;
    var ready = Fixture.ready(owner);
    ready.transaction_coverage = .{ .record = @splat(41), .physical_root = owner.physical_root_incarnation };
    try Fixture.apply(&head, &owner, ready);
    head = try head.decideInstall(head.revision);
    var installed = ready;
    installed.installation = .{ .decision = try head.installDecision(), .ready_digest = try ready.digest(), .ready_position = ready.updated, .ready_last_effect = ready.last_effect, .physical_catalog = @splat(16), .activation = @splat(17), .position = Fixture.position(owner, 4) };
    installed.phase = .installed;
    installed.revision += 1;
    installed.updated = Fixture.position(owner, 4);
    installed.last_effect = @splat(18);
    try Fixture.apply(&head, &owner, installed);
    head = try head.publish(head.revision);
    var released = installed;
    released.release = .{ .publication = try head.publicationDecision(), .installed_digest = try installed.digest(), .installed_last_effect = installed.last_effect };
    released.phase = .released;
    released.revision += 1;
    released.updated = Fixture.position(owner, 5);
    released.last_effect = @splat(19);
    const accepted = owner;
    owner.root_epoch = 1;
    owner.root_origin = owner.physical_root_incarnation;
    owner.root_digest = @splat(44);
    owner.physical_root_incarnation += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, acknowledge(head, owner, head.revision, try Fixture.proof(owner, ready)));
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, acknowledge(head, owner, head.revision, try Fixture.proof(owner, installed)));
    try Fixture.apply(&head, &owner, released);
    try std.testing.expectEqual(Phase.succeeded, head.phase);
    try std.testing.expectEqualSlices(u8, &accepted.headers[@backingInt(Stage.ready)], &owner.headers[@backingInt(Stage.ready)]);
    try std.testing.expectEqualSlices(u8, &accepted.headers[@backingInt(Stage.installed)], &owner.headers[@backingInt(Stage.installed)]);
    try std.testing.expectEqual(@as(u64, 0), owner.proof_epochs[@backingInt(Stage.installed)]);
    try std.testing.expectEqual(@as(u64, 1), owner.proof_epochs[@backingInt(Stage.released)]);
}
